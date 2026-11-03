//! Sampled per-stage timing (cargo feature `timing`, C define RUST_IO_TIMING).
//!
//! One command in `N` (env RUST_IO_TIMING_N, default 64) gets a trace slot
//! on the IO thread. Each pipeline stage stores an rdtsc stamp in the slot;
//! the last stage (reply bytes written) folds the deltas into per-stage
//! log-linear histograms and frees the slot. A stage's latency is its stamp
//! minus the latest earlier stamp that is present (precheck is absent in io
//! mode). Stamps are TSC ticks; the TSC rate is calibrated against
//! CLOCK_MONOTONIC between the first trace and the INFO call.
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::OnceLock;

pub const ST_READ: usize = 0;
pub const ST_PARSED: usize = 1;
pub const ST_ARGV: usize = 2;
pub const ST_PRECHECK: usize = 3;
pub const ST_ENQUEUED: usize = 4;
pub const ST_DEQUEUED: usize = 5;
pub const ST_PROCESS: usize = 6;
pub const ST_CALL: usize = 7;
pub const ST_CALL_EXIT: usize = 8;
pub const ST_REPLY_COPIED: usize = 9;
pub const ST_WRITTEN: usize = 10;
pub const NSTAMPS: usize = 11;
/// Histogram NSTAMPS is end to end (written - read).
pub const NHIST: usize = NSTAMPS + 1;

const SLOTS: usize = 1024;
const SUB: u32 = 8; /* sub-buckets per power of two: <= 12.5% bucket width */
const NBUCKETS: usize = 16 + 60 * SUB as usize;

struct Slot {
    busy: AtomicBool,
    t: [AtomicU64; NSTAMPS],
}

struct Hist {
    count: AtomicU64,
    max: AtomicU64,
    b: Vec<AtomicU64>,
}

struct State {
    n: u32,
    slots: Vec<Slot>,
    next: AtomicU32,
    dropped: AtomicU64,
    hist: Vec<Hist>,
    tsc0: AtomicU64,
    ns0: AtomicU64,
}

static STATE: OnceLock<State> = OnceLock::new();

#[inline(always)]
pub fn rdtsc() -> u64 {
    crate::sys::rdtsc()
}

fn mono_ns() -> u64 {
    crate::sys::monotonic_ns()
}

fn st() -> &'static State {
    STATE.get_or_init(|| {
        let n = std::env::var("RUST_IO_TIMING_N").ok().and_then(|v| v.parse().ok()).unwrap_or(64u32).max(1);
        State {
            n,
            slots: (0..SLOTS)
                .map(|_| Slot { busy: AtomicBool::new(false), t: std::array::from_fn(|_| AtomicU64::new(0)) })
                .collect(),
            next: AtomicU32::new(0),
            dropped: AtomicU64::new(0),
            hist: (0..NHIST)
                .map(|_| Hist {
                    count: AtomicU64::new(0),
                    max: AtomicU64::new(0),
                    b: (0..NBUCKETS).map(|_| AtomicU64::new(0)).collect(),
                })
                .collect(),
            tsc0: AtomicU64::new(rdtsc()),
            ns0: AtomicU64::new(mono_ns()),
        }
    })
}

thread_local! {
    static COUNTDOWN: Cell<u32> = const { Cell::new(1) };
    static LAST_READ: Cell<u64> = const { Cell::new(0) };
    static NEXT_WRITE: Cell<u32> = const { Cell::new(0) };
}

/// Main thread: tag the bytes of the next vio_conn_writev() (vio_timing_next_write).
pub fn set_next_write(id: u32) {
    NEXT_WRITE.with(|c| c.set(id));
}

pub fn sample_n() -> u32 {
    st().n
}

pub fn dropped() -> u64 {
    st().dropped.load(Ordering::Relaxed)
}

/// Clear all histograms (vio_timing_reset).
pub fn reset() {
    let s = st();
    for h in &s.hist {
        h.count.store(0, Ordering::Relaxed);
        h.max.store(0, Ordering::Relaxed);
        for b in &h.b {
            b.store(0, Ordering::Relaxed);
        }
    }
    s.dropped.store(0, Ordering::Relaxed);
}

/// Main thread: the trace id for the next vio_conn_writev().
pub fn take_next_write() -> u32 {
    NEXT_WRITE.with(|c| c.replace(0))
}


/// IO thread: a read(2) just returned bytes.
#[inline(always)]
pub fn on_read() {
    LAST_READ.with(|c| c.set(rdtsc()));
}

/// IO thread: a frame was parsed. Returns a trace id (0 = not sampled).
#[inline(always)]
pub fn sample_parsed() -> u32 {
    let hit = COUNTDOWN.with(|c| {
        let v = c.get() - 1;
        if v == 0 {
            c.set(st().n);
            true
        } else {
            c.set(v);
            false
        }
    });
    if !hit {
        return 0;
    }
    let now = rdtsc();
    let s = st();
    let i = s.next.fetch_add(1, Ordering::Relaxed) as usize % SLOTS;
    let slot = &s.slots[i];
    /* Slots are reused round-robin; an abandoned trace (no reply, closed
     * connection) is simply overwritten. SLOTS * N commands in flight would
     * be needed to overwrite a live one. */
    slot.busy.store(true, Ordering::Relaxed);
    for t in &slot.t {
        t.store(0, Ordering::Relaxed);
    }
    slot.t[ST_READ].store(LAST_READ.with(|c| c.get()), Ordering::Relaxed);
    slot.t[ST_PARSED].store(now, Ordering::Relaxed);
    i as u32 + 1
}

#[inline(always)]
pub fn stamp(id: u32, stage: usize) {
    if id != 0 && stage < NSTAMPS {
        st().slots[id as usize - 1].t[stage].store(rdtsc(), Ordering::Release);
    }
}

fn bucket(v: u64) -> usize {
    if v < 16 {
        return v as usize;
    }
    let e = 63 - v.leading_zeros(); /* >= 4 */
    let sub = ((v >> (e - 3)) & (SUB as u64 - 1)) as usize;
    (16 + (e as usize - 4) * SUB as usize + sub).min(NBUCKETS - 1)
}

fn bucket_upper(i: usize) -> u64 {
    if i < 16 {
        return i as u64;
    }
    let e = (i - 16) / SUB as usize + 4;
    let sub = ((i - 16) % SUB as usize) as u64;
    ((SUB as u64 + sub + 1) << (e - 3)) - 1
}

fn record(h: &Hist, v: u64) {
    h.count.fetch_add(1, Ordering::Relaxed);
    h.max.fetch_max(v, Ordering::Relaxed);
    h.b[bucket(v)].fetch_add(1, Ordering::Relaxed);
}

/// Last stage: stamp, fold into histograms (TSC ticks), free the slot.
pub fn complete(id: u32) {
    if id == 0 {
        return;
    }
    let s = st();
    let slot = &s.slots[id as usize - 1];
    if !slot.busy.swap(false, Ordering::AcqRel) {
        return;
    }
    slot.t[ST_WRITTEN].store(rdtsc(), Ordering::Relaxed);
    let t: [u64; NSTAMPS] = std::array::from_fn(|k| slot.t[k].load(Ordering::Acquire));
    let mut prev = t[ST_READ];
    if prev != 0 {
        for (k, &tk) in t.iter().enumerate().skip(1) {
            if tk == 0 {
                continue;
            }
            record(&s.hist[k], tk.saturating_sub(prev));
            prev = tk;
        }
        record(&s.hist[NSTAMPS], t[ST_WRITTEN].saturating_sub(t[ST_READ]));
    }
}

/// Abandon a trace (command freed without a reply, connection closed).
pub fn drop_trace(id: u32) {
    if id != 0 {
        st().dropped.fetch_add(1, Ordering::Relaxed);
        st().slots[id as usize - 1].busy.store(false, Ordering::Release);
    }
}

#[repr(C)]
pub struct VioTimingStat {
    pub count: u64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
}

fn ns_per_tick() -> f64 {
    let s = st();
    let dt = rdtsc().saturating_sub(s.tsc0.load(Ordering::Relaxed)) as f64;
    let dn = mono_ns().saturating_sub(s.ns0.load(Ordering::Relaxed)) as f64;
    if dt > 0.0 {
        dn / dt
    } else {
        1.0
    }
}

/// Histogram summary for one stage (exported as vio_timing_stat).
pub fn stat(stage: u32) -> VioTimingStat {
    let s = st();
    let h = &s.hist[(stage as usize).min(NHIST - 1)];
    let k = ns_per_tick();
    let counts: Vec<u64> = h.b.iter().map(|b| b.load(Ordering::Relaxed)).collect();
    let total: u64 = counts.iter().sum();
    let q = |p: f64| -> u64 {
        if total == 0 {
            return 0;
        }
        let want = ((total as f64) * p).ceil().max(1.0) as u64;
        let mut acc = 0;
        for (i, c) in counts.iter().enumerate() {
            acc += c;
            if acc >= want {
                return (bucket_upper(i) as f64 * k) as u64;
            }
        }
        0
    };
    VioTimingStat {
        count: total,
        p50_ns: q(0.50),
        p90_ns: q(0.90),
        p99_ns: q(0.99),
        max_ns: (h.max.load(Ordering::Relaxed) as f64 * k) as u64,
    }
}






#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_monotonic_and_bounded() {
        let mut last = 0;
        for v in [0u64, 1, 15, 16, 17, 100, 1000, 12345, 1 << 30, u64::MAX] {
            let b = bucket(v);
            assert!(b >= last);
            last = b;
            if v < (1 << 60) {
                assert!(bucket_upper(b) >= v, "v={} b={} up={}", v, b, bucket_upper(b));
            }
        }
    }
}
