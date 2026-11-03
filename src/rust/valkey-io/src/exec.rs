//! Cut point 3 ("exec"): the IO layer owns the top-level call().
//!
//! `vio_exec_call()` is a Rust port of the control flow of `call()`
//! (src/server.c). Rust cannot see the `client` or `server` structs, so every
//! engine side effect goes through a small exported C helper in
//! `VioExecOps` (src/rust_io_exec.c). Rust owns the sequencing, the
//! conditions that decide which side effects run, the timing and the
//! propagation flag computation.
//!
//! In exec-rtc mode the IO thread that parsed a batch also executes it, under
//! the engine lock (the module GIL), see `rtc_batch()` and lib.rs `Worker::run`.
//! See DESIGN.md, "Stage 3".

use std::ffi::{c_int, c_void};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

/* call() flags and propagation targets (src/server.h); static_assert'ed on
 * the C side in rust_io_exec.c. */
pub const CMD_CALL_PROPAGATE_AOF: c_int = 1 << 0;
pub const CMD_CALL_PROPAGATE_REPL: c_int = 1 << 1;
pub const PROPAGATE_AOF: c_int = 1;
pub const PROPAGATE_REPL: c_int = 2;

/* Bits returned by VioExecOps::prop_info. */
pub const PI_PREVENT_PROP: u32 = 1 << 0; /* c->flag.prevent_prop */
pub const PI_IS_EXEC: u32 = 1 << 1; /* c->cmd->proc == execCommand */
pub const PI_IS_MODULE: u32 = 1 << 2; /* c->cmd->flags & CMD_MODULE */
pub const PI_FORCE_REPL: u32 = 1 << 3;
pub const PI_FORCE_AOF: u32 = 1 << 4;
pub const PI_PREVENT_REPL: u32 = 1 << 5; /* prevent_repl_prop || module_prevent_repl_prop */
pub const PI_PREVENT_AOF: u32 = 1 << 6; /* prevent_aof_prop || module_prevent_aof_prop */

/// State call() keeps on its stack, owned by Rust here.
#[repr(C)]
pub struct VioCallFrame {
    pub prev_client: *mut c_void,
    pub real_cmd: *mut c_void,
    pub dirty: i64,
    pub old_repl_offset: i64,
    pub old_flags: u32,
    pub update_command_stats: c_int,
    pub reprocessing: c_int,
}

type Cl = *mut c_void;

/// Engine helpers used by the Rust call(). Keep in sync with
/// include/valkey_io.h and src/rust_io_exec.c.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VioExecOps {
    pub bg_block: extern "C" fn(Cl) -> c_int,
    pub enter: extern "C" fn(Cl, *mut VioCallFrame),
    pub stats_err_reset: extern "C" fn(),
    pub enter_execution_unit: extern "C" fn(c_int, i64),
    pub begin_exec: extern "C" fn(Cl),
    pub proc_of: extern "C" fn(Cl) -> Option<extern "C" fn(Cl)>,
    pub debug_argv_begin: extern "C" fn(Cl) -> *mut c_void,
    pub debug_argv_end: extern "C" fn(Cl, *mut c_void),
    pub exit_execution_unit: extern "C" fn(),
    pub after_proc: extern "C" fn(Cl) -> c_int,
    pub add_duration: extern "C" fn(Cl, i64) -> i64,
    pub server_dirty: extern "C" fn() -> i64,
    pub count_failure: extern "C" fn(Cl, *mut c_void) -> c_int,
    pub fire_result_event: extern "C" fn(Cl, *mut c_void, c_int, i64, i64),
    pub close_after_command: extern "C" fn(Cl),
    pub latency: extern "C" fn(Cl, *mut c_void, i64),
    pub commandlog: extern "C" fn(Cl, *mut c_void),
    pub feed_monitors: extern "C" fn(Cl),
    pub cmd_stats: extern "C" fn(Cl, *mut c_void),
    pub reset_duration: extern "C" fn(Cl),
    pub prop_info: extern "C" fn(Cl) -> u32,
    pub also_propagate: extern "C" fn(Cl, c_int),
    pub restore_flags: extern "C" fn(Cl, u32),
    pub track_keys: extern "C" fn(Cl),
    pub count_command: extern "C" fn(),
    pub peak_memory: extern "C" fn(),
    pub after_command: extern "C" fn(Cl),
    pub set_woff: extern "C" fn(Cl, i64),
    pub leave: extern "C" fn(*mut c_void),
}

/// exec-rtc engine hooks, called on Rust IO threads.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VioRtcOps {
    /// Non-blocking acquire of the engine lock; 0 on success.
    pub try_lock: extern "C" fn() -> c_int,
    /// Blocking acquire; 0 when acquired, 1 when the holder is inside a
    /// yielding long command and the batch must go through the ready list.
    pub lock: extern "C" fn() -> c_int,
    pub unlock: extern "C" fn(),
    /// Run the engine side of one ready connection (adopt, execute, close).
    pub exec_ready: extern "C" fn(*const crate::Conn),
    /// End of a batch, lock still held: AOF write, reply flush, wake main if
    /// it has work only it does (beforeSleep()).
    pub after_batch: extern "C" fn(),
}

#[repr(C)]
pub struct VioExecConfig {
    pub ops: VioExecOps,
    pub rtc: c_int,
    pub rtc_ops: VioRtcOps,
}

struct Exec {
    ops: VioExecOps,
    rtc: Option<VioRtcOps>,
}
unsafe impl Sync for Exec {}
unsafe impl Send for Exec {}

static EXEC: OnceLock<Exec> = OnceLock::new();

/// Configure exec mode. Must be called before vio_start(). Returns 0.
///
/// # Safety
///
/// `cfg` must point to a valid `VioExecConfig`.
#[no_mangle]
pub unsafe extern "C" fn vio_exec_configure(cfg: *const VioExecConfig) -> c_int {
    let cfg = unsafe { &*cfg };
    let e = Exec { ops: cfg.ops, rtc: if cfg.rtc != 0 { Some(cfg.rtc_ops) } else { None } };
    if EXEC.set(e).is_err() {
        return -1;
    }
    0
}

pub(crate) fn rtc_ops() -> Option<&'static VioRtcOps> {
    EXEC.get().and_then(|e| e.rtc.as_ref())
}

/* time_t and c_long are i64 only on some targets: keep the casts. */
#[allow(clippy::unnecessary_cast)]
fn now_us(clock: libc::clockid_t) -> i64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe { libc::clock_gettime(clock, &mut ts) };
    ts.tv_sec as i64 * 1_000_000 + ts.tv_nsec as i64 / 1000
}

pub(crate) fn now_ns() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// The top-level call() for a client on a Rust connection. Mirrors
/// call() in src/server.c step by step; nested execution (EXEC, scripts,
/// RM_Call) still uses the C call().
#[no_mangle]
pub extern "C" fn vio_exec_call(c: *mut c_void, flags: c_int) {
    let o = &EXEC.get().expect("exec not configured").ops;

    /* Forkless save: may block the client (BLOCKED_INUSE) and re-run the
     * command later through processCommand(), i.e. back through here. */
    if (o.bg_block)(c) != 0 {
        return;
    }

    let mut f = VioCallFrame {
        prev_client: std::ptr::null_mut(),
        real_cmd: std::ptr::null_mut(),
        dirty: 0,
        old_repl_offset: 0,
        old_flags: 0,
        update_command_stats: 0,
        reprocessing: 0,
    };
    (o.enter)(c, &mut f);
    (o.stats_err_reset)();

    let call_timer = now_us(libc::CLOCK_REALTIME);
    (o.enter_execution_unit)(1, call_timer);
    (o.begin_exec)(c);

    let start = now_us(libc::CLOCK_MONOTONIC);
    let dbg = (o.debug_argv_begin)(c);
    let proc_ = (o.proc_of)(c).expect("command without proc");
    proc_(c);
    (o.debug_argv_end)(c, dbg);
    (o.exit_execution_unit)();

    let blocked = (o.after_proc)(c) != 0;
    let duration = now_us(libc::CLOCK_MONOTONIC) - start;
    let total = (o.add_duration)(c, duration);
    let mut dirty = (o.server_dirty)() - f.dirty;
    if dirty < 0 {
        dirty = 0;
    }

    let failed = (o.count_failure)(c, f.real_cmd);
    (o.fire_result_event)(c, f.real_cmd, failed, duration, dirty);
    (o.close_after_command)(c);

    let stats = f.update_command_stats != 0;
    if stats {
        (o.latency)(c, f.real_cmd, duration);
    }
    if stats && !blocked {
        (o.commandlog)(c, f.real_cmd);
    }
    if stats && f.reprocessing == 0 {
        (o.feed_monitors)(c);
    }
    if stats && !blocked {
        let _ = total;
        (o.cmd_stats)(c, f.real_cmd);
    }
    if !blocked {
        (o.reset_duration)(c);
    }

    if flags & (CMD_CALL_PROPAGATE_AOF | CMD_CALL_PROPAGATE_REPL) != 0 {
        let pi = (o.prop_info)(c);
        if pi & (PI_PREVENT_PROP | PI_IS_EXEC | PI_IS_MODULE) == 0 {
            let mut pf = 0;
            if dirty != 0 {
                pf |= PROPAGATE_AOF | PROPAGATE_REPL;
            }
            if pi & PI_FORCE_REPL != 0 {
                pf |= PROPAGATE_REPL;
            }
            if pi & PI_FORCE_AOF != 0 {
                pf |= PROPAGATE_AOF;
            }
            if pi & PI_PREVENT_REPL != 0 || flags & CMD_CALL_PROPAGATE_REPL == 0 {
                pf &= !PROPAGATE_REPL;
            }
            if pi & PI_PREVENT_AOF != 0 || flags & CMD_CALL_PROPAGATE_AOF == 0 {
                pf &= !PROPAGATE_AOF;
            }
            if pf != 0 {
                (o.also_propagate)(c, pf);
            }
        }
    }
    (o.restore_flags)(c, f.old_flags);
    (o.track_keys)(c);
    if !blocked {
        (o.count_command)();
    }
    (o.peak_memory)();
    (o.after_command)(c);
    (o.set_woff)(c, f.old_repl_offset);
    (o.leave)(f.prev_client);
}

/* ---------------------------------------------------------------------------
 * exec-rtc lock statistics
 * ------------------------------------------------------------------------- */

pub const BUCKETS: usize = 40;
pub const MAX_THREADS: usize = 128;

#[repr(align(64))]
pub struct RtcStats {
    pub wait: [AtomicU64; BUCKETS],
    pub hold: [AtomicU64; BUCKETS],
    pub wait_sum_ns: AtomicU64,
    pub hold_sum_ns: AtomicU64,
    pub batches: AtomicU64,
    pub uncontended: AtomicU64,
    pub conns: AtomicU64,
}

impl RtcStats {
    /* The array-repeat initializer for atomics needs a const item. */
    #[allow(clippy::declare_interior_mutable_const)]
    const fn new() -> Self {
        const Z: AtomicU64 = AtomicU64::new(0);
        RtcStats {
            wait: [Z; BUCKETS],
            hold: [Z; BUCKETS],
            wait_sum_ns: Z,
            hold_sum_ns: Z,
            batches: Z,
            uncontended: Z,
            conns: Z,
        }
    }
}

pub static RTC_STATS: [RtcStats; MAX_THREADS] = {
    #[allow(clippy::declare_interior_mutable_const)]
    const S: RtcStats = RtcStats::new();
    [S; MAX_THREADS]
};

pub fn bucket(ns: u64) -> usize {
    ((64 - ns.leading_zeros()) as usize).min(BUCKETS - 1)
}

/// Record one batch. Only the owning thread writes its slot (Relaxed).
pub(crate) fn rtc_record(t: usize, wait_ns: u64, hold_ns: u64, uncontended: bool, conns: usize) {
    let s = &RTC_STATS[t.min(MAX_THREADS - 1)];
    s.wait[bucket(wait_ns)].fetch_add(1, Ordering::Relaxed);
    s.hold[bucket(hold_ns)].fetch_add(1, Ordering::Relaxed);
    s.wait_sum_ns.fetch_add(wait_ns, Ordering::Relaxed);
    s.hold_sum_ns.fetch_add(hold_ns, Ordering::Relaxed);
    s.batches.fetch_add(1, Ordering::Relaxed);
    s.conns.fetch_add(conns as u64, Ordering::Relaxed);
    if uncontended {
        s.uncontended.fetch_add(1, Ordering::Relaxed);
    }
}

/// Format the lock statistics as INFO fields into buf (NUL terminated).
/// Returns the length written. Bucket i counts samples in [2^(i-1), 2^i) ns.
///
/// # Safety
///
/// `buf` must be writable for `len` bytes.
#[no_mangle]
pub unsafe extern "C" fn vio_rtc_stats(buf: *mut u8, len: usize) -> usize {
    use std::fmt::Write;
    let mut s = String::new();
    let mut wait = [0u64; BUCKETS];
    let mut hold = [0u64; BUCKETS];
    let (mut ws, mut hs, mut b, mut u, mut cn) = (0u64, 0u64, 0u64, 0u64, 0u64);
    for t in RTC_STATS.iter() {
        for i in 0..BUCKETS {
            wait[i] += t.wait[i].load(Ordering::Relaxed);
            hold[i] += t.hold[i].load(Ordering::Relaxed);
        }
        ws += t.wait_sum_ns.load(Ordering::Relaxed);
        hs += t.hold_sum_ns.load(Ordering::Relaxed);
        b += t.batches.load(Ordering::Relaxed);
        u += t.uncontended.load(Ordering::Relaxed);
        cn += t.conns.load(Ordering::Relaxed);
    }
    let _ = write!(
        s,
        "rtc_batches:{}\r\nrtc_uncontended:{}\r\nrtc_conns:{}\r\nrtc_wait_ns_sum:{}\r\nrtc_hold_ns_sum:{}\r\n",
        b, u, cn, ws, hs
    );
    let join = |h: &[u64; BUCKETS]| h.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",");
    let _ = write!(s, "rtc_wait_hist_log2ns:{}\r\nrtc_hold_hist_log2ns:{}\r\n", join(&wait), join(&hold));
    let n = s.len().min(len.saturating_sub(1));
    unsafe {
        std::ptr::copy_nonoverlapping(s.as_ptr(), buf, n);
        *buf.add(n) = 0;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets() {
        assert_eq!(bucket(0), 0);
        assert_eq!(bucket(1), 1);
        assert_eq!(bucket(2), 2);
        assert_eq!(bucket(3), 2);
        assert_eq!(bucket(1024), 11);
        assert_eq!(bucket(u64::MAX), BUCKETS - 1);
    }

    #[test]
    fn stats_format() {
        rtc_record(0, 100, 3000, true, 2);
        let mut buf = vec![0u8; 4096];
        let n = unsafe { vio_rtc_stats(buf.as_mut_ptr(), buf.len()) };
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(s.contains("rtc_batches:1\r\n"));
        assert!(s.contains("rtc_uncontended:1\r\n"));
        assert!(s.contains("rtc_conns:2\r\n"));
    }
}
