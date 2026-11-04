//! Cut point 2 ("precheck"): per-batch glue between the IO thread and the
//! engine's precheck callbacks.
//!
//! The policy decisions live in C (src/rust_io_precheck.c), because they read
//! engine structures (command table, ACL user copies). Rust owns the parts
//! that are about ordering on one connection:
//!
//! - the session view the engine publishes: a single-writer seqlock written
//!   only when a field changes (`vio_conn_set_session`), plus `consumed`, an
//!   atomic stored after each delivery (`vio_conn_publish`). Read once per
//!   parsed batch, without a lock;
//! - session barriers: after a command that can change the session (AUTH,
//!   SELECT, MULTI, ...), later commands on the same connection are sent
//!   unchecked until the engine reports, through `consumed`, that
//!   it popped the barrier command. Correctness never depends on this: the
//!   engine re-validates every token against the live client. Barriers only
//!   avoid prechecking against a session that is known to be stale.
//! - one precheck call per parsed batch (`precheck_batch`); the policy
//!   snapshot is pinned once per IO-thread poll iteration (lib.rs Worker).

use crate::{VioCallbacks, VioCmd};
use std::ffi::{c_int, c_void};
use std::sync::atomic::{fence, AtomicI32, AtomicPtr, AtomicU32, AtomicU64, Ordering};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct VioSession {
    pub user: *mut c_void,
    pub gen: u32,
    pub flags: u32,
    pub dbid: c_int,
}

impl VioSession {
    pub const fn empty() -> Self {
        VioSession { user: std::ptr::null_mut(), gen: 0, flags: 0, dbid: 0 }
    }
}

/// VioSession.flags bits (published by the engine).
pub const VIO_SESS_AUTHENTICATED: u32 = 1 << 0;
pub const VIO_SESS_MULTI: u32 = 1 << 1;
pub const VIO_SESS_PUBSUB_RESP2: u32 = 1 << 2;
/// Replica, primary, slot migration, no user.
pub const VIO_SESS_SPECIAL: u32 = 1 << 3;

/// The engine's session state for one connection: written by the engine
/// thread only (single writer), read by the connection's IO thread.
///
/// The session fields are a seqlock: `set` makes `seq` odd, stores the
/// fields, and makes it even again; `get` retries while `seq` is odd or
/// moved. The engine calls `set` only when a field changed (the generation
/// moves with it), so the common delivery touches only `consumed`.
///
/// `consumed` is outside the seqlock. It counts the commands the engine
/// popped; a Release store after the pop pairs with the Acquire load in
/// `get`, so an IO thread that sees a barrier command consumed also sees
/// the session the engine published before popping further.
#[derive(Default)]
pub struct SessionView {
    seq: AtomicU32,
    user: AtomicPtr<c_void>,
    gen: AtomicU32,
    flags: AtomicU32,
    dbid: AtomicI32,
    consumed: AtomicU64,
}

impl SessionView {
    pub fn new() -> Self {
        Self::default()
    }

    /// Engine thread only.
    pub fn set(&self, s: &VioSession) {
        let q = self.seq.load(Ordering::Relaxed);
        self.seq.store(q.wrapping_add(1), Ordering::Relaxed);
        fence(Ordering::Release);
        self.user.store(s.user, Ordering::Relaxed);
        self.gen.store(s.gen, Ordering::Relaxed);
        self.flags.store(s.flags, Ordering::Relaxed);
        self.dbid.store(s.dbid, Ordering::Relaxed);
        self.seq.store(q.wrapping_add(2), Ordering::Release);
    }

    /// Engine thread only.
    pub fn set_consumed(&self, n: u64) {
        self.consumed.store(n, Ordering::Release);
    }

    /// The session and the consumed count.
    pub fn get(&self) -> (VioSession, u64) {
        let consumed = self.consumed.load(Ordering::Acquire);
        loop {
            let q = self.seq.load(Ordering::Acquire);
            if q & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let s = VioSession {
                user: self.user.load(Ordering::Relaxed),
                gen: self.gen.load(Ordering::Relaxed),
                flags: self.flags.load(Ordering::Relaxed),
                dbid: self.dbid.load(Ordering::Relaxed),
            };
            fence(Ordering::Acquire);
            if self.seq.load(Ordering::Relaxed) == q {
                return (s, consumed);
            }
        }
    }
}

/// Per-connection state owned by the IO thread.
#[derive(Default)]
pub struct Local {
    /// Commands handed to the engine so far (1-based sequence of the last).
    seq: u64,
    /// Sequence number of the last barrier command, 0 if none.
    barrier_seq: u64,
}

/// Precheck one parsed batch on one connection with the pinned snapshot
/// `snap`: one crossing for the whole batch. Fills cmd/slot/read_flags/token
/// of every command (entries with `err != 0` are skipped by the engine).
/// `ncmds` is the number of commands (not errors); an error entry can only
/// be last.
pub fn check_batch(cbs: &VioCallbacks, snap: *mut c_void, view: &SessionView, local: &mut Local, cmds: &mut [VioCmd], ncmds: usize) {
    let check = match cbs.precheck_batch {
        Some(f) => f,
        None => return,
    };
    if cmds.is_empty() {
        return;
    }
    let (sess, consumed) = view.get();
    let barrier_active = local.barrier_seq > consumed;
    let last = check(snap, &sess, barrier_active as c_int, cmds.as_mut_ptr(), cmds.len());
    if last > 0 {
        local.barrier_seq = local.seq + last as u64;
    }
    local.seq += ncmds as u64;
}

#[cfg(test)]
#[allow(unsafe_code)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    static BARRIER_FLAGS: AtomicU32 = AtomicU32::new(0);

    extern "C" fn t_begin(_tid: c_int) -> *mut c_void {
        std::ptr::null_mut()
    }
    extern "C" fn t_end(_tid: c_int) {}
    /// argc encodes the test command: 1 = barrier, else plain. Token records
    /// whether the barrier was active. Mirrors rustPrecheckBatch().
    extern "C" fn t_check(_s: *mut c_void, _sess: *const VioSession, active: c_int, cmds: *mut VioCmd, n: usize) -> usize {
        let cmds = unsafe { std::slice::from_raw_parts_mut(cmds, n) };
        let mut last = 0;
        for (i, cmd) in cmds.iter_mut().enumerate() {
            let a = active != 0 || last != 0;
            cmd.token = a as u64;
            if a {
                BARRIER_FLAGS.fetch_add(1, Ordering::Relaxed);
            }
            if cmd.argc == 1 {
                last = i + 1;
            }
        }
        last
    }
    extern "C" fn t_create(_p: *const *const u8, _l: *const usize, _c: *mut VioCmd, _n: usize) {}
    extern "C" fn t_free(_a: *mut *mut c_void, _c: c_int) {}

    fn cbs() -> VioCallbacks {
        VioCallbacks {
            create_argvs: t_create,
            free_argv: t_free,
            write_job: None,
            precheck_begin: Some(t_begin),
            precheck_end: Some(t_end),
            precheck_batch: Some(t_check),
        }
    }

    fn cmd(argc: c_int) -> VioCmd {
        VioCmd {
            argv: std::ptr::null_mut(),
            argc,
            err: 0,
            err_byte: 0,
            argv_len_sum: 0,
            input_bytes: 0,
            cmd: std::ptr::null_mut(),
            slot: -1,
            read_flags: 0,
            token: 0,
            trace: 0,
        }
    }

    #[test]
    fn barrier_until_consumed() {
        let cbs = cbs();
        let cell = SessionView::new();
        let mut local = Local::default();
        let snap = std::ptr::null_mut();
        // Batch 1: plain, barrier (seq 2), plain, plain.
        let mut cs = [cmd(2), cmd(1), cmd(2), cmd(2)];
        check_batch(&cbs, snap, &cell, &mut local, &mut cs, 4);
        let active: Vec<u64> = cs.iter().map(|c| c.token).collect();
        assert_eq!(active, vec![0, 0, 1, 1]);
        // Engine consumed only the first command: still behind the barrier.
        cell.set_consumed(1);
        let mut c = [cmd(2)];
        check_batch(&cbs, snap, &cell, &mut local, &mut c, 1);
        assert_eq!(c[0].token, 1);
        // Engine popped the barrier: lifted.
        cell.set_consumed(2);
        let mut c = [cmd(2)];
        check_batch(&cbs, snap, &cell, &mut local, &mut c, 1);
        assert_eq!(c[0].token, 0);
    }

    #[test]
    fn seqlock_never_tears() {
        let v = std::sync::Arc::new(SessionView::new());
        let w = v.clone();
        let n: u32 = if cfg!(miri) { 50 } else { 20000 };
        let t = std::thread::spawn(move || {
            for i in 1..=n {
                let p = i as usize as *mut c_void;
                w.set(&VioSession { user: p, gen: i, flags: i, dbid: i as c_int });
                w.set_consumed(i as u64);
            }
        });
        let mut last = 0u64;
        loop {
            let (s, c) = v.get();
            assert!(s.gen == s.flags && s.gen as c_int == s.dbid && s.user as usize == s.gen as usize);
            assert!(c >= last);
            last = c;
            if c == n as u64 {
                break;
            }
        }
        t.join().unwrap();
    }

    #[test]
    fn off_without_callbacks() {
        let mut cbs = cbs();
        cbs.precheck_batch = None;
        let cell = SessionView::new();
        let mut c = [cmd(1)];
        check_batch(&cbs, std::ptr::null_mut(), &cell, &mut Local::default(), &mut c, 1);
        assert_eq!(c[0].token, 0);
    }
}
