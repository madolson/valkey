//! Cut point 2 ("precheck"): per-batch glue between the IO thread and the
//! engine's precheck callbacks.
//!
//! The policy decisions live in C (src/rust_io_precheck.c), because they read
//! engine structures (command table, ACL user copies). Rust owns the parts
//! that are about ordering on one connection:
//!
//! - the session mirror the engine publishes (`vio_conn_set_session`), read
//!   once per parsed batch;
//! - session barriers: after a command that can change the session (AUTH,
//!   SELECT, MULTI, ...), later commands on the same connection are sent
//!   unchecked until the engine reports, through `VioSession.consumed`, that
//!   it popped the barrier command. Correctness never depends on this: the
//!   engine re-validates every token against the live client. Barriers only
//!   avoid prechecking against a session that is known to be stale.
//! - pinning the policy snapshot for the duration of one batch.

use crate::{VioCallbacks, VioCmd};
use std::ffi::{c_int, c_void};
use std::sync::Mutex;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct VioSession {
    pub user: *mut c_void,
    pub consumed: u64,
    pub gen: u32,
    pub flags: u32,
    pub dbid: c_int,
}

impl VioSession {
    pub const fn empty() -> Self {
        VioSession { user: std::ptr::null_mut(), consumed: 0, gen: 0, flags: 0, dbid: 0 }
    }
}

pub type BeginFn = extern "C" fn(tid: c_int) -> *mut c_void;
pub type EndFn = extern "C" fn(tid: c_int);
pub type CheckFn =
    extern "C" fn(snap: *mut c_void, sess: *const VioSession, barrier_active: c_int, cmd: *mut VioCmd) -> c_int;

/// Per-connection state, shared with the main thread.
pub struct SessionCell(Mutex<VioSession>);

impl Default for SessionCell {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionCell {
    pub fn new() -> Self {
        SessionCell(Mutex::new(VioSession::empty()))
    }
    pub fn set(&self, s: &VioSession) {
        *self.0.lock().unwrap() = *s;
    }
    fn get(&self) -> VioSession {
        *self.0.lock().unwrap()
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

/// One parsed batch on one connection.
pub struct Batch<'a> {
    cbs: &'a VioCallbacks,
    tid: c_int,
    snap: *mut c_void,
    sess: VioSession,
}

impl<'a> Batch<'a> {
    /// None when precheck mode is off (no callbacks).
    pub fn begin(cbs: &'a VioCallbacks, tid: usize, cell: &SessionCell) -> Option<Batch<'a>> {
        let begin = cbs.precheck_begin?;
        cbs.precheck_cmd?;
        let snap = begin(tid as c_int);
        Some(Batch { cbs, tid: tid as c_int, snap, sess: cell.get() })
    }

    /// Precheck one command. Fills cmd/slot/read_flags/token.
    pub fn check(&self, local: &mut Local, cmd: &mut VioCmd) {
        local.seq += 1;
        let barrier_active = local.barrier_seq > self.sess.consumed;
        let check = self.cbs.precheck_cmd.unwrap();
        let is_barrier = check(self.snap, &self.sess, barrier_active as c_int, cmd as *mut VioCmd);
        if is_barrier != 0 {
            local.barrier_seq = local.seq;
        }
    }
}

impl Drop for Batch<'_> {
    fn drop(&mut self) {
        if let Some(end) = self.cbs.precheck_end {
            end(self.tid);
        }
    }
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
    /// whether the barrier was active.
    extern "C" fn t_check(_s: *mut c_void, _sess: *const VioSession, active: c_int, cmd: *mut VioCmd) -> c_int {
        let cmd = unsafe { &mut *cmd };
        cmd.token = active as u64;
        if active != 0 {
            BARRIER_FLAGS.fetch_add(1, Ordering::Relaxed);
        }
        (cmd.argc == 1) as c_int
    }
    extern "C" fn t_create(_p: *const *const u8, _l: *const usize, _a: c_int) -> *mut *mut c_void {
        std::ptr::null_mut()
    }
    extern "C" fn t_free(_a: *mut *mut c_void, _c: c_int) {}

    fn cbs() -> VioCallbacks {
        VioCallbacks {
            create_argv: t_create,
            free_argv: t_free,
            write_job: None,
            precheck_begin: Some(t_begin),
            precheck_end: Some(t_end),
            precheck_cmd: Some(t_check),
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
            #[cfg(feature = "timing")]
            trace: 0,
        }
    }

    #[test]
    fn barrier_until_consumed() {
        let cbs = cbs();
        let cell = SessionCell::new();
        let mut local = Local::default();
        // Batch 1: plain, barrier (seq 2), plain, plain.
        {
            let b = Batch::begin(&cbs, 0, &cell).unwrap();
            let mut cs = [cmd(2), cmd(1), cmd(2), cmd(2)];
            for c in cs.iter_mut() {
                b.check(&mut local, c);
            }
            let active: Vec<u64> = cs.iter().map(|c| c.token).collect();
            assert_eq!(active, vec![0, 0, 1, 1]);
        }
        // Engine consumed only the first command: still behind the barrier.
        cell.set(&VioSession { consumed: 1, ..VioSession::empty() });
        {
            let b = Batch::begin(&cbs, 0, &cell).unwrap();
            let mut c = cmd(2);
            b.check(&mut local, &mut c);
            assert_eq!(c.token, 1);
        }
        // Engine popped the barrier: lifted.
        cell.set(&VioSession { consumed: 2, ..VioSession::empty() });
        {
            let b = Batch::begin(&cbs, 0, &cell).unwrap();
            let mut c = cmd(2);
            b.check(&mut local, &mut c);
            assert_eq!(c.token, 0);
        }
    }

    #[test]
    fn off_without_callbacks() {
        let mut cbs = cbs();
        cbs.precheck_begin = None;
        let cell = SessionCell::new();
        assert!(Batch::begin(&cbs, 0, &cell).is_none());
    }
}
