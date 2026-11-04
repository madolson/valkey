//! Internals re-exported for the micro-benchmarks in benches/ (feature
//! `bench`). Each wrapper does exactly what the named lib.rs path does.
use super::{Shared, VioCmd};
use std::collections::VecDeque;
use std::os::unix::io::RawFd;
use std::sync::Mutex;

pub fn eventfd_signal(fd: RawFd) {
    super::sys::eventfd_signal(fd)
}

pub fn eventfd_drain(fd: RawFd) {
    super::sys::eventfd_drain(fd)
}

pub fn empty_cmd() -> VioCmd {
    VioCmd {
        argv: std::ptr::null_mut(),
        argc: 2,
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

/// The per-connection command queue (`Conn::shared`).
pub struct CmdQueue(Mutex<Shared>);

impl Default for CmdQueue {
    fn default() -> Self {
        CmdQueue(Mutex::new(Shared {
            cmds: VecDeque::new(),
            cmds_input: 0,
            cmds_argv: 0,
            err_sample: Vec::new(),
            err_input_len: 0,
            out: Vec::new(),
        }))
    }
}

impl CmdQueue {
    /// IO thread side, as in Worker::parse_buffered(): one lock per batch.
    pub fn push_batch(&self, batch: &mut Vec<VioCmd>) -> usize {
        let mut sh = self.0.lock().unwrap();
        sh.cmds.extend(batch.drain(..));
        sh.cmds.len()
    }

    /// Main thread side, as in vio_conn_pop_cmds() (without the kick).
    pub fn pop(&self, out: &mut [std::mem::MaybeUninit<VioCmd>]) -> usize {
        let mut sh = self.0.lock().unwrap();
        let n = out.len().min(sh.cmds.len());
        for slot in out.iter_mut().take(n) {
            slot.write(sh.cmds.pop_front().unwrap());
        }
        n
    }

    /// Main thread reply copy, as in vio_conn_writev(): append under the lock.
    pub fn append_out(&self, b: &[u8]) {
        self.0.lock().unwrap().out.extend_from_slice(b);
    }

    /// IO thread side of Worker::flush(): swap the outbound buffer out.
    pub fn swap_out(&self, local: &mut Vec<u8>) {
        local.clear();
        std::mem::swap(local, &mut self.0.lock().unwrap().out);
    }
}
