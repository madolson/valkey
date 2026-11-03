//! The one wakeup protocol between threads (plan step 11).
//!
//! A `Mailbox` is a queue guarded by a mutex plus a `Waker` for the thread
//! that drains it. The IO threads' inboxes and the engine thread's ready list
//! are both mailboxes; before this module each hand-rolled the same Dekker
//! handshake.
//!
//! Protocol (all SeqCst):
//! - producer: push under the lock, then `pending = true`, then if the
//!   consumer is `sleeping` (or `always_signal`), write the eventfd unless
//!   `signaled` was already set.
//! - consumer, before blocking: `sleeping = true`, then read `pending`; if it
//!   is set, do not block. Either the producer sees `sleeping` and signals,
//!   or the consumer sees `pending`.
//! - consumer, on the eventfd: drain it, then clear `signaled`. Clearing
//!   first would lose a wakeup: a producer that sees `signaled` set skips the
//!   write, and its work must not wait behind a drained eventfd.

use crate::sys;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::Mutex;

pub(crate) struct Waker {
    /// eventfd the consumer polls; -1 in unit tests (signals are counted).
    efd: RawFd,
    signaled: AtomicBool,
    /// Set by the consumer just before it blocks, cleared after.
    sleeping: AtomicBool,
    /// The mailbox has entries the consumer has not taken.
    pending: AtomicBool,
    #[cfg(test)]
    signals: AtomicU64,
}

impl Waker {
    pub(crate) fn new(efd: RawFd, sleeping: bool) -> Self {
        Waker {
            efd,
            signaled: AtomicBool::new(false),
            sleeping: AtomicBool::new(sleeping),
            pending: AtomicBool::new(false),
            #[cfg(test)]
            signals: AtomicU64::new(0),
        }
    }

    /// Producer, after publishing work. `always_signal` writes the eventfd
    /// even to an awake consumer (the handoff measurement switches).
    pub(crate) fn wake(&self, always_signal: bool) {
        self.pending.store(true, Ordering::SeqCst);
        if (always_signal || self.sleeping.load(Ordering::SeqCst)) && !self.signaled.swap(true, Ordering::SeqCst) {
            self.signal();
        }
    }

    fn signal(&self) {
        #[cfg(test)]
        self.signals.fetch_add(1, Ordering::SeqCst);
        if self.efd >= 0 {
            sys::eventfd_signal(self.efd);
        }
    }

    /// Consumer, before blocking. True when work is pending: do not block.
    pub(crate) fn prepare_sleep(&self) -> bool {
        self.sleeping.store(true, Ordering::SeqCst);
        self.pending.load(Ordering::SeqCst)
    }

    /// Consumer, after its poll returned.
    pub(crate) fn awake(&self) {
        self.sleeping.store(false, Ordering::SeqCst);
    }

    /// Consumer, on its eventfd: drain, then clear.
    pub(crate) fn drain(&self) {
        if self.efd >= 0 {
            sys::eventfd_drain(self.efd);
        }
        self.signaled.store(false, Ordering::SeqCst);
    }

    /// An eventfd write is outstanding (its handler will take the work).
    pub(crate) fn signaled(&self) -> bool {
        self.signaled.load(Ordering::SeqCst)
    }

    /// Consumer: whether work was published since the last take.
    pub(crate) fn take_pending(&self) -> bool {
        self.pending.swap(false, Ordering::SeqCst)
    }

}

/// A queue of `B` (a Vec of messages, or the ready lists) and its waker.
pub(crate) struct Mailbox<B: Default> {
    pub(crate) waker: Waker,
    q: Mutex<B>,
}

impl<B: Default> Mailbox<B> {
    pub(crate) fn new(efd: RawFd, sleeping: bool) -> Self {
        Mailbox { waker: Waker::new(efd, sleeping), q: Mutex::new(B::default()) }
    }

    /// Add work under the lock, then wake the consumer.
    pub(crate) fn post<R>(&self, always_signal: bool, f: impl FnOnce(&mut B) -> R) -> R {
        let r = f(&mut self.q.lock().unwrap());
        self.waker.wake(always_signal);
        r
    }

    /// Consumer: take everything if work was published. None otherwise.
    pub(crate) fn take(&self) -> Option<B> {
        if !self.waker.take_pending() {
            return None;
        }
        Some(std::mem::take(&mut *self.q.lock().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// A consumer that blocks only when prepare_sleep() says it may never
    /// misses a message: every post is either seen as pending or produces a
    /// signal that ends the "block".
    #[test]
    fn no_lost_wakeup() {
        let n: usize = if cfg!(miri) { 30 } else { 20000 };
        let mb: Arc<Mailbox<Vec<usize>>> = Arc::new(Mailbox::new(-1, false));
        let p = mb.clone();
        let t = std::thread::spawn(move || {
            for i in 0..n {
                p.post(false, |q| q.push(i));
            }
        });
        let mut got = 0usize;
        let mut seen_signals = 0u64;
        while got < n {
            if let Some(v) = mb.take() {
                got += v.len();
                continue;
            }
            if mb.waker.prepare_sleep() {
                mb.waker.awake();
                continue;
            }
            // "Block" until a signal arrives (the eventfd would wake us).
            loop {
                let s = mb.waker.signals.load(Ordering::SeqCst);
                if s > seen_signals {
                    seen_signals = s;
                    break;
                }
                std::hint::spin_loop();
            }
            mb.waker.awake();
            mb.waker.drain();
        }
        t.join().unwrap();
        assert_eq!(got, n);
    }

    /// An awake consumer is not signaled; a sleeping one is, once.
    #[test]
    fn signal_only_when_sleeping() {
        let mb: Mailbox<Vec<u8>> = Mailbox::new(-1, false);
        mb.post(false, |q| q.push(1));
        assert_eq!(mb.waker.signals.load(Ordering::SeqCst), 0);
        assert_eq!(mb.take().unwrap(), vec![1]);
        assert!(!mb.waker.prepare_sleep());
        mb.post(false, |q| q.push(2));
        mb.post(false, |q| q.push(3));
        assert_eq!(mb.waker.signals.load(Ordering::SeqCst), 1);
        assert!(mb.waker.signaled());
        mb.waker.awake();
        mb.waker.drain();
        assert_eq!(mb.take().unwrap(), vec![2, 3]);
        mb.post(true, |q| q.push(4));
        assert_eq!(mb.waker.signals.load(Ordering::SeqCst), 2);
    }
}
