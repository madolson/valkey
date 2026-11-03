//! Admission control (DESIGN.md "Admission control"). The C listener
//! throttles itself because accept runs on the busy main thread; here the IO
//! threads accept and read regardless of main-thread load, so bound what they
//! hand over:
//! - accepted connections the engine has not adopted yet (VIO_EV_NEW not
//!   taken): stop accepting at NEW_HIGH, resume below NEW_LOW. The kernel
//!   backlog holds the rest, as for the C listener.
//! - open connections: MAXCLIENTS_SLACK past maxclients, refuse on the IO
//!   thread with the engine's error, without involving the main thread.
//!   Below that the engine decides at adoption (acceptCommonHandler():
//!   maxclients-reserved, priority subnets, cluster links, its stats); this
//!   is only the fd backstop for when it is behind.
//! - parsed commands not taken by the engine, across all connections: above
//!   QUEUED_HIGH a connection stops reading once it holds FAIR_SHARE
//!   commands, so a busy pipeline cannot starve a fresh connection. It is
//!   resumed by the existing pop -> kick path, since it holds commands.

use crate::{CMDS_HIGH, CMDS_LOW};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const NEW_HIGH: usize = 128;
const NEW_LOW: usize = 64;
const QUEUED_HIGH: usize = 4096;
const FAIR_SHARE: usize = 32;
/// Below CONFIG_MIN_RESERVED_FDS (32), which the engine adds to maxclients
/// for its fd limit.
const MAXCLIENTS_SLACK: usize = 16;
pub(crate) const MAXCLIENTS_ERR: &[u8] = b"-ERR max number of clients reached\r\n";

pub(crate) struct Admission {
    /// Accepted connections whose VIO_EV_NEW the engine has not taken.
    unadopted: AtomicUsize,
    /// Open connections (accepted, fd not closed yet).
    live: AtomicUsize,
    /// Parsed commands queued in Rust, all connections.
    queued: AtomicUsize,
    /// The engine's maxclients, published by vio_set_maxclients().
    maxclients: AtomicUsize,
    /// Connections refused at maxclients, not yet counted by the engine.
    rejected: AtomicUsize,
    /// The listener is out of thread 0's epoll set (unadopted reached NEW_HIGH).
    accept_paused: AtomicBool,
}

impl Admission {
    pub(crate) fn new() -> Self {
        Admission {
            unadopted: AtomicUsize::new(0),
            live: AtomicUsize::new(0),
            queued: AtomicUsize::new(0),
            maxclients: AtomicUsize::new(usize::MAX),
            rejected: AtomicUsize::new(0),
            accept_paused: AtomicBool::new(false),
        }
    }

    /// Whether a connection holding `queued` commands may read more.
    pub(crate) fn may_read(&self, queued: usize) -> bool {
        queued < CMDS_HIGH && (queued < FAIR_SHARE || self.queued.load(Ordering::Relaxed) < QUEUED_HIGH)
    }

    /// Whether a paused connection holding `queued` commands may resume.
    pub(crate) fn may_resume(&self, queued: usize) -> bool {
        queued < CMDS_LOW && (queued < FAIR_SHARE || self.queued.load(Ordering::Relaxed) < QUEUED_HIGH)
    }

    pub(crate) fn queued_add(&self, n: usize) {
        self.queued.fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn queued_sub(&self, n: usize) {
        if n > 0 {
            self.queued.fetch_sub(n, Ordering::Relaxed);
        }
    }

    /// An accepted fd: false when it must be refused (maxclients backstop).
    pub(crate) fn admit(&self) -> bool {
        if self.live.load(Ordering::Relaxed) >= self.maxclients.load(Ordering::Relaxed).saturating_add(MAXCLIENTS_SLACK) {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        self.live.fetch_add(1, Ordering::Relaxed);
        self.unadopted.fetch_add(1, Ordering::SeqCst);
        true
    }

    /// The fd of an admitted connection was closed.
    pub(crate) fn closed(&self) {
        self.live.fetch_sub(1, Ordering::Relaxed);
    }

    /// Thread 0, before accept: true when accepting must pause. The caller
    /// then removes the listener from its epoll set.
    pub(crate) fn should_pause(&self) -> bool {
        self.unadopted.load(Ordering::SeqCst) >= NEW_HIGH
    }

    pub(crate) fn set_paused(&self) {
        self.accept_paused.store(true, Ordering::SeqCst);
    }

    /// After set_paused() and removing the listener: the engine may have
    /// adopted below NEW_LOW between the load and the store, and seen
    /// accept_paused clear. True when the caller must re-add it itself.
    pub(crate) fn undo_pause(&self) -> bool {
        self.unadopted.load(Ordering::SeqCst) < NEW_LOW && self.accept_paused.swap(false, Ordering::SeqCst)
    }

    /// The engine took a VIO_EV_NEW. True when thread 0 must resume
    /// accepting.
    pub(crate) fn adopted(&self) -> bool {
        self.unadopted.fetch_sub(1, Ordering::SeqCst) - 1 < NEW_LOW && self.accept_paused.swap(false, Ordering::SeqCst)
    }

    pub(crate) fn set_maxclients(&self, n: usize) {
        self.maxclients.store(n, Ordering::Relaxed);
    }

    pub(crate) fn take_rejected(&self) -> usize {
        self.rejected.swap(0, Ordering::Relaxed)
    }
}
