//! A connection's lifecycle flags in one word, and the pure transition
//! function that decides what each event does (API plan step 10).
//!
//! The IO thread and the engine thread both change these flags. `step()`
//! has no side effects: it maps (flags, event) to (new flags, effects), and
//! `ConnState::apply()` installs the new flags with one compare-exchange and
//! returns the effects for the caller to carry out. The cases the bridge
//! got wrong or found subtle are unit tests below (and run under Miri).

use std::sync::atomic::{AtomicU32, Ordering};

/// The engine kicked the connection and the IO thread has not taken it.
pub(crate) const KICKED: u32 = 1 << 0;
/// The connection is on the engine's ready list (or about to be).
pub(crate) const IN_READY: u32 = 1 << 1;
/// The engine closed its side (vio_conn_close).
pub(crate) const C_CLOSED: u32 = 1 << 2;
/// The peer is gone (EOF or socket error).
pub(crate) const PEER_CLOSED: u32 = 1 << 3;
/// The engine asked for shutdown(2) and it has not been done yet.
pub(crate) const HALF_CLOSE: u32 = 1 << 4;

/// Post a kick message to the owning IO thread.
pub(crate) const FX_POST_KICK: u32 = 1 << 0;
/// Put the connection on the engine's ready list.
pub(crate) const FX_PUSH_READY: u32 = 1 << 1;
/// Tell the engine the peer is gone (VIO_EV_CLOSED).
pub(crate) const FX_NOTIFY_CLOSED: u32 = 1 << 2;
/// Release the connection (close the fd).
pub(crate) const FX_REMOVE: u32 = 1 << 3;
/// shutdown(2) the socket now.
pub(crate) const FX_SHUTDOWN: u32 = 1 << 4;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Event {
    /// Engine: something for the IO thread (output, resume, close).
    Kick,
    /// IO thread: took the kick message.
    KickTaken,
    /// IO thread: the engine should look at the connection.
    Notify,
    /// Engine: took the connection off the ready list.
    ReadyTaken,
    /// IO thread: EOF or socket error.
    PeerClosed,
    /// Engine: vio_conn_close().
    EngineClose,
    /// Engine: vio_conn_shutdown().
    EngineShutdown,
    /// IO thread, after a flush: whether every byte the engine handed over
    /// is on the socket (`out_drained`) and no write job is in flight.
    Flushed { out_drained: bool, job_idle: bool },
}

/// The transition function. Pure.
pub(crate) fn step(s: u32, ev: Event) -> (u32, u32) {
    match ev {
        /* Coalesced: one message per connection until the IO thread takes it. */
        Event::Kick => {
            if s & KICKED != 0 {
                (s, 0)
            } else {
                (s | KICKED, FX_POST_KICK)
            }
        }
        Event::KickTaken => (s & !KICKED, 0),
        Event::Notify => {
            if s & IN_READY != 0 {
                (s, 0)
            } else {
                (s | IN_READY, FX_PUSH_READY)
            }
        }
        Event::ReadyTaken => (s & !IN_READY, 0),
        /* Before the engine closed: tell it (also before it adopted the
         * connection; it sees NEW and CLOSED together). After: nothing waits
         * for the socket any more. A pending shutdown is moot. */
        Event::PeerClosed => {
            let n = (s | PEER_CLOSED) & !HALF_CLOSE;
            if s & C_CLOSED != 0 {
                (n, FX_REMOVE)
            } else {
                (n, FX_NOTIFY_CLOSED)
            }
        }
        /* The IO thread releases it once the output is written or the peer
         * is gone (Flushed, or the kick's own Flushed). */
        Event::EngineClose => step(s | C_CLOSED, Event::Kick),
        /* Deferred: shutdown(2) now could cut off a reply the IO thread has
         * not written (CLIENT KILL during BGSAVE lost it). */
        Event::EngineShutdown => {
            if s & PEER_CLOSED != 0 {
                (s, 0)
            } else {
                step(s | HALF_CLOSE, Event::Kick)
            }
        }
        Event::Flushed { out_drained, job_idle } => {
            let mut n = s;
            let mut fx = 0;
            if s & HALF_CLOSE != 0 && out_drained && job_idle {
                n &= !HALF_CLOSE;
                fx |= FX_SHUTDOWN;
            }
            if s & C_CLOSED != 0 && (out_drained || s & PEER_CLOSED != 0) {
                fx |= FX_REMOVE;
            }
            (n, fx)
        }
    }
}

pub(crate) struct ConnState(AtomicU32);

impl ConnState {
    pub(crate) fn new() -> Self {
        ConnState(AtomicU32::new(0))
    }

    pub(crate) fn get(&self) -> u32 {
        self.0.load(Ordering::Acquire)
    }

    pub(crate) fn has(&self, f: u32) -> bool {
        self.get() & f != 0
    }

    /// Apply `ev`; returns the effects to carry out.
    pub(crate) fn apply(&self, ev: Event) -> u32 {
        let mut s = self.0.load(Ordering::Acquire);
        loop {
            let (n, fx) = step(s, ev);
            if n == s {
                return fx;
            }
            match self.0.compare_exchange_weak(s, n, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return fx,
                Err(cur) => s = cur,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(evs: &[Event]) -> (u32, Vec<u32>) {
        let mut s = 0;
        let mut out = Vec::new();
        for &e in evs {
            let (n, fx) = step(s, e);
            s = n;
            out.push(fx);
        }
        (s, out)
    }

    /// The peer closes before the engine has adopted the connection: the
    /// engine is told (it sees NEW and CLOSED), and the connection is
    /// released only when the engine closes its side.
    #[test]
    fn peer_close_before_adopt() {
        let (s, fx) = run(&[Event::Notify, Event::PeerClosed, Event::ReadyTaken, Event::EngineClose, Event::KickTaken]);
        assert_eq!(fx, vec![FX_PUSH_READY, FX_NOTIFY_CLOSED, 0, FX_POST_KICK, 0]);
        assert_eq!(s & (C_CLOSED | PEER_CLOSED), C_CLOSED | PEER_CLOSED);
        /* The kick's evaluation releases it, whatever the output state. */
        assert_eq!(step(s, Event::Flushed { out_drained: false, job_idle: true }).1 & FX_REMOVE, FX_REMOVE);
    }

    /// The engine closes with output still pending: nothing is released
    /// until the output drains, or until the peer goes away.
    #[test]
    fn engine_close_with_output_pending() {
        let (s, fx) = run(&[Event::EngineClose, Event::KickTaken, Event::Flushed { out_drained: false, job_idle: true }]);
        assert_eq!(fx, vec![FX_POST_KICK, 0, 0]);
        assert_eq!(step(s, Event::Flushed { out_drained: true, job_idle: true }).1, FX_REMOVE);
        assert_eq!(step(s, Event::PeerClosed).1, FX_REMOVE);
    }

    /// Kicks coalesce until the IO thread takes the message; notifications
    /// until the engine takes the connection off the ready list.
    #[test]
    fn kick_coalescing() {
        let (_, fx) = run(&[Event::Kick, Event::Kick, Event::EngineClose, Event::KickTaken, Event::Kick]);
        assert_eq!(fx, vec![FX_POST_KICK, 0, 0, 0, FX_POST_KICK]);
        let (_, fx) = run(&[Event::Notify, Event::Notify, Event::ReadyTaken, Event::Notify]);
        assert_eq!(fx, vec![FX_PUSH_READY, 0, 0, FX_PUSH_READY]);
    }

    /// shutdown(2) waits until the output handed over before it is written
    /// and no job is in flight, happens once, and is dropped if the peer
    /// goes away first.
    #[test]
    fn deferred_shutdown() {
        let (s, fx) = run(&[
            Event::EngineShutdown,
            Event::KickTaken,
            Event::Flushed { out_drained: false, job_idle: true },
            Event::Flushed { out_drained: true, job_idle: false },
        ]);
        assert_eq!(fx, vec![FX_POST_KICK, 0, 0, 0]);
        let (s2, fx) = step(s, Event::Flushed { out_drained: true, job_idle: true });
        assert_eq!(fx, FX_SHUTDOWN);
        assert_eq!(step(s2, Event::Flushed { out_drained: true, job_idle: true }).1, 0);
        assert_eq!(s2 & C_CLOSED, 0, "a shutdown does not release the connection");
        let (s3, _) = step(s, Event::PeerClosed);
        assert_eq!(step(s3, Event::Flushed { out_drained: true, job_idle: true }).1, 0);
        assert_eq!(step(PEER_CLOSED, Event::EngineShutdown), (PEER_CLOSED, 0));
    }

    /// Two threads hammering apply(): every Kick that posted is matched by
    /// a KickTaken, so no kick is lost or doubled.
    #[test]
    fn concurrent_kicks_post_once_per_take() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;
        let n: usize = if cfg!(miri) { 40 } else { 20000 };
        let st = Arc::new(ConnState::new());
        let posted = Arc::new(AtomicUsize::new(0));
        let (s2, p2) = (st.clone(), posted.clone());
        let engine = std::thread::spawn(move || {
            for _ in 0..n {
                if s2.apply(Event::Kick) & FX_POST_KICK != 0 {
                    p2.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        let mut taken = 0usize;
        while !engine.is_finished() || posted.load(Ordering::SeqCst) > taken {
            if posted.load(Ordering::SeqCst) > taken {
                st.apply(Event::KickTaken);
                taken += 1;
            }
            std::hint::spin_loop();
        }
        engine.join().unwrap();
        assert_eq!(posted.load(Ordering::SeqCst), taken);
        assert!(!st.has(KICKED));
    }
}
