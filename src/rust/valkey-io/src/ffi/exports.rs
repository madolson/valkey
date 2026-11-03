//! `#[no_mangle]` exports. Thin shims over the crate internals.
use crate::precheck;
use crate::*;
use std::ffi::{c_int, c_longlong, c_void, CStr};
use std::sync::atomic::Ordering;
use super::{ConnHandle, ConnRef};

/// Bind the listener and start the IO threads. Returns the eventfd the
/// engine must poll for readability (then call vio_main_begin_drain), or
/// -1 with errno set.
///
/// # Safety
/// `cfg` must point at a valid VioConfig. Call once, on the engine thread.
#[no_mangle]
pub unsafe extern "C" fn vio_start(cfg: *const VioConfig) -> c_int {
    // SAFETY: caller contract.
    let cfg = unsafe { &*cfg };
    let bind = if cfg.bindaddr.is_null() {
        None
    } else {
        // SAFETY: a non-NULL bindaddr is a NUL-terminated string.
        Some(unsafe { CStr::from_ptr(cfg.bindaddr) }.to_string_lossy().into_owned())
    };
    let limits = EngineLimits::new(cfg.proto_max_bulk_len, cfg.client_max_querybuf_len);
    match start(bind, cfg.port, cfg.threads, cfg.cbs, limits) {
        Ok(fd) => fd,
        Err(e) => {
            sys::set_errno(e);
            -1
        }
    }
}

/// The engine changed proto-max-bulk-len or client-query-buffer-limit.
///
/// # Safety
/// None beyond the FFI ABI; any thread, after vio_start().
#[no_mangle]
pub unsafe extern "C" fn vio_set_limits(proto_max_bulk_len: c_longlong, client_max_querybuf_len: usize) {
    if let Some(g) = GLOBAL.get() {
        g.limits.set(proto_max_bulk_len, client_max_querybuf_len);
    }
}

/// Hand the kicks collected on the main thread to their IO threads. Called
/// once per event-loop iteration, before the main thread polls, and inside
/// processEventsWhileBlocked().
///
/// # Safety
/// Engine thread only (the thread that called vio_start()).
#[no_mangle]
pub unsafe extern "C" fn vio_main_flush_kicks() {
    if GLOBAL.get().is_none() {
        return;
    }
    MAIN_KICKS.with(|k| {
        MAIN_ARGV.with(|a| {
            let mut k = k.borrow_mut();
            let mut a = a.borrow_mut();
            let n = k.len().max(a.len());
            if k.len() < n {
                k.resize_with(n, Vec::new);
            }
            for (idx, list) in k.iter_mut().enumerate() {
                flush_thread(idx, list, a.get_mut(idx));
            }
        })
    });
}

/// Consume the main eventfd and take the current batch of ready
/// connections. Connections that become ready later go to the next batch.
///
/// # Safety
/// Engine thread only (the thread that called vio_start()).
#[no_mangle]
pub unsafe extern "C" fn vio_main_begin_drain() {
    let g = global();
    /* Drain, then clear, then take the list. Clearing first loses a wakeup
     * when a producer signals between the clear and the drain. */
    sys::eventfd_drain(g.main_efd);
    g.main_signaled.store(false, Ordering::SeqCst);
    take_ready(g);
}

/// Last call before the main thread polls. Returns 1 when work arrived that
/// it must not sleep on (poll with a zero timeout).
///
/// # Safety
/// Engine thread only (the thread that called vio_start()).
#[no_mangle]
pub unsafe extern "C" fn vio_main_before_poll() -> c_int {
    let g = global();
    g.main_sleeping.store(true, Ordering::SeqCst);
    g.main_pending.load(Ordering::SeqCst) as c_int
}

/// First call after the main thread polled. Takes work handed over while
/// it was awake and returns 1 if there is some, unless an eventfd write is
/// pending: then the eventfd handler takes it in this iteration, and taking
/// it here as well would run two batches before the first one's replies.
///
/// # Safety
/// Engine thread only (the thread that called vio_start()).
#[no_mangle]
pub unsafe extern "C" fn vio_main_after_poll() -> c_int {
    let g = global();
    g.main_sleeping.store(false, Ordering::SeqCst);
    if g.main_signaled.load(Ordering::SeqCst) {
        return 0;
    }
    take_ready(g)
}

/// Next ready connection from the current batch, or NULL. The caller owns
/// one reference and must vio_conn_release() it.
///
/// # Safety
/// Engine thread only (the thread that called vio_start()).
#[no_mangle]
pub unsafe extern "C" fn vio_main_next_ready() -> Option<ConnHandle> {
    MAIN_READY.with(|r| r.borrow_mut().pop_front()).map(|c| {
        c.in_ready.store(false, Ordering::Release);
        ConnHandle::new(c)
    })
}

/// Next completed zero-copy write job from the current batch, or NULL.
///
/// # Safety
/// Engine thread only (the thread that called vio_start()).
#[no_mangle]
pub unsafe extern "C" fn vio_main_next_done() -> *mut c_void {
    MAIN_DONE.with(|d| d.borrow_mut().pop_front()).unwrap_or(0) as *mut c_void
}

/// Whether the current batch still holds connections. True inside a long
/// command (script, module) that was started from vio_main_next_ready():
/// the rest of that batch is only reachable through this list, because the
/// eventfd that announced it was already drained.
///
/// # Safety
/// Engine thread only (the thread that called vio_start()).
#[no_mangle]
pub unsafe extern "C" fn vio_main_has_ready() -> c_int {
    (MAIN_READY.with(|r| !r.borrow().is_empty()) || MAIN_DONE.with(|d| !d.borrow().is_empty())) as c_int
}

/// A new strong reference to `h`.
///
/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_retain(h: ConnRef<'_>) -> ConnHandle {
    h.retain()
}

/// # Safety
/// `h` must be a live handle; the caller gives up this reference.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_release(h: ConnHandle) {
    drop(h);
}

/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_take_events(h: ConnRef<'_>) -> u32 {
    h.events.swap(0, Ordering::AcqRel)
}

/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_udata(h: ConnRef<'_>) -> *mut c_void {
    h.udata.load(Ordering::Relaxed)
}

/// # Safety
/// `h` must be a live handle. Rust stores `p` and never dereferences it.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_set_udata(h: ConnRef<'_>, p: *mut c_void) {
    h.udata.store(p, Ordering::Relaxed)
}

/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_fd(h: ConnRef<'_>) -> c_int {
    h.fd
}

/// Move up to `max` parsed commands into `out`, in order. argv ownership
/// moves to the caller.
///
/// # Safety
/// `h` must be a live handle and `out` valid for `max` writes.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_pop_cmds(h: ConnRef<'_>, out: *mut VioCmd, max: usize) -> usize {
    let conn = h.arc();
    let (n, left) = {
        let mut sh = conn.shared.lock().unwrap();
        let n = max.min(sh.cmds.len());
        for i in 0..n {
            // SAFETY: i < n <= max, and the caller provides max slots.
            unsafe { out.add(i).write(sh.cmds.pop_front().unwrap()) };
        }
        (n, sh.cmds.len())
    };
    if n > 0 && left < CMDS_LOW && conn.read_paused.load(Ordering::Acquire) {
        kick(&conn);
    }
    n
}

/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_pending_cmds(h: ConnRef<'_>) -> usize {
    h.shared.lock().unwrap().cmds.len()
}

/// Bytes accepted from the engine and not yet written to the socket.
///
/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_pending_out(h: ConnRef<'_>) -> usize {
    h.out_bytes.load(Ordering::Acquire)
}

/// Copy engine output into the connection's outbound buffer and wake its
/// IO thread. Returns bytes accepted, or -1 with errno EAGAIN (buffer full,
/// engine keeps the rest and installs its write handler) or EPIPE.
///
/// # Safety
/// `h` must be a live handle and `iov[0..iovcnt]` valid iovecs, each
/// pointing at `iov_len` readable bytes, for the duration of the call.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_writev(h: ConnRef<'_>, iov: *const libc::iovec, iovcnt: c_int) -> isize {
    let conn = h.arc();
    if conn.peer_closed.load(Ordering::Acquire) {
        sys::set_errno(libc::EPIPE);
        return -1;
    }
    let pending = conn.out_bytes.load(Ordering::Acquire);
    if pending >= OUT_HIGH {
        sys::set_errno(libc::EAGAIN);
        return -1;
    }
    let mut room = OUT_HIGH - pending;
    let mut written = 0usize;
    {
        let mut sh = conn.shared.lock().unwrap();
        #[cfg(feature = "timing")]
        {
            let t = timing::take_next_write();
            if t != 0 {
                /* One trace per flush: keep the older one. */
                if conn.trace.compare_exchange(0, t, Ordering::Relaxed, Ordering::Relaxed).is_err() {
                    timing::drop_trace(t);
                }
            }
        }
        for i in 0..iovcnt as usize {
            if room == 0 {
                break;
            }
            // SAFETY: i < iovcnt; the caller guarantees the iovecs and their
            // buffers are valid for the call.
            let v = unsafe { &*iov.add(i) };
            let n = v.iov_len.min(room);
            let s = unsafe { std::slice::from_raw_parts(v.iov_base as *const u8, n) };
            sh.out.extend_from_slice(s);
            room -= n;
            written += n;
        }
    }
    if written > 0 {
        conn.out_bytes.fetch_add(written, Ordering::AcqRel);
        kick(&conn);
    }
    written as isize
}

/// Hand the IO thread a sealed reply chunk to write from engine memory
/// (zero-copy). `job` comes back exactly once, through write_job on the IO
/// thread and then vio_main_next_done(). `release_offset` is the durable
/// offset the chunk's replies depend on; 0 means none (the only value used
/// until a durability fence exists, see DESIGN.md "Zero-copy replies").
///
/// # Safety
/// `h` must be a live handle with no job in flight; `job` stays valid until
/// it is returned.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_submit_write(h: ConnRef<'_>, job: *mut c_void, release_offset: u64) {
    assert_eq!(release_offset, 0, "durability fences are not implemented");
    #[cfg(feature = "timing")]
    {
        let t = crate::timing::take_next_write();
        if t != 0 && h.trace.compare_exchange(0, t, Ordering::Relaxed, Ordering::Relaxed).is_err() {
            crate::timing::drop_trace(t);
        }
    }
    let prev = h.job.swap(job as usize, Ordering::AcqRel);
    assert_eq!(prev, 0, "one write job per connection");
    kick(&h.arc());
}

/// Return an executed argv to the IO thread that built it, to be freed
/// there (batched with the kicks). NULL entries are skipped.
///
/// # Safety
/// `h` must be a live handle; ownership of `argv[0..argc]` moves to Rust.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_return_argv(h: ConnRef<'_>, argv: *mut *mut c_void, argc: c_int) {
    return_argv(h.thread, argv as usize, argc);
}

/// The engine wants VIO_EV_WRITABLE once outbound bytes drop below the low
/// watermark (it has data it could not hand over).
///
/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_want_writable(h: ConnRef<'_>, on: c_int) {
    let conn = h.arc();
    conn.want_writable.store(on != 0, Ordering::Release);
    if on != 0 {
        kick(&conn);
    }
}

/// Published engine state the parser needs: unauthenticated clients get
/// tighter multibulk/bulk limits.
///
/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_set_auth_required(h: ConnRef<'_>, on: c_int) {
    let conn = h.arc();
    conn.auth_required.store(on != 0, Ordering::Release);
    /* Store, then check: the IO thread sets parse_held before the engine
     * can see the command it stopped after, so this cannot miss it. */
    if conn.parse_held.load(Ordering::Acquire) {
        kick(&conn);
    }
}

/// # Safety
/// None beyond the FFI ABI; any thread.
#[no_mangle]
pub unsafe extern "C" fn vio_set_default_auth_required(on: c_int) {
    DEFAULT_AUTH_REQUIRED.store(on != 0, Ordering::Relaxed);
}

/// Publish the session fields the precheck reads (cut point 2). Read by
/// the IO thread once per parsed batch.
///
/// # Safety
/// `h` must be a live handle and `s` a valid VioSession for the call.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_set_session(h: ConnRef<'_>, s: *const precheck::VioSession) {
    // SAFETY: caller contract; the session is copied.
    h.session.set(unsafe { &*s });
}

/// The engine is done with the connection. Pending output is flushed, then
/// the socket is closed once every reference is released.
///
/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_close(h: ConnRef<'_>) {
    let conn = h.arc();
    conn.udata.store(std::ptr::null_mut(), Ordering::Relaxed);
    conn.want_writable.store(false, Ordering::Release);
    conn.c_closed.store(true, Ordering::Release);
    kick(&conn);
}

/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_shutdown(h: ConnRef<'_>) {
    /* Not shutdown(2) here: the IO thread may not have written the reply
     * the engine just handed over (CLIENT KILL during BGSAVE lost it). */
    h.half_close.store(true, Ordering::Release);
    kick(&h.arc());
}

/* ---------------------------------------------------------------------------
 * Per connection. `h` is a ConnRef: the engine holds a ConnHandle for it.
 * Calls are made on the thread that called vio_start().
 * ------------------------------------------------------------------------- */

#[cfg(feature = "timing")]
mod timing_exports {
    use crate::timing::{self, VioTimingStat};

    /// # Safety
    /// None beyond the FFI ABI.
    #[no_mangle]
    pub unsafe extern "C" fn vio_timing_next_write(id: u32) {
        timing::set_next_write(id);
    }

    /// # Safety
    /// None beyond the FFI ABI.
    #[no_mangle]
    pub unsafe extern "C" fn vio_timing_sample_n() -> u32 {
        timing::sample_n()
    }

    /// # Safety
    /// None beyond the FFI ABI.
    #[no_mangle]
    pub unsafe extern "C" fn vio_timing_dropped() -> u64 {
        timing::dropped()
    }

    /// # Safety
    /// None beyond the FFI ABI.
    #[no_mangle]
    pub unsafe extern "C" fn vio_timing_reset() {
        timing::reset();
    }

    /// # Safety
    /// None beyond the FFI ABI.
    #[no_mangle]
    pub unsafe extern "C" fn vio_timing_stamp(id: u32, stage: u32) {
        timing::stamp(id, stage as usize);
    }

    /// # Safety
    /// None beyond the FFI ABI.
    #[no_mangle]
    pub unsafe extern "C" fn vio_timing_drop(id: u32) {
        timing::drop_trace(id);
    }

    /// # Safety
    /// `out` must be valid for one write.
    #[no_mangle]
    pub unsafe extern "C" fn vio_timing_stat(stage: u32, out: *mut VioTimingStat) {
        // SAFETY: caller contract.
        unsafe { *out = timing::stat(stage) };
    }
}
