//! `#[no_mangle]` exports. Thin shims over the crate internals.
use crate::precheck;
use crate::*;
use std::ffi::{c_int, c_longlong, c_void, CStr};
use std::sync::atomic::Ordering;
use super::{ConnHandle, ConnRef};

/// Bind the listener and start the IO threads. Returns the runtime every
/// other export takes, and stores in `*efd` the eventfd the engine must
/// poll for readability (then call vio_main_begin_drain); NULL with errno
/// set on failure. The calling thread is the runtime's engine thread.
///
/// # Safety
/// `cfg` must point at a valid VioConfig and `efd` be writable. Call on the
/// engine thread.
#[no_mangle]
pub unsafe extern "C" fn vio_start(cfg: *const VioConfig, efd: *mut c_int) -> Option<&'static Runtime> {
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
        Ok((rt, fd)) => {
            // SAFETY: caller contract.
            unsafe { efd.write(fd) };
            Some(rt)
        }
        Err(e) => {
            sys::set_errno(e);
            None
        }
    }
}

/// The engine changed proto-max-bulk-len or client-query-buffer-limit.
///
/// # Safety
/// None beyond the FFI ABI; any thread, after vio_start().
#[no_mangle]
pub unsafe extern "C" fn vio_set_limits(rt: &Runtime, proto_max_bulk_len: c_longlong, client_max_querybuf_len: usize) {
    {
        let g = rt.g;
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
pub unsafe extern "C" fn vio_main_flush_kicks(rt: &Runtime) {
    let g = rt.g;
    g.adm.flush_popped();
    let mut m = rt.main.borrow_mut();
    let m = &mut *m;
    let (k, a) = (&mut m.kicks, &mut m.argv);
    let n = k.len().max(a.len());
    if k.len() < n {
        k.resize_with(n, Vec::new);
    }
    for (idx, list) in k.iter_mut().enumerate() {
        flush_thread(g, idx, list, a.get_mut(idx));
    }
}

/// Consume the main eventfd and take the current batch of ready
/// connections. Connections that become ready later go to the next batch.
///
/// # Safety
/// Engine thread only (the thread that called vio_start()).
#[no_mangle]
pub unsafe extern "C" fn vio_main_begin_drain(rt: &Runtime) {
    let g = rt.g;
    /* Drain, then clear, then take the list. Clearing first loses a wakeup
     * when a producer signals between the clear and the drain. */
    g.ready.waker.drain();
    take_ready(rt);
}

/// Last call before the main thread polls. Returns 1 when work arrived that
/// it must not sleep on (poll with a zero timeout).
///
/// # Safety
/// Engine thread only (the thread that called vio_start()).
#[no_mangle]
pub unsafe extern "C" fn vio_main_before_poll(rt: &Runtime) -> c_int {
    let g = rt.g;
    g.ready.waker.prepare_sleep() as c_int
}

/// First call after the main thread polled. Takes work handed over while
/// it was awake and returns 1 if there is some, unless an eventfd write is
/// pending: then the eventfd handler takes it in this iteration, and taking
/// it here as well would run two batches before the first one's replies.
///
/// # Safety
/// Engine thread only (the thread that called vio_start()).
#[no_mangle]
pub unsafe extern "C" fn vio_main_after_poll(rt: &Runtime) -> c_int {
    let g = rt.g;
    g.ready.waker.awake();
    if g.ready.waker.signaled() {
        return 0;
    }
    take_ready(rt)
}

/// Next ready connection from the current batch, or NULL. The caller owns
/// one reference and must vio_conn_release() it.
///
/// # Safety
/// Engine thread only (the thread that called vio_start()).
#[no_mangle]
pub unsafe extern "C" fn vio_main_next_ready(rt: &Runtime) -> Option<ConnHandle> {
    rt.main.borrow_mut().ready.pop_front().map(|c| {
        c.state.apply(crate::conn_state::Event::ReadyTaken);
        ConnHandle::new(c)
    })
}

/// Next completed zero-copy write job from the current batch, or NULL.
///
/// # Safety
/// Engine thread only (the thread that called vio_start()).
#[no_mangle]
pub unsafe extern "C" fn vio_main_next_done(rt: &Runtime) -> *mut c_void {
    rt.main.borrow_mut().done.pop_front().unwrap_or(0) as *mut c_void
}

/// Whether the current batch still holds connections. True inside a long
/// command (script, module) that was started from vio_main_next_ready():
/// the rest of that batch is only reachable through this list, because the
/// eventfd that announced it was already drained.
///
/// # Safety
/// Engine thread only (the thread that called vio_start()).
#[no_mangle]
pub unsafe extern "C" fn vio_main_has_ready(rt: &Runtime) -> c_int {
    let m = rt.main.borrow();
    (!m.ready.is_empty() || !m.done.is_empty()) as c_int
}

/// A new strong reference to `h`.
///
/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_retain(_rt: &Runtime, h: ConnRef<'_>) -> ConnHandle {
    h.retain()
}

/// # Safety
/// `h` must be a live handle; the caller gives up this reference.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_release(_rt: &Runtime, h: ConnHandle) {
    drop(h);
}

/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_take_events(rt: &Runtime, h: ConnRef<'_>) -> u32 {
    let ev = h.events.swap(0, Ordering::AcqRel);
    let g = rt.g;
    if ev & VIO_EV_NEW != 0 && g.adm.adopted() {
        g.threads[0].post(g, |q| q.push(Msg::ResumeAccept));
    }
    ev
}

/// Publish the engine's maxclients (admission control).
///
/// # Safety
/// None beyond the FFI ABI; any thread.
#[no_mangle]
pub unsafe extern "C" fn vio_set_maxclients(rt: &Runtime, n: usize) {
    {
        let g = rt.g;
        g.adm.set_maxclients(n);
    }
}

/// Connections refused at maxclients since the last call.
///
/// # Safety
/// None beyond the FFI ABI; any thread.
#[no_mangle]
pub unsafe extern "C" fn vio_take_rejected(rt: &Runtime) -> usize {
    rt.g.adm.take_rejected()
}

/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_udata(_rt: &Runtime, h: ConnRef<'_>) -> *mut c_void {
    h.udata.load(Ordering::Relaxed)
}

/// # Safety
/// `h` must be a live handle. Rust stores `p` and never dereferences it.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_set_udata(_rt: &Runtime, h: ConnRef<'_>, p: *mut c_void) {
    h.udata.store(p, Ordering::Relaxed)
}

/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_fd(_rt: &Runtime, h: ConnRef<'_>) -> c_int {
    h.fd
}

/// Move up to `max` parsed commands into `out`, in order. argv ownership
/// moves to the caller.
///
/// # Safety
/// `h` must be a live handle and `out` valid for `max` writes.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_pop_cmds(rt: &Runtime, h: ConnRef<'_>, out: *mut VioCmd, max: usize) -> usize {
    let conn = h.arc();
    let (n, left) = {
        let mut sh = conn.shared.lock().unwrap();
        let n = max.min(sh.cmds.len());
        for i in 0..n {
            // SAFETY: i < n <= max, and the caller provides max slots.
            unsafe { out.add(i).write(sh.cmds.pop_front().unwrap()) };
        }
        conn.ncmds.store(sh.cmds.len(), Ordering::Release);
        (n, sh.cmds.len())
    };
    rt.g.adm.popped(n);
    if n > 0 && left < CMDS_LOW && conn.read_paused.load(Ordering::Acquire) {
        kick(rt, &conn);
    }
    n
}

/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_pending_cmds(_rt: &Runtime, h: ConnRef<'_>) -> usize {
    h.ncmds.load(Ordering::Acquire)
}

/// Bytes accepted from the engine and not yet written to the socket.
///
/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_pending_out(_rt: &Runtime, h: ConnRef<'_>) -> usize {
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
pub unsafe extern "C" fn vio_conn_writev(rt: &Runtime, h: ConnRef<'_>, iov: *const libc::iovec, iovcnt: c_int) -> isize {
    let conn = h.arc();
    if conn.state.has(crate::conn_state::PEER_CLOSED) {
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
        kick(rt, &conn);
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
pub unsafe extern "C" fn vio_conn_submit_write(rt: &Runtime, h: ConnRef<'_>, job: *mut c_void, release_offset: u64) {
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
    kick(rt, &h.arc());
}

/// Return an executed argv to the IO thread that built it, to be freed
/// there (batched with the kicks). NULL entries are skipped.
///
/// # Safety
/// `h` must be a live handle; ownership of `argv[0..argc]` moves to Rust.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_return_argv(rt: &Runtime, h: ConnRef<'_>, argv: *mut *mut c_void, argc: c_int) {
    return_argv(rt, h.thread, argv as usize, argc);
}

/// The engine wants VIO_EV_WRITABLE once outbound bytes drop below the low
/// watermark (it has data it could not hand over).
///
/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_want_writable(rt: &Runtime, h: ConnRef<'_>, on: c_int) {
    let conn = h.arc();
    conn.want_writable.store(on != 0, Ordering::Release);
    if on != 0 {
        kick(rt, &conn);
    }
}

/// After each delivery: the commands the engine has popped so far (the
/// precheck lifts a session barrier once it is consumed) and whether the
/// client must still authenticate (unauthenticated clients get tighter
/// multibulk/bulk limits).
///
/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_publish(rt: &Runtime, h: ConnRef<'_>, consumed: u64, auth_required: c_int) {
    let conn = h.arc();
    conn.session.set_consumed(consumed);
    conn.auth_required.store(auth_required != 0, Ordering::Release);
    /* Store, then check: the IO thread sets parse_held before the engine
     * can see the command it stopped after, so this cannot miss it. */
    if conn.parse_held.load(Ordering::Acquire) {
        kick(rt, &conn);
    }
}

/// # Safety
/// None beyond the FFI ABI; any thread.
#[no_mangle]
pub unsafe extern "C" fn vio_set_default_auth_required(rt: &Runtime, on: c_int) {
    rt.g.default_auth_required.store(on != 0, Ordering::Relaxed);
}

/// Publish the session fields the precheck reads (cut point 2), when one
/// of them changed. Read by the IO thread once per parsed batch.
///
/// # Safety
/// `h` must be a live handle and `s` a valid VioSession for the call.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_set_session(_rt: &Runtime, h: ConnRef<'_>, s: *const precheck::VioSession) {
    // SAFETY: caller contract; the session is copied into the view.
    h.session.set(unsafe { &*s });
}

/// The engine is done with the connection. Pending output is flushed, then
/// the socket is closed once every reference is released.
///
/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_close(rt: &Runtime, h: ConnRef<'_>) {
    let conn = h.arc();
    /* Commands the engine will never pop: free them now instead of when
     * the output drains, so they stop counting against the queued budget. */
    let dropped: Vec<VioCmd> = {
        let mut sh = conn.shared.lock().unwrap();
        conn.ncmds.store(0, Ordering::Release);
        sh.cmds.drain(..).collect()
    };
    rt.g.adm.popped(dropped.len());
    for cmd in dropped {
        if !cmd.argv.is_null() {
            (rt.g.cbs.free_argv)(cmd.argv, cmd.argc);
        }
    }
    conn.udata.store(std::ptr::null_mut(), Ordering::Relaxed);
    conn.want_writable.store(false, Ordering::Release);
    close_conn(rt, &conn);
}

/// # Safety
/// `h` must be a live handle.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_shutdown(rt: &Runtime, h: ConnRef<'_>) {
    /* Not shutdown(2) here: the IO thread may not have written the reply
     * the engine just handed over (CLIENT KILL during BGSAVE lost it). */
    shutdown_conn(rt, &h.arc());
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
