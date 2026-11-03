//! valkey-io: a Rust IO layer for valkey-server.
//!
//! Rust owns the listening socket, the client sockets, reads, RESP parsing and
//! writes. The engine (C) sees one `VioConn` handle per connection and pulls
//! parsed commands from it. See DESIGN.md for the threading and ownership
//! model and the full FFI surface; the C declarations live in
//! include/valkey_io.h.

#![deny(unsafe_code, unsafe_op_in_unsafe_fn)]

mod admission;
mod conn_state;
mod ffi;
mod handoff;
pub mod precheck;
pub mod sys;
pub mod resp;
#[cfg(feature = "timing")]
pub mod timing;
#[cfg(feature = "bench")]
pub mod benchapi;

use conn_state::{Event, C_CLOSED, FX_NOTIFY_CLOSED, FX_POST_KICK, FX_PUSH_READY, FX_REMOVE, FX_SHUTDOWN, HALF_CLOSE, PEER_CLOSED};
use resp::{Arg, Limits, Outcome, Parser};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::ffi::{c_char, c_int, c_void};
use std::os::unix::io::{IntoRawFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/* ---------------------------------------------------------------------------
 * FFI types (keep in sync with include/valkey_io.h)
 * ------------------------------------------------------------------------- */

/// Engine callbacks. Called on Rust IO threads, so they must be thread safe.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VioCallbacks {
    /// Build the argv of every command in `cmds[0..n]` (entries with
    /// `err == 0`): `robj **` arrays of `argc` string objects, taking the
    /// byte slices from `ptrs`/`lens` in order. One call per parsed batch.
    pub create_argvs: extern "C" fn(ptrs: *const *const u8, lens: *const usize, cmds: *mut VioCmd, n: usize),
    /// Free an argv built by create_argv that the engine never consumed.
    pub free_argv: extern "C" fn(argv: *mut *mut c_void, argc: c_int),
    /// Zero-copy replies: write the sealed reply chunk of `job` (an engine
    /// session, see vio_conn_submit_write) to `fd`, from engine memory, on
    /// the IO thread. `can_write` 0: do not write (copied bytes are still
    /// queued ahead of it, or the peer is gone), only complete. Returns 1
    /// when the chunk was written, 0 when the socket would block, -1 on a
    /// socket error. The job is complete when this returns; the engine
    /// releases it on its own thread (vio_main_next_done). NULL disables.
    pub write_job: Option<extern "C" fn(job: *mut c_void, fd: c_int, can_write: c_int) -> c_int>,
    /// Precheck mode only (cut point 2), NULL otherwise. See precheck.rs.
    /// begin/end pin the policy snapshot for one IO-thread poll iteration;
    /// precheck_batch checks one parsed batch and returns the 1-based index
    /// of its last session barrier, 0 if none.
    pub precheck_begin: Option<extern "C" fn(tid: c_int) -> *mut c_void>,
    pub precheck_end: Option<extern "C" fn(tid: c_int)>,
    pub precheck_batch: Option<
        extern "C" fn(snap: *mut c_void, sess: *const precheck::VioSession, barrier_active: c_int, cmds: *mut VioCmd, n: usize) -> usize,
    >,
}

#[repr(C)]
pub struct VioConfig {
    pub bindaddr: *const c_char,
    pub port: c_int,
    pub threads: c_int,
    /// proto-max-bulk-len and client-query-buffer-limit at start;
    /// vio_set_limits() publishes later changes.
    pub proto_max_bulk_len: std::ffi::c_longlong,
    pub client_max_querybuf_len: usize,
    pub cbs: VioCallbacks,
}

/// One parsed command, or a protocol error when `err != 0`.
#[repr(C)]
pub struct VioCmd {
    pub argv: *mut *mut c_void,
    pub argc: c_int,
    /// resp::ProtoError value, 0 for a command.
    pub err: c_int,
    /// Offending byte for ProtoError::UnexpectedChar.
    pub err_byte: c_int,
    /// Sampled stage-timing id (RUST_IO_TIMING builds), else 0. Always
    /// present: cbindgen emits a `#[cfg]` field's `;` after the `#endif`,
    /// which -pedantic rejects when the feature is off. It sits in the
    /// padding before argv_len_sum, so it costs no space.
    pub trace: u32,
    pub argv_len_sum: usize,
    pub input_bytes: u64,
    /// Precheck mode: struct serverCommand *, slot, read flags and token
    /// filled on the IO thread; token 0 when not prechecked.
    pub cmd: *mut c_void,
    pub slot: c_int,
    pub read_flags: c_int,
    pub token: u64,
}

pub const VIO_EV_NEW: u32 = 1 << 0;
pub const VIO_EV_CLOSED: u32 = 1 << 1;
pub const VIO_EV_WRITABLE: u32 = 1 << 2;
pub const VIO_EV_QBLIMIT: u32 = 1 << 3;
/// The input this layer holds for the connection grew (client memory
/// accounting: the engine re-reads vio_conn_mem() on delivery).
pub const VIO_EV_MEM: u32 = 1 << 4;

/* Backpressure thresholds. */
pub(crate) const CMDS_HIGH: usize = 1024; /* stop reading when this many commands are queued */
pub(crate) const CMDS_LOW: usize = 256; /* resume below this */
const OUT_HIGH: usize = 1 << 20; /* refuse engine writes past this many pending bytes */
const OUT_LOW: usize = 256 << 10; /* fire the engine write handler below this */
const READ_CHUNK: usize = 16 * 1024;

/* Conn.detach states. */
pub(crate) const DET_NONE: u8 = 0;
pub(crate) const DET_WANTED: u8 = 1;
pub(crate) const DET_DONE: u8 = 2;
pub(crate) const DET_FAILED: u8 = 3;
/// Read-buffer sweep period (clientsCron visits a client about this often
/// at the default hz with few clients).
const SWEEP_EVERY: Duration = Duration::from_millis(100);
/// How long the IO thread waits for the socket to take the output pending
/// at detach time.
const DETACH_FLUSH_MS: u64 = 5000;

/* ---------------------------------------------------------------------------
 * Connection shared between one IO thread and the main thread
 * ------------------------------------------------------------------------- */

struct Shared {
    cmds: VecDeque<VioCmd>,
    /// For vio_conn_mem(): input bytes and argv bytes (argv_len_sum plus
    /// the pointer array) of `cmds`.
    cmds_input: usize,
    cmds_argv: usize,
    /// Protocol error: a sample of the input at the error and its length,
    /// for the engine's "Query buffer: ..." log line (vio_conn_err_sample).
    err_sample: Vec<u8>,
    err_input_len: usize,
    out: Vec<u8>,
}

/// setProtocolError()'s PROTO_DUMP_LEN.
const PROTO_DUMP_LEN: usize = 128;

/// What setProtocolError() logs of the query buffer: all of it below
/// PROTO_DUMP_LEN bytes, else the first and last PROTO_DUMP_LEN / 2.
fn err_sample(input: &[u8]) -> Vec<u8> {
    if input.len() < PROTO_DUMP_LEN {
        input.to_vec()
    } else {
        let h = PROTO_DUMP_LEN / 2;
        [&input[..h], &input[input.len() - h..]].concat()
    }
}

/// argv bytes of a parsed command, as getClientMemoryUsage() counts argv.
fn cmd_argv_bytes(c: &VioCmd) -> usize {
    c.argv_len_sum + c.argc.max(0) as usize * std::mem::size_of::<*mut c_void>()
}

/// Engine thread: mark a connection high priority (or not).
fn set_priority(conn: &Conn, on: bool) {
    if conn.priority.swap(on, Ordering::Relaxed) != on {
        if on {
            conn.g.prio_conns.fetch_add(1, Ordering::Relaxed);
        } else {
            conn.g.prio_conns.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

pub struct Conn {
    /// The runtime this connection belongs to.
    g: &'static Global,
    fd: RawFd,
    thread: usize,
    token: AtomicUsize,
    /// Engine connection pointer. Main thread only.
    udata: AtomicPtr<c_void>,
    /// Lifecycle flags (conn_state.rs): kicked, in_ready, engine closed,
    /// peer closed, shutdown pending.
    state: conn_state::ConnState,
    events: AtomicU32,
    want_writable: AtomicBool,
    read_paused: AtomicBool,
    auth_required: AtomicBool,
    /// Unparsed input held until the engine republishes auth_required.
    parse_held: AtomicBool,
    /// Bytes accepted from the engine and not yet written to the socket.
    out_bytes: AtomicUsize,
    /// `shared.cmds.len()`, stored under the lock by whoever changed it, so
    /// the engine can ask for it without taking the lock. A stale zero is
    /// harmless: commands queued after the engine's last pop announce
    /// themselves with a new readiness (in_ready was cleared before the pop).
    ncmds: AtomicUsize,
    /// Input bytes the parser has not consumed (CLIENT LIST qbuf) and
    /// private read-buffer capacity, written by
    /// the IO thread when they change, read by vio_conn_mem().
    in_len: AtomicUsize,
    in_cap: AtomicUsize,
    /// High-priority connection (connSetPriority()): the engine drains it
    /// ahead of the others in a ready batch.
    priority: AtomicBool,
    /// Fork-child IO window (DESIGN.md "Detached connections"): DET_*.
    /// While DET_DONE the engine does IO on the fd itself and this layer
    /// neither reads nor writes it.
    pub(crate) detach: AtomicU8,
    /// Zero-copy write job in flight (engine pointer), 0 if none. Set by
    /// the engine thread, taken by the IO thread. At most one.
    job: AtomicUsize,
    shared: Mutex<Shared>,
    /// The engine's session view (precheck mode).
    session: precheck::SessionView,
    /// Stage-timing id riding on the bytes in `shared.out`.
    #[cfg(feature = "timing")]
    trace: AtomicU32,
}

impl Drop for Conn {
    fn drop(&mut self) {
        set_priority(self, false);
        let g = self.g;
        let sh = self.shared.get_mut().unwrap();
        g.adm.queued_sub(sh.cmds.len());
        g.adm.closed();
        for cmd in sh.cmds.drain(..) {
            if !cmd.argv.is_null() {
                (g.cbs.free_argv)(cmd.argv, cmd.argc);
            }
        }
        sys::close(self.fd);
    }
}

/* ---------------------------------------------------------------------------
 * Globals
 * ------------------------------------------------------------------------- */

enum Msg {
    NewFd(RawFd),
    Kick(Arc<Conn>),
    /// An executed argv the engine returned, to free on the thread that
    /// built it: (argv, argc). One entry per argv, so the engine's batch
    /// Vec keeps its capacity instead of being moved to the IO thread.
    FreeArgv(usize, c_int),
    /// To thread 0: unadopted connections fell below NEW_LOW, poll the
    /// listener again.
    ResumeAccept,
    /// Write what is pending, then stop all IO on the fd (vio_conn_detach).
    Detach(Arc<Conn>),
    /// Resume IO on a detached fd (vio_conn_attach).
    Attach(Arc<Conn>),
}

struct IoThread {
    epfd: RawFd,
    /// Messages for this thread; its waker's eventfd is in `epfd`.
    inbox: handoff::Mailbox<Vec<Msg>>,
}

impl IoThread {
    fn post(&self, g: &Global, f: impl FnOnce(&mut Vec<Msg>)) {
        self.inbox.post(g.hd(HD_IO_AWAKE), f)
    }
}

/// What the IO threads hand the engine thread: connections with work, and
/// completed zero-copy write jobs.
#[derive(Default)]
struct Ready {
    conns: Vec<Arc<Conn>>,
    done: Vec<usize>,
}

/// Engine config the parser applies, pushed by value by the engine
/// (vio_start, vio_set_limits). Read with Relaxed: a CONFIG SET only has to
/// apply to reads that start after it.
pub(crate) struct EngineLimits {
    max_bulk_len: std::sync::atomic::AtomicI64,
    max_querybuf_len: AtomicUsize,
}

impl EngineLimits {
    pub(crate) fn new(max_bulk_len: i64, max_querybuf_len: usize) -> Self {
        EngineLimits {
            max_bulk_len: std::sync::atomic::AtomicI64::new(max_bulk_len),
            max_querybuf_len: AtomicUsize::new(max_querybuf_len),
        }
    }
    pub(crate) fn set(&self, max_bulk_len: i64, max_querybuf_len: usize) {
        self.max_bulk_len.store(max_bulk_len, Ordering::Relaxed);
        self.max_querybuf_len.store(max_querybuf_len, Ordering::Relaxed);
    }
    fn max_bulk_len(&self) -> i64 {
        self.max_bulk_len.load(Ordering::Relaxed)
    }
    fn max_querybuf_len(&self) -> usize {
        self.max_querybuf_len.load(Ordering::Relaxed)
    }
}

struct Global {
    cbs: VioCallbacks,
    limits: EngineLimits,
    threads: Vec<IoThread>,
    /// What the IO threads hand the engine thread. Its waker's sleeping flag
    /// is set by vio_main_before_poll and cleared by vio_main_after_poll.
    ready: handoff::Mailbox<Ready>,
    listen_fd: RawFd,
    /// The port the listener is bound to (the configured one, or the
    /// kernel's choice for port 0).
    #[cfg_attr(not(test), allow(dead_code))]
    port: u16,
    adm: admission::Admission,
    /// authRequired() of a client that has not authenticated yet, published
    /// by the engine. New connections start with it, so their first read is
    /// parsed with the right limits before the engine adopts them.
    default_auth_required: AtomicBool,
    /// Measurement switch, read once from VIO_HANDOFF_DISABLE at
    /// vio_start(): bit 1 = no main-thread kick batching, bit 2 = always
    /// write the IO-thread eventfd, bit 4 = always write the main eventfd
    /// (the stage 1 handoff is 7). Default 2: skipping the eventfd for an
    /// awake IO thread lets it drain its kicks early, sleep, and be woken
    /// again; with one IO thread at -P 16 that cost 16% throughput
    /// (DESIGN.md "Handoff v2").
    handoff_disable: u32,
    /// Capacity of every connection's private read buffer, all threads: the
    /// engine adds it to used memory for maxmemory (vio_held_input_total()).
    rbuf_total: AtomicUsize,
    /// Connections with `priority` set; take_ready() reorders only when > 0.
    prio_conns: AtomicUsize,
}

impl Global {
    fn hd(&self, bit: u32) -> bool {
        self.handoff_disable & bit != 0
    }
}

/// Engine-thread state: what used to be thread-locals of the thread that
/// called vio_start().
#[derive(Default)]
pub(crate) struct MainState {
    /// Kicks per IO thread, flushed by vio_main_flush_kicks().
    kicks: Vec<Vec<Arc<Conn>>>,
    /// Returned argv per IO thread, flushed with the kicks.
    argv: Vec<Vec<(usize, c_int)>>,
    ready: VecDeque<Arc<Conn>>,
    /// Completed write jobs taken with the current batch.
    done: VecDeque<usize>,
    /// Swapped with the ready mailbox (handoff::Mailbox::swap).
    swap: Ready,
}

/// One IO layer instance (`VioRuntime` in C): returned by vio_start() and
/// passed to every export. It lives until the process exits (there is no
/// vio_stop), so its IO threads and connections hold `&'static Global`.
pub struct Runtime {
    pub(crate) g: &'static Global,
    /// Engine thread only (exports document it); checked in kick().
    pub(crate) main: RefCell<MainState>,
    main_thread: usize,
}


const HD_KICK_BATCH: u32 = 1;
const HD_IO_AWAKE: u32 = 2;
const HD_MAIN_AWAKE: u32 = 4;

/// Ask the owning IO thread to re-evaluate a connection (flush output,
/// resume reading, finish a close). Coalesced: one inbox entry per conn
/// until the IO thread picks it up, one eventfd write per wakeup.
fn kick(rt: &Runtime, conn: &Arc<Conn>) {
    if conn.state.apply(Event::Kick) & FX_POST_KICK != 0 {
        post_kick(rt, conn);
    }
}

/// Engine thread: hand the fd to the engine (fork-child IO window). Waits
/// until the IO thread has written the pending output and stopped all IO
/// on the fd. False if the output did not drain in time.
fn detach(conn: &Arc<Conn>) -> bool {
    match conn.detach.load(Ordering::Acquire) {
        DET_DONE => return true,
        DET_NONE | DET_FAILED => {}
        _ => unreachable!("detach in progress on the engine thread"),
    }
    conn.detach.store(DET_WANTED, Ordering::Release);
    let g = conn.g;
    g.threads[conn.thread].post(g, |q| q.push(Msg::Detach(conn.clone())));
    let deadline = Instant::now() + Duration::from_millis(DETACH_FLUSH_MS + 1000);
    loop {
        match conn.detach.load(Ordering::Acquire) {
            DET_DONE => return true,
            DET_FAILED => return false,
            _ => {}
        }
        if Instant::now() > deadline {
            return false;
        }
        std::thread::sleep(Duration::from_micros(50));
    }
}

/// Engine thread: give the fd back to the IO thread.
fn attach(conn: &Arc<Conn>) {
    if conn.detach.load(Ordering::Acquire) == DET_NONE {
        return;
    }
    let g = conn.g;
    g.threads[conn.thread].post(g, |q| q.push(Msg::Attach(conn.clone())));
}

/// vio_conn_close(): released by the IO thread once the output is written
/// or the peer is gone.
fn close_conn(rt: &Runtime, conn: &Arc<Conn>) {
    if conn.state.apply(Event::EngineClose) & FX_POST_KICK != 0 {
        post_kick(rt, conn);
    }
}

/// vio_conn_shutdown(): shutdown(2) deferred until the output is written.
fn shutdown_conn(rt: &Runtime, conn: &Arc<Conn>) {
    if conn.state.apply(Event::EngineShutdown) & FX_POST_KICK != 0 {
        post_kick(rt, conn);
    }
}

fn post_kick(rt: &Runtime, conn: &Arc<Conn>) {
    let g = rt.g;
    /* Main thread: collect, and hand each IO thread its kicks in one inbox
     * append and at most one eventfd write at the end of beforeSleep()
     * (vio_main_flush_kicks). */
    if sys::thread_self() == rt.main_thread && !g.hd(HD_KICK_BATCH) {
        {
            let mut m = rt.main.borrow_mut();
            let k = &mut m.kicks;
            if k.len() <= conn.thread {
                k.resize_with(conn.thread + 1, Vec::new);
            }
            let list = &mut k[conn.thread];
            list.push(conn.clone());
            /* Large batches: hand over early so the IO thread writes while
             * the main thread is still producing replies. */
            if list.len() >= KICK_BATCH {
                flush_kicks(g, conn.thread, list);
            }
        }
        return;
    }
    g.threads[conn.thread].post(g, |q| q.push(Msg::Kick(conn.clone())));
}


/// Main-thread kicks per IO thread handed over before the end of the
/// iteration.
const KICK_BATCH: usize = 32;

fn flush_kicks(g: &Global, idx: usize, list: &mut Vec<Arc<Conn>>) {
    g.threads[idx].post(g, |q| q.extend(list.drain(..).map(Msg::Kick)));
}

/// Argv returned per IO thread handed over before the end of the iteration.
const ARGV_BATCH: usize = 256;

/// Main thread: queue an executed argv for the IO thread that built it.
/// Handed over with the kicks (vio_main_flush_kicks), or early when the
/// batch is large.
fn return_argv(rt: &Runtime, thread: usize, argv: usize, argc: c_int) {
    let g = rt.g;
    {
        let mut m = rt.main.borrow_mut();
        let a = &mut m.argv;
        if a.len() <= thread {
            a.resize_with(thread + 1, Vec::new);
        }
        a[thread].push((argv, argc));
        if a[thread].len() >= ARGV_BATCH {
            let v = &mut a[thread];
            g.threads[thread].post(g, |q| q.extend(v.drain(..).map(|(p, n)| Msg::FreeArgv(p, n))));
        }
    }
}

/// Main thread: hand IO thread `idx` its kicks and returned argv with one
/// inbox lock and at most one wakeup.
fn flush_thread(g: &Global, idx: usize, kicks: &mut Vec<Arc<Conn>>, argv: Option<&mut Vec<(usize, c_int)>>) {
    let argv = argv.filter(|a| !a.is_empty());
    if kicks.is_empty() && argv.is_none() {
        return;
    }
    g.threads[idx].post(g, |q| {
        q.extend(kicks.drain(..).map(Msg::Kick));
        if let Some(v) = argv {
            q.extend(v.drain(..).map(|(p, n)| Msg::FreeArgv(p, n)));
        }
    });
}

/* ---------------------------------------------------------------------------
 * IO thread
 * ------------------------------------------------------------------------- */


const TOK_WAKE: u64 = 0;
const TOK_LISTEN: u64 = 1;
const TOK_BASE: u64 = 2;

struct Local {
    conn: Arc<Conn>,
    rbuf: Vec<u8>,
    rpos: usize,
    parser: Parser,
    precheck: precheck::Local,
    proto_err: bool,
    wbuf: Vec<u8>,
    woff: usize,
    /// The last zero-copy write would have blocked: wait for EPOLLOUT
    /// before reporting VIO_EV_WRITABLE.
    sock_blocked: bool,
    registered: bool,
    interest: u32,
    /// `rbuf` is the worker's shared read buffer, lent for one read.
    rbuf_shared: bool,
    /// in_len/in_cap last published, and peak use since the last sweep
    /// (querybuf_peak).
    len_pub: usize,
    cap_pub: usize,
    peak: usize,
    last_read: Instant,
}

struct Worker {
    g: &'static Global,
    idx: usize,
    slab: Vec<Option<Local>>,
    free: Vec<usize>,
    to_notify: Vec<Arc<Conn>>,
    /// Write jobs completed in this poll iteration.
    done: Vec<usize>,
    rr: usize,
    /// Precheck snapshot pinned for this poll iteration (pointer as usize).
    pinned: Option<usize>,
    /// Reused across batches (growing them per batch cost the IO thread
    /// several glibc allocations per batch): argument pointers (as usize, so
    /// the Worker stays Send), lengths, and the parsed arguments kept alive
    /// until create_argvs.
    ptrs: Vec<usize>,
    lens: Vec<usize>,
    held: Vec<Vec<Arg>>,
    /// Swapped with the inbox (handoff::Mailbox::swap).
    inbox_buf: Vec<Msg>,
    /// Shared read buffer (thread_shared_qb): a connection reads into it
    /// unless it holds a partial command, and keeps it only if input is
    /// left unparsed. Idle connections then hold no read buffer.
    shared_rbuf: Vec<u8>,
    /// Connections holding a private read buffer (cap_pub != 0).
    private_bufs: usize,
    /// Time after the last epoll_wait, and of the last buffer sweep.
    now: Instant,
    last_sweep: Instant,
}

impl Worker {
    fn me(&self) -> &'static IoThread {
        &self.g.threads[self.idx]
    }

    /// Precheck mode: the policy snapshot, pinned on first use in this poll
    /// iteration and released by unpin() before the thread sleeps.
    fn pin(&mut self) -> Option<*mut c_void> {
        let g = self.g;
        let begin = g.cbs.precheck_begin?;
        g.cbs.precheck_batch?;
        let p = *self.pinned.get_or_insert_with(|| begin(self.idx as c_int) as usize);
        Some(p as *mut c_void)
    }

    fn unpin(&mut self) {
        if self.pinned.take().is_some() {
            if let Some(end) = self.g.cbs.precheck_end {
                end(self.idx as c_int);
            }
        }
    }

    fn notify(&mut self, conn: &Arc<Conn>, ev: u32) {
        if ev != 0 {
            conn.events.fetch_or(ev, Ordering::AcqRel);
        }
        if conn.state.apply(Event::Notify) & FX_PUSH_READY != 0 {
            self.to_notify.push(conn.clone());
        }
    }

    /// Hand everything gathered in this poll iteration to the main thread
    /// with a single wakeup.
    fn flush_notify(&mut self) {
        if self.to_notify.is_empty() && self.done.is_empty() {
            return;
        }
        let (conns, done) = (&mut self.to_notify, &mut self.done);
        self.g.ready.post(self.g.hd(HD_MAIN_AWAKE), |r| {
            r.conns.append(conns);
            r.done.append(done);
        });
    }

    /// Run and complete the connection's write job, if any. Writes only
    /// when every copied byte ahead of it is on the socket and the peer is
    /// still there; otherwise the engine gets it back unwritten.
    fn run_job(&mut self, idx: usize) {
        let l = match self.slab[idx].as_mut() {
            Some(l) => l,
            None => return,
        };
        let job = l.conn.job.swap(0, Ordering::AcqRel);
        if job == 0 {
            return;
        }
        let can = !l.conn.state.has(PEER_CLOSED) && l.woff == l.wbuf.len();
        let r = (self.g.cbs.write_job.expect("write_job"))(job as *mut c_void, l.conn.fd, can as c_int);
        #[cfg(feature = "timing")]
        if l.woff == l.wbuf.len() {
            let t = l.conn.trace.swap(0, Ordering::Relaxed);
            if t != 0 {
                timing::complete(t);
            }
        }
        /* Would block, or copied bytes still wait for the socket: report
         * VIO_EV_WRITABLE only after EPOLLOUT. */
        if (can && r == 0) || (!can && !l.conn.state.has(PEER_CLOSED)) {
            l.sock_blocked = true;
        }
        self.done.push(job);
    }

    fn add_conn(&mut self, fd: RawFd) {
        let conn = Arc::new(Conn {
            g: self.g,
            fd,
            thread: self.idx,
            token: AtomicUsize::new(0),
            udata: AtomicPtr::new(std::ptr::null_mut()),
            state: conn_state::ConnState::new(),
            events: AtomicU32::new(0),
            want_writable: AtomicBool::new(false),
            read_paused: AtomicBool::new(false),
            auth_required: AtomicBool::new(self.g.default_auth_required.load(Ordering::Relaxed)),
            parse_held: AtomicBool::new(false),
            ncmds: AtomicUsize::new(0),
            priority: AtomicBool::new(false),
            detach: AtomicU8::new(DET_NONE),
            in_len: AtomicUsize::new(0),
            in_cap: AtomicUsize::new(0),
            out_bytes: AtomicUsize::new(0),
            job: AtomicUsize::new(0),
            shared: Mutex::new(Shared {
                cmds: VecDeque::new(),
                cmds_input: 0,
                cmds_argv: 0,
                err_sample: Vec::new(),
                err_input_len: 0,
                out: Vec::new(),
            }),
            session: precheck::SessionView::new(),
            #[cfg(feature = "timing")]
            trace: AtomicU32::new(0),
        });
        let idx = match self.free.pop() {
            Some(i) => i,
            None => {
                self.slab.push(None);
                self.slab.len() - 1
            }
        };
        conn.token.store(idx, Ordering::Relaxed);
        sys::epoll_add(self.me().epfd, fd, sys::EPOLLIN, idx as u64 + TOK_BASE);
        self.slab[idx] = Some(Local {
            conn: conn.clone(),
            rbuf: Vec::new(),
            rpos: 0,
            parser: Parser::new(),
            precheck: precheck::Local::default(),
            proto_err: false,
            wbuf: Vec::new(),
            woff: 0,
            sock_blocked: false,
            registered: true,
            interest: libc::EPOLLIN as u32,
            rbuf_shared: false,
            len_pub: 0,
            cap_pub: 0,
            peak: 0,
            last_read: self.now,
        });
        self.notify(&conn, VIO_EV_NEW);
    }

    fn remove(&mut self, idx: usize) {
        if let Some(l) = self.slab[idx].take() {
            self.g.rbuf_total.fetch_sub(l.cap_pub, Ordering::Relaxed);
            if l.cap_pub != 0 {
                self.private_bufs -= 1;
            }
            if l.registered {
                sys::epoll_del(self.me().epfd, l.conn.fd);
            }
            self.free.push(idx);
        }
    }

    fn update_interest(&mut self, idx: usize) {
        let epfd = self.me().epfd;
        let l = match self.slab[idx].as_mut() {
            Some(l) => l,
            None => return,
        };
        if !l.registered {
            return;
        }
        let c = &l.conn;
        let mut want = 0u32;
        if c.detach.load(Ordering::Acquire) == DET_DONE {
            /* Only watch for the peer going away. */
            want = sys::EPOLLRDHUP;
        } else if !c.read_paused.load(Ordering::Acquire) && !c.state.has(C_CLOSED) && !l.proto_err {
            want |= libc::EPOLLIN as u32;
        }
        if l.woff < l.wbuf.len() || l.sock_blocked {
            want |= libc::EPOLLOUT as u32;
        }
        if want != l.interest {
            sys::epoll_mod(epfd, c.fd, want, idx as u64 + TOK_BASE);
            l.interest = want;
        }
    }

    /// Peer went away (EOF or socket error). Stop polling but keep the fd
    /// open until the engine closes its side, so the fd number cannot be
    /// reused under an engine connection that still reports it.
    fn peer_gone(&mut self, idx: usize) {
        let epfd = self.me().epfd;
        let conn = {
            let l = self.slab[idx].as_mut().unwrap();
            if l.registered {
                sys::epoll_del(epfd, l.conn.fd);
                l.registered = false;
            }
            l.conn.clone()
        };
        let fx = conn.state.apply(Event::PeerClosed);
        self.run_job(idx);
        if fx & FX_REMOVE != 0 {
            self.remove(idx);
        } else if fx & FX_NOTIFY_CLOSED != 0 {
            self.notify(&conn, VIO_EV_CLOSED);
        }
    }

    fn on_readable(&mut self, idx: usize) {
        let now = self.now;
        if let Some(l) = self.slab[idx].as_mut() {
            l.last_read = now;
            if l.rbuf.capacity() == 0 {
                std::mem::swap(&mut l.rbuf, &mut self.shared_rbuf);
                l.rbuf_shared = true;
            }
        }
        self.read_and_parse(idx);
        self.settle_rbuf(idx);
    }

    /// After a read or a parse: return a lent shared buffer that holds no
    /// unparsed input (otherwise the connection keeps it, as C hands a
    /// client the shared query buffer), and publish what is held.
    fn settle_rbuf(&mut self, idx: usize) {
        let l = match self.slab[idx].as_mut() {
            Some(l) => l,
            None => return,
        };
        if l.rbuf_shared {
            l.rbuf_shared = false;
            if l.rpos == l.rbuf.len() {
                l.rbuf.clear();
                l.rpos = 0;
                if l.rbuf.capacity() <= 4 * READ_CHUNK {
                    std::mem::swap(&mut l.rbuf, &mut self.shared_rbuf);
                } else {
                    l.rbuf = Vec::new();
                }
            }
        }
        let len = (l.rbuf.len() - l.rpos).saturating_sub(l.parser.frame_consumed());
        if len != l.len_pub {
            l.len_pub = len;
            l.conn.in_len.store(len, Ordering::Relaxed);
        }
        let cap = l.rbuf.capacity();
        if cap != l.cap_pub {
            let grew = cap > l.cap_pub;
            if l.cap_pub == 0 {
                self.private_bufs += 1;
            } else if cap == 0 {
                self.private_bufs -= 1;
            }
            if grew {
                self.g.rbuf_total.fetch_add(cap - l.cap_pub, Ordering::Relaxed);
            } else {
                self.g.rbuf_total.fetch_sub(l.cap_pub - cap, Ordering::Relaxed);
            }
            l.cap_pub = cap;
            l.conn.in_cap.store(cap, Ordering::Relaxed);
            if grew {
                let c = l.conn.clone();
                self.notify(&c, VIO_EV_MEM);
            }
        }
    }

    /// Every SWEEP_EVERY: clientsCronResizeQueryBuffer() for the private read
    /// buffers. Free an idle connection's empty buffer, shrink an idle one
    /// holding a partial command to fit, and trim a buffer that is twice
    /// its recent peak.
    fn sweep(&mut self) {
        let now = self.now;
        self.last_sweep = now;
        for idx in 0..self.slab.len() {
            let l = match self.slab[idx].as_mut() {
                Some(l) => l,
                None => continue,
            };
            let cap = l.rbuf.capacity();
            if cap == 0 {
                l.peak = 0;
                continue;
            }
            let unparsed = l.rbuf.len() - l.rpos;
            let needed = l.parser.needed();
            if cap - l.rbuf.len() > 4096 {
                if now.duration_since(l.last_read) > Duration::from_secs(2) {
                    if unparsed == 0 {
                        l.rbuf = Vec::new();
                        l.rpos = 0;
                    } else {
                        l.rbuf.drain(..l.rpos);
                        l.rpos = 0;
                        l.rbuf.shrink_to_fit();
                    }
                } else if cap > 32 * 1024 && cap / 2 > l.peak {
                    l.rbuf.drain(..l.rpos);
                    l.rpos = 0;
                    let keep = l.rbuf.len().max(l.peak).max(needed);
                    l.rbuf.shrink_to(keep);
                }
            }
            l.peak = (l.rbuf.len() - l.rpos).max(needed);
            self.settle_rbuf(idx);
        }
    }

    fn read_and_parse(&mut self, idx: usize) {
        let g = self.g;
        let l = match self.slab[idx].as_mut() {
            Some(l) => l,
            None => return,
        };
        let conn = l.conn.clone();
        if conn.state.has(C_CLOSED) || l.proto_err {
            return;
        }

        /* Size the read for a large pending bulk so it lands in one buffer. */
        let have = l.rbuf.len() - l.rpos;
        let rem = l.parser.needed().saturating_sub(have);
        let want = if l.parser.big_bulk() && rem > 0 { rem } else { rem.max(READ_CHUNK) };
        l.peak = l.peak.max(have + want);
        let n = sys::read_append(conn.fd, &mut l.rbuf, want);
        if n == 0 {
            self.peer_gone(idx);
            return;
        }
        if n < 0 {
            let e = sys::errno();
            if e != libc::EAGAIN && e != libc::EINTR {
                self.peer_gone(idx);
            }
            return;
        }
        #[cfg(feature = "timing")]
        timing::on_read();

        /* client-query-buffer-limit, checked like readToQueryBuf(): on the
         * unparsed bytes, before parsing what this read added. */
        let auth_required = conn.auth_required.load(Ordering::Relaxed);
        let unparsed = l.rbuf.len() - l.rpos;
        let qb_limit = g.limits.max_querybuf_len();
        if unparsed > qb_limit || (auth_required && unparsed > 1024 * 1024) {
            l.proto_err = true;
            l.rbuf = Vec::new();
            l.rpos = 0;
            self.notify(&conn, VIO_EV_QBLIMIT);
            self.update_interest(idx);
            return;
        }

        if !conn.parse_held.load(Ordering::Acquire) {
            self.parse_buffered(idx);
        } else {
            self.update_interest(idx);
        }
    }

    /// Parse what is buffered and hand complete commands to the engine.
    /// While the connection is unauthenticated, stop after one command,
    /// like the C parser: AUTH/HELLO change the limits for what follows.
    /// The engine's next vio_conn_publish() resumes parsing.
    fn parse_buffered(&mut self, idx: usize) {
        let g = self.g;
        let snap = self.pin();
        let l = match self.slab[idx].as_mut() {
            Some(l) => l,
            None => return,
        };
        let conn = l.conn.clone();
        if conn.state.has(C_CLOSED) || l.proto_err {
            return;
        }
        let auth_required = conn.auth_required.load(Ordering::Acquire);
        let lim = Limits { max_bulk_len: g.limits.max_bulk_len(), auth_required };
        let mut batch: Vec<VioCmd> = Vec::new();
        /* Owned (inline) arguments stay alive until create_argvs. */
        let held = &mut self.held;
        let ptrs = &mut self.ptrs;
        let lens = &mut self.lens;
        held.clear();
        ptrs.clear();
        lens.clear();
        let mut ncmds = 0usize;
        let mut sample: Option<(Vec<u8>, usize)> = None;
        loop {
            let frame = &l.rbuf[l.rpos..];
            match l.parser.parse(frame, &lim) {
                Outcome::Command { args, consumed, input_bytes } => {
                    #[cfg(feature = "timing")]
                    let tr = timing::sample_parsed();
                    let mut sum = 0usize;
                    for a in &args {
                        let (p, len) = match a {
                            Arg::Range(o, len) => (frame[*o..].as_ptr(), *len),
                            Arg::Owned(v) => (v.as_ptr(), v.len()),
                        };
                        ptrs.push(p as usize);
                        lens.push(len);
                        sum += len;
                    }
                    #[allow(unused_mut)]
                    let mut cmd = VioCmd {
                        argv: std::ptr::null_mut(),
                        argc: args.len() as c_int,
                        err: 0,
                        err_byte: 0,
                        argv_len_sum: sum,
                        input_bytes,
                        cmd: std::ptr::null_mut(),
                        slot: -1,
                        read_flags: 0,
                        token: 0,
                        trace: 0,
                    };
                    #[cfg(feature = "timing")]
                    {
                        cmd.trace = tr;
                    }
                    batch.push(cmd);
                    held.push(args);
                    ncmds += 1;
                    l.rpos += consumed;
                    if auth_required && l.rpos < l.rbuf.len() {
                        conn.parse_held.store(true, Ordering::Release);
                        break;
                    }
                }
                Outcome::Empty { consumed } => l.rpos += consumed,
                Outcome::Incomplete => break,
                Outcome::Error(e, b) => {
                    /* C's qb_pos is past the parts already parsed. */
                    let at = &frame[l.parser.frame_consumed().min(frame.len())..];
                    sample = Some((err_sample(at), at.len()));
                    batch.push(VioCmd {
                        argv: std::ptr::null_mut(),
                        argc: 0,
                        err: e as c_int,
                        err_byte: b as c_int,
                        argv_len_sum: 0,
                        input_bytes: 0,
                        cmd: std::ptr::null_mut(),
                        slot: -1,
                        read_flags: 0,
                        token: 0,
                        trace: 0,
                    });
                    l.proto_err = true;
                    break;
                }
            }
        }
        if ncmds > 0 {
            (g.cbs.create_argvs)(ptrs.as_ptr() as *const *const u8, lens.as_ptr(), batch.as_mut_ptr(), batch.len());
            #[cfg(feature = "timing")]
            for c in batch.iter().filter(|c| c.trace != 0) {
                timing::stamp(c.trace, timing::ST_ARGV);
            }
            if let Some(snap) = snap {
                precheck::check_batch(&g.cbs, snap, &conn.session, &mut l.precheck, &mut batch, ncmds);
                #[cfg(feature = "timing")]
                for c in batch.iter().filter(|c| c.trace != 0) {
                    timing::stamp(c.trace, timing::ST_PRECHECK);
                }
            }
        }
        held.clear();
        if l.rpos == l.rbuf.len() {
            l.rbuf.clear();
            l.rpos = 0;
        } else if l.rpos > 0 && l.rpos >= l.rbuf.len() / 2 {
            l.rbuf.drain(..l.rpos);
            l.rpos = 0;
        }
        if !batch.is_empty() {
            let n = batch.len();
            let (input, argv) = batch.iter().fold((0, 0), |(i, a), c| (i + c.input_bytes as usize, a + cmd_argv_bytes(c)));
            let queued = {
                let mut sh = conn.shared.lock().unwrap();
                sh.cmds_input += input;
                sh.cmds_argv += argv;
                if let Some((v, n)) = sample.take() {
                    sh.err_sample = v;
                    sh.err_input_len = n;
                }
                #[cfg(feature = "timing")]
                for c in batch.iter().filter(|c| c.trace != 0) {
                    timing::stamp(c.trace, timing::ST_ENQUEUED);
                }
                sh.cmds.extend(batch);
                conn.ncmds.store(sh.cmds.len(), Ordering::Release);
                sh.cmds.len()
            };
            g.adm.queued_add(n);
            if !g.adm.may_read(queued) {
                conn.read_paused.store(true, Ordering::Release);
            }
            self.notify(&conn, 0);
        }
        self.update_interest(idx);
    }

    /// Write pending output. Swaps the engine-filled buffer into the
    /// thread-local one so the main thread never waits on a socket write.
    fn flush(&mut self, idx: usize) {
        let l = match self.slab[idx].as_mut() {
            Some(l) => l,
            None => return,
        };
        let conn = l.conn.clone();
        if conn.state.has(PEER_CLOSED) {
            self.run_job(idx);
            return;
        }
        if conn.detach.load(Ordering::Acquire) == DET_DONE {
            return;
        }
        let mut failed = false;
        #[cfg(feature = "timing")]
        let mut tr = 0u32;
        loop {
            if l.woff == l.wbuf.len() {
                l.wbuf.clear();
                l.woff = 0;
                let mut sh = conn.shared.lock().unwrap();
                std::mem::swap(&mut l.wbuf, &mut sh.out);
                #[cfg(feature = "timing")]
                {
                    let t = conn.trace.swap(0, Ordering::Relaxed);
                    if t != 0 {
                        timing::complete(tr);
                        tr = t;
                    }
                }
                drop(sh);
                if l.wbuf.is_empty() {
                    break;
                }
            }
            let n = sys::write(conn.fd, &l.wbuf[l.woff..]);
            if n > 0 {
                l.woff += n as usize;
                conn.out_bytes.fetch_sub(n as usize, Ordering::AcqRel);
            } else {
                let e = sys::errno();
                if n < 0 && (e == libc::EAGAIN || e == libc::EINTR) {
                    break;
                }
                failed = true;
                break;
            }
        }
        #[cfg(feature = "timing")]
        timing::complete(tr);
        if l.wbuf.capacity() > 4 * OUT_HIGH && l.woff == l.wbuf.len() {
            l.wbuf = Vec::new();
            l.woff = 0;
        }
        if failed {
            self.peer_gone(idx);
            return;
        }
        self.run_job(idx);
        let blocked = self.slab[idx].as_ref().is_some_and(|l| l.sock_blocked);
        /* Level triggered, like a socket write handler: keep firing after
         * each flush while the engine keeps its handler installed. */
        if !blocked
            && conn.want_writable.load(Ordering::Acquire)
            && conn.out_bytes.load(Ordering::Acquire) < OUT_LOW
        {
            self.notify(&conn, VIO_EV_WRITABLE);
        }
        self.update_interest(idx);
    }

    fn on_kick(&mut self, conn: Arc<Conn>) {
        conn.state.apply(Event::KickTaken);
        let idx = conn.token.load(Ordering::Relaxed);
        match self.slab.get(idx) {
            Some(Some(l)) if Arc::ptr_eq(&l.conn, &conn) => {}
            _ => {
                /* Removed already: hand a job back unwritten. */
                let job = conn.job.swap(0, Ordering::AcqRel);
                if job != 0 {
                    (self.g.cbs.write_job.expect("write_job"))(job as *mut c_void, conn.fd, 0);
                    self.done.push(job);
                }
                return;
            }
        }
        if conn.state.get() & (C_CLOSED | PEER_CLOSED) == C_CLOSED | PEER_CLOSED {
            self.remove(idx);
            return;
        }
        if conn.detach.load(Ordering::Acquire) == DET_DONE {
            /* Nothing pending here (the detach flushed it), so a close or a
             * shutdown takes effect at once. */
            self.after_flush(idx);
            return;
        }
        if conn.read_paused.load(Ordering::Acquire) && self.g.adm.may_resume(conn.ncmds.load(Ordering::Acquire)) {
            conn.read_paused.store(false, Ordering::Release);
        }
        self.flush(idx);
        if self.after_flush(idx) {
            return;
        }
        if conn.parse_held.swap(false, Ordering::AcqRel) {
            self.parse_buffered(idx);
            self.settle_rbuf(idx);
        }
        self.update_interest(idx);
    }

    /// After a flush: the deferred shutdown(2) once everything the engine
    /// handed over before vio_conn_shutdown() is on the socket, and the
    /// release of an engine-closed connection once its output drained or
    /// its peer left. True when the connection was released.
    fn after_flush(&mut self, idx: usize) -> bool {
        let l = match self.slab[idx].as_ref() {
            Some(l) => l,
            None => return true,
        };
        let c = &l.conn;
        if c.state.get() & (HALF_CLOSE | C_CLOSED) == 0 {
            return false;
        }
        let out_drained = l.woff == l.wbuf.len() && c.out_bytes.load(Ordering::Acquire) == 0;
        let job_idle = c.job.load(Ordering::Acquire) == 0;
        let fx = c.state.apply(Event::Flushed { out_drained, job_idle });
        if fx & FX_SHUTDOWN != 0 {
            sys::shutdown(c.fd);
        }
        if fx & FX_REMOVE != 0 {
            self.remove(idx);
            return true;
        }
        false
    }

    fn on_event(&mut self, idx: usize, mask: u32) {
        if let Some(l) = self.slab[idx].as_ref() {
            if l.conn.detach.load(Ordering::Acquire) == DET_DONE {
                if mask & (sys::EPOLLRDHUP | sys::EPOLLHUP | sys::EPOLLERR) != 0 {
                    self.peer_gone(idx);
                }
                return;
            }
        }
        if mask & (libc::EPOLLOUT as u32) != 0 {
            if let Some(l) = self.slab[idx].as_mut() {
                l.sock_blocked = false;
            }
            self.flush(idx);
            if self.after_flush(idx) {
                return;
            }
        }
        if mask & ((libc::EPOLLIN | libc::EPOLLHUP | libc::EPOLLERR) as u32) != 0 && self.slab[idx].is_some() {
            let l = self.slab[idx].as_ref().unwrap();
            if l.interest & (libc::EPOLLIN as u32) != 0 {
                self.on_readable(idx);
            } else if mask & ((libc::EPOLLHUP | libc::EPOLLERR) as u32) != 0 {
                self.peer_gone(idx);
            }
        }
    }

    /// Msg::Detach: put everything accepted so far on the socket (the fd
    /// is still non-blocking), then leave it to the engine. Reading stops
    /// here too, so input not yet read stays in the socket for the engine.
    fn on_detach(&mut self, conn: Arc<Conn>) {
        let idx = conn.token.load(Ordering::Relaxed);
        let live = matches!(self.slab.get(idx), Some(Some(l)) if Arc::ptr_eq(&l.conn, &conn));
        if !live {
            conn.detach.store(DET_DONE, Ordering::Release);
            return;
        }
        let deadline = Instant::now() + Duration::from_millis(DETACH_FLUSH_MS);
        let mut ok = true;
        loop {
            self.flush(idx);
            let l = match self.slab[idx].as_ref() {
                Some(l) => l,
                None => break,
            };
            let pending = l.woff < l.wbuf.len() || conn.out_bytes.load(Ordering::Acquire) > 0;
            if !pending || conn.state.has(PEER_CLOSED) {
                break;
            }
            let now = Instant::now();
            if now >= deadline {
                ok = false;
                break;
            }
            sys::poll_writable(conn.fd, (deadline - now).as_millis().max(1) as c_int);
        }
        conn.detach.store(if ok { DET_DONE } else { DET_FAILED }, Ordering::Release);
        if ok {
            self.update_interest(idx);
        }
    }

    /// Msg::Attach: the engine is done with the fd (and made it
    /// non-blocking again). Resume reading, and parse what was buffered.
    fn on_attach(&mut self, conn: Arc<Conn>) {
        conn.detach.store(DET_NONE, Ordering::Release);
        let idx = conn.token.load(Ordering::Relaxed);
        if !matches!(self.slab.get(idx), Some(Some(l)) if Arc::ptr_eq(&l.conn, &conn)) {
            return;
        }
        self.update_interest(idx);
        self.flush(idx);
        if !conn.parse_held.load(Ordering::Acquire) {
            self.parse_buffered(idx);
            self.settle_rbuf(idx);
        }
    }

    /// Thread 0 only: add or remove the listener from the epoll set.
    fn listen_interest(&self, on: bool) {
        sys::epoll_mod(self.me().epfd, self.g.listen_fd, if on { sys::EPOLLIN } else { 0 }, TOK_LISTEN);
    }

    /// Stop polling the listener while the engine is NEW_HIGH connections
    /// behind. Returns true when paused.
    fn accept_backoff(&self) -> bool {
        let adm = &self.g.adm;
        if !adm.should_pause() {
            return false;
        }
        adm.set_paused();
        self.listen_interest(false);
        if adm.undo_pause() {
            self.listen_interest(true);
            return false;
        }
        true
    }

    fn accept_all(&mut self) {
        let g = self.g;
        loop {
            if self.accept_backoff() {
                return;
            }
            let fd = match sys::accept_nodelay(g.listen_fd) {
                Some(fd) => fd,
                None => return,
            };
            if !g.adm.admit() {
                /* What acceptCommonHandler() does at maxclients: best-effort
                 * error, then close. */
                sys::write(fd, admission::MAXCLIENTS_ERR);
                sys::close(fd);
                continue;
            }
            let target = self.rr % g.threads.len();
            self.rr = self.rr.wrapping_add(1);
            if target == self.idx {
                self.add_conn(fd);
            } else {
                g.threads[target].post(g, |q| q.push(Msg::NewFd(fd)));
            }
        }
    }

    fn take_inbox(&mut self) {
        let me = self.me();
        let mut msgs = std::mem::take(&mut self.inbox_buf);
        if me.inbox.swap(&mut msgs) {
            for m in msgs.drain(..) {
                match m {
                    Msg::NewFd(fd) => self.add_conn(fd),
                    Msg::Kick(c) => self.on_kick(c),
                    Msg::ResumeAccept => {
                        self.listen_interest(true);
                        self.accept_all();
                    }
                    Msg::Detach(c) => self.on_detach(c),
                    Msg::Attach(c) => self.on_attach(c),
                    Msg::FreeArgv(argv, argc) => (self.g.cbs.free_argv)(argv as *mut *mut c_void, argc),
                }
            }
        }
        self.inbox_buf = msgs;
    }

    fn run(mut self) {
        let me = self.me();
        let mut timeout: c_int = -1;
        let mut events: Vec<sys::EpollEvent> = vec![sys::EpollEvent { events: 0, u64: 0 }; 256];
        loop {
            if timeout != 0 && me.inbox.waker.prepare_sleep() {
                timeout = 0;
            }
            let n = sys::epoll_wait(me.epfd, &mut events, timeout);
            me.inbox.waker.awake();
            self.now = Instant::now();
            for ev in events.iter().take(n) {
                let tok = ev.u64;
                let mask = ev.events;
                if tok == TOK_WAKE {
                    /* Drain, then take (handoff.rs). */
                    me.inbox.waker.drain();
                    self.take_inbox();
                } else if tok == TOK_LISTEN {
                    self.accept_all();
                } else {
                    self.on_event((tok - TOK_BASE) as usize, mask);
                }
            }
            /* Kicks pushed while this thread was awake, so no eventfd was
             * written for them. When one was, leave them to its TOK_WAKE:
             * taking them here too, ahead of the notification, tripled the
             * IO thread's sleeps and cost 18% at -P 16 with one IO thread. */
            if !me.inbox.waker.signaled() {
                self.take_inbox();
            }
            if self.now.duration_since(self.last_sweep) >= SWEEP_EVERY {
                self.sweep();
            }
            /* Wake up for the sweep while private read buffers exist. */
            timeout = if self.private_bufs > 0 { SWEEP_EVERY.as_millis() as c_int } else { -1 };
            self.unpin();
            self.flush_notify();
        }
    }
}

/* ---------------------------------------------------------------------------
 * Startup
 * ------------------------------------------------------------------------- */

/// Bind the listener and start the IO threads (vio_start). Returns the
/// runtime and the engine's eventfd, or an errno value. Call on the engine
/// thread: it becomes the runtime's main thread.
fn start(bind: Option<String>, port: c_int, threads: c_int, cbs: VioCallbacks, limits: EngineLimits) -> Result<(&'static Runtime, RawFd), i32> {
    let addr = match bind {
        None => "0.0.0.0".to_string(),
        Some(s) => {
            let s = s.trim_start_matches('-').to_string();
            if s == "*" { "0.0.0.0".to_string() } else if s == "::*" { "::".to_string() } else { s }
        }
    };
    let listener = match std::net::TcpListener::bind((addr.as_str(), port as u16)) {
        Ok(l) => l,
        Err(e) => return Err(e.raw_os_error().unwrap_or(libc::EINVAL)),
    };
    if let Err(e) = listener.set_nonblocking(true) {
        return Err(e.raw_os_error().unwrap_or(libc::EINVAL));
    }
    let bound_port = listener.local_addr().map(|a| a.port()).unwrap_or(0);
    let listen_fd = listener.into_raw_fd();
    let nthreads = threads.max(1) as usize;
    let mut threads = Vec::with_capacity(nthreads);
    for _ in 0..nthreads {
        let epfd = sys::epoll_create();
        let wakefd = sys::eventfd();
        if epfd < 0 || wakefd < 0 {
            return Err(sys::errno());
        }
        sys::epoll_add(epfd, wakefd, sys::EPOLLIN, TOK_WAKE);
        threads.push(IoThread { epfd, inbox: handoff::Mailbox::new(wakefd, false) });
    }
    sys::epoll_add(threads[0].epfd, listen_fd, sys::EPOLLIN, TOK_LISTEN);
    let main_efd = sys::eventfd();
    if main_efd < 0 {
        return Err(sys::errno());
    }
    let handoff_disable = std::env::var("VIO_HANDOFF_DISABLE").ok().and_then(|v| v.parse().ok()).unwrap_or(HD_IO_AWAKE);
    let g: &'static Global = Box::leak(Box::new(Global {
        cbs,
        limits,
        threads,
        ready: handoff::Mailbox::new(main_efd, true),
        listen_fd,
        port: bound_port,
        adm: admission::Admission::new(),
        default_auth_required: AtomicBool::new(false),
        handoff_disable,
        rbuf_total: AtomicUsize::new(0),
        prio_conns: AtomicUsize::new(0),
    }));
    let rt: &'static Runtime = Box::leak(Box::new(Runtime { g, main: RefCell::new(MainState::default()), main_thread: sys::thread_self() }));
    for idx in 0..nthreads {
        let w = Worker {
            g,
            idx,
            slab: Vec::new(),
            free: Vec::new(),
            to_notify: Vec::new(),
            done: Vec::new(),
            rr: 0,
            pinned: None,
            ptrs: Vec::new(),
            lens: Vec::new(),
            held: Vec::new(),
            inbox_buf: Vec::new(),
            shared_rbuf: Vec::new(),
            private_bufs: 0,
            now: Instant::now(),
            last_sweep: Instant::now(),
        };
        std::thread::Builder::new()
            .name(format!("rust-io-{}", idx))
            .spawn(move || w.run())
            .expect("spawn rust-io thread");
    }
    Ok((rt, main_efd))
}

/* ---------------------------------------------------------------------------
 * Main thread state (exports in ffi/exports.rs)
 * ------------------------------------------------------------------------- */



/// Take connections that became ready while the main thread was awake (no
/// eventfd was written for them). Returns 1 when there was pending work.
fn take_ready(rt: &Runtime) -> c_int {
    let mut m = rt.main.borrow_mut();
    let m = &mut *m;
    if !rt.g.ready.swap(&mut m.swap) {
        return 0;
    }
    if rt.g.prio_conns.load(Ordering::Relaxed) == 0 {
        m.ready.extend(m.swap.conns.drain(..));
    } else {
        /* High-priority connections first, in arrival order (the QoS event
         * loop drains its priority set before the normal one). */
        let mut prio = Vec::new();
        for c in m.swap.conns.drain(..) {
            if c.priority.load(Ordering::Relaxed) {
                prio.push(c);
            } else {
                m.ready.push_back(c);
            }
        }
        for c in prio.into_iter().rev() {
            m.ready.push_front(c);
        }
    }
    m.done.extend(m.swap.done.drain(..));
    1
}























#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    extern "C" fn t_create(_p: *const *const u8, _l: *const usize, _c: *mut VioCmd, _n: usize) {}
    extern "C" fn t_free(_a: *mut *mut c_void, _c: c_int) {}

    fn cbs() -> VioCallbacks {
        VioCallbacks { create_argvs: t_create, free_argv: t_free, write_job: None, precheck_begin: None, precheck_end: None, precheck_batch: None }
    }

    /// Wait until `rt` has a ready connection; false after 5 s.
    fn wait_ready(rt: &Runtime) -> bool {
        let t = Instant::now();
        while t.elapsed() < Duration::from_secs(5) {
            take_ready(rt);
            if !rt.main.borrow().ready.is_empty() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        false
    }

    /// Two runtimes in one process: each has its own listener, IO threads,
    /// ready queue and engine-thread state, and a connection to one shows
    /// up only in that one.
    #[test]
    #[cfg_attr(miri, ignore)] // sockets and epoll
    fn two_runtimes() {
        let (a, _) = start(Some("127.0.0.1".into()), 0, 2, cbs(), EngineLimits::new(512 << 20, 1 << 30)).unwrap();
        let (b, _) = start(Some("127.0.0.1".into()), 0, 1, cbs(), EngineLimits::new(512 << 20, 1 << 30)).unwrap();
        assert!(!std::ptr::eq(a.g, b.g));
        assert_ne!(a.g.port, b.g.port);
        assert_eq!((a.g.threads.len(), b.g.threads.len()), (2, 1));
        a.g.default_auth_required.store(true, Ordering::Relaxed);

        let _ca = std::net::TcpStream::connect(("127.0.0.1", a.g.port)).unwrap();
        assert!(wait_ready(a));
        take_ready(b);
        assert!(b.main.borrow().ready.is_empty());
        let conn = a.main.borrow_mut().ready.pop_front().unwrap();
        assert!(std::ptr::eq(conn.g, a.g));
        assert!(conn.auth_required.load(Ordering::Relaxed));

        let _cb = std::net::TcpStream::connect(("127.0.0.1", b.g.port)).unwrap();
        assert!(wait_ready(b));
        let conn = b.main.borrow_mut().ready.pop_front().unwrap();
        assert!(std::ptr::eq(conn.g, b.g));
        assert!(!conn.auth_required.load(Ordering::Relaxed));
        take_ready(a);
        assert!(a.main.borrow().ready.is_empty());

        /* Engine-thread state is per runtime: the kick batches in b's. */
        kick(b, &conn);
        assert_eq!(b.main.borrow().kicks.iter().map(|k| k.len()).sum::<usize>(), 1);
        assert_eq!(a.main.borrow().kicks.iter().map(|k| k.len()).sum::<usize>(), 0);
    }
}
