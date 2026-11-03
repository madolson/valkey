//! valkey-io: a Rust IO layer for valkey-server.
//!
//! Rust owns the listening socket, the client sockets, reads, RESP parsing and
//! writes. The engine (C) sees one `VioConn` handle per connection and pulls
//! parsed commands from it. See DESIGN.md for the threading and ownership
//! model and the full FFI surface; the C declarations live in
//! include/valkey_io.h.

#![deny(unsafe_code, unsafe_op_in_unsafe_fn)]

mod admission;
mod ffi;
mod handoff;
pub mod precheck;
pub mod sys;
pub mod resp;
#[cfg(feature = "timing")]
pub mod timing;
#[cfg(feature = "bench")]
pub mod benchapi;

use resp::{Arg, Limits, Outcome, Parser};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::ffi::{c_char, c_int, c_void};
use std::os::unix::io::{IntoRawFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
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
    pub argv_len_sum: usize,
    pub input_bytes: u64,
    /// Precheck mode: struct serverCommand *, slot, read flags and token
    /// filled on the IO thread; token 0 when not prechecked.
    pub cmd: *mut c_void,
    pub slot: c_int,
    pub read_flags: c_int,
    pub token: u64,
    /// Sampled stage-timing id, 0 when not sampled.
    #[cfg(feature = "timing")]
    pub trace: u32,
}

pub const VIO_EV_NEW: u32 = 1 << 0;
pub const VIO_EV_CLOSED: u32 = 1 << 1;
pub const VIO_EV_WRITABLE: u32 = 1 << 2;
pub const VIO_EV_QBLIMIT: u32 = 1 << 3;

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
/// How long the IO thread waits for the socket to take the output pending
/// at detach time.
const DETACH_FLUSH_MS: u64 = 5000;

/* ---------------------------------------------------------------------------
 * Connection shared between one IO thread and the main thread
 * ------------------------------------------------------------------------- */

struct Shared {
    cmds: VecDeque<VioCmd>,
    out: Vec<u8>,
}


pub struct Conn {
    fd: RawFd,
    thread: usize,
    token: AtomicUsize,
    /// Engine connection pointer. Main thread only.
    udata: AtomicPtr<c_void>,
    in_ready: AtomicBool,
    kicked: AtomicBool,
    events: AtomicU32,
    c_closed: AtomicBool,
    peer_closed: AtomicBool,
    want_writable: AtomicBool,
    read_paused: AtomicBool,
    auth_required: AtomicBool,
    /// Unparsed input held until the engine republishes auth_required.
    parse_held: AtomicBool,
    /// The engine asked for shutdown(2) (a fork child shares the fd): done
    /// by the IO thread once the output accepted before it is written.
    half_close: AtomicBool,
    /// Bytes accepted from the engine and not yet written to the socket.
    out_bytes: AtomicUsize,
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
        let g = global();
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
    /// Executed argv the engine returned, to free on the thread that built
    /// them: (argv, argc) pairs.
    FreeArgv(Vec<(usize, c_int)>),
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
    fn post(&self, f: impl FnOnce(&mut Vec<Msg>)) {
        self.inbox.post(hd(HD_IO_AWAKE), f)
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
    adm: admission::Admission,
}


static GLOBAL: OnceLock<Global> = OnceLock::new();

/// authRequired() of a client that has not authenticated yet, published by
/// the engine. New connections start with it, so their first read is parsed
/// with the right limits before the engine adopts them.
static DEFAULT_AUTH_REQUIRED: AtomicBool = AtomicBool::new(false);

/// Measurement switch, read once from VIO_HANDOFF_DISABLE at vio_start():
/// bit 1 = no main-thread kick batching, bit 2 = always write the IO-thread
/// eventfd, bit 4 = always write the main eventfd (the stage 1 handoff is 7).
/// Default 2: skipping the eventfd for an awake IO thread lets it drain its
/// kicks early, sleep, and be woken again; with one IO thread at -P 16 that
/// cost 16% throughput (DESIGN.md "Handoff v2").
static HANDOFF_DISABLE: AtomicU32 = AtomicU32::new(HD_IO_AWAKE);
const HD_KICK_BATCH: u32 = 1;
const HD_IO_AWAKE: u32 = 2;
const HD_MAIN_AWAKE: u32 = 4;

fn hd(bit: u32) -> bool {
    HANDOFF_DISABLE.load(Ordering::Relaxed) & bit != 0
}

fn global() -> &'static Global {
    GLOBAL.get().expect("valkey-io not started")
}

/// Ask the owning IO thread to re-evaluate a connection (flush output,
/// resume reading, finish a close). Coalesced: one inbox entry per conn
/// until the IO thread picks it up, one eventfd write per wakeup.
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
    global().threads[conn.thread].post(|q| q.push(Msg::Detach(conn.clone())));
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
    global().threads[conn.thread].post(|q| q.push(Msg::Attach(conn.clone())));
}

fn kick(conn: &Arc<Conn>) {
    if conn.kicked.swap(true, Ordering::AcqRel) {
        return;
    }
    /* Main thread: collect, and hand each IO thread its kicks in one inbox
     * append and at most one eventfd write at the end of beforeSleep()
     * (vio_main_flush_kicks). */
    if IS_MAIN.with(|m| m.get()) && !hd(HD_KICK_BATCH) {
        MAIN_KICKS.with(|k| {
            let mut k = k.borrow_mut();
            if k.len() <= conn.thread {
                k.resize_with(conn.thread + 1, Vec::new);
            }
            let list = &mut k[conn.thread];
            list.push(conn.clone());
            /* Large batches: hand over early so the IO thread writes while
             * the main thread is still producing replies. */
            if list.len() >= KICK_BATCH {
                flush_kicks(conn.thread, list);
            }
        });
        return;
    }
    global().threads[conn.thread].post(|q| q.push(Msg::Kick(conn.clone())));
}


/// Main-thread kicks per IO thread handed over before the end of the
/// iteration.
const KICK_BATCH: usize = 32;

fn flush_kicks(idx: usize, list: &mut Vec<Arc<Conn>>) {
    global().threads[idx].post(|q| q.extend(list.drain(..).map(Msg::Kick)));
}

/// Argv returned per IO thread handed over before the end of the iteration.
const ARGV_BATCH: usize = 256;

/// Main thread: queue an executed argv for the IO thread that built it.
/// Handed over with the kicks (vio_main_flush_kicks), or early when the
/// batch is large.
fn return_argv(thread: usize, argv: usize, argc: c_int) {
    MAIN_ARGV.with(|a| {
        let mut a = a.borrow_mut();
        if a.len() <= thread {
            a.resize_with(thread + 1, Vec::new);
        }
        a[thread].push((argv, argc));
        if a[thread].len() >= ARGV_BATCH {
            let v = std::mem::take(&mut a[thread]);
            global().threads[thread].post(|q| q.push(Msg::FreeArgv(v)));
        }
    });
}

/// Main thread: hand IO thread `idx` its kicks and returned argv with one
/// inbox lock and at most one wakeup.
fn flush_thread(idx: usize, kicks: &mut Vec<Arc<Conn>>, argv: Option<&mut Vec<(usize, c_int)>>) {
    let argv = argv.filter(|a| !a.is_empty()).map(std::mem::take);
    if kicks.is_empty() && argv.is_none() {
        return;
    }
    global().threads[idx].post(|q| {
        q.extend(kicks.drain(..).map(Msg::Kick));
        if let Some(v) = argv {
            q.push(Msg::FreeArgv(v));
        }
    });
}

/* ---------------------------------------------------------------------------
 * IO thread
 * ------------------------------------------------------------------------- */

thread_local! {
    /// True on the thread that called vio_start() (the engine main thread).
    static IS_MAIN: Cell<bool> = const { Cell::new(false) };
    /// Main thread: kicks per IO thread, flushed by vio_main_flush_kicks().
    static MAIN_KICKS: RefCell<Vec<Vec<Arc<Conn>>>> = const { RefCell::new(Vec::new()) };
    /// Main thread: returned argv per IO thread, flushed with the kicks.
    static MAIN_ARGV: RefCell<Vec<Vec<(usize, c_int)>>> = const { RefCell::new(Vec::new()) };
}

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
}

struct Worker {
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
}

impl Worker {
    fn me(&self) -> &'static IoThread {
        &global().threads[self.idx]
    }

    /// Precheck mode: the policy snapshot, pinned on first use in this poll
    /// iteration and released by unpin() before the thread sleeps.
    fn pin(&mut self) -> Option<*mut c_void> {
        let g = global();
        let begin = g.cbs.precheck_begin?;
        g.cbs.precheck_batch?;
        let p = *self.pinned.get_or_insert_with(|| begin(self.idx as c_int) as usize);
        Some(p as *mut c_void)
    }

    fn unpin(&mut self) {
        if self.pinned.take().is_some() {
            if let Some(end) = global().cbs.precheck_end {
                end(self.idx as c_int);
            }
        }
    }

    fn notify(&mut self, conn: &Arc<Conn>, ev: u32) {
        if ev != 0 {
            conn.events.fetch_or(ev, Ordering::AcqRel);
        }
        if !conn.in_ready.swap(true, Ordering::AcqRel) {
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
        global().ready.post(hd(HD_MAIN_AWAKE), |r| {
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
        let can = !l.conn.peer_closed.load(Ordering::Acquire) && l.woff == l.wbuf.len();
        let r = (global().cbs.write_job.expect("write_job"))(job as *mut c_void, l.conn.fd, can as c_int);
        #[cfg(feature = "timing")]
        if l.woff == l.wbuf.len() {
            let t = l.conn.trace.swap(0, Ordering::Relaxed);
            if t != 0 {
                timing::complete(t);
            }
        }
        /* Would block, or copied bytes still wait for the socket: report
         * VIO_EV_WRITABLE only after EPOLLOUT. */
        if (can && r == 0) || (!can && !l.conn.peer_closed.load(Ordering::Acquire)) {
            l.sock_blocked = true;
        }
        self.done.push(job);
    }

    fn add_conn(&mut self, fd: RawFd) {
        let conn = Arc::new(Conn {
            fd,
            thread: self.idx,
            token: AtomicUsize::new(0),
            udata: AtomicPtr::new(std::ptr::null_mut()),
            in_ready: AtomicBool::new(false),
            kicked: AtomicBool::new(false),
            events: AtomicU32::new(0),
            c_closed: AtomicBool::new(false),
            peer_closed: AtomicBool::new(false),
            want_writable: AtomicBool::new(false),
            read_paused: AtomicBool::new(false),
            auth_required: AtomicBool::new(DEFAULT_AUTH_REQUIRED.load(Ordering::Relaxed)),
            parse_held: AtomicBool::new(false),
            half_close: AtomicBool::new(false),
            detach: AtomicU8::new(DET_NONE),
            out_bytes: AtomicUsize::new(0),
            job: AtomicUsize::new(0),
            shared: Mutex::new(Shared { cmds: VecDeque::new(), out: Vec::new() }),
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
        });
        self.notify(&conn, VIO_EV_NEW);
    }

    fn remove(&mut self, idx: usize) {
        if let Some(l) = self.slab[idx].take() {
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
        } else if !c.read_paused.load(Ordering::Acquire) && !c.c_closed.load(Ordering::Acquire) && !l.proto_err {
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
        conn.peer_closed.store(true, Ordering::Release);
        self.run_job(idx);
        if conn.c_closed.load(Ordering::Acquire) {
            self.remove(idx);
        } else {
            self.notify(&conn, VIO_EV_CLOSED);
        }
    }

    fn on_readable(&mut self, idx: usize) {
        let g = global();
        let l = match self.slab[idx].as_mut() {
            Some(l) => l,
            None => return,
        };
        let conn = l.conn.clone();
        if conn.c_closed.load(Ordering::Acquire) || l.proto_err {
            return;
        }

        /* Size the read for a large pending bulk so it lands in one buffer. */
        let have = l.rbuf.len() - l.rpos;
        let want = l.parser.needed().saturating_sub(have).max(READ_CHUNK);
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
        let g = global();
        let snap = self.pin();
        let l = match self.slab[idx].as_mut() {
            Some(l) => l,
            None => return,
        };
        let conn = l.conn.clone();
        if conn.c_closed.load(Ordering::Acquire) || l.proto_err {
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
                        #[cfg(feature = "timing")]
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
                        #[cfg(feature = "timing")]
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
        if l.rbuf.is_empty() && l.rbuf.capacity() > 4 * READ_CHUNK {
            l.rbuf = Vec::new();
        }

        if !batch.is_empty() {
            let n = batch.len();
            let queued = {
                let mut sh = conn.shared.lock().unwrap();
                #[cfg(feature = "timing")]
                for c in batch.iter().filter(|c| c.trace != 0) {
                    timing::stamp(c.trace, timing::ST_ENQUEUED);
                }
                sh.cmds.extend(batch);
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
        if conn.peer_closed.load(Ordering::Acquire) {
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
        conn.kicked.store(false, Ordering::Release);
        let idx = conn.token.load(Ordering::Relaxed);
        match self.slab.get(idx) {
            Some(Some(l)) if Arc::ptr_eq(&l.conn, &conn) => {}
            _ => {
                /* Removed already: hand a job back unwritten. */
                let job = conn.job.swap(0, Ordering::AcqRel);
                if job != 0 {
                    (global().cbs.write_job.expect("write_job"))(job as *mut c_void, conn.fd, 0);
                    self.done.push(job);
                }
                return;
            }
        }
        if conn.c_closed.load(Ordering::Acquire) && conn.peer_closed.load(Ordering::Acquire) {
            self.remove(idx);
            return;
        }
        if conn.detach.load(Ordering::Acquire) == DET_DONE {
            /* Nothing pending here (the detach flushed it), so a close or a
             * shutdown takes effect at once. */
            self.maybe_half_close(idx);
            if conn.c_closed.load(Ordering::Acquire) {
                self.remove(idx);
            }
            return;
        }
        if conn.read_paused.load(Ordering::Acquire) && global().adm.may_resume(conn.shared.lock().unwrap().cmds.len()) {
            conn.read_paused.store(false, Ordering::Release);
        }
        self.flush(idx);
        self.maybe_half_close(idx);
        if conn.parse_held.swap(false, Ordering::AcqRel) {
            self.parse_buffered(idx);
        }
        if conn.c_closed.load(Ordering::Acquire) {
            let drained = match self.slab[idx].as_ref() {
                Some(l) => l.woff == l.wbuf.len() && conn.out_bytes.load(Ordering::Acquire) == 0,
                None => return,
            };
            if drained || conn.peer_closed.load(Ordering::Acquire) {
                self.remove(idx);
                return;
            }
        }
        self.update_interest(idx);
    }

    /// shutdown(2) once everything the engine handed over before
    /// vio_conn_shutdown() is on the socket (or the peer is gone).
    fn maybe_half_close(&mut self, idx: usize) {
        let l = match self.slab[idx].as_ref() {
            Some(l) => l,
            None => return,
        };
        let c = &l.conn;
        if !c.half_close.load(Ordering::Acquire) {
            return;
        }
        let drained = l.woff == l.wbuf.len() && c.out_bytes.load(Ordering::Acquire) == 0 && c.job.load(Ordering::Acquire) == 0;
        if drained || c.peer_closed.load(Ordering::Acquire) {
            c.half_close.store(false, Ordering::Release);
            sys::shutdown(c.fd);
        }
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
            self.maybe_half_close(idx);
            /* A closing connection is released once its output drains. */
            if let Some(l) = self.slab[idx].as_ref() {
                let c = l.conn.clone();
                if c.c_closed.load(Ordering::Acquire)
                    && l.woff == l.wbuf.len()
                    && c.out_bytes.load(Ordering::Acquire) == 0
                {
                    self.remove(idx);
                    return;
                }
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
            if !pending || conn.peer_closed.load(Ordering::Acquire) {
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
        }
    }

    /// Thread 0 only: add or remove the listener from the epoll set.
    fn listen_interest(&self, on: bool) {
        sys::epoll_mod(self.me().epfd, global().listen_fd, if on { sys::EPOLLIN } else { 0 }, TOK_LISTEN);
    }

    /// Stop polling the listener while the engine is NEW_HIGH connections
    /// behind. Returns true when paused.
    fn accept_backoff(&self) -> bool {
        let adm = &global().adm;
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
        let g = global();
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
                g.threads[target].post(|q| q.push(Msg::NewFd(fd)));
            }
        }
    }

    fn take_inbox(&mut self) {
        let me = self.me();
        if let Some(msgs) = me.inbox.take() {
            for m in msgs {
                match m {
                    Msg::NewFd(fd) => self.add_conn(fd),
                    Msg::Kick(c) => self.on_kick(c),
                    Msg::ResumeAccept => {
                        self.listen_interest(true);
                        self.accept_all();
                    }
                    Msg::Detach(c) => self.on_detach(c),
                    Msg::Attach(c) => self.on_attach(c),
                    Msg::FreeArgv(v) => {
                        let free = global().cbs.free_argv;
                        for (argv, argc) in v {
                            free(argv as *mut *mut c_void, argc);
                        }
                    }
                }
            }
        }
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
            timeout = -1;
            self.unpin();
            self.flush_notify();
        }
    }
}

/* ---------------------------------------------------------------------------
 * Startup
 * ------------------------------------------------------------------------- */

/// Bind the listener and start the IO threads (vio_start). Returns the
/// engine's eventfd, or an errno value.
fn start(bind: Option<String>, port: c_int, threads: c_int, cbs: VioCallbacks, limits: EngineLimits) -> Result<RawFd, i32> {
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
    let g = Global {
        cbs,
        limits,
        threads,
        ready: handoff::Mailbox::new(main_efd, true),
        listen_fd,
        adm: admission::Admission::new(),
    };
    if GLOBAL.set(g).is_err() {
        return Err(libc::EALREADY);
    }
    IS_MAIN.with(|m| m.set(true));
    if let Some(v) = std::env::var("VIO_HANDOFF_DISABLE").ok().and_then(|v| v.parse().ok()) {
        HANDOFF_DISABLE.store(v, Ordering::Relaxed);
    }
    for idx in 0..nthreads {
        let w = Worker {
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
        };
        std::thread::Builder::new()
            .name(format!("rust-io-{}", idx))
            .spawn(move || w.run())
            .expect("spawn rust-io thread");
    }
    Ok(main_efd)
}

/* ---------------------------------------------------------------------------
 * Main thread state (exports in ffi/exports.rs)
 * ------------------------------------------------------------------------- */

thread_local! {
    static MAIN_READY: RefCell<VecDeque<Arc<Conn>>> = const { RefCell::new(VecDeque::new()) };
    /// Completed write jobs taken with the current batch.
    static MAIN_DONE: RefCell<VecDeque<usize>> = const { RefCell::new(VecDeque::new()) };
}


/// Take connections that became ready while the main thread was awake (no
/// eventfd was written for them). Returns 1 when there was pending work.
fn take_ready(g: &Global) -> c_int {
    let Some(batch) = g.ready.take() else {
        return 0;
    };
    MAIN_READY.with(|r| r.borrow_mut().extend(batch.conns));
    MAIN_DONE.with(|d| d.borrow_mut().extend(batch.done));
    1
}






















