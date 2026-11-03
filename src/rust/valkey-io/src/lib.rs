//! valkey-io: a Rust IO layer for valkey-server.
//!
//! Rust owns the listening socket, the client sockets, reads, RESP parsing and
//! writes. The engine (C) sees one `VioConn` handle per connection and pulls
//! parsed commands from it. See DESIGN.md for the threading and ownership
//! model and the full FFI surface; the C declarations live in
//! include/valkey_io.h.

pub mod exec;
pub mod precheck;
pub mod resp;

use resp::{Arg, Limits, Outcome, Parser};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::ffi::{c_char, c_int, c_void, CStr};
use std::os::unix::io::{IntoRawFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/* ---------------------------------------------------------------------------
 * FFI types (keep in sync with include/valkey_io.h)
 * ------------------------------------------------------------------------- */

/// Engine callbacks. Called on Rust IO threads, so they must be thread safe.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VioCallbacks {
    /// Build a `robj **` argv of `argc` string objects from byte slices.
    pub create_argv: extern "C" fn(ptrs: *const *const u8, lens: *const usize, argc: c_int) -> *mut *mut c_void,
    /// Free an argv built by create_argv that the engine never consumed.
    pub free_argv: extern "C" fn(argv: *mut *mut c_void, argc: c_int),
    /// Precheck mode only (cut point 2), NULL otherwise. See precheck.rs.
    pub precheck_begin: Option<precheck::BeginFn>,
    pub precheck_end: Option<precheck::EndFn>,
    pub precheck_cmd: Option<precheck::CheckFn>,
}

#[repr(C)]
pub struct VioConfig {
    pub bindaddr: *const c_char,
    pub port: c_int,
    pub threads: c_int,
    pub proto_max_bulk_len: *const i64,
    pub client_max_querybuf_len: *const usize,
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
}
unsafe impl Send for VioCmd {}

pub const VIO_EV_NEW: u32 = 1 << 0;
pub const VIO_EV_CLOSED: u32 = 1 << 1;
pub const VIO_EV_WRITABLE: u32 = 1 << 2;
pub const VIO_EV_QBLIMIT: u32 = 1 << 3;

/* Backpressure thresholds. */
const CMDS_HIGH: usize = 1024; /* stop reading when this many commands are queued */
const CMDS_LOW: usize = 256; /* resume below this */
const OUT_HIGH: usize = 1 << 20; /* refuse engine writes past this many pending bytes */
const OUT_LOW: usize = 256 << 10; /* fire the engine write handler below this */
const READ_CHUNK: usize = 16 * 1024;

/* Admission control (DESIGN.md "Admission control"). The C listener throttles
 * itself because accept runs on the busy main thread; here the IO threads
 * accept and read regardless of main-thread load, so bound what they hand
 * over:
 * - accepted connections the engine has not adopted yet (VIO_EV_NEW not
 *   taken): stop accepting at NEW_HIGH, resume below NEW_LOW. The kernel
 *   backlog holds the rest, as for the C listener.
 * - open connections: MAXCLIENTS_SLACK past maxclients, refuse on the IO
 *   thread with the engine's error, without involving the main thread.
 *   Below that the engine decides at adoption (acceptCommonHandler():
 *   maxclients-reserved, priority subnets, cluster links, its stats); this
 *   is only the fd backstop for when it is behind.
 * - parsed commands not taken by the engine, across all connections: above
 *   QUEUED_HIGH a connection stops reading once it holds FAIR_SHARE
 *   commands, so a busy pipeline cannot starve a fresh connection. It is
 *   resumed by the existing pop -> kick path, since it holds commands. */
const NEW_HIGH: usize = 128;
const NEW_LOW: usize = 64;
const QUEUED_HIGH: usize = 4096;
const FAIR_SHARE: usize = 32;
/// Below CONFIG_MIN_RESERVED_FDS (32), which the engine adds to maxclients
/// for its fd limit.
const MAXCLIENTS_SLACK: usize = 16;
const MAXCLIENTS_ERR: &[u8] = b"-ERR max number of clients reached\r\n";

/// Accepted connections whose VIO_EV_NEW the engine has not taken.
static UNADOPTED: AtomicUsize = AtomicUsize::new(0);
/// Open connections (accepted, fd not closed yet).
static LIVE: AtomicUsize = AtomicUsize::new(0);
/// Parsed commands queued in Rust, all connections.
static QUEUED: AtomicUsize = AtomicUsize::new(0);
/// The engine's maxclients, published by vio_set_maxclients().
static MAXCLIENTS: AtomicUsize = AtomicUsize::new(usize::MAX);
/// Connections refused at maxclients, not yet counted by the engine.
static REJECTED: AtomicUsize = AtomicUsize::new(0);
/// The listener is out of thread 0's epoll set (UNADOPTED reached NEW_HIGH).
static ACCEPT_PAUSED: AtomicBool = AtomicBool::new(false);

/// Whether a connection holding `queued` commands may read more.
fn may_read(queued: usize) -> bool {
    queued < CMDS_HIGH && (queued < FAIR_SHARE || QUEUED.load(Ordering::Relaxed) < QUEUED_HIGH)
}

/// Whether a paused connection holding `queued` commands may resume.
fn may_resume(queued: usize) -> bool {
    queued < CMDS_LOW && (queued < FAIR_SHARE || QUEUED.load(Ordering::Relaxed) < QUEUED_HIGH)
}

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
    /// Bytes accepted from the engine and not yet written to the socket.
    out_bytes: AtomicUsize,
    /// vio_conn_shutdown() was called: shutdown(2) before close(2).
    shutdown_wanted: AtomicBool,
    shared: Mutex<Shared>,
    /// Engine session mirror (precheck mode).
    session: precheck::SessionCell,
}

impl Drop for Conn {
    fn drop(&mut self) {
        let g = global();
        let sh = self.shared.get_mut().unwrap();
        QUEUED.fetch_sub(sh.cmds.len(), Ordering::Relaxed);
        LIVE.fetch_sub(1, Ordering::Relaxed);
        for cmd in sh.cmds.drain(..) {
            if !cmd.argv.is_null() {
                (g.cbs.free_argv)(cmd.argv, cmd.argc);
            }
        }
        unsafe {
            if self.shutdown_wanted.load(Ordering::Acquire) {
                libc::shutdown(self.fd, libc::SHUT_RDWR);
            }
            libc::close(self.fd)
        };
    }
}

/* ---------------------------------------------------------------------------
 * Globals
 * ------------------------------------------------------------------------- */

enum Msg {
    NewFd(RawFd),
    Kick(Arc<Conn>),
    /// To thread 0: UNADOPTED fell below NEW_LOW, poll the listener again.
    ResumeAccept,
}

struct IoThread {
    epfd: RawFd,
    wakefd: RawFd,
    signaled: AtomicBool,
    /// Set just before epoll_wait, cleared after: producers skip the eventfd
    /// write while the thread is awake.
    sleeping: AtomicBool,
    /// Inbox has entries the thread has not taken yet.
    pending: AtomicBool,
    inbox: Mutex<Vec<Msg>>,
}

struct Global {
    cbs: VioCallbacks,
    max_bulk_len: *const i64,
    max_querybuf_len: *const usize,
    threads: Vec<IoThread>,
    main_efd: RawFd,
    main_signaled: AtomicBool,
    /// Main thread is (about to be) blocked in poll; set by
    /// vio_main_before_poll, cleared by vio_main_after_poll. While it is
    /// clear, producers skip the eventfd write.
    main_sleeping: AtomicBool,
    /// `ready` has entries, or the main loop has beforeSleep() work
    /// (vio_wake_main), that the main thread has not taken yet.
    main_pending: AtomicBool,
    ready: Mutex<Vec<Arc<Conn>>>,
    listen_fd: RawFd,
}

unsafe impl Sync for Global {}
unsafe impl Send for Global {}

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

fn eventfd_signal(fd: RawFd) {
    let one: u64 = 1;
    unsafe { libc::write(fd, &one as *const u64 as *const c_void, 8) };
}

fn eventfd_drain(fd: RawFd) {
    let mut v: u64 = 0;
    unsafe { libc::read(fd, &mut v as *mut u64 as *mut c_void, 8) };
}

fn errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

fn set_errno(e: i32) {
    unsafe { *libc::__errno_location() = e };
}

/// Ask the owning IO thread to re-evaluate a connection (flush output,
/// resume reading, finish a close). Coalesced: one inbox entry per conn
/// until the IO thread picks it up, one eventfd write per wakeup.
fn kick(conn: &Arc<Conn>) {
    if conn.kicked.swap(true, Ordering::AcqRel) {
        return;
    }
    /* exec-rtc: the owning IO thread is executing this connection's
     * commands right now; handle the kick after the batch, no eventfd. */
    if IO_THREAD.with(|t| t.get()) == conn.thread {
        LOCAL_KICKS.with(|k| k.borrow_mut().push(conn.clone()));
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
    let t = &global().threads[conn.thread];
    t.inbox.lock().unwrap().push(Msg::Kick(conn.clone()));
    wake_thread(t);
}

/// Call after pushing to `t.inbox`. Pairs with the sleep check in
/// Worker::run (Dekker, all SeqCst): either this sees `sleeping` set and
/// writes the eventfd, or the thread sees `pending` set and does not block.
/// Call after pushing to `g.ready` (or creating beforeSleep() work). Pairs
/// with vio_main_before_poll (Dekker, all SeqCst): either this sees the main
/// thread sleeping and writes the eventfd, or the main thread sees
/// `main_pending` and polls without blocking.
fn wake_main(g: &Global) {
    g.main_pending.store(true, Ordering::SeqCst);
    if (hd(HD_MAIN_AWAKE) || g.main_sleeping.load(Ordering::SeqCst)) && !g.main_signaled.swap(true, Ordering::SeqCst) {
        eventfd_signal(g.main_efd);
    }
}

fn wake_thread(t: &IoThread) {
    t.pending.store(true, Ordering::SeqCst);
    if (hd(HD_IO_AWAKE) || t.sleeping.load(Ordering::SeqCst)) && !t.signaled.swap(true, Ordering::SeqCst) {
        eventfd_signal(t.wakefd);
    }
}

/// Hand the kicks collected on the main thread to their IO threads. Called
/// once per event-loop iteration, before the main thread polls, and inside
/// processEventsWhileBlocked().
#[no_mangle]
pub extern "C" fn vio_main_flush_kicks() {
    if GLOBAL.get().is_none() {
        return;
    }
    MAIN_KICKS.with(|k| {
        for (idx, list) in k.borrow_mut().iter_mut().enumerate() {
            if !list.is_empty() {
                flush_kicks(idx, list);
            }
        }
    });
}

/// Main-thread kicks per IO thread handed over before the end of the
/// iteration.
const KICK_BATCH: usize = 32;

fn flush_kicks(idx: usize, list: &mut Vec<Arc<Conn>>) {
    let t = &global().threads[idx];
    t.inbox.lock().unwrap().extend(list.drain(..).map(Msg::Kick));
    wake_thread(t);
}

/* ---------------------------------------------------------------------------
 * IO thread
 * ------------------------------------------------------------------------- */

thread_local! {
    /// Index of the IO thread running on this OS thread, usize::MAX elsewhere.
    static IO_THREAD: Cell<usize> = const { Cell::new(usize::MAX) };
    /// exec-rtc: kicks for this thread's own connections raised during a batch.
    static LOCAL_KICKS: RefCell<Vec<Arc<Conn>>> = const { RefCell::new(Vec::new()) };
    /// True on the thread that called vio_start() (the engine main thread).
    static IS_MAIN: Cell<bool> = const { Cell::new(false) };
    /// Main thread: kicks per IO thread, flushed by vio_main_flush_kicks().
    static MAIN_KICKS: RefCell<Vec<Vec<Arc<Conn>>>> = const { RefCell::new(Vec::new()) };
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
    registered: bool,
    interest: u32,
}

struct Worker {
    idx: usize,
    slab: Vec<Option<Local>>,
    free: Vec<usize>,
    to_notify: Vec<Arc<Conn>>,
    rr: usize,
}

impl Worker {
    fn me(&self) -> &'static IoThread {
        &global().threads[self.idx]
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
        if self.to_notify.is_empty() {
            return;
        }
        let g = global();
        g.ready.lock().unwrap().append(&mut self.to_notify);
        wake_main(g);
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
            out_bytes: AtomicUsize::new(0),
            shutdown_wanted: AtomicBool::new(false),
            shared: Mutex::new(Shared { cmds: VecDeque::new(), out: Vec::new() }),
            session: precheck::SessionCell::new(),
        });
        let idx = match self.free.pop() {
            Some(i) => i,
            None => {
                self.slab.push(None);
                self.slab.len() - 1
            }
        };
        conn.token.store(idx, Ordering::Relaxed);
        let mut ev = libc::epoll_event { events: libc::EPOLLIN as u32, u64: idx as u64 + TOK_BASE };
        unsafe { libc::epoll_ctl(self.me().epfd, libc::EPOLL_CTL_ADD, fd, &mut ev) };
        self.slab[idx] = Some(Local {
            conn: conn.clone(),
            rbuf: Vec::new(),
            rpos: 0,
            parser: Parser::new(),
            precheck: precheck::Local::default(),
            proto_err: false,
            wbuf: Vec::new(),
            woff: 0,
            registered: true,
            interest: libc::EPOLLIN as u32,
        });
        self.notify(&conn, VIO_EV_NEW);
    }

    fn remove(&mut self, idx: usize) {
        if let Some(l) = self.slab[idx].take() {
            if l.registered {
                unsafe { libc::epoll_ctl(self.me().epfd, libc::EPOLL_CTL_DEL, l.conn.fd, std::ptr::null_mut()) };
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
        if !c.read_paused.load(Ordering::Acquire) && !c.c_closed.load(Ordering::Acquire) && !l.proto_err {
            want |= libc::EPOLLIN as u32;
        }
        if l.woff < l.wbuf.len() {
            want |= libc::EPOLLOUT as u32;
        }
        if want != l.interest {
            let mut ev = libc::epoll_event { events: want, u64: idx as u64 + TOK_BASE };
            unsafe { libc::epoll_ctl(epfd, libc::EPOLL_CTL_MOD, c.fd, &mut ev) };
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
                unsafe { libc::epoll_ctl(epfd, libc::EPOLL_CTL_DEL, l.conn.fd, std::ptr::null_mut()) };
                l.registered = false;
            }
            l.conn.clone()
        };
        conn.peer_closed.store(true, Ordering::Release);
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
        l.rbuf.reserve(want);
        let n = unsafe {
            libc::read(conn.fd, l.rbuf.as_mut_ptr().add(l.rbuf.len()) as *mut c_void, want)
        };
        if n == 0 {
            self.peer_gone(idx);
            return;
        }
        if n < 0 {
            let e = errno();
            if e != libc::EAGAIN && e != libc::EINTR {
                self.peer_gone(idx);
            }
            return;
        }
        unsafe { l.rbuf.set_len(l.rbuf.len() + n as usize) };

        /* client-query-buffer-limit, checked like readToQueryBuf(): on the
         * unparsed bytes, before parsing what this read added. */
        let auth_required = conn.auth_required.load(Ordering::Relaxed);
        let unparsed = l.rbuf.len() - l.rpos;
        let qb_limit = unsafe { std::ptr::read_volatile(g.max_querybuf_len) };
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
    /// The engine's next vio_conn_set_auth_required() resumes parsing.
    fn parse_buffered(&mut self, idx: usize) {
        let g = global();
        let l = match self.slab[idx].as_mut() {
            Some(l) => l,
            None => return,
        };
        let conn = l.conn.clone();
        if conn.c_closed.load(Ordering::Acquire) || l.proto_err {
            return;
        }
        let auth_required = conn.auth_required.load(Ordering::Acquire);
        let lim = Limits { max_bulk_len: unsafe { std::ptr::read_volatile(g.max_bulk_len) }, auth_required };
        let mut batch: Vec<VioCmd> = Vec::new();
        let mut ptrs: Vec<*const u8> = Vec::new();
        let mut lens: Vec<usize> = Vec::new();
        let pc = precheck::Batch::begin(&g.cbs, self.idx, &conn.session);
        loop {
            let frame = &l.rbuf[l.rpos..];
            match l.parser.parse(frame, &lim) {
                Outcome::Command { args, consumed, input_bytes } => {
                    ptrs.clear();
                    lens.clear();
                    let mut sum = 0usize;
                    for a in &args {
                        let (p, len) = match a {
                            Arg::Range(o, len) => (frame[*o..].as_ptr(), *len),
                            Arg::Owned(v) => (v.as_ptr(), v.len()),
                        };
                        ptrs.push(p);
                        lens.push(len);
                        sum += len;
                    }
                    let argv = (g.cbs.create_argv)(ptrs.as_ptr(), lens.as_ptr(), args.len() as c_int);
                    let mut cmd = VioCmd {
                        argv,
                        argc: args.len() as c_int,
                        err: 0,
                        err_byte: 0,
                        argv_len_sum: sum,
                        input_bytes,
                        cmd: std::ptr::null_mut(),
                        slot: -1,
                        read_flags: 0,
                        token: 0,
                    };
                    if let Some(pc) = pc.as_ref() {
                        pc.check(&mut l.precheck, &mut cmd);
                    }
                    batch.push(cmd);
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
                    });
                    l.proto_err = true;
                    break;
                }
            }
        }
        drop(pc);
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
                sh.cmds.extend(batch);
                sh.cmds.len()
            };
            QUEUED.fetch_add(n, Ordering::Relaxed);
            if !may_read(queued) {
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
            return;
        }
        let mut failed = false;
        loop {
            if l.woff == l.wbuf.len() {
                l.wbuf.clear();
                l.woff = 0;
                let mut sh = conn.shared.lock().unwrap();
                std::mem::swap(&mut l.wbuf, &mut sh.out);
                drop(sh);
                if l.wbuf.is_empty() {
                    break;
                }
            }
            let n = unsafe {
                libc::write(conn.fd, l.wbuf.as_ptr().add(l.woff) as *const c_void, l.wbuf.len() - l.woff)
            };
            if n > 0 {
                l.woff += n as usize;
                conn.out_bytes.fetch_sub(n as usize, Ordering::AcqRel);
            } else {
                let e = errno();
                if n < 0 && (e == libc::EAGAIN || e == libc::EINTR) {
                    break;
                }
                failed = true;
                break;
            }
        }
        if l.wbuf.capacity() > 4 * OUT_HIGH && l.woff == l.wbuf.len() {
            l.wbuf = Vec::new();
            l.woff = 0;
        }
        if failed {
            self.peer_gone(idx);
            return;
        }
        /* Level triggered, like a socket write handler: keep firing after
         * each flush while the engine keeps its handler installed. */
        if conn.want_writable.load(Ordering::Acquire) && conn.out_bytes.load(Ordering::Acquire) < OUT_LOW {
            self.notify(&conn, VIO_EV_WRITABLE);
        }
        self.update_interest(idx);
    }

    fn on_kick(&mut self, conn: Arc<Conn>) {
        conn.kicked.store(false, Ordering::Release);
        let idx = conn.token.load(Ordering::Relaxed);
        match self.slab.get(idx) {
            Some(Some(l)) if Arc::ptr_eq(&l.conn, &conn) => {}
            _ => return,
        }
        if conn.c_closed.load(Ordering::Acquire) && conn.peer_closed.load(Ordering::Acquire) {
            self.remove(idx);
            return;
        }
        if conn.read_paused.load(Ordering::Acquire) && may_resume(conn.shared.lock().unwrap().cmds.len()) {
            conn.read_paused.store(false, Ordering::Release);
        }
        self.flush(idx);
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

    fn on_event(&mut self, idx: usize, mask: u32) {
        if mask & (libc::EPOLLOUT as u32) != 0 {
            self.flush(idx);
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
        if mask & ((libc::EPOLLIN | libc::EPOLLHUP | libc::EPOLLERR) as u32) != 0 {
            if let Some(l) = self.slab[idx].as_ref() {
                if l.interest & (libc::EPOLLIN as u32) != 0 {
                    self.on_readable(idx);
                } else if mask & ((libc::EPOLLHUP | libc::EPOLLERR) as u32) != 0 {
                    self.peer_gone(idx);
                }
            }
        }
    }

    /// Thread 0 only: add or remove the listener from the epoll set.
    fn listen_interest(&mut self, on: bool) {
        let g = global();
        let mut ev = libc::epoll_event { events: if on { libc::EPOLLIN as u32 } else { 0 }, u64: TOK_LISTEN };
        unsafe { libc::epoll_ctl(self.me().epfd, libc::EPOLL_CTL_MOD, g.listen_fd, &mut ev) };
    }

    /// Stop polling the listener while the engine is NEW_HIGH connections
    /// behind. Returns true when paused.
    fn accept_backoff(&mut self) -> bool {
        if UNADOPTED.load(Ordering::SeqCst) < NEW_HIGH {
            return false;
        }
        ACCEPT_PAUSED.store(true, Ordering::SeqCst);
        self.listen_interest(false);
        /* The engine may have adopted below NEW_LOW between the load and
         * the store, and seen ACCEPT_PAUSED clear: undo it ourselves. */
        if UNADOPTED.load(Ordering::SeqCst) < NEW_LOW && ACCEPT_PAUSED.swap(false, Ordering::SeqCst) {
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
            let fd = unsafe {
                libc::accept4(g.listen_fd, std::ptr::null_mut(), std::ptr::null_mut(), libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
            };
            if fd < 0 {
                return;
            }
            if LIVE.load(Ordering::Relaxed) >= MAXCLIENTS.load(Ordering::Relaxed).saturating_add(MAXCLIENTS_SLACK) {
                /* What acceptCommonHandler() does at maxclients: best-effort
                 * error, then close. */
                unsafe {
                    libc::write(fd, MAXCLIENTS_ERR.as_ptr() as *const c_void, MAXCLIENTS_ERR.len());
                    libc::close(fd);
                }
                REJECTED.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            LIVE.fetch_add(1, Ordering::Relaxed);
            UNADOPTED.fetch_add(1, Ordering::SeqCst);
            let one: c_int = 1;
            unsafe {
                libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, &one as *const c_int as *const c_void, 4);
            }
            let target = self.rr % g.threads.len();
            self.rr = self.rr.wrapping_add(1);
            if target == self.idx {
                self.add_conn(fd);
            } else {
                let t = &g.threads[target];
                t.inbox.lock().unwrap().push(Msg::NewFd(fd));
                wake_thread(t);
            }
        }
    }

    /// exec-rtc: execute everything this poll iteration produced, under one
    /// engine lock acquisition, then flush the replies it generated. Returns
    /// true when work is left (poll without blocking next time).
    fn rtc_cycle(&mut self, rtc: &exec::VioRtcOps) -> bool {
        for _ in 0..16 {
            if self.to_notify.is_empty() {
                return false;
            }
            let batch = std::mem::take(&mut self.to_notify);
            let t0 = exec::now_ns();
            let uncontended = (rtc.try_lock)() == 0;
            if !uncontended && (rtc.lock)() != 0 {
                /* The holder is yielding inside a long script/module
                 * command: give the batch to whoever polls the main
                 * eventfd (processEventsWhileBlocked() on the holder). */
                self.to_notify = batch;
                self.flush_notify();
                return false;
            }
            let t1 = exec::now_ns();
            let n = batch.len();
            for conn in batch {
                /* Same order as vio_main_next_ready(): clear in_ready before
                 * reading the events and commands it guards. */
                conn.in_ready.store(false, Ordering::Release);
                (rtc.exec_ready)(Arc::as_ptr(&conn));
            }
            (rtc.after_batch)();
            let t2 = exec::now_ns();
            (rtc.unlock)();
            exec::rtc_record(self.idx, t1 - t0, t2 - t1, uncontended, n);
            let kicks = LOCAL_KICKS.with(|k| std::mem::take(&mut *k.borrow_mut()));
            for c in kicks {
                self.on_kick(c);
            }
        }
        !self.to_notify.is_empty()
    }

    fn take_inbox(&mut self) {
        let me = self.me();
        if me.pending.swap(false, Ordering::SeqCst) {
            let msgs = std::mem::take(&mut *me.inbox.lock().unwrap());
            for m in msgs {
                match m {
                    Msg::NewFd(fd) => self.add_conn(fd),
                    Msg::Kick(c) => self.on_kick(c),
                    Msg::ResumeAccept => {
                        self.listen_interest(true);
                        self.accept_all();
                    }
                }
            }
        }
    }

    fn run(mut self) {
        let me = self.me();
        let rtc = exec::rtc_ops();
        if rtc.is_some() {
            IO_THREAD.with(|t| t.set(self.idx));
        }
        let mut timeout: c_int = -1;
        let mut events: Vec<libc::epoll_event> = vec![libc::epoll_event { events: 0, u64: 0 }; 256];
        loop {
            if timeout != 0 {
                me.sleeping.store(true, Ordering::SeqCst);
                if me.pending.load(Ordering::SeqCst) {
                    timeout = 0;
                }
            }
            let n = unsafe { libc::epoll_wait(me.epfd, events.as_mut_ptr(), events.len() as c_int, timeout) };
            me.sleeping.store(false, Ordering::SeqCst);
            for ev in events.iter().take(n.max(0) as usize) {
                let tok = ev.u64;
                let mask = ev.events;
                if tok == TOK_WAKE {
                    /* Drain before clearing: a producer that sees the flag
                     * set skips the eventfd write, so the flag must not be
                     * cleared after its write was consumed. The inbox is
                     * taken below, after the clear. */
                    eventfd_drain(me.wakefd);
                    me.signaled.store(false, Ordering::SeqCst);
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
            if !me.signaled.load(Ordering::SeqCst) {
                self.take_inbox();
            }
            timeout = -1;
            match rtc {
                Some(r) => timeout = if self.rtc_cycle(r) { 0 } else { -1 },
                None => self.flush_notify(),
            }
        }
    }
}

/* ---------------------------------------------------------------------------
 * Exported API: startup
 * ------------------------------------------------------------------------- */

/// Bind the listener and start the IO threads. Returns the eventfd the
/// engine must poll for readability (then call vio_main_begin_drain), or
/// -1 with errno set.
///
/// # Safety
///
/// `cfg` must point to a valid `VioConfig`. `bindaddr` is NULL or a NUL-terminated
/// string; `proto_max_bulk_len` and `client_max_querybuf_len` must stay valid
/// for the life of the process (IO threads read them).
#[no_mangle]
pub unsafe extern "C" fn vio_start(cfg: *const VioConfig) -> c_int {
    let cfg = unsafe { &*cfg };
    let addr = if cfg.bindaddr.is_null() {
        "0.0.0.0".to_string()
    } else {
        let s = unsafe { CStr::from_ptr(cfg.bindaddr) }.to_string_lossy().into_owned();
        let s = s.trim_start_matches('-').to_string();
        if s == "*" { "0.0.0.0".to_string() } else if s == "::*" { "::".to_string() } else { s }
    };
    let listener = match std::net::TcpListener::bind((addr.as_str(), cfg.port as u16)) {
        Ok(l) => l,
        Err(e) => {
            set_errno(e.raw_os_error().unwrap_or(libc::EINVAL));
            return -1;
        }
    };
    if listener.set_nonblocking(true).is_err() {
        return -1;
    }
    let listen_fd = listener.into_raw_fd();
    let nthreads = cfg.threads.max(1) as usize;
    let mut threads = Vec::with_capacity(nthreads);
    for _ in 0..nthreads {
        let epfd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        let wakefd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if epfd < 0 || wakefd < 0 {
            return -1;
        }
        let mut ev = libc::epoll_event { events: libc::EPOLLIN as u32, u64: TOK_WAKE };
        unsafe { libc::epoll_ctl(epfd, libc::EPOLL_CTL_ADD, wakefd, &mut ev) };
        threads.push(IoThread {
            epfd,
            wakefd,
            signaled: AtomicBool::new(false),
            sleeping: AtomicBool::new(false),
            pending: AtomicBool::new(false),
            inbox: Mutex::new(Vec::new()),
        });
    }
    let mut ev = libc::epoll_event { events: libc::EPOLLIN as u32, u64: TOK_LISTEN };
    unsafe { libc::epoll_ctl(threads[0].epfd, libc::EPOLL_CTL_ADD, listen_fd, &mut ev) };
    let main_efd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    if main_efd < 0 {
        return -1;
    }
    let g = Global {
        cbs: cfg.cbs,
        max_bulk_len: cfg.proto_max_bulk_len,
        max_querybuf_len: cfg.client_max_querybuf_len,
        threads,
        main_efd,
        main_signaled: AtomicBool::new(false),
        main_sleeping: AtomicBool::new(true),
        main_pending: AtomicBool::new(false),
        ready: Mutex::new(Vec::new()),
        listen_fd,
    };
    if GLOBAL.set(g).is_err() {
        set_errno(libc::EALREADY);
        return -1;
    }
    IS_MAIN.with(|m| m.set(true));
    if let Some(v) = std::env::var("VIO_HANDOFF_DISABLE").ok().and_then(|v| v.parse().ok()) {
        HANDOFF_DISABLE.store(v, Ordering::Relaxed);
    }
    for idx in 0..nthreads {
        let w = Worker { idx, slab: Vec::new(), free: Vec::new(), to_notify: Vec::new(), rr: 0 };
        std::thread::Builder::new()
            .name(format!("rust-io-{}", idx))
            .spawn(move || w.run())
            .expect("spawn rust-io thread");
    }
    main_efd
}

/* ---------------------------------------------------------------------------
 * Exported API: main thread
 * ------------------------------------------------------------------------- */

thread_local! {
    static MAIN_READY: RefCell<VecDeque<Arc<Conn>>> = const { RefCell::new(VecDeque::new()) };
}

/// Consume the main eventfd and take the current batch of ready
/// connections. Connections that become ready later go to the next batch.
#[no_mangle]
pub extern "C" fn vio_main_begin_drain() {
    let g = global();
    /* Drain, then clear, then take the list. Clearing first loses a wakeup
     * when a producer signals between the clear and the drain. */
    eventfd_drain(g.main_efd);
    g.main_signaled.store(false, Ordering::SeqCst);
    vio_main_take_ready();
}

/// Take connections that became ready while the main thread was awake (no
/// eventfd was written for them). Returns 1 when there was pending work.
/// Called from beforeSleep() through the connection type's pending-data hook.
#[no_mangle]
pub extern "C" fn vio_main_take_ready() -> c_int {
    let g = global();
    if !g.main_pending.swap(false, Ordering::SeqCst) {
        return 0;
    }
    let batch = std::mem::take(&mut *g.ready.lock().unwrap());
    MAIN_READY.with(|r| r.borrow_mut().extend(batch));
    1
}

/// Last call before the main thread polls. Returns 1 when work arrived that
/// it must not sleep on (poll with a zero timeout).
#[no_mangle]
pub extern "C" fn vio_main_before_poll() -> c_int {
    let g = global();
    g.main_sleeping.store(true, Ordering::SeqCst);
    g.main_pending.load(Ordering::SeqCst) as c_int
}

/// First call after the main thread polled. Takes work handed over while
/// it was awake and returns 1 if there is some, unless an eventfd write is
/// pending: then the eventfd handler takes it in this iteration, and taking
/// it here as well would run two batches before the first one's replies.
#[no_mangle]
pub extern "C" fn vio_main_after_poll() -> c_int {
    let g = global();
    g.main_sleeping.store(false, Ordering::SeqCst);
    if g.main_signaled.load(Ordering::SeqCst) {
        return 0;
    }
    vio_main_take_ready()
}

/// Next ready connection from the current batch, or NULL. The caller owns
/// one reference and must vio_conn_release() it.
#[no_mangle]
pub extern "C" fn vio_main_next_ready() -> *const Conn {
    match MAIN_READY.with(|r| r.borrow_mut().pop_front()) {
        Some(c) => {
            c.in_ready.store(false, Ordering::Release);
            Arc::into_raw(c)
        }
        None => std::ptr::null(),
    }
}

/// Whether the current batch still holds connections. True inside a long
/// command (script, module) that was started from vio_main_next_ready():
/// the rest of that batch is only reachable through this list, because the
/// eventfd that announced it was already drained.
#[no_mangle]
pub extern "C" fn vio_main_has_ready() -> c_int {
    MAIN_READY.with(|r| !r.borrow().is_empty()) as c_int
}

/* ---------------------------------------------------------------------------
 * Exported API: per connection (main thread)
 * ------------------------------------------------------------------------- */

fn conn_ref<'a>(h: *const Conn) -> &'a Conn {
    unsafe { &*h }
}

/// Borrow the handle as an Arc without changing the refcount.
fn conn_arc(h: *const Conn) -> std::mem::ManuallyDrop<Arc<Conn>> {
    std::mem::ManuallyDrop::new(unsafe { Arc::from_raw(h) })
}

///
/// # Safety
///
/// `h` must be a live handle the caller holds a reference to.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_retain(h: *const Conn) {
    unsafe { Arc::increment_strong_count(h) };
}

///
/// # Safety
///
/// `h` must be a live handle; the caller gives up one reference it holds.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_release(h: *const Conn) {
    unsafe { Arc::decrement_strong_count(h) };
}

#[no_mangle]
pub extern "C" fn vio_conn_take_events(h: *const Conn) -> u32 {
    let ev = conn_ref(h).events.swap(0, Ordering::AcqRel);
    if ev & VIO_EV_NEW != 0 && UNADOPTED.fetch_sub(1, Ordering::SeqCst) - 1 < NEW_LOW && ACCEPT_PAUSED.swap(false, Ordering::SeqCst) {
        let t = &global().threads[0];
        t.inbox.lock().unwrap().push(Msg::ResumeAccept);
        wake_thread(t);
    }
    ev
}

/// Publish the engine's maxclients (admission control).
#[no_mangle]
pub extern "C" fn vio_set_maxclients(n: usize) {
    MAXCLIENTS.store(n, Ordering::Relaxed);
}

/// Connections refused at maxclients since the last call.
#[no_mangle]
pub extern "C" fn vio_take_rejected() -> usize {
    REJECTED.swap(0, Ordering::Relaxed)
}

#[no_mangle]
pub extern "C" fn vio_conn_udata(h: *const Conn) -> *mut c_void {
    conn_ref(h).udata.load(Ordering::Relaxed)
}

#[no_mangle]
pub extern "C" fn vio_conn_set_udata(h: *const Conn, p: *mut c_void) {
    conn_ref(h).udata.store(p, Ordering::Relaxed)
}

#[no_mangle]
pub extern "C" fn vio_conn_fd(h: *const Conn) -> c_int {
    conn_ref(h).fd
}

/// Move up to `max` parsed commands into `out`, in order.
///
/// # Safety
///
/// `h` must be a live handle and `out` must have room for `max` `VioCmd`s.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_pop_cmds(h: *const Conn, out: *mut VioCmd, max: usize) -> usize {
    let conn = conn_arc(h);
    let (n, left) = {
        let mut sh = conn.shared.lock().unwrap();
        let n = max.min(sh.cmds.len());
        for i in 0..n {
            unsafe { out.add(i).write(sh.cmds.pop_front().unwrap()) };
        }
        (n, sh.cmds.len())
    };
    QUEUED.fetch_sub(n, Ordering::Relaxed);
    if n > 0 && left < CMDS_LOW && conn.read_paused.load(Ordering::Acquire) {
        kick(&conn);
    }
    n
}

#[no_mangle]
pub extern "C" fn vio_conn_pending_cmds(h: *const Conn) -> usize {
    conn_ref(h).shared.lock().unwrap().cmds.len()
}

/// Bytes accepted from the engine and not yet written to the socket.
#[no_mangle]
pub extern "C" fn vio_conn_pending_out(h: *const Conn) -> usize {
    conn_ref(h).out_bytes.load(Ordering::Acquire)
}

/// Copy engine output into the connection's outbound buffer and wake its
/// IO thread. Returns bytes accepted, or -1 with errno EAGAIN (buffer full,
/// engine keeps the rest and installs its write handler) or EPIPE.
///
/// # Safety
///
/// `h` must be a live handle and `iov` must point to `iovcnt` valid iovecs.
#[no_mangle]
pub unsafe extern "C" fn vio_conn_writev(h: *const Conn, iov: *const libc::iovec, iovcnt: c_int) -> isize {
    let conn = conn_arc(h);
    if conn.peer_closed.load(Ordering::Acquire) {
        set_errno(libc::EPIPE);
        return -1;
    }
    let pending = conn.out_bytes.load(Ordering::Acquire);
    if pending >= OUT_HIGH {
        set_errno(libc::EAGAIN);
        return -1;
    }
    let mut room = OUT_HIGH - pending;
    let mut written = 0usize;
    {
        let mut sh = conn.shared.lock().unwrap();
        for i in 0..iovcnt as usize {
            if room == 0 {
                break;
            }
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

/// The engine wants VIO_EV_WRITABLE once outbound bytes drop below the low
/// watermark (it has data it could not hand over).
#[no_mangle]
pub extern "C" fn vio_conn_want_writable(h: *const Conn, on: c_int) {
    let conn = conn_arc(h);
    conn.want_writable.store(on != 0, Ordering::Release);
    if on != 0 {
        kick(&conn);
    }
}

/// Published engine state the parser needs: unauthenticated clients get
/// tighter multibulk/bulk limits.
#[no_mangle]
pub extern "C" fn vio_conn_set_auth_required(h: *const Conn, on: c_int) {
    let conn = conn_arc(h);
    conn.auth_required.store(on != 0, Ordering::Release);
    /* Store, then check: the IO thread sets parse_held before the engine
     * can see the command it stopped after, so this cannot miss it. */
    if conn.parse_held.load(Ordering::Acquire) {
        kick(&conn);
    }
}

#[no_mangle]
pub extern "C" fn vio_set_default_auth_required(on: c_int) {
    DEFAULT_AUTH_REQUIRED.store(on != 0, Ordering::Relaxed);
}

/// Publish the session fields the precheck reads (cut point 2). Read by
/// the IO thread once per parsed batch.
///
/// # Safety
///
/// `h` must be a live handle and `s` must point to a valid `VioSession`
/// (copied before return).
#[no_mangle]
pub unsafe extern "C" fn vio_conn_set_session(h: *const Conn, s: *const precheck::VioSession) {
    conn_ref(h).session.set(unsafe { &*s });
}

/// The engine is done with the connection. Pending output is flushed, then
/// the socket is closed once every reference is released.
#[no_mangle]
pub extern "C" fn vio_conn_close(h: *const Conn) {
    let conn = conn_arc(h);
    /* Commands the engine will never pop: free them now instead of when
     * the output drains, so they stop counting against QUEUED_HIGH. */
    let dropped: Vec<VioCmd> = conn.shared.lock().unwrap().cmds.drain(..).collect();
    QUEUED.fetch_sub(dropped.len(), Ordering::Relaxed);
    for cmd in dropped {
        if !cmd.argv.is_null() {
            (global().cbs.free_argv)(cmd.argv, cmd.argc);
        }
    }
    conn.udata.store(std::ptr::null_mut(), Ordering::Relaxed);
    conn.want_writable.store(false, Ordering::Release);
    conn.c_closed.store(true, Ordering::Release);
    kick(&conn);
}

/// exec-rtc: queue the connection on the main thread's ready list, as the
/// io mode does, so the main thread pulls and executes its commands.
#[no_mangle]
pub extern "C" fn vio_conn_defer_to_main(h: *const Conn) {
    let conn = conn_arc(h);
    if !conn.in_ready.swap(true, Ordering::AcqRel) {
        let g = global();
        g.ready.lock().unwrap().push((*conn).clone());
        wake_main(g);
    }
}

/// Wake the main thread's event loop (exec-rtc: it has beforeSleep() work).
#[no_mangle]
pub extern "C" fn vio_wake_main() {
    wake_main(global());
}

#[no_mangle]
pub extern "C" fn vio_conn_shutdown(h: *const Conn) {
    /* freeClient() calls this while a fork child holds a copy of the fd,
     * then connClose(). A socket write would already be in the kernel, but
     * the output handed to Rust is not: shutting down now discarded it
     * (CLIENT KILL SKIPME no lost its own reply during BGSAVE). Shut down
     * when the IO thread closes the fd, after the output drained. */
    conn_ref(h).shutdown_wanted.store(true, Ordering::Release);
}
