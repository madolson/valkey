//! valkey-io: a Rust IO layer for valkey-server.
//!
//! Rust owns the listening socket, the client sockets, reads, RESP parsing and
//! writes. The engine (C) sees one `VioConn` handle per connection and pulls
//! parsed commands from it. See DESIGN.md for the threading and ownership
//! model and the full FFI surface; the C declarations live in
//! include/valkey_io.h.

pub mod exec;
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
    /// Bytes accepted from the engine and not yet written to the socket.
    out_bytes: AtomicUsize,
    shared: Mutex<Shared>,
}

impl Drop for Conn {
    fn drop(&mut self) {
        let g = global();
        let sh = self.shared.get_mut().unwrap();
        for cmd in sh.cmds.drain(..) {
            if !cmd.argv.is_null() {
                (g.cbs.free_argv)(cmd.argv, cmd.argc);
            }
        }
        unsafe { libc::close(self.fd) };
    }
}

/* ---------------------------------------------------------------------------
 * Globals
 * ------------------------------------------------------------------------- */

enum Msg {
    NewFd(RawFd),
    Kick(Arc<Conn>),
}

struct IoThread {
    epfd: RawFd,
    wakefd: RawFd,
    signaled: AtomicBool,
    inbox: Mutex<Vec<Msg>>,
}

struct Global {
    cbs: VioCallbacks,
    max_bulk_len: *const i64,
    max_querybuf_len: *const usize,
    threads: Vec<IoThread>,
    main_efd: RawFd,
    main_signaled: AtomicBool,
    ready: Mutex<Vec<Arc<Conn>>>,
    listen_fd: RawFd,
}

unsafe impl Sync for Global {}
unsafe impl Send for Global {}

static GLOBAL: OnceLock<Global> = OnceLock::new();

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
    let t = &global().threads[conn.thread];
    t.inbox.lock().unwrap().push(Msg::Kick(conn.clone()));
    if !t.signaled.swap(true, Ordering::AcqRel) {
        eventfd_signal(t.wakefd);
    }
}

/* ---------------------------------------------------------------------------
 * IO thread
 * ------------------------------------------------------------------------- */

thread_local! {
    /// Index of the IO thread running on this OS thread, usize::MAX elsewhere.
    static IO_THREAD: Cell<usize> = const { Cell::new(usize::MAX) };
    /// exec-rtc: kicks for this thread's own connections raised during a batch.
    static LOCAL_KICKS: RefCell<Vec<Arc<Conn>>> = const { RefCell::new(Vec::new()) };
}

const TOK_WAKE: u64 = 0;
const TOK_LISTEN: u64 = 1;
const TOK_BASE: u64 = 2;

struct Local {
    conn: Arc<Conn>,
    rbuf: Vec<u8>,
    rpos: usize,
    parser: Parser,
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
        if !g.main_signaled.swap(true, Ordering::AcqRel) {
            eventfd_signal(g.main_efd);
        }
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
            auth_required: AtomicBool::new(false),
            out_bytes: AtomicUsize::new(0),
            shared: Mutex::new(Shared { cmds: VecDeque::new(), out: Vec::new() }),
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

        let lim = Limits { max_bulk_len: unsafe { std::ptr::read_volatile(g.max_bulk_len) }, auth_required };
        let mut batch: Vec<VioCmd> = Vec::new();
        let mut ptrs: Vec<*const u8> = Vec::new();
        let mut lens: Vec<usize> = Vec::new();
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
                    batch.push(VioCmd {
                        argv,
                        argc: args.len() as c_int,
                        err: 0,
                        err_byte: 0,
                        argv_len_sum: sum,
                        input_bytes,
                    });
                    l.rpos += consumed;
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
                    });
                    l.proto_err = true;
                    break;
                }
            }
        }
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
            let queued = {
                let mut sh = conn.shared.lock().unwrap();
                sh.cmds.extend(batch);
                sh.cmds.len()
            };
            if queued >= CMDS_HIGH {
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
        if conn.read_paused.load(Ordering::Acquire) && conn.shared.lock().unwrap().cmds.len() < CMDS_LOW {
            conn.read_paused.store(false, Ordering::Release);
        }
        self.flush(idx);
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
            if self.slab[idx].is_some() {
                let l = self.slab[idx].as_ref().unwrap();
                if l.interest & (libc::EPOLLIN as u32) != 0 {
                    self.on_readable(idx);
                } else if mask & ((libc::EPOLLHUP | libc::EPOLLERR) as u32) != 0 {
                    self.peer_gone(idx);
                }
            }
        }
    }

    fn accept_all(&mut self) {
        let g = global();
        loop {
            let fd = unsafe {
                libc::accept4(g.listen_fd, std::ptr::null_mut(), std::ptr::null_mut(), libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
            };
            if fd < 0 {
                return;
            }
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
                if !t.signaled.swap(true, Ordering::AcqRel) {
                    eventfd_signal(t.wakefd);
                }
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

    fn run(mut self) {
        let me = self.me();
        let rtc = exec::rtc_ops();
        if rtc.is_some() {
            IO_THREAD.with(|t| t.set(self.idx));
        }
        let mut timeout: c_int = -1;
        let mut events: Vec<libc::epoll_event> = vec![libc::epoll_event { events: 0, u64: 0 }; 256];
        loop {
            let n = unsafe { libc::epoll_wait(me.epfd, events.as_mut_ptr(), events.len() as c_int, timeout) };
            if n < 0 {
                continue;
            }
            for i in 0..n as usize {
                let tok = events[i].u64;
                let mask = events[i].events;
                if tok == TOK_WAKE {
                    /* Drain before clearing: a producer that sees the flag
                     * set skips the eventfd write, so the flag must not be
                     * cleared after its write was consumed. */
                    eventfd_drain(me.wakefd);
                    me.signaled.store(false, Ordering::Release);
                    let msgs = std::mem::take(&mut *me.inbox.lock().unwrap());
                    for m in msgs {
                        match m {
                            Msg::NewFd(fd) => self.add_conn(fd),
                            Msg::Kick(c) => self.on_kick(c),
                        }
                    }
                } else if tok == TOK_LISTEN {
                    self.accept_all();
                } else {
                    self.on_event((tok - TOK_BASE) as usize, mask);
                }
            }
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
#[no_mangle]
pub extern "C" fn vio_start(cfg: *const VioConfig) -> c_int {
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
        threads.push(IoThread { epfd, wakefd, signaled: AtomicBool::new(false), inbox: Mutex::new(Vec::new()) });
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
        ready: Mutex::new(Vec::new()),
        listen_fd,
    };
    if GLOBAL.set(g).is_err() {
        set_errno(libc::EALREADY);
        return -1;
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
    static MAIN_READY: RefCell<VecDeque<Arc<Conn>>> = RefCell::new(VecDeque::new());
}

/// Consume the main eventfd and take the current batch of ready
/// connections. Connections that become ready later go to the next batch.
#[no_mangle]
pub extern "C" fn vio_main_begin_drain() {
    let g = global();
    /* Drain, then clear, then take the list. Clearing first loses a wakeup
     * when a producer signals between the clear and the drain. */
    eventfd_drain(g.main_efd);
    g.main_signaled.store(false, Ordering::Release);
    let batch = std::mem::take(&mut *g.ready.lock().unwrap());
    MAIN_READY.with(|r| r.borrow_mut().extend(batch));
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

#[no_mangle]
pub extern "C" fn vio_conn_retain(h: *const Conn) {
    unsafe { Arc::increment_strong_count(h) };
}

#[no_mangle]
pub extern "C" fn vio_conn_release(h: *const Conn) {
    unsafe { Arc::decrement_strong_count(h) };
}

#[no_mangle]
pub extern "C" fn vio_conn_take_events(h: *const Conn) -> u32 {
    conn_ref(h).events.swap(0, Ordering::AcqRel)
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
#[no_mangle]
pub extern "C" fn vio_conn_pop_cmds(h: *const Conn, out: *mut VioCmd, max: usize) -> usize {
    let conn = conn_arc(h);
    let (n, left) = {
        let mut sh = conn.shared.lock().unwrap();
        let n = max.min(sh.cmds.len());
        for i in 0..n {
            unsafe { out.add(i).write(sh.cmds.pop_front().unwrap()) };
        }
        (n, sh.cmds.len())
    };
    if n > 0 && left < CMDS_LOW && conn.read_paused.load(Ordering::Acquire) {
        kick(&conn);
    }
    n
}

#[no_mangle]
pub extern "C" fn vio_conn_pending_cmds(h: *const Conn) -> usize {
    conn_ref(h).shared.lock().unwrap().cmds.len()
}

/// Copy engine output into the connection's outbound buffer and wake its
/// IO thread. Returns bytes accepted, or -1 with errno EAGAIN (buffer full,
/// engine keeps the rest and installs its write handler) or EPIPE.
#[no_mangle]
pub extern "C" fn vio_conn_writev(h: *const Conn, iov: *const libc::iovec, iovcnt: c_int) -> isize {
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
    conn_ref(h).auth_required.store(on != 0, Ordering::Relaxed);
}

/// The engine is done with the connection. Pending output is flushed, then
/// the socket is closed once every reference is released.
#[no_mangle]
pub extern "C" fn vio_conn_close(h: *const Conn) {
    let conn = conn_arc(h);
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
        if !g.main_signaled.swap(true, Ordering::AcqRel) {
            eventfd_signal(g.main_efd);
        }
    }
}

/// Wake the main thread's event loop (exec-rtc: it has beforeSleep() work).
#[no_mangle]
pub extern "C" fn vio_wake_main() {
    let g = global();
    if !g.main_signaled.swap(true, Ordering::AcqRel) {
        eventfd_signal(g.main_efd);
    }
}

#[no_mangle]
pub extern "C" fn vio_conn_shutdown(h: *const Conn) {
    unsafe { libc::shutdown(conn_ref(h).fd, libc::SHUT_RDWR) };
}
