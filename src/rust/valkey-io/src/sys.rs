//! Thin wrappers over the Linux syscalls the IO threads use. Together with
//! `ffi/`, the only module allowed to contain `unsafe`.
#![allow(unsafe_code)]

use std::ffi::{c_int, c_void};
use std::os::unix::io::RawFd;

pub type EpollEvent = libc::epoll_event;
pub const EPOLLIN: u32 = libc::EPOLLIN as u32;
pub const EPOLLOUT: u32 = libc::EPOLLOUT as u32;
pub const EPOLLHUP: u32 = libc::EPOLLHUP as u32;
pub const EPOLLERR: u32 = libc::EPOLLERR as u32;
pub const EPOLLRDHUP: u32 = libc::EPOLLRDHUP as u32;

pub fn errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

pub fn set_errno(e: i32) {
    // SAFETY: __errno_location() returns this thread's errno slot.
    unsafe { *libc::__errno_location() = e };
}

pub fn close(fd: RawFd) {
    // SAFETY: plain syscall; the caller owns fd.
    unsafe { libc::close(fd) };
}

/// Wait up to `ms` for `fd` to become writable. False on timeout or error.
pub fn poll_writable(fd: RawFd, ms: c_int) -> bool {
    let mut p = libc::pollfd { fd, events: libc::POLLOUT, revents: 0 };
    // SAFETY: `p` is live for the call.
    unsafe { libc::poll(&mut p, 1, ms) > 0 }
}

pub fn shutdown(fd: RawFd) {
    // SAFETY: plain syscall on an fd the caller keeps open.
    unsafe { libc::shutdown(fd, libc::SHUT_RDWR) };
}

/// Non-blocking, close-on-exec eventfd, or -1.
pub fn eventfd() -> RawFd {
    // SAFETY: plain syscall.
    unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) }
}

pub fn eventfd_signal(fd: RawFd) {
    let one: u64 = 1;
    // SAFETY: writes 8 bytes from a live u64.
    unsafe { libc::write(fd, &one as *const u64 as *const c_void, 8) };
}

pub fn eventfd_drain(fd: RawFd) {
    let mut v: u64 = 0;
    // SAFETY: reads 8 bytes into a live u64.
    unsafe { libc::read(fd, &mut v as *mut u64 as *mut c_void, 8) };
}

/// Close-on-exec epoll instance, or -1.
pub fn epoll_create() -> RawFd {
    // SAFETY: plain syscall.
    unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) }
}

fn epoll_ctl(epfd: RawFd, op: c_int, fd: RawFd, events: u32, tok: u64) {
    let mut ev = libc::epoll_event { events, u64: tok };
    // SAFETY: `ev` is live for the call (ignored for EPOLL_CTL_DEL).
    unsafe { libc::epoll_ctl(epfd, op, fd, &mut ev) };
}

pub fn epoll_add(epfd: RawFd, fd: RawFd, events: u32, tok: u64) {
    epoll_ctl(epfd, libc::EPOLL_CTL_ADD, fd, events, tok)
}

pub fn epoll_mod(epfd: RawFd, fd: RawFd, events: u32, tok: u64) {
    epoll_ctl(epfd, libc::EPOLL_CTL_MOD, fd, events, tok)
}

pub fn epoll_del(epfd: RawFd, fd: RawFd) {
    epoll_ctl(epfd, libc::EPOLL_CTL_DEL, fd, 0, 0)
}

/// Number of events filled (0 on EINTR or timeout).
pub fn epoll_wait(epfd: RawFd, events: &mut [EpollEvent], timeout: c_int) -> usize {
    // SAFETY: the kernel writes at most events.len() entries.
    let n = unsafe { libc::epoll_wait(epfd, events.as_mut_ptr(), events.len() as c_int, timeout) };
    n.max(0) as usize
}

/// read(2) up to `want` bytes appended to `buf`. Returns read(2)'s result.
pub fn read_append(fd: RawFd, buf: &mut Vec<u8>, want: usize) -> isize {
    buf.reserve(want);
    // SAFETY: reserve() made room for `want` bytes past len().
    let n = unsafe { libc::read(fd, buf.as_mut_ptr().add(buf.len()) as *mut c_void, want) };
    if n > 0 {
        // SAFETY: the kernel initialized n <= want bytes past len().
        unsafe { buf.set_len(buf.len() + n as usize) };
    }
    n
}

/// write(2). Returns write(2)'s result.
pub fn write(fd: RawFd, b: &[u8]) -> isize {
    // SAFETY: `b` is live and readable for its length.
    unsafe { libc::write(fd, b.as_ptr() as *const c_void, b.len()) }
}

/// writev(2) over `iov`. Returns writev(2)'s result.
pub fn writev(fd: RawFd, iov: &[libc::iovec]) -> isize {
    // SAFETY: the caller built `iov` from buffers that are live for the call.
    unsafe { libc::writev(fd, iov.as_ptr(), iov.len().min(libc::UIO_MAXIOV as usize) as c_int) }
}

/// accept4(2) a non-blocking, close-on-exec socket with TCP_NODELAY.
pub fn accept_nodelay(listen_fd: RawFd) -> Option<RawFd> {
    // SAFETY: NULL address arguments are allowed.
    let fd = unsafe {
        libc::accept4(listen_fd, std::ptr::null_mut(), std::ptr::null_mut(), libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
    };
    if fd < 0 {
        return None;
    }
    let one: c_int = 1;
    // SAFETY: passes a live c_int of the stated size.
    unsafe {
        libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, &one as *const c_int as *const c_void, 4);
    }
    Some(fd)
}

#[cfg(feature = "timing")]
pub fn monotonic_ns() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: writes into a live timespec.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

#[cfg(feature = "timing")]
#[inline(always)]
#[allow(unused_unsafe)]
pub fn rdtsc() -> u64 {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: rdtsc has no preconditions on x86_64.
    unsafe {
        core::arch::x86_64::_rdtsc()
    }
    #[cfg(not(target_arch = "x86_64"))]
    monotonic_ns()
}
