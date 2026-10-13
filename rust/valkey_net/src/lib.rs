//! Valkey networking layer — Rust implementation (Phase 2 stub).
//!
//! This file exists to establish and prove the C <-> Rust FFI/link path before
//! any real networking logic is ported. It exports a minimal, stable C ABI that
//! `networking.c` calls at startup; a non-trivial return value (not a constant
//! the C side could have produced itself) demonstrates the Rust code actually
//! ran.
//!
//! Everything here is `#[no_mangle] extern "C"` and uses only C-ABI types, so
//! the symbols are callable from C exactly as declared in `valkey_net.h`.

use core::ffi::{c_char, c_int, c_uint, c_void};

/// ABI version of this Rust networking layer. Bumped when the exported C ABI
/// changes so the C side can assert compatibility at startup.
const VALKEY_NET_ABI_VERSION: c_uint = 1;

/// Return the ABI version of the Rust networking layer.
///
/// Called from `networking.c` at init to prove the Rust static library is
/// linked and executing. Pure, no state, always safe to call.
#[no_mangle]
pub extern "C" fn valkey_net_abi_version() -> c_uint {
    VALKEY_NET_ABI_VERSION
}

/// Compute a trivial checksum of a NUL-terminated C string, in Rust.
///
/// This is a stand-in "do real work in Rust" entry point for the Phase 2 smoke
/// test: it reads a C string passed from C, does a byte computation Rust-side,
/// and returns it. It is deliberately simple and panic-free.
///
/// # Safety
/// `s` must be either NULL or a valid pointer to a NUL-terminated C string that
/// stays valid for the duration of the call. On NULL, returns -1.
#[no_mangle]
pub unsafe extern "C" fn valkey_net_cstr_checksum(s: *const c_char) -> c_int {
    if s.is_null() {
        return -1;
    }
    let mut sum: u32 = 0;
    let mut p = s;
    // Walk the C string until the NUL terminator.
    loop {
        let b = *p as u8;
        if b == 0 {
            break;
        }
        sum = sum.wrapping_add(b as u32);
        p = p.add(1);
    }
    (sum & 0x7fff_ffff) as c_int
}

// ---------------------------------------------------------------------------
// Socket byte-I/O leaf operations (Phase 3).
//
// These mirror the raw syscall core of connSocketRead/Write/Writev in
// socket.c. The Rust side performs the I/O and reports the raw syscall result
// plus the resulting errno; the thin C wrapper keeps ownership of the
// `connection` struct state transitions (and its debug asserts, which call
// C-only helpers). This is the deliberate FFI granularity for the port: Rust
// owns the I/O, C owns the struct policy.
//
// We declare the libc syscalls directly rather than pulling in the `libc`
// crate, to keep the static library dependency-free and the review small.
// ---------------------------------------------------------------------------

// iovec, matching struct iovec on Linux/*BSD (void* base, size_t len).
#[repr(C)]
pub struct ValkeyNetIovec {
    pub iov_base: *mut c_void,
    pub iov_len: usize,
}

/* Guard against drift from the platform `struct iovec`. On every OS valkey
 * targets, iovec is { void *iov_base; size_t iov_len; } == two pointer-sized
 * words with the pointer first, which is exactly this layout. If a future
 * target reorders or repacks iovec, this assertion fails at compile time
 * rather than silently passing writev a mis-shaped array. */
const _: () = {
    assert!(core::mem::size_of::<ValkeyNetIovec>() == 2 * core::mem::size_of::<usize>());
    assert!(core::mem::align_of::<ValkeyNetIovec>() == core::mem::align_of::<*mut c_void>());
};

extern "C" {
    fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize;
    fn writev(fd: c_int, iov: *const ValkeyNetIovec, iovcnt: c_int) -> isize;
}

/* The errno thread-local is reached through a different accessor symbol per
 * platform: __errno_location on Linux (glibc and musl both export it),
 * __error on Apple, and __errno on *BSD. Declare each under its own cfg so the
 * crate links on every platform valkey_server targets. */
#[cfg(target_os = "linux")]
extern "C" {
    #[link_name = "__errno_location"]
    fn errno_location() -> *mut c_int;
}
#[cfg(any(target_os = "macos", target_os = "ios"))]
extern "C" {
    #[link_name = "__error"]
    fn errno_location() -> *mut c_int;
}
#[cfg(any(target_os = "freebsd", target_os = "dragonfly", target_os = "openbsd", target_os = "netbsd"))]
extern "C" {
    #[link_name = "__errno"]
    fn errno_location() -> *mut c_int;
}

#[inline]
unsafe fn current_errno() -> c_int {
    *errno_location()
}

/// write(2) on `fd`. Returns the raw syscall result (bytes written, or <0 on
/// error). On a negative return, `*out_errno` is set to the errno value so the
/// C caller can apply its state-transition policy; on success it is set to 0.
///
/// # Safety
/// `data` must point to at least `data_len` readable bytes. `out_errno` must be
/// a valid pointer or NULL.
#[no_mangle]
pub unsafe extern "C" fn valkey_net_write(
    fd: c_int,
    data: *const c_void,
    data_len: usize,
    out_errno: *mut c_int,
) -> isize {
    let ret = write(fd, data, data_len);
    if !out_errno.is_null() {
        *out_errno = if ret < 0 { current_errno() } else { 0 };
    }
    ret
}

/// writev(2) on `fd`. See `valkey_net_write` for the return/errno contract.
///
/// # Safety
/// `iov` must point to `iovcnt` valid iovec entries. `out_errno` may be NULL.
#[no_mangle]
pub unsafe extern "C" fn valkey_net_writev(
    fd: c_int,
    iov: *const ValkeyNetIovec,
    iovcnt: c_int,
    out_errno: *mut c_int,
) -> isize {
    let ret = writev(fd, iov, iovcnt);
    if !out_errno.is_null() {
        *out_errno = if ret < 0 { current_errno() } else { 0 };
    }
    ret
}

/// read(2) on `fd`. Returns the raw syscall result: 0 means the peer closed,
/// <0 means error (with `*out_errno` set), >0 is the byte count.
///
/// # Safety
/// `buf` must point to at least `buf_len` writable bytes. `out_errno` may be
/// NULL.
#[no_mangle]
pub unsafe extern "C" fn valkey_net_read(
    fd: c_int,
    buf: *mut c_void,
    buf_len: usize,
    out_errno: *mut c_int,
) -> isize {
    let ret = read(fd, buf, buf_len);
    if !out_errno.is_null() {
        *out_errno = if ret < 0 { current_errno() } else { 0 };
    }
    ret
}
