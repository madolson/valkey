//! Reply-append microbenchmark: the Rust side.
use std::os::raw::c_char;

/// Same layout as the C `cbuf` in bench.c: C owns the struct, Rust writes it
/// (shape 1/2 style: Rust touches C buffer fields in place).
#[repr(C)]
pub struct CBuf { pub buf: *mut c_char, pub pos: usize, pub cap: usize }

/// Append into a C-layout buffer, returning bytes that fit (rest spills).
#[no_mangle]
pub unsafe extern "C" fn rs_append_cbuf(b: *mut CBuf, p: *const c_char, len: usize) -> usize {
    let b = &mut *b;
    let avail = b.cap - b.pos;
    let n = if len < avail { len } else { avail };
    std::ptr::copy_nonoverlapping(p, b.buf.add(b.pos), n);
    b.pos += n;
    n
}

/// Rust-owned reply buffer behind an opaque handle (shape 3 style).
pub struct RsBuf { v: Vec<u8> }

#[no_mangle]
pub extern "C" fn rs_buf_new(cap: usize) -> *mut RsBuf {
    Box::into_raw(Box::new(RsBuf { v: Vec::with_capacity(cap) }))
}

#[no_mangle]
pub unsafe extern "C" fn rs_buf_append(b: *mut RsBuf, p: *const u8, len: usize) {
    let b = &mut *b;
    b.v.extend_from_slice(std::slice::from_raw_parts(p, len));
}

#[no_mangle]
pub unsafe extern "C" fn rs_buf_len(b: *const RsBuf) -> usize { (*b).v.len() }

#[no_mangle]
pub unsafe extern "C" fn rs_buf_clear(b: *mut RsBuf) { (*b).v.clear(); }
