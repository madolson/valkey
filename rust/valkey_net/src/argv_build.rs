//! Phase R2 -- the Rust parser builds argv via the C object constructors.
//!
//! Rust does NOT build robjs in place: the robj layout (embedded key/expire/
//! value) belongs to the engine, and copying it into Rust would couple the two
//! permanently. Instead Rust calls createStringObject(ptr,len) per argument, so
//! the only thing crossing the boundary per arg is a (ptr,len) pair in and an
//! opaque robj* out.
//!
//! This builds the normal (copying) argument path. The big-argument zero-copy
//! path (createObject(OBJ_STRING, sds) + querybuf hand-off) stays in C for now
//! because it mutates the query buffer, which Rust owns only in R3; moving it
//! belongs with that phase.
//!
//! R2's gate is the per-argument FFI cost, measured with an argument-heavy
//! command (MSET with many small args) -- see the harness bench and
//! RUST_DEVELOPMENT_NOTES.

use core::ffi::{c_char, c_void};

extern "C" {
    /// C: robj *createStringObject(const char *ptr, size_t len);
    fn createStringObject(ptr: *const c_char, len: usize) -> *mut c_void;
}

/// Build `n` argv entries from parsed spans by calling createStringObject per
/// argument. `querybuf` is the base of the client's query buffer; `offs[i]` /
/// `lens[i]` locate argument i within it. `argv_out` is a C-allocated array of
/// at least `n` robj* slots, filled in order. Returns argv_len_sum (the summed
/// argument byte length); argc is `n` on success.
///
/// # Safety
/// `querybuf` must cover every (off,len) span; `argv_out`, `offs`, `lens` must
/// each have room for `n` entries. Single-writer, same as the C arg loop.
#[no_mangle]
pub unsafe extern "C" fn valkey_net_build_argv(
    querybuf: *const c_char,
    offs: *const usize,
    lens: *const usize,
    n: usize,
    argv_out: *mut *mut c_void,
) -> usize {
    if querybuf.is_null() || argv_out.is_null() || n == 0 {
        return 0;
    }
    let offs = core::slice::from_raw_parts(offs, n);
    let lens = core::slice::from_raw_parts(lens, n);
    let argv = core::slice::from_raw_parts_mut(argv_out, n);
    let mut sum = 0usize;
    for i in 0..n {
        argv[i] = createStringObject(querybuf.add(offs[i]), lens[i]);
        sum += lens[i];
    }
    sum
}

/// Single-argument constructor hop: build ONE string object from a span by
/// calling createStringObject. This is the exact per-argument FFI boundary R2
/// measures -- the C multibulk loop calls it per arg instead of calling
/// createStringObject directly, so the MSET-100 microbench isolates the cost of
/// the Rust hop itself.
///
/// # Safety
/// `ptr` must point to `len` readable bytes.
#[no_mangle]
pub unsafe extern "C" fn valkey_net_make_string_arg(ptr: *const c_char, len: usize) -> *mut c_void {
    createStringObject(ptr, len)
}

#[cfg(test)]
mod tests {
    // The constructor is a C symbol not linked in the standalone crate test
    // binary, so this path is exercised by the harness Tcl/bench runs against
    // the full server, not by a crate unit test. The builder's span math is
    // covered by shadow_parser's tests (which produce the offs/lens).
}
