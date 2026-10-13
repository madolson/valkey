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

use core::ffi::{c_char, c_int, c_uint};

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
