//! The FFI boundary: every `#[no_mangle]` export the engine calls, the raw
//! handle conversions, and the Send/Sync claims for the `#[repr(C)]` types.
//! Together with `sys.rs`, the only place allowed to contain `unsafe`.
#![allow(unsafe_code)]

mod exports;
pub use exports::*;

use crate::{precheck::VioSession, Global, VioCmd};

// SAFETY: a VioCmd's argv is private to the IO thread that built it until
// it is popped, and then owned by the engine thread; ownership moves with
// the value.
unsafe impl Send for VioCmd {}
// SAFETY: `user` is an identity key, never dereferenced by Rust.
unsafe impl Send for VioSession {}
// SAFETY: Global's raw pointers are the limit pointers (read only, see
// LimitPtrs::get) and the callbacks are thread-safe C functions.
unsafe impl Sync for Global {}
// SAFETY: as above.
unsafe impl Send for Global {}

/// Pointers into the engine's config the IO threads read live (plan step 9
/// replaces them with values pushed by the engine).
#[derive(Clone, Copy)]
pub(crate) struct LimitPtrs {
    pub max_bulk_len: *const i64,
    pub max_querybuf_len: *const usize,
}

impl LimitPtrs {
    pub(crate) fn max_bulk_len(&self) -> i64 {
        // SAFETY: vio_start's contract keeps the pointer valid for the
        // process lifetime. The engine writes it without atomics (CONFIG
        // SET); a volatile read of an aligned word is what the C side does.
        unsafe { std::ptr::read_volatile(self.max_bulk_len) }
    }
    pub(crate) fn max_querybuf_len(&self) -> usize {
        // SAFETY: as above.
        unsafe { std::ptr::read_volatile(self.max_querybuf_len) }
    }
}
