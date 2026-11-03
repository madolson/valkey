//! The FFI boundary: every `#[no_mangle]` export the engine calls, the raw
//! handle conversions, and the Send/Sync claims for the `#[repr(C)]` types.
//! Together with `sys.rs`, the only place allowed to contain `unsafe`.
#![allow(unsafe_code)]

mod exports;
pub use exports::*;

use crate::{precheck::VioSession, Conn, Global, VioCmd};
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::Arc;

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

/// The engine's strong reference to a connection (`const VioConn *` in C).
/// Created by vio_main_next_ready() and vio_conn_retain(), consumed by
/// vio_conn_release(). Every refcount change on the engine side happens in
/// this impl and in Drop.
#[repr(transparent)]
pub struct ConnHandle(NonNull<Conn>);

impl ConnHandle {
    pub(crate) fn new(c: Arc<Conn>) -> ConnHandle {
        // Arc::into_raw never returns NULL.
        ConnHandle(NonNull::new(Arc::into_raw(c) as *mut Conn).expect("Arc::into_raw"))
    }
}

impl Drop for ConnHandle {
    fn drop(&mut self) {
        // SAFETY: a ConnHandle owns one strong count, from Arc::into_raw.
        unsafe { Arc::decrement_strong_count(self.0.as_ptr()) };
    }
}

/// A connection borrowed for one FFI call: the engine holds a ConnHandle
/// for it for at least the duration of the call.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct ConnRef<'a>(NonNull<Conn>, PhantomData<&'a Conn>);

impl<'a> ConnRef<'a> {
    /// A new strong reference (vio_conn_retain).
    pub(crate) fn retain(self) -> ConnHandle {
        // SAFETY: the pointer came from Arc::into_raw and is alive (a
        // ConnHandle exists for it).
        unsafe { Arc::increment_strong_count(self.0.as_ptr()) };
        ConnHandle(self.0)
    }

    /// Borrow as an Arc without touching the refcount.
    pub(crate) fn arc(self) -> ManuallyDrop<Arc<Conn>> {
        // SAFETY: the pointer came from Arc::into_raw and is alive;
        // ManuallyDrop keeps this borrow from releasing it.
        ManuallyDrop::new(unsafe { Arc::from_raw(self.0.as_ptr()) })
    }
}

impl std::ops::Deref for ConnRef<'_> {
    type Target = Conn;
    fn deref(&self) -> &Conn {
        // SAFETY: alive for 'a, see ConnRef.
        unsafe { self.0.as_ref() }
    }
}
