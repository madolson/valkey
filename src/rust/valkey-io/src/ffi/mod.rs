//! The FFI boundary: every `#[no_mangle]` export the engine calls, the raw
//! handle conversions, and the Send/Sync claims for the `#[repr(C)]` types.
//! Together with `sys.rs`, the only place allowed to contain `unsafe`.
#![allow(unsafe_code)]

mod exports;

use crate::resp::ProtoError;

/// Protocol error codes in VioCmd.err (resp::ProtoError; mirrors READ_FLAGS_ERROR_*).
#[allow(dead_code)]
pub const VIO_ERR_NONE: i32 = 0;
pub const VIO_ERR_BIG_INLINE: i32 = 1;
pub const VIO_ERR_BIG_MULTIBULK: i32 = 2;
pub const VIO_ERR_INVALID_MULTIBULK_LEN: i32 = 3;
pub const VIO_ERR_UNAUTH_MULTIBULK_LEN: i32 = 4;
pub const VIO_ERR_UNAUTH_BULK_LEN: i32 = 5;
pub const VIO_ERR_BIG_BULK_COUNT: i32 = 6;
pub const VIO_ERR_UNEXPECTED_CHAR: i32 = 7;
pub const VIO_ERR_INVALID_BULK_LEN: i32 = 8;
pub const VIO_ERR_UNBALANCED_QUOTES: i32 = 9;
pub const VIO_ERR_NUL_IN_INLINE: i32 = 10;
pub const VIO_ERR_INVALID_CRLF: i32 = 11;
const _: () = {
    assert!(ProtoError::BigInline as i32 == VIO_ERR_BIG_INLINE);
    assert!(ProtoError::BigMultibulk as i32 == VIO_ERR_BIG_MULTIBULK);
    assert!(ProtoError::InvalidMultibulkLen as i32 == VIO_ERR_INVALID_MULTIBULK_LEN);
    assert!(ProtoError::UnauthMultibulkLen as i32 == VIO_ERR_UNAUTH_MULTIBULK_LEN);
    assert!(ProtoError::UnauthBulkLen as i32 == VIO_ERR_UNAUTH_BULK_LEN);
    assert!(ProtoError::BigBulkCount as i32 == VIO_ERR_BIG_BULK_COUNT);
    assert!(ProtoError::UnexpectedChar as i32 == VIO_ERR_UNEXPECTED_CHAR);
    assert!(ProtoError::InvalidBulkLen as i32 == VIO_ERR_INVALID_BULK_LEN);
    assert!(ProtoError::UnbalancedQuotes as i32 == VIO_ERR_UNBALANCED_QUOTES);
    assert!(ProtoError::NulInInline as i32 == VIO_ERR_NUL_IN_INLINE);
    assert!(ProtoError::InvalidCrlf as i32 == VIO_ERR_INVALID_CRLF);
};
#[allow(unused_imports)]
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
// SAFETY: Global holds no raw pointers besides the callbacks, which are
// thread-safe C functions.
unsafe impl Sync for Global {}
// SAFETY: as above.
unsafe impl Send for Global {}

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
