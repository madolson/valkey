//! Shape 2 (embedded IO sub-struct). The read-side IO state is a
//! `#[repr(C)]` type defined here. cbindgen writes `src/client_io.h`, and
//! `struct client` embeds it as `c->io`. C keeps direct field access where it
//! must (replication, slot import, CLIENT LIST, crons); Rust gets a typed
//! `&mut ClientIoRead` instead of raw `client` offsets.
use std::os::raw::{c_char, c_int, c_long, c_ulonglong};

/// C's `sds` (`char *`). Declared in sds.h; excluded from the generated header.
pub type sds = *mut c_char;

/// Read and parse state of one connection (was server.h:1426-1438, 1492, 1500, 1514).
#[repr(C)]
pub struct ClientIoRead {
    /// Buffer we use to accumulate client queries.
    pub querybuf: sds,
    /// The position we have read in querybuf.
    pub qb_pos: usize,
    /// Right boundary of the current command in querybuf (replicated clients).
    pub qb_applied: usize,
    /// Recent (100ms or more) peak of querybuf size.
    pub querybuf_peak: usize,
    /// Length of bulk argument in multi bulk request.
    pub bulklen: c_long,
    /// Request protocol type: PROTO_REQ_*.
    pub reqtype: c_int,
    /// Number of multi bulk arguments left to read.
    pub multibulklen: c_int,
    /// Number of bytes of the last read.
    pub nread: c_int,
    /// Total network input bytes read from this client.
    pub net_input_bytes: c_ulonglong,
}

// PROTO_REQ_* live in server.h; reuse the bindgen copies.
const PROTO_REQ_INLINE: c_int = crate::ffi::PROTO_REQ_INLINE as c_int;
const PROTO_REQ_MULTIBULK: c_int = crate::ffi::PROTO_REQ_MULTIBULK as c_int;

impl ClientIoRead {
    /// Request-type detection from parseInputBuffer (networking.c:4529-4535).
    /// The only unsafe left is the sds read: querybuf is a C allocation.
    #[inline]
    pub fn detect_reqtype(&mut self) {
        if self.reqtype != 0 {
            return;
        }
        // SAFETY: parseInputBuffer is only called with qb_pos < sdslen(querybuf).
        let b = unsafe { *self.querybuf.add(self.qb_pos) } as u8;
        self.reqtype = if b == b'*' { PROTO_REQ_MULTIBULK } else { PROTO_REQ_INLINE };
    }
}

/// Exported so cbindgen emits the type; also a sanity hook for the C side.
#[no_mangle]
pub extern "C" fn vc_io_read_size() -> usize {
    std::mem::size_of::<ClientIoRead>()
}
