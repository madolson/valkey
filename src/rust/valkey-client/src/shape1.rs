//! Shape 1 (shared struct). Rust reads and writes `struct client` fields in
//! place through the bindgen view. Beat-for-beat port of
//! `parseInputBuffer` (networking.c:4519).
use crate::ffi::*;
use std::os::raw::c_int;

extern "C" {
    // Defined in networking.c but not declared in any header.
    fn parseInlineBuffer(c: *mut client);
    fn parseMultibulkBuffer(c: *mut client);
}

/// `clientConnPostponeMaskFromIOState` (server.h:1542) is `static inline`, so
/// bindgen cannot link it. Re-implemented here; it must track the C copy.
#[inline]
unsafe fn postpone_mask_from_io_state(c: *const client) -> c_int {
    let c = &*c;
    let mut mask = 0;
    // io_read_state is `volatile uint8_t` in C; bindgen drops `volatile`.
    if std::ptr::read_volatile(&c.io_read_state) != clientIOState::CLIENT_IDLE as u8 {
        mask |= CONN_POSTPONE_READ as c_int;
    }
    if std::ptr::read_volatile(&c.io_write_state) != clientIOState::CLIENT_IDLE as u8 {
        mask |= CONN_POSTPONE_WRITE as c_int;
    }
    if c.__bindgen_anon_1.flag.forkless_managed() != 0 {
        mask |= CONN_POSTPONE_WRITE as c_int;
    }
    mask
}

/// `connSetPostponeUpdateState` (connection.h:570) is also `static inline`.
/// Rust keeps the connection abstraction by dispatching through the same
/// `ConnectionType` vtable C uses. No C shim, no extra call.
#[inline]
unsafe fn conn_set_postpone_update_state(conn: *mut connection, mask: c_int) {
    if conn.is_null() {
        return;
    }
    if mask != 0 {
        (*conn).flags |= CONN_FLAG_POSTPONE_UPDATE_STATE as i16;
    } else {
        (*conn).flags &= !(CONN_FLAG_POSTPONE_UPDATE_STATE as i16);
    }
    let ty = (*conn).type_;
    if !ty.is_null() {
        if let Some(f) = (*ty).postpone_update_state {
            f(conn, mask);
        }
    }
}

#[cold]
unsafe fn assert_failed(e: &'static [u8], line: c_int) -> ! {
    _serverAssert(e.as_ptr().cast(), b"shape1.rs\0".as_ptr().cast(), line);
    std::process::abort()
}

/// # Safety
/// Main thread only, `c` a live client whose read side is not offloaded.
#[no_mangle]
pub unsafe extern "C" fn vc_parse_input_buffer(c: *mut client) {
    if (*c).cmd_queue.len != 0 {
        assert_failed(b"c->cmd_queue.len == 0\0", line!() as c_int);
    }
    let conn = (*c).conn;
    if !conn.is_null() {
        conn_set_postpone_update_state(conn, postpone_mask_from_io_state(c) | CONN_POSTPONE_READ as c_int);
    }
    if (*c).reqtype == 0 {
        // querybuf is an sds (char *); qb_pos indexes into it.
        let b = *(*c).querybuf.add((*c).qb_pos);
        (*c).reqtype = if b == b'*' as std::os::raw::c_char {
            PROTO_REQ_MULTIBULK as c_int
        } else {
            PROTO_REQ_INLINE as c_int
        };
    }
    match (*c).reqtype as u32 {
        PROTO_REQ_INLINE => parseInlineBuffer(c),
        PROTO_REQ_MULTIBULK => parseMultibulkBuffer(c),
        _ => {
            _serverPanic(b"shape1.rs\0".as_ptr().cast(), line!() as c_int, b"Unknown request type\0".as_ptr().cast());
            std::process::abort()
        }
    }
    if !conn.is_null() {
        conn_set_postpone_update_state(conn, postpone_mask_from_io_state(c));
    }
}
