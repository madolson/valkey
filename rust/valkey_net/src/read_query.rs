//! Phase R3 -- Rust owns the read into the query buffer.
//!
//! A faithful port of readToQueryBuf (networking.c): it decides the read
//! length (including the big-argument alignment that enables the zero-copy
//! path), grows the query buffer, performs the socket read, advances the
//! length, tracks querybuf_peak, and applies the query-buffer limit.
//!
//! The sds query buffer itself stays entirely in C -- Rust never decodes the
//! sds header. It drives sds through the vk_sds_* shims (sdslen/avail/alloc and
//! the two grow variants are static inline in sds.h, so the shims expose them
//! as real symbols) and reads/writes the client's own fields through the
//! bindgen layout mirror. C keeps the CLIENT LIST qbuf/qbuf-free accessors and
//! memory accounting; those read the same fields and are unchanged.

use crate::client_layout::client;
use core::ffi::{c_char, c_int, c_void};

const PROTO_IOBUF_LEN: usize = 1024 * 16;
const PROTO_MBULK_BIG_ARG: i64 = 1024 * 32;
const PROTO_REQ_MULTIBULK: c_int = 2;
const READ_FLAGS_QB_LIMIT_REACHED: c_int = 1 << 0;
const READ_FLAGS_AUTH_REQUIRED: c_int = 1 << 16;

#[repr(C)]
struct VkReadCtx {
    close_asap: c_int,
    is_replicated: c_int,
    replicated_client: c_int,
    max_querybuf_len: u64,
    mstate_argv_len_sums: u64,
}

extern "C" {
    fn vk_sdslen(s: *const c_char) -> usize;
    fn vk_sdsavail(s: *const c_char) -> usize;
    fn vk_sdsalloc(s: *const c_char) -> usize;
    fn vk_sds_make_room(s: *mut c_char, addlen: usize) -> *mut c_char;
    fn vk_sds_make_room_nongreedy(s: *mut c_char, addlen: usize) -> *mut c_char;
    fn vk_sds_incr_len(s: *mut c_char, incr: core::ffi::c_long);
    fn vk_sds_empty() -> *mut c_char;
    fn vk_thread_shared_qb() -> *mut c_char;
    fn vk_conn_read(conn: *mut c_void, buf: *mut c_char, len: usize) -> c_int;
    fn vk_read_ctx(c: *mut client, out: *mut VkReadCtx);
}

/// Read available bytes from the client's connection into its query buffer.
/// Returns true if the buffer was filled to the requested length (more may be
/// available), false on short read / EOF / error / nothing-to-do. Mirrors
/// readToQueryBuf exactly.
///
/// # Safety
/// `c` must be a valid live client. Single-reader for this client, same as the
/// C path (the IO-thread model guarantees it).
#[no_mangle]
pub unsafe extern "C" fn valkey_net_read_to_query_buf(c: *mut client) -> bool {
    let c = &mut *c;
    let mut ctx = VkReadCtx {
        close_asap: 0,
        is_replicated: 0,
        replicated_client: 0,
        max_querybuf_len: 0,
        mstate_argv_len_sums: 0,
    };
    vk_read_ctx(c, &mut ctx);

    if ctx.close_asap != 0 {
        c.nread = 0;
        return false;
    }
    let is_replicated = ctx.is_replicated != 0;

    let mut big_arg = false;
    let mut readlen: usize = PROTO_IOBUF_LEN;
    let mut qblen: usize = if c.querybuf.is_null() { 0 } else { vk_sdslen(c.querybuf) };

    // Big-arg alignment: when mid-way through a large bulk, size the read so the
    // buffer ends up holding exactly this arg (enables the zero-copy path).
    if c.reqtype == PROTO_REQ_MULTIBULK
        && c.multibulklen != 0
        && c.bulklen != -1
        && c.bulklen >= PROTO_MBULK_BIG_ARG
    {
        let remaining: isize = (c.bulklen as isize + 2) - (qblen as isize - c.qb_pos as isize);
        big_arg = true;
        if remaining > 0 {
            readlen = remaining as usize;
        }
        if ctx.replicated_client != 0 && readlen < PROTO_IOBUF_LEN {
            readlen = PROTO_IOBUF_LEN;
        }
    }

    if c.querybuf.is_null() {
        // Shared scratch buffer must be empty here (asserted in C); take it, or
        // a fresh empty sds for a big arg.
        c.querybuf = if big_arg { vk_sds_empty() } else { vk_thread_shared_qb() };
        qblen = vk_sdslen(c.querybuf);
    }

    let use_thread_shared = c.querybuf == vk_thread_shared_qb();
    if !is_replicated && (big_arg || vk_sdsalloc(c.querybuf) < PROTO_IOBUF_LEN) {
        c.querybuf = vk_sds_make_room_nongreedy(c.querybuf, readlen);
        if (c.querybuf_peak as usize) < qblen + readlen {
            c.querybuf_peak = qblen + readlen;
        }
    } else {
        c.querybuf = vk_sds_make_room(c.querybuf, readlen);
        readlen = vk_sdsavail(c.querybuf);
    }
    let _ = use_thread_shared; // parity with the C assert; no action needed

    let nread = vk_conn_read(c.conn as *mut c_void, c.querybuf.add(qblen), readlen);
    c.nread = nread;
    if nread <= 0 {
        return false;
    }

    vk_sds_incr_len(c.querybuf, nread as core::ffi::c_long);
    qblen = vk_sdslen(c.querybuf);
    if (c.querybuf_peak as usize) < qblen {
        c.querybuf_peak = qblen;
    }
    if !is_replicated {
        let qb_memory = qblen + ctx.mstate_argv_len_sums as usize;
        if qb_memory > ctx.max_querybuf_len as usize
            || (qb_memory > 1024 * 1024 && (c.read_flags & READ_FLAGS_AUTH_REQUIRED) != 0)
        {
            c.read_flags |= READ_FLAGS_QB_LIMIT_REACHED;
        }
    }
    (nread as usize) == readlen
}
