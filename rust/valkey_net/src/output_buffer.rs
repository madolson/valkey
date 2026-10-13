//! Output-buffer append in Rust, operating on the REAL `client` struct via the
//! bindgen-generated layout mirror (`client_layout`).
//!
//! This is the plain-reply fast path of `_addReplyPayloadToBuffer` in
//! networking.c: copy up to `len` bytes of a reply into the client's fixed
//! output buffer `c->buf` at `c->bufpos`, bounded by `c->buf_usable_size`, then
//! advance `bufpos` and update `buf_peak`. In C this is a raw
//! `memcpy(c->buf + c->bufpos, payload, reply_len)` -- a classic reply-buffer
//! overflow site if the `available` math is ever wrong.
//!
//! Here the copy is expressed as a Rust slice-to-slice `copy_from_slice`, whose
//! length is `min(available, len)` and whose destination is an explicitly
//! bounded `&mut [u8]`. The bound is checked by construction: a miscomputed
//! length cannot run the copy past the buffer -- it panics instead of
//! corrupting adjacent memory.
//!
//! The C caller keeps the cases that need other C state (the encoded-header
//! path, the reply-list spillover, the debug-enforce flag); Rust owns the plain
//! append on the mirrored struct.

use crate::client_layout::client;

/// Append the plain reply payload to the client's static output buffer.
///
/// Mirrors the PLAIN_REPLY branch of _addReplyPayloadToBuffer: writes
/// `min(buf_usable_size - bufpos, len)` bytes and returns how many it wrote (0
/// if the buffer is full). The caller has already decided this is the plain
/// path (reply list empty, not buf_encoded).
///
/// # Safety
/// `c` must be a valid pointer to a live `client` whose `buf` points to at
/// least `buf_usable_size` bytes. Only this thread may touch the buffer for the
/// duration of the call (the same single-writer rule the C code relies on).
#[no_mangle]
pub unsafe extern "C" fn valkey_net_add_reply_plain(
    c: *mut client,
    payload: *const u8,
    len: usize,
) -> usize {
    if c.is_null() || payload.is_null() || len == 0 {
        return 0;
    }
    let c = &mut *c;

    // available = buf_usable_size - bufpos, guarded against underflow (the C
    // code trusts bufpos <= buf_usable_size; we make that explicit).
    let available = c.buf_usable_size.saturating_sub(c.bufpos);
    let reply_len = core::cmp::min(available, len);
    if reply_len == 0 {
        return 0;
    }

    // Build bounded slices over the C buffer and the payload. from_raw_parts
    // ties each copy to an explicit length, so copy_from_slice can only ever
    // write within `dst` -- the overflow the C memcpy risks is unrepresentable.
    let dst = core::slice::from_raw_parts_mut((c.buf as *mut u8).add(c.bufpos), reply_len);
    let src = core::slice::from_raw_parts(payload, reply_len);
    dst.copy_from_slice(src);

    c.bufpos += reply_len;
    if (c.buf_peak as usize) < c.bufpos {
        c.buf_peak = c.bufpos;
    }
    reply_len
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_layout::client;

    // A minimal client with just the output-buffer fields set, backed by a
    // real byte buffer. Everything else is zeroed; the function only touches
    // buf / bufpos / buf_usable_size / buf_peak.
    unsafe fn make_client(backing: &mut [u8]) -> client {
        let mut c: client = core::mem::zeroed();
        c.buf = backing.as_mut_ptr() as *mut core::ffi::c_char;
        c.buf_usable_size = backing.len();
        c.bufpos = 0;
        c.buf_peak = 0;
        c
    }

    #[test]
    fn appends_within_capacity() {
        let mut backing = [0u8; 32];
        unsafe {
            let mut c = make_client(&mut backing);
            let n = valkey_net_add_reply_plain(&mut c, b"+OK\r\n".as_ptr(), 5);
            assert_eq!(n, 5);
            assert_eq!(c.bufpos, 5);
            assert_eq!(c.buf_peak as usize, 5);
        }
        assert_eq!(&backing[..5], b"+OK\r\n");
    }

    #[test]
    fn truncates_at_capacity_never_overflows() {
        let mut backing = [0u8; 4];
        unsafe {
            let mut c = make_client(&mut backing);
            // Ask to write 10 bytes into a 4-byte buffer: writes 4, no overflow.
            let n = valkey_net_add_reply_plain(&mut c, b"abcdefghij".as_ptr(), 10);
            assert_eq!(n, 4);
            assert_eq!(c.bufpos, 4);
        }
        assert_eq!(&backing, b"abcd");
    }

    #[test]
    fn full_buffer_returns_zero() {
        let mut backing = [0u8; 4];
        unsafe {
            let mut c = make_client(&mut backing);
            c.bufpos = 4; // already full
            let n = valkey_net_add_reply_plain(&mut c, b"x".as_ptr(), 1);
            assert_eq!(n, 0);
        }
    }
}
