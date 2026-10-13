//! RESP input parsing in safe Rust -- a PoC slice that mirrors the multibulk
//! header logic of parseMultibulk() in networking.c, to demonstrate what Rust
//! changes about DEVELOPING this layer.
//!
//! The C version walks the query buffer with raw pointer arithmetic
//! (`newline - (c->querybuf + c->qb_pos)`, `c->querybuf + 1 + c->qb_pos`,
//! `(ssize_t)(sdslen - qb_pos - 2)`) and manual bounds checks. That arithmetic
//! is the historical home of RESP protocol CVEs: an off-by-one or a missing
//! length check reads or writes out of bounds, and nothing in C stops it at
//! compile time.
//!
//! This module does the same job over a `&[u8]` slice. Every read goes through
//! slice indexing or iterator APIs that are **bounds-checked by construction**:
//! an out-of-range access cannot compile as a silent pointer walk, and at
//! runtime it panics instead of reading foreign memory. The parser is a pure
//! function of its input (no `client` state), which is also why it is trivially
//! unit-testable without a running server -- another concrete dev win.

use core::ffi::c_int;

/// Outcome of parsing one multibulk header, mirroring the C READ_FLAGS_* set
/// the caller cares about. Kept as a small enum so the match is exhaustive --
/// the compiler forces every caller to handle every case, unlike the C `int`
/// flag soup where a forgotten branch is silent.
#[derive(Debug, PartialEq, Eq)]
pub enum MultibulkHeader {
    /// A complete `*<n>\r\n` header was parsed; `count` is the element count
    /// and `consumed` is how many bytes of the input it spanned.
    Parsed { count: i64, consumed: usize },
    /// Not enough bytes yet; the caller should read more and retry.
    NeedMore,
    /// `*<n>\r\n` where n <= 0 (empty/null multibulk).
    NonPositive { consumed: usize },
    /// Malformed: bad integer, missing/!= `\n` after `\r`, or over the cap.
    Error,
}

const PROTO_INLINE_MAX_SIZE: usize = 1024 * 64;

/// Parse a RESP multibulk header from the front of `buf` (which must begin at
/// the `*`). This is the safe-Rust counterpart of the `c->multibulklen == 0`
/// branch of parseMultibulk(). No raw pointers, no manual length subtraction
/// that can underflow: slice operations carry their own bounds.
pub fn parse_multibulk_header(buf: &[u8]) -> MultibulkHeader {
    // Find the CR. `iter().position` is bounds-safe and returns an Option, so
    // "not found" is a value we must handle, not a NULL we might deref.
    let cr = match buf.iter().position(|&b| b == b'\r') {
        Some(i) => i,
        None => {
            // No line yet. The C code caps the unparsed run at PROTO_INLINE_MAX_SIZE.
            if buf.len() > PROTO_INLINE_MAX_SIZE {
                return MultibulkHeader::Error;
            }
            return MultibulkHeader::NeedMore;
        }
    };

    // The buffer must also contain the \n after the \r. In C this is
    // `newline - (buf+pos) > (ssize_t)(len - pos - 2)` -- a signed subtraction
    // that underflows into a huge unsigned value if len < pos+2. Here it is a
    // plain bounds question the slice answers safely.
    if cr + 1 >= buf.len() {
        return MultibulkHeader::NeedMore;
    }
    if buf[cr + 1] != b'\n' {
        return MultibulkHeader::Error;
    }

    // Must start with '*'. buf[0] is only reached after we know buf is
    // non-empty (cr >= 0 implies at least one byte).
    if buf[0] != b'*' {
        return MultibulkHeader::Error;
    }

    // The digits are buf[1..cr]. This sub-slice is bounds-checked: if cr < 1
    // (i.e. "\r..." with no digits) the range is empty and parsing fails
    // cleanly rather than reading buf[1] past a 1-byte buffer.
    let digits = &buf[1..cr];
    let count = match parse_i64(digits) {
        Some(n) => n,
        None => return MultibulkHeader::Error,
    };
    if count > i32::MAX as i64 {
        return MultibulkHeader::Error;
    }

    let consumed = cr + 2; // past the \r\n
    if count <= 0 {
        return MultibulkHeader::NonPositive { consumed };
    }
    MultibulkHeader::Parsed { count, consumed }
}

/// Parse a base-10 i64 from ASCII bytes, like the subset of string2ll() this
/// path uses. Rejects empty input, non-digits, and anything with a sign other
/// than a leading '-'. Overflow is checked (checked_mul / checked_sub) rather
/// than wrapping silently as C's `ll*10` can.
fn parse_i64(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    let (neg, rest) = if b[0] == b'-' { (true, &b[1..]) } else { (false, b) };
    if rest.is_empty() {
        return None;
    }
    let mut acc: i64 = 0;
    for &c in rest {
        if !c.is_ascii_digit() {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_sub((c - b'0') as i64)?; // accumulate as negative to reach i64::MIN
    }
    Some(if neg { acc } else { acc.checked_neg()? })
}

/// C-ABI probe used by the PoC demonstration: parse a multibulk header from a
/// byte buffer and return the element count, or a negative sentinel for the
/// non-Parsed outcomes. Lets the C side exercise the safe parser and compare
/// its answer with parseMultibulk() on the same bytes.
///
/// # Safety
/// `data` must point to `len` readable bytes (or be NULL with len 0).
#[no_mangle]
pub unsafe extern "C" fn valkey_net_parse_multibulk_count(data: *const u8, len: usize) -> i64 {
    let buf: &[u8] = if data.is_null() || len == 0 {
        &[]
    } else {
        core::slice::from_raw_parts(data, len)
    };
    match parse_multibulk_header(buf) {
        MultibulkHeader::Parsed { count, .. } => count,
        MultibulkHeader::NonPositive { .. } => 0,
        MultibulkHeader::NeedMore => -1,
        MultibulkHeader::Error => -2,
    }
}

/// Exposed so the C demonstration can assert the Rust and C parsers agree on
/// how many bytes a header consumes.
///
/// # Safety
/// Same contract as `valkey_net_parse_multibulk_count`.
#[no_mangle]
pub unsafe extern "C" fn valkey_net_parse_multibulk_consumed(data: *const u8, len: usize) -> c_int {
    let buf: &[u8] = if data.is_null() || len == 0 {
        &[]
    } else {
        core::slice::from_raw_parts(data, len)
    };
    match parse_multibulk_header(buf) {
        MultibulkHeader::Parsed { consumed, .. } | MultibulkHeader::NonPositive { consumed } => consumed as c_int,
        _ => -1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_normal_header() {
        assert_eq!(
            parse_multibulk_header(b"*3\r\n$3\r\nSET\r\n"),
            MultibulkHeader::Parsed { count: 3, consumed: 4 }
        );
    }

    #[test]
    fn needs_more_without_crlf() {
        assert_eq!(parse_multibulk_header(b"*3"), MultibulkHeader::NeedMore);
        assert_eq!(parse_multibulk_header(b"*3\r"), MultibulkHeader::NeedMore);
    }

    #[test]
    fn rejects_bad_crlf_and_nondigits() {
        assert_eq!(parse_multibulk_header(b"*3\rX"), MultibulkHeader::Error);
        assert_eq!(parse_multibulk_header(b"*xy\r\n"), MultibulkHeader::Error);
    }

    #[test]
    fn handles_nonpositive() {
        assert_eq!(parse_multibulk_header(b"*0\r\n"), MultibulkHeader::NonPositive { consumed: 4 });
        assert_eq!(parse_multibulk_header(b"*-1\r\n"), MultibulkHeader::NonPositive { consumed: 5 });
    }

    #[test]
    fn empty_digits_do_not_read_out_of_bounds() {
        // "*\r\n": the digit slice buf[1..1] is empty. In C the equivalent is a
        // zero-length string2ll on buf+1; here it is a safe empty sub-slice.
        assert_eq!(parse_multibulk_header(b"*\r\n"), MultibulkHeader::Error);
    }

    #[test]
    fn rejects_overflow_instead_of_wrapping() {
        // A count that would overflow i64*10 is rejected, not silently wrapped.
        assert_eq!(parse_multibulk_header(b"*99999999999999999999\r\n"), MultibulkHeader::Error);
    }
}
