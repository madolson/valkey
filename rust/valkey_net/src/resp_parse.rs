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

/// Parse a base-10 integer with EXACTLY the acceptance rules of C's string2ll
/// (util.c:string2llScalar), so the Rust parser cannot diverge from the C one:
///   - empty, or length >= LONG_STR_SIZE (21), is rejected;
///   - a single '0'..='9' is accepted as itself;
///   - an optional leading '-', then a first digit that must be '1'..='9'
///     (so "00", "0123", "-0", "-01" are all rejected, matching C);
///   - every subsequent byte must be a digit;
///   - overflow is checked at every step against the unsigned accumulator,
///     with the final signed-range check (LLONG_MIN is representable).
/// Returns None for every input string2ll returns 0 for.
fn parse_i64(b: &[u8]) -> Option<i64> {
    const LONG_STR_SIZE: usize = 21; // mirrors util.h LONG_STR_SIZE
    let slen = b.len();
    if slen == 0 || slen >= LONG_STR_SIZE {
        return None;
    }
    // Special case: a single digit (handles lone "0" too).
    if slen == 1 && b[0].is_ascii_digit() {
        return Some((b[0] - b'0') as i64);
    }
    let mut p = b;
    let negative = p[0] == b'-';
    if negative {
        p = &p[1..];
        if p.is_empty() {
            return None; // lone "-"
        }
    }
    // First digit must be 1..=9 (rejects leading zero / "0..." multi-char).
    if !(b'1'..=b'9').contains(&p[0]) {
        return None;
    }
    let mut v: u64 = (p[0] - b'0') as u64;
    for &c in &p[1..] {
        if !c.is_ascii_digit() {
            return None;
        }
        if v > u64::MAX / 10 {
            return None; // overflow
        }
        v *= 10;
        let d = (c - b'0') as u64;
        if v > u64::MAX - d {
            return None; // overflow
        }
        v += d;
    }
    if negative {
        // -(LLONG_MIN+1)+1 == i64::MIN magnitude; v may equal that.
        let min_mag = (-(i64::MIN + 1)) as u64 + 1;
        if v > min_mag {
            return None;
        }
        Some((v as i128 * -1) as i64)
    } else {
        if v > i64::MAX as u64 {
            return None;
        }
        Some(v as i64)
    }
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

/* Status codes for the full-result FFI, mirroring the subset of READ_FLAGS_*
 * the multibulk-header branch of parseMultibulk() produces. The C side maps
 * these back to its own flag constants. */
pub const MBH_NEEDMORE: c_int = 0;
pub const MBH_PARSED: c_int = 1;
pub const MBH_NONPOSITIVE: c_int = 2;
pub const MBH_ERR_INVALID_LEN: c_int = 3; /* bad integer / over INT_MAX */
pub const MBH_ERR_INVALID_CRLF: c_int = 4; /* char after \r is not \n */
pub const MBH_ERR_BIG_MBULK: c_int = 5; /* no CRLF and over inline cap */

/// Full C-ABI result of parsing a multibulk header, enough for the live C read
/// path to replace its memchr + bounds + string2ll block with one call.
#[repr(C)]
pub struct MultibulkParse {
    pub status: c_int, /* one of the MBH_* codes */
    pub count: i64,    /* element count when status == MBH_PARSED */
    pub consumed: usize, /* bytes consumed when PARSED or NONPOSITIVE */
}

/// Parse a multibulk header for the live read path. `data`/`len` is the
/// unparsed query-buffer tail (starting at `*`). Pure function of the bytes;
/// the C caller applies client-state-dependent checks (auth limits) itself.
///
/// # Safety
/// `data` must point to `len` readable bytes, or be NULL with len 0.
#[no_mangle]
pub unsafe extern "C" fn valkey_net_parse_multibulk(data: *const u8, len: usize) -> MultibulkParse {
    let buf: &[u8] = if data.is_null() || len == 0 {
        &[]
    } else {
        core::slice::from_raw_parts(data, len)
    };
    // Distinguish the two "need more vs error" cases the C path separates.
    if buf.iter().position(|&b| b == b'\r').is_none() {
        let status = if buf.len() > PROTO_INLINE_MAX_SIZE { MBH_ERR_BIG_MBULK } else { MBH_NEEDMORE };
        return MultibulkParse { status, count: 0, consumed: 0 };
    }
    match parse_multibulk_header(buf) {
        MultibulkHeader::Parsed { count, consumed } => MultibulkParse { status: MBH_PARSED, count, consumed },
        MultibulkHeader::NonPositive { consumed } => MultibulkParse { status: MBH_NONPOSITIVE, count: 0, consumed },
        MultibulkHeader::NeedMore => MultibulkParse { status: MBH_NEEDMORE, count: 0, consumed: 0 },
        MultibulkHeader::Error => {
            // Decide which error: a non-\n after \r is INVALID_CRLF; otherwise
            // it is an invalid length (bad digits / overflow / leading char).
            let cr = buf.iter().position(|&b| b == b'\r').unwrap();
            let status = if cr + 1 < buf.len() && buf[cr + 1] != b'\n' {
                MBH_ERR_INVALID_CRLF
            } else {
                MBH_ERR_INVALID_LEN
            };
            MultibulkParse { status, count: 0, consumed: 0 }
        }
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

    #[test]
    fn string2ll_exact_edge_cases() {
        // Leading zeros are rejected by string2ll (except lone "0").
        assert_eq!(parse_i64(b"0"), Some(0));
        assert_eq!(parse_i64(b"00"), None);
        assert_eq!(parse_i64(b"0123"), None);
        assert_eq!(parse_i64(b"-0"), None);
        assert_eq!(parse_i64(b"-01"), None);
        // Lone minus and empty.
        assert_eq!(parse_i64(b"-"), None);
        assert_eq!(parse_i64(b""), None);
        // Normal values and the signed boundaries.
        assert_eq!(parse_i64(b"3"), Some(3));
        assert_eq!(parse_i64(b"-5"), Some(-5));
        assert_eq!(parse_i64(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_i64(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(parse_i64(b"9223372036854775808"), None); // MAX+1
        assert_eq!(parse_i64(b"-9223372036854775809"), None); // MIN-1
        // Length cap: >= 21 chars rejected like string2ll (slen >= LONG_STR_SIZE).
        assert_eq!(parse_i64(b"100000000000000000000"), None); // 21 digits
        // Non-digits.
        assert_eq!(parse_i64(b"1a"), None);
        assert_eq!(parse_i64(b"+5"), None);
    }
}
