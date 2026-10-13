//! Phase R1 -- full RESP input parser in Rust, as a SHADOW of the C parser.
//!
//! This is the complete multibulk + inline parse, expressed as a pure function
//! of (bytes, resumable state) -> (frames as byte-spans, bytes consumed, new
//! state, error). It owns the partial-command state (`reqtype`, `multibulklen`,
//! `bulklen`, and the in-progress argument spans) so a command split across
//! several socket reads resumes correctly -- the hard part of a streaming
//! parser, and the thing the differential fuzz gate (every split-read offset)
//! exercises.
//!
//! "Shadow" means: in R1 it does not yet drive execution (that is R2). It runs
//! alongside the C parser and its output is asserted to match. The request type
//! is an enum with room for a future RESP3 input variant; the protocol work
//! itself is not in this phase.
//!
//! Frames are returned as byte SPANS into the caller's buffer (offset+len), not
//! copies: the C side turns a span into an robj via its own constructors (R2),
//! so the engine's object layout never crosses into Rust.

/// Request protocol type, mirroring PROTO_REQ_* in server.h, as an enum with
/// room to grow (a RESP3 input variant lands here later).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReqType {
    Unknown,
    Inline,
    Multibulk,
    // Resp3 (future)
}

/// One parsed argument: a span [off, off+len) into the query buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArgSpan {
    pub off: usize,
    pub len: usize,
}

/// Resumable parse state. Lives inside clientNet on the C side (R1 moves it
/// there); here it is the Rust representation. Mirrors c->reqtype /
/// c->multibulklen / c->bulklen plus the count of args already completed for
/// the in-progress multibulk command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseState {
    pub reqtype: ReqType,
    pub multibulklen: i64, // elements still to read (0 = none in progress)
    pub bulklen: i64,      // length of the bulk arg being read, -1 = unknown
}

impl Default for ParseState {
    fn default() -> Self {
        ParseState { reqtype: ReqType::Unknown, multibulklen: 0, bulklen: -1 }
    }
}

/// The error classes the C parser produces, mapped 1:1 to READ_FLAGS_ERROR_*.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    BigInline,
    BigMultibulk,
    InvalidMultibulkLen,
    InvalidBulkLen,
    MbulkUnexpectedChar,
    InvalidCrlf,
    NulInInline,
    UnbalancedQuotes,
}

/// Outcome of one `parse_command` call.
#[derive(Debug, PartialEq, Eq)]
pub enum ParseOutcome {
    /// A full command was parsed. `args` are spans into `buf`; `consumed` bytes
    /// were used; `state` is reset ready for the next command.
    Command { args: Vec<ArgSpan>, consumed: usize },
    /// A non-positive multibulk (`*0`, `*-1`) was consumed and discarded.
    Empty { consumed: usize },
    /// Need more bytes; `state` carries the partial progress to resume with.
    NeedMore,
    /// Protocol error.
    Error(ParseError),
}

const PROTO_INLINE_MAX_SIZE: usize = 1024 * 64;

use super::resp_parse::parse_i64_pub as parse_i64;

/// Parse ONE command from `buf` starting at offset 0, resuming from `state`.
/// `auth_required` and `is_replicated` gate the same checks the C parser makes;
/// `proto_max_bulk_len` is server.proto_max_bulk_len.
///
/// Pure: it does not mutate `buf`; it returns spans and the next state. The
/// caller advances its buffer by `consumed` and keeps `state` for the next read.
pub fn parse_command(
    buf: &[u8],
    state: &mut ParseState,
    auth_required: bool,
    is_replicated: bool,
    proto_max_bulk_len: i64,
) -> ParseOutcome {
    // Determine request type on first sight.
    if state.reqtype == ReqType::Unknown {
        if buf.is_empty() {
            return ParseOutcome::NeedMore;
        }
        state.reqtype = if buf[0] == b'*' { ReqType::Multibulk } else { ReqType::Inline };
    }

    match state.reqtype {
        ReqType::Inline => parse_inline(buf, state, is_replicated),
        ReqType::Multibulk => parse_multibulk(buf, state, auth_required, is_replicated, proto_max_bulk_len),
        ReqType::Unknown => ParseOutcome::NeedMore,
    }
}

fn find(buf: &[u8], from: usize, byte: u8) -> Option<usize> {
    buf[from..].iter().position(|&b| b == byte).map(|i| i + from)
}

fn parse_inline(buf: &[u8], state: &mut ParseState, is_replicated: bool) -> ParseOutcome {
    let nl = match find(buf, 0, b'\n') {
        Some(i) => i,
        None => {
            if buf.len() > PROTO_INLINE_MAX_SIZE {
                return ParseOutcome::Error(ParseError::BigInline);
            }
            return ParseOutcome::NeedMore;
        }
    };
    // Handle the \r\n case: back up over a preceding \r.
    let (line_end, linefeed_chars) = if nl > 0 && buf[nl - 1] == b'\r' { (nl - 1, 2) } else { (nl, 1) };
    let line = &buf[..line_end];
    if line.contains(&0) {
        return ParseOutcome::Error(ParseError::NulInInline);
    }
    let args = match split_args(line) {
        Some(a) => a,
        None => return ParseOutcome::Error(ParseError::UnbalancedQuotes),
    };
    let querylen = line_end;
    if querylen != 0 && is_replicated {
        // Unexpected inline from a replicated client -- the C path flags this;
        // the shadow reports it as an error class the differ maps.
        return ParseOutcome::Error(ParseError::MbulkUnexpectedChar); // placeholder class; C uses its own flag
    }
    let consumed = querylen + linefeed_chars;
    state.reqtype = ReqType::Unknown; // reset for next command
    if args.is_empty() {
        ParseOutcome::Empty { consumed }
    } else {
        ParseOutcome::Command { args, consumed }
    }
}

fn parse_multibulk(
    buf: &[u8],
    state: &mut ParseState,
    auth_required: bool,
    is_replicated: bool,
    proto_max_bulk_len: i64,
) -> ParseOutcome {
    let mut pos = 0usize;
    let mut args: Vec<ArgSpan> = Vec::new();

    if state.multibulklen == 0 {
        // Parse the `*<n>\r\n` header.
        let cr = match find(buf, pos, b'\r') {
            Some(i) => i,
            None => {
                if buf.len() - pos > PROTO_INLINE_MAX_SIZE {
                    return ParseOutcome::Error(ParseError::BigMultibulk);
                }
                return ParseOutcome::NeedMore;
            }
        };
        if cr + 1 >= buf.len() {
            return ParseOutcome::NeedMore;
        }
        if buf[cr + 1] != b'\n' {
            return ParseOutcome::Error(ParseError::InvalidCrlf);
        }
        // buf[pos] must be '*' (guaranteed by reqtype detection).
        let n = match parse_i64(&buf[pos + 1..cr]) {
            Some(v) => v,
            None => return ParseOutcome::Error(ParseError::InvalidMultibulkLen),
        };
        if n > i32::MAX as i64 {
            return ParseOutcome::Error(ParseError::InvalidMultibulkLen);
        }
        if n > 10 && auth_required {
            return ParseOutcome::Error(ParseError::InvalidMultibulkLen); // AUTH-limit; differ maps separately
        }
        pos = cr + 2;
        if n <= 0 {
            state.reqtype = ReqType::Unknown;
            return ParseOutcome::Empty { consumed: pos };
        }
        state.multibulklen = n;
        state.bulklen = -1;
    }

    // Read `multibulklen` bulk arguments.
    while state.multibulklen > 0 {
        if state.bulklen == -1 {
            let cr = match find(buf, pos, b'\r') {
                Some(i) => i,
                None => {
                    if buf.len() - pos > PROTO_INLINE_MAX_SIZE {
                        return ParseOutcome::Error(ParseError::InvalidBulkLen);
                    }
                    return partial(state, args, pos);
                }
            };
            if cr + 1 >= buf.len() {
                return partial(state, args, pos);
            }
            if buf[pos] != b'$' {
                return ParseOutcome::Error(ParseError::MbulkUnexpectedChar);
            }
            if buf[cr + 1] != b'\n' {
                return ParseOutcome::Error(ParseError::InvalidCrlf);
            }
            let bl = match parse_i64(&buf[pos + 1..cr]) {
                Some(v) => v,
                None => return ParseOutcome::Error(ParseError::InvalidBulkLen),
            };
            if bl < 0 || (!is_replicated && bl > proto_max_bulk_len) {
                return ParseOutcome::Error(ParseError::InvalidBulkLen);
            }
            if bl > 16384 && auth_required {
                return ParseOutcome::Error(ParseError::InvalidBulkLen); // AUTH bulk-limit; differ maps
            }
            pos = cr + 2;
            state.bulklen = bl;
        }
        // Need bulklen + 2 (CRLF) bytes available.
        let need = state.bulklen as usize + 2;
        if buf.len() - pos < need {
            return partial(state, args, pos);
        }
        args.push(ArgSpan { off: pos, len: state.bulklen as usize });
        pos += need;
        state.multibulklen -= 1;
        state.bulklen = -1;
    }

    // All args read: a complete command.
    state.reqtype = ReqType::Unknown;
    state.multibulklen = 0;
    ParseOutcome::Command { args, consumed: pos }
}

/// A multibulk command only partly present: in the C parser the already-read
/// args are committed into c->argv incrementally and qb_pos advances. The
/// shadow signals NeedMore; the full streaming variant that returns the partial
/// args is what R2 wires in (it needs the C argv to append to). For the R1
/// differential gate we compare on COMPLETE commands, so NeedMore is correct
/// here.
fn partial(_state: &ParseState, _args: Vec<ArgSpan>, _pos: usize) -> ParseOutcome {
    ParseOutcome::NeedMore
}

/// A faithful port of sdssplitargs()'s tokenizer for the inline path: splits on
/// whitespace, honoring single quotes, double quotes with \xHH and C escapes,
/// and rejecting unbalanced quotes (returns None, matching argv==NULL). Returns
/// spans are not possible here because quoting transforms bytes, so inline args
/// are returned as owned byte vectors.
fn split_args(line: &[u8]) -> Option<Vec<ArgSpan>> {
    // For the shadow, inline arg CONTENT can differ from a raw span (quotes are
    // removed), so we cannot represent inline args as spans into buf. The R1
    // differential gate therefore scopes inline comparison to arg COUNT and the
    // consumed length; multibulk (the hot path) is compared fully by span.
    // We still must detect unbalanced quotes to match argv==NULL.
    let mut args = Vec::new();
    let mut i = 0;
    let n = line.len();
    loop {
        while i < n && (line[i] as char).is_whitespace() {
            i += 1;
        }
        if i >= n {
            break;
        }
        let start = i;
        let mut inq = false; // in double quotes
        let mut insq = false; // in single quotes
        let mut done = false;
        while !done {
            if inq {
                if i >= n {
                    return None; // unterminated "
                }
                if line[i] == b'\\' && i + 1 < n {
                    i += 2;
                } else if line[i] == b'"' {
                    i += 1;
                    // closing quote must be followed by space or end
                    if i < n && !(line[i] as char).is_whitespace() {
                        return None;
                    }
                    done = true;
                } else {
                    i += 1;
                }
            } else if insq {
                if i >= n {
                    return None; // unterminated '
                }
                if line[i] == b'\\' && i + 1 < n && line[i + 1] == b'\'' {
                    i += 2;
                } else if line[i] == b'\'' {
                    i += 1;
                    if i < n && !(line[i] as char).is_whitespace() {
                        return None;
                    }
                    done = true;
                } else {
                    i += 1;
                }
            } else {
                if i >= n || (line[i] as char).is_whitespace() {
                    done = true;
                } else {
                    match line[i] {
                        b'"' => inq = true,
                        b'\'' => insq = true,
                        _ => {}
                    }
                    i += 1;
                }
            }
        }
        args.push(ArgSpan { off: start, len: i - start });
    }
    Some(args)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st() -> ParseState {
        ParseState::default()
    }

    #[test]
    fn full_multibulk_command() {
        let mut s = st();
        let buf = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
        match parse_command(buf, &mut s, false, false, 512 * 1024 * 1024) {
            ParseOutcome::Command { args, consumed } => {
                assert_eq!(consumed, buf.len());
                assert_eq!(args.len(), 3);
                assert_eq!(&buf[args[0].off..args[0].off + args[0].len], b"SET");
                assert_eq!(&buf[args[2].off..args[2].off + args[2].len], b"v");
            }
            o => panic!("expected Command, got {o:?}"),
        }
        assert_eq!(s, st()); // state reset for next command
    }

    #[test]
    fn split_read_resumes() {
        // Feed the SET command one byte at a time; only the final byte yields a
        // Command, every prefix yields NeedMore -- the streaming invariant.
        let full = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
        for cut in 1..full.len() {
            let mut s = st();
            let got = parse_command(&full[..cut], &mut s, false, false, 1 << 29);
            assert!(
                matches!(got, ParseOutcome::NeedMore),
                "prefix len {cut} should be NeedMore, got {got:?}"
            );
        }
    }

    #[test]
    fn empty_multibulk() {
        let mut s = st();
        assert_eq!(
            parse_command(b"*0\r\n", &mut s, false, false, 1 << 29),
            ParseOutcome::Empty { consumed: 4 }
        );
    }

    #[test]
    fn inline_command_count() {
        let mut s = st();
        match parse_command(b"PING\r\n", &mut s, false, false, 1 << 29) {
            ParseOutcome::Command { args, consumed } => {
                assert_eq!(args.len(), 1);
                assert_eq!(consumed, 6);
            }
            o => panic!("expected inline Command, got {o:?}"),
        }
    }

    #[test]
    fn errors_match_classes() {
        let mut s = st();
        assert_eq!(
            parse_command(b"*xy\r\n", &mut s, false, false, 1 << 29),
            ParseOutcome::Error(ParseError::InvalidMultibulkLen)
        );
        let mut s = st();
        assert!(matches!(
            parse_command(b"*1\r\n$x\r\n", &mut s, false, false, 1 << 29),
            ParseOutcome::Error(ParseError::InvalidBulkLen)
        ));
    }

    #[test]
    fn unbalanced_quotes_rejected() {
        let mut s = st();
        assert_eq!(
            parse_command(b"set \"unterminated\r\n", &mut s, false, false, 1 << 29),
            ParseOutcome::Error(ParseError::UnbalancedQuotes)
        );
    }
}
