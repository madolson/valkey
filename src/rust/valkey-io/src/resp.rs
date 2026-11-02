//! Incremental RESP request parser.
//!
//! Mirrors parseMultibulk() and parseInlineBuffer() in src/networking.c,
//! including their error precedence, so a client sees the same protocol
//! errors whether it is served by the C or the Rust IO layer.
//!
//! The parser never copies multibulk arguments. It returns byte ranges
//! relative to the start of the frame; the caller turns them into engine
//! objects and then discards `consumed` bytes. State is kept across calls, so
//! a frame split across many reads is not re-scanned from the start.

pub const PROTO_INLINE_MAX_SIZE: usize = 64 * 1024;

/// Protocol errors. Values are part of the FFI (see valkey_io.h).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ProtoError {
    BigInline = 1,
    BigMultibulk = 2,
    InvalidMultibulkLen = 3,
    UnauthMultibulkLen = 4,
    UnauthBulkLen = 5,
    BigBulkCount = 6,
    UnexpectedChar = 7,
    InvalidBulkLen = 8,
    UnbalancedQuotes = 9,
    NulInInline = 10,
    InvalidCrlf = 11,
}

pub struct Limits {
    pub max_bulk_len: i64,
    pub auth_required: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Arg {
    /// (offset, len) relative to the frame start.
    Range(usize, usize),
    /// Inline arguments are unescaped, so they own their bytes.
    Owned(Vec<u8>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Command { args: Vec<Arg>, consumed: usize, input_bytes: u64 },
    /// Empty inline line, blank inline line, or `*0` / `*-1`: skip bytes.
    Empty { consumed: usize },
    Incomplete,
    /// The byte is only meaningful for UnexpectedChar.
    Error(ProtoError, u8),
}

#[derive(Default)]
pub struct Parser {
    in_mb: bool,
    mb_left: i64,
    bulklen: i64,
    pos: usize,
    args: Vec<Arg>,
    input_bytes: u64,
}

fn memchr(b: u8, s: &[u8]) -> Option<usize> {
    s.iter().position(|&c| c == b)
}

/// Port of string2ll() (src/util.c): strict decimal, no '+', no leading zeros.
pub fn string2ll(s: &[u8]) -> Option<i64> {
    if s.is_empty() || s.len() >= 21 {
        return None;
    }
    if s.len() == 1 && s[0] == b'0' {
        return Some(0);
    }
    let (neg, mut i) = if s[0] == b'-' { (true, 1) } else { (false, 0) };
    if i >= s.len() || !(b'1'..=b'9').contains(&s[i]) {
        return None;
    }
    let mut v: u64 = 0;
    while i < s.len() {
        let c = s[i];
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((c - b'0') as u64)?;
        i += 1;
    }
    if neg {
        if v > (i64::MAX as u64) + 1 {
            return None;
        }
        Some((v as i64).wrapping_neg())
    } else {
        if v > i64::MAX as u64 {
            return None;
        }
        Some(v as i64)
    }
}

fn is_hex(c: u8) -> bool {
    c.is_ascii_hexdigit()
}

fn hex_val(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        _ => c - b'A' + 10,
    }
}

/// Port of sdsparsearg() (src/sds.c). Returns the argument and bytes used.
fn parse_arg(line: &[u8], start: usize) -> Option<(Vec<u8>, usize)> {
    let end = line.len();
    let mut p = start;
    if p >= end {
        return None;
    }
    let (mut inq, mut insq, mut done) = (false, false, false);
    let mut out = Vec::new();
    while !done {
        let mut nc: Option<u8> = None;
        let c = line[p];
        if inq {
            if p + 4 < end && c == b'\\' && line[p + 1] == b'x' && is_hex(line[p + 2]) && is_hex(line[p + 3]) {
                nc = Some(hex_val(line[p + 2]) * 16 + hex_val(line[p + 3]));
                p += 4;
            } else if p + 2 < end && c == b'\\' && line[p + 1] != 0 {
                nc = Some(match line[p + 1] {
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'b' => 8,
                    b'a' => 7,
                    o => o,
                });
                p += 2;
            } else if c == b'"' {
                inq = false;
                p += 1;
            } else {
                nc = Some(c);
                p += 1;
            }
        } else if insq {
            if p + 2 < end && c == b'\\' && line[p + 1] == b'\'' {
                nc = Some(b'\'');
                p += 2;
            } else if c == b'\'' {
                insq = false;
                p += 1;
            } else {
                nc = Some(c);
                p += 1;
            }
        } else {
            match c {
                b' ' | b'\n' | b'\r' | b'\t' | 0 => done = true,
                b'"' => inq = true,
                b'\'' => insq = true,
                _ => nc = Some(c),
            }
            if c != 0 {
                p += 1;
            }
        }
        if let Some(ch) = nc {
            out.push(ch);
        }
        if p >= end {
            if inq || insq {
                return None;
            }
            done = true;
        }
    }
    Some((out, p - start))
}

fn is_c_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// Port of sdsnsplitargs(). None means unbalanced quotes.
pub fn split_args(line: &[u8]) -> Option<Vec<Vec<u8>>> {
    let end = line.len();
    let mut p = 0;
    let mut args = Vec::new();
    while p < end && line[p] != 0 {
        while p < end && line[p] != 0 && is_c_space(line[p]) {
            p += 1;
        }
        if !(p < end && line[p] != 0) {
            break;
        }
        let (a, n) = parse_arg(line, p)?;
        p += n;
        args.push(a);
    }
    Some(args)
}

impl Parser {
    pub fn new() -> Self {
        Parser { bulklen: -1, ..Default::default() }
    }

    /// Minimum frame length needed before the next call can make progress.
    /// Lets the reader size one read() for a large bulk argument.
    pub fn needed(&self) -> usize {
        if self.in_mb && self.bulklen >= 0 {
            self.pos + self.bulklen as usize + 2
        } else {
            0
        }
    }

    fn reset(&mut self) {
        self.in_mb = false;
        self.mb_left = 0;
        self.bulklen = -1;
        self.pos = 0;
        self.input_bytes = 0;
    }

    /// `buf` must start at the beginning of the current frame and must be
    /// the same bytes (possibly extended) as the previous call's `buf`.
    pub fn parse(&mut self, buf: &[u8], lim: &Limits) -> Outcome {
        if !self.in_mb {
            if buf.is_empty() {
                return Outcome::Incomplete;
            }
            if buf[0] != b'*' {
                return self.parse_inline(buf);
            }
            let i = match memchr(b'\r', buf) {
                None => {
                    if buf.len() > PROTO_INLINE_MAX_SIZE {
                        return Outcome::Error(ProtoError::BigMultibulk, 0);
                    }
                    return Outcome::Incomplete;
                }
                Some(i) => i,
            };
            if i + 1 >= buf.len() {
                return Outcome::Incomplete;
            }
            if buf[i + 1] != b'\n' {
                return Outcome::Error(ProtoError::InvalidCrlf, 0);
            }
            let ll = match string2ll(&buf[1..i]) {
                Some(v) if v <= i32::MAX as i64 => v,
                _ => return Outcome::Error(ProtoError::InvalidMultibulkLen, 0),
            };
            if ll > 10 && lim.auth_required {
                return Outcome::Error(ProtoError::UnauthMultibulkLen, 0);
            }
            if ll <= 0 {
                return Outcome::Empty { consumed: i + 2 };
            }
            self.in_mb = true;
            self.mb_left = ll;
            self.bulklen = -1;
            self.pos = i + 2;
            self.args = Vec::with_capacity((ll as usize).min(1024));
            self.input_bytes = (i - 1) as u64 + 3;
        }

        while self.mb_left > 0 {
            if self.bulklen == -1 {
                let rest = &buf[self.pos..];
                let i = match memchr(b'\r', rest) {
                    None => {
                        if rest.len() > PROTO_INLINE_MAX_SIZE {
                            return Outcome::Error(ProtoError::BigBulkCount, 0);
                        }
                        return Outcome::Incomplete;
                    }
                    Some(i) => i,
                };
                if i + 1 >= rest.len() {
                    return Outcome::Incomplete;
                }
                if rest[0] != b'$' {
                    return Outcome::Error(ProtoError::UnexpectedChar, rest[0]);
                }
                if rest[i + 1] != b'\n' {
                    return Outcome::Error(ProtoError::InvalidCrlf, 0);
                }
                let ll = match string2ll(&rest[1..i]) {
                    Some(v) if v >= 0 && v <= lim.max_bulk_len => v,
                    _ => return Outcome::Error(ProtoError::InvalidBulkLen, 0),
                };
                if ll > 16384 && lim.auth_required {
                    return Outcome::Error(ProtoError::UnauthBulkLen, 0);
                }
                self.pos += i + 2;
                self.bulklen = ll;
                self.input_bytes += (i - 1) as u64 + 3;
            }
            let bl = self.bulklen as usize;
            if buf.len() - self.pos < bl + 2 {
                return Outcome::Incomplete;
            }
            if buf[self.pos + bl] != b'\r' || buf[self.pos + bl + 1] != b'\n' {
                return Outcome::Error(ProtoError::InvalidCrlf, 0);
            }
            self.args.push(Arg::Range(self.pos, bl));
            self.input_bytes += bl as u64 + 2;
            self.pos += bl + 2;
            self.bulklen = -1;
            self.mb_left -= 1;
        }

        let args = std::mem::take(&mut self.args);
        let consumed = self.pos;
        let input_bytes = self.input_bytes;
        self.reset();
        Outcome::Command { args, consumed, input_bytes }
    }

    fn parse_inline(&mut self, buf: &[u8]) -> Outcome {
        let nl = match memchr(b'\n', buf) {
            None => {
                if buf.len() > PROTO_INLINE_MAX_SIZE {
                    return Outcome::Error(ProtoError::BigInline, 0);
                }
                return Outcome::Incomplete;
            }
            Some(n) => n,
        };
        let (end, lf) = if nl > 0 && buf[nl - 1] == b'\r' { (nl - 1, 2) } else { (nl, 1) };
        let line = &buf[..end];
        if memchr(0, line).is_some() {
            return Outcome::Error(ProtoError::NulInInline, 0);
        }
        let parts = match split_args(line) {
            Some(p) => p,
            None => return Outcome::Error(ProtoError::UnbalancedQuotes, 0),
        };
        let consumed = end + lf;
        if parts.is_empty() {
            return Outcome::Empty { consumed };
        }
        let sum: usize = parts.iter().map(|a| a.len()).sum();
        let input_bytes = (sum + parts.len() - 1 + 2) as u64;
        Outcome::Command { args: parts.into_iter().map(Arg::Owned).collect(), consumed, input_bytes }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIM: Limits = Limits { max_bulk_len: 512 * 1024 * 1024, auth_required: false };

    fn materialize(buf: &[u8], args: &[Arg]) -> Vec<Vec<u8>> {
        args.iter()
            .map(|a| match a {
                Arg::Range(o, l) => buf[*o..*o + *l].to_vec(),
                Arg::Owned(v) => v.clone(),
            })
            .collect()
    }

    /// Feed `input` in two pieces split at `split`, returning parsed commands
    /// and the terminal outcome, the way the IO thread drives the parser.
    fn run(input: &[u8], split: usize, lim: &Limits) -> (Vec<Vec<Vec<u8>>>, Option<Outcome>) {
        let mut p = Parser::new();
        let mut buf: Vec<u8> = Vec::new();
        let mut cmds = Vec::new();
        for chunk in [&input[..split], &input[split..]] {
            buf.extend_from_slice(chunk);
            loop {
                match p.parse(&buf, lim) {
                    Outcome::Command { args, consumed, .. } => {
                        cmds.push(materialize(&buf, &args));
                        buf.drain(..consumed);
                    }
                    Outcome::Empty { consumed } => {
                        buf.drain(..consumed);
                    }
                    Outcome::Incomplete => break,
                    e @ Outcome::Error(..) => return (cmds, Some(e)),
                }
            }
        }
        (cmds, None)
    }

    fn v(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    #[test]
    fn multibulk_basic() {
        let mut p = Parser::new();
        let buf = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        match p.parse(buf, &LIM) {
            Outcome::Command { args, consumed, input_bytes } => {
                assert_eq!(materialize(buf, &args), v(&["SET", "key", "value"]));
                assert_eq!(consumed, buf.len());
                // Same accounting as the C parser: equals the frame length.
                assert_eq!(input_bytes, buf.len() as u64);
            }
            o => panic!("{:?}", o),
        }
    }

    #[test]
    fn multibulk_split_at_every_offset() {
        let input = b"*2\r\n$4\r\nPING\r\n$0\r\n\r\n*1\r\n$4\r\nPING\r\nPING inline\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$10\r\n0123456789\r\n";
        let want = vec![v(&["PING", ""]), v(&["PING"]), v(&["PING", "inline"]), v(&["SET", "k", "0123456789"])];
        for split in 0..=input.len() {
            let (cmds, err) = run(input, split, &LIM);
            assert!(err.is_none(), "split {}", split);
            assert_eq!(cmds, want, "split {}", split);
        }
    }

    #[test]
    fn byte_at_a_time() {
        let input = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let mut p = Parser::new();
        let mut buf = Vec::new();
        let mut got = None;
        for &b in input.iter() {
            buf.push(b);
            if let Outcome::Command { args, .. } = p.parse(&buf, &LIM) {
                got = Some(materialize(&buf, &args));
            }
        }
        assert_eq!(got.unwrap(), v(&["GET", "foo"]));
    }

    #[test]
    fn empty_and_negative() {
        let (cmds, err) = run(b"*0\r\n*-1\r\n\r\n   \r\n*1\r\n$4\r\nPING\r\n", 0, &LIM);
        assert!(err.is_none());
        assert_eq!(cmds, vec![v(&["PING"])]);
        assert_eq!(Parser::new().parse(b"", &LIM), Outcome::Incomplete);
    }

    #[test]
    fn inline_quotes() {
        let (cmds, err) = run(b"SET \"a b\" 'c\\'d' \"\\x41\\n\"\r\n", 3, &LIM);
        assert!(err.is_none());
        assert_eq!(cmds, vec![vec![b"SET".to_vec(), b"a b".to_vec(), b"c'd".to_vec(), b"A\n".to_vec()]]);
        let (_, err) = run(b"SET \"unterminated\r\n", 0, &LIM);
        assert_eq!(err, Some(Outcome::Error(ProtoError::UnbalancedQuotes, 0)));
        // A bare \n terminates inline commands too.
        let (cmds, _) = run(b"PING\nPING\n", 0, &LIM);
        assert_eq!(cmds, vec![v(&["PING"]), v(&["PING"])]);
    }

    #[test]
    fn inline_nul_and_big() {
        assert_eq!(run(b"PI\0NG\r\n", 0, &LIM).1, Some(Outcome::Error(ProtoError::NulInInline, 0)));
        let big = vec![b'a'; PROTO_INLINE_MAX_SIZE + 1];
        assert_eq!(Parser::new().parse(&big, &LIM), Outcome::Error(ProtoError::BigInline, 0));
        let mut bigmb = vec![b'*'];
        bigmb.extend(vec![b'1'; PROTO_INLINE_MAX_SIZE + 1]);
        assert_eq!(Parser::new().parse(&bigmb, &LIM), Outcome::Error(ProtoError::BigMultibulk, 0));
        let mut bigcount = b"*1\r\n$".to_vec();
        bigcount.extend(vec![b'1'; PROTO_INLINE_MAX_SIZE + 1]);
        assert_eq!(Parser::new().parse(&bigcount, &LIM), Outcome::Error(ProtoError::BigBulkCount, 0));
    }

    #[test]
    fn bad_lengths() {
        let e = |s: &[u8]| run(s, 0, &LIM).1;
        assert_eq!(e(b"*abc\r\n"), Some(Outcome::Error(ProtoError::InvalidMultibulkLen, 0)));
        assert_eq!(e(b"*3000000000\r\n"), Some(Outcome::Error(ProtoError::InvalidMultibulkLen, 0)));
        assert_eq!(e(b"*01\r\n"), Some(Outcome::Error(ProtoError::InvalidMultibulkLen, 0)));
        assert_eq!(e(b"*1\r\n$-1\r\n"), Some(Outcome::Error(ProtoError::InvalidBulkLen, 0)));
        assert_eq!(e(b"*1\r\n$x\r\n"), Some(Outcome::Error(ProtoError::InvalidBulkLen, 0)));
        assert_eq!(e(b"*1\r\nfoo\r\n"), Some(Outcome::Error(ProtoError::UnexpectedChar, b'f')));
        assert_eq!(e(b"*1\r\n$3\r\nfooXY"), Some(Outcome::Error(ProtoError::InvalidCrlf, 0)));
        assert_eq!(e(b"*1\rX"), Some(Outcome::Error(ProtoError::InvalidCrlf, 0)));
        assert_eq!(e(b"*1\r\n$3\rX"), Some(Outcome::Error(ProtoError::InvalidCrlf, 0)));
    }

    #[test]
    fn oversize_bulk() {
        let lim = Limits { max_bulk_len: 100, auth_required: false };
        assert_eq!(run(b"*1\r\n$101\r\n", 0, &lim).1, Some(Outcome::Error(ProtoError::InvalidBulkLen, 0)));
        assert!(run(b"*1\r\n$100\r\n", 0, &lim).1.is_none());
    }

    #[test]
    fn auth_limits() {
        let lim = Limits { max_bulk_len: 1 << 30, auth_required: true };
        assert_eq!(run(b"*11\r\n", 0, &lim).1, Some(Outcome::Error(ProtoError::UnauthMultibulkLen, 0)));
        assert_eq!(run(b"*1\r\n$16385\r\n", 0, &lim).1, Some(Outcome::Error(ProtoError::UnauthBulkLen, 0)));
    }

    #[test]
    fn nested_garbage() {
        // A nested array where a bulk is expected is an unexpected '*'.
        assert_eq!(run(b"*2\r\n*1\r\n$1\r\na\r\n", 0, &LIM).1, Some(Outcome::Error(ProtoError::UnexpectedChar, b'*')));
        // Garbage that happens to start with '*' is a bad multibulk length.
        assert_eq!(run(b"*\x01\x02\r\n", 0, &LIM).1, Some(Outcome::Error(ProtoError::InvalidMultibulkLen, 0)));
    }

    #[test]
    fn large_bulk_needed_hint() {
        let mut p = Parser::new();
        let mut buf = b"*2\r\n$3\r\nSET\r\n$1000000\r\n".to_vec();
        assert_eq!(p.parse(&buf, &LIM), Outcome::Incomplete);
        assert_eq!(p.needed(), buf.len() + 1000000 + 2);
        buf.extend(vec![b'x'; 1000000]);
        buf.extend(b"\r\n");
        match p.parse(&buf, &LIM) {
            Outcome::Command { args, consumed, .. } => {
                assert_eq!(consumed, buf.len());
                assert_eq!(args[1], Arg::Range(buf.len() - 1000002, 1000000));
            }
            o => panic!("{:?}", o),
        }
    }

    #[test]
    fn string2ll_matches_c() {
        assert_eq!(string2ll(b"0"), Some(0));
        assert_eq!(string2ll(b"-1"), Some(-1));
        assert_eq!(string2ll(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(string2ll(b"-9223372036854775808"), Some(i64::MIN));
        for bad in [&b""[..], b"-", b"-0", b"+1", b"01", b" 1", b"1 ", b"9223372036854775808"] {
            assert_eq!(string2ll(bad), None, "{:?}", bad);
        }
    }
}
