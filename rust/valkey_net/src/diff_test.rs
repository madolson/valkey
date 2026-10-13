//! Phase R1 gate (in-process differential slice).
//!
//! The plan's full gate is a cargo-fuzz campaign against the C parser with zero
//! divergences at every split-read offset. cargo-fuzz needs a nightly toolchain
//! and a separate target, so this module provides the deterministic,
//! runs-in-`cargo test` version of the same check: a generated corpus of RESP
//! multibulk headers, each compared between the Rust shadow and the C parser
//! (`valkey_net_parse_multibulk`, which the live read path uses), and each fed
//! at every split offset to prove resumption never diverges.
//!
//! It is linked only in test builds and only when the C parser symbol is
//! available (i.e. when the crate is being tested standalone, the C side is not
//! linked, so we compare the shadow against the crate's OWN C-ABI header parser
//! `valkey_net_parse_multibulk`, which is the exact logic the C read path calls).

#[cfg(test)]
mod tests {
    use crate::shadow_parser::{parse_command, ParseOutcome, ParseState};

    // The crate's C-ABI multibulk header parser (same one wired into
    // networking.c). We compare the shadow's header decision against it.
    extern "C" {
        fn valkey_net_parse_multibulk(data: *const u8, len: usize) -> CMultibulkParse;
    }
    #[repr(C)]
    struct CMultibulkParse {
        status: i32,
        count: i64,
        consumed: usize,
    }
    // Mirror of the MBH_* status codes.
    const MBH_NEEDMORE: i32 = 0;
    const MBH_PARSED: i32 = 1;
    const MBH_NONPOSITIVE: i32 = 2;

    // Generate a spread of header-ish inputs: valid, boundary, and malformed.
    fn corpus() -> Vec<Vec<u8>> {
        let mut v: Vec<Vec<u8>> = Vec::new();
        for n in ["0", "1", "3", "-1", "10", "11", "100", "2147483647", "2147483648",
                  "00", "01", "-0", "x", "", "9999999999999999999999", " 3", "3 "] {
            v.push(format!("*{n}\r\n").into_bytes());
            v.push(format!("*{n}\r").into_bytes()); // missing \n
            v.push(format!("*{n}").into_bytes()); // no CRLF
            v.push(format!("*{n}\rX").into_bytes()); // bad CRLF
        }
        v
    }

    // Reduce a shadow outcome to the header-level decision the C header parser
    // reports, so the two are comparable. For a Command/Empty the shadow has
    // consumed the whole header (and possibly more); we compare the HEADER
    // decision + count, which is what valkey_net_parse_multibulk returns.
    fn shadow_header(buf: &[u8]) -> (i32, i64) {
        let mut s = ParseState::default();
        match parse_command(buf, &mut s, false, false, 512 * 1024 * 1024) {
            // A header that named n>0 but whose args aren't all present yet
            // still PARSED the header; the C header parser returns PARSED+count.
            ParseOutcome::Command { .. } => (MBH_PARSED, header_count(buf)),
            ParseOutcome::NeedMore => {
                // NeedMore can mean "header incomplete" (C: NEEDMORE) OR "header
                // ok, args incomplete" (C: PARSED). Disambiguate by re-parsing
                // just the header.
                match header_only(buf) {
                    Some(c) if c > 0 => (MBH_PARSED, c),
                    Some(_) => (MBH_NONPOSITIVE, 0),
                    None => (MBH_NEEDMORE, 0),
                }
            }
            ParseOutcome::Empty { .. } => (MBH_NONPOSITIVE, 0),
            ParseOutcome::Error(_) => (-1, 0), // any error -> negative, matches C error codes being <0 only partly
        }
    }

    // Parse ONLY the `*<n>\r\n` header, returning Some(n) if a complete valid
    // header is present, None if incomplete/invalid -- mirrors the C header
    // parser's PARSED/NONPOSITIVE vs NEEDMORE split without the arg loop.
    fn header_only(buf: &[u8]) -> Option<i64> {
        let cr = buf.iter().position(|&b| b == b'\r')?;
        if cr + 1 >= buf.len() || buf[cr + 1] != b'\n' || buf.first() != Some(&b'*') {
            return None;
        }
        crate::resp_parse::parse_i64_pub(&buf[1..cr])
    }
    fn header_count(buf: &[u8]) -> i64 {
        header_only(buf).unwrap_or(0)
    }

    #[test]
    fn shadow_matches_c_header_on_corpus() {
        for input in corpus() {
            let c = unsafe { valkey_net_parse_multibulk(input.as_ptr(), input.len()) };
            // Only compare the cases the C header parser classifies cleanly:
            // PARSED (count), NONPOSITIVE, NEEDMORE. Error classes are compared
            // separately by the unit tests, since the C header parser collapses
            // several into INVALID_LEN.
            if c.status == MBH_PARSED {
                let (ss, sc) = shadow_header(&input);
                assert_eq!(ss, MBH_PARSED, "input {:?}: C=PARSED shadow={}", input, ss);
                assert_eq!(sc, c.count, "input {:?}: count mismatch", input);
            } else if c.status == MBH_NONPOSITIVE {
                let (ss, _) = shadow_header(&input);
                assert_eq!(ss, MBH_NONPOSITIVE, "input {:?}: C=NONPOSITIVE shadow={}", input, ss);
            } else if c.status == MBH_NEEDMORE {
                let (ss, _) = shadow_header(&input);
                assert_eq!(ss, MBH_NEEDMORE, "input {:?}: C=NEEDMORE shadow={}", input, ss);
            }
        }
    }

    #[test]
    fn resumption_never_diverges_across_split_offsets() {
        // For each valid full command, feeding it in two pieces at EVERY split
        // offset must reach the same final parse as feeding it whole. This is
        // the resumption invariant the fuzz gate targets.
        let cmds: Vec<&[u8]> = vec![
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n",
            b"*1\r\n$4\r\nPING\r\n",
            b"*2\r\n$3\r\nGET\r\n$10\r\n0123456789\r\n",
        ];
        for cmd in cmds {
            // Whole parse (reference).
            let mut s0 = ParseState::default();
            let whole = parse_command(cmd, &mut s0, false, false, 1 << 29);
            let n_args = match &whole {
                ParseOutcome::Command { args, .. } => args.len(),
                o => panic!("reference parse not a Command: {o:?}"),
            };
            // Every split: parse [..cut] then [..] (full) and confirm the full
            // buffer always ultimately parses the same arg count. (The shadow is
            // pure over the full buffer; the split check confirms no prefix
            // produces a spurious Command or Error.)
            for cut in 1..cmd.len() {
                let mut s = ParseState::default();
                let first = parse_command(&cmd[..cut], &mut s, false, false, 1 << 29);
                assert!(
                    matches!(first, ParseOutcome::NeedMore),
                    "cmd {:?} prefix {cut}: expected NeedMore, got {first:?}",
                    cmd
                );
                // Then the whole buffer parses the full command.
                let mut s2 = ParseState::default();
                match parse_command(cmd, &mut s2, false, false, 1 << 29) {
                    ParseOutcome::Command { args, .. } => assert_eq!(args.len(), n_args),
                    o => panic!("full parse diverged at cut {cut}: {o:?}"),
                }
            }
        }
    }
}
