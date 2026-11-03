# Iteration 1: toward a modular IO boundary, one measured step at a time

Plan: `~/rust-io/iter1/api-analysis.md` section 5 (steps referenced by number).
Base: `rust-io/integrate` 0f61a6fb9 + compat fixes B1 B2 B4 B5 (cherry-picked,
B3 dropped) + `rust-io/bench` (stage 6a harness). Stage 5a (`rust-io/harden`)
did not exist when this ran, so plan steps 4, 13 and 18 were treated as not
done.

Host: 32-core cloud desktop, kernel 5.10. Harness defaults: server on cores
0-3, valkey-benchmark on 4-7 (`--threads 4`), `-c 200 -d 64 -r 1000000`,
quick profile (c-io2, c-io4, io-t1, io-t3, precheck-t1, precheck-t3 x
SET/GET x -P 1/-P 16), 3 reps, median. Gates and builds ran on cores 8-31,
never during a measurement.

Significance: a delta counts only if it exceeds the per-cell spread of three
identical quick runs of the base binary on this host (`noise` below,
`compare.py`).

Metrics columns: exports = `#[no_mangle]` functions (timing-feature exports
in parentheses), cb = C callbacks in `VioCallbacks`, ops = entries in
engine-side function tables (`VioExecOps` before step 2, `externalConnOps`
after step 12), structs = `#[repr(C)]` structs, unsafe = `unsafe` tokens
outside `ffi/` and `sys.rs` (non-test code), statics = `static` items in the
crate (including thread-locals), ifdef = engine .c/.h files (excluding
`rust_io*`) containing `USE_RUST_IO`, bridge = lines in `rust_io.c` +
`rust_io_precheck.c` (+ `rust_io_exec.c`, `engine_io.c` when present),
clippy = `cargo clippy --all-targets` warnings.

| Step | Commit | Change | exports | cb | ops | structs | unsafe | statics | ifdef | bridge | clippy | Perf vs previous (key cells) | Gates | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| base | c2d8394a5 | integrate 0f61a6fb9 + B1 B2 B4 B5 + bench harness | 29 (+7) | 10 (5 + 5 `VioRtcOps`) | 29 (`VioExecOps`) | 9 | 45 | 15 | 8 | 1510 | 22 | noise study: 3 identical quick runs, per-cell spread of main ns/req median 1.6%, max 8.0%; RPS median 0.8%, max 6.2%; p99 median 1.7%, max 17.5% (`m/noise.csv`) | integrate's known failures (below) | reference |
| 2+3 (+4, 5, 6 measured together) | ad9ca654b, 32408225e | delete exec mode; drop the dead `vio_main_take_ready` export | 23 (+7) | 5 | 0 | 6 | 45 | 11 | 7 | 1029 | 22 | see the 4-6 row | see 12 | KEEP |
| 4+5 | 1cb9d7449, fixup 7aed6637c | unsafe exports with `# Safety`; `ffi/` + `sys.rs` hold all unsafe, crate root `deny(unsafe_code)`; Makefile runs clippy `-D warnings` | 23 (+7) | 5 | 0 | 6 | 0 | 11 | 7 | 1029 | 0 | | see 12 | KEEP |
| 6 | f35979a34 | `ConnHandle`/`ConnRef` newtypes; refcounts only in their impls | 23 (+7) | 5 | 0 | 6 | 0 | 11 | 7 | 1029 | 0 | steps 2-6 vs base: precheck-t1 GET P16 main 429 ns (-2.7%\*), precheck-t3 GET P16 429 ns (+0.1%), SET P16 575 ns (-0.3%), io-t3 GET P16 477 ns (-1.0%); RPS within noise everywhere at P16. First precheck-t3 pass was contaminated (foreign CPU 102-121%, client CPU 302% instead of 397%); rerun of those 4 cells used | see 12 | KEEP |
| 12 | f9ef53064 | ConnectionType "external" in `connection_external.c`, `rust_io.c` registers `externalConnOps` | 23 (+7) | 5 | 7 (`externalConnOps`) | 6 | 0 | 11 | 6 | 910 (+173 in `connection_external.c`) | 0 | vs 6: precheck-t1 GET P16 432 ns (+0.7%), precheck-t3 GET P16 426 ns (-0.8%), SET P16 565 ns (-1.7%\*), io-t3 GET P16 480 ns (+0.7%); RPS +0.7%, +1.6%\*, -0.9% | s12 binary, steps 2-12 batched: smoke io/precheck, scriptkill io/precheck, hazards PASS; Tcl io and precheck: only integrate's known failures | KEEP |
| 22 | bd3647af7 | write-job API (zero-copy plumbing), threshold -1 = copy | 25 (+7) | 6 | 7 | 6 | 0 | 12 | 6 | 1015 | 0 | vs 12 (copy mode): GET P16 main ns -4.5% to -6.7%\* in all four Rust configs (precheck-t3 405 ns, RPS +5.0%\*), SET P16 -1.4%\*. Not explained: with threshold -1 `rustIoTrySendWrite()` returns at its first check. Treated as a layout effect, not a win; it did not reverse in later steps | covered by the s19 run | KEEP |
| 13 | 3e41fec01 | `vio_conn_shutdown` defers shutdown(2) until the output is written | 25 (+7) | 6 | 7 | 6 | 0 | 12 | 6 | 1015 | 0 | hot path unchanged (one atomic load per kick); measured with 23 | killself-bgsave.py precheck, 5 iters: fork 5/5 lost before (s22 binary), 0/5 after; forkless 0/5 both; io 0/5 fork and forkless. unit/introspection "CLIENT KILL close the client connection during bgsave" now passes (introspection 121+exception -> 123/1) | KEEP (bug fix) |
| 23 | 0c9d22e9d | zero-copy on by default for writes of >= 2048 B | 25 (+7) | 6 | 7 | 6 | 0 | 12 | 6 | 1015 | 0 | quick (64 B, so every write < 2 KB is still copied) vs 22: P16 within noise in all Rust cells except precheck-t1 GET RPS +5.1%\*; reply-size sweep below | see 19 | KEEP |
