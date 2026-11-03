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
| 23 fixup | 52be810dd | new payload headers start after the seal (ASAN soak finding) | 25 (+7) | 6 | 7 | 6 | 0 | 12 | 6 | 1021 | 0 | not separately measured (one pointer store per seal) | ASAN soak, zero-copy for every reply: before the fix the server asserted `ptr == buf + bufpos` in trackBufReferences() on an IO thread after about 40 s; after it, 600 s clean (below) | FIXED |
| 23 default off | 6fb1b388c | `rust-io-zero-copy-min-bytes` default back to -1 | 25 (+7) | 6 | 7 | 6 | 0 | 12 | 6 | 1021 | 0 | default build of the Rust modes is copy mode again; the quick profile is 64 B, so it is unaffected | zero-copy Tcl gates: unit/maxmemory 26/4 with zero-copy vs 28/2 in copy mode (below) | REVERTED to opt-in |
| 19 | ab094767c, fixup 604e1e2e3 | executed argv freed on the IO thread that built it, batched with the kicks; only with >= 2 IO threads | 26 (+7) | 6 | 7 | 6 | 0 | 13 | 6 | 1047 | 0 | freeClientArgv timer (RUST_IO_TIMING, precheck-t3 P16, every call): GET 56.4 -> 33.6 ns/call, SET 53.3 -> 37.1. Main-thread profile (precheck-t3 GET P16): "freeing argv" inclusive 14.0% -> 8.9%, jemalloc self 13.2% -> 4.4%. Quick vs 23, first build (offload at every thread count): GET P16 main ns -3.3% to -6.5%\*, but precheck-t1 GET P16 RPS -9.0%\* (the single IO thread is the bottleneck, main 44% busy). With the >= 2 threads rule: precheck-t3 GET P16 405 ns (-0.7%), SET 552 (-1.0%), precheck-t1 GET RPS -6.1% vs 23 but -1.3% vs 22 (23's +5.1% looks like an outlier). Prediction (-8% to -10% main ns/request) not met | s19 binary: smoke, scriptkill, hazards PASS; Tcl subset only integrate's known failures; introspection 123/1 | KEEP (gain smaller than predicted) |
| 9 | 1754f496a | engine limits by value; `vio_set_limits()` from the configs' apply functions | 26 (+7) | 6 | 7 | 6 | 0 | 13 | 6 | 1047 | 0 | measured with 19 (one Relaxed load instead of a volatile load per read) | CONFIG SET proto-max-bulk-len and client-query-buffer-limit over the Rust port, both directions; final gate run | KEEP |
| 15 | 9e5670340 | processCommand split into `processCommandChecks()` / `processCommandExecute()` | 26 (+7) | 6 | 7 | 6 | 0 | 13 | 6 | 1047 | 0 | measured with 19. Default build (no BUILD_RUST_IO) at HEAD, c-io cells vs the base binary's c-io cells (a BUILD_RUST_IO=yes binary run as C; no default-build base was measured): c-io2 P1 RPS -1.5%\* / -1.6%\* (threshold 1.0%), c-io2 P16 -0.5% / -0.4%, c-io4 P1 -0.3% / -0.1%, c-io4 P16 +8.6%\* / +3.8%\* RPS | default build (no Rust): unit/type/string 125/0, unit/multi 82/0, unit/acl 127/0 | KEEP |
| 7 | 784cc881a | `valkey_io.h` generated by cbindgen, `make check-rust-header` | 26 (+7) | 6 | 7 | 6 | 0 | 13 | 6 | 1047 | 0 | no codegen change; measured with 19 | header diff check passes | KEEP |

Known failures on the base (integrate) in this Tcl subset, unchanged throughout: unit/auth "primaryauth test with binary password dualchannel = yes" (RDB channel), unit/protocol "Partial command after blocking command is completed on unblock", unit/scripting "Test scripting debug lua stack overflow" plus the aborted forked-debugger file tail, unit/introspection "CONFIG SET rollback on apply error" (takeover). Before step 13 unit/introspection also aborted at "CLIENT KILL close the client connection during bgsave - fork".

Note on -P 1 cells with 3 IO threads: they are client-bound (client CPU 397%), the main thread is about 50% busy, and its ns/request moved by up to 15% between reruns of the same binary on this host (precheck-t3: 1590-2062 ns). They are not used for verdicts.

## Reply-size sweep (step 22 binary, copy = threshold -1, zc = threshold 0)

GET, value size x pipeline, `-c 200` (`-c 50` for 200 KB and 1 MB), 3 reps x 3 s. C io-threads 2/4 vs Rust precheck 1/3 IO threads. CSV: `m/sweep-s22.csv`.

| size | P | config | RPS | main ns/req | p99 ms | IO-thread CPU % | main CPU % |
|---|---|---|---|---|---|---|---|
| 64 | 1 | c-io2 | 91606 | 10919 | 2.34 | 100 | 100 |
| 64 | 1 | c-io4 | 274188 | 3639 | 0.66 | 300 | 100 |
| 64 | 1 | precheck-t1/copy | 91848 | 1060 | 3.28 | 100 | 10 |
| 64 | 1 | precheck-t1/zc | 94382 | 1042 | 2.29 | 100 | 10 |
| 64 | 1 | precheck-t3/copy | 274505 | 1669 | 0.70 | 297 | 46 |
| 64 | 1 | precheck-t3/zc | 274865 | 1966 | 0.57 | 296 | 54 |
| 64 | 16 | c-io2 | 1176814 | 850 | 2.84 | 100 | 100 |
| 64 | 16 | c-io4 | 2809445 | 356 | 1.70 | 300 | 100 |
| 64 | 16 | precheck-t1/copy | 1168451 | 372 | 3.61 | 100 | 43 |
| 64 | 16 | precheck-t1/zc | 1196263 | 370 | 2.80 | 100 | 44 |
| 64 | 16 | precheck-t3/copy | 2789814 | 356 | 1.87 | 242 | 99 |
| 64 | 16 | precheck-t3/zc | 2787287 | 359 | 1.63 | 237 | 100 |
| 1024 | 1 | c-io2 | 91234 | 10964 | 2.37 | 100 | 100 |
| 1024 | 1 | c-io4 | 274011 | 3643 | 0.61 | 300 | 100 |
| 1024 | 1 | precheck-t1/copy | 91583 | 1380 | 3.27 | 100 | 13 |
| 1024 | 1 | precheck-t1/zc | 93411 | 1364 | 2.30 | 100 | 13 |
| 1024 | 1 | precheck-t3/copy | 274746 | 1924 | 0.72 | 297 | 53 |
| 1024 | 1 | precheck-t3/zc | 278984 | 1938 | 0.61 | 297 | 54 |
| 1024 | 16 | c-io2 | 1066001 | 939 | 3.10 | 100 | 100 |
| 1024 | 16 | c-io4 | 1697337 | 590 | 2.13 | 300 | 100 |
| 1024 | 16 | precheck-t1/copy | 1032668 | 628 | 4.10 | 100 | 65 |
| 1024 | 16 | precheck-t1/zc | 1054370 | 591 | 3.19 | 100 | 62 |
| 1024 | 16 | precheck-t3/copy | 1572945 | 637 | 2.55 | 154 | 100 |
| 1024 | 16 | precheck-t3/zc | 1677430 | 597 | 2.32 | 162 | 100 |
| 16384 | 1 | c-io2 | 77363 | 12919 | 2.75 | 100 | 100 |
| 16384 | 1 | c-io4 | 241358 | 4120 | 1.12 | 300 | 100 |
| 16384 | 1 | precheck-t1/copy | 76430 | 2544 | 3.87 | 100 | 19 |
| 16384 | 1 | precheck-t1/zc | 74799 | 1084 | 2.81 | 100 | 8 |
| 16384 | 1 | precheck-t3/copy | 238830 | 2906 | 1.40 | 297 | 69 |
| 16384 | 1 | precheck-t3/zc | 227032 | 1470 | 1.32 | 298 | 33 |
| 16384 | 16 | c-io2 | 186957 | 5337 | 13.33 | 100 | 100 |
| 16384 | 16 | c-io4 | 287163 | 3480 | 8.53 | 300 | 100 |
| 16384 | 16 | precheck-t1/copy | 257153 | 2005 | 16.83 | 100 | 52 |
| 16384 | 16 | precheck-t1/zc | 241327 | 403 | 13.89 | 100 | 10 |
| 16384 | 16 | precheck-t3/copy | 493980 | 2026 | 10.20 | 189 | 100 |
| 16384 | 16 | precheck-t3/zc | 741626 | 416 | 4.41 | 299 | 31 |
| 204800 | 1 | c-io2 | 20034 | 49915 | 2.69 | 100 | 100 |
| 204800 | 1 | c-io4 | 62280 | 16081 | 0.85 | 300 | 100 |
| 204800 | 1 | precheck-t1/copy | 20763 | 12410 | 3.77 | 96 | 26 |
| 204800 | 1 | precheck-t1/zc | 19851 | 1890 | 2.71 | 100 | 4 |
| 204800 | 1 | precheck-t3/copy | 51349 | 13979 | 1.66 | 229 | 72 |
| 204800 | 1 | precheck-t3/zc | 60236 | 2986 | 0.95 | 295 | 18 |
| 204800 | 16 | c-io2 | 33820 | 29605 | 24.18 | 100 | 100 |
| 204800 | 16 | c-io4 | 93968 | 10635 | 8.33 | 299 | 100 |
| 204800 | 16 | precheck-t1/copy | 29266 | 17214 | 15.69 | 97 | 50 |
| 204800 | 16 | precheck-t1/zc | 32795 | 500 | 25.01 | 100 | 2 |
| 204800 | 16 | precheck-t3/copy | 49609 | 19954 | 8.49 | 177 | 99 |
| 204800 | 16 | precheck-t3/zc | 93979 | 580 | 8.56 | 298 | 5 |
| 1048576 | 1 | c-io2 | 7049 | 141881 | 7.37 | 100 | 100 |
| 1048576 | 1 | c-io4 | 12162 | 82079 | 3.60 | 300 | 100 |
| 1048576 | 1 | precheck-t1/copy | 4408 | 112828 | 10.99 | 83 | 50 |
| 1048576 | 1 | precheck-t1/zc | 7039 | 2534 | 7.45 | 100 | 2 |
| 1048576 | 1 | precheck-t3/copy | 6278 | 141862 | 8.11 | 168 | 89 |
| 1048576 | 1 | precheck-t3/zc | 12561 | 8714 | 3.71 | 292 | 11 |
| 1048576 | 16 | c-io2 | 7618 | 131331 | 104.70 | 99 | 100 |
| 1048576 | 16 | c-io4 | 8044 | 114702 | 30.61 | 295 | 93 |
| 1048576 | 16 | precheck-t1/copy | 5579 | 111402 | 18.59 | 100 | 62 |
| 1048576 | 16 | precheck-t1/zc | 7615 | 968 | 107.14 | 99 | 1 |
| 1048576 | 16 | precheck-t3/copy | 7582 | 131576 | 10.52 | 160 | 100 |
| 1048576 | 16 | precheck-t3/zc | 8041 | 12091 | 18.96 | 222 | 10 |


```
zero-copy vs copy, main ns/req delta and RPS delta
      64 P1  precheck-t1    main   -1.7%  rps   +2.8%  p99  -30.3%
      64 P1  precheck-t3    main  +17.8%  rps   +0.1%  p99  -19.3%
      64 P16 precheck-t1    main   -0.5%  rps   +2.4%  p99  -22.4%
      64 P16 precheck-t3    main   +0.8%  rps   -0.1%  p99  -12.8%
    1024 P1  precheck-t1    main   -1.2%  rps   +2.0%  p99  -29.6%
    1024 P1  precheck-t3    main   +0.7%  rps   +1.5%  p99  -14.5%
    1024 P16 precheck-t1    main   -5.9%  rps   +2.1%  p99  -22.2%
    1024 P16 precheck-t3    main   -6.3%  rps   +6.6%  p99   -9.1%
   16384 P1  precheck-t1    main  -57.4%  rps   -2.1%  p99  -27.5%
   16384 P1  precheck-t3    main  -49.4%  rps   -4.9%  p99   -5.7%
   16384 P16 precheck-t1    main  -79.9%  rps   -6.2%  p99  -17.4%
   16384 P16 precheck-t3    main  -79.4%  rps  +50.1%  p99  -56.8%
  204800 P1  precheck-t1    main  -84.8%  rps   -4.4%  p99  -28.0%
  204800 P1  precheck-t3    main  -78.6%  rps  +17.3%  p99  -42.5%
  204800 P16 precheck-t1    main  -97.1%  rps  +12.1%  p99  +59.4%
  204800 P16 precheck-t3    main  -97.1%  rps  +89.4%  p99   +0.8%
 1048576 P1  precheck-t1    main  -97.8%  rps  +59.7%  p99  -32.2%
 1048576 P1  precheck-t3    main  -93.9%  rps +100.1%  p99  -54.2%
 1048576 P16 precheck-t1    main  -99.1%  rps  +36.5%  p99 +476.3%
 1048576 P16 precheck-t3    main  -90.8%  rps   +6.0%  p99  +80.2%
```

Small-write curve at precheck-t3 (main ns/request, copy -> zc): 64 B P1 1590/1598 -> 1855/1892 (two reruns, +17%), 256 B P1 1691 -> 1910 (+13%), 512 B P1 1871 -> 1958 (+5%), 1 KB P1 1878 -> 2040 (+9%); P16: 64 B 358 -> 358-361, 256 B 464 -> 460, 512 B 557 -> 540, 1 KB 632 -> 593 (RPS +6.4%). The job's round trip is a fixed cost per write; break-even is about 1.2 KB per write, hence the recommended threshold 2048 (bytes of one write, so 16 pipelined 128 B replies already take the job).

Large values: at 200 KB and 1 MB the copy path is the main thread (`connRustWritev` memcpy 93.1% of main-thread samples at 200 KB -P 16, profile `m/prof/precheck-t3-main-get-P16-d200k-zc-1`); with zero-copy the main thread is nearly idle (267 samples in 6 s against 5860, connRustWritev absent) and the IO threads become the limit (IO CPU 298% of 300%). p99 at 1 MB -P 16 rises (precheck-t1 18.6 -> 107 ms) because copy mode's 1 MB per-connection Rust buffer limit no longer throttles the clients; C io-threads 2 shows the same 104.7 ms.

Stage timer "reply_copied" (handed to Rust) at 64 B, precheck-t3 P16, RUST_IO_TIMING build: GET p50 327681 -> 302475 ns, SET 453712 -> 453712 (the histogram buckets are powers of two, so only bucket moves show).

## Zero-copy safety gates

- ASAN churn soak, 600 s, precheck, 4 IO threads, `rust-io-zero-copy-min-bytes 0` (every reply through a job), `~/rust-io/compat/bin/soak.py` (output-buffer kills, mid-reply closes with and without RST, CLIENT KILL loops, BLPOP kills, kill with argv in flight): first run asserted after about 40 s (fixed in 52be810dd); rerun: no ASAN or LeakSanitizer report, server exit 0, `rust_io_writes_submitted:618105`, `rust_io_writes_outstanding:0` after the load stopped. The soak's final PING probe reported not alive while its 10000 never-reading clients were still connected (`rejected_connections:304359`: maxclients, admission control is not in this tree); INFO and SHUTDOWN answered right after.
- Tcl, precheck and io, takeover, zero-copy 0 vs copy (s12): unit/obuf-limits 13/0 both, unit/replybufsize 1/0 both, unit/type/string 125/0 both, unit/querybuf 0/3 both (Rust-held input), integration/replication 48/3 both (dual-channel RDB), unit/introspection 81/1 both (CONFIG sanity under takeover). unit/maxmemory: copy 28/2, zero-copy 26/4 (adds "output buffers of many MGET clients, client eviction: true" and "client tracking don't cause eviction feedback loop"). unit/client-eviction (input-side tests skipped): copy 8/8, zero-copy (default 2048) 9/7, adding "BLPOP + maxmemory-clients: successor eviction during ready-key reexec". So 1 of the 13 MISSING-FEATURE client-eviction/maxmemory tests passes with zero-copy, and 2 maxmemory tests regress; that is why zero-copy stays opt-in.

## Final vs base (shipped binary, `m/quick-head2.csv` vs `m/base-median.csv`, -P 16)

| cell | main ns/req | RPS | p99 |
|---|---|---|---|
| io-t1 GET | 456.5 (-4.5%\*) | +0.2% | -1.3% |
| io-t1 SET | 582.5 (-6.8%\*) | -0.1% | -0.2% |
| io-t3 GET | 438.6 (-9.0%\*) | +9.9%\* | -1.6%\* |
| io-t3 SET | 620.6 (-0.2%) | +0.3% | +1.0% |
| precheck-t1 GET | 419.1 (-5.1%\*) | +0.4% | +0.0% |
| precheck-t1 SET | 569.0 (-2.5%\*) | -0.8% | -0.2% |
| precheck-t3 GET | 400.1 (-6.7%\*) | +7.3%\* | +4.1%\* |
| precheck-t3 SET | 549.3 (-4.7%\*) | +5.1%\* | -3.5%\* |

Micro-benchmarks (criterion, same host, base commit vs HEAD): every bench within ±2.5% except `syscall/tcp_loopback_write_read_64B` (-4.9%, kernel path). See `~/rust-io/iter1/report.md`.

# Iteration 2 (stage 6c, branch rust-io/iter2)

Base: `rust-io/iter1` a0d9c428b + `rust-io/bench` (refined threshold, full
baseline) + `rust-io/harden` (merged by intent, below). Same host, same
harness and core layout as iteration 1 (server 0-3, client 4-7), same noise
file (`~/rust-io/iter1/m/noise.csv`). Per-step quick profile: the four Rust
configs only (the C cells do not run changed code), against the previous
step. Raw data: `~/rust-io/iter2/m/`, gates `~/rust-io/iter2/gates/<label>/`,
profiles `~/rust-io/iter2/prof/`, stage timers `~/rust-io/iter2/m/timing-<label>/`.
Predictions were written to `~/rust-io/iter2/notes.md` before each run.

Metrics columns as in iteration 1, but counted by one script
(`~/rust-io/iter2/metrics.sh`) for every row, so they differ from
iteration 1's hand counts: exports = all `#[no_mangle]` fns minus the 7
timing ones; unsafe = `unsafe` tokens outside `ffi/`/`sys.rs` (the 1 is the
crate's `#![deny(unsafe_code)]` line); bridge = `rust_io.c` +
`rust_io_precheck.c` + `engine_io.c`. By the same script, iteration 1's end
(a0d9c428b) is exports 27, cb 7, ops 7, structs 3, unsafe 1, statics 13,
ifdef 6, bridge 1055; integrate is exports 28, cb 5 (+34 exec ops),
structs 4, unsafe 42, statics 11, ifdef 7, bridge 1474.

| Step | Commit | Change | exports | cb | statics | bridge | Perf vs previous (key cells) | Gates | Verdict |
|---|---|---|---|---|---|---|---|---|---|
| A merge | 0736e616c | `rust-io/harden` merged by intent: compat picks kept once, rtc/exec fixes dropped, step 13's deferred shutdown kept (harden's shutdown-in-Drop dropped), admission control ported to `admission.rs` and held in `Global` (no new statics), iter1's stricter clippy kept | 29 | 7 | 13 | 1058 | vs iter1 end (`quick-head2`): -P 16 all Rust cells within noise (main ns -4.4% to +3.0%); -P 1 Rust main ns +3% to +5% everywhere (precheck-t1 SET +4.9%\*, precheck-t3 SET +5.1%\*) | all pass, only the known failures; unit/qos 16/0 and unit/limits 1/0 in io and precheck; overload soak below | FIXED (next row) |
| A fixup | e610e558a | the engine thread counts pops in a counter only it writes and subtracts them from the shared budget once per iteration | 29 | 7 | 13 | 1058 | -P 1 vs merge: main ns -1.6% to -6.2% (io-t1 SET -6.2%\*, precheck-t1 GET -4.8%\*); vs iter1 end every -P 1 cell within noise (-5.2% to +3.7%) | (code path covered by the s1 gates) | KEEP |
| C1 (plan 16) | 88b5b69b9 | session view: single-writer seqlock written only on change, `consumed` atomic, `vio_conn_publish()` replaces `vio_conn_set_auth_required()` + per-delivery `vio_conn_set_session()` | 29 | 7 | 13 | 1072 | predicted -1% to -3% main ns at -P 1 t3: first run precheck-t3 GET -1.8%, SET -1.7% (inside the prediction, below noise), but a rerun of the same cells gave 2152 vs 1746 ns for the same binary, so these client-bound cells cannot resolve it; -P 16 within noise (precheck-t3 GET 393 ns, -1.9%) | all pass; hazards barrier fallbacks 13 (iter1: 13), no_session 4 (iter1: 5) | KEEP (effect below resolution) |
| B (plan 11) | b9b2cc0b5 | `handoff.rs`: `Waker` + `Mailbox` replace the IO-inbox and main-ready Dekker copies; protocol unit tests under Miri | 29 | 7 | 13 | 1072 | predicted neutral: -P 16 within noise (main ns -2.4% to +1.6%); -P 1 t3 cells +10%/+19% in the first run, a rerun gave -1.7%/+16.7% for SET only and the c1 rerun itself moved 23%: noise of those cells. cs.sh t1 -P 16 (3 runs each): main 2034/2897/2043 vs 2977/2035/2055 voluntary switches, rust-io-0 96/19/97 vs 0/108/115 | all pass; stall.py 600 s precheck: `stalls 0`, max burst latency 0.102 s | KEEP |
| B (plan 14) | db4bf6a84 | `engine_io.{h,c}`: `engineSubmit()` fills `cmd_queue`/`parsedCommand` (VioCmd = `engineCmd`, static_assert), `engineAccountRead()` | 29 | 7 | 13 | 1098 | predicted neutral: -P 16 main ns -2.2% to +2.2%, RPS within noise | all pass | KEEP (partial: deliver and stall stay in rust_io.c) |
| C2 (plan 17) | fb8a89752 | one `create_argvs` and one `precheck_batch` crossing per parsed batch; snapshot pinned once per poll iteration | 29 | 7 | 13 | 1117 | first version: precheck-t1 GET -P 16 RPS -4.3%\*, io-t3 IO CPU +9% to +20%\*; IO-thread profile showed `_int_malloc`+`_int_free`+`malloc` 3.7% (three Vecs grown per batch) | (fixup) | FIXED |
| C2 fixup | eb888ed0d | batch buffers are Worker fields | 29 | 7 | 13 | 1117 | predicted IO CPU/request -1% to -5% where the IO thread is the limit: two back-to-back reruns, precheck-t1 -P 16 RPS GET +0.9%, SET +2.3% (IO thread at 100%), i.e. -1% to -2% IO CPU per request. Main-bound t3 cells: main ns +0.5% to +3.5% in reruns (below the 4.8% threshold); main-thread profiles of io-t3 show no new frame (io mode runs none of the changed code on main) | all pass (Tcl rerun after a gate-harness error, see deviations) | KEEP |
| C3 (plan 20) | dc0537dad, fixup 3a26b934c | adaptive 2-20 us spin on IO threads, awake check on by default | | | | | first version live-locked (rust-io-0 at 100%, a -P 1 benchmark hung: the spin returned for an inbox whose eventfd write only TOK_WAKE drains), fixed in 3a26b934c. Then, predicted t1 -P 16 RPS within 3% and -P 1 main -11% to -16%: quick io/precheck-t1 -P 16 RPS -30% to -38%\*; sweep on 8 server cores (GET) t1 -28%, t2 -22%, t3 -11%, t6 +0.7% RPS at -P 16; -P 1 main ns -8% to -18% as predicted but p99 +11% to +16%. cs.sh t1 -P 16: rust-io-0 and main both block 814 times per 3 s with `VIO_SPIN_US` 0, 20 and 200 alike | | REVERTED (8da21ddac) |
| C4 (plan 21) | c7d34a90f | ready connections' commands go through the memory_prefetch batch (the C IO threads' scheme), tails run by client id after the batch; resumable from a nested drain | 29 | 7 | 13 | 1186 | bound first (C io4, `-r 10000000`, prefetch on vs `prefetch-batch-max-size 0`): -P 1 main ns -0.8%/-1.1%, so no prefetch gain at -P 1 for C; -P 16 +44%/+28% without it. Rust, 8 server cores: -P 1 main ns -4.5% to -11.8% (t3, t6, r1M and r10M); -P 16 GET RPS -3.7% to -8.8%\* | all pass incl. scriptkill io/precheck | FIXED (next row) |
| C4 fixup | 43538efc4 | only queues of <= 2 commands go into the batch (a pipelined queue is prefetched per client by processInputBuffer()) | 29 | 7 | 13 | ~1190 | vs C2 fixup, r1M: -P 1 main ns -10.3% (t3 GET and SET), -7.5%/-7.4% (t6); -P 16 within noise except precheck-t3 SET RPS -2.7%\* (threshold 2.5%) | final gates (below) | KEEP |

## Final verification (stage 6d, on 93010a6b5)

Stage 6c was killed before its phase D; stage 6d ran it. Full numbers and
citations: `~/rust-io/iter2/report.md`; data `~/rust-io/iter2fin/`.

- Gates: all pass, line-for-line the same as the C4 gate
  (`gates/head/summary.txt`); `cargo +nightly miri test` 17 passed.
- Full Tcl suite (6c's runs plus a rerun of `integration/replication-psync`,
  which 6c's SIGKILL interrupted): 0 RUST-IO-BUG, 0 FLAKY in io and precheck.
- ASAN soak, precheck, 600 s, zero-copy off and at 2048: both clean.
- Stage 5b scaling method, precheck vs integrate in the same session:
  GET -P 16 +9.6% RPS (T=4), +10.4% (T=8); -P 1 main ns/req -13% to -17%.
  T=4 Q4 main thread 427 -> 390 ns/req (free argv 36 -> 1 ns, handoff/drain
  20 -> 39 ns).
- Zero-copy (T=4, -P 16, GET): main ns/req 2200 -> 371 (16 KB) and
  20284 -> 511 (200 KB); RPS +2% / +10%, IO-thread bound.

# Iteration 3b (stage 7b): engine features over external connections

Branch `rust-io/iter3b` on iter2 `300a1c03a`. Design: DESIGN.md
"Iteration 3b". Report with pasted results: `~/rust-io/iter3b/report.md`
on the stage 7b host.

| Commit | Feature | Target tests, io and precheck with takeover (before: iter2) |
|---|---|---|
| af6e1e199 | slot import over cmd_queue (B2 refusal removed) | unit/cluster/cluster-migrateslots 123/0 (iter2 io: 9/53 and aborted), unit/cluster/cli 40/0, unit/cluster/socket-prioritization 5/0; ASAN io and precheck 123/0, no sanitizer report |
| 30d70cbda | detached connections: fork-child and synchronous IO | integration/dual-channel-replication 43/0, unit/auth 16/0, unit/scripting 670/0 (debugger tests included), integration/replication 95/0 and replication-psync 90/0 (no 40-minute cap) |
| 466bdba26 | client memory and query buffer accounting | unit/client-eviction 18/1, unit/maxmemory 30/0, unit/querybuf 3/0, unit/protocol 37/0 |
| 9c739c136 | protocol-error log line with the query buffer sample | integration/logging 10/0 |
| 2f0840970 | priority connections | unit/socket-prioritization 8/0 |
| 1ca6807e7 | slot import stream pulled in PROTO_IOBUF_LEN rounds | fixes "Evictions on a full import target are replicated" (OOM, 3 of 3 runs after 466bdba26) |

Full default suite (174 files, per file `--single`, as stage 5a), io /
precheck: 6110 / 6102 pass, 14 / 14 fail, 0 RUST-IO-BUG. Remaining:
config-set-bind 6 and config-set-port 1 (intended), c-io-threads 2,
rust-held-output-not-in-cob 4 (stage 7a), and client-eviction "BLPOP +
maxmemory-clients: successor eviction during ready-key reexec"
(TRANSPORT-ASSUMPTION: the test measures tot-mem while the IO thread is
still handing output to the kernel; 4.87 MB after its fill loop, 1.94 MB
once blocked).

Perf, precheck-t3, GET/SET -P 16 -c 200 -d 64, 3 reps x 2 rounds,
iter2 HEAD vs 1ca6807e7: GET RPS -1.8%, main ns/req +1.7%; SET RPS +1.4%,
main ns/req -1.4%.
