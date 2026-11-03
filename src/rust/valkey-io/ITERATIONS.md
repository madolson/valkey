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

# Iteration 3a (stage 7a, branch rust-io/iter3a)

Base: `rust-io/iter2` 300a1c03a. Same host class (32 cores). Method: stage
5b's matched-thread bench (`~/rust-io/iter2fin/tools/bench.py`, copied to
`~/rust-io/iter3a/tools/`): server on cores 0..T-1, valkey-benchmark
`--threads 16` on 16-31, `-c 400 -d 64 -r 1000000`, 8 s, 3 reps, median.
C = `--io-threads T`, precheck = `--rust-io-threads T-1 --rust-io-takeover yes`.
Main ns/request = main-thread CPU / RPS. Baseline: the iter2 HEAD binary
(`~/rust-io/iter3a/bin/valkey-server.iter2`, `sha=300a1c03:0`), rerun in the
same session as each step. Raw data `~/rust-io/iter3a/m/bench.csv`,
profiles `~/rust-io/iter3a/perf/`. Predictions are written here before the
step is measured.

## Step 1a: no allocation in the handoff (swap-the-Vec mailboxes)

Finding (6d profile `head-precheck4-get-main.data`, call graph under
`vio_conn_return_argv`, 5.84% inclusive of main-thread samples): 2.68% is
`Vec::push` -> `RawVec::grow_one` -> glibc `realloc`, and 1.01% of that is
`__lll_unlock_wake_private`. The "mailbox mutex" futex in the 6d table is
mostly glibc's malloc arena lock, not the mailbox: Rust uses the system
allocator, `return_argv` handed its per-thread Vec to the IO thread
(`mem::take`) every flush and regrew a new one from capacity 0 on the next
command, and the IO thread freed it into the engine thread's arena. The two
mailboxes had the same pattern: `Mailbox::take()` left the producer an
empty Vec, so every inbox and ready batch was allocated on one thread and
freed on the other.

Change: `Mailbox::swap()` replaces `take()`: the consumer swaps its emptied
buffer with the queue, so both keep capacity. `Msg::FreeArgv` carries one
(argv, argc) and the engine thread's per-IO-thread Vec is drained into the
inbox, never moved. Steady state: no allocation on either side.

Prediction (T=4 GET -P 16, main thread): realloc + its futex (~3.7% of
390 ns = 14 ns) and the remaining futex (~0.7%, 3 ns) go away, minus the
cost of the 16 B/entry copy into the inbox: -10 to -15 ns/request on main,
+2.5% to +4% RPS at T=4 GET -P 16 (main-bound). T=8 GET -P 16 similar
in ns. SET -P 16: same ns saving, smaller relative (~580 ns/req): +1% to
+2%. -P 1 cells: main ns -1% to -2%, RPS client-bound (unchanged).

Result (`s1a` vs `base`, same session, `m/bench.csv`):

| T | test | P | RPS base -> s1a | main ns/req base -> s1a |
|---|---|---|---|---|
| 4 | GET | 16 | 2,602,074 -> 2,711,186 (+4.2%) | 380 -> 365 (-3.8%) |
| 4 | SET | 16 | 1,772,948 -> 1,775,630 (+0.2%) | 563 -> 563 (-0.1%) |
| 4 | GET | 1 | 222,087 -> 221,287 (-0.4%) | 847 -> 823 (-2.7%) |
| 4 | SET | 1 | 220,056 -> 220,738 (+0.3%) | 991 -> 933 (-5.8%) |
| 8 | GET | 16 | 2,476,618 -> 2,724,327 (+10.0%) | 403 -> 367 (-8.9%) |
| 8 | SET | 16 | 1,711,088 -> 1,747,125 (+2.1%) | 584 -> 572 (-2.1%) |
| 8 | GET | 1 | 518,034 -> 517,048 (-0.2%) | 1009 -> 940 (-6.8%) |
| 8 | SET | 1 | 513,240 -> 515,318 (+0.4%) | 1160 -> 1096 (-5.5%) |

Spreads 0.1-2.9%. GET -P 16 met the prediction at T=4 (-15 ns) and beat
it at T=8 (-36 ns, where more IO threads meant more cross-arena frees).
SET -P 16 at T=4 did not move (predicted +1% to +2%): its main thread is
`setCommand`-bound and the saved ns are inside the spread. -P 1 main ns
fell 3% to 7% (more than predicted; one return_argv per command there).
Gates (`gates/s1a/summary.txt`): line-for-line the iter2 failures;
miri 18 passed. unit/client-eviction aborted on its first input-side test
in both modes; later gates skip the three input-side tests as iteration 1
did. Verdict: KEEP.


## Step 1b: fewer lock round trips per delivery

Each delivery took the connection's `shared` mutex three times: one
`vio_conn_pop_cmds()` that got the commands, a second one that found the
queue empty (`rustIoPull()` looped until 0), and `vio_conn_pending_cmds()`
right after (`rustIoDeliverPull()`). The line is written by the IO thread
that queues commands, so each round trip is a likely cache miss on the
engine thread. Change: `rustIoPull()` stops when a pop returns fewer than
it asked for, and `Conn::ncmds` mirrors the queue length (stored under the
lock), so `vio_conn_pending_cmds()` is one load.

Prediction: two of three lock round trips go away per delivery. -P 16
(16 commands per delivery): -2 to -5 ns/request main, RPS +0.5% to +1.5%
at T=4 GET. -P 1 (one command per delivery): -4% to -8% main ns/request,
RPS unchanged (client-bound).

Result: the first `s1b` run had 3.3-6.1% spreads at T=4 (rep 3 slow in
every cell, foreign load), so T=4 was rerun for both binaries back to back
(`s1a-r`, `s1b-r`):

| T | test | P | RPS s1a -> s1b | main ns/req s1a -> s1b |
|---|---|---|---|---|
| 4 | GET | 16 | 2,716,118 -> 2,728,972 (+0.5%) | 363 -> 349 (-3.8%) |
| 4 | SET | 16 | 1,758,666 -> 1,833,623 (+4.3%) | 568 -> 545 (-4.0%) |
| 4 | GET | 1 | 221,571 -> 221,955 (+0.2%) | 815 -> 802 (-1.6%) |
| 4 | SET | 1 | 219,642 -> 220,905 (+0.6%) | 972 -> 951 (-2.2%) |
| 8 | GET | 16 | 2,724,327 -> 2,741,906 (+0.6%) | 367 -> 364 (-0.9%) |
| 8 | SET | 16 | 1,747,125 -> 1,774,440 (+1.6%) | 572 -> 564 (-1.4%) |

Spreads 0.3-1.8% in the rerun. -P 16 beat the prediction in ns (-14 and
-23 ns at T=4), but GET RPS moved only +0.5%: at 2.73M the main thread is
at 95% and the three IO threads at 99% (`max_other_thread_pct` 98.8-99.1),
so T=4 GET -P 16 is now IO-thread bound. -P 1 missed the prediction
(-1.6%/-2.2%, predicted -4% to -8%). Gates (`gates/s1b/summary.txt`): the
same as s1a; unit/client-eviction with the input-side tests skipped 8/8,
unit/maxmemory 28/2 (input-buffer tests) in io and precheck, as in iteration
1's copy mode. Verdict: KEEP.

## Step 2: zero-copy replies on by default

Deviation: this step's prediction was not written before measuring. The
two failing unit/maxmemory tests were reproduced and diagnosed first
(`~/rust-io/iter3a/scripts/mget_repro.py`), then fixed, then measured.

Diagnosis, `rust-io-zero-copy-min-bytes 2048`, s1b binary, the MGET test's
setup (11 MB maxmemory, 3 MB maxmemory-clients, 20 non-reading MGET
clients): after 1 s `evicted_clients:20` and `evicted_keys:13`. Client
eviction picked the clients, but each had a write job in flight, so
`freeClient()` only scheduled them (`clientHasPendingIO()`), and the next
`performEvictions()` still counted their memory and evicted keys. The
tracking test emptied the database (`evicted: 300`, dbsize 0): the
invalidation messages of the keys one eviction removed sat in jobs while
the next commands ran `performEvictions()`, which evicted more keys.

Change (`src/evict.c` `freeMemoryGetNotCountedMemory()`):
- (a) not counted: `last_memory_usage` of normal and pubsub clients in
  `server.clients_to_close`. Engine code, also active for C IO threads.
- (b) not counted: `server.io_inflight_reply_bytes`, the `reply_bytes`
  sealed into Rust write jobs in flight (`rustIoTrySendWrite()` adds,
  `rustIoDrainDone()` subtracts). Only rust_io.c writes it.
- `rust-io-zero-copy-min-bytes` default -1 -> 2048 (`src/config.c`).

Attribution (precheck, zero-copy 2048, unit/maxmemory): s1b 26/4; (a)
only 27/3, MGET passes, tracking fails; (a)+(b) 28/2, the copy-mode result
(the two remaining failures are the input-buffer tests, Rust-held query
buffers). The repro after (a): `evicted_clients:20`, `evicted_keys:0`.

Quick profile (64 B, so writes stay under 2048 B and are copied), s1b-r vs
s2, T=4: GET -P 16 +0.6% RPS / +0.5% main ns, SET -P 16 -0.8% / +0.6%,
-P 1 within 0.6%; T=8 vs s1b: GET -P 16 -2.0% RPS (spreads 0.7%/1.1%),
others within 0.6%. Neutral, as expected for copied writes.

Headline, T=4 GET -P 16, `-c 400`, `KEYS=10000`, 3 reps (variants
`zc16384-*`, `zc204800-*`):

| value | C io-threads 4 | precheck, copy (-1) | precheck, default (2048) |
|---|---|---|---|
| 16 KB | 173,012 RPS, main 5786 ns/req | 277,199 RPS, main 2124 ns/req (main CPU 58%) | 282,532 RPS, main 370 ns/req (10.5%) |
| 200 KB | 30,731 RPS, main 32,508 ns/req | 27,758 RPS, main 19,479 ns/req (54%) | 28,415 RPS, main 459 ns/req (1.3%) |

Same shape as 6d (371 and 511 ns): the IO threads are at 99-100% in every
Rust cell, so RPS moves +2%; the main thread leaves the payload.

Gates (`gates/s2/summary.txt`, default now zero-copy 2048): identical to
s1b for every file outside the eviction set. unit/maxmemory 28/2 (s1b
copy mode 28/2; zero-copy before the fix 26/4). unit/client-eviction
(input-side tests skipped) 9/7: "BLPOP + maxmemory-clients: successor
eviction during ready-key reexec" now passes, the other 7 are unchanged.
unit/obuf-limits 13/0. integration/throttle-repl io 2/5, precheck 3/4,
the 6d counts (the 4 COB tests remain). So of the suite's MISSING-FEATURE
tests, 1 of the 14 client-mem-accounting ones passes by default now
(the BLPOP one) and the 4 rust-held-output-not-in-cob ones still fail.
Default build (no Rust) of this tree vs iter2's, unit/maxmemory,
unit/client-eviction, unit/obuf-limits, with and without
`--config io-threads 4`: 30/0, 19/0, 13/0 in all four runs
(`gates/def-{s2,iter2}{,-io4}`). ASAN soak, precheck, default config,
600 s (`soak/asan-precheck-s2`): `server alive after soak: True`,
`SOAK EXIT 0`, `SERVER EXIT 0`, no `asan.*` report; 401,800 connections.
Zero-copy engaged by default on that binary: three `GET` of a 200 KB value
moved `rust_io_writes_submitted` 0 -> 3. Verdict: KEEP.

## Step 3: call() reads zmalloc_used_memory() per command (upstreamable)

Commit 8bb5292d7 here, cherry-picked from 18782f73d on branch
`rust-io/iter3a-peak`, which is d80033654 (valkey-io/valkey unstable) plus
that one commit. It also applies to the current upstream tip 7220c38f9
(`git apply --check`). No Rust dependency.

Change: `zmalloc_thread_used_memory()` returns the calling thread's own
counter (one cache line it owns). call() runs the full
`zmalloc_used_memory()` sum and peak update only when that counter grew
since the thread's last peak update (`peak_thread_used`, thread-local).
Growth on other threads alone is recorded by beforeSleep() and the cron, as
before, and by getMaxmemoryState(), which already has the sum. New test in
unit/info: SETRANGE of 50 MB and DEL in one write; without the call()
update, `used_memory_peak` is 1,278,632 against the 41,297,016 it must
exceed (pasted in the report).

Prediction (stage 5b put the sum at 8% of C's main thread at 16 threads):
GET -P 16 never grows the main thread's counter (argv are built and freed
on IO threads, replies go into the fixed `c->buf`), and SET -P 16 at
`-r 1000000` mostly overwrites (frees values the IO threads allocated), so
the sum almost never runs per command. C io-threads: T=16 GET/SET -P 16
main ns/req -6% to -8% (RPS +6% to +9%, the C main thread is the limit),
T=8 -2% to -4%, T=4 -1% to -2%. Rust precheck: same order at T=16 (-4% to
-7% main ns), within noise at T=4.

Measurement. The first run (one server process per config, 3 reps) had
4-17% spreads at T=8/16 with RPS falling from rep to rep in the same
process (C T=16 GET 1.37M, 1.29M, 1.21M), so every step-3 cell was rerun
with a fresh server per rep and the two binaries alternated
(`tools/alt.sh`, 3 rounds, median). C = upstream d80033654 default builds
without (`up-base`) and with (`up-peak`) the commit; Rust = s2 vs s3:

| T | config | test | RPS before -> after | main ns/req before -> after | spreads |
|---|---|---|---|---|---|
| 4 | C io-threads | GET | 2,532,679 -> 2,597,552 (+2.6%) | 390 -> 377 | 5.2% / 5.1% |
| 4 | C io-threads | SET | 1,760,008 -> 1,804,343 (+2.5%) | 568 -> 554 | 0.8% / 1.5% |
| 8 | C io-threads | GET | 1,998,320 -> 2,134,670 (+6.8%) | 496 -> 462 | 18.3% / 11.8% |
| 8 | C io-threads | SET | 1,686,666 -> 1,767,537 (+4.8%) | 592 -> 566 | 1.2% / 2.6% |
| 16 | C io-threads | GET | 1,333,779 -> 1,420,022 (+6.5%) | 750 -> 704 | 4.4% / 1.5% |
| 16 | C io-threads | SET | 1,094,239 -> 1,124,302 (+2.7%) | 914 -> 889 | 3.1% / 4.1% |
| 4 | precheck | GET | 2,620,368 -> 2,580,033 (-1.5%) | 346 -> 355 | 2.3% / 3.2% |
| 4 | precheck | SET | 1,775,762 -> 1,691,806 (-4.7%) | 559 -> 589 | 1.3% / 0.5% |
| 8 | precheck | GET | 2,683,542 -> 2,640,858 (-1.6%) | 373 -> 379 | 1.5% / 2.6% |
| 8 | precheck | SET | 1,752,608 -> 1,672,402 (-4.6%) | 571 -> 598 | 0.9% / 0.4% |
| 16 | precheck | GET | 2,311,014 -> 2,427,956 (+5.1%) | 433 -> 411 | 0.9% / 2.6% |
| 16 | precheck | SET | 1,600,016 -> 1,558,236 (-2.6%) | 624 -> 641 | 1.4% / 0.9% |

C io-threads 16 GET -P 16 main-thread profile (`perf/s3-up-{base,peak}-c16-get-main.*`):
`call` self 11.4% -> 5.0% and `zmalloc_used_memory` self 2.24% -> absent,
so about 8.6% of main-thread samples go, as 5b found. RPS under the
profiler did not rise (1.35M -> 1.28M, inside the per-process drift); the
freed time reappears as memmove in reply building (28.1% -> 34.7%), so at
16 threads C's main thread stalls on the IO threads' lines either way.

Precheck SET regressed by 4.6-4.7%. A control binary `s3x` (this tree with
call() back to an unconditional update, everything else as s3) was
measured against s2 and s3, alternated: s3x vs s2 SET +4.1% (T=4) and
+2.5% (T=8); s3 vs s3x SET -5.5% (T=4) and -5.2% (T=16), GET T=16 +4.9% for
s3. Main-thread `perf stat`, T=4 SET -P 16 (`tools/ipr.sh`):

```
s3:  instructions per_req=4727.6  cycles per_req=2044.5  branch-misses per_req=2.4
s3x: instructions per_req=4761.0  cycles per_req=1868.3  branch-misses per_req=2.4
s2:  instructions per_req=4767.4  cycles per_req=1953.2  branch-misses per_req=2.6
```

s3 retires 33-40 fewer instructions per SET (the sum it skips) and the
same branch misses, but spends 4.7% more cycles than s2, while s2 and s3x
run the same work and differ by 4.5% in cycles. I read that as a
code-placement effect of the LTO build on the Rust SET path, not as cost
of the gate, but I did not prove it (no frontend-stall counters were
taken). Verdict: KEEP for the C commit (+2.5% to +6.8% on C io-threads,
default-build tests unchanged: unit/info 33/0 with the new test,
unit/memefficiency 36/0, unit/lazyfree 4/0, unit/other 56/0,
unit/expire 85/0, unit/acl 127/0, unit/introspection 123/1 with the same
failure on unmodified d80033654). The precheck SET -5% is open.

## Step 4: runtime handle instead of statics (API plan step 8)

Change: `GLOBAL`, `DEFAULT_AUTH_REQUIRED` and `HANDOFF_DISABLE` become
fields of `Global`; the engine-thread thread-locals (`IS_MAIN`,
`MAIN_KICKS`, `MAIN_ARGV`, `MAIN_READY`, `MAIN_DONE`, `MAIN_SWAP`) become a
`MainState` in a `RefCell` of `Runtime` (`VioRuntime` in C), which
`vio_start()` returns and every export takes. The engine-thread check that
`IS_MAIN` did is a `pthread_self()` compare. A runtime is leaked on
purpose (there is no `vio_stop`), so the IO threads and connections hold
a `&'static Global` and nothing pays a refcount. `BARRIER_FLAGS` is a
test fixture in `precheck.rs`'s tests and stays; the four `timing.rs`
statics are the measurement build's and stay.

Prediction: neutral. A thread-local access (`__tls_get_addr`-free in a
static binary, one `%fs` load) becomes a load through the runtime pointer
the engine passes; the RefCell borrow flag replaces the thread-local
RefCell's. Main ns/request within +-1% (2-4 ns) at T=4 and T=8, GET and
SET -P 16 and -P 1.

Result (`tools/alt.sh`, fresh server per rep, s3 vs s4 alternated, 3
rounds):

| T | test | P | RPS s3 -> s4 | main ns/req s3 -> s4 | spreads |
|---|---|---|---|---|---|
| 4 | GET | 1 | 222,032 -> 222,291 (+0.1%) | 826 -> 792 (-4.2%) | 1.1% / 0.7% |
| 4 | GET | 16 | 2,599,703 -> 2,636,462 (+1.4%) | 349 -> 335 (-3.9%) | 20.0% / 5.4% |
| 4 | SET | 1 | 219,441 -> 221,202 (+0.8%) | 1033 -> 1041 (+0.9%) | 0.8% / 1.5% |
| 4 | SET | 16 | 1,722,932 -> 1,784,806 (+3.6%) | 580 -> 560 (-3.4%) | 0.1% / 0.9% |
| 8 | GET | 1 | 504,743 -> 517,355 (+2.5%) | 946 -> 926 (-2.2%) | 2.9% / 2.2% |
| 8 | GET | 16 | 2,671,402 -> 2,835,891 (+6.2%) | 374 -> 353 (-5.6%) | 1.0% / 2.3% |
| 8 | SET | 1 | 500,158 -> 508,121 (+1.6%) | 1156 -> 1145 (-0.9%) | 3.2% / 2.8% |
| 8 | SET | 16 | 1,690,929 -> 1,754,812 (+3.8%) | 591 -> 570 (-3.6%) | 0.5% / 4.4% |

Predicted neutral; no cell got slower beyond 1%, and the -P 16 cells got
faster by 1.4% to 6.2%. SET -P 16 is back at s2's level (s2 1.78M at T=4),
which fits step 3's reading that its SET drop was code placement; I do not
claim the gain for this step. Gates (`gates/s4/summary.txt`): the same as
s2 line for line, plus the new cargo test (19 passed); miri 18 passed and
1 ignored (`two_runtimes` uses sockets). `make check-rust-header` passes
with the regenerated header. Verdict: KEEP.

## Step 5: sans-IO connection state machine (API plan step 10)

Change: `conn_state.rs`. The five lifecycle flags that both threads change
(`kicked`, `in_ready`, `c_closed`, `peer_closed`, `half_close`, five
`AtomicBool`s on `Conn`) become one `AtomicU32`. `step(flags, event) ->
(flags, effects)` is pure; `ConnState::apply()` installs the result with
one compare-exchange (none when nothing changes) and the caller carries out
the effects (post a kick, push to the ready list, notify VIO_EV_CLOSED,
release, shutdown(2)). The IO thread's three hand-written release and
shutdown checks (`on_kick`, `maybe_half_close`, the EPOLLOUT path) become
one `after_flush()` that applies `Flushed { out_drained, job_idle }`.
`want_writable`, `read_paused`, `auth_required` and `parse_held` stay
separate atomics: each has one writer per direction and no effect to
decide. Unit tests (also under Miri): peer close before adopt, engine
close with output pending, kick and notify coalescing, deferred shutdown
(waits for the output and the job, happens once, moot after a peer
close), and a two-thread kick/take race that checks one post per take.

Prediction: neutral. A Kick that coalesces is now a load instead of a
swap; one that posts is a load plus a CAS instead of a swap. Main ns/req
within +-1% at T=4/T=8, GET/SET, -P 1 and -P 16.

Result (`tools/alt.sh`, s4 vs s5, 3 rounds):

| T | test | P | RPS s4 -> s5 | main ns/req s4 -> s5 | spreads |
|---|---|---|---|---|---|
| 4 | GET | 1 | 221,919 -> 221,478 (-0.2%) | 789 -> 806 (+2.2%) | 0.4% / 0.9% |
| 4 | GET | 16 | 2,786,862 -> 2,783,458 (-0.1%) | 330 -> 332 (+0.6%) | 0.5% / 0.3% |
| 4 | SET | 1 | 220,886 -> 219,671 (-0.6%) | 1028 -> 1039 (+1.1%) | 0.0% / 1.1% |
| 4 | SET | 16 | 1,791,160 -> 1,778,214 (-0.7%) | 558 -> 562 (+0.6%) | 0.7% / 0.3% |
| 8 | GET | 1 | 518,581 -> 517,418 (-0.2%) | 937 -> 929 (-0.9%) | 0.2% / 0.3% |
| 8 | GET | 16 | 2,847,860 -> 2,870,116 (+0.8%) | 351 -> 348 (-1.0%) | 0.9% / 2.0% |
| 8 | SET | 1 | 511,850 -> 510,608 (-0.2%) | 1160 -> 1148 (-1.1%) | 1.1% / 0.3% |
| 8 | SET | 16 | 1,745,088 -> 1,763,285 (+1.0%) | 573 -> 567 (-1.0%) | 1.2% / 1.7% |

Within the +-1% prediction except T=4 GET -P 1 main ns +2.2% (client-bound
cell, RPS -0.2%). Gates (`gates/s5/summary.txt`): the same as s4 line for
line, cargo test 24 passed (5 new), miri 23 passed and 1 ignored. Stall
detector, precheck, 600 s, 400 connections (`gates/s5/stall-precheck.txt`):

```
duration 601 s, connections 400, bursts 2174779, requests 35869275, max burst latency 0.133 s, p99.9 0.000 s, stalls 0
EXIT 0
```

Verdict: KEEP.

## Final (iteration 3a)

iter2 HEAD vs final, same session, fresh server per rep, alternated
(`~/rust-io/iter3a/m/final-table.md`; report `~/rust-io/iter3a/report.md`):
precheck GET -P 16 2.58M -> 2.78M (T=4, now +0.1% vs C's 2.77M),
2.49M -> 2.84M (T=8), 1.99M -> 2.52M (T=16); SET -P 16 +1.0% / +1.5% /
+11.8%. Q4 T=4 main thread: precheck 386 -> 365 ns/req (C 375),
handoff/drain 38 -> 32 ns, glibc arena futex 1.2% -> 0.
