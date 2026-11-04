# Rust IO layer for Valkey: three cut points, evaluated

Epic: https://github.com/madolson/valkey/issues/196. Base: valkey-io/valkey unstable d80033654. Code: `src/rust/valkey-io` on the fork branches below. All numbers are from a 32-vCPU EPYC 9R14 KVM guest where syscalls are expensive (C single-threaded spends ~14 us of main-thread CPU per non-pipelined request), so absolute RPS is low and the handoff costs look larger than on bare metal.

## Recommendation

Build the IO layer at cut point 2's shape, not cut point 3. Rust owns the sockets, parsing, transport-level session state, admission control and the routing key (slot). The engine exposes one entry point, "execute this prepared command", and a thread-safe precheck that runs against a published policy snapshot and is validated by epoch on the engine thread. Do not move call() into Rust.

- Cut point 1 works with almost no engine change: a new `ConnectionType` plus the existing per-client `cmd_queue` let Rust feed the unchanged `processInputBuffer()` path, with zero lines changed in networking.c. It is a Rust replacement for io_threads.c, and on its own it is not faster than C IO threads at equal thread count (see Performance).
- Cut point 2 is where the modularity and performance actually are. It removes 8-24% of main-thread CPU per request (more with ACL key patterns and in cluster mode) and is the minimum needed to route commands to N engine shards. After iterating, its main thread costs 390 ns/request against C io-threads' 382 at 4 threads. At equal thread count it leads C by 22-60% from 8 threads up (C io-threads degrade past 4), and it trails by 6% only at 4 threads for pipelined GET. It cannot reach "the main thread only runs call()": eviction, MULTI queueing, pause, throttling, module filters and migrating-slot key lookups mutate the engine or read the keyspace, so they stay on the engine thread.
- Cut point 3 gains nothing a single engine can use and costs the most. A Rust call() needed 29 engine helpers because nothing in call() that touches engine state could move, so call() now exists twice (three places counting nested EXEC/script/RM_Call). Run-to-completion under the engine lock hits the same ceiling as one main thread (lock 93-95% busy at saturation) and conflicts directly with AOF durability (#3381), forkless save and tiering, which all hook call().
- For vertical scaling, choose shared-nothing (N single-threaded engines, each owning slots, the IO layer routing with batched per-shard queues) over a concurrent keyspace. The engine writes shared state on every read (LRU, hit counters) and decides propagation from a global dirty counter, so a concurrent hashmap needs every t_*.c rewritten. Shared-nothing needs the `server` singleton split, which costs the same at every cut point.

## What was built

| Branch / PR | Mode | What Rust owns | FFI (Rust exports / C callbacks / engine helpers) |
|---|---|---|---|
| `rust-io/bootstrap`, https://github.com/madolson/valkey/pull/198 | io | listener, sockets, RESP parse, argv allocation, writes, backpressure | 22 / 2 / 0 |
| `rust-io/precheck`, https://github.com/madolson/valkey/pull/200 | precheck | + lookup, arity, slot, auth, ACL, session checks, token | 23 / 5 / 0 |
| `rust-io/exec`, https://github.com/madolson/valkey/pull/199 | exec handoff / rtc | + top-level call(); rtc executes on the IO thread under the module GIL | 25-28 / 5-10 / 29 |
| `rust-io/integrate` | all four, cumulative | handoff v2, SCRIPT KILL fix | 28 total |
| `rust-io/compat-fixes` | | 5 fixes from the full-suite and ASAN pass | |
| `rust-io/harden` | | 14 commits: compat fixes, admission control, shutdown-after-drain, rtc beforeSleep, clippy -D warnings | |
| `rust-io/bench` | | measurement harness: e2e quick/full, compare with noise thresholds, per-stage TSC timers (zero cost when off), criterion benches, perf grouping | |
| `rust-io/iter1` | | iteration 1: exec deleted, `ffi/` + `sys.rs` hold all unsafe, handle newtypes, cbindgen header, `connection_external.c`, zero-copy replies (opt-in), argv returned to the IO thread, limits by value, processCommand split | |
| `rust-io/iter2` (current) | precheck is the shipped mode | iteration 2: hardening merged, admission.rs, single session view, `handoff.rs` (one wakeup protocol), `engine_io.c`, batched FFI, cross-connection prefetch | 29 / 6 / 7 engine-side `externalConnOps` |

FFI counts are from 4a's DESIGN.md "FFI surface per mode" (they include the stage 4a handoff exports). Only `libc` is a dependency; the epoll loop is ~100 lines.

Engine changes outside the bridge for cut point 1: Makefile, connection.{h,c} (register the type), server.{h,c} (`prepareCommandGeneric()` made non-static, takeover skips the C TCP listener, a ready log line), config.c, io_threads.c (refuse Rust connections). Cut point 2 adds a +15/-3 `goto` in processCommand() and epoch bumps at 5 ACL mutators. Cut point 3 splits at the final call() in processCommand().

## Cut point 1: IO and parsing in Rust

Design: each Rust connection is an ordinary engine `client` on a `"rust"` ConnectionType. argv is built as robj on the Rust thread through a callback, moved into `c->cmd_queue`, and run through `processInputBuffer()`, so blocking, MULTI, pause and pipelining order are inherited. Replies go through the C write path into the connection's `writev`, which memcpys into a bounded (1 MiB) Rust buffer.

What it still costs the main thread: `prepareCommandGeneric()` per command (C IO threads already do this off-thread), a memcpy of every reply byte, and per-wakeup syscalls. Handoff v2 batched IO-thread kicks and skipped eventfd writes to an awake main thread, and that cut main-thread CPU per request by only 1-8%. Skipping eventfd writes to awake IO threads cut it 15-22%, but it made the single IO thread sleep and wake per batch and lost 29-34% throughput, so it ships off.

Why the seam matters: the existing ConnectionType and cmd_queue abstractions were enough to swap the transport without touching networking.c. The coupling that remains is the C `client` struct, because replies are built into it and commands read session state from it.

## Cut point 2: processCommand's checks on the Rust thread

processCommand's checks split four ways by what they read:
- **A, static tables:** lookup, arity, slot. Already off-thread in C.
- **B, per-client session:** auth, MULTI, the RESP2 pub/sub restriction, READONLY, db, user.
- **C, global policy:** ACL users, cluster ownership, role, loading, pause, disk errors, min-replicas.
- **D, engine-mutating or keyspace-reading:** filters, evictions, client eviction, tracking, migrating-slot lookups, MULTI queueing, throttle, rejection stats.

Precheck runs A and B, plus the ACL part of C, on the Rust thread, and tags each command with {ACL epoch, session generation, outcome}. The main thread compares the token and skips the block; anything stale runs the unchanged full path.

Findings:
- **The policy code stays C.** Rust threads call C `lookupCommand`, `commandCheckArity`, `clusterSlotByCommand` and `ACLCheckAllUserCommandPerm` on a copy-on-write user snapshot. Porting ACL selectors to Rust would have meant two implementations. So "the IO layer does processCommand" really means "the engine's policy code runs on IO threads against a snapshot". The modular boundary is an engine-provided thread-safe precheck, not Rust policy logic.
- **Pipeline hazards are real.** AUTH, SELECT, MULTI, HELLO, ACL and CLIENT change the answer for later commands in the same pipeline. Barriers plus validation by direct comparison on the main thread handle them. Container commands (ACL, CLIENT) have no proc of their own, and the first barrier list missed them; the epoch check caught it.
- **Cluster ownership is read live at validation, not snapshotted,** because the cluster bus changes ownership without a command.
- **Correctness is proven by negative controls.** With the epoch check removed, a revoked ACL permission is served. With the live slot check removed, ASK/MOVED are lost.
- **Stage 1 bugs it exposed:** the first read was parsed without auth limits, and commands pipelined behind AUTH were parsed with pre-AUTH limits.

Measured, main-thread CPU per request against io mode at -P 16:
- default user: -8% to -17%
- ACL user with key patterns: -16% to -24%
- one-node cluster: -15% to -17%
- -P 1: only -1% to -9%, because handoff costs dominate there.

Throughput gains only where the main thread is the bottleneck: +7% to +29% at 4 threads and -P 16. With one IO thread, that thread is the bottleneck and now does more per command, so throughput drops 2-5%.

## Cut point 3: call() in Rust

- **handoff (Rust call() on the main thread).** C call() is 279 lines. The Rust port is 96 lines but needs 29 helpers, because stats, propagation, commandlog, MONITOR, tracking and woff all stay C. Overhead is ~60-70 ns per command and -8% to -10% at peak. Every future call() change has to land in three places.
- **rtc (the IO thread executes under the module GIL).**
  - Main-thread CPU drops to 0.1-0.3%, and below the knee it uses 19-24% less process CPU at the same RPS.
  - At saturation the lock is 93-95% busy with ~1% uncontended acquisitions, and it lands 3-7% below io. The lock hold per SET (0.66 us) is what io's main thread spends per SET including the handoff (0.62 us), so lock transfer and engine-state migration across cores cancel the removed hop.
  - With precheck composed in (stage 4a), rtc reaches 2.0M GET at -P 16, slightly above io, because precheck runs outside the lock.
  - Engine invariants broke when commands ran off the main thread:
    - a startup deadlock, because the GIL is held from module init;
    - SIGSEGV, because `thread_shared_qb` is NULL on Rust threads;
    - `onServerMainThread()` asserts in forkless and scripting, handled by swapping `server.main_thread_id`, which is a data race;
    - scripts, functions, DEBUG and module commands yield through processEventsWhileBlocked and had to be deferred to the main thread;
    - a stale cached clock;
    - `-LOADING` semantics, which are lost;
    - beforeSleep-deferred work: tracking BCAST invalidations are 36-100 ms late, and cluster state updates are delayed.

## Performance

Matched comparison (stage 5b): equal total threads T (C `io-threads T` = Rust `rust-io-threads T-1` + main), server pinned to T cores, valkey-benchmark `--threads 16` on 16 other cores, `-c 400 -d 64`, median of 3 x 8 s.

| T | GET -P 1 (Rust best / C) | SET -P 1 | GET -P 16 | SET -P 16 |
|---|---|---|---|---|
| 2 | 1.04 | 1.05 | 1.06 | 1.06 |
| 4 | 1.03 | 1.03 | 0.88 (C 2.61M peak) | 1.05 |
| 6 | 1.03 | 1.03 | 1.14 | 1.22 |
| 8 | 1.00 | 1.00 | 1.13 | 1.19 |
| 12 | 1.05 | 1.25 | 1.69 | 1.59 |
| 16 | 1.23 | 1.19 | 1.55 | 1.52 |

- **-P 1 is a tie up to 8 threads.** Every design is bound by IO-thread syscalls in this KVM guest, about 73K requests/s per IO thread.
- **Precheck is the best Rust mode at -P 16.**
- **C peaks at T=4, then degrades:** 2.61M → 2.00M → 1.23M GET. Its main-thread cost per request doubles from 413 ns to 822 ns. Part of that is `call()` calling `zmalloc_used_memory()` on every command (server.c:4429), which walks one counter cache line per thread (zmalloc.c:530). That is an upstream C scaling issue, independent of Rust.
- **C's T=4 peak needs L3 locality.** C io-threads do not pin by default. Unpinned on 16 cores, the same config does 1.43M against Rust precheck's 1.99M.
- **The single-engine ceiling is still one thread.** The main thread is 100% busy in every -P 16 cell, and rtc's engine lock is 92-99% held.

Main thread at T=4, GET -P 16 (perf, hardware cycles):

| | C | Rust io | Rust precheck |
|---|---|---|---|
| ns/request | 413 | 463 | 427 |
| freeing argv | 2 | 44 | 36 |
| lookupCommand | 0 | 27 | 0 |
| processCommand checks | 38 | 24 | 20 |
| handoff/drain | 12 | 19 | 20 |
| reply memcpy into Rust | - | 4 | 3 |
| main-thread syscalls | 6 | 5 | 7 |

- `call()`, the command proc, reply building and prefetch are 70-74% in all three.
- Freeing argv on the IO thread would put precheck at about 391 ns, below C.
- At 64 B the reply memcpy is under 1%, so zero-copy matters for large values, output-buffer accounting and durability fences, not small GETs.

## Iteration (stages 6a-6d): one measured step at a time

Every step was one commit, gated by cargo test, clippy `-D warnings`, the smoke/hazard/scriptkill/backpressure/killself scripts and a Tcl subset. Each step was also measured with the stage 6a harness against the noise of three identical runs on the same host: main ns/request median spread 1.6%, max 8%. A prediction was written before each measurement. Full table: `src/rust/valkey-io/ITERATIONS.md` on `rust-io/iter2`.

| Step | Prediction | Measured | Verdict |
|---|---|---|---|
| Delete exec mode, ffi/sys split, handle newtypes, `connection_external.c` | neutral | -0.8% to -2.7% main ns, RPS within noise | keep |
| Zero-copy replies (write jobs on the C IO-thread contract) | 64 B neutral, 16 KB+ large | 64 B -P 16 neutral, 64 B -P 1 +17% (job round trip), 16 KB -P 16 main -79% / RPS +50%, 200 KB RPS +89% | keep, opt-in (`rust-io-zero-copy-min-bytes`, 2048 recommended). Default stays off: two maxmemory tests regress because a client with a write in flight can only be freed asynchronously |
| Return argv to the IO thread that built it | -8% to -10% main ns | `freeClientArgv` 56 -> 34 ns/call, 1-3% end to end; only with >= 2 IO threads | keep (below prediction) |
| Defer shutdown(2) until output drains | fixes BGSAVE reply loss | 5/5 -> 0/5 lost | keep |
| Merge hardening, admission control in `admission.rs` | neutral | -P 1 +3-5%, fixed by counting pops once per iteration | keep |
| Single seqlock session view | -1% to -3% at -P 1 | below resolution | keep |
| One wakeup protocol (`handoff.rs`, Miri-tested) | neutral | neutral, 600 s stall test 0 stalls | keep |
| Batched argv/precheck FFI, one policy pin per poll | -1% to -5% IO CPU | -1% to -2% IO CPU where IO-bound | keep |
| Bounded IO-thread spin + awake check | -11% to -16% main ns at -P 1, t1 within 3% | -P 1 as predicted, but t1 -P 16 RPS -28% to -38% | reverted |
| Cross-connection prefetch batch (shallow queues) | -P 1 gain | -P 1 main ns -7% to -10%, -P 16 neutral | keep |

Same-session before/after, stage 5b method (T threads on T cores, client on 16 others):

| T | test | -P | integrate precheck | iterated precheck | change | C io-threads T | iterated / C |
|---|---|---|---|---|---|---|---|
| 4 | GET | 16 | 2.37M | 2.60M | +9.6% | 2.75M | 0.94 |
| 4 | SET | 16 | 1.75M | 1.76M | +0.9% | 1.69M | 1.04 |
| 8 | GET | 16 | 2.28M | 2.51M | +10.4% | 2.00M | 1.26 |
| 8 | SET | 16 | 1.71M | 1.72M | +0.9% | 1.42M | 1.22 |
| 16 | GET | 16 | 1.89M (5b) | 1.94M | +2.3% | 1.21M | 1.60 |
| 16 | GET | 1 | 0.69M (5b) | 0.78M | +13.2% | 0.62M | 1.26 |

-P 1 at T=4/8 is client-bound for every binary. There the main thread's cost per request fell 13-17% (1020 -> 856 ns at T=4), which is headroom rather than RPS.

Main thread at T=4, GET -P 16, perf, iterated precheck vs C io-threads 4 in the same session: 390 vs 382 ns/request. Freeing argv went from 36 to 1 ns. processCommand checks are 18 ns vs C's 33. The remaining gap is the handoff and drain (39 vs 15 ns): the argv return queue and the mailbox mutex. SET is flat because `setCommand` dominates its main thread, and nothing in the IO layer touches it.

Zero-copy at T=4, -P 16, GET:

| value | C io-threads 4 | precheck, copy | precheck, zero-copy | main ns/request, copy -> zero-copy |
|---|---|---|---|---|
| 16 KB | 192K | 276K | 283K | 2200 -> 371 |
| 200 KB | 30.6K | 27.1K | 29.7K | 20284 -> 511 |

Zero-copy takes the payload off the main thread entirely. RPS barely moves, because the IO threads' socket writes become the limit (busiest IO thread at 100%).

The boundary, integrate -> iterated:

| | integrate | iterated |
|---|---|---|
| exec op-table entries | 34 | 0 |
| `unsafe` outside `ffi/` + `sys.rs` | 42 | 0 (crate root `#![deny(unsafe_code)]`) |
| clippy warnings | 22 | 0, enforced in the Makefile |
| header | hand-written | cbindgen, checked by `make check-rust-header` |
| C bridge lines | 1474 | 1193, plus 173 in `connection_external.c` |
| engine files with `USE_RUST_IO` | 7 | 6 |
| Rust exports / C callbacks | 28 / 5 | 29 / 6. Exec exports were removed, and submit_write, return_argv, set_limits and publish were added |
| statics | 11 | 13. The `Runtime` handle step was not done |
| full suite RUST-IO-BUG | 14 (stage 4b) | 0 |
| ASAN soaks | | clean, zero-copy off and on |

Not done: the `Runtime` handle in place of statics (plan step 8), the sans-IO connection state machine (step 10), zero-copy by default (it needs `evictClients()` to account for clients pending an async free), and the upstream `zmalloc_used_memory()` fix.

## Compatibility and safety (stage 4b, full default suite, per file)

| mode | files clean / 174 | pass | distinct failures not on control | TRANSPORT-ASSUMPTION | MISSING-FEATURE | RUST-IO-BUG | FLAKY |
|---|---|---|---|---|---|---|---|
| control (C) | 173 | 6113 | | | | | |
| io | 150 | 5699 | 84 | 11 | 57 | 14 | 2 |
| precheck | 149 | 5713 | 88 | 11 | 57 | 20 | 0 |
| exec rtc | 146 | 5706 | 94 | 12 | 65 | 17 | 0 |

Missing features, which are the cost of parity:
- the dual-channel RDB channel (a forked child writes into a Rust connection whose threads do not exist in the child): 38 tests;
- client memory accounting (Rust-held buffers are invisible to client eviction and maxmemory): 13;
- the Lua debugger over fork: 5;
- priority connections: 1;
- not tested: TLS and Unix sockets.

Bugs:
- **Fixed:**
  - a use-after-free found by ASAN in precheck's ACL snapshot build;
  - the SCRIPT KILL hang, where a yielding command starved the rest of the ready batch;
  - the precheck exit log;
  - replica output lost at shutdown.
- **Contained:** atomic slot migration import over a Rust connection crashes the target, because slot-import offsets are derived from `querybuf` and Rust bypasses it.
- **Open:**
  - the rtc beforeSleep work (B6);
  - CLIENT KILL during BGSAVE loses its reply over Rust;
  - no admission control. Under reconnecting, never-reading clients a fresh PING timed out after 10 s with 27,666 fds open against maxclients 10000, while the C build stayed responsive. The C listener throttles itself because accept runs on the busy main thread.

ASAN: 7 Tcl files in four modes, plus the smoke, hazard and backpressure scripts, plus 62 minutes of connection-churn soak (~2.4M connections, kills mid-argv, BLPOP kills, output-buffer kills) ran clean apart from the one use-after-free. Miri passes the crate's unit tests, which cover only the parser and the precheck barriers. clippy fails a deny-by-default lint on the `extern "C"` entry points.

Hardening (stage 5a, `rust-io/harden`). After rerunning the affected files on the final binary, RUST-IO-BUG is 0 in io and in rtc. The full suite ran one commit before the last fix.

- **Admission control** bounds four things:
  - unadopted connections, by pausing accept at 128;
  - fds, with a backstop at maxclients+16;
  - parsed commands held in Rust, through a global budget of 4096;
  - each connection's share once over that budget, with a 32-command fair share, so a pipelining connection cannot starve a fresh one.

  Under the overload soak, a fresh PING now answers in 13 ms p50 against C's 20 ms, where it used to time out at 10 s.
- **The CLIENT KILL during BGSAVE reply loss** was the immediate `shutdown(2)` (networking.c:2302 with lib.rs:1188). Shutdown now waits for the output to drain: 0/5 losses in all modes.
- **B5 was ineffective** after handoff v2, because kicks parked until a beforeSleep that never comes on the way to exit. Fixed.
- **rtc fixes:**
  - beforeSleep work (25-row audit) is now run after each batch or woken for.
  - The command time snapshot is refreshed when an IO thread takes the lock.
  - Lock fairness was added.
- **One gap is left open.** Output held in Rust (up to 1 MiB) is invisible to client-output-buffer accounting, so integration/throttle-repl fails. A naive fix broke obuf-limits. Zero-copy replies fix this structurally, because bytes stay in `c->reply` until written.

After hardening, the 186 remaining io failures are 171 MISSING-FEATURE (107 refused ASM imports, 38 RDB channel, 14 client memory accounting, 7 Lua debugger, 4 COB, 1 priority) and 14 TRANSPORT-ASSUMPTION.

## Impact on in-flight upstream work

- **AOF durability (#3381, major-decision-approved, targeting 10.0).** It holds replies behind per-client byte-offset fences in the C write path and brackets call().
  - Cut points 1 and 2 keep it working unchanged, because Rust replies still pass through `writevToClient`/`_writeToClient` before the copy.
  - A Rust-owned write path would have to rebuild the fences as "reply chunk plus release offset".
  - Rust's ability to stop reading a socket solves the output-buffer growth #3381 has no answer for.
  - Cut point 3 conflicts: the offset baseline, compute and woff live in call().
- **Sync replication (#3704) and Raft durability (RFC 33, Cluster V2).** Same fences, plus a committed offset that precheck would need published. Nobody has addressed per-engine offsets for a multi-engine design: a pipeline spanning engines would be released in client order and blocked on the slowest shard.
- **Data tiering (#3326).**
  - Its gate needs `dbFind`, blocks with BLOCKED_INUSE and re-executes, so it is class D and stays on the engine thread.
  - Cut point 2's design accommodates it: the gate goes after throttle, and re-execution ignores the precheck token and re-runs ACL and cluster checks live.
  - Cut point 3 conflicts unless Rust owns one generic pre-exec gate chain.
  - Shared-nothing makes tiering easier: per-shard storage and completions.
- **Forkless save (merged).** Its gate is call()'s first line, which conflicts with cut point 3.
- **Throttling (merged).** A class D gate, and a natural fit for read throttling in Rust.
- **Batch prefetch.** It needs batched cross-client handoff, which every mode keeps.
- **Module getkeys race (#5054).** Precheck has the same hazard, so module commands are never prechecked off-thread.
- **Chunky shards.** zuiderkwast's summit sketch, "Each IO thread can compute the slot and delegate to the right main thread", is cut point 2 plus multi-engine.
- **Maintainers.** The 2026-10-19 summit assigned this exact experiment ("rewriting the I/O system in Rust as a fast experiment"). No source endorses moving processCommand or call() into Rust, and the summit says "Multi-threading is hard, so we should look at alternatives like chunky shards".

## Vertical scaling: shared-nothing vs a concurrent hashmap

- **The engine blocks a concurrent keyspace, not the IO layer.**
  - Every read writes `val->lru` (db.c:120) and `server.stat_keyspace_hits` (db.c:123).
  - Propagation is decided from the `server.dirty` delta (server.c:4213, 4289), which ~107 sites mutate.
  - `executing_client` is a global, and modules assume the GIL.
  - Concurrency would mean reworking every t_*.c. Upstream tried reads on IO threads with per-slot exclusion (#2022: GET 924K to 3.1M) and closed the PR.
- **Shared-nothing reuses cluster semantics.** CROSSSLOT, single-slot MULTI and scripts, per-slot kvstore and SCAN cursors, and sharded pub/sub already define what one shard may touch. An in-process shard is a cluster node with a cheaper hop.
- **Its cost is de-singletoning the engine:** 551 `server` fields referenced 5,842 times in 82 files, plus ~222 mutable statics. That is identical at every cut point. Threads (a per-shard context) or processes (shared-memory rings) is the first decision.
- **Microbenchmark** (in-memory map, 16 threads across two L3s):
  - shared-nothing run-to-completion with batch 16: 62-114 Mops/s
  - 128 lock stripes: 33-48
  - lock-free papaya: 7.7 at 50% writes
  - one global lock: under 2

  Only shared-nothing kept scaling with zipf skew across L3s. Batching the cross-thread hop is worth 1.3-1.9x, and a cross-L3 cache-line handoff costs 171-191 ns against 48 ns within an L3.
- **Where the bottleneck sits.** A non-pipelined request costs ~14 us of server CPU here against ~0.6 us in call(). So the shards needed are about cores / (1 + IO:engine ratio): 1-4 on 16 cores, 3-16 on 64, 8-48 on 192. A single hot key caps zipf-0.99 traffic at ~15x one shard.
- **Measured shard ceiling on Valkey itself.** A K-shard cluster of valkey-server processes on the same 16 cores, each shard on its own slice:
  - 4 x 4 cores: 11.0M GET (C) and 10.3M (Rust precheck) at -P 16, against 2.61M for the best single engine, so 4.2x.
  - 8 x 2 cores: 6.7-6.8M SET, against 1.74M, so 3.9x.
  - K=1 cluster mode costs nothing on GET and 17-25% on SET.
  - -P 1 cluster runs saturate the client at about 800K, so the -P 1 sharding gain is unmeasured.
  - De-singletoning the engine is worth up to ~4x here. No IO-layer design gets more than 1.7x over C.

## Plan

Done on the fork: admission control, zero-copy replies (opt-in), argv return, `connection_external.c`, `engine_io.c` (partial), the processCommand checks/execute split, the cbindgen header, and unsafe confined to `ffi/` + `sys.rs`.

1. **Parity IO layer (cut point 1 plus class A on Rust threads).** Replace io_threads.c behind a build flag. Still needed to match C:
   - TLS
   - Unix sockets
   - the RDB channel and fork-child writes (38 tests)
   - Rust buffers counted in client memory (14 tests; zero-copy fixes the output side once `evictClients()` accounts for clients pending an async free)
   - CLIENT LIST qbuf
   - priority connections
   - atomic slot migration over Rust connections (107 tests; replicated-client offsets must come from `input_bytes`, not `querybuf`)
   - the Lua debugger over fork

   Keep the cluster bus in C at first. Exit criterion: the full suite at control parity, and RPS at equal thread count no worse than C io-threads. Today the suite has 0 RUST-IO-BUG and 188 MISSING-FEATURE/TRANSPORT-ASSUMPTION failures. RPS is ahead from 8 threads and 6% behind at 4 for pipelined GET.
2. **Finish the structure.** The `Runtime` handle instead of statics (the first N-engine affordance), the sans-IO connection state machine, and the rest of `engine_io.c` (deliver and stall still live in rust_io.c).
3. **Engine-side split, in C first.** Split processCommand into a thread-safe precheck (classes A, B and C against a published snapshot with an epoch) and an engine-residual gate chain (class D, throttle, tiering, forkless, the durability bracket) plus call(). This is upstreamable without Rust and helps the C IO threads too. Then run the precheck on Rust threads.
4. **Routing and N engines.** Decide threads vs processes for shards. Add batched per-shard queues in the IO layer keyed by the slot precheck already computes. Resolve replication (one merged stream or per-shard), maxmemory (global or per-shard), cross-shard scripts and standalone semantics.
5. **Upstream the C-only findings independently:** `call()` calling `zmalloc_used_memory()` per command (server.c:4429), which costs C io-threads up to 8% of the main thread at 16 threads, and the processCommand split.
6. **Do not pursue cut point 3.**

## Decisions for maintainers

1. Is cut point 2's split (precheck against a snapshot, plus an engine gate chain) acceptable as a C refactor ahead of any Rust?
2. Threads or processes for engine shards?
3. One cluster node per process presenting N shards, or N cluster nodes?
4. Who owns replica sockets and the RDB channel in a Rust IO layer, given that fork children cannot use Rust threads?
5. Is a Rust toolchain acceptable as an optional build dependency (`BUILD_RUST_IO=yes`) for 10.x?
