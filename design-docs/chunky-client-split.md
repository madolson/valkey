# Splitting `struct client` for a beat-for-beat Rust IO path

Job `client-split`, 2026-11-05. Base: `fork/epic/chunky-shards` = upstream unstable `686abfe75`. Every `file:line` below is at `686abfe75` unless prefixed `s2:` (branch commit `7c55b246f`). Raw data: `~/chunky/data/client-split/`. Branch: `chunky/client-split` (commits `9220fbc94` shape 1, `7c55b246f` shape 2).

## Summary

The IO path the task names (read/parse, write, io_threads.c: 47 functions) touches 52 `client` fields and flag bits with 567 references. 39 fields and 11 flag bits are shared with the C command layer; the rest of the 154 fields and bits are C-only. The read state is cheap to move: outside the IO path it has 121 references in 8 files, nearly all in replication, slot import, crons and CLIENT LIST. The reply buffers are not: C writes them in the addReply layer and also reads them directly for fake clients in module.c, call_reply.c, aof.c and replication.c (139 C-side references in 8 files). I prototyped shape 1 (bindgen view of the real struct) and shape 2 (a Rust-defined `#[repr(C)]` read-state struct embedded in `client` through a checked-in cbindgen header). Both build, link into valkey-server and pass the Tcl tests I ran (413 with shape 1; 1,020 with shape 2, replication, slot migration and AOF included). Measured, an FFI call into Rust costs the same as the C call it would replace in the reply path: 13.57 ns against 14.09 ns per three-append reply, medians of 5 runs. A server benchmark shows no throughput change from either shape: GET -P 16 medians are 887k base and 864k for both shapes, with the base run ranging 820-888k. Recommendation: port functions over the shape 1 view, move each field group into a shape 2 struct once its C IO users are gone, keep replies in C buffers with today's snapshot handoff, and turn the embedded struct into a pointer only when the chunky-shard session split starts. PARTIAL because shapes 3-5 are evaluated from code only, gtest unit tests could not be built (googletest is not installed), and only 13 test files ran, not the full suite.

## 1. Field ownership

Method: libclang 11 walked every server source (134 files, the `valkey-server` link list) and recorded every `MemberRefExpr` whose field belongs to `struct client` or `ClientFlags`, with read, write (assignment LHS, `++`, `--`) or address-of, and the enclosing function (`/tmp/chunky-client-split-tools/fieldrefs.py`, output `all.tsv`, 6,942 references). Functions are then classified (`classify.py`):

- **RD** read path (22 functions: `readQueryFromClient`, `readToQueryBuf`, `parseInputBuffer`, `parseMultibulk*`, `parseInlineBuffer`, `handleRead*`/`handleParse*`, shared query buffer, `processClientIOReadsDone`, `ioThreadReadQueryFromClient`, ...)
- **WR** write path (28: `writeToClient`, `_writeToClient`, `writevToClient`, iovec builders, `_postWriteToClient`, `trackBufReferences`/`releaseBufReferences`, `writeToReplica*`, `handleClientsWithPendingWrites`, `sendReplyToClient`, `ioThreadWriteToClient`, ...)
- **IOT** all of io_threads.c
- **SEAM** the parse-to-execute loop (`processInputBuffer`, `commandProcessed`, `consumeCommandQueue`, `beforeNextClient`, prefetch, memory_prefetch.c)
- **REPLY** the addReply layer in networking.c
- **CMD** everything else

Counts are exact member references, not grep hits. "IO" = RD+WR+IOT, "C" = SEAM+REPLY+CMD.

### 1.1 Shared fields (touched by both sides)

| Field | IO refs (RD/WR/IOT) | C refs (SEAM/REPLY/CMD) | IO writes | C writes | Where C touches it |
|---|---|---|---|---|---|
| `argv` | 5 (5/0/0) | 1,758 | 2 | 14 | every command proc (42 files) |
| `argc` | 7 | 716 | 3 | 13 | every command proc |
| `conn` | 42 (10/18/14) | 125 | 0 | 5 | unlinkClient networking.c:2249-2307, blocked.c:179-200, forkless.c:135-171, 452-460, CLIENT LIST networking.c:5053-5079 |
| `querybuf` | 77 (77/0/0) | 54 | 6 | 10 | query buffer resize cron server.c:986-1006, replDecodeToQueryBuf replication.c:294-315, streamReplDataBufToDb replication.c:3970, replicationCachePrimary replication.c:5444, slot import cluster_migrateslots.c:877-884, proxying in commandProcessed networking.c:4413, 4431, loop condition networking.c:4741, shared-qb check networking.c:4778, beforeNextClient trim networking.c:2648 |
| `qb_pos` | 52 | 7 | 7 | 4 | networking.c:2649, 4741; replication.c:5445; cluster_migrateslots.c:879, 882; server.c:996 |
| `qb_applied` | 5 | 7 | 5 | 5 | consumeCommandQueue networking.c:4618, beforeNextClient 2653-2654, commandProcessed 4413, slot import cluster_migrateslots.c:882, replication.c:5446 |
| `bulklen`, `multibulklen`, `reqtype` | 17, 11, 11 | 8, 2, 2 | 3, 2, 7 | 2, 2, 2 | createClient networking.c:371-386; replicationCachePrimary replication.c:5457-5459 |
| `querybuf_peak` | 6 | 9 | 3 | 4 | server.c resize cron, replication.c:315 |
| `nread`, `net_input_bytes` | 19, 1 | 1, 2 | 5, 1 | 1, 1 | createClient; CLIENT LIST networking.c:5079 |
| `read_flags` | 31 (27/0/4) | 21 | 12 | 9 | processInputBuffer networking.c:4747-4748, processCommand server.c:4695, 4716, 4792, getNodeByQuery cluster.c:1076-1108, prefetch networking.c:4677-4692 |
| `cmd_queue` | 2 | 20 | 1 | 8 | consumeCommandQueue, prefetch, prepareCommandQueue server.c:4629 |
| `parsed_cmd`, `argv_len`, `argv_len_sum`, `net_input_bytes_curr_cmd` | 1, 3, 4, 2 | 11, 14, 15, 6 | | | execution context the parser fills |
| `buf` | 8 (0/8/0) | 16 | 0 | 3 | createClient, freeClient, resize cron server.c:1037-1080, call_reply.c:825-842, module.c:972 |
| `bufpos` | 14 (0/12/2) | 35 | 1 | 9 | addReply layer networking.c:661-689, call_reply.c:821-828, module.c:794, 972-973, 7339, aof.c:1698, replication.c:5454, cluster.c:1573 |
| `reply` | 9 | 38 | 0 | 2 | addReply layer, call_reply.c:821-847, module.c:790, 974-978, 9301, aof.c:1698, server.c:4532 |
| `reply_bytes` | 1 | 15 | 1 | 7 | COB networking.c:6720, 6884-6888; AddReplyFromClient 1818; module.c:791 |
| `last_header`, `flag.buf_encoded` | 3, 7 | 4, 6 | 3, 1 | 4, 2 | upsertPayloadHeader call networking.c:665, 806, 823 |
| `io_last_written` | 27 | 8 | 12 | 4 | setDeferredReply networking.c:1242-1258, trimReplyUnusedTailSpace 1165, resize cron server.c:1072-1073 |
| `io_last_reply_block`, `io_last_bufpos` | 12, 11 | 1, 1 | 5, 6 | 1, 1 | createClient only |
| `nwritten`, `write_flags` | 23, 15 | 1, 1 | 10, 11 | 1, 1 | createClient only |
| `io_read_state` | 23 (6/4/13) | 6 | 8 | 1 | clientsCron server.c:1295, processPendingReplStreamDecode 1926, unlinkClient networking.c:2322, socket.c:206 (debug assert), cluster_migrateslots.c:2181 |
| `io_write_state` | 18 | 13 | 5 | 1 | same sites plus setDeferredReply 1242, trimReplyUnusedTailSpace 1165, clientHasPendingReplies 1866, socket.c:171, replication.c:232 |
| `clients_pending_write_node`, `flag.pending_write` | 1, 4 | 4, 10 | 1, 2 | 4, 3 | putClientInPendingWriteQueue networking.c:467-479, unlinkClient 2316-2319, module.c:9336-9338 |
| `io_tracked_reply_len` | 2 | 3 | 2 | 3 | COB networking.c:6726, 6887 |
| `last_interaction` | 2 | 11 | 2 | 2 | idle timeout, CLIENT LIST/KILL, replication |
| `flag.close_asap`, `flag.close_after_reply`, `flag.protected`, `flag.pending_command`, `flag.blocked`, `flag.unblocked`, `flag.primary`, `flag.lua_debug`, `flag.protocol_error` | 1-8 each | 1-44 each | | | requests from the command layer to the IO layer and back |
| `id`, `repl_data`, `mstate`, `slot_migration_job`, `cur_tid`, `net_output_bytes` | small | | | | |

Full table: `~/chunky/data/client-split/table.tsv`. C-side sites for the groups: `csites-read.txt`, `csites-write.txt`, `csites-reply.txt`.

### 1.2 By group

From `setcost.txt`:

| Group | Fields | IO refs | C-side refs | C files | Verdict |
|---|---|---|---|---|---|
| Read state | `querybuf qb_pos qb_applied reqtype multibulklen bulklen querybuf_peak nread net_input_bytes read_flags io_read_state cur_tid flag.pending_read flag.protocol_error` | 260 | 121 (39 writes) | 8 | Moves. `read_flags` should stay: it is execution state processCommand reads |
| Write cursor | `io_last_reply_block io_last_bufpos io_last_written nwritten write_flags io_write_state clients_pending_write_node io_tracked_reply_len net_output_bytes flag.pending_write` | 114 | 44 (14 writes) | 6 | Moves |
| Reply buffers | `buf buf_usable_size bufpos reply reply_bytes last_header buf_peak buf_peak_last_reset_time flag.buf_encoded` | 42 | 139 (29 writes) | 8 | Stays in C. C is the writer |
| Connection | `conn` | 42 | 125 | 13 | Stays in C (the connection abstraction stays) |
| Parse-to-execute handoff | `argv argc argv_len argv_len_sum cmd_queue parsed_cmd net_input_bytes_curr_cmd` | 24 | 2,540 | 43 | Stays in C; the IO path writes it |

The hot shared fields: `querybuf`/`qb_pos` (loop condition read on every parse iteration, networking.c:4741), `cmd_queue` (networking.c:4742), `read_flags` (written per command, networking.c:4747), `bufpos`/`buf`/`reply` (every addReply), `io_read_state`/`io_write_state` (every IO-thread handoff), `flag.pending_command` (per command, networking.c:4786).

Three constraints fall out of the sites, and they hold for every shape:

1. **`querybuf` must stay an sds.** The big-argument path hands the query buffer itself to the command layer as an `robj` (`createObject(OBJ_STRING, c->querybuf)`, networking.c:4359). Replicated clients proxy the exact bytes from it to sub-replicas and the backlog (networking.c:4431) and compute `reploff` from `sdslen(querybuf)` (networking.c:4413). Replication and slot import write it directly (replication.c:294-315, 3970, cluster_migrateslots.c:877-884). A Rust `Vec<u8>` query buffer would copy on all of those paths.
2. **The reply buffers have consumers that are not the write path.** Fake clients never reach `writeToClient`. Their replies are read straight out of `c->buf`/`c->reply` by RM_Call (module.c:972-978), call_reply (call_reply.c:821-847), temp-client release (module.c:790-794), AOF loading (aof.c:1698) and cluster slot caching (cluster.c:1573).
3. **The connection API is 48 `static inline` functions** in connection.h, so bindgen cannot link them. Rust can dispatch through the same `ConnectionType` vtable instead: the shape 1 prototype does this for `connSetPostponeUpdateState` (`src/rust/valkey-client/src/shape1.rs`), with no C shim and no extra call.

## 2. Shapes

FFI counts are per command on the hot path (non-replicated client, GET, pipelined). They are inferred from the cited code, not measured, except where marked. Common to every shape: one Rust-to-C call to execute the command (`processCommandAndResetClient`, networking.c:4453), one per argument to build the `robj` (`createStringObject`, networking.c:4367), and the `decrRefCount` of copy-avoided bulk replies after the write (`releaseBufReferences`, networking.c:3416). Robj refcounts are not atomic, so those releases stay on the main thread.

| | 1 Shared struct (bindgen) | 2 Embedded Rust-defined sub-struct | 3 Opaque Rust `IoClient` + accessors | 4 Accessor facade (C only) | 5 Inverted: Rust session, C `client` = context |
|---|---|---|---|---|---|
| C changes | Makefile + one call per ported function. Measured: 26 C lines for `parseInputBuffer` (commit `9220fbc94`) | Per group: read group = 290 refs rewritten in 5 files + server.h, 334+/210- (commit `7c55b246f`, 280 by tool, 10 by hand). Write group: 158 refs in 7 files (inferred from counts) | The 165 C-side read+write refs become calls; create/free allocate the object. +139 if replies move | Same 165 sites become inline functions or macros; no Rust | Session fields: `db` 522 refs (dbid half), `resp` 119, `id` 68, `user` 39, `name`/`lib_*` 52, auth/tracking/readonly flags, plus 19 `lookupClientByID` sites and 15 `server.clients` scans that become a registry (report 1 sections 1.5, 2) |
| Extra FFI per command | 0 (fields in place) | 0 | About 5 accessor calls: loop condition 4741 (2), shared-qb check 4778, `qb_applied` 4618, trim in beforeNextClient 2648 (per read). About 14 ns at 2.8 ns per call (microbenchmark, 3.6% of a 383 ns command at 2.61M/s); inferred | 0 | Session reads per command (`resp` in 19 reply-builder refs, `user` in the ACL check) must be served from a per-context cache, else one call each |
| What stays `unsafe` in Rust | Everything that touches `client`. bindgen drops `volatile` (`io_read_state` is a plain `u8`) and `_Atomic` (`io_tracked_reply_len` is a plain `u64`), so Rust has to remember `read_volatile` and atomic casts by hand (shape1.rs does this). Bitfield flags become accessor methods | Getting `&mut ClientIoRead` from `*mut client` (the aliasing guarantee is the `io_read_state` protocol, which Rust cannot check) and deref of the sds `querybuf`. Methods on the struct can be safe: `detect_reqtype` has one `unsafe` block for the sds read | The back-pointer to `client` and the sds. Rust can use its own types for everything except `querybuf` (constraint 1) | Not applicable | Same as 3, plus the session view the engines cache |
| IO threads and `io_*_state` | Unchanged protocol: main thread sets `PENDING`, the IO thread owns the fields until `COMPLETED` (io_threads.c). Rust IO threads take `*mut client` exactly as C does | The handoff unit becomes the sub-struct: the IO thread gets `&mut ClientIoRead` only. `io_read_state` stays in `client` because C reads it in 5 places (server.c:1295, 1926, networking.c:2322, socket.c:206, cluster_migrateslots.c:2181). Read group has 0 references from io_threads.c | The whole `IoClient` is handed off; `io_*_state` can move into it behind accessors | Neutral | IO threads own sessions permanently; no state handoff, only messages |
| Chunky-shard routing later | No help: `client` stays the unit, so you still need 5 | The IO group is already per connection. One step from 3: `ClientIoRead io` becomes `ClientIoRead *io`, mechanical (`c->io.` to `c->io->`), then N contexts can share it | The IO object is a natural per-session object N contexts point to | Neutral | Target state (report 1 section 5) |
| Incremental | Per function. Demonstrated: `parseInputBuffer` ported, 413 tests pass | Per field group. Demonstrated: read group moved, 1,020 tests pass | Per group, after 4 | Per group, C only, upstreamable alone | Not incrementally: the registry, barriers and deltas arrive together |
| Build | libclang is a build dependency of the Rust build only, because the layout depends on `LOG_REQ_RES` (server.h:1533) | Header checked in; C builds need no cargo (verified: `BUILD_RUST_CLIENT=no` build of `7c55b246f` compiles clean, no `vc_` symbols). `make check-rust-client-header` keeps it in sync | Header for the accessor API | None | As 3 |

Shape 6, found while reading: **message boundary only**. Rust owns the IO state, C owns `client`, and they share no fields; they exchange parsed-command batches and sealed reply chunks. This is report 1's section 3-4 boundary and is roughly what PR 207 does with `ioCommand`. For this direction it fails constraint 1: the 54 C-side `querybuf` references (replication, slot import, cron, CLIENT LIST) need a raw-stream mode or published stats before the field can leave C. It is the end state of 3 or 5, not a starting shape.

### 2.1 Reply path

| | A. C buffers, Rust writes from a snapshot | B. Rust reply buffer, addReply* calls into Rust | C. Hybrid: C appends, seals chunks at command boundaries, Rust writes and releases |
|---|---|---|---|
| C changes | None in the addReply layer. `payloadHeader` and `bulkStrRef` are defined in networking.c:122-140 (packed, bitfields, `_Atomic(uint8_t)`), so they must move to a header or be redefined in Rust for Rust to decode encoded buffers | The bottom of 52 addReply* functions (`_addReplyPayloadToBuffer` :654, `_addReplyPayloadToList` :697, `_addBulkStrRefToBufferOrList` :813, deferred replies :1179-1335, `AddReplyFromClient` :1790, `commitDeferredReplyBuffer` :1768, `trimReplyUnusedTailSpace` :1136) plus 139 C-side reply-buffer refs in 8 files, including the fake-client readers in constraint 2. The ~2,541 addReply call sites in server sources do not change | As A, plus a seal step after `afterCommand` joins pending pushes (server.c:4532) |
| FFI per command | 0 per append; one release batch per write completion | GET without copy avoidance = 3 appends (`addReplyBulk` networking.c:1548-1567). Measured: Rust call 13.57 ns vs C call 14.09 ns per 3 appends. Today's C is already a call: in the base binary `addReplyBulk` calls `addReply`, `addReplyProto`, `_addReplyLongLongWithPrefix`, `_addReplyPayloadToBuffer.part.12` (gdb disassembly). So B costs nothing extra now and about 2.8 ns per append against a future inlined C path (5.76 ns per 3 appends) | 0 per append; 1 per chunk |
| Snapshot semantics | Exactly today's: `io_last_reply_block`/`io_last_bufpos` (io_threads.c:767-777), `last_header = NULL` so the main thread starts a new header (io_threads.c:797-803) | Rust owns both the append and the write cursor, so no snapshot is needed, but all fake-client readers need Rust accessors or a separate C buffer | Generalizes A: a seal is the snapshot plus an end position (report 1 section 3.2) |
| Chunky shard | Works if each engine thread owns its contexts' buffers | Per-(session, engine) Rust buffers written from engine threads; possible, no advantage | Designed for it: chunks from N engines merge per session in the IO layer |

Recommendation: A now, C later. B buys nothing measurable and costs the fake-client paths.

## 3. Prototypes

Both build with gcc 7.3.1 and rustc 1.95, offline, with `make BUILD_RUST_CLIENT=yes` (Makefile target `rust-client-cargo`). Crate: `src/rust/valkey-client`.

**Shape 1** (`9220fbc94`). `build.rs` runs bindgen 0.70 over `server.h` and allowlists `client`, `connection`, `ConnectionType`. Layout tests are compile-time `offset_of!` asserts on every field (`client::io_read_state` at 362, `io_tracked_reply_len` at 408 at that commit). `vc_parse_input_buffer` is a line-for-line port of `parseInputBuffer` (networking.c:4519-4550). It reads `cmd_queue`, `conn`, `querybuf`, `qb_pos`, `io_read_state`, `io_write_state` and `flag.forkless_managed`, writes `reqtype` and `conn->flags`, and calls back into C `parseInlineBuffer`/`parseMultibulkBuffer`. Those two are non-static but declared in no header, so Rust declares them itself. `nm` shows `vc_parse_input_buffer` in valkey-server. Tests: `unit/protocol unit/querybuf unit/introspection unit/introspection-2 unit/type/incr unit/multi unit/obuf-limits`, 413 `[ok]`, "All tests passed" (`test-s1.log`).

**Shape 2** (`7c55b246f`). `ClientIoRead` (`querybuf qb_pos qb_applied querybuf_peak bulklen reqtype multibulklen nread net_input_bytes`) is a `#[repr(C)]` type in `shape2.rs`. cbindgen writes `src/client_io.h`, server.h includes it, and `struct client` embeds `ClientIoRead io` where `querybuf` was (s2:server.h). `rewrite.py` rewrote 280 references from libclang member locations. 10 more sit inside two multi-line macro-argument statements (networking.c:4343 and 5087 at `9220fbc94`), where libclang reports the macro's expansion point; the compiler found them and I fixed them by hand. Bindgen blocklists `ClientIoRead` and reuses the Rust type, so shape 1's layout tests now check the embedded copy. `vc_parse_input_buffer` calls the safe method `(*c).io.detect_reqtype()`. Tests: the 7 files above plus `unit/client-eviction`, 434 `[ok]` (`test-s2.log`); then `integration/replication integration/replication-4 integration/replication-psync unit/cluster/cluster-migrateslots integration/aof unit/introspection`, 586 `[ok]`, "All tests passed" (`test-s2b.log`). The first run stopped on a wrong test path I passed (`unit/cluster/atomic-slot-migration`); the second run covers it.

### 3.1 Measurements

**FFI microbenchmark** (`ffibench/`, `results.txt`). One GET-shaped reply = 3 appends (`"$3\r\n"`, value, `"\r\n"`) into a 16 KiB buffer, 200M iterations, `flock /tmp/chunky-bench-24-31.lock taskset -c 30 ./bench 200000000`, 5 runs. gcc -O3, separate TU, no LTO, for the C call; Rust `--release` staticlib.

| Variant | Median ns/reply | Range |
|---|---|---|
| C inline | 5.76 | 5.66-5.80 |
| C call (other TU) | 14.09 | 14.08-14.75 |
| Rust `extern "C"` writing a C-layout buffer (shapes 1/2) | 13.57 | 12.46-15.88 |
| Rust opaque handle, `Vec<u8>` (shape 3, reply option B) | 13.56 | 13.54-17.93 |

**Server throughput** (`bench/raw.txt`, `bench.sh`). Four binaries: `base` = `686abfe75`, `s1` = `9220fbc94` with Rust, `s2c` = `7c55b246f` without Rust (layout change only), `s2` = `7c55b246f` with Rust. Server `taskset -c 24`, single main thread, no IO threads. Client `taskset -c 26-31 valkey-benchmark -P 16 -c 50 --threads 4 -r 100000` (8M requests) and `-P 1` (2M). 3 interleaved rounds under the bench lock.

Requests per second, median of 3 rounds (min-max):

| Test | base | s1 | s2c | s2 |
|---|---|---|---|---|
| GET -P 16 | 887k (820-888) | 864k (841-864) | 864k (840-864) | 864k (842-864) |
| SET -P 16 | 743k (549-743) | 761k (760-761) | 743k (743-760) | 761k (743-761) |
| GET -P 1 | 84k (73-84) | 83k (82-83) | 84k (82-84) | 83k (81-85) |
| SET -P 1 | 81k (75-82) | 81k (79-82) | 82k (82-82) | 82k (82-84) |

The whole run took 17.5 minutes under the lock (08:23-08:40 UTC). The -P 1 numbers are bound by round-trip latency (p50 about 0.56 ms with 50 clients), so they say nothing about server cost.

The absolute numbers are a third of the research's 2.61M GET -P 16 because the server and client share cores 24-31 with four other agents and the client has only 6 cores. Only the relative comparison means anything, and the differences are inside the round-to-round spread.

## 4. Recommendation

1. **Start with shape 1 for porting functions.** Each C function is replaced by a Rust one over the bindgen view, one slice at a time, each slice passing the suite. Rust dispatches `conn*` through the `ConnectionType` vtable, so socket.c, unix.c, tls.c and rdma.c stay untouched.
2. **Move each field group into a shape 2 struct** once its C IO users are ported: read state first (done in the prototype), the write cursor second. Leave `read_flags`, `argv`/`cmd_queue`, `conn` and the reply buffers in `client`. Rust then owns typed structs, and the IO-thread handoff hands over `&mut ClientIoRead` rather than `*mut client`.
3. **Replies:** keep C buffers and the snapshot handoff (option A). When engines multiply, evolve to sealed chunks (option C).
4. **For chunky shards,** turn `ClientIoRead io` into a pointer when the session split starts. The IO structs become the session's IO half, and shape 5's session work (identity, auth, protocol, registry) proceeds from there.

Shape 3 loses because constraint 1 keeps `querybuf` an sds anyway, so the Rust-native types it promises do not materialize for the hottest field, and it adds about 5 calls per command plus a second allocation per client. Shape 4 is unnecessary: the shape 2 rewrite was tool-driven from exact libclang locations, so a facade layer adds maintenance without saving work. It only pays if shape 3 is chosen.

## 5. Decisions for madolson

1. **Bindgen at build time (libclang dependency for Rust builds) or checked-in bindings?** Proposal: build time. The layout changes with `LOG_REQ_RES`, so checked-in bindings would need a variant per config.
2. **Embedded (`c->io.x`) or pointer (`c->io->x`) for moved groups?** Proposal: embedded now, pointer when the chunky-shard session split starts.
3. **Which groups move?** Proposal: read state and write cursor. Not reply buffers, `read_flags`, `argv`/`cmd_queue`, or `conn`.
4. **Rust IO threads with today's `io_*_state` handoff, or message passing now?** Proposal: today's handoff, beat-for-beat. Message passing (shape 6) needs a raw-stream answer for the replication `querybuf` uses first.
5. **Check in cbindgen headers with a CI diff check?** Proposal: yes (`check-rust-client-header`). It also needs a clang-format step pinned to CI's clang-format-18, as PR 207 has; this box has no clang-format-18, so the prototype header is not formatted.
6. **`payloadHeader`/`bulkStrRef` to a header** (needed by a Rust writer over C buffers): move them to server.h, or define them in Rust as `#[repr(C, packed)]` and export them through cbindgen?

## 6. Strongest evidence against

- **Embedding entrenches one IO state per `client`.** A chunky shard needs one session and up to N execution contexts, and report 1 argues the IO layer must own the session (report 1 section 5.1). Shape 2 puts the IO state inside the struct that becomes the per-engine context, so the pointer conversion and a session object are still owed. The conversion is mechanical; the session work (shape 5) is not, and none of shapes 1-2 advances it.
- **Shapes 1 and 2 leave Rust mostly `unsafe`.** In the prototype, every line of `vc_parse_input_buffer` sits in an `unsafe fn`. The ownership protocol between the main thread and IO threads (`io_read_state`) is a convention Rust cannot check, and bindgen silently drops `volatile` and `_Atomic`. The memory-safety benefit of the rewrite is small until groups move into Rust-defined types and the handoff is restructured.
- **The measured gain does not come from the split.** Report 7 notes the prototype's IO-layer designs top out around 1.7x while separate engines give about 4x (report 7 summary, citing 207:`design-docs/rust-io-layer.md:265`). The split here buys a maintainable Rust port, not throughput; the benchmark shows no change either way.

## 7. Verified vs inferred, and what I could not test

Verified (code read or run): every count in section 1 (libclang, exact); constraints 1-3 at the cited lines; both prototypes build and link and pass the listed tests; a C-only build of shape 2 compiles without Rust; the microbenchmark and server benchmark numbers; in the base binary, `addReplyBulk` calls rather than inlines the append layer (gdb).

Inferred: the per-command FFI counts and cost for shapes 3 and 5; C change sizes for the write group, shape 3, shape 4 and shape 5; reply options B and C; all chunky-shard statements.

Not tested:
- gtest unit tests: `make test-unit` fails with "googletest not found" (`test-unit-s2c.log`). The one-line change in `src/unit/test_networking.cpp` is not compiled.
- Full `make test`: only 13 test files ran, for time.
- TLS and RDMA builds: not built. Shape 1 calls through the vtable, so they should be unaffected, but that is unverified.
- IO threads > 1 under the Rust parse path: only as far as the listed tests enable io-threads.
- Shapes 3, 4 and 5 were not prototyped.
- clang-format-18 is not installed, so formatting is unchecked.
