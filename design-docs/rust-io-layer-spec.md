# valkey-io: Rust IO layer for valkey-server (specification)

Normative description of the code on branch `rust-io/iter4` at the commit that adds this file. Paths are relative to the repository root. `file:line` citations under `src/` were taken at `a65218da2` and still hold: the later commits change `src/` only in `src/rust/valkey-io/src/benchapi.rs`. Test citations are at this commit. Where code and older documents (`src/rust/valkey-io/DESIGN.md`, `ITERATIONS.md`, `design-docs/rust-io-layer*.md`) disagree, this document follows the code.

## 1. Scope and status

The layer replaces the C socket transport for TCP client connections. With `rust-io-takeover yes` Rust binds the server's main `port` and the C TCP listener is not created (`src/server.c:3348`). Rust owns the listening socket, the client sockets, `read(2)`, RESP parsing, building the argv objects, and `write(2)`, on its own IO threads. The C engine keeps one ordinary `client` per connection, backed by a connection of ConnectionType `"external"` (`src/connection_external.c:149`), and runs `processCommand()` and `call()` on its main thread.

Two modes (`src/rust_io.h:14-18`):

| `rust-io-mode` | IO thread does | Engine thread does |
|---|---|---|
| `io` (cut point 1) | accept, read, parse, argv build, write | `prepareCommandGeneric()` (lookup, arity, slot), all of `processCommand()`, `call()` |
| `precheck` (cut point 2) | `io` plus lookup, arity, slot, the auth check, the ACL check against a policy snapshot, session-dependent eligibility, and a token per command | token validation, every live-state check, `call()` |

Not supported: TLS, Unix sockets, RDMA, the cluster bus, outbound connections (a replica's link to its primary, cluster bus links and `MIGRATE` targets stay C sockets). Only the first `bind` address is bound (`src/rust_io.c:775`).

Maturity: research prototype. One runtime per process, never stopped (`src/rust/valkey-io/include/valkey_io.h:97-102`). Not upstreamed, not in the default build.

## 2. Build and configuration

### 2.1 Build

| Variable | Effect | Lines |
|---|---|---|
| `BUILD_RUST_IO=yes` | `-DUSE_RUST_IO`; links `rust/valkey-io/target/release/libvalkey_io.a` plus `-lpthread -ldl -lrt -lm -lgcc_s` | `src/Makefile:433-442` |
| | adds `connection_external.o engine_io.o rust_io.o rust_io_precheck.o rust_io_timing.o` | `src/Makefile:785` |
| | every server build runs `cargo clippy --release -- -D warnings` then `cargo build --release` (cargo decides staleness) | `src/Makefile:786-792` |
| | defines `rust-header` and `check-rust-header` (cbindgen piped through `$(CLANG_FORMAT)`) | `src/Makefile:793-804` |
| | persisted in `.make-settings`, so later `make` invocations keep it | `src/Makefile:752`, `src/Makefile:125` |
| `RUST_IO_TIMING=yes` (with `BUILD_RUST_IO=yes`) | `-DRUST_IO_TIMING`, cargo `--features timing` | `src/Makefile:437-440` |
| unit tests | `src/unit/Makefile` adds `-DUSE_RUST_IO` (and `-DRUST_IO_TIMING`) and links the static library | `src/unit/Makefile:244-253` |

The default build (no `BUILD_RUST_IO`) compiles and links no Rust code and none of the five bridge objects; everything in `rust_io.h` and `rust_io_precheck.h` is under `#ifdef USE_RUST_IO` (`src/rust_io.h:10`, `src/rust_io_precheck.h:9`). The default build does contain these unconditional C changes from the branch:

| Change | Lines |
|---|---|
| `acl_mutation_epoch` and its 5 increments; `ACLCopyUser()` no longer static | `src/acl.c:48`, `src/acl.c:559`, `:609`, `:1014`, `:1770`, `:3035` |
| `moduleCommandFilterCount()` | `src/module.c:15204` |
| `processCommand()` split into `processCommandChecks()` / `processCommandExecute()`; `prepareCommandGeneric()` non-static | `src/server.c:4694`, `:4752`, `:4999`, `src/server.h:3686` |
| peak memory: `call()` updates `stat_peak_memory` only when this thread's allocations grew (`zmalloc_thread_used_memory()`); `getMaxmemoryState()` also updates it | `src/server.c:1548-1555`, `:4464`, `src/zmalloc.c:548`, `src/evict.c:294` |
| eviction: memory of normal/pubsub clients in `clients_to_close` and `io_inflight_reply_bytes` (always 0 without Rust) are not counted | `src/evict.c:238-253` |
| `ioThreadsEnterExternal()` | `src/io_threads.c:135` |
| `parsedCommand.rust_precheck`, `client.rust_precheck`, `server.io_inflight_reply_bytes` | `src/server.h:1388`, `:1435`, `:2047` |

### 2.2 Configuration

All defined in `src/config.c` only under `USE_RUST_IO`.

| Config | Type | Default | Mutability | Meaning | Line |
|---|---|---|---|---|---|
| `rust-io-mode` | enum `off`/`io`/`precheck` | `off` | immutable | Selects the mode; `off` starts nothing | `src/config.c:3747` |
| `rust-io-port` | int 0..65535 | 0 | immutable | Extra port served by Rust while the C listener keeps `port`. Ignored under takeover. 0 disables | `src/config.c:3748` |
| `rust-io-threads` | int 1..128 | 1 | immutable | Rust IO threads (`rust-io-<n>`) | `src/config.c:3749` |
| `rust-io-takeover` | bool | no | immutable | Rust binds `port`; the C TCP listener is not created | `src/config.c:3750` |
| `rust-io-zero-copy-min-bytes` | int -1..INT_MAX | 2048 | modifiable | Minimum `c->bufpos` for a zero-copy write job when the reply is only in `c->buf`; -1 always copies (section 5d) | `src/config.c:3751` |

The layer starts in `rustIoInitListeners()` (`src/rust_io.c:769`) if the mode is not `off` and the chosen port (`port` under takeover, else `rust-io-port`) is nonzero. A bind failure logs `Failed listening on rust-io port` and exits (`src/rust_io.c:794-797`). The C-built `rust_io_config` defaults (`src/rust_io.c:50`) are overwritten by the config table.

Existing configs that reach Rust by value: `proto-max-bulk-len` and `client-query-buffer-limit` (apply function `applyRustIoLimits`, `src/config.c:74-81`, `:3677`, `:3698`, calling `vio_set_limits`); `maxclients` and the default user's auth requirement are republished every event-loop iteration (`src/rust_io.c:637-639`); `tcp-keepalive` is applied at adoption (`src/rust_io.c:524`). `bind` and `port` changes are refused under takeover (section 9).

### 2.3 INFO

Both sections are printed only when requested by name, not by `INFO`, `all` or `everything` (`src/server.c:7295-7306`). Neither is reset by `CONFIG RESETSTAT` except the timing histograms (`src/server.c:2954-2956`).

`INFO rust_io` (`rustPrecheckCatInfo` `src/rust_io_precheck.c:370`, then `rustIoCatInfo` `src/rust_io.c:499`):

| Field | Meaning |
|---|---|
| `rust_io_mode` | `precheck`, `io`, or `other` (mode off) (`src/rust_io_precheck.c:397`) |
| `precheck_fast` | commands that took the fast path |
| `precheck_rejected` | predicted rejections that validated and then ran the full path |
| `precheck_fallback` | sum of all `precheck_fallback_*` |
| `precheck_fallback_no_snapshot`, `_barrier`, `_modules`, `_ineligible`, `_multi`, `_pubsub_resp2`, `_special_client`, `_acl_user`, `_no_session` | fallbacks decided on the IO thread (`src/rust_io_precheck.h:28-36`) |
| `precheck_fallback_epoch`, `_session`, `_modules_live`, `_cluster` | fallbacks decided on the engine thread (`src/rust_io_precheck.h:38-41`) |
| `precheck_snapshots_built`, `precheck_snapshots_freed` | ACL snapshot lifecycle |
| `acl_mutation_epoch` | current epoch |
| `rust_io_zero_copy_min_bytes` | current threshold |
| `rust_io_writes_submitted` | zero-copy jobs handed to Rust (lifetime) |
| `rust_io_writes_outstanding` | jobs not yet returned (0 when idle) |
| `rust_io_held_input_bytes` | sum of private read-buffer capacity (`vio_held_input_total`) |

Fields in the counters are printed in `io` mode too (all precheck values 0). A `rust-io precheck: fast=... rejected=... barrier=... epoch=... session=... cluster=...` line is logged at exit when any precheck counter is nonzero (`src/rust_io_precheck.c:193-200`).

`INFO rust_io_timing` (only `RUST_IO_TIMING` builds, `ritCatInfo` `src/rust_io_timing.c:115`): `rust_io_timing_sample_n`, `rust_io_timing_dropped`, `rust_io_timing_evicted`, `rust_io_timing_queued`, `rust_io_timing_replied`, `free_argv_calls`, `free_argv_ns_per_call`, and `stage_<name>:count=,p50=,p90=,p99=,max=` for `parsed argv precheck enqueued dequeued process_entry call_entry call_exit reply_copied written end_to_end` (`src/rust_io_timing.c:112-135`; a stage is its stamp minus the latest earlier stamp present).

Standard fields fed by the layer: `rejected_connections` adds IO-thread refusals (`src/rust_io.c:640`); `client_query_buffer_limit_disconnections` (`src/rust_io.c:561`); `total_reads_processed` counts one per submit, not per `read(2)` (`src/engine_io.c:96-101`).

### 2.4 Test and measurement knobs

| Knob | Read at | Effect |
|---|---|---|
| env `VIO_HANDOFF_DISABLE` | `vio_start` (`src/rust/valkey-io/src/lib.rs:1430`) | bit 1: no main-thread kick batching; bit 2: always write the IO-thread eventfd; bit 4: always write the main eventfd (`lib.rs:323-330`, `:370-372`). Default 2 (IO-thread awake check off, the other two optimizations on). 7 is the original protocol |
| env `RUST_IO_TIMING_N` | first timing use (`src/rust/valkey-io/src/timing.rs:67`) | sample 1 command in N (default 64), timing builds only |

There are no DEBUG subcommands for the layer.

Log lines: `Rust IO layer listening on <addr>:<port> with <n> thread(s)[, precheck]` (`src/rust_io.c:807`), `Ready to accept connections rust` (`:901`), `Accepted <ip>:<port> (rust-io)` at verbose (`:523`), `Client closed connection (rust-io) id=<id>` at verbose (`:572`).

## 3. Architecture

### 3.1 Threads

| Thread | Owns | Lines |
|---|---|---|
| Engine thread (the thread that called `vio_start`, the server main thread) | all engine state; `rustConnection` structs; `MainState` (kick batches, returned argv batches, current ready batch, completed jobs) in `Runtime.main: RefCell` | `lib.rs:347-367`, `src/rust_io.c:65-76` |
| `rust-io-<i>`, i in 0..`rust-io-threads` | its epoll fd and inbox; per connection a `Local` (read buffer, parser, write buffer, precheck sequence state); a shared read buffer; the ready list it is building | `lib.rs:513-566`, `:1445-1468` |
| `rust-io-0` additionally | the listening socket and `accept4(2)`; round-robin distribution of new fds | `lib.rs:1425`, `:1300-1325` |
| C IO threads (`io-threads`) | unchanged, but never read, write or free argv for external connections (section 8.6) | `src/io_threads.c:708-711`, `:758-761`, `:1029-1031` |
| Fork children | no IO threads; external connections use the fd directly | `src/connection_external.c:17-19` |

Shared per connection (`Conn`, `lib.rs:176-221`), always behind `Arc`: lifecycle word `state` (conn_state.rs), atomics (`events`, `want_writable`, `read_paused`, `auth_required`, `parse_held`, `out_bytes`, `ncmds`, `in_len`, `in_cap`, `priority`, `detach`, `job`, `udata`), a `Mutex<Shared>` holding the parsed-command queue, the outbound copy buffer, the protocol-error sample and queue byte counters (`lib.rs:133-144`), and the precheck `SessionView` seqlock (`src/rust/valkey-io/src/precheck.rs:60`).

Global (`Global`, `lib.rs:306-336`): callbacks, limits, IO-thread table, the engine's ready mailbox, listener fd, admission counters, `default_auth_required`, `handoff_disable`, `rbuf_total`, `prio_conns`. Leaked to `'static` at start (`lib.rs:1431`).

### 3.2 One command, end to end

| Step | Thread | What happens | Lines |
|---|---|---|---|
| accept | rust-io-0 | `accept4(SOCK_NONBLOCK\|SOCK_CLOEXEC)`, `TCP_NODELAY`; admission (`admit`); refuse past maxclients+16; round robin to thread `rr % n` (local `add_conn` or `Msg::NewFd`) | `sys.rs:115-129`, `lib.rs:1300-1325` |
| register | owning IO thread | `Conn` created with `auth_required = default_auth_required`, slab slot, `EPOLLIN`, `notify(VIO_EV_NEW)` | `lib.rs:642-702` |
| hand over readiness | IO thread | `notify` sets `events`, pushes the conn once per `IN_READY`; at the end of the poll iteration `flush_notify` posts all of them (and completed jobs) to the ready mailbox with one wakeup | `lib.rs:591-611`, `:1385` |
| adopt | engine | eventfd handler `rustIoEventHandler` -> `vio_main_begin_drain` -> `rustIoDrainReady`; or `rustIoAfterPoll` when no eventfd was written. `vio_main_next_ready` -> `rustIoHandleReady` -> `vio_conn_take_events`; on `VIO_EV_NEW`, `rustIoAccept`: `connCreateExternal`, `vio_conn_retain`, `vio_conn_set_udata`, `acceptCommonHandler`, first `rustIoPublish` | `src/rust_io.c:622-629`, `:584-620`, `:535-577`, `:513-531` |
| read | IO thread | on `EPOLLIN`, borrow the thread's shared buffer if the conn has none; read `READ_CHUNK` (16 KiB) or exactly the rest of a bulk of 32 KiB or more; check `client-query-buffer-limit` (and 1 MiB while unauthenticated) on unparsed bytes | `lib.rs:765-776`, `:866-916` |
| parse | IO thread | `Parser::parse` loop. Multibulk arguments are byte ranges into the read buffer; inline arguments are owned vectors. While `auth_required`, stop after one command and set `parse_held` | `lib.rs:922-1011`, `src/rust/valkey-io/src/resp.rs:248` |
| argv build | IO thread | one `create_argvs` call per batch: `robj **` arrays of string objects (zmalloc) | `lib.rs:1012-1013`, `src/rust_io.c:107-116` |
| precheck (precheck mode) | IO thread | one `precheck_batch` call per batch with the pinned snapshot and the session view | `lib.rs:1018-1024`, `precheck.rs:128-143` |
| enqueue | IO thread | under the `Shared` lock append the batch, update `ncmds`, byte counters, error sample; admission may set `read_paused`; `notify(conn, 0)` | `lib.rs:1034-1058` |
| submit | engine | `rustIoDeliverBatched` (shallow queues, into the key-prefetch batch) or `rustIoDeliver`; `rustIoPull` pops at most 128 per call while `cmd_queue` holds fewer than 1024 (16 per call for a replicated client); `engineSubmit` appends to `cmd_queue`, running `prepareCommandGeneric()` unless prechecked; `engineAccountRead` | `src/rust_io.c:359-375`, `:334-347`, `:168-192`, `src/engine_io.c:45-101` |
| execute | engine | `processInputBuffer()` or `processClientsCommandsBatch()` -> `consumeCommandQueue()` (copies the token, `src/networking.c:4606`) -> `processCommand()` -> `rustPrecheckValidate()` -> `call()` | `src/server.c:4999-5119` |
| after commands | engine | pending protocol error replied once the queue is empty; `trimCommandQueue`; `rustIoPublish` (session if changed, `consumed`/`auth_required` if changed) | `src/rust_io.c:299-306`, `:204-228` |
| argv free | engine -> IO thread | `freeClientArgv` -> `tryOffloadFreeArgvToIOThreads` -> `rustIoTryOffloadFreeArgv` -> `vio_conn_return_argv` (section 5c) | `src/networking.c:2210`, `src/rust_io.c:130-150` |

### 3.3 One reply

| Step | Thread | What happens | Lines |
|---|---|---|---|
| build | engine | `addReply*` into `c->buf` / `c->reply` as for any client | |
| decide | engine | `handleClientsWithPendingWrites()` / `writeToClient()` call `trySendWriteToIOThreads()`, which routes external connections to `rustIoTrySendWrite()` | `src/networking.c:3830`, `:3590`, `src/io_threads.c:760` |
| zero-copy path | engine then IO thread | seal and `vio_conn_submit_write` (section 5d) | `src/rust_io.c:412-450` |
| copy path | engine | `writeToClient` -> `_writeToClient` -> `connWritev` -> `connExternalWritev` -> `rustExtWritev` -> `vio_conn_writev`: bytes copied into `Shared.out` up to `OUT_HIGH` pending, `out_bytes` raised, `kick` | `src/connection_external.c:65-77`, `src/rust_io.c:692-701`, `src/rust/valkey-io/src/ffi/exports.rs:339-383` |
| kick | engine | coalesced per conn, batched per IO thread, handed over at the end of `beforeSleep()` (section 5b) | `lib.rs:377-456`, `src/rust_io.c:816-820` |
| write | IO thread | `on_kick` -> `flush`: swap `Shared.out` into the thread-local `wbuf` under the lock, `write(2)` until EAGAIN, decrement `out_bytes`; then the write job if any; `VIO_EV_WRITABLE` if the engine wants it and `out_bytes < OUT_LOW` | `lib.rs:1135-1173`, `:1064-1133` |
| EAGAIN from Rust | engine | `vio_conn_writev` returns -1/EAGAIN at 1 MiB pending; the engine keeps the rest in its COB and installs its write handler (`want_writable`), which fires on `VIO_EV_WRITABLE` | `exports.rs:346-349`, `src/rust_io.c:542-544` |

### 3.4 Ownership

| Object | Created by | Owner, in order | Freed by |
|---|---|---|---|
| socket fd | `accept4` on rust-io-0 | `Conn.fd` (Rust) for the connection's life; the engine's `conn->fd` is the same number, used for addresses, keepalive, and direct IO while detached or in a fork child | `close(2)` in `Drop for Conn` when the last `Arc` drops (`lib.rs:223-237`). After a peer close the fd stays open until the engine closes its side, so the number is not reused (`lib.rs:743-763`) |
| `Conn` (`Arc`) | `add_conn` | strong refs: the IO thread's slab `Local`, ready/kick/detach messages in flight, the ready batch, one `VioConnHandle` per `vio_main_next_ready` (released by `rustIoDrainReady`, `src/rust_io.c:610`) and one long-lived handle `rc->h` from `vio_conn_retain` at adoption | `rc->h` released in `rustExtClose` (`src/rust_io.c:703-710`); slab ref dropped by `remove()` (`lib.rs:704-715`) |
| `rustConnection` (embeds `connection`) | `connCreateExternal` (`zcalloc`), `src/connection_external.c:192-200` | engine | `connExternalClose`: `zfree`, or deferred with `CONN_FLAG_CLOSE_SCHEDULED` while handlers hold refs (`src/connection_external.c:49-56`). Rust keeps its pointer as `udata` (never dereferenced) until `vio_conn_close` clears it (`exports.rs:493`) |
| argv (`robj **` + objects) | `create_argvs` on the IO thread | `Shared.cmds` (Rust) -> `vio_conn_pop_cmds` moves it to the engine -> `parsedCommand` -> `c->argv` | after execution: objects with refcount > 1 decremented on the engine thread, the rest returned to and freed on the building IO thread (threads >= 2); else freed on the engine thread. Queued and never popped: `free_argv` from `vio_conn_close` (engine thread, `exports.rs:480-492`) or from `Drop for Conn` (either thread) |
| inline argument bytes | parser (`Arg::Owned`) | `Worker.held` until `create_argvs` returns | cleared after the batch (`lib.rs:1026`) |
| copied reply bytes | engine buffers | copied into `Shared.out` during `vio_conn_writev`; the engine's iovecs are borrowed only for the call | Rust `Vec` (system allocator) |
| zero-copy job (`client *`) | engine | engine memory lent to the IO thread from `vio_conn_submit_write` until it comes back through `vio_main_next_done` | released on the engine thread by `processClientIOWriteDone()` (`src/rust_io.c:477-488`) |
| policy snapshot | `pcBuild` (engine) | published in `pc_current`, pinned by IO threads through hazard slots | `pcReclaim` on the engine thread once no hazard slot holds it (`src/rust_io_precheck.c:154-187`) |

## 4. Engine boundary (normative)

Calling-thread vocabulary: **E** engine thread (the `vio_start` caller), **IO** a rust-io thread, **any**. Frequency: per command, per batch (one parsed batch or one delivery), per iteration (one event-loop iteration), per connection, rare.

All lists in this section were generated mechanically. To regenerate, from the repo root:

```sh
H=src/rust/valkey-io/include/valkey_io.h
grep -nE '^[A-Za-z].*\bvio_[a-z_]+\(' $H | sed -E 's/^([0-9]+):.*\b(vio_[a-z_]+)\(.*/\1 \2/'          # exports
awk '/^ Engine callbacks/,/\} VioCallbacks;/{print NR": "$0}' $H | grep -E '\(\*' \
  | sed -E 's/^([0-9]+):.*\(\*([a-z_]+)\).*/\1 \2/'                                                  # VioCallbacks
grep -nE '^[a-z].*\bengine[A-Z][A-Za-z]+\(' src/engine_io.h | sed -E 's/^([0-9]+):.*\b(engine[A-Za-z]+)\(.*/\1 \2/'
grep -nE '\(\*[a-z_]+\)' src/connection_external.h | sed -E 's/^([0-9]+):.*\(\*([a-z_]+)\).*/\1 \2/'  # externalConnOps
awk '/^static ConnectionType CT_External = \{/,/^\};/{print NR": "$0}' src/connection_external.c | grep -v 'NULL\|{\|}'
```

The header itself is generated: `make -C src rust-header` (needs `cbindgen` and `BUILD_RUST_IO=yes` in effect, `src/Makefile:784`, `:799`).

### 4.1 Shared types

| Type | Header line | Contract |
|---|---|---|
| `VioCmd` | `valkey_io.h:107-135` | Same layout as `engineCmd` (`src/engine_io.h:15-27`), enforced by 12 `static_assert`s (`src/rust_io.c:33-44`). `err != 0` marks a protocol-error entry (always the last of a batch, `argv` NULL). `trace` is always present (`valkey_io.h:118-124`). `cmd`, `slot`, `read_flags`, `token` are filled by the precheck; `token == 0` means not prechecked |
| `VioSession` | `valkey_io.h:137-142` | `user` is an identity key only; `gen` moves when any field changes; `flags` are `VIO_SESS_*` (`valkey_io.h:56-65`) |
| `VioConfig` | `valkey_io.h:183-194` | read once by `vio_start` |
| `VioConnMem` | `valkey_io.h:213-233` | per-connection held memory, see `vio_conn_mem` |
| `VIO_EV_*` | `valkey_io.h:12-24` | `NEW`, `CLOSED`, `WRITABLE`, `QBLIMIT`, `MEM` |
| `VIO_ERR_*` | `valkey_io.h:29-51` | tied to `resp::ProtoError` by const asserts (`src/rust/valkey-io/src/ffi/mod.rs:24-36`) |
| `VioConnHandle` / `VioConnRef` | `valkey_io.h:202`, `:208` | both `VioConn *` in C. A handle owns one strong count (`ffi/mod.rs:63-77`); a ref borrows for one call and requires that the caller hold a handle (`ffi/mod.rs:83-108`) |

### 4.2 Exported `vio_*` functions (42: 35 always, 7 with `RUST_IO_TIMING`)

Every export is `pub unsafe extern "C"` in `src/rust/valkey-io/src/ffi/exports.rs`. A Rust panic aborts the process (`panic = "abort"`, `src/rust/valkey-io/Cargo.toml:56`).

| # | Function | Header | Thread | Frequency | Pointers, ownership | Returns / errors |
|---|---|---|---|---|---|---|
| 1 | `vio_start(cfg, efd)` | `valkey_io.h:255` | E (becomes the runtime's engine thread) | once (`src/rust_io.c:791`) | `cfg` borrowed for the call, `bindaddr` copied, callbacks copied by value; `*efd` receives the engine eventfd, which the engine polls forever | runtime pointer valid until exit (leaked, `lib.rs:1431`, `:1444`); NULL with `errno` (bind, epoll, eventfd failures) |
| 2 | `vio_set_limits(rt, bulk, qbuf)` | `:263` | any | rare (CONFIG SET apply) | none | none. Relaxed stores; applies to reads that start later (`lib.rs:279-304`) |
| 3 | `vio_main_flush_kicks(rt)` | `:275` | E | per iteration (`src/rust_io.c:818`), plus `waitForClientIO` (`src/io_threads.c:150`), `rustExtDetach`, `rustIoFlushReplicas` | none | none. Also folds the engine's popped count into the admission budget (`exports.rs:61`) |
| 4 | `vio_main_begin_drain(rt)` | `:284` | E | per main eventfd wakeup (`src/rust_io.c:627`) | none | none. Drains the eventfd, then takes the ready batch |
| 5 | `vio_main_before_poll(rt)` | `:293` | E | per iteration, only when the loop may sleep (`src/rust_io.c:819`) | none | 1: work pending, poll with zero timeout |
| 6 | `vio_main_after_poll(rt)` | `:304` | E | per iteration (`src/rust_io.c:829`) | none | 1 when it took work handed over while awake; 0 when nothing, or when an eventfd write is pending (its handler takes it) |
| 7 | `vio_main_next_ready(rt)` | `:313` | E | per ready connection | returns a new `VioConnHandle` the caller must release | NULL at end of batch. Clears `IN_READY` |
| 8 | `vio_main_next_done(rt)` | `:321` | E | per completed write job (`src/rust_io.c:479`) | returns the engine's job pointer back to it | NULL at end of batch |
| 9 | `vio_main_has_ready(rt)` | `:332` | E | per iteration (`src/rust_io.c:675`) | none | 1 if ready conns or done jobs remain in the current batch |
| 10 | `vio_conn_retain(rt, h)` | `:340` | E | per connection (`src/rust_io.c:515`) | new handle | handle |
| 11 | `vio_conn_release(rt, h)` | `:346` | E | per ready connection and per close | consumes `h` | none; may drop the last ref, which runs `Drop for Conn` (frees queued argv, closes fd) |
| 12 | `vio_conn_take_events(rt, h)` | `:352` | E | per ready connection (`src/rust_io.c:537`) | none | `VIO_EV_*` bits since the last call. Taking `NEW` counts the connection adopted and may post `ResumeAccept` to thread 0 (`exports.rs:173-175`) |
| 13 | `vio_set_maxclients(rt, n)` | `:360` | any (E per iteration, `src/rust_io.c:639`) | per iteration | none | none |
| 14 | `vio_take_rejected(rt)` | `:368` | any (E per iteration) | per iteration | none | refusals since last call |
| 15 | `vio_conn_udata(rt, h)` | `:374` | E | per ready connection | none | the stored engine pointer or NULL after close |
| 16 | `vio_conn_set_udata(rt, h, p)` | `:380` | E | per connection | `p` stored, never dereferenced | none |
| 17 | `vio_conn_fd(rt, h)` | `:386` | E | per connection | none | the socket fd (Rust owns it) |
| 18 | `vio_conn_pop_cmds(rt, h, out, max)` | `:395` | E | per delivery batch (`src/rust_io.c:181`) | `out` valid for `max` writes; each `VioCmd` moved, argv ownership to caller | count popped. Kicks the IO thread if reading was paused and fewer than `CMDS_LOW` remain (`exports.rs:243-245`) |
| 19 | `vio_conn_pending_cmds(rt, h)` | `:401` | E | per delivery | none | queued count (lock-free `ncmds`; a stale 0 is harmless, `lib.rs:195-199`) |
| 20 | `vio_conn_mem(rt, h, out)` | `:407` | E | each client memory update and `CLIENT LIST` | `out` written | none |
| 21 | `vio_conn_err_sample(rt, h, buf, cap, total)` | `:419` | E | rare (protocol error log) | `buf` writable for `cap`, `total` written | bytes copied |
| 22 | `vio_held_input_total(rt)` | `:432` | E | each `getMaxmemoryState()` and `INFO rust_io` | none | sum of private read-buffer capacity |
| 23 | `vio_conn_pending_out(rt, h)` | `:440` | E | each replica COB computation; shutdown loop | none | copied bytes not yet written |
| 24 | `vio_conn_writev(rt, h, iov, n)` | `:451` | E | per copy-path write | `iov` and its buffers borrowed for the call; bytes copied | bytes accepted (partial allowed); -1/`EAGAIN` at `OUT_HIGH` pending; -1/`EPIPE` after a peer close (`exports.rs:341-349`) |
| 25 | `vio_conn_submit_write(rt, h, job, off)` | `:464` | E | per zero-copy write | `job` lent until returned once through `write_job` and `vio_main_next_done` | none. Aborts if `off != 0` or a job is already in flight (`exports.rs:396`, `:405`) |
| 26 | `vio_conn_return_argv(rt, h, argv, argc)` | `:473` | E (uses `MainState`) | per executed command when `rust-io-threads >= 2` | ownership of `argv[0..argc]` moves to Rust; NULL entries allowed | none |
| 27 | `vio_conn_want_writable(rt, h, on)` | `:482` | E | when the engine installs or removes its write handler | none | none; `on` kicks |
| 28 | `vio_conn_publish(rt, h, consumed, auth)` | `:493` | E | per delivery when either value changed (`src/rust_io.c:223-227`) | none | none; kicks if `parse_held` |
| 29 | `vio_set_default_auth_required(rt, on)` | `:499` | any (E at start and per iteration) | per iteration | none | none |
| 30 | `vio_conn_set_session(rt, h, s)` | `:508` | E | precheck mode, when a session field changed | `s` copied into the seqlock | none |
| 31 | `vio_conn_close(rt, h)` | `:517` | E | per connection (`src/rust_io.c:707`) | none | none. Frees queued commands now, clears `udata` and `want_writable`, marks `C_CLOSED`, kicks |
| 32 | `vio_conn_set_priority(rt, h, on)` | `:526` | E | rare | none | none |
| 33 | `vio_conn_detach(rt, h)` | `:539` | E, never in a fork child | rare | none | 0, or -1 when pending output did not drain (IO side 5 s, engine side gives up after 6 s, `lib.rs:395`). Blocks |
| 34 | `vio_conn_attach(rt, h)` | `:548` | E | rare | none | none |
| 35 | `vio_conn_shutdown(rt, h)` | `:554` | E | rare (`freeClient` while a fork child exists, `src/networking.c:2305-2306`) | none | none. Deferred, section 5f |
| 36 | `vio_timing_next_write(id)` | `:561` | E | sampled replies | none | none (thread-local) |
| 37 | `vio_timing_sample_n()` | `:569` | any | INFO | none | N |
| 38 | `vio_timing_dropped()` | `:577` | any | INFO | none | dropped traces |
| 39 | `vio_timing_reset()` | `:585` | any | CONFIG RESETSTAT | none | none |
| 40 | `vio_timing_stamp(id, stage)` | `:593` | any | sampled commands | none | none |
| 41 | `vio_timing_drop(id)` | `:601` | any | rare | none | none |
| 42 | `vio_timing_stat(stage, out)` | `:609` | any | INFO | `out` written | none |

The engine-thread rule for exports that touch `Runtime.main` (3-9, 26, and every export that may `kick`: 18, 24, 25, 27, 28, 31, 35) is documented, not enforced; `post_kick` compares `pthread_self()` only to decide between batching and a direct post (`lib.rs:438`).

### 4.3 `VioCallbacks` (6 fields; engine functions Rust calls)

Set in `rustIoInitListeners` (`src/rust_io.c:780`, `:786-788`).

| Field | Header | C implementation | Thread | Frequency | Pointers, ownership | Contract |
|---|---|---|---|---|---|---|
| `create_argvs(ptrs, lens, cmds, n)` | `valkey_io.h:153` | `rustIoCreateArgvs` `src/rust_io.c:107` | IO | per parsed batch with at least one command (`lib.rs:1012-1013`) | `ptrs`/`lens` borrowed for the call (they point into the read buffer and `Worker.held`); C stores a new `robj **` in `cmds[i].argv` for every `err == 0` entry, consuming `argc` slices in order | must not fail (zmalloc aborts on OOM) |
| `free_argv(argv, argc)` | `:157` | `rustIoFreeArgv` `src/rust_io.c:120` | IO (`Msg::FreeArgv`, `lib.rs:1341`; `Drop for Conn`) or E (`vio_conn_close`; `Drop` when the engine drops the last ref) | per returned argv; per unconsumed command | takes ownership; NULL entries skipped | `decrRefCount` each, then `zfree` |
| `write_job(job, fd, can_write)` | `:167` | `rustIoWriteJob` `src/rust_io.c:453` | IO | per zero-copy job (`lib.rs:626`, `:1144`) | `job` is the engine's `client *` | `can_write` 1: write the sealed chunk with `_writeToClient()` straight to `fd`; 0: do not write, flag a write error. Returns 1 written, 0 would block, -1 error. Must publish `CLIENT_COMPLETED_IO` after a release fence (`src/rust_io.c:471-472`). NULL disables zero copy (Rust then panics on a job: `expect("write_job")`, `lib.rs:626`) |
| `precheck_begin(tid)` | `:174` | `rustPrecheckBegin` `src/rust_io_precheck.c:221` | IO | at most once per poll iteration, on first parse (`lib.rs:575-581`) | returns the pinned snapshot (may be NULL) | hazard-pointer pin of slot `tid` |
| `precheck_end(tid)` | `:175` | `rustPrecheckEnd` `src/rust_io_precheck.c:231` | IO | once per iteration that pinned (`lib.rs:1384`) | none | clears the hazard slot |
| `precheck_batch(snap, sess, barrier_active, cmds, n)` | `:176` | `rustPrecheckBatch` `src/rust_io_precheck.c:308` | IO | per parsed batch with a command (`precheck.rs:128-143`) | `snap` valid until `precheck_end`; `sess` borrowed for the call; `cmds` argv valid (just built) | fills `cmd`/`slot`/`read_flags`/`token` of each `err == 0` entry; returns the 1-based index of the last session barrier, 0 if none |

All three precheck fields are NULL outside precheck mode; Rust skips the precheck unless both `precheck_begin` and `precheck_batch` are set (`lib.rs:577-578`).

### 4.4 `engine_io.h` (4 functions; the engine side of an external IO layer)

| Function | Header | Impl | Thread | Frequency | Contract |
|---|---|---|---|---|---|
| `engineCmdQueuePending(c)` | `src/engine_io.h:30` | `src/engine_io.c:41` | E | per delivery | commands in `c->cmd_queue` not yet consumed |
| `engineSubmit(c, cmds, n, &err, &err_byte, &bytes)` | `src/engine_io.h:37` | `src/engine_io.c:45` | E | per pop batch | Compacts and grows `cmd_queue` (min capacity 16, doubling). Appends until the first `err` entry, storing its code and byte. Takes argv ownership. Prechecked entries (`token != 0` and not `READ_FLAGS_RUST_PREPARE_ON_MAIN`) keep the IO thread's `cmd`/`slot`/`read_flags` plus `READ_FLAGS_RUST_PRECHECKED`; others run `prepareCommandGeneric()` (`src/engine_io.c:80-88`). Adds input bytes to `*bytes`. For a replicated client re-serializes each command into `querybuf` (`src/engine_io.c:90`). Returns the number appended |
| `engineReplicatedBackfill(c)` | `src/engine_io.h:42` | `src/engine_io.c:33` | E | rare (slot import job creation, `src/cluster_migrateslots.c:888`) | appends the commands already in `cmd_queue` to `querybuf` and resets their `input_bytes` |
| `engineAccountRead(c, bytes)` | `src/engine_io.h:46` | `src/engine_io.c:96` | E | per submit that appended | `stat_total_reads_processed++`, net input bytes, `last_interaction` |

### 4.5 `externalConnOps` (10 entries; the engine calls the IO layer)

Declared `src/connection_external.h:19-45`, registered once by `connExternalAttach()` (`src/connection_external.c:186`, called at `src/rust_io.c:798`), implemented in `src/rust_io.c:692-759`. All on E (except `writev`, see below).

| Entry | Header | Implementation | Called from | Contract |
|---|---|---|---|---|
| `writev` | `:23` | `rustExtWritev` `src/rust_io.c:692` | `connExternalWritev` when not direct | inside a write job on an IO thread (`rust_direct_fd >= 0`) writes the socket directly; else `vio_conn_writev`. `EBADF` after close. -1/`EAGAIN` means keep the rest and wait for the write handler |
| `close` | `:26` | `rustExtClose` `:703` | `connExternalClose` | `vio_conn_close` + `vio_conn_release`, `rc->h = NULL` |
| `shutdown` | `:28` | `rustExtShutdown` `:712` | `connExternalShutdown` | `vio_conn_shutdown` (deferred) |
| `want_writable` | `:30` | `rustExtWantWritable` `:717` | `connExternalSetWriteHandler` | on = handler installed |
| `read_resumed` | `:33` | `rustExtReadResumed` `:723` | `connExternalSetReadHandler(func != NULL)` | re-stall the client if Rust holds commands (unprotect) |
| `detach` | `:37` | `rustExtDetach` `:729` | `connExternalDetach` | flush kicks, `vio_conn_detach`; returns 0/-1 |
| `attach` | `:39` | `rustExtAttach` `:743` | `connExternalNonBlock` | `vio_conn_attach` |
| `set_priority` | `:41` | `rustExtSetPriority` `:738` | `connExternalSetPriority` | `vio_conn_set_priority` |
| `has_pending_data` | `:43` | `connRustHasPendingData` `:674` | `connTypeHasPendingData()` in `beforeSleep()` | 1 if the current ready batch is non-empty or a stalled client can progress |
| `process_pending_data` | `:44` | `connRustProcessPendingData` `:636` | `connTypeProcessPendingData()` in `beforeSleep()` (`src/server.c:1997`) and in `processEventsWhileBlocked()` (`src/server.c:1972`) | per iteration: publish default auth, maxclients, rejected count, precheck snapshot; drain the rest of a ready batch; serve stalled clients |

### 4.6 ConnectionType `"external"` (18 non-NULL entries)

`CT_External`, `src/connection_external.c:149-184`. Type id `CONN_TYPE_EXTERNAL` (`src/connection.h:89-91`), name `"external"`. "Direct" means the connection is detached (`CONN_FLAG_EXT_DETACHED`, bit 15, `src/connection_external.c:13`) or the process is a fork child (`:17-19`).

| Entry | Line | Behavior |
|---|---|---|
| `get_type` | `:150` | `CONN_TYPE_EXTERNAL` |
| `addr` | `:156` | `anetFdToString` on the shared fd |
| `is_local` | `:157` | `127.*` or `::1` |
| `shutdown` | `:162` | `ext_ops->shutdown` |
| `close` | `:163` | `ext_ops->close`, then free or schedule |
| `accept` | `:166` | ACCEPTING -> CONNECTED, run accept handler |
| `write` | `:167` | one-iovec `writev` |
| `writev` | `:168` | direct: `writev(2)` with socket error handling; else `ext_ops->writev`; a non-EAGAIN error sets `CONN_STATE_ERROR` |
| `read` | `:169` | direct: `read(2)` (0 sets CLOSED); else always -1/`EAGAIN` (input arrives parsed) |
| `set_write_handler` | `:170` | store handler and barrier flag, `ext_ops->want_writable` |
| `set_read_handler` | `:171` | store handler; `read_resumed` when non-NULL |
| `get_last_error` | `:172` | `strerror(last_errno)` |
| `sync_write` | `:173` | direct: `syncWrite`; else ordinary write through Rust |
| `sync_read` | `:174` | direct: `syncRead`; else `read` (EAGAIN) |
| `sync_readline` | `:175` | direct: `syncReadLine`; else `read` (EAGAIN) |
| `has_pending_data` | `:176` | `ext_ops->has_pending_data` |
| `process_pending_data` | `:177` | `ext_ops->process_pending_data` |
| `is_closing` | `:183` | state != CONNECTED |

Dispatch outside the table: `connBlock()`/`connNonBlock()` call `connExternalBlock()`/`connExternalNonBlock()` (`src/socket.c:512-522`); `connSetPriority()` calls `connExternalSetPriority()` (`src/connection.c:179-184`); the forked Lua debugger calls `connExternalDetach()` before `serverFork()` (`src/scripting_engine.c:645-649`).

### 4.7 Engine hooks into the bridge (`src/rust_io.h`, `src/rust_io_precheck.h`)

| Hook | Declared | Called from |
|---|---|---|
| `rustIoTakesOverPort` | `src/rust_io.h:34` | `initListeners` `src/server.c:3348`, `updatePort`/`applyBind` `src/config.c:2722`, `:2907` |
| `rustIoListenerUnchanged` | `:35` | `src/config.c:2723`, `:2908` |
| `rustIoInitListeners` | `:37` | `src/server.c:3422` |
| `rustIoLogReady` | `:38` | `src/server.c:8330` |
| `rustIoBeforePoll` | `:39` | `src/server.c:1986` (sleeping 0), `:2154` (sleeping 1) |
| `rustIoAfterPoll` | `:40` | `src/server.c:2215` |
| `rustIoClientMem` | `:43` | `getClientMemoryUsage` `src/networking.c:6735` |
| `rustIoPendingOut` | `:44` | `getClientOutputBufferMemoryUsage` `src/networking.c:6707` |
| `rustIoClientQbuf` | `:45` | `catClientInfoString` `src/networking.c:5066` |
| `rustIoHeldMemory` | `:46` | `getMaxmemoryState` `src/evict.c:290` |
| `rustIoTrySendWrite` | `:48` | `trySendWriteToIOThreads` `src/io_threads.c:760` |
| `rustIoTryOffloadFreeArgv` | `:50` | `tryOffloadFreeArgvToIOThreads` `src/io_threads.c:1030` |
| `rustIoFlushKicks` | `:52` | `waitForClientIO` `src/io_threads.c:150` |
| `rustIoPublishLimits` | `:53` | `applyRustIoLimits` `src/config.c:77` |
| `rustIoCatInfo` | `:54` | `src/server.c:7299` |
| `rustIoFlushReplicas` | `:56` | `finishShutdown` `src/server.c:5420` |
| `rustPrecheckValidate` | `src/rust_io_precheck.h:51` | `processCommand` `src/server.c:5019` |
| `rustPrecheckSessionFlags` | `:53` | `rustIoPublish`, `rustPrecheckValidate` |
| `rustPrecheckCatInfo` | `:54` | `src/server.c:7298` |
| `rustPrecheckInit`, `rustPrecheckPublish` | `:45`, `:48` | `src/rust_io.c:784-785`, `:641` |

## 5. Protocols

### 5.1 (a) Wakeup and handoff (`src/rust/valkey-io/src/handoff.rs`)

One mechanism carries every cross-thread handoff: each IO thread's inbox (`Mailbox<Vec<Msg>>`, `lib.rs:259-263`) and the engine's ready list (`Mailbox<Ready>`, `lib.rs:312`). A `Mailbox` is a `Mutex<B>` plus a `Waker` with three `AtomicBool`s: `pending`, `sleeping`, `signaled`, and an eventfd (`handoff.rs:26-36`, `:99-102`). All atomics are `SeqCst`.

| Actor | Operation | Lines |
|---|---|---|
| producer | push under the lock (`post`), then `wake`: store `pending = true`; if `always_signal` or `sleeping` is set, and `signaled.swap(true)` returned false, write the eventfd | `handoff.rs:110-114`, `:52-57` |
| consumer, before blocking | store `sleeping = true`, then load `pending`; if set, do not block | `handoff.rs:68-71`; IO `lib.rs:1353-1355`; engine `exports.rs:96` via `src/rust_io.c:819` |
| consumer, after poll | store `sleeping = false` | `handoff.rs:74-76`; IO `lib.rs:1357`; engine `exports.rs:109` |
| consumer, on its eventfd | read the eventfd, then store `signaled = false`, then take | `handoff.rs:79-84`; IO `lib.rs:1364-1365`; engine `exports.rs:84-85` |
| consumer, take | `pending.swap(false)`; if it was set, swap the queue with the caller's emptied buffer under the lock (both keep capacity) | `handoff.rs:92-94`, `:131-137` |

Memory-ordering rule (normative):

1. **Dekker pair.** The producer's store `pending = true` must precede, in the single SeqCst order, its load of `sleeping`; the consumer's store `sleeping = true` must precede its load of `pending`. Then either the producer sees `sleeping` and signals, or the consumer sees `pending` and does not block.
2. **Drain before clear.** The consumer must consume the eventfd counter before it stores `signaled = false`, and must take the queue only after both. A producer that finds `signaled` set skips the eventfd write, so `signaled` may be cleared only once the write it stands for has been consumed. Clearing first loses a wakeup: consumer clears, a producer sees false, sets it and writes; the consumer's drain then eats that write, `signaled` stays set with the counter at zero, every later producer skips its write, and the consumer sleeps forever. The fix for that stall is commit `a317f3ae3` ("fix lost wakeup in the eventfd handoff").
3. A stale `pending == true` with an empty queue is allowed (a spurious non-blocking poll).

Mode-specific rules:

- Main mailbox: created with `sleeping = true` (`lib.rs:1435`). `vio_main_after_poll` does not take work while `signaled` is set: the eventfd handler takes it in the same iteration, and taking it twice would run two batches before the first one's replies (`exports.rs:107-114`).
- IO thread: after handling events, takes its inbox only if no eventfd write is outstanding (`lib.rs:1376-1378`); with an outstanding write the `TOK_WAKE` event takes it.
- By default IO-thread inboxes are posted with `always_signal = true` (bit 2 of `VIO_HANDOFF_DISABLE`, default 2), so an awake IO thread is still signaled; the main mailbox is not signaled while the engine thread is awake (`lib.rs:267`, `:607`, `:1430`).

Unit tests: `no_lost_wakeup`, `swap_keeps_capacity`, `signal_only_when_sleeping` (`handoff.rs:148-224`), also under Miri.

### 5.2 (b) Kick batching and coalescing

A kick asks the owning IO thread to re-evaluate a connection (flush output, resume reading, finish a close or shutdown, run a job).

- Coalescing: `Event::Kick` posts only if `KICKED` is clear; `KickTaken` (first thing in `on_kick`) clears it (`src/rust/valkey-io/src/conn_state.rs:59-66`, `lib.rs:1136`). Notifications coalesce the same way on `IN_READY` until `vio_main_next_ready` (`conn_state.rs:67-74`, `exports.rs:124`).
- Batching: on the engine thread, unless bit 1 of `VIO_HANDOFF_DISABLE`, a kick is appended to `MainState.kicks[thread]` (`lib.rs:433-456`). A list reaching `KICK_BATCH` (32) is handed over at once (`lib.rs:449-451`, `:461`); the rest go in `vio_main_flush_kicks` with the returned argv, one inbox lock and at most one eventfd write per IO thread (`exports.rs:59-72`, `lib.rs:491-502`).
- `vio_main_flush_kicks` is called at the end of every `beforeSleep()` and inside `processEventsWhileBlocked()` (`src/rust_io.c:816-820`, `src/server.c:1986`, `:2154`), before a detach (`src/rust_io.c:734`), at shutdown (`src/rust_io.c:838`) and from `waitForClientIO()` when a write job may still sit in a batch (`src/io_threads.c:148-151`).
- Off the engine thread, a kick is posted directly (`lib.rs:455`).

### 5.3 (c) Argv return

`rustIoTryOffloadFreeArgv()` (`src/rust_io.c:130-150`) handles `tryOffloadFreeArgvToIOThreads()` for external connections, for `argv` and `original_argv` (`src/networking.c:2192`, `:2210`):

1. Returns `C_ERR` (engine frees locally) when the connection is closed, `argc == 0`, or `rust-io-threads < 2` (`src/rust_io.c:135`).
2. Objects with `refcount > 1` are decremented on the engine thread and their slots set to NULL (the C IO threads' rule).
3. If any object is left, `vio_conn_return_argv` queues `(argv, argc)` in `MainState.argv[thread]` for the thread that built it (`exports.rs:415-417`, `lib.rs:473-487`); otherwise the array is `zfree`d.
4. The queue is handed over with the kicks (`Msg::FreeArgv`, one message per argv), or early at `ARGV_BATCH` (256) (`lib.rs:468`, `:482-485`).
5. The IO thread calls `free_argv`, which skips NULL entries (`lib.rs:1341`, `src/rust_io.c:120-124`).

MULTI-queued argv and rewritten argv are freed by the engine as in C (DESIGN.md "Executed argv freed on the IO thread").

### 5.4 (d) Zero-copy write jobs

Contract: the C IO threads' `io_write_state` protocol (`trySendWriteToIOThreads`, `ioThreadWriteToClient`, `processClientIOWriteDone`), reused unchanged. The engine never touches a sealed chunk while `io_write_state != CLIENT_IDLE`, and `freeClient()` defers while `clientHasPendingIO()`.

Eligibility (`rustIoTrySendWrite`, `src/rust_io.c:412-450`), all required:

| Condition | Line |
|---|---|
| `rust-io-zero-copy-min-bytes >= 0` and the connection is open | `:414` |
| no job in flight (else returns `C_OK`: nothing to do) and no read in flight | `:415-416` |
| pending replies exist | `:417` |
| client type normal or pubsub, not the Lua debugger (replicas, primary, slot-migration links copy: their output is the shared replication buffer) | `:420-421` |
| `c->buf` holds bulk-string references, or the reply list is non-empty, or `c->bufpos >= threshold` (default 2048, `src/config.c:3751`) | `:425` |

Sealing (engine): snapshot `io_last_reply_block` and `io_last_bufpos` (`:428-434`); clear `c->last_header` and the last block's `last_header`, so the engine, which keeps executing pipelined commands, starts new payload headers after the seal (`:435-440`); `write_flags = 0`; `io_write_state = CLIENT_PENDING_IO`; `rust_writes_outstanding++`, `rust_writes_submitted++`; `io_inflight_reply_bytes += reply_bytes` (`:441-447`); `vio_conn_submit_write(rt, h, c, 0)` (`:448`), which stores the job (at most one, asserted) and kicks (`exports.rs:395-407`).

Writing (IO thread, `run_job`, `lib.rs:616-640`, called after every `flush` and from `peer_gone`):

- `can_write = !PEER_CLOSED && every copied byte ahead is on the socket` (`lib.rs:625`).
- `write_job(job, fd, can_write)`: with 1, `rustIoWriteJob` sets `thread_id = -1` (`ioThreadsEnterExternal(1)`, `src/io_threads.c:135-137`) so `inMainThread()` is false, sets the thread-local `rust_direct_fd`, and runs `_writeToClient()`, which builds the iovec from the snapshot and writes the socket through `rustExtWritev` (`src/rust_io.c:453-474`, `:694`). With 0 it sets `WRITE_FLAGS_WRITE_ERROR` (the engine re-arms its write handler). It then issues a release fence and stores `CLIENT_COMPLETED_IO` (`src/rust_io.c:471-472`).
- If the job would block, or copied bytes are still queued, `sock_blocked` holds `VIO_EV_WRITABLE` until `EPOLLOUT` (`lib.rs:636-638`, `:1209-1213`).
- A job whose connection was already removed is completed unwritten in `on_kick` (`lib.rs:1141-1147`).
- Completed jobs go to `Ready.done` with the next `flush_notify`.

Release (engine): `rustIoDrainDone()` first in every `rustIoDrainReady()` (`src/rust_io.c:589`): for each `vio_main_next_done`, assert `outstanding >= 0` and `CLIENT_COMPLETED_IO`, subtract `io_inflight_reply_bytes`, `processClientIOWriteDone()` (`src/rust_io.c:477-488`).

Every job returns exactly once. `release_offset` is reserved for durability fences and must be 0 (`exports.rs:396`). Bytes in flight stay in `c->reply`/`c->buf` (counted in `omem`, COB limits, client eviction) until released; `io_inflight_reply_bytes` is excluded from maxmemory accounting (`src/evict.c:238-240`).

### 5.5 (e) Detach and attach

Purpose: hand the fd to the engine for blocking IO in the parent (synchronous Lua debugger; the dual-channel RDB channel, `src/rdb.c:4372`) or in a fork child (forked Lua debugger).

| Step | Thread | Lines |
|---|---|---|
| `connBlock()` -> `connExternalBlock()`: set `CONN_FLAG_EXT_DETACHED`; in a fork child stop there; else `ext_ops->detach`; on -1 set `last_errno = EIO` and `CONN_STATE_ERROR`; then `anetBlock` | E | `src/connection_external.c:206-226` |
| `rustExtDetach`: `vio_main_flush_kicks`, `vio_conn_detach` | E | `src/rust_io.c:729-736` |
| `detach()`: `DET_DONE` returns at once; else store `DET_WANTED`, post `Msg::Detach`, poll every 50 us up to 6 s for `DET_DONE`/`DET_FAILED` | E | `lib.rs:386-407` |
| `on_detach`: flush repeatedly, `poll(POLLOUT)` between attempts, until nothing is pending, the peer closed, or 5 s (`DETACH_FLUSH_MS`); store `DET_DONE` or `DET_FAILED`; on success watch only `EPOLLRDHUP` | IO (blocks that thread) | `lib.rs:1231-1261`, `:728-730` |
| while `DET_DONE`: the IO thread neither reads nor writes; `EPOLLRDHUP/HUP/ERR` still reports `VIO_EV_CLOSED`; a close or shutdown takes effect at once | IO | `lib.rs:1074-1076`, `:1154-1159`, `:1200-1208` |
| direct IO: `read`/`write`/`writev`/`sync_*` use the fd | E or fork child | `src/connection_external.c:65-135` |
| `connNonBlock()` -> `connExternalNonBlock()`: `anetNonBlock`; if detached and not a fork child, clear the flag and `ext_ops->attach` | E | `src/connection_external.c:228-235` |
| `attach()`: if not `DET_NONE`, post `Msg::Attach`; `on_attach`: `DET_NONE`, re-arm interest, flush, parse anything buffered | E, IO | `lib.rs:410-416`, `:1265-1277` |

States (`lib.rs:118-121`): `DET_NONE` 0, `DET_WANTED` 1, `DET_DONE` 2, `DET_FAILED` 3. No bytes are lost: output accepted before the detach is on the socket first; input Rust already read stays in Rust; unread input stays in the socket for the direct reader.

### 5.6 (f) Close, shutdown and exit

Lifecycle word (`conn_state.rs:13-21`): `KICKED`, `IN_READY`, `C_CLOSED` (engine closed), `PEER_CLOSED`, `HALF_CLOSE` (shutdown requested, not done). Transitions are a pure function `step()` (`conn_state.rs:56-111`) installed by CAS in `apply()` (`:129-141`), which returns effects `FX_POST_KICK`, `FX_PUSH_READY`, `FX_NOTIFY_CLOSED`, `FX_REMOVE`, `FX_SHUTDOWN`.

| Event | Rule | Lines |
|---|---|---|
| peer EOF or socket error | stop polling, keep the fd; `PEER_CLOSED`, clear `HALF_CLOSE`; complete any job unwritten; if the engine already closed: remove, else notify `VIO_EV_CLOSED` | `lib.rs:746-763`, `conn_state.rs:78-85` |
| engine sees `VIO_EV_CLOSED` | state CLOSED, `freeClientAsync` | `src/rust_io.c:566-575` |
| `vio_conn_close` | free queued commands now, clear `udata`/`want_writable`, set `C_CLOSED` and kick | `exports.rs:476-496`, `conn_state.rs:88` |
| `vio_conn_shutdown` | no `shutdown(2)` now: set `HALF_CLOSE` and kick (ignored after a peer close) | `exports.rs:539-543`, `conn_state.rs:91-97` |
| after every flush (`after_flush`) | `Flushed{out_drained, job_idle}`: with `HALF_CLOSE`, `out_drained` and `job_idle`, `shutdown(fd, SHUT_RDWR)` once; with `C_CLOSED` and (`out_drained` or `PEER_CLOSED`), remove | `lib.rs:1178-1198`, `conn_state.rs:98-110` |
| `on_kick` with `C_CLOSED` and `PEER_CLOSED` | remove at once | `lib.rs:1150-1153` |

The deferral exists because `freeClient()` shuts the socket down while a fork child exists (`src/networking.c:2304-2306`); an immediate `shutdown(2)` discarded a reply still in Rust's buffer (`CLIENT KILL` during `BGSAVE`, `killself-bgsave.py`).

Process exit: there is no `vio_stop`; IO threads die with the process. `finishShutdown()` calls `rustIoFlushReplicas(1000)` after `flushReplicasOutputBuffers()` (`src/server.c:5417-5421`). It flushes the kick batches, then polls every 1 ms, up to 1000 ms, until the copied output of every external replica is written (`src/rust_io.c:836-854`). Normal clients' Rust-held output is not waited for.

## 6. Precheck semantics (`rust-io-mode precheck`)

### 6.1 Classification and where each check runs

Classes (design-docs/rust-io-layer.md:42-45): A static tables (lookup, arity, slot); B per-client session (auth, MULTI, RESP2 pub/sub, READONLY, db, user); C global policy (ACL, cluster ownership, role, loading, pause, disk errors, min-replicas); D engine-mutating or keyspace-reading (filters, evictions, tracking, MULTI queueing, throttle, rejection stats). Verified against the code:

| Check | Where | Lines |
|---|---|---|
| lookup, arity, cluster slot (A) | IO thread, `lookupCommand`, `commandCheckArity`, `clusterSlotByCommand`; module commands are left to the engine (`READ_FLAGS_RUST_PREPARE_ON_MAIN`) because their getkeys is module code | `src/rust_io_precheck.c:276-291` |
| auth (B) | IO thread, against the snapshot's `default_auth_required` and `VIO_SESS_AUTHENTICATED` | `src/rust_io_precheck.c:253-254` |
| MULTI, RESP2 pub/sub, special clients (B) | not checked: they force a fallback | `:247-249` |
| db, user (B) | inputs to the ACL check | `:258-261` |
| ACL (C) | IO thread, unchanged `ACLCheckAllUserCommandPerm()` on a private copy of the user | `:261` |
| cluster ownership (C) | engine thread at validation, live | `:353-361` |
| NO_MULTI | skipped when prechecked (MULTI always falls back) | `src/server.c:4695-4701` |
| protected (DEBUG/MODULE) | not prechecked: `CMD_PROTECTED` is ineligible | `src/rust_io_precheck.c:255` |
| role, loading, pause, disk errors, min-replicas, read-only and stale replica, busy script/module, replica keyspace, redirects (C) | engine, live, unchanged (`processCommandExecute`) | `src/server.c:4752-4996` |
| D (filters when not prechecked, evictions, tracking, MULTI queue, throttle, `call()`) | engine | `src/server.c:5022-5026`, `:4752-4996` |

Two blocks of engine checks are skipped on a validated fast path: the top block of `processCommand()` (security warning, existence, auth, arity, protected, `src/server.c:5040-5096`) and `processCommandChecks()` (NO_MULTI, ACL, `getNodeByQuery()`, `src/server.c:4694-4746`, skipped at `:5118`). Module command filters are skipped too (`:5024`); validation guarantees none is registered.

The header comment "class A-C checks" (`src/rust_io_precheck.c:4`, `src/rust_io_precheck.h:24`) means A, B and the ACL part of C.

### 6.2 Policy snapshot and epoch

`pcSnapshot` (`src/rust_io_precheck.c:41-49`): `epoch`, `default_auth_required`, `modules` (a command filter is registered), up to `PC_MAX_USERS` (64) `live[i]` identity pointers and `copy[i]` deep copies (`ACLCreateUnlinkedUser` + `ACLCopyUser`), built after walking `Users` (`:115-144`). Users with roles, or role objects, are not copied (`:127-129`). Build and free save and restore `acl_mutation_epoch`, because the copies bump it (`:116`, `:140`, `:147-149`).

Publication: `rustPrecheckPublish()` on the engine thread at start (`src/rust_io.c:785`) and every iteration (`src/rust_io.c:641`) rebuilds when there is no snapshot, the epoch moved, or filter presence changed; the old one goes to a retired list (`src/rust_io_precheck.c:174-187`). Reclaim frees a retired snapshot when no hazard slot `0..rust-io-threads` holds it (`:154-172`). IO threads pin with store-then-recheck (`:221-229`), one pin per poll iteration (`lib.rs:575-589`). `ACLFreeUser` therefore runs only on the engine thread.

Epoch mutation sites (`acl_mutation_epoch++`):

| Function | Line |
|---|---|
| `ACLFreeUser()` | `src/acl.c:559` |
| `ACLCopyUser()` | `src/acl.c:609` |
| `ACLRecomputeCommandBitsFromCommandRulesAllUsers()` | `src/acl.c:1014` |
| `ACLSetUser()` | `src/acl.c:1770` |
| `ACLRemapSurvivingRoleMembers()` | `src/acl.c:3035` |

No other state is epoch-tracked; everything the fast path does not skip is read live.

### 6.3 Session view and barriers

- The engine publishes `VioSession {user, gen, flags, dbid}` after adoption and after every delivery, only when a field changed (`gen++`) (`src/rust_io.c:204-221`). `flags` (`rustPrecheckSessionFlags`, `src/rust_io_precheck.c:321-329`): `AUTHENTICATED`, `MULTI`, `PUBSUB_RESP2` (pubsub and RESP2), `SPECIAL` (replica, primary, slot migration job, no user, or `mustObeyClient`).
- Rust stores it in a single-writer seqlock (`precheck.rs:60-112`): writer makes `seq` odd, Release fence, Relaxed field stores, `seq` even with Release; reader loads `consumed` (Acquire), retries while `seq` is odd or moved, Acquire fence before the recheck.
- `consumed` (commands the engine took from `cmd_queue`: `pulled - pending`, `src/rust_io.c:209`) is published with `vio_conn_publish` after the session (`src/rust_io.c:219`, `:226`); its Release store pairs with the reader's Acquire load (`precheck.rs:87-89`, `:93`).
- Barrier commands, resolved by name at init and compared by command pointer (the parent for subcommands): `auth hello select multi exec discard reset subscribe unsubscribe psubscribe punsubscribe ssubscribe sunsubscribe readonly readwrite client acl sync psync replconf quit` (`src/rust_io_precheck.c:65-90`, `:202-212`, `:299-302`).
- Per connection the IO thread numbers the commands it hands over (`Local.seq`). After a batch whose last barrier is at 1-based index `k`, `barrier_seq = seq + k`; while `barrier_seq > consumed`, every command is checked with `barrier_active` and gets `PC_FB_BARRIER` (`precheck.rs:128-143`). Within a batch, commands after a barrier are checked with the barrier active (`src/rust_io_precheck.c:308-315`).
- Barriers are an optimization; correctness rests on the engine-thread comparison in 6.4.
- While a connection must authenticate, the parser stops after one command and resumes on the next `vio_conn_publish` (`lib.rs:983-986`, `exports.rs:447-449`), so commands behind `AUTH` are parsed with post-AUTH limits.

### 6.4 Token, validation and fallback

Token (`src/rust_io_precheck.h:20-21`, `src/rust_io_precheck.c:296-297`): `(epoch & 0xffffffff) << 32 | (gen & 0xffffff) << 8 | code`.

IO-thread decision (`pcDecide`, `src/rust_io_precheck.c:242-263`), first match wins:

| Condition | Code |
|---|---|
| no snapshot | `PC_FB_NO_SNAPSHOT` |
| session never published (`gen == 0`) | `PC_FB_NO_SESSION` |
| barrier active | `PC_FB_BARRIER` |
| command filter registered at build | `PC_FB_MODULES` |
| `VIO_SESS_SPECIAL` | `PC_FB_SPECIAL_CLIENT` |
| `VIO_SESS_MULTI` | `PC_FB_MULTI` |
| `VIO_SESS_PUBSUB_RESP2` | `PC_FB_PUBSUB` |
| unknown command or bad arity | `PC_REJECT` |
| auth required and not `CMD_NO_AUTH` | `PC_REJECT` |
| `CMD_MODULE`, `CMD_PROTECTED`, `CMD_PUBSUB`, EXEC, EVAL, EVALSHA (and `_RO`), FCALL, FCALL_RO | `PC_FB_INELIGIBLE` |
| `READ_FLAGS_CROSSSLOT` | `PC_REJECT` |
| user not in snapshot | `PC_FB_ACL_USER` |
| ACL denies | `PC_REJECT` |
| otherwise | `PC_FAST` |

Module commands get `PC_FB_INELIGIBLE` with no epoch and are prepared on the engine (`src/rust_io_precheck.c:280-285`, `src/engine_io.c:86-87`).

Engine validation (`rustPrecheckValidate`, `src/rust_io_precheck.c:331-368`), only for commands carrying `READ_FLAGS_RUST_PRECHECKED` and not being reprocessed (`src/server.c:5019`):

| Step | Fails as |
|---|---|
| code >= `PC_FB_FIRST` (or out of range) | that code |
| token epoch != low 32 bits of `acl_mutation_epoch` | `PC_FB_EPOCH` |
| token gen != published gen, or published `user`/`dbid`/`flags` differ from the live client | `PC_FB_SESSION` |
| a command filter is registered, or a module set `CLUSTER_MODULE_FLAG_NO_REDIRECTION` | `PC_FB_MODULES_LIVE` |
| code is `PC_REJECT` | counted `rejected`, full path |
| cluster mode, keyed command: slot not owned by myself, migrating, importing, or cluster unhealthy | `PC_FB_CLUSTER` |
| all pass | `PC_FAST`: return 1 |

Every outcome except non-prechecked commands is counted once (`pc_stats.code`). A fallback runs the unchanged full path; a wrong fast path is a bug, a fallback is always allowed.

Rejection protocol: Rust never emits errors. A predicted rejection validates like a fast path and then runs the full path, which emits the error in order and produces `rejected_calls`, error stats, `ACL LOG` and module ACL-rejected events with the engine's own code.

Reprocessed commands (blocked and resumed, `c->cmd` set) never consult the token (`src/server.c:5016-5019`).

### 6.5 Module restrictions

- Module commands: never prepared or checked off-thread (`src/rust_io_precheck.c:280-285`).
- Command filters: any registered filter disables the fast path, at build (`PC_FB_MODULES`) and live (`PC_FB_MODULES_LIVE`) (`:120`, `:178`, `:348`).
- `CLUSTER_MODULE_FLAG_NO_REDIRECTION`: live fallback (`:348`).
- Module users and users with roles: not in the snapshot (`PC_FB_ACL_USER`).
- `MODULE LOAD`/`UNLOAD` mutate the command table while IO threads may run `lookupCommand`; this race is shared with the C IO threads and is not guarded.

## 7. Admission control and backpressure

| Constant | Value | Where | When hit |
|---|---|---|---|
| `CMDS_HIGH` | 1024 | `lib.rs:111` | a connection holding this many parsed commands stops reading (`read_paused`, `EPOLLIN` removed); TCP backpressure follows (`admission.rs:63-65`, `lib.rs:1054-1056`, `:731`) |
| `CMDS_LOW` | 256 | `lib.rs:112` | a pop leaving fewer kicks a paused connection; `on_kick` resumes it if `may_resume` (`exports.rs:243-245`, `lib.rs:1160-1162`) |
| `QUEUED_HIGH` | 4096 | `admission.rs:23` | above this many commands queued across all connections, a connection holding `FAIR_SHARE` or more stops reading |
| `FAIR_SHARE` | 32 | `admission.rs:24` | per-connection share under global pressure; a connection with fewer can always read |
| `NEW_HIGH` | 128 | `admission.rs:21` | unadopted connections (NEW not taken): thread 0 removes the listener from epoll; the kernel backlog holds the rest (`lib.rs:1286-1298`) |
| `NEW_LOW` | 64 | `admission.rs:22` | adopting below this posts `ResumeAccept` to thread 0 (`admission.rs:133-135`, `exports.rs:173-175`); pause/resume race closed by `undo_pause` (`admission.rs:127-129`) |
| `MAXCLIENTS_SLACK` | 16 | `admission.rs:27` | at `maxclients + 16` live Rust connections the IO thread writes `-ERR max number of clients reached` and closes; counted in `rejected_connections`. Below that the engine decides at adoption (`acceptCommonHandler`) (`admission.rs:99-107`, `lib.rs:1310-1316`) |
| popped-count flush | per iteration | `admission.rs:79-90`, `exports.rs:61` | the global queued count overstates by at most one iteration's pops |
| `OUT_HIGH` | 1 MiB | `lib.rs:113` | copied output pending per connection: `vio_conn_writev` accepts only up to it, then -1/`EAGAIN`; the engine keeps the rest and installs its write handler (`exports.rs:346-383`). A write buffer larger than 4 MiB is freed once drained (`lib.rs:1114-1117`) |
| `OUT_LOW` | 256 KiB | `lib.rs:114` | `VIO_EV_WRITABLE` fires (level-triggered, after each flush) while the engine wants it and pending output is below this (`lib.rs:1126-1131`) |
| `READ_CHUNK` | 16 KiB | `lib.rs:115` | read size; a shared read buffer is returned to the thread only if its capacity is at most 64 KiB (`lib.rs:791`) |
| big bulk | >= 32 KiB | `resp.rs:235` | read exactly to the end of the bulk (`lib.rs:880`) |
| `client-query-buffer-limit` | config | `lib.rs:901-909` | unparsed bytes above it (or above 1 MiB while unauthenticated): parsing stops, buffer dropped, `VIO_EV_QBLIMIT`; the engine delivers what was parsed, logs `Closing client that reached max query buffer length`, `freeClientAsync`, counts the disconnection (`src/rust_io.c:546-564`) |
| `PROTO_INLINE_MAX_SIZE` | 64 KiB | `resp.rs:12` | inline request, multibulk count line or bulk count line longer than this without CR: protocol error (`resp.rs:258`, `:294`, `:345`) |
| unauthenticated multibulk / bulk | > 10 args / > 16384 bytes | `resp.rs:275`, `:314` | protocol error |
| `SWEEP_EVERY` | 100 ms | `lib.rs:124` | private read buffers: free when idle > 2 s and empty, shrink to fit when idle with a partial command, shrink to the recent peak when > 32 KiB and more than twice the peak; only with > 4 KiB free (`lib.rs:829-864`). The IO thread uses this as its epoll timeout while any private buffer exists (`lib.rs:1383`) |
| `DETACH_FLUSH_MS` | 5000 ms | `lib.rs:127` | detach fails (`DET_FAILED`, the engine marks the connection `EIO`/ERROR); the engine side waits 1 s longer (`lib.rs:395`) |
| `KICK_BATCH` | 32 | `lib.rs:461` | early hand-over of one IO thread's kicks |
| `ARGV_BATCH` | 256 | `lib.rs:468` | early hand-over of returned argv |
| epoll batch | 256 events | `lib.rs:1351` | per `epoll_wait` |
| `PROTO_DUMP_LEN` | 128 | `lib.rs:147` | protocol-error sample size |
| `RUST_IO_CMD_QUEUE_MAX` | 1024 | `src/rust_io.c:62` | `cmd_queue` cap; further commands wait in Rust and the client is put on `rust_stalled`, revisited every iteration (`src/rust_io.c:156-160`, `:636-672`) |
| `RUST_IO_POP_BATCH` | 128 | `src/rust_io.c:63` | commands per `vio_conn_pop_cmds` |
| `RUST_IO_REPL_POP_BATCH` | 16 | `src/rust_io.c:165` | per pop for a replicated client; pulling stops once `PROTO_IOBUF_LEN` of unapplied stream is in `querybuf` (`:177-180`) |
| `RUST_IO_REPL_ROUNDS` | 64 | `src/rust_io.c:330` | pull/execute rounds per delivery for a replicated client |
| `RUST_IO_BATCH_MAX_QUEUED` | 2 | `src/rust_io.c:357` | only clients with 1-2 queued commands go into the cross-connection prefetch batch |
| `RUST_IO_DRAIN_BATCH` | 128 | `src/rust_io.c:377` | prefetch batch flushed every 128 batched clients |
| replica flush at exit | 1000 ms, 1 ms poll | `src/server.c:5420`, `src/rust_io.c:852` | gives up |
| argv offload | `rust-io-threads >= 2` | `src/rust_io.c:135` | with one thread argv is freed on the engine thread |
| `PC_MAX_USERS` | 64 | `src/rust_io_precheck.c:38` | further users fall back (`acl_user`) |
| `PC_MAX_THREADS` | 128 | `src/rust_io_precheck.c:39` | hazard slots (equals the `rust-io-threads` maximum) |
| `PC_MAX_BARRIERS` | 32 | `src/rust_io_precheck.c:89` | barrier table size |
| `COMMAND_QUEUE_MIN_CAPACITY` | 16 | `src/engine_io.c:8` | initial `cmd_queue` capacity |

A single read can parse more than `FAIR_SHARE` commands (one `READ_CHUNK`), the same granularity as the C read path.

## 8. Engine features over external connections

### 8.1 Replicated clients (atomic slot migration import)

`commandProcessed()` derives a replicated client's reploff from `querybuf` and proxies the applied part to replicas. An import link is the only replicated client that can arrive through Rust. `engineSubmit` therefore re-serializes each command of a replicated client into `querybuf` in canonical multibulk (`engineAppendReplicated`, `src/engine_io.c:16-31`), keeps `qb_pos` at the end, advances `read_reploff`, and makes the length the command's `input_bytes` (`src/engine_io.c:90`), so `consumeCommandQueue()` advances `qb_applied` as for a C client. `createSlotImportJob()` calls `engineReplicatedBackfill()` for commands queued before the client became replicated (`src/cluster_migrateslots.c:885-889`). The unapplied stream exists twice (argv and RESP); `rustIoPull` and `rustIoDeliver` bound it with the replicated pull limits in section 7.

### 8.2 Client memory, `qbuf`, maxmemory

`vio_conn_mem` (`exports.rs:275-289`) reports `in_unparsed` (bytes past the parser position), `in_alloc` (private read-buffer capacity; 0 while using the thread's shared buffer), `queued_input` and `queued_argv` (parsed commands not yet popped), `out_pending` (copied output not yet written).

| Consumer | Adds | Lines |
|---|---|---|
| `getClientMemoryUsage()` (`tot-mem`, client eviction) | `in_alloc + queued_argv + out_pending`, with `out_pending` left out for replicas | `src/networking.c:6734-6736`, `src/rust_io.c:863-871` |
| `CLIENT LIST` `qbuf` | `in_unparsed + queued_input + input bytes still in cmd_queue` | `src/networking.c:5061-5070`, `src/rust_io.c:882-892` |
| `CLIENT LIST` `qbuf-free` | `in_alloc - in_unparsed` | `src/rust_io.c:891` |
| `getMaxmemoryState()` | `vio_held_input_total()` (sum of private read buffers; system allocator, not in `zmalloc_used_memory()`) | `src/evict.c:288-291`, `src/rust_io.c:896-898` |

`VIO_EV_MEM` (a private read buffer grew) makes the engine deliver, so `beforeNextClient()` re-buckets the client as after a C read (`lib.rs:818-821`). `INFO used_memory` does not include Rust buffers; `used_memory_peak` can, because `getMaxmemoryState()` updates the peak from its total (`src/evict.c:294`).

### 8.3 Replica output in the COB

For a replica on an external connection, `getClientOutputBufferMemoryUsage()` adds `rustIoPendingOut()` (up to 1 MiB copied and not yet written) to the repl buffer size behind the cursor (`src/networking.c:6703-6708`), so the replica limits and the replication throttle see it, and `rustIoClientMem()` leaves it out of `tot-mem` to avoid double counting (`src/rust_io.c:868-870`). Normal and pubsub clients' Rust-held output is counted only in `tot-mem` (section 9).

### 8.4 Eviction accounting (`src/evict.c`)

`freeMemoryGetNotCountedMemory()` excludes `server.io_inflight_reply_bytes` (reply bytes sealed into jobs in flight, written only by `src/rust_io.c:447`, `:484`) (`src/evict.c:238-240`) and the `last_memory_usage` of normal and pubsub clients waiting in `clients_to_close` (`src/evict.c:242-253`). The second rule applies to the C IO threads too.

### 8.5 Priority connections, protocol errors

- Priority: `connSetPriority()` on an external connection sets `is_priority` and `vio_conn_set_priority` (`src/connection_external.c:218-221`, `lib.rs:166-174`). While any connection has it, `take_ready` moves priority connections to the front of the batch in arrival order (`lib.rs:1486-1502`); `rustIoDrainReady` runs them immediately (not batched) and reports their span to the event loop's QoS stats callback as one priority cycle (`src/rust_io.c:579-617`, `:546-549`).
- Protocol errors: the IO thread queues an error entry after the last good command and stores a sample of the input from the parser's position (whole if under 128 bytes, else first and last 64) and its length (`lib.rs:990-1008`, `:151-158`). The engine replies with the C error text once every earlier command ran and the client can parse (`src/rust_io.c:299-303`, `:257-285`), sets `close_after_reply` and `protocol_error`, and logs `Protocol error (...) from client: ... Query buffer: ...` at verbose, redacted under `hide-user-data-from-log` (`src/rust_io.c:233-255`).

### 8.6 C IO threads

- `trySendReadToIOThreads()` refuses external connections (`src/io_threads.c:708-711`); `trySendWriteToIOThreads()` and `tryOffloadFreeArgvToIOThreads()` route them to Rust (`src/io_threads.c:758-761`, `:1029-1031`). C IO threads keep serving C connections (Unix socket, TLS, the C port when not taken over, outbound links).
- `waitForClientIO()` flushes kick batches before waiting on an external client's write job (`src/io_threads.c:148-151`).
- `ioThreadsEnterExternal()` makes `inMainThread()` false on a Rust thread during a write job (`src/io_threads.c:135-137`).
- The cross-connection key-prefetch batch (`prefetchCommandsBatchInit`, `src/rust_io.c:801`) is the C IO threads' batch; under `processEventsWhileBlocked()` the outer batch is run first (`src/rust_io.c:592`).

### 8.7 Blocking, protection and long commands

- `protectClient()` removes the read handler: Rust holds the input and the client is stalled (`src/rust_io.c:315-319`); `unprotectClient()` resumes it through `read_resumed`.
- Blocked clients keep their queue; `processUnblockedClients()` resumes them (`src/rust_io.c:324`).
- A long command (script, module) started from a ready batch leaves the rest of the batch behind an already drained eventfd; `connRustProcessPendingData()` drains it from `processEventsWhileBlocked()` (`src/rust_io.c:642-646`), which is what makes `SCRIPT KILL` from a connection in the same batch work.

## 9. Compatibility

Observable differences from the C transport with `rust-io-takeover yes`:

| Area | C transport | Rust IO layer | Lines |
|---|---|---|---|
| `CONFIG SET bind` / `port` | re-binds | refused with `Changing bind or port is not supported with rust-io-takeover` unless the value equals the startup value | `src/config.c:2720-2727`, `:2905-2912`, `src/rust_io.c:93-99` |
| Bind addresses | every `bind` entry, IPv4 and IPv6 | only the first entry; a leading `-` is stripped, `*` -> `0.0.0.0`, `::*` -> `::`; the default `* -::*` yields IPv4 only | `src/rust_io.c:775`, `lib.rs:1398-1405` |
| `tcp-backlog` | applied to `listen(2)` | not applied (Rust `std::net::TcpListener::bind` default backlog) | `lib.rs:1405` |
| Unix socket, TLS, RDMA | supported | not served by Rust (C listeners still work for them) | |
| C IO threads | read and write client sockets | idle for external connections; `io_threaded_reads_processed`/`io_threaded_writes_processed` and per-thread active time stay 0 for them | `src/io_threads.c:708-711`, `:758-761` |
| `omem` / COB limits of normal clients | all unsent output | output copied into Rust (up to 1 MiB) is not in `omem` or the COB limit check; it is in `tot-mem` and client eviction | `src/rust_io.c:863-871`, `src/networking.c:6692-6724` |
| Replica COB | all unsent output | includes Rust-held output | `src/networking.c:6703-6708` |
| `used_memory` | includes query buffers | excludes Rust read and write buffers (system allocator); maxmemory adds private read buffers only; shared per-thread read buffers and write buffers are not counted anywhere | `src/evict.c:288-291` |
| `CLIENT LIST qbuf` | `sdslen(querybuf)` | unparsed + parsed-not-executed input; `qbuf-free` is the private buffer's free space | `src/rust_io.c:882-892` |
| maxclients | engine refuses at `maxclients` | same, plus an IO-thread refusal at `maxclients + 16` connections before adoption | `admission.rs:99-107` |
| Accept throttling | accept on the main thread | at most 128 unadopted connections, then the listener is paused | `admission.rs:21-22` |
| Query buffer limit log | dumps `querybuf` | `Closing client that reached max query buffer length: <client info>` without the buffer | `src/rust_io.c:556-559` |
| Net counters | `total_reads_processed` per read | per submit batch; `net_input_bytes` counts parsed command bytes | `src/engine_io.c:96-101` |
| `INFO rust_io`, `INFO rust_io_timing` | absent | explicit sections; not reset by `CONFIG RESETSTAT` (timing histograms excepted) | `src/server.c:7294-7306` |
| Shutdown | replica output written synchronously | waits up to 1 s for replicas' Rust-held output; normal clients' Rust-held output may be lost at exit | `src/rust_io.c:836-854` |
| Log lines | `Accepted ...`, `Client closed connection` | suffixed `(rust-io)`; extra `Rust IO layer listening ...`, `Ready to accept connections rust` | `src/rust_io.c:523`, `:572`, `:807`, `:901` |

Test tag `needs:c-transport` (`tests/README.md:108`) marks the 8 tests that assert C-transport behavior; run the Tcl suite under the layer with `--tags -needs:c-transport`:

| Test | File:line (tag) |
|---|---|
| Primary link stays on the main thread with I/O threads enabled | `tests/integration/replication-flow-control.tcl:8` |
| Test change port and tls-port on runtime | `tests/unit/cluster/announced-endpoints.tcl:57` |
| CONFIG SET rollback on apply error | `tests/unit/introspection.tcl:1524` |
| Force the use of IO threads and assert active IO thread usage | `tests/unit/io-threads.tcl:64` |
| CONFIG SET port number | `tests/unit/networking.tcl:25` |
| CONFIG SET bind address | `tests/unit/networking.tcl:38` |
| Default bind address configuration handling | `tests/unit/networking.tcl:138` |
| Protected mode works as expected | `tests/unit/networking.tcl:140` |

Each failure was confirmed in io mode before tagging: the six bind/port tests fail with `Changing bind or port is not supported with rust-io-takeover`, and the two IO-thread tests with `Expected '0' to be more than '0'` (no C IO thread reads) and `Expected '0.000000' to be more than '0'` (no active time).

The immutable `rust-io-*` configs are excluded from `CONFIG SET` sanity (`tests/unit/introspection.tcl:1382-1385`).

## 10. Safety

### 10.1 Where `unsafe` lives

The crate root denies `unsafe_code` and `unsafe_op_in_unsafe_fn` (`src/rust/valkey-io/src/lib.rs:9`). Exceptions: `ffi/` (`#![allow(unsafe_code)]`, `src/rust/valkey-io/src/ffi/mod.rs:4`, covering `ffi/exports.rs`), `sys.rs` (`#![allow(unsafe_code)]`, `src/rust/valkey-io/src/sys.rs:3`), and the precheck unit tests (`#[cfg(test)] #[allow(unsafe_code)]`, `precheck.rs:146`). Counts below are `unsafe {` blocks, `unsafe extern "C" fn` items and `unsafe impl` items:

| File | Blocks | `unsafe extern "C" fn` | `unsafe impl` | Contents |
|---|---|---|---|---|
| `src/rust/valkey-io/src/sys.rs` | 19 | 0 | 0 | syscall wrappers: errno, close, poll, shutdown, eventfd create/signal/drain, epoll create/ctl/wait, `read` into reserved Vec capacity, write, writev, accept4 + `TCP_NODELAY`, clock/rdtsc (timing), `pthread_self` |
| `src/rust/valkey-io/src/ffi/mod.rs` | 4 | 0 | 4 | `ConnHandle` drop (`Arc::decrement_strong_count`), `ConnRef::retain` (`increment_strong_count`), `ConnRef::arc` (`Arc::from_raw` in `ManuallyDrop`), `Deref`; `Send for VioCmd`, `Send for VioSession`, `Sync`/`Send for Global` |
| `src/rust/valkey-io/src/ffi/exports.rs` | 10 | 42 (35 + 7 timing) | 0 | deref of `cfg`, `CStr` of `bindaddr`, `efd` write, `out.add(i).write` in pop, `VioConnMem` write, error-sample copy, iovec deref and slice, session deref, timing stat write |
| `src/rust/valkey-io/src/precheck.rs` (tests only) | 1 | 0 | 0 | `slice::from_raw_parts_mut` in a test callback |
| all other modules | 0 | 0 | 0 | |

Generated with `for f in src/rust/valkey-io/src/*.rs src/rust/valkey-io/src/ffi/*.rs; do echo "$f $(grep -c 'unsafe {' $f)"; done` (and the same for `unsafe extern "C" fn` and `unsafe impl`).

### 10.2 Invariants

| Invariant | Relied on by | Who guarantees it |
|---|---|---|
| A `VioConnRef` passed to an export points at a live `Conn` for the call: the caller holds a `VioConnHandle` | `ConnRef::arc`/`Deref` (`ffi/mod.rs:94-108`) | the bridge: `rc->h` is retained at adoption and cleared on close; ready handles are released after use (`src/rust_io.c:515`, `:610`, `:703-710`) |
| Each `VioConnHandle` is released exactly once | `ConnHandle::drop` | the bridge |
| The runtime pointer is valid forever | every export (`&Runtime`) | `Box::leak` (`lib.rs:1431`, `:1444`) |
| Engine-thread-only exports run on the `vio_start` thread | `RefCell<MainState>` (not `Sync`); a re-entrant borrow panics and aborts | documented only (section 4.2) |
| Pointer arguments are valid for their stated length (`out[max]`, `iov[iovcnt]` and their buffers, `buf[cap]`, `cfg`, `efd`, `s`) | export bodies | caller contract in each `# Safety` comment |
| A `VioCmd`'s argv is private to its IO thread until popped, then the engine's | `Send for VioCmd` (`ffi/mod.rs:46-49`) | queue discipline under `Shared` lock |
| `VioSession.user` is never dereferenced by Rust | `Send for VioSession` | code |
| Callbacks are thread-safe C functions | `Sync for Global` | C: `create_argvs` only allocates (zmalloc, `createStringObject`); `free_argv` only decrements objects whose other references were dropped on the engine thread; `write_job` touches only a client in `CLIENT_PENDING_IO`; precheck callbacks read only the pinned snapshot, the session copy, the command table and the private argv |
| The engine never touches a sealed chunk or frees a client with a job in flight | zero-copy jobs | `io_write_state` checks in the engine, `freeClient()` deferral |
| A fork child never calls into Rust | Rust state is unusable in a child (only the forking thread exists) | `connExternalDirect()` for IO, `connExternalDetach()` returns early in a child, replica and Lua-debugger clients never take the zero-copy path |
| `read_append` sets the length only over bytes the kernel wrote | `sys.rs:91-100` | code |
| An fd is closed only by its `Conn` (`Drop`) or on refusal before a `Conn` exists | `sys::close` | code (`lib.rs:235`, `:1314`) |

### 10.3 What the tools cover

| Tool | Covers | Does not cover |
|---|---|---|
| `cargo test` (24 tests, 25 with `--features timing`) | parser (12: framing, splits, byte-at-a-time, limits, errors, `string2ll`), handoff protocol (3), `conn_state` transitions and concurrent kicks (5), precheck barriers, seqlock tearing, callbacks off (3), two runtimes in one process over real sockets (1) | the FFI exports, the IO-thread loop beyond `two_runtimes`, the C bridge |
| `cargo +nightly miri test` | the same tests except `two_runtimes` (`#[cfg_attr(miri, ignore)]`, `lib.rs:1558`): data races and UB in handoff, conn_state, seqlock, parser | anything touching syscalls, epoll, the exports and their raw pointers, C code |
| `cargo clippy --all-targets -- -D warnings` | lints on lib and tests (not the `bench` feature; `autobenches = false`) | |
| ASAN build of the server | C side (bridge, engine) under the Tcl suite, gate scripts and soaks | Rust code (built without a sanitizer); leaks in the system allocator only through LeakSanitizer |
| Soak tests (connection churn, kills mid-argv and mid-reply, BLPOP kills, output-buffer kills; 600 s stall test) | lifecycle and wakeup races at scale | the soak and stall scripts are not in the repository (`~/rust-io/compat/bin/soak.py`, `stall.py` on the test host) |

Results:

Results at HEAD (raw output: `~/rust-io/final/logs/`, `~/rust-io/final/asan/`, `~/rust-io/final/soak/` on the test host):

| Gate | Result |
|---|---|
| Miri | 23 passed, 1 ignored (`two_runtimes`) |
| cargo test | 24 passed; 25 with `--features timing` |
| clippy `--all-targets -D warnings` | clean; also clean with `--features bench` |
| ASAN (gcc 7, `HAVE_IFUNC 0` local edit, `OPTIMIZATION=-O1`) | smoke.py io and precheck, hazards.py io and precheck: 0 failures, 0 sanitizer reports, server exit 0 |
| soak (ASAN, precheck, 4 IO threads, 600 s) | 498,997 connections, `server alive after soak: True`, 0 sanitizer reports, server exit 0 |
| stall detector (300 s per mode, 2 IO threads, takeover) | io: 18,037,869 requests, max burst latency 0.081 s, stalls 0; precheck: 17,985,307 requests, 0.111 s, stalls 0 |

## 11. Performance

Method: stage 5b matched threads (`~/rust-io/iter3a/tools/bench.py`, `~/rust-io/scaling/report.md`). Server on cores 0..T-1, `valkey-benchmark --threads 16` on cores 16-31, `-c 400 -d 64 -r 1000000 -P 16`, 8 s per cell, SET then GET. C = `--io-threads T`; precheck = `--rust-io-threads T-1 --rust-io-takeover yes`. Fresh server per cell, binaries alternated, 3 rounds, median. Two sessions on the same host. Baseline: `rust-io/iter3a` HEAD `09022d9d5` built the same way. Raw: `~/rust-io/final/perf/bench.csv`, `bench2.csv`.

| T | Mode | Test | iter3a | HEAD | HEAD vs iter3a (session 1 / 2) |
|---|---|---|---|---|---|
| 4 | C io-threads | GET | 2.80M | 2.60M | -7.1% / -7.2% |
| 4 | C io-threads | SET | 1.77M | 1.77M | -0.0% / +0.4% |
| 4 | precheck | GET | 2.77M | 2.72M | -1.7% / -1.6% |
| 4 | precheck | SET | 1.77M | 1.78M | +0.6% / -0.3% |
| 8 | C io-threads | GET | 2.15M | 2.02M | -6.0% / +0.2% (spread 11-20%) |
| 8 | C io-threads | SET | 1.71M | 1.69M | -1.7% / -1.9% |
| 8 | precheck | GET | 2.86M | 2.68M | -6.3% / -7.4% |
| 8 | precheck | SET | 1.75M | 1.75M | -0.2% / +0.2% |

Characteristics at HEAD:

- precheck matches C at T=4 (GET 2.72M vs 2.60M, SET 1.78M vs 1.77M) and leads at T=8 (GET 2.68M vs 2.02M, +33%).
- GET -P 16 in the Rust binary is 2 to 7% below iter3a. The loss arrives with the iter3b side of the merge (C mode T=4 GET: iter3a 2.78M, iter3b 2.38M, merge commit 2.53M, HEAD 2.62M, `bisect2.csv`), and it shows in C mode, where no iter3b code runs. Main-thread instructions per request are unchanged (iter3a 3194-3322, HEAD 3313-3327) while cycles per request vary by up to 12% (`ipr.sh`), as in iter3a's step 3: code placement, not proven.
- The default build (no Rust) shows no GET loss: iter3a 2.51M, HEAD 2.53M (T=4 C mode, `bisect3.csv`).

## 12. Testing

| Gate | Command (from the repo root unless noted) |
|---|---|
| default build | `make distclean && make SERVER_CFLAGS=-Werror` |
| Rust build | `make distclean && make BUILD_RUST_IO=yes SERVER_CFLAGS=-Werror` (also runs `cargo clippy --release -- -D warnings`, `src/Makefile:790`) |
| timing build | `make BUILD_RUST_IO=yes RUST_IO_TIMING=yes SERVER_CFLAGS=-Werror` |
| cargo tests | `cd src/rust/valkey-io && cargo test` |
| clippy | `cd src/rust/valkey-io && cargo clippy --all-targets -- -D warnings` |
| Miri | `cd src/rust/valkey-io && cargo +nightly miri test` |
| header drift | `make -C src BUILD_RUST_IO=yes check-rust-header CLANG_FORMAT=clang-format` with `cbindgen` installed and clang-format 18.1.8 (`src/Makefile:793-804`). `BUILD_RUST_IO=yes` may come from `.make-settings` after a Rust build |
| unit tests, default | `make -C src test-unit` (needs googletest: pkg-config `gtest gmock`, or `GTEST_CFLAGS="-isystem <prefix>/include" GTEST_LIBS="-L<prefix>/lib64 -lgmock -lgtest -lpthread"`; `-isystem` keeps gtest's headers out of `-Werror`) |
| unit tests, Rust | `make -C src BUILD_RUST_IO=yes test-unit` with the same gtest variables (compiles the tests with `-DUSE_RUST_IO` and links `libvalkey_io.a`, `src/unit/Makefile:244-253`). There are no gtests for the layer itself |
| clang-format (CI) | `pip install clang-format==18.1.8; cd src; shopt -s globstar; clang-format -i **/*.c **/*.h **/*.cpp **/*.hpp; git diff --exit-code` (`.github/workflows/clang-format.yml:33-45`) |
| typos (CI) | `typos --config=./.config/typos.toml` (`.github/workflows/spell-check.yml:34`) |
| Tcl suite, io | `./runtest --config rust-io-mode io --config rust-io-takeover yes --tags -needs:c-transport` |
| Tcl suite, precheck | `./runtest --config rust-io-mode precheck --config rust-io-takeover yes --tags -needs:c-transport` (add `--config rust-io-threads N` for more IO threads; `--single <file>` per file) |

Gate scripts in `src/rust/valkey-io/` (start the server yourself unless noted):

| Script | Usage | Checks |
|---|---|---|
| `smoke.py` | `smoke.py <rust_port> <c_port>` | basic commands, pipelining, large values on the Rust port against the C port |
| `hazards.py` | `hazards.py <valkey-server binary> [server args, default: --rust-io-mode precheck]` (starts its own servers) | a prechecked command queued behind BLPOP while ACL, SELECT, cluster or other state changes from a second connection must see the new state; uses `INFO rust_io` deltas |
| `scriptkill.py` | `scriptkill.py <port> <iters>` (docstring says `killrepro.py`) | `SCRIPT KILL` from a connection in the same ready batch as a long script |
| `backpressure.py` | `backpressure.py <rust_port> <c_port>` (no usage line; from `sys.argv`, `backpressure.py:6`) | a client blocked in BLPOP that keeps pipelining hits `EAGAIN` after socket buffers plus about 2048 queued commands |
| `killself-bgsave.py` | `killself-bgsave.py <port> [iterations, default 5] [fork\|forkless, default fork]` | `CLIENT KILL SKIPME no` during BGSAVE still delivers its own reply (deferred shutdown) |

Suite status at HEAD (per file, `--single --clients 1 --timeout 600`, each file in its own tree copy, 6 files at a time per mode, three modes at once):

| Mode | Files | Pass | Fail | Skip | Aborted file |
|---|---|---|---|---|---|
| default build | 174 | 6132 (+ 4 tests in the rerun of the files below) | 0 | 44 | unit/mptcp |
| io, takeover, `--tags -needs:c-transport` | 174 | 6118 | 0 | 52 | unit/mptcp |
| precheck, takeover, `--tags -needs:c-transport` | 174 | 6115 | 0 | 52 | unit/mptcp |

- unit/mptcp times out on this host with plain upstream `6c4f6fb85` too (`[TIMEOUT]: ASSIGNED: sock6 (unit/mptcp)`): host environment.
- The default run used test files from before `c7c080aae`, so four files there failed on the `{}` result pattern (section 9); rerun at HEAD they pass: introspection 125/0, networking 6/0, announced-endpoints 4/0, replication-flow-control 1/0, io-threads 2/0.
- unit/cluster/slot-migration "Empty-shard migration target/source is auto-updated after failover" are known upstream flakes (valkey-io/valkey#5215, #5232); they passed in all three local runs.
- No server crashed (`ASSERTION FAILED`, `BUG REPORT START`, `crashed by signal`: no match in any log).

## 13. Known limitations and open work

Unsupported or refused:

- TLS, Unix sockets, RDMA, cluster bus, outbound connections; only the first bind address; no IPv6 from the default bind.
- `CONFIG SET bind`/`port` changes under takeover.
- Durability fences: `release_offset` must be 0; a nonzero value aborts (`exports.rs:396`).
- No `vio_stop`; one runtime per process lifetime (two can coexist in tests).

Accounting:

- Normal clients' Rust-held output (up to 1 MiB each) is not in `omem` or the COB limits. Counting it broke `unit/obuf-limits` because Rust drains without a limit check; a drain-side check is not implemented.
- Shared per-thread read buffers (up to 64 KiB each) and write buffers (up to 4 MiB retained) are in the system allocator and counted nowhere; private read buffers count only toward maxmemory, not `used_memory`.
- `INFO rust_io` counters are not reset by `CONFIG RESETSTAT`.

Precheck:

- At most 64 users are copied; users with roles, module users and users beyond 64 fall back. Every ACL mutation rebuilds the whole snapshot (O(users)).
- The token keeps 32 bits of the epoch and 24 bits of the session generation: a false match needs exactly 2^32 ACL mutations (or 2^24 session changes on one connection) while one command waits.
- Class C state (pause, loading, disk errors, min-replicas, role) is not snapshotted; those checks stay live on the engine thread.
- Any registered command filter disables the fast path.
- `MODULE LOAD`/`UNLOAD` races off-thread `lookupCommand` (shared with C IO threads).

Threading and latency:

- Accept runs only on thread 0.
- A detach blocks its IO thread (all of that thread's connections) for up to 5 s and the engine thread for up to 6 s. After `DET_FAILED` the connection stays flagged detached on the engine side while Rust keeps polling it; the engine treats it as broken.
- Argv is freed on the engine thread when `rust-io-threads` is 1.
- Replica, primary and slot-migration links always copy their output.
- The slot-import stream exists twice (argv in `cmd_queue` and RESP in `querybuf`).
- The engine-thread contract of the exports is not enforced at runtime.

Tooling and code hygiene:

- `valkey_io.h` defines `NHIST (NSTAMPS + 1)` under `RUST_IO_TIMING` while `NSTAMPS` is excluded from the header (`valkey_io.h:92`, `cbindgen.toml:18`); unused by C.
- Stale comments and docs: `src/rust_io.c:1-10` (says cut point 1 and a connection of type `"rust"`; the type is `"external"`), `src/rust_io_precheck.c:265` (`VioCallbacks.precheck_cmd`, now `precheck_batch`), `hazards.py:5` (exec mode example; exec mode is deleted), `scriptkill.py:3` (`killrepro.py`).
- `sys::writev` is unused.
