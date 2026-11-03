# valkey-io: Rust IO layer, stage 1 (cut point 1, "io")

Rust owns the client sockets, reads, RESP parsing and writes. The engine keeps
all client handling: `processCommand()` and `call()` run exactly as for a TCP
client. The boundary is one C file on the engine side (`src/rust_io.c`), one
hand-written header for the Rust exports (`include/valkey_io.h`), and 16
exported Rust functions plus 2 engine callbacks.

## Build and configuration

`make BUILD_RUST_IO=yes` runs `cargo build --release` in this directory, links
`target/release/libvalkey_io.a` and compiles the engine with `-DUSE_RUST_IO`.
The default `make` does not build or link the crate and compiles none of the
bridge (everything is behind `USE_RUST_IO`).

| Config | Values | Meaning |
|---|---|---|
| `rust-io-mode` | `off` (default), `io`, `precheck`, `exec` | `exec` logs a warning and runs as `io`. `precheck`: see "Stage 2" below. |
| `rust-io-port` | 0..65535, default 0 | Extra port served by Rust. Ignored when takeover is on. |
| `rust-io-threads` | 1..128, default 1 | Rust IO threads, each with its own epoll loop. |
| `rust-io-takeover` | yes/no, default no | Rust binds `port`; the C TCP listener is not created. |

All four are startup-only (`IMMUTABLE_CONFIG`). Rust binds the first `bind`
address (`*` maps to 0.0.0.0, `-::*` to `::`).

## Threading and ownership

```
            accept (thread 0)            round robin
listener ───────────────▶ rust-io-0 ──────────────▶ rust-io-1..N-1
                            │  each IO thread: epoll over its own sockets,
                            │  per-connection read buffer + RESP parser,
                            │  builds robj argv via create_argv()
                            ▼
                ready list (Mutex<Vec<Arc<Conn>>>)  + one eventfd write
                            │  per IO-thread poll iteration
                            ▼
main thread: ae event on the eventfd -> rustIoEventHandler()
   for each ready conn: adopt (NEW) / pull commands into c->cmd_queue /
   processInputBuffer() / free on CLOSED
   replies: writeToClient() -> conn writev -> copy into Conn.out, kick
                            │  inbox entry + eventfd write, coalesced
                            ▼
IO thread swaps Conn.out into its local buffer and write()s the socket
```

- A connection (`Conn`, `src/lib.rs`) is an `Arc` shared by exactly one IO
  thread and the main thread. The IO thread exclusively owns the read buffer,
  the parser and the in-flight write buffer. The shared part is a `Mutex`
  with the parsed-command queue and the outbound byte buffer, plus atomics
  for events and flags.
- The engine holds one strong reference per adopted connection (in
  `rustConnection.h`) and drops it in the connection type's `close`. Every
  handle returned by `vio_main_next_ready()` carries one more reference that
  the caller releases.
- The fd is closed in `Conn::drop`, i.e. only when both sides released it.
  When the peer closes first, the IO thread stops polling the fd but keeps it
  open, so the engine's `conn->fd` (used by `CLIENT LIST`, `connAddr`) cannot
  be reused under it.
- Rust connections are never handed to the C IO threads
  (`trySendReadToIOThreads`/`trySendWriteToIOThreads` return `C_ERR`).

### argv construction (decision)

Built on the Rust IO thread through the `create_argv` callback, which runs
`createStringObject()` per argument (`rustIoCreateArgv`, `src/rust_io.c`). The
C IO threads already allocate argv off the main thread, `zmalloc` is thread
safe and accounts extra threads in `used_memory_for_additional_threads`
(`src/zmalloc.c:136`), and the objects are private until handed over. The
alternative, building robj on the main thread from Rust-owned slices, keeps
the boundary free of engine callbacks but puts one allocation and copy per
argument back on the main thread, which is the work this layer exists to
move. Rejected.

## Handoff protocol

Rust to engine, per connection:

1. The IO thread parses everything in the read buffer, builds argv for each
   command, and appends the batch to `Shared.cmds` under the lock. A protocol
   error is appended as a final item with `err != 0`, and the IO thread stops
   reading that socket.
2. It sets event bits (`VIO_EV_NEW`, `VIO_EV_CLOSED`, `VIO_EV_WRITABLE`,
   `VIO_EV_QBLIMIT`) and,
   if the connection is not already queued (`in_ready`), adds it to the
   thread's notify list. After the epoll iteration the list is appended to the
   global ready list and the main eventfd is written only if
   `main_signaled` was clear. One wakeup per poll iteration, not per command.
3. The main thread's ae handler calls `vio_main_begin_drain()` (reads the
   eventfd, then clears `main_signaled`, then takes the ready list), then for
   each handle `vio_main_next_ready()` (clears `in_ready`),
   `vio_conn_take_events()`, and pulls commands. Order matters: a producer
   that finds `main_signaled` set skips the eventfd write, so clearing the
   flag before draining the eventfd can lose the last wakeup forever (this
   happened; fixed in "fix lost wakeup in the eventfd handoff"). Clearing a
   flag before reading the data it guards (`in_ready` before events and
   commands) means a racing producer always re-queues. The IO-thread inbox
   uses the same drain-clear-take order.
4. `rustIoPull()` moves up to 1024 commands into `c->cmd_queue` as
   `parsedCommand` with `READ_FLAGS_PARSING_COMPLETED`, runs
   `prepareCommandGeneric()` (lookup, arity, cluster slot) on each, and
   accounts input bytes like `handleReadResult()`. Then the tail of
   `readQueryFromClient()` runs: `processInputBuffer()`, `trimCommandQueue()`,
   `beforeNextClient()`.
5. `c->querybuf` stays NULL, so `processInputBuffer()` only consumes the
   queue. Blocking commands, MULTI, CLIENT PAUSE, `pending_command`, and
   resume after unblock (`processUnblockedClients()` ->
   `processPendingCommandAndInputBuffer()`) all act on `cmd_queue` and work
   unchanged.
6. A protocol error is replied (same text as `handleParseError()`) only after
   every earlier command executed, then `close_after_reply` is set.

Engine to Rust (replies):

1. `writeToClient()` calls the connection's `writev`, which copies into
   `Shared.out` (up to 1 MiB pending) and kicks the IO thread: one inbox entry
   per connection until the IO thread picks it up, one eventfd write per IO
   thread wakeup.
2. Past 1 MiB, `writev` returns -1/`EAGAIN`. The engine keeps the rest in its
   own reply list (so `client-output-buffer-limit` applies as for TCP) and
   installs its write handler. `set_write_handler` turns into
   `vio_conn_want_writable(1)`; after each flush that leaves less than 256 KiB
   pending, Rust sends `VIO_EV_WRITABLE` and the main thread calls the write
   handler. Level triggered, like a socket.
3. The IO thread swaps `Shared.out` with its local buffer and writes without
   holding the lock, so the main thread never waits on a socket.

Close:

- Engine close (`freeClient`, `CLIENT KILL`, `QUIT` after its reply):
  `connRustClose()` -> `vio_conn_close()` + release. Rust flushes pending
  output, then drops its reference. There is no flush timeout: a peer that
  never reads keeps the Rust buffer (at most 1 MiB) until it disconnects.
- Peer close or socket error: Rust sets `VIO_EV_CLOSED`. The main thread
  first runs whatever commands were already parsed, then `freeClientAsync()`.

### Backpressure

- Input: the IO thread stops polling `EPOLLIN` once 1024 commands wait in
  `Shared.cmds` and resumes below 256 (`vio_conn_pop_cmds` kicks it). The
  engine pulls at most 1024 into `cmd_queue`. A slow or blocked client
  therefore costs at most ~2048 parsed commands plus one 16 KiB read.
- Output: 1 MiB in Rust, the rest in the engine's reply list (see above).
- Query buffer: after each read the IO thread compares its unparsed bytes
  with the live `client-query-buffer-limit` (1 MiB while unauthenticated),
  like `readToQueryBuf()`, and reports `VIO_EV_QBLIMIT`; the engine logs,
  counts `client_query_buffer_limit_disconnections` and frees the client.
  Unlike C, the MULTI queue (`mstate->argv_len_sums`) is not added in.
- Clients whose input is held in Rust (queue full, blocked, or
  `protectClient()` removed the read handler) are kept in `rust_stalled` and
  retried from `beforeSleep()` through the connection type's
  `process_pending_data`. `has_pending_data` only reports clients that can
  progress, so a blocked client does not make the event loop spin.

## FFI surface

### Rust symbols called by C (`include/valkey_io.h`)

| Symbol | Purpose |
|---|---|
| `vio_start(cfg)` | Bind the listener, start N IO threads; returns the eventfd the engine polls. |
| `vio_main_begin_drain()` | Clear the wakeup and take the current batch of ready connections. |
| `vio_main_next_ready()` | Next ready handle (owned reference) or NULL. |
| `vio_conn_retain(h)` | Take a reference (engine adopts a connection). |
| `vio_conn_release(h)` | Drop a reference. |
| `vio_conn_take_events(h)` | Read and clear `VIO_EV_*` bits. |
| `vio_conn_udata(h)` | Engine connection pointer, NULL once closed. |
| `vio_conn_set_udata(h, p)` | Set it on adoption. |
| `vio_conn_fd(h)` | Socket fd, for address formatting and `CLIENT LIST`. |
| `vio_conn_pop_cmds(h, out, max)` | Move parsed commands out, in order. |
| `vio_conn_pending_cmds(h)` | Commands still queued in Rust. |
| `vio_conn_writev(h, iov, n)` | Copy reply bytes out; -1/EAGAIN when full, -1/EPIPE after peer close. |
| `vio_conn_want_writable(h, on)` | Engine write handler installed/removed. |
| `vio_conn_set_auth_required(h, on)` | Publish `authRequired(c)` so the parser applies unauthenticated limits. |
| `vio_conn_close(h)` | Engine is done; flush then close. |
| `vio_conn_shutdown(h)` | `shutdown(2)` on the socket. |

### C symbols called by Rust (callbacks in `VioCallbacks`, IO threads)

| Symbol | Purpose |
|---|---|
| `rustIoCreateArgv(ptrs, lens, argc)` | `zmalloc` an argv and `createStringObject()` each argument. |
| `rustIoFreeArgv(argv, argc)` | Free commands the engine never consumed (connection dropped). |

Rust links no engine symbol directly; both are passed in `vio_start()`. That
keeps `cargo test` linkable without the engine and keeps the dependency
explicit. The IO threads also read `server.proto_max_bulk_len` through a
pointer given in `VioConfig` (a racy aligned 64-bit read, as the C IO threads
do), so `CONFIG SET proto-max-bulk-len` takes effect. Same for
`server.client_max_querybuf_len`.

## Changes outside the bridge and the crate

All guarded by `USE_RUST_IO` except where noted.

| File | Change | Why |
|---|---|---|
| `src/Makefile` | `BUILD_RUST_IO=yes`: `-DUSE_RUST_IO`, link the static lib and `-lpthread -ldl -lrt -lm -lgcc_s` (Rust std), add `rust_io.o`, a cargo rule, persist the setting. | Build integration. Inert when unset. |
| `src/connection.h` | `CONN_TYPE_RUST` enum value and its name `"rust"`. | Register a connection type. |
| `src/connection.c` | Register the type in `connTypeInitialize()`. | Same. |
| `src/server.h` | Include `rust_io.h` (empty without `USE_RUST_IO`). Prototype for `prepareCommandGeneric()` (unguarded). | Declarations. |
| `src/server.c` | `prepareCommandGeneric()` made non-static (unguarded). | The bridge prepares each queued command the way `prepareCommandQueue()` does, without re-preparing the current one. |
| `src/server.c` | `initListeners()`: skip the C TCP listener under takeover; start the Rust listener and count it. | Takeover, and avoid "Configured to not listen anywhere". |
| `src/server.c` | Log `Ready to accept connections rust`. | The Tcl harness waits for a "Ready to accept" line per listener. |
| `src/config.c` | The four configs and an `extern` for the mode enum. | Configuration. |
| `src/io_threads.c` | `trySendReadToIOThreads`/`trySendWriteToIOThreads` refuse Rust connections. | Rust does its own IO. |

No change to `networking.c`.

## What cut point 1 still costs the main thread

Per wakeup (one per Rust IO thread poll iteration that produced work):
`epoll_wait` return, one 8-byte `read(2)` on the eventfd, one mutex
lock/swap of the ready list.

Per connection with work: two atomic swaps, one mutex lock per 128
commands popped, a `memmove`/`zrealloc` of `cmd_queue` when needed.

Per command, unchanged from today's single-threaded path:
`prepareCommandGeneric()` (command lookup, arity, cluster slot: the C IO
threads do this off the main thread today, so this is a regression versus
`io-threads > 1`), all of `processCommand()`, `call()`, reply construction
into `c->buf`, `commandProcessed()`/`resetClient()` including freeing argv
(`decrRefCount` + `zfree` per argument), `beforeNextClient()`.

Per reply flush: `writeToClient()` builds the iovec as today, then a
`memcpy` of every reply byte into the Rust buffer under a mutex (instead of
`writev(2)`), plus one inbox lock and at most one eventfd `write(2)` per IO
thread per main-loop iteration.

Not on the main thread any more: `read(2)`, `write(2)`/`writev(2)`, RESP
parsing, argv allocation and copy, `epoll` registration changes for client
sockets.

Measured (valkey-benchmark, 64-byte values, see the stage report for the
full matrix): main-thread CPU per million requests at `-P 1` is ~14 s for
C with `io-threads 1`, ~4.4 s for C with `io-threads 4`, 1.1-1.6 s for Rust
with 1 IO thread (the IO thread is the bottleneck, so batches are large),
and ~4.0 s for Rust with 4 IO threads (the main thread is the bottleneck at
~222k RPS). The per-request main-thread cost is dominated by per-iteration
fixed costs (epoll, eventfd read, one eventfd write per IO thread kicked)
that amortize only when batches are large. Not profiled (no `perf` on the
build host); this attribution is inferred from how the cost moves with
batch size. Candidate for later stages: defer IO-thread kicks to one pass
at the end of `beforeSleep()` and let busy IO threads skip the eventfd.

## Known gaps

- `CLIENT LIST` `qbuf`/`qbuf-free` are always 0: buffered input lives in Rust.
- Rust read and write buffers use the system allocator and are not in
  `used_memory` or client eviction accounting. Parsed-but-unexecuted
  commands are bounded instead (see Backpressure).
- `CONFIG SET bind` / `port` under takeover try to bind a C listener on the
  port Rust holds and fail.
- Only TCP. No TLS, no Unix socket, no outbound connections (replication and
  cluster bus stay on C sockets). A replica connecting to the Rust port is
  untested; `SYNC` writes go through the same copy path.
- `auth_required` is published after each batch, so the parser may apply the
  pre-AUTH limits to commands pipelined behind `AUTH` in the same read.
  The C parser stops at `AUTH` for the same reason.
- No `rust-io` stats in `INFO`.

## Hooks for later stages

### Stage 2 ("precheck")

Implemented; see "Stage 2: precheck" below.

### Stage 3 ("exec")

Where: the end of `src/rust_io.c`, marked `STAGE 3 HOOK`. Rust would call
an exported C entry point equivalent to `call()` (or `cmd->proc`) itself
instead of queuing for `processInputBuffer()`. It must hold the module GIL
(`moduleAcquireGIL()`/`moduleReleaseGIL()`, `src/module.c`) which the main
thread already releases around `aeApiPoll()` in `beforeSleep()`/`afterSleep()`
(`src/server.c`), so engine state is only touched by one thread at a time.
`server.current_client`, `server.in_exec`, propagation and keyspace
notification paths assume the main thread and would need auditing.

# Stage 2: precheck (cut point 2)

`rust-io-mode precheck`. The Rust IO thread runs the checks at the top of
`processCommand()` right after parsing and tags each command with a token.
The main thread validates the token with a few loads; if it holds,
`processCommand()` skips the checks that were done off-thread and runs the
rest unchanged. Any doubt falls back to the full, unchanged path. A wrong
fast path is a bug; a fallback is always allowed.

Code: `src/rust_io_precheck.c` (policy, snapshot, validation),
`src/rust_io_precheck.h`, `src/rust/valkey-io/src/precheck.rs` (batch
pinning, session mirror, barriers). Edits to shared files are listed at the
end.

## Pipeline per command

1. IO thread, `parse_buffered()` (`src/lib.rs:408`): pin the snapshot for the
   batch (`precheck::Batch::begin`, `lib.rs:423`), copy the connection's
   published session, and for each parsed command call `precheck_cmd`
   (`rustPrecheckCmd`, `rust_io_precheck.c:236`):
   - lookup, arity and cluster slot (what `prepareCommandGeneric()` does),
     except module commands, which are left for the main thread
     (`READ_FLAGS_RUST_PREPARE_ON_MAIN`): their getkeys callback is module
     code, the valkey#5054 race;
   - the decision in `pcDecide()` (`rust_io_precheck.c:209`, table below);
   - token = `epoch32 << 32 | session_gen24 << 8 | code`.
2. Main thread, `rustIoPull()` (`src/rust_io.c:89`): copies cmd/slot/flags/
   token into the `parsedCommand` and sets `READ_FLAGS_RUST_PRECHECKED`; no
   `prepareCommandGeneric()` on the main thread in precheck mode.
3. `consumeCommandQueue()` copies the token into `c->rust_precheck`.
4. `processCommand()` (`src/server.c:4674`) calls `rustPrecheckValidate()`
   (`rust_io_precheck.c:289`) when not reprocessing. It returns 1 only if:
   code is `PC_FAST`; the token's epoch equals `acl_mutation_epoch`; the
   token's session generation equals the last one published for this
   connection *and* the client's current `user`, `db->id` and session flags
   equal the published ones; no module command filter is registered and no
   module disabled cluster redirection; in cluster mode, for a keyed command,
   `getNodeBySlot(slot) == myself`, the slot is neither migrating nor
   importing, and `isClusterHealthy()`. Every outcome is counted.
5. When it returns 1, `processCommand()` skips module filters (none exist),
   the security/existence/auth/arity/protected block (`server.c:4695`), the
   NO_MULTI check, `ACLCheckAllPerm()` and `getNodeByQuery()`
   (`goto prechecked_resume`, `server.c:4773` to `:4825`). Everything else
   runs unchanged.

Session correctness never depends on Rust: the main thread compares the
current client fields with the published copy the token was computed
against. Barriers (below) only avoid prechecking against a session that is
known to be stale.

## Snapshot contents and publication

`pcSnapshot` (`rust_io_precheck.c`):

| Field | Source | Why |
|---|---|---|
| `epoch` | `acl_mutation_epoch` at build | validation key |
| `default_auth_required` | `DefaultUser` nopass/disabled | `authRequired()` off-thread |
| `modules` | `moduleCommandFilterCount() > 0` | filters can rewrite argv: fall back |
| `live[i]`, `copy[i]` (max 64) | every user in `Users` without roles | ACL check off-thread |

ACL users are mutated in place (`ACL SETUSER` builds a temp user and
`ACLCopyUser()`s it back), so the IO thread never reads a live `user`. The
snapshot holds private deep copies (`ACLCreateUnlinkedUser()` +
`ACLCopyUser()`), and the IO thread runs the unchanged
`ACLCheckAllUserCommandPerm()` on the copy. Live pointers are kept only as
identity keys. Users that hold roles are not copied (the copy would read the
roles' live selectors) and fall back with `acl_user`.

Publication (RCU with hazard pointers):

- Build and swap on the main thread in `rustPrecheckPublish()`
  (`rust_io_precheck.c:146`), called from `beforeSleep()` through the
  connection type's `process_pending_data` (`rust_io.c:325`) and at startup.
  Rebuild only when `acl_mutation_epoch` or the filter count changed. Build
  and free of the copies restore `acl_mutation_epoch` so they do not
  invalidate themselves.
- IO thread pins per batch: store the pointer in its hazard slot, re-load
  the global pointer, retry if it moved (`rustPrecheckBegin`,
  `rust_io_precheck.c:188`); clear the slot after the batch. One seq_cst
  store and two loads per batch, not per command.
- Old snapshots go on a retired list and are freed on the main thread once no
  hazard slot holds them (`pcReclaim`). `ACLFreeUser()` therefore never runs
  off the main thread.

Deviation from the recommended design: the slot-ownership bitmap and the
class C flags are **not** in the snapshot. Slot ownership is re-read live in
`rustPrecheckValidate()` (3 loads and `isClusterHealthy()`), which is as
cheap as an epoch compare and needs no hook in the cluster bus code, where
ownership changes without any command. The class C checks (disk error,
min-replicas, read-only replica, stale replica, loading, busy script/module,
pause, replica keyspace access, redirects) are not skipped at all: they stay
in `processCommand()` after `prechecked_resume:` and read live state. They
cost a few loads each in the boring state and removing them would need an
epoch hook at every site listed below; the measured gain of the fast path
comes from lookup, ACL and slot work, not from these flags.

## Epoch mutation sites

Hooked (`acl_mutation_epoch++`, `src/acl.c`):

| Site | Line | Covers |
|---|---|---|
| `ACLFreeUser()` | `acl.c:559` | `ACL DELUSER`, `ACL LOAD` replacing users, pointer reuse |
| `ACLCopyUser()` | `acl.c:609` | `ACL SETUSER` commit, `ACL LOAD` of the default user |
| `ACLRecomputeCommandBitsFromCommandRulesAllUsers()` | `acl.c:1014` | module command registration |
| `ACLSetUser()` | `acl.c:1770` | `requirepass`, module ACL APIs, every op on a user |
| `ACLRemapSurvivingRoleMembers()` | `acl.c:3035` | role membership changes in `ACL LOAD` |

Not hooked, because the PoC does not skip the check they feed (it stays live
in `processCommand()`). A production version that moves these into the
snapshot must hook every one:

| State | Mutation sites |
|---|---|
| client pause | `pauseActions()`/`unpauseActions()` (`networking.c`), pause expiry in `checkClientPauseTimeoutAndReturnIfPaused()`, `FAILOVER` state machine (`replication.c` `updateFailoverStatus`, `abortFailover`) |
| loading / async loading | `startLoading()`/`stopLoading()` (`rdb.c`), `DEBUG RELOAD`, diskless replica load |
| disk write errors | `server.lastbgsave_status` (`backgroundSaveDoneHandler`), `server.aof_last_write_status` (`aof.c` `flushAppendOnlyFile`), `stop-writes-on-bgsave-error` config |
| min-replicas | `refreshGoodReplicasCount()` (`replication.c`, from `replicationCron` and replica ack), the two `min-replicas-*` configs |
| role, read-only, stale replica | `replicationSetPrimary()`/`replicationUnsetPrimary()`, `server.repl_state` transitions, `replica-read-only`, `replica-serve-stale-data` |
| busy script / module | `scriptTimedOut` paths (`script.c`), `server.busy_module_yield_flags` (`module.c` `RM_Yield`) |
| cluster slots | `clusterUpdateSlotsConfigWith()`, `clusterAddSlot()`/`clusterDelSlot()`, `CLUSTER SETSLOT` (`cluster_legacy.c`), migrating/importing arrays, atomic slot migration jobs, `clusterUpdateState()`, `cluster_module_flags` |
| command table | `MODULE LOAD/UNLOAD` (`moduleRegisterCommand`, unregister), which also races any off-thread lookup (C IO threads share this) |
| protected configs | `enable-debug-command`, `enable-module-command` (DEBUG/MODULE are `CMD_PROTECTED`, always fall back) |

## Barrier commands

Resolved by name at init (`pc_barrier_names`, `rust_io_precheck.c:65`) and
compared by command pointer, parent for subcommands: `AUTH HELLO SELECT MULTI
EXEC DISCARD RESET SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE SSUBSCRIBE
SUNSUBSCRIBE READONLY READWRITE CLIENT ACL SYNC PSYNC REPLCONF QUIT`.

After a barrier with sequence number B, later commands on the connection get
`PC_FB_BARRIER` until the engine publishes `consumed >= B`
(`precheck.rs`). The engine publishes `{user, db, flags, gen, consumed}` after
every delivery (`rustIoPublishSession`, `rust_io.c:161`); `gen` changes only
when a field changes.

The first version compared the parent's `proc`; container commands have no
proc of their own, so `ACL SETUSER` and every `CLIENT` subcommand were never
barriers. The epoch check still caught it (fallback `epoch` instead of
`barrier`), which is the point of validating on the main thread. Found with
the negative-control build in the stage report.

Unauthenticated connections never hit the barrier path in practice: the
parser now stops after one command while `auth_required` is set (see "Stage
1 fixes"), so the command after `AUTH` is parsed after the engine published
the post-AUTH session.

## Decision table

IO thread (`pcDecide`), first match wins:

| Condition | Code | Main thread then |
|---|---|---|
| no snapshot published yet | `no_snapshot` | full path |
| session never published (first read before adoption) | `no_session` | full path |
| barrier pending | `barrier` | full path |
| command filters registered | `modules` | full path |
| replica, primary, slot-migration client, no user, must-obey | `special_client` | full path |
| `MULTI` open | `multi` | full path (queues) |
| RESP2 pub/sub mode | `pubsub_resp2` | full path |
| unknown command, wrong arity | `PC_REJECT` | validate, then full path emits the error |
| auth required and command not `CMD_NO_AUTH` | `PC_REJECT` | same |
| `CMD_MODULE`, `CMD_PROTECTED`, `CMD_PUBSUB`, EXEC, EVAL*, FCALL* | `ineligible` | full path |
| `CROSSSLOT` | `PC_REJECT` | same |
| user not in snapshot (roles, >64 users, module user) | `acl_user` | full path |
| ACL denies (cmd, key, channel, db) | `PC_REJECT` | same |
| otherwise | `PC_FAST` | validate |

Main thread (`rustPrecheckValidate`), for `PC_FAST` and `PC_REJECT`:

| Check | Fails as |
|---|---|
| token epoch != `acl_mutation_epoch` | `epoch` |
| token gen != published gen, or user/db/flags differ from published | `session` |
| filters registered, or module disabled cluster redirection | `modules_live` |
| (`PC_REJECT` stops here) | `rejected` |
| cluster, keyed: slot not mine, migrating, importing, or cluster unhealthy | `cluster` |
| all hold | `fast` |

Rejection protocol: a predicted rejection is validated like a fast path and
then runs the full path, which emits the error. Its position in the reply
stream is therefore the command's own, and `rejected_calls`, error stats,
`ACL LOG` (with client info) and module ACL-rejected events are produced by
the unchanged code on the main thread. Rejected alternative: Rust building
the error text and a "rejected" record doing the accounting. It would
duplicate `rejectCommand*()`, `addACLLogEntry()` and the module events for a
path that is rare in steady state; the only saving would be the check itself.

Counters: `INFO rust_io` (explicit section only), plus one log line at exit.

## What the main thread still does per command in precheck mode

Per command on the fast path (`src/server.c`):

- `rustPrecheckValidate()` (`:4674`): one load of the epoch, the published
  session (5 compares), `moduleCommandFilterCount()`, and in cluster mode
  `getNodeBySlot`/`getMigratingSlotDest`/`getImportingSlotSource`/
  `isClusterHealthy`.
- `reqresAppendRequest()` (`:4680`, no-op unless built with req-res logging).
- busy-module-yield postpone check (`:4685`).
- `getCommandFlags()` and flag derivation (`:4754`), `mustObeyClient()`.
- cluster replica keyless redirect and standalone `-REDIRECT` checks
  (`:4825`-`:4890`).
- class D, unchanged: `evictClients()` (`:4892`), maxmemory with
  `performEvictions()` and `trackingHandlePendingKeyInvalidations()`
  (`:4904`), `trackingLimitUsedSlots()` (`:4937`), deny-oom.
- class C, unchanged, live: disk error (`:4942`), min-replicas (`:4967`),
  read-only replica (`:4974`), RESP2 pub/sub (`:4981`), stale replica
  (`:4997`), loading (`:5005`, `:5011`), busy script/module (`:5023`),
  replica keyspace (`:5041`), pause with `blockPostponeClient()` (`:5050`).
- throttling (`:5055`), `queueMultiCommand()` (`:5061`, never on the fast
  path since MULTI falls back), `call()` (`:5065`),
  `handleClientsBlockedOnKeys()`.
- everything around `processCommand()` from stage 1: `commandProcessed()`,
  `resetClient()` (argv free), reply construction, the writev copy, the
  handoff.

Removed from the main thread compared with io mode: `prepareCommandGeneric()`
(lookup, arity, slot hashing) for every command, including fallbacks; the
auth, existence, arity, protected, NO_MULTI checks; `ACLCheckAllPerm()`
(key-pattern matching for restricted users); `getNodeByQuery()`.

Added on the main thread: `rustIoPublishSession()` per delivery (field
compare plus one mutex store), the validation loads per command, and a
snapshot rebuild after each ACL mutation (copies every user; O(users)).

## Compatibility with in-flight upstream work

AOF durability (valkey#3381). It fences replies at per-client byte offsets
inside the C write path (`clientHasPendingReplies`, `writevToClient`,
`_writeToClient`) and brackets `call()` with
`beginCommandReplyBlocking()`/`finalizeCommandReplyBlocking()` in
`processCommand()`. Precheck mode keeps both: the fast path still reaches the
same `call()` site, so the bracket wraps it unchanged, and replies still
leave through `writeToClient()` before the Rust `writev` copy, so the fence
caps what is handed to Rust exactly as it caps what is written to a socket.
If `processCommand()` is later split into "checks" and "execute", the bracket
belongs in the execute half, immediately around `call()`, after
`throttle_throttleClientIfNeeded()` and after any gate that may block the
client (below), so a blocked-then-re-executed command opens its bracket only
when it really runs. One interaction to watch: #3381 also touches
`trySendWriteToIOThreads`; Rust connections never go there (stage 1), so the
Rust copy path must apply the same cap, which it does today only because it
sits behind `writevToClient()`.

Data tiering (valkey#3326). Its gate goes at the end of `processCommand()`
after throttling: `dbFind()` on the command's keys, and for cold keys block
with `BLOCKED_INUSE`, fetch, then re-execute. It needs the keyspace, so it is
class D and stays on the main thread. In precheck mode it goes at the same
place, after `prechecked_resume:` and after
`throttle_throttleClientIfNeeded()` (`server.c:5055`), before the MULTI
queue/`call()` branch. Re-execution goes through the reprocessing path
(`c->cmd` set, `client_reprocessing_command`), which never consults the
token, so a command that blocked on a cold key re-runs ACL and cluster checks
against live state when it resumes, which is correct after an arbitrary wait.

Throttling and module getkeys (valkey#5054). Throttling stays on the main
thread unchanged. Module commands are never prepared off-thread (step 1), so
no module getkeys callback runs on a Rust thread. Module command filters
force full fallback. Other module callbacks reachable from the precheck: none
(`ACLCheckAllUserCommandPerm()` on a copied user does not call into modules;
module users are not in the snapshot).

## Stage 1 fixes made in this stage

- New connections start with `auth_required` from the default user
  (`vio_set_default_auth_required`, `lib.rs:916`), so the first read is
  parsed with unauthenticated limits when a password is set. Before, a client
  could send a 100 MB bulk header as its first command and hang the
  connection (unit/auth "multibulk and bulk length are limited").
- While `auth_required`, the parser stops after one command and resumes when
  the engine republishes (`parse_held`, `lib.rs:459`), matching the C parser.
  Before, commands pipelined behind AUTH were parsed with pre-AUTH limits
  (unit/auth "Unauthenticated multibulk limit works in pipeline after AUTH").

## FFI added in stage 2

Rust exports (`include/valkey_io.h`):

| Symbol | Purpose |
|---|---|
| `vio_conn_set_session(h, s)` | Publish the session fields the precheck reads. |
| `vio_set_default_auth_required(on)` | Initial `auth_required` for new connections (fix above). |

C callbacks (`VioCallbacks`, NULL outside precheck mode):

| Symbol | Purpose |
|---|---|
| `precheck_begin(tid)` = `rustPrecheckBegin` | Pin the snapshot for one batch (hazard slot). |
| `precheck_end(tid)` = `rustPrecheckEnd` | Unpin. |
| `precheck_cmd(snap, sess, barrier_active, cmd)` = `rustPrecheckCmd` | Lookup/arity/slot, decision, token; returns 1 for a barrier. |

Structs: `VioSession` (new), `VioCmd` gained `cmd`, `slot`, `read_flags`,
`token`.

## Edits to shared files in stage 2

| File | Change |
|---|---|
| `src/server.c` | `processCommand()`: call `rustPrecheckValidate()`, skip filters/top checks/NO_MULTI/ACL/getNodeByQuery when it returns 1 (`goto prechecked_resume`); `is_keyless` moved above the jump. `INFO rust_io` section. |
| `src/server.h` | `rust_precheck` field in `parsedCommand` and `client`. |
| `src/networking.c` | `consumeCommandQueue()` copies `rust_precheck`. |
| `src/acl.c` | `acl_mutation_epoch` and its 5 bumps; `ACLCopyUser()` no longer static. |
| `src/module.c` | `moduleCommandFilterCount()`. |
| `src/rust_io.c` | token handoff in `rustIoPull()`, session publication, snapshot publish in `process_pending_data`, callbacks at start. |
| `src/rust_io.h` | include `rust_io_precheck.h`. |
| `src/Makefile` | `rust_io_precheck.o`. |
| `src/rust/valkey-io/src/lib.rs` | `VioCmd`/`VioCallbacks` fields, session cell, precheck calls in the parse loop (split into `parse_buffered()`), `parse_held`, two exports. |

## Thread-safety notes for the off-thread checks

- Command lookup: `server.commands` and subcommand tables are hashtables
  with `instant_rehashing = 1` (`src/server.c:735`), so a lookup never takes
  a rehash step; the tables change only at startup and on `MODULE LOAD`
  (the latter is a race shared with the C IO threads, see the table above).
- ACL: only snapshot copies are read. `ACLCheckAllUserCommandPerm()` uses
  stack-local key caches; `get_dbid_args` and native getkeys procs are pure
  functions of argv and allocate with `zmalloc` (thread safe).
- Cluster slot: `clusterSlotByCommand()` is what the C IO threads already
  run; ownership is not read off-thread.

## Known gaps

- At most 64 users are copied; more, users with roles, and module users fall
  back (`acl_user`). Every ACL mutation rebuilds the whole snapshot.
- The token carries the low 32 bits of the epoch and 24 bits of the session
  generation. A false match needs exactly 2^32 ACL mutations (or 2^24 session
  changes on one connection) while one command waits in the queue.
- `INFO rust_io` counters are not reset by `CONFIG RESETSTAT`.
- Replication over the Rust port still does not work (stage 1 gap; the
  dual-channel replica test in unit/auth fails in io mode too).

## What I would change in the shared handoff (not done: sibling stage)

- Publish the session from `beforeNextClient()` only when it changed, and
  carry `consumed` in an atomic instead of the session mutex.
- Let the IO thread hold unparsed input for any connection with a pending
  barrier, the way `parse_held` does for AUTH, instead of prechecking and
  discarding. That turns `barrier` fallbacks into fast paths one handoff
  later, at the cost of a round trip per barrier.
- Batch IO-thread kicks at the end of `beforeSleep()` (stage 1 note); that is
  still where the main thread's time goes at `-P 1` (see the stage report).
