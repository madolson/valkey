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
| `rust-io-mode` | `off` (default), `io`, `precheck`, `exec` | `precheck` logs a warning and runs as `io`. `exec`: see Stage 3. |
| `rust-io-exec-model` | `handoff` (default), `rtc` | exec mode only. |
| `rust-io-port` | 0..65535, default 0 | Extra port served by Rust. Ignored when takeover is on. |
| `rust-io-threads` | 1..128, default 1 | Rust IO threads, each with its own epoll loop. |
| `rust-io-takeover` | yes/no, default no | Rust binds `port`; the C TCP listener is not created. |

All five are startup-only (`IMMUTABLE_CONFIG`). Rust binds the first `bind`
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

Where: `rustIoPull()` in `src/rust_io.c`, marked `STAGE 2 HOOK`. Today the
main thread runs `prepareCommandGeneric()` there and the rest of the checks
inside `processCommand()`. In precheck mode the IO thread would run them
right after parsing, before step 1 of the handoff, and set `p->cmd`,
`p->slot` and a "rejected with this reply" outcome per command.

Engine state the IO thread would need published (read-mostly, versioned or
RCU-style): the command table (`server.commands`, renames), per-client
`authenticated` flag and ACL user (selectors and key/channel patterns),
`server.cluster_enabled` and the slot-to-node map with migration state
(`ASKING`, importing/migrating), `server.loading`, OOM state
(`maxmemory`/`used_memory` and the deny-oom decision), replica read-only and
`replica-serve-stale-data` state, `CLIENT PAUSE` state, `MULTI` state of the
client (queueing changes which checks apply), and `protected-mode`. Today
only `authRequired(c)` is published (`vio_conn_set_auth_required`).

### Stage 3 ("exec")

Where: the end of `src/rust_io.c`, marked `STAGE 3 HOOK`. Rust would call
an exported C entry point equivalent to `call()` (or `cmd->proc`) itself
instead of queuing for `processInputBuffer()`. It must hold the module GIL
(`moduleAcquireGIL()`/`moduleReleaseGIL()`, `src/module.c`) which the main
thread already releases around `aeApiPoll()` in `beforeSleep()`/`afterSleep()`
(`src/server.c`), so engine state is only touched by one thread at a time.
`server.current_client`, `server.in_exec`, propagation and keyspace
notification paths assume the main thread and would need auditing.

# Stage 3: cut point 3, "exec"

`rust-io-mode exec` makes the IO layer own the top-level `call()` for
clients on Rust connections. Two models:

- **handoff** (`rust-io-exec-model handoff`): everything stays where io
  mode puts it (main thread, stage 1 drain hook, `processInputBuffer()`),
  but the final `call(c, CMD_CALL_FULL)` in `processCommand()`
  (`src/server.c:5060`) becomes `vio_exec_call()`, a Rust port of
  `call()`'s control flow (`src/exec.rs:148`).
- **rtc** (`rust-io-exec-model rtc`, run to completion): the Rust IO thread
  that parsed a poll iteration's commands executes them itself, under the
  module GIL, using the same `vio_exec_call()`. No hop to the main thread.

The split point for "the C checks as an engine-provided function" is the
same line: `processCommand()` runs every check unchanged and only the final
`call()` is replaced. A stage 2 precheck plugs in before it without touching
exec.

## The Rust call()

`vio_exec_call()` keeps call()'s order exactly and owns: the forkless-save
gate decision, the call frame (`VioCallFrame`: prev executing client,
real_cmd, dirty and repl-offset snapshots, saved propagation flags,
update_command_stats, reprocessing), the wall-clock `call_timer` and the
monotonic duration (Rust `clock_gettime`, C uses `ustime()`/`getMonotonicUs()`),
calling `cmd->proc` through the pointer, every "blocked / update stats /
reprocessing" condition, and the full propagation flag computation
(`PROPAGATE_AOF|REPL` from dirty, force and prevent bits and the call
flags). Everything that reads or writes `client`, `server` or a
`serverCommand` is a C helper, because Rust has no view of those structs.

Duplication, measured on this branch: C `call()` is 279 lines
(`src/server.c:4185-4463`), 149 without comments and blanks. The Rust port
is 96 lines (`src/exec.rs:148-243`), 86 without comments, plus 190
non-comment lines of C helpers in `src/rust_io_exec.c` that are copies of
call() fragments (field accesses, the latency block, the debug-argv check,
tracking condition). So the whole of call() exists twice: ~86 lines of
control flow in Rust and ~190 lines of re-expressed C. Nothing could be
moved into Rust that touches engine state; what moved is control flow and
arithmetic.

What could not move at all: nested `call()` from EXEC (`execCommand`),
scripts (`scriptCall`), `RM_Call` and `processPendingCommandAndInputBuffer`
re-execution still use the C `call()`. Divergence risk: every future change
to `call()` (for example #3381's two new hooks) must be made in three places
(C call(), exec.rs, a new helper), and a missed one only shows up for
top-level commands on Rust connections. Commandstats, slowlog, MONITOR and
propagation tests pass in both models (unit/info-command, unit/info,
unit/multi propagation tests), which is the only guard. A real
implementation would need either a single call() with the side effects
behind a table both sides use, or call() moved wholesale.

### C symbols the exec modes call

`VioExecOps` (`include/valkey_io.h`, built in `rustIoExecInit()`,
`src/rust_io_exec.c`), in the order `vio_exec_call()` uses them:

| Op | C | Purpose |
|---|---|---|
| `bg_block` | `exBgBlock` -> `bgIteration_blockClientIfRequired()` | Forkless save gate, first line of call(). |
| `enter` | `exEnter` | `in_call++`, `executing_client` swap, frame snapshot, clear force/prevent flags and redaction bitmap. |
| `stats_err_reset` | `exStatsErrReset` -> `incrCommandStatsOnError(NULL, 0)` | Reset the error-stat snapshot. |
| `enter_execution_unit` | `enterExecutionUnit()` | Nesting + cached time. |
| `begin_exec` | `exBeginExec` | `executing_command=1`, clear `buffered_reply`, `keyspace_notified`. |
| `proc_of` | `exProcOf` | `c->cmd->proc`; Rust calls it. |
| `debug_argv_begin/end` | `exDebugArgv*` | enable-debug-assert check that a borrowed argv is untouched. |
| `exit_execution_unit` | `exitExecutionUnit()` | |
| `after_proc` | `exAfterProc` | Clear `executing_command` unless blocked; returns blocked. |
| `add_duration` | `exAddDuration` | `c->duration += d`. |
| `server_dirty` | `exServerDirty` | `server.dirty`. |
| `count_failure` | `exCountFailure` | `incrCommandStatsOnError(real_cmd, FAILED)` + deferred errors. |
| `fire_result_event` | `exFireResultEvent` | USDT trace + `moduleFireCommandResultEvent()`. |
| `close_after_command` | `exCloseAfterCommand` | close_after_command -> close_after_reply. |
| `latency` | `exLatency` | Latency monitor, trace, event-loop duration samples. |
| `commandlog` | `exCommandlog` -> `commandlogPushCurrentCommand()` | Slowlog / commandlog. |
| `feed_monitors` | `exFeedMonitors` | `replicationFeedMonitors()` unless SKIP_MONITOR/ADMIN. |
| `cmd_stats` | `exCmdStats` | calls, usec, latency histogram, slot CPU. |
| `reset_duration` | `exResetDuration` | |
| `prop_info` | `exPropInfo` | Propagation-relevant flag bits (`VIO_PI_*`). |
| `also_propagate` | `exAlsoPropagate` -> `alsoPropagate()` | AOF/replication. |
| `restore_flags` | `exRestoreFlags` | Restore force/prevent flags (re-entrancy). |
| `track_keys` | `exTrackKeys` | `trackingRememberKeys()` for read-only commands. |
| `count_command` | `exCountCommand` | `commands_processed`, `stat_numcommands`. |
| `peak_memory` | `exPeakMemory` | `stat_peak_memory`. |
| `after_command` | `exAfterCommand` -> `afterCommand()` | Post-execution unit ops, propagatePendingCommands, tracking, push messages. |
| `set_woff` | `exSetWoff` | `c->woff`. |
| `leave` | `exLeave` | Pause-after-transaction, restore `executing_client`, `in_call--`. |

29 ops: `enterExecutionUnit()`/`exitExecutionUnit()` are passed directly; the other 27 are new static functions in `rust_io_exec.c` (four of them, `bg_block`, `stats_err_reset`, `commandlog`, `after_command`, are one-line wrappers that exist only to fix the signature).

`VioRtcOps` (rtc only, called on IO threads):

| Op | C | Purpose |
|---|---|---|
| `try_lock` | `rtcTryLock` -> `moduleTryAcquireGIL()` | Uncontended fast path (counted). |
| `lock` | `rtcLock` -> `moduleTimedAcquireGIL(1 ms)` loop | Blocking acquire; returns 1 if the holder is yielding in a long command (see below). Both lock paths then run `rtcTakeEngine()`: main-thread identity and `updateCachedTime(0)`. |
| `unlock` | `rtcUnlock` -> `moduleReleaseGIL()` | Restores `server.main_thread_id` first. |
| `exec_ready` | `rtcExecReady` -> `rustIoHandleReady()` | The stage 1 per-connection engine work (adopt, pull, `processInputBuffer()`, close). |
| `after_batch` | `rtcAfterBatch` | `flushAppendOnlyFile(0)`, `handleClientsWithPendingWrites()`, wake main if it has `beforeSleep()` work. |

Other engine entry points: `rustIoRtcShouldDefer()` (called from
`rustIoDeliver()`, `src/rust_io.c:194`), `rustIoGenInfo()` (`INFO rustio`).

New Rust exports: `vio_exec_configure`, `vio_exec_call`,
`vio_conn_defer_to_main`, `vio_wake_main`, `vio_rtc_stats` (21 total).

### Changes outside the new files

| File | Change |
|---|---|
| `src/server.c:5060` | `processCommand()`: Rust connections in exec mode use `rustIoExecCall()` instead of `call()`. |
| `src/server.c:129,2156,2175` | `engineLockShared()`: the main thread releases/acquires the GIL around poll when modules are loaded *or* rtc is active. |
| `src/server.c:6970` | `INFO rustio` section. |
| `src/module.c:9588` | `moduleTimedAcquireGIL()` (the GIL mutex is static). |
| `src/rust_io.c` | `rustIoHandleReady()` non-static; exec init; the defer check in `rustIoDeliver()`. |
| `src/rust/valkey-io/src/lib.rs` | `kick()` short-circuits to a thread-local list for the IO thread's own connections; `Worker::rtc_cycle()`; two exports. |
| `src/config.c`, `src/Makefile`, `src/rust_io.h` | The config and the object file. |

## exec-rtc

### Control flow

```
IO thread poll iteration:
  epoll events -> parse -> to_notify (as io mode)
  rtc_cycle():
    t0; try_lock() || lock(); t1          <- wait = t1 - t0
    for conn in to_notify: clear in_ready; exec_ready(conn)
         (adopt / pull into cmd_queue / processInputBuffer -> processCommand
          -> vio_exec_call -> cmd->proc, all on this thread)
    after_batch(): AOF write, write replies of every pending client
         (Rust conns: memcpy into Conn.out; kick is local for own conns)
    t2; unlock()                           <- hold = t2 - t1
    run local kicks: write(2) the replies, no eventfd
    repeat while flushes produced new events (max 16, then poll with 0 timeout)
main thread: unchanged event loop; holds the GIL except around aeApiPoll.
```

One lock acquisition per poll iteration per IO thread, covering every
connection that had input in that iteration. `in_ready` is cleared before
the connection's events and commands are read, the same rule as
`vio_main_next_ready()`, so a racing producer re-queues instead of being lost.

### Engine invariants that assume the main thread, and what happened

| Invariant | Where | Hit? | Handling |
|---|---|---|---|
| Main thread holds the GIL from startup | `moduleInitModulesSystem()`, `src/module.c:13576` | Yes: a second `moduleAcquireGIL()` in exec init deadlocked startup | Rely on it: IO threads that start before load finishes wait on the lock. |
| GIL released around poll only with modules | `beforeSleep()`/`afterSleep()`, `src/server.c:2156,2175` | By design | `engineLockShared()`. |
| `thread_shared_qb` is a per-thread buffer, NULL on new threads | `src/networking.c:164`, `trimClientQueryBuffer()` `:2592` | Yes: SIGSEGV in `resetSharedQueryBuf()` (`c->querybuf == thread_shared_qb` is `NULL == NULL`) | Give each Rust thread a private empty sds (`rtcExecReady`). Not `initSharedQueryBuf()`: it registers under `getCurTid()`, which is 0 (the main thread's slot) on a Rust thread. |
| `inMainThread()` = io-thread id 0 | `src/io_threads.c:98` | Silent: true on Rust threads (`_Thread_local thread_id` defaults to 0), so `serverAssert(inMainThread())` (`networking.c:3616` etc.) and the main-thread branches in the reply path (`networking.c:2792,3213,3317`) take the main-thread side, which is correct only because the lock is held. | None needed; the asserts prove nothing in this mode. |
| `onServerMainThread()` = `pthread_self() == server.main_thread_id` | `src/server.h:3009`; asserts in `forkless.c` (8), `bgiteration.c:2448`, `scripting_engine.c:311` | Yes: `forkless.c:309 'onServerMainThread()' is not true` on BGSAVE (unit/introspection). Also `scriptingEngineCallFreeFunction()` took the async path and self-deadlocked on the GIL in FUNCTION LOAD (unit/info, eu-stack in `functionLoadCommand`). | The lock holder sets `server.main_thread_id = pthread_self()` and restores the real main id before unlocking (`rtcLock`/`rtcUnlock`). "Main thread" now means "GIL holder". Racy unlocked reads of `main_thread_id` by forkless/bio threads (`forkless.c:69,177`) still see a non-matching id. |
| Long commands yield via `processEventsWhileBlocked()` on the executing thread | `scriptInterrupt`, module yield | Yes: "MULTI and script timeout" hung. The script ran on IO thread 1, the test's SCRIPT KILL connection lived on the same IO thread, which cannot poll its sockets while running Lua; other IO threads sat in `pthread_mutex_lock` | Two fixes. (1) `rustIoRtcShouldDefer()`: a connection whose queue holds EVAL*/FCALL*/DEBUG/module commands, or EXEC of a MULTI containing one, is handed to the main thread (`vio_conn_defer_to_main`, the stage 1 ready list). (2) Waiters use a 1 ms timed lock and, if `isInsideYieldingLongCommand()`, route their batch through the ready list, which the holder's `processEventsWhileBlocked()` drains, so they get `-BUSY` and SCRIPT KILL works. |
| `processEventsWhileBlocked()` on a non-main thread | `src/networking.c`, `ae.c` | Avoided by the deferral above. If a deferred-set gap lets one run on an IO thread, it polls `server.el` concurrently with the main thread's `aeApiPoll` and both write `el->fired`. | Not fixed; the deferral list is the mitigation. |
| Loading: `-LOADING` replies come from `processEventsWhileBlocked()` | `loadDataFromDisk()` | Yes: "config during loading", "MEMORY commands during loading" fail: commands on Rust connections wait for the GIL until loading ends instead of getting `-LOADING` | Not fixed. Would need the same routing as the yield case, keyed on `server.loading`. |
| Cached clock `server.unixtime`/`mstime` refreshed by the main loop | `updateCachedTime()` in `afterSleep()`/`serverCron()`; `call()` refreshes only inside an execution unit | Yes: "CLIENT KILL maxAGE will kill old clients" killed 2 instead of 1: a connection adopted on an IO thread after the main thread's DEBUG SLEEP got the pre-sleep `unixtime` as `ctime` | `rtcTakeEngine()` calls `updateCachedTime(0)` on every acquisition. |
| Event-loop metrics count main-loop work | `eventloop_cycles`, `el_cmd_cnt_max` (`server.c` beforeSleep) | Yes: "stats: eventloop metrics", "stats: debug metrics" (unit/info) see 0 cycles with commands | Not fixed: commands no longer run inside the main event loop. |
| `server.current_client`/`executing_client`, `in_call`, `execution_nesting`, propagation, `also_propagate` | `server.c` | No failures | Plain globals, safe under the lock; set and restored inside one batch. |
| Blocking/unblocking | `handleClientsBlockedOnKeys()` (`processCommand` tail), `processUnblockedClients()` (beforeSleep) | No failures (unit/type/list 289/0 with blocked clients on other IO threads) | Unblock-on-key runs on the IO thread inside `processCommand()`; the unblocked client's reply is written in `after_batch`; its remaining pipeline runs from the main thread's `beforeSleep()` (main is woken when `unblocked_clients` is non-empty). |
| AOF write before reply | `beforeSleep()` order | Not tested by the suites run | `after_batch` calls `flushAppendOnlyFile(0)` before `handleClientsWithPendingWrites()`. fsync policy as in beforeSleep. |
| Lazyfree, client eviction, keyspace notifications, MONITOR, pub/sub to C clients | various | No failures (pubsub 38/0, info-command) | Thread-safe or plain globals under the lock. Replies to C-port clients are written from the IO thread in `after_batch` (`writev` and, if needed, `aeCreateFileEvent` on `server.el` while the main thread may be in `epoll_wait`: epoll_ctl is thread safe and ae reads `el->events` only after reacquiring the GIL). |
| `freeClientAsync()`, `clients_to_close` freed in beforeSleep | `networking.c` | No failures | Main woken when `clients_to_close` is non-empty. |
| Forked children (Lua debugger) talk on the client's fd | `ldb` | Yes in all Rust modes: "Forked scripting debug session ..." gets EOF; the child has no Rust IO threads. Transport problem, not exec. | Skipped. |

### Lock statistics

`INFO rustio` (rtc only): `rtc_batches`, `rtc_uncontended` (try_lock
succeeded), `rtc_conns` (connections per batch), wait and hold sums and
log2-ns histograms (bucket i = [2^(i-1), 2^i) ns). `bench.py` diffs them
around each benchmark cell.

## Compatibility with in-flight upstream work

| Hook | Where in C | exec-handoff / Rust call() | exec-rtc |
|---|---|---|---|
| Forkless save `bgIteration_blockClientIfRequired()` | first line of `call()` | Kept as `bg_block`, first op of `vio_exec_call()`. A BLOCKED_INUSE client is re-executed through `processCommand()`, which routes back into Rust. | Same; the forkless machinery asserts `onServerMainThread()`, satisfied by the identity swap. Its propagation hook (`server.c` `alsoPropagate` path) is reached through the `also_propagate` op. |
| AOF durability #3381: `recordReplOffsetBaseline()` next to `executing_client = c`; `computeCommandBlockingOffset()` after `afterCommand()`; stamping in `postExecutionUnitOperations()`; reply byte-offset fences in the C write path | `call()` and `networking.c` | Two more ops (`enter` would call the baseline, a new op after `after_command`). Stamping is inside `afterCommand()`, so it is inherited. Fences live in `_writeToClient()`/`writevToClient()`, which still run (they produce the iovec the Rust `writev` copies), so a fence that stops the write before the fenced offset works unchanged. | Replies are produced and handed to Rust on the IO thread inside `after_batch`. The fence check in the C write path runs there too, under the lock, so a client's bytes past its fence stay in `c->reply`. What is missing is the release: when fsync completes the fenced clients must be re-queued for write; as #3381 is written that would happen on the main thread in `beforeSleep()`, which then copies into Rust and kicks the IO thread. (Design only: #3381 is not in this tree.) Correct but adds a main-thread hop per fsync. A better design moves the fence into Rust: `vio_conn_writev(h, iov, n, fence_offset)` keeps bytes beyond the fence in `Conn.out` with a per-connection fence, and a single atomic "durable offset" published by the fsync path lets IO threads release them without the engine lock. |
| Data tiering #3326: pre-execution gate that may block and re-execute | would sit with the forkless gate | Add as an op before `enter`; re-execution comes back through `processCommand()`. | Same; if the gate does disk IO it must not do it under the engine lock: it should block the client and let a background thread unblock it. |
| BLOCKED_ASYNC (Cluster V2, #3536) and `VALKEYMODULE_REPLY_AGAIN` (#3707) | blocked.c / module.c | Nothing in `call()` itself; the Rust port already treats "blocked after proc" generically (no stats, no duration reset, no commandlog). Re-execution re-enters via `processCommand()`. | Unblock completions arrive on the main thread (module/bio pipes) and run there; module commands are deferred to the main thread by `rustIoRtcShouldDefer()`, so re-block from a module never runs on an IO thread. |

## What a shared-nothing future would additionally need

Target: N engine shards in one process, each owning a slot range, the IO
layer routing each command to its owner.

Carries over from exec-rtc:
- The IO thread as the execution site, batching per poll iteration, and the
  measured lock-hold/batch numbers as the cost model of "one owner at a
  time".
- `rtc_cycle()`'s structure: acquire the owner, run the batch, flush replies
  with local kicks, release. With shards, "acquire" becomes "am I the
  owner of these slots" (or a per-shard lock/queue).
- `vio_conn_defer_to_main()`: the mechanism for "this command must run
  somewhere else" already exists and is what cross-shard routing would use
  (enqueue the connection to the owning shard's ready list + eventfd).
- `prepareCommandGeneric()` already computes `c->slot` before execution, so
  routing by slot has its input.
- The Rust `call()` frame shows what per-call state is really per-call vs
  global.

Blocks it (each is a global in `struct valkeyServer server` or a file
static, touched on every command):
- `server.db` / keyspace, `server.dirty`, `server.stat_*`, `server.commands`
  (per-command `calls`/`microseconds`/histograms mutated in `cmd_stats`),
  `server.errors`.
- Propagation: `server.also_propagate`, `server.execution_nesting`,
  `server.in_call`, `server.executing_client`, `server.current_client`,
  `server.primary_repl_offset`, the single replication backlog and AOF
  buffer. One ordered replication stream cannot be fed by N shards without
  a merge point.
- Blocking: `server.ready_keys`, `server.unblocked_clients`, the blocking
  keys dicts per db; tracking (`TrackingTable`), pub/sub dicts, MONITOR list,
  client list, `clients_pending_write`.
- Lua/function engine: one Lua state, `curr_run_ctx`.
- Module API: assumes one GIL and one main thread.
- Multi-key commands and MULTI across slots owned by different shards need
  cross-shard coordination that cluster mode avoids by rejecting them
  (CROSSSLOT); in one process the IO layer would have to reject the same way.
- `onServerMainThread()` / `inMainThread()` and every `serverAssert` on
  them would have to become "owns shard X".

Minimal first step: make `server` a pointer selected per shard
(thread-local `server_shard`) for the keyspace, dirty/stat counters and
command stats, keep replication/AOF/pubsub/modules on a coordinator thread
reached through the existing ready-list handoff, and route by
`prepareCommandGeneric()`'s slot. The exec-rtc numbers below bound the gain:
the engine lock is the serialization point that sharding removes.

## What I would change in the shared handoff (not done, sibling stage)

- rtc removed per-command main-thread work but kept the main loop running
  `beforeSleep()` for unblocked clients etc. A per-IO-thread "pending
  beforeSleep work" list would let IO threads do it at the end of their batch.
- io/handoff: defer IO-thread kicks to one pass at the end of
  `beforeSleep()` (stage 1 note still stands; rtc's local-kick shortcut
  shows the gain of skipping the eventfd for the common case).

## Measured (details and raw numbers in the stage 3 report)

- The Rust call() costs ~60-70 ns per command on the main thread: at
  `-P 16 -c 200` main-thread CPU per 1M SET is 0.62-0.68 s (io) vs
  0.69-0.74 s (handoff), and handoff peaks at 1.37-1.39M SET/s vs 1.49-1.54M
  for io. Likely the 29 indirect calls per command that LTO cannot inline
  across the function-pointer table (inferred, no perf).
- rtc takes the main thread off the request path entirely (0.0-0.8% CPU vs
  8-97% for io). At `-P 1` with 4-6 IO threads and a client-bound load
  (210k RPS) it serves the same RPS with 19-24% less process CPU (318-362%
  vs 398-457%). Server-bound (8 benchmark threads) it is 5-7% below io
  (279k vs 293k at t4, 400k vs 428k at t6) while the lock is still mostly
  uncontended (61-78%). Where it saturates (`-P 16`, t4/t6) it converges on the
  same single-engine ceiling as io (1.41-1.44M vs 1.49-1.54M SET/s): lock
  busy 93-95%, uncontended acquisitions ~1%, mean wait 0.8-1.2 ms, with
  1.6-3.5 threads waiting on average. The lock hold per SET (0.66 us) is
  about what io's main thread spends per SET including the handoff
  (0.62 us), so what rtc saves in handoff it pays back in lock transfer
  and in engine state moving between cores (inferred).
- rtc with one IO thread loses overlap: parse, execute and write run one
  after another on the same thread, so `-P 16` drops to 499-571k vs
  666-799k for io-t1 (process CPU 74-82%: the thread idles waiting for the
  client).
