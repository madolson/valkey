# valkey-io: a Rust IO layer for valkey-server

Rust owns the client sockets, reads, RESP parsing and writes. Three cut
points move more of the per-command path into the IO layer, and the modes
are cumulative:

| `rust-io-mode` | What runs on the Rust IO thread | What runs on the main thread | Stage |
|---|---|---|---|
| `io` | accept, read, RESP parse, argv build, write | `prepareCommandGeneric()`, all of `processCommand()`, C `call()` | 1 |
| `precheck` | io + lookup/arity/slot, auth, ACL against a snapshot, session checks (token per command) | token validation, residual (class C/D) checks, C `call()` | 2 |
| `exec` + `rust-io-exec-model handoff` | precheck | token validation, residual checks, Rust `call()` (`vio_exec_call`) | 3 + 4a |
| `exec` + `rust-io-exec-model rtc` | precheck + token validation, residual checks and Rust `call()` under the engine lock (module GIL), reply write | yield-capable commands (scripts, functions, DEBUG, module commands), beforeSleep() work | 3 + 4a |

`rust-io-exec-precheck no` is a debug switch: exec without the precheck fast
path, so the contribution of each part can be measured on its own. The
boundary is three C files on the engine side (`src/rust_io.c`,
`src/rust_io_precheck.c`, `src/rust_io_exec.c`), one hand-written header for
the Rust exports (`include/valkey_io.h`), and the crate. "FFI surface per
mode" below has the counts.

Sections: build and configuration; the stage 1 design (threading,
handoff, FFI, costs); "Composed modes" and "Handoff v2" (stage 4a); "Stage 2:
precheck"; "Stage 3: exec".

## Build and configuration

`make BUILD_RUST_IO=yes` runs `cargo build --release` in this directory, links
`target/release/libvalkey_io.a` and compiles the engine with `-DUSE_RUST_IO`.
The default `make` does not build or link the crate and compiles none of the
bridge (everything is behind `USE_RUST_IO`).

| Config | Values | Meaning |
|---|---|---|
| `rust-io-mode` | `off` (default), `io`, `precheck`, `exec` | Cumulative, see the table above. |
| `rust-io-exec-model` | `handoff` (default), `rtc` | exec mode only. |
| `rust-io-exec-precheck` | `yes` (default), `no` | exec mode only. `no` runs exec without the precheck fast path. |
| `rust-io-port` | 0..65535, default 0 | Extra port served by Rust. Ignored when takeover is on. |
| `rust-io-threads` | 1..128, default 1 | Rust IO threads, each with its own epoll loop. |
| `rust-io-takeover` | yes/no, default no | Rust binds `port`; the C TCP listener is not created. |

All are startup-only (`IMMUTABLE_CONFIG`). Rust binds the first `bind`
address (`*` maps to 0.0.0.0, `-::*` to `::`).

Environment variable `VIO_HANDOFF_DISABLE` (read once at startup) turns off
parts of "Handoff v2" for measurement: bit 1 kick batching, bit 2 the
IO-thread awake check, bit 4 the main-thread awake check. 7 is the stage 1
handoff protocol.

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

Stage 2, 3 and 4a additions are listed in their sections; "FFI surface
per mode" has the per-mode counts.

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
batch size. Stage 4a's "Handoff v2" changes this.

## Known gaps

- `CLIENT LIST` `qbuf`/`qbuf-free` are always 0: buffered input lives in Rust.
- Rust read and write buffers use the system allocator and are not in
  `used_memory` or client eviction accounting. Parsed-but-unexecuted
  commands are bounded instead (see Backpressure).
- `CONFIG SET bind` / `port` under takeover: Rust cannot re-bind. Setting
  the startup value succeeds; any change fails with `Changing bind or port
  is not supported with rust-io-takeover` (stage 4a; `updatePort()` and
  `applyBind()` in `config.c`, `rustIoListenerUnchanged()`).
- Only TCP. No TLS, no Unix socket, no outbound connections (replication and
  cluster bus stay on C sockets). A replica connecting to the Rust port is
  untested; `SYNC` writes go through the same copy path.
- `auth_required` is published after each batch, so the parser may apply the
  pre-AUTH limits to commands pipelined behind `AUTH` in the same read.
  The C parser stops at `AUTH` for the same reason.
- `CLIENT KILL SKIPME no` during a BGSAVE with `rdb-key-save-delay` can
  close the caller without its `:N` reply (5 of 5 in
  `tools/killself-bgsave.py`, merge and final binaries; 0 of 5 over the C
  transport). Not root-caused (stage 4a). It makes "CLIENT KILL close the
  client connection during bgsave" in unit/introspection flaky.
- `INFO rust_io` (precheck counters) and `INFO rustio` (exec model, rtc
  lock statistics) are explicit sections only, not reset by `CONFIG RESETSTAT`.


# Stage 4a: composed modes and handoff v2

## Composed modes

exec mode turns on the precheck fast path unless `rust-io-exec-precheck no`
(`rust_io_precheck_active`, set in `rustIoInitListeners()`, `src/rust_io.c`).
Every precheck check that looked at `rust_io_config.mode == PRECHECK` now
looks at that flag: callback registration, `rustIoPublishSession()`,
`rustPrecheckPublish()`. `INFO rust_io` reports `rust_io_mode:exec` or
`exec-noprecheck`.

The two stages compose without code in the middle: precheck ends in
`processCommand()` jumping over the checked block, exec starts at the final
`call()` of the same function. In handoff, everything after parsing runs on
the main thread as in precheck mode, then the Rust `call()`.

### Where each precheck piece runs in exec-rtc

| Piece | Thread | Lock | Reads / writes |
|---|---|---|---|
| `rustPrecheckBegin/End` (hazard pin) and `rustPrecheckCmd` | IO thread, in `parse_buffered()` | none (as in precheck mode) | pinned snapshot, session copy, command table, keyspecs, private argv |
| `rustPrecheckValidate()` and the residual checks | IO thread, inside `processCommand()` | engine lock | `acl_mutation_epoch`, the published session, client fields, live cluster slot state, `pc_stats` |
| `rustIoPublishSession()` | IO thread, end of `rustIoDeliver()` | engine lock | `rustConnection.sess` (read only under the lock), the Rust session mutex |
| `rustPrecheckPublish()` (build, swap, reclaim) | main thread in `beforeSleep()`, and the IO thread in `rtcAfterBatch()` | engine lock | `Users`, `pc_current`, `pc_retired`, `pc_hazard[]`, `acl_mutation_epoch` (saved and restored) |

The snapshot reclaim is the one place that assumed a thread. It does not
depend on which thread reclaims, only that one reclaimer runs at a time and
that it scans every hazard slot after the swap. Both callers hold the engine
lock, so they are serialized with each other and with every ACL mutation.
The C11 atomics on `pc_current` and `pc_hazard` are sequentially consistent
(`atomic_load`/`atomic_store` without an order), so the reader's
load-store-reload in `rustPrecheckBegin()` pairs with the reclaimer's
swap-then-scan regardless of thread. An IO thread that reclaims has already
cleared its own hazard slot (the parse batch ends before `rtc_cycle()`), and
if it had not, the scan would skip that snapshot.

Why `rtcAfterBatch()` publishes: in rtc an `ACL SETUSER` usually runs on an
IO thread, and `rtcAfterBatch()` does not wake the main thread for it. Until
the next `beforeSleep()` (up to one `hz` tick on an idle server) every
command would fall back with `epoch`. Correct, but slow. Publishing at the
end of the batch costs one epoch compare when nothing changed. The default
user's `auth_required` is republished in the same call
(`rustIoPublishGlobals()`).

Module commands are prepared by `prepareCommandGeneric()` in `rustIoPull()`
(`READ_FLAGS_RUST_PREPARE_ON_MAIN`). In rtc that runs on the IO thread with
the engine lock held, which is what valkey#5054 needs: getkeys never runs
concurrently with module code.

## SCRIPT KILL hang (stage 1 bug, fixed here)

Cause: `rustIoEventHandler()` takes the whole ready list into the main
thread's `MAIN_READY` and handles connections one by one. When one of them
runs a script past `busy-reply-threshold`, `processEventsWhileBlocked()`
polls the event loop, but the connections still in `MAIN_READY` were
announced by an eventfd write that is already consumed. Nothing polls them.
A client in the same batch as the script (the test's `r` client, whose PING
is parsed while the script starts) never gets `-BUSY`, so it never sends
`SCRIPT KILL`. Intermittent because it needs both connections in one batch.

Fix: `connRustProcessPendingData()`, which `beforeSleep()` calls inside
`processEventsWhileBlocked()`, drains `MAIN_READY` (`rustIoDrainReady()`),
and `connRustHasPendingData()` reports it (`vio_main_has_ready()`). Outside
a long command the list is always empty there. Regression check:
`scriptkill.py` (300 iterations of script, BUSY, SCRIPT KILL, PONG); the
binary without the fix hung at iteration 41 and 59 in two runs.

## Handoff v2

The stage 1 handoff paid fixed costs per main-loop iteration and per IO
thread: an eventfd `read(2)` on the main thread per wakeup, and an eventfd
`write(2)` per IO thread kicked, each a few microseconds on this KVM host.
With 4+ IO threads at `-P 1` the batches are small, so these dominated.

### 1. Main-thread kicks batched

`kick()` on the main thread (`IS_MAIN`, set in `vio_start()`) appends to a
per-IO-thread list instead of the thread's inbox. The list is handed over
with one inbox lock and at most one eventfd write when it reaches
`KICK_BATCH` (32) entries, and at the end of `beforeSleep()`
(`rustIoBeforePoll()` -> `vio_main_flush_kicks()`), also in the
`processEventsWhileBlocked()` branch, so `-BUSY` replies still leave during
a script. The 32 cap exists because deferring everything to the end of
`beforeSleep()` lost overlap with a single IO thread: the IO thread waited
for the main thread to copy all replies before writing any.

IO threads (exec-rtc) still kick directly; own-thread kicks stay local.

### 2. No eventfd write to an awake IO thread (implemented, off by default)

Each IO thread has `sleeping` and `pending` flags. Before `epoll_wait` with
a non-zero timeout it stores `sleeping = true` and then loads `pending`; if
set, it polls with timeout 0. After the poll it clears `sleeping`. A
producer pushes to the inbox, stores `pending = true`, then loads `sleeping`
and writes the eventfd only if it is set (and `signaled` was clear). All
SeqCst: Dekker's pattern, so either the producer sees `sleeping` and
signals, or the thread sees `pending` and does not block. The inbox is
taken through `pending.swap(false)` at `TOK_WAKE` (after the stage 1
drain-then-clear), and at the end of an iteration only when no eventfd
write is pending (`take_inbox()`, `lib.rs`).

It is off by default (`VIO_HANDOFF_DISABLE` defaults to 2). Measured (see
"Measured" below): on top of the default it saves the main thread 11-16%
CPU per request at `-P 1` with 4-6 IO threads, but with one IO thread at
`-P 16` GET drops from 959k to 685k RPS and SET from 958k to 630k. The
voluntary context switches of the IO thread over 3 s go from 4 to 832: an
IO thread that takes its kicks as soon as they appear drains them, finds
nothing else, sleeps, and is woken again for the next reply. With the
eventfd always written, a kick that arrives during an iteration leaves the
eventfd readable, the next `epoll_wait` returns immediately, and by then
the clients' next requests are there. Two placements of the early take
were tried (end of iteration, right after `epoll_wait`); both lost the same
way. A bounded spin before sleeping would address it but costs IO-thread
CPU, and was not tried for time.

### 3. No eventfd write to an awake main thread

Same pattern on the main thread with `main_sleeping`/`main_pending`.
`vio_main_before_poll()` at the end of `beforeSleep()` stores
`main_sleeping = true` and returns `main_pending`; if set, `beforeSleep()`
sets `dont_sleep` so the poll does not block. `vio_main_after_poll()` at the
end of `afterSleep()` clears it and, if no eventfd write is pending, takes
the work handed over while the main thread was awake into the current
batch; `rustIoAfterPoll()` runs it right there, which is where the eventfd
handler would have run. When an eventfd write is pending the handler takes
it in the same iteration. Both rules matter: the first version took the
work in `connRustProcessPendingData()` at the start of `beforeSleep()`, and
the second took it in `afterSleep()` in addition to the handler. Each ran a
second batch before the first batch's replies were written, and a single
IO thread waited for those.

`vio_wake_main()` (rtc: beforeSleep() work) and `vio_conn_defer_to_main()`
use the same path, so a wake request made while the main thread is awake
is not lost: `main_pending` keeps the next poll non-blocking. Inside
`processEventsWhileBlocked()` the nested `afterSleep()` takes the work too.

### Not done

- Session publish only when changed, `consumed` in an atomic: one
  uncontended mutex lock per delivered connection, precheck and exec only.
  Not measured separately; skipped for time in favor of 1-3, which act on
  syscalls.
- A bounded spin before sleeping. On the main thread it would raise exactly
  the metric this work reduces. On IO threads it is the likely fix for 2;
  not tried for time.
- rtc per-IO-thread pending list for `beforeSleep()` work: rtc main-thread
  CPU was already 0.0-0.8% (stage 3), so there is nothing to win on the
  primary metric.

### Measured

Server on cores 0-7, valkey-benchmark on 8-15 (`--threads 8`), `-c 200`,
`-d 64 -r 1000000`, io mode, 3 reps, median, runs of 5.2-10 s. "s/1M" is
main-thread CPU seconds per million requests. One binary (the final tree),
parts switched off with `VIO_HANDOFF_DISABLE`: 7 = stage 1 protocol, 6 =
kick batching only, default (2) = batching + main awake check, 0 = all
three. Raw rows: `bench.csv` (variants `final-d7`, `final-d6`, `final`,
`final-d0`).

| threads | -P | test | s/1M 7 / 6 / default / 0 | RPS 7 / 6 / default / 0 |
|---|---|---|---|---|
| 1 | 1 | GET | 1.180 / 1.140 / 1.180 / 1.080 | 71.3k / 71.3k / 71.3k / 68.9k |
| 1 | 16 | GET | 0.468 / 0.463 / 0.487 / 0.472 | 999k / 959k / 959k / 685k |
| 1 | 16 | SET | 0.608 / 0.607 / 0.607 / 0.602 | 959k / 959k / 958k / 630k |
| 4 | 1 | GET | 1.393 / 1.347 / 1.373 / 1.180 | 286k / 286k / 286k / 273k |
| 4 | 1 | SET | 1.553 / 1.507 / 1.500 / 1.333 | 286k / 286k / 286k / 273k |
| 6 | 1 | GET | 1.673 / 1.587 / 1.540 / 1.300 | 400k / 400k / 400k / 400k |
| 6 | 1 | SET | 1.827 / 1.733 / 1.687 / 1.427 | 375k / 400k / 400k / 400k |
| 4 | 16 | GET | 0.493 / 0.477 / 0.483 / 0.469 | 1.92M / 2.00M / 1.92M / 2.00M |
| 6 | 16 | SET | 0.636 / 0.638 / 0.621 / 0.616 | 1.50M / 1.50M / 1.55M / 1.55M |

Reading it: the default cuts main-thread time per request at `-P 1` by 3%
(4 threads, SET) to 8% (6 threads), and t6 `-P 1` p99 from 0.97/1.11 ms to
0.62/0.65 ms (GET/SET), with no throughput loss. The IO-thread awake check
(0) is the larger lever on CPU, -11% to -16% against the default at `-P 1`
with 4-6 threads, and costs 29-34% throughput with one IO thread at
`-P 16` and 5% at 4 threads `-P 1`. `-P 16` RPS at 4-6 threads moves in 4% steps (12M
requests in 6.0 vs 6.26 s), within noise.

With 8 benchmark threads the stage 1 figure (4.0 s/1M at `-P 1`, 4 IO
threads) does not appear: the baseline is 1.4-1.8 s/1M. Stages 1-3 used 4
benchmark threads, client-bound at 210-222k RPS. Rerun that way (4 IO
threads, `-P 1`, GET/SET): 3.99/4.04 s/1M for the merge binary, 3.81/3.87
with the default, 3.13/3.21 with all three parts on, all at 222k RPS.

## Edits to shared files in stage 4a

All guarded by `USE_RUST_IO`.

| File | Change | Why |
|---|---|---|
| `src/server.c` | `rustIoBeforePoll(1)` before `aeSetDontWait()` in `beforeSleep()`, `rustIoBeforePoll(0)` in its `processEventsWhileBlocked()` branch, `rustIoAfterPoll()` at the end of `afterSleep()` | Handoff v2: kick flush, main awake flag, take work handed over while awake. |
| `src/config.c` | `rust-io-exec-precheck`; `updatePort()`/`applyBind()` refuse changes under takeover | Composed modes; task 4. |
| `src/rust_io_exec.c` | `rtcAfterBatch()` calls `rustIoPublishGlobals()` | Publish the ACL snapshot after an IO thread mutated ACLs. |
| `src/rust_io_precheck.c` | gate on `rust_io_precheck_active` instead of the mode | Composed modes. |

## FFI surface per mode

Rust exports are `#[no_mangle]` functions called by C. C callbacks are
function pointers Rust calls (`VioCallbacks`, `VioRtcOps`). Engine helpers
are the `VioExecOps` table the Rust `call()` drives. Counted on the final
tree.

| Mode | Rust exports | C callbacks | Engine helpers (`VioExecOps`) |
|---|---|---|---|
| io | 22 | 2 (`create_argv`, `free_argv`) | 0 |
| precheck | 23 (+ `vio_conn_set_session`) | 5 (+ `precheck_begin/end/cmd`) | 0 |
| exec handoff | 25 (+ `vio_exec_configure`, `vio_exec_call`) | 5 | 29 |
| exec rtc | 28 (+ `vio_rtc_stats`, `vio_wake_main`, `vio_conn_defer_to_main`) | 10 (+ 5 `VioRtcOps`) | 29 |
| exec handoff, `rust-io-exec-precheck no` | 24 | 2 | 29 |
| exec rtc, `rust-io-exec-precheck no` | 27 | 7 | 29 |

The 22 io-mode exports are stage 1's 16, `vio_set_default_auth_required`
(stage 2's first-read fix, used in every mode), and five added here:
`vio_main_has_ready` (SCRIPT KILL fix), `vio_main_flush_kicks`,
`vio_main_take_ready`, `vio_main_before_poll`, `vio_main_after_poll`
(handoff v2). By stage: 16 (stage 1) + 2 (stage 2) + 5 (stage 3) + 5
(stage 4a) = 28 `#[no_mangle]` functions in the crate. The tables in the
stage 1, 2 and 3 sections below list each symbol's purpose.

# Stage 5a: hardening

## beforeSleep() under rtc

In exec-rtc an IO thread runs a batch under the engine lock and calls
`rtcAfterBatch()` (`src/rust_io_exec.c`) before it unlocks. The main thread
keeps running its own `beforeSleep()` whenever it wakes: on its timers
(`serverCron` every 1000/hz ms), on its own sockets (cluster bus,
replication, C listeners), and when `rtcAfterBatch()` calls
`vio_wake_main()`. Each `beforeSleep()` item (`src/server.c`, line numbers
at stage 5a) is covered one of three ways: **batch** (run in
`rtcAfterBatch()` under the lock), **wake** (`rtcAfterBatch()` wakes the
main thread when it is pending), or **main** (left to the next main-thread
iteration, with the reason).

| server.c | item | rtc coverage |
|---|---|---|
| 1958 | `trySendPollJobToIOThreads()` | main. C IO threads only; Rust connections never use them. |
| 1961 | `stat_peak_memory` | batch, per command (`exPeakMemory`). |
| 1968-1988 | `ProcessingEventsWhileBlocked` subset | main. A yielding command runs on the main thread (`rustIoRtcShouldDefer`), and an IO thread hands its batch to the yielding holder (`rtcLock` returns 1). |
| 1992, 2099 | `processIOThreadsResponses()` | main. C IO threads only. |
| 1996 | `connTypeProcessPendingData()` (Rust stalled clients, TLS) | wake (`connTypeHasPendingData()`). |
| 2001 | `processPendingReplStreamDecode()` | main. Primary link of a replica, which is a C connection. |
| 2010 | `clusterBeforeSleep()` | `CLUSTER_TODO_UPDATE_STATE`: batch (`clusterUpdateState()`), so the next command sees the new `cluster_state` (unit/cluster/misc "Cluster state transitions when toggling cluster-require-full-coverage"). Other todo flags (failover, config save, pong broadcast, slot migration): wake. They use the cluster bus and stay on the main thread. |
| 2013 | `bgIteration_beforeSleep()` | wake while `bgIteration_iterationActive()`. |
| 2017 | `blockedBeforeSleep()` | wake: `unblocked_clients`, `ready_keys`, `postponed_clients`, `clients_waiting_acks` (WAIT/WAITAOF). Timeouts of blocked clients: main, the same timer-driven latency as the C build. |
| 2028 | `activeExpireCycle(FAST)` | main. Not needed for correctness; the slow cycle in `serverCron` and lazy expiry still run. |
| 2032 | module `EVENTLOOP_BEFORE_SLEEP` event | main. Fires once per main-thread iteration, not per IO-thread batch. Known gap for modules that rely on it per batch. |
| 2045 | `sendGetackToReplicas()` | wake (`get_ack_from_replicas`). |
| 2052 | `updateFailoverStatus()` | wake while `failover_state == FAILOVER_WAIT_FOR_SYNC`. |
| 2057-2058 | tracking asserts | batch: invalidations flush per command, as on the main thread. |
| 2062 | `trackingBroadcastInvalidationMessages()` | batch, before the batch's replies are written. BCAST latency went from 36-100 ms to 0 ms (`bcast-latency.py`). |
| 2074 | `flushAppendOnlyFile(0)` | batch, before the writes. |
| 2086 | `fsynced_reploff` update | wake via `clients_waiting_acks`, for WAITAOF. |
| 2094 | `handleClientsWithPendingWrites()` | batch. |
| 2111 | `freeClientsInAsyncFreeQueue()` | wake (`clients_to_close`). |
| 2115 | `incrementalTrimReplicationBacklog()` | main. The backlog trims at the next main-thread iteration (at most one cron tick). |
| 2118 | `evictClients()` | main. Rust-held buffers are not in client memory anyway (MISSING-FEATURE). |
| 2127-2147 | event-loop metrics | main. Event-loop metrics in rtc are a known MISSING-FEATURE. |
| 2153 | `rustIoBeforePoll(1)` | main. It belongs to the main thread's poll. |
| 2157 | `IOThreadsBeforeSleep()` | main. C IO threads. |
| 2164 | GIL release | n/a. The IO thread unlocks in `rtcUnlock()`. |

`rtcTakeEngine()` also sets `cmd_time_snapshot` as `afterSleep()` does.
`processCommand()` checks expiry (`getNodeByQuery()`) before `call()` takes
its own snapshot, so an IO thread used the previous command's time.

## Admission control

The C listener throttles itself: accept runs on the busy main thread, and
`acceptCommonHandler()` refuses clients at maxclients. Rust IO threads
accept and read no matter how far behind the main thread is. Three bounds
(`src/lib.rs`, constants next to `CMDS_HIGH`):

- **Unadopted connections.** `UNADOPTED` counts accepted connections whose
  `VIO_EV_NEW` the engine has not taken (`vio_conn_take_events`). At
  `NEW_HIGH` (128), thread 0 removes the listener from its epoll set
  (`accept_backoff`). Taking a `VIO_EV_NEW` below `NEW_LOW` (64) sends
  `Msg::ResumeAccept` to thread 0. The kernel backlog holds the rest, as
  for the C listener. The pause and resume race is closed by re-checking
  after setting `ACCEPT_PAUSED` (both SeqCst).
- **Open connections.** `LIVE` counts Rust connections from accept until
  the fd closes, including closed connections still draining output. At
  maxclients + `MAXCLIENTS_SLACK` (16; maxclients is published by
  `rustIoPublishGlobals()` through `vio_set_maxclients`), the IO thread
  writes `-ERR max number of clients reached` and closes.
  `rejected_connections` includes these (`vio_take_rejected`). Below that
  the engine decides at adoption, with its exact rules
  (`hasMaxClientsLimitReached()`: maxclients-reserved, priority subnets,
  cluster links, `rejected_priority_connections`). The first version
  refused at maxclients and broke unit/qos. The slack stays under the 32
  fds the engine reserves above maxclients, so accept does not hit
  EMFILE first.
- **Queued commands, global.** `QUEUED` counts parsed commands in Rust
  across all connections. Above `QUEUED_HIGH` (4096), a connection stops
  reading once it holds `FAIR_SHARE` (32) commands (`may_read`). The usual
  per-connection `CMDS_HIGH` still applies below the budget. A paused
  connection always holds commands, so it is in the engine's ready path.
  The engine's pop kicks it below `CMDS_LOW`, and `on_kick` resumes it
  (`may_resume`), so no global resume broadcast is needed. A connection
  with nothing queued can always read, so a fresh connection is never
  starved by pipelines. `vio_conn_close()` frees commands the engine will
  never pop, so closed connections draining output stop counting.

Not bounded: a single read can parse more than `FAIR_SHARE` commands (one
`READ_CHUNK`). That is the same per-read granularity as the C read path.

## Shutdown of a closing connection

`freeClient()` calls `connShutdown()` before `connClose()` while a fork
child holds a copy of the fd (`src/networking.c`, "Only use shutdown when
the fork is active"). `vio_conn_shutdown()` now only sets
`shutdown_wanted`. `Drop for Conn` shuts the fd down right before closing
it, which happens after the output drained or the peer went away.
Shutting down at once discarded the reply still in the Rust buffer
(`killself-bgsave.py`).

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

Stage 4a did the kick batching and the awake checks; see "Handoff v2". The session publish change was not done.

- Publish the session from `beforeNextClient()` only when it changed, and
  carry `consumed` in an atomic instead of the session mutex.
- Let the IO thread hold unparsed input for any connection with a pending
  barrier, the way `parse_held` does for AUTH, instead of prechecking and
  discarding. That turns `barrier` fallbacks into fast paths one handoff
  later, at the cost of a round trip per barrier.
- Batch IO-thread kicks at the end of `beforeSleep()` (stage 1 note); that is
  still where the main thread's time goes at `-P 1` (see the stage report).

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

Stage 4a did the kick deferral; see "Handoff v2". The rtc pending list was not done.

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
