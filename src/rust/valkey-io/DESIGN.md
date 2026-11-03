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
| `rust-io-mode` | `off` (default), `io`, `precheck`, `exec` | Cumulative: `precheck` = io + Rust-side checks, `exec` = precheck + Rust-owned `call()`. See "Composed modes". |
| `rust-io-exec-model` | `handoff` (default), `rtc` | exec mode only. |
| `rust-io-exec-precheck` | yes (default)/no | exec mode only. Debug switch: `no` runs exec without the precheck fast path, to isolate the two. |
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

| `rust-io-mode` | `off` (default), `io`, `precheck`, `exec` | Cumulative: `precheck` = io + Rust-side checks, `exec` = precheck + Rust-owned `call()`. See "Composed modes". |
| `rust-io-exec-model` | `handoff` (default), `rtc` | exec mode only. |
| `rust-io-exec-precheck` | yes (default)/no | exec mode only. Debug switch: `no` runs exec without the precheck fast path, to isolate the two. |
