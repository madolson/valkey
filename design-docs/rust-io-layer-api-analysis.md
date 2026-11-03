# Rust IO layer: API surface analysis

Snapshot analysis of `rust-io/integrate` at 0f61a6fb9, written before the iteration stages. It drove the refactor plan; the steps that were executed, with measurements, are in `src/rust/valkey-io/ITERATIONS.md`. The evaluation is in `rust-io-layer.md`.


Code: branch `rust-io/integrate` at `0f61a6fb9` (`/Users/matolson/git/valkey-agents-rust-io-integrate`). Fixes: `origin/rust-io/compat-fixes`. Line numbers refer to the integrate tree unless marked otherwise. "Today" means what the code does. "Proposal" means what I recommend. Every cost attribution below is inferred from CPU and context-switch data, because no stage had `perf` on the build host.

## Summary

In exec-rtc the boundary is 28 Rust exports, 10 C callbacks, a 29-entry `VioExecOps` table, 9 FFI structs and 31 engine invariants. io mode alone uses 21 exports, 2 callbacks and 14 invariants. Most ownership and thread contracts are prose in DESIGN.md and comments, not types. The worst coupling is not the FFI surface. It is the fake `"rust"` ConnectionType: the engine treats the IO layer as a socket (errno from `writev`, `read` always EAGAIN, sync IO stubs, a shared fd). That fake socket causes the RDB-channel, Lua-debugger and slot-import failures. From the code, it is also the likely cause of the unexplained CLIENT KILL during BGSAVE reply loss: `unlinkClient()` calls `connShutdown()` while a fork child exists (`networking.c:2302`), and `vio_conn_shutdown()` runs `shutdown(2)` at once (`lib.rs:1188`), before the IO thread has written the reply it was handed. Second is exec mode, which is behind every "main thread means GIL holder" contract and makes call() exist twice. Delete it. After those come session state mirrored in four places, thread contracts encoded in thread-locals, Rust reading engine memory through raw pointers, and 28 safe-looking exports that dereference raw pointers. Clippy finds 9 of them, and `conn_ref`/`conn_arc` hide the rest. The proposed engine API (`engine_io.h`) is small and IO-layer-agnostic, so today's C IO threads could use it too: session open/close, a batched submit of prechecked commands, a thread-safe precheck against a pinned snapshot, sealed reply chunks carrying a release offset and a completion token, published admission state, and an ops table the engine calls back through. The Rust side gets one `ffi` module holding all unsafe code, a `Runtime` handle instead of statics, a sans-IO connection state machine, one Waker/Mailbox implementation, RAII policy pins and typed statuses. Generate the export header with cbindgen and bind only `engine_io.h` with bindgen, both checked in with a diff check. Passing an engine handle everywhere and routing by the precheck slot makes the API N-engine ready. Session ownership and reply ordering across shards stay open. For main-thread cost, the best-supported changes are returning argv to the IO thread that allocated it, cross-connection batch submit (which restores the prefetch C IO threads already get), and a bounded IO-thread spin, which would let the measured 11-16% awake-check win ship. Zero-copy replies matter for large values, COB accounting and durability fences, and little for 64-byte GETs. Class C flags in the snapshot (2-5% at best, 20+ hooks) and io_uring on 5.10 (it never touches the main thread) are not worth doing now.

## 1. Boundary inventory

### 1.1 Rust exports (C calls Rust)

Thread "main" means the event-loop thread. "rtc-IO" means a Rust IO thread holding the module GIL. "Documented?" refers to `include/valkey_io.h` (H), `DESIGN.md` (D) or code comments (C).

| # | Export (def) | C caller | Thread | Frequency | Pointer ownership / lifetime | Documented? | Modes |
|---|---|---|---|---|---|---|---|
| R1 | `vio_start` (`lib.rs:864`) | `rust_io.c:613` | main | once | `cfg` and `bindaddr` borrowed for the call. `proto_max_bulk_len` and `client_max_querybuf_len` are pointers into `server` that Rust **keeps for the process lifetime** and reads without synchronization (`lib.rs:495`, `:527`) | H:52-53 "read live". The data race is not written down | all |
| R2 | `vio_main_begin_drain` (`:950`) | `rust_io.c:350` | main | per wakeup | none. Order is drain, clear, take | C `lib.rs:952`, D:112-121 | all |
| R3 | `vio_main_take_ready` (`:963`) | **none** | n/a | n/a | n/a | doc is stale: `lib.rs:961` says beforeSleep calls it | dead export |
| R4 | `vio_main_before_poll` (`:976`) | `rust_io.c:641` from `server.c:2153` | main | per loop iteration | none | H:105, D:412-427 | all |
| R5 | `vio_main_after_poll` (`:987`) | `rust_io.c:651` from `server.c:2214` | main | per iteration | none | H:107-109 | all |
| R6 | `vio_main_next_ready` (`:999`) | `rust_io.c:337` | main, nested loop too | per ready conn | returns an **owned** strong ref (`Arc::into_raw`) that the caller must release | H:96-97 | all |
| R7 | `vio_main_has_ready` (`:1014`) | `rust_io.c:401` | main | per beforeSleep | none. It reads a **thread-local**, so it is always 0 on an IO thread | H:99, comment misplaced | all |
| R8 | `vio_main_flush_kicks` (`:274`) | `rust_io.c:640` | main | per iteration, plus nested loop | none | H:100 | all |
| R9 | `vio_conn_retain` (`:1032`) | `rust_io.c:276` | main/rtc-IO | per connection | +1 strong ref, owned by `rustConnection.h` | D:75-78 | all |
| R10 | `vio_conn_release` (`:1037`) | `rust_io.c:339`, `:444` | main/rtc-IO | per ready conn and close | -1. The last drop closes the fd (`lib.rs:122`) and may run `free_argv` on the main thread | D:79-82 | all |
| R11 | `vio_conn_take_events` (`:1042`) | `rust_io.c:296` | main/rtc-IO | per ready conn | borrowed handle | H:87-91 | all |
| R12 | `vio_conn_udata` (`:1047`) | `rust_io.c:287,299,308,323` | main/rtc-IO | 1-4 per ready conn | returns `rustConnection*`, which the engine frees in `connRustClose` (`rust_io.c:451`). NULL after close | C `lib.rs:94` says "main thread only", false in rtc | all |
| R13 | `vio_conn_set_udata` (`:1052`) | `rust_io.c:277` | main/rtc-IO | per connection | Rust stores the engine pointer and never dereferences it (`Relaxed`) | no | all |
| R14 | `vio_conn_fd` (`:1057`) | `rust_io.c:272` | main | per connection | Rust owns the fd. The engine copies it into `connection.fd` and uses it for addresses and raw ops (`networking.c:2305`, `replication.c:1543`) | D:79-82, partly | all |
| R15 | `vio_conn_pop_cmds` (`:1063`) | `rust_io.c:118` | main/rtc-IO | per delivery, 1 lock per 128 cmds | caller owns `out[]`. **argv ownership moves to C** | D:122. H:120 says nothing about ownership | all |
| R16 | `vio_conn_pending_cmds` (`:1080`) | `rust_io.c:225,240,245,392,507` | main | 2-5 per delivery, plus the stalled scan | takes the connection mutex | no | all |
| R17 | `vio_conn_writev` (`:1088`) | `rust_io.c:468` | main/rtc-IO | per reply flush | `iov` borrowed for the call, every byte copied. Status comes back through **errno** | C `lib.rs:1084-1086` | all |
| R18 | `vio_conn_want_writable` (`:1125`) | `rust_io.c:498` | main | per write-handler change | none | D:142-147 | all |
| R19 | `vio_conn_set_auth_required` (`:1136`) | `rust_io.c:259`, `:289` | main/rtc-IO | per delivery | none | D:785-791 | all |
| R20 | `vio_set_default_auth_required` (`:1147`) | `rust_io.c:362`, `:615` | main/rtc-IO | per beforeSleep | none | H:126 | all |
| R21 | `vio_conn_set_session` (`:1154`) | `rust_io.c:193` | main/rtc-IO | per delivery | `VioSession` copied by value under a mutex. `user` is an identity key only | H:20-32 | precheck, exec |
| R22 | `vio_conn_close` (`:1161`) | `rust_io.c:443` | main/rtc-IO | per connection | engine must release afterwards. Rust flushes, then closes | D:153-156 | all |
| R23 | `vio_conn_shutdown` (`:1188`) | `rust_io.c:436` | main | `freeClient` with a fork child (`networking.c:2302`) | immediate `shutdown(2)` that **does not wait for `Conn.out`** | no | all |
| R24 | `vio_conn_defer_to_main` (`:1172`) | `rust_io.c:250` | rtc-IO | per deferred conn | borrowed | H:209 | rtc |
| R25 | `vio_wake_main` (`:1183`) | `rust_io_exec.c:350` | rtc-IO | per batch with beforeSleep work | none | H:211 | rtc |
| R26 | `vio_exec_configure` (`exec.rs:119`) | `rust_io_exec.c:424` | main | once | `cfg` copied | H:205 | exec |
| R27 | `vio_exec_call` (`exec.rs:148`) | `rust_io_exec.c:274` from `server.c:5084` | main/rtc-IO | per command | `client*` borrowed, reentrant through nested `processCommand` | D:885-904 | exec |
| R28 | `vio_rtc_stats` (`exec.rs:304`) | `rust_io_exec.c:447` | main | per INFO | caller owns `buf` | H:213 | rtc |
| R29 | `vio_conn_pending_out` (compat-fixes `lib.rs:855`) | compat `rust_io.c` `rustIoFlushReplicas` | main | shutdown | none | compat H | all |

### 1.2 C callbacks and op tables (Rust calls C)

| # | Entry | Called from | Thread | Frequency | Ownership / lifetime | Documented? | Modes |
|---|---|---|---|---|---|---|---|
| C1 | `create_argv` = `rustIoCreateArgv` (`rust_io.c:79`) | `lib.rs:548` | IO | **per command** | `ptrs`/`lens` borrowed. Returns a zmalloc'd `robj**` that Rust owns until popped | H:36-37, D:86-96 | all |
| C2 | `free_argv` = `rustIoFreeArgv` (`rust_io.c:85`) | `Conn::drop` `lib.rs:119` | **whichever thread drops the last Arc** (IO or main) | per unconsumed command at teardown | consumes argv | H:38-39, thread not stated | all |
| C3 | `precheck_begin` = `rustPrecheckBegin` (`rust_io_precheck.c:188`) | `precheck.rs:81` | IO | per `parse_buffered()`, i.e. per readable connection, not per poll iteration | returns a snapshot pinned in hazard slot `tid` until C4 | H:40-42, D:594-597 | precheck |
| C4 | `precheck_end` (`rust_io_precheck.c:198`) | `precheck.rs:100` (Drop) | IO | same | unpins | same | precheck |
| C5 | `precheck_cmd` = `rustPrecheckCmd` (`rust_io_precheck.c:236`) | `precheck.rs:90` | IO | **per command** | `snap`/`sess` borrowed. Writes `cmd` (a command-table pointer valid until MODULE UNLOAD), `slot`, `read_flags`, `token`. Returns 1 for a barrier | H:40-45. The MODULE UNLOAD race is at D:639 | precheck |
| T1-T5 | `VioRtcOps` `try_lock`, `lock`, `unlock` (per IO batch), `exec_ready` (per conn, handle borrowed), `after_batch` (per batch) (`rust_io_exec.c:297-351`) | `lib.rs:764-783` | IO | per poll iteration | `exec_ready` borrows `Arc::as_ptr` | H:191-197, D:955-963 | rtc |
| X1-X29 | `VioExecOps`, 29 entries (`exec.rs:52-82`, `rust_io_exec.c:52-271`) | `exec.rs:153-242` | main/rtc-IO | **29 indirect calls per command** | `client*` borrowed. `VioCallFrame` sits on the Rust stack and C fills it | H:157-188, D:919-953 | exec |

### 1.3 Structs crossing the boundary

| Struct | Rust def | C def | Notes |
|---|---|---|---|
| `VioConfig` | `lib.rs:40` | H:48 | Holds two retained raw pointers into `server` (`rust_io.c:597-598`). |
| `VioCallbacks` | `lib.rs:28` | H:35 | The mode is encoded as NULL vs non-NULL callbacks (`precheck.rs:78-82`). |
| `VioCmd` | `lib.rs:51` | H:73 | Union by convention: `err != 0` marks an error record, `token == 0` means not prechecked, and `read_flags` bit 29 is private (`rust_io_precheck.h:18`). |
| `struct iovec` | `libc` | `<sys/uio.h>` | `writev` input. |
| `VioSession` | `precheck.rs:24` | H:26 | `VIO_SESS_*` bits are hand-mirrored with no assert. |
| `VioCallFrame`, `VioExecOps`, `VioRtcOps`, `VioExecConfig` | `exec.rs:36-106` | H:136-203 | exec only. |
| C structs the bridge writes | n/a | `rustConnection` embeds `connection` (`rust_io.c:42-50`) | The bridge writes `cmdQueue` and `parsedCommand` internals directly (`rust_io.c:121-159`), calls `trimCommandQueue` through an extern (`rust_io.c:23`), and reads about 20 `client`/`server` fields in `rust_io.c` alone. |

### 1.4 Engine globals and invariants the bridge relies on

| # | Invariant | Where | Written? | Modes |
|---|---|---|---|---|
| G1 | argv `robj` built off-thread stays private until popped. zmalloc is thread safe | `rust_io.c:79-83` | D:88-96 | all |
| G2 | `c->querybuf == NULL` means `processInputBuffer()` consumes only `cmd_queue`. False for replicated clients (B2 `reploff`) | `rust_io.c:253` | D:128-132 | all |
| G3 | The bridge may write `cmd_queue`/`parsedCommand` layout directly | `rust_io.c:121-159`, `server.h:1380-1389` | no | all |
| G4 | ConnectionType `has/process_pending_data` run in beforeSleep, including inside `processEventsWhileBlocked()`, and drain the rest of a batch | `rust_io.c:366-412` | D:357-360 | all |
| G5 | The thread that called `vio_start` is "main". Kick batching keys off it | `lib.rs:925`, `:228` | D:373 | all |
| G6 | Hook placement: kick flush after every reply flush, before poll and inside the nested loop. after_poll at the end of afterSleep | `server.c:1985`, `:2153`, `:2214` | D:488 | all |
| G7 | C IO threads never take a Rust connection | `io_threads.c:697-700`, `:749-751` | D:83-84 | all |
| G8 | `connection.fd` is Rust's fd and stays open until both sides release it. The engine still runs raw fd ops on it | `lib.rs:439-441`, `networking.c:2302-2305`, `replication.c:1543` | D:79-82, partly | all |
| G9 | Limits are read through pointers into `server` | `lib.rs:495`, `:527` | H:52 | all |
| G10 | Default-user auth state is republished every beforeSleep | `rust_io.c:361-364` | D:785-788 | all |
| G11 | Takeover: no C TCP listener, and bind/port are frozen | `server.c:3342`, `config.c:2713`, `:2898`, `rust_io.c:67` | D:278-281 | all |
| G12 | Fork children have no Rust threads (RDB channel, ldb) | `rdb.c:4390` path | compat report | all |
| G13 | Shutdown must wait for Rust to flush replica output | compat `server.c` `finishShutdown` | compat | all |
| G14 | errno is the `writev` status channel | `lib.rs:1091`, `:1096` to `rust_io.c:470` | C | all |
| G15 | `acl_mutation_epoch` is bumped at every ACL mutation, and snapshot build/free restores it | `acl.c:559,609,1014,1770,3035`, `rust_io_precheck.c:90,112,119,121` | D:614-624 | precheck |
| G16 | The command table is immutable off-thread, except MODULE LOAD/UNLOAD | `server.c:735` | D:830-833 | precheck |
| G17 | `ACLCheckAllUserCommandPerm()` is thread safe on a copied user | `rust_io_precheck.c:229` | D:834-836 | precheck |
| G18 | Hazard slot = IO thread index < 128, with one reclaimer at a time. The 128 limit is repeated three times | `rust_io_precheck.c:39,131,189`, `config.c:3741`, `exec.rs:250` | D:322-331, partly | precheck |
| G19 | The engine never uses `read_flags` bits 29 and 30 | `rust_io_precheck.h:16-18` | no (not reserved in `server.h`) | precheck |
| G20 | The token survives `consumeCommandQueue` and is validated live | `networking.c:4603`, `server.c:4691` | D:543-560 | precheck |
| G21 | The barrier list is complete (the epoch check is the backstop) | `rust_io_precheck.c:65-69` | D:642-664 | precheck |
| G22 | call() control flow is mirrored in Rust, so every call() change lands three times | `exec.rs:148-243` | D:896-915 | exec |
| G23 | Nested call() (EXEC, scripts, RM_Call) stays C | `server.c` | D:906-909 | exec |
| G24 | The GIL is held by main from module init. Main releases it around poll | `server.c:128-132`, `module.c:9586-9596` | D:1012-1013 | rtc |
| G25 | The lock holder swaps `server.main_thread_id` (a data race for unlocked readers) | `rust_io_exec.c:293`, `:321` | D:1016 | rtc |
| G26 | `thread_shared_qb` per thread | `rust_io_exec.c:336` | D:1014 | rtc |
| G27 | `inMainThread()` is true on Rust threads (thread_id defaults to 0) | `io_threads.c:98` | D:1015 | rtc |
| G28 | Cached clock refreshed on every acquire | `rust_io_exec.c:294` | D:1020 | rtc |
| G29 | Yielding commands are deferred to main, and waiters use a 1 ms timed lock | `rust_io_exec.c:312-318`, `:357-377` | D:1017 | rtc |
| G30 | `after_batch` runs "enough" of beforeSleep. B6 shows it does not | `rust_io_exec.c:342-351` | compat B6 | rtc |
| G31 | "Main thread only" in the header really means "GIL holder" | H:113 | no | rtc |

### 1.5 Counts per mode

| Mode | Rust exports called | C callbacks | Op-table entries | FFI structs | Invariants |
|---|---|---|---|---|---|
| io | 21 (plus 1 dead, plus `defer_to_main` linked but unreachable) | 2 | 0 | 4 | 14 (G1-G14) |
| precheck | 22 | 5 | 0 | 5 | 21 (+G15-G21) |
| exec handoff | 24 | 5 | 29 | 8 | 23 (+G22-G23) |
| exec rtc | 27 | 5 + 5 `VioRtcOps` | 29 | 9 | 31 (+G24-G31) |

DESIGN.md's per-mode table (D:500-507) gives io 22. The difference is the dead `vio_main_take_ready`. compat-fixes adds `vio_conn_pending_out` (R29) to every mode.

## 2. Smells and coupling, ranked by modularity damage

1. **The fake ConnectionType and the shared `client`.** `CT_Rust` (`rust_io.c:534-569`) pretends to be a socket. `read` always returns EAGAIN (`:483-489`), `sync_read`/`sync_write` are stubs (`:515-528`), `writev` reports through errno (`lib.rs:1091`, `:1096`), and `connection.fd` is Rust's fd (`rust_io.c:272`). Engine code that runs raw fd operations therefore acts on Rust's socket behind its back. `unlinkClient()` shuts the fd down while a fork child exists (`networking.c:2302-2303`), and `vio_conn_shutdown()` does it immediately (`lib.rs:1188-1190`), before the IO thread writes the reply that `writev` only copied. With kick batching the write is delayed further, which fits "CLIENT KILL SKIPME no during BGSAVE loses its reply, final binary every time" (integrate report "Could not verify"). This is a hypothesis from code reading, not yet tested. The same pretence produces B2 (offsets from `querybuf`), the RDB channel and ldb failures, and the bridge writing `cmd_queue` internals (`rust_io.c:121-159`).
2. **Exec mode re-expresses call() through a function-pointer table.** 29 ops (`exec.rs:52-82`), 27 of them new C helpers (`rust_io_exec.c:52-271`), 29 indirect calls per command (about 60-70 ns measured). rtc redefines "main thread" as "GIL holder" (`rust_io_exec.c:293`, `:321`), touches `engineLockShared` (`server.c:128-132`) and adds a timed GIL acquire (`module.c:9586-9596`).
3. **Implicit thread contracts.** The header says "main thread only" (H:113), but rtc calls those functions on IO threads. `IS_MAIN` (`lib.rs:228`, `:925`), `MAIN_READY` (`:944`) and `LOCAL_KICKS` (`:305`) make the same export behave differently per thread. `udata` is "main thread only" through a `Relaxed` AtomicPtr (`lib.rs:94-95`). The drain-clear-take ordering rules exist only in comments (`lib.rs:827-833`, `:952-953`, `:776-777`). `free_argv` runs on whichever thread drops the last Arc (`lib.rs:113-123`).
4. **Session state mirrored in four places.** `rustConnection.sess` (`rust_io.c:49`), the Rust `SessionCell` mutex (`precheck.rs:45`), `Conn.auth_required` (`lib.rs:103`) plus `DEFAULT_AUTH_REQUIRED` (`lib.rs:173`), `pcSnapshot.default_auth_required` (`rust_io_precheck.c:43`), and the live `client`. That takes three publish calls per delivery or iteration (`rust_io.c:193`, `:259`, `:362`).
5. **Rust reads engine memory through retained pointers.** `read_volatile` of `server.proto_max_bulk_len` and `client_max_querybuf_len` (`lib.rs:495`, `:527`) while `CONFIG SET` writes them without atomics. Benign on x86 and undefined in Rust's memory model.
6. **Takeover and listener special cases spread across the engine.** `server.c:3342-3346`, `:3416-3418`, `:8297-8298` (an extra "Ready to accept" line for the Tcl harness), `config.c:2713-2719`, `:2898-2903`, and the bind-string compare in `rust_io.c:58-73`.
7. **Raw pointers in safe signatures, and unsafe everywhere.** All 28 exports are `pub extern "C" fn` taking raw pointers. My clippy run (1.94, Linux target, copy in `/tmp/rust-io/api-clippy`) gives 9 `not_unsafe_ptr_arg_deref` errors: `lib.rs:865, 1033, 1038, 1069, 1107, 1155`, `exec.rs:120, 330, 331`. Twelve more exports dereference through the safe private helpers `conn_ref`/`conn_arc` (`lib.rs:1022-1029`), which clippy cannot see. `unsafe` appears 35 times in lib.rs, 6 in exec.rs and 2 in precheck.rs, with syscalls inline in the worker loop (`lib.rs:390-905`). resp.rs is clean.
8. **Global statics in Rust.** `GLOBAL` (`lib.rs:168`), `DEFAULT_AUTH_REQUIRED` (`:173`), `HANDOFF_DISABLE` (`:181`), `EXEC` (`exec.rs:115`), `RTC_STATS` (`exec.rs:278`), and five thread-locals (`lib.rs:301-310`, `:943-945`). There is no `static mut`. The statics block N engines and keep the handoff out of `cargo test`/Miri (compat: Miri covers only resp/precheck/exec stats).
9. **Function-pointer tables as mode switches.** precheck on/off is decided by NULL callbacks (`precheck.rs:79-80`), and rtc by `Option<VioRtcOps>` (`exec.rs:110`). `rust_io_precheck_active` is a second copy of the same decision (`rust_io.c:603-604`).
10. **Hand-written header drift.** A misplaced comment (H:99-100 describes `has_ready` above `flush_kicks`), a dead export with a stale doc (R3), "keep in sync" notes (`lib.rs:22`, `exec.rs:48`, H:4), and constants mirrored without asserts (`VIO_EV_*`, `VIO_ERR_*` vs `ProtoError` `resp.rs:17-29`, `VIO_SESS_*`, `VIO_PI_*`; only `CMD_CALL_*` is asserted, `rust_io_exec.c:29-30`). The opaque name `VioConn` (H:16) does not match Rust's `Conn`. DESIGN.md:234 says "No change to networking.c", but stage 2 changed `networking.c:4603`.
11. **Unguarded engine additions.** `rust_precheck` fields in `client` and `parsedCommand` (`server.h:1388`, `:1435`) and `acl_mutation_epoch` (`acl.c:48`) compile into default builds.
12. **Panics as error handling.** `expect("valkey-io not started")` (`lib.rs:191`), `expect("spawn rust-io thread")` (`:934`), and `unwrap()` on every mutex lock. With `panic = "abort"`, any of these kills the server. `vio_start` leaks fds on a partial failure (`lib.rs:886-907`).

## 3. Target API

### 3a. Engine side: `src/engine_io.h`

Principles: the engine never sees an IO-layer type except an opaque `ioConn*`. The IO layer never sees `client`, `server` or `robj`. Every function is tagged `[engine]` (only on the thread that runs this engine's event loop) or `[any]` (thread safe).

```c
/* engine_io.h - what the engine exposes to an IO layer (Rust or C).
 * [engine]: only on the thread running this engine's event loop.
 * [any]:    thread safe. */
#include <stddef.h>
#include <stdint.h>
#include <sys/uio.h>

typedef struct engine engine;               /* one engine shard; today a wrapper over `server` */
typedef struct engineSession engineSession; /* per-connection execution context; today `client` */
typedef struct enginePolicy enginePolicy;   /* immutable published policy snapshot */
typedef struct ioConn ioConn;               /* IO layer's connection, opaque to the engine */

/* ---- Commands -------------------------------------------------------- */
enum { ENGINE_CMD_NOT_FOUND = 1u << 0, ENGINE_CMD_BAD_ARITY = 1u << 1,
       ENGINE_CMD_CROSSSLOT = 1u << 2, ENGINE_CMD_NO_KEYS = 1u << 3,
       ENGINE_CMD_PREPARE_ON_ENGINE = 1u << 4 /* module command: getkeys must run on the engine */ };

typedef struct ioCommand {
    void *argv;            /* engine argv, opaque; ownership moves on submit */
    uint32_t argc;
    uint32_t flags;        /* ENGINE_CMD_*, set by enginePrecheck */
    uint64_t argv_len_sum;
    uint64_t input_bytes;
    const void *cmd;       /* engine command, opaque; valid while the policy epoch holds */
    int32_t slot;          /* -1: keyless or not computed */
    uint64_t token;        /* 0: not prechecked */
} ioCommand;

typedef struct ioArg { const unsigned char *p; size_t len; } ioArg;

/* [any] Build argv for n commands in one call; cmds[i].argc args each, taken
 * from args in order. Objects are private to the caller until submitted. */
int engineArgvBuild(engine *e, const ioArg *args, ioCommand *cmds, size_t n);
/* [any] Free argv that never reached the engine, or that the engine
 * returned through ioOps.return_argv. Frees on the calling thread. */
void engineArgvFree(engine *e, void *const *argv, const uint32_t *argc, size_t n);

/* ---- Policy snapshot and precheck ----------------------------------- */
/* [any] Pin the current snapshot in hazard slot `io_thread`. Never NULL once
 * the engine has started. One pin per slot at a time. */
const enginePolicy *enginePolicyPin(engine *e, unsigned io_thread);
void enginePolicyUnpin(engine *e, unsigned io_thread);
uint64_t enginePolicyEpoch(const enginePolicy *p);
int enginePolicyAuthRequiredDefault(const enginePolicy *p); /* replaces vio_set_default_auth_required */
int enginePolicyShardOf(const enginePolicy *p, int slot);    /* N engines; 0 today */

enum { ENGINE_SESS_AUTHENTICATED = 1u << 0, ENGINE_SESS_MULTI = 1u << 1,
       ENGINE_SESS_PUBSUB_RESP2 = 1u << 2, ENGINE_SESS_SPECIAL = 1u << 3,
       ENGINE_SESS_AUTH_REQUIRED = 1u << 4 /* parser limits */ };

typedef struct engineSessionView {
    const void *user;      /* identity only */
    uint32_t gen;          /* changes iff any field below changes */
    uint32_t flags;        /* ENGINE_SESS_* */
    int32_t dbid;
    uint64_t consumed;     /* commands the engine has dequeued for execution */
} engineSessionView;

/* [any] Lookup, arity, slot and the class A/B/ACL decision for cmds[0..n).
 * barrier_active applies from cmds[0]; after a barrier command the rest of
 * the call is checked as barrier_active. Returns 1 + index of the last
 * barrier, or 0. Module commands get ENGINE_CMD_PREPARE_ON_ENGINE. */
size_t enginePrecheck(const enginePolicy *p, const engineSessionView *s,
                      int barrier_active, ioCommand *cmds, size_t n);

/* ---- Admission (published by the engine, read by the IO layer) ------- */
typedef struct engineAdmission {
    uint32_t accepting;          /* 0: refuse new sessions (loading, overload) */
    uint32_t sessions_free;      /* maxclients - connected, refreshed per iteration */
    uint64_t queued_cmd_budget;  /* commands the IO layer may hold for this engine */
} engineAdmission;
void engineAdmissionGet(engine *e, engineAdmission *out); /* [any] */

/* ---- Sessions and submit ([engine]) ---------------------------------- */
typedef struct ioPeerInfo { const char *addr; int port; const char *laddr; int lport;
                            int fd_for_display; const char *transport /* "rust" */; } ioPeerInfo;
engineSession *engineSessionOpen(engine *e, ioConn *c, const ioPeerInfo *peer,
                                 const char **err /* static reply text */);
void engineSessionPeerClosed(engineSession *s);        /* run what is queued, then free */
void engineSessionWritable(engineSession *s);          /* IO layer has room again */

enum { ENGINE_SUBMIT_BLOCKED = 1u << 0, ENGINE_SUBMIT_CLOSING = 1u << 1,
       ENGINE_SUBMIT_FULL = 1u << 2 /* stop handing commands; resume on session_update */ };
typedef struct engineSubmit {
    engineSession *s;
    ioCommand *cmds;
    size_t n;
    size_t accepted;   /* out: engine took ownership of cmds[0..accepted) */
    uint32_t state;    /* out: ENGINE_SUBMIT_* */
    int proto_err;     /* in: VIO protocol error after cmds[n-1], 0 if none */
} engineSubmit;
/* Validates tokens, runs the residual checks and call(), prefetching keys
 * across all sessions in the batch (processClientsCommandsBatch). Reentrant
 * from processEventsWhileBlocked(): entries already run are not rerun. */
void engineSubmitBatch(engine *e, engineSubmit *batch, size_t n);

/* ---- Replies ([engine]) ---------------------------------------------- */
typedef struct engineReplyChunk {
    const struct iovec *iov;   /* valid until engineRepliesDone(token) */
    int iovcnt;
    uint64_t bytes;
    uint64_t release_offset;   /* 0: none; else hold until durable >= release_offset */
    uint64_t token;
} engineReplyChunk;
/* Chunks handed through ioOps.push_replies were written or dropped. An IO
 * layer that copies (today's behaviour) calls this before push_replies returns. */
void engineRepliesDone(engine *e, const uint64_t *tokens, size_t n);

/* ---- What the engine calls on the IO layer --------------------------- */
typedef struct ioOps {
    /* [engine] Take ownership of chunks[0..ret). The engine keeps the rest
     * (client-output-buffer-limit applies) until engineSessionWritable(). */
    size_t (*push_replies)(void *io, ioConn *c, const engineReplyChunk *chunks, size_t n);
    /* [engine] After a delivery, when gen or consumed changed. */
    void (*session_update)(void *io, ioConn *c, const engineSessionView *v);
    /* [engine] Executed argv for the IO thread that built them to free
     * (refcount > 1 objects were already decremented, as in io_threads.c). */
    void (*return_argv)(void *io, ioConn *c, void *const *argv, const uint32_t *argc, size_t n);
    /* [engine] Close after writing everything pushed. IO_CLOSE_HALF: also
     * shutdown(2) once flushed, because a fork child holds the fd. */
    void (*close)(void *io, ioConn *c, int flags);
    /* [engine] Last call before poll; 1 = do not sleep. */
    int (*before_poll)(void *io);
    void (*after_poll)(void *io);
    /* [any] AOF/replication durable offset advanced (valkey#3381 fsync path). */
    void (*durable_advanced)(void *io, uint64_t offset);
} ioOps;

/* [engine] Attach an IO layer. The engine polls wake_fd and calls on_wake. */
int engineAttachIo(engine *e, const ioOps *ops, void *io, int wake_fd, void (*on_wake)(void *io));
engine *engineDefault(void); /* the singleton until shards exist */
```

**C IO threads on the same interface.** The main-thread half of io_threads.c already is a batch submit. `processIOThreadsResponses()` feeds `processClientIOReadsDone()` and then `processClientsCommandsBatch()` (`io_threads.c:1230-1250`, `memory_prefetch.c:324`). The reply snapshot `io_last_reply_block`/`io_last_bufpos` (`io_threads.c:767-783`) already seals a chunk, and `processClientIOWriteDone()` calls `_postWriteToClient()` (`networking.c:3455`), which is `engineRepliesDone`. argv offload (`io_threads.c:1016`) is `return_argv`. A C IO layer would parse into `ioCommand` arrays instead of `c->cmd_queue`, and would write chunks instead of reading `client` buffers under a snapshot. The interface turns into a contract what the C threads do today by reaching into `client`.

**What has to change in C.**
- Move `CT_Rust` out of rust_io.c into an engine-owned `connection_external.c` that delegates to `ioOps`. Removing ConnectionType outright is not small: engine code treats `c->conn == NULL` as a fake client (for example `io_threads.c:696`). Raw fd operations on external connections go through `ioOps.close(HALF)` instead of `shutdown(c->conn->fd)` (`networking.c:2302-2305`).
- `engineSubmitBatch` absorbs `rustIoPull`/`rustIoDeliver`/stall handling (`rust_io.c:109-263`, `:366-398`), so `cmd_queue` is written only by engine code.
- Split processCommand at `prechecked_resume:` (`server.c:4842`): `processCommandChecks()` (precheck-able) and `processCommandExecute()` (class C/D gates, throttle, the future tiering gate, the #3381 bracket, call()). This is upstreamable without Rust.
- Reply sealing: a `clientSealReplies(c, max, chunks)` built from the `writevToClient` iovec builder (`networking.c:3020-3150`) that stops at a fence, plus release through `_postWriteToClient`. Replies stay in `client` buffers internally. Only chunks leave.
- `freeClientArgv` calls `return_argv` for external sessions, the same way it calls `tryOffloadFreeArgvToIOThreads` today (`networking.c:2208`).
- Replicated-client offsets come from `input_bytes`, not `querybuf` (B2, `commandProcessed`).
- Session vs execution context. Session identity and protocol state (user, db, auth, resp, name, lib-name) is what `engineSessionView` publishes. The execution context (MULTI queue, blocking, reply buffers and COB, tracking, pubsub) stays in `client`. With one engine, both stay in `client` behind the opaque `engineSession*`. With N engines they separate (3c).

### 3b. Rust side

```
valkey-io/src/
  lib.rs        #![deny(unsafe_code, unsafe_op_in_unsafe_fn, clippy::undocumented_unsafe_blocks)]
  ffi/mod.rs    #![allow(unsafe_code)]  the only place with unsafe besides sys.rs
  ffi/engine_sys.rs  bindgen of engine_io.h (checked in)
  ffi/exports.rs     #[no_mangle] unsafe extern "C" fn vio_*: thin shims into runtime
  ffi/engine.rs      FfiEngine: impl Engine over engine_sys, plus the Send/Sync proofs
  sys.rs        #![allow(unsafe_code)]  epoll, eventfd, accept4, read, write: io::Result wrappers
  engine.rs     safe traits Engine / Policy / EngineThread, plus newtypes
  runtime.rs    Runtime<E>: IO threads, shards, limits, admission, knobs (no statics)
  reactor.rs    per-IO-thread loop over sys::Epoll (io_uring could implement the same trait)
  conn.rs       sans-IO connection state machine
  handoff.rs    Waker (Dekker sleep/pending), Mailbox<T>, KickBatch, completion queues
  resp.rs       unchanged
  precheck.rs   barrier sequencing over Policy
  admission.rs  pending-adoption bound, maxclients, queued-command budget
```

```rust
// engine.rs: safe interface. A mock implementation runs the whole handoff under cargo test and Miri.
pub trait Engine: Send + Sync + 'static {
    type Policy<'a>: Policy where Self: 'a;           // RAII pin; Drop unpins
    fn build_argv(&self, args: &[&[u8]], cmds: &mut [Command]) -> Result<(), AllocError>;
    fn free_argv(&self, argv: Vec<Argv>);             // on the calling (IO) thread
    fn pin(&self, t: IoThreadId) -> Self::Policy<'_>;
    fn admission(&self) -> Admission;
}
pub trait Policy {
    fn epoch(&self) -> u64;
    fn default_auth_required(&self) -> bool;
    fn shard_of(&self, slot: Slot) -> ShardId;
    /// Returns the index of the last barrier in `cmds`.
    fn precheck(&self, s: &SessionView, barrier_active: bool, cmds: &mut [Command]) -> Option<usize>;
}
/// Only constructible inside the engine's on_wake / before_poll callbacks,
/// so "who may call what" is a type, not a comment. !Send.
pub trait EngineThread {
    fn open(&mut self, c: ConnRef<'_>, peer: &PeerInfo) -> Result<SessionId, OpenError>;
    fn submit(&mut self, batch: &mut [Submit<'_>]);
    fn replies_done(&mut self, tokens: &[ReplyToken]);
    fn peer_closed(&mut self, s: SessionId);
    fn writable(&mut self, s: SessionId);
}

pub struct Argv(NonNull<c_void>);                  // owned engine argv; Send, not Clone
pub struct Command { pub argv: Option<Argv>, pub argc: u32, pub flags: CmdFlags,
                     pub argv_len_sum: u64, pub input_bytes: u64,
                     pub cmd: Option<CmdRef>, pub slot: Option<Slot>, pub token: Option<NonZeroU64> }
#[derive(Clone, Copy)] pub struct SessionId(NonNull<c_void>, PhantomData<*mut ()>); // !Send
#[derive(Clone, Copy, PartialEq, Eq)] pub struct IoThreadId(u16);
#[derive(Clone, Copy, PartialEq, Eq)] pub struct ShardId(u16);
pub struct ReplyToken(u64);

#[derive(Debug)] pub enum StartError { Bind(io::Error), Poll(io::Error), Spawn(io::Error), AlreadyStarted }
#[derive(Debug)] pub enum OpenError { MaxClients, NotAccepting, Engine(&'static CStr) }
#[repr(C)] pub enum PushStatus { Accepted = 0, Full = 1, PeerClosed = 2 }   // replaces errno
```

```rust
// conn.rs: ownership and the state machine
/// Shared by the owning IO thread and the engine thread.
pub(crate) struct ConnShared {
    flags: AtomicU32,                  // ConnFlags: IN_READY KICKED ENGINE_CLOSED PEER_CLOSED
                                       // WANT_WRITABLE READ_PAUSED PARSE_HELD HALF_CLOSE
    inbound: Mutex<VecDeque<Command>>,
    outbound: Mutex<ReplyQueue>,       // copied bytes now; chunks + fences later
    out_bytes: AtomicUsize,
    session: SessionCell,              // single writer (engine thread), seqlock; replaces 4 mirrors
    thread: IoThreadId,
}
/// The engine's strong reference: created by adopt, consumed by close.
#[repr(transparent)] pub struct ConnHandle(NonNull<ConnShared>);
impl Drop for ConnHandle { fn drop(&mut self) { /* Arc::decrement_strong_count, in ffi */ } }
/// A borrowed handle for the duration of one FFI call or callback.
#[repr(transparent)] #[derive(Clone, Copy)] pub struct ConnRef<'a>(&'a ConnShared);

/// IO-thread-local half. Pure transitions that return effects, so it is testable without sockets.
pub(crate) enum Phase { Accepted, Adopted, PeerGone, Closing { half: bool }, Done }
pub(crate) struct Effects { pub notify: u32, pub interest: Option<Interest>, pub remove: bool }
impl ConnLocal {
    pub fn on_read(&mut self, sh: &ConnShared, bytes: usize, lim: &Limits) -> Effects;
    pub fn on_flushed(&mut self, sh: &ConnShared, written: usize, err: Option<io::Error>) -> Effects;
    pub fn on_kick(&mut self, sh: &ConnShared) -> Effects;
}
```

```rust
// handoff.rs: one implementation for engine-bound and IO-bound wakeups (today lib.rs:249-268, 806-853, 947-994)
pub(crate) struct Waker { efd: sys::EventFd, signaled: AtomicBool, sleeping: AtomicBool, pending: AtomicBool }
impl Waker {
    /// Producer, after pushing. SeqCst Dekker with `prepare_sleep`.
    pub fn wake(&self);
    /// Consumer, before poll. Spins up to `spin` checking `pending`, then sets
    /// `sleeping`. Returns 0 if work is pending.
    pub fn prepare_sleep(&self, spin: SpinBudget) -> Timeout;
    pub fn after_poll(&self);
    /// eventfd readable: drain, then clear `signaled`. That order is load-bearing (bootstrap stall).
    pub fn on_readable(&self);
}
pub(crate) struct Mailbox<T> { items: Mutex<Vec<T>>, waker: Waker }

// precheck.rs: the pin is RAII, one per poll iteration instead of per connection
let pin = engine.pin(self.id);                 // hazard pointer stored
for conn in readable { conn.parse_into(&pin, &mut cmds); }
drop(pin);                                     // hazard slot cleared
```

```rust
// ffi/exports.rs: every export is unsafe, documented, and delegates
/// # Safety
/// `rt` comes from `vio_start`. `c` is a handle the engine adopted and has not
/// closed. `chunks[0..n]` stay valid until `engineRepliesDone`. Engine thread only.
#[no_mangle]
pub unsafe extern "C" fn vio_push_replies(rt: &Runtime<FfiEngine>, c: ConnRef<'_>,
                                          chunks: *const sys::engineReplyChunk, n: usize) -> usize {
    // SAFETY: caller contract above.
    let chunks = unsafe { std::slice::from_raw_parts(chunks, n) };
    rt.engine_thread().push_replies(c, chunks)
}
```

**Header generation.** Generate `valkey_io.h` (the Rust exports) with cbindgen from `ffi/exports.rs` and the `#[repr(C)]` types. Check it in, and add a `make check-rust-header` diff check. Rust is the source of truth for those signatures, and every drift item in smell 10 is something generation removes. Bind only `engine_io.h` with bindgen, with an allowlist, and check the output in as `engine_sys.rs`. Reasons: C is the source of truth for the engine API, the constants stop being hand-mirrored, and builds keep working without libclang. **Never bindgen `server.h`.** Exposing `client`/`server` to Rust recreates exactly the coupling exec mode had. The rejected alternative, hand-written headers with `static_assert` on sizes and offsets, catches layout drift but not signature or doc drift.

### 3c. N engines

- Every engine call takes an `engine*`. `vio_start(cfg, engines[], n)` gives the `Runtime` one `ShardQueue { ready: Mailbox<ConnArc>, waker }` per engine, each engine polling its own `wake_fd`. The singleton becomes `engineDefault()`.
- Routing: precheck already computes `slot` (`rust_io_precheck.c:257`). `Policy::shard_of(slot)` comes from the same snapshot, so a slot moving between shards is an epoch change and any in-flight token falls back. Keyless commands go to the connection's home shard.
- Per-connection ordering: each command carries a sequence number. The IO layer releases replies in sequence order and holds shard B's reply until shard A's earlier one arrives. Session barriers (SELECT, AUTH, CLIENT, ...) drain all shards the connection has in flight before later commands are prechecked. The barrier mechanism already exists (`precheck.rs:86-94`).
- MULTI, EVAL and FCALL must be single-shard, rejected with CROSSSLOT like cluster mode. Cross-shard pipelines are allowed and reordered.
- Session ownership is the main open design point. Identity and protocol state either moves into the IO layer and is passed to each shard as an `engineSessionView`, or each shard holds its own copy. The execution context (MULTI, blocking, COB, tracking) is created lazily per (connection, shard).
- Durability: `engineReplyChunk.release_offset` becomes `(shard, offset)`, and `durable_advanced` gains a shard id.

### 3d. Deletions

**Delete exec mode from the main line.** Keep `rust-io/exec` and `rust-io/integrate` as tagged references; do not keep a feature flag. Why:
- The evaluation already recommends against cut point 3.
- A feature flag means every refactor step below has to build and pass tests in two more modes.
- Its thread contracts (G24-G31) contradict the target. With exec gone, "engine thread" and "main thread" are the same thread.
- It is the largest source of duplication: call() twice, 27 helpers.

What survives as ideas, not code: the lock statistics method, `defer_to_main` as the prototype for shard routing, and the rtc measurement (lock busy 93-95% at saturation) as the cost model for a single engine. Delete `rust-io-exec-model`, `rust-io-exec-precheck`, `engineLockShared` (`server.c:128-132`), `moduleTimedAcquireGIL` (`module.c:9586-9596`), `INFO rustio`, `rtc_cycle`, `LOCAL_KICKS`, `IO_THREAD`, `vio_conn_defer_to_main`, `vio_wake_main`, `vio_rtc_stats`, `VioExecOps`, `VioRtcOps`, `VioCallFrame`.

Also delete the dead `vio_main_take_ready` export. Keep `rust-io-mode io` as a baseline, expressed as precheck off. Keep `VIO_HANDOFF_DISABLE` as a `Runtime` knob until plan step 20 decides the IO-thread awake check, then remove bits 1 and 4, which only switch off measured wins.

## 4. Performance-relevant API changes

Baseline, from the integrate final matrix (main-thread CPU per request, precheck mode): `-P 16` GET 433-438 ns, SET 582-591 ns; `-P 1` 1.12-1.63 us. C `io-threads 4` at `-P 16`: GET 407 ns, SET 610 ns. The `-P 1` minus `-P 16` gap (about 0.7-1.1 us) is per-wakeup and per-connection fixed cost (inferred, bootstrap report line 224).

| Candidate | Main-thread work removed | Expected gain (inferred) | Risk | Verdict |
|---|---|---|---|---|
| **Free argv on the allocating IO thread** (`ioOps.return_argv`) | `decrRefCount` plus `zfree` per argument and the array. In Rust mode all of it runs on main, because `tryOffloadFreeArgvToIOThreads` needs `active_io_threads_num > 1` (`io_threads.c:1017`). It is also a cross-thread jemalloc free, since the IO thread allocated it | 2-4 frees per GET/SET: about 5-10% at `-P 16`, more for MSET and other wide commands | Medium. `original_argv`, MULTI-queued argv, argv kept by blocked clients, and a client freed while its argv is in flight. Copy the C rule: refcount > 1 decremented on main | **Do first** |
| **Cross-connection batch submit** (`engineSubmitBatch`) | Per-connection crossings: today about 6 Rust calls and 3-5 mutex ops per delivery (`rust_io.c:118,193,225,240,245,259`). Restores cross-client key prefetch: Rust calls `processInputBuffer` per client (`rust_io.c:253`), so it gets only per-client prefetch (`networking.c:4747`), while C IO threads batch across clients (`memory_prefetch.c:374`, `:324`) | At `-P 1`: up to what prefetch gives C, plus the per-connection crossings. Bound it first: C io4 with `prefetch-batch-max-size 0` vs default, `-P 1`, large `-r` | Medium-high. The batch must be resumable from a nested `processEventsWhileBlocked()` (the B3 lesson; `memory_prefetch.c:338` clears entries for this reason) | **Do** |
| **Bounded spin on IO threads, then enable the IO-thread awake check** | eventfd `write(2)` per IO thread kick from main | The measured d0 effect: -11% to -16% main s/1M at `-P 1` with 4-6 threads, -21% in the client-bound t4 run. The spin targets the d0 regression (t1 `-P 16` -29% to -34% RPS, IO thread at 820-830 sleeps per 3 s vs 4) | Medium. IO-thread CPU at idle, so the budget must be adaptive (spin only after a recent wake, about 2-20 us). Never spin on main | **Do** |
| **Zero-copy reply handoff** (sealed chunks plus completion tokens) | The reply memcpy under the conn mutex (`lib.rs:1102-1114`). Release (`_postWriteToClient`) still runs on the engine, because `releaseBufReferences` decrements `robj` refcounts (`networking.c:3402-3437`) | About 0-2% at 64 B, where the mutex and kick remain either way. Large for big values: a 200 KB GET is about 10-20 us of main-thread memcpy | High (lifetimes, release on close, partial writes). But it is also the vehicle for COB accounting (bytes stay in `c->reply` until released, which fixes part of the client-eviction gap) and for #3381 fences | **Do after the chunk API exists, threshold-based**: copy below about 16 KiB |
| **io_uring on IO threads (kernel 5.10)** | Nothing on main. The engine wakeup stays eventfd plus ae epoll | IO-thread syscall batching only. 5.10 lacks multishot accept/recv (5.19/6.0), ring-mapped provided buffers (5.19), `MSG_RING` for cross-ring wakeups (5.18) and `SEND_ZC` (6.0), and needs CAP_SYS_ADMIN for SQPOLL (relaxed in 5.11). Verify against the host kernel's feature probe | Medium (a second reactor) | **Defer.** Keep the `reactor` trait so it can be added for IO-thread-bound configs (t1) |
| **Class C flags in the snapshot** | About 15 live checks after `prechecked_resume` (`server.c:4959-5067`: disk error, min-replicas, read-only and stale replica, loading, busy, pause) | 10-30 ns, i.e. 2-5% at best, since each is 1-3 loads in the boring state | High. 20+ mutation sites need epoch hooks (D:630-640), and a mid-batch state change (CLIENT PAUSE) must invalidate | **Reject.** Measure the bound with a measurement-only build that skips the block |
| Session publish only on change, `consumed` atomic | One mutex plus a 32-byte copy per delivered connection (`rust_io.c:181-194`, `precheck.rs:51-52`) | 1-3% at `-P 1`, about 0 at `-P 16` | Low-medium (barrier lift ordering) | Do, as part of the session-view unification |
| Batched argv/precheck FFI, pin per iteration | Nothing on main. On IO threads it removes 2 indirect calls per command and a seq_cst store per readable connection (`lib.rs:531`, `:548`, `:562`) | 1-5% IO-thread CPU per request, so RPS only where the IO thread is the bottleneck (t1 `-P 16`) | Low | Do |

**How a #3381 durability fence works on the chunk path.** Today #3381 caps writes at per-client byte offsets inside `clientHasPendingReplies`, `writevToClient` and `_writeToClient`, and re-queues clients from beforeSleep (`notifyReplyBlockingProgress`) when fsync advances. Rust inherits that only because it sits behind `writevToClient` (D:753-762). With chunks:
1. Fence stamping stays in the engine. `computeCommandBlockingOffset()` after `afterCommand()` gives the offset a command's reply depends on.
2. `clientSealReplies()` never lets a chunk straddle a fence, and it stamps each chunk with `release_offset` (0 when no fence applies).
3. Rust keeps a FIFO of chunks per connection. The IO thread writes from the head while `head.release_offset <= durable.load(Acquire)`. A held chunk blocks the ones behind it, which preserves reply order.
4. The fsync completion path (BIO, per #3381) calls `ioOps.durable_advanced(offset)`: one `Release` store, plus a wake of the IO threads that have fenced connections. Rust keeps a per-thread list of those.
5. This removes the main-thread hop per fsync that cut points 1 and 2 would otherwise pay (D:1041).
6. Held chunks stay engine memory, so `client-output-buffer-limit` still applies. Rust also stops reading a connection whose held bytes pass a threshold, which answers #3381's open "unbounded output growth while held" problem on the input side.

Per-shard offsets come with 3c.

## 5. Refactor plan

Steps 1-15 are structural and change no behavior except two marked bug fixes. Steps 16-23 change performance or behavior and land on the clean code. The gate for each step: `cargo test`, clippy, `smoke.py`, `hazards.py`, `scriptkill.py`, the Tcl files from the integrate matrix, and the bench matrix (t1/t4/t6 × `-P 1`/`-P 16` × GET/SET, 3 reps, median) within ±3%.

| # | Change | Files | Invariant established | Confirming measurement | Risk |
|---|---|---|---|---|---|
| 1 | Bring in compat-fixes B1, B2, B4, B5. B1's UAF is still on integrate (`rust_io_precheck.c:96-111`). Drop B3 (`62fe59ecc`), which the integrate SCRIPT KILL fix supersedes | `rust_io_precheck.c`, `cluster_migrateslots.c`, `rust_io.c`, `server.c`, `lib.rs`, header | ASAN precheck smoke clean | ASAN smoke, bench neutral | Low. Conflict in `connRustProcessPendingData`: keep integrate's version |
| 2 | Delete exec mode (3d) | `exec.rs`, `rust_io_exec.c`, `lib.rs:219-224,754-791,1169-1185`, H:131-214, `rust_io.c:248-252,591`, `rust_io.h`, `server.c:128-132,5082-5086,6990-6993`, `module.c:9586-9596`, `config.c:3743-3744`, Makefile | Engine code runs only on the main thread, so H:113 becomes true | Exports 28→24, callbacks 10→5, ops 29→0. io/precheck rows neutral | Low |
| 3 | Remove the `vio_main_take_ready` export (keep it as an internal fn). Fix H:99-100 | `lib.rs:959-971`, H | No dead exports | `nm` symbol list | None |
| 4 | Make all exports `unsafe extern "C"` with `# Safety` docs. `conn_ref`/`conn_arc` become `unsafe fn`. Fix the 8 clippy warnings. Add `cargo clippy -- -D warnings` to the Makefile cargo rule | `lib.rs`, `precheck.rs`, Makefile | clippy gates the build | clippy exit 0. Codegen unchanged (same `nm`, bench neutral) | None |
| 5 | Split `ffi/` (exports, raw types) and `sys.rs` (syscalls). `#![deny(unsafe_code)]` at the crate root | new `ffi/*.rs`, `sys.rs`, `lib.rs` | unsafe only in `ffi/` and `sys.rs`, each block with a SAFETY note | `grep unsafe` outside them = 0. Bench neutral | Low |
| 6 | `ConnHandle` / `ConnRef` newtypes over `NonNull`. `vio_main_next_ready` returns `Option<ConnHandle>` | `ffi/exports.rs`, `conn.rs`, `rust_io.c` (no change to the C types) | Refcount changes only inside the handle impls | Bench neutral. Leak check: smoke under ASAN `detect_leaks=1` | Low |
| 7 | Generate `valkey_io.h` with cbindgen. Check it in, add `make check-rust-header` | `cbindgen.toml`, H, Makefile | Header = exports | Diff check in CI | Low (type renames such as `Conn`/`VioConn`) |
| 8 | `Runtime` handle instead of `GLOBAL`, `DEFAULT_AUTH_REQUIRED`, `HANDOFF_DISABLE` and the main-thread thread-locals. `vio_start` returns `VioRuntime*`, and every export takes it. Engine-thread state lives in an `EngineSide` field, debug-asserted to the starting thread | `lib.rs`, `runtime.rs`, `rust_io.c`, H | No `static` items in the crate | A cargo test that builds two runtimes. Bench neutral | Medium (mechanical, touches every call site) |
| 9 | Limits by value: `vio_set_limits(rt, &limits)` from the config apply functions and at start. Remove the raw `server` pointers | `lib.rs:44-45,495,527`, `rust_io.c:597-598`, `config.c` (apply for `proto-max-bulk-len`, `client-query-buffer-limit`) | Rust reads no engine memory outside a call | Tcl unit/protocol, `CONFIG SET proto-max-bulk-len` over the Rust port. Neutral | Low-medium (a missed update path) |
| 10 | Connection state machine: the 8 `AtomicBool`s become one `AtomicU32` `ConnFlags`, plus an IO-local `Phase` with pure transitions returning `Effects`. Unit tests with a mock: peer close before adopt, engine close with output pending, kick coalescing | `conn.rs`, `lib.rs` | Every transition lives in one file and is covered by Miri | 600 s stall detector, `scriptkill.py`. Neutral | Medium (ordering) |
| 11 | `handoff.rs`: one `Waker` plus `Mailbox` replacing the three hand-rolled Dekker copies (`lib.rs:249-268`, `806-853`, `947-994`) | `handoff.rs`, `lib.rs` | One wakeup protocol, unit-tested (loom optional) | Voluntary context switches with `tools/cs.sh` unchanged (main about 3000, rust-io-0 under 10 at t1 `-P 16`). Bench neutral | Medium. This is where the 29-34% regression lived |
| 12 | C: move `CT_Rust` into `connection_external.c` with an `ioOps` table. rust_io.c only wires | `connection_external.c`, `rust_io.c`, `connection.{h,c}`, Makefile | The engine-side transport shim knows nothing about Rust | Neutral. Default build unchanged | Low |
| 13 | **Bug fix**: an external connection's `shutdown` means "flush, then shutdown(2)" (`IO_CLOSE_HALF`). Rust defers `shutdown` until `Conn.out` drains | `connection_external.c`, `lib.rs:1188-1190`, `networking.c:2302-2305` (call the transport op) | No reply bytes lost to a fork-time shutdown | `tools/killself-bgsave.py`: 5/5 losses today; expect 0/5. Introspection "CLIENT KILL ... during bgsave - fork" passes | Low. If 0/5 is not reached, the hypothesis is wrong: report it and keep the change only if it is neutral |
| 14 | C: `engine_io.h` and `engineSubmit` (single-session form) absorb `rustIoPull`/`rustIoDeliver`/stall (`rust_io.c:94-263`, `:366-398`) into `engine_io.c` | `engine_io.{h,c}`, `rust_io.c`, `networking.c` (export `trimCommandQueue`) | Only engine code writes `cmd_queue`/`parsedCommand` | Neutral. backpressure.py | Medium (stalled and blocked paths). May need two sittings (pull, then deliver) |
| 15 | C: split processCommand at `prechecked_resume` into `processCommandChecks` and `processCommandExecute`, documenting where the #3381 bracket and the tiering gate go | `server.c:4673-5090` | One execute entry point for prepared commands | Neutral on the default C build (unit/type/string, unit/multi, unit/acl) and in precheck | Low-medium |
| 16 | **Behavior and perf**: single session view (seqlock written by the engine, `consumed` atomic, published only on change). Delete `SessionCell` (`precheck.rs:45-57`), `rustConnection.sess` (`rust_io.c:49`), `Conn.auth_required`, `DEFAULT_AUTH_REQUIRED`. The default auth moves into the policy | `precheck.rs`, `conn.rs`, `rust_io.c`, `rust_io_precheck.c`, `engine_io.c` | One writer and one copy of session state | Main s/1M at `-P 1` t4/t6: predict -1% to -3%. hazards.py barrier counts unchanged | Medium (barrier lift needs a `Release` store after dequeue and an `Acquire` load) |
| 17 | Batched `engineArgvBuild` / `enginePrecheck`, pin per poll iteration | `engine_io.c`, `rust_io_precheck.c`, `precheck.rs`, `reactor.rs` | 1 crossing per parse batch | IO-thread CPU per request at t1 `-P 16`: predict -1% to -5%. Main neutral | Low |
| 18 | Admission control: bound unadopted connections per IO thread, enforce `maxclients` at accept from `engineAdmission` (Rust writes the engine's static error text), and a global queued-command budget | `admission.rs`, `reactor.rs`, `engine_io.c` | The IO layer cannot outrun the engine | Compat soak overload case: fresh PING < 100 ms, fds ≤ maxclients + per-thread bound. Bench neutral | Low-medium (the error text must match the C reply) |
| 19 | Return argv to the allocating IO thread (`return_argv`, batched with kicks) | `networking.c:2200-2215`, `engine_io.c`, `handoff.rs` | Argv freed on the thread that allocated it | Per-stage timer around `freeClientArgv` (measurement build) plus main ns/request at `-P 16`: predict -5% to -10% | Medium (argv lifetime cases, column 4 of section 4) |
| 20 | Bounded adaptive spin on IO threads, then set the IO-thread awake check on by default | `handoff.rs`, `reactor.rs` | IO threads do not sleep between back-to-back kicks | Full matrix: t4/t6 `-P 1` main s/1M -11% to -16%, t1 `-P 16` RPS within 3% of today, idle-server CPU about 0, `cs.sh` | Medium (CPU burn; tune the budget) |
| 21 | `engineSubmitBatch` across connections with prefetch. The drain collects every ready session first | `engine_io.c`, `memory_prefetch.c`, `runtime.rs` | One submit per wakeup | First the bound: C io4 with prefetch on vs off. Then Rust `-P 1` t4/t6 main ns, and with `-r 10000000` | Medium-high (nested-loop reentrancy) |
| 22 | Reply chunk API in copy mode: `clientSealReplies`, `push_replies`, `engineRepliesDone` called before returning | `networking.c`, `engine_io.c`, `conn.rs` | Replies leave the engine only as chunks | Neutral. unit/introspection, client-output-buffer tests | Medium |
| 23 | Zero-copy for chunks of at least about 16 KiB, released through the completion queue. Bytes held in Rust count toward COB | `conn.rs`, `handoff.rs`, `engine_io.c` | No memcpy of large replies on main | Main ns/request for 16 KB and 200 KB GETs. client-eviction tests (some of the 13 MISSING-FEATURE should pass) | High |

When #3381 lands, add `release_offset` and `durable_advanced` on top of 22-23 (section 4).

## Open questions

1. Session ownership with N engines: does identity and protocol state (user, db, resp, name) move into the IO layer, or does each shard keep a copy? This decides whether `engineSessionView` flows IO to engine (per submit) or engine to IO (per update, as proposed for one engine).
2. Should the C IO threads also move onto `engine_io.h`? That is upstream value independent of Rust, and the strongest argument that the boundary is the engine's and not Rust's. It is not needed for the Rust work.
3. Is a reply in client order that waits on the slowest shard acceptable for cross-shard pipelines? Or should the IO layer reject cross-shard pipelines the way cluster mode rejects cross-slot MULTI?
4. Step 13 rests on a code-reading hypothesis for the fork case. Forkless BGSAVE also lost the reply in the integrate repro, and I did not find what calls `connShutdown` there. Something else may be involved.
5. What is the zero-copy threshold? It needs the step 22 copy-mode numbers per reply size.
6. Is `rustix` acceptable as a second dependency? It would remove most of the `sys.rs` unsafe. Today only `libc` is used, and I did not assume a new dependency.
7. Who owns replica sockets and the RDB channel, given that fork children have no Rust threads? This is unchanged from EVALUATION.md, and it constrains `ioOps.close(HALF)` and any chunk held at fork time.
8. All attributions in sections 4 and 5 are inferred. Should step 0 be a per-stage `rdtsc` timing build (precheck validate, residual checks, call(), reply build, argv free, writev copy), so the perf steps are ranked on data before 19-23?
