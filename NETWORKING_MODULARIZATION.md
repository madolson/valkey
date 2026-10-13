# Networking Layer Modularization

Status: in progress. This document tracks the effort to build a clean,
minimal API boundary between Valkey's networking / client-I/O path and its
command-execution path, and then to re-implement that layer (first in C with a
narrow interface, then in Rust).

## Goals

1. A clean API boundary between the networking path and the command-execution
   path, with the *smallest practical number of unique entry points*.
2. `server.h` must no longer be the place that declares the client networking
   interface.
3. The boundary must be stable enough that an alternate implementation (e.g.
   Rust) can satisfy it without the rest of the server changing.
4. Changes must stay reviewable and backport-friendly (Valkey backports
   heavily; large mechanical diffs are costly — see `DEVELOPMENT_GUIDE.md`).

## Starting point (what already existed)

Valkey already has a transport abstraction in `src/connection.h`: the
`ConnectionType` vtable (`read`/`write`/`writev`/`accept`/`listen`/
`set_read_handler`/`set_write_handler`/…) with per-transport implementations in
`socket.c`, `unix.c`, `tls.c`, `rdma.c`. That is the *byte-transport* seam and
it is clean.

The coupling that was NOT clean is the **client I/O API**: ~140 functions
(client lifecycle, the `addReply*` reply-encoding family, the transport
read/write entry points, and the I/O-thread offload entry points) were all
declared inline in `server.h`, and the `client` struct — defined in `server.h`
— embeds all of its networking/I/O fields directly.

## The three concern-groups inside the networking API

The old `networking.c` prototype block in `server.h` mixed three concerns:

| Group | Examples | Count | Notes |
|---|---|---|---|
| Transport / client I/O | `createClient`, `freeClient`, `readQueryFromClient`, `writeToClient`, `sendReplyToClient`, `handleClientsWithPendingWrites`, `link/unlinkClient`, `listenToPort`, `acceptCommonHandler`, `ioThread*` | ~25 | The real networking seam. |
| Reply encoding (`addReply*`) | `addReply`, `addReplyBulk`, `addReplyError*`, `addReplyArrayLen`, deferred-reply API | ~60 | Called by every command in `t_*.c`. This *is* the command→protocol API already. |
| Introspection / lifecycle | `catClientInfoString`, `getClientType`, pause actions, client mem usage | ~55 | Cold-path, management. |

### Decision: do not churn the `addReply*` family

The reply-encoding family is the dominant contributor to the API count, but it
is already the de-facto clean interface between command handlers and the
protocol writer, and it is on the hottest path. Renaming or collapsing those
~60 symbols would touch nearly every command file for no structural gain and a
large, backport-hostile diff. They are kept as-is. "Minimize unique APIs" is
applied to the transport/I/O seam, where reducing coupling is real.

## Step 1 (done): extract the API declarations into `client_net.h`

`src/client_net.h` now owns the networking/client-I/O API:
- the RESP `READ_FLAGS_*` / `WRITE_FLAGS_*` state bits,
- the client lifecycle, reply-encoding, transport, and I/O-thread offload
  prototypes.

`server.h` includes `client_net.h` at the point where the block used to be
(after the core types `client`, `connection`, `connListener`, `robj`, `sds`,
`list`, `user`, `writePreparedClient`, `pause_purpose`, `ClientFlags` are
defined — `client_net.h` is a server.h-internal header, not standalone, and
documents that).

Result: **server.h no longer textually declares the client networking/I/O
interface.** No symbol was renamed; the struct layout is unchanged; no behavior
change. Verified with a clean `make -j8` and a server smoke test
(`ping`/`set`/`get`/`client info`).

Commit: `net: extract networking/client-IO API from server.h into client_net.h`.

## The `client` struct fields: constraint and options

The second half of goal 2 — removing the networking *field declarations* from
`server.h` — is constrained by two hard facts:

1. The networking/I/O fields (`conn`, `querybuf`+parse state, output
   `buf`/`reply`, `io_read_state`/`io_write_state`, byte counters, `peerid`,
   `sockname`, …) are **scattered** through the 110-field `client` struct and
   **deliberately ordered for cache locality** (the struct carries explicit
   comments such as *"Cache Locality: Grouped with 'flag' for getClientType()
   hot path."*). Pulling them into one contiguous block would reorder the
   struct and change its performance profile.
2. The fields are read/written directly at **500+ call sites** (`c->conn` alone
   appears 222 times), including lock-free cross-thread access from
   `io_threads.c` and `memory_prefetch.c`. A rename to `c->net.conn` would be a
   mechanical diff across nearly every file — exactly the kind of change the
   project's backport policy discourages.

Three options, in increasing invasiveness:

- **(A) Macro field block (layout-preserving).** Define the networking fields
  via a `CLIENT_NET_FIELDS` macro in `client_net.h`, expanded inside the
  `client` struct. Field names/positions are identical (zero call-site churn,
  identical layout), but the *declaration text* lives in the networking header.
  Caveat: only the networking fields that already form contiguous runs can be
  grouped without reordering; the scattered ones cannot move.
- **(B) Named sub-struct (`clientNet net;`) + accessors.** True separation; IO
  threads keep field access via `c->net.*`. Requires the 500+ site rename and a
  careful re-ordering pass. High value for the Rust port, high churn.
- **(C) Opaque handle.** Rejected: `io_threads.c` needs direct field access for
  the lock-free offload model, so the state cannot be fully opaque.

Current position: Step 1 (prototype extraction) is the low-risk win and is
done. Options A/B for the field declarations are a reviewed decision, not an
autonomous one, because both carry a layout/perf or churn tradeoff the
maintainers must weigh.

## Next phases

- **Phase 2 — Rust FFI stub.** A `rust/` crate built as a static lib exporting
  one `extern "C"` symbol, linked into the server via `src/Makefile`, called
  from C, proven at runtime. Establishes the toolchain and link path.
- **Phase 3 — Rust networking (vanilla, no tokio).** Re-implement a slice of
  the networking layer in Rust behind the existing `ConnectionType` vtable and
  the `client_net.h` interface, using the existing `ae` event loop and
  I/O-thread model. No new runtime.
- **Phase 3b — tokio feasibility spike.** A separate branch exploring a
  tokio-owned ingress, to understand feasibility and cost. Expected to be a
  research prototype, not a drop-in.
- **Phase 4 — interface review.** Parallel sub-agents review the C↔Rust
  interface for consistency and coherence.
- **Phase 5 — local benchmarking.** `valkey-benchmark` A/B on one host to check
  the Rust path matches the C path.
