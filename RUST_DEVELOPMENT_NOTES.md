# How Rust Helps: evidence from the networking PoC

This document answers one question with evidence from *this* codebase, not
generalities: **when you develop Valkey's networking layer, what does Rust
actually change?** Every claim below is backed by something runnable in this
repo (`rust/valkey_net`).

## The networking layer is exactly where C's costs land

The code we ported is the server's most dangerous C:

- `socket.c` / `networking.c`: raw `read`/`write`/`writev` and pointer-walked
  RESP parsing (`newline - (c->querybuf + c->qb_pos)`, `(ssize_t)(len - pos - 2)`).
- `io_threads.c`: a lock-free SPMC/MPSC/SPSC worker pool where correctness rests
  on hand-placed `_Atomic` fields and `atomic_thread_fence()` calls, with client
  state (`bufpos`, `io_read_state`, `querybuf`) shared across threads.

These two patterns -- unchecked buffer arithmetic and unsynchronized shared
state -- are the two classic sources of serious Redis/Valkey CVEs. They are
also precisely the two things Rust checks at compile time.

## Evidence 1 -- out-of-bounds buffer access becomes a compile error

The RESP parser does arithmetic like `querybuf[qb_pos]` where `qb_pos` comes
from a length calculation. If that calculation is wrong, C reads foreign memory
with zero diagnostics. The same shape in Rust:

```
error: this operation will panic at runtime
 --> oob.rs:8:14
  |
8 |     let _c = querybuf[qb_pos];
  |              ^^^^^^^^^^^^^^^^ index out of bounds: the length is 4 but the index is 10
  = note: `#[deny(unconditional_panic)]` on by default
```

`rustc` **refuses to compile** the out-of-bounds access (and where the index
isn't a constant, it is bounds-checked at runtime and panics with a clear
message instead of reading past the buffer). The identical C compiles clean.

## Evidence 2 -- cross-thread data races become a compile error

The IO-thread hazard is a worker and the main thread both touching a client
field. Modelled in Rust, the borrow checker stops it before it runs:

```
error[E0373]: closure may outlive the current function, but it borrows `c.bufpos`
10 |         c.bufpos += 1;   // worker mutates shared state
   |         -------- `c.bufpos` is borrowed here
   = help: to force the closure to take ownership ... use the `move` keyword
```

To actually share mutable state across threads in Rust you must reach for
`Arc<Mutex<...>>` or an atomic type -- the compiler makes the synchronization
decision *explicit and mandatory*. In `io_threads.c` the same sharing is legal C;
its safety depends entirely on a human having placed every `_Atomic` and every
`atomic_thread_fence()` correctly, and a mistake is silent.

## Evidence 3 -- the logic is unit-testable without the server

`rust/valkey_net/src/resp_parse.rs` reimplements the multibulk-header parse as a
pure function of a `&[u8]`. Its tests run in **half a second with no server, no
gtest, no engine link**:

```
running 6 tests
test resp_parse::tests::empty_digits_do_not_read_out_of_bounds ... ok
test resp_parse::tests::rejects_overflow_instead_of_wrapping ... ok
... 6 passed; 0 failed ... finished in 0.00s
```

Two of those tests pin bug classes the C path has to get right by hand:
`empty_digits_do_not_read_out_of_bounds` (the `*\r\n` case where the digit
sub-slice is empty) and `rejects_overflow_instead_of_wrapping` (C's `ll*10`
can wrap silently; the Rust `checked_mul` returns `None`). Testing the C parser
equivalently means standing up a `client` and a query buffer inside the gtest
harness.

## Evidence 4 -- exhaustiveness: the compiler enforces every case is handled

The C read path communicates outcomes through a bitfield of ~24 `READ_FLAGS_*`
ints; a caller that forgets a flag compiles fine and misbehaves at runtime. The
Rust parser returns an `enum MultibulkHeader { Parsed | NeedMore | NonPositive |
Error }`, and `match` on it is checked for exhaustiveness -- add a variant and
every call site that doesn't handle it fails to compile.

## What Rust does NOT buy here (the honest other half)

- **Not free performance.** Benchmarked at parity, not faster: the Rust socket
  I/O does the same syscalls, and the Rust-owned IO threads run the same C job
  handlers. Rust's win here is safety and maintainability, not speed.
- **Not safety across the FFI boundary.** Every `extern "C"` fn that takes a
  pointer is an `unsafe` block; the guarantees start *after* we validate the
  pointer into a slice. The boundary itself is as unsafe as C, so the FFI
  surface must stay small (which is why we kept it to leaf operations).
- **A real cost: two toolchains and a build dependency.** The server now needs
  cargo; the 32-bit build had to fall back to C. A production move would have to
  weigh that against the safety gains.

## Bottom line for development

For *this* layer, Rust turns the two highest-severity bug classes in the code --
OOB buffer access and cross-thread data races -- from "silent, found in
production, assigned a CVE" into "the code does not compile." That is the
concrete development help: the feedback moves from a post-incident bug report to
a red squiggle in the editor. The price is a second toolchain and an unsafe FFI
seam that must be kept narrow.

## Reproduce

```
cd rust/valkey_net && cargo test          # Evidence 3 + 4
# Evidence 1 / 2: the two snippets above, saved as .rs files, `rustc` them.
```
