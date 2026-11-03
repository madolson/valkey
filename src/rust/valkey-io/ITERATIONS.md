# Iteration 1: toward a modular IO boundary, one measured step at a time

Plan: `~/rust-io/iter1/api-analysis.md` section 5 (steps referenced by number).
Base: `rust-io/integrate` 0f61a6fb9 + compat fixes B1 B2 B4 B5 (cherry-picked,
B3 dropped) + `rust-io/bench` (stage 6a harness). Stage 5a (`rust-io/harden`)
did not exist when this ran, so plan steps 4, 13 and 18 were treated as not
done.

Host: 32-core cloud desktop, kernel 5.10. Harness defaults: server on cores
0-3, valkey-benchmark on 4-7 (`--threads 4`), `-c 200 -d 64 -r 1000000`,
quick profile (c-io2, c-io4, io-t1, io-t3, precheck-t1, precheck-t3 x
SET/GET x -P 1/-P 16), 3 reps, median. Gates and builds ran on cores 8-31,
never during a measurement.

Significance: a delta counts only if it exceeds the per-cell spread of three
identical quick runs of the base binary on this host (`noise` below,
`compare.py`).

Metrics columns: exports = `#[no_mangle]` functions (timing-feature exports
in parentheses), cb = C callbacks in `VioCallbacks`, ops = entries in
engine-side function tables (`VioExecOps` before step 2, `externalConnOps`
after step 12), structs = `#[repr(C)]` structs, unsafe = `unsafe` tokens
outside `ffi/` and `sys.rs` (non-test code), statics = `static` items in the
crate (including thread-locals), ifdef = engine .c/.h files (excluding
`rust_io*`) containing `USE_RUST_IO`, bridge = lines in `rust_io.c` +
`rust_io_precheck.c` (+ `rust_io_exec.c`, `engine_io.c` when present),
clippy = `cargo clippy --all-targets` warnings.

| Step | Commit | Change | exports | cb | ops | structs | unsafe | statics | ifdef | bridge | clippy | Perf vs previous (key cells) | Gates | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
