# valkey-io measurement harness

Run from the repo root after `make BUILD_RUST_IO=yes`.

| Tool | Command |
|---|---|
| End to end | `python3 src/rust/valkey-io/bench/e2e.py --label <step> [--profile quick\|full] --out <step>.csv` |
| Compare | `python3 src/rust/valkey-io/bench/compare.py baseline/quick-base-1.csv <step>.csv` |
| Noise study | `python3 src/rust/valkey-io/bench/noise.py a.csv b.csv c.csv -o noise.csv` |
| Profile one thread | `src/rust/valkey-io/bench/profile.sh src/valkey-server <config> main\|io0 get\|set 16 <outdir>` |
| Stage timing | `make BUILD_RUST_IO=yes RUST_IO_TIMING=yes`, run with `RUST_IO_TIMING_N=<n>` (default 64), `INFO rust_io_timing`, reset with `CONFIG RESETSTAT`. `e2e.py --timing-dir d` saves one INFO dump per cell. |
| Micro-benchmarks | `cd src/rust/valkey-io/benches && cargo bench` |

Host assumptions: 8 cores, server pinned to 0-3, valkey-benchmark to 4-7 with
`--threads 4`. Edit `SERVER_CPUS`/`CLIENT_CPUS` in e2e.py for other hosts.

Configs: `c-ioT` is C with `io-threads T` (T counts the main thread);
`io-tN`, `precheck-tN`, `handoff-tN`, `rtc-tN` use `rust-io-threads N` (N does
not count the main thread). Compare `c-ioT` with `<rust>-t(T-1)`.

Why e2e.py measures over a server-side window: valkey-benchmark publishes
completions on a 250 ms timer (`SHOW_THROUGHPUT_INTERVAL`,
`src/valkey-benchmark.c:75`), so its RPS and its exit time are quantized to
250 ms (4% of a 6 s run). e2e.py reads `total_commands_processed` and
per-thread `schedstat` over an exact window while the load runs. Its `p50_ms`
and `p99_ms` are still valkey-benchmark's.

A change is significant when `compare.py` flags it: the delta exceeds the
per-cell spread of three identical runs (`baseline/noise.csv`).
