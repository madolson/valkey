# Baseline: rust-io/integrate at 0f61a6fb9

Binary: `make BUILD_RUST_IO=yes` at 0f61a6fb9 (the integrate branch HEAD).
Host: 8-vCPU KVM guest, 15 GB, kernel 5.10, gcc 7.3.1, rustc 1.95,
TSC constant_tsc + nonstop_tsc, clocksource tsc. No PMU in the guest
(`perf stat -e instructions` reports `<not supported>`), so instr/cycles
columns are empty and profiles use cpu-clock. Shared host: other agents and a
BPF-based security agent run alongside (see `foreign_cpu_pct`, and the
`bpf_*` frames in the IO-thread profiles).

Method (`../e2e.py --profile quick`): server on cores 0-3, valkey-benchmark on
4-7 with `--threads 4`, `-c 200 -d 64 -r 1000000`. Per config the cells run
SET then GET, `-P 1` then `-P 16`. Each cell: 2 s calibration, then 3 runs,
each measured over a 5 s window after 1 s of load; the cell row is the median.
RPS is `total_commands_processed` over the window, CPU is schedstat over the
window.

Files:
- `quick-base-{1,2,3}.csv` (+ `.raw.csv`): three identical quick runs.
- `quick-base-3r.csv`: run 3 with `precheck-t3 get P1` replaced by
  `recheck.csv`. In run 3 that cell lost 20% RPS in reps 2-3 while server and
  client CPU both fell, which points to host interference (main ns/req was
  flat). `noise.csv` and `base-median.csv` use runs 1, 2, 3r.
- `noise.csv`: per cell and metric, spread % = (max - min) / median over
  the three runs. `compare.py` reads it as the significance threshold.
- `base-median.csv`: per-metric median of the three runs; the reference
  for `compare.py`.
- `quick-timing.csv`, `timing-overhead.txt`: the same profile on a
  `RUST_IO_TIMING=yes` build (N=64), compared with `base-median.csv`.
- `timing-n64/`: `INFO rust_io_timing` per cell; `stages.md` as tables.
- `prof/`: perf profiles at `-P 16` (top self symbols, grouped breakdown,
  flamegraphs), `per-request.md` = group share x thread CPU ns/req.
