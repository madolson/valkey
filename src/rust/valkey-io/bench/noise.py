#!/usr/bin/env python3
"""Noise study: per-cell spread of identical runs.

  noise.py run1.csv run2.csv run3.csv [-o noise.csv]

Each input is an e2e.py cell CSV from the same binary and profile. For every
(config, test, pipeline) and metric, spread_pct = (max - min) / median * 100
over the runs' cell medians. compare.py uses the output as its per-cell
significance threshold."""
import argparse, csv, statistics

METRICS = ["rps", "p50_ms", "p99_ms", "main_cpu_pct", "main_ns_per_req", "io_cpu_pct", "proc_cpu_pct",
           "client_cpu_pct", "instr_per_req", "cycles_per_req"]


def load(path):
    with open(path) as f:
        return {(r["config"], r["test"], r["pipeline"]): r for r in csv.DictReader(f)}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs", nargs="+")
    ap.add_argument("-o", "--out", default="noise.csv")
    ap.add_argument("--median-out", help="also write a cell CSV of per-metric medians over the runs "
                                         "(a compare.py reference)")
    a = ap.parse_args()
    runs = [load(p) for p in a.runs]
    cells = [k for k in runs[0] if all(k in r for r in runs)]
    per_metric = {m: [] for m in METRICS}
    with open(a.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["config", "test", "pipeline", "metric", "median", "min", "max", "spread_pct"])
        for k in cells:
            for m in METRICS:
                vals = [float(r[k][m]) for r in runs if r[k].get(m, "") != ""]
                if len(vals) < 2:
                    continue
                med = statistics.median(vals)
                sp = 100.0 * (max(vals) - min(vals)) / med if med else 0.0
                per_metric[m].append(sp)
                w.writerow(list(k) + [m, round(med, 3), min(vals), max(vals), round(sp, 2)])
    if a.median_out:
        cols = list(runs[0][cells[0]].keys())
        with open(a.median_out, "w", newline="") as f:
            w = csv.DictWriter(f, cols)
            w.writeheader()
            for k in cells:
                row = dict(runs[0][k])
                row["label"] = "median-of-%d" % len(runs)
                for m in cols:
                    try:
                        vals = [float(r[k][m]) for r in runs if r[k].get(m, "") != ""]
                    except ValueError:
                        continue
                    if vals and m not in ("pipeline", "clients", "reps"):
                        row[m] = round(statistics.median(vals), 3)
                w.writerow(row)
    print("%-16s %8s %8s %8s  (spread %% over %d runs, %d cells)" % ("metric", "median", "p90", "max", len(runs),
                                                                    len(cells)))
    for m, v in per_metric.items():
        if v:
            v = sorted(v)
            print("%-16s %8.2f %8.2f %8.2f" % (m, statistics.median(v), v[int(0.9 * (len(v) - 1))], v[-1]))


main()
