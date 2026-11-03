#!/usr/bin/env python3
"""Per-cell deltas between two e2e.py cell CSVs.

  compare.py old.csv new.csv [--noise noise.csv] [--floor 1.0] [--metrics rps,p99_ms,...]

A delta is flagged '*' when |delta %| exceeds the noise threshold:
max(the cell's spread_pct for that metric, the metric's p90 spread_pct over
all cells, --floor), from noise.py output (default: baseline/noise.csv next to
this script). Three runs' range underestimates a cell's spread, hence the p90
term; --cell-only drops it. Cells without a noise entry use --default."""
import argparse, csv, os

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_METRICS = "rps,p50_ms,p99_ms,main_ns_per_req,main_cpu_pct,proc_cpu_pct,client_cpu_pct"


def load(path):
    with open(path) as f:
        return {(r["config"], r["test"], r["pipeline"]): r for r in csv.DictReader(f)}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("old")
    ap.add_argument("new")
    ap.add_argument("--noise", default=os.path.join(HERE, "baseline", "noise.csv"))
    ap.add_argument("--floor", type=float, default=1.0)
    ap.add_argument("--default", type=float, default=5.0)
    ap.add_argument("--metrics", default=DEFAULT_METRICS)
    ap.add_argument("--cell-only", action="store_true")
    a = ap.parse_args()
    old, new = load(a.old), load(a.new)
    noise = {}
    if os.path.exists(a.noise):
        with open(a.noise) as f:
            for r in csv.DictReader(f):
                noise[(r["config"], r["test"], r["pipeline"], r["metric"])] = float(r["spread_pct"])
        if not a.cell_only:
            by_m = {}
            for k, v in noise.items():
                by_m.setdefault(k[3], []).append(v)
            p90 = {m: sorted(v)[int(0.9 * (len(v) - 1))] for m, v in by_m.items()}
            noise = {k: max(v, p90[k[3]]) for k, v in noise.items()}
    else:
        print("no noise file at %s; threshold %.1f%%" % (a.noise, a.default))
    metrics = a.metrics.split(",")
    print("%-12s %-4s %3s  " % ("config", "test", "P") + "  ".join("%22s" % m for m in metrics))
    flagged = 0
    for k in sorted(set(old) & set(new), key=lambda k: (k[0], int(k[2]), k[1])):
        cols = []
        for m in metrics:
            o, n = old[k].get(m, ""), new[k].get(m, "")
            if o in ("", None) or n in ("", None) or float(o) == 0:
                cols.append("%22s" % "-")
                continue
            o, n = float(o), float(n)
            d = 100.0 * (n - o) / o
            th = max(noise.get(k + (m,), a.default), a.floor)
            mark = "*" if abs(d) > th else " "
            flagged += mark == "*"
            cols.append("%10.4g %+6.1f%%%s(%4.1f)" % (n, d, mark, th))
        print("%-12s %-4s %3s  " % k + "  ".join(cols))
    print("columns: value, delta %%, * = beyond noise, (threshold %%). %d flagged" % flagged)


main()
