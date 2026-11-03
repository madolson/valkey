#!/usr/bin/env python3
"""Markdown table of INFO rust_io_timing dumps (e2e.py --timing-dir).

  stages.py <dir> [cell ...]     cell = file stem, e.g. precheck-t1-get-P16"""
import os, sys

d = sys.argv[1]
cells = sys.argv[2:] or sorted(f[:-4] for f in os.listdir(d) if f.endswith(".txt"))
for cell in cells:
    rows = []
    with open(os.path.join(d, cell + ".txt")) as f:
        for line in f:
            line = line.strip()
            if line.startswith("stage_"):
                name, kv = line[6:].split(":", 1)
                v = dict(x.split("=") for x in kv.split(","))
                if int(v["count"]):
                    rows.append((name, v))
    if not rows:
        continue
    print("\n%s\n\n| stage (since previous) | count | p50 ns | p90 ns | p99 ns | max ns |\n|---|---|---|---|---|---|" % cell)
    for name, v in rows:
        print("| %s | %s | %s | %s | %s | %s |" % (name, v["count"], v["p50"], v["p90"], v["p99"], v["max"]))
