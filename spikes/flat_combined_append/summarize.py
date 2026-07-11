#!/usr/bin/env python3
"""Summarize flat_combined_results.csv into per-cell best/median tables
with ratios against bare and log (Spike B report input)."""

import csv
import statistics
import sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "flat_combined_results.csv"
rows = list(csv.DictReader(open(path)))

cells = defaultdict(lambda: defaultdict(list))
for r in rows:
    cells[r["cell"]][r["engine"]].append(r)

ORDER = ["bare", "log", "b0", "b1", "b0d", "b0dth"]

for cell in cells:
    engines = cells[cell]
    print(f"\n== {cell}")
    stats = {}
    for e in ORDER:
        if e not in engines:
            continue
        evs = sorted(float(r["ev_s"]) for r in engines[e])
        best = evs[-1]
        med = statistics.median(evs)
        # p99 / fsync stats from the best-ev/s rep
        bestrow = max(engines[e], key=lambda r: float(r["ev_s"]))
        stats[e] = (best, med, bestrow)
    bare = stats.get("bare", (None,))[0]
    barem = stats.get("bare", (None, None))[1]
    log = stats.get("log", (None,))[0]
    logm = stats.get("log", (None, None))[1]
    for e in ORDER:
        if e not in stats:
            continue
        best, med, br = stats[e]
        vb = f"{best / bare * 100:5.1f}%" if bare else "  -  "
        vbm = f"{med / barem * 100:5.1f}%" if barem else "  -  "
        vl = f"{best / log * 100:5.1f}%" if log else "  -  "
        print(
            f"  {e:<5} best {best:>10.0f} ev/s ({vb} bare, {vl} log)  "
            f"median {med:>10.0f} ({vbm} bare-med)  "
            f"p50 {float(br['p50_us']):>8.1f}us p99 {float(br['p99_us']):>9.1f}us  "
            f"fsyncs {br['fsyncs']:>5} mean_fsync {float(br['mean_fsync_us']):>8.1f}us"
        )
