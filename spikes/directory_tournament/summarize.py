#!/usr/bin/env python3
"""Fold tournament CSVs into per-dataset markdown tables + admission deltas
vs the incumbent (h0_hashmap_sip). Usage: summarize.py logs/foo.csv..."""
import csv
import sys
from collections import defaultdict

rows = []
for path in sys.argv[1:]:
    with open(path) as f:
        rows.extend(csv.DictReader(f))

by_ds = defaultdict(list)
for r in rows:
    by_ds[r["dataset"]].append(r)


def f(r, k):
    return float(r[k])


def fmt_bytes(b):
    b = float(b)
    if b >= 1 << 20:
        return f"{b / (1 << 20):.1f}M"
    if b >= 1 << 10:
        return f"{b / (1 << 10):.1f}K"
    return f"{b:.0f}"


for ds in by_ds:
    rs = by_ds[ds]
    inc = next((r for r in rs if r["cand"] == "h0_hashmap_sip"), None)
    n = rs[0]["n"]
    un = rs[0]["u_over_n"]
    print(f"\n### {ds} (n={n}, U/n={un})\n")
    print(
        "| cand | build p50 | open p50 | ser B/key | res B/key | warm hit p50 | miss p50 | batch32 ns/op | serial ns/op | cold p50 | vs h0 (batch) | vs h0 (serial) | vs h0 (cold) |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rs:
        nn = int(r["n"])
        d_batch = d_serial = d_cold = ""
        if inc and r is not inc:
            d_batch = f'{(1 - f(r, "batch32_ns_per_op") / f(inc, "batch32_ns_per_op")) * 100:+.0f}%'
            d_serial = f'{(1 - f(r, "serial_ns_per_op") / f(inc, "serial_ns_per_op")) * 100:+.0f}%'
            if f(inc, "coldish_p50_ns") > 0:
                d_cold = f'{(1 - f(r, "coldish_p50_ns") / f(inc, "coldish_p50_ns")) * 100:+.0f}%'
        print(
            f'| {r["cand"]} | {f(r, "build_p50_us"):.0f}us | {f(r, "open_p50_us"):.0f}us '
            f'| {f(r, "ser_bytes") / nn:.1f} | {f(r, "res_bytes") / nn:.1f} '
            f'| {f(r, "warm_hit_p50_ns"):.0f}ns | {f(r, "warm_miss_p50_ns"):.0f}ns '
            f'| {f(r, "batch32_ns_per_op"):.2f} | {f(r, "serial_ns_per_op"):.2f} '
            f'| {f(r, "coldish_p50_ns"):.0f}ns | {d_batch} | {d_serial} | {d_cold} |'
        )
