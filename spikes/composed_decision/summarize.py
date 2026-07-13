#!/usr/bin/env python3
"""composed_results.csv -> per-cell median/best tables with vs-bare and vs-log
ratios. Median-of-3 AND best-of-3 are reported separately (research/05 §15.2).

    python3 summarize.py [matrix|newname|convoy|seal]
"""
import csv
import sys
from collections import defaultdict

ENGINES = ["bare", "log", "logsp", "b0d", "b0dth"]


def load():
    rows = defaultdict(lambda: defaultdict(list))
    meta = {}
    with open("composed_results.csv") as f:
        for r in csv.DictReader(f):
            cell = r["cell"]
            rows[cell][r["engine"]].append(r)
            meta[cell] = r
    return rows, meta


def med(vals):
    v = sorted(vals)
    return v[len(v) // 2]


def summarize(prefix):
    rows, meta = load()
    cells = [c for c in rows if c.startswith(prefix)]
    if not cells:
        return
    engines = [e for e in ENGINES if any(e in rows[c] for c in cells)]
    hdr = ["cell", "batch", "payload", "w"]
    for e in engines:
        hdr += [f"{e} med", f"{e} %bare", f"{e} %log", f"{e} fsync"]
    print("| " + " | ".join(hdr) + " |")
    print("|" + "|".join("---" for _ in hdr) + "|")
    for c in sorted(
        cells,
        key=lambda c: (
            int(meta[c]["payload"]),
            int(meta[c]["batch"]),
            int(meta[c]["writers"]),
        ),
    ):
        m = meta[c]
        line = [c, m["batch"], m["payload"], m["writers"]]
        base = {}
        for e in engines:
            if e in rows[c]:
                base[e] = med([float(x["ev_s"]) for x in rows[c][e]])
        for e in engines:
            if e not in rows[c]:
                line += ["-", "-", "-", "-"]
                continue
            v = base[e]
            vb = f"{100*v/base['bare']:.1f}%" if "bare" in base else "-"
            vl = f"{100*v/base['log']:.1f}%" if "log" in base else "-"
            fs = med([int(x["fsyncs"]) for x in rows[c][e]])
            line += [f"{v:,.0f}", vb, vl, str(fs)]
        print("| " + " | ".join(line) + " |")


def loadcheck():
    """Prove the A/B/B/A interleave gave every engine the same ambient load."""
    rows, _ = load()
    per = defaultdict(list)
    for c in rows:
        for e in rows[c]:
            for r in rows[c][e]:
                if "load1" in r and r["load1"]:
                    per[e].append(float(r["load1"]))
    print("\nengine | n | mean load1 | min | max")
    print("---|---|---|---|---")
    for e in ENGINES:
        if per[e]:
            v = per[e]
            print(
                f"{e} | {len(v)} | {sum(v)/len(v):.2f} | {min(v):.2f} | {max(v):.2f}"
            )


if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else "matrix"
    if what == "load":
        loadcheck()
        sys.exit(0)
    if what == "matrix":
        for p in ("proc-", "group-"):
            print(f"\n### {p}\n")
            summarize(p)
    else:
        summarize({"newname": "newname-", "convoy": "convoy-", "seal": "seal-"}[what])
