#!/usr/bin/env python3
"""Literal three-cell gate evaluator for the publication-v2 candidate."""

from __future__ import annotations

import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path


CELLS = (("process", 1), ("process", 100), ("group", 1000))
CYCLES = range(1, 6)
VARIANTS = ("control", "candidate")
EXPECTED_BPW = {
    ("process", 1): 40_000,
    ("process", 100): 2_500,
    ("group", 1000): 100,
}


def number(row: dict[str, str], field: str) -> float:
    return float(row[field])


def integer(row: dict[str, str], field: str) -> int:
    return int(row[field])


def median_ratio(
    cycles: dict[int, dict[str, list[dict[str, str]]]], field: str
) -> tuple[float, list[float]]:
    ratios = []
    for cycle in CYCLES:
        control = statistics.median(
            number(row, field) for row in cycles[cycle]["control"]
        )
        candidate = statistics.median(
            number(row, field) for row in cycles[cycle]["candidate"]
        )
        ratios.append(candidate / control)
    return statistics.median(ratios), ratios


def fail(errors: list[str], message: str) -> None:
    errors.append(message)


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: evaluate_focused.py <paired.csv>", file=sys.stderr)
        return 2
    path = Path(sys.argv[1])
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))

    errors: list[str] = []
    expected_rows = len(CELLS) * len(CYCLES) * 4
    if len(rows) != expected_rows:
        fail(errors, f"row count {len(rows)} != {expected_rows}")

    sources = defaultdict(set)
    binaries = defaultdict(set)
    degraded_rows = defaultdict(int)
    observed_order: list[tuple[str, int, int, int, str]] = []
    cells: dict[
        tuple[str, int], dict[int, dict[str, list[dict[str, str]]]]
    ] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for row in rows:
        try:
            variant = row["variant"]
            mode = row["mode"]
            batch = integer(row, "batch")
            cycle = integer(row, "cycle")
            slot = integer(row, "slot")
        except (KeyError, ValueError) as error:
            fail(errors, f"malformed row: {error}: {row}")
            continue
        if variant not in VARIANTS:
            fail(errors, f"unexpected variant {variant}")
        if (mode, batch) not in CELLS or cycle not in CYCLES:
            fail(errors, f"unexpected cell {mode}/b{batch}/cycle{cycle}")
        if slot not in (1, 2, 3, 4):
            fail(errors, f"unexpected slot {slot}")
        observed_order.append((mode, batch, cycle, slot, variant))
        expected_order = (
            ("control", "candidate", "candidate", "control")
            if cycle % 2 == 1
            else ("candidate", "control", "control", "candidate")
        )
        if slot in (1, 2, 3, 4) and variant != expected_order[slot - 1]:
            fail(
                errors,
                f"{mode}/b{batch}/c{cycle}/s{slot}: {variant} violates "
                "counterbalanced order",
            )
        sources[variant].add(row["source"])
        binaries[variant].add(row["binary_sha256"])
        cells[(mode, batch)][cycle][variant].append(row)

        events = integer(row, "events")
        writers = integer(row, "writers")
        bpw = integer(row, "bpw")
        appends = writers * bpw
        if writers != 4 or bpw != EXPECTED_BPW.get((mode, batch)):
            fail(
                errors,
                f"{mode}/b{batch}/c{cycle}/s{slot}: "
                f"unexpected writers/bpw {writers}/{bpw}",
            )
        if integer(row, "payload") != 250:
            fail(errors, f"{mode}/b{batch}/c{cycle}/s{slot}: bad payload")
        if events != batch * appends:
            fail(errors, f"{mode}/b{batch}/c{cycle}/s{slot}: event mismatch")
        if integer(row, "batches") != appends:
            fail(errors, f"{mode}/b{batch}/c{cycle}/s{slot}: batch mismatch")
        if row["fsync_degraded"] == "true":
            degraded_rows[(variant, mode, batch)] += 1
        elif row["fsync_degraded"] != "false":
            fail(errors, f"{mode}/b{batch}/c{cycle}/s{slot}: bad alarm value")
        if number(row, "pre_load1") >= 6.0:
            fail(errors, f"{mode}/b{batch}/c{cycle}/s{slot}: pre-load >= 6")
        if mode == "process" and integer(row, "fsyncs") != 0:
            fail(errors, f"{mode}/b{batch}/c{cycle}/s{slot}: nonzero fsync")
        if mode == "group" and integer(row, "fsyncs") != integer(row, "groups"):
            fail(errors, f"{mode}/b{batch}/c{cycle}/s{slot}: group/fsync mismatch")
        copied_records = events if batch in (1, 10) else 0
        copied_bytes = (
            events * (len("bench.event") + integer(row, "payload"))
            if copied_records
            else 0
        )
        exact = {
            "owned_batches": 0,
            "owned_records": 0,
            "owned_payload_bytes": 0,
            "borrowed_batches": appends,
            "borrowed_records": events,
            "copied_records": copied_records,
            "copied_bytes": copied_bytes,
        }
        for field, wanted in exact.items():
            if integer(row, field) != wanted:
                fail(
                    errors,
                    f"{mode}/b{batch}/c{cycle}/s{slot}: "
                    f"{variant} {field}={row[field]} != {wanted}",
                )

    for variant in VARIANTS:
        if len(sources[variant]) != 1 or "unknown" in sources[variant]:
            fail(errors, f"{variant} source provenance: {sources[variant]}")
        if len(binaries[variant]) != 1 or "unknown" in binaries[variant]:
            fail(errors, f"{variant} binary provenance: {binaries[variant]}")

    expected_order = []
    for mode, batch in CELLS:
        for cycle in CYCLES:
            variants = (
                ("control", "candidate", "candidate", "control")
                if cycle % 2 == 1
                else ("candidate", "control", "control", "candidate")
            )
            expected_order.extend(
                (mode, batch, cycle, slot, variant)
                for slot, variant in enumerate(variants, start=1)
            )
    if observed_order != expected_order:
        fail(errors, "physical CSV row sequence violates the focused protocol")

    print(
        "mode,batch,throughput_B_over_A,p99_B_over_A,allocs_B_over_A,"
        "bytes_B_over_A,fsync_B_minus_A,barriers_B_over_A,verdict"
    )
    for mode, batch in CELLS:
        cycles = cells[(mode, batch)]
        for cycle in CYCLES:
            for variant in VARIANTS:
                if len(cycles[cycle][variant]) != 2:
                    fail(
                        errors,
                        f"{mode}/b{batch}/cycle{cycle}/{variant}: "
                        f"{len(cycles[cycle][variant])} rows, expected 2",
                    )
            slots = {
                integer(row, "slot")
                for variant in VARIANTS
                for row in cycles[cycle][variant]
            }
            if slots != {1, 2, 3, 4}:
                fail(errors, f"{mode}/b{batch}/cycle{cycle}: slots={slots}")
        if any(len(cycles[c][v]) != 2 for c in CYCLES for v in VARIANTS):
            continue

        throughput, _ = median_ratio(cycles, "ev_s")
        p99, _ = median_ratio(cycles, "p99_us")
        allocs, _ = median_ratio(cycles, "allocs_per_event")
        alloc_bytes, _ = median_ratio(cycles, "alloc_bytes_per_event")
        fsync_deltas = []
        for cycle in CYCLES:
            control = statistics.median(
                integer(row, "fsyncs") for row in cycles[cycle]["control"]
            )
            candidate = statistics.median(
                integer(row, "fsyncs") for row in cycles[cycle]["candidate"]
            )
            fsync_deltas.append(candidate - control)
        fsync_delta = statistics.median(fsync_deltas)

        control_total = sum(
            integer(row, "fsyncs")
            for cycle in CYCLES
            for row in cycles[cycle]["control"]
        )
        candidate_total = sum(
            integer(row, "fsyncs")
            for cycle in CYCLES
            for row in cycles[cycle]["candidate"]
        )
        barrier_ratio = (
            candidate_total / control_total if control_total else 1.0
        )
        passed = (
            throughput >= 0.97
            and p99 <= 1.10
            and allocs <= 1.05
            and alloc_bytes <= 1.05
            and fsync_delta <= 0
        )
        if mode == "group":
            passed = (
                passed
                and control_total > 0
                and candidate_total > 0
                and candidate_total <= control_total * 1.0025
            )
        if not passed:
            fail(errors, f"gate failure: {mode}/b{batch}")
        print(
            f"{mode},{batch},{throughput:.6f},{p99:.6f},"
            f"{allocs:.6f},{alloc_bytes:.6f},{fsync_delta:.1f},"
            f"{barrier_ratio:.6f},{'PASS' if passed else 'FAIL'}"
        )

    if errors:
        print("\nFAIL", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("\nfsync-degraded rows (retained in timing medians):")
    for mode, batch in CELLS:
        print(
            f"{mode}/b{batch}: control="
            f"{degraded_rows[('control', mode, batch)]} candidate="
            f"{degraded_rows[('candidate', mode, batch)]}"
        )
    print("\nPASS: all literal publication-v2 focused gates satisfied")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
