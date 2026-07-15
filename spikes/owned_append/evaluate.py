#!/usr/bin/env python3
"""Literal gate evaluator for the Process-only bn-2yye successor."""

from __future__ import annotations

import csv
import hashlib
import io
import math
import re
import statistics
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


MODES = ("process",)
BATCHES = (1, 10, 100, 1000)
CYCLES = range(1, 6)
VARIANTS = ("control", "candidate")
WRITERS = 4
PAYLOAD = 250
PROCESS_BPW = {1: 40_000, 10: 12_500, 100: 2_500, 1000: 250}
THROUGHPUT_FLOOR = 0.97
P99_CEILING = 1.10
ALLOCATION_CEILING = 1.05
MATERIAL_THROUGHPUT_FLOOR = 1.10
MATERIAL_ALLOCATION_CEILING = 0.90
PROTOCOL = "bn-2yye-process-successor-v1"
SHA256 = re.compile(r"[0-9a-f]{64}")
COMMIT = re.compile(r"[0-9a-f]{40}")
CSV_FIELDS = (
    "variant",
    "source",
    "binary_sha256",
    "cycle",
    "slot",
    "mode",
    "batch",
    "writers",
    "bpw",
    "payload",
    "events",
    "ev_s",
    "p50_us",
    "p99_us",
    "allocs",
    "alloc_bytes",
    "allocs_per_event",
    "alloc_bytes_per_event",
    "owned_batches",
    "owned_records",
    "owned_payload_bytes",
    "borrowed_batches",
    "borrowed_records",
    "copied_records",
    "copied_bytes",
    "batches",
    "groups",
    "fsyncs",
    "fsync_p99_ns",
    "fsync_degraded",
    "pre_load1",
    "post_load1",
)
PROVENANCE_FIELDS = (
    "protocol",
    "control_source",
    "candidate_source",
    "control_dirty",
    "candidate_dirty",
    "control_root",
    "candidate_root",
    "control_binary_path",
    "candidate_binary_path",
    "control_harness_path",
    "candidate_harness_path",
    "control_cargo_lock_path",
    "candidate_cargo_lock_path",
    "runner_path",
    "evaluator_path",
    "control_harness_sha256",
    "candidate_harness_sha256",
    "control_binary_sha256",
    "candidate_binary_sha256",
    "control_cargo_lock_sha256",
    "candidate_cargo_lock_sha256",
    "runner_sha256",
    "evaluator_sha256",
    "paired_csv_path",
    "paired_csv_sha256",
    "paired_csv_data_rows",
    "paired_csv_columns",
)


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


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_provenance(path: Path, errors: list[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    wanted = set(PROVENANCE_FIELDS)
    try:
        lines = path.read_text().splitlines()
    except OSError as error:
        fail(errors, f"cannot read provenance {path}: {error}")
        return values
    for line in lines:
        key, separator, value = line.partition("=")
        if not separator or key not in wanted:
            continue
        if key in values:
            fail(errors, f"duplicate provenance field {key}")
        else:
            values[key] = value
    for key in PROVENANCE_FIELDS:
        if key not in values:
            fail(errors, f"missing provenance field {key}")
    return values


def git_value(root: Path, args: list[str], errors: list[str]) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), *args],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        fail(errors, f"git {' '.join(args)} failed for {root}: {error}")
        return ""
    return result.stdout.strip()


def validate_provenance(
    values: dict[str, str],
    csv_path: Path,
    csv_digest: str,
    errors: list[str],
) -> None:
    if values["protocol"] != PROTOCOL:
        fail(errors, f"protocol {values['protocol']!r} != {PROTOCOL!r}")
    for variant in VARIANTS:
        if values[f"{variant}_dirty"] != "false":
            fail(errors, f"{variant} provenance is dirty")
        if not COMMIT.fullmatch(values[f"{variant}_source"]):
            fail(errors, f"{variant} source is not a full commit id")
        if not SHA256.fullmatch(values[f"{variant}_binary_sha256"]):
            fail(errors, f"{variant} binary hash is malformed")

    control_root = Path(values["control_root"]).resolve()
    candidate_root = Path(values["candidate_root"]).resolve()
    evaluator = Path(__file__).resolve()
    runner = evaluator.with_name("run_paired.sh")
    expected_paths = {
        "control_binary_path": control_root
        / "target/release/examples/owned_append_bench",
        "candidate_binary_path": candidate_root
        / "target/release/examples/owned_append_bench",
        "control_harness_path": control_root
        / "crates/mess-store/examples/owned_append_bench.rs",
        "candidate_harness_path": candidate_root
        / "crates/mess-store/examples/owned_append_bench.rs",
        "control_cargo_lock_path": control_root / "Cargo.lock",
        "candidate_cargo_lock_path": candidate_root / "Cargo.lock",
        "runner_path": runner,
        "evaluator_path": evaluator,
        "paired_csv_path": csv_path.resolve(),
    }
    for key, expected in expected_paths.items():
        observed = Path(values[key]).resolve()
        if observed != expected:
            fail(errors, f"{key}={observed} != {expected}")

    hashed_paths = {
        "control_binary_sha256": expected_paths["control_binary_path"],
        "candidate_binary_sha256": expected_paths["candidate_binary_path"],
        "control_harness_sha256": expected_paths["control_harness_path"],
        "candidate_harness_sha256": expected_paths["candidate_harness_path"],
        "control_cargo_lock_sha256": expected_paths[
            "control_cargo_lock_path"
        ],
        "candidate_cargo_lock_sha256": expected_paths[
            "candidate_cargo_lock_path"
        ],
        "runner_sha256": runner,
        "evaluator_sha256": evaluator,
    }
    for key, path in hashed_paths.items():
        try:
            observed = sha256(path)
        except OSError as error:
            fail(errors, f"cannot hash {path}: {error}")
            continue
        if observed != values[key]:
            fail(errors, f"{key}={values[key]} != current {observed}")
    if csv_digest != values["paired_csv_sha256"]:
        fail(
            errors,
            f"paired_csv_sha256={values['paired_csv_sha256']} != "
            f"current {csv_digest}",
        )

    if values["control_harness_sha256"] != values[
        "candidate_harness_sha256"
    ]:
        fail(errors, "control and candidate harness hashes differ")
    if values["control_cargo_lock_sha256"] != values[
        "candidate_cargo_lock_sha256"
    ]:
        fail(errors, "control and candidate Cargo.lock hashes differ")
    if values["control_source"] == values["candidate_source"]:
        fail(errors, "control and candidate source commits are identical")
    if values["control_binary_sha256"] == values[
        "candidate_binary_sha256"
    ]:
        fail(errors, "control and candidate binary hashes are identical")

    for variant, root in (
        ("control", control_root),
        ("candidate", candidate_root),
    ):
        head = git_value(root, ["rev-parse", "HEAD"], errors)
        if head != values[f"{variant}_source"]:
            fail(errors, f"{variant} HEAD {head} != frozen source")
        dirty = git_value(root, ["status", "--porcelain"], errors)
        if dirty:
            fail(errors, f"{variant} worktree is currently dirty")


def geometric_mean(values: list[float]) -> float:
    if not values or any(value <= 0 for value in values):
        raise ValueError("geometric mean requires positive values")
    return math.exp(sum(math.log(value) for value in values) / len(values))


def physical_order() -> list[tuple[str, int, int, int, str]]:
    order = []
    for batch in BATCHES:
        for cycle in CYCLES:
            variants = (
                ("control", "candidate", "candidate", "control")
                if cycle % 2 == 1
                else ("candidate", "control", "control", "candidate")
            )
            for slot, variant in enumerate(variants, start=1):
                order.append(("process", batch, cycle, slot, variant))
    return order


def main() -> int:
    if len(sys.argv) != 3:
        print(
            "usage: evaluate.py <paired.csv> <provenance.txt>",
            file=sys.stderr,
        )
        return 2
    path = Path(sys.argv[1]).resolve()
    provenance_path = Path(sys.argv[2]).resolve()
    errors: list[str] = []
    provenance = read_provenance(provenance_path, errors)
    if errors:
        print("FAIL", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    try:
        csv_bytes = path.read_bytes()
    except OSError as error:
        fail(errors, f"cannot read CSV {path}: {error}")
        csv_bytes = b""
    csv_digest = hashlib.sha256(csv_bytes).hexdigest()
    validate_provenance(provenance, path, csv_digest, errors)

    try:
        csv_text = csv_bytes.decode()
        reader = csv.DictReader(io.StringIO(csv_text, newline=""))
        fields = tuple(reader.fieldnames or ())
        rows = list(reader)
    except UnicodeDecodeError as error:
        fail(errors, f"CSV is not UTF-8: {error}")
        rows = []
        fields = ()
    if fields != CSV_FIELDS:
        fail(errors, f"CSV fields {fields} != frozen schema")

    expected_order = physical_order()
    expected_rows = len(expected_order)
    if len(rows) != expected_rows:
        fail(errors, f"row count {len(rows)} != {expected_rows}")
    try:
        provenance_rows = int(provenance["paired_csv_data_rows"])
        provenance_columns = int(provenance["paired_csv_columns"])
    except ValueError as error:
        fail(errors, f"non-integer CSV cardinality in provenance: {error}")
    else:
        if provenance_rows != len(rows) or provenance_rows != expected_rows:
            fail(
                errors,
                f"provenance rows {provenance_rows} != {len(rows)} "
                f"!= {expected_rows}",
            )
        if provenance_columns != len(fields) or len(fields) != len(CSV_FIELDS):
            fail(
                errors,
                f"provenance columns {provenance_columns} != "
                f"{len(fields)} != {len(CSV_FIELDS)}",
            )

    sources = defaultdict(set)
    binaries = defaultdict(set)
    cells: dict[
        tuple[str, int], dict[int, dict[str, list[dict[str, str]]]]
    ] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for ordinal, row in enumerate(rows):
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
        if mode not in MODES or batch not in BATCHES or cycle not in CYCLES:
            fail(errors, f"unexpected cell {mode}/{batch}/cycle{cycle}")
        if slot not in (1, 2, 3, 4):
            fail(errors, f"unexpected slot {slot}")
        if ordinal < len(expected_order):
            expected = expected_order[ordinal]
            observed = (mode, batch, cycle, slot, variant)
            if observed != expected:
                fail(
                    errors,
                    f"row {ordinal + 1}: physical order {observed} != "
                    f"{expected}",
                )
        expected_variants = (
            ("control", "candidate", "candidate", "control")
            if cycle % 2 == 1
            else ("candidate", "control", "control", "candidate")
        )
        if slot in (1, 2, 3, 4) and variant != expected_variants[slot - 1]:
            fail(
                errors,
                f"{mode}/b{batch}/c{cycle}/s{slot}: {variant} violates "
                f"counterbalanced order",
            )
        sources[variant].add(row["source"])
        binaries[variant].add(row["binary_sha256"])
        cells[(mode, batch)][cycle][variant].append(row)

        if integer(row, "writers") != WRITERS:
            fail(errors, f"{mode}/b{batch}/c{cycle}/s{slot}: writers != 4")
        wanted_bpw = PROCESS_BPW.get(batch)
        if wanted_bpw is not None and integer(row, "bpw") != wanted_bpw:
            fail(
                errors,
                f"{mode}/b{batch}/c{cycle}/s{slot}: "
                f"bpw={row['bpw']} != {wanted_bpw}",
            )
        if integer(row, "payload") != PAYLOAD:
            fail(errors, f"{mode}/b{batch}/c{cycle}/s{slot}: payload != 250")
        appends = WRITERS * (wanted_bpw or 0)
        wanted_events = batch * appends
        events = integer(row, "events")
        if events != wanted_events:
            fail(
                errors,
                f"{mode}/b{batch}/c{cycle}/s{slot}: "
                f"events={events} != {wanted_events}",
            )
        if integer(row, "batches") != appends:
            fail(errors, f"{mode}/b{batch}/c{cycle}/s{slot}: batch mismatch")
        if row["fsync_degraded"] != "false":
            fail(errors, f"{mode}/b{batch}/c{cycle}/s{slot}: fsync degraded")
        if number(row, "pre_load1") >= 6.0:
            fail(errors, f"{mode}/b{batch}/c{cycle}/s{slot}: pre-load >= 6")
        if integer(row, "fsyncs") != 0:
            fail(errors, f"{mode}/b{batch}/c{cycle}/s{slot}: nonzero fsync")
        if variant == "candidate":
            exact = {
                "owned_batches": appends,
                "owned_records": events,
                "owned_payload_bytes": events * integer(row, "payload"),
                "borrowed_batches": 0,
                "borrowed_records": 0,
                "copied_records": 0,
                "copied_bytes": 0,
            }
            for field, wanted in exact.items():
                if integer(row, field) != wanted:
                    fail(
                        errors,
                        f"{mode}/b{batch}/c{cycle}/s{slot}: "
                        f"candidate {field}={row[field]} != {wanted}",
                    )

    for variant in VARIANTS:
        expected_source = {provenance[f"{variant}_source"]}
        if sources[variant] != expected_source:
            fail(
                errors,
                f"{variant} CSV sources {sources[variant]} != "
                f"{expected_source}",
            )
        expected_binary = {provenance[f"{variant}_binary_sha256"]}
        if binaries[variant] != expected_binary:
            fail(
                errors,
                f"{variant} CSV binaries {binaries[variant]} != "
                f"{expected_binary}",
            )

    print("mode,batch,throughput_B_over_A,p99_B_over_A,allocs_B_over_A,bytes_B_over_A,fsync_B_minus_A,verdict")
    throughput_cells: list[float] = []
    allocation_cells: list[float] = []
    allocation_byte_cells: list[float] = []
    for mode in MODES:
        for batch in BATCHES:
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
            throughput_cells.append(throughput)
            allocation_cells.append(allocs)
            allocation_byte_cells.append(alloc_bytes)
            fsync_deltas = []
            for cycle in CYCLES:
                control = statistics.median(
                    integer(row, "fsyncs")
                    for row in cycles[cycle]["control"]
                )
                candidate = statistics.median(
                    integer(row, "fsyncs")
                    for row in cycles[cycle]["candidate"]
                )
                fsync_deltas.append(candidate - control)
            fsync_delta = statistics.median(fsync_deltas)

            passed = (
                throughput >= THROUGHPUT_FLOOR
                and p99 <= P99_CEILING
                and allocs <= ALLOCATION_CEILING
                and alloc_bytes <= ALLOCATION_CEILING
                and fsync_delta == 0
            )
            if not passed:
                fail(errors, f"gate failure: {mode}/b{batch}")
            print(
                f"{mode},{batch},{throughput:.6f},{p99:.6f},"
                f"{allocs:.6f},{alloc_bytes:.6f},{fsync_delta:.1f},"
                f"{'PASS' if passed else 'FAIL'}"
            )

    if (
        len(throughput_cells) == len(BATCHES)
        and len(allocation_cells) == len(BATCHES)
        and len(allocation_byte_cells) == len(BATCHES)
    ):
        throughput_material = geometric_mean(throughput_cells)
        allocation_material = geometric_mean(allocation_cells)
        allocation_byte_material = geometric_mean(allocation_byte_cells)
        material_passed = (
            throughput_material >= MATERIAL_THROUGHPUT_FLOOR
            and allocation_material <= MATERIAL_ALLOCATION_CEILING
            and allocation_byte_material <= MATERIAL_ALLOCATION_CEILING
        )
        print(
            "process,all,"
            f"{throughput_material:.6f},n/a,{allocation_material:.6f},"
            f"{allocation_byte_material:.6f},0.0,"
            f"{'PASS' if material_passed else 'FAIL'}"
        )
        if not material_passed:
            fail(errors, "gate failure: Process material geometric means")
    else:
        fail(errors, "cannot compute Process material geometric means")

    if errors:
        print("\nFAIL", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("\nPASS: all literal owned-append gates satisfied")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
