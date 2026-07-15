#!/usr/bin/env python3
"""Fail-closed selector and admission evaluator for bn-1zv6."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

import run_matrix as matrix


class InvalidEvidence(RuntimeError):
    pass


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def integer(row: dict[str, str], field: str) -> int:
    try:
        return int(row[field])
    except (KeyError, ValueError) as error:
        raise InvalidEvidence(f"invalid integer {field}: {row.get(field)!r}") from error


def number(row: dict[str, str], field: str) -> float:
    try:
        value = float(row[field])
    except (KeyError, ValueError) as error:
        raise InvalidEvidence(f"invalid number {field}: {row.get(field)!r}") from error
    if not math.isfinite(value):
        raise InvalidEvidence(f"non-finite {field}")
    return value


def geometric_mean(values: Iterable[float], weights: Iterable[int] | None = None) -> float:
    values = list(values)
    if not values or any(value <= 0 or not math.isfinite(value) for value in values):
        raise InvalidEvidence("geometric mean requires finite positive values")
    weight_values = list(weights) if weights is not None else [1] * len(values)
    if len(weight_values) != len(values) or any(weight <= 0 for weight in weight_values):
        raise InvalidEvidence("invalid geometric weights")
    return math.exp(
        sum(weight * math.log(value) for value, weight in zip(values, weight_values))
        / sum(weight_values)
    )


def median(values: Iterable[float]) -> float:
    values = list(values)
    if not values:
        raise InvalidEvidence("empty median")
    return statistics.median(values)


def constant_provenance(all_rows: list[dict[str, str]]) -> dict[str, str]:
    if not all_rows:
        raise InvalidEvidence("no evidence rows")
    result = {}
    immutable_fields = set(matrix.ROW_PROVENANCE) - {
        "captured_at_utc",
        "free_bytes",
    }
    for field in sorted(immutable_fields):
        values = {row.get(field, "") for row in all_rows}
        if len(values) != 1 or "" in values:
            raise InvalidEvidence(f"provenance field is not constant: {field}")
        result[field] = values.pop()
    if result["source_dirty"] != "false":
        raise InvalidEvidence("source_dirty must be false")
    return result


def validate_manifest(
    evidence: list[dict[str, str]], manifest: list[dict[str, str]]
) -> None:
    manifest_ids = [row["point_id"] for row in manifest]
    evidence_ids = [row["point_id"] for row in evidence]
    if len(manifest_ids) != len(set(manifest_ids)):
        raise InvalidEvidence("duplicate manifest point_id")
    if len(evidence_ids) != len(set(evidence_ids)):
        raise InvalidEvidence("duplicate evidence point_id")
    if set(manifest_ids) != set(evidence_ids):
        raise InvalidEvidence("manifest/evidence point set mismatch")
    by_id = {row["point_id"]: row for row in manifest}
    for row in evidence:
        planned = by_id[row["point_id"]]
        for field in planned:
            if row.get(field) != planned[field]:
                raise InvalidEvidence(
                    f"manifest axis mismatch {row['point_id']} field={field}"
                )


def validate_predeclared_manifest(
    actual: list[dict[str, str]], expected: list[dict[str, object]]
) -> None:
    normalized_expected = [
        {field: str(value) for field, value in row.items()} for row in expected
    ]
    if actual != normalized_expected:
        raise InvalidEvidence("manifest differs from regenerated predeclared matrix")


def validate_attempt_protocol(output: Path, provenance: dict[str, str]) -> None:
    try:
        document = json.loads((output / "attempt_protocol.json").read_text())
    except (OSError, ValueError) as error:
        raise InvalidEvidence(f"invalid Attempt-2 protocol artifact: {error}") from error
    expected = {
        "schema": "pwritev_group.attempt_protocol.v1",
        "attempt": 2,
        "source_commit": provenance["source_commit"],
        "absolute_timeout_seconds": matrix.ATTEMPT_TIMEOUT_SECONDS,
        "prior_attempt_classification": "INCONCLUSIVE_INFRASTRUCTURE",
        "prior_attempt_evidence": (
            "spikes/pwritev_group/evidence/attempt1-inconclusive-infrastructure"
        ),
        "prior_attempt_timing_rows_reused": False,
        "post_row_noise": "retain_and_continue_after_next_pre_row_guard",
        "row_retry_or_replacement": False,
        "stage1_physical_comparison_clean_rows_required": 4,
        "stage2_adjacent_clean_pairs_required_for_adopt": 6,
    }
    observed = dict(document)
    started = observed.pop("started_at_utc", None)
    if observed != expected:
        raise InvalidEvidence("Attempt-2 protocol artifact differs from frozen policy")
    try:
        captured = datetime.fromisoformat(started)
    except (TypeError, ValueError) as error:
        raise InvalidEvidence("Attempt-2 protocol start timestamp is invalid") from error
    if captured.tzinfo is None:
        raise InvalidEvidence("Attempt-2 protocol start timestamp lacks timezone")


def validate_stop(
    row: dict[str, str], group_bytes: int, threshold_bytes: int, threshold_ms: int
) -> None:
    written = integer(row, "bytes")
    wall_ns = integer(row, "wall_ns")
    reason = row["stop_reason"]
    if written > matrix.MAX_CORPUS_BYTES:
        raise InvalidEvidence("point exceeds 2GiB cap")
    if reason == "bytes":
        if written < threshold_bytes:
            raise InvalidEvidence("bytes stop below phase threshold")
    elif reason == "time":
        if written >= threshold_bytes or wall_ns < threshold_ms * 1_000_000:
            raise InvalidEvidence("invalid time stop")
    elif reason == "cap":
        if written >= threshold_bytes or wall_ns >= threshold_ms * 1_000_000:
            raise InvalidEvidence("cap stop occurred after another threshold")
        if written + group_bytes <= matrix.MAX_CORPUS_BYTES:
            raise InvalidEvidence("cap stop without next-group overflow")
    else:
        raise InvalidEvidence(f"unexpected stop reason {reason}")


def validate_row_environment(row: dict[str, str], label: str) -> bool:
    status = row.get("status")
    error = row.get("error", "")
    if status == "accepted":
        if error:
            raise InvalidEvidence(f"{label} accepted row has an error")
    elif status == "rejected_noisy":
        if not error.startswith("post-row load"):
            raise InvalidEvidence(f"{label} noisy row lacks load rejection reason")
    else:
        raise InvalidEvidence(f"{label} has invalid terminal status {status!r}")
    if integer(row, "bytes") <= 0:
        raise InvalidEvidence(f"{label} has nonpositive measured bytes")
    if integer(row, "cpu_ns") <= 0 or integer(row, "wall_ns") <= 0:
        raise InvalidEvidence(f"{label} has nonpositive measured time")
    if integer(row, "free_bytes") < matrix.MIN_FREE_BYTES:
        raise InvalidEvidence(f"{label} free-space guard violated")
    guard_load = number(row, "guard_load1")
    load_before = number(row, "load1_before")
    load_after = number(row, "load1_after")
    if not (0 <= guard_load < matrix.MAX_LOAD1):
        raise InvalidEvidence(f"{label} guard_load1 violates quiet-load guard")
    if load_before < 0 or load_after < 0:
        raise InvalidEvidence(f"{label} has negative load observation")
    noisy = load_before >= matrix.MAX_LOAD1 or load_after >= matrix.MAX_LOAD1
    if (status == "rejected_noisy") != noisy:
        raise InvalidEvidence(f"{label} status disagrees with load observations")
    try:
        captured = datetime.fromisoformat(row["captured_at_utc"])
    except (KeyError, ValueError) as error:
        raise InvalidEvidence(f"{label} has invalid capture timestamp") from error
    if captured.tzinfo is None:
        raise InvalidEvidence(f"{label} capture timestamp lacks timezone")
    return status == "accepted"


def validate_latency(row: dict[str, str], label: str) -> None:
    p50 = integer(row, "p50_ns")
    p95 = integer(row, "p95_ns")
    p99 = integer(row, "p99_ns")
    wall = integer(row, "wall_ns")
    if not (0 < p50 <= p95 <= p99 <= wall):
        raise InvalidEvidence(f"{label} latency ordering mismatch")


def variant_call_shape(
    variant: str, lengths: tuple[int, ...], iov_max: int
) -> tuple[int, int]:
    if variant == "k_pwrite":
        return len(lengths), 0
    if variant == "copy_contiguous":
        return 1, 0
    if not variant.startswith("pwritev_"):
        raise InvalidEvidence(f"unknown variant {variant}")
    _, iov_name, byte_name = variant.split("_")
    iov_cap = matrix.effective_iov_cap(iov_name, iov_max)
    signature = matrix.policy_signature(lengths, iov_cap, matrix.BYTE_CAPS[byte_name])
    return len(signature), max(len(call) for call in signature)


def validate_raw_row(row: dict[str, str]) -> bool:
    payload = integer(row, "payload")
    events_per_batch = integer(row, "events_per_batch")
    intents = integer(row, "owner_intents")
    writers = integer(row, "writers")
    unit_shape = row["unit_shape"]
    shape = matrix.Shape(payload, events_per_batch, intents, writers, unit_shape)
    if not shape.valid:
        raise InvalidEvidence("invalid measured shape")
    if integer(row, "chain") != 0:
        raise InvalidEvidence("timed raw row unexpectedly enables chaining")
    iterations = integer(row, "iterations")
    if iterations <= 0:
        raise InvalidEvidence("nonpositive iterations")
    if integer(row, "owner_units") != intents:
        raise InvalidEvidence("owner-intent mismatch")
    if integer(row, "physical_batches") != shape.physical_batches:
        raise InvalidEvidence("physical-batch derivation mismatch")
    if integer(row, "bytes") != shape.group_bytes * iterations:
        raise InvalidEvidence("byte equation mismatch")
    domain_events = intents * events_per_batch * iterations
    events = domain_events + (
        intents * iterations if unit_shape == "new_name" else 0
    )
    if integer(row, "domain_events") != domain_events or integer(row, "events") != events:
        raise InvalidEvidence("event equation mismatch")
    mode = row["mode"]
    expected_barriers = {
        "process": 0,
        "group": iterations,
        "os": shape.physical_batches * iterations,
    }[mode]
    if integer(row, "barriers") != expected_barriers:
        raise InvalidEvidence("barrier equation mismatch")
    expected_calls, expected_iov = variant_call_shape(
        row["variant"], shape.buffer_lengths, integer(row, "iov_max")
    )
    if mode == "os":
        expected_calls, expected_iov = shape.physical_batches, 0
    if integer(row, "short_writes") != 0 or integer(row, "interrupted") != 0:
        raise InvalidEvidence("timed raw row contains a short write or EINTR")
    if integer(row, "write_syscalls") != expected_calls * iterations:
        raise InvalidEvidence("syscall equation mismatch")
    if integer(row, "max_iovecs") != expected_iov:
        raise InvalidEvidence("max_iovecs mismatch")
    copied = integer(row, "copied_bytes")
    if row["variant"] == "copy_contiguous" and mode != "os":
        if copied != integer(row, "bytes"):
            raise InvalidEvidence("copy byte equation mismatch")
    elif copied != 0:
        raise InvalidEvidence("non-copy variant reports copied bytes")
    validate_latency(row, "raw")
    accepted = validate_row_environment(row, "raw")
    validate_stop(
        row,
        shape.group_bytes,
        matrix.EXPLORATORY_BYTES,
        matrix.EXPLORATORY_MILLIS,
    )
    return accepted


def validate_boundary_row(row: dict[str, str]) -> bool:
    target = integer(row, "target_bytes")
    batches = integer(row, "physical_batches")
    iterations = integer(row, "iterations")
    if iterations <= 0:
        raise InvalidEvidence("nonpositive boundary iterations")
    if integer(row, "bytes") != target * iterations:
        raise InvalidEvidence("boundary byte equation mismatch")
    captured_iov_max = integer(row, "iov_max")
    candidates = matrix.boundary_candidates(target, batches, captured_iov_max)
    if row["comparison"] not in candidates:
        raise InvalidEvidence("boundary comparison was not predeclared")
    if integer(row, "events") != batches * iterations:
        raise InvalidEvidence("boundary event equation mismatch")
    fixed = matrix.HEADER_LEN + matrix.SUBFRAME_HEADER_LEN + matrix.MARKER_LEN
    budget = target - batches * fixed
    q, remainder = divmod(budget, batches)
    lengths = tuple(fixed + q + (index < remainder) for index in range(batches))
    if integer(row, "min_payload") != q:
        raise InvalidEvidence("boundary minimum payload mismatch")
    if integer(row, "max_payload") != q + int(remainder != 0):
        raise InvalidEvidence("boundary maximum payload mismatch")
    calls, max_iov = variant_call_shape(
        row["variant"], lengths, integer(row, "iov_max")
    )
    if integer(row, "write_syscalls") != calls * iterations:
        raise InvalidEvidence("boundary syscall mismatch")
    if integer(row, "short_writes") != 0 or integer(row, "interrupted") != 0:
        raise InvalidEvidence("timed boundary row contains a short write or EINTR")
    if integer(row, "max_iovecs") != max_iov:
        raise InvalidEvidence("boundary iovec mismatch")
    if integer(row, "barriers") != (iterations if row["mode"] == "group" else 0):
        raise InvalidEvidence("boundary barrier mismatch")
    copied = integer(row, "copied_bytes")
    if row["variant"] == "copy_contiguous":
        if copied != integer(row, "bytes"):
            raise InvalidEvidence("boundary copy byte equation mismatch")
    elif copied != 0:
        raise InvalidEvidence("boundary non-copy variant reports copied bytes")
    validate_latency(row, "boundary")
    accepted = validate_row_environment(row, "boundary")
    validate_stop(
        row, target, matrix.BOUNDARY_BYTES, matrix.BOUNDARY_MILLIS
    )
    return accepted


def validate_pipeline_row(row: dict[str, str]) -> bool:
    shape = matrix.Shape(
        integer(row, "payload"),
        integer(row, "events_per_batch"),
        integer(row, "owner_units"),
        integer(row, "writers"),
        row["unit_shape"],
    )
    if not shape.valid:
        raise InvalidEvidence("invalid prepared-pipeline shape")
    if integer(row, "chain") != 0:
        raise InvalidEvidence("prepared-pipeline row unexpectedly enables chaining")
    iterations = integer(row, "iterations")
    if iterations <= 0:
        raise InvalidEvidence("nonpositive prepared-pipeline iterations")
    if integer(row, "physical_batches") != shape.physical_batches:
        raise InvalidEvidence("prepared-pipeline physical-batch mismatch")
    if integer(row, "bytes") != shape.group_bytes * iterations:
        raise InvalidEvidence("prepared-pipeline byte equation mismatch")
    expected_domain_events = shape.intents * shape.events * iterations
    expected_events = expected_domain_events + (
        shape.intents * iterations if shape.unit_shape == "new_name" else 0
    )
    if integer(row, "domain_events") != expected_domain_events:
        raise InvalidEvidence("prepared-pipeline domain-event equation mismatch")
    if integer(row, "events") != expected_events:
        raise InvalidEvidence("prepared-pipeline event equation mismatch")
    if integer(row, "prepared_batches") != shape.physical_batches * iterations:
        raise InvalidEvidence("prepared-batch equation mismatch")
    if integer(row, "cpu_ns") <= 0 or integer(row, "wall_ns") <= 0:
        raise InvalidEvidence("nonpositive prepared-pipeline time")
    validate_latency(row, "prepared-pipeline")
    accepted = validate_row_environment(row, "prepared-pipeline")
    validate_stop(
        row,
        shape.group_bytes,
        matrix.EXPLORATORY_BYTES,
        matrix.EXPLORATORY_MILLIS,
    )
    return accepted


def parse_histogram(encoded: str) -> dict[int, int]:
    histogram: dict[int, int] = {}
    for entry in encoded.split(";"):
        try:
            width_text, count_text = entry.split(":", 1)
            width, count = int(width_text), int(count_text)
        except ValueError as error:
            raise InvalidEvidence(f"invalid trace histogram {encoded!r}") from error
        if width <= 0 or count <= 0 or width in histogram:
            raise InvalidEvidence(f"invalid trace histogram entry {entry!r}")
        histogram[width] = count
    if not histogram:
        raise InvalidEvidence("empty trace histogram")
    return histogram


def trace_expected_calls(
    row: dict[str, str], winner: dict[str, object], iov_max: int
) -> tuple[int, int, int, bool]:
    histogram = parse_histogram(row["feed_histogram"])
    batch_len = matrix.Shape(
        integer(row, "payload"),
        integer(row, "events_per_batch"),
        1,
        integer(row, "writers"),
        "domain",
    ).domain_batch_len
    calls = 0
    copied = 0
    max_iov = 0
    eligible = False
    for width, frequency in histogram.items():
        shape = matrix.Shape(
            integer(row, "payload"),
            integer(row, "events_per_batch"),
            width,
            integer(row, "writers"),
            "domain",
        )
        effective = (
            "k_pwrite"
            if row["role"] == "baseline"
            else matrix.effective_variant(winner, shape, row["mode"])
        )
        one_calls, one_iov = variant_call_shape(
            effective, (batch_len,) * width, iov_max
        )
        calls += frequency * one_calls
        max_iov = max(max_iov, one_iov)
        if effective == "copy_contiguous" and row["mode"] != "os":
            copied += frequency * batch_len * width
        if effective != "k_pwrite" and row["mode"] != "os":
            eligible = True
    return calls, copied, max_iov, eligible


def validate_trace_row(
    row: dict[str, str], winner: dict[str, object], iov_max: int
) -> bool:
    if row["trace_histogram"] != row["feed_histogram"]:
        raise InvalidEvidence("trace histogram differs from frozen manifest")
    histogram = parse_histogram(row["trace_histogram"])
    groups = sum(histogram.values())
    intents = sum(width * count for width, count in histogram.items())
    if integer(row, "trace_groups") != groups:
        raise InvalidEvidence("trace group count mismatch")
    if integer(row, "trace_owner_intents") != intents:
        raise InvalidEvidence("trace owner-intent count mismatch")
    if integer(row, "trace_physical_batches") != intents:
        raise InvalidEvidence("domain trace physical count mismatch")
    cycles = integer(row, "cycles")
    if cycles <= 0:
        raise InvalidEvidence("nonpositive trace cycles")
    batch_len = matrix.Shape(
        integer(row, "payload"),
        integer(row, "events_per_batch"),
        1,
        integer(row, "writers"),
        "domain",
    ).domain_batch_len
    cycle_bytes = intents * batch_len
    expected_events = intents * integer(row, "events_per_batch") * cycles
    if integer(row, "bytes") != cycle_bytes * cycles:
        raise InvalidEvidence("trace byte equation mismatch")
    if integer(row, "events") != expected_events or integer(row, "domain_events") != expected_events:
        raise InvalidEvidence("trace event equation mismatch")
    expected_barriers = {
        "process": 0,
        "group": groups * cycles,
        "os": intents * cycles,
    }[row["mode"]]
    if integer(row, "barriers") != expected_barriers:
        raise InvalidEvidence("trace barrier equation mismatch")
    calls, copied, max_iov, _ = trace_expected_calls(row, winner, iov_max)
    if integer(row, "short_writes") != 0 or integer(row, "interrupted") != 0:
        raise InvalidEvidence("timed trace row contains a short write or EINTR")
    if integer(row, "write_syscalls") != calls * cycles:
        raise InvalidEvidence("trace syscall equation mismatch")
    if integer(row, "copied_bytes") != copied * cycles:
        raise InvalidEvidence("trace copied-byte equation mismatch")
    if integer(row, "max_iovecs") != max_iov:
        raise InvalidEvidence("trace max_iovecs mismatch")
    validate_latency(row, "trace")
    accepted = validate_row_environment(row, "trace")
    validate_stop(
        row,
        cycle_bytes,
        matrix.CONFIRMATION_BYTES,
        matrix.CONFIRMATION_MILLIS,
    )
    return accepted


def validate_paired_order(group: list[dict[str, str]]) -> list[dict[str, str]]:
    if len(group) != 4:
        raise InvalidEvidence("paired group must have four rows")
    group = sorted(group, key=lambda row: integer(row, "slot"))
    order = group[0]["order"]
    expected = (
        ("baseline", "candidate", "candidate", "baseline")
        if order == "ABBA"
        else ("candidate", "baseline", "baseline", "candidate")
    )
    if tuple(row["role"] for row in group) != expected:
        raise InvalidEvidence("ABBA/BAAB role order mismatch")
    return group


def paired_ratios(group: list[dict[str, str]]) -> dict[str, float]:
    group = validate_paired_order(group)
    if any(row["status"] != "accepted" for row in group):
        raise InvalidEvidence("partial/noisy physical comparison reached metrics")
    results: dict[str, list[float]] = defaultdict(list)
    for left, right in ((group[0], group[1]), (group[2], group[3])):
        baseline = left if left["role"] == "baseline" else right
        candidate = right if left["role"] == "baseline" else left
        base_events = integer(baseline, "events")
        cand_events = integer(candidate, "events")
        results["throughput"].append(
            (cand_events / integer(candidate, "wall_ns"))
            / (base_events / integer(baseline, "wall_ns"))
        )
        results["cpu"].append(
            (integer(candidate, "cpu_ns") / cand_events)
            / (integer(baseline, "cpu_ns") / base_events)
        )
        results["p99"].append(
            integer(candidate, "p99_ns") / integer(baseline, "p99_ns")
        )
        results["syscalls"].append(
            (integer(candidate, "write_syscalls") / integer(candidate, "iterations"))
            / (integer(baseline, "write_syscalls") / integer(baseline, "iterations"))
        )
        results["copied"].append(integer(candidate, "copied_bytes"))
        results["baseline_cpu_group"].append(
            integer(baseline, "cpu_ns") / integer(baseline, "iterations")
        )
        results["candidate_cpu_group"].append(
            integer(candidate, "cpu_ns") / integer(candidate, "iterations")
        )
        results["baseline_wall_group"].append(
            integer(baseline, "wall_ns") / integer(baseline, "iterations")
        )
        results["candidate_wall_group"].append(
            integer(candidate, "wall_ns") / integer(candidate, "iterations")
        )
    return {key: median(values) for key, values in results.items()}


def grouped_pairs(
    evidence: list[dict[str, str]], keys: tuple[str, ...]
) -> dict[tuple[str, ...], dict[str, float]]:
    groups: dict[tuple[str, ...], list[dict[str, str]]] = defaultdict(list)
    for row in evidence:
        groups[tuple(row[key] for key in keys)].append(row)
    result = {}
    for key, group in groups.items():
        ordered = validate_paired_order(group)
        if all(row["status"] == "accepted" for row in ordered):
            result[key] = paired_ratios(ordered)
    return result


def prep_lookup(pipeline: list[dict[str, str]]) -> dict[tuple[str, ...], tuple[float, float]]:
    lookup = {}
    seen = set()
    for row in pipeline:
        key = (
            row["payload"],
            row["events_per_batch"],
            row["owner_units"],
            row["writers"],
            row["unit_shape"],
        )
        if key in seen:
            raise InvalidEvidence("duplicate prepared-pipeline shape")
        seen.add(key)
        if row["status"] != "accepted":
            continue
        iterations = integer(row, "iterations")
        lookup[key] = (
            integer(row, "cpu_ns") / iterations,
            integer(row, "wall_ns") / iterations,
        )
    return lookup


def representative_candidate(
    shape: matrix.Shape, variant: str, iov_max: int
) -> str:
    if variant == "copy_contiguous":
        if variant in matrix.natural_candidates(shape, iov_max):
            return variant
        raise InvalidEvidence("missing natural copy representative")
    if not variant.startswith("pwritev_"):
        raise InvalidEvidence(f"unknown natural candidate {variant}")
    _, desired_iov, desired_bytes = variant.split("_")
    desired_cap = matrix.effective_iov_cap(desired_iov, iov_max)
    desired = matrix.policy_signature(
        shape.buffer_lengths,
        desired_cap,
        matrix.BYTE_CAPS[desired_bytes],
    )
    for candidate in matrix.natural_candidates(shape, iov_max):
        if not candidate.startswith("pwritev_"):
            continue
        _, candidate_iov, candidate_bytes = candidate.split("_")
        cap = matrix.effective_iov_cap(candidate_iov, iov_max)
        signature = matrix.policy_signature(
            shape.buffer_lengths,
            cap,
            matrix.BYTE_CAPS[candidate_bytes],
        )
        if signature == desired:
            return candidate
    raise InvalidEvidence("missing natural pwrite representative")


def representative_boundary_variant(
    target: int, batches: int, variant: str, iov_max: int
) -> str:
    candidates = matrix.boundary_candidates(target, batches, iov_max)
    if variant == "copy_contiguous":
        if variant in candidates:
            return variant
        raise InvalidEvidence("missing boundary copy representative")
    if not variant.startswith("pwritev_"):
        raise InvalidEvidence(f"unknown boundary variant {variant}")
    _, iov, byte_name = variant.split("_")
    fixed = matrix.HEADER_LEN + matrix.SUBFRAME_HEADER_LEN + matrix.MARKER_LEN
    budget = target - batches * fixed
    q, remainder = divmod(budget, batches)
    lengths = tuple(fixed + q + (index < remainder) for index in range(batches))
    desired_cap = matrix.effective_iov_cap(iov, iov_max)
    desired = matrix.policy_signature(
        lengths, desired_cap, matrix.BYTE_CAPS[byte_name]
    )
    for candidate in candidates:
        if not candidate.startswith("pwritev_"):
            continue
        _, candidate_iov, candidate_bytes = candidate.split("_")
        cap = matrix.effective_iov_cap(candidate_iov, iov_max)
        signature = matrix.policy_signature(
            lengths, cap, matrix.BYTE_CAPS[candidate_bytes]
        )
        if signature == desired:
            return candidate
    raise InvalidEvidence("missing boundary representative")


def rule_boundary_variant(
    rule: dict[str, object], target: int, batches: int
) -> str:
    if batches < int(rule["min_batches"]):
        return "k_pwrite"
    if rule["kind"] == "fixed":
        variant = str(rule["variant"])
        if variant == "copy_contiguous" and target > 8 << 20:
            return "k_pwrite"
        return variant
    copy_ceiling = int(rule["copy_ceiling"])
    if copy_ceiling and target <= copy_ceiling:
        return "copy_contiguous"
    return f"pwritev_{rule['iov']}_{rule['byte_cap']}"


def required_boundary_centers(
    rule: dict[str, object], iov_max: int
) -> set[str]:
    if rule["kind"] == "fixed" and rule["variant"] == "copy_contiguous":
        return {f"width_{rule['min_batches']}", f"bytes_{8 << 20}"}
    iov = str(rule["iov"])
    iov_cap = matrix.effective_iov_cap(iov, iov_max)
    required = {
        f"width_{rule['min_batches']}",
        f"iov_{iov_cap}",
        f"bytes_{matrix.BYTE_CAPS[str(rule['byte_cap'])]}",
    }
    copy_ceiling = int(rule["copy_ceiling"])
    if copy_ceiling:
        required.add(f"bytes_{copy_ceiling}")
    return required


def evaluate_rule_boundaries(
    rule: dict[str, object],
    pairs: dict[tuple[str, ...], dict[str, float]],
    iov_max: int,
) -> list[str]:
    reasons: list[str] = []
    covered: dict[str, set[int]] = defaultdict(set)
    for boundary, target, batches in matrix.boundary_shapes(iov_max):
        center, delta_text = boundary.rsplit("_", 1)
        covered[center].add(int(delta_text))
        variant = rule_boundary_variant(rule, target, batches)
        if variant == "k_pwrite":
            continue
        try:
            representative = representative_boundary_variant(
                target, batches, variant, iov_max
            )
        except InvalidEvidence:
            reasons.append(
                f"missing exact boundary call-shape evidence {boundary}/{variant}"
            )
            continue
        observed = pairs.get((boundary, "process", representative))
        if observed is None:
            reasons.append(f"missing exact boundary pair {boundary}/{representative}")
            continue
        if observed["throughput"] < 0.95:
            reasons.append(f"boundary throughput regression {boundary}/{variant}")
        if observed["cpu"] > 1.05:
            reasons.append(f"boundary cpu regression {boundary}/{variant}")
        if observed["p99"] > 1.05:
            reasons.append(f"boundary p99 regression {boundary}/{variant}")
        if observed["syscalls"] > 0.50:
            reasons.append(
                f"boundary syscall reduction below 2x {boundary}/{variant}"
            )
    for center in sorted(required_boundary_centers(rule, iov_max)):
        if covered.get(center) != {-1, 0, 1}:
            reasons.append(f"incomplete exact boundary transition {center}")
    return reasons


def load_stage(output: Path):
    exploratory = rows(output / "exploratory.csv")
    boundary = rows(output / "boundaries.csv")
    pipeline = rows(output / "prepared_pipeline.csv")
    exploratory_manifest = rows(output / "exploratory_manifest.csv")
    boundary_manifest = rows(output / "boundary_manifest.csv")
    validate_manifest(exploratory, exploratory_manifest)
    validate_manifest(boundary, boundary_manifest)
    provenance = constant_provenance(exploratory + boundary + pipeline)
    validate_attempt_protocol(output, provenance)
    root = Path(__file__).resolve().parents[2]
    correctness = output / "correctness.json"
    try:
        matrix.validate_correctness(correctness, provenance["binary_sha256"])
    except (OSError, ValueError, RuntimeError) as error:
        raise InvalidEvidence(f"invalid correctness artifact: {error}") from error
    if matrix.sha256_file(correctness) != provenance["correctness_sha256"]:
        raise InvalidEvidence("correctness artifact hash mismatch")
    if matrix.sha256_file(Path(matrix.__file__).resolve()) != provenance["runner_sha256"]:
        raise InvalidEvidence("runner hash differs from captured source")
    if matrix.sha256_file(Path(__file__).resolve()) != provenance["evaluator_sha256"]:
        raise InvalidEvidence("evaluator hash differs from captured source")
    if matrix.harness_sha256(root) != provenance["harness_sha256"]:
        raise InvalidEvidence("harness/source-feed hash differs from captured source")
    iov_max = int(provenance["iov_max"])
    validate_predeclared_manifest(
        exploratory_manifest,
        [matrix.asdict(point) for point in matrix.exploratory_points(iov_max)],
    )
    validate_predeclared_manifest(
        boundary_manifest,
        matrix.boundary_plan(iov_max),
    )
    for row in exploratory:
        validate_raw_row(row)
    for row in boundary:
        validate_boundary_row(row)
    for row in pipeline:
        validate_pipeline_row(row)
    expected_pipeline = {
        (
            str(shape.payload),
            str(shape.events),
            str(shape.intents),
            str(shape.writers),
            shape.unit_shape,
        )
        for shape in matrix.prepared_pipeline_shapes(
            root
        )
    }
    actual_pipeline = {
        (
            row["payload"],
            row["events_per_batch"],
            row["owner_units"],
            row["writers"],
            row["unit_shape"],
        )
        for row in pipeline
    }
    if actual_pipeline != expected_pipeline or len(pipeline) != len(expected_pipeline):
        raise InvalidEvidence(
            "prepared-pipeline evidence differs from predeclared shapes"
        )
    return exploratory, boundary, pipeline, provenance


def evaluate_rule(
    rule: dict[str, object],
    natural_pairs: dict[tuple[str, ...], dict[str, float]],
    boundary_pairs: dict[tuple[str, ...], dict[str, float]],
    prep: dict[tuple[str, ...], tuple[float, float]],
    iov_max: int,
) -> dict[str, object]:
    cell_results = []
    reasons = []
    required_prep = {
        (
            str(shape.payload),
            str(shape.events),
            str(shape.intents),
            str(shape.writers),
            shape.unit_shape,
        )
        for shape in matrix.prepared_pipeline_shapes(
            Path(__file__).resolve().parents[2]
        )
    }
    missing_prep = sorted(required_prep - set(prep))
    if missing_prep:
        return {
            "rule": rule,
            "eligible": False,
            "reasons": [
                f"missing/noisy prepared-pipeline shape {key}"
                for key in missing_prep
            ],
        }
    for shape in matrix.selection_shapes():
        effective = matrix.effective_variant(rule, shape, "process")
        if effective == "k_pwrite":
            continue
        try:
            representative = representative_candidate(shape, effective, iov_max)
        except InvalidEvidence:
            reasons.append(
                f"missing exact natural call-shape evidence {effective}/{shape}"
            )
            continue
        adjustment = {
            metric: 1.0
            for metric in ("throughput", "cpu", "p99", "syscalls")
        }
        key = (
            representative,
            "process",
            str(shape.payload),
            str(shape.events),
            str(shape.intents),
            str(shape.writers),
            shape.unit_shape,
        )
        observed = natural_pairs.get(key)
        if observed is None:
            reasons.append(f"missing natural pair {key}")
            continue
        result = {
            metric: observed[metric] * adjustment[metric]
            for metric in ("throughput", "cpu", "p99", "syscalls")
        }
        prep_key = (
            str(shape.payload),
            str(shape.events),
            str(shape.intents),
            str(shape.writers),
            shape.unit_shape,
        )
        if prep_key not in prep:
            reasons.append(f"missing prepared-pipeline shape {prep_key}")
            continue
        prep_cpu, prep_wall = prep[prep_key]
        base_cpu = observed["baseline_cpu_group"]
        cand_cpu = observed["candidate_cpu_group"] * adjustment["cpu"]
        base_wall = observed["baseline_wall_group"]
        cand_wall = observed["candidate_wall_group"] / adjustment["throughput"]
        result["pipeline_cpu"] = (prep_cpu + cand_cpu) / (prep_cpu + base_cpu)
        result["pipeline_throughput"] = (prep_wall + base_wall) / (prep_wall + cand_wall)
        result["shape"] = shape
        result["copied"] = observed["copied"] if representative == "copy_contiguous" else 0
        cell_results.append(result)
    reasons.extend(evaluate_rule_boundaries(rule, boundary_pairs, iov_max))
    if reasons:
        return {"rule": rule, "eligible": False, "reasons": sorted(set(reasons))}
    if not cell_results:
        return {"rule": rule, "eligible": False, "reasons": ["no eligible cells"]}
    for result in cell_results:
        shape = result["shape"]
        if result["throughput"] < 0.95:
            reasons.append(f"throughput regression {shape}")
        if result["cpu"] > 1.05:
            reasons.append(f"cpu regression {shape}")
        if result["p99"] > 1.05:
            reasons.append(f"p99 regression {shape}")
        if result["syscalls"] > 0.50:
            reasons.append(f"syscall reduction below 2x {shape}")
    important = [
        result
        for result in cell_results
        if result["shape"].payload in (24, 250)
        and result["shape"].events in (10, 100)
        and result["shape"].writers == 4
    ]
    isolated_cpu = geometric_mean(result["cpu"] for result in important)
    isolated_throughput = geometric_mean(
        result["throughput"] for result in important
    )
    pipeline_cpu = geometric_mean(result["pipeline_cpu"] for result in important)
    pipeline_throughput = geometric_mean(
        result["pipeline_throughput"] for result in important
    )
    if isolated_cpu > 0.90 and isolated_throughput < 1.10:
        reasons.append("isolated 10% gate failed")
    if pipeline_cpu > 0.90 and pipeline_throughput < 1.10:
        reasons.append("prepared-write-pipeline 10% gate failed")
    copied = sum(int(result["copied"]) for result in cell_results)
    summary = {
        "rule": rule,
        "eligible": not reasons,
        "reasons": reasons,
        "isolated_cpu_ratio": isolated_cpu,
        "isolated_throughput_ratio": isolated_throughput,
        "pipeline_cpu_ratio": pipeline_cpu,
        "pipeline_throughput_ratio": pipeline_throughput,
        "copied_bytes_observed": copied,
        "mean_syscall_ratio": geometric_mean(
            result["syscalls"] for result in cell_results
        ),
    }
    return summary


def select(output: Path, selection_path: Path) -> int:
    exploratory, boundary, pipeline, provenance = load_stage(output)
    natural_keys = (
        "comparison",
        "mode",
        "payload",
        "events_per_batch",
        "owner_intents",
        "writers",
        "unit_shape",
    )
    boundary_keys = ("boundary", "mode", "comparison")
    natural_pairs = grouped_pairs(exploratory, natural_keys)
    boundary_pairs = grouped_pairs(boundary, boundary_keys)
    prep = prep_lookup(pipeline)
    iov_max = int(provenance["iov_max"])
    candidates = []
    for rule in matrix.fixed_rules() + matrix.adaptive_configs():
        candidates.append(
            evaluate_rule(rule, natural_pairs, boundary_pairs, prep, iov_max)
        )
    eligible = [candidate for candidate in candidates if candidate["eligible"]]
    if not eligible:
        document = {
            "schema": "pwritev_group.selection.v1",
            "verdict": "DECLINED",
            "winner": None,
            "candidates": candidates,
        }
        selection_path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
        print("DECLINED: no Stage-1 rule cleared every frozen gate")
        return 1
    eligible.sort(
        key=lambda candidate: (
            candidate["pipeline_cpu_ratio"],
            candidate["isolated_cpu_ratio"],
            -candidate["pipeline_throughput_ratio"],
            candidate["copied_bytes_observed"],
            candidate["mean_syscall_ratio"],
            candidate["rule"]["id"],
        )
    )
    winner = eligible[0]["rule"]
    document = {
        "schema": "pwritev_group.selection.v1",
        "verdict": "SELECTED",
        "winner": winner,
        "winner_metrics": eligible[0],
        "candidates": candidates,
    }
    selection_path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    print(f"SELECTED {winner['id']} for confirmation")
    return 0


def trace_pipeline_cost(
    row: dict[str, str], prep: dict[tuple[str, ...], tuple[float, float]]
) -> tuple[float, float]:
    cpu = 0.0
    wall = 0.0
    for width, frequency in parse_histogram(row["feed_histogram"]).items():
        key = (
            row["payload"],
            row["events_per_batch"],
            str(width),
            row["writers"],
            "domain",
        )
        if key not in prep:
            raise InvalidEvidence(f"missing trace pipeline preparation {key}")
        one_cpu, one_wall = prep[key]
        cpu += frequency * one_cpu
        wall += frequency * one_wall
    return cpu, wall


def eligible_syscall_counts(
    row: dict[str, str], winner: dict[str, object], iov_max: int
) -> tuple[int, int]:
    batch_len = matrix.Shape(
        integer(row, "payload"),
        integer(row, "events_per_batch"),
        1,
        integer(row, "writers"),
        "domain",
    ).domain_batch_len
    baseline_calls = 0
    candidate_calls = 0
    for width, frequency in parse_histogram(row["feed_histogram"]).items():
        shape = matrix.Shape(
            integer(row, "payload"),
            integer(row, "events_per_batch"),
            width,
            integer(row, "writers"),
            "domain",
        )
        effective = matrix.effective_variant(winner, shape, row["mode"])
        if effective == "k_pwrite" or row["mode"] == "os":
            continue
        baseline_calls += frequency * width
        calls, _ = variant_call_shape(
            effective, (batch_len,) * width, iov_max
        )
        candidate_calls += frequency * calls
    return baseline_calls, candidate_calls


def trace_cell(
    group: list[dict[str, str]],
    winner: dict[str, object],
    prep: dict[tuple[str, ...], tuple[float, float]],
    iov_max: int,
) -> dict[str, object]:
    if len(group) != 12:
        raise InvalidEvidence("confirmation cell must contain 12 ABBA rows")
    group = sorted(group, key=lambda row: (integer(row, "repetition"), integer(row, "slot")))
    metrics: dict[str, list[float]] = defaultdict(list)
    eligible_baseline_calls = 0
    eligible_candidate_calls = 0
    accepted_pairs = 0
    for repetition in matrix.CONFIRM_REPETITIONS:
        quartet = [row for row in group if integer(row, "repetition") == repetition]
        if len(quartet) != 4:
            raise InvalidEvidence("confirmation repetition is incomplete")
        order = quartet[0]["order"]
        expected = (
            ("baseline", "candidate", "candidate", "baseline")
            if order == "ABBA"
            else ("candidate", "baseline", "baseline", "candidate")
        )
        if tuple(row["role"] for row in quartet) != expected:
            raise InvalidEvidence("confirmation ABBA/BAAB order mismatch")
        for left, right in ((quartet[0], quartet[1]), (quartet[2], quartet[3])):
            baseline = left if left["role"] == "baseline" else right
            candidate = right if left["role"] == "baseline" else left
            if (
                baseline["feed_histogram"] != candidate["feed_histogram"]
                or baseline["feed_source"] != candidate["feed_source"]
            ):
                raise InvalidEvidence("paired trace feed mismatch")
            if baseline["status"] != "accepted" or candidate["status"] != "accepted":
                continue
            accepted_pairs += 1
            base_events = integer(baseline, "events")
            cand_events = integer(candidate, "events")
            metrics["throughput"].append(
                (cand_events / integer(candidate, "wall_ns"))
                / (base_events / integer(baseline, "wall_ns"))
            )
            metrics["cpu"].append(
                (integer(candidate, "cpu_ns") / cand_events)
                / (integer(baseline, "cpu_ns") / base_events)
            )
            metrics["p99"].append(
                integer(candidate, "p99_ns") / integer(baseline, "p99_ns")
            )
            metrics["syscalls"].append(
                (integer(candidate, "write_syscalls") / integer(candidate, "cycles"))
                / (integer(baseline, "write_syscalls") / integer(baseline, "cycles"))
            )
            prep_cpu, prep_wall = trace_pipeline_cost(baseline, prep)
            base_cpu = integer(baseline, "cpu_ns") / integer(baseline, "cycles")
            cand_cpu = integer(candidate, "cpu_ns") / integer(candidate, "cycles")
            base_wall = integer(baseline, "wall_ns") / integer(baseline, "cycles")
            cand_wall = integer(candidate, "wall_ns") / integer(candidate, "cycles")
            metrics["pipeline_cpu"].append(
                (prep_cpu + cand_cpu) / (prep_cpu + base_cpu)
            )
            metrics["pipeline_throughput"].append(
                (prep_wall + base_wall) / (prep_wall + cand_wall)
            )
            base_calls, cand_calls = eligible_syscall_counts(
                baseline, winner, iov_max
            )
            eligible_baseline_calls += base_calls
            eligible_candidate_calls += cand_calls
    first = group[0]
    result: dict[str, object] = {
        "comparison": first["comparison"],
        "mode": first["mode"],
        "payload": integer(first, "payload"),
        "events": integer(first, "events_per_batch"),
        "writers": integer(first, "writers"),
        "feed_sources": sorted({row["feed_source"] for row in group}),
        "feed_histograms": sorted({row["feed_histogram"] for row in group}),
        "pair_count": accepted_pairs,
        "planned_pair_count": 6,
    }
    metric_names = (
        "throughput",
        "cpu",
        "p99",
        "syscalls",
        "pipeline_cpu",
        "pipeline_throughput",
    )
    for metric in metric_names:
        values = metrics[metric]
        if len(values) != accepted_pairs:
            raise InvalidEvidence(f"confirmation {metric} pair count mismatch")
        result[metric] = median(values) if accepted_pairs >= 3 else None
    result["eligible_syscall_ratio"] = (
        eligible_candidate_calls / eligible_baseline_calls
        if accepted_pairs >= 3 and eligible_baseline_calls
        else None
    )
    result["eligible_baseline_calls"] = eligible_baseline_calls
    result["eligible_candidate_calls"] = eligible_candidate_calls
    return result


def confirmation_noise_reasons(cells: list[dict[str, object]]) -> list[str]:
    return [
        "confirmation cell lacks all six clean adjacent pairs: "
        f"{cell['mode']}/{cell['payload']}/{cell['events']}/"
        f"{cell['writers']} accepted={cell['pair_count']}"
        for cell in cells
        if cell["pair_count"] != 6
    ]


def confirmation_noise_verdict(
    cells: list[dict[str, object]],
) -> tuple[str | None, list[str]]:
    reasons = confirmation_noise_reasons(cells)
    return ("INCONCLUSIVE_NOISE", reasons) if reasons else (None, [])


def final(output: Path) -> int:
    exploratory, boundary, pipeline, provenance = load_stage(output)
    confirmation = rows(output / "confirmation.csv")
    selection = json.loads((output / "selection.json").read_text())
    if selection.get("verdict") != "SELECTED" or not selection.get("winner"):
        raise InvalidEvidence("confirmation lacks frozen selected winner")
    winner = selection["winner"]
    confirmation_manifest = rows(output / "confirmation_manifest.csv")
    validate_manifest(confirmation, confirmation_manifest)
    expected_points = matrix.confirmation_points(
        Path(__file__).resolve().parents[2], winner
    )
    validate_predeclared_manifest(
        confirmation_manifest,
        [matrix.asdict(point) for point in expected_points],
    )
    combined_provenance = constant_provenance(confirmation + pipeline)
    if any(
        combined_provenance[field] != provenance[field]
        for field in combined_provenance
    ):
        raise InvalidEvidence("confirmation provenance differs from Stage 1")
    iov_max = int(provenance["iov_max"])
    for row in confirmation:
        validate_trace_row(row, winner, iov_max)
        if row["comparison"] != winner["id"]:
            raise InvalidEvidence("confirmation comparison is not frozen winner")

    # Recompute Stage-1 selection so selection.json cannot substitute a winner.
    natural_pairs = grouped_pairs(
        exploratory,
        (
            "comparison",
            "mode",
            "payload",
            "events_per_batch",
            "owner_intents",
            "writers",
            "unit_shape",
        ),
    )
    pipeline_lookup = prep_lookup(pipeline)
    candidates = [
        evaluate_rule(
            rule,
            natural_pairs,
            grouped_pairs(
                boundary,
                ("boundary", "mode", "comparison"),
            ),
            pipeline_lookup,
            iov_max,
        )
        for rule in matrix.fixed_rules() + matrix.adaptive_configs()
    ]
    eligible = [candidate for candidate in candidates if candidate["eligible"]]
    eligible.sort(
        key=lambda candidate: (
            candidate["pipeline_cpu_ratio"],
            candidate["isolated_cpu_ratio"],
            -candidate["pipeline_throughput_ratio"],
            candidate["copied_bytes_observed"],
            candidate["mean_syscall_ratio"],
            candidate["rule"]["id"],
        )
    )
    if not eligible or eligible[0]["rule"] != winner:
        raise InvalidEvidence("selection artifact does not match recomputed winner")

    grouped: dict[tuple[str, ...], list[dict[str, str]]] = defaultdict(list)
    for row in confirmation:
        grouped[
            (
                row["comparison"],
                row["mode"],
                row["payload"],
                row["events_per_batch"],
                row["writers"],
            )
        ].append(row)
    cells = [
        trace_cell(group, winner, pipeline_lookup, iov_max)
        for group in grouped.values()
    ]
    expected_axes = {
        (mode, payload, events, writers)
        for mode in ("process", "group", "os")
        for payload in matrix.CONFIRM_PAYLOADS
        for events in (
            (1, 10) if mode == "os" else matrix.CONFIRM_EVENTS
        )
        for writers in matrix.CONFIRM_WRITERS
    }
    actual_axes = {
        (cell["mode"], cell["payload"], cell["events"], cell["writers"])
        for cell in cells
    }
    if actual_axes != expected_axes or len(cells) != 40:
        raise InvalidEvidence("confirmation axes differ from exact 40-cell matrix")
    noise_verdict, noise_reasons = confirmation_noise_verdict(cells)
    if noise_verdict:
        report = {
            "schema": "pwritev_group.evaluation.v1",
            "verdict": noise_verdict,
            "winner": winner,
            "isolated_cpu_ratio": None,
            "isolated_throughput_ratio": None,
            "prepared_pipeline_cpu_ratio": None,
            "prepared_pipeline_throughput_ratio": None,
            "integration_prerequisites": [
                "real public cancellation-before-admission",
                "admitted I/O remains terminal after receiver drop",
                "full FlatOwner grouping/publication/completion product gate",
            ],
            "reasons": noise_reasons,
            "cells": cells,
        }
        (output / "evaluation.json").write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n"
        )
        print(
            json.dumps(
                {key: report[key] for key in report if key != "cells"}, indent=2
            )
        )
        return 1
    reasons = []
    eligible_syscall_cells = 0
    for cell in cells:
        if cell["throughput"] < 0.95:
            reasons.append(f"throughput regression {cell}")
        if cell["cpu"] > 1.05:
            reasons.append(f"cpu regression {cell}")
        if cell["p99"] > 1.05:
            reasons.append(f"p99 regression {cell}")
        if cell["mode"] == "os":
            if not (0.95 <= cell["throughput"] <= 1.05):
                reasons.append(f"Os throughput outside 5% {cell}")
            if not (0.95 <= cell["cpu"] <= 1.05):
                reasons.append(f"Os CPU outside 5% {cell}")
        elif cell["eligible_syscall_ratio"] is not None:
            eligible_syscall_cells += 1
            if cell["eligible_syscall_ratio"] > 0.50:
                reasons.append(f"eligible syscall reduction below 2x {cell}")
    if eligible_syscall_cells == 0:
        reasons.append("no eligible syscall-reduction evidence")
    ranked = [cell for cell in cells if cell["mode"] in ("process", "group")]
    weights = [
        2
        if cell["writers"] == 4 and cell["events"] in (10, 100)
        else 1
        for cell in ranked
    ]
    isolated_cpu = geometric_mean((cell["cpu"] for cell in ranked), weights)
    isolated_throughput = geometric_mean(
        (cell["throughput"] for cell in ranked), weights
    )
    pipeline_cpu = geometric_mean(
        (cell["pipeline_cpu"] for cell in ranked), weights
    )
    pipeline_throughput = geometric_mean(
        (cell["pipeline_throughput"] for cell in ranked), weights
    )
    if isolated_cpu > 0.90 and isolated_throughput < 1.10:
        reasons.append("isolated weighted 10% gate failed")
    if pipeline_cpu > 0.90 and pipeline_throughput < 1.10:
        reasons.append("prepared-write-pipeline weighted 10% gate failed")
    verdict = "ADOPT" if not reasons else "DECLINED"
    report = {
        "schema": "pwritev_group.evaluation.v1",
        "verdict": verdict,
        "winner": winner,
        "isolated_cpu_ratio": isolated_cpu,
        "isolated_throughput_ratio": isolated_throughput,
        "prepared_pipeline_cpu_ratio": pipeline_cpu,
        "prepared_pipeline_throughput_ratio": pipeline_throughput,
        "integration_prerequisites": [
            "real public cancellation-before-admission",
            "admitted I/O remains terminal after receiver drop",
            "full FlatOwner grouping/publication/completion product gate",
        ],
        "reasons": reasons,
        "cells": cells,
    }
    (output / "evaluation.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps({key: report[key] for key in report if key != "cells"}, indent=2))
    return 0 if verdict == "ADOPT" else 1


def self_test() -> int:
    assert abs(geometric_mean([0.81, 1.0]) - 0.9) < 1e-12
    assert median([3.0, 1.0, 2.0]) == 2.0
    assert len(matrix.adaptive_configs()) == 144
    assert len(matrix.fixed_rules()) == 4
    summary = matrix.matrix_summary(Path(__file__).resolve().parents[2], 1024)
    assert summary["counts"]["exploratory"] == 952
    assert summary["counts"]["boundary"] == 440
    assert summary["counts"]["confirmation_if_selected"] == 480

    quartet = []
    for slot, role in enumerate(
        ("baseline", "candidate", "candidate", "baseline"), 1
    ):
        quartet.append(
            {
                "comparison": "candidate",
                "slot": str(slot),
                "order": "ABBA",
                "role": role,
                "status": "accepted",
                "events": "100",
                "wall_ns": "1000" if role == "baseline" else "900",
                "cpu_ns": "1000" if role == "baseline" else "900",
                "p99_ns": "100" if role == "baseline" else "90",
                "write_syscalls": "20" if role == "baseline" else "10",
                "iterations": "10",
                "copied_bytes": "0",
            }
        )
    assert ("candidate",) in grouped_pairs(quartet, ("comparison",))
    quartet[0]["status"] = "rejected_noisy"
    assert ("candidate",) not in grouped_pairs(quartet, ("comparison",))
    clean_prep = {
        (
            str(shape.payload),
            str(shape.events),
            str(shape.intents),
            str(shape.writers),
            shape.unit_shape,
        ): (1.0, 1.0)
        for shape in matrix.prepared_pipeline_shapes(
            Path(__file__).resolve().parents[2]
        )
    }
    missing_comparison = evaluate_rule(
        matrix.fixed_rules()[0], {}, {}, clean_prep, 1024
    )
    assert not missing_comparison["eligible"]
    assert any(
        "missing natural pair" in reason
        for reason in missing_comparison["reasons"]
    )

    trace_rows = []
    for repetition in matrix.CONFIRM_REPETITIONS:
        order = "ABBA" if repetition % 2 else "BAAB"
        roles = (
            ("baseline", "candidate", "candidate", "baseline")
            if order == "ABBA"
            else ("candidate", "baseline", "baseline", "candidate")
        )
        for slot, role in enumerate(roles, 1):
            trace_rows.append(
                {
                    "comparison": "fixed_pwritev_full_8m",
                    "mode": "process",
                    "payload": "24",
                    "events_per_batch": "1",
                    "writers": "1",
                    "repetition": str(repetition),
                    "slot": str(slot),
                    "order": order,
                    "role": role,
                    "status": "accepted",
                    "feed_source": f"feed-{repetition}",
                    "feed_histogram": "2:1",
                    "events": "2",
                    "wall_ns": "1000" if role == "baseline" else "900",
                    "cpu_ns": "1000" if role == "baseline" else "900",
                    "p99_ns": "100" if role == "baseline" else "90",
                    "write_syscalls": "2" if role == "baseline" else "1",
                    "cycles": "1",
                }
            )
    trace_rows[0]["status"] = "rejected_noisy"
    cell = trace_cell(
        trace_rows,
        matrix.fixed_rules()[0],
        {("24", "1", "2", "1", "domain"): (100.0, 100.0)},
        1024,
    )
    assert cell["pair_count"] == 5 and cell["cpu"] is not None
    noise_verdict, noise_reasons = confirmation_noise_verdict([cell])
    assert noise_verdict == "INCONCLUSIVE_NOISE" and noise_reasons
    print("PASS evaluator self-test")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("select", "final", "self-test"))
    parser.add_argument("output", type=Path, nargs="?")
    parser.add_argument("selection", type=Path, nargs="?")
    args = parser.parse_args()
    try:
        if args.command == "self-test":
            return self_test()
        if args.output is None:
            parser.error("output directory is required")
        if args.command == "select":
            if args.selection is None:
                parser.error("selection output path is required")
            return select(args.output, args.selection)
        return final(args.output)
    except InvalidEvidence as error:
        print(f"INVALID_EVIDENCE: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
