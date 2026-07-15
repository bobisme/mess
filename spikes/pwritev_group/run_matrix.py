#!/usr/bin/env python3
"""Two-stage Attempt-2 runner for the clarified bn-1zv6 experiment."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import platform
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


MODES = ("process", "os", "group")
PAYLOADS = (24, 250, 4096, 65536)
EVENT_COUNTS = (1, 10, 100, 1000)
OWNER_INTENTS = (1, 2, 4, 16, 64, 256, 1024, 1025)
UNIT_SHAPES = ("domain", "new_name")
IOV_NAMES = ("full", "256", "64")
BYTE_CAP_NAMES = ("1m", "4m", "8m")
BYTE_CAPS = {"1m": 1 << 20, "4m": 4 << 20, "8m": 8 << 20}
COPY_CEILINGS = (0, 64 << 10, 256 << 10, 1 << 20)
MIN_WIDTHS = (2, 4, 8, 16)
CONFIRM_PAYLOADS = (24, 250)
CONFIRM_EVENTS = (1, 10, 100, 1000)
CONFIRM_WRITERS = (1, 4)
CONFIRM_REPETITIONS = (1, 2, 3)

HEADER_LEN = 72
SUBFRAME_HEADER_LEN = 28
MARKER_LEN = 16
REGISTRY_BATCH_LEN = 72 + 28 + 24 + 16
SEGMENT_HEADER_LEN = 128
MAX_BATCH_LEN = 64 << 20
MAX_CORPUS_BYTES = 2 << 30
MIN_FREE_BYTES = 16 << 30
MAX_LOAD1 = 6.0
POINT_TIMEOUT_SECONDS = 600
ATTEMPT_TIMEOUT_SECONDS = 2 * 60 * 60
EXPLORATORY_BYTES = 32 << 20
EXPLORATORY_MILLIS = 250
BOUNDARY_BYTES = 64 << 20
BOUNDARY_MILLIS = 500
CONFIRMATION_BYTES = 128 << 20
CONFIRMATION_MILLIS = 750
FOREIGN_PROCESS_NAMES = {
    "cargo",
    "rustc",
    "cc",
    "ld",
    "collect2",
    "pwritev_group",
}


class AttemptTimeout(RuntimeError):
    """The one permitted Attempt-2 run exhausted its absolute wall budget."""


ACTIVE_ATTEMPT_DEADLINE: float | None = None

REQUIRED_CORRECTNESS_CASES = frozenset(
    {
        "pwritev.short.within_first_iovec",
        "pwritev.short.on_first_iovec_boundary",
        "pwritev.short.across_first_iovec_boundary",
        "pwritev.short.on_second_iovec_boundary",
        "pwritev.short.within_last_iovec",
        "pwritev.short.repeated_partials",
        "pwritev.eintr.before_progress",
        "pwritev.eintr.after_progress",
        "pwritev.zero_progress.exact_cursor_no_retry",
        "pwritev.eio.before_progress_no_retry",
        "pwritev.eio.after_partial_no_retry",
        "pwritev.enospc.before_progress_no_retry",
        "pwritev.enospc.after_partial_no_retry",
        "pwritev.iov_max.minus_one",
        "pwritev.iov_max.equal",
        "pwritev.iov_max.plus_one",
        "pwritev.runtime_iov_below_configured.full",
        "pwritev.runtime_iov_below_configured.64",
        "pwritev.runtime_iov_below_configured.256",
        "pwritev.byte_cap.64k_below",
        "pwritev.byte_cap.64k_equal",
        "pwritev.byte_cap.64k_above",
        "pwritev.byte_cap.1m_below",
        "pwritev.byte_cap.1m_equal",
        "pwritev.byte_cap.1m_above",
        "pwritev.byte_cap.4m_below",
        "pwritev.byte_cap.4m_equal",
        "pwritev.byte_cap.4m_above",
        "pwritev.byte_cap.8m_below",
        "pwritev.byte_cap.8m_equal",
        "pwritev.byte_cap.8m_above",
        "pwritev.limit.zero_iov_cap_before_syscall",
        "pwritev.limit.zero_byte_cap_before_syscall",
        "pwritev.limit.byte_cap_over_isize_before_syscall",
        "pwritev.offset.over_off_t_before_syscall",
        "pwritev.offset.progress_overflow_exact_cursor",
        "copy.cap.partitions_at_batch_boundary",
        "copy.cap.oversize_batch_uses_no_copy_fallback",
        "copy.failure.prior_progress_exact_cursor_no_retry",
        "fdatasync.eio.single_attempt",
        "roll.exact_remaining_stays_old_file",
        "roll.before_first_batch",
        "roll.old_and_new_file_partitions",
        "roll.fault.old_prefix_preserved_new_eio_no_retry",
        "roll.oversize_batch_rejected",
        "canonical_batch.64m_legal",
        "canonical_batch.64m_plus_one_rejected",
        "owner.production_layout_receipts_crc_chain",
        "owner.barrier.process_zero",
        "owner.barrier.group_one",
        "owner.barrier.os_per_batch",
        "owner.os_fallback.failure.prior_progress_exact_cursor_no_retry",
        "owner.roll.new_allocate_fault_old_prefix_preserved",
        "owner.recovery.current_candidate_partial_prefix_equal",
        "owner.fdatasync.enospc_sticky_poison",
    }
)

RAW_FIELDS = (
    "variant",
    "mode",
    "payload",
    "events_per_batch",
    "owner_units",
    "physical_batches",
    "writers",
    "chain",
    "unit_shape",
    "iterations",
    "stop_reason",
    "events",
    "domain_events",
    "bytes",
    "write_syscalls",
    "short_writes",
    "interrupted",
    "max_iovecs",
    "copied_bytes",
    "barriers",
    "cpu_ns",
    "wall_ns",
    "p50_ns",
    "p95_ns",
    "p99_ns",
    "load1_before",
    "load1_after",
)
PREP_FIELDS = (
    "payload",
    "events_per_batch",
    "owner_units",
    "physical_batches",
    "writers",
    "chain",
    "unit_shape",
    "iterations",
    "stop_reason",
    "events",
    "domain_events",
    "bytes",
    "cpu_ns",
    "wall_ns",
    "p50_ns",
    "p95_ns",
    "p99_ns",
    "prepared_batches",
    "load1_before",
    "load1_after",
)
BOUNDARY_FIELDS = (
    "variant",
    "mode",
    "target_bytes",
    "physical_batches",
    "writers",
    "iterations",
    "stop_reason",
    "events",
    "bytes",
    "write_syscalls",
    "short_writes",
    "interrupted",
    "max_iovecs",
    "copied_bytes",
    "barriers",
    "cpu_ns",
    "wall_ns",
    "p50_ns",
    "p95_ns",
    "p99_ns",
    "min_payload",
    "max_payload",
    "load1_before",
    "load1_after",
)
TRACE_FIELDS = (
    "policy",
    "mode",
    "payload",
    "events_per_batch",
    "writers",
    "unit_shape",
    "trace_histogram",
    "trace_groups",
    "trace_owner_intents",
    "trace_physical_batches",
    "cycles",
    "stop_reason",
    "events",
    "domain_events",
    "bytes",
    "write_syscalls",
    "short_writes",
    "interrupted",
    "max_iovecs",
    "copied_bytes",
    "barriers",
    "cpu_ns",
    "wall_ns",
    "p50_ns",
    "p95_ns",
    "p99_ns",
    "load1_before",
    "load1_after",
)
BINARY_CONTRACT = {
    "schema": "pwritev_group.contract.v1",
    "argv_lengths": {
        "contract-json": [2],
        "correctness-json": [4],
        "verify": [2],
        "point-auto": [9, 10, 11],
        "prep-auto": [7, 8],
        "boundary-auto": [7],
        "point-trace": [8, 9],
    },
    "thresholds": {
        "point-auto": {"bytes": EXPLORATORY_BYTES, "millis": EXPLORATORY_MILLIS},
        "prep-auto": {"bytes": EXPLORATORY_BYTES, "millis": EXPLORATORY_MILLIS},
        "boundary-auto": {"bytes": BOUNDARY_BYTES, "millis": BOUNDARY_MILLIS},
        "point-trace": {"bytes": CONFIRMATION_BYTES, "millis": CONFIRMATION_MILLIS},
    },
    "fields": {
        "point-auto": list(RAW_FIELDS),
        "prep-auto": list(PREP_FIELDS),
        "boundary-auto": list(BOUNDARY_FIELDS),
        "point-trace": list(TRACE_FIELDS),
    },
    "correctness_schema": "pwritev_group.correctness.v1",
}
ROW_PROVENANCE = (
    "source_commit",
    "source_dirty",
    "harness_sha256",
    "binary_sha256",
    "runner_sha256",
    "evaluator_sha256",
    "correctness_sha256",
    "captured_at_utc",
    "hostname",
    "kernel",
    "cpu_model",
    "cpu_topology",
    "governor",
    "rustc",
    "llvm",
    "cargo",
    "build_profile",
    "scratch_root",
    "mount_source",
    "mount_fstype",
    "mount_target",
    "device_model",
    "scheduler",
    "free_bytes",
    "page_size",
    "iov_max",
    "corpus_seed",
)


@dataclass(frozen=True)
class Shape:
    payload: int
    events: int
    intents: int
    writers: int
    unit_shape: str

    @property
    def physical_batches(self) -> int:
        return self.intents * (2 if self.unit_shape == "new_name" else 1)

    @property
    def domain_batch_len(self) -> int:
        return HEADER_LEN + MARKER_LEN + self.events * (
            SUBFRAME_HEADER_LEN + self.payload
        )

    @property
    def group_bytes(self) -> int:
        per_intent = self.domain_batch_len
        if self.unit_shape == "new_name":
            per_intent += REGISTRY_BATCH_LEN
        return self.intents * per_intent

    @property
    def buffer_lengths(self) -> tuple[int, ...]:
        if self.unit_shape == "domain":
            return (self.domain_batch_len,) * self.intents
        return (REGISTRY_BATCH_LEN, self.domain_batch_len) * self.intents

    @property
    def valid(self) -> bool:
        return (
            self.domain_batch_len <= MAX_BATCH_LEN
            and self.group_bytes <= MAX_CORPUS_BYTES
        )


@dataclass(frozen=True)
class Point:
    point_id: str
    phase: str
    comparison: str
    role: str
    repetition: int
    slot: int
    order: str
    variant: str
    mode: str
    payload: int
    events_per_batch: int
    owner_intents: int
    writers: int
    unit_shape: str
    feed_source: str
    feed_weight: int

    @property
    def shape(self) -> Shape:
        return Shape(
            self.payload,
            self.events_per_batch,
            self.owner_intents,
            self.writers,
            self.unit_shape,
        )


@dataclass(frozen=True)
class TracePoint:
    point_id: str
    phase: str
    comparison: str
    role: str
    repetition: int
    slot: int
    order: str
    policy: str
    mode: str
    payload: int
    events_per_batch: int
    writers: int
    unit_shape: str
    feed_source: str
    feed_histogram: str
    trace_groups: int
    trace_owner_intents: int


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def harness_sha256(root: Path) -> str:
    digest = hashlib.sha256()
    for relative in (
        "spikes/pwritev_group/Cargo.toml",
        "spikes/pwritev_group/Cargo.lock",
        "spikes/pwritev_group/EXPERIMENT.md",
        "spikes/pwritev_group/src/lib.rs",
        "spikes/pwritev_group/src/main.rs",
        "spikes/direct_outcomes/BN-21EW-DIAG-GROUPS.csv",
        "spikes/direct_outcomes/BN-21EW-FINAL-ABBA.csv",
        "spikes/direct_outcomes/BN-21EW-GROUP-PARITY-ABBA.csv",
    ):
        path = root / relative
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def run_text(*args: str, cwd: Path | None = None) -> str:
    timeout = (
        remaining_seconds(ACTIVE_ATTEMPT_DEADLINE)
        if ACTIVE_ATTEMPT_DEADLINE is not None
        else None
    )
    try:
        return subprocess.check_output(
            args, cwd=cwd, text=True, timeout=timeout
        ).strip()
    except subprocess.TimeoutExpired as error:
        raise AttemptTimeout(
            f"absolute Attempt-2 limit expired while running {args[0]}"
        ) from error


def policy_signature(
    lengths: tuple[int, ...], iov_cap: int, byte_cap: int
) -> tuple[tuple[int, ...], ...]:
    calls: list[tuple[int, ...]] = []
    index = 0
    intra = 0
    while index < len(lengths):
        offered = 0
        call: list[int] = []
        while index < len(lengths) and len(call) < iov_cap and offered < byte_cap:
            available = lengths[index] - intra
            take = min(available, byte_cap - offered)
            call.append(take)
            offered += take
            if take < available:
                intra += take
                break
            index += 1
            intra = 0
        calls.append(tuple(call))
    return tuple(calls)


def effective_iov_cap(iov_name: str, iov_max: int) -> int:
    configured = iov_max if iov_name == "full" else int(iov_name)
    return min(configured, iov_max)


def natural_candidates(shape: Shape, iov_max: int) -> list[str]:
    if shape.physical_batches == 1:
        return []
    candidates: list[str] = []
    seen: set[tuple[tuple[int, ...], ...]] = set()
    for iov_name in IOV_NAMES:
        cap = effective_iov_cap(iov_name, iov_max)
        signature = policy_signature(shape.buffer_lengths, cap, BYTE_CAPS["8m"])
        if signature not in seen:
            seen.add(signature)
            candidates.append(f"pwritev_{iov_name}_8m")
    if shape.group_bytes <= 8 << 20:
        candidates.append("copy_contiguous")
    return candidates


def all_shapes() -> list[Shape]:
    shapes = []
    for payload in PAYLOADS:
        for events in EVENT_COUNTS:
            for intents in OWNER_INTENTS:
                for unit_shape in UNIT_SHAPES:
                    shape = Shape(
                        payload,
                        events,
                        intents,
                        min(intents, 4),
                        unit_shape,
                    )
                    if shape.valid:
                        shapes.append(shape)
    return shapes


def selection_shapes() -> list[Shape]:
    shapes: set[Shape] = set()
    for intents in (2, 4):
        for payload in PAYLOADS:
            for events in EVENT_COUNTS:
                for unit_shape in UNIT_SHAPES:
                    shape = Shape(payload, events, intents, min(intents, 4), unit_shape)
                    if shape.valid:
                        shapes.add(shape)
    for intents in (16, 64, 256, 1024, 1025):
        for payload, events in ((24, 1), (24, 10), (250, 10), (4096, 10)):
            for unit_shape in UNIT_SHAPES:
                shape = Shape(payload, events, intents, 4, unit_shape)
                if shape.valid:
                    shapes.add(shape)
    return sorted(
        shapes,
        key=lambda shape: (
            shape.payload,
            shape.events,
            shape.intents,
            shape.unit_shape,
        ),
    )


def abba_variants(candidate: str, inverse: bool) -> tuple[tuple[str, str], ...]:
    if inverse:
        return (
            (candidate, "candidate"),
            ("k_pwrite", "baseline"),
            ("k_pwrite", "baseline"),
            (candidate, "candidate"),
        )
    return (
        ("k_pwrite", "baseline"),
        (candidate, "candidate"),
        (candidate, "candidate"),
        ("k_pwrite", "baseline"),
    )


def exploratory_points(iov_max: int) -> list[Point]:
    points: list[Point] = []
    next_id = 1
    comparison_index = 0
    for shape in selection_shapes():
        mode = "process"
        for candidate in natural_candidates(shape, iov_max):
            inverse = comparison_index % 2 == 1
            order = "BAAB" if inverse else "ABBA"
            for slot, (variant, role) in enumerate(
                abba_variants(candidate, inverse), 1
            ):
                points.append(
                    Point(
                        f"e{next_id:06d}",
                        "exploratory",
                        candidate,
                        role,
                        1,
                        slot,
                        order,
                        variant,
                        mode,
                        shape.payload,
                        shape.events,
                        shape.intents,
                        shape.writers,
                        shape.unit_shape,
                        "synthetic_sweep",
                        1,
                    )
                )
                next_id += 1
            comparison_index += 1
    return points


def boundary_shapes(iov_max: int) -> list[tuple[str, int, int]]:
    shapes = []
    # Adaptive/fixed activation widths are policy transitions too. A 24-byte,
    # one-event domain batch is exactly 140 bytes in the frozen layout.
    for minimum in MIN_WIDTHS:
        for delta in (-1, 0, 1):
            batches = minimum + delta
            shapes.append(
                (f"width_{minimum}_{delta:+d}", batches * 140, batches)
            )
    # Every configured byte/copy transition gets below/equal/above evidence.
    for cap, batches in (
        (64 << 10, 64),
        (256 << 10, 256),
        (1 << 20, 256),
        (4 << 20, 1024),
        (8 << 20, 1024),
    ):
        for delta in (-1, 0, 1):
            shapes.append((f"bytes_{cap}_{delta:+d}", cap + delta, batches))
    # Likewise, exercise every fixed iovec transition and the captured runtime
    # limit. De-duplicate centers on unusual hosts with a small SC_IOV_MAX.
    for cap in sorted(
        {
            effective_iov_cap("64", iov_max),
            effective_iov_cap("256", iov_max),
            effective_iov_cap("full", iov_max),
        }
    ):
        for delta in (-1, 0, 1):
            batches = cap + delta
            if batches > 0:
                shapes.append((f"iov_{cap}_{delta:+d}", batches * 140, batches))
    return shapes


def boundary_candidates(target: int, batches: int, iov_max: int) -> list[str]:
    fixed = HEADER_LEN + SUBFRAME_HEADER_LEN + MARKER_LEN
    budget = target - batches * fixed
    q, r = divmod(budget, batches)
    lengths = tuple(fixed + q + (index < r) for index in range(batches))
    seen: set[tuple[tuple[int, ...], ...]] = set()
    out = []
    for iov_name in IOV_NAMES:
        cap = effective_iov_cap(iov_name, iov_max)
        for byte_name in BYTE_CAP_NAMES:
            signature = policy_signature(lengths, cap, BYTE_CAPS[byte_name])
            if signature not in seen:
                seen.add(signature)
                out.append(f"pwritev_{iov_name}_{byte_name}")
    if target <= 8 << 20:
        out.append("copy_contiguous")
    return out


def boundary_plan(iov_max: int) -> list[dict[str, object]]:
    rows = []
    next_id = 1
    comparison_index = 0
    for boundary, target, batches in boundary_shapes(iov_max):
        for mode in ("process",):
            for candidate in boundary_candidates(target, batches, iov_max):
                inverse = comparison_index % 2 == 1
                order = "BAAB" if inverse else "ABBA"
                for slot, (variant, role) in enumerate(
                    abba_variants(candidate, inverse), 1
                ):
                    rows.append(
                        {
                            "point_id": f"b{next_id:05d}",
                            "phase": "boundary",
                            "boundary": boundary,
                            "comparison": candidate,
                            "role": role,
                            "slot": slot,
                            "order": order,
                            "variant": variant,
                            "mode": mode,
                            "target_bytes": target,
                            "physical_batches": batches,
                            "writers": min(batches, 4),
                        }
                    )
                    next_id += 1
                comparison_index += 1
    return rows


def process_histograms(root: Path, events: int) -> list[tuple[Counter[int], str]]:
    source_events = 10 if events in (1, 10) else 100
    path = root / "spikes/direct_outcomes/BN-21EW-DIAG-GROUPS.csv"
    histograms = []
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            if int(row["batch"]) != source_events:
                continue
            histogram: Counter[int] = Counter()
            groups = int(row["owner_groups"])
            plans = int(row["owner_plans"])
            maximum = int(row["max_group"])
            extra = plans - groups
            q, remainder = divmod(extra, maximum - 1)
            histogram[maximum] += q
            used = q
            if remainder:
                histogram[1 + remainder] += 1
                used += 1
            histogram[1] += groups - used
            assert sum(histogram.values()) == groups
            assert sum(width * count for width, count in histogram.items()) == plans
            histograms.append(
                (
                    histogram,
                    f"process_diag_b{source_events}:{row['phase']}:rep{row['rep']}",
                )
            )
    if len(histograms) != 6:
        raise AssertionError("Process replay requires exactly six source rows")
    return histograms


def process_histogram(root: Path, events: int) -> tuple[Counter[int], str]:
    histogram: Counter[int] = Counter()
    sources = []
    for one, source in process_histograms(root, events):
        histogram.update(one)
        sources.append(source)
    return histogram, "+".join(sources)


def balanced_histogram(plans: int, groups: int) -> Counter[int]:
    low, remainder = divmod(plans, groups)
    histogram = Counter({low: groups - remainder})
    if remainder:
        histogram[low + 1] += remainder
    assert sum(histogram.values()) == groups
    assert sum(width * count for width, count in histogram.items()) == plans
    return histogram


def group_histograms(root: Path, events: int) -> list[tuple[Counter[int], str]]:
    histograms = []
    final = root / "spikes/direct_outcomes/BN-21EW-FINAL-ABBA.csv"
    with final.open(newline="") as handle:
        for row in csv.DictReader(handle):
            if row["mode"] != "group" or int(row["batch"]) != events:
                continue
            plans = int(row["writers"]) * int(row["bpw"])
            histograms.append(
                (
                    balanced_histogram(plans, int(row["fsyncs"])),
                    f"group_final:{row['phase']}:rep{row['rep']}",
                )
            )
    if events in (1, 10):
        parity = root / "spikes/direct_outcomes/BN-21EW-GROUP-PARITY-ABBA.csv"
        with parity.open(newline="") as handle:
            for row in csv.DictReader(handle):
                if int(row["batch"]) != events:
                    continue
                plans = int(row["writers"]) * int(row["bpw"])
                histograms.append(
                    (
                        balanced_histogram(plans, int(row["barriers"])),
                        f"group_parity:{row['phase']}",
                    )
                )
    if not histograms:
        raise AssertionError("missing Group feed evidence")
    return histograms


def group_histogram(root: Path, events: int) -> tuple[Counter[int], str]:
    histogram: Counter[int] = Counter()
    sources = []
    for one, source in group_histograms(root, events):
        histogram.update(one)
        sources.append(source)
    return histogram, "+".join(sources)


def os_batch_traces(root: Path, events: int) -> list[tuple[Counter[int], str]]:
    traces = []
    source = root / "spikes/direct_outcomes/BN-21EW-FINAL-ABBA.csv"
    with source.open(newline="") as handle:
        for row in csv.DictReader(handle):
            if row["mode"] == "os" and int(row["batch"]) == events:
                batches = int(row["writers"]) * int(row["bpw"])
                traces.append(
                    (
                        Counter({1: batches}),
                        f"os_final_b{events}:{row['phase']}:rep{row['rep']}",
                    )
                )
    if not traces:
        raise AssertionError("missing Os steady-domain batch evidence")
    return traces


def os_batch_trace(root: Path, events: int) -> tuple[Counter[int], str]:
    histogram: Counter[int] = Counter()
    sources = []
    for one, source in os_batch_traces(root, events):
        histogram.update(one)
        sources.append(source)
    return histogram, "+".join(sources)


def partition_histograms(
    source: list[tuple[Counter[int], str]], buckets: int = 6
) -> list[tuple[Counter[int], str]]:
    out = [(Counter(), []) for _ in range(buckets)]
    for index, (histogram, name) in enumerate(source):
        target, names = out[index % buckets]
        target.update(histogram)
        names.append(name)
    if any(not histogram for histogram, _ in out):
        raise AssertionError("not enough source histograms for six ABBA pairs")
    return [(histogram, "+".join(names)) for histogram, names in out]


def confirmation_histogram(
    root: Path, mode: str, events: int, writers: int
) -> tuple[Counter[int], str]:
    if writers == 1:
        return Counter({1: 1}), "single_writer"
    if mode == "group":
        return group_histogram(root, events)
    if mode == "os":
        return os_batch_trace(root, events)
    histogram, source = process_histogram(root, events)
    return histogram, source


def confirmation_trace_histograms(
    root: Path, mode: str, events: int, writers: int
) -> list[tuple[Counter[int], str]]:
    if writers == 4 and mode == "process":
        return process_histograms(root, events)
    if writers == 4 and mode == "group":
        return partition_histograms(group_histograms(root, events))
    if writers == 4 and mode == "os":
        return partition_histograms(os_batch_traces(root, events))
    histogram, source = confirmation_histogram(root, mode, events, writers)
    return [(histogram, source)] * 6


def adaptive_configs() -> list[dict[str, object]]:
    configs = []
    for minimum in MIN_WIDTHS:
        for iov in IOV_NAMES:
            for byte_name in BYTE_CAP_NAMES:
                for copy in COPY_CEILINGS:
                    configs.append(
                        {
                            "id": f"adaptive_m{minimum}_{iov}_{byte_name}_c{copy}",
                            "kind": "adaptive",
                            "min_batches": minimum,
                            "iov": iov,
                            "byte_cap": byte_name,
                            "copy_ceiling": copy,
                        }
                    )
    assert len(configs) == 144
    return configs


def fixed_rules() -> list[dict[str, object]]:
    return [
        {
            "id": f"fixed_pwritev_{iov}_8m",
            "kind": "fixed",
            "variant": f"pwritev_{iov}_8m",
            "min_batches": 2,
            "iov": iov,
            "byte_cap": "8m",
            "copy_ceiling": 0,
        }
        for iov in IOV_NAMES
    ] + [
        {
            "id": "fixed_copy_contiguous_8m",
            "kind": "fixed",
            "variant": "copy_contiguous",
            "min_batches": 2,
            "iov": "full",
            "byte_cap": "8m",
            "copy_ceiling": 8 << 20,
        }
    ]


def effective_variant(rule: dict[str, object], shape: Shape, mode: str) -> str:
    if mode == "os" or shape.physical_batches < int(rule["min_batches"]):
        return "k_pwrite"
    if rule["kind"] == "fixed":
        if rule["variant"] == "copy_contiguous" and shape.group_bytes > 8 << 20:
            return "k_pwrite"
        return str(rule["variant"])
    copy = int(rule["copy_ceiling"])
    if copy and shape.group_bytes <= copy:
        return "copy_contiguous"
    return f"pwritev_{rule['iov']}_{rule['byte_cap']}"


def trace_policy(rule: dict[str, object]) -> str:
    if rule["kind"] == "fixed":
        return f"fixed:{rule['variant']}"
    return (
        f"adaptive:{rule['min_batches']}:{rule['iov']}:"
        f"{rule['byte_cap']}:{rule['copy_ceiling']}"
    )


def confirmation_points(
    root: Path, winner: dict[str, object]
) -> list[TracePoint]:
    points: list[TracePoint] = []
    next_id = 1
    for mode in ("process", "group", "os"):
        events_values = (1, 10) if mode == "os" else CONFIRM_EVENTS
        for payload in CONFIRM_PAYLOADS:
            for events in events_values:
                for writers in CONFIRM_WRITERS:
                    trace_histograms = confirmation_trace_histograms(
                        root, mode, events, writers
                    )
                    candidate = trace_policy(winner)
                    for repetition in CONFIRM_REPETITIONS:
                        inverse = repetition % 2 == 0
                        order = "BAAB" if inverse else "ABBA"
                        for slot, (policy, role) in enumerate(
                            abba_variants(candidate, inverse), 1
                        ):
                            pair_index = (repetition - 1) * 2 + (0 if slot <= 2 else 1)
                            histogram, source = trace_histograms[pair_index]
                            encoded = ";".join(
                                f"{width}:{frequency}"
                                for width, frequency in sorted(histogram.items())
                            )
                            points.append(
                                TracePoint(
                                    f"c{next_id:06d}",
                                    "confirmation",
                                    str(winner["id"]),
                                    role,
                                    repetition,
                                    slot,
                                    order,
                                    policy,
                                    mode,
                                    payload,
                                    events,
                                    writers,
                                    "domain",
                                    source,
                                    encoded,
                                    sum(histogram.values()),
                                    sum(
                                        width * frequency
                                        for width, frequency in histogram.items()
                                    ),
                                )
                            )
                            next_id += 1
    return points


def prepared_pipeline_shapes(root: Path) -> list[Shape]:
    shapes = set(selection_shapes())
    for mode in ("process", "group", "os"):
        events_values = (1, 10) if mode == "os" else CONFIRM_EVENTS
        for payload in CONFIRM_PAYLOADS:
            for events in events_values:
                for writers in CONFIRM_WRITERS:
                    histogram, _ = confirmation_histogram(
                        root, mode, events, writers
                    )
                    for width in histogram:
                        shapes.add(Shape(payload, events, width, writers, "domain"))
    return sorted(
        shapes,
        key=lambda shape: (
            shape.payload,
            shape.events,
            shape.intents,
            shape.writers,
            shape.unit_shape,
        ),
    )


def process_names() -> set[str]:
    names = set()
    own_pid = os.getpid()
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit() or int(entry.name) == own_pid:
            continue
        try:
            names.add((entry / "comm").read_text().strip())
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            pass
    return names


def mount_facts(path: Path) -> tuple[str, str, str]:
    return tuple(
        run_text("findmnt", "-n", "-o", "SOURCE,FSTYPE,TARGET", "-T", str(path))
        .split(maxsplit=2)
    )


def block_facts(source: str) -> tuple[str, str]:
    device = Path(source).name
    parent = run_text("lsblk", "-ndo", "PKNAME", source) or device
    model = run_text("lsblk", "-ndo", "MODEL", f"/dev/{parent}")
    scheduler_path = Path(f"/sys/block/{parent}/queue/scheduler")
    scheduler = scheduler_path.read_text().strip() if scheduler_path.exists() else "unknown"
    return model, scheduler


def cpu_model() -> str:
    for line in Path("/proc/cpuinfo").read_text().splitlines():
        if line.startswith("model name"):
            return line.split(":", 1)[1].strip()
    return "unknown"


def cpu_topology() -> str:
    rows = [
        line
        for line in run_text("lscpu", "-p=CPU,CORE,SOCKET,NODE").splitlines()
        if line and not line.startswith("#")
    ]
    parsed = [tuple(map(int, row.split(","))) for row in rows]
    return (
        f"logical={len(parsed)},cores={len({(r[1], r[2]) for r in parsed})},"
        f"sockets={len({r[2] for r in parsed})},nodes={len({r[3] for r in parsed})}"
    )


def governors() -> str:
    values = {
        path.read_text().strip()
        for path in Path("/sys/devices/system/cpu").glob(
            "cpu*/cpufreq/scaling_governor"
        )
    }
    return "+".join(sorted(values)) if values else "unknown"


def provenance(
    workspace: Path,
    binary: Path,
    evaluator: Path,
    correctness: Path,
    scratch_root: Path,
    source: str,
) -> dict[str, object]:
    actual = run_text("git", "rev-parse", "HEAD", cwd=workspace)
    dirty = run_text(
        "git", "status", "--porcelain", "--untracked-files=no", cwd=workspace
    )
    if actual != source or dirty:
        raise RuntimeError(f"source/dirty mismatch: actual={actual} dirty={dirty!r}")
    mount_source, mount_fstype, mount_target = mount_facts(scratch_root)
    if mount_fstype != "ext4":
        raise RuntimeError(f"scratch filesystem must be ext4, got {mount_fstype}")
    model, scheduler = block_facts(mount_source)
    rustc = run_text("rustc", "-Vv")
    llvm = next(
        (
            line.split(":", 1)[1].strip()
            for line in rustc.splitlines()
            if line.startswith("LLVM version:")
        ),
        "unknown",
    )
    return {
        "source_commit": source,
        "source_dirty": "false",
        "harness_sha256": harness_sha256(workspace),
        "binary_sha256": sha256_file(binary),
        "runner_sha256": sha256_file(Path(__file__).resolve()),
        "evaluator_sha256": sha256_file(evaluator),
        "correctness_sha256": sha256_file(correctness),
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "hostname": socket.gethostname(),
        "kernel": platform.release(),
        "cpu_model": cpu_model(),
        "cpu_topology": cpu_topology(),
        "governor": governors(),
        "rustc": rustc.splitlines()[0],
        "llvm": llvm,
        "cargo": run_text("cargo", "-V"),
        "build_profile": "release,lto=thin,debug=true",
        "scratch_root": str(scratch_root),
        "mount_source": mount_source,
        "mount_fstype": mount_fstype,
        "mount_target": mount_target,
        "device_model": model,
        "scheduler": scheduler,
        "free_bytes": shutil.disk_usage(scratch_root).free,
        "page_size": os.sysconf("SC_PAGE_SIZE"),
        "iov_max": os.sysconf("SC_IOV_MAX"),
        "corpus_seed": "payload[i]=(i*131+17)&0xff",
        "owner_feed_hashes": {
            name: sha256_file(workspace / "spikes/direct_outcomes" / name)
            for name in (
                "BN-21EW-DIAG-GROUPS.csv",
                "BN-21EW-FINAL-ABBA.csv",
                "BN-21EW-GROUP-PARITY-ABBA.csv",
            )
        },
    }


def remaining_seconds(attempt_deadline: float) -> float:
    remaining = attempt_deadline - time.monotonic()
    if remaining <= 0:
        raise AttemptTimeout(
            f"absolute Attempt-2 limit of {ATTEMPT_TIMEOUT_SECONDS}s expired"
        )
    return remaining


def write_json(path: Path, document: dict[str, object]) -> None:
    with path.open("w") as handle:
        json.dump(document, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def completed_evidence_rows(output: Path) -> int:
    total = 0
    for name in (
        "prepared_pipeline.csv",
        "exploratory.csv",
        "boundaries.csv",
        "confirmation.csv",
    ):
        path = output / name
        if path.exists():
            with path.open(newline="") as handle:
                total += sum(1 for _ in csv.DictReader(handle))
    return total


def validate_decision_artifact(
    path: Path, phase: str, returncode: int
) -> dict[str, object]:
    try:
        document = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        raise RuntimeError(f"{phase} decision artifact is missing or invalid") from error
    if not isinstance(document, dict):
        raise RuntimeError(f"{phase} decision artifact must be an object")
    if phase == "selection":
        if document.get("schema") != "pwritev_group.selection.v1":
            raise RuntimeError("selection decision schema mismatch")
        expected = {0: {"SELECTED"}, 1: {"DECLINED"}}
        if not isinstance(document.get("candidates"), list):
            raise RuntimeError("selection decision lacks candidate evidence")
        verdict = document.get("verdict")
        winner = document.get("winner")
        if verdict == "SELECTED" and not isinstance(winner, dict):
            raise RuntimeError("selected decision lacks a winner")
        if verdict == "DECLINED" and winner is not None:
            raise RuntimeError("declined selection unexpectedly has a winner")
    elif phase == "final":
        if document.get("schema") != "pwritev_group.evaluation.v1":
            raise RuntimeError("final decision schema mismatch")
        expected = {0: {"ADOPT"}, 1: {"DECLINED", "INCONCLUSIVE_NOISE"}}
        if not isinstance(document.get("winner"), dict):
            raise RuntimeError("final decision lacks the frozen winner")
        if not isinstance(document.get("reasons"), list) or not isinstance(
            document.get("cells"), list
        ):
            raise RuntimeError("final decision lacks evidence details")
        verdict = document.get("verdict")
    else:
        raise AssertionError(f"unknown decision phase {phase}")
    if returncode not in expected or verdict not in expected[returncode]:
        raise RuntimeError(
            f"{phase} exit/artifact mismatch: exit={returncode} verdict={verdict!r}"
        )
    return document


def record_failure(
    output: Path,
    *,
    stage: str,
    label: str,
    error: Exception,
    completed_rows: int,
    point: dict[str, object] | None = None,
) -> None:
    write_json(
        output / "failure.json",
        {
            "schema": "pwritev_group.failure.v1",
            "classification": "FATAL",
            "stage": stage,
            "label": label,
            "point": point,
            "error": str(error),
            "completed_rows": completed_rows,
            "failed_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )


def record_timeout(
    output: Path,
    *,
    stage: str,
    label: str,
    error: Exception,
    completed_rows: int,
    attempt_started: float,
    attempt_started_utc: str,
) -> None:
    write_json(
        output / "attempt_outcome.json",
        {
            "schema": "pwritev_group.attempt_outcome.v1",
            "verdict": "INCONCLUSIVE_TIMEOUT",
            "stage": stage,
            "label": label,
            "error": str(error),
            "completed_rows": completed_rows,
            "attempt_timeout_seconds": ATTEMPT_TIMEOUT_SECONDS,
            "elapsed_seconds": time.monotonic() - attempt_started,
            "started_at_utc": attempt_started_utc,
            "stopped_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )


def guard(
    scratch_root: Path,
    expected_mount: tuple[str, str, str],
    attempt_deadline: float,
) -> tuple[int, float]:
    conflict = process_names() & FOREIGN_PROCESS_NAMES
    if conflict:
        raise RuntimeError(f"foreign build/benchmark process: {sorted(conflict)}")
    while os.getloadavg()[0] >= MAX_LOAD1:
        time.sleep(min(2.0, remaining_seconds(attempt_deadline)))
    remaining_seconds(attempt_deadline)
    load = os.getloadavg()[0]
    free = shutil.disk_usage(scratch_root).free
    if free < MIN_FREE_BYTES:
        raise RuntimeError(f"free bytes {free} below {MIN_FREE_BYTES}")
    if mount_facts(scratch_root) != expected_mount:
        raise RuntimeError("scratch mount identity changed")
    return free, load


def run_with_heartbeat(
    command: list[str],
    label: str,
    log,
    attempt_deadline: float,
    timeout: int = POINT_TIMEOUT_SECONDS,
) -> subprocess.CompletedProcess[str]:
    started = time.monotonic()
    last = started
    process = subprocess.Popen(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=os.environ,
    )
    while process.poll() is None:
        now = time.monotonic()
        if now >= attempt_deadline:
            process.kill()
            stdout, stderr = process.communicate()
            log.write(
                f"[{datetime.now(timezone.utc).isoformat()}] {label} "
                f"{' '.join(command)}\nstdout:\n{stdout}stderr:\n{stderr}\n"
            )
            log.flush()
            os.fsync(log.fileno())
            raise AttemptTimeout(
                f"absolute Attempt-2 limit expired while running {label}"
            )
        if now - started > timeout:
            process.kill()
            stdout, stderr = process.communicate()
            raise RuntimeError(f"{label} timed out after {timeout}s: {stderr}")
        if now - last >= 30:
            print(f"heartbeat {label} elapsed={now-started:.1f}s", flush=True)
            last = now
        time.sleep(1)
    stdout, stderr = process.communicate()
    log.write(
        f"[{datetime.now(timezone.utc).isoformat()}] {label} {' '.join(command)}\n"
        f"stdout:\n{stdout}stderr:\n{stderr}\n"
    )
    log.flush()
    os.fsync(log.fileno())
    if time.monotonic() >= attempt_deadline:
        raise AttemptTimeout(
            f"absolute Attempt-2 limit expired while completing {label}"
        )
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


def parse_csv_output(stdout: str, expected: tuple[str, ...]) -> dict[str, str]:
    lines = [line for line in stdout.splitlines() if line]
    if len(lines) != 2:
        raise RuntimeError(f"expected header+row, got {len(lines)} lines")
    rows = list(csv.DictReader(lines))
    if len(rows) != 1 or tuple(rows[0]) != expected:
        raise RuntimeError("benchmark CSV schema mismatch")
    return rows[0]


def load_rejection(raw: dict[str, str]) -> str | None:
    try:
        before = float(raw["load1_before"])
        after = float(raw["load1_after"])
    except (KeyError, ValueError) as error:
        raise RuntimeError("benchmark emitted invalid load observations") from error
    if not math.isfinite(before) or not math.isfinite(after) or before < 0 or after < 0:
        raise RuntimeError("benchmark emitted invalid load observations")
    if before >= MAX_LOAD1 or after >= MAX_LOAD1:
        return f"post-row load before={before} after={after}"
    return None


def write_rows(
    path: Path,
    fieldnames: list[str],
    rows: list[dict[str, object]],
) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        handle.flush()
        os.fsync(handle.fileno())


def row_prefix(prov: dict[str, object]) -> dict[str, object]:
    row = {field: prov[field] for field in ROW_PROVENANCE}
    row["captured_at_utc"] = datetime.now(timezone.utc).isoformat()
    return row


def point_command(binary: Path, point: Point) -> list[str]:
    return [
        str(binary),
        "point-auto",
        point.variant,
        point.mode,
        str(point.payload),
        str(point.events_per_batch),
        str(point.owner_intents),
        str(point.writers),
        point.unit_shape,
        "0",
    ]


def trace_command(binary: Path, point: TracePoint) -> list[str]:
    return [
        str(binary),
        "point-trace",
        point.policy,
        point.mode,
        str(point.payload),
        str(point.events_per_batch),
        str(point.writers),
        point.feed_histogram,
        "0",
    ]


def pipeline_command(binary: Path, shape: Shape) -> list[str]:
    return [
        str(binary),
        "prep-auto",
        str(shape.payload),
        str(shape.events),
        str(shape.intents),
        str(shape.writers),
        shape.unit_shape,
        "0",
    ]


def boundary_command(binary: Path, plan: dict[str, object]) -> list[str]:
    return [
        str(binary),
        "boundary-auto",
        str(plan["variant"]),
        str(plan["mode"]),
        str(plan["target_bytes"]),
        str(plan["physical_batches"]),
        str(plan["writers"]),
    ]


def execute_points(
    points: list[Point],
    binary: Path,
    output: Path,
    filename: str,
    prov: dict[str, object],
    scratch_root: Path,
    log,
    attempt_deadline: float,
    attempt_started: float,
    attempt_started_utc: str,
) -> int:
    metadata_fields = list(asdict(points[0])) if points else list(Point.__annotations__)
    fields = metadata_fields + list(ROW_PROVENANCE) + [
        "guard_load1",
        "status",
        "error",
    ] + [field for field in RAW_FIELDS if field not in metadata_fields]
    path = output / filename
    expected_mount = (
        str(prov["mount_source"]),
        str(prov["mount_fstype"]),
        str(prov["mount_target"]),
    )
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for index, point in enumerate(points, 1):
            prefix = asdict(point) | row_prefix(prov)
            raw: dict[str, str] = {}
            try:
                free, guard_load = guard(
                    scratch_root, expected_mount, attempt_deadline
                )
                prefix["free_bytes"] = free
                prefix["guard_load1"] = guard_load
                captured = run_with_heartbeat(
                    point_command(binary, point),
                    point.point_id,
                    log,
                    attempt_deadline,
                )
                if captured.returncode != 0:
                    raise RuntimeError(
                        f"exit {captured.returncode}: {captured.stderr.strip()}"
                    )
                raw = parse_csv_output(captured.stdout, RAW_FIELDS)
                noise = load_rejection(raw)
                if noise:
                    writer.writerow(
                        prefix
                        | {
                            "status": "rejected_noisy",
                            "error": noise,
                        }
                        | raw
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
                    print(
                        f"REJECTED_NOISY {point.point_id}: {noise}",
                        flush=True,
                    )
                else:
                    writer.writerow(
                        prefix | {"status": "accepted", "error": ""} | raw
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
            except AttemptTimeout as error:
                if not raw:
                    writer.writerow(
                        prefix | {"status": "incomplete_timeout", "error": str(error)}
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
                record_timeout(
                    output,
                    stage=filename,
                    label=point.point_id,
                    error=error,
                    completed_rows=index - 1,
                    attempt_started=attempt_started,
                    attempt_started_utc=attempt_started_utc,
                )
                print(f"INCONCLUSIVE_TIMEOUT {point.point_id}: {error}", file=sys.stderr)
                return 4
            except Exception as error:
                if not raw:
                    writer.writerow(
                        prefix | {"status": "failed", "error": str(error)}
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
                record_failure(
                    output,
                    stage=filename,
                    label=point.point_id,
                    point=asdict(point),
                    error=error,
                    completed_rows=index - 1,
                )
                print(f"FAIL-STOP {point.point_id}: {error}", file=sys.stderr)
                return 3
            if index % 25 == 0 or index == len(points):
                print(f"progress {filename} {index}/{len(points)}", flush=True)
    return 0


def execute_trace_points(
    points: list[TracePoint],
    binary: Path,
    output: Path,
    prov: dict[str, object],
    scratch_root: Path,
    log,
    attempt_deadline: float,
    attempt_started: float,
    attempt_started_utc: str,
) -> int:
    metadata_fields = list(asdict(points[0]))
    fields = metadata_fields + list(ROW_PROVENANCE) + [
        "guard_load1",
        "status",
        "error",
    ] + [field for field in TRACE_FIELDS if field not in metadata_fields]
    expected_mount = (
        str(prov["mount_source"]),
        str(prov["mount_fstype"]),
        str(prov["mount_target"]),
    )
    path = output / "confirmation.csv"
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for index, point in enumerate(points, 1):
            prefix = asdict(point) | row_prefix(prov)
            raw: dict[str, str] = {}
            try:
                free, guard_load = guard(
                    scratch_root, expected_mount, attempt_deadline
                )
                prefix["free_bytes"] = free
                prefix["guard_load1"] = guard_load
                command = trace_command(binary, point)
                captured = run_with_heartbeat(
                    command,
                    point.point_id,
                    log,
                    attempt_deadline,
                    timeout=1800,
                )
                if captured.returncode != 0:
                    raise RuntimeError(
                        f"exit {captured.returncode}: {captured.stderr.strip()}"
                    )
                raw = parse_csv_output(captured.stdout, TRACE_FIELDS)
                noise = load_rejection(raw)
                if noise:
                    writer.writerow(
                        prefix
                        | {"status": "rejected_noisy", "error": noise}
                        | raw
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
                    print(
                        f"REJECTED_NOISY {point.point_id}: {noise}",
                        flush=True,
                    )
                else:
                    writer.writerow(
                        prefix | {"status": "accepted", "error": ""} | raw
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
            except AttemptTimeout as error:
                if not raw:
                    writer.writerow(
                        prefix | {"status": "incomplete_timeout", "error": str(error)}
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
                record_timeout(
                    output,
                    stage="confirmation",
                    label=point.point_id,
                    error=error,
                    completed_rows=index - 1,
                    attempt_started=attempt_started,
                    attempt_started_utc=attempt_started_utc,
                )
                print(f"INCONCLUSIVE_TIMEOUT {point.point_id}: {error}", file=sys.stderr)
                return 4
            except Exception as error:
                if not raw:
                    writer.writerow(prefix | {"status": "failed", "error": str(error)})
                    handle.flush()
                    os.fsync(handle.fileno())
                record_failure(
                    output,
                    stage="confirmation",
                    label=point.point_id,
                    point=asdict(point),
                    error=error,
                    completed_rows=index - 1,
                )
                print(f"FAIL-STOP {point.point_id}: {error}", file=sys.stderr)
                return 3
            if index % 10 == 0 or index == len(points):
                print(f"progress confirmation {index}/{len(points)}", flush=True)
    return 0


def execute_pipeline(
    shapes: list[Shape],
    binary: Path,
    output: Path,
    prov: dict[str, object],
    scratch_root: Path,
    log,
    attempt_deadline: float,
    attempt_started: float,
    attempt_started_utc: str,
) -> int:
    fields = ["shape_id"] + list(ROW_PROVENANCE) + [
        "guard_load1",
        "status",
        "error",
    ] + list(PREP_FIELDS)
    expected_mount = (
        str(prov["mount_source"]),
        str(prov["mount_fstype"]),
        str(prov["mount_target"]),
    )
    with (output / "prepared_pipeline.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for index, shape in enumerate(shapes, 1):
            shape_id = f"s{index:04d}"
            prefix = {"shape_id": shape_id} | row_prefix(prov)
            raw: dict[str, str] = {}
            try:
                free, guard_load = guard(
                    scratch_root, expected_mount, attempt_deadline
                )
                prefix["free_bytes"] = free
                prefix["guard_load1"] = guard_load
                command = pipeline_command(binary, shape)
                captured = run_with_heartbeat(
                    command, shape_id, log, attempt_deadline
                )
                if captured.returncode != 0:
                    raise RuntimeError(f"exit {captured.returncode}")
                raw = parse_csv_output(captured.stdout, PREP_FIELDS)
                noise = load_rejection(raw)
                if noise:
                    writer.writerow(
                        prefix
                        | {"status": "rejected_noisy", "error": noise}
                        | raw
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
                    print(
                        f"REJECTED_NOISY {shape_id}: {noise}",
                        flush=True,
                    )
                else:
                    writer.writerow(
                        prefix | {"status": "accepted", "error": ""} | raw
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
            except AttemptTimeout as error:
                if not raw:
                    writer.writerow(
                        prefix | {"status": "incomplete_timeout", "error": str(error)}
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
                record_timeout(
                    output,
                    stage="prepared_pipeline",
                    label=shape_id,
                    error=error,
                    completed_rows=index - 1,
                    attempt_started=attempt_started,
                    attempt_started_utc=attempt_started_utc,
                )
                print(f"INCONCLUSIVE_TIMEOUT {shape_id}: {error}", file=sys.stderr)
                return 4
            except Exception as error:
                if not raw:
                    writer.writerow(
                        prefix | {"status": "failed", "error": str(error)}
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
                record_failure(
                    output,
                    stage="prepared_pipeline",
                    label=shape_id,
                    point={"shape_id": shape_id, "shape": asdict(shape)},
                    error=error,
                    completed_rows=index - 1,
                )
                print(f"FAIL-STOP {shape_id}: {error}", file=sys.stderr)
                return 3
    return 0


def execute_boundaries(
    plans: list[dict[str, object]],
    binary: Path,
    output: Path,
    prov: dict[str, object],
    scratch_root: Path,
    log,
    attempt_deadline: float,
    attempt_started: float,
    attempt_started_utc: str,
) -> int:
    metadata = list(plans[0])
    fields = metadata + list(ROW_PROVENANCE) + [
        "guard_load1",
        "status",
        "error",
    ] + [field for field in BOUNDARY_FIELDS if field not in metadata]
    expected_mount = (
        str(prov["mount_source"]),
        str(prov["mount_fstype"]),
        str(prov["mount_target"]),
    )
    with (output / "boundaries.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for index, plan in enumerate(plans, 1):
            prefix = plan | row_prefix(prov)
            raw: dict[str, str] = {}
            try:
                free, guard_load = guard(
                    scratch_root, expected_mount, attempt_deadline
                )
                prefix["free_bytes"] = free
                prefix["guard_load1"] = guard_load
                command = boundary_command(binary, plan)
                captured = run_with_heartbeat(
                    command,
                    str(plan["point_id"]),
                    log,
                    attempt_deadline,
                )
                if captured.returncode != 0:
                    raise RuntimeError(f"exit {captured.returncode}")
                raw = parse_csv_output(captured.stdout, BOUNDARY_FIELDS)
                noise = load_rejection(raw)
                if noise:
                    writer.writerow(
                        prefix
                        | {"status": "rejected_noisy", "error": noise}
                        | raw
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
                    print(
                        f"REJECTED_NOISY {plan['point_id']}: {noise}",
                        flush=True,
                    )
                else:
                    writer.writerow(
                        prefix | {"status": "accepted", "error": ""} | raw
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
            except AttemptTimeout as error:
                if not raw:
                    writer.writerow(
                        prefix | {"status": "incomplete_timeout", "error": str(error)}
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
                record_timeout(
                    output,
                    stage="boundaries",
                    label=str(plan["point_id"]),
                    error=error,
                    completed_rows=index - 1,
                    attempt_started=attempt_started,
                    attempt_started_utc=attempt_started_utc,
                )
                print(
                    f"INCONCLUSIVE_TIMEOUT {plan['point_id']}: {error}",
                    file=sys.stderr,
                )
                return 4
            except Exception as error:
                if not raw:
                    writer.writerow(
                        prefix | {"status": "failed", "error": str(error)}
                    )
                    handle.flush()
                    os.fsync(handle.fileno())
                record_failure(
                    output,
                    stage="boundaries",
                    label=str(plan["point_id"]),
                    point=plan,
                    error=error,
                    completed_rows=index - 1,
                )
                print(f"FAIL-STOP {plan['point_id']}: {error}", file=sys.stderr)
                return 3
            if index % 25 == 0 or index == len(plans):
                print(f"progress boundaries {index}/{len(plans)}", flush=True)
    return 0


def matrix_summary(root: Path, iov_max: int) -> dict[str, object]:
    exploratory = exploratory_points(iov_max)
    boundaries = boundary_plan(iov_max)
    pipeline_shapes = prepared_pipeline_shapes(root)
    placeholder = adaptive_configs()[0]
    confirmation = confirmation_points(root, placeholder)
    def point_target(group: int, threshold: int) -> int:
        return min(
            math.ceil(threshold / group) * group,
            matrix_cap_for_group(group),
        )
    trace_cycle_bytes = {
        point.point_id: point.trace_owner_intents
        * Shape(
            point.payload,
            point.events_per_batch,
            1,
            point.writers,
            "domain",
        ).domain_batch_len
        for point in confirmation
    }
    logical_by_phase = {
        "prepared_pipeline": sum(shape.group_bytes for shape in pipeline_shapes),
        "exploratory": sum(point.shape.group_bytes for point in exploratory),
        "boundary": sum(int(row["target_bytes"]) for row in boundaries),
        "confirmation": sum(trace_cycle_bytes[point.point_id] for point in confirmation),
    }
    byte_target_by_phase = {
        "prepared_pipeline": sum(
            point_target(shape.group_bytes, EXPLORATORY_BYTES)
            for shape in pipeline_shapes
        ),
        "exploratory": sum(
            point_target(point.shape.group_bytes, EXPLORATORY_BYTES)
            for point in exploratory
        ),
        "boundary": sum(
            point_target(int(row["target_bytes"]), BOUNDARY_BYTES)
            for row in boundaries
        ),
        "confirmation": sum(
            point_target(
                trace_cycle_bytes[point.point_id], CONFIRMATION_BYTES
            )
            for point in confirmation
        ),
    }
    device_by_phase = {
        "exploratory": sum(
            3 * point.shape.group_bytes + 2 * SEGMENT_HEADER_LEN
            for point in exploratory
        ),
        "boundary": sum(
            3 * int(row["target_bytes"]) + 2 * SEGMENT_HEADER_LEN
            for row in boundaries
        ),
        "confirmation": sum(
            Shape(
                point.payload,
                point.events_per_batch,
                max(int(entry.split(":")[0]) for entry in point.feed_histogram.split(";")),
                point.writers,
                "domain",
            ).group_bytes
            + 2 * trace_cycle_bytes[point.point_id]
            + 2 * SEGMENT_HEADER_LEN
            for point in confirmation
        ),
    }
    device_target_by_phase = {
        "exploratory": sum(
            2 * point.shape.group_bytes
            + point_target(point.shape.group_bytes, EXPLORATORY_BYTES)
            + 2 * SEGMENT_HEADER_LEN
            for point in exploratory
        ),
        "boundary": sum(
            2 * int(row["target_bytes"])
            + point_target(int(row["target_bytes"]), BOUNDARY_BYTES)
            + 2 * SEGMENT_HEADER_LEN
            for row in boundaries
        ),
        "confirmation": sum(
            Shape(
                point.payload,
                point.events_per_batch,
                max(int(entry.split(":")[0]) for entry in point.feed_histogram.split(";")),
                point.writers,
                "domain",
            ).group_bytes
            + trace_cycle_bytes[point.point_id]
            + point_target(
                trace_cycle_bytes[point.point_id], CONFIRMATION_BYTES
            )
            + 2 * SEGMENT_HEADER_LEN
            for point in confirmation
        ),
    }
    counts = {
        "prepared_pipeline": len(pipeline_shapes),
        "exploratory": len(exploratory),
        "boundary": len(boundaries),
        "confirmation_if_selected": len(confirmation),
        "adaptive_configs": 144,
        "fixed_rules": len(fixed_rules()),
    }
    wall_seconds = {
        "correctness_and_setup": 600,
        "prepared_pipeline": math.ceil(len(pipeline_shapes) * 1.5),
        "exploratory": math.ceil(len(exploratory) * 3.0),
        "boundary": math.ceil(len(boundaries) * 3.0),
        "confirmation_if_selected": math.ceil(
            sum(
                2.0
                + 0.004
                * (
                    point.trace_groups
                    if point.mode == "group"
                    else point.trace_owner_intents
                    if point.mode == "os"
                    else 0
                )
                for point in confirmation
            )
        ),
    }
    return {
        "counts": counts,
        "guaranteed_minimum_logical_bytes": logical_by_phase,
        "guaranteed_minimum_device_write_bytes": device_by_phase,
        "byte_threshold_logical_bytes_if_time_not_first": byte_target_by_phase,
        "byte_threshold_device_write_bytes_if_time_not_first": device_target_by_phase,
        "conservative_wall_seconds": wall_seconds,
        "conservative_wall_total_seconds": sum(wall_seconds.values()),
        "scratch_peak_required_bytes": MIN_FREE_BYTES,
    }


def matrix_cap_for_group(group_bytes: int) -> int:
    if group_bytes > MAX_CORPUS_BYTES:
        raise AssertionError("group exceeds safety cap")
    cycles = MAX_CORPUS_BYTES // group_bytes
    return max(1, cycles) * group_bytes


def validate_correctness(path: Path, binary_hash: str) -> None:
    document = json.loads(path.read_text())
    if document.get("schema") != "pwritev_group.correctness.v1":
        raise RuntimeError("correctness schema mismatch")
    if document.get("binary_sha256") != binary_hash:
        raise RuntimeError("correctness artifact binary hash mismatch")
    if document.get("all_passed") is not True:
        raise RuntimeError("correctness artifact is not all-pass")
    cases = document.get("cases")
    if not isinstance(cases, list):
        raise RuntimeError("correctness cases must be a list")
    names = [case.get("name") for case in cases if isinstance(case, dict)]
    if len(names) != len(cases) or len(names) != len(set(names)):
        raise RuntimeError("correctness cases are malformed or duplicated")
    observed = set(names)
    if observed != REQUIRED_CORRECTNESS_CASES:
        raise RuntimeError(
            "correctness exact-set mismatch: "
            f"missing={sorted(REQUIRED_CORRECTNESS_CASES - observed)} "
            f"extra={sorted(observed - REQUIRED_CORRECTNESS_CASES)}"
        )
    if set(document.get("required_cases", [])) != REQUIRED_CORRECTNESS_CASES:
        raise RuntimeError("binary required-case declaration differs from frozen set")
    failed = [case["name"] for case in cases if case.get("status") != "pass"]
    if failed:
        raise RuntimeError(f"correctness cases failed: {sorted(failed)}")


def validate_binary_contract(
    binary: Path, attempt_deadline: float | None = None
) -> None:
    try:
        captured = subprocess.run(
            [str(binary), "contract-json"],
            text=True,
            capture_output=True,
            check=False,
            timeout=(
                remaining_seconds(attempt_deadline)
                if attempt_deadline is not None
                else None
            ),
        )
    except subprocess.TimeoutExpired as error:
        raise AttemptTimeout(
            "Attempt-2 limit expired during contract validation"
        ) from error
    if captured.returncode != 0:
        raise RuntimeError(f"binary contract command failed: {captured.stderr}")
    try:
        observed = json.loads(captured.stdout)
    except ValueError as error:
        raise RuntimeError("binary contract is not valid JSON") from error
    if observed != BINARY_CONTRACT:
        raise RuntimeError(
            "release binary contract mismatch:\n"
            f"expected={json.dumps(BINARY_CONTRACT, sort_keys=True)}\n"
            f"observed={json.dumps(observed, sort_keys=True)}"
        )


def run_matrix_once(
    args: argparse.Namespace,
    attempt_started: float,
    attempt_started_utc: str,
    attempt_deadline: float,
) -> int:
    workspace = args.workspace.resolve()
    binary = args.binary.resolve()
    output = args.output.resolve()
    evaluator = Path(__file__).with_name("evaluate.py").resolve()
    if output.exists():
        raise RuntimeError(f"refusing existing output: {output}")
    output.mkdir(parents=True)
    remaining_seconds(attempt_deadline)
    actual_source = run_text("git", "rev-parse", "HEAD", cwd=workspace)
    initial_dirty = run_text(
        "git", "status", "--porcelain", "--untracked-files=all", cwd=workspace
    )
    if actual_source != args.source or initial_dirty:
        raise RuntimeError(
            f"workspace must be completely clean before output creation: "
            f"actual={actual_source} dirty={initial_dirty!r}"
        )
    scratch_root = Path(
        os.environ.get("MESS_BENCH_DIR", Path.home() / ".cache/mess-bench")
    ).resolve()
    scratch_root.mkdir(parents=True, exist_ok=True)
    binary_hash = sha256_file(binary)
    validate_binary_contract(binary, attempt_deadline)
    remaining_seconds(attempt_deadline)
    correctness = output / "correctness.json"
    try:
        correctness_run = subprocess.run(
            [str(binary), "correctness-json", str(correctness), binary_hash],
            text=True,
            capture_output=True,
            check=False,
            timeout=remaining_seconds(attempt_deadline),
        )
    except subprocess.TimeoutExpired as error:
        raise AttemptTimeout("Attempt-2 limit expired during correctness") from error
    if correctness_run.returncode != 0:
        raise RuntimeError(f"correctness command failed: {correctness_run.stderr}")
    validate_correctness(correctness, binary_hash)
    prov = provenance(
        workspace, binary, evaluator, correctness, scratch_root, args.source
    )
    write_json(
        output / "attempt_protocol.json",
        {
            "schema": "pwritev_group.attempt_protocol.v1",
            "attempt": 2,
            "source_commit": args.source,
            "started_at_utc": attempt_started_utc,
            "absolute_timeout_seconds": ATTEMPT_TIMEOUT_SECONDS,
            "prior_attempt_classification": "INCONCLUSIVE_INFRASTRUCTURE",
            "prior_attempt_evidence": (
                "spikes/pwritev_group/evidence/"
                "attempt1-inconclusive-infrastructure"
            ),
            "prior_attempt_timing_rows_reused": False,
            "post_row_noise": "retain_and_continue_after_next_pre_row_guard",
            "row_retry_or_replacement": False,
            "stage1_physical_comparison_clean_rows_required": 4,
            "stage2_adjacent_clean_pairs_required_for_adopt": 6,
        },
    )
    iov_max = int(prov["iov_max"])
    summary = matrix_summary(workspace, iov_max)
    (output / "plan.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    (output / "provenance.json").write_text(
        json.dumps(prov, indent=2, sort_keys=True) + "\n"
    )
    rules = fixed_rules() + adaptive_configs()
    (output / "adaptive_rules.json").write_text(
        json.dumps(rules, indent=2, sort_keys=True) + "\n"
    )
    exploratory = exploratory_points(iov_max)
    boundaries = boundary_plan(iov_max)
    write_rows(
        output / "exploratory_manifest.csv",
        list(asdict(exploratory[0])),
        [asdict(point) for point in exploratory],
    )
    write_rows(
        output / "boundary_manifest.csv",
        list(boundaries[0]),
        boundaries,
    )
    with (output / "run.log").open("w") as log:
        result = execute_pipeline(
            prepared_pipeline_shapes(workspace),
            binary,
            output,
            prov,
            scratch_root,
            log,
            attempt_deadline,
            attempt_started,
            attempt_started_utc,
        )
        if result:
            return result
        result = execute_points(
            exploratory,
            binary,
            output,
            "exploratory.csv",
            prov,
            scratch_root,
            log,
            attempt_deadline,
            attempt_started,
            attempt_started_utc,
        )
        if result:
            return result
        result = execute_boundaries(
            boundaries,
            binary,
            output,
            prov,
            scratch_root,
            log,
            attempt_deadline,
            attempt_started,
            attempt_started_utc,
        )
        if result:
            return result
        selection = output / "selection.json"
        try:
            selected = subprocess.run(
                [
                    str(evaluator),
                    "select",
                    str(output),
                    str(selection),
                ],
                text=True,
                capture_output=True,
                check=False,
                timeout=remaining_seconds(attempt_deadline),
            )
        except subprocess.TimeoutExpired as error:
            raise AttemptTimeout("Attempt-2 limit expired during selection") from error
        (output / "selection.log").write_text(selected.stdout + selected.stderr)
        selection_doc = validate_decision_artifact(
            selection, "selection", selected.returncode
        )
        if selection_doc["verdict"] == "DECLINED":
            print(selected.stdout, end="")
            return 1
        winner = selection_doc["winner"]
        confirmation = confirmation_points(workspace, winner)
        write_rows(
            output / "confirmation_manifest.csv",
            list(asdict(confirmation[0])),
            [asdict(point) for point in confirmation],
        )
        result = execute_trace_points(
            confirmation,
            binary,
            output,
            prov,
            scratch_root,
            log,
            attempt_deadline,
            attempt_started,
            attempt_started_utc,
        )
        if result:
            return result
    try:
        final = subprocess.run(
            [str(evaluator), "final", str(output)],
            text=True,
            capture_output=True,
            check=False,
            timeout=remaining_seconds(attempt_deadline),
        )
    except subprocess.TimeoutExpired as error:
        raise AttemptTimeout("Attempt-2 limit expired during final evaluation") from error
    (output / "evaluation.txt").write_text(final.stdout + final.stderr)
    print(final.stdout + final.stderr, end="")
    validate_decision_artifact(
        output / "evaluation.json", "final", final.returncode
    )
    return final.returncode


def run_matrix(args: argparse.Namespace) -> int:
    global ACTIVE_ATTEMPT_DEADLINE
    attempt_started = time.monotonic()
    attempt_started_utc = datetime.now(timezone.utc).isoformat()
    attempt_deadline = attempt_started + ATTEMPT_TIMEOUT_SECONDS
    ACTIVE_ATTEMPT_DEADLINE = attempt_deadline
    output = args.output.resolve()
    output_preexisting = output.exists()
    try:
        return run_matrix_once(
            args,
            attempt_started,
            attempt_started_utc,
            attempt_deadline,
        )
    except AttemptTimeout as error:
        if output.exists() and not output_preexisting:
            record_timeout(
                output,
                stage="runner",
                label="preflight_or_evaluation",
                error=error,
                completed_rows=completed_evidence_rows(output),
                attempt_started=attempt_started,
                attempt_started_utc=attempt_started_utc,
            )
        print(f"INCONCLUSIVE_TIMEOUT: {error}", file=sys.stderr)
        return 4
    except Exception as error:
        if output.exists() and not output_preexisting:
            record_failure(
                output,
                stage="runner",
                label="preflight_or_evaluation",
                error=error,
                completed_rows=completed_evidence_rows(output),
            )
        print(f"FAIL-STOP: {error}", file=sys.stderr)
        return 3
    finally:
        ACTIVE_ATTEMPT_DEADLINE = None


def self_test(root: Path) -> int:
    iov_max = os.sysconf("SC_IOV_MAX")
    assert len(adaptive_configs()) == 144
    assert len(fixed_rules()) == 4
    assert effective_iov_cap("full", 32) == 32
    assert effective_iov_cap("64", 32) == 32
    assert effective_iov_cap("256", 128) == 128
    assert ATTEMPT_TIMEOUT_SECONDS == 2 * 60 * 60
    assert load_rejection({"load1_before": "5.9", "load1_after": "5.99"}) is None
    assert load_rejection(
        {"load1_before": "6.0", "load1_after": "5.0"}
    ).startswith("post-row load")
    assert load_rejection(
        {"load1_before": "5.0", "load1_after": "6.0"}
    ).startswith("post-row load")
    with tempfile.TemporaryDirectory() as directory:
        failure_root = Path(directory)
        record_failure(
            failure_root,
            stage="self-test",
            label="fixture",
            error=RuntimeError("fixture failure"),
            completed_rows=7,
        )
        failure = json.loads((failure_root / "failure.json").read_text())
        assert failure["classification"] == "FATAL"
        assert failure["stage"] == "self-test"
        assert failure["completed_rows"] == 7
        selection = failure_root / "selection.json"
        write_json(
            selection,
            {
                "schema": "pwritev_group.selection.v1",
                "verdict": "DECLINED",
                "winner": None,
                "candidates": [],
            },
        )
        assert validate_decision_artifact(selection, "selection", 1)[
            "verdict"
        ] == "DECLINED"
        try:
            validate_decision_artifact(
                failure_root / "missing-selection.json", "selection", 1
            )
        except RuntimeError:
            pass
        else:
            raise AssertionError("missing selector artifact was accepted")
        write_json(
            failure_root / "evaluation.json",
            {
                "schema": "pwritev_group.evaluation.v1",
                "verdict": "INCONCLUSIVE_NOISE",
                "winner": {"id": "fixture"},
                "reasons": ["fixture noise"],
                "cells": [],
            },
        )
        assert validate_decision_artifact(
            failure_root / "evaluation.json", "final", 1
        )["verdict"] == "INCONCLUSIVE_NOISE"
    with tempfile.TemporaryDirectory() as directory:
        failed_output = Path(directory) / "fatal-preflight"
        failed = run_matrix(
            argparse.Namespace(
                workspace=root,
                binary=Path("/does/not/exist"),
                source="deliberately-wrong-source",
                output=failed_output,
            )
        )
        assert failed == 3
        failure = json.loads((failed_output / "failure.json").read_text())
        assert failure["classification"] == "FATAL"
        assert failure["stage"] == "runner"
    low_runtime_centers = {
        name.rsplit("_", 1)[0]
        for name, _, _ in boundary_shapes(32)
        if name.startswith("iov_")
    }
    assert low_runtime_centers == {"iov_32"}
    for events in CONFIRM_EVENTS:
        for mode in ("process", "group", "os"):
            histogram, _ = confirmation_histogram(root, mode, events, 4)
            assert histogram
            assert min(histogram) >= 1
    exploratory = exploratory_points(iov_max)
    boundaries = boundary_plan(iov_max)
    confirmation = confirmation_points(root, adaptive_configs()[0])
    pipeline = prepared_pipeline_shapes(root)
    assert len({point.point_id for point in exploratory}) == len(exploratory)
    assert len({row["point_id"] for row in boundaries}) == len(boundaries)
    binary = Path("/predeclared/pwritev_group")
    command_samples = {
        "point-auto": point_command(binary, exploratory[0]),
        "prep-auto": pipeline_command(binary, pipeline[0]),
        "boundary-auto": boundary_command(binary, boundaries[0]),
        "point-trace": trace_command(binary, confirmation[0]),
        "correctness-json": [str(binary), "correctness-json", "out", "hash"],
        "contract-json": [str(binary), "contract-json"],
    }
    for command, argv in command_samples.items():
        assert len(argv) in BINARY_CONTRACT["argv_lengths"][command]
    summary = matrix_summary(root, iov_max)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--source")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    if args.self_test:
        return self_test(root)
    if not all((args.workspace, args.binary, args.source, args.output)):
        parser.error("--workspace, --binary, --source, and --output are required")
    return run_matrix(args)


if __name__ == "__main__":
    raise SystemExit(main())
