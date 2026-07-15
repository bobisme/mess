#!/usr/bin/env python3
"""Fail-closed evaluator for the bn-22it Process owned-append gate.

Admission mode consumes a fresh 80-row matrix and its bound provenance.  The
explicit fixture mode exercises the same parser, artifact bindings, physical
order, and gate implementation, but can only report a fixture result: it can
never emit ``ADOPT`` or ``DECLINED``.
"""

from __future__ import annotations

import csv
import builtins
import dis
import errno
import fcntl
import hashlib
import io
import json
import math
import os
import re
import shlex
import socket
import stat
import statistics
import subprocess
import sys
import tempfile
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from types import FunctionType
from typing import Any


MODES = ("process",)
BATCHES = (1, 10, 100, 1000)
CYCLES = range(1, 6)
VARIANTS = ("control", "candidate")
WRITERS = 4
PAYLOAD = 250
PROCESS_BPW = {1: 40_000, 10: 12_500, 100: 2_500, 1000: 250}

# These are the retained bn-2yye thresholds.  bn-22it deliberately changes
# evidence handling, not the performance decision.
THROUGHPUT_FLOOR = 0.97
P99_CEILING = 1.10
ALLOCATION_CEILING = 1.05
MATERIAL_THROUGHPUT_FLOOR = 1.10
MATERIAL_ALLOCATION_CEILING = 0.90

PROTOCOL = "bn-22it-process-owned-v1"
RESULT_SCHEMA = "bn-22it-evaluation-result-v1"
FIXTURE_MARKER = "bn-22it-tooling-fixture-v1"
SHA256 = re.compile(r"[0-9a-f]{64}")
COMMIT = re.compile(r"[0-9a-f]{40}")
FORBIDDEN_COMM = frozenset(
    {"cargo", "rustc", "cc", "ld", "collect2", "owned_append_be"}
)
SOURCE_APPROVAL_SCHEMA = "bn-22it-source-approval-v1"
PREPARED_PAIR_SCHEMA = "bn-22it-prepared-pair-v2"
BUILD_ATTESTATION_SCHEMA = "bn-22it-build-attestation-v2"
SOURCE_MANIFEST_SCHEMA = "bn-22it-source-manifest-v1"
PREPARE_PRE_RELEASE_SCHEMA = "bn-22it-prepare-pre-release-v1"
PREPARE_RELEASE_SCHEMA = "bn-22it-prepare-release-v1"
PREPARE_TERMINAL_SCHEMA = "bn-22it-prepare-terminal-v1"
PAIR_CLAIM_SCHEMA = "bn-22it-pair-consumption-v1"
TERMINAL_VERIFICATION_SCHEMA = "bn-22it-terminal-verification-v1"

EXIT_ADOPT = 0
EXIT_USAGE = 2
EXIT_DECLINED = 10
EXIT_INVALID = 20
EXIT_INTERNAL = 30

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

INTEGER_FIELDS = (
    "cycle",
    "slot",
    "batch",
    "writers",
    "bpw",
    "payload",
    "events",
    "allocs",
    "alloc_bytes",
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
)

FLOAT_FIELDS = (
    "ev_s",
    "p50_us",
    "p99_us",
    "allocs_per_event",
    "alloc_bytes_per_event",
    "pre_load1",
    "post_load1",
)

COMMON_PROVENANCE_FIELDS = (
    "protocol",
    "evidence_mode",
    "paired_csv_path",
    "paired_csv_sha256",
    "paired_csv_bytes",
    "paired_csv_data_rows",
    "paired_csv_columns",
    "lease_event_path",
    "lease_event_sha256",
    "lease_event_records",
    "guard_manifest_path",
    "guard_manifest_sha256",
    "guard_manifest_records",
    "child_manifest_path",
    "child_manifest_sha256",
    "child_manifest_records",
    "smoke_manifest_path",
    "smoke_manifest_sha256",
    "smoke_manifest_records",
)

ADMISSION_PROVENANCE_FIELDS = (
    "baseline_source",
    "baseline_tree",
    "tooling_source",
    "tooling_tree",
    "source_approval_path",
    "source_approval_sha256",
    "source_approval_schema",
    "source_review_id",
    "source_review_status",
    "prepare_manifest_path",
    "prepare_manifest_sha256",
    "prepare_pre_release_path",
    "prepare_pre_release_sha256",
    "prepare_release_path",
    "prepare_release_sha256",
    "prepare_terminal_path",
    "prepare_terminal_sha256",
    "prepare_failure_path",
    "prepare_failure_absent",
    "pair_claim_path",
    "pair_claim_sha256",
    "pair_claim_schema",
    "prepare_runner_path",
    "prepare_runner_sha256",
    "prepare_orchestrator_path",
    "prepare_orchestrator_sha256",
    "control_source",
    "candidate_source",
    "control_tree",
    "candidate_tree",
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
    "orchestrator_path",
    "evaluator_path",
    "control_harness_sha256",
    "candidate_harness_sha256",
    "control_binary_sha256",
    "candidate_binary_sha256",
    "control_cargo_lock_sha256",
    "candidate_cargo_lock_sha256",
    "control_diff_manifest_sha256",
    "candidate_diff_manifest_sha256",
    "control_patch_sha256",
    "candidate_patch_sha256",
    "control_build_attestation_path",
    "candidate_build_attestation_path",
    "control_build_attestation_sha256",
    "candidate_build_attestation_sha256",
    "control_build_nonce",
    "candidate_build_nonce",
    "control_build_log_path",
    "candidate_build_log_path",
    "control_build_log_sha256",
    "candidate_build_log_sha256",
    "control_target_dir",
    "candidate_target_dir",
    "control_materialized_root",
    "candidate_materialized_root",
    "control_source_archive_path",
    "candidate_source_archive_path",
    "control_source_archive_sha256",
    "candidate_source_archive_sha256",
    "control_tree_manifest_path",
    "candidate_tree_manifest_path",
    "control_tree_manifest_sha256",
    "candidate_tree_manifest_sha256",
    "control_materialized_manifest_path",
    "candidate_materialized_manifest_path",
    "control_materialized_manifest_sha256",
    "candidate_materialized_manifest_sha256",
    "control_sandbox_path",
    "candidate_sandbox_path",
    "control_sandbox_sha256",
    "candidate_sandbox_sha256",
    "runner_sha256",
    "orchestrator_sha256",
    "evaluator_sha256",
    "physical_order_sha256",
    "started_at",
    "command",
    "kernel",
    "rustc",
    "cargo",
    "bench_dir",
    "filesystem_source",
    "filesystem_type",
    "filesystem_target",
    "filesystem_free_bytes",
    "filesystem_total_bytes",
    "cpu_count",
    "cpu_online",
    "cpu_topology_sha256",
    "page_size",
    "governors",
    "lease_path",
    "lease_device",
    "lease_inode",
    "lease_holder_pid",
    "lease_holder_starttime",
    "lease_holder_uid",
    "lease_hostname",
    "lease_acquired_at",
    "lease_nonce",
    "lease_boot_id",
    "coordination_confirmed",
)

MANIFESTS = {
    "lease_event": 1,
    "guard_manifest": 161,
    "child_manifest": 80,
    "smoke_manifest": 1,
}


def timestamp() -> str:
    return datetime.now(UTC).isoformat()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def command_output(argv: list[str], cwd: Path | None = None) -> str:
    """Run one bounded observation command and return its exact trimmed stdout."""

    result = subprocess.run(
        argv,
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result.stdout.strip()


def canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        + "\n"
    ).encode()


def read_canonical_json(
    path: Path,
    label: str,
    errors: list[str],
) -> dict[str, Any] | None:
    try:
        payload = path.read_bytes()
        value = json.loads(payload)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        errors.append(f"cannot read {label} {path}: {error}")
        return None
    if not isinstance(value, dict):
        errors.append(f"{label} is not a JSON object")
        return None
    if payload != canonical_json_bytes(value):
        errors.append(f"{label} is not canonical JSON")
    return value


def read_json_object(
    path: Path,
    label: str,
    errors: list[str],
) -> dict[str, Any] | None:
    """Read a JSON object whose producer does not promise compact encoding."""

    try:
        payload = path.read_bytes()
        value = json.loads(payload)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        errors.append(f"cannot read {label} {path}: {error}")
        return None
    if not isinstance(value, dict):
        errors.append(f"{label} is not a JSON object")
        return None
    stable = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    if payload != stable:
        errors.append(f"{label} is not stable sorted JSON")
    return value


def parse_timestamp(
    value: Any,
    label: str,
    errors: list[str],
) -> datetime | None:
    if not isinstance(value, str) or not value:
        errors.append(f"{label} is not a nonempty timestamp")
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        errors.append(f"{label} is not ISO-8601: {value!r}")
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        errors.append(f"{label} has no UTC offset: {value!r}")
        return None
    return parsed


def proc_starttime(pid: int) -> int:
    stat = (Path("/proc") / str(pid) / "stat").read_text()
    close = stat.rfind(")")
    if close < 0:
        raise OSError(f"malformed /proc/{pid}/stat")
    rest = stat[close + 2 :].split()
    if len(rest) <= 19:
        raise OSError(f"short /proc/{pid}/stat")
    return int(rest[19])


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    """Durably publish *path* exactly once as one complete JSON object."""

    path = path.resolve()
    path.parent.mkdir(parents=False, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    payload = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o644,
        )
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            # fdopen owns descriptor after construction.
            raise
        # A second evaluator invocation may not overwrite a terminal result.
        # Linking is an atomic no-replace publication on the same filesystem.
        os.link(temporary, path)
        temporary.unlink()
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


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


def physical_order_sha256() -> str:
    rows = [
        {
            "ordinal": ordinal,
            "mode": mode,
            "batch": batch,
            "cycle": cycle,
            "slot": slot,
            "variant": variant,
        }
        for ordinal, (mode, batch, cycle, slot, variant) in enumerate(
            physical_order(),
            start=1,
        )
    ]
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def parse_provenance(
    path: Path,
    required: tuple[str, ...],
    errors: list[str],
) -> dict[str, str]:
    values: dict[str, str] = {}
    required_set = set(required)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        errors.append(f"cannot read provenance {path}: {error}")
        return values
    for line_number, line in enumerate(lines, start=1):
        key, separator, value = line.partition("=")
        if not separator or key not in required_set:
            continue
        if key in values:
            errors.append(
                f"duplicate provenance field {key!r} at line {line_number}"
            )
        else:
            values[key] = value
    for key in required:
        if key not in values:
            errors.append(f"missing provenance field {key}")
    return values


def parse_nonnegative_integer(
    value: str,
    label: str,
    errors: list[str],
) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        errors.append(f"{label} is not an integer: {value!r}")
        return None
    if parsed < 0:
        errors.append(f"{label} is negative: {parsed}")
        return None
    return parsed


def is_within(path: Path, directory: Path) -> bool:
    try:
        path.relative_to(directory)
    except ValueError:
        return False
    return True


def read_jsonl(path: Path, label: str, errors: list[str]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    try:
        with path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    value = json.loads(line)
                except json.JSONDecodeError as error:
                    errors.append(
                        f"{label} line {line_number} is not JSON: {error}"
                    )
                    continue
                if not isinstance(value, dict):
                    errors.append(f"{label} line {line_number} is not an object")
                    continue
                records.append(value)
    except (OSError, UnicodeError) as error:
        errors.append(f"cannot read {label} {path}: {error}")
    return records


def record_integer(
    record: dict[str, Any],
    field: str,
    context: str,
    errors: list[str],
    *,
    positive: bool = False,
) -> int | None:
    value = record.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        errors.append(f"{context} {field} is not an integer: {value!r}")
        return None
    if value < (1 if positive else 0):
        qualifier = "positive" if positive else "nonnegative"
        errors.append(f"{context} {field} is not {qualifier}: {value}")
        return None
    return value


def record_sha256(
    record: dict[str, Any],
    field: str,
    context: str,
    errors: list[str],
) -> str | None:
    value = record.get(field)
    if not isinstance(value, str) or not SHA256.fullmatch(value):
        errors.append(f"{context} {field} is not SHA-256: {value!r}")
        return None
    return value


def require_protocol(
    record: dict[str, Any],
    context: str,
    errors: list[str],
) -> None:
    if record.get("protocol") != PROTOCOL:
        errors.append(
            f"{context} protocol {record.get('protocol')!r} != {PROTOCOL!r}"
        )


def validate_lease_held(
    lock_path: Path,
    lock_stat: os.stat_result,
    holder_pid: int | None,
    context: str,
    errors: list[str],
) -> None:
    """Prove both kernel-visible ownership and non-blocking contention."""

    expected_device = (os.major(lock_stat.st_dev), os.minor(lock_stat.st_dev))
    found_kernel_lock = False
    try:
        lock_lines = Path("/proc/locks").read_text().splitlines()
    except (OSError, UnicodeError) as error:
        errors.append(f"{context} cannot read /proc/locks: {error}")
    else:
        for line in lock_lines:
            fields = line.split()
            if len(fields) < 8 or fields[1:4] != ["FLOCK", "ADVISORY", "WRITE"]:
                continue
            try:
                pid = int(fields[4])
                major_text, minor_text, inode_text = fields[5].split(":", 2)
                device = (int(major_text, 16), int(minor_text, 16))
                inode = int(inode_text)
            except (TypeError, ValueError):
                continue
            if (
                pid == holder_pid
                and device == expected_device
                and inode == lock_stat.st_ino
            ):
                found_kernel_lock = True
                break
        if not found_kernel_lock:
            errors.append(
                f"{context} has no matching FLOCK ADVISORY WRITE in /proc/locks"
            )

    try:
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CLOEXEC)
    except OSError as error:
        errors.append(f"{context} cannot open canonical lease: {error}")
        return
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            if error.errno not in {errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK}:
                errors.append(f"{context} lock contention returned {error}")
        except OSError as error:
            errors.append(f"{context} cannot test lock contention: {error}")
        else:
            errors.append(f"{context} canonical lease is not held exclusively")
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def validate_fixture_manifest(
    prefix: str,
    records: list[dict[str, Any]],
    errors: list[str],
) -> None:
    for ordinal, record in enumerate(records, start=1):
        context = f"fixture {prefix} record {ordinal}"
        require_protocol(record, context, errors)
        if record.get("fixture_marker") != FIXTURE_MARKER:
            errors.append(
                f"{context} fixture_marker {record.get('fixture_marker')!r} "
                f"!= {FIXTURE_MARKER!r}"
            )
        if record.get("kind") != prefix:
            errors.append(
                f"{context} kind {record.get('kind')!r} != {prefix!r}"
            )
        if record.get("status") != "PASS":
            errors.append(f"{context} status is not PASS")


def validate_lease_manifest(
    records: list[dict[str, Any]],
    values: dict[str, str],
    errors: list[str],
) -> None:
    if len(records) != 1:
        return
    record = records[0]
    context = "lease acquisition record"
    require_protocol(record, context, errors)
    if record.get("event") != "acquired":
        errors.append(f"{context} event is not acquired")
    lease_fields = (
        "lease_path",
        "lease_device",
        "lease_inode",
        "lease_holder_pid",
        "lease_holder_starttime",
        "lease_holder_uid",
        "lease_hostname",
        "lease_acquired_at",
        "lease_nonce",
        "lease_boot_id",
    )
    for field in lease_fields:
        claimed = values.get(field)
        observed = record.get(field)
        if claimed is not None and str(observed) != claimed:
            errors.append(
                f"{context} {field}={observed!r} != provenance {claimed!r}"
            )
    for field in (
        "lease_device",
        "lease_inode",
        "lease_holder_pid",
        "lease_holder_starttime",
        "lease_holder_uid",
    ):
        record_integer(record, field, context, errors, positive=field != "lease_holder_uid")
    for field in (
        "lease_path",
        "lease_hostname",
        "lease_nonce",
        "lease_boot_id",
    ):
        value = record.get(field)
        if not isinstance(value, str) or not value:
            errors.append(f"{context} {field} is not a nonempty string")
    parse_timestamp(record.get("lease_acquired_at"), f"{context} acquired_at", errors)

    canonical = Path.home() / ".cache/mess-bench/global-measurement.lock"
    lock_stat: os.stat_result | None = None
    try:
        lock_stat = canonical.stat()
    except OSError as error:
        errors.append(f"{context} cannot stat canonical lease: {error}")
    else:
        if record.get("lease_device") != lock_stat.st_dev:
            errors.append(f"{context} device does not match canonical lease")
        if record.get("lease_inode") != lock_stat.st_ino:
            errors.append(f"{context} inode does not match canonical lease")
    parent = os.getppid()
    if record.get("lease_holder_pid") != parent:
        errors.append(
            f"{context} holder PID {record.get('lease_holder_pid')!r} "
            f"!= evaluator parent {parent}"
        )
    try:
        parent_starttime = proc_starttime(parent)
        parent_uid = (Path("/proc") / str(parent)).stat().st_uid
    except (OSError, ValueError) as error:
        errors.append(f"{context} cannot observe evaluator parent: {error}")
    else:
        if record.get("lease_holder_starttime") != parent_starttime:
            errors.append(f"{context} holder starttime does not match parent")
        if record.get("lease_holder_uid") != parent_uid:
            errors.append(f"{context} holder UID does not match parent")
    try:
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError as error:
        errors.append(f"{context} cannot read boot ID: {error}")
    else:
        if record.get("lease_boot_id") != boot_id:
            errors.append(f"{context} boot ID does not match current host")
    if record.get("lease_hostname") != socket.gethostname():
        errors.append(f"{context} hostname does not match current host")
    if lock_stat is not None:
        validate_lease_held(
            canonical,
            lock_stat,
            record.get("lease_holder_pid")
            if isinstance(record.get("lease_holder_pid"), int)
            else None,
            context,
            errors,
        )


def expected_guard_labels() -> list[str]:
    labels = []
    for ordinal in range(1, len(physical_order()) + 1):
        labels.extend((f"row-{ordinal:03d}-pre", f"row-{ordinal:03d}-post"))
    labels.append("pre-evaluator")
    return labels


def validate_preidentity_vanished(
    snapshot: dict[str, Any],
    context: str,
    errors: list[str],
) -> None:
    observations = snapshot.get("preidentity_vanished", [])
    if not isinstance(observations, list):
        errors.append(f"{context} preidentity_vanished is not a list")
        return
    for ordinal, observation in enumerate(observations, start=1):
        observation_context = f"{context} preidentity observation {ordinal}"
        if not isinstance(observation, dict):
            errors.append(f"{observation_context} is not an object")
            continue
        record_integer(
            observation,
            "pid",
            observation_context,
            errors,
            positive=True,
        )
        for field in ("observed_at", "read_error"):
            value = observation.get(field)
            if not isinstance(value, str) or not value:
                errors.append(
                    f"{observation_context} {field} is not a nonempty string"
                )
        parse_timestamp(
            observation.get("observed_at"),
            f"{observation_context} observed_at",
            errors,
        )


def validate_guard_manifest(
    records: list[dict[str, Any]],
    values: dict[str, str],
    output_dir: Path,
    errors: list[str],
) -> None:
    labels = expected_guard_labels()
    allowed_classifications = {
        "runner",
        "current_child",
        "expected_helper",
    }
    for ordinal, record in enumerate(records, start=1):
        context = f"guard record {ordinal}"
        require_protocol(record, context, errors)
        observed_ordinal = record_integer(record, "ordinal", context, errors)
        if observed_ordinal != ordinal:
            errors.append(
                f"{context} ordinal={observed_ordinal} != physical {ordinal}"
            )
        expected_label = labels[ordinal - 1] if ordinal <= len(labels) else None
        if record.get("label") != expected_label:
            errors.append(
                f"{context} label={record.get('label')!r} != {expected_label!r}"
            )
        if record.get("publication") != "atomic":
            errors.append(f"{context} publication is not atomic")
        if record.get("verdict") != "pass":
            errors.append(f"{context} verdict is not pass")
        manifest_runner = record.get("runner")
        if not isinstance(manifest_runner, dict):
            errors.append(f"{context} runner is not an object")
        else:
            require_exact_keys(
                manifest_runner,
                {"pid", "comm", "state", "ppid", "starttime_ticks"},
                f"{context} runner",
                errors,
            )
            expected_pid = parse_nonnegative_integer(
                values.get("lease_holder_pid"),
                f"{context} lease_holder_pid",
                errors,
            )
            expected_start = parse_nonnegative_integer(
                values.get("lease_holder_starttime"),
                f"{context} lease_holder_starttime",
                errors,
            )
            if manifest_runner.get("pid") != expected_pid:
                errors.append(f"{context} runner PID does not match lease holder")
            if manifest_runner.get("starttime_ticks") != expected_start:
                errors.append(
                    f"{context} runner starttime does not match lease holder"
                )
            record_integer(
                manifest_runner, "ppid", f"{context} runner", errors
            )
            if not isinstance(manifest_runner.get("comm"), str) or not isinstance(
                manifest_runner.get("state"), str
            ):
                errors.append(f"{context} runner comm/state is not textual")
        if record.get("active_child") is not None:
            errors.append(f"{context} active_child is not null")
        if record.get("forbidden_comm") != sorted(FORBIDDEN_COMM):
            errors.append(f"{context} forbidden_comm does not match frozen set")
        started_at = parse_timestamp(
            record.get("started_at"), f"{context} started_at", errors
        )
        completed_at = parse_timestamp(
            record.get("completed_at"), f"{context} completed_at", errors
        )
        if started_at is not None and completed_at is not None and completed_at < started_at:
            errors.append(f"{context} completed before it started")
        started_ns = record_integer(
            record, "started_monotonic_ns", context, errors, positive=True
        )
        completed_ns = record_integer(
            record, "completed_monotonic_ns", context, errors, positive=True
        )
        if started_ns is not None and completed_ns is not None and completed_ns < started_ns:
            errors.append(f"{context} monotonic completion precedes start")
        matches = record.get("matches")
        if not isinstance(matches, list):
            errors.append(f"{context} matches is not a list")
            continue
        if matches:
            errors.append(f"{context} contains unexpected process matches")
        for match_ordinal, match in enumerate(matches, start=1):
            match_context = f"{context} match {match_ordinal}"
            if not isinstance(match, dict):
                errors.append(f"{match_context} is not an object")
                continue
            classification = match.get("classification")
            if classification not in allowed_classifications:
                errors.append(
                    f"{match_context} unresolved classification "
                    f"{classification!r}"
                )
            record_integer(match, "pid", match_context, errors, positive=True)
            record_integer(match, "ppid", match_context, errors)
            record_integer(
                match,
                "starttime_ticks",
                match_context,
                errors,
                positive=True,
            )
            record_integer(match, "uid", match_context, errors)
            for field in ("comm", "state", "cmdline"):
                if not isinstance(match.get(field), str):
                    errors.append(f"{match_context} {field} is not a string")
            read_errors = match.get("read_errors")
            if not isinstance(read_errors, list):
                errors.append(f"{match_context} read_errors is not a list")
            if not isinstance(match.get("exe"), str) and not read_errors:
                errors.append(f"{match_context} has neither exe nor read error")
        snapshot_path_value = record.get("path")
        if not isinstance(snapshot_path_value, str):
            errors.append(f"{context} snapshot path is not a string")
            continue
        snapshot_path = Path(snapshot_path_value).resolve()
        if not is_within(snapshot_path, output_dir):
            errors.append(f"{context} snapshot path is outside output directory")
        claimed_snapshot_sha = record_sha256(
            record,
            "sha256",
            context,
            errors,
        )
        try:
            snapshot_bytes = snapshot_path.read_bytes()
            observed_snapshot_sha = hashlib.sha256(snapshot_bytes).hexdigest()
            snapshot = json.loads(snapshot_bytes)
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            errors.append(f"{context} cannot read snapshot: {error}")
            continue
        if claimed_snapshot_sha is not None and claimed_snapshot_sha != observed_snapshot_sha:
            errors.append(f"{context} snapshot hash does not match")
        if not isinstance(snapshot, dict):
            errors.append(f"{context} snapshot is not an object")
            continue
        if snapshot.get("protocol") != PROTOCOL:
            errors.append(f"{context} snapshot protocol mismatch")
        if snapshot.get("label") != expected_label:
            errors.append(f"{context} snapshot label mismatch")
        if snapshot.get("verdict") != "pass":
            errors.append(f"{context} snapshot verdict is not pass")
        if snapshot.get("entries") != matches:
            errors.append(f"{context} snapshot entries do not match manifest")
        for field in (
            "runner",
            "active_child",
            "forbidden_comm",
            "started_at",
            "completed_at",
            "started_monotonic_ns",
            "completed_monotonic_ns",
        ):
            if snapshot.get(field) != record.get(field):
                errors.append(f"{context} snapshot {field} does not match manifest")
        observations = snapshot.get("preidentity_vanished")
        manifest_preidentity = record_integer(
            record, "preidentity_vanished", context, errors
        )
        if isinstance(observations, list) and manifest_preidentity != len(observations):
            errors.append(f"{context} preidentity count does not match snapshot")
        validate_preidentity_vanished(snapshot, context, errors)


def validate_reaping(
    reaping: Any,
    pid: int | None,
    starttime: int | None,
    context: str,
    errors: list[str],
) -> None:
    if not isinstance(reaping, dict):
        errors.append(f"{context} reaping is not an object")
        return
    reaping_pid = record_integer(reaping, "pid", f"{context} reaping", errors)
    reaping_start = record_integer(
        reaping,
        "starttime",
        f"{context} reaping",
        errors,
        positive=True,
    )
    if pid is not None and reaping_pid != pid:
        errors.append(f"{context} reaping pid={reaping_pid} != child {pid}")
    if starttime is not None and reaping_start != starttime:
        errors.append(
            f"{context} reaping starttime={reaping_start} != child {starttime}"
        )
    status = reaping.get("status")
    if status not in {"absent", "pid_reused"}:
        errors.append(f"{context} reaping status is unresolved: {status!r}")
    if status == "pid_reused":
        observed = record_integer(
            reaping,
            "observed_starttime",
            f"{context} reaping",
            errors,
            positive=True,
        )
        if starttime is not None and observed == starttime:
            errors.append(f"{context} reused PID retained original starttime")


def validate_child_manifest(
    records: list[dict[str, Any]],
    values: dict[str, str],
    output_dir: Path,
    errors: list[str],
) -> None:
    order = physical_order()
    previous_bytes = 0
    for ordinal, record in enumerate(records, start=1):
        context = f"child record {ordinal}"
        expected_variant: str | None = None
        expected_binary_path: Path | None = None
        require_protocol(record, context, errors)
        if record.get("kind") != "benchmark":
            errors.append(f"{context} kind is not benchmark")
        observed_ordinal = record_integer(record, "ordinal", context, errors)
        if observed_ordinal != ordinal:
            errors.append(f"{context} ordinal={observed_ordinal} != {ordinal}")
        if ordinal <= len(order):
            mode, batch, cycle, slot, variant = order[ordinal - 1]
            expected_variant = variant
            binary_path_value = values.get(f"{variant}_binary_path")
            if binary_path_value is not None:
                expected_binary_path = Path(binary_path_value).resolve()
            expected = {
                "mode": mode,
                "batch": batch,
                "cycle": cycle,
                "slot": slot,
                "variant": variant,
            }
            for field, wanted in expected.items():
                if record.get(field) != wanted:
                    errors.append(
                        f"{context} {field}={record.get(field)!r} != {wanted!r}"
                    )
            claimed_binary = values.get(f"{variant}_binary_sha256")
            if claimed_binary is not None and record.get("binary_sha256") != claimed_binary:
                errors.append(
                    f"{context} binary_sha256={record.get('binary_sha256')!r} "
                    f"!= {variant} provenance"
                )
            if expected_binary_path is not None and record.get("argv") != [
                str(expected_binary_path)
            ]:
                errors.append(
                    f"{context} argv={record.get('argv')!r} != exact binary"
                )
        pid = record_integer(record, "pid", context, errors, positive=True)
        starttime = record_integer(
            record,
            "starttime",
            context,
            errors,
            positive=True,
        )
        waited_pid = record_integer(
            record,
            "waited_pid",
            context,
            errors,
            positive=True,
        )
        if pid is not None and waited_pid != pid:
            errors.append(f"{context} waited_pid={waited_pid} != child {pid}")
        identity = record.get("identity")
        if not isinstance(identity, dict):
            errors.append(f"{context} identity is not an object")
        else:
            if identity.get("pid") != pid:
                errors.append(f"{context} identity pid does not match")
            if identity.get("starttime_ticks") != starttime:
                errors.append(f"{context} identity starttime does not match")
        if record.get("exit_status") != 0:
            errors.append(f"{context} exit_status is not zero")
        if record.get("timed_out") is not False:
            errors.append(f"{context} timed_out is not false")
        validate_reaping(record.get("reaping"), pid, starttime, context, errors)
        started_at = parse_timestamp(
            record.get("started_at"), f"{context} started_at", errors
        )
        completed_at = parse_timestamp(
            record.get("completed_at"), f"{context} completed_at", errors
        )
        if started_at is not None and completed_at is not None and completed_at < started_at:
            errors.append(f"{context} completed before it started")
        started_ns = record_integer(
            record, "started_monotonic_ns", context, errors, positive=True
        )
        completed_ns = record_integer(
            record, "completed_monotonic_ns", context, errors, positive=True
        )
        if started_ns is not None and completed_ns is not None and completed_ns < started_ns:
            errors.append(f"{context} monotonic completion precedes start")

        rows_before = record_integer(record, "csv_rows_before", context, errors)
        rows_after = record_integer(record, "csv_rows_after", context, errors)
        row_delta = record_integer(record, "csv_row_delta", context, errors)
        bytes_before = record_integer(record, "csv_bytes_before", context, errors)
        bytes_after = record_integer(record, "csv_bytes_after", context, errors)
        byte_delta = record_integer(
            record,
            "csv_byte_delta",
            context,
            errors,
            positive=True,
        )
        if rows_before != ordinal - 1 or rows_after != ordinal or row_delta != 1:
            errors.append(
                f"{context} CSV rows {rows_before}->{rows_after} "
                f"delta={row_delta}, expected {ordinal - 1}->{ordinal} delta=1"
            )
        if bytes_before is not None and bytes_before != previous_bytes:
            errors.append(
                f"{context} csv_bytes_before={bytes_before} != prior {previous_bytes}"
            )
        if (
            bytes_before is not None
            and bytes_after is not None
            and byte_delta is not None
            and bytes_after - bytes_before != byte_delta
        ):
            errors.append(f"{context} CSV byte delta is inconsistent")
        if bytes_after is not None:
            previous_bytes = bytes_after

        record_sha256(record, "csv_prefix_sha256_before", context, errors)
        record_sha256(record, "csv_prefix_sha256_after", context, errors)

        for label, shape, wanted_rows, wanted_bytes in (
            ("csv_before", record.get("csv_before"), rows_before, bytes_before),
            ("csv_after", record.get("csv_after"), rows_after, bytes_after),
        ):
            if not isinstance(shape, dict):
                errors.append(f"{context} {label} is not an object")
                continue
            if shape.get("complete") is not True:
                errors.append(f"{context} {label} is not complete")
            if shape.get("data_rows") != wanted_rows:
                errors.append(f"{context} {label} row count disagrees")
            if shape.get("bytes") != wanted_bytes:
                errors.append(f"{context} {label} byte count disagrees")
            expected_columns = 0 if ordinal == 1 and label == "csv_before" else len(CSV_FIELDS)
            if shape.get("columns") != expected_columns:
                errors.append(
                    f"{context} {label} columns={shape.get('columns')!r} "
                    f"!= {expected_columns}"
                )

        output_path_value = record.get("output_path")
        if not isinstance(output_path_value, str):
            errors.append(f"{context} output_path is not a string")
        else:
            output_path = Path(output_path_value).resolve()
            if not is_within(output_path, output_dir):
                errors.append(f"{context} output_path is outside output directory")
            if expected_variant is not None:
                expected_output = (
                    output_dir / "rows" / f"{ordinal:03d}-{expected_variant}.log"
                ).resolve()
                if output_path != expected_output:
                    errors.append(
                        f"{context} output_path={output_path} != {expected_output}"
                    )
            claimed_output_sha = record_sha256(
                record,
                "output_sha256",
                context,
                errors,
            )
            try:
                observed_output_sha = sha256(output_path)
            except OSError as error:
                errors.append(f"{context} cannot hash output: {error}")
            else:
                if claimed_output_sha is not None and claimed_output_sha != observed_output_sha:
                    errors.append(f"{context} output hash does not match")


def validate_smoke_child(
    child: Any,
    variant: str,
    values: dict[str, str],
    output_dir: Path,
    errors: list[str],
) -> str | None:
    context = f"smoke {variant} child"
    if not isinstance(child, dict):
        errors.append(f"{context} is not an object")
        return None
    if child.get("contract_mode") is not True:
        errors.append(f"{context} contract_mode is not true")
    binary_path_value = values.get(f"{variant}_binary_path")
    expected_binary = (
        Path(binary_path_value).resolve() if binary_path_value is not None else None
    )
    if expected_binary is not None and child.get("argv") != [str(expected_binary)]:
        errors.append(f"{context} argv is not the exact frozen binary")
    claimed = values.get(f"{variant}_binary_sha256")
    observed_hash = record_sha256(child, "binary_sha256", context, errors)
    if claimed is not None and observed_hash != claimed:
        errors.append(f"{context} binary hash does not match provenance")
    pid = record_integer(child, "pid", context, errors, positive=True)
    starttime = record_integer(child, "starttime", context, errors, positive=True)
    waited_pid = record_integer(child, "waited_pid", context, errors, positive=True)
    if pid is not None and waited_pid != pid:
        errors.append(f"{context} waited_pid={waited_pid} != child {pid}")
    if child.get("exit_status") != 0:
        errors.append(f"{context} exit_status is not zero")
    if child.get("timed_out") is not False:
        errors.append(f"{context} timed_out is not false")
    if child.get("process_group_absent") is not True:
        errors.append(f"{context} process_group_absent is not true")
    validate_reaping(child.get("reaping"), pid, starttime, context, errors)
    started_at = parse_timestamp(
        child.get("started_at"), f"{context} started_at", errors
    )
    completed_at = parse_timestamp(
        child.get("completed_at"), f"{context} completed_at", errors
    )
    if started_at is not None and completed_at is not None and completed_at < started_at:
        errors.append(f"{context} completed before it started")
    started_ns = record_integer(
        child, "started_monotonic_ns", context, errors, positive=True
    )
    completed_ns = record_integer(
        child, "completed_monotonic_ns", context, errors, positive=True
    )
    if started_ns is not None and completed_ns is not None and completed_ns < started_ns:
        errors.append(f"{context} monotonic completion precedes start")
    claimed_output_sha = record_sha256(child, "output_sha256", context, errors)
    output_path_value = child.get("output_path")
    if not isinstance(output_path_value, str):
        errors.append(f"{context} output_path is not a string")
    else:
        output_path = Path(output_path_value).resolve()
        expected_output = (output_dir / "smoke" / f"{variant}.log").resolve()
        if output_path != expected_output:
            errors.append(f"{context} output_path={output_path} != {expected_output}")
        try:
            output_bytes = output_path.read_bytes()
            observed_output_sha = hashlib.sha256(output_bytes).hexdigest()
            contract = json.loads(output_bytes)
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            errors.append(f"{context} cannot validate output: {error}")
        else:
            if claimed_output_sha is not None and claimed_output_sha != observed_output_sha:
                errors.append(f"{context} output hash does not match")
            expected_contract = {
                "contract_mode": True,
                "csv_written": False,
                "protocol": PROTOCOL,
                "baseline_source": values.get("baseline_source"),
                "baseline_tree": values.get("baseline_tree"),
                "source_commit": values.get(f"{variant}_source"),
                "source_tree": values.get(f"{variant}_tree"),
                "harness_sha256": values.get(f"{variant}_harness_sha256"),
                "cargo_lock_sha256": values.get(
                    f"{variant}_cargo_lock_sha256"
                ),
                "source_approval_sha256": values.get("source_approval_sha256"),
                "build_nonce": values.get(f"{variant}_build_nonce"),
            }
            if contract != expected_contract:
                errors.append(f"{context} contract output is not exact")
            if child.get("contract") != expected_contract:
                errors.append(f"{context} manifest contract is not exact")
    return observed_hash


def validate_smoke_manifest(
    records: list[dict[str, Any]],
    values: dict[str, str],
    output_dir: Path,
    errors: list[str],
) -> None:
    if len(records) != 1:
        return
    record = records[0]
    context = "smoke record"
    require_protocol(record, context, errors)
    if record.get("status") != "PASS":
        errors.append(f"{context} status is not PASS")
    guards = record.get("guards")
    expected_guard_labels = (
        "control-pre",
        "control-post",
        "candidate-pre",
        "candidate-post",
        "evaluator-pre",
    )
    if not isinstance(guards, list):
        errors.append(f"{context} guards is not a list")
        guards = []
    if len(guards) != len(expected_guard_labels):
        errors.append(
            f"{context} guard count {len(guards)} != {len(expected_guard_labels)}"
        )
    for ordinal, guard in enumerate(guards, start=1):
        guard_context = f"{context} guard {ordinal}"
        if not isinstance(guard, dict):
            errors.append(f"{guard_context} is not an object")
            continue
        expected_label = (
            expected_guard_labels[ordinal - 1]
            if ordinal <= len(expected_guard_labels)
            else None
        )
        if guard.get("label") != expected_label:
            errors.append(f"{guard_context} label mismatch")
        if guard.get("publication") != "atomic":
            errors.append(f"{guard_context} publication is not atomic")
        if guard.get("verdict") != "pass":
            errors.append(f"{guard_context} verdict is not pass")
        guard_started = parse_timestamp(
            guard.get("started_at"), f"{guard_context} started_at", errors
        )
        guard_completed = parse_timestamp(
            guard.get("completed_at"), f"{guard_context} completed_at", errors
        )
        if (
            guard_started is not None
            and guard_completed is not None
            and guard_completed < guard_started
        ):
            errors.append(f"{guard_context} completed before it started")
        guard_started_ns = record_integer(
            guard, "started_monotonic_ns", guard_context, errors, positive=True
        )
        guard_completed_ns = record_integer(
            guard, "completed_monotonic_ns", guard_context, errors, positive=True
        )
        if (
            guard_started_ns is not None
            and guard_completed_ns is not None
            and guard_completed_ns < guard_started_ns
        ):
            errors.append(f"{guard_context} monotonic completion precedes start")
        path_value = guard.get("path")
        claimed_sha = record_sha256(guard, "sha256", guard_context, errors)
        if not isinstance(path_value, str):
            errors.append(f"{guard_context} path is not a string")
            continue
        path = Path(path_value).resolve()
        if not is_within(path, output_dir / "smoke"):
            errors.append(f"{guard_context} path is outside smoke output")
        try:
            snapshot_bytes = path.read_bytes()
            observed_sha = hashlib.sha256(snapshot_bytes).hexdigest()
            snapshot = json.loads(snapshot_bytes)
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            errors.append(f"{guard_context} cannot validate snapshot: {error}")
            continue
        if claimed_sha is not None and claimed_sha != observed_sha:
            errors.append(f"{guard_context} snapshot hash mismatch")
        if not isinstance(snapshot, dict):
            errors.append(f"{guard_context} snapshot is not an object")
            continue
        if snapshot.get("protocol") != PROTOCOL:
            errors.append(f"{guard_context} snapshot protocol mismatch")
        if snapshot.get("label") != expected_label:
            errors.append(f"{guard_context} snapshot label mismatch")
        if snapshot.get("verdict") != "pass":
            errors.append(f"{guard_context} snapshot verdict is not pass")
        if snapshot.get("entries") != []:
            errors.append(f"{guard_context} snapshot entries are not empty")
        if snapshot.get("active_child") is not None:
            errors.append(f"{guard_context} snapshot active_child is not null")
        if snapshot.get("forbidden_comm") != sorted(FORBIDDEN_COMM):
            errors.append(f"{guard_context} forbidden_comm does not match")
        runner = snapshot.get("runner")
        if not isinstance(runner, dict):
            errors.append(f"{guard_context} runner is not an object")
        else:
            if str(runner.get("pid")) != values.get("lease_holder_pid"):
                errors.append(f"{guard_context} runner PID differs from lease")
            if str(runner.get("starttime_ticks")) != values.get(
                "lease_holder_starttime"
            ):
                errors.append(f"{guard_context} runner starttime differs from lease")
        for field in (
            "started_at",
            "completed_at",
            "started_monotonic_ns",
            "completed_monotonic_ns",
        ):
            if snapshot.get(field) != guard.get(field):
                errors.append(f"{guard_context} snapshot {field} differs")
        observations = snapshot.get("preidentity_vanished")
        if isinstance(observations, list) and guard.get(
            "preidentity_vanished"
        ) != len(observations):
            errors.append(f"{guard_context} preidentity count differs")
        validate_preidentity_vanished(snapshot, guard_context, errors)
    children = record.get("children")
    if not isinstance(children, dict):
        errors.append(f"{context} children is not an object")
        children = {}
    control_hash = validate_smoke_child(
        children.get("control"),
        "control",
        values,
        output_dir,
        errors,
    )
    candidate_hash = validate_smoke_child(
        children.get("candidate"),
        "candidate",
        values,
        output_dir,
        errors,
    )
    if control_hash is not None and control_hash == candidate_hash:
        errors.append("smoke control and candidate binary hashes are identical")
    evaluator = record.get("evaluator")
    if not isinstance(evaluator, dict):
        errors.append(f"{context} evaluator is not an object")
        return
    if evaluator.get("exit_status") != 0:
        errors.append(f"{context} evaluator exit_status is not zero")
    if evaluator.get("outcome") != "FIXTURE_PASS":
        errors.append(f"{context} evaluator outcome is not FIXTURE_PASS")
    evaluator_context = f"{context} evaluator"
    expected_evaluator = Path(values["evaluator_path"]).resolve()
    expected_fixture_root = output_dir / "smoke" / "evaluator-fixture"
    expected_argv = [
        str(expected_evaluator),
        "--fixture",
        str((expected_fixture_root / "paired.csv").resolve()),
        str((expected_fixture_root / "provenance.txt").resolve()),
        str((expected_fixture_root / "result.json").resolve()),
    ]
    if evaluator.get("argv") != expected_argv:
        errors.append(f"{evaluator_context} argv is not exact")
    pid = record_integer(evaluator, "pid", evaluator_context, errors, positive=True)
    starttime = record_integer(
        evaluator,
        "starttime",
        evaluator_context,
        errors,
        positive=True,
    )
    waited_pid = record_integer(
        evaluator,
        "waited_pid",
        evaluator_context,
        errors,
        positive=True,
    )
    if pid is not None and waited_pid != pid:
        errors.append(f"{evaluator_context} waited_pid does not match child")
    if evaluator.get("timed_out") is not False:
        errors.append(f"{evaluator_context} timed_out is not false")
    if evaluator.get("process_group_absent") is not True:
        errors.append(f"{evaluator_context} process_group_absent is not true")
    validate_reaping(
        evaluator.get("reaping"),
        pid,
        starttime,
        evaluator_context,
        errors,
    )
    evaluator_started = parse_timestamp(
        evaluator.get("started_at"), f"{evaluator_context} started_at", errors
    )
    evaluator_completed = parse_timestamp(
        evaluator.get("completed_at"), f"{evaluator_context} completed_at", errors
    )
    if (
        evaluator_started is not None
        and evaluator_completed is not None
        and evaluator_completed < evaluator_started
    ):
        errors.append(f"{evaluator_context} completed before it started")
    evaluator_started_ns = record_integer(
        evaluator,
        "started_monotonic_ns",
        evaluator_context,
        errors,
        positive=True,
    )
    evaluator_completed_ns = record_integer(
        evaluator,
        "completed_monotonic_ns",
        evaluator_context,
        errors,
        positive=True,
    )
    if (
        evaluator_started_ns is not None
        and evaluator_completed_ns is not None
        and evaluator_completed_ns < evaluator_started_ns
    ):
        errors.append(f"{evaluator_context} monotonic completion precedes start")
    claimed_output_sha = record_sha256(
        evaluator,
        "output_sha256",
        evaluator_context,
        errors,
    )
    output_path_value = evaluator.get("output_path")
    if not isinstance(output_path_value, str):
        errors.append(f"{evaluator_context} output_path is not a string")
    else:
        output_path = Path(output_path_value).resolve()
        expected_output = (output_dir / "smoke" / "evaluator.log").resolve()
        if output_path != expected_output:
            errors.append(f"{evaluator_context} output path mismatch")
        try:
            observed_output_sha = sha256(output_path)
        except OSError as error:
            errors.append(f"{evaluator_context} cannot hash output: {error}")
        else:
            if claimed_output_sha is not None and claimed_output_sha != observed_output_sha:
                errors.append(f"{evaluator_context} output hash mismatch")
    claimed_result_sha = record_sha256(
        evaluator,
        "result_sha256",
        evaluator_context,
        errors,
    )
    result_path_value = evaluator.get("result_path")
    if not isinstance(result_path_value, str):
        errors.append(f"{evaluator_context} result_path is not a string")
    else:
        result_path = Path(result_path_value).resolve()
        expected_result = (
            output_dir / "smoke" / "evaluator-fixture" / "result.json"
        ).resolve()
        if result_path != expected_result:
            errors.append(f"{evaluator_context} result path mismatch")
        try:
            result_bytes = result_path.read_bytes()
            observed_result_sha = hashlib.sha256(result_bytes).hexdigest()
            fixture_result = json.loads(result_bytes)
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            errors.append(f"{evaluator_context} cannot validate result: {error}")
        else:
            if claimed_result_sha is not None and claimed_result_sha != observed_result_sha:
                errors.append(f"{evaluator_context} result hash mismatch")
            if not isinstance(fixture_result, dict):
                errors.append(f"{evaluator_context} result is not an object")
            elif (
                fixture_result.get("protocol") != PROTOCOL
                or fixture_result.get("evidence_mode") != "fixture"
                or fixture_result.get("outcome") != "FIXTURE_PASS"
                or fixture_result.get("exit_code") != 0
            ):
                errors.append(f"{evaluator_context} fixture result is not exact")

    if len(guards) == 5:
        for variant, pre_index, post_index in (
            ("control", 0, 1),
            ("candidate", 2, 3),
        ):
            child = children.get(variant)
            if not isinstance(child, dict):
                continue
            points = (
                guards[pre_index].get("completed_monotonic_ns"),
                child.get("started_monotonic_ns"),
                child.get("completed_monotonic_ns"),
                guards[post_index].get("started_monotonic_ns"),
            )
            if all(isinstance(point, int) and not isinstance(point, bool) for point in points):
                if not points[0] <= points[1] <= points[2] <= points[3]:
                    errors.append(
                        f"smoke {variant} is not ordered pre-guard -> child -> post-guard"
                    )
        for prior, following in ((1, 2), (3, 4)):
            prior_end = guards[prior].get("completed_monotonic_ns")
            following_start = guards[following].get("started_monotonic_ns")
            if (
                isinstance(prior_end, int)
                and not isinstance(prior_end, bool)
                and isinstance(following_start, int)
                and not isinstance(following_start, bool)
                and following_start < prior_end
            ):
                errors.append("smoke guard sequence overlaps or reorders")
        evaluator_guard_end = guards[4].get("completed_monotonic_ns")
        evaluator_start = evaluator.get("started_monotonic_ns")
        if (
            isinstance(evaluator_guard_end, int)
            and not isinstance(evaluator_guard_end, bool)
            and isinstance(evaluator_start, int)
            and not isinstance(evaluator_start, bool)
            and evaluator_start < evaluator_guard_end
        ):
            errors.append("smoke evaluator starts before its pre-guard completes")


def validate_manifest_semantics(
    prefix: str,
    records: list[dict[str, Any]],
    values: dict[str, str],
    evidence_mode: str,
    output_dir: Path,
    errors: list[str],
) -> None:
    if evidence_mode == "fixture":
        validate_fixture_manifest(prefix, records, errors)
        return
    if prefix == "lease_event":
        validate_lease_manifest(records, values, errors)
    elif prefix == "guard_manifest":
        validate_guard_manifest(records, values, output_dir, errors)
    elif prefix == "child_manifest":
        validate_child_manifest(records, values, output_dir, errors)
    elif prefix == "smoke_manifest":
        validate_smoke_manifest(records, values, output_dir, errors)


def validate_admission_manifest_links(
    manifests: dict[str, list[dict[str, Any]]],
    values: dict[str, str],
    csv_bytes: bytes,
    errors: list[str],
) -> None:
    guards = manifests.get("guard_manifest", [])
    children = manifests.get("child_manifest", [])
    if len(guards) != MANIFESTS["guard_manifest"] or len(children) != len(
        physical_order()
    ):
        return

    claimed_bytes = parse_nonnegative_integer(
        values.get("paired_csv_bytes"), "paired_csv_bytes", errors
    )
    if claimed_bytes is not None and claimed_bytes != len(csv_bytes):
        errors.append(
            f"paired_csv_bytes={claimed_bytes} != current {len(csv_bytes)}"
        )

    prior_post_end: int | None = None
    for ordinal, child in enumerate(children, start=1):
        context = f"child record {ordinal}"
        pre = guards[(ordinal - 1) * 2]
        post = guards[(ordinal - 1) * 2 + 1]
        pre_start = pre.get("started_monotonic_ns")
        pre_end = pre.get("completed_monotonic_ns")
        child_start = child.get("started_monotonic_ns")
        child_end = child.get("completed_monotonic_ns")
        post_start = post.get("started_monotonic_ns")
        post_end = post.get("completed_monotonic_ns")
        if all(
            isinstance(value, int) and not isinstance(value, bool)
            for value in (pre_start, pre_end, child_start, child_end, post_start, post_end)
        ):
            if not pre_start <= pre_end <= child_start <= child_end <= post_start <= post_end:
                errors.append(
                    f"{context} is not ordered pre-guard -> child -> post-guard"
                )
            if prior_post_end is not None and pre_start < prior_post_end:
                errors.append(f"{context} pre-guard overlaps prior post-guard")
            prior_post_end = post_end

        before = child.get("csv_bytes_before")
        after = child.get("csv_bytes_after")
        if (
            isinstance(before, int)
            and not isinstance(before, bool)
            and isinstance(after, int)
            and not isinstance(after, bool)
            and 0 <= before <= after <= len(csv_bytes)
        ):
            expected_before = hashlib.sha256(csv_bytes[:before]).hexdigest()
            expected_after = hashlib.sha256(csv_bytes[:after]).hexdigest()
            if child.get("csv_prefix_sha256_before") != expected_before:
                errors.append(f"{context} before-prefix hash does not match final CSV")
            if child.get("csv_prefix_sha256_after") != expected_after:
                errors.append(f"{context} after-prefix hash does not match final CSV")
        else:
            errors.append(f"{context} CSV byte bounds exceed final CSV")

    final_after = children[-1].get("csv_bytes_after")
    if final_after != len(csv_bytes):
        errors.append(
            f"final child csv_bytes_after={final_after!r} != final CSV {len(csv_bytes)}"
        )
    pre_evaluator = guards[-1]
    final_post_end = guards[-2].get("completed_monotonic_ns")
    evaluator_guard_start = pre_evaluator.get("started_monotonic_ns")
    if (
        isinstance(final_post_end, int)
        and not isinstance(final_post_end, bool)
        and isinstance(evaluator_guard_start, int)
        and not isinstance(evaluator_guard_start, bool)
        and evaluator_guard_start < final_post_end
    ):
        errors.append("pre-evaluator guard precedes final post-child guard")


def validate_bound_manifests(
    values: dict[str, str],
    output_dir: Path,
    evidence_mode: str,
    csv_bytes: bytes,
    errors: list[str],
) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
    all_records: dict[str, list[dict[str, Any]]] = {}
    observed_paths: set[Path] = set()
    for prefix, admission_count in MANIFESTS.items():
        path_value = values.get(f"{prefix}_path")
        digest = values.get(f"{prefix}_sha256")
        count_value = values.get(f"{prefix}_records")
        if path_value is None or digest is None or count_value is None:
            continue
        path = Path(path_value).resolve()
        if not is_within(path, output_dir):
            errors.append(f"{prefix}_path {path} is outside {output_dir}")
        if path in observed_paths:
            errors.append(f"duplicate manifest path {path}")
        observed_paths.add(path)
        if not SHA256.fullmatch(digest):
            errors.append(f"{prefix}_sha256 is malformed: {digest!r}")
        count = parse_nonnegative_integer(
            count_value,
            f"{prefix}_records",
            errors,
        )
        if evidence_mode == "admission" and count != admission_count:
            errors.append(
                f"{prefix}_records={count} != admission {admission_count}"
            )
        if evidence_mode == "fixture" and count is not None and count < 1:
            errors.append(f"{prefix}_records must be positive in fixture mode")

        records = read_jsonl(path, prefix, errors)
        all_records[prefix] = records
        if count is not None and len(records) != count:
            errors.append(
                f"{prefix} JSON records {len(records)} != provenance {count}"
            )
        validate_manifest_semantics(
            prefix,
            records,
            values,
            evidence_mode,
            output_dir,
            errors,
        )
        try:
            observed_digest = sha256(path)
        except OSError as error:
            errors.append(f"cannot hash {prefix} {path}: {error}")
            observed_digest = None
        if observed_digest is not None and digest != observed_digest:
            errors.append(
                f"{prefix}_sha256={digest} != current {observed_digest}"
            )
        summaries[prefix] = {
            "path": str(path),
            "sha256": observed_digest,
            "records": len(records),
        }
    if evidence_mode == "admission":
        validate_admission_manifest_links(all_records, values, csv_bytes, errors)
    return summaries


def git_value(root: Path, args: list[str], errors: list[str]) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), *args],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        errors.append(f"git {' '.join(args)} failed for {root}: {error}")
        return ""
    return result.stdout.strip()


def git_bytes(root: Path, args: list[str], label: str, errors: list[str]) -> bytes:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), *args],
            check=True,
            capture_output=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as error:
        errors.append(f"{label} failed for {root}: {error}")
        return b""
    return result.stdout


def canonical_diff_manifest(
    root: Path,
    baseline: str,
    source: str,
    label: str,
    errors: list[str],
) -> list[dict[str, str]]:
    payload = git_bytes(
        root,
        ["diff", "--name-status", "--no-renames", "-z", f"{baseline}..{source}"],
        f"{label} name-status diff",
        errors,
    )
    fields = payload.split(b"\0")
    if fields and fields[-1] == b"":
        fields.pop()
    if len(fields) % 2:
        errors.append(f"{label} name-status diff has an odd field count")
        return []
    entries = []
    for offset in range(0, len(fields), 2):
        try:
            status = fields[offset].decode("ascii")
            path = fields[offset + 1].decode("utf-8")
        except UnicodeError as error:
            errors.append(f"{label} name-status diff is not decodable: {error}")
            continue
        if status not in {"A", "M", "D"}:
            errors.append(f"{label} unsupported diff status {status!r}")
        if not path or path.startswith("/") or ".." in Path(path).parts:
            errors.append(f"{label} invalid changed path {path!r}")
        entries.append({"status": status, "path": path})
    ordered = sorted(entries, key=lambda entry: (entry["path"], entry["status"]))
    if entries != ordered:
        errors.append(f"{label} git name-status output is not canonically sorted")
    return ordered


def canonical_patch_sha256(
    root: Path,
    baseline: str,
    source: str,
    changed_paths: list[str],
    label: str,
    errors: list[str],
) -> str:
    payload = git_bytes(
        root,
        [
            "diff",
            "--binary",
            "--full-index",
            "--no-ext-diff",
            "--no-renames",
            f"{baseline}..{source}",
            "--",
            *changed_paths,
        ],
        f"{label} canonical patch",
        errors,
    )
    return hashlib.sha256(payload).hexdigest()


def require_exact_keys(
    value: dict[str, Any],
    expected: set[str],
    label: str,
    errors: list[str],
) -> None:
    observed = set(value)
    if observed != expected:
        errors.append(
            f"{label} keys differ: missing={sorted(expected - observed)}, "
            f"extra={sorted(observed - expected)}"
        )


def validate_bound_file(
    path_value: Any,
    digest_value: Any,
    label: str,
    errors: list[str],
    *,
    within: Path | None = None,
) -> Path | None:
    if not isinstance(path_value, str) or not path_value:
        errors.append(f"{label} path is not a nonempty string")
        return None
    path = Path(path_value).resolve()
    if within is not None and not is_within(path, within):
        errors.append(f"{label} path {path} is outside {within}")
    if not isinstance(digest_value, str) or not SHA256.fullmatch(digest_value):
        errors.append(f"{label} digest is not SHA-256: {digest_value!r}")
    try:
        observed = sha256(path)
    except OSError as error:
        errors.append(f"cannot hash {label} {path}: {error}")
    else:
        if observed != digest_value:
            errors.append(f"{label} digest does not match {path}")
    return path


def validate_completed_child(
    child: Any,
    label: str,
    errors: list[str],
    *,
    expected_argv: list[str] | None = None,
    expected_output: Path | None = None,
    expected_cwd: Path | None = None,
) -> None:
    if not isinstance(child, dict):
        errors.append(f"{label} is not an object")
        return
    require_exact_keys(
        child,
        {
            "pid",
            "starttime",
            "waited_pid",
            "argv",
            "cwd",
            "started_at",
            "completed_at",
            "started_monotonic_ns",
            "completed_monotonic_ns",
            "exit_status",
            "timed_out",
            "terminated_by_runner",
            "reaping",
            "process_group_absent",
            "output_path",
            "output_sha256",
        },
        label,
        errors,
    )
    pid = record_integer(child, "pid", label, errors, positive=True)
    starttime = record_integer(child, "starttime", label, errors, positive=True)
    waited_pid = record_integer(child, "waited_pid", label, errors, positive=True)
    if pid is not None and waited_pid != pid:
        errors.append(f"{label} waited_pid does not match pid")
    if child.get("exit_status") != 0:
        errors.append(f"{label} exit_status is not zero")
    if child.get("timed_out") is not False:
        errors.append(f"{label} timed_out is not false")
    if child.get("terminated_by_runner") is not False:
        errors.append(f"{label} terminated_by_runner is not false")
    if child.get("process_group_absent") is not True:
        errors.append(f"{label} process_group_absent is not true")
    validate_reaping(child.get("reaping"), pid, starttime, label, errors)
    started_at = parse_timestamp(child.get("started_at"), f"{label} started_at", errors)
    completed_at = parse_timestamp(
        child.get("completed_at"), f"{label} completed_at", errors
    )
    if started_at is not None and completed_at is not None and completed_at < started_at:
        errors.append(f"{label} completed before it started")
    started_ns = record_integer(
        child, "started_monotonic_ns", label, errors, positive=True
    )
    completed_ns = record_integer(
        child, "completed_monotonic_ns", label, errors, positive=True
    )
    if started_ns is not None and completed_ns is not None and completed_ns < started_ns:
        errors.append(f"{label} monotonic completion precedes start")
    if expected_argv is not None and child.get("argv") != expected_argv:
        errors.append(f"{label} argv is not exact")
    cwd_value = child.get("cwd")
    if expected_cwd is not None and (
        not isinstance(cwd_value, str) or Path(cwd_value).resolve() != expected_cwd
    ):
        errors.append(f"{label} cwd is not exact")
    output_value = child.get("output_path")
    if expected_output is not None and (
        not isinstance(output_value, str)
        or Path(output_value).resolve() != expected_output
    ):
        errors.append(f"{label} output_path is not exact")
    validate_bound_file(output_value, child.get("output_sha256"), label, errors)


STABLE_LSCPU_FIELDS = {
    "Architecture:",
    "CPU op-mode(s):",
    "Address sizes:",
    "Byte Order:",
    "CPU(s):",
    "On-line CPU(s) list:",
    "Vendor ID:",
    "Model name:",
    "CPU family:",
    "Model:",
    "Thread(s) per core:",
    "Core(s) per socket:",
    "Socket(s):",
    "Stepping:",
    "L1d cache:",
    "L1i cache:",
    "L2 cache:",
    "L3 cache:",
    "NUMA node(s):",
}
NUMA_CPU_FIELD = re.compile(r"NUMA node[0-9]+ CPU\(s\):")


def stable_cpu_topology_sha256() -> str:
    environment = os.environ.copy()
    environment["LC_ALL"] = "C"

    def lscpu_json(arguments: list[str]) -> Any:
        result = subprocess.run(
            ["lscpu", *arguments],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
            env=environment,
        )
        return json.loads(result.stdout)

    summary_document = lscpu_json(["-J"])
    cpu_document = lscpu_json(["-J", "-e=CPU,NODE,SOCKET,CORE,ONLINE"])
    summary_rows = summary_document.get("lscpu")
    cpu_rows = cpu_document.get("cpus")
    if not isinstance(summary_rows, list) or not isinstance(cpu_rows, list):
        raise ValueError("lscpu JSON does not contain expected arrays")
    summary = []
    for row in summary_rows:
        if not isinstance(row, dict):
            raise ValueError("lscpu summary row is not an object")
        field = row.get("field")
        if field in STABLE_LSCPU_FIELDS or (
            isinstance(field, str) and NUMA_CPU_FIELD.fullmatch(field)
        ):
            summary.append({"field": field, "data": row.get("data")})
    summary.sort(key=lambda row: str(row["field"]))
    cpus = []
    for row in cpu_rows:
        if not isinstance(row, dict):
            raise ValueError("lscpu CPU row is not an object")
        if set(row) != {"cpu", "node", "socket", "core", "online"}:
            raise ValueError(f"lscpu CPU row has unexpected fields: {row}")
        cpus.append(dict(row))
    cpus.sort(
        key=lambda row: (
            int(row["cpu"]),
            int(row["node"]),
            int(row["socket"]),
            int(row["core"]),
            bool(row["online"]),
        )
    )
    payload = json.dumps(
        {"summary": summary, "cpus": cpus},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def current_governors() -> str:
    governors = []
    for path in sorted(
        Path("/sys/devices/system/cpu").glob("cpu*/cpufreq/scaling_governor")
    ):
        try:
            governors.append(path.read_text().strip())
        except OSError:
            governors.append("unreadable")
    return ",".join(governors)


def observe_admission_environment(bench_dir: Path) -> dict[str, str]:
    """Observe every live host field used by admission validation."""

    filesystem = os.statvfs(bench_dir)
    return {
        "filesystem_source": command_output(
            ["findmnt", "-n", "-o", "SOURCE", "-T", str(bench_dir)]
        ),
        "filesystem_type": command_output(
            ["findmnt", "-n", "-o", "FSTYPE", "-T", str(bench_dir)]
        ),
        "filesystem_target": command_output(
            ["findmnt", "-n", "-o", "TARGET", "-T", str(bench_dir)]
        ),
        "filesystem_free_bytes": str(filesystem.f_bavail * filesystem.f_frsize),
        "filesystem_total_bytes": str(filesystem.f_blocks * filesystem.f_frsize),
        "cpu_count": str(os.cpu_count() or 0),
        "cpu_online": Path("/sys/devices/system/cpu/online").read_text().strip(),
        "cpu_topology_sha256": stable_cpu_topology_sha256(),
        "page_size": str(os.sysconf("SC_PAGE_SIZE")),
        "governors": current_governors(),
    }


def validate_source_approval(
    values: dict[str, str],
    errors: list[str],
) -> dict[str, Any] | None:
    path = validate_bound_file(
        values.get("source_approval_path"),
        values.get("source_approval_sha256"),
        "source approval",
        errors,
    )
    if path is None:
        return None
    approval = read_canonical_json(path, "source approval", errors)
    if approval is None:
        return None
    require_exact_keys(
        approval,
        {
            "schema",
            "protocol",
            "status",
            "review_id",
            "reviewed_at",
            "baseline",
            "common",
            "variants",
        },
        "source approval",
        errors,
    )
    expected_top = {
        "schema": SOURCE_APPROVAL_SCHEMA,
        "protocol": PROTOCOL,
        "status": "approved",
        "review_id": values.get("source_review_id"),
    }
    for field, expected in expected_top.items():
        if approval.get(field) != expected:
            errors.append(
                f"source approval {field}={approval.get(field)!r} != {expected!r}"
            )
    if values.get("source_approval_schema") != SOURCE_APPROVAL_SCHEMA:
        errors.append("source_approval_schema is not frozen")
    if values.get("source_review_status") != "approved":
        errors.append("source_review_status is not approved")
    parse_timestamp(approval.get("reviewed_at"), "source approval reviewed_at", errors)

    baseline = approval.get("baseline")
    if not isinstance(baseline, dict):
        errors.append("source approval baseline is not an object")
    else:
        require_exact_keys(baseline, {"source", "tree"}, "approval baseline", errors)
        if baseline.get("source") != values.get("baseline_source"):
            errors.append("approval baseline source does not match provenance")
        if baseline.get("tree") != values.get("baseline_tree"):
            errors.append("approval baseline tree does not match provenance")

    common = approval.get("common")
    if not isinstance(common, dict):
        errors.append("source approval common is not an object")
    else:
        require_exact_keys(
            common,
            {"harness_sha256", "cargo_lock_sha256"},
            "approval common",
            errors,
        )
        if common.get("harness_sha256") != values.get("control_harness_sha256"):
            errors.append("approval common harness hash does not match provenance")
        if common.get("cargo_lock_sha256") != values.get(
            "control_cargo_lock_sha256"
        ):
            errors.append("approval common Cargo.lock hash does not match provenance")

    variants = approval.get("variants")
    if not isinstance(variants, dict):
        errors.append("source approval variants is not an object")
        return approval
    require_exact_keys(variants, set(VARIANTS), "approval variants", errors)
    for variant in VARIANTS:
        item = variants.get(variant)
        context = f"approval {variant}"
        if not isinstance(item, dict):
            errors.append(f"{context} is not an object")
            continue
        require_exact_keys(
            item,
            {
                "source",
                "tree",
                "diff_manifest_sha256",
                "patch_sha256",
                "allowed_changes",
            },
            context,
            errors,
        )
        for field in ("source", "tree", "diff_manifest_sha256", "patch_sha256"):
            expected = values.get(f"{variant}_{field}")
            if item.get(field) != expected:
                errors.append(f"{context} {field} does not match provenance")
        allowed = item.get("allowed_changes")
        if not isinstance(allowed, list):
            errors.append(f"{context} allowed_changes is not a list")
            continue
        for ordinal, change in enumerate(allowed, start=1):
            if not isinstance(change, dict):
                errors.append(f"{context} change {ordinal} is not an object")
                continue
            require_exact_keys(
                change,
                {"status", "path"},
                f"{context} change {ordinal}",
                errors,
            )
        ordered = sorted(
            allowed,
            key=lambda change: (
                str(change.get("path")) if isinstance(change, dict) else "",
                str(change.get("status")) if isinstance(change, dict) else "",
            ),
        )
        if allowed != ordered:
            errors.append(f"{context} allowed_changes is not canonically sorted")
    return approval


def validate_source_graph(
    values: dict[str, str],
    approval: dict[str, Any] | None,
    roots: dict[str, Path],
    errors: list[str],
) -> None:
    baseline = values.get("baseline_source", "")
    baseline_tree = values.get("baseline_tree", "")
    if not COMMIT.fullmatch(baseline):
        errors.append("baseline_source is not a full commit id")
    if not COMMIT.fullmatch(baseline_tree):
        errors.append("baseline_tree is not a full tree id")
    variants = approval.get("variants") if isinstance(approval, dict) else None

    for variant, root in roots.items():
        source = values.get(f"{variant}_source", "")
        tree = values.get(f"{variant}_tree", "")
        context = f"{variant} source graph"
        if not COMMIT.fullmatch(source):
            errors.append(f"{context} source is not a full commit id")
        if not COMMIT.fullmatch(tree):
            errors.append(f"{context} tree is not a full tree id")
        observed_source = git_value(root, ["rev-parse", "HEAD^{commit}"], errors)
        observed_tree = git_value(root, ["rev-parse", "HEAD^{tree}"], errors)
        observed_baseline_tree = git_value(
            root, ["rev-parse", f"{baseline}^{{tree}}"], errors
        )
        merge_base = git_value(root, ["merge-base", baseline, source], errors)
        if observed_source != source:
            errors.append(f"{context} HEAD commit does not match")
        if observed_tree != tree:
            errors.append(f"{context} HEAD tree does not match")
        if observed_baseline_tree != baseline_tree:
            errors.append(f"{context} baseline tree does not match")
        if merge_base != baseline:
            errors.append(f"{context} merge-base is not exact baseline")

        changes = canonical_diff_manifest(root, baseline, source, context, errors)
        diff_digest = hashlib.sha256(canonical_json_bytes(changes)).hexdigest()
        if diff_digest != values.get(f"{variant}_diff_manifest_sha256"):
            errors.append(f"{context} diff manifest hash does not match")
        approved_item = variants.get(variant) if isinstance(variants, dict) else None
        if isinstance(approved_item, dict) and changes != approved_item.get(
            "allowed_changes"
        ):
            errors.append(f"{context} live changes differ from approved allowlist")
        changed_paths = [change["path"] for change in changes]
        patch_digest = canonical_patch_sha256(
            root, baseline, source, changed_paths, context, errors
        )
        if patch_digest != values.get(f"{variant}_patch_sha256"):
            errors.append(f"{context} canonical patch hash does not match")
    pair_merge_base = git_value(
        roots["control"],
        [
            "merge-base",
            values.get("control_source", ""),
            values.get("candidate_source", ""),
        ],
        errors,
    )
    if pair_merge_base != baseline:
        errors.append("control/candidate merge-base is not exact baseline")


def git_tree_entries(
    root: Path,
    source: str,
    label: str,
    errors: list[str],
) -> list[dict[str, Any]]:
    payload = git_bytes(
        root,
        ["ls-tree", "-rz", "--full-tree", "-l", source],
        f"{label} git tree",
        errors,
    )
    records = payload.split(b"\0")
    if records and records[-1] == b"":
        records.pop()
    entries: list[dict[str, Any]] = []
    for ordinal, record in enumerate(records, start=1):
        try:
            metadata, path_bytes = record.split(b"\t", 1)
            mode_bytes, kind_bytes, object_bytes, size_bytes = metadata.split()
            mode = mode_bytes.decode("ascii")
            kind = kind_bytes.decode("ascii")
            object_id = object_bytes.decode("ascii")
            size = int(size_bytes)
            path = path_bytes.decode("utf-8")
        except (UnicodeError, ValueError) as error:
            errors.append(f"{label} git tree entry {ordinal} is malformed: {error}")
            continue
        if kind != "blob" or mode not in {"100644", "100755"}:
            errors.append(
                f"{label} rejects non-regular tree entry {mode} {kind} {path!r}"
            )
        if not COMMIT.fullmatch(object_id):
            errors.append(f"{label} tree object is malformed for {path!r}")
        if not path or path.startswith("/") or ".." in Path(path).parts:
            errors.append(f"{label} tree path is unsafe: {path!r}")
        entries.append(
            {
                "path": path,
                "mode": mode,
                "object_id": object_id,
                "size": size,
            }
        )
    ordered = sorted(entries, key=lambda entry: entry["path"])
    if entries != ordered:
        errors.append(f"{label} git tree is not path sorted")
    return ordered


def validate_source_manifest(
    path: Path,
    digest: str,
    *,
    variant: str,
    source: str,
    tree: str,
    root: Path | None,
    label: str,
    errors: list[str],
) -> dict[str, Any] | None:
    # The attestation caller has already resolved and bounded this path.  Keep
    # the independent hash replay without widening validate_bound_file's
    # external string boundary: explicitly serialize the trusted Path again.
    validated_path = validate_bound_file(str(path), digest, label, errors)
    if validated_path is None:
        return None
    manifest = read_canonical_json(validated_path, label, errors)
    if manifest is None:
        return None
    require_exact_keys(
        manifest,
        {
            "schema",
            "protocol",
            "variant",
            "source_commit",
            "source_tree",
            "root",
            "entries",
        },
        label,
        errors,
    )
    expected = {
        "schema": SOURCE_MANIFEST_SCHEMA,
        "protocol": PROTOCOL,
        "variant": variant,
        "source_commit": source,
        "source_tree": tree,
        "root": str(root) if root is not None else None,
    }
    for field, value in expected.items():
        if manifest.get(field) != value:
            errors.append(f"{label} {field} does not match")
    entries = manifest.get("entries")
    if not isinstance(entries, list):
        errors.append(f"{label} entries is not a list")
        return manifest
    for ordinal, entry in enumerate(entries, start=1):
        context = f"{label} entry {ordinal}"
        if not isinstance(entry, dict):
            errors.append(f"{context} is not an object")
            continue
        require_exact_keys(
            entry,
            {"path", "mode", "object_id", "size", "sha256"},
            context,
            errors,
        )
        if not isinstance(entry.get("path"), str) or not entry.get("path"):
            errors.append(f"{context} path is invalid")
        if entry.get("mode") not in {"100644", "100755"}:
            errors.append(f"{context} mode is not a regular Git mode")
        if not isinstance(entry.get("object_id"), str) or not COMMIT.fullmatch(
            entry["object_id"]
        ):
            errors.append(f"{context} object_id is not full SHA-1")
        if (
            isinstance(entry.get("size"), bool)
            or not isinstance(entry.get("size"), int)
            or entry["size"] < 0
        ):
            errors.append(f"{context} size is not nonnegative")
        if not isinstance(entry.get("sha256"), str) or not SHA256.fullmatch(
            entry["sha256"]
        ):
            errors.append(f"{context} sha256 is malformed")
    ordered = sorted(
        entries,
        key=lambda entry: str(entry.get("path")) if isinstance(entry, dict) else "",
    )
    if entries != ordered:
        errors.append(f"{label} entries are not path sorted")
    return manifest


def validate_materialized_files(
    materialized_root: Path,
    entries: list[dict[str, Any]],
    label: str,
    errors: list[str],
) -> None:
    observed_paths: set[str] = set()
    try:
        root_stat = materialized_root.lstat()
    except OSError as error:
        errors.append(f"{label} cannot stat root: {error}")
        return
    if not stat.S_ISDIR(root_stat.st_mode) or stat.S_IMODE(root_stat.st_mode) != 0o555:
        errors.append(f"{label} root is not a read-only directory")
    for directory, directory_names, file_names in os.walk(
        materialized_root, followlinks=False
    ):
        directory_path = Path(directory)
        for name in directory_names:
            path = directory_path / name
            try:
                mode = path.lstat().st_mode
            except OSError as error:
                errors.append(f"{label} cannot stat directory {path}: {error}")
                continue
            if not stat.S_ISDIR(mode) or stat.S_IMODE(mode) != 0o555:
                errors.append(f"{label} directory is not regular/read-only: {path}")
        for name in file_names:
            path = directory_path / name
            relative = path.relative_to(materialized_root).as_posix()
            observed_paths.add(relative)
            try:
                file_stat = path.lstat()
            except OSError as error:
                errors.append(f"{label} cannot stat {relative}: {error}")
                continue
            if not stat.S_ISREG(file_stat.st_mode) or file_stat.st_mode & 0o222:
                errors.append(f"{label} file is not regular/read-only: {relative}")
    expected_paths = {
        entry.get("path") for entry in entries if isinstance(entry.get("path"), str)
    }
    if observed_paths != expected_paths:
        errors.append(
            f"{label} filesystem paths differ: missing={sorted(expected_paths - observed_paths)}, "
            f"extra={sorted(observed_paths - expected_paths)}"
        )
    for entry in entries:
        path_value = entry.get("path")
        if not isinstance(path_value, str):
            continue
        path = materialized_root / path_value
        try:
            file_stat = path.lstat()
            payload = path.read_bytes()
        except OSError as error:
            errors.append(f"{label} cannot read {path_value}: {error}")
            continue
        git_object = hashlib.sha1(
            f"blob {len(payload)}\0".encode() + payload
        ).hexdigest()
        wanted_permissions = 0o555 if entry.get("mode") == "100755" else 0o444
        if stat.S_IMODE(file_stat.st_mode) != wanted_permissions:
            errors.append(f"{label} permissions mismatch for {path_value}")
        if len(payload) != entry.get("size"):
            errors.append(f"{label} size mismatch for {path_value}")
        if hashlib.sha256(payload).hexdigest() != entry.get("sha256"):
            errors.append(f"{label} SHA-256 mismatch for {path_value}")
        if git_object != entry.get("object_id"):
            errors.append(f"{label} Git object mismatch for {path_value}")


def validate_build_attestation(
    variant: str,
    values: dict[str, str],
    prepare_root: Path,
    errors: list[str],
) -> dict[str, Any] | None:
    context = f"{variant} build attestation"
    path = validate_bound_file(
        values.get(f"{variant}_build_attestation_path"),
        values.get(f"{variant}_build_attestation_sha256"),
        context,
        errors,
        within=prepare_root,
    )
    if path is None:
        return None
    attestation = read_canonical_json(path, context, errors)
    if attestation is None:
        return None
    require_exact_keys(
        attestation,
        {
            "schema",
            "protocol",
            "variant",
            "source_root",
            "baseline_source",
            "baseline_tree",
            "source_commit",
            "source_tree",
            "source_approval_sha256",
            "build_nonce",
            "target_dir",
            "target_dir_was_absent",
            "binary_path",
            "binary_sha256",
            "harness_path",
            "harness_sha256",
            "cargo_lock_path",
            "cargo_lock_sha256",
            "source_archive_path",
            "source_archive_sha256",
            "git_archive_argv",
            "tree_manifest_path",
            "tree_manifest_sha256",
            "materialized_root",
            "materialized_manifest_path",
            "materialized_manifest_sha256",
            "materialized_manifest_pre_sha256",
            "materialized_manifest_post_sha256",
            "source_read_only",
            "gitlinks_present",
            "sandbox_path",
            "sandbox_sha256",
            "diff_manifest_path",
            "diff_manifest_sha256",
            "patch_path",
            "patch_sha256",
            "build_log_path",
            "build_log_sha256",
            "cargo_argv",
            "build_argv",
            "contract_argv",
            "build_started_at",
            "build_completed_at",
            "build_child",
            "rustc",
            "cargo",
            "contract_output_path",
            "contract_output_sha256",
            "contract_child",
            "contract",
            "prepare_runner_path",
            "prepare_runner_sha256",
            "prepare_orchestrator_path",
            "prepare_orchestrator_sha256",
        },
        context,
        errors,
    )
    exact = {
        "schema": BUILD_ATTESTATION_SCHEMA,
        "protocol": PROTOCOL,
        "variant": variant,
        "source_root": values.get(f"{variant}_root"),
        "baseline_source": values.get("baseline_source"),
        "baseline_tree": values.get("baseline_tree"),
        "source_commit": values.get(f"{variant}_source"),
        "source_tree": values.get(f"{variant}_tree"),
        "source_approval_sha256": values.get("source_approval_sha256"),
        "build_nonce": values.get(f"{variant}_build_nonce"),
        "target_dir": values.get(f"{variant}_target_dir"),
        "binary_path": values.get(f"{variant}_binary_path"),
        "binary_sha256": values.get(f"{variant}_binary_sha256"),
        "harness_path": values.get(f"{variant}_harness_path"),
        "harness_sha256": values.get(f"{variant}_harness_sha256"),
        "cargo_lock_path": values.get(f"{variant}_cargo_lock_path"),
        "cargo_lock_sha256": values.get(f"{variant}_cargo_lock_sha256"),
        "materialized_root": values.get(f"{variant}_materialized_root"),
        "source_archive_path": values.get(f"{variant}_source_archive_path"),
        "source_archive_sha256": values.get(
            f"{variant}_source_archive_sha256"
        ),
        "tree_manifest_path": values.get(f"{variant}_tree_manifest_path"),
        "tree_manifest_sha256": values.get(
            f"{variant}_tree_manifest_sha256"
        ),
        "materialized_manifest_path": values.get(
            f"{variant}_materialized_manifest_path"
        ),
        "materialized_manifest_sha256": values.get(
            f"{variant}_materialized_manifest_sha256"
        ),
        "sandbox_path": values.get(f"{variant}_sandbox_path"),
        "sandbox_sha256": values.get(f"{variant}_sandbox_sha256"),
        "diff_manifest_sha256": values.get(f"{variant}_diff_manifest_sha256"),
        "patch_sha256": values.get(f"{variant}_patch_sha256"),
        "build_log_path": values.get(f"{variant}_build_log_path"),
        "build_log_sha256": values.get(f"{variant}_build_log_sha256"),
        "rustc": values.get("rustc"),
        "cargo": values.get("cargo"),
        "prepare_runner_path": values.get("prepare_runner_path"),
        "prepare_runner_sha256": values.get("prepare_runner_sha256"),
        "prepare_orchestrator_path": values.get("prepare_orchestrator_path"),
        "prepare_orchestrator_sha256": values.get(
            "prepare_orchestrator_sha256"
        ),
    }
    for field, expected in exact.items():
        if attestation.get(field) != expected:
            errors.append(f"{context} {field} does not match provenance")
    if attestation.get("target_dir_was_absent") is not True:
        errors.append(f"{context} target_dir_was_absent is not true")
    if attestation.get("source_read_only") is not True:
        errors.append(f"{context} source_read_only is not true")
    if attestation.get("gitlinks_present") is not False:
        errors.append(f"{context} gitlinks_present is not false")

    source_root_value = attestation.get("source_root")
    source_root = (
        Path(source_root_value).resolve()
        if isinstance(source_root_value, str)
        else prepare_root
    )
    materialized_value = attestation.get("materialized_root")
    materialized_root = (
        Path(materialized_value).resolve()
        if isinstance(materialized_value, str)
        else prepare_root
    )
    if source_root == materialized_root:
        errors.append(f"{context} materialized_root is the live Git source root")
    if not is_within(materialized_root, prepare_root):
        errors.append(f"{context} materialized_root is outside prepare output")
    if (materialized_root / ".git").exists() or (
        materialized_root / ".git"
    ).is_symlink():
        errors.append(f"{context} materialized source contains .git")

    archive_path = validate_bound_file(
        attestation.get("source_archive_path"),
        attestation.get("source_archive_sha256"),
        f"{context} source archive",
        errors,
        within=prepare_root,
    )
    expected_archive_argv = [
        "git",
        "-C",
        str(source_root),
        "archive",
        "--format=tar",
        values.get(f"{variant}_source", ""),
    ]
    if attestation.get("git_archive_argv") != expected_archive_argv:
        errors.append(f"{context} git_archive_argv is not exact")
    if archive_path is not None:
        archive_bytes = git_bytes(
            source_root,
            ["archive", "--format=tar", values.get(f"{variant}_source", "")],
            f"{context} source archive replay",
            errors,
        )
        if hashlib.sha256(archive_bytes).hexdigest() != attestation.get(
            "source_archive_sha256"
        ):
            errors.append(f"{context} source archive differs from live Git archive")

    tree_manifest_path = validate_bound_file(
        attestation.get("tree_manifest_path"),
        attestation.get("tree_manifest_sha256"),
        f"{context} tree manifest",
        errors,
        within=prepare_root,
    )
    materialized_manifest_path = validate_bound_file(
        attestation.get("materialized_manifest_path"),
        attestation.get("materialized_manifest_sha256"),
        f"{context} materialized manifest",
        errors,
        within=prepare_root,
    )
    tree_manifest = (
        validate_source_manifest(
            tree_manifest_path,
            str(attestation.get("tree_manifest_sha256", "")),
            variant=variant,
            source=values.get(f"{variant}_source", ""),
            tree=values.get(f"{variant}_tree", ""),
            root=None,
            label=f"{context} tree manifest",
            errors=errors,
        )
        if tree_manifest_path is not None
        else None
    )
    materialized_manifest = (
        validate_source_manifest(
            materialized_manifest_path,
            str(attestation.get("materialized_manifest_sha256", "")),
            variant=variant,
            source=values.get(f"{variant}_source", ""),
            tree=values.get(f"{variant}_tree", ""),
            root=materialized_root,
            label=f"{context} materialized manifest",
            errors=errors,
        )
        if materialized_manifest_path is not None
        else None
    )
    manifest_hash = attestation.get("materialized_manifest_sha256")
    if attestation.get("materialized_manifest_pre_sha256") != manifest_hash:
        errors.append(f"{context} pre-build manifest replay differs")
    if attestation.get("materialized_manifest_post_sha256") != manifest_hash:
        errors.append(f"{context} post-build manifest replay differs")
    live_tree = git_tree_entries(
        source_root,
        values.get(f"{variant}_source", ""),
        context,
        errors,
    )
    tree_entries = (
        tree_manifest.get("entries") if isinstance(tree_manifest, dict) else None
    )
    materialized_entries = (
        materialized_manifest.get("entries")
        if isinstance(materialized_manifest, dict)
        else None
    )
    if isinstance(tree_entries, list):
        projected = [
            {
                "path": entry.get("path"),
                "mode": entry.get("mode"),
                "object_id": entry.get("object_id"),
                "size": entry.get("size"),
            }
            for entry in tree_entries
            if isinstance(entry, dict)
        ]
        if projected != live_tree:
            errors.append(f"{context} tree manifest differs from live Git tree")
    if tree_entries != materialized_entries:
        errors.append(f"{context} materialized manifest differs from Git manifest")
    if isinstance(materialized_entries, list):
        validate_materialized_files(
            materialized_root, materialized_entries, context, errors
        )
    validate_bound_file(
        attestation.get("sandbox_path"),
        attestation.get("sandbox_sha256"),
        f"{context} sandbox",
        errors,
    )

    nonce = attestation.get("build_nonce")
    if not isinstance(nonce, str) or not SHA256.fullmatch(nonce):
        errors.append(f"{context} build_nonce is not 64 lowercase hex characters")
    target_value = attestation.get("target_dir")
    target_dir = (
        Path(target_value).resolve() if isinstance(target_value, str) else prepare_root
    )
    if not is_within(target_dir, prepare_root):
        errors.append(f"{context} target_dir is outside prepare output")
    expected_binary = target_dir / "release/examples/owned_append_bench"
    if isinstance(attestation.get("binary_path"), str) and Path(
        attestation["binary_path"]
    ).resolve() != expected_binary:
        errors.append(f"{context} binary is not under its fresh target directory")
    expected_harness = (
        materialized_root / "crates/mess-store/examples/owned_append_bench.rs"
    )
    expected_cargo_lock = materialized_root / "Cargo.lock"
    for field, expected_path in (
        ("harness_path", expected_harness),
        ("cargo_lock_path", expected_cargo_lock),
    ):
        value = attestation.get(field)
        if not isinstance(value, str) or Path(value).resolve() != expected_path:
            errors.append(f"{context} {field} is not in materialized source")
    expected_argv = [
        "cargo",
        "build",
        "--locked",
        "--release",
        "-p",
        "mess-store",
        "--example",
        "owned_append_bench",
        "--target-dir",
        str(target_dir),
    ]
    if attestation.get("cargo_argv") != expected_argv:
        errors.append(f"{context} cargo_argv is not frozen")
    build_argv = attestation.get("build_argv")
    contract_argv = attestation.get("contract_argv")
    if not isinstance(build_argv, list) or not all(
        isinstance(value, str) for value in build_argv
    ):
        errors.append(f"{context} build_argv is not a string list")
        build_argv = []
    if not isinstance(contract_argv, list) or not all(
        isinstance(value, str) for value in contract_argv
    ):
        errors.append(f"{context} contract_argv is not a string list")
        contract_argv = []
    sandbox_value = attestation.get("sandbox_path")
    if isinstance(sandbox_value, str):
        sandbox = str(Path(sandbox_value).resolve())
        sandbox_prefix = [
            sandbox,
            "--die-with-parent",
            "--ro-bind",
            "/",
            "/",
            "--proc",
            "/proc",
            "--dev-bind",
            "/dev",
            "/dev",
            "--tmpfs",
            "/tmp",
            "--bind",
            str(target_dir),
            str(target_dir),
            "--chdir",
            str(materialized_root),
        ]
        if build_argv != [*sandbox_prefix, *expected_argv]:
            errors.append(f"{context} build_argv is not exact sandboxed Cargo")
        if contract_argv != [*sandbox_prefix, str(expected_binary)]:
            errors.append(f"{context} contract_argv is not exact sandboxed binary")
    build_started = parse_timestamp(
        attestation.get("build_started_at"), f"{context} build_started_at", errors
    )
    build_completed = parse_timestamp(
        attestation.get("build_completed_at"),
        f"{context} build_completed_at",
        errors,
    )
    if (
        build_started is not None
        and build_completed is not None
        and build_completed < build_started
    ):
        errors.append(f"{context} build completed before it started")
    build_log = Path(str(attestation.get("build_log_path"))).resolve()
    validate_completed_child(
        attestation.get("build_child"),
        f"{context} build child",
        errors,
        expected_argv=build_argv,
        expected_output=build_log,
        expected_cwd=materialized_root,
    )
    contract_output = Path(str(attestation.get("contract_output_path"))).resolve()
    validate_completed_child(
        attestation.get("contract_child"),
        f"{context} contract child",
        errors,
        expected_argv=contract_argv,
        expected_output=contract_output,
        expected_cwd=materialized_root,
    )

    for label, path_field, hash_field in (
        ("build log", "build_log_path", "build_log_sha256"),
        ("contract output", "contract_output_path", "contract_output_sha256"),
        ("diff manifest", "diff_manifest_path", "diff_manifest_sha256"),
        ("canonical patch", "patch_path", "patch_sha256"),
    ):
        validate_bound_file(
            attestation.get(path_field),
            attestation.get(hash_field),
            f"{context} {label}",
            errors,
            within=prepare_root,
        )

    contract = attestation.get("contract")
    expected_contract = {
        "protocol": PROTOCOL,
        "contract_mode": True,
        "csv_written": False,
        "baseline_source": values.get("baseline_source"),
        "baseline_tree": values.get("baseline_tree"),
        "source_commit": values.get(f"{variant}_source"),
        "source_tree": values.get(f"{variant}_tree"),
        "harness_sha256": values.get(f"{variant}_harness_sha256"),
        "cargo_lock_sha256": values.get(f"{variant}_cargo_lock_sha256"),
        "source_approval_sha256": values.get("source_approval_sha256"),
        "build_nonce": values.get(f"{variant}_build_nonce"),
    }
    if contract != expected_contract:
        errors.append(f"{context} embedded contract is not exact")
    output_value = attestation.get("contract_output_path")
    if isinstance(output_value, str):
        try:
            output_contract = json.loads(Path(output_value).read_bytes())
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            errors.append(f"cannot read {context} contract output: {error}")
        else:
            if output_contract != contract:
                errors.append(f"{context} contract output differs from attestation")
    return attestation


def validate_prepared_pair(
    values: dict[str, str],
    errors: list[str],
) -> dict[str, dict[str, Any] | None]:
    manifest_path = validate_bound_file(
        values.get("prepare_manifest_path"),
        values.get("prepare_manifest_sha256"),
        "prepared pair manifest",
        errors,
    )
    if manifest_path is None:
        return {}
    prepare_root = manifest_path.parent.resolve()
    pair = read_canonical_json(manifest_path, "prepared pair manifest", errors)
    if pair is None:
        return {}
    require_exact_keys(
        pair,
        {
            "schema",
            "protocol",
            "created_at",
            "created_monotonic_ns",
            "tooling_source",
            "tooling_tree",
            "source_approval_path",
            "source_approval_sha256",
            "prepare_runner_path",
            "prepare_runner_sha256",
            "prepare_orchestrator_path",
            "prepare_orchestrator_sha256",
            "lease_event_path",
            "lease_event_sha256",
            "attestations",
            "pre_release_path",
            "pre_release_sha256",
            "release_path",
            "release_sha256",
            "terminal_path",
            "terminal_sha256",
            "failure_path",
            "failure_absent",
        },
        "prepared pair manifest",
        errors,
    )
    exact = {
        "schema": PREPARED_PAIR_SCHEMA,
        "protocol": PROTOCOL,
        "tooling_source": values.get("tooling_source"),
        "tooling_tree": values.get("tooling_tree"),
        "source_approval_path": values.get("source_approval_path"),
        "source_approval_sha256": values.get("source_approval_sha256"),
        "prepare_runner_path": values.get("prepare_runner_path"),
        "prepare_runner_sha256": values.get("prepare_runner_sha256"),
        "prepare_orchestrator_path": values.get("prepare_orchestrator_path"),
        "prepare_orchestrator_sha256": values.get("prepare_orchestrator_sha256"),
        "pre_release_path": values.get("prepare_pre_release_path"),
        "pre_release_sha256": values.get("prepare_pre_release_sha256"),
        "release_path": values.get("prepare_release_path"),
        "release_sha256": values.get("prepare_release_sha256"),
        "terminal_path": values.get("prepare_terminal_path"),
        "terminal_sha256": values.get("prepare_terminal_sha256"),
        "failure_path": values.get("prepare_failure_path"),
    }
    for field, expected in exact.items():
        if pair.get(field) != expected:
            errors.append(f"prepared pair {field} does not match provenance")
    created_at = parse_timestamp(pair.get("created_at"), "prepared pair created_at", errors)
    pair_created_ns = record_integer(
        pair, "created_monotonic_ns", "prepared pair", errors, positive=True
    )
    prepare_lease_path = validate_bound_file(
        pair.get("lease_event_path"),
        pair.get("lease_event_sha256"),
        "prepare lease event",
        errors,
        within=prepare_root,
    )
    prepare_lease: dict[str, Any] | None = None
    acquired_ns: int | None = None
    if prepare_lease_path is not None:
        prepare_lease = read_canonical_json(
            prepare_lease_path, "prepare lease event", errors
        )
        if prepare_lease is not None:
            require_exact_keys(
                prepare_lease,
                {
                    "protocol",
                    "event",
                    "path",
                    "device",
                    "inode",
                    "holder",
                    "uid",
                    "hostname",
                    "boot_id",
                    "nonce",
                    "acquired_at",
                    "acquired_monotonic_ns",
                },
                "prepare lease event",
                errors,
            )
            if prepare_lease.get("protocol") != PROTOCOL:
                errors.append("prepare lease protocol mismatch")
            if prepare_lease.get("event") != "prepare_acquired":
                errors.append("prepare lease event is not prepare_acquired")
            canonical_lock = (
                Path.home() / ".cache/mess-bench/global-measurement.lock"
            ).resolve()
            if prepare_lease.get("path") != str(canonical_lock):
                errors.append("prepare lease path is not canonical")
            try:
                current_lock = canonical_lock.stat()
            except OSError as error:
                errors.append(f"cannot stat prepare lease path: {error}")
            else:
                if prepare_lease.get("device") != current_lock.st_dev:
                    errors.append("prepare lease device changed")
                if prepare_lease.get("inode") != current_lock.st_ino:
                    errors.append("prepare lease inode changed")
            holder = prepare_lease.get("holder")
            if not isinstance(holder, dict):
                errors.append("prepare lease holder is not an object")
            else:
                require_exact_keys(
                    holder,
                    {"pid", "comm", "state", "ppid", "starttime"},
                    "prepare lease holder",
                    errors,
                )
                record_integer(holder, "pid", "prepare lease holder", errors, positive=True)
                record_integer(
                    holder, "starttime", "prepare lease holder", errors, positive=True
                )
            if prepare_lease.get("uid") != os.getuid():
                errors.append("prepare lease UID differs from evaluator")
            if prepare_lease.get("hostname") != socket.gethostname():
                errors.append("prepare lease hostname differs from evaluator")
            try:
                current_boot = Path(
                    "/proc/sys/kernel/random/boot_id"
                ).read_text().strip()
            except OSError as error:
                errors.append(f"cannot read current boot ID: {error}")
            else:
                if prepare_lease.get("boot_id") != current_boot:
                    errors.append("prepare lease boot ID differs from evaluator")
            if not isinstance(prepare_lease.get("nonce"), str) or not SHA256.fullmatch(
                prepare_lease["nonce"]
            ):
                errors.append("prepare lease nonce is malformed")
            acquired_ns = record_integer(
                prepare_lease,
                "acquired_monotonic_ns",
                "prepare lease event",
                errors,
                positive=True,
            )
            acquired_at = parse_timestamp(
                prepare_lease.get("acquired_at"),
                "prepare lease acquired_at",
                errors,
            )
            if (
                created_at is not None
                and acquired_at is not None
                and created_at < acquired_at
            ):
                errors.append("prepared pair predates prepare lease acquisition")

    failure_value = pair.get("failure_path")
    failure_path = (
        Path(failure_value).resolve()
        if isinstance(failure_value, str)
        else prepare_root / "failure.json"
    )
    if failure_path != (prepare_root / "failure.json").resolve():
        errors.append("prepared pair failure_path is not canonical")
    if values.get("prepare_failure_absent") != "true":
        errors.append("prepare_failure_absent provenance is not true")
    if pair.get("failure_absent") is not True or failure_path.exists() or failure_path.is_symlink():
        errors.append("prepared pair does not prove failure.json absent")

    pre_release_path = validate_bound_file(
        pair.get("pre_release_path"),
        pair.get("pre_release_sha256"),
        "prepare pre-release",
        errors,
        within=prepare_root,
    )
    release_path = validate_bound_file(
        pair.get("release_path"),
        pair.get("release_sha256"),
        "prepare release",
        errors,
        within=prepare_root,
    )
    terminal_path = validate_bound_file(
        pair.get("terminal_path"),
        pair.get("terminal_sha256"),
        "prepare terminal",
        errors,
        within=prepare_root,
    )
    pre_release = (
        read_canonical_json(pre_release_path, "prepare pre-release", errors)
        if pre_release_path is not None
        else None
    )
    release = (
        read_canonical_json(release_path, "prepare release", errors)
        if release_path is not None
        else None
    )
    terminal = (
        read_canonical_json(terminal_path, "prepare terminal", errors)
        if terminal_path is not None
        else None
    )
    pre_release_ns: int | None = None
    release_ns: int | None = None
    terminal_ns: int | None = None
    if pre_release is not None:
        require_exact_keys(
            pre_release,
            {
                "schema",
                "protocol",
                "created_at",
                "created_monotonic_ns",
                "tooling_source",
                "tooling_tree",
                "source_approval_path",
                "source_approval_sha256",
                "prepare_runner_path",
                "prepare_runner_sha256",
                "prepare_orchestrator_path",
                "prepare_orchestrator_sha256",
                "lease_event_path",
                "lease_event_sha256",
                "attestations",
                "failure_path",
                "failure_absent",
                "lease_held",
            },
            "prepare pre-release",
            errors,
        )
        expected_pre = {
            "schema": PREPARE_PRE_RELEASE_SCHEMA,
            "protocol": PROTOCOL,
            "tooling_source": pair.get("tooling_source"),
            "tooling_tree": pair.get("tooling_tree"),
            "source_approval_path": pair.get("source_approval_path"),
            "source_approval_sha256": pair.get("source_approval_sha256"),
            "prepare_runner_path": pair.get("prepare_runner_path"),
            "prepare_runner_sha256": pair.get("prepare_runner_sha256"),
            "prepare_orchestrator_path": pair.get("prepare_orchestrator_path"),
            "prepare_orchestrator_sha256": pair.get(
                "prepare_orchestrator_sha256"
            ),
            "lease_event_path": pair.get("lease_event_path"),
            "lease_event_sha256": pair.get("lease_event_sha256"),
            "attestations": pair.get("attestations"),
            "failure_path": pair.get("failure_path"),
            "failure_absent": True,
            "lease_held": True,
        }
        for field, expected_value in expected_pre.items():
            if pre_release.get(field) != expected_value:
                errors.append(f"prepare pre-release {field} does not match")
        parse_timestamp(
            pre_release.get("created_at"), "prepare pre-release created_at", errors
        )
        pre_release_ns = record_integer(
            pre_release,
            "created_monotonic_ns",
            "prepare pre-release",
            errors,
            positive=True,
        )
    if release is not None:
        require_exact_keys(
            release,
            {
                "schema",
                "protocol",
                "event",
                "released_at",
                "released_monotonic_ns",
                "lease_nonce",
                "pre_release_path",
                "pre_release_sha256",
            },
            "prepare release",
            errors,
        )
        expected_release = {
            "schema": PREPARE_RELEASE_SCHEMA,
            "protocol": PROTOCOL,
            "event": "prepare_released",
            "lease_nonce": prepare_lease.get("nonce")
            if isinstance(prepare_lease, dict)
            else None,
            "pre_release_path": pair.get("pre_release_path"),
            "pre_release_sha256": pair.get("pre_release_sha256"),
        }
        for field, expected_value in expected_release.items():
            if release.get(field) != expected_value:
                errors.append(f"prepare release {field} does not match")
        parse_timestamp(release.get("released_at"), "prepare released_at", errors)
        release_ns = record_integer(
            release,
            "released_monotonic_ns",
            "prepare release",
            errors,
            positive=True,
        )
    if terminal is not None:
        require_exact_keys(
            terminal,
            {
                "schema",
                "protocol",
                "outcome",
                "completed_at",
                "completed_monotonic_ns",
                "pre_release_path",
                "pre_release_sha256",
                "release_path",
                "release_sha256",
                "failure_path",
                "failure_absent",
            },
            "prepare terminal",
            errors,
        )
        expected_terminal = {
            "schema": PREPARE_TERMINAL_SCHEMA,
            "protocol": PROTOCOL,
            "outcome": "PREPARED",
            "pre_release_path": pair.get("pre_release_path"),
            "pre_release_sha256": pair.get("pre_release_sha256"),
            "release_path": pair.get("release_path"),
            "release_sha256": pair.get("release_sha256"),
            "failure_path": pair.get("failure_path"),
            "failure_absent": True,
        }
        for field, expected_value in expected_terminal.items():
            if terminal.get(field) != expected_value:
                errors.append(f"prepare terminal {field} does not match")
        parse_timestamp(
            terminal.get("completed_at"), "prepare terminal completed_at", errors
        )
        terminal_ns = record_integer(
            terminal,
            "completed_monotonic_ns",
            "prepare terminal",
            errors,
            positive=True,
        )
    timeline = [acquired_ns, pre_release_ns, release_ns, terminal_ns, pair_created_ns]
    if all(value is not None for value in timeline) and timeline != sorted(timeline):
        errors.append("prepare acquisition/release/terminal/pair chronology is invalid")

    for tool in ("prepare_runner", "prepare_orchestrator"):
        validate_bound_file(
            values.get(f"{tool}_path"),
            values.get(f"{tool}_sha256"),
            tool.replace("_", " "),
            errors,
        )
    attestations = pair.get("attestations")
    if not isinstance(attestations, dict):
        errors.append("prepared pair attestations is not an object")
        attestations = {}
    else:
        require_exact_keys(attestations, set(VARIANTS), "prepared pair attestations", errors)

    validated: dict[str, dict[str, Any] | None] = {}
    for variant in VARIANTS:
        binding = attestations.get(variant)
        context = f"prepared pair {variant} attestation"
        if not isinstance(binding, dict):
            errors.append(f"{context} is not an object")
        else:
            require_exact_keys(binding, {"path", "sha256"}, context, errors)
            if binding.get("path") != values.get(f"{variant}_build_attestation_path"):
                errors.append(f"{context} path does not match provenance")
            if binding.get("sha256") != values.get(
                f"{variant}_build_attestation_sha256"
            ):
                errors.append(f"{context} hash does not match provenance")
        validated[variant] = validate_build_attestation(
            variant, values, prepare_root, errors
        )
    if values.get("control_build_nonce") == values.get("candidate_build_nonce"):
        errors.append("control and candidate build nonces are identical")
    if Path(values.get("control_target_dir", ".")).resolve() == Path(
        values.get("candidate_target_dir", ".")
    ).resolve():
        errors.append("control and candidate target directories are identical")
    child_sequence: list[int] = []
    for variant in VARIANTS:
        attestation = validated.get(variant)
        if not isinstance(attestation, dict):
            continue
        build_child = attestation.get("build_child")
        contract_child = attestation.get("contract_child")
        if not isinstance(build_child, dict) or not isinstance(contract_child, dict):
            continue
        points = [
            build_child.get("started_monotonic_ns"),
            build_child.get("completed_monotonic_ns"),
            contract_child.get("started_monotonic_ns"),
            contract_child.get("completed_monotonic_ns"),
        ]
        if all(isinstance(point, int) and not isinstance(point, bool) for point in points):
            if points != sorted(points):
                errors.append(f"{variant} build/contract chronology is invalid")
            child_sequence.extend(points)
        build_started = parse_timestamp(
            attestation.get("build_started_at"),
            f"{variant} outer build_started_at",
            errors,
        )
        child_started = parse_timestamp(
            build_child.get("started_at"), f"{variant} build child started_at", errors
        )
        child_completed = parse_timestamp(
            build_child.get("completed_at"),
            f"{variant} build child completed_at",
            errors,
        )
        build_completed = parse_timestamp(
            attestation.get("build_completed_at"),
            f"{variant} outer build_completed_at",
            errors,
        )
        contract_started = parse_timestamp(
            contract_child.get("started_at"),
            f"{variant} contract child started_at",
            errors,
        )
        wall_points = [
            build_started,
            child_started,
            child_completed,
            build_completed,
            contract_started,
        ]
        if all(point is not None for point in wall_points) and wall_points != sorted(
            wall_points
        ):
            errors.append(f"{variant} outer build/contract timestamps are invalid")
    if child_sequence and child_sequence != sorted(child_sequence):
        errors.append("control/candidate builds or contract checks overlap/reorder")
    if (
        acquired_ns is not None
        and child_sequence
        and child_sequence[0] < acquired_ns
    ):
        errors.append("first build predates prepare lease acquisition")
    if (
        pre_release_ns is not None
        and child_sequence
        and pre_release_ns < child_sequence[-1]
    ):
        errors.append("prepare pre-release predates final contract child")
    return validated


def validate_pair_claim(
    values: dict[str, str],
    errors: list[str],
) -> dict[str, Any] | None:
    """Bind this admission attempt to the pair's one durable consumption record."""

    prepare_value = values.get("prepare_manifest_path")
    if not isinstance(prepare_value, str) or not prepare_value:
        errors.append("pair claim has no prepared-pair path")
        return None
    prepare_path = Path(prepare_value).resolve()
    prepare_root = prepare_path.parent
    claim_value = values.get("pair_claim_path")
    raw_claim_path = Path(claim_value) if isinstance(claim_value, str) else None
    claim_path = validate_bound_file(
        claim_value,
        values.get("pair_claim_sha256"),
        "pair claim",
        errors,
        within=prepare_root,
    )
    if claim_path is None:
        return None
    expected_claim_path = (prepare_root / "consumption.json").resolve()
    if claim_path != expected_claim_path:
        errors.append("pair claim path is not canonical consumption.json")
    if raw_claim_path is not None and raw_claim_path.is_symlink():
        errors.append("pair claim is a symbolic link")
    try:
        claim_stat = claim_path.stat()
    except OSError as error:
        errors.append(f"cannot stat pair claim {claim_path}: {error}")
    else:
        if not stat.S_ISREG(claim_stat.st_mode):
            errors.append("pair claim is not a regular file")
        if claim_stat.st_nlink != 1:
            errors.append("pair claim has unexpected additional hard links")

    claim = read_canonical_json(claim_path, "pair claim", errors)
    if claim is None:
        return None
    require_exact_keys(
        claim,
        {
            "schema",
            "protocol",
            "claimed_at",
            "claimed_monotonic_ns",
            "pair_path",
            "pair_sha256",
            "output_dir",
            "lease_nonce",
            "lease_path",
            "lease_device",
            "lease_inode",
            "lease_holder_pid",
            "lease_holder_starttime",
            "lease_boot_id",
        },
        "pair claim",
        errors,
    )
    output_value = values.get("paired_csv_path")
    expected_output = (
        Path(output_value).resolve().parent
        if isinstance(output_value, str) and output_value
        else None
    )
    expected: dict[str, Any] = {
        "schema": PAIR_CLAIM_SCHEMA,
        "protocol": PROTOCOL,
        "pair_path": str(prepare_path),
        "pair_sha256": values.get("prepare_manifest_sha256"),
        "output_dir": str(expected_output) if expected_output is not None else None,
        "lease_nonce": values.get("lease_nonce"),
        "lease_path": values.get("lease_path"),
        "lease_boot_id": values.get("lease_boot_id"),
    }
    for field in (
        "lease_device",
        "lease_inode",
        "lease_holder_pid",
        "lease_holder_starttime",
    ):
        expected[field] = parse_nonnegative_integer(
            values.get(field), f"pair claim provenance {field}", errors
        )
    for field, expected_value in expected.items():
        if claim.get(field) != expected_value:
            errors.append(f"pair claim {field} does not match provenance")
    if values.get("pair_claim_schema") != PAIR_CLAIM_SCHEMA:
        errors.append("pair_claim_schema provenance does not match")

    claimed_at = parse_timestamp(claim.get("claimed_at"), "pair claim claimed_at", errors)
    record_integer(
        claim,
        "claimed_monotonic_ns",
        "pair claim",
        errors,
        positive=True,
    )
    acquired_at = parse_timestamp(
        values.get("lease_acquired_at"), "pair claim lease_acquired_at", errors
    )
    started_at = parse_timestamp(
        values.get("started_at"), "pair claim provenance started_at", errors
    )
    if (
        claimed_at is not None
        and acquired_at is not None
        and claimed_at < acquired_at
    ):
        errors.append("pair claim predates measurement lease acquisition")
    if claimed_at is not None and started_at is not None and started_at < claimed_at:
        errors.append("measurement provenance predates pair claim")
    return claim


def validate_admission_provenance(
    values: dict[str, str],
    errors: list[str],
) -> None:
    needed = set(ADMISSION_PROVENANCE_FIELDS)
    if not needed.issubset(values):
        return

    for variant in VARIANTS:
        if values[f"{variant}_dirty"] != "false":
            errors.append(f"{variant} provenance is dirty")
        if not COMMIT.fullmatch(values[f"{variant}_source"]):
            errors.append(f"{variant} source is not a full commit id")
        if not SHA256.fullmatch(values[f"{variant}_binary_sha256"]):
            errors.append(f"{variant} binary hash is malformed")

    control_root = Path(values["control_root"]).resolve()
    candidate_root = Path(values["candidate_root"]).resolve()
    evaluator = Path(__file__).resolve()
    runner = evaluator.with_name("run_paired.sh")
    orchestrator = evaluator.with_name("run_paired.py")
    expected_paths = {
        "control_binary_path": Path(values["control_target_dir"]).resolve()
        / "release/examples/owned_append_bench",
        "candidate_binary_path": Path(values["candidate_target_dir"]).resolve()
        / "release/examples/owned_append_bench",
        "control_harness_path": Path(values["control_harness_path"]).resolve(),
        "candidate_harness_path": Path(values["candidate_harness_path"]).resolve(),
        "control_cargo_lock_path": Path(
            values["control_cargo_lock_path"]
        ).resolve(),
        "candidate_cargo_lock_path": Path(
            values["candidate_cargo_lock_path"]
        ).resolve(),
        "runner_path": runner,
        "orchestrator_path": orchestrator,
        "evaluator_path": evaluator,
    }
    for key, expected in expected_paths.items():
        observed = Path(values[key]).resolve()
        if observed != expected:
            errors.append(f"{key}={observed} != {expected}")

    hashed_paths = {
        "control_binary_sha256": expected_paths["control_binary_path"],
        "candidate_binary_sha256": expected_paths["candidate_binary_path"],
        "control_harness_sha256": expected_paths["control_harness_path"],
        "candidate_harness_sha256": expected_paths["candidate_harness_path"],
        "control_cargo_lock_sha256": expected_paths["control_cargo_lock_path"],
        "candidate_cargo_lock_sha256": expected_paths[
            "candidate_cargo_lock_path"
        ],
        "runner_sha256": runner,
        "orchestrator_sha256": orchestrator,
        "evaluator_sha256": evaluator,
    }
    for key, path in hashed_paths.items():
        claimed = values[key]
        if not SHA256.fullmatch(claimed):
            errors.append(f"{key} is malformed: {claimed!r}")
        try:
            observed = sha256(path)
        except OSError as error:
            errors.append(f"cannot hash {path}: {error}")
            continue
        if observed != claimed:
            errors.append(f"{key}={claimed} != current {observed}")

    if values["control_harness_sha256"] != values["candidate_harness_sha256"]:
        errors.append("control and candidate harness hashes differ")
    if values["control_cargo_lock_sha256"] != values[
        "candidate_cargo_lock_sha256"
    ]:
        errors.append("control and candidate Cargo.lock hashes differ")
    if values["control_source"] == values["candidate_source"]:
        errors.append("control and candidate source commits are identical")
    if values["control_binary_sha256"] == values["candidate_binary_sha256"]:
        errors.append("control and candidate binary hashes are identical")

    roots = {"control": control_root, "candidate": candidate_root}
    approval = validate_source_approval(values, errors)
    tooling_root = evaluator.parents[2]
    tooling_head = git_value(tooling_root, ["rev-parse", "HEAD^{commit}"], errors)
    tooling_tree = git_value(tooling_root, ["rev-parse", "HEAD^{tree}"], errors)
    tooling_dirty = git_value(tooling_root, ["status", "--porcelain"], errors)
    if values.get("tooling_source") != values.get("baseline_source"):
        errors.append("tooling source does not equal approval baseline")
    if values.get("tooling_tree") != values.get("baseline_tree"):
        errors.append("tooling tree does not equal approval baseline tree")
    if tooling_head != values.get("baseline_source"):
        errors.append("tooling checkout HEAD is not approval baseline")
    if tooling_tree != values.get("baseline_tree"):
        errors.append("tooling checkout tree is not approval baseline tree")
    if tooling_dirty:
        errors.append("tooling checkout is dirty during admission")
    validate_source_graph(values, approval, roots, errors)
    validate_prepared_pair(values, errors)
    validate_pair_claim(values, errors)

    for variant, root in roots.items():
        head = git_value(root, ["rev-parse", "HEAD"], errors)
        if head != values[f"{variant}_source"]:
            errors.append(f"{variant} HEAD {head} != frozen source")
        dirty = git_value(root, ["status", "--porcelain"], errors)
        if dirty:
            errors.append(f"{variant} worktree is currently dirty")

    started_at = parse_timestamp(values["started_at"], "started_at", errors)
    acquired_at = parse_timestamp(
        values["lease_acquired_at"], "lease_acquired_at", errors
    )
    if started_at is not None and acquired_at is not None and started_at < acquired_at:
        errors.append("provenance started_at predates lease acquisition")
    try:
        current_toolchain = {
            "kernel": command_output(["uname", "-a"]),
            "rustc": command_output(["rustc", "-Vv"]).replace("\n", "\\n"),
            "cargo": command_output(["cargo", "-V"]),
        }
    except (OSError, subprocess.SubprocessError) as error:
        errors.append(f"cannot observe current toolchain: {error}")
    else:
        for key, observed in current_toolchain.items():
            if values[key] != observed:
                errors.append(f"{key}={values[key]!r} != current {observed!r}")
    try:
        command = shlex.split(values["command"])
    except ValueError as error:
        errors.append(f"command is not valid shell syntax: {error}")
    else:
        expected_command = [
            str(orchestrator),
            str(Path(values["prepare_manifest_path"]).resolve()),
            str(Path(values["paired_csv_path"]).resolve().parent),
        ]
        if command != expected_command:
            errors.append(f"command={command!r} != exact runner invocation")

    canonical_lease = Path.home() / ".cache/mess-bench/global-measurement.lock"
    if Path(values["lease_path"]).resolve() != canonical_lease.resolve():
        errors.append(f"lease_path is not canonical {canonical_lease}")
    for key in (
        "lease_device",
        "lease_inode",
        "lease_holder_pid",
        "lease_holder_starttime",
        "lease_holder_uid",
    ):
        parse_nonnegative_integer(values[key], key, errors)
    for key in (
        "lease_hostname",
        "lease_acquired_at",
        "lease_nonce",
        "lease_boot_id",
    ):
        if not values[key]:
            errors.append(f"{key} is empty")
    if values["coordination_confirmed"] != "true":
        errors.append("coordination_confirmed is not true")
    claimed_order = values["physical_order_sha256"]
    expected_order = physical_order_sha256()
    if not SHA256.fullmatch(claimed_order):
        errors.append(f"physical_order_sha256 is malformed: {claimed_order!r}")
    elif claimed_order != expected_order:
        errors.append(
            f"physical_order_sha256={claimed_order} != evaluator {expected_order}"
        )

    expected_bench = Path(
        os.environ.get(
            "MESS_BENCH_DIR",
            str(Path.home() / ".cache/mess-bench"),
        )
    ).expanduser().resolve()
    observed_bench = Path(values["bench_dir"]).resolve()
    if observed_bench != expected_bench:
        errors.append(f"bench_dir={observed_bench} != current {expected_bench}")
    try:
        environment = observe_admission_environment(expected_bench)
    except (
        OSError,
        ValueError,
        subprocess.SubprocessError,
        json.JSONDecodeError,
    ) as error:
        errors.append(f"cannot observe benchmark environment: {error}")
    else:
        for key in (
            "filesystem_source",
            "filesystem_type",
            "filesystem_target",
            "filesystem_total_bytes",
            "cpu_count",
            "cpu_online",
            "cpu_topology_sha256",
            "page_size",
            "governors",
        ):
            current = environment[key]
            if values[key] != current:
                errors.append(f"{key}={values[key]!r} != current {current!r}")
        if environment["filesystem_type"] != "ext4":
            errors.append(
                f"benchmark filesystem is {environment['filesystem_type']}, not ext4"
            )
    free = parse_nonnegative_integer(
        values["filesystem_free_bytes"],
        "filesystem_free_bytes",
        errors,
    )
    if free is not None and free <= 0:
        errors.append("filesystem_free_bytes is not positive")


def parse_csv(
    csv_bytes: bytes,
    errors: list[str],
) -> tuple[tuple[str, ...], list[dict[str, str]]]:
    try:
        text = csv_bytes.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text, newline=""), strict=True)
        fields = tuple(reader.fieldnames or ())
        rows = list(reader)
    except (UnicodeDecodeError, csv.Error) as error:
        errors.append(f"cannot parse paired CSV: {error}")
        return (), []
    if fields != CSV_FIELDS:
        errors.append(f"CSV fields {fields} != frozen schema")
    for ordinal, row in enumerate(rows, start=1):
        if None in row:
            errors.append(f"row {ordinal} has extra CSV columns")
    return fields, rows


def parse_row_numbers(
    row: dict[str, str],
    ordinal: int,
    errors: list[str],
) -> tuple[dict[str, int], dict[str, float]] | None:
    integers: dict[str, int] = {}
    floats: dict[str, float] = {}
    before = len(errors)
    for field in INTEGER_FIELDS:
        raw = row.get(field)
        if raw is None:
            errors.append(f"row {ordinal}: missing {field}")
            continue
        parsed = parse_nonnegative_integer(raw, f"row {ordinal} {field}", errors)
        if parsed is not None:
            integers[field] = parsed
    for field in FLOAT_FIELDS:
        raw = row.get(field)
        try:
            parsed = float(raw) if raw is not None else math.nan
        except (TypeError, ValueError):
            errors.append(f"row {ordinal} {field} is not numeric: {raw!r}")
            continue
        if not math.isfinite(parsed):
            errors.append(f"row {ordinal} {field} is not finite: {raw!r}")
            continue
        if parsed < 0:
            errors.append(f"row {ordinal} {field} is negative: {parsed}")
            continue
        floats[field] = parsed
    for field in ("ev_s", "p99_us", "allocs_per_event", "alloc_bytes_per_event"):
        if field in floats and floats[field] <= 0:
            errors.append(f"row {ordinal} {field} must be positive")
    if len(errors) != before:
        return None
    return integers, floats


def validate_rows(
    rows: list[dict[str, str]],
    values: dict[str, str],
    errors: list[str],
) -> dict[tuple[str, int], dict[int, dict[str, list[dict[str, Any]]]]]:
    expected_order = physical_order()
    if len(rows) != len(expected_order):
        errors.append(f"row count {len(rows)} != {len(expected_order)}")

    cells: dict[
        tuple[str, int], dict[int, dict[str, list[dict[str, Any]]]]
    ] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    sources: dict[str, set[str]] = defaultdict(set)
    binaries: dict[str, set[str]] = defaultdict(set)

    for ordinal, row in enumerate(rows, start=1):
        numbers = parse_row_numbers(row, ordinal, errors)
        if numbers is None:
            continue
        integers, floats = numbers
        variant = row.get("variant", "")
        mode = row.get("mode", "")
        batch = integers["batch"]
        cycle = integers["cycle"]
        slot = integers["slot"]

        coordinates_valid = True
        if variant not in VARIANTS:
            errors.append(f"row {ordinal}: unexpected variant {variant!r}")
            coordinates_valid = False
        if mode not in MODES or batch not in BATCHES or cycle not in CYCLES:
            errors.append(
                f"row {ordinal}: unexpected cell {mode}/{batch}/cycle{cycle}"
            )
            coordinates_valid = False
        if slot not in (1, 2, 3, 4):
            errors.append(f"row {ordinal}: unexpected slot {slot}")
            coordinates_valid = False

        observed = (mode, batch, cycle, slot, variant)
        if ordinal <= len(expected_order) and observed != expected_order[ordinal - 1]:
            errors.append(
                f"row {ordinal}: physical order {observed} != "
                f"{expected_order[ordinal - 1]}"
            )
        if not coordinates_valid:
            continue

        enriched: dict[str, Any] = dict(row)
        enriched["_integers"] = integers
        enriched["_floats"] = floats
        cells[(mode, batch)][cycle][variant].append(enriched)
        sources[variant].add(row.get("source", ""))
        binaries[variant].add(row.get("binary_sha256", ""))

        context = f"{mode}/b{batch}/c{cycle}/s{slot}/{variant}"
        if integers["writers"] != WRITERS:
            errors.append(f"{context}: writers != {WRITERS}")
        wanted_bpw = PROCESS_BPW[batch]
        if integers["bpw"] != wanted_bpw:
            errors.append(f"{context}: bpw={integers['bpw']} != {wanted_bpw}")
        if integers["payload"] != PAYLOAD:
            errors.append(f"{context}: payload != {PAYLOAD}")
        appends = WRITERS * wanted_bpw
        wanted_events = batch * appends
        if integers["events"] != wanted_events:
            errors.append(
                f"{context}: events={integers['events']} != {wanted_events}"
            )
        if integers["batches"] != appends:
            errors.append(f"{context}: batches={integers['batches']} != {appends}")
        if row.get("fsync_degraded") != "false":
            errors.append(f"{context}: fsync_degraded is not false")
        if floats["pre_load1"] >= 6.0:
            errors.append(f"{context}: pre_load1 >= 6")
        if integers["batches"] != appends:
            errors.append(f"{context}: batches != measured appends")
        if integers["groups"] != 0:
            errors.append(f"{context}: Process groups != 0")
        if integers["fsyncs"] != 0 or integers["fsync_p99_ns"] != 0:
            errors.append(f"{context}: Process fsync counters are not both zero")
        if floats["p50_us"] <= 0 or floats["p50_us"] > floats["p99_us"]:
            errors.append(f"{context}: latency order is not 0 < p50 <= p99")

        raw_allocs_per_event = integers["allocs"] / wanted_events
        raw_bytes_per_event = integers["alloc_bytes"] / wanted_events
        expected_allocs_text = f"{raw_allocs_per_event:.4f}"
        expected_bytes_text = f"{raw_bytes_per_event:.2f}"
        if row.get("allocs_per_event") != expected_allocs_text:
            errors.append(
                f"{context}: allocs_per_event={row.get('allocs_per_event')!r} "
                f"!= raw-derived {expected_allocs_text!r} at frozen precision"
            )
        if row.get("alloc_bytes_per_event") != expected_bytes_text:
            errors.append(
                f"{context}: alloc_bytes_per_event="
                f"{row.get('alloc_bytes_per_event')!r} != raw-derived "
                f"{expected_bytes_text!r} at frozen precision"
            )
        # Gate only on the authoritative raw totals, never the redundant CSV
        # ratios. The textual checks above retain corruption detection.
        floats["allocs_per_event"] = raw_allocs_per_event
        floats["alloc_bytes_per_event"] = raw_bytes_per_event
        if variant == "candidate":
            exact = {
                "owned_batches": appends,
                "owned_records": wanted_events,
                "owned_payload_bytes": wanted_events * PAYLOAD,
                "borrowed_batches": 0,
                "borrowed_records": 0,
                "copied_records": 0,
                "copied_bytes": 0,
            }
            for field, wanted in exact.items():
                if integers[field] != wanted:
                    errors.append(
                        f"{context}: candidate {field}={integers[field]} != {wanted}"
                    )
        else:
            copied_records = wanted_events if batch in (1, 10) else 0
            exact = {
                "owned_batches": 0,
                "owned_records": 0,
                "owned_payload_bytes": 0,
                "borrowed_batches": appends,
                "borrowed_records": wanted_events,
                "copied_records": copied_records,
                "copied_bytes": copied_records * 261,
            }
            for field, wanted in exact.items():
                if integers[field] != wanted:
                    errors.append(
                        f"{context}: control {field}={integers[field]} != {wanted}"
                    )

    if values.get("evidence_mode") == "admission":
        for variant in VARIANTS:
            source = values.get(f"{variant}_source")
            binary = values.get(f"{variant}_binary_sha256")
            if source is not None and sources[variant] != {source}:
                errors.append(
                    f"{variant} CSV sources {sources[variant]} != {{{source}}}"
                )
            if binary is not None and binaries[variant] != {binary}:
                errors.append(
                    f"{variant} CSV binaries {binaries[variant]} != {{{binary}}}"
                )
    else:
        for variant in VARIANTS:
            if len(sources[variant]) != 1 or not all(sources[variant]):
                errors.append(f"fixture {variant} sources are not singular")
            if len(binaries[variant]) != 1 or not all(binaries[variant]):
                errors.append(f"fixture {variant} binaries are not singular")

    for mode in MODES:
        for batch in BATCHES:
            cycles = cells[(mode, batch)]
            for cycle in CYCLES:
                for variant in VARIANTS:
                    count = len(cycles[cycle][variant])
                    if count != 2:
                        errors.append(
                            f"{mode}/b{batch}/cycle{cycle}/{variant}: "
                            f"{count} rows, expected 2"
                        )
                slots = {
                    row["_integers"]["slot"]
                    for variant in VARIANTS
                    for row in cycles[cycle][variant]
                }
                if slots != {1, 2, 3, 4}:
                    errors.append(f"{mode}/b{batch}/cycle{cycle}: slots={slots}")
    return cells


def median_ratio(
    cycles: dict[int, dict[str, list[dict[str, Any]]]],
    field: str,
) -> tuple[float, list[float]]:
    ratios = []
    for cycle in CYCLES:
        control = statistics.median(
            row["_floats"][field] for row in cycles[cycle]["control"]
        )
        candidate = statistics.median(
            row["_floats"][field] for row in cycles[cycle]["candidate"]
        )
        ratios.append(candidate / control)
    return statistics.median(ratios), ratios


def geometric_mean(values: list[float]) -> float:
    if not values or any(not math.isfinite(value) or value <= 0 for value in values):
        raise ValueError("geometric mean requires finite positive values")
    return math.exp(sum(math.log(value) for value in values) / len(values))


def evaluate_gates(
    cells: dict[
        tuple[str, int], dict[int, dict[str, list[dict[str, Any]]]]
    ],
) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
    results: list[dict[str, Any]] = []
    failures: list[str] = []
    throughput_cells: list[float] = []
    allocation_cells: list[float] = []
    allocation_byte_cells: list[float] = []

    for mode in MODES:
        for batch in BATCHES:
            cycles = cells[(mode, batch)]
            throughput, throughput_cycles = median_ratio(cycles, "ev_s")
            p99, p99_cycles = median_ratio(cycles, "p99_us")
            allocs, alloc_cycles = median_ratio(cycles, "allocs_per_event")
            alloc_bytes, byte_cycles = median_ratio(
                cycles,
                "alloc_bytes_per_event",
            )
            throughput_cells.append(throughput)
            allocation_cells.append(allocs)
            allocation_byte_cells.append(alloc_bytes)
            passed = (
                throughput >= THROUGHPUT_FLOOR
                and p99 <= P99_CEILING
                and allocs <= ALLOCATION_CEILING
                and alloc_bytes <= ALLOCATION_CEILING
            )
            if not passed:
                failures.append(f"gate failure: {mode}/b{batch}")
            results.append(
                {
                    "mode": mode,
                    "batch": batch,
                    "throughput_B_over_A": throughput,
                    "p99_B_over_A": p99,
                    "allocs_B_over_A": allocs,
                    "bytes_B_over_A": alloc_bytes,
                    "cycle_ratios": {
                        "throughput": throughput_cycles,
                        "p99": p99_cycles,
                        "allocs": alloc_cycles,
                        "bytes": byte_cycles,
                    },
                    "passed": passed,
                }
            )

    material = {
        "throughput_B_over_A": geometric_mean(throughput_cells),
        "allocs_B_over_A": geometric_mean(allocation_cells),
        "bytes_B_over_A": geometric_mean(allocation_byte_cells),
    }
    material["passed"] = (
        material["throughput_B_over_A"] >= MATERIAL_THROUGHPUT_FLOOR
        and material["allocs_B_over_A"] <= MATERIAL_ALLOCATION_CEILING
        and material["bytes_B_over_A"] <= MATERIAL_ALLOCATION_CEILING
    )
    if not material["passed"]:
        failures.append("gate failure: Process material geometric means")
    return results, material, failures


def thresholds() -> dict[str, float]:
    return {
        "cell_throughput_floor": THROUGHPUT_FLOOR,
        "cell_p99_ceiling": P99_CEILING,
        "cell_allocation_calls_ceiling": ALLOCATION_CEILING,
        "cell_allocation_bytes_ceiling": ALLOCATION_CEILING,
        "material_throughput_floor": MATERIAL_THROUGHPUT_FLOOR,
        "material_allocation_calls_ceiling": MATERIAL_ALLOCATION_CEILING,
        "material_allocation_bytes_ceiling": MATERIAL_ALLOCATION_CEILING,
    }


def make_base_result(evidence_mode: str) -> dict[str, Any]:
    return {
        "schema": RESULT_SCHEMA,
        "protocol": PROTOCOL,
        "evidence_mode": evidence_mode,
        "evaluated_at": timestamp(),
        "evaluator_path": str(Path(__file__).resolve()),
        "thresholds": thresholds(),
        "expected_rows": len(physical_order()),
    }


def evaluate(
    csv_path: Path,
    provenance_path: Path,
    evidence_mode: str,
) -> tuple[dict[str, Any], int]:
    errors: list[str] = []
    required = COMMON_PROVENANCE_FIELDS
    if evidence_mode == "admission":
        required += ADMISSION_PROVENANCE_FIELDS
    values = parse_provenance(provenance_path, required, errors)

    if values.get("protocol") != PROTOCOL:
        errors.append(
            f"protocol {values.get('protocol')!r} != {PROTOCOL!r}"
        )
    if values.get("evidence_mode") != evidence_mode:
        errors.append(
            f"evidence_mode {values.get('evidence_mode')!r} != "
            f"invocation {evidence_mode!r}"
        )

    try:
        csv_bytes = csv_path.read_bytes()
    except OSError as error:
        errors.append(f"cannot read CSV {csv_path}: {error}")
        csv_bytes = b""
    csv_digest = hashlib.sha256(csv_bytes).hexdigest()
    fields, rows = parse_csv(csv_bytes, errors)
    output_dir = csv_path.parent.resolve()

    claimed_csv_path = values.get("paired_csv_path")
    if claimed_csv_path is not None and Path(claimed_csv_path).resolve() != csv_path:
        errors.append(
            f"paired_csv_path={Path(claimed_csv_path).resolve()} != {csv_path}"
        )
    claimed_digest = values.get("paired_csv_sha256")
    if claimed_digest is not None:
        if not SHA256.fullmatch(claimed_digest):
            errors.append(f"paired_csv_sha256 is malformed: {claimed_digest!r}")
        elif claimed_digest != csv_digest:
            errors.append(
                f"paired_csv_sha256={claimed_digest} != current {csv_digest}"
            )
    rows_claimed = values.get("paired_csv_data_rows")
    columns_claimed = values.get("paired_csv_columns")
    bytes_claimed = values.get("paired_csv_bytes")
    parsed_bytes = (
        parse_nonnegative_integer(bytes_claimed, "paired_csv_bytes", errors)
        if bytes_claimed is not None
        else None
    )
    parsed_rows = (
        parse_nonnegative_integer(rows_claimed, "paired_csv_data_rows", errors)
        if rows_claimed is not None
        else None
    )
    parsed_columns = (
        parse_nonnegative_integer(columns_claimed, "paired_csv_columns", errors)
        if columns_claimed is not None
        else None
    )
    if parsed_rows is not None and parsed_rows != len(rows):
        errors.append(f"provenance rows {parsed_rows} != current {len(rows)}")
    if parsed_columns is not None and parsed_columns != len(fields):
        errors.append(
            f"provenance columns {parsed_columns} != current {len(fields)}"
        )
    if parsed_bytes is not None and parsed_bytes != len(csv_bytes):
        errors.append(
            f"provenance bytes {parsed_bytes} != current {len(csv_bytes)}"
        )

    manifest_summaries = validate_bound_manifests(
        values,
        output_dir,
        evidence_mode,
        csv_bytes,
        errors,
    )
    if evidence_mode == "admission":
        validate_admission_provenance(values, errors)
    cells = validate_rows(rows, values, errors)

    cell_results: list[dict[str, Any]] = []
    material: dict[str, Any] = {}
    gate_failures: list[str] = []
    if not errors:
        try:
            cell_results, material, gate_failures = evaluate_gates(cells)
        except (
            ArithmeticError,
            KeyError,
            statistics.StatisticsError,
            ValueError,
        ) as error:
            errors.append(f"cannot compute gates: {error}")

    try:
        provenance_digest = sha256(provenance_path)
    except OSError as error:
        errors.append(f"cannot hash provenance {provenance_path}: {error}")
        provenance_digest = None

    result = make_base_result(evidence_mode)
    result.update(
        {
            "complete_matrix": len(rows) == len(physical_order()),
            "valid_evidence": not errors,
            "errors": errors,
            "gate_failures": gate_failures,
            "cells": cell_results,
            "material": material,
            "artifacts": {
                "paired_csv": {
                    "path": str(csv_path),
                    "sha256": csv_digest,
                    "rows": len(rows),
                    "columns": len(fields),
                },
                "provenance": {
                    "path": str(provenance_path),
                    "sha256": provenance_digest,
                },
                **manifest_summaries,
            },
        }
    )

    if evidence_mode == "fixture":
        if errors or gate_failures:
            result["outcome"] = "FIXTURE_INVALID"
            result["exit_code"] = EXIT_INVALID
            return result, EXIT_INVALID
        result["outcome"] = "FIXTURE_PASS"
        result["exit_code"] = EXIT_ADOPT
        return result, EXIT_ADOPT
    if errors:
        result["outcome"] = "INVALID_EVIDENCE"
        result["exit_code"] = EXIT_INVALID
        return result, EXIT_INVALID
    if gate_failures:
        result["outcome"] = "DECLINED"
        result["exit_code"] = EXIT_DECLINED
        return result, EXIT_DECLINED
    result["outcome"] = "ADOPT"
    result["exit_code"] = EXIT_ADOPT
    return result, EXIT_ADOPT


def validate_terminal_evaluator_child(
    child: Any,
    output_dir: Path,
    provenance: dict[str, str],
    expected_exit: int | None,
    pre_release_at: datetime | None,
    errors: list[str],
) -> tuple[datetime | None, int | None]:
    """Validate the admission evaluator child after its parent released the lease."""

    label = "terminal evaluator child"
    if not isinstance(child, dict):
        errors.append(f"{label} is not an object")
        return None, None
    require_exact_keys(
        child,
        {
            "protocol",
            "kind",
            "context",
            "ordinal",
            "mode",
            "batch",
            "cycle",
            "slot",
            "variant",
            "argv",
            "started_at",
            "started_monotonic_ns",
            "completed_at",
            "completed_monotonic_ns",
            "identity",
            "pid",
            "starttime",
            "waited_pid",
            "binary_sha256",
            "exit_status",
            "timed_out",
            "terminated_by_runner",
            "interrupted",
            "reaping",
            "process_group_absent",
            "output_path",
            "output_sha256",
            "csv_before",
            "csv_after",
            "csv_byte_delta",
            "csv_row_delta",
            "csv_rows_before",
            "csv_rows_after",
            "csv_bytes_before",
            "csv_bytes_after",
            "csv_prefix_sha256_before",
            "csv_prefix_sha256_after",
        },
        label,
        errors,
    )
    if child.get("protocol") != PROTOCOL:
        errors.append(f"{label} protocol mismatch")
    if child.get("kind") != "evaluator":
        errors.append(f"{label} kind is not evaluator")
    if child.get("context") != {"evidence_mode": "admission"}:
        errors.append(f"{label} context is not exact")
    for field in ("ordinal", "mode", "batch", "cycle", "slot", "variant"):
        if child.get(field) is not None:
            errors.append(f"{label} {field} is not null")
    expected_argv = [
        str(Path(provenance.get("evaluator_path", "")).resolve()),
        str((output_dir / "paired.csv").resolve()),
        str((output_dir / "provenance.txt").resolve()),
        str((output_dir / "result.json").resolve()),
    ]
    if child.get("argv") != expected_argv:
        errors.append(f"{label} argv is not exact")
    pid = record_integer(child, "pid", label, errors, positive=True)
    starttime = record_integer(child, "starttime", label, errors, positive=True)
    waited_pid = record_integer(child, "waited_pid", label, errors, positive=True)
    if pid is not None and waited_pid != pid:
        errors.append(f"{label} waited_pid does not match pid")
    identity = child.get("identity")
    if not isinstance(identity, dict):
        errors.append(f"{label} identity is not an object")
    else:
        require_exact_keys(
            identity,
            {"pid", "comm", "state", "ppid", "starttime_ticks"},
            f"{label} identity",
            errors,
        )
        if identity.get("pid") != pid:
            errors.append(f"{label} identity pid does not match")
        if identity.get("starttime_ticks") != starttime:
            errors.append(f"{label} identity starttime does not match")
        identity_ppid = record_integer(
            identity, "ppid", f"{label} identity", errors, positive=True
        )
        expected_parent = parse_nonnegative_integer(
            provenance.get("lease_holder_pid"),
            f"{label} lease holder pid",
            errors,
        )
        if identity_ppid != expected_parent:
            errors.append(f"{label} parent pid differs from lease holder")
        for field in ("comm", "state"):
            if not isinstance(identity.get(field), str) or not identity.get(field):
                errors.append(f"{label} identity {field} is not textual")
    if child.get("exit_status") != expected_exit:
        errors.append(f"{label} exit_status does not match evaluator outcome")
    if child.get("timed_out") is not False:
        errors.append(f"{label} timed_out is not false")
    if child.get("terminated_by_runner") is not False:
        errors.append(f"{label} terminated_by_runner is not false")
    if child.get("interrupted") is not None:
        errors.append(f"{label} interrupted is not null")
    if child.get("process_group_absent") is not True:
        errors.append(f"{label} process_group_absent is not true")
    if child.get("binary_sha256") is not None:
        errors.append(f"{label} binary_sha256 is not null")
    reaping = child.get("reaping")
    validate_reaping(reaping, pid, starttime, label, errors)
    if isinstance(reaping, dict):
        status = reaping.get("status")
        expected_reaping_keys = (
            {"status", "pid", "starttime"}
            if status == "absent"
            else {
                "status",
                "pid",
                "starttime",
                "observed_starttime",
                "observed",
            }
        )
        require_exact_keys(reaping, expected_reaping_keys, f"{label} reaping", errors)
        if status == "pid_reused":
            observed = reaping.get("observed")
            if not isinstance(observed, dict):
                errors.append(f"{label} reused process identity is not an object")
            else:
                require_exact_keys(
                    observed,
                    {"pid", "comm", "state", "ppid", "starttime_ticks"},
                    f"{label} reused process identity",
                    errors,
                )
                if observed.get("pid") != pid:
                    errors.append(f"{label} reused process pid differs")
                if observed.get("starttime_ticks") != reaping.get(
                    "observed_starttime"
                ):
                    errors.append(f"{label} reused process starttime differs")
    empty_digest = hashlib.sha256(b"").hexdigest()
    for field in (
        "csv_before",
        "csv_after",
        "csv_byte_delta",
        "csv_row_delta",
        "csv_rows_before",
        "csv_rows_after",
        "csv_bytes_before",
        "csv_bytes_after",
    ):
        if child.get(field) is not None:
            errors.append(f"{label} {field} is not null")
    for field in ("csv_prefix_sha256_before", "csv_prefix_sha256_after"):
        if child.get(field) != empty_digest:
            errors.append(f"{label} {field} is not the empty-prefix digest")

    expected_output = (output_dir / "evaluation.log").resolve()
    validate_bound_file(
        child.get("output_path"),
        child.get("output_sha256"),
        f"{label} output",
        errors,
        within=output_dir,
    )
    if child.get("output_path") != str(expected_output):
        errors.append(f"{label} output path is not exact")
    started_at = parse_timestamp(child.get("started_at"), f"{label} started_at", errors)
    completed_at = parse_timestamp(
        child.get("completed_at"), f"{label} completed_at", errors
    )
    if started_at is not None and completed_at is not None and completed_at < started_at:
        errors.append(f"{label} completed before it started")
    if (
        completed_at is not None
        and pre_release_at is not None
        and pre_release_at < completed_at
    ):
        errors.append("terminal pre-release predates evaluator completion")
    started_ns = record_integer(
        child, "started_monotonic_ns", label, errors, positive=True
    )
    completed_ns = record_integer(
        child, "completed_monotonic_ns", label, errors, positive=True
    )
    if started_ns is not None and completed_ns is not None and completed_ns < started_ns:
        errors.append(f"{label} monotonic completion precedes start")
    return started_at, started_ns


def verify_terminal(output_dir: Path) -> tuple[dict[str, Any], int]:
    """Independently validate the released measurement terminal chain."""

    errors: list[str] = []
    output_dir = output_dir.resolve()
    terminal_path = output_dir / "terminal.json"
    release_path = output_dir / "lease_release.json"
    pre_release_path = output_dir / "terminal_pre_release.json"
    result_path = output_dir / "result.json"
    provenance_path = output_dir / "provenance.txt"
    for failure_name in ("failure.json", "post_release_failure.json"):
        failure_path = output_dir / failure_name
        if failure_path.exists() or failure_path.is_symlink():
            errors.append(f"terminal output contains {failure_name}")
    if not output_dir.is_dir():
        errors.append(f"terminal output directory is missing: {output_dir}")

    for label, path in (
        ("terminal", terminal_path),
        ("release", release_path),
        ("pre-release", pre_release_path),
        ("result", result_path),
        ("provenance", provenance_path),
    ):
        if path.is_symlink():
            errors.append(f"{label} artifact is a symbolic link")
        try:
            mode = path.stat().st_mode
        except OSError as error:
            errors.append(f"cannot stat {label} artifact {path}: {error}")
        else:
            if not stat.S_ISREG(mode):
                errors.append(f"{label} artifact is not a regular file")

    pre_release = read_canonical_json(pre_release_path, "terminal pre-release", errors)
    release = read_canonical_json(release_path, "terminal release", errors)
    terminal = read_canonical_json(terminal_path, "terminal", errors)
    result = read_json_object(result_path, "evaluation result", errors)
    provenance = parse_provenance(
        provenance_path,
        COMMON_PROVENANCE_FIELDS + ADMISSION_PROVENANCE_FIELDS,
        errors,
    )

    pre_fields = {
        "protocol",
        "outcome",
        "evaluator_exit",
        "completed_at",
        "result_path",
        "result_sha256",
        "provenance_sha256",
        "pair_claim_path",
        "pair_claim_sha256",
        "evaluator_child",
        "artifact_inventory",
    }
    release_fields = {"protocol", "event", "nonce", "terminal", "released_at"}
    terminal_extra = {
        "terminal_pre_release_path",
        "terminal_pre_release_sha256",
        "lease_release_path",
        "lease_release_sha256",
        "lease_release",
        "terminal_published_at",
    }
    if pre_release is not None:
        require_exact_keys(pre_release, pre_fields, "terminal pre-release", errors)
    if release is not None:
        require_exact_keys(release, release_fields, "terminal release", errors)
    if terminal is not None:
        require_exact_keys(terminal, pre_fields | terminal_extra, "terminal", errors)
    if pre_release is not None and terminal is not None:
        for field in pre_fields:
            if terminal.get(field) != pre_release.get(field):
                errors.append(f"terminal {field} differs from pre-release")
    if terminal is not None:
        bindings = {
            "terminal_pre_release_path": str(pre_release_path),
            "terminal_pre_release_sha256": (
                sha256(pre_release_path) if pre_release_path.is_file() else None
            ),
            "lease_release_path": str(release_path),
            "lease_release_sha256": (
                sha256(release_path) if release_path.is_file() else None
            ),
            "lease_release": release,
        }
        for field, expected in bindings.items():
            if terminal.get(field) != expected:
                errors.append(f"terminal {field} binding mismatch")

    expected_exit: int | None = None
    expected_outcome: str | None = None
    if pre_release is not None:
        if pre_release.get("protocol") != PROTOCOL:
            errors.append("terminal pre-release protocol mismatch")
        expected_outcome = pre_release.get("outcome")
        expected_exit = {"ADOPT": EXIT_ADOPT, "DECLINED": EXIT_DECLINED}.get(
            expected_outcome
        )
        if expected_exit is None:
            errors.append("terminal pre-release outcome is not ADOPT or DECLINED")
        if pre_release.get("evaluator_exit") != expected_exit:
            errors.append("terminal pre-release evaluator exit mismatch")
        if pre_release.get("result_path") != str(result_path):
            errors.append("terminal result path is not exact")
        if result_path.is_file() and pre_release.get("result_sha256") != sha256(result_path):
            errors.append("terminal result hash mismatch")
        if provenance_path.is_file() and pre_release.get("provenance_sha256") != sha256(
            provenance_path
        ):
            errors.append("terminal provenance hash mismatch")
        if not isinstance(pre_release.get("artifact_inventory"), list):
            errors.append("terminal artifact inventory is not a list")

    if release is not None:
        if release.get("protocol") != PROTOCOL or release.get("event") != "released":
            errors.append("terminal release protocol/event mismatch")
        if release.get("nonce") != provenance.get("lease_nonce"):
            errors.append("terminal release nonce differs from provenance")
        if release.get("terminal") != expected_outcome:
            errors.append("terminal release outcome mismatch")
    pre_release_at = (
        parse_timestamp(
            pre_release.get("completed_at"), "terminal pre-release completed_at", errors
        )
        if pre_release is not None
        else None
    )
    release_at = (
        parse_timestamp(release.get("released_at"), "terminal released_at", errors)
        if release is not None
        else None
    )
    terminal_at = (
        parse_timestamp(
            terminal.get("terminal_published_at"), "terminal published_at", errors
        )
        if terminal is not None
        else None
    )
    chronology = [pre_release_at, release_at, terminal_at]
    if all(point is not None for point in chronology) and chronology != sorted(chronology):
        errors.append("terminal pre-release/release/publication chronology is invalid")

    if provenance.get("protocol") != PROTOCOL:
        errors.append("terminal provenance protocol mismatch")
    if provenance.get("evidence_mode") != "admission":
        errors.append("terminal provenance is not admission evidence")
    if provenance.get("paired_csv_path") != str((output_dir / "paired.csv").resolve()):
        errors.append("terminal provenance CSV path is not exact")
    claim = validate_pair_claim(provenance, errors)
    claim_path_value = (
        pre_release.get("pair_claim_path") if pre_release is not None else None
    )
    claim_path = (
        Path(claim_path_value).resolve()
        if isinstance(claim_path_value, str) and claim_path_value
        else Path(provenance.get("pair_claim_path", output_dir / "missing-claim")).resolve()
    )
    if pre_release is not None:
        if pre_release.get("pair_claim_path") != provenance.get("pair_claim_path"):
            errors.append("terminal pair claim path differs from provenance")
        if pre_release.get("pair_claim_sha256") != provenance.get("pair_claim_sha256"):
            errors.append("terminal pair claim hash differs from provenance")
    if claim is not None and claim.get("output_dir") != str(output_dir):
        errors.append("terminal pair claim output directory mismatch")
    if claim_path.is_file() and pre_release is not None:
        if pre_release.get("pair_claim_sha256") != sha256(claim_path):
            errors.append("terminal pair claim hash mismatch")

    if provenance.get("prepare_failure_absent") != "true":
        errors.append("terminal provenance does not prove preparation failure absence")
    prepare_failure_value = provenance.get("prepare_failure_path")
    if isinstance(prepare_failure_value, str):
        prepare_failure = Path(prepare_failure_value)
        if prepare_failure.exists() or prepare_failure.is_symlink():
            errors.append("preparation failure marker exists at terminal verification")

    if result is not None:
        if result.get("schema") != RESULT_SCHEMA:
            errors.append("terminal evaluation result schema mismatch")
        if result.get("protocol") != PROTOCOL:
            errors.append("terminal evaluation result protocol mismatch")
        if result.get("evidence_mode") != "admission":
            errors.append("terminal evaluation result is not admission evidence")
        if result.get("outcome") != expected_outcome:
            errors.append("terminal evaluation result outcome mismatch")
        if result.get("exit_code") != expected_exit:
            errors.append("terminal evaluation result exit mismatch")
        if result.get("valid_evidence") is not True:
            errors.append("terminal evaluation result is not valid evidence")
        if result.get("complete_matrix") is not True:
            errors.append("terminal evaluation result matrix is incomplete")
        if result.get("errors") != []:
            errors.append("terminal evaluation result contains errors")
        artifacts = result.get("artifacts")
        if not isinstance(artifacts, dict):
            errors.append("terminal evaluation result artifacts is not an object")
        else:
            result_provenance = artifacts.get("provenance")
            if not isinstance(result_provenance, dict):
                errors.append("terminal result provenance binding is missing")
            elif result_provenance != {
                "path": str(provenance_path),
                "sha256": sha256(provenance_path) if provenance_path.is_file() else None,
            }:
                errors.append("terminal result provenance binding mismatch")

    evaluator_started_at: datetime | None = None
    evaluator_started_ns: int | None = None
    if pre_release is not None:
        evaluator_started_at, evaluator_started_ns = validate_terminal_evaluator_child(
            pre_release.get("evaluator_child"),
            output_dir,
            provenance,
            expected_exit,
            pre_release_at,
            errors,
        )
    guard_path_value = provenance.get("guard_manifest_path")
    guard_path = (
        Path(guard_path_value).resolve()
        if isinstance(guard_path_value, str) and guard_path_value
        else output_dir / "missing-guard-manifest"
    )
    if guard_path != (output_dir / "guard_manifest.jsonl").resolve():
        errors.append("terminal guard manifest path is not exact")
    if guard_path.is_file():
        if provenance.get("guard_manifest_sha256") != sha256(guard_path):
            errors.append("terminal guard manifest hash mismatch")
        guards = read_jsonl(guard_path, "terminal guard manifest", errors)
        if len(guards) != MANIFESTS["guard_manifest"]:
            errors.append("terminal guard manifest record count mismatch")
        elif guards:
            final_guard = guards[-1]
            require_exact_keys(
                final_guard,
                {
                    "protocol",
                    "ordinal",
                    "label",
                    "publication",
                    "verdict",
                    "matches",
                    "preidentity_vanished",
                    "runner",
                    "active_child",
                    "forbidden_comm",
                    "started_at",
                    "completed_at",
                    "started_monotonic_ns",
                    "completed_monotonic_ns",
                    "path",
                    "sha256",
                },
                "terminal pre-evaluator guard",
                errors,
            )
            if final_guard.get("protocol") != PROTOCOL:
                errors.append("terminal pre-evaluator guard protocol mismatch")
            if final_guard.get("ordinal") != MANIFESTS["guard_manifest"]:
                errors.append("terminal pre-evaluator guard ordinal mismatch")
            if final_guard.get("label") != "pre-evaluator":
                errors.append("terminal final guard is not pre-evaluator")
            if final_guard.get("verdict") != "pass":
                errors.append("terminal pre-evaluator guard did not pass")
            if final_guard.get("publication") != "atomic":
                errors.append("terminal pre-evaluator guard was not atomically published")
            if final_guard.get("active_child") is not None:
                errors.append("terminal pre-evaluator guard has an active child")
            if final_guard.get("forbidden_comm") != sorted(FORBIDDEN_COMM):
                errors.append("terminal pre-evaluator forbidden process set changed")
            guard_runner = final_guard.get("runner")
            if not isinstance(guard_runner, dict):
                errors.append("terminal pre-evaluator runner is not an object")
            else:
                require_exact_keys(
                    guard_runner,
                    {"pid", "comm", "state", "ppid", "starttime_ticks"},
                    "terminal pre-evaluator runner",
                    errors,
                )
                if str(guard_runner.get("pid")) != provenance.get("lease_holder_pid"):
                    errors.append("terminal pre-evaluator runner pid differs from lease")
                if str(guard_runner.get("starttime_ticks")) != provenance.get(
                    "lease_holder_starttime"
                ):
                    errors.append(
                        "terminal pre-evaluator runner starttime differs from lease"
                    )
            snapshot_value = final_guard.get("path")
            snapshot_path = (
                Path(snapshot_value).resolve()
                if isinstance(snapshot_value, str) and snapshot_value
                else output_dir / "missing-pre-evaluator-guard"
            )
            expected_snapshot = (
                output_dir / "guards" / "161-pre-evaluator.json"
            ).resolve()
            if snapshot_path != expected_snapshot:
                errors.append("terminal pre-evaluator snapshot path is not exact")
            snapshot = read_canonical_json(
                snapshot_path, "terminal pre-evaluator snapshot", errors
            )
            if snapshot_path.is_file() and final_guard.get("sha256") != sha256(
                snapshot_path
            ):
                errors.append("terminal pre-evaluator snapshot hash mismatch")
            if snapshot is not None:
                for field, expected_value in (
                    ("protocol", PROTOCOL),
                    ("label", "pre-evaluator"),
                    ("verdict", "pass"),
                    ("runner", guard_runner),
                    ("active_child", None),
                    ("forbidden_comm", sorted(FORBIDDEN_COMM)),
                    ("entries", final_guard.get("matches")),
                    ("started_at", final_guard.get("started_at")),
                    ("completed_at", final_guard.get("completed_at")),
                    (
                        "started_monotonic_ns",
                        final_guard.get("started_monotonic_ns"),
                    ),
                    (
                        "completed_monotonic_ns",
                        final_guard.get("completed_monotonic_ns"),
                    ),
                ):
                    if snapshot.get(field) != expected_value:
                        errors.append(
                            f"terminal pre-evaluator snapshot {field} mismatch"
                        )
                vanished = snapshot.get("preidentity_vanished")
                if not isinstance(vanished, list) or final_guard.get(
                    "preidentity_vanished"
                ) != len(vanished):
                    errors.append(
                        "terminal pre-evaluator vanished-process count mismatch"
                    )
            guard_completed_at = parse_timestamp(
                final_guard.get("completed_at"),
                "terminal pre-evaluator guard completed_at",
                errors,
            )
            guard_completed_ns = record_integer(
                final_guard,
                "completed_monotonic_ns",
                "terminal pre-evaluator guard",
                errors,
                positive=True,
            )
            if (
                guard_completed_at is not None
                and evaluator_started_at is not None
                and evaluator_started_at < guard_completed_at
            ):
                errors.append("evaluator started before pre-evaluator guard completed")
            if (
                guard_completed_ns is not None
                and evaluator_started_ns is not None
                and evaluator_started_ns < guard_completed_ns
            ):
                errors.append(
                    "evaluator monotonic start precedes pre-evaluator guard completion"
                )

    def digest_or_empty(path: Path) -> str:
        try:
            return sha256(path)
        except OSError:
            return ""

    verification = {
        "schema": TERMINAL_VERIFICATION_SCHEMA,
        "protocol": PROTOCOL,
        "outcome": (
            "TERMINAL_VERIFIED" if not errors else "TERMINAL_VERIFICATION_FAILED"
        ),
        "verified_at": timestamp(),
        "output_dir": str(output_dir),
        "terminal_path": str(terminal_path),
        "terminal_sha256": digest_or_empty(terminal_path),
        "release_path": str(release_path),
        "release_sha256": digest_or_empty(release_path),
        "pre_release_path": str(pre_release_path),
        "pre_release_sha256": digest_or_empty(pre_release_path),
        "result_path": str(result_path),
        "result_sha256": digest_or_empty(result_path),
        "provenance_path": str(provenance_path),
        "provenance_sha256": digest_or_empty(provenance_path),
        "pair_claim_path": str(claim_path),
        "pair_claim_sha256": digest_or_empty(claim_path),
        "errors": errors,
    }
    return verification, EXIT_ADOPT if not errors else EXIT_INTERNAL


def admission_global_name_errors() -> list[str]:
    """Detect unresolved globals in every live admission helper."""

    functions: tuple[FunctionType, ...] = (
        command_output,
        stable_cpu_topology_sha256,
        current_governors,
        observe_admission_environment,
        validate_lease_held,
        validate_source_approval,
        validate_source_graph,
        validate_build_attestation,
        validate_prepared_pair,
        validate_pair_claim,
        validate_admission_provenance,
        validate_terminal_evaluator_child,
        verify_terminal,
    )
    errors = []
    for function in functions:
        for instruction in dis.get_instructions(function):
            if instruction.opname != "LOAD_GLOBAL":
                continue
            name = instruction.argval
            if not isinstance(name, str):
                continue
            if name not in globals() and not hasattr(builtins, name):
                errors.append(f"{function.__name__} has undefined global {name}")
    return sorted(set(errors))


def synthetic_rows() -> list[dict[str, str]]:
    rows = []
    for mode, batch, cycle, slot, variant in physical_order():
        bpw = PROCESS_BPW[batch]
        appends = WRITERS * bpw
        events = batch * appends
        candidate = variant == "candidate"
        allocs = events * (8 if candidate else 10)
        alloc_bytes = events * (80 if candidate else 100)
        copied_records = 0 if candidate or batch > 10 else events
        row = {
            "variant": variant,
            "source": "2" * 40 if candidate else "1" * 40,
            "binary_sha256": "b" * 64 if candidate else "a" * 64,
            "cycle": str(cycle),
            "slot": str(slot),
            "mode": mode,
            "batch": str(batch),
            "writers": str(WRITERS),
            "bpw": str(bpw),
            "payload": str(PAYLOAD),
            "events": str(events),
            "ev_s": "120" if candidate else "100",
            "p50_us": "45" if candidate else "50",
            "p99_us": "90" if candidate else "100",
            "allocs": str(allocs),
            "alloc_bytes": str(alloc_bytes),
            "allocs_per_event": f"{allocs / events:.4f}",
            "alloc_bytes_per_event": f"{alloc_bytes / events:.2f}",
            "owned_batches": str(appends if candidate else 0),
            "owned_records": str(events if candidate else 0),
            "owned_payload_bytes": str(events * PAYLOAD if candidate else 0),
            "borrowed_batches": str(0 if candidate else appends),
            "borrowed_records": str(0 if candidate else events),
            "copied_records": str(copied_records),
            "copied_bytes": str(copied_records * 261),
            "batches": str(appends),
            "groups": "0",
            "fsyncs": "0",
            "fsync_p99_ns": "0",
            "fsync_degraded": "false",
            "pre_load1": "0.1",
            "post_load1": "0.1",
        }
        rows.append(row)
    return rows


def self_test() -> dict[str, Any]:
    errors: list[str] = []
    errors.extend(admission_global_name_errors())
    try:
        with tempfile.TemporaryDirectory(
            prefix="bn-22it-source-manifest-"
        ) as root:
            manifest_root = Path(root)
            for variant in VARIANTS:
                for suffix, bound_root in (
                    ("tree", None),
                    ("materialized", manifest_root),
                ):
                    source = "1" * 40 if variant == "control" else "2" * 40
                    tree = "3" * 40 if variant == "control" else "4" * 40
                    manifest = {
                        "schema": SOURCE_MANIFEST_SCHEMA,
                        "protocol": PROTOCOL,
                        "variant": variant,
                        "source_commit": source,
                        "source_tree": tree,
                        "root": (
                            str(bound_root) if bound_root is not None else None
                        ),
                        "entries": [],
                    }
                    path = manifest_root / f"{variant}-{suffix}.json"
                    path.write_bytes(canonical_json_bytes(manifest))
                    manifest_errors: list[str] = []
                    observed = validate_source_manifest(
                        path,
                        sha256(path),
                        variant=variant,
                        source=source,
                        tree=tree,
                        root=bound_root,
                        label=f"self-test {variant} {suffix} manifest",
                        errors=manifest_errors,
                    )
                    if observed != manifest or manifest_errors:
                        errors.append(
                            f"resolved {variant} {suffix} manifest failed "
                            f"validation: {manifest_errors}"
                        )
            boundary_digest = hashlib.sha256(b"").hexdigest()
            for invalid_path in (None, "", manifest_root):
                boundary_errors: list[str] = []
                validate_bound_file(
                    invalid_path,
                    boundary_digest,
                    "self-test external manifest",
                    boundary_errors,
                )
                if not any(
                    "path is not a nonempty string" in error
                    for error in boundary_errors
                ):
                    errors.append(
                        "external manifest boundary accepted a non-string or "
                        "empty path"
                    )
    except BaseException as error:
        errors.append(f"resolved source-manifest validation raised {error!r}")
    try:
        with tempfile.TemporaryDirectory(prefix="bn-22it-terminal-negative-") as root:
            terminal_result, terminal_rc = verify_terminal(Path(root))
    except BaseException as error:
        errors.append(f"empty terminal verification raised {error!r}")
    else:
        terminal_keys = {
            "schema",
            "protocol",
            "outcome",
            "verified_at",
            "output_dir",
            "terminal_path",
            "terminal_sha256",
            "release_path",
            "release_sha256",
            "pre_release_path",
            "pre_release_sha256",
            "result_path",
            "result_sha256",
            "provenance_path",
            "provenance_sha256",
            "pair_claim_path",
            "pair_claim_sha256",
            "errors",
        }
        if set(terminal_result) != terminal_keys:
            errors.append("terminal verification schema keys changed")
        if (
            terminal_result.get("schema") != TERMINAL_VERIFICATION_SCHEMA
            or terminal_result.get("protocol") != PROTOCOL
            or terminal_result.get("outcome") != "TERMINAL_VERIFICATION_FAILED"
            or not terminal_result.get("errors")
            or terminal_rc != EXIT_INTERNAL
        ):
            errors.append("empty terminal chain did not fail closed")
    try:
        observed_environment = observe_admission_environment(
            Path(__file__).resolve().parent
        )
    except BaseException as error:
        errors.append(f"live admission environment helpers raised {error!r}")
    else:
        expected_environment = {
            "filesystem_source",
            "filesystem_type",
            "filesystem_target",
            "filesystem_free_bytes",
            "filesystem_total_bytes",
            "cpu_count",
            "cpu_online",
            "cpu_topology_sha256",
            "page_size",
            "governors",
        }
        if set(observed_environment) != expected_environment:
            errors.append("live admission environment observation is incomplete")
        if not SHA256.fullmatch(
            observed_environment.get("cpu_topology_sha256", "")
        ):
            errors.append("live CPU topology observation is not a SHA-256")

    rows = synthetic_rows()
    row_errors: list[str] = []
    cells_from_rows = validate_rows(
        rows,
        {"evidence_mode": "fixture"},
        row_errors,
    )
    if row_errors:
        errors.append(f"exact synthetic rows failed validation: {row_errors}")
    else:
        try:
            _, material, failures = evaluate_gates(cells_from_rows)
        except BaseException as error:
            errors.append(f"exact synthetic row gates raised {error!r}")
        else:
            if failures or not material.get("passed"):
                errors.append("exact synthetic rows did not pass all gates")
    corrupt_rows = [dict(row) for row in rows]
    corrupt_rows[0]["allocs"] = str(
        int(corrupt_rows[0]["allocs"]) + int(corrupt_rows[0]["events"])
    )
    corrupt_errors: list[str] = []
    validate_rows(
        corrupt_rows,
        {"evidence_mode": "fixture"},
        corrupt_errors,
    )
    if not any("raw-derived" in error for error in corrupt_errors):
        errors.append("raw allocation corruption was not rejected")
    corrupt_rows = [dict(row) for row in rows]
    corrupt_rows[0]["groups"] = "1"
    corrupt_errors = []
    validate_rows(
        corrupt_rows,
        {"evidence_mode": "fixture"},
        corrupt_errors,
    )
    if not any("Process groups" in error for error in corrupt_errors):
        errors.append("Process group corruption was not rejected")

    order = physical_order()
    if len(order) != 80:
        errors.append(f"physical order has {len(order)} rows")
    if order[:4] != [
        ("process", 1, 1, 1, "control"),
        ("process", 1, 1, 2, "candidate"),
        ("process", 1, 1, 3, "candidate"),
        ("process", 1, 1, 4, "control"),
    ]:
        errors.append("first cycle is not ABBA")
    if order[4:8] != [
        ("process", 1, 2, 1, "candidate"),
        ("process", 1, 2, 2, "control"),
        ("process", 1, 2, 3, "control"),
        ("process", 1, 2, 4, "candidate"),
    ]:
        errors.append("second cycle is not BAAB")
    if thresholds() != {
        "cell_throughput_floor": 0.97,
        "cell_p99_ceiling": 1.10,
        "cell_allocation_calls_ceiling": 1.05,
        "cell_allocation_bytes_ceiling": 1.05,
        "material_throughput_floor": 1.10,
        "material_allocation_calls_ceiling": 0.90,
        "material_allocation_bytes_ceiling": 0.90,
    }:
        errors.append("threshold constants changed")

    cells: dict[
        tuple[str, int], dict[int, dict[str, list[dict[str, Any]]]]
    ] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for batch in BATCHES:
        for cycle in CYCLES:
            for variant in VARIANTS:
                ratios = (
                    {
                        "ev_s": 100.0,
                        "p99_us": 100.0,
                        "allocs_per_event": 10.0,
                        "alloc_bytes_per_event": 100.0,
                    }
                    if variant == "control"
                    else {
                        "ev_s": 120.0,
                        "p99_us": 80.0,
                        "allocs_per_event": 5.0,
                        "alloc_bytes_per_event": 80.0,
                    }
                )
                cells[("process", batch)][cycle][variant] = [
                    {"_floats": dict(ratios)},
                    {"_floats": dict(ratios)},
                ]
    try:
        cell_results, material, gate_failures = evaluate_gates(cells)
    except BaseException as error:
        errors.append(f"passing synthetic gate raised {error!r}")
    else:
        if len(cell_results) != 4 or gate_failures or not material.get("passed"):
            errors.append("passing synthetic matrix did not pass all gates")

    for cycle in CYCLES:
        cells[("process", 1)][cycle]["candidate"][0]["_floats"]["ev_s"] = 1.0
        cells[("process", 1)][cycle]["candidate"][1]["_floats"]["ev_s"] = 1.0
    try:
        _, _, gate_failures = evaluate_gates(cells)
    except BaseException as error:
        errors.append(f"declining synthetic gate raised {error!r}")
    else:
        if "gate failure: process/b1" not in gate_failures:
            errors.append("declining synthetic matrix did not fail process/b1")
    result = make_base_result("self-test")
    result.update(
        {
            "outcome": "SELF_TEST_PASS" if not errors else "SELF_TEST_FAILED",
            "valid_evidence": False,
            "complete_matrix": False,
            "errors": errors,
            "gate_failures": [],
            "cells": [],
            "material": {},
            "exit_code": EXIT_ADOPT if not errors else EXIT_INTERNAL,
        }
    )
    return result


def usage() -> int:
    print(
        "usage:\n"
        "  evaluate.py <paired.csv> <provenance.txt> <result.json>\n"
        "  evaluate.py --fixture <paired.csv> <provenance.txt> <result.json>\n"
        "  evaluate.py --verify-terminal <absolute-output-dir> "
        "<absolute-terminal-verification.json>\n"
        "  evaluate.py --self-test <result.json>",
        file=sys.stderr,
    )
    return EXIT_USAGE


def main() -> int:
    if len(sys.argv) == 3 and sys.argv[1] == "--self-test":
        result_path = Path(sys.argv[2]).resolve()
        result = self_test()
        try:
            atomic_write_json(result_path, result)
        except BaseException as error:
            print(f"cannot write self-test result: {error}", file=sys.stderr)
            return EXIT_INTERNAL
        print(result["outcome"])
        return int(result["exit_code"])

    if len(sys.argv) == 4 and sys.argv[1] == "--verify-terminal":
        raw_output = Path(sys.argv[2])
        raw_verification = Path(sys.argv[3])
        if not raw_output.is_absolute() or not raw_verification.is_absolute():
            print("terminal verification paths must be absolute", file=sys.stderr)
            return EXIT_USAGE
        output_dir = raw_output.resolve()
        verification_path = raw_verification.resolve()
        if verification_path.parent != output_dir:
            print(
                "terminal verification artifact must be inside the output directory",
                file=sys.stderr,
            )
            return EXIT_USAGE
        try:
            verification, exit_code = verify_terminal(output_dir)
            atomic_write_json(verification_path, verification)
        except BaseException as error:
            print(f"cannot publish terminal verification: {error}", file=sys.stderr)
            return EXIT_INTERNAL
        print(verification["outcome"])
        for error in verification["errors"]:
            print(f"- {error}", file=sys.stderr)
        return exit_code

    evidence_mode = "admission"
    arguments = sys.argv[1:]
    if arguments[:1] == ["--fixture"]:
        evidence_mode = "fixture"
        arguments = arguments[1:]
    if len(arguments) != 3:
        return usage()
    csv_path = Path(arguments[0]).resolve()
    provenance_path = Path(arguments[1]).resolve()
    result_path = Path(arguments[2]).resolve()

    try:
        result, exit_code = evaluate(csv_path, provenance_path, evidence_mode)
    except BaseException as error:
        result = make_base_result(evidence_mode)
        result.update(
            {
                "outcome": "EVALUATOR_ERROR",
                "exit_code": EXIT_INTERNAL,
                "valid_evidence": False,
                "complete_matrix": False,
                "errors": [f"{type(error).__name__}: {error}"],
                "gate_failures": [],
                "cells": [],
                "material": {},
            }
        )
        exit_code = EXIT_INTERNAL

    try:
        atomic_write_json(result_path, result)
    except BaseException as error:
        print(f"cannot atomically write result {result_path}: {error}", file=sys.stderr)
        return EXIT_INTERNAL

    print(result["outcome"])
    for error in result.get("errors", []):
        print(f"- {error}", file=sys.stderr)
    for failure in result.get("gate_failures", []):
        print(f"- {failure}", file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
