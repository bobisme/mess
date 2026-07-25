#!/usr/bin/env python3
"""Strict, neutral metric adapters for the bn-2l3n rebaseline.

This module does not launch a benchmark or make an admission decision.  It
turns kernel/tool observations into identity-bound, JSON-ready counters for the
reviewed runner and evaluator.  Every parser is fail-closed: missing fields,
duplicate counters, TID reuse, counter rollback, and ambiguous role bindings
are evidence errors rather than ``not_available`` values.
"""

from __future__ import annotations

import ast
import hashlib
import json
import math
import os
import re
import stat
import threading
import time
from collections import Counter
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Mapping, Sequence


PROTOCOL = "bn-2l3n-asterism-rebaseline-v3"
PROTOCOL_SHA256 = "d9ee10b2cccdaf6428bf1419a8c2ee74d272e987dc3617a80b64ad2e9d7a18dd"
PROFILE_SCHEMA = "bn-2l3n-profile-adapters-v3"
PREFLIGHT_SCHEMA = "bn-2l3n-profile-preflight-v3"
AUTHORITY_SCHEMA = "bn-2l3n-profile-authority-v3"
C_ROLE_LIFETIME_CONTRACT = {
    "schema": "bn-2l3n-c-role-lifetime-v3",
    "blocking_thread_keep_alive_ns": 3_600_000_000_000,
    "maximum_profile_child_timeout_ns": 120_000_000_000,
    "ready_to_measured_spawn_blocking_sites": 1,
    "ready_to_measured_other_thread_birth_sites": 0,
}
SCHEDSTAT_DECISION_MULTIPLIER = 20
PERF_EVENTS = ("cycles", "instructions", "task-clock", "context-switches")
PERF_EVENT_SPECS = tuple(f"{event}:u" for event in PERF_EVENTS)
VARIANT_SOURCE_BINDINGS = {
    "A": {
        "commit": "d644dc583dfe6a3d2cd07e71ce0212a323875ab4",
        "tree": "205d853905bdb648ee997900c6aef24a323aa380",
    },
    "B": {
        "commit": "d644dc583dfe6a3d2cd07e71ce0212a323875ab4",
        "tree": "205d853905bdb648ee997900c6aef24a323aa380",
    },
    "C": {
        "commit": "f0ab89e92e44253f8fe48cf19d7a93e39263585b",
        "tree": "7ca2228fcd1c65ef942da35144fcf507d8a72e12",
    },
    "D": {
        "commit": "69b95604b9e7c924314cf7d82b86a2edb7dccde6",
        "tree": "f738cde2b5414d1a2926ac86573163d505754ba9",
    },
}
# Linux exposes only 15 comm bytes. These are the exact reviewed helper names
# that may be born while the historical public engine opens.
OPEN_HELPER_COMMS = frozenset(("fjall:worker", "mess-sealer", "mess-engine-rol"))
TOKIO_WORKER_COMM = "tokio-rt-worker"
SYSCALL_EVENTS = (
    "write",
    "pwrite64",
    "writev",
    "pwritev",
    "pwritev2",
    "fsync",
    "fdatasync",
    "futex",
    "open",
    "openat",
    "openat2",
    "creat",
    "rename",
    "renameat",
    "renameat2",
    "unlink",
    "unlinkat",
    "mkdir",
    "mkdirat",
    "rmdir",
    "getdents64",
    "read",
    "pread64",
    # The ready/measured trace boundary markers are UnixStream writes, which
    # Rust std lowers to sendto(.., MSG_NOSIGNAL, ..) on Linux.  Approve sendto
    # so the markers are recognised; no domain sendto occurs inside the interval.
    "sendto",
)


class ProfileEvidenceError(ValueError):
    """An observation cannot enter evidence without ambiguity."""


class ProcPathVanished(ProfileEvidenceError):
    """A /proc path disappeared mid-read because its task/process exited.

    A subclass of ProfileEvidenceError so existing catchers keep their
    behaviour; only the task-set enumeration distinguishes it, to tolerate a
    tokio worker that the runtime reaps between listing /proc/<pid>/task and
    reading an individual thread.
    """


@dataclass(frozen=True, order=True)
class TaskIdentity:
    pid: int
    tid: int
    start_ticks: int
    comm: str

    def to_json(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class TaskCounters:
    identity: TaskIdentity
    on_cpu_ns: int
    voluntary_context_switches: int
    nonvoluntary_context_switches: int

    def to_json(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class TaskDelta:
    identity: TaskIdentity
    on_cpu_ns: int
    voluntary_context_switches: int
    nonvoluntary_context_switches: int

    def to_json(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class ProcessIo:
    rchar: int
    wchar: int
    syscr: int
    syscw: int
    read_bytes: int
    write_bytes: int
    cancelled_write_bytes: int


@dataclass(frozen=True)
class ProcessCounters:
    pid: int
    start_ticks: int
    vm_hwm_bytes: int
    voluntary_context_switches: int
    nonvoluntary_context_switches: int
    io: ProcessIo

    def to_json(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class ProcessDelta:
    pid: int
    start_ticks: int
    vm_hwm_bytes: int
    voluntary_context_switches: int
    nonvoluntary_context_switches: int
    io: ProcessIo

    def to_json(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class RusageCounters:
    user_ns: int
    system_ns: int

    def to_json(self) -> dict[str, int]:
        return asdict(self)


@dataclass(frozen=True)
class AllocationCounters:
    calls: int
    bytes: int

    def to_json(self) -> dict[str, int]:
        return asdict(self)


@dataclass(frozen=True)
class SchedstatResolution:
    samples_ns: tuple[int, ...]
    minimum_nonzero_increment_ns: int

    def to_json(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class PerfCounter:
    event: str
    status: str
    value: int | str | None
    unit: str
    runtime_ns: int | None
    running_percent: str | None

    def to_json(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class SyscallCount:
    syscall: str
    calls: int
    errors: int

    def to_json(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class PhaseSnapshot:
    phase: str
    pid: int
    process_start_ticks: int
    tasks: tuple[TaskIdentity, ...]

    def to_json(self) -> dict[str, object]:
        return {
            "phase": self.phase,
            "pid": self.pid,
            "process_start_ticks": self.process_start_ticks,
            "tasks": [task.to_json() for task in self.tasks],
        }


@dataclass(frozen=True)
class RoleBinding:
    label: str
    tasks: tuple[TaskIdentity, ...]
    born_in_window: bool = False

    def to_json(self) -> dict[str, object]:
        return {
            "label": self.label,
            "born_in_window": self.born_in_window,
            "tasks": [task.to_json() for task in self.tasks],
        }


@dataclass(frozen=True)
class ProfileWindowStart:
    variant: str
    track: str
    ready: PhaseSnapshot
    process: ProcessCounters
    roles: tuple[RoleBinding, ...]
    task_counters: tuple[TaskCounters, ...]

    def to_json(self) -> dict[str, object]:
        return {
            "variant": self.variant,
            "track": self.track,
            "ready": self.ready.to_json(),
            "process": self.process.to_json(),
            "roles": [role.to_json() for role in self.roles],
            "task_counters": [counters.to_json() for counters in self.task_counters],
        }


@dataclass(frozen=True)
class RoleDelta:
    label: str
    born_in_window: bool
    tasks: tuple[TaskDelta, ...]

    def to_json(self) -> dict[str, object]:
        return {
            "label": self.label,
            "born_in_window": self.born_in_window,
            "on_cpu_ns": sum(task.on_cpu_ns for task in self.tasks),
            "voluntary_context_switches": sum(
                task.voluntary_context_switches for task in self.tasks
            ),
            "nonvoluntary_context_switches": sum(
                task.nonvoluntary_context_switches for task in self.tasks
            ),
            "tasks": [task.to_json() for task in self.tasks],
        }


@dataclass(frozen=True)
class ProfileWindowResult:
    schema: str
    protocol: str
    authority: dict[str, object]
    variant: str
    track: str
    context: dict[str, object]
    process: ProcessDelta
    roles: tuple[RoleDelta, ...]
    phase_snapshots: tuple[PhaseSnapshot, ...]
    unattributed_births: tuple[TaskIdentity, ...]

    def to_json(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "protocol": self.protocol,
            "authority": self.authority,
            "variant": self.variant,
            "track": self.track,
            "context": self.context,
            "process": self.process.to_json(),
            "roles": [role.to_json() for role in self.roles],
            "phase_snapshots": [phase.to_json() for phase in self.phase_snapshots],
            "unattributed_births": [task.to_json() for task in self.unattributed_births],
        }


def canonical_json(value: object) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode()


def _local_canonical_json(value: object) -> bytes:
    """Return the ASCII-canonical form used by builder-local evidence."""

    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("ascii")


def profile_contract() -> dict[str, object]:
    """Return the frozen JSON-ready adapter contract consumed by tooling."""

    return {
        "schema": PROFILE_SCHEMA,
        "protocol": PROTOCOL,
        "protocol_sha256": PROTOCOL_SHA256,
        "authority_schema": AUTHORITY_SCHEMA,
        "c_role_lifetime_contract": C_ROLE_LIFETIME_CONTRACT,
        "variant_source_bindings": VARIANT_SOURCE_BINDINGS,
        "schedstat_decision_multiplier": SCHEDSTAT_DECISION_MULTIPLIER,
        "perf_events": list(PERF_EVENT_SPECS),
        "perf_event_scope_policy": "exact-user-only",
        "syscall_events": list(SYSCALL_EVENTS),
        "process_cpu_source": "getrusage(RUSAGE_SELF)",
        "allocation_source": "counting-global-allocator:snapshot-delta",
        "role_cpu_source": "/proc/<pid>/task/<tid>/schedstat:first-field",
        "context_switch_source": "/proc/<pid>/task/<tid>/status",
        "process_io_source": "/proc/<pid>/io",
        "peak_rss_source": "/proc/<pid>/status:VmHWM",
        "hardware_profile_timing_authority": False,
        "trace_profile_timing_authority": False,
        "trace_path_marker_schema": {
            "fields": ["kind", "path"],
            "kinds": ["exact", "file_prefix", "directory_prefix"],
            "path_authority": "canonical-absolute",
            "empty_family_allowed": True,
            "combined_empty_allowed": False,
            "overlap_allowed": False,
        },
        "profile_inputs_persistence": {
            "payload": "child.profile_tool_inputs",
            "sha256": "child.profile_tool_inputs_sha256",
            "raw_artifacts": "one-fd-nofollow-0444-sha256-and-byte-length",
        },
        "perf_disable_owner": "child-at-t1-before-measured-serialization",
        "perf_ack_ledger": {
            "ownership": "one-shared-offset",
            "artifact_mode": 0o444,
            "exact_bytes_utf8": "ack\nack\n",
            "exact_bytes": len(b"ack\nack\n"),
            "sha256": _sha256_bytes(b"ack\nack\n"),
        },
        "perf_child_environment": {
            "cpu_all": ["ASTERISM_REBASELINE_PERF_PERMISSION_RESULT"],
            "cpu_available_only": [
                "ASTERISM_REBASELINE_PERF_COMMAND_FD",
                "ASTERISM_REBASELINE_PERF_ACK_FD",
                "ASTERISM_REBASELINE_PERF_ACK_LEDGER_FD",
            ],
            "non_cpu": [],
        },
        "c_role_lifetime_proof": [
            "source-approval-static-proof",
            "prepared-binary-contract",
            "runner-child-timeout-cap",
        ],
        "control_phases": [
            "boot",
            "runtime",
            "opened",
            "ready",
            "start",
            "measured",
            "release",
        ],
        "role_rules": {
            "A": ["owner:comm=mess-flat-owner"],
            "B": ["committer:runtime-to-opened-unique-unnamed-birth"],
            "C": [
                "committer:runtime-to-opened-unique-unnamed-birth",
                f"producer-runtime:boot-to-runtime-births:comm={TOKIO_WORKER_COMM}",
                f"spawn_blocking-publication:ready-to-measured-births:comm={TOKIO_WORKER_COMM}",
            ],
            "D": ["owner:comm=mess-flat-owner"],
        },
        "open_helper_comms": sorted(OPEN_HELPER_COMMS),
        "profile_field_inputs": {
            "primary,new_names,fairness": [
                "schedstat_resolution_ns",
            ],
            "reopen": [],
            "cpu_profiles": [
                "schedstat_resolution_ns",
                "perf_permission",
                "perf_control_events",
                "perf_raw_artifacts",
            ],
            "syscall_profiles": [
                "trace_raw_artifact",
                "log_path_markers",
                "metadata_path_markers",
            ],
            "structural_traces": [
                "trace_raw_artifact",
                "log_path_markers",
                "metadata_path_markers",
            ],
        },
    }


def _nonnegative_integer(value: str, context: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise ProfileEvidenceError(f"{context} is not an integer: {value!r}") from error
    if parsed < 0:
        raise ProfileEvidenceError(f"{context} is negative: {parsed}")
    return parsed


def _integer(value: str, context: str) -> int:
    try:
        return int(value)
    except ValueError as error:
        raise ProfileEvidenceError(f"{context} is not an integer: {value!r}") from error


def parse_proc_stat(payload: str, expected_id: int | None = None) -> tuple[int, str, int]:
    """Parse pid/comm/starttime without splitting a parenthesized comm."""

    line = payload.strip()
    left = line.find("(")
    right = line.rfind(")")
    if left <= 0 or right <= left or not line[right + 1 :].startswith(" "):
        raise ProfileEvidenceError("malformed /proc stat record")
    pid = _nonnegative_integer(line[:left].strip(), "stat pid")
    if pid == 0 or (expected_id is not None and pid != expected_id):
        raise ProfileEvidenceError(f"stat identity mismatch: expected {expected_id}, got {pid}")
    comm = line[left + 1 : right]
    if not comm or "\n" in comm or "\x00" in comm:
        raise ProfileEvidenceError("invalid empty/control comm in /proc stat")
    fields = line[right + 2 :].split()
    # fields[0] is Linux stat field 3 (state); starttime is field 22.
    if len(fields) < 20:
        raise ProfileEvidenceError("short /proc stat record")
    start_ticks = _nonnegative_integer(fields[19], "stat starttime")
    if start_ticks == 0:
        raise ProfileEvidenceError("zero /proc stat starttime")
    return pid, comm, start_ticks


def parse_status(payload: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in payload.splitlines():
        if not raw or ":" not in raw:
            continue
        key, value = raw.split(":", 1)
        if key in values:
            raise ProfileEvidenceError(f"duplicate status field {key}")
        values[key] = value.strip()
    return values


def parse_kib_field(value: str, context: str) -> int:
    parts = value.split()
    if len(parts) != 2 or parts[1] != "kB":
        raise ProfileEvidenceError(f"{context} must use kB: {value!r}")
    kib = _nonnegative_integer(parts[0], context)
    return kib * 1024


def parse_schedstat(payload: str) -> int:
    fields = payload.split()
    if len(fields) < 3:
        raise ProfileEvidenceError("short schedstat record")
    return _nonnegative_integer(fields[0], "schedstat on_cpu_ns")


IO_FIELDS = (
    "rchar",
    "wchar",
    "syscr",
    "syscw",
    "read_bytes",
    "write_bytes",
    "cancelled_write_bytes",
)


def parse_proc_io(payload: str) -> ProcessIo:
    values: dict[str, int] = {}
    for raw in payload.splitlines():
        if not raw:
            continue
        if ":" not in raw:
            raise ProfileEvidenceError(f"malformed /proc io line: {raw!r}")
        key, value = raw.split(":", 1)
        if key in values:
            raise ProfileEvidenceError(f"duplicate /proc io field {key}")
        values[key] = (
            _integer(value.strip(), f"io {key}")
            if key == "cancelled_write_bytes"
            else _nonnegative_integer(value.strip(), f"io {key}")
        )
    missing = set(IO_FIELDS) - set(values)
    extra = set(values) - set(IO_FIELDS)
    if missing or extra:
        raise ProfileEvidenceError(
            f"/proc io fields differ: missing={sorted(missing)} extra={sorted(extra)}"
        )
    return ProcessIo(**{name: values[name] for name in IO_FIELDS})


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CURRENT_CHILDREN_ATTESTATION_SCHEMA = "bn-ecm1-current-children-build-v2"
_SOURCE_REVIEW_CONTENT_SCHEMAS = {
    "bundle": "bn-3hch-source-review-bundle-v1",
    "current_children_attestation": _CURRENT_CHILDREN_ATTESTATION_SCHEMA,
    "lock_authority": "bn-31gp-current-lock-authority-v1",
    "lock_review_bundle": "bn-31gp-current-lock-review-bundle-v1",
}
_SOURCE_REVIEW_PATHS = {
    "bundle": "bindings/source-review-bundle.json",
    "current_children_attestation": "bindings/current-children-attestation.json",
    "lock_authority": "bindings/lock-review-authority.json",
    "lock_review_bundle": "bindings/lock-review-bundle.json",
}
_RELEASE_COMPILE_OUT_REQUIREMENT_FIELDS = (
    "schema",
    "status",
    "variant",
    "product_overlay_sha256",
    "preapproval_compile_out_sha256",
    "proof_must_bind_enclosing_approval_sha256",
    "repeat_under_real_source_approval",
    "same_contract_nonce_lock_toolchain_sandbox",
    "cfg_test",
    "rustc_workspace_wrapper",
    "ordinary_a_role",
    "overlay_a_role",
    "binary_byte_identical",
    "symbol_inventory_byte_identical",
    "forbidden_hook_strings",
    "forbidden_hook_strings_absent",
)
_RELEASE_COMPILE_OUT_FIELDS = (
    "schema",
    "protocol",
    "protocol_sha256",
    "status",
    "source_approval_sha256",
    "requirement_sha256",
    "current_children_attestation_sha256",
    "product_overlay_sha256",
    "equivalence_contract",
    "builds",
    "binaries",
    "nm",
    "symbol_inventories",
    "forbidden_hook_strings",
    "binary_byte_identical",
    "symbol_inventory_byte_identical",
    "forbidden_hook_strings_absent",
    "published_a_sha256",
)
_RELEASE_COMPILE_OUT_EQUIVALENCE_FIELDS = (
    "source_approval_sha256",
    "contract_sha256",
    "build_nonce",
    "cargo_lock_sha256",
    "toolchain_sha256",
    "build_environment_sha256",
    "sandbox_sha256",
    "cfg_test",
    "rustc_workspace_wrapper",
    "ordinary_a_role",
    "overlay_a_role",
)
_RELEASE_COMPILE_OUT_BUILD_FIELDS = (
    "role",
    "artifact_role",
    "source_approval_sha256",
    "contract_sha256",
    "build_nonce",
    "cargo_lock_sha256",
    "toolchain_sha256",
    "build_environment_sha256",
    "sandbox_sha256",
    "cfg_test",
    "rustc_workspace_wrapper",
    "attestation",
    "attestation_sha256",
)
_RELEASE_COMPILE_OUT_FILE_FIELDS = (
    "path",
    "sha256",
    "size",
    "mode",
    "identity",
)
_RELEASE_COMPILE_OUT_IDENTITY_FIELDS = (
    "changed_ns",
    "device",
    "inode",
    "link_count",
    "modified_ns",
)
_RELEASE_COMPILE_OUT_NM_CHILD_FIELDS = (
    "argv",
    "completed_at",
    "completed_monotonic_ns",
    "cwd",
    "exit_status",
    "output_path",
    "output_sha256",
    "pid",
    "process_group_absent",
    "reaping",
    "start_ticks",
    "started_at",
    "started_monotonic_ns",
    "timed_out",
    "waited_pid",
)
_FORBIDDEN_RELEASE_HOOK_STRINGS = (
    "TestEngineHook",
    "TestEngineHooks",
    "TestEngineFs",
    "arm_test_hook",
    "arm_test_owner_cohort",
    "asterism_rebaseline_correctness",
)
_CURRENT_PRODUCT_OVERLAY_SHA256 = (
    "dd36dee2b53831eb0274b4dac0252d154caae1a24991e167faeb2bf3682b26cd"
)
_AUTHORITY_FIELDS = frozenset(
    {
        "schema",
        "protocol",
        "protocol_sha256",
        "attempt_nonce",
        "child_ordinal",
        "row_ordinal",
        "context_sha256",
        "prepared_artifacts_path",
        "prepared_artifacts_sha256",
        "source_approval_path",
        "source_approval_sha256",
        "profile_adapter_path",
        "profile_adapter_sha256",
        "profile_tools",
        "perf_permission_result",
        "variant",
        "source_commit",
        "source_tree",
        "track",
        "executable_path",
        "executable_sha256",
        "executable_mode",
        "executable_comm",
        "child_pid",
        "child_start_ticks",
        "control_fd",
    }
)


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _read_exact_fd(
    descriptor: int, context: str, limit: int | None = 16 * 1024 * 1024
) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        read_size = (
            1024 * 1024
            if limit is None
            else min(1024 * 1024, limit + 1 - total)
        )
        chunk = os.read(descriptor, read_size)
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)
        total += len(chunk)
        if limit is not None and total > limit:
            raise ProfileEvidenceError(f"{context} exceeds {limit} bytes")


def _immutable_file_payload(
    path_value: object,
    expected_mode: int | None,
    context: str,
    *,
    limit: int | None = 16 * 1024 * 1024,
) -> tuple[Path, bytes, tuple[int, int, int, int, int, int, int]]:
    if not isinstance(path_value, str):
        raise ProfileEvidenceError(f"{context} path is not text")
    path = Path(path_value)
    if (
        not path.is_absolute()
        or path_value.startswith("//")
        or ".." in path.parts
        or str(path) != path_value
    ):
        raise ProfileEvidenceError(f"{context} path is not canonical absolute")
    directory_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY
    file_flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        directory_flags |= os.O_NOFOLLOW
        file_flags |= os.O_NOFOLLOW
    parent = os.open("/", directory_flags)
    try:
        for part in path.parts[1:-1]:
            next_parent = os.open(part, directory_flags, dir_fd=parent)
            os.close(parent)
            parent = next_parent
        descriptor = os.open(path.name, file_flags, dir_fd=parent)
    except OSError as error:
        os.close(parent)
        raise ProfileEvidenceError(f"cannot open immutable {context}: {error}") from error
    os.close(parent)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or (
            expected_mode is not None and stat.S_IMODE(before.st_mode) != expected_mode
        ):
            expected = (
                f"a regular {expected_mode:#06o} file"
                if expected_mode is not None
                else "a regular file"
            )
            raise ProfileEvidenceError(f"{context} must be {expected}")
        payload = _read_exact_fd(descriptor, context, limit)
        after = os.fstat(descriptor)
        if (
            (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            or len(payload) != before.st_size
        ):
            raise ProfileEvidenceError(f"{context} changed during its one-fd snapshot")
    finally:
        os.close(descriptor)
    return path, payload, (
        after.st_dev,
        after.st_ino,
        after.st_nlink,
        after.st_size,
        stat.S_IMODE(after.st_mode),
        after.st_mtime_ns,
        after.st_ctime_ns,
    )


def _immutable_file_snapshot(
    path_value: object,
    claimed_sha256: object,
    expected_mode: int | None,
    context: str,
    *,
    limit: int | None = 16 * 1024 * 1024,
) -> tuple[Path, bytes, tuple[int, int, int, int, int, int, int]]:
    if not isinstance(claimed_sha256, str) or not _SHA256_RE.fullmatch(claimed_sha256):
        raise ProfileEvidenceError(f"{context} SHA-256 is malformed")
    path, payload, identity = _immutable_file_payload(
        path_value, expected_mode, context, limit=limit
    )
    if _sha256_bytes(payload) != claimed_sha256:
        raise ProfileEvidenceError(f"{context} SHA-256 differs")
    return path, payload, identity


def _canonical_json_payload(payload: bytes, context: str) -> dict[str, object]:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProfileEvidenceError(f"{context} is not canonical JSON: {error}") from error
    if not isinstance(value, dict) or canonical_json(value) != payload:
        raise ProfileEvidenceError(f"{context} is not one canonical JSON object")
    return value


def _local_canonical_json_payload(
    payload: bytes, context: str
) -> dict[str, object]:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProfileEvidenceError(
            f"{context} is not local canonical JSON: {error}"
        ) from error
    if not isinstance(value, dict) or _local_canonical_json(value) != payload:
        raise ProfileEvidenceError(
            f"{context} is not one local canonical JSON object"
        )
    return value


def _unclaimed_local_canonical_json_snapshot(
    path_value: object, context: str
) -> tuple[Path, bytes, dict[str, object]]:
    path, payload, _identity = _immutable_file_payload(path_value, 0o444, context)
    return path, payload, _local_canonical_json_payload(payload, context)


def _raw_artifact_snapshot(
    value: object, context: str, *, limit: int | None = 16 * 1024 * 1024
) -> tuple[Path, bytes]:
    binding = _exact_mapping(
        value,
        ("path", "sha256", "bytes", "mode"),
        f"{context} binding",
    )
    if binding["mode"] != 0o444:
        raise ProfileEvidenceError(f"{context} mode authority differs")
    path, payload, _identity = _immutable_file_snapshot(
        binding["path"], binding["sha256"], 0o444, context, limit=limit
    )
    if _json_nonnegative_integer(binding["bytes"], f"{context} bytes") != len(payload):
        raise ProfileEvidenceError(f"{context} byte length differs")
    return path, payload


class ProcReader:
    """Read race-checked counters through one nofollow-bound proc-root fd."""

    def __init__(self, root: Path = Path("/proc")) -> None:
        self.root = root
        flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        if not root.is_absolute() or ".." in root.parts:
            raise ProfileEvidenceError("proc root must be canonical absolute")
        descriptor = os.open("/", flags)
        try:
            for part in root.parts[1:]:
                next_descriptor = os.open(part, flags, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = next_descriptor
            self._root_fd = descriptor
        except OSError as error:
            os.close(descriptor)
            raise ProfileEvidenceError(f"cannot bind proc root {root}: {error}") from error

    def close(self) -> None:
        descriptor = getattr(self, "_root_fd", -1)
        if descriptor >= 0:
            os.close(descriptor)
            self._root_fd = -1

    def __del__(self) -> None:
        try:
            self.close()
        except OSError:
            pass

    @staticmethod
    def _validate_part(part: str) -> None:
        if not part or part in {".", ".."} or "/" in part or "\x00" in part:
            raise ProfileEvidenceError(f"invalid proc path component {part!r}")

    def _open_directory(self, *parts: str) -> int:
        descriptor = os.dup(self._root_fd)
        flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            for part in parts:
                self._validate_part(part)
                next_descriptor = os.open(part, flags, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = next_descriptor
            return descriptor
        except OSError as error:
            os.close(descriptor)
            if isinstance(error, (FileNotFoundError, ProcessLookupError)):
                raise ProcPathVanished(
                    f"proc path vanished mid-traversal {'/'.join(parts)}: {error}"
                ) from error
            raise ProfileEvidenceError(
                f"cannot traverse proc path {'/'.join(parts)}: {error}"
            ) from error

    def _read(self, *parts: str) -> str:
        if not parts:
            raise ProfileEvidenceError("empty proc read path")
        parent = self._open_directory(*parts[:-1])
        flags = os.O_RDONLY | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(parts[-1], flags, dir_fd=parent)
            before = os.fstat(descriptor)
            payload = _read_exact_fd(descriptor, f"proc {'/'.join(parts)}", 1024 * 1024)
            after = os.fstat(descriptor)
        except OSError as error:
            if isinstance(error, (FileNotFoundError, ProcessLookupError)):
                raise ProcPathVanished(
                    f"proc path vanished before read {'/'.join(parts)}: {error}"
                ) from error
            raise ProfileEvidenceError(
                f"cannot read proc path {'/'.join(parts)}: {error}"
            ) from error
        finally:
            if "descriptor" in locals():
                os.close(descriptor)
            os.close(parent)
        if (
            not stat.S_ISREG(before.st_mode)
            or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
        ):
            raise ProfileEvidenceError(f"proc file identity changed: {'/'.join(parts)}")
        try:
            return payload.decode()
        except UnicodeDecodeError as error:
            raise ProfileEvidenceError(
                f"proc file is not UTF-8: {'/'.join(parts)}"
            ) from error

    def process_identity(self, pid: int) -> tuple[int, str, int]:
        return parse_proc_stat(self._read(str(pid), "stat"), pid)

    def task_identity(self, pid: int, tid: int) -> TaskIdentity:
        observed, comm, start_ticks = parse_proc_stat(
            self._read(str(pid), "task", str(tid), "stat"), tid
        )
        return TaskIdentity(pid=pid, tid=observed, start_ticks=start_ticks, comm=comm)

    def tasks(self, pid: int, *, attempts: int = 8) -> tuple[TaskIdentity, ...]:
        # A live tokio runtime spawns and reaps worker threads continuously
        # (the fairness 64-writer cell on this host is the worst case), so a
        # thread listed in /proc/<pid>/task can exit before we stat it.  Retry
        # to obtain a clean pass where every listed thread resolved; if churn
        # persists past `attempts`, keep the survivors from the final pass
        # rather than fail-stop on a reaped worker -- the same scheduler reality
        # already tolerated for tokio births and VmHWM elsewhere.  A vanished
        # process itself (the whole task dir gone) still raises: that is the
        # caller's stability guard, not a benign per-thread reap.
        identities: tuple[TaskIdentity, ...] = ()
        for attempt in range(attempts):
            last_pass = attempt == attempts - 1
            directory = self._open_directory(str(pid), "task")
            try:
                entries = os.listdir(directory)
            except OSError as error:
                raise ProfileEvidenceError(
                    f"cannot enumerate task directory: {error}"
                ) from error
            finally:
                os.close(directory)
            if any(not item.isdigit() for item in entries):
                raise ProfileEvidenceError("task directory has a nonnumeric entry")
            tids = sorted(int(item) for item in entries)
            if not tids:
                raise ProfileEvidenceError(f"no task identities for pid {pid}")
            collected: list[TaskIdentity] = []
            churned = False
            for tid in tids:
                try:
                    collected.append(self.task_identity(pid, tid))
                except ProcPathVanished:
                    churned = True
                    if not last_pass:
                        break
                    # Final attempt: drop the reaped thread, keep the rest.
            if churned and not last_pass:
                continue
            identities = tuple(collected)
            break
        if not identities:
            raise ProfileEvidenceError(f"no task identities for pid {pid}")
        if len({identity.tid for identity in identities}) != len(identities):
            raise ProfileEvidenceError("duplicate task identity")
        return identities

    def task_counters(self, identity: TaskIdentity) -> TaskCounters:
        before = self.task_identity(identity.pid, identity.tid)
        if before != identity:
            raise ProfileEvidenceError(f"task identity changed before sampling: {identity}")
        on_cpu_ns = parse_schedstat(
            self._read(str(identity.pid), "task", str(identity.tid), "schedstat")
        )
        status = parse_status(
            self._read(str(identity.pid), "task", str(identity.tid), "status")
        )
        required = ("voluntary_ctxt_switches", "nonvoluntary_ctxt_switches")
        if any(name not in status for name in required):
            raise ProfileEvidenceError("task status lacks context-switch counters")
        after = self.task_identity(identity.pid, identity.tid)
        if after != identity:
            raise ProfileEvidenceError(f"task identity changed during sampling: {identity}")
        return TaskCounters(
            identity=identity,
            on_cpu_ns=on_cpu_ns,
            voluntary_context_switches=_nonnegative_integer(
                status["voluntary_ctxt_switches"], "voluntary context switches"
            ),
            nonvoluntary_context_switches=_nonnegative_integer(
                status["nonvoluntary_ctxt_switches"], "nonvoluntary context switches"
            ),
        )

    def process_counters(self, pid: int) -> ProcessCounters:
        before = self.process_identity(pid)
        status = parse_status(self._read(str(pid), "status"))
        required = (
            "VmHWM",
            "voluntary_ctxt_switches",
            "nonvoluntary_ctxt_switches",
        )
        if any(name not in status for name in required):
            raise ProfileEvidenceError("process status lacks RSS/context-switch counters")
        io = parse_proc_io(self._read(str(pid), "io"))
        after = self.process_identity(pid)
        if before != after:
            raise ProfileEvidenceError(f"process identity changed during sampling: pid {pid}")
        return ProcessCounters(
            pid=pid,
            start_ticks=before[2],
            vm_hwm_bytes=parse_kib_field(status["VmHWM"], "VmHWM"),
            voluntary_context_switches=_nonnegative_integer(
                status["voluntary_ctxt_switches"],
                "process voluntary context switches",
            ),
            nonvoluntary_context_switches=_nonnegative_integer(
                status["nonvoluntary_ctxt_switches"],
                "process nonvoluntary context switches",
            ),
            io=io,
        )

    def executable_binding(self, pid: int) -> tuple[str, str]:
        before = self.process_identity(pid)
        process = self._open_directory(str(pid))
        try:
            try:
                target = os.readlink("exe", dir_fd=process)
                descriptor = os.open("exe", os.O_RDONLY | os.O_CLOEXEC, dir_fd=process)
            except OSError as error:
                raise ProfileEvidenceError(f"cannot bind process executable: {error}") from error
            try:
                metadata = os.fstat(descriptor)
                if not stat.S_ISREG(metadata.st_mode):
                    raise ProfileEvidenceError("process executable is not a regular file")
                payload = _read_exact_fd(descriptor, "process executable", 1024 * 1024 * 1024)
            finally:
                os.close(descriptor)
        finally:
            os.close(process)
        after = self.process_identity(pid)
        if before != after:
            raise ProfileEvidenceError("process identity changed while binding executable")
        if target.endswith(" (deleted)") or not Path(target).is_absolute():
            raise ProfileEvidenceError("process executable target is not immutable absolute")
        return target, _sha256_bytes(payload)


def _delta(before: int, after: int, context: str) -> int:
    if after < before:
        raise ProfileEvidenceError(f"{context} rolled back: {before} -> {after}")
    return after - before


def task_delta(before: TaskCounters, after: TaskCounters) -> TaskDelta:
    if before.identity != after.identity:
        raise ProfileEvidenceError("cannot subtract counters across task identities")
    return TaskDelta(
        identity=before.identity,
        on_cpu_ns=_delta(before.on_cpu_ns, after.on_cpu_ns, "task on_cpu_ns"),
        voluntary_context_switches=_delta(
            before.voluntary_context_switches,
            after.voluntary_context_switches,
            "voluntary context switches",
        ),
        nonvoluntary_context_switches=_delta(
            before.nonvoluntary_context_switches,
            after.nonvoluntary_context_switches,
            "nonvoluntary context switches",
        ),
    )


def process_delta(before: ProcessCounters, after: ProcessCounters) -> ProcessDelta:
    if (before.pid, before.start_ticks) != (after.pid, after.start_ticks):
        raise ProfileEvidenceError("cannot subtract counters across process identities")
    io_values = {
        name: _delta(getattr(before.io, name), getattr(after.io, name), f"io {name}")
        for name in IO_FIELDS
        if name != "cancelled_write_bytes"
    }
    # Linux exposes cancelled_write_bytes as signed: it can move either way
    # when one process truncates data dirtied by another.  It is retained but
    # is not a monotone decision counter.
    io_values["cancelled_write_bytes"] = (
        after.io.cancelled_write_bytes - before.io.cancelled_write_bytes
    )
    io = ProcessIo(**io_values)
    # VmHWM (/proc/pid/status, mm->hiwater_rss) is a peak since process birth,
    # not an interval counter.  It is NOT strictly monotonic: recent kernels can
    # recompute hiwater_rss lower under heavy concurrent memory reclaim (observed
    # with the fairness 64-writer cells on this host), and the process identity
    # (pid+start_ticks) is already verified equal above, so an apparent rollback
    # is kernel accounting, not PID reuse.  Keep the true peak as the max of the
    # two observations.
    vm_hwm_bytes = max(before.vm_hwm_bytes, after.vm_hwm_bytes)
    return ProcessDelta(
        pid=before.pid,
        start_ticks=before.start_ticks,
        vm_hwm_bytes=vm_hwm_bytes,
        voluntary_context_switches=_delta(
            before.voluntary_context_switches,
            after.voluntary_context_switches,
            "process voluntary context switches",
        ),
        nonvoluntary_context_switches=_delta(
            before.nonvoluntary_context_switches,
            after.nonvoluntary_context_switches,
            "process nonvoluntary context switches",
        ),
        io=io,
    )


def rusage_delta(before: RusageCounters, after: RusageCounters) -> RusageCounters:
    return RusageCounters(
        user_ns=_delta(before.user_ns, after.user_ns, "rusage user_ns"),
        system_ns=_delta(before.system_ns, after.system_ns, "rusage system_ns"),
    )


def allocation_delta(
    before: AllocationCounters, after: AllocationCounters
) -> AllocationCounters:
    return AllocationCounters(
        calls=_delta(before.calls, after.calls, "allocation calls"),
        bytes=_delta(before.bytes, after.bytes, "allocation bytes"),
    )


def identities_by_key(tasks: Iterable[TaskIdentity]) -> dict[tuple[int, int], TaskIdentity]:
    result: dict[tuple[int, int], TaskIdentity] = {}
    for identity in tasks:
        key = (identity.tid, identity.start_ticks)
        if key in result:
            raise ProfileEvidenceError(f"duplicate TID/start identity {key}")
        result[key] = identity
    return result


def task_births(
    before: Iterable[TaskIdentity], after: Iterable[TaskIdentity]
) -> tuple[TaskIdentity, ...]:
    old = identities_by_key(before)
    new = identities_by_key(after)
    reused = {
        tid
        for tid, start in old
        for new_tid, new_start in new
        if tid == new_tid and start != new_start
    }
    if reused:
        raise ProfileEvidenceError(f"TID reuse across role phase: {sorted(reused)}")
    return tuple(sorted(identity for key, identity in new.items() if key not in old))


def require_unique_comm(
    tasks: Iterable[TaskIdentity], comm: str, role: str
) -> TaskIdentity:
    matches = tuple(identity for identity in tasks if identity.comm == comm)
    if len(matches) != 1:
        raise ProfileEvidenceError(
            f"role {role} requires one {comm!r} task, observed {len(matches)}"
        )
    return matches[0]


def require_main_task(snapshot: PhaseSnapshot) -> TaskIdentity:
    matches = tuple(task for task in snapshot.tasks if task.tid == snapshot.pid)
    if (
        len(matches) != 1
        or matches[0].pid != snapshot.pid
        or matches[0].start_ticks != snapshot.process_start_ticks
    ):
        raise ProfileEvidenceError("phase snapshot lacks the exact process main task")
    return matches[0]


def bind_unique_birth(
    role: str,
    before: Iterable[TaskIdentity],
    after: Iterable[TaskIdentity],
    *,
    excluded_comms: Iterable[str] = (),
    required_comm: str | None = None,
) -> TaskIdentity:
    excluded = frozenset(excluded_comms)
    births = task_births(before, after)
    unexpected_exclusions = tuple(
        identity
        for identity in births
        if identity.comm != required_comm and identity.comm not in excluded
    )
    if required_comm is not None and unexpected_exclusions:
        raise ProfileEvidenceError(
            f"role {role} has unreviewed open-phase births: "
            f"{[item.to_json() for item in unexpected_exclusions]}"
        )
    candidates = tuple(
        identity
        for identity in births
        if identity.comm not in excluded
        and (required_comm is None or identity.comm == required_comm)
    )
    if len(candidates) != 1:
        rendered = [identity.to_json() for identity in candidates]
        raise ProfileEvidenceError(
            f"role {role} requires one phase birth after exclusions, observed {rendered}"
        )
    return candidates[0]


def bind_birth_group(
    role: str,
    before: Iterable[TaskIdentity],
    after: Iterable[TaskIdentity],
    *,
    allowed_comms: Iterable[str] | None = None,
    allow_empty: bool = False,
) -> tuple[TaskIdentity, ...]:
    births = task_births(before, after)
    if allowed_comms is not None:
        allowed = frozenset(allowed_comms)
        unexpected = tuple(identity for identity in births if identity.comm not in allowed)
        if unexpected:
            raise ProfileEvidenceError(
                f"role {role} has unexpected births: {[item.to_json() for item in unexpected]}"
            )
    if not births and not allow_empty:
        raise ProfileEvidenceError(f"role {role} has no phase births")
    return births


class ProfileCoordinator:
    """Bind lifecycle phases and capture one parked child measurement window.

    The runner owns the inherited-socket handshake and parks the child at each
    named phase.  This class owns only `/proc` observation and role inference.
    Append order is ``boot -> runtime -> opened -> ready -> measured``.
    Reopen evidence cannot truthfully put ``opened`` before the timed open, so
    it uses ``boot -> runtime -> ready -> opened -> measured``; the runner
    sends ``start`` between ``ready`` and ``opened`` in both cases.
    """

    APPEND_PHASES = ("boot", "runtime", "opened", "ready", "measured")
    REOPEN_PHASES = ("boot", "runtime", "ready", "opened", "measured")
    PHASES = APPEND_PHASES
    ROLE_TRACKS = frozenset(("primary", "new_names", "fairness", "cpu_profiles"))
    VARIANTS = ("A", "B", "C", "D")

    def __init__(
        self,
        pid: int,
        variant: str,
        track: str,
        *,
        authority: Mapping[str, object],
        context: Mapping[str, object] | None = None,
        reader: ProcReader | None = None,
    ) -> None:
        if variant not in self.VARIANTS or not track:
            raise ProfileEvidenceError("invalid profile variant/track")
        supplied_context = dict(context or {})
        if any(not isinstance(key, str) for key in supplied_context):
            raise ProfileEvidenceError("profile context keys must be strings")
        try:
            normalized_context = json.loads(canonical_json(supplied_context))
        except (TypeError, ValueError) as error:
            raise ProfileEvidenceError("profile context is not canonical-JSON compatible") from error
        if not isinstance(normalized_context, dict):
            raise ProfileEvidenceError("profile context must be an object")
        self.reader = ProcReader() if reader is None else reader
        self.pid = pid
        self.variant = variant
        self.track = track
        self.context: dict[str, object] = normalized_context
        self.authority = validate_profile_authority(
            authority,
            pid=pid,
            variant=variant,
            track=track,
            context=self.context,
            reader=self.reader,
        )
        reopen_shape = track == "reopen" or (
            track == "structural_traces" and self.context.get("trace_kind") == "reopen"
        )
        self.phase_order = self.REOPEN_PHASES if reopen_shape else self.APPEND_PHASES
        self._phases: dict[str, PhaseSnapshot] = {}
        self._start: ProfileWindowStart | None = None
        self._result: ProfileWindowResult | None = None

    @classmethod
    def for_child(
        cls,
        child_pid: int,
        variant: str,
        track: str,
        *,
        authority: Mapping[str, object],
        context: Mapping[str, object] | None = None,
        proc_root: Path = Path("/proc"),
    ) -> "ProfileCoordinator":
        return cls(
            child_pid,
            variant,
            track,
            authority=authority,
            context=context,
            reader=ProcReader(proc_root),
        )

    def capture_phase(self, phase: str) -> PhaseSnapshot:
        if phase not in self.phase_order:
            raise ProfileEvidenceError(f"unknown profile phase {phase!r}")
        expected_index = len(self._phases)
        expected = (
            self.phase_order[expected_index]
            if expected_index < len(self.phase_order)
            else None
        )
        if expected != phase:
            raise ProfileEvidenceError(
                f"profile phase order differs: expected {expected!r}, got {phase!r}"
            )
        before = self.reader.process_identity(self.pid)
        tasks = self.reader.tasks(self.pid)
        after = self.reader.process_identity(self.pid)
        if before != after:
            raise ProfileEvidenceError("process identity changed during phase capture")
        snapshot = PhaseSnapshot(phase, self.pid, before[2], tasks)
        self._phases[phase] = snapshot
        return snapshot

    def _require_premeasurement_phases(self) -> dict[str, PhaseSnapshot]:
        ready_index = self.phase_order.index("ready")
        names = self.phase_order[: ready_index + 1]
        if tuple(self._phases) != names:
            raise ProfileEvidenceError(
                f"profile phases before begin differ: {tuple(self._phases)!r}"
            )
        phases = tuple(self._phases[name] for name in names)
        identities = {(phase.pid, phase.process_start_ticks) for phase in phases}
        if len(identities) != 1:
            raise ProfileEvidenceError("process identity differs across profile phases")
        return {name: self._phases[name] for name in names}

    def begin(self) -> ProfileWindowStart:
        if self._start is not None:
            raise ProfileEvidenceError("profile window already began")
        phases = self._require_premeasurement_phases()
        boot = phases["boot"]
        runtime = phases["runtime"]
        ready = phases["ready"]
        main_comm = require_main_task(boot).comm
        # A newly-spawned tokio worker inherits the process comm (main_comm) and
        # only renames itself to TOKIO_WORKER_COMM once it runs (prctl in-thread),
        # so a birth snapshot can legitimately catch a worker mid-rename with
        # main_comm.  Tolerate that pre-rename state in the tokio-worker birth
        # groups below (and in end()).
        self._main_comm = main_comm
        roles: list[RoleBinding] = []
        if self.track not in self.ROLE_TRACKS:
            pass
        elif self.variant in ("A", "D"):
            roles.append(
                RoleBinding(
                    "owner", (require_unique_comm(ready.tasks, "mess-flat-owner", "owner"),)
                )
            )
        elif self.variant == "B":
            opened = phases["opened"]
            roles.append(
                RoleBinding(
                    "committer",
                    (
                        bind_unique_birth(
                            "committer",
                            runtime.tasks,
                            opened.tasks,
                            excluded_comms=OPEN_HELPER_COMMS,
                            required_comm=main_comm,
                        ),
                    ),
                )
            )
        else:
            opened = phases["opened"]
            roles.extend(
                (
                    RoleBinding(
                        "committer",
                        (
                            bind_unique_birth(
                                "committer",
                                runtime.tasks,
                                opened.tasks,
                                excluded_comms=OPEN_HELPER_COMMS,
                                required_comm=main_comm,
                            ),
                        ),
                    ),
                    RoleBinding(
                        "producer-runtime",
                        bind_birth_group(
                            "producer-runtime",
                            boot.tasks,
                            runtime.tasks,
                            allowed_comms=(TOKIO_WORKER_COMM, main_comm),
                        ),
                    ),
                )
            )
        counters = tuple(
            self.reader.task_counters(task)
            for role in roles
            for task in role.tasks
        )
        self._start = ProfileWindowStart(
            variant=self.variant,
            track=self.track,
            ready=ready,
            process=self.reader.process_counters(self.pid),
            roles=tuple(roles),
            task_counters=counters,
        )
        return self._start

    def end(self) -> ProfileWindowResult:
        if self._start is None or self._result is not None:
            raise ProfileEvidenceError("profile window is not open exactly once")
        start = self._start
        end_phase = "opened" if self.phase_order == self.REOPEN_PHASES else "measured"
        if end_phase in self._phases:
            if tuple(self._phases)[-1] != end_phase:
                raise ProfileEvidenceError("profile end phase is not the latest capture")
            terminal = self._phases[end_phase]
        else:
            terminal = self.capture_phase(end_phase)
        if terminal.process_start_ticks != start.ready.process_start_ticks:
            raise ProfileEvidenceError("process identity changed across measured window")
        before_by_identity = {
            counters.identity: counters for counters in start.task_counters
        }
        roles: list[RoleDelta] = []
        bound_keys: set[tuple[int, int]] = set()
        for role in start.roles:
            deltas = []
            for identity in role.tasks:
                before = before_by_identity.get(identity)
                if before is None:
                    raise ProfileEvidenceError(f"missing start counters for role {role.label}")
                deltas.append(task_delta(before, self.reader.task_counters(identity)))
                bound_keys.add((identity.tid, identity.start_ticks))
            roles.append(RoleDelta(role.label, False, tuple(deltas)))

        births = (
            task_births(start.ready.tasks, terminal.tasks)
            if start.track in self.ROLE_TRACKS
            else ()
        )
        if start.track in self.ROLE_TRACKS and start.variant == "C":
            # The fairness track runs warm_rounds=4 (public/main.rs) which invokes
            # the injected spawn_blocking BEFORE the ready snapshot; with
            # thread_keep_alive=3600s those warm blocking threads persist, so the
            # in-window spawn_blocking legitimately REUSES one and no fresh TID is
            # born ready->measured.  Tolerate zero births for fairness only; the
            # other C tracks (warm_rounds=0) start with an empty pool and must
            # still observe the birth.  The role stays present (born_in_window=True)
            # so the variant-C role tuple and the birth replay (empty==empty) hold.
            publication = bind_birth_group(
                "spawn_blocking-publication",
                start.ready.tasks,
                terminal.tasks,
                allowed_comms=(TOKIO_WORKER_COMM, self._main_comm),
                allow_empty=(start.track == "fairness"),
            )
            publication_deltas = tuple(
                TaskDelta(
                    identity=identity,
                    on_cpu_ns=counters.on_cpu_ns,
                    voluntary_context_switches=counters.voluntary_context_switches,
                    nonvoluntary_context_switches=counters.nonvoluntary_context_switches,
                )
                for identity in publication
                for counters in (self.reader.task_counters(identity),)
            )
            roles.append(RoleDelta("spawn_blocking-publication", True, publication_deltas))
            bound_keys.update((identity.tid, identity.start_ticks) for identity in publication)

        unattributed = tuple(
            identity
            for identity in births
            if (identity.tid, identity.start_ticks) not in bound_keys
        )
        self._result = ProfileWindowResult(
            schema=PROFILE_SCHEMA,
            protocol=PROTOCOL,
            authority=self.authority,
            variant=start.variant,
            track=start.track,
            context=self.context,
            process=process_delta(start.process, self.reader.process_counters(self.pid)),
            roles=tuple(roles),
            phase_snapshots=tuple(self._phases.values()),
            unattributed_births=unattributed,
        )
        return self._result

    def finish(self) -> dict[str, object]:
        if self._result is None or tuple(self._phases) != self.phase_order:
            raise ProfileEvidenceError("profile coordinator finished before one complete window")
        # Round-trip once more so the runner receives a detached JSON object,
        # not references to mutable caller context.
        value = self._result.to_json()
        # Reopen ends its counter window while SIGSTOP-parked at `opened`, then
        # captures `measured` as a proof-only control phase.  Preserve the full
        # lifecycle without moving the already-frozen process delta.
        value["phase_snapshots"] = [
            self._phases[name].to_json() for name in self.phase_order
        ]
        return json.loads(canonical_json(value))


def schedstat_resolution(samples_ns: Sequence[int]) -> SchedstatResolution:
    if len(samples_ns) < 3:
        raise ProfileEvidenceError("schedstat preflight requires at least three samples")
    parsed = tuple(_nonnegative_integer(str(value), "schedstat sample") for value in samples_ns)
    increments = []
    for before, after in zip(parsed, parsed[1:]):
        if after < before:
            raise ProfileEvidenceError("schedstat preflight is not monotone")
        if after > before:
            increments.append(after - before)
    if not increments:
        raise ProfileEvidenceError("schedstat preflight observed no nonzero increment")
    return SchedstatResolution(parsed, min(increments))


def require_measurable_role_cpu(
    delta_ns: int,
    resolution: SchedstatResolution,
    multiplier: int = SCHEDSTAT_DECISION_MULTIPLIER,
) -> None:
    if multiplier <= 0:
        raise ProfileEvidenceError("CPU resolution multiplier must be positive")
    required = resolution.minimum_nonzero_increment_ns * multiplier
    if delta_ns < required:
        raise ProfileEvidenceError(
            f"role CPU delta {delta_ns}ns is below measurable floor {required}ns"
        )


def measure_schedstat_resolution(
    *,
    proc_root: Path = Path("/proc"),
    sample_count: int = 64,
    timeout_seconds: float = 5.0,
) -> SchedstatResolution:
    """Measure schedstat's observed increment with an owned CPU-bound thread.

    The helper is deliberately created and joined inside this call so its TID
    cannot be confused with a benchmark role.  Sampling remains identity-bound
    until the final observation; a disappearing/reused helper or a scheduler
    that exposes no nonzero increment fails the preflight.
    """

    if sample_count < 3:
        raise ProfileEvidenceError("schedstat live preflight requires at least three samples")
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ProfileEvidenceError("schedstat live preflight timeout must be positive")
    ready = threading.Event()
    stop = threading.Event()
    native_tid: list[int] = []

    def burn_cpu() -> None:
        native_tid.append(threading.get_native_id())
        ready.set()
        value = 1
        while not stop.is_set():
            # Bounded integer arithmetic prevents the loop from allocating an
            # ever-growing Python integer while keeping the helper runnable.
            value = (value * 1_664_525 + 1_013_904_223) & 0xFFFF_FFFF
        if value == -1:  # pragma: no cover - keeps the loop result observable.
            raise AssertionError("unreachable CPU helper state")

    helper = threading.Thread(target=burn_cpu, name="asterism-schedstat", daemon=False)
    helper.start()
    try:
        if not ready.wait(timeout_seconds) or len(native_tid) != 1:
            raise ProfileEvidenceError("schedstat CPU helper did not publish one native TID")
        pid = os.getpid()
        tid = native_tid[0]
        reader = ProcReader(proc_root)
        identity = reader.task_identity(pid, tid)
        deadline = time.monotonic() + timeout_seconds
        samples: list[int] = []
        while time.monotonic() < deadline:
            observed = reader.task_identity(pid, tid)
            if observed != identity:
                raise ProfileEvidenceError("schedstat CPU helper identity changed")
            samples.append(
                parse_schedstat(reader._read(str(pid), "task", str(tid), "schedstat"))
            )
            if len(samples) >= sample_count and len(set(samples)) > 1:
                break
            # Yield the GIL so the CPU-bound helper accumulates scheduled time.
            time.sleep(0)
        if reader.task_identity(pid, tid) != identity:
            raise ProfileEvidenceError("schedstat CPU helper identity changed after sampling")
        return schedstat_resolution(samples)
    finally:
        stop.set()
        helper.join(timeout=timeout_seconds)
        if helper.is_alive():
            raise ProfileEvidenceError("schedstat CPU helper did not terminate")


def preflight_profile_contract(
    *,
    proc_root: Path = Path("/proc"),
    sample_count: int = 64,
    timeout_seconds: float = 5.0,
) -> dict[str, object]:
    """Return the canonical once-per-attempt schedstat preflight artifact."""

    resolution = measure_schedstat_resolution(
        proc_root=proc_root,
        sample_count=sample_count,
        timeout_seconds=timeout_seconds,
    )
    return {
        "schema": PREFLIGHT_SCHEMA,
        "protocol": PROTOCOL,
        "protocol_sha256": PROTOCOL_SHA256,
        "profile_contract_sha256": _sha256_bytes(canonical_json(profile_contract())),
        "source": "/proc/<pid>/task/<native-tid>/schedstat:first-field",
        "helper": "adapter-owned-cpu-bound-native-thread",
        "samples_ns": list(resolution.samples_ns),
        "minimum_nonzero_increment_ns": resolution.minimum_nonzero_increment_ns,
        "decision_multiplier": SCHEDSTAT_DECISION_MULTIPLIER,
        "decision_floor_ns": (
            resolution.minimum_nonzero_increment_ns * SCHEDSTAT_DECISION_MULTIPLIER
        ),
    }


_PERF_UNAVAILABLE = frozenset({"<not supported>", "<not counted>"})


def parse_perf_stat_csv(
    payload: str, expected_events: Sequence[str] = PERF_EVENT_SPECS
) -> tuple[PerfCounter, ...]:
    """Parse ``perf stat -x, --no-big-num`` output in an LC_ALL=C locale."""

    expected = tuple(expected_events)
    if len(set(expected)) != len(expected) or not expected:
        raise ProfileEvidenceError("perf expected-event list is empty or duplicated")
    observed: dict[str, PerfCounter] = {}
    expected_by_event = {spec.removesuffix(":u"): spec for spec in expected}
    if set(expected_by_event) != set(PERF_EVENTS) or len(expected_by_event) != len(expected):
        raise ProfileEvidenceError("perf expected-event authority differs")
    for raw in payload.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        fields = [part.strip() for part in line.split(",")]
        # Real perf 7.1.4 emits 7 CSV fields:
        # value,unit,event,runtime,percent,metric-value,metric-unit — the last
        # two are empty for these counters.  Accept 5-7 fields provided every
        # field beyond index 4 (percent) is empty.
        if len(fields) not in {5, 6, 7} or any(fields[5:]):
            raise ProfileEvidenceError(f"perf CSV row shape differs: {raw!r}")
        value_text, unit, raw_event = fields[:3]
        event = raw_event.removesuffix(":u")
        if expected_by_event.get(event) != raw_event:
            raise ProfileEvidenceError(f"unexpected perf event {raw_event!r}")
        if event in observed:
            raise ProfileEvidenceError(f"duplicate perf event {event!r}")
        runtime_ns = _nonnegative_integer(fields[3], f"perf {event} runtime")
        if runtime_ns == 0:
            raise ProfileEvidenceError(f"perf {event} runtime is zero")
        percent_text = fields[4].removesuffix("%")
        if not re.fullmatch(r"\d+(?:\.\d+)?", percent_text):
            raise ProfileEvidenceError(f"bad perf running percent for {event}")
        try:
            running_percent = Decimal(percent_text)
        except InvalidOperation as error:
            raise ProfileEvidenceError(f"bad perf running percent for {event}") from error
        if not Decimal(0) < running_percent <= Decimal(100):
            raise ProfileEvidenceError(f"bad perf running percent for {event}")
        if value_text in _PERF_UNAVAILABLE:
            raise ProfileEvidenceError(f"perf {event} unavailable after available preflight")
        if event == "task-clock":
            if unit != "msec" or not re.fullmatch(r"\d+(?:\.\d+)?", value_text):
                raise ProfileEvidenceError("perf task-clock unit/value differs")
            try:
                if Decimal(value_text) < 0:
                    raise ProfileEvidenceError("perf task-clock is negative")
            except InvalidOperation as error:
                raise ProfileEvidenceError("perf task-clock is malformed") from error
            value: int | str = value_text
        else:
            if unit or not re.fullmatch(r"\d+", value_text):
                raise ProfileEvidenceError(f"perf {event} unit/value differs")
            value = int(value_text)
        counter = PerfCounter(
            event=event,
            status="available",
            value=value,
            unit=unit,
            runtime_ns=runtime_ns,
            running_percent=percent_text,
        )
        observed[event] = counter
    if set(observed) != set(expected_by_event):
        raise ProfileEvidenceError(
            f"perf events differ: missing={sorted(set(expected_by_event) - set(observed))}"
        )
    return tuple(observed[event] for event in PERF_EVENTS)


def _validate_perf_control_event(
    value: object, command: str, index: int
) -> dict[str, object]:
    event = _exact_mapping(
        value,
        (
            "command",
            "nonce",
            "sent_monotonic_ns",
            "ack",
            "ack_received_monotonic_ns",
        ),
        f"perf control event[{index}]",
    )
    nonce = event["nonce"]
    sent = _json_nonnegative_integer(event["sent_monotonic_ns"], f"perf {command} sent")
    received = _json_nonnegative_integer(
        event["ack_received_monotonic_ns"], f"perf {command} ack received"
    )
    if (
        event["command"] != command
        or event["ack"] != "ack"
        or not isinstance(nonce, str)
        or not _SHA256_RE.fullmatch(nonce)
        or received <= sent
    ):
        raise ProfileEvidenceError(f"perf {command} control authority differs")
    return dict(event)


def _validate_perf_control_events(
    control_events_value: object,
) -> tuple[dict[str, object], dict[str, object]]:
    if not isinstance(control_events_value, Sequence) or isinstance(
        control_events_value, (str, bytes)
    ):
        raise ProfileEvidenceError("perf control events must be a sequence")
    events = list(control_events_value)
    if len(events) != 2:
        raise ProfileEvidenceError("perf control event cardinality differs")
    normalized = [
        _validate_perf_control_event(value, command, index)
        for index, (value, command) in enumerate(
            zip(events, ("enable", "disable"), strict=True)
        )
    ]
    if (
        normalized[0]["nonce"] != normalized[1]["nonce"]
        or normalized[1]["sent_monotonic_ns"]
        <= normalized[0]["ack_received_monotonic_ns"]
    ):
        raise ProfileEvidenceError("perf enable/disable control sequence differs")
    return normalized[0], normalized[1]


def validate_perf_control_ack(
    payload: str, control_events_value: object
) -> tuple[dict[str, object], dict[str, object]]:
    """Bind generic perf ACK bytes to exact runner commands/nonces/timestamps."""

    if payload != "ack\nack\n":
        raise ProfileEvidenceError("perf control acknowledgements differ")
    return _validate_perf_control_events(control_events_value)


def _perf_permission_status(value: object) -> str:
    if not isinstance(value, str):
        raise ProfileEvidenceError("perf permission result is not text")
    if re.fullmatch(r"available;perf_event_paranoid=-?\d+;scope=user-only", value):
        return "available"
    unavailable = re.fullmatch(
        r"not_available;perf_event_paranoid=-?\d+;scope=user-only;exit_status=(\d+)",
        value,
    )
    if unavailable is None:
        raise ProfileEvidenceError("perf permission result is not exact")
    exit_status = _nonnegative_integer(unavailable.group(1), "perf permission exit status")
    if exit_status == 0:
        raise ProfileEvidenceError("unavailable perf permission has zero exit status")
    return "not_available"


def perf_profile_inputs(
    stat_payload: str,
    control_ack_payload: str,
    permission_result: str,
    *,
    control_events: Sequence[Mapping[str, object]] = (),
) -> dict[str, object]:
    """Normalize runner-owned perf artifacts into ``profile_fields`` inputs."""

    permission_status = _perf_permission_status(permission_result)
    if not stat_payload and not control_ack_payload:
        if permission_status == "available":
            raise ProfileEvidenceError("available perf permission lacks row artifacts")
        if control_events:
            raise ProfileEvidenceError("unavailable perf permission has control events")
        value = {
            "perf_permission": permission_result,
            "perf_control_acknowledged": False,
            "perf_counters": [
                PerfCounter(event, "not_available", None, "", None, None).to_json()
                for event in PERF_EVENTS
            ],
            "perf_control_events": [],
            "perf_stat_sha256": _sha256_bytes(b""),
            "perf_stat_bytes": 0,
            "perf_ack_sha256": _sha256_bytes(b""),
            "perf_ack_bytes": 0,
        }
        detached = json.loads(canonical_json(value))
        if not isinstance(detached, dict):
            raise AssertionError("perf input adapter did not produce an object")
        return detached
    if not stat_payload or not control_ack_payload:
        raise ProfileEvidenceError("perf row artifacts are partial")
    if permission_status != "available":
        raise ProfileEvidenceError("unavailable perf permission has counter artifacts")
    validated_control = validate_perf_control_ack(control_ack_payload, control_events)
    counters = parse_perf_stat_csv(stat_payload)
    value = {
        "perf_permission": permission_result,
        "perf_control_acknowledged": True,
        "perf_counters": [counter.to_json() for counter in counters],
        "perf_control_events": [event for event in validated_control],
        "perf_stat_sha256": _sha256_bytes(stat_payload.encode()),
        "perf_stat_bytes": len(stat_payload.encode()),
        "perf_ack_sha256": _sha256_bytes(control_ack_payload.encode()),
        "perf_ack_bytes": len(control_ack_payload.encode()),
    }
    detached = json.loads(canonical_json(value))
    if not isinstance(detached, dict):
        raise AssertionError("perf input adapter did not produce an object")
    return detached


def parse_strace_summary(payload: str) -> tuple[SyscallCount, ...]:
    """Parse the calls/errors portion of ``strace -c`` output."""

    result: dict[str, SyscallCount] = {}
    saw_header = False
    for raw in payload.splitlines():
        line = raw.strip()
        if not line or set(line.replace(" ", "")) == {"-"}:
            continue
        if line.startswith("% time") and line.endswith("syscall"):
            saw_header = True
            continue
        if line.startswith("100.00") and line.endswith("total"):
            continue
        if not saw_header:
            continue
        fields = line.split()
        if len(fields) not in (5, 6):
            raise ProfileEvidenceError(f"malformed strace summary row: {raw!r}")
        syscall = fields[-1]
        calls = _nonnegative_integer(fields[3], f"strace {syscall} calls")
        errors = 0 if len(fields) == 5 else _nonnegative_integer(
            fields[4], f"strace {syscall} errors"
        )
        if syscall in result:
            raise ProfileEvidenceError(f"duplicate strace syscall {syscall}")
        result[syscall] = SyscallCount(syscall, calls, errors)
    if not saw_header or not result:
        raise ProfileEvidenceError("strace summary has no parsed syscall rows")
    return tuple(result[name] for name in sorted(result))


_RAW_CALL = re.compile(
    r"^(?:\[pid\s+\d+\]\s+|\d+\s+)?(?:\d+\.\d+\s+)?([A-Za-z0-9_]+)\("
)
_TRACE_MARKER_WRITE = re.compile(
    # The child emits markers over a UnixStream, so Rust std lowers each write
    # to sendto(fd, payload, len, MSG_NOSIGNAL, NULL, 0) on Linux.  Accept both
    # sendto and write(2); tolerate sendto's trailing flags/addr args after the
    # byte count.  The fd + exact-payload check in _is_exact_trace_marker keeps
    # a same-payload domain write on another fd from matching.
    #
    # Under concurrency strace can interleave another thread's syscall and split
    # the marker send into an `<unfinished ...>` entry line + a later
    # `<... sendto resumed> ) = N`.  The entry line already carries fd, payload
    # and the requested byte count -- everything needed to identify the boundary
    # -- so accept that termination too; the resumed line's `= N` is optional
    # (see _is_exact_trace_marker) and the interval simply closes where the send
    # STARTED.  `[^)<]*` stops the flags/addr run before either `)` or `<`.
    r"^(?:\[pid\s+(?P<bracket_pid>\d+)\]\s+|(?P<pid>\d+)\s+)"
    r"(?:\d+\.\d+\s+)?(?:write|sendto)\((?P<fd>\d+)(?:<[^,]*>)?,\s*"
    r"(?P<payload>\"(?:\\.|[^\"\\])*\"),\s*(?P<count>\d+)"
    r"(?:,\s*[^)<]*)?"
    r"(?:\)\s*=\s*(?P<result>\d+)|\s*<unfinished \.\.\.>)$"
)
_TRACE_RESUMED = re.compile(
    r"^(?:\[pid\s+(?P<bracket_pid>\d+)\]\s+|(?P<pid>\d+)\s+)?"
    r"(?:\d+\.\d+\s+)?<\.\.\.\s+(?P<syscall>[A-Za-z0-9_]+) resumed>"
)
_TRACE_SYNC_FD = re.compile(
    r"^(?:\[pid\s+\d+\]\s+|\d+\s+)?(?:\d+\.\d+\s+)?"
    r"(?P<syscall>fsync|fdatasync)\(\d+<(?P<path>[^>]*)>"
    # Two terminators: a complete single-line call, or an interleaved
    # `<unfinished ...>` entry line (which carries the resolved -yy path but no
    # closing paren / `= N`).  The entry line is the line the interval counter
    # records, so accepting it lets the path family classify correctly.
    r"(?:\)\s*=\s*-?\d+(?:\s+.*)?|\s+<unfinished \.\.\.>)$"
)


def _wire_event(event_value: object, context: str) -> bytes:
    event = _as_mapping(event_value, context)
    wire = {key: value for key, value in event.items() if not key.startswith("_runner_")}
    # The child sends the canonical JSON and its trailing LF as two separate
    # UnixStream writes (sendto on Linux), so the marker payload is JSON-only
    # WITHOUT the LF that canonical_json appends.  Strip it here so the exact-
    # marker match can succeed; canonical_json itself is left intact for the
    # boundary sha256 binding.
    return canonical_json(wire)[:-1]


def _is_exact_trace_marker(
    raw: str,
    *,
    payload: bytes,
    child_pid: int,
    control_fd: int,
) -> bool:
    match = _TRACE_MARKER_WRITE.fullmatch(raw.strip())
    if match is None:
        return False
    pid_text = match.group("bracket_pid") or match.group("pid")
    try:
        rendered = ast.literal_eval(match.group("payload"))
    except (SyntaxError, ValueError) as error:
        raise ProfileEvidenceError(f"trace marker string is malformed: {raw!r}") from error
    if not isinstance(rendered, str):
        raise ProfileEvidenceError("trace marker payload is not text")
    rendered_bytes = rendered.encode()
    # When strace split the send (`<unfinished ...>`) the result byte count lives
    # on the later resumed line, so `result` is absent here.  The entry line's
    # fd + payload + requested count already identify the boundary uniquely; the
    # interval closes where the send started.  When present, the result must
    # equal the payload length (a complete, fully-sent marker).
    result_text = match.group("result")
    return (
        int(pid_text) == child_pid
        and int(match.group("fd")) == control_fd
        and int(match.group("count")) == len(payload)
        and (result_text is None or int(result_text) == len(payload))
        and rendered_bytes == payload
    )


def _trace_interval_records(
    payload: str,
    boundary: Mapping[str, object],
) -> list[tuple[str, str]]:
    exact = _exact_mapping(
        boundary,
        ("child_pid", "control_fd", "begin_event", "end_event"),
        "trace boundary authority",
    )
    child_pid = _json_nonnegative_integer(exact["child_pid"], "trace child pid")
    control_fd = _json_nonnegative_integer(exact["control_fd"], "trace control fd")
    if child_pid == 0 or control_fd == 0:
        raise ProfileEvidenceError("trace boundary pid/fd must be positive")
    begin_payload = _wire_event(exact["begin_event"], "trace begin event")
    end_payload = _wire_event(exact["end_event"], "trace end event")
    if begin_payload == end_payload:
        raise ProfileEvidenceError("trace boundary payloads are not distinct")
    active = False
    began = 0
    ended = 0
    records: list[tuple[str, str]] = []
    unfinished: Counter[tuple[str, str]] = Counter()
    for raw in payload.splitlines():
        begin = _is_exact_trace_marker(
            raw,
            payload=begin_payload,
            child_pid=child_pid,
            control_fd=control_fd,
        )
        end = _is_exact_trace_marker(
            raw,
            payload=end_payload,
            child_pid=child_pid,
            control_fd=control_fd,
        )
        if begin and end:
            raise ProfileEvidenceError("one trace line matches both exact boundaries")
        if begin:
            began += 1
            if active or began != 1:
                raise ProfileEvidenceError("duplicate/nested exact trace begin")
            active = True
            continue
        if end:
            ended += 1
            # Outstanding `unfinished` entries are allowed at the end marker:
            # threads blocked in futex across the interval edge leave an
            # in-window `<unfinished ...>` still outstanding.  It was already
            # counted at its initial in-window line, so this is not an error.
            if not active or ended != 1:
                raise ProfileEvidenceError("trace end has invalid state/outstanding calls")
            active = False
            continue
        if not active:
            continue
        stripped = raw.strip()
        resumed = _TRACE_RESUMED.match(stripped)
        if resumed is not None:
            pid_text = resumed.group("bracket_pid") or resumed.group("pid") or "unknown"
            key = (pid_text, resumed.group("syscall"))
            if unfinished[key] != 1:
                # A `<... resumed>` with no matching in-window `<unfinished ...>`
                # began before the interval (e.g. a futex blocked across the
                # begin marker).  It was never counted in-window, so skip it
                # rather than treating it as an unpaired call.
                continue
            del unfinished[key]
            continue
        # strace signal / stop frames (e.g. `--- SIGSTOP {...} ---`,
        # `--- stopped by SIGSTOP ---`, `--- SIGCONT {...} ---`) appear inside
        # the window when SIGSTOP-parked children are resumed.  They are not
        # syscalls; skip them rather than failing the interval parse.
        if stripped.startswith("---") and stripped.endswith("---"):
            continue
        match = _RAW_CALL.match(stripped)
        if match is None:
            raise ProfileEvidenceError(f"unparsed trace line inside interval: {raw!r}")
        syscall = match.group(1)
        if syscall not in SYSCALL_EVENTS:
            raise ProfileEvidenceError(f"unapproved syscall inside trace interval: {syscall}")
        records.append((raw, syscall))
        if "<unfinished ...>" in raw:
            prefix = re.match(r"^(?:\[pid\s+(\d+)\]|(\d+))", stripped)
            pid_text = (
                (prefix.group(1) or prefix.group(2)) if prefix is not None else "unknown"
            )
            unfinished[(pid_text, syscall)] += 1
    # Outstanding `unfinished` calls are permitted here (see the end-marker
    # branch): they were counted at their in-window initial line and their
    # resumed line lands after the interval.  Keep begin/end/active intact.
    if began != 1 or ended != 1 or active:
        raise ProfileEvidenceError(
            f"trace exact boundary/state invalid: begin={began} end={ended} "
            f"active={active} unfinished={dict(unfinished)}"
        )
    return records


def trace_interval_counts(
    payload: str,
    boundary: Mapping[str, object],
    *,
    allowed_syscalls: Iterable[str] | None = None,
) -> dict[str, int]:
    """Count calls strictly between exact PID/fd/full-frame marker writes.

    The marker-emitting ``write`` calls themselves are excluded.  An unfinished
    call is counted at its initial line; its ``resumed`` line is not counted a
    second time.
    """

    allowed = None if allowed_syscalls is None else frozenset(allowed_syscalls)
    counts: Counter[str] = Counter()
    for _, syscall in _trace_interval_records(payload, boundary):
        if allowed is not None and syscall not in allowed:
            continue
        counts[syscall] += 1
    return dict(sorted(counts.items()))


def trace_interval_metrics(
    payload: str,
    boundary: Mapping[str, object],
    *,
    allowed_syscalls: Iterable[str] = SYSCALL_EVENTS,
    log_path_markers: Iterable[object] = (),
    metadata_path_markers: Iterable[object] = (),
) -> dict[str, int]:
    """Parse exact operation families and sync-path partitions in one interval.

    Structural runs freeze ``strace -yy`` so a sync line contains the resolved
    descriptor path.  Every observed fsync/fdatasync must match exactly one of
    the reviewed log/metadata marker sets; an unclassified or doubly classified
    barrier fails closed instead of silently disappearing from either spine.
    """

    counts = trace_interval_counts(
        payload,
        boundary,
        allowed_syscalls=allowed_syscalls,
    )
    log_markers = _trace_path_markers(log_path_markers, "log")
    metadata_markers = _trace_path_markers(metadata_path_markers, "metadata")
    classified_markers = [("log", marker) for marker in log_markers] + [
        ("metadata", marker) for marker in metadata_markers
    ]
    if not classified_markers:
        raise ProfileEvidenceError("trace path marker authority is empty")
    for index, (left_family, left) in enumerate(classified_markers):
        for right_family, right in classified_markers[index + 1 :]:
            if _trace_markers_overlap(left, right):
                raise ProfileEvidenceError(
                    "trace path marker authority overlaps: "
                    f"{left_family}={left!r} {right_family}={right!r}"
                )
    file_create = 0
    file_rename = 0
    file_unlink = 0
    files_opened = 0
    log_sync = 0
    metadata_sync = 0
    for raw, syscall in _trace_interval_records(payload, boundary):
        if syscall in ("open", "openat", "openat2", "creat"):
            files_opened += 1
            if syscall == "creat" or "O_CREAT" in raw:
                file_create += 1
        elif syscall in ("mkdir", "mkdirat"):
            file_create += 1
        elif syscall in ("rename", "renameat", "renameat2"):
            file_rename += 1
        elif syscall in ("unlink", "unlinkat", "rmdir"):
            file_unlink += 1
        if syscall in ("fsync", "fdatasync"):
            path = _trace_sync_path(raw, syscall)
            matches = [
                family
                for family, marker in classified_markers
                if _trace_path_matches(path, marker)
            ]
            if len(matches) != 1:
                raise ProfileEvidenceError(
                    f"sync call does not match exactly one reviewed path family: {raw!r}"
                )
            log_sync += int(matches[0] == "log")
            metadata_sync += int(matches[0] == "metadata")
    result = {
        **counts,
        "file_create": file_create,
        "file_rename": file_rename,
        "file_unlink": file_unlink,
        "log_sync_calls": log_sync,
        "metadata_sync_calls": metadata_sync,
        "files_opened": files_opened,
    }
    return dict(sorted(result.items()))


def _trace_path_markers(
    value: Iterable[object], context: str
) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ProfileEvidenceError(f"trace {context} path markers are not a sequence")
    markers: list[tuple[str, str]] = []
    for index, value_marker in enumerate(value):
        marker = _exact_mapping(
            value_marker,
            ("kind", "path"),
            f"trace {context} path marker[{index}]",
        )
        kind = marker["kind"]
        path_value = marker["path"]
        if kind not in {"exact", "file_prefix", "directory_prefix"}:
            raise ProfileEvidenceError(f"trace {context} path marker kind differs")
        if not isinstance(path_value, str):
            raise ProfileEvidenceError(f"trace {context} path marker path is not text")
        directory = kind == "directory_prefix"
        if directory != path_value.endswith("/"):
            raise ProfileEvidenceError(
                f"trace {context} {kind} path suffix differs: {path_value!r}"
            )
        candidate = path_value[:-1] if directory else path_value
        path = Path(candidate)
        if (
            not candidate
            or "\x00" in path_value
            or "\\" in path_value
            or path_value.startswith("//")
            or not path.is_absolute()
            or ".." in path.parts
            or str(path) != candidate
            or (kind == "file_prefix" and not path.name)
        ):
            raise ProfileEvidenceError(
                f"trace {context} path marker is not canonical absolute: {value_marker!r}"
            )
        markers.append((str(kind), path_value))
    if len(set(markers)) != len(markers):
        raise ProfileEvidenceError(f"trace {context} path markers are duplicated")
    return tuple(markers)


def _trace_sync_path(raw: str, syscall: str) -> str:
    match = _TRACE_SYNC_FD.fullmatch(raw.strip())
    if match is None or match.group("syscall") != syscall:
        raise ProfileEvidenceError(f"sync trace lacks one exact -yy FD target: {raw!r}")
    target = match.group("path")
    if (
        not target
        or "\\" in target
        or target.startswith("//")
        or target.endswith(" (deleted)")
        or not Path(target).is_absolute()
        or ".." in Path(target).parts
        or str(Path(target)) != target
    ):
        raise ProfileEvidenceError(f"sync trace FD target is ambiguous: {target!r}")
    return target


def _trace_path_matches(path: str, marker: tuple[str, str]) -> bool:
    kind, authority_path = marker
    if kind == "exact":
        return path == authority_path
    if kind == "directory_prefix":
        # Normal case: a file inside the directory subtree.  Directory-fd case:
        # an fsync of the directory *itself* renders its -yy path with no
        # trailing slash (e.g. `<store>/log`), which does not `startswith` the
        # slash-terminated prefix; classify it into this family too.  This does
        # not leak across families because the reviewed markers are proven
        # non-overlapping (checked in trace_interval_metrics).
        return path.startswith(authority_path) or path == authority_path.rstrip("/")
    # file_prefix: files sharing the prefix, plus a barrier fsync of the parent
    # directory that holds them (e.g. the log directory `<store>/log` for a
    # `<store>/log/seg-` file prefix).
    return path.startswith(authority_path) or path == str(Path(authority_path).parent)


def _trace_markers_overlap(
    left: tuple[str, str], right: tuple[str, str]
) -> bool:
    left_kind, left_path = left
    right_kind, right_path = right
    if left_kind == "exact":
        return _trace_path_matches(left_path, right)
    if right_kind == "exact":
        return _trace_path_matches(right_path, left)
    return left_path.startswith(right_path) or right_path.startswith(left_path)


def strace_profile_inputs(
    payload: str,
    boundary: Mapping[str, object],
    *,
    log_path_markers: Iterable[object] = (),
    metadata_path_markers: Iterable[object] = (),
) -> dict[str, object]:
    """Normalize one runner-owned ``strace -f -yy`` artifact."""

    counts = trace_interval_metrics(
        payload,
        boundary,
        log_path_markers=log_path_markers,
        metadata_path_markers=metadata_path_markers,
    )
    records = _trace_interval_records(payload, boundary)
    value = {
        "begin_markers": 1,
        "end_markers": 1,
        "trace_counts": counts,
        "trace_raw_sha256": _sha256_bytes(payload.encode()),
        "trace_raw_bytes": len(payload.encode()),
        "trace_interval_lines": len(records),
        "trace_boundary_sha256": _sha256_bytes(canonical_json(dict(boundary))),
    }
    detached = json.loads(canonical_json(value))
    if not isinstance(detached, dict):
        raise AssertionError("strace input adapter did not produce an object")
    return detached


_RICH_RESULT_FIELDS = frozenset(
    {
        "schema",
        "protocol",
        "authority",
        "variant",
        "track",
        "context",
        "process",
        "roles",
        "phase_snapshots",
        "unattributed_births",
    }
)
_ROLE_SAMPLE_FIELDS = frozenset(
    {
        "role",
        "tid",
        "start_ticks",
        "cpu_ns",
        "voluntary_switches",
        "nonvoluntary_switches",
    }
)
_TRACE_AGGREGATES = frozenset(
    {
        "file_create",
        "file_rename",
        "file_unlink",
        "log_sync_calls",
        "metadata_sync_calls",
        "files_opened",
    }
)
_NOT_AVAILABLE = "not_available"


def _as_mapping(value: object, context: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise ProfileEvidenceError(f"{context} must be a string-keyed object")
    return value


def _exact_mapping(
    value: object, expected: Iterable[str], context: str
) -> Mapping[str, object]:
    mapping = _as_mapping(value, context)
    expected_keys = frozenset(expected)
    if frozenset(mapping) != expected_keys:
        raise ProfileEvidenceError(
            f"{context} fields differ: missing={sorted(expected_keys - frozenset(mapping))} "
            f"extra={sorted(frozenset(mapping) - expected_keys)}"
        )
    return mapping


def _json_nonnegative_integer(value: object, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ProfileEvidenceError(f"{context} must be a nonnegative JSON integer")
    return value


def _sha256_authority(value: object, context: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ProfileEvidenceError(f"{context} SHA-256 is malformed")
    return value


def _release_file_snapshot(
    value: object,
    *,
    expected_mode: int,
    context: str,
) -> tuple[Path, bytes, tuple[int, int, int, int, int, int, int]]:
    binding = _exact_mapping(value, _RELEASE_COMPILE_OUT_FILE_FIELDS, context)
    if binding["mode"] != expected_mode:
        raise ProfileEvidenceError(f"{context} mode authority differs")
    identity = _exact_mapping(
        binding["identity"], _RELEASE_COMPILE_OUT_IDENTITY_FIELDS, f"{context} identity"
    )
    for name in _RELEASE_COMPILE_OUT_IDENTITY_FIELDS:
        item = identity[name]
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise ProfileEvidenceError(f"{context} identity {name} is invalid")
    path, payload, observed = _immutable_file_snapshot(
        binding["path"], binding["sha256"], expected_mode, context
    )
    observed_identity = {
        "changed_ns": observed[6],
        "device": observed[0],
        "inode": observed[1],
        "link_count": observed[2],
        "modified_ns": observed[5],
    }
    if (
        _json_nonnegative_integer(binding["size"], f"{context} size")
        != len(payload)
        or identity != observed_identity
        or observed[2] != 1
    ):
        raise ProfileEvidenceError(f"{context} file identity differs")
    return path, payload, observed


def _validate_release_compile_out_authority(
    approval: Mapping[str, object],
    prepared: Mapping[str, object],
    prepared_root: Path,
    source_approval_sha256: str,
    executable_path: str,
    executable_sha256: str,
    variant: str,
) -> None:
    source_review = _exact_mapping(
        approval.get("source_review"),
        (
            "assertion_sha256",
            "bundle",
            "current_children_attestation",
            "lock_authority",
            "lock_review_bundle",
            "release_compile_out_requirement",
        ),
        "source-approved source review",
    )
    assertion_sha256 = _sha256_authority(
        source_review["assertion_sha256"], "source-review assertion"
    )
    requirement = _exact_mapping(
        source_review["release_compile_out_requirement"],
        _RELEASE_COMPILE_OUT_REQUIREMENT_FIELDS,
        "release compile-out requirement",
    )
    if (
        requirement["schema"] != "bn-3hch-release-compile-out-requirement-v1"
        or requirement["status"] != "required"
        or requirement["variant"] != "A"
        or requirement["product_overlay_sha256"]
        != _CURRENT_PRODUCT_OVERLAY_SHA256
        or requirement["proof_must_bind_enclosing_approval_sha256"] is not True
        or requirement["repeat_under_real_source_approval"] is not True
        or requirement["same_contract_nonce_lock_toolchain_sandbox"] is not True
        or requirement["cfg_test"] is not False
        or requirement["rustc_workspace_wrapper"] != "absent"
        or requirement["ordinary_a_role"] != "published"
        or requirement["overlay_a_role"] != "proof_only"
        or requirement["binary_byte_identical"] is not True
        or requirement["symbol_inventory_byte_identical"] is not True
        or requirement["forbidden_hook_strings"]
        != list(_FORBIDDEN_RELEASE_HOOK_STRINGS)
        or requirement["forbidden_hook_strings_absent"] is not True
    ):
        raise ProfileEvidenceError("release compile-out requirement differs")
    preapproval_sha256 = _sha256_authority(
        requirement["preapproval_compile_out_sha256"],
        "preapproval release compile-out",
    )
    requirement_sha256 = _sha256_bytes(canonical_json(dict(requirement)))

    prepared_source_review = _exact_mapping(
        prepared.get("source_review"),
        _SOURCE_REVIEW_CONTENT_SCHEMAS,
        "prepared source-review bindings",
    )
    source_review_values: dict[str, dict[str, object]] = {}
    source_review_payloads: dict[str, bytes] = {}
    for name, schema in _SOURCE_REVIEW_CONTENT_SCHEMAS.items():
        approved_binding = _exact_mapping(
            source_review[name],
            ("schema", "sha256", "mode"),
            f"source-approved source-review {name}",
        )
        local_binding = _exact_mapping(
            prepared_source_review[name],
            ("path", "sha256", "mode"),
            f"prepared source-review {name}",
        )
        expected_path = prepared_root / _SOURCE_REVIEW_PATHS[name]
        if (
            approved_binding["schema"] != schema
            or approved_binding["mode"] != 0o444
            or local_binding["mode"] != 0o444
            or local_binding["path"] != str(expected_path)
            or local_binding["sha256"] != approved_binding["sha256"]
        ):
            raise ProfileEvidenceError(f"source-review {name} binding differs")
        path, payload, _identity = _immutable_file_snapshot(
            local_binding["path"],
            local_binding["sha256"],
            0o444,
            f"prepared source-review {name}",
        )
        if path != expected_path:
            raise ProfileEvidenceError(f"source-review {name} path differs")
        source_review_payloads[name] = payload
        source_review_values[name] = _canonical_json_payload(
            payload, f"prepared source-review {name}"
        )
        if source_review_values[name].get("schema") != schema:
            raise ProfileEvidenceError(f"source-review {name} schema differs")

    bundle = _exact_mapping(
        source_review_values["bundle"],
        ("assertion", "assertion_sha256", "review_created", "schema", "verdict"),
        "source-review bundle",
    )
    assertion = _as_mapping(bundle["assertion"], "source-review assertion")
    if (
        bundle["assertion_sha256"] != assertion_sha256
        or _sha256_bytes(canonical_json(dict(assertion))) != assertion_sha256
        or assertion.get("release_compile_out_requirement") != requirement
    ):
        raise ProfileEvidenceError("source-review assertion/requirement binding differs")
    current_children = source_review_values["current_children_attestation"]
    preapproval = current_children.get("release_compile_out")
    if (
        not isinstance(preapproval, dict)
        or _sha256_bytes(canonical_json(preapproval)) != preapproval_sha256
    ):
        raise ProfileEvidenceError("current-child preapproval proof binding differs")

    release_binding = _exact_mapping(
        prepared.get("release_compile_out"),
        ("path", "sha256", "mode"),
        "prepared release compile-out binding",
    )
    expected_release_path = prepared_root / "manifests" / "release-compile-out.json"
    if (
        release_binding["path"] != str(expected_release_path)
        or release_binding["mode"] != 0o444
    ):
        raise ProfileEvidenceError("prepared release compile-out path/mode differs")
    release_path, release_payload, _release_identity = _immutable_file_snapshot(
        release_binding["path"],
        release_binding["sha256"],
        0o444,
        "release compile-out proof",
    )
    if release_path != expected_release_path:
        raise ProfileEvidenceError("release compile-out proof path differs")
    proof = _exact_mapping(
        _canonical_json_payload(release_payload, "release compile-out proof"),
        _RELEASE_COMPILE_OUT_FIELDS,
        "release compile-out proof",
    )
    if (
        proof["schema"] != "bn-3hch-release-compile-out-v1"
        or proof["protocol"] != PROTOCOL
        or proof["protocol_sha256"] != PROTOCOL_SHA256
        or proof["status"] != "ok"
        or proof["source_approval_sha256"] != source_approval_sha256
        or proof["requirement_sha256"] != requirement_sha256
        or proof["current_children_attestation_sha256"]
        != _sha256_bytes(source_review_payloads["current_children_attestation"])
        or proof["product_overlay_sha256"] != _CURRENT_PRODUCT_OVERLAY_SHA256
        or proof["forbidden_hook_strings"]
        != list(_FORBIDDEN_RELEASE_HOOK_STRINGS)
        or proof["binary_byte_identical"] is not True
        or proof["symbol_inventory_byte_identical"] is not True
        or proof["forbidden_hook_strings_absent"] is not True
    ):
        raise ProfileEvidenceError("release compile-out proof authority differs")

    equivalence = _exact_mapping(
        proof["equivalence_contract"],
        _RELEASE_COMPILE_OUT_EQUIVALENCE_FIELDS,
        "release compile-out equivalence contract",
    )
    if (
        equivalence["source_approval_sha256"] != source_approval_sha256
        or equivalence["cfg_test"] is not False
        or equivalence["rustc_workspace_wrapper"] != "absent"
        or equivalence["ordinary_a_role"] != "published"
        or equivalence["overlay_a_role"] != "proof_only"
    ):
        raise ProfileEvidenceError("release compile-out equivalence differs")
    for name in (
        "contract_sha256",
        "build_nonce",
        "cargo_lock_sha256",
        "toolchain_sha256",
        "build_environment_sha256",
        "sandbox_sha256",
    ):
        _sha256_authority(equivalence[name], f"release equivalence {name}")

    builds = _exact_mapping(
        proof["builds"], ("ordinary_a", "overlay_a"), "release proof builds"
    )
    for name, artifact_role in (
        ("ordinary_a", "published"),
        ("overlay_a", "proof_only"),
    ):
        build = _exact_mapping(
            builds[name], _RELEASE_COMPILE_OUT_BUILD_FIELDS, f"release build {name}"
        )
        if (
            build["role"] != name
            or build["artifact_role"] != artifact_role
            or build["source_approval_sha256"] != source_approval_sha256
            or build["cfg_test"] is not False
            or build["rustc_workspace_wrapper"] != "absent"
        ):
            raise ProfileEvidenceError(f"release build {name} role differs")
        for field in (
            "contract_sha256",
            "build_nonce",
            "cargo_lock_sha256",
            "toolchain_sha256",
            "build_environment_sha256",
            "sandbox_sha256",
        ):
            if build[field] != equivalence[field]:
                raise ProfileEvidenceError(
                    f"release build {name} equivalence {field} differs"
                )
        attestation = _as_mapping(
            build["attestation"], f"release build {name} attestation"
        )
        if (
            _sha256_bytes(canonical_json(dict(attestation)))
            != build["attestation_sha256"]
            or _sha256_bytes(
                canonical_json(
                    dict(
                        _as_mapping(
                            attestation.get("build_env"),
                            f"release build {name} environment",
                        )
                    )
                )
            )
            != build["build_environment_sha256"]
            or _sha256_bytes(
                canonical_json(
                    dict(
                        _as_mapping(
                            attestation.get("toolchain"),
                            f"release build {name} toolchain",
                        )
                    )
                )
            )
            != build["toolchain_sha256"]
        ):
            raise ProfileEvidenceError(f"release build {name} attestation differs")

    binaries = _exact_mapping(
        proof["binaries"], ("ordinary_a", "overlay_a"), "release proof binaries"
    )
    inventories = _exact_mapping(
        proof["symbol_inventories"],
        ("ordinary_a", "overlay_a"),
        "release proof symbol inventories",
    )
    ordinary_binary = _release_file_snapshot(
        binaries["ordinary_a"], expected_mode=0o555, context="ordinary A binary"
    )
    overlay_binary = _release_file_snapshot(
        binaries["overlay_a"], expected_mode=0o555, context="overlay A proof binary"
    )
    ordinary_inventory = _release_file_snapshot(
        inventories["ordinary_a"],
        expected_mode=0o444,
        context="ordinary A symbol inventory",
    )
    overlay_inventory = _release_file_snapshot(
        inventories["overlay_a"],
        expected_mode=0o444,
        context="overlay A symbol inventory",
    )
    ordinary_binding = _as_mapping(binaries["ordinary_a"], "ordinary A binary")
    overlay_binding = _as_mapping(binaries["overlay_a"], "overlay A proof binary")
    prepared_variants = _as_mapping(prepared.get("variants"), "prepared variants")
    prepared_a = _as_mapping(prepared_variants.get("A"), "prepared variant A")
    prepared_a_binary = _as_mapping(prepared_a.get("binary"), "prepared A binary")
    if (
        ordinary_binary[1] != overlay_binary[1]
        or ordinary_inventory[1] != overlay_inventory[1]
        or ordinary_binary[2][:2] == overlay_binary[2][:2]
        or ordinary_inventory[2][:2] == overlay_inventory[2][:2]
        or proof["published_a_sha256"] != ordinary_binding["sha256"]
        or (prepared_a_binary.get("path"), prepared_a_binary.get("sha256"))
        != (ordinary_binding["path"], ordinary_binding["sha256"])
        or overlay_binding["path"] == ordinary_binding["path"]
        or overlay_binding["path"] == executable_path
        or str(overlay_binding["path"]).encode() in canonical_json(dict(prepared))
    ):
        raise ProfileEvidenceError("release proof publication/twin isolation differs")
    if variant == "A" and (executable_path, executable_sha256) != (
        ordinary_binding["path"],
        ordinary_binding["sha256"],
    ):
        raise ProfileEvidenceError("profiled A is not the published ordinary A")
    inspected_payloads = (
        ordinary_binary[1],
        overlay_binary[1],
        ordinary_inventory[1],
        overlay_inventory[1],
    )
    if any(
        marker.encode() in payload
        for marker in _FORBIDDEN_RELEASE_HOOK_STRINGS
        for payload in inspected_payloads
    ):
        raise ProfileEvidenceError("release proof contains a forbidden hook string")

    # The nm tool is the live system linker tool (e.g. /usr/bin/nm), not a
    # write-stripped pinned child binary, so its mode is whatever the
    # preapproval authority recorded (0o555 is wrong for it; the system nm is
    # 0o755).  evidence_schema.validate_preapproval_nm_authority binds the same
    # mode to this preapproval record, so derive expected_mode from it here to
    # keep the two validators from ever diverging (previously a stale 0o555).
    preapproval_nm = _as_mapping(
        preapproval.get("nm"), "preapproval nm authority"
    )
    preapproval_nm_identity = _as_mapping(
        preapproval_nm.get("identity"), "preapproval nm authority identity"
    )
    preapproval_nm_mode = preapproval_nm_identity.get("mode")
    if isinstance(preapproval_nm_mode, bool) or not isinstance(
        preapproval_nm_mode, int
    ):
        raise ProfileEvidenceError("preapproval nm authority mode differs")
    nm = _exact_mapping(
        proof["nm"], ("tool", "ordinary_a", "overlay_a"), "release proof nm"
    )
    nm_tool = _release_file_snapshot(
        nm["tool"], expected_mode=preapproval_nm_mode, context="release proof nm tool"
    )
    for name, inventory_payload in (
        ("ordinary_a", ordinary_inventory[1]),
        ("overlay_a", overlay_inventory[1]),
    ):
        child = _exact_mapping(
            nm[name], _RELEASE_COMPILE_OUT_NM_CHILD_FIELDS, f"release nm child {name}"
        )
        argv = child["argv"]
        expected_log = prepared_root / "logs" / f"nm-{name.replace('_', '-')}.json"
        if (
            not isinstance(argv, list)
            or argv[:4]
            != [
                str(nm_tool[0]),
                "--defined-only",
                "--demangle=rust",
                "--format=posix",
            ]
            or len(argv) != 5
            or re.fullmatch(r"/proc/self/fd/[1-9][0-9]*", str(argv[4])) is None
            or child["output_path"] != str(expected_log)
            or child["exit_status"] != 0
            or child["timed_out"] is not False
            or child["process_group_absent"] is not True
            or child["pid"] != child["waited_pid"]
        ):
            raise ProfileEvidenceError(f"release nm child {name} differs")
        reaping = _exact_mapping(
            child["reaping"], ("pid", "start_ticks", "status"), f"release nm {name} reaping"
        )
        if (
            reaping["pid"] != child["pid"]
            or reaping["start_ticks"] != child["start_ticks"]
            or reaping["status"] != "absent"
        ):
            raise ProfileEvidenceError(f"release nm child {name} reaping differs")
        _log_path, log_payload, _log_identity = _immutable_file_snapshot(
            child["output_path"],
            child["output_sha256"],
            0o444,
            f"release nm child {name} log",
        )
        log = _exact_mapping(
            _canonical_json_payload(log_payload, f"release nm child {name} log"),
            ("exit_status", "stderr", "stderr_sha256", "stdout", "stdout_sha256"),
            f"release nm child {name} log",
        )
        if (
            log["exit_status"] != 0
            or log["stderr"] != ""
            or log["stderr_sha256"] != _sha256_bytes(b"")
            or not isinstance(log["stdout"], str)
            or log["stdout"].encode() != inventory_payload
            or log["stdout_sha256"] != _sha256_bytes(inventory_payload)
        ):
            raise ProfileEvidenceError(f"release nm child {name} output differs")


def validate_profile_authority(
    value: object,
    *,
    pid: int,
    variant: str,
    track: str,
    context: Mapping[str, object],
    reader: ProcReader,
) -> dict[str, object]:
    """Replay the exact prepared/source/binary identity at the adapter seam."""

    authority = _exact_mapping(value, _AUTHORITY_FIELDS, "profile authority")
    if (
        authority["schema"] != AUTHORITY_SCHEMA
        or authority["protocol"] != PROTOCOL
        or authority["protocol_sha256"] != PROTOCOL_SHA256
        or authority["variant"] != variant
        or authority["track"] != track
        or authority["child_pid"] != pid
    ):
        raise ProfileEvidenceError("profile authority identity differs")
    for name in (
        "attempt_nonce",
        "context_sha256",
        "prepared_artifacts_sha256",
        "source_approval_sha256",
        "profile_adapter_sha256",
        "executable_sha256",
    ):
        item = authority[name]
        if not isinstance(item, str) or not _SHA256_RE.fullmatch(item):
            raise ProfileEvidenceError(f"profile authority {name} is malformed")
    for name in ("child_ordinal", "child_pid", "child_start_ticks", "control_fd"):
        item = authority[name]
        if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
            raise ProfileEvidenceError(f"profile authority {name} must be positive")
    row_ordinal = authority["row_ordinal"]
    if row_ordinal != "not_applicable" and (
        isinstance(row_ordinal, bool) or not isinstance(row_ordinal, int) or row_ordinal <= 0
    ):
        raise ProfileEvidenceError("profile authority row_ordinal is invalid")
    if (
        authority["executable_mode"] != 0o555
        or not isinstance(authority["executable_comm"], str)
        or not 0 < len(authority["executable_comm"].encode()) <= 15
    ):
        raise ProfileEvidenceError("profile executable mode/comm authority differs")
    expected_source = VARIANT_SOURCE_BINDINGS.get(variant)
    if expected_source is None or {
        "commit": authority["source_commit"],
        "tree": authority["source_tree"],
    } != expected_source:
        raise ProfileEvidenceError("profile variant source binding differs")
    if _sha256_bytes(canonical_json(dict(context))) != authority["context_sha256"]:
        raise ProfileEvidenceError("profile context SHA-256 differs")

    (
        attempt_approval_path,
        attempt_approval_payload,
        attempt_approval_identity,
    ) = _immutable_file_snapshot(
        authority["source_approval_path"],
        authority["source_approval_sha256"],
        0o444,
        "profile source approval",
    )
    approval = _canonical_json_payload(
        attempt_approval_payload, "profile source approval"
    )
    (
        attempt_prepared_path,
        attempt_prepared_payload,
        attempt_prepared_identity,
    ) = _immutable_file_snapshot(
        authority["prepared_artifacts_path"],
        authority["prepared_artifacts_sha256"],
        0o444,
        "profile prepared artifacts",
    )
    prepared = _canonical_json_payload(
        attempt_prepared_payload, "profile prepared artifacts"
    )
    if (
        attempt_prepared_path.name != "prepared-artifacts.json"
        or attempt_approval_path
        != attempt_prepared_path.with_name("source-approval.json")
    ):
        raise ProfileEvidenceError("profile attempt authority paths are not exact siblings")
    if (
        approval.get("schema") != "bn-2l3n-source-approval-v3"
        or approval.get("status") != "approved"
        or approval.get("protocol") != PROTOCOL
        or approval.get("protocol_sha256") != PROTOCOL_SHA256
    ):
        raise ProfileEvidenceError("profile source approval is not exact v3 approval")
    approval_variants = approval.get("variants")
    claim = approval_variants.get(variant) if isinstance(approval_variants, dict) else None
    expected_role_lifetime: object = (
        C_ROLE_LIFETIME_CONTRACT if variant == "C" else "not_applicable"
    )
    if not isinstance(claim, dict) or (
        claim.get("product_commit"), claim.get("product_tree")
    ) != (authority["source_commit"], authority["source_tree"]):
        raise ProfileEvidenceError("profile source approval variant differs")
    if claim.get("profile_role_lifetime") != expected_role_lifetime:
        raise ProfileEvidenceError("source-approved profile role lifetime differs")
    tools_manifest = approval.get("tools_manifest")
    support_claim = (
        tools_manifest.get("support_files", {}).get("profile_adapter")
        if isinstance(tools_manifest, dict)
        and isinstance(tools_manifest.get("support_files"), dict)
        else None
    )
    if not isinstance(support_claim, dict) or (
        support_claim.get("sha256"), support_claim.get("mode")
    ) != (authority["profile_adapter_sha256"], 0o444):
        raise ProfileEvidenceError("source-approved profile adapter differs")
    expected_tool_names = (
        {"perf"}
        if track == "cpu_profiles"
        else {"strace", "strace_launcher_runtime"}
        if track in {"syscall_profiles", "structural_traces"}
        else set()
    )
    profile_tools = authority["profile_tools"]
    if not isinstance(profile_tools, dict) or set(profile_tools) != expected_tool_names:
        raise ProfileEvidenceError("profile tool authority set differs")
    if track == "cpu_profiles":
        _perf_permission_status(authority["perf_permission_result"])
    elif authority["perf_permission_result"] != "not_applicable":
        raise ProfileEvidenceError("non-CPU profile has perf permission authority")
    approved_tools = tools_manifest.get("tools") if isinstance(tools_manifest, dict) else None
    if not isinstance(approved_tools, dict):
        raise ProfileEvidenceError("source-approved tool map is absent")

    if (
        prepared.get("schema") != "bn-2l3n-prepared-artifacts-v3"
        or prepared.get("protocol") != PROTOCOL
        or prepared.get("protocol_sha256") != PROTOCOL_SHA256
    ):
        raise ProfileEvidenceError("profile prepared artifacts are not exact v3")
    approval_binding = _exact_mapping(
        prepared.get("source_approval"),
        ("path", "sha256"),
        "prepared source-approval binding",
    )
    claim_binding = _exact_mapping(
        prepared.get("single_use_claim"),
        ("path",),
        "prepared single-use claim binding",
    )
    # Protocol v4 (bn-2vih): the prepared root is read-only and reusable and no
    # claim file is written under claims/.  The recorded v3 claim path is
    # validated lexically only (to anchor the prepared root), and the live claim
    # record is replayed from the run's own output directory
    # (<output>/run-claim.json, written by claim_prepared before any child and
    # re-verified by the runner every phase).  The prepared root itself is still
    # content-hash grounded below, so a forged lexical path cannot pass.
    claim_path_value = claim_binding["path"]
    if not isinstance(claim_path_value, str):
        raise ProfileEvidenceError("prepared single-use claim path is not text")
    claim_path = Path(claim_path_value)
    if (
        not claim_path.is_absolute()
        or claim_path_value.startswith("//")
        or ".." in claim_path.parts
        or str(claim_path) != claim_path_value
        or claim_path.name != "single-use-claim.json"
        or claim_path.parent.name != "claims"
    ):
        raise ProfileEvidenceError("prepared single-use claim path differs")
    prepared_root = claim_path.parent.parent
    try:
        prepared_root_mode = stat.S_IMODE(prepared_root.stat().st_mode)
    except OSError as error:
        raise ProfileEvidenceError(
            f"cannot replay prepared root mode: {error}"
        ) from error
    if prepared_root_mode != 0o555:
        raise ProfileEvidenceError("prepared root mode authority differs")
    _run_claim_path, _run_claim_payload, prepared_claim = (
        _unclaimed_local_canonical_json_snapshot(
            str(attempt_prepared_path.with_name("run-claim.json")),
            "run claim record",
        )
    )
    expected_claim_fields = (
        "schema",
        "protocol",
        "prepared_artifacts_path",
        "prepared_artifacts_sha256",
        "output_dir",
        "attempt_nonce",
        "lease_nonce",
        "claimed_at",
        "claimed_monotonic_ns",
    )
    prepared_claim = _exact_mapping(
        prepared_claim, expected_claim_fields, "run claim record"
    )
    original_prepared_path = prepared_root / "prepared-artifacts.json"
    if (
        original_prepared_path == attempt_prepared_path
        or prepared_root == attempt_prepared_path.parent
    ):
        raise ProfileEvidenceError("original/attempt prepared paths are not distinct")
    if (
        prepared_claim["schema"] != "bn-2l3n-prepared-claim-v3"
        or prepared_claim["protocol"] != PROTOCOL
        or prepared_claim["prepared_artifacts_path"] != str(original_prepared_path)
        or prepared_claim["prepared_artifacts_sha256"]
        != authority["prepared_artifacts_sha256"]
        or prepared_claim["output_dir"] != str(attempt_prepared_path.parent)
        or prepared_claim["attempt_nonce"] != authority["attempt_nonce"]
        or not isinstance(prepared_claim["lease_nonce"], str)
        or not _SHA256_RE.fullmatch(prepared_claim["lease_nonce"])
        or not isinstance(prepared_claim["claimed_at"], str)
        or not prepared_claim["claimed_at"]
        or isinstance(prepared_claim["claimed_monotonic_ns"], bool)
        or not isinstance(prepared_claim["claimed_monotonic_ns"], int)
        or prepared_claim["claimed_monotonic_ns"] <= 0
    ):
        raise ProfileEvidenceError("run claim record authority differs")
    (
        original_prepared_path,
        original_prepared_payload,
        original_prepared_identity,
    ) = _immutable_file_snapshot(
        str(original_prepared_path),
        authority["prepared_artifacts_sha256"],
        0o444,
        "original prepared artifacts",
    )
    if original_prepared_identity == attempt_prepared_identity:
        raise ProfileEvidenceError("original/attempt prepared file identities alias")
    if original_prepared_payload != attempt_prepared_payload:
        raise ProfileEvidenceError("original/attempt prepared artifact bytes differ")
    original_approval_path = prepared_root / "bindings" / "source-approval.json"
    if original_approval_path == attempt_approval_path:
        raise ProfileEvidenceError("original/attempt source-approval paths are not distinct")
    if (
        approval_binding["path"] != str(original_approval_path)
        or approval_binding["sha256"] != authority["source_approval_sha256"]
    ):
        raise ProfileEvidenceError("prepared/source-approval binding differs")
    (
        original_approval_path,
        original_approval_payload,
        original_approval_identity,
    ) = _immutable_file_snapshot(
        str(original_approval_path),
        authority["source_approval_sha256"],
        0o444,
        "original source approval",
    )
    if original_approval_identity == attempt_approval_identity:
        raise ProfileEvidenceError("original/attempt source-approval file identities alias")
    if original_approval_payload != attempt_approval_payload:
        raise ProfileEvidenceError("original/attempt source-approval bytes differ")
    _validate_release_compile_out_authority(
        approval,
        prepared,
        prepared_root,
        str(authority["source_approval_sha256"]),
        str(authority["executable_path"]),
        str(authority["executable_sha256"]),
        variant,
    )
    support_files = prepared.get("support_files")
    adapter_binding = (
        support_files.get("profile_adapter") if isinstance(support_files, dict) else None
    )
    if not isinstance(adapter_binding, dict) or (
        adapter_binding.get("path"),
        adapter_binding.get("sha256"),
        adapter_binding.get("mode"),
    ) != (
        authority["profile_adapter_path"],
        authority["profile_adapter_sha256"],
        0o444,
    ):
        raise ProfileEvidenceError("prepared profile adapter binding differs")
    prepared_variants = prepared.get("variants")
    prepared_variant = (
        prepared_variants.get(variant) if isinstance(prepared_variants, dict) else None
    )
    if not isinstance(prepared_variant, dict):
        raise ProfileEvidenceError("prepared profile variant is absent")
    binary = prepared_variant.get("binary")
    contract = prepared_variant.get("contract")
    if (
        not isinstance(binary, dict)
        or not isinstance(contract, dict)
        or (binary.get("path"), binary.get("sha256"))
        != (authority["executable_path"], authority["executable_sha256"])
        or prepared_variant.get("executable_mode") != 0o555
        or prepared_variant.get("comm") != authority["executable_comm"]
        or contract.get("protocol_sha256") != PROTOCOL_SHA256
        or contract.get("profile_role_lifetime") != expected_role_lifetime
        or (contract.get("product_commit"), contract.get("product_tree"))
        != (authority["source_commit"], authority["source_tree"])
    ):
        raise ProfileEvidenceError("prepared profile binary/contract differs")
    prepared_tools = prepared.get("tools")
    if not isinstance(prepared_tools, dict):
        raise ProfileEvidenceError("prepared tool map is absent")
    for name in sorted(expected_tool_names):
        tool = _exact_mapping(
            profile_tools[name],
            ("path", "sha256", "executable_mode", "comm"),
            f"profile tool {name}",
        )
        # prepare-build copies base tools into the pinned prepared root, so the
        # source-approved tools manifest records each tool at its base-tools
        # staging path while the runtime/prepared bindings use the prepared-root
        # path.  The runner's own load_prepared reconciles this by matching the
        # approved manifest on identity only (sha256, executable_mode, comm),
        # NOT the relocatable path; do the same here.  The runtime tool binding
        # is built from prepared.tools, so full equality with prepared_tools is
        # still required (paths match there).
        approved_tool = approved_tools.get(name)
        if (
            tool != prepared_tools.get(name)
            or not isinstance(approved_tool, dict)
            or (tool["sha256"], tool["executable_mode"], tool["comm"])
            != (
                approved_tool.get("sha256"),
                approved_tool.get("executable_mode"),
                approved_tool.get("comm"),
            )
            or tool["executable_mode"] != 0o555
        ):
            raise ProfileEvidenceError(f"profile tool {name} binding differs")
        _immutable_file_snapshot(
            tool["path"], tool["sha256"], 0o555, f"prepared profile tool {name}"
        )

    adapter_path, adapter_payload, _adapter_identity = _immutable_file_snapshot(
        authority["profile_adapter_path"],
        authority["profile_adapter_sha256"],
        0o444,
        "prepared profile adapter",
    )
    _, executable_payload, _executable_identity = _immutable_file_snapshot(
        authority["executable_path"],
        authority["executable_sha256"],
        0o555,
        "prepared profile executable",
    )
    _, executed_adapter_payload, _executed_adapter_identity = _immutable_file_snapshot(
        str(Path(__file__).resolve()),
        authority["profile_adapter_sha256"],
        None,
        "executed profile adapter",
    )
    if _sha256_bytes(executed_adapter_payload) != _sha256_bytes(adapter_payload):
        raise ProfileEvidenceError("executed profile adapter bytes differ from authority")
    observed_pid, observed_comm, observed_start = reader.process_identity(pid)
    observed_exe_path, observed_exe_sha256 = reader.executable_binding(pid)
    if (
        observed_pid != pid
        or observed_comm != authority["executable_comm"]
        or observed_start != authority["child_start_ticks"]
        or observed_exe_path != authority["executable_path"]
        or observed_exe_sha256 != authority["executable_sha256"]
        or _sha256_bytes(executable_payload) != observed_exe_sha256
    ):
        raise ProfileEvidenceError("live profile process/executable identity differs")
    # Detach a canonical object so caller-owned nested dictionaries cannot mutate it.
    normalized = json.loads(canonical_json(dict(authority)))
    if not isinstance(normalized, dict):
        raise AssertionError("profile authority normalization did not produce an object")
    # Keep local names live for static analyzers: both snapshots are intentional authority.
    _ = original_prepared_path, original_approval_path, adapter_path
    return normalized


def _rich_result(
    track: str, value: object, authority_value: object
) -> Mapping[str, object]:
    result = _exact_mapping(value, _RICH_RESULT_FIELDS, "rich profile result")
    authority = _exact_mapping(authority_value, _AUTHORITY_FIELDS, "profile authority replay")
    expected_source = VARIANT_SOURCE_BINDINGS.get(str(authority.get("variant")))
    if (
        authority.get("schema") != AUTHORITY_SCHEMA
        or authority.get("protocol") != PROTOCOL
        or authority.get("protocol_sha256") != PROTOCOL_SHA256
        or authority.get("track") != track
        or expected_source is None
        or (authority.get("source_commit"), authority.get("source_tree"))
        != (expected_source["commit"], expected_source["tree"])
        or not isinstance(authority.get("attempt_nonce"), str)
        or not _SHA256_RE.fullmatch(str(authority.get("attempt_nonce")))
        or not isinstance(authority.get("context_sha256"), str)
        or _sha256_bytes(
            canonical_json(dict(_as_mapping(result["context"], "rich result context")))
        )
        != authority.get("context_sha256")
    ):
        raise ProfileEvidenceError("profile authority replay differs")
    if (
        result["schema"] != PROFILE_SCHEMA
        or result["protocol"] != PROTOCOL
        or result["track"] != track
        or result["variant"] not in ProfileCoordinator.VARIANTS
        or canonical_json(result["authority"]) != canonical_json(dict(authority))
    ):
        raise ProfileEvidenceError("rich profile result binding differs")
    context = _as_mapping(result["context"], "rich profile context")
    reopen_shape = track == "reopen" or (
        track == "structural_traces" and context.get("trace_kind") == "reopen"
    )
    expected_phases = (
        ProfileCoordinator.REOPEN_PHASES
        if reopen_shape
        else ProfileCoordinator.APPEND_PHASES
    )
    phases = result["phase_snapshots"]
    if not isinstance(phases, list) or len(phases) != len(expected_phases):
        raise ProfileEvidenceError("rich profile phase sequence differs")
    normalized_phases: list[Mapping[str, object]] = []
    for index, phase_value in enumerate(phases):
        phase = _exact_mapping(
            phase_value,
            ("phase", "pid", "process_start_ticks", "tasks"),
            f"rich profile phase[{index}]",
        )
        if (
            phase["phase"] != expected_phases[index]
            or phase["pid"] != authority["child_pid"]
            or phase["process_start_ticks"] != authority["child_start_ticks"]
            or not isinstance(phase["tasks"], list)
            or not phase["tasks"]
        ):
            raise ProfileEvidenceError("rich profile phase identity differs")
        identities: set[tuple[int, int]] = set()
        saw_main = False
        for task_index, task_value in enumerate(phase["tasks"]):
            task = _exact_mapping(
                task_value,
                ("pid", "tid", "start_ticks", "comm"),
                f"rich profile phase[{index}].tasks[{task_index}]",
            )
            pid = _json_nonnegative_integer(task["pid"], "phase task pid")
            tid = _json_nonnegative_integer(task["tid"], "phase task tid")
            start_ticks = _json_nonnegative_integer(
                task["start_ticks"], "phase task start_ticks"
            )
            comm = task["comm"]
            if (
                pid != authority["child_pid"]
                or not isinstance(comm, str)
                or not 0 < len(comm.encode()) <= 15
                or (tid, start_ticks) in identities
            ):
                raise ProfileEvidenceError("rich profile task identity differs")
            identities.add((tid, start_ticks))
            saw_main |= (
                tid == authority["child_pid"]
                and start_ticks == authority["child_start_ticks"]
                and comm == authority["executable_comm"]
            )
        if not saw_main:
            raise ProfileEvidenceError("rich profile phase lacks exact main task")
        normalized_phases.append(phase)
    process = _exact_mapping(
        result["process"],
        (
            "pid",
            "start_ticks",
            "vm_hwm_bytes",
            "voluntary_context_switches",
            "nonvoluntary_context_switches",
            "io",
        ),
        "rich profile process",
    )
    if (
        process["pid"] != authority["child_pid"]
        or process["start_ticks"] != authority["child_start_ticks"]
    ):
        raise ProfileEvidenceError("rich profile process identity differs")
    if result["unattributed_births"] != []:
        raise ProfileEvidenceError("rich profile contains unattributed post-start task births")
    if not isinstance(result["roles"], list):
        raise ProfileEvidenceError("rich profile roles are not a list")
    if track in ProfileCoordinator.ROLE_TRACKS and not result["roles"]:
        raise ProfileEvidenceError("rich profile has no role samples")
    if track not in ProfileCoordinator.ROLE_TRACKS and result["roles"]:
        raise ProfileEvidenceError("non-role profile unexpectedly bound append roles")
    return result


def _rusage_profile(inputs: Mapping[str, object]) -> RusageCounters:
    before = _exact_mapping(
        inputs["rusage_before"], ("user_ns", "system_ns"), "rusage_before"
    )
    after = _exact_mapping(
        inputs["rusage_after"], ("user_ns", "system_ns"), "rusage_after"
    )
    return rusage_delta(
        RusageCounters(
            _json_nonnegative_integer(before["user_ns"], "rusage_before.user_ns"),
            _json_nonnegative_integer(before["system_ns"], "rusage_before.system_ns"),
        ),
        RusageCounters(
            _json_nonnegative_integer(after["user_ns"], "rusage_after.user_ns"),
            _json_nonnegative_integer(after["system_ns"], "rusage_after.system_ns"),
        ),
    )


def _roles(result: Mapping[str, object]) -> list[Mapping[str, object]]:
    authority = _as_mapping(result["authority"], "rich profile authority")
    child_pid = authority["child_pid"]
    rendered: list[Mapping[str, object]] = []
    task_keys: set[tuple[int, int]] = set()
    role_task_keys: dict[str, set[tuple[int, int]]] = {}
    for index, value in enumerate(result["roles"]):
        role = _exact_mapping(
            value,
            (
                "label",
                "born_in_window",
                "on_cpu_ns",
                "voluntary_context_switches",
                "nonvoluntary_context_switches",
                "tasks",
            ),
            f"role[{index}]",
        )
        if not isinstance(role["label"], str) or not role["label"]:
            raise ProfileEvidenceError(f"role[{index}] label is invalid")
        if not isinstance(role["born_in_window"], bool):
            raise ProfileEvidenceError(f"role[{index}] born flag is invalid")
        tasks = role["tasks"]
        # The fairness spawn_blocking-publication role can legitimately bind zero
        # tasks (warm-pool reuse; see ProfileCoordinator.end); it stays present as
        # a proof-only role with born_in_window=True.  Scope the empty-task
        # tolerance to that exact case (fairness track + publication label) so
        # every other role -- and this role on the deterministic C tracks -- must
        # still have at least one task.  The birth replay below independently
        # requires role tasks to equal the re-derived ready->measured births, so
        # an empty publication is only accepted when no birth truly occurred.
        publication_may_be_empty = (
            role["label"] == "spawn_blocking-publication"
            and result.get("track") == "fairness"
        )
        if not isinstance(tasks, list) or (not tasks and not publication_may_be_empty):
            raise ProfileEvidenceError(f"role[{index}] has no tasks")
        sums = {
            "on_cpu_ns": 0,
            "voluntary_context_switches": 0,
            "nonvoluntary_context_switches": 0,
        }
        for task_index, task_value in enumerate(tasks):
            task = _exact_mapping(
                task_value,
                (
                    "identity",
                    "on_cpu_ns",
                    "voluntary_context_switches",
                    "nonvoluntary_context_switches",
                ),
                f"role[{index}].tasks[{task_index}]",
            )
            identity = _exact_mapping(
                task["identity"], ("pid", "tid", "start_ticks", "comm"), "task identity"
            )
            pid = _json_nonnegative_integer(identity["pid"], "role task pid")
            tid = _json_nonnegative_integer(identity["tid"], "role task tid")
            start_ticks = _json_nonnegative_integer(
                identity["start_ticks"], "role task start_ticks"
            )
            comm = identity["comm"]
            if (
                pid != child_pid
                or not isinstance(comm, str)
                or not 0 < len(comm.encode()) <= 15
                or (tid, start_ticks) in task_keys
            ):
                raise ProfileEvidenceError("role task identity is invalid or duplicated")
            task_keys.add((tid, start_ticks))
            for field in sums:
                sums[field] += _json_nonnegative_integer(
                    task[field], f"role[{index}].tasks[{task_index}].{field}"
                )
        for field, total in sums.items():
            if _json_nonnegative_integer(role[field], f"role[{index}].{field}") != total:
                raise ProfileEvidenceError(f"role[{index}] aggregate {field} differs")
        role_task_keys[str(role["label"])] = {
            (task["identity"]["tid"], task["identity"]["start_ticks"])
            for task in tasks
        }
        rendered.append(role)
    labels = [role["label"] for role in rendered]
    if len(labels) != len(set(labels)):
        raise ProfileEvidenceError("rich profile role labels are duplicated")
    variant = result["variant"]
    expected = {
        "A": (("owner", False),),
        "B": (("committer", False),),
        "C": (
            ("committer", False),
            ("producer-runtime", False),
            ("spawn_blocking-publication", True),
        ),
        "D": (("owner", False),),
    }[str(variant)]
    if tuple((role["label"], role["born_in_window"]) for role in rendered) != expected:
        raise ProfileEvidenceError(f"variant {variant} role set/order differs")
    phase_values = _as_mapping(
        {str(phase["phase"]): phase for phase in result["phase_snapshots"]},
        "profile phases by name",
    )
    phase_tasks: dict[str, dict[tuple[int, int], Mapping[str, object]]] = {}
    for phase_name, phase_value in phase_values.items():
        phase = _as_mapping(phase_value, f"{phase_name} phase")
        phase_tasks[phase_name] = {
            (task["tid"], task["start_ticks"]): task
            for task in phase["tasks"]
        }

    def births(before_name: str, after_name: str) -> dict[tuple[int, int], Mapping[str, object]]:
        before = phase_tasks[before_name]
        after = phase_tasks[after_name]
        before_tid = {key[0]: key[1] for key in before}
        after_tid = {key[0]: key[1] for key in after}
        reused = {
            tid
            for tid in before_tid.keys() & after_tid.keys()
            if before_tid[tid] != after_tid[tid]
        }
        if reused:
            raise ProfileEvidenceError(
                f"TID reuse across retained {before_name}->{after_name}: {sorted(reused)}"
            )
        return {key: task for key, task in after.items() if key not in before}

    ready_tasks = set(phase_tasks["ready"])
    terminal_name = "opened" if result["track"] == "reopen" else "measured"
    terminal_tasks = set(phase_tasks[terminal_name])
    for role in rendered:
        for task in role["tasks"]:
            identity = task["identity"]
            key = (identity["tid"], identity["start_ticks"])
            label = role["label"]
            # tokio worker roles tolerate a member caught mid-rename, whose comm
            # is still the process (executable) name before prctl renames it to
            # TOKIO_WORKER_COMM (see ProfileCoordinator birth groups).
            expected_comms = (
                ("mess-flat-owner",)
                if label == "owner"
                else (TOKIO_WORKER_COMM, authority["executable_comm"])
                if label in {"producer-runtime", "spawn_blocking-publication"}
                else (authority["executable_comm"],)
            )
            if identity["comm"] not in expected_comms:
                raise ProfileEvidenceError(f"role {label} comm differs")
            if label == "spawn_blocking-publication":
                if key in ready_tasks or key not in terminal_tasks:
                    raise ProfileEvidenceError("publication role birth/liveness differs")
            elif key not in ready_tasks or key not in terminal_tasks:
                raise ProfileEvidenceError(f"role {label} was not live across the window")
    terminal_births = births("ready", terminal_name)
    expected_terminal_births: set[tuple[int, int]] = set()
    if variant in {"B", "C"}:
        open_births = births("runtime", "opened")
        committer_births = {
            key: task
            for key, task in open_births.items()
            if task["comm"] not in OPEN_HELPER_COMMS
        }
        if set(committer_births) != role_task_keys["committer"] or any(
            task["comm"] != authority["executable_comm"]
            for task in committer_births.values()
        ):
            raise ProfileEvidenceError("committer runtime-to-opened birth replay differs")
        if any(
            task["comm"] not in OPEN_HELPER_COMMS
            for key, task in open_births.items()
            if key not in committer_births
        ):
            raise ProfileEvidenceError("open helper exclusion replay differs")
    if variant == "B" and births("boot", "runtime"):
        raise ProfileEvidenceError("bare variant has unbound boot-to-runtime births")
    if variant == "C":
        producer_births = births("boot", "runtime")
        tokio_worker_comms = {TOKIO_WORKER_COMM, authority["executable_comm"]}
        if (
            set(producer_births) != role_task_keys["producer-runtime"]
            or any(
                task["comm"] not in tokio_worker_comms
                for task in producer_births.values()
            )
        ):
            raise ProfileEvidenceError("producer boot-to-runtime birth replay differs")
        if (
            set(terminal_births) != role_task_keys["spawn_blocking-publication"]
            or any(
                task["comm"] not in tokio_worker_comms
                for task in terminal_births.values()
            )
        ):
            raise ProfileEvidenceError("publication ready-to-measured birth replay differs")
        expected_terminal_births = role_task_keys["spawn_blocking-publication"]
    if set(terminal_births) != expected_terminal_births:
        raise ProfileEvidenceError("unattributed retained task births differ")
    return rendered


def _critical_role(result: Mapping[str, object]) -> tuple[str, Mapping[str, object]]:
    variant = result["variant"]
    internal_label = "owner" if variant in ("A", "D") else "committer"
    output_label = "mess-flat-owner" if internal_label == "owner" else "committer"
    matches = [role for role in _roles(result) if role["label"] == internal_label]
    if len(matches) != 1:
        raise ProfileEvidenceError(
            f"variant {variant} requires one critical {internal_label} role"
        )
    tasks = matches[0]["tasks"]
    if not isinstance(tasks, list) or len(tasks) != 1:
        raise ProfileEvidenceError("serialized critical role must bind exactly one task")
    task = _as_mapping(tasks[0], "critical role task")
    identity = _as_mapping(task["identity"], "critical role identity")
    if internal_label == "owner" and identity["comm"] != "mess-flat-owner":
        raise ProfileEvidenceError("owner critical role has the wrong comm")
    return output_label, task


def _resolution(inputs: Mapping[str, object]) -> SchedstatResolution:
    value = _json_nonnegative_integer(
        inputs["schedstat_resolution_ns"], "schedstat_resolution_ns"
    )
    if value == 0:
        raise ProfileEvidenceError("schedstat_resolution_ns must be positive")
    return SchedstatResolution((0, value), value)


def _primary_profile_fields(
    result: Mapping[str, object], inputs: Mapping[str, object]
) -> dict[str, object]:
    rusage = _rusage_profile(inputs)
    role_label, task = _critical_role(result)
    identity = _as_mapping(task["identity"], "critical role identity")
    cpu_ns = _json_nonnegative_integer(task["on_cpu_ns"], "critical role cpu_ns")
    # A critical-role CPU delta below the decision floor (20 x schedstat
    # resolution) is NOT a fail-stop: it means the role is I/O-bound (e.g. an
    # owner dominated by fsync wait under durability=Process) so its CPU is below
    # the scheduler's noise floor.  The canonical evaluator is designed to treat
    # this as an INCONCLUSIVE outcome (evaluate.py primary cpu-resolution and
    # cpu-profile-resolution reasons); record serialized_role_cpu_ns and let it
    # decide rather than crashing the run before the evaluator ever sees it.
    _resolution(inputs)  # still validates schedstat_resolution_ns is positive
    return {
        "process_user_cpu_ns": rusage.user_ns,
        "process_system_cpu_ns": rusage.system_ns,
        "serialized_role": role_label,
        "serialized_role_tid": _json_nonnegative_integer(identity["tid"], "role tid"),
        "serialized_role_start_ticks": _json_nonnegative_integer(
            identity["start_ticks"], "role start_ticks"
        ),
        "serialized_role_cpu_ns": cpu_ns,
        "serialized_role_voluntary_switches": _json_nonnegative_integer(
            task["voluntary_context_switches"], "role voluntary switches"
        ),
        "serialized_role_nonvoluntary_switches": _json_nonnegative_integer(
            task["nonvoluntary_context_switches"], "role nonvoluntary switches"
        ),
    }


def _flatten_role_samples(result: Mapping[str, object]) -> list[dict[str, object]]:
    samples: list[dict[str, object]] = []
    for role in _roles(result):
        label = str(role["label"])
        if label == "owner":
            label = "mess-flat-owner"
        for task_value in role["tasks"]:
            task = _as_mapping(task_value, "role task")
            identity = _as_mapping(task["identity"], "role task identity")
            sample = {
                "role": label,
                "tid": _json_nonnegative_integer(identity["tid"], "role sample tid"),
                "start_ticks": _json_nonnegative_integer(
                    identity["start_ticks"], "role sample start_ticks"
                ),
                "cpu_ns": _json_nonnegative_integer(task["on_cpu_ns"], "role sample cpu"),
                "voluntary_switches": _json_nonnegative_integer(
                    task["voluntary_context_switches"], "role sample voluntary switches"
                ),
                "nonvoluntary_switches": _json_nonnegative_integer(
                    task["nonvoluntary_context_switches"],
                    "role sample nonvoluntary switches",
                ),
            }
            if frozenset(sample) != _ROLE_SAMPLE_FIELDS:
                raise AssertionError("internal role sample schema differs")
            samples.append(sample)
    if len({(item["role"], item["tid"], item["start_ticks"]) for item in samples}) != len(samples):
        raise ProfileEvidenceError("flattened role sample identities are duplicated")
    return samples


def _perf_profile(inputs: Mapping[str, object]) -> dict[str, object]:
    permission = inputs["perf_permission"]
    acknowledged = inputs["perf_control_acknowledged"]
    permission_status = _perf_permission_status(permission)
    if not isinstance(acknowledged, bool):
        raise ProfileEvidenceError("perf_control_acknowledged must be boolean")
    counters_value = inputs["perf_counters"]
    if not isinstance(counters_value, Sequence) or isinstance(counters_value, (str, bytes)):
        raise ProfileEvidenceError("perf_counters must be a sequence")
    counters: dict[str, Mapping[str, object]] = {}
    for index, value in enumerate(counters_value):
        if isinstance(value, PerfCounter):
            value = value.to_json()
        counter = _exact_mapping(
            value,
            ("event", "status", "value", "unit", "runtime_ns", "running_percent"),
            f"perf_counters[{index}]",
        )
        event = counter["event"]
        if not isinstance(event, str) or event not in PERF_EVENTS or event in counters:
            raise ProfileEvidenceError("perf counter event is unknown or duplicated")
        counters[event] = counter
    if set(counters) != set(PERF_EVENTS):
        raise ProfileEvidenceError("perf counter set differs from the frozen event set")
    raw_fields = (
        ("perf_stat_sha256", "perf_stat_bytes"),
        ("perf_ack_sha256", "perf_ack_bytes"),
    )
    for sha_name, bytes_name in raw_fields:
        sha = inputs[sha_name]
        if not isinstance(sha, str) or not _SHA256_RE.fullmatch(sha):
            raise ProfileEvidenceError(f"{sha_name} is malformed")
        _json_nonnegative_integer(inputs[bytes_name], bytes_name)
    available = [counter["status"] == "available" for counter in counters.values()]
    if any(available) and not all(available):
        raise ProfileEvidenceError("perf counters are partially available")
    if not all(available):
        if permission_status == "available":
            raise ProfileEvidenceError("perf counters unavailable despite available permission")
        if (
            acknowledged
            or any(counter["status"] != "not_available" for counter in counters.values())
            or any(counter["value"] is not None for counter in counters.values())
            or inputs["perf_stat_bytes"] != 0
            or inputs["perf_ack_bytes"] != 0
            or inputs["perf_stat_sha256"] != _sha256_bytes(b"")
            or inputs["perf_ack_sha256"] != _sha256_bytes(b"")
            or inputs["perf_control_events"] != []
        ):
            raise ProfileEvidenceError("unavailable perf evidence is contradictory")
        return {
            "perf_permission": permission,
            "perf_control_acknowledged": acknowledged,
            "cycles": _NOT_AVAILABLE,
            "instructions": _NOT_AVAILABLE,
            "task_clock_ns": _NOT_AVAILABLE,
        }
    if not acknowledged:
        raise ProfileEvidenceError("available perf counters lack exact control acknowledgements")
    _validate_perf_control_events(inputs["perf_control_events"])
    if (
        permission_status != "available"
        or inputs["perf_stat_bytes"] == 0
        or inputs["perf_ack_bytes"] != len(b"ack\nack\n")
        or inputs["perf_ack_sha256"] != _sha256_bytes(b"ack\nack\n")
    ):
        raise ProfileEvidenceError("available perf raw/permission authority differs")

    def count(name: str) -> int:
        value = counters[name]["value"]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ProfileEvidenceError(f"perf {name} value is not an exact integer")
        if counters[name]["unit"] != "":
            raise ProfileEvidenceError(f"perf {name} unit differs")
        return value

    task_counter = counters["task-clock"]
    task_clock = task_counter["value"]
    if (
        not isinstance(task_clock, str)
        or not re.fullmatch(r"\d+(?:\.\d+)?", task_clock)
        or task_counter["unit"] != "msec"
    ):
        raise ProfileEvidenceError("perf task-clock unit/value is invalid")
    scaled = Decimal(task_clock) * Decimal(1_000_000)
    if scaled != scaled.to_integral_value():
        raise ProfileEvidenceError("perf task-clock does not convert to exact nanoseconds")
    for counter in counters.values():
        if (
            _json_nonnegative_integer(counter["runtime_ns"], "perf runtime") == 0
            or not isinstance(counter["running_percent"], str)
            or not re.fullmatch(r"\d+(?:\.\d+)?", counter["running_percent"])
            or not Decimal(0) < Decimal(counter["running_percent"]) <= Decimal(100)
        ):
            raise ProfileEvidenceError("perf runtime/running percentage differs")
    count("context-switches")
    return {
        "perf_permission": permission,
        "perf_control_acknowledged": acknowledged,
        "cycles": count("cycles"),
        "instructions": count("instructions"),
        "task_clock_ns": int(scaled),
    }


def _replay_perf_inputs(inputs: Mapping[str, object]) -> dict[str, object]:
    permission = inputs["perf_permission"]
    status = _perf_permission_status(permission)
    events = inputs["perf_control_events"]
    artifacts = _as_mapping(inputs["perf_raw_artifacts"], "perf raw artifacts")
    if status == "not_available":
        if artifacts or events != []:
            raise ProfileEvidenceError("unavailable perf has raw artifacts/control events")
        return perf_profile_inputs("", "", str(permission), control_events=[])
    if set(artifacts) != {"stat", "ack"}:
        raise ProfileEvidenceError("available perf raw artifact set differs")
    _, stat_payload = _raw_artifact_snapshot(artifacts["stat"], "perf stat artifact")
    _, ack_payload = _raw_artifact_snapshot(artifacts["ack"], "perf ACK artifact")
    try:
        stat_text = stat_payload.decode()
        ack_text = ack_payload.decode()
    except UnicodeDecodeError as error:
        raise ProfileEvidenceError("perf raw artifact is not UTF-8") from error
    normalized = perf_profile_inputs(
        stat_text,
        ack_text,
        str(permission),
        control_events=events,
    )
    if (
        normalized["perf_stat_sha256"] != _sha256_bytes(stat_payload)
        or normalized["perf_stat_bytes"] != len(stat_payload)
        or normalized["perf_ack_sha256"] != _sha256_bytes(ack_payload)
        or normalized["perf_ack_bytes"] != len(ack_payload)
    ):
        raise AssertionError("perf raw replay lost its artifact binding")
    return normalized


def _replay_trace_inputs(
    inputs: Mapping[str, object], boundary: Mapping[str, object]
) -> dict[str, object]:
    # The runner snapshots the trace with no size limit and a busy interval can
    # exceed the default 16 MiB cap.  Lift the cap for the trace artifact only;
    # the sha256/byte-length binding below remains the integrity authority.
    _, payload = _raw_artifact_snapshot(
        inputs["trace_raw_artifact"], "strace raw artifact", limit=None
    )
    try:
        text = payload.decode()
    except UnicodeDecodeError as error:
        raise ProfileEvidenceError("strace raw artifact is not UTF-8") from error
    normalized = strace_profile_inputs(
        text,
        boundary,
        log_path_markers=inputs["log_path_markers"],
        metadata_path_markers=inputs["metadata_path_markers"],
    )
    if (
        normalized["trace_raw_sha256"] != _sha256_bytes(payload)
        or normalized["trace_raw_bytes"] != len(payload)
    ):
        raise AssertionError("strace raw replay lost its artifact binding")
    return normalized


def _trace_inputs(inputs: Mapping[str, object]) -> tuple[int, int, dict[str, object]]:
    begin = _json_nonnegative_integer(inputs["begin_markers"], "begin_markers")
    end = _json_nonnegative_integer(inputs["end_markers"], "end_markers")
    if begin != 1 or end != 1:
        raise ProfileEvidenceError("trace marker cardinality is not exact")
    raw_sha256 = inputs["trace_raw_sha256"]
    boundary_sha256 = inputs["trace_boundary_sha256"]
    if (
        not isinstance(raw_sha256, str)
        or not _SHA256_RE.fullmatch(raw_sha256)
        or not isinstance(boundary_sha256, str)
        or not _SHA256_RE.fullmatch(boundary_sha256)
        or _json_nonnegative_integer(inputs["trace_raw_bytes"], "trace_raw_bytes") == 0
        or _json_nonnegative_integer(
            inputs["trace_interval_lines"], "trace_interval_lines"
        )
        == 0
    ):
        raise ProfileEvidenceError("trace raw/boundary provenance is invalid")
    raw_counts = _as_mapping(inputs["trace_counts"], "trace_counts")
    allowed = frozenset(SYSCALL_EVENTS) | _TRACE_AGGREGATES
    if not set(raw_counts).issubset(allowed):
        raise ProfileEvidenceError(
            f"trace_counts has unknown fields: {sorted(set(raw_counts) - allowed)}"
        )
    counts: dict[str, object] = {}
    for name, value in raw_counts.items():
        if value == _NOT_AVAILABLE:
            counts[name] = value
        else:
            counts[name] = _json_nonnegative_integer(value, f"trace_counts.{name}")
    return begin, end, counts


def _syscall_profile_fields(inputs: Mapping[str, object]) -> dict[str, object]:
    begin, end, counts = _trace_inputs(inputs)
    for aggregate in ("file_create", "file_rename", "file_unlink"):
        if aggregate not in counts or counts[aggregate] == _NOT_AVAILABLE:
            raise ProfileEvidenceError(f"syscall profile lacks exact {aggregate} total")
    return {
        "begin_markers": begin,
        "end_markers": end,
        **{name: int(counts.get(name, 0)) for name in (
            "write",
            "pwrite64",
            "writev",
            "pwritev",
            "pwritev2",
            "fsync",
            "fdatasync",
            "futex",
        )},
        "file_create": int(counts["file_create"]),
        "file_rename": int(counts["file_rename"]),
        "file_unlink": int(counts["file_unlink"]),
    }


def _structural_profile_fields(
    raw_point: Mapping[str, object], inputs: Mapping[str, object]
) -> dict[str, object]:
    begin, end, counts = _trace_inputs(inputs)
    trace_kind = raw_point.get("trace_kind")
    if trace_kind not in ("new_names", "reopen"):
        raise ProfileEvidenceError("structural trace_kind is invalid")
    for aggregate in ("log_sync_calls", "metadata_sync_calls"):
        if aggregate not in counts or counts[aggregate] == _NOT_AVAILABLE:
            raise ProfileEvidenceError(f"structural trace lacks exact {aggregate}")
    write_like = sum(
        int(counts.get(name, 0))
        for name in ("write", "pwrite64", "writev", "pwritev", "pwritev2")
    )
    sync_family = sum(int(counts.get(name, 0)) for name in ("fsync", "fdatasync"))
    result: dict[str, object] = {
        "begin_markers": begin,
        "end_markers": end,
        "write_like_calls": write_like,
        "sync_family_calls": sync_family,
        "log_sync_calls": int(counts["log_sync_calls"]),
        "metadata_sync_calls": int(counts["metadata_sync_calls"]),
    }
    if trace_kind == "new_names":
        result.update(
            {
                "openat": _NOT_AVAILABLE,
                "getdents64": _NOT_AVAILABLE,
                "read": _NOT_AVAILABLE,
                "pread64": _NOT_AVAILABLE,
                "files_opened": _NOT_AVAILABLE,
            }
        )
    else:
        if "files_opened" not in counts or counts["files_opened"] == _NOT_AVAILABLE:
            raise ProfileEvidenceError("reopen trace lacks exact files_opened")
        result.update(
            {
                "openat": int(counts.get("openat", 0)),
                "getdents64": int(counts.get("getdents64", 0)),
                "read": int(counts.get("read", 0)),
                "pread64": int(counts.get("pread64", 0)),
                "files_opened": int(counts["files_opened"]),
            }
        )
    return result


def _control_timestamp(event: Mapping[str, object]) -> int:
    names = (
        "_runner_received_monotonic_ns",
        "_runner_sent_monotonic_ns",
    )
    present = [name for name in names if name in event]
    if len(present) != 1:
        raise ProfileEvidenceError("control event lacks one runner timestamp")
    return _json_nonnegative_integer(event[present[0]], present[0])


def _validate_control_events(
    events_value: object,
    *,
    authority: Mapping[str, object],
    reopen_shape: bool,
) -> tuple[
    Mapping[str, object],
    Mapping[str, object],
    Mapping[str, object],
    Mapping[str, object],
]:
    if not isinstance(events_value, Sequence) or isinstance(events_value, (str, bytes)):
        raise ProfileEvidenceError("control_events must be a sequence")
    events = list(events_value)
    expected_kinds = (
        (
            ("child", "boot"),
            ("continue", "boot"),
            ("child", "runtime"),
            ("continue", "runtime"),
            ("child", "ready"),
            ("start", None),
            ("child", "opened"),
            ("continue", "opened"),
            ("child", "measured"),
            ("release", None),
        )
        if reopen_shape
        else (
            ("child", "boot"),
            ("continue", "boot"),
            ("child", "runtime"),
            ("continue", "runtime"),
            ("child", "opened"),
            ("continue", "opened"),
            ("child", "ready"),
            ("start", None),
            ("child", "measured"),
            ("release", None),
        )
    )
    if len(events) != len(expected_kinds):
        raise ProfileEvidenceError("control event cardinality differs")
    normalized: list[Mapping[str, object]] = []
    timestamps: list[int] = []
    by_phase: dict[str, Mapping[str, object]] = {}
    previous_nonce: str | None = None
    start_nonce: str | None = None
    start_event: Mapping[str, object] | None = None
    release_event: Mapping[str, object] | None = None
    for index, (value, (kind, phase)) in enumerate(zip(events, expected_kinds, strict=True)):
        event = _as_mapping(value, f"control event[{index}]")
        try:
            rendered_event = canonical_json(dict(event))
            round_tripped_event = canonical_json(json.loads(rendered_event))
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise ProfileEvidenceError(
                f"control event[{index}] is not canonical JSON"
            ) from error
        if rendered_event != round_tripped_event:
            raise ProfileEvidenceError(f"control event[{index}] is not canonical JSON")
        if kind == "child":
            if phase == "boot":
                expected_fields = {
                    "context_sha256",
                    "phase",
                    "protocol_sha256",
                    "variant",
                    "_runner_received_monotonic_ns",
                }
            elif phase == "ready":
                expected_fields = {
                    "allocated_bytes_start",
                    "allocation_calls_start",
                    "context_sha256",
                    "counter_start_monotonic_ns",
                    "nonce",
                    "phase",
                    "process_system_cpu_start_ns",
                    "process_user_cpu_start_ns",
                    "protocol_sha256",
                    "ready_monotonic_ns",
                    "variant",
                    "_runner_received_monotonic_ns",
                }
            elif phase == "opened" and reopen_shape:
                expected_fields = {
                    "nonce",
                    "opened_monotonic_ns",
                    "open_start_monotonic_ns",
                    "phase",
                    "_runner_received_monotonic_ns",
                }
            elif phase == "measured":
                expected_fields = {
                    "allocated_bytes_end",
                    "allocation_calls_end",
                    "counter_end_monotonic_ns",
                    "last_completion_monotonic_ns",
                    "nonce",
                    "phase",
                    "process_system_cpu_end_ns",
                    "process_user_cpu_end_ns",
                    "release_monotonic_ns",
                    "t0_monotonic_ns",
                    "t1_monotonic_ns",
                    "_runner_received_monotonic_ns",
                }
                if authority["track"] == "cpu_profiles":
                    expected_fields.add("perf_disable")
            else:
                expected_fields = {"nonce", "phase", "_runner_received_monotonic_ns"}
            if set(event) != expected_fields or event.get("phase") != phase:
                raise ProfileEvidenceError(f"control child event {phase} fields differ")
            if phase in {"boot", "ready"} and (
                event.get("context_sha256") != authority["context_sha256"]
                or event.get("protocol_sha256") != PROTOCOL_SHA256
                or event.get("variant") != authority["variant"]
            ):
                raise ProfileEvidenceError(f"control child event {phase} authority differs")
            if phase != "boot":
                nonce = event.get("nonce")
                if not isinstance(nonce, str) or not _SHA256_RE.fullmatch(nonce):
                    raise ProfileEvidenceError(f"control child event {phase} nonce invalid")
                expected_nonce = start_nonce if phase == "measured" and not reopen_shape else previous_nonce
                if nonce != expected_nonce:
                    raise ProfileEvidenceError(f"control child event {phase} nonce differs")
            by_phase[str(phase)] = event
        elif kind == "continue":
            if set(event) != {"command", "phase", "nonce", "_runner_sent_monotonic_ns"}:
                raise ProfileEvidenceError("control continue fields differ")
            nonce = event.get("nonce")
            if (
                event.get("command") != "continue"
                or event.get("phase") != phase
                or not isinstance(nonce, str)
                or not _SHA256_RE.fullmatch(nonce)
            ):
                raise ProfileEvidenceError("control continue identity differs")
            previous_nonce = nonce
        else:
            if set(event) != {"command", "nonce", "_runner_sent_monotonic_ns"}:
                raise ProfileEvidenceError(f"control {kind} fields differ")
            nonce = event.get("nonce")
            if (
                event.get("command") != kind
                or not isinstance(nonce, str)
                or not _SHA256_RE.fullmatch(nonce)
            ):
                raise ProfileEvidenceError(f"control {kind} identity differs")
            if kind == "start":
                start_nonce = nonce
                previous_nonce = nonce
                start_event = event
            elif nonce != previous_nonce:
                raise ProfileEvidenceError("control release nonce differs")
            else:
                release_event = event
        timestamps.append(_control_timestamp(event))
        normalized.append(event)
    if any(after <= before for before, after in zip(timestamps, timestamps[1:])):
        raise ProfileEvidenceError("control runner timestamps are not strictly ordered")
    ready = by_phase["ready"]
    measured = by_phase["measured"]
    if start_event is None or release_event is None:
        raise ProfileEvidenceError("control start/release event is absent")
    for name in (
        "allocated_bytes_start",
        "allocation_calls_start",
        "counter_start_monotonic_ns",
        "process_system_cpu_start_ns",
        "process_user_cpu_start_ns",
        "ready_monotonic_ns",
    ):
        _json_nonnegative_integer(ready[name], f"ready.{name}")
    for name in (
        "allocated_bytes_end",
        "allocation_calls_end",
        "counter_end_monotonic_ns",
        "last_completion_monotonic_ns",
        "process_system_cpu_end_ns",
        "process_user_cpu_end_ns",
        "release_monotonic_ns",
        "t0_monotonic_ns",
        "t1_monotonic_ns",
    ):
        _json_nonnegative_integer(measured[name], f"measured.{name}")
    if measured.get("nonce") != start_nonce and not reopen_shape:
        raise ProfileEvidenceError("measured/start nonce differs")
    ready_monotonic = _json_nonnegative_integer(
        ready["ready_monotonic_ns"], "ready.ready_monotonic_ns"
    )
    counter_start = _json_nonnegative_integer(
        ready["counter_start_monotonic_ns"], "ready.counter_start_monotonic_ns"
    )
    ready_received = _control_timestamp(ready)
    start_sent = _control_timestamp(start_event)
    t0 = _json_nonnegative_integer(measured["t0_monotonic_ns"], "measured.t0")
    last_completion = _json_nonnegative_integer(
        measured["last_completion_monotonic_ns"], "measured.last_completion"
    )
    t1 = _json_nonnegative_integer(measured["t1_monotonic_ns"], "measured.t1")
    counter_end = _json_nonnegative_integer(
        measured["counter_end_monotonic_ns"], "measured.counter_end"
    )
    measured_received = _control_timestamp(measured)
    release_sent = _control_timestamp(release_event)
    if not (
        ready_monotonic
        <= counter_start
        <= ready_received
        < start_sent
        <= t0
        <= last_completion
        <= t1
        <= counter_end
        <= measured_received
        < release_sent
    ):
        raise ProfileEvidenceError("control lifecycle monotonic bounds differ")
    for start_name, end_name in (
        ("allocated_bytes_start", "allocated_bytes_end"),
        ("allocation_calls_start", "allocation_calls_end"),
        ("process_system_cpu_start_ns", "process_system_cpu_end_ns"),
        ("process_user_cpu_start_ns", "process_user_cpu_end_ns"),
    ):
        if _json_nonnegative_integer(
            measured[end_name], f"measured.{end_name}"
        ) < _json_nonnegative_integer(ready[start_name], f"ready.{start_name}"):
            raise ProfileEvidenceError(f"control lifecycle counter {end_name} rolled back")
    if not reopen_shape:
        release_monotonic = _json_nonnegative_integer(
            measured["release_monotonic_ns"], "measured.release_monotonic_ns"
        )
        # The child records t0 (main.rs) immediately before release_monotonic,
        # then the writers complete: t0 <= release <= last_completion.  This
        # matches the canonical monotone phase order in evidence_schema
        # (ready <= counter_start <= t0 <= release <= last_completion <= t1).
        # (The prior bound start_sent <= release <= t0 had release and t0
        # swapped and fail-stopped every completed primary cell.)
        if not t0 <= release_monotonic <= last_completion:
            raise ProfileEvidenceError("control t0/release/completion ordering differs")
    else:
        opened = by_phase["opened"]
        open_start = _json_nonnegative_integer(
            opened["open_start_monotonic_ns"], "opened.open_start"
        )
        open_end = _json_nonnegative_integer(
            opened["opened_monotonic_ns"], "opened.opened"
        )
        if not start_sent <= open_start <= open_end <= _control_timestamp(opened):
            raise ProfileEvidenceError("reopen lifecycle bounds differ")
    if authority["track"] == "cpu_profiles":
        disable = measured["perf_disable"]
        if disable is not None:
            _validate_perf_control_event(disable, "disable", 1)
    return ready, measured, start_event, release_event


def profile_fields(
    track: str,
    finish_result: Mapping[str, object] | None,
    *,
    raw_point: Mapping[str, object],
    control_events: Sequence[Mapping[str, object]],
    authority: Mapping[str, object],
    profile_inputs: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Flatten retained lifecycle/tool evidence into evaluator-owned fields.

    The runner persists ``finish_result`` and every ``profile_inputs`` source
    before calling this function.  This boundary deliberately refuses partial
    dictionaries: each track has one exact input shape and one exact output
    shape, so adding a counter requires a reviewed schema change on both sides.
    """

    expected_inputs = {
        "primary": ("schedstat_resolution_ns",),
        "new_names": ("schedstat_resolution_ns",),
        "fairness": ("schedstat_resolution_ns",),
        "reopen": (),
        "cpu_profiles": (
            "schedstat_resolution_ns",
            "perf_permission",
            "perf_control_events",
            "perf_raw_artifacts",
        ),
        "syscall_profiles": (
            "trace_raw_artifact",
            "log_path_markers",
            "metadata_path_markers",
        ),
        "structural_traces": (
            "trace_raw_artifact",
            "log_path_markers",
            "metadata_path_markers",
        ),
    }
    if track not in expected_inputs:
        raise ProfileEvidenceError(f"unknown profile field track {track!r}")
    point = _as_mapping(raw_point, "raw_point")
    if point.get("track") != track:
        raise ProfileEvidenceError("raw point track differs from profile track")
    authority_mapping = _exact_mapping(authority, _AUTHORITY_FIELDS, "profile authority")
    inputs = _exact_mapping(
        {} if profile_inputs is None else profile_inputs,
        expected_inputs[track],
        f"{track} profile_inputs",
    )
    rich = _rich_result(track, finish_result, authority_mapping)
    if point.get("variant") != rich["variant"]:
        raise ProfileEvidenceError("raw point and rich profile variants differ")
    context = _as_mapping(rich["context"], "rich profile context")
    reopen_shape = track == "reopen" or (
        track == "structural_traces" and context.get("trace_kind") == "reopen"
    )
    ready_event, measured_event, start_event, _release_event = _validate_control_events(
        control_events,
        authority=authority_mapping,
        reopen_shape=reopen_shape,
    )
    measured_inputs = {
        **dict(inputs),
        "rusage_before": {
            "user_ns": ready_event["process_user_cpu_start_ns"],
            "system_ns": ready_event["process_system_cpu_start_ns"],
        },
        "rusage_after": {
            "user_ns": measured_event["process_user_cpu_end_ns"],
            "system_ns": measured_event["process_system_cpu_end_ns"],
        },
    }
    replayed_trace_inputs: Mapping[str, object] | None = None
    replayed_perf_inputs: Mapping[str, object] | None = None
    if track in {"syscall_profiles", "structural_traces"}:
        expected_markers = context.get("trace_path_markers")
        if expected_markers is None:
            expected_markers = context.get("variant_trace_path_markers")
        marker_authority = _exact_mapping(
            expected_markers,
            ("log", "metadata"),
            "trace path marker authority",
        )
        input_log_markers = _trace_path_markers(inputs["log_path_markers"], "log")
        input_metadata_markers = _trace_path_markers(
            inputs["metadata_path_markers"], "metadata"
        )
        authority_log_markers = _trace_path_markers(marker_authority["log"], "log")
        authority_metadata_markers = _trace_path_markers(
            marker_authority["metadata"], "metadata"
        )
        if (
            input_log_markers != authority_log_markers
            or input_metadata_markers != authority_metadata_markers
        ):
            raise ProfileEvidenceError("trace path marker/context binding differs")
        boundary = {
            "child_pid": authority_mapping["child_pid"],
            "control_fd": authority_mapping["control_fd"],
            "begin_event": dict(ready_event),
            "end_event": dict(measured_event),
        }
        replayed_trace_inputs = _replay_trace_inputs(inputs, boundary)
        if replayed_trace_inputs["trace_boundary_sha256"] != _sha256_bytes(
            canonical_json(boundary)
        ):
            raise ProfileEvidenceError("trace boundary/control-event binding differs")
    elif track == "cpu_profiles":
        if inputs["perf_permission"] != authority_mapping["perf_permission_result"]:
            raise ProfileEvidenceError("perf permission/profile authority differs")
        replayed_perf_inputs = _replay_perf_inputs(inputs)
        if inputs["perf_control_events"] == []:
            if measured_event["perf_disable"] is not None:
                raise ProfileEvidenceError("unavailable perf has a child disable event")
        else:
            perf_enable, perf_disable = _validate_perf_control_events(
                inputs["perf_control_events"]
            )
            if canonical_json(perf_disable) != canonical_json(measured_event["perf_disable"]):
                raise ProfileEvidenceError("child/ledger perf disable event differs")
            start_sent = _control_timestamp(start_event)
            ready_received = _control_timestamp(ready_event)
            measured_received = _control_timestamp(measured_event)
            t1 = _json_nonnegative_integer(
                measured_event["t1_monotonic_ns"], "measured t1"
            )
            counter_end = _json_nonnegative_integer(
                measured_event["counter_end_monotonic_ns"], "measured counter end"
            )
            if (
                perf_enable["nonce"] != start_event["nonce"]
                or perf_disable["nonce"] != start_event["nonce"]
                or not (
                    ready_received
                    <= perf_enable["sent_monotonic_ns"]
                    < perf_enable["ack_received_monotonic_ns"]
                    < start_sent
                )
                or not (
                    t1
                    <= perf_disable["sent_monotonic_ns"]
                    < perf_disable["ack_received_monotonic_ns"]
                    <= counter_end
                    <= measured_received
                )
            ):
                raise ProfileEvidenceError("perf control/lifecycle binding differs")

    if track in ("primary", "new_names", "fairness"):
        result = _primary_profile_fields(rich, measured_inputs)
    elif track == "reopen":
        process = _exact_mapping(
            rich["process"],
            (
                "pid",
                "start_ticks",
                "vm_hwm_bytes",
                "voluntary_context_switches",
                "nonvoluntary_context_switches",
                "io",
            ),
            "reopen process",
        )
        io = _exact_mapping(process["io"], IO_FIELDS, "reopen process io")
        result = {
            "peak_rss_bytes": _json_nonnegative_integer(
                process["vm_hwm_bytes"], "reopen peak RSS"
            ),
            "proc_read_bytes": _json_nonnegative_integer(io["read_bytes"], "proc read bytes"),
            "proc_write_bytes": _json_nonnegative_integer(
                io["write_bytes"], "proc write bytes"
            ),
            "proc_read_syscalls": _json_nonnegative_integer(io["syscr"], "proc read syscalls"),
            "proc_write_syscalls": _json_nonnegative_integer(
                io["syscw"], "proc write syscalls"
            ),
        }
    elif track == "cpu_profiles":
        if replayed_perf_inputs is None:
            raise AssertionError("CPU profile raw evidence was not replayed")
        rusage = _rusage_profile(measured_inputs)
        # Validate the critical-role structure but do NOT gate on its CPU delta:
        # a below-decision-floor role CPU is an INCONCLUSIVE outcome the evaluator
        # handles (cpu-profile-resolution), not a fail-stop.  The role CPU is
        # recorded in role_samples_json below.
        _critical_role(rich)
        process = _as_mapping(rich["process"], "CPU profile process")
        result = {
            **_perf_profile(replayed_perf_inputs),
            "process_user_cpu_ns": rusage.user_ns,
            "process_system_cpu_ns": rusage.system_ns,
            "process_voluntary_switches": _json_nonnegative_integer(
                process["voluntary_context_switches"], "process voluntary switches"
            ),
            "process_nonvoluntary_switches": _json_nonnegative_integer(
                process["nonvoluntary_context_switches"],
                "process nonvoluntary switches",
            ),
            "schedstat_resolution_ns": _resolution(
                measured_inputs
            ).minimum_nonzero_increment_ns,
            "role_samples_json": _flatten_role_samples(rich),
        }
    elif track == "syscall_profiles":
        if replayed_trace_inputs is None:
            raise AssertionError("syscall raw evidence was not replayed")
        result = _syscall_profile_fields(replayed_trace_inputs)
    else:
        if replayed_trace_inputs is None:
            raise AssertionError("structural raw evidence was not replayed")
        result = _structural_profile_fields(point, replayed_trace_inputs)

    # Reject NaN, infinity, non-string keys, or adapter objects before handing
    # the result to the runner/schema boundary.
    detached = json.loads(canonical_json(result))
    if not isinstance(detached, dict):
        raise AssertionError("profile field adapter did not produce an object")
    return detached


def _canonical_type_self_test() -> None:
    """Prove reviewed UTF-8 and builder-local ASCII forms stay disjoint."""

    value = {
        "label": "A — current",
        "schema": "bn-ecm1-profile-canonical-type-self-test-v1",
    }
    reviewed = canonical_json(value)
    local = _local_canonical_json(value)
    if (
        reviewed == local
        or b"\xe2\x80\x94" not in reviewed
        or b"\\u2014" in reviewed
        or b"\\u2014" not in local
        or b"\xe2\x80\x94" in local
        or _canonical_json_payload(reviewed, "reviewed UTF-8 self-test") != value
        or _local_canonical_json_payload(local, "local ASCII self-test") != value
    ):
        raise AssertionError("profile canonical type self-test differs")
    for payload, parser, context in (
        (local, _canonical_json_payload, "ASCII-escaped reviewed hostile"),
        (reviewed, _local_canonical_json_payload, "UTF-8 local hostile"),
    ):
        try:
            parser(payload, context)
        except ProfileEvidenceError:
            pass
        else:
            raise AssertionError(f"{context} was accepted")


def main() -> int:
    _canonical_type_self_test()
    print(canonical_json(profile_contract()).decode(), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
