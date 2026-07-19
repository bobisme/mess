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
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Callable, Iterable, Mapping, Sequence


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
TOKIO_WORKER_COMM = "tokio-runtime-w"
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
)


class ProfileEvidenceError(ValueError):
    """An observation cannot enter evidence without ambiguity."""


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
_SEMANTIC_INPUT_AUTHORITY_SCHEMA = "bn-ecm1-semantic-input-authority-v1"
_RECURSIVE_TREE_AUTHORITY_SCHEMA = "bn-ecm1-recursive-tree-authority-v1"
_TRUSTED_SYSTEM_CLOSURE_SCHEMA = "bn-ecm1-trusted-system-closure-v1"
_CURRENT_CHILDREN_ATTESTATION_SCHEMA = "bn-ecm1-current-children-build-v2"
_LOCK_CANDIDATES_SCHEMA = "asterism-rebaseline-lock-candidates-v3"
_GUEST_ROOT = "/asterism"
_GUEST_SOURCE = f"{_GUEST_ROOT}/source"
_GUEST_TARGET = f"{_GUEST_ROOT}/target"
_GUEST_TOOLCHAIN_ROOT = f"{_GUEST_ROOT}/toolchain"
_GUEST_TOOLCHAIN_BIN = f"{_GUEST_TOOLCHAIN_ROOT}/bin"
_GUEST_CARGO = f"{_GUEST_TOOLCHAIN_ROOT}/bin/cargo"
_GUEST_RUSTC = f"{_GUEST_TOOLCHAIN_ROOT}/bin/rustc"
_GUEST_CARGO_HOME = f"{_GUEST_ROOT}/cargo-home"
_GUEST_BOUND_CONFIG_PATHS = (
    f"{_GUEST_SOURCE}/.cargo/config.toml",
    f"{_GUEST_SOURCE}/.cargo/config",
    f"{_GUEST_CARGO_HOME}/config.toml",
    f"{_GUEST_CARGO_HOME}/config",
)
_CARGO_CONFIG_SEARCH_SCHEMA = "asterism-rebaseline-cargo-config-search-v3"
_EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
_TRUSTED_SYSTEM_MOUNTS = (
    (Path("/usr/bin"), "/usr/bin"),
    (Path("/usr/lib"), "/usr/lib"),
    (Path("/usr/include"), "/usr/include"),
)
_TRUSTED_SYSTEM_OWNER_UID = 0
_RELEASE_BUILD_ENVIRONMENT_FIELDS = frozenset(
    (
        "ASTERISM_BUILD_ADAPTER_SHA256",
        "ASTERISM_BUILD_BINARY_KIND",
        "ASTERISM_BUILD_NONCE",
        "ASTERISM_BUILD_CARGO_LOCK_SHA256",
        "ASTERISM_BUILD_PRODUCT_COMMIT",
        "ASTERISM_BUILD_PRODUCT_TREE",
        "ASTERISM_BUILD_PROTOCOL",
        "ASTERISM_BUILD_PROTOCOL_SHA256",
        "ASTERISM_BUILD_SHARED_MANIFEST_SHA256",
        "ASTERISM_BUILD_SOURCE_APPROVAL_SHA256",
        "ASTERISM_BUILD_TIMED_SURFACE",
        "ASTERISM_BUILD_TOOLING_COMMIT",
        "ASTERISM_BUILD_TOOLING_TREE",
        "ASTERISM_BUILD_VARIANT",
    )
)
_TOOLCHAIN_FIELDS = (
    "bwrap_path",
    "bwrap_sha256",
    "cargo_home_path",
    "cargo_path",
    "cargo_sha256",
    "cargo_version_verbose",
    "git_path",
    "git_sha256",
    "rustc_path",
    "rustc_sha256",
    "rustc_version_verbose",
    "rustc_host",
    "rust_lld_path",
    "rust_lld_sha256",
    "rustup_home_path",
    "rustup_path",
    "rustup_sha256",
    "rustup_toolchain",
)
_CURRENT_BUILD_FIELDS = (
    "argv",
    "environment",
    "execution",
    "filesystem_admission",
    "cargo_config_prebuild",
    "cargo_config_postbuild",
    "execution_tools",
    "artifacts",
    "binds",
    "lock_prebuild",
    "lock_postbuild",
    "source_manifest_sha256",
    "semantic_input_authority",
    "toolchain_manifest",
    "target",
    "target_was_absent",
)
_CURRENT_CHILD_WRAPPER_FIELDS = (
    "wrapper_receipt",
    "wrapper_receipt_identity",
    "wrapper_receipt_sha256",
    "wrapper_input_identity",
)
_CURRENT_CARGO_CONFIG_SCHEMA = "bn-30fs-build-cargo-config-search-v1"
_CURRENT_WRAPPER_RECEIPT_SCHEMA = "bn-30fs-rustc-workspace-wrapper-receipt-v1"
_CURRENT_WRAPPER_ARGUMENTS = (
    "--cfg",
    "test",
    "--allow",
    "explicit_builtin_cfgs_in_flags",
    "--cfg",
    "asterism_rebaseline_correctness",
    "--check-cfg",
    "cfg(asterism_rebaseline_correctness)",
)
_CURRENT_FAULT_COMPILE_OUT_SCHEMA = "bn-2l3n-fault-compile-out-authority-v1"
_CURRENT_EXPECTED_LIB_SOURCE = "crates/mess-store/src/lib.rs"
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
_RELEASE_COMPILE_OUT_BUILD_CHILD_FIELDS = (
    *_RELEASE_COMPILE_OUT_NM_CHILD_FIELDS,
    "passed_file_descriptors",
)
_PREPARED_VARIANT_FIELDS = (
    "contract",
    "binary",
    "executable_mode",
    "artifact_root",
    "contract_argv",
    "contract_env",
    "comm",
    "evidence_argv",
    "evidence_env",
    "trace_path_marker_templates",
    "correctness_oracle_mode",
    "attestation",
)
_PREPARED_ATTESTATION_FIELDS = (
    "source_commit",
    "source_tree",
    "source_archive_path",
    "source_archive_sha256",
    "source_archive_bytes",
    "archive_manifest_path",
    "archive_manifest_sha256",
    "overlay_manifest_path",
    "overlay_manifest_sha256",
    "materialized_root",
    "materialized_manifest_path",
    "materialized_manifest_sha256",
    "materialized_manifest_pre_sha256",
    "materialized_manifest_post_sha256",
    "source_read_only",
    "cargo_lock_path",
    "cargo_lock_sha256",
    "cargo_lock_pre_sha256",
    "cargo_lock_post_sha256",
    "target_dir",
    "target_dir_was_absent",
    "build_nonce",
    "toolchain",
    "build_argv",
    "build_env",
    "cargo_config_search",
    "execution_tools",
    "semantic_input_authority",
    "build_started_at",
    "build_started_monotonic_ns",
    "build_completed_at",
    "build_completed_monotonic_ns",
    "build_log_path",
    "build_log_sha256",
    "build_child",
    "contract_output_path",
    "contract_output_sha256",
    "contract_child",
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
    "db060c902d7d1a2664dcaea44525adac727b1bef1de32bb01b33be2561143a39"
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
) -> tuple[Path, bytes, tuple[int, int, int, int, int, int, int]]:
    if not isinstance(claimed_sha256, str) or not _SHA256_RE.fullmatch(claimed_sha256):
        raise ProfileEvidenceError(f"{context} SHA-256 is malformed")
    path, payload, identity = _immutable_file_payload(
        path_value, expected_mode, context
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


def _canonical_json_snapshot(
    path_value: object,
    claimed_sha256: object,
    context: str,
) -> tuple[Path, dict[str, object]]:
    path, payload, _identity = _immutable_file_snapshot(
        path_value, claimed_sha256, 0o444, context
    )
    return path, _canonical_json_payload(payload, context)


def _unclaimed_local_canonical_json_snapshot(
    path_value: object, context: str
) -> tuple[Path, bytes, dict[str, object]]:
    path, payload, _identity = _immutable_file_payload(path_value, 0o444, context)
    return path, payload, _local_canonical_json_payload(payload, context)


def _raw_artifact_snapshot(value: object, context: str) -> tuple[Path, bytes]:
    binding = _exact_mapping(
        value,
        ("path", "sha256", "bytes", "mode"),
        f"{context} binding",
    )
    if binding["mode"] != 0o444:
        raise ProfileEvidenceError(f"{context} mode authority differs")
    path, payload, _identity = _immutable_file_snapshot(
        binding["path"], binding["sha256"], 0o444, context
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

    def tasks(self, pid: int) -> tuple[TaskIdentity, ...]:
        directory = self._open_directory(str(pid), "task")
        try:
            entries = os.listdir(directory)
        except OSError as error:
            raise ProfileEvidenceError(f"cannot enumerate task directory: {error}") from error
        finally:
            os.close(directory)
        if any(not item.isdigit() for item in entries):
            raise ProfileEvidenceError("task directory has a nonnumeric entry")
        tids = sorted(int(item) for item in entries)
        if not tids:
            raise ProfileEvidenceError(f"no task identities for pid {pid}")
        identities = tuple(self.task_identity(pid, tid) for tid in tids)
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
    # VmHWM is a peak since process birth, not an interval counter.  Retain the
    # post-window absolute peak and reject impossible rollback.
    if after.vm_hwm_bytes < before.vm_hwm_bytes:
        raise ProfileEvidenceError("VmHWM rolled back within one process identity")
    return ProcessDelta(
        pid=before.pid,
        start_ticks=before.start_ticks,
        vm_hwm_bytes=after.vm_hwm_bytes,
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
) -> tuple[TaskIdentity, ...]:
    births = task_births(before, after)
    if allowed_comms is not None:
        allowed = frozenset(allowed_comms)
        unexpected = tuple(identity for identity in births if identity.comm not in allowed)
        if unexpected:
            raise ProfileEvidenceError(
                f"role {role} has unexpected births: {[item.to_json() for item in unexpected]}"
            )
    if not births:
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
                            allowed_comms=(TOKIO_WORKER_COMM,),
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
            publication = bind_birth_group(
                "spawn_blocking-publication",
                start.ready.tasks,
                terminal.tasks,
                allowed_comms=(TOKIO_WORKER_COMM,),
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
        if len(fields) not in {5, 6} or (len(fields) == 6 and fields[5]):
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
    r"^(?:\[pid\s+(?P<bracket_pid>\d+)\]\s+|(?P<pid>\d+)\s+)"
    r"(?:\d+\.\d+\s+)?write\((?P<fd>\d+)(?:<[^>]*>)?,\s*"
    r"(?P<payload>\"(?:\\.|[^\"\\])*\"),\s*(?P<count>\d+)\)\s*"
    r"=\s*(?P<result>\d+)$"
)
_TRACE_RESUMED = re.compile(
    r"^(?:\[pid\s+(?P<bracket_pid>\d+)\]\s+|(?P<pid>\d+)\s+)?"
    r"(?:\d+\.\d+\s+)?<\.\.\.\s+(?P<syscall>[A-Za-z0-9_]+) resumed>"
)
_TRACE_SYNC_FD = re.compile(
    r"^(?:\[pid\s+\d+\]\s+|\d+\s+)?(?:\d+\.\d+\s+)?"
    r"(?P<syscall>fsync|fdatasync)\(\d+<(?P<path>[^>]*)>\)\s*"
    r"=\s*-?\d+(?:\s+.*)?$"
)


def _wire_event(event_value: object, context: str) -> bytes:
    event = _as_mapping(event_value, context)
    wire = {key: value for key, value in event.items() if not key.startswith("_runner_")}
    return canonical_json(wire)


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
    return (
        int(pid_text) == child_pid
        and int(match.group("fd")) == control_fd
        and int(match.group("count")) == len(payload)
        and int(match.group("result")) == len(payload)
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
            if not active or ended != 1 or unfinished:
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
                raise ProfileEvidenceError(f"unpaired/resumed trace call: {raw!r}")
            del unfinished[key]
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
    if began != 1 or ended != 1 or active or unfinished:
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
    return path == authority_path if kind == "exact" else path.startswith(authority_path)


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


_SEMANTIC_MANIFEST_ENTRY_FIELDS = frozenset(
    (
        "changed_ns",
        "device",
        "file_type",
        "gid",
        "inode",
        "link_count",
        "modified_ns",
        "path",
        "permissions",
        "sha256",
        "size",
        "symlink_target",
        "symlink_scope",
        "uid",
    )
)
_RECURSIVE_METADATA_FIELDS = (
    "st_dev",
    "st_ino",
    "st_mode",
    "st_nlink",
    "st_size",
    "st_mtime_ns",
    "st_ctime_ns",
)


def _same_recursive_metadata(left: os.stat_result, right: os.stat_result) -> bool:
    return all(
        getattr(left, field) == getattr(right, field)
        for field in _RECURSIVE_METADATA_FIELDS
    )


def _system_symlink_scope(root: Path, relative: str, target: str) -> str:
    raw_guest = (
        PurePosixPath(target)
        if PurePosixPath(target).is_absolute()
        else PurePosixPath(str(root)) / PurePosixPath(relative).parent / target
    )
    rendered = os.path.normpath(str(raw_guest))
    for alias, destination in (
        ("/bin", "/usr/bin"),
        ("/lib", "/usr/lib"),
        ("/lib64", "/usr/lib"),
    ):
        if rendered == alias or rendered.startswith(alias + "/"):
            rendered = destination + rendered.removeprefix(alias)
            break
    exposed = tuple(guest for _host, guest in _TRUSTED_SYSTEM_MOUNTS)
    if any(
        rendered == authority or rendered.startswith(authority + "/")
        for authority in exposed
    ):
        return "within_closure"
    if any(
        rendered == authority or rendered.startswith(authority + "/")
        for authority in ("/asterism", "/dev", "/proc", "/run", "/sys", "/tmp")
    ):
        raise ProfileEvidenceError(
            "trusted system symlink reaches mutable guest authority"
        )
    return "guest_inaccessible_external"


def _validate_recursive_manifest(
    value: object, role: str, context: str, *, trusted_system: bool
) -> dict[str, object]:
    manifest = _exact_mapping(value, ("entries", "role", "schema"), context)
    entries = manifest["entries"]
    if (
        manifest["schema"] != _RECURSIVE_TREE_AUTHORITY_SCHEMA
        or manifest["role"] != role
        or not isinstance(entries, list)
        or not entries
    ):
        raise ProfileEvidenceError(f"{context} recursive manifest fields differ")
    paths: list[str] = []
    directory_count = 0
    for index, item in enumerate(entries):
        entry = _exact_mapping(item, _SEMANTIC_MANIFEST_ENTRY_FIELDS, context)
        relative = entry["path"]
        if not isinstance(relative, str) or (
            relative != "."
            and (
                PurePosixPath(relative).is_absolute()
                or str(PurePosixPath(relative)) != relative
                or ".." in PurePosixPath(relative).parts
                or "." in PurePosixPath(relative).parts
            )
        ):
            raise ProfileEvidenceError(f"{context} recursive manifest path differs")
        if relative in paths or (index == 0) != (relative == "."):
            raise ProfileEvidenceError(f"{context} recursive manifest paths alias")
        paths.append(relative)
        kind = entry["file_type"]
        if kind not in {"directory", "regular", "symlink"}:
            raise ProfileEvidenceError(f"{context} recursive manifest type differs")
        if any(
            isinstance(entry[field], bool) or not isinstance(entry[field], int)
            for field in (
                "changed_ns",
                "device",
                "gid",
                "inode",
                "link_count",
                "modified_ns",
                "permissions",
                "size",
                "uid",
            )
        ):
            raise ProfileEvidenceError(f"{context} recursive metadata differs")
        if kind == "directory":
            directory_count += 1
            if any(
                entry[field] is not None
                for field in ("sha256", "symlink_target", "symlink_scope")
            ):
                raise ProfileEvidenceError(f"{context} directory digest differs")
        elif kind == "regular":
            digest = entry["sha256"]
            if (
                entry["symlink_target"] is not None
                or entry["symlink_scope"] is not None
                or (
                    digest is not None
                    if trusted_system
                    else not isinstance(digest, str)
                    or _SHA256_RE.fullmatch(digest) is None
                )
            ):
                raise ProfileEvidenceError(f"{context} regular-file digest differs")
        elif (
            not isinstance(entry["symlink_target"], str)
            or not isinstance(entry["sha256"], str)
            or _SHA256_RE.fullmatch(entry["sha256"]) is None
            or entry["symlink_scope"]
            not in (
                {"within_closure", "guest_inaccessible_external"}
                if trusted_system
                else {"within_root"}
            )
        ):
            raise ProfileEvidenceError(f"{context} symlink target differs")
        if trusted_system and (
            entry["uid"] != _TRUSTED_SYSTEM_OWNER_UID
            or (kind != "symlink" and entry["permissions"] & 0o022)
        ):
            raise ProfileEvidenceError(f"{context} trusted system policy differs")
    if directory_count < 1:
        raise ProfileEvidenceError(f"{context} recursive directory set is empty")
    if paths != sorted(paths, key=lambda path: (path != ".", path)):
        raise ProfileEvidenceError(f"{context} recursive path order differs")
    return dict(manifest)


def _recursive_live_manifest(
    root: Path,
    role: str,
    context: str,
    *,
    allow_internal_symlinks: bool,
    hash_regular_contents: bool,
    trusted_system_roots: tuple[Path, ...] = (),
    excluded_relative_paths: tuple[str, ...] = (),
    volatile_directory_metadata_paths: tuple[str, ...] = (),
) -> dict[str, object]:
    """Independently resample a producer manifest through retained descriptors."""

    try:
        lexical = Path(str(root))
        metadata = lexical.lstat()
        resolved = lexical.resolve(strict=True)
    except OSError as error:
        raise ProfileEvidenceError(f"cannot resolve {context} root: {error}") from error
    if (
        not lexical.is_absolute()
        or str(lexical) != str(root)
        or resolved != lexical
        or stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
    ):
        raise ProfileEvidenceError(f"{context} root is not an exact directory")
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
    try:
        root_descriptor = os.open(lexical, flags)
    except OSError as error:
        raise ProfileEvidenceError(f"cannot retain {context} root: {error}") from error
    entries: list[dict[str, object]] = []
    excluded = frozenset(excluded_relative_paths)
    volatile = frozenset(volatile_directory_metadata_paths)

    def trusted(selected: os.stat_result, relative: str, *, symlink: bool) -> None:
        candidate = lexical if relative == "." else lexical / relative
        if trusted_system_roots and (
            selected.st_uid != _TRUSTED_SYSTEM_OWNER_UID
            or (not symlink and stat.S_IMODE(selected.st_mode) & 0o022)
            or (not symlink and os.access(candidate, os.W_OK))
        ):
            raise ProfileEvidenceError(f"{context} trusted entry is writable: {relative}")

    def record(
        selected: os.stat_result,
        relative: str,
        kind: str,
        digest: str | None,
        target: str | None,
        scope: str | None,
    ) -> dict[str, object]:
        item: dict[str, object] = {
            "changed_ns": selected.st_ctime_ns,
            "device": selected.st_dev,
            "file_type": kind,
            "gid": selected.st_gid,
            "inode": selected.st_ino,
            "link_count": selected.st_nlink,
            "modified_ns": selected.st_mtime_ns,
            "path": relative,
            "permissions": stat.S_IMODE(selected.st_mode),
            "sha256": digest,
            "size": selected.st_size,
            "symlink_target": target,
            "symlink_scope": scope,
            "uid": selected.st_uid,
        }
        if kind == "directory" and relative in volatile:
            for field in ("changed_ns", "modified_ns", "permissions", "size"):
                item[field] = 0
        return item

    def walk(descriptor: int, relative: str) -> None:
        before = os.fstat(descriptor)
        trusted(before, relative, symlink=False)
        entries.append(record(before, relative, "directory", None, None, None))
        try:
            names = sorted(os.listdir(descriptor))
        except OSError as error:
            raise ProfileEvidenceError(f"cannot enumerate {context}: {error}") from error
        if len(names) != len(set(names)):
            raise ProfileEvidenceError(f"{context} names alias")
        for name in names:
            child_relative = name if relative == "." else f"{relative}/{name}"
            if child_relative in excluded:
                continue
            try:
                selected = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            except OSError as error:
                raise ProfileEvidenceError(f"cannot stat {context}: {error}") from error
            if stat.S_ISDIR(selected.st_mode):
                try:
                    child = os.open(name, flags, dir_fd=descriptor)
                except OSError as error:
                    raise ProfileEvidenceError(
                        f"cannot retain {context} directory: {error}"
                    ) from error
                try:
                    if not _same_recursive_metadata(selected, os.fstat(child)):
                        raise ProfileEvidenceError(
                            f"{context} directory selection changed"
                        )
                    walk(child, child_relative)
                finally:
                    os.close(child)
            elif stat.S_ISREG(selected.st_mode):
                file_flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
                try:
                    child = os.open(name, file_flags, dir_fd=descriptor)
                except OSError as error:
                    raise ProfileEvidenceError(
                        f"cannot retain {context} file: {error}"
                    ) from error
                try:
                    opened_before = os.fstat(child)
                    if not _same_recursive_metadata(selected, opened_before):
                        raise ProfileEvidenceError(f"{context} file selection changed")
                    hasher = hashlib.sha256() if hash_regular_contents else None
                    offset = 0
                    while True:
                        chunk = os.pread(child, 1024 * 1024, offset)
                        if not chunk:
                            break
                        if hasher is not None:
                            hasher.update(chunk)
                        offset += len(chunk)
                    digest = hasher.hexdigest() if hasher is not None else None
                    opened_after = os.fstat(child)
                    selected_after = os.stat(
                        name, dir_fd=descriptor, follow_symlinks=False
                    )
                    if (
                        offset != opened_before.st_size
                        or not _same_recursive_metadata(opened_before, opened_after)
                        or not _same_recursive_metadata(opened_after, selected_after)
                    ):
                        raise ProfileEvidenceError(
                            f"{context} file changed while snapshotting"
                        )
                finally:
                    os.close(child)
                trusted(selected, child_relative, symlink=False)
                entries.append(
                    record(selected, child_relative, "regular", digest, None, None)
                )
            elif stat.S_ISLNK(selected.st_mode):
                if not allow_internal_symlinks:
                    raise ProfileEvidenceError(
                        f"{context} contains a symlink: {child_relative}"
                    )
                target = os.readlink(name, dir_fd=descriptor)
                selected_after = os.stat(
                    name, dir_fd=descriptor, follow_symlinks=False
                )
                if not _same_recursive_metadata(selected, selected_after):
                    raise ProfileEvidenceError(f"{context} symlink changed")
                candidate = lexical / Path(child_relative).parent / target
                try:
                    resolved_target = candidate.resolve(strict=True)
                except OSError as error:
                    raise ProfileEvidenceError(
                        f"{context} symlink target is unresolved"
                    ) from error
                if trusted_system_roots:
                    scope = _system_symlink_scope(lexical, child_relative, target)
                else:
                    if resolved_target != lexical and lexical not in resolved_target.parents:
                        raise ProfileEvidenceError(
                            f"{context} symlink escapes the retained root"
                        )
                    scope = "within_root"
                trusted(selected, child_relative, symlink=True)
                entries.append(
                    record(
                        selected,
                        child_relative,
                        "symlink",
                        _sha256_bytes(os.fsencode(target)),
                        target,
                        scope,
                    )
                )
            else:
                raise ProfileEvidenceError(
                    f"{context} contains an unsupported node: {child_relative}"
                )
        if not _same_recursive_metadata(before, os.fstat(descriptor)):
            raise ProfileEvidenceError(f"{context} directory changed during traversal")

    try:
        walk(root_descriptor, ".")
    finally:
        os.close(root_descriptor)
    hardlinks: dict[tuple[int, int], list[dict[str, object]]] = {}
    for entry in entries:
        if entry["file_type"] == "regular":
            hardlinks.setdefault(
                (int(entry["device"]), int(entry["inode"])), []
            ).append(entry)
    if not trusted_system_roots and any(
        len(aliases) != aliases[0]["link_count"] for aliases in hardlinks.values()
    ):
        raise ProfileEvidenceError(f"{context} hard link escapes the retained tree")
    entries.sort(key=lambda entry: (entry["path"] != ".", entry["path"]))
    return {
        "entries": entries,
        "role": role,
        "schema": _RECURSIVE_TREE_AUTHORITY_SCHEMA,
    }


def _semantic_runtime_sha256(
    components: Mapping[str, object],
    canonical_bytes: Callable[[object], bytes] = canonical_json,
) -> str:
    if set(components) != {"cargo_home", "toolchain", "trusted_system_closure"}:
        raise ProfileEvidenceError("semantic runtime authority components differ")
    tree_fields = (
        "schema",
        "role",
        "manifest_sha256",
        "entry_count",
        "watch_count",
        "equal_pre_post",
        "mutation_events_absent",
    )
    closure_fields = (
        "schema",
        "sha256",
        "entry_count",
        "mounts",
        "watch_count",
        "mutation_events_absent",
    )
    closure = _as_mapping(components["trusted_system_closure"], "semantic closure")
    mounts = closure.get("mounts")
    if (
        not isinstance(mounts, list)
        or len(mounts) != len(_TRUSTED_SYSTEM_MOUNTS)
        or any(
            not isinstance(mount, dict)
            or mount.get("guest_path") != guest
            or mount.get("host_path") != str(host)
            or mount.get("resolved_path") != str(host)
            for mount, (host, guest) in zip(
                mounts, _TRUSTED_SYSTEM_MOUNTS, strict=True
            )
        )
    ):
        raise ProfileEvidenceError("semantic runtime trusted-system mounts differ")
    normalized = {
        "cargo_home": {
            field: _as_mapping(
                components["cargo_home"], "semantic Cargo home"
            ).get(field)
            for field in tree_fields
        },
        "schema": _SEMANTIC_INPUT_AUTHORITY_SCHEMA,
        "toolchain": {
            field: _as_mapping(components["toolchain"], "semantic toolchain").get(
                field
            )
            for field in tree_fields
        },
        "trusted_system_closure": {
            field: closure.get(field) for field in closure_fields
        },
    }
    return _sha256_bytes(canonical_bytes(normalized))


def _semantic_manifest_snapshot(
    path_value: object,
    digest: object,
    context: str,
    canonical_payload: Callable[[bytes, str], dict[str, object]],
) -> tuple[Path, bytes, dict[str, object], tuple[int, int]]:
    if not isinstance(digest, str) or _SHA256_RE.fullmatch(digest) is None:
        raise ProfileEvidenceError(f"{context} SHA-256 is malformed")
    path, payload, identity = _immutable_file_payload(
        path_value, 0o444, context, limit=None
    )
    if _sha256_bytes(payload) != digest or identity[2] != 1:
        raise ProfileEvidenceError(f"{context} SHA-256 differs")
    value = canonical_payload(payload, context)
    return path, payload, value, (identity[0], identity[1])


def _validate_semantic_input_authority(
    value: object,
    context: str,
    *,
    live_roots: Mapping[str, Path],
    expected_manifest_paths: Mapping[str, Path],
    source_role: str = "source",
    live_cache: dict[tuple[object, ...], dict[str, object]] | None = None,
    manifest_identities: set[tuple[int, int]] | None = None,
    manifest_canonical_payload: Callable[
        [bytes, str], dict[str, object]
    ] = _canonical_json_payload,
    manifest_canonical_json: Callable[[object], bytes] = canonical_json,
) -> tuple[str, tuple[Path, Path, Path, Path]]:
    authority = _exact_mapping(
        value,
        (
            "cargo_home",
            "runtime_sha256",
            "schema",
            "source",
            "toolchain",
            "trusted_system_closure",
        ),
        context,
    )
    if authority["schema"] != _SEMANTIC_INPUT_AUTHORITY_SCHEMA:
        raise ProfileEvidenceError(f"{context} semantic input schema differs")

    def live_observation(
        root: Path,
        role: str,
        observation_context: str,
        *,
        allow_internal_symlinks: bool,
        hash_regular_contents: bool,
        trusted_system_roots: tuple[Path, ...] = (),
        excluded_relative_paths: tuple[str, ...] = (),
        volatile_directory_metadata_paths: tuple[str, ...] = (),
    ) -> dict[str, object]:
        key: tuple[object, ...] = (
            str(root),
            role,
            allow_internal_symlinks,
            hash_regular_contents,
            tuple(str(path) for path in trusted_system_roots),
            excluded_relative_paths,
            volatile_directory_metadata_paths,
        )
        if live_cache is not None and key in live_cache:
            return live_cache[key]
        observed = _recursive_live_manifest(
            root,
            role,
            observation_context,
            allow_internal_symlinks=allow_internal_symlinks,
            hash_regular_contents=hash_regular_contents,
            trusted_system_roots=trusted_system_roots,
            excluded_relative_paths=excluded_relative_paths,
            volatile_directory_metadata_paths=volatile_directory_metadata_paths,
        )
        if live_cache is not None:
            live_cache[key] = observed
            return observed
        replayed = _recursive_live_manifest(
            root,
            role,
            f"replayed {observation_context}",
            allow_internal_symlinks=allow_internal_symlinks,
            hash_regular_contents=hash_regular_contents,
            trusted_system_roots=trusted_system_roots,
            excluded_relative_paths=excluded_relative_paths,
            volatile_directory_metadata_paths=volatile_directory_metadata_paths,
        )
        if replayed != observed:
            raise ProfileEvidenceError(f"{observation_context} changed while sampling")
        return observed
    tree_fields = (
        "entry_count",
        "equal_pre_post",
        "manifest_path",
        "manifest_sha256",
        "mutation_events_absent",
        "role",
        "schema",
        "watch_count",
    )
    manifest_paths: list[Path] = []
    authority_manifest_identities: set[tuple[int, int]] = set()
    for name, role in (
        ("source", source_role),
        ("toolchain", "toolchain"),
        ("cargo_home", "cargo_home"),
    ):
        tree = _exact_mapping(authority[name], tree_fields, f"{context} {name}")
        manifest_path = Path(str(tree["manifest_path"]))
        if (
            tree["schema"] != _RECURSIVE_TREE_AUTHORITY_SCHEMA
            or tree["role"] != role
            or not isinstance(tree["manifest_sha256"], str)
            or _SHA256_RE.fullmatch(tree["manifest_sha256"]) is None
            or tree["manifest_path"] != str(expected_manifest_paths[name])
            or manifest_path != expected_manifest_paths[name]
            or isinstance(tree["entry_count"], bool)
            or not isinstance(tree["entry_count"], int)
            or tree["entry_count"] < 1
            or isinstance(tree["watch_count"], bool)
            or not isinstance(tree["watch_count"], int)
            or tree["watch_count"] < 1
            or tree["equal_pre_post"] is not True
            or tree["mutation_events_absent"] is not True
        ):
            raise ProfileEvidenceError(f"{context} semantic {name} binding differs")
        path, payload, evidence, identity = _semantic_manifest_snapshot(
            tree["manifest_path"],
            tree["manifest_sha256"],
            f"{context} semantic {name} evidence",
            manifest_canonical_payload,
        )
        if identity in authority_manifest_identities:
            raise ProfileEvidenceError(f"{context} semantic manifest identity aliases")
        authority_manifest_identities.add(identity)
        manifest = _validate_recursive_manifest(
            evidence,
            role,
            f"{context} semantic {name} evidence",
            trusted_system=False,
        )
        entries = manifest["entries"]
        assert isinstance(entries, list)
        if (
            len(entries) != tree["entry_count"]
            or sum(entry.get("file_type") == "directory" for entry in entries)
            != tree["watch_count"]
        ):
            raise ProfileEvidenceError(f"{context} semantic {name} counts differ")
        observed = live_observation(
            live_roots[name],
            role,
            f"{context} live semantic {name}",
            allow_internal_symlinks=name != "source",
            hash_regular_contents=True,
            excluded_relative_paths=("Cargo.lock",)
            if source_role == "resolution_source_without_cargo_lock"
            and name == "source"
            else (),
            volatile_directory_metadata_paths=(".",)
            if source_role == "resolution_source_without_cargo_lock"
            and name == "source"
            else (),
        )
        if manifest_canonical_json(observed) != payload:
            raise ProfileEvidenceError(f"{context} live semantic {name} differs")
        manifest_paths.append(path)

    closure_fields = (
        "entry_count",
        "manifest_path",
        "mounts",
        "mutation_events_absent",
        "schema",
        "sha256",
        "watch_count",
    )
    mount_fields = (
        "device",
        "gid",
        "guest_path",
        "host_path",
        "inode",
        "permissions",
        "resolved_path",
        "trusted_root_owned_non_writable",
        "uid",
    )
    closure = _exact_mapping(
        authority["trusted_system_closure"], closure_fields, f"{context} closure"
    )
    mounts = closure["mounts"]
    closure_path = Path(str(closure["manifest_path"]))
    if (
        closure["schema"] != _TRUSTED_SYSTEM_CLOSURE_SCHEMA
        or not isinstance(closure["sha256"], str)
        or _SHA256_RE.fullmatch(closure["sha256"]) is None
        or closure["manifest_path"] != str(expected_manifest_paths["closure"])
        or closure_path != expected_manifest_paths["closure"]
        or isinstance(closure["entry_count"], bool)
        or not isinstance(closure["entry_count"], int)
        or closure["entry_count"] < len(_TRUSTED_SYSTEM_MOUNTS)
        or isinstance(closure["watch_count"], bool)
        or not isinstance(closure["watch_count"], int)
        or closure["watch_count"] < len(_TRUSTED_SYSTEM_MOUNTS)
        or closure["mutation_events_absent"] is not True
        or not isinstance(mounts, list)
        or len(mounts) != len(_TRUSTED_SYSTEM_MOUNTS)
    ):
        raise ProfileEvidenceError(f"{context} trusted system closure differs")
    bound_mounts: list[Mapping[str, object]] = []
    for item, (host, guest) in zip(mounts, _TRUSTED_SYSTEM_MOUNTS, strict=True):
        mount = _exact_mapping(item, mount_fields, f"{context} closure mount")
        if (
            mount["guest_path"] != guest
            or mount["host_path"] != str(host)
            or mount["resolved_path"] != str(host)
            or mount["trusted_root_owned_non_writable"] is not True
            or mount["uid"] != _TRUSTED_SYSTEM_OWNER_UID
            or any(
                isinstance(mount[field], bool) or not isinstance(mount[field], int)
                for field in ("device", "gid", "inode", "permissions", "uid")
            )
            or mount["permissions"] & 0o022
        ):
            raise ProfileEvidenceError(f"{context} trusted system mount differs")
        bound_mounts.append(mount)
    closure_evidence_path, closure_payload, closure_evidence, closure_identity = (
        _semantic_manifest_snapshot(
            closure["manifest_path"],
            closure["sha256"],
            f"{context} closure evidence",
            manifest_canonical_payload,
        )
    )
    if closure_identity in authority_manifest_identities:
        raise ProfileEvidenceError(f"{context} semantic manifest identity aliases")
    authority_manifest_identities.add(closure_identity)
    closure_value = _exact_mapping(
        closure_evidence, ("mounts", "schema"), f"{context} closure evidence"
    )
    evidence_mounts = closure_value["mounts"]
    if (
        closure_value["schema"] != _TRUSTED_SYSTEM_CLOSURE_SCHEMA
        or not isinstance(evidence_mounts, list)
        or len(evidence_mounts) != len(_TRUSTED_SYSTEM_MOUNTS)
    ):
        raise ProfileEvidenceError(f"{context} closure evidence differs")
    trusted_roots = tuple(host.resolve(strict=True) for host, _guest in _TRUSTED_SYSTEM_MOUNTS)
    entry_count = 0
    watch_count = 0
    for item, binding, (host, guest) in zip(
        evidence_mounts, bound_mounts, _TRUSTED_SYSTEM_MOUNTS, strict=True
    ):
        evidence_mount = _exact_mapping(
            item,
            ("guest_path", "host_path", "resolved_path", "tree"),
            f"{context} closure evidence mount",
        )
        if (
            evidence_mount["guest_path"] != guest
            or evidence_mount["host_path"] != str(host)
            or evidence_mount["resolved_path"] != str(host)
        ):
            raise ProfileEvidenceError(f"{context} closure evidence mount differs")
        role = "system-" + guest.removeprefix("/").replace("/", "-")
        tree = _validate_recursive_manifest(
            evidence_mount["tree"],
            role,
            f"{context} trusted system {guest}",
            trusted_system=True,
        )
        entries = tree["entries"]
        assert isinstance(entries, list)
        root_entry = entries[0]
        for field in ("device", "gid", "inode", "permissions", "uid"):
            if binding[field] != root_entry[field]:
                raise ProfileEvidenceError(
                    f"{context} trusted system root binding differs"
                )
        observed = live_observation(
            host,
            role,
            f"{context} live trusted system {guest}",
            allow_internal_symlinks=True,
            hash_regular_contents=False,
            trusted_system_roots=trusted_roots,
        )
        if observed != tree:
            raise ProfileEvidenceError(f"{context} live trusted system {guest} differs")
        entry_count += len(entries)
        watch_count += sum(entry.get("file_type") == "directory" for entry in entries)
    if entry_count != closure["entry_count"] or watch_count != closure["watch_count"]:
        raise ProfileEvidenceError(f"{context} closure counts differ")
    if manifest_canonical_json(dict(closure_value)) != closure_payload:
        raise ProfileEvidenceError(f"{context} closure canonical bytes differ")
    components = {
        name: authority[name]
        for name in ("cargo_home", "toolchain", "trusted_system_closure")
    }
    runtime = authority["runtime_sha256"]
    if (
        not isinstance(runtime, str)
        or _SHA256_RE.fullmatch(runtime) is None
        or runtime
        != _semantic_runtime_sha256(
            components, canonical_bytes=manifest_canonical_json
        )
    ):
        raise ProfileEvidenceError(f"{context} semantic runtime digest differs")
    manifest_paths.append(closure_evidence_path)
    if len(manifest_paths) != 4:
        raise ProfileEvidenceError(f"{context} semantic manifest count differs")
    if manifest_identities is not None:
        if authority_manifest_identities & manifest_identities:
            raise ProfileEvidenceError(f"{context} semantic manifest files alias")
        manifest_identities.update(authority_manifest_identities)
    return runtime, (
        manifest_paths[0],
        manifest_paths[1],
        manifest_paths[2],
        manifest_paths[3],
    )


def _replay_semantic_live_cache(
    live_cache: Mapping[tuple[object, ...], dict[str, object]],
) -> None:
    for key, expected in live_cache.items():
        (
            root,
            role,
            allow_internal_symlinks,
            hash_regular_contents,
            trusted_roots,
            excluded,
            volatile,
        ) = key
        observed = _recursive_live_manifest(
            Path(str(root)),
            str(role),
            f"phase 4 replayed live semantic {role}",
            allow_internal_symlinks=bool(allow_internal_symlinks),
            hash_regular_contents=bool(hash_regular_contents),
            trusted_system_roots=tuple(Path(str(path)) for path in trusted_roots),
            excluded_relative_paths=tuple(str(path) for path in excluded),
            volatile_directory_metadata_paths=tuple(str(path) for path in volatile),
        )
        if observed != expected:
            raise ProfileEvidenceError(
                f"phase 4 live semantic {role} changed across replay"
            )


def _sandbox_environment(value: object, context: str, *, allow_extra: bool) -> None:
    environment = _as_mapping(value, f"{context} environment")
    required = {
        "CARGO_HOME": _GUEST_CARGO_HOME,
        "CARGO_INCREMENTAL": "0",
        "CARGO_NET_OFFLINE": "true",
        "GIT_CONFIG_COUNT": "0",
        "GIT_CONFIG_GLOBAL": f"{_GUEST_ROOT}/absent-gitconfig",
        "GIT_CONFIG_NOSYSTEM": "1",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": f"{_GUEST_TOOLCHAIN_ROOT}/bin:/usr/bin:/bin",
        "RUSTC": _GUEST_RUSTC,
        "RUSTUP_HOME": "/nonexistent",
        "TZ": "UTC",
    }
    if allow_extra:
        required["LD_ORIGIN_PATH"] = _GUEST_TOOLCHAIN_BIN
    if any(environment.get(name) != expected for name, expected in required.items()):
        raise ProfileEvidenceError(f"{context} sandbox environment differs")
    if not isinstance(environment.get("RUSTUP_TOOLCHAIN"), str) or not environment[
        "RUSTUP_TOOLCHAIN"
    ]:
        raise ProfileEvidenceError(f"{context} Rust toolchain environment differs")
    expected_fields = set(required) | {"RUSTUP_TOOLCHAIN"}
    if allow_extra:
        expected_fields.update(_RELEASE_BUILD_ENVIRONMENT_FIELDS)
    if set(environment) != expected_fields or any(
        not isinstance(environment[field], str)
        for field in _RELEASE_BUILD_ENVIRONMENT_FIELDS
        if allow_extra
    ):
        raise ProfileEvidenceError(f"{context} sandbox environment fields differ")
    if any(
        name in environment
        for name in (
            "CARGO_ENCODED_RUSTFLAGS",
            "RUSTC_WORKSPACE_WRAPPER",
            "RUSTC_WRAPPER",
            "RUSTFLAGS",
        )
    ):
        raise ProfileEvidenceError(f"{context} enables a wrapper or rustflags")


def _cargo_config_sha256(
    attestation: Mapping[str, object], context: str
) -> str:
    binding = _exact_mapping(
        attestation.get("cargo_config_search"),
        ("path", "sha256"),
        f"{context} Cargo config binding",
    )
    manifest_path, payload, _identity = _immutable_file_snapshot(
        binding["path"],
        binding["sha256"],
        0o444,
        f"{context} Cargo config manifest",
    )
    manifest = _exact_mapping(
        _canonical_json_payload(payload, f"{context} Cargo config manifest"),
        ("schema", "cargo_home_path", "cwd", "entries"),
        f"{context} Cargo config manifest",
    )
    if (
        manifest["schema"] != _CARGO_CONFIG_SEARCH_SCHEMA
        or manifest["cwd"] != _GUEST_SOURCE
        or manifest["cargo_home_path"] != _GUEST_CARGO_HOME
    ):
        raise ProfileEvidenceError(f"{context} Cargo config guest identity differs")
    try:
        materialized_text = attestation["materialized_root"]
        source_root = Path(str(materialized_text)).resolve(strict=True)
        toolchain = _as_mapping(attestation["toolchain"], f"{context} toolchain")
        cargo_home_text = toolchain.get("cargo_home_path")
        cargo_home = Path(str(cargo_home_text)).resolve(strict=True)
    except (KeyError, OSError, RuntimeError) as error:
        raise ProfileEvidenceError(f"{context} Cargo config host roots differ") from error
    if (
        not isinstance(materialized_text, str)
        or materialized_text != str(source_root)
        or not isinstance(cargo_home_text, str)
        or cargo_home_text != str(cargo_home)
    ):
        raise ProfileEvidenceError(f"{context} Cargo config host roots differ")
    candidates: tuple[tuple[str, Path | None], ...] = (
        (f"{_GUEST_SOURCE}/.cargo/config.toml", source_root / ".cargo/config.toml"),
        (f"{_GUEST_SOURCE}/.cargo/config", source_root / ".cargo/config"),
        (f"{_GUEST_ROOT}/.cargo/config.toml", None),
        (f"{_GUEST_ROOT}/.cargo/config", None),
        ("/.cargo/config.toml", None),
        ("/.cargo/config", None),
        (f"{_GUEST_CARGO_HOME}/config.toml", cargo_home / "config.toml"),
        (f"{_GUEST_CARGO_HOME}/config", cargo_home / "config"),
    )
    entries = manifest["entries"]
    if not isinstance(entries, list) or len(entries) != len(candidates):
        raise ProfileEvidenceError(f"{context} Cargo config candidate count differs")
    for ordinal, (entry_value, (guest_path, host_path)) in enumerate(
        zip(entries, candidates, strict=True), start=1
    ):
        entry = _exact_mapping(
            entry_value,
            ("path", "status", "sha256"),
            f"{context} Cargo config entry {ordinal}",
        )
        if entry["path"] != guest_path:
            raise ProfileEvidenceError(f"{context} Cargo config guest path/order differs")
        if host_path is None:
            if entry["status"] != "absent" or entry["sha256"] is not None:
                raise ProfileEvidenceError(f"{context} private Cargo config is present")
            continue
        if host_path.is_symlink():
            raise ProfileEvidenceError(f"{context} Cargo config input is a symlink")
        expected_sha256 = _EMPTY_SHA256
        if host_path.exists():
            _path, host_payload, _host_identity = _immutable_file_payload(
                str(host_path), None, f"{context} Cargo config input"
            )
            expected_sha256 = _sha256_bytes(host_payload)
        if entry["status"] != "present" or entry["sha256"] != expected_sha256:
            raise ProfileEvidenceError(f"{context} effective Cargo config differs")
    empty_path, empty_payload, _empty_identity = _immutable_file_snapshot(
        str(manifest_path.with_name(f"{manifest_path.name}.empty")),
        _EMPTY_SHA256,
        0o444,
        f"{context} empty Cargo config",
    )
    if empty_payload != b"" or empty_path.parent != manifest_path.parent:
        raise ProfileEvidenceError(f"{context} empty Cargo config differs")
    return str(binding["sha256"])


def _sandbox_descriptors(
    argv: object,
    expected_bindings: Sequence[tuple[str, str, str]],
    context: str,
) -> tuple[list[str], dict[str, int]]:
    if (
        not isinstance(argv, list)
        or not argv
        or any(not isinstance(argument, str) for argument in argv)
    ):
        raise ProfileEvidenceError(f"{context} argv is not an exact string list")
    normalized = list(argv)
    descriptors: dict[str, int] = {}
    observed: list[tuple[str, str, str]] = []
    for index, argument in enumerate(argv):
        if argument == "--dev-bind":
            segment = argv[index : index + 3]
            source = segment[1] if len(segment) == 3 else ""
            prefix = "/proc/self/fd/"
            descriptor_text = (
                source.removeprefix(prefix)
                if isinstance(source, str) and source.startswith(prefix)
                else ""
            )
            if (
                len(segment) != 3
                or segment[2] != "/dev/null"
                or not descriptor_text.isdecimal()
                or len(descriptor_text) > 10
                or str(int(descriptor_text)) != descriptor_text
                or int(descriptor_text) < 3
            ):
                raise ProfileEvidenceError(
                    f"{context} null-device descriptor binding differs"
                )
            ordinal = len(observed)
            if ordinal >= len(expected_bindings):
                raise ProfileEvidenceError(
                    f"{context} has an extra descriptor binding"
                )
            name, _option, _destination = expected_bindings[ordinal]
            observed.append((name, "--dev-bind", segment[2]))
            descriptors[name] = int(descriptor_text)
            normalized[index + 1] = "$FD:/dev/null"
            continue
        if argument == "--overlay-src":
            segment = argv[index : index + 4]
            source = segment[1] if len(segment) == 4 else ""
            prefix = "/proc/self/fd/"
            descriptor_text = (
                source.removeprefix(prefix)
                if isinstance(source, str) and source.startswith(prefix)
                else ""
            )
            if (
                len(segment) != 4
                or segment[2] != "--tmp-overlay"
                or not descriptor_text.isdecimal()
                or len(descriptor_text) > 10
                or str(int(descriptor_text)) != descriptor_text
                or int(descriptor_text) < 3
            ):
                raise ProfileEvidenceError(
                    f"{context} overlay descriptor binding differs"
                )
            ordinal = len(observed)
            if ordinal >= len(expected_bindings):
                raise ProfileEvidenceError(
                    f"{context} has an extra descriptor binding"
                )
            name, _option, _destination = expected_bindings[ordinal]
            observed.append((name, "--tmp-overlay", segment[3]))
            descriptors[name] = int(descriptor_text)
            normalized[index + 1] = "$FD:cargo-home-overlay"
            continue
        if argument not in {"--ro-bind-fd", "--bind-fd", "--ro-bind-data"}:
            continue
        descriptor_text = argv[index + 1] if index + 1 < len(argv) else ""
        if (
            index + 2 >= len(argv)
            or not descriptor_text.isdecimal()
            or str(int(descriptor_text)) != descriptor_text
            or int(descriptor_text) < 3
        ):
            raise ProfileEvidenceError(f"{context} descriptor binding differs")
        ordinal = len(observed)
        if ordinal >= len(expected_bindings):
            raise ProfileEvidenceError(f"{context} has an extra descriptor binding")
        name, _option, _destination = expected_bindings[ordinal]
        observed.append((name, argument, argv[index + 2]))
        descriptors[name] = int(descriptor_text)
        normalized[index + 1] = f"$FD:{argv[index + 2]}"
    if tuple(observed) != tuple(expected_bindings) or len(descriptors) != len(
        set(descriptors.values())
    ):
        raise ProfileEvidenceError(f"{context} descriptor topology differs")
    return normalized, descriptors


def _release_sandbox_argv(
    descriptors: Mapping[str, int],
    package: str,
    example: str,
    bwrap_path: str,
    rustc_host: str,
) -> list[str]:
    rust_lld_guest_path = (
        f"{_GUEST_TOOLCHAIN_ROOT}/lib/rustlib/{rustc_host}/bin/gcc-ld/ld.lld"
    )
    argv = [
        bwrap_path,
        "--die-with-parent",
        "--new-session",
        "--unshare-net",
        "--dir",
        "/usr",
    ]
    for _host, guest in _TRUSTED_SYSTEM_MOUNTS:
        argv.extend(["--ro-bind-fd", str(descriptors[f"system:{guest}"]), guest])
    argv.extend(
        [
            "--symlink",
            "usr/bin",
            "/bin",
            "--symlink",
            "usr/lib",
            "/lib",
            "--symlink",
            "usr/lib",
            "/lib64",
            "--dir",
            "/dev",
            "--dev-bind",
            f"/proc/self/fd/{descriptors['dev_null']}",
            "/dev/null",
            "--dir",
            "/proc",
            "--tmpfs",
            "/tmp",
            "--tmpfs",
            _GUEST_ROOT,
            "--dir",
            f"{_GUEST_ROOT}/.cargo",
            "--tmpfs",
            f"{_GUEST_ROOT}/.cargo",
            "--remount-ro",
            f"{_GUEST_ROOT}/.cargo",
            "--dir",
            "/.cargo",
            "--tmpfs",
            "/.cargo",
            "--remount-ro",
            "/.cargo",
            "--ro-bind-fd",
            str(descriptors["source"]),
            _GUEST_SOURCE,
            "--bind-fd",
            str(descriptors["target"]),
            _GUEST_TARGET,
            "--ro-bind-fd",
            str(descriptors["toolchain_root"]),
            _GUEST_TOOLCHAIN_ROOT,
            "--ro-bind-fd",
            str(descriptors["cargo"]),
            _GUEST_CARGO,
            "--ro-bind-fd",
            str(descriptors["rustc"]),
            _GUEST_RUSTC,
            "--ro-bind-fd",
            str(descriptors["rust_lld"]),
            rust_lld_guest_path,
            "--overlay-src",
            f"/proc/self/fd/{descriptors['cargo_home']}",
            "--tmp-overlay",
            _GUEST_CARGO_HOME,
            "--dir",
            f"{_GUEST_SOURCE}/.cargo",
            "--tmpfs",
            f"{_GUEST_SOURCE}/.cargo",
        ]
    )
    for guest in _GUEST_BOUND_CONFIG_PATHS[:2]:
        argv.extend(["--ro-bind-fd", str(descriptors[f"config:{guest}"]), guest])
    argv.extend(["--remount-ro", f"{_GUEST_SOURCE}/.cargo"])
    for guest in _GUEST_BOUND_CONFIG_PATHS[2:]:
        argv.extend(["--ro-bind-fd", str(descriptors[f"config:{guest}"]), guest])
    argv.extend(
        [
            "--remount-ro",
            _GUEST_CARGO_HOME,
            "--chdir",
            _GUEST_SOURCE,
            _GUEST_CARGO,
            "build",
            "--locked",
            "--offline",
            "--release",
            "-p",
            package,
            "--example",
            example,
            "--target-dir",
            _GUEST_TARGET,
        ]
    )
    return argv


def _validate_release_sandbox(
    attestation: Mapping[str, object],
    context: str,
    *,
    package: str,
    example: str,
    runtime_sha256: str,
) -> str:
    toolchain = _as_mapping(attestation.get("toolchain"), f"{context} toolchain")
    bwrap_path = toolchain.get("bwrap_path")
    rustc_host = toolchain.get("rustc_host")
    if (
        not isinstance(bwrap_path, str)
        or not isinstance(rustc_host, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", rustc_host) is None
    ):
        raise ProfileEvidenceError(f"{context} bwrap authority is absent")
    rust_lld_guest_path = (
        f"{_GUEST_TOOLCHAIN_ROOT}/lib/rustlib/{rustc_host}/bin/gcc-ld/ld.lld"
    )
    system_bindings = tuple(
        (f"system:{guest}", "--ro-bind-fd", guest)
        for _host, guest in _TRUSTED_SYSTEM_MOUNTS
    )
    core_bindings = (
        ("dev_null", "--dev-bind", "/dev/null"),
        ("source", "--ro-bind-fd", _GUEST_SOURCE),
        ("target", "--bind-fd", _GUEST_TARGET),
        ("toolchain_root", "--ro-bind-fd", _GUEST_TOOLCHAIN_ROOT),
        ("cargo", "--ro-bind-fd", _GUEST_CARGO),
        ("rustc", "--ro-bind-fd", _GUEST_RUSTC),
        ("rust_lld", "--ro-bind-fd", rust_lld_guest_path),
        ("cargo_home", "--tmp-overlay", _GUEST_CARGO_HOME),
    )
    config_bindings = tuple(
        (f"config:{guest}", "--ro-bind-fd", guest)
        for guest in _GUEST_BOUND_CONFIG_PATHS
    )
    expected_bindings = (*system_bindings, *core_bindings, *config_bindings)
    normalized, descriptors = _sandbox_descriptors(
        attestation.get("build_argv"), expected_bindings, context
    )
    argv = attestation["build_argv"]
    if argv != _release_sandbox_argv(
        descriptors, package, example, bwrap_path, rustc_host
    ):
        raise ProfileEvidenceError(f"{context} sandbox argv differs")
    if any(
        forbidden in argv
        for forbidden in ("--dev", "--proc", "--share-net")
    ) or any(
        argv[index : index + 3] in (["--ro-bind", "/", "/"], ["--bind", "/", "/"])
        for index in range(max(0, len(argv) - 2))
    ):
        raise ProfileEvidenceError(f"{context} exposes forbidden host authority")
    _sandbox_environment(attestation.get("build_env"), context, allow_extra=True)
    if (
        any(
            not isinstance(attestation.get(field), str)
            or attestation[field] in argv
            for field in ("materialized_root", "target_dir")
        )
        or not isinstance(attestation.get("build_child"), Mapping)
        or _as_mapping(attestation["build_child"], f"{context} build child").get(
            "argv"
        )
        != argv
    ):
        raise ProfileEvidenceError(f"{context} sandbox/child authority differs")
    cargo_config_sha256 = _cargo_config_sha256(attestation, context)
    execution_tools_sha256 = _validate_prepared_execution_tools(
        attestation, context
    )
    return _sha256_bytes(
        canonical_json(
            {
                "argv": normalized,
                "cargo_config_search_sha256": cargo_config_sha256,
                "execution_tools_sha256": execution_tools_sha256,
                "semantic_runtime_sha256": runtime_sha256,
            }
        )
    )


def _authority_timestamp(value: object, context: str) -> datetime:
    if not isinstance(value, str):
        raise ProfileEvidenceError(f"{context} timestamp is not text")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ProfileEvidenceError(f"{context} timestamp is malformed") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ProfileEvidenceError(f"{context} timestamp is not timezone-aware")
    return parsed


def _materialized_root_identity(
    attestation: Mapping[str, object], context: str
) -> tuple[str, int, int]:
    value = attestation.get("materialized_root")
    if not isinstance(value, str):
        raise ProfileEvidenceError(f"{context} materialized root is absent")
    try:
        lexical = Path(value)
        resolved = lexical.resolve(strict=True)
        descriptor = os.open(
            resolved,
            os.O_RDONLY
            | os.O_DIRECTORY
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            opened = os.fstat(descriptor)
            current = os.stat(resolved, follow_symlinks=False)
        finally:
            os.close(descriptor)
    except (OSError, RuntimeError) as error:
        raise ProfileEvidenceError(f"{context} materialized root cannot be bound") from error
    if (
        not lexical.is_absolute()
        or lexical != resolved
        or value != str(resolved)
        or not stat.S_ISDIR(opened.st_mode)
        or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
    ):
        raise ProfileEvidenceError(f"{context} materialized root identity differs")
    return value, opened.st_dev, opened.st_ino


def _validate_release_build_child(
    attestation: Mapping[str, object], context: str
) -> tuple[
    Mapping[str, object],
    Path,
    tuple[int, int],
    tuple[str, int, int],
]:
    child = _exact_mapping(
        attestation.get("build_child"),
        _RELEASE_COMPILE_OUT_BUILD_CHILD_FIELDS,
        f"{context} build child",
    )
    reaping = _exact_mapping(
        child["reaping"],
        ("pid", "start_ticks", "status"),
        f"{context} build child reaping",
    )
    integer_fields = (
        "pid",
        "start_ticks",
        "started_monotonic_ns",
        "completed_monotonic_ns",
        "waited_pid",
    )
    root_identity = _materialized_root_identity(attestation, context)
    if (
        child["argv"] != attestation.get("build_argv")
        or child["cwd"] != attestation.get("materialized_root")
        or child["output_path"] != attestation.get("build_log_path")
        or child["output_sha256"] != attestation.get("build_log_sha256")
        or any(
            child[field] != attestation.get(f"build_{field}")
            for field in (
                "started_at",
                "started_monotonic_ns",
                "completed_at",
                "completed_monotonic_ns",
            )
        )
        or any(
            isinstance(child[field], bool)
            or not isinstance(child[field], int)
            or child[field] <= 0
            for field in integer_fields
        )
        or child["waited_pid"] != child["pid"]
        or type(child["passed_file_descriptors"]) is not int
        or child["passed_file_descriptors"] != 16
        or child["exit_status"] != 0
        or isinstance(child["exit_status"], bool)
        or child["timed_out"] is not False
        or child["process_group_absent"] is not True
        or reaping
        != {
            "pid": child["pid"],
            "start_ticks": child["start_ticks"],
            "status": "absent",
        }
        or child["completed_monotonic_ns"] < child["started_monotonic_ns"]
    ):
        raise ProfileEvidenceError(f"{context} build child authority differs")
    started_at = _authority_timestamp(child["started_at"], f"{context} build start")
    completed_at = _authority_timestamp(
        child["completed_at"], f"{context} build completion"
    )
    if completed_at < started_at:
        raise ProfileEvidenceError(f"{context} build wall chronology differs")
    log_path, log_payload, log_identity = _immutable_file_snapshot(
        attestation.get("build_log_path"),
        attestation.get("build_log_sha256"),
        0o444,
        f"{context} build log",
    )
    log = _exact_mapping(
        _canonical_json_payload(log_payload, f"{context} build log"),
        ("exit_status", "stderr", "stderr_sha256", "stdout", "stdout_sha256"),
        f"{context} build log",
    )
    stdout = log["stdout"]
    stderr = log["stderr"]
    if (
        log["exit_status"] != 0
        or log["exit_status"] != child["exit_status"]
        or not isinstance(stdout, str)
        or not isinstance(stderr, str)
        or log["stdout_sha256"] != _sha256_bytes(stdout.encode())
        or log["stderr_sha256"] != _sha256_bytes(stderr.encode())
    ):
        raise ProfileEvidenceError(f"{context} build log authority differs")
    return child, log_path, (log_identity[0], log_identity[1]), root_identity


def _resolution_sandbox_argv(
    descriptors: Mapping[str, int], cargo_arguments: Sequence[str], bwrap_path: str
) -> list[str]:
    argv = [
        bwrap_path,
        "--die-with-parent",
        "--new-session",
        "--unshare-net",
        "--dir",
        "/usr",
    ]
    for _host, guest in _TRUSTED_SYSTEM_MOUNTS:
        argv.extend(["--ro-bind-fd", str(descriptors[f"system:{guest}"]), guest])
    argv.extend(
        [
            "--symlink",
            "usr/bin",
            "/bin",
            "--symlink",
            "usr/lib",
            "/lib",
            "--symlink",
            "usr/lib",
            "/lib64",
            "--dir",
            "/dev",
            "--dir",
            "/proc",
            "--tmpfs",
            "/tmp",
            "--tmpfs",
            _GUEST_ROOT,
            "--bind-fd",
            str(descriptors["source"]),
            _GUEST_SOURCE,
            "--ro-bind-fd",
            str(descriptors["toolchain_root"]),
            _GUEST_TOOLCHAIN_ROOT,
            "--ro-bind-fd",
            str(descriptors["cargo"]),
            _GUEST_CARGO,
            "--ro-bind-fd",
            str(descriptors["rustc"]),
            _GUEST_RUSTC,
            "--overlay-src",
            f"/proc/self/fd/{descriptors['cargo_home']}",
            "--tmp-overlay",
            _GUEST_CARGO_HOME,
            "--dir",
            f"{_GUEST_ROOT}/.cargo",
            "--tmpfs",
            f"{_GUEST_ROOT}/.cargo",
            "--remount-ro",
            f"{_GUEST_ROOT}/.cargo",
            "--dir",
            "/.cargo",
            "--tmpfs",
            "/.cargo",
            "--remount-ro",
            "/.cargo",
            "--dir",
            f"{_GUEST_SOURCE}/.cargo",
            "--tmpfs",
            f"{_GUEST_SOURCE}/.cargo",
        ]
    )
    for guest in _GUEST_BOUND_CONFIG_PATHS[:2]:
        argv.extend(["--ro-bind-fd", str(descriptors[f"config:{guest}"]), guest])
    argv.extend(["--remount-ro", f"{_GUEST_SOURCE}/.cargo"])
    for guest in _GUEST_BOUND_CONFIG_PATHS[2:]:
        argv.extend(["--ro-bind-fd", str(descriptors[f"config:{guest}"]), guest])
    argv.extend(
        [
            "--remount-ro",
            _GUEST_CARGO_HOME,
            "--chdir",
            _GUEST_SOURCE,
            _GUEST_CARGO,
            *cargo_arguments,
        ]
    )
    return argv


def _current_sandbox(value: object, context: str) -> None:
    build = _as_mapping(value, context)
    argv = build.get("argv")
    toolchain = _as_mapping(build.get("toolchain"), f"{context} toolchain")
    if (
        not isinstance(argv, list)
        or any(not isinstance(argument, str) for argument in argv)
        or argv[:4]
        != [
            toolchain.get("bwrap_path"),
            "--die-with-parent",
            "--new-session",
            "--unshare-net",
        ]
        or ["--dir", "/dev"]
        not in [argv[index : index + 2] for index in range(len(argv) - 1)]
        or ["--dir", "/proc"]
        not in [argv[index : index + 2] for index in range(len(argv) - 1)]
        or any(forbidden in argv for forbidden in ("--dev", "--dev-bind", "--proc"))
    ):
        raise ProfileEvidenceError(f"{context} sandbox argv differs")
    environment = _as_mapping(build.get("environment"), f"{context} environment")
    required_environment = {
        "CARGO_HOME": _GUEST_CARGO_HOME,
        "CARGO_INCREMENTAL": "0",
        "CARGO_NET_OFFLINE": "true",
        "GIT_CONFIG_COUNT": "0",
        "GIT_CONFIG_GLOBAL": f"{_GUEST_ROOT}/absent-gitconfig",
        "GIT_CONFIG_NOSYSTEM": "1",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "LD_ORIGIN_PATH": _GUEST_TOOLCHAIN_BIN,
        "PATH": "/usr/bin:/bin",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "RUSTC": _GUEST_RUSTC,
        "RUSTUP_HOME": "/nonexistent",
        "TZ": "UTC",
    }
    if (
        any(
            environment.get(name) != expected
            for name, expected in required_environment.items()
        )
        or not isinstance(environment.get("RUSTUP_TOOLCHAIN"), str)
        or not environment["RUSTUP_TOOLCHAIN"]
        or any(
            name not in set(required_environment) | {"RUSTUP_TOOLCHAIN"}
            and not name.startswith("ASTERISM_")
            and name != "RUSTC_WORKSPACE_WRAPPER"
            for name in environment
        )
        or any(name in environment for name in ("CARGO_ENCODED_RUSTFLAGS", "RUSTFLAGS"))
    ):
        raise ProfileEvidenceError(f"{context} sandbox environment differs")


def _snapshot_reviewed_lock_manifest(
    current_children: Mapping[str, object], assertion: Mapping[str, object]
) -> tuple[dict[str, object], Path]:
    inputs = _as_mapping(assertion.get("inputs"), "source-review assertion inputs")
    reviewed = _exact_mapping(
        inputs.get("lock_manifest"),
        ("identity", "mode", "path", "schema", "sha256", "size"),
        "source-reviewed lock manifest",
    )
    authority = _as_mapping(
        current_children.get("lock_authority"), "current-child lock authority"
    )
    embedded = _exact_mapping(
        authority.get("lock_manifest"),
        ("identity", "mode", "path", "payload", "schema", "sha256", "size"),
        "current-child embedded lock manifest",
    )
    if (
        reviewed != {name: embedded[name] for name in reviewed}
        or embedded["schema"] != _LOCK_CANDIDATES_SCHEMA
        or embedded["mode"] != 0o444
    ):
        raise ProfileEvidenceError("reviewed lock-manifest binding differs")
    path, payload, _identity = _release_file_snapshot(
        {field: embedded[field] for field in _RELEASE_COMPILE_OUT_FILE_FIELDS},
        expected_mode=0o444,
        context="reviewed lock manifest",
    )
    value = _canonical_json_payload(payload, "reviewed lock manifest")
    if value != embedded["payload"] or value.get("schema") != _LOCK_CANDIDATES_SCHEMA:
        raise ProfileEvidenceError("reviewed lock-manifest payload differs")
    return value, path


def _replay_executable_binding(value: object, context: str) -> Mapping[str, object]:
    binding = _exact_mapping(value, _RELEASE_COMPILE_OUT_FILE_FIELDS, context)
    identity = _exact_mapping(
        binding["identity"], _RELEASE_COMPILE_OUT_IDENTITY_FIELDS, f"{context} identity"
    )
    if any(
        isinstance(identity[field], bool)
        or not isinstance(identity[field], int)
        or identity[field] < 0
        for field in _RELEASE_COMPILE_OUT_IDENTITY_FIELDS
    ):
        raise ProfileEvidenceError(f"{context} identity differs")
    path, payload, observed = _immutable_file_snapshot(
        binding["path"], binding["sha256"], None, context
    )
    observed_identity = {
        "changed_ns": observed[6],
        "device": observed[0],
        "inode": observed[1],
        "link_count": observed[2],
        "modified_ns": observed[5],
    }
    mode = binding["mode"]
    if (
        identity != observed_identity
        or _json_nonnegative_integer(binding["size"], f"{context} size")
        != len(payload)
        or isinstance(mode, bool)
        or not isinstance(mode, int)
        or mode != observed[4]
        or mode & 0o111 == 0
        or path != Path(str(binding["path"]))
    ):
        raise ProfileEvidenceError(f"{context} executable identity differs")
    return binding


def _validate_toolchain_contract(value: object, context: str) -> tuple[Mapping[str, object], Path]:
    """Replay the producer's exact sampled toolchain without executing it."""

    toolchain = _exact_mapping(value, _TOOLCHAIN_FIELDS, context)
    executable_identities: set[tuple[int, int]] = set()
    for name in ("bwrap", "cargo", "git", "rustc", "rust_lld", "rustup"):
        path_value = toolchain[f"{name}_path"]
        digest = toolchain[f"{name}_sha256"]
        path, payload, identity = _immutable_file_payload(
            path_value,
            None,
            f"{context} {name} executable",
            limit=None,
        )
        try:
            resolved = path.resolve(strict=True)
        except (OSError, RuntimeError) as error:
            raise ProfileEvidenceError(f"{context} {name} path cannot be resolved") from error
        if (
            _sha256_bytes(payload) != digest
            or resolved != path
            or identity[2] != 1
            or identity[4] & 0o111 == 0
            or (identity[0], identity[1]) in executable_identities
        ):
            raise ProfileEvidenceError(f"{context} {name} executable identity differs")
        executable_identities.add((identity[0], identity[1]))
    for name in ("cargo_home", "rustup_home"):
        path_value = toolchain[f"{name}_path"]
        if not isinstance(path_value, str):
            raise ProfileEvidenceError(f"{context} {name} path is not text")
        path = Path(path_value)
        try:
            metadata = path.lstat()
            resolved = path.resolve(strict=True)
        except (OSError, RuntimeError) as error:
            raise ProfileEvidenceError(f"{context} {name} path cannot be resolved") from error
        if (
            not path.is_absolute()
            or str(path) != path_value
            or resolved != path
            or not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
        ):
            raise ProfileEvidenceError(f"{context} {name} directory identity differs")
    for field, prefix in (
        ("cargo_version_verbose", "cargo "),
        ("rustc_version_verbose", "rustc "),
    ):
        version = toolchain[field]
        if (
            not isinstance(version, str)
            or not version
            or version != version.strip()
            or not version.splitlines()[0].startswith(prefix)
        ):
            raise ProfileEvidenceError(f"{context} {field} differs")
    rustc_version = str(toolchain["rustc_version_verbose"])
    host_lines = [
        line.removeprefix("host: ")
        for line in rustc_version.splitlines()
        if line.startswith("host: ")
    ]
    rustc_host = toolchain["rustc_host"]
    rustup_toolchain = toolchain["rustup_toolchain"]
    if (
        len(host_lines) != 1
        or host_lines[0] != rustc_host
        or not isinstance(rustc_host, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", rustc_host) is None
        or not isinstance(rustup_toolchain, str)
        or not rustup_toolchain
        or any(character.isspace() for character in rustup_toolchain)
    ):
        raise ProfileEvidenceError(f"{context} sampled version identity differs")
    cargo_root = Path(str(toolchain["cargo_path"])).parent.parent
    rustc_root = Path(str(toolchain["rustc_path"])).parent.parent
    try:
        cargo_root = cargo_root.resolve(strict=True)
        rustc_root = rustc_root.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ProfileEvidenceError(f"{context} semantic toolchain root is absent") from error
    if cargo_root != rustc_root or not cargo_root.is_dir():
        raise ProfileEvidenceError(f"{context} Cargo/rustc semantic roots differ")
    expected_rust_lld = (
        cargo_root / "lib" / "rustlib" / str(rustc_host) / "bin" / "rust-lld"
    )
    if Path(str(toolchain["rust_lld_path"])) != expected_rust_lld:
        raise ProfileEvidenceError(f"{context} rust-lld topology differs")
    return toolchain, cargo_root


_CURRENT_FILE_IDENTITY_FIELDS = (
    "bytes",
    "ctime_ns",
    "device",
    "inode",
    "link_count",
    "mode",
    "mtime_ns",
    "path",
    "sha256",
    "size",
)
_CURRENT_DIRECTORY_IDENTITY_FIELDS = (
    "changed_ns",
    "device",
    "file_type",
    "inode",
    "link_count",
    "modified_ns",
    "path",
    "permissions",
    "size",
)
_CURRENT_PATH_CHAIN_FIELDS = (
    "changed_ns",
    "device",
    "gid",
    "inode",
    "link_count",
    "mode",
    "modified_ns",
    "path",
    "size",
    "type",
    "uid",
)


def _current_file_identity(
    value: object,
    context: str,
    *,
    expected_path: Path | None = None,
    executable: bool,
    expected_link_count: int = 1,
    replay_live: bool = True,
) -> Mapping[str, object]:
    identity = _exact_mapping(value, _CURRENT_FILE_IDENTITY_FIELDS, context)
    path_value = identity["path"]
    digest = _sha256_authority(identity["sha256"], context)
    if not isinstance(path_value, str):
        raise ProfileEvidenceError(f"{context} path is not text")
    if expected_path is not None and path_value != str(expected_path):
        raise ProfileEvidenceError(f"{context} path differs")
    for field in ("bytes", "ctime_ns", "device", "inode", "link_count", "mode", "mtime_ns", "size"):
        _json_nonnegative_integer(identity[field], f"{context} {field}")
    if (
        identity["bytes"] != identity["size"]
        or identity["link_count"] != expected_link_count
    ):
        raise ProfileEvidenceError(f"{context} size/link identity differs")
    if executable and int(identity["mode"]) & 0o111 == 0:
        raise ProfileEvidenceError(f"{context} is not executable")
    if replay_live:
        path, payload, observed = _immutable_file_payload(
            path_value, None, context, limit=None
        )
        if _sha256_bytes(payload) != digest:
            raise ProfileEvidenceError(f"{context} SHA-256 differs")
        if expected_path is not None and path != expected_path:
            raise ProfileEvidenceError(f"{context} selected path differs")
        expected = {
            "bytes": len(payload),
            "ctime_ns": observed[6],
            "device": observed[0],
            "inode": observed[1],
            "link_count": observed[2],
            "mode": observed[4],
            "mtime_ns": observed[5],
            "path": str(path),
            "sha256": digest,
            "size": observed[3],
        }
        if identity != expected:
            raise ProfileEvidenceError(f"{context} live identity differs")
    return identity


def _current_path_chain(path: Path, value: object, context: str) -> None:
    if not isinstance(value, list):
        raise ProfileEvidenceError(f"{context} path chain differs")
    paths = [Path("/")]
    selected = Path("/")
    for part in path.parts[1:]:
        selected /= part
        paths.append(selected)
    if len(value) != len(paths):
        raise ProfileEvidenceError(f"{context} path chain length differs")
    for ordinal, (item, selected) in enumerate(zip(value, paths, strict=True), start=1):
        record = _exact_mapping(item, _CURRENT_PATH_CHAIN_FIELDS, f"{context} path chain {ordinal}")
        metadata = selected.lstat()
        expected = {
            "changed_ns": metadata.st_ctime_ns,
            "device": metadata.st_dev,
            "gid": metadata.st_gid,
            "inode": metadata.st_ino,
            "link_count": metadata.st_nlink,
            "mode": stat.S_IMODE(metadata.st_mode),
            "modified_ns": metadata.st_mtime_ns,
            "path": str(selected),
            "size": metadata.st_size,
            "type": stat.S_IFMT(metadata.st_mode),
            "uid": metadata.st_uid,
        }
        if record != expected or metadata.st_uid != 0 or stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ProfileEvidenceError(f"{context} trusted path chain differs")


def _current_retained_file(
    value: object,
    context: str,
    *,
    expected_path: Path,
    expected_sha256: object,
    trusted_system: bool,
) -> Mapping[str, object]:
    record = _exact_mapping(value, ("identity", "path_chain", "trusted_system"), context)
    identity = _current_file_identity(
        record["identity"], context, expected_path=expected_path, executable=True
    )
    if identity["sha256"] != expected_sha256 or record["trusted_system"] is not trusted_system:
        raise ProfileEvidenceError(f"{context} retained authority differs")
    if trusted_system:
        _current_path_chain(expected_path, record["path_chain"], context)
    elif record["path_chain"] is not None:
        raise ProfileEvidenceError(f"{context} non-system path chain differs")
    return record


def _current_retained_null_device(
    value: object, context: str
) -> Mapping[str, object]:
    record = _exact_mapping(
        value, ("identity", "parent_path_chain", "trusted_system"), context
    )
    identity = _exact_mapping(
        record["identity"],
        (
            "changed_ns",
            "device",
            "gid",
            "inode",
            "link_count",
            "major",
            "minor",
            "modified_ns",
            "path",
            "permissions",
            "size",
            "type",
            "uid",
        ),
        f"{context} identity",
    )
    metadata = Path("/dev/null").lstat()
    expected = {
        "changed_ns": metadata.st_ctime_ns,
        "device": metadata.st_dev,
        "gid": metadata.st_gid,
        "inode": metadata.st_ino,
        "link_count": metadata.st_nlink,
        "major": os.major(metadata.st_rdev),
        "minor": os.minor(metadata.st_rdev),
        "modified_ns": metadata.st_mtime_ns,
        "path": "/dev/null",
        "permissions": stat.S_IMODE(metadata.st_mode),
        "size": metadata.st_size,
        "type": stat.S_IFMT(metadata.st_mode),
        "uid": metadata.st_uid,
    }
    for field in expected:
        if field != "path":
            _json_nonnegative_integer(identity[field], f"{context} {field}")
    if (
        identity != expected
        or record["trusted_system"] is not True
        or not stat.S_ISCHR(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o666
        or metadata.st_nlink != 1
        or os.major(metadata.st_rdev) != 1
        or os.minor(metadata.st_rdev) != 3
    ):
        raise ProfileEvidenceError(f"{context} null-device authority differs")
    _current_path_chain(Path("/dev"), record["parent_path_chain"], context)
    return record


def _validate_prepared_execution_tools(
    attestation: Mapping[str, object], context: str
) -> str:
    tools = _exact_mapping(
        attestation.get("execution_tools"),
        ("bwrap", "cargo", "dev_null", "rustc", "rust_lld", "toolchain_root"),
        f"{context} execution tools",
    )
    toolchain = _as_mapping(attestation.get("toolchain"), f"{context} toolchain")
    rustc_host = toolchain.get("rustc_host")
    cargo_path = Path(str(toolchain.get("cargo_path")))
    rustc_path = Path(str(toolchain.get("rustc_path")))
    toolchain_root = cargo_path.parent.parent
    if (
        not isinstance(rustc_host, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", rustc_host) is None
        or rustc_path.parent.parent != toolchain_root
    ):
        raise ProfileEvidenceError(f"{context} execution toolchain differs")
    rust_lld_path = (
        toolchain_root / "lib" / "rustlib" / rustc_host / "bin" / "rust-lld"
    )
    if Path(str(toolchain.get("rust_lld_path"))) != rust_lld_path:
        raise ProfileEvidenceError(f"{context} rust-lld topology differs")
    expected_files = {
        "bwrap": (Path(str(toolchain.get("bwrap_path"))), toolchain.get("bwrap_sha256")),
        "cargo": (cargo_path, toolchain.get("cargo_sha256")),
        "rustc": (rustc_path, toolchain.get("rustc_sha256")),
        "rust_lld": (
            Path(str(toolchain.get("rust_lld_path"))),
            toolchain.get("rust_lld_sha256"),
        ),
    }
    for name, (path, expected_sha256) in expected_files.items():
        binding = _exact_mapping(
            tools[name], ("identity", "mode", "path", "sha256", "size"),
            f"{context} {name} execution tool",
        )
        binding_identity = _exact_mapping(
            binding["identity"],
            ("changed_ns", "device", "inode", "link_count", "modified_ns"),
            f"{context} {name} execution identity",
        )
        live_path, payload, observed = _immutable_file_payload(
            str(path), None, f"{context} {name} execution tool"
        )
        expected = {
            "identity": {
                "changed_ns": observed[6],
                "device": observed[0],
                "inode": observed[1],
                "link_count": observed[2],
                "modified_ns": observed[5],
            },
            "mode": observed[4],
            "path": str(live_path),
            "sha256": _sha256_bytes(payload),
            "size": observed[3],
        }
        for field in expected["identity"]:
            _json_nonnegative_integer(
                binding_identity[field],
                f"{context} {name} execution identity {field}",
            )
        _json_nonnegative_integer(
            binding["mode"], f"{context} {name} execution mode"
        )
        _json_nonnegative_integer(
            binding["size"], f"{context} {name} execution size"
        )
        if (
            binding != expected
            or observed[2] != 1
            or observed[4] & 0o111 == 0
            or (expected_sha256 is not None and binding["sha256"] != expected_sha256)
        ):
            raise ProfileEvidenceError(f"{context} {name} execution binding differs")
    _current_retained_null_device(tools["dev_null"], f"{context} dev-null tool")
    root_metadata = toolchain_root.lstat()
    root_binding = _exact_mapping(
        tools["toolchain_root"],
        ("device", "inode", "link_count", "mode"),
        f"{context} toolchain-root binding",
    )
    for field in root_binding:
        _json_nonnegative_integer(
            root_binding[field], f"{context} toolchain-root {field}"
        )
    if root_binding != {
        "device": root_metadata.st_dev,
        "inode": root_metadata.st_ino,
        "link_count": root_metadata.st_nlink,
        "mode": stat.S_IMODE(root_metadata.st_mode),
    }:
        raise ProfileEvidenceError(f"{context} toolchain-root binding differs")
    return _sha256_bytes(canonical_json(tools))


def _current_directory_identity(
    value: object, context: str, *, expected_path: Path | None = None, replay_live: bool
) -> Mapping[str, object]:
    record = _exact_mapping(value, _CURRENT_DIRECTORY_IDENTITY_FIELDS, context)
    if not isinstance(record["path"], str):
        raise ProfileEvidenceError(f"{context} path is not text")
    if expected_path is not None and record["path"] != str(expected_path):
        raise ProfileEvidenceError(f"{context} path differs")
    for field in _CURRENT_DIRECTORY_IDENTITY_FIELDS:
        if field == "path":
            continue
        _json_nonnegative_integer(record[field], f"{context} {field}")
    if record["file_type"] != stat.S_IFDIR:
        raise ProfileEvidenceError(f"{context} is not a directory")
    if replay_live:
        path = Path(str(record["path"]))
        metadata = path.lstat()
        expected = {
            "changed_ns": metadata.st_ctime_ns,
            "device": metadata.st_dev,
            "file_type": stat.S_IFMT(metadata.st_mode),
            "inode": metadata.st_ino,
            "link_count": metadata.st_nlink,
            "modified_ns": metadata.st_mtime_ns,
            "path": str(path),
            "permissions": stat.S_IMODE(metadata.st_mode),
            "size": metadata.st_size,
        }
        if record != expected or path.resolve(strict=True) != path:
            raise ProfileEvidenceError(f"{context} live directory differs")
    return record


def _validate_current_cargo_config(
    value: object,
    context: str,
    *,
    source_root: Path,
    cargo_home: Path,
    semantic: Mapping[str, object],
    expected_entries: Sequence[Mapping[str, object]],
) -> tuple[Mapping[str, object], tuple[tuple[str, str, str], ...]]:
    record = _exact_mapping(
        value,
        ("cargo_search", "cargo_home_tree", "preserved_top_level_entries", "schema"),
        context,
    )
    if record["schema"] != _CURRENT_CARGO_CONFIG_SCHEMA:
        raise ProfileEvidenceError(f"{context} schema differs")
    search = _exact_mapping(
        record["cargo_search"], ("cargo_home_path", "cwd", "entries", "schema"), context
    )
    if (
        search["schema"] != _CARGO_CONFIG_SEARCH_SCHEMA
        or search["cargo_home_path"] != _GUEST_CARGO_HOME
        or search["cwd"] != _GUEST_SOURCE
        or not isinstance(search["entries"], list)
        or len(search["entries"]) != len(_GUEST_BOUND_CONFIG_PATHS) + 4
        or search["entries"] != list(expected_entries)
    ):
        raise ProfileEvidenceError(f"{context} Cargo search differs")
    candidates = (
        (f"{_GUEST_SOURCE}/.cargo/config.toml", source_root / ".cargo/config.toml"),
        (f"{_GUEST_SOURCE}/.cargo/config", source_root / ".cargo/config"),
        (f"{_GUEST_ROOT}/.cargo/config.toml", None),
        (f"{_GUEST_ROOT}/.cargo/config", None),
        ("/.cargo/config.toml", None),
        ("/.cargo/config", None),
        (f"{_GUEST_CARGO_HOME}/config.toml", cargo_home / "config.toml"),
        (f"{_GUEST_CARGO_HOME}/config", cargo_home / "config"),
    )
    for ordinal, (item, (guest, host)) in enumerate(zip(search["entries"], candidates, strict=True), start=1):
        entry = _exact_mapping(item, ("path", "sha256", "status"), f"{context} entry {ordinal}")
        if entry["path"] != guest:
            raise ProfileEvidenceError(f"{context} Cargo search order differs")
        if host is not None and host.is_symlink():
            raise ProfileEvidenceError(f"{context} Cargo config is a symlink")
        if host is None:
            if entry != {"path": guest, "sha256": None, "status": "absent"}:
                raise ProfileEvidenceError(f"{context} absent Cargo config differs")
        else:
            expected_sha256 = _EMPTY_SHA256
            if host.exists():
                _path, payload, identity = _immutable_file_payload(
                    str(host), None, f"{context} Cargo config"
                )
                if identity[2] != 1:
                    raise ProfileEvidenceError(
                        f"{context} present Cargo config differs"
                    )
                expected_sha256 = _sha256_bytes(payload)
            if entry != {
                "path": guest,
                "sha256": expected_sha256,
                "status": "present",
            }:
                raise ProfileEvidenceError(f"{context} bound Cargo config differs")
    cargo_tree = _exact_mapping(
        record["cargo_home_tree"],
        ("entry_count", "equal_pre_post", "path", "post_sha256", "pre_sha256", "watch_count"),
        context,
    )
    semantic_cargo = _as_mapping(semantic["cargo_home"], f"{context} semantic Cargo home")
    if cargo_tree != {
        "entry_count": semantic_cargo.get("entry_count"),
        "equal_pre_post": True,
        "path": semantic_cargo.get("manifest_path"),
        "post_sha256": semantic_cargo.get("manifest_sha256"),
        "pre_sha256": semantic_cargo.get("manifest_sha256"),
        "watch_count": semantic_cargo.get("watch_count"),
    }:
        raise ProfileEvidenceError(f"{context} Cargo-home tree differs")
    preserved = _exact_mapping(
        record["preserved_top_level_entries"], ("cargo-home", "source"), context
    )
    bindings: list[tuple[str, str, str]] = []
    for origin, root, guest_root in (
        ("source", source_root / ".cargo", f"{_GUEST_SOURCE}/.cargo"),
        ("cargo-home", cargo_home, _GUEST_CARGO_HOME),
    ):
        try:
            if root.resolve(strict=True) != root or not root.is_dir():
                raise ProfileEvidenceError(f"{context} preserved {origin} root differs")
        except (OSError, RuntimeError) as error:
            raise ProfileEvidenceError(
                f"{context} preserved {origin} root is absent"
            ) from error
        entries = preserved[origin]
        if not isinstance(entries, list):
            raise ProfileEvidenceError(f"{context} preserved {origin} differs")
        live_names = sorted(path.name for path in root.iterdir() if path.name not in {"config", "config.toml"})
        if [item.get("name") if isinstance(item, Mapping) else None for item in entries] != live_names:
            raise ProfileEvidenceError(f"{context} preserved {origin} names differ")
        for item in entries:
            entry = _exact_mapping(item, ("identity", "name", "type"), f"{context} preserved {origin}")
            path = root / str(entry["name"])
            destination = f"{guest_root}/{entry['name']}"
            if entry["type"] == "regular":
                _current_file_identity(entry["identity"], context, expected_path=path, executable=False)
                if origin == "source":
                    bindings.append((f"preserved:{origin}:{entry['name']}", "--ro-bind-data", destination))
            elif entry["type"] == "directory":
                _current_directory_identity(entry["identity"], context, expected_path=path, replay_live=True)
                if origin == "source":
                    bindings.append((f"preserved:{origin}:{entry['name']}", "--ro-bind-fd", destination))
            else:
                raise ProfileEvidenceError(f"{context} preserved {origin} type differs")
    for guest, _host in (candidates[0], candidates[1]):
        bindings.append((f"config:{guest}", "--ro-bind-data", guest))
    bindings.append(("cargo_home", "--tmp-overlay", _GUEST_CARGO_HOME))
    for guest, _host in (candidates[6], candidates[7]):
        bindings.append((f"config:{guest}", "--ro-bind-data", guest))
    return record, tuple(bindings)


def _current_build_argv(
    descriptors: Mapping[str, int],
    config_bindings: Sequence[tuple[str, str, str]],
    *,
    bwrap_path: str,
    rustc_host: str,
    examples: Sequence[str],
    wrapper: bool,
) -> list[str]:
    rust_lld_guest_path = (
        f"{_GUEST_TOOLCHAIN_ROOT}/lib/rustlib/{rustc_host}/bin/gcc-ld/ld.lld"
    )
    argv = [
        bwrap_path,
        "--die-with-parent",
        "--new-session",
        "--unshare-net",
        "--dir",
        "/usr",
    ]
    for _host, guest in _TRUSTED_SYSTEM_MOUNTS:
        argv.extend(["--ro-bind-fd", str(descriptors[f"system:{guest}"]), guest])
    argv.extend(
        [
            "--symlink",
            "usr/bin",
            "/bin",
            "--symlink",
            "usr/lib",
            "/lib",
            "--symlink",
            "usr/lib",
            "/lib64",
            "--dir",
            "/dev",
            "--dev-bind",
            f"/proc/self/fd/{descriptors['dev_null']}",
            "/dev/null",
            "--dir",
            "/proc",
            "--tmpfs",
            "/tmp",
            "--tmpfs",
            _GUEST_ROOT,
            "--ro-bind-fd",
            str(descriptors["source"]),
            _GUEST_SOURCE,
            "--ro-bind-fd",
            str(descriptors["toolchain_root"]),
            _GUEST_TOOLCHAIN_ROOT,
            "--ro-bind-fd",
            str(descriptors["cargo"]),
            _GUEST_CARGO,
            "--ro-bind-fd",
            str(descriptors["rustc"]),
            _GUEST_RUSTC,
            "--ro-bind-fd",
            str(descriptors["rust_lld"]),
            rust_lld_guest_path,
            "--ro-bind-fd",
            str(descriptors["python"]),
            f"{_GUEST_ROOT}/python3",
            "--dir",
            f"{_GUEST_SOURCE}/.cargo",
            "--tmpfs",
            f"{_GUEST_SOURCE}/.cargo",
        ]
    )
    cargo_home_seen = False
    for name, option, destination in config_bindings:
        if name == "cargo_home":
            if option != "--tmp-overlay" or destination != _GUEST_CARGO_HOME:
                raise ProfileEvidenceError(
                    "current build Cargo-home overlay binding differs"
                )
            argv.extend(
                [
                    "--remount-ro",
                    f"{_GUEST_SOURCE}/.cargo",
                    "--overlay-src",
                    f"/proc/self/fd/{descriptors[name]}",
                    "--tmp-overlay",
                    destination,
                ]
            )
            cargo_home_seen = True
            continue
        argv.extend([option, str(descriptors[name]), destination])
    if not cargo_home_seen:
        raise ProfileEvidenceError("current build Cargo-home binding is absent")
    argv.extend(
        [
            "--remount-ro",
            _GUEST_CARGO_HOME,
            "--dir",
            f"{_GUEST_ROOT}/.cargo",
            "--tmpfs",
            f"{_GUEST_ROOT}/.cargo",
            "--remount-ro",
            f"{_GUEST_ROOT}/.cargo",
            "--dir",
            "/.cargo",
            "--tmpfs",
            "/.cargo",
            "--remount-ro",
            "/.cargo",
            "--bind-fd",
            str(descriptors["target"]),
            _GUEST_TARGET,
        ]
    )
    if wrapper:
        argv.extend(
            [
                "--ro-bind-fd",
                str(descriptors["wrapper"]),
                f"{_GUEST_ROOT}/rustc_workspace_wrapper.py",
                "--bind-fd",
                str(descriptors["receipt"]),
                f"{_GUEST_ROOT}/receipt",
            ]
        )
    argv.extend(
        [
            "--chdir",
            _GUEST_SOURCE,
            _GUEST_CARGO,
            "build",
            "--locked",
            "--offline",
            "--release",
            "-p",
            "mess-store",
        ]
    )
    for example in examples:
        argv.extend(["--example", example])
    argv.extend(["--target-dir", _GUEST_TARGET])
    return argv


def _current_base_environment(toolchain: Mapping[str, object]) -> dict[str, str]:
    return {
        "CARGO_HOME": _GUEST_CARGO_HOME,
        "CARGO_INCREMENTAL": "0",
        "CARGO_NET_OFFLINE": "true",
        "GIT_CONFIG_COUNT": "0",
        "GIT_CONFIG_GLOBAL": f"{_GUEST_ROOT}/absent-gitconfig",
        "GIT_CONFIG_NOSYSTEM": "1",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "LD_ORIGIN_PATH": _GUEST_TOOLCHAIN_BIN,
        "PATH": "/usr/bin:/bin",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "RUSTC": _GUEST_RUSTC,
        "RUSTUP_HOME": "/nonexistent",
        "RUSTUP_TOOLCHAIN": str(toolchain["rustup_toolchain"]),
        "TZ": "UTC",
    }


def _current_build_environment(
    value: object,
    context: str,
    *,
    child: bool,
    current: Mapping[str, object],
    toolchain: Mapping[str, object],
    lock_sha256: str,
) -> Mapping[str, object]:
    environment = _as_mapping(value, f"{context} environment")
    expected = _current_base_environment(toolchain)
    if child:
        compile_out = _as_mapping(current["release_compile_out"], "current release compile-out")
        expected.update(
            {
                "ASTERISM_FAULT_COMPILE_OUT_IDENTICAL": "true",
                "ASTERISM_FAULT_COMPILE_OUT_OVERLAY_RELEASE_SHA256": str(compile_out.get("overlay_release_sha256")),
                "ASTERISM_FAULT_COMPILE_OUT_PRISTINE_SHA256": str(compile_out.get("pristine_sha256")),
                "ASTERISM_FAULT_COMPILE_OUT_SCHEMA": _CURRENT_FAULT_COMPILE_OUT_SCHEMA,
                "ASTERISM_FAULT_COMPILE_OUT_SYMBOL_ABSENCE_SHA256": str(compile_out.get("symbol_absence_sha256")),
                "ASTERISM_REBASELINE_CHILD_BUILD_NONCE": str(current["build_nonce"]),
                "ASTERISM_REBASELINE_EXPECTED_LIB_SOURCE": _CURRENT_EXPECTED_LIB_SOURCE,
                "ASTERISM_REBASELINE_PINNED_RUSTC": _GUEST_RUSTC,
                "ASTERISM_REBASELINE_WRAPPER_RECEIPT": f"{_GUEST_ROOT}/receipt/injection.json",
                "RUSTC_WORKSPACE_WRAPPER": f"{_GUEST_ROOT}/rustc_workspace_wrapper.py",
            }
        )
        for field in (
            "ASTERISM_FAULT_COMPILE_OUT_OVERLAY_RELEASE_SHA256",
            "ASTERISM_FAULT_COMPILE_OUT_PRISTINE_SHA256",
            "ASTERISM_FAULT_COMPILE_OUT_SYMBOL_ABSENCE_SHA256",
        ):
            _sha256_authority(expected[field], f"{context} {field}")
    else:
        extras = {name: environment.get(name) for name in _RELEASE_BUILD_ENVIRONMENT_FIELDS}
        if (
            extras["ASTERISM_BUILD_BINARY_KIND"] != "public"
            or extras["ASTERISM_BUILD_NONCE"] != current["build_nonce"]
            or extras["ASTERISM_BUILD_CARGO_LOCK_SHA256"] != lock_sha256
            or extras["ASTERISM_BUILD_PRODUCT_COMMIT"] != current["product_commit"]
            or extras["ASTERISM_BUILD_PRODUCT_TREE"] != current["product_tree"]
            or extras["ASTERISM_BUILD_PROTOCOL"] != current["protocol"]
            or extras["ASTERISM_BUILD_PROTOCOL_SHA256"] != current["protocol_sha256"]
            or extras["ASTERISM_BUILD_SOURCE_APPROVAL_SHA256"]
            != "fa2acb626f303f8a65a16a6c8a1fd86b7e80cf48e092ae21a7308984ae790c94"
            or extras["ASTERISM_BUILD_TIMED_SURFACE"] != "public-event-store"
            or extras["ASTERISM_BUILD_VARIANT"] != "A"
        ):
            raise ProfileEvidenceError(f"{context} release contract environment differs")
        for field in (
            "ASTERISM_BUILD_ADAPTER_SHA256",
            "ASTERISM_BUILD_CARGO_LOCK_SHA256",
            "ASTERISM_BUILD_NONCE",
            "ASTERISM_BUILD_PROTOCOL_SHA256",
            "ASTERISM_BUILD_SHARED_MANIFEST_SHA256",
            "ASTERISM_BUILD_SOURCE_APPROVAL_SHA256",
        ):
            _sha256_authority(extras[field], f"{context} {field}")
        for field in ("ASTERISM_BUILD_PRODUCT_COMMIT", "ASTERISM_BUILD_PRODUCT_TREE", "ASTERISM_BUILD_TOOLING_COMMIT", "ASTERISM_BUILD_TOOLING_TREE"):
            candidate = extras[field]
            if not isinstance(candidate, str) or re.fullmatch(r"[0-9a-f]{40}", candidate) is None:
                raise ProfileEvidenceError(f"{context} {field} differs")
        expected.update({name: str(extras[name]) for name in _RELEASE_BUILD_ENVIRONMENT_FIELDS})
    if environment != expected or any(name in environment for name in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC_WRAPPER")):
        raise ProfileEvidenceError(f"{context} exact environment differs")
    return environment


def _current_lock_record(value: object, context: str, expected_path: Path) -> tuple[Mapping[str, object], str]:
    record = _exact_mapping(value, _RELEASE_COMPILE_OUT_FILE_FIELDS, context)
    path, _payload, _identity = _release_file_snapshot(record, expected_mode=0o444, context=context)
    if path != expected_path:
        raise ProfileEvidenceError(f"{context} path differs")
    return record, str(record["sha256"])


def _current_bound_file_identity(
    value: object,
    context: str,
    *,
    actual_path: Path,
    logical_path: str,
    executable: bool,
    expected_link_count: int = 1,
) -> Mapping[str, object]:
    record = _current_file_identity(
        value,
        context,
        expected_path=Path(logical_path),
        executable=executable,
        expected_link_count=expected_link_count,
        replay_live=False,
    )
    exact = actual_path.resolve(strict=True)
    path, payload, observed = _immutable_file_payload(
        str(exact), 0o555 if executable else 0o444, context, limit=None
    )
    expected = {
        "bytes": len(payload),
        "ctime_ns": observed[6],
        "device": observed[0],
        "inode": observed[1],
        "link_count": observed[2],
        "mode": observed[4],
        "mtime_ns": observed[5],
        "path": logical_path,
        "sha256": _sha256_bytes(payload),
        "size": observed[3],
    }
    if path != exact or exact != actual_path or record != expected:
        raise ProfileEvidenceError(f"{context} retained descriptor identity differs")
    return record


def _validate_current_build_record(
    value: object,
    context: str,
    *,
    name: str,
    source_name: str,
    current: Mapping[str, object],
    current_root: Path,
    toolchain: Mapping[str, object],
    toolchain_root: Path,
    filesystem_admission: object,
    expected_cargo_config_entries: Sequence[Mapping[str, object]],
) -> Mapping[str, object]:
    child = name == "children"
    fields = (*_CURRENT_BUILD_FIELDS, *_CURRENT_CHILD_WRAPPER_FIELDS) if child else _CURRENT_BUILD_FIELDS
    build = _exact_mapping(value, fields, context)
    source_root = current_root / "materialized" / source_name
    target = current_root / "targets" / source_name
    if build["target"] != str(target) or build["target_was_absent"] is not True:
        raise ProfileEvidenceError(f"{context} target topology differs")
    target.resolve(strict=True)
    if build["filesystem_admission"] != filesystem_admission:
        raise ProfileEvidenceError(f"{context} filesystem admission differs")
    semantic = _as_mapping(build["semantic_input_authority"], f"{context} semantic authority")
    config_pre, config_bindings = _validate_current_cargo_config(
        build["cargo_config_prebuild"],
        f"{context} prebuild Cargo config",
        source_root=source_root,
        cargo_home=Path(str(toolchain["cargo_home_path"])),
        semantic=semantic,
        expected_entries=expected_cargo_config_entries,
    )
    if build["cargo_config_postbuild"] != config_pre:
        raise ProfileEvidenceError(f"{context} Cargo config changed")
    lock_pre, lock_sha256 = _current_lock_record(
        build["lock_prebuild"], f"{context} prebuild lock", source_root / "Cargo.lock"
    )
    if build["lock_postbuild"] != lock_pre:
        raise ProfileEvidenceError(f"{context} lock changed")
    examples = (
        ("asterism_rebaseline_current_correctness", "asterism_rebaseline_current_fault")
        if child
        else ("asterism_rebaseline_public",)
    )
    rustc_host = toolchain.get("rustc_host")
    if (
        not isinstance(rustc_host, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", rustc_host) is None
    ):
        raise ProfileEvidenceError(f"{context} rustc host differs")
    rust_lld_path = (
        toolchain_root / "lib" / "rustlib" / rustc_host / "bin" / "rust-lld"
    )
    rust_lld_guest_path = (
        f"{_GUEST_TOOLCHAIN_ROOT}/lib/rustlib/{rustc_host}/bin/gcc-ld/ld.lld"
    )
    expected_bindings = (
        *((f"system:{guest}", "--ro-bind-fd", guest) for _host, guest in _TRUSTED_SYSTEM_MOUNTS),
        ("dev_null", "--dev-bind", "/dev/null"),
        ("source", "--ro-bind-fd", _GUEST_SOURCE),
        ("toolchain_root", "--ro-bind-fd", _GUEST_TOOLCHAIN_ROOT),
        ("cargo", "--ro-bind-fd", _GUEST_CARGO),
        ("rustc", "--ro-bind-fd", _GUEST_RUSTC),
        ("rust_lld", "--ro-bind-fd", rust_lld_guest_path),
        ("python", "--ro-bind-fd", f"{_GUEST_ROOT}/python3"),
        *config_bindings,
        ("target", "--bind-fd", _GUEST_TARGET),
        *(
            (
                ("wrapper", "--ro-bind-fd", f"{_GUEST_ROOT}/rustc_workspace_wrapper.py"),
                ("receipt", "--bind-fd", f"{_GUEST_ROOT}/receipt"),
            )
            if child
            else ()
        ),
    )
    _normalized, descriptors = _sandbox_descriptors(build["argv"], expected_bindings, context)
    if build["argv"] != _current_build_argv(
        descriptors,
        config_bindings,
        bwrap_path=str(toolchain["bwrap_path"]),
        rustc_host=rustc_host,
        examples=examples,
        wrapper=child,
    ):
        raise ProfileEvidenceError(f"{context} exact sandbox argv differs")
    argv = build["argv"]
    assert isinstance(argv, list)
    if any(forbidden in argv for forbidden in ("--dev", "--proc", "--share-net")) or any(
        argv[index : index + 3] in (["--ro-bind", "/", "/"], ["--bind", "/", "/"])
        for index in range(max(0, len(argv) - 2))
    ):
        raise ProfileEvidenceError(f"{context} exposes forbidden host authority")
    environment = _current_build_environment(
        build["environment"],
        context,
        child=child,
        current=current,
        toolchain=toolchain,
        lock_sha256=lock_sha256,
    )
    tools = _exact_mapping(
        build["execution_tools"],
        ("bwrap", "cargo", "dev_null", "python", "rustc", "rust_lld", "toolchain_root"),
        context,
    )
    bwrap_record = _current_retained_file(
        tools["bwrap"], context, expected_path=Path(str(toolchain["bwrap_path"])), expected_sha256=toolchain["bwrap_sha256"], trusted_system=True
    )
    _current_retained_file(
        tools["cargo"], context, expected_path=Path(str(toolchain["cargo_path"])), expected_sha256=toolchain["cargo_sha256"], trusted_system=False
    )
    _current_retained_file(
        tools["rustc"], context, expected_path=Path(str(toolchain["rustc_path"])), expected_sha256=toolchain["rustc_sha256"], trusted_system=False
    )
    _current_retained_file(
        tools["rust_lld"], context, expected_path=rust_lld_path,
        expected_sha256=_sha256_bytes(rust_lld_path.read_bytes()),
        trusted_system=False,
    )
    _current_retained_null_device(tools["dev_null"], f"{context} null device")
    system_python = Path("/usr/bin/python3").resolve(strict=True)
    _current_retained_file(
        tools["python"], context, expected_path=system_python, expected_sha256=_sha256_bytes(system_python.read_bytes()), trusted_system=True
    )
    _current_directory_identity(
        tools["toolchain_root"], context, expected_path=toolchain_root, replay_live=True
    )
    execution = _exact_mapping(
        build["execution"],
        ("argv", "cwd", "environment", "exit_status", "execution_authority", "passed_file_descriptors", "stderr_bytes", "stderr_sha256", "stdout_bytes", "stdout_sha256"),
        f"{context} execution",
    )
    preserved_entries = _as_mapping(
        config_pre["preserved_top_level_entries"], f"{context} preserved config"
    )
    cargo_home_preserved = preserved_entries["cargo-home"]
    assert isinstance(cargo_home_preserved, list)
    expected_passed_descriptors = (
        len(expected_bindings)
        + 1  # retained source .cargo directory guard is not itself bound
        + len(cargo_home_preserved)  # retained children are covered by the root bind
        + 1  # run_capture also inherits the retained bwrap execution lease
    )
    if (
        execution["argv"] != argv
        or execution["environment"] != environment
        or execution["cwd"] != str(current_root)
        or _json_nonnegative_integer(
            execution["exit_status"], f"{context} exit status"
        )
        != 0
        or execution["exit_status"] != 0
        or execution["execution_authority"] != bwrap_record
        or _json_nonnegative_integer(
            execution["passed_file_descriptors"],
            f"{context} passed file descriptors",
        )
        != expected_passed_descriptors
        or execution["passed_file_descriptors"] != expected_passed_descriptors
    ):
        raise ProfileEvidenceError(f"{context} execution replay differs")
    for stream in ("stdout", "stderr"):
        _json_nonnegative_integer(execution[f"{stream}_bytes"], f"{context} {stream} bytes")
        _sha256_authority(execution[f"{stream}_sha256"], f"{context} {stream}")
    log_path, log_payload, log_identity = _immutable_file_payload(
        str(current_root / "logs" / f"cargo-build-{source_name}.json"), 0o444, f"{context} execution log", limit=None
    )
    if (
        log_identity[2] != 1
        or _local_canonical_json_payload(
            log_payload, f"{context} execution log"
        )
        != execution
    ):
        raise ProfileEvidenceError(f"{context} execution log differs")
    if log_path != current_root / "logs" / f"cargo-build-{source_name}.json":
        raise ProfileEvidenceError(f"{context} execution log topology differs")
    binds = _exact_mapping(build["binds"], ("receipt", "target") if child else ("target",), context)
    target_bind = _exact_mapping(binds["target"], ("parent", "post", "pre"), context)
    target_parent = _current_directory_identity(
        target_bind["parent"], context, expected_path=target.parent, replay_live=False
    )
    target_pre = _current_directory_identity(
        target_bind["pre"], context, expected_path=target, replay_live=False
    )
    target_post = _current_directory_identity(
        target_bind["post"], context, expected_path=target, replay_live=False
    )
    for field in ("device", "file_type", "inode", "permissions"):
        if target_pre[field] != target_post[field]:
            raise ProfileEvidenceError(f"{context} target selection changed")
    live_target = target.lstat()
    if (
        live_target.st_dev != target_post["device"]
        or live_target.st_ino != target_post["inode"]
        or stat.S_IFMT(live_target.st_mode) != target_post["file_type"]
        or target.resolve(strict=True) != target
    ):
        raise ProfileEvidenceError(f"{context} live target selection differs")
    if target_parent["path"] != str(target.parent):
        raise ProfileEvidenceError(f"{context} target parent differs")
    artifact_paths = {
        "asterism_rebaseline_current_correctness": current_root / "artifacts" / "tools" / "ast-rb-check",
        "asterism_rebaseline_current_fault": current_root / "artifacts" / "tools" / "ast-rb-fault",
        "asterism_rebaseline_public": current_root / "artifacts" / "release" / ("hooked-A" if name == "hooked_release" else "pristine-A"),
    }
    artifacts = _exact_mapping(build["artifacts"], examples, context)
    for example in examples:
        artifact = _exact_mapping(artifacts[example], ("binding", "source"), context)
        binding = _exact_mapping(artifact["binding"], ("comm", "executable_mode", "path", "sha256"), context)
        artifact_path = artifact_paths[example]
        _path, artifact_payload, observed = _immutable_file_payload(
            binding["path"], 0o555, context, limit=None
        )
        if (
            _sha256_bytes(artifact_payload) != binding["sha256"]
            or binding["path"] != str(artifact_path)
            or binding["comm"] != artifact_path.name
            or binding["executable_mode"] != 0o555
            or observed[2] != 1
        ):
            raise ProfileEvidenceError(f"{context} artifact binding differs")
        source = _current_bound_file_identity(
            artifact["source"],
            context,
            actual_path=target / "release" / "examples" / example,
            logical_path=(
                f"/proc/self/fd/{descriptors['target']}"
                f"/release/examples/{example}"
            ),
            executable=True,
            expected_link_count=2,
        )
        if source["sha256"] != binding["sha256"]:
            raise ProfileEvidenceError(f"{context} artifact source differs")
    semantic_toolchain = _as_mapping(semantic["toolchain"], f"{context} semantic toolchain")
    toolchain_manifest = _exact_mapping(
        build["toolchain_manifest"], ("entry_count", "equal_pre_post", "path", "post_sha256", "pre_sha256"), context
    )
    if toolchain_manifest != {
        "entry_count": semantic_toolchain.get("entry_count"),
        "equal_pre_post": True,
        "path": semantic_toolchain.get("manifest_path"),
        "post_sha256": semantic_toolchain.get("manifest_sha256"),
        "pre_sha256": semantic_toolchain.get("manifest_sha256"),
    }:
        raise ProfileEvidenceError(f"{context} toolchain manifest differs")
    source_manifest_path = current_root / "manifests" / f"materialized-{source_name}.json"
    _path, source_manifest_payload, source_manifest_identity = _immutable_file_snapshot(
        str(source_manifest_path),
        build["source_manifest_sha256"],
        0o444,
        f"{context} source manifest",
    )
    source_manifest = _local_canonical_json_payload(
        source_manifest_payload, f"{context} source manifest"
    )
    if (
        source_manifest_identity[2] != 1
        or source_manifest.get("schema") != "bn-30fs-file-manifest-v2"
        or not isinstance(source_manifest.get("entries"), list)
    ):
        raise ProfileEvidenceError(f"{context} source manifest differs")
    if child:
        receipt_root = current_root / "receipts" / "children"
        receipt_bind = _exact_mapping(binds["receipt"], ("parent", "post", "pre"), context)
        receipt_pre = _current_directory_identity(receipt_bind["pre"], context, expected_path=receipt_root, replay_live=False)
        receipt_post = _current_directory_identity(receipt_bind["post"], context, expected_path=receipt_root, replay_live=False)
        _current_directory_identity(receipt_bind["parent"], context, expected_path=receipt_root.parent, replay_live=False)
        for field in ("device", "file_type", "inode", "permissions"):
            if receipt_pre[field] != receipt_post[field]:
                raise ProfileEvidenceError(f"{context} receipt selection changed")
        live_receipt = receipt_root.lstat()
        if (
            live_receipt.st_dev != receipt_post["device"]
            or live_receipt.st_ino != receipt_post["inode"]
            or stat.S_IFMT(live_receipt.st_mode) != receipt_post["file_type"]
            or receipt_root.resolve(strict=True) != receipt_root
        ):
            raise ProfileEvidenceError(f"{context} live receipt selection differs")
        wrapper_path = current_root / "inputs" / "rustc_workspace_wrapper.py"
        _current_file_identity(
            build["wrapper_input_identity"], context, expected_path=wrapper_path, executable=True
        )
        receipt_path = receipt_root / "injection.json"
        receipt_identity = _current_bound_file_identity(
            build["wrapper_receipt_identity"],
            context,
            actual_path=receipt_path,
            logical_path=f"/proc/self/fd/{descriptors['receipt']}/injection.json",
            executable=False,
        )
        receipt = _exact_mapping(
            build["wrapper_receipt"],
            ("build_nonce", "crate_name", "crate_type", "injected_arguments", "original_argv_sha256", "package", "rustc", "schema", "source"),
            context,
        )
        _path, receipt_payload, _observed = _immutable_file_snapshot(
            str(receipt_path), build["wrapper_receipt_sha256"], 0o444, context
        )
        if (
            _local_canonical_json_payload(receipt_payload, context) != receipt
            or receipt_identity["sha256"] != build["wrapper_receipt_sha256"]
            or receipt != {
                "build_nonce": current["build_nonce"],
                "crate_name": "mess_store",
                "crate_type": "lib",
                "injected_arguments": list(_CURRENT_WRAPPER_ARGUMENTS),
                "original_argv_sha256": receipt["original_argv_sha256"],
                "package": "mess-store",
                "rustc": _GUEST_RUSTC,
                "schema": _CURRENT_WRAPPER_RECEIPT_SCHEMA,
                "source": _CURRENT_EXPECTED_LIB_SOURCE,
            }
        ):
            raise ProfileEvidenceError(f"{context} wrapper receipt differs")
        _sha256_authority(receipt["original_argv_sha256"], f"{context} wrapper argv")
    return build


def _tracked_resolver_environment(
    toolchain: Mapping[str, object], context: str
) -> dict[str, str]:
    required_paths = (
        "cargo_home_path",
        "cargo_path",
        "rustc_path",
        "rustup_home_path",
        "rustup_toolchain",
    )
    if any(
        not isinstance(toolchain.get(field), str) or not toolchain[field]
        for field in required_paths
    ):
        raise ProfileEvidenceError(f"{context} tracked toolchain paths differ")
    cargo_bin = str(Path(str(toolchain["cargo_path"])).parent)
    rustc_bin = str(Path(str(toolchain["rustc_path"])).parent)
    path = ":".join(dict.fromkeys((cargo_bin, rustc_bin, "/usr/bin", "/bin")))
    return {
        "CARGO_HOME": str(toolchain["cargo_home_path"]),
        "CARGO_INCREMENTAL": "0",
        "CARGO_NET_OFFLINE": "true",
        "GIT_CONFIG_COUNT": "0",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": path,
        "RUSTC": str(toolchain["rustc_path"]),
        "RUSTUP_HOME": str(toolchain["rustup_home_path"]),
        "RUSTUP_TOOLCHAIN": str(toolchain["rustup_toolchain"]),
        "TZ": "UTC",
    }


def _lock_repository_from_source_plan(value: object) -> Path:
    try:
        source_plan = Path(str(value))
        resolved = source_plan.resolve(strict=True)
        repository = resolved.parents[3]
    except (IndexError, OSError, RuntimeError) as error:
        raise ProfileEvidenceError("lock source-plan topology differs") from error
    if (
        not isinstance(value, str)
        or value != str(resolved)
        or source_plan != resolved
        or resolved
        != repository
        / "spikes"
        / "asterism_rebaseline"
        / "tooling"
        / "source-plan.json"
    ):
        raise ProfileEvidenceError("lock source-plan topology differs")
    return repository


def _validate_tracked_resolver_record(
    record_value: object,
    *,
    claim: Mapping[str, object],
    variant: str,
    lock_root: Path,
    repository: Path,
    toolchain: Mapping[str, object],
) -> None:
    context = f"{variant} tracked resolver"
    if claim.get("current_lock_attempt") is not None:
        raise ProfileEvidenceError(f"{context} has a current-lock attempt")
    fields = (
        "argv",
        "cargo_config_search",
        "cwd",
        "environment",
        "exit_status",
        "host_source_root",
        "resolver_kind",
        "stderr",
        "stderr_sha256",
        "stdout",
        "stdout_sha256",
        "toolchain",
    )
    record = _exact_mapping(record_value, fields, context)
    historical = _exact_mapping(
        claim.get("historical_lock"),
        ("commit", "path", "sha256"),
        f"{context} historical lock",
    )
    commit = historical["commit"]
    historical_sha256 = historical["sha256"]
    final_sha256 = claim.get("final_lock_sha256")
    if (
        not isinstance(commit, str)
        or re.fullmatch(r"[0-9a-f]{40}", commit) is None
        or historical["path"] != "Cargo.lock"
        or not isinstance(historical_sha256, str)
        or _SHA256_RE.fullmatch(historical_sha256) is None
        or final_sha256 != historical_sha256
    ):
        raise ProfileEvidenceError(f"{context} historical lock differs")
    expected_final_lock = lock_root / "locks" / f"Cargo-{variant}.lock"
    final_lock, _final_payload, _final_identity = _immutable_file_snapshot(
        claim.get("final_lock_path"),
        final_sha256,
        0o444,
        f"{context} final lock",
    )
    if final_lock != expected_final_lock:
        raise ProfileEvidenceError(f"{context} final-lock topology differs")
    expected_source_root = lock_root / "materialized" / variant
    try:
        source_root = Path(str(record["host_source_root"]))
        cwd = Path(str(record["cwd"]))
        resolved_source = source_root.resolve(strict=True)
        resolved_cwd = cwd.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ProfileEvidenceError(f"{context} roots cannot be resolved") from error
    if (
        not isinstance(record["host_source_root"], str)
        or not isinstance(record["cwd"], str)
        or record["host_source_root"] != str(resolved_source)
        or record["cwd"] != str(resolved_cwd)
        or source_root != expected_source_root
        or resolved_source != expected_source_root
        or cwd != repository
        or resolved_cwd != repository
    ):
        raise ProfileEvidenceError(f"{context} roots differ")
    git_path = toolchain.get("git_path")
    git_sha256 = toolchain.get("git_sha256")
    if (
        not isinstance(git_path, str)
        or not isinstance(git_sha256, str)
        or _SHA256_RE.fullmatch(git_sha256) is None
    ):
        raise ProfileEvidenceError(f"{context} Git authority differs")
    retained_git, _payload, git_identity = _immutable_file_snapshot(
        git_path, git_sha256, None, f"{context} retained Git"
    )
    if retained_git != Path(git_path) or git_identity[4] & 0o111 == 0:
        raise ProfileEvidenceError(f"{context} retained Git differs")
    cargo_config = _exact_mapping(
        record["cargo_config_search"],
        ("path", "sha256"),
        f"{context} Cargo config binding",
    )
    if (
        cargo_config["path"]
        != str(lock_root / "manifests" / f"cargo-config-{variant}.json")
        or not isinstance(cargo_config["sha256"], str)
        or _SHA256_RE.fullmatch(cargo_config["sha256"]) is None
    ):
        raise ProfileEvidenceError(f"{context} Cargo config authority differs")
    _cargo_config_sha256(
        {
            "cargo_config_search": cargo_config,
            "materialized_root": str(resolved_source),
            "toolchain": toolchain,
        },
        context,
    )
    expected_argv = [
        git_path,
        "-C",
        str(resolved_cwd),
        "show",
        f"{commit}:{historical['path']}",
    ]
    stdout = record["stdout"]
    stderr = record["stderr"]
    if (
        record["resolver_kind"] != "tracked_git_readback"
        or record["toolchain"] != toolchain
        or record["argv"] != expected_argv
        or record["environment"] != _tracked_resolver_environment(toolchain, context)
        or record["exit_status"] != 0
        or isinstance(record["exit_status"], bool)
        or not isinstance(stdout, str)
        or not isinstance(stderr, str)
        or record["stdout_sha256"] != _sha256_bytes(stdout.encode())
        or record["stderr_sha256"] != _sha256_bytes(stderr.encode())
        or record["stdout_sha256"] != historical_sha256
        or stderr != ""
    ):
        raise ProfileEvidenceError(f"{context} replay differs")


def _validate_resolver_record(
    record_value: object,
    *,
    variant: str,
    role: str,
    lock_root: Path,
    toolchain: Mapping[str, object],
    runtime_digests: set[str],
    manifest_paths: list[Path],
    manifest_identities: set[tuple[int, int]] | None = None,
    live_cache: dict[tuple[object, ...], dict[str, object]] | None = None,
    expected_current_lock_sha256: object = None,
    expected_final_lock_sha256: object = None,
) -> None:
    context = f"{variant} {role} resolver"
    fields = (
        "argv",
        "cargo_config_search",
        "cwd",
        "environment",
        "execution_authority",
        "exit_status",
        "host_source_root",
        "lock_output",
        "passed_file_descriptors",
        "resolver_kind",
        "semantic_input_authority",
        "stderr",
        "stderr_sha256",
        "stdout",
        "stdout_sha256",
        "toolchain",
    )
    record = _exact_mapping(record_value, fields, context)
    source_root = lock_root / "materialized" / variant
    manifests_root = lock_root / "manifests"
    label = ("current-lock-" if role == "current" else "resolved-lock-") + variant
    if (
        record["resolver_kind"] != "sandboxed_cargo_resolution"
        or record["cwd"] != _GUEST_SOURCE
        or record["host_source_root"] != str(source_root)
        or record["toolchain"] != toolchain
        or record["exit_status"] != 0
        or isinstance(record["exit_status"], bool)
        or record["passed_file_descriptors"] != 13
    ):
        raise ProfileEvidenceError(f"{context} authority differs")
    cargo_config = _exact_mapping(
        record["cargo_config_search"],
        ("path", "sha256"),
        f"{context} Cargo config binding",
    )
    if (
        cargo_config["path"]
        != str(lock_root / "manifests" / f"cargo-config-{variant}.json")
        or not isinstance(cargo_config["sha256"], str)
        or _SHA256_RE.fullmatch(cargo_config["sha256"]) is None
    ):
        raise ProfileEvidenceError(f"{context} Cargo config authority differs")
    _cargo_config_sha256(
        {
            "cargo_config_search": cargo_config,
            "materialized_root": str(source_root),
            "toolchain": toolchain,
        },
        context,
    )
    for stream in ("stdout", "stderr"):
        payload = record[stream]
        digest = record[f"{stream}_sha256"]
        if (
            not isinstance(payload, str)
            or not isinstance(digest, str)
            or _SHA256_RE.fullmatch(digest) is None
            or _sha256_bytes(payload.encode()) != digest
        ):
            raise ProfileEvidenceError(f"{context} {stream} authority differs")
    execution = _replay_executable_binding(
        record["execution_authority"], f"{context} execution"
    )
    if (
        execution.get("path") != toolchain.get("bwrap_path")
        or execution.get("sha256") != toolchain.get("bwrap_sha256")
    ):
        raise ProfileEvidenceError(f"{context} bwrap authority differs")
    bindings = (
        *(
            (f"system:{guest}", "--ro-bind-fd", guest)
            for _host, guest in _TRUSTED_SYSTEM_MOUNTS
        ),
        ("source", "--bind-fd", _GUEST_SOURCE),
        ("toolchain_root", "--ro-bind-fd", _GUEST_TOOLCHAIN_ROOT),
        ("cargo", "--ro-bind-fd", _GUEST_CARGO),
        ("rustc", "--ro-bind-fd", _GUEST_RUSTC),
        ("cargo_home", "--tmp-overlay", _GUEST_CARGO_HOME),
        *(
            (f"config:{guest}", "--ro-bind-fd", guest)
            for guest in _GUEST_BOUND_CONFIG_PATHS
        ),
    )
    _normalized, descriptors = _sandbox_descriptors(record["argv"], bindings, context)
    cargo_arguments = (
        ["metadata", "--locked", "--offline", "--format-version", "1", "--no-deps"]
        if role == "current"
        else ["generate-lockfile", "--offline"]
    )
    if record["argv"] != _resolution_sandbox_argv(
        descriptors, cargo_arguments, str(toolchain["bwrap_path"])
    ):
        raise ProfileEvidenceError(f"{context} sandbox argv differs")
    _sandbox_environment(record["environment"], context, allow_extra=False)
    runtime, paths = _validate_semantic_input_authority(
        record["semantic_input_authority"],
        context,
        live_roots={
            "source": source_root,
            "toolchain": Path(str(toolchain["cargo_path"])).parent.parent,
            "cargo_home": Path(str(toolchain["cargo_home_path"])),
        },
        expected_manifest_paths={
            "source": manifests_root / f"resolution-source-{label}.json",
            "toolchain": manifests_root / f"resolution-toolchain-{label}.json",
            "cargo_home": manifests_root / f"resolution-cargo-home-{label}.json",
            "closure": manifests_root / f"resolution-{label}-system-closure.json",
        },
        source_role="resolution_source_without_cargo_lock",
        live_cache=live_cache,
        manifest_identities=manifest_identities,
    )
    runtime_digests.add(runtime)
    manifest_paths.extend(paths)
    lock_output = _exact_mapping(record["lock_output"], ("post", "pre"), context)
    pre = _exact_mapping(lock_output["pre"], ("path", "sha256", "status"), context)
    post = _exact_mapping(lock_output["post"], ("path", "sha256", "status"), context)
    lock_path = str(source_root / "Cargo.lock")
    if role == "current":
        expected_boundary = {
            "path": lock_path,
            "sha256": expected_current_lock_sha256,
            "status": "present",
        }
        if pre != expected_boundary or post != expected_boundary:
            raise ProfileEvidenceError(f"{context} current-lock transition differs")
    elif pre != {"path": lock_path, "sha256": None, "status": "absent"} or (
        post["path"] != lock_path
        or post["status"] != "present"
        or not isinstance(post["sha256"], str)
        or _SHA256_RE.fullmatch(post["sha256"]) is None
        or post["sha256"] != expected_final_lock_sha256
    ):
        raise ProfileEvidenceError(f"{context} generated-lock transition differs")
    if role == "generated":
        live_lock, _payload, _identity = _immutable_file_snapshot(
            lock_path,
            expected_final_lock_sha256,
            None,
            f"{context} live generated lock",
        )
        if live_lock != source_root / "Cargo.lock":
            raise ProfileEvidenceError(f"{context} live generated lock path differs")


def _validate_current_cargo_config_authority(
    current: Mapping[str, object],
) -> tuple[Mapping[str, object], ...]:
    lock_authority = _as_mapping(
        current.get("lock_authority"), "current-child lock authority"
    )
    lock_manifest = _as_mapping(
        lock_authority.get("lock_manifest"), "current-child embedded lock manifest"
    )
    lock_payload = _as_mapping(
        lock_manifest.get("payload"), "current-child embedded lock payload"
    )
    variants = _exact_mapping(
        lock_payload.get("variants"), VARIANT_SOURCE_BINDINGS, "current-child lock variants"
    )
    reviewed_a = _as_mapping(variants["A"], "current-child reviewed A")
    reviewed_resolver = _as_mapping(
        reviewed_a.get("resolver"), "current-child reviewed A resolver"
    )
    reviewed_binding = _exact_mapping(
        reviewed_resolver.get("cargo_config_search"),
        ("path", "sha256"),
        "current-child reviewed A Cargo config",
    )

    cargo = _exact_mapping(
        current.get("cargo_config_authority"),
        ("binding", "identity", "recorded", "translated_entries"),
        "current-child Cargo config authority",
    )
    binding = _exact_mapping(
        cargo["binding"], ("path", "sha256"), "current-child Cargo config binding"
    )
    path_value = binding["path"]
    if not isinstance(path_value, str):
        raise ProfileEvidenceError("current-child Cargo config path is not text")
    config_path = Path(path_value)
    identity = _current_file_identity(
        cargo["identity"],
        "current-child Cargo config identity",
        expected_path=config_path,
        executable=False,
    )
    selected, payload, observed = _immutable_file_payload(
        path_value,
        0o444,
        "current-child Cargo config manifest",
        limit=None,
    )
    observed_identity = {
        "bytes": len(payload),
        "ctime_ns": observed[6],
        "device": observed[0],
        "inode": observed[1],
        "link_count": observed[2],
        "mode": observed[4],
        "mtime_ns": observed[5],
        "path": str(selected),
        "sha256": _sha256_bytes(payload),
        "size": observed[3],
    }
    recorded = _exact_mapping(
        cargo["recorded"],
        ("cargo_home_path", "cwd", "entries", "schema"),
        "current-child recorded Cargo config",
    )
    if (
        binding != reviewed_binding
        or binding
        != {"path": str(selected), "sha256": _sha256_bytes(payload)}
        or identity != observed_identity
        or _canonical_json_payload(payload, "current-child Cargo config manifest")
        != recorded
        or recorded["schema"] != _CARGO_CONFIG_SEARCH_SCHEMA
        or recorded["cargo_home_path"] != _GUEST_CARGO_HOME
        or recorded["cwd"] != _GUEST_SOURCE
    ):
        raise ProfileEvidenceError("current-child Cargo config authority differs")
    candidates = (
        f"{_GUEST_SOURCE}/.cargo/config.toml",
        f"{_GUEST_SOURCE}/.cargo/config",
        f"{_GUEST_ROOT}/.cargo/config.toml",
        f"{_GUEST_ROOT}/.cargo/config",
        "/.cargo/config.toml",
        "/.cargo/config",
        f"{_GUEST_CARGO_HOME}/config.toml",
        f"{_GUEST_CARGO_HOME}/config",
    )
    raw_entries = recorded["entries"]
    translated = cargo["translated_entries"]
    if (
        not isinstance(raw_entries, list)
        or len(raw_entries) != len(candidates)
        or not isinstance(translated, list)
        or translated != raw_entries
    ):
        raise ProfileEvidenceError("current-child Cargo config topology differs")
    entries: list[Mapping[str, object]] = []
    for ordinal, (raw, guest) in enumerate(
        zip(raw_entries, candidates, strict=True), start=1
    ):
        entry = _exact_mapping(
            raw,
            ("path", "sha256", "status"),
            f"current-child Cargo config entry {ordinal}",
        )
        middle = 2 <= ordinal - 1 < 6
        if (
            entry["path"] != guest
            or (
                middle
                and entry
                != {"path": guest, "sha256": None, "status": "absent"}
            )
            or (
                not middle
                and (
                    entry["status"] != "present"
                    or not isinstance(entry["sha256"], str)
                    or _SHA256_RE.fullmatch(entry["sha256"]) is None
                )
            )
        ):
            raise ProfileEvidenceError("current-child Cargo config entry differs")
        entries.append(entry)
    return tuple(entries)


def _validate_current_preapproval(
    current: Mapping[str, object],
    assertion: Mapping[str, object],
    approval: Mapping[str, object],
) -> None:
    inputs = _as_mapping(assertion.get("inputs"), "source-review assertion inputs")
    tools_input = _as_mapping(inputs.get("tools_manifest"), "reviewed tools manifest")
    lock_input = _as_mapping(inputs.get("lock_manifest"), "reviewed lock manifest")
    review_input = _as_mapping(
        inputs.get("lock_review_bundle"), "reviewed lock-review bundle"
    )
    if (
        current.get("schema") != _CURRENT_CHILDREN_ATTESTATION_SCHEMA
        or current.get("protocol") != PROTOCOL
        or current.get("protocol_sha256") != PROTOCOL_SHA256
        or current.get("status") != "ok"
        or current.get("tools_manifest_sha256") != tools_input.get("sha256")
        or current.get("tools_manifest_sha256")
        != approval.get("tools_manifest_sha256")
        or current.get("lock_manifest_sha256") != lock_input.get("sha256")
        or current.get("review_bundle_sha256") != review_input.get("sha256")
    ):
        raise ProfileEvidenceError("current-child reviewed inputs differ")
    authority_inputs = _exact_mapping(
        current.get("lock_authority_inputs"),
        ("authority", "lock_manifest", "review_bundle"),
        "current-child lock-authority inputs",
    )
    names = {
        "authority": "lock_authority",
        "lock_manifest": "lock_manifest",
        "review_bundle": "lock_review_bundle",
    }
    for current_name, assertion_name in names.items():
        source_input = _as_mapping(
            inputs.get(assertion_name), f"reviewed input {assertion_name}"
        )
        expected = {
            field: source_input[field]
            for field in source_input
            if field != "schema"
        }
        if authority_inputs[current_name] != expected:
            raise ProfileEvidenceError(
                f"current-child {current_name} input binding differs"
            )
    if current.get("release_compile_out_approval") != {
        "final_integration_action": (
            "repeat-release-equality-proof-under-real-source-approval"
        ),
        "source_approval_sha256": (
            "fa2acb626f303f8a65a16a6c8a1fd86b7e80cf48e092ae21a7308984ae790c94"
        ),
        "source_approval_status": "preapproval-sentinel-not-source-approved",
    }:
        raise ProfileEvidenceError("current-child preapproval sentinel differs")
    requirement = _as_mapping(
        assertion.get("release_compile_out_requirement"),
        "release compile-out requirement",
    )
    preapproval = current.get("release_compile_out")
    overlay = _as_mapping(
        current.get("product_overlay_authority"), "current product overlay"
    )
    patch = _as_mapping(overlay.get("patch"), "current product overlay patch")
    if (
        not isinstance(preapproval, Mapping)
        or _sha256_bytes(canonical_json(dict(preapproval)))
        != requirement.get("preapproval_compile_out_sha256")
        or patch.get("sha256") != requirement.get("product_overlay_sha256")
    ):
        raise ProfileEvidenceError("current-child preapproval/overlay binding differs")


def _validate_prepared_release_attestation(
    value: object,
    prepared_toolchain: Mapping[str, object],
    context: str,
    *,
    overlay: bool = False,
) -> Mapping[str, object]:
    fields = (
        (*_PREPARED_ATTESTATION_FIELDS, "product_overlay_sha256")
        if overlay
        else _PREPARED_ATTESTATION_FIELDS
    )
    attestation = _exact_mapping(value, fields, f"{context} attestation")
    environment = _as_mapping(attestation["build_env"], f"{context} environment")
    if (
        attestation["toolchain"] != prepared_toolchain
        or attestation["source_read_only"] is not True
        or attestation["target_dir_was_absent"] is not True
        or attestation["materialized_manifest_pre_sha256"]
        != attestation["materialized_manifest_sha256"]
        or attestation["materialized_manifest_post_sha256"]
        != attestation["materialized_manifest_sha256"]
        or attestation["cargo_lock_pre_sha256"]
        != attestation["cargo_lock_sha256"]
        or attestation["cargo_lock_post_sha256"]
        != attestation["cargo_lock_sha256"]
    ):
        raise ProfileEvidenceError(f"{context} authority differs")
    _sandbox_environment(environment, context, allow_extra=True)
    return attestation


def _validate_phase4_semantic_authority(
    *,
    assertion: Mapping[str, object],
    approval: Mapping[str, object],
    local_current_children: Mapping[str, object],
    prepared: Mapping[str, object],
    prepared_root: Path,
    proof: Mapping[str, object],
) -> None:
    inputs = _as_mapping(assertion.get("inputs"), "source-review assertion inputs")
    reviewed_current = _exact_mapping(
        inputs.get("current_children_attestation"),
        ("identity", "mode", "path", "schema", "sha256", "size"),
        "source-reviewed current children",
    )
    if (
        reviewed_current["schema"] != _CURRENT_CHILDREN_ATTESTATION_SCHEMA
        or reviewed_current["mode"] != 0o444
    ):
        raise ProfileEvidenceError("source-reviewed current-child binding differs")
    current_path, current_payload, _identity = _release_file_snapshot(
        {
            field: reviewed_current[field]
            for field in _RELEASE_COMPILE_OUT_FILE_FIELDS
        },
        expected_mode=0o444,
        context="source-reviewed current children",
    )
    current_children = _canonical_json_payload(
        current_payload, "source-reviewed current children"
    )
    if current_children != local_current_children:
        raise ProfileEvidenceError("copied/original current-child payload differs")
    current_fields = (
        "artifacts",
        "build_nonce",
        "builds",
        "cargo_config_authority",
        "construction_path",
        "construction_sha256",
        "fault_authority",
        "inputs",
        "lock_authority",
        "lock_authority_inputs",
        "lock_authority_validation",
        "lock_candidates",
        "lock_manifest_sha256",
        "prebuild_filesystem_admissions",
        "product_commit",
        "product_overlay_authority",
        "product_tree",
        "protocol",
        "protocol_sha256",
        "release_compile_out",
        "release_compile_out_approval",
        "review_bundle_sha256",
        "schema",
        "static_authority",
        "status",
        "toolchain",
        "toolchain_identities",
        "tools_manifest_path",
        "tools_manifest_sha256",
    )
    current = _exact_mapping(current_children, current_fields, "current children")
    current_builds = _exact_mapping(
        current["builds"],
        ("children", "hooked_release", "pristine_release"),
        "current-child builds",
    )
    _validate_current_preapproval(current, assertion, approval)
    current_cargo_config_entries = _validate_current_cargo_config_authority(current)
    current_toolchain, current_toolchain_root = _validate_toolchain_contract(
        current["toolchain"], "current-child toolchain"
    )
    filesystem_admissions = _exact_mapping(
        current["prebuild_filesystem_admissions"],
        ("children", "hooked_release", "pristine_release"),
        "current-child filesystem admissions",
    )
    runtime_digests: set[str] = set()
    manifest_paths: list[Path] = []
    manifest_identities: set[tuple[int, int]] = set()
    live_cache: dict[tuple[object, ...], dict[str, object]] = {}
    current_roots = {
        "children": "children",
        "hooked_release": "hooked-release",
        "pristine_release": "pristine-release",
    }
    release_environments: list[Mapping[str, object]] = []
    for name, source_name in current_roots.items():
        build = _validate_current_build_record(
            current_builds[name],
            f"current-child {name}",
            name=name,
            source_name=source_name,
            current=current,
            current_root=current_path.parent,
            toolchain=current_toolchain,
            toolchain_root=current_toolchain_root,
            filesystem_admission=filesystem_admissions[name],
            expected_cargo_config_entries=current_cargo_config_entries,
        )
        if name != "children":
            release_environments.append(
                _as_mapping(build["environment"], f"current-child {name} environment")
            )
        runtime, paths = _validate_semantic_input_authority(
            build.get("semantic_input_authority"),
            f"current-child {name}",
            live_roots={
                "source": current_path.parent / "materialized" / source_name,
                "toolchain": current_toolchain_root,
                "cargo_home": Path(str(current_toolchain["cargo_home_path"])),
            },
            expected_manifest_paths={
                "source": current_path.parent
                / "manifests"
                / f"semantic-source-{source_name}.json",
                "toolchain": current_path.parent
                / "manifests"
                / f"semantic-toolchain-{source_name}.json",
                "cargo_home": current_path.parent
                / "manifests"
                / f"cargo-home-{source_name}.json",
                "closure": current_path.parent
                / "manifests"
                / f"{source_name}-system-closure.json",
            },
            live_cache=live_cache,
            manifest_identities=manifest_identities,
            manifest_canonical_payload=_local_canonical_json_payload,
            manifest_canonical_json=_local_canonical_json,
        )
        runtime_digests.add(runtime)
        manifest_paths.extend(paths)
    if len(release_environments) != 2 or release_environments[0] != release_environments[1]:
        raise ProfileEvidenceError("current release build environments differ")

    prepared_variants = _exact_mapping(
        prepared.get("variants"), ("A", "B", "C", "D"), "prepared variants"
    )
    proof_builds = _exact_mapping(
        proof.get("builds"), ("ordinary_a", "overlay_a"), "release proof builds"
    )
    ordinary_record = _as_mapping(proof_builds["ordinary_a"], "ordinary A record")
    prepared_toolchain, prepared_toolchain_root = _validate_toolchain_contract(
        prepared.get("toolchain"), "prepared toolchain"
    )
    if prepared_toolchain != current_toolchain or prepared_toolchain_root != current_toolchain_root:
        raise ProfileEvidenceError("current/prepared toolchain authority differs")
    prepared_a = _exact_mapping(
        prepared_variants["A"], _PREPARED_VARIANT_FIELDS, "prepared A"
    )
    prepared_a_contract = _as_mapping(
        prepared_a.get("contract"), "prepared A contract"
    )
    source_approval_sha256 = _sha256_bytes(canonical_json(dict(approval)))
    if ordinary_record.get("attestation") != prepared_a.get("attestation"):
        raise ProfileEvidenceError("ordinary proof/prepared A attestation differs")
    release_specs: list[tuple[str, Mapping[str, object], str, str, str]] = []
    for variant in ("A", "B", "C", "D"):
        prepared_variant = _exact_mapping(
            prepared_variants[variant],
            _PREPARED_VARIANT_FIELDS,
            f"prepared variant {variant}",
        )
        attestation = _validate_prepared_release_attestation(
            prepared_variant.get("attestation"),
            prepared_toolchain,
            f"prepared {variant} attestation",
        )
        release_specs.append(
            (
                f"prepared {variant}",
                attestation,
                f"build-{variant}",
                "mess-log" if variant == "B" else "mess-store",
                "asterism_rebaseline_bare"
                if variant == "B"
                else "asterism_rebaseline_public",
            )
        )
    overlay_record = _as_mapping(proof_builds["overlay_a"], "overlay A record")
    overlay_attestation = _validate_prepared_release_attestation(
        overlay_record.get("attestation"),
        prepared_toolchain,
        "overlay A attestation",
        overlay=True,
    )
    requirement = _as_mapping(
        assertion.get("release_compile_out_requirement"),
        "release compile-out requirement",
    )
    if overlay_attestation["product_overlay_sha256"] != requirement.get(
        "product_overlay_sha256"
    ):
        raise ProfileEvidenceError("proof overlay product authority differs")
    release_specs.append(
        (
            "proof overlay A",
            overlay_attestation,
            "build-A-product-overlay",
            "mess-store",
            "asterism_rebaseline_public",
        )
    )
    build_events: dict[
        str,
        tuple[
            Mapping[str, object],
            Path,
            tuple[int, int],
            tuple[str, int, int],
        ],
    ] = {}
    for context, attestation, label, package, example in release_specs:
        toolchain = _as_mapping(attestation.get("toolchain"), f"{context} toolchain")
        source_root = prepared_root / "materialized" / label.removeprefix("build-")
        if attestation.get("materialized_root") != str(source_root):
            raise ProfileEvidenceError(f"{context} materialized root differs")
        runtime, paths = _validate_semantic_input_authority(
            attestation.get("semantic_input_authority"),
            context,
            live_roots={
                "source": source_root,
                "toolchain": prepared_toolchain_root,
                "cargo_home": Path(str(toolchain["cargo_home_path"])),
            },
            expected_manifest_paths={
                "source": prepared_root
                / "manifests"
                / f"semantic-source-{label}.json",
                "toolchain": prepared_root
                / "manifests"
                / f"semantic-toolchain-{label}.json",
                "cargo_home": prepared_root
                / "manifests"
                / f"semantic-cargo-home-{label}.json",
                "closure": prepared_root
                / "manifests"
                / f"{label}-system-closure.json",
            },
            live_cache=live_cache,
            manifest_identities=manifest_identities,
        )
        sandbox_sha = _validate_release_sandbox(
            attestation,
            context,
            package=package,
            example=example,
            runtime_sha256=runtime,
        )
        if context in {"prepared A", "proof overlay A"}:
            record = ordinary_record if context == "prepared A" else overlay_record
            if record.get("sandbox_sha256") != sandbox_sha:
                raise ProfileEvidenceError(f"{context} normalized sandbox differs")
            environment = _as_mapping(
                attestation.get("build_env"), f"{context} build environment"
            )
            contract_path, contract_payload, _contract_identity = (
                _immutable_file_snapshot(
                    attestation.get("contract_output_path"),
                    attestation.get("contract_output_sha256"),
                    0o444,
                    f"{context} contract output",
                )
            )
            contract = _canonical_json_payload(
                contract_payload, f"{context} contract output"
            )
            if (
                attestation.get("build_nonce") != record.get("build_nonce")
                or attestation.get("cargo_lock_sha256")
                != record.get("cargo_lock_sha256")
                or _sha256_bytes(canonical_json(dict(toolchain)))
                != record.get("toolchain_sha256")
                or _sha256_bytes(canonical_json(dict(environment)))
                != record.get("build_environment_sha256")
                or environment.get("ASTERISM_BUILD_SOURCE_APPROVAL_SHA256")
                != source_approval_sha256
                or _sha256_bytes(canonical_json(dict(contract)))
                != record.get("contract_sha256")
                or contract != prepared_a_contract
                or contract_path != Path(str(attestation["contract_output_path"]))
            ):
                raise ProfileEvidenceError(f"{context} embedded authority differs")
        child, log_path, log_identity, root_identity = _validate_release_build_child(
            attestation, context
        )
        if context == "prepared A":
            build_events["ordinary_a"] = (
                child,
                log_path,
                log_identity,
                root_identity,
            )
        elif context == "proof overlay A":
            build_events["overlay_a"] = (
                child,
                log_path,
                log_identity,
                root_identity,
            )
        runtime_digests.add(runtime)
        manifest_paths.extend(paths)
    ordinary_event = build_events.get("ordinary_a")
    overlay_event = build_events.get("overlay_a")
    if ordinary_event is None or overlay_event is None:
        raise ProfileEvidenceError("release build event authority is incomplete")
    ordinary_child, ordinary_log, ordinary_log_identity, ordinary_root = ordinary_event
    overlay_child, overlay_log, overlay_log_identity, overlay_root = overlay_event
    if (
        ordinary_root[0] == overlay_root[0]
        or ordinary_root[1:] == overlay_root[1:]
        or (ordinary_child["pid"], ordinary_child["start_ticks"])
        == (overlay_child["pid"], overlay_child["start_ticks"])
        or ordinary_child["completed_monotonic_ns"]
        >= overlay_child["started_monotonic_ns"]
        or ordinary_log == overlay_log
        or ordinary_log_identity == overlay_log_identity
        or _authority_timestamp(
            ordinary_child["completed_at"], "ordinary release completion"
        )
        > _authority_timestamp(overlay_child["started_at"], "overlay release start")
    ):
        raise ProfileEvidenceError("release build events overlap or alias")

    locks, lock_manifest_path = _snapshot_reviewed_lock_manifest(current, assertion)
    repository = _lock_repository_from_source_plan(locks.get("source_plan_path"))
    lock_variants = _exact_mapping(
        locks.get("variants"), ("A", "B", "C", "D"), "reviewed lock variants"
    )
    lock_toolchain, lock_toolchain_root = _validate_toolchain_contract(
        locks.get("toolchain"), "reviewed lock toolchain"
    )
    if lock_toolchain != prepared_toolchain or lock_toolchain_root != prepared_toolchain_root:
        raise ProfileEvidenceError("lock/release toolchain authority differs")
    current_claim = _as_mapping(lock_variants["A"], "reviewed lock A")
    current_historical = _exact_mapping(
        current_claim.get("historical_lock"),
        ("commit", "path", "sha256"),
        "reviewed current historical lock",
    )
    current_lock_sha256 = current_historical["sha256"]
    if (
        current_historical["path"] != "Cargo.lock"
        or not isinstance(current_lock_sha256, str)
        or _SHA256_RE.fullmatch(current_lock_sha256) is None
    ):
        raise ProfileEvidenceError("reviewed current-lock authority differs")
    lock_roots: set[Path] = set()
    for variant in ("A", "B", "C", "D"):
        claim = _as_mapping(lock_variants[variant], f"reviewed lock {variant}")
        final_lock = Path(str(claim.get("final_lock_path")))
        final_lock_sha256 = claim.get("final_lock_sha256")
        lock_root = final_lock.parent.parent
        lock_roots.add(lock_root)
        if (
            not final_lock.is_absolute()
            or str(final_lock) != str(claim.get("final_lock_path"))
            or lock_manifest_path != lock_root / "lock-candidates.json"
            or final_lock != lock_root / "locks" / f"Cargo-{variant}.lock"
            or not isinstance(final_lock_sha256, str)
            or _SHA256_RE.fullmatch(final_lock_sha256) is None
        ):
            raise ProfileEvidenceError(f"reviewed lock {variant} topology differs")
        _immutable_file_snapshot(
            str(final_lock),
            final_lock_sha256,
            0o444,
            f"reviewed final lock {variant}",
        )
        if variant in {"A", "B"}:
            if claim.get("current_lock_attempt") is not None:
                raise ProfileEvidenceError(
                    f"reviewed lock {variant} unexpectedly has a current attempt"
                )
            _validate_tracked_resolver_record(
                claim.get("resolver"),
                claim=claim,
                variant=variant,
                lock_root=lock_root,
                repository=repository,
                toolchain=lock_toolchain,
            )
            continue
        _validate_resolver_record(
            claim.get("current_lock_attempt"),
            variant=variant,
            role="current",
            lock_root=lock_root,
            toolchain=lock_toolchain,
            runtime_digests=runtime_digests,
            manifest_paths=manifest_paths,
            manifest_identities=manifest_identities,
            live_cache=live_cache,
            expected_current_lock_sha256=current_lock_sha256,
            expected_final_lock_sha256=final_lock_sha256,
        )
        _validate_resolver_record(
            claim.get("resolver"),
            variant=variant,
            role="generated",
            lock_root=lock_root,
            toolchain=lock_toolchain,
            runtime_digests=runtime_digests,
            manifest_paths=manifest_paths,
            manifest_identities=manifest_identities,
            live_cache=live_cache,
            expected_current_lock_sha256=current_lock_sha256,
            expected_final_lock_sha256=final_lock_sha256,
        )
    if len(lock_roots) != 1:
        raise ProfileEvidenceError("reviewed A-D lock roots differ")
    _replay_semantic_live_cache(live_cache)
    if len(runtime_digests) != 1:
        raise ProfileEvidenceError("phase 4 semantic runtime authorities differ")
    if len(manifest_paths) != 48 or len(set(manifest_paths)) != 48:
        raise ProfileEvidenceError("phase 4 semantic manifest topology differs")
    if len(manifest_identities) != 48:
        raise ProfileEvidenceError("phase 4 semantic manifest identities differ")


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
    _validate_phase4_semantic_authority(
        assertion=assertion,
        approval=approval,
        local_current_children=current_children,
        prepared=prepared,
        prepared_root=prepared_root,
        proof=proof,
    )

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

    nm = _exact_mapping(
        proof["nm"], ("tool", "ordinary_a", "overlay_a"), "release proof nm"
    )
    nm_tool = _release_file_snapshot(
        nm["tool"], expected_mode=0o555, context="release proof nm tool"
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
    claim_path, _claim_payload, prepared_claim = _unclaimed_local_canonical_json_snapshot(
        claim_binding["path"], "prepared single-use claim"
    )
    if (
        claim_path.name != "single-use-claim.json"
        or claim_path.parent.name != "claims"
    ):
        raise ProfileEvidenceError("prepared single-use claim path differs")
    prepared_root = claim_path.parent.parent
    try:
        claim_directory_mode = stat.S_IMODE(claim_path.parent.stat().st_mode)
        prepared_root_mode = stat.S_IMODE(prepared_root.stat().st_mode)
    except OSError as error:
        raise ProfileEvidenceError(
            f"cannot replay prepared root/claims modes: {error}"
        ) from error
    if claim_directory_mode != 0o700 or prepared_root_mode != 0o555:
        raise ProfileEvidenceError("prepared root/claims mode authority differs")
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
        prepared_claim, expected_claim_fields, "prepared single-use claim"
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
        raise ProfileEvidenceError("prepared single-use claim authority differs")
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
        if (
            tool != approved_tools.get(name)
            or tool != prepared_tools.get(name)
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
        if not isinstance(tasks, list) or not tasks:
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
            expected_comm = (
                "mess-flat-owner"
                if label == "owner"
                else TOKIO_WORKER_COMM
                if label in {"producer-runtime", "spawn_blocking-publication"}
                else authority["executable_comm"]
            )
            if identity["comm"] != expected_comm:
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
        if (
            set(producer_births) != role_task_keys["producer-runtime"]
            or any(task["comm"] != TOKIO_WORKER_COMM for task in producer_births.values())
        ):
            raise ProfileEvidenceError("producer boot-to-runtime birth replay differs")
        if (
            set(terminal_births) != role_task_keys["spawn_blocking-publication"]
            or any(task["comm"] != TOKIO_WORKER_COMM for task in terminal_births.values())
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
    require_measurable_role_cpu(cpu_ns, _resolution(inputs))
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
    _, payload = _raw_artifact_snapshot(
        inputs["trace_raw_artifact"], "strace raw artifact"
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
        if not start_sent <= release_monotonic <= t0:
            raise ProfileEvidenceError("control start/release/t0 ordering differs")
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
        _, critical = _critical_role(rich)
        require_measurable_role_cpu(
            _json_nonnegative_integer(critical["on_cpu_ns"], "critical role cpu_ns"),
            _resolution(measured_inputs),
        )
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
