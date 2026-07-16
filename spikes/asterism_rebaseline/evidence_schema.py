#!/usr/bin/env python3
"""Canonical evidence contract for the bn-2l3n Asterism rebaseline.

The runner consumes this module to reject malformed child output before it is
appended to a CSV.  The evaluator deliberately replays the same contract from
the persisted bytes; runner acceptance is never treated as evidence of
validity.

All decision-driving quantities are integers.  ``not_available`` is permitted
only for fields whose protocol semantics explicitly allow it.  Normalized
rates and ratios are derived by the evaluator from these raw integers.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import stat
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final


PROTOCOL: Final = "bn-2l3n-asterism-rebaseline-v3"
VARIANTS: Final = ("A", "B", "C", "D")
PUBLIC_VARIANTS: Final = ("A", "C", "D")
DURABILITIES: Final = ("Process", "Group")
NOT_AVAILABLE: Final = "not_available"
REQUIRED_FILESYSTEM_TYPE: Final = "ext4"
MIN_FREE_BYTES: Final = 137_438_953_472
MIN_FREE_INODES: Final = 1_000_000
ARTIFACT_FILE_MODE: Final = 0o444
ARTIFACT_INVENTORY_FIELDS: Final = ("path", "bytes", "sha256", "mode")


@dataclass(frozen=True, eq=False)
class FileSnapshot:
    """Bytes and metadata captured through one no-following file descriptor."""

    path: Path
    data: bytes
    sha256: str
    size: int
    mode: int
    device: int
    inode: int
    _stat: os.stat_result

    def __fspath__(self) -> str:
        return str(self.path)

    def __str__(self) -> str:
        return str(self.path)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, FileSnapshot):
            return self.path == other.path
        if isinstance(other, (Path, str)):
            return self.path == Path(other)
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.path)

    @property
    def name(self) -> str:
        return self.path.name

    @property
    def parent(self) -> Path:
        return self.path.parent

    def relative_to(self, other: Path) -> Path:
        return self.path.relative_to(other)

    def resolve(self, *, strict: bool = False) -> Path:
        del strict
        return self.path

    def stat(self) -> os.stat_result:
        return self._stat

    def lstat(self) -> os.stat_result:
        return self._stat

    def read_bytes(self) -> bytes:
        return self.data

    def open(
        self,
        mode: str = "r",
        *,
        encoding: str | None = None,
        newline: str | None = None,
    ) -> io.BytesIO | io.TextIOWrapper:
        if mode not in {"r", "rt", "rb"}:
            raise ValueError("a frozen file snapshot is read-only")
        raw = io.BytesIO(self.data)
        if "b" in mode:
            return raw
        return io.TextIOWrapper(
            raw, encoding=encoding or "utf-8", newline=newline
        )

    def is_symlink(self) -> bool:
        return False

    def is_file(self) -> bool:
        return True

    def exists(self) -> bool:
        return True


def _stat_identity(info: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def _absolute_lexical_path(path: Path) -> Path:
    if not path.is_absolute():
        raise ValueError("bound path is not absolute")
    if any(part in {"", ".", ".."} for part in path.parts[1:]):
        raise ValueError("bound path is not lexically normalized")
    return path


def snapshot_regular_file(
    path: Path,
    *,
    within: Path | None = None,
    expected_mode: int | None = None,
    reject_hidden: bool = False,
) -> FileSnapshot:
    """Capture a regular file without following any symlink in its path.

    Every ancestor is opened descriptor-relative with ``O_NOFOLLOW``.  The
    final file is read once from one descriptor, then both the descriptor and
    lexical path chain are rechecked so concurrent replacement fails closed.
    """

    lexical = _absolute_lexical_path(Path(path))
    if within is not None:
        root = _absolute_lexical_path(Path(within))
        try:
            relative = lexical.relative_to(root)
        except ValueError as error:
            raise ValueError(f"bound path escapes {root}") from error
        if reject_hidden and any(part.startswith(".") for part in relative.parts):
            raise ValueError("hidden bound path is prohibited")
    elif reject_hidden and any(part.startswith(".") for part in lexical.parts[1:]):
        raise ValueError("hidden bound path is prohibited")

    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    file_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    directory_fds: list[int] = []
    directory_identities: list[tuple[int, int]] = []
    file_fd: int | None = None
    try:
        current = os.open("/", directory_flags)
        directory_fds.append(current)
        directory_identities.append((os.fstat(current).st_dev, os.fstat(current).st_ino))
        for component in lexical.parts[1:-1]:
            current = os.open(component, directory_flags, dir_fd=current)
            directory_fds.append(current)
            info = os.fstat(current)
            if not stat.S_ISDIR(info.st_mode):
                raise OSError(f"ancestor {component!r} is not a directory")
            directory_identities.append((info.st_dev, info.st_ino))
        if len(lexical.parts) < 2:
            raise ValueError("bound path names no file")
        file_fd = os.open(lexical.name, file_flags, dir_fd=current)
        before = os.fstat(file_fd)
        if not stat.S_ISREG(before.st_mode):
            raise OSError("bound path is not a regular file")
        mode = stat.S_IMODE(before.st_mode)
        if expected_mode is not None and mode != expected_mode:
            raise OSError(
                f"bound file mode {mode:#06o} differs from exact {expected_mode:#06o}"
            )
        chunks: list[bytes] = []
        while True:
            chunk = os.read(file_fd, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        data = b"".join(chunks)
        after = os.fstat(file_fd)
        if _stat_identity(before) != _stat_identity(after) or len(data) != before.st_size:
            raise OSError("bound file changed while it was being snapshotted")

        # Reopen only directories and lstat the final name.  This proves the
        # lexical path still identifies the exact descriptor that supplied the
        # bytes, including across ancestor or final-component replacement.
        verify = os.open("/", directory_flags)
        try:
            if (os.fstat(verify).st_dev, os.fstat(verify).st_ino) != directory_identities[0]:
                raise OSError("root directory identity changed")
            for index, component in enumerate(lexical.parts[1:-1], start=1):
                next_fd = os.open(component, directory_flags, dir_fd=verify)
                os.close(verify)
                verify = next_fd
                info = os.fstat(verify)
                if (info.st_dev, info.st_ino) != directory_identities[index]:
                    raise OSError("bound path ancestor was replaced during snapshot")
            path_info = os.stat(lexical.name, dir_fd=verify, follow_symlinks=False)
            if (path_info.st_dev, path_info.st_ino) != (before.st_dev, before.st_ino):
                raise OSError("bound file was replaced during snapshot")
        finally:
            os.close(verify)
        return FileSnapshot(
            path=lexical,
            data=data,
            sha256=hashlib.sha256(data).hexdigest(),
            size=len(data),
            mode=mode,
            device=before.st_dev,
            inode=before.st_ino,
            _stat=before,
        )
    finally:
        if file_fd is not None:
            os.close(file_fd)
        for descriptor in reversed(directory_fds):
            os.close(descriptor)


def artifact_inventory(
    root: Path,
    *,
    excluded_names: Iterable[str] = (),
) -> tuple[list[dict[str, Any]], dict[str, FileSnapshot]]:
    """Snapshot the exact public artifact tree, rejecting unsafe entries."""

    lexical_root = _absolute_lexical_path(Path(root))
    excluded = set(excluded_names)
    inventory: list[dict[str, Any]] = []
    snapshots: dict[str, FileSnapshot] = {}

    def visit(directory: Path) -> None:
        directory_fd = os.open(
            directory,
            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
        )
        try:
            for name in sorted(os.listdir(directory_fd)):
                if name.startswith("."):
                    raise OSError(f"hidden artifact entry is prohibited: {directory / name}")
                if directory == lexical_root and name in excluded:
                    continue
                info = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                child = directory / name
                if stat.S_ISLNK(info.st_mode):
                    raise OSError(f"artifact symlink is prohibited: {child}")
                if stat.S_ISDIR(info.st_mode):
                    visit(child)
                    continue
                if not stat.S_ISREG(info.st_mode):
                    raise OSError(f"nonregular artifact entry is prohibited: {child}")
                snapshot = snapshot_regular_file(
                    child,
                    within=lexical_root,
                    expected_mode=ARTIFACT_FILE_MODE,
                    reject_hidden=True,
                )
                relative = child.relative_to(lexical_root).as_posix()
                inventory.append(
                    {
                        "path": relative,
                        "bytes": snapshot.size,
                        "sha256": snapshot.sha256,
                        "mode": snapshot.mode,
                    }
                )
                snapshots[relative] = snapshot
        finally:
            os.close(directory_fd)

    visit(lexical_root)
    inventory.sort(key=lambda item: item["path"])
    return inventory, snapshots

VARIANT_SOURCE_BINDINGS: Final = {
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
CURRENT_LOCK_SHA256: Final = (
    "9c24189940d9b43d7798c6680c8aeab6ddc270ef9b450390334d9327405cbea0"
)
PROTOCOL_RELATIVE_PATH: Final = "spikes/asterism_rebaseline/BN-2L3N-PROTOCOL.md"
PROTOCOL_SHA256: Final = "d9ee10b2cccdaf6428bf1419a8c2ee74d272e987dc3617a80b64ad2e9d7a18dd"
HISTORICAL_BASELINE_SHA256: Final = (
    "b2801a056a711de7a8643c15af2eb7d12a04b6315e0dec40a7beacc53c5bde40"
)

BINARY_CONTRACT_SCHEMA: Final = "bn-2l3n-binary-contract-v3"
RAW_POINT_SCHEMA: Final = "bn-2l3n-point-v3"
RAW_BINDING_SCHEMA: Final = "bn-2l3n-raw-binding-v3"
CONFIG_SCHEMA: Final = "bn-2l3n-config-v3"
SOURCE_APPROVAL_SCHEMA: Final = "bn-2l3n-source-approval-v3"
PREPARED_ARTIFACTS_SCHEMA: Final = "bn-2l3n-prepared-artifacts-v3"
PREPARED_CLAIM_SCHEMA: Final = "bn-2l3n-prepared-claim-v3"
FILE_MANIFEST_SCHEMA: Final = "bn-2l3n-file-manifest-v3"
SMOKE_SCHEMA: Final = "bn-2l3n-smoke-v3"
PROVENANCE_SCHEMA: Final = "bn-2l3n-provenance-v3"
GUARD_SCHEMA: Final = "bn-2l3n-guard-v3"
GUARD_BINDING_SCHEMA: Final = "bn-2l3n-manifest-v3"
CHILD_SCHEMA: Final = "bn-2l3n-child-v3"
CORRECTNESS_SCHEMA: Final = "bn-2l3n-correctness-v3"
CORRECTNESS_CHILD_SCHEMA: Final = "bn-2l3n-correctness-child-v3"
CORRECTNESS_ONLY_SCHEMA: Final = "bn-2l3n-correctness-only-v3"
CORRECTNESS_ONLY_FIELDS: Final = (
    "schema", "protocol", "attempt_nonce", "trigger",
    "current_pre_failed_case_ids", "current_post_failed_case_ids",
    "historical_failed_cases",
    "timing_child_records", "created_at", "created_monotonic_ns",
)
CORRECTNESS_ONLY_HISTORICAL_FAILURE_FIELDS: Final = ("variant", "phase", "id")
RESULT_SCHEMA: Final = "bn-2l3n-evaluation-result-v3"
TERMINAL_PRE_RELEASE_SCHEMA: Final = "bn-2l3n-terminal-pre-release-v3"
LEASE_RELEASE_SCHEMA: Final = "bn-2l3n-lease-release-v3"
TERMINAL_SCHEMA: Final = "bn-2l3n-terminal-v3"
TERMINAL_VERIFICATION_SCHEMA: Final = "bn-2l3n-terminal-verification-v3"

TERMINAL_PRE_RELEASE_FIELDS: Final = (
    "schema", "protocol", "attempt_nonce", "outcome", "evaluator_exit",
    "result_path", "result_sha256", "provenance_path", "provenance_sha256",
    "report_path", "report_sha256", "sha256sums_path", "sha256sums_sha256",
    "artifact_inventory", "evaluator_transition", "guard_manifest_path",
    "guard_manifest_sha256", "guard_manifest_records", "child_manifest_path",
    "child_manifest_sha256", "child_manifest_records", "lease", "completed_at",
    "completed_monotonic_ns",
)
LEASE_RELEASE_FIELDS: Final = (
    "schema", "protocol", "event", "attempt_nonce", "lease_nonce", "lease_path",
    "lease_device", "lease_inode", "outcome", "released_at", "released_monotonic_ns",
)
TERMINAL_FIELDS: Final = (
    "schema", "protocol", "attempt_nonce", "outcome", "terminal_pre_release_path",
    "terminal_pre_release_sha256", "lease_release_path", "lease_release_sha256",
    "result_path", "result_sha256", "provenance_path", "provenance_sha256",
    "sha256sums_path", "sha256sums_sha256", "artifact_inventory_sha256",
    "runner", "terminal_published_at", "terminal_published_monotonic_ns",
)
TERMINAL_RUNNER_FIELDS: Final = ("identity", "runtime", "support", "cmdline")
TERMINAL_RUNTIME_FIELDS: Final = ("path", "sha256", "mode", "comm")
TERMINAL_SUPPORT_FIELDS: Final = ("path", "sha256", "mode")
EVALUATOR_TRANSITION_SCHEMA: Final = "bn-2l3n-evaluator-transition-v3"
EVALUATOR_TRANSITION_FIELDS: Final = (
    "schema", "protocol", "attempt_nonce", "pre_guard", "child",
    "post_snapshot", "lease_held", "completed_at", "completed_monotonic_ns",
)
EVALUATOR_TRANSITION_CHILD_FIELDS: Final = (
    "argv", "environment", "runtime", "support", "identity", "waited_pid",
    "started_at", "started_monotonic_ns", "completed_at", "completed_monotonic_ns",
    "exit_status", "timed_out", "terminated_by_runner", "interrupted", "reaping",
    "process_group_absent", "orphan_process_group_detected", "stdout", "stderr",
)
EVALUATOR_TRANSITION_FILE_FIELDS: Final = ("path", "sha256", "bytes", "mode")
EVALUATOR_TRANSITION_BINDING_FIELDS: Final = ("path", "sha256")
EVALUATOR_TRANSITION_ENV: Final = {
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "TZ": "UTC",
    "ASTERISM_REBASELINE_MODE": "evaluate",
}
LEASE_HELD_PROOF_FIELDS: Final = (
    "path", "device", "inode", "holder_pid", "holder_start_ticks", "nonce",
    "proc_locks_proof", "second_exclusive_failed", "observed_at",
    "observed_monotonic_ns",
)

ROW_SCHEMAS: Final = {
    "primary": "bn-2l3n-primary-row-v3",
    "new_names": "bn-2l3n-new-names-row-v3",
    "fairness": "bn-2l3n-fairness-row-v3",
    "reopen": "bn-2l3n-reopen-row-v3",
    "cpu_profiles": "bn-2l3n-cpu-profile-row-v3",
    "syscall_profiles": "bn-2l3n-syscall-profile-row-v3",
    "structural_traces": "bn-2l3n-structural-trace-row-v3",
}

CSV_FILENAMES: Final = {track: f"{track}.csv" for track in ROW_SCHEMAS}
TRACK_EXECUTION_ORDER: Final = (
    "primary",
    "new_names",
    "fairness",
    "cpu_profiles",
    "syscall_profiles",
    "reopen",
    "structural_traces",
)
CONFIG_CELL_ORDER_TRACKS: Final = (
    "primary",
    "new_names",
    "fairness",
    "cpu_profiles",
    "syscall_profiles",
)
# This is the insertion order used by run_rebaseline.TRACK_CARDINALITY when it
# constructs the once-per-shape smoke plan.  It intentionally differs from
# TRACK_EXECUTION_ORDER, whose tail is the physical measurement-row order.
RUNNER_SMOKE_TRACK_ORDER: Final = (
    "primary",
    "new_names",
    "fairness",
    "cpu_profiles",
    "syscall_profiles",
    "structural_traces",
    "reopen",
)
RUNNER_SMOKE_TOOL_TARGETS: Final = ("correctness", "fault")
RUNNER_SMOKE_RUNTIME_ROLES: Final = (
    ("evaluator", "evaluator_runtime", "evaluator"),
    ("terminal_verifier", "terminal_verifier_runtime", "terminal_verifier"),
)
SPECIALIZED_SMOKE_TRANSITION_KINDS: Final = (
    "smoke_reopen_seed",
    "smoke_reopen",
    "smoke_structural_reopen",
)
TRANSITION_CHILD_KINDS: Final = (
    "contract",
    "smoke",
    *SPECIALIZED_SMOKE_TRANSITION_KINDS,
)


def transition_child_kind(transition_id: object) -> str | None:
    """Return the one child kind authorized by a config transition ID."""

    if not isinstance(transition_id, str) or not transition_id:
        return None
    if transition_id.startswith("contract-"):
        return "contract"
    if transition_id in SPECIALIZED_SMOKE_TRANSITION_KINDS:
        return transition_id
    return "smoke"


CONTROL_EVENTS_BY_TRACK: Final = {
    "primary": ("ready", "start", "measured", "release"),
    "new_names": ("ready", "start", "measured", "release"),
    "fairness": ("ready", "start", "measured", "release"),
    "cpu_profiles": ("ready", "start", "measured", "release"),
    "syscall_profiles": ("ready", "start", "measured", "release"),
    "reopen": ("boot", "runtime", "ready", "start", "opened", "measured", "release"),
    "structural_traces": ("ready", "start", "completed"),
}
EXPECTED_CARDINALITY: Final = {
    "primary": 512,
    "new_names": 64,
    "fairness": 64,
    "reopen": 9,
    "cpu_profiles": 64,
    "syscall_profiles": 64,
    "structural_traces": 15,
}

CONFIG_FIELDS: Final = (
    "schema",
    "protocol",
    "protocol_sha256",
    "approved",
    "review_id",
    "tooling_commit",
    "tooling_tree",
    "attempt_nonce",
    "seed_sha256",
    "cell_orders",
    "variant_sources",
    "lock_hashes",
    "resource_limits",
    "settle_ms",
    "argv_templates",
    "smoke_transitions",
    "correctness_cases",
    "correctness_execution",
    "profile_contract_sha256",
)

PROVENANCE_FIELDS: Final = (
    "schema",
    "protocol",
    "protocol_sha256",
    "evidence_mode",
    "attempt_nonce",
    "output_dir",
    "output_dir_absent_before",
    "source_approval_path",
    "source_approval_sha256",
    "config_path",
    "config_sha256",
    "prepared_artifacts_path",
    "prepared_artifacts_sha256",
    "runner_path",
    "runner_sha256",
    "evaluator_path",
    "evaluator_sha256",
    "terminal_verifier_path",
    "terminal_verifier_sha256",
    "schema_path",
    "schema_sha256",
    "profile_adapter_path",
    "profile_adapter_sha256",
    "profile_contract_path",
    "profile_contract_sha256",
    "correctness_path",
    "correctness_sha256",
    "correctness_executable_path",
    "correctness_executable_sha256",
    "csv_artifacts",
    "raw_manifest_path",
    "raw_manifest_sha256",
    "guard_manifest_path",
    "guard_manifest_sha256",
    "child_manifest_path",
    "child_manifest_sha256",
    "lease",
    "host",
    "started_at",
    "started_monotonic_ns",
    "completed_at",
    "completed_monotonic_ns",
    "partial",
    "failure_absent",
)
PROVENANCE_HOST_FIELDS: Final = (
    "runner", "hostname", "boot_id", "kernel", "cpu_model", "cpu_topology",
    "governors", "turbo", "affinity", "page_size", "cpu_count",
    "memory_bytes", "filesystem",
    "scheduler", "uid", "scratch_root", "scratch_free_bytes_initial",
    "scratch_free_inodes_initial", "scratch_free_bytes_final",
    "scratch_free_inodes_final", "tracked_comm", "frozen_files",
    "guard_records", "child_records", "resource_manifest_path",
    "resource_manifest_sha256", "correctness_manifest_path",
    "correctness_manifest_sha256",
)
PROVENANCE_CPU_TOPOLOGY_FIELDS: Final = (
    "logical_cpus", "physical_packages", "cores", "threads_per_core",
)
PROVENANCE_TURBO_FIELDS: Final = (
    "intel_pstate_no_turbo", "cpufreq_boost",
)
PROVENANCE_SCHEDULER_FIELDS: Final = (
    "logical_device", "base_device", "scheduler_path", "scheduler_value",
)
PROVENANCE_FILESYSTEM_FIELDS: Final = (
    "mount_id", "parent_mount_id", "device", "root", "target",
    "mount_options", "filesystem_type", "source", "super_options",
)

SOURCE_APPROVAL_FIELDS: Final = (
    "schema",
    "protocol",
    "protocol_sha256",
    "status",
    "review_id",
    "reviewed_at",
    "tooling_commit",
    "tooling_tree",
    "toolchain",
    "shared_manifest_sha256",
    "tools_manifest",
    "tools_manifest_sha256",
    "filesystem_admission",
    "comm_allowlist",
    "variants",
)
SOURCE_APPROVAL_VARIANT_FIELDS: Final = (
    "product_commit",
    "product_tree",
    "binary_kind",
    "timed_surface",
    "adapter_sha256",
    "cargo_lock_sha256",
    "overlay_manifest_sha256",
    "allowed_overlay_paths",
    "lock_resolution",
    "current_lock_attempt",
    "correctness_oracle_mode",
    "profile_role_lifetime",
    "trace_path_marker_templates",
)
PREPARED_FIELDS: Final = (
    "schema",
    "protocol",
    "protocol_sha256",
    "tooling_commit",
    "tooling_tree",
    "created_at",
    "created_monotonic_ns",
    "source_approval",
    "tools_manifest",
    "single_use_claim",
    "comm_allowlist",
    "tools",
    "support_files",
    "inputs",
    "filesystem_admission",
    "build_order",
    "toolchain",
    "variants",
)
PREPARED_SOURCE_APPROVAL_RELATIVE_PATH: Final = (
    "bindings",
    "source-approval.json",
)
PREPARED_CLAIM_FIELDS: Final = (
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
FILESYSTEM_ADMISSION_SCHEMA: Final = "asterism-rebaseline-filesystem-admission-v3"
FILESYSTEM_ADMISSION_FIELDS: Final = (
    "schema", "checked_path", "filesystem", "available_bytes",
    "available_inodes", "minimum_available_bytes", "minimum_available_inodes",
)
PREPARED_VARIANT_FIELDS: Final = (
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
PREPARED_ATTESTATION_FIELDS: Final = (
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
FILE_BINDING_FIELDS: Final = ("path", "sha256")
CARGO_CONFIG_SEARCH_SCHEMA: Final = "asterism-rebaseline-cargo-config-search-v3"
CARGO_CONFIG_SEARCH_FIELDS: Final = (
    "schema", "cargo_home_path", "cwd", "entries",
)
CARGO_CONFIG_SEARCH_ENTRY_FIELDS: Final = ("path", "status", "sha256")
TOOL_BINDING_FIELDS: Final = ("path", "sha256", "executable_mode", "comm")
TOOLCHAIN_FIELDS: Final = (
    "bwrap_path", "bwrap_sha256", "cargo_home_path", "cargo_path",
    "cargo_sha256", "cargo_version_verbose", "git_path", "git_sha256",
    "rustc_path", "rustc_sha256", "rustc_version_verbose", "rustc_host",
    "rustup_home_path", "rustup_path", "rustup_sha256", "rustup_toolchain",
)
LOCK_RESOLUTION_FIELDS: Final = (
    "argv", "cwd", "exit_status", "stdout_sha256", "stderr_sha256", "stdout",
    "stderr", "toolchain", "environment", "cargo_config_search",
)
SANITIZED_CARGO_ENV_FIELDS: Final = (
    "CARGO_HOME", "CARGO_INCREMENTAL", "CARGO_NET_OFFLINE",
    "GIT_CONFIG_COUNT", "GIT_CONFIG_GLOBAL", "GIT_CONFIG_NOSYSTEM", "HOME",
    "LANG", "LC_ALL", "PATH", "RUSTC", "RUSTUP_HOME", "RUSTUP_TOOLCHAIN",
    "TZ",
)
CONTRACT_ENV_FIELDS: Final = (
    "HOME", "LANG", "LC_ALL", "PATH", "TZ", "ASTERISM_REBASELINE_MODE",
)
BUILD_ENV_FIELDS: Final = (
    *SANITIZED_CARGO_ENV_FIELDS,
    "ASTERISM_BUILD_ADAPTER_SHA256",
    "ASTERISM_BUILD_BINARY_KIND", "ASTERISM_BUILD_NONCE",
    "ASTERISM_BUILD_CARGO_LOCK_SHA256", "ASTERISM_BUILD_PRODUCT_COMMIT",
    "ASTERISM_BUILD_PRODUCT_TREE", "ASTERISM_BUILD_PROTOCOL",
    "ASTERISM_BUILD_PROTOCOL_SHA256", "ASTERISM_BUILD_SHARED_MANIFEST_SHA256",
    "ASTERISM_BUILD_SOURCE_APPROVAL_SHA256", "ASTERISM_BUILD_TIMED_SURFACE",
    "ASTERISM_BUILD_TOOLING_COMMIT", "ASTERISM_BUILD_TOOLING_TREE",
    "ASTERISM_BUILD_VARIANT",
)


def sanitized_cargo_environment(toolchain: Mapping[str, Any]) -> dict[str, str]:
    """Return the complete non-inherited environment for Cargo subprocesses."""

    cargo = Path(str(toolchain["cargo_path"]))
    rustc = Path(str(toolchain["rustc_path"]))
    path_entries = list(dict.fromkeys((str(cargo.parent), str(rustc.parent), "/usr/bin", "/bin")))
    cargo_home = str(toolchain["cargo_home_path"])
    return {
        "CARGO_HOME": cargo_home,
        "CARGO_INCREMENTAL": "0",
        "CARGO_NET_OFFLINE": "true",
        "GIT_CONFIG_COUNT": "0",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": ":".join(path_entries),
        "RUSTC": str(rustc),
        "RUSTUP_HOME": str(toolchain["rustup_home_path"]),
        "RUSTUP_TOOLCHAIN": str(toolchain["rustup_toolchain"]),
        "TZ": "UTC",
    }


def sanitized_contract_environment(toolchain: Mapping[str, Any]) -> dict[str, str]:
    cargo_environment = sanitized_cargo_environment(toolchain)
    return {
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": cargo_environment["PATH"],
        "TZ": "UTC",
        "ASTERISM_REBASELINE_MODE": "contract",
    }
PREPARED_TOOL_NAMES: Final = (
    "runner_runtime",
    "evaluator_runtime",
    "terminal_verifier_runtime",
    "perf",
    "strace",
    "strace_launcher_runtime",
    "correctness",
    "fault",
)
PREPARED_TOOL_COMMS: Final = {
    "correctness": "ast-rb-check",
    "evaluator_runtime": "asterism-eval",
    "fault": "ast-rb-fault",
    "perf": "perf",
    "runner_runtime": "asterism-run",
    "strace": "strace",
    "strace_launcher_runtime": "ast-trace-wait",
    "terminal_verifier_runtime": "asterism-term",
}
VARIANT_COMMS: Final = {
    "A": "ast-rb-a", "B": "ast-rb-b", "C": "ast-rb-c", "D": "ast-rb-d",
}
BASE_TRACKED_COMMS: Final = (
    "ar", "ast-rb-a", "ast-rb-b", "ast-rb-c", "ast-rb-d",
    "build-script-bu", "cargo", "cargo-nextest", "cc", "clang", "clang++",
    "clippy-driver", "collect2", "g++", "gcc", "ld", "ld.lld", "mold",
    "nextest", "ranlib", "rustc", "rustdoc", "rustfmt",
)


def expected_comm_allowlist() -> list[str]:
    """Return the one source-approved process-name guard authority."""

    return sorted(
        set(BASE_TRACKED_COMMS)
        | set(VARIANT_COMMS.values())
        | set(PREPARED_TOOL_COMMS.values())
    )
SUPPORT_FILE_FIELDS: Final = ("path", "sha256", "mode")
TOOLS_MANIFEST_SCHEMA: Final = "asterism-rebaseline-tools-v3"
TOOLS_MANIFEST_FIELDS: Final = (
    "schema", "comm_allowlist", "tools", "support_files",
)
TOOLS_MANIFEST_BINDING_FIELDS: Final = ("path", "sha256", "mode")
PREPARED_SUPPORT_FILE_NAMES: Final = (
    "runner",
    "evaluator",
    "terminal_verifier",
    "evidence_schema",
    "profile_adapter",
    "strace_attach",
)
PREPARED_INPUT_FIELDS: Final = ("path", "sha256", "mode")
PREPARED_INPUT_NAMES: Final = ("protocol", "historical_baseline")
PREPARED_INPUT_FILENAMES: Final = {
    "protocol": "BN-2L3N-PROTOCOL.md",
    "historical_baseline": "BN-2SU-FINAL.csv",
}
PREPARED_INPUT_RELATIVE_PATHS: Final = {
    "protocol": "BN-2L3N-PROTOCOL.md",
    "historical_baseline": "inputs/BN-2SU-FINAL.csv",
}
PREPARED_INPUT_SHA256: Final = {
    "protocol": PROTOCOL_SHA256,
    "historical_baseline": HISTORICAL_BASELINE_SHA256,
}


def expected_result_artifact_names(*, correctness_only: bool) -> tuple[str, ...]:
    """Return the evaluator inputs that one result must bind byte-for-byte."""

    common = (
        PREPARED_INPUT_FILENAMES["protocol"],
        PREPARED_INPUT_FILENAMES["historical_baseline"],
        "config.json",
        "source-approval.json",
        "prepared-artifacts.json",
        "provenance.json",
        "correctness.json",
        "raw-manifest.json",
        "guard-manifest.jsonl",
        "child-manifest.jsonl",
    )
    suffix = (
        ("correctness-only.json",)
        if correctness_only
        else tuple(CSV_FILENAMES.values())
    )
    return (*common, *suffix)


TRACE_LOG_PATH_MARKERS: Final = {
    "A": ({"kind": "file_prefix", "path": "log/seg-"},),
    "B": ({"kind": "exact", "path": "segment-1.log"},),
    "C": ({"kind": "file_prefix", "path": "log/seg-"},),
    "D": ({"kind": "file_prefix", "path": "log/seg-"},),
}
TRACE_METADATA_PATH_MARKERS: Final = {
    "A": (
        {"kind": "directory_prefix", "path": "log/sealed/"},
        {"kind": "directory_prefix", "path": "snapshots/blobs/"},
        {"kind": "directory_prefix", "path": "snapshots/meta/"},
    ),
    "B": (),
    "C": (
        {"kind": "directory_prefix", "path": "log/meta/"},
        {"kind": "directory_prefix", "path": "log/sealed/"},
        {"kind": "directory_prefix", "path": "snapshots/blobs/"},
        {"kind": "directory_prefix", "path": "snapshots/meta/"},
    ),
    "D": (
        {"kind": "directory_prefix", "path": "log/sealed/"},
        {"kind": "directory_prefix", "path": "snapshots/blobs/"},
        {"kind": "directory_prefix", "path": "snapshots/meta/"},
    ),
}
TRACE_MARKER_ENV_FIELDS: Final = (
    "ASTERISM_REBASELINE_LOG_PATH_MARKERS",
    "ASTERISM_REBASELINE_METADATA_PATH_MARKERS",
)


def expected_trace_marker_environment(variant: str) -> dict[str, str]:
    """Return exact, disjoint path-marker authority for structural tracing."""

    if variant not in VARIANTS:
        raise ValueError(f"unknown trace-marker variant {variant}")
    return {
        "ASTERISM_REBASELINE_LOG_PATH_MARKERS": json.dumps(
            list(TRACE_LOG_PATH_MARKERS[variant]), separators=(",", ":")
        ),
        "ASTERISM_REBASELINE_METADATA_PATH_MARKERS": json.dumps(
            list(TRACE_METADATA_PATH_MARKERS[variant]), separators=(",", ":")
        ),
    }


def validate_trace_marker_environment(variant: str, value: Any) -> None:
    if not isinstance(value, dict) or set(value) != set(TRACE_MARKER_ENV_FIELDS):
        raise ValueError(f"{variant} trace-marker environment fields are not exact")
    expected = expected_trace_marker_environment(variant)
    if value != expected:
        raise ValueError(f"{variant} trace-marker environment differs from authority")
    _validated_trace_marker_templates(variant)


def _validated_trace_marker_templates(
    variant: str,
) -> dict[str, list[dict[str, str]]]:
    groups = {
        "log": list(TRACE_LOG_PATH_MARKERS[variant]),
        "metadata": list(TRACE_METADATA_PATH_MARKERS[variant]),
    }
    flattened: list[tuple[str, str, str]] = []
    for family, markers in groups.items():
        for marker in markers:
            if not isinstance(marker, dict) or set(marker) != {"kind", "path"}:
                raise ValueError(f"{variant} {family} trace marker fields differ")
            kind = marker.get("kind")
            path = marker.get("path")
            if kind not in {"exact", "file_prefix", "directory_prefix"} or not isinstance(
                path, str
            ):
                raise ValueError(f"{variant} {family} trace marker type differs")
            directory = kind == "directory_prefix"
            candidate = path[:-1] if directory and path.endswith("/") else path
            if (
                not path
                or path.startswith("/")
                or "//" in path
                or "\\" in path
                or "\x00" in path
                or directory != path.endswith("/")
                or not candidate
                or any(part in {"", ".", ".."} for part in candidate.split("/"))
                or str(Path(candidate)) != candidate
            ):
                raise ValueError(
                    f"{variant} {family} trace marker path is not canonical relative"
                )
            flattened.append((family, str(kind), path))
    if not flattened or len({(kind, path) for _, kind, path in flattened}) != len(
        flattened
    ):
        raise ValueError(f"{variant} trace markers are empty or duplicated")
    for index, (left_family, left_kind, left_path) in enumerate(flattened):
        for right_family, right_kind, right_path in flattened[index + 1 :]:
            if _typed_marker_overlap(
                (left_kind, left_path), (right_kind, right_path)
            ):
                raise ValueError(
                    f"{variant} {left_family}/{right_family} trace markers overlap: "
                    f"{left_path!r} vs {right_path!r}"
                )
    return {
        family: [dict(marker) for marker in markers]
        for family, markers in groups.items()
    }


def expected_trace_path_marker_templates(variant: str) -> dict[str, Any]:
    return {
        "root_environment": "ASTERISM_REBASELINE_STORE",
        **_validated_trace_marker_templates(variant),
    }


def _typed_marker_overlap(
    left: tuple[str, str], right: tuple[str, str]
) -> bool:
    left_kind, left_path = left
    right_kind, right_path = right
    if left_kind == "exact":
        return left_path == right_path if right_kind == "exact" else left_path.startswith(
            right_path
        )
    if right_kind == "exact":
        return right_path.startswith(left_path)
    return left_path.startswith(right_path) or right_path.startswith(left_path)


def resolved_trace_path_markers(store_path: Path, variant: str) -> dict[str, list[dict[str, str]]]:
    """Resolve exact source-approved relative templates under one canonical store."""

    store = Path(store_path)
    if (
        not store.is_absolute()
        or str(store) != str(store_path)
        or str(store).startswith("//")
        or ".." in store.parts
    ):
        raise ValueError("trace marker store path is not canonical absolute")
    templates = _validated_trace_marker_templates(variant)
    resolved: dict[str, list[dict[str, str]]] = {"log": [], "metadata": []}
    for family, markers in templates.items():
        for marker in markers:
            directory = marker["kind"] == "directory_prefix"
            relative = marker["path"][:-1] if directory else marker["path"]
            absolute = str(store / relative) + ("/" if directory else "")
            resolved[family].append({"kind": marker["kind"], "path": absolute})
    return resolved

PROFILE_PREFLIGHT_SCHEMA: Final = "bn-2l3n-profile-preflight-v3"
PROFILE_ADAPTER_SCHEMA: Final = "bn-2l3n-profile-adapters-v3"
PROFILE_AUTHORITY_SCHEMA: Final = "bn-2l3n-profile-authority-v3"
PROFILE_PERF_EVENT_SPECS: Final = (
    "cycles:u",
    "instructions:u",
    "task-clock:u",
    "context-switches:u",
)
PROFILE_SYSCALL_EVENTS: Final = (
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
PROFILE_OPEN_HELPER_COMMS: Final = (
    "fjall:worker",
    "mess-engine-rol",
    "mess-sealer",
)
PROFILE_TOKIO_WORKER_COMM: Final = "tokio-runtime-w"
PROFILE_C_ROLE_LIFETIME_CONTRACT: Final = {
    "schema": "bn-2l3n-c-role-lifetime-v3",
    "blocking_thread_keep_alive_ns": 3_600_000_000_000,
    "maximum_profile_child_timeout_ns": 120_000_000_000,
    "ready_to_measured_spawn_blocking_sites": 1,
    "ready_to_measured_other_thread_birth_sites": 0,
}
PROFILE_PREFLIGHT_FIELDS: Final = (
    "schema",
    "protocol",
    "protocol_sha256",
    "profile_contract_sha256",
    "source",
    "helper",
    "samples_ns",
    "minimum_nonzero_increment_ns",
    "decision_multiplier",
    "decision_floor_ns",
)
SCHEDSTAT_DECISION_MULTIPLIER: Final = 20
PROFILE_RAW_ARTIFACT_BINDING_FIELDS: Final = ("path", "sha256", "bytes", "mode")
PROFILE_TOOL_INPUT_FIELDS_BY_TRACK: Final = {
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
EXTERNAL_PROFILE_SMOKE_TRACKS: Final = (
    "cpu_profiles",
    "syscall_profiles",
    "structural_traces",
)


def profile_tool_track_for_child(
    kind: object, context: Mapping[str, Any] | None
) -> str | None:
    """Resolve only explicit or external-tool smoke profiling authority."""

    if isinstance(kind, str) and kind in PROFILE_TOOL_INPUT_FIELDS_BY_TRACK:
        return str(kind)
    if not isinstance(context, Mapping):
        return None
    explicit = context.get("profile_smoke_track")
    if (
        isinstance(explicit, str)
        and explicit in PROFILE_TOOL_INPUT_FIELDS_BY_TRACK
    ):
        return str(explicit)
    external = context.get("smoke_target")
    if isinstance(external, str) and external in EXTERNAL_PROFILE_SMOKE_TRACKS:
        return str(external)
    return None


PROFILE_AUTHORITY_FIELDS: Final = (
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
)
PROFILE_RICH_RESULT_FIELDS: Final = (
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
)


def expected_profile_contract() -> dict[str, Any]:
    """Return the exact adapter authority hashed into profile preflight."""

    return {
        "schema": PROFILE_ADAPTER_SCHEMA,
        "protocol": PROTOCOL,
        "protocol_sha256": PROTOCOL_SHA256,
        "authority_schema": PROFILE_AUTHORITY_SCHEMA,
        "c_role_lifetime_contract": PROFILE_C_ROLE_LIFETIME_CONTRACT,
        "variant_source_bindings": VARIANT_SOURCE_BINDINGS,
        "schedstat_decision_multiplier": SCHEDSTAT_DECISION_MULTIPLIER,
        "perf_events": list(PROFILE_PERF_EVENT_SPECS),
        "perf_event_scope_policy": "exact-user-only",
        "syscall_events": list(PROFILE_SYSCALL_EVENTS),
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
            "sha256": hashlib.sha256(b"ack\nack\n").hexdigest(),
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
                f"producer-runtime:boot-to-runtime-births:comm={PROFILE_TOKIO_WORKER_COMM}",
                f"spawn_blocking-publication:ready-to-measured-births:comm={PROFILE_TOKIO_WORKER_COMM}",
            ],
            "D": ["owner:comm=mess-flat-owner"],
        },
        "open_helper_comms": list(PROFILE_OPEN_HELPER_COMMS),
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


def expected_profile_contract_sha256() -> str:
    return hashlib.sha256(canonical_json_bytes(expected_profile_contract())).hexdigest()

# The child manifest is a byte-level execution and composition ledger.  The
# context and profile payloads vary by transition, but their hashes and every
# state transition around them are fixed here.  Successful admission evidence
# never contains a partially populated child record.
CHILD_FIELDS: Final = (
    "schema",
    "protocol",
    "ordinal",
    "kind",
    "context",
    "context_sha256",
    "argv",
    "environment",
    "executable_path",
    "executable_sha256",
    "executable_mode",
    "executable_comm",
    "identity",
    "waited_pid",
    "started_at",
    "started_monotonic_ns",
    "completed_at",
    "completed_monotonic_ns",
    "exit_status",
    "timed_out",
    "terminated_by_runner",
    "interrupted",
    "reaping",
    "process_group_absent",
    "orphan_process_group_detected",
    "control_events",
    "control_events_sha256",
    "profile_events",
    "profile_events_sha256",
    "parked_state_proofs",
    "profile_rich_result",
    "runner_context",
    "runner_context_sha256",
    "profile_result",
    "profile_result_sha256",
    "profile_contract_sha256",
    "profile_tool_inputs",
    "profile_tool_inputs_sha256",
    "profile_tool_helper_records",
    "raw_path",
    "raw_sha256",
    "raw_bytes",
    "raw_mode_after",
    "stderr_path",
    "stderr_sha256",
    "stderr_bytes",
    "stderr_mode_after",
    "expected_records",
    "combined_row_sha256",
    "csv_append",
    "guard_pre_ordinal",
    "guard_post_ordinal",
    "validation_error",
)
PROCESS_IDENTITY_FIELDS: Final = (
    "pid",
    "comm",
    "state",
    "ppid",
    "pgrp",
    "session",
    "starttime_ticks",
)
REAPING_FIELDS: Final = ("pid", "start_ticks", "status")
CSV_APPEND_FIELDS: Final = (
    "path",
    "bytes_before",
    "bytes_after",
    "rows_before",
    "rows_after",
    "prefix_sha256_before",
    "prefix_sha256_after",
    "sha256_after",
)
CHILD_FILE_BINDING_FIELDS: Final = ("path", "sha256", "bytes")
RAW_BINDING_FIELDS: Final = (
    "schema",
    "protocol",
    "child_ordinal",
    "kind",
    "context_sha256",
    "raw_path",
    "raw_sha256",
    "raw_bytes",
    "expected_records",
    "stderr_path",
    "stderr_sha256",
)
CHILD_KINDS: Final = (
    *TRANSITION_CHILD_KINDS,
    "correctness",
    "fault",
    "primary",
    "new_names",
    "fairness",
    "cpu_profiles",
    "syscall_profiles",
    "reopen",
    "structural_traces",
)

# A guard-manifest line binds one immutable, fully populated process snapshot.
# This two-level contract avoids duplicating a large /proc scan in JSONL while
# still making both the ledger and the snapshot independently replayable.
GUARD_BINDING_FIELDS: Final = (
    "schema",
    "protocol",
    "kind",
    "ordinal",
    "label",
    "path",
    "sha256",
    "verdict",
    "started_monotonic_ns",
    "completed_monotonic_ns",
)
GUARD_SNAPSHOT_FIELDS: Final = (
    "schema",
    "protocol",
    "ordinal",
    "label",
    "tracked_comm",
    "runner",
    "active_child",
    "active_helpers",
    "records",
    "final_resource",
    "preidentity_vanished",
    "verdict",
    "started_at",
    "started_monotonic_ns",
    "completed_at",
    "completed_monotonic_ns",
)
GUARD_PROCESS_FIELDS: Final = (
    *PROCESS_IDENTITY_FIELDS,
    "uid",
    "exe",
    "exe_sha256",
    "cmdline",
    "read_errors",
    "classification",
    "observed_at",
)
GUARD_RESOURCE_FIELDS: Final = ("load1", "free_bytes", "free_inodes", "enforced")

CORRECTNESS_CASE_IDS: Final = (
    "public-ordinary-append-command-cache-read-subscribe",
    "same-stream-exact-race",
    "registry-first-use-ordered-failure-unit",
    "error-ordering",
    "cancel-before-admission",
    "cancel-after-ownership",
    "borrowed-owned-mixed-order-and-type",
    "two-live-rolls",
    "clean-repeated-active-tail-sealed-recovery",
    "kill-pre-write",
    "kill-partial-write",
    "kill-post-write-pre-barrier",
    "kill-post-barrier-pre-publication",
    "kill-post-publication-pre-completion",
    "short-write",
    "write-error",
    "fdatasync-error",
    "torn-truncated-tail",
    "invalid-marker-crc",
    "corrupt-registry-record",
    "refuted-corrupt-sidecar",
    "uncertain-persistence-poison",
    "acknowledged-group-survives-reopen",
    "owner-ring-intent-bound",
    "group-byte-time-bounds",
    "zero-reservations-after-cancel-complete",
    "fault-hook-compiles-out-binary-identical",
)
CORRECTNESS_CHILD_FIELDS: Final = (
    "schema", "protocol", "attempt_nonce", "variant", "phase", "suite",
    "harness_sound", "boundedness", "cases",
)
CORRECTNESS_CHILD_CASE_FIELDS: Final = ("id", "classification", "status")
CORRECTNESS_BOUNDEDNESS_FIELDS: Final = (
    "owner_ring_intents", "group_byte_bound_proven", "group_time_bound_proven",
    "waiter_reservations_after", "byte_reservations_after",
)
CORRECTNESS_EXPECTED_BOUNDEDNESS: Final = {
    "owner_ring_intents": 1024,
    "group_byte_bound_proven": True,
    "group_time_bound_proven": True,
    "waiter_reservations_after": 0,
    "byte_reservations_after": 0,
}
CORRECTNESS_AGGREGATE_FIELDS: Final = (
    "schema", "protocol", "attempt_nonce", "harness_sound", "boundedness", "cases",
)
CORRECTNESS_AGGREGATE_CASE_FIELDS: Final = (
    "id", "variant", "phase", "suite", "kind", "classification", "status",
    "child_ordinal", "output_path", "output_sha256",
)
CORRECTNESS_EXECUTION_FIELDS: Final = (
    "variant", "phase", "suite", "kind", "executable_path",
    "executable_sha256", "executable_mode", "executable_comm", "argv",
    "environment",
)
CORRECTNESS_ENV_FIELDS: Final = (
    "ASTERISM_REBASELINE_MODE",
    "ASTERISM_REBASELINE_PROTOCOL",
    "ASTERISM_REBASELINE_ATTEMPT_NONCE",
    "ASTERISM_REBASELINE_VARIANT",
    "ASTERISM_REBASELINE_PHASE",
    "ASTERISM_REBASELINE_SUITE",
)
CORRECTNESS_GROUPS: Final = (
    ("C", "oracle", "common-public-oracle", "correctness"),
    ("D", "oracle", "common-public-oracle", "correctness"),
    ("A", "pre", "current-product", "correctness"),
    ("A", "pre", "current-fault", "fault"),
    ("A", "post", "current-product", "correctness"),
    ("A", "post", "current-fault", "fault"),
)
CORRECTNESS_CASE_PARTITIONS: Final = {
    "public-ordinary-append-command-cache-read-subscribe": {"suite": "current-product", "kind": "correctness", "classification": "correctness"},
    "same-stream-exact-race": {"suite": "current-product", "kind": "correctness", "classification": "correctness"},
    "registry-first-use-ordered-failure-unit": {"suite": "current-product", "kind": "correctness", "classification": "correctness"},
    "error-ordering": {"suite": "current-product", "kind": "correctness", "classification": "correctness"},
    "cancel-before-admission": {"suite": "current-fault", "kind": "fault", "classification": "cancellation"},
    "cancel-after-ownership": {"suite": "current-fault", "kind": "fault", "classification": "cancellation"},
    "borrowed-owned-mixed-order-and-type": {"suite": "current-product", "kind": "correctness", "classification": "correctness"},
    "two-live-rolls": {"suite": "current-product", "kind": "correctness", "classification": "roll-recovery"},
    "clean-repeated-active-tail-sealed-recovery": {"suite": "current-product", "kind": "correctness", "classification": "roll-recovery"},
    "kill-pre-write": {"suite": "current-fault", "kind": "fault", "classification": "durability"},
    "kill-partial-write": {"suite": "current-fault", "kind": "fault", "classification": "durability"},
    "kill-post-write-pre-barrier": {"suite": "current-fault", "kind": "fault", "classification": "durability"},
    "kill-post-barrier-pre-publication": {"suite": "current-fault", "kind": "fault", "classification": "durability"},
    "kill-post-publication-pre-completion": {"suite": "current-fault", "kind": "fault", "classification": "durability"},
    "short-write": {"suite": "current-fault", "kind": "fault", "classification": "durability"},
    "write-error": {"suite": "current-fault", "kind": "fault", "classification": "durability"},
    "fdatasync-error": {"suite": "current-fault", "kind": "fault", "classification": "durability"},
    "torn-truncated-tail": {"suite": "current-fault", "kind": "fault", "classification": "roll-recovery"},
    "invalid-marker-crc": {"suite": "current-fault", "kind": "fault", "classification": "roll-recovery"},
    "corrupt-registry-record": {"suite": "current-fault", "kind": "fault", "classification": "roll-recovery"},
    "refuted-corrupt-sidecar": {"suite": "current-fault", "kind": "fault", "classification": "roll-recovery"},
    "uncertain-persistence-poison": {"suite": "current-fault", "kind": "fault", "classification": "poison"},
    "acknowledged-group-survives-reopen": {"suite": "current-fault", "kind": "fault", "classification": "durability"},
    "owner-ring-intent-bound": {"suite": "current-fault", "kind": "fault", "classification": "boundedness"},
    "group-byte-time-bounds": {"suite": "current-fault", "kind": "fault", "classification": "boundedness"},
    "zero-reservations-after-cancel-complete": {"suite": "current-fault", "kind": "fault", "classification": "boundedness"},
    "fault-hook-compiles-out-binary-identical": {"suite": "current-fault", "kind": "fault", "classification": "harness"},
}


def correctness_argv(
    executable: str,
    variant: str,
    phase: str,
    suite: str,
    attempt_nonce: str,
) -> list[str]:
    """Return the exact attempt-bound command for one correctness child."""

    if variant in {"C", "D"}:
        mode_flag = "--correctness-oracle"
    elif suite == "current-product":
        mode_flag = "--correctness"
    elif suite == "current-fault":
        mode_flag = "--fault"
    else:
        raise ValueError("unapproved correctness execution identity")
    return [
        executable,
        mode_flag,
        "--protocol",
        PROTOCOL,
        "--attempt-nonce",
        attempt_nonce,
        "--variant",
        variant,
        "--phase",
        phase,
        "--suite",
        suite,
    ]


def correctness_environment(
    variant: str,
    phase: str,
    suite: str,
    attempt_nonce: str,
) -> dict[str, str]:
    """Return the exact non-ambient identity environment for a child."""

    mode = (
        "correctness_oracle"
        if variant in {"C", "D"}
        else "correctness" if suite == "current-product" else "fault"
    )
    return {
        "ASTERISM_REBASELINE_MODE": mode,
        "ASTERISM_REBASELINE_PROTOCOL": PROTOCOL,
        "ASTERISM_REBASELINE_ATTEMPT_NONCE": attempt_nonce,
        "ASTERISM_REBASELINE_VARIANT": variant,
        "ASTERISM_REBASELINE_PHASE": phase,
        "ASTERISM_REBASELINE_SUITE": suite,
    }


CHILD_BASE_ENVIRONMENT: Final = {
    "PATH": "/usr/bin:/bin",
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "TZ": "UTC",
}
ROW_DYNAMIC_ENVIRONMENT_FIELDS: Final = {
    "durability": "ASTERISM_DURABILITY",
    "payload_size": "ASTERISM_PAYLOAD_BYTES",
    "batch_size": "ASTERISM_BATCH",
    "writers": "ASTERISM_WRITERS",
    "batches_per_writer": "ASTERISM_BATCHES_PER_WRITER",
    "trace_kind": "ASTERISM_TRACE_KIND",
    "expected_domain_events": "ASTERISM_EXPECTED_DOMAIN_EVENTS",
    "expected_visible_events": "ASTERISM_EXPECTED_VISIBLE_EVENTS",
    "expected_log_events": "ASTERISM_EXPECTED_LOG_EVENTS",
    "expected_logical_digest": "ASTERISM_EXPECTED_LOGICAL_DIGEST",
    "expected_registry_head_digest": "ASTERISM_EXPECTED_REGISTRY_HEAD_DIGEST",
}


def fresh_store_path(
    scratch_root: Path,
    attempt_nonce: str,
    track: str,
    ordinal: int,
    variant: str,
) -> Path:
    """Return the deterministic, attempt-bound child store identity."""

    identity = hashlib.sha256(
        f"{attempt_nonce}\0{track}\0{ordinal}\0{variant}".encode()
    ).hexdigest()[:20]
    return (
        scratch_root
        / "attempts"
        / attempt_nonce
        / "stores"
        / f"{track}-{ordinal:05d}-{variant}-{identity}"
    )


def _controlled_child_environment(
    *,
    scratch_root: Path,
    attempt_nonce: str,
    output_dir: Path,
    physical_ordinal: int,
    context_sha256: str,
    control_fd: str,
    plan_environment: Mapping[str, str],
    store_path: Path,
    ptracer_pid: int | None = None,
) -> dict[str, str]:
    if not control_fd.isascii() or not control_fd.isdecimal() or int(control_fd) < 3:
        raise ValueError("controlled child file descriptor is not canonical")
    result = {
        "HOME": str(scratch_root / "attempts" / attempt_nonce / "home"),
        **CHILD_BASE_ENVIRONMENT,
        **dict(plan_environment),
        "ASTERISM_REBASELINE_PROTOCOL": PROTOCOL,
        "ASTERISM_REBASELINE_PROTOCOL_SHA256": PROTOCOL_SHA256,
        "ASTERISM_REBASELINE_CONTEXT": str(
            output_dir / "contexts" / f"{physical_ordinal:05d}.json"
        ),
        "ASTERISM_REBASELINE_CONTEXT_SHA256": context_sha256,
        "ASTERISM_CONTEXT_SHA256": context_sha256,
        "ASTERISM_REBASELINE_STORE": str(store_path),
        "ASTERISM_REBASELINE_CONTROL_FD": control_fd,
    }
    if ptracer_pid is not None:
        if isinstance(ptracer_pid, bool) or not isinstance(ptracer_pid, int) or ptracer_pid <= 0:
            raise ValueError("ptracer pid is invalid")
        result["ASTERISM_REBASELINE_PTRACER_PID"] = str(ptracer_pid)
    return result


def transition_child_environment(
    *,
    scratch_root: Path,
    attempt_nonce: str,
    output_dir: Path,
    physical_ordinal: int,
    context_sha256: str,
    plan_environment: Mapping[str, str],
    controlled: bool,
    store_path: Path | None,
    control_fd: str | None = None,
    profile_track: str | None = None,
    ptracer_pid: int | None = None,
    perf_permission_result: str | None = None,
    perf_command_fd: str | None = None,
    perf_ack_fd: str | None = None,
    perf_ack_ledger_fd: str | None = None,
) -> dict[str, str]:
    """Reconstruct the full inherited environment for a row-zero transition."""

    result = {
        "HOME": str(scratch_root / "attempts" / attempt_nonce / "home"),
        **CHILD_BASE_ENVIRONMENT,
        **dict(plan_environment),
        "ASTERISM_REBASELINE_PROTOCOL": PROTOCOL,
        "ASTERISM_REBASELINE_PROTOCOL_SHA256": PROTOCOL_SHA256,
        "ASTERISM_REBASELINE_CONTEXT": str(
            output_dir / "contexts" / f"{physical_ordinal:05d}.json"
        ),
        "ASTERISM_REBASELINE_CONTEXT_SHA256": context_sha256,
        "ASTERISM_CONTEXT_SHA256": context_sha256,
    }
    if controlled:
        if (
            not isinstance(control_fd, str)
            or not control_fd.isascii()
            or not control_fd.isdecimal()
            or int(control_fd) < 3
            or str(int(control_fd)) != control_fd
        ):
            raise ValueError("transition control file descriptor is not canonical")
        result["ASTERISM_REBASELINE_CONTROL_FD"] = control_fd
    elif control_fd is not None:
        raise ValueError("uncontrolled transition has a control descriptor")
    if store_path is not None:
        result["ASTERISM_REBASELINE_STORE"] = str(store_path)
    if ptracer_pid is not None:
        if isinstance(ptracer_pid, bool) or not isinstance(ptracer_pid, int) or ptracer_pid <= 0:
            raise ValueError("transition ptracer pid is invalid")
        result["ASTERISM_REBASELINE_PTRACER_PID"] = str(ptracer_pid)

    perf_fds = (perf_command_fd, perf_ack_fd, perf_ack_ledger_fd)
    if profile_track == "cpu_profiles":
        permission_status = profile_perf_permission_status(perf_permission_result)
        result["ASTERISM_REBASELINE_PERF_PERMISSION_RESULT"] = str(
            perf_permission_result
        )
        if permission_status == "available":
            if any(value is None for value in perf_fds):
                raise ValueError("available transition perf descriptors are partial")
            rendered = tuple(str(value) for value in perf_fds)
            if any(
                not value.isascii()
                or not value.isdecimal()
                or int(value) < 3
                or str(int(value)) != value
                for value in rendered
            ):
                raise ValueError("transition perf descriptor is not canonical")
            if len({str(control_fd), *rendered}) != 4:
                raise ValueError("transition perf/control descriptors are not distinct")
            for name, value in zip(
                (
                    "ASTERISM_REBASELINE_PERF_COMMAND_FD",
                    "ASTERISM_REBASELINE_PERF_ACK_FD",
                    "ASTERISM_REBASELINE_PERF_ACK_LEDGER_FD",
                ),
                rendered,
                strict=True,
            ):
                result[name] = value
        elif any(value is not None for value in perf_fds):
            raise ValueError("unavailable transition perf descriptors were inherited")
    elif perf_permission_result is not None or any(value is not None for value in perf_fds):
        raise ValueError("non-CPU transition has perf environment authority")
    return result


def row_child_environment(
    *,
    scratch_root: Path,
    attempt_nonce: str,
    output_dir: Path,
    config_path: Path,
    physical_ordinal: int,
    track: str,
    identity: Mapping[str, Any],
    context: Mapping[str, Any],
    context_sha256: str,
    control_fd: str,
    ptracer_pid: int | None = None,
    perf_permission_result: str | None = None,
    perf_command_fd: str | None = None,
    perf_ack_fd: str | None = None,
    perf_ack_ledger_fd: str | None = None,
) -> dict[str, str]:
    """Reconstruct every inherited and plan-specific row child variable."""

    row_ordinal = identity["row_ordinal"]
    variant = identity["variant"]
    plan_environment = {
        "ASTERISM_REBASELINE_MODE": track,
        "ASTERISM_REBASELINE_VARIANT": variant,
        "ASTERISM_REBASELINE_CONFIG": str(config_path),
        "ASTERISM_REBASELINE_ROW_ORDINAL": str(row_ordinal),
    }
    for context_field, environment_field in ROW_DYNAMIC_ENVIRONMENT_FIELDS.items():
        value = context.get(context_field)
        if value is not None:
            plan_environment[environment_field] = str(value)
    store_track = (
        f"{track}-corpus"
        if track == "reopen"
        or (track == "structural_traces" and context.get("trace_kind") == "reopen")
        else track
    )
    result = _controlled_child_environment(
        scratch_root=scratch_root,
        attempt_nonce=attempt_nonce,
        output_dir=output_dir,
        physical_ordinal=physical_ordinal,
        context_sha256=context_sha256,
        control_fd=control_fd,
        plan_environment=plan_environment,
        store_path=fresh_store_path(
            scratch_root, attempt_nonce, store_track, row_ordinal, variant
        ),
        ptracer_pid=ptracer_pid,
    )
    perf_fds = (perf_command_fd, perf_ack_fd, perf_ack_ledger_fd)
    if track == "cpu_profiles":
        permission_status = profile_perf_permission_status(perf_permission_result)
        result["ASTERISM_REBASELINE_PERF_PERMISSION_RESULT"] = str(
            perf_permission_result
        )
        if permission_status == "available":
            if any(value is None for value in perf_fds):
                raise ValueError("available perf child file descriptors are partial")
            rendered_fds = tuple(str(value) for value in perf_fds)
            if any(
                not value.isascii()
                or not value.isdecimal()
                or int(value) < 3
                or str(int(value)) != value
                for value in rendered_fds
            ):
                raise ValueError("available perf child file descriptor is not canonical")
            if len({control_fd, *rendered_fds}) != 4:
                raise ValueError("perf/control child file descriptors are not distinct")
            for name, value in zip(
                (
                    "ASTERISM_REBASELINE_PERF_COMMAND_FD",
                    "ASTERISM_REBASELINE_PERF_ACK_FD",
                    "ASTERISM_REBASELINE_PERF_ACK_LEDGER_FD",
                ),
                rendered_fds,
                strict=True,
            ):
                result[name] = value
        elif any(value is not None for value in perf_fds):
            raise ValueError("unavailable perf child inherited file descriptors")
    elif perf_permission_result is not None or any(value is not None for value in perf_fds):
        raise ValueError("non-CPU child has perf environment authority")
    return result


def profile_perf_permission_status(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("perf permission result is not text")
    if re.fullmatch(r"available;perf_event_paranoid=-?\d+;scope=user-only", value):
        return "available"
    unavailable = re.fullmatch(
        r"not_available;perf_event_paranoid=-?\d+;scope=user-only;exit_status=(\d+)",
        value,
    )
    if unavailable is None or int(unavailable.group(1)) == 0:
        raise ValueError("perf permission result is not exact")
    return "not_available"


def correctness_child_environment(
    *,
    scratch_root: Path,
    attempt_nonce: str,
    output_dir: Path,
    physical_ordinal: int,
    variant: str,
    phase: str,
    suite: str,
    phase_ordinal: int,
    context_sha256: str,
    control_fd: str,
) -> dict[str, str]:
    """Reconstruct every inherited and plan-specific correctness variable."""

    plan_environment = correctness_environment(variant, phase, suite, attempt_nonce)
    return _controlled_child_environment(
        scratch_root=scratch_root,
        attempt_nonce=attempt_nonce,
        output_dir=output_dir,
        physical_ordinal=physical_ordinal,
        context_sha256=context_sha256,
        control_fd=control_fd,
        plan_environment=plan_environment,
        store_path=fresh_store_path(
            scratch_root,
            attempt_nonce,
            f"correctness-{'pre' if phase in {'oracle', 'pre'} else 'post'}",
            phase_ordinal,
            variant,
        ),
    )


def correctness_execution_contract(
    prepared: Mapping[str, Any], attempt_nonce: str
) -> list[dict[str, Any]]:
    """Bind all six correctness invocations to prepared executable bytes."""

    result: list[dict[str, Any]] = []
    for variant, phase, suite, kind in CORRECTNESS_GROUPS:
        if variant in {"C", "D"}:
            prepared_variant = prepared["variants"][variant]
            binary = prepared_variant["binary"]
            binding = {
                "path": binary["path"],
                "sha256": binary["sha256"],
                "executable_mode": prepared_variant["executable_mode"],
                "comm": prepared_variant["comm"],
            }
        else:
            binding = prepared["tools"][kind]
        executable = binding["path"]
        result.append(
            {
                "variant": variant,
                "phase": phase,
                "suite": suite,
                "kind": kind,
                "executable_path": executable,
                "executable_sha256": binding["sha256"],
                "executable_mode": binding["executable_mode"],
                "executable_comm": binding["comm"],
                "argv": correctness_argv(
                    executable, variant, phase, suite, attempt_nonce
                ),
                "environment": correctness_environment(
                    variant, phase, suite, attempt_nonce
                ),
            }
        )
    return result


def correctness_descriptors() -> list[dict[str, str]]:
    """Return the one frozen aggregate order shared by runner and evaluator."""

    result = [
        {"id": "public-common-oracle", "variant": variant, "phase": "oracle",
         "suite": "common-public-oracle", "kind": "correctness",
         "classification": "historical-oracle"}
        for variant in ("C", "D")
    ]
    for phase in ("pre", "post"):
        for case_id in CORRECTNESS_CASE_IDS:
            result.append({
                "id": case_id, "variant": "A", "phase": phase,
                **CORRECTNESS_CASE_PARTITIONS[case_id],
            })
    return result

WILLIAMS: Final = {
    1: ("A", "B", "D", "C"),
    2: ("B", "C", "A", "D"),
    3: ("C", "D", "B", "A"),
    4: ("D", "A", "C", "B"),
}

PROCESS_BPW: Final = {1: 40_000, 10: 12_500, 100: 2_500, 1000: 250}
GROUP_BPW: Final = {1: 800, 10: 500, 100: 300, 1000: 100}
NEW_NAME_BPW: Final = {"Process": 4_000, "Group": 1_000}
FAIRNESS_BPW: Final = {
    ("Process", 1): 5_000,
    ("Process", 100): 500,
    ("Group", 1): 200,
    ("Group", 100): 200,
}

COMMON_ROW_FIELDS: Final = (
    "schema",
    "protocol",
    "protocol_sha256",
    "attempt_nonce",
    "track",
    "row_ordinal",
    "block",
    "cell_ordinal",
    "variant",
    "durability",
    "payload_size",
    "batch_size",
    "writers",
    "batches_per_writer",
    "store_id",
    "store_absent_before",
    "ready_monotonic_ns",
    "counter_start_monotonic_ns",
    "t0_monotonic_ns",
    "release_monotonic_ns",
    "last_completion_monotonic_ns",
    "t1_monotonic_ns",
    "counter_end_monotonic_ns",
    "wall_ns",
    "appends",
    "accepted_batches",
    "conflicts",
    "domain_events",
    "visible_events",
    "log_events",
    "control_events",
    "payload_bytes",
    "logical_digest",
    "latency_samples",
    "latency_p50_ns",
    "latency_p99_ns",
    "latency_max_ns",
    "allocation_calls",
    "allocated_bytes",
    "path_label",
    "owned_batches",
    "owned_records",
    "owned_payload_bytes",
    "borrowed_batches",
    "borrowed_records",
    "borrowed_payload_bytes",
    "defensive_copy_records",
    "defensive_copy_bytes",
    "process_user_cpu_ns",
    "process_system_cpu_ns",
    "serialized_role",
    "serialized_role_tid",
    "serialized_role_start_ticks",
    "serialized_role_cpu_ns",
    "serialized_role_voluntary_switches",
    "serialized_role_nonvoluntary_switches",
    "group_count",
    "group_batches",
    "group_events",
    "barrier_count",
    "fsync_count",
    "fsync_total_ns",
    "fsync_p50_ns",
    "fsync_p95_ns",
    "fsync_p99_ns",
    "fsync_max_ns",
    "durability_degraded",
    "write_like_calls",
    "sync_family_calls",
    "host_write_bytes",
)

CSV_FIELDS_BY_TRACK: Final = {
    "primary": COMMON_ROW_FIELDS,
    "new_names": COMMON_ROW_FIELDS
    + (
        "distinct_streams",
        "registry_events",
        "opaque_cursor_monotone",
    ),
    "fairness": COMMON_ROW_FIELDS
    + (
        "warm_rounds",
        "warm_writers",
        "warm_names_established",
        "counter_snapshot_after_warm",
        "fsync_histogram_includes_warm",
        "writer_samples_json",
        "jain_ppb",
        "min_to_median_rate_ppb",
        "max_to_median_p99_ppb",
        "waiter_reservations_after",
        "byte_reservations_after",
        "queue_depth",
        "queue_bytes",
        "group_width_distribution",
        "adaptive_group_width_target",
        "oldest_queued_age_ns",
    ),
    "reopen": (
        "schema",
        "protocol",
        "protocol_sha256",
        "attempt_nonce",
        "track",
        "row_ordinal",
        "latin_block",
        "ordinal_in_block",
        "variant",
        "archive_manifest_sha256",
        "copy_manifest_sha256",
        "copy_id",
        "copy_absent_before",
        "copy_verified_read_only",
        "syncfs_complete",
        "cache_state",
        "boot_monotonic_ns",
        "runtime_monotonic_ns",
        "ready_monotonic_ns",
        "start_sent_monotonic_ns",
        "open_start_monotonic_ns",
        "opened_monotonic_ns",
        "measured_monotonic_ns",
        "release_monotonic_ns",
        "wall_ns",
        "peak_rss_bytes",
        "proc_read_bytes",
        "proc_write_bytes",
        "proc_read_syscalls",
        "proc_write_syscalls",
        "recovery_payload_decodes",
        "domain_events",
        "visible_events",
        "log_events",
        "logical_digest",
        "registry_head_digest",
    ),
    "cpu_profiles": (
        "schema",
        "protocol",
        "protocol_sha256",
        "attempt_nonce",
        "track",
        "row_ordinal",
        "block",
        "cell_ordinal",
        "variant",
        "durability",
        "payload_size",
        "batch_size",
        "writers",
        "batches_per_writer",
        "appends",
        "domain_events",
        "profile_timing_discarded",
        "perf_permission",
        "perf_control_acknowledged",
        "process_user_cpu_ns",
        "process_system_cpu_ns",
        "process_voluntary_switches",
        "process_nonvoluntary_switches",
        "schedstat_resolution_ns",
        "role_samples_json",
        "cycles",
        "instructions",
        "task_clock_ns",
    ),
    "syscall_profiles": (
        "schema",
        "protocol",
        "protocol_sha256",
        "attempt_nonce",
        "track",
        "row_ordinal",
        "block",
        "cell_ordinal",
        "variant",
        "durability",
        "payload_size",
        "batch_size",
        "writers",
        "batches_per_writer",
        "appends",
        "domain_events",
        "profile_timing_discarded",
        "begin_markers",
        "end_markers",
        "write",
        "pwrite64",
        "writev",
        "pwritev",
        "pwritev2",
        "fsync",
        "fdatasync",
        "futex",
        "file_create",
        "file_rename",
        "file_unlink",
    ),
    "structural_traces": (
        "schema",
        "protocol",
        "protocol_sha256",
        "attempt_nonce",
        "track",
        "row_ordinal",
        "trace_kind",
        "variant",
        "durability",
        "writers",
        "appends_per_writer",
        "group_count",
        "barrier_count",
        "profile_timing_discarded",
        "begin_markers",
        "end_markers",
        "write_like_calls",
        "sync_family_calls",
        "log_sync_calls",
        "metadata_sync_calls",
        "openat",
        "getdents64",
        "read",
        "pread64",
        "files_opened",
    ),
}

# Child stdout is intentionally smaller than the persisted row.  It remains
# authoritative for workload/product observations.  The reviewed runner adds
# ordering/control-handshake fields, and the separately reviewed profile
# adapter adds external counters.  Each input is retained and hash-bound in the
# child manifest; overlap is forbidden.
RAW_POINT_FIELDS_BY_TRACK: Final = {
    "primary": (
        "schema",
        "protocol",
        "protocol_sha256",
        "track",
        "variant",
        "durability",
        "payload_size",
        "batch_size",
        "writers",
        "batches_per_writer",
        "wall_ns",
        "appends",
        "accepted_batches",
        "conflicts",
        "domain_events",
        "visible_events",
        "log_events",
        "control_events",
        "payload_bytes",
        "logical_digest",
        "latency_samples",
        "latency_p50_ns",
        "latency_p99_ns",
        "latency_max_ns",
        "allocation_calls",
        "allocated_bytes",
        "path_label",
        "owned_batches",
        "owned_records",
        "owned_payload_bytes",
        "borrowed_batches",
        "borrowed_records",
        "borrowed_payload_bytes",
        "defensive_copy_records",
        "defensive_copy_bytes",
        "group_count",
        "group_batches",
        "group_events",
        "barrier_count",
        "fsync_count",
        "fsync_total_ns",
        "fsync_p50_ns",
        "fsync_p95_ns",
        "fsync_p99_ns",
        "fsync_max_ns",
        "durability_degraded",
        "write_like_calls",
        "sync_family_calls",
        "host_write_bytes",
    ),
}
RAW_POINT_FIELDS_BY_TRACK["new_names"] = RAW_POINT_FIELDS_BY_TRACK["primary"] + (
    "distinct_streams",
    "registry_events",
    "opaque_cursor_monotone",
)
RAW_POINT_FIELDS_BY_TRACK["fairness"] = RAW_POINT_FIELDS_BY_TRACK["primary"] + (
    "warm_rounds",
    "warm_writers",
    "warm_names_established",
    "counter_snapshot_after_warm",
    "fsync_histogram_includes_warm",
    "writer_samples_json",
    "jain_ppb",
    "min_to_median_rate_ppb",
    "max_to_median_p99_ppb",
    "waiter_reservations_after",
    "byte_reservations_after",
    "queue_depth",
    "queue_bytes",
    "group_width_distribution",
    "adaptive_group_width_target",
    "oldest_queued_age_ns",
)
RAW_POINT_FIELDS_BY_TRACK["reopen"] = (
    "schema",
    "protocol",
    "protocol_sha256",
    "track",
    "variant",
    "wall_ns",
    "recovery_payload_decodes",
    "domain_events",
    "visible_events",
    "log_events",
    "logical_digest",
    "registry_head_digest",
)
RAW_POINT_FIELDS_BY_TRACK["cpu_profiles"] = (
    "schema",
    "protocol",
    "protocol_sha256",
    "track",
    "variant",
    "durability",
    "payload_size",
    "batch_size",
    "writers",
    "batches_per_writer",
    "appends",
    "domain_events",
)
RAW_POINT_FIELDS_BY_TRACK["syscall_profiles"] = RAW_POINT_FIELDS_BY_TRACK[
    "cpu_profiles"
]
RAW_POINT_FIELDS_BY_TRACK["structural_traces"] = (
    "schema",
    "protocol",
    "protocol_sha256",
    "track",
    "trace_kind",
    "variant",
    "durability",
    "writers",
    "appends_per_writer",
    "group_count",
    "barrier_count",
)

RUNNER_CONTEXT_FIELDS_BY_TRACK: Final = {
    "primary": (
        "schema",
        "attempt_nonce",
        "row_ordinal",
        "block",
        "cell_ordinal",
        "store_id",
        "store_absent_before",
        "ready_monotonic_ns",
        "counter_start_monotonic_ns",
        "t0_monotonic_ns",
        "release_monotonic_ns",
        "last_completion_monotonic_ns",
        "t1_monotonic_ns",
        "counter_end_monotonic_ns",
    ),
    "new_names": (),
    "fairness": (),
    "reopen": (
        "schema",
        "attempt_nonce",
        "row_ordinal",
        "latin_block",
        "ordinal_in_block",
        "archive_manifest_sha256",
        "copy_manifest_sha256",
        "copy_id",
        "copy_absent_before",
        "copy_verified_read_only",
        "syncfs_complete",
        "cache_state",
        "boot_monotonic_ns",
        "runtime_monotonic_ns",
        "ready_monotonic_ns",
        "start_sent_monotonic_ns",
        "open_start_monotonic_ns",
        "opened_monotonic_ns",
        "measured_monotonic_ns",
        "release_monotonic_ns",
    ),
    "cpu_profiles": (
        "schema",
        "attempt_nonce",
        "row_ordinal",
        "block",
        "cell_ordinal",
        "profile_timing_discarded",
    ),
    "syscall_profiles": (
        "schema",
        "attempt_nonce",
        "row_ordinal",
        "block",
        "cell_ordinal",
        "profile_timing_discarded",
    ),
    "structural_traces": (
        "schema",
        "attempt_nonce",
        "row_ordinal",
        "profile_timing_discarded",
    ),
}
RUNNER_CONTEXT_FIELDS_BY_TRACK["new_names"] = RUNNER_CONTEXT_FIELDS_BY_TRACK[
    "primary"
]
RUNNER_CONTEXT_FIELDS_BY_TRACK["fairness"] = RUNNER_CONTEXT_FIELDS_BY_TRACK[
    "primary"
]

PROFILE_FIELDS_BY_TRACK: Final = {
    "primary": (
        "process_user_cpu_ns",
        "process_system_cpu_ns",
        "serialized_role",
        "serialized_role_tid",
        "serialized_role_start_ticks",
        "serialized_role_cpu_ns",
        "serialized_role_voluntary_switches",
        "serialized_role_nonvoluntary_switches",
    ),
    "new_names": (),
    "fairness": (),
    "reopen": (
        "peak_rss_bytes",
        "proc_read_bytes",
        "proc_write_bytes",
        "proc_read_syscalls",
        "proc_write_syscalls",
    ),
    "cpu_profiles": (
        "perf_permission",
        "perf_control_acknowledged",
        "process_user_cpu_ns",
        "process_system_cpu_ns",
        "process_voluntary_switches",
        "process_nonvoluntary_switches",
        "schedstat_resolution_ns",
        "role_samples_json",
        "cycles",
        "instructions",
        "task_clock_ns",
    ),
    "syscall_profiles": (
        "begin_markers",
        "end_markers",
        "write",
        "pwrite64",
        "writev",
        "pwritev",
        "pwritev2",
        "fsync",
        "fdatasync",
        "futex",
        "file_create",
        "file_rename",
        "file_unlink",
    ),
    "structural_traces": (
        "begin_markers",
        "end_markers",
        "write_like_calls",
        "sync_family_calls",
        "log_sync_calls",
        "metadata_sync_calls",
        "openat",
        "getdents64",
        "read",
        "pread64",
        "files_opened",
    ),
}
PROFILE_FIELDS_BY_TRACK["new_names"] = PROFILE_FIELDS_BY_TRACK["primary"]
PROFILE_FIELDS_BY_TRACK["fairness"] = PROFILE_FIELDS_BY_TRACK["primary"]


_BOOL_FIELDS: Final = frozenset(
    {
        "store_absent_before",
        "durability_degraded",
        "opaque_cursor_monotone",
        "warm_names_established",
        "counter_snapshot_after_warm",
        "fsync_histogram_includes_warm",
        "copy_absent_before",
        "copy_verified_read_only",
        "syncfs_complete",
        "profile_timing_discarded",
        "perf_control_acknowledged",
    }
)
_JSON_FIELDS: Final = frozenset({"writer_samples_json", "role_samples_json"})
_TEXT_FIELDS: Final = frozenset(
    {
        "schema",
        "protocol",
        "protocol_sha256",
        "attempt_nonce",
        "track",
        "variant",
        "durability",
        "store_id",
        "logical_digest",
        "path_label",
        "serialized_role",
        "archive_manifest_sha256",
        "copy_manifest_sha256",
        "copy_id",
        "cache_state",
        "registry_head_digest",
        "perf_permission",
        "trace_kind",
        "queue_depth",
        "queue_bytes",
        "group_width_distribution",
        "adaptive_group_width_target",
        "oldest_queued_age_ns",
    }
)
_OPTIONAL_INTEGER_FIELDS: Final = frozenset(
    {
        "host_write_bytes",
        "write_like_calls",
        "sync_family_calls",
        "defensive_copy_records",
        "defensive_copy_bytes",
        "waiter_reservations_after",
        "byte_reservations_after",
        "cycles",
        "instructions",
        "task_clock_ns",
        "openat",
        "getdents64",
        "read",
        "pread64",
        "files_opened",
    }
)


def canonical_json_bytes(value: Any) -> bytes:
    """Return the one accepted JSON representation, including one LF."""

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


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _is_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _require_nonnegative_integer(record: Mapping[str, Any], field: str) -> None:
    value = record[field]
    if field in _OPTIONAL_INTEGER_FIELDS and value == NOT_AVAILABLE:
        return
    if not _is_integer(value) or value < 0:
        raise ValueError(f"{field} must be a nonnegative integer")


def _validate_writer_samples(value: Any, record: Mapping[str, Any]) -> None:
    if not isinstance(value, list) or len(value) != 64:
        raise ValueError("writer_samples_json must contain exactly 64 writers")
    expected_keys = {
        "writer",
        "completed_appends",
        "completed_events",
        "elapsed_ns",
        "p50_ns",
        "p99_ns",
        "max_ns",
    }
    seen: set[int] = set()
    for sample in value:
        if not isinstance(sample, dict) or set(sample) != expected_keys:
            raise ValueError("writer sample keys are not exact")
        for field in expected_keys:
            if not _is_integer(sample[field]) or sample[field] < 0:
                raise ValueError(f"writer sample {field} must be nonnegative integer")
        writer = sample["writer"]
        if writer >= 64 or writer in seen:
            raise ValueError("writer sample identity is duplicate or out of range")
        seen.add(writer)
        if sample["completed_appends"] != record["batches_per_writer"]:
            raise ValueError("writer completed appends mismatch")
        if sample["completed_events"] != (
            record["batches_per_writer"] * record["batch_size"]
        ):
            raise ValueError("writer completed events mismatch")
        if sample["elapsed_ns"] <= 0:
            raise ValueError("writer elapsed_ns must be positive")
        if not sample["p50_ns"] <= sample["p99_ns"] <= sample["max_ns"]:
            raise ValueError("writer latency order is invalid")


def _validate_role_samples(value: Any) -> None:
    if not isinstance(value, list) or not value:
        raise ValueError("role_samples_json must be a nonempty list")
    keys = {
        "role",
        "tid",
        "start_ticks",
        "cpu_ns",
        "voluntary_switches",
        "nonvoluntary_switches",
    }
    seen: set[tuple[str, int, int]] = set()
    for sample in value:
        if not isinstance(sample, dict) or set(sample) != keys:
            raise ValueError("role sample keys are not exact")
        if not isinstance(sample["role"], str) or not sample["role"]:
            raise ValueError("role sample role must be nonempty")
        for field in keys - {"role"}:
            if not _is_integer(sample[field]) or sample[field] < 0:
                raise ValueError(f"role sample {field} must be nonnegative integer")
        identity = (sample["role"], sample["tid"], sample["start_ticks"])
        if identity in seen:
            raise ValueError("role sample identity is duplicated")
        seen.add(identity)


def _validate_common_invariants(record: Mapping[str, Any]) -> None:
    expected_appends = record["writers"] * record["batches_per_writer"]
    expected_events = expected_appends * record["batch_size"]
    if record["appends"] != expected_appends:
        raise ValueError("appends != writers * batches_per_writer")
    if record["accepted_batches"] != record["appends"]:
        raise ValueError("accepted_batches != appends")
    if record["conflicts"] != 0:
        raise ValueError("timed row contains a conflict")
    if record["domain_events"] != expected_events:
        raise ValueError("domain_events != appends * batch_size")
    if record["visible_events"] != record["domain_events"]:
        raise ValueError("visible_events != domain_events")
    if record["log_events"] != record["domain_events"] + record["control_events"]:
        raise ValueError("log_events != domain_events + control_events")
    if record["payload_bytes"] != expected_events * record["payload_size"]:
        raise ValueError("payload_bytes mismatch")
    if record["wall_ns"] <= 0:
        raise ValueError("wall_ns must be positive")
    if record["wall_ns"] != record["t1_monotonic_ns"] - record["t0_monotonic_ns"]:
        raise ValueError("wall_ns does not match t0/t1")
    phases = [
        record["ready_monotonic_ns"],
        record["counter_start_monotonic_ns"],
        record["t0_monotonic_ns"],
        record["release_monotonic_ns"],
        record["last_completion_monotonic_ns"],
        record["t1_monotonic_ns"],
        record["counter_end_monotonic_ns"],
    ]
    if phases != sorted(phases):
        raise ValueError("measurement phase markers are not monotone")
    if not record["store_absent_before"]:
        raise ValueError("store was not absent before creation")
    expected_samples = record["writers"] * (
        record["batches_per_writer"]
        if record.get("track") == "fairness"
        else record["batches_per_writer"] - record["batches_per_writer"] // 10
    )
    if record["latency_samples"] != expected_samples:
        raise ValueError("latency sample count does not match per-writer 10% drop")
    if not (
        record["latency_p50_ns"]
        <= record["latency_p99_ns"]
        <= record["latency_max_ns"]
    ):
        raise ValueError("row latency order is invalid")
    if record["visible_events"] > record["log_events"]:
        raise ValueError("visible events exceed log events")
    if record["control_events"] > record["log_events"]:
        raise ValueError("control events exceed log events")
    durability = record["durability"]
    if durability == "Process":
        if record["barrier_count"] != 0 or record["group_count"] != 0:
            raise ValueError("Process row contains groups or barriers")
        if record["fsync_count"] != 0:
            raise ValueError("Process row contains fsync samples")
        if record["variant"] == "A":
            expected = (
                record["appends"],
                record["domain_events"],
                record["payload_bytes"],
            )
            actual = (
                record["owned_batches"],
                record["owned_records"],
                record["owned_payload_bytes"],
            )
            if actual != expected or any(
                record[name] != 0
                for name in (
                    "borrowed_batches",
                    "borrowed_records",
                    "borrowed_payload_bytes",
                    "defensive_copy_records",
                    "defensive_copy_bytes",
                )
            ):
                raise ValueError("A Process owned-path counters are not exact")
            if record["path_label"] != "owned":
                raise ValueError("A Process did not select owned path")
        elif record["variant"] in {"C", "D"} and record["path_label"] != "borrowed":
            raise ValueError("historical public Process path is not borrowed")
    elif durability == "Group":
        if record["group_count"] <= 0:
            raise ValueError("Group row contains no commit group")
        if record["barrier_count"] != record["group_count"]:
            raise ValueError("Group does not have one covering barrier per group")
        if record.get("track") == "fairness":
            if record["fsync_count"] < record["barrier_count"]:
                raise ValueError("fairness Group fsync histogram misses measured barriers")
        elif record["fsync_count"] != record["barrier_count"]:
            raise ValueError("Group fsync samples do not cover barriers")
        if record["group_batches"] != record["appends"]:
            raise ValueError("Group batches do not cover all appends")
        if record["group_events"] != record["domain_events"]:
            raise ValueError("Group events do not cover all domain events")
        if record["variant"] == "A":
            if record["path_label"] != "borrowed-compatible":
                raise ValueError("A Group did not select borrowed-compatible path")
            if any(
                record[name] != 0
                for name in ("owned_batches", "owned_records", "owned_payload_bytes")
            ):
                raise ValueError("A Group consumed owned inputs")
            if (
                record["borrowed_batches"] != record["appends"]
                or record["borrowed_records"] != record["domain_events"]
                or record["borrowed_payload_bytes"] != record["payload_bytes"]
            ):
                raise ValueError("A Group borrowed counters are not exact")
        elif record["variant"] in {"C", "D"} and record["path_label"] != "borrowed":
            raise ValueError("historical public Group path is not borrowed")
    if record["variant"] == "B" and record["path_label"] != "raw-numeric":
        raise ValueError("bare row path is not raw-numeric")


def validate_child_record(track: str, record: Mapping[str, Any], context: str = "row") -> None:
    """Validate one native JSON record, raising ``ValueError`` on mismatch."""

    if track not in CSV_FIELDS_BY_TRACK:
        raise ValueError(f"{context}: unknown track {track!r}")
    expected_fields = set(CSV_FIELDS_BY_TRACK[track])
    if set(record) != expected_fields:
        missing = sorted(expected_fields - set(record))
        extra = sorted(set(record) - expected_fields)
        raise ValueError(f"{context}: fields are not exact; missing={missing}, extra={extra}")
    for field in CSV_FIELDS_BY_TRACK[track]:
        value = record[field]
        if field in _BOOL_FIELDS:
            if not isinstance(value, bool):
                raise ValueError(f"{context}: {field} must be boolean")
        elif field in _JSON_FIELDS:
            if field == "writer_samples_json":
                _validate_writer_samples(value, record)
            else:
                _validate_role_samples(value)
        elif field in _TEXT_FIELDS:
            if not isinstance(value, str) or not value:
                raise ValueError(f"{context}: {field} must be nonempty text")
        else:
            _require_nonnegative_integer(record, field)

    if record["schema"] != ROW_SCHEMAS[track]:
        raise ValueError(f"{context}: row schema mismatch")
    if record["protocol"] != PROTOCOL:
        raise ValueError(f"{context}: protocol mismatch")
    if record["track"] != track:
        raise ValueError(f"{context}: track field mismatch")
    if record["variant"] not in VARIANTS:
        raise ValueError(f"{context}: unknown variant")

    if track in {"primary", "new_names", "fairness"}:
        if record["durability"] not in DURABILITIES:
            raise ValueError(f"{context}: invalid durability")
        _validate_common_invariants(record)
    if track == "primary":
        bpw = (PROCESS_BPW if record["durability"] == "Process" else GROUP_BPW)[
            record["batch_size"]
        ]
        if record["batches_per_writer"] != bpw:
            raise ValueError(f"{context}: primary work count mismatch")
    elif track == "new_names":
        if record["payload_size"] != 250 or record["batch_size"] != 1:
            raise ValueError(f"{context}: new-name shape mismatch")
        if record["writers"] not in {1, 4}:
            raise ValueError(f"{context}: new-name writer count mismatch")
        if record["batches_per_writer"] != NEW_NAME_BPW[record["durability"]]:
            raise ValueError(f"{context}: new-name work count mismatch")
        if record["distinct_streams"] != record["appends"]:
            raise ValueError(f"{context}: new-name stream count mismatch")
        if record["variant"] in PUBLIC_VARIANTS and not record["opaque_cursor_monotone"]:
            raise ValueError(f"{context}: public cursor is not monotone")
    elif track == "fairness":
        if record["payload_size"] != 250 or record["writers"] != 64:
            raise ValueError(f"{context}: fairness shape mismatch")
        expected = FAIRNESS_BPW[(record["durability"], record["batch_size"])]
        if record["batches_per_writer"] != expected:
            raise ValueError(f"{context}: fairness work count mismatch")
        if (
            record["warm_rounds"] != 4
            or record["warm_writers"] != 64
            or not record["warm_names_established"]
            or not record["counter_snapshot_after_warm"]
            or not record["fsync_histogram_includes_warm"]
        ):
            raise ValueError(f"{context}: fairness warm policy mismatch")
        if record["variant"] == "A":
            # Exact zero is an outcome gate, not a structural parsing rule.
            # A nonzero, nonnegative observation is valid evidence of a
            # cancellation/boundedness regression and must reach REVERT.
            for field in ("waiter_reservations_after", "byte_reservations_after"):
                if not _is_integer(record[field]) or record[field] < 0:
                    raise ValueError(f"{context}: A {field} must be a nonnegative integer")
        elif (
            record["waiter_reservations_after"] != NOT_AVAILABLE
            or record["byte_reservations_after"] != NOT_AVAILABLE
        ):
            raise ValueError(f"{context}: non-A fairness reservation fields must be not_available")
        for field in (
            "queue_depth",
            "queue_bytes",
            "group_width_distribution",
            "adaptive_group_width_target",
            "oldest_queued_age_ns",
        ):
            if record[field] != NOT_AVAILABLE:
                raise ValueError(
                    f"{context}: fairness diagnostic {field} must be literal not_available"
                )
        samples = record["writer_samples_json"]
        # Rust and Python replay the exact same dependency-free u128
        # fixed-point algorithm. Rates are events per nanosecond scaled by
        # 1e18 and floored once; every threshold summary thereafter is integer
        # arithmetic. This avoids float drift and unbounded rational products.
        rates = [
            sample["completed_events"] * 1_000_000_000_000_000_000
            // sample["elapsed_ns"]
            for sample in samples
        ]
        total = sum(rates)
        square_total = sum(value * value for value in rates)
        ordered_rates = sorted(rates)
        median_rate_twice = ordered_rates[31] + ordered_rates[32]
        p99s = sorted(sample["p99_ns"] for sample in samples)
        median_p99_twice = p99s[31] + p99s[32]
        expected_summaries = {
            "jain_ppb": total * total * 1_000_000_000 // (64 * square_total),
            "min_to_median_rate_ppb": ordered_rates[0] * 2 * 1_000_000_000
            // median_rate_twice,
            "max_to_median_p99_ppb": p99s[-1] * 2 * 1_000_000_000
            // median_p99_twice,
        }
        for field, expected_value in expected_summaries.items():
            if record[field] != expected_value:
                raise ValueError(f"{context}: {field} does not match raw writer samples")
    elif track == "reopen":
        if record["variant"] not in PUBLIC_VARIANTS:
            raise ValueError(f"{context}: reopen variant is not public")
        if record["cache_state"] != "warm-from-materialization":
            raise ValueError(f"{context}: reopen cache state mismatch")
        if not all(
            record[field]
            for field in ("copy_absent_before", "copy_verified_read_only", "syncfs_complete")
        ):
            raise ValueError(f"{context}: reopen copy preconditions failed")
        if record["archive_manifest_sha256"] != record["copy_manifest_sha256"]:
            raise ValueError(f"{context}: reopen copy differs from archive")
        phases = [
            record["boot_monotonic_ns"],
            record["runtime_monotonic_ns"],
            record["ready_monotonic_ns"],
            record["start_sent_monotonic_ns"],
            record["open_start_monotonic_ns"],
            record["opened_monotonic_ns"],
            record["measured_monotonic_ns"],
            record["release_monotonic_ns"],
        ]
        if phases != sorted(phases):
            raise ValueError(f"{context}: reopen handshake is not monotone")
        if record["wall_ns"] != record["opened_monotonic_ns"] - record["open_start_monotonic_ns"]:
            raise ValueError(f"{context}: reopen wall mismatch")
        if record["domain_events"] != 2_000_000:
            raise ValueError(f"{context}: reopen corpus event count mismatch")
    elif track in {"cpu_profiles", "syscall_profiles"}:
        if not record["profile_timing_discarded"]:
            raise ValueError(f"{context}: profiler timing was not discarded")
        expected_appends = record["writers"] * record["batches_per_writer"]
        if record["appends"] != expected_appends:
            raise ValueError(f"{context}: profile append count mismatch")
        if record["domain_events"] != expected_appends * record["batch_size"]:
            raise ValueError(f"{context}: profile event count mismatch")
        if track == "cpu_profiles" and record["schedstat_resolution_ns"] <= 0:
            raise ValueError(f"{context}: invalid schedstat resolution")
        if track == "syscall_profiles" and (
            record["begin_markers"] != 1 or record["end_markers"] != 1
        ):
            raise ValueError(f"{context}: syscall markers are not exact")
    elif track == "structural_traces":
        if not record["profile_timing_discarded"]:
            raise ValueError(f"{context}: structural trace timing was not discarded")
        if record["variant"] not in PUBLIC_VARIANTS:
            raise ValueError(f"{context}: structural trace variant is not public")
        if record["begin_markers"] != 1 or record["end_markers"] != 1:
            raise ValueError(f"{context}: structural markers are not exact")
        if record["trace_kind"] not in {"new_names", "reopen"}:
            raise ValueError(f"{context}: invalid structural trace kind")
        if record["durability"] == "Process":
            if record["group_count"] != 0 or record["barrier_count"] != 0:
                raise ValueError(f"{context}: Process structural trace reported groups/barriers")
        elif record["trace_kind"] == "new_names":
            if record["group_count"] <= 0 or record["barrier_count"] != record["group_count"]:
                raise ValueError(f"{context}: Group structural counts are not one covering barrier per group")
        elif record["group_count"] != 0 or record["barrier_count"] != 0:
            raise ValueError(f"{context}: reopen structural trace reported append groups/barriers")


def validate_child_records(
    kind: str,
    records: Sequence[Mapping[str, Any]],
    context: str,
    runner_context: Mapping[str, Any] | None = None,
    profile_result: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate an exact child record batch.

    Timed/profile children emit one row.  This sequence API also gives the
    runner a fail-closed boundary if a future helper accidentally emits zero or
    multiple JSON objects.
    """

    if len(records) != 1:
        raise ValueError(f"{context}: child emitted {len(records)} records, expected 1")
    point = records[0]
    expected_point = set(RAW_POINT_FIELDS_BY_TRACK[kind])
    if set(point) != expected_point:
        raise ValueError(
            f"{context}: raw point fields are not exact; "
            f"missing={sorted(expected_point - set(point))}, "
            f"extra={sorted(set(point) - expected_point)}"
        )
    if point.get("schema") != RAW_POINT_SCHEMA:
        raise ValueError(f"{context}: raw point schema mismatch")
    if point.get("protocol") != PROTOCOL or point.get("track") != kind:
        raise ValueError(f"{context}: raw point protocol/track mismatch")

    context_fields = set(RUNNER_CONTEXT_FIELDS_BY_TRACK[kind])
    profile_fields = set(PROFILE_FIELDS_BY_TRACK[kind])
    supplied_context = {} if runner_context is None else dict(runner_context)
    supplied_profile = {} if profile_result is None else dict(profile_result)
    if set(supplied_context) != context_fields:
        raise ValueError(
            f"{context}: runner context fields are not exact; "
            f"missing={sorted(context_fields - set(supplied_context))}, "
            f"extra={sorted(set(supplied_context) - context_fields)}"
        )
    if set(supplied_profile) != profile_fields:
        raise ValueError(
            f"{context}: profile fields are not exact; "
            f"missing={sorted(profile_fields - set(supplied_profile))}, "
            f"extra={sorted(set(supplied_profile) - profile_fields)}"
        )

    row = dict(point)
    row.pop("schema")
    row.update(supplied_context)
    overlap = set(row) & set(supplied_profile)
    if overlap:
        raise ValueError(f"{context}: profile fields overlap child/context: {sorted(overlap)}")
    row.update(supplied_profile)
    validate_child_record(kind, row, context)
    return row


def _csv_value(field: str, value: Any) -> str:
    if field in _BOOL_FIELDS:
        return "true" if value else "false"
    if field in _JSON_FIELDS:
        return canonical_json_bytes(value).decode("ascii").removesuffix("\n")
    return str(value)


def encode_csv_row(track: str, row: Mapping[str, Any], write_header: bool = False) -> str:
    """Validate and encode one canonical RFC-4180-compatible CSV record."""

    validate_child_record(track, row, f"{track} child row")
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    fields = CSV_FIELDS_BY_TRACK[track]
    if write_header:
        writer.writerow(fields)
    writer.writerow([_csv_value(field, row[field]) for field in fields])
    return buffer.getvalue()


def parse_csv_row(track: str, row: Mapping[str, str], context: str) -> dict[str, Any]:
    """Parse one persisted CSV record back into its native canonical object."""

    expected = set(CSV_FIELDS_BY_TRACK[track])
    if set(row) != expected:
        raise ValueError(f"{context}: CSV fields are not exact")
    parsed: dict[str, Any] = {}
    for field in CSV_FIELDS_BY_TRACK[track]:
        value = row[field]
        if field in _BOOL_FIELDS:
            if value not in {"true", "false"}:
                raise ValueError(f"{context}: {field} is not canonical boolean")
            parsed[field] = value == "true"
        elif field in _JSON_FIELDS:
            try:
                item = json.loads(value)
            except json.JSONDecodeError as error:
                raise ValueError(f"{context}: {field} is invalid JSON: {error}") from error
            canonical = canonical_json_bytes(item).decode("ascii").removesuffix("\n")
            if value != canonical:
                raise ValueError(f"{context}: {field} is not canonical JSON")
            parsed[field] = item
        elif field in _TEXT_FIELDS:
            if not value:
                raise ValueError(f"{context}: {field} is empty")
            parsed[field] = value
        elif field in _OPTIONAL_INTEGER_FIELDS and value == NOT_AVAILABLE:
            parsed[field] = value
        else:
            try:
                integer = int(value, 10)
            except ValueError as error:
                raise ValueError(f"{context}: {field} is not an integer") from error
            if value != str(integer):
                raise ValueError(f"{context}: {field} is not canonical integer text")
            parsed[field] = integer
    validate_child_record(track, parsed, context)
    return parsed


def _cell_key(cell: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(cell[key] for key in sorted(cell))


def canonical_cells(track: str) -> list[dict[str, Any]]:
    if track == "primary":
        return [
            {
                "durability": durability,
                "payload_size": payload,
                "batch_size": batch,
                "writers": writers,
            }
            for durability in DURABILITIES
            for payload in (24, 250)
            for batch in (1, 10, 100, 1000)
            for writers in (1, 4)
        ]
    if track == "new_names":
        return [
            {"durability": durability, "payload_size": 250, "batch_size": 1, "writers": writers}
            for durability in DURABILITIES
            for writers in (1, 4)
        ]
    if track == "fairness":
        return [
            {"durability": durability, "payload_size": 250, "batch_size": batch, "writers": 64}
            for durability in DURABILITIES
            for batch in (1, 100)
        ]
    if track in {"cpu_profiles", "syscall_profiles"}:
        return [
            {"durability": durability, "payload_size": 250, "batch_size": batch, "writers": 4}
            for durability in DURABILITIES
            for batch in (1, 1000)
        ]
    raise ValueError(f"track {track!r} has no Williams cells")


def validate_cell_orders(config: Mapping[str, Any]) -> None:
    orders = config.get("cell_orders")
    if not isinstance(orders, dict) or set(orders) != {
        "primary",
        "new_names",
        "fairness",
        "cpu_profiles",
        "syscall_profiles",
    }:
        raise ValueError("config cell_orders keys are not exact")
    for track, cells in orders.items():
        if not isinstance(cells, list) or not all(isinstance(cell, dict) for cell in cells):
            raise ValueError(f"config {track} cell order is not a list of objects")
        canonical = canonical_cells(track)
        if len(cells) != len(canonical):
            raise ValueError(f"config {track} cell order cardinality mismatch")
        if {_cell_key(cell) for cell in cells} != {_cell_key(cell) for cell in canonical}:
            raise ValueError(f"config {track} cell order is not the exact cell set")


def expected_order(config: Mapping[str, Any], track: str) -> list[dict[str, Any]]:
    """Return exact physical row identities for a CSV track."""

    if track in config.get("cell_orders", {}):
        validate_cell_orders(config)
        cells = config["cell_orders"][track]
        result: list[dict[str, Any]] = []
        row_ordinal = 0
        for block in range(1, 5):
            physical_cells = list(enumerate(cells, start=1))
            if block in {2, 4}:
                physical_cells.reverse()
            for cell_ordinal, cell in physical_cells:
                for variant in WILLIAMS[block]:
                    row_ordinal += 1
                    result.append(
                        {
                            "row_ordinal": row_ordinal,
                            "block": block,
                            "cell_ordinal": cell_ordinal,
                            "variant": variant,
                            **cell,
                        }
                    )
        return result
    if track == "reopen":
        result = []
        orders = (("A", "C", "D"), ("C", "D", "A"), ("D", "A", "C"))
        for latin_block, variants in enumerate(orders, start=1):
            for ordinal, variant in enumerate(variants, start=1):
                result.append(
                    {
                        "row_ordinal": len(result) + 1,
                        "latin_block": latin_block,
                        "ordinal_in_block": ordinal,
                        "variant": variant,
                    }
                )
        return result
    if track == "structural_traces":
        result = []
        for variant in PUBLIC_VARIANTS:
            for durability in DURABILITIES:
                for writers in (1, 4):
                    result.append(
                        {
                            "row_ordinal": len(result) + 1,
                            "trace_kind": "new_names",
                            "variant": variant,
                            "durability": durability,
                            "writers": writers,
                            "appends_per_writer": 8,
                        }
                    )
        for variant in PUBLIC_VARIANTS:
            result.append(
                {
                    "row_ordinal": len(result) + 1,
                    "trace_kind": "reopen",
                    "variant": variant,
                    "durability": "Process",
                    "writers": 1,
                    "appends_per_writer": 0,
                }
            )
        return result
    raise ValueError(f"unknown track {track!r}")


def exact_binary_contract_fields() -> tuple[str, ...]:
    return (
        "schema",
        "protocol",
        "protocol_sha256",
        "tooling_commit",
        "tooling_tree",
        "variant",
        "product_commit",
        "product_tree",
        "adapter_sha256",
        "shared_manifest_sha256",
        "cargo_lock_sha256",
        "source_approval_sha256",
        "build_nonce",
        "binary_kind",
        "timed_surface",
        "correctness_oracle_mode",
        "profile_role_lifetime",
        "contract_mode",
        "rows_written",
    )


def validate_binary_contract(contract: Mapping[str, Any]) -> None:
    if tuple(contract) != exact_binary_contract_fields() and set(contract) != set(
        exact_binary_contract_fields()
    ):
        raise ValueError("binary contract fields are not exact")
    if contract["schema"] != BINARY_CONTRACT_SCHEMA or contract["protocol"] != PROTOCOL:
        raise ValueError("binary contract schema/protocol mismatch")
    variant = contract["variant"]
    if variant not in VARIANTS:
        raise ValueError("binary contract variant mismatch")
    expected_kind = "bare" if variant == "B" else "public"
    expected_surface = "raw-numeric" if variant == "B" else "public-event-store"
    if contract["binary_kind"] != expected_kind or contract["timed_surface"] != expected_surface:
        raise ValueError("binary contract surface mismatch")
    if contract["correctness_oracle_mode"] is not (variant != "B"):
        raise ValueError("binary contract correctness oracle capability mismatch")
    expected_role_lifetime: Any = (
        PROFILE_C_ROLE_LIFETIME_CONTRACT if variant == "C" else "not_applicable"
    )
    if contract["profile_role_lifetime"] != expected_role_lifetime:
        raise ValueError("binary contract profile role lifetime differs")
    if contract["contract_mode"] is not True or contract["rows_written"] != 0:
        raise ValueError("binary contract mode wrote rows")
    for field in (
        "protocol_sha256",
        "adapter_sha256",
        "shared_manifest_sha256",
        "cargo_lock_sha256",
        "source_approval_sha256",
        "build_nonce",
    ):
        value = contract[field]
        if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError(f"binary contract {field} is not SHA-256")
    for field in ("tooling_commit", "tooling_tree", "product_commit", "product_tree"):
        value = contract[field]
        if not isinstance(value, str) or len(value) != 40 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError(f"binary contract {field} is not a Git object id")


def parse_canonical_json_object(data: bytes, context: str) -> dict[str, Any]:
    try:
        value = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{context}: invalid JSON: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"{context}: expected JSON object")
    if canonical_json_bytes(value) != data:
        raise ValueError(f"{context}: JSON bytes are not canonical")
    return value


def ensure_exact_sequence(actual: Iterable[Mapping[str, Any]], expected: Sequence[Mapping[str, Any]]) -> None:
    rows = list(actual)
    if len(rows) != len(expected):
        raise ValueError(f"row count {len(rows)} != {len(expected)}")
    for ordinal, (row, identity) in enumerate(zip(rows, expected, strict=True), start=1):
        for field, value in identity.items():
            if row.get(field) != value:
                raise ValueError(
                    f"row {ordinal} identity {field}={row.get(field)!r}, expected {value!r}"
                )
