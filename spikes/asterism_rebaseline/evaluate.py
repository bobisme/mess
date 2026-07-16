#!/usr/bin/env python3
"""Independent fail-closed evaluator for the bn-2l3n rebaseline.

Admission mode is intended to be launched exactly once by the reviewed runner
while it still holds the host-wide lease.  The only other executable mode is a
synthetic self-test.  Historical rows are never read and there is no CLI mode
that can turn a hand-selected CSV into a verdict.
"""

from __future__ import annotations

import copy
import ctypes
import csv
import errno
import fcntl
import hashlib
import io
import json
import os
import shutil
import socket
import stat
import sys
import tempfile
import types
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from fractions import Fraction
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import evidence_schema as schema


EXIT_ADMIT = 0
EXIT_NARROW = 10
EXIT_REVERT = 11
EXIT_INCONCLUSIVE = 20
EXIT_USAGE = 2
EXIT_INTERNAL = 30

SHA256 = frozenset("0123456789abcdef")
RESULT_NAME = "result.json"
TERMINAL_NAMES = {
    "terminal.json",
    "terminal-pre-release.json",
    "lease-release.json",
    "terminal-verification.json",
}
CORPUS_EXECUTION_AUTHORITY_SCHEMA = (
    "bn-2l3n-runner-corpus-execution-authority-v3"
)
CORPUS_EXECUTION_AUTHORITY_FD_ENVIRONMENT = (
    "ASTERISM_REBASELINE_CORPUS_EXECUTION_AUTHORITY_FD"
)
F_ADD_SEALS = getattr(fcntl, "F_ADD_SEALS", 1033)
F_GET_SEALS = getattr(fcntl, "F_GET_SEALS", 1034)
F_SEAL_SEAL = getattr(fcntl, "F_SEAL_SEAL", 0x0001)
F_SEAL_SHRINK = getattr(fcntl, "F_SEAL_SHRINK", 0x0002)
F_SEAL_GROW = getattr(fcntl, "F_SEAL_GROW", 0x0004)
F_SEAL_WRITE = getattr(fcntl, "F_SEAL_WRITE", 0x0008)
MFD_CLOEXEC = getattr(os, "MFD_CLOEXEC", 0x0001)
MFD_ALLOW_SEALING = getattr(os, "MFD_ALLOW_SEALING", 0x0002)
CORPUS_EXECUTION_AUTHORITY_SEALS = (
    F_SEAL_WRITE | F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL
)
MAX_FILE_DESCRIPTOR = (
    1 << (ctypes.sizeof(ctypes.c_int) * 8 - 1)
) - 1
MAX_FILE_DESCRIPTOR_DECIMAL = str(MAX_FILE_DESCRIPTOR)

LOGICAL_DIGEST_OFFSET = 0xCBF29CE484222325
LOGICAL_DIGEST_PRIME = 0x100000001B3


def _mirror_logical_digest(data: bytes) -> int:
    """Independently mirror shared/digest.rs's wrapping FNV-1a fold."""

    value = LOGICAL_DIGEST_OFFSET
    for byte in data:
        value ^= byte
        value = value * LOGICAL_DIGEST_PRIME & ((1 << 64) - 1)
    return value


def exact_specialized_seed_authority() -> dict[str, int | str]:
    """Derive the exact fixed A seed observations without trusting child output."""

    stream_digest = _mirror_logical_digest(bytes(range(64)))
    logical_digest = _mirror_logical_digest(stream_digest.to_bytes(8, "little"))
    registry_head_digest = _mirror_logical_digest(
        (0).to_bytes(8, "little") + (0).to_bytes(8, "little")
    )
    return {
        "domain_events": 1,
        "visible_events": 1,
        # One StreamRegistered, one EventTypeRegistered, one domain event.
        "log_events": 3,
        "stream_digest": f"{stream_digest:016x}",
        "logical_digest": f"{logical_digest:064x}",
        "registry_head_digest": f"{registry_head_digest:064x}",
    }


SPECIALIZED_SEED_AUTHORITY = exact_specialized_seed_authority()

SOURCE_APPROVAL_FIELDS = set(schema.SOURCE_APPROVAL_FIELDS)
SOURCE_VARIANT_FIELDS = set(schema.SOURCE_APPROVAL_VARIANT_FIELDS)
PREPARED_FIELDS = set(schema.PREPARED_FIELDS)
PREPARED_VARIANT_FIELDS = set(schema.PREPARED_VARIANT_FIELDS)
PREPARED_ATTESTATION_FIELDS = set(schema.PREPARED_ATTESTATION_FIELDS)
CHILD_FIELDS = set(schema.CHILD_FIELDS)
GUARD_BINDING_FIELDS = set(schema.GUARD_BINDING_FIELDS)
GUARD_SNAPSHOT_FIELDS = set(schema.GUARD_SNAPSHOT_FIELDS)


class Problems:
    def __init__(self) -> None:
        self.errors: list[str] = []
        self.snapshots: dict[Path, schema.FileSnapshot] = {}

    def add(self, message: str) -> None:
        self.errors.append(message)

    def capture(self, context: str, action: Callable[[], Any]) -> Any | None:
        try:
            return action()
        except Exception as error:  # fail closed at every external-data boundary
            self.add(f"{context}: {error}")
            return None

    def remember(self, snapshot: schema.FileSnapshot) -> schema.FileSnapshot:
        self.snapshots[snapshot.path] = snapshot
        return snapshot

    def recalled(self, path: Path | schema.FileSnapshot) -> schema.FileSnapshot | None:
        if isinstance(path, schema.FileSnapshot):
            return path
        return self.snapshots.get(Path(path))


@dataclass(frozen=True)
class CorpusSnapshot:
    """One stable no-follow snapshot of an exact corpus directory tree."""

    root: Path
    root_identity: tuple[int, int]
    entries: tuple[dict[str, Any], ...]
    manifest_bytes: bytes
    sha256: str
    file_identities: frozenset[tuple[int, int]]


def canonical_json_bytes(value: Any) -> bytes:
    return schema.canonical_json_bytes(value)


def create_sealable_memfd(name: str) -> int:
    """Create one Linux memfd even when Python omits ``os.memfd_create``."""

    flags = MFD_CLOEXEC | MFD_ALLOW_SEALING
    native = getattr(os, "memfd_create", None)
    if callable(native):
        return int(native(name, flags))
    libc = ctypes.CDLL(None, use_errno=True)
    function = getattr(libc, "memfd_create", None)
    if function is None:
        raise OSError("Linux memfd_create is unavailable")
    function.argtypes = (ctypes.c_char_p, ctypes.c_uint)
    function.restype = ctypes.c_int
    descriptor = int(function(name.encode("utf-8"), flags))
    if descriptor < 0:
        error = ctypes.get_errno()
        raise OSError(error, f"memfd_create failed: errno={error}")
    return descriptor


def create_corpus_execution_authority_fd(
    payload: bytes,
    *,
    seal: bool = True,
) -> int:
    """Create a fixture FD with the production memfd write/seal contract."""

    descriptor = create_sealable_memfd("asterism-corpus-execution-authority")
    try:
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            if written <= 0:
                raise OSError("corpus execution authority write made no progress")
            offset += written
        if seal:
            fcntl.fcntl(
                descriptor,
                F_ADD_SEALS,
                CORPUS_EXECUTION_AUTHORITY_SEALS,
            )
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def parse_production_corpus_authority_fd(value: Any) -> int | None:
    """Parse one canonical positive C-int descriptor without coercion."""

    if (
        type(value) is not str
        or not value
        or len(value) > len(MAX_FILE_DESCRIPTOR_DECIMAL)
        or not value.isascii()
        or not value.isdecimal()
        or (
            len(value) == len(MAX_FILE_DESCRIPTOR_DECIMAL)
            and value > MAX_FILE_DESCRIPTOR_DECIMAL
        )
    ):
        return None
    try:
        descriptor = int(value)
    except (ValueError, OverflowError):
        return None
    if descriptor <= 2 or str(descriptor) != value:
        return None
    return descriptor


def capture_corpus_execution_authority(
    problems: Problems,
    *,
    synthetic: bool,
    provided_fd: int | None,
) -> dict[str, Any] | None:
    """Capture one sealed inherited memfd and relinquish descriptor ownership."""

    environment_value = os.environ.get(
        CORPUS_EXECUTION_AUTHORITY_FD_ENVIRONMENT
    )
    descriptor: int | None = None
    if synthetic:
        if environment_value is not None:
            problems.add(
                "synthetic evaluation inherited production corpus authority environment"
            )
        if (
            type(provided_fd) is not int
            or provided_fd <= 2
            or provided_fd > MAX_FILE_DESCRIPTOR
        ):
            problems.add("synthetic corpus execution authority FD is invalid")
        else:
            descriptor = provided_fd
    else:
        if provided_fd is not None:
            problems.add("production corpus authority was not supplied by environment")
        descriptor = parse_production_corpus_authority_fd(environment_value)
        if descriptor is None:
            problems.add("production corpus execution authority FD is invalid")

    payload: bytes | None = None
    if descriptor is not None:
        captured = False
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode):
                raise OSError("corpus execution authority FD is not regular")
            seals = int(fcntl.fcntl(descriptor, F_GET_SEALS))
            if seals != CORPUS_EXECUTION_AUTHORITY_SEALS:
                raise OSError(
                    "corpus execution authority FD seals differ from exact 0x0f"
                )
            chunks: list[bytes] = []
            offset = 0
            while offset < before.st_size:
                chunk = os.pread(
                    descriptor,
                    min(1024 * 1024, before.st_size - offset),
                    offset,
                )
                if not chunk:
                    raise OSError("corpus execution authority pread was short")
                chunks.append(chunk)
                offset += len(chunk)
            payload = b"".join(chunks)
            after = os.fstat(descriptor)
            if (
                _corpus_stat_identity(before) != _corpus_stat_identity(after)
                or len(payload) != before.st_size
            ):
                raise OSError("corpus execution authority FD changed during capture")
            captured = True
        except (OSError, ValueError, OverflowError) as error:
            problems.add(f"cannot capture corpus execution authority: {error}")
            payload = None
        finally:
            try:
                os.close(descriptor)
            except OSError as error:
                if error.errno != errno.EBADF or captured:
                    problems.add(
                        f"cannot close corpus execution authority FD: {error}"
                    )
            except (ValueError, OverflowError) as error:
                problems.add(
                    f"cannot close corpus execution authority FD: {error}"
                )
    if payload is None:
        return None
    try:
        return schema.parse_canonical_json_object(
            payload, "corpus execution authority"
        )
    except ValueError as error:
        problems.add(str(error))
        return None


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def snapshot_sha256(
    path: Path | schema.FileSnapshot,
    problems: Problems,
    *,
    expected_mode: int | None = 0o444,
) -> str:
    snapshot = problems.recalled(path)
    if snapshot is None:
        snapshot = schema.snapshot_regular_file(
            Path(path), expected_mode=expected_mode
        )
        problems.remember(snapshot)
    return snapshot.sha256


def is_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and set(value) <= SHA256


def is_git_id(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 40 and set(value) <= SHA256


def require_exact_keys(value: Any, expected: set[str], context: str, problems: Problems) -> bool:
    if not isinstance(value, dict):
        problems.add(f"{context} is not an object")
        return False
    if set(value) != expected:
        problems.add(
            f"{context} keys are not exact; missing={sorted(expected - set(value))}, "
            f"extra={sorted(set(value) - expected)}"
        )
        return False
    return True


def parse_timestamp(value: Any, context: str, problems: Problems) -> datetime | None:
    if not isinstance(value, str):
        problems.add(f"{context} is not a timestamp")
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        problems.add(f"{context} is invalid: {error}")
        return None
    if parsed.tzinfo is None:
        problems.add(f"{context} is not timezone-aware")
        return None
    return parsed


def read_canonical_object(
    path: Path | schema.FileSnapshot, context: str, problems: Problems
) -> dict[str, Any] | None:
    try:
        snapshot = problems.recalled(path)
        if snapshot is None:
            snapshot = problems.remember(
                schema.snapshot_regular_file(Path(path), expected_mode=0o444)
            )
        data = snapshot.data
        return schema.parse_canonical_json_object(data, context)
    except (OSError, ValueError) as error:
        problems.add(str(error))
        return None


def read_jsonl(
    path: Path | schema.FileSnapshot, context: str, problems: Problems
) -> list[dict[str, Any]]:
    try:
        snapshot = problems.recalled(path)
        if snapshot is None:
            snapshot = problems.remember(
                schema.snapshot_regular_file(Path(path), expected_mode=0o444)
            )
        data = snapshot.data
    except (OSError, ValueError) as error:
        problems.add(f"cannot read {context}: {error}")
        return []
    if data and not data.endswith(b"\n"):
        problems.add(f"{context} lacks final LF")
    records: list[dict[str, Any]] = []
    for ordinal, line in enumerate(data.splitlines(keepends=True), start=1):
        try:
            record = schema.parse_canonical_json_object(line, f"{context} line {ordinal}")
        except ValueError as error:
            problems.add(str(error))
            continue
        records.append(record)
    return records


def resolve_bound_file(
    path_value: Any,
    claimed_sha256: Any,
    context: str,
    problems: Problems,
    *,
    within: Path | None = None,
    expected_mode: int | None = schema.ARTIFACT_FILE_MODE,
) -> schema.FileSnapshot | None:
    if not isinstance(path_value, str) or not path_value:
        problems.add(f"{context} path is not text")
        return None
    if not is_sha256(claimed_sha256):
        problems.add(f"{context} claimed hash is not SHA-256")
        return None
    path = Path(path_value)
    snapshot = problems.recalled(path)
    if snapshot is None:
        snapshot = problems.capture(
            f"snapshot {context}",
            lambda: schema.snapshot_regular_file(
                path, within=within, expected_mode=expected_mode
            ),
        )
        if snapshot is None:
            return None
        problems.remember(snapshot)
    elif expected_mode is not None and snapshot.mode != expected_mode:
        problems.add(
            f"{context} mode {snapshot.mode:#06o} differs from exact {expected_mode:#06o}"
        )
    if snapshot.sha256 != claimed_sha256:
        problems.add(f"{context} hash mismatch")
    return snapshot


def validate_exact_mode(
    path: Path | schema.FileSnapshot,
    expected: int,
    context: str,
    problems: Problems,
) -> None:
    snapshot = problems.recalled(path)
    if snapshot is None:
        snapshot = problems.capture(
            f"snapshot {context}",
            lambda: schema.snapshot_regular_file(Path(path), expected_mode=expected),
        )
        if snapshot is None:
            return
        problems.remember(snapshot)
    if snapshot.mode != expected:
        problems.add(f"{context} mode is not exact {expected:04o}")


def live_filesystem_identity(path: Path) -> dict[str, str]:
    resolved = path.resolve(strict=True)
    chosen: dict[str, str] | None = None
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        before, separator, after = line.partition(" - ")
        if not separator:
            continue
        fields = before.split()
        tail = after.split()
        if len(fields) < 6 or len(tail) < 3:
            continue
        def decode_mount(value: str) -> str:
            return (
                value.replace("\\040", " ")
                .replace("\\011", "\t")
                .replace("\\012", "\n")
                .replace("\\134", "\\")
            )

        target = Path(decode_mount(fields[4]))
        try:
            resolved.relative_to(target)
        except ValueError:
            continue
        candidate = {
            "mount_id": fields[0], "parent_mount_id": fields[1],
            "device": fields[2], "root": decode_mount(fields[3]), "target": str(target),
            "mount_options": decode_mount(fields[5]), "filesystem_type": tail[0],
            "source": decode_mount(tail[1]), "super_options": decode_mount(tail[2]),
        }
        if chosen is None or len(candidate["target"]) > len(chosen["target"]):
            chosen = candidate
    if chosen is None:
        raise ValueError("cannot resolve live mount identity")
    return chosen


def live_scheduler_identity(logical_device: str) -> dict[str, str]:
    logical = (Path("/sys/dev/block") / logical_device).resolve(strict=True)
    for candidate in (logical, *logical.parents):
        scheduler = candidate / "queue" / "scheduler"
        if scheduler.is_file():
            return {
                "logical_device": logical_device,
                "base_device": candidate.name,
                "scheduler_path": str(scheduler.resolve(strict=True)),
                "scheduler_value": scheduler.read_text().strip(),
            }
    raise ValueError(f"cannot resolve base scheduler for {logical_device}")


def live_cpu_model() -> str:
    for line in Path("/proc/cpuinfo").read_text().splitlines():
        key, separator, value = line.partition(":")
        if separator and key.strip() == "model name" and value.strip():
            return value.strip()
    raise ValueError("CPU model name is unavailable")


def live_cpu_topology() -> dict[str, int]:
    cpus = sorted(
        path for path in Path("/sys/devices/system/cpu").glob("cpu[0-9]*")
        if path.name[3:].isdigit()
    )
    cores: set[tuple[int, int]] = set()
    packages: set[int] = set()
    for cpu in cpus:
        topology = cpu / "topology"
        package = int((topology / "physical_package_id").read_text().strip())
        core = int((topology / "core_id").read_text().strip())
        packages.add(package)
        cores.add((package, core))
    if not cpus or not cores or len(cpus) % len(cores):
        raise ValueError("CPU topology is incomplete or nonuniform")
    return {
        "logical_cpus": len(cpus),
        "physical_packages": len(packages),
        "cores": len(cores),
        "threads_per_core": len(cpus) // len(cores),
    }


def live_governors() -> dict[str, str]:
    result = {
        str(policy.resolve(strict=True)): (policy / "scaling_governor").read_text().strip()
        for policy in sorted(Path("/sys/devices/system/cpu/cpufreq").glob("policy*"))
        if (policy / "scaling_governor").is_file()
    }
    if not result:
        raise ValueError("CPU governor policies are unavailable")
    return result


def live_turbo_state() -> dict[str, str]:
    paths = {
        "intel_pstate_no_turbo": Path("/sys/devices/system/cpu/intel_pstate/no_turbo"),
        "cpufreq_boost": Path("/sys/devices/system/cpu/cpufreq/boost"),
    }
    return {
        name: path.read_text().strip() if path.is_file() else schema.NOT_AVAILABLE
        for name, path in paths.items()
    }


def exact_config_cell_orders() -> dict[str, list[dict[str, Any]]]:
    """Reconstruct config_for's deterministic orders without attempt input."""

    orders: dict[str, list[dict[str, Any]]] = {}
    for track in schema.CONFIG_CELL_ORDER_TRACKS:
        cells = schema.canonical_cells(track)

        def key(cell: Mapping[str, Any]) -> tuple[str, bytes]:
            payload = canonical_json_bytes(cell)
            digest = hashlib.sha256(
                schema.PROTOCOL_SHA256.encode()
                + b"\0"
                + track.encode()
                + b"\0"
                + payload
            ).hexdigest()
            return digest, payload

        orders[track] = sorted(cells, key=key)
    return orders


def exact_config_seed_sha256(source_approval_sha256: object) -> str | None:
    if not is_sha256(source_approval_sha256):
        return None
    material = (
        schema.PROTOCOL_SHA256 + "\0" + str(source_approval_sha256)
    ).encode()
    return hashlib.sha256(material).hexdigest()


def validate_config(
    config: dict[str, Any] | None,
    protocol_sha256: str,
    source_approval_sha256: object,
    problems: Problems,
) -> None:
    if config is None or not require_exact_keys(config, set(schema.CONFIG_FIELDS), "config", problems):
        return
    exact = {
        "schema": schema.CONFIG_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": protocol_sha256,
        "approved": True,
        "variant_sources": schema.VARIANT_SOURCE_BINDINGS,
        "resource_limits": {"free_bytes": schema.MIN_FREE_BYTES, "free_inodes": schema.MIN_FREE_INODES, "load1_milli": 6_000, "quiet_wait_seconds": 120},
        "settle_ms": {"Process": 400, "Group": 4_000},
    }
    for field, expected in exact.items():
        if config.get(field) != expected:
            problems.add(f"config {field} does not match frozen contract")
    for field in ("tooling_commit", "tooling_tree"):
        if not is_git_id(config.get(field)):
            problems.add(f"config {field} is not a Git object id")
    for field in ("attempt_nonce", "seed_sha256", "profile_contract_sha256"):
        if not is_sha256(config.get(field)):
            problems.add(f"config {field} is not SHA-256")
    expected_seed = exact_config_seed_sha256(source_approval_sha256)
    if expected_seed is None or config.get("seed_sha256") != expected_seed:
        problems.add("config seed differs from original source approval authority")
    if not isinstance(config.get("review_id"), str) or not config["review_id"]:
        problems.add("config review_id is empty")
    locks = config.get("lock_hashes")
    if not isinstance(locks, dict) or set(locks) != set(schema.VARIANTS):
        problems.add("config lock_hashes keys are not exact")
    else:
        for variant, digest in locks.items():
            if not is_sha256(digest):
                problems.add(f"config {variant} lock hash is invalid")
        if locks.get("A") != schema.CURRENT_LOCK_SHA256 or locks.get("B") != schema.CURRENT_LOCK_SHA256:
            problems.add("config current lock hashes differ from frozen lock")
    problems.capture("config cell orders", lambda: schema.validate_cell_orders(config))
    if config.get("cell_orders") != exact_config_cell_orders():
        problems.add("config cell orders differ from reconstructed runner authority")
    templates = config.get("argv_templates")
    if not isinstance(templates, dict) or set(templates) != {
        "primary", "new_names", "fairness", "reopen", "cpu_profiles",
        "syscall_profiles", "structural_traces",
    }:
        problems.add("config argv_templates keys are not exact")
    elif not all(isinstance(value, list) and value and all(isinstance(item, str) for item in value) for value in templates.values()):
        problems.add("config argv_templates are not nonempty string arrays")
    else:
        expected_row_template = [
            "{binary}", "--run-row", "--track", "{track}",
            "--row-ordinal", "{row_ordinal}", "--config", "{config}",
        ]
        for track, template in templates.items():
            if template != expected_row_template:
                problems.add(f"config {track} argv template differs from frozen row command")
    cases = config.get("correctness_cases")
    if cases != schema.correctness_descriptors():
        problems.add("config correctness_cases differ from frozen partition/order")
    executions = config.get("correctness_execution")
    if not isinstance(executions, list) or len(executions) != len(schema.CORRECTNESS_GROUPS):
        problems.add("config correctness_execution cardinality differs from frozen contract")
    else:
        for ordinal, (execution, group) in enumerate(
            zip(executions, schema.CORRECTNESS_GROUPS), start=1
        ):
            context = f"config correctness execution {ordinal}"
            if not require_exact_keys(
                execution, set(schema.CORRECTNESS_EXECUTION_FIELDS), context, problems
            ):
                continue
            if tuple(execution.get(field) for field in ("variant", "phase", "suite", "kind")) != group:
                problems.add(f"{context} group/order differs from frozen contract")
            environment = execution.get("environment")
            if not isinstance(environment, dict) or set(environment) != set(schema.CORRECTNESS_ENV_FIELDS):
                problems.add(f"{context} environment fields are not exact")
    smokes = config.get("smoke_transitions")
    if not isinstance(smokes, list) or not smokes:
        problems.add("config smoke_transitions is empty")
    else:
        for ordinal, smoke in enumerate(smokes, start=1):
            if not isinstance(smoke, dict) or set(smoke) != {"id", "variant", "argv"}:
                problems.add(f"config smoke transition {ordinal} fields are not exact")
                continue
            if not isinstance(smoke["id"], str) or not smoke["id"]:
                problems.add(f"config smoke transition {ordinal} id invalid")
            if smoke["variant"] not in set(schema.VARIANTS):
                problems.add(f"config smoke transition {ordinal} variant invalid")
            if not isinstance(smoke["argv"], list) or not smoke["argv"] or not all(isinstance(item, str) for item in smoke["argv"]):
                problems.add(f"config smoke transition {ordinal} argv invalid")


def _corpus_stat_identity(
    info: os.stat_result,
) -> tuple[int, int, int, int, int, int, int]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_nlink,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def _corpus_lexical_root(root: Path) -> Path:
    value = Path(root)
    if not value.is_absolute():
        raise ValueError("corpus root is not absolute")
    if any(component in {"", ".", ".."} for component in value.parts[1:]):
        raise ValueError("corpus root is not lexically normalized")
    if len(value.parts) < 2:
        raise ValueError("filesystem root cannot be a corpus")
    return value


def _snapshot_corpus_tree(
    root: Path,
    *,
    allow_missing: bool,
    root_mode: int | None,
    directory_mode: int | None,
    file_mode: int | None,
    include_modes: bool,
) -> CorpusSnapshot | None:
    """Snapshot a corpus through stable descriptor-relative no-follow walks."""

    lexical_root = _corpus_lexical_root(root)
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    file_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    chain_fds: list[int] = []
    chain_identities: list[tuple[int, int]] = []
    entries: list[dict[str, Any]] = []
    file_identities: set[tuple[int, int]] = set()

    def require_open_identity(
        before: os.stat_result,
        opened: os.stat_result,
        context: str,
    ) -> None:
        if _corpus_stat_identity(before) != _corpus_stat_identity(opened):
            raise OSError(f"{context} was replaced between lstat and open")

    def walk(
        directory_fd: int,
        relative: Path,
        expected_directory_mode: int | None,
    ) -> None:
        directory_before = os.fstat(directory_fd)
        if not stat.S_ISDIR(directory_before.st_mode):
            raise OSError(f"corpus entry is not a directory: {relative.as_posix()}")
        if (
            expected_directory_mode is not None
            and stat.S_IMODE(directory_before.st_mode)
            != expected_directory_mode
        ):
            label = relative.as_posix() if relative.parts else "."
            raise OSError(f"corpus directory mode differs at {label}")
        names_before = sorted(os.listdir(directory_fd))
        for name in names_before:
            child_relative = relative / name
            child_label = child_relative.as_posix()
            before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if stat.S_ISLNK(before.st_mode):
                raise OSError(f"corpus contains symlink {child_label}")
            if stat.S_ISDIR(before.st_mode):
                child_fd = os.open(name, directory_flags, dir_fd=directory_fd)
                try:
                    opened = os.fstat(child_fd)
                    require_open_identity(before, opened, child_label)
                    entry = {"path": child_label, "kind": "directory"}
                    if include_modes:
                        entry["mode"] = stat.S_IMODE(opened.st_mode)
                    entries.append(entry)
                    walk(child_fd, child_relative, directory_mode)
                    after = os.fstat(child_fd)
                    if _corpus_stat_identity(opened) != _corpus_stat_identity(after):
                        raise OSError(
                            f"corpus directory changed during snapshot: {child_label}"
                        )
                finally:
                    os.close(child_fd)
                path_after = os.stat(
                    name, dir_fd=directory_fd, follow_symlinks=False
                )
                if _corpus_stat_identity(before) != _corpus_stat_identity(path_after):
                    raise OSError(
                        f"corpus directory path changed during snapshot: {child_label}"
                    )
                continue
            if not stat.S_ISREG(before.st_mode):
                raise OSError(f"corpus contains non-file {child_label}")
            if before.st_nlink != 1:
                raise OSError(f"corpus file is hard-linked: {child_label}")
            if (
                file_mode is not None
                and stat.S_IMODE(before.st_mode) != file_mode
            ):
                raise OSError(f"corpus file mode differs at {child_label}")
            file_fd = os.open(name, file_flags, dir_fd=directory_fd)
            try:
                opened = os.fstat(file_fd)
                require_open_identity(before, opened, child_label)
                digest = hashlib.sha256()
                byte_count = 0
                while True:
                    chunk = os.read(file_fd, 1024 * 1024)
                    if not chunk:
                        break
                    digest.update(chunk)
                    byte_count += len(chunk)
                after = os.fstat(file_fd)
                if (
                    _corpus_stat_identity(opened) != _corpus_stat_identity(after)
                    or byte_count != opened.st_size
                ):
                    raise OSError(
                        f"corpus file changed during snapshot: {child_label}"
                    )
            finally:
                os.close(file_fd)
            path_after = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if _corpus_stat_identity(before) != _corpus_stat_identity(path_after):
                raise OSError(
                    f"corpus file path changed during snapshot: {child_label}"
                )
            identity = (opened.st_dev, opened.st_ino)
            if identity in file_identities:
                raise OSError(f"corpus file identity is aliased: {child_label}")
            file_identities.add(identity)
            entry = {
                "path": child_label,
                "kind": "file",
                "bytes": byte_count,
                "sha256": digest.hexdigest(),
            }
            if include_modes:
                entry["mode"] = stat.S_IMODE(opened.st_mode)
            entries.append(entry)
        if sorted(os.listdir(directory_fd)) != names_before:
            label = relative.as_posix() if relative.parts else "."
            raise OSError(f"corpus directory names changed during snapshot: {label}")
        directory_after = os.fstat(directory_fd)
        if _corpus_stat_identity(directory_before) != _corpus_stat_identity(
            directory_after
        ):
            label = relative.as_posix() if relative.parts else "."
            raise OSError(f"corpus directory changed during snapshot: {label}")

    try:
        current = os.open("/", directory_flags)
        chain_fds.append(current)
        root_info = os.fstat(current)
        chain_identities.append((root_info.st_dev, root_info.st_ino))
        components = lexical_root.parts[1:]
        for index, component in enumerate(components):
            try:
                before = os.stat(
                    component, dir_fd=current, follow_symlinks=False
                )
            except FileNotFoundError:
                if allow_missing and index == len(components) - 1:
                    return None
                raise
            if stat.S_ISLNK(before.st_mode):
                raise OSError(f"corpus path contains symlink component {component}")
            if not stat.S_ISDIR(before.st_mode):
                raise OSError(f"corpus path component is not a directory: {component}")
            next_fd = os.open(component, directory_flags, dir_fd=current)
            try:
                opened = os.fstat(next_fd)
                require_open_identity(before, opened, str(lexical_root))
            except Exception:
                os.close(next_fd)
                raise
            chain_fds.append(next_fd)
            current = next_fd
            chain_identities.append((opened.st_dev, opened.st_ino))

        corpus_root_before = os.fstat(current)
        walk(current, Path(), root_mode)
        corpus_root_after = os.fstat(current)
        if _corpus_stat_identity(corpus_root_before) != _corpus_stat_identity(
            corpus_root_after
        ):
            raise OSError("corpus root changed during snapshot")

        # Reopen the complete lexical chain and prove that every name still
        # resolves to the directory descriptor identity observed initially.
        verify = os.open("/", directory_flags)
        try:
            if (os.fstat(verify).st_dev, os.fstat(verify).st_ino) != chain_identities[0]:
                raise OSError("filesystem root identity changed")
            for index, component in enumerate(components, start=1):
                before = os.stat(
                    component, dir_fd=verify, follow_symlinks=False
                )
                next_fd = os.open(component, directory_flags, dir_fd=verify)
                try:
                    opened = os.fstat(next_fd)
                    require_open_identity(
                        before, opened, str(lexical_root)
                    )
                    if (
                        opened.st_dev,
                        opened.st_ino,
                    ) != chain_identities[index]:
                        raise OSError(
                            "corpus path ancestor identity changed"
                        )
                except Exception:
                    os.close(next_fd)
                    raise
                try:
                    os.close(verify)
                except Exception:
                    os.close(next_fd)
                    raise
                verify = next_fd
        finally:
            os.close(verify)
        # Match runner 40a's ``sorted(root.rglob("*"))`` Path ordering exactly,
        # including directory-prefix ordering that differs from raw strings.
        entries.sort(key=lambda item: Path(str(item["path"])))
        if not any(entry["kind"] == "file" for entry in entries):
            raise OSError("corpus contains no regular files")
        manifest_bytes = canonical_json_bytes(entries)
        return CorpusSnapshot(
            root=lexical_root,
            root_identity=(corpus_root_before.st_dev, corpus_root_before.st_ino),
            entries=tuple(entries),
            manifest_bytes=manifest_bytes,
            sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            file_identities=frozenset(file_identities),
        )
    finally:
        for descriptor in reversed(chain_fds):
            os.close(descriptor)


def snapshot_corpus(
    root: Path,
    problems: Problems,
    context: str,
    *,
    required: bool = True,
    require_read_only: bool = False,
) -> CorpusSnapshot | None:
    try:
        snapshot = _snapshot_corpus_tree(
            root,
            allow_missing=not required,
            root_mode=0o555 if require_read_only else None,
            directory_mode=0o555 if require_read_only else None,
            file_mode=0o444 if require_read_only else None,
            include_modes=False,
        )
    except (OSError, ValueError) as error:
        problems.add(f"{context}: {error}")
        return None
    if snapshot is None and required:
        problems.add(f"{context}: required corpus is missing")
    return snapshot


def corpus_content_sha256(
    root: Path,
    problems: Problems,
    context: str,
) -> str | None:
    snapshot = snapshot_corpus(root, problems, context)
    return snapshot.sha256 if snapshot is not None else None


def expected_corpus_execution_records(
    scratch_root: Path,
    attempt_nonce: str,
    *,
    correctness_only: bool,
) -> list[dict[str, Any]]:
    """Independently reconstruct runner corpus registration order and paths."""

    records: list[dict[str, Any]] = []

    def append(
        role: str,
        track: str,
        row_ordinal: int,
        variant: str,
        root: Path,
        *,
        read_only: bool,
    ) -> None:
        records.append(
            {
                "ordinal": len(records) + 1,
                "role": role,
                "track": track,
                "row_ordinal": row_ordinal,
                "variant": variant,
                "root": str(root),
                "root_mode": 0o555 if read_only else 0o755,
                "directory_mode": 0o555 if read_only else 0o755,
                "file_mode": 0o444 if read_only else 0o644,
            }
        )

    attempt_scratch = scratch_root / "attempts" / attempt_nonce
    append(
        "specialized-source",
        "specialized_reopen",
        0,
        "A",
        attempt_scratch / "smoke-reopen" / "archive-source",
        read_only=True,
    )
    for row_ordinal, track in enumerate(
        ("smoke_reopen", "smoke_structural_reopen"), start=1
    ):
        append(
            "specialized-copy",
            track,
            row_ordinal,
            "A",
            schema.fresh_store_path(
                scratch_root,
                attempt_nonce,
                "smoke-reopen-corpus",
                row_ordinal,
                "A",
            ),
            read_only=False,
        )
    if correctness_only:
        return records
    for variant in schema.PUBLIC_VARIANTS:
        append(
            "full-source",
            "reopen_seed",
            0,
            variant,
            attempt_scratch / "corpora" / f"{variant}-archive-source",
            read_only=True,
        )
    for row_ordinal, variant in enumerate(
        ("A", "C", "D", "C", "D", "A", "D", "A", "C"), start=1
    ):
        append(
            "full-copy",
            "reopen",
            row_ordinal,
            variant,
            schema.fresh_store_path(
                scratch_root,
                attempt_nonce,
                "reopen-corpus",
                row_ordinal,
                variant,
            ),
            read_only=False,
        )
    for row_ordinal, variant in zip(
        (13, 14, 15), schema.PUBLIC_VARIANTS, strict=True
    ):
        append(
            "full-copy",
            "structural_traces",
            row_ordinal,
            variant,
            schema.fresh_store_path(
                scratch_root,
                attempt_nonce,
                "structural_traces-corpus",
                row_ordinal,
                variant,
            ),
            read_only=False,
        )
    return records


def corpus_authority_content_entries(
    record: Mapping[str, Any],
) -> list[dict[str, Any]]:
    entries = record.get("entries")
    if not isinstance(entries, list):
        return []
    return [
        {key: value for key, value in entry.items() if key != "mode"}
        for entry in entries
        if isinstance(entry, Mapping)
    ]


def corpus_authority_content_sha256(record: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        canonical_json_bytes(corpus_authority_content_entries(record))
    ).hexdigest()


def validate_corpus_execution_authority(
    value: Mapping[str, Any] | None,
    scratch_root: Path | None,
    attempt_nonce: str | None,
    child_records: Sequence[Mapping[str, Any]],
    problems: Problems,
    *,
    correctness_only: bool,
) -> list[dict[str, Any]]:
    """Validate sealed pre-execution records and exact current tree replay."""

    error_count = len(problems.errors)
    top_fields = {
        "schema",
        "protocol",
        "protocol_sha256",
        "attempt_nonce",
        "correctness_only",
        "records",
    }
    if not require_exact_keys(
        value, top_fields, "corpus execution authority", problems
    ):
        return []
    assert isinstance(value, Mapping)
    if value.get("schema") != CORPUS_EXECUTION_AUTHORITY_SCHEMA:
        problems.add("corpus execution authority schema differs")
    if (
        value.get("protocol") != schema.PROTOCOL
        or value.get("protocol_sha256") != schema.PROTOCOL_SHA256
    ):
        problems.add("corpus execution authority protocol differs")
    if value.get("attempt_nonce") != attempt_nonce:
        problems.add("corpus execution authority attempt nonce differs")
    if value.get("correctness_only") is not correctness_only:
        problems.add("corpus execution authority mode differs")
    if scratch_root is None or not isinstance(attempt_nonce, str):
        problems.add("corpus execution authority path inputs are unavailable")
        return []
    expected = expected_corpus_execution_records(
        scratch_root, attempt_nonce, correctness_only=correctness_only
    )
    records_value = value.get("records")
    if not isinstance(records_value, list):
        problems.add("corpus execution authority records are not a list")
        return []
    records = [record for record in records_value if isinstance(record, dict)]
    if len(records) != len(records_value):
        problems.add("corpus execution authority record is not an object")
    if len(records) != len(expected):
        problems.add("corpus execution authority record count differs")

    record_fields = {
        "ordinal",
        "role",
        "track",
        "row_ordinal",
        "variant",
        "root",
        "root_mode",
        "directory_mode",
        "file_mode",
        "entries",
        "tree_sha256",
    }
    snapshots: list[CorpusSnapshot] = []
    for index, (record, expected_record) in enumerate(
        zip(records, expected), start=1
    ):
        context = f"corpus execution authority record {index}"
        if not require_exact_keys(record, record_fields, context, problems):
            continue
        binding = {
            key: record.get(key)
            for key in expected_record
        }
        if binding != expected_record or any(
            type(binding[key]) is not type(expected_value)
            for key, expected_value in expected_record.items()
        ):
            problems.add(f"{context} physical plan differs")
        entries = record.get("entries")
        if not isinstance(entries, list) or not entries:
            problems.add(f"{context} entries are empty or invalid")
            continue
        entry_paths: list[Path] = []
        for entry_index, entry in enumerate(entries, start=1):
            entry_context = f"{context} entry {entry_index}"
            if not isinstance(entry, Mapping):
                problems.add(f"{entry_context} is not an object")
                continue
            kind = entry.get("kind")
            fields = (
                {"path", "kind", "mode"}
                if kind == "directory"
                else {"path", "kind", "mode", "bytes", "sha256"}
                if kind == "file"
                else set()
            )
            if not fields or set(entry) != fields:
                problems.add(f"{entry_context} fields/kind are not exact")
                continue
            path_value = entry.get("path")
            relative = Path(path_value) if isinstance(path_value, str) else None
            if (
                relative is None
                or relative.is_absolute()
                or not relative.parts
                or any(part in {"", ".", ".."} for part in relative.parts)
                or relative.as_posix() != path_value
            ):
                problems.add(f"{entry_context} path is not canonical relative")
            else:
                entry_paths.append(relative)
            expected_mode = (
                record.get("directory_mode")
                if kind == "directory"
                else record.get("file_mode")
            )
            if (
                type(entry.get("mode")) is not int
                or entry.get("mode") != expected_mode
            ):
                problems.add(f"{entry_context} mode differs")
            if kind == "file" and (
                isinstance(entry.get("bytes"), bool)
                or not isinstance(entry.get("bytes"), int)
                or entry["bytes"] < 0
                or not is_sha256(entry.get("sha256"))
            ):
                problems.add(f"{entry_context} file metadata is invalid")
        if (
            len(set(entry_paths)) != len(entry_paths)
            or entry_paths != sorted(entry_paths)
        ):
            problems.add(f"{context} entry path order/uniqueness differs")
        tree_sha256 = hashlib.sha256(canonical_json_bytes(entries)).hexdigest()
        if record.get("tree_sha256") != tree_sha256:
            problems.add(f"{context} tree hash differs from sealed entries")
        snapshot = problems.capture(
            f"{context} current tree snapshot",
            lambda record=record: _snapshot_corpus_tree(
                Path(str(record["root"])),
                allow_missing=False,
                root_mode=int(record["root_mode"]),
                directory_mode=int(record["directory_mode"]),
                file_mode=int(record["file_mode"]),
                include_modes=True,
            ),
        )
        if isinstance(snapshot, CorpusSnapshot):
            snapshots.append(snapshot)
            if list(snapshot.entries) != entries:
                problems.add(f"{context} current tree differs from sealed authority")
            if snapshot.sha256 != record.get("tree_sha256"):
                problems.add(f"{context} current tree hash differs")

    root_identities = [snapshot.root_identity for snapshot in snapshots]
    file_identities = [
        identity
        for snapshot in snapshots
        for identity in snapshot.file_identities
    ]
    if len(set(root_identities)) != len(root_identities):
        problems.add("corpus execution authority current root identities alias")
    if len(set(file_identities)) != len(file_identities):
        problems.add("corpus execution authority current file identities alias")
    if len(set(root_identities + file_identities)) != len(
        root_identities + file_identities
    ):
        problems.add("corpus execution authority current root/file identities alias")

    if len(records) == len(expected):
        specialized_source = records[0]
        full_sources = {
            record["variant"]: record
            for record in records
            if record.get("role") == "full-source"
        }
        for record in records:
            role = record.get("role")
            if role not in {"specialized-copy", "full-copy"}:
                continue
            source = (
                specialized_source
                if role == "specialized-copy"
                else full_sources.get(record.get("variant"))
            )
            if not isinstance(source, Mapping):
                problems.add("corpus execution authority copy source is unavailable")
                continue
            if corpus_authority_content_entries(record) != (
                corpus_authority_content_entries(source)
            ):
                problems.add(
                    f"corpus execution authority copy {record.get('ordinal')} "
                    "content differs from sealed source"
                )
            matching_children = [
                child
                for child in child_records
                if child.get("kind") == record.get("track")
                and isinstance(child.get("context"), Mapping)
                and child["context"].get("variant") == record.get("variant")
                and (
                    role == "specialized-copy"
                    or child["context"].get("row_ordinal")
                    == record.get("row_ordinal")
                )
            ]
            if len(matching_children) != 1:
                problems.add(
                    f"corpus execution authority copy {record.get('ordinal')} "
                    "does not bind one executed child"
                )
                continue
            child_context = matching_children[0]["context"]
            expected_context = {
                "archive_manifest_sha256": corpus_authority_content_sha256(
                    source
                ),
                "copy_manifest_sha256": corpus_authority_content_sha256(record),
                "copy_id": Path(str(record.get("root"))).name,
                "copy_verified_read_only": True,
            }
            for field, expected_value in expected_context.items():
                if child_context.get(field) != expected_value:
                    problems.add(
                        f"corpus execution authority copy {record.get('ordinal')} "
                        f"child context {field} differs"
                    )

    return records if len(problems.errors) == error_count else []


def reconstruct_transition_authority(
    prepared: Mapping[str, Any] | None,
    output_dir: Path,
    scratch_root: Path | None,
    attempt_nonce: str | None,
    records: Sequence[Mapping[str, Any]],
    corpus_authority_records: Sequence[Mapping[str, Any]],
    problems: Problems,
) -> list[dict[str, Any]]:
    """Rebuild the runner's complete row-zero plan from prepared authority."""

    if not isinstance(prepared, Mapping):
        problems.add("transition authority prepared artifacts are unavailable")
        return []
    variants = prepared.get("variants")
    tools = prepared.get("tools")
    support = prepared.get("support_files")
    if not isinstance(variants, Mapping):
        problems.add("transition authority prepared variants are unavailable")
        return []
    if not isinstance(tools, Mapping):
        tools = {}
    if not isinstance(support, Mapping):
        support = {}

    if scratch_root is None or not isinstance(attempt_nonce, str):
        problems.add("transition scratch/attempt authority is unavailable")
        return []
    plan: list[dict[str, Any]] = []

    def variant_plan_environment(
        item: Mapping[str, Any],
        context: Mapping[str, Any],
        mode: str,
    ) -> dict[str, str]:
        value = item.get("evidence_env")
        environment = dict(value) if isinstance(value, Mapping) else {}
        environment.pop("ASTERISM_REBASELINE_LOG_PATH_MARKERS", None)
        environment.pop("ASTERISM_REBASELINE_METADATA_PATH_MARKERS", None)
        environment.update(
            {
                "ASTERISM_REBASELINE_MODE": mode,
                "ASTERISM_REBASELINE_VARIANT": str(context["variant"]),
            }
        )
        if mode == "smoke":
            environment["ASTERISM_REBASELINE_SMOKE_TARGET"] = str(
                context["smoke_target"]
            )
        for field, name in schema.ROW_DYNAMIC_ENVIRONMENT_FIELDS.items():
            value = context.get(field)
            if value is not None:
                environment[name] = str(value)
        return environment

    def variant_transition(
        transition_id: str,
        kind: str,
        variant: str,
        argv_field: str,
        context: Mapping[str, Any],
        plan_environment: Mapping[str, str],
        *,
        controlled: bool,
        store_path: Path | None,
        profile_track: str | None = None,
    ) -> dict[str, Any]:
        item = variants.get(variant)
        item = item if isinstance(item, Mapping) else {}
        binary = item.get("binary")
        binary = binary if isinstance(binary, Mapping) else {}
        argv = item.get(argv_field)
        return {
            "id": transition_id,
            "kind": kind,
            "variant": variant,
            "argv": list(argv) if isinstance(argv, list) else None,
            "executable_path": binary.get("path"),
            "executable_sha256": binary.get("sha256"),
            "executable_mode": item.get("executable_mode"),
            "executable_comm": item.get("comm"),
            "context": dict(context),
            "plan_environment": dict(plan_environment),
            "controlled": controlled,
            "store_path": store_path,
            "profile_track": profile_track,
        }

    for variant in schema.VARIANTS:
        item = variants.get(variant)
        item = item if isinstance(item, Mapping) else {}
        context = {"transition": "contract", "variant": variant}
        contract_environment = item.get("contract_env")
        plan.append(
            variant_transition(
                f"contract-{variant}",
                "contract",
                variant,
                "contract_argv",
                context,
                (
                    contract_environment
                    if isinstance(contract_environment, Mapping)
                    else {}
                ),
                controlled=False,
                store_path=None,
            )
        )

    authority_config = {"cell_orders": exact_config_cell_orders()}
    unique_shapes: list[tuple[str, str]] = []
    for track in schema.RUNNER_SMOKE_TRACK_ORDER:
        rows = problems.capture(
            f"reconstruct {track} smoke order",
            lambda track=track: schema.expected_order(authority_config, track),
        )
        if not isinstance(rows, list):
            continue
        for row in rows:
            variant = row.get("variant") if isinstance(row, Mapping) else None
            shape = (track, variant) if isinstance(variant, str) else None
            if shape is not None and shape not in unique_shapes:
                unique_shapes.append(shape)
    for smoke_ordinal, (track, variant) in enumerate(unique_shapes, start=1):
        item = variants.get(variant)
        item = item if isinstance(item, Mapping) else {}
        context: dict[str, Any] = {
            "transition": "smoke",
            "smoke_ordinal": smoke_ordinal,
            "smoke_target": track,
            "variant": variant,
            "durability": "Process",
        }
        store_path = schema.fresh_store_path(
            scratch_root, attempt_nonce, "smoke", smoke_ordinal, variant
        )
        if track in {"syscall_profiles", "structural_traces"}:
            context["variant_trace_path_markers"] = problems.capture(
                f"reconstruct {track}-{variant} trace markers",
                lambda store_path=store_path, variant=variant: (
                    schema.resolved_trace_path_markers(store_path, variant)
                ),
            )
        plan.append(
            variant_transition(
                f"{track}-{variant}",
                "smoke",
                variant,
                "evidence_argv",
                context,
                variant_plan_environment(item, context, "smoke"),
                controlled=True,
                store_path=store_path,
                profile_track=(
                    track
                    if track
                    in {"cpu_profiles", "syscall_profiles", "structural_traces"}
                    else None
                ),
            )
        )

    def binding_transition(
        transition_id: str,
        binding: Mapping[str, Any] | None,
        argv: list[str] | None,
        context: Mapping[str, Any],
        store_path: Path | None,
    ) -> dict[str, Any]:
        value = binding if isinstance(binding, Mapping) else {}
        return {
            "id": transition_id,
            "kind": "smoke",
            "variant": "A",
            "argv": argv,
            "executable_path": value.get("path"),
            "executable_sha256": value.get("sha256"),
            "executable_mode": value.get("executable_mode"),
            "executable_comm": value.get("comm"),
            "context": dict(context),
            "plan_environment": {
                "ASTERISM_REBASELINE_MODE": "smoke",
                "ASTERISM_REBASELINE_SMOKE_TARGET": str(
                    context["smoke_target"]
                ),
            },
            "controlled": True,
            "store_path": store_path,
            "profile_track": None,
        }

    next_smoke_ordinal = len(unique_shapes) + 1
    for target in schema.RUNNER_SMOKE_TOOL_TARGETS:
        binding = tools.get(target)
        binding = binding if isinstance(binding, Mapping) else None
        argv = [str(binding.get("path")), "--smoke"] if binding is not None else None
        context = {
            "transition": "smoke",
            "smoke_ordinal": next_smoke_ordinal,
            "smoke_target": target,
            "variant": "A",
            "durability": "Process",
        }
        store_path = schema.fresh_store_path(
            scratch_root, attempt_nonce, "smoke", next_smoke_ordinal, "A"
        )
        plan.append(
            binding_transition(
                f"{target}-A", binding, argv, context, store_path
            )
        )
        next_smoke_ordinal += 1
    for role, runtime_name, support_name in schema.RUNNER_SMOKE_RUNTIME_ROLES:
        runtime = tools.get(runtime_name)
        runtime = runtime if isinstance(runtime, Mapping) else None
        script = support.get(support_name)
        script = script if isinstance(script, Mapping) else None
        argv = (
            [str(runtime.get("path")), str(script.get("path")), "--smoke"]
            if runtime is not None and script is not None
            else None
        )
        context = {
            "transition": "smoke",
            "smoke_ordinal": next_smoke_ordinal,
            "smoke_target": role,
            "variant": "A",
            "durability": "Process",
        }
        plan.append(
            binding_transition(f"{role}-A", runtime, argv, context, None)
        )
        next_smoke_ordinal += 1

    variant_a = variants.get("A")
    variant_a = variant_a if isinstance(variant_a, Mapping) else {}
    seed_context = {
        "transition": "smoke",
        "smoke_id": "smoke_reopen_seed",
        "smoke_target": "smoke_reopen_seed",
        "variant": "A",
        "durability": "Process",
        "domain_events": 1,
        "streams": 1,
        "batch": 1,
        "payload": 64,
        "segment_bytes": 8 * 1024 * 1024,
    }
    source_store = (
        scratch_root
        / "attempts"
        / attempt_nonce
        / "smoke-reopen"
        / "archive-source"
    )
    plan.append(
        variant_transition(
            "smoke_reopen_seed",
            "smoke_reopen_seed",
            "A",
            "evidence_argv",
            seed_context,
            variant_plan_environment(
                variant_a, seed_context, "smoke_reopen_seed"
            ),
            controlled=True,
            store_path=source_store,
        )
    )

    seed_record = next(
        (
            record
            for record in records
            if record.get("kind") == "smoke_reopen_seed"
        ),
        None,
    )
    seed = (
        read_canonical_object(
            Path(str(seed_record.get("raw_path"))),
            "specialized smoke seed output",
            problems,
        )
        if isinstance(seed_record, Mapping)
        else None
    )
    seed_fields = {
        "schema",
        "protocol",
        "protocol_sha256",
        "variant",
        "domain_events",
        "visible_events",
        "log_events",
        "logical_digest",
        "registry_head_digest",
    }
    if (
        not isinstance(seed, Mapping)
        or set(seed) != seed_fields
        or seed.get("schema") != "bn-2l3n-overlay-smoke-reopen-seed-v3"
        or seed.get("protocol") != schema.PROTOCOL
        or seed.get("protocol_sha256") != schema.PROTOCOL_SHA256
        or seed.get("variant") != "A"
        or seed.get("domain_events")
        != SPECIALIZED_SEED_AUTHORITY["domain_events"]
        or seed.get("visible_events")
        != SPECIALIZED_SEED_AUTHORITY["visible_events"]
        or seed.get("log_events") != SPECIALIZED_SEED_AUTHORITY["log_events"]
        or seed.get("logical_digest")
        != SPECIALIZED_SEED_AUTHORITY["logical_digest"]
        or seed.get("registry_head_digest")
        != SPECIALIZED_SEED_AUTHORITY["registry_head_digest"]
    ):
        problems.add("specialized smoke seed output differs from exact authority")
        seed = {}
    specialized_specs: list[tuple[str, str, str, Path]] = []
    for copy_ordinal, (transition_id, profile_track, smoke_target) in enumerate(
        (
            ("smoke_reopen", "reopen", "smoke_reopen"),
            (
                "smoke_structural_reopen",
                "structural_traces",
                "structural_traces",
            ),
        ),
        start=1,
    ):
        store_path = schema.fresh_store_path(
            scratch_root,
            attempt_nonce,
            "smoke-reopen-corpus",
            copy_ordinal,
            "A",
        )
        specialized_specs.append(
            (transition_id, profile_track, smoke_target, store_path)
        )

    specialized_authority = {
        str(record.get("track")): record
        for record in corpus_authority_records
        if record.get("role") in {"specialized-source", "specialized-copy"}
    }
    source_authority = specialized_authority.get("specialized_reopen")
    if not isinstance(source_authority, Mapping):
        problems.add("sealed specialized source corpus authority is unavailable")
    source_digest = (
        corpus_authority_content_sha256(source_authority)
        if isinstance(source_authority, Mapping)
        else None
    )
    for transition_id, _, _, store_path in specialized_specs:
        record = next(
            (item for item in records if item.get("kind") == transition_id),
            None,
        )
        environment = (
            record.get("environment") if isinstance(record, Mapping) else None
        )
        if (
            not isinstance(environment, Mapping)
            or environment.get("ASTERISM_REBASELINE_STORE") != str(store_path)
        ):
            problems.add(
                f"{transition_id} child store differs from exact executed-copy path"
            )
    for transition_id, profile_track, smoke_target, store_path in specialized_specs:
        copy_authority = specialized_authority.get(transition_id)
        copy_digest = (
            corpus_authority_content_sha256(copy_authority)
            if isinstance(copy_authority, Mapping)
            else None
        )
        if not isinstance(copy_authority, Mapping):
            problems.add(f"sealed {transition_id} corpus authority is unavailable")
        context = {
            "transition": "smoke",
            "smoke_id": transition_id,
            "smoke_target": smoke_target,
            "profile_smoke_track": profile_track,
            "reopen_order": True,
            "trace_kind": (
                "reopen" if transition_id == "smoke_structural_reopen" else None
            ),
            "variant": "A",
            "durability": "Process",
            "archive_manifest_sha256": source_digest,
            "copy_manifest_sha256": copy_digest,
            "copy_id": store_path.name,
            "copy_verified_read_only": True,
            "expected_domain_events": seed.get("domain_events"),
            "expected_visible_events": seed.get("visible_events"),
            "expected_log_events": seed.get("log_events"),
            "expected_logical_digest": seed.get("logical_digest"),
            "expected_registry_head_digest": seed.get(
                "registry_head_digest"
            ),
        }
        if transition_id == "smoke_structural_reopen":
            context["variant_trace_path_markers"] = problems.capture(
                "reconstruct specialized structural trace markers",
                lambda store_path=store_path: schema.resolved_trace_path_markers(
                    store_path, "A"
                ),
            )
        plan.append(
            variant_transition(
                transition_id,
                transition_id,
                "A",
                "evidence_argv",
                context,
                variant_plan_environment(variant_a, context, transition_id),
                controlled=True,
                store_path=store_path,
                profile_track=profile_track,
            )
        )
    return plan


def validate_transition_config_authority(
    config: Mapping[str, Any] | None,
    expected: Sequence[Mapping[str, Any]],
    problems: Problems,
) -> None:
    observed = config.get("smoke_transitions") if isinstance(config, Mapping) else None
    authority = [
        {
            "id": item.get("id"),
            "variant": item.get("variant"),
            "argv": item.get("argv"),
        }
        for item in expected
    ]
    if observed != authority:
        problems.add("config smoke_transitions differ from reconstructed authority")


def validate_toolchain(
    value: Any, context: str, problems: Problems, *, synthetic: bool = False
) -> bool:
    if not require_exact_keys(value, set(schema.TOOLCHAIN_FIELDS), context, problems):
        return False
    cargo = resolve_bound_file(value.get("cargo_path"), value.get("cargo_sha256"), f"{context} cargo", problems, expected_mode=0o555)
    rustc = resolve_bound_file(value.get("rustc_path"), value.get("rustc_sha256"), f"{context} rustc", problems, expected_mode=0o555)
    rustup = resolve_bound_file(value.get("rustup_path"), value.get("rustup_sha256"), f"{context} rustup", problems, expected_mode=0o555)
    resolve_bound_file(value.get("bwrap_path"), value.get("bwrap_sha256"), f"{context} bwrap", problems, expected_mode=0o555)
    resolve_bound_file(value.get("git_path"), value.get("git_sha256"), f"{context} git", problems, expected_mode=0o555)
    cargo_version = value.get("cargo_version_verbose")
    rustc_version = value.get("rustc_version_verbose")
    host = value.get("rustc_host")
    if not isinstance(host, str) or not host or "\n" in host:
        problems.add(f"{context} rustc host is invalid")
    if not isinstance(cargo_version, str) or "release: 1.97.0" not in cargo_version or f"host: {host}" not in cargo_version:
        problems.add(f"{context} cargo verbose identity is not frozen 1.97.0/host")
    if not isinstance(rustc_version, str) or "release: 1.97.0" not in rustc_version or f"host: {host}" not in rustc_version:
        problems.add(f"{context} rustc verbose identity is not frozen 1.97.0/host")
    for field in ("cargo_home_path", "rustup_home_path"):
        try:
            directory = Path(str(value.get(field))).resolve(strict=True)
            if not directory.is_dir():
                raise OSError("not a directory")
        except OSError as error:
            problems.add(f"{context} {field} invalid: {error}")
    toolchain_name = value.get("rustup_toolchain")
    if not isinstance(toolchain_name, str) or not toolchain_name:
        problems.add(f"{context} rustup toolchain is invalid")
    try:
        toolchain_root = (
            Path(str(value.get("rustup_home_path"))) / "toolchains" / str(toolchain_name) / "bin"
        ).resolve(strict=True)
    except OSError as error:
        if not synthetic:
            problems.add(f"{context} effective toolchain root invalid: {error}")
    else:
        if cargo is not None and cargo != toolchain_root / "cargo":
            problems.add(f"{context} cargo is not the exact rustup toolchain binary")
        if rustc is not None and rustc != toolchain_root / "rustc":
            problems.add(f"{context} rustc is not the exact rustup toolchain binary")
    return cargo is not None and rustc is not None and rustup is not None


def validate_filesystem_admission(
    value: Any, context: str, problems: Problems, *, synthetic: bool
) -> None:
    if not require_exact_keys(
        value, set(schema.FILESYSTEM_ADMISSION_FIELDS), context, problems
    ):
        return
    exact = {
        "schema": schema.FILESYSTEM_ADMISSION_SCHEMA,
        "filesystem": schema.REQUIRED_FILESYSTEM_TYPE,
        "minimum_available_bytes": schema.MIN_FREE_BYTES,
        "minimum_available_inodes": schema.MIN_FREE_INODES,
    }
    for field, expected in exact.items():
        if value.get(field) != expected:
            problems.add(f"{context} {field} mismatch")
    checked_path: Path | None = None
    try:
        checked_path = Path(str(value.get("checked_path"))).resolve(strict=True)
        if not checked_path.is_dir():
            raise OSError("not a directory")
    except OSError as error:
        problems.add(f"{context} checked path invalid: {error}")
    for field, floor in (
        ("available_bytes", schema.MIN_FREE_BYTES),
        ("available_inodes", schema.MIN_FREE_INODES),
    ):
        observed = value.get(field)
        if not isinstance(observed, int) or isinstance(observed, bool) or observed < floor:
            problems.add(f"{context} {field} is below the exact floor")
    if not synthetic and checked_path is not None:
        live_filesystem = problems.capture(
            f"{context} live filesystem", lambda: live_filesystem_identity(checked_path)
        )
        if isinstance(live_filesystem, dict) and live_filesystem.get("filesystem_type") != schema.REQUIRED_FILESYSTEM_TYPE:
            problems.add(f"{context} live filesystem is not ext4")
        observed = problems.capture(f"{context} live capacity", lambda: os.statvfs(checked_path))
        if observed is not None and (
            observed.f_bavail * observed.f_frsize < schema.MIN_FREE_BYTES
            or observed.f_favail < schema.MIN_FREE_INODES
        ):
            problems.add(f"{context} live capacity is below the exact floor")


def validate_tools_manifest(
    value: Any,
    claimed_sha256: Any,
    context: str,
    problems: Problems,
) -> dict[str, Any] | None:
    if not require_exact_keys(
        value, set(schema.TOOLS_MANIFEST_FIELDS), context, problems
    ):
        return None
    observed_sha256 = hashlib.sha256(canonical_json_bytes(value)).hexdigest()
    if not is_sha256(claimed_sha256) or claimed_sha256 != observed_sha256:
        problems.add(f"{context} canonical digest mismatch")
    if value.get("schema") != schema.TOOLS_MANIFEST_SCHEMA:
        problems.add(f"{context} schema mismatch")
    allowlist = value.get("comm_allowlist")
    if allowlist != schema.expected_comm_allowlist():
        problems.add(f"{context} comm allowlist differs from exact authority")
    tools = value.get("tools")
    if not isinstance(tools, dict) or set(tools) != set(schema.PREPARED_TOOL_NAMES):
        problems.add(f"{context} tool names are not exact")
    else:
        for name, binding in tools.items():
            binding_context = f"{context} tool {name}"
            if not require_exact_keys(
                binding, set(schema.TOOL_BINDING_FIELDS), binding_context, problems
            ):
                continue
            path = resolve_bound_file(
                binding.get("path"), binding.get("sha256"), binding_context, problems,
                expected_mode=0o555,
            )
            if path is not None:
                mode = stat.S_IMODE(path.stat().st_mode)
                if binding.get("executable_mode") != 0o555 or mode != 0o555:
                    problems.add(f"{binding_context} mode differs from exact 0555")
            if binding.get("comm") != schema.PREPARED_TOOL_COMMS.get(name):
                problems.add(f"{binding_context} comm differs from exact role authority")
    support = value.get("support_files")
    if not isinstance(support, dict) or set(support) != set(
        schema.PREPARED_SUPPORT_FILE_NAMES
    ):
        problems.add(f"{context} support names are not exact")
    else:
        for name, binding in support.items():
            binding_context = f"{context} support {name}"
            if not require_exact_keys(
                binding, set(schema.SUPPORT_FILE_FIELDS), binding_context, problems
            ):
                continue
            path = resolve_bound_file(
                binding.get("path"), binding.get("sha256"), binding_context, problems
            )
            if path is not None:
                mode = stat.S_IMODE(path.stat().st_mode)
                if binding.get("mode") != 0o444 or mode != 0o444:
                    problems.add(f"{binding_context} mode differs from exact 0444")
    return value


def validate_source_approval(
    approval: dict[str, Any] | None,
    config: dict[str, Any] | None,
    protocol_sha256: str,
    repo: Path,
    problems: Problems,
    *,
    synthetic: bool,
) -> None:
    if approval is None or not require_exact_keys(approval, SOURCE_APPROVAL_FIELDS, "source approval", problems):
        return
    for field, expected in {
        "schema": schema.SOURCE_APPROVAL_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": protocol_sha256,
        "status": "approved",
    }.items():
        if approval.get(field) != expected:
            problems.add(f"source approval {field} mismatch")
    if config is not None and approval.get("review_id") != config.get("review_id"):
        problems.add("source approval review id differs from config")
    parse_timestamp(approval.get("reviewed_at"), "source approval reviewed_at", problems)
    for field in ("tooling_commit", "tooling_tree"):
        if not is_git_id(approval.get(field)):
            problems.add(f"source approval {field} invalid")
    if config is not None and (
        approval.get("tooling_commit"), approval.get("tooling_tree")
    ) != (config.get("tooling_commit"), config.get("tooling_tree")):
        problems.add("source approval tooling identity differs from config")
    if not is_sha256(approval.get("shared_manifest_sha256")):
        problems.add("source approval shared manifest hash invalid")
    validate_toolchain(
        approval.get("toolchain"), "source approval toolchain", problems,
        synthetic=synthetic,
    )
    validate_filesystem_admission(
        approval.get("filesystem_admission"), "source approval filesystem admission",
        problems, synthetic=synthetic,
    )
    if approval.get("comm_allowlist") != schema.expected_comm_allowlist():
        problems.add("source approval comm allowlist differs from exact shared authority")
    tools_manifest = validate_tools_manifest(
        approval.get("tools_manifest"),
        approval.get("tools_manifest_sha256"),
        "source approval tools manifest",
        problems,
    )
    if (
        tools_manifest is not None
        and tools_manifest.get("comm_allowlist") != approval.get("comm_allowlist")
    ):
        problems.add("source approval tools manifest comm allowlist differs")

    variants = approval.get("variants")
    if not isinstance(variants, dict) or set(variants) != set(schema.VARIANTS):
        problems.add("source approval variant keys are not exact")
    else:
        for variant in schema.VARIANTS:
            item = variants[variant]
            context = f"source approval variant {variant}"
            if not require_exact_keys(item, SOURCE_VARIANT_FIELDS, context, problems):
                continue
            binding = schema.VARIANT_SOURCE_BINDINGS[variant]
            if item.get("product_commit") != binding["commit"] or item.get("product_tree") != binding["tree"]:
                problems.add(f"{context} source binding mismatch")
            expected_kind = "bare" if variant == "B" else "public"
            expected_surface = "raw-numeric" if variant == "B" else "public-event-store"
            if item.get("binary_kind") != expected_kind or item.get("timed_surface") != expected_surface:
                problems.add(f"{context} surface mismatch")
            if item.get("correctness_oracle_mode") is not (variant != "B"):
                problems.add(f"{context} correctness oracle capability mismatch")
            expected_role_lifetime: Any = (
                schema.PROFILE_C_ROLE_LIFETIME_CONTRACT
                if variant == "C"
                else "not_applicable"
            )
            if item.get("profile_role_lifetime") != expected_role_lifetime:
                problems.add(f"{context} profile role lifetime differs")
            if (
                item.get("trace_path_marker_templates")
                != schema.expected_trace_path_marker_templates(variant)
            ):
                problems.add(f"{context} trace path marker templates differ")
            for field in ("adapter_sha256", "cargo_lock_sha256", "overlay_manifest_sha256"):
                if not is_sha256(item.get(field)):
                    problems.add(f"{context} {field} invalid")
            allowed = item.get("allowed_overlay_paths")
            if not isinstance(allowed, list) or not allowed or allowed != sorted(allowed) or len(set(allowed)) != len(allowed):
                problems.add(f"{context} overlay allowlist invalid")
            lock_resolution = item.get("lock_resolution")
            if require_exact_keys(lock_resolution, set(schema.LOCK_RESOLUTION_FIELDS), f"{context} lock resolution", problems):
                argv = lock_resolution.get("argv")
                if lock_resolution.get("exit_status") != 0 or not isinstance(argv, list) or "--offline" not in argv:
                    problems.add(f"{context} lock resolution is not successful offline")
                if not argv or argv[0] != approval.get("toolchain", {}).get("cargo_path"):
                    problems.add(f"{context} lock resolution did not use approved cargo path")
                cwd = lock_resolution.get("cwd")
                if not isinstance(cwd, str) or not Path(cwd).is_absolute():
                    problems.add(f"{context} lock resolution cwd is not absolute")
                else:
                    validate_cargo_config_search(
                        lock_resolution.get("cargo_config_search"), Path(cwd),
                        approval.get("toolchain", {}),
                        f"{context} Cargo config search", problems,
                    )
                if lock_resolution.get("toolchain") != approval.get("toolchain"):
                    problems.add(f"{context} lock resolution toolchain differs from approval")
                expected_environment = problems.capture(
                    f"{context} derive sanitized Cargo environment",
                    lambda: schema.sanitized_cargo_environment(approval.get("toolchain", {})),
                )
                if lock_resolution.get("environment") != expected_environment:
                    problems.add(f"{context} lock resolution environment is not exact sanitized map")
                for field in ("stdout_sha256", "stderr_sha256"):
                    payload = lock_resolution.get(field.removesuffix("_sha256"))
                    if not isinstance(payload, str) or hashlib.sha256(payload.encode()).hexdigest() != lock_resolution.get(field):
                        problems.add(f"{context} lock resolution {field} mismatch")
            current_attempt = item.get("current_lock_attempt")
            if variant in {"A", "B"}:
                if current_attempt is not None:
                    problems.add(f"{context} current lock attempt must be null")
            elif require_exact_keys(
                current_attempt, set(schema.LOCK_RESOLUTION_FIELDS),
                f"{context} current lock attempt", problems,
            ):
                attempt_context = f"{context} current lock attempt"
                argv = current_attempt.get("argv")
                cwd = current_attempt.get("cwd")
                if not isinstance(argv, list) or not argv or argv[0] != approval.get("toolchain", {}).get("cargo_path") or "--offline" not in argv:
                    problems.add(f"{attempt_context} argv is not exact offline Cargo")
                if not isinstance(current_attempt.get("exit_status"), int) or isinstance(current_attempt.get("exit_status"), bool):
                    problems.add(f"{attempt_context} exit status invalid")
                if not isinstance(cwd, str) or not Path(cwd).is_absolute():
                    problems.add(f"{attempt_context} cwd invalid")
                else:
                    validate_cargo_config_search(
                        current_attempt.get("cargo_config_search"), Path(cwd),
                        approval.get("toolchain", {}),
                        f"{attempt_context} Cargo config search", problems,
                    )
                if current_attempt.get("toolchain") != approval.get("toolchain"):
                    problems.add(f"{attempt_context} toolchain differs")
                if current_attempt.get("environment") != schema.sanitized_cargo_environment(approval.get("toolchain", {})):
                    problems.add(f"{attempt_context} environment differs")
                for field in ("stdout_sha256", "stderr_sha256"):
                    payload = current_attempt.get(field.removesuffix("_sha256"))
                    if not isinstance(payload, str) or hashlib.sha256(payload.encode()).hexdigest() != current_attempt.get(field):
                        problems.add(f"{attempt_context} {field} mismatch")
            if config is not None and item.get("cargo_lock_sha256") != config.get("lock_hashes", {}).get(variant):
                problems.add(f"{context} lock differs from config")


def validate_manifest(
    path_value: Any,
    claimed_sha256: Any,
    context: str,
    problems: Problems,
    *,
    expected_root: Path | None = None,
    replay_files: bool = False,
) -> dict[str, Any] | None:
    path = resolve_bound_file(path_value, claimed_sha256, context, problems)
    if path is None:
        return None
    manifest = read_canonical_object(path, context, problems)
    if manifest is None:
        return None
    if not require_exact_keys(manifest, {"schema", "protocol", "root", "entries"}, context, problems):
        return manifest
    if manifest.get("schema") != schema.FILE_MANIFEST_SCHEMA or manifest.get("protocol") != schema.PROTOCOL:
        problems.add(f"{context} schema/protocol mismatch")
    if expected_root is not None and manifest.get("root") != str(expected_root):
        problems.add(f"{context} root mismatch")
    entries = manifest.get("entries")
    if not isinstance(entries, list):
        problems.add(f"{context} entries is not a list")
        return manifest
    previous = ""
    observed_paths: set[str] = set()
    for ordinal, entry in enumerate(entries, start=1):
        label = f"{context} entry {ordinal}"
        if not require_exact_keys(entry, {"path", "mode", "bytes", "sha256"}, label, problems):
            continue
        relative = entry.get("path")
        if not isinstance(relative, str) or not relative or relative.startswith("/") or ".." in Path(relative).parts:
            problems.add(f"{label} path is unsafe")
            continue
        if relative <= previous:
            problems.add(f"{context} paths are not strictly sorted")
        previous = relative
        observed_paths.add(relative)
        if entry.get("mode") not in {"0444", "0555"}:
            problems.add(f"{label} mode is not read-only")
        if not isinstance(entry.get("bytes"), int) or isinstance(entry.get("bytes"), bool) or entry["bytes"] < 0:
            problems.add(f"{label} byte count invalid")
        if not is_sha256(entry.get("sha256")):
            problems.add(f"{label} hash invalid")
        if replay_files and expected_root is not None:
            file_path = expected_root / relative
            expected_mode = 0o555 if entry["mode"] == "0555" else 0o444
            try:
                snapshot = schema.snapshot_regular_file(
                    file_path,
                    within=expected_root,
                    expected_mode=expected_mode,
                )
            except (OSError, ValueError) as error:
                problems.add(f"{label} cannot replay file: {error}")
                continue
            if snapshot.size != entry["bytes"] or snapshot.sha256 != entry["sha256"]:
                problems.add(f"{label} bytes/hash mismatch")
    if replay_files and expected_root is not None:
        actual: set[str] = set()
        try:
            for directory, directories, files in os.walk(expected_root, followlinks=False):
                directory_path = Path(directory)
                for name in directories:
                    info = (directory_path / name).lstat()
                    if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o222:
                        problems.add(f"{context} directory is not read-only: {directory_path / name}")
                for name in files:
                    actual.add((directory_path / name).relative_to(expected_root).as_posix())
        except OSError as error:
            problems.add(f"{context} cannot walk materialization: {error}")
        if actual != observed_paths:
            problems.add(f"{context} filesystem membership differs from manifest")
    return manifest


def cargo_config_candidates(cwd: Path, cargo_home: Path) -> list[Path]:
    """Return Cargo's frozen cwd-to-root and Cargo-home config search order."""

    result: list[Path] = []
    seen: set[str] = set()
    for directory in (cwd, *cwd.parents):
        for candidate in (
            directory / ".cargo" / "config.toml",
            directory / ".cargo" / "config",
        ):
            text = str(candidate)
            if text not in seen:
                seen.add(text)
                result.append(candidate)
    for candidate in (cargo_home / "config.toml", cargo_home / "config"):
        text = str(candidate)
        if text not in seen:
            seen.add(text)
            result.append(candidate)
    return result


def validate_cargo_config_search(
    binding: Any,
    cwd: Path,
    toolchain: Mapping[str, Any],
    context: str,
    problems: Problems,
) -> None:
    if not require_exact_keys(
        binding, set(schema.FILE_BINDING_FIELDS), f"{context} binding", problems
    ):
        return
    path = resolve_bound_file(
        binding.get("path"), binding.get("sha256"), context, problems
    )
    if path is None:
        return
    manifest = read_canonical_object(path, context, problems)
    if manifest is None or not require_exact_keys(
        manifest, set(schema.CARGO_CONFIG_SEARCH_FIELDS), context, problems
    ):
        return
    try:
        exact_cwd = cwd.resolve(strict=True)
        cargo_home = Path(str(toolchain.get("cargo_home_path"))).resolve(strict=True)
    except OSError as error:
        problems.add(f"{context} root resolution failed: {error}")
        return
    if manifest.get("schema") != schema.CARGO_CONFIG_SEARCH_SCHEMA:
        problems.add(f"{context} schema mismatch")
    if manifest.get("cwd") != str(exact_cwd) or manifest.get("cargo_home_path") != str(cargo_home):
        problems.add(f"{context} cwd/Cargo-home binding mismatch")
    entries = manifest.get("entries")
    candidates = cargo_config_candidates(exact_cwd, cargo_home)
    if not isinstance(entries, list) or len(entries) != len(candidates):
        problems.add(f"{context} candidate cardinality mismatch")
        return
    for ordinal, (entry, candidate) in enumerate(zip(entries, candidates), start=1):
        entry_context = f"{context} entry {ordinal}"
        if not require_exact_keys(
            entry, set(schema.CARGO_CONFIG_SEARCH_ENTRY_FIELDS), entry_context, problems
        ):
            continue
        if entry.get("path") != str(candidate):
            problems.add(f"{entry_context} path/order mismatch")
        exists = candidate.exists() or candidate.is_symlink()
        if exists:
            if entry.get("status") != "present" or not is_sha256(entry.get("sha256")):
                problems.add(f"{entry_context} present binding invalid")
                continue
            resolve_bound_file(
                str(candidate), entry.get("sha256"), entry_context, problems
            )
        elif entry.get("status") != "absent" or entry.get("sha256") is not None:
            problems.add(f"{entry_context} absence binding invalid")


def validate_completed_child(
    child: Any,
    context: str,
    problems: Problems,
    *,
    expected_argv: Sequence[str] | None = None,
) -> None:
    fields = {
        "argv",
        "cwd",
        "pid",
        "start_ticks",
        "started_at",
        "started_monotonic_ns",
        "completed_at",
        "completed_monotonic_ns",
        "exit_status",
        "waited_pid",
        "timed_out",
        "process_group_absent",
        "reaping",
        "output_path",
        "output_sha256",
    }
    if not require_exact_keys(child, fields, context, problems):
        return
    if expected_argv is not None and child.get("argv") != list(expected_argv):
        problems.add(f"{context} argv mismatch")
    for field in ("pid", "start_ticks", "started_monotonic_ns", "completed_monotonic_ns", "waited_pid"):
        if not isinstance(child.get(field), int) or isinstance(child.get(field), bool) or child[field] <= 0:
            problems.add(f"{context} {field} invalid")
    if child.get("waited_pid") != child.get("pid"):
        problems.add(f"{context} waited_pid mismatch")
    if child.get("exit_status") != 0 or child.get("timed_out") is not False or child.get("process_group_absent") is not True:
        problems.add(f"{context} completion/reaping status invalid")
    if child.get("completed_monotonic_ns", 0) < child.get("started_monotonic_ns", 0):
        problems.add(f"{context} monotonic chronology invalid")
    start = parse_timestamp(child.get("started_at"), f"{context} started_at", problems)
    end = parse_timestamp(child.get("completed_at"), f"{context} completed_at", problems)
    if start is not None and end is not None and end < start:
        problems.add(f"{context} wall chronology invalid")
    reaping = child.get("reaping")
    if not require_exact_keys(reaping, {"pid", "start_ticks", "status"}, f"{context} reaping", problems):
        reaping = None
    if reaping is not None and (
        reaping.get("pid") != child.get("pid")
        or reaping.get("start_ticks") != child.get("start_ticks")
        or reaping.get("status") != "absent"
    ):
        problems.add(f"{context} reaping identity/status mismatch")
    resolve_bound_file(child.get("output_path"), child.get("output_sha256"), f"{context} output", problems)


def validate_prepared_artifacts(
    prepared: dict[str, Any] | None,
    prepared_path: Path,
    output_dir: Path,
    approval: dict[str, Any] | None,
    config: dict[str, Any] | None,
    source_approval_path: Path,
    source_approval_sha256: str,
    config_path: Path,
    config_sha256: str,
    protocol_sha256: str,
    lease: Mapping[str, Any] | None,
    problems: Problems,
    *,
    synthetic: bool,
) -> tuple[dict[str, Path], dict[str, Path]]:
    binaries: dict[str, Path] = {}
    attempt_inputs: dict[str, Path] = {}
    if prepared is None or not require_exact_keys(prepared, PREPARED_FIELDS, "prepared artifacts", problems):
        return binaries, attempt_inputs
    exact = {
        "schema": schema.PREPARED_ARTIFACTS_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": protocol_sha256,
        "build_order": list(schema.VARIANTS),
    }
    for field, expected in exact.items():
        if prepared.get(field) != expected:
            problems.add(f"prepared artifacts {field} mismatch")
    prepared_created_at = parse_timestamp(
        prepared.get("created_at"), "prepared artifacts created_at", problems
    )
    if not isinstance(prepared.get("created_monotonic_ns"), int) or prepared["created_monotonic_ns"] <= 0:
        problems.add("prepared artifacts monotonic timestamp invalid")
    if config is not None and (
        prepared.get("tooling_commit"), prepared.get("tooling_tree")
    ) != (config.get("tooling_commit"), config.get("tooling_tree")):
        problems.add("prepared tooling identity differs from config")
    prepared_root: Path | None = None
    original_prepared_snapshot: schema.FileSnapshot | None = None
    attempt_prepared_snapshot = resolve_bound_file(
        str(prepared_path),
        snapshot_sha256(prepared_path, problems),
        "attempt prepared artifacts copy",
        problems,
    )
    claim_binding = prepared.get("single_use_claim")
    if require_exact_keys(claim_binding, {"path"}, "prepared single-use claim binding", problems):
        claim_path = Path(str(claim_binding.get("path")))
        claim = read_canonical_object(claim_path, "prepared single-use claim", problems)
        validate_exact_mode(claim_path, 0o444, "prepared single-use claim", problems)
        try:
            claims_directory = claim_path.parent.resolve(strict=True)
            prepared_root = claims_directory.parent.resolve(strict=True)
            if (
                claims_directory.name != "claims"
                or claim_path != claims_directory / "single-use-claim.json"
            ):
                problems.add("prepared single-use claim is outside dedicated claims directory")
            if stat.S_IMODE(claims_directory.stat().st_mode) != 0o700:
                problems.add("prepared claims directory mode is not exact 0700")
            if stat.S_IMODE(prepared_root.stat().st_mode) != 0o555:
                problems.add("prepared bundle root mode is not exact 0555")
        except OSError as error:
            problems.add(f"prepared claim/root mode replay failed: {error}")
        if claim is not None and require_exact_keys(
            claim,
            set(schema.PREPARED_CLAIM_FIELDS),
            "prepared single-use claim",
            problems,
        ):
            expected_original_path = (
                prepared_root / "prepared-artifacts.json"
                if prepared_root is not None
                else None
            )
            if (
                claim.get("schema") != schema.PREPARED_CLAIM_SCHEMA
                or claim.get("protocol") != schema.PROTOCOL
                or expected_original_path is None
                or claim.get("prepared_artifacts_path")
                != str(expected_original_path)
            ):
                problems.add("prepared single-use claim original artifact path differs")
            else:
                original_prepared_snapshot = resolve_bound_file(
                    claim.get("prepared_artifacts_path"),
                    claim.get("prepared_artifacts_sha256"),
                    "original prepared artifacts",
                    problems,
                    within=prepared_root,
                )
            if (
                original_prepared_snapshot is not None
                and attempt_prepared_snapshot is not None
                and original_prepared_snapshot.data != attempt_prepared_snapshot.data
            ):
                problems.add("attempt/original prepared artifacts bytes differ")
            if (
                original_prepared_snapshot is not None
                and attempt_prepared_snapshot is not None
                and (
                    original_prepared_snapshot.device,
                    original_prepared_snapshot.inode,
                )
                == (
                    attempt_prepared_snapshot.device,
                    attempt_prepared_snapshot.inode,
                )
            ):
                problems.add(
                    "attempt/original prepared artifacts are hardlink aliases"
                )
            if claim.get("output_dir") != str(output_dir):
                problems.add("prepared single-use claim output differs")
            if config is None or claim.get("attempt_nonce") != config.get(
                "attempt_nonce"
            ):
                problems.add("prepared single-use claim attempt nonce differs")
            if not isinstance(lease, Mapping) or claim.get(
                "lease_nonce"
            ) != lease.get("nonce"):
                problems.add("prepared single-use claim lease nonce differs")
            claimed_at = parse_timestamp(
                claim.get("claimed_at"), "prepared single-use claim claimed_at", problems
            )
            lease_acquired_at = (
                parse_timestamp(
                    lease.get("acquired_at"), "claim lease acquired_at", problems
                )
                if isinstance(lease, Mapping)
                else None
            )
            chronology = (
                prepared.get("created_monotonic_ns"),
                lease.get("acquired_monotonic_ns")
                if isinstance(lease, Mapping)
                else None,
                claim.get("claimed_monotonic_ns"),
            )
            if (
                prepared_created_at is None
                or lease_acquired_at is None
                or claimed_at is None
                or not prepared_created_at <= lease_acquired_at <= claimed_at
                or not all(
                    isinstance(value, int)
                    and not isinstance(value, bool)
                    and value > 0
                    for value in chronology
                )
                or not chronology[0] < chronology[1] < chronology[2]
            ):
                problems.add("prepared single-use claim chronology differs")

    approval_binding = prepared.get("source_approval")
    if require_exact_keys(
        approval_binding,
        {"path", "sha256"},
        "prepared source approval binding",
        problems,
    ):
        original_approval_path = Path(str(approval_binding.get("path")))
        expected_original_approval = (
            prepared_root.joinpath(*schema.PREPARED_SOURCE_APPROVAL_RELATIVE_PATH)
            if prepared_root is not None
            else None
        )
        if (
            expected_original_approval is None
            or original_approval_path != expected_original_approval
        ):
            problems.add("prepared original source approval path differs")
        original_approval_snapshot = resolve_bound_file(
            approval_binding.get("path"),
            approval_binding.get("sha256"),
            "original prepared source approval",
            problems,
            within=prepared_root,
        )
        attempt_approval_snapshot = resolve_bound_file(
            str(source_approval_path),
            source_approval_sha256,
            "attempt source approval copy",
            problems,
        )
        if approval_binding.get("sha256") != source_approval_sha256:
            problems.add("attempt/original source approval hash differs")
        if (
            original_approval_snapshot is not None
            and attempt_approval_snapshot is not None
            and original_approval_snapshot.data != attempt_approval_snapshot.data
        ):
            problems.add("attempt/original source approval bytes differ")
        if (
            original_approval_snapshot is not None
            and attempt_approval_snapshot is not None
            and (
                original_approval_snapshot.device,
                original_approval_snapshot.inode,
            )
            == (
                attempt_approval_snapshot.device,
                attempt_approval_snapshot.inode,
            )
        ):
            problems.add("attempt/original source approval are hardlink aliases")
        if original_approval_snapshot is not None:
            observed = read_canonical_object(
                original_approval_snapshot,
                "original prepared source approval",
                problems,
            )
            if observed != approval:
                problems.add("attempt/original source approval objects differ")
    approved_tools_manifest = (
        approval.get("tools_manifest") if isinstance(approval, dict) else None
    )
    tools_manifest_binding = prepared.get("tools_manifest")
    if require_exact_keys(
        tools_manifest_binding,
        set(schema.TOOLS_MANIFEST_BINDING_FIELDS),
        "prepared tools manifest binding",
        problems,
    ):
        if tools_manifest_binding.get("mode") != 0o444:
            problems.add("prepared tools manifest binding mode differs from exact 0444")
        tools_manifest_path = resolve_bound_file(
            tools_manifest_binding.get("path"),
            tools_manifest_binding.get("sha256"),
            "prepared tools manifest",
            problems,
            within=prepared_root,
        )
        if tools_manifest_path is not None:
            validate_exact_mode(
                tools_manifest_path, 0o444, "prepared tools manifest", problems
            )
            if prepared_root is not None and tools_manifest_path != (
                prepared_root / "bindings" / "tools-manifest.json"
            ).resolve():
                problems.add("prepared tools manifest path differs from exact layout")
            observed_manifest = read_canonical_object(
                tools_manifest_path, "prepared tools manifest", problems
            )
            if observed_manifest != approved_tools_manifest:
                problems.add("prepared tools manifest differs from approved canonical object")
        if approval is not None and tools_manifest_binding.get("sha256") != approval.get(
            "tools_manifest_sha256"
        ):
            problems.add("prepared tools manifest hash differs from source approval")
    allowlist = prepared.get("comm_allowlist")
    if not isinstance(allowlist, list) or allowlist != sorted(allowlist) or len(set(allowlist)) != len(allowlist) or not all(isinstance(item, str) and 0 < len(item.encode()) <= 15 for item in allowlist):
        problems.add("prepared comm allowlist invalid")
    if allowlist != schema.expected_comm_allowlist():
        problems.add("prepared comm allowlist differs from exact shared authority")
    if approval is not None and allowlist != approval.get("comm_allowlist"):
        problems.add("prepared comm allowlist differs from source approval")
    if isinstance(approved_tools_manifest, dict) and allowlist != approved_tools_manifest.get(
        "comm_allowlist"
    ):
        problems.add("prepared comm allowlist differs from approved tools manifest")
    tools = prepared.get("tools")
    required_tools = set(schema.PREPARED_TOOL_NAMES)
    if not isinstance(tools, dict) or set(tools) != required_tools:
        problems.add(
            "prepared tool bindings are not exact; "
            f"missing={sorted(required_tools - set(tools or {}))} "
            f"extra={sorted(set(tools or {}) - required_tools)}"
        )
    else:
        for name, binding in tools.items():
            context = f"prepared tool {name}"
            if not require_exact_keys(binding, set(schema.TOOL_BINDING_FIELDS), context, problems):
                continue
            path = resolve_bound_file(
                binding.get("path"), binding.get("sha256"), context, problems,
                expected_mode=0o555,
            )
            if path is not None:
                mode = stat.S_IMODE(path.stat().st_mode)
                if binding.get("executable_mode") != mode or mode & 0o222 or not mode & 0o111:
                    problems.add(f"{context} mode is not immutable executable")
            if not isinstance(binding.get("comm"), str) or binding["comm"] not in allowlist:
                problems.add(f"{context} comm is not allowlisted")
            if binding.get("comm") != schema.PREPARED_TOOL_COMMS.get(name):
                problems.add(f"{context} comm differs from exact role authority")
            approved_binding = (
                approved_tools_manifest.get("tools", {}).get(name, {})
                if isinstance(approved_tools_manifest, dict)
                else {}
            )
            for field in ("sha256", "executable_mode", "comm"):
                if binding.get(field) != approved_binding.get(field):
                    problems.add(f"{context} {field} differs from approved tools manifest")
        runtime_names = {
            "runner_runtime", "evaluator_runtime", "terminal_verifier_runtime",
            "strace_launcher_runtime",
        }
        runtime_paths = [tools[name].get("path") for name in runtime_names]
        runtime_comms = [tools[name].get("comm") for name in runtime_names]
        if len(set(runtime_paths)) != len(runtime_names) or len(set(runtime_comms)) != len(runtime_names):
            problems.add("prepared Python runtimes are not role-distinct paths/comms")
    support = prepared.get("support_files")
    required_support = set(schema.PREPARED_SUPPORT_FILE_NAMES)
    support_parents: set[Path] = set()
    if not isinstance(support, dict) or set(support) != required_support:
        problems.add(
            "prepared support file bindings are not exact; "
            f"missing={sorted(required_support - set(support or {}))} "
            f"extra={sorted(set(support or {}) - required_support)}"
        )
    else:
        for name, binding in support.items():
            context = f"prepared support file {name}"
            if not require_exact_keys(binding, set(schema.SUPPORT_FILE_FIELDS), context, problems):
                continue
            path = resolve_bound_file(binding.get("path"), binding.get("sha256"), context, problems)
            if path is not None:
                support_parents.add(path.parent)
                mode = stat.S_IMODE(path.stat().st_mode)
                if binding.get("mode") != mode or mode != 0o444:
                    problems.add(f"{context} mode is not exact read-only 0444")
            approved_binding = (
                approved_tools_manifest.get("support_files", {}).get(name, {})
                if isinstance(approved_tools_manifest, dict)
                else {}
            )
            for field in ("sha256", "mode"):
                if binding.get(field) != approved_binding.get(field):
                    problems.add(f"{context} {field} differs from approved tools manifest")
        if len(support_parents) != 1:
            problems.add("prepared support files do not share one directory")
        else:
            support_parent = next(iter(support_parents))
            try:
                if stat.S_IMODE(support_parent.stat().st_mode) != 0o555:
                    problems.add("prepared support directory mode is not exact 0555")
                expected_entries = {
                    Path(str(binding["path"])).name for binding in support.values()
                }
                observed_entries = {entry.name for entry in support_parent.iterdir()}
                if observed_entries != expected_entries:
                    problems.add(
                        "prepared support directory entries are not exact; "
                        f"missing={sorted(expected_entries - observed_entries)} "
                        f"extra={sorted(observed_entries - expected_entries)}"
                    )
            except (KeyError, OSError) as error:
                problems.add(f"prepared support directory replay failed: {error}")
    inputs = prepared.get("inputs")
    required_inputs = set(schema.PREPARED_INPUT_NAMES)
    if not isinstance(inputs, dict) or set(inputs) != required_inputs:
        problems.add(
            "prepared input bindings are not exact; "
            f"missing={sorted(required_inputs - set(inputs or {}))} "
            f"extra={sorted(set(inputs or {}) - required_inputs)}"
        )
    else:
        for name in schema.PREPARED_INPUT_NAMES:
            binding = inputs[name]
            context = f"prepared input {name}"
            if not require_exact_keys(
                binding, set(schema.PREPARED_INPUT_FIELDS), context, problems
            ):
                continue
            expected_sha256 = schema.PREPARED_INPUT_SHA256[name]
            if binding.get("sha256") != expected_sha256:
                problems.add(f"{context} hash differs from frozen authority")
            if binding.get("mode") != 0o444:
                problems.add(f"{context} mode differs from exact 0444")
            bound = resolve_bound_file(
                binding.get("path"), binding.get("sha256"), context, problems,
                within=prepared_root,
            )
            if bound is not None:
                validate_exact_mode(bound, 0o444, context, problems)
                try:
                    if stat.S_IMODE(bound.parent.stat().st_mode) != 0o555:
                        problems.add(f"{context} parent mode is not exact 0555")
                except OSError as error:
                    problems.add(f"{context} parent mode replay failed: {error}")
                if prepared_root is not None:
                    expected_source = (
                        prepared_root / schema.PREPARED_INPUT_RELATIVE_PATHS[name]
                    ).resolve()
                    if bound != expected_source:
                        problems.add(f"{context} path differs from prepared layout")
            attempt_path = output_dir / schema.PREPARED_INPUT_FILENAMES[name]
            copied = resolve_bound_file(
                str(attempt_path), expected_sha256, f"attempt input {name}",
                problems, within=output_dir,
            )
            if copied is not None:
                validate_exact_mode(copied, 0o444, f"attempt input {name}", problems)
                attempt_inputs[name] = copied
    toolchain = prepared.get("toolchain")
    validate_toolchain(
        toolchain, "prepared toolchain", problems, synthetic=synthetic
    )
    if not isinstance(toolchain, dict):
        toolchain = {}
    if approval is not None and toolchain != approval.get("toolchain"):
        problems.add("prepared toolchain differs from source approval")
    validate_filesystem_admission(
        prepared.get("filesystem_admission"),
        "prepared filesystem admission", problems, synthetic=synthetic,
    )

    variants = prepared.get("variants")
    if not isinstance(variants, dict) or set(variants) != set(schema.VARIANTS):
        problems.add("prepared artifact variant keys are not exact")
        return binaries, attempt_inputs
    previous_end = -1
    binary_hashes: set[str] = set()
    target_dirs: set[str] = set()
    nonces: set[str] = set()
    for variant in schema.VARIANTS:
        item = variants[variant]
        context = f"prepared variant {variant}"
        if not require_exact_keys(item, PREPARED_VARIANT_FIELDS, context, problems):
            continue
        if item.get("correctness_oracle_mode") is not (variant != "B"):
            problems.add(f"{context} correctness oracle capability mismatch")
        attestation = item.get("attestation")
        if not require_exact_keys(attestation, PREPARED_ATTESTATION_FIELDS, f"{context} attestation", problems):
            continue
        if attestation.get("toolchain") != toolchain:
            problems.add(f"{context} build toolchain differs from prepared toolchain")
        binding = schema.VARIANT_SOURCE_BINDINGS[variant]
        if attestation.get("source_commit") != binding["commit"] or attestation.get("source_tree") != binding["tree"]:
            problems.add(f"{context} source identity mismatch")
        if approval is not None:
            approved = approval.get("variants", {}).get(variant, {})
            if attestation.get("cargo_lock_sha256") != approved.get("cargo_lock_sha256"):
                problems.add(f"{context} lock differs from approval")
            if attestation.get("overlay_manifest_sha256") != approved.get("overlay_manifest_sha256"):
                problems.add(f"{context} overlay manifest differs from approval")
            if item.get("trace_path_marker_templates") != approved.get(
                "trace_path_marker_templates"
            ):
                problems.add(f"{context} trace marker templates differ from approval")
        if (
            item.get("trace_path_marker_templates")
            != schema.expected_trace_path_marker_templates(variant)
        ):
            problems.add(f"{context} trace marker templates differ from exact authority")
        for field in (
            "source_archive_sha256",
            "archive_manifest_sha256",
            "materialized_manifest_sha256",
            "cargo_lock_sha256",
            "cargo_lock_pre_sha256",
            "cargo_lock_post_sha256",
            "build_nonce",
            "contract_output_sha256",
            "build_log_sha256",
            "overlay_manifest_sha256",
        ):
            if not is_sha256(attestation.get(field)):
                problems.add(f"{context} {field} invalid")
        if not (
            attestation.get("cargo_lock_sha256")
            == attestation.get("cargo_lock_pre_sha256")
            == attestation.get("cargo_lock_post_sha256")
        ):
            problems.add(f"{context} lock changed during build")
        archive = resolve_bound_file(attestation.get("source_archive_path"), attestation.get("source_archive_sha256"), f"{context} source archive", problems)
        if archive is not None and archive.stat().st_size != attestation.get("source_archive_bytes"):
            problems.add(f"{context} source archive byte count mismatch")
        materialized_root: Path | None = None
        try:
            materialized_root = Path(attestation["materialized_root"]).resolve(strict=True)
            if not materialized_root.is_dir():
                raise ValueError("not directory")
        except (KeyError, OSError, ValueError) as error:
            problems.add(f"{context} materialized root invalid: {error}")
        validate_manifest(
            attestation.get("archive_manifest_path"),
            attestation.get("archive_manifest_sha256"),
            f"{context} archive manifest",
            problems,
        )
        if materialized_root is not None:
            validate_manifest(
                attestation.get("materialized_manifest_path"),
                attestation.get("materialized_manifest_sha256"),
                f"{context} materialized manifest",
                problems,
                expected_root=materialized_root,
                replay_files=not synthetic,
            )
        if attestation.get("materialized_manifest_pre_sha256") != attestation.get("materialized_manifest_post_sha256") or attestation.get("materialized_manifest_sha256") != attestation.get("materialized_manifest_post_sha256"):
            problems.add(f"{context} materialized manifest changed across build")
        if attestation.get("source_read_only") is not True:
            problems.add(f"{context} source is not attested read-only")
        resolve_bound_file(attestation.get("overlay_manifest_path"), attestation.get("overlay_manifest_sha256"), f"{context} overlay manifest", problems)
        lock_path = resolve_bound_file(attestation.get("cargo_lock_path"), attestation.get("cargo_lock_sha256"), f"{context} Cargo.lock", problems)
        if lock_path is not None and materialized_root is not None:
            try:
                lock_path.relative_to(materialized_root)
            except ValueError:
                problems.add(f"{context} Cargo.lock escapes materialization")
        if attestation.get("target_dir_was_absent") is not True:
            problems.add(f"{context} target directory was reused")
        build_argv = attestation.get("build_argv")
        if not isinstance(build_argv, list) or "--locked" not in build_argv or "--offline" not in build_argv:
            problems.add(f"{context} build is not --locked --offline")
        if not isinstance(build_argv, list) or "--unshare-net" not in build_argv or "--ro-bind" not in build_argv:
            problems.add(f"{context} sandbox lacks network/read-only controls")
        if not isinstance(build_argv, list) or not build_argv or build_argv[0] != toolchain.get("bwrap_path"):
            problems.add(f"{context} build did not use bound bwrap executable")
        if not isinstance(build_argv, list) or toolchain.get("cargo_path") not in build_argv:
            problems.add(f"{context} build argv omits approved cargo path")
        build_env = attestation.get("build_env")
        if not require_exact_keys(build_env, set(schema.BUILD_ENV_FIELDS), f"{context} build environment", problems):
            build_env = {}
        approved_variant = approval.get("variants", {}).get(variant, {}) if approval else {}
        expected_build_env = {
            **schema.sanitized_cargo_environment(toolchain),
            "ASTERISM_BUILD_PROTOCOL": schema.PROTOCOL,
            "ASTERISM_BUILD_PROTOCOL_SHA256": protocol_sha256,
            "ASTERISM_BUILD_TOOLING_COMMIT": prepared.get("tooling_commit"),
            "ASTERISM_BUILD_TOOLING_TREE": prepared.get("tooling_tree"),
            "ASTERISM_BUILD_VARIANT": variant,
            "ASTERISM_BUILD_PRODUCT_COMMIT": binding["commit"],
            "ASTERISM_BUILD_PRODUCT_TREE": binding["tree"],
            "ASTERISM_BUILD_ADAPTER_SHA256": approved_variant.get("adapter_sha256"),
            "ASTERISM_BUILD_BINARY_KIND": approved_variant.get("binary_kind"),
            "ASTERISM_BUILD_SHARED_MANIFEST_SHA256": approval.get("shared_manifest_sha256") if approval else None,
            "ASTERISM_BUILD_CARGO_LOCK_SHA256": attestation.get("cargo_lock_sha256"),
            "ASTERISM_BUILD_SOURCE_APPROVAL_SHA256": source_approval_sha256,
            "ASTERISM_BUILD_TIMED_SURFACE": approved_variant.get("timed_surface"),
            "ASTERISM_BUILD_NONCE": attestation.get("build_nonce"),
        }
        if build_env != expected_build_env:
            problems.add(f"{context} build environment differs from exact contract identity")
        if materialized_root is not None:
            validate_cargo_config_search(
                attestation.get("cargo_config_search"), materialized_root,
                toolchain, f"{context} Cargo config search", problems,
            )
        started = attestation.get("build_started_monotonic_ns")
        completed = attestation.get("build_completed_monotonic_ns")
        if not isinstance(started, int) or not isinstance(completed, int) or started <= previous_end or completed < started:
            problems.add(f"{context} build is not sequential")
        if isinstance(completed, int):
            previous_end = completed
        parse_timestamp(attestation.get("build_started_at"), f"{context} build_started_at", problems)
        parse_timestamp(attestation.get("build_completed_at"), f"{context} build_completed_at", problems)
        resolve_bound_file(attestation.get("build_log_path"), attestation.get("build_log_sha256"), f"{context} build log", problems)
        validate_completed_child(attestation.get("build_child"), f"{context} build child", problems, expected_argv=build_argv)
        validate_completed_child(attestation.get("contract_child"), f"{context} contract child", problems, expected_argv=item.get("contract_argv") if isinstance(item.get("contract_argv"), list) else None)
        contract_path = resolve_bound_file(attestation.get("contract_output_path"), attestation.get("contract_output_sha256"), f"{context} contract output", problems)
        contract = item.get("contract")
        if isinstance(contract, dict):
            problems.capture(f"{context} binary contract", lambda contract=contract: schema.validate_binary_contract(contract))
            if contract_path is not None:
                observed = read_canonical_object(contract_path, f"{context} contract output", problems)
                if observed != contract:
                    problems.add(f"{context} contract output differs from attestation")
            if config is not None:
                expected_contract = {
                    "protocol_sha256": protocol_sha256,
                    "tooling_commit": config.get("tooling_commit"),
                    "tooling_tree": config.get("tooling_tree"),
                    "variant": variant,
                    "product_commit": binding["commit"],
                    "product_tree": binding["tree"],
                    "cargo_lock_sha256": attestation.get("cargo_lock_sha256"),
                    "source_approval_sha256": source_approval_sha256,
                    "build_nonce": attestation.get("build_nonce"),
                }
                for field, expected in expected_contract.items():
                    if contract.get(field) != expected:
                        problems.add(f"{context} contract {field} mismatch")
        else:
            problems.add(f"{context} contract is not an object")
        binary_binding = item.get("binary")
        if not require_exact_keys(binary_binding, {"path", "sha256"}, f"{context} binary binding", problems):
            binary_binding = {}
        binary = resolve_bound_file(
            binary_binding.get("path"), binary_binding.get("sha256"),
            f"{context} binary", problems, expected_mode=0o555,
        )
        if binary is not None:
            binaries[variant] = binary
            try:
                mode = stat.S_IMODE(binary.stat().st_mode)
                if mode != item.get("executable_mode") or not mode & 0o111 or mode & 0o222:
                    problems.add(f"{context} binary mode is not immutable executable")
            except OSError as error:
                problems.add(f"{context} cannot stat binary: {error}")
        if not isinstance(item.get("comm"), str) or item["comm"] not in allowlist:
            problems.add(f"{context} comm is not allowlisted")
        if item.get("comm") != schema.VARIANT_COMMS[variant]:
            problems.add(f"{context} comm differs from exact variant authority")
        if not isinstance(item.get("contract_argv"), list) or not isinstance(item.get("evidence_argv"), list) or not item["contract_argv"] or not item["evidence_argv"] or item["contract_argv"][0] != str(binary) or item["evidence_argv"][0] != str(binary):
            problems.add(f"{context} executable argv binding invalid")
        for env_field in ("contract_env", "evidence_env"):
            env = item.get(env_field)
            if not isinstance(env, dict) or not all(isinstance(key, str) and isinstance(value, str) for key, value in env.items()):
                problems.add(f"{context} {env_field} invalid")
        if item.get("contract_env") != schema.sanitized_contract_environment(toolchain):
            problems.add(f"{context} contract environment is not exact sanitized map")
        problems.capture(
            f"{context} trace marker environment",
            lambda variant=variant, item=item: schema.validate_trace_marker_environment(
                variant, item.get("evidence_env")
            ),
        )
        binary_hashes.add(str(binary_binding.get("sha256")))
        target_dirs.add(str(attestation.get("target_dir")))
        nonces.add(str(attestation.get("build_nonce")))
    if len(binary_hashes) != 4 or len(target_dirs) != 4 or len(nonces) != 4:
        problems.add("prepared binaries, targets, or nonces are not unique")
    return binaries, attempt_inputs


def validate_live_python_bindings(
    prepared: dict[str, Any] | None,
    problems: Problems,
    *,
    synthetic: bool,
) -> None:
    if synthetic or prepared is None:
        return
    tools = prepared.get("tools", {})
    support = prepared.get("support_files", {})
    expected_script = support.get("evaluator", {}).get("path")
    if expected_script != str(Path(__file__).resolve()) or sys.argv[0] != expected_script:
        problems.add("live evaluator script is not the prepared support file")
    for context, proc_path, binding_name in (
        ("evaluator runtime", Path("/proc/self/exe"), "evaluator_runtime"),
        ("runner runtime", Path("/proc") / str(os.getppid()) / "exe", "runner_runtime"),
    ):
        binding = tools.get(binding_name, {})
        try:
            observed = proc_path.resolve(strict=True)
        except OSError as error:
            problems.add(f"cannot resolve live {context}: {error}")
            continue
        if str(observed) != binding.get("path"):
            problems.add(f"live {context} path differs from prepared binding")
        else:
            digest = problems.capture(
                f"snapshot live {context}",
                lambda observed=observed: snapshot_sha256(
                    observed, problems, expected_mode=None
                ),
            )
            if digest != binding.get("sha256"):
                problems.add(f"live {context} hash differs from prepared binding")


def read_csv_tracks(
    output_dir: Path,
    config: dict[str, Any] | None,
    protocol_sha256: str,
    attempt_nonce: str | None,
    problems: Problems,
) -> dict[str, list[dict[str, Any]]]:
    tracks: dict[str, list[dict[str, Any]]] = {}
    if config is None:
        return tracks
    store_ids: set[str] = set()
    copy_ids: set[str] = set()
    for track, filename in schema.CSV_FILENAMES.items():
        path = output_dir / filename
        rows: list[dict[str, Any]] = []
        try:
            snapshot = problems.recalled(path)
            if snapshot is None:
                snapshot = problems.remember(
                    schema.snapshot_regular_file(path, expected_mode=0o444)
                )
            with snapshot.open("r", encoding="ascii", newline="") as handle:
                reader = csv.DictReader(handle)
                if reader.fieldnames != list(schema.CSV_FIELDS_BY_TRACK[track]):
                    problems.add(f"{filename} header is not exact")
                for ordinal, raw in enumerate(reader, start=1):
                    parsed = problems.capture(
                        f"parse {filename} row {ordinal}",
                        lambda raw=raw, ordinal=ordinal: schema.parse_csv_row(
                            track, raw, f"{filename} row {ordinal}"
                        ),
                    )
                    if parsed is not None:
                        rows.append(parsed)
        except (OSError, UnicodeError, csv.Error) as error:
            problems.add(f"cannot read {filename}: {error}")
            tracks[track] = rows
            continue
        if len(rows) != schema.EXPECTED_CARDINALITY[track]:
            problems.add(
                f"{filename} cardinality {len(rows)} != {schema.EXPECTED_CARDINALITY[track]}"
            )
        expected = problems.capture(
            f"compute {track} order", lambda track=track: schema.expected_order(config, track)
        )
        if expected is not None:
            problems.capture(
                f"validate {track} order", lambda rows=rows, expected=expected: schema.ensure_exact_sequence(rows, expected)
            )
        for ordinal, row in enumerate(rows, start=1):
            if row.get("protocol_sha256") != protocol_sha256:
                problems.add(f"{filename} row {ordinal} protocol hash mismatch")
            if row.get("attempt_nonce") != attempt_nonce:
                problems.add(f"{filename} row {ordinal} attempt nonce mismatch")
            if track in {"primary", "new_names", "fairness"}:
                store_id = row["store_id"]
                if store_id in store_ids:
                    problems.add(f"duplicate store identity {store_id}")
                store_ids.add(store_id)
            if track == "reopen":
                copy_id = row["copy_id"]
                if copy_id in copy_ids:
                    problems.add(f"duplicate reopen copy identity {copy_id}")
                copy_ids.add(copy_id)
        tracks[track] = rows
    return tracks


def expand_argv_template(
    template: Sequence[str],
    *,
    binary: Path,
    track: str,
    row_ordinal: int,
    variant: str,
    config_path: Path,
    output_dir: Path,
    profile_adapter: Path,
) -> list[str]:
    values = {
        "binary": str(binary),
        "track": track,
        "row_ordinal": str(row_ordinal),
        "variant": variant,
        "config": str(config_path),
        "output_dir": str(output_dir),
        "profile_adapter": str(profile_adapter),
    }
    return [item.format_map(values) for item in template]


def validate_row_children(
    records: list[dict[str, Any]],
    output_dir: Path,
    config: dict[str, Any] | None,
    config_path: Path,
    binaries: Mapping[str, Path],
    profile_adapter: Path,
    prepared: Mapping[str, Any] | None,
    prepared_path: Path,
    prepared_sha256: str,
    source_approval_path: Path,
    source_approval_sha256: str,
    expected_transitions: Sequence[Mapping[str, Any]],
    tracks: Mapping[str, list[dict[str, Any]]],
    guard_count: int,
    scratch_root: Path | None,
    attempt_nonce: str | None,
    problems: Problems,
    *,
    require_matrix: bool,
) -> list[dict[str, Any]]:
    profile_adapter_snapshot: schema.FileSnapshot | None = None
    profile_replay: Callable[..., dict[str, Any]] | None = None
    if require_matrix:
        support_files = (
            prepared.get("support_files")
            if isinstance(prepared, Mapping)
            else None
        )
        adapter_binding = (
            support_files.get("profile_adapter")
            if isinstance(support_files, dict)
            else None
        )
        if isinstance(adapter_binding, dict):
            profile_adapter_snapshot = resolve_bound_file(
                str(profile_adapter),
                adapter_binding.get("sha256"),
                "profile replay adapter",
                problems,
                expected_mode=0o444,
            )
        else:
            problems.add("prepared profile replay adapter binding is absent")
        if profile_adapter_snapshot is not None:
            profile_replay = load_profile_replay_adapter(
                profile_adapter_snapshot, problems
            )
    for ordinal, record in enumerate(records, start=1):
        if not require_exact_keys(record, CHILD_FIELDS, f"child record {ordinal}", problems):
            continue
        if record.get("schema") != schema.CHILD_SCHEMA or record.get("protocol") != schema.PROTOCOL:
            problems.add(f"child record {ordinal} schema/protocol mismatch")
        if record.get("kind") not in set(schema.CHILD_KINDS):
            problems.add(f"child record {ordinal} kind invalid")
        if record.get("ordinal") != ordinal:
            problems.add(f"child record {ordinal} physical ordinal mismatch")
        identity = record.get("identity")
        if not require_exact_keys(
            identity, set(schema.PROCESS_IDENTITY_FIELDS), f"child {ordinal} identity", problems
        ):
            identity = {}
        for field in ("started_monotonic_ns", "completed_monotonic_ns", "waited_pid"):
            if not isinstance(record.get(field), int) or isinstance(record.get(field), bool) or record[field] <= 0:
                problems.add(f"child record {ordinal} {field} invalid")
        if record.get("waited_pid") != identity.get("pid") or record.get("exit_status") != 0:
            problems.add(f"child record {ordinal} wait/exit mismatch")
        if identity.get("comm") != record.get("executable_comm") or identity.get("pgrp") != identity.get("pid"):
            problems.add(f"child record {ordinal} process identity/comm mismatch")
        if (
            record.get("timed_out") is not False
            or record.get("terminated_by_runner") is not False
            or record.get("interrupted") is not None
            or record.get("process_group_absent") is not True
            or record.get("orphan_process_group_detected") is not False
            or record.get("validation_error") is not None
        ):
            problems.add(f"child record {ordinal} timeout/process group mismatch")
        if record.get("completed_monotonic_ns", 0) < record.get("started_monotonic_ns", 0):
            problems.add(f"child record {ordinal} chronology mismatch")
        parse_timestamp(record.get("started_at"), f"child {ordinal} started_at", problems)
        parse_timestamp(record.get("completed_at"), f"child {ordinal} completed_at", problems)
        reaping = record.get("reaping")
        if not require_exact_keys(reaping, set(schema.REAPING_FIELDS), f"child {ordinal} reaping", problems):
            reaping = None
        if reaping is not None and (reaping.get("pid"), reaping.get("start_ticks"), reaping.get("status")) != (
            identity.get("pid"), identity.get("starttime_ticks"), "absent"
        ):
            problems.add(f"child record {ordinal} reaping mismatch")
        if not isinstance(record.get("executable_mode"), int) or record["executable_mode"] & 0o222 or not record["executable_mode"] & 0o111:
            problems.add(f"child record {ordinal} executable mode invalid")
        executable = resolve_bound_file(
            record.get("executable_path"), record.get("executable_sha256"),
            f"child {ordinal} executable", problems,
            expected_mode=(
                record.get("executable_mode")
                if isinstance(record.get("executable_mode"), int)
                and not isinstance(record.get("executable_mode"), bool)
                else 0o555
            ),
        )
        if executable is not None:
            try:
                if stat.S_IMODE(executable.stat().st_mode) != record.get("executable_mode"):
                    problems.add(f"child record {ordinal} executable mode changed")
            except OSError as error:
                problems.add(f"child record {ordinal} executable stat failed: {error}")
        context_value = record.get("context")
        if not isinstance(context_value, dict) or hashlib.sha256(canonical_json_bytes(context_value)).hexdigest() != record.get("context_sha256"):
            problems.add(f"child record {ordinal} context/hash mismatch")
        for field in (
            "argv",
            "control_events",
            "profile_events",
            "parked_state_proofs",
            "profile_tool_helper_records",
        ):
            if not isinstance(record.get(field), list):
                problems.add(f"child record {ordinal} {field} is not a list")
        if not isinstance(record.get("environment"), dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in (record.get("environment") or {}).items()
        ):
            problems.add(f"child record {ordinal} environment invalid")
        for field, hash_field in (("control_events", "control_events_sha256"), ("profile_events", "profile_events_sha256")):
            if hashlib.sha256(canonical_json_bytes(record.get(field))).hexdigest() != record.get(hash_field):
                problems.add(f"child record {ordinal} {field} hash mismatch")
        profile_inputs = record.get("profile_tool_inputs")
        if not isinstance(profile_inputs, dict):
            problems.add(f"child record {ordinal} profile tool inputs are not an object")
            profile_inputs = {}
        if (
            hashlib.sha256(canonical_json_bytes(profile_inputs)).hexdigest()
            != record.get("profile_tool_inputs_sha256")
        ):
            problems.add(f"child record {ordinal} profile tool input hash mismatch")
        profile_context = (
            record.get("context") if isinstance(record.get("context"), dict) else {}
        )
        profile_track = schema.profile_tool_track_for_child(
            record.get("kind"), profile_context
        )
        validate_profile_tool_inputs(
            profile_inputs,
            profile_track,
            record,
            output_dir,
            problems,
            context=f"child record {ordinal}",
        )
        for prefix in ("raw", "stderr"):
            bound = resolve_bound_file(
                record.get(f"{prefix}_path"), record.get(f"{prefix}_sha256"),
                f"child {ordinal} {prefix}", problems, within=output_dir,
                expected_mode=0o444,
            )
            if bound is not None:
                if bound.stat().st_size != record.get(f"{prefix}_bytes"):
                    problems.add(f"child record {ordinal} {prefix} byte count mismatch")
                mode = stat.S_IMODE(bound.stat().st_mode)
                if mode != record.get(f"{prefix}_mode_after") or mode != 0o444:
                    problems.add(f"child record {ordinal} {prefix} mode mismatch")
        if not isinstance(record.get("expected_records"), int) or record["expected_records"] <= 0:
            problems.add(f"child record {ordinal} expected_records invalid")
        row_child = record.get("kind") in schema.TRACK_EXECUTION_ORDER
        if row_child:
            if (
                not is_sha256(record.get("runner_context_sha256"))
                or not is_sha256(record.get("profile_result_sha256"))
                or not is_sha256(record.get("combined_row_sha256"))
            ):
                problems.add(f"child record {ordinal} row composition hash invalid")
            if hashlib.sha256(canonical_json_bytes(record.get("runner_context"))).hexdigest() != record.get("runner_context_sha256"):
                problems.add(f"child record {ordinal} runner context hash mismatch")
            if hashlib.sha256(canonical_json_bytes(record.get("profile_result"))).hexdigest() != record.get("profile_result_sha256"):
                problems.add(f"child record {ordinal} profile result hash mismatch")
            validate_profile_rich_authority(
                record,
                profile_inputs,
                attempt_nonce,
                profile_adapter_snapshot,
                prepared_path,
                prepared_sha256,
                source_approval_path,
                source_approval_sha256,
                prepared,
                problems,
                context=f"child record {ordinal}",
            )
            if not require_exact_keys(record.get("csv_append"), set(schema.CSV_APPEND_FIELDS), f"child {ordinal} CSV append", problems):
                pass
        elif any(record.get(field) is not None for field in ("runner_context", "runner_context_sha256", "profile_result", "profile_result_sha256", "combined_row_sha256", "csv_append")):
            problems.add(f"child record {ordinal} non-row has row composition fields")
        if config is not None and record.get("profile_contract_sha256") != config.get("profile_contract_sha256"):
            problems.add(f"child record {ordinal} profile contract mismatch")
        if not isinstance(record.get("guard_pre_ordinal"), int) or not isinstance(record.get("guard_post_ordinal"), int):
            problems.add(f"child record {ordinal} guard binding invalid")
        elif not (1 <= record["guard_pre_ordinal"] < record["guard_post_ordinal"] <= guard_count):
            problems.add(f"child record {ordinal} guard binding out of range")

    row_records = [record for record in records if record.get("kind") in schema.TRACK_EXECUTION_ORDER]
    transition_records = [
        record
        for record in records
        if record.get("kind") in schema.TRANSITION_CHILD_KINDS
    ]
    if len(transition_records) != len(expected_transitions):
        problems.add("transition child count differs from reconstructed authority")
    if transition_records != records[: len(transition_records)]:
        problems.add("transition children are not the exact physical prefix")
    for ordinal, (record, expected) in enumerate(
        zip(transition_records, expected_transitions), start=1
    ):
        context_value = record.get("context")
        context = context_value if isinstance(context_value, Mapping) else {}
        variant = context.get("variant")
        if record.get("kind") == "contract":
            transition_id = (
                f"contract-{variant}" if isinstance(variant, str) else None
            )
        elif isinstance(context.get("smoke_id"), str):
            transition_id = context["smoke_id"]
        else:
            smoke_target = context.get("smoke_target")
            transition_id = (
                f"{smoke_target}-{variant}"
                if isinstance(smoke_target, str) and isinstance(variant, str)
                else None
            )
        if (
            record.get("kind") != expected.get("kind")
            or transition_id != expected.get("id")
            or variant != expected.get("variant")
            or record.get("argv") != expected.get("argv")
            or record.get("executable_path") != expected.get("executable_path")
            or record.get("executable_sha256")
            != expected.get("executable_sha256")
            or record.get("executable_mode") != expected.get("executable_mode")
            or record.get("executable_comm") != expected.get("executable_comm")
        ):
            problems.add(
                f"transition child {ordinal} differs from reconstructed authority"
            )
        expected_context = expected.get("context")
        if record.get("context") != expected_context:
            problems.add(
                f"transition child {ordinal} context differs from reconstructed authority"
            )
        expected_context_sha256 = hashlib.sha256(
            canonical_json_bytes(expected_context)
        ).hexdigest()
        if record.get("context_sha256") != expected_context_sha256:
            problems.add(
                f"transition child {ordinal} context hash differs from reconstructed authority"
            )
        environment = record.get("environment")
        environment = environment if isinstance(environment, Mapping) else {}
        profile_track = expected.get("profile_track")
        ptracer_pid: int | None = None
        if profile_track in {"syscall_profiles", "structural_traces"}:
            helper_records = record.get("profile_tool_helper_records")
            if (
                not isinstance(helper_records, list)
                or len(helper_records) != 1
                or not isinstance(helper_records[0], Mapping)
                or not isinstance(helper_records[0].get("identity"), Mapping)
                or isinstance(helper_records[0]["identity"].get("pid"), bool)
                or not isinstance(helper_records[0]["identity"].get("pid"), int)
            ):
                problems.add(
                    f"transition child {ordinal} ptracer authority is invalid"
                )
            else:
                ptracer_pid = helper_records[0]["identity"]["pid"]
        profile_inputs = record.get("profile_tool_inputs")
        profile_inputs = profile_inputs if isinstance(profile_inputs, Mapping) else {}
        perf_permission = (
            profile_inputs.get("perf_permission")
            if profile_track == "cpu_profiles"
            else None
        )
        perf_available = (
            problems.capture(
                f"transition child {ordinal} reconstruct perf permission",
                lambda: schema.profile_perf_permission_status(perf_permission),
            )
            == "available"
            if profile_track == "cpu_profiles"
            else False
        )
        expected_environment = problems.capture(
            f"transition child {ordinal} reconstruct environment",
            lambda: schema.transition_child_environment(
                scratch_root=scratch_root,
                attempt_nonce=attempt_nonce,
                output_dir=output_dir,
                physical_ordinal=record["ordinal"],
                context_sha256=expected_context_sha256,
                plan_environment=expected["plan_environment"],
                controlled=bool(expected["controlled"]),
                store_path=expected.get("store_path"),
                control_fd=(
                    environment.get("ASTERISM_REBASELINE_CONTROL_FD")
                    if expected["controlled"]
                    else None
                ),
                profile_track=(
                    str(profile_track) if isinstance(profile_track, str) else None
                ),
                ptracer_pid=ptracer_pid,
                perf_permission_result=perf_permission,
                perf_command_fd=(
                    environment.get("ASTERISM_REBASELINE_PERF_COMMAND_FD")
                    if perf_available
                    else None
                ),
                perf_ack_fd=(
                    environment.get("ASTERISM_REBASELINE_PERF_ACK_FD")
                    if perf_available
                    else None
                ),
                perf_ack_ledger_fd=(
                    environment.get("ASTERISM_REBASELINE_PERF_ACK_LEDGER_FD")
                    if perf_available
                    else None
                ),
            ),
        )
        if expected_environment is not None and environment != expected_environment:
            problems.add(
                f"transition child {ordinal} environment differs from reconstructed authority"
            )
    first_row_ordinal = min(
        (record.get("ordinal", 0) for record in row_records), default=len(records) + 1
    )
    if any(
        record.get("ordinal", first_row_ordinal) >= first_row_ordinal
        for record in transition_records
    ):
        problems.add("transition child occurred after row zero")
    if not require_matrix:
        if row_records:
            problems.add("correctness-only evidence contains timing row children")
        return row_records
    expected_identities: list[tuple[str, dict[str, Any]]] = []
    for track in schema.TRACK_EXECUTION_ORDER:
        expected = schema.expected_order(config, track) if config is not None else []
        expected_identities.extend((track, identity) for identity in expected)
    if len(row_records) != sum(schema.EXPECTED_CARDINALITY.values()):
        problems.add("row child count is not exact")
    if len(row_records) != len(expected_identities):
        return row_records

    csv_prefix: dict[str, bytes] = {
        track: (",".join(schema.CSV_FIELDS_BY_TRACK[track]) + "\n").encode("ascii")
        for track in schema.CSV_FIELDS_BY_TRACK
    }
    # csv.writer may quote headers in a future schema; derive the exact header
    # from an actual first row when available below before comparing prefix 0.
    csv_prefix.clear()
    for physical, (record, (track, identity)) in enumerate(
        zip(row_records, expected_identities, strict=True), start=1
    ):
        context = f"row child {physical}"
        row_ordinal = identity["row_ordinal"]
        variant = identity["variant"]
        child_context = record.get("context", {})
        if (record.get("kind"), child_context.get("row_ordinal"), child_context.get("variant")) != (
            track, row_ordinal, variant
        ):
            problems.add(f"{context} physical identity mismatch")
        expected_context: dict[str, Any] = {"track": track, **identity}
        store_track = (
            f"{track}-corpus"
            if track == "reopen"
            or (track == "structural_traces" and identity.get("trace_kind") == "reopen")
            else track
        )
        expected_store_path = (
            schema.fresh_store_path(
                scratch_root,
                attempt_nonce,
                store_track,
                row_ordinal,
                variant,
            )
            if scratch_root is not None and isinstance(attempt_nonce, str)
            else None
        )
        if track in {"primary", "cpu_profiles", "syscall_profiles"}:
            schedule = (
                schema.PROCESS_BPW
                if identity["durability"] == "Process"
                else schema.GROUP_BPW
            )
            expected_context["batches_per_writer"] = schedule[
                identity["batch_size"]
            ]
        elif track == "new_names":
            expected_context["batches_per_writer"] = schema.NEW_NAME_BPW[
                identity["durability"]
            ]
        elif track == "fairness":
            expected_context["batches_per_writer"] = schema.FAIRNESS_BPW[
                (identity["durability"], identity["batch_size"])
            ]
        if track in {"syscall_profiles", "structural_traces"}:
            if expected_store_path is None:
                problems.add(f"{context} trace marker store authority is unavailable")
            else:
                expected_context["variant_trace_path_markers"] = problems.capture(
                    f"{context} resolve trace path markers",
                    lambda: schema.resolved_trace_path_markers(
                        expected_store_path, variant
                    ),
                )
        corpus_fields = {
            "archive_manifest_sha256",
            "copy_manifest_sha256",
            "copy_id",
            "copy_verified_read_only",
            "expected_domain_events",
            "expected_visible_events",
            "expected_log_events",
            "expected_logical_digest",
            "expected_registry_head_digest",
        }
        corpus_row = track == "reopen" or (
            track == "structural_traces" and identity.get("trace_kind") == "reopen"
        )
        expected_keys = set(expected_context) | (corpus_fields if corpus_row else set())
        if set(child_context) != expected_keys:
            problems.add(
                f"{context} context keys are not exact; "
                f"missing={sorted(expected_keys - set(child_context))} "
                f"extra={sorted(set(child_context) - expected_keys)}"
            )
        for field, expected in expected_context.items():
            if child_context.get(field) != expected:
                problems.add(f"{context} context {field} differs from frozen authority")
        binary = binaries.get(variant)
        if binary is None:
            continue
        if Path(str(record.get("executable_path"))).resolve() != binary.resolve():
            problems.add(f"{context} executable differs from prepared binary")
        if config is not None:
            template = config.get("argv_templates", {}).get(track, [])
            expected_argv = problems.capture(
                f"{context} argv expansion",
                lambda template=template, binary=binary, track=track, row_ordinal=row_ordinal, variant=variant: expand_argv_template(
                    template,
                    binary=binary,
                    track=track,
                    row_ordinal=row_ordinal,
                    variant=variant,
                    config_path=config_path,
                    output_dir=output_dir,
                    profile_adapter=profile_adapter,
                ),
            )
            if expected_argv is not None and record.get("argv") != expected_argv:
                problems.add(f"{context} argv is not source-approved")
            environment = record.get("environment", {})
            if scratch_root is None or not isinstance(attempt_nonce, str):
                problems.add(f"{context} environment authority is unavailable")
            else:
                helper_records = record.get("profile_tool_helper_records")
                ptracer_pid: int | None = None
                if track in {"syscall_profiles", "structural_traces"}:
                    if (
                        not isinstance(helper_records, list)
                        or len(helper_records) != 1
                        or not isinstance(helper_records[0], dict)
                        or not isinstance(helper_records[0].get("identity"), dict)
                        or isinstance(helper_records[0]["identity"].get("pid"), bool)
                        or not isinstance(helper_records[0]["identity"].get("pid"), int)
                    ):
                        problems.add(f"{context} ptracer identity authority is invalid")
                    else:
                        ptracer_pid = helper_records[0]["identity"]["pid"]
                control_fd = environment.get("ASTERISM_REBASELINE_CONTROL_FD")
                profile_inputs = record.get("profile_tool_inputs", {})
                perf_permission = (
                    profile_inputs.get("perf_permission")
                    if track == "cpu_profiles" and isinstance(profile_inputs, dict)
                    else None
                )
                perf_available = problems.capture(
                    f"{context} reconstruct perf permission",
                    lambda: schema.profile_perf_permission_status(perf_permission),
                ) == "available" if track == "cpu_profiles" else False
                expected_environment = problems.capture(
                    f"{context} reconstruct environment",
                    lambda: schema.row_child_environment(
                        scratch_root=scratch_root,
                        attempt_nonce=attempt_nonce,
                        output_dir=output_dir,
                        config_path=config_path,
                        physical_ordinal=record["ordinal"],
                        track=track,
                        identity=identity,
                        context=child_context,
                        context_sha256=record["context_sha256"],
                        control_fd=control_fd,
                        ptracer_pid=ptracer_pid,
                        perf_permission_result=perf_permission,
                        perf_command_fd=(
                            environment.get("ASTERISM_REBASELINE_PERF_COMMAND_FD")
                            if perf_available
                            else None
                        ),
                        perf_ack_fd=(
                            environment.get("ASTERISM_REBASELINE_PERF_ACK_FD")
                            if perf_available
                            else None
                        ),
                        perf_ack_ledger_fd=(
                            environment.get("ASTERISM_REBASELINE_PERF_ACK_LEDGER_FD")
                            if perf_available
                            else None
                        ),
                    ),
                )
                if expected_environment is not None and environment != expected_environment:
                    problems.add(f"{context} environment is not the exact frozen map")
        raw_path = Path(str(record.get("raw_path")))
        raw_point = read_canonical_object(raw_path, f"{context} raw point", problems)
        if raw_point is None:
            continue
        if profile_replay is None:
            problems.add(f"{context} profile replay adapter is unavailable")
        else:
            replay_profile_result(
                profile_replay,
                record,
                raw_point,
                record.get("profile_tool_inputs", {}),
                output_dir,
                problems,
                context=context,
            )
        combined = problems.capture(
            f"{context} replay composition",
            lambda track=track, raw_point=raw_point, record=record: schema.validate_child_records(
                track,
                [raw_point],
                context,
                runner_context=record["runner_context"],
                profile_result=record["profile_result"],
            ),
        )
        if combined is None:
            continue
        if track == "reopen":
            reopen_context = {
                "archive_manifest_sha256": combined["archive_manifest_sha256"],
                "copy_manifest_sha256": combined["copy_manifest_sha256"],
                "copy_id": combined["copy_id"],
                "copy_verified_read_only": combined["copy_verified_read_only"],
                "expected_domain_events": combined["domain_events"],
                "expected_visible_events": combined["visible_events"],
                "expected_log_events": combined["log_events"],
                "expected_logical_digest": combined["logical_digest"],
                "expected_registry_head_digest": combined[
                    "registry_head_digest"
                ],
            }
            for field, expected in reopen_context.items():
                if child_context.get(field) != expected:
                    problems.add(
                        f"{context} context {field} differs from reconstructed row"
                    )
        combined_hash = hashlib.sha256(canonical_json_bytes(combined)).hexdigest()
        if combined_hash != record.get("combined_row_sha256"):
            problems.add(f"{context} combined row hash mismatch")
        persisted = tracks.get(track, [])
        if row_ordinal <= len(persisted) and combined != persisted[row_ordinal - 1]:
            problems.add(f"{context} combined row differs from CSV")
        encoded = schema.encode_csv_row(track, combined, write_header=row_ordinal == 1).encode("ascii")
        before = csv_prefix.get(track, b"")
        csv_append = record.get("csv_append", {})
        if hashlib.sha256(before).hexdigest() != csv_append.get("prefix_sha256_before"):
            problems.add(f"{context} CSV prefix-before hash mismatch")
        after = before + encoded
        if hashlib.sha256(after).hexdigest() != csv_append.get("prefix_sha256_after"):
            problems.add(f"{context} CSV prefix-after hash mismatch")
        if hashlib.sha256(after).hexdigest() != csv_append.get("sha256_after"):
            problems.add(f"{context} CSV sha256-after mismatch")
        csv_prefix[track] = after
        if (csv_append.get("rows_before"), csv_append.get("rows_after")) != (
            row_ordinal - 1, row_ordinal
        ):
            problems.add(f"{context} CSV count binding mismatch")
        if (csv_append.get("bytes_before"), csv_append.get("bytes_after")) != (len(before), len(after)):
            problems.add(f"{context} CSV byte binding mismatch")
        if Path(str(csv_append.get("path"))).resolve() != (output_dir / schema.CSV_FILENAMES[track]).resolve():
            problems.add(f"{context} CSV path mismatch")
    for track, data in csv_prefix.items():
        path = output_dir / schema.CSV_FILENAMES[track]
        snapshot = problems.recalled(path)
        if snapshot is None or snapshot.data != data:
            problems.add(f"{track} final CSV bytes differ from replayed prefix chain")
    return row_records


def validate_profile_artifact_binding(
    value: Any,
    output_dir: Path,
    problems: Problems,
    context: str,
) -> schema.FileSnapshot | None:
    if not require_exact_keys(
        value,
        set(schema.PROFILE_RAW_ARTIFACT_BINDING_FIELDS),
        context,
        problems,
    ):
        return None
    if value.get("mode") != 0o444:
        problems.add(f"{context} mode authority differs")
    path_value = value.get("path")
    path = Path(path_value) if isinstance(path_value, str) else None
    if (
        path is None
        or not path.is_absolute()
        or path_value.startswith("//")
        or ".." in path.parts
        or str(path) != path_value
    ):
        problems.add(f"{context} path is not canonical absolute")
        return None
    snapshot = resolve_bound_file(
        path_value,
        value.get("sha256"),
        context,
        problems,
        within=output_dir,
        expected_mode=0o444,
    )
    if snapshot is not None and value.get("bytes") != len(snapshot.data):
        problems.add(f"{context} byte length differs")
    return snapshot


def validate_profile_tool_inputs(
    value: Mapping[str, Any],
    track: Any,
    record: Mapping[str, Any],
    output_dir: Path,
    problems: Problems,
    *,
    context: str,
) -> None:
    if track not in schema.PROFILE_TOOL_INPUT_FIELDS_BY_TRACK:
        if value:
            problems.add(f"{context} non-profile child has profile tool inputs")
        return
    expected = set(schema.PROFILE_TOOL_INPUT_FIELDS_BY_TRACK[str(track)])
    if set(value) != expected:
        problems.add(
            f"{context} {track} profile tool input keys differ; "
            f"missing={sorted(expected - set(value))} extra={sorted(set(value) - expected)}"
        )
        return
    if track in {"primary", "new_names", "fairness", "cpu_profiles"}:
        resolution = value.get("schedstat_resolution_ns")
        if isinstance(resolution, bool) or not isinstance(resolution, int) or resolution <= 0:
            problems.add(f"{context} schedstat resolution is invalid")
    if track == "cpu_profiles":
        permission = value.get("perf_permission")
        status = problems.capture(
            f"{context} perf permission",
            lambda: schema.profile_perf_permission_status(permission),
        )
        events = value.get("perf_control_events")
        artifacts = value.get("perf_raw_artifacts")
        if not isinstance(events, list) or not isinstance(artifacts, dict):
            problems.add(f"{context} perf events/artifacts have invalid types")
            return
        if status == "available":
            if len(events) != 2 or set(artifacts) != {"stat", "ack"}:
                problems.add(f"{context} available perf evidence is incomplete")
                return
            stat_snapshot = validate_profile_artifact_binding(
                artifacts.get("stat"), output_dir, problems, f"{context} perf stat"
            )
            ack_snapshot = validate_profile_artifact_binding(
                artifacts.get("ack"), output_dir, problems, f"{context} perf ACK"
            )
            if stat_snapshot is not None and not stat_snapshot.data:
                problems.add(f"{context} perf stat artifact is empty")
            if ack_snapshot is not None and ack_snapshot.data != b"ack\nack\n":
                problems.add(f"{context} perf ACK artifact bytes differ")
        elif status == "not_available" and (events or artifacts):
            problems.add(f"{context} unavailable perf has events/artifacts")
    elif track in {"syscall_profiles", "structural_traces"}:
        validate_profile_artifact_binding(
            value.get("trace_raw_artifact"),
            output_dir,
            problems,
            f"{context} strace raw",
        )
        markers = {
            "log": value.get("log_path_markers"),
            "metadata": value.get("metadata_path_markers"),
        }
        child_context = record.get("context")
        authority = (
            child_context.get("trace_path_markers")
            if isinstance(child_context, dict)
            else None
        )
        if authority is None and isinstance(child_context, dict):
            authority = child_context.get("variant_trace_path_markers")
        if markers != authority:
            problems.add(f"{context} trace marker input/context authority differs")


def load_profile_replay_adapter(
    snapshot: schema.FileSnapshot,
    problems: Problems,
) -> Callable[..., dict[str, Any]] | None:
    """Execute the already-approved adapter from the evaluator's captured bytes."""

    module_name = f"_asterism_profile_replay_{snapshot.sha256}"
    module = types.ModuleType(module_name)
    module.__file__ = str(snapshot.path)
    module.__package__ = None
    try:
        code = compile(snapshot.data, str(snapshot.path), "exec", dont_inherit=True)
        sys.modules[module_name] = module
        exec(code, module.__dict__)
    except Exception as error:
        problems.add(f"profile replay adapter load failed: {error}")
        return None
    finally:
        sys.modules.pop(module_name, None)
    replay = getattr(module, "profile_fields", None)
    if not callable(replay):
        problems.add("profile replay adapter has no callable profile_fields")
        return None
    return replay


def replay_profile_result(
    replay: Callable[..., dict[str, Any]],
    record: Mapping[str, Any],
    raw_point: Mapping[str, Any],
    profile_inputs: Mapping[str, Any],
    output_dir: Path,
    problems: Problems,
    *,
    context: str,
) -> None:
    """Recompute a profile result from evaluator-snapshotted raw artifacts."""

    detached = copy.deepcopy(dict(profile_inputs))
    bindings: list[tuple[dict[str, Any], schema.FileSnapshot, str]] = []
    if record.get("kind") == "cpu_profiles":
        artifacts = detached.get("perf_raw_artifacts")
        if isinstance(artifacts, dict):
            for name, binding in sorted(artifacts.items()):
                if not isinstance(binding, dict):
                    continue
                snapshot = problems.recalled(Path(str(binding.get("path"))))
                if snapshot is not None:
                    bindings.append((binding, snapshot, f"perf-{name}"))
    elif record.get("kind") in {"syscall_profiles", "structural_traces"}:
        binding = detached.get("trace_raw_artifact")
        if isinstance(binding, dict):
            snapshot = problems.recalled(Path(str(binding.get("path"))))
            if snapshot is not None:
                bindings.append((binding, snapshot, "strace"))

    if len({(item.device, item.inode) for _, item, _ in bindings}) != len(bindings):
        problems.add(f"{context} profile raw artifacts alias one inode")
        return
    rich = record.get("profile_rich_result")
    authority = rich.get("authority") if isinstance(rich, dict) else None
    if not isinstance(authority, dict):
        return
    try:
        with tempfile.TemporaryDirectory(
            prefix=".profile-replay-", dir=output_dir
        ) as directory:
            replay_root = Path(directory)
            for ordinal, (binding, snapshot, label) in enumerate(bindings, start=1):
                replay_path = replay_root / f"{ordinal:02d}-{label}.raw"
                descriptor = os.open(
                    replay_path,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | os.O_CLOEXEC
                    | os.O_NOFOLLOW,
                    0o600,
                )
                try:
                    view = memoryview(snapshot.data)
                    while view:
                        written = os.write(descriptor, view)
                        if written <= 0:
                            raise OSError("short profile replay artifact write")
                        view = view[written:]
                    os.fchmod(descriptor, 0o444)
                finally:
                    os.close(descriptor)
                binding["path"] = str(replay_path)
            observed = replay(
                str(record.get("kind")),
                rich,
                raw_point=raw_point,
                control_events=record.get("control_events"),
                authority=authority,
                profile_inputs=detached,
            )
    except Exception as error:
        problems.add(f"{context} profile raw replay failed: {error}")
        return
    if observed != record.get("profile_result"):
        problems.add(f"{context} profile result differs from raw replay")


def validate_profile_rich_authority(
    record: Mapping[str, Any],
    profile_inputs: Mapping[str, Any],
    attempt_nonce: str | None,
    profile_adapter: schema.FileSnapshot | None,
    prepared_path: Path,
    prepared_sha256: str,
    source_approval_path: Path,
    source_approval_sha256: str,
    prepared: Mapping[str, Any] | None,
    problems: Problems,
    *,
    context: str,
) -> None:
    rich = record.get("profile_rich_result")
    if not require_exact_keys(
        rich,
        set(schema.PROFILE_RICH_RESULT_FIELDS),
        f"{context} rich profile result",
        problems,
    ):
        return
    authority = rich.get("authority")
    if not require_exact_keys(
        authority,
        set(schema.PROFILE_AUTHORITY_FIELDS),
        f"{context} profile authority",
        problems,
    ):
        return
    child_identity = record.get("identity") if isinstance(record.get("identity"), dict) else {}
    child_context = record.get("context") if isinstance(record.get("context"), dict) else {}
    expected_permission: Any = (
        profile_inputs.get("perf_permission")
        if record.get("kind") == "cpu_profiles"
        else "not_applicable"
    )
    expected_source = schema.VARIANT_SOURCE_BINDINGS.get(str(child_context.get("variant")))
    control_fd_text = (
        record.get("environment", {}).get("ASTERISM_REBASELINE_CONTROL_FD")
        if isinstance(record.get("environment"), dict)
        else None
    )
    control_fd = (
        int(control_fd_text)
        if isinstance(control_fd_text, str)
        and control_fd_text.isascii()
        and control_fd_text.isdecimal()
        and str(int(control_fd_text)) == control_fd_text
        else None
    )
    exact = {
        "schema": schema.PROFILE_AUTHORITY_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "attempt_nonce": attempt_nonce,
        "child_ordinal": record.get("ordinal"),
        "row_ordinal": child_context.get("row_ordinal"),
        "context_sha256": record.get("context_sha256"),
        "variant": child_context.get("variant"),
        "track": record.get("kind"),
        "executable_path": record.get("executable_path"),
        "executable_sha256": record.get("executable_sha256"),
        "executable_mode": record.get("executable_mode"),
        "executable_comm": record.get("executable_comm"),
        "child_pid": child_identity.get("pid"),
        "child_start_ticks": child_identity.get("starttime_ticks"),
        "control_fd": control_fd,
        "perf_permission_result": expected_permission,
        "prepared_artifacts_path": str(prepared_path),
        "prepared_artifacts_sha256": prepared_sha256,
        "source_approval_path": str(source_approval_path),
        "source_approval_sha256": source_approval_sha256,
    }
    if profile_adapter is not None:
        exact.update(
            {
                "profile_adapter_path": str(profile_adapter.path),
                "profile_adapter_sha256": profile_adapter.sha256,
            }
        )
    if expected_source is not None:
        exact.update(
            {
                "source_commit": expected_source["commit"],
                "source_tree": expected_source["tree"],
            }
        )
    for field, expected in exact.items():
        if authority.get(field) != expected:
            problems.add(f"{context} profile authority {field} differs")
    expected_tool_names = (
        {"perf"}
        if record.get("kind") == "cpu_profiles"
        else {"strace", "strace_launcher_runtime"}
        if record.get("kind") in {"syscall_profiles", "structural_traces"}
        else set()
    )
    prepared_tools = prepared.get("tools") if isinstance(prepared, Mapping) else None
    expected_tools = (
        {
            name: prepared_tools.get(name)
            for name in expected_tool_names
        }
        if isinstance(prepared_tools, dict)
        else None
    )
    if authority.get("profile_tools") != expected_tools:
        problems.add(f"{context} profile tool authority differs from prepared tools")
    if (
        rich.get("schema") != schema.PROFILE_ADAPTER_SCHEMA
        or rich.get("protocol") != schema.PROTOCOL
        or rich.get("variant") != exact["variant"]
        or rich.get("track") != record.get("kind")
        or rich.get("context") != child_context
    ):
        problems.add(f"{context} rich profile identity/context differs")


def validate_raw_manifest(
    records: list[dict[str, Any]],
    children: list[dict[str, Any]],
    output_dir: Path,
    problems: Problems,
) -> None:
    if len(records) != len(children):
        problems.add(f"raw manifest count {len(records)} != child count {len(children)}")
    for ordinal, (binding, child) in enumerate(zip(records, children), start=1):
        context = f"raw binding {ordinal}"
        if not require_exact_keys(binding, set(schema.RAW_BINDING_FIELDS), context, problems):
            continue
        exact = {
            "schema": schema.RAW_BINDING_SCHEMA,
            "protocol": schema.PROTOCOL,
            "child_ordinal": ordinal,
            "kind": child.get("kind"),
            "context_sha256": child.get("context_sha256"),
            "raw_path": child.get("raw_path"),
            "raw_sha256": child.get("raw_sha256"),
            "raw_bytes": child.get("raw_bytes"),
            "expected_records": child.get("expected_records"),
            "stderr_path": child.get("stderr_path"),
            "stderr_sha256": child.get("stderr_sha256"),
        }
        for field, expected in exact.items():
            if binding.get(field) != expected:
                problems.add(f"{context} {field} differs from child ledger")
        resolve_bound_file(
            binding.get("raw_path"), binding.get("raw_sha256"),
            f"{context} raw", problems, within=output_dir,
        )
        resolve_bound_file(
            binding.get("stderr_path"), binding.get("stderr_sha256"),
            f"{context} stderr", problems, within=output_dir,
        )


def validate_guards(
    records: list[dict[str, Any]],
    children: list[dict[str, Any]],
    output_dir: Path,
    lease: Mapping[str, Any] | None,
    allowlist: Sequence[str],
    problems: Problems,
) -> None:
    expected_count = 2 * len(children) + 1
    if len(records) != expected_count:
        problems.add(f"guard count {len(records)} != {expected_count}")
    for ordinal, record in enumerate(records, start=1):
        context = f"guard record {ordinal}"
        if not require_exact_keys(record, GUARD_BINDING_FIELDS, context, problems):
            continue
        expected_label = (
            f"{(ordinal + 1) // 2:05d}-{children[(ordinal - 1) // 2].get('kind')}-"
            f"{'pre' if ordinal % 2 else 'post'}"
            if ordinal <= 2 * len(children) else "pre-evaluator"
        )
        for field, expected in {
            "schema": schema.GUARD_BINDING_SCHEMA,
            "protocol": schema.PROTOCOL,
            "kind": "process_guard",
            "ordinal": ordinal,
            "label": expected_label,
            "verdict": "pass",
        }.items():
            if record.get(field) != expected:
                problems.add(f"{context} {field} mismatch")
        for field in ("started_monotonic_ns", "completed_monotonic_ns"):
            if not isinstance(record.get(field), int) or isinstance(record.get(field), bool) or record[field] < 0:
                problems.add(f"{context} {field} invalid")
        if record.get("completed_monotonic_ns", 0) < record.get("started_monotonic_ns", 0):
            problems.add(f"{context} chronology invalid")
        snapshot = resolve_bound_file(
            record.get("path"),
            record.get("sha256"),
            f"{context} snapshot",
            problems,
            within=output_dir,
        )
        if snapshot is not None:
            observed = read_canonical_object(snapshot, f"{context} snapshot", problems)
            if observed is None or not require_exact_keys(observed, GUARD_SNAPSHOT_FIELDS, f"{context} snapshot", problems):
                continue
            for field, expected in {
                "schema": schema.GUARD_SCHEMA,
                "protocol": schema.PROTOCOL,
                "ordinal": ordinal,
                "label": expected_label,
                "tracked_comm": list(allowlist),
                "active_child": None,
                "active_helpers": [],
                "verdict": "pass",
            }.items():
                if observed.get(field) != expected:
                    problems.add(f"{context} snapshot {field} mismatch")
            for field in ("started_monotonic_ns", "completed_monotonic_ns"):
                if observed.get(field) != record.get(field):
                    problems.add(f"{context} binding {field} differs from snapshot")
            parse_timestamp(observed.get("started_at"), f"{context} snapshot started_at", problems)
            parse_timestamp(observed.get("completed_at"), f"{context} snapshot completed_at", problems)
            runner = observed.get("runner")
            if not require_exact_keys(runner, set(schema.PROCESS_IDENTITY_FIELDS), f"{context} runner", problems):
                runner = {}
            if lease is not None and (
                runner.get("pid"), runner.get("starttime_ticks")
            ) != (lease.get("holder_pid"), lease.get("holder_start_ticks")):
                problems.add(f"{context} runner identity differs from lease")
            process_records = observed.get("records")
            if not isinstance(process_records, list):
                problems.add(f"{context} process records are not a list")
            else:
                for process_ordinal, process in enumerate(process_records, start=1):
                    if not require_exact_keys(process, set(schema.GUARD_PROCESS_FIELDS), f"{context} process {process_ordinal}", problems):
                        continue
                    if process.get("classification") not in {"runner", "current_child", "declared_helper"} or process.get("read_errors") != []:
                        problems.add(f"{context} process {process_ordinal} is unexplained/unresolved")
                    if process.get("comm") not in allowlist or not is_sha256(process.get("exe_sha256")):
                        problems.add(f"{context} process {process_ordinal} executable binding invalid")
                    parse_timestamp(process.get("observed_at"), f"{context} process {process_ordinal} observed_at", problems)
            vanished = observed.get("preidentity_vanished")
            if not isinstance(vanished, list) or any(not isinstance(pid, int) or pid <= 0 for pid in vanished):
                problems.add(f"{context} preidentity_vanished invalid")
            resource = observed.get("final_resource")
            if require_exact_keys(resource, set(schema.GUARD_RESOURCE_FIELDS), f"{context} resources", problems):
                load1 = resource.get("load1")
                if isinstance(load1, bool) or not isinstance(load1, (int, float)) or not (0 <= load1 < 6.0):
                    problems.add(f"{context} load is not quiet")
                if resource.get("free_bytes", 0) < 137_438_953_472 or resource.get("free_inodes", 0) < 1_000_000:
                    problems.add(f"{context} resource floor failed")
                if expected_label.endswith("-pre") and resource.get("enforced") is not True:
                    problems.add(f"{context} pre-child resources were not enforced")
    for child_index, child in enumerate(children, start=1):
        expected = (2 * child_index - 1, 2 * child_index)
        if (child.get("guard_pre_ordinal"), child.get("guard_post_ordinal")) != expected:
            problems.add(f"child {child_index} guard pair is not exact")
        if expected[1] <= len(records):
            pre = records[expected[0] - 1]
            post = records[expected[1] - 1]
            if pre.get("completed_monotonic_ns", 0) > child.get("started_monotonic_ns", 0):
                problems.add(f"child {child_index} starts before pre-guard completes")
            if post.get("started_monotonic_ns", 0) < child.get("completed_monotonic_ns", 0):
                problems.add(f"child {child_index} post-guard starts before completion")


def validate_lease(
    lease: Any,
    problems: Problems,
    *,
    synthetic: bool,
) -> None:
    fields = {
        "path",
        "device",
        "inode",
        "holder_pid",
        "holder_start_ticks",
        "holder_uid",
        "hostname",
        "boot_id",
        "nonce",
        "acquired_at",
        "acquired_monotonic_ns",
        "proc_locks_proof",
        "second_exclusive_failed",
    }
    if not require_exact_keys(lease, fields, "lease", problems):
        return
    canonical = Path.home() / ".cache/mess-bench/global-measurement.lock"
    if lease.get("path") != str(canonical):
        problems.add("lease path is not canonical")
    for field in ("device", "inode", "holder_pid", "holder_start_ticks", "holder_uid", "acquired_monotonic_ns"):
        if not isinstance(lease.get(field), int) or isinstance(lease.get(field), bool) or lease[field] < 0:
            problems.add(f"lease {field} invalid")
    for field in ("hostname", "boot_id", "nonce", "proc_locks_proof"):
        if not isinstance(lease.get(field), str) or not lease[field]:
            problems.add(f"lease {field} invalid")
    if not is_sha256(lease.get("nonce")):
        problems.add("lease nonce is not SHA-256")
    if lease.get("second_exclusive_failed") is not True:
        problems.add("lease second exclusive acquisition did not fail")
    parse_timestamp(lease.get("acquired_at"), "lease acquired_at", problems)
    if synthetic:
        return
    try:
        info = canonical.stat()
    except OSError as error:
        problems.add(f"cannot stat live lease: {error}")
        return
    if info.st_dev != lease.get("device") or info.st_ino != lease.get("inode"):
        problems.add("live lease device/inode mismatch")
    parent = os.getppid()
    if lease.get("holder_pid") != parent:
        problems.add("evaluator parent is not lease holder")
    try:
        proc_stat = (Path("/proc") / str(parent) / "stat").read_text()
        tail = proc_stat[proc_stat.rfind(")") + 2 :].split()
        start_ticks = int(tail[19])
        uid = (Path("/proc") / str(parent)).stat().st_uid
    except (OSError, ValueError, IndexError) as error:
        problems.add(f"cannot replay lease holder identity: {error}")
    else:
        if start_ticks != lease.get("holder_start_ticks") or uid != lease.get("holder_uid"):
            problems.add("lease holder PID reuse identity mismatch")
    if lease.get("hostname") != socket.gethostname():
        problems.add("lease hostname mismatch")
    try:
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError as error:
        problems.add(f"cannot read boot id: {error}")
    else:
        if boot_id != lease.get("boot_id"):
            problems.add("lease boot id mismatch")
    found = False
    try:
        for line in Path("/proc/locks").read_text().splitlines():
            parts = line.split()
            if len(parts) < 6 or parts[1:4] != ["FLOCK", "ADVISORY", "WRITE"]:
                continue
            major_text, minor_text, inode_text = parts[5].split(":", 2)
            device = os.makedev(int(major_text, 16), int(minor_text, 16))
            if int(parts[4]) == parent and device == info.st_dev and int(inode_text) == info.st_ino:
                found = True
                break
    except (OSError, ValueError) as error:
        problems.add(f"cannot replay /proc/locks: {error}")
    if not found:
        problems.add("no matching live exclusive lease in /proc/locks")
    try:
        descriptor = os.open(canonical, os.O_RDWR | os.O_CLOEXEC)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                if error.errno not in {errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK}:
                    problems.add(f"lease contention returned {error}")
            else:
                problems.add("live lease is not held exclusively")
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)
    except OSError as error:
        problems.add(f"cannot test live lease: {error}")


def validate_correctness(
    value: dict[str, Any] | None,
    config: dict[str, Any] | None,
    prepared: dict[str, Any] | None,
    children: list[dict[str, Any]],
    attempt_nonce: str | None,
    output_dir: Path,
    scratch_root: Path | None,
    problems: Problems,
    *,
    require_matrix: bool,
) -> tuple[list[str], list[str]]:
    current_failures: list[str] = []
    historical_failures: list[str] = []
    if value is None or not require_exact_keys(
        value, set(schema.CORRECTNESS_AGGREGATE_FIELDS), "correctness", problems
    ):
        return current_failures, historical_failures
    if value.get("schema") != schema.CORRECTNESS_SCHEMA or value.get("protocol") != schema.PROTOCOL or value.get("attempt_nonce") != attempt_nonce:
        problems.add("correctness identity mismatch")
    expected_descriptors = schema.correctness_descriptors()
    if config is not None and config.get("correctness_cases") != expected_descriptors:
        problems.add("correctness config partition/order changed")

    child_groups = schema.CORRECTNESS_GROUPS
    expected_execution: list[dict[str, Any]] = []
    if prepared is None or not isinstance(attempt_nonce, str):
        problems.add("correctness executable authority is unavailable")
    else:
        captured = problems.capture(
            "derive correctness executable authority",
            lambda: schema.correctness_execution_contract(prepared, attempt_nonce),
        )
        if captured is not None:
            expected_execution = captured
    if config is not None and config.get("correctness_execution") != expected_execution:
        problems.add("correctness config execution differs from prepared executable authority")
    execution_by_group = {
        tuple(item[field] for field in ("variant", "phase", "suite", "kind")): item
        for item in expected_execution
    }
    observed_order = [
        (child.get("context", {}).get("variant"), child.get("context", {}).get("phase"),
         child.get("context", {}).get("suite"), child.get("kind"))
        for child in children if child.get("kind") in {"correctness", "fault"}
    ]
    if observed_order != list(child_groups):
        problems.add("correctness child invocation order differs from frozen contract")
    observed_groups: dict[tuple[str, str, str, str], tuple[dict[str, Any], dict[str, Any]]] = {}
    fault_bounds: dict[str, bytes] = {}
    for child in children:
        if child.get("kind") not in {"correctness", "fault"}:
            continue
        ordinal = child.get("ordinal")
        raw_path = Path(str(child.get("raw_path")))
        record = read_canonical_object(raw_path, f"correctness child {ordinal}", problems)
        if record is None or not require_exact_keys(
            record, set(schema.CORRECTNESS_CHILD_FIELDS), f"correctness child {ordinal}", problems
        ):
            continue
        identity = (record.get("variant"), record.get("phase"), record.get("suite"), child.get("kind"))
        if identity not in child_groups:
            problems.add(f"correctness child {ordinal} selected an unapproved variant/phase/suite/kind")
            continue
        if identity in observed_groups:
            problems.add(f"correctness child group duplicated: {identity}")
            continue
        observed_groups[identity] = (child, record)
        execution = execution_by_group.get(identity)
        if execution is None:
            problems.add(f"correctness child {ordinal} has no prepared executable authority")
        else:
            for child_field, execution_field in (
                ("executable_path", "executable_path"),
                ("executable_sha256", "executable_sha256"),
                ("executable_mode", "executable_mode"),
                ("executable_comm", "executable_comm"),
                ("argv", "argv"),
            ):
                if child.get(child_field) != execution.get(execution_field):
                    problems.add(
                        f"correctness child {ordinal} {child_field} differs from prepared authority"
                    )
            child_environment = child.get("environment")
            phase_groups = [
                group for group in child_groups
                if (group[1] in {"oracle", "pre"}) == (identity[1] in {"oracle", "pre"})
            ]
            phase_ordinal = phase_groups.index(identity) + 1
            expected_environment = None
            if (
                not isinstance(child_environment, dict)
                or not all(
                    isinstance(key, str) and isinstance(value, str)
                    for key, value in child_environment.items()
                )
                or scratch_root is None
                or not isinstance(attempt_nonce, str)
            ):
                problems.add(
                    f"correctness child {ordinal} environment authority is invalid"
                )
            else:
                expected_environment = problems.capture(
                    f"correctness child {ordinal} reconstruct environment",
                    lambda: schema.correctness_child_environment(
                        scratch_root=scratch_root,
                        attempt_nonce=attempt_nonce,
                        output_dir=output_dir,
                        physical_ordinal=child["ordinal"],
                        variant=identity[0],
                        phase=identity[1],
                        suite=identity[2],
                        phase_ordinal=phase_ordinal,
                        context_sha256=child["context_sha256"],
                        control_fd=child_environment.get(
                            "ASTERISM_REBASELINE_CONTROL_FD"
                        ),
                    ),
                )
            if expected_environment is not None and child_environment != expected_environment:
                problems.add(
                    f"correctness child {ordinal} environment differs from exact frozen map"
                )
        if record.get("schema") != schema.CORRECTNESS_CHILD_SCHEMA or record.get("protocol") != schema.PROTOCOL or record.get("attempt_nonce") != attempt_nonce:
            problems.add(f"correctness child {ordinal} identity mismatch")
        if record.get("harness_sound") is not True:
            problems.add(f"correctness child {ordinal} harness is unsound")
        expected_cases = [
            descriptor for descriptor in expected_descriptors
            if (descriptor["variant"], descriptor["phase"], descriptor["suite"], descriptor["kind"]) == identity
        ]
        child_cases = record.get("cases")
        if not isinstance(child_cases, list) or len(child_cases) != len(expected_cases):
            problems.add(f"correctness child {ordinal} case cardinality mismatch")
            child_cases = []
        for case_ordinal, (case, descriptor) in enumerate(zip(child_cases, expected_cases), start=1):
            context = f"correctness child {ordinal} case {case_ordinal}"
            if not require_exact_keys(case, set(schema.CORRECTNESS_CHILD_CASE_FIELDS), context, problems):
                continue
            expected_case = {
                "id": descriptor["id"], "classification": descriptor["classification"],
                "status": case.get("status"),
            }
            if case != expected_case or case.get("status") not in {"PASS", "FAIL"}:
                problems.add(f"{context} id/classification/status mismatch")
        bounds = record.get("boundedness")
        if identity[0] == "A" and identity[2] == "current-fault":
            if bounds != schema.CORRECTNESS_EXPECTED_BOUNDEDNESS:
                current_failures.append(f"{identity[1]}:boundedness")
            else:
                fault_bounds[identity[1]] = canonical_json_bytes(bounds)
        elif bounds is not None:
            problems.add(f"correctness child {ordinal} non-fault boundedness must be null")
    missing_groups = set(child_groups) - set(observed_groups)
    if missing_groups:
        problems.add(f"correctness child groups missing: {sorted(missing_groups)}")
    if set(fault_bounds) == {"pre", "post"} and fault_bounds["pre"] != fault_bounds["post"]:
        problems.add("correctness fault pre/post boundedness bytes differ")

    cases = value.get("cases")
    if not isinstance(cases, list) or len(cases) != len(expected_descriptors):
        problems.add("correctness aggregate case cardinality mismatch")
        cases = []
    for ordinal, (case, descriptor) in enumerate(zip(cases, expected_descriptors), start=1):
        context = f"correctness aggregate case {ordinal}"
        if not require_exact_keys(case, set(schema.CORRECTNESS_AGGREGATE_CASE_FIELDS), context, problems):
            continue
        for field, expected in descriptor.items():
            if case.get(field) != expected:
                problems.add(f"{context} {field} differs from frozen descriptor")
        identity = (descriptor["variant"], descriptor["phase"], descriptor["suite"], descriptor["kind"])
        group = observed_groups.get(identity)
        if group is None:
            continue
        child, child_record = group
        child_case = next((
            item for item in child_record.get("cases", [])
            if isinstance(item, dict) and item.get("id") == descriptor["id"]
        ), None)
        expected_binding = {
            "child_ordinal": child.get("ordinal"), "output_path": child.get("raw_path"),
            "output_sha256": child.get("raw_sha256"),
        }
        for field, expected in expected_binding.items():
            if case.get(field) != expected:
                problems.add(f"{context} {field} differs from child artifact")
        if child_case is None or case.get("status") != child_case.get("status"):
            problems.add(f"{context} status differs from child output")
        elif case["status"] == "FAIL":
            failure = f"{descriptor['phase']}:{descriptor['id']}"
            (current_failures if descriptor["variant"] == "A" else historical_failures).append(failure)

    if value.get("harness_sound") is not True or any(
        record.get("harness_sound") is not True for _, record in observed_groups.values()
    ):
        problems.add("correctness aggregate harness soundness mismatch")
    if value.get("boundedness") != schema.CORRECTNESS_EXPECTED_BOUNDEDNESS:
        current_failures.append("aggregate:boundedness")

    if require_matrix:
        row_ordinals = [
            child.get("ordinal")
            for child in children
            if child.get("kind") in schema.TRACK_EXECUTION_ORDER
        ]
        first_row = min(row_ordinals) if row_ordinals else 0
        last_row = max(row_ordinals) if row_ordinals else 0
        for identity, (child, _) in observed_groups.items():
            child_ordinal = child.get("ordinal", 0)
            if identity[1] in {"pre", "oracle"} and child_ordinal >= first_row:
                problems.add(f"correctness {identity} did not finish before row zero")
            if identity[1] == "post" and child_ordinal <= last_row:
                problems.add(f"correctness {identity} did not run after timing")
    return current_failures, historical_failures


def validate_provenance(
    provenance: dict[str, Any] | None,
    output_dir: Path,
    protocol_sha256: str,
    config: dict[str, Any] | None,
    problems: Problems,
    *,
    synthetic: bool,
    correctness_only: bool,
) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    if provenance is None or not require_exact_keys(
        provenance, set(schema.PROVENANCE_FIELDS), "provenance", problems
    ):
        return paths
    exact = {
        "schema": schema.PROVENANCE_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": protocol_sha256,
        "evidence_mode": "correctness-only" if correctness_only else "admission",
        "attempt_nonce": config.get("attempt_nonce") if config else None,
        "output_dir": str(output_dir),
        "output_dir_absent_before": True,
        "partial": False,
        "failure_absent": True,
    }
    for field, expected in exact.items():
        if provenance.get(field) != expected:
            problems.add(f"provenance {field} mismatch")
    start = parse_timestamp(provenance.get("started_at"), "provenance started_at", problems)
    end = parse_timestamp(provenance.get("completed_at"), "provenance completed_at", problems)
    if start is not None and end is not None and end < start:
        problems.add("provenance wall chronology invalid")
    for field in ("started_monotonic_ns", "completed_monotonic_ns"):
        if not isinstance(provenance.get(field), int) or isinstance(provenance.get(field), bool) or provenance[field] <= 0:
            problems.add(f"provenance {field} invalid")
    if provenance.get("completed_monotonic_ns", 0) < provenance.get("started_monotonic_ns", 0):
        problems.add("provenance monotonic chronology invalid")

    exact_local = {
        "source_approval": output_dir / "source-approval.json",
        "config": output_dir / "config.json",
        "prepared_artifacts": output_dir / "prepared-artifacts.json",
        "correctness": output_dir / "correctness.json",
        "raw_manifest": output_dir / "raw-manifest.json",
        "guard_manifest": output_dir / "guard-manifest.jsonl",
        "child_manifest": output_dir / "child-manifest.jsonl",
        "profile_contract": output_dir / "profile-contract.json",
    }
    for prefix, expected_path in exact_local.items():
        path_field = f"{prefix}_path"
        hash_field = f"{prefix}_sha256"
        if provenance.get(path_field) != str(expected_path):
            problems.add(f"provenance {path_field} is not exact")
        path = resolve_bound_file(
            provenance.get(path_field), provenance.get(hash_field), prefix.replace("_", " "), problems, within=output_dir
        )
        if path is not None:
            paths[prefix] = path

    tooling_fields = {
        "runner": "run_rebaseline.py",
        "evaluator": "evaluate.py",
        "terminal_verifier": "verify_terminal.py",
        "schema": "evidence_schema.py",
        "profile_adapter": "profile_adapters.py",
    }
    for prefix, filename in tooling_fields.items():
        path = resolve_bound_file(
            provenance.get(f"{prefix}_path"),
            provenance.get(f"{prefix}_sha256"),
            f"provenance {prefix}",
            problems,
        )
        if path is not None:
            paths[prefix] = path
            if path.name != filename:
                problems.add(f"provenance {prefix} filename mismatch")
    correctness_executable = resolve_bound_file(
        provenance.get("correctness_executable_path"),
        provenance.get("correctness_executable_sha256"),
        "provenance correctness executable", problems, expected_mode=0o555,
    )
    if correctness_executable is not None:
        paths["correctness_executable"] = correctness_executable
    if "evaluator" in paths and not synthetic and paths["evaluator"] != Path(__file__).resolve():
        problems.add("running evaluator differs from provenance evaluator")
    if "schema" in paths and not synthetic and paths["schema"] != Path(schema.__file__).resolve():
        problems.add("loaded schema differs from provenance schema")

    csv_artifacts = provenance.get("csv_artifacts")
    expected_csv_artifacts = set() if correctness_only else set(schema.CSV_FILENAMES)
    if not isinstance(csv_artifacts, dict) or set(csv_artifacts) != expected_csv_artifacts:
        problems.add("provenance CSV artifact keys are not exact")
    else:
        for track, item in csv_artifacts.items():
            context = f"provenance CSV {track}"
            if not require_exact_keys(item, {"path", "sha256", "bytes", "rows", "columns"}, context, problems):
                continue
            expected_path = output_dir / schema.CSV_FILENAMES[track]
            if item.get("path") != str(expected_path):
                problems.add(f"{context} path mismatch")
            path = resolve_bound_file(item.get("path"), item.get("sha256"), context, problems, within=output_dir)
            if path is not None:
                try:
                    if path.stat().st_size != item.get("bytes"):
                        problems.add(f"{context} byte count mismatch")
                except OSError as error:
                    problems.add(f"{context} cannot stat: {error}")
            if item.get("rows") != schema.EXPECTED_CARDINALITY[track] or item.get("columns") != len(schema.CSV_FIELDS_BY_TRACK[track]):
                problems.add(f"{context} cardinality/schema mismatch")

    host = provenance.get("host")
    host_fields = set(schema.PROVENANCE_HOST_FIELDS)
    if require_exact_keys(host, host_fields, "provenance host", problems):
        text_fields = {
            "hostname", "boot_id", "kernel", "cpu_model", "scratch_root",
        }
        for field in text_fields:
            if not isinstance(host.get(field), str) or not host[field]:
                problems.add(f"provenance host {field} invalid")
        integer_fields = {
            "page_size", "cpu_count", "memory_bytes", "uid",
            "scratch_free_bytes_initial", "scratch_free_inodes_initial",
            "scratch_free_bytes_final", "scratch_free_inodes_final",
            "guard_records", "child_records",
        }
        for field in integer_fields:
            if not isinstance(host.get(field), int) or isinstance(host.get(field), bool) or host[field] < 0:
                problems.add(f"provenance host {field} invalid")
        for field in ("resource_manifest_sha256", "correctness_manifest_sha256"):
            if not is_sha256(host.get(field)):
                problems.add(f"provenance host {field} invalid")
        topology = host.get("cpu_topology")
        if require_exact_keys(
            topology, set(schema.PROVENANCE_CPU_TOPOLOGY_FIELDS),
            "provenance CPU topology", problems,
        ):
            for field in schema.PROVENANCE_CPU_TOPOLOGY_FIELDS:
                if not isinstance(topology.get(field), int) or isinstance(topology.get(field), bool) or topology[field] <= 0:
                    problems.add(f"provenance CPU topology {field} invalid")
        governors = host.get("governors")
        if (
            not isinstance(governors, dict)
            or not governors
            or not all(isinstance(policy, str) and policy and isinstance(value, str) and value for policy, value in governors.items())
        ):
            problems.add("provenance CPU governors are invalid")
        turbo = host.get("turbo")
        if require_exact_keys(
            turbo, set(schema.PROVENANCE_TURBO_FIELDS),
            "provenance turbo state", problems,
        ) and not all(
            isinstance(turbo[field], str) and turbo[field]
            for field in schema.PROVENANCE_TURBO_FIELDS
        ):
            problems.add("provenance turbo state values are invalid")
        scheduler = host.get("scheduler")
        if require_exact_keys(
            scheduler, set(schema.PROVENANCE_SCHEDULER_FIELDS),
            "provenance scheduler", problems,
        ) and not all(
            isinstance(scheduler[field], str) and scheduler[field]
            for field in schema.PROVENANCE_SCHEDULER_FIELDS
        ):
            problems.add("provenance scheduler values are invalid")
        affinity = host.get("affinity")
        if (
            not isinstance(affinity, list)
            or not affinity
            or affinity != sorted(set(affinity))
            or not all(isinstance(cpu, int) and not isinstance(cpu, bool) and cpu >= 0 for cpu in affinity)
        ):
            problems.add("provenance host affinity is invalid")
        filesystem = host.get("filesystem")
        if require_exact_keys(
            filesystem, set(schema.PROVENANCE_FILESYSTEM_FIELDS),
            "provenance filesystem", problems,
        ) and not all(isinstance(value, str) and value for value in filesystem.values()):
            problems.add("provenance filesystem fields are invalid")
        if isinstance(filesystem, dict) and filesystem.get("filesystem_type") != schema.REQUIRED_FILESYSTEM_TYPE:
            problems.add("provenance scratch filesystem is not ext4")
        try:
            Path(str(host.get("scratch_root"))).resolve(strict=True)
            output_dir.relative_to(Path(str(host.get("scratch_root"))).resolve(strict=True))
        except (OSError, ValueError):
            problems.add("provenance output directory is outside scratch root")
        tracked_comm = host.get("tracked_comm")
        if (
            not isinstance(tracked_comm, list)
            or tracked_comm != sorted(set(tracked_comm))
            or not all(isinstance(item, str) and item for item in tracked_comm)
        ):
            problems.add("provenance tracked comm is invalid")
        frozen = host.get("frozen_files")
        if not isinstance(frozen, dict) or not frozen:
            problems.add("provenance frozen file map is invalid")
        else:
            for frozen_path, frozen_sha256 in frozen.items():
                resolve_bound_file(
                    frozen_path, frozen_sha256,
                    f"provenance frozen file {frozen_path}", problems,
                )
        for field, filename in (
            ("resource_manifest", "resource-manifest.jsonl"),
            ("correctness_manifest", "correctness-manifest.jsonl"),
        ):
            expected_path = output_dir / filename
            if host.get(f"{field}_path") != str(expected_path):
                problems.add(f"provenance host {field} path mismatch")
            resolve_bound_file(
                host.get(f"{field}_path"), host.get(f"{field}_sha256"),
                f"provenance host {field}", problems, within=output_dir,
            )
        limits = config.get("resource_limits", {}) if config else {}
        for suffix in ("initial", "final"):
            if host.get(f"scratch_free_bytes_{suffix}", -1) < limits.get("free_bytes", 0):
                problems.add(f"provenance scratch free bytes {suffix} below frozen floor")
            if host.get(f"scratch_free_inodes_{suffix}", -1) < limits.get("free_inodes", 0):
                problems.add(f"provenance scratch free inodes {suffix} below frozen floor")
        runner = host.get("runner")
        if require_exact_keys(runner, set(schema.PROCESS_IDENTITY_FIELDS), "provenance host runner", problems):
            lease = provenance.get("lease", {})
            if (runner.get("pid"), runner.get("starttime_ticks")) != (
                lease.get("holder_pid"), lease.get("holder_start_ticks")
            ):
                problems.add("provenance runner identity differs from lease")
        if not synthetic:
            if host.get("hostname") != socket.gethostname():
                problems.add("provenance host name differs from live host")
            if host.get("uid") != os.getuid():
                problems.add("provenance host uid differs from live evaluator")
            if host.get("page_size") != os.sysconf("SC_PAGE_SIZE"):
                problems.add("provenance page size differs from live host")
            if host.get("affinity") != sorted(os.sched_getaffinity(0)):
                problems.add("provenance affinity differs from live evaluator")
            if host.get("cpu_count") != os.cpu_count():
                problems.add("provenance CPU count differs from live host")
            live_observations = {
                "kernel": problems.capture(
                    "read live kernel identity",
                    lambda: " ".join(os.uname()),
                ),
                "cpu_model": problems.capture("read live CPU model", live_cpu_model),
                "cpu_topology": problems.capture("read live CPU topology", live_cpu_topology),
                "governors": problems.capture("read live CPU governors", live_governors),
                "turbo": problems.capture("read live turbo state", live_turbo_state),
                "memory_bytes": problems.capture(
                    "read live memory size",
                    lambda: os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE"),
                ),
                "filesystem": problems.capture(
                    "read live scratch mount identity",
                    lambda: live_filesystem_identity(Path(str(host.get("scratch_root")))),
                ),
            }
            live_filesystem = live_observations.get("filesystem")
            live_observations["scheduler"] = problems.capture(
                "read live block scheduler identity",
                lambda: live_scheduler_identity(
                    live_filesystem["device"] if isinstance(live_filesystem, dict) else ""
                ),
            )
            for field, observed in live_observations.items():
                if observed is not None and host.get(field) != observed:
                    problems.add(f"provenance {field} differs from live host")
            try:
                live_boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
            except OSError as error:
                problems.add(f"cannot replay host boot id: {error}")
            else:
                if host.get("boot_id") != live_boot:
                    problems.add("provenance boot id differs from live host")
    validate_lease(provenance.get("lease"), problems, synthetic=synthetic)
    return paths


def validate_profile_contract(
    path: Path | None,
    config: dict[str, Any] | None,
    problems: Problems,
) -> int:
    """Replay the once-per-attempt schedstat resolution contract."""

    if path is None:
        problems.add("profile preflight artifact is absent from provenance")
        return 0
    value = read_canonical_object(path, "profile preflight", problems)
    if value is None or not require_exact_keys(
        value, set(schema.PROFILE_PREFLIGHT_FIELDS), "profile preflight", problems
    ):
        return 0
    exact = {
        "schema": schema.PROFILE_PREFLIGHT_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "profile_contract_sha256": schema.expected_profile_contract_sha256(),
        "source": "/proc/<pid>/task/<native-tid>/schedstat:first-field",
        "helper": "adapter-owned-cpu-bound-native-thread",
        "decision_multiplier": schema.SCHEDSTAT_DECISION_MULTIPLIER,
    }
    for field, expected in exact.items():
        if value.get(field) != expected:
            problems.add(f"profile preflight {field} mismatch")
    if config is not None and snapshot_sha256(path, problems) != config.get("profile_contract_sha256"):
        problems.add("profile preflight hash differs from config")
    samples = value.get("samples_ns")
    if (
        not isinstance(samples, list)
        or len(samples) < 3
        or any(not isinstance(item, int) or isinstance(item, bool) or item < 0 for item in samples)
    ):
        problems.add("profile preflight samples are invalid")
        return 0
    increments: list[int] = []
    for before, after in zip(samples, samples[1:]):
        if after < before:
            problems.add("profile preflight samples are not monotone")
            return 0
        if after > before:
            increments.append(after - before)
    if not increments:
        problems.add("profile preflight has no nonzero increment")
        return 0
    resolution = min(increments)
    if value.get("minimum_nonzero_increment_ns") != resolution:
        problems.add("profile preflight minimum increment replay mismatch")
    if value.get("decision_floor_ns") != resolution * schema.SCHEDSTAT_DECISION_MULTIPLIER:
        problems.add("profile preflight decision floor mismatch")
    return resolution


def median_fraction(values: Sequence[Fraction]) -> Fraction:
    if not values:
        raise ValueError("median of empty sequence")
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def ratio_string(value: Fraction) -> str:
    return f"{value.numerator}/{value.denominator}"


def cell_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("durability"),
        row.get("payload_size"),
        row.get("batch_size"),
        row.get("writers"),
    )


def paired_rows(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[Any, ...], dict[int, dict[str, Mapping[str, Any]]]]:
    result: dict[tuple[Any, ...], dict[int, dict[str, Mapping[str, Any]]]] = defaultdict(lambda: defaultdict(dict))
    for row in rows:
        result[cell_key(row)][row["block"]][row["variant"]] = row
    return result


def raw_ratio(a: Mapping[str, Any], reference: Mapping[str, Any], metric: str) -> Fraction:
    if metric == "throughput":
        return Fraction(a["domain_events"] * reference["wall_ns"], reference["domain_events"] * a["wall_ns"])
    if metric in {"allocation_calls", "allocated_bytes", "serialized_role_cpu_ns"}:
        return Fraction(a[metric] * reference["domain_events"], reference[metric] * a["domain_events"])
    if metric == "fsync_mean_ns":
        return Fraction(a["fsync_total_ns"] * reference["fsync_count"], reference["fsync_total_ns"] * a["fsync_count"])
    return Fraction(a[metric], reference[metric])


def paired_ratio(
    blocks: Mapping[int, Mapping[str, Mapping[str, Any]]],
    reference: str,
    metric: str,
) -> tuple[Fraction, list[Fraction]]:
    values = [raw_ratio(blocks[block]["A"], blocks[block][reference], metric) for block in range(1, 5)]
    return median_fraction(values), values


def fraction_statistics(
    values: Sequence[Fraction], *, higher_is_better: bool
) -> dict[str, Any]:
    """Return the frozen exact four-block descriptive statistics."""

    median = median_fraction(values)
    deviations = [abs(value - median) for value in values]
    return {
        "observations": [ratio_string(value) for value in values],
        "median": ratio_string(median),
        "best_of_four": ratio_string(max(values) if higher_is_better else min(values)),
        "minimum": ratio_string(min(values)),
        "maximum": ratio_string(max(values)),
        "median_absolute_deviation": ratio_string(median_fraction(deviations)),
        "higher_is_better": higher_is_better,
    }


def gate_record(
    gate_id: str,
    comparison: str,
    track: str,
    cell: tuple[Any, ...] | str,
    metric: str,
    observed: Fraction | int | str,
    operator: str,
    threshold: Fraction | int | str,
    passed: bool,
    failure_outcome: str,
    *,
    block_ratios: Sequence[Fraction] | None = None,
    note: str = "",
) -> dict[str, Any]:
    def encoded(value: Fraction | int | str) -> str | int:
        return ratio_string(value) if isinstance(value, Fraction) else value

    return {
        "id": gate_id,
        "comparison": comparison,
        "track": track,
        "cell": list(cell) if isinstance(cell, tuple) else cell,
        "metric": metric,
        "observed": encoded(observed),
        "operator": operator,
        "threshold": encoded(threshold),
        "pass": passed,
        "failure_outcome": failure_outcome,
        "block_ratios": [ratio_string(value) for value in block_ratios] if block_ratios is not None else [],
        "statistics": (
            fraction_statistics(
                block_ratios,
                higher_is_better=operator == ">=" or "throughput" in metric,
            )
            if block_ratios
            else None
        ),
        "note": note,
    }


def group_performance_outcome(
    blocks: Mapping[int, Mapping[str, Mapping[str, Any]]],
    reference: str,
) -> tuple[str, str]:
    mean_ratio, _ = paired_ratio(blocks, reference, "fsync_mean_ns")
    p99_ratio, _ = paired_ratio(blocks, reference, "fsync_p99_ns")
    if mean_ratio <= Fraction(102, 100) and p99_ratio <= Fraction(102, 100):
        return "NARROW", f"fsync mean={ratio_string(mean_ratio)}, p99={ratio_string(p99_ratio)} <= 1.02"
    return "INCONCLUSIVE", f"device-confounded fsync mean={ratio_string(mean_ratio)}, p99={ratio_string(p99_ratio)}"


def evaluate_gates(
    tracks: Mapping[str, list[dict[str, Any]]],
    current_correctness_failures: Sequence[str],
    historical_correctness_failures: Sequence[str],
    sched_resolution: int,
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    gates: list[dict[str, Any]] = []
    outcome_reasons: dict[str, list[str]] = defaultdict(list)
    if current_correctness_failures:
        outcome_reasons["REVERT"].extend(f"correctness:{item}" for item in current_correctness_failures)
    if historical_correctness_failures:
        outcome_reasons["INCONCLUSIVE"].extend(f"historical-oracle:{item}" for item in historical_correctness_failures)

    primary = paired_rows(tracks["primary"])
    process_cells: list[tuple[Any, ...]] = []
    comparison_rows: dict[str, Any] = {"A/D": {}, "A/C": {}, "A/B": {}}
    sched_resolutions = [row["schedstat_resolution_ns"] for row in tracks["cpu_profiles"]]
    if sched_resolution <= 0 or any(value != sched_resolution for value in sched_resolutions):
        outcome_reasons["INCONCLUSIVE"].append("cpu-profile-resolution-contract")

    for cell, blocks in sorted(primary.items(), key=lambda item: str(item[0])):
        durability = cell[0]
        if durability == "Process":
            process_cells.append(cell)
        for reference in ("D", "C", "B"):
            comparison = f"A/{reference}"
            metrics = (
                "throughput",
                "latency_p99_ns",
                "allocation_calls",
                "allocated_bytes",
                "serialized_role_cpu_ns",
                "barrier_count",
                "fsync_mean_ns" if durability == "Group" else "process_user_cpu_ns",
                "fsync_p99_ns" if durability == "Group" else "process_system_cpu_ns",
            )
            result: dict[str, Any] = {}
            for metric in metrics:
                try:
                    median, values = paired_ratio(blocks, reference, metric)
                except (KeyError, ZeroDivisionError):
                    continue
                result[metric] = {
                    **fraction_statistics(
                        values, higher_is_better=metric == "throughput"
                    ),
                    "median_ratio": ratio_string(median),
                    "block_ratios": [ratio_string(value) for value in values],
                }
            comparison_rows[comparison][str(cell)] = result

        # Bare is descriptive budget evidence with no universal threshold.
        budget, budget_blocks = paired_ratio(blocks, "B", "throughput")
        gates.append(gate_record(
            f"primary-{cell}-A-B-budget", "A/B", "primary", cell, "throughput-budget", budget,
            "descriptive", "no-universal-threshold", True, "ADMIT", block_ratios=budget_blocks,
            note="fresh bare lower-bound budget; not pure engine overhead",
        ))

        if durability == "Process":
            process_specs = (
                ("D", "throughput", ">=", Fraction(97, 100), "REVERT"),
                ("D", "latency_p99_ns", "<=", Fraction(110, 100), "REVERT"),
                ("D", "allocation_calls", "<=", Fraction(105, 100), "REVERT"),
                ("D", "allocated_bytes", "<=", Fraction(105, 100), "REVERT"),
                ("D", "serialized_role_cpu_ns", "<=", Fraction(105, 100), "REVERT"),
                ("C", "throughput", ">=", Fraction(110, 100), "NARROW"),
                ("C", "latency_p99_ns", "<=", Fraction(110, 100), "NARROW"),
            )
            for reference, metric, operator, threshold, failure in process_specs:
                median, values = paired_ratio(blocks, reference, metric)
                passed = median >= threshold if operator == ">=" else median <= threshold
                gate = gate_record(
                    f"primary-{cell}-A-{reference}-{metric}", f"A/{reference}", "primary", cell,
                    metric, median, operator, threshold, passed, failure, block_ratios=values,
                )
                gates.append(gate)
                if not passed:
                    outcome_reasons[failure].append(gate["id"])
            for block, variants in blocks.items():
                a = variants["A"]
                d = variants["D"]
                if a["barrier_count"] != 0 or a["durability_degraded"]:
                    outcome_reasons["REVERT"].append(f"primary-{cell}-block-{block}-process-durability")
                if a["serialized_role_cpu_ns"] < 20 * sched_resolution or d["serialized_role_cpu_ns"] < 20 * sched_resolution:
                    outcome_reasons["INCONCLUSIVE"].append(f"primary-{cell}-block-{block}-cpu-resolution")
        else:
            for reference, throughput_floor in (("D", Fraction(90, 100)), ("C", Fraction(90, 100))):
                for metric, operator, threshold in (
                    ("throughput", ">=", throughput_floor),
                    ("latency_p99_ns", "<=", Fraction(110, 100)),
                ):
                    median, values = paired_ratio(blocks, reference, metric)
                    passed = median >= threshold if operator == ">=" else median <= threshold
                    failure, note = group_performance_outcome(blocks, reference) if not passed else ("NARROW", "literal pass")
                    gate = gate_record(
                        f"primary-{cell}-A-{reference}-{metric}", f"A/{reference}", "primary", cell,
                        metric, median, operator, threshold, passed, failure, block_ratios=values, note=note,
                    )
                    gates.append(gate)
                    if not passed:
                        outcome_reasons[failure].append(gate["id"])
            for reference in ("D", "C"):
                alloc_threshold = Fraction(105, 100) if reference == "D" else None
                if alloc_threshold is not None:
                    for metric in ("allocation_calls", "allocated_bytes"):
                        median, values = paired_ratio(blocks, reference, metric)
                        passed = median <= alloc_threshold
                        gate = gate_record(
                            f"primary-{cell}-A-{reference}-{metric}", f"A/{reference}", "primary", cell,
                            metric, median, "<=", alloc_threshold, passed, "NARROW", block_ratios=values,
                        )
                        gates.append(gate)
                        if not passed:
                            outcome_reasons["NARROW"].append(gate["id"])
                a_total = sum(blocks[block]["A"]["barrier_count"] for block in range(1, 5))
                ref_total = sum(blocks[block][reference]["barrier_count"] for block in range(1, 5))
                aggregate = Fraction(a_total, ref_total)
                deltas = [Fraction(blocks[block]["A"]["barrier_count"] - blocks[block][reference]["barrier_count"]) for block in range(1, 5)]
                median_delta = median_fraction(deltas)
                aggregate_limit = Fraction(10025, 10000) if reference == "D" else Fraction(1)
                for suffix, observed, operator, threshold in (
                    ("aggregate-barriers", aggregate, "<=", aggregate_limit),
                    ("median-block-barrier-delta", median_delta, "<=", Fraction(0)),
                ):
                    passed = observed <= threshold
                    gate = gate_record(
                        f"primary-{cell}-A-{reference}-{suffix}", f"A/{reference}", "primary", cell,
                        suffix, observed, operator, threshold, passed, "NARROW",
                    )
                    gates.append(gate)
                    if not passed:
                        outcome_reasons["NARROW"].append(gate["id"])

    # A/C Process aggregate material gates.
    aggregate_specs = (
        ("throughput", Fraction(120, 100), ">="),
        ("allocation_calls", Fraction(90, 100), "<="),
        ("allocated_bytes", Fraction(95, 100), "<="),
    )
    for metric, threshold, operator in aggregate_specs:
        medians = [paired_ratio(primary[cell], "C", metric)[0] for cell in process_cells]
        product = Fraction(1)
        for value in medians:
            product *= value
        product_threshold = threshold ** len(medians)
        passed = product >= product_threshold if operator == ">=" else product <= product_threshold
        display = f"{float(product) ** (1 / len(medians)):.9f}"
        gate = gate_record(
            f"primary-process-geomean-A-C-{metric}", "A/C", "primary", "all-process-cells",
            f"geomean-{metric}-exact-product", product, operator, product_threshold, passed, "NARROW",
            note=f"geomean display={display}; decision compares exact product over {len(medians)} cells",
        )
        gates.append(gate)
        if not passed:
            outcome_reasons["NARROW"].append(gate["id"])

    # New-name throughput gates and structural durability-spine evidence.
    new_names = paired_rows(tracks["new_names"])
    for cell, blocks in sorted(new_names.items(), key=lambda item: str(item[0])):
        durability = cell[0]
        for reference, threshold in (
            ("D", Fraction(97 if durability == "Process" else 90, 100)),
            ("C", Fraction(105 if durability == "Process" else 95, 100)),
        ):
            observed, values = paired_ratio(blocks, reference, "throughput")
            passed = observed >= threshold
            if durability == "Group" and not passed:
                failure, note = group_performance_outcome(blocks, reference)
            else:
                failure, note = "NARROW", ""
            gate = gate_record(
                f"new-names-{cell}-A-{reference}-throughput", f"A/{reference}", "new_names", cell,
                "throughput", observed, ">=", threshold, passed, failure, block_ratios=values, note=note,
            )
            gates.append(gate)
            if not passed:
                outcome_reasons[failure].append(gate["id"])
    for row in tracks["structural_traces"]:
        if row["trace_kind"] != "new_names" or row["variant"] != "A":
            continue
        if row["durability"] == "Process":
            passed = (
                row["sync_family_calls"] == 0
                and row["group_count"] == 0
                and row["barrier_count"] == 0
            )
        else:
            passed = (
                row["group_count"] > 0
                and row["log_sync_calls"] == row["barrier_count"] == row["group_count"]
                and row["metadata_sync_calls"] == 0
            )
        gate = gate_record(
            f"new-names-structural-{row['row_ordinal']}", "A/structure", "structural_traces",
            (row["durability"], 250, 1, row["writers"]), "durability-spine", row["sync_family_calls"],
            "exact", "Process=0;Group=log=barrier=group,metadata=0", passed, "NARROW",
        )
        gates.append(gate)
        if not passed:
            outcome_reasons["NARROW"].append(gate["id"])

    # Fairness gates: warm rows are included in fsync evidence but never in
    # measured latency/throughput. Group misses deterministically narrow.
    fairness = paired_rows(tracks["fairness"])
    for cell, blocks in sorted(fairness.items(), key=lambda item: str(item[0])):
        for block in range(1, 5):
            row = blocks[block]["A"]
            for metric, operator, threshold in (
                ("jain_ppb", ">=", 990_000_000),
                ("min_to_median_rate_ppb", ">=", 500_000_000),
                ("max_to_median_p99_ppb", "<=", 2_000_000_000),
            ):
                observed = row[metric]
                passed = observed >= threshold if operator == ">=" else observed <= threshold
                gate = gate_record(
                    f"fairness-{cell}-block-{block}-{metric}", "A/absolute", "fairness", cell,
                    metric, observed, operator, threshold, passed, "NARROW",
                )
                gates.append(gate)
                if not passed:
                    outcome_reasons["NARROW"].append(gate["id"])
            reservations_pass = (
                row["waiter_reservations_after"] == 0
                and row["byte_reservations_after"] == 0
            )
            reservation_gate = gate_record(
                f"fairness-{cell}-block-{block}-quiescent-reservations",
                "A/absolute", "fairness", cell, "waiter-and-byte-reservations",
                f"{row['waiter_reservations_after']},{row['byte_reservations_after']}",
                "==", "0,0", reservations_pass, "REVERT",
                note="independent of pre/post focused boundedness correctness",
            )
            gates.append(reservation_gate)
            if not reservations_pass:
                outcome_reasons["REVERT"].append(reservation_gate["id"])
        observed, values = paired_ratio(blocks, "D", "throughput")
        threshold = Fraction(95 if cell[0] == "Process" else 90, 100)
        passed = observed >= threshold
        gate = gate_record(
            f"fairness-{cell}-A-D-throughput", "A/D", "fairness", cell, "throughput",
            observed, ">=", threshold, passed, "NARROW", block_ratios=values,
            note="Group fairness misses cannot use device-variance exception",
        )
        gates.append(gate)
        if not passed:
            outcome_reasons["NARROW"].append(gate["id"])
        if cell[0] == "Group":
            a_total = sum(blocks[block]["A"]["barrier_count"] for block in range(1, 5))
            d_total = sum(blocks[block]["D"]["barrier_count"] for block in range(1, 5))
            deltas = [Fraction(blocks[block]["A"]["barrier_count"] - blocks[block]["D"]["barrier_count"]) for block in range(1, 5)]
            for suffix, observed_value, threshold_value in (
                ("aggregate-barriers", Fraction(a_total, d_total), Fraction(10025, 10000)),
                ("median-block-barrier-delta", median_fraction(deltas), Fraction(0)),
            ):
                passed_value = observed_value <= threshold_value
                gate = gate_record(
                    f"fairness-{cell}-{suffix}", "A/D", "fairness", cell, suffix,
                    observed_value, "<=", threshold_value, passed_value, "NARROW",
                )
                gates.append(gate)
                if not passed_value:
                    outcome_reasons["NARROW"].append(gate["id"])

    # Syscall sentinel: Process no sync calls; A/D call counts <= 1.05.
    syscall = paired_rows(tracks["syscall_profiles"])
    for cell, blocks in sorted(syscall.items(), key=lambda item: str(item[0])):
        if cell[0] == "Process":
            for block in range(1, 5):
                row = blocks[block]["A"]
                observed = row["fsync"] + row["fdatasync"]
                passed = observed == 0
                gate = gate_record(
                    f"syscall-{cell}-block-{block}-process-sync", "A/absolute", "syscall_profiles", cell,
                    "sync-family-calls", observed, "==", 0, passed, "NARROW",
                )
                gates.append(gate)
                if not passed:
                    outcome_reasons["NARROW"].append(gate["id"])
        for metric_set, fields in (
            ("write-like", ("write", "pwrite64", "writev", "pwritev", "pwritev2")),
            ("sync-family", ("fsync", "fdatasync")),
        ):
            ratios: list[Fraction] = []
            for block in range(1, 5):
                a_total = sum(blocks[block]["A"][field] for field in fields)
                d_total = sum(blocks[block]["D"][field] for field in fields)
                if d_total == 0:
                    ratios.append(Fraction(1) if a_total == 0 else Fraction(a_total, 1))
                else:
                    ratios.append(Fraction(a_total, d_total))
            observed = median_fraction(ratios)
            passed = observed <= Fraction(105, 100)
            gate = gate_record(
                f"syscall-{cell}-A-D-{metric_set}", "A/D", "syscall_profiles", cell,
                f"{metric_set}-calls-per-append", observed, "<=", Fraction(105, 100), passed,
                "NARROW", block_ratios=ratios,
            )
            gates.append(gate)
            if not passed:
                outcome_reasons["NARROW"].append(gate["id"])

    # Reopen medians, exact corpus digest, and zero payload decoding.
    reopen = tracks["reopen"]
    by_variant: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in reopen:
        by_variant[row["variant"]].append(row)
        if row["recovery_payload_decodes"] != 0:
            outcome_reasons["NARROW"].append(f"reopen-{row['row_ordinal']}-payload-decodes")
    digests = {row["logical_digest"] for row in reopen}
    if len(digests) != 1:
        outcome_reasons["REVERT"].append("reopen-corpus-digest-mismatch")
    for reference in ("C", "D"):
        for metric in ("wall_ns", "peak_rss_bytes"):
            a_median = median_fraction([Fraction(row[metric]) for row in by_variant["A"]])
            ref_median = median_fraction([Fraction(row[metric]) for row in by_variant[reference]])
            observed = a_median / ref_median
            passed = observed <= Fraction(110, 100)
            gate = gate_record(
                f"reopen-A-{reference}-{metric}", f"A/{reference}", "reopen", "2m-corpus",
                metric, observed, "<=", Fraction(110, 100), passed, "NARROW",
            )
            gates.append(gate)
            if not passed:
                outcome_reasons["NARROW"].append(gate["id"])

    if outcome_reasons["REVERT"]:
        outcome = "REVERT"
    elif outcome_reasons["INCONCLUSIVE"]:
        outcome = "INCONCLUSIVE"
    elif outcome_reasons["NARROW"]:
        outcome = "NARROW"
    else:
        outcome = "ADMIT"
    summary = {
        "comparisons": comparison_rows,
        "schedstat_resolution_ns": sched_resolution,
        "reasons": {key: values for key, values in sorted(outcome_reasons.items()) if values},
        "gate_counts": {
            "total": len(gates),
            "passed": sum(gate["pass"] for gate in gates),
            "failed": sum(not gate["pass"] for gate in gates),
        },
    }
    return gates, summary, outcome


def build_artifact_bindings(
    output_dir: Path, problems: Problems, *, correctness_only: bool
) -> dict[str, dict[str, Any]]:
    names = schema.expected_result_artifact_names(
        correctness_only=correctness_only
    )
    result: dict[str, dict[str, Any]] = {}
    for name in names:
        path = output_dir / name
        try:
            snapshot = problems.recalled(path)
            if snapshot is None:
                snapshot = problems.remember(
                    schema.snapshot_regular_file(path, expected_mode=0o444)
                )
            result[name] = {
                "sha256": snapshot.sha256,
                "bytes": snapshot.size,
            }
        except (OSError, ValueError) as error:
            problems.add(f"cannot bind result artifact {name}: {error}")
    return result


def validate_correctness_only_marker(
    marker: dict[str, Any] | None,
    marker_path: Path,
    correctness: Mapping[str, Any] | None,
    children: Sequence[dict[str, Any]],
    provenance: Mapping[str, Any] | None,
    attempt_nonce: str | None,
    historical_failures: Sequence[str],
    problems: Problems,
) -> tuple[bool, dict[str, Any]]:
    """Validate the zero-timing terminal and classify reproducibility."""

    pre_failed = sorted(
        {
            str(case.get("id"))
            for case in correctness.get("cases", [])
            if isinstance(correctness, Mapping)
            and isinstance(case, Mapping)
            and case.get("variant") == "A"
            and case.get("phase") == "pre"
            and case.get("status") == "FAIL"
        }
    ) if isinstance(correctness, Mapping) else []
    post_failed = sorted(
        {
            str(case.get("id"))
            for case in correctness.get("cases", [])
            if isinstance(correctness, Mapping)
            and isinstance(case, Mapping)
            and case.get("variant") == "A"
            and case.get("phase") == "post"
            and case.get("status") == "FAIL"
        }
    ) if isinstance(correctness, Mapping) else []
    historical_failure_objects = sorted(
        [
            {
                "variant": str(case.get("variant")),
                "phase": str(case.get("phase")),
                "id": str(case.get("id")),
            }
            for case in correctness.get("cases", [])
            if isinstance(case, Mapping)
            and case.get("variant") in {"C", "D"}
            and case.get("phase") == "oracle"
            and case.get("status") == "FAIL"
        ],
        key=lambda item: (item["variant"], item["phase"], item["id"]),
    ) if isinstance(correctness, Mapping) else []
    expected_trigger = (
        "mixed"
        if pre_failed and historical_failure_objects
        else "current"
        if pre_failed
        else "historical"
        if historical_failure_objects
        else None
    )
    timing_children = sum(
        child.get("kind") in schema.TRACK_EXECUTION_ORDER for child in children
    )
    if marker is None or not require_exact_keys(
        marker, set(schema.CORRECTNESS_ONLY_FIELDS),
        "correctness-only marker", problems,
    ):
        marker = {}
    exact = {
        "schema": schema.CORRECTNESS_ONLY_SCHEMA,
        "protocol": schema.PROTOCOL,
        "attempt_nonce": attempt_nonce,
        "trigger": expected_trigger,
        "current_pre_failed_case_ids": pre_failed,
        "current_post_failed_case_ids": post_failed,
        "historical_failed_cases": historical_failure_objects,
        "timing_child_records": 0,
    }
    for field, expected in exact.items():
        if marker.get(field) != expected:
            problems.add(f"correctness-only marker {field} mismatch")
    if expected_trigger is None:
        problems.add("correctness-only marker has no pre/oracle failed case")
    marker_historical = marker.get("historical_failed_cases")
    if isinstance(marker_historical, list):
        for ordinal, failure in enumerate(marker_historical, start=1):
            if not require_exact_keys(
                failure,
                set(schema.CORRECTNESS_ONLY_HISTORICAL_FAILURE_FIELDS),
                f"correctness-only historical failure {ordinal}",
                problems,
            ):
                continue
            if failure.get("variant") not in {"C", "D"}:
                problems.add(
                    f"correctness-only historical failure {ordinal} variant invalid"
                )
    parse_timestamp(marker.get("created_at"), "correctness-only marker created_at", problems)
    created_ns = marker.get("created_monotonic_ns")
    if not isinstance(created_ns, int) or isinstance(created_ns, bool) or created_ns <= 0:
        problems.add("correctness-only marker monotonic timestamp invalid")
    else:
        last_child_ns = max(
            (int(child.get("completed_monotonic_ns", 0)) for child in children),
            default=0,
        )
        if created_ns <= last_child_ns:
            problems.add("correctness-only marker predates child completion")
        completed_ns = provenance.get("completed_monotonic_ns") if isinstance(provenance, Mapping) else None
        if isinstance(completed_ns, int) and created_ns > completed_ns:
            problems.add("correctness-only marker postdates provenance completion")
    if timing_children != 0:
        problems.add("correctness-only marker has timing children")
    validate_exact_mode(marker_path, 0o444, "correctness-only marker", problems)
    marker_sha256 = problems.capture(
        "snapshot correctness-only marker", lambda: snapshot_sha256(marker_path, problems)
    )
    frozen_files = (
        provenance.get("host", {}).get("frozen_files", {})
        if isinstance(provenance, Mapping)
        else {}
    )
    if marker_sha256 is not None and frozen_files.get(str(marker_path)) != marker_sha256:
        problems.add("correctness-only marker is absent from frozen-file authority")

    fault_bounds: dict[str, Any] = {}
    for child in children:
        context = child.get("context", {})
        if (
            child.get("kind") == "fault"
            and context.get("variant") == "A"
            and context.get("suite") == "current-fault"
            and context.get("phase") in {"pre", "post"}
        ):
            raw = read_canonical_object(
                Path(str(child.get("raw_path"))),
                f"correctness-only {context.get('phase')} fault bounds",
                problems,
            )
            if raw is not None:
                fault_bounds[str(context["phase"])] = raw.get("boundedness")
    bounds_reproduced = (
        set(fault_bounds) == {"pre", "post"}
        and fault_bounds["pre"] == schema.CORRECTNESS_EXPECTED_BOUNDEDNESS
        and fault_bounds["post"] == schema.CORRECTNESS_EXPECTED_BOUNDEDNESS
    )
    if not bounds_reproduced:
        problems.add(
            "correctness-only pre/post boundedness differs from exact authority"
        )
    reproducible = (
        bool(pre_failed)
        and pre_failed == post_failed
        and bounds_reproduced
        and not historical_failure_objects
        and timing_children == 0
    )
    return reproducible, {
        "pre_failed_case_ids": pre_failed,
        "post_failed_case_ids": post_failed,
        "bounds_reproduced": bounds_reproduced,
        "historical_failures": historical_failure_objects,
        "timing_child_records": timing_children,
    }


def build_report_data(
    tracks: Mapping[str, list[dict[str, Any]]],
    gates: Sequence[dict[str, Any]],
    comparison_rows: Mapping[str, Any],
    config: Mapping[str, Any] | None,
    historical_path: Path,
    correctness: Mapping[str, Any] | None,
    children: Sequence[dict[str, Any]],
    current_correctness_failures: Sequence[str],
) -> dict[str, Any]:
    """Assemble all deterministic, report-driving facts from accepted rows."""

    barrier_totals: list[dict[str, Any]] = []
    for track, rows in sorted(tracks.items()):
        grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)
        for row in rows:
            if "barrier_count" in row:
                grouped[(row.get("variant"), *cell_key(row))].append(row)
        for identity, group_rows in sorted(grouped.items(), key=lambda item: str(item[0])):
            barrier_totals.append(
                {
                    "track": track,
                    "variant": identity[0],
                    "cell": list(identity[1:]),
                    "rows": len(group_rows),
                    "barrier_count": sum(int(row["barrier_count"]) for row in group_rows),
                    "group_count": sum(int(row.get("group_count", 0)) for row in group_rows),
                }
            )

    degraded_rows = [
        {
            "track": track,
            "row_ordinal": row.get("row_ordinal"),
            "variant": row.get("variant"),
            "block": row.get("block"),
            "cell": list(cell_key(row)),
            "barrier_count": row.get("barrier_count"),
        }
        for track, rows in sorted(tracks.items())
        for row in rows
        if row.get("durability_degraded") is True
    ]
    fairness_diagnostics = [
        {
            "row_ordinal": row.get("row_ordinal"),
            "variant": row.get("variant"),
            "durability": row.get("durability"),
            "batch_size": row.get("batch_size"),
            "queue_depth": row.get("queue_depth"),
            "queue_bytes": row.get("queue_bytes"),
            "group_width_distribution": row.get("group_width_distribution"),
            "adaptive_group_width_target": row.get(
                "adaptive_group_width_target"
            ),
            "oldest_queued_age_ns": row.get("oldest_queued_age_ns"),
        }
        for row in tracks.get("fairness", [])
    ]

    cases = correctness.get("cases", []) if isinstance(correctness, Mapping) else []
    correctness_commands: list[dict[str, Any]] = []
    executions = config.get("correctness_execution", []) if isinstance(config, Mapping) else []
    for execution in executions if isinstance(executions, list) else []:
        if not isinstance(execution, dict):
            continue
        identity = tuple(execution.get(field) for field in ("variant", "phase", "suite", "kind"))
        matching_cases = [
            {
                "id": case.get("id"),
                "classification": case.get("classification"),
                "status": case.get("status"),
            }
            for case in cases
            if isinstance(case, dict)
            and tuple(case.get(field) for field in ("variant", "phase", "suite", "kind")) == identity
        ]
        matching_child = next(
            (
                child
                for child in children
                if child.get("kind") == identity[3]
                and child.get("context", {}).get("variant") == identity[0]
                and child.get("context", {}).get("phase") == identity[1]
                and child.get("context", {}).get("suite") == identity[2]
            ),
            {},
        )
        correctness_commands.append(
            {
                "variant": identity[0],
                "phase": identity[1],
                "suite": identity[2],
                "kind": identity[3],
                "argv": execution.get("argv"),
                "environment": execution.get("environment"),
                "child_ordinal": matching_child.get("ordinal"),
                "exit_status": matching_child.get("exit_status"),
                "harness_sound": correctness.get("harness_sound") if isinstance(correctness, Mapping) else None,
                "cases": matching_cases,
            }
        )

    primary = paired_rows(tracks.get("primary", []))
    residual_budgets: list[dict[str, Any]] = []
    ac_process: list[Fraction] = []
    ab_all: list[Fraction] = []
    for cell, blocks in sorted(primary.items(), key=lambda item: str(item[0])):
        if not all(reference in blocks.get(block, {}) for block in range(1, 5) for reference in ("A", "B")):
            continue
        median, values = paired_ratio(blocks, "B", "throughput")
        ab_all.append(median)
        residual_budgets.append(
            {
                "cell": list(cell),
                "batch_size": cell[2],
                "a_over_b_throughput": ratio_string(median),
                "residual_to_bare": ratio_string(Fraction(1) - median),
                **fraction_statistics(values, higher_is_better=True),
            }
        )
        if cell[0] == "Process" and all(
            "C" in blocks.get(block, {}) for block in range(1, 5)
        ):
            ac_process.append(paired_ratio(blocks, "C", "throughput")[0])

    failed_gate_ids = {gate["id"] for gate in gates if gate.get("pass") is not True}
    mechanism_specs = (
        ("process-owned-append", lambda gate: gate.get("track") == "primary" and (gate.get("cell") or [None])[0] == "Process"),
        ("group-flat-owner", lambda gate: gate.get("track") == "primary" and (gate.get("cell") or [None])[0] == "Group"),
        ("single-log-new-name-durability", lambda gate: gate.get("track") in {"new_names", "structural_traces"}),
        ("owner-ring-fairness-and-boundedness", lambda gate: gate.get("track") == "fairness"),
        ("syscall-shape", lambda gate: gate.get("track") == "syscall_profiles"),
        ("sealed-reopen-recovery", lambda gate: gate.get("track") == "reopen"),
    )
    mechanisms = []
    for name, predicate in mechanism_specs:
        relevant = [gate["id"] for gate in gates if predicate(gate)]
        declined = sorted(set(relevant) & failed_gate_ids)
        if current_correctness_failures:
            declined.extend(f"correctness:{item}" for item in current_correctness_failures)
        mechanisms.append(
            {
                "mechanism": name,
                "status": "declined" if declined else "admitted",
                "reasons": declined,
            }
        )

    def exact_range(values: Sequence[Fraction]) -> dict[str, str] | None:
        if not values:
            return None
        return {
            "minimum": ratio_string(min(values)),
            "median": ratio_string(median_fraction(values)),
            "maximum": ratio_string(max(values)),
        }

    historical_rows: dict[tuple[str, int, int, int], list[dict[str, str]]] = defaultdict(list)
    with historical_path.open("r", encoding="ascii", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("engine") == "log" and row.get("kind") == "stable":
                historical_rows[
                    (
                        row["mode"].capitalize(), int(row["payload"]),
                        int(row["batch"]), int(row["writers"]),
                    )
                ].append(row)
    historical_orientation: list[dict[str, Any]] = []
    fresh_primary: dict[tuple[str, tuple[Any, ...]], list[dict[str, Any]]] = defaultdict(list)
    for row in tracks.get("primary", []):
        if row.get("variant") in {"A", "D"}:
            fresh_primary[(row["variant"], cell_key(row))].append(row)
    for (variant, cell), fresh_rows in sorted(fresh_primary.items(), key=lambda item: str(item[0])):
        old_rows = historical_rows.get(tuple(cell), [])
        if not old_rows:
            continue
        fresh_throughput = median_fraction(
            [Fraction(row["domain_events"] * 1_000_000_000, row["wall_ns"]) for row in fresh_rows]
        )
        old_throughput = median_fraction([Fraction(row["ev_s"]) for row in old_rows])
        fresh_p99 = median_fraction([Fraction(row["latency_p99_ns"]) for row in fresh_rows])
        old_p99 = median_fraction([Fraction(row["p99_us"]) * 1000 for row in old_rows])
        historical_orientation.append(
            {
                "variant": variant,
                "cell": list(cell),
                "fresh_over_historical_log_throughput": ratio_string(
                    fresh_throughput / old_throughput
                ),
                "fresh_over_historical_log_p99": ratio_string(fresh_p99 / old_p99),
                "fresh_samples": len(fresh_rows),
                "historical_samples": len(old_rows),
            }
        )

    return {
        "per_cell_metric_statistics": comparison_rows,
        "barrier_totals": barrier_totals,
        "degraded_rows": degraded_rows,
        "fairness_diagnostics": fairness_diagnostics,
        "correctness_commands": correctness_commands,
        "phase4_mechanisms": mechanisms,
        "residual_a_b_budgets": residual_budgets,
        "plain_english": {
            "a_over_fjall_process_throughput": exact_range(ac_process),
            "a_over_bare_throughput": exact_range(ab_all),
            "bare_caveat": (
                "A/B prices the complete public API and topology relative to a fresh bare lower bound; "
                "it is not pure engine overhead and has no universal admission threshold."
            ),
        },
        "historical_orientation": {
            "source_path": str(historical_path),
            "source_sha256": sha256_file(historical_path),
            "rows": historical_orientation,
            "note": (
                "Descriptive cross-topology orientation only; any difference is a "
                "cross-topology and/or host/device difference and cannot enter a gate."
            ),
        },
    }


def build_correctness_only_report_data(
    details: Mapping[str, Any],
    config: Mapping[str, Any] | None,
    correctness: Mapping[str, Any] | None,
    children: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    """Build the no-matrix report without touching performance tracks."""

    cases = correctness.get("cases", []) if isinstance(correctness, Mapping) else []
    executions = config.get("correctness_execution", []) if isinstance(config, Mapping) else []
    commands: list[dict[str, Any]] = []
    for execution in executions if isinstance(executions, list) else []:
        if not isinstance(execution, dict):
            continue
        identity = tuple(
            execution.get(field) for field in ("variant", "phase", "suite", "kind")
        )
        child = next(
            (
                item
                for item in children
                if item.get("kind") == identity[3]
                and item.get("context", {}).get("variant") == identity[0]
                and item.get("context", {}).get("phase") == identity[1]
                and item.get("context", {}).get("suite") == identity[2]
            ),
            {},
        )
        commands.append(
            {
                "variant": identity[0],
                "phase": identity[1],
                "suite": identity[2],
                "kind": identity[3],
                "argv": execution.get("argv"),
                "environment": execution.get("environment"),
                "child_ordinal": child.get("ordinal"),
                "exit_status": child.get("exit_status"),
                "harness_sound": (
                    correctness.get("harness_sound")
                    if isinstance(correctness, Mapping)
                    else None
                ),
                "cases": [
                    {
                        "id": case.get("id"),
                        "classification": case.get("classification"),
                        "status": case.get("status"),
                    }
                    for case in cases
                    if isinstance(case, Mapping)
                    and tuple(
                        case.get(field)
                        for field in ("variant", "phase", "suite", "kind")
                    )
                    == identity
                ],
            }
        )
    return {
        "correctness_only": True,
        "timing_rows": 0,
        "current_pre_failed_case_ids": details.get("pre_failed_case_ids", []),
        "current_post_failed_case_ids": details.get("post_failed_case_ids", []),
        "historical_failed_cases": details.get("historical_failures", []),
        "bounds_reproduced": details.get("bounds_reproduced"),
        "correctness_commands": commands,
        "per_cell_metric_statistics": {},
        "barrier_totals": [],
        "degraded_rows": [],
        "fairness_diagnostics": [],
        "phase4_mechanisms": [],
        "residual_a_b_budgets": [],
        "plain_english": {
            "a_over_fjall_process_throughput": None,
            "a_over_bare_throughput": None,
            "bare_caveat": (
                "No performance claim is available: this terminal intentionally "
                "contains zero timing rows."
            ),
        },
        "historical_orientation": {
            "source_path": None,
            "source_sha256": None,
            "rows": [],
            "note": "Not evaluated in the correctness-only terminal.",
        },
    }


def atomic_write_new_bytes(path: Path, data: bytes) -> None:
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{hashlib.sha256(data).hexdigest()}.tmp"
    )
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
        0o600,
    )
    try:
        offset = 0
        while offset < len(data):
            offset += os.write(descriptor, data[offset:])
        os.fsync(descriptor)
        os.fchmod(descriptor, 0o444)
    finally:
        os.close(descriptor)
    try:
        os.link(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_new(path: Path, value: Mapping[str, Any]) -> None:
    atomic_write_new_bytes(path, canonical_json_bytes(value))


def render_report(result: Mapping[str, Any]) -> bytes:
    """Render the one canonical human-readable decision summary."""

    gate_counts = result.get("summary", {}).get("gate_counts", {})
    failures = result.get("gate_failures", [])
    errors = result.get("errors", [])
    report_data = result.get("summary", {}).get("report_data", {})

    def percent(exact: str) -> str:
        try:
            return f"{float(Fraction(exact)) * 100:.2f}%"
        except (ValueError, ZeroDivisionError):
            return "invalid"

    lines = [
        "# Asterism Rebaseline Evaluation",
        "",
        f"Protocol: `{result.get('protocol')}`",
        f"Outcome: **{result.get('outcome')}**",
        f"Evidence mode: `{result.get('evidence_mode')}`",
        f"Evidence valid: {'yes' if result.get('evidence_valid') is True else 'no'}",
        f"Matrix complete: {'yes' if result.get('matrix_complete') is True else 'no'}",
        f"Gates: {gate_counts.get('passed', 0)} passed, {gate_counts.get('failed', 0)} failed, {gate_counts.get('total', 0)} total",
        "Gate failures: " + (", ".join(str(item) for item in failures) if failures else "none"),
        f"Evidence errors: {len(errors) if isinstance(errors, list) else 'invalid'}",
        "",
        "## Plain-English performance summary",
        "",
    ]
    if report_data.get("correctness_only") is True:
        lines.extend(
            [
                "This is a correctness-only terminal with exactly zero timing rows; "
                "it makes no performance claim.",
                "",
                "## Correctness-only failure identity",
                "",
                "Current pre failures: `"
                + json.dumps(
                    report_data.get("current_pre_failed_case_ids", []),
                    separators=(",", ":"),
                )
                + "`",
                "Current post failures: `"
                + json.dumps(
                    report_data.get("current_post_failed_case_ids", []),
                    separators=(",", ":"),
                )
                + "`",
                "Historical failures: `"
                + json.dumps(
                    report_data.get("historical_failed_cases", []),
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "`",
                f"Pre/post boundedness reproduced: {report_data.get('bounds_reproduced')}",
                "",
            ]
        )
    plain = report_data.get("plain_english", {}) if isinstance(report_data, dict) else {}
    fjall = plain.get("a_over_fjall_process_throughput") if isinstance(plain, dict) else None
    bare = plain.get("a_over_bare_throughput") if isinstance(plain, dict) else None
    if isinstance(fjall, dict):
        lines.append(
            "Against the Fjall-era production engine, current Process throughput is "
            f"{percent(fjall['minimum'])} to {percent(fjall['maximum'])} cell-by-cell "
            f"(median {percent(fjall['median'])}), using fresh paired A/C measurements."
        )
    else:
        lines.append("The fresh A/Fjall Process comparison is unavailable because the evidence is incomplete.")
    if isinstance(bare, dict):
        lines.append(
            "Against fresh bare logging, the complete public path delivers "
            f"{percent(bare['minimum'])} to {percent(bare['maximum'])} of bare throughput "
            f"across primary cells (median {percent(bare['median'])})."
        )
    else:
        lines.append("The fresh A/bare comparison is unavailable because the evidence is incomplete.")
    lines.extend(
        [
            str(plain.get("bare_caveat", "A/B is descriptive budget evidence, not pure engine overhead.")),
            "",
            "## Per-cell metric observations and exact statistics",
            "",
            "Each observation is one within-block A/reference ratio. Decisions use exact fractions; no display rounding enters a gate.",
            "",
        ]
    )
    comparisons = report_data.get("per_cell_metric_statistics", {}) if isinstance(report_data, dict) else {}
    for comparison, cells in sorted(comparisons.items()):
        for cell, metrics in sorted(cells.items()):
            for metric, stats in sorted(metrics.items()):
                lines.append(
                    f"- {comparison} cell `{cell}` metric `{metric}`: observations="
                    f"{','.join(stats.get('observations', stats.get('block_ratios', [])))}; "
                    f"median={stats.get('median', stats.get('median_ratio'))}; "
                    f"best-of-four={stats.get('best_of_four')}; min={stats.get('minimum')}; "
                    f"max={stats.get('maximum')}; MAD={stats.get('median_absolute_deviation')}"
                )
    lines.extend(["", "## Every declared gate", ""])
    for gate in result.get("gates", []):
        statistics = gate.get("statistics") or {}
        lines.append(
            f"- {'PASS' if gate.get('pass') else 'FAIL'} `{gate.get('id')}`: "
            f"observed={gate.get('observed')} {gate.get('operator')} {gate.get('threshold')}; "
            f"block-ratios={','.join(gate.get('block_ratios', [])) or 'not-applicable'}; "
            f"median={statistics.get('median', gate.get('observed'))}; "
            f"best-of-four={statistics.get('best_of_four', 'not-applicable')}; "
            f"min={statistics.get('minimum', 'not-applicable')}; "
            f"max={statistics.get('maximum', 'not-applicable')}; "
            f"MAD={statistics.get('median_absolute_deviation', 'not-applicable')}; "
            f"failure-outcome={gate.get('failure_outcome')}; note={gate.get('note') or 'none'}"
        )
    lines.extend(["", "## Raw barrier and group totals", ""])
    for item in report_data.get("barrier_totals", []) if isinstance(report_data, dict) else []:
        lines.append(
            f"- track={item['track']} variant={item['variant']} cell={item['cell']} rows={item['rows']} "
            f"groups={item['group_count']} barriers={item['barrier_count']}"
        )
    lines.extend(["", "## Durability-degraded rows", ""])
    degraded = report_data.get("degraded_rows", []) if isinstance(report_data, dict) else []
    if not degraded:
        lines.append("- none")
    for item in degraded:
        lines.append(
            f"- track={item['track']} row={item['row_ordinal']} variant={item['variant']} "
            f"block={item['block']} cell={item['cell']} barriers={item['barrier_count']}"
        )
    lines.extend(["", "## Fairness diagnostic availability", ""])
    fairness_diagnostics = (
        report_data.get("fairness_diagnostics", [])
        if isinstance(report_data, dict)
        else []
    )
    if not fairness_diagnostics:
        lines.append("- no fairness timing rows in this evidence mode")
    for item in fairness_diagnostics:
        lines.append(
            f"- row={item['row_ordinal']} variant={item['variant']} "
            f"durability={item['durability']} batch={item['batch_size']}: "
            f"queue-depth={item['queue_depth']}; queue-bytes={item['queue_bytes']}; "
            "group-width-distribution="
            f"{item['group_width_distribution']}; adaptive-group-width-target="
            f"{item['adaptive_group_width_target']}; oldest-queued-age-ns="
            f"{item['oldest_queued_age_ns']}"
        )
    lines.extend(["", "## Correctness and fault commands", ""])
    for command in report_data.get("correctness_commands", []) if isinstance(report_data, dict) else []:
        statuses = ",".join(
            f"{case['id']}={case['status']}" for case in command.get("cases", [])
        )
        lines.append(
            f"- {command['variant']}/{command['phase']}/{command['suite']}/{command['kind']}: "
            f"argv={json.dumps(command.get('argv'), separators=(',', ':'))}; "
            f"environment={json.dumps(command.get('environment'), sort_keys=True, separators=(',', ':'))}; "
            f"child={command.get('child_ordinal')} exit={command.get('exit_status')} "
            f"harness-sound={command.get('harness_sound')}; cases={statuses}"
        )
    lines.extend(["", "## Phase-4 mechanism decision", ""])
    for mechanism in report_data.get("phase4_mechanisms", []) if isinstance(report_data, dict) else []:
        lines.append(
            f"- {mechanism['status'].upper()} `{mechanism['mechanism']}`: "
            f"{','.join(mechanism['reasons']) if mechanism['reasons'] else 'all applicable gates passed'}"
        )
    lines.extend(["", "## Residual public-to-bare budget by primary cell and batch", ""])
    for budget in report_data.get("residual_a_b_budgets", []) if isinstance(report_data, dict) else []:
        lines.append(
            f"- cell={budget['cell']} batch={budget['batch_size']}: A/B={budget['a_over_b_throughput']} "
            f"({percent(budget['a_over_b_throughput'])}); residual-to-bare={budget['residual_to_bare']}; "
            f"observations={','.join(budget['observations'])}; median={budget['median']}; "
            f"best-of-four={budget['best_of_four']}; min={budget['minimum']}; max={budget['maximum']}; "
            f"MAD={budget['median_absolute_deviation']}"
        )
    historical = report_data.get("historical_orientation", {}) if isinstance(report_data, dict) else {}
    lines.extend(["", "## Historical direct-LogEngine orientation (non-decision)", ""])
    lines.append(f"Source: `{historical.get('source_path')}` SHA-256 `{historical.get('source_sha256')}`")
    lines.append(str(historical.get("note", "Historical orientation is unavailable.")))
    for item in historical.get("rows", []) if isinstance(historical, dict) else []:
        lines.append(
            f"- variant={item['variant']} cell={item['cell']}: fresh/historical-log throughput="
            f"{item['fresh_over_historical_log_throughput']}; fresh/historical-log p99="
            f"{item['fresh_over_historical_log_p99']}; fresh-n={item['fresh_samples']} "
            f"historical-n={item['historical_samples']}"
        )
    lines.extend(
        [
            "",
            "The A/B quotient includes the public async API, record construction, topology, and engine path. It must not be described as pure engine overhead.",
            "",
            "This file is generated from the canonical `result.json` decision.",
            "",
        ]
    )
    return "\n".join(lines).encode("utf-8")


def evaluate_directory(
    output_dir: Path,
    *,
    synthetic: bool,
    publish: bool,
    correctness_only: bool = False,
    corpus_authority_fd: int | None = None,
) -> tuple[dict[str, Any], int]:
    problems = Problems()
    corpus_execution_authority = capture_corpus_execution_authority(
        problems,
        synthetic=synthetic,
        provided_fd=corpus_authority_fd,
    )
    try:
        output_dir = output_dir.resolve(strict=True)
    except OSError as error:
        result = {
            "schema": schema.RESULT_SCHEMA,
            "protocol": schema.PROTOCOL,
            "evidence_mode": "synthetic" if synthetic else "admission",
            "outcome": "INCONCLUSIVE",
            "exit_code": EXIT_INCONCLUSIVE,
            "evidence_valid": False,
            "matrix_complete": False,
            "errors": [
                *problems.errors,
                f"cannot resolve output directory: {error}",
            ],
            "gate_failures": [],
            "gates": [],
            "summary": {},
            "artifacts": {},
            "evaluated_at": datetime.now(UTC).isoformat(),
            "evaluator_path": str(Path(__file__).resolve()),
            "evaluator_sha256": sha256_file(Path(__file__).resolve()),
        }
        return result, EXIT_INCONCLUSIVE
    if not output_dir.is_dir():
        problems.add("output path is not a directory")
    if publish and any((output_dir / name).exists() for name in (RESULT_NAME, "REPORT.md")):
        problems.add("result.json or REPORT.md already exists; evidence is single-use")
    for name in TERMINAL_NAMES:
        if (output_dir / name).exists():
            problems.add(f"terminal artifact exists before evaluation: {name}")
    failure_path = output_dir / "failure.json"
    if failure_path.exists():
        problems.add("failure.json exists")
    correctness_only_path = output_dir / "correctness-only.json"
    if not correctness_only and correctness_only_path.exists():
        problems.add("correctness-only marker exists in full-matrix evaluation")

    protocol_path = output_dir / schema.PREPARED_INPUT_FILENAMES["protocol"]
    protocol_sha256 = problems.capture(
        "snapshot attempt protocol", lambda: snapshot_sha256(protocol_path, problems)
    ) or ""
    if protocol_sha256 != schema.PROTOCOL_SHA256:
        problems.add("attempt protocol hash differs from the approved v3 protocol")
    validate_exact_mode(protocol_path, 0o444, "attempt protocol", problems)
    config_path = output_dir / "config.json"
    approval_path = output_dir / "source-approval.json"
    prepared_path = output_dir / "prepared-artifacts.json"
    provenance_path = output_dir / "provenance.json"
    correctness_path = output_dir / "correctness.json"
    config = read_canonical_object(config_path, "config", problems)
    approval = read_canonical_object(approval_path, "source approval", problems)
    prepared = read_canonical_object(prepared_path, "prepared artifacts", problems)
    provenance = read_canonical_object(provenance_path, "provenance", problems)
    correctness = read_canonical_object(correctness_path, "correctness", problems)
    config_sha256 = problems.capture(
        "snapshot config", lambda: snapshot_sha256(config_path, problems)
    ) or ""
    approval_sha256 = problems.capture(
        "snapshot source approval", lambda: snapshot_sha256(approval_path, problems)
    ) or ""
    prepared_sha256 = problems.capture(
        "snapshot prepared artifacts", lambda: snapshot_sha256(prepared_path, problems)
    ) or ""
    validate_exact_mode(approval_path, 0o444, "source-approval.json", problems)
    validate_exact_mode(prepared_path, 0o444, "prepared-artifacts.json", problems)

    prepared_source_binding = (
        prepared.get("source_approval") if isinstance(prepared, Mapping) else None
    )
    original_source_approval_sha256 = (
        prepared_source_binding.get("sha256")
        if isinstance(prepared_source_binding, Mapping)
        else None
    )
    validate_config(
        config,
        protocol_sha256,
        original_source_approval_sha256,
        problems,
    )
    repo = Path(__file__).parents[2]
    validate_source_approval(
        approval,
        config,
        protocol_sha256,
        repo,
        problems,
        synthetic=synthetic,
    )
    provenance_paths = validate_provenance(
        provenance,
        output_dir,
        protocol_sha256,
        config,
        problems,
        synthetic=synthetic,
        correctness_only=correctness_only,
    )
    sched_resolution = validate_profile_contract(
        provenance_paths.get("profile_contract"), config, problems
    )
    binaries, attempt_inputs = validate_prepared_artifacts(
        prepared,
        prepared_path,
        output_dir,
        approval,
        config,
        approval_path,
        approval_sha256,
        config_path,
        config_sha256,
        protocol_sha256,
        provenance.get("lease") if isinstance(provenance, Mapping) else None,
        problems,
        synthetic=synthetic,
    )
    validate_live_python_bindings(prepared, problems, synthetic=synthetic)

    if prepared is not None and provenance is not None:
        support_map = {
            "runner": "runner",
            "evaluator": "evaluator",
            "terminal_verifier": "terminal_verifier",
            "schema": "evidence_schema",
            "profile_adapter": "profile_adapter",
        }
        for prefix, support_name in support_map.items():
            binding = prepared.get("support_files", {}).get(support_name, {})
            path = provenance_paths.get(prefix)
            if path is not None and (
                str(path) != binding.get("path")
                or provenance.get(f"{prefix}_sha256") != binding.get("sha256")
            ):
                problems.add(f"prepared support binding differs for {prefix}")
        correctness_binding = prepared.get("tools", {}).get("correctness", {})
        correctness_path = provenance_paths.get("correctness_executable")
        if correctness_path is not None and (
            str(correctness_path) != correctness_binding.get("path")
            or provenance.get("correctness_executable_sha256") != correctness_binding.get("sha256")
        ):
            problems.add("prepared tool binding differs for correctness")
        frozen_files = provenance.get("host", {}).get("frozen_files", {})
        for name in schema.PREPARED_INPUT_NAMES:
            path = attempt_inputs.get(name)
            if path is None or frozen_files.get(str(path)) != schema.PREPARED_INPUT_SHA256[name]:
                problems.add(f"attempt input {name} is absent from the frozen-file authority")

    if correctness_only:
        tracks = {}
        for filename in schema.CSV_FILENAMES.values():
            if (output_dir / filename).exists() or (output_dir / filename).is_symlink():
                problems.add(f"correctness-only evidence contains timing CSV {filename}")
    else:
        tracks = read_csv_tracks(
            output_dir,
            config,
            protocol_sha256,
            config.get("attempt_nonce") if config else None,
            problems,
        )
    child_records = read_jsonl(output_dir / "child-manifest.jsonl", "child manifest", problems)
    raw_records = read_jsonl(output_dir / "raw-manifest.json", "raw manifest", problems)
    guard_records = read_jsonl(output_dir / "guard-manifest.jsonl", "guard manifest", problems)
    if provenance is not None:
        host = provenance.get("host", {})
        if host.get("child_records") != len(child_records):
            problems.add("provenance host child record count mismatch")
        if host.get("guard_records") != len(guard_records):
            problems.add("provenance host guard record count mismatch")
        if prepared is not None and host.get("tracked_comm") != prepared.get("comm_allowlist"):
            problems.add("provenance host tracked comm differs from prepared allowlist")
    validate_raw_manifest(raw_records, child_records, output_dir, problems)
    profile_adapter = provenance_paths.get("profile_adapter", Path("missing-profile-adapter"))
    scratch_root = (
        Path(provenance["host"]["scratch_root"])
        if provenance is not None
        and isinstance(provenance.get("host"), dict)
        and isinstance(provenance["host"].get("scratch_root"), str)
        else None
    )
    attempt_nonce = config.get("attempt_nonce") if config else None
    corpus_authority_records = validate_corpus_execution_authority(
        corpus_execution_authority,
        scratch_root,
        attempt_nonce,
        child_records,
        problems,
        correctness_only=correctness_only,
    )
    expected_transitions = reconstruct_transition_authority(
        prepared,
        output_dir,
        scratch_root,
        attempt_nonce,
        child_records,
        corpus_authority_records,
        problems,
    )
    validate_transition_config_authority(config, expected_transitions, problems)
    validate_row_children(
        child_records,
        output_dir,
        config,
        config_path,
        binaries,
        profile_adapter,
        prepared,
        prepared_path,
        prepared_sha256,
        approval_path,
        approval_sha256,
        expected_transitions,
        tracks,
        len(guard_records),
        scratch_root,
        attempt_nonce,
        problems,
        require_matrix=not correctness_only,
    )
    validate_guards(
        guard_records,
        child_records,
        output_dir,
        provenance.get("lease") if provenance else None,
        prepared.get("comm_allowlist", []) if prepared else [],
        problems,
    )
    current_failures, historical_failures = validate_correctness(
        correctness,
        config,
        prepared,
        child_records,
        attempt_nonce,
        output_dir,
        scratch_root,
        problems,
        require_matrix=not correctness_only,
    )
    correctness_only_reproducible = False
    correctness_only_details: dict[str, Any] = {}
    if correctness_only:
        marker = read_canonical_object(
            correctness_only_path, "correctness-only marker", problems
        )
        correctness_only_reproducible, correctness_only_details = (
            validate_correctness_only_marker(
                marker,
                correctness_only_path,
                correctness,
                child_records,
                provenance,
                config.get("attempt_nonce") if config else None,
                historical_failures,
                problems,
            )
        )

    matrix_complete = not correctness_only and all(
        len(tracks.get(track, [])) == count
        for track, count in schema.EXPECTED_CARDINALITY.items()
    )
    gates: list[dict[str, Any]] = []
    summary: dict[str, Any] = {}
    provisional = "INCONCLUSIVE"
    if matrix_complete:
        gate_result = problems.capture(
            "evaluate gates",
            lambda: evaluate_gates(
                tracks, current_failures, historical_failures, sched_resolution
            ),
        )
        if gate_result is not None:
            gates, summary, provisional = gate_result
    elif correctness_only:
        provisional = "REVERT" if correctness_only_reproducible else "INCONCLUSIVE"
        correctness_only_reasons: list[str] = []
        if correctness_only_details.get("historical_failures"):
            correctness_only_reasons.append("historical-oracle-failure")
        if (
            correctness_only_details.get("pre_failed_case_ids")
            != correctness_only_details.get("post_failed_case_ids")
            or correctness_only_details.get("bounds_reproduced") is not True
        ):
            correctness_only_reasons.append(
                "current-correctness-failure-not-exactly-reproduced"
            )
        if correctness_only_reproducible:
            correctness_only_reasons = [
                "reproducible-current-correctness-failure"
            ]
        if not correctness_only_reasons:
            correctness_only_reasons = ["correctness-only-trigger-invalid"]
        summary = {
            "reasons": {provisional: correctness_only_reasons},
            "correctness_only": correctness_only_details,
            "gate_counts": {"total": 0, "passed": 0, "failed": 0},
        }
    summary = dict(summary)
    if correctness_only:
        summary["report_data"] = problems.capture(
            "build correctness-only report data",
            lambda: build_correctness_only_report_data(
                correctness_only_details, config, correctness, child_records
            ),
        ) or {}
    else:
        summary["report_data"] = problems.capture(
            "build report data",
            lambda: build_report_data(
                tracks,
                gates,
                summary.get("comparisons", {}),
                config,
                attempt_inputs.get(
                    "historical_baseline",
                    output_dir / "missing-historical-orientation.csv",
                ),
                correctness,
                child_records,
                current_failures,
            ),
        ) or {}
    artifacts = build_artifact_bindings(
        output_dir, problems, correctness_only=correctness_only
    )
    evidence_valid = not problems.errors and (matrix_complete or correctness_only)
    outcome = provisional if evidence_valid else "INCONCLUSIVE"
    if not evidence_valid:
        summary = dict(summary)
        reasons = dict(summary.get("reasons", {}))
        reasons["INCONCLUSIVE"] = ["invalid-or-incomplete-evidence"]
        summary["reasons"] = reasons
    exit_code = {
        "ADMIT": EXIT_ADMIT,
        "NARROW": EXIT_NARROW,
        "REVERT": EXIT_REVERT,
        "INCONCLUSIVE": EXIT_INCONCLUSIVE,
    }[outcome]
    gate_failures = [gate["id"] for gate in gates if not gate["pass"]]
    result = {
        "schema": schema.RESULT_SCHEMA,
        "protocol": schema.PROTOCOL,
        "evidence_mode": (
            "correctness-only"
            if correctness_only
            else "synthetic" if synthetic else "admission"
        ),
        "outcome": outcome,
        "exit_code": exit_code,
        "evidence_valid": evidence_valid,
        "matrix_complete": matrix_complete,
        "errors": problems.errors,
        "gate_failures": gate_failures,
        "gates": gates,
        "summary": summary,
        "artifacts": artifacts,
        "evaluated_at": datetime.now(UTC).isoformat(),
        "evaluator_path": str(Path(__file__).resolve()),
        "evaluator_sha256": sha256_file(Path(__file__).resolve()),
    }
    if publish:
        try:
            atomic_write_new_bytes(output_dir / "REPORT.md", render_report(result))
            atomic_write_new(output_dir / RESULT_NAME, result)
        except OSError as error:
            # A publication failure cannot be represented by overwriting or
            # repairing the output.  The runner observes the nonzero exit and
            # retains the incomplete attempt.
            return result, EXIT_INTERNAL if error.errno != errno.EEXIST else EXIT_INCONCLUSIVE
    return result, exit_code


def fixture_write_json(path: Path, value: Mapping[str, Any], mode: int = 0o444) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value))
    path.chmod(mode)


def fixture_write_file(path: Path, data: bytes, mode: int = 0o444) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    path.chmod(mode)


def fixture_manifest(root: Path, path: Path) -> str:
    entries = []
    for item in sorted(root.rglob("*")):
        if not item.is_file():
            continue
        data = item.read_bytes()
        entries.append(
            {
                "path": item.relative_to(root).as_posix(),
                "mode": "0555" if item.stat().st_mode & 0o111 else "0444",
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )
    value = {
        "schema": schema.FILE_MANIFEST_SCHEMA,
        "protocol": schema.PROTOCOL,
        "root": str(root),
        "entries": entries,
    }
    fixture_write_json(path, value)
    return sha256_file(path)


def fixture_full_corpus_payload(variant: str) -> bytes:
    return f"fixture-full-reopen-corpus-{variant}\n".encode()


def fixture_full_corpus_content_sha256(variant: str) -> str:
    payload = fixture_full_corpus_payload(variant)
    entries = [
        {
            "path": "corpus.bin",
            "kind": "file",
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
    ]
    return hashlib.sha256(canonical_json_bytes(entries)).hexdigest()


def synthetic_corpus_execution_authority_payload(
    output: Path,
    *,
    correctness_only: bool,
) -> bytes:
    """Capture a synthetic fixture's pre-mutation runner authority bytes."""

    config = schema.parse_canonical_json_object(
        (output / "config.json").read_bytes(), "synthetic config"
    )
    provenance = schema.parse_canonical_json_object(
        (output / "provenance.json").read_bytes(), "synthetic provenance"
    )
    scratch_root = Path(str(provenance["host"]["scratch_root"]))
    attempt_nonce = str(config["attempt_nonce"])
    plan = expected_corpus_execution_records(
        scratch_root,
        attempt_nonce,
        correctness_only=correctness_only,
    )
    records: list[dict[str, Any]] = []
    for binding in plan:
        snapshot = _snapshot_corpus_tree(
            Path(str(binding["root"])),
            allow_missing=False,
            root_mode=int(binding["root_mode"]),
            directory_mode=int(binding["directory_mode"]),
            file_mode=int(binding["file_mode"]),
            include_modes=True,
        )
        if snapshot is None:
            raise AssertionError("synthetic corpus authority tree is missing")
        records.append(
            {
                **binding,
                "entries": list(snapshot.entries),
                "tree_sha256": snapshot.sha256,
            }
        )
    return canonical_json_bytes(
        {
            "schema": CORPUS_EXECUTION_AUTHORITY_SCHEMA,
            "protocol": schema.PROTOCOL,
            "protocol_sha256": schema.PROTOCOL_SHA256,
            "attempt_nonce": attempt_nonce,
            "correctness_only": correctness_only,
            "records": records,
        }
    )


def evaluate_synthetic_fixture(
    output: Path,
    authority_payload: bytes,
    *,
    publish: bool,
    correctness_only: bool = False,
) -> tuple[dict[str, Any], int]:
    descriptor = create_corpus_execution_authority_fd(authority_payload)
    return evaluate_directory(
        output,
        synthetic=True,
        publish=publish,
        correctness_only=correctness_only,
        corpus_authority_fd=descriptor,
    )


def fixture_common_row(
    track: str,
    identity: Mapping[str, Any],
    protocol_sha256: str,
    nonce: str,
    sequence: int,
) -> dict[str, Any]:
    variant = identity["variant"]
    durability = identity["durability"]
    batch = identity["batch_size"]
    writers = identity["writers"]
    if track == "primary":
        bpw = (schema.PROCESS_BPW if durability == "Process" else schema.GROUP_BPW)[batch]
    elif track == "new_names":
        bpw = schema.NEW_NAME_BPW[durability]
    else:
        bpw = schema.FAIRNESS_BPW[(durability, batch)]
    appends = writers * bpw
    events = appends * batch
    wall = {"A": 1_000_000_000, "D": 1_000_000_000, "C": 1_500_000_000, "B": 800_000_000}[variant]
    base = 10_000_000_000 + sequence * 2_000_000_000
    allocation_factor = {"A": 50, "D": 50, "C": 70, "B": 20}[variant]
    cpu_factor = {"A": 40, "D": 40, "C": 60, "B": 20}[variant]
    control_events = 0
    if variant in {"A", "D"}:
        control_events = appends + 1 if track == "new_names" else writers + 1
    group_count = max(1, appends // 10) if durability == "Group" else 0
    fsync_count = group_count
    if track == "fairness" and durability == "Group":
        fsync_count += 4
    owned = variant == "A" and durability == "Process"
    borrowed = variant in {"C", "D"} or (variant == "A" and durability == "Group")
    path_label = (
        "owned"
        if owned
        else "borrowed-compatible"
        if variant == "A"
        else "borrowed"
        if variant in {"C", "D"}
        else "raw-numeric"
    )
    digest_key = json.dumps(
        {key: identity[key] for key in ("durability", "payload_size", "batch_size", "writers")},
        sort_keys=True,
    ).encode()
    row: dict[str, Any] = {
        "schema": schema.ROW_SCHEMAS[track],
        "protocol": schema.PROTOCOL,
        "protocol_sha256": protocol_sha256,
        "attempt_nonce": nonce,
        "track": track,
        "row_ordinal": identity["row_ordinal"],
        "block": identity["block"],
        "cell_ordinal": identity["cell_ordinal"],
        "variant": variant,
        "durability": durability,
        "payload_size": identity["payload_size"],
        "batch_size": batch,
        "writers": writers,
        "batches_per_writer": bpw,
        "store_id": f"fixture-{track}-{identity['row_ordinal']:04d}",
        "store_absent_before": True,
        "ready_monotonic_ns": base,
        "counter_start_monotonic_ns": base + 1,
        "t0_monotonic_ns": base + 2,
        "release_monotonic_ns": base + 3,
        "last_completion_monotonic_ns": base + 1 + wall,
        "t1_monotonic_ns": base + 2 + wall,
        "counter_end_monotonic_ns": base + 3 + wall,
        "wall_ns": wall,
        "appends": appends,
        "accepted_batches": appends,
        "conflicts": 0,
        "domain_events": events,
        "visible_events": events,
        "log_events": events + control_events,
        "control_events": control_events,
        "payload_bytes": events * identity["payload_size"],
        "logical_digest": hashlib.sha256(digest_key).hexdigest(),
        "latency_samples": writers * (bpw if track == "fairness" else bpw - bpw // 10),
        "latency_p50_ns": 50,
        "latency_p99_ns": 100,
        "latency_max_ns": 150,
        "allocation_calls": allocation_factor * events,
        "allocated_bytes": allocation_factor * 64 * events,
        "path_label": path_label,
        "owned_batches": appends if owned else 0,
        "owned_records": events if owned else 0,
        "owned_payload_bytes": events * identity["payload_size"] if owned else 0,
        "borrowed_batches": appends if borrowed else 0,
        "borrowed_records": events if borrowed else 0,
        "borrowed_payload_bytes": events * identity["payload_size"] if borrowed else 0,
        "defensive_copy_records": schema.NOT_AVAILABLE if variant in {"C", "D"} else 0,
        "defensive_copy_bytes": schema.NOT_AVAILABLE if variant in {"C", "D"} else 0,
        "process_user_cpu_ns": cpu_factor * events,
        "process_system_cpu_ns": cpu_factor * events // 4,
        "serialized_role": {"A": "mess-flat-owner", "D": "mess-flat-owner", "C": "fjall-committer", "B": "bare-committer"}[variant],
        "serialized_role_tid": 10_000 + sequence,
        "serialized_role_start_ticks": 20_000 + sequence,
        "serialized_role_cpu_ns": cpu_factor * events,
        "serialized_role_voluntary_switches": appends,
        "serialized_role_nonvoluntary_switches": 0,
        "group_count": group_count,
        "group_batches": appends if durability == "Group" else 0,
        "group_events": events if durability == "Group" else 0,
        "barrier_count": group_count,
        "fsync_count": fsync_count,
        "fsync_total_ns": fsync_count * 100,
        "fsync_p50_ns": 100 if fsync_count else 0,
        "fsync_p95_ns": 100 if fsync_count else 0,
        "fsync_p99_ns": 100 if fsync_count else 0,
        "fsync_max_ns": 100 if fsync_count else 0,
        "durability_degraded": False,
        "write_like_calls": schema.NOT_AVAILABLE,
        "sync_family_calls": schema.NOT_AVAILABLE,
        "host_write_bytes": schema.NOT_AVAILABLE,
    }
    if track == "new_names":
        row.update(
            {
                "distinct_streams": appends,
                "registry_events": control_events,
                "opaque_cursor_monotone": True,
            }
        )
    if track == "fairness":
        samples = [
            {
                "writer": writer,
                "completed_appends": bpw,
                "completed_events": bpw * batch,
                "elapsed_ns": wall,
                "p50_ns": 50,
                "p99_ns": 100,
                "max_ns": 150,
            }
            for writer in range(64)
        ]
        row.update(
            {
                "warm_rounds": 4,
                "warm_writers": 64,
                "warm_names_established": True,
                "counter_snapshot_after_warm": True,
                "fsync_histogram_includes_warm": True,
                "writer_samples_json": samples,
                "jain_ppb": 1_000_000_000,
                "min_to_median_rate_ppb": 1_000_000_000,
                "max_to_median_p99_ppb": 1_000_000_000,
                "waiter_reservations_after": 0 if variant == "A" else schema.NOT_AVAILABLE,
                "byte_reservations_after": 0 if variant == "A" else schema.NOT_AVAILABLE,
                "queue_depth": schema.NOT_AVAILABLE,
                "queue_bytes": schema.NOT_AVAILABLE,
                "group_width_distribution": schema.NOT_AVAILABLE,
                "adaptive_group_width_target": schema.NOT_AVAILABLE,
                "oldest_queued_age_ns": schema.NOT_AVAILABLE,
            }
        )
    schema.validate_child_record(track, row, "fixture row")
    return row


def fixture_profile_row(
    track: str,
    identity: Mapping[str, Any],
    protocol_sha256: str,
    nonce: str,
) -> dict[str, Any]:
    variant = identity["variant"]
    durability = identity["durability"]
    batch = identity["batch_size"]
    bpw = (schema.PROCESS_BPW if durability == "Process" else schema.GROUP_BPW)[batch]
    appends = identity["writers"] * bpw
    events = appends * batch
    base = {
        "schema": schema.ROW_SCHEMAS[track],
        "protocol": schema.PROTOCOL,
        "protocol_sha256": protocol_sha256,
        "attempt_nonce": nonce,
        "track": track,
        "row_ordinal": identity["row_ordinal"],
        "block": identity["block"],
        "cell_ordinal": identity["cell_ordinal"],
        "variant": variant,
        "durability": durability,
        "payload_size": 250,
        "batch_size": batch,
        "writers": 4,
        "batches_per_writer": bpw,
        "appends": appends,
        "domain_events": events,
        "profile_timing_discarded": True,
    }
    if track == "cpu_profiles":
        role = {"A": "mess-flat-owner", "D": "mess-flat-owner", "C": "fjall-committer", "B": "bare-committer"}[variant]
        base.update(
            {
                "perf_permission": (
                    "not_available;perf_event_paranoid=4;scope=user-only;exit_status=255"
                ),
                "perf_control_acknowledged": False,
                "process_user_cpu_ns": events * 40,
                "process_system_cpu_ns": events * 10,
                "process_voluntary_switches": appends,
                "process_nonvoluntary_switches": 0,
                "schedstat_resolution_ns": 1,
                "role_samples_json": [
                    {
                        "role": role,
                        "tid": 30_000 + identity["row_ordinal"],
                        "start_ticks": 40_000 + identity["row_ordinal"],
                        "cpu_ns": max(20, events * 40),
                        "voluntary_switches": appends,
                        "nonvoluntary_switches": 0,
                    }
                ],
                "cycles": schema.NOT_AVAILABLE,
                "instructions": schema.NOT_AVAILABLE,
                "task_clock_ns": schema.NOT_AVAILABLE,
            }
        )
    else:
        groups = max(1, appends // 10) if durability == "Group" else 0
        base.update(
            {
                "begin_markers": 1,
                "end_markers": 1,
                "write": appends,
                "pwrite64": 0,
                "writev": 0,
                "pwritev": 0,
                "pwritev2": 0,
                "fsync": 0,
                "fdatasync": groups,
                "futex": appends,
                "file_create": 1,
                "file_rename": 0,
                "file_unlink": 0,
            }
        )
    schema.validate_child_record(track, base, "fixture profile row")
    return base


def fixture_reopen_row(
    identity: Mapping[str, Any],
    protocol_sha256: str,
    nonce: str,
    sequence: int,
    scratch_root: Path,
) -> dict[str, Any]:
    base = 1_000_000_000_000 + sequence * 2_000_000_000
    manifest = fixture_full_corpus_content_sha256(str(identity["variant"]))
    row = {
        "schema": schema.ROW_SCHEMAS["reopen"],
        "protocol": schema.PROTOCOL,
        "protocol_sha256": protocol_sha256,
        "attempt_nonce": nonce,
        "track": "reopen",
        "row_ordinal": identity["row_ordinal"],
        "latin_block": identity["latin_block"],
        "ordinal_in_block": identity["ordinal_in_block"],
        "variant": identity["variant"],
        "archive_manifest_sha256": manifest,
        "copy_manifest_sha256": manifest,
        "copy_id": schema.fresh_store_path(
            scratch_root,
            nonce,
            "reopen-corpus",
            int(identity["row_ordinal"]),
            str(identity["variant"]),
        ).name,
        "copy_absent_before": True,
        "copy_verified_read_only": True,
        "syncfs_complete": True,
        "cache_state": "warm-from-materialization",
        "boot_monotonic_ns": base,
        "runtime_monotonic_ns": base + 1,
        "ready_monotonic_ns": base + 2,
        "start_sent_monotonic_ns": base + 3,
        "open_start_monotonic_ns": base + 4,
        "opened_monotonic_ns": base + 1_000_000_004,
        "measured_monotonic_ns": base + 1_000_000_005,
        "release_monotonic_ns": base + 1_000_000_006,
        "wall_ns": 1_000_000_000,
        "peak_rss_bytes": 64 * 1024 * 1024,
        "proc_read_bytes": 4096,
        "proc_write_bytes": 0,
        "proc_read_syscalls": 10,
        "proc_write_syscalls": 0,
        "recovery_payload_decodes": 0,
        "domain_events": 2_000_000,
        "visible_events": 2_000_000,
        "log_events": 2_000_001,
        "logical_digest": hashlib.sha256(b"fixture-reopen-digest").hexdigest(),
        "registry_head_digest": hashlib.sha256(b"fixture-registry-head").hexdigest(),
    }
    schema.validate_child_record("reopen", row, "fixture reopen")
    return row


def fixture_structural_row(
    identity: Mapping[str, Any], protocol_sha256: str, nonce: str
) -> dict[str, Any]:
    process = identity["durability"] == "Process"
    groups = 0 if process else identity["appends_per_writer"] * identity["writers"]
    row = {
        "schema": schema.ROW_SCHEMAS["structural_traces"],
        "protocol": schema.PROTOCOL,
        "protocol_sha256": protocol_sha256,
        "attempt_nonce": nonce,
        "track": "structural_traces",
        **identity,
        "group_count": groups,
        "barrier_count": groups,
        "profile_timing_discarded": True,
        "begin_markers": 1,
        "end_markers": 1,
        "write_like_calls": max(1, identity["appends_per_writer"] * identity["writers"]),
        "sync_family_calls": 0 if process else groups,
        "log_sync_calls": 0 if process else groups,
        "metadata_sync_calls": groups if identity["variant"] == "C" and not process else 0,
        "openat": 10,
        "getdents64": 2,
        "read": 10,
        "pread64": 0,
        "files_opened": 10,
    }
    # C's historical metadata sync is additional to its log sync.
    if row["metadata_sync_calls"]:
        row["sync_family_calls"] = 2 * groups
    schema.validate_child_record("structural_traces", row, "fixture structural")
    return row


def build_synthetic_fixture(
    root: Path,
    *,
    correctness_only: bool = False,
    pre_failures: Sequence[str] = (),
    post_failures: Sequence[str] = (),
    historical_failure: bool = False,
) -> Path:
    output = root / "result"
    output.mkdir()
    prepared_bundle_root = root / "prepared-bundle"
    prepared_support = prepared_bundle_root / "support"
    prepared_inputs = prepared_bundle_root / "inputs"
    prepared_bindings = prepared_bundle_root / "bindings"
    prepared_support.mkdir(parents=True)
    prepared_inputs.mkdir()
    prepared_bindings.mkdir()
    tooling = root / "tooling"
    tooling.mkdir()
    protocol_path = Path(__file__).with_name("BN-2L3N-PROTOCOL.md")
    protocol_sha256 = sha256_file(protocol_path)
    historical_path = (
        Path(__file__).parents[2] / "spikes/baseline_matrix/BN-2SU-FINAL.csv"
    )
    if sha256_file(historical_path) != schema.HISTORICAL_BASELINE_SHA256:
        raise AssertionError("fixture historical baseline differs from frozen hash")
    prepared_protocol_path = prepared_bundle_root / schema.PREPARED_INPUT_RELATIVE_PATHS["protocol"]
    prepared_historical_path = prepared_bundle_root / schema.PREPARED_INPUT_RELATIVE_PATHS["historical_baseline"]
    fixture_write_file(prepared_protocol_path, protocol_path.read_bytes(), 0o444)
    fixture_write_file(prepared_historical_path, historical_path.read_bytes(), 0o444)
    fixture_write_file(
        output / schema.PREPARED_INPUT_FILENAMES["protocol"],
        protocol_path.read_bytes(),
        0o444,
    )
    fixture_write_file(
        output / schema.PREPARED_INPUT_FILENAMES["historical_baseline"],
        historical_path.read_bytes(),
        0o444,
    )
    prepared_inputs.chmod(0o555)
    nonce = hashlib.sha256(b"bn-2l3n-synthetic-attempt").hexdigest()
    fake_commit = "1" * 40
    fake_tree = "2" * 40
    synthetic_profile_adapter = b'''import hashlib
import json
import os
import stat


def profile_fields(track, finish_result, *, raw_point, control_events, authority, profile_inputs=None):
    if not isinstance(finish_result, dict) or finish_result.get("authority") != authority:
        raise ValueError("synthetic rich/authority binding differs")
    if raw_point.get("track") != track or finish_result.get("track") != track:
        raise ValueError("synthetic profile track differs")
    inputs = {} if profile_inputs is None else profile_inputs
    if track in {"syscall_profiles", "structural_traces"}:
        binding = inputs["trace_raw_artifact"]
        descriptor = os.open(binding["path"], os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
        try:
            before = os.fstat(descriptor)
            payload = b""
            while True:
                chunk = os.read(descriptor, 1024 * 1024)
                if not chunk:
                    break
                payload += chunk
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o444
            or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            or binding != {
                "path": binding["path"],
                "sha256": hashlib.sha256(payload).hexdigest(),
                "bytes": len(payload),
                "mode": 0o444,
            }
        ):
            raise ValueError("synthetic trace artifact binding differs")
        return json.loads(payload)
    return finish_result["process"]["profile_result"]
'''

    tooling_paths: dict[str, Path] = {}
    for name in (
        "evidence_schema.py",
        "evaluate.py",
        "verify_terminal.py",
        "run_rebaseline.py",
        "run_rebaseline.sh",
        "profile_adapters.py",
    ):
        path = prepared_support / name
        payload = (
            synthetic_profile_adapter
            if name == "profile_adapters.py"
            else f"fixture tooling {name}\n".encode()
        )
        fixture_write_file(path, payload, 0o444)
        tooling_paths[name] = path
    prepared_support.chmod(0o555)
    tool_comms = dict(schema.PREPARED_TOOL_COMMS)
    tool_paths: dict[str, Path] = {}
    for name in schema.PREPARED_TOOL_NAMES:
        path = tooling / "executables" / name
        fixture_write_file(path, f"fixture executable {name}\n".encode(), 0o555)
        tool_paths[name] = path
    cargo_path = tooling / "toolchain" / "cargo"
    rustc_path = tooling / "toolchain" / "rustc"
    rustup_path = tooling / "toolchain" / "rustup"
    bwrap_path = tooling / "toolchain" / "bwrap"
    git_path = tooling / "toolchain" / "git"
    fixture_write_file(cargo_path, b"fixture cargo\n", 0o555)
    fixture_write_file(rustc_path, b"fixture rustc\n", 0o555)
    fixture_write_file(rustup_path, b"fixture rustup\n", 0o555)
    fixture_write_file(bwrap_path, b"fixture bwrap\n", 0o555)
    fixture_write_file(git_path, b"fixture git\n", 0o555)
    cargo_home = tooling / "cargo-home"
    cargo_home.mkdir()
    cargo_config = cargo_home / "config.toml"
    fixture_write_file(cargo_config, b"", 0o444)
    cargo_home.chmod(0o555)
    rustup_home = tooling / "rustup-home"
    rustup_home.mkdir()
    rustup_home.chmod(0o555)
    toolchain = {
        "bwrap_path": str(bwrap_path),
        "bwrap_sha256": sha256_file(bwrap_path),
        "cargo_home_path": str(cargo_home),
        "cargo_path": str(cargo_path),
        "cargo_sha256": sha256_file(cargo_path),
        "cargo_version_verbose": "cargo 1.97.0\nrelease: 1.97.0\nhost: x86_64-unknown-linux-gnu\n",
        "git_path": str(git_path),
        "git_sha256": sha256_file(git_path),
        "rustc_path": str(rustc_path),
        "rustc_sha256": sha256_file(rustc_path),
        "rustc_version_verbose": "rustc 1.97.0\nrelease: 1.97.0\nhost: x86_64-unknown-linux-gnu\n",
        "rustc_host": "x86_64-unknown-linux-gnu",
        "rustup_home_path": str(rustup_home),
        "rustup_path": str(rustup_path),
        "rustup_sha256": sha256_file(rustup_path),
        "rustup_toolchain": "1.97.0-x86_64-unknown-linux-gnu",
    }

    def fixture_cargo_config_search(cwd: Path, name: str) -> dict[str, str]:
        path = tooling / "cargo-config-search" / f"{name}.json"
        entries = []
        for candidate in cargo_config_candidates(cwd.resolve(), cargo_home.resolve()):
            if candidate.exists() or candidate.is_symlink():
                entries.append(
                    {"path": str(candidate), "status": "present", "sha256": sha256_file(candidate)}
                )
            else:
                entries.append({"path": str(candidate), "status": "absent", "sha256": None})
        fixture_write_json(
            path,
            {
                "schema": schema.CARGO_CONFIG_SEARCH_SCHEMA,
                "cargo_home_path": str(cargo_home.resolve()),
                "cwd": str(cwd.resolve()),
                "entries": entries,
            },
        )
        return {"path": str(path), "sha256": sha256_file(path)}

    adapters: dict[str, Path] = {}
    binaries: dict[str, Path] = {}
    source_data: dict[str, dict[str, Any]] = {}
    current_lock = Path(__file__).parents[2] / "Cargo.lock"
    current_lock_bytes = current_lock.read_bytes()
    if hashlib.sha256(current_lock_bytes).hexdigest() != schema.CURRENT_LOCK_SHA256:
        raise AssertionError("fixture host Cargo.lock differs from frozen hash")
    for variant in schema.VARIANTS:
        adapter = root / "adapters" / f"{variant}.rs"
        fixture_write_file(adapter, f"fixture adapter {variant}\n".encode())
        adapters[variant] = adapter
        source_root = root / "sources" / variant
        source_root.mkdir(parents=True)
        lock_bytes = current_lock_bytes if variant in {"A", "B"} else f"fixture-lock-{variant}\n".encode()
        lock_path = source_root / "Cargo.lock"
        fixture_write_file(lock_path, lock_bytes)
        source_root.chmod(0o555)
        materialized_manifest_path = root / "manifests" / f"{variant}-materialized.json"
        materialized_manifest_sha = fixture_manifest(source_root, materialized_manifest_path)
        archive_root = root / "archive-inputs" / variant
        archive_root.mkdir(parents=True)
        fixture_write_file(archive_root / "source.txt", f"archive {variant}\n".encode())
        archive_manifest_path = root / "manifests" / f"{variant}-archive.json"
        archive_manifest_sha = fixture_manifest(archive_root, archive_manifest_path)
        archive_path = root / "archives" / f"{variant}.tar"
        fixture_write_file(archive_path, f"fixture git archive {variant}\n".encode())
        overlay_manifest_path = root / "manifests" / f"{variant}-overlay.json"
        fixture_write_json(
            overlay_manifest_path,
            {"schema": "fixture-overlay-v3", "variant": variant},
        )
        binary = root / "targets" / variant / "rebaseline-bench"
        fixture_write_file(binary, f"#!/bin/sh\n# fixture binary {variant}\n".encode(), 0o555)
        binaries[variant] = binary
        source_data[variant] = {
            "source_root": source_root,
            "lock_path": lock_path,
            "lock_sha256": hashlib.sha256(lock_bytes).hexdigest(),
            "materialized_manifest_path": materialized_manifest_path,
            "materialized_manifest_sha256": materialized_manifest_sha,
            "archive_manifest_path": archive_manifest_path,
            "archive_manifest_sha256": archive_manifest_sha,
            "archive_path": archive_path,
            "archive_sha256": sha256_file(archive_path),
            "overlay_manifest_path": overlay_manifest_path,
            "overlay_manifest_sha256": sha256_file(overlay_manifest_path),
            "binary_sha256": sha256_file(binary),
        }

    correctness_descriptors = schema.correctness_descriptors()

    def correctness_case_status(descriptor: Mapping[str, Any]) -> str:
        if descriptor.get("variant") == "A" and descriptor.get("phase") == "pre":
            return "FAIL" if descriptor.get("id") in pre_failures else "PASS"
        if descriptor.get("variant") == "A" and descriptor.get("phase") == "post":
            return "FAIL" if descriptor.get("id") in post_failures else "PASS"
        if descriptor.get("variant") == "C" and historical_failure:
            return "FAIL"
        return "PASS"

    row_template = [
        "{binary}",
        "--run-row",
        "--track",
        "{track}",
        "--row-ordinal",
        "{row_ordinal}",
        "--config",
        "{config}",
    ]

    def fixture_variant_transition_environment(
        variant: str,
        context: Mapping[str, Any],
        mode: str,
    ) -> dict[str, str]:
        environment = schema.expected_trace_marker_environment(variant)
        environment.pop("ASTERISM_REBASELINE_LOG_PATH_MARKERS", None)
        environment.pop("ASTERISM_REBASELINE_METADATA_PATH_MARKERS", None)
        environment.update(
            {
                "ASTERISM_REBASELINE_MODE": mode,
                "ASTERISM_REBASELINE_VARIANT": variant,
            }
        )
        if mode == "smoke":
            environment["ASTERISM_REBASELINE_SMOKE_TARGET"] = str(
                context["smoke_target"]
            )
        for field, name in schema.ROW_DYNAMIC_ENVIRONMENT_FIELDS.items():
            value = context.get(field)
            if value is not None:
                environment[name] = str(value)
        return environment

    transition_fixtures: list[dict[str, Any]] = [
        {
            "id": f"contract-{variant}",
            "variant": variant,
            "argv": [str(binaries[variant]), "--contract"],
            "kind": "contract",
            "context": {"transition": "contract", "variant": variant},
            "executable": binaries[variant],
            "plan_environment": schema.sanitized_contract_environment(toolchain),
            "controlled": False,
            "store_path": None,
            "profile_track": None,
        }
        for variant in schema.VARIANTS
    ]
    authority_order_config = {"cell_orders": exact_config_cell_orders()}
    ordinary_shapes: list[tuple[str, str]] = []
    for target in schema.RUNNER_SMOKE_TRACK_ORDER:
        for identity in schema.expected_order(authority_order_config, target):
            shape = (target, identity["variant"])
            if shape not in ordinary_shapes:
                ordinary_shapes.append(shape)
    for smoke_ordinal, (target, variant) in enumerate(ordinary_shapes, start=1):
        smoke_context: dict[str, Any] = {
            "transition": "smoke",
            "smoke_ordinal": smoke_ordinal,
            "smoke_target": target,
            "variant": variant,
            "durability": "Process",
        }
        store_path = schema.fresh_store_path(
            root, nonce, "smoke", smoke_ordinal, variant
        )
        if target in {"syscall_profiles", "structural_traces"}:
            smoke_context["variant_trace_path_markers"] = (
                schema.resolved_trace_path_markers(
                    store_path, variant
                )
            )
        transition_fixtures.append(
            {
                "id": f"{target}-{variant}",
                "variant": variant,
                "argv": [str(binaries[variant])],
                "kind": "smoke",
                "context": smoke_context,
                "executable": binaries[variant],
                "plan_environment": fixture_variant_transition_environment(
                    variant, smoke_context, "smoke"
                ),
                "controlled": True,
                "store_path": store_path,
                "profile_track": (
                    target
                    if target
                    in {"cpu_profiles", "syscall_profiles", "structural_traces"}
                    else None
                ),
            }
        )
    next_smoke_ordinal = len(ordinary_shapes) + 1
    for role in schema.RUNNER_SMOKE_TOOL_TARGETS:
        context = {
            "transition": "smoke",
            "smoke_ordinal": next_smoke_ordinal,
            "smoke_target": role,
            "variant": "A",
            "durability": "Process",
        }
        transition_fixtures.append(
            {
                "id": f"{role}-A",
                "variant": "A",
                "argv": [str(tool_paths[role]), "--smoke"],
                "kind": "smoke",
                "context": context,
                "executable": tool_paths[role],
                "plan_environment": {
                    "ASTERISM_REBASELINE_MODE": "smoke",
                    "ASTERISM_REBASELINE_SMOKE_TARGET": role,
                },
                "controlled": True,
                "store_path": schema.fresh_store_path(
                    root, nonce, "smoke", next_smoke_ordinal, "A"
                ),
                "profile_track": None,
            }
        )
        next_smoke_ordinal += 1
    runtime_support_filenames = {
        "evaluator": "evaluate.py",
        "terminal_verifier": "verify_terminal.py",
    }
    transition_fixtures.extend(
        [
            {
                "id": f"{role}-A",
                "variant": "A",
                "argv": [
                    str(tool_paths[tool]),
                    str(tooling_paths[runtime_support_filenames[support]]),
                    "--smoke",
                ],
                "kind": "smoke",
                "context": {
                    "transition": "smoke",
                    "smoke_ordinal": next_smoke_ordinal + ordinal - 1,
                    "smoke_target": role,
                    "variant": "A",
                    "durability": "Process",
                },
                "executable": tool_paths[tool],
                "plan_environment": {
                    "ASTERISM_REBASELINE_MODE": "smoke",
                    "ASTERISM_REBASELINE_SMOKE_TARGET": role,
                },
                "controlled": True,
                "store_path": None,
                "profile_track": None,
            }
            for ordinal, (role, tool, support) in enumerate(
                schema.RUNNER_SMOKE_RUNTIME_ROLES, start=1
            )
        ]
    )
    specialized_source_store = (
        root
        / "attempts"
        / nonce
        / "smoke-reopen"
        / "archive-source"
    )
    specialized_payload = b"fixture-specialized-reopen-corpus\n"
    fixture_write_file(specialized_source_store / "corpus.bin", specialized_payload)
    specialized_source_store.chmod(0o555)
    specialized_copy_stores = {
        ordinal: schema.fresh_store_path(
            root, nonce, "smoke-reopen-corpus", ordinal, "A"
        )
        for ordinal in (1, 2)
    }
    for store in specialized_copy_stores.values():
        fixture_write_file(store / "corpus.bin", specialized_payload, mode=0o644)
    if not correctness_only:
        for binding in expected_corpus_execution_records(
            root, nonce, correctness_only=False
        )[3:]:
            corpus_root = Path(str(binding["root"]))
            fixture_write_file(
                corpus_root / "corpus.bin",
                fixture_full_corpus_payload(str(binding["variant"])),
                mode=int(binding["file_mode"]),
            )
            corpus_root.chmod(int(binding["root_mode"]))
    corpus_problems = Problems()
    specialized_archive_digest = corpus_content_sha256(
        specialized_source_store,
        corpus_problems,
        "fixture specialized source corpus",
    )
    specialized_copy_digests = {
        ordinal: corpus_content_sha256(
            store,
            corpus_problems,
            f"fixture specialized copy {ordinal}",
        )
        for ordinal, store in specialized_copy_stores.items()
    }
    if corpus_problems.errors:
        raise AssertionError(corpus_problems.errors)
    specialized_logical_digest = str(
        SPECIALIZED_SEED_AUTHORITY["logical_digest"]
    )
    specialized_registry_digest = str(
        SPECIALIZED_SEED_AUTHORITY["registry_head_digest"]
    )
    seed_context = {
        "transition": "smoke",
        "smoke_id": "smoke_reopen_seed",
        "smoke_target": "smoke_reopen_seed",
        "variant": "A",
        "durability": "Process",
        "domain_events": 1,
        "streams": 1,
        "batch": 1,
        "payload": 64,
        "segment_bytes": 8 * 1024 * 1024,
    }
    transition_fixtures.append(
        {
            "id": "smoke_reopen_seed",
            "variant": "A",
            "argv": [str(binaries["A"])],
            "kind": "smoke_reopen_seed",
            "context": seed_context,
            "executable": binaries["A"],
            "plan_environment": fixture_variant_transition_environment(
                "A", seed_context, "smoke_reopen_seed"
            ),
            "controlled": True,
            "store_path": specialized_source_store,
            "profile_track": None,
        }
    )
    for ordinal, (transition_id, profile_track, smoke_target) in enumerate(
        (
            ("smoke_reopen", "reopen", "smoke_reopen"),
            (
                "smoke_structural_reopen",
                "structural_traces",
                "structural_traces",
            ),
        ),
        start=1,
    ):
        store = specialized_copy_stores[ordinal]
        context = {
            "transition": "smoke",
            "smoke_id": transition_id,
            "smoke_target": smoke_target,
            "profile_smoke_track": profile_track,
            "reopen_order": True,
            "trace_kind": (
                "reopen" if transition_id == "smoke_structural_reopen" else None
            ),
            "variant": "A",
            "durability": "Process",
            "archive_manifest_sha256": specialized_archive_digest,
            "copy_manifest_sha256": specialized_copy_digests[ordinal],
            "copy_id": store.name,
            "copy_verified_read_only": True,
            "expected_domain_events": SPECIALIZED_SEED_AUTHORITY[
                "domain_events"
            ],
            "expected_visible_events": SPECIALIZED_SEED_AUTHORITY[
                "visible_events"
            ],
            "expected_log_events": SPECIALIZED_SEED_AUTHORITY["log_events"],
            "expected_logical_digest": specialized_logical_digest,
            "expected_registry_head_digest": specialized_registry_digest,
        }
        if transition_id == "smoke_structural_reopen":
            context["variant_trace_path_markers"] = (
                schema.resolved_trace_path_markers(store, "A")
            )
        transition_fixtures.append(
            {
                "id": transition_id,
                "variant": "A",
                "argv": [str(binaries["A"])],
                "kind": transition_id,
                "context": context,
                "executable": binaries["A"],
                "plan_environment": fixture_variant_transition_environment(
                    "A", context, transition_id
                ),
                "controlled": True,
                "store_path": store,
                "profile_track": profile_track,
            }
        )
    smoke_transitions = [
        {
            "id": transition["id"],
            "variant": transition["variant"],
            "argv": transition["argv"],
        }
        for transition in transition_fixtures
    ]
    profile_contract = {
        "schema": schema.PROFILE_PREFLIGHT_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "profile_contract_sha256": schema.expected_profile_contract_sha256(),
        "source": "/proc/<pid>/task/<native-tid>/schedstat:first-field",
        "helper": "adapter-owned-cpu-bound-native-thread",
        "samples_ns": [0, 1, 2, 3],
        "minimum_nonzero_increment_ns": 1,
        "decision_multiplier": schema.SCHEDSTAT_DECISION_MULTIPLIER,
        "decision_floor_ns": schema.SCHEDSTAT_DECISION_MULTIPLIER,
    }
    profile_contract_path = output / "profile-contract.json"
    fixture_write_json(profile_contract_path, profile_contract)
    correctness_authority = {
        "variants": {
            variant: {
                "binary": {
                    "path": str(binaries[variant]),
                    "sha256": sha256_file(binaries[variant]),
                },
                "executable_mode": 0o555,
                "comm": schema.VARIANT_COMMS[variant],
            }
            for variant in schema.VARIANTS
        },
        "tools": {
            name: {
                "path": str(tool_paths[name]),
                "sha256": sha256_file(tool_paths[name]),
                "executable_mode": 0o555,
                "comm": tool_comms[name],
            }
            for name in ("correctness", "fault")
        },
    }
    support_filenames = {
        "runner": "run_rebaseline.py",
        "evaluator": "evaluate.py",
        "terminal_verifier": "verify_terminal.py",
        "evidence_schema": "evidence_schema.py",
        "profile_adapter": "profile_adapters.py",
        "strace_attach": "run_rebaseline.sh",
    }
    tools_manifest = {
        "schema": schema.TOOLS_MANIFEST_SCHEMA,
        "comm_allowlist": schema.expected_comm_allowlist(),
        "tools": {
            name: {
                "path": str(tool_paths[name]),
                "sha256": sha256_file(tool_paths[name]),
                "executable_mode": 0o555,
                "comm": tool_comms[name],
            }
            for name in schema.PREPARED_TOOL_NAMES
        },
        "support_files": {
            name: {
                "path": str(tooling_paths[filename]),
                "sha256": sha256_file(tooling_paths[filename]),
                "mode": 0o444,
            }
            for name, filename in support_filenames.items()
        },
    }
    tools_manifest_sha256 = hashlib.sha256(
        canonical_json_bytes(tools_manifest)
    ).hexdigest()

    approval = {
        "schema": schema.SOURCE_APPROVAL_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": protocol_sha256,
        "status": "approved",
        "review_id": "cr-synthetic",
        "reviewed_at": "2026-07-15T00:00:00+00:00",
        "tooling_commit": fake_commit,
        "tooling_tree": fake_tree,
        "toolchain": toolchain,
        "shared_manifest_sha256": hashlib.sha256(b"fixture-shared-manifest").hexdigest(),
        "tools_manifest": tools_manifest,
        "tools_manifest_sha256": tools_manifest_sha256,
        "filesystem_admission": {
            "schema": schema.FILESYSTEM_ADMISSION_SCHEMA,
            "checked_path": str(root.resolve()),
            "filesystem": schema.REQUIRED_FILESYSTEM_TYPE,
            "available_bytes": 200_000_000_000,
            "available_inodes": 2_000_000,
            "minimum_available_bytes": schema.MIN_FREE_BYTES,
            "minimum_available_inodes": schema.MIN_FREE_INODES,
        },
        "comm_allowlist": schema.expected_comm_allowlist(),
        "variants": {},
    }
    resolution_config_search = fixture_cargo_config_search(root, "resolution")
    for variant in schema.VARIANTS:
        binding = schema.VARIANT_SOURCE_BINDINGS[variant]
        approval["variants"][variant] = {
            "product_commit": binding["commit"],
            "product_tree": binding["tree"],
            "adapter_sha256": sha256_file(adapters[variant]),
            "cargo_lock_sha256": source_data[variant]["lock_sha256"],
            "overlay_manifest_sha256": source_data[variant]["overlay_manifest_sha256"],
            "allowed_overlay_paths": ["fixture/adapter.rs", "fixture/main.rs"],
            "lock_resolution": {
                "argv": [str(cargo_path), "generate-lockfile", "--offline"],
                "cwd": str(root),
                "exit_status": 0,
                "stdout_sha256": hashlib.sha256(b"").hexdigest(),
                "stderr_sha256": hashlib.sha256(b"").hexdigest(),
                "stdout": "",
                "stderr": "",
                "toolchain": toolchain,
                "environment": schema.sanitized_cargo_environment(toolchain),
                "cargo_config_search": resolution_config_search,
            },
            "binary_kind": "bare" if variant == "B" else "public",
            "timed_surface": "raw-numeric" if variant == "B" else "public-event-store",
            "correctness_oracle_mode": variant != "B",
            "profile_role_lifetime": (
                schema.PROFILE_C_ROLE_LIFETIME_CONTRACT
                if variant == "C"
                else "not_applicable"
            ),
            "trace_path_marker_templates": (
                schema.expected_trace_path_marker_templates(variant)
            ),
        }
        if variant in {"A", "B"}:
            approval["variants"][variant]["current_lock_attempt"] = None
        else:
            current_attempt = copy.deepcopy(
                approval["variants"][variant]["lock_resolution"]
            )
            current_attempt["exit_status"] = 1
            current_attempt["stderr"] = "fixture current lock rejected\n"
            current_attempt["stderr_sha256"] = hashlib.sha256(
                current_attempt["stderr"].encode()
            ).hexdigest()
            approval["variants"][variant]["current_lock_attempt"] = current_attempt
    original_approval_path = prepared_bindings / "source-approval.json"
    fixture_write_json(original_approval_path, approval)
    approval_sha256 = sha256_file(original_approval_path)
    approval_path = output / "source-approval.json"
    fixture_write_file(approval_path, original_approval_path.read_bytes())

    config = {
        "schema": schema.CONFIG_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": protocol_sha256,
        "approved": True,
        "review_id": "cr-synthetic",
        "tooling_commit": fake_commit,
        "tooling_tree": fake_tree,
        "attempt_nonce": nonce,
        "seed_sha256": exact_config_seed_sha256(approval_sha256),
        "cell_orders": exact_config_cell_orders(),
        "variant_sources": schema.VARIANT_SOURCE_BINDINGS,
        "lock_hashes": {
            variant: source_data[variant]["lock_sha256"]
            for variant in schema.VARIANTS
        },
        "resource_limits": {
            "free_bytes": schema.MIN_FREE_BYTES,
            "free_inodes": schema.MIN_FREE_INODES,
            "load1_milli": 6_000,
            "quiet_wait_seconds": 120,
        },
        "settle_ms": {"Process": 400, "Group": 4_000},
        "argv_templates": {
            "primary": row_template,
            "new_names": row_template,
            "fairness": row_template,
            "reopen": row_template,
            "cpu_profiles": row_template,
            "syscall_profiles": row_template,
            "structural_traces": row_template,
        },
        "smoke_transitions": smoke_transitions,
        "correctness_cases": correctness_descriptors,
        "correctness_execution": schema.correctness_execution_contract(
            correctness_authority, nonce
        ),
        "profile_contract_sha256": sha256_file(profile_contract_path),
    }
    config_path = output / "config.json"
    fixture_write_json(config_path, config)
    config_sha256 = sha256_file(config_path)

    prepared_variants: dict[str, Any] = {}
    previous_end = 100
    for index, variant in enumerate(schema.VARIANTS, start=1):
        item = source_data[variant]
        build_log = root / "logs" / f"{variant}-build.json"
        fixture_write_json(build_log, {"status": "PASS", "variant": variant})
        build_nonce = hashlib.sha256(f"fixture-build-{variant}".encode()).hexdigest()
        contract = {
            "schema": schema.BINARY_CONTRACT_SCHEMA,
            "protocol": schema.PROTOCOL,
            "protocol_sha256": protocol_sha256,
            "tooling_commit": fake_commit,
            "tooling_tree": fake_tree,
            "variant": variant,
            "product_commit": schema.VARIANT_SOURCE_BINDINGS[variant]["commit"],
            "product_tree": schema.VARIANT_SOURCE_BINDINGS[variant]["tree"],
            "adapter_sha256": sha256_file(adapters[variant]),
            "shared_manifest_sha256": approval["shared_manifest_sha256"],
            "cargo_lock_sha256": item["lock_sha256"],
            "source_approval_sha256": approval_sha256,
            "build_nonce": build_nonce,
            "binary_kind": "bare" if variant == "B" else "public",
            "timed_surface": "raw-numeric" if variant == "B" else "public-event-store",
            "correctness_oracle_mode": variant != "B",
            "profile_role_lifetime": (
                schema.PROFILE_C_ROLE_LIFETIME_CONTRACT
                if variant == "C"
                else "not_applicable"
            ),
            "contract_mode": True,
            "rows_written": 0,
        }
        contract_path = root / "contracts" / f"{variant}.json"
        fixture_write_json(contract_path, contract)
        target_dir = root / "targets" / variant
        sandbox_argv = [str(bwrap_path), "--unshare-net", "--ro-bind", "/", "/", "--", str(cargo_path), "build", "--locked", "--offline"]
        build_argv = ["cargo", "build", "--locked", "--offline"]
        contract_argv = [str(binaries[variant]), "--contract"]
        started = previous_end + 1
        completed = started + 10
        previous_end = completed

        def completed_child(argv: list[str], output_path: Path, start_ns: int) -> dict[str, Any]:
            return {
                "argv": argv,
                "cwd": str(item["source_root"]),
                "pid": 1000 + start_ns,
                "start_ticks": 2000 + start_ns,
                "started_at": "2026-07-15T00:00:00+00:00",
                "started_monotonic_ns": start_ns,
                "completed_at": "2026-07-15T00:00:01+00:00",
                "completed_monotonic_ns": start_ns + 1,
                "exit_status": 0,
                "waited_pid": 1000 + start_ns,
                "timed_out": False,
                "process_group_absent": True,
                "reaping": {"pid": 1000 + start_ns, "start_ticks": 2000 + start_ns, "status": "absent"},
                "output_path": str(output_path),
                "output_sha256": sha256_file(output_path),
            }

        prepared_variants[variant] = {
            "contract": contract,
            "binary": {
                "path": str(binaries[variant]),
                "sha256": item["binary_sha256"],
            },
            "executable_mode": 0o555,
            "artifact_root": str(target_dir),
            "contract_argv": contract_argv,
            "contract_env": schema.sanitized_contract_environment(toolchain),
            "comm": schema.VARIANT_COMMS[variant],
            "evidence_argv": [str(binaries[variant])],
            "evidence_env": schema.expected_trace_marker_environment(variant),
            "trace_path_marker_templates": (
                schema.expected_trace_path_marker_templates(variant)
            ),
            "correctness_oracle_mode": variant != "B",
            "attestation": {
                "source_commit": schema.VARIANT_SOURCE_BINDINGS[variant]["commit"],
                "source_tree": schema.VARIANT_SOURCE_BINDINGS[variant]["tree"],
                "source_archive_path": str(item["archive_path"]),
                "source_archive_sha256": item["archive_sha256"],
                "source_archive_bytes": item["archive_path"].stat().st_size,
                "archive_manifest_path": str(item["archive_manifest_path"]),
                "archive_manifest_sha256": item["archive_manifest_sha256"],
                "overlay_manifest_path": str(item["overlay_manifest_path"]),
                "overlay_manifest_sha256": item["overlay_manifest_sha256"],
                "materialized_root": str(item["source_root"]),
                "materialized_manifest_path": str(item["materialized_manifest_path"]),
                "materialized_manifest_sha256": item["materialized_manifest_sha256"],
                "materialized_manifest_pre_sha256": item["materialized_manifest_sha256"],
                "materialized_manifest_post_sha256": item["materialized_manifest_sha256"],
                "source_read_only": True,
                "cargo_lock_path": str(item["lock_path"]),
                "cargo_lock_sha256": item["lock_sha256"],
                "cargo_lock_pre_sha256": item["lock_sha256"],
                "cargo_lock_post_sha256": item["lock_sha256"],
                "target_dir": str(target_dir),
                "target_dir_was_absent": True,
                "build_nonce": build_nonce,
                "toolchain": toolchain,
                "build_argv": sandbox_argv,
                "build_env": {
                    **schema.sanitized_cargo_environment(toolchain),
                    "ASTERISM_BUILD_PROTOCOL": schema.PROTOCOL,
                    "ASTERISM_BUILD_PROTOCOL_SHA256": protocol_sha256,
                    "ASTERISM_BUILD_TOOLING_COMMIT": fake_commit,
                    "ASTERISM_BUILD_TOOLING_TREE": fake_tree,
                    "ASTERISM_BUILD_VARIANT": variant,
                    "ASTERISM_BUILD_PRODUCT_COMMIT": schema.VARIANT_SOURCE_BINDINGS[variant]["commit"],
                    "ASTERISM_BUILD_PRODUCT_TREE": schema.VARIANT_SOURCE_BINDINGS[variant]["tree"],
                    "ASTERISM_BUILD_ADAPTER_SHA256": sha256_file(adapters[variant]),
                    "ASTERISM_BUILD_BINARY_KIND": "bare" if variant == "B" else "public",
                    "ASTERISM_BUILD_SHARED_MANIFEST_SHA256": approval["shared_manifest_sha256"],
                    "ASTERISM_BUILD_CARGO_LOCK_SHA256": item["lock_sha256"],
                    "ASTERISM_BUILD_SOURCE_APPROVAL_SHA256": approval_sha256,
                    "ASTERISM_BUILD_TIMED_SURFACE": "raw-numeric" if variant == "B" else "public-event-store",
                    "ASTERISM_BUILD_NONCE": build_nonce,
                },
                "cargo_config_search": fixture_cargo_config_search(
                    item["source_root"], f"build-{variant}"
                ),
                "build_started_at": "2026-07-15T00:00:00+00:00",
                "build_started_monotonic_ns": started,
                "build_completed_at": "2026-07-15T00:00:01+00:00",
                "build_completed_monotonic_ns": completed,
                "build_log_path": str(build_log),
                "build_log_sha256": sha256_file(build_log),
                "build_child": completed_child(sandbox_argv, build_log, started),
                "contract_output_path": str(contract_path),
                "contract_output_sha256": sha256_file(contract_path),
                "contract_child": completed_child(contract_argv, contract_path, completed),
            },
        }
    claims_directory = prepared_bundle_root / "claims"
    bindings_directory = prepared_bindings
    prepared_tools_manifest_path = bindings_directory / "tools-manifest.json"
    fixture_write_json(prepared_tools_manifest_path, tools_manifest)
    bindings_directory.chmod(0o555)
    claims_directory.mkdir(parents=True)
    claims_directory.chmod(0o700)
    claim_path = claims_directory / "single-use-claim.json"
    prepared = {
        "schema": schema.PREPARED_ARTIFACTS_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": protocol_sha256,
        "tooling_commit": fake_commit,
        "tooling_tree": fake_tree,
        "created_at": "2026-07-15T00:00:02+00:00",
        "created_monotonic_ns": previous_end + 10,
        "source_approval": {
            "path": str(original_approval_path),
            "sha256": approval_sha256,
        },
        "tools_manifest": {
            "path": str(prepared_tools_manifest_path),
            "sha256": tools_manifest_sha256,
            "mode": 0o444,
        },
        "single_use_claim": {"path": str(claim_path)},
        "comm_allowlist": approval["comm_allowlist"],
        "tools": {
            name: {
                "path": str(tool_paths[name]),
                "sha256": sha256_file(tool_paths[name]),
                "executable_mode": 0o555,
                "comm": tool_comms[name],
            }
            for name in schema.PREPARED_TOOL_NAMES
        },
        "support_files": {
            name: {
                "path": str(tooling_paths[filename]),
                "sha256": sha256_file(tooling_paths[filename]),
                "mode": 0o444,
            }
            for name, filename in support_filenames.items()
        },
        "inputs": {
            "protocol": {
                "path": str(prepared_protocol_path),
                "sha256": schema.PROTOCOL_SHA256,
                "mode": 0o444,
            },
            "historical_baseline": {
                "path": str(prepared_historical_path),
                "sha256": schema.HISTORICAL_BASELINE_SHA256,
                "mode": 0o444,
            },
        },
        "filesystem_admission": {
            "schema": schema.FILESYSTEM_ADMISSION_SCHEMA,
            "checked_path": str(root.resolve()),
            "filesystem": schema.REQUIRED_FILESYSTEM_TYPE,
            "available_bytes": 199_000_000_000,
            "available_inodes": 1_999_000,
            "minimum_available_bytes": schema.MIN_FREE_BYTES,
            "minimum_available_inodes": schema.MIN_FREE_INODES,
        },
        "toolchain": toolchain,
        "build_order": list(schema.VARIANTS),
        "variants": prepared_variants,
    }
    original_prepared_path = prepared_bundle_root / "prepared-artifacts.json"
    fixture_write_json(original_prepared_path, prepared)
    prepared_bundle_root.chmod(0o555)
    prepared_path = output / "prepared-artifacts.json"
    fixture_write_file(prepared_path, original_prepared_path.read_bytes())
    lease = {
        "path": str(Path.home() / ".cache/mess-bench/global-measurement.lock"),
        "device": 1,
        "inode": 2,
        "holder_pid": 3,
        "holder_start_ticks": 4,
        "holder_uid": os.getuid(),
        "hostname": "synthetic-host",
        "boot_id": "synthetic-boot",
        "nonce": nonce,
        "acquired_at": "2026-07-15T00:00:02.500000+00:00",
        "acquired_monotonic_ns": previous_end + 11,
        "proc_locks_proof": "fixture-exclusive",
        "second_exclusive_failed": True,
    }
    fixture_write_json(
        claim_path,
        {
            "schema": schema.PREPARED_CLAIM_SCHEMA,
            "protocol": schema.PROTOCOL,
            "prepared_artifacts_path": str(original_prepared_path),
            "prepared_artifacts_sha256": sha256_file(original_prepared_path),
            "output_dir": str(output),
            "attempt_nonce": nonce,
            "lease_nonce": lease["nonce"],
            "claimed_at": "2026-07-15T00:00:03+00:00",
            "claimed_monotonic_ns": previous_end + 12,
        },
    )

    rows: dict[str, list[dict[str, Any]]] = {}
    sequence = 0
    if not correctness_only:
        for track in schema.TRACK_EXECUTION_ORDER:
            track_rows = []
            for identity in schema.expected_order(config, track):
                sequence += 1
                if track in {"primary", "new_names", "fairness"}:
                    row = fixture_common_row(track, identity, protocol_sha256, nonce, sequence)
                elif track in {"cpu_profiles", "syscall_profiles"}:
                    row = fixture_profile_row(track, identity, protocol_sha256, nonce)
                elif track == "reopen":
                    row = fixture_reopen_row(
                        identity,
                        protocol_sha256,
                        nonce,
                        sequence,
                        root,
                    )
                else:
                    row = fixture_structural_row(identity, protocol_sha256, nonce)
                track_rows.append(row)
            rows[track] = track_rows

    child_records: list[dict[str, Any]] = []
    raw_bindings: list[dict[str, Any]] = []
    correctness_cases: list[dict[str, Any]] = []
    csv_prefixes: dict[str, bytes] = {track: b"" for track in schema.CSV_FILENAMES}
    csv_counts: dict[str, int] = {track: 0 for track in schema.CSV_FILENAMES}

    def add_child(
        kind: str,
        track: str,
        variant: str,
        argv: list[str],
        executable: Path,
        raw_value: Mapping[str, Any],
        *,
        row: Mapping[str, Any] | None = None,
        runner_context: Mapping[str, Any] | None = None,
        profile_result: Mapping[str, Any] | None = None,
        environment: Mapping[str, str] | None = None,
        context: Mapping[str, Any] | None = None,
        transition: Mapping[str, Any] | None = None,
    ) -> int:
        ordinal = len(child_records) + 1
        raw_path = output / "raw" / kind / f"{ordinal:05d}.json"
        stderr_path = output / "raw" / kind / f"{ordinal:05d}.stderr"
        fixture_write_json(raw_path, raw_value)
        fixture_write_file(stderr_path, b"")
        if row is not None:
            identity = schema.expected_order(config, track)[row["row_ordinal"] - 1]
            context_value = {"track": track, **identity}
            store_track = (
                f"{track}-corpus"
                if track == "reopen"
                or (
                    track == "structural_traces"
                    and identity.get("trace_kind") == "reopen"
                )
                else track
            )
            store_path = schema.fresh_store_path(
                root,
                nonce,
                store_track,
                identity["row_ordinal"],
                variant,
            )
            if "batches_per_writer" in row:
                context_value["batches_per_writer"] = row["batches_per_writer"]
            if track == "reopen":
                context_value.update(
                    {
                        "archive_manifest_sha256": row["archive_manifest_sha256"],
                        "copy_manifest_sha256": row["copy_manifest_sha256"],
                        "copy_id": row["copy_id"],
                        "copy_verified_read_only": row["copy_verified_read_only"],
                        "expected_domain_events": row["domain_events"],
                        "expected_visible_events": row["visible_events"],
                        "expected_log_events": row["log_events"],
                        "expected_logical_digest": row["logical_digest"],
                        "expected_registry_head_digest": row[
                            "registry_head_digest"
                        ],
                    }
                )
            elif track == "structural_traces" and identity.get("trace_kind") == "reopen":
                corpus_digest = fixture_full_corpus_content_sha256(variant)
                context_value.update(
                    {
                        "archive_manifest_sha256": corpus_digest,
                        "copy_manifest_sha256": corpus_digest,
                        "copy_id": store_path.name,
                        "copy_verified_read_only": True,
                        "expected_domain_events": 1,
                        "expected_visible_events": 1,
                        "expected_log_events": 1,
                        "expected_logical_digest": corpus_digest,
                        "expected_registry_head_digest": corpus_digest,
                    }
                )
            if track in {"syscall_profiles", "structural_traces"}:
                context_value["variant_trace_path_markers"] = (
                    schema.resolved_trace_path_markers(store_path, variant)
                )
        elif context is not None:
            context_value = dict(context)
        elif kind == "contract":
            context_value = {"transition": "contract", "variant": variant}
        elif kind == "smoke":
            context_value = {"transition": "smoke", "smoke_target": track, "variant": variant}
        else:
            context_value = {
                "transition": "correctness",
                "suite": track,
                "variant": variant,
                "phase": raw_value.get("phase", "pre"),
            }
        context_path = output / "contexts" / f"{ordinal:05d}.json"
        fixture_write_json(context_path, context_value)
        context_sha256 = sha256_file(context_path)
        control_fd = str(100 + ordinal)
        helper_records: list[dict[str, Any]] = []
        ptracer_pid: int | None = None
        profile_inputs: dict[str, Any] = {}
        profile_track = schema.profile_tool_track_for_child(kind, context_value)
        if row is not None and track in {"primary", "new_names", "fairness"}:
            profile_inputs = {"schedstat_resolution_ns": 1}
        elif profile_track == "cpu_profiles":
            profile_inputs = {
                "schedstat_resolution_ns": 1,
                "perf_permission": (
                    "not_available;perf_event_paranoid=4;scope=user-only;exit_status=255"
                ),
                "perf_control_events": [],
                "perf_raw_artifacts": {},
            }
        elif profile_track in {"syscall_profiles", "structural_traces"}:
            trace_path = output / "profile-tools" / f"{ordinal:05d}.strace"
            fixture_write_file(
                trace_path,
                canonical_json_bytes(dict(profile_result or {})),
            )
            profile_inputs = {
                "trace_raw_artifact": {
                    "path": str(trace_path),
                    "sha256": sha256_file(trace_path),
                    "bytes": trace_path.stat().st_size,
                    "mode": 0o444,
                },
                "log_path_markers": context_value["variant_trace_path_markers"][
                    "log"
                ],
                "metadata_path_markers": context_value[
                    "variant_trace_path_markers"
                ]["metadata"],
            }
        if profile_track in {"syscall_profiles", "structural_traces"}:
            ptracer_pid = 70_000 + ordinal
            helper_records = [{"identity": {"pid": ptracer_pid}}]
        if row is not None:
            child_environment = schema.row_child_environment(
                scratch_root=root,
                attempt_nonce=nonce,
                output_dir=output,
                config_path=config_path,
                physical_ordinal=ordinal,
                track=track,
                identity=identity,
                context=context_value,
                context_sha256=context_sha256,
                control_fd=control_fd,
                ptracer_pid=ptracer_pid,
                perf_permission_result=(
                    profile_inputs.get("perf_permission")
                    if track == "cpu_profiles"
                    else None
                ),
            )
        elif transition is not None:
            transition_profile_track = transition.get("profile_track")
            child_environment = schema.transition_child_environment(
                scratch_root=root,
                attempt_nonce=nonce,
                output_dir=output,
                physical_ordinal=ordinal,
                context_sha256=context_sha256,
                plan_environment=transition["plan_environment"],
                controlled=bool(transition["controlled"]),
                store_path=transition.get("store_path"),
                control_fd=(control_fd if transition["controlled"] else None),
                profile_track=(
                    str(transition_profile_track)
                    if isinstance(transition_profile_track, str)
                    else None
                ),
                ptracer_pid=ptracer_pid,
                perf_permission_result=(
                    profile_inputs.get("perf_permission")
                    if transition_profile_track == "cpu_profiles"
                    else None
                ),
            )
        elif kind in {"correctness", "fault"}:
            phase = raw_value["phase"]
            phase_groups = [
                group for group in schema.CORRECTNESS_GROUPS
                if (group[1] in {"oracle", "pre"}) == (phase in {"oracle", "pre"})
            ]
            group = (variant, phase, track, kind)
            child_environment = schema.correctness_child_environment(
                scratch_root=root,
                attempt_nonce=nonce,
                output_dir=output,
                physical_ordinal=ordinal,
                variant=variant,
                phase=phase,
                suite=track,
                phase_ordinal=phase_groups.index(group) + 1,
                context_sha256=context_sha256,
                control_fd=control_fd,
            )
        else:
            child_environment = (
                dict(environment)
                if environment is not None
                else {"ASTERISM_REBASELINE_MODE": kind}
            )
        runner_value = None if runner_context is None else dict(runner_context)
        profile_value = None if profile_result is None else dict(profile_result)
        before_count = csv_counts.get(track, 0)
        before = csv_prefixes.get(track, b"")
        after = before
        after_count = before_count
        csv_append: dict[str, Any] | None = None
        combined_hash: str | None = None
        if row is not None:
            encoded = schema.encode_csv_row(track, row, write_header=before_count == 0).encode("ascii")
            after = before + encoded
            after_count += 1
            csv_prefixes[track] = after
            csv_counts[track] = after_count
            combined_hash = hashlib.sha256(canonical_json_bytes(row)).hexdigest()
            csv_append = {
                "path": str(output / schema.CSV_FILENAMES[track]),
                "bytes_before": len(before),
                "bytes_after": len(after),
                "rows_before": before_count,
                "rows_after": after_count,
                "prefix_sha256_before": hashlib.sha256(before).hexdigest(),
                "prefix_sha256_after": hashlib.sha256(after).hexdigest(),
                "sha256_after": hashlib.sha256(after).hexdigest(),
            }
        started_ns = 10_000_000 + ordinal * 100
        completed_ns = started_ns + 10
        executable_comm = (
            schema.VARIANT_COMMS[variant] if executable in binaries.values()
            else next((tool_comms[name] for name, path in tool_paths.items() if path == executable), "ast-helper")
        )
        profile_tools: dict[str, Any] = {}
        if row is not None and track == "cpu_profiles":
            profile_tools = {
                "perf": {
                    "path": str(tool_paths["perf"]),
                    "sha256": sha256_file(tool_paths["perf"]),
                    "executable_mode": 0o555,
                    "comm": tool_comms["perf"],
                }
            }
        elif row is not None and track in {"syscall_profiles", "structural_traces"}:
            profile_tools = {
                name: {
                    "path": str(tool_paths[name]),
                    "sha256": sha256_file(tool_paths[name]),
                    "executable_mode": 0o555,
                    "comm": tool_comms[name],
                }
                for name in ("strace", "strace_launcher_runtime")
            }
        profile_rich_result = None
        if row is not None:
            source = schema.VARIANT_SOURCE_BINDINGS[variant]
            authority = {
                "schema": schema.PROFILE_AUTHORITY_SCHEMA,
                "protocol": schema.PROTOCOL,
                "protocol_sha256": schema.PROTOCOL_SHA256,
                "attempt_nonce": nonce,
                "child_ordinal": ordinal,
                "row_ordinal": row["row_ordinal"],
                "context_sha256": context_sha256,
                "prepared_artifacts_path": str(prepared_path),
                "prepared_artifacts_sha256": sha256_file(prepared_path),
                "source_approval_path": str(approval_path),
                "source_approval_sha256": approval_sha256,
                "profile_adapter_path": str(tooling_paths["profile_adapters.py"]),
                "profile_adapter_sha256": sha256_file(
                    tooling_paths["profile_adapters.py"]
                ),
                "profile_tools": profile_tools,
                "perf_permission_result": (
                    profile_inputs["perf_permission"]
                    if track == "cpu_profiles"
                    else "not_applicable"
                ),
                "variant": variant,
                "source_commit": source["commit"],
                "source_tree": source["tree"],
                "track": track,
                "executable_path": str(executable),
                "executable_sha256": sha256_file(executable),
                "executable_mode": stat.S_IMODE(executable.stat().st_mode),
                "executable_comm": executable_comm,
                "child_pid": 50_000 + ordinal,
                "child_start_ticks": 60_000 + ordinal,
                "control_fd": int(control_fd),
            }
            profile_rich_result = {
                "schema": schema.PROFILE_ADAPTER_SCHEMA,
                "protocol": schema.PROTOCOL,
                "authority": authority,
                "variant": variant,
                "track": track,
                "context": context_value,
                "process": {"profile_result": profile_value},
                "roles": [],
                "phase_snapshots": [],
                "unattributed_births": [],
            }
        record = {
            "schema": schema.CHILD_SCHEMA,
            "protocol": schema.PROTOCOL,
            "ordinal": ordinal,
            "kind": track if row is not None else kind,
            "context": context_value,
            "context_sha256": context_sha256,
            "argv": argv,
            "environment": child_environment,
            "executable_path": str(executable),
            "executable_sha256": sha256_file(executable),
            "executable_mode": stat.S_IMODE(executable.stat().st_mode),
            "executable_comm": executable_comm,
            "identity": {
                "pid": 50_000 + ordinal,
                "comm": executable_comm,
                "state": "R",
                "ppid": 3,
                "pgrp": 50_000 + ordinal,
                "session": 50_000 + ordinal,
                "starttime_ticks": 60_000 + ordinal,
            },
            "started_at": "2026-07-15T01:00:00+00:00",
            "started_monotonic_ns": started_ns,
            "completed_at": "2026-07-15T01:00:01+00:00",
            "completed_monotonic_ns": completed_ns,
            "exit_status": 0,
            "waited_pid": 50_000 + ordinal,
            "timed_out": False,
            "terminated_by_runner": False,
            "interrupted": None,
            "process_group_absent": True,
            "orphan_process_group_detected": False,
            "reaping": {"pid": 50_000 + ordinal, "start_ticks": 60_000 + ordinal, "status": "absent"},
            "control_events": [],
            "control_events_sha256": hashlib.sha256(canonical_json_bytes([])).hexdigest(),
            "profile_events": [],
            "profile_events_sha256": hashlib.sha256(canonical_json_bytes([])).hexdigest(),
            "parked_state_proofs": [],
            "profile_rich_result": profile_rich_result,
            "runner_context": runner_value,
            "runner_context_sha256": (
                hashlib.sha256(canonical_json_bytes(runner_value)).hexdigest() if row is not None else None
            ),
            "profile_result": profile_value,
            "profile_result_sha256": (
                hashlib.sha256(canonical_json_bytes(profile_value)).hexdigest() if row is not None else None
            ),
            "profile_contract_sha256": config["profile_contract_sha256"],
            "profile_tool_inputs": profile_inputs,
            "profile_tool_inputs_sha256": hashlib.sha256(
                canonical_json_bytes(profile_inputs)
            ).hexdigest(),
            "profile_tool_helper_records": helper_records,
            "raw_path": str(raw_path),
            "raw_sha256": sha256_file(raw_path),
            "raw_bytes": raw_path.stat().st_size,
            "raw_mode_after": 0o444,
            "stderr_path": str(stderr_path),
            "stderr_sha256": sha256_file(stderr_path),
            "stderr_bytes": 0,
            "stderr_mode_after": 0o444,
            "expected_records": 1,
            "combined_row_sha256": combined_hash,
            "csv_append": csv_append,
            "guard_pre_ordinal": 2 * ordinal - 1,
            "guard_post_ordinal": 2 * ordinal,
            "validation_error": None,
        }
        child_records.append(record)
        raw_bindings.append({
            "schema": schema.RAW_BINDING_SCHEMA,
            "protocol": schema.PROTOCOL,
            "child_ordinal": ordinal,
            "kind": record["kind"],
            "context_sha256": record["context_sha256"],
            "raw_path": str(raw_path),
            "raw_sha256": record["raw_sha256"],
            "raw_bytes": record["raw_bytes"],
            "expected_records": 1,
            "stderr_path": str(stderr_path),
            "stderr_sha256": record["stderr_sha256"],
        })
        return ordinal

    for transition in transition_fixtures:
        raw_value: Mapping[str, Any] = {
            "schema": schema.SMOKE_SCHEMA,
            "status": "PASS",
        }
        if transition["kind"] == "smoke_reopen_seed":
            raw_value = {
                "schema": "bn-2l3n-overlay-smoke-reopen-seed-v3",
                "protocol": schema.PROTOCOL,
                "protocol_sha256": schema.PROTOCOL_SHA256,
                "variant": "A",
                "domain_events": SPECIALIZED_SEED_AUTHORITY["domain_events"],
                "visible_events": SPECIALIZED_SEED_AUTHORITY["visible_events"],
                "log_events": SPECIALIZED_SEED_AUTHORITY["log_events"],
                "logical_digest": specialized_logical_digest,
                "registry_head_digest": specialized_registry_digest,
            }
        add_child(
            transition["kind"],
            transition["id"],
            transition["variant"],
            transition["argv"],
            transition["executable"],
            raw_value,
            context=transition["context"],
            transition=transition,
        )

    correctness_bindings: dict[tuple[str, str, str, str], tuple[int, Path]] = {}
    correctness_groups = schema.CORRECTNESS_GROUPS

    def add_correctness_group(group: tuple[str, str, str, str]) -> None:
        variant, phase, suite, kind = group
        descriptors = [
            item for item in correctness_descriptors
            if (item["variant"], item["phase"], item["suite"], item["kind"]) == group
        ]
        raw = {
            "schema": schema.CORRECTNESS_CHILD_SCHEMA,
            "protocol": schema.PROTOCOL,
            "attempt_nonce": nonce,
            "variant": variant,
            "phase": phase,
            "suite": suite,
            "harness_sound": True,
            "boundedness": schema.CORRECTNESS_EXPECTED_BOUNDEDNESS if suite == "current-fault" else None,
            "cases": [
                {
                    "id": item["id"],
                    "classification": item["classification"],
                    "status": correctness_case_status(item),
                }
                for item in descriptors
            ],
        }
        executable = binaries[variant] if variant in {"C", "D"} else tool_paths[kind]
        ordinal = add_child(
            kind,
            suite,
            variant,
            schema.correctness_argv(str(executable), variant, phase, suite, nonce),
            executable,
            raw,
            environment=schema.correctness_environment(
                variant, phase, suite, nonce
            ),
        )
        path = output / "raw" / kind / f"{ordinal:05d}.json"
        correctness_bindings[group] = (ordinal, path)

    for group in correctness_groups[:4]:
        add_correctness_group(group)

    if not correctness_only:
        for track in schema.TRACK_EXECUTION_ORDER:
            for row in rows[track]:
                raw_point = {field: row[field] for field in schema.RAW_POINT_FIELDS_BY_TRACK[track]}
                raw_point["schema"] = schema.RAW_POINT_SCHEMA
                runner_context = {field: row[field] for field in schema.RUNNER_CONTEXT_FIELDS_BY_TRACK[track]}
                profile_result = {field: row[field] for field in schema.PROFILE_FIELDS_BY_TRACK[track]}
                combined = schema.validate_child_records(
                    track,
                    [raw_point],
                    "fixture composition",
                    runner_context=runner_context,
                    profile_result=profile_result,
                )
                if combined != row:
                    raise AssertionError("fixture split/recomposition mismatch")
                binary = binaries[row["variant"]]
                argv = expand_argv_template(
                    config["argv_templates"][track],
                    binary=binary,
                    track=track,
                    row_ordinal=row["row_ordinal"],
                    variant=row["variant"],
                    config_path=config_path,
                    output_dir=output,
                    profile_adapter=tooling_paths["profile_adapters.py"],
                )
                add_child(
                    track, track, row["variant"], argv, binary, raw_point,
                    row=row, runner_context=runner_context, profile_result=profile_result,
                )

    for group in correctness_groups[4:]:
        add_correctness_group(group)

    for descriptor in correctness_descriptors:
        group = (descriptor["variant"], descriptor["phase"], descriptor["suite"], descriptor["kind"])
        ordinal, path = correctness_bindings[group]
        correctness_cases.append({
            **descriptor,
            "status": correctness_case_status(descriptor),
            "child_ordinal": ordinal,
            "output_path": str(path),
            "output_sha256": sha256_file(path),
        })

    if not correctness_only:
        for track, data in csv_prefixes.items():
            fixture_write_file(output / schema.CSV_FILENAMES[track], data)

    correctness = {
        "schema": schema.CORRECTNESS_SCHEMA,
        "protocol": schema.PROTOCOL,
        "attempt_nonce": nonce,
        "harness_sound": True,
        "boundedness": schema.CORRECTNESS_EXPECTED_BOUNDEDNESS,
        "cases": correctness_cases,
    }
    fixture_write_json(output / "correctness.json", correctness)
    correctness_only_path = output / "correctness-only.json"
    if correctness_only:
        historical_failed_cases = (
            [{"variant": "C", "phase": "oracle", "id": "public-common-oracle"}]
            if historical_failure
            else []
        )
        trigger = (
            "mixed"
            if pre_failures and historical_failed_cases
            else "current"
            if pre_failures
            else "historical"
            if historical_failed_cases
            else "current"
        )
        fixture_write_json(
            correctness_only_path,
            {
                "schema": schema.CORRECTNESS_ONLY_SCHEMA,
                "protocol": schema.PROTOCOL,
                "attempt_nonce": nonce,
                "trigger": trigger,
                "current_pre_failed_case_ids": sorted(set(pre_failures)),
                "current_post_failed_case_ids": sorted(set(post_failures)),
                "historical_failed_cases": historical_failed_cases,
                "timing_child_records": 0,
                "created_at": "2026-07-15T01:30:00+00:00",
                "created_monotonic_ns": 90_000_000,
            },
        )

    raw_manifest_bytes = b"".join(canonical_json_bytes(record) for record in raw_bindings)
    fixture_write_file(output / "raw-manifest.json", raw_manifest_bytes)
    raw_manifest_sha = sha256_file(output / "raw-manifest.json")
    child_bytes = b"".join(canonical_json_bytes(record) for record in child_records)
    fixture_write_file(output / "child-manifest.jsonl", child_bytes)

    guards: list[dict[str, Any]] = []
    runner_identity = {
        "pid": 3,
        "comm": tool_comms["runner_runtime"],
        "state": "R",
        "ppid": 1,
        "pgrp": 3,
        "session": 3,
        "starttime_ticks": 4,
    }
    tracked_comm = prepared["comm_allowlist"]
    for ordinal in range(1, 2 * len(child_records) + 2):
        if ordinal <= 2 * len(child_records):
            child_index = (ordinal + 1) // 2
            is_pre = ordinal % 2 == 1
            label = f"{child_index:05d}-{child_records[child_index - 1]['kind']}-{'pre' if is_pre else 'post'}"
            child = child_records[child_index - 1]
            started_ns = child["started_monotonic_ns"] - 2 if is_pre else child["completed_monotonic_ns"] + 1
        else:
            label = "pre-evaluator"
            started_ns = child_records[-1]["completed_monotonic_ns"] + 100
        completed_ns = started_ns + 1
        snapshot = {
            "schema": schema.GUARD_SCHEMA,
            "protocol": schema.PROTOCOL,
            "ordinal": ordinal,
            "label": label,
            "tracked_comm": tracked_comm,
            "runner": runner_identity,
            "active_child": None,
            "active_helpers": [],
            "records": [{
                **runner_identity,
                "uid": os.getuid(),
                "exe": str(tool_paths["runner_runtime"]),
                "exe_sha256": sha256_file(tool_paths["runner_runtime"]),
                "cmdline": f"{tool_paths['runner_runtime']} {tooling_paths['run_rebaseline.py']}",
                "read_errors": [],
                "classification": "runner",
                "observed_at": "2026-07-15T01:00:00+00:00",
            }],
            "final_resource": {
                "load1": 0.0,
                "free_bytes": 200_000_000_000,
                "free_inodes": 2_000_000,
                "enforced": label.endswith("-pre") or label == "pre-evaluator",
            },
            "preidentity_vanished": [],
            "verdict": "pass",
            "started_at": "2026-07-15T01:00:00+00:00",
            "started_monotonic_ns": started_ns,
            "completed_at": "2026-07-15T01:00:00+00:00",
            "completed_monotonic_ns": completed_ns,
        }
        snapshot_path = output / "guards" / f"{ordinal:05d}-{label}.json"
        fixture_write_json(snapshot_path, snapshot)
        guards.append(
            {
                "schema": schema.GUARD_BINDING_SCHEMA,
                "protocol": schema.PROTOCOL,
                "kind": "process_guard",
                "ordinal": ordinal,
                "label": label,
                "path": str(snapshot_path),
                "sha256": sha256_file(snapshot_path),
                "verdict": "pass",
                "started_monotonic_ns": started_ns,
                "completed_monotonic_ns": completed_ns,
            }
        )
    fixture_write_file(
        output / "guard-manifest.jsonl",
        b"".join(canonical_json_bytes(record) for record in guards),
    )

    csv_artifacts = {}
    if not correctness_only:
        for track, filename in schema.CSV_FILENAMES.items():
            path = output / filename
            csv_artifacts[track] = {
                "path": str(path),
                "sha256": sha256_file(path),
                "bytes": path.stat().st_size,
                "rows": schema.EXPECTED_CARDINALITY[track],
                "columns": len(schema.CSV_FIELDS_BY_TRACK[track]),
            }
    resource_manifest_path = output / "resource-manifest.jsonl"
    correctness_manifest_path = output / "correctness-manifest.jsonl"
    fixture_write_file(
        resource_manifest_path,
        canonical_json_bytes({"schema": "bn-2l3n-resource-record-v3", "status": "PASS"}),
    )
    fixture_write_file(
        correctness_manifest_path,
        b"".join(
            canonical_json_bytes(
                {"ordinal": child["ordinal"], "kind": child["kind"], "status": "PASS"}
            )
            for child in child_records
            if child["kind"] in {"correctness", "fault"}
        ),
    )
    frozen_files = {
        str(path): sha256_file(path)
        for path in (
            config_path,
            approval_path,
            prepared_path,
            profile_contract_path,
            output / schema.PREPARED_INPUT_FILENAMES["protocol"],
            output / schema.PREPARED_INPUT_FILENAMES["historical_baseline"],
        )
    }
    if correctness_only:
        frozen_files[str(correctness_only_path)] = sha256_file(correctness_only_path)
    provenance = {
        "schema": schema.PROVENANCE_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": protocol_sha256,
        "evidence_mode": "correctness-only" if correctness_only else "admission",
        "attempt_nonce": nonce,
        "output_dir": str(output),
        "output_dir_absent_before": True,
        "source_approval_path": str(approval_path),
        "source_approval_sha256": approval_sha256,
        "config_path": str(config_path),
        "config_sha256": config_sha256,
        "prepared_artifacts_path": str(prepared_path),
        "prepared_artifacts_sha256": sha256_file(prepared_path),
        "runner_path": str(tooling_paths["run_rebaseline.py"]),
        "runner_sha256": sha256_file(tooling_paths["run_rebaseline.py"]),
        "evaluator_path": str(tooling_paths["evaluate.py"]),
        "evaluator_sha256": sha256_file(tooling_paths["evaluate.py"]),
        "terminal_verifier_path": str(tooling_paths["verify_terminal.py"]),
        "terminal_verifier_sha256": sha256_file(tooling_paths["verify_terminal.py"]),
        "schema_path": str(tooling_paths["evidence_schema.py"]),
        "schema_sha256": sha256_file(tooling_paths["evidence_schema.py"]),
        "profile_adapter_path": str(tooling_paths["profile_adapters.py"]),
        "profile_adapter_sha256": sha256_file(tooling_paths["profile_adapters.py"]),
        "profile_contract_path": str(profile_contract_path),
        "profile_contract_sha256": sha256_file(profile_contract_path),
        "correctness_path": str(output / "correctness.json"),
        "correctness_sha256": sha256_file(output / "correctness.json"),
        "correctness_executable_path": str(tool_paths["correctness"]),
        "correctness_executable_sha256": sha256_file(tool_paths["correctness"]),
        "csv_artifacts": csv_artifacts,
        "raw_manifest_path": str(output / "raw-manifest.json"),
        "raw_manifest_sha256": raw_manifest_sha,
        "guard_manifest_path": str(output / "guard-manifest.jsonl"),
        "guard_manifest_sha256": sha256_file(output / "guard-manifest.jsonl"),
        "child_manifest_path": str(output / "child-manifest.jsonl"),
        "child_manifest_sha256": sha256_file(output / "child-manifest.jsonl"),
        "lease": lease,
        "host": {
            "runner": runner_identity,
            "hostname": "synthetic-host",
            "boot_id": "synthetic-boot",
            "kernel": "synthetic-kernel",
            "cpu_model": "synthetic-cpu",
            "cpu_topology": {
                "logical_cpus": 4, "physical_packages": 1,
                "cores": 2, "threads_per_core": 2,
            },
            "governors": {"/sys/devices/system/cpu/cpufreq/policy0": "performance"},
            "turbo": {
                "intel_pstate_no_turbo": schema.NOT_AVAILABLE,
                "cpufreq_boost": "1",
            },
            "affinity": [0, 1, 2, 3],
            "page_size": 4096,
            "cpu_count": 4,
            "memory_bytes": 1 << 40,
            "filesystem": {
                "mount_id": "1", "parent_mount_id": "0", "device": "8:1",
                "root": "/", "target": "/fixture", "mount_options": "rw",
                "filesystem_type": schema.REQUIRED_FILESYSTEM_TYPE, "source": "/dev/fixture",
                "super_options": "rw",
            },
            "scheduler": {
                "logical_device": "8:1", "base_device": "fixture-device",
                "scheduler_path": "/sys/class/block/fixture-device/queue/scheduler",
                "scheduler_value": "none",
            },
            "uid": os.getuid(),
            "scratch_root": str(root.resolve()),
            "scratch_free_bytes_initial": 200_000_000_000,
            "scratch_free_inodes_initial": 2_000_000,
            "scratch_free_bytes_final": 199_000_000_000,
            "scratch_free_inodes_final": 1_999_000,
            "tracked_comm": tracked_comm,
            "frozen_files": frozen_files,
            "guard_records": len(guards),
            "child_records": len(child_records),
            "resource_manifest_path": str(resource_manifest_path),
            "resource_manifest_sha256": sha256_file(resource_manifest_path),
            "correctness_manifest_path": str(correctness_manifest_path),
            "correctness_manifest_sha256": sha256_file(correctness_manifest_path),
        },
        "started_at": "2026-07-15T00:59:00+00:00",
        "started_monotonic_ns": 9_000_000,
        "completed_at": "2026-07-15T02:00:00+00:00",
        "completed_monotonic_ns": 100_000_000,
        "partial": False,
        "failure_absent": True,
    }
    fixture_write_json(output / "provenance.json", provenance)
    return output


def self_test() -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def check(name: str, action: Callable[[], bool]) -> None:
        try:
            passed = bool(action())
            detail = ""
        except Exception as error:
            passed = False
            detail = repr(error)
        checks.append({"name": name, "pass": passed, "detail": detail})

    for value, label in ((float("nan"), "nan"), (float("inf"), "infinity")):
        def rejects_nonfinite(value: float = value) -> bool:
            try:
                canonical_json_bytes({"value": value})
            except ValueError:
                return True
            return False

        check(f"canonical-json-rejects-{label}", rejects_nonfinite)

    check(
        "profile-smoke-track-inference-is-exact",
        lambda: all(
            (
                schema.profile_tool_track_for_child(
                    "smoke", {"smoke_target": "primary"}
                )
                is None,
                schema.profile_tool_track_for_child(
                    "smoke", {"smoke_target": "reopen"}
                )
                is None,
                schema.profile_tool_track_for_child(
                    "smoke", {"smoke_target": "cpu_profiles"}
                )
                == "cpu_profiles",
                schema.profile_tool_track_for_child(
                    "smoke", {"profile_smoke_track": "reopen"}
                )
                == "reopen",
            )
        ),
    )

    with tempfile.TemporaryDirectory(prefix="bn-2l3n-snapshot-selftest-") as snapshot_temp:
        snapshot_root = Path(snapshot_temp)
        regular = snapshot_root / "regular.json"
        fixture_write_json(regular, {"value": 1})

        def rejects_final_symlink() -> bool:
            link = snapshot_root / "final-link.json"
            link.symlink_to(regular.name)
            try:
                schema.snapshot_regular_file(link, expected_mode=0o444)
            except OSError:
                return True
            return False

        check("snapshot-rejects-final-symlink", rejects_final_symlink)

        def rejects_ancestor_symlink() -> bool:
            real = snapshot_root / "real"
            real.mkdir()
            target = real / "value.json"
            fixture_write_json(target, {"value": 2})
            link = snapshot_root / "ancestor-link"
            link.symlink_to(real.name)
            try:
                schema.snapshot_regular_file(link / target.name, expected_mode=0o444)
            except OSError:
                return True
            return False

        check("snapshot-rejects-ancestor-symlink", rejects_ancestor_symlink)
        def rejects_writable_mode() -> bool:
            regular.chmod(0o644)
            try:
                schema.snapshot_regular_file(regular, expected_mode=0o444)
            except OSError:
                return True
            return False

        check("snapshot-rejects-writable-mode", rejects_writable_mode)
        regular.chmod(0o444)

        def rejects_replacement(*, ancestor: bool) -> bool:
            parent = snapshot_root / ("race-ancestor" if ancestor else "race-file")
            parent.mkdir()
            path = parent / "bound.json"
            fixture_write_json(path, {"value": "original"})
            saved_read = schema.os.read
            replaced = False

            def replacing_read(fd: int, size: int) -> bytes:
                nonlocal replaced
                data = saved_read(fd, size)
                if data and not replaced:
                    replaced = True
                    if ancestor:
                        old = snapshot_root / "race-ancestor-old"
                        parent.rename(old)
                        parent.mkdir()
                        fixture_write_json(parent / path.name, {"value": "replacement"})
                    else:
                        replacement = parent / "replacement.json"
                        fixture_write_json(replacement, {"value": "replacement"})
                        replacement.replace(path)
                return data

            schema.os.read = replacing_read
            try:
                schema.snapshot_regular_file(path, expected_mode=0o444)
            except OSError:
                return True
            finally:
                schema.os.read = saved_read
            return False

        check("snapshot-rejects-file-replacement-race", lambda: rejects_replacement(ancestor=False))
        check("snapshot-rejects-ancestor-replacement-race", lambda: rejects_replacement(ancestor=True))

        def rejects_corpus_preopen_replacement(kind: str) -> bool:
            race_parent = snapshot_root / f"corpus-race-{kind}"
            race_parent.mkdir()
            corpus_root = race_parent / "corpus"
            corpus_root.mkdir()
            nested = corpus_root / "nested"
            nested.mkdir()
            fixture_write_file(nested / "corpus.bin", b"original\n")
            saved_open = os.open
            replaced = False
            old_path: Path | None = None
            replacement_path: Path | None = None
            trigger_name = {
                "file": "corpus.bin",
                "ancestor": "nested",
                "root": "corpus",
            }[kind]
            if kind == "file":
                replacement_path = race_parent / "replacement.bin"
                fixture_write_file(replacement_path, b"replacement\n")
            else:
                replacement_path = race_parent / f"replacement-{kind}"
                replacement_path.mkdir()
                if kind == "ancestor":
                    fixture_write_file(
                        replacement_path / "corpus.bin", b"replacement\n"
                    )
                else:
                    replacement_nested = replacement_path / "nested"
                    replacement_nested.mkdir()
                    fixture_write_file(
                        replacement_nested / "corpus.bin", b"replacement\n"
                    )

            def replacing_open(
                path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
                flags: int,
                mode: int = 0o777,
                *,
                dir_fd: int | None = None,
            ) -> int:
                nonlocal replaced, old_path
                if (
                    not replaced
                    and dir_fd is not None
                    and os.fspath(path) == trigger_name
                ):
                    replaced = True
                    if kind == "file":
                        target = nested / "corpus.bin"
                        old_path = race_parent / "old.bin"
                    elif kind == "ancestor":
                        target = nested
                        old_path = race_parent / "old-ancestor"
                    else:
                        target = corpus_root
                        old_path = race_parent / "old-root"
                    target.rename(old_path)
                    assert replacement_path is not None
                    replacement_path.rename(target)
                return saved_open(path, flags, mode, dir_fd=dir_fd)

            os.open = replacing_open
            try:
                try:
                    _snapshot_corpus_tree(
                        corpus_root,
                        allow_missing=False,
                        root_mode=None,
                        directory_mode=None,
                        file_mode=None,
                        include_modes=False,
                    )
                except OSError as error:
                    return "replaced between lstat and open" in str(error)
                return False
            finally:
                os.open = saved_open
                shutil.rmtree(race_parent)

        for race_kind in ("file", "ancestor", "root"):
            check(
                f"corpus-snapshot-rejects-{race_kind}-preopen-replacement",
                lambda race_kind=race_kind: rejects_corpus_preopen_replacement(
                    race_kind
                ),
            )

        def rejects_corpus_ancestor_failure_without_fd_leak(
            failure: str,
        ) -> bool:
            corpus_root = snapshot_root / f"corpus-{failure}-fd-ownership"
            corpus_root.mkdir()
            fixture_write_file(corpus_root / "corpus.bin", b"original\n")
            saved_open = os.open
            saved_fstat = os.fstat
            watched_fd: int | None = None

            def tracking_open(
                path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
                flags: int,
                mode: int = 0o777,
                *,
                dir_fd: int | None = None,
            ) -> int:
                nonlocal watched_fd
                descriptor = saved_open(path, flags, mode, dir_fd=dir_fd)
                if (
                    dir_fd is not None
                    and os.fspath(path) == corpus_root.name
                ):
                    watched_fd = descriptor
                return descriptor

            def injected_fstat(descriptor: int) -> os.stat_result:
                info = saved_fstat(descriptor)
                if descriptor != watched_fd:
                    return info
                if failure == "fstat":
                    raise OSError(errno.EIO, "injected ancestor fstat failure")
                values = list(info)
                values[stat.ST_INO] += 1
                return os.stat_result(values)

            descriptors_before = set(os.listdir("/proc/self/fd"))
            rejected = False
            os.open = tracking_open
            os.fstat = injected_fstat
            try:
                try:
                    _snapshot_corpus_tree(
                        corpus_root,
                        allow_missing=False,
                        root_mode=None,
                        directory_mode=None,
                        file_mode=None,
                        include_modes=False,
                    )
                except OSError:
                    rejected = True
            finally:
                os.open = saved_open
                os.fstat = saved_fstat
            descriptors_after = set(os.listdir("/proc/self/fd"))
            return (
                rejected
                and watched_fd is not None
                and descriptors_after == descriptors_before
            )

        for failure in ("fstat", "identity"):
            check(
                f"corpus-snapshot-{failure}-failure-closes-ancestor-fd",
                lambda failure=failure: (
                    rejects_corpus_ancestor_failure_without_fd_leak(failure)
                ),
            )

        def inventory_has_exact_mode_and_rejects_hidden() -> bool:
            inventory_root = snapshot_root / "inventory"
            inventory_root.mkdir()
            fixture_write_json(inventory_root / "visible.json", {"value": 3})
            inventory, _ = schema.artifact_inventory(inventory_root)
            if inventory != [
                {
                    "path": "visible.json",
                    "bytes": (inventory_root / "visible.json").stat().st_size,
                    "sha256": sha256_file(inventory_root / "visible.json"),
                    "mode": 0o444,
                }
            ]:
                return False
            fixture_write_json(inventory_root / ".hidden.json", {"value": 4})
            try:
                schema.artifact_inventory(inventory_root)
            except OSError:
                return True
            return False

        check(
            "inventory-mode-and-hidden-entry-contract",
            inventory_has_exact_mode_and_rejects_hidden,
        )

    with tempfile.TemporaryDirectory(prefix="bn-2l3n-evaluator-selftest-") as temp:
        output = build_synthetic_fixture(Path(temp))
        authority_payload = synthetic_corpus_execution_authority_payload(
            output, correctness_only=False
        )
        evaluate_directory_implementation = globals()["evaluate_directory"]

        def evaluate_directory(
            output_dir: Path,
            *,
            synthetic: bool,
            publish: bool,
            correctness_only: bool = False,
            corpus_authority_fd: int | None = None,
        ) -> tuple[dict[str, Any], int]:
            """Self-test adapter that always passes one sealed fixture memfd."""

            descriptor = corpus_authority_fd
            if synthetic and descriptor is None:
                payload = (
                    authority_payload
                    if Path(output_dir) == output
                    else synthetic_corpus_execution_authority_payload(
                        Path(output_dir), correctness_only=correctness_only
                    )
                )
                descriptor = create_corpus_execution_authority_fd(payload)
            return evaluate_directory_implementation(
                output_dir,
                synthetic=synthetic,
                publish=publish,
                correctness_only=correctness_only,
                corpus_authority_fd=descriptor,
            )

        positive, positive_rc = evaluate_synthetic_fixture(
            output, authority_payload, publish=False
        )
        checks.append(
            {
                "name": "full-positive-evidence-chain",
                "pass": positive_rc == EXIT_ADMIT and positive["outcome"] == "ADMIT" and positive["evidence_valid"],
                "detail": "" if positive_rc == EXIT_ADMIT else json.dumps(positive["errors"][:20]),
            }
        )

        def descriptor_is_closed(descriptor: int) -> bool:
            try:
                os.fstat(descriptor)
            except OSError as error:
                return error.errno == errno.EBADF
            return False

        def production_environment_authority_is_consumed() -> bool:
            descriptor = create_corpus_execution_authority_fd(
                authority_payload
            )
            previous = os.environ.get(
                CORPUS_EXECUTION_AUTHORITY_FD_ENVIRONMENT
            )
            try:
                os.environ[CORPUS_EXECUTION_AUTHORITY_FD_ENVIRONMENT] = str(
                    descriptor
                )
                validation = Problems()
                captured = capture_corpus_execution_authority(
                    validation,
                    synthetic=False,
                    provided_fd=None,
                )
                return (
                    not validation.errors
                    and captured == json.loads(authority_payload)
                    and descriptor_is_closed(descriptor)
                )
            finally:
                if previous is None:
                    os.environ.pop(
                        CORPUS_EXECUTION_AUTHORITY_FD_ENVIRONMENT, None
                    )
                else:
                    os.environ[
                        CORPUS_EXECUTION_AUTHORITY_FD_ENVIRONMENT
                    ] = previous
                if not descriptor_is_closed(descriptor):
                    os.close(descriptor)

        check(
            "corpus-authority-production-environment-fd-consumed",
            production_environment_authority_is_consumed,
        )

        def production_environment_fd_rejected(
            value: str,
            expected_error: str,
        ) -> bool:
            previous = os.environ.get(
                CORPUS_EXECUTION_AUTHORITY_FD_ENVIRONMENT
            )
            descriptors_before = set(os.listdir("/proc/self/fd"))
            try:
                os.environ[CORPUS_EXECUTION_AUTHORITY_FD_ENVIRONMENT] = value
                result, code = evaluate_directory_implementation(
                    output,
                    synthetic=False,
                    publish=False,
                )
            finally:
                if previous is None:
                    os.environ.pop(
                        CORPUS_EXECUTION_AUTHORITY_FD_ENVIRONMENT, None
                    )
                else:
                    os.environ[
                        CORPUS_EXECUTION_AUTHORITY_FD_ENVIRONMENT
                    ] = previous
            descriptors_after = set(os.listdir("/proc/self/fd"))
            return (
                code == EXIT_INCONCLUSIVE
                and result["evidence_valid"] is False
                and descriptors_after == descriptors_before
                and any(
                    expected_error in error for error in result["errors"]
                )
            )

        invalid_production_fd_forms = (
            ("ten-digits-out-of-range", "9" * 10),
            ("absurd-positive-digits", "9" * 5_000),
            ("negative", "-3"),
            ("explicit-plus", "+3"),
            ("leading-whitespace", " 3"),
            ("trailing-whitespace", "3 "),
            ("nondecimal", "3x"),
            ("leading-zero", "03"),
        )
        for label, value in invalid_production_fd_forms:
            check(
                f"mutation-production-corpus-authority-fd-{label}",
                lambda value=value: production_environment_fd_rejected(
                    value,
                    "production corpus execution authority FD is invalid",
                ),
            )
        check(
            "mutation-production-corpus-authority-fd-in-range-absent",
            lambda: production_environment_fd_rejected(
                MAX_FILE_DESCRIPTOR_DECIMAL,
                "cannot capture corpus execution authority",
            ),
        )

        def authority_payload_rejected(
            payload: bytes,
            expected_error: str,
            *,
            seal: bool = True,
        ) -> bool:
            descriptor = create_corpus_execution_authority_fd(
                payload, seal=seal
            )
            result, code = evaluate_directory_implementation(
                output,
                synthetic=True,
                publish=False,
                corpus_authority_fd=descriptor,
            )
            return (
                code == EXIT_INCONCLUSIVE
                and result["evidence_valid"] is False
                and descriptor_is_closed(descriptor)
                and any(
                    expected_error in error for error in result["errors"]
                )
            )

        def mutated_authority_payload(
            mutator: Callable[[dict[str, Any]], None],
        ) -> bytes:
            value = copy.deepcopy(json.loads(authority_payload))
            mutator(value)
            return canonical_json_bytes(value)

        def missing_authority_rejected() -> bool:
            result, code = evaluate_directory_implementation(
                output,
                synthetic=True,
                publish=False,
                corpus_authority_fd=None,
            )
            return (
                code == EXIT_INCONCLUSIVE
                and result["evidence_valid"] is False
                and any(
                    "synthetic corpus execution authority FD is invalid"
                    in error
                    for error in result["errors"]
                )
            )

        check("mutation-corpus-authority-fd-missing", missing_authority_rejected)
        check(
            "mutation-corpus-authority-payload-malformed",
            lambda: authority_payload_rejected(
                b"not-json\n", "corpus execution authority: invalid JSON"
            ),
        )
        check(
            "mutation-corpus-authority-fd-unsealed",
            lambda: authority_payload_rejected(
                authority_payload,
                "FD seals differ from exact 0x0f",
                seal=False,
            ),
        )

        def non_regular_authority_fd_rejected() -> bool:
            read_descriptor, write_descriptor = os.pipe()
            os.close(write_descriptor)
            result, code = evaluate_directory_implementation(
                output,
                synthetic=True,
                publish=False,
                corpus_authority_fd=read_descriptor,
            )
            return (
                code == EXIT_INCONCLUSIVE
                and result["evidence_valid"] is False
                and descriptor_is_closed(read_descriptor)
                and any(
                    "FD is not regular" in error
                    for error in result["errors"]
                )
            )

        check(
            "mutation-corpus-authority-fd-non-regular",
            non_regular_authority_fd_rejected,
        )
        check(
            "mutation-corpus-authority-extra-record",
            lambda: authority_payload_rejected(
                mutated_authority_payload(
                    lambda value: value["records"].append(
                        copy.deepcopy(value["records"][-1])
                    )
                ),
                "record count differs",
            ),
        )
        check(
            "mutation-corpus-authority-partial-records",
            lambda: authority_payload_rejected(
                mutated_authority_payload(
                    lambda value: value["records"].pop()
                ),
                "record count differs",
            ),
        )

        def reorder_authority_records(value: dict[str, Any]) -> None:
            value["records"][0], value["records"][1] = (
                value["records"][1],
                value["records"][0],
            )

        check(
            "mutation-corpus-authority-record-order",
            lambda: authority_payload_rejected(
                mutated_authority_payload(reorder_authority_records),
                "physical plan differs",
            ),
        )
        check(
            "mutation-corpus-authority-root-rebound",
            lambda: authority_payload_rejected(
                mutated_authority_payload(
                    lambda value: value["records"][0].__setitem__(
                        "root", value["records"][0]["root"] + "-rebound"
                    )
                ),
                "physical plan differs",
            ),
        )

        def rebind_authority_mode(value: dict[str, Any]) -> None:
            record = value["records"][1]
            record["file_mode"] = 0o600
            for entry in record["entries"]:
                if entry.get("kind") == "file":
                    entry["mode"] = 0o600
            record["tree_sha256"] = hashlib.sha256(
                canonical_json_bytes(record["entries"])
            ).hexdigest()

        check(
            "mutation-corpus-authority-mode-rebound",
            lambda: authority_payload_rejected(
                mutated_authority_payload(rebind_authority_mode),
                "physical plan differs",
            ),
        )
        check(
            "mutation-corpus-authority-record-bool-for-int",
            lambda: authority_payload_rejected(
                mutated_authority_payload(
                    lambda value: value["records"][0].__setitem__(
                        "ordinal", True
                    )
                ),
                "physical plan differs",
            ),
        )

        def rebind_authority_entry_mode(value: dict[str, Any]) -> None:
            record = value["records"][1]
            for entry in record["entries"]:
                if entry.get("kind") == "file":
                    entry["mode"] = 0o600
            record["tree_sha256"] = hashlib.sha256(
                canonical_json_bytes(record["entries"])
            ).hexdigest()

        check(
            "mutation-corpus-authority-entry-mode-rebound",
            lambda: authority_payload_rejected(
                mutated_authority_payload(rebind_authority_entry_mode),
                "entry 1 mode differs",
            ),
        )

        def rebind_authority_entry_mode_as_float(
            value: dict[str, Any],
        ) -> None:
            record = value["records"][1]
            for entry in record["entries"]:
                if entry.get("kind") == "file":
                    entry["mode"] = float(entry["mode"])
            record["tree_sha256"] = hashlib.sha256(
                canonical_json_bytes(record["entries"])
            ).hexdigest()

        check(
            "mutation-corpus-authority-entry-mode-float-for-int",
            lambda: authority_payload_rejected(
                mutated_authority_payload(
                    rebind_authority_entry_mode_as_float
                ),
                "entry 1 mode differs",
            ),
        )
        check(
            "mutation-corpus-authority-tree-hash-rebound",
            lambda: authority_payload_rejected(
                mutated_authority_payload(
                    lambda value: value["records"][0].__setitem__(
                        "tree_sha256", "0" * 64
                    )
                ),
                "tree hash differs from sealed entries",
            ),
        )
        positive_children = read_jsonl(
            output / "child-manifest.jsonl",
            "self-test positive child manifest",
            Problems(),
        )
        positive_specialized = [
            child
            for child in positive_children
            if child.get("kind") in schema.SPECIALIZED_SMOKE_TRANSITION_KINDS
        ]
        checks.append(
            {
                "name": "full-positive-specialized-transition-order-and-context",
                "pass": (
                    [child.get("kind") for child in positive_specialized]
                    == list(schema.SPECIALIZED_SMOKE_TRANSITION_KINDS)
                    and [
                        child.get("context", {}).get("smoke_id")
                        for child in positive_specialized
                    ]
                    == list(schema.SPECIALIZED_SMOKE_TRANSITION_KINDS)
                    and all(
                        child.get("context", {}).get("transition") == "smoke"
                        and child.get("context", {}).get("variant") == "A"
                        and isinstance(child.get("argv"), list)
                        and len(child["argv"]) == 1
                        and Path(child["argv"][0]).name == "rebaseline-bench"
                        for child in positive_specialized
                    )
                ),
                "detail": json.dumps(
                    [
                        {
                            "kind": child.get("kind"),
                            "smoke_id": child.get("context", {}).get("smoke_id"),
                        }
                        for child in positive_specialized
                    ],
                    sort_keys=True,
                ),
            }
        )
        positive_seed_child = next(
            child
            for child in positive_specialized
            if child.get("kind") == "smoke_reopen_seed"
        )
        positive_seed_raw = json.loads(
            Path(str(positive_seed_child["raw_path"])).read_bytes()
        )
        expected_seed_authority = {
            "domain_events": 1,
            "visible_events": 1,
            "log_events": 3,
            "stream_digest": "8368214f77995ee5",
            "logical_digest": (
                "000000000000000000000000000000000000000000000000"
                "e2d874aa120f66af"
            ),
            "registry_head_digest": (
                "000000000000000000000000000000000000000000000000"
                "88201fb960ff6465"
            ),
        }
        checks.append(
            {
                "name": "full-positive-specialized-seed-independent-exact-oracle",
                "pass": (
                    SPECIALIZED_SEED_AUTHORITY == expected_seed_authority
                    and all(
                        positive_seed_raw.get(field) == expected_seed_authority[field]
                        for field in (
                            "domain_events",
                            "visible_events",
                            "log_events",
                            "logical_digest",
                            "registry_head_digest",
                        )
                    )
                    and all(
                        child.get("context", {}).get("expected_log_events") == 3
                        and child.get("context", {}).get(
                            "expected_logical_digest"
                        )
                        == expected_seed_authority["logical_digest"]
                        and child.get("context", {}).get(
                            "expected_registry_head_digest"
                        )
                        == expected_seed_authority["registry_head_digest"]
                        for child in positive_specialized
                        if child.get("kind")
                        in {"smoke_reopen", "smoke_structural_reopen"}
                    )
                ),
                "detail": json.dumps(
                    {
                        "authority": SPECIALIZED_SEED_AUTHORITY,
                        "seed": positive_seed_raw,
                    },
                    sort_keys=True,
                ),
            }
        )
        prepared_fixture = json.loads((output / "prepared-artifacts.json").read_bytes())
        config_fixture = json.loads((output / "config.json").read_bytes())
        claim_fixture_path = Path(prepared_fixture["single_use_claim"]["path"])
        claim_fixture = json.loads(claim_fixture_path.read_bytes())
        original_prepared_fixture_path = Path(
            claim_fixture["prepared_artifacts_path"]
        )
        original_approval_fixture_path = Path(
            prepared_fixture["source_approval"]["path"]
        )
        checks.append(
            {
                "name": "producer-real-original-and-attempt-authority-copies",
                "pass": (
                    original_prepared_fixture_path
                    != output / "prepared-artifacts.json"
                    and original_approval_fixture_path
                    != output / "source-approval.json"
                    and original_prepared_fixture_path.read_bytes()
                    == (output / "prepared-artifacts.json").read_bytes()
                    and original_approval_fixture_path.read_bytes()
                    == (output / "source-approval.json").read_bytes()
                    and original_approval_fixture_path.relative_to(
                        original_prepared_fixture_path.parent
                    ).parts
                    == schema.PREPARED_SOURCE_APPROVAL_RELATIVE_PATH
                ),
                "detail": str(original_prepared_fixture_path.parent),
            }
        )
        checks.append(
            {
                "name": "producer-manifest-source-approval-binding-layout",
                "pass": (
                    prepared_fixture["source_approval"]["path"]
                    == str(
                        original_prepared_fixture_path.parent.joinpath(
                            *schema.PREPARED_SOURCE_APPROVAL_RELATIVE_PATH
                        )
                    )
                    and stat.S_IMODE(
                        original_approval_fixture_path.parent.stat().st_mode
                    )
                    == 0o555
                    and original_approval_fixture_path.name
                    == "source-approval.json"
                ),
                "detail": prepared_fixture["source_approval"]["path"],
            }
        )
        producer_source_path = (
            Path(__file__).resolve().parent
            / "tooling"
            / "prepare_overlays.py"
        )
        producer_source = (
            producer_source_path.read_text()
            if producer_source_path.is_file()
            else None
        )
        checks.append(
            {
                "name": "colocated-producer-source-approval-layout-static-compatibility",
                "pass": (
                    producer_source is None
                    or (
                        'bound_approval_path = output / "bindings" / "source-approval.json"'
                        in producer_source
                        and '"source_approval": {"path": str(bound_approval_path.resolve()), "sha256": approval_sha256}'
                        in producer_source
                    )
                ),
                "detail": (
                    str(producer_source_path)
                    if producer_source is not None
                    else "producer source is merged in its separate owned path"
                ),
            }
        )
        transition_authority_problems = Problems()
        transition_corpus_authority = validate_corpus_execution_authority(
            schema.parse_canonical_json_object(
                authority_payload, "self-test corpus authority"
            ),
            output.parent,
            config_fixture["attempt_nonce"],
            positive_children,
            transition_authority_problems,
            correctness_only=False,
        )
        transition_authority = reconstruct_transition_authority(
            prepared_fixture,
            output,
            output.parent,
            config_fixture["attempt_nonce"],
            positive_children,
            transition_corpus_authority,
            transition_authority_problems,
        )
        checks.append(
            {
                "name": "full-positive-reconstructed-runner-transition-plan",
                "pass": (
                    not transition_authority_problems.errors
                    and config_fixture["smoke_transitions"]
                    == [
                        {
                            "id": item["id"],
                            "variant": item["variant"],
                            "argv": item["argv"],
                        }
                        for item in transition_authority
                    ]
                    and [
                        child["kind"]
                        for child in positive_children[: len(transition_authority)]
                    ]
                    == [item["kind"] for item in transition_authority]
                ),
                "detail": str(len(transition_authority)),
            }
        )
        support_parent = Path(
            prepared_fixture["support_files"]["evaluator"]["path"]
        ).parent
        checks.append(
            {
                "name": "relocated-support-consumes-attempt-root-inputs",
                "pass": (
                    positive_rc == EXIT_ADMIT
                    and support_parent != Path(__file__).resolve().parent
                    and not (support_parent / schema.PREPARED_INPUT_FILENAMES["protocol"]).exists()
                    and not (support_parent / schema.PREPARED_INPUT_FILENAMES["historical_baseline"]).exists()
                ),
                "detail": str(support_parent),
            }
        )

        def rejects_tooling_transition_variant() -> bool:
            config_value = json.loads((output / "config.json").read_bytes())
            config_value["smoke_transitions"][0]["variant"] = "tooling"
            validation = Problems()
            validate_config(
                config_value,
                schema.PROTOCOL_SHA256,
                prepared_fixture["source_approval"]["sha256"],
                validation,
            )
            return any(
                "smoke transition 1 variant invalid" in error
                for error in validation.errors
            )

        check(
            "config-rejects-obsolete-tooling-transition-variant",
            rejects_tooling_transition_variant,
        )

        def mutate_json(path: Path, mutator: Callable[[dict[str, Any]], None]) -> bool:
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                value = json.loads(original)
                mutator(value)
                path.chmod(0o644)
                path.write_bytes(canonical_json_bytes(value))
                path.chmod(mode)
                result, rc = evaluate_directory(output, synthetic=True, publish=False)
                return rc == EXIT_INCONCLUSIVE and result["outcome"] == "INCONCLUSIVE" and not result["evidence_valid"]
            finally:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)

        def mutate_bytes(path: Path, mutation: bytes) -> bool:
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                path.chmod(0o755 if mode & 0o111 else 0o644)
                path.write_bytes(mutation)
                path.chmod(mode)
                result, rc = evaluate_directory(output, synthetic=True, publish=False)
                return rc == EXIT_INCONCLUSIVE and not result["evidence_valid"]
            finally:
                path.chmod(0o755 if mode & 0o111 else 0o644)
                path.write_bytes(original)
                path.chmod(mode)

        def mutate_json_expect(
            path: Path,
            mutator: Callable[[dict[str, Any]], None],
            expected_error: str,
        ) -> bool:
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                value = json.loads(original)
                mutator(value)
                path.chmod(0o644)
                path.write_bytes(canonical_json_bytes(value))
                path.chmod(mode)
                result, rc = evaluate_directory(
                    output, synthetic=True, publish=False
                )
                return (
                    rc == EXIT_INCONCLUSIVE
                    and not result["evidence_valid"]
                    and any(
                        expected_error in error for error in result["errors"]
                    )
                )
            finally:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)

        def hardlink_alias_expect(
            original_path: Path,
            attempt_path: Path,
            expected_error: str,
        ) -> bool:
            attempt_bytes = attempt_path.read_bytes()
            attempt_mode = stat.S_IMODE(attempt_path.stat().st_mode)
            try:
                attempt_path.unlink()
                os.link(original_path, attempt_path)
                result, rc = evaluate_directory(
                    output, synthetic=True, publish=False
                )
                return (
                    rc == EXIT_INCONCLUSIVE
                    and not result["evidence_valid"]
                    and any(
                        expected_error in error for error in result["errors"]
                    )
                )
            finally:
                if attempt_path.exists():
                    attempt_path.unlink()
                fixture_write_file(attempt_path, attempt_bytes, attempt_mode)

        def root_level_source_approval_rejected() -> bool:
            prepared_root = original_prepared_fixture_path.parent
            root_level_approval = prepared_root / "source-approval.json"
            attempt_prepared_path = output / "prepared-artifacts.json"
            original_prepared_bytes = original_prepared_fixture_path.read_bytes()
            attempt_prepared_bytes = attempt_prepared_path.read_bytes()
            claim_bytes = claim_fixture_path.read_bytes()
            root_mode = stat.S_IMODE(prepared_root.stat().st_mode)
            try:
                prepared_root.chmod(0o755)
                fixture_write_file(
                    root_level_approval,
                    original_approval_fixture_path.read_bytes(),
                )
                prepared_root.chmod(root_mode)
                value = json.loads(original_prepared_bytes)
                value["source_approval"]["path"] = str(root_level_approval)
                rebound = canonical_json_bytes(value)
                for path in (original_prepared_fixture_path, attempt_prepared_path):
                    path.chmod(0o644)
                    path.write_bytes(rebound)
                    path.chmod(0o444)
                claim = json.loads(claim_bytes)
                claim["prepared_artifacts_sha256"] = hashlib.sha256(
                    rebound
                ).hexdigest()
                claim_fixture_path.chmod(0o644)
                claim_fixture_path.write_bytes(canonical_json_bytes(claim))
                claim_fixture_path.chmod(0o444)
                result, rc = evaluate_directory(
                    output, synthetic=True, publish=False
                )
                return (
                    rc == EXIT_INCONCLUSIVE
                    and not result["evidence_valid"]
                    and any(
                        "prepared original source approval path differs" in error
                        for error in result["errors"]
                    )
                )
            finally:
                for path, payload in (
                    (original_prepared_fixture_path, original_prepared_bytes),
                    (attempt_prepared_path, attempt_prepared_bytes),
                    (claim_fixture_path, claim_bytes),
                ):
                    path.chmod(0o644)
                    path.write_bytes(payload)
                    path.chmod(0o444)
                prepared_root.chmod(0o755)
                if root_level_approval.exists():
                    root_level_approval.unlink()
                prepared_root.chmod(root_mode)

        check(
            "mutation-attempt-original-prepared-hardlink-alias",
            lambda: hardlink_alias_expect(
                original_prepared_fixture_path,
                output / "prepared-artifacts.json",
                "attempt/original prepared artifacts are hardlink aliases",
            ),
        )
        check(
            "mutation-attempt-original-source-hardlink-alias",
            lambda: hardlink_alias_expect(
                original_approval_fixture_path,
                output / "source-approval.json",
                "attempt/original source approval are hardlink aliases",
            ),
        )
        check(
            "mutation-root-level-source-approval-layout-rejected",
            root_level_source_approval_rejected,
        )

        check(
            "mutation-attempt-original-prepared-divergence",
            lambda: mutate_json_expect(
                output / "prepared-artifacts.json",
                lambda value: value.__setitem__(
                    "created_monotonic_ns", value["created_monotonic_ns"] + 1
                ),
                "attempt/original prepared artifacts bytes differ",
            ),
        )
        check(
            "mutation-claim-rejects-attempt-prepared-path",
            lambda: mutate_json_expect(
                claim_fixture_path,
                lambda value: value.__setitem__(
                    "prepared_artifacts_path",
                    str(output / "prepared-artifacts.json"),
                ),
                "claim original artifact path differs",
            ),
        )
        check(
            "mutation-attempt-original-source-divergence",
            lambda: mutate_json_expect(
                original_approval_fixture_path,
                lambda value: value.__setitem__("review_id", "cr-forged"),
                "original prepared source approval hash mismatch",
            ),
        )
        for field, value, expected_error in (
            ("output_dir", "/tmp/forged-output", "claim output differs"),
            ("attempt_nonce", "0" * 64, "claim attempt nonce differs"),
            ("lease_nonce", "0" * 64, "claim lease nonce differs"),
            (
                "claimed_at",
                "2020-01-01T00:00:00+00:00",
                "claim chronology differs",
            ),
            ("claimed_monotonic_ns", 1, "claim chronology differs"),
        ):
            check(
                f"mutation-claim-{field.replace('_', '-')}",
                lambda field=field, value=value, expected_error=expected_error: (
                    mutate_json_expect(
                        claim_fixture_path,
                        lambda claim: claim.__setitem__(field, value),
                        expected_error,
                    )
                ),
            )

        def mutate_mode(path: Path, mutation: int) -> bool:
            original = stat.S_IMODE(path.stat().st_mode)
            try:
                path.chmod(mutation)
                result, rc = evaluate_directory(output, synthetic=True, publish=False)
                return rc == EXIT_INCONCLUSIVE and not result["evidence_valid"]
            finally:
                path.chmod(original)

        check(
            "mutation-config-resource-floor",
            lambda: mutate_json(output / "config.json", lambda value: value["resource_limits"].__setitem__("free_bytes", 1)),
        )
        check(
            "mutation-v2-config-schema-rejected",
            lambda: mutate_json(
                output / "config.json",
                lambda value: value.__setitem__("schema", "bn-2l3n-config-v2"),
            ),
        )
        check(
            "mutation-source-approval-binding",
            lambda: mutate_json(output / "source-approval.json", lambda value: value.__setitem__("status", "pending")),
        )
        check(
            "mutation-source-filesystem-admission",
            lambda: mutate_json(
                output / "source-approval.json",
                lambda value: value["filesystem_admission"].__setitem__(
                    "filesystem", "xfs"
                ),
            ),
        )
        check(
            "mutation-prepared-filesystem-admission",
            lambda: mutate_json(
                output / "prepared-artifacts.json",
                lambda value: value["filesystem_admission"].__setitem__(
                    "available_inodes", schema.MIN_FREE_INODES - 1
                ),
            ),
        )
        check(
            "mutation-source-comm-allowlist-extra",
            lambda: mutate_json(
                output / "source-approval.json",
                lambda value: value["comm_allowlist"].append("unapproved"),
            ),
        )
        check(
            "mutation-source-tools-manifest-claim",
            lambda: mutate_json(
                output / "source-approval.json",
                lambda value: value["tools_manifest"]["tools"]["perf"].__setitem__(
                    "comm", "unapproved"
                ),
            ),
        )
        check(
            "mutation-v2-tools-manifest-schema-rejected",
            lambda: mutate_json(
                output / "source-approval.json",
                lambda value: value["tools_manifest"].__setitem__(
                    "schema", "asterism-rebaseline-tools-v2"
                ),
            ),
        )
        check(
            "mutation-source-approval-mode",
            lambda: mutate_mode(output / "source-approval.json", 0o644),
        )
        check(
            "mutation-prepared-artifacts-mode",
            lambda: mutate_mode(output / "prepared-artifacts.json", 0o644),
        )
        check(
            "mutation-attempt-protocol-bytes",
            lambda: mutate_bytes(
                output / schema.PREPARED_INPUT_FILENAMES["protocol"],
                b"mutated protocol\n",
            ),
        )
        check(
            "mutation-attempt-historical-baseline-bytes",
            lambda: mutate_bytes(
                output / schema.PREPARED_INPUT_FILENAMES["historical_baseline"],
                b"mutated historical baseline\n",
            ),
        )
        check(
            "mutation-attempt-historical-baseline-mode",
            lambda: mutate_mode(
                output / schema.PREPARED_INPUT_FILENAMES["historical_baseline"],
                0o644,
            ),
        )
        prepared = json.loads((output / "prepared-artifacts.json").read_bytes())
        check(
            "mutation-prepared-tools-manifest-mode-binding",
            lambda: mutate_json(
                output / "prepared-artifacts.json",
                lambda value: value["tools_manifest"].__setitem__("mode", 0o644),
            ),
        )
        check(
            "mutation-prepared-tool-differs-from-approved-claim",
            lambda: mutate_json(
                output / "prepared-artifacts.json",
                lambda value: value["tools"]["perf"].__setitem__(
                    "comm", "unapproved"
                ),
            ),
        )
        check(
            "mutation-prepared-tools-manifest-bytes",
            lambda: mutate_bytes(
                Path(prepared["tools_manifest"]["path"]),
                b'{"mutated":true}\n',
            ),
        )
        check(
            "mutation-prepared-historical-baseline-bytes",
            lambda: mutate_bytes(
                Path(prepared["inputs"]["historical_baseline"]["path"]),
                b"mutated prepared historical baseline\n",
            ),
        )
        check(
            "mutation-prepared-input-mode-binding",
            lambda: mutate_json(
                output / "prepared-artifacts.json",
                lambda value: value["inputs"]["protocol"].__setitem__("mode", 0o644),
            ),
        )
        check(
            "mutation-prepared-support-directory-mode",
            lambda: mutate_mode(support_parent, 0o755),
        )

        def unexpected_support_entry() -> bool:
            extra = support_parent / "__pycache__"
            original_mode = stat.S_IMODE(support_parent.stat().st_mode)
            try:
                support_parent.chmod(0o755)
                extra.mkdir()
                support_parent.chmod(original_mode)
                result, rc = evaluate_directory(output, synthetic=True, publish=False)
                return rc == EXIT_INCONCLUSIVE and not result["evidence_valid"]
            finally:
                support_parent.chmod(0o755)
                extra.rmdir()
                support_parent.chmod(original_mode)

        check("mutation-prepared-support-unexpected-entry", unexpected_support_entry)
        binary_path = Path(prepared["variants"]["A"]["binary"]["path"])
        check("mutation-binary-hash-replay", lambda: mutate_bytes(binary_path, b"mutated fixture binary\n"))
        lock_path = Path(prepared["variants"]["C"]["attestation"]["cargo_lock_path"])
        check("mutation-lock-hash-replay", lambda: mutate_bytes(lock_path, b"mutated fixture lock\n"))
        check(
            "mutation-resolution-environment-injected-key",
            lambda: mutate_json(
                output / "source-approval.json",
                lambda value: value["variants"]["A"]["lock_resolution"]["environment"].__setitem__(
                    "RUSTFLAGS", "-Ctarget-cpu=native"
                ),
            ),
        )
        check(
            "mutation-build-environment-injected-key",
            lambda: mutate_json(
                output / "prepared-artifacts.json",
                lambda value: value["variants"]["A"]["attestation"]["build_env"].__setitem__(
                    "RUSTC_WRAPPER", "/tmp/wrapper"
                ),
            ),
        )
        check(
            "mutation-C-trace-metadata-marker-omitted",
            lambda: mutate_json(
                output / "prepared-artifacts.json",
                lambda value: value["variants"]["C"]["evidence_env"].__setitem__(
                    "ASTERISM_REBASELINE_METADATA_PATH_MARKERS",
                    '["log/sealed/","snapshots/blobs/","snapshots/meta/"]',
                ),
            ),
        )
        check(
            "mutation-C-trace-marker-overlap",
            lambda: mutate_json(
                output / "prepared-artifacts.json",
                lambda value: value["variants"]["C"]["evidence_env"].__setitem__(
                    "ASTERISM_REBASELINE_METADATA_PATH_MARKERS",
                    '["log/","log/meta/","log/sealed/","snapshots/blobs/","snapshots/meta/"]',
                ),
            ),
        )
        approval_fixture = json.loads((output / "source-approval.json").read_bytes())
        config_search_path = Path(
            approval_fixture["variants"]["A"]["lock_resolution"]["cargo_config_search"]["path"]
        )
        check(
            "mutation-cargo-config-search-state",
            lambda: mutate_json(
                config_search_path,
                lambda value: value["entries"][0].__setitem__("status", "present"),
            ),
        )
        check(
            "mutation-lease-proof",
            lambda: mutate_json(output / "provenance.json", lambda value: value["lease"].__setitem__("second_exclusive_failed", False)),
        )
        check(
            "mutation-host-filesystem-not-ext4",
            lambda: mutate_json(
                output / "provenance.json",
                lambda value: value["host"]["filesystem"].__setitem__(
                    "filesystem_type", "xfs"
                ),
            ),
        )
        check(
            "mutation-host-initial-capacity-below-floor",
            lambda: mutate_json(
                output / "provenance.json",
                lambda value: value["host"].__setitem__(
                    "scratch_free_bytes_initial", 137_438_953_471
                ),
            ),
        )
        check(
            "mutation-host-initial-inodes-below-floor",
            lambda: mutate_json(
                output / "provenance.json",
                lambda value: value["host"].__setitem__(
                    "scratch_free_inodes_initial", 999_999
                ),
            ),
        )

        def host_floor_boundary_accepts() -> bool:
            path = output / "provenance.json"
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                value = json.loads(original)
                for suffix in ("initial", "final"):
                    value["host"][f"scratch_free_bytes_{suffix}"] = schema.MIN_FREE_BYTES
                    value["host"][f"scratch_free_inodes_{suffix}"] = schema.MIN_FREE_INODES
                path.chmod(0o644)
                path.write_bytes(canonical_json_bytes(value))
                path.chmod(mode)
                result, code = evaluate_directory(output, synthetic=True, publish=False)
                return code == EXIT_ADMIT and result["evidence_valid"] is True
            finally:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)

        check("host-capacity-exact-boundary-accepted", host_floor_boundary_accepts)

        def mutate_jsonl(path: Path, line_index: int, mutator: Callable[[dict[str, Any]], None]) -> bool:
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                lines = original.splitlines()
                value = json.loads(lines[line_index])
                mutator(value)
                lines[line_index] = canonical_json_bytes(value).rstrip(b"\n")
                path.chmod(0o644)
                path.write_bytes(b"\n".join(lines) + b"\n")
                path.chmod(mode)
                result, rc = evaluate_directory(output, synthetic=True, publish=False)
                return rc == EXIT_INCONCLUSIVE and not result["evidence_valid"]
            finally:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)

        def mutate_child_environment(
            predicate: Callable[[dict[str, Any]], bool],
            mutator: Callable[[dict[str, str]], None],
        ) -> bool:
            path = output / "child-manifest.jsonl"
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                records = [json.loads(line) for line in original.splitlines()]
                child = next(record for record in records if predicate(record))
                mutator(child["environment"])
                path.chmod(0o644)
                path.write_bytes(
                    b"".join(canonical_json_bytes(record) for record in records)
                )
                path.chmod(mode)
                result, rc = evaluate_directory(output, synthetic=True, publish=False)
                return rc == EXIT_INCONCLUSIVE and not result["evidence_valid"]
            finally:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)

        def mutate_rebound_children(
            mutator: Callable[[list[dict[str, Any]]], None],
            expected_error: str,
        ) -> bool:
            child_path = output / "child-manifest.jsonl"
            provenance_path = output / "provenance.json"
            original_children = child_path.read_bytes()
            original_provenance = provenance_path.read_bytes()
            child_mode = stat.S_IMODE(child_path.stat().st_mode)
            provenance_mode = stat.S_IMODE(provenance_path.stat().st_mode)
            try:
                records = [json.loads(line) for line in original_children.splitlines()]
                mutator(records)
                child_path.chmod(0o644)
                child_path.write_bytes(
                    b"".join(canonical_json_bytes(record) for record in records)
                )
                child_path.chmod(child_mode)
                provenance = json.loads(original_provenance)
                provenance["child_manifest_sha256"] = sha256_file(child_path)
                provenance_path.chmod(0o644)
                provenance_path.write_bytes(canonical_json_bytes(provenance))
                provenance_path.chmod(provenance_mode)
                result, rc = evaluate_directory(output, synthetic=True, publish=False)
                return (
                    rc == EXIT_INCONCLUSIVE
                    and not result["evidence_valid"]
                    and any(expected_error in error for error in result["errors"])
                )
            finally:
                child_path.chmod(0o644)
                child_path.write_bytes(original_children)
                child_path.chmod(child_mode)
                provenance_path.chmod(0o644)
                provenance_path.write_bytes(original_provenance)
                provenance_path.chmod(provenance_mode)

        def mutate_rebound_child(
            predicate: Callable[[dict[str, Any]], bool],
            mutator: Callable[[dict[str, Any]], None],
            expected_error: str,
        ) -> bool:
            def mutate(records: list[dict[str, Any]]) -> None:
                mutator(next(record for record in records if predicate(record)))

            return mutate_rebound_children(mutate, expected_error)

        def coordinated_transition_payload_mutation(
            predicate: Callable[[dict[str, Any]], bool],
            mutator: Callable[[dict[str, Any]], None],
            expected_error: str,
        ) -> bool:
            child_path = output / "child-manifest.jsonl"
            raw_path = output / "raw-manifest.json"
            provenance_path = output / "provenance.json"
            original_child = child_path.read_bytes()
            original_raw = raw_path.read_bytes()
            original_provenance = provenance_path.read_bytes()
            modes = {
                path: stat.S_IMODE(path.stat().st_mode)
                for path in (child_path, raw_path, provenance_path)
            }
            context_path: Path | None = None
            original_context: bytes | None = None
            try:
                children = [
                    json.loads(line) for line in original_child.splitlines()
                ]
                raw = [json.loads(line) for line in original_raw.splitlines()]
                index = next(
                    index
                    for index, child in enumerate(children)
                    if predicate(child)
                )
                child = children[index]
                before_context = copy.deepcopy(child["context"])
                mutator(child)
                if child["context"] != before_context:
                    context_sha256 = hashlib.sha256(
                        canonical_json_bytes(child["context"])
                    ).hexdigest()
                    child["context_sha256"] = context_sha256
                    child["environment"][
                        "ASTERISM_REBASELINE_CONTEXT_SHA256"
                    ] = context_sha256
                    child["environment"]["ASTERISM_CONTEXT_SHA256"] = (
                        context_sha256
                    )
                    raw[index]["context_sha256"] = context_sha256
                    context_path = output / "contexts" / f"{index + 1:05d}.json"
                    original_context = context_path.read_bytes()
                    context_path.chmod(0o644)
                    context_path.write_bytes(canonical_json_bytes(child["context"]))
                    context_path.chmod(0o444)
                child_path.chmod(0o644)
                child_path.write_bytes(
                    b"".join(canonical_json_bytes(item) for item in children)
                )
                child_path.chmod(modes[child_path])
                raw_path.chmod(0o644)
                raw_path.write_bytes(
                    b"".join(canonical_json_bytes(item) for item in raw)
                )
                raw_path.chmod(modes[raw_path])
                provenance = json.loads(original_provenance)
                provenance["child_manifest_sha256"] = sha256_file(child_path)
                provenance["raw_manifest_sha256"] = sha256_file(raw_path)
                provenance_path.chmod(0o644)
                provenance_path.write_bytes(canonical_json_bytes(provenance))
                provenance_path.chmod(modes[provenance_path])
                result, rc = evaluate_directory(
                    output, synthetic=True, publish=False
                )
                return (
                    rc == EXIT_INCONCLUSIVE
                    and not result["evidence_valid"]
                    and any(
                        expected_error in error for error in result["errors"]
                    )
                )
            finally:
                for path, payload in (
                    (child_path, original_child),
                    (raw_path, original_raw),
                    (provenance_path, original_provenance),
                ):
                    path.chmod(0o644)
                    path.write_bytes(payload)
                    path.chmod(modes[path])
                if context_path is not None and original_context is not None:
                    context_path.chmod(0o644)
                    context_path.write_bytes(original_context)
                    context_path.chmod(0o444)

        def mutate_rebound_config(
            mutator: Callable[[dict[str, Any]], None],
            expected_errors: Sequence[str],
        ) -> bool:
            config_path = output / "config.json"
            provenance_path = output / "provenance.json"
            original_config = config_path.read_bytes()
            original_provenance = provenance_path.read_bytes()
            config_mode = stat.S_IMODE(config_path.stat().st_mode)
            provenance_mode = stat.S_IMODE(provenance_path.stat().st_mode)
            try:
                config_value = json.loads(original_config)
                mutator(config_value)
                config_path.chmod(0o644)
                config_path.write_bytes(canonical_json_bytes(config_value))
                config_path.chmod(config_mode)
                config_digest = sha256_file(config_path)
                provenance = json.loads(original_provenance)
                provenance["config_sha256"] = config_digest
                provenance["host"]["frozen_files"][str(config_path)] = config_digest
                provenance_path.chmod(0o644)
                provenance_path.write_bytes(canonical_json_bytes(provenance))
                provenance_path.chmod(provenance_mode)
                result, rc = evaluate_directory(
                    output, synthetic=True, publish=False
                )
                return (
                    rc == EXIT_INCONCLUSIVE
                    and not result["evidence_valid"]
                    and all(
                        any(expected in error for error in result["errors"])
                        for expected in expected_errors
                    )
                )
            finally:
                config_path.chmod(0o644)
                config_path.write_bytes(original_config)
                config_path.chmod(config_mode)
                provenance_path.chmod(0o644)
                provenance_path.write_bytes(original_provenance)
                provenance_path.chmod(provenance_mode)

        contract_transition = lambda child: child.get("kind") == "contract"
        ordinary_transition = lambda child: (
            child.get("kind") == "smoke"
            and child.get("context", {}).get("smoke_target") == "primary"
        )
        tool_transition = lambda child: (
            child.get("kind") == "smoke"
            and child.get("context", {}).get("smoke_target") == "correctness"
        )
        runtime_transition = lambda child: (
            child.get("kind") == "smoke"
            and child.get("context", {}).get("smoke_target") == "evaluator"
        )
        specialized_transition = lambda child: (
            child.get("kind") == "smoke_reopen_seed"
        )
        for name, predicate, mutator, expected_error in (
            (
                "contract-context",
                contract_transition,
                lambda child: child["context"].__setitem__(
                    "transition", "smoke"
                ),
                "context differs from reconstructed authority",
            ),
            (
                "contract-environment",
                contract_transition,
                lambda child: child["environment"].__setitem__("LANG", "C"),
                "environment differs from reconstructed authority",
            ),
            (
                "ordinary-context",
                ordinary_transition,
                lambda child: child["context"].__setitem__(
                    "durability", "Group"
                ),
                "context differs from reconstructed authority",
            ),
            (
                "ordinary-environment",
                ordinary_transition,
                lambda child: child["environment"].__setitem__(
                    "ASTERISM_REBASELINE_SMOKE_TARGET", "forged"
                ),
                "environment differs from reconstructed authority",
            ),
            (
                "tool-context",
                tool_transition,
                lambda child: child["context"].__setitem__(
                    "smoke_ordinal", child["context"]["smoke_ordinal"] + 1
                ),
                "context differs from reconstructed authority",
            ),
            (
                "tool-environment",
                tool_transition,
                lambda child: child["environment"].__setitem__(
                    "ASTERISM_REBASELINE_MODE", "fault"
                ),
                "environment differs from reconstructed authority",
            ),
            (
                "runtime-context",
                runtime_transition,
                lambda child: child["context"].__setitem__(
                    "durability", "Group"
                ),
                "context differs from reconstructed authority",
            ),
            (
                "runtime-environment",
                runtime_transition,
                lambda child: child["environment"].__setitem__(
                    "ASTERISM_REBASELINE_SMOKE_TARGET", "forged"
                ),
                "environment differs from reconstructed authority",
            ),
            (
                "specialized-context-domain-events",
                specialized_transition,
                lambda child: child["context"].__setitem__(
                    "domain_events", 999
                ),
                "context differs from reconstructed authority",
            ),
            (
                "specialized-environment-mode",
                specialized_transition,
                lambda child: child["environment"].__setitem__(
                    "ASTERISM_REBASELINE_MODE", "smoke"
                ),
                "environment differs from reconstructed authority",
            ),
        ):
            check(
                f"mutation-coordinated-{name}",
                lambda predicate=predicate, mutator=mutator, expected_error=expected_error: (
                    coordinated_transition_payload_mutation(
                        predicate, mutator, expected_error
                    )
                ),
            )

        specialized_source = (
            output.parent
            / "attempts"
            / config_fixture["attempt_nonce"]
            / "smoke-reopen"
            / "archive-source"
        )
        specialized_copies = [
            schema.fresh_store_path(
                output.parent,
                config_fixture["attempt_nonce"],
                "smoke-reopen-corpus",
                ordinal,
                "A",
            )
            for ordinal in (1, 2)
        ]

        def unsafe_fixture_corpus_digest(root: Path) -> str:
            entries: list[dict[str, Any]] = []
            for path in sorted(root.rglob("*")):
                relative = path.relative_to(root).as_posix()
                metadata = path.lstat()
                if stat.S_ISDIR(metadata.st_mode):
                    entries.append({"path": relative, "kind": "directory"})
                elif stat.S_ISREG(metadata.st_mode):
                    data = path.read_bytes()
                    entries.append(
                        {
                            "path": relative,
                            "kind": "file",
                            "bytes": len(data),
                            "sha256": hashlib.sha256(data).hexdigest(),
                        }
                    )
                else:
                    raise AssertionError(f"unsafe digest unsupported entry {path}")
            return hashlib.sha256(canonical_json_bytes(entries)).hexdigest()

        def run_specialized_corpus_hostile(
            apply: Callable[[], None],
            restore: Callable[[], None],
            expected_errors: Sequence[str],
            *,
            rebind_source_digest: bool = False,
        ) -> bool:
            child_path = output / "child-manifest.jsonl"
            raw_path = output / "raw-manifest.json"
            provenance_path = output / "provenance.json"
            artifact_paths = (child_path, raw_path, provenance_path)
            originals = {path: path.read_bytes() for path in artifact_paths}
            modes = {
                path: stat.S_IMODE(path.stat().st_mode)
                for path in artifact_paths
            }
            context_originals: dict[Path, tuple[bytes, int]] = {}
            try:
                apply()
                if rebind_source_digest:
                    rebound_digest = unsafe_fixture_corpus_digest(
                        specialized_source
                    )
                    children = [
                        json.loads(line)
                        for line in originals[child_path].splitlines()
                    ]
                    raw = [
                        json.loads(line)
                        for line in originals[raw_path].splitlines()
                    ]
                    for index, child in enumerate(children):
                        if child.get("kind") not in {
                            "smoke_reopen",
                            "smoke_structural_reopen",
                        }:
                            continue
                        child["context"]["archive_manifest_sha256"] = (
                            rebound_digest
                        )
                        child["context"]["copy_manifest_sha256"] = (
                            rebound_digest
                        )
                        context_sha256 = hashlib.sha256(
                            canonical_json_bytes(child["context"])
                        ).hexdigest()
                        child["context_sha256"] = context_sha256
                        child["environment"][
                            "ASTERISM_REBASELINE_CONTEXT_SHA256"
                        ] = context_sha256
                        child["environment"]["ASTERISM_CONTEXT_SHA256"] = (
                            context_sha256
                        )
                        raw[index]["context_sha256"] = context_sha256
                        context_path = (
                            output / "contexts" / f"{index + 1:05d}.json"
                        )
                        context_originals[context_path] = (
                            context_path.read_bytes(),
                            stat.S_IMODE(context_path.stat().st_mode),
                        )
                        context_path.chmod(0o644)
                        context_path.write_bytes(
                            canonical_json_bytes(child["context"])
                        )
                        context_path.chmod(0o444)
                    child_path.chmod(0o644)
                    child_path.write_bytes(
                        b"".join(
                            canonical_json_bytes(child) for child in children
                        )
                    )
                    child_path.chmod(modes[child_path])
                    raw_path.chmod(0o644)
                    raw_path.write_bytes(
                        b"".join(canonical_json_bytes(item) for item in raw)
                    )
                    raw_path.chmod(modes[raw_path])
                    provenance = json.loads(originals[provenance_path])
                    provenance["child_manifest_sha256"] = sha256_file(
                        child_path
                    )
                    provenance["raw_manifest_sha256"] = sha256_file(raw_path)
                    provenance_path.chmod(0o644)
                    provenance_path.write_bytes(
                        canonical_json_bytes(provenance)
                    )
                    provenance_path.chmod(modes[provenance_path])
                result, rc = evaluate_directory(
                    output, synthetic=True, publish=False
                )
                return (
                    rc == EXIT_INCONCLUSIVE
                    and not result["evidence_valid"]
                    and all(
                        any(expected in error for error in result["errors"])
                        for expected in expected_errors
                    )
                )
            finally:
                for path, payload in originals.items():
                    path.chmod(0o644)
                    path.write_bytes(payload)
                    path.chmod(modes[path])
                for path, (payload, mode) in context_originals.items():
                    path.chmod(0o644)
                    path.write_bytes(payload)
                    path.chmod(mode)
                restore()

        def source_file_rebind_hostile() -> bool:
            path = specialized_source / "corpus.bin"
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)

            def apply() -> None:
                path.chmod(0o644)
                path.write_bytes(b"attacker-rebound-source\n")
                path.chmod(mode)

            def restore() -> None:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)

            return run_specialized_corpus_hostile(
                apply,
                restore,
                ["current tree differs from sealed authority"],
                rebind_source_digest=True,
            )

        check(
            "mutation-specialized-source-only-digest-context-provenance-rebind",
            source_file_rebind_hostile,
        )

        def coordinated_all_specialized_trees_rebind_hostile() -> bool:
            paths = [
                specialized_source / "corpus.bin",
                *(root / "corpus.bin" for root in specialized_copies),
            ]
            originals = {path: path.read_bytes() for path in paths}
            modes = {
                path: stat.S_IMODE(path.stat().st_mode) for path in paths
            }

            def apply() -> None:
                for path in paths:
                    path.chmod(0o644)
                    path.write_bytes(b"coordinated-attacker-corpus\n")
                    path.chmod(modes[path])

            def restore() -> None:
                for path in paths:
                    path.chmod(0o644)
                    path.write_bytes(originals[path])
                    path.chmod(modes[path])

            return run_specialized_corpus_hostile(
                apply,
                restore,
                ["current tree differs from sealed authority"],
                rebind_source_digest=True,
            )

        check(
            "mutation-specialized-all-three-trees-context-provenance-rebind",
            coordinated_all_specialized_trees_rebind_hostile,
        )

        def source_extra_rebind_hostile() -> bool:
            path = specialized_source / "attacker-extra.bin"
            root_mode = stat.S_IMODE(specialized_source.stat().st_mode)

            def apply() -> None:
                specialized_source.chmod(0o755)
                fixture_write_file(path, b"extra\n")
                specialized_source.chmod(root_mode)

            def restore() -> None:
                specialized_source.chmod(0o755)
                path.unlink(missing_ok=True)
                specialized_source.chmod(root_mode)

            return run_specialized_corpus_hostile(
                apply,
                restore,
                ["current tree differs from sealed authority"],
                rebind_source_digest=True,
            )

        check(
            "mutation-specialized-source-extra-entry-rebind",
            source_extra_rebind_hostile,
        )

        def source_empty_rebind_hostile() -> bool:
            path = specialized_source / "corpus.bin"
            parked = specialized_source.parent / "parked-empty-corpus.bin"
            root_mode = stat.S_IMODE(specialized_source.stat().st_mode)

            def apply() -> None:
                specialized_source.chmod(0o755)
                path.rename(parked)
                specialized_source.chmod(root_mode)

            def restore() -> None:
                specialized_source.chmod(0o755)
                if parked.exists():
                    parked.rename(path)
                specialized_source.chmod(root_mode)

            return run_specialized_corpus_hostile(
                apply,
                restore,
                ["corpus contains no regular files"],
                rebind_source_digest=True,
            )

        check(
            "mutation-specialized-source-empty-corpus-rebind",
            source_empty_rebind_hostile,
        )

        def source_missing_hostile() -> bool:
            parked = specialized_source.parent / "parked-missing-source"

            def apply() -> None:
                specialized_source.rename(parked)

            def restore() -> None:
                if parked.exists():
                    parked.rename(specialized_source)

            return run_specialized_corpus_hostile(
                apply,
                restore,
                ["corpus execution authority record 1 current tree snapshot"],
            )

        check("mutation-specialized-source-missing", source_missing_hostile)

        def source_root_symlink_hostile() -> bool:
            parked = specialized_source.parent / "parked-symlink-source"

            def apply() -> None:
                specialized_source.rename(parked)
                specialized_source.symlink_to(
                    specialized_copies[0], target_is_directory=True
                )

            def restore() -> None:
                if specialized_source.is_symlink():
                    specialized_source.unlink()
                if parked.exists():
                    parked.rename(specialized_source)

            return run_specialized_corpus_hostile(
                apply,
                restore,
                ["corpus path contains symlink component"],
            )

        check(
            "mutation-specialized-source-root-symlink",
            source_root_symlink_hostile,
        )

        def source_outside_hardlink_rebind_hostile() -> bool:
            outside = output.parent / "outside-hardlink.bin"
            link = specialized_source / "outside-hardlink.bin"
            root_mode = stat.S_IMODE(specialized_source.stat().st_mode)

            def apply() -> None:
                fixture_write_file(outside, b"outside\n")
                specialized_source.chmod(0o755)
                os.link(outside, link)
                specialized_source.chmod(root_mode)

            def restore() -> None:
                specialized_source.chmod(0o755)
                link.unlink(missing_ok=True)
                specialized_source.chmod(root_mode)
                outside.unlink(missing_ok=True)

            return run_specialized_corpus_hostile(
                apply,
                restore,
                ["corpus file is hard-linked"],
                rebind_source_digest=True,
            )

        check(
            "mutation-specialized-source-outside-hardlink-rebind",
            source_outside_hardlink_rebind_hostile,
        )

        def copy_file_mutation_hostile(copy_index: int) -> bool:
            path = specialized_copies[copy_index] / "corpus.bin"
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)

            def apply() -> None:
                path.chmod(0o644)
                path.write_bytes(f"mutated-copy-{copy_index}\n".encode())
                path.chmod(mode)

            def restore() -> None:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)

            return run_specialized_corpus_hostile(
                apply,
                restore,
                ["current tree differs from sealed authority"],
            )

        for copy_index in (0, 1):
            check(
                f"mutation-specialized-executed-copy-{copy_index + 1}-bytes",
                lambda copy_index=copy_index: copy_file_mutation_hostile(
                    copy_index
                ),
            )

        def full_corpus_copy_mutation_hostile() -> bool:
            record = json.loads(authority_payload)["records"][6]
            path = Path(str(record["root"])) / "corpus.bin"
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                path.chmod(0o644)
                path.write_bytes(b"attacker-full-corpus-copy\n")
                path.chmod(mode)
                result, code = evaluate_directory(
                    output, synthetic=True, publish=False
                )
                return (
                    code == EXIT_INCONCLUSIVE
                    and result["evidence_valid"] is False
                    and any(
                        "current tree differs from sealed authority" in error
                        for error in result["errors"]
                    )
                )
            finally:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)

        check(
            "mutation-full-corpus-current-tree-bytes",
            full_corpus_copy_mutation_hostile,
        )

        def specialized_copy_chmod_hostile() -> bool:
            path = specialized_copies[0] / "corpus.bin"
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                path.chmod(0o600)
                result, code = evaluate_directory(
                    output, synthetic=True, publish=False
                )
                return (
                    code == EXIT_INCONCLUSIVE
                    and result["evidence_valid"] is False
                    and any(
                        "corpus file mode differs" in error
                        for error in result["errors"]
                    )
                )
            finally:
                path.chmod(mode)

        check(
            "mutation-specialized-copy-current-file-mode-0600",
            specialized_copy_chmod_hostile,
        )

        def copy_extra_hostile() -> bool:
            path = specialized_copies[0] / "attacker-extra.bin"

            def apply() -> None:
                fixture_write_file(path, b"copy-extra\n", mode=0o644)

            def restore() -> None:
                path.unlink(missing_ok=True)

            return run_specialized_corpus_hostile(
                apply,
                restore,
                ["current tree differs from sealed authority"],
            )

        check("mutation-specialized-copy-extra-entry", copy_extra_hostile)

        def copy_missing_hostile() -> bool:
            path = specialized_copies[0]
            parked = path.parent / "parked-missing-copy"

            def apply() -> None:
                path.rename(parked)

            def restore() -> None:
                if parked.exists():
                    parked.rename(path)

            return run_specialized_corpus_hostile(
                apply,
                restore,
                ["corpus execution authority record 2 current tree snapshot"],
            )

        check("mutation-specialized-copy-missing", copy_missing_hostile)

        def copy_symlink_hostile() -> bool:
            root = specialized_copies[0]
            path = root / "corpus.bin"
            parked = root.parent / "parked-symlink-copy.bin"

            def apply() -> None:
                path.rename(parked)
                path.symlink_to(parked)

            def restore() -> None:
                if path.is_symlink():
                    path.unlink()
                if parked.exists():
                    parked.rename(path)

            return run_specialized_corpus_hostile(
                apply,
                restore,
                ["corpus contains symlink"],
            )

        check("mutation-specialized-copy-symlink", copy_symlink_hostile)

        def copy_cross_hardlink_hostile() -> bool:
            source_path = specialized_copies[0] / "corpus.bin"
            target_path = specialized_copies[1] / "corpus.bin"
            original = target_path.read_bytes()
            mode = stat.S_IMODE(target_path.stat().st_mode)

            def apply() -> None:
                target_path.unlink()
                os.link(source_path, target_path)

            def restore() -> None:
                target_path.unlink(missing_ok=True)
                fixture_write_file(target_path, original, mode=mode)

            return run_specialized_corpus_hostile(
                apply,
                restore,
                ["corpus file is hard-linked"],
            )

        check(
            "mutation-specialized-copy-cross-hardlink-alias",
            copy_cross_hardlink_hostile,
        )

        def source_writable_mode_hostile() -> bool:
            path = specialized_source / "corpus.bin"
            mode = stat.S_IMODE(path.stat().st_mode)

            def apply() -> None:
                path.chmod(0o644)

            def restore() -> None:
                path.chmod(mode)

            return run_specialized_corpus_hostile(
                apply,
                restore,
                ["corpus file mode differs"],
            )

        check(
            "mutation-specialized-source-read-only-mode",
            source_writable_mode_hostile,
        )

        def coordinated_seed_observation_rebind(
            logical_digest: str,
            registry_head_digest: str,
            log_events: int,
        ) -> bool:
            child_path = output / "child-manifest.jsonl"
            raw_manifest_path = output / "raw-manifest.json"
            provenance_path = output / "provenance.json"
            artifact_paths = (child_path, raw_manifest_path, provenance_path)
            originals = {path: path.read_bytes() for path in artifact_paths}
            modes = {
                path: stat.S_IMODE(path.stat().st_mode)
                for path in artifact_paths
            }
            children = [
                json.loads(line) for line in originals[child_path].splitlines()
            ]
            raw_bindings = [
                json.loads(line)
                for line in originals[raw_manifest_path].splitlines()
            ]
            seed_index = next(
                index
                for index, child in enumerate(children)
                if child.get("kind") == "smoke_reopen_seed"
            )
            seed_raw_path = Path(str(children[seed_index]["raw_path"]))
            seed_raw_original = seed_raw_path.read_bytes()
            seed_raw_mode = stat.S_IMODE(seed_raw_path.stat().st_mode)
            context_originals: dict[Path, tuple[bytes, int]] = {}
            try:
                seed_raw = json.loads(seed_raw_original)
                seed_raw["logical_digest"] = logical_digest
                seed_raw["registry_head_digest"] = registry_head_digest
                seed_raw["log_events"] = log_events
                seed_raw_path.chmod(0o644)
                seed_raw_path.write_bytes(canonical_json_bytes(seed_raw))
                seed_raw_path.chmod(seed_raw_mode)
                children[seed_index]["raw_sha256"] = sha256_file(seed_raw_path)
                children[seed_index]["raw_bytes"] = seed_raw_path.stat().st_size
                raw_bindings[seed_index]["raw_sha256"] = children[seed_index][
                    "raw_sha256"
                ]
                raw_bindings[seed_index]["raw_bytes"] = children[seed_index][
                    "raw_bytes"
                ]

                for index, child in enumerate(children):
                    if child.get("kind") not in {
                        "smoke_reopen",
                        "smoke_structural_reopen",
                    }:
                        continue
                    child["context"]["expected_logical_digest"] = logical_digest
                    child["context"]["expected_registry_head_digest"] = (
                        registry_head_digest
                    )
                    child["context"]["expected_log_events"] = log_events
                    context_sha256 = hashlib.sha256(
                        canonical_json_bytes(child["context"])
                    ).hexdigest()
                    child["context_sha256"] = context_sha256
                    child["environment"][
                        "ASTERISM_REBASELINE_CONTEXT_SHA256"
                    ] = context_sha256
                    child["environment"]["ASTERISM_CONTEXT_SHA256"] = (
                        context_sha256
                    )
                    child["environment"]["ASTERISM_EXPECTED_LOGICAL_DIGEST"] = (
                        logical_digest
                    )
                    child["environment"][
                        "ASTERISM_EXPECTED_REGISTRY_HEAD_DIGEST"
                    ] = registry_head_digest
                    child["environment"]["ASTERISM_EXPECTED_LOG_EVENTS"] = str(
                        log_events
                    )
                    raw_bindings[index]["context_sha256"] = context_sha256
                    context_path = (
                        output / "contexts" / f"{index + 1:05d}.json"
                    )
                    context_originals[context_path] = (
                        context_path.read_bytes(),
                        stat.S_IMODE(context_path.stat().st_mode),
                    )
                    context_path.chmod(0o644)
                    context_path.write_bytes(
                        canonical_json_bytes(child["context"])
                    )
                    context_path.chmod(0o444)

                child_path.chmod(0o644)
                child_path.write_bytes(
                    b"".join(canonical_json_bytes(child) for child in children)
                )
                child_path.chmod(modes[child_path])
                raw_manifest_path.chmod(0o644)
                raw_manifest_path.write_bytes(
                    b"".join(
                        canonical_json_bytes(binding)
                        for binding in raw_bindings
                    )
                )
                raw_manifest_path.chmod(modes[raw_manifest_path])
                provenance = json.loads(originals[provenance_path])
                provenance["child_manifest_sha256"] = sha256_file(child_path)
                provenance["raw_manifest_sha256"] = sha256_file(
                    raw_manifest_path
                )
                provenance_path.chmod(0o644)
                provenance_path.write_bytes(canonical_json_bytes(provenance))
                provenance_path.chmod(modes[provenance_path])
                result, rc = evaluate_directory(
                    output, synthetic=True, publish=False
                )
                return (
                    rc == EXIT_INCONCLUSIVE
                    and not result["evidence_valid"]
                    and any(
                        "specialized smoke seed output differs from exact authority"
                        in error
                        for error in result["errors"]
                    )
                )
            finally:
                seed_raw_path.chmod(0o644)
                seed_raw_path.write_bytes(seed_raw_original)
                seed_raw_path.chmod(seed_raw_mode)
                for path, payload in originals.items():
                    path.chmod(0o644)
                    path.write_bytes(payload)
                    path.chmod(modes[path])
                for path, (payload, mode) in context_originals.items():
                    path.chmod(0o644)
                    path.write_bytes(payload)
                    path.chmod(mode)

        check(
            "mutation-specialized-seed-arbitrary-64hex-coordinated-rebind",
            lambda: coordinated_seed_observation_rebind("a" * 64, "b" * 64, 3),
        )
        check(
            "mutation-specialized-seed-legacy-16hex-coordinated-rebind",
            lambda: coordinated_seed_observation_rebind(
                "e2d874aa120f66af", "88201fb960ff6465", 3
            ),
        )
        check(
            "mutation-specialized-seed-old-log-count-coordinated-rebind",
            lambda: coordinated_seed_observation_rebind(
                str(SPECIALIZED_SEED_AUTHORITY["logical_digest"]),
                str(SPECIALIZED_SEED_AUTHORITY["registry_head_digest"]),
                1,
            ),
        )

        def coordinated_transition_rebinding_rejected() -> bool:
            paths = {
                name: output / filename
                for name, filename in {
                    "config": "config.json",
                    "children": "child-manifest.jsonl",
                    "raw": "raw-manifest.json",
                    "guards": "guard-manifest.jsonl",
                    "provenance": "provenance.json",
                }.items()
            }
            original = {name: path.read_bytes() for name, path in paths.items()}
            modes = {
                name: stat.S_IMODE(path.stat().st_mode)
                for name, path in paths.items()
            }
            snapshot_originals: dict[Path, bytes] = {}
            try:
                config_value = json.loads(original["config"])
                children = [
                    json.loads(line) for line in original["children"].splitlines()
                ]
                raw = [json.loads(line) for line in original["raw"].splitlines()]
                guards = [
                    json.loads(line) for line in original["guards"].splitlines()
                ]
                index = next(
                    index
                    for index, transition in enumerate(
                        config_value["smoke_transitions"]
                    )
                    if transition["id"] == "smoke_reopen_seed"
                )
                config_value["smoke_transitions"][index]["id"] = "forged-A"
                child = children[index]
                child["kind"] = "smoke"
                child["context"] = {
                    "transition": "smoke",
                    "smoke_target": "forged",
                    "variant": "A",
                }
                child["context_sha256"] = hashlib.sha256(
                    canonical_json_bytes(child["context"])
                ).hexdigest()
                raw[index]["kind"] = "smoke"
                raw[index]["context_sha256"] = child["context_sha256"]
                for guard_index, suffix in ((2 * index, "pre"), (2 * index + 1, "post")):
                    guard = guards[guard_index]
                    label = f"{index + 1:05d}-smoke-{suffix}"
                    guard["label"] = label
                    snapshot_path = Path(guard["path"])
                    snapshot_originals[snapshot_path] = snapshot_path.read_bytes()
                    snapshot = json.loads(snapshot_originals[snapshot_path])
                    snapshot["label"] = label
                    snapshot_path.chmod(0o644)
                    snapshot_path.write_bytes(canonical_json_bytes(snapshot))
                    snapshot_path.chmod(0o444)
                    guard["sha256"] = sha256_file(snapshot_path)

                rewritten = {
                    "config": canonical_json_bytes(config_value),
                    "children": b"".join(
                        canonical_json_bytes(item) for item in children
                    ),
                    "raw": b"".join(canonical_json_bytes(item) for item in raw),
                    "guards": b"".join(
                        canonical_json_bytes(item) for item in guards
                    ),
                }
                for name, payload in rewritten.items():
                    paths[name].chmod(0o644)
                    paths[name].write_bytes(payload)
                    paths[name].chmod(modes[name])
                provenance = json.loads(original["provenance"])
                provenance["config_sha256"] = sha256_file(paths["config"])
                provenance["child_manifest_sha256"] = sha256_file(
                    paths["children"]
                )
                provenance["raw_manifest_sha256"] = sha256_file(paths["raw"])
                provenance["guard_manifest_sha256"] = sha256_file(paths["guards"])
                provenance["host"]["frozen_files"][str(paths["config"])] = (
                    provenance["config_sha256"]
                )
                paths["provenance"].chmod(0o644)
                paths["provenance"].write_bytes(canonical_json_bytes(provenance))
                paths["provenance"].chmod(modes["provenance"])
                result, rc = evaluate_directory(
                    output, synthetic=True, publish=False
                )
                return (
                    rc == EXIT_INCONCLUSIVE
                    and not result["evidence_valid"]
                    and any(
                        "config smoke_transitions differ from reconstructed authority"
                        in error
                        for error in result["errors"]
                    )
                    and any(
                        "differs from reconstructed authority" in error
                        for error in result["errors"]
                    )
                )
            finally:
                for path, payload in snapshot_originals.items():
                    path.chmod(0o644)
                    path.write_bytes(payload)
                    path.chmod(0o444)
                for name, path in paths.items():
                    path.chmod(0o644)
                    path.write_bytes(original[name])
                    path.chmod(modes[name])

        check(
            "mutation-coordinated-config-child-raw-guard-provenance-transition-rebinding",
            coordinated_transition_rebinding_rejected,
        )
        check(
            "mutation-transition-authority-omission",
            lambda: mutate_rebound_config(
                lambda value: value["smoke_transitions"].pop(),
                ["config smoke_transitions differ from reconstructed authority"],
            ),
        )
        check(
            "mutation-transition-authority-reorder",
            lambda: mutate_rebound_config(
                lambda value: value["smoke_transitions"].__setitem__(
                    slice(0, 2),
                    list(reversed(value["smoke_transitions"][:2])),
                ),
                ["config smoke_transitions differ from reconstructed authority"],
            ),
        )
        check(
            "mutation-transition-authority-duplicate",
            lambda: mutate_rebound_config(
                lambda value: value["smoke_transitions"].append(
                    copy.deepcopy(value["smoke_transitions"][-1])
                ),
                ["config smoke_transitions differ from reconstructed authority"],
            ),
        )
        check(
            "mutation-coordinated-seed-cell-order-rebinding",
            lambda: mutate_rebound_config(
                lambda value: (
                    value.__setitem__("seed_sha256", "0" * 64),
                    value["cell_orders"]["primary"].reverse(),
                ),
                [
                    "config seed differs from original source approval authority",
                    "config cell orders differ from reconstructed runner authority",
                ],
            ),
        )

        check(
            "mutation-guard-foreign-process",
            lambda: mutate_jsonl(output / "guard-manifest.jsonl", 0, lambda value: value.__setitem__("unexplained", [{"pid": 9}])),
        )
        check(
            "mutation-contract-transition-id-rebound",
            lambda: mutate_rebound_child(
                lambda child: child.get("kind") == "contract",
                lambda child: (
                    child["context"].__setitem__("variant", "B"),
                    child.__setitem__(
                        "context_sha256",
                        hashlib.sha256(
                            canonical_json_bytes(child["context"])
                        ).hexdigest(),
                    ),
                ),
                "transition child 1 differs from reconstructed authority",
            ),
        )

        for specialized_kind in schema.SPECIALIZED_SMOKE_TRANSITION_KINDS:
            check(
                f"mutation-{specialized_kind}-transition-omitted",
                lambda specialized_kind=specialized_kind: mutate_rebound_children(
                    lambda records: records.pop(
                        next(
                            index
                            for index, child in enumerate(records)
                            if child.get("kind") == specialized_kind
                        )
                    ),
                    "transition child count differs from reconstructed authority",
                ),
            )
            check(
                f"mutation-{specialized_kind}-transition-relabeled",
                lambda specialized_kind=specialized_kind: mutate_rebound_child(
                    lambda child: child.get("kind") == specialized_kind,
                    lambda child: child.__setitem__("kind", "smoke"),
                    "differs from reconstructed authority",
                ),
            )

        def mutate_ordinary_smoke_profile_inputs(child: dict[str, Any]) -> None:
            child["profile_tool_inputs"] = {"schedstat_resolution_ns": 1}
            child["profile_tool_inputs_sha256"] = hashlib.sha256(
                canonical_json_bytes(child["profile_tool_inputs"])
            ).hexdigest()

        check(
            "mutation-ordinary-smoke-target-is-not-profile-authority",
            lambda: mutate_rebound_child(
                lambda child: child.get("kind") == "smoke"
                and child.get("context", {}).get("smoke_target") == "primary",
                mutate_ordinary_smoke_profile_inputs,
                "non-profile child has profile tool inputs",
            ),
        )
        check(
            "mutation-child-combined-hash",
            lambda: mutate_jsonl(output / "child-manifest.jsonl", 0, lambda value: value.__setitem__("combined_row_sha256", "0" * 64)),
        )
        row_predicate = lambda child: child.get("kind") == "primary"
        trace_predicate = lambda child: child.get("kind") == "structural_traces"
        check(
            "mutation-row-environment-mode",
            lambda: mutate_child_environment(
                row_predicate,
                lambda environment: environment.__setitem__(
                    "ASTERISM_REBASELINE_MODE", "new_names"
                ),
            ),
        )
        check(
            "mutation-row-environment-variant",
            lambda: mutate_child_environment(
                row_predicate,
                lambda environment: environment.__setitem__(
                    "ASTERISM_REBASELINE_VARIANT", "D"
                ),
            ),
        )
        check(
            "mutation-row-environment-trace-marker",
            lambda: mutate_child_environment(
                trace_predicate,
                lambda environment: environment.__setitem__(
                    "ASTERISM_REBASELINE_LOG_PATH_MARKERS", "[]"
                ),
            ),
        )
        check(
            "mutation-row-environment-cell-variable",
            lambda: mutate_child_environment(
                row_predicate,
                lambda environment: environment.__setitem__(
                    "ASTERISM_PAYLOAD_BYTES", "999"
                ),
            ),
        )
        check(
            "mutation-profile-tool-input-hash-rebound",
            lambda: mutate_rebound_child(
                lambda child: child.get("kind") == "cpu_profiles",
                lambda child: child.__setitem__(
                    "profile_tool_inputs_sha256", "0" * 64
                ),
                "profile tool input hash mismatch",
            ),
        )
        check(
            "mutation-profile-authority-adapter-hash-rebound",
            lambda: mutate_rebound_child(
                lambda child: child.get("kind") == "primary",
                lambda child: child["profile_rich_result"]["authority"].__setitem__(
                    "profile_adapter_sha256", "0" * 64
                ),
                "profile authority profile_adapter_sha256 differs",
            ),
        )
        check(
            "mutation-perf-environment-partial-fd-rebound",
            lambda: mutate_rebound_child(
                lambda child: child.get("kind") == "cpu_profiles",
                lambda child: child["environment"].__setitem__(
                    "ASTERISM_REBASELINE_PERF_COMMAND_FD", "999"
                ),
                "environment is not the exact frozen map",
            ),
        )
        check(
            "mutation-trace-input-context-marker-rebound",
            lambda: mutate_rebound_child(
                lambda child: child.get("kind") == "syscall_profiles",
                lambda child: (
                    child["profile_tool_inputs"]["log_path_markers"][0].__setitem__(
                        "path", "/unapproved/log/"
                    ),
                    child.__setitem__(
                        "profile_tool_inputs_sha256",
                        hashlib.sha256(
                            canonical_json_bytes(child["profile_tool_inputs"])
                        ).hexdigest(),
                    ),
                ),
                "trace marker input/context authority differs",
            ),
        )

        def mutate_trace_raw_with_rebound_binding() -> bool:
            child_path = output / "child-manifest.jsonl"
            records = [json.loads(line) for line in child_path.read_bytes().splitlines()]
            child = next(
                record for record in records if record.get("kind") == "syscall_profiles"
            )
            binding = child["profile_tool_inputs"]["trace_raw_artifact"]
            artifact_path = Path(binding["path"])
            original_artifact = artifact_path.read_bytes()
            artifact_mode = stat.S_IMODE(artifact_path.stat().st_mode)
            try:
                mutated_result = dict(child["profile_result"])
                mutated_result["write"] += 1
                artifact_path.chmod(0o644)
                artifact_path.write_bytes(canonical_json_bytes(mutated_result))
                artifact_path.chmod(artifact_mode)

                def mutate(record: dict[str, Any]) -> None:
                    rebound = record["profile_tool_inputs"]["trace_raw_artifact"]
                    rebound["sha256"] = sha256_file(artifact_path)
                    rebound["bytes"] = artifact_path.stat().st_size
                    record["profile_tool_inputs_sha256"] = hashlib.sha256(
                        canonical_json_bytes(record["profile_tool_inputs"])
                    ).hexdigest()

                return mutate_rebound_child(
                    lambda record: record.get("ordinal") == child["ordinal"],
                    mutate,
                    "profile result differs from raw replay",
                )
            finally:
                artifact_path.chmod(0o644)
                artifact_path.write_bytes(original_artifact)
                artifact_path.chmod(artifact_mode)

        check(
            "mutation-trace-raw-rebound-still-replayed",
            mutate_trace_raw_with_rebound_binding,
        )

        def mutate_trace_raw_path_alias(record: dict[str, Any]) -> None:
            binding = record["profile_tool_inputs"]["trace_raw_artifact"]
            path = Path(binding["path"])
            binding["path"] = str(path.parent) + "/./" + path.name
            record["profile_tool_inputs_sha256"] = hashlib.sha256(
                canonical_json_bytes(record["profile_tool_inputs"])
            ).hexdigest()

        check(
            "mutation-trace-raw-noncanonical-path-rebound",
            lambda: mutate_rebound_child(
                lambda record: record.get("kind") == "syscall_profiles",
                mutate_trace_raw_path_alias,
                "strace raw path is not canonical absolute",
            ),
        )

        def mutate_csv(path: Path, mutator: Callable[[list[bytes]], None]) -> bool:
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                lines = original.splitlines(keepends=True)
                mutator(lines)
                path.chmod(0o644)
                path.write_bytes(b"".join(lines))
                path.chmod(mode)
                result, rc = evaluate_directory(output, synthetic=True, publish=False)
                return rc == EXIT_INCONCLUSIVE and not result["evidence_valid"]
            finally:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)

        check(
            "mutation-primary-cardinality",
            lambda: mutate_csv(output / "primary.csv", lambda lines: lines.pop()),
        )
        check(
            "mutation-reopen-physical-order",
            lambda: mutate_csv(output / "reopen.csv", lambda lines: lines.__setitem__(slice(1, 3), [lines[2], lines[1]])),
        )
        check(
            "mutation-fairness-warm-policy",
            lambda: mutate_csv(
                output / "fairness.csv",
                lambda lines: lines.__setitem__(1, lines[1].replace(b",4,64,true,true,true,", b",3,64,true,true,true,", 1)),
            ),
        )

        def expose_unavailable_fairness_diagnostic(lines: list[bytes]) -> None:
            table = list(csv.reader(io.StringIO(b"".join(lines).decode("ascii"))))
            column = table[0].index("adaptive_group_width_target")
            table[1][column] = "1"
            buffer = io.StringIO(newline="")
            writer = csv.writer(buffer, lineterminator="\n")
            writer.writerows(table)
            lines[:] = [buffer.getvalue().encode("ascii")]

        check(
            "mutation-fairness-unavailable-diagnostic",
            lambda: mutate_csv(
                output / "fairness.csv",
                expose_unavailable_fairness_diagnostic,
            ),
        )

        def invalidate_reopen_handshake(lines: list[bytes]) -> None:
            table = list(csv.reader(io.StringIO(b"".join(lines).decode("ascii"))))
            header = table[0]
            ready = header.index("ready_monotonic_ns")
            start_sent = header.index("start_sent_monotonic_ns")
            table[1][ready] = str(int(table[1][start_sent]) + 1)
            buffer = io.StringIO(newline="")
            writer = csv.writer(buffer, lineterminator="\n")
            writer.writerows(table)
            lines[:] = [buffer.getvalue().encode("ascii")]

        check(
            "mutation-reopen-handshake",
            lambda: mutate_csv(
                output / "reopen.csv",
                invalidate_reopen_handshake,
            ),
        )

        def correctness_mutation(
            mutator: Callable[
                [list[dict[str, Any]], dict[tuple[str, str, str, str], dict[str, Any]], dict[str, Any]],
                None,
            ]
        ) -> bool:
            aggregate = json.loads((output / "correctness.json").read_bytes())
            child_records = read_jsonl(
                output / "child-manifest.jsonl", "self-test child manifest", Problems()
            )
            raw_records: dict[tuple[str, str, str, str], dict[str, Any]] = {}
            originals: dict[Path, tuple[bytes, int]] = {}
            for child in child_records:
                if child.get("kind") not in {"correctness", "fault"}:
                    continue
                path = Path(child["raw_path"])
                original = path.read_bytes()
                originals[path] = (original, stat.S_IMODE(path.stat().st_mode))
                record = json.loads(original)
                raw_records[
                    (
                        record["variant"], record["phase"], record["suite"],
                        child["kind"],
                    )
                ] = record
            try:
                mutator(child_records, raw_records, aggregate)
                for identity, record in raw_records.items():
                    child = next(
                        item
                        for item in child_records
                        if item.get("raw_path") in {str(path) for path in originals}
                        and json.loads(Path(item["raw_path"]).read_bytes()).get("variant") == identity[0]
                        and json.loads(Path(item["raw_path"]).read_bytes()).get("phase") == identity[1]
                        and json.loads(Path(item["raw_path"]).read_bytes()).get("suite") == identity[2]
                    )
                    path = Path(child["raw_path"])
                    path.chmod(0o644)
                    path.write_bytes(canonical_json_bytes(record))
                    path.chmod(originals[path][1])
                mutation_problems = Problems()
                current, historical = validate_correctness(
                    aggregate,
                    json.loads((output / "config.json").read_bytes()),
                    json.loads((output / "prepared-artifacts.json").read_bytes()),
                    child_records,
                    json.loads((output / "config.json").read_bytes())["attempt_nonce"],
                    output,
                    output.parent,
                    mutation_problems,
                    require_matrix=True,
                )
                return bool(mutation_problems.errors or current or historical)
            finally:
                for path, (original, mode) in originals.items():
                    path.chmod(0o644)
                    path.write_bytes(original)
                    path.chmod(mode)

        def wrong_correctness_partition(
            children: list[dict[str, Any]],
            _: dict[tuple[str, str, str, str], dict[str, Any]],
            __: dict[str, Any],
        ) -> None:
            child = next(
                item
                for item in children
                if item.get("kind") == "fault"
                and item.get("context", {}).get("phase") == "pre"
            )
            child["kind"] = "correctness"

        def duplicate_correctness_case(
            _: list[dict[str, Any]],
            records: dict[tuple[str, str, str, str], dict[str, Any]],
            __: dict[str, Any],
        ) -> None:
            cases = records[("A", "pre", "current-product", "correctness")]["cases"]
            cases[1] = copy.deepcopy(cases[0])

        def missing_correctness_case(
            _: list[dict[str, Any]],
            records: dict[tuple[str, str, str, str], dict[str, Any]],
            __: dict[str, Any],
        ) -> None:
            records[("A", "pre", "current-fault", "fault")]["cases"].pop()

        def false_harness(
            _: list[dict[str, Any]],
            records: dict[tuple[str, str, str, str], dict[str, Any]],
            __: dict[str, Any],
        ) -> None:
            records[("C", "oracle", "common-public-oracle", "correctness")]["harness_sound"] = False

        def bounds_mismatch(
            _: list[dict[str, Any]],
            records: dict[tuple[str, str, str, str], dict[str, Any]],
            __: dict[str, Any],
        ) -> None:
            records[("A", "post", "current-fault", "fault")]["boundedness"]["owner_ring_intents"] = 1023

        def historical_oracle_mislabeled_pre(
            _: list[dict[str, Any]],
            records: dict[tuple[str, str, str, str], dict[str, Any]],
            __: dict[str, Any],
        ) -> None:
            records[("C", "oracle", "common-public-oracle", "correctness")]["phase"] = "pre"

        def executable_authority_tail(
            children: list[dict[str, Any]],
            _: dict[tuple[str, str, str, str], dict[str, Any]],
            __: dict[str, Any],
        ) -> None:
            child = next(item for item in children if item.get("kind") == "correctness")
            child["argv"].append("--unapproved-tail")

        def correctness_environment_extra(
            children: list[dict[str, Any]],
            _: dict[tuple[str, str, str, str], dict[str, Any]],
            __: dict[str, Any],
        ) -> None:
            child = next(item for item in children if item.get("kind") == "correctness")
            child["environment"]["UNAPPROVED"] = "1"

        def correctness_environment_missing(
            children: list[dict[str, Any]],
            _: dict[tuple[str, str, str, str], dict[str, Any]],
            __: dict[str, Any],
        ) -> None:
            child = next(item for item in children if item.get("kind") == "correctness")
            child["environment"].pop("ASTERISM_REBASELINE_MODE")

        def correctness_environment_changed(
            children: list[dict[str, Any]],
            _: dict[tuple[str, str, str, str], dict[str, Any]],
            __: dict[str, Any],
        ) -> None:
            child = next(item for item in children if item.get("kind") == "correctness")
            child["environment"]["ASTERISM_REBASELINE_VARIANT"] = "B"

        for name, mutation in (
            ("correctness-wrong-kind-partition", wrong_correctness_partition),
            ("correctness-duplicate-case", duplicate_correctness_case),
            ("correctness-missing-case", missing_correctness_case),
            ("correctness-false-harness", false_harness),
            ("correctness-bounds-mismatch", bounds_mismatch),
            ("correctness-historical-oracle-mislabeled-pre", historical_oracle_mislabeled_pre),
            ("correctness-executable-authority-tail", executable_authority_tail),
            ("correctness-environment-extra", correctness_environment_extra),
            ("correctness-environment-missing", correctness_environment_missing),
            ("correctness-environment-changed", correctness_environment_changed),
        ):
            check(f"mutation-{name}", lambda mutation=mutation: correctness_mutation(mutation))

        # Outcome mapping is tested on valid, already parsed synthetic rows so
        # evidence mutations can never be misreported as performance declines.
        config = json.loads((output / "config.json").read_bytes())
        parsed_problems = Problems()
        tracks = read_csv_tracks(
            output,
            config,
            config["protocol_sha256"],
            config["attempt_nonce"],
            parsed_problems,
        )
        if parsed_problems.errors:
            checks.append({"name": "outcome-fixture-parse", "pass": False, "detail": repr(parsed_problems.errors[:10])})
        else:
            check(
                "outcome-current-correctness-is-revert",
                lambda: evaluate_gates(copy.deepcopy(tracks), ["fault"], [], 1)[2] == "REVERT",
            )
            check(
                "outcome-historical-oracle-is-inconclusive",
                lambda: evaluate_gates(copy.deepcopy(tracks), [], ["C:oracle"], 1)[2] == "INCONCLUSIVE",
            )

            def process_regression() -> bool:
                mutated = copy.deepcopy(tracks)
                for row in mutated["primary"]:
                    if row["variant"] == "A" and row["durability"] == "Process" and row["payload_size"] == 24 and row["batch_size"] == 1 and row["writers"] == 1:
                        row["wall_ns"] *= 2
                return evaluate_gates(mutated, [], [], 1)[2] == "REVERT"

            check("outcome-A-D-process-regression-is-revert", process_regression)

            def fjall_material_miss() -> bool:
                mutated = copy.deepcopy(tracks)
                for row in mutated["primary"]:
                    if row["variant"] == "C" and row["durability"] == "Process":
                        row["wall_ns"] = 900_000_000
                return evaluate_gates(mutated, [], [], 1)[2] == "NARROW"

            check("outcome-A-C-material-miss-is-narrow", fjall_material_miss)

            def group_miss(confounded: bool) -> str:
                mutated = copy.deepcopy(tracks)
                for row in mutated["primary"]:
                    if row["variant"] == "A" and row["durability"] == "Group" and row["payload_size"] == 24 and row["batch_size"] == 1 and row["writers"] == 1:
                        row["wall_ns"] *= 2
                        if confounded:
                            row["fsync_total_ns"] *= 2
                            row["fsync_p99_ns"] *= 2
                return evaluate_gates(mutated, [], [], 1)[2]

            check("outcome-group-literal-code-miss-is-narrow", lambda: group_miss(False) == "NARROW")
            check("outcome-group-device-confound-is-inconclusive", lambda: group_miss(True) == "INCONCLUSIVE")

            def fairness_miss() -> bool:
                mutated = copy.deepcopy(tracks)
                mutated["fairness"][0]["jain_ppb"] = 980_000_000
                return evaluate_gates(mutated, [], [], 1)[2] == "NARROW"

            check("outcome-fairness-miss-is-narrow", fairness_miss)

            def reservation_leak_reverts() -> bool:
                mutated = copy.deepcopy(tracks)
                row = next(
                    item
                    for item in mutated["fairness"]
                    if item["variant"] == "A"
                )
                row["waiter_reservations_after"] = 1
                schema.validate_child_record(
                    "fairness", row, "self-test structurally valid reservation leak"
                )
                gates, _, outcome = evaluate_gates(mutated, [], [], 1)
                reservation_gates = [
                    gate
                    for gate in gates
                    if gate["id"].endswith("quiescent-reservations")
                    and gate["pass"] is False
                ]
                return (
                    outcome == "REVERT"
                    and len(reservation_gates) == 1
                    and reservation_gates[0]["failure_outcome"] == "REVERT"
                )

            check("outcome-A-reservation-leak-is-revert", reservation_leak_reverts)

            def reopen_digest_miss() -> bool:
                mutated = copy.deepcopy(tracks)
                mutated["reopen"][0]["logical_digest"] = "f" * 64
                return evaluate_gates(mutated, [], [], 1)[2] == "REVERT"

            check("outcome-reopen-digest-mismatch-is-revert", reopen_digest_miss)

        def publication_is_canonical() -> bool:
            published, code = evaluate_directory(output, synthetic=True, publish=True)
            return (
                code == EXIT_ADMIT
                and (output / RESULT_NAME).read_bytes() == canonical_json_bytes(published)
                and (output / "REPORT.md").read_bytes() == render_report(published)
                and b"Fjall-era production engine" in (output / "REPORT.md").read_bytes()
                and b"not pure engine overhead" in (output / "REPORT.md").read_bytes()
            )

        check("publication-result-and-report-canonical", publication_is_canonical)

    first_case, second_case, third_case = schema.CORRECTNESS_CASE_IDS[:3]
    early_scenarios = (
        ("identical", (first_case,), (first_case,), False, "REVERT", True),
        ("pre-only", (first_case,), (), False, "INCONCLUSIVE", True),
        ("disjoint", (first_case,), (second_case,), False, "INCONCLUSIVE", True),
        (
            "post-subset",
            (first_case, second_case),
            (first_case,),
            False,
            "INCONCLUSIVE",
            True,
        ),
        (
            "post-superset",
            (first_case,),
            (first_case, second_case),
            False,
            "INCONCLUSIVE",
            True,
        ),
        (
            "partial-overlap",
            (first_case, second_case),
            (first_case, third_case),
            False,
            "INCONCLUSIVE",
            True,
        ),
        ("historical-empty-current", (), (), True, "INCONCLUSIVE", True),
        (
            "historical-with-identical-current",
            (first_case,),
            (first_case,),
            True,
            "INCONCLUSIVE",
            True,
        ),
    )
    with tempfile.TemporaryDirectory(
        prefix="bn-2l3n-correctness-only-selftest-"
    ) as early_temp:
        early_root = Path(early_temp)
        for name, pre, post, historical, expected, expected_valid in early_scenarios:
            scenario_root = early_root / name
            scenario_root.mkdir()
            scenario_output = build_synthetic_fixture(
                scenario_root,
                correctness_only=True,
                pre_failures=pre,
                post_failures=post,
                historical_failure=historical,
            )
            result, code = evaluate_directory(
                scenario_output,
                synthetic=True,
                publish=False,
                correctness_only=True,
            )
            children = read_jsonl(
                scenario_output / "child-manifest.jsonl",
                f"self-test correctness-only {name}",
                Problems(),
            )
            zero_timing = (
                not any(
                    child.get("kind") in schema.TRACK_EXECUTION_ORDER
                    for child in children
                )
                and not any(
                    (scenario_output / filename).exists()
                    for filename in schema.CSV_FILENAMES.values()
                )
            )
            expected_code = EXIT_REVERT if expected == "REVERT" else EXIT_INCONCLUSIVE
            checks.append(
                {
                    "name": f"correctness-only-{name}",
                    "pass": (
                        result["outcome"] == expected
                        and code == expected_code
                        and result["evidence_valid"] is expected_valid
                        and result["matrix_complete"] is False
                        and result["evidence_mode"] == "correctness-only"
                        and zero_timing
                    ),
                    "detail": json.dumps(result["errors"][:20]),
                }
            )

        mutation_root = early_root / "marker-mutation"
        mutation_root.mkdir()
        mutation_output = build_synthetic_fixture(
            mutation_root,
            correctness_only=True,
            pre_failures=(first_case,),
            post_failures=(first_case,),
        )
        marker_path = mutation_output / "correctness-only.json"
        marker_value = json.loads(marker_path.read_bytes())
        marker_value["trigger"] = "historical"
        marker_path.chmod(0o644)
        marker_path.write_bytes(canonical_json_bytes(marker_value))
        marker_path.chmod(0o444)
        mutated, mutated_code = evaluate_directory(
            mutation_output,
            synthetic=True,
            publish=False,
            correctness_only=True,
        )
        checks.append(
            {
                "name": "correctness-only-marker-mutation-rejected",
                "pass": (
                    mutated_code == EXIT_INCONCLUSIVE
                    and mutated["evidence_valid"] is False
                ),
                "detail": json.dumps(mutated["errors"][:20]),
            }
        )

        csv_root = early_root / "csv-mutation"
        csv_root.mkdir()
        csv_output = build_synthetic_fixture(
            csv_root,
            correctness_only=True,
            pre_failures=(first_case,),
            post_failures=(first_case,),
        )
        fixture_write_file(csv_output / schema.CSV_FILENAMES["primary"], b"injected\n")
        mutated, mutated_code = evaluate_directory(
            csv_output,
            synthetic=True,
            publish=False,
            correctness_only=True,
        )
        checks.append(
            {
                "name": "correctness-only-timing-csv-rejected",
                "pass": (
                    mutated_code == EXIT_INCONCLUSIVE
                    and mutated["evidence_valid"] is False
                ),
                "detail": json.dumps(mutated["errors"][:20]),
            }
        )

    passed = all(item["pass"] for item in checks)
    return {
        "schema": "bn-2l3n-evaluator-self-test-v3",
        "protocol": schema.PROTOCOL,
        "outcome": "SELF_TEST_PASS" if passed else "SELF_TEST_FAIL",
        "checks": checks,
    }


def usage() -> int:
    print(
        f"usage: {Path(sys.argv[0]).name} --smoke | --self-test | "
        "--evaluate OUTPUT_DIR | --evaluate-correctness-only OUTPUT_DIR",
        file=sys.stderr,
    )
    return EXIT_USAGE


def main() -> int:
    if sys.argv[1:] == ["--smoke"]:
        sys.stdout.buffer.write(canonical_json_bytes({
            "schema": "bn-2l3n-evaluator-smoke-v3",
            "protocol": schema.PROTOCOL,
            "status": "PASS",
        }))
        return 0
    if sys.argv[1:] == ["--self-test"]:
        result = self_test()
        sys.stdout.buffer.write(canonical_json_bytes(result))
        return 0 if result["outcome"] == "SELF_TEST_PASS" else EXIT_INTERNAL
    if len(sys.argv) == 3 and sys.argv[1] == "--evaluate":
        result, exit_code = evaluate_directory(Path(sys.argv[2]), synthetic=False, publish=True)
        sys.stdout.buffer.write(canonical_json_bytes(result))
        return exit_code
    if len(sys.argv) == 3 and sys.argv[1] == "--evaluate-correctness-only":
        result, exit_code = evaluate_directory(
            Path(sys.argv[2]),
            synthetic=False,
            publish=True,
            correctness_only=True,
        )
        sys.stdout.buffer.write(canonical_json_bytes(result))
        return exit_code
    return usage()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as error:
        print(f"internal evaluator failure: {error!r}", file=sys.stderr)
        raise SystemExit(EXIT_INTERNAL)
