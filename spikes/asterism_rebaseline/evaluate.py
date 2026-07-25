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
import re
import shutil
import socket
import stat
import sys
import tarfile
import tempfile
import types
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from fractions import Fraction
from pathlib import Path, PurePosixPath
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
PREPARED_ATTESTATION_FIELDS = set(schema.PREPARED_ATTESTATION_FIELDS) | {
    "semantic_input_authority"
}
RELEASE_ORDINARY_ATTESTATION_FIELDS = set(
    schema.RELEASE_COMPILE_OUT_ORDINARY_ATTESTATION_FIELDS
) | {"semantic_input_authority"}
RELEASE_OVERLAY_ATTESTATION_FIELDS = set(
    schema.RELEASE_COMPILE_OUT_OVERLAY_ATTESTATION_FIELDS
) | {"semantic_input_authority"}
SOURCE_REVIEW_FIELDS = set(schema.SOURCE_REVIEW_FIELDS)
SOURCE_REVIEW_INPUT_FIELDS = set(schema.SOURCE_REVIEW_INPUT_FIELDS)
SOURCE_REVIEW_ASSERTION_FIELDS = set(schema.SOURCE_REVIEW_ASSERTION_FIELDS)
SOURCE_REVIEW_BUNDLE_FIELDS = set(schema.SOURCE_REVIEW_BUNDLE_FIELDS)
RELEASE_COMPILE_OUT_REQUIREMENT_FIELDS = set(
    schema.RELEASE_COMPILE_OUT_REQUIREMENT_FIELDS
)
RELEASE_COMPILE_OUT_FIELDS = set(schema.RELEASE_COMPILE_OUT_FIELDS)
GUEST_ROOT = "/asterism"
GUEST_SOURCE = f"{GUEST_ROOT}/source"
GUEST_TARGET = f"{GUEST_ROOT}/target"
GUEST_TOOLCHAIN_ROOT = f"{GUEST_ROOT}/toolchain"
GUEST_TOOLCHAIN_BIN = f"{GUEST_TOOLCHAIN_ROOT}/bin"
GUEST_CARGO = f"{GUEST_TOOLCHAIN_ROOT}/bin/cargo"
GUEST_RUSTC = f"{GUEST_TOOLCHAIN_ROOT}/bin/rustc"
GUEST_CARGO_HOME = f"{GUEST_ROOT}/cargo-home"
GUEST_RUSTUP_HOME = "/nonexistent"
GUEST_BOUND_CONFIG_PATHS = (
    f"{GUEST_SOURCE}/.cargo/config.toml",
    f"{GUEST_SOURCE}/.cargo/config",
    f"{GUEST_CARGO_HOME}/config.toml",
    f"{GUEST_CARGO_HOME}/config",
)
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
SEMANTIC_INPUT_AUTHORITY_SCHEMA = "bn-ecm1-semantic-input-authority-v1"
CURRENT_CHILDREN_SCHEMA = "bn-ecm1-current-children-build-v2"
RECURSIVE_TREE_AUTHORITY_SCHEMA = "bn-ecm1-recursive-tree-authority-v1"
TRUSTED_SYSTEM_CLOSURE_SCHEMA = "bn-ecm1-trusted-system-closure-v1"
TRUSTED_SYSTEM_MOUNTS = (
    (Path("/usr/bin"), "/usr/bin"),
    (Path("/usr/lib"), "/usr/lib"),
    (Path("/usr/include"), "/usr/include"),
)
SEMANTIC_INPUT_AUTHORITY_FIELDS = {
    "cargo_home",
    "runtime_sha256",
    "schema",
    "source",
    "toolchain",
    "trusted_system_closure",
}
SEMANTIC_TREE_BINDING_FIELDS = {
    "entry_count",
    "equal_pre_post",
    "manifest_path",
    "manifest_sha256",
    "mutation_events_absent",
    "role",
    "schema",
    "watch_count",
}
SEMANTIC_CLOSURE_FIELDS = {
    "entry_count",
    "manifest_path",
    "mounts",
    "mutation_events_absent",
    "schema",
    "sha256",
    "watch_count",
}
SEMANTIC_CLOSURE_MOUNT_FIELDS = {
    "device",
    "gid",
    "guest_path",
    "host_path",
    "inode",
    "permissions",
    "resolved_path",
    "trusted_root_owned_non_writable",
    "uid",
}
SEMANTIC_MANIFEST_ENTRY_FIELDS = {
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
}
SEMANTIC_RESOLUTION_FIELDS = {
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
}
TOOLCHAIN_FIELDS = {
    "bwrap_path", "bwrap_sha256", "cargo_home_path", "cargo_path",
    "cargo_sha256", "cargo_version_verbose", "git_path", "git_sha256",
    "rustc_path", "rustc_sha256", "rustc_version_verbose", "rustc_host",
    "rust_lld_path", "rust_lld_sha256",
    "rustup_home_path", "rustup_path", "rustup_sha256", "rustup_toolchain",
}
CURRENT_BUILD_FIELDS = {
    "argv", "environment", "execution", "filesystem_admission",
    "cargo_config_prebuild", "cargo_config_postbuild", "execution_tools",
    "artifacts", "binds", "lock_prebuild", "lock_postbuild",
    "source_manifest_sha256", "semantic_input_authority",
    "toolchain_manifest", "target", "target_was_absent",
}
CURRENT_CHILD_BUILD_FIELDS = CURRENT_BUILD_FIELDS | {
    "wrapper_receipt", "wrapper_receipt_identity", "wrapper_receipt_sha256",
    "wrapper_input_identity",
}
CURRENT_EXECUTION_FIELDS = {
    "argv", "cwd", "environment", "execution_authority", "exit_status",
    "passed_file_descriptors", "stderr_bytes", "stderr_sha256",
    "stdout_bytes", "stdout_sha256",
}
CURRENT_FILE_IDENTITY_FIELDS = {
    "bytes", "ctime_ns", "device", "inode", "link_count", "mode",
    "mtime_ns", "path", "sha256", "size",
}
CURRENT_DIRECTORY_IDENTITY_FIELDS = {
    "changed_ns", "device", "file_type", "inode", "link_count",
    "modified_ns", "path", "permissions", "size",
}
CURRENT_TOOL_RECORD_FIELDS = {"identity", "path_chain", "trusted_system"}
CURRENT_DEVICE_RECORD_FIELDS = {
    "identity", "parent_path_chain", "trusted_system",
}
CURRENT_DEVICE_IDENTITY_FIELDS = {
    "changed_ns", "device", "gid", "inode", "link_count", "major", "minor",
    "modified_ns", "path", "permissions", "size", "type", "uid",
}
CURRENT_TRUSTED_CHAIN_FIELDS = {
    "changed_ns", "device", "gid", "inode", "link_count", "mode",
    "modified_ns", "path", "size", "type", "uid",
}
CURRENT_IMMUTABLE_FILE_FIELDS = {"identity", "mode", "path", "sha256", "size"}
CURRENT_IMMUTABLE_IDENTITY_FIELDS = {
    "changed_ns", "device", "inode", "link_count", "modified_ns",
}
CURRENT_CARGO_CONFIG_FIELDS = {
    "cargo_home_tree", "cargo_search", "preserved_top_level_entries", "schema",
}
CURRENT_CARGO_HOME_TREE_FIELDS = {
    "entry_count", "equal_pre_post", "path", "post_sha256", "pre_sha256",
    "watch_count",
}
CURRENT_BUILD_BASE_ENV_FIELDS = {
    "CARGO_HOME", "CARGO_INCREMENTAL", "CARGO_NET_OFFLINE",
    "GIT_CONFIG_COUNT", "GIT_CONFIG_GLOBAL", "GIT_CONFIG_NOSYSTEM", "HOME",
    "LANG", "LC_ALL", "LD_ORIGIN_PATH", "PATH", "PYTHONDONTWRITEBYTECODE",
    "PYTHONNOUSERSITE", "RUSTC", "RUSTUP_HOME", "RUSTUP_TOOLCHAIN", "TZ",
}
CURRENT_RELEASE_ENV_FIELDS = CURRENT_BUILD_BASE_ENV_FIELDS | {
    "ASTERISM_BUILD_ADAPTER_SHA256", "ASTERISM_BUILD_BINARY_KIND",
    "ASTERISM_BUILD_NONCE", "ASTERISM_BUILD_CARGO_LOCK_SHA256",
    "ASTERISM_BUILD_PRODUCT_COMMIT", "ASTERISM_BUILD_PRODUCT_TREE",
    "ASTERISM_BUILD_PROTOCOL", "ASTERISM_BUILD_PROTOCOL_SHA256",
    "ASTERISM_BUILD_SHARED_MANIFEST_SHA256",
    "ASTERISM_BUILD_SOURCE_APPROVAL_SHA256", "ASTERISM_BUILD_TIMED_SURFACE",
    "ASTERISM_BUILD_TOOLING_COMMIT", "ASTERISM_BUILD_TOOLING_TREE",
    "ASTERISM_BUILD_VARIANT",
}
CURRENT_CHILD_ENV_FIELDS = CURRENT_BUILD_BASE_ENV_FIELDS | {
    "ASTERISM_FAULT_COMPILE_OUT_IDENTICAL",
    "ASTERISM_FAULT_COMPILE_OUT_OVERLAY_RELEASE_SHA256",
    "ASTERISM_FAULT_COMPILE_OUT_PRISTINE_SHA256",
    "ASTERISM_FAULT_COMPILE_OUT_SCHEMA",
    "ASTERISM_FAULT_COMPILE_OUT_SYMBOL_ABSENCE_SHA256",
    "ASTERISM_REBASELINE_CHILD_BUILD_NONCE",
    "ASTERISM_REBASELINE_EXPECTED_LIB_SOURCE",
    "ASTERISM_REBASELINE_PINNED_RUSTC",
    "ASTERISM_REBASELINE_WRAPPER_RECEIPT", "RUSTC_WORKSPACE_WRAPPER",
}
CURRENT_CHILDREN_FIELDS = {
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
}
CURRENT_SYSTEM_PYTHON = Path("/usr/bin/python3").resolve(strict=True)
CURRENT_ADAPTER_DESTINATION = Path(
    "crates/mess-store/examples/asterism_rebaseline_adapter.rs"
)
CURRENT_ENGINE_PATH = Path("crates/mess-store/src/engine.rs")
CURRENT_PRODUCT_ENGINE_SHA256 = (
    "c995c27d8fff3e1ddfffdb700dfc94160a99ea0c7fe731017d3f1db99d7b59e7"
)
CURRENT_SHARED_DESTINATION = Path(
    "crates/mess-store/examples/asterism_rebaseline_shared"
)
CURRENT_SHARED_NAMES = (
    "allocation.rs", "contract.rs", "control.rs", "digest.rs", "schema.rs",
    "semantic_oracle.rs", "timing.rs", "workload.rs",
)
CURRENT_CONSTRUCTION_SCHEMA = "bn-30fs-current-children-construction-v1"
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


def sandboxed_cargo_environment(
    toolchain: Mapping[str, Any], extra: Mapping[str, Any]
) -> dict[str, Any]:
    """Project the exact fixed-guest environment used by Cargo resolution."""

    environment = {**frozen_cargo_environment(toolchain), **extra}
    environment.update(
        {
            "CARGO_HOME": GUEST_CARGO_HOME,
            "GIT_CONFIG_GLOBAL": f"{GUEST_ROOT}/absent-gitconfig",
            "PATH": f"{GUEST_TOOLCHAIN_ROOT}/bin:/usr/bin:/bin",
            "RUSTC": GUEST_RUSTC,
            "RUSTUP_HOME": GUEST_RUSTUP_HOME,
        }
    )
    return environment


def sandboxed_build_environment(
    toolchain: Mapping[str, Any], extra: Mapping[str, Any]
) -> dict[str, Any]:
    """Add rustc's fixed loader origin to the retained build environment."""

    if "LD_ORIGIN_PATH" in extra:
        raise ValueError("sandboxed build loader origin override")
    environment = sandboxed_cargo_environment(toolchain, extra)
    environment["LD_ORIGIN_PATH"] = GUEST_TOOLCHAIN_BIN
    return environment


def frozen_cargo_environment(toolchain: Mapping[str, Any]) -> dict[str, str]:
    """Independently reconstruct the retained host Cargo/Git environment."""

    required = (
        "cargo_path",
        "rustc_path",
        "cargo_home_path",
        "rustup_home_path",
        "rustup_toolchain",
    )
    if not all(isinstance(toolchain.get(field), str) and toolchain[field] for field in required):
        raise ValueError("semantic Cargo environment toolchain differs")
    path = ":".join(
        dict.fromkeys(
            (
                str(Path(toolchain["cargo_path"]).parent),
                str(Path(toolchain["rustc_path"]).parent),
                "/usr/bin",
                "/bin",
            )
        )
    )
    return {
        "CARGO_HOME": toolchain["cargo_home_path"],
        "CARGO_INCREMENTAL": "0",
        "CARGO_NET_OFFLINE": "true",
        "GIT_CONFIG_COUNT": "0",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": path,
        "RUSTC": toolchain["rustc_path"],
        "RUSTUP_HOME": toolchain["rustup_home_path"],
        "RUSTUP_TOOLCHAIN": toolchain["rustup_toolchain"],
        "TZ": "UTC",
    }


def validate_sandboxed_build_argv(
    value: Any,
    *,
    bwrap_path: Any,
    cargo_config_search_sha256: Any,
    semantic_runtime_sha256: Any,
    rustc_host: Any,
    execution_tools_sha256: Any,
    package: str,
    example: str,
    context: str,
    problems: Problems,
) -> str | None:
    """Validate and normalize the exact release-build descriptor bindings."""

    if not isinstance(value, list) or any(
        not isinstance(argument, str) for argument in value
    ):
        problems.add(f"{context} is not an exact string list")
        return None
    system_bindings = tuple(
        ("--ro-bind-fd", guest_path)
        for _host_path, guest_path in TRUSTED_SYSTEM_MOUNTS
    )
    if (
        not isinstance(rustc_host, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", rustc_host) is None
    ):
        problems.add(f"{context} rustc host differs")
        return None
    rust_lld_guest_path = (
        f"{GUEST_TOOLCHAIN_ROOT}/lib/rustlib/{rustc_host}/bin/gcc-ld/ld.lld"
    )
    core_bindings = (
        ("--ro-bind-fd", GUEST_SOURCE),
        ("--bind-fd", GUEST_TARGET),
        ("--ro-bind-fd", GUEST_TOOLCHAIN_ROOT),
        ("--ro-bind-fd", GUEST_CARGO),
        ("--ro-bind-fd", GUEST_RUSTC),
        ("--ro-bind-fd", rust_lld_guest_path),
    )
    config_bindings = tuple(
        ("--ro-bind-fd", guest_path) for guest_path in GUEST_BOUND_CONFIG_PATHS
    )
    prefix = [
        bwrap_path,
        "--die-with-parent",
        "--new-session",
        "--unshare-net",
        "--dir",
        "/usr",
    ]
    private_prefix = [
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
        "$DEV_NULL_FD",
        "/dev/null",
        "--dir",
        "/proc",
        "--tmpfs",
        "/tmp",
        "--tmpfs",
        GUEST_ROOT,
        "--dir",
        f"{GUEST_ROOT}/.cargo",
        "--tmpfs",
        f"{GUEST_ROOT}/.cargo",
        "--remount-ro",
        f"{GUEST_ROOT}/.cargo",
        "--dir",
        "/.cargo",
        "--tmpfs",
        "/.cargo",
        "--remount-ro",
        "/.cargo",
    ]
    suffix = [
        "--chdir",
        GUEST_SOURCE,
        GUEST_CARGO,
        "build",
        "--locked",
        "--offline",
        "--release",
        "-p",
        package,
        "--example",
        example,
        "--target-dir",
        GUEST_TARGET,
    ]
    source_config_prefix = [
        "--dir",
        f"{GUEST_SOURCE}/.cargo",
        "--tmpfs",
        f"{GUEST_SOURCE}/.cargo",
    ]
    source_config_suffix = ["--remount-ro", f"{GUEST_SOURCE}/.cargo"]
    cargo_home_suffix = ["--remount-ro", GUEST_CARGO_HOME]
    expected_length = (
        len(prefix)
        + 3 * len(system_bindings)
        + len(private_prefix)
        + 3 * len(core_bindings)
        + 4
        + len(source_config_prefix)
        + 3 * len(config_bindings)
        + len(source_config_suffix)
        + len(cargo_home_suffix)
        + len(suffix)
    )
    if len(value) != expected_length or value[: len(prefix)] != prefix:
        problems.add(f"{context} fixed sandbox prefix/cardinality differs")
        return None
    descriptors: list[str] = []
    normalized = list(value)
    offset = len(prefix)

    def consume_binding(operation: str, destination: str) -> bool:
        nonlocal offset
        segment = value[offset : offset + 3]
        descriptor = segment[1] if len(segment) == 3 else ""
        if (
            len(segment) != 3
            or segment[0] != operation
            or segment[2] != destination
            or not descriptor.isascii()
            or not descriptor.isdecimal()
            or len(descriptor) > 10
            or str(int(descriptor)) != descriptor
            or int(descriptor) < 3
        ):
            problems.add(f"{context} descriptor binding for {destination} differs")
            return False
        descriptors.append(descriptor)
        normalized[offset + 1] = f"$FD:{destination}"
        offset += 3
        return True

    for operation, destination in system_bindings:
        if not consume_binding(operation, destination):
            return None
    observed_private = value[offset : offset + len(private_prefix)]
    dev_null_index = private_prefix.index("$DEV_NULL_FD")
    dev_null_source = (
        observed_private[dev_null_index]
        if len(observed_private) == len(private_prefix)
        else ""
    )
    dev_null_descriptor = (
        dev_null_source.removeprefix("/proc/self/fd/")
        if dev_null_source.startswith("/proc/self/fd/")
        else ""
    )
    expected_private = list(private_prefix)
    expected_private[dev_null_index] = dev_null_source
    if (
        observed_private != expected_private
        or not dev_null_descriptor.isascii()
        or not dev_null_descriptor.isdecimal()
        or len(dev_null_descriptor) > 10
        or str(int(dev_null_descriptor)) != dev_null_descriptor
        or int(dev_null_descriptor) < 3
    ):
        problems.add(f"{context} private namespace differs")
        return None
    descriptors.append(dev_null_descriptor)
    normalized[offset + dev_null_index] = "$FD:/dev/null"
    offset += len(private_prefix)
    for operation, destination in core_bindings:
        if not consume_binding(operation, destination):
            return None
    cargo_home_overlay = value[offset : offset + 4]
    cargo_home_source = (
        cargo_home_overlay[1] if len(cargo_home_overlay) == 4 else ""
    )
    cargo_home_descriptor = (
        cargo_home_source.removeprefix("/proc/self/fd/")
        if cargo_home_source.startswith("/proc/self/fd/")
        else ""
    )
    if (
        len(cargo_home_overlay) != 4
        or cargo_home_overlay[0] != "--overlay-src"
        or not cargo_home_descriptor.isascii()
        or not cargo_home_descriptor.isdecimal()
        or len(cargo_home_descriptor) > 10
        or str(int(cargo_home_descriptor)) != cargo_home_descriptor
        or int(cargo_home_descriptor) < 3
        or cargo_home_source != f"/proc/self/fd/{cargo_home_descriptor}"
        or cargo_home_overlay[2:] != ["--tmp-overlay", GUEST_CARGO_HOME]
    ):
        problems.add(f"{context} Cargo-home overlay differs")
        return None
    descriptors.append(cargo_home_descriptor)
    normalized[offset + 1] = "$FD:cargo-home-overlay"
    offset += 4
    if value[offset : offset + len(source_config_prefix)] != source_config_prefix:
        problems.add(f"{context} source Cargo-config private view differs")
        return None
    offset += len(source_config_prefix)
    for operation, destination in config_bindings[:2]:
        if not consume_binding(operation, destination):
            return None
    if value[offset : offset + len(source_config_suffix)] != source_config_suffix:
        problems.add(f"{context} source Cargo-config read-only remount differs")
        return None
    offset += len(source_config_suffix)
    for operation, destination in config_bindings[2:]:
        if not consume_binding(operation, destination):
            return None
    if value[offset : offset + len(cargo_home_suffix)] != cargo_home_suffix:
        problems.add(f"{context} Cargo-home read-only remount differs")
        return None
    offset += len(cargo_home_suffix)
    if len(set(descriptors)) != len(descriptors):
        problems.add(f"{context} descriptor operands are not distinct")
    if value[offset:] != suffix:
        problems.add(f"{context} fixed guest build command differs")
    if (
        not is_sha256(cargo_config_search_sha256)
        or not is_sha256(semantic_runtime_sha256)
        or not is_sha256(execution_tools_sha256)
    ):
        problems.add(f"{context} Cargo config/tool/semantic hash is invalid")
        return None
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "argv": normalized,
                "cargo_config_search_sha256": cargo_config_search_sha256,
                "execution_tools_sha256": execution_tools_sha256,
                "semantic_runtime_sha256": semantic_runtime_sha256,
            }
        )
    ).hexdigest()


def _semantic_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _semantic_exact(value: Any, fields: set[str], context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ValueError(f"{context} fields are not exact")
    return value


def _semantic_metadata_equal(
    left: os.stat_result, right: os.stat_result
) -> bool:
    return all(
        getattr(left, field) == getattr(right, field)
        for field in (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_nlink",
            "st_uid",
            "st_gid",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
    )


def _semantic_entry(
    metadata: os.stat_result,
    relative: str,
    file_type: str,
    digest: str | None,
    symlink_target: str | None,
    symlink_scope: str | None,
    volatile_directories: frozenset[str],
) -> dict[str, Any]:
    entry = {
        "changed_ns": metadata.st_ctime_ns,
        "device": metadata.st_dev,
        "file_type": file_type,
        "gid": metadata.st_gid,
        "inode": metadata.st_ino,
        "link_count": metadata.st_nlink,
        "modified_ns": metadata.st_mtime_ns,
        "path": relative,
        "permissions": stat.S_IMODE(metadata.st_mode),
        "sha256": digest,
        "size": metadata.st_size,
        "symlink_target": symlink_target,
        "symlink_scope": symlink_scope,
        "uid": metadata.st_uid,
    }
    if file_type == "directory" and relative in volatile_directories:
        for field in ("changed_ns", "modified_ns", "permissions", "size"):
            entry[field] = 0
    return entry


def _semantic_system_symlink_scope(
    root: Path, relative: str, target: str
) -> str:
    rendered = os.path.normpath(
        str(
            PurePosixPath(target)
            if PurePosixPath(target).is_absolute()
            else PurePosixPath(str(root))
            / PurePosixPath(relative).parent
            / target
        )
    )
    for alias, destination in (
        ("/bin", "/usr/bin"),
        ("/lib", "/usr/lib"),
        ("/lib64", "/usr/lib"),
    ):
        if rendered == alias or rendered.startswith(alias + "/"):
            rendered = destination + rendered.removeprefix(alias)
            break
    exposed = tuple(guest for _host, guest in TRUSTED_SYSTEM_MOUNTS)
    if any(
        rendered == authority or rendered.startswith(authority + "/")
        for authority in exposed
    ):
        return "within_closure"
    if any(
        rendered == authority or rendered.startswith(authority + "/")
        for authority in ("/asterism", "/dev", "/proc", "/run", "/sys", "/tmp")
    ):
        raise ValueError("trusted-system symlink reaches mutable guest authority")
    return "guest_inaccessible_external"


def sample_semantic_tree(
    root: Path,
    role: str,
    context: str,
    *,
    allow_internal_symlinks: bool,
    hash_regular_contents: bool,
    trusted_system: bool = False,
    excluded_paths: frozenset[str] = frozenset(),
    volatile_directories: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Descriptor-resample one semantic root without shared validators."""

    lexical = Path(os.path.abspath(os.fspath(root)))
    resolved = lexical.resolve(strict=True)
    if lexical != resolved:
        raise ValueError(f"{context} root is not canonical")
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    regular_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    root_fd = os.open(resolved, directory_flags)
    try:
        entries: list[dict[str, Any]] = []

        def walk(descriptor: int, relative: str) -> None:
            before = os.fstat(descriptor)
            directory_path = resolved if relative == "." else resolved / relative
            if not stat.S_ISDIR(before.st_mode):
                raise ValueError(f"{context} directory type changed")
            if trusted_system and (
                before.st_uid != 0
                or stat.S_IMODE(before.st_mode) & 0o022
                or os.access(directory_path, os.W_OK)
            ):
                raise ValueError(f"{context} trusted directory is writable")
            entries.append(
                _semantic_entry(
                    before,
                    relative,
                    "directory",
                    None,
                    None,
                    None,
                    volatile_directories,
                )
            )
            names = sorted(os.listdir(descriptor))
            if len(names) != len(set(names)):
                raise ValueError(f"{context} directory names alias")
            for name in names:
                child_relative = name if relative == "." else f"{relative}/{name}"
                if child_relative in excluded_paths:
                    continue
                selected = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
                candidate = resolved / child_relative
                if stat.S_ISDIR(selected.st_mode):
                    child = os.open(name, directory_flags, dir_fd=descriptor)
                    try:
                        if not _semantic_metadata_equal(selected, os.fstat(child)):
                            raise ValueError(f"{context} directory selection changed")
                        walk(child, child_relative)
                    finally:
                        os.close(child)
                elif stat.S_ISREG(selected.st_mode):
                    child = os.open(name, regular_flags, dir_fd=descriptor)
                    try:
                        opened = os.fstat(child)
                        if not _semantic_metadata_equal(selected, opened):
                            raise ValueError(f"{context} file selection changed")
                        digest = hashlib.sha256()
                        observed_size = 0
                        while True:
                            chunk = os.read(child, 1024 * 1024)
                            if not chunk:
                                break
                            observed_size += len(chunk)
                            if hash_regular_contents:
                                digest.update(chunk)
                        after = os.fstat(child)
                    finally:
                        os.close(child)
                    if observed_size != opened.st_size or not _semantic_metadata_equal(
                        opened, after
                    ):
                        raise ValueError(f"{context} regular file changed")
                    if trusted_system and (
                        opened.st_uid != 0
                        or stat.S_IMODE(opened.st_mode) & 0o022
                        or os.access(candidate, os.W_OK)
                    ):
                        raise ValueError(f"{context} trusted file is writable")
                    entries.append(
                        _semantic_entry(
                            opened,
                            child_relative,
                            "regular",
                            digest.hexdigest() if hash_regular_contents else None,
                            None,
                            None,
                            volatile_directories,
                        )
                    )
                elif stat.S_ISLNK(selected.st_mode):
                    if not allow_internal_symlinks:
                        raise ValueError(f"{context} contains a symlink")
                    target = os.readlink(name, dir_fd=descriptor)
                    selected_after = os.stat(
                        name, dir_fd=descriptor, follow_symlinks=False
                    )
                    if not _semantic_metadata_equal(selected, selected_after):
                        raise ValueError(f"{context} symlink changed")
                    if trusted_system and selected.st_uid != 0:
                        raise ValueError(f"{context} trusted symlink is not root-owned")
                    target_path = candidate.parent.joinpath(target).resolve(strict=True)
                    if trusted_system:
                        scope = _semantic_system_symlink_scope(
                            resolved, child_relative, target
                        )
                    else:
                        if target_path != resolved and resolved not in target_path.parents:
                            raise ValueError(f"{context} symlink escapes root")
                        scope = "within_root"
                    entries.append(
                        _semantic_entry(
                            selected,
                            child_relative,
                            "symlink",
                            hashlib.sha256(os.fsencode(target)).hexdigest(),
                            target,
                            scope,
                            volatile_directories,
                        )
                    )
                else:
                    raise ValueError(f"{context} contains unsupported file type")
            if not _semantic_metadata_equal(before, os.fstat(descriptor)):
                raise ValueError(f"{context} directory changed")

        walk(root_fd, ".")
        entries.sort(key=lambda entry: (entry["path"] != ".", entry["path"]))
        return {
            "entries": entries,
            "role": role,
            "schema": RECURSIVE_TREE_AUTHORITY_SCHEMA,
        }
    finally:
        os.close(root_fd)


class SemanticReplay:
    """Independent 48-manifest semantic-input replay and topology ledger."""

    def __init__(self, problems: Problems, *, live_system: bool = True) -> None:
        self.problems = problems
        self.live_system = live_system
        self.manifest_paths: set[str] = set()
        self.manifest_identities: set[tuple[int, int]] = set()
        self.authority_paths: set[tuple[str, ...]] = set()
        self.runtimes: set[str] = set()
        self.live_cache: dict[tuple[Any, ...], Mapping[str, Any]] = {}

    def _validate_tree(
        self, value: Any, role: str, context: str, *, trusted_system: bool
    ) -> Mapping[str, Any]:
        manifest = _semantic_exact(
            value, {"entries", "role", "schema"}, f"{context} manifest"
        )
        entries = manifest["entries"]
        if (
            manifest["schema"] != RECURSIVE_TREE_AUTHORITY_SCHEMA
            or manifest["role"] != role
            or not isinstance(entries, list)
            or not entries
        ):
            raise ValueError(f"{context} manifest identity differs")
        paths: list[str] = []
        for index, raw in enumerate(entries):
            entry = _semantic_exact(
                raw, SEMANTIC_MANIFEST_ENTRY_FIELDS, f"{context} entry {index}"
            )
            relative = entry["path"]
            parsed = PurePosixPath(relative) if isinstance(relative, str) else None
            if (
                parsed is None
                or (
                    relative != "."
                    and (
                        parsed.is_absolute()
                        or str(parsed) != relative
                        or "." in parsed.parts
                        or ".." in parsed.parts
                    )
                )
                or (index == 0) != (relative == ".")
                or relative in paths
            ):
                raise ValueError(f"{context} path differs")
            paths.append(relative)
            kind = entry["file_type"]
            integers = (
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
            if kind not in {"directory", "regular", "symlink"} or any(
                not _semantic_integer(entry[field]) for field in integers
            ):
                raise ValueError(f"{context} metadata differs")
            if kind == "directory":
                if any(
                    entry[field] is not None
                    for field in ("sha256", "symlink_target", "symlink_scope")
                ):
                    raise ValueError(f"{context} directory digest differs")
            elif kind == "regular":
                digest_valid = (
                    entry["sha256"] is None
                    if trusted_system
                    else is_sha256(entry["sha256"])
                )
                if (
                    not digest_valid
                    or entry["symlink_target"] is not None
                    or entry["symlink_scope"] is not None
                ):
                    raise ValueError(f"{context} regular digest differs")
            else:
                scopes = (
                    {"within_closure", "guest_inaccessible_external"}
                    if trusted_system
                    else {"within_root"}
                )
                if (
                    not isinstance(entry["symlink_target"], str)
                    or not is_sha256(entry["sha256"])
                    or entry["symlink_scope"] not in scopes
                ):
                    raise ValueError(f"{context} symlink authority differs")
            if trusted_system and (
                entry["uid"] != 0
                or (kind != "symlink" and entry["permissions"] & 0o022)
            ):
                raise ValueError(f"{context} trusted-system policy differs")
        if paths != sorted(paths, key=lambda item: (item != ".", item)):
            raise ValueError(f"{context} path order differs")
        return manifest

    def _live_tree(
        self,
        root: Path,
        role: str,
        context: str,
        *,
        source_role: str,
        name: str,
        trusted_system: bool,
    ) -> Mapping[str, Any]:
        excluded = (
            frozenset({"Cargo.lock"})
            if source_role == "resolution_source_without_cargo_lock"
            and name == "source"
            else frozenset()
        )
        volatile = (
            frozenset({"."})
            if source_role == "resolution_source_without_cargo_lock"
            and name == "source"
            else frozenset()
        )
        key = (
            str(root),
            role,
            name != "source",
            name != "trusted_system",
            trusted_system,
            tuple(sorted(excluded)),
            tuple(sorted(volatile)),
        )
        observed = self.live_cache.get(key)
        if observed is None:
            arguments = {
                "allow_internal_symlinks": name != "source",
                "hash_regular_contents": not trusted_system,
                "trusted_system": trusted_system,
                "excluded_paths": excluded,
                "volatile_directories": volatile,
            }
            first = sample_semantic_tree(root, role, context, **arguments)
            second = sample_semantic_tree(root, role, context, **arguments)
            if first != second:
                raise ValueError(f"{context} changed across live replay")
            observed = first
            self.live_cache[key] = observed
        return observed

    @staticmethod
    def live_roots(
        source_root: Path, toolchain: Any, context: str
    ) -> dict[str, Path]:
        if not isinstance(toolchain, Mapping):
            raise ValueError(f"{context} toolchain is absent")
        values = tuple(
            toolchain.get(field)
            for field in ("cargo_path", "rustc_path", "cargo_home_path")
        )
        if not all(isinstance(value, str) for value in values):
            raise ValueError(f"{context} toolchain paths differ")
        source = Path(os.path.abspath(os.fspath(source_root))).resolve(strict=True)
        cargo = Path(os.path.abspath(values[0])).resolve(strict=True)
        rustc = Path(os.path.abspath(values[1])).resolve(strict=True)
        cargo_home = Path(os.path.abspath(values[2])).resolve(strict=True)
        toolchain_root = cargo.parent.parent
        if rustc.parent.parent != toolchain_root:
            raise ValueError(f"{context} Cargo/rustc roots differ")
        return {
            "cargo_home": cargo_home,
            "source": source,
            "toolchain": toolchain_root,
        }

    def validate_authority(
        self,
        value: Any,
        context: str,
        *,
        live_roots: Mapping[str, Path],
        source_role: str = "source",
        prepared_authority: bool = False,
    ) -> str:
        parse_manifest = (
            schema.parse_prepared_authority_json_object
            if prepared_authority
            else schema.parse_canonical_json_object
        )
        canonicalize_manifest = (
            schema.prepared_authority_canonical_json_bytes
            if prepared_authority
            else canonical_json_bytes
        )
        authority = _semantic_exact(
            value, SEMANTIC_INPUT_AUTHORITY_FIELDS, f"{context} authority"
        )
        if authority["schema"] != SEMANTIC_INPUT_AUTHORITY_SCHEMA:
            raise ValueError(f"{context} authority schema differs")
        authority_manifest_paths: list[str] = []
        authority_manifest_identities: set[tuple[int, int]] = set()
        runtime_manifests: dict[str, Mapping[str, Any]] = {}
        for name, role in (
            ("source", source_role),
            ("toolchain", "toolchain"),
            ("cargo_home", "cargo_home"),
        ):
            binding = _semantic_exact(
                authority[name],
                SEMANTIC_TREE_BINDING_FIELDS,
                f"{context} {name} binding",
            )
            manifest_path = binding["manifest_path"]
            if (
                binding["schema"] != RECURSIVE_TREE_AUTHORITY_SCHEMA
                or binding["role"] != role
                or not isinstance(manifest_path, str)
                or not Path(manifest_path).is_absolute()
                or not is_sha256(binding["manifest_sha256"])
                or not _semantic_integer(binding["entry_count"])
                or binding["entry_count"] < 1
                or not _semantic_integer(binding["watch_count"])
                or binding["watch_count"] < 1
                or binding["equal_pre_post"] is not True
                or binding["mutation_events_absent"] is not True
            ):
                raise ValueError(f"{context} {name} binding differs")
            snapshot = schema.snapshot_regular_file(
                Path(manifest_path), expected_mode=0o444
            )
            identity = (snapshot.device, snapshot.inode)
            if (
                snapshot._stat.st_nlink != 1
                or identity in authority_manifest_identities
            ):
                raise ValueError(f"{context} semantic manifest identity aliases")
            authority_manifest_identities.add(identity)
            manifest = self._validate_tree(
                parse_manifest(
                    snapshot.data, f"{context} {name} manifest"
                ),
                role,
                f"{context} {name}",
                trusted_system=False,
            )
            if name != "source":
                runtime_manifests[name] = manifest
            entries = manifest["entries"]
            if (
                snapshot.sha256 != binding["manifest_sha256"]
                or len(entries) != binding["entry_count"]
                or sum(entry["file_type"] == "directory" for entry in entries)
                != binding["watch_count"]
                or canonicalize_manifest(
                    self._live_tree(
                        live_roots[name],
                        role,
                        f"{context} live {name}",
                        source_role=source_role,
                        name=name,
                        trusted_system=False,
                    )
                )
                != snapshot.data
            ):
                raise ValueError(f"{context} {name} live manifest differs")
            authority_manifest_paths.append(manifest_path)

        closure = _semantic_exact(
            authority["trusted_system_closure"],
            SEMANTIC_CLOSURE_FIELDS,
            f"{context} system closure",
        )
        mounts = closure["mounts"]
        if (
            closure["schema"] != TRUSTED_SYSTEM_CLOSURE_SCHEMA
            or not isinstance(closure["manifest_path"], str)
            or not Path(closure["manifest_path"]).is_absolute()
            or not is_sha256(closure["sha256"])
            or not _semantic_integer(closure["entry_count"])
            or closure["entry_count"] < 3
            or not _semantic_integer(closure["watch_count"])
            or closure["watch_count"] < 3
            or closure["mutation_events_absent"] is not True
            or not isinstance(mounts, list)
            or len(mounts) != 3
        ):
            raise ValueError(f"{context} system closure differs")
        validated_mounts = []
        for index, (raw, (host_path, guest_path)) in enumerate(
            zip(mounts, TRUSTED_SYSTEM_MOUNTS, strict=True)
        ):
            mount = _semantic_exact(
                raw,
                SEMANTIC_CLOSURE_MOUNT_FIELDS,
                f"{context} system mount {index}",
            )
            if (
                mount["host_path"] != str(host_path)
                or mount["resolved_path"] != str(host_path)
                or mount["guest_path"] != guest_path
                or mount["trusted_root_owned_non_writable"] is not True
                or mount["uid"] != 0
                or any(
                    not _semantic_integer(mount[field])
                    for field in ("device", "gid", "inode", "permissions", "uid")
                )
                or mount["permissions"] & 0o022
            ):
                raise ValueError(f"{context} system mount differs")
            validated_mounts.append(mount)
        closure_snapshot = schema.snapshot_regular_file(
            Path(closure["manifest_path"]), expected_mode=0o444
        )
        closure_identity = (closure_snapshot.device, closure_snapshot.inode)
        if (
            closure_snapshot._stat.st_nlink != 1
            or closure_identity in authority_manifest_identities
        ):
            raise ValueError(f"{context} semantic manifest identity aliases")
        authority_manifest_identities.add(closure_identity)
        closure_manifest = _semantic_exact(
            parse_manifest(
                closure_snapshot.data, f"{context} closure manifest"
            ),
            {"mounts", "schema"},
            f"{context} closure manifest",
        )
        runtime_manifests["trusted_system_closure"] = closure_manifest
        evidence_mounts = closure_manifest["mounts"]
        if (
            closure_manifest["schema"] != TRUSTED_SYSTEM_CLOSURE_SCHEMA
            or closure_snapshot.sha256 != closure["sha256"]
            or not isinstance(evidence_mounts, list)
            or len(evidence_mounts) != 3
        ):
            raise ValueError(f"{context} closure evidence differs")
        total_entries = 0
        total_watches = 0
        for index, (raw, (host_path, guest_path), binding) in enumerate(
            zip(
                evidence_mounts,
                TRUSTED_SYSTEM_MOUNTS,
                validated_mounts,
                strict=True,
            )
        ):
            evidence = _semantic_exact(
                raw,
                {"guest_path", "host_path", "resolved_path", "tree"},
                f"{context} closure mount {index}",
            )
            if (
                evidence["guest_path"] != guest_path
                or evidence["host_path"] != str(host_path)
                or evidence["resolved_path"] != str(host_path)
            ):
                raise ValueError(f"{context} closure path differs")
            role = "system-" + guest_path.removeprefix("/").replace("/", "-")
            tree = self._validate_tree(
                evidence["tree"], role, f"{context} {role}", trusted_system=True
            )
            root_entry = tree["entries"][0]
            if any(
                binding[field] != root_entry[field]
                for field in ("device", "gid", "inode", "permissions", "uid")
            ):
                raise ValueError(f"{context} system root binding differs")
            total_entries += len(tree["entries"])
            total_watches += sum(
                entry["file_type"] == "directory" for entry in tree["entries"]
            )
            if self.live_system and self._live_tree(
                host_path,
                role,
                f"{context} live {role}",
                source_role=source_role,
                name="trusted_system",
                trusted_system=True,
            ) != tree:
                raise ValueError(f"{context} live system tree differs")
        if (
            total_entries != closure["entry_count"]
            or total_watches != closure["watch_count"]
        ):
            raise ValueError(f"{context} closure counts differ")
        authority_manifest_paths.append(closure["manifest_path"])
        if len(set(authority_manifest_paths)) != 4:
            raise ValueError(f"{context} manifest files alias")
        normalized = {
            "cargo_home": {
                field: authority["cargo_home"][field]
                for field in (
                    "schema",
                    "role",
                    "manifest_sha256",
                    "entry_count",
                    "watch_count",
                    "equal_pre_post",
                    "mutation_events_absent",
                )
            },
            "schema": SEMANTIC_INPUT_AUTHORITY_SCHEMA,
            "toolchain": {
                field: authority["toolchain"][field]
                for field in (
                    "schema",
                    "role",
                    "manifest_sha256",
                    "entry_count",
                    "watch_count",
                    "equal_pre_post",
                    "mutation_events_absent",
                )
            },
            "trusted_system_closure": {
                field: closure[field]
                for field in (
                    "schema",
                    "sha256",
                    "entry_count",
                    "mounts",
                    "watch_count",
                    "mutation_events_absent",
                )
            },
        }
        recorded_runtime = hashlib.sha256(
            canonicalize_manifest(normalized)
        ).hexdigest()
        if authority["runtime_sha256"] != recorded_runtime:
            raise ValueError(f"{context} runtime digest differs")
        components = {
            name: authority[name]
            for name in ("cargo_home", "toolchain", "trusted_system_closure")
        }
        content_runtime = schema.semantic_runtime_content_sha256(
            components, runtime_manifests
        )
        path_tuple = tuple(sorted(authority_manifest_paths))
        if authority_manifest_identities & self.manifest_identities:
            raise ValueError(f"{context} semantic manifest files alias")
        self.authority_paths.add(path_tuple)
        self.manifest_paths.update(authority_manifest_paths)
        self.manifest_identities.update(authority_manifest_identities)
        self.runtimes.add(content_runtime)
        return content_runtime

    def capture(self, context: str, action: Callable[[], Any]) -> Any | None:
        try:
            return action()
        except Exception as error:
            self.problems.add(f"{context}: {error}")
            return None

    def finalize(self) -> None:
        if (
            len(self.authority_paths) != 12
            or len(self.manifest_paths) != 48
            or len(self.manifest_identities) != 48
        ):
            self.problems.add(
                "semantic manifest topology differs; "
                f"authorities={len(self.authority_paths)} manifests={len(self.manifest_paths)} "
                f"identities={len(self.manifest_identities)}"
            )
        if len(self.runtimes) != 1:
            self.problems.add(
                f"semantic runtime identity differs; observed={sorted(self.runtimes)}"
            )


def validate_resolution_sandbox_argv(
    value: Any,
    *,
    bwrap_path: Any,
    cargo_arguments: Sequence[str],
    context: str,
) -> None:
    """Independently replay the exact 13-FD private resolver namespace."""

    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError(f"{context} argv is not an exact string list")
    system_bindings = tuple(
        ("--ro-bind-fd", guest_path)
        for _host_path, guest_path in TRUSTED_SYSTEM_MOUNTS
    )
    core_bindings = (
        ("--bind-fd", GUEST_SOURCE),
        ("--ro-bind-fd", GUEST_TOOLCHAIN_ROOT),
        ("--ro-bind-fd", GUEST_CARGO),
        ("--ro-bind-fd", GUEST_RUSTC),
    )
    config_bindings = tuple(
        ("--ro-bind-fd", guest_path) for guest_path in GUEST_BOUND_CONFIG_PATHS
    )
    prefix = [
        bwrap_path,
        "--die-with-parent",
        "--new-session",
        "--unshare-net",
        "--dir",
        "/usr",
    ]
    private = [
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
        GUEST_ROOT,
    ]
    cargo_private = [
        "--dir",
        f"{GUEST_ROOT}/.cargo",
        "--tmpfs",
        f"{GUEST_ROOT}/.cargo",
        "--remount-ro",
        f"{GUEST_ROOT}/.cargo",
        "--dir",
        "/.cargo",
        "--tmpfs",
        "/.cargo",
        "--remount-ro",
        "/.cargo",
        "--dir",
        f"{GUEST_SOURCE}/.cargo",
        "--tmpfs",
        f"{GUEST_SOURCE}/.cargo",
    ]
    source_remount = ["--remount-ro", f"{GUEST_SOURCE}/.cargo"]
    cargo_home_remount = ["--remount-ro", GUEST_CARGO_HOME]
    suffix = ["--chdir", GUEST_SOURCE, GUEST_CARGO, *cargo_arguments]
    expected_length = (
        len(prefix)
        + 3 * len(system_bindings)
        + len(private)
        + 3 * len(core_bindings)
        + 4
        + len(cargo_private)
        + 3 * len(config_bindings)
        + len(source_remount)
        + len(cargo_home_remount)
        + len(suffix)
    )
    if len(value) != expected_length or value[: len(prefix)] != prefix:
        raise ValueError(f"{context} prefix/cardinality differs")
    descriptors: list[str] = []
    offset = len(prefix)

    def consume(bindings: Sequence[tuple[str, str]]) -> None:
        nonlocal offset
        for operation, destination in bindings:
            segment = value[offset : offset + 3]
            descriptor = segment[1] if len(segment) == 3 else ""
            if (
                len(segment) != 3
                or segment[0] != operation
                or segment[2] != destination
                or not descriptor.isascii()
                or not descriptor.isdecimal()
                or len(descriptor) > 10
                or str(int(descriptor)) != descriptor
                or int(descriptor) < 3
            ):
                raise ValueError(f"{context} binding differs for {destination}")
            descriptors.append(descriptor)
            offset += 3

    consume(system_bindings)
    if value[offset : offset + len(private)] != private:
        raise ValueError(f"{context} private namespace differs")
    offset += len(private)
    consume(core_bindings)
    cargo_home_overlay = value[offset : offset + 4]
    cargo_home_source = (
        cargo_home_overlay[1] if len(cargo_home_overlay) == 4 else ""
    )
    cargo_home_descriptor = (
        cargo_home_source.removeprefix("/proc/self/fd/")
        if cargo_home_source.startswith("/proc/self/fd/")
        else ""
    )
    if (
        len(cargo_home_overlay) != 4
        or cargo_home_overlay[0] != "--overlay-src"
        or not cargo_home_descriptor.isascii()
        or not cargo_home_descriptor.isdecimal()
        or len(cargo_home_descriptor) > 10
        or str(int(cargo_home_descriptor)) != cargo_home_descriptor
        or int(cargo_home_descriptor) < 3
        or cargo_home_source != f"/proc/self/fd/{cargo_home_descriptor}"
        or cargo_home_overlay[2:] != ["--tmp-overlay", GUEST_CARGO_HOME]
    ):
        raise ValueError(f"{context} Cargo-home overlay differs")
    descriptors.append(cargo_home_descriptor)
    offset += 4
    if value[offset : offset + len(cargo_private)] != cargo_private:
        raise ValueError(f"{context} private Cargo view differs")
    offset += len(cargo_private)
    consume(config_bindings[:2])
    if value[offset : offset + len(source_remount)] != source_remount:
        raise ValueError(f"{context} source config remount differs")
    offset += len(source_remount)
    consume(config_bindings[2:])
    if value[offset : offset + len(cargo_home_remount)] != cargo_home_remount:
        raise ValueError(f"{context} Cargo-home remount differs")
    offset += len(cargo_home_remount)
    if len(descriptors) != 12 or len(set(descriptors)) != 12:
        raise ValueError(f"{context} bound descriptor topology differs")
    # bwrap itself is the thirteenth inherited descriptor, bound separately as
    # execution_authority rather than exposed inside the guest.
    if value[offset:] != suffix:
        raise ValueError(f"{context} Cargo command differs")


def validate_resolution_record(
    record: Mapping[str, Any],
    claim: Mapping[str, Any],
    variant: str,
    label: str,
    toolchain: Mapping[str, Any],
    current_lock_sha256: str,
    problems: Problems,
) -> None:
    """Replay retained resolver execution, config, and lock transitions."""

    context = f"semantic resolver {variant} {label}"
    cargo_arguments = (
        ["metadata", "--locked", "--offline", "--format-version", "1", "--no-deps"]
        if label == "current"
        else ["generate-lockfile", "--offline"]
    )
    try:
        validate_resolution_sandbox_argv(
            record.get("argv"),
            bwrap_path=toolchain.get("bwrap_path"),
            cargo_arguments=cargo_arguments,
            context=context,
        )
    except Exception as error:
        problems.add(f"{context} sandbox: {error}")
    try:
        expected_environment = sandboxed_cargo_environment(toolchain, {})
    except Exception as error:
        problems.add(f"{context} environment: {error}")
        expected_environment = None
    if record.get("environment") != expected_environment:
        problems.add(f"{context} environment differs")
    for stream in ("stdout", "stderr"):
        payload = record.get(stream)
        if (
            not isinstance(payload, str)
            or record.get(f"{stream}_sha256")
            != hashlib.sha256(payload.encode()).hexdigest()
        ):
            problems.add(f"{context} {stream} authority differs")
    execution = record.get("execution_authority")
    mode = execution.get("mode") if isinstance(execution, Mapping) else None
    if (
        not _semantic_integer(mode)
        or mode & 0o111 == 0
        or not isinstance(execution, Mapping)
        or execution.get("path") != toolchain.get("bwrap_path")
        or execution.get("sha256") != toolchain.get("bwrap_sha256")
    ):
        problems.add(f"{context} retained bwrap authority differs")
    else:
        validate_release_file_binding(
            execution, f"{context} retained bwrap", problems, expected_mode=mode
        )
    source_root_value = record.get("host_source_root")
    if not isinstance(source_root_value, str):
        problems.add(f"{context} source root differs")
        return
    source_root = Path(source_root_value)
    validate_sandboxed_cargo_config_search(
        record.get("cargo_config_search"),
        source_root,
        toolchain,
        f"{context} Cargo config search",
        problems,
    )
    lock_output = record.get("lock_output")
    if not isinstance(lock_output, Mapping) or set(lock_output) != {"post", "pre"}:
        problems.add(f"{context} lock output fields differ")
        return
    expected_path = str(source_root / "Cargo.lock")
    for boundary in ("pre", "post"):
        binding = lock_output.get(boundary)
        if not isinstance(binding, Mapping) or set(binding) != {
            "path",
            "sha256",
            "status",
        }:
            problems.add(f"{context} lock {boundary} fields differ")
    expected = (
        {
            boundary: {
                "path": expected_path,
                "sha256": current_lock_sha256,
                "status": "present",
            }
            for boundary in ("pre", "post")
        }
        if label == "current"
        else {
            "pre": {"path": expected_path, "sha256": None, "status": "absent"},
            "post": {
                "path": expected_path,
                "sha256": claim.get("final_lock_sha256"),
                "status": "present",
            },
        }
    )
    if lock_output != expected:
        problems.add(f"{context} lock transition differs")
    if label == "generated":
        snapshot = resolve_bound_file(
            expected_path,
            claim.get("final_lock_sha256"),
            f"{context} live generated Cargo.lock",
            problems,
            expected_mode=None,
        )
        if snapshot is None:
            problems.add(f"{context} live generated lock differs")


def validate_semantic_toolchain(
    value: Any, context: str, *, live: bool
) -> Mapping[str, Any]:
    toolchain = _semantic_exact(value, TOOLCHAIN_FIELDS, context)
    file_pairs = (
        ("bwrap_path", "bwrap_sha256"),
        ("cargo_path", "cargo_sha256"),
        ("git_path", "git_sha256"),
        ("rustc_path", "rustc_sha256"),
        ("rust_lld_path", "rust_lld_sha256"),
        ("rustup_path", "rustup_sha256"),
    )
    resolved: dict[str, Path] = {}
    for path_field, digest_field in file_pairs:
        raw = toolchain[path_field]
        if not isinstance(raw, str) or not Path(raw).is_absolute() or not is_sha256(
            toolchain[digest_field]
        ):
            raise ValueError(f"{context} {path_field} binding differs")
        path = Path(raw).resolve(strict=True)
        if raw != str(path):
            raise ValueError(f"{context} {path_field} is not canonical")
        metadata = path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o111 == 0
            or (live and sha256_file(path) != toolchain[digest_field])
        ):
            raise ValueError(f"{context} {path_field} live identity differs")
        resolved[path_field] = path
    path_identities = [
        (path.stat().st_dev, path.stat().st_ino) for path in resolved.values()
    ]
    if (
        len(set(resolved.values())) != len(resolved)
        or len(set(path_identities)) != len(path_identities)
    ):
        raise ValueError(f"{context} executable paths physically alias")
    for field in ("cargo_home_path", "rustup_home_path"):
        raw = toolchain[field]
        if not isinstance(raw, str) or not Path(raw).is_absolute():
            raise ValueError(f"{context} {field} differs")
        path = Path(raw).resolve(strict=True)
        if raw != str(path) or not path.is_dir():
            raise ValueError(f"{context} {field} live root differs")
    for field in ("cargo_version_verbose", "rustc_version_verbose"):
        if (
            not isinstance(toolchain[field], str)
            or not toolchain[field]
            or toolchain[field] != toolchain[field].strip()
        ):
            raise ValueError(f"{context} {field} differs")
    rustc_host = toolchain["rustc_host"]
    rustup_toolchain = toolchain["rustup_toolchain"]
    host_lines = [
        line.removeprefix("host: ")
        for line in toolchain["rustc_version_verbose"].splitlines()
        if line.startswith("host: ")
    ]
    if (
        not isinstance(rustc_host, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", rustc_host) is None
    ):
        raise ValueError(f"{context} rustc host differs")
    expected_rust_lld = (
        Path(str(toolchain["cargo_path"])).parent.parent
        / "lib"
        / "rustlib"
        / str(rustc_host)
        / "bin"
        / "rust-lld"
    )
    if Path(str(toolchain["rust_lld_path"])) != expected_rust_lld:
        raise ValueError(f"{context} rust-lld topology differs")
    if (
        host_lines != [rustc_host]
        or not isinstance(rustup_toolchain, str)
        or not rustup_toolchain
        or any(character.isspace() for character in rustup_toolchain)
        or Path(rustup_toolchain).is_absolute()
        or Path(rustup_toolchain).parts != (rustup_toolchain,)
        or "\\" in rustup_toolchain
        or rustup_toolchain in {".", ".."}
    ):
        raise ValueError(f"{context} rustup/rustc sampled identity differs")
    rustup_home = Path(toolchain["rustup_home_path"])
    toolchain_root = rustup_home / "toolchains" / rustup_toolchain
    if (
        toolchain_root.resolve(strict=True) != toolchain_root
        or resolved["cargo_path"] != toolchain_root / "bin" / "cargo"
        or resolved["rustc_path"] != toolchain_root / "bin" / "rustc"
    ):
        raise ValueError(f"{context} Cargo/rustc rustup paths differ")
    return toolchain


def _current_live_file_identity(value: Any, context: str) -> Mapping[str, Any]:
    record = _semantic_exact(value, CURRENT_FILE_IDENTITY_FIELDS, context)
    path = record["path"]
    if not isinstance(path, str) or not Path(path).is_absolute():
        raise ValueError(f"{context} path differs")
    for field in CURRENT_FILE_IDENTITY_FIELDS - {"path", "sha256"}:
        if not _semantic_integer(record[field]):
            raise ValueError(f"{context} metadata differs")
    exact = Path(path).resolve(strict=True)
    metadata = exact.lstat()
    if (
        path != str(exact)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
    ):
        raise ValueError(f"{context} is not one canonical regular file")
    expected = {
        "bytes": metadata.st_size,
        "ctime_ns": metadata.st_ctime_ns,
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "link_count": metadata.st_nlink,
        "mode": stat.S_IMODE(metadata.st_mode),
        "mtime_ns": metadata.st_mtime_ns,
        "path": str(exact),
        "sha256": sha256_file(exact),
        "size": metadata.st_size,
    }
    if record != expected:
        raise ValueError(f"{context} live identity differs")
    return record


def _current_bound_file_identity(
    value: Any,
    live_path: Path,
    recorded_path: str,
    context: str,
    *,
    executable: bool,
    expected_link_count: int = 1,
) -> Mapping[str, Any]:
    """Replay an identity read through a now-closed retained directory FD."""

    record = _semantic_exact(value, CURRENT_FILE_IDENTITY_FIELDS, context)
    if any(
        not _semantic_integer(record[field])
        for field in CURRENT_FILE_IDENTITY_FIELDS - {"path", "sha256"}
    ):
        raise ValueError(f"{context} metadata differs")
    exact = live_path.resolve(strict=True)
    metadata = exact.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != expected_link_count
        or exact != live_path
        or record["path"] != recorded_path
        or record["bytes"] != metadata.st_size
        or record["device"] != metadata.st_dev
        or record["inode"] != metadata.st_ino
        or record["link_count"] != metadata.st_nlink
        or record["ctime_ns"] != metadata.st_ctime_ns
        or record["mode"] != stat.S_IMODE(metadata.st_mode)
        or record["mtime_ns"] != metadata.st_mtime_ns
        or record["sha256"] != sha256_file(exact)
        or record["size"] != metadata.st_size
        or (record["mode"] & 0o111 != 0) is not executable
        or stat.S_IMODE(metadata.st_mode) != (0o555 if executable else 0o444)
    ):
        raise ValueError(f"{context} retained descriptor identity differs")
    return record


def _current_bound_descriptor(argv: Any, destination: str, context: str) -> str:
    if not isinstance(argv, list):
        raise ValueError(f"{context} argv differs")
    matches = [
        argv[index + 1]
        for index in range(len(argv) - 2)
        if argv[index] in {"--bind-fd", "--ro-bind-fd", "--ro-bind-data"}
        and argv[index + 2] == destination
    ]
    if len(matches) != 1 or not isinstance(matches[0], str):
        raise ValueError(f"{context} descriptor binding differs")
    return matches[0]


def _current_directory_identity(
    value: Any, expected_path: Path, context: str, *, live: bool
) -> Mapping[str, Any]:
    record = _semantic_exact(value, CURRENT_DIRECTORY_IDENTITY_FIELDS, context)
    if record["path"] != str(expected_path):
        raise ValueError(f"{context} path differs")
    for field in CURRENT_DIRECTORY_IDENTITY_FIELDS - {"path"}:
        if not _semantic_integer(record[field]):
            raise ValueError(f"{context} metadata differs")
    if live:
        metadata = expected_path.lstat()
        expected = {
            "changed_ns": metadata.st_ctime_ns, "device": metadata.st_dev,
            "file_type": stat.S_IFMT(metadata.st_mode),
            "inode": metadata.st_ino, "link_count": metadata.st_nlink,
            "modified_ns": metadata.st_mtime_ns, "path": str(expected_path),
            "permissions": stat.S_IMODE(metadata.st_mode),
            "size": metadata.st_size,
        }
        if record != expected:
            raise ValueError(f"{context} live identity differs")
    return record


def _current_final_bound_directory(
    recorded: Mapping[str, Any], path: Path, context: str
) -> None:
    metadata = path.lstat()
    if (
        path.resolve(strict=True) != path
        or not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o555
        or recorded["path"] != str(path)
        or recorded["device"] != metadata.st_dev
        or recorded["inode"] != metadata.st_ino
        or recorded["file_type"] != stat.S_IFMT(metadata.st_mode)
    ):
        raise ValueError(f"{context} final frozen directory differs")


def _current_materialized_manifest(root: Path, context: str) -> dict[str, Any]:
    if root.resolve(strict=True) != root:
        raise ValueError(f"{context} root is not canonical")
    entries: list[dict[str, Any]] = []
    for path in (root, *sorted(root.rglob("*"))):
        metadata = path.lstat()
        if path.resolve(strict=True) != path or stat.S_ISLNK(metadata.st_mode):
            raise ValueError(f"{context} contains an aliased path")
        if stat.S_ISDIR(metadata.st_mode):
            file_type = "directory"
            digest = None
            if stat.S_IMODE(metadata.st_mode) != 0o555:
                raise ValueError(f"{context} directory is not frozen")
        elif stat.S_ISREG(metadata.st_mode):
            file_type = "regular"
            digest = sha256_file(path)
            if metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) not in {
                0o444, 0o555,
            }:
                raise ValueError(f"{context} file is not frozen and exclusive")
        else:
            raise ValueError(f"{context} contains an unsupported node")
        entries.append({
            "changed_ns": metadata.st_ctime_ns,
            "device": metadata.st_dev,
            "file_type": file_type,
            "inode": metadata.st_ino,
            "link_count": metadata.st_nlink,
            "modified_ns": metadata.st_mtime_ns,
            "path": "." if path == root else path.relative_to(root).as_posix(),
            "permissions": stat.S_IMODE(metadata.st_mode),
            "sha256": digest,
            "size": metadata.st_size,
        })
    return {"entries": entries, "schema": "bn-30fs-file-manifest-v2"}


def _current_cargo_hardlink_paths(root: Path, context: str) -> set[Path]:
    layouts = {
        "children": (
            "asterism_rebaseline_current_correctness",
            "asterism_rebaseline_current_fault",
        ),
        "hooked-release": ("asterism_rebaseline_public",),
        "pristine-release": ("asterism_rebaseline_public",),
    }
    allowed = set()
    for directory, examples in layouts.items():
        examples_root = root / "targets" / directory / "release" / "examples"
        try:
            entries = tuple(examples_root.iterdir())
        except OSError as error:
            raise ValueError(f"{context} Cargo examples directory differs") from error
        for example in examples:
            primary = examples_root / example
            hashed = [
                path
                for path in entries
                if re.fullmatch(rf"{re.escape(example)}-[0-9a-f]{{16}}", path.name)
                is not None
            ]
            if len(hashed) != 1:
                raise ValueError(f"{context} Cargo artifact alias topology differs")
            aliases = (primary, hashed[0])
            metadata = [path.lstat() for path in aliases]
            if (
                any(
                    path.resolve(strict=True) != path
                    or not stat.S_ISREG(value.st_mode)
                    or value.st_nlink != 2
                    or stat.S_IMODE(value.st_mode) != 0o555
                    for path, value in zip(aliases, metadata, strict=True)
                )
                or len({(value.st_dev, value.st_ino) for value in metadata}) != 1
            ):
                raise ValueError(f"{context} Cargo artifact hard-link identity differs")
            allowed.update(aliases)
    return allowed


def _current_final_frozen_tree(root: Path, context: str) -> None:
    if root.resolve(strict=True) != root:
        raise ValueError(f"{context} root is not canonical")
    cargo_hardlinks = _current_cargo_hardlink_paths(root, context)
    for path in (root, *sorted(root.rglob("*"))):
        metadata = path.lstat()
        mode = stat.S_IMODE(metadata.st_mode)
        if path.resolve(strict=True) != path or stat.S_ISLNK(metadata.st_mode):
            raise ValueError(f"{context} contains an aliased path")
        if stat.S_ISDIR(metadata.st_mode):
            if mode != 0o555:
                raise ValueError(f"{context} directory is not frozen: {path}")
        elif stat.S_ISREG(metadata.st_mode):
            expected_links = 2 if path in cargo_hardlinks else 1
            if metadata.st_nlink != expected_links or mode not in {0o444, 0o555}:
                raise ValueError(f"{context} file is not frozen: {path}")
        else:
            raise ValueError(f"{context} contains an unsupported node")


def _current_materialized_manifest_sidecar(
    current_root: Path, directory: str, source_root: Path, expected_sha256: str,
    context: str,
) -> Mapping[str, Any]:
    path = current_root / "manifests" / f"materialized-{directory}.json"
    snapshot = schema.snapshot_regular_file(path, expected_mode=0o444)
    value = schema.parse_canonical_json_object(snapshot.data, context)
    if (
        snapshot.sha256 != expected_sha256
        or value != _current_materialized_manifest(source_root, context + " live")
    ):
        raise ValueError(f"{context} exact live replay differs")
    return value


def _current_tool_record(
    value: Any,
    expected_path: Path,
    expected_sha256: str | None,
    context: str,
    *,
    trusted: bool,
    live_system: bool,
) -> Mapping[str, Any]:
    record = _semantic_exact(value, CURRENT_TOOL_RECORD_FIELDS, context)
    identity = _current_live_file_identity(record["identity"], context + " identity")
    if (
        identity["path"] != str(expected_path)
        or (expected_sha256 is not None and identity["sha256"] != expected_sha256)
        or identity["mode"] & 0o111 == 0
        or record["trusted_system"] is not trusted
    ):
        raise ValueError(f"{context} binding differs")
    chain = record["path_chain"]
    if not trusted:
        if chain is not None:
            raise ValueError(f"{context} unexpected trusted path chain")
        return record
    if not isinstance(chain, list) or not chain:
        raise ValueError(f"{context} trusted path chain is absent")
    paths = [Path("/"), *list(expected_path.parents)[::-1][1:], expected_path]
    if [entry.get("path") for entry in chain if isinstance(entry, Mapping)] != [
        str(path) for path in paths
    ]:
        raise ValueError(f"{context} trusted path chain differs")
    for raw, path in zip(chain, paths, strict=True):
        item = _semantic_exact(raw, CURRENT_TRUSTED_CHAIN_FIELDS, context + " chain")
        metadata = path.lstat()
        expected = {
            "changed_ns": metadata.st_ctime_ns, "device": metadata.st_dev,
            "gid": metadata.st_gid, "inode": metadata.st_ino,
            "link_count": metadata.st_nlink,
            "mode": stat.S_IMODE(metadata.st_mode),
            "modified_ns": metadata.st_mtime_ns, "path": str(path),
            "size": metadata.st_size, "type": stat.S_IFMT(metadata.st_mode),
            "uid": metadata.st_uid,
        }
        if item["path"] != str(path) or (
            live_system
            and (
                item != expected
                or item["uid"] != 0
                or item["mode"] & 0o022
            )
        ):
            raise ValueError(f"{context} trusted path chain live identity differs")
    return record


def _null_device_record(value: Any, context: str) -> Mapping[str, Any]:
    record = _semantic_exact(value, CURRENT_DEVICE_RECORD_FIELDS, context)
    identity = _semantic_exact(
        record["identity"], CURRENT_DEVICE_IDENTITY_FIELDS, context + " identity"
    )
    path = Path("/dev/null")
    metadata = path.lstat()
    expected = {
        "changed_ns": metadata.st_ctime_ns,
        "device": metadata.st_dev,
        "gid": metadata.st_gid,
        "inode": metadata.st_ino,
        "link_count": metadata.st_nlink,
        "major": os.major(metadata.st_rdev),
        "minor": os.minor(metadata.st_rdev),
        "modified_ns": metadata.st_mtime_ns,
        "path": str(path),
        "permissions": stat.S_IMODE(metadata.st_mode),
        "size": metadata.st_size,
        "type": stat.S_IFMT(metadata.st_mode),
        "uid": metadata.st_uid,
    }
    if (
        any(
            not _semantic_integer(identity[field])
            for field in CURRENT_DEVICE_IDENTITY_FIELDS - {"path"}
        )
        or identity != expected
        or record["trusted_system"] is not True
        or not stat.S_ISCHR(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o666
        or metadata.st_nlink != 1
        or os.major(metadata.st_rdev) != 1
        or os.minor(metadata.st_rdev) != 3
    ):
        raise ValueError(f"{context} null-device identity differs")
    chain = record["parent_path_chain"]
    paths = (Path("/"), Path("/dev"))
    if not isinstance(chain, list) or len(chain) != len(paths):
        raise ValueError(f"{context} null-device parent chain differs")
    for raw, selected in zip(chain, paths, strict=True):
        item = _semantic_exact(
            raw, CURRENT_TRUSTED_CHAIN_FIELDS, context + " parent chain"
        )
        selected_metadata = selected.lstat()
        if any(
            not _semantic_integer(item[field])
            for field in CURRENT_TRUSTED_CHAIN_FIELDS - {"path"}
        ) or item != {
            "changed_ns": selected_metadata.st_ctime_ns,
            "device": selected_metadata.st_dev,
            "gid": selected_metadata.st_gid,
            "inode": selected_metadata.st_ino,
            "link_count": selected_metadata.st_nlink,
            "mode": stat.S_IMODE(selected_metadata.st_mode),
            "modified_ns": selected_metadata.st_mtime_ns,
            "path": str(selected),
            "size": selected_metadata.st_size,
            "type": stat.S_IFMT(selected_metadata.st_mode),
            "uid": selected_metadata.st_uid,
        }:
            raise ValueError(f"{context} null-device parent chain differs")
    return record


def validate_prepared_execution_tools(
    value: Any,
    toolchain: Mapping[str, Any],
    context: str,
    problems: Problems,
) -> str | None:
    try:
        tools = _semantic_exact(
            value,
            {"bwrap", "cargo", "dev_null", "rustc", "rust_lld", "toolchain_root"},
            context,
        )
        rustc_host = toolchain.get("rustc_host")
        if (
            not isinstance(rustc_host, str)
            or re.fullmatch(r"[A-Za-z0-9_-]+", rustc_host) is None
        ):
            raise ValueError(f"{context} rustc host differs")
        toolchain_root = Path(str(toolchain["cargo_path"])).parent.parent
        expected_files = {
            "bwrap": (
                Path(str(toolchain["bwrap_path"])),
                toolchain["bwrap_sha256"],
            ),
            "cargo": (
                Path(str(toolchain["cargo_path"])),
                toolchain["cargo_sha256"],
            ),
            "rustc": (
                Path(str(toolchain["rustc_path"])),
                toolchain["rustc_sha256"],
            ),
            "rust_lld": (
                Path(str(toolchain["rust_lld_path"])),
                toolchain["rust_lld_sha256"],
            ),
        }
        expected_rust_lld = (
            toolchain_root
            / "lib"
            / "rustlib"
            / rustc_host
            / "bin"
            / "rust-lld"
        )
        if expected_files["rust_lld"][0] != expected_rust_lld:
            raise ValueError(f"{context} rust-lld topology differs")
        for name, (path, expected_sha256) in expected_files.items():
            binding = _semantic_exact(
                tools[name], {"identity", "mode", "path", "sha256", "size"},
                f"{context} {name}",
            )
            identity = _semantic_exact(
                binding["identity"],
                {"changed_ns", "device", "inode", "link_count", "modified_ns"},
                f"{context} {name} identity",
            )
            snapshot = schema.snapshot_regular_file(path, expected_mode=None)
            metadata = snapshot.stat()
            if (
                any(
                    not _semantic_integer(identity[field]) for field in identity
                )
                or not _semantic_integer(binding["mode"])
                or not _semantic_integer(binding["size"])
                or binding["path"] != str(path)
                or binding["sha256"] != expected_sha256
                or binding["sha256"] != snapshot.sha256
                or binding["size"] != snapshot.size
                or binding["mode"] != snapshot.mode
                or snapshot.mode & 0o111 == 0
                or metadata.st_nlink != 1
                or identity
                != {
                    "changed_ns": metadata.st_ctime_ns,
                    "device": metadata.st_dev,
                    "inode": metadata.st_ino,
                    "link_count": metadata.st_nlink,
                    "modified_ns": metadata.st_mtime_ns,
                }
            ):
                raise ValueError(f"{context} {name} binding differs")
        _null_device_record(tools["dev_null"], f"{context} dev-null")
        root = _semantic_exact(
            tools["toolchain_root"], {"device", "inode", "link_count", "mode"},
            f"{context} toolchain root",
        )
        root_metadata = toolchain_root.lstat()
        if any(
            not _semantic_integer(root[field]) for field in root
        ) or root != {
            "device": root_metadata.st_dev,
            "inode": root_metadata.st_ino,
            "link_count": root_metadata.st_nlink,
            "mode": stat.S_IMODE(root_metadata.st_mode),
        }:
            raise ValueError(f"{context} toolchain-root binding differs")
    except (KeyError, OSError, ValueError) as error:
        problems.add(str(error))
        return None
    return hashlib.sha256(
        schema.prepared_authority_canonical_json_bytes(tools)
    ).hexdigest()


def _current_config_record(
    value: Any,
    semantic_authority: Mapping[str, Any],
    source_root: Path,
    cargo_home_root: Path,
    context: str,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    record = _semantic_exact(value, CURRENT_CARGO_CONFIG_FIELDS, context)
    search = _semantic_exact(
        record["cargo_search"],
        {"cargo_home_path", "cwd", "entries", "schema"},
        context + " Cargo search",
    )
    candidates = (
        f"{GUEST_SOURCE}/.cargo/config.toml", f"{GUEST_SOURCE}/.cargo/config",
        f"{GUEST_ROOT}/.cargo/config.toml", f"{GUEST_ROOT}/.cargo/config",
        "/.cargo/config.toml", "/.cargo/config",
        f"{GUEST_CARGO_HOME}/config.toml", f"{GUEST_CARGO_HOME}/config",
    )
    entries = search["entries"]
    if (
        record["schema"] != "bn-30fs-build-cargo-config-search-v1"
        or search["schema"] != schema.CARGO_CONFIG_SEARCH_SCHEMA
        or search["cargo_home_path"] != GUEST_CARGO_HOME
        or search["cwd"] != GUEST_SOURCE
        or not isinstance(entries, list)
        or len(entries) != len(candidates)
    ):
        raise ValueError(f"{context} identity differs")
    hosts: tuple[Path | None, ...] = (
        source_root / ".cargo/config.toml", source_root / ".cargo/config",
        None, None, None, None,
        cargo_home_root / "config.toml", cargo_home_root / "config",
    )
    for raw, path, host in zip(entries, candidates, hosts, strict=True):
        entry = _semantic_exact(
            raw, {"path", "sha256", "status"}, context + " entry"
        )
        if (
            entry["path"] != path
            or entry["status"] not in {"absent", "present"}
            or (entry["status"] == "absent") != (entry["sha256"] is None)
            or (entry["status"] == "present" and not is_sha256(entry["sha256"]))
            or (path in candidates[2:6] and entry["status"] != "absent")
        ):
            raise ValueError(f"{context} Cargo config entry differs")
        if host is not None:
            try:
                snapshot = schema.snapshot_regular_file(host, expected_mode=None)
            except FileNotFoundError:
                expected_sha256 = EMPTY_SHA256
            except (OSError, ValueError) as error:
                raise ValueError(
                    f"{context} live Cargo config entry is not exact regular"
                ) from error
            else:
                if snapshot._stat.st_nlink != 1:
                    raise ValueError(
                        f"{context} live Cargo config entry is not exact regular"
                    )
                expected_sha256 = snapshot.sha256
            if (
                path not in GUEST_BOUND_CONFIG_PATHS
                or entry["status"] != "present"
                or entry["sha256"] != expected_sha256
            ):
                raise ValueError(f"{context} live Cargo config entry differs")
    cargo_home_tree = _semantic_exact(
        record["cargo_home_tree"], CURRENT_CARGO_HOME_TREE_FIELDS,
        context + " Cargo-home tree",
    )
    semantic_cargo_home = semantic_authority["cargo_home"]
    expected_tree = {
        "entry_count": semantic_cargo_home["entry_count"],
        "equal_pre_post": True,
        "path": semantic_cargo_home["manifest_path"],
        "post_sha256": semantic_cargo_home["manifest_sha256"],
        "pre_sha256": semantic_cargo_home["manifest_sha256"],
        "watch_count": semantic_cargo_home["watch_count"],
    }
    if cargo_home_tree != expected_tree:
        raise ValueError(f"{context} Cargo-home semantic crosslink differs")
    preserved = _semantic_exact(
        record["preserved_top_level_entries"], {"cargo-home", "source"},
        context + " preserved entries",
    )
    for origin in ("source", "cargo-home"):
        values = preserved[origin]
        if not isinstance(values, list):
            raise ValueError(f"{context} preserved {origin} differs")
        base = (
            source_root / ".cargo"
            if origin == "source"
            else cargo_home_root
        )
        live_entries = [
            path
            for path in sorted(base.iterdir(), key=lambda item: item.name)
            if path.name not in {"config", "config.toml"}
        ]
        if any(
            path.is_symlink() or not (path.is_file() or path.is_dir())
            for path in live_entries
        ):
            raise ValueError(f"{context} preserved {origin} live topology differs")
        names = []
        for raw in values:
            entry = _semantic_exact(
                raw, {"identity", "name", "type"}, context + " preserved entry"
            )
            if (
                not isinstance(entry["name"], str)
                or not entry["name"]
                or "/" in entry["name"]
                or entry["name"] in {"config", "config.toml"}
                or entry["type"] not in {"directory", "regular"}
            ):
                raise ValueError(f"{context} preserved entry differs")
            names.append(entry["name"])
            expected_path = base / entry["name"]
            if entry["type"] == "regular":
                _current_live_file_identity(
                    entry["identity"], context + " preserved file"
                )
                if entry["identity"]["path"] != str(expected_path):
                    raise ValueError(f"{context} preserved file path differs")
            else:
                _current_directory_identity(
                    entry["identity"], expected_path,
                    context + " preserved directory", live=True,
                )
        if (
            names != sorted(names)
            or len(names) != len(set(names))
            or names != [path.name for path in live_entries]
        ):
            raise ValueError(f"{context} preserved entry order differs")
    return search, preserved


_CURRENT_PATCH_HUNK = re.compile(
    r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(?: .*)?$"
)


def _current_apply_product_overlay(
    base: bytes, patch: bytes, context: str
) -> bytes:
    try:
        source = base.decode("utf-8")
        patch_text = patch.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError(f"{context} is not UTF-8") from error
    expected = CURRENT_ENGINE_PATH.as_posix()
    if (
        re.findall(r"^diff --git a/(.+) b/(.+)$", patch_text, re.MULTILINE)
        != [(expected, expected)]
        or re.findall(r"^--- a/(.+)$", patch_text, re.MULTILINE)
        != [expected]
        or re.findall(r"^\+\+\+ b/(.+)$", patch_text, re.MULTILINE)
        != [expected]
    ):
        raise ValueError(f"{context} exact path topology differs")
    patch_lines = patch_text.splitlines(keepends=True)
    source_lines = source.splitlines(keepends=True)
    output: list[str] = []
    cursor = 0
    new_cursor = 0
    hunk_count = 0
    index = 0
    while index < len(patch_lines):
        match = _CURRENT_PATCH_HUNK.match(patch_lines[index].rstrip("\n"))
        if match is None:
            index += 1
            continue
        hunk_count += 1
        old_start = int(match.group(1))
        old_count = int(match.group(2) or "1")
        new_start = int(match.group(3))
        new_count = int(match.group(4) or "1")
        old_index = old_start if old_count == 0 else old_start - 1
        expected_new = new_start if new_count == 0 else new_start - 1
        if old_index < cursor or new_cursor + old_index - cursor != expected_new:
            raise ValueError(f"{context} hunk coordinates differ")
        output.extend(source_lines[cursor:old_index])
        new_cursor += old_index - cursor
        cursor = old_index
        index += 1
        consumed = 0
        produced = 0
        while index < len(patch_lines):
            line = patch_lines[index]
            if _CURRENT_PATCH_HUNK.match(line.rstrip("\n")) or line.startswith(
                "diff --git "
            ):
                break
            if not line.startswith((" ", "+", "-", "\\")):
                break
            index += 1
            if line.startswith("\\"):
                continue
            marker, payload = line[0], line[1:]
            if marker in {" ", "-"}:
                if cursor >= len(source_lines) or source_lines[cursor] != payload:
                    raise ValueError(f"{context} hunk context differs")
                cursor += 1
                consumed += 1
            if marker in {" ", "+"}:
                output.append(payload)
                new_cursor += 1
                produced += 1
        if consumed != old_count or produced != new_count:
            raise ValueError(f"{context} hunk cardinality differs")
    if hunk_count == 0:
        raise ValueError(f"{context} has no unified hunks")
    output.extend(source_lines[cursor:])
    return "".join(output).encode()


def _current_archive_nodes(
    archive: bytes, context: str
) -> dict[str, tuple[str, int, bytes | None]]:
    nodes: dict[str, tuple[str, int, bytes | None]] = {}
    members_seen: set[str] = set()

    def add_directory(path: PurePosixPath) -> None:
        if not path.parts:
            return
        rendered = path.as_posix()
        existing = nodes.get(rendered)
        if existing is not None and existing[0] != "directory":
            raise ValueError(f"{context} file/directory topology overlaps")
        nodes[rendered] = ("directory", 0o555, None)
        add_directory(path.parent)

    try:
        source = tarfile.open(fileobj=io.BytesIO(archive), mode="r:")
    except tarfile.TarError as error:
        raise ValueError(f"{context} is not an exact tar archive") from error
    with source:
        try:
            members = source.getmembers()
        except tarfile.TarError as error:
            raise ValueError(f"{context} is not an exact tar archive") from error
        logical_end = source.offset
        tar_block = 512
        end_markers = b"\0" * (2 * tar_block)
        if (
            logical_end < 0
            or logical_end % tar_block
            or len(archive) % tar_block
            or archive[logical_end : logical_end + len(end_markers)]
            != end_markers
            or any(archive[logical_end + len(end_markers) :])
        ):
            raise ValueError(f"{context} trailing payload differs")
        for member in members:
            raw = (
                member.name[:-1]
                if member.isdir() and member.name.endswith("/")
                else member.name
            )
            path = PurePosixPath(raw)
            if (
                not raw
                or path.is_absolute()
                or "\\" in raw
                or any(part in {"", ".", ".."} for part in path.parts)
                or path.as_posix() != raw
                or raw in members_seen
            ):
                raise ValueError(f"{context} member path differs")
            members_seen.add(raw)
            add_directory(path.parent)
            if member.isdir():
                add_directory(path)
                continue
            if not member.isfile():
                raise ValueError(f"{context} member kind differs")
            if raw in nodes:
                raise ValueError(f"{context} file/directory topology overlaps")
            extracted = source.extractfile(member)
            if extracted is None:
                raise ValueError(f"{context} member payload is absent")
            payload = extracted.read()
            if len(payload) != member.size:
                raise ValueError(f"{context} member size differs")
            nodes[raw] = (
                "regular", 0o555 if member.mode & 0o111 else 0o444, payload
            )
    for path, node in nodes.items():
        parts = PurePosixPath(path).parts
        if any(
            nodes.get(PurePosixPath(*parts[:index]).as_posix(), ("", 0, None))[0]
            == "regular"
            for index in range(1, len(parts))
        ):
            raise ValueError(f"{context} member descends from a file")
        if node[0] == "regular" and node[2] is None:
            raise ValueError(f"{context} regular payload is absent")
    return nodes


def _current_replay_materialization(
    *,
    archive: bytes,
    lock_payload: bytes,
    placements: Sequence[Mapping[str, Any]],
    root: Path,
    patch_payload: bytes,
    overlay: bool,
    context: str,
) -> None:
    nodes = _current_archive_nodes(archive, context + " archive")

    def add_parents(relative: PurePosixPath) -> None:
        parent = relative.parent
        while parent.parts:
            rendered = parent.as_posix()
            existing = nodes.get(rendered)
            if existing is not None and existing[0] != "directory":
                raise ValueError(f"{context} destination parent is a file")
            nodes[rendered] = ("directory", 0o555, None)
            parent = parent.parent

    lock_name = PurePosixPath("Cargo.lock")
    if nodes.get(lock_name.as_posix(), ("", 0, None))[0] != "regular":
        raise ValueError(f"{context} archived Cargo.lock is absent")
    nodes[lock_name.as_posix()] = ("regular", 0o444, lock_payload)
    for placement in placements:
        destination = Path(str(placement["destination"]))
        try:
            relative_path = destination.relative_to(root)
        except ValueError as error:
            raise ValueError(f"{context} placement escapes materialization") from error
        relative = PurePosixPath(relative_path.as_posix())
        if relative.as_posix() in nodes:
            raise ValueError(f"{context} placement was not a fresh destination")
        source = schema.snapshot_regular_file(Path(str(placement["source"])))
        if (
            placement["mode"] != 0o444
            or placement["sha256"] != source.sha256
        ):
            raise ValueError(f"{context} placement source differs")
        add_parents(relative)
        nodes[relative.as_posix()] = ("regular", 0o444, source.data)
    engine_name = PurePosixPath(CURRENT_ENGINE_PATH.as_posix())
    engine = nodes.get(engine_name.as_posix())
    if (
        engine is None
        or engine[0] != "regular"
        or engine[2] is None
        or hashlib.sha256(engine[2]).hexdigest()
        != CURRENT_PRODUCT_ENGINE_SHA256
    ):
        raise ValueError(f"{context} frozen archive engine differs")
    if overlay:
        nodes[engine_name.as_posix()] = (
            "regular",
            engine[1],
            _current_apply_product_overlay(
                engine[2], patch_payload, context + " product overlay"
            ),
        )
    live_paths = [root, *sorted(root.rglob("*"))]
    live_names = {
        "." if path == root else path.relative_to(root).as_posix(): path
        for path in live_paths
    }
    expected_names = {".", *nodes}
    if set(live_names) != expected_names:
        raise ValueError(f"{context} live path topology differs")
    for name, path in live_names.items():
        metadata = path.lstat()
        expected = ("directory", 0o555, None) if name == "." else nodes[name]
        if (
            path.resolve(strict=True) != path
            or stat.S_IMODE(metadata.st_mode) != expected[1]
            or (expected[0] == "directory") != stat.S_ISDIR(metadata.st_mode)
            or (expected[0] == "regular") != stat.S_ISREG(metadata.st_mode)
        ):
            raise ValueError(f"{context} live node differs: {name}")
        if expected[0] == "regular":
            payload = expected[2]
            assert payload is not None
            if (
                metadata.st_nlink != 1
                or metadata.st_size != len(payload)
                or sha256_file(path) != hashlib.sha256(payload).hexdigest()
            ):
                raise ValueError(f"{context} live file differs: {name}")


def validate_current_construction(
    current: Mapping[str, Any], current_root: Path, context: str
) -> Mapping[str, str]:
    frozen_a = schema.VARIANT_SOURCE_BINDINGS["A"]
    path = current_root / "manifests" / "current-children-construction.json"
    if current.get("construction_path") != str(path):
        raise ValueError(f"{context} path differs")
    snapshot = schema.snapshot_regular_file(path, expected_mode=0o444)
    if (
        snapshot.sha256 != current.get("construction_sha256")
        or snapshot.sha256 != current.get("build_nonce")
    ):
        raise ValueError(f"{context} build nonce differs")
    value = _semantic_exact(
        schema.parse_canonical_json_object(snapshot.data, context),
        {
            "archive", "cargo_config_manifest_sha256",
            "cargo_config_view_sha256", "kinds", "lock_sha256",
            "product_commit", "product_overlay_sha256", "product_tree",
            "protocol", "schema",
        },
        context,
    )
    archive = _semantic_exact(
        value["archive"], {"bytes", "commit", "sha256", "tree"},
        context + " archive",
    )
    archive_path = current_root / "archives" / "source-A.tar"
    archive_snapshot = schema.snapshot_regular_file(archive_path, expected_mode=0o444)
    cargo_authority = current.get("cargo_config_authority")
    cargo_identity = (
        cargo_authority.get("identity")
        if isinstance(cargo_authority, Mapping) else None
    )
    translated = (
        cargo_authority.get("translated_entries")
        if isinstance(cargo_authority, Mapping) else None
    )
    overlay_authority = current.get("product_overlay_authority")
    overlay_patch = (
        overlay_authority.get("patch")
        if isinstance(overlay_authority, Mapping) else None
    )
    lock_candidates = current.get("lock_candidates")
    candidate_a = (
        lock_candidates.get("A")
        if isinstance(lock_candidates, Mapping) else None
    )
    if (
        value["schema"] != CURRENT_CONSTRUCTION_SCHEMA
        or value["protocol"] != schema.PROTOCOL
        or current.get("product_commit") != frozen_a["commit"]
        or current.get("product_tree") != frozen_a["tree"]
        or value["product_commit"] != current.get("product_commit")
        or value["product_tree"] != current.get("product_tree")
        or archive != {
            "bytes": archive_snapshot.size,
            "commit": current.get("product_commit"),
            "sha256": archive_snapshot.sha256,
            "tree": current.get("product_tree"),
        }
        or not isinstance(cargo_identity, Mapping)
        or value["cargo_config_manifest_sha256"] != cargo_identity.get("sha256")
        or not isinstance(translated, list)
        or value["cargo_config_view_sha256"]
        != hashlib.sha256(canonical_json_bytes(translated)).hexdigest()
        or value["lock_sha256"] != schema.CURRENT_LOCK_SHA256
        or not isinstance(candidate_a, Mapping)
        or value["lock_sha256"] != candidate_a.get("sha256")
        or not isinstance(overlay_patch, Mapping)
        or value["product_overlay_sha256"] != overlay_patch.get("sha256")
        or any(
            not is_sha256(value[field])
            for field in (
                "cargo_config_manifest_sha256", "cargo_config_view_sha256",
                "lock_sha256", "product_overlay_sha256",
            )
        )
    ):
        raise ValueError(f"{context} identity differs")
    kinds = _semantic_exact(
        value["kinds"], {"children", "hooked-release", "pristine-release"},
        context + " kinds",
    )
    result: dict[str, str] = {}
    try:
        fault_path = Path(str(current["fault_authority"]["source"]["path"]))
        repository = fault_path.parents[4]
    except (KeyError, IndexError, TypeError) as error:
        raise ValueError(f"{context} repository topology differs") from error
    tooling = repository / "spikes" / "asterism_rebaseline" / "tooling"
    current_dir = tooling / "current"
    patch_snapshot = schema.snapshot_regular_file(
        current_dir / "product-test-overlay.patch"
    )
    if patch_snapshot.sha256 != value["product_overlay_sha256"]:
        raise ValueError(f"{context} product overlay bytes differ")
    candidate_a_path = Path(str(candidate_a["path"]))
    candidate_a_snapshot = schema.snapshot_regular_file(
        candidate_a_path, expected_mode=0o444
    )
    if (
        candidate_a_snapshot.sha256 != value["lock_sha256"]
        or candidate_a_snapshot.sha256 != candidate_a["sha256"]
    ):
        raise ValueError(f"{context} A lock payload differs")
    expected_sources = {
        "children": [
            (current_dir / "correctness.rs", Path(
                "crates/mess-store/examples/asterism_rebaseline_current_correctness.rs"
            )),
            (current_dir / "fault.rs", Path(
                "crates/mess-store/examples/asterism_rebaseline_current_fault.rs"
            )),
            *((tooling / "overlay" / "shared" / shared,
               CURRENT_SHARED_DESTINATION / shared)
              for shared in CURRENT_SHARED_NAMES),
        ],
        "hooked-release": [
            (tooling / "overlay" / "public" / "main.rs", Path(
                "crates/mess-store/examples/asterism_rebaseline_public.rs"
            )),
            (tooling / "overlay" / "public" / "adapters" / "current.rs",
             CURRENT_ADAPTER_DESTINATION),
            *((tooling / "overlay" / "shared" / shared,
               CURRENT_SHARED_DESTINATION / shared)
              for shared in CURRENT_SHARED_NAMES),
        ],
        "pristine-release": [
            (tooling / "overlay" / "public" / "main.rs", Path(
                "crates/mess-store/examples/asterism_rebaseline_public.rs"
            )),
            (tooling / "overlay" / "public" / "adapters" / "current.rs",
             CURRENT_ADAPTER_DESTINATION),
            *((tooling / "overlay" / "shared" / shared,
               CURRENT_SHARED_DESTINATION / shared)
              for shared in CURRENT_SHARED_NAMES),
        ],
    }
    for name, directory in (
        ("children", "children"), ("hooked_release", "hooked-release"),
        ("pristine_release", "pristine-release"),
    ):
        kind = _semantic_exact(
            kinds[directory], {"manifest_sha256", "placements"},
            context + f" {directory}",
        )
        if not is_sha256(kind["manifest_sha256"]) or not isinstance(
            kind["placements"], list
        ):
            raise ValueError(f"{context} {directory} identity differs")
        manifest_path = current_root / "manifests" / f"materialized-{directory}.json"
        if schema.snapshot_regular_file(
            manifest_path, expected_mode=0o444
        ).sha256 != kind["manifest_sha256"]:
            raise ValueError(f"{context} {directory} manifest binding differs")
        expected = expected_sources[directory]
        if len(kind["placements"]) != len(expected):
            raise ValueError(f"{context} {directory} placement cardinality differs")
        for raw, (source_path, relative_destination) in zip(
            kind["placements"], expected, strict=True
        ):
            placement = _semantic_exact(
                raw, {"destination", "mode", "sha256", "source"},
                context + f" {directory} placement",
            )
            destination = (
                current_root
                / "materialized"
                / directory
                / relative_destination
            )
            source = source_path.resolve(strict=True)
            copied = schema.snapshot_regular_file(destination, expected_mode=0o444)
            if (
                placement != {
                    "destination": destination.as_posix(),
                    "mode": 0o444,
                    "sha256": sha256_file(source),
                    "source": str(source),
                }
                or copied.sha256 != placement["sha256"]
            ):
                raise ValueError(f"{context} {directory} placement differs")
        _current_replay_materialization(
            archive=archive_snapshot.data,
            lock_payload=candidate_a_snapshot.data,
            placements=kind["placements"],
            root=current_root / "materialized" / directory,
            patch_payload=patch_snapshot.data,
            overlay=directory != "pristine-release",
            context=context + f" {directory}",
        )
        result[name] = kind["manifest_sha256"]
    builds = current.get("builds")
    if not isinstance(builds, Mapping):
        raise ValueError(f"{context} build topology differs")
    for name in ("hooked_release", "pristine_release"):
        build = builds.get(name)
        environment = build.get("environment") if isinstance(build, Mapping) else None
        if (
            not isinstance(environment, Mapping)
            or environment.get("ASTERISM_BUILD_NONCE")
            != current.get("build_nonce")
            or environment.get("ASTERISM_BUILD_CARGO_LOCK_SHA256")
            != value["lock_sha256"]
            or environment.get("ASTERISM_BUILD_PRODUCT_COMMIT")
            != value["product_commit"]
            or environment.get("ASTERISM_BUILD_PRODUCT_TREE")
            != value["product_tree"]
        ):
            raise ValueError(f"{context} {name} release environment differs")
    return result


def _current_immutable_record(
    value: Any, expected_path: Path, context: str
) -> Mapping[str, Any]:
    record = _semantic_exact(value, CURRENT_IMMUTABLE_FILE_FIELDS, context)
    identity = _semantic_exact(
        record["identity"], CURRENT_IMMUTABLE_IDENTITY_FIELDS,
        context + " identity",
    )
    exact = expected_path.resolve(strict=True)
    metadata = exact.lstat()
    expected_identity = {
        "changed_ns": metadata.st_ctime_ns, "device": metadata.st_dev,
        "inode": metadata.st_ino, "link_count": metadata.st_nlink,
        "modified_ns": metadata.st_mtime_ns,
    }
    if (
        exact != expected_path
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o444
        or identity != expected_identity
        or record["mode"] != 0o444
        or record["path"] != str(exact)
        or record["sha256"] != sha256_file(exact)
        or record["size"] != metadata.st_size
    ):
        raise ValueError(f"{context} immutable file differs")
    return record


def _current_validator_execution(
    value: Any,
    *,
    result: Mapping[str, Any],
    script_identity: Mapping[str, Any],
    repository: Path,
    self_test: bool,
    live_system: bool,
    context: str,
) -> Mapping[str, Any]:
    record = _semantic_exact(
        value, CURRENT_EXECUTION_FIELDS | {"script_authority"}, context
    )
    script_path = Path(str(script_identity["path"]))
    expected_script = {
        "identity": script_identity, "path_chain": None,
        "trusted_system": False,
    }
    python = _current_tool_record(
        record["execution_authority"], CURRENT_SYSTEM_PYTHON, None,
        context + " Python", trusted=True, live_system=live_system,
    )
    expected_environment = {
        "HOME": "/nonexistent", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1", "TZ": "UTC",
    }
    stdout = canonical_json_bytes(result)
    expected_argv = [
        str(CURRENT_SYSTEM_PYTHON), "-I", "-B", str(script_path),
        *(("--self-test",) if self_test else ()),
    ]
    if (
        record["argv"] != expected_argv
        or record["cwd"] != str(repository)
        or record["environment"] != expected_environment
        or record["execution_authority"] != python
        or record["script_authority"] != expected_script
        or record["exit_status"] != 0
        or record["passed_file_descriptors"] != 2
        or record["stderr_bytes"] != 0
        or record["stderr_sha256"] != EMPTY_SHA256
        or record["stdout_bytes"] != len(stdout)
        or record["stdout_sha256"]
        != hashlib.sha256(stdout).hexdigest()
    ):
        raise ValueError(f"{context} exact execution differs")
    return record


def _current_validator_authority(
    value: Any,
    *,
    repository: Path,
    script_path: Path,
    schema_name: str,
    source_path: Path | None,
    normal_hostile_zero: bool,
    live_system: bool,
    context: str,
) -> Mapping[str, Any]:
    fields = {"executions", "normal", "self_test", "validator"}
    if source_path is not None:
        fields.add("source")
    record = _semantic_exact(value, fields, context)
    validator = _current_live_file_identity(
        record["validator"], context + " validator"
    )
    if validator["path"] != str(script_path):
        raise ValueError(f"{context} validator path differs")
    if source_path is not None:
        source = _current_live_file_identity(record["source"], context + " source")
        if source["path"] != str(source_path):
            raise ValueError(f"{context} source path differs")
    outputs = []
    for name in ("normal", "self_test"):
        output = _semantic_exact(
            record[name],
            {"checks", "hostile_mutations_rejected", "schema", "status"},
            context + f" {name}",
        )
        checks = output["checks"]
        hostile = output["hostile_mutations_rejected"]
        if (
            output["schema"] != schema_name
            or output["status"] != "ok"
            or not isinstance(checks, list)
            or not checks
            or any(not isinstance(check, str) or not check for check in checks)
            or len(checks) != len(set(checks))
            or not _semantic_integer(hostile)
            or hostile < 0
        ):
            raise ValueError(f"{context} {name} result differs")
        outputs.append(output)
    if (
        outputs[0]["checks"] != outputs[1]["checks"]
        or (normal_hostile_zero and outputs[0]["hostile_mutations_rejected"] != 0)
        or outputs[1]["hostile_mutations_rejected"] <= 0
    ):
        raise ValueError(f"{context} normal/self-test relation differs")
    executions = record["executions"]
    if not isinstance(executions, list) or len(executions) != 2:
        raise ValueError(f"{context} executions differ")
    for index, execution in enumerate(executions):
        _current_validator_execution(
            execution,
            result=outputs[index],
            script_identity=validator,
            repository=repository,
            self_test=index == 1,
            live_system=live_system,
            context=context + f" execution {index}",
        )
    return record


def _current_final_tools(
    current: Mapping[str, Any], current_root: Path, context: str
) -> Mapping[str, Any]:
    path = current_root / "asterism-rebaseline-tools.json"
    snapshot = schema.snapshot_regular_file(path, expected_mode=0o444)
    value = _semantic_exact(
        schema.parse_prepared_authority_json_object(snapshot.data, context),
        set(schema.TOOLS_MANIFEST_FIELDS), context,
    )
    if (
        current.get("tools_manifest_path") != str(path)
        or current.get("tools_manifest_sha256") != snapshot.sha256
        or value["schema"] != schema.TOOLS_MANIFEST_SCHEMA
        or value["comm_allowlist"] != schema.expected_comm_allowlist()
    ):
        raise ValueError(f"{context} binding differs")
    tools = _semantic_exact(
        value["tools"], set(schema.PREPARED_TOOL_NAMES), context + " tools"
    )
    support = _semantic_exact(
        value["support_files"], set(schema.PREPARED_SUPPORT_FILE_NAMES),
        context + " support",
    )
    physical: list[tuple[int, int]] = []
    for name, raw in tools.items():
        binding = _semantic_exact(
            raw, set(schema.TOOL_BINDING_FIELDS), context + f" tool {name}"
        )
        tool_path = Path(str(binding["path"]))
        tool = schema.snapshot_regular_file(tool_path, expected_mode=0o555)
        metadata = tool_path.stat()
        if (
            binding["path"] != str(tool_path.resolve(strict=True))
            or binding["sha256"] != tool.sha256
            or binding["executable_mode"] != 0o555
            or binding["comm"] != schema.PREPARED_TOOL_COMMS[name]
        ):
            raise ValueError(f"{context} tool {name} differs")
        physical.append((metadata.st_dev, metadata.st_ino))
    for name, raw in support.items():
        binding = _semantic_exact(
            raw, set(schema.SUPPORT_FILE_FIELDS), context + f" support {name}"
        )
        support_path = Path(str(binding["path"]))
        item = schema.snapshot_regular_file(support_path, expected_mode=0o444)
        metadata = support_path.stat()
        if (
            binding["path"] != str(support_path.resolve(strict=True))
            or binding["sha256"] != item.sha256
            or binding["mode"] != 0o444
        ):
            raise ValueError(f"{context} support {name} differs")
        physical.append((metadata.st_dev, metadata.st_ino))
    if len(physical) != len(set(physical)):
        raise ValueError(f"{context} files physically alias")
    if current.get("artifacts") != {
        name: tools[name] for name in ("correctness", "fault")
    }:
        raise ValueError(f"{context} child artifact crosslink differs")
    return value


def _current_final_tools_inheritance(
    base: Mapping[str, Any],
    final: Mapping[str, Any],
    artifacts: Any,
    context: str,
) -> None:
    if not isinstance(artifacts, Mapping) or set(artifacts) != {
        "correctness", "fault",
    }:
        raise ValueError(f"{context} child artifacts differ")
    expected = copy.deepcopy(base)
    tools = expected.get("tools")
    if not isinstance(tools, dict):
        raise ValueError(f"{context} base tools differ")
    for name in ("correctness", "fault"):
        tools[name] = copy.deepcopy(artifacts[name])
    if final != expected:
        raise ValueError(f"{context} exact base reconstruction differs")


def validate_current_top_authorities(
    current: Mapping[str, Any], current_root: Path,
    toolchain: Mapping[str, Any], replay: SemanticReplay,
) -> None:
    fault_raw = _semantic_exact(
        current.get("fault_authority"),
        {"executions", "normal", "self_test", "source", "validator"},
        "semantic current fault authority",
    )
    fault_source = _current_live_file_identity(
        fault_raw["source"], "semantic current fault authority source"
    )
    fault_path = Path(str(fault_source["path"]))
    try:
        repository = fault_path.parents[4]
    except IndexError as error:
        raise ValueError("semantic current repository topology differs") from error
    current_dir = repository / "spikes" / "asterism_rebaseline" / "tooling" / "current"
    tooling = current_dir.parent
    if fault_path != current_dir / "fault.rs":
        raise ValueError("semantic current fault source topology differs")
    _current_validator_authority(
        fault_raw, repository=repository,
        script_path=current_dir / "validate_fault.py",
        schema_name="bn-20be-current-fault-validator-v1",
        source_path=fault_path, normal_hostile_zero=False,
        live_system=replay.live_system, context="semantic current fault authority",
    )
    _current_validator_authority(
        current.get("static_authority"), repository=repository,
        script_path=current_dir / "validate_build_children.py",
        schema_name="bn-30fs-build-children-validator-v1",
        source_path=None, normal_hostile_zero=True,
        live_system=replay.live_system, context="semantic current static authority",
    )

    lock_authority = current.get("lock_authority")
    embedded_lock = (
        lock_authority.get("lock_manifest")
        if isinstance(lock_authority, Mapping) else None
    )
    lock_payload = (
        embedded_lock.get("payload")
        if isinstance(embedded_lock, Mapping) else None
    )
    lock_variants = (
        lock_payload.get("variants")
        if isinstance(lock_payload, Mapping) else None
    )
    if (
        not isinstance(lock_variants, Mapping)
        or set(lock_variants) != set(schema.VARIANTS)
    ):
        raise ValueError("semantic current embedded lock variants differ")
    reviewed_a = lock_variants["A"]
    reviewed_resolver = (
        reviewed_a.get("resolver") if isinstance(reviewed_a, Mapping) else None
    )
    reviewed_config_binding = (
        reviewed_resolver.get("cargo_config_search")
        if isinstance(reviewed_resolver, Mapping) else None
    )

    cargo = _semantic_exact(
        current.get("cargo_config_authority"),
        {"binding", "identity", "recorded", "translated_entries"},
        "semantic current Cargo config authority",
    )
    binding = _semantic_exact(
        cargo["binding"], {"path", "sha256"},
        "semantic current Cargo config binding",
    )
    config_identity = _current_live_file_identity(
        cargo["identity"], "semantic current Cargo config identity"
    )
    config_path = Path(str(config_identity["path"]))
    config_snapshot = schema.snapshot_regular_file(
        config_path, expected_mode=0o444
    )
    config_value = schema.parse_prepared_authority_json_object(
        config_snapshot.data, "semantic current Cargo config manifest"
    )
    recorded = _semantic_exact(
        cargo["recorded"], {"cargo_home_path", "cwd", "entries", "schema"},
        "semantic current Cargo config recorded",
    )
    translated = cargo["translated_entries"]
    guest_paths = (
        f"{GUEST_SOURCE}/.cargo/config.toml", f"{GUEST_SOURCE}/.cargo/config",
        f"{GUEST_ROOT}/.cargo/config.toml", f"{GUEST_ROOT}/.cargo/config",
        "/.cargo/config.toml", "/.cargo/config",
        f"{GUEST_CARGO_HOME}/config.toml", f"{GUEST_CARGO_HOME}/config",
    )
    if (
        not isinstance(reviewed_config_binding, Mapping)
        or binding != reviewed_config_binding
        or binding != {"path": str(config_path), "sha256": config_snapshot.sha256}
        or config_identity["mode"] != 0o444
        or config_value != recorded
        or recorded["schema"] != schema.CARGO_CONFIG_SEARCH_SCHEMA
        or recorded["cargo_home_path"] != GUEST_CARGO_HOME
        or recorded["cwd"] != GUEST_SOURCE
        or not isinstance(recorded["entries"], list)
        or len(recorded["entries"]) != 8
        or not isinstance(translated, list)
        or translated != recorded["entries"]
    ):
        raise ValueError("semantic current Cargo config authority differs")
    recorded_entries = []
    for raw, guest in zip(recorded["entries"], guest_paths, strict=True):
        entry = _semantic_exact(
            raw, {"path", "sha256", "status"},
            "semantic current recorded Cargo config entry",
        )
        if (
            entry["path"] != guest
            or entry["status"] not in {"absent", "present"}
            or (entry["status"] == "absent") != (entry["sha256"] is None)
            or (entry["status"] == "present" and not is_sha256(entry["sha256"]))
            or (
                guest in guest_paths[2:6]
                and (entry["status"] != "absent" or entry["sha256"] is not None)
            )
            or (
                guest in GUEST_BOUND_CONFIG_PATHS
                and entry["status"] != "present"
            )
        ):
            raise ValueError("semantic current recorded Cargo config differs")
        recorded_entries.append(entry)
    for entry, guest in zip(translated, guest_paths, strict=True):
        exact = _semantic_exact(
            entry, {"path", "sha256", "status"},
            "semantic current translated Cargo config entry",
        )
        if (
            exact["path"] != guest
            or exact["status"] not in {"absent", "present"}
            or (exact["status"] == "absent") != (exact["sha256"] is None)
            or (exact["status"] == "present" and not is_sha256(exact["sha256"]))
        ):
            raise ValueError("semantic current translated Cargo config differs")
    builds = current.get("builds")
    if not isinstance(builds, Mapping) or any(
        build.get("cargo_config_prebuild", {}).get("cargo_search", {}).get("entries")
        != translated
        for build in builds.values() if isinstance(build, Mapping)
    ):
        raise ValueError("semantic current Cargo config build crosslink differs")

    lock_inputs = _semantic_exact(
        current.get("lock_authority_inputs"),
        {"authority", "lock_manifest", "review_bundle"},
        "semantic current lock authority inputs",
    )
    lock_input_snapshots = {}
    for name in ("authority", "lock_manifest", "review_bundle"):
        raw = lock_inputs[name]
        path = Path(str(raw.get("path"))) if isinstance(raw, Mapping) else Path("/")
        _current_immutable_record(
            raw, path, f"semantic current lock authority input {name}"
        )
        lock_input_snapshots[name] = schema.snapshot_regular_file(
            path, expected_mode=0o444
        )
    exact_authority = schema.parse_prepared_authority_json_object(
        lock_input_snapshots["authority"].data,
        "semantic current exact lock authority",
    )
    exact_lock_manifest = schema.parse_prepared_authority_json_object(
        lock_input_snapshots["lock_manifest"].data,
        "semantic current exact lock manifest",
    )
    if (
        current.get("lock_manifest_sha256")
        != lock_inputs["lock_manifest"]["sha256"]
        or current.get("review_bundle_sha256")
        != lock_inputs["review_bundle"]["sha256"]
        or not isinstance(current.get("lock_authority"), Mapping)
        or current["lock_authority"].get("lock_manifest", {}).get("sha256")
        != lock_inputs["lock_manifest"]["sha256"]
        or current["lock_authority"].get("review_bundle", {}).get("sha256")
        != lock_inputs["review_bundle"]["sha256"]
        or current["lock_authority"] != exact_authority
        or current["lock_authority"].get("lock_manifest", {}).get("payload")
        != exact_lock_manifest
    ):
        raise ValueError("semantic current lock authority crosslink differs")
    lock_candidates = _semantic_exact(
        current.get("lock_candidates"), {"A", "C", "D"},
        "semantic current lock candidates",
    )
    candidate_records = []
    for name in ("A", "C", "D"):
        raw = lock_candidates[name]
        path = Path(str(raw.get("path"))) if isinstance(raw, Mapping) else Path("/")
        candidate = _current_immutable_record(
            raw, path, f"semantic current lock candidate {name}"
        )
        claim = lock_variants[name]
        if (
            not isinstance(claim, Mapping)
            or candidate["path"] != claim.get("final_lock_path")
            or candidate["sha256"] != claim.get("final_lock_sha256")
        ):
            raise ValueError(
                f"semantic current lock candidate {name} reviewed binding differs"
            )
        candidate_records.append(candidate)
    if (
        candidate_records[0]["sha256"] != schema.CURRENT_LOCK_SHA256
        or len({record["path"] for record in candidate_records}) != 3
        or len({
            (record["identity"]["device"], record["identity"]["inode"])
            for record in candidate_records
        }) != 3
    ):
        raise ValueError("semantic current lock candidate disjointness differs")

    validation = _semantic_exact(
        current.get("lock_authority_validation"),
        {"execution_authority", "result", "semantic_validator"},
        "semantic current lock validation",
    )
    if validation["semantic_validator"] != "descriptor-cross-bound-authority-context-v1":
        raise ValueError("semantic current lock validator differs")
    result = _semantic_exact(
        validation["result"],
        {"authority_sha256", "lock_manifest_sha256", "schema", "status"},
        "semantic current lock validation result",
    )
    if result != {
        "authority_sha256": lock_inputs["authority"]["sha256"],
        "lock_manifest_sha256": lock_inputs["lock_manifest"]["sha256"],
        "schema": "bn-31gp-current-lock-authority-validation-v1",
        "status": "ok",
    }:
        raise ValueError("semantic current lock validation result differs")
    execution_tools = _semantic_exact(
        validation["execution_authority"],
        {"bwrap", "cargo", "git", "rustc", "rustup"},
        "semantic current lock execution tools",
    )
    for name in ("bwrap", "cargo", "git", "rustc", "rustup"):
        _current_tool_record(
            execution_tools[name], Path(toolchain[f"{name}_path"]),
            toolchain[f"{name}_sha256"],
            f"semantic current lock execution tool {name}",
            trusted=name in {"bwrap", "git", "rustup"},
            live_system=replay.live_system,
        )

    expected_prefix = [
        current_dir / "correctness.rs", current_dir / "fault.rs",
        current_dir / "validate_fault.py", current_dir / "lock_authority.py",
        tooling / "prepare_overlays.py", tooling / "overlay_pins.py",
        current_dir / "product-test-overlay.patch",
        current_dir / "validate_product_test_overlay.py",
        current_dir / "rustc_workspace_wrapper.py",
        current_dir / "validate_build_children.py",
        tooling / "overlay" / "public" / "main.rs",
        tooling / "overlay" / "public" / "adapters" / "current.rs",
        *(tooling / "overlay" / "shared" / name for name in CURRENT_SHARED_NAMES),
        Path(str(lock_inputs["lock_manifest"]["path"])),
        Path(str(lock_inputs["authority"]["path"])),
        Path(str(lock_inputs["review_bundle"]["path"])),
    ]
    inputs = current.get("inputs")
    if not isinstance(inputs, list) or len(inputs) != 28:
        raise ValueError("semantic current input cardinality differs")
    input_records = [
        _current_live_file_identity(raw, f"semantic current input {index}")
        for index, raw in enumerate(inputs)
    ]
    expected_paths = [
        *expected_prefix,
        Path(str(input_records[23]["path"])),
        *(Path(record["path"]) for record in candidate_records),
        config_path,
    ]
    if (
        [record["path"] for record in input_records]
        != [str(path.resolve(strict=True)) for path in expected_paths]
        or len({record["path"] for record in input_records}) != 28
        or len({(record["device"], record["inode"]) for record in input_records}) != 28
    ):
        raise ValueError("semantic current input ordering/disjointness differs")
    if (
        input_records[1] != fault_raw["source"]
        or input_records[2] != fault_raw["validator"]
        or input_records[9]
        != current["static_authority"]["validator"]
        or input_records[27] != cargo["identity"]
        or input_records[6]["sha256"]
        != current.get("product_overlay_authority", {}).get("patch", {}).get(
            "sha256"
        )
    ):
        raise ValueError("semantic current input authority crosslink differs")
    base_tools = schema.parse_prepared_authority_json_object(
        Path(input_records[23]["path"]).read_bytes(),
        "semantic current base tools manifest",
    )
    base_tool_values = base_tools.get("tools")
    base_support_values = base_tools.get("support_files")
    if (
        set(base_tools) != set(schema.TOOLS_MANIFEST_FIELDS)
        or base_tools.get("schema") != schema.TOOLS_MANIFEST_SCHEMA
        or base_tools.get("comm_allowlist") != schema.expected_comm_allowlist()
        or not isinstance(base_tool_values, Mapping)
        or set(base_tool_values) != set(schema.PREPARED_TOOL_NAMES)
        or not isinstance(base_support_values, Mapping)
        or set(base_support_values) != set(schema.PREPARED_SUPPORT_FILE_NAMES)
    ):
        raise ValueError("semantic current base tools manifest differs")
    placeholders = {
        "correctness": {
            "comm": "ast-rb-check", "executable_mode": 0o555,
            "path": "/asterism/preapproval-placeholder/ast-rb-check",
            "sha256": "a48e573b0cbd89a11ece523fbc79e7d6a54aa42fa417e913f70861c0846ed3d6",
        },
        "fault": {
            "comm": "ast-rb-fault", "executable_mode": 0o555,
            "path": "/asterism/preapproval-placeholder/ast-rb-fault",
            "sha256": "a9826b2a400813f9c0ab0b9a8e6998c2c40bf3fc6bee07495552e6d23a7c8367",
        },
    }
    observed_base_files: set[tuple[int, int]] = set()
    for name, raw in base_tool_values.items():
        tool_binding = _semantic_exact(
            raw, set(schema.TOOL_BINDING_FIELDS),
            f"semantic current base tool {name}",
        )
        if name in placeholders:
            if tool_binding != placeholders[name]:
                raise ValueError("semantic current base child placeholder differs")
            continue
        tool_path = Path(str(tool_binding["path"]))
        snapshot = schema.snapshot_regular_file(tool_path, expected_mode=0o555)
        metadata = tool_path.stat()
        if (
            tool_binding["path"] != str(tool_path.resolve(strict=True))
            or tool_binding["sha256"] != snapshot.sha256
            or tool_binding["executable_mode"] != 0o555
            or tool_binding["comm"] != schema.PREPARED_TOOL_COMMS[name]
        ):
            raise ValueError(f"semantic current base tool {name} differs")
        observed_base_files.add((metadata.st_dev, metadata.st_ino))
    for name, raw in base_support_values.items():
        support_binding = _semantic_exact(
            raw, set(schema.SUPPORT_FILE_FIELDS),
            f"semantic current base support {name}",
        )
        support_path = Path(str(support_binding["path"]))
        snapshot = schema.snapshot_regular_file(support_path, expected_mode=0o444)
        metadata = support_path.stat()
        identity = (metadata.st_dev, metadata.st_ino)
        if (
            support_binding["path"] != str(support_path.resolve(strict=True))
            or support_binding["sha256"] != snapshot.sha256
            or support_binding["mode"] != 0o444
            or identity in observed_base_files
        ):
            raise ValueError(f"semantic current base support {name} differs")
        observed_base_files.add(identity)
    if len(observed_base_files) != len(schema.PREPARED_TOOL_NAMES) - 2 + len(
        schema.PREPARED_SUPPORT_FILE_NAMES
    ):
        raise ValueError("semantic current base tools physically alias")

    identities = current.get("toolchain_identities")
    expected_tool_names = (
        "bwrap", "cargo", "git", "rustc", "rust_lld", "rustup",
    )
    if not isinstance(identities, list) or len(identities) != 6:
        raise ValueError("semantic current toolchain identities differ")
    for raw, name in zip(identities, expected_tool_names, strict=True):
        identity = _current_live_file_identity(
            raw, f"semantic current toolchain identity {name}"
        )
        if (
            identity["path"] != toolchain[f"{name}_path"]
            or identity["sha256"] != toolchain[f"{name}_sha256"]
        ):
            raise ValueError("semantic current toolchain identity crosslink differs")
    final_tools = _current_final_tools(
        current, current_root, "semantic current final tools"
    )
    _current_final_tools_inheritance(
        base_tools, final_tools, current.get("artifacts"),
        "semantic current final tools inheritance",
    )


def validate_current_build_record(
    value: Any,
    *,
    name: str,
    directory: str,
    current: Mapping[str, Any],
    current_root: Path,
    source_root: Path,
    toolchain: Mapping[str, Any],
    replay: SemanticReplay,
    expected_source_manifest_sha256: str,
) -> None:
    child = name == "children"
    context = f"semantic current build {name} record"
    record = _semantic_exact(
        value, CURRENT_CHILD_BUILD_FIELDS if child else CURRENT_BUILD_FIELDS,
        context,
    )
    semantic_authority = record["semantic_input_authority"]
    environment = record["environment"]
    expected_environment_fields = (
        CURRENT_CHILD_ENV_FIELDS if child else CURRENT_RELEASE_ENV_FIELDS
    )
    if not isinstance(environment, Mapping) or set(environment) != expected_environment_fields:
        raise ValueError(f"{context} environment fields differ")
    base_environment = {
        "CARGO_HOME": GUEST_CARGO_HOME, "CARGO_INCREMENTAL": "0",
        "CARGO_NET_OFFLINE": "true", "GIT_CONFIG_COUNT": "0",
        "GIT_CONFIG_GLOBAL": f"{GUEST_ROOT}/absent-gitconfig",
        "GIT_CONFIG_NOSYSTEM": "1", "HOME": "/nonexistent",
        "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
        "LD_ORIGIN_PATH": GUEST_TOOLCHAIN_BIN, "PATH": "/usr/bin:/bin",
        "PYTHONDONTWRITEBYTECODE": "1", "PYTHONNOUSERSITE": "1",
        "RUSTC": GUEST_RUSTC, "RUSTUP_HOME": "/nonexistent",
        "RUSTUP_TOOLCHAIN": toolchain["rustup_toolchain"], "TZ": "UTC",
    }
    if any(environment.get(field) != expected for field, expected in base_environment.items()):
        raise ValueError(f"{context} frozen environment differs")
    if child:
        compile_out = current.get("release_compile_out")
        if (
            not isinstance(compile_out, Mapping)
            or environment["ASTERISM_REBASELINE_CHILD_BUILD_NONCE"] != current["build_nonce"]
            or environment["ASTERISM_REBASELINE_EXPECTED_LIB_SOURCE"]
            != "crates/mess-store/src/lib.rs"
            or environment["ASTERISM_REBASELINE_PINNED_RUSTC"] != GUEST_RUSTC
            or environment["ASTERISM_REBASELINE_WRAPPER_RECEIPT"]
            != "/asterism/receipt/injection.json"
            or environment["RUSTC_WORKSPACE_WRAPPER"]
            != "/asterism/rustc_workspace_wrapper.py"
            or environment["ASTERISM_FAULT_COMPILE_OUT_IDENTICAL"] != "true"
            or environment["ASTERISM_FAULT_COMPILE_OUT_SCHEMA"]
            != "bn-2l3n-fault-compile-out-authority-v1"
            or environment["ASTERISM_FAULT_COMPILE_OUT_OVERLAY_RELEASE_SHA256"]
            != compile_out.get("overlay_release_sha256")
            or environment["ASTERISM_FAULT_COMPILE_OUT_PRISTINE_SHA256"]
            != compile_out.get("pristine_sha256")
            or environment["ASTERISM_FAULT_COMPILE_OUT_SYMBOL_ABSENCE_SHA256"]
            != compile_out.get("symbol_absence_sha256")
        ):
            raise ValueError(f"{context} wrapper environment differs")
    else:
        compile_out = current.get("release_compile_out")
        approval = current.get("release_compile_out_approval")
        lock_authority = current.get("lock_authority")
        expected_adapter_sha256 = sha256_file(
            source_root / CURRENT_ADAPTER_DESTINATION
        )
        shared_entries = []
        for shared_name in CURRENT_SHARED_NAMES:
            shared_path = source_root / CURRENT_SHARED_DESTINATION / shared_name
            shared_entries.append({
                "name": shared_name, "sha256": sha256_file(shared_path),
                "size": shared_path.stat().st_size,
            })
        expected_shared_sha256 = hashlib.sha256(canonical_json_bytes({
            "entries": shared_entries, "schema": "asterism-rebaseline-shared-v3",
        })).hexdigest()
        if (
            not isinstance(compile_out, Mapping)
            or not isinstance(approval, Mapping)
            or not isinstance(lock_authority, Mapping)
            or environment["ASTERISM_BUILD_NONCE"] != current["build_nonce"]
            or environment["ASTERISM_BUILD_CARGO_LOCK_SHA256"]
            != sha256_file(source_root / "Cargo.lock")
            or environment["ASTERISM_BUILD_PRODUCT_COMMIT"] != current["product_commit"]
            or environment["ASTERISM_BUILD_PRODUCT_TREE"] != current["product_tree"]
            or environment["ASTERISM_BUILD_PROTOCOL"] != schema.PROTOCOL
            or environment["ASTERISM_BUILD_PROTOCOL_SHA256"] != schema.PROTOCOL_SHA256
            or environment["ASTERISM_BUILD_BINARY_KIND"] != "public"
            or environment["ASTERISM_BUILD_TIMED_SURFACE"] != "public-event-store"
            or environment["ASTERISM_BUILD_VARIANT"] != "A"
            or environment["ASTERISM_BUILD_SOURCE_APPROVAL_SHA256"]
            != compile_out.get("preapproval_source_sentinel")
            or environment["ASTERISM_BUILD_SOURCE_APPROVAL_SHA256"]
            != approval.get("source_approval_sha256")
            or environment["ASTERISM_BUILD_TOOLING_COMMIT"]
            != lock_authority.get("tooling_commit")
            or environment["ASTERISM_BUILD_TOOLING_TREE"]
            != lock_authority.get("tooling_tree")
            or environment["ASTERISM_BUILD_ADAPTER_SHA256"]
            != expected_adapter_sha256
            or environment["ASTERISM_BUILD_SHARED_MANIFEST_SHA256"]
            != expected_shared_sha256
        ):
            raise ValueError(f"{context} release environment differs")

    search, preserved = _current_config_record(
        record["cargo_config_prebuild"], semantic_authority, source_root,
        Path(toolchain["cargo_home_path"]), context + " prebuild config"
    )
    if record["cargo_config_postbuild"] != record["cargo_config_prebuild"]:
        raise ValueError(f"{context} Cargo config changed across build")
    target = current_root / "targets" / directory
    if record["target"] != str(target) or record["target_was_absent"] is not True:
        raise ValueError(f"{context} target authority differs")
    binds = _semantic_exact(
        record["binds"], {"target", "receipt"} if child else {"target"},
        context + " binds",
    )
    target_bind = _semantic_exact(
        binds["target"], {"parent", "post", "pre"}, context + " target bind"
    )
    _current_directory_identity(
        target_bind["parent"], target.parent, context + " target parent", live=False
    )
    target_pre = _current_directory_identity(
        target_bind["pre"], target, context + " target pre", live=False
    )
    target_post = _current_directory_identity(
        target_bind["post"], target, context + " target post", live=False
    )
    if any(target_pre[field] != target_post[field] for field in ("device", "file_type", "inode", "permissions")):
        raise ValueError(f"{context} target selection changed")
    _current_final_bound_directory(
        target_bind["parent"], target.parent, context + " target parent"
    )
    _current_final_bound_directory(
        target_post, target, context + " target"
    )
    if child:
        receipt_root = current_root / "receipts" / directory
        receipt_bind = _semantic_exact(
            binds["receipt"], {"parent", "post", "pre"},
            context + " receipt bind",
        )
        _current_directory_identity(
            receipt_bind["parent"], receipt_root.parent,
            context + " receipt parent", live=False,
        )
        receipt_pre = _current_directory_identity(
            receipt_bind["pre"], receipt_root,
            context + " receipt pre", live=False,
        )
        receipt_post = _current_directory_identity(
            receipt_bind["post"], receipt_root,
            context + " receipt post", live=False,
        )
        if any(
            receipt_pre[field] != receipt_post[field]
            for field in ("device", "file_type", "inode", "permissions")
        ):
            raise ValueError(f"{context} receipt selection changed")
        _current_final_bound_directory(
            receipt_bind["parent"], receipt_root.parent,
            context + " receipt parent",
        )
        _current_final_bound_directory(
            receipt_post, receipt_root, context + " receipt",
        )

    lock_pre = _semantic_exact(
        record["lock_prebuild"], CURRENT_IMMUTABLE_FILE_FIELDS,
        context + " lock prebuild",
    )
    if record["lock_postbuild"] != lock_pre:
        raise ValueError(f"{context} lock changed across build")
    expected_lock = source_root / "Cargo.lock"
    lock_identity = _semantic_exact(
        lock_pre["identity"], CURRENT_IMMUTABLE_IDENTITY_FIELDS,
        context + " lock identity",
    )
    lock_metadata = expected_lock.lstat()
    expected_lock_identity = {
        "changed_ns": lock_metadata.st_ctime_ns,
        "device": lock_metadata.st_dev,
        "inode": lock_metadata.st_ino,
        "link_count": lock_metadata.st_nlink,
        "modified_ns": lock_metadata.st_mtime_ns,
    }
    if (
        lock_identity != expected_lock_identity
        or lock_pre["path"] != str(expected_lock)
        or lock_pre["mode"] != 0o444
        or lock_pre["sha256"] != sha256_file(expected_lock)
        or lock_pre["size"] != lock_metadata.st_size
    ):
        raise ValueError(f"{context} lock authority differs")
    if record["source_manifest_sha256"] != expected_source_manifest_sha256:
        raise ValueError(f"{context} source manifest hash differs")
    toolchain_manifest = _semantic_exact(
        record["toolchain_manifest"],
        {"entry_count", "equal_pre_post", "path", "post_sha256", "pre_sha256"},
        context + " toolchain manifest",
    )
    semantic_toolchain = semantic_authority["toolchain"]
    if toolchain_manifest != {
        "entry_count": semantic_toolchain["entry_count"],
        "equal_pre_post": True,
        "path": semantic_toolchain["manifest_path"],
        "post_sha256": semantic_toolchain["manifest_sha256"],
        "pre_sha256": semantic_toolchain["manifest_sha256"],
    }:
        raise ValueError(f"{context} toolchain semantic crosslink differs")

    execution_tools = _semantic_exact(
        record["execution_tools"],
        {"bwrap", "cargo", "dev_null", "python", "rustc", "rust_lld", "toolchain_root"},
        context + " execution tools",
    )
    bwrap = _current_tool_record(
        execution_tools["bwrap"], Path(toolchain["bwrap_path"]),
        toolchain["bwrap_sha256"], context + " bwrap", trusted=True,
        live_system=replay.live_system,
    )
    _current_tool_record(
        execution_tools["cargo"], Path(toolchain["cargo_path"]),
        toolchain["cargo_sha256"], context + " Cargo", trusted=False,
        live_system=replay.live_system,
    )
    _current_tool_record(
        execution_tools["rustc"], Path(toolchain["rustc_path"]),
        toolchain["rustc_sha256"], context + " rustc", trusted=False,
        live_system=replay.live_system,
    )
    _current_tool_record(
        execution_tools["rust_lld"], Path(toolchain["rust_lld_path"]),
        toolchain["rust_lld_sha256"], context + " rust-lld", trusted=False,
        live_system=replay.live_system,
    )
    _null_device_record(
        execution_tools["dev_null"], context + " null device"
    )
    _current_tool_record(
        execution_tools["python"], CURRENT_SYSTEM_PYTHON, None,
        context + " Python",
        trusted=True, live_system=replay.live_system,
    )
    toolchain_root = Path(toolchain["cargo_path"]).parent.parent
    _current_directory_identity(
        execution_tools["toolchain_root"], toolchain_root,
        context + " toolchain root", live=True,
    )

    expected_examples = (
        ("asterism_rebaseline_current_correctness", "asterism_rebaseline_current_fault")
        if child else ("asterism_rebaseline_public",)
    )
    artifacts = record["artifacts"]
    if not isinstance(artifacts, Mapping) or set(artifacts) != set(expected_examples):
        raise ValueError(f"{context} artifact topology differs")
    target_descriptor = _current_bound_descriptor(
        record["argv"], GUEST_TARGET, context + " target"
    )
    for example in expected_examples:
        artifact = _semantic_exact(
            artifacts[example], {"binding", "source"}, context + " artifact"
        )
        binding = _semantic_exact(
            artifact["binding"], set(schema.TOOL_BINDING_FIELDS),
            context + " artifact binding",
        )
        source_path = target / "release" / "examples" / example
        source = _current_bound_file_identity(
            artifact["source"], source_path,
            f"/proc/self/fd/{target_descriptor}/release/examples/{example}",
            context + " artifact source", executable=True,
            expected_link_count=2,
        )
        published = Path(str(binding["path"]))
        published_snapshot = schema.snapshot_regular_file(published, expected_mode=0o555)
        expected_published = (
            current_root / "artifacts" / "tools"
            / ("ast-rb-check" if example.endswith("correctness") else "ast-rb-fault")
            if child
            else current_root / "artifacts" / "release"
            / ("hooked-A" if name == "hooked_release" else "pristine-A")
        )
        if (
            binding["sha256"] != published_snapshot.sha256
            or binding["executable_mode"] != 0o555
            or not isinstance(binding["comm"], str)
            or not binding["comm"]
            or (
                published != expected_published
                or binding["comm"] != expected_published.name
            )
            or source["sha256"] != binding["sha256"]
        ):
            raise ValueError(f"{context} artifact copy authority differs")
    if child:
        expected_artifacts = {
            "correctness": artifacts["asterism_rebaseline_current_correctness"]["binding"],
            "fault": artifacts["asterism_rebaseline_current_fault"]["binding"],
        }
        if current.get("artifacts") != expected_artifacts:
            raise ValueError(f"{context} published child artifacts differ")

    execution = _semantic_exact(
        record["execution"], CURRENT_EXECUTION_FIELDS, context + " execution"
    )
    argv = record["argv"]
    if (
        not isinstance(argv, list)
        or any(not isinstance(argument, str) for argument in argv)
        or execution["argv"] != argv
        or execution["cwd"] != str(current_root)
        or execution["environment"] != environment
        or execution["execution_authority"] != bwrap
        or not _semantic_integer(execution["exit_status"])
        or execution["exit_status"] != 0
        or not _semantic_integer(execution["passed_file_descriptors"])
        or any(
            not _semantic_integer(execution[field]) or execution[field] < 0
            for field in ("stderr_bytes", "stdout_bytes")
        )
        or not is_sha256(execution["stderr_sha256"])
        or not is_sha256(execution["stdout_sha256"])
    ):
        raise ValueError(f"{context} execution authority differs")
    log_path = current_root / "logs" / f"cargo-build-{directory}.json"
    log_snapshot = schema.snapshot_regular_file(log_path, expected_mode=0o444)
    if schema.parse_canonical_json_object(
        log_snapshot.data, context + " execution log"
    ) != execution:
        raise ValueError(f"{context} execution log sidecar differs")
    offset = 0
    prefix = [toolchain["bwrap_path"], "--die-with-parent", "--new-session", "--unshare-net", "--dir", "/usr"]
    if argv[: len(prefix)] != prefix:
        raise ValueError(f"{context} sandbox prefix differs")
    offset = len(prefix)
    descriptors: list[str] = []

    def consume(operation: str, destination: str) -> None:
        nonlocal offset
        segment = argv[offset : offset + 3]
        descriptor = segment[1] if len(segment) == 3 else ""
        if (
            len(segment) != 3 or segment[0] != operation
            or segment[2] != destination or not descriptor.isascii()
            or not descriptor.isdecimal() or str(int(descriptor)) != descriptor
            or int(descriptor) < 3
        ):
            raise ValueError(f"{context} sandbox binding differs: {destination}")
        descriptors.append(descriptor)
        offset += 3

    for _host, guest in TRUSTED_SYSTEM_MOUNTS:
        consume("--ro-bind-fd", guest)
    aliases = ["--symlink", "usr/bin", "/bin", "--symlink", "usr/lib", "/lib", "--symlink", "usr/lib", "/lib64"]
    if argv[offset : offset + len(aliases)] != aliases:
        raise ValueError(f"{context} sandbox system aliases differ")
    offset += len(aliases)
    private_prefix = ["--dir", "/dev"]
    if argv[offset : offset + len(private_prefix)] != private_prefix:
        raise ValueError(f"{context} sandbox private namespace differs")
    offset += len(private_prefix)
    dev_null = argv[offset : offset + 3]
    dev_null_source = dev_null[1] if len(dev_null) == 3 else ""
    dev_null_descriptor = (
        dev_null_source.removeprefix("/proc/self/fd/")
        if dev_null_source.startswith("/proc/self/fd/")
        else ""
    )
    if (
        len(dev_null) != 3
        or dev_null[0] != "--dev-bind"
        or dev_null[2] != "/dev/null"
        or not dev_null_descriptor.isdecimal()
        or len(dev_null_descriptor) > 10
        or str(int(dev_null_descriptor)) != dev_null_descriptor
        or int(dev_null_descriptor) < 3
    ):
        raise ValueError(f"{context} null-device binding differs")
    descriptors.append(dev_null_descriptor)
    offset += 3
    private_suffix = ["--dir", "/proc", "--tmpfs", "/tmp", "--tmpfs", GUEST_ROOT]
    if argv[offset : offset + len(private_suffix)] != private_suffix:
        raise ValueError(f"{context} sandbox private namespace differs")
    offset += len(private_suffix)
    rustc_host = toolchain.get("rustc_host")
    if (
        not isinstance(rustc_host, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", rustc_host) is None
    ):
        raise ValueError(f"{context} rustc host differs")
    rust_lld_guest_path = (
        f"{GUEST_TOOLCHAIN_ROOT}/lib/rustlib/{rustc_host}/bin/gcc-ld/ld.lld"
    )
    for operation, destination in (
        ("--ro-bind-fd", GUEST_SOURCE),
        ("--ro-bind-fd", GUEST_TOOLCHAIN_ROOT),
        ("--ro-bind-fd", GUEST_CARGO), ("--ro-bind-fd", GUEST_RUSTC),
        ("--ro-bind-fd", rust_lld_guest_path),
        ("--ro-bind-fd", f"{GUEST_ROOT}/python3"),
    ):
        consume(operation, destination)
    source_config_prefix = ["--dir", f"{GUEST_SOURCE}/.cargo", "--tmpfs", f"{GUEST_SOURCE}/.cargo"]
    if argv[offset : offset + len(source_config_prefix)] != source_config_prefix:
        raise ValueError(f"{context} source config namespace differs")
    offset += len(source_config_prefix)
    for entry in preserved["source"]:
        consume(
            "--ro-bind-data" if entry["type"] == "regular" else "--ro-bind-fd",
            f"{GUEST_SOURCE}/.cargo/{entry['name']}",
        )
    for entry in search["entries"][:2]:
        if entry["status"] == "present":
            consume("--ro-bind-data", entry["path"])
    if argv[offset : offset + 2] != ["--remount-ro", f"{GUEST_SOURCE}/.cargo"]:
        raise ValueError(f"{context} source config remount differs")
    offset += 2
    cargo_home_overlay = argv[offset : offset + 4]
    cargo_home_source = (
        cargo_home_overlay[1] if len(cargo_home_overlay) == 4 else ""
    )
    cargo_home_descriptor = (
        cargo_home_source.removeprefix("/proc/self/fd/")
        if cargo_home_source.startswith("/proc/self/fd/")
        else ""
    )
    if (
        len(cargo_home_overlay) != 4
        or cargo_home_overlay[0] != "--overlay-src"
        or not cargo_home_descriptor.isascii()
        or not cargo_home_descriptor.isdecimal()
        or len(cargo_home_descriptor) > 10
        or str(int(cargo_home_descriptor)) != cargo_home_descriptor
        or int(cargo_home_descriptor) < 3
        or cargo_home_source != f"/proc/self/fd/{cargo_home_descriptor}"
        or cargo_home_overlay[2:] != ["--tmp-overlay", GUEST_CARGO_HOME]
    ):
        raise ValueError(f"{context} Cargo-home overlay differs")
    descriptors.append(cargo_home_descriptor)
    offset += 4
    for entry in search["entries"][6:]:
        if entry["status"] == "present":
            consume("--ro-bind-data", entry["path"])
    cargo_tail = ["--remount-ro", GUEST_CARGO_HOME, "--dir", f"{GUEST_ROOT}/.cargo", "--tmpfs", f"{GUEST_ROOT}/.cargo", "--remount-ro", f"{GUEST_ROOT}/.cargo", "--dir", "/.cargo", "--tmpfs", "/.cargo", "--remount-ro", "/.cargo"]
    if argv[offset : offset + len(cargo_tail)] != cargo_tail:
        raise ValueError(f"{context} private config roots differ")
    offset += len(cargo_tail)
    consume("--bind-fd", GUEST_TARGET)
    if child:
        consume("--ro-bind-fd", f"{GUEST_ROOT}/rustc_workspace_wrapper.py")
        consume("--bind-fd", f"{GUEST_ROOT}/receipt")
    suffix = ["--chdir", GUEST_SOURCE, GUEST_CARGO, "build", "--locked", "--offline", "--release", "-p", "mess-store"]
    for example in expected_examples:
        suffix.extend(("--example", example))
    suffix.extend(("--target-dir", GUEST_TARGET))
    if argv[offset:] != suffix or len(descriptors) != len(set(descriptors)):
        raise ValueError(f"{context} sandbox descriptor/command differs")
    # run_capture inherits the argv-bound descriptors plus the source Cargo
    # search guard, bwrap execution lease, and unbound preserved Cargo-home
    # children covered by the retained-FD overlay source.
    expected_passed = len(descriptors) + 2 + len(preserved["cargo-home"])
    if (
        not _semantic_integer(execution["passed_file_descriptors"])
        or execution["passed_file_descriptors"] != expected_passed
    ):
        raise ValueError(f"{context} passed descriptor cardinality differs")

    filesystem = _semantic_exact(
        record["filesystem_admission"], set(schema.FILESYSTEM_ADMISSION_FIELDS),
        context + " filesystem admission",
    )
    admissions = current.get("prebuild_filesystem_admissions")
    if (
        filesystem["schema"] != schema.FILESYSTEM_ADMISSION_SCHEMA
        or filesystem["checked_path"] != str(current_root.parent)
        or filesystem["filesystem"] != schema.REQUIRED_FILESYSTEM_TYPE
        or filesystem["minimum_available_bytes"] != schema.MIN_FREE_BYTES
        or filesystem["minimum_available_inodes"] != schema.MIN_FREE_INODES
        or not isinstance(admissions, Mapping)
        or set(admissions) != {"children", "hooked_release", "pristine_release"}
        or admissions.get(name) != filesystem
        or filesystem["available_bytes"] < schema.MIN_FREE_BYTES
        or filesystem["available_inodes"] < schema.MIN_FREE_INODES
        or any(
            not _semantic_integer(filesystem[field]) or filesystem[field] < 0
            for field in (
                "available_bytes", "available_inodes", "minimum_available_bytes",
                "minimum_available_inodes",
            )
        )
    ):
        raise ValueError(f"{context} filesystem admission differs")

    if child:
        receipt = _semantic_exact(
            record["wrapper_receipt"],
            {"build_nonce", "crate_name", "crate_type", "injected_arguments", "original_argv_sha256", "package", "rustc", "schema", "source"},
            context + " wrapper receipt",
        )
        if (
            receipt["schema"] != "bn-30fs-rustc-workspace-wrapper-receipt-v1"
            or receipt["build_nonce"] != current["build_nonce"]
            or receipt["crate_name"] != "mess_store" or receipt["package"] != "mess-store"
            or receipt["crate_type"] != "lib" or receipt["rustc"] != GUEST_RUSTC
            or receipt["source"] != "crates/mess-store/src/lib.rs"
            or receipt["injected_arguments"] != ["--cfg", "test", "--allow", "explicit_builtin_cfgs_in_flags", "--cfg", "asterism_rebaseline_correctness", "--check-cfg", "cfg(asterism_rebaseline_correctness)"]
            or not is_sha256(receipt["original_argv_sha256"])
        ):
            raise ValueError(f"{context} wrapper receipt differs")
        receipt_path = current_root / "receipts" / directory / "injection.json"
        if schema.parse_canonical_json_object(
            receipt_path.read_bytes(), context + " wrapper receipt payload"
        ) != receipt:
            raise ValueError(f"{context} wrapper receipt payload differs")
        receipt_descriptor = _current_bound_descriptor(
            record["argv"], f"{GUEST_ROOT}/receipt", context + " receipt"
        )
        receipt_identity = _current_bound_file_identity(
            record["wrapper_receipt_identity"],
            receipt_path,
            f"/proc/self/fd/{receipt_descriptor}/injection.json",
            context + " wrapper receipt identity", executable=False,
        )
        if (
            record["wrapper_receipt_sha256"] != receipt_identity["sha256"]
            or receipt_identity["mode"] != 0o444
        ):
            raise ValueError(f"{context} wrapper receipt identity differs")
        wrapper = _current_live_file_identity(
            record["wrapper_input_identity"], context + " wrapper input identity"
        )
        if (
            wrapper["path"]
            != str(current_root / "inputs" / "rustc_workspace_wrapper.py")
            or wrapper["mode"] & 0o111 == 0
        ):
            raise ValueError(f"{context} wrapper input is not executable")


def replay_current_and_resolution_semantics(
    current_children: Mapping[str, Any],
    assertion: Mapping[str, Any],
    lock_authority: Mapping[str, Any],
    replay: SemanticReplay,
) -> None:
    if not require_exact_keys(
        current_children,
        CURRENT_CHILDREN_FIELDS,
        "semantic current-child v2",
        replay.problems,
    ):
        return
    builds = current_children.get("builds")
    expected_builds = {
        "children": "children",
        "hooked_release": "hooked-release",
        "pristine_release": "pristine-release",
    }
    if not isinstance(builds, Mapping) or set(builds) != set(expected_builds):
        replay.problems.add("semantic current build topology differs")
        builds = {}
    inputs = assertion.get("inputs")
    current_input = (
        inputs.get("current_children_attestation")
        if isinstance(inputs, Mapping)
        else None
    )
    current_path = current_input.get("path") if isinstance(current_input, Mapping) else None
    if not isinstance(current_path, str) or not Path(current_path).is_absolute():
        replay.problems.add("semantic current reviewed path differs")
        materialized = None
    else:
        materialized = Path(current_path).parent / "materialized"
    construction_manifests: Mapping[str, str] = {}
    if isinstance(current_path, str):
        replay.capture(
            "semantic current final frozen tree",
            lambda: _current_final_frozen_tree(
                Path(current_path).parent,
                "semantic current final frozen tree",
            ),
        )
        captured_construction = replay.capture(
            "semantic current construction",
            lambda: validate_current_construction(
                current_children, Path(current_path).parent,
                "semantic current construction",
            ),
        )
        if isinstance(captured_construction, Mapping):
            construction_manifests = captured_construction
    toolchain = replay.capture(
        "semantic current toolchain",
        lambda: validate_semantic_toolchain(
            current_children.get("toolchain"),
            "semantic current toolchain",
            live=True,
        ),
    )
    if toolchain is None:
        return
    replay.capture(
        "semantic current top authorities",
        lambda: validate_current_top_authorities(
            current_children, Path(current_path).parent, toolchain, replay
        ),
    )
    for name, directory in expected_builds.items():
        build = builds.get(name)
        if not isinstance(build, Mapping) or materialized is None:
            replay.problems.add(f"semantic current build {name} is absent")
            continue
        replay.capture(
            f"semantic current build {name} materialized manifest",
            lambda directory=directory, name=name: _current_materialized_manifest_sidecar(
                Path(current_path).parent,
                directory,
                materialized / directory,
                construction_manifests.get(name, ""),
                f"semantic current build {name} materialized manifest",
            ),
        )
        replay.capture(
            f"semantic current build {name} producer record",
            lambda build=build, directory=directory, name=name: validate_current_build_record(
                build,
                name=name,
                directory=directory,
                current=current_children,
                current_root=Path(current_path).parent,
                source_root=materialized / directory,
                toolchain=toolchain,
                replay=replay,
                expected_source_manifest_sha256=construction_manifests.get(
                    name, ""
                ),
            ),
        )
        replay.capture(
            f"semantic current build {name}",
            lambda build=build, directory=directory, name=name: replay.validate_authority(
                build.get("semantic_input_authority"),
                f"semantic current build {name}",
                live_roots=SemanticReplay.live_roots(
                    materialized / directory,
                    toolchain,
                    f"semantic current build {name}",
                ),
            ),
        )

    if (
        isinstance(builds.get("hooked_release"), Mapping)
        and isinstance(builds.get("pristine_release"), Mapping)
        and builds["hooked_release"].get("environment")
        != builds["pristine_release"].get("environment")
    ):
        replay.problems.add("semantic current release environments differ")

    lock_manifest = lock_authority.get("lock_manifest")
    payload = lock_manifest.get("payload") if isinstance(lock_manifest, Mapping) else None
    variants = payload.get("variants") if isinstance(payload, Mapping) else None
    expected_toolchain = payload.get("toolchain") if isinstance(payload, Mapping) else None
    if (
        not isinstance(variants, Mapping)
        or set(variants) != set(schema.VARIANTS)
        or not isinstance(expected_toolchain, Mapping)
        or expected_toolchain != toolchain
    ):
        replay.problems.add("semantic resolution variant topology differs")
        return
    replay.capture(
        "semantic resolution toolchain",
        lambda: validate_semantic_toolchain(
            expected_toolchain, "semantic resolution toolchain", live=True
        ),
    )
    source_plan_value = payload.get("source_plan_path")
    try:
        source_plan = Path(source_plan_value).resolve(strict=True)
        repository = source_plan.parents[3]
    except (IndexError, OSError, RuntimeError, TypeError) as error:
        replay.problems.add(f"semantic source-plan topology differs: {error}")
        return
    if (
        source_plan_value != str(source_plan)
        or source_plan
        != repository
        / "spikes"
        / "asterism_rebaseline"
        / "tooling"
        / "source-plan.json"
    ):
        replay.problems.add("semantic source-plan topology differs")
        return
    variant_a = variants.get("A")
    current_lock = (
        variant_a.get("historical_lock")
        if isinstance(variant_a, Mapping)
        else None
    )
    current_lock_sha256 = (
        current_lock.get("sha256")
        if isinstance(current_lock, Mapping)
        else None
    )
    if not is_sha256(current_lock_sha256):
        replay.problems.add("semantic current lock authority differs")
        return
    tracked_fields = {
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
    }
    output_roots: set[Path] = set()
    for variant in schema.VARIANTS:
        claim = variants[variant]
        if not isinstance(claim, Mapping):
            replay.problems.add(f"semantic resolution claim {variant} differs")
            continue
        current_attempt = claim.get("current_lock_attempt")
        resolver = claim.get("resolver")
        final_lock_value = claim.get("final_lock_path")
        try:
            final_lock = Path(final_lock_value).resolve(strict=True)
            output_root = final_lock.parents[1]
            expected_source_root = (
                output_root / "materialized" / variant
            ).resolve(strict=True)
        except (IndexError, OSError, RuntimeError, TypeError) as error:
            replay.problems.add(
                f"semantic resolution {variant} final-lock topology differs: {error}"
            )
            continue
        expected_config_path = (
            output_root / "manifests" / f"cargo-config-{variant}.json"
        )
        output_roots.add(output_root)
        try:
            final_lock_snapshot = schema.snapshot_regular_file(
                final_lock, expected_mode=0o444
            )
        except (OSError, ValueError) as error:
            replay.problems.add(
                f"semantic resolution {variant} final lock differs: {error}"
            )
            continue
        if (
            final_lock_value != str(final_lock)
            or final_lock
            != output_root / "locks" / f"Cargo-{variant}.lock"
            or not is_sha256(claim.get("final_lock_sha256"))
            or final_lock_snapshot.sha256 != claim.get("final_lock_sha256")
        ):
            replay.problems.add(
                f"semantic resolution {variant} final-lock authority differs"
            )
            continue
        if variant in {"A", "B"}:
            historical = claim.get("historical_lock")
            if (
                current_attempt is not None
                or not isinstance(resolver, Mapping)
                or set(resolver) != tracked_fields
                or resolver.get("resolver_kind") != "tracked_git_readback"
                or resolver.get("toolchain") != expected_toolchain
                or resolver.get("environment")
                != frozen_cargo_environment(expected_toolchain)
                or type(resolver.get("exit_status")) is not int
                or resolver.get("exit_status") != 0
                or not isinstance(resolver.get("stdout"), str)
                or resolver.get("stdout_sha256")
                != hashlib.sha256(resolver.get("stdout", "").encode()).hexdigest()
                or not isinstance(resolver.get("stderr"), str)
                or resolver.get("stderr_sha256")
                != hashlib.sha256(resolver.get("stderr", "").encode()).hexdigest()
                or resolver.get("stderr") != ""
                or not isinstance(historical, Mapping)
                or set(historical) != {"commit", "path", "sha256"}
            ):
                replay.problems.add(f"semantic tracked resolver {variant} differs")
                continue
            try:
                source_root = Path(resolver["host_source_root"]).resolve(strict=True)
                cwd = Path(resolver["cwd"]).resolve(strict=True)
            except (KeyError, OSError, RuntimeError, TypeError) as error:
                replay.problems.add(
                    f"semantic tracked resolver {variant} roots differ: {error}"
                )
                continue
            expected_argv = [
                expected_toolchain.get("git_path"),
                "-C",
                str(cwd),
                "show",
                f"{historical['commit']}:{historical['path']}",
            ]
            if (
                resolver.get("host_source_root") != str(source_root)
                or source_root != expected_source_root
                or resolver.get("cwd") != str(repository)
                or cwd != repository
                or not isinstance(resolver.get("cargo_config_search"), Mapping)
                or resolver["cargo_config_search"].get("path")
                != str(expected_config_path)
                or resolver.get("argv") != expected_argv
                or resolver.get("stdout_sha256") != historical.get("sha256")
                or resolver.get("stdout_sha256") != claim.get("final_lock_sha256")
            ):
                replay.problems.add(
                    f"semantic tracked resolver {variant} replay differs"
                )
            validate_sandboxed_cargo_config_search(
                resolver.get("cargo_config_search"),
                source_root,
                expected_toolchain,
                f"semantic tracked resolver {variant} Cargo config search",
                replay.problems,
            )
            continue
        for label, record in (("current", current_attempt), ("generated", resolver)):
            context = f"semantic resolver {variant} {label}"
            if not isinstance(record, Mapping) or set(record) != SEMANTIC_RESOLUTION_FIELDS:
                replay.problems.add(f"{context} fields are not exact")
                continue
            source_root = record.get("host_source_root")
            final_lock_path = claim.get("final_lock_path")
            cargo_config = record.get("cargo_config_search")
            if (
                record.get("resolver_kind") != "sandboxed_cargo_resolution"
                or record.get("cwd") != GUEST_SOURCE
                or type(record.get("exit_status")) is not int
                or record.get("exit_status") != 0
                or record.get("passed_file_descriptors") != 13
                or not isinstance(source_root, str)
                or Path(source_root) != expected_source_root
                or record.get("toolchain") != expected_toolchain
                or not isinstance(cargo_config, Mapping)
                or cargo_config.get("path") != str(expected_config_path)
            ):
                replay.problems.add(f"{context} execution authority differs")
                continue
            validate_resolution_record(
                record,
                claim,
                variant,
                label,
                expected_toolchain,
                current_lock_sha256,
                replay.problems,
            )
            replay.capture(
                context,
                lambda record=record, source_root=source_root, context=context: replay.validate_authority(
                    record.get("semantic_input_authority"),
                    context,
                    live_roots=SemanticReplay.live_roots(
                        Path(source_root), record.get("toolchain"), context
                    ),
                    source_role="resolution_source_without_cargo_lock",
                    prepared_authority=True,
                ),
            )
    if len(output_roots) != 1:
        replay.problems.add("semantic resolution output roots differ")


def replay_prepared_release_semantics(
    prepared: Mapping[str, Any], replay: SemanticReplay
) -> None:
    variants = prepared.get("variants")
    if not isinstance(variants, Mapping) or set(variants) != set(schema.VARIANTS):
        replay.problems.add("semantic prepared release topology differs")
        return
    for variant in schema.VARIANTS:
        item = variants[variant]
        attestation = item.get("attestation") if isinstance(item, Mapping) else None
        context = f"semantic prepared release {variant}"
        if not isinstance(attestation, Mapping):
            replay.problems.add(f"{context} attestation is absent")
            continue
        source_root = attestation.get("materialized_root")
        if not isinstance(source_root, str):
            replay.problems.add(f"{context} materialized root differs")
            continue
        toolchain = replay.capture(
            context + " toolchain",
            lambda attestation=attestation, context=context: validate_semantic_toolchain(
                attestation.get("toolchain"), context + " toolchain", live=True
            ),
        )
        if toolchain is None:
            continue
        replay.capture(
            context,
            lambda attestation=attestation, source_root=source_root, context=context: replay.validate_authority(
                attestation.get("semantic_input_authority"),
                context,
                live_roots=SemanticReplay.live_roots(
                    Path(source_root), toolchain, context
                ),
                prepared_authority=True,
            ),
        )


_EXACT_ZONED_TIME = re.compile(
    r"(?P<head>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?(?P<zone>Z|[+-]\d{2}:\d{2})\Z"
)


def parse_timestamp_key(
    value: Any, context: str, problems: Problems
) -> tuple[datetime, int] | None:
    if not isinstance(value, str):
        problems.add(f"{context} is not a timestamp")
        return None
    match = _EXACT_ZONED_TIME.fullmatch(value)
    if match is None:
        problems.add(f"{context} is not an exact zoned timestamp")
        return None
    fraction = match.group("fraction")
    nanoseconds = (fraction or "").ljust(9, "0")
    microseconds = f".{nanoseconds[:6]}" if fraction else ""
    zone = "+00:00" if match.group("zone") == "Z" else match.group("zone")
    try:
        parsed = datetime.fromisoformat(
            f"{match.group('head')}{microseconds}{zone}"
        )
    except ValueError as error:
        problems.add(f"{context} is invalid: {error}")
        return None
    return parsed, int(nanoseconds[6:] or "0")


def parse_timestamp(value: Any, context: str, problems: Problems) -> datetime | None:
    parsed = parse_timestamp_key(value, context, problems)
    return parsed[0] if parsed is not None else None


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


def read_prepared_authority_object(
    path: Path | schema.FileSnapshot, context: str, problems: Problems
) -> dict[str, Any] | None:
    try:
        snapshot = problems.recalled(path)
        if snapshot is None:
            snapshot = problems.remember(
                schema.snapshot_regular_file(Path(path), expected_mode=0o444)
            )
        return schema.parse_prepared_authority_json_object(
            snapshot.data, context
        )
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
    if config.get("rehearsal") not in (True, False):
        problems.add("config rehearsal flag invalid")
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
        controlled: bool = True,
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
            "controlled": controlled,
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
            binding_transition(
                f"{role}-A", runtime, argv, context, None, controlled=False
            )
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
    rust_lld = resolve_bound_file(value.get("rust_lld_path"), value.get("rust_lld_sha256"), f"{context} rust-lld", problems, expected_mode=0o555)
    rustup = resolve_bound_file(value.get("rustup_path"), value.get("rustup_sha256"), f"{context} rustup", problems, expected_mode=0o555)
    resolve_bound_file(value.get("bwrap_path"), value.get("bwrap_sha256"), f"{context} bwrap", problems, expected_mode=0o555)
    resolve_bound_file(value.get("git_path"), value.get("git_sha256"), f"{context} git", problems, expected_mode=0o555)
    cargo_version = value.get("cargo_version_verbose")
    rustc_version = value.get("rustc_version_verbose")
    host = value.get("rustc_host")
    if (
        not isinstance(host, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", host) is None
    ):
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
        expected_rust_lld = (
            toolchain_root.parent
            / "lib"
            / "rustlib"
            / str(host)
            / "bin"
            / "rust-lld"
        )
        if rust_lld is not None and rust_lld != expected_rust_lld:
            problems.add(f"{context} rust-lld is not the exact rustup toolchain binary")
    return all(item is not None for item in (cargo, rustc, rust_lld, rustup))


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
    observed_sha256 = hashlib.sha256(
        schema.prepared_authority_canonical_json_bytes(value)
    ).hexdigest()
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


def validate_release_compile_out_requirement(
    value: Any, context: str, problems: Problems
) -> dict[str, Any] | None:
    if not require_exact_keys(
        value, RELEASE_COMPILE_OUT_REQUIREMENT_FIELDS, context, problems
    ):
        return None
    expected = {
        "schema": schema.RELEASE_COMPILE_OUT_REQUIREMENT_SCHEMA,
        "status": "required",
        "variant": "A",
        "proof_must_bind_enclosing_approval_sha256": True,
        "repeat_under_real_source_approval": True,
        "same_contract_nonce_lock_toolchain_sandbox": True,
        "cfg_test": False,
        "rustc_workspace_wrapper": "absent",
        "ordinary_a_role": "published",
        "overlay_a_role": "proof_only",
        "binary_byte_identical": True,
        "symbol_inventory_byte_identical": True,
        "forbidden_hook_strings": list(
            schema.RELEASE_COMPILE_OUT_FORBIDDEN_HOOK_STRINGS
        ),
        "forbidden_hook_strings_absent": True,
    }
    for field, expected_value in expected.items():
        if value.get(field) != expected_value:
            problems.add(f"{context} {field} differs from exact authority")
    for field in ("product_overlay_sha256", "preapproval_compile_out_sha256"):
        if not is_sha256(value.get(field)):
            problems.add(f"{context} {field} is not SHA-256")
    return value


def validate_source_review_claim(
    approval: Mapping[str, Any], problems: Problems
) -> dict[str, Any] | None:
    source_review = approval.get("source_review")
    if not require_exact_keys(
        source_review, SOURCE_REVIEW_FIELDS, "source review", problems
    ):
        return None
    if not is_sha256(source_review.get("assertion_sha256")):
        problems.add("source review assertion hash is invalid")
    for name, expected_schema in schema.SOURCE_REVIEW_CONTENT_SCHEMAS.items():
        if name == "current_children_attestation":
            expected_schema = CURRENT_CHILDREN_SCHEMA
        binding = source_review.get(name)
        context = f"source review {name}"
        if not require_exact_keys(
            binding,
            set(schema.SOURCE_REVIEW_CONTENT_BINDING_FIELDS),
            context,
            problems,
        ):
            continue
        if (
            binding.get("schema") != expected_schema
            or binding.get("mode") != 0o444
            or not is_sha256(binding.get("sha256"))
        ):
            problems.add(f"{context} content binding differs")
    validate_release_compile_out_requirement(
        source_review.get("release_compile_out_requirement"),
        "source review release compile-out requirement",
        problems,
    )
    return source_review


def validate_source_review_input(
    value: Any, name: str, problems: Problems
) -> dict[str, Any] | None:
    context = f"source review assertion input {name}"
    if not require_exact_keys(value, SOURCE_REVIEW_INPUT_FIELDS, context, problems):
        return None
    if (
        value.get("schema") != schema.SOURCE_REVIEW_INPUT_SCHEMA
        or value.get("mode") != 0o444
        or not isinstance(value.get("path"), str)
        or not Path(value["path"]).is_absolute()
        or not is_sha256(value.get("sha256"))
        or isinstance(value.get("size"), bool)
        or not isinstance(value.get("size"), int)
        or value["size"] <= 0
    ):
        problems.add(f"{context} immutable binding differs")
    identity = value.get("identity")
    if require_exact_keys(
        identity,
        set(schema.SOURCE_REVIEW_IDENTITY_FIELDS),
        f"{context} identity",
        problems,
    ):
        if any(
            isinstance(identity.get(field), bool)
            or not isinstance(identity.get(field), int)
            or identity[field] <= 0
            for field in schema.SOURCE_REVIEW_IDENTITY_FIELDS
        ):
            problems.add(f"{context} identity values are invalid")
        if identity.get("link_count") != 1:
            problems.add(f"{context} link count is not exact one")
    return value


def validate_source_review_bundle_authority(
    bundle: Mapping[str, Any],
    source_review: Mapping[str, Any],
    approval: Mapping[str, Any],
    current_children: Mapping[str, Any],
    lock_authority: Mapping[str, Any],
    lock_review_bundle: Mapping[str, Any],
    problems: Problems,
) -> None:
    if not require_exact_keys(
        bundle, SOURCE_REVIEW_BUNDLE_FIELDS, "source review bundle", problems
    ):
        return
    assertion = bundle.get("assertion")
    if not require_exact_keys(
        assertion,
        SOURCE_REVIEW_ASSERTION_FIELDS,
        "source review assertion",
        problems,
    ):
        return
    assertion_sha256 = hashlib.sha256(
        schema.prepared_authority_canonical_json_bytes(assertion)
    ).hexdigest()
    if (
        bundle.get("schema") != schema.SOURCE_REVIEW_BUNDLE_SCHEMA
        or bundle.get("assertion_sha256") != assertion_sha256
        or source_review.get("assertion_sha256") != assertion_sha256
    ):
        problems.add("source review assertion digest binding differs")
    for field, expected in {
        "schema": schema.SOURCE_REVIEW_ASSERTION_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "status": "approved",
        "open_findings": 0,
        "tooling_commit": approval.get("tooling_commit"),
        "tooling_tree": approval.get("tooling_tree"),
        "release_compile_out_requirement": source_review.get(
            "release_compile_out_requirement"
        ),
    }.items():
        if assertion.get(field) != expected:
            problems.add(f"source review assertion {field} differs")
    inputs = assertion.get("inputs")
    if not isinstance(inputs, dict) or set(inputs) != set(
        schema.SOURCE_REVIEW_INPUT_NAMES
    ):
        problems.add("source review assertion input names differ")
        return
    validated_inputs = {
        name: validate_source_review_input(inputs[name], name, problems)
        for name in schema.SOURCE_REVIEW_INPUT_NAMES
    }
    if all(record is not None for record in validated_inputs.values()):
        paths = [
            str(record.get("path"))
            for record in validated_inputs.values()
            if record is not None
        ]
        identities = [
            (
                record["identity"].get("device"),
                record["identity"].get("inode"),
            )
            for record in validated_inputs.values()
            if record is not None and isinstance(record.get("identity"), Mapping)
        ]
        if (
            len(identities) != len(validated_inputs)
            or len(set(paths)) != len(paths)
            or len(set(identities)) != len(identities)
        ):
            problems.add("source review assertion inputs are not physically disjoint")
    for name in (
        "current_children_attestation",
        "lock_authority",
        "lock_review_bundle",
        "tools_manifest",
    ):
        expected_hash = (
            approval.get("tools_manifest_sha256")
            if name == "tools_manifest"
            else source_review.get(name, {}).get("sha256")
        )
        record = validated_inputs.get(name)
        if record is not None and record.get("sha256") != expected_hash:
            problems.add(f"source review assertion {name} hash differs")

    created = bundle.get("review_created")
    verdict = bundle.get("verdict")
    if require_exact_keys(
        created,
        set(schema.SOURCE_REVIEW_SEAL_EVENT_FIELDS),
        "source review ReviewCreated",
        problems,
    ):
        created_data = created.get("data")
        if require_exact_keys(
            created_data,
            set(schema.SOURCE_REVIEW_CREATED_DATA_FIELDS),
            "source review ReviewCreated data",
            problems,
        ):
            review_id = created_data.get("review_id")
            detached = f"detached:{approval.get('tooling_commit')}"
            if (
                created.get("event") != "ReviewCreated"
                or not isinstance(created.get("author"), str)
                or not created["author"]
                or created_data.get("initial_commit")
                != approval.get("tooling_commit")
                or created_data.get("jj_change_id") != detached
                or created_data.get("scm_anchor") != detached
                or created_data.get("scm_kind") != "git"
                or not isinstance(review_id, str)
                or not review_id
                or not isinstance(created_data.get("title"), str)
                or not created_data["title"]
                or not isinstance(created_data.get("description"), str)
                or not created_data["description"]
                or approval.get("review_id") != review_id
            ):
                problems.add("source review ReviewCreated anchor/content differs")
        created_at = parse_timestamp(
            created.get("ts"), "source review ReviewCreated timestamp", problems
        )
    else:
        created_data = {}
        created_at = None
    if require_exact_keys(
        verdict,
        set(schema.SOURCE_REVIEW_SEAL_EVENT_FIELDS),
        "source review ReviewerVoted",
        problems,
    ):
        verdict_data = verdict.get("data")
        if require_exact_keys(
            verdict_data,
            set(schema.SOURCE_REVIEW_VERDICT_DATA_FIELDS),
            "source review ReviewerVoted data",
            problems,
        ):
            expected_reason = (
                f"APPROVED assertion_sha256={assertion_sha256}; open_findings=0"
            )
            if (
                verdict.get("event") != "ReviewerVoted"
                or not isinstance(verdict.get("author"), str)
                or not verdict["author"]
                or verdict_data
                != {
                    "reason": expected_reason,
                    "review_id": created_data.get("review_id"),
                    "vote": "lgtm",
                }
            ):
                problems.add("source review ReviewerVoted verdict differs")
        reviewed_at = parse_timestamp(
            verdict.get("ts"), "source review ReviewerVoted timestamp", problems
        )
        if approval.get("reviewed_at") != verdict.get("ts"):
            problems.add("source approval review time is not Seal-derived")
        if created_at is not None and reviewed_at is not None and reviewed_at < created_at:
            problems.add("source review verdict predates ReviewCreated")

    expected_lock_inputs = {
        "authority": inputs.get("lock_authority"),
        "lock_manifest": inputs.get("lock_manifest"),
        "review_bundle": inputs.get("lock_review_bundle"),
    }
    attested_inputs = current_children.get("lock_authority_inputs")
    if not isinstance(attested_inputs, dict) or set(attested_inputs) != set(
        expected_lock_inputs
    ):
        problems.add("current-child lock authority inputs differ")
    else:
        for name, reviewed in expected_lock_inputs.items():
            expected = dict(reviewed) if isinstance(reviewed, dict) else {}
            expected.pop("schema", None)
            if attested_inputs.get(name) != expected:
                problems.add(f"current-child lock input {name} differs")
    if (
        current_children.get("schema") != CURRENT_CHILDREN_SCHEMA
        or current_children.get("status") != "ok"
        or current_children.get("protocol") != schema.PROTOCOL
        or current_children.get("protocol_sha256") != schema.PROTOCOL_SHA256
        or current_children.get("tools_manifest_sha256")
        != inputs["tools_manifest"].get("sha256")
        or current_children.get("lock_manifest_sha256")
        != inputs["lock_manifest"].get("sha256")
        or current_children.get("review_bundle_sha256")
        != inputs["lock_review_bundle"].get("sha256")
        or current_children.get("lock_authority") != lock_authority
    ):
        problems.add("current-child reviewed input crosslinks differ")
    bound_lock_manifest = lock_authority.get("lock_manifest")
    bound_lock_review = lock_authority.get("review_bundle")
    if (
        lock_authority.get("schema")
        != schema.SOURCE_REVIEW_CONTENT_SCHEMAS["lock_authority"]
        or lock_authority.get("status") != "approved"
        or lock_authority.get("protocol") != schema.PROTOCOL
        or lock_authority.get("protocol_sha256") != schema.PROTOCOL_SHA256
        or lock_authority.get("review_sha256")
        != inputs["lock_review_bundle"].get("sha256")
        or not isinstance(bound_lock_manifest, Mapping)
        or bound_lock_manifest.get("sha256")
        != inputs["lock_manifest"].get("sha256")
        or bound_lock_manifest.get("schema")
        != "asterism-rebaseline-lock-candidates-v3"
        or not isinstance(bound_lock_manifest.get("payload"), Mapping)
        or bound_lock_manifest["payload"].get("schema")
        != "asterism-rebaseline-lock-candidates-v3"
        or not isinstance(bound_lock_review, Mapping)
        or bound_lock_review.get("sha256")
        != inputs["lock_review_bundle"].get("sha256")
        or bound_lock_review.get("schema")
        != schema.SOURCE_REVIEW_CONTENT_SCHEMAS["lock_review_bundle"]
        or bound_lock_review.get("payload") != lock_review_bundle
        or lock_review_bundle.get("schema")
        != schema.SOURCE_REVIEW_CONTENT_SCHEMAS["lock_review_bundle"]
    ):
        problems.add("current lock review authority crosslinks differ")
    release_approval = current_children.get("release_compile_out_approval")
    preapproval_sentinel = (
        "fa2acb626f303f8a65a16a6c8a1fd86b7e80cf48e092ae21a7308984ae790c94"
    )
    if release_approval != {
        "final_integration_action": (
            "repeat-release-equality-proof-under-real-source-approval"
        ),
        "source_approval_sha256": preapproval_sentinel,
        "source_approval_status": "preapproval-sentinel-not-source-approved",
    }:
        problems.add("current-child preapproval sentinel/action differs")
    preapproval = current_children.get("release_compile_out")
    requirement = source_review.get("release_compile_out_requirement", {})
    if (
        not isinstance(preapproval, dict)
        or preapproval.get("preapproval_source_sentinel") != preapproval_sentinel
        or preapproval.get("binary_byte_identical") is not True
        or preapproval.get("symbol_inventory_byte_identical") is not True
        or preapproval.get("forbidden_hook_strings")
        != list(schema.RELEASE_COMPILE_OUT_FORBIDDEN_HOOK_STRINGS)
        or hashlib.sha256(
            schema.prepared_authority_canonical_json_bytes(preapproval)
        ).hexdigest()
        != requirement.get("preapproval_compile_out_sha256")
        or current_children.get("product_overlay_authority", {})
        .get("patch", {})
        .get("sha256")
        != requirement.get("product_overlay_sha256")
    ):
        problems.add("current-child preapproval compile-out authority differs")
    artifacts = current_children.get("artifacts")
    approved_tools = approval.get("tools_manifest", {}).get("tools", {})
    if not isinstance(artifacts, dict) or any(
        artifacts.get(name) != approved_tools.get(name)
        for name in ("correctness", "fault")
    ):
        problems.add("current-child tool artifacts differ from source approval")
    if b"/asterism/preapproval-placeholder/" in canonical_json_bytes(
        approval.get("tools_manifest", {})
    ):
        problems.add("source-approved tools retain a preapproval placeholder")


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
    if not synthetic:
        problems.capture(
            "shared source approval authority replay",
            lambda: schema.validate_source_approval(approval),
        )
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
    validate_source_review_claim(approval, problems)

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
    manifest = read_prepared_authority_object(path, context, problems)
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
    manifest = read_prepared_authority_object(path, context, problems)
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


def validate_sandboxed_cargo_config_search(
    binding: Any,
    source_root: Path,
    toolchain: Mapping[str, Any],
    context: str,
    problems: Problems,
) -> None:
    """Replay the exact eight Cargo config candidates visible inside bwrap."""

    if not require_exact_keys(
        binding, set(schema.FILE_BINDING_FIELDS), f"{context} binding", problems
    ):
        return
    path = resolve_bound_file(
        binding.get("path"), binding.get("sha256"), context, problems
    )
    if path is None:
        return
    empty_path = path.path.with_name(f"{path.path.name}.empty")
    resolve_bound_file(
        str(empty_path),
        EMPTY_SHA256,
        f"{context} retained empty authority",
        problems,
        expected_mode=0o444,
    )
    manifest = read_prepared_authority_object(path, context, problems)
    if manifest is None or not require_exact_keys(
        manifest, set(schema.CARGO_CONFIG_SEARCH_FIELDS), context, problems
    ):
        return
    try:
        exact_source = source_root.resolve(strict=True)
        cargo_home = Path(str(toolchain.get("cargo_home_path"))).resolve(
            strict=True
        )
    except OSError as error:
        problems.add(f"{context} root resolution failed: {error}")
        return
    candidates: tuple[tuple[str, Path | None], ...] = (
        (f"{GUEST_SOURCE}/.cargo/config.toml", exact_source / ".cargo/config.toml"),
        (f"{GUEST_SOURCE}/.cargo/config", exact_source / ".cargo/config"),
        (f"{GUEST_ROOT}/.cargo/config.toml", None),
        (f"{GUEST_ROOT}/.cargo/config", None),
        ("/.cargo/config.toml", None),
        ("/.cargo/config", None),
        (f"{GUEST_CARGO_HOME}/config.toml", cargo_home / "config.toml"),
        (f"{GUEST_CARGO_HOME}/config", cargo_home / "config"),
    )
    if (
        manifest.get("schema") != schema.CARGO_CONFIG_SEARCH_SCHEMA
        or manifest.get("cwd") != GUEST_SOURCE
        or manifest.get("cargo_home_path") != GUEST_CARGO_HOME
    ):
        problems.add(f"{context} fixed guest identity differs")
    entries = manifest.get("entries")
    if not isinstance(entries, list) or len(entries) != len(candidates):
        problems.add(f"{context} candidate cardinality differs")
        return
    for ordinal, (entry, (guest_path, host_path)) in enumerate(
        zip(entries, candidates, strict=True), start=1
    ):
        entry_context = f"{context} entry {ordinal}"
        if not require_exact_keys(
            entry,
            set(schema.CARGO_CONFIG_SEARCH_ENTRY_FIELDS),
            entry_context,
            problems,
        ):
            continue
        if entry.get("path") != guest_path:
            problems.add(f"{entry_context} guest path/order differs")
        if host_path is None:
            if entry.get("status") != "absent" or entry.get("sha256") is not None:
                problems.add(f"{entry_context} empty guest config is not absent")
            continue
        if host_path.is_symlink():
            problems.add(f"{entry_context} host source is a symlink")
            continue
        if host_path.exists():
            if entry.get("status") != "present" or not is_sha256(
                entry.get("sha256")
            ):
                problems.add(f"{entry_context} present binding is invalid")
                continue
            resolve_bound_file(
                str(host_path), entry.get("sha256"), entry_context, problems
            )
        elif (
            guest_path not in GUEST_BOUND_CONFIG_PATHS
            or entry.get("status") != "present"
            or entry.get("sha256") != EMPTY_SHA256
        ):
            problems.add(f"{entry_context} retained empty binding is invalid")


def validate_completed_child(
    child: Any,
    context: str,
    problems: Problems,
    *,
    expected_argv: Sequence[str] | None = None,
    expected_passed_file_descriptors: int | None = None,
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
    if expected_passed_file_descriptors is not None:
        fields.add("passed_file_descriptors")
    if not require_exact_keys(child, fields, context, problems):
        return
    if expected_argv is not None and child.get("argv") != list(expected_argv):
        problems.add(f"{context} argv mismatch")
    if (
        expected_passed_file_descriptors is not None
        and (
            type(child.get("passed_file_descriptors")) is not int
            or child.get("passed_file_descriptors")
            != expected_passed_file_descriptors
        )
    ):
        problems.add(f"{context} passed descriptor cardinality mismatch")
    integer_fields = (
        "pid",
        "start_ticks",
        "started_monotonic_ns",
        "completed_monotonic_ns",
        "waited_pid",
    )
    valid_integers = all(
        isinstance(child.get(field), int)
        and not isinstance(child.get(field), bool)
        and child[field] > 0
        for field in integer_fields
    )
    for field in integer_fields:
        if (
            not isinstance(child.get(field), int)
            or isinstance(child.get(field), bool)
            or child[field] <= 0
        ):
            problems.add(f"{context} {field} invalid")
    if child.get("waited_pid") != child.get("pid"):
        problems.add(f"{context} waited_pid mismatch")
    if (
        not isinstance(child.get("exit_status"), int)
        or isinstance(child.get("exit_status"), bool)
        or child.get("exit_status") != 0
        or child.get("timed_out") is not False
        or child.get("process_group_absent") is not True
    ):
        problems.add(f"{context} completion/reaping status invalid")
    if valid_integers and (
        child["completed_monotonic_ns"] < child["started_monotonic_ns"]
    ):
        problems.add(f"{context} monotonic chronology invalid")
    start = parse_timestamp_key(
        child.get("started_at"), f"{context} started_at", problems
    )
    end = parse_timestamp_key(
        child.get("completed_at"), f"{context} completed_at", problems
    )
    if start is not None and end is not None and end < start:
        problems.add(f"{context} wall chronology invalid")
    reaping = child.get("reaping")
    if not require_exact_keys(reaping, {"pid", "start_ticks", "status"}, f"{context} reaping", problems):
        reaping = None
    if reaping is not None:
        reaping_identity_valid = all(
            isinstance(reaping.get(field), int)
            and not isinstance(reaping.get(field), bool)
            and reaping[field] > 0
            for field in ("pid", "start_ticks")
        )
        if not reaping_identity_valid:
            problems.add(f"{context} reaping identity invalid")
        if (
            not reaping_identity_valid
            or reaping.get("pid") != child.get("pid")
            or reaping.get("start_ticks") != child.get("start_ticks")
            or reaping.get("status") != "absent"
        ):
            problems.add(f"{context} reaping identity/status mismatch")
    resolve_bound_file(child.get("output_path"), child.get("output_sha256"), f"{context} output", problems)


def validate_release_materialized_root(
    attestation: Mapping[str, Any], context: str, problems: Problems
) -> tuple[str, int, int] | None:
    value = attestation.get("materialized_root")
    if not isinstance(value, str) or not Path(value).is_absolute():
        problems.add(f"{context} materialized root is not absolute text")
        return None
    try:
        resolved = Path(value).resolve(strict=True)
    except (OSError, RuntimeError) as error:
        problems.add(f"{context} materialized root cannot be resolved: {error}")
        return None
    if value != str(resolved):
        problems.add(f"{context} materialized root is not canonical")
        return None
    descriptor: int | None = None
    try:
        descriptor = os.open(
            resolved,
            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
        )
        opened = os.fstat(descriptor)
        current = os.stat(resolved, follow_symlinks=False)
    except OSError as error:
        problems.add(f"{context} materialized root cannot be opened: {error}")
        return None
    finally:
        if descriptor is not None:
            os.close(descriptor)
    if (
        not stat.S_ISDIR(opened.st_mode)
        or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
    ):
        problems.add(f"{context} materialized root identity differs")
        return None
    return str(resolved), opened.st_dev, opened.st_ino


def validate_release_build_child(
    attestation: Mapping[str, Any], context: str, problems: Problems
) -> tuple[
    Mapping[str, Any], schema.FileSnapshot, tuple[str, int, int]
] | None:
    """Independently replay a release build child and its canonical log."""

    child = attestation.get("build_child")
    build_argv = attestation.get("build_argv")
    validate_completed_child(
        child,
        f"{context} child",
        problems,
        expected_argv=build_argv if isinstance(build_argv, list) else None,
        expected_passed_file_descriptors=16,
    )
    root_identity = validate_release_materialized_root(
        attestation, context, problems
    )
    if not isinstance(child, Mapping):
        return None
    if child.get("cwd") != attestation.get("materialized_root"):
        problems.add(f"{context} child cwd differs")
    if (
        child.get("output_path") != attestation.get("build_log_path")
        or child.get("output_sha256") != attestation.get("build_log_sha256")
    ):
        problems.add(f"{context} child/log binding differs")
    for field in (
        "started_at",
        "started_monotonic_ns",
        "completed_at",
        "completed_monotonic_ns",
    ):
        if child.get(field) != attestation.get(f"build_{field}"):
            problems.add(f"{context} child {field} attestation crosslink differs")

    log_snapshot = resolve_bound_file(
        attestation.get("build_log_path"),
        attestation.get("build_log_sha256"),
        f"{context} build log",
        problems,
        expected_mode=0o444,
    )
    log = (
        read_prepared_authority_object(
            log_snapshot, f"{context} build log", problems
        )
        if log_snapshot is not None
        else None
    )
    if not require_exact_keys(
        log,
        set(schema.RELEASE_COMPILE_OUT_BUILD_LOG_FIELDS),
        f"{context} build log",
        problems,
    ):
        return None
    stdout = log.get("stdout")
    stderr = log.get("stderr")
    if (
        not isinstance(log.get("exit_status"), int)
        or isinstance(log.get("exit_status"), bool)
        or log.get("exit_status") != 0
        or log.get("exit_status") != child.get("exit_status")
        or not isinstance(stdout, str)
        or not isinstance(stderr, str)
        or log.get("stdout_sha256")
        != hashlib.sha256(
            stdout.encode() if isinstance(stdout, str) else b""
        ).hexdigest()
        or log.get("stderr_sha256")
        != hashlib.sha256(
            stderr.encode() if isinstance(stderr, str) else b""
        ).hexdigest()
    ):
        problems.add(f"{context} build log output authority differs")
    if root_identity is None:
        return None
    return child, log_snapshot, root_identity


def validate_release_build_events(
    events: Mapping[
        str,
        tuple[
            Mapping[str, Any],
            Mapping[str, Any],
            schema.FileSnapshot,
            tuple[str, int, int],
        ],
    ],
    problems: Problems,
) -> None:
    """Prove the release proof contains two ordered, disjoint build events."""

    if set(events) != set(schema.RELEASE_COMPILE_OUT_BUILD_NAMES):
        problems.add("release compile-out build event authority is incomplete")
        return
    ordinary_attestation, ordinary_child, ordinary_log, ordinary_root = events[
        "ordinary_a"
    ]
    overlay_attestation, overlay_child, overlay_log, overlay_root = events[
        "overlay_a"
    ]
    if ordinary_root[0] == overlay_root[0] or ordinary_root[1:] == overlay_root[1:]:
        problems.add("release compile-out build materializations are not distinct")
    if (ordinary_child.get("pid"), ordinary_child.get("start_ticks")) == (
        overlay_child.get("pid"),
        overlay_child.get("start_ticks"),
    ):
        problems.add("release compile-out build event identities are not distinct")
    ordinary_completed_monotonic = ordinary_child.get("completed_monotonic_ns")
    overlay_started_monotonic = overlay_child.get("started_monotonic_ns")
    cross_monotonic_valid = all(
        isinstance(value, int) and not isinstance(value, bool) and value > 0
        for value in (ordinary_completed_monotonic, overlay_started_monotonic)
    )
    if not cross_monotonic_valid:
        problems.add("release compile-out build cross-event chronology is invalid")
    elif ordinary_completed_monotonic >= overlay_started_monotonic:
        problems.add("release compile-out build monotonic chronology overlaps")
    ordinary_completed = parse_timestamp_key(
        ordinary_child.get("completed_at"),
        "release compile-out ordinary build completion",
        problems,
    )
    overlay_started = parse_timestamp_key(
        overlay_child.get("started_at"),
        "release compile-out proof-only build start",
        problems,
    )
    if (
        ordinary_completed is not None
        and overlay_started is not None
        and ordinary_completed > overlay_started
    ):
        problems.add("release compile-out build wall chronology overlaps")
    if ordinary_log.path == overlay_log.path or (
        ordinary_log.device,
        ordinary_log.inode,
    ) == (overlay_log.device, overlay_log.inode):
        problems.add("release compile-out build logs are not physically disjoint")


def validate_release_file_binding(
    value: Any,
    context: str,
    problems: Problems,
    *,
    expected_mode: int,
) -> schema.FileSnapshot | None:
    if not require_exact_keys(
        value, set(schema.RELEASE_COMPILE_OUT_FILE_FIELDS), context, problems
    ):
        return None
    snapshot = resolve_bound_file(
        value.get("path"), value.get("sha256"), context, problems,
        expected_mode=expected_mode,
    )
    if snapshot is None:
        return None
    if value.get("mode") != expected_mode or value.get("size") != snapshot.size:
        problems.add(f"{context} size/mode differs from live immutable file")
    expected_identity = {
        "changed_ns": snapshot._stat.st_ctime_ns,
        "device": snapshot.device,
        "inode": snapshot.inode,
        "link_count": snapshot._stat.st_nlink,
        "modified_ns": snapshot._stat.st_mtime_ns,
    }
    if value.get("identity") != expected_identity or snapshot._stat.st_nlink != 1:
        problems.add(f"{context} identity differs from live immutable file")
    return snapshot


def preapproval_nm_identity(
    value: Any, problems: Problems
) -> Mapping[str, Any] | None:
    try:
        return schema.validate_preapproval_nm_authority(value)
    except ValueError:
        problems.add("release compile-out preapproval nm authority is absent")
        return None


def validate_release_compile_out_proof(
    proof: Mapping[str, Any],
    *,
    approval_sha256: str,
    source_review: Mapping[str, Any],
    current_children: Mapping[str, Any],
    current_children_sha256: str,
    prepared: Mapping[str, Any],
    config: Mapping[str, Any] | None,
    semantic_replay: SemanticReplay,
    problems: Problems,
) -> None:
    if not require_exact_keys(
        proof, RELEASE_COMPILE_OUT_FIELDS, "release compile-out proof", problems
    ):
        return
    requirement = source_review.get("release_compile_out_requirement")
    requirement_sha256 = (
        hashlib.sha256(
            schema.prepared_authority_canonical_json_bytes(requirement)
        ).hexdigest()
        if isinstance(requirement, dict)
        else None
    )
    expected_top = {
        "schema": schema.RELEASE_COMPILE_OUT_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "status": "ok",
        "source_approval_sha256": approval_sha256,
        "requirement_sha256": requirement_sha256,
        "current_children_attestation_sha256": current_children_sha256,
        "product_overlay_sha256": (
            requirement.get("product_overlay_sha256")
            if isinstance(requirement, dict)
            else None
        ),
        "forbidden_hook_strings": list(
            schema.RELEASE_COMPILE_OUT_FORBIDDEN_HOOK_STRINGS
        ),
        "binary_byte_identical": True,
        "symbol_inventory_byte_identical": True,
        "forbidden_hook_strings_absent": True,
    }
    for field, expected in expected_top.items():
        if proof.get(field) != expected:
            problems.add(f"release compile-out {field} differs")

    equivalence = proof.get("equivalence_contract")
    if require_exact_keys(
        equivalence,
        set(schema.RELEASE_COMPILE_OUT_EQUIVALENCE_CONTRACT_FIELDS),
        "release compile-out equivalence contract",
        problems,
    ):
        for field, expected in {
            "source_approval_sha256": approval_sha256,
            "cfg_test": False,
            "rustc_workspace_wrapper": "absent",
            "ordinary_a_role": "published",
            "overlay_a_role": "proof_only",
        }.items():
            if equivalence.get(field) != expected:
                problems.add(f"release compile-out equivalence {field} differs")
        for field in (
            "contract_sha256", "build_nonce", "cargo_lock_sha256",
            "toolchain_sha256", "build_environment_sha256", "sandbox_sha256",
        ):
            if not is_sha256(equivalence.get(field)):
                problems.add(f"release compile-out equivalence {field} is not SHA-256")

    builds = proof.get("builds")
    if not isinstance(builds, dict) or set(builds) != set(
        schema.RELEASE_COMPILE_OUT_BUILD_NAMES
    ):
        problems.add("release compile-out build names differ")
        builds = {}
    build_events: dict[
        str,
        tuple[
            Mapping[str, Any],
            Mapping[str, Any],
            schema.FileSnapshot,
            tuple[str, int, int],
        ],
    ] = {}
    for name in schema.RELEASE_COMPILE_OUT_BUILD_NAMES:
        build = builds.get(name)
        context = f"release compile-out build {name}"
        if not require_exact_keys(
            build, set(schema.RELEASE_COMPILE_OUT_BUILD_FIELDS), context, problems
        ):
            continue
        expected_role = "published" if name == "ordinary_a" else "proof_only"
        if build.get("role") != name or build.get("artifact_role") != expected_role:
            problems.add(f"{context} role differs")
        for field in (
            "source_approval_sha256", "contract_sha256", "build_nonce",
            "cargo_lock_sha256", "toolchain_sha256",
            "build_environment_sha256", "sandbox_sha256",
        ):
            expected = equivalence.get(field) if isinstance(equivalence, dict) else None
            if build.get(field) != expected:
                problems.add(f"{context} {field} differs from equivalence contract")
        if build.get("cfg_test") is not False or build.get(
            "rustc_workspace_wrapper"
        ) != "absent":
            problems.add(f"{context} is not a wrapper-free release build")
        attestation = build.get("attestation")
        attestation_fields = set(
            RELEASE_ORDINARY_ATTESTATION_FIELDS
            if name == "ordinary_a"
            else RELEASE_OVERLAY_ATTESTATION_FIELDS
        )
        if not require_exact_keys(
            attestation, attestation_fields, f"{context} attestation", problems
        ):
            continue
        if hashlib.sha256(
            schema.prepared_authority_canonical_json_bytes(attestation)
        ).hexdigest() != build.get("attestation_sha256"):
            problems.add(f"{context} attestation digest differs")
            continue
        prepared_a_attestation = prepared.get("variants", {}).get("A", {}).get(
            "attestation"
        )
        if name == "ordinary_a" and attestation != prepared_a_attestation:
            problems.add(
                "release compile-out ordinary A attestation differs from prepared A"
            )
        if isinstance(prepared_a_attestation, Mapping) and (
            attestation.get("build_env") != prepared_a_attestation.get("build_env")
            or attestation.get("toolchain")
            != prepared_a_attestation.get("toolchain")
        ):
            problems.add(f"{context} environment/toolchain differs from prepared A")
        embedded_toolchain = attestation.get("toolchain")
        if not isinstance(embedded_toolchain, Mapping):
            embedded_toolchain = {}
        validated_release_toolchain = semantic_replay.capture(
            context + " toolchain",
            lambda embedded_toolchain=embedded_toolchain, context=context: validate_semantic_toolchain(
                embedded_toolchain, context + " toolchain", live=True
            ),
        )
        if (
            validated_release_toolchain is not None
            and validated_release_toolchain != current_children.get("toolchain")
        ):
            problems.add(f"{context} toolchain differs from current authority")
        for field, embedded in (
            ("build_environment_sha256", attestation.get("build_env")),
            ("toolchain_sha256", embedded_toolchain),
        ):
            if (
                not isinstance(embedded, Mapping)
                or hashlib.sha256(
                    schema.prepared_authority_canonical_json_bytes(embedded)
                ).hexdigest()
                != build.get(field)
            ):
                problems.add(f"{context} embedded {field} differs")
        build_environment = attestation.get("build_env")
        if not isinstance(build_environment, Mapping):
            problems.add(f"{context} embedded build environment is not an object")
            build_environment = {}
        if (
            attestation.get("build_nonce") != build.get("build_nonce")
            or attestation.get("cargo_lock_sha256")
            != build.get("cargo_lock_sha256")
            or build_environment.get("ASTERISM_BUILD_SOURCE_APPROVAL_SHA256")
            != approval_sha256
            or build_environment.get("CARGO_HOME") != GUEST_CARGO_HOME
            or build_environment.get("RUSTC") != GUEST_RUSTC
            or build_environment.get("RUSTUP_HOME") != GUEST_RUSTUP_HOME
            or build_environment.get("LD_ORIGIN_PATH") != GUEST_TOOLCHAIN_BIN
            or build_environment.get("PATH")
            != f"{GUEST_TOOLCHAIN_ROOT}/bin:/usr/bin:/bin"
            or any(
                field in build_environment
                for field in (
                    "RUSTC_WORKSPACE_WRAPPER",
                    "RUSTC_WRAPPER",
                    "RUSTFLAGS",
                    "CARGO_ENCODED_RUSTFLAGS",
                )
            )
        ):
            problems.add(f"{context} embedded nonce/lock/environment differs")
        try:
            build_source_root = Path(
                str(attestation.get("materialized_root"))
            ).resolve(strict=True)
        except OSError as error:
            problems.add(f"{context} materialized root is invalid: {error}")
        else:
            validate_sandboxed_cargo_config_search(
                attestation.get("cargo_config_search"),
                build_source_root,
                embedded_toolchain,
                f"{context} Cargo config search",
                problems,
            )
        if name == "ordinary_a" and attestation == prepared_a_attestation:
            semantic_runtime = attestation.get(
                "semantic_input_authority", {}
            ).get("runtime_sha256")
            if semantic_runtime not in semantic_replay.runtimes:
                problems.add(
                    "release compile-out ordinary A semantic authority was not "
                    "replayed as prepared A"
                )
        else:
            semantic_runtime = semantic_replay.capture(
                f"{context} semantic authority",
                lambda: semantic_replay.validate_authority(
                    attestation.get("semantic_input_authority"),
                    f"{context} semantic authority",
                    live_roots=SemanticReplay.live_roots(
                        Path(str(attestation.get("materialized_root"))),
                        embedded_toolchain,
                        f"{context} semantic authority",
                    ),
                ),
            )
        execution_tools_sha256 = validate_prepared_execution_tools(
            attestation.get("execution_tools"),
            embedded_toolchain,
            f"{context} execution tools",
            problems,
        )
        normalized_sandbox_sha256 = validate_sandboxed_build_argv(
            attestation.get("build_argv"),
            bwrap_path=embedded_toolchain.get("bwrap_path"),
            cargo_config_search_sha256=(
                attestation["cargo_config_search"].get("sha256")
                if isinstance(attestation.get("cargo_config_search"), Mapping)
                else None
            ),
            semantic_runtime_sha256=(
                attestation["semantic_input_authority"].get("runtime_sha256")
                if isinstance(
                    attestation.get("semantic_input_authority"), Mapping
                )
                else None
            ),
            rustc_host=embedded_toolchain.get("rustc_host"),
            execution_tools_sha256=execution_tools_sha256,
            package="mess-store",
            example="asterism_rebaseline_public",
            context=f"{context} sandbox argv",
            problems=problems,
        )
        if normalized_sandbox_sha256 != build.get("sandbox_sha256"):
            problems.add(f"{context} normalized sandbox hash differs")
        build_event = validate_release_build_child(attestation, context, problems)
        if build_event is not None:
            build_events[name] = (attestation, *build_event)
        build_argv = attestation.get("build_argv")
        if isinstance(build_argv, list) and any(
            attestation.get(field) in build_argv
            for field in ("materialized_root", "target_dir")
        ):
            problems.add(f"{context} sandbox exposes a mutable host path")
        contract_snapshot = resolve_bound_file(
            attestation.get("contract_output_path"),
            attestation.get("contract_output_sha256"),
            f"{context} contract output",
            problems,
            expected_mode=0o444,
        )
        if contract_snapshot is not None:
            contract = read_prepared_authority_object(
                contract_snapshot, f"{context} contract output", problems
            )
            prepared_contract = prepared.get("variants", {}).get("A", {}).get(
                "contract"
            )
            if (
                contract is None
                or hashlib.sha256(
                    schema.prepared_authority_canonical_json_bytes(contract)
                ).hexdigest()
                != build.get("contract_sha256")
                or contract != prepared_contract
            ):
                problems.add(f"{context} contract replay differs")
        requirement_overlay_sha256 = (
            requirement.get("product_overlay_sha256")
            if isinstance(requirement, Mapping)
            else None
        )
        if name == "ordinary_a" and "product_overlay_sha256" in attestation:
            problems.add("release compile-out ordinary A has overlay authority")
        if name == "overlay_a" and attestation.get(
            "product_overlay_sha256"
        ) != requirement_overlay_sha256:
            problems.add("release compile-out proof-only overlay authority differs")
    validate_release_build_events(build_events, problems)

    binaries = proof.get("binaries")
    if not isinstance(binaries, dict) or set(binaries) != set(
        schema.RELEASE_COMPILE_OUT_BUILD_NAMES
    ):
        problems.add("release compile-out binary names differ")
        binaries = {}
    binary_snapshots = {
        name: validate_release_file_binding(
            binaries.get(name), f"release compile-out binary {name}", problems,
            expected_mode=0o555,
        )
        for name in schema.RELEASE_COMPILE_OUT_BUILD_NAMES
    }
    ordinary = binary_snapshots.get("ordinary_a")
    overlay = binary_snapshots.get("overlay_a")
    if ordinary is not None and overlay is not None:
        if (
            ordinary.data != overlay.data
            or ordinary.sha256 != overlay.sha256
            or (ordinary.device, ordinary.inode) == (overlay.device, overlay.inode)
        ):
            problems.add("release compile-out binaries are not equal disjoint files")
        if proof.get("published_a_sha256") != ordinary.sha256:
            problems.add("release compile-out published A hash differs")
        for forbidden in schema.RELEASE_COMPILE_OUT_FORBIDDEN_HOOK_STRINGS:
            if forbidden.encode() in ordinary.data or forbidden.encode() in overlay.data:
                problems.add(f"release binary contains forbidden hook string {forbidden}")

    inventories = proof.get("symbol_inventories")
    if not isinstance(inventories, dict) or set(inventories) != set(
        schema.RELEASE_COMPILE_OUT_BUILD_NAMES
    ):
        problems.add("release compile-out symbol inventory names differ")
        inventories = {}
    inventory_snapshots = {
        name: validate_release_file_binding(
            inventories.get(name),
            f"release compile-out symbol inventory {name}", problems,
            expected_mode=0o444,
        )
        for name in schema.RELEASE_COMPILE_OUT_BUILD_NAMES
    }
    ordinary_inventory = inventory_snapshots.get("ordinary_a")
    overlay_inventory = inventory_snapshots.get("overlay_a")
    if ordinary_inventory is not None and overlay_inventory is not None:
        if (
            ordinary_inventory.data != overlay_inventory.data
            or ordinary_inventory.sha256 != overlay_inventory.sha256
            or ordinary_inventory.size != overlay_inventory.size
            or ordinary_inventory.path == overlay_inventory.path
            or (ordinary_inventory.device, ordinary_inventory.inode)
            == (overlay_inventory.device, overlay_inventory.inode)
        ):
            problems.add(
                "release compile-out symbol inventories are not equal disjoint files"
            )
        for forbidden in schema.RELEASE_COMPILE_OUT_FORBIDDEN_HOOK_STRINGS:
            if (
                forbidden.encode() in ordinary_inventory.data
                or forbidden.encode() in overlay_inventory.data
            ):
                problems.add(f"release symbols contain forbidden hook string {forbidden}")

    nm = proof.get("nm")
    if require_exact_keys(
        nm, set(schema.RELEASE_COMPILE_OUT_NM_FIELDS),
        "release compile-out nm", problems,
    ):
        preapproval_nm = current_children.get("release_compile_out", {}).get("nm")
        preapproval_identity = preapproval_nm_identity(preapproval_nm, problems)
        expected_nm_mode = (
            preapproval_identity["mode"] if preapproval_identity is not None else -1
        )
        tool = validate_release_file_binding(
            nm.get("tool"), "release compile-out nm tool", problems,
            expected_mode=expected_nm_mode,
        )
        if preapproval_identity is not None and isinstance(nm.get("tool"), Mapping):
            expected_nm = {
                "identity": {
                    "changed_ns": preapproval_identity["ctime_ns"],
                    "device": preapproval_identity["device"],
                    "inode": preapproval_identity["inode"],
                    "link_count": preapproval_identity["link_count"],
                    "modified_ns": preapproval_identity["mtime_ns"],
                },
                "mode": preapproval_identity["mode"],
                "path": preapproval_identity["path"],
                "sha256": preapproval_identity["sha256"],
                "size": preapproval_identity["size"],
            }
            if nm.get("tool") != expected_nm:
                problems.add("release compile-out nm tool differs from preapproval")
        for name in schema.RELEASE_COMPILE_OUT_BUILD_NAMES:
            child = nm.get(name)
            if require_exact_keys(
                child, set(schema.RELEASE_COMPILE_OUT_NM_CHILD_FIELDS),
                f"release compile-out nm child {name}", problems,
            ):
                argv = child.get("argv")
                expected_prefix = [
                    str(tool.path) if tool is not None else None,
                    "--defined-only",
                    "--demangle=rust",
                    "--format=posix",
                ]
                if (
                    tool is None
                    or not isinstance(argv, list)
                    or len(argv) != 5
                    or argv[:4] != expected_prefix
                    or not isinstance(argv[4], str)
                    or re.fullmatch(r"/proc/self/fd/[0-9]+", argv[4]) is None
                ):
                    problems.add(f"release compile-out nm child {name} argv differs")
                validate_completed_child(
                    child, f"release compile-out nm child {name}", problems,
                    expected_argv=argv if isinstance(argv, list) else None,
                )
                output_snapshot = resolve_bound_file(
                    child.get("output_path"),
                    child.get("output_sha256"),
                    f"release compile-out nm child {name} output",
                    problems,
                    expected_mode=0o444,
                )
                output = (
                    read_prepared_authority_object(
                        output_snapshot,
                        f"release compile-out nm child {name} output",
                        problems,
                    )
                    if output_snapshot is not None
                    else None
                )
                output_fields = {
                    "exit_status",
                    "stderr",
                    "stderr_sha256",
                    "stdout",
                    "stdout_sha256",
                }
                if not require_exact_keys(
                    output,
                    output_fields,
                    f"release compile-out nm child {name} output",
                    problems,
                ):
                    continue
                stdout = output.get("stdout")
                stderr = output.get("stderr")
                if (
                    output.get("exit_status") != 0
                    or not isinstance(stdout, str)
                    or not isinstance(stderr, str)
                    or stderr != ""
                    or output.get("stderr_sha256") != EMPTY_SHA256
                    or output.get("stdout_sha256")
                    != (
                        hashlib.sha256(stdout.encode()).hexdigest()
                        if isinstance(stdout, str)
                        else None
                    )
                ):
                    problems.add(
                        f"release compile-out nm child {name} output binding differs"
                    )
                inventory = inventory_snapshots.get(name)
                if (
                    inventory is not None
                    and isinstance(stdout, str)
                    and inventory.data != stdout.encode()
                ):
                    problems.add(
                        f"release compile-out nm child {name} inventory bytes differ"
                    )

    prepared_a = prepared.get("variants", {}).get("A", {})
    prepared_a_binary = prepared_a.get("binary", {})
    if (
        prepared_a_binary.get("sha256") != proof.get("published_a_sha256")
        or (ordinary is not None and prepared_a_binary.get("path") != str(ordinary.path))
    ):
        problems.add("prepared variant A is not the published ordinary A")
    overlay_path = str(overlay.path) if overlay is not None else str(
        binaries.get("overlay_a", {}).get("path", "")
    )
    reachable_values: list[Any] = [prepared.get("tools"), prepared.get("variants")]
    if config is not None:
        reachable_values.extend((
            config.get("smoke_transitions"), config.get("correctness_execution"),
            config.get("argv_templates"),
        ))
    if overlay_path and any(
        overlay_path in canonical_json_bytes(value).decode(errors="replace")
        for value in reachable_values
    ):
        problems.add("release compile-out proof-only overlay A is child-reachable")


def validate_prepared_source_review_and_release(
    prepared: Mapping[str, Any],
    prepared_root: Path,
    approval: Mapping[str, Any],
    source_approval_sha256: str,
    config: Mapping[str, Any] | None,
    problems: Problems,
    *,
    live_system: bool,
) -> None:
    semantic_replay = SemanticReplay(problems, live_system=live_system)
    source_review = validate_source_review_claim(approval, problems)
    if source_review is None:
        return
    prepared_review = prepared.get("source_review")
    if not require_exact_keys(
        prepared_review, set(schema.PREPARED_SOURCE_REVIEW_FIELDS),
        "prepared source review", problems,
    ):
        return
    snapshots: dict[str, schema.FileSnapshot] = {}
    values: dict[str, dict[str, Any]] = {}
    for name in schema.PREPARED_SOURCE_REVIEW_FIELDS:
        binding = prepared_review.get(name)
        context = f"prepared source review {name}"
        if not require_exact_keys(
            binding, set(schema.PREPARED_SOURCE_REVIEW_BINDING_FIELDS),
            context, problems,
        ):
            continue
        expected_path = (
            prepared_root / schema.PREPARED_SOURCE_REVIEW_RELATIVE_PATHS[name]
        ).resolve()
        if binding.get("path") != str(expected_path) or binding.get("mode") != 0o444:
            problems.add(f"{context} layout/mode differs")
        claimed = source_review.get(name, {})
        if binding.get("sha256") != claimed.get("sha256"):
            problems.add(f"{context} differs from source approval")
        snapshot = resolve_bound_file(
            binding.get("path"), binding.get("sha256"), context, problems,
            within=prepared_root, expected_mode=0o444,
        )
        if snapshot is None:
            continue
        snapshots[name] = snapshot
        observed = read_prepared_authority_object(snapshot, context, problems)
        if isinstance(observed, dict):
            values[name] = observed
            if observed.get("schema") != claimed.get("schema"):
                problems.add(f"{context} payload schema differs")

    if set(values) == set(schema.PREPARED_SOURCE_REVIEW_FIELDS):
        validate_source_review_bundle_authority(
            values["bundle"], source_review, approval,
            values["current_children_attestation"], values["lock_authority"],
            values["lock_review_bundle"], problems,
        )
        assertion = values["bundle"].get("assertion")
        if isinstance(assertion, Mapping):
            replay_current_and_resolution_semantics(
                values["current_children_attestation"],
                assertion,
                values["lock_authority"],
                semantic_replay,
            )
    replay_prepared_release_semantics(prepared, semantic_replay)

    proof_binding = prepared.get("release_compile_out")
    if not require_exact_keys(
        proof_binding, set(schema.RELEASE_COMPILE_OUT_BINDING_FIELDS),
        "prepared release compile-out binding", problems,
    ):
        return
    expected_proof_path = (
        prepared_root / schema.RELEASE_COMPILE_OUT_RELATIVE_PATH
    ).resolve()
    if proof_binding.get("path") != str(expected_proof_path) or proof_binding.get(
        "mode"
    ) != 0o444:
        problems.add("prepared release compile-out layout/mode differs")
    proof_snapshot = resolve_bound_file(
        proof_binding.get("path"), proof_binding.get("sha256"),
        "prepared release compile-out proof", problems, within=prepared_root,
        expected_mode=0o444,
    )
    if proof_snapshot is None:
        return
    proof = read_prepared_authority_object(
        proof_snapshot, "prepared release compile-out proof", problems
    )
    current = snapshots.get("current_children_attestation")
    if isinstance(proof, dict) and current is not None:
        validate_release_compile_out_proof(
            proof, approval_sha256=source_approval_sha256,
            source_review=source_review, current_children=values[
                "current_children_attestation"
            ], current_children_sha256=current.sha256,
            prepared=prepared,
            config=config,
            semantic_replay=semantic_replay,
            problems=problems,
        )
    semantic_replay.finalize()


def validate_proof_only_path_after_children(
    prepared: Mapping[str, Any] | None,
    children: Sequence[Mapping[str, Any]],
    problems: Problems,
) -> None:
    """Prove the proof-only twin stayed unreachable through the final child ledger."""

    if not isinstance(prepared, Mapping):
        return
    binding = prepared.get("release_compile_out")
    if not isinstance(binding, Mapping):
        return
    snapshot = resolve_bound_file(
        binding.get("path"),
        binding.get("sha256"),
        "post-child release compile-out proof",
        problems,
        expected_mode=0o444,
    )
    if snapshot is None:
        return
    proof = read_prepared_authority_object(
        snapshot, "post-child release compile-out proof", problems
    )
    overlay_path = (
        proof.get("binaries", {}).get("overlay_a", {}).get("path")
        if isinstance(proof, Mapping)
        else None
    )
    if not isinstance(overlay_path, str) or not overlay_path:
        problems.add("post-child proof-only overlay A path is absent")
        return

    def reaches_overlay(value: Any) -> bool:
        if isinstance(value, Mapping):
            return any(reaches_overlay(item) for item in value.values())
        if isinstance(value, (list, tuple)):
            return any(reaches_overlay(item) for item in value)
        return value == overlay_path

    if any(reaches_overlay(child) for child in children):
        problems.add(
            "proof-only overlay A is reachable from the completed child manifest"
        )


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
            observed = read_prepared_authority_object(
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
            observed_manifest = read_prepared_authority_object(
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
    if (
        not synthetic
        and approval is not None
        and original_prepared_snapshot is not None
    ):
        problems.capture(
            "shared prepared source-review/release authority replay",
            lambda: schema.validate_prepared_artifacts(
                prepared, approval, original_prepared_snapshot.path
            ),
        )
    if prepared_root is not None and approval is not None:
        validate_prepared_source_review_and_release(
            prepared,
            prepared_root,
            approval,
            source_approval_sha256,
            config,
            problems,
            live_system=not synthetic,
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
        execution_tools_sha256 = validate_prepared_execution_tools(
            attestation.get("execution_tools"),
            toolchain,
            f"{context} execution tools",
            problems,
        )
        validate_sandboxed_build_argv(
            build_argv,
            bwrap_path=toolchain.get("bwrap_path"),
            cargo_config_search_sha256=(
                attestation["cargo_config_search"].get("sha256")
                if isinstance(attestation.get("cargo_config_search"), Mapping)
                else None
            ),
            semantic_runtime_sha256=(
                attestation.get("semantic_input_authority", {}).get(
                    "runtime_sha256"
                )
                if isinstance(
                    attestation.get("semantic_input_authority"), Mapping
                )
                else None
            ),
            rustc_host=toolchain.get("rustc_host"),
            execution_tools_sha256=execution_tools_sha256,
            package="mess-log" if variant == "B" else "mess-store",
            example=(
                "asterism_rebaseline_bare"
                if variant == "B"
                else "asterism_rebaseline_public"
            ),
            context=f"{context} sandbox argv",
            problems=problems,
        )
        build_env = attestation.get("build_env")
        if not require_exact_keys(build_env, set(schema.BUILD_ENV_FIELDS), f"{context} build environment", problems):
            build_env = {}
        approved_variant = approval.get("variants", {}).get(variant, {}) if approval else {}
        expected_build_env = sandboxed_build_environment(
            toolchain,
            {
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
            },
        )
        if build_env != expected_build_env:
            problems.add(f"{context} build environment differs from exact contract identity")
        if materialized_root is not None:
            validate_sandboxed_cargo_config_search(
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
        validate_completed_child(
            attestation.get("build_child"),
            f"{context} build child",
            problems,
            expected_argv=build_argv,
            expected_passed_file_descriptors=16,
        )
        validate_completed_child(attestation.get("contract_child"), f"{context} contract child", problems, expected_argv=item.get("contract_argv") if isinstance(item.get("contract_argv"), list) else None)
        contract_path = resolve_bound_file(attestation.get("contract_output_path"), attestation.get("contract_output_sha256"), f"{context} contract output", problems)
        contract = item.get("contract")
        if isinstance(contract, dict):
            problems.capture(f"{context} binary contract", lambda contract=contract: schema.validate_binary_contract(contract))
            if contract_path is not None:
                observed = read_prepared_authority_object(
                    contract_path, f"{context} contract output", problems
                )
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
        elif (
            track == "structural_traces"
            and identity.get("trace_kind") == "new_names"
        ):
            # Mirror the runner's derived structural new_names context
            # (run_rebaseline.evidence_plans) so the frozen context key set stays
            # exact.  appends_per_writer is the batches_per_writer (batch=1);
            # payload_size 250 / batch_size 1 are the canonical public values.
            expected_context["payload_size"] = 250
            expected_context["batch_size"] = 1
            expected_context["batches_per_writer"] = identity["appends_per_writer"]
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
        # Protocol v4: rehearsal outputs live in pre-declared directories,
        # accepted outputs in operator-created ones — the runner records
        # which applied; both self-consistency shapes are valid.
        "output_dir_absent_before": bool(provenance.get("rehearsal")),
        "partial": False,
        "failure_absent": True,
    }
    if provenance.get("rehearsal") not in (True, False):
        problems.add("provenance rehearsal flag invalid")
    if config is not None and provenance.get("rehearsal") != config.get("rehearsal"):
        problems.add("provenance rehearsal flag differs from config")
    # Protocol v4 §4: an accepted run carries the operator declaration hash;
    # a rehearsal carries none.  The two are mutually exclusive.
    declaration = provenance.get("declaration_sha256")
    if provenance.get("rehearsal"):
        if declaration is not None:
            problems.add("rehearsal provenance carries a declaration hash")
    elif not is_sha256(declaration):
        problems.add("accepted provenance lacks a declaration hash")
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
    approval = read_prepared_authority_object(
        approval_path, "source approval", problems
    )
    prepared = read_prepared_authority_object(
        prepared_path, "prepared artifacts", problems
    )
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
    validate_proof_only_path_after_children(prepared, child_records, problems)
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
    rehearsal = bool(provenance.get("rehearsal")) if provenance is not None else False
    outcome = provisional if evidence_valid else "INCONCLUSIVE"
    # Protocol v4 §3: rehearsal rows are non-evidence by construction.  A
    # rehearsal can never yield an ADMIT/NARROW/REVERT decision no matter how
    # clean its rows are; its terminal outcome is forced to INCONCLUSIVE so
    # it cannot be laundered into an accepted result.
    if rehearsal:
        outcome = "INCONCLUSIVE"
    if not evidence_valid:
        summary = dict(summary)
        reasons = dict(summary.get("reasons", {}))
        reasons["INCONCLUSIVE"] = ["invalid-or-incomplete-evidence"]
        summary["reasons"] = reasons
    elif rehearsal:
        summary = dict(summary)
        reasons = dict(summary.get("reasons", {}))
        reasons["INCONCLUSIVE"] = ["rehearsal-non-evidence"]
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
        "rehearsal": rehearsal,
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
    fixture_rustup_toolchain = "1.97.0-x86_64-unknown-linux-gnu"
    rustup_home = tooling / "rustup-home"
    cargo_path = (
        rustup_home / "toolchains" / fixture_rustup_toolchain / "bin" / "cargo"
    )
    rustc_path = (
        rustup_home / "toolchains" / fixture_rustup_toolchain / "bin" / "rustc"
    )
    rustc_host = "x86_64-unknown-linux-gnu"
    rust_lld_path = (
        rustup_home
        / "toolchains"
        / fixture_rustup_toolchain
        / "lib"
        / "rustlib"
        / rustc_host
        / "bin"
        / "rust-lld"
    )
    rustup_path = tooling / "host-tools" / "rustup"
    bwrap_path = tooling / "host-tools" / "bwrap"
    git_path = tooling / "host-tools" / "git"
    python_path = CURRENT_SYSTEM_PYTHON
    fixture_write_file(cargo_path, b"fixture cargo\n", 0o555)
    fixture_write_file(rustc_path, b"fixture rustc\n", 0o555)
    fixture_write_file(rust_lld_path, b"fixture rust-lld\n", 0o555)
    fixture_write_file(rustup_path, b"fixture rustup\n", 0o555)
    fixture_write_file(bwrap_path, b"fixture bwrap\n", 0o555)
    fixture_write_file(git_path, b"fixture git\n", 0o555)
    cargo_home = tooling / "cargo-home"
    cargo_home.mkdir()
    cargo_config = cargo_home / "config.toml"
    fixture_write_file(cargo_config, b"", 0o444)
    (cargo_home / "registry").mkdir()
    cargo_home.chmod(0o555)
    rustup_home.chmod(0o555)
    toolchain = {
        "bwrap_path": str(bwrap_path),
        "bwrap_sha256": sha256_file(bwrap_path),
        "cargo_home_path": str(cargo_home),
        "cargo_path": str(cargo_path),
        "cargo_sha256": sha256_file(cargo_path),
        "cargo_version_verbose": "cargo 1.97.0\nrelease: 1.97.0\nhost: x86_64-unknown-linux-gnu",
        "git_path": str(git_path),
        "git_sha256": sha256_file(git_path),
        "rustc_path": str(rustc_path),
        "rustc_sha256": sha256_file(rustc_path),
        "rustc_version_verbose": "rustc 1.97.0\nbinary: rustc\ncommit-hash: 1111111111111111111111111111111111111111\nhost: x86_64-unknown-linux-gnu\nrelease: 1.97.0",
        "rustc_host": rustc_host,
        "rust_lld_path": str(rust_lld_path),
        "rust_lld_sha256": sha256_file(rust_lld_path),
        "rustup_home_path": str(rustup_home),
        "rustup_path": str(rustup_path),
        "rustup_sha256": sha256_file(rustup_path),
        "rustup_toolchain": fixture_rustup_toolchain,
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

    def fixture_sandboxed_cargo_config_search(
        source_root: Path, name: str, *, output_path: Path | None = None
    ) -> dict[str, str]:
        path = (
            tooling / "cargo-config-search" / f"{name}.json"
            if output_path is None
            else output_path
        )
        fixture_write_file(path.with_name(f"{path.name}.empty"), b"")
        candidates: tuple[tuple[str, Path | None], ...] = (
            (
                f"{GUEST_SOURCE}/.cargo/config.toml",
                source_root / ".cargo/config.toml",
            ),
            (f"{GUEST_SOURCE}/.cargo/config", source_root / ".cargo/config"),
            (f"{GUEST_ROOT}/.cargo/config.toml", None),
            (f"{GUEST_ROOT}/.cargo/config", None),
            ("/.cargo/config.toml", None),
            ("/.cargo/config", None),
            (f"{GUEST_CARGO_HOME}/config.toml", cargo_home / "config.toml"),
            (f"{GUEST_CARGO_HOME}/config", cargo_home / "config"),
        )
        entries = []
        for guest_path, host_path in candidates:
            if host_path is not None and host_path.exists():
                entries.append(
                    {
                        "path": guest_path,
                        "sha256": sha256_file(host_path),
                        "status": "present",
                    }
                )
            elif guest_path in GUEST_BOUND_CONFIG_PATHS:
                entries.append(
                    {
                        "path": guest_path,
                        "sha256": EMPTY_SHA256,
                        "status": "present",
                    }
                )
            else:
                entries.append(
                    {"path": guest_path, "sha256": None, "status": "absent"}
                )
        fixture_write_json(
            path,
            {
                "cargo_home_path": GUEST_CARGO_HOME,
                "cwd": GUEST_SOURCE,
                "entries": entries,
                "schema": schema.CARGO_CONFIG_SEARCH_SCHEMA,
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

    semantic_manifest_root = root / "semantic-manifests"
    semantic_manifest_root.mkdir()

    def fixture_runtime_digest(authority: Mapping[str, Any]) -> str:
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
        return hashlib.sha256(
            canonical_json_bytes(
                {
                    "cargo_home": {
                        field: authority["cargo_home"][field]
                        for field in tree_fields
                    },
                    "schema": SEMANTIC_INPUT_AUTHORITY_SCHEMA,
                    "toolchain": {
                        field: authority["toolchain"][field]
                        for field in tree_fields
                    },
                    "trusted_system_closure": {
                        field: authority["trusted_system_closure"][field]
                        for field in closure_fields
                    },
                }
            )
        ).hexdigest()

    system_fixture_trees: list[dict[str, Any]] = []
    for host_path, guest_path in TRUSTED_SYSTEM_MOUNTS:
        metadata = host_path.stat()
        role = "system-" + guest_path.removeprefix("/").replace("/", "-")
        system_fixture_trees.append(
            {
                "entries": [
                    _semantic_entry(
                        metadata,
                        ".",
                        "directory",
                        None,
                        None,
                        None,
                        frozenset(),
                    )
                ],
                "role": role,
                "schema": RECURSIVE_TREE_AUTHORITY_SCHEMA,
            }
        )

    def fixture_semantic_authority(
        source_root: Path, label: str, *, source_role: str = "source"
    ) -> dict[str, Any]:
        safe_label = label.replace("/", "-")
        roots = SemanticReplay.live_roots(source_root, toolchain, label)
        bindings: dict[str, Any] = {}
        for name, role in (
            ("source", source_role),
            ("toolchain", "toolchain"),
            ("cargo_home", "cargo_home"),
        ):
            excluded = (
                frozenset({"Cargo.lock"})
                if source_role == "resolution_source_without_cargo_lock"
                and name == "source"
                else frozenset()
            )
            volatile = (
                frozenset({"."})
                if source_role == "resolution_source_without_cargo_lock"
                and name == "source"
                else frozenset()
            )
            tree = sample_semantic_tree(
                roots[name],
                role,
                f"fixture {label} {name}",
                allow_internal_symlinks=name != "source",
                hash_regular_contents=True,
                excluded_paths=excluded,
                volatile_directories=volatile,
            )
            path = semantic_manifest_root / f"{safe_label}-{name}.json"
            fixture_write_json(path, tree)
            entries = tree["entries"]
            bindings[name] = {
                "entry_count": len(entries),
                "equal_pre_post": True,
                "manifest_path": str(path.resolve()),
                "manifest_sha256": sha256_file(path),
                "mutation_events_absent": True,
                "role": role,
                "schema": RECURSIVE_TREE_AUTHORITY_SCHEMA,
                "watch_count": sum(
                    entry["file_type"] == "directory" for entry in entries
                ),
            }
        evidence_mounts = []
        mount_bindings = []
        for tree, (host_path, guest_path) in zip(
            system_fixture_trees, TRUSTED_SYSTEM_MOUNTS, strict=True
        ):
            root_entry = tree["entries"][0]
            evidence_mounts.append(
                {
                    "guest_path": guest_path,
                    "host_path": str(host_path),
                    "resolved_path": str(host_path),
                    "tree": tree,
                }
            )
            mount_bindings.append(
                {
                    "device": root_entry["device"],
                    "gid": root_entry["gid"],
                    "guest_path": guest_path,
                    "host_path": str(host_path),
                    "inode": root_entry["inode"],
                    "permissions": root_entry["permissions"],
                    "resolved_path": str(host_path),
                    "trusted_root_owned_non_writable": True,
                    "uid": root_entry["uid"],
                }
            )
        closure_manifest = {
            "mounts": evidence_mounts,
            "schema": TRUSTED_SYSTEM_CLOSURE_SCHEMA,
        }
        closure_path = semantic_manifest_root / f"{safe_label}-system.json"
        fixture_write_json(closure_path, closure_manifest)
        bindings["trusted_system_closure"] = {
            "entry_count": sum(
                len(tree["entries"]) for tree in system_fixture_trees
            ),
            "manifest_path": str(closure_path.resolve()),
            "mounts": mount_bindings,
            "mutation_events_absent": True,
            "schema": TRUSTED_SYSTEM_CLOSURE_SCHEMA,
            "sha256": sha256_file(closure_path),
            "watch_count": sum(
                sum(
                    entry["file_type"] == "directory"
                    for entry in tree["entries"]
                )
                for tree in system_fixture_trees
            ),
        }
        authority = {
            **bindings,
            "runtime_sha256": "",
            "schema": SEMANTIC_INPUT_AUTHORITY_SCHEMA,
        }
        authority["runtime_sha256"] = fixture_runtime_digest(authority)
        return authority

    prepared_semantic_authorities = {
        variant: fixture_semantic_authority(
            source_data[variant]["source_root"], f"prepared-{variant}"
        )
        for variant in schema.VARIANTS
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
                "controlled": False,
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
    reviewed_root = root / "source-review-inputs"
    reviewed_root.mkdir()
    current_child_artifacts: dict[str, Path] = {}
    for name, filename in (
        ("correctness", "ast-rb-check"), ("fault", "ast-rb-fault")
    ):
        destination = reviewed_root / "artifacts" / "tools" / filename
        fixture_write_file(destination, tool_paths[name].read_bytes(), 0o555)
        current_child_artifacts[name] = destination
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
    base_tools_manifest = copy.deepcopy(tools_manifest)
    base_tools_manifest["tools"].update({
        "correctness": {
            "comm": "ast-rb-check", "executable_mode": 0o555,
            "path": "/asterism/preapproval-placeholder/ast-rb-check",
            "sha256": "a48e573b0cbd89a11ece523fbc79e7d6a54aa42fa417e913f70861c0846ed3d6",
        },
        "fault": {
            "comm": "ast-rb-fault", "executable_mode": 0o555,
            "path": "/asterism/preapproval-placeholder/ast-rb-fault",
            "sha256": "a9826b2a400813f9c0ab0b9a8e6998c2c40bf3fc6bee07495552e6d23a7c8367",
        },
    })
    tools_manifest["tools"].update({
        name: {
            "path": str(path.resolve()), "sha256": sha256_file(path),
            "executable_mode": 0o555, "comm": path.name,
        }
        for name, path in current_child_artifacts.items()
    })
    tools_manifest_sha256 = hashlib.sha256(
        canonical_json_bytes(tools_manifest)
    ).hexdigest()
    current_materialized = reviewed_root / "materialized"
    current_build_directories = {
        "children": "children",
        "hooked_release": "hooked-release",
        "pristine_release": "pristine-release",
    }
    current_build_nonce = hashlib.sha256(b"fixture-current-build").hexdigest()
    current_product_commit = schema.VARIANT_SOURCE_BINDINGS["A"]["commit"]
    current_product_tree = schema.VARIANT_SOURCE_BINDINGS["A"]["tree"]
    fixture_current_release_payload = b"fixture current release\n"
    fixture_current_release_sha256 = hashlib.sha256(
        fixture_current_release_payload
    ).hexdigest()
    fixture_current_symbol_absence_sha256 = hashlib.sha256(
        b"fixture symbol absence"
    ).hexdigest()

    def fixture_current_file_identity(path: Path) -> dict[str, Any]:
        metadata = path.stat()
        return {
            "bytes": metadata.st_size, "ctime_ns": metadata.st_ctime_ns,
            "device": metadata.st_dev, "inode": metadata.st_ino,
            "link_count": metadata.st_nlink,
            "mode": stat.S_IMODE(metadata.st_mode),
            "mtime_ns": metadata.st_mtime_ns, "path": str(path.resolve()),
            "sha256": sha256_file(path), "size": metadata.st_size,
        }

    def fixture_current_directory_identity(path: Path) -> dict[str, Any]:
        metadata = path.stat()
        return {
            "changed_ns": metadata.st_ctime_ns, "device": metadata.st_dev,
            "file_type": stat.S_IFMT(metadata.st_mode), "inode": metadata.st_ino,
            "link_count": metadata.st_nlink,
            "modified_ns": metadata.st_mtime_ns, "path": str(path.resolve()),
            "permissions": stat.S_IMODE(metadata.st_mode), "size": metadata.st_size,
        }

    def fixture_freeze_tree(path: Path) -> None:
        for item in sorted(
            path.rglob("*"), key=lambda candidate: len(candidate.parts), reverse=True
        ):
            if item.is_dir():
                if stat.S_IMODE(item.stat().st_mode) != 0o555:
                    item.chmod(0o555)
            elif item.is_file():
                desired = 0o555 if item.stat().st_mode & 0o111 else 0o444
                if stat.S_IMODE(item.stat().st_mode) != desired:
                    item.chmod(desired)
            else:
                raise AssertionError(f"unsupported fixture node: {item}")
        if stat.S_IMODE(path.stat().st_mode) != 0o555:
            path.chmod(0o555)

    def fixture_current_manifest(path: Path) -> dict[str, Any]:
        return _current_materialized_manifest(path.resolve(), "fixture materialized")

    def fixture_current_path_chain(path: Path) -> list[dict[str, Any]]:
        paths = [Path("/"), *list(path.parents)[::-1][1:], path]
        records = []
        for item in paths:
            metadata = item.stat()
            records.append({
                "changed_ns": metadata.st_ctime_ns, "device": metadata.st_dev,
                "gid": metadata.st_gid, "inode": metadata.st_ino,
                "link_count": metadata.st_nlink,
                "mode": stat.S_IMODE(metadata.st_mode),
                "modified_ns": metadata.st_mtime_ns, "path": str(item),
                "size": metadata.st_size, "type": stat.S_IFMT(metadata.st_mode),
                "uid": metadata.st_uid,
            })
        return records

    def fixture_current_tool(path: Path, *, trusted: bool) -> dict[str, Any]:
        exact = path.resolve()
        return {
            "identity": fixture_current_file_identity(exact),
            "path_chain": fixture_current_path_chain(exact) if trusted else None,
            "trusted_system": trusted,
        }

    def fixture_current_null_device() -> dict[str, Any]:
        path = Path("/dev/null")
        metadata = path.lstat()
        return {
            "identity": {
                "changed_ns": metadata.st_ctime_ns,
                "device": metadata.st_dev,
                "gid": metadata.st_gid,
                "inode": metadata.st_ino,
                "link_count": metadata.st_nlink,
                "major": os.major(metadata.st_rdev),
                "minor": os.minor(metadata.st_rdev),
                "modified_ns": metadata.st_mtime_ns,
                "path": str(path),
                "permissions": stat.S_IMODE(metadata.st_mode),
                "size": metadata.st_size,
                "type": stat.S_IFMT(metadata.st_mode),
                "uid": metadata.st_uid,
            },
            "parent_path_chain": fixture_current_path_chain(path.parent),
            "trusted_system": True,
        }

    def fixture_current_config(
        authority: Mapping[str, Any], source_root: Path
    ) -> dict[str, Any]:
        entries = copy.deepcopy(cargo_config_authority["translated_entries"])
        def preserved_entries(base: Path) -> list[dict[str, Any]]:
            result = []
            for path in sorted(base.iterdir(), key=lambda item: item.name):
                if path.name in {"config", "config.toml"}:
                    continue
                directory = path.is_dir()
                result.append({
                    "identity": (
                        fixture_current_directory_identity(path)
                        if directory else fixture_current_file_identity(path)
                    ),
                    "name": path.name,
                    "type": "directory" if directory else "regular",
                })
            return result
        cargo_binding = authority["cargo_home"]
        return {
            "cargo_home_tree": {
                "entry_count": cargo_binding["entry_count"],
                "equal_pre_post": True, "path": cargo_binding["manifest_path"],
                "post_sha256": cargo_binding["manifest_sha256"],
                "pre_sha256": cargo_binding["manifest_sha256"],
                "watch_count": cargo_binding["watch_count"],
            },
            "cargo_search": {
                "cargo_home_path": GUEST_CARGO_HOME, "cwd": GUEST_SOURCE,
                "entries": entries, "schema": schema.CARGO_CONFIG_SEARCH_SCHEMA,
            },
            "preserved_top_level_entries": {
                "cargo-home": preserved_entries(cargo_home),
                "source": preserved_entries(source_root / ".cargo"),
            },
            "schema": "bn-30fs-build-cargo-config-search-v1",
        }

    def fixture_current_shared_sha256(source_root: Path) -> str:
        return hashlib.sha256(canonical_json_bytes({
            "entries": [
                {
                    "name": name,
                    "sha256": sha256_file(
                        source_root / CURRENT_SHARED_DESTINATION / name
                    ),
                    "size": (
                        source_root / CURRENT_SHARED_DESTINATION / name
                    ).stat().st_size,
                }
                for name in CURRENT_SHARED_NAMES
            ],
            "schema": "asterism-rebaseline-shared-v3",
        })).hexdigest()

    def fixture_current_build(
        name: str, directory: str, source_root: Path, authority: dict[str, Any],
        source_manifest_sha256: str, ordinal: int,
    ) -> dict[str, Any]:
        child = name == "children"
        target = reviewed_root / "targets" / directory
        target.mkdir(parents=True)
        expected_examples = (
            ("asterism_rebaseline_current_correctness", "asterism_rebaseline_current_fault")
            if child else ("asterism_rebaseline_public",)
        )
        artifacts: dict[str, Any] = {}
        for index, example in enumerate(expected_examples):
            source = target / "release" / "examples" / example
            if child:
                key = "correctness" if index == 0 else "fault"
                binding = tools_manifest["tools"][key]
                payload = Path(binding["path"]).read_bytes()
            else:
                published = reviewed_root / "artifacts" / "release" / (
                    "hooked-A" if name == "hooked_release" else "pristine-A"
                )
                payload = fixture_current_release_payload
                fixture_write_file(published, payload, 0o555)
                binding = {
                    "comm": published.name, "executable_mode": 0o555,
                    "path": str(published.resolve()), "sha256": sha256_file(published),
                }
            fixture_write_file(source, payload, 0o555)
            source.with_name(
                f"{example}-0123456789abcdef"
            ).hardlink_to(source)
            artifacts[example] = {
                "binding": binding, "source": fixture_current_file_identity(source)
            }
        target_identity = fixture_current_directory_identity(target)
        config_record = fixture_current_config(authority, source_root)
        base_environment = {
            "CARGO_HOME": GUEST_CARGO_HOME, "CARGO_INCREMENTAL": "0",
            "CARGO_NET_OFFLINE": "true", "GIT_CONFIG_COUNT": "0",
            "GIT_CONFIG_GLOBAL": f"{GUEST_ROOT}/absent-gitconfig",
            "GIT_CONFIG_NOSYSTEM": "1", "HOME": "/nonexistent",
            "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
            "LD_ORIGIN_PATH": GUEST_TOOLCHAIN_BIN, "PATH": "/usr/bin:/bin",
            "PYTHONDONTWRITEBYTECODE": "1", "PYTHONNOUSERSITE": "1",
            "RUSTC": GUEST_RUSTC, "RUSTUP_HOME": "/nonexistent",
            "RUSTUP_TOOLCHAIN": toolchain["rustup_toolchain"], "TZ": "UTC",
        }
        if child:
            environment = {
                **base_environment,
                "ASTERISM_FAULT_COMPILE_OUT_IDENTICAL": "true",
                "ASTERISM_FAULT_COMPILE_OUT_OVERLAY_RELEASE_SHA256": fixture_current_release_sha256,
                "ASTERISM_FAULT_COMPILE_OUT_PRISTINE_SHA256": fixture_current_release_sha256,
                "ASTERISM_FAULT_COMPILE_OUT_SCHEMA": "bn-2l3n-fault-compile-out-authority-v1",
                "ASTERISM_FAULT_COMPILE_OUT_SYMBOL_ABSENCE_SHA256": fixture_current_symbol_absence_sha256,
                "ASTERISM_REBASELINE_CHILD_BUILD_NONCE": current_build_nonce,
                "ASTERISM_REBASELINE_EXPECTED_LIB_SOURCE": "crates/mess-store/src/lib.rs",
                "ASTERISM_REBASELINE_PINNED_RUSTC": GUEST_RUSTC,
                "ASTERISM_REBASELINE_WRAPPER_RECEIPT": "/asterism/receipt/injection.json",
                "RUSTC_WORKSPACE_WRAPPER": "/asterism/rustc_workspace_wrapper.py",
            }
        else:
            environment = {
                **base_environment,
                "ASTERISM_BUILD_ADAPTER_SHA256": sha256_file(
                    source_root / CURRENT_ADAPTER_DESTINATION
                ),
                "ASTERISM_BUILD_BINARY_KIND": "public",
                "ASTERISM_BUILD_NONCE": current_build_nonce,
                "ASTERISM_BUILD_CARGO_LOCK_SHA256": sha256_file(source_root / "Cargo.lock"),
                "ASTERISM_BUILD_PRODUCT_COMMIT": current_product_commit,
                "ASTERISM_BUILD_PRODUCT_TREE": current_product_tree,
                "ASTERISM_BUILD_PROTOCOL": schema.PROTOCOL,
                "ASTERISM_BUILD_PROTOCOL_SHA256": schema.PROTOCOL_SHA256,
                "ASTERISM_BUILD_SHARED_MANIFEST_SHA256": (
                    fixture_current_shared_sha256(source_root)
                ),
                "ASTERISM_BUILD_SOURCE_APPROVAL_SHA256": "fa2acb626f303f8a65a16a6c8a1fd86b7e80cf48e092ae21a7308984ae790c94",
                "ASTERISM_BUILD_TIMED_SURFACE": "public-event-store",
                "ASTERISM_BUILD_TOOLING_COMMIT": fake_commit,
                "ASTERISM_BUILD_TOOLING_TREE": fake_tree,
                "ASTERISM_BUILD_VARIANT": "A",
            }
        descriptors = iter(str(900 + ordinal * 30 + index) for index in range(24))
        system = [("--ro-bind-fd", next(descriptors), guest) for _host, guest in TRUSTED_SYSTEM_MOUNTS]
        dev_null_fd = next(descriptors)
        source_fd, toolchain_fd, cargo_fd, rustc_fd, rust_lld_fd, python_fd = [
            next(descriptors) for _ in range(6)
        ]
        _source_config_guard_fd = next(descriptors)
        cargo_home_fd, target_fd = [next(descriptors) for _ in range(2)]
        argv = [str(bwrap_path), "--die-with-parent", "--new-session", "--unshare-net", "--dir", "/usr"]
        argv.extend(argument for binding in system for argument in binding)
        argv.extend([
            "--symlink", "usr/bin", "/bin",
            "--symlink", "usr/lib", "/lib",
            "--symlink", "usr/lib", "/lib64",
            "--dir", "/dev",
            "--dev-bind", f"/proc/self/fd/{dev_null_fd}", "/dev/null",
            "--dir", "/proc", "--tmpfs", "/tmp", "--tmpfs", GUEST_ROOT,
        ])
        rust_lld_guest_path = (
            f"{GUEST_TOOLCHAIN_ROOT}/lib/rustlib/{rustc_host}"
            "/bin/gcc-ld/ld.lld"
        )
        for binding in (
            ("--ro-bind-fd", source_fd, GUEST_SOURCE),
            ("--ro-bind-fd", toolchain_fd, GUEST_TOOLCHAIN_ROOT),
            ("--ro-bind-fd", cargo_fd, GUEST_CARGO),
            ("--ro-bind-fd", rustc_fd, GUEST_RUSTC),
            ("--ro-bind-fd", rust_lld_fd, rust_lld_guest_path),
            ("--ro-bind-fd", python_fd, f"{GUEST_ROOT}/python3"),
        ):
            argv.extend(binding)
        argv.extend([
            "--dir", f"{GUEST_SOURCE}/.cargo",
            "--tmpfs", f"{GUEST_SOURCE}/.cargo",
        ])
        for entry in config_record["preserved_top_level_entries"]["source"]:
            argv.extend((
                "--ro-bind-data" if entry["type"] == "regular" else "--ro-bind-fd",
                next(descriptors),
                f"{GUEST_SOURCE}/.cargo/{entry['name']}",
            ))
        for entry in config_record["cargo_search"]["entries"][:2]:
            if entry["status"] == "present":
                argv.extend(("--ro-bind-data", next(descriptors), entry["path"]))
        argv.extend([
            "--remount-ro", f"{GUEST_SOURCE}/.cargo",
            "--overlay-src", f"/proc/self/fd/{cargo_home_fd}",
            "--tmp-overlay", GUEST_CARGO_HOME,
        ])
        for entry in config_record["cargo_search"]["entries"][6:]:
            if entry["status"] == "present":
                argv.extend(("--ro-bind-data", next(descriptors), entry["path"]))
        argv.extend([
            "--remount-ro", GUEST_CARGO_HOME,
            "--dir", f"{GUEST_ROOT}/.cargo",
            "--tmpfs", f"{GUEST_ROOT}/.cargo",
            "--remount-ro", f"{GUEST_ROOT}/.cargo",
            "--dir", "/.cargo", "--tmpfs", "/.cargo",
            "--remount-ro", "/.cargo",
            "--bind-fd", target_fd, GUEST_TARGET,
        ])
        receipt_bind = None
        if child:
            wrapper_fd, receipt_fd = next(descriptors), next(descriptors)
            argv.extend(["--ro-bind-fd", wrapper_fd, f"{GUEST_ROOT}/rustc_workspace_wrapper.py", "--bind-fd", receipt_fd, f"{GUEST_ROOT}/receipt"])
            receipt_bind = receipt_fd
        argv.extend(["--chdir", GUEST_SOURCE, GUEST_CARGO, "build", "--locked", "--offline", "--release", "-p", "mess-store"])
        for example in expected_examples:
            argv.extend(["--example", example])
        argv.extend(["--target-dir", GUEST_TARGET])
        passed_file_descriptors = sum(
            argument in {"--bind-fd", "--ro-bind-fd", "--ro-bind-data"}
            for argument in argv
        ) + 2 + 2 + len(
            config_record["preserved_top_level_entries"]["cargo-home"]
        )
        for example in expected_examples:
            artifacts[example]["source"]["path"] = (
                f"/proc/self/fd/{target_fd}/release/examples/{example}"
            )
        execution_tools = {
            "bwrap": fixture_current_tool(bwrap_path, trusted=True),
            "cargo": fixture_current_tool(cargo_path, trusted=False),
            "dev_null": fixture_current_null_device(),
            "python": fixture_current_tool(python_path, trusted=True),
            "rustc": fixture_current_tool(rustc_path, trusted=False),
            "rust_lld": fixture_current_tool(rust_lld_path, trusted=False),
            "toolchain_root": fixture_current_directory_identity(cargo_path.parent.parent),
        }
        lock_path = source_root / "Cargo.lock"
        lock_metadata = lock_path.stat()
        lock_identity = {
            "changed_ns": lock_metadata.st_ctime_ns,
            "device": lock_metadata.st_dev, "inode": lock_metadata.st_ino,
            "link_count": lock_metadata.st_nlink,
            "modified_ns": lock_metadata.st_mtime_ns,
        }
        lock_record = {
            "identity": lock_identity, "mode": 0o444,
            "path": str(lock_path.resolve()), "sha256": sha256_file(lock_path),
            "size": lock_metadata.st_size,
        }
        result = {
            "argv": argv, "environment": environment,
            "execution": {
                "argv": argv, "cwd": str(reviewed_root.resolve()),
                "environment": environment, "execution_authority": execution_tools["bwrap"],
                "exit_status": 0,
                "passed_file_descriptors": passed_file_descriptors,
                "stderr_bytes": 0, "stderr_sha256": EMPTY_SHA256,
                "stdout_bytes": 0, "stdout_sha256": EMPTY_SHA256,
            },
            "filesystem_admission": {
                "available_bytes": 200_000_000_000, "available_inodes": 2_000_000,
                "checked_path": str(root.resolve()),
                "filesystem": schema.REQUIRED_FILESYSTEM_TYPE,
                "minimum_available_bytes": schema.MIN_FREE_BYTES,
                "minimum_available_inodes": schema.MIN_FREE_INODES,
                "schema": schema.FILESYSTEM_ADMISSION_SCHEMA,
            },
            "cargo_config_prebuild": config_record,
            "cargo_config_postbuild": json.loads(json.dumps(config_record)),
            "execution_tools": execution_tools, "artifacts": artifacts,
            "binds": {"target": {"parent": fixture_current_directory_identity(target.parent), "post": target_identity, "pre": target_identity}},
            "lock_prebuild": lock_record,
            "lock_postbuild": json.loads(json.dumps(lock_record)),
            "source_manifest_sha256": source_manifest_sha256,
            "semantic_input_authority": authority,
            "toolchain_manifest": {
                "entry_count": authority["toolchain"]["entry_count"],
                "equal_pre_post": True,
                "path": authority["toolchain"]["manifest_path"],
                "post_sha256": authority["toolchain"]["manifest_sha256"],
                "pre_sha256": authority["toolchain"]["manifest_sha256"],
            },
            "target": str(target.resolve()), "target_was_absent": True,
        }
        if child:
            wrapper = reviewed_root / "inputs" / "rustc_workspace_wrapper.py"
            fixture_write_file(wrapper, b"fixture workspace wrapper\n", 0o555)
            receipt_root = reviewed_root / "receipts" / directory
            receipt = {
                "build_nonce": current_build_nonce, "crate_name": "mess_store",
                "crate_type": "lib",
                "injected_arguments": ["--cfg", "test", "--allow", "explicit_builtin_cfgs_in_flags", "--cfg", "asterism_rebaseline_correctness", "--check-cfg", "cfg(asterism_rebaseline_correctness)"],
                "original_argv_sha256": hashlib.sha256(b"fixture rustc argv").hexdigest(),
                "package": "mess-store", "rustc": GUEST_RUSTC,
                "schema": "bn-30fs-rustc-workspace-wrapper-receipt-v1",
                "source": "crates/mess-store/src/lib.rs",
            }
            receipt_path = receipt_root / "injection.json"
            fixture_write_json(receipt_path, receipt)
            receipt_identity = fixture_current_file_identity(receipt_path)
            receipt_identity["path"] = (
                f"/proc/self/fd/{receipt_bind}/injection.json"
            )
            receipt_directory = fixture_current_directory_identity(receipt_root)
            result["binds"]["receipt"] = {
                "parent": fixture_current_directory_identity(receipt_root.parent),
                "post": receipt_directory, "pre": receipt_directory,
            }
            result.update({
                "wrapper_receipt": receipt,
                "wrapper_receipt_identity": receipt_identity,
                "wrapper_receipt_sha256": receipt_identity["sha256"],
                "wrapper_input_identity": fixture_current_file_identity(wrapper),
            })
        fixture_write_json(
            reviewed_root / "logs" / f"cargo-build-{directory}.json",
            result["execution"],
        )
        return result

    current_sources: dict[str, Path] = {}
    current_authorities: dict[str, dict[str, Any]] = {}
    current_manifest_sha256s: dict[str, str] = {}
    current_placements: dict[str, list[dict[str, Any]]] = {}
    repository = Path(__file__).parents[2].resolve()
    current_tooling = repository / "spikes" / "asterism_rebaseline" / "tooling"
    current_dir = current_tooling / "current"
    current_shared = current_tooling / "overlay" / "shared"
    current_public = current_tooling / "overlay" / "public"
    resolution_root = root / "resolution-authority"
    product_lock_payload = (repository / "Cargo.lock").read_bytes()
    if hashlib.sha256(product_lock_payload).hexdigest() != schema.CURRENT_LOCK_SHA256:
        raise AssertionError("fixture product Cargo.lock differs from frozen authority")
    product_engine_payload = (repository / CURRENT_ENGINE_PATH).read_bytes()
    if (
        hashlib.sha256(product_engine_payload).hexdigest()
        != CURRENT_PRODUCT_ENGINE_SHA256
    ):
        raise AssertionError("fixture product engine differs from frozen authority")
    product_patch_payload = (
        current_dir / "product-test-overlay.patch"
    ).read_bytes()
    patched_engine_payload = _current_apply_product_overlay(
        product_engine_payload, product_patch_payload, "fixture product overlay"
    )
    archive_files = {
        ".cargo/preserved.txt": b"fixture preserved Cargo entry\n",
        "Cargo.lock": b"fixture archived Cargo.lock replaced during materialization\n",
        CURRENT_ENGINE_PATH.as_posix(): product_engine_payload,
        "src/lib.rs": b"fixture current source\n",
    }

    def fixture_product_archive(files: Mapping[str, bytes]) -> bytes:
        directories = {
            parent.as_posix()
            for name in files
            for parent in PurePosixPath(name).parents
            if parent.parts
        }
        output = io.BytesIO()
        with tarfile.open(
            fileobj=output, mode="w:", format=tarfile.USTAR_FORMAT
        ) as archive:
            for name in sorted(
                directories,
                key=lambda candidate: (
                    len(PurePosixPath(candidate).parts), candidate
                ),
            ):
                info = tarfile.TarInfo(name + "/")
                info.type = tarfile.DIRTYPE
                info.mode = 0o755
                info.mtime = 0
                archive.addfile(info)
            for name in sorted(files):
                payload = files[name]
                info = tarfile.TarInfo(name)
                info.mode = 0o644
                info.mtime = 0
                info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))
        return output.getvalue()

    archive_path = reviewed_root / "archives" / "source-A.tar"
    fixture_write_file(
        archive_path, fixture_product_archive(archive_files), 0o444
    )
    for name, directory in current_build_directories.items():
        source_root = current_materialized / directory
        for relative, archived_payload in archive_files.items():
            payload = (
                product_lock_payload
                if relative == "Cargo.lock"
                else (
                    patched_engine_payload
                    if relative == CURRENT_ENGINE_PATH.as_posix()
                    and directory != "pristine-release"
                    else archived_payload
                )
            )
            fixture_write_file(source_root / relative, payload)
        placements: list[dict[str, Any]] = []
        sources_and_destinations = (
            (
                (current_dir / "correctness.rs", Path(
                    "crates/mess-store/examples/asterism_rebaseline_current_correctness.rs"
                )),
                (current_dir / "fault.rs", Path(
                    "crates/mess-store/examples/asterism_rebaseline_current_fault.rs"
                )),
                *((current_shared / shared_name,
                   CURRENT_SHARED_DESTINATION / shared_name)
                  for shared_name in CURRENT_SHARED_NAMES),
            )
            if name == "children"
            else (
                (current_public / "main.rs", Path(
                    "crates/mess-store/examples/asterism_rebaseline_public.rs"
                )),
                (current_public / "adapters" / "current.rs",
                 CURRENT_ADAPTER_DESTINATION),
                *((current_shared / shared_name,
                   CURRENT_SHARED_DESTINATION / shared_name)
                  for shared_name in CURRENT_SHARED_NAMES),
            )
        )
        for source, relative_destination in sources_and_destinations:
            destination = source_root / relative_destination
            fixture_write_file(destination, source.read_bytes(), 0o444)
            placements.append({
                "destination": destination.as_posix(), "mode": 0o444,
                "sha256": sha256_file(source), "source": str(source),
            })
        fixture_freeze_tree(source_root)
        current_sources[name] = source_root
        current_placements[directory] = placements
        materialized_manifest_path = (
            reviewed_root / "manifests" / f"materialized-{directory}.json"
        )
        fixture_write_json(
            materialized_manifest_path, fixture_current_manifest(source_root)
        )
        current_manifest_sha256s[name] = sha256_file(materialized_manifest_path)
        current_authorities[name] = fixture_semantic_authority(
            source_root, f"current-{name}"
        )

    resolution_a_source = resolution_root / "materialized" / "A"
    fixture_write_file(
        resolution_a_source / "src" / "lib.rs", b"fixture resolution A\n"
    )
    fixture_write_file(resolution_a_source / "Cargo.lock", product_lock_payload)
    reviewed_cargo_config_binding = fixture_sandboxed_cargo_config_search(
        resolution_a_source,
        "resolution-A",
        output_path=resolution_root / "manifests" / "cargo-config-A.json",
    )
    reviewed_cargo_config_path = Path(
        reviewed_cargo_config_binding["path"]
    )
    cargo_config_recorded = schema.parse_canonical_json_object(
        reviewed_cargo_config_path.read_bytes(),
        "fixture reviewed Cargo config",
    )
    cargo_config_translated = copy.deepcopy(cargo_config_recorded["entries"])
    cargo_config_authority = {
        "binding": reviewed_cargo_config_binding,
        "identity": fixture_current_file_identity(reviewed_cargo_config_path),
        "recorded": cargo_config_recorded,
        "translated_entries": cargo_config_translated,
    }

    archive_identity = {
        "bytes": archive_path.stat().st_size,
        "commit": current_product_commit,
        "sha256": sha256_file(archive_path),
        "tree": current_product_tree,
    }
    construction_path = (
        reviewed_root / "manifests" / "current-children-construction.json"
    )
    construction = {
        "archive": archive_identity,
        "cargo_config_manifest_sha256": cargo_config_authority["identity"]["sha256"],
        "cargo_config_view_sha256": hashlib.sha256(
            canonical_json_bytes(cargo_config_translated)
        ).hexdigest(),
        "kinds": {
            directory: {
                "manifest_sha256": current_manifest_sha256s[name],
                "placements": current_placements[directory],
            }
            for name, directory in current_build_directories.items()
        },
        "lock_sha256": schema.CURRENT_LOCK_SHA256,
        "product_commit": current_product_commit,
        "product_overlay_sha256": sha256_file(
            current_dir / "product-test-overlay.patch"
        ),
        "product_tree": current_product_tree,
        "protocol": schema.PROTOCOL,
        "schema": CURRENT_CONSTRUCTION_SCHEMA,
    }
    fixture_write_json(construction_path, construction)
    current_build_nonce = sha256_file(construction_path)
    current_builds: dict[str, Any] = {
        name: fixture_current_build(
            name, directory, current_sources[name], current_authorities[name],
            current_manifest_sha256s[name], ordinal,
        )
        for ordinal, (name, directory) in enumerate(
            current_build_directories.items(), start=1
        )
    }

    def fixture_resolution_argv(
        ordinal: int, cargo_arguments: Sequence[str]
    ) -> list[str]:
        descriptors = [str(700 + ordinal * 20 + index) for index in range(12)]
        system_bindings = tuple(
            ("--ro-bind-fd", descriptors[index], guest_path)
            for index, (_host_path, guest_path) in enumerate(TRUSTED_SYSTEM_MOUNTS)
        )
        core_bindings = (
            ("--bind-fd", descriptors[3], GUEST_SOURCE),
            ("--ro-bind-fd", descriptors[4], GUEST_TOOLCHAIN_ROOT),
            ("--ro-bind-fd", descriptors[5], GUEST_CARGO),
            ("--ro-bind-fd", descriptors[6], GUEST_RUSTC),
        )
        config_bindings = tuple(
            ("--ro-bind-fd", descriptors[8 + index], guest_path)
            for index, guest_path in enumerate(GUEST_BOUND_CONFIG_PATHS)
        )
        return [
            str(bwrap_path),
            "--die-with-parent",
            "--new-session",
            "--unshare-net",
            "--dir",
            "/usr",
            *(argument for binding in system_bindings for argument in binding),
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
            GUEST_ROOT,
            *(argument for binding in core_bindings for argument in binding),
            "--overlay-src",
            f"/proc/self/fd/{descriptors[7]}",
            "--tmp-overlay",
            GUEST_CARGO_HOME,
            "--dir",
            f"{GUEST_ROOT}/.cargo",
            "--tmpfs",
            f"{GUEST_ROOT}/.cargo",
            "--remount-ro",
            f"{GUEST_ROOT}/.cargo",
            "--dir",
            "/.cargo",
            "--tmpfs",
            "/.cargo",
            "--remount-ro",
            "/.cargo",
            "--dir",
            f"{GUEST_SOURCE}/.cargo",
            "--tmpfs",
            f"{GUEST_SOURCE}/.cargo",
            *(argument for binding in config_bindings[:2] for argument in binding),
            "--remount-ro",
            f"{GUEST_SOURCE}/.cargo",
            *(argument for binding in config_bindings[2:] for argument in binding),
            "--remount-ro",
            GUEST_CARGO_HOME,
            "--chdir",
            GUEST_SOURCE,
            GUEST_CARGO,
            *cargo_arguments,
        ]

    resolution_variants: dict[str, Any] = {}
    fixture_current_lock = product_lock_payload
    fixture_current_lock_sha256 = hashlib.sha256(fixture_current_lock).hexdigest()
    for ordinal, variant in enumerate(schema.VARIANTS, start=1):
        source_root = resolution_root / "materialized" / variant
        if variant != "A":
            fixture_write_file(
                source_root / "src" / "lib.rs",
                f"fixture resolution {variant}\n".encode(),
            )
        final_lock_payload = (
            fixture_current_lock
            if variant in {"A", "B"}
            else f"fixture final lock {variant}\n".encode()
        )
        final_lock_sha256 = hashlib.sha256(final_lock_payload).hexdigest()
        if variant != "A":
            fixture_write_file(source_root / "Cargo.lock", final_lock_payload)
        final_lock = resolution_root / "locks" / f"Cargo-{variant}.lock"
        fixture_write_file(final_lock, final_lock_payload)
        config_binding = (
            copy.deepcopy(reviewed_cargo_config_binding)
            if variant == "A"
            else fixture_sandboxed_cargo_config_search(
                source_root,
                f"resolution-{variant}",
                output_path=(
                    resolution_root
                    / "manifests"
                    / f"cargo-config-{variant}.json"
                ),
            )
        )
        tracked_record = {
            "argv": [
                str(git_path),
                "-C",
                str(Path(__file__).parents[2].resolve()),
                "show",
                f"{'3' * 40}:Cargo.lock",
            ],
            "cargo_config_search": config_binding,
            "cwd": str(Path(__file__).parents[2].resolve()),
            "environment": frozen_cargo_environment(toolchain),
            "exit_status": 0,
            "host_source_root": str(source_root.resolve()),
            "resolver_kind": "tracked_git_readback",
            "stderr": "",
            "stderr_sha256": EMPTY_SHA256,
            "stdout": final_lock_payload.decode(),
            "stdout_sha256": final_lock_sha256,
            "toolchain": toolchain,
        }
        claim: dict[str, Any] = {
            "current_lock_attempt": None,
            "final_lock_path": str(final_lock.resolve()),
            "final_lock_sha256": final_lock_sha256,
            "historical_lock": {
                "commit": "3" * 40,
                "path": "Cargo.lock",
                "sha256": fixture_current_lock_sha256,
            },
            "resolver": tracked_record,
        }
        if variant in {"C", "D"}:
            sandbox_environment = sandboxed_cargo_environment(toolchain, {})

            def resolver_record(label: str, index: int) -> dict[str, Any]:
                cargo_arguments = (
                    [
                        "metadata",
                        "--locked",
                        "--offline",
                        "--format-version",
                        "1",
                        "--no-deps",
                    ]
                    if label == "current"
                    else ["generate-lockfile", "--offline"]
                )
                return {
                    "argv": fixture_resolution_argv(index, cargo_arguments),
                    "cargo_config_search": config_binding,
                    "cwd": GUEST_SOURCE,
                    "environment": sandbox_environment,
                    "execution_authority": (
                        lambda metadata: {
                            "identity": {
                                "changed_ns": metadata.st_ctime_ns,
                                "device": metadata.st_dev,
                                "inode": metadata.st_ino,
                                "link_count": metadata.st_nlink,
                                "modified_ns": metadata.st_mtime_ns,
                            },
                            "mode": stat.S_IMODE(metadata.st_mode),
                            "path": str(bwrap_path.resolve()),
                            "sha256": sha256_file(bwrap_path),
                            "size": metadata.st_size,
                        }
                    )(bwrap_path.stat()),
                    "exit_status": 0,
                    "host_source_root": str(source_root.resolve()),
                    "lock_output": (
                        {
                            boundary: {
                                "path": str(source_root / "Cargo.lock"),
                                "sha256": fixture_current_lock_sha256,
                                "status": "present",
                            }
                            for boundary in ("post", "pre")
                        }
                        if label == "current"
                        else {
                            "post": {
                                "path": str(source_root / "Cargo.lock"),
                                "sha256": final_lock_sha256,
                                "status": "present",
                            },
                            "pre": {
                                "path": str(source_root / "Cargo.lock"),
                                "sha256": None,
                                "status": "absent",
                            },
                        }
                    ),
                    "passed_file_descriptors": 13,
                    "resolver_kind": "sandboxed_cargo_resolution",
                    "semantic_input_authority": fixture_semantic_authority(
                        source_root,
                        f"resolver-{variant}-{label}",
                        source_role="resolution_source_without_cargo_lock",
                    ),
                    "stderr": "",
                    "stderr_sha256": EMPTY_SHA256,
                    "stdout": "",
                    "stdout_sha256": EMPTY_SHA256,
                    "toolchain": toolchain,
                }

            claim["current_lock_attempt"] = resolver_record(
                "current", ordinal * 2
            )
            claim["resolver"] = resolver_record("generated", ordinal * 2 + 1)
        resolution_variants[variant] = claim

    base_tools_path = reviewed_root / "base-tools-manifest.json"
    fixture_write_json(base_tools_path, base_tools_manifest)
    reviewed_tools_path = reviewed_root / "asterism-rebaseline-tools.json"
    fixture_write_json(reviewed_tools_path, tools_manifest)
    lock_manifest_path = reviewed_root / "lock-manifest.json"
    lock_manifest_payload = {
        "schema": "asterism-rebaseline-lock-candidates-v3",
        "source_plan_path": str(
            (
                Path(__file__).parents[2]
                / "spikes/asterism_rebaseline/tooling/source-plan.json"
            ).resolve()
        ),
        "toolchain": toolchain,
        "variants": resolution_variants,
    }
    fixture_write_json(
        lock_manifest_path,
        lock_manifest_payload,
    )
    lock_review_path = reviewed_root / "lock-review-bundle.json"
    fixture_write_json(
        lock_review_path,
        {"schema": schema.SOURCE_REVIEW_CONTENT_SCHEMAS["lock_review_bundle"]},
    )

    def source_review_input(path: Path) -> dict[str, Any]:
        metadata = path.stat()
        return {
            "identity": {
                "changed_ns": metadata.st_ctime_ns,
                "device": metadata.st_dev,
                "inode": metadata.st_ino,
                "link_count": metadata.st_nlink,
                "modified_ns": metadata.st_mtime_ns,
            },
            "mode": stat.S_IMODE(metadata.st_mode),
            "path": str(path.resolve()),
            "schema": schema.SOURCE_REVIEW_INPUT_SCHEMA,
            "sha256": sha256_file(path),
            "size": metadata.st_size,
        }

    lock_manifest_input = source_review_input(lock_manifest_path)
    lock_review_input = source_review_input(lock_review_path)
    lock_authority_path = reviewed_root / "lock-authority.json"
    lock_authority = {
        "lock_manifest": {
            "payload": lock_manifest_payload,
            "schema": "asterism-rebaseline-lock-candidates-v3",
            "sha256": lock_manifest_input["sha256"],
        },
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "review_sha256": lock_review_input["sha256"],
        "review_bundle": {
            "payload": {
                "schema": schema.SOURCE_REVIEW_CONTENT_SCHEMAS[
                    "lock_review_bundle"
                ]
            },
            "schema": schema.SOURCE_REVIEW_CONTENT_SCHEMAS[
                "lock_review_bundle"
            ],
            "sha256": lock_review_input["sha256"],
        },
        "schema": schema.SOURCE_REVIEW_CONTENT_SCHEMAS["lock_authority"],
        "status": "approved",
        "tooling_commit": fake_commit,
        "tooling_tree": fake_tree,
    }
    fixture_write_json(lock_authority_path, lock_authority)
    lock_authority_input = source_review_input(lock_authority_path)
    product_overlay_sha256 = sha256_file(
        current_dir / "product-test-overlay.patch"
    )

    def fixture_current_immutable(path: Path) -> dict[str, Any]:
        metadata = path.stat()
        return {
            "identity": {
                "changed_ns": metadata.st_ctime_ns,
                "device": metadata.st_dev, "inode": metadata.st_ino,
                "link_count": metadata.st_nlink,
                "modified_ns": metadata.st_mtime_ns,
            },
            "mode": stat.S_IMODE(metadata.st_mode), "path": str(path.resolve()),
            "sha256": sha256_file(path), "size": metadata.st_size,
        }

    lock_candidate_paths = {
        name: Path(resolution_variants[name]["final_lock_path"])
        for name in ("A", "C", "D")
    }
    lock_candidates = {
        name: fixture_current_immutable(path)
        for name, path in lock_candidate_paths.items()
    }

    validator_environment = {
        "HOME": "/nonexistent", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1", "TZ": "UTC",
    }

    def fixture_validator_execution(
        script: Path, result: Mapping[str, Any], *, self_test: bool
    ) -> dict[str, Any]:
        stdout = canonical_json_bytes(result)
        return {
            "argv": [
                str(CURRENT_SYSTEM_PYTHON), "-I", "-B", str(script),
                *(("--self-test",) if self_test else ()),
            ],
            "cwd": str(repository), "environment": validator_environment,
            "execution_authority": fixture_current_tool(
                CURRENT_SYSTEM_PYTHON, trusted=True
            ),
            "exit_status": 0, "passed_file_descriptors": 2,
            "script_authority": fixture_current_tool(script, trusted=False),
            "stderr_bytes": 0, "stderr_sha256": EMPTY_SHA256,
            "stdout_bytes": len(stdout),
            "stdout_sha256": hashlib.sha256(stdout).hexdigest(),
        }

    fault_normal = {
        "checks": ["fault-source-contract", "fault-hostile-contract"],
        "hostile_mutations_rejected": 0,
        "schema": "bn-20be-current-fault-validator-v1", "status": "ok",
    }
    fault_self_test = {
        **fault_normal, "hostile_mutations_rejected": 2,
    }
    fault_validator_path = current_dir / "validate_fault.py"
    fault_authority = {
        "executions": [
            fixture_validator_execution(
                fault_validator_path, fault_normal, self_test=False
            ),
            fixture_validator_execution(
                fault_validator_path, fault_self_test, self_test=True
            ),
        ],
        "normal": fault_normal, "self_test": fault_self_test,
        "source": fixture_current_file_identity(current_dir / "fault.rs"),
        "validator": fixture_current_file_identity(fault_validator_path),
    }
    static_normal = {
        "checks": ["builder-static-contract", "builder-source-contract"],
        "hostile_mutations_rejected": 0,
        "schema": "bn-30fs-build-children-validator-v1", "status": "ok",
    }
    static_self_test = {
        **static_normal, "hostile_mutations_rejected": 2,
    }
    static_validator_path = current_dir / "validate_build_children.py"
    static_authority = {
        "executions": [
            fixture_validator_execution(
                static_validator_path, static_normal, self_test=False
            ),
            fixture_validator_execution(
                static_validator_path, static_self_test, self_test=True
            ),
        ],
        "normal": static_normal, "self_test": static_self_test,
        "validator": fixture_current_file_identity(static_validator_path),
    }
    lock_validation = {
        "execution_authority": {
            name: fixture_current_tool(
                Path(toolchain[f"{name}_path"]),
                trusted=name in {"bwrap", "git", "rustup"},
            )
            for name in ("bwrap", "cargo", "git", "rustc", "rustup")
        },
        "result": {
            "authority_sha256": lock_authority_input["sha256"],
            "lock_manifest_sha256": lock_manifest_input["sha256"],
            "schema": "bn-31gp-current-lock-authority-validation-v1",
            "status": "ok",
        },
        "semantic_validator": "descriptor-cross-bound-authority-context-v1",
    }
    current_input_paths = (
        current_dir / "correctness.rs",
        current_dir / "fault.rs",
        current_dir / "validate_fault.py",
        current_dir / "lock_authority.py",
        current_tooling / "prepare_overlays.py",
        current_tooling / "overlay_pins.py",
        current_dir / "product-test-overlay.patch",
        current_dir / "validate_product_test_overlay.py",
        current_dir / "rustc_workspace_wrapper.py",
        current_dir / "validate_build_children.py",
        current_public / "main.rs",
        current_public / "adapters" / "current.rs",
        *(current_shared / name for name in CURRENT_SHARED_NAMES),
        lock_manifest_path, lock_authority_path, lock_review_path,
        base_tools_path,
        *(lock_candidate_paths[name] for name in ("A", "C", "D")),
        reviewed_cargo_config_path,
    )
    current_inputs = [
        fixture_current_file_identity(path) for path in current_input_paths
    ]
    current_toolchain_identities = [
        fixture_current_file_identity(Path(toolchain[f"{name}_path"]))
        for name in ("bwrap", "cargo", "git", "rustc", "rust_lld", "rustup")
    ]
    nm_path = reviewed_root / "nm"
    fixture_write_file(nm_path, b"fixture reviewed nm\n", 0o555)
    current_release_sha256 = current_builds["hooked_release"]["artifacts"][
        "asterism_rebaseline_public"
    ]["binding"]["sha256"]
    if current_release_sha256 != fixture_current_release_sha256:
        raise AssertionError("fixture release hash differs")
    current_symbol_absence_sha256 = fixture_current_symbol_absence_sha256
    preapproval = {
        "binary_byte_identical": True,
        "forbidden_hook_strings": list(
            schema.RELEASE_COMPILE_OUT_FORBIDDEN_HOOK_STRINGS
        ),
        "nm": fixture_current_tool(nm_path, trusted=True),
        "preapproval_source_sentinel": (
            "fa2acb626f303f8a65a16a6c8a1fd86b7e80cf48e092ae21a7308984ae790c94"
        ),
        "overlay_release_sha256": current_release_sha256,
        "pristine_sha256": current_release_sha256,
        "symbol_absence_sha256": current_symbol_absence_sha256,
        "symbol_inventory_byte_identical": True,
    }
    current_children_path = reviewed_root / "current-children-attestation.json"
    current_children = {
        "artifacts": {
            name: tools_manifest["tools"][name]
            for name in ("correctness", "fault")
        },
        "build_nonce": current_build_nonce,
        "builds": current_builds,
        "cargo_config_authority": cargo_config_authority,
        "construction_path": str(construction_path.resolve()),
        "construction_sha256": sha256_file(construction_path),
        "fault_authority": fault_authority,
        "inputs": current_inputs,
        "lock_authority": lock_authority,
        "lock_authority_inputs": {
            "authority": {
                key: value
                for key, value in lock_authority_input.items()
                if key != "schema"
            },
            "lock_manifest": {
                key: value
                for key, value in lock_manifest_input.items()
                if key != "schema"
            },
            "review_bundle": {
                key: value
                for key, value in lock_review_input.items()
                if key != "schema"
            },
        },
        "lock_authority_validation": lock_validation,
        "lock_candidates": lock_candidates,
        "lock_manifest_sha256": lock_manifest_input["sha256"],
        "prebuild_filesystem_admissions": {
            name: build["filesystem_admission"]
            for name, build in current_builds.items()
        },
        "product_commit": current_product_commit,
        "product_overlay_authority": {
            "patch": {"sha256": product_overlay_sha256}
        },
        "product_tree": current_product_tree,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "release_compile_out": preapproval,
        "release_compile_out_approval": {
            "final_integration_action": (
                "repeat-release-equality-proof-under-real-source-approval"
            ),
            "source_approval_sha256": (
                "fa2acb626f303f8a65a16a6c8a1fd86b7e80cf48e092ae21a7308984ae790c94"
            ),
            "source_approval_status": "preapproval-sentinel-not-source-approved",
        },
        "review_bundle_sha256": lock_review_input["sha256"],
        "schema": CURRENT_CHILDREN_SCHEMA,
        "static_authority": static_authority,
        "status": "ok",
        "toolchain": toolchain,
        "toolchain_identities": current_toolchain_identities,
        "tools_manifest_path": str(reviewed_tools_path.resolve()),
        "tools_manifest_sha256": tools_manifest_sha256,
    }
    fixture_write_json(current_children_path, current_children)
    fixture_freeze_tree(reviewed_root)
    source_inputs = {
        "current_children_attestation": source_review_input(current_children_path),
        "tools_manifest": source_review_input(reviewed_tools_path),
        "lock_manifest": lock_manifest_input,
        "lock_authority": lock_authority_input,
        "lock_review_bundle": lock_review_input,
    }
    requirement = {
        "binary_byte_identical": True,
        "cfg_test": False,
        "forbidden_hook_strings": list(
            schema.RELEASE_COMPILE_OUT_FORBIDDEN_HOOK_STRINGS
        ),
        "forbidden_hook_strings_absent": True,
        "ordinary_a_role": "published",
        "overlay_a_role": "proof_only",
        "preapproval_compile_out_sha256": hashlib.sha256(
            canonical_json_bytes(preapproval)
        ).hexdigest(),
        "product_overlay_sha256": product_overlay_sha256,
        "proof_must_bind_enclosing_approval_sha256": True,
        "repeat_under_real_source_approval": True,
        "rustc_workspace_wrapper": "absent",
        "same_contract_nonce_lock_toolchain_sandbox": True,
        "schema": schema.RELEASE_COMPILE_OUT_REQUIREMENT_SCHEMA,
        "status": "required",
        "symbol_inventory_byte_identical": True,
        "variant": "A",
    }
    assertion = {
        "inputs": source_inputs,
        "open_findings": 0,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "release_compile_out_requirement": requirement,
        "schema": schema.SOURCE_REVIEW_ASSERTION_SCHEMA,
        "status": "approved",
        "tooling_commit": fake_commit,
        "tooling_tree": fake_tree,
    }
    assertion_sha256 = hashlib.sha256(
        schema.prepared_authority_canonical_json_bytes(assertion)
    ).hexdigest()
    review_id = "cr-synthetic"
    review_time = "2026-07-15T00:00:00+00:00"
    bundle = {
        "assertion": assertion,
        "assertion_sha256": assertion_sha256,
        "review_created": {
            "author": "synthetic-reviewer",
            "data": {
                "description": "Synthetic exact source authority — reviewed",
                "initial_commit": fake_commit,
                "jj_change_id": f"detached:{fake_commit}",
                "review_id": review_id,
                "scm_anchor": f"detached:{fake_commit}",
                "scm_kind": "git",
                "title": "Synthetic source authority résumé",
            },
            "event": "ReviewCreated",
            "ts": review_time,
        },
        "schema": schema.SOURCE_REVIEW_BUNDLE_SCHEMA,
        "verdict": {
            "author": "synthetic-reviewer",
            "data": {
                "reason": (
                    f"APPROVED assertion_sha256={assertion_sha256}; open_findings=0"
                ),
                "review_id": review_id,
                "vote": "lgtm",
            },
            "event": "ReviewerVoted",
            "ts": review_time,
        },
    }
    reviewed_bundle_path = root / "source-review-bundle.json"
    fixture_write_file(
        reviewed_bundle_path,
        schema.prepared_authority_canonical_json_bytes(bundle),
    )
    source_review = {
        "assertion_sha256": assertion_sha256,
        "bundle": {
            "mode": 0o444,
            "schema": schema.SOURCE_REVIEW_BUNDLE_SCHEMA,
            "sha256": sha256_file(reviewed_bundle_path),
        },
        "current_children_attestation": {
            "mode": 0o444,
            "schema": CURRENT_CHILDREN_SCHEMA,
            "sha256": source_inputs["current_children_attestation"]["sha256"],
        },
        "lock_authority": {
            "mode": 0o444,
            "schema": schema.SOURCE_REVIEW_CONTENT_SCHEMAS["lock_authority"],
            "sha256": source_inputs["lock_authority"]["sha256"],
        },
        "lock_review_bundle": {
            "mode": 0o444,
            "schema": schema.SOURCE_REVIEW_CONTENT_SCHEMAS["lock_review_bundle"],
            "sha256": source_inputs["lock_review_bundle"]["sha256"],
        },
        "release_compile_out_requirement": requirement,
    }

    approval = {
        "schema": schema.SOURCE_APPROVAL_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": protocol_sha256,
        "status": "approved",
        "review_id": review_id,
        "reviewed_at": review_time,
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
        "source_review": source_review,
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
        "rehearsal": False,
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

    def fixture_execution_file_binding(path: Path) -> dict[str, Any]:
        snapshot = schema.snapshot_regular_file(path.resolve(), expected_mode=None)
        metadata = snapshot.stat()
        return {
            "identity": {
                "changed_ns": metadata.st_ctime_ns,
                "device": metadata.st_dev,
                "inode": metadata.st_ino,
                "link_count": metadata.st_nlink,
                "modified_ns": metadata.st_mtime_ns,
            },
            "mode": snapshot.mode,
            "path": str(path.resolve()),
            "sha256": snapshot.sha256,
            "size": snapshot.size,
        }

    def fixture_prepared_execution_tools() -> dict[str, Any]:
        toolchain_root = cargo_path.parent.parent
        root_metadata = toolchain_root.lstat()
        return {
            "bwrap": fixture_execution_file_binding(bwrap_path),
            "cargo": fixture_execution_file_binding(cargo_path),
            "dev_null": fixture_current_null_device(),
            "rustc": fixture_execution_file_binding(rustc_path),
            "rust_lld": fixture_execution_file_binding(rust_lld_path),
            "toolchain_root": {
                "device": root_metadata.st_dev,
                "inode": root_metadata.st_ino,
                "link_count": root_metadata.st_nlink,
                "mode": stat.S_IMODE(root_metadata.st_mode),
            },
        }

    def synthetic_sandbox_argv(
        ordinal: int, package: str, example: str
    ) -> list[str]:
        descriptors = [str(300 + ordinal * 20 + offset) for offset in range(15)]
        system_bindings = tuple(
            ("--ro-bind-fd", descriptors[offset], guest_path)
            for offset, (_host_path, guest_path) in enumerate(TRUSTED_SYSTEM_MOUNTS)
        )
        core_bindings = (
            ("--ro-bind-fd", descriptors[4], GUEST_SOURCE),
            ("--bind-fd", descriptors[5], GUEST_TARGET),
            ("--ro-bind-fd", descriptors[6], GUEST_TOOLCHAIN_ROOT),
            ("--ro-bind-fd", descriptors[7], GUEST_CARGO),
            ("--ro-bind-fd", descriptors[8], GUEST_RUSTC),
            (
                "--ro-bind-fd",
                descriptors[9],
                f"{GUEST_TOOLCHAIN_ROOT}/lib/rustlib/{rustc_host}"
                "/bin/gcc-ld/ld.lld",
            ),
        )
        config_bindings = tuple(
            ("--ro-bind-fd", descriptors[11 + offset], guest_path)
            for offset, guest_path in enumerate(GUEST_BOUND_CONFIG_PATHS)
        )
        return [
            str(bwrap_path),
            "--die-with-parent",
            "--new-session",
            "--unshare-net",
            "--dir",
            "/usr",
            *(argument for binding in system_bindings for argument in binding),
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
            f"/proc/self/fd/{descriptors[3]}",
            "/dev/null",
            "--dir",
            "/proc",
            "--tmpfs",
            "/tmp",
            "--tmpfs",
            GUEST_ROOT,
            "--dir",
            f"{GUEST_ROOT}/.cargo",
            "--tmpfs",
            f"{GUEST_ROOT}/.cargo",
            "--remount-ro",
            f"{GUEST_ROOT}/.cargo",
            "--dir",
            "/.cargo",
            "--tmpfs",
            "/.cargo",
            "--remount-ro",
            "/.cargo",
            *(argument for binding in core_bindings for argument in binding),
            "--overlay-src",
            f"/proc/self/fd/{descriptors[10]}",
            "--tmp-overlay",
            GUEST_CARGO_HOME,
            "--dir",
            f"{GUEST_SOURCE}/.cargo",
            "--tmpfs",
            f"{GUEST_SOURCE}/.cargo",
            *(argument for binding in config_bindings[:2] for argument in binding),
            "--remount-ro",
            f"{GUEST_SOURCE}/.cargo",
            *(argument for binding in config_bindings[2:] for argument in binding),
            "--remount-ro",
            GUEST_CARGO_HOME,
            "--chdir",
            GUEST_SOURCE,
            GUEST_CARGO,
            "build",
            "--locked",
            "--offline",
            "--release",
            "-p",
            package,
            "--example",
            example,
            "--target-dir",
            GUEST_TARGET,
        ]

    for index, variant in enumerate(schema.VARIANTS, start=1):
        item = source_data[variant]
        build_log = root / "logs" / f"{variant}-build.json"
        fixture_write_json(
            build_log,
            {
                "exit_status": 0,
                "stderr": "",
                "stderr_sha256": EMPTY_SHA256,
                "stdout": "",
                "stdout_sha256": EMPTY_SHA256,
            },
        )
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
        sandbox_argv = synthetic_sandbox_argv(
            index,
            "mess-log" if variant == "B" else "mess-store",
            (
                "asterism_rebaseline_bare"
                if variant == "B"
                else "asterism_rebaseline_public"
            ),
        )
        build_argv = ["cargo", "build", "--locked", "--offline"]
        contract_argv = [str(binaries[variant]), "--contract"]
        started = previous_end + 1
        completed = started + 1
        previous_end = completed

        def completed_child(
            argv: list[str],
            output_path: Path,
            start_ns: int,
            *,
            passed_file_descriptors: int | None = None,
        ) -> dict[str, Any]:
            child = {
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
            if passed_file_descriptors is not None:
                child["passed_file_descriptors"] = passed_file_descriptors
            return child

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
                "execution_tools": fixture_prepared_execution_tools(),
                "build_env": sandboxed_build_environment(
                    toolchain,
                    {
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
                ),
                "cargo_config_search": fixture_sandboxed_cargo_config_search(
                    item["source_root"], f"build-{variant}"
                ),
                "semantic_input_authority": prepared_semantic_authorities[
                    variant
                ],
                "build_started_at": "2026-07-15T00:00:00+00:00",
                "build_started_monotonic_ns": started,
                "build_completed_at": "2026-07-15T00:00:01+00:00",
                "build_completed_monotonic_ns": completed,
                "build_log_path": str(build_log),
                "build_log_sha256": sha256_file(build_log),
                "build_child": completed_child(
                    sandbox_argv,
                    build_log,
                    started,
                    passed_file_descriptors=16,
                ),
                "contract_output_path": str(contract_path),
                "contract_output_sha256": sha256_file(contract_path),
                "contract_child": completed_child(contract_argv, contract_path, completed),
            },
        }
    claims_directory = prepared_bundle_root / "claims"
    bindings_directory = prepared_bindings
    prepared_tools_manifest_path = bindings_directory / "tools-manifest.json"
    fixture_write_json(prepared_tools_manifest_path, tools_manifest)
    prepared_source_review: dict[str, Any] = {}
    source_review_copies = {
        "bundle": (
            reviewed_bundle_path,
            bindings_directory / "source-review-bundle.json",
        ),
        "current_children_attestation": (
            current_children_path,
            bindings_directory / "current-children-attestation.json",
        ),
        "lock_authority": (
            lock_authority_path,
            bindings_directory / "lock-review-authority.json",
        ),
        "lock_review_bundle": (
            lock_review_path,
            bindings_directory / "lock-review-bundle.json",
        ),
    }
    for name, (source, destination) in source_review_copies.items():
        fixture_write_file(destination, source.read_bytes(), 0o444)
        prepared_source_review[name] = {
            "mode": 0o444,
            "path": str(destination.resolve()),
            "sha256": sha256_file(destination),
        }
    bindings_directory.chmod(0o555)

    def release_file_record(path: Path) -> dict[str, Any]:
        metadata = path.stat()
        return {
            "identity": {
                "changed_ns": metadata.st_ctime_ns,
                "device": metadata.st_dev,
                "inode": metadata.st_ino,
                "link_count": metadata.st_nlink,
                "modified_ns": metadata.st_mtime_ns,
            },
            "mode": stat.S_IMODE(metadata.st_mode),
            "path": str(path.resolve()),
            "sha256": sha256_file(path),
            "size": metadata.st_size,
        }

    ordinary_binary = binaries["A"]
    overlay_binary = root / "artifacts" / "proof-only" / "ast-rb-a-overlay"
    fixture_write_file(overlay_binary, ordinary_binary.read_bytes(), 0o555)
    symbol_bytes = b"fixture_release_symbol T 0\n"
    ordinary_symbols = root / "manifests" / "symbols-ordinary-a.txt"
    overlay_symbols = root / "manifests" / "symbols-overlay-a.txt"
    fixture_write_file(ordinary_symbols, symbol_bytes, 0o444)
    fixture_write_file(overlay_symbols, symbol_bytes, 0o444)

    preapproval_nm_identity = preapproval["nm"]["identity"]
    nm_tool_record = {
        "identity": {
            "changed_ns": preapproval_nm_identity["ctime_ns"],
            "device": preapproval_nm_identity["device"],
            "inode": preapproval_nm_identity["inode"],
            "link_count": preapproval_nm_identity["link_count"],
            "modified_ns": preapproval_nm_identity["mtime_ns"],
        },
        "mode": preapproval_nm_identity["mode"],
        "path": preapproval_nm_identity["path"],
        "sha256": preapproval_nm_identity["sha256"],
        "size": preapproval_nm_identity["size"],
    }

    def synthetic_nm_child(name: str, ordinal: int) -> dict[str, Any]:
        output_path = root / "logs" / f"nm-{name}.json"
        stdout = symbol_bytes.decode()
        fixture_write_json(
            output_path,
            {
                "exit_status": 0,
                "stderr": "",
                "stderr_sha256": hashlib.sha256(b"").hexdigest(),
                "stdout": stdout,
                "stdout_sha256": hashlib.sha256(symbol_bytes).hexdigest(),
            },
        )
        pid = 40_000 + ordinal
        start_ticks = 80_000 + ordinal
        return {
            "argv": [
                str(nm_path.resolve()),
                "--defined-only",
                "--demangle=rust",
                "--format=posix",
                f"/proc/self/fd/{600 + ordinal}",
            ],
            "completed_at": "2026-07-15T00:00:01+00:00",
            "completed_monotonic_ns": 20_000 + ordinal,
            "cwd": str(root.resolve()),
            "exit_status": 0,
            "output_path": str(output_path.resolve()),
            "output_sha256": sha256_file(output_path),
            "pid": pid,
            "process_group_absent": True,
            "reaping": {
                "pid": pid,
                "start_ticks": start_ticks,
                "status": "absent",
            },
            "start_ticks": start_ticks,
            "started_at": "2026-07-15T00:00:00+00:00",
            "started_monotonic_ns": 10_000 + ordinal,
            "timed_out": False,
            "waited_pid": pid,
        }

    ordinary_attestation = copy.deepcopy(prepared_variants["A"]["attestation"])
    overlay_attestation = copy.deepcopy(ordinary_attestation)
    ordinary_config_path = Path(
        ordinary_attestation["cargo_config_search"]["path"]
    )
    overlay_config_path = (
        root / "manifests" / "cargo-config-build-A-product-overlay.json"
    )
    fixture_write_file(
        overlay_config_path.with_name(f"{overlay_config_path.name}.empty"),
        b"",
        0o444,
    )
    fixture_write_file(
        overlay_config_path, ordinary_config_path.read_bytes(), 0o444
    )
    overlay_attestation["cargo_config_search"] = {
        "path": str(overlay_config_path.resolve()),
        "sha256": sha256_file(overlay_config_path),
    }
    overlay_build_log = root / "logs" / "A-product-overlay-build.json"
    fixture_write_json(
        overlay_build_log,
        {
            "exit_status": 0,
            "stderr": "",
            "stderr_sha256": EMPTY_SHA256,
            "stdout": "",
            "stdout_sha256": EMPTY_SHA256,
        },
    )
    overlay_attestation["build_log_path"] = str(overlay_build_log.resolve())
    overlay_attestation["build_log_sha256"] = sha256_file(overlay_build_log)
    overlay_attestation["build_child"]["output_path"] = str(
        overlay_build_log.resolve()
    )
    overlay_attestation["build_child"]["output_sha256"] = sha256_file(
        overlay_build_log
    )
    overlay_materialized_root = root / "materialized" / "A-product-overlay"
    fixture_write_file(
        overlay_materialized_root / "src" / "lib.rs",
        b"fixture proof overlay A\n",
    )
    overlay_materialized_root.chmod(0o555)
    overlay_attestation["materialized_root"] = str(
        overlay_materialized_root.resolve()
    )
    overlay_child = overlay_attestation["build_child"]
    overlay_attestation["semantic_input_authority"] = (
        fixture_semantic_authority(
            overlay_materialized_root, "proof-overlay-A"
        )
    )
    overlay_child["cwd"] = str(overlay_materialized_root.resolve())
    overlay_child["pid"] += 10_000
    overlay_child["waited_pid"] = overlay_child["pid"]
    overlay_child["start_ticks"] += 10_000
    overlay_child["reaping"] = {
        "pid": overlay_child["pid"],
        "start_ticks": overlay_child["start_ticks"],
        "status": "absent",
    }
    overlay_child["started_at"] = "2026-07-15T00:00:02+00:00"
    overlay_child["completed_at"] = "2026-07-15T00:00:03+00:00"
    overlay_child["started_monotonic_ns"] = (
        ordinary_attestation["build_child"]["completed_monotonic_ns"] + 1
    )
    overlay_child["completed_monotonic_ns"] = (
        overlay_child["started_monotonic_ns"] + 1
    )
    for field in (
        "started_at",
        "started_monotonic_ns",
        "completed_at",
        "completed_monotonic_ns",
    ):
        overlay_attestation[f"build_{field}"] = overlay_child[field]
    overlay_attestation["product_overlay_sha256"] = product_overlay_sha256
    normalized_sandbox = list(ordinary_attestation["build_argv"])
    for normalized_index, argument in enumerate(
        ordinary_attestation["build_argv"]
    ):
        if argument in {"--ro-bind-fd", "--bind-fd"}:
            normalized_sandbox[normalized_index + 1] = (
                f"$FD:{ordinary_attestation['build_argv'][normalized_index + 2]}"
            )
        elif argument == "--dev-bind":
            normalized_sandbox[normalized_index + 1] = "$FD:/dev/null"
        elif argument == "--overlay-src":
            normalized_sandbox[normalized_index + 1] = "$FD:cargo-home-overlay"
    equivalence_contract = {
        "build_environment_sha256": hashlib.sha256(
            canonical_json_bytes(ordinary_attestation["build_env"])
        ).hexdigest(),
        "build_nonce": ordinary_attestation["build_nonce"],
        "cargo_lock_sha256": ordinary_attestation["cargo_lock_sha256"],
        "cfg_test": False,
        "contract_sha256": hashlib.sha256(
            canonical_json_bytes(prepared_variants["A"]["contract"])
        ).hexdigest(),
        "ordinary_a_role": "published",
        "overlay_a_role": "proof_only",
        "rustc_workspace_wrapper": "absent",
        "sandbox_sha256": hashlib.sha256(
            canonical_json_bytes(
                {
                    "argv": normalized_sandbox,
                    "cargo_config_search_sha256": ordinary_attestation[
                        "cargo_config_search"
                    ]["sha256"],
                    "execution_tools_sha256": hashlib.sha256(
                        schema.prepared_authority_canonical_json_bytes(
                            ordinary_attestation["execution_tools"]
                        )
                    ).hexdigest(),
                    "semantic_runtime_sha256": ordinary_attestation[
                        "semantic_input_authority"
                    ]["runtime_sha256"],
                }
            )
        ).hexdigest(),
        "source_approval_sha256": approval_sha256,
        "toolchain_sha256": hashlib.sha256(
            canonical_json_bytes(ordinary_attestation["toolchain"])
        ).hexdigest(),
    }

    def release_build(
        name: str, role: str, attestation: Mapping[str, Any]
    ) -> dict[str, Any]:
        return {
            "artifact_role": role,
            "attestation": attestation,
            "attestation_sha256": hashlib.sha256(
                canonical_json_bytes(attestation)
            ).hexdigest(),
            "build_environment_sha256": equivalence_contract[
                "build_environment_sha256"
            ],
            "build_nonce": equivalence_contract["build_nonce"],
            "cargo_lock_sha256": equivalence_contract["cargo_lock_sha256"],
            "cfg_test": False,
            "contract_sha256": equivalence_contract["contract_sha256"],
            "role": name,
            "rustc_workspace_wrapper": "absent",
            "sandbox_sha256": equivalence_contract["sandbox_sha256"],
            "source_approval_sha256": approval_sha256,
            "toolchain_sha256": equivalence_contract["toolchain_sha256"],
        }

    release_compile_out = {
        "binaries": {
            "ordinary_a": release_file_record(ordinary_binary),
            "overlay_a": release_file_record(overlay_binary),
        },
        "binary_byte_identical": True,
        "builds": {
            "ordinary_a": release_build(
                "ordinary_a", "published", ordinary_attestation
            ),
            "overlay_a": release_build(
                "overlay_a", "proof_only", overlay_attestation
            ),
        },
        "current_children_attestation_sha256": source_inputs[
            "current_children_attestation"
        ]["sha256"],
        "equivalence_contract": equivalence_contract,
        "forbidden_hook_strings": list(
            schema.RELEASE_COMPILE_OUT_FORBIDDEN_HOOK_STRINGS
        ),
        "forbidden_hook_strings_absent": True,
        "nm": {
            "ordinary_a": synthetic_nm_child("ordinary-a", 1),
            "overlay_a": synthetic_nm_child("overlay-a", 2),
            "tool": nm_tool_record,
        },
        "product_overlay_sha256": product_overlay_sha256,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "published_a_sha256": sha256_file(ordinary_binary),
        "requirement_sha256": hashlib.sha256(
            canonical_json_bytes(requirement)
        ).hexdigest(),
        "schema": schema.RELEASE_COMPILE_OUT_SCHEMA,
        "source_approval_sha256": approval_sha256,
        "status": "ok",
        "symbol_inventories": {
            "ordinary_a": release_file_record(ordinary_symbols),
            "overlay_a": release_file_record(overlay_symbols),
        },
        "symbol_inventory_byte_identical": True,
    }
    release_compile_out_path = (
        prepared_bundle_root / schema.RELEASE_COMPILE_OUT_RELATIVE_PATH
    )
    fixture_write_json(release_compile_out_path, release_compile_out, 0o444)
    release_compile_out_binding = {
        "mode": 0o444,
        "path": str(release_compile_out_path.resolve()),
        "sha256": sha256_file(release_compile_out_path),
    }
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
        "source_review": prepared_source_review,
        "release_compile_out": release_compile_out_binding,
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
            if (
                track == "structural_traces"
                and identity.get("trace_kind") == "new_names"
            ):
                # Mirror the runner's derived structural new_names context so the
                # self-test fixture records the same keys the evaluator expects.
                context_value["payload_size"] = 250
                context_value["batch_size"] = 1
                context_value["batches_per_writer"] = identity["appends_per_writer"]
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
        "rehearsal": False,
        "declaration_sha256": "d" * 64,
        "attempt_nonce": nonce,
        "output_dir": str(output),
        "output_dir_absent_before": False,
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

    def semantic_sampler_prefix_sibling_order() -> bool:
        import importlib.util

        module_name = "_asterism_evaluator_prepare_overlays_self_test"
        module_path = (
            Path(schema.__file__).resolve().parent / "tooling" / "prepare_overlays.py"
        )
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None or spec.loader is None:
            return False
        tooling = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = tooling
        try:
            spec.loader.exec_module(tooling)
            with tempfile.TemporaryDirectory(
                prefix="bn-1h32-evaluator-semantic-order-"
            ) as temporary:
                root = Path(temporary).resolve(strict=True)
                (root / "a").mkdir()
                (root / "a-b").mkdir()
                (root / "a" / "b").write_bytes(b"a/b\n")
                (root / "a-b" / "y").write_bytes(b"a-b/y\n")
                produced = tooling.resample_recursive_manifest(
                    root,
                    "source",
                    "evaluator producer prefix-sibling fixture",
                    allow_internal_symlinks=False,
                    hash_regular_contents=True,
                )
                sampled = sample_semantic_tree(
                    root,
                    "source",
                    "evaluator prefix-sibling fixture",
                    allow_internal_symlinks=False,
                    hash_regular_contents=True,
                )
                validated = SemanticReplay(
                    Problems(), live_system=False
                )._validate_tree(
                    sampled,
                    "source",
                    "evaluator prefix-sibling fixture",
                    trusted_system=False,
                )
                return (
                    sampled == produced
                    and validated == sampled
                    and [entry["path"] for entry in sampled["entries"]]
                    == [".", "a", "a-b", "a-b/y", "a/b"]
                )
        finally:
            sys.modules.pop(module_name, None)

    check(
        "semantic-sampler-prefix-sibling-order-matches-producer",
        semantic_sampler_prefix_sibling_order,
    )

    nm_identity = {
        "bytes": 17,
        "ctime_ns": 11,
        "device": 12,
        "inode": 13,
        "link_count": 1,
        "mode": 0o555,
        "mtime_ns": 14,
        "path": "/usr/bin/nm",
        "sha256": "a" * 64,
        "size": 17,
    }

    def nm_chain_entry(path: str, ordinal: int) -> dict[str, Any]:
        return {
            "changed_ns": ordinal,
            "device": 12,
            "gid": 0,
            "inode": ordinal,
            "link_count": 1,
            "mode": 0o555,
            "modified_ns": ordinal,
            "path": path,
            "size": ordinal,
            "type": stat.S_IFDIR if path != nm_identity["path"] else stat.S_IFREG,
            "uid": 0,
        }

    nm_authority = {
        "identity": nm_identity,
        "path_chain": [
            nm_chain_entry("/", 1),
            nm_chain_entry(nm_identity["path"], 2),
        ],
        "trusted_system": True,
    }

    def changed_nm(callback: Callable[[dict[str, Any]], Any]) -> dict[str, Any]:
        hostile = copy.deepcopy(nm_authority)
        callback(hostile)
        return hostile

    nm_hostiles = {
        "flat-legacy": dict(nm_identity),
        "missing-wrapper-key": changed_nm(lambda value: value.pop("path_chain")),
        "extra-wrapper-key": changed_nm(
            lambda value: value.__setitem__("extra", True)
        ),
        "missing-identity": changed_nm(lambda value: value.pop("identity")),
        "wrong-identity-keys": changed_nm(
            lambda value: value["identity"].pop("bytes")
        ),
        "trusted-system-false": changed_nm(
            lambda value: value.__setitem__("trusted_system", False)
        ),
        "trusted-system-absent": changed_nm(
            lambda value: value.pop("trusted_system")
        ),
        "path-null": changed_nm(
            lambda value: value["identity"].__setitem__("path", None)
        ),
        "path-non-string": changed_nm(
            lambda value: value["identity"].__setitem__("path", 7)
        ),
        "path-relative": changed_nm(
            lambda value: value["identity"].__setitem__("path", "usr/bin/nm")
        ),
        "bool-integer": changed_nm(
            lambda value: value["identity"].__setitem__("mode", True)
        ),
        "path-chain-last-mismatch": changed_nm(
            lambda value: value["path_chain"][-1].__setitem__(
                "path", "/usr/bin/other"
            )
        ),
    }

    def accepts_nm_authority() -> bool:
        problems = Problems()
        return (
            preapproval_nm_identity(nm_authority, problems) == nm_identity
            and not problems.errors
        )

    check("preapproval-nm-wrapper-positive", accepts_nm_authority)
    for name, hostile in nm_hostiles.items():
        def rejects_nm_authority(hostile: Any = hostile) -> bool:
            problems = Problems()
            return (
                preapproval_nm_identity(hostile, problems) is None
                and problems.errors
                == ["release compile-out preapproval nm authority is absent"]
            )

        check(f"preapproval-nm-wrapper-rejects-{name}", rejects_nm_authority)

    for value, label in ((float("nan"), "nan"), (float("inf"), "infinity")):
        def rejects_nonfinite(value: float = value) -> bool:
            try:
                canonical_json_bytes({"value": value})
            except ValueError:
                return True
            return False

        check(f"canonical-json-rejects-{label}", rejects_nonfinite)

    def prepared_authority_utf8_boundary() -> bool:
        value = {
            "label": "A — evaluator",
            "schema": "bn-ecm1-evaluator-prepared-authority-utf8-self-test-v1",
        }
        with tempfile.TemporaryDirectory(
            prefix="bn-ecm1-evaluator-authority-"
        ) as temporary:
            root = Path(temporary)
            accepted_path = root / "authority-utf8.json"
            accepted_path.write_bytes(
                schema.prepared_authority_canonical_json_bytes(value)
            )
            accepted_path.chmod(0o444)
            accepted_problems = Problems()
            if (
                read_prepared_authority_object(
                    accepted_path,
                    "evaluator prepared authority UTF-8 self-test",
                    accepted_problems,
                )
                != value
                or accepted_problems.errors
            ):
                return False
            escaped_path = root / "authority-ascii-escaped.json"
            escaped_path.write_bytes(canonical_json_bytes(value))
            escaped_path.chmod(0o444)
            escaped_problems = Problems()
            return (
                read_prepared_authority_object(
                    escaped_path,
                    "evaluator ASCII-escaped prepared authority hostile",
                    escaped_problems,
                )
                is None
                and bool(escaped_problems.errors)
            )

    check(
        "prepared-authority-utf8-accepted-ascii-escaped-rejected",
        prepared_authority_utf8_boundary,
    )

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

        approval_fixture = json.loads(
            (output / "source-approval.json").read_bytes()
        )
        source_review_fixture = approval_fixture["source_review"]
        current_children_fixture = json.loads(
            Path(
                prepared_fixture["source_review"][
                    "current_children_attestation"
                ]["path"]
            ).read_bytes()
        )
        proof_fixture = json.loads(
            Path(prepared_fixture["release_compile_out"]["path"]).read_bytes()
        )
        lock_authority_fixture = json.loads(
            Path(prepared_fixture["source_review"]["lock_authority"]["path"])
            .read_bytes()
        )
        source_review_bundle_fixture = json.loads(
            Path(prepared_fixture["source_review"]["bundle"]["path"])
            .read_bytes()
        )

        def semantic_chain_errors(
            mutator: Callable[
                [dict[str, Any], dict[str, Any], dict[str, Any]], None
            ],
            *,
            include_overlay: bool,
        ) -> list[str]:
            current = copy.deepcopy(current_children_fixture)
            lock_authority = copy.deepcopy(lock_authority_fixture)
            prepared = copy.deepcopy(prepared_fixture)
            mutator(current, lock_authority, prepared)
            validation = Problems()
            replay = SemanticReplay(validation, live_system=False)
            replay_current_and_resolution_semantics(
                current,
                source_review_bundle_fixture["assertion"],
                lock_authority,
                replay,
            )
            replay_prepared_release_semantics(prepared, replay)
            if include_overlay:
                validate_release_compile_out_proof(
                    proof_fixture,
                    approval_sha256=sha256_file(output / "source-approval.json"),
                    source_review=source_review_fixture,
                    current_children=current,
                    current_children_sha256=hashlib.sha256(
                        canonical_json_bytes(current)
                    ).hexdigest(),
                    prepared=prepared,
                    config=config_fixture,
                    semantic_replay=replay,
                    problems=validation,
                )
            replay.finalize()
            return validation.errors

        check(
            "mutation-semantic-current-child-extra-field",
            lambda: any(
                "semantic current-child v2 keys are not exact" in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current.__setitem__(
                        "forged", True
                    ),
                    include_overlay=True,
                )
            ),
        )
        check(
            "mutation-semantic-current-build-truncated",
            lambda: any(
                "semantic current build children record fields are not exact" in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["builds"][
                        "children"
                    ].pop("wrapper_input_identity"),
                    include_overlay=True,
                )
            ),
        )
        check(
            "mutation-semantic-current-build-extra-field",
            lambda: any(
                "semantic current build hooked_release record fields are not exact"
                in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["builds"][
                        "hooked_release"
                    ].__setitem__("forged", True),
                    include_overlay=True,
                )
            ),
        )

        def duplicate_current_build_descriptor(
            current: dict[str, Any],
            _lock: dict[str, Any],
            _prepared: dict[str, Any],
        ) -> None:
            build = current["builds"]["pristine_release"]
            argv = build["argv"]
            target_descriptor_index = argv.index("--bind-fd") + 1
            duplicate = argv[argv.index("--ro-bind-fd") + 1]
            argv[target_descriptor_index] = duplicate
            build["execution"]["argv"][target_descriptor_index] = duplicate
            for artifact in build["artifacts"].values():
                suffix = artifact["source"]["path"].split("/", 5)[-1]
                artifact["source"]["path"] = f"/proc/self/fd/{duplicate}/{suffix}"

        check(
            "mutation-semantic-current-build-duplicate-fd",
            lambda: any(
                any(expected in error for expected in (
                    "semantic current build pristine_release record execution log sidecar differs",
                    "semantic current build pristine_release record sandbox descriptor/command differs",
                ))
                for error in semantic_chain_errors(
                    duplicate_current_build_descriptor, include_overlay=True
                )
            ),
        )
        check(
            "mutation-semantic-current-build-passed-fd-cardinality",
            lambda: any(
                any(expected in error for expected in (
                    "semantic current build children record execution log sidecar differs",
                    "semantic current build children record passed descriptor cardinality differs",
                ))
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["builds"][
                        "children"
                    ]["execution"].__setitem__("passed_file_descriptors", 14),
                    include_overlay=True,
                )
            ),
        )
        for name, field, value, expected_error in (
            (
                "mutation-semantic-current-build-float-exit-status",
                "exit_status",
                0.0,
                "execution authority differs",
            ),
            (
                "mutation-semantic-current-build-bool-exit-status",
                "exit_status",
                False,
                "execution authority differs",
            ),
            (
                "mutation-semantic-current-build-float-passed-fd-cardinality",
                "passed_file_descriptors",
                float(
                    current_children_fixture["builds"]["children"]["execution"][
                        "passed_file_descriptors"
                    ]
                ),
                "execution authority differs",
            ),
            (
                "mutation-semantic-current-build-bool-passed-fd-cardinality",
                "passed_file_descriptors",
                True,
                "execution authority differs",
            ),
        ):
            check(
                name,
                lambda field=field, value=value, expected_error=expected_error: any(
                    expected_error in error
                    for error in semantic_chain_errors(
                        lambda current, _lock, _prepared: current["builds"][
                            "children"
                        ]["execution"].__setitem__(field, value),
                        include_overlay=True,
                    )
                ),
            )
        check(
            "mutation-semantic-current-build-null-device-float-minor",
            lambda: any(
                "semantic current build children record null device null-device identity differs"
                in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["builds"][
                        "children"
                    ]["execution_tools"]["dev_null"]["identity"].__setitem__(
                        "minor", 3.0
                    ),
                    include_overlay=True,
                )
            ),
        )
        check(
            "mutation-semantic-current-build-rust-lld-binding",
            lambda: any(
                "semantic current build children record rust-lld identity live identity differs"
                in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["builds"][
                        "children"
                    ]["execution_tools"]["rust_lld"]["identity"].__setitem__(
                        "path", current["toolchain"]["rustc_path"]
                    ),
                    include_overlay=True,
                )
            ),
        )
        def mutate_current_null_destination(
            current: dict[str, Any],
            _lock: dict[str, Any],
            _prepared: dict[str, Any],
        ) -> None:
            build = current["builds"]["children"]
            for argv in (build["argv"], build["execution"]["argv"]):
                argv[argv.index("--dev-bind") + 2] = "/dev/zero"

        def current_null_destination_rejected() -> bool:
            current_root = Path(
                current_children_fixture["construction_path"]
            ).parents[1]
            log_path = current_root / "logs" / "cargo-build-children.json"
            original = log_path.read_bytes()
            mode = stat.S_IMODE(log_path.stat().st_mode)

            def mutate_with_sidecar(
                current: dict[str, Any],
                lock: dict[str, Any],
                prepared: dict[str, Any],
            ) -> None:
                mutate_current_null_destination(current, lock, prepared)
                log_path.chmod(0o644)
                log_path.write_bytes(
                    canonical_json_bytes(current["builds"]["children"]["execution"])
                )
                log_path.chmod(mode)

            try:
                errors = semantic_chain_errors(
                    mutate_with_sidecar,
                    include_overlay=True,
                )
                return any(
                    "semantic current build children record null-device binding differs"
                    in error
                    for error in errors
                )
            finally:
                log_path.chmod(0o644)
                log_path.write_bytes(original)
                log_path.chmod(mode)

        check(
            "mutation-semantic-current-build-null-device-destination",
            current_null_destination_rejected,
        )
        check(
            "mutation-semantic-current-build-config-transition",
            lambda: any(
                "semantic current build hooked_release record Cargo config changed across build"
                in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["builds"][
                        "hooked_release"
                    ]["cargo_config_postbuild"].__setitem__("schema", "forged"),
                    include_overlay=True,
                )
            ),
        )

        def current_config_reserved_type_rejected(kind: str) -> bool:
            build = current_children_fixture["builds"]["children"]
            record = copy.deepcopy(build["cargo_config_prebuild"])
            record["preserved_top_level_entries"] = {
                "cargo-home": [],
                "source": [],
            }
            record["cargo_search"]["entries"][6]["sha256"] = EMPTY_SHA256
            with tempfile.TemporaryDirectory(
                prefix="bn-2l3n-config-type-", dir=Path(temp).parent
            ) as scratch_text:
                scratch = Path(scratch_text)
                source_root = scratch / "source"
                cargo_home = scratch / "cargo-home"
                (source_root / ".cargo").mkdir(parents=True)
                cargo_home.mkdir()
                (cargo_home / "config.toml").write_bytes(b"")
                hostile = cargo_home / "config"
                if kind == "directory":
                    hostile.mkdir()
                elif kind == "hardlink":
                    target = scratch / "hardlink-target"
                    target.write_bytes(b"")
                    os.link(target, hostile)
                else:
                    hostile.symlink_to("missing-config-target")
                try:
                    _current_config_record(
                        record,
                        build["semantic_input_authority"],
                        source_root,
                        cargo_home,
                        f"hostile Cargo config {kind}",
                    )
                except ValueError as error:
                    return (
                        "live Cargo config entry is not exact regular"
                        in str(error)
                    )
            return False

        def omitted_preserved_source_entry_rejected() -> bool:
            build = current_children_fixture["builds"]["children"]
            record = copy.deepcopy(build["cargo_config_prebuild"])
            record["preserved_top_level_entries"]["source"] = []
            current_root = Path(current_children_fixture["construction_path"]).parents[1]
            try:
                _current_config_record(
                    record,
                    build["semantic_input_authority"],
                    current_root / "materialized" / "children",
                    Path(current_children_fixture["toolchain"]["cargo_home_path"]),
                    "hostile omitted preserved source",
                )
            except ValueError as error:
                return "preserved entry order differs" in str(error)
            return False

        check(
            "mutation-semantic-current-preserved-source-omission",
            omitted_preserved_source_entry_rejected,
        )

        def omitted_preserved_cargo_home_entry_rejected() -> bool:
            build = current_children_fixture["builds"]["children"]
            record = copy.deepcopy(build["cargo_config_prebuild"])
            record["preserved_top_level_entries"]["cargo-home"] = []
            current_root = Path(current_children_fixture["construction_path"]).parents[1]
            try:
                _current_config_record(
                    record,
                    build["semantic_input_authority"],
                    current_root / "materialized" / "children",
                    Path(current_children_fixture["toolchain"]["cargo_home_path"]),
                    "hostile omitted preserved Cargo-home",
                )
            except ValueError as error:
                return "preserved entry order differs" in str(error)
            return False

        check(
            "mutation-semantic-current-preserved-cargo-home-omission",
            omitted_preserved_cargo_home_entry_rejected,
        )
        check(
            "mutation-semantic-current-build-lock-transition",
            lambda: any(
                "semantic current build pristine_release record lock changed across build"
                in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["builds"][
                        "pristine_release"
                    ]["lock_postbuild"].__setitem__("sha256", "0" * 64),
                    include_overlay=True,
                )
            ),
        )

        def widen_current_lock_identity(
            current: dict[str, Any],
            _lock: dict[str, Any],
            _prepared: dict[str, Any],
        ) -> None:
            build = current["builds"]["pristine_release"]
            for boundary in ("lock_prebuild", "lock_postbuild"):
                build[boundary]["identity"]["sha256"] = build[boundary][
                    "sha256"
                ]

        check(
            "mutation-semantic-current-build-lock-identity-shape",
            lambda: any(
                "semantic current build pristine_release record lock identity fields are not exact"
                in error
                for error in semantic_chain_errors(
                    widen_current_lock_identity, include_overlay=True
                )
            ),
        )
        check(
            "mutation-semantic-current-build-fault-proof-binding",
            lambda: any(
                "semantic current build children record wrapper environment differs"
                in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["builds"][
                        "children"
                    ]["environment"].__setitem__(
                        "ASTERISM_FAULT_COMPILE_OUT_PRISTINE_SHA256", "0" * 64
                    ),
                    include_overlay=True,
                )
            ),
        )
        check(
            "mutation-semantic-current-build-receipt-path",
            lambda: any(
                "semantic current build children record wrapper receipt identity"
                in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["builds"][
                        "children"
                    ]["wrapper_receipt_identity"].__setitem__(
                        "path",
                        current["builds"]["children"]["wrapper_input_identity"][
                            "path"
                        ],
                    ),
                    include_overlay=True,
                )
            ),
        )

        def redirect_current_filesystem_admission(
            current: dict[str, Any],
            _lock: dict[str, Any],
            _prepared: dict[str, Any],
        ) -> None:
            build = current["builds"]["hooked_release"]
            build["filesystem_admission"]["checked_path"] = build["target"]
            current["prebuild_filesystem_admissions"]["hooked_release"] = copy.deepcopy(
                build["filesystem_admission"]
            )

        check(
            "mutation-semantic-current-build-filesystem-path",
            lambda: any(
                "semantic current build hooked_release record filesystem admission differs"
                in error
                for error in semantic_chain_errors(
                    redirect_current_filesystem_admission, include_overlay=True
                )
            ),
        )
        check(
            "mutation-semantic-current-build-wrapper-policy",
            lambda: any(
                "semantic current build hooked_release record environment fields differ"
                in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["builds"][
                        "hooked_release"
                    ]["environment"].__setitem__(
                        "RUSTC_WORKSPACE_WRAPPER",
                        f"{GUEST_ROOT}/rustc_workspace_wrapper.py",
                    ),
                    include_overlay=True,
                )
            ),
        )
        check(
            "mutation-semantic-toolchain-extra-field",
            lambda: any(
                "semantic current toolchain fields are not exact" in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current[
                        "toolchain"
                    ].__setitem__("forged", True),
                    include_overlay=True,
                )
            ),
        )

        def alternate_toolchain_root_rejected() -> bool:
            hostile = copy.deepcopy(current_children_fixture["toolchain"])
            hostile["rustc_path"] = hostile["bwrap_path"]
            hostile["rustc_sha256"] = hostile["bwrap_sha256"]
            try:
                validate_semantic_toolchain(
                    hostile, "hostile semantic toolchain", live=True
                )
            except ValueError as error:
                return any(
                    expected in str(error)
                    for expected in (
                        "executable paths physically alias",
                        "Cargo/rustc rustup paths differ",
                    )
                )
            return False

        check(
            "mutation-semantic-toolchain-split-root",
            alternate_toolchain_root_rejected,
        )

        def alternate_rust_lld_rejected() -> bool:
            hostile = copy.deepcopy(current_children_fixture["toolchain"])
            hostile["rust_lld_path"] = hostile["rustc_path"]
            hostile["rust_lld_sha256"] = hostile["rustc_sha256"]
            try:
                validate_semantic_toolchain(
                    hostile, "hostile semantic rust-lld", live=True
                )
            except ValueError as error:
                return any(
                    expected in str(error)
                    for expected in (
                        "rust-lld topology differs",
                        "executable paths physically alias",
                    )
                )
            return False

        check(
            "mutation-semantic-toolchain-rust-lld-topology",
            alternate_rust_lld_rejected,
        )

        def path_like_rustc_host_rejected() -> bool:
            hostile = copy.deepcopy(current_children_fixture["toolchain"])
            hostile["rustc_host"] = "../escape"
            try:
                validate_semantic_toolchain(
                    hostile, "hostile path-like rustc host", live=True
                )
            except ValueError as error:
                return "rustc host differs" in str(error)
            return False

        check(
            "mutation-semantic-toolchain-path-like-rustc-host",
            path_like_rustc_host_rejected,
        )
        check(
            "mutation-semantic-toolchain-version-not-stripped",
            lambda: any(
                "semantic current toolchain cargo_version_verbose differs" in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["toolchain"].__setitem__(
                        "cargo_version_verbose",
                        current["toolchain"]["cargo_version_verbose"] + "\n",
                    ),
                    include_overlay=True,
                )
            ),
        )
        check(
            "mutation-semantic-toolchain-duplicate-rustc-host",
            lambda: any(
                "semantic current toolchain rustup/rustc sampled identity differs"
                in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["toolchain"].__setitem__(
                        "rustc_version_verbose",
                        current["toolchain"]["rustc_version_verbose"]
                        + "\nhost: "
                        + current["toolchain"]["rustc_host"],
                    ),
                    include_overlay=True,
                )
            ),
        )

        def opaque_cargo_verbose_accepted() -> bool:
            value = copy.deepcopy(current_children_fixture["toolchain"])
            value["cargo_version_verbose"] = "producer-opaque-output"
            try:
                validate_semantic_toolchain(
                    value, "opaque Cargo verbose semantics", live=True
                )
            except ValueError:
                return False
            return True

        check(
            "semantic-toolchain-cargo-verbose-remains-opaque",
            opaque_cargo_verbose_accepted,
        )
        check(
            "mutation-semantic-toolchain-path-like-rustup-token",
            lambda: any(
                "semantic current toolchain rustup/rustc sampled identity differs"
                in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["toolchain"].__setitem__(
                        "rustup_toolchain", "nested/1.97.0-x86_64-unknown-linux-gnu"
                    ),
                    include_overlay=True,
                )
            ),
        )
        check(
            "mutation-semantic-current-static-authority-status",
            lambda: any(
                "semantic current static authority normal result differs" in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["static_authority"][
                        "normal"
                    ].__setitem__("status", "forged"),
                    include_overlay=True,
                )
            ),
        )

        def rebind_fault_source_and_input(
            current: dict[str, Any],
            _lock: dict[str, Any],
            _prepared: dict[str, Any],
        ) -> None:
            replacement = copy.deepcopy(current["inputs"][0])
            current["fault_authority"]["source"] = replacement
            current["inputs"][1] = replacement

        check(
            "mutation-semantic-current-fault-source-input-coordinated-rebind",
            lambda: any(
                "semantic current fault source topology differs" in error
                or "semantic current fault authority source path differs" in error
                for error in semantic_chain_errors(
                    rebind_fault_source_and_input, include_overlay=True
                )
            ),
        )

        def rebind_static_validator_and_input(
            current: dict[str, Any],
            _lock: dict[str, Any],
            _prepared: dict[str, Any],
        ) -> None:
            replacement = copy.deepcopy(current["inputs"][2])
            current["static_authority"]["validator"] = replacement
            current["inputs"][9] = replacement

        check(
            "mutation-semantic-current-static-validator-input-coordinated-rebind",
            lambda: any(
                "semantic current static authority validator path differs" in error
                for error in semantic_chain_errors(
                    rebind_static_validator_and_input, include_overlay=True
                )
            ),
        )
        check(
            "mutation-semantic-current-input-order",
            lambda: any(
                "semantic current input ordering/disjointness differs" in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["inputs"].__setitem__(
                        slice(0, 2), list(reversed(current["inputs"][:2]))
                    ),
                    include_overlay=True,
                )
            ),
        )

        def embedded_lock_candidate_binding_rejected() -> bool:
            def mutate(
                current: dict[str, Any],
                _lock: dict[str, Any],
                _prepared: dict[str, Any],
            ) -> None:
                variants = current["lock_authority"]["lock_manifest"]["payload"][
                    "variants"
                ]
                variants["C"]["final_lock_path"] = variants["D"][
                    "final_lock_path"
                ]

            return any(
                "semantic current lock authority crosslink differs" in error
                for error in semantic_chain_errors(
                    mutate, include_overlay=True
                )
            )

        check(
            "mutation-semantic-current-lock-candidate-reviewed-binding",
            embedded_lock_candidate_binding_rejected,
        )

        def reviewed_cargo_binding_rejected() -> bool:
            def mutate(
                current: dict[str, Any],
                _lock: dict[str, Any],
                _prepared: dict[str, Any],
            ) -> None:
                variants = current["lock_authority"]["lock_manifest"]["payload"][
                    "variants"
                ]
                current["cargo_config_authority"]["binding"] = copy.deepcopy(
                    variants["D"]["resolver"]["cargo_config_search"]
                )

            return any(
                "semantic current Cargo config authority differs" in error
                for error in semantic_chain_errors(
                    mutate, include_overlay=True
                )
            )

        check(
            "mutation-semantic-current-cargo-reviewed-binding",
            reviewed_cargo_binding_rejected,
        )

        def final_tools_inheritance_rejected() -> bool:
            base = schema.parse_canonical_json_object(
                Path(current_children_fixture["inputs"][23]["path"]).read_bytes(),
                "hostile base tools",
            )
            final = schema.parse_canonical_json_object(
                Path(current_children_fixture["tools_manifest_path"]).read_bytes(),
                "hostile final tools",
            )
            final["tools"]["perf"] = copy.deepcopy(final["tools"]["strace"])
            try:
                _current_final_tools_inheritance(
                    base,
                    final,
                    current_children_fixture["artifacts"],
                    "hostile final tools inheritance",
                )
            except ValueError as error:
                return "exact base reconstruction differs" in str(error)
            return False

        check(
            "mutation-semantic-current-final-tools-inheritance",
            final_tools_inheritance_rejected,
        )
        check(
            "mutation-semantic-current-final-tools-path",
            lambda: any(
                "semantic current final tools binding differs" in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current.__setitem__(
                        "tools_manifest_path", current["construction_path"]
                    ),
                    include_overlay=True,
                )
            ),
        )
        check(
            "mutation-semantic-current-child-artifact-destination",
            lambda: any(
                "semantic current build children record artifact copy authority differs"
                in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["builds"][
                        "children"
                    ]["artifacts"][
                        "asterism_rebaseline_current_correctness"
                    ]["binding"].__setitem__(
                        "path",
                        current["builds"]["hooked_release"]["artifacts"][
                            "asterism_rebaseline_public"
                        ]["binding"]["path"],
                    ),
                    include_overlay=True,
                )
            ),
        )
        for hostile_link_count in (1, 3):
            check(
                f"mutation-semantic-current-child-artifact-source-links-{hostile_link_count}",
                lambda hostile_link_count=hostile_link_count: any(
                    "artifact source retained descriptor identity differs" in error
                    for error in semantic_chain_errors(
                        lambda current, _lock, _prepared: current["builds"][
                            "children"
                        ]["artifacts"][
                            "asterism_rebaseline_current_correctness"
                        ]["source"].__setitem__(
                            "link_count", hostile_link_count
                        ),
                        include_overlay=True,
                    )
                ),
            )
        for hostile_name, field, value in (
            ("mode", "mode", 0o755),
            (
                "ctime",
                "ctime_ns",
                current_children_fixture["builds"]["children"]["artifacts"][
                    "asterism_rebaseline_current_correctness"
                ]["source"]["ctime_ns"]
                + 1,
            ),
        ):
            check(
                f"mutation-semantic-current-child-artifact-source-{hostile_name}",
                lambda field=field, value=value: any(
                    "artifact source retained descriptor identity differs" in error
                    for error in semantic_chain_errors(
                        lambda current, _lock, _prepared: current["builds"][
                            "children"
                        ]["artifacts"][
                            "asterism_rebaseline_current_correctness"
                        ]["source"].__setitem__(field, value),
                        include_overlay=True,
                    )
                ),
            )

        def current_artifact_chmod_restore_rejected() -> bool:
            with tempfile.TemporaryDirectory(
                prefix="bn-1o85-evaluator-artifact-", dir=Path(temp).parent
            ) as scratch_text:
                scratch = Path(scratch_text)
                primary = scratch / "artifact"
                primary.write_bytes(b"synthetic evaluator artifact\n")
                primary.chmod(0o555)
                os.link(primary, scratch / "artifact-0123456789abcdef")
                metadata = primary.stat()
                record = {
                    "bytes": metadata.st_size,
                    "ctime_ns": metadata.st_ctime_ns,
                    "device": metadata.st_dev,
                    "inode": metadata.st_ino,
                    "link_count": metadata.st_nlink,
                    "mode": stat.S_IMODE(metadata.st_mode),
                    "mtime_ns": metadata.st_mtime_ns,
                    "path": "/proc/self/fd/900/release/examples/artifact",
                    "sha256": sha256_file(primary),
                    "size": metadata.st_size,
                }
                primary.chmod(0o755)
                primary.chmod(0o555)
                try:
                    _current_bound_file_identity(
                        record,
                        primary,
                        record["path"],
                        "hostile chmod-restored artifact source",
                        executable=True,
                        expected_link_count=2,
                    )
                except ValueError as error:
                    return "retained descriptor identity differs" in str(error)
            return False

        check(
            "mutation-semantic-current-child-artifact-live-chmod-restore",
            current_artifact_chmod_restore_rejected,
        )
        check(
            "mutation-semantic-current-wrapper-receipt-two-links",
            lambda: any(
                "wrapper receipt identity retained descriptor identity differs" in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["builds"][
                        "children"
                    ]["wrapper_receipt_identity"].__setitem__("link_count", 2),
                    include_overlay=True,
                )
            ),
        )

        def final_target_thaw_rejected() -> bool:
            target = Path(current_children_fixture["builds"]["children"]["target"])
            try:
                target.chmod(0o755)
                return any(
                    "semantic current build children record target final frozen directory differs"
                    in error
                    for error in semantic_chain_errors(
                        lambda _current, _lock, _prepared: None,
                        include_overlay=True,
                    )
                )
            finally:
                target.chmod(0o555)

        check(
            "mutation-semantic-current-final-target-not-frozen",
            final_target_thaw_rejected,
        )

        def final_evidence_directory_thaw_rejected() -> bool:
            current_root = Path(current_children_fixture["construction_path"]).parents[1]
            directory = current_root / "manifests"
            try:
                directory.chmod(0o755)
                return any(
                    "semantic current final frozen tree directory is not frozen"
                    in error
                    for error in semantic_chain_errors(
                        lambda _current, _lock, _prepared: None,
                        include_overlay=True,
                    )
                )
            finally:
                directory.chmod(0o555)

        check(
            "mutation-semantic-current-final-evidence-directory-not-frozen",
            final_evidence_directory_thaw_rejected,
        )
        check(
            "mutation-semantic-current-historical-target-selection",
            lambda: any(
                "semantic current build children record target selection changed"
                in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["builds"][
                        "children"
                    ]["binds"]["target"]["post"].__setitem__(
                        "inode",
                        current["builds"]["children"]["binds"]["target"][
                            "post"
                        ]["inode"] + 1,
                    ),
                    include_overlay=True,
                )
            ),
        )

        def current_construction_pin_rejected() -> bool:
            hostile = copy.deepcopy(current_children_fixture)
            hostile["product_commit"] = "0" * 40
            current_root = Path(hostile["construction_path"]).parents[1]
            try:
                validate_current_construction(
                    hostile, current_root, "hostile current construction pin"
                )
            except ValueError as error:
                return "identity differs" in str(error)
            return False

        check(
            "mutation-semantic-current-construction-product-pin",
            current_construction_pin_rejected,
        )

        def current_release_environment_crosslink_rejected() -> bool:
            hostile = copy.deepcopy(current_children_fixture)
            hostile["builds"]["hooked_release"]["environment"][
                "ASTERISM_BUILD_PRODUCT_TREE"
            ] = "0" * 40
            current_root = Path(hostile["construction_path"]).parents[1]
            try:
                validate_current_construction(
                    hostile, current_root,
                    "hostile current construction release environment",
                )
            except ValueError as error:
                return "hooked_release release environment differs" in str(error)
            return False

        check(
            "mutation-semantic-current-construction-release-environment",
            current_release_environment_crosslink_rejected,
        )

        def current_materialization_replay_rejected(
            *, extra_archive_member: bool
        ) -> bool:
            current_root = Path(
                current_children_fixture["construction_path"]
            ).parents[1]
            construction = schema.parse_canonical_json_object(
                Path(current_children_fixture["construction_path"]).read_bytes(),
                "hostile current construction replay",
            )
            archive = (current_root / "archives" / "source-A.tar").read_bytes()
            if extra_archive_member:
                buffer = io.BytesIO(archive)
                with tarfile.open(fileobj=buffer, mode="a:") as value:
                    payload = b"forged archive member\n"
                    info = tarfile.TarInfo("forged.txt")
                    info.mode = 0o644
                    info.mtime = 0
                    info.size = len(payload)
                    value.addfile(info, io.BytesIO(payload))
                archive = buffer.getvalue()
            lock_payload = Path(
                current_children_fixture["lock_candidates"]["A"]["path"]
            ).read_bytes()
            if not extra_archive_member:
                lock_payload += b"forged lock payload\n"
            try:
                _current_replay_materialization(
                    archive=archive,
                    lock_payload=lock_payload,
                    placements=construction["kinds"]["children"]["placements"],
                    root=current_root / "materialized" / "children",
                    patch_payload=Path(
                        current_children_fixture["inputs"][6]["path"]
                    ).read_bytes(),
                    overlay=True,
                    context="hostile current materialization replay",
                )
            except ValueError as error:
                return (
                    "live path topology differs" in str(error)
                    if extra_archive_member
                    else "live file differs: Cargo.lock" in str(error)
                )
            return False

        check(
            "mutation-semantic-current-materialization-archive-member",
            lambda: current_materialization_replay_rejected(
                extra_archive_member=True
            ),
        )

        def current_archive_appended_payload_rejected() -> bool:
            current_root = Path(
                current_children_fixture["construction_path"]
            ).parents[1]
            archive = (current_root / "archives" / "source-A.tar").read_bytes()
            archive += b"forged concatenated payload".ljust(512, b"\0")
            try:
                _current_archive_nodes(
                    archive, "hostile current archive appended payload"
                )
            except ValueError as error:
                return "trailing payload differs" in str(error)
            return False

        check(
            "mutation-semantic-current-materialization-archive-appended-payload",
            current_archive_appended_payload_rejected,
        )
        check(
            "mutation-semantic-current-materialization-lock-replacement",
            lambda: current_materialization_replay_rejected(
                extra_archive_member=False
            ),
        )

        def construction_placement_rejected() -> bool:
            path = Path(current_children_fixture["construction_path"])
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                value = json.loads(original)
                value["kinds"]["children"]["placements"][0][
                    "destination"
                ] = value["kinds"]["children"]["placements"][1]["destination"]
                path.chmod(0o644)
                path.write_bytes(canonical_json_bytes(value))
                path.chmod(mode)
                hostile = copy.deepcopy(current_children_fixture)
                hostile["construction_sha256"] = sha256_file(path)
                hostile["build_nonce"] = sha256_file(path)
                try:
                    validate_current_construction(
                        hostile, path.parents[1], "hostile current construction"
                    )
                except ValueError as error:
                    return "children placement differs" in str(error)
                return False
            finally:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)

        check(
            "mutation-semantic-current-construction-placement",
            construction_placement_rejected,
        )

        def current_build_log_sidecar_rejected() -> bool:
            current_root = Path(current_children_fixture["construction_path"]).parents[1]
            path = current_root / "logs" / "cargo-build-children.json"
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                value = json.loads(original)
                value["exit_status"] = 1
                path.chmod(0o644)
                path.write_bytes(canonical_json_bytes(value))
                path.chmod(mode)
                return any(
                    "semantic current build children record execution log sidecar differs"
                    in error
                    for error in semantic_chain_errors(
                        lambda _current, _lock, _prepared: None,
                        include_overlay=True,
                    )
                )
            finally:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)

        check(
            "mutation-semantic-current-build-log-sidecar",
            current_build_log_sidecar_rejected,
        )

        def current_materialized_manifest_sidecar_rejected() -> bool:
            current_root = Path(current_children_fixture["construction_path"]).parents[1]
            path = current_root / "manifests" / "materialized-children.json"
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                value = json.loads(original)
                value["entries"][0]["size"] += 1
                path.chmod(0o644)
                path.write_bytes(canonical_json_bytes(value))
                path.chmod(mode)
                try:
                    _current_materialized_manifest_sidecar(
                        current_root, "children",
                        current_root / "materialized" / "children",
                        current_children_fixture["builds"]["children"][
                            "source_manifest_sha256"
                        ],
                        "hostile current materialized manifest",
                    )
                except ValueError as error:
                    return "exact live replay differs" in str(error)
                return False
            finally:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)

        check(
            "mutation-semantic-current-materialized-manifest-sidecar",
            current_materialized_manifest_sidecar_rejected,
        )
        check(
            "mutation-semantic-resolver-output-redirection",
            lambda: any(
                "semantic resolution B final-lock authority differs" in error
                for error in semantic_chain_errors(
                    lambda _current, lock, _prepared: lock["lock_manifest"][
                        "payload"
                    ]["variants"]["B"].__setitem__(
                        "final_lock_path",
                        lock["lock_manifest"]["payload"]["variants"]["A"][
                            "final_lock_path"
                        ],
                    ),
                    include_overlay=True,
                )
            ),
        )

        def redirect_resolver_namespace(
            _current: dict[str, Any],
            lock_authority: dict[str, Any],
            _prepared: dict[str, Any],
        ) -> None:
            argv = lock_authority["lock_manifest"]["payload"]["variants"]["C"][
                "current_lock_attempt"
            ]["argv"]
            argv[argv.index("/dev") - 1] = "--dev-bind"

        check(
            "mutation-semantic-resolver-private-namespace",
            lambda: any(
                "semantic resolver C current" in error
                and "private namespace differs" in error
                for error in semantic_chain_errors(
                    redirect_resolver_namespace, include_overlay=True
                )
            ),
        )

        def redirect_resolver_cargo_home_overlay(
            _current: dict[str, Any],
            lock_authority: dict[str, Any],
            _prepared: dict[str, Any],
        ) -> None:
            argv = lock_authority["lock_manifest"]["payload"]["variants"]["C"][
                "current_lock_attempt"
            ]["argv"]
            overlay = argv.index("--overlay-src")
            argv[overlay + 1] = "/proc/self/fd/0707"

        check(
            "mutation-semantic-resolver-cargo-home-overlay",
            lambda: any(
                "semantic resolver C current" in error
                and "Cargo-home overlay differs" in error
                for error in semantic_chain_errors(
                    redirect_resolver_cargo_home_overlay, include_overlay=True
                )
            ),
        )
        check(
            "mutation-semantic-runtime-digest",
            lambda: any(
                "runtime digest differs" in error
                for error in semantic_chain_errors(
                    lambda current, _lock, _prepared: current["builds"][
                        "children"
                    ]["semantic_input_authority"].__setitem__(
                        "runtime_sha256", "0" * 64
                    ),
                    include_overlay=True,
                )
            ),
        )
        check(
            "mutation-semantic-missing-overlay-topology",
            lambda: any(
                "authorities=11 manifests=44 identities=44" in error
                for error in semantic_chain_errors(
                    lambda _current, _lock, _prepared: None,
                    include_overlay=False,
                )
            ),
        )

        def semantic_manifest_hardlink_rejected() -> bool:
            first_attestation = prepared_fixture["variants"]["A"]["attestation"]
            second_attestation = prepared_fixture["variants"]["B"]["attestation"]
            first_path = Path(
                first_attestation["semantic_input_authority"]["toolchain"][
                    "manifest_path"
                ]
            )
            second_path = Path(
                second_attestation["semantic_input_authority"]["toolchain"][
                    "manifest_path"
                ]
            )
            second_bytes = second_path.read_bytes()
            second_mode = stat.S_IMODE(second_path.stat().st_mode)
            try:
                second_path.unlink()
                os.link(first_path, second_path)
                validation = Problems()
                replay = SemanticReplay(validation, live_system=False)
                replay.capture(
                    "hostile hardlink semantic A",
                    lambda: replay.validate_authority(
                        first_attestation["semantic_input_authority"],
                        "hostile hardlink semantic A",
                        live_roots=SemanticReplay.live_roots(
                            Path(first_attestation["materialized_root"]),
                            first_attestation["toolchain"],
                            "hostile hardlink semantic A",
                        ),
                    ),
                )
                return any(
                    "semantic manifest identity aliases" in error
                    for error in validation.errors
                )
            finally:
                if second_path.exists():
                    second_path.unlink()
                fixture_write_file(second_path, second_bytes, second_mode)

        check(
            "mutation-semantic-manifest-hardlink-alias",
            semantic_manifest_hardlink_rejected,
        )

        def sandbox_argv_mutation(
            mutator: Callable[[list[str]], None], expected_error: str
        ) -> bool:
            argv = copy.deepcopy(
                prepared_fixture["variants"]["A"]["attestation"]["build_argv"]
            )
            mutator(argv)
            validation = Problems()
            validate_sandboxed_build_argv(
                argv,
                bwrap_path=prepared_fixture["toolchain"]["bwrap_path"],
                cargo_config_search_sha256=prepared_fixture["variants"]["A"][
                    "attestation"
                ]["cargo_config_search"]["sha256"],
                semantic_runtime_sha256=prepared_fixture["variants"]["A"][
                    "attestation"
                ]["semantic_input_authority"]["runtime_sha256"],
                rustc_host=prepared_fixture["toolchain"]["rustc_host"],
                execution_tools_sha256=hashlib.sha256(
                    schema.prepared_authority_canonical_json_bytes(
                        prepared_fixture["variants"]["A"]["attestation"][
                            "execution_tools"
                        ]
                    )
                ).hexdigest(),
                package="mess-store",
                example="asterism_rebaseline_public",
                context="hostile release sandbox argv",
                problems=validation,
            )
            return any(expected_error in error for error in validation.errors)

        check(
            "mutation-release-sandbox-fd-leading-zero",
            lambda: sandbox_argv_mutation(
                lambda argv: argv.__setitem__(
                    argv.index("--ro-bind-fd") + 1, "03"
                ),
                "descriptor binding",
            ),
        )
        check(
            "mutation-release-sandbox-fd-duplicate",
            lambda: sandbox_argv_mutation(
                lambda argv: argv.__setitem__(
                    argv.index("--bind-fd") + 1,
                    argv[argv.index("--ro-bind-fd") + 1],
                ),
                "descriptor operands are not distinct",
            ),
        )
        check(
            "mutation-release-sandbox-null-device-leading-zero",
            lambda: sandbox_argv_mutation(
                lambda argv: argv.__setitem__(
                    argv.index("--dev-bind") + 1, "/proc/self/fd/0404"
                ),
                "private namespace differs",
            ),
        )
        check(
            "mutation-release-sandbox-null-device-destination",
            lambda: sandbox_argv_mutation(
                lambda argv: argv.__setitem__(
                    argv.index("--dev-bind") + 2, "/dev/zero"
                ),
                "private namespace differs",
            ),
        )
        check(
            "mutation-release-sandbox-rust-lld-destination",
            lambda: sandbox_argv_mutation(
                lambda argv: argv.__setitem__(
                    next(
                        index
                        for index, argument in enumerate(argv)
                        if argument.endswith("/bin/gcc-ld/ld.lld")
                    ),
                    f"{GUEST_TOOLCHAIN_ROOT}/bin/rust-lld",
                ),
                "descriptor binding",
            ),
        )
        check(
            "mutation-release-sandbox-config-fd-alias",
            lambda: sandbox_argv_mutation(
                lambda argv: argv.__setitem__(
                    argv.index(GUEST_BOUND_CONFIG_PATHS[0]) - 1,
                    argv[argv.index("--ro-bind-fd") + 1],
                ),
                "descriptor operands are not distinct",
            ),
        )
        check(
            "mutation-release-sandbox-config-order",
            lambda: sandbox_argv_mutation(
                lambda argv: argv.__setitem__(
                    argv.index(GUEST_BOUND_CONFIG_PATHS[0]),
                    GUEST_BOUND_CONFIG_PATHS[1],
                ),
                "descriptor binding",
            ),
        )
        check(
            "mutation-release-sandbox-extra-bind-fd",
            lambda: sandbox_argv_mutation(
                lambda argv: argv.extend(("--bind-fd", "999", "/forged")),
                "prefix/cardinality differs",
            ),
        )
        check(
            "mutation-release-sandbox-overlay-fd-leading-zero",
            lambda: sandbox_argv_mutation(
                lambda argv: argv.__setitem__(
                    argv.index("--overlay-src") + 1, "/proc/self/fd/0409"
                ),
                "Cargo-home overlay differs",
            ),
        )
        check(
            "mutation-release-sandbox-overlay-fd-alias",
            lambda: sandbox_argv_mutation(
                lambda argv: argv.__setitem__(
                    argv.index("--overlay-src") + 1,
                    f"/proc/self/fd/{argv[argv.index('--ro-bind-fd') + 1]}",
                ),
                "descriptor operands are not distinct",
            ),
        )
        check(
            "mutation-release-sandbox-old-direct-cargo-home-bind",
            lambda: sandbox_argv_mutation(
                lambda argv: argv.__setitem__(
                    argv.index("--overlay-src"), "--ro-bind-fd"
                ),
                "Cargo-home overlay differs",
            ),
        )
        check(
            "mutation-release-sandbox-cargo-home-lower-path",
            lambda: sandbox_argv_mutation(
                lambda argv: argv.__setitem__(
                    argv.index("--tmp-overlay") + 1,
                    f"{GUEST_ROOT}/cargo-home-lower",
                ),
                "Cargo-home overlay differs",
            ),
        )

        def sandbox_config_hash_rejected() -> bool:
            validation = Problems()
            validate_sandboxed_build_argv(
                prepared_fixture["variants"]["A"]["attestation"]["build_argv"],
                bwrap_path=prepared_fixture["toolchain"]["bwrap_path"],
                cargo_config_search_sha256="not-a-sha256",
                semantic_runtime_sha256=prepared_fixture["variants"]["A"][
                    "attestation"
                ]["semantic_input_authority"]["runtime_sha256"],
                rustc_host=prepared_fixture["toolchain"]["rustc_host"],
                execution_tools_sha256=hashlib.sha256(
                    schema.prepared_authority_canonical_json_bytes(
                        prepared_fixture["variants"]["A"]["attestation"][
                            "execution_tools"
                        ]
                    )
                ).hexdigest(),
                package="mess-store",
                example="asterism_rebaseline_public",
                context="hostile release sandbox config",
                problems=validation,
            )
            return any(
                "Cargo config/tool/semantic hash is invalid" in error
                for error in validation.errors
            )

        check(
            "mutation-release-sandbox-config-hash-malformed",
            sandbox_config_hash_rejected,
        )

        def execution_tools_mutation(
            mutator: Callable[[dict[str, Any]], None], expected_error: str
        ) -> bool:
            tools = copy.deepcopy(
                prepared_fixture["variants"]["A"]["attestation"][
                    "execution_tools"
                ]
            )
            mutator(tools)
            validation = Problems()
            validate_prepared_execution_tools(
                tools,
                prepared_fixture["toolchain"],
                "hostile release execution tools",
                validation,
            )
            return any(expected_error in error for error in validation.errors)

        check(
            "mutation-release-execution-tools-missing-rust-lld",
            lambda: execution_tools_mutation(
                lambda tools: tools.pop("rust_lld"), "fields are not exact"
            ),
        )
        check(
            "mutation-release-execution-tools-null-device-float-minor",
            lambda: execution_tools_mutation(
                lambda tools: tools["dev_null"]["identity"].__setitem__(
                    "minor", 3.0
                ),
                "null-device identity differs",
            ),
        )
        check(
            "mutation-release-execution-tools-rust-lld-path",
            lambda: execution_tools_mutation(
                lambda tools: tools["rust_lld"].__setitem__(
                    "path", prepared_fixture["toolchain"]["rustc_path"]
                ),
                "rust_lld binding differs",
            ),
        )
        check(
            "mutation-release-execution-tools-root-bool-mode",
            lambda: execution_tools_mutation(
                lambda tools: tools["toolchain_root"].__setitem__("mode", True),
                "toolchain-root binding differs",
            ),
        )

        def sandbox_empty_config_authority_rejected() -> bool:
            attestation = prepared_fixture["variants"]["A"]["attestation"]
            binding = attestation["cargo_config_search"]
            empty_path = Path(f"{binding['path']}.empty")
            original = empty_path.read_bytes()
            mode = stat.S_IMODE(empty_path.stat().st_mode)
            try:
                empty_path.chmod(0o644)
                empty_path.write_bytes(b"forged\n")
                empty_path.chmod(mode)
                validation = Problems()
                validate_sandboxed_cargo_config_search(
                    binding,
                    Path(attestation["materialized_root"]),
                    prepared_fixture["toolchain"],
                    "hostile release sandbox config",
                    validation,
                )
                return any(
                    "retained empty authority hash mismatch" in error
                    for error in validation.errors
                )
            finally:
                empty_path.chmod(0o644)
                empty_path.write_bytes(original)
                empty_path.chmod(mode)

        check(
            "mutation-release-sandbox-empty-config-authority",
            sandbox_empty_config_authority_rejected,
        )

        def semantic_proof_mutation(
            mutator: Callable[[dict[str, Any], dict[str, Any]], None],
            expected_error: str,
        ) -> bool:
            proof = copy.deepcopy(proof_fixture)
            hostile_prepared = copy.deepcopy(prepared_fixture)
            mutator(proof, hostile_prepared)
            validation = Problems()
            validate_release_compile_out_proof(
                proof,
                approval_sha256=sha256_file(output / "source-approval.json"),
                source_review=source_review_fixture,
                current_children=current_children_fixture,
                current_children_sha256=hashlib.sha256(
                    canonical_json_bytes(current_children_fixture)
                ).hexdigest(),
                prepared=hostile_prepared,
                config=config_fixture,
                semantic_replay=SemanticReplay(
                    validation, live_system=False
                ),
                problems=validation,
            )
            return any(expected_error in error for error in validation.errors)

        check(
            "mutation-release-proof-published-a-rebound",
            lambda: semantic_proof_mutation(
                lambda proof, _prepared: proof.__setitem__(
                    "published_a_sha256", "0" * 64
                ),
                "published A hash differs",
            ),
        )
        check(
            "mutation-release-proof-forbidden-absence-rebound",
            lambda: semantic_proof_mutation(
                lambda proof, _prepared: proof.__setitem__(
                    "forbidden_hook_strings_absent", False
                ),
                "forbidden_hook_strings_absent differs",
            ),
        )

        def rebind_overlay_nonce(
            proof: dict[str, Any], _prepared: dict[str, Any]
        ) -> None:
            build = proof["builds"]["overlay_a"]
            build["attestation"]["build_nonce"] = "0" * 64
            build["attestation_sha256"] = hashlib.sha256(
                canonical_json_bytes(build["attestation"])
            ).hexdigest()

        check(
            "mutation-release-proof-overlay-nonce-rebound",
            lambda: semantic_proof_mutation(
                rebind_overlay_nonce,
                "embedded nonce/lock/environment differs",
            ),
        )

        def rehash_release_build(
            proof: dict[str, Any], name: str
        ) -> dict[str, Any]:
            build = proof["builds"][name]
            build["attestation_sha256"] = hashlib.sha256(
                canonical_json_bytes(build["attestation"])
            ).hexdigest()
            return build["attestation"]

        def mutate_build_child_field(
            proof: dict[str, Any],
            _prepared: dict[str, Any],
            name: str,
            field: str,
            value: Any,
        ) -> None:
            attestation = proof["builds"][name]["attestation"]
            attestation["build_child"][field] = value
            rehash_release_build(proof, name)

        check(
            "mutation-release-build-ordinary-nonzero-exit",
            lambda: semantic_proof_mutation(
                lambda proof, prepared: mutate_build_child_field(
                    proof, prepared, "ordinary_a", "exit_status", 1
                ),
                "child completion/reaping status invalid",
            ),
        )
        check(
            "mutation-release-build-overlay-timeout",
            lambda: semantic_proof_mutation(
                lambda proof, prepared: mutate_build_child_field(
                    proof, prepared, "overlay_a", "timed_out", True
                ),
                "child completion/reaping status invalid",
            ),
        )
        check(
            "mutation-release-build-overlay-orphan-process-group",
            lambda: semantic_proof_mutation(
                lambda proof, prepared: mutate_build_child_field(
                    proof, prepared, "overlay_a", "process_group_absent", False
                ),
                "child completion/reaping status invalid",
            ),
        )
        check(
            "mutation-release-build-overlay-passed-fd-cardinality",
            lambda: semantic_proof_mutation(
                lambda proof, prepared: mutate_build_child_field(
                    proof,
                    prepared,
                    "overlay_a",
                    "passed_file_descriptors",
                    16.0,
                ),
                "passed descriptor cardinality mismatch",
            ),
        )

        def mutate_build_reaping(
            proof: dict[str, Any], _prepared: dict[str, Any]
        ) -> None:
            attestation = proof["builds"]["overlay_a"]["attestation"]
            attestation["build_child"]["reaping"]["status"] = "present"
            rehash_release_build(proof, "overlay_a")

        check(
            "mutation-release-build-overlay-reaping",
            lambda: semantic_proof_mutation(
                mutate_build_reaping, "reaping identity/status mismatch"
            ),
        )

        def mutate_build_reaping_bool_identity(
            proof: dict[str, Any], _prepared: dict[str, Any]
        ) -> None:
            attestation = proof["builds"]["overlay_a"]["attestation"]
            child = attestation["build_child"]
            child["pid"] = 1
            child["waited_pid"] = 1
            child["start_ticks"] = 1
            child["reaping"] = {
                "pid": True,
                "start_ticks": True,
                "status": "absent",
            }
            rehash_release_build(proof, "overlay_a")

        check(
            "mutation-release-build-overlay-reaping-bool-identity",
            lambda: semantic_proof_mutation(
                mutate_build_reaping_bool_identity,
                "reaping identity invalid",
            ),
        )
        check(
            "mutation-release-build-overlay-argv",
            lambda: semantic_proof_mutation(
                lambda proof, prepared: mutate_build_child_field(
                    proof, prepared, "overlay_a", "argv", ["/forged"]
                ),
                "child argv mismatch",
            ),
        )
        check(
            "mutation-release-build-overlay-cwd",
            lambda: semantic_proof_mutation(
                lambda proof, prepared: mutate_build_child_field(
                    proof, prepared, "overlay_a", "cwd", "/forged"
                ),
                "child cwd differs",
            ),
        )

        def mutate_build_monotonic_chronology(
            proof: dict[str, Any], _prepared: dict[str, Any]
        ) -> None:
            attestation = proof["builds"]["overlay_a"]["attestation"]
            child = attestation["build_child"]
            completed = child["started_monotonic_ns"] - 1
            child["completed_monotonic_ns"] = completed
            attestation["build_completed_monotonic_ns"] = completed
            rehash_release_build(proof, "overlay_a")

        check(
            "mutation-release-build-overlay-monotonic-chronology",
            lambda: semantic_proof_mutation(
                mutate_build_monotonic_chronology,
                "child monotonic chronology invalid",
            ),
        )

        def mutate_build_time_scalar(
            proof: dict[str, Any],
            _prepared: dict[str, Any],
            field: str,
            value: Any,
        ) -> None:
            attestation = proof["builds"]["overlay_a"]["attestation"]
            attestation["build_child"][field] = value
            attestation[f"build_{field}"] = value
            rehash_release_build(proof, "overlay_a")

        check(
            "mutation-release-build-overlay-string-monotonic-scalar",
            lambda: semantic_proof_mutation(
                lambda proof, prepared: mutate_build_time_scalar(
                    proof,
                    prepared,
                    "started_monotonic_ns",
                    "forged",
                ),
                "child started_monotonic_ns invalid",
            ),
        )
        check(
            "mutation-release-build-overlay-bool-monotonic-scalar",
            lambda: semantic_proof_mutation(
                lambda proof, prepared: mutate_build_time_scalar(
                    proof,
                    prepared,
                    "completed_monotonic_ns",
                    False,
                ),
                "child completed_monotonic_ns invalid",
            ),
        )
        check(
            "mutation-release-build-overlay-bool-wall-start",
            lambda: semantic_proof_mutation(
                lambda proof, prepared: mutate_build_time_scalar(
                    proof, prepared, "started_at", False
                ),
                "child started_at is not a timestamp",
            ),
        )
        check(
            "mutation-release-build-overlay-scalar-wall-completion",
            lambda: semantic_proof_mutation(
                lambda proof, prepared: mutate_build_time_scalar(
                    proof, prepared, "completed_at", 7
                ),
                "child completed_at is not a timestamp",
            ),
        )
        check(
            "mutation-release-build-overlay-noncanonical-wall-time",
            lambda: semantic_proof_mutation(
                lambda proof, prepared: mutate_build_time_scalar(
                    proof,
                    prepared,
                    "started_at",
                    "2026-07-15 00:00:02+00:00",
                ),
                "child started_at is not an exact zoned timestamp",
            ),
        )

        def mutate_build_cross_wall_nanosecond_reversal(
            proof: dict[str, Any], _prepared: dict[str, Any]
        ) -> None:
            ordinary_attestation = proof["builds"]["ordinary_a"]["attestation"]
            overlay_attestation = proof["builds"]["overlay_a"]["attestation"]
            ordinary_attestation["build_child"]["completed_at"] = (
                "2026-07-15T00:00:02.000000001+00:00"
            )
            ordinary_attestation["build_completed_at"] = ordinary_attestation[
                "build_child"
            ]["completed_at"]
            overlay_attestation["build_child"]["started_at"] = (
                "2026-07-15T00:00:02.000000000+00:00"
            )
            overlay_attestation["build_started_at"] = overlay_attestation[
                "build_child"
            ]["started_at"]
            rehash_release_build(proof, "ordinary_a")
            rehash_release_build(proof, "overlay_a")

        check(
            "mutation-release-build-cross-wall-nanosecond-reversal",
            lambda: semantic_proof_mutation(
                mutate_build_cross_wall_nanosecond_reversal,
                "build wall chronology overlaps",
            ),
        )

        def mutate_build_wall_chronology(
            proof: dict[str, Any], _prepared: dict[str, Any]
        ) -> None:
            attestation = proof["builds"]["overlay_a"]["attestation"]
            completed = "2026-07-14T23:59:59+00:00"
            attestation["build_child"]["completed_at"] = completed
            attestation["build_completed_at"] = completed
            rehash_release_build(proof, "overlay_a")

        check(
            "mutation-release-build-overlay-wall-chronology",
            lambda: semantic_proof_mutation(
                mutate_build_wall_chronology,
                "child wall chronology invalid",
            ),
        )

        def mutate_build_crosslink(
            proof: dict[str, Any], _prepared: dict[str, Any]
        ) -> None:
            attestation = proof["builds"]["overlay_a"]["attestation"]
            attestation["build_started_monotonic_ns"] += 1
            rehash_release_build(proof, "overlay_a")

        check(
            "mutation-release-build-overlay-attestation-crosslink",
            lambda: semantic_proof_mutation(
                mutate_build_crosslink,
                "child started_monotonic_ns attestation crosslink differs",
            ),
        )

        def rebind_release_build_log(
            proof: dict[str, Any],
            name: str,
            filename: str,
            payload: dict[str, Any],
        ) -> None:
            path = Path(
                prepared_fixture["release_compile_out"]["path"]
            ).parent / filename
            fixture_write_json(path, payload)
            attestation = proof["builds"][name]["attestation"]
            attestation["build_log_path"] = str(path.resolve())
            attestation["build_log_sha256"] = sha256_file(path)
            attestation["build_child"]["output_path"] = str(path.resolve())
            attestation["build_child"]["output_sha256"] = sha256_file(path)
            rehash_release_build(proof, name)

        check(
            "mutation-release-build-overlay-rehashed-failed-log",
            lambda: semantic_proof_mutation(
                lambda proof, _prepared: rebind_release_build_log(
                    proof,
                    "overlay_a",
                    "A-product-overlay-failed.json",
                    {
                        "exit_status": 1,
                        "stderr": "failed",
                        "stderr_sha256": hashlib.sha256(b"failed").hexdigest(),
                        "stdout": "",
                        "stdout_sha256": EMPTY_SHA256,
                    },
                ),
                "build log output authority differs",
            ),
        )
        check(
            "mutation-release-build-overlay-log-output-hash",
            lambda: semantic_proof_mutation(
                lambda proof, _prepared: rebind_release_build_log(
                    proof,
                    "overlay_a",
                    "A-product-overlay-bad-stdout-hash.json",
                    {
                        "exit_status": 0,
                        "stderr": "",
                        "stderr_sha256": EMPTY_SHA256,
                        "stdout": "forged",
                        "stdout_sha256": EMPTY_SHA256,
                    },
                ),
                "build log output authority differs",
            ),
        )

        def mutate_build_log_hash(
            proof: dict[str, Any], _prepared: dict[str, Any]
        ) -> None:
            attestation = proof["builds"]["overlay_a"]["attestation"]
            attestation["build_log_sha256"] = "0" * 64
            attestation["build_child"]["output_sha256"] = "0" * 64
            rehash_release_build(proof, "overlay_a")

        check(
            "mutation-release-build-overlay-log-hash",
            lambda: semantic_proof_mutation(
                mutate_build_log_hash, "build log hash mismatch"
            ),
        )

        def alias_release_build_log(
            proof: dict[str, Any], _prepared: dict[str, Any]
        ) -> None:
            ordinary = proof["builds"]["ordinary_a"]["attestation"]
            overlay = proof["builds"]["overlay_a"]["attestation"]
            overlay["build_log_path"] = ordinary["build_log_path"]
            overlay["build_log_sha256"] = ordinary["build_log_sha256"]
            overlay["build_child"]["output_path"] = ordinary["build_log_path"]
            overlay["build_child"]["output_sha256"] = ordinary[
                "build_log_sha256"
            ]
            rehash_release_build(proof, "overlay_a")

        check(
            "mutation-release-build-log-physical-alias",
            lambda: semantic_proof_mutation(
                alias_release_build_log, "build logs are not physically disjoint"
            ),
        )

        def alias_release_materialized_root(
            proof: dict[str, Any], _prepared: dict[str, Any]
        ) -> None:
            ordinary = proof["builds"]["ordinary_a"]["attestation"]
            overlay = proof["builds"]["overlay_a"]["attestation"]
            overlay["materialized_root"] = ordinary["materialized_root"] + "/."
            overlay["build_child"]["cwd"] = overlay["materialized_root"]
            rehash_release_build(proof, "overlay_a")

        check(
            "mutation-release-build-materialized-root-lexical-alias",
            lambda: semantic_proof_mutation(
                alias_release_materialized_root,
                "materialized root is not canonical",
            ),
        )

        def duplicate_release_build_identity(
            proof: dict[str, Any], _prepared: dict[str, Any]
        ) -> None:
            ordinary = proof["builds"]["ordinary_a"]["attestation"][
                "build_child"
            ]
            overlay = proof["builds"]["overlay_a"]["attestation"][
                "build_child"
            ]
            overlay["pid"] = ordinary["pid"]
            overlay["waited_pid"] = ordinary["pid"]
            overlay["start_ticks"] = ordinary["start_ticks"]
            overlay["reaping"] = {
                "pid": ordinary["pid"],
                "start_ticks": ordinary["start_ticks"],
                "status": "absent",
            }
            rehash_release_build(proof, "overlay_a")

        check(
            "mutation-release-build-duplicate-event-identity",
            lambda: semantic_proof_mutation(
                duplicate_release_build_identity,
                "build event identities are not distinct",
            ),
        )

        def overlap_release_build_events(
            proof: dict[str, Any], _prepared: dict[str, Any]
        ) -> None:
            ordinary = proof["builds"]["ordinary_a"]["attestation"][
                "build_child"
            ]
            overlay_attestation = proof["builds"]["overlay_a"]["attestation"]
            overlay = overlay_attestation["build_child"]
            overlay["started_monotonic_ns"] = ordinary[
                "completed_monotonic_ns"
            ]
            overlay_attestation["build_started_monotonic_ns"] = overlay[
                "started_monotonic_ns"
            ]
            rehash_release_build(proof, "overlay_a")

        check(
            "mutation-release-build-overlapping-events",
            lambda: semantic_proof_mutation(
                overlap_release_build_events,
                "build monotonic chronology overlaps",
            ),
        )
        check(
            "mutation-release-proof-inventory-alias",
            lambda: semantic_proof_mutation(
                lambda proof, _prepared: proof["symbol_inventories"].__setitem__(
                    "overlay_a",
                    copy.deepcopy(proof["symbol_inventories"]["ordinary_a"]),
                ),
                "symbol inventories are not equal disjoint files",
            ),
        )
        check(
            "mutation-release-proof-nm-argv",
            lambda: semantic_proof_mutation(
                lambda proof, _prepared: proof["nm"]["overlay_a"]["argv"].__setitem__(
                    1, "--external-only"
                ),
                "nm child overlay_a argv differs",
            ),
        )

        def rebind_nm_stdout(
            proof: dict[str, Any], _prepared: dict[str, Any]
        ) -> None:
            output_path = output.parent / "logs" / "nm-overlay-a-hostile.json"
            stdout = "forged_release_symbol T 0\n"
            fixture_write_json(
                output_path,
                {
                    "exit_status": 0,
                    "stderr": "",
                    "stderr_sha256": EMPTY_SHA256,
                    "stdout": stdout,
                    "stdout_sha256": hashlib.sha256(stdout.encode()).hexdigest(),
                },
            )
            child = proof["nm"]["overlay_a"]
            child["output_path"] = str(output_path.resolve())
            child["output_sha256"] = sha256_file(output_path)

        check(
            "mutation-release-proof-nm-stdout-inventory-divergence",
            lambda: semantic_proof_mutation(
                rebind_nm_stdout,
                "nm child overlay_a inventory bytes differ",
            ),
        )

        def make_twin_reachable(
            proof: dict[str, Any], hostile_prepared: dict[str, Any]
        ) -> None:
            hostile_prepared["variants"]["A"]["evidence_argv"].append(
                proof["binaries"]["overlay_a"]["path"]
            )

        check(
            "mutation-release-proof-twin-child-reachable",
            lambda: semantic_proof_mutation(
                make_twin_reachable,
                "proof-only overlay A is child-reachable",
            ),
        )

        def completed_child_twin_rejected() -> bool:
            hostile_children = copy.deepcopy(positive_children)
            hostile_children[-1]["argv"].append(
                proof_fixture["binaries"]["overlay_a"]["path"]
            )
            validation = Problems()
            validate_proof_only_path_after_children(
                prepared_fixture, hostile_children, validation
            )
            return any(
                "completed child manifest" in error
                for error in validation.errors
            )

        check(
            "mutation-release-proof-twin-final-child-reachable",
            completed_child_twin_rejected,
        )

        def hostile_seal_verdict_rejected() -> bool:
            bundle = json.loads(
                Path(prepared_fixture["source_review"]["bundle"]["path"])
                .read_bytes()
            )
            bundle["verdict"]["data"]["vote"] = "reject"
            validation = Problems()
            validate_source_review_bundle_authority(
                bundle,
                source_review_fixture,
                approval_fixture,
                current_children_fixture,
                json.loads(
                    Path(
                        prepared_fixture["source_review"]["lock_authority"][
                            "path"
                        ]
                    ).read_bytes()
                ),
                json.loads(
                    Path(
                        prepared_fixture["source_review"][
                            "lock_review_bundle"
                        ]["path"]
                    ).read_bytes()
                ),
                validation,
            )
            return any(
                "ReviewerVoted verdict differs" in error
                for error in validation.errors
            )

        check("mutation-source-review-seal-verdict", hostile_seal_verdict_rejected)

        def hostile_source_input_alias_rejected() -> bool:
            bundle = json.loads(
                Path(prepared_fixture["source_review"]["bundle"]["path"])
                .read_bytes()
            )
            hostile_review = copy.deepcopy(source_review_fixture)
            first, second = schema.SOURCE_REVIEW_INPUT_NAMES[:2]
            first_input = bundle["assertion"]["inputs"][first]
            second_input = bundle["assertion"]["inputs"][second]
            second_input["path"] = first_input["path"]
            second_input["identity"] = copy.deepcopy(first_input["identity"])
            assertion_sha256 = hashlib.sha256(
                canonical_json_bytes(bundle["assertion"])
            ).hexdigest()
            bundle["assertion_sha256"] = assertion_sha256
            hostile_review["assertion_sha256"] = assertion_sha256
            validation = Problems()
            validate_source_review_bundle_authority(
                bundle,
                hostile_review,
                approval_fixture,
                current_children_fixture,
                json.loads(
                    Path(
                        prepared_fixture["source_review"]["lock_authority"]["path"]
                    ).read_bytes()
                ),
                json.loads(
                    Path(
                        prepared_fixture["source_review"]["lock_review_bundle"][
                            "path"
                        ]
                    ).read_bytes()
                ),
                validation,
            )
            return any(
                "inputs are not physically disjoint" in error
                for error in validation.errors
            )

        check(
            "mutation-source-review-input-alias",
            hostile_source_input_alias_rejected,
        )

        def hostile_embedded_lock_review_rejected() -> bool:
            lock_authority = json.loads(
                Path(
                    prepared_fixture["source_review"]["lock_authority"]["path"]
                ).read_bytes()
            )
            lock_review = json.loads(
                Path(
                    prepared_fixture["source_review"]["lock_review_bundle"]["path"]
                ).read_bytes()
            )
            lock_authority["review_bundle"]["payload"] = {"forged": True}
            validation = Problems()
            validate_source_review_bundle_authority(
                json.loads(
                    Path(prepared_fixture["source_review"]["bundle"]["path"])
                    .read_bytes()
                ),
                source_review_fixture,
                approval_fixture,
                current_children_fixture,
                lock_authority,
                lock_review,
                validation,
            )
            return any(
                "current lock review authority crosslinks differ" in error
                for error in validation.errors
            )

        check(
            "mutation-source-review-embedded-lock-review",
            hostile_embedded_lock_review_rejected,
        )

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

        def rehearsal_cannot_admit() -> bool:
            # Protocol v4 §3: a fully consistent rehearsal bundle (clean rows,
            # no structural errors) must still refuse to yield an ADMIT/NARROW/
            # REVERT decision — its outcome is forced to INCONCLUSIVE so it can
            # never be laundered into an accepted result.
            config_path = output / "config.json"
            provenance_path = output / "provenance.json"
            originals = {p: p.read_bytes() for p in (config_path, provenance_path)}
            modes = {p: stat.S_IMODE(p.stat().st_mode) for p in originals}
            try:
                config_value = json.loads(originals[config_path])
                config_value["rehearsal"] = True
                provenance_value = json.loads(originals[provenance_path])
                provenance_value["rehearsal"] = True
                provenance_value["declaration_sha256"] = None
                provenance_value["output_dir_absent_before"] = True
                for path, value in (
                    (config_path, config_value),
                    (provenance_path, provenance_value),
                ):
                    path.chmod(0o644)
                    path.write_bytes(canonical_json_bytes(value))
                    path.chmod(modes[path])
                result, rc = evaluate_directory(
                    output, synthetic=True, publish=False
                )
                # The post-hoc edit of config/provenance also breaks their
                # recorded self-hashes, so evidence_valid is incidentally
                # False here; the decisive property is that a rehearsal-marked
                # bundle yields INCONCLUSIVE with the rehearsal flag surfaced,
                # never ADMIT/NARROW/REVERT.  The terminal verifier's outright
                # rejection of rehearsal results is the complementary guard.
                return (
                    rc == EXIT_INCONCLUSIVE
                    and result["outcome"] == "INCONCLUSIVE"
                    and result["rehearsal"] is True
                    and result["outcome"] not in {"ADMIT", "NARROW", "REVERT"}
                )
            finally:
                for path, original in originals.items():
                    path.chmod(0o644)
                    path.write_bytes(original)
                    path.chmod(modes[path])

        check("v4-rehearsal-evidence-cannot-admit", rehearsal_cannot_admit)
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
        lock_path = Path(prepared["variants"]["C"]["attestation"]["cargo_lock_path"])
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
                if code != EXIT_ADMIT or result["evidence_valid"] is not True:
                    raise AssertionError(result["errors"][:20])
                return True
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
            if code != EXIT_ADMIT:
                raise AssertionError(published["errors"][:20])
            return (
                (output / RESULT_NAME).read_bytes() == canonical_json_bytes(published)
                and (output / "REPORT.md").read_bytes() == render_report(published)
                and b"Fjall-era production engine" in (output / "REPORT.md").read_bytes()
                and b"not pure engine overhead" in (output / "REPORT.md").read_bytes()
            )

        check("publication-result-and-report-canonical", publication_is_canonical)
        check(
            "mutation-lock-hash-replay",
            lambda: mutate_bytes(lock_path, b"mutated fixture lock\n"),
        )
        check(
            "mutation-binary-hash-replay",
            lambda: mutate_bytes(binary_path, b"mutated fixture binary\n"),
        )

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

        check(
            "mutation-semantic-current-cargo-config-directory",
            lambda: current_config_reserved_type_rejected("directory"),
        )
        check(
            "mutation-semantic-current-cargo-config-dangling-symlink",
            lambda: current_config_reserved_type_rejected("symlink"),
        )
        check(
            "mutation-semantic-current-cargo-config-hardlink",
            lambda: current_config_reserved_type_rejected("hardlink"),
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
