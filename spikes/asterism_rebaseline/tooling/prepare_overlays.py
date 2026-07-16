#!/usr/bin/env python3
"""Stage, attest, and build source-bound bn-2l3n measurement overlays.

`stage-locks` is resolution-only: it performs no Rust compilation and emits no
measurement row. `build` requires a separately approved canonical source
manifest, builds every variant `--locked --offline`, and runs contract mode
only. The timing runner is a separate reviewed component.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import io
import json
import os
import secrets
import selectors
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROTOCOL = "bn-2l3n-asterism-rebaseline-v3"
PLAN_SCHEMA = "asterism-rebaseline-source-plan-v3"
LOCK_SCHEMA = "asterism-rebaseline-lock-candidates-v3"
APPROVAL_SCHEMA = "bn-2l3n-source-approval-v3"
PREPARED_SCHEMA = "bn-2l3n-prepared-artifacts-v3"
CONTRACT_SCHEMA = "bn-2l3n-binary-contract-v3"
CARGO_CONFIG_SCHEMA = "asterism-rebaseline-cargo-config-search-v3"
FILESYSTEM_ADMISSION_SCHEMA = "asterism-rebaseline-filesystem-admission-v3"
PROTOCOL_DOCUMENT_SHA256 = "d9ee10b2cccdaf6428bf1419a8c2ee74d272e987dc3617a80b64ad2e9d7a18dd"
HISTORICAL_BASELINE_SHA256 = "b2801a056a711de7a8643c15af2eb7d12a04b6315e0dec40a7beacc53c5bde40"
MEASUREMENT_FILESYSTEM = "ext4"
MIN_AVAILABLE_BYTES = 128 * 1024 * 1024 * 1024
MIN_AVAILABLE_INODES = 1_000_000
VARIANTS = ("A", "B", "C", "D")
REQUIRED_TOOLS = {
    "correctness",
    "evaluator_runtime",
    "fault",
    "perf",
    "strace",
    "strace_launcher_runtime",
    "runner_runtime",
    "terminal_verifier_runtime",
}
REQUIRED_TOOL_COMMS = {
    "correctness": "ast-rb-check",
    "evaluator_runtime": "asterism-eval",
    "fault": "ast-rb-fault",
    "perf": "perf",
    "runner_runtime": "asterism-run",
    "strace": "strace",
    "strace_launcher_runtime": "ast-trace-wait",
    "terminal_verifier_runtime": "asterism-term",
}
TRACKED_COMM_BASE = {
    "ar",
    "ast-rb-a",
    "ast-rb-b",
    "ast-rb-c",
    "ast-rb-d",
    "build-script-bu",
    "cargo",
    "cargo-nextest",
    "cc",
    "clang",
    "clang++",
    "clippy-driver",
    "collect2",
    "g++",
    "gcc",
    "ld",
    "ld.lld",
    "mold",
    "nextest",
    "ranlib",
    "rustc",
    "rustdoc",
    "rustfmt",
}
COMM_ALLOWLIST = sorted(TRACKED_COMM_BASE | set(REQUIRED_TOOL_COMMS.values()))
REQUIRED_SUPPORT_FILES = {
    "evaluator",
    "evidence_schema",
    "profile_adapter",
    "runner",
    "strace_attach",
    "terminal_verifier",
}
REQUIRED_SUPPORT_BASENAMES = {
    "evaluator": "evaluate.py",
    "evidence_schema": "evidence_schema.py",
    "profile_adapter": "profile_adapters.py",
    "runner": "run_rebaseline.py",
    "strace_attach": "strace_attach.py",
    "terminal_verifier": "verify_terminal.py",
}
SHA256 = 64
GIT_OBJECT = 40
PR_SET_CHILD_SUBREAPER = 36
PR_GET_CHILD_SUBREAPER = 37
ATTESTED_OUTPUT_LIMIT = 64 * 1024 * 1024
ATTESTED_CLEANUP_SECONDS = 2.0
ATTESTED_DRAIN_SECONDS = 2.0
C_ROLE_LIFETIME_CONTRACT = {
    "schema": "bn-2l3n-c-role-lifetime-v3",
    "blocking_thread_keep_alive_ns": 3_600_000_000_000,
    "maximum_profile_child_timeout_ns": 120_000_000_000,
    "ready_to_measured_spawn_blocking_sites": 1,
    "ready_to_measured_other_thread_birth_sites": 0,
}
NOT_APPLICABLE_ROLE_LIFETIME = "not_applicable"
TOOLCHAIN_FIELDS = {
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
    "rustup_home_path",
    "rustup_path",
    "rustup_sha256",
    "rustup_toolchain",
}
BUILD_ENV_BY_CONTRACT_FIELD = {
    "adapter_sha256": "ASTERISM_BUILD_ADAPTER_SHA256",
    "binary_kind": "ASTERISM_BUILD_BINARY_KIND",
    "build_nonce": "ASTERISM_BUILD_NONCE",
    "cargo_lock_sha256": "ASTERISM_BUILD_CARGO_LOCK_SHA256",
    "product_commit": "ASTERISM_BUILD_PRODUCT_COMMIT",
    "product_tree": "ASTERISM_BUILD_PRODUCT_TREE",
    "protocol": "ASTERISM_BUILD_PROTOCOL",
    "protocol_sha256": "ASTERISM_BUILD_PROTOCOL_SHA256",
    "shared_manifest_sha256": "ASTERISM_BUILD_SHARED_MANIFEST_SHA256",
    "source_approval_sha256": "ASTERISM_BUILD_SOURCE_APPROVAL_SHA256",
    "timed_surface": "ASTERISM_BUILD_TIMED_SURFACE",
    "tooling_commit": "ASTERISM_BUILD_TOOLING_COMMIT",
    "tooling_tree": "ASTERISM_BUILD_TOOLING_TREE",
    "variant": "ASTERISM_BUILD_VARIANT",
}
FROZEN_CARGO_ENV_FIELDS = {
    "CARGO_HOME",
    "CARGO_INCREMENTAL",
    "CARGO_NET_OFFLINE",
    "GIT_CONFIG_COUNT",
    "GIT_CONFIG_GLOBAL",
    "GIT_CONFIG_NOSYSTEM",
    "HOME",
    "LANG",
    "LC_ALL",
    "PATH",
    "RUSTC",
    "RUSTUP_HOME",
    "RUSTUP_TOOLCHAIN",
    "TZ",
}
FROZEN_RUNTIME_ENV_FIELDS = {"HOME", "LANG", "LC_ALL", "PATH", "TZ"}
HERE = Path(__file__).resolve().parent
PLAN_PATH = HERE / "source-plan.json"
SHARED_SOURCE = HERE / "overlay" / "shared"
PUBLIC_SOURCE = HERE / "overlay" / "public"
BARE_SOURCE = HERE / "overlay" / "bare"
PROTOCOL_DOCUMENT = HERE.parent / "BN-2L3N-PROTOCOL.md"
HISTORICAL_BASELINE = HERE.parents[1] / "baseline_matrix" / "BN-2SU-FINAL.csv"


class PreparationError(RuntimeError):
    pass


@dataclass(frozen=True)
class CanonicalSnapshot:
    """One immutable regular-file observation used for every later binding."""

    path: Path
    payload: bytes
    sha256: str
    value: dict[str, Any]


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode() + b"\n"


def validate_trace_path_marker_templates(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "root_environment",
        "log",
        "metadata",
    }:
        raise PreparationError("trace path marker template fields differ")
    if value["root_environment"] != "ASTERISM_REBASELINE_STORE":
        raise PreparationError("trace marker root environment differs")
    normalized: dict[str, list[dict[str, str]]] = {}
    for family in ("log", "metadata"):
        markers = value[family]
        if not isinstance(markers, list):
            raise PreparationError(f"trace {family} marker templates are not a list")
        normalized[family] = []
        for marker in markers:
            if not isinstance(marker, dict) or set(marker) != {"kind", "path"}:
                raise PreparationError(f"trace {family} marker template fields differ")
            kind = marker["kind"]
            path = marker["path"]
            if kind not in {"exact", "file_prefix", "directory_prefix"}:
                raise PreparationError(f"trace {family} marker kind differs")
            if not isinstance(path, str):
                raise PreparationError(f"trace {family} marker path is not text")
            candidate = path[:-1] if path.endswith("/") else path
            if (
                not candidate
                or path.startswith("/")
                or "//" in path
                or "\\" in path
                or "\x00" in path
                or any(part in {"", ".", ".."} for part in candidate.split("/"))
                or str(Path(candidate)) != candidate
                or (kind == "directory_prefix") != path.endswith("/")
            ):
                raise PreparationError(
                    f"trace {family} marker template is not canonical relative"
                )
            normalized[family].append({"kind": kind, "path": path})
    flat = [
        (family, marker)
        for family in ("log", "metadata")
        for marker in normalized[family]
    ]
    if not flat:
        raise PreparationError("trace marker template families are both empty")

    def overlaps(left: dict[str, str], right: dict[str, str]) -> bool:
        left_prefix = left["kind"] != "exact"
        right_prefix = right["kind"] != "exact"
        if not left_prefix and not right_prefix:
            return left["path"] == right["path"]
        if left_prefix and right_prefix:
            return left["path"].startswith(right["path"]) or right[
                "path"
            ].startswith(left["path"])
        prefix, exact = (left, right) if left_prefix else (right, left)
        return exact["path"].startswith(prefix["path"])

    for index, (left_family, left) in enumerate(flat):
        for right_family, right in flat[index + 1 :]:
            if overlaps(left, right):
                raise PreparationError(
                    "trace marker templates overlap: "
                    f"{left_family}:{left!r} {right_family}:{right!r}"
                )
    return {
        "root_environment": value["root_environment"],
        "log": normalized["log"],
        "metadata": normalized["metadata"],
    }


def trace_path_marker_templates(variant: str) -> dict[str, Any]:
    log = (
        [{"kind": "exact", "path": "segment-1.log"}]
        if variant == "B"
        else [{"kind": "file_prefix", "path": "log/seg-"}]
    )
    metadata = [] if variant == "B" else [
        {"kind": "directory_prefix", "path": "log/sealed/"},
        {"kind": "directory_prefix", "path": "snapshots/blobs/"},
        {"kind": "directory_prefix", "path": "snapshots/meta/"},
    ]
    if variant == "C":
        metadata.insert(
            0, {"kind": "directory_prefix", "path": "log/meta/"}
        )
    return validate_trace_path_marker_templates(
        {
            "root_environment": "ASTERISM_REBASELINE_STORE",
            "log": log,
            "metadata": metadata,
        }
    )


def atomic_write(path: Path, payload: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def atomic_json(path: Path, value: Any) -> None:
    atomic_write(path, canonical_json(value))


def hash_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_canonical_object(
    payload: bytes, schema: str, context: str
) -> dict[str, Any]:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreparationError(f"{context} is not valid JSON") from error
    if not isinstance(value, dict) or payload != canonical_json(value):
        raise PreparationError(f"{context} is not a canonical JSON object")
    if value.get("schema") != schema:
        raise PreparationError(f"{context} schema mismatch")
    return value


def immutable_canonical_snapshot(
    path: Path, schema: str, context: str
) -> CanonicalSnapshot:
    """Read an exact 0444 nonsymlink regular file once through one descriptor."""

    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise PreparationError(f"{context} cannot be opened without following links") from error
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o444
        ):
            raise PreparationError(
                f"{context} is not an exact 0444 regular file"
            )
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    stable_fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
        raise PreparationError(f"{context} changed while it was snapshotted")
    payload = b"".join(chunks)
    if len(payload) != after.st_size:
        raise PreparationError(f"{context} size changed while it was snapshotted")
    try:
        lexical = path.absolute()
        resolved = path.resolve(strict=True)
        current = path.lstat()
    except OSError as error:
        raise PreparationError(f"{context} path changed after snapshot") from error
    if (
        lexical != resolved
        or stat.S_ISLNK(current.st_mode)
        or (current.st_dev, current.st_ino) != (after.st_dev, after.st_ino)
    ):
        raise PreparationError(
            f"{context} path is aliased or changed after snapshot"
        )
    value = parse_canonical_object(payload, schema, context)
    return CanonicalSnapshot(
        path=lexical,
        payload=payload,
        sha256=hash_bytes(payload),
        value=value,
    )


def bootstrap_environment(cargo_home: Path, rustup_home: Path) -> dict[str, str]:
    return {
        "CARGO_HOME": str(cargo_home),
        "GIT_CONFIG_COUNT": "0",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
        "RUSTUP_HOME": str(rustup_home),
        "TZ": "UTC",
    }


def checked_tool(name: str) -> Path:
    if name not in {"bwrap", "git", "rustup"}:
        raise PreparationError(f"unapproved system tool: {name}")
    path = Path("/usr/bin") / name
    if not path.is_file():
        raise PreparationError(f"required tool is not a regular file: {path}")
    return path


def toolchain_identity() -> dict[str, str]:
    cargo_home = Path(
        os.environ.get("CARGO_HOME", str(Path.home() / ".cargo"))
    ).resolve(strict=True)
    rustup_home = Path(
        os.environ.get("RUSTUP_HOME", str(Path.home() / ".rustup"))
    ).resolve(strict=True)
    if not cargo_home.is_dir() or not rustup_home.is_dir():
        raise PreparationError("Cargo and rustup homes must be directories")
    rustup = checked_tool("rustup")
    bwrap = checked_tool("bwrap")
    git = checked_tool("git")
    bootstrap = bootstrap_environment(cargo_home, rustup_home)
    active = subprocess.run(
        [str(rustup), "show", "active-toolchain"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
        env=bootstrap,
    ).stdout.strip()
    toolchain = active.split(maxsplit=1)[0]
    if not toolchain or any(character.isspace() for character in toolchain):
        raise PreparationError("rustup active toolchain is invalid")

    def effective(component: str) -> Path:
        value = subprocess.run(
            [str(rustup), "which", component, "--toolchain", toolchain],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
            env=bootstrap,
        ).stdout.strip()
        path = Path(value).resolve(strict=True)
        if not path.is_file():
            raise PreparationError(f"rustup {component} target is not a file")
        return path

    cargo = effective("cargo")
    rustc = effective("rustc")
    version_env = {
        **bootstrap,
        "RUSTUP_TOOLCHAIN": toolchain,
    }
    cargo_version = subprocess.run(
        [str(cargo), "--version", "--verbose"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
        env=version_env,
    ).stdout.strip()
    rustc_version = subprocess.run(
        [str(rustc), "--version", "--verbose"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
        env=version_env,
    ).stdout.strip()
    host_lines = [
        line.removeprefix("host: ")
        for line in rustc_version.splitlines()
        if line.startswith("host: ")
    ]
    if len(host_lines) != 1 or not host_lines[0]:
        raise PreparationError("rustc verbose version has no unique host triple")
    return {
        "bwrap_path": str(bwrap),
        "bwrap_sha256": hash_file(bwrap),
        "cargo_home_path": str(cargo_home),
        "cargo_path": str(cargo),
        "cargo_sha256": hash_file(cargo),
        "cargo_version_verbose": cargo_version,
        "git_path": str(git),
        "git_sha256": hash_file(git),
        "rustc_path": str(rustc),
        "rustc_sha256": hash_file(rustc),
        "rustc_version_verbose": rustc_version,
        "rustc_host": host_lines[0],
        "rustup_home_path": str(rustup_home),
        "rustup_path": str(rustup),
        "rustup_sha256": hash_file(rustup),
        "rustup_toolchain": toolchain,
    }


def validate_toolchain(value: Any) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != TOOLCHAIN_FIELDS:
        raise PreparationError("toolchain identity fields differ")
    for field in (
        "bwrap_sha256", "cargo_sha256", "git_sha256", "rustc_sha256",
        "rustup_sha256",
    ):
        if not is_lower_hex(value.get(field), SHA256):
            raise PreparationError(f"toolchain executable hash is invalid: {field}")
    if value != toolchain_identity():
        raise PreparationError("toolchain identity changed")
    return value


def frozen_cargo_environment(
    toolchain: dict[str, str],
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    path = ":".join(dict.fromkeys((
        str(Path(toolchain["cargo_path"]).parent),
        str(Path(toolchain["rustc_path"]).parent),
        "/usr/bin",
        "/bin",
    )))
    environment = {
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
    if extra is not None:
        overlap = set(environment) & set(extra)
        if overlap:
            raise PreparationError(f"frozen environment override: {sorted(overlap)}")
        environment.update(extra)
    expected = FROZEN_CARGO_ENV_FIELDS | (set(extra) if extra is not None else set())
    if set(environment) != expected:
        raise PreparationError("frozen Cargo environment fields differ")
    return environment


def frozen_runtime_environment(extra: dict[str, str]) -> dict[str, str]:
    environment = {
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
        "TZ": "UTC",
    }
    overlap = set(environment) & set(extra)
    if overlap:
        raise PreparationError(f"frozen runtime environment override: {sorted(overlap)}")
    environment.update(extra)
    if set(environment) != FROZEN_RUNTIME_ENV_FIELDS | set(extra):
        raise PreparationError("frozen runtime environment fields differ")
    return environment


def filesystem_admission(path: Path) -> dict[str, Any]:
    path = path.resolve(strict=True)

    def unescape_mount(value: str) -> str:
        output = bytearray()
        index = 0
        payload = value.encode()
        while index < len(payload):
            if (
                payload[index] == ord("\\")
                and index + 3 < len(payload)
                and all(byte in b"01234567" for byte in payload[index + 1 : index + 4])
            ):
                output.append(int(payload[index + 1 : index + 4], 8))
                index += 4
            else:
                output.append(payload[index])
                index += 1
        return output.decode()

    matches: list[tuple[int, str]] = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields = line.split()
        try:
            separator = fields.index("-")
        except ValueError as error:
            raise PreparationError("invalid /proc/self/mountinfo") from error
        mount = Path(unescape_mount(fields[4]))
        if path == mount or mount in path.parents:
            matches.append((len(mount.parts), fields[separator + 1]))
    if not matches:
        raise PreparationError(f"no mountinfo entry covers {path}")
    filesystem = max(matches)[1]
    stats = os.statvfs(path)
    available_bytes = stats.f_bavail * stats.f_frsize
    available_inodes = stats.f_favail
    if filesystem != MEASUREMENT_FILESYSTEM:
        raise PreparationError(
            f"measurement filesystem {filesystem!r} != {MEASUREMENT_FILESYSTEM!r}"
        )
    if available_bytes < MIN_AVAILABLE_BYTES:
        raise PreparationError("measurement filesystem has less than 128 GiB free")
    if available_inodes < MIN_AVAILABLE_INODES:
        raise PreparationError("measurement filesystem has fewer than 1M free inodes")
    return {
        "available_bytes": available_bytes,
        "available_inodes": available_inodes,
        "checked_path": str(path),
        "filesystem": filesystem,
        "minimum_available_bytes": MIN_AVAILABLE_BYTES,
        "minimum_available_inodes": MIN_AVAILABLE_INODES,
        "schema": FILESYSTEM_ADMISSION_SCHEMA,
    }


def cargo_config_search(cwd: Path, cargo_home: Path) -> dict[str, Any]:
    cwd = cwd.resolve(strict=True)
    cargo_home = cargo_home.resolve(strict=True)
    directories = [cwd, *cwd.parents]
    candidates = [
        directory / ".cargo" / name
        for directory in directories
        for name in ("config.toml", "config")
    ]
    candidates.extend(cargo_home / name for name in ("config.toml", "config"))
    entries = []
    seen: set[Path] = set()
    for candidate in candidates:
        candidate = Path(os.path.abspath(candidate))
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.is_symlink():
            raise PreparationError(f"Cargo config may not be a symlink: {candidate}")
        if candidate.exists():
            if not candidate.is_file():
                raise PreparationError(f"Cargo config is not a file: {candidate}")
            entries.append({
                "path": str(candidate),
                "sha256": hash_file(candidate),
                "status": "present",
            })
        else:
            entries.append({
                "path": str(candidate),
                "sha256": None,
                "status": "absent",
            })
    return {
        "cargo_home_path": str(cargo_home),
        "cwd": str(cwd),
        "entries": entries,
        "schema": CARGO_CONFIG_SCHEMA,
    }


def write_cargo_config_search(
    path: Path, cwd: Path, toolchain: dict[str, str]
) -> dict[str, Any]:
    manifest = cargo_config_search(cwd, Path(toolchain["cargo_home_path"]))
    atomic_json(path, manifest)
    return {
        "path": str(path.resolve()),
        "sha256": hash_file(path),
    }


def replay_cargo_config_search(
    binding: dict[str, str], cwd: Path, toolchain: dict[str, str]
) -> None:
    if not isinstance(binding, dict) or set(binding) != {"path", "sha256"}:
        raise PreparationError("Cargo config-search binding fields differ")
    path = Path(binding["path"]).resolve(strict=True)
    if hash_file(path) != binding["sha256"]:
        raise PreparationError("Cargo config-search manifest hash changed")
    recorded = load_canonical(path, CARGO_CONFIG_SCHEMA)
    current = cargo_config_search(cwd, Path(toolchain["cargo_home_path"]))
    if recorded != current:
        raise PreparationError("Cargo config search changed")


def is_lower_hex(value: Any, length: int) -> bool:
    return isinstance(value, str) and len(value) == length and all(character in "0123456789abcdef" for character in value)


def load_canonical(path: Path, schema: str) -> dict[str, Any]:
    payload = path.read_bytes()
    return parse_canonical_object(payload, schema, str(path))


def validate_tools_manifest(path: Path) -> dict[str, Any]:
    metadata = path.lstat()
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o444
    ):
        raise PreparationError("tools manifest is not an immutable regular file")
    manifest = load_canonical(path, "asterism-rebaseline-tools-v3")
    if set(manifest) != {"comm_allowlist", "schema", "support_files", "tools"}:
        raise PreparationError("tools manifest top-level fields differ")
    if manifest.get("comm_allowlist") != COMM_ALLOWLIST:
        raise PreparationError("tools manifest comm allowlist differs")

    tools = manifest.get("tools")
    if not isinstance(tools, dict) or set(tools) != REQUIRED_TOOLS:
        raise PreparationError("tools manifest bindings are not the exact set")
    support = manifest.get("support_files")
    if not isinstance(support, dict) or set(support) != REQUIRED_SUPPORT_FILES:
        raise PreparationError("support manifest bindings are not the exact set")

    observed_paths: set[Path] = set()
    for name, binding in sorted(tools.items()):
        if not isinstance(binding, dict) or set(binding) != {
            "comm", "executable_mode", "path", "sha256"
        }:
            raise PreparationError(f"tool {name} binding fields differ")
        if binding.get("comm") != REQUIRED_TOOL_COMMS[name]:
            raise PreparationError(f"tool {name} comm differs")
        if binding.get("executable_mode") != 0o555:
            raise PreparationError(f"tool {name} claimed mode differs")
        source = validate_bound_source_file(
            binding.get("path"), binding.get("sha256"), 0o555,
            f"tool {name}",
        )
        if source in observed_paths:
            raise PreparationError(f"tool {name} reuses a bound source path")
        observed_paths.add(source)

    for name, binding in sorted(support.items()):
        if not isinstance(binding, dict) or set(binding) != {
            "mode", "path", "sha256"
        }:
            raise PreparationError(f"support file {name} binding fields differ")
        if binding.get("mode") != 0o444:
            raise PreparationError(f"support file {name} claimed mode differs")
        source = validate_bound_source_file(
            binding.get("path"), binding.get("sha256"), 0o444,
            f"support file {name}",
        )
        if source.name != REQUIRED_SUPPORT_BASENAMES[name]:
            raise PreparationError(f"support file {name} basename differs")
        if source in observed_paths:
            raise PreparationError(f"support file {name} reuses a bound source path")
        observed_paths.add(source)
    return manifest


def validate_bound_source_file(
    path_value: Any,
    claimed_sha256: Any,
    expected_mode: int,
    context: str,
) -> Path:
    if not isinstance(path_value, str) or not Path(path_value).is_absolute():
        raise PreparationError(f"{context} path is not absolute")
    lexical = Path(path_value)
    try:
        metadata = lexical.lstat()
        source = lexical.resolve(strict=True)
    except OSError as error:
        raise PreparationError(f"{context} source is unavailable") from error
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or str(source) != path_value
    ):
        raise PreparationError(f"{context} source is not an exact regular file")
    if stat.S_IMODE(metadata.st_mode) != expected_mode:
        raise PreparationError(f"{context} source mode differs")
    if not is_lower_hex(claimed_sha256, SHA256) or hash_file(source) != claimed_sha256:
        raise PreparationError(f"{context} source hash differs")
    return source


def run(
    argv: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    timeout: int = 3600,
) -> dict[str, Any]:
    completed = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, timeout=timeout)
    return {
        "argv": argv,
        "cwd": str(cwd.resolve()),
        "exit_status": completed.returncode,
        "stdout_sha256": hash_bytes(completed.stdout),
        "stderr_sha256": hash_bytes(completed.stderr),
        "stdout": completed.stdout.decode(errors="replace"),
        "stderr": completed.stderr.decode(errors="replace"),
    }


def proc_start_ticks(pid: int) -> int:
    payload = Path(f"/proc/{pid}/stat").read_text()
    close = payload.rfind(")")
    if close < 0:
        raise PreparationError(f"cannot parse /proc/{pid}/stat")
    fields = payload[close + 2 :].split()
    return int(fields[19])


def process_group_absent(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def child_subreaper_enabled() -> bool:
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl.argtypes = [
        ctypes.c_int,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    ]
    libc.prctl.restype = ctypes.c_int
    observed = ctypes.c_int()
    if libc.prctl(
        PR_GET_CHILD_SUBREAPER,
        ctypes.addressof(observed),
        0,
        0,
        0,
    ) != 0:
        error = ctypes.get_errno()
        raise PreparationError("cannot read child-subreaper state") from OSError(
            error, os.strerror(error)
        )
    return bool(observed.value)


def set_child_subreaper(enabled: bool) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl.argtypes = [
        ctypes.c_int,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    ]
    libc.prctl.restype = ctypes.c_int
    if libc.prctl(PR_SET_CHILD_SUBREAPER, int(enabled), 0, 0, 0) != 0:
        error = ctypes.get_errno()
        raise PreparationError("cannot set child-subreaper state") from OSError(
            error, os.strerror(error)
        )


def process_table() -> dict[int, tuple[int, int]]:
    """Return PID -> (PPID, start ticks), omitting identities that race away."""

    observed: dict[int, tuple[int, int]] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            payload = (entry / "stat").read_text()
            close = payload.rfind(")")
            if close < 0:
                continue
            fields = payload[close + 2 :].split()
            observed[int(entry.name)] = (int(fields[1]), int(fields[19]))
        except (OSError, ValueError, IndexError):
            continue
    return observed


def discover_attested_lineage(
    root_pid: int,
    root_start_ticks: int,
    known: dict[int, int],
    baseline_children: dict[int, int],
) -> dict[int, tuple[int, int]]:
    """Grow a lineage across reparenting and setsid using subreaper ownership."""

    table = process_table()
    if table.get(root_pid, (None, None))[1] == root_start_ticks:
        known[root_pid] = root_start_ticks
    parent_pid = os.getpid()
    changed = True
    while changed:
        changed = False
        for pid, (ppid, start_ticks) in table.items():
            if pid in known:
                continue
            baseline_start = baseline_children.get(pid)
            reparented_to_subreaper = (
                ppid == parent_pid and baseline_start != start_ticks
            )
            parent_is_lineage = (
                ppid in known
                and table.get(ppid, (None, None))[1] == known[ppid]
            )
            if reparented_to_subreaper or parent_is_lineage:
                known[pid] = start_ticks
                changed = True
    return table


def cleanup_attested_descendants(
    root_pid: int,
    root_start_ticks: int,
    known: dict[int, int],
    baseline_children: dict[int, int],
    *,
    deadline: float,
) -> tuple[bool, list[int]]:
    """Kill/reap every surviving descendant, including escaped process groups."""

    detected = False
    remaining: list[int] = []
    while True:
        table = discover_attested_lineage(
            root_pid, root_start_ticks, known, baseline_children
        )
        live = [
            pid
            for pid, start_ticks in known.items()
            if pid != root_pid and table.get(pid, (None, None))[1] == start_ticks
        ]
        if live:
            detected = True
        for pid in live:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        for pid in list(known):
            if pid == root_pid:
                continue
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass
        if not live:
            remaining = []
            break
        if time.monotonic() >= deadline:
            remaining = live
            break
        time.sleep(0.01)
    return detected, remaining


def run_attested(
    argv: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    output_path: Path,
    raw_stdout: bool,
    timeout: int = 3600,
    max_output_bytes: int = ATTESTED_OUTPUT_LIMIT,
) -> tuple[dict[str, Any], bytes, bytes]:
    if max_output_bytes <= 0:
        raise PreparationError("attested output limit must be positive")
    if timeout <= 0:
        raise PreparationError("attested wall timeout must be positive")
    prior_subreaper = child_subreaper_enabled()
    if not prior_subreaper:
        set_child_subreaper(True)
    parent_pid = os.getpid()
    baseline_table = process_table()
    baseline_children = {
        pid: start_ticks
        for pid, (ppid, start_ticks) in baseline_table.items()
        if ppid == parent_pid
    }
    stdout_read, stdout_write = os.pipe()
    stderr_read, stderr_write = os.pipe()
    gate_read, gate_write = os.pipe()
    started_at = datetime.now(timezone.utc).isoformat()
    started_monotonic_ns = time.monotonic_ns()
    pid = -1
    start_ticks = -1
    status: int | None = None
    known: dict[int, int] = {}
    selector = selectors.DefaultSelector()
    open_descriptors = {
        stdout_read,
        stdout_write,
        stderr_read,
        stderr_write,
        gate_read,
        gate_write,
    }
    try:
        pid = os.fork()
        if pid == 0:
            try:
                os.close(stdout_read)
                os.close(stderr_read)
                os.close(gate_write)
                os.setsid()
                os.dup2(stdout_write, 1)
                os.dup2(stderr_write, 2)
                for descriptor in (stdout_write, stderr_write):
                    if descriptor > 2:
                        os.close(descriptor)
                if os.read(gate_read, 1) != b"x":
                    os._exit(126)
                os.close(gate_read)
                os.chdir(cwd)
                os.execvpe(argv[0], argv, env)
            except BaseException as error:
                os.write(2, f"prepare-overlays exec failed: {error}\n".encode())
                os._exit(127)

        for descriptor in (stdout_write, stderr_write, gate_read):
            os.close(descriptor)
            open_descriptors.discard(descriptor)
        start_ticks = proc_start_ticks(pid)
        known[pid] = start_ticks
        buffers = {"stdout": bytearray(), "stderr": bytearray()}
        output_bytes = 0
        output_overflow = False
        deadline = time.monotonic() + timeout
        for name, descriptor in (("stdout", stdout_read), ("stderr", stderr_read)):
            os.set_blocking(descriptor, False)
            selector.register(descriptor, selectors.EVENT_READ, name)

        def drain_ready(
            wait_seconds: float,
            *,
            stop_deadline: float,
            stop_on_output_limit: bool,
        ) -> None:
            """Drain ready pipes without letting a readable fd monopolize control."""

            nonlocal output_bytes, output_overflow
            remaining = stop_deadline - time.monotonic()
            if remaining <= 0 or (stop_on_output_limit and output_overflow):
                return
            for key, _mask in selector.select(min(wait_seconds, remaining)):
                descriptor = int(key.fd)
                while True:
                    if time.monotonic() >= stop_deadline or (
                        stop_on_output_limit and output_overflow
                    ):
                        return
                    try:
                        chunk = os.read(descriptor, 64 * 1024)
                    except BlockingIOError:
                        break
                    if not chunk:
                        selector.unregister(descriptor)
                        os.close(descriptor)
                        open_descriptors.discard(descriptor)
                        break
                    output_bytes += len(chunk)
                    if output_bytes > max_output_bytes:
                        output_overflow = True
                        if stop_on_output_limit:
                            return
                    elif not output_overflow:
                        buffers[str(key.data)].extend(chunk)

        os.write(gate_write, b"x")
        os.close(gate_write)
        open_descriptors.discard(gate_write)
        timed_out = False
        while status is None:
            drain_ready(
                0.01,
                stop_deadline=deadline,
                stop_on_output_limit=True,
            )
            waited, observed = os.waitpid(pid, os.WNOHANG)
            if waited == pid:
                status = observed
                break
            if output_overflow or time.monotonic() >= deadline:
                timed_out = not output_overflow
                try:
                    os.killpg(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                _, status = os.waitpid(pid, 0)
                break

        escaped, remaining = cleanup_attested_descendants(
            pid,
            start_ticks,
            known,
            baseline_children,
            deadline=time.monotonic() + ATTESTED_CLEANUP_SECONDS,
        )
        drain_deadline = time.monotonic() + ATTESTED_DRAIN_SECONDS
        while selector.get_map() and time.monotonic() < drain_deadline:
            drain_ready(
                min(0.05, drain_deadline - time.monotonic()),
                stop_deadline=drain_deadline,
                stop_on_output_limit=False,
            )
        pipes_stalled = bool(selector.get_map())
        completed_monotonic_ns = time.monotonic_ns()
        completed_at = datetime.now(timezone.utc).isoformat()
        group_absent = process_group_absent(pid)
        if not group_absent:
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if remaining:
            raise PreparationError(
                f"attested child descendants survived cleanup: {remaining}"
            )
        if escaped:
            raise PreparationError("attested child left an escaped descendant")
        if pipes_stalled:
            raise PreparationError("attested child output pipes did not close")
        if output_overflow:
            raise PreparationError("attested child exceeded the output byte limit")
        if not group_absent:
            raise PreparationError("attested child left an orphan process group")
        stdout = bytes(buffers["stdout"])
        stderr = bytes(buffers["stderr"])
        exit_status = os.waitstatus_to_exitcode(status)
        payload = (
            stdout
            if raw_stdout
            else canonical_json(
                {
                    "exit_status": exit_status,
                    "stderr": stderr.decode(errors="replace"),
                    "stderr_sha256": hash_bytes(stderr),
                    "stdout": stdout.decode(errors="replace"),
                    "stdout_sha256": hash_bytes(stdout),
                }
            )
        )
        atomic_write(output_path, payload, mode=0o444)
        reaped = not Path(f"/proc/{pid}").exists()
        child = {
            "argv": argv,
            "completed_at": completed_at,
            "completed_monotonic_ns": completed_monotonic_ns,
            "cwd": str(cwd.resolve()),
            "exit_status": exit_status,
            "output_path": str(output_path.resolve()),
            "output_sha256": hash_file(output_path),
            "pid": pid,
            "process_group_absent": group_absent,
            "reaping": {
                "pid": pid,
                "start_ticks": start_ticks,
                "status": "absent" if reaped else "present",
            },
            "start_ticks": start_ticks,
            "started_at": started_at,
            "started_monotonic_ns": started_monotonic_ns,
            "timed_out": timed_out,
            "waited_pid": pid,
        }
        return child, stdout, stderr
    finally:
        if pid > 0 and status is None:
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass
        if pid > 0 and start_ticks > 0:
            cleanup_attested_descendants(
                pid,
                start_ticks,
                known,
                baseline_children,
                deadline=time.monotonic() + ATTESTED_CLEANUP_SECONDS,
            )
        for descriptor in list(open_descriptors):
            try:
                selector.unregister(descriptor)
            except (KeyError, ValueError):
                pass
            try:
                os.close(descriptor)
            except OSError:
                pass
        selector.close()
        if not prior_subreaper:
            set_child_subreaper(False)


def validate_attested_child(child: dict[str, Any], context: str) -> None:
    pid = child.get("pid")
    start_ticks = child.get("start_ticks")
    if (
        not isinstance(pid, int)
        or pid <= 0
        or not isinstance(start_ticks, int)
        or start_ticks <= 0
        or child.get("waited_pid") != pid
        or child.get("timed_out") is not False
        or child.get("process_group_absent") is not True
        or child.get("exit_status") != 0
        or child.get("reaping") != {
            "pid": pid,
            "start_ticks": start_ticks,
            "status": "absent",
        }
    ):
        raise PreparationError(
            f"{context} child was not successful, explicitly reaped, and orphan-free"
        )


def git_bytes(
    repository: Path,
    arguments: list[str],
    toolchain: dict[str, str],
) -> bytes:
    completed = subprocess.run(
        [toolchain["git_path"], "-C", str(repository), *arguments],
        check=True,
        capture_output=True,
        timeout=120,
        env=frozen_cargo_environment(toolchain),
    )
    return completed.stdout


def git_text(
    repository: Path,
    arguments: list[str],
    toolchain: dict[str, str],
) -> str:
    return git_bytes(repository, arguments, toolchain).decode().strip()


def rust_item(source: str, marker: str, context: str) -> str:
    """Extract one brace-delimited Rust item for a fail-closed static proof."""

    if source.count(marker) != 1:
        raise PreparationError(f"{context} marker cardinality differs")
    start = source.index(marker)
    opening = source.find("{", start + len(marker))
    if opening < 0:
        raise PreparationError(f"{context} has no body")
    depth = 0
    offset = opening
    while offset < len(source):
        character = source[offset]
        if source.startswith("//", offset):
            newline = source.find("\n", offset + 2)
            offset = len(source) if newline < 0 else newline + 1
            continue
        if source.startswith("/*", offset):
            comment_depth = 1
            offset += 2
            while offset < len(source) and comment_depth:
                if source.startswith("/*", offset):
                    comment_depth += 1
                    offset += 2
                elif source.startswith("*/", offset):
                    comment_depth -= 1
                    offset += 2
                else:
                    offset += 1
            if comment_depth:
                raise PreparationError(f"{context} has an unterminated comment")
            continue
        if character == "r":
            hashes = 0
            raw_quote = offset + 1
            while raw_quote < len(source) and source[raw_quote] == "#":
                hashes += 1
                raw_quote += 1
            if raw_quote < len(source) and source[raw_quote] == '"':
                closing = '"' + "#" * hashes
                end = source.find(closing, raw_quote + 1)
                if end < 0:
                    raise PreparationError(f"{context} has an unterminated raw string")
                offset = end + len(closing)
                continue
        if character == '"':
            offset += 1
            while offset < len(source):
                if source[offset] == "\\":
                    offset += 2
                elif source[offset] == '"':
                    offset += 1
                    break
                else:
                    offset += 1
            else:
                raise PreparationError(f"{context} has an unterminated string")
            continue
        if character == "'":
            lifetime = (
                offset + 1 < len(source)
                and (source[offset + 1].isalnum() or source[offset + 1] == "_")
                and (offset + 2 >= len(source) or source[offset + 2] != "'")
            )
            if not lifetime:
                offset += 1
                while offset < len(source):
                    if source[offset] == "\\":
                        offset += 2
                    elif source[offset] == "'":
                        offset += 1
                        break
                    else:
                        offset += 1
                else:
                    raise PreparationError(
                        f"{context} has an unterminated character literal"
                    )
                continue
        if character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                return source[start : offset + 1]
            if depth < 0:
                break
        offset += 1
    raise PreparationError(f"{context} body is not balanced")


def require_exact_fragment(source: str, fragment: str, context: str) -> None:
    """Require one exact source expression in an attested Rust item."""

    if source.count(fragment) != 1:
        raise PreparationError(f"{context} exact expression differs")


def require_exact_signature(item: str, signature: str, context: str) -> None:
    """Require an extracted Rust item to start with one exact signature."""

    if not item.startswith(signature):
        raise PreparationError(f"{context} exact typed signature differs")


def replace_exact_once(
    source: str, old: str, new: str, context: str
) -> str:
    """Construct one hostile source only when its target is unambiguous."""

    if source.count(old) != 1:
        raise AssertionError(f"{context} hostile target cardinality differs")
    return source.replace(old, new, 1)


def validate_shared_control_child_source(control: str) -> None:
    """Bind the shared ordinary-child wire phases to runner v3 fields."""

    boot = rust_item(control, "pub fn boot(", "shared control boot")
    require_exact_signature(
        boot,
        "pub fn boot(&mut self) -> Nonce {",
        "shared control boot",
    )
    for marker in (
        '("context_sha256", json_string(&self.context_sha256))',
        '("phase", json_string("boot"))',
        '("protocol_sha256", json_string(crate::contract::PROTOCOL_SHA256))',
        '("variant", json_string(crate::contract::VARIANT))',
        'continue_command(&self.receive(), "boot")',
    ):
        if boot.count(marker) != 1:
            raise PreparationError(f"shared boot field differs: {marker}")

    for method, phase in (("runtime", "runtime"), ("opened", "opened")):
        item = rust_item(
            control, f"pub fn {method}(", f"shared control {method}"
        )
        require_exact_signature(
            item,
            f"pub fn {method}(&mut self, nonce: &Nonce) -> Nonce {{",
            f"shared control {method}",
        )
        for marker in (
            '("nonce", json_string(nonce.as_str()))',
            f'("phase", json_string("{phase}"))',
            f'continue_command(&self.receive(), "{phase}")',
        ):
            if item.count(marker) != 1:
                raise PreparationError(
                    f"shared {phase} field differs: {marker}"
                )

    opened_after_start = rust_item(
        control,
        "pub fn opened_after_start(",
        "shared control opened-after-start",
    )
    require_exact_signature(
        opened_after_start,
        """pub fn opened_after_start(
        &mut self,
        nonce: &Nonce,
        open_start_monotonic_ns: u64,
        opened_monotonic_ns: u64,
    ) -> Nonce {""",
        "shared control opened-after-start",
    )

    ready = rust_item(
        control,
        "pub fn ready_and_wait_start(",
        "shared control ready/start",
    )
    require_exact_signature(
        ready,
        """pub fn ready_and_wait_start(
        &mut self,
        nonce: &Nonce,
        allocation_calls_start: u64,
        allocated_bytes_start: u64,
        process_user_cpu_start_ns: u64,
        process_system_cpu_start_ns: u64,
        ready_monotonic_ns: u64,
        counter_start_monotonic_ns: u64,
    ) -> Nonce {""",
        "shared control ready/start",
    )
    ready_fields = (
        '\\"allocated_bytes_start\\":{}',
        '\\"allocation_calls_start\\":{}',
        '\\"context_sha256\\":\\"{}\\"',
        '\\"counter_start_monotonic_ns\\":{}',
        '\\"nonce\\":\\"{}\\"',
        '\\"phase\\":\\"ready\\"',
        '\\"process_system_cpu_start_ns\\":{}',
        'process_user_cpu_start_ns\\":{}',
        '\\"protocol_sha256\\":\\"{}\\"',
        '\\"ready_monotonic_ns\\":{}',
        '\\"variant\\":\\"{}\\"',
    )
    for field in ready_fields:
        if ready.count(field) != 1:
            raise PreparationError(f"shared ready field differs: {field}")
    if [ready.index(field) for field in ready_fields] != sorted(
        ready.index(field) for field in ready_fields
    ):
        raise PreparationError("shared ready fields are reordered")
    ready_arguments = """            allocated_bytes_start,
            allocation_calls_start,
            self.context_sha256,
            counter_start_monotonic_ns,
            nonce.as_str(),
            process_system_cpu_start_ns,
            process_user_cpu_start_ns,
            crate::contract::PROTOCOL_SHA256,
            ready_monotonic_ns,
            crate::contract::VARIANT,"""
    require_exact_fragment(
        ready,
        ready_arguments,
        "shared ready format argument tuple",
    )
    if ready.count('command(&self.receive(), "start")') != 1:
        raise PreparationError("shared ready/start command differs")

    measured = rust_item(
        control,
        "pub fn measured_and_wait_release(",
        "shared control measured/release",
    )
    require_exact_signature(
        measured,
        """pub fn measured_and_wait_release(
        &mut self,
        nonce: &Nonce,
        markers: MeasuredMarkers,
    ) {""",
        "shared control measured/release",
    )
    measured_fields = """        let mut fields = vec![
            ("allocated_bytes_end", json_u64(markers.allocated_bytes_end)),
            ("allocation_calls_end", json_u64(markers.allocation_calls_end)),
            (
                "counter_end_monotonic_ns",
                json_u64(markers.counter_end_monotonic_ns),
            ),
            (
                "last_completion_monotonic_ns",
                json_u64(markers.last_completion_monotonic_ns),
            ),
            ("nonce", json_string(nonce.as_str())),
            ("phase", json_string("measured")),
            (
                "process_system_cpu_end_ns",
                json_u64(markers.process_system_cpu_end_ns),
            ),
            (
                "process_user_cpu_end_ns",
                json_u64(markers.process_user_cpu_end_ns),
            ),
            ("release_monotonic_ns", json_u64(markers.release_monotonic_ns)),
            ("t0_monotonic_ns", json_u64(markers.t0_monotonic_ns)),
            ("t1_monotonic_ns", json_u64(markers.t1_monotonic_ns)),
        ];"""
    require_exact_fragment(
        measured,
        measured_fields,
        "shared measured field/expression tuple",
    )
    for marker in (
        'command(&self.receive(), "release")',
        'assert_eq!(released, *nonce, "release nonce mismatch")',
    ):
        if measured.count(marker) != 1:
            raise PreparationError(f"shared measured release differs: {marker}")


def validate_correctness_oracle_control_source(public: str) -> None:
    """Prove the historical oracle uses the shared ordinary-child protocol."""

    oracle = rust_item(
        public,
        "fn run_common_public_oracle(",
        "public correctness oracle",
    )
    require_exact_signature(
        oracle,
        "fn run_common_public_oracle(root: PathBuf) {",
        "public correctness oracle",
    )
    phase_markers = (
        "let mut control = Control::connect();",
        "let boot_nonce = control.boot();",
        "let runtime = tokio::runtime::Builder::new_current_thread()",
        "let runtime_nonce = control.runtime(&boot_nonce);",
        "let engine = LogEngine::open_with(",
        "let opened_nonce = control.opened(&runtime_nonce);",
        "let ready_monotonic_ns = monotonic_ns();",
        "let alloc_before = allocation::snapshot();",
        "let cpu_before = cpu_snapshot();",
        "let counter_start_monotonic_ns = monotonic_ns();",
        "let start_nonce = control.ready_and_wait_start(",
        "let t0_monotonic_ns = monotonic_ns();",
        "let release_monotonic_ns = monotonic_ns();",
        "let last_completion_monotonic_ns = monotonic_ns();",
        "let t1_monotonic_ns = monotonic_ns();",
        "let alloc_after = allocation::snapshot();",
        "let cpu_after = cpu_snapshot();",
        "let counter_end_monotonic_ns = monotonic_ns();",
        "control.measured_and_wait_release(",
    )
    for marker in phase_markers:
        if oracle.count(marker) != 1:
            raise PreparationError(
                f"correctness oracle control marker differs: {marker}"
            )
    offsets = [oracle.index(marker) for marker in phase_markers]
    if offsets != sorted(offsets):
        raise PreparationError("correctness oracle control phases are reordered")
    ready_call = """    let start_nonce = control.ready_and_wait_start(
        &opened_nonce,
        alloc_before.calls,
        alloc_before.bytes,
        cpu_before.user_ns,
        cpu_before.system_ns,
        ready_monotonic_ns,
        counter_start_monotonic_ns,
    );"""
    require_exact_fragment(
        oracle,
        ready_call,
        "correctness oracle ready counter/CPU/timestamp tuple",
    )
    measured_call = """    control.measured_and_wait_release(
        &start_nonce,
        MeasuredMarkers {
            allocation_calls_end: alloc_after.calls,
            allocated_bytes_end: alloc_after.bytes,
            counter_end_monotonic_ns,
            last_completion_monotonic_ns,
            release_monotonic_ns,
            process_system_cpu_end_ns: cpu_after.system_ns,
            process_user_cpu_end_ns: cpu_after.user_ns,
            t0_monotonic_ns,
            t1_monotonic_ns,
        },
    );"""
    require_exact_fragment(
        oracle,
        measured_call,
        "correctness oracle measured counter/CPU/timestamp tuple",
    )
    if ".send(" in oracle or ".receive(" in oracle:
        raise PreparationError(
            "correctness oracle bypasses the shared control API"
        )

    emit = rust_item(
        public,
        "fn emit_correctness_oracle(",
        "public correctness oracle emitter",
    )
    require_exact_signature(
        emit,
        "fn emit_correctness_oracle(args: CorrectnessOracleArgs) {",
        "public correctness oracle emitter",
    )
    if emit.count("run_common_public_oracle(") != 1:
        raise PreparationError("correctness oracle emitter routing differs")
    main = rust_item(public, "fn main()", "public overlay main")
    arm_start = main.find('"correctness_oracle" => {')
    arm_end = main.find('"self-test" => {', arm_start + 1)
    if arm_start < 0 or arm_end < 0:
        raise PreparationError("correctness oracle mode arm is absent")
    arm = main[arm_start:arm_end]
    routing = (
        '"correctness_oracle" => {',
        "contract::validate(BINARY_KIND, TIMED_SURFACE);",
        "emit_correctness_oracle(correctness_oracle_args());",
    )
    if any(arm.count(marker) != 1 for marker in routing):
        raise PreparationError("correctness oracle mode routing differs")
    if [arm.index(marker) for marker in routing] != sorted(
        arm.index(marker) for marker in routing
    ):
        raise PreparationError("correctness oracle mode routing is reordered")


def validate_reopen_digest_sources(public: str, digest: str) -> None:
    """Bind reopen evidence to one 64-lowercase-hex logical spelling."""

    formatter = rust_item(
        digest, "pub fn canonical_hex(", "shared logical digest formatter"
    )
    if formatter.count('format!("{:064x}", self.0)') != 1:
        raise PreparationError("shared logical digest spelling differs")
    verification = rust_item(
        public, "fn verify_corpus(", "public corpus verification"
    )
    for marker in (
        "logical_digest: logical_digest.canonical_hex(),",
        "registry_head_digest: registry_head_digest.canonical_hex(),",
    ):
        if verification.count(marker) != 1:
            raise PreparationError(f"corpus digest marker differs: {marker}")
    seed = rust_item(public, "fn run_reopen_seed(", "public reopen seed")
    reopen = rust_item(public, "fn run_reopen(", "public reopen")
    for marker in (
        '("logical_digest", json_string(&verified.logical_digest))',
        '("registry_head_digest", json_string(&verified.registry_head_digest))',
    ):
        if seed.count(marker) != 1:
            raise PreparationError(f"reopen seed digest marker differs: {marker}")
    for marker in (
        "verified.logical_digest, logical_digest,",
        "verified.registry_head_digest, registry_head_digest,",
    ):
        if reopen.count(marker) != 1:
            raise PreparationError(f"reopen digest comparison differs: {marker}")
    if ':016x' in verification or ':016x' in seed or ':016x' in reopen:
        raise PreparationError("legacy 16-hex reopen digest spelling remains")


def validate_reopen_seed_accounting_sources(
    public: str,
    current_adapter: str,
    borrowed_adapter: str,
) -> None:
    """Bind full/smoke inputs and A/C/D log-event accounting exactly."""

    verification = rust_item(
        public,
        "fn verify_corpus(",
        "public corpus verification",
    )
    require_exact_signature(
        verification,
        """fn verify_corpus(
    runtime: &tokio::runtime::Runtime,
    store: &EventStore<FjallSnapshotBackend<LogEngine>>,
    streams: usize,
    events_per_stream: u64,
) -> CorpusVerification {""",
        "public corpus verification",
    )

    seed = rust_item(public, "fn run_reopen_seed(", "public reopen seed")
    require_exact_signature(
        seed,
        """fn run_reopen_seed(
    root: PathBuf,
    streams: usize,
    batches_per_stream: usize,
    batch: usize,
    workers: usize,
    output_schema: &str,
) {""",
        "public reopen seed",
    )
    require_exact_fragment(
        seed,
        """    adapter::assert_reopen_seed_accounting(
        &engine,
        verified.domain_events,
        streams as u64,
    );""",
        "public reopen seed accounting call",
    )

    main = rust_item(public, "fn main()", "public overlay main")
    require_exact_fragment(
        main,
        'run_reopen_seed(root, 1_000, 200, 10, 8, "bn-2l3n-reopen-seed-v3");',
        "public full reopen seed route",
    )
    require_exact_fragment(
        main,
        """            run_reopen_seed(
                root,
                1,
                1,
                1,
                1,
                "bn-2l3n-overlay-smoke-reopen-seed-v3",
            );""",
        "public smoke reopen seed route",
    )

    accounting_signature = """pub fn assert_oracle_accounting(
    engine: &LogEngine,
    domain_events: u64,
    public_appends: u64,
    fresh_streams: u64,
    group: bool,
) {"""
    for generation, adapter in (
        ("current", current_adapter),
        ("historical", borrowed_adapter),
    ):
        accounting = rust_item(
            adapter,
            "pub fn assert_oracle_accounting(",
            f"{generation} oracle accounting",
        )
        require_exact_signature(
            accounting,
            accounting_signature,
            f"{generation} oracle accounting",
        )

    current = rust_item(
        current_adapter,
        "pub fn assert_reopen_seed_accounting(",
        "current reopen seed accounting",
    )
    seed_accounting_signature = """pub fn assert_reopen_seed_accounting(
    engine: &LogEngine,
    domain_events: u64,
    fresh_streams: u64,
) {"""
    require_exact_signature(
        current,
        seed_accounting_signature,
        "current reopen seed accounting",
    )
    require_exact_fragment(
        current,
        """    let high_water = engine.total_events() as u64;
    assert_eq!(engine.metrics().total_events, high_water);
    assert_eq!(
        high_water,
        domain_events + fresh_streams + 1,
        "v3 reopen seed differs from domain + streams + one shared type",
    );""",
        "A reopen seed accounting formula",
    )

    borrowed = rust_item(
        borrowed_adapter,
        "pub fn assert_reopen_seed_accounting(",
        "historical reopen seed accounting",
    )
    require_exact_signature(
        borrowed,
        seed_accounting_signature,
        "historical reopen seed accounting",
    )
    require_exact_fragment(
        borrowed,
        """    let high_water = engine.total_events() as u64;
    assert_eq!(engine.metrics().total_events, high_water);
    match contract::VARIANT {
        "C" => assert_eq!(
            high_water, domain_events,
            "C reopen seed unexpectedly consumed metadata positions",
        ),
        "D" => assert_eq!(
            high_water,
            domain_events + fresh_streams + 1,
            "D reopen seed differs from domain + streams + one shared type",
        ),
        variant => panic!("borrowed adapter used by variant {variant}"),
    }""",
        "C/D reopen seed accounting formulas",
    )


def validate_c_role_lifetime_sources(public: str, engine: str) -> None:
    run_point = rust_item(public, "fn run_point(", "C overlay run_point")
    append_batch = rust_item(
        engine, "async fn append_batch(", "C product append_batch"
    )
    runtime_builder = "tokio::runtime::Builder::new_multi_thread()"
    keep_alive = ".thread_keep_alive(Duration::from_secs(3_600))"
    if (
        public.count(runtime_builder) != 3
        or public.count(keep_alive) != 3
        or run_point.count(runtime_builder) != 1
        or run_point.count(keep_alive) != 1
    ):
        raise PreparationError("C measured Tokio lifetime proof differs")
    blocking_site = "let appended = tokio::task::spawn_blocking("
    if append_batch.count(blocking_site) != 1:
        raise PreparationError("C measured spawn_blocking site count differs")
    forbidden_thread_birth = (
        "std::thread::spawn(",
        "std::thread::Builder::new(",
        "std::thread::scope(",
        "thread::spawn(",
        "thread::Builder::new(",
        "thread::scope(",
        "rayon::spawn(",
    )
    if any(marker in run_point for marker in forbidden_thread_birth) or any(
        marker in append_batch for marker in forbidden_thread_birth
    ):
        raise PreparationError("C measured path has another thread-birth site")
    if append_batch.count("tokio::task::spawn_blocking(") != 1:
        raise PreparationError("C measured path has another blocking birth site")


def profile_role_lifetime(
    repository: Path,
    plan: dict[str, Any],
    toolchain: dict[str, str],
    variant: str,
) -> object:
    if variant != "C":
        return NOT_APPLICABLE_ROLE_LIFETIME
    public = (PUBLIC_SOURCE / "main.rs").read_text()
    claim = plan["variants"]["C"]
    engine = git_bytes(
        repository,
        [
            "show",
            f"{claim['product_commit']}:crates/mess-store/src/engine.rs",
        ],
        toolchain,
    ).decode()
    validate_c_role_lifetime_sources(public, engine)
    return dict(C_ROLE_LIFETIME_CONTRACT)


def load_plan(repository: Path, toolchain: dict[str, str]) -> dict[str, Any]:
    plan = load_canonical(PLAN_PATH, PLAN_SCHEMA)
    if (
        plan.get("protocol") != PROTOCOL
        or plan.get("protocol_sha256") != PROTOCOL_DOCUMENT_SHA256
        or hash_file(PROTOCOL_DOCUMENT) != PROTOCOL_DOCUMENT_SHA256
    ):
        raise PreparationError("source plan protocol identity is invalid")
    if set(plan.get("variants", {})) != set(VARIANTS):
        raise PreparationError("source plan variants differ")
    for variant in VARIANTS:
        claim = plan["variants"][variant]
        commit = claim.get("product_commit")
        tree = claim.get("product_tree")
        if not is_lower_hex(commit, GIT_OBJECT) or not is_lower_hex(tree, GIT_OBJECT):
            raise PreparationError(f"{variant} product identity is invalid")
        if git_text(
            repository, ["rev-parse", f"{commit}^{{tree}}"], toolchain
        ) != tree:
            raise PreparationError(f"{variant} product tree mismatch")
        lock = claim.get("lock", {})
        payload = git_bytes(
            repository,
            ["show", f"{lock.get('commit')}:{lock.get('path')}"],
            toolchain,
        )
        if hash_bytes(payload) != lock.get("sha256"):
            raise PreparationError(f"{variant} historical lock input mismatch")
    return plan


def file_manifest(root: Path) -> dict[str, Any]:
    entries = []
    for path in sorted(root.rglob("*")):
        if path.is_dir():
            continue
        if not path.is_file() or path.is_symlink():
            raise PreparationError(f"unsupported manifest path {path}")
        observed_mode = stat.S_IMODE(path.stat().st_mode)
        entries.append(
            {
                "bytes": path.stat().st_size,
                "mode": f"{observed_mode:04o}",
                "path": path.relative_to(root).as_posix(),
                "sha256": hash_file(path),
            }
        )
    return {
        "entries": entries,
        "protocol": PROTOCOL,
        "root": str(root.resolve()),
        "schema": "bn-2l3n-file-manifest-v3",
    }


def canonical_archive_member(
    member: tarfile.TarInfo,
) -> tuple[str, tuple[str, ...]]:
    """Return one unambiguous POSIX member key and its exact components."""

    name = member.name
    path_name = name[:-1] if member.isdir() and name.endswith("/") else name
    parts = tuple(path_name.split("/"))
    if (
        not path_name
        or name.startswith("/")
        or any(part in {"", ".", ".."} for part in parts)
        or "/".join(parts) != path_name
    ):
        raise PreparationError(f"unsafe archive member {name!r}")
    return "/".join(parts), parts


def extract_archive_payload(archive: bytes, destination: Path) -> list[str]:
    """Extract regular Git-archive entries with no links or path aliases."""

    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as source:
        members = source.getmembers()
        validated: list[tuple[tarfile.TarInfo, tuple[str, ...]]] = []
        observed: dict[str, str] = {}
        kinds: dict[str, str] = {}
        for member in members:
            canonical, parts = canonical_archive_member(member)
            if canonical in observed:
                raise PreparationError(
                    "duplicate normalized archive members "
                    f"{observed[canonical]!r} and {member.name!r}"
                )
            if not member.isdir() and not member.isfile():
                raise PreparationError(f"unsupported archive member {member.name!r}")
            observed[canonical] = member.name
            kinds[canonical] = "directory" if member.isdir() else "file"
            validated.append((member, parts))
        for canonical in kinds:
            parts = canonical.split("/")
            if any(
                kinds.get("/".join(parts[:index])) == "file"
                for index in range(1, len(parts))
            ):
                raise PreparationError(
                    f"archive member {observed[canonical]!r} descends from a file"
                )

        destination.mkdir(parents=True)
        files: list[str] = []
        for member, parts in validated:
            target = destination.joinpath(*parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            extracted = source.extractfile(member)
            if extracted is None:
                raise PreparationError(f"cannot extract archive member {member.name!r}")
            target.parent.mkdir(parents=True, exist_ok=True)
            atomic_write(
                target,
                extracted.read(),
                mode=0o555 if member.mode & 0o111 else 0o444,
            )
            files.append(member.name)
    return files


def extract_archive(
    repository: Path,
    commit: str,
    destination: Path,
    archive_path: Path,
    manifest_path: Path,
    toolchain: dict[str, str],
) -> dict[str, Any]:
    archive = git_bytes(
        repository, ["archive", "--format=tar", commit], toolchain
    )
    atomic_write(archive_path, archive, mode=0o444)
    files = extract_archive_payload(archive, destination)
    atomic_json(manifest_path, file_manifest(destination))
    return {
        "archive_bytes": len(archive),
        "archive_manifest_path": manifest_path,
        "archive_manifest_sha256": hash_file(manifest_path),
        "archive_path": archive_path,
        "archive_sha256": hash_bytes(archive),
        "file_count": len(files),
    }


def copy_new(source: Path, destination: Path) -> dict[str, Any]:
    if destination.exists() or destination.is_symlink():
        raise PreparationError(f"overlay destination already exists: {destination}")
    payload = source.read_bytes()
    atomic_write(destination, payload, mode=0o444)
    return {"path": destination.as_posix(), "sha256": hash_bytes(payload), "size": len(payload)}


def shared_manifest() -> dict[str, Any]:
    entries = []
    for source in sorted(SHARED_SOURCE.glob("*.rs")):
        entries.append({"name": source.name, "sha256": hash_file(source), "size": source.stat().st_size})
    return {"schema": "asterism-rebaseline-shared-v3", "entries": entries}


def inject_overlay(variant: str, root: Path) -> dict[str, Any]:
    shared = shared_manifest()
    if variant == "B":
        examples = root / "crates" / "mess-log" / "examples"
        main_source = BARE_SOURCE / "main.rs"
        adapter_source = BARE_SOURCE / "adapter.rs"
        main_destination = examples / "asterism_rebaseline_bare.rs"
    else:
        examples = root / "crates" / "mess-store" / "examples"
        main_source = PUBLIC_SOURCE / "main.rs"
        adapter_source = PUBLIC_SOURCE / "adapters" / ("current.rs" if variant == "A" else "borrowed.rs")
        main_destination = examples / "asterism_rebaseline_public.rs"
    placements = [copy_new(main_source, main_destination)]
    adapter_destination = examples / "asterism_rebaseline_adapter.rs"
    placements.append(copy_new(adapter_source, adapter_destination))
    shared_root = examples / "asterism_rebaseline_shared"
    for entry in shared["entries"]:
        placements.append(copy_new(SHARED_SOURCE / entry["name"], shared_root / entry["name"]))
    for placement in placements:
        placement["path"] = Path(placement["path"]).relative_to(root).as_posix()
    observed = {entry["name"]: hash_file(shared_root / entry["name"]) for entry in shared["entries"]}
    expected = {entry["name"]: entry["sha256"] for entry in shared["entries"]}
    if observed != expected:
        raise PreparationError(f"{variant} shared overlay differs after copy")
    manifest = {
        "schema": "asterism-rebaseline-overlay-v3",
        "variant": variant,
        "adapter_sha256": hash_file(adapter_destination),
        "main_sha256": hash_file(main_destination),
        "placements": sorted(placements, key=lambda item: item["path"]),
        "shared_manifest": shared,
        "shared_manifest_sha256": hash_bytes(canonical_json(shared)),
    }
    return manifest


def lock_packages(payload: bytes) -> dict[tuple[str, str, str], dict[str, Any]]:
    parsed = tomllib.loads(payload.decode())
    output = {}
    for package in parsed.get("package", []):
        key = (package["name"], package["version"], package.get("source", "path"))
        output[key] = {
            "checksum": package.get("checksum"),
            "dependencies": sorted(package.get("dependencies", [])),
        }
    return output


def lock_diff(left: bytes, right: bytes) -> dict[str, Any]:
    left_packages = lock_packages(left)
    right_packages = lock_packages(right)
    left_keys = set(left_packages)
    right_keys = set(right_packages)
    changed = sorted(key for key in left_keys & right_keys if left_packages[key] != right_packages[key])
    encode = lambda key: {"name": key[0], "version": key[1], "source": key[2]}
    return {
        "added": [encode(key) for key in sorted(right_keys - left_keys)],
        "changed": [encode(key) for key in changed],
        "removed": [encode(key) for key in sorted(left_keys - right_keys)],
    }


def stage_variant(
    repository: Path,
    output: Path,
    variant: str,
    claim: dict[str, Any],
    toolchain: dict[str, str],
) -> dict[str, Any]:
    root = output / "materialized" / variant
    archive = extract_archive(
        repository,
        claim["product_commit"],
        root,
        output / "archives" / f"source-{variant}.tar",
        output / "manifests" / f"archive-{variant}.json",
        toolchain,
    )
    overlay = inject_overlay(variant, root)
    overlay_path = output / "manifests" / f"overlay-{variant}.json"
    atomic_json(overlay_path, overlay)
    return {
        "root": root,
        "archive": archive,
        "overlay": overlay,
        "overlay_manifest_path": overlay_path,
        "overlay_manifest_sha256": hash_file(overlay_path),
    }


def sandboxed_resolution_argv(
    root: Path, toolchain: dict[str, str], cargo_arguments: list[str]
) -> list[str]:
    return [
        toolchain["bwrap_path"],
        "--die-with-parent",
        "--new-session",
        "--unshare-net",
        "--ro-bind",
        "/",
        "/",
        "--dev-bind",
        "/dev",
        "/dev",
        "--proc",
        "/proc",
        "--tmpfs",
        "/tmp",
        "--bind",
        str(root),
        str(root),
        "--chdir",
        str(root),
        toolchain["cargo_path"],
        *cargo_arguments,
    ]


def stage_locks(repository: Path, output: Path) -> None:
    if output.exists() or output.is_symlink():
        raise PreparationError(f"output must be absent: {output}")
    admission = filesystem_admission(output.parent)
    output.mkdir(parents=True)
    toolchain = toolchain_identity()
    plan = load_plan(repository, toolchain)
    resolution_env = frozen_cargo_environment(toolchain)
    current_lock = git_bytes(
        repository,
        ["show", f"{plan['variants']['A']['product_commit']}:Cargo.lock"],
        toolchain,
    )
    records: dict[str, Any] = {}
    shared_hashes = set()
    public_hashes = set()
    for variant in VARIANTS:
        claim = plan["variants"][variant]
        staged = stage_variant(repository, output, variant, claim, toolchain)
        shared_hashes.add(staged["overlay"]["shared_manifest_sha256"])
        if variant != "B":
            public_hashes.add(staged["overlay"]["main_sha256"])
        root = staged["root"]
        config_binding = write_cargo_config_search(
            output / "manifests" / f"cargo-config-{variant}.json",
            root,
            toolchain,
        )
        replay_cargo_config_search(config_binding, root, toolchain)
        historical = git_bytes(
            repository,
            ["show", f"{claim['lock']['commit']}:{claim['lock']['path']}"],
            toolchain,
        )
        lock_path = root / "Cargo.lock"
        current_attempt = None
        if variant in {"C", "D"}:
            atomic_write(lock_path, current_lock)
            replay_cargo_config_search(config_binding, root, toolchain)
            current_attempt = run(
                sandboxed_resolution_argv(
                    root,
                    toolchain,
                    [
                        "metadata",
                        "--locked",
                        "--offline",
                        "--format-version",
                        "1",
                        "--no-deps",
                    ],
                ),
                cwd=root,
                env=resolution_env,
                timeout=300,
            )
            replay_cargo_config_search(config_binding, root, toolchain)
            current_attempt["cargo_config_search"] = config_binding
            current_attempt["environment"] = resolution_env
            current_attempt["toolchain"] = toolchain
            lock_path.unlink()
            replay_cargo_config_search(config_binding, root, toolchain)
            resolved = run(
                sandboxed_resolution_argv(
                    root, toolchain, ["generate-lockfile", "--offline"]
                ),
                cwd=root,
                env=resolution_env,
                timeout=300,
            )
            replay_cargo_config_search(config_binding, root, toolchain)
            resolved["cargo_config_search"] = config_binding
            resolved["environment"] = resolution_env
            resolved["toolchain"] = toolchain
            if resolved["exit_status"] != 0 or not lock_path.is_file():
                raise PreparationError(f"{variant} offline lock resolution failed: {resolved['stderr']}")
            final_lock = lock_path.read_bytes()
        else:
            if hash_bytes(historical) != claim["lock"]["sha256"]:
                raise PreparationError(f"{variant} tracked lock changed")
            final_lock = historical
            resolved = run(
                [toolchain["git_path"], "show", f"{claim['lock']['commit']}:{claim['lock']['path']}"],
                cwd=repository,
                env=resolution_env,
                timeout=120,
            )
            resolved["cargo_config_search"] = config_binding
            resolved["environment"] = resolution_env
            resolved["toolchain"] = toolchain
            if (
                resolved["exit_status"] != 0
                or resolved["stdout_sha256"] != hash_bytes(historical)
            ):
                raise PreparationError(f"{variant} tracked lock readback failed")
        replay_cargo_config_search(config_binding, root, toolchain)
        candidate_path = output / "locks" / f"Cargo-{variant}.lock"
        atomic_write(candidate_path, final_lock)
        records[variant] = {
            "adapter_sha256": staged["overlay"]["adapter_sha256"],
            "archive_sha256": staged["archive"]["archive_sha256"],
            "current_lock_attempt": current_attempt,
            "final_lock_path": str(candidate_path.resolve()),
            "final_lock_sha256": hash_bytes(final_lock),
            "historical_lock": claim["lock"],
            "historical_to_final": lock_diff(historical, final_lock),
            "current_to_final": lock_diff(current_lock, final_lock),
            "overlay_manifest_path": str(staged["overlay_manifest_path"].resolve()),
            "overlay_manifest_sha256": staged["overlay_manifest_sha256"],
            "resolver": resolved,
            "shared_manifest_sha256": staged["overlay"]["shared_manifest_sha256"],
        }
        if toolchain_identity() != toolchain:
            raise PreparationError(f"toolchain changed while staging {variant}")
    if len(shared_hashes) != 1 or len(public_hashes) != 1:
        raise PreparationError("shared/public overlays are not byte-identical")
    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "filesystem_admission": admission,
        "protocol": PROTOCOL,
        "protocol_sha256": plan["protocol_sha256"],
        "schema": LOCK_SCHEMA,
        "source_plan_path": str(PLAN_PATH),
        "source_plan_sha256": hash_file(PLAN_PATH),
        "toolchain": toolchain,
        "variants": records,
    }
    atomic_json(output / "lock-candidates.json", manifest)
    make_read_only(output)


def tooling_identity(
    repository: Path, toolchain: dict[str, str]
) -> tuple[str, str]:
    commit = git_text(repository, ["rev-parse", "HEAD"], toolchain)
    tree = git_text(repository, ["rev-parse", "HEAD^{tree}"], toolchain)
    if git_text(repository, ["status", "--porcelain"], toolchain):
        raise PreparationError("tooling checkout must be clean")
    return commit, tree


def validate_lock_manifest(
    locks: dict[str, Any], plan: dict[str, Any]
) -> None:
    if (
        locks.get("protocol") != PROTOCOL
        or locks.get("protocol_sha256") != plan["protocol_sha256"]
        or locks.get("source_plan_sha256") != hash_file(PLAN_PATH)
        or set(locks.get("variants", {})) != set(VARIANTS)
    ):
        raise PreparationError("lock candidates are not bound to the current source plan")
    toolchain = validate_toolchain(locks.get("toolchain"))
    admission = locks.get("filesystem_admission")
    if not isinstance(admission, dict) or admission.get("schema") != FILESYSTEM_ADMISSION_SCHEMA:
        raise PreparationError("lock candidates lack filesystem admission")
    if (
        admission.get("filesystem") != MEASUREMENT_FILESYSTEM
        or admission.get("minimum_available_bytes") != MIN_AVAILABLE_BYTES
        or admission.get("minimum_available_inodes") != MIN_AVAILABLE_INODES
        or admission.get("available_bytes", 0) < MIN_AVAILABLE_BYTES
        or admission.get("available_inodes", 0) < MIN_AVAILABLE_INODES
    ):
        raise PreparationError("lock-candidate filesystem admission differs")
    expected_environment = frozen_cargo_environment(toolchain)
    shared_manifest_hashes: set[str] = set()
    for variant in VARIANTS:
        claim = locks["variants"][variant]
        resolver = claim.get("resolver", {})
        if (
            resolver.get("toolchain") != toolchain
            or resolver.get("environment") != expected_environment
        ):
            raise PreparationError(f"{variant} resolver identity differs")
        current_attempt = claim.get("current_lock_attempt")
        records = [resolver]
        if current_attempt is not None:
            if (
                current_attempt.get("toolchain") != toolchain
                or current_attempt.get("environment") != expected_environment
            ):
                raise PreparationError(f"{variant} current-lock identity differs")
            records.append(current_attempt)
        for record in records:
            binding = record.get("cargo_config_search")
            if not isinstance(binding, dict):
                raise PreparationError(f"{variant} Cargo config binding is absent")
            config = load_canonical(Path(binding["path"]), CARGO_CONFIG_SCHEMA)
            replay_cargo_config_search(
                binding, Path(config["cwd"]), toolchain
            )
        lock_path = Path(claim["final_lock_path"]).resolve(strict=True)
        if hash_file(lock_path) != claim.get("final_lock_sha256"):
            raise PreparationError(f"{variant} lock candidate changed")
        overlay_path = Path(claim["overlay_manifest_path"]).resolve(strict=True)
        if hash_file(overlay_path) != claim.get("overlay_manifest_sha256"):
            raise PreparationError(f"{variant} overlay manifest changed")
        overlay = load_canonical(
            overlay_path, "asterism-rebaseline-overlay-v3"
        )
        shared_manifest_sha256 = claim.get("shared_manifest_sha256")
        if (
            overlay.get("variant") != variant
            or overlay.get("adapter_sha256") != claim.get("adapter_sha256")
            or overlay.get("shared_manifest_sha256")
            != shared_manifest_sha256
            or not is_lower_hex(shared_manifest_sha256, SHA256)
        ):
            raise PreparationError(f"{variant} overlay identity mismatch")
        shared_manifest_hashes.add(shared_manifest_sha256)
    if len(shared_manifest_hashes) != 1:
        raise PreparationError("lock/overlay shared manifest hashes differ")


def validate_approval(
    repository: Path,
    approval_path: Path,
    lock_manifest_path: Path,
    tools_path: Path,
    toolchain: dict[str, str],
) -> tuple[CanonicalSnapshot, dict[str, Any], dict[str, Any]]:
    approval_snapshot = immutable_canonical_snapshot(
        approval_path, APPROVAL_SCHEMA, "source approval"
    )
    approval = approval_snapshot.value
    locks = load_canonical(lock_manifest_path, LOCK_SCHEMA)
    tools = validate_tools_manifest(tools_path)
    plan = load_plan(repository, toolchain)
    validate_lock_manifest(locks, plan)
    expected_top = {
        "schema", "protocol", "protocol_sha256", "status", "review_id", "reviewed_at",
        "comm_allowlist", "filesystem_admission", "toolchain", "tooling_commit", "tooling_tree",
        "shared_manifest_sha256", "tools_manifest", "tools_manifest_sha256", "variants",
    }
    if set(approval) != expected_top or approval.get("status") != "approved":
        raise PreparationError("source approval top-level/status mismatch")
    if approval.get("protocol") != PROTOCOL or approval.get("protocol_sha256") != plan["protocol_sha256"]:
        raise PreparationError("source approval protocol mismatch")
    if approval.get("toolchain") != locks.get("toolchain"):
        raise PreparationError("source approval toolchain mismatch")
    if approval.get("filesystem_admission") != locks.get("filesystem_admission"):
        raise PreparationError("source approval filesystem admission mismatch")
    if approval.get("comm_allowlist") != COMM_ALLOWLIST:
        raise PreparationError("source approval comm allowlist mismatch")
    tools_sha256 = hash_file(tools_path)
    if (
        approval.get("tools_manifest") != tools
        or approval.get("tools_manifest_sha256") != tools_sha256
        or tools_sha256 != hash_bytes(canonical_json(tools))
    ):
        raise PreparationError("source approval tools manifest mismatch")
    commit, tree = tooling_identity(repository, toolchain)
    if approval.get("tooling_commit") != commit or approval.get("tooling_tree") != tree:
        raise PreparationError("source approval tooling identity mismatch")
    if set(approval.get("variants", {})) != set(VARIANTS):
        raise PreparationError("source approval variant set mismatch")
    shared_manifest_hashes = {
        locks["variants"][variant]["shared_manifest_sha256"]
        for variant in VARIANTS
    }
    if (
        len(shared_manifest_hashes) != 1
        or approval.get("shared_manifest_sha256")
        != next(iter(shared_manifest_hashes))
    ):
        raise PreparationError("source approval shared manifest mismatch")
    for variant in VARIANTS:
        claim = approval["variants"][variant]
        expected = {
            "product_commit", "product_tree", "binary_kind", "timed_surface", "adapter_sha256",
            "cargo_lock_sha256", "overlay_manifest_sha256", "allowed_overlay_paths", "lock_resolution",
            "correctness_oracle_mode", "current_lock_attempt", "profile_role_lifetime",
            "trace_path_marker_templates",
        }
        if set(claim) != expected:
            raise PreparationError(f"{variant} source approval fields differ")
        planned = plan["variants"][variant]
        locked = locks["variants"][variant]
        checks = {
            "product_commit": planned["product_commit"],
            "product_tree": planned["product_tree"],
            "binary_kind": planned["binary_kind"],
            "timed_surface": planned["timed_surface"],
            "adapter_sha256": locked["adapter_sha256"],
            "cargo_lock_sha256": locked["final_lock_sha256"],
            "correctness_oracle_mode": variant != "B",
            "current_lock_attempt": locked["current_lock_attempt"],
            "overlay_manifest_sha256": locked["overlay_manifest_sha256"],
            "lock_resolution": locked["resolver"],
            "profile_role_lifetime": profile_role_lifetime(
                repository, plan, toolchain, variant
            ),
            "trace_path_marker_templates": trace_path_marker_templates(variant),
        }
        for key, value in checks.items():
            if claim.get(key) != value:
                raise PreparationError(f"{variant} approval {key} mismatch")
        overlay = load_canonical(Path(locked["overlay_manifest_path"]), "asterism-rebaseline-overlay-v3")
        paths = [entry["path"] for entry in overlay["placements"]]
        if claim.get("allowed_overlay_paths") != paths:
            raise PreparationError(f"{variant} overlay allowlist mismatch")
    return approval_snapshot, locks, tools


def write_approval(
    repository: Path,
    lock_manifest_path: Path,
    tools_path: Path,
    output: Path,
    review_id: str,
    reviewed_at: str,
) -> None:
    if output.exists() or output.is_symlink():
        raise PreparationError(f"approval output must be absent: {output}")
    toolchain = toolchain_identity()
    locks = load_canonical(lock_manifest_path, LOCK_SCHEMA)
    tools = validate_tools_manifest(tools_path)
    plan = load_plan(repository, toolchain)
    validate_lock_manifest(locks, plan)
    commit, tree = tooling_identity(repository, toolchain)
    try:
        timestamp = datetime.fromisoformat(reviewed_at)
    except ValueError as error:
        raise PreparationError("reviewed_at is not ISO-8601") from error
    if timestamp.tzinfo is None or not review_id:
        raise PreparationError("approval requires an independent review id and zoned time")
    variants: dict[str, Any] = {}
    shared_hashes = set()
    for variant in VARIANTS:
        planned = plan["variants"][variant]
        locked = locks["variants"][variant]
        overlay = load_canonical(Path(locked["overlay_manifest_path"]), "asterism-rebaseline-overlay-v3")
        shared_hashes.add(locked["shared_manifest_sha256"])
        variants[variant] = {
            "adapter_sha256": locked["adapter_sha256"],
            "allowed_overlay_paths": [entry["path"] for entry in overlay["placements"]],
            "binary_kind": planned["binary_kind"],
            "cargo_lock_sha256": locked["final_lock_sha256"],
            "correctness_oracle_mode": variant != "B",
            "current_lock_attempt": locked["current_lock_attempt"],
            "lock_resolution": locked["resolver"],
            "overlay_manifest_sha256": locked["overlay_manifest_sha256"],
            "product_commit": planned["product_commit"],
            "product_tree": planned["product_tree"],
            "profile_role_lifetime": profile_role_lifetime(
                repository, plan, toolchain, variant
            ),
            "timed_surface": planned["timed_surface"],
            "trace_path_marker_templates": trace_path_marker_templates(variant),
        }
    if len(shared_hashes) != 1:
        raise PreparationError("lock candidates disagree on shared module manifest")
    approval = {
        "comm_allowlist": COMM_ALLOWLIST,
        "filesystem_admission": locks["filesystem_admission"],
        "protocol": PROTOCOL,
        "protocol_sha256": plan["protocol_sha256"],
        "review_id": review_id,
        "reviewed_at": reviewed_at,
        "schema": APPROVAL_SCHEMA,
        "shared_manifest_sha256": shared_hashes.pop(),
        "status": "approved",
        "toolchain": locks["toolchain"],
        "tools_manifest": tools,
        "tools_manifest_sha256": hash_file(tools_path),
        "tooling_commit": commit,
        "tooling_tree": tree,
        "variants": variants,
    }
    atomic_write(output, canonical_json(approval), mode=0o444)


def make_read_only(root: Path) -> None:
    for path in sorted(root.rglob("*"), key=lambda value: len(value.parts), reverse=True):
        if path.is_dir():
            path.chmod(0o555)
        elif path.is_file():
            path.chmod(0o555 if path.stat().st_mode & 0o111 else 0o444)
        else:
            raise PreparationError(f"unsupported staged path {path}")
    root.chmod(0o555)


def freeze_prepared_root(root: Path, claims: Path) -> None:
    claims = claims.resolve(strict=True)
    for path in sorted(root.rglob("*"), key=lambda value: len(value.parts), reverse=True):
        if path == claims or claims in path.parents:
            continue
        if path.is_symlink() or (not path.is_file() and not path.is_dir()):
            raise PreparationError(f"unsupported prepared path {path}")
        if path.is_dir():
            path.chmod(0o555)
        else:
            path.chmod(0o555 if path.stat().st_mode & 0o111 else 0o444)
    claims.chmod(0o700)
    root.chmod(0o555)


def validate_frozen_prepared_root(root: Path, claims: Path) -> None:
    root = root.resolve(strict=True)
    claims = claims.resolve(strict=True)
    for path in [root, *sorted(root.rglob("*"))]:
        if path.is_symlink():
            raise PreparationError(f"frozen prepared root contains symlink {path}")
        mode = stat.S_IMODE(path.stat().st_mode)
        if path == claims or claims in path.parents:
            if path.is_dir() and mode != 0o700:
                raise PreparationError(f"prepared claim directory mode differs: {path}")
            continue
        if path.is_dir():
            if mode != 0o555:
                raise PreparationError(f"prepared directory is not 0555: {path}")
        elif path.is_file():
            expected = 0o555 if path.stat().st_mode & 0o111 else 0o444
            if mode != expected:
                raise PreparationError(f"prepared file mode differs: {path}")
        else:
            raise PreparationError(f"unsupported frozen prepared path {path}")


def validate_support_import_immutability(
    support_root: Path,
    bound_support: dict[str, Any],
    bound_tools: dict[str, Any],
) -> None:
    expected = {
        Path(binding["path"]).resolve() for binding in bound_support.values()
    }
    observed = {
        path.resolve() for path in support_root.iterdir() if path.is_file()
    }
    if observed != expected or any(path.is_dir() for path in support_root.iterdir()):
        raise PreparationError("prepared support directory contains an extra path")
    before = file_manifest(support_root)
    import_program = (
        "import sys;"
        f"sys.path.insert(0,{json.dumps(str(support_root.resolve()))});"
        "import evidence_schema"
    )
    environment = frozen_runtime_environment({})
    for tool_name in ("evaluator_runtime", "terminal_verifier_runtime"):
        result = run(
            [bound_tools[tool_name]["path"], "-I", "-c", import_program],
            cwd=support_root,
            env=environment,
            timeout=30,
        )
        if result["exit_status"] != 0:
            raise PreparationError(
                f"{tool_name} cannot import frozen evidence schema: {result['stderr']}"
            )
        if file_manifest(support_root) != before:
            raise PreparationError(f"{tool_name} mutated frozen support files")


def sandboxed_build_argv(
    root: Path,
    target: Path,
    package: str,
    example: str,
    cargo_path: str,
    bwrap_path: str,
) -> list[str]:
    return [
        bwrap_path,
        "--die-with-parent",
        "--new-session",
        "--unshare-net",
        "--ro-bind",
        "/",
        "/",
        "--dev-bind",
        "/dev",
        "/dev",
        "--proc",
        "/proc",
        "--tmpfs",
        "/tmp",
        "--bind",
        str(target),
        str(target),
        "--chdir",
        str(root),
        cargo_path,
        "build",
        "--locked",
        "--offline",
        "--release",
        "-p",
        package,
        "--example",
        example,
        "--target-dir",
        str(target),
    ]


def expected_contract(
    *, plan: dict[str, Any], approval: dict[str, Any], approval_sha256: str,
    variant: str, nonce: str,
) -> dict[str, Any]:
    claim = approval["variants"][variant]
    return {
        "adapter_sha256": claim["adapter_sha256"],
        "binary_kind": claim["binary_kind"],
        "build_nonce": nonce,
        "cargo_lock_sha256": claim["cargo_lock_sha256"],
        "contract_mode": True,
        "correctness_oracle_mode": claim["correctness_oracle_mode"],
        "product_commit": claim["product_commit"],
        "product_tree": claim["product_tree"],
        "profile_role_lifetime": claim["profile_role_lifetime"],
        "protocol": PROTOCOL,
        "protocol_sha256": plan["protocol_sha256"],
        "rows_written": 0,
        "schema": CONTRACT_SCHEMA,
        "shared_manifest_sha256": approval["shared_manifest_sha256"],
        "source_approval_sha256": approval_sha256,
        "timed_surface": claim["timed_surface"],
        "tooling_commit": approval["tooling_commit"],
        "tooling_tree": approval["tooling_tree"],
        "variant": variant,
    }


def build(
    repository: Path, output: Path, approval_path: Path, lock_manifest_path: Path,
    tools_path: Path,
) -> None:
    if output.exists() or output.is_symlink():
        raise PreparationError(f"output must be absent: {output}")
    admission = filesystem_admission(output.parent)
    current_toolchain = toolchain_identity()
    approval_snapshot, locks, tools = validate_approval(
        repository, approval_path, lock_manifest_path, tools_path,
        current_toolchain,
    )
    approval = approval_snapshot.value
    plan = load_plan(repository, current_toolchain)
    output.mkdir(parents=True)
    approval_sha256 = approval_snapshot.sha256
    bound_approval_path = output / "bindings" / "source-approval.json"
    atomic_write(bound_approval_path, approval_snapshot.payload, mode=0o444)
    bound_approval = immutable_canonical_snapshot(
        bound_approval_path, APPROVAL_SCHEMA, "copied source approval"
    )
    if (
        bound_approval.payload != approval_snapshot.payload
        or bound_approval.sha256 != approval_sha256
        or bound_approval.value != approval
    ):
        raise PreparationError("copied source approval differs")
    tools_manifest_sha256 = hash_file(tools_path)
    bound_tools_manifest_path = output / "bindings" / "tools-manifest.json"
    atomic_write(
        bound_tools_manifest_path, canonical_json(tools), mode=0o444
    )
    if (
        hash_file(bound_tools_manifest_path) != tools_manifest_sha256
        or stat.S_IMODE(bound_tools_manifest_path.lstat().st_mode) != 0o444
        or bound_tools_manifest_path.is_symlink()
    ):
        raise PreparationError("copied tools manifest binding differs")
    input_sources = {
        "historical_baseline": (
            HISTORICAL_BASELINE,
            output / "inputs" / "BN-2SU-FINAL.csv",
            HISTORICAL_BASELINE_SHA256,
        ),
        "protocol": (
            PROTOCOL_DOCUMENT,
            output / "BN-2L3N-PROTOCOL.md",
            PROTOCOL_DOCUMENT_SHA256,
        ),
    }
    bound_inputs: dict[str, Any] = {}
    for name, (source, destination, expected_sha256) in input_sources.items():
        source = source.resolve(strict=True)
        if not source.is_file() or source.is_symlink():
            raise PreparationError(f"prepared input {name} source is not a regular file")
        if hash_file(source) != expected_sha256:
            raise PreparationError(f"prepared input {name} source hash differs")
        atomic_write(destination, source.read_bytes(), mode=0o444)
        if hash_file(destination) != expected_sha256:
            raise PreparationError(f"prepared input {name} copy hash differs")
        bound_inputs[name] = {
            "mode": 0o444,
            "path": str(destination.resolve()),
            "sha256": expected_sha256,
        }
    variants: dict[str, Any] = {}
    approved_toolchain = approval["toolchain"]
    for variant in VARIANTS:
        if toolchain_identity() != approved_toolchain:
            raise PreparationError(f"toolchain changed before {variant} build")
        claim = plan["variants"][variant]
        staged = stage_variant(
            repository, output, variant, claim, approved_toolchain
        )
        if variant == "C":
            validate_c_role_lifetime_sources(
                (
                    staged["root"]
                    / "crates/mess-store/examples/asterism_rebaseline_public.rs"
                ).read_text(),
                (staged["root"] / "crates/mess-store/src/engine.rs").read_text(),
            )
        if staged["overlay_manifest_sha256"] != approval["variants"][variant]["overlay_manifest_sha256"]:
            raise PreparationError(f"{variant} rebuilt overlay manifest mismatch")
        root = staged["root"]
        config_binding = write_cargo_config_search(
            output / "manifests" / f"cargo-config-build-{variant}.json",
            root,
            approved_toolchain,
        )
        replay_cargo_config_search(config_binding, root, approved_toolchain)
        approved_lock_path = Path(locks["variants"][variant]["final_lock_path"]).resolve(strict=True)
        if hash_file(approved_lock_path) != approval["variants"][variant]["cargo_lock_sha256"]:
            raise PreparationError(f"{variant} approved lock changed")
        lock_destination = root / "Cargo.lock"
        lock_destination.unlink(missing_ok=True)
        atomic_write(lock_destination, approved_lock_path.read_bytes(), mode=0o444)
        lock_pre_sha256 = hash_file(lock_destination)
        make_read_only(root)
        source_before = file_manifest(root)
        materialized_manifest_path = output / "manifests" / f"materialized-{variant}.json"
        atomic_json(materialized_manifest_path, source_before)
        materialized_manifest_sha256 = hash_file(materialized_manifest_path)
        nonce = secrets.token_hex(32)
        contract = expected_contract(
            plan=plan, approval=approval, approval_sha256=approval_sha256,
            variant=variant, nonce=nonce,
        )
        build_env = frozen_cargo_environment(
            approved_toolchain,
            {
                environment_name: str(contract[field])
                for field, environment_name in BUILD_ENV_BY_CONTRACT_FIELD.items()
            },
        )
        target = output / "targets" / variant
        target_was_absent = not target.exists() and not target.is_symlink()
        if not target_was_absent:
            raise PreparationError(f"{variant} target directory is not fresh")
        target.mkdir(parents=True)
        example = "asterism_rebaseline_bare" if variant == "B" else "asterism_rebaseline_public"
        package = "mess-log" if variant == "B" else "mess-store"
        build_argv = sandboxed_build_argv(
            root,
            target.resolve(),
            package,
            example,
            approved_toolchain["cargo_path"],
            approved_toolchain["bwrap_path"],
        )
        build_log_path = output / "logs" / f"build-{variant}.json"
        replay_cargo_config_search(config_binding, root, approved_toolchain)
        build_child, _build_stdout, build_stderr = run_attested(
            build_argv,
            cwd=root,
            env=build_env,
            output_path=build_log_path,
            raw_stdout=False,
        )
        validate_attested_child(build_child, f"{variant} build")
        replay_cargo_config_search(config_binding, root, approved_toolchain)
        if toolchain_identity() != approved_toolchain:
            raise PreparationError(f"toolchain changed during {variant} build")
        lock_post_sha256 = hash_file(lock_destination)
        if lock_post_sha256 != approval["variants"][variant]["cargo_lock_sha256"]:
            raise PreparationError(f"{variant} Cargo.lock changed during build")
        source_after = file_manifest(root)
        if source_after != source_before:
            raise PreparationError(f"{variant} read-only materialization changed during build")
        built = target / "release" / "examples" / example
        artifact_root = output / "artifacts" / variant
        artifact_root.mkdir(parents=True)
        binary = artifact_root / f"ast-rb-{variant.lower()}"
        atomic_write(binary, built.read_bytes(), mode=0o555)
        contract_env = frozen_runtime_environment(
            {"ASTERISM_REBASELINE_MODE": "contract"}
        )
        contract_output_path = output / "logs" / f"contract-{variant}.json"
        contract_child, contract_stdout, contract_stderr = run_attested(
            [str(binary.resolve())],
            cwd=artifact_root,
            env=contract_env,
            output_path=contract_output_path,
            raw_stdout=True,
            timeout=30,
        )
        validate_attested_child(contract_child, f"{variant} contract")
        if contract_stderr:
            raise PreparationError(f"{variant} contract smoke failed")
        if contract_stdout != canonical_json(contract):
            raise PreparationError(f"{variant} contract output mismatch")
        marker_templates = validate_trace_path_marker_templates(
            approval["variants"][variant]["trace_path_marker_templates"]
        )
        evidence_env = {
            "ASTERISM_REBASELINE_LOG_PATH_MARKERS": json.dumps(
                marker_templates["log"],
                sort_keys=True,
                separators=(",", ":"),
            ),
            "ASTERISM_REBASELINE_METADATA_PATH_MARKERS": json.dumps(
                marker_templates["metadata"],
                sort_keys=True,
                separators=(",", ":"),
            ),
        }
        attestation = {
            "archive_manifest_path": str(staged["archive"]["archive_manifest_path"].resolve()),
            "archive_manifest_sha256": staged["archive"]["archive_manifest_sha256"],
            "build_argv": build_argv,
            "build_child": build_child,
            "build_env": build_env,
            "build_completed_at": build_child["completed_at"],
            "build_completed_monotonic_ns": build_child["completed_monotonic_ns"],
            "build_log_path": str(build_log_path.resolve()),
            "build_log_sha256": hash_file(build_log_path),
            "build_nonce": nonce,
            "build_started_at": build_child["started_at"],
            "build_started_monotonic_ns": build_child["started_monotonic_ns"],
            "cargo_lock_path": str(lock_destination.resolve()),
            "cargo_lock_post_sha256": lock_post_sha256,
            "cargo_lock_pre_sha256": lock_pre_sha256,
            "cargo_lock_sha256": hash_file(lock_destination),
            "cargo_config_search": config_binding,
            "contract_child": contract_child,
            "contract_output_path": str(contract_output_path.resolve()),
            "contract_output_sha256": hash_file(contract_output_path),
            "materialized_manifest_path": str(materialized_manifest_path.resolve()),
            "materialized_manifest_post_sha256": materialized_manifest_sha256,
            "materialized_manifest_pre_sha256": materialized_manifest_sha256,
            "materialized_manifest_sha256": materialized_manifest_sha256,
            "materialized_root": str(root.resolve()),
            "overlay_manifest_path": str(staged["overlay_manifest_path"].resolve()),
            "overlay_manifest_sha256": staged["overlay_manifest_sha256"],
            "source_archive_bytes": staged["archive"]["archive_bytes"],
            "source_archive_path": str(staged["archive"]["archive_path"].resolve()),
            "source_archive_sha256": staged["archive"]["archive_sha256"],
            "source_commit": claim["product_commit"],
            "source_read_only": True,
            "source_tree": claim["product_tree"],
            "target_dir": str(target.resolve()),
            "target_dir_was_absent": target_was_absent,
            "toolchain": approved_toolchain,
        }
        variants[variant] = {
            "artifact_root": str(artifact_root.resolve()),
            "attestation": attestation,
            "binary": {"path": str(binary.resolve()), "sha256": hash_file(binary)},
            "comm": binary.name,
            "contract": contract,
            "contract_argv": [str(binary.resolve())],
            "contract_env": contract_env,
            "correctness_oracle_mode": contract["correctness_oracle_mode"],
            "evidence_argv": [str(binary.resolve())],
            "evidence_env": evidence_env,
            "executable_mode": 0o555,
            "trace_path_marker_templates": marker_templates,
        }
    source_tools = tools.get("tools")
    if not isinstance(source_tools, dict) or set(source_tools) != REQUIRED_TOOLS:
        raise PreparationError("tools manifest bindings are not the frozen exact set")
    bound_tools: dict[str, Any] = {}
    tool_root = output / "artifacts" / "tools"
    seen_destinations: set[Path] = set()
    for name, binding in sorted(source_tools.items()):
        if not isinstance(binding, dict) or set(binding) != {
            "path", "sha256", "executable_mode", "comm"
        }:
            raise PreparationError(f"tool {name} binding fields differ")
        if binding["executable_mode"] != 0o555:
            raise PreparationError(f"tool {name} executable mode differs")
        source = validate_bound_source_file(
            binding["path"], binding["sha256"], 0o555, f"tool {name}"
        )
        comm = binding["comm"]
        if (
            not isinstance(comm, str)
            or not comm
            or len(comm.encode()) > 15
            or Path(comm).name != comm
        ):
            raise PreparationError(f"tool {name} comm is invalid")
        if comm != REQUIRED_TOOL_COMMS[name]:
            raise PreparationError(f"tool {name} comm differs from reviewed source")
        destination = tool_root / comm
        if destination in seen_destinations:
            raise PreparationError(f"tool destination collision: {destination.name}")
        atomic_write(destination, source.read_bytes(), mode=0o555)
        seen_destinations.add(destination)
        bound_tools[name] = {
            "comm": comm,
            "executable_mode": 0o555,
            "path": str(destination.resolve()),
            "sha256": hash_file(destination),
        }
        if {
            "comm": bound_tools[name]["comm"],
            "executable_mode": bound_tools[name]["executable_mode"],
            "sha256": bound_tools[name]["sha256"],
        } != {
            "comm": binding["comm"],
            "executable_mode": binding["executable_mode"],
            "sha256": binding["sha256"],
        }:
            raise PreparationError(f"prepared tool {name} differs from approval")
    source_support = tools.get("support_files")
    if not isinstance(source_support, dict) or set(source_support) != REQUIRED_SUPPORT_FILES:
        raise PreparationError("support file bindings are not the frozen exact set")
    bound_support: dict[str, Any] = {}
    support_root = output / "artifacts" / "support"
    seen_support: set[Path] = set()
    for name, binding in sorted(source_support.items()):
        if not isinstance(binding, dict) or set(binding) != {"path", "sha256", "mode"}:
            raise PreparationError(f"support file {name} binding fields differ")
        if binding["mode"] != 0o444:
            raise PreparationError(f"support file {name} mode differs")
        source = validate_bound_source_file(
            binding["path"], binding["sha256"], 0o444,
            f"support file {name}",
        )
        destination = support_root / source.name
        if destination in seen_support:
            raise PreparationError(f"support file destination collision: {source.name}")
        seen_support.add(destination)
        atomic_write(destination, source.read_bytes(), mode=0o444)
        bound_support[name] = {
            "mode": 0o444,
            "path": str(destination.resolve()),
            "sha256": hash_file(destination),
        }
        if {
            "mode": bound_support[name]["mode"],
            "sha256": bound_support[name]["sha256"],
        } != {"mode": binding["mode"], "sha256": binding["sha256"]}:
            raise PreparationError(
                f"prepared support file {name} differs from approval"
            )
    comm_allowlist = list(approval["comm_allowlist"])
    prepared_comms = (
        {entry["comm"] for entry in variants.values()}
        | {entry["comm"] for entry in bound_tools.values()}
    )
    if not prepared_comms.issubset(set(comm_allowlist)):
        raise PreparationError("reviewed comm allowlist omits a prepared executable")
    claims = output / "claims"
    claims.mkdir(mode=0o700)
    prepared = {
        "comm_allowlist": comm_allowlist,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_monotonic_ns": time.monotonic_ns(),
        "filesystem_admission": admission,
        "inputs": bound_inputs,
        "build_order": list(VARIANTS),
        "protocol": PROTOCOL,
        "protocol_sha256": plan["protocol_sha256"],
        "schema": PREPARED_SCHEMA,
        "single_use_claim": {
            "path": str((claims / "single-use-claim.json").resolve())
        },
        "source_approval": {"path": str(bound_approval_path.resolve()), "sha256": approval_sha256},
        "tools_manifest": {
            "mode": 0o444,
            "path": str(bound_tools_manifest_path.resolve()),
            "sha256": tools_manifest_sha256,
        },
        "support_files": bound_support,
        "tooling_commit": approval["tooling_commit"],
        "tooling_tree": approval["tooling_tree"],
        "toolchain": approved_toolchain,
        "tools": bound_tools,
        "variants": variants,
    }
    atomic_json(output / "prepared-artifacts.json", prepared)
    freeze_prepared_root(output, claims)
    validate_frozen_prepared_root(output, claims)
    validate_support_import_immutability(support_root, bound_support, bound_tools)


def static_self_test() -> None:
    plan = load_canonical(PLAN_PATH, PLAN_SCHEMA)
    assert APPROVAL_SCHEMA == "bn-2l3n-source-approval-v3"
    assert PREPARED_SCHEMA == "bn-2l3n-prepared-artifacts-v3"
    assert C_ROLE_LIFETIME_CONTRACT == {
        "schema": "bn-2l3n-c-role-lifetime-v3",
        "blocking_thread_keep_alive_ns": 3_600_000_000_000,
        "maximum_profile_child_timeout_ns": 120_000_000_000,
        "ready_to_measured_spawn_blocking_sites": 1,
        "ready_to_measured_other_thread_birth_sites": 0,
    }
    assert plan["protocol"] == PROTOCOL
    assert plan["protocol_sha256"] == PROTOCOL_DOCUMENT_SHA256
    for variant in ("A", "B"):
        claim = plan["variants"][variant]
        assert claim["product_commit"] == (
            "d644dc583dfe6a3d2cd07e71ce0212a323875ab4"
        )
        assert claim["product_tree"] == (
            "205d853905bdb648ee997900c6aef24a323aa380"
        )
        assert claim["lock"]["commit"] == claim["product_commit"]
    assert len(shared_manifest()["entries"]) == 7
    assert len({entry["sha256"] for entry in shared_manifest()["entries"]}) == 7
    assert hash_file(PLAN_PATH) == hash_bytes(PLAN_PATH.read_bytes())
    assert hash_file(PROTOCOL_DOCUMENT) == PROTOCOL_DOCUMENT_SHA256
    assert hash_file(HISTORICAL_BASELINE) == HISTORICAL_BASELINE_SHA256
    assert COMM_ALLOWLIST == sorted(set(COMM_ALLOWLIST))
    assert all(0 < len(value.encode()) <= 15 for value in COMM_ALLOWLIST)
    assert set(REQUIRED_TOOL_COMMS) == REQUIRED_TOOLS
    assert set(REQUIRED_SUPPORT_BASENAMES) == REQUIRED_SUPPORT_FILES
    assert {
        "ast-rb-a", "ast-rb-b", "ast-rb-c", "ast-rb-d",
        *REQUIRED_TOOL_COMMS.values(),
    }.issubset(set(COMM_ALLOWLIST))
    public = (PUBLIC_SOURCE / "main.rs").read_text()
    digest_source = (SHARED_SOURCE / "digest.rs").read_text()
    validate_correctness_oracle_control_source(public)
    validate_reopen_digest_sources(public, digest_source)

    def mirror_logical_digest(payload: bytes) -> int:
        value = 0xCBF29CE484222325
        for byte in payload:
            value ^= byte
            value = value * 0x100000001B3 & ((1 << 64) - 1)
        return value

    smoke_stream_digest = mirror_logical_digest(bytes(range(64)))
    smoke_logical_digest = mirror_logical_digest(
        smoke_stream_digest.to_bytes(8, "little")
    )
    smoke_registry_digest = mirror_logical_digest(
        (0).to_bytes(8, "little") + (0).to_bytes(8, "little")
    )
    canonical_logical_digest = f"{smoke_logical_digest:064x}"
    canonical_registry_digest = f"{smoke_registry_digest:064x}"
    assert canonical_logical_digest == (
        "000000000000000000000000000000000000000000000000"
        "e2d874aa120f66af"
    )
    assert canonical_registry_digest == (
        "000000000000000000000000000000000000000000000000"
        "88201fb960ff6465"
    )
    assert is_lower_hex(canonical_logical_digest, SHA256)
    assert is_lower_hex(canonical_registry_digest, SHA256)
    assert not is_lower_hex(f"{smoke_logical_digest:016x}", SHA256)
    assert not is_lower_hex(f"{smoke_registry_digest:016x}", SHA256)
    oracle_source = rust_item(
        public,
        "fn run_common_public_oracle(",
        "public correctness oracle",
    )

    def hostile_oracle(old: str, new: str, context: str) -> str:
        mutated = replace_exact_once(oracle_source, old, new, context)
        return replace_exact_once(
            public,
            oracle_source,
            mutated,
            f"{context} oracle item",
        )

    hostile_oracle_sources = (
        public.replace(
            "let opened_nonce = control.opened(&runtime_nonce);",
            "let opened_nonce = control.runtime(&runtime_nonce);",
        ),
        public.replace(
            "let opened_nonce = control.opened(&runtime_nonce);",
            "let opened_nonce = runtime_nonce;",
        ),
        public.replace(
            "emit_correctness_oracle(correctness_oracle_args());",
            "self_test();",
        ),
        hostile_oracle(
            """        alloc_before.calls,
        alloc_before.bytes,""",
            """        alloc_before.bytes,
        alloc_before.calls,""",
            "oracle allocation-start swap",
        ),
        hostile_oracle(
            """        cpu_before.user_ns,
        cpu_before.system_ns,""",
            """        cpu_before.system_ns,
        cpu_before.user_ns,""",
            "oracle CPU-start swap",
        ),
        hostile_oracle(
            """        ready_monotonic_ns,
        counter_start_monotonic_ns,""",
            """        counter_start_monotonic_ns,
        ready_monotonic_ns,""",
            "oracle ready/counter-start timestamp swap",
        ),
        hostile_oracle(
            """            allocation_calls_end: alloc_after.calls,
            allocated_bytes_end: alloc_after.bytes,""",
            """            allocation_calls_end: alloc_after.bytes,
            allocated_bytes_end: alloc_after.calls,""",
            "oracle allocation-end swap",
        ),
        hostile_oracle(
            """            process_system_cpu_end_ns: cpu_after.system_ns,
            process_user_cpu_end_ns: cpu_after.user_ns,""",
            """            process_system_cpu_end_ns: cpu_after.user_ns,
            process_user_cpu_end_ns: cpu_after.system_ns,""",
            "oracle CPU-end swap",
        ),
        hostile_oracle(
            """            counter_end_monotonic_ns,
            last_completion_monotonic_ns,""",
            """            counter_end_monotonic_ns: last_completion_monotonic_ns,
            last_completion_monotonic_ns: counter_end_monotonic_ns,""",
            "oracle counter/completion timestamp swap",
        ),
        hostile_oracle(
            """            t0_monotonic_ns,
            t1_monotonic_ns,""",
            """            t0_monotonic_ns: t1_monotonic_ns,
            t1_monotonic_ns: t0_monotonic_ns,""",
            "oracle t0/t1 timestamp swap",
        ),
    )
    for hostile_public in hostile_oracle_sources:
        try:
            validate_correctness_oracle_control_source(hostile_public)
        except PreparationError:
            pass
        else:
            raise AssertionError(
                "hostile correctness oracle control source was accepted"
            )
    hostile_digest_sources = (
        (
            public,
            digest_source.replace(
                'format!("{:064x}", self.0)',
                'format!("{:016x}", self.0)',
            ),
        ),
        (
            public.replace(
                "logical_digest: logical_digest.canonical_hex(),",
                "logical_digest: format!(\"{:016x}\", logical_digest.value()),",
            ),
            digest_source,
        ),
    )
    for hostile_public, hostile_digest in hostile_digest_sources:
        try:
            validate_reopen_digest_sources(hostile_public, hostile_digest)
        except PreparationError:
            pass
        else:
            raise AssertionError("hostile reopen digest source was accepted")
    reopen = public[
        public.index("fn run_reopen(") : public.index("fn emit_reopen(")
    ]
    frozen_order = [
        "let opened_monotonic_ns = monotonic_ns();",
        "let alloc_after = allocation::snapshot();",
        "let cpu_after = cpu_snapshot();",
        "let counter_end_monotonic_ns = monotonic_ns();",
        "let measured_nonce = control.opened_after_start(",
        "let log_events = engine.total_events() as u64;",
        "let recovery_payload_decodes = engine.recover_payload_decodes();",
        "control.measured_and_wait_release(",
    ]
    offsets = [reopen.index(marker) for marker in frozen_order]
    assert offsets == sorted(offsets), "reopen observations entered a measured window"
    oracle = public[
        public.index("fn correctness_oracle_args(") : public.index("fn parse_usize(")
    ]
    for marker in (
        'assert_eq!(arguments.len(), 11, "correctness-oracle argv differs");',
        'assert_eq!(arguments[0], "--correctness-oracle");',
        'assert_eq!(arguments[2], contract::PROTOCOL);',
        'assert_eq!(arguments[6], contract::VARIANT);',
        'assert_eq!(arguments[8], "oracle");',
        'assert_eq!(arguments[10], "common-public-oracle");',
        'required("ASTERISM_REBASELINE_ATTEMPT_NONCE")',
        'required("ASTERISM_REBASELINE_PROTOCOL")',
    ):
        assert marker in oracle, f"correctness oracle identity marker absent: {marker}"
    assert '"correctness_oracle" => {' in public
    assert 'json_string("bn-2l3n-correctness-child-v3")' in public
    for marker in (
        "Err(AppendError::Conflict {",
        "Err(AppendError::Backend(_))",
        "adapter::assert_oracle_accounting(&engine, 4, 3, 2, false);",
        "adapter::assert_oracle_accounting(&group_engine, 1, 1, 1, true);",
        'assert_eq!(arguments[0], "--run-row");',
        'required("ASTERISM_REBASELINE_ROW_ORDINAL")',
        'required("ASTERISM_REBASELINE_CONFIG")',
    ):
        assert marker in public, f"public integration marker absent: {marker}"
    bare = (BARE_SOURCE / "main.rs").read_text()
    assert 'assert_eq!(arguments[0], "--run-row");' in bare
    assert 'required("ASTERISM_REBASELINE_CONFIG")' in bare
    for name, source in (("public", public), ("bare", bare)):
        assert "control::validate_perf_environment_mode(&mode);" in source
        assert '"smoke" => {' in source
        assert "let row = run_point(workload, root, 0);" in source
        measured = rust_item(source, "fn run_point(", f"{name} run_point")
        assert measured.index(
            "let mut writer_results = Vec::with_capacity(joins.len());"
        ) < measured.index("let alloc_before = allocation::snapshot();"), (
            f"{name} writer result allocation entered the measured window"
        )
        perf_order = [
            "let t1_monotonic_ns = monotonic_ns();",
            "control.disable_perf_after_t1(&control_nonce, t1_monotonic_ns);",
            "let alloc_after = allocation::snapshot();",
            "let cpu_after = cpu_snapshot();",
            "let counter_end_monotonic_ns = monotonic_ns();",
            "control.measured_and_wait_release(",
        ]
        perf_offsets = [measured.index(marker) for marker in perf_order]
        assert perf_offsets == sorted(perf_offsets), (
            f"{name} child perf disable/t1/counter order differs"
        )
        for field in (
            "adaptive_group_width_target",
            "group_width_distribution",
            "oldest_queued_age_ns",
            "queue_bytes",
            "queue_depth",
        ):
            assert f'("{field}", json_string("not_available"))' in source or (
                field == "adaptive_group_width_target"
                and f'"{field}",\n                json_string("not_available")' in source
            ), f"{name} fairness field absent: {field}"
    current_adapter = (PUBLIC_SOURCE / "adapters" / "current.rs").read_text()
    borrowed_adapter = (PUBLIC_SOURCE / "adapters" / "borrowed.rs").read_text()
    validate_reopen_seed_accounting_sources(
        public,
        current_adapter,
        borrowed_adapter,
    )

    def expected_seed_log_events(
        variant: str,
        domain_events: int,
        fresh_streams: int,
    ) -> int:
        if variant == "C":
            return domain_events
        if variant in ("A", "D"):
            return domain_events + fresh_streams + 1
        raise AssertionError(f"unexpected seed variant {variant}")

    full_domain_events = 1_000 * 200 * 10
    full_seed_log_events = {
        variant: expected_seed_log_events(variant, full_domain_events, 1_000)
        for variant in ("A", "C", "D")
    }
    assert full_seed_log_events == {
        "A": 2_001_001,
        "C": 2_000_000,
        "D": 2_001_001,
    }
    smoke_seed_log_events = {
        variant: expected_seed_log_events(variant, 1, 1)
        for variant in ("A", "C", "D")
    }
    assert smoke_seed_log_events == {"A": 3, "C": 1, "D": 3}

    hostile_seed_sources = (
        (
            replace_exact_once(
                public,
                """    streams: usize,
    batches_per_stream: usize,""",
                """    batches_per_stream: usize,
    streams: usize,""",
                "public reopen seed streams/batches signature swap",
            ),
            current_adapter,
            borrowed_adapter,
        ),
        (
            public,
            replace_exact_once(
                current_adapter,
                """    domain_events: u64,
    fresh_streams: u64,""",
                """    fresh_streams: u64,
    domain_events: u64,""",
                "current reopen seed accounting signature swap",
            ),
            borrowed_adapter,
        ),
        (
            public,
            current_adapter,
            replace_exact_once(
                borrowed_adapter,
                """    domain_events: u64,
    fresh_streams: u64,""",
                """    fresh_streams: u64,
    domain_events: u64,""",
                "historical reopen seed accounting signature swap",
            ),
        ),
        (
            public,
            replace_exact_once(
                current_adapter,
                """    domain_events: u64,
    public_appends: u64,
    fresh_streams: u64,""",
                """    public_appends: u64,
    domain_events: u64,
    fresh_streams: u64,""",
                "current oracle accounting signature swap",
            ),
            borrowed_adapter,
        ),
        (
            public,
            current_adapter,
            replace_exact_once(
                borrowed_adapter,
                """    domain_events: u64,
    public_appends: u64,
    fresh_streams: u64,""",
                """    public_appends: u64,
    domain_events: u64,
    fresh_streams: u64,""",
                "historical oracle accounting signature swap",
            ),
        ),
        (
            public,
            replace_exact_once(
                current_adapter,
                """        domain_events + fresh_streams + 1,
        "v3 reopen seed differs from domain + streams + one shared type",""",
                """        domain_events + 17,
        "v3 reopen seed differs from domain + streams + one shared type",""",
                "A reopen seed domain-events-plus-17 formula",
            ),
            borrowed_adapter,
        ),
        (
            public,
            current_adapter,
            replace_exact_once(
                borrowed_adapter,
                """            high_water, domain_events,
            "C reopen seed unexpectedly consumed metadata positions",""",
                """            high_water, domain_events + 17,
            "C reopen seed unexpectedly consumed metadata positions",""",
                "C reopen seed domain-events-plus-17 formula",
            ),
        ),
        (
            public,
            current_adapter,
            replace_exact_once(
                borrowed_adapter,
                """            domain_events + fresh_streams + 1,
            "D reopen seed differs from domain + streams + one shared type",""",
                """            domain_events + 17,
            "D reopen seed differs from domain + streams + one shared type",""",
                "D reopen seed domain-events-plus-17 formula",
            ),
        ),
        (
            replace_exact_once(
                public,
                'run_reopen_seed(root, 1_000, 200, 10, 8, "bn-2l3n-reopen-seed-v3");',
                'run_reopen_seed(root, 1_000, 201, 10, 8, "bn-2l3n-reopen-seed-v3");',
                "full reopen seed workload formula",
            ),
            current_adapter,
            borrowed_adapter,
        ),
        (
            replace_exact_once(
                public,
                """            run_reopen_seed(
                root,
                1,
                1,
                1,
                1,
                "bn-2l3n-overlay-smoke-reopen-seed-v3",
            );""",
                """            run_reopen_seed(
                root,
                1,
                1,
                2,
                1,
                "bn-2l3n-overlay-smoke-reopen-seed-v3",
            );""",
                "smoke reopen seed workload formula",
            ),
            current_adapter,
            borrowed_adapter,
        ),
        (
            replace_exact_once(
                public,
                """        verified.domain_events,
        streams as u64,""",
                """        streams as u64,
        verified.domain_events,""",
                "reopen seed accounting argument swap",
            ),
            current_adapter,
            borrowed_adapter,
        ),
    )
    for hostile_public, hostile_current, hostile_borrowed in (
        hostile_seed_sources
    ):
        try:
            validate_reopen_seed_accounting_sources(
                hostile_public,
                hostile_current,
                hostile_borrowed,
            )
        except PreparationError:
            pass
        else:
            raise AssertionError("hostile reopen seed accounting was accepted")
    assert current_adapter.count("assert_eq!(metrics.commit.groups, 1);") == 1
    assert borrowed_adapter.count("assert_eq!(metrics.commit.groups, 1);") == 1
    workload = (SHARED_SOURCE / "workload.rs").read_text()
    assert "(0..length as u32)" in workload
    assert "(index & 0xff) as u8" in workload
    assert "assert_eq!(payload_bytes(24), PAYLOAD_24);" in workload
    assert "assert_eq!(payload_bytes(250), PAYLOAD_250);" in workload
    contract = (SHARED_SOURCE / "contract.rs").read_text()
    control = (SHARED_SOURCE / "control.rs").read_text()
    validate_shared_control_child_source(control)
    hostile_control_sources = (
        control.replace(
            '("context_sha256", json_string(&self.context_sha256)),\n',
            "",
            1,
        ),
        control.replace(
            '("phase", json_string("opened"))',
            '("phase", json_string("runtime"))',
        ),
        control.replace(
            '("phase", json_string("measured"))',
            '("phase", json_string("ready"))',
            1,
        ),
        replace_exact_once(
            control,
            """        allocation_calls_start: u64,
        allocated_bytes_start: u64,""",
            """        allocated_bytes_start: u64,
        allocation_calls_start: u64,""",
            "shared ready allocation signature swap",
        ),
        replace_exact_once(
            control,
            """        process_user_cpu_start_ns: u64,
        process_system_cpu_start_ns: u64,""",
            """        process_system_cpu_start_ns: u64,
        process_user_cpu_start_ns: u64,""",
            "shared ready CPU signature swap",
        ),
        replace_exact_once(
            control,
            """        ready_monotonic_ns: u64,
        counter_start_monotonic_ns: u64,""",
            """        counter_start_monotonic_ns: u64,
        ready_monotonic_ns: u64,""",
            "shared ready/counter timestamp signature swap",
        ),
        replace_exact_once(
            control,
            """        open_start_monotonic_ns: u64,
        opened_monotonic_ns: u64,""",
            """        opened_monotonic_ns: u64,
        open_start_monotonic_ns: u64,""",
            "shared opened timestamp signature swap",
        ),
        replace_exact_once(
            control,
            """            allocated_bytes_start,
            allocation_calls_start,
            self.context_sha256,
            counter_start_monotonic_ns,
            nonce.as_str(),
            process_system_cpu_start_ns,
            process_user_cpu_start_ns,
            crate::contract::PROTOCOL_SHA256,
            ready_monotonic_ns,
            crate::contract::VARIANT,""",
            """            allocation_calls_start,
            allocated_bytes_start,
            self.context_sha256,
            ready_monotonic_ns,
            nonce.as_str(),
            process_user_cpu_start_ns,
            process_system_cpu_start_ns,
            crate::contract::PROTOCOL_SHA256,
            counter_start_monotonic_ns,
            crate::contract::VARIANT,""",
            "shared ready same-typed tuple swaps",
        ),
        replace_exact_once(
            control,
            """            (
                "process_system_cpu_end_ns",
                json_u64(markers.process_system_cpu_end_ns),
            ),
            (
                "process_user_cpu_end_ns",
                json_u64(markers.process_user_cpu_end_ns),
            ),""",
            """            (
                "process_system_cpu_end_ns",
                json_u64(markers.process_user_cpu_end_ns),
            ),
            (
                "process_user_cpu_end_ns",
                json_u64(markers.process_system_cpu_end_ns),
            ),""",
            "shared measured CPU swap",
        ),
        replace_exact_once(
            control,
            """            (
                "counter_end_monotonic_ns",
                json_u64(markers.counter_end_monotonic_ns),
            ),
            (
                "last_completion_monotonic_ns",
                json_u64(markers.last_completion_monotonic_ns),
            ),""",
            """            (
                "counter_end_monotonic_ns",
                json_u64(markers.last_completion_monotonic_ns),
            ),
            (
                "last_completion_monotonic_ns",
                json_u64(markers.counter_end_monotonic_ns),
            ),""",
            "shared measured counter/completion timestamp swap",
        ),
        replace_exact_once(
            control,
            """            ("t0_monotonic_ns", json_u64(markers.t0_monotonic_ns)),
            ("t1_monotonic_ns", json_u64(markers.t1_monotonic_ns)),""",
            """            ("t0_monotonic_ns", json_u64(markers.t1_monotonic_ns)),
            ("t1_monotonic_ns", json_u64(markers.t0_monotonic_ns)),""",
            "shared measured t0/t1 timestamp swap",
        ),
    )
    for hostile_control in hostile_control_sources:
        try:
            validate_shared_control_child_source(hostile_control)
        except PreparationError:
            pass
        else:
            raise AssertionError("hostile shared control source was accepted")
    assert public.count(
        ".thread_keep_alive(Duration::from_secs(3_600))"
    ) == public.count("tokio::runtime::Builder::new_multi_thread()") == 3
    synthetic_append = """
async fn append_batch() {
    let appended = tokio::task::spawn_blocking(move || 1).await;
}
"""
    validate_c_role_lifetime_sources(public, synthetic_append)
    hostile_lifetime_sources = (
        (
            public.replace(
                "Duration::from_secs(3_600)",
                "Duration::from_secs(3_599)",
                1,
            ),
            synthetic_append,
        ),
        (
            public,
            synthetic_append.replace(
                "let appended =", "std::thread::spawn(|| {}); let appended ="
            ),
        ),
        (
            public,
            synthetic_append.replace(
                "let appended =",
                'let decoy = "}"; std::thread::spawn(|| {}); let appended =',
            ),
        ),
        (
            public,
            synthetic_append.replace(
                ".await;",
                ".await; tokio::task::spawn_blocking(|| 2).await;",
            ),
        ),
    )
    for hostile_public, hostile_engine in hostile_lifetime_sources:
        try:
            validate_c_role_lifetime_sources(hostile_public, hostile_engine)
        except PreparationError:
            pass
        else:
            raise AssertionError("hostile C role lifetime source was accepted")
    for name in (
        "ASTERISM_REBASELINE_PERF_PERMISSION_RESULT",
        "ASTERISM_REBASELINE_PERF_COMMAND_FD",
        "ASTERISM_REBASELINE_PERF_ACK_FD",
        "ASTERISM_REBASELINE_PERF_ACK_LEDGER_FD",
    ):
        assert control.count(f'"{name}"') == 1, f"perf child env drift: {name}"
    for marker in (
        'mode == "cpu_profiles"',
        'mode == "smoke"',
        'std::env::var("ASTERISM_REBASELINE_SMOKE_TARGET")',
        '== "cpu_profiles")',
        'pipes.command.write_all(b"disable\\n")',
        "pipes.ack.read_exact(&mut ack)",
        'assert_eq!(&ack, b"ack\\n"',
        "pipes.ledger.write_all(&ack)",
        '("perf_disable", perf_disable)',
    ):
        assert marker in control, f"perf child integration marker absent: {marker}"
    assert 'json_string("bn-2l3n-c-role-lifetime-v3")' in contract
    assert '("profile_role_lifetime", profile_role_lifetime())' in contract
    assert "json_u64(3_600_000_000_000)" in contract
    for source in (public, bare, contract, workload):
        assert "-v2" not in source, "active overlay retained a v2 identity"
    expected_templates = {
        "A": {
            "log": [{"kind": "file_prefix", "path": "log/seg-"}],
            "metadata": [
                {"kind": "directory_prefix", "path": "log/sealed/"},
                {"kind": "directory_prefix", "path": "snapshots/blobs/"},
                {"kind": "directory_prefix", "path": "snapshots/meta/"},
            ],
        },
        "B": {
            "log": [{"kind": "exact", "path": "segment-1.log"}],
            "metadata": [],
        },
    }
    for variant in VARIANTS:
        templates = trace_path_marker_templates(variant)
        assert templates["root_environment"] == "ASTERISM_REBASELINE_STORE"
        reference = expected_templates["B" if variant == "B" else "A"]
        assert templates["log"] == reference["log"]
        if variant != "C":
            assert templates["metadata"] == reference["metadata"]
        else:
            assert templates["metadata"][0] == {
                "kind": "directory_prefix",
                "path": "log/meta/",
            }
            assert templates["metadata"][1:] == reference["metadata"]
    hostile_trace_templates = (
        {
            "root_environment": "ASTERISM_REBASELINE_STORE",
            "log": [{"kind": "exact", "path": "/escape"}],
            "metadata": [],
        },
        {
            "root_environment": "ASTERISM_REBASELINE_STORE",
            "log": [{"kind": "exact", "path": "../escape"}],
            "metadata": [],
        },
        {
            "root_environment": "ASTERISM_REBASELINE_STORE",
            "log": [],
            "metadata": [],
        },
        {
            "root_environment": "ASTERISM_REBASELINE_STORE",
            "log": [{"kind": "file_prefix", "path": "log/m"}],
            "metadata": [
                {"kind": "directory_prefix", "path": "log/meta/"}
            ],
        },
        {
            "root_environment": "ASTERISM_REBASELINE_STORE",
            "log": [{"kind": "directory_prefix", "path": "log"}],
            "metadata": [],
        },
    )
    for hostile in hostile_trace_templates:
        try:
            validate_trace_path_marker_templates(hostile)
        except PreparationError:
            pass
        else:
            raise AssertionError("hostile trace marker template was accepted")
    preparer = Path(__file__).read_text()
    for marker in (
        "approval_snapshot, locks, tools = validate_approval(",
        "approval_sha256 = approval_snapshot.sha256",
        "atomic_write(bound_approval_path, approval_snapshot.payload, mode=0o444)",
        '"tools_manifest": tools,',
        '"tools_manifest_sha256": hash_file(tools_path),',
        '"mode": 0o444,\n            "path": str(bound_tools_manifest_path.resolve()),',
        'marker_templates = validate_trace_path_marker_templates(',
        '"trace_path_marker_templates": marker_templates,',
        'validate_attested_child(build_child, f"{variant} build")',
        'validate_attested_child(contract_child, f"{variant} contract")',
    ):
        assert marker in preparer, f"preparer authority marker absent: {marker}"
    build_source = preparer[
        preparer.index("def build(") : preparer.index("def static_self_test(")
    ]
    assert "hash_file(approval_path)" not in build_source
    assert "approval_path.read_bytes()" not in build_source
    assert '"correctness_oracle_mode"' in contract
    assert 'json_bool(BINARY_KIND == "public")' in contract
    with tempfile.TemporaryDirectory(prefix="asterism-static-self-test-") as temporary:
        root = Path(temporary).resolve()

        approval_path = root / "approval.json"
        original_approval = {
            "schema": APPROVAL_SCHEMA,
            "status": "original",
        }
        atomic_write(
            approval_path, canonical_json(original_approval), mode=0o444
        )
        approval_snapshot = immutable_canonical_snapshot(
            approval_path, APPROVAL_SCHEMA, "static approval"
        )
        replacement_path = root / "approval-replacement.json"
        replacement_approval = {
            "schema": APPROVAL_SCHEMA,
            "status": "replacement",
        }
        atomic_write(
            replacement_path, canonical_json(replacement_approval), mode=0o444
        )
        os.replace(replacement_path, approval_path)
        copied_approval_path = root / "approval-copy.json"
        atomic_write(
            copied_approval_path, approval_snapshot.payload, mode=0o444
        )
        copied_approval = immutable_canonical_snapshot(
            copied_approval_path, APPROVAL_SCHEMA, "static copied approval"
        )
        assert copied_approval.value == original_approval
        assert copied_approval.sha256 == approval_snapshot.sha256
        assert immutable_canonical_snapshot(
            approval_path, APPROVAL_SCHEMA, "static replacement approval"
        ).value == replacement_approval

        approval_path.chmod(0o644)
        try:
            immutable_canonical_snapshot(
                approval_path, APPROVAL_SCHEMA, "writable approval"
            )
        except PreparationError:
            pass
        else:
            raise AssertionError("writable approval snapshot was accepted")
        approval_path.chmod(0o444)
        approval_link = root / "approval-link.json"
        approval_link.symlink_to(approval_path)
        try:
            immutable_canonical_snapshot(
                approval_link, APPROVAL_SCHEMA, "symlink approval"
            )
        except PreparationError:
            pass
        else:
            raise AssertionError("symlink approval snapshot was accepted")

        def archive_payload(*entries: tuple[str, bytes]) -> bytes:
            buffer = io.BytesIO()
            with tarfile.open(fileobj=buffer, mode="w") as archive:
                for name, member_type in entries:
                    member = tarfile.TarInfo(name)
                    member.type = member_type
                    payload = b"archive\n"
                    member.size = (
                        len(payload) if member_type == tarfile.REGTYPE else 0
                    )
                    archive.addfile(
                        member,
                        (
                            io.BytesIO(payload)
                            if member_type == tarfile.REGTYPE
                            else None
                        ),
                    )
            return buffer.getvalue()

        hostile_archives = (
            archive_payload(("../escape", tarfile.REGTYPE)),
            archive_payload(("./alias", tarfile.REGTYPE)),
            archive_payload(("alias//child", tarfile.REGTYPE)),
            archive_payload(("alias/./child", tarfile.REGTYPE)),
            archive_payload(("link", tarfile.SYMTYPE)),
            archive_payload(
                ("duplicate-directory", tarfile.DIRTYPE),
                ("duplicate-directory/", tarfile.DIRTYPE),
            ),
            archive_payload(
                ("alias", tarfile.REGTYPE),
                ("./alias", tarfile.REGTYPE),
            ),
        )
        for ordinal, archive in enumerate(hostile_archives, start=1):
            destination = root / f"hostile-archive-{ordinal}"
            try:
                extract_archive_payload(archive, destination)
            except PreparationError:
                assert not destination.exists(), (
                    "hostile archive mutated its destination before full validation"
                )
            else:
                raise AssertionError("hostile archive member was accepted")

        canonical_archive = archive_payload(
            (
                "canonical-directory/",
                tarfile.DIRTYPE,
            ),
            ("canonical-directory/file", tarfile.REGTYPE),
        )
        canonical_destination = root / "canonical-archive"
        assert extract_archive_payload(
            canonical_archive, canonical_destination
        ) == ["canonical-directory/file"]
        assert (canonical_destination / "canonical-directory" / "file").is_file()

        runtime_environment = {
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/bin:/bin",
            "TZ": "UTC",
        }
        python = str(Path(sys.executable).resolve())
        child, stdout, stderr = run_attested(
            [python, "-I", "-c", "import os; os.write(1, b'ok')"],
            cwd=root,
            env=runtime_environment,
            output_path=root / "attested-ok.json",
            raw_stdout=True,
            timeout=5,
        )
        assert child["exit_status"] == 0 and stdout == b"ok" and stderr == b""
        escaped_program = (
            "import os,time; pid=os.fork(); "
            "(os.setsid(),time.sleep(30)) if pid == 0 else os._exit(0)"
        )
        try:
            run_attested(
                [python, "-I", "-c", escaped_program],
                cwd=root,
                env=runtime_environment,
                output_path=root / "attested-escaped.json",
                raw_stdout=True,
                timeout=5,
            )
        except PreparationError as error:
            assert "escaped descendant" in str(error)
        else:
            raise AssertionError("escaped setsid descendant was accepted")
        try:
            run_attested(
                [
                    python,
                    "-I",
                    "-c",
                    "import os; os.write(1, b'x' * 2048)",
                ],
                cwd=root,
                env=runtime_environment,
                output_path=root / "attested-oversize.json",
                raw_stdout=True,
                timeout=5,
                max_output_bytes=1024,
            )
        except PreparationError as error:
            assert "output byte limit" in str(error)
        else:
            raise AssertionError("oversized attested output was accepted")

        continuous_started = time.monotonic()
        continuous_child, continuous_stdout, continuous_stderr = run_attested(
            [
                python,
                "-I",
                "-c",
                "import os\nwhile True:\n os.write(1, b'x')",
            ],
            cwd=root,
            env=runtime_environment,
            output_path=root / "attested-continuous.json",
            raw_stdout=True,
            timeout=0.25,
        )
        continuous_elapsed = time.monotonic() - continuous_started
        assert continuous_child["timed_out"] is True
        assert continuous_child["exit_status"] < 0
        assert continuous_stderr == b""
        assert 0 < len(continuous_stdout) <= ATTESTED_OUTPUT_LIMIT
        assert continuous_elapsed < (
            1.0 + ATTESTED_CLEANUP_SECONDS + ATTESTED_DRAIN_SECONDS
        ), "continuously readable attested output defeated its wall deadline"

        tool_bindings: dict[str, Any] = {}
        support_bindings: dict[str, Any] = {}
        for ordinal, name in enumerate(sorted(REQUIRED_TOOLS)):
            source = root / f"tool-{ordinal}"
            atomic_write(source, f"tool:{name}\n".encode(), mode=0o555)
            tool_bindings[name] = {
                "comm": REQUIRED_TOOL_COMMS[name],
                "executable_mode": 0o555,
                "path": str(source),
                "sha256": hash_file(source),
            }
        for name in sorted(REQUIRED_SUPPORT_FILES):
            source = root / REQUIRED_SUPPORT_BASENAMES[name]
            atomic_write(source, f"support:{name}\n".encode(), mode=0o444)
            support_bindings[name] = {
                "mode": 0o444,
                "path": str(source),
                "sha256": hash_file(source),
            }
        fixture = {
            "comm_allowlist": COMM_ALLOWLIST,
            "schema": "asterism-rebaseline-tools-v3",
            "support_files": support_bindings,
            "tools": tool_bindings,
        }

        def write_fixture(name: str, value: dict[str, Any]) -> Path:
            path = root / name
            atomic_write(path, canonical_json(value), mode=0o444)
            return path

        def rejects(value: dict[str, Any]) -> bool:
            try:
                validate_tools_manifest(write_fixture(
                    f"mutation-{secrets.token_hex(4)}.json", value
                ))
            except PreparationError:
                return True
            return False

        manifest_path = write_fixture("tools.json", fixture)
        assert validate_tools_manifest(manifest_path) == fixture
        assert hash_file(manifest_path) == hash_bytes(canonical_json(fixture))
        mutated = json.loads(json.dumps(fixture))
        mutated["schema"] = "asterism-rebaseline-tools-v2"
        assert rejects(mutated)
        mutated = json.loads(json.dumps(fixture))
        mutated["tools"]["correctness"]["comm"] = "unapproved"
        assert rejects(mutated)
        mutated = json.loads(json.dumps(fixture))
        mutated["support_files"]["runner"]["sha256"] = "0" * SHA256
        assert rejects(mutated)
        mutated = json.loads(json.dumps(fixture))
        del mutated["support_files"]["runner"]
        assert rejects(mutated)
        mutated = json.loads(json.dumps(fixture))
        mutated["support_files"]["runner"] = dict(
            mutated["support_files"]["evaluator"]
        )
        assert rejects(mutated)
        runner_path = Path(fixture["support_files"]["runner"]["path"])
        runner_path.chmod(0o644)
        assert rejects(fixture)
        runner_path.chmod(0o444)
    print(json.dumps({
        "schema": "asterism-rebaseline-prepare-static-self-test-v3",
        "status": "ok",
    }, sort_keys=True, separators=(",", ":")))


def self_test(repository: Path) -> None:
    toolchain = toolchain_identity()
    plan = load_plan(repository, toolchain)
    cargo_environment = frozen_cargo_environment(toolchain)
    assert canonical_json(json.loads(canonical_json(plan))) == canonical_json(plan)
    assert set(toolchain) == TOOLCHAIN_FIELDS
    assert set(cargo_environment) == FROZEN_CARGO_ENV_FIELDS
    assert Path(toolchain["cargo_path"]).parent == Path(toolchain["rustc_path"]).parent
    assert toolchain["cargo_sha256"] != toolchain["rustup_sha256"]
    assert toolchain["rustc_sha256"] != toolchain["rustup_sha256"]
    assert not ({
        "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
    } & set(cargo_environment))
    config_search = cargo_config_search(
        repository, Path(toolchain["cargo_home_path"])
    )
    assert config_search["schema"] == CARGO_CONFIG_SCHEMA
    assert canonical_json(json.loads(canonical_json(config_search))) == canonical_json(
        config_search
    )
    assert filesystem_admission(repository)["schema"] == FILESYSTEM_ADMISSION_SCHEMA
    static_self_test()
    print(json.dumps({"schema": "asterism-rebaseline-prepare-self-test-v3", "status": "ok"}, sort_keys=True, separators=(",", ":")))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=("self-test", "static-self-test", "stage-locks", "write-approval", "build"),
    )
    parser.add_argument("--repository", type=Path, default=HERE.parents[2])
    parser.add_argument("--output", type=Path)
    parser.add_argument("--approval", type=Path)
    parser.add_argument("--lock-manifest", type=Path)
    parser.add_argument("--tools", type=Path)
    parser.add_argument("--review-id")
    parser.add_argument("--reviewed-at")
    arguments = parser.parse_args()
    repository = arguments.repository.resolve(strict=True)
    if arguments.command == "self-test":
        self_test(repository)
    elif arguments.command == "static-self-test":
        static_self_test()
    elif arguments.command == "stage-locks":
        if arguments.output is None:
            parser.error("stage-locks requires --output")
        stage_locks(repository, arguments.output.resolve())
    elif arguments.command == "write-approval":
        for name in (
            "output", "lock_manifest", "tools", "review_id", "reviewed_at"
        ):
            if getattr(arguments, name) is None:
                parser.error(f"write-approval requires --{name.replace('_', '-')}")
        write_approval(
            repository,
            arguments.lock_manifest.resolve(strict=True),
            arguments.tools.resolve(strict=True),
            arguments.output.resolve(),
            arguments.review_id,
            arguments.reviewed_at,
        )
    else:
        for name in ("output", "approval", "lock_manifest", "tools"):
            if getattr(arguments, name) is None:
                parser.error(f"build requires --{name.replace('_', '-')}")
        build(
            repository, arguments.output.resolve(), arguments.approval.resolve(strict=True),
            arguments.lock_manifest.resolve(strict=True), arguments.tools.resolve(strict=True),
        )


if __name__ == "__main__":
    try:
        main()
    except (OSError, subprocess.SubprocessError, PreparationError, ValueError, json.JSONDecodeError) as error:
        print(f"prepare-overlays: {error}", file=os.sys.stderr)
        raise SystemExit(2) from error
