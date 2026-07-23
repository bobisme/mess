#!/usr/bin/env python3
"""Synthetic positive and mutation-negative tests for profile adapters."""

from __future__ import annotations

import importlib.util
import hashlib
import json
import math
import os
import shutil
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Callable
from unittest import mock


MODULE_PATH = Path(__file__).with_name("profile_adapters.py")
SPEC = importlib.util.spec_from_file_location("bn2l3n_profile_adapters", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
adapters = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = adapters
SPEC.loader.exec_module(adapters)


# Local fixture constants.  These used to be read from profile_adapters, but
# the redundant Phase-4 source-review re-validation (and its constants) was
# removed from the adapter; the canonical validators live in
# evidence_schema.py / evaluate.py.  The fixture still fabricates the same
# evidence shapes, so the literals are pinned here.
GUEST_ROOT = "/asterism"
GUEST_SOURCE = f"{GUEST_ROOT}/source"
GUEST_TOOLCHAIN_ROOT = f"{GUEST_ROOT}/toolchain"
GUEST_TOOLCHAIN_BIN = f"{GUEST_TOOLCHAIN_ROOT}/bin"
GUEST_RUSTC = f"{GUEST_TOOLCHAIN_ROOT}/bin/rustc"
GUEST_CARGO_HOME = f"{GUEST_ROOT}/cargo-home"
GUEST_BOUND_CONFIG_PATHS = (
    f"{GUEST_SOURCE}/.cargo/config.toml",
    f"{GUEST_SOURCE}/.cargo/config",
    f"{GUEST_CARGO_HOME}/config.toml",
    f"{GUEST_CARGO_HOME}/config",
)
CARGO_CONFIG_SEARCH_SCHEMA = "asterism-rebaseline-cargo-config-search-v3"
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
RELEASE_BUILD_ENVIRONMENT_FIELDS = frozenset(
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
SEMANTIC_INPUT_AUTHORITY_SCHEMA = "bn-ecm1-semantic-input-authority-v1"
RECURSIVE_TREE_AUTHORITY_SCHEMA = "bn-ecm1-recursive-tree-authority-v1"
TRUSTED_SYSTEM_CLOSURE_SCHEMA = "bn-ecm1-trusted-system-closure-v1"
LOCK_CANDIDATES_SCHEMA = "asterism-rebaseline-lock-candidates-v3"
CURRENT_CARGO_CONFIG_SCHEMA = "bn-30fs-build-cargo-config-search-v1"
CURRENT_WRAPPER_RECEIPT_SCHEMA = "bn-30fs-rustc-workspace-wrapper-receipt-v1"
CURRENT_WRAPPER_ARGUMENTS = (
    "--cfg",
    "test",
    "--allow",
    "explicit_builtin_cfgs_in_flags",
    "--cfg",
    "asterism_rebaseline_correctness",
    "--check-cfg",
    "cfg(asterism_rebaseline_correctness)",
)
CURRENT_FAULT_COMPILE_OUT_SCHEMA = "bn-2l3n-fault-compile-out-authority-v1"
CURRENT_EXPECTED_LIB_SOURCE = "crates/mess-store/src/lib.rs"
# Patched per-test to fixture mounts (see fixture_trusted_mounts).
TRUSTED_SYSTEM_MOUNTS: tuple[tuple[Path, str], ...] = ()


def stat_record(identity: int, comm: str, start_ticks: int) -> str:
    fields = ["S"] + [str(index) for index in range(4, 22)] + [str(start_ticks)]
    return f"{identity} ({comm}) {' '.join(fields)}\n"


def io_record(offset: int = 0) -> str:
    return "".join(
        f"{name}: {offset + index}\n"
        for index, name in enumerate(adapters.IO_FIELDS, start=1)
    )


def write_task(
    root: Path,
    pid: int,
    tid: int,
    comm: str,
    start_ticks: int,
    *,
    on_cpu_ns: int = 0,
    voluntary: int = 0,
    nonvoluntary: int = 0,
) -> None:
    task = root / str(pid) / "task" / str(tid)
    task.mkdir(parents=True, exist_ok=True)
    (task / "stat").write_text(stat_record(tid, comm, start_ticks))
    (task / "schedstat").write_text(f"{on_cpu_ns} 0 0\n")
    (task / "status").write_text(
        f"Name: {comm}\n"
        f"voluntary_ctxt_switches: {voluntary}\n"
        f"nonvoluntary_ctxt_switches: {nonvoluntary}\n"
    )


def sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def trace_marker(kind: str, path: str) -> dict[str, str]:
    return {"kind": kind, "path": path}


def write_canonical(path: Path, value: object, mode: int) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = adapters.canonical_json(value)
    path.write_bytes(payload)
    path.chmod(mode)
    return sha256(payload)


def release_file_binding(path: Path) -> dict[str, object]:
    metadata = path.stat()
    return {
        "path": str(path.resolve()),
        "sha256": sha256(path.read_bytes()),
        "size": metadata.st_size,
        "mode": stat.S_IMODE(metadata.st_mode),
        "identity": {
            "changed_ns": metadata.st_ctime_ns,
            "device": metadata.st_dev,
            "inode": metadata.st_ino,
            "link_count": metadata.st_nlink,
            "modified_ns": metadata.st_mtime_ns,
        },
    }


def review_input_binding(path: Path, schema: str) -> dict[str, object]:
    binding = release_file_binding(path)
    return {**binding, "schema": schema}


def sandbox_environment(
    toolchain: dict[str, object], *, release: bool = False
) -> dict[str, str]:
    environment = {
        "CARGO_HOME": GUEST_CARGO_HOME,
        "CARGO_INCREMENTAL": "0",
        "CARGO_NET_OFFLINE": "true",
        "GIT_CONFIG_COUNT": "0",
        "GIT_CONFIG_GLOBAL": f"{GUEST_ROOT}/absent-gitconfig",
        "GIT_CONFIG_NOSYSTEM": "1",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": f"{GUEST_TOOLCHAIN_ROOT}/bin:/usr/bin:/bin",
        "RUSTC": GUEST_RUSTC,
        "RUSTUP_HOME": "/nonexistent",
        "RUSTUP_TOOLCHAIN": str(toolchain["rustup_toolchain"]),
        "TZ": "UTC",
    }
    if release:
        environment["LD_ORIGIN_PATH"] = GUEST_TOOLCHAIN_BIN
        environment.update(
            {name: f"synthetic-{name.lower()}" for name in RELEASE_BUILD_ENVIRONMENT_FIELDS}
        )
    return environment


def current_sandbox_environment(toolchain: dict[str, object]) -> dict[str, str]:
    environment = sandbox_environment(toolchain)
    environment["LD_ORIGIN_PATH"] = GUEST_TOOLCHAIN_BIN
    environment["PATH"] = "/usr/bin:/bin"
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTHONNOUSERSITE"] = "1"
    return environment


def current_file_identity(path: Path, *, logical_path: str | None = None) -> dict[str, object]:
    metadata = path.stat()
    payload = path.read_bytes()
    return {
        "bytes": metadata.st_size,
        "ctime_ns": metadata.st_ctime_ns,
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "link_count": metadata.st_nlink,
        "mode": stat.S_IMODE(metadata.st_mode),
        "mtime_ns": metadata.st_mtime_ns,
        "path": logical_path or str(path.resolve()),
        "sha256": sha256(payload),
        "size": metadata.st_size,
    }


def current_directory_identity(path: Path) -> dict[str, object]:
    metadata = path.stat()
    return {
        "changed_ns": metadata.st_ctime_ns,
        "device": metadata.st_dev,
        "file_type": stat.S_IFMT(metadata.st_mode),
        "inode": metadata.st_ino,
        "link_count": metadata.st_nlink,
        "modified_ns": metadata.st_mtime_ns,
        "path": str(path.resolve()),
        "permissions": stat.S_IMODE(metadata.st_mode),
        "size": metadata.st_size,
    }


def current_path_chain(path: Path) -> list[dict[str, object]]:
    selected = Path("/")
    paths = [selected]
    for part in path.parts[1:]:
        selected /= part
        paths.append(selected)
    records = []
    for selected in paths:
        metadata = selected.lstat()
        records.append(
            {
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
        )
    return records


def current_retained_file(path: Path, *, trusted: bool) -> dict[str, object]:
    exact = path.resolve(strict=True)
    return {
        "identity": current_file_identity(exact),
        "path_chain": current_path_chain(exact) if trusted else None,
        "trusted_system": trusted,
    }


def retained_null_device() -> dict[str, object]:
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
        "parent_path_chain": current_path_chain(path.parent),
        "trusted_system": True,
    }


def prepare_tree(root: Path, name: str, payload: bytes = b"semantic input\n") -> None:
    root.mkdir(parents=True)
    entry = root / name
    entry.write_bytes(payload)
    entry.chmod(0o444)
    root.chmod(0o555)


def write_cargo_config_manifest(
    path: Path, source_root: Path, cargo_home: Path
) -> str:
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
        (
            f"{GUEST_CARGO_HOME}/config.toml",
            cargo_home / "config.toml",
        ),
        (f"{GUEST_CARGO_HOME}/config", cargo_home / "config"),
    )
    entries = []
    for guest, host in candidates:
        if host is None:
            entries.append({"path": guest, "status": "absent", "sha256": None})
        else:
            digest = sha256(host.read_bytes()) if host.exists() else sha256(b"")
            entries.append({"path": guest, "status": "present", "sha256": digest})
    digest = write_canonical(
        path,
        {
            "schema": CARGO_CONFIG_SEARCH_SCHEMA,
            "cargo_home_path": GUEST_CARGO_HOME,
            "cwd": GUEST_SOURCE,
            "entries": entries,
        },
        0o444,
    )
    empty = path.with_name(f"{path.name}.empty")
    empty.write_bytes(b"")
    empty.chmod(0o444)
    return digest


def fixture_recursive_manifest(
    root: Path,
    role: str,
    *,
    hash_regular_contents: bool,
    excluded_relative_paths: tuple[str, ...] = (),
    volatile_directory_metadata_paths: tuple[str, ...] = (),
) -> dict[str, object]:
    entries: list[dict[str, object]] = []

    def record(path: Path, relative: str, kind: str) -> dict[str, object]:
        metadata = path.lstat()
        item: dict[str, object] = {
            "changed_ns": metadata.st_ctime_ns,
            "device": metadata.st_dev,
            "file_type": kind,
            "gid": metadata.st_gid,
            "inode": metadata.st_ino,
            "link_count": metadata.st_nlink,
            "modified_ns": metadata.st_mtime_ns,
            "path": relative,
            "permissions": stat.S_IMODE(metadata.st_mode),
            "sha256": (
                sha256(path.read_bytes())
                if kind == "regular" and hash_regular_contents
                else None
            ),
            "size": metadata.st_size,
            "symlink_target": None,
            "symlink_scope": None,
            "uid": metadata.st_uid,
        }
        if kind == "directory" and relative in volatile_directory_metadata_paths:
            for field in ("changed_ns", "modified_ns", "permissions", "size"):
                item[field] = 0
        return item

    def walk(directory: Path, relative: str) -> None:
        entries.append(record(directory, relative, "directory"))
        for child in sorted(directory.iterdir(), key=lambda path: path.name):
            child_relative = child.name if relative == "." else f"{relative}/{child.name}"
            if child_relative in excluded_relative_paths:
                continue
            metadata = child.lstat()
            if stat.S_ISDIR(metadata.st_mode):
                walk(child, child_relative)
            elif stat.S_ISREG(metadata.st_mode):
                entries.append(record(child, child_relative, "regular"))
            else:
                raise AssertionError(f"fixture has an unsupported node: {child}")

    walk(root, ".")
    return {
        "entries": entries,
        "role": role,
        "schema": RECURSIVE_TREE_AUTHORITY_SCHEMA,
    }


def fixture_semantic_runtime_sha256(authority: dict[str, object]) -> str:
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
    normalized = {
        "cargo_home": {
            field: authority["cargo_home"][field] for field in tree_fields
        },
        "schema": SEMANTIC_INPUT_AUTHORITY_SCHEMA,
        "toolchain": {
            field: authority["toolchain"][field] for field in tree_fields
        },
        "trusted_system_closure": {
            field: authority["trusted_system_closure"][field]
            for field in closure_fields
        },
    }
    return sha256(adapters.canonical_json(normalized))


def semantic_authority(
    *,
    source_root: Path,
    toolchain_root: Path,
    cargo_home: Path,
    paths: dict[str, Path],
    source_role: str = "source",
) -> dict[str, object]:
    specifications = (
        (
            "source",
            source_root,
            source_role,
            True,
            ("Cargo.lock",)
            if source_role == "resolution_source_without_cargo_lock"
            else (),
            (".",) if source_role == "resolution_source_without_cargo_lock" else (),
        ),
        ("toolchain", toolchain_root, "toolchain", True, (), ()),
        ("cargo_home", cargo_home, "cargo_home", True, (), ()),
    )
    authority: dict[str, object] = {
        "schema": SEMANTIC_INPUT_AUTHORITY_SCHEMA
    }
    for name, root, role, hash_contents, excluded, volatile in specifications:
        manifest = fixture_recursive_manifest(
            root,
            role,
            hash_regular_contents=hash_contents,
            excluded_relative_paths=excluded,
            volatile_directory_metadata_paths=volatile,
        )
        digest = write_canonical(paths[name], manifest, 0o444)
        entries = manifest["entries"]
        authority[name] = {
            "entry_count": len(entries),
            "equal_pre_post": True,
            "manifest_path": str(paths[name]),
            "manifest_sha256": digest,
            "mutation_events_absent": True,
            "role": role,
            "schema": RECURSIVE_TREE_AUTHORITY_SCHEMA,
            "watch_count": sum(
                entry["file_type"] == "directory" for entry in entries
            ),
        }
    roots = tuple(path.resolve(strict=True) for path, _guest in TRUSTED_SYSTEM_MOUNTS)
    evidence_mounts = []
    binding_mounts = []
    closure_entries = 0
    closure_watches = 0
    for (host, guest), resolved in zip(
        TRUSTED_SYSTEM_MOUNTS, roots, strict=True
    ):
        role = "system-" + guest.removeprefix("/").replace("/", "-")
        tree = fixture_recursive_manifest(
            resolved,
            role,
            hash_regular_contents=False,
        )
        root_entry = tree["entries"][0]
        evidence_mounts.append(
            {
                "guest_path": guest,
                "host_path": str(host),
                "resolved_path": str(resolved),
                "tree": tree,
            }
        )
        binding_mounts.append(
            {
                "device": root_entry["device"],
                "gid": root_entry["gid"],
                "guest_path": guest,
                "host_path": str(host),
                "inode": root_entry["inode"],
                "permissions": root_entry["permissions"],
                "resolved_path": str(resolved),
                "trusted_root_owned_non_writable": True,
                "uid": root_entry["uid"],
            }
        )
        closure_entries += len(tree["entries"])
        closure_watches += sum(
            entry["file_type"] == "directory" for entry in tree["entries"]
        )
    closure_value = {
        "mounts": evidence_mounts,
        "schema": TRUSTED_SYSTEM_CLOSURE_SCHEMA,
    }
    closure_sha = write_canonical(paths["closure"], closure_value, 0o444)
    closure = {
        "entry_count": closure_entries,
        "manifest_path": str(paths["closure"]),
        "mounts": binding_mounts,
        "mutation_events_absent": True,
        "schema": TRUSTED_SYSTEM_CLOSURE_SCHEMA,
        "sha256": closure_sha,
        "watch_count": closure_watches,
    }
    authority["trusted_system_closure"] = closure
    authority["runtime_sha256"] = fixture_semantic_runtime_sha256(authority)
    return authority


def semantic_paths(root: Path, label: str, *, resolver: bool = False) -> dict[str, Path]:
    if resolver:
        return {
            "source": root / f"resolution-source-{label}.json",
            "toolchain": root / f"resolution-toolchain-{label}.json",
            "cargo_home": root / f"resolution-cargo-home-{label}.json",
            "closure": root / f"resolution-{label}-system-closure.json",
        }
    return {
        "source": root / f"semantic-source-{label}.json",
        "toolchain": root / f"semantic-toolchain-{label}.json",
        "cargo_home": root / f"semantic-cargo-home-{label}.json",
        "closure": root / f"{label}-system-closure.json",
    }


def fixture_trusted_mounts(root: Path) -> tuple[tuple[Path, str], ...]:
    return (
        (root / "trusted" / "usr-bin", "/usr/bin"),
        (root / "trusted" / "usr-lib", "/usr/lib"),
        (root / "trusted" / "usr-include", "/usr/include"),
    )


def release_attestation(
    *,
    prepared_root: Path,
    label: str,
    source_root: Path,
    toolchain: dict[str, object],
    package: str,
    example: str,
    descriptors: range,
    source_approval_sha256: str,
    contract: dict[str, object],
) -> dict[str, object]:
    paths = semantic_paths(prepared_root / "manifests", label)
    semantic = semantic_authority(
        source_root=source_root,
        toolchain_root=Path(str(toolchain["cargo_path"])).parent.parent,
        cargo_home=Path(str(toolchain["cargo_home_path"])),
        paths=paths,
    )
    config_path = prepared_root / "manifests" / f"cargo-config-{label}.json"
    config_sha = write_cargo_config_manifest(
        config_path, source_root, Path(str(toolchain["cargo_home_path"]))
    )
    argv = [
        str(toolchain["bwrap_path"]),
        "synthetic-release-sandbox",
        package,
        example,
    ]
    build_env = sandbox_environment(toolchain, release=True)
    build_env["ASTERISM_BUILD_SOURCE_APPROVAL_SHA256"] = source_approval_sha256
    normalized = list(argv)
    for index, argument in enumerate(normalized):
        if argument in {"--ro-bind-fd", "--bind-fd"}:
            normalized[index + 1] = f"$FD:{normalized[index + 2]}"
        elif argument == "--dev-bind":
            normalized[index + 1] = "$FD:/dev/null"
        elif argument == "--overlay-src":
            normalized[index + 1] = "$FD:cargo-home-overlay"
    sequence = {
        "build-A": 1,
        "build-B": 3,
        "build-C": 5,
        "build-D": 7,
        "build-A-product-overlay": 10,
    }[label]
    build_log_path = prepared_root / "logs" / f"{label}.json"
    build_log_sha256 = write_canonical(
        build_log_path,
        {
            "exit_status": 0,
            "stderr": "",
            "stderr_sha256": sha256(b""),
            "stdout": "",
            "stdout_sha256": sha256(b""),
        },
        0o444,
    )
    pid = 400 + sequence
    started_at = f"2026-07-16T00:00:{sequence:02d}+00:00"
    completed_at = f"2026-07-16T00:00:{sequence + 1:02d}+00:00"
    build_child = {
        "argv": argv,
        "completed_at": completed_at,
        "completed_monotonic_ns": sequence + 1,
        "cwd": str(source_root),
        "exit_status": 0,
        "output_path": str(build_log_path),
        "output_sha256": build_log_sha256,
        "pid": pid,
        "passed_file_descriptors": 16,
        "process_group_absent": True,
        "reaping": {"pid": pid, "start_ticks": pid + 1, "status": "absent"},
        "start_ticks": pid + 1,
        "started_at": started_at,
        "started_monotonic_ns": sequence,
        "timed_out": False,
        "waited_pid": pid,
    }
    contract_output = prepared_root / "manifests" / f"contract-{label}.json"
    contract_output_sha256 = write_canonical(contract_output, contract, 0o444)
    toolchain_root = Path(str(toolchain["cargo_path"])).parent.parent
    rust_lld_path = (
        toolchain_root
        / "lib"
        / "rustlib"
        / str(toolchain["rustc_host"])
        / "bin"
        / "rust-lld"
    )
    root_metadata = toolchain_root.lstat()
    execution_tools = {
        "bwrap": release_file_binding(Path(str(toolchain["bwrap_path"]))),
        "cargo": release_file_binding(Path(str(toolchain["cargo_path"]))),
        "dev_null": retained_null_device(),
        "rustc": release_file_binding(Path(str(toolchain["rustc_path"]))),
        "rust_lld": release_file_binding(rust_lld_path),
        "toolchain_root": {
            "device": root_metadata.st_dev,
            "inode": root_metadata.st_ino,
            "link_count": root_metadata.st_nlink,
            "mode": stat.S_IMODE(root_metadata.st_mode),
        },
    }
    attestation: dict[str, object] = {
        "source_commit": "1" * 40,
        "source_tree": "2" * 40,
        "source_archive_path": str(prepared_root / "archives" / f"{label}.tar"),
        "source_archive_sha256": "1" * 64,
        "source_archive_bytes": 1,
        "archive_manifest_path": str(
            prepared_root / "manifests" / f"archive-{label}.json"
        ),
        "archive_manifest_sha256": "2" * 64,
        "overlay_manifest_path": str(
            prepared_root / "manifests" / f"overlay-{label}.json"
        ),
        "overlay_manifest_sha256": "3" * 64,
        "materialized_root": str(source_root),
        "materialized_manifest_path": str(
            prepared_root / "manifests" / f"materialized-{label}.json"
        ),
        "materialized_manifest_sha256": "4" * 64,
        "materialized_manifest_pre_sha256": "4" * 64,
        "materialized_manifest_post_sha256": "4" * 64,
        "source_read_only": True,
        "cargo_lock_path": str(source_root / "Cargo.lock"),
        "cargo_lock_sha256": "5" * 64,
        "cargo_lock_pre_sha256": "5" * 64,
        "cargo_lock_post_sha256": "5" * 64,
        "target_dir": str(prepared_root / "targets" / label),
        "target_dir_was_absent": True,
        "build_nonce": "4" * 64,
        "build_argv": argv,
        "build_env": build_env,
        "cargo_config_search": {
            "path": str(config_path),
            "sha256": config_sha,
        },
        "execution_tools": execution_tools,
        "semantic_input_authority": semantic,
        "toolchain": toolchain,
        "build_started_at": started_at,
        "build_started_monotonic_ns": sequence,
        "build_completed_at": completed_at,
        "build_completed_monotonic_ns": sequence + 1,
        "build_log_path": str(build_log_path),
        "build_log_sha256": build_log_sha256,
        "build_child": build_child,
        "contract_output_path": str(contract_output),
        "contract_output_sha256": contract_output_sha256,
        "contract_child": {},
    }
    attestation["_sandbox_sha256"] = sha256(
        adapters.canonical_json(
            {
                "argv": normalized,
                "cargo_config_search_sha256": config_sha,
                "execution_tools_sha256": sha256(
                    adapters.canonical_json(execution_tools)
                ),
                "semantic_runtime_sha256": semantic["runtime_sha256"],
            }
        )
    )
    return attestation


def nm_child(
    *,
    name: str,
    pid: int,
    nm_path: Path,
    inventory: Path,
    prepared_root: Path,
) -> dict[str, object]:
    log_path = prepared_root / "logs" / f"nm-{name.replace('_', '-')}.json"
    inventory_payload = inventory.read_bytes()
    write_canonical(
        log_path,
        {
            "exit_status": 0,
            "stderr": "",
            "stderr_sha256": sha256(b""),
            "stdout": inventory_payload.decode(),
            "stdout_sha256": sha256(inventory_payload),
        },
        0o444,
    )
    return {
        "argv": [
            str(nm_path.resolve()),
            "--defined-only",
            "--demangle=rust",
            "--format=posix",
            f"/proc/self/fd/{pid + 20}",
        ],
        "completed_at": "2026-07-16T00:00:01+00:00",
        "completed_monotonic_ns": 2,
        "cwd": str(prepared_root),
        "exit_status": 0,
        "output_path": str(log_path.resolve()),
        "output_sha256": sha256(log_path.read_bytes()),
        "pid": pid,
        "process_group_absent": True,
        "reaping": {"pid": pid, "start_ticks": pid + 1, "status": "absent"},
        "start_ticks": pid + 1,
        "started_at": "2026-07-16T00:00:00+00:00",
        "started_monotonic_ns": 1,
        "timed_out": False,
        "waited_pid": pid,
    }


def profile_tools_for(track: str, root: Path | None = None) -> dict[str, object]:
    names = (
        ("perf",)
        if track == "cpu_profiles"
        else ("strace", "strace_launcher_runtime")
        if track in {"syscall_profiles", "structural_traces"}
        else ()
    )
    tools: dict[str, object] = {}
    for name in names:
        path = (root / name if root is not None else Path(f"/authority/{name}"))
        digest = "b" * 64
        if root is not None:
            path.write_bytes(f"{name}\n".encode())
            path.chmod(0o555)
            digest = sha256(path.read_bytes())
        tools[name] = {
            "path": str(path),
            "sha256": digest,
            "executable_mode": 0o555,
            "comm": name[:15],
        }
    return tools


def synthetic_authority(
    variant: str,
    track: str,
    context: dict[str, object],
    *,
    pid: int = 10,
    start_ticks: int = 100,
    comm: str = "bench",
) -> dict[str, object]:
    source = adapters.VARIANT_SOURCE_BINDINGS[variant]
    role_lifetime: object = (
        adapters.C_ROLE_LIFETIME_CONTRACT if variant == "C" else "not_applicable"
    )
    return {
        "schema": adapters.AUTHORITY_SCHEMA,
        "protocol": adapters.PROTOCOL,
        "protocol_sha256": adapters.PROTOCOL_SHA256,
        "attempt_nonce": "a" * 64,
        "child_ordinal": 1,
        "row_ordinal": 1,
        "context_sha256": sha256(adapters.canonical_json(context)),
        "prepared_artifacts_path": "/authority/prepared-artifacts.json",
        "prepared_artifacts_sha256": "c" * 64,
        "source_approval_path": "/authority/source-approval.json",
        "source_approval_sha256": "d" * 64,
        "profile_adapter_path": "/authority/profile_adapters.py",
        "profile_adapter_sha256": "e" * 64,
        "profile_tools": profile_tools_for(track),
        "perf_permission_result": (
            "available;perf_event_paranoid=2;scope=user-only"
            if track == "cpu_profiles"
            else "not_applicable"
        ),
        "variant": variant,
        "source_commit": source["commit"],
        "source_tree": source["tree"],
        "track": track,
        "executable_path": "/authority/bench",
        "executable_sha256": "f" * 64,
        "executable_mode": 0o555,
        "executable_comm": comm,
        "child_pid": pid,
        "child_start_ticks": start_ticks,
        "control_fd": 9,
    }


def control_events(
    authority: dict[str, object],
    *,
    reopen: bool = False,
    perf_available: bool = True,
) -> list[dict[str, object]]:
    nonce1, nonce2, nonce3, nonce4 = (character * 64 for character in "1234")
    timestamp = 10

    def child(phase: str, nonce: str | None = None) -> dict[str, object]:
        nonlocal timestamp
        if phase == "boot":
            value: dict[str, object] = {
                "context_sha256": authority["context_sha256"],
                "phase": phase,
                "protocol_sha256": adapters.PROTOCOL_SHA256,
                "variant": authority["variant"],
            }
        elif phase == "ready":
            ready_ns = 41 if reopen else 61
            value = {
                "allocated_bytes_start": 0,
                "allocation_calls_start": 0,
                "context_sha256": authority["context_sha256"],
                "counter_start_monotonic_ns": ready_ns + 1,
                "nonce": nonce,
                "phase": phase,
                "process_system_cpu_start_ns": 10,
                "process_user_cpu_start_ns": 10,
                "protocol_sha256": adapters.PROTOCOL_SHA256,
                "ready_monotonic_ns": ready_ns,
                "variant": authority["variant"],
            }
        elif phase == "opened" and reopen:
            value = {
                "nonce": nonce,
                "opened_monotonic_ns": 62,
                "open_start_monotonic_ns": 61,
                "phase": phase,
            }
        elif phase == "measured":
            value = {
                "allocated_bytes_end": 0,
                "allocation_calls_end": 0,
                "counter_end_monotonic_ns": 88,
                "last_completion_monotonic_ns": 84,
                "nonce": nonce,
                "phase": phase,
                "process_system_cpu_end_ns": 20,
                "process_user_cpu_end_ns": 30,
                "release_monotonic_ns": 81,
                "t0_monotonic_ns": 82,
                "t1_monotonic_ns": 85,
            }
            if authority["track"] == "cpu_profiles":
                value["perf_disable"] = (
                    perf_control_events()[1] if perf_available else None
                )
        else:
            value = {"nonce": nonce, "phase": phase}
        value["_runner_received_monotonic_ns"] = timestamp
        timestamp += 10
        return value

    def command(name: str, nonce: str, phase: str | None = None) -> dict[str, object]:
        nonlocal timestamp
        value: dict[str, object] = {"command": name, "nonce": nonce}
        if phase is not None:
            value["phase"] = phase
        value["_runner_sent_monotonic_ns"] = timestamp
        timestamp += 10
        return value

    if reopen:
        return [
            child("boot"), command("continue", nonce1, "boot"),
            child("runtime", nonce1), command("continue", nonce2, "runtime"),
            child("ready", nonce2), command("start", nonce3),
            child("opened", nonce3), command("continue", nonce4, "opened"),
            child("measured", nonce4), command("release", nonce4),
        ]
    return [
        child("boot"), command("continue", nonce1, "boot"),
        child("runtime", nonce1), command("continue", nonce2, "runtime"),
        child("opened", nonce2), command("continue", nonce3, "opened"),
        child("ready", nonce3), command("start", nonce4),
        child("measured", nonce4), command("release", nonce4),
    ]


def perf_control_events() -> list[dict[str, object]]:
    return [
        {
            "command": "enable",
            "nonce": "4" * 64,
            "sent_monotonic_ns": 71,
            "ack": "ack",
            "ack_received_monotonic_ns": 72,
        },
        {
            "command": "disable",
            "nonce": "4" * 64,
            "sent_monotonic_ns": 86,
            "ack": "ack",
            "ack_received_monotonic_ns": 87,
        },
    ]


def trace_boundary(
    authority: dict[str, object], events: list[dict[str, object]]
) -> dict[str, object]:
    phases = {
        event["phase"]: event
        for event in events
        if "command" not in event and event.get("phase") in {"ready", "measured"}
    }
    return {
        "child_pid": authority["child_pid"],
        "control_fd": authority["control_fd"],
        "begin_event": phases["ready"],
        "end_event": phases["measured"],
    }


def marker_line(authority: dict[str, object], event: dict[str, object]) -> str:
    wire = {
        key: value for key, value in event.items() if not key.startswith("_runner_")
    }
    payload = adapters.canonical_json(wire).decode()
    return (
        f'{authority["child_pid"]} write({authority["control_fd"]}, '
        f"{json.dumps(payload)}, {len(payload.encode())}) = {len(payload.encode())}"
    )


def live_authority(
    root: Path,
    *,
    pid: int,
    start_ticks: int,
    comm: str,
    variant: str,
    track: str,
    context: dict[str, object],
) -> dict[str, object]:
    prepared_root = root / "prepared"
    bindings = prepared_root / "bindings"
    bindings.mkdir(parents=True)
    artifacts = prepared_root / "artifacts"
    artifacts.mkdir()
    attempt_root = root / "attempt"
    attempt_root.mkdir()
    binary = artifacts / "bench"
    binary.write_bytes(b"synthetic benchmark executable\n")
    binary.chmod(0o555)
    binary_sha = sha256(binary.read_bytes())
    adapter = artifacts / "profile_adapters.py"
    adapter.write_bytes(MODULE_PATH.read_bytes())
    adapter.chmod(0o444)
    adapter_sha = sha256(adapter.read_bytes())
    tools = profile_tools_for(track, artifacts)
    ordinary_a_binary = binary
    if variant != "A":
        ordinary_a_binary = artifacts / "ast-rb-a"
        ordinary_a_binary.write_bytes(b"synthetic ordinary A executable\n")
        ordinary_a_binary.chmod(0o555)
    overlay_binary = artifacts / "ast-rb-a-product-overlay"
    overlay_binary.write_bytes(ordinary_a_binary.read_bytes())
    overlay_binary.chmod(0o555)
    ordinary_inventory = artifacts / "symbols-ordinary-a.txt"
    overlay_inventory = artifacts / "symbols-overlay-a.txt"
    for inventory in (ordinary_inventory, overlay_inventory):
        inventory.write_bytes(b"main T 0 1\n")
        inventory.chmod(0o444)
    nm_path = artifacts / "nm"
    nm_path.write_bytes(b"synthetic nm executable\n")
    nm_path.chmod(0o555)

    bwrap = Path("/usr/bin/true").resolve(strict=True)
    rustup = Path("/usr/bin/false").resolve(strict=True)
    toolchain_root = root / "toolchain"
    toolchain_bin = toolchain_root / "bin"
    toolchain_bin.mkdir(parents=True)
    cargo = toolchain_bin / "cargo"
    git = toolchain_bin / "git"
    rustc = toolchain_bin / "rustc"
    for tool in (cargo, git, rustc):
        tool.write_bytes(f"synthetic {tool.name}\n".encode())
        tool.chmod(0o555)
    rust_lld = (
        toolchain_root
        / "lib"
        / "rustlib"
        / "x86_64-unknown-linux-gnu"
        / "bin"
        / "rust-lld"
    )
    rust_lld.parent.mkdir(parents=True)
    rust_lld.write_bytes(b"synthetic rust-lld\n")
    rust_lld.chmod(0o555)
    toolchain_bin.chmod(0o555)
    toolchain_root.chmod(0o555)
    cargo_home = root / "cargo-home"
    prepare_tree(cargo_home, "config.toml", b"[net]\noffline = true\n")
    cargo_home.chmod(0o755)
    cargo_home_preserved = cargo_home / "registry-cache"
    cargo_home_preserved.write_bytes(b"synthetic preserved Cargo-home child\n")
    cargo_home_preserved.chmod(0o444)
    cargo_home.chmod(0o555)
    rustup_home = root / "rustup-home"
    rustup_home.mkdir()
    rustup_home.chmod(0o555)
    toolchain: dict[str, object] = {
        "bwrap_path": str(bwrap),
        "bwrap_sha256": sha256(bwrap.read_bytes()),
        "cargo_home_path": str(cargo_home),
        "cargo_path": str(cargo),
        "cargo_sha256": sha256(cargo.read_bytes()),
        "cargo_version_verbose": "cargo 1.99.0 (synthetic)",
        "git_path": str(git),
        "git_sha256": sha256(git.read_bytes()),
        "rustc_path": str(rustc),
        "rustc_sha256": sha256(rustc.read_bytes()),
        "rustc_version_verbose": (
            "rustc 1.99.0 (synthetic)\n"
            "binary: rustc\n"
            "host: x86_64-unknown-linux-gnu"
        ),
        "rustc_host": "x86_64-unknown-linux-gnu",
        "rust_lld_path": str(rust_lld),
        "rust_lld_sha256": sha256(rust_lld.read_bytes()),
        "rustup_home_path": str(rustup_home),
        "rustup_path": str(rustup),
        "rustup_sha256": sha256(rustup.read_bytes()),
        "rustup_toolchain": "synthetic-stable",
    }
    for index, (trusted_root, _guest) in enumerate(
        TRUSTED_SYSTEM_MOUNTS
    ):
        prepare_tree(trusted_root, f"trusted-{index}", b"trusted metadata\n")

    current_root = root / "reviewed-current"
    current_manifests = current_root / "manifests"
    current_manifests.mkdir(parents=True)
    (current_root / "logs").mkdir()
    (current_root / "targets").mkdir()
    (current_root / "artifacts" / "tools").mkdir(parents=True)
    (current_root / "artifacts" / "release").mkdir()
    (current_root / "receipts").mkdir()
    (current_root / "inputs").mkdir()
    wrapper_input = current_root / "inputs" / "rustc_workspace_wrapper.py"
    wrapper_input.write_bytes(b"#!/usr/bin/python3\n# synthetic wrapper\n")
    wrapper_input.chmod(0o555)
    current_lock_payload = b"synthetic lock A\n"
    current_lock_sha256 = sha256(current_lock_payload)
    build_nonce = "0" * 64
    current_compile_out = {
        "binary_byte_identical": True,
        "forbidden_hook_strings": list(adapters._FORBIDDEN_RELEASE_HOOK_STRINGS),
        "overlay_release_sha256": "6" * 64,
        "pristine_sha256": "6" * 64,
        "symbol_absence_sha256": "7" * 64,
        "symbol_inventory_byte_identical": True,
    }
    filesystem_admissions: dict[str, object] = {}
    current_builds: dict[str, object] = {}
    for name, source_name in (
        ("children", "children"),
        ("hooked_release", "hooked-release"),
        ("pristine_release", "pristine-release"),
    ):
        source_root = current_root / "materialized" / source_name
        source_root.mkdir(parents=True)
        (source_root / "Cargo.toml").write_bytes(f"[{name}]\n".encode())
        (source_root / "Cargo.toml").chmod(0o444)
        (source_root / "Cargo.lock").write_bytes(current_lock_payload)
        (source_root / "Cargo.lock").chmod(0o444)
        (source_root / ".cargo").mkdir()
        (source_root / ".cargo").chmod(0o555)
        source_root.chmod(0o555)
        paths = semantic_paths(current_manifests, source_name)
        paths["cargo_home"] = current_manifests / f"cargo-home-{source_name}.json"
        semantic = semantic_authority(
            source_root=source_root,
            toolchain_root=toolchain_root,
            cargo_home=cargo_home,
            paths=paths,
        )
        materialized_manifest_sha256 = write_canonical(
            current_manifests / f"materialized-{source_name}.json",
            {
                "entries": [{"path": "."}, {"path": "Cargo.lock"}, {"path": "Cargo.toml"}],
                "schema": "bn-30fs-file-manifest-v2",
            },
            0o444,
        )
        cargo_search_entries = [
            {
                "path": f"{GUEST_SOURCE}/.cargo/config.toml",
                "sha256": EMPTY_SHA256,
                "status": "present",
            },
            {
                "path": f"{GUEST_SOURCE}/.cargo/config",
                "sha256": EMPTY_SHA256,
                "status": "present",
            },
            *(
                {
                    "path": path,
                    "sha256": None,
                    "status": "absent",
                }
                for path in (
                    f"{GUEST_ROOT}/.cargo/config.toml",
                    f"{GUEST_ROOT}/.cargo/config",
                    "/.cargo/config.toml",
                    "/.cargo/config",
                )
            ),
            {
                "path": f"{GUEST_CARGO_HOME}/config.toml",
                "sha256": sha256((cargo_home / "config.toml").read_bytes()),
                "status": "present",
            },
            {
                "path": f"{GUEST_CARGO_HOME}/config",
                "sha256": EMPTY_SHA256,
                "status": "present",
            },
        ]
        semantic_cargo_home = semantic["cargo_home"]
        cargo_config = {
            "cargo_search": {
                "cargo_home_path": GUEST_CARGO_HOME,
                "cwd": GUEST_SOURCE,
                "entries": cargo_search_entries,
                "schema": CARGO_CONFIG_SEARCH_SCHEMA,
            },
            "cargo_home_tree": {
                "entry_count": semantic_cargo_home["entry_count"],
                "equal_pre_post": True,
                "path": semantic_cargo_home["manifest_path"],
                "post_sha256": semantic_cargo_home["manifest_sha256"],
                "pre_sha256": semantic_cargo_home["manifest_sha256"],
                "watch_count": semantic_cargo_home["watch_count"],
            },
            "preserved_top_level_entries": {
                "cargo-home": [
                    {
                        "identity": current_file_identity(cargo_home_preserved),
                        "name": cargo_home_preserved.name,
                        "type": "regular",
                    }
                ],
                "source": [],
            },
            "schema": CURRENT_CARGO_CONFIG_SCHEMA,
        }
        config_bindings = (
            (
                f"config:{GUEST_SOURCE}/.cargo/config.toml",
                "--ro-bind-data",
                f"{GUEST_SOURCE}/.cargo/config.toml",
            ),
            (
                f"config:{GUEST_SOURCE}/.cargo/config",
                "--ro-bind-data",
                f"{GUEST_SOURCE}/.cargo/config",
            ),
            ("cargo_home", "--tmp-overlay", GUEST_CARGO_HOME),
            (
                f"config:{GUEST_CARGO_HOME}/config.toml",
                "--ro-bind-data",
                f"{GUEST_CARGO_HOME}/config.toml",
            ),
            (
                f"config:{GUEST_CARGO_HOME}/config",
                "--ro-bind-data",
                f"{GUEST_CARGO_HOME}/config",
            ),
        )
        descriptor_names = (
            *(f"system:{guest}" for _host, guest in TRUSTED_SYSTEM_MOUNTS),
            "dev_null",
            "source",
            "toolchain_root",
            "cargo",
            "rustc",
            "rust_lld",
            "python",
            *(binding[0] for binding in config_bindings),
            "target",
            *(("wrapper", "receipt") if name == "children" else ()),
        )
        descriptors = dict(
            zip(descriptor_names, range(100, 100 + len(descriptor_names)), strict=True)
        )
        examples = (
            (
                "asterism_rebaseline_current_correctness",
                "asterism_rebaseline_current_fault",
            )
            if name == "children"
            else ("asterism_rebaseline_public",)
        )
        argv = [str(bwrap), "synthetic-current-build", *examples]
        environment = current_sandbox_environment(toolchain)
        if name == "children":
            environment.update(
                {
                    "ASTERISM_FAULT_COMPILE_OUT_IDENTICAL": "true",
                    "ASTERISM_FAULT_COMPILE_OUT_OVERLAY_RELEASE_SHA256": current_compile_out["overlay_release_sha256"],
                    "ASTERISM_FAULT_COMPILE_OUT_PRISTINE_SHA256": current_compile_out["pristine_sha256"],
                    "ASTERISM_FAULT_COMPILE_OUT_SCHEMA": CURRENT_FAULT_COMPILE_OUT_SCHEMA,
                    "ASTERISM_FAULT_COMPILE_OUT_SYMBOL_ABSENCE_SHA256": current_compile_out["symbol_absence_sha256"],
                    "ASTERISM_REBASELINE_CHILD_BUILD_NONCE": build_nonce,
                    "ASTERISM_REBASELINE_EXPECTED_LIB_SOURCE": CURRENT_EXPECTED_LIB_SOURCE,
                    "ASTERISM_REBASELINE_PINNED_RUSTC": GUEST_RUSTC,
                    "ASTERISM_REBASELINE_WRAPPER_RECEIPT": f"{GUEST_ROOT}/receipt/injection.json",
                    "RUSTC_WORKSPACE_WRAPPER": f"{GUEST_ROOT}/rustc_workspace_wrapper.py",
                }
            )
        else:
            environment.update(
                {
                    "ASTERISM_BUILD_ADAPTER_SHA256": "8" * 64,
                    "ASTERISM_BUILD_BINARY_KIND": "public",
                    "ASTERISM_BUILD_NONCE": build_nonce,
                    "ASTERISM_BUILD_CARGO_LOCK_SHA256": current_lock_sha256,
                    "ASTERISM_BUILD_PRODUCT_COMMIT": adapters.VARIANT_SOURCE_BINDINGS["A"]["commit"],
                    "ASTERISM_BUILD_PRODUCT_TREE": adapters.VARIANT_SOURCE_BINDINGS["A"]["tree"],
                    "ASTERISM_BUILD_PROTOCOL": adapters.PROTOCOL,
                    "ASTERISM_BUILD_PROTOCOL_SHA256": adapters.PROTOCOL_SHA256,
                    "ASTERISM_BUILD_SHARED_MANIFEST_SHA256": "9" * 64,
                    "ASTERISM_BUILD_SOURCE_APPROVAL_SHA256": "fa2acb626f303f8a65a16a6c8a1fd86b7e80cf48e092ae21a7308984ae790c94",
                    "ASTERISM_BUILD_TIMED_SURFACE": "public-event-store",
                    "ASTERISM_BUILD_TOOLING_COMMIT": "1" * 40,
                    "ASTERISM_BUILD_TOOLING_TREE": "2" * 40,
                    "ASTERISM_BUILD_VARIANT": "A",
                }
            )
        target = current_root / "targets" / source_name
        target_examples = target / "release" / "examples"
        target_examples.mkdir(parents=True)
        current_artifacts: dict[str, object] = {}
        artifact_destinations = {
            "asterism_rebaseline_current_correctness": current_root / "artifacts" / "tools" / "ast-rb-check",
            "asterism_rebaseline_current_fault": current_root / "artifacts" / "tools" / "ast-rb-fault",
            "asterism_rebaseline_public": current_root
            / "artifacts"
            / "release"
            / ("hooked-A" if name == "hooked_release" else "pristine-A"),
        }
        for example in examples:
            source_artifact = target_examples / example
            source_artifact.write_bytes(f"synthetic current {source_name} {example}\n".encode())
            source_artifact.chmod(0o555)
            source_artifact.with_name(
                f"{example}-0123456789abcdef"
            ).hardlink_to(source_artifact)
            destination = artifact_destinations[example]
            destination.write_bytes(source_artifact.read_bytes())
            destination.chmod(0o555)
            current_artifacts[example] = {
                "binding": {
                    "comm": destination.name,
                    "executable_mode": 0o555,
                    "path": str(destination.resolve()),
                    "sha256": sha256(destination.read_bytes()),
                },
                "source": current_file_identity(
                    source_artifact,
                    logical_path=(
                        f"/proc/self/fd/{descriptors['target']}"
                        f"/release/examples/{example}"
                    ),
                ),
            }
        target_identity = current_directory_identity(target)
        binds: dict[str, object] = {
            "target": {
                "parent": current_directory_identity(target.parent),
                "post": target_identity,
                "pre": dict(target_identity),
            }
        }
        execution_tools = {
            "bwrap": current_retained_file(bwrap, trusted=True),
            "cargo": current_retained_file(cargo, trusted=False),
            "dev_null": retained_null_device(),
            "python": current_retained_file(
                Path("/usr/bin/python3").resolve(strict=True), trusted=True
            ),
            "rustc": current_retained_file(rustc, trusted=False),
            "rust_lld": current_retained_file(rust_lld, trusted=False),
            "toolchain_root": current_directory_identity(toolchain_root),
        }
        execution = {
            "argv": argv,
            "cwd": str(current_root),
            "environment": dict(sorted(environment.items())),
            "exit_status": 0,
            "execution_authority": execution_tools["bwrap"],
            "passed_file_descriptors": (
                len(descriptor_names)
                + 2
                + len(cargo_config["preserved_top_level_entries"]["cargo-home"])
            ),
            "stderr_bytes": 0,
            "stderr_sha256": sha256(b""),
            "stdout_bytes": 0,
            "stdout_sha256": sha256(b""),
        }
        filesystem_admission = {"build": source_name, "schema": "synthetic-admission-v1"}
        filesystem_admissions[name] = filesystem_admission
        current_build: dict[str, object] = {
            "argv": argv,
            "environment": dict(sorted(environment.items())),
            "execution": execution,
            "filesystem_admission": filesystem_admission,
            "cargo_config_prebuild": cargo_config,
            "cargo_config_postbuild": cargo_config,
            "execution_tools": execution_tools,
            "artifacts": current_artifacts,
            "binds": binds,
            "lock_prebuild": release_file_binding(source_root / "Cargo.lock"),
            "lock_postbuild": release_file_binding(source_root / "Cargo.lock"),
            "source_manifest_sha256": materialized_manifest_sha256,
            "semantic_input_authority": semantic,
            "toolchain_manifest": {
                "entry_count": semantic["toolchain"]["entry_count"],
                "equal_pre_post": True,
                "path": semantic["toolchain"]["manifest_path"],
                "post_sha256": semantic["toolchain"]["manifest_sha256"],
                "pre_sha256": semantic["toolchain"]["manifest_sha256"],
            },
            "target": str(target.resolve()),
            "target_was_absent": True,
        }
        if name == "children":
            receipt_root = current_root / "receipts" / "children"
            receipt_root.mkdir()
            receipt = {
                "build_nonce": build_nonce,
                "crate_name": "mess_store",
                "crate_type": "lib",
                "injected_arguments": list(CURRENT_WRAPPER_ARGUMENTS),
                "original_argv_sha256": "b" * 64,
                "package": "mess-store",
                "rustc": GUEST_RUSTC,
                "schema": CURRENT_WRAPPER_RECEIPT_SCHEMA,
                "source": CURRENT_EXPECTED_LIB_SOURCE,
            }
            receipt_path = receipt_root / "injection.json"
            write_canonical(receipt_path, receipt, 0o444)
            receipt_identity = current_directory_identity(receipt_root)
            binds["receipt"] = {
                "parent": current_directory_identity(receipt_root.parent),
                "post": receipt_identity,
                "pre": dict(receipt_identity),
            }
            current_build.update(
                {
                    "wrapper_receipt": receipt,
                    "wrapper_receipt_identity": current_file_identity(
                        receipt_path,
                        logical_path=(
                            f"/proc/self/fd/{descriptors['receipt']}/injection.json"
                        ),
                    ),
                    "wrapper_receipt_sha256": sha256(receipt_path.read_bytes()),
                    "wrapper_input_identity": current_file_identity(wrapper_input),
                }
            )
        write_canonical(
            current_root / "logs" / f"cargo-build-{source_name}.json",
            execution,
            0o444,
        )
        current_builds[name] = current_build

    lock_root = root / "reviewed-locks"
    lock_manifests = lock_root / "manifests"
    lock_manifests.mkdir(parents=True)
    repository = root / "repository"
    source_plan_path = (
        repository
        / "spikes"
        / "asterism_rebaseline"
        / "tooling"
        / "source-plan.json"
    )
    write_canonical(source_plan_path, {"schema": "synthetic-source-plan-v1"}, 0o444)
    lock_claims: dict[str, object] = {}
    for variant_name in ("A", "B"):
        resolver_source = lock_root / "materialized" / variant_name
        resolver_source.mkdir(parents=True)
        (resolver_source / "Cargo.toml").write_bytes(
            f"[resolver-{variant_name}]\n".encode()
        )
        (resolver_source / "Cargo.toml").chmod(0o444)
        resolver_source.chmod(0o555)
        lock_payload = (
            current_lock_payload
            if variant_name == "A"
            else b"synthetic lock B\n"
        )
        final_lock = lock_root / "locks" / f"Cargo-{variant_name}.lock"
        final_lock.parent.mkdir(parents=True, exist_ok=True)
        final_lock.write_bytes(lock_payload)
        final_lock.chmod(0o444)
        config_path = lock_manifests / f"cargo-config-{variant_name}.json"
        config_sha256 = write_cargo_config_manifest(
            config_path, resolver_source, cargo_home
        )
        historical = {
            "commit": variant_name.lower() * 40,
            "path": "Cargo.lock",
            "sha256": sha256(lock_payload),
        }
        tracked_environment = {
            "CARGO_HOME": str(cargo_home),
            "CARGO_INCREMENTAL": "0",
            "CARGO_NET_OFFLINE": "true",
            "GIT_CONFIG_COUNT": "0",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1",
            "HOME": "/nonexistent",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": f"{toolchain_bin}:/usr/bin:/bin",
            "RUSTC": str(rustc),
            "RUSTUP_HOME": str(rustup_home),
            "RUSTUP_TOOLCHAIN": "synthetic-stable",
            "TZ": "UTC",
        }
        tracked = {
            "argv": [
                str(git),
                "-C",
                str(repository),
                "show",
                f"{historical['commit']}:{historical['path']}",
            ],
            "cargo_config_search": {
                "path": str(config_path),
                "sha256": config_sha256,
            },
            "cwd": str(repository),
            "environment": tracked_environment,
            "exit_status": 0,
            "host_source_root": str(resolver_source),
            "resolver_kind": "tracked_git_readback",
            "stderr": "",
            "stderr_sha256": sha256(b""),
            "stdout": lock_payload.decode(),
            "stdout_sha256": sha256(lock_payload),
            "toolchain": toolchain,
        }
        lock_claims[variant_name] = {
            "current_lock_attempt": None,
            "final_lock_path": str(final_lock),
            "final_lock_sha256": sha256(lock_payload),
            "historical_lock": historical,
            "resolver": tracked,
        }
    for variant_name in ("C", "D"):
        resolver_source = lock_root / "materialized" / variant_name
        resolver_source.mkdir(parents=True)
        (resolver_source / "Cargo.toml").write_bytes(
            f"[resolver-{variant_name}]\n".encode()
        )
        (resolver_source / "Cargo.toml").chmod(0o444)
        lock_payload = f"synthetic lock {variant_name}\n".encode()
        (resolver_source / "Cargo.lock").write_bytes(lock_payload)
        (resolver_source / "Cargo.lock").chmod(0o444)
        resolver_source.chmod(0o555)
        final_lock = lock_root / "locks" / f"Cargo-{variant_name}.lock"
        final_lock.parent.mkdir(parents=True, exist_ok=True)
        final_lock.write_bytes(lock_payload)
        final_lock.chmod(0o444)
        config_path = lock_manifests / f"cargo-config-{variant_name}.json"
        config_sha256 = write_cargo_config_manifest(
            config_path, resolver_source, cargo_home
        )

        def resolver_record(role: str) -> dict[str, object]:
            label = (
                f"current-lock-{variant_name}"
                if role == "current"
                else f"resolved-lock-{variant_name}"
            )
            semantic = semantic_authority(
                source_root=resolver_source,
                toolchain_root=toolchain_root,
                cargo_home=cargo_home,
                paths=semantic_paths(lock_manifests, label, resolver=True),
                source_role="resolution_source_without_cargo_lock",
            )
            descriptor_names = (
                *(
                    f"system:{guest}"
                    for _host, guest in TRUSTED_SYSTEM_MOUNTS
                ),
                "source",
                "toolchain_root",
                "cargo",
                "rustc",
                "cargo_home",
                *(f"config:{guest}" for guest in GUEST_BOUND_CONFIG_PATHS),
            )
            descriptors = dict(
                zip(descriptor_names, range(40, 52), strict=True)
            )
            arguments = (
                [
                    "metadata",
                    "--locked",
                    "--offline",
                    "--format-version",
                    "1",
                    "--no-deps",
                ]
                if role == "current"
                else ["generate-lockfile", "--offline"]
            )
            digest = (
                current_lock_sha256 if role == "current" else sha256(lock_payload)
            )
            boundary = {
                "path": str(resolver_source / "Cargo.lock"),
                "sha256": digest,
                "status": "present",
            }
            lock_output = (
                {"pre": boundary, "post": dict(boundary)}
                if role == "current"
                else {
                    "pre": {
                        "path": str(resolver_source / "Cargo.lock"),
                        "sha256": None,
                        "status": "absent",
                    },
                    "post": boundary,
                }
            )
            return {
                "argv": [
                    str(bwrap),
                    "synthetic-resolution-sandbox",
                    *arguments,
                ],
                "cargo_config_search": {
                    "path": str(config_path),
                    "sha256": config_sha256,
                },
                "cwd": GUEST_SOURCE,
                "environment": sandbox_environment(toolchain),
                "execution_authority": release_file_binding(bwrap),
                "exit_status": 0,
                "host_source_root": str(resolver_source),
                "lock_output": lock_output,
                "passed_file_descriptors": 13,
                "resolver_kind": "sandboxed_cargo_resolution",
                "semantic_input_authority": semantic,
                "stderr": "",
                "stderr_sha256": sha256(b""),
                "stdout": "",
                "stdout_sha256": sha256(b""),
                "toolchain": toolchain,
            }

        lock_claims[variant_name] = {
            "current_lock_attempt": resolver_record("current"),
            "final_lock_path": str(final_lock),
            "final_lock_sha256": sha256(lock_payload),
            "historical_lock": {
                "commit": variant_name.lower() * 40,
                "path": "Cargo.lock",
                "sha256": sha256(lock_payload),
            },
            "resolver": resolver_record("generated"),
        }
    reviewed_lock_manifest_path = lock_root / "lock-candidates.json"
    reviewed_lock_manifest = {
        "schema": LOCK_CANDIDATES_SCHEMA,
        "source_plan_path": str(source_plan_path),
        "toolchain": toolchain,
        "variants": lock_claims,
    }
    write_canonical(reviewed_lock_manifest_path, reviewed_lock_manifest, 0o444)
    lock_manifest_binding = review_input_binding(
        reviewed_lock_manifest_path, LOCK_CANDIDATES_SCHEMA
    )
    embedded_lock_binding = {
        **lock_manifest_binding,
        "payload": reviewed_lock_manifest,
    }
    lock_authority_value = {
        "lock_manifest": embedded_lock_binding,
        "schema": "bn-31gp-current-lock-authority-v1",
    }
    reviewed_cargo_binding = lock_claims["A"]["resolver"]["cargo_config_search"]
    reviewed_cargo_path = Path(str(reviewed_cargo_binding["path"]))
    reviewed_cargo_record = json.loads(reviewed_cargo_path.read_bytes())
    cargo_config_authority = {
        "binding": dict(reviewed_cargo_binding),
        "identity": current_file_identity(reviewed_cargo_path),
        "recorded": reviewed_cargo_record,
        "translated_entries": json.loads(
            json.dumps(reviewed_cargo_record["entries"])
        ),
    }

    preapproval_compile_out = current_compile_out
    requirement = {
        "binary_byte_identical": True,
        "cfg_test": False,
        "forbidden_hook_strings": list(adapters._FORBIDDEN_RELEASE_HOOK_STRINGS),
        "forbidden_hook_strings_absent": True,
        "ordinary_a_role": "published",
        "overlay_a_role": "proof_only",
        "preapproval_compile_out_sha256": sha256(
            adapters.canonical_json(preapproval_compile_out)
        ),
        "product_overlay_sha256": adapters._CURRENT_PRODUCT_OVERLAY_SHA256,
        "proof_must_bind_enclosing_approval_sha256": True,
        "repeat_under_real_source_approval": True,
        "rustc_workspace_wrapper": "absent",
        "same_contract_nonce_lock_toolchain_sandbox": True,
        "schema": "bn-3hch-release-compile-out-requirement-v1",
        "status": "required",
        "symbol_inventory_byte_identical": True,
        "variant": "A",
    }
    lock_review_value = {"schema": "bn-31gp-current-lock-review-bundle-v1"}
    tools_review_value = {
        "schema": "asterism-rebaseline-tools-v3",
        "support_files": {
            "profile_adapter": {
                "path": str(adapter),
                "sha256": adapter_sha,
                "mode": 0o444,
            }
        },
        "tools": tools,
    }
    reviewed_lock_authority_path = current_root / "lock-review-authority.json"
    reviewed_lock_review_path = current_root / "lock-review-bundle.json"
    reviewed_tools_path = current_root / "tools-manifest.json"
    write_canonical(reviewed_lock_authority_path, lock_authority_value, 0o444)
    write_canonical(reviewed_lock_review_path, lock_review_value, 0o444)
    write_canonical(reviewed_tools_path, tools_review_value, 0o444)
    lock_authority_binding = review_input_binding(
        reviewed_lock_authority_path, "bn-31gp-current-lock-authority-v1"
    )
    lock_review_binding = review_input_binding(
        reviewed_lock_review_path, "bn-31gp-current-lock-review-bundle-v1"
    )
    tools_binding = review_input_binding(
        reviewed_tools_path, "asterism-rebaseline-tools-v3"
    )

    def without_schema(binding: dict[str, object]) -> dict[str, object]:
        return {field: value for field, value in binding.items() if field != "schema"}

    current_children = {
        "artifacts": {},
        "build_nonce": build_nonce,
        "builds": current_builds,
        "cargo_config_authority": cargo_config_authority,
        "construction_path": "/authority/construction.py",
        "construction_sha256": "0" * 64,
        "fault_authority": {},
        "inputs": {},
        "lock_authority": lock_authority_value,
        "lock_authority_inputs": {
            "authority": without_schema(lock_authority_binding),
            "lock_manifest": without_schema(lock_manifest_binding),
            "review_bundle": without_schema(lock_review_binding),
        },
        "lock_authority_validation": {},
        "lock_candidates": {},
        "lock_manifest_sha256": lock_manifest_binding["sha256"],
        "prebuild_filesystem_admissions": filesystem_admissions,
        "product_commit": adapters.VARIANT_SOURCE_BINDINGS["A"]["commit"],
        "product_overlay_authority": {
            "patch": {"sha256": adapters._CURRENT_PRODUCT_OVERLAY_SHA256}
        },
        "product_tree": adapters.VARIANT_SOURCE_BINDINGS["A"]["tree"],
        "protocol": adapters.PROTOCOL,
        "protocol_sha256": adapters.PROTOCOL_SHA256,
        "release_compile_out": preapproval_compile_out,
        "release_compile_out_approval": {
            "final_integration_action": (
                "repeat-release-equality-proof-under-real-source-approval"
            ),
            "source_approval_sha256": (
                "fa2acb626f303f8a65a16a6c8a1fd86b7e80cf48e092ae21a7308984ae790c94"
            ),
            "source_approval_status": "preapproval-sentinel-not-source-approved",
        },
        "review_bundle_sha256": lock_review_binding["sha256"],
        "schema": adapters._CURRENT_CHILDREN_ATTESTATION_SCHEMA,
        "static_authority": {},
        "status": "ok",
        "toolchain": toolchain,
        "toolchain_identities": {},
        "tools_manifest_path": str(reviewed_tools_path),
        "tools_manifest_sha256": tools_binding["sha256"],
    }
    reviewed_current_path = current_root / "current-children-attestation.json"
    write_canonical(reviewed_current_path, current_children, 0o444)
    assertion = {
        "inputs": {
            "current_children_attestation": review_input_binding(
                reviewed_current_path, adapters._CURRENT_CHILDREN_ATTESTATION_SCHEMA
            ),
            "lock_authority": lock_authority_binding,
            "lock_manifest": lock_manifest_binding,
            "lock_review_bundle": lock_review_binding,
            "tools_manifest": tools_binding,
        },
        "open_findings": 0,
        "protocol": adapters.PROTOCOL,
        "protocol_sha256": adapters.PROTOCOL_SHA256,
        "release_compile_out_requirement": requirement,
        "schema": "bn-3hch-source-review-assertion-v1",
        "status": "approved",
        "tooling_commit": "1" * 40,
        "tooling_tree": "2" * 40,
    }
    assertion_sha = sha256(adapters.canonical_json(assertion))
    source_review_values = {
        "bundle": {
            "assertion": assertion,
            "assertion_sha256": assertion_sha,
            "review_created": {},
            "schema": "bn-3hch-source-review-bundle-v1",
            "verdict": {},
        },
        "current_children_attestation": current_children,
        "lock_authority": lock_authority_value,
        "lock_review_bundle": lock_review_value,
    }
    source_review_paths = {
        "bundle": bindings / "source-review-bundle.json",
        "current_children_attestation": (
            bindings / "current-children-attestation.json"
        ),
        "lock_authority": bindings / "lock-review-authority.json",
        "lock_review_bundle": bindings / "lock-review-bundle.json",
    }
    source_review_schemas = adapters._SOURCE_REVIEW_CONTENT_SCHEMAS
    for name, source_review_path in source_review_paths.items():
        write_canonical(source_review_path, source_review_values[name], 0o444)
    approved_source_review = {
        "assertion_sha256": assertion_sha,
        **{
            name: {
                "mode": 0o444,
                "schema": source_review_schemas[name],
                "sha256": sha256(source_review_paths[name].read_bytes()),
            }
            for name in source_review_paths
        },
        "release_compile_out_requirement": requirement,
    }
    source = adapters.VARIANT_SOURCE_BINDINGS[variant]
    role_lifetime: object = (
        adapters.C_ROLE_LIFETIME_CONTRACT if variant == "C" else "not_applicable"
    )
    approval = {
        "schema": "bn-2l3n-source-approval-v3",
        "status": "approved",
        "protocol": adapters.PROTOCOL,
        "protocol_sha256": adapters.PROTOCOL_SHA256,
        "tools_manifest_sha256": tools_binding["sha256"],
        "source_review": approved_source_review,
        "variants": {
            variant: {
                "product_commit": source["commit"],
                "product_tree": source["tree"],
                "profile_role_lifetime": role_lifetime,
            }
        },
        "tools_manifest": {
            "support_files": {
                "profile_adapter": {
                    "path": str(adapter),
                    "sha256": adapter_sha,
                    "mode": 0o444,
                }
            },
            "tools": tools,
        },
    }
    original_approval_path = bindings / "source-approval.json"
    approval_sha = write_canonical(original_approval_path, approval, 0o444)
    prepared_contracts = {
        variant_name: {
            "protocol_sha256": adapters.PROTOCOL_SHA256,
            "product_commit": adapters.VARIANT_SOURCE_BINDINGS[variant_name][
                "commit"
            ],
            "product_tree": adapters.VARIANT_SOURCE_BINDINGS[variant_name]["tree"],
            "profile_role_lifetime": (
                adapters.C_ROLE_LIFETIME_CONTRACT
                if variant_name == "C"
                else "not_applicable"
            ),
        }
        for variant_name in ("A", "B", "C", "D")
    }
    ordinary_nm_child = nm_child(
        name="ordinary_a",
        pid=301,
        nm_path=nm_path,
        inventory=ordinary_inventory,
        prepared_root=prepared_root,
    )
    overlay_nm_child = nm_child(
        name="overlay_a",
        pid=302,
        nm_path=nm_path,
        inventory=overlay_inventory,
        prepared_root=prepared_root,
    )
    release_attestations: dict[str, dict[str, object]] = {}
    release_sandbox_hashes: dict[str, str] = {}
    for variant_name in ("A", "B", "C", "D"):
        source_root = prepared_root / "materialized" / variant_name
        prepare_tree(source_root, "Cargo.toml", f"[release-{variant_name}]\n".encode())
        attestation = release_attestation(
            prepared_root=prepared_root,
            label=f"build-{variant_name}",
            source_root=source_root,
            toolchain=toolchain,
            package="mess-log" if variant_name == "B" else "mess-store",
            example=(
                "asterism_rebaseline_bare"
                if variant_name == "B"
                else "asterism_rebaseline_public"
            ),
            descriptors=range(60, 75),
            source_approval_sha256=approval_sha,
            contract=prepared_contracts[variant_name],
        )
        release_sandbox_hashes[variant_name] = str(
            attestation.pop("_sandbox_sha256")
        )
        release_attestations[variant_name] = attestation
    overlay_source = prepared_root / "materialized" / "A-product-overlay"
    prepare_tree(overlay_source, "Cargo.toml", b"[release-A-overlay]\n")
    overlay_attestation = release_attestation(
        prepared_root=prepared_root,
        label="build-A-product-overlay",
        source_root=overlay_source,
        toolchain=toolchain,
        package="mess-store",
        example="asterism_rebaseline_public",
        descriptors=range(80, 95),
        source_approval_sha256=approval_sha,
        contract=prepared_contracts["A"],
    )
    overlay_attestation["product_overlay_sha256"] = (
        adapters._CURRENT_PRODUCT_OVERLAY_SHA256
    )
    overlay_sandbox_sha = str(overlay_attestation.pop("_sandbox_sha256"))
    assert overlay_sandbox_sha == release_sandbox_hashes["A"]
    equivalence = {
        "source_approval_sha256": approval_sha,
        "contract_sha256": sha256(
            adapters.canonical_json(prepared_contracts["A"])
        ),
        "build_nonce": "4" * 64,
        "cargo_lock_sha256": "5" * 64,
        "toolchain_sha256": sha256(adapters.canonical_json(toolchain)),
        "build_environment_sha256": sha256(
            adapters.canonical_json(release_attestations["A"]["build_env"])
        ),
        "sandbox_sha256": release_sandbox_hashes["A"],
        "cfg_test": False,
        "rustc_workspace_wrapper": "absent",
        "ordinary_a_role": "published",
        "overlay_a_role": "proof_only",
    }

    def release_build(
        name: str, artifact_role: str, attestation: dict[str, object]
    ) -> dict[str, object]:
        return {
            "role": name,
            "artifact_role": artifact_role,
            "source_approval_sha256": approval_sha,
            "contract_sha256": equivalence["contract_sha256"],
            "build_nonce": equivalence["build_nonce"],
            "cargo_lock_sha256": equivalence["cargo_lock_sha256"],
            "toolchain_sha256": equivalence["toolchain_sha256"],
            "build_environment_sha256": equivalence[
                "build_environment_sha256"
            ],
            "sandbox_sha256": equivalence["sandbox_sha256"],
            "cfg_test": False,
            "rustc_workspace_wrapper": "absent",
            "attestation": attestation,
            "attestation_sha256": sha256(adapters.canonical_json(attestation)),
        }

    release_compile_out = {
        "schema": "bn-3hch-release-compile-out-v1",
        "protocol": adapters.PROTOCOL,
        "protocol_sha256": adapters.PROTOCOL_SHA256,
        "status": "ok",
        "source_approval_sha256": approval_sha,
        "requirement_sha256": sha256(adapters.canonical_json(requirement)),
        "current_children_attestation_sha256": sha256(
            source_review_paths["current_children_attestation"].read_bytes()
        ),
        "product_overlay_sha256": adapters._CURRENT_PRODUCT_OVERLAY_SHA256,
        "equivalence_contract": equivalence,
        "builds": {
            "ordinary_a": release_build(
                "ordinary_a", "published", release_attestations["A"]
            ),
            "overlay_a": release_build(
                "overlay_a", "proof_only", overlay_attestation
            ),
        },
        "binaries": {
            "ordinary_a": release_file_binding(ordinary_a_binary),
            "overlay_a": release_file_binding(overlay_binary),
        },
        "nm": {
            "tool": release_file_binding(nm_path),
            "ordinary_a": ordinary_nm_child,
            "overlay_a": overlay_nm_child,
        },
        "symbol_inventories": {
            "ordinary_a": release_file_binding(ordinary_inventory),
            "overlay_a": release_file_binding(overlay_inventory),
        },
        "forbidden_hook_strings": list(adapters._FORBIDDEN_RELEASE_HOOK_STRINGS),
        "binary_byte_identical": True,
        "symbol_inventory_byte_identical": True,
        "forbidden_hook_strings_absent": True,
        "published_a_sha256": sha256(ordinary_a_binary.read_bytes()),
    }
    release_compile_out_path = prepared_root / "manifests" / "release-compile-out.json"
    release_compile_out_sha = write_canonical(
        release_compile_out_path, release_compile_out, 0o444
    )
    claim_path = prepared_root / "claims" / "single-use-claim.json"
    prepared_variants: dict[str, object] = {}
    for variant_name in ("A", "B", "C", "D"):
        if variant_name == variant:
            variant_binary = binary
            variant_comm = comm
        elif variant_name == "A":
            variant_binary = ordinary_a_binary
            variant_comm = ordinary_a_binary.name
        else:
            variant_binary = artifacts / f"ast-rb-{variant_name.lower()}"
            variant_binary.write_bytes(
                f"synthetic {variant_name} executable\n".encode()
            )
            variant_binary.chmod(0o555)
            variant_comm = variant_binary.name
        prepared_variants[variant_name] = {
            "attestation": release_attestations[variant_name],
            "artifact_root": str(artifacts),
            "binary": {
                "path": str(variant_binary.resolve()),
                "sha256": sha256(variant_binary.read_bytes()),
            },
            "executable_mode": 0o555,
            "comm": variant_comm,
            "contract": prepared_contracts[variant_name],
            "contract_argv": [str(variant_binary)],
            "contract_env": {},
            "evidence_argv": [str(variant_binary)],
            "evidence_env": {},
            "trace_path_marker_templates": {},
            "correctness_oracle_mode": variant_name != "B",
        }
    prepared = {
        "schema": "bn-2l3n-prepared-artifacts-v3",
        "protocol": adapters.PROTOCOL,
        "protocol_sha256": adapters.PROTOCOL_SHA256,
        "toolchain": toolchain,
        "source_approval": {
            "path": str(original_approval_path),
            "sha256": approval_sha,
        },
        "source_review": {
            name: {
                "path": str(source_review_paths[name].resolve()),
                "sha256": sha256(source_review_paths[name].read_bytes()),
                "mode": 0o444,
            }
            for name in source_review_paths
        },
        "release_compile_out": {
            "path": str(release_compile_out_path.resolve()),
            "sha256": release_compile_out_sha,
            "mode": 0o444,
        },
        "single_use_claim": {"path": str(claim_path)},
        "support_files": {
            "profile_adapter": {
                "path": str(adapter),
                "sha256": adapter_sha,
                "mode": 0o444,
            }
        },
        "tools": tools,
        "variants": prepared_variants,
    }
    original_prepared_path = prepared_root / "prepared-artifacts.json"
    prepared_sha = write_canonical(original_prepared_path, prepared, 0o444)
    claim = {
        "schema": "bn-2l3n-prepared-claim-v3",
        "protocol": adapters.PROTOCOL,
        "prepared_artifacts_path": str(original_prepared_path),
        "prepared_artifacts_sha256": prepared_sha,
        "output_dir": str(attempt_root),
        "attempt_nonce": "a" * 64,
        "lease_nonce": "1" * 64,
        "claimed_at": "2026-07-16T00:00:00+00:00",
        "claimed_monotonic_ns": 1,
    }
    # Protocol v4: the claim record lives in the run's output directory
    # (run-claim.json), not under the prepared root's claims/.  A v4-built
    # prepared root still carries an empty read-only claims/ directory.
    write_canonical(attempt_root / "run-claim.json", claim, 0o444)
    claim_path.parent.mkdir(mode=0o700)
    bindings.chmod(0o555)
    artifacts.chmod(0o555)
    release_compile_out_path.parent.chmod(0o555)
    ordinary_nm_child_log = Path(str(ordinary_nm_child["output_path"]))
    ordinary_nm_child_log.parent.chmod(0o555)
    prepared_root.chmod(0o555)
    attempt_approval_path = attempt_root / "source-approval.json"
    attempt_prepared_path = attempt_root / "prepared-artifacts.json"
    attempt_approval_path.write_bytes(original_approval_path.read_bytes())
    attempt_approval_path.chmod(0o444)
    attempt_prepared_path.write_bytes(original_prepared_path.read_bytes())
    attempt_prepared_path.chmod(0o444)
    exe = root / str(pid) / "exe"
    exe.symlink_to(binary)
    authority = synthetic_authority(
        variant,
        track,
        context,
        pid=pid,
        start_ticks=start_ticks,
        comm=comm,
    )
    authority.update(
        {
            "prepared_artifacts_path": str(attempt_prepared_path),
            "prepared_artifacts_sha256": prepared_sha,
            "source_approval_path": str(attempt_approval_path),
            "source_approval_sha256": approval_sha,
            "profile_adapter_path": str(adapter),
            "profile_adapter_sha256": adapter_sha,
            "profile_tools": tools,
            "executable_path": str(binary),
            "executable_sha256": binary_sha,
        }
    )
    return authority


class ParseTests(unittest.TestCase):
    def test_current_overlay_constant_matches_reviewed_patch(self) -> None:
        overlay = (
            MODULE_PATH.parent
            / "tooling"
            / "current"
            / "product-test-overlay.patch"
        )
        self.assertEqual(
            sha256(overlay.read_bytes()),
            adapters._CURRENT_PRODUCT_OVERLAY_SHA256,
        )

    def test_profile_contract_exposes_integration_obligations(self) -> None:
        contract = adapters.profile_contract()
        self.assertEqual(
            contract["profile_inputs_persistence"],
            {
                "payload": "child.profile_tool_inputs",
                "sha256": "child.profile_tool_inputs_sha256",
                "raw_artifacts": "one-fd-nofollow-0444-sha256-and-byte-length",
            },
        )
        self.assertEqual(
            contract["perf_disable_owner"],
            "child-at-t1-before-measured-serialization",
        )
        self.assertEqual(
            contract["perf_ack_ledger"],
            {
                "ownership": "one-shared-offset",
                "artifact_mode": 0o444,
                "exact_bytes_utf8": "ack\nack\n",
                "exact_bytes": 8,
                "sha256": sha256(b"ack\nack\n"),
            },
        )
        self.assertEqual(
            contract["c_role_lifetime_proof"],
            [
                "source-approval-static-proof",
                "prepared-binary-contract",
                "runner-child-timeout-cap",
            ],
        )
        self.assertEqual(
            contract["perf_child_environment"],
            {
                "cpu_all": ["ASTERISM_REBASELINE_PERF_PERMISSION_RESULT"],
                "cpu_available_only": [
                    "ASTERISM_REBASELINE_PERF_COMMAND_FD",
                    "ASTERISM_REBASELINE_PERF_ACK_FD",
                    "ASTERISM_REBASELINE_PERF_ACK_LEDGER_FD",
                ],
                "non_cpu": [],
            },
        )

    def test_proc_stat_handles_spaces_and_parentheses(self) -> None:
        payload = stat_record(42, "owner (one) thread", 987654)
        self.assertEqual(
            adapters.parse_proc_stat(payload, 42),
            (42, "owner (one) thread", 987654),
        )

    def test_proc_stat_rejects_identity_and_short_record(self) -> None:
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.parse_proc_stat(stat_record(42, "owner", 5), 41)
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.parse_proc_stat("42 (owner) S 1 2\n", 42)

    def test_proc_io_is_exact(self) -> None:
        parsed = adapters.parse_proc_io(io_record())
        self.assertEqual(parsed.read_bytes, 5)
        signed = io_record().replace("cancelled_write_bytes: 7", "cancelled_write_bytes: -7")
        self.assertEqual(adapters.parse_proc_io(signed).cancelled_write_bytes, -7)
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.parse_proc_io(io_record() + "extra: 9\n")

    def test_perf_parser_is_exact_and_integer_lossless(self) -> None:
        payload = "\n".join(
            (
                "9007199254740993,,cycles:u,5000,100.00,",
                "2000,,instructions:u,5000,100.00,",
                "3.5,msec,task-clock:u,5000,100.00,",
                "4,,context-switches:u,5000,100.00,",
            )
        )
        counters = adapters.parse_perf_stat_csv(payload)
        self.assertEqual(counters[0].value, 9007199254740993)
        self.assertEqual(counters[2].value, "3.5")
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.parse_perf_stat_csv(payload.replace("cycles:u", "cycles"))
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.parse_perf_stat_csv(
                payload.replace("4,,context-switches:u", "<not supported>,,context-switches:u")
            )
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.parse_perf_stat_csv(payload + "\n1,,cycles:u,1,100.00,")

    def test_perf_ack_is_exact(self) -> None:
        adapters.validate_perf_control_ack("ack\nack\n", perf_control_events())
        for payload in (
            "ack\n",
            " ack \n\nack\t\n",
            "ack\r\nack\r\n",
            "ack\n\nack\n",
            "ack\nack",
        ):
            with self.subTest(payload=payload), self.assertRaises(
                adapters.ProfileEvidenceError
            ):
                adapters.validate_perf_control_ack(payload, perf_control_events())
        inputs = adapters.perf_profile_inputs(
            "\n".join(
                (
                    "1000,,cycles,5000,100.00,",
                    "2000,,instructions:u,5000,100.00,",
                    "3.5,msec,task-clock:u,5000,100.00,",
                    "4,,context-switches:u,5000,100.00,",
                )
            ).replace("cycles,", "cycles:u,"),
            "ack\nack\n",
            "available;perf_event_paranoid=2;scope=user-only",
            control_events=perf_control_events(),
        )
        self.assertTrue(inputs["perf_control_acknowledged"])
        self.assertEqual(len(inputs["perf_counters"]), 4)
        unavailable = adapters.perf_profile_inputs(
            "", "", "not_available;perf_event_paranoid=4;scope=user-only;exit_status=255"
        )
        self.assertFalse(unavailable["perf_control_acknowledged"])
        self.assertEqual(unavailable["perf_control_events"], [])
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.perf_profile_inputs(
                "",
                "",
                "not_available;perf_event_paranoid=4;scope=user-only;exit_status=255",
                control_events=perf_control_events(),
            )
        self.assertTrue(
            all(
                counter["status"] == "not_available"
                for counter in unavailable["perf_counters"]
            )
        )
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.perf_profile_inputs("", "ack\n", "permission-denied")
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.perf_profile_inputs(
                inputs["perf_counters"] and "1,,cycles:u,1,100,\n",
                "ack\nack\n",
                "not_available;perf_event_paranoid=4;scope=user-only;exit_status=255",
            )
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.perf_profile_inputs(
                "",
                "",
                "not_available;perf_event_paranoid=4;scope=user-only;exit_status=0",
            )

    def test_strace_summary(self) -> None:
        payload = """% time     seconds  usecs/call     calls    errors syscall
------ ----------- ----------- --------- --------- ----------------
 80.00    0.000008           4         2           write
 20.00    0.000002           2         1         1 openat
------ ----------- ----------- --------- --------- ----------------
100.00    0.000010           3         3         1 total
"""
        parsed = {entry.syscall: entry for entry in adapters.parse_strace_summary(payload)}
        self.assertEqual(parsed["write"].calls, 2)
        self.assertEqual(parsed["openat"].errors, 1)

    def test_raw_trace_interval_excludes_markers_and_resumed_line(self) -> None:
        authority = synthetic_authority("A", "syscall_profiles", {}, pid=101)
        events = control_events(authority)
        boundary = trace_boundary(authority, events)
        begin = marker_line(authority, boundary["begin_event"])
        end = marker_line(authority, boundary["end_event"])
        payload = f"""{begin}
101 1.1 pwrite64(3, \"x\", 1, 0) = 1
[pid 102] 1.2 futex(0x1, FUTEX_WAIT, 0, NULL <unfinished ...>
[pid 102] 1.3 <... futex resumed>) = 0
101 1.4 fdatasync(3) = 0
{end}
"""
        counts = adapters.trace_interval_counts(
            payload,
            boundary,
            allowed_syscalls=("pwrite64", "futex", "fdatasync"),
        )
        self.assertEqual(counts, {"fdatasync": 1, "futex": 1, "pwrite64": 1})
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.trace_interval_counts(payload.replace(end, "101 write(9, \"missing\", 7) = 7"), boundary)
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.trace_interval_counts(
                payload.replace("101 1.1 pwrite64", "unparsed garbage\n101 1.1 pwrite64"),
                boundary,
            )
        forged = payload.replace(
            "101 1.1 pwrite64",
            f'101 openat(AT_FDCWD, {json.dumps(begin)}, O_RDONLY) = 4\n101 1.1 pwrite64',
        )
        self.assertEqual(
            adapters.trace_interval_counts(
                forged, boundary, allowed_syscalls=("pwrite64", "futex", "fdatasync")
            ),
            counts,
        )

    def test_raw_trace_metrics_partition_sync_and_file_operations(self) -> None:
        authority = synthetic_authority("A", "structural_traces", {}, pid=1)
        events = control_events(authority)
        boundary = trace_boundary(authority, events)
        payload = f"""{marker_line(authority, boundary["begin_event"])}
1 openat(AT_FDCWD, \"/store/meta\", O_CREAT|O_RDWR, 0600) = 3</store/meta>
1 renameat(AT_FDCWD, \"/store/a\", AT_FDCWD, \"/store/b\") = 0
1 unlink(\"/store/b\") = 0
1 fdatasync(4</store/log/active>) = 0
1 fsync(3</store/meta>) = 0
{marker_line(authority, boundary["end_event"])}
"""
        metrics = adapters.trace_interval_metrics(
            payload,
            boundary,
            log_path_markers=(trace_marker("directory_prefix", "/store/log/"),),
            metadata_path_markers=(trace_marker("exact", "/store/meta"),),
        )
        self.assertEqual(metrics["file_create"], 1)
        self.assertEqual(metrics["file_rename"], 1)
        self.assertEqual(metrics["file_unlink"], 1)
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.trace_interval_metrics(
                payload,
                boundary,
                log_path_markers=(1,),
                metadata_path_markers=(trace_marker("exact", "/store/meta"),),
            )
        self.assertEqual(metrics["log_sync_calls"], 1)
        self.assertEqual(metrics["metadata_sync_calls"], 1)
        normalized = adapters.strace_profile_inputs(
            payload,
            boundary,
            log_path_markers=(trace_marker("file_prefix", "/store/log/act"),),
            metadata_path_markers=(trace_marker("exact", "/store/meta"),),
        )
        self.assertEqual(normalized["begin_markers"], 1)
        self.assertEqual(normalized["trace_counts"], metrics)
        log_only_payload = payload.replace("1 fsync(3</store/meta>) = 0\n", "")
        log_only = adapters.trace_interval_metrics(
            log_only_payload,
            boundary,
            log_path_markers=(trace_marker("exact", "/store/log/active"),),
            metadata_path_markers=(),
        )
        self.assertEqual(log_only["metadata_sync_calls"], 0)
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.trace_interval_metrics(
                log_only_payload,
                boundary,
                log_path_markers=(),
                metadata_path_markers=(),
            )
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.trace_interval_metrics(
                log_only_payload,
                boundary,
                log_path_markers=(
                    trace_marker("directory_prefix", "/store/log"),
                ),
                metadata_path_markers=(),
            )
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.trace_interval_metrics(
                payload,
                boundary,
                log_path_markers=(trace_marker("directory_prefix", "/store/"),),
                metadata_path_markers=(trace_marker("exact", "/store/meta"),),
            )
        spoofed = payload.replace(
            "4</store/log/active>", "4</tmp/evil/store/log/active>"
        )
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.trace_interval_metrics(
                spoofed,
                boundary,
                log_path_markers=(trace_marker("directory_prefix", "/store/log/"),),
                metadata_path_markers=(trace_marker("exact", "/store/meta"),),
            )
        for target in ("/store/log/active (deleted)", r"/store/log/bad\x20name"):
            mutated = payload.replace("/store/log/active", target)
            with self.subTest(target=target), self.assertRaises(
                adapters.ProfileEvidenceError
            ):
                adapters.trace_interval_metrics(
                    mutated,
                    boundary,
                    log_path_markers=(
                        trace_marker("directory_prefix", "/store/log/"),
                    ),
                    metadata_path_markers=(trace_marker("exact", "/store/meta"),),
                )


class SnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.pid = 100
        self.tid = 101
        process = self.root / str(self.pid)
        task = process / "task" / str(self.tid)
        task.mkdir(parents=True)
        (process / "stat").write_text(stat_record(self.pid, "bench", 900))
        (process / "status").write_text(
            "Name:\tbench\nVmHWM:\t128 kB\n"
            "voluntary_ctxt_switches:\t3\n"
            "nonvoluntary_ctxt_switches:\t1\n"
        )
        (process / "io").write_text(io_record(10))
        (task / "stat").write_text(stat_record(self.tid, "mess-flat-owner", 901))
        (task / "schedstat").write_text("1000 20 3\n")
        (task / "status").write_text(
            "Name:\tmess-flat-owner\n"
            "voluntary_ctxt_switches:\t4\n"
            "nonvoluntary_ctxt_switches:\t2\n"
        )
        self.reader = adapters.ProcReader(self.root)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_identity_bound_task_and_process_deltas(self) -> None:
        identity = self.reader.tasks(self.pid)[0]
        before_task = self.reader.task_counters(identity)
        before_process = self.reader.process_counters(self.pid)

        task = self.root / str(self.pid) / "task" / str(self.tid)
        (task / "schedstat").write_text("1900 25 4\n")
        (task / "status").write_text(
            "voluntary_ctxt_switches: 9\nnonvoluntary_ctxt_switches: 3\n"
        )
        process = self.root / str(self.pid)
        (process / "status").write_text(
            "VmHWM: 256 kB\n"
            "voluntary_ctxt_switches: 8\n"
            "nonvoluntary_ctxt_switches: 3\n"
        )
        (process / "io").write_text(io_record(20))

        task_delta = adapters.task_delta(before_task, self.reader.task_counters(identity))
        process_delta = adapters.process_delta(
            before_process, self.reader.process_counters(self.pid)
        )
        self.assertEqual(task_delta.on_cpu_ns, 900)
        self.assertEqual(task_delta.voluntary_context_switches, 5)
        self.assertEqual(process_delta.io.read_bytes, 10)
        self.assertEqual(process_delta.vm_hwm_bytes, 256 * 1024)
        self.assertEqual(process_delta.voluntary_context_switches, 5)
        self.assertEqual(process_delta.nonvoluntary_context_switches, 2)

    def test_tid_reuse_is_rejected(self) -> None:
        identity = self.reader.tasks(self.pid)[0]
        path = self.root / str(self.pid) / "task" / str(self.tid) / "stat"
        path.write_text(stat_record(self.tid, "mess-flat-owner", 999))
        with self.assertRaises(adapters.ProfileEvidenceError):
            self.reader.task_counters(identity)


class AuthorityMutationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.mount_patch = mock.patch.object(
            sys.modules[__name__],
            "TRUSTED_SYSTEM_MOUNTS",
            fixture_trusted_mounts(self.root),
        )
        self.mount_patch.start()
        self.pid = 150
        process = self.root / str(self.pid)
        process.mkdir(parents=True)
        (process / "stat").write_text(stat_record(self.pid, "bench", 700))
        (process / "status").write_text(
            "VmHWM: 1 kB\nvoluntary_ctxt_switches: 0\n"
            "nonvoluntary_ctxt_switches: 0\n"
        )
        (process / "io").write_text(io_record())
        write_task(self.root, self.pid, self.pid, "bench", 700)
        self.authority = live_authority(
            self.root,
            pid=self.pid,
            start_ticks=700,
            comm="bench",
            variant="A",
            track="primary",
            context={},
        )

    def tearDown(self) -> None:
        try:
            self.temporary.cleanup()
        finally:
            self.mount_patch.stop()

    def construct(self, authority: dict[str, object] | None = None) -> object:
        return adapters.ProfileCoordinator.for_child(
            self.pid,
            "A",
            "primary",
            authority=self.authority if authority is None else authority,
            proc_root=self.root,
        )

    def authority_paths(self) -> dict[str, Path]:
        attempt_prepared = Path(str(self.authority["prepared_artifacts_path"]))
        attempt_approval = Path(str(self.authority["source_approval_path"]))
        prepared = json.loads(attempt_prepared.read_bytes())
        # Protocol v4: the live claim is the run-local run-claim.json.
        claim = attempt_prepared.parent / "run-claim.json"
        claim_value = json.loads(claim.read_bytes())
        original_prepared = Path(str(claim_value["prepared_artifacts_path"]))
        original = json.loads(original_prepared.read_bytes())
        original_approval = Path(str(original["source_approval"]["path"]))
        source_review = original["source_review"]
        return {
            "attempt_prepared": attempt_prepared,
            "attempt_approval": attempt_approval,
            "claim": claim,
            "original_prepared": original_prepared,
            "original_approval": original_approval,
            "release_compile_out": Path(
                str(original["release_compile_out"]["path"])
            ),
            **{
                f"source_review_{name}": Path(str(binding["path"]))
                for name, binding in source_review.items()
            },
        }

    def rewrite_complete_chain(
        self,
        *,
        mutate_approval: object | None = None,
        mutate_prepared: object | None = None,
    ) -> dict[str, object]:
        paths = self.authority_paths()
        approval = json.loads(paths["original_approval"].read_bytes())
        if callable(mutate_approval):
            mutate_approval(approval)
        paths["original_approval"].chmod(0o644)
        approval_sha = write_canonical(
            paths["original_approval"], approval, 0o444
        )
        paths["attempt_approval"].chmod(0o644)
        paths["attempt_approval"].write_bytes(
            paths["original_approval"].read_bytes()
        )
        paths["attempt_approval"].chmod(0o444)

        prepared = json.loads(paths["original_prepared"].read_bytes())
        prepared["source_approval"]["sha256"] = approval_sha
        if callable(mutate_prepared):
            mutate_prepared(prepared)
        paths["original_prepared"].chmod(0o644)
        prepared_sha = write_canonical(
            paths["original_prepared"], prepared, 0o444
        )
        paths["attempt_prepared"].chmod(0o644)
        paths["attempt_prepared"].write_bytes(
            paths["original_prepared"].read_bytes()
        )
        paths["attempt_prepared"].chmod(0o444)

        claim = json.loads(paths["claim"].read_bytes())
        claim["prepared_artifacts_sha256"] = prepared_sha
        paths["claim"].chmod(0o644)
        write_canonical(paths["claim"], claim, 0o444)
        return {
            **self.authority,
            "source_approval_sha256": approval_sha,
            "prepared_artifacts_sha256": prepared_sha,
        }

    def rewrite_release_proof(self, mutate: object) -> dict[str, object]:
        paths = self.authority_paths()
        proof_path = paths["release_compile_out"]
        proof = json.loads(proof_path.read_bytes())
        if callable(mutate):
            mutate(proof)
        proof_path.chmod(0o644)
        proof_sha = write_canonical(proof_path, proof, 0o444)
        return self.rewrite_complete_chain(
            mutate_prepared=lambda value: value["release_compile_out"].__setitem__(
                "sha256", proof_sha
            )
        )

    def test_distinct_attempt_copies_replay_original_authority(self) -> None:
        paths = self.authority_paths()
        self.assertNotEqual(
            paths["attempt_prepared"], paths["original_prepared"]
        )
        self.assertNotEqual(
            paths["attempt_approval"], paths["original_approval"]
        )
        self.assertEqual(
            paths["attempt_prepared"].read_bytes(),
            paths["original_prepared"].read_bytes(),
        )
        self.assertEqual(
            paths["attempt_approval"].read_bytes(),
            paths["original_approval"].read_bytes(),
        )
        self.construct()

    def test_protocol_context_and_executable_mutations_fail(self) -> None:
        for field, value in (
            ("protocol_sha256", "0" * 64),
            ("context_sha256", "0" * 64),
            ("executable_sha256", "0" * 64),
            ("source_commit", "0" * 40),
        ):
            mutated = dict(self.authority)
            mutated[field] = value
            with self.subTest(field=field), self.assertRaises(
                adapters.ProfileEvidenceError
            ):
                self.construct(mutated)

    def test_source_approval_schema_identity_is_exact(self) -> None:
        approval = json.loads(
            self.authority_paths()["original_approval"].read_bytes()
        )
        self.assertEqual(approval["schema"], "bn-2l3n-source-approval-v3")
        mutated = self.rewrite_complete_chain(
            mutate_approval=lambda value: value.__setitem__(
                "schema", "asterism-rebaseline-source-approval-v3"
            )
        )
        with self.assertRaises(adapters.ProfileEvidenceError):
            self.construct(mutated)

    def test_prepared_artifacts_schema_identity_is_exact(self) -> None:
        prepared = json.loads(
            self.authority_paths()["original_prepared"].read_bytes()
        )
        self.assertEqual(prepared["schema"], "bn-2l3n-prepared-artifacts-v3")
        mutated = self.rewrite_complete_chain(
            mutate_prepared=lambda value: value.__setitem__(
                "schema", "asterism-rebaseline-prepared-v3"
            )
        )
        with self.assertRaises(adapters.ProfileEvidenceError):
            self.construct(mutated)
        truncated_a = self.rewrite_complete_chain(
            mutate_prepared=lambda value: value["variants"]["A"].pop("binary")
        )
        with self.assertRaises(adapters.ProfileEvidenceError):
            self.construct(truncated_a)

    def test_mutable_or_symlinked_authority_files_fail(self) -> None:
        paths = self.authority_paths()
        for name in (
            "attempt_prepared",
            "attempt_approval",
            "original_prepared",
            "original_approval",
            "claim",
            "release_compile_out",
            "source_review_bundle",
            "source_review_current_children_attestation",
            "source_review_lock_authority",
            "source_review_lock_review_bundle",
        ):
            path = paths[name]
            path.chmod(0o644)
            with self.subTest(name=name), self.assertRaises(
                adapters.ProfileEvidenceError
            ):
                self.construct()
            path.chmod(0o444)
        attempt_prepared = paths["attempt_prepared"]
        payload = attempt_prepared.read_bytes()
        attempt_prepared.chmod(0o644)
        attempt_prepared.unlink()
        target = attempt_prepared.with_suffix(".target")
        target.write_bytes(payload)
        target.chmod(0o444)
        attempt_prepared.symlink_to(target)
        with self.assertRaises(adapters.ProfileEvidenceError):
            self.construct()

    def test_release_proof_must_bind_real_source_approval(self) -> None:
        mutated = self.rewrite_release_proof(
            lambda value: value.__setitem__("source_approval_sha256", "0" * 64)
        )
        with self.assertRaises(adapters.ProfileEvidenceError):
            self.construct(mutated)

    def test_proof_only_twin_cannot_become_prepared_reachable(self) -> None:
        proof = json.loads(
            self.authority_paths()["release_compile_out"].read_bytes()
        )
        twin = proof["binaries"]["overlay_a"]["path"]
        mutated = self.rewrite_complete_chain(
            mutate_prepared=lambda value: value.__setitem__(
                "proof_only_twin", twin
            )
        )
        with self.assertRaises(adapters.ProfileEvidenceError):
            self.construct(mutated)

    def test_role_lifetime_contract_is_source_and_binary_bound(self) -> None:
        mutated = self.rewrite_complete_chain(
            mutate_approval=lambda value: value["variants"]["A"].__setitem__(
                "profile_role_lifetime", adapters.C_ROLE_LIFETIME_CONTRACT
            ),
            mutate_prepared=lambda value: value["variants"]["A"][
                "contract"
            ].__setitem__(
                "profile_role_lifetime", adapters.C_ROLE_LIFETIME_CONTRACT
            ),
        )
        with self.assertRaises(adapters.ProfileEvidenceError):
            self.construct(mutated)

    def test_rebound_or_divergent_attempt_manifest_is_rejected(self) -> None:
        paths = self.authority_paths()
        prepared = json.loads(paths["attempt_prepared"].read_bytes())
        prepared["source_approval"]["path"] = str(paths["attempt_approval"])
        paths["attempt_prepared"].chmod(0o644)
        digest = write_canonical(paths["attempt_prepared"], prepared, 0o444)
        with self.assertRaises(adapters.ProfileEvidenceError):
            self.construct({**self.authority, "prepared_artifacts_sha256": digest})

    def test_attempt_authority_paths_must_be_exact_siblings(self) -> None:
        paths = self.authority_paths()
        alias = paths["attempt_prepared"].with_name("prepared-alias.json")
        alias.write_bytes(paths["attempt_prepared"].read_bytes())
        alias.chmod(0o444)
        with self.assertRaises(adapters.ProfileEvidenceError):
            self.construct({**self.authority, "prepared_artifacts_path": str(alias)})

    def test_original_authority_paths_cannot_replace_attempt_paths(self) -> None:
        paths = self.authority_paths()
        for field, path in (
            ("prepared_artifacts_path", paths["original_prepared"]),
            ("source_approval_path", paths["original_approval"]),
        ):
            with self.subTest(field=field), self.assertRaises(
                adapters.ProfileEvidenceError
            ):
                self.construct({**self.authority, field: str(path)})

    def test_attempt_approval_divergence_is_rejected_after_rehash(self) -> None:
        paths = self.authority_paths()
        approval = json.loads(paths["attempt_approval"].read_bytes())
        approval["attempt_only"] = True
        paths["attempt_approval"].chmod(0o644)
        digest = write_canonical(paths["attempt_approval"], approval, 0o444)
        with self.assertRaises(adapters.ProfileEvidenceError):
            self.construct({**self.authority, "source_approval_sha256": digest})

    def test_attempt_prepared_hardlink_to_original_is_rejected(self) -> None:
        paths = self.authority_paths()
        paths["attempt_prepared"].unlink()
        os.link(paths["original_prepared"], paths["attempt_prepared"])
        self.assertTrue(
            os.path.samestat(
                paths["original_prepared"].stat(),
                paths["attempt_prepared"].stat(),
            )
        )
        with self.assertRaises(adapters.ProfileEvidenceError):
            self.construct()

    def test_attempt_approval_hardlink_to_original_is_rejected(self) -> None:
        paths = self.authority_paths()
        paths["attempt_approval"].unlink()
        os.link(paths["original_approval"], paths["attempt_approval"])
        self.assertTrue(
            os.path.samestat(
                paths["original_approval"].stat(),
                paths["attempt_approval"].stat(),
            )
        )
        with self.assertRaises(adapters.ProfileEvidenceError):
            self.construct()

    def test_claim_cannot_bind_attempt_manifest(self) -> None:
        paths = self.authority_paths()
        claim = json.loads(paths["claim"].read_bytes())
        claim["prepared_artifacts_path"] = str(paths["attempt_prepared"])
        paths["claim"].chmod(0o644)
        write_canonical(paths["claim"], claim, 0o444)
        with self.assertRaises(adapters.ProfileEvidenceError):
            self.construct()

    def test_missing_run_claim_record_fails(self) -> None:
        # Protocol v4: the run-local claim record must exist; a missing
        # run-claim.json cannot establish profile authority.
        paths = self.authority_paths()
        paths["claim"].chmod(0o644)
        paths["claim"].unlink()
        with self.assertRaises(adapters.ProfileEvidenceError):
            self.construct()

    def test_claim_attempt_and_lease_identity_are_exact(self) -> None:
        paths = self.authority_paths()
        for field, value in (
            ("output_dir", str(self.root / "other-attempt")),
            ("attempt_nonce", "2" * 64),
            ("lease_nonce", "not-a-lease"),
        ):
            claim = json.loads(paths["claim"].read_bytes())
            original = claim[field]
            claim[field] = value
            paths["claim"].chmod(0o644)
            write_canonical(paths["claim"], claim, 0o444)
            with self.subTest(field=field), self.assertRaises(
                adapters.ProfileEvidenceError
            ):
                self.construct()
            claim[field] = original
            paths["claim"].chmod(0o644)
            write_canonical(paths["claim"], claim, 0o444)

    def test_symlinked_adapter_and_proc_root_fail(self) -> None:
        adapter = Path(str(self.authority["profile_adapter_path"]))
        payload = adapter.read_bytes()
        adapter.parent.chmod(0o755)
        adapter.chmod(0o644)
        adapter.unlink()
        target = adapter.with_suffix(".target")
        target.write_bytes(payload)
        target.chmod(0o444)
        adapter.symlink_to(target)
        with self.assertRaises(adapters.ProfileEvidenceError):
            self.construct()
        proc_link = self.root.parent / f"{self.root.name}-link"
        proc_link.symlink_to(self.root, target_is_directory=True)
        try:
            with self.assertRaises(adapters.ProfileEvidenceError):
                adapters.ProcReader(proc_link)
        finally:
            proc_link.unlink()


class RoleAndResolutionTests(unittest.TestCase):
    def identity(self, tid: int, start: int, comm: str) -> object:
        return adapters.TaskIdentity(1, tid, start, comm)

    def test_role_births_and_named_owner(self) -> None:
        before = (self.identity(1, 10, "bench"),)
        owner = self.identity(2, 11, "mess-flat-owner")
        helper = self.identity(3, 12, "fjall-worker")
        after = (*before, owner, helper)
        self.assertEqual(adapters.require_unique_comm(after, "mess-flat-owner", "owner"), owner)
        self.assertEqual(
            adapters.bind_unique_birth(
                "committer", before, after, excluded_comms=("fjall-worker",)
            ),
            owner,
        )
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.bind_unique_birth("committer", before, after)

    def test_tid_reuse_across_phase_is_rejected(self) -> None:
        before = (self.identity(2, 10, "old"),)
        after = (self.identity(2, 11, "new"),)
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.task_births(before, after)

    def test_schedstat_resolution_and_floor(self) -> None:
        resolution = adapters.schedstat_resolution((100, 100, 150, 240))
        self.assertEqual(resolution.minimum_nonzero_increment_ns, 50)
        adapters.require_measurable_role_cpu(1000, resolution)
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.require_measurable_role_cpu(999, resolution)
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.schedstat_resolution((100, 90, 120))

    def test_allocation_and_rusage_deltas_fail_closed(self) -> None:
        allocation = adapters.allocation_delta(
            adapters.AllocationCounters(100, 1000),
            adapters.AllocationCounters(125, 1400),
        )
        self.assertEqual(allocation, adapters.AllocationCounters(25, 400))
        rusage = adapters.rusage_delta(
            adapters.RusageCounters(50, 10), adapters.RusageCounters(80, 20)
        )
        self.assertEqual(rusage, adapters.RusageCounters(30, 10))
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.allocation_delta(
                adapters.AllocationCounters(100, 1000),
                adapters.AllocationCounters(99, 1400),
            )


class CoordinatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.mount_patch = mock.patch.object(
            sys.modules[__name__],
            "TRUSTED_SYSTEM_MOUNTS",
            fixture_trusted_mounts(self.root),
        )
        self.mount_patch.start()
        self.pid = 200
        process = self.root / str(self.pid)
        process.mkdir(parents=True)
        (process / "stat").write_text(stat_record(self.pid, "public-bench", 1000))
        (process / "status").write_text(
            "VmHWM: 100 kB\n"
            "voluntary_ctxt_switches: 10\n"
            "nonvoluntary_ctxt_switches: 2\n"
        )
        (process / "io").write_text(io_record(100))
        write_task(self.root, self.pid, self.pid, "public-bench", 1000)

    def authority(
        self, variant: str, track: str, context: dict[str, object]
    ) -> dict[str, object]:
        return live_authority(
            self.root,
            pid=self.pid,
            start_ticks=1000,
            comm="public-bench",
            variant=variant,
            track=track,
            context=context,
        )

    def tearDown(self) -> None:
        try:
            self.temporary.cleanup()
        finally:
            self.mount_patch.stop()

    def test_fjall_phase_roles_and_window(self) -> None:
        context = {"cell": "group-b1"}
        authority = self.authority("C", "cpu_profiles", context)
        coordinator = adapters.ProfileCoordinator.for_child(
            self.pid,
            "C",
            "cpu_profiles",
            authority=authority,
            context=context,
            proc_root=self.root,
        )
        coordinator.capture_phase("boot")
        write_task(self.root, self.pid, 201, "tokio-runtime-w", 1001, on_cpu_ns=50)
        coordinator.capture_phase("runtime")
        write_task(self.root, self.pid, 202, "public-bench", 1002, on_cpu_ns=100)
        coordinator.capture_phase("opened")
        coordinator.capture_phase("ready")
        start = coordinator.begin()
        self.assertEqual([role.label for role in start.roles], ["committer", "producer-runtime"])

        write_task(
            self.root,
            self.pid,
            201,
            "tokio-runtime-w",
            1001,
            on_cpu_ns=550,
            voluntary=3,
        )
        write_task(
            self.root,
            self.pid,
            202,
            "public-bench",
            1002,
            on_cpu_ns=900,
            nonvoluntary=2,
        )
        write_task(
            self.root,
            self.pid,
            203,
            "tokio-runtime-w",
            1003,
            on_cpu_ns=300,
            voluntary=1,
        )
        process = self.root / str(self.pid)
        (process / "status").write_text(
            "VmHWM: 200 kB\n"
            "voluntary_ctxt_switches: 14\n"
            "nonvoluntary_ctxt_switches: 5\n"
        )
        (process / "io").write_text(io_record(110))
        result = coordinator.end()
        rendered = coordinator.finish()
        roles = {role["label"]: role for role in rendered["roles"]}
        self.assertEqual(roles["committer"]["on_cpu_ns"], 800)
        self.assertEqual(roles["producer-runtime"]["on_cpu_ns"], 500)
        self.assertEqual(roles["spawn_blocking-publication"]["on_cpu_ns"], 300)
        self.assertTrue(roles["spawn_blocking-publication"]["born_in_window"])
        self.assertEqual(rendered["context"], {"cell": "group-b1"})
        self.assertEqual(result.process.io.read_bytes, 10)

    def test_phase_order_and_early_finish_fail(self) -> None:
        authority = self.authority("A", "primary", {})
        coordinator = adapters.ProfileCoordinator.for_child(
            self.pid, "A", "primary", authority=authority, proc_root=self.root
        )
        with self.assertRaises(adapters.ProfileEvidenceError):
            coordinator.capture_phase("runtime")
        coordinator.capture_phase("boot")
        with self.assertRaises(adapters.ProfileEvidenceError):
            coordinator.capture_phase("boot")
        with self.assertRaises(adapters.ProfileEvidenceError):
            coordinator.finish()
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.ProfileCoordinator.for_child(
                self.pid,
                "A",
                "primary",
                authority=authority,
                context={"bad": math.nan},
                proc_root=self.root,
            )

    def test_reopen_phase_order_keeps_open_after_start_boundary(self) -> None:
        context = {"trace_kind": "reopen"}
        authority = self.authority("A", "reopen", context)
        coordinator = adapters.ProfileCoordinator.for_child(
            self.pid,
            "A",
            "reopen",
            authority=authority,
            context=context,
            proc_root=self.root,
        )
        coordinator.capture_phase("boot")
        coordinator.capture_phase("runtime")
        coordinator.capture_phase("ready")
        start = coordinator.begin()
        self.assertEqual(start.roles, ())
        # The timed open may legitimately create engine workers. Reopen does
        # not relabel them as append-critical roles.
        write_task(self.root, self.pid, 250, "mess-flat-owner", 1050)
        coordinator.capture_phase("opened")
        process = self.root / str(self.pid)
        (process / "status").write_text(
            "VmHWM: 220 kB\n"
            "voluntary_ctxt_switches: 13\n"
            "nonvoluntary_ctxt_switches: 4\n"
        )
        (process / "io").write_text(io_record(120))
        coordinator.end()
        # `measured` is proof-only for reopen; the process/io delta was frozen
        # at the immediately post-open `opened` stop.
        coordinator.capture_phase("measured")
        result = coordinator.finish()
        self.assertEqual(
            [phase["phase"] for phase in result["phase_snapshots"]],
            ["boot", "runtime", "ready", "opened", "measured"],
        )
        fields = adapters.profile_fields(
            "reopen",
            result,
            raw_point={"track": "reopen", "variant": "A"},
            control_events=control_events(authority, reopen=True),
            authority=authority,
        )
        self.assertEqual(fields["peak_rss_bytes"], 220 * 1024)
        self.assertEqual(fields["proc_read_syscalls"], 20)

    def test_unreviewed_open_birth_and_transient_publication_fail(self) -> None:
        context = {"cell": "group-b1"}
        authority = self.authority("C", "cpu_profiles", context)
        coordinator = adapters.ProfileCoordinator.for_child(
            self.pid,
            "C",
            "cpu_profiles",
            authority=authority,
            context=context,
            proc_root=self.root,
        )
        coordinator.capture_phase("boot")
        write_task(self.root, self.pid, 201, adapters.TOKIO_WORKER_COMM, 1001)
        coordinator.capture_phase("runtime")
        write_task(self.root, self.pid, 202, "public-bench", 1002)
        write_task(self.root, self.pid, 204, "unreviewed", 1004)
        coordinator.capture_phase("opened")
        coordinator.capture_phase("ready")
        with self.assertRaises(adapters.ProfileEvidenceError):
            coordinator.begin()

        for tid in (201, 202, 204):
            shutil.rmtree(self.root / str(self.pid) / "task" / str(tid))
        coordinator = adapters.ProfileCoordinator.for_child(
            self.pid,
            "C",
            "cpu_profiles",
            authority=authority,
            context=context,
            proc_root=self.root,
        )
        coordinator.capture_phase("boot")
        write_task(self.root, self.pid, 201, adapters.TOKIO_WORKER_COMM, 1001)
        coordinator.capture_phase("runtime")
        write_task(self.root, self.pid, 202, "public-bench", 1002)
        coordinator.capture_phase("opened")
        coordinator.capture_phase("ready")
        coordinator.begin()
        write_task(self.root, self.pid, 205, adapters.TOKIO_WORKER_COMM, 1005)
        shutil.rmtree(self.root / str(self.pid) / "task" / "205")
        with self.assertRaises(adapters.ProfileEvidenceError):
            coordinator.end()


class ProfileFieldTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.artifact_root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def raw_artifact(self, name: str, payload: bytes) -> dict[str, object]:
        path = self.artifact_root / name
        path.write_bytes(payload)
        path.chmod(0o444)
        return {
            "path": str(path),
            "sha256": sha256(payload),
            "bytes": len(payload),
            "mode": 0o444,
        }

    @staticmethod
    def process() -> dict[str, object]:
        return {
            "pid": 10,
            "start_ticks": 100,
            "vm_hwm_bytes": 4096,
            "voluntary_context_switches": 8,
            "nonvoluntary_context_switches": 3,
            "io": {
                "rchar": 1,
                "wchar": 2,
                "syscr": 3,
                "syscw": 4,
                "read_bytes": 5,
                "write_bytes": 6,
                "cancelled_write_bytes": 0,
            },
        }

    @classmethod
    def rich(
        cls,
        track: str,
        *,
        variant: str = "A",
        roles: list[dict[str, object]] | None = None,
        context: dict[str, object] | None = None,
    ) -> dict[str, object]:
        context = {} if context is None else context
        if track in {"syscall_profiles", "structural_traces"}:
            context = {
                **context,
                "trace_path_markers": {
                    "log": [trace_marker("directory_prefix", "/store/log/")],
                    "metadata": [trace_marker("exact", "/store/meta")],
                },
            }
        reopen = track == "reopen" or (
            track == "structural_traces" and context.get("trace_kind") == "reopen"
        )
        phases = (
            adapters.ProfileCoordinator.REOPEN_PHASES
            if reopen
            else adapters.ProfileCoordinator.APPEND_PHASES
        )
        authority = synthetic_authority(variant, track, context)
        main_task = {
            "pid": 10,
            "tid": 10,
            "start_ticks": 100,
            "comm": "bench",
        }
        role_values = [] if roles is None else roles
        snapshots = []
        for phase in phases:
            tasks = [main_task]
            for role in role_values:
                if role["label"] == "spawn_blocking-publication" and phase != phases[-1]:
                    continue
                tasks.extend(task["identity"] for task in role["tasks"])
            snapshots.append(
                {
                    "phase": phase,
                    "pid": 10,
                    "process_start_ticks": 100,
                    "tasks": tasks,
                }
            )
        return {
            "schema": adapters.PROFILE_SCHEMA,
            "protocol": adapters.PROTOCOL,
            "authority": authority,
            "variant": variant,
            "track": track,
            "context": context,
            "process": cls.process(),
            "roles": role_values,
            "phase_snapshots": snapshots,
            "unattributed_births": [],
        }

    @staticmethod
    def owner_role(cpu_ns: int = 400) -> dict[str, object]:
        return {
            "label": "owner",
            "born_in_window": False,
            "on_cpu_ns": cpu_ns,
            "voluntary_context_switches": 2,
            "nonvoluntary_context_switches": 1,
            "tasks": [
                {
                    "identity": {
                        "pid": 10,
                        "tid": 11,
                        "start_ticks": 101,
                        "comm": "mess-flat-owner",
                    },
                    "on_cpu_ns": cpu_ns,
                    "voluntary_context_switches": 2,
                    "nonvoluntary_context_switches": 1,
                }
            ],
        }

    @staticmethod
    def role(
        label: str,
        tid: int,
        start_ticks: int,
        comm: str,
        *,
        born_in_window: bool = False,
    ) -> dict[str, object]:
        return {
            "label": label,
            "born_in_window": born_in_window,
            "on_cpu_ns": 2_000,
            "voluntary_context_switches": 2,
            "nonvoluntary_context_switches": 1,
            "tasks": [
                {
                    "identity": {
                        "pid": 10,
                        "tid": tid,
                        "start_ticks": start_ticks,
                        "comm": comm,
                    },
                    "on_cpu_ns": 2_000,
                    "voluntary_context_switches": 2,
                    "nonvoluntary_context_switches": 1,
                }
            ],
        }

    @classmethod
    def c_rich(cls) -> dict[str, object]:
        roles = [
            cls.role("committer", 12, 102, "bench"),
            cls.role("producer-runtime", 11, 101, adapters.TOKIO_WORKER_COMM),
            cls.role(
                "spawn_blocking-publication",
                13,
                103,
                adapters.TOKIO_WORKER_COMM,
                born_in_window=True,
            ),
        ]
        main = {"pid": 10, "tid": 10, "start_ticks": 100, "comm": "bench"}
        producer = roles[1]["tasks"][0]["identity"]
        committer = roles[0]["tasks"][0]["identity"]
        publication = roles[2]["tasks"][0]["identity"]
        tasks = {
            "boot": [main],
            "runtime": [main, producer],
            "opened": [main, producer, committer],
            "ready": [main, producer, committer],
            "measured": [main, producer, committer, publication],
        }
        context: dict[str, object] = {}
        authority = synthetic_authority("C", "cpu_profiles", context)
        return {
            "schema": adapters.PROFILE_SCHEMA,
            "protocol": adapters.PROTOCOL,
            "authority": authority,
            "variant": "C",
            "track": "cpu_profiles",
            "context": context,
            "process": cls.process(),
            "roles": roles,
            "phase_snapshots": [
                {
                    "phase": phase,
                    "pid": 10,
                    "process_start_ticks": 100,
                    "tasks": phase_tasks,
                }
                for phase, phase_tasks in tasks.items()
            ],
            "unattributed_births": [],
        }

    def test_role_replay_rejects_forged_births_and_hidden_tasks(self) -> None:
        rich = self.c_rich()
        self.assertEqual(
            [role["label"] for role in adapters._roles(rich)],
            ["committer", "producer-runtime", "spawn_blocking-publication"],
        )
        phase_index = {
            phase["phase"]: index
            for index, phase in enumerate(rich["phase_snapshots"])
        }
        mutations: list[dict[str, object]] = []
        committer_early = json.loads(adapters.canonical_json(rich))
        committer_early["phase_snapshots"][phase_index["runtime"]]["tasks"].append(
            rich["roles"][0]["tasks"][0]["identity"]
        )
        mutations.append(committer_early)
        producer_early = json.loads(adapters.canonical_json(rich))
        producer_early["phase_snapshots"][phase_index["boot"]]["tasks"].append(
            rich["roles"][1]["tasks"][0]["identity"]
        )
        mutations.append(producer_early)
        hidden_terminal = json.loads(adapters.canonical_json(rich))
        hidden_terminal["phase_snapshots"][phase_index["measured"]]["tasks"].append(
            {
                "pid": 10,
                "tid": 14,
                "start_ticks": 104,
                "comm": adapters.TOKIO_WORKER_COMM,
            }
        )
        mutations.append(hidden_terminal)
        unreviewed_open = json.loads(adapters.canonical_json(rich))
        unreviewed_open["phase_snapshots"][phase_index["opened"]]["tasks"].append(
            {"pid": 10, "tid": 15, "start_ticks": 105, "comm": "unreviewed"}
        )
        mutations.append(unreviewed_open)
        for index, mutated in enumerate(mutations):
            with self.subTest(index=index), self.assertRaises(
                adapters.ProfileEvidenceError
            ):
                adapters._roles(mutated)

    def test_primary_fields_are_exact_and_apply_resolution_floor(self) -> None:
        rich = self.rich("primary", roles=[self.owner_role()])
        inputs = {"schedstat_resolution_ns": 20}
        fields = adapters.profile_fields(
            "primary",
            rich,
            raw_point={"track": "primary", "variant": "A"},
            control_events=control_events(rich["authority"]),
            authority=rich["authority"],
            profile_inputs=inputs,
        )
        self.assertEqual(fields["process_user_cpu_ns"], 20)
        self.assertEqual(fields["serialized_role"], "mess-flat-owner")
        self.assertEqual(fields["serialized_role_cpu_ns"], 400)
        inputs["schedstat_resolution_ns"] = 21
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.profile_fields(
                "primary",
                rich,
                raw_point={"track": "primary", "variant": "A"},
                control_events=control_events(rich["authority"]),
                authority=rich["authority"],
                profile_inputs=inputs,
            )

    def test_cpu_fields_flatten_roles_and_perf(self) -> None:
        rich = self.rich("cpu_profiles", roles=[self.owner_role(2_000)])
        stat_payload = "\n".join(
            (
                "1000,,cycles:u,5000,100.0,",
                "2000,,instructions:u,5000,100.0,",
                "1.5,msec,task-clock:u,5000,100.0,",
                "8,,context-switches:u,5000,100.0,",
            )
        ).encode()
        ack = b"ack\nack\n"
        events = control_events(rich["authority"])
        inputs = {
            "schedstat_resolution_ns": 50,
            "perf_permission": "available;perf_event_paranoid=2;scope=user-only",
            "perf_control_events": perf_control_events(),
            "perf_raw_artifacts": {
                "stat": self.raw_artifact("perf.csv", stat_payload),
                "ack": self.raw_artifact("perf.ack", ack),
            },
        }
        fields = adapters.profile_fields(
            "cpu_profiles",
            rich,
            raw_point={"track": "cpu_profiles", "variant": "A"},
            control_events=events,
            authority=rich["authority"],
            profile_inputs=inputs,
        )
        self.assertEqual(fields["task_clock_ns"], 1_500_000)
        self.assertEqual(fields["role_samples_json"][0]["role"], "mess-flat-owner")
        self.assertEqual(fields["process_voluntary_switches"], 8)

        for field, value in (
            ("nonce", "0" * 64),
            ("ack_received_monotonic_ns", 81),
        ):
            mutated = {
                **inputs,
                "perf_control_events": json.loads(
                    adapters.canonical_json(inputs["perf_control_events"])
                ),
            }
            mutated["perf_control_events"][0][field] = value
            with self.subTest(field=field), self.assertRaises(
                adapters.ProfileEvidenceError
            ):
                adapters.profile_fields(
                    "cpu_profiles",
                    rich,
                    raw_point={"track": "cpu_profiles", "variant": "A"},
                    control_events=events,
                    authority=rich["authority"],
                    profile_inputs=mutated,
                )
        mutated = {
            **inputs,
            "perf_control_events": json.loads(
                adapters.canonical_json(inputs["perf_control_events"])
            ),
        }
        mutated["perf_control_events"][1]["sent_monotonic_ns"] = 89
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.profile_fields(
                "cpu_profiles",
                rich,
                raw_point={"track": "cpu_profiles", "variant": "A"},
                control_events=events,
                authority=rich["authority"],
                profile_inputs=mutated,
            )
        late_inputs = {
            **inputs,
            "perf_control_events": json.loads(
                adapters.canonical_json(inputs["perf_control_events"])
            ),
        }
        late_events = json.loads(adapters.canonical_json(events))
        late_inputs["perf_control_events"][1].update(
            {"sent_monotonic_ns": 89, "ack_received_monotonic_ns": 90}
        )
        late_events[8]["perf_disable"] = late_inputs["perf_control_events"][1]
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.profile_fields(
                "cpu_profiles",
                rich,
                raw_point={"track": "cpu_profiles", "variant": "A"},
                control_events=late_events,
                authority=rich["authority"],
                profile_inputs=late_inputs,
            )
        bad_binding = {
            **inputs,
            "perf_raw_artifacts": json.loads(
                adapters.canonical_json(inputs["perf_raw_artifacts"])
            ),
        }
        bad_binding["perf_raw_artifacts"]["stat"]["sha256"] = "0" * 64
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.profile_fields(
                "cpu_profiles",
                rich,
                raw_point={"track": "cpu_profiles", "variant": "A"},
                control_events=events,
                authority=rich["authority"],
                profile_inputs=bad_binding,
            )

        unavailable_inputs = {
            "schedstat_resolution_ns": 50,
            "perf_permission": (
                "not_available;perf_event_paranoid=4;scope=user-only;exit_status=255"
            ),
            "perf_control_events": [],
            "perf_raw_artifacts": {},
        }
        unavailable_rich = json.loads(adapters.canonical_json(rich))
        unavailable_authority = unavailable_rich["authority"]
        unavailable_authority["perf_permission_result"] = unavailable_inputs[
            "perf_permission"
        ]
        unavailable_events = control_events(
            unavailable_authority, perf_available=False
        )
        unavailable = adapters.profile_fields(
            "cpu_profiles",
            unavailable_rich,
            raw_point={"track": "cpu_profiles", "variant": "A"},
            control_events=unavailable_events,
            authority=unavailable_authority,
            profile_inputs=unavailable_inputs,
        )
        self.assertEqual(unavailable["cycles"], "not_available")

    def test_trace_field_profiles_are_exact(self) -> None:
        syscall_rich = self.rich("syscall_profiles")
        syscall_authority = syscall_rich["authority"]
        syscall_events = control_events(syscall_authority)
        syscall_boundary = trace_boundary(syscall_authority, syscall_events)
        syscall_payload = f"""{marker_line(syscall_authority, syscall_boundary["begin_event"])}
10 write(4</store/log/active>, "x", 1) = 1
10 write(4</store/log/active>, "y", 1) = 1
10 pwritev(4</store/log/active>, [], 0, 0) = 0
10 fdatasync(4</store/log/active>) = 0
10 mkdir("/store/new", 0700) = 0
10 rename("/store/new", "/store/renamed") = 0
10 unlink("/store/renamed") = 0
{marker_line(syscall_authority, syscall_boundary["end_event"])}
""".encode()
        syscall = adapters.profile_fields(
            "syscall_profiles",
            syscall_rich,
            raw_point={"track": "syscall_profiles", "variant": "A"},
            control_events=syscall_events,
            authority=syscall_authority,
            profile_inputs={
                "trace_raw_artifact": self.raw_artifact("syscall.strace", syscall_payload),
                "log_path_markers": [
                    trace_marker("directory_prefix", "/store/log/")
                ],
                "metadata_path_markers": [trace_marker("exact", "/store/meta")],
            },
        )
        self.assertEqual(syscall["pwritev"], 1)
        self.assertEqual(syscall["file_unlink"], 1)
        bad_markers = {
            "trace_raw_artifact": self.raw_artifact(
                "syscall-copy.strace", syscall_payload
            ),
            "log_path_markers": [
                trace_marker("directory_prefix", "/tmp/evil/store/log/")
            ],
            "metadata_path_markers": [trace_marker("exact", "/store/meta")],
        }
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.profile_fields(
                "syscall_profiles",
                syscall_rich,
                raw_point={"track": "syscall_profiles", "variant": "A"},
                control_events=syscall_events,
                authority=syscall_authority,
                profile_inputs=bad_markers,
            )
        bad_mode = {
            **bad_markers,
            "log_path_markers": [trace_marker("directory_prefix", "/store/log/")],
            "trace_raw_artifact": {
                **bad_markers["trace_raw_artifact"],
                "mode": 0o644,
            },
        }
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.profile_fields(
                "syscall_profiles",
                syscall_rich,
                raw_point={"track": "syscall_profiles", "variant": "A"},
                control_events=syscall_events,
                authority=syscall_authority,
                profile_inputs=bad_mode,
            )
        aliased_path = {
            **bad_markers,
            "log_path_markers": [trace_marker("directory_prefix", "/store/log/")],
            "trace_raw_artifact": {
                **bad_markers["trace_raw_artifact"],
                "path": str(bad_markers["trace_raw_artifact"]["path"]).replace(
                    "/", "//", 1
                ),
            },
        }
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.profile_fields(
                "syscall_profiles",
                syscall_rich,
                raw_point={"track": "syscall_profiles", "variant": "A"},
                control_events=syscall_events,
                authority=syscall_authority,
                profile_inputs=aliased_path,
            )

        structural_rich = self.rich(
            "structural_traces",
            context={"trace_kind": "reopen"},
        )
        structural_authority = structural_rich["authority"]
        structural_events = control_events(structural_authority, reopen=True)
        structural_boundary = trace_boundary(structural_authority, structural_events)
        structural_payload = f"""{marker_line(structural_authority, structural_boundary["begin_event"])}
10 openat(AT_FDCWD, "/store/log/active", O_RDONLY) = 4</store/log/active>
10 read(4</store/log/active>, "x", 1) = 1
10 fdatasync(4</store/log/active>) = 0
{marker_line(structural_authority, structural_boundary["end_event"])}
""".encode()
        structural = adapters.profile_fields(
            "structural_traces",
            structural_rich,
            raw_point={
                "track": "structural_traces",
                "variant": "A",
                "trace_kind": "reopen",
            },
            control_events=structural_events,
            authority=structural_authority,
            profile_inputs={
                "trace_raw_artifact": self.raw_artifact(
                    "structural.strace", structural_payload
                ),
                "log_path_markers": [
                    trace_marker("directory_prefix", "/store/log/")
                ],
                "metadata_path_markers": [trace_marker("exact", "/store/meta")],
            },
        )
        self.assertEqual(structural["sync_family_calls"], 1)
        self.assertEqual(structural["files_opened"], 1)

    def test_profile_fields_reject_nonfinite_and_partial_inputs(self) -> None:
        rich = self.rich("primary", roles=[self.owner_role()])
        events = control_events(rich["authority"])
        events[8]["process_user_cpu_end_ns"] = math.inf
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.profile_fields(
                "primary",
                rich,
                raw_point={"track": "primary", "variant": "A"},
                control_events=events,
                authority=rich["authority"],
                profile_inputs={"schedstat_resolution_ns": 1},
            )

    def test_profile_fields_reject_control_role_and_authority_mutations(self) -> None:
        rich = self.rich("primary", roles=[self.owner_role()])
        authority = rich["authority"]
        inputs = {"schedstat_resolution_ns": 10}
        mutations: list[tuple[dict[str, object], list[dict[str, object]], dict[str, object]]] = []
        bad_nonce = control_events(authority)
        bad_nonce[2]["nonce"] = "0" * 64
        mutations.append((rich, bad_nonce, authority))
        extra_event = control_events(authority)
        extra_event.append({"command": "release", "nonce": "4" * 64})
        mutations.append((rich, extra_event, authority))
        bad_role = json.loads(adapters.canonical_json(rich))
        bad_role["roles"][0]["label"] = "committer"
        mutations.append((bad_role, control_events(authority), authority))
        bad_phase = json.loads(adapters.canonical_json(rich))
        bad_phase["phase_snapshots"][0]["pid"] = 999
        mutations.append((bad_phase, control_events(authority), authority))
        bad_authority = dict(authority)
        bad_authority["source_tree"] = "0" * 40
        bad_rich_authority = json.loads(adapters.canonical_json(rich))
        bad_rich_authority["authority"] = bad_authority
        mutations.append((bad_rich_authority, control_events(bad_authority), bad_authority))
        for mutated_rich, events, mutated_authority in mutations:
            with self.assertRaises(adapters.ProfileEvidenceError):
                adapters.profile_fields(
                    "primary",
                    mutated_rich,
                    raw_point={"track": "primary", "variant": "A"},
                    control_events=events,
                    authority=mutated_authority,
                    profile_inputs=inputs,
                )

    def test_profile_fields_reject_lifecycle_counter_and_reopen_mutations(self) -> None:
        rich = self.rich("primary", roles=[self.owner_role()])
        authority = rich["authority"]
        inputs = {"schedstat_resolution_ns": 10}
        lifecycle_mutations: list[list[dict[str, object]]] = []
        bad_t1 = control_events(authority)
        bad_t1[8]["t0_monotonic_ns"] = 86
        lifecycle_mutations.append(bad_t1)
        cpu_rollback = control_events(authority)
        cpu_rollback[8]["process_user_cpu_end_ns"] = 9
        lifecycle_mutations.append(cpu_rollback)
        future_ready = control_events(authority)
        future_ready[6]["counter_start_monotonic_ns"] = 71
        lifecycle_mutations.append(future_ready)
        for index, events in enumerate(lifecycle_mutations):
            with self.subTest(index=index), self.assertRaises(
                adapters.ProfileEvidenceError
            ):
                adapters.profile_fields(
                    "primary",
                    rich,
                    raw_point={"track": "primary", "variant": "A"},
                    control_events=events,
                    authority=authority,
                    profile_inputs=inputs,
                )

        reopen = self.rich("reopen")
        reopen_events = control_events(reopen["authority"], reopen=True)
        reopen_events[6]["open_start_monotonic_ns"] = 63
        with self.assertRaises(adapters.ProfileEvidenceError):
            adapters.profile_fields(
                "reopen",
                reopen,
                raw_point={"track": "reopen", "variant": "A"},
                control_events=reopen_events,
                authority=reopen["authority"],
            )

    def test_live_schedstat_preflight_is_canonical(self) -> None:
        artifact = adapters.preflight_profile_contract(sample_count=8)
        self.assertEqual(artifact["schema"], adapters.PREFLIGHT_SCHEMA)
        self.assertGreater(artifact["minimum_nonzero_increment_ns"], 0)
        self.assertEqual(
            artifact["decision_floor_ns"],
            artifact["minimum_nonzero_increment_ns"]
            * adapters.SCHEDSTAT_DECISION_MULTIPLIER,
        )
        adapters.canonical_json(artifact)


if __name__ == "__main__":
    unittest.main()
