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
import re
import secrets
import selectors
import signal
import stat
import struct
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from contextlib import ExitStack
from pathlib import Path, PurePosixPath
from typing import Any


PROTOCOL = "bn-2l3n-asterism-rebaseline-v3"
PLAN_SCHEMA = "asterism-rebaseline-source-plan-v3"
LOCK_SCHEMA = "asterism-rebaseline-lock-candidates-v3"
APPROVAL_SCHEMA = "bn-2l3n-source-approval-v3"
PREPARED_SCHEMA = "bn-2l3n-prepared-artifacts-v3"
CONTRACT_SCHEMA = "bn-2l3n-binary-contract-v3"
SOURCE_REVIEW_ASSERTION_SCHEMA = "bn-3hch-source-review-assertion-v1"
SOURCE_REVIEW_BUNDLE_SCHEMA = "bn-3hch-source-review-bundle-v1"
SOURCE_REVIEW_INPUT_SCHEMA = "bn-3hch-source-review-input-v1"
CURRENT_CHILDREN_ATTESTATION_SCHEMA = "bn-ecm1-current-children-build-v2"
CURRENT_LOCK_AUTHORITY_SCHEMA = "bn-31gp-current-lock-authority-v1"
CURRENT_LOCK_REVIEW_BUNDLE_SCHEMA = "bn-31gp-current-lock-review-bundle-v1"
RELEASE_COMPILE_OUT_REQUIREMENT_SCHEMA = (
    "bn-3hch-release-compile-out-requirement-v1"
)
RELEASE_COMPILE_OUT_SCHEMA = "bn-3hch-release-compile-out-v1"
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
    "rust_lld_path",
    "rust_lld_sha256",
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
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
from overlay_pins import (  # noqa: E402
    PINNED_SHARED_OVERLAY_SHA256,
    validate_pinned_shared_overlay_payload as validate_raw_shared_overlay_payload,
    validate_pinned_shared_overlay_set,
)

PLAN_PATH = HERE / "source-plan.json"
SHARED_SOURCE = HERE / "overlay" / "shared"
PUBLIC_SOURCE = HERE / "overlay" / "public"
BARE_SOURCE = HERE / "overlay" / "bare"
CURRENT_SOURCE = HERE / "current"
CURRENT_PREPARE_CHILDREN = CURRENT_SOURCE / "prepare_children.py"
CURRENT_CORRECTNESS = CURRENT_SOURCE / "correctness.rs"
CURRENT_PRODUCT_OVERLAY = CURRENT_SOURCE / (
    "product" + "-test-overlay.patch"
)
CURRENT_PRODUCT_OVERLAY_VALIDATOR = (
    CURRENT_SOURCE / "validate_product_test_overlay.py"
)
CURRENT_PRODUCT_COMMIT = "d644dc583dfe6a3d2cd07e71ce0212a323875ab4"
CURRENT_PRODUCT_TREE = "205d853905bdb648ee997900c6aef24a323aa380"
CURRENT_PRODUCT_OVERLAY_REVIEWED_COMMIT = (
    "86027c98605d9ea01c3e385b0702741723d5a538"
)
CURRENT_PRODUCT_OVERLAY_SHA256 = (
    "dd36dee2b53831eb0274b4dac0252d154caae1a24991e167faeb2bf3682b26cd"
)
PREAPPROVAL_SOURCE_SENTINEL = (
    "fa2acb626f303f8a65a16a6c8a1fd86b7e80cf48e092ae21a7308984ae790c94"
)
PREAPPROVAL_FINAL_ACTION = (
    "repeat-release-equality-proof-under-real-source-approval"
)
FORBIDDEN_RELEASE_HOOK_STRINGS = (
    "TestEngineHook",
    "TestEngineHooks",
    "TestEngineFs",
    "arm_test_hook",
    "arm_test_owner_cohort",
    "asterism_rebaseline_correctness",
)
SOURCE_REVIEW_INPUT_NAMES = {
    "current_children_attestation",
    "lock_authority",
    "lock_manifest",
    "lock_review_bundle",
    "tools_manifest",
}
GUEST_ROOT = "/asterism"
GUEST_SOURCE = f"{GUEST_ROOT}/source"
GUEST_TARGET = f"{GUEST_ROOT}/target"
GUEST_TOOLCHAIN_ROOT = f"{GUEST_ROOT}/toolchain"
GUEST_TOOLCHAIN_BIN = f"{GUEST_TOOLCHAIN_ROOT}/bin"
GUEST_CARGO = f"{GUEST_TOOLCHAIN_ROOT}/bin/cargo"
GUEST_RUSTC = f"{GUEST_TOOLCHAIN_ROOT}/bin/rustc"
GUEST_CARGO_HOME = f"{GUEST_ROOT}/cargo-home"
GUEST_RUSTUP_HOME = f"{GUEST_ROOT}/rustup-home"
HOST_DEV_NULL = Path("/dev/null")
GUEST_BOUND_CONFIG_PATHS = (
    f"{GUEST_SOURCE}/.cargo/config.toml",
    f"{GUEST_SOURCE}/.cargo/config",
    f"{GUEST_CARGO_HOME}/config.toml",
    f"{GUEST_CARGO_HOME}/config",
)
SEMANTIC_INPUT_AUTHORITY_SCHEMA = "bn-ecm1-semantic-input-authority-v1"
RECURSIVE_TREE_AUTHORITY_SCHEMA = "bn-ecm1-recursive-tree-authority-v1"
TRUSTED_SYSTEM_CLOSURE_SCHEMA = "bn-ecm1-trusted-system-closure-v1"
TRUSTED_SYSTEM_MOUNTS = (
    (Path("/usr/bin"), "/usr/bin"),
    (Path("/usr/lib"), "/usr/lib"),
    (Path("/usr/include"), "/usr/include"),
)
INOTIFY_EVENT_HEADER = struct.Struct("iIII")
INOTIFY_MUTATION_MASK = (
    0x00000002
    | 0x00000004
    | 0x00000008
    | 0x00000040
    | 0x00000080
    | 0x00000100
    | 0x00000200
    | 0x00000400
    | 0x00000800
    | 0x00002000
    | 0x00004000
    | 0x00008000
)
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
REVIEW_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@+\-]{0,255}\Z")
ZONED_TIME = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    r"(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})\Z"
)
PROTOCOL_DOCUMENT = HERE.parent / "BN-2L3N-PROTOCOL.md"
HISTORICAL_BASELINE = HERE.parents[1] / "baseline_matrix" / "BN-2SU-FINAL.csv"


class PreparationError(RuntimeError):
    pass


def load_evidence_schema_self_test_module() -> Any:
    """Load the real shared semantic-manifest validator for producer tests."""

    import importlib.util

    module_name = "_asterism_prepare_overlays_evidence_schema_self_test"
    spec = importlib.util.spec_from_file_location(
        module_name, HERE.parent / "evidence_schema.py"
    )
    if spec is None or spec.loader is None:
        raise PreparationError("evidence-schema self-test module is unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


@dataclass(frozen=True)
class CanonicalSnapshot:
    """One immutable regular-file observation used for every later binding."""

    path: Path
    payload: bytes
    sha256: str
    value: dict[str, Any]
    mode: int
    size: int
    identity: dict[str, int]


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode() + b"\n"


def builder_local_canonical_json(value: Any) -> bytes:
    """Exact JSON+LF form used for current-child builder-local evidence."""

    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        .encode("ascii")
        + b"\n"
    )


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


def semantic_runtime_sha256(components: dict[str, dict[str, Any]]) -> str:
    """Hash runtime semantics while excluding relocatable evidence pathnames."""

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
    if set(components) != {
        "cargo_home",
        "toolchain",
        "trusted_system_closure",
    }:
        raise PreparationError("semantic runtime authority components differ")
    closure = components["trusted_system_closure"]
    mounts = closure.get("mounts")
    if (
        not isinstance(mounts, list)
        or len(mounts) != len(TRUSTED_SYSTEM_MOUNTS)
        or any(
            not isinstance(mount, dict)
            or mount.get("guest_path") != guest_path
            or mount.get("host_path") != str(host_path)
            or mount.get("resolved_path") != str(host_path)
            for mount, (host_path, guest_path) in zip(
                mounts, TRUSTED_SYSTEM_MOUNTS, strict=True
            )
        )
    ):
        raise PreparationError("semantic runtime trusted-system mounts differ")
    normalized = {
        "cargo_home": {
            field: components["cargo_home"].get(field) for field in tree_fields
        },
        "schema": SEMANTIC_INPUT_AUTHORITY_SCHEMA,
        "toolchain": {
            field: components["toolchain"].get(field) for field in tree_fields
        },
        "trusted_system_closure": {
            field: closure.get(field)
            for field in closure_fields
        },
    }
    return hash_bytes(canonical_json(normalized))


def system_symlink_scope(root: Path, relative: str, target: str) -> str:
    raw_guest = (
        PurePosixPath(target)
        if PurePosixPath(target).is_absolute()
        else PurePosixPath(str(root)) / PurePosixPath(relative).parent / target
    )
    rendered = os.path.normpath(str(raw_guest))
    if rendered.startswith("/"):
        rendered = "/" + rendered.lstrip("/")
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
    if rendered == "/" or any(
        rendered == authority or rendered.startswith(authority + "/")
        for authority in ("/asterism", "/dev", "/proc", "/run", "/sys", "/tmp")
    ):
        raise PreparationError(
            "trusted system symlink reaches mutable guest authority"
        )
    return "guest_inaccessible_external"


def recursive_symlink_scope(
    root: Path,
    relative: str,
    target: str,
    *,
    trusted_system: bool,
) -> str:
    if trusted_system:
        return system_symlink_scope(root, relative, target)
    candidate = root / Path(relative).parent / target
    try:
        resolved = candidate.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise PreparationError("symlink target is unresolved") from error
    if resolved != root and root not in resolved.parents:
        raise PreparationError("symlink escapes the retained root")
    return "within_root"


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


def validate_recursive_manifest_value(
    value: Any, role: str, context: str, *, trusted_system: bool
) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or set(value) != {"entries", "role", "schema"}
        or value.get("schema") != RECURSIVE_TREE_AUTHORITY_SCHEMA
        or value.get("role") != role
        or not isinstance(value.get("entries"), list)
        or not value["entries"]
    ):
        raise PreparationError(f"{context} recursive manifest fields differ")
    paths: set[str] = set()
    directory_count = 0
    for index, entry in enumerate(value["entries"]):
        if not isinstance(entry, dict) or set(entry) != SEMANTIC_MANIFEST_ENTRY_FIELDS:
            raise PreparationError(f"{context} recursive manifest entry differs")
        relative = entry.get("path")
        if not isinstance(relative, str) or (
            relative != "."
            and (
                PurePosixPath(relative).is_absolute()
                or str(PurePosixPath(relative)) != relative
                or ".." in PurePosixPath(relative).parts
                or "." in PurePosixPath(relative).parts
            )
        ):
            raise PreparationError(f"{context} recursive manifest path differs")
        if relative in paths or (index == 0) != (relative == "."):
            raise PreparationError(f"{context} recursive manifest paths alias")
        paths.add(relative)
        kind = entry.get("file_type")
        if kind not in {"directory", "regular", "symlink"}:
            raise PreparationError(f"{context} recursive manifest type differs")
        if any(
            not isinstance(entry.get(field), int)
            or isinstance(entry.get(field), bool)
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
            raise PreparationError(f"{context} recursive manifest metadata differs")
        if kind == "directory":
            directory_count += 1
            if (
                entry.get("sha256") is not None
                or entry.get("symlink_target") is not None
                or entry.get("symlink_scope") is not None
            ):
                raise PreparationError(f"{context} directory digest differs")
        elif kind == "regular":
            if (
                entry.get("symlink_target") is not None
                or entry.get("symlink_scope") is not None
                or (
                entry.get("sha256") is not None
                if trusted_system
                else not is_lower_hex(entry.get("sha256"), SHA256)
                )
            ):
                raise PreparationError(f"{context} regular-file digest differs")
        elif (
            not isinstance(entry.get("symlink_target"), str)
            or not is_lower_hex(entry.get("sha256"), SHA256)
            or entry.get("symlink_scope")
            not in (
                {"within_closure", "guest_inaccessible_external"}
                if trusted_system
                else {"within_root"}
            )
        ):
            raise PreparationError(f"{context} symlink target differs")
        if trusted_system and (
            entry["uid"] != 0
            or (kind != "symlink" and entry["permissions"] & 0o022)
        ):
            raise PreparationError(f"{context} trusted system policy differs")
    if directory_count < 1:
        raise PreparationError(f"{context} recursive directory set is empty")
    return value


def resample_recursive_manifest(
    root: Path,
    role: str,
    context: str,
    *,
    allow_internal_symlinks: bool,
    hash_regular_contents: bool,
    trusted_system_roots: tuple[Path, ...] = (),
    excluded_relative_paths: tuple[str, ...] = (),
    volatile_directory_metadata_paths: tuple[str, ...] = (),
) -> dict[str, Any]:
    guard = RecursiveTreeAuthorityGuard(
        root,
        root / ".unused-semantic-evidence",
        role,
        context,
        allow_internal_symlinks=allow_internal_symlinks,
        hash_regular_contents=hash_regular_contents,
        trusted_system_roots=trusted_system_roots,
        excluded_relative_paths=excluded_relative_paths,
        volatile_directory_metadata_paths=volatile_directory_metadata_paths,
    )
    try:
        guard.descriptor, guard.identity = open_retained_directory(root, context)
        first = guard._manifest(install_watches=False)
        second = guard._manifest(install_watches=False)
        if first != second:
            raise PreparationError(f"{context} changed while resampling")
        return second
    finally:
        guard.close()


def validate_semantic_input_authority(
    value: Any,
    context: str,
    *,
    replay_evidence: bool = False,
    live_roots: dict[str, Path] | None = None,
    source_role: str = "source",
    builder_local_evidence: bool = False,
) -> dict[str, Any]:
    """Validate the exact producer contract consumed by bn-17nh integrations."""

    evidence_snapshot = (
        immutable_builder_local_canonical_snapshot
        if builder_local_evidence
        else immutable_canonical_snapshot
    )
    evidence_encoder = (
        builder_local_canonical_json if builder_local_evidence else canonical_json
    )
    if not isinstance(value, dict) or set(value) != {
        "cargo_home",
        "runtime_sha256",
        "schema",
        "source",
        "toolchain",
        "trusted_system_closure",
    } or value.get("schema") != SEMANTIC_INPUT_AUTHORITY_SCHEMA:
        raise PreparationError(f"{context} semantic input authority fields differ")
    tree_fields = {
        "entry_count",
        "equal_pre_post",
        "manifest_path",
        "manifest_sha256",
        "mutation_events_absent",
        "role",
        "schema",
        "watch_count",
    }
    for name, role in (
        ("source", source_role),
        ("toolchain", "toolchain"),
        ("cargo_home", "cargo_home"),
    ):
        tree = value.get(name)
        if (
            not isinstance(tree, dict)
            or set(tree) != tree_fields
            or tree.get("schema") != RECURSIVE_TREE_AUTHORITY_SCHEMA
            or tree.get("role") != role
            or not is_lower_hex(tree.get("manifest_sha256"), SHA256)
            or not isinstance(tree.get("manifest_path"), str)
            or not Path(tree["manifest_path"]).is_absolute()
            or not isinstance(tree.get("entry_count"), int)
            or isinstance(tree.get("entry_count"), bool)
            or tree["entry_count"] < 1
            or not isinstance(tree.get("watch_count"), int)
            or isinstance(tree.get("watch_count"), bool)
            or tree["watch_count"] < 1
            or tree.get("equal_pre_post") is not True
            or tree.get("mutation_events_absent") is not True
        ):
            raise PreparationError(f"{context} semantic {name} binding differs")
        if replay_evidence:
            evidence = evidence_snapshot(
                Path(tree["manifest_path"]),
                RECURSIVE_TREE_AUTHORITY_SCHEMA,
                f"{context} semantic {name} evidence",
            )
            validate_recursive_manifest_value(
                evidence.value, role, f"{context} semantic {name} evidence",
                trusted_system=False,
            )
            if (
                evidence.sha256 != tree["manifest_sha256"]
                or evidence.value.get("role") != role
                or len(evidence.value.get("entries", [])) != tree["entry_count"]
                or sum(
                    entry.get("file_type") == "directory"
                    for entry in evidence.value.get("entries", [])
                )
                != tree["watch_count"]
            ):
                raise PreparationError(
                    f"{context} semantic {name} evidence differs"
                )
            if live_roots is not None:
                root = live_roots[name].resolve(strict=True)
                observed = resample_recursive_manifest(
                    root,
                    role,
                    f"{context} live semantic {name}",
                    allow_internal_symlinks=name != "source",
                    hash_regular_contents=True,
                    excluded_relative_paths=("Cargo.lock",)
                    if role == "resolution_source_without_cargo_lock"
                    else (),
                    volatile_directory_metadata_paths=(".",)
                    if role == "resolution_source_without_cargo_lock"
                    else (),
                )
                if evidence_encoder(observed) != evidence.payload:
                    raise PreparationError(
                        f"{context} live semantic {name} differs"
                    )
    closure = value.get("trusted_system_closure")
    closure_fields = {
        "entry_count",
        "manifest_path",
        "mounts",
        "mutation_events_absent",
        "schema",
        "sha256",
        "watch_count",
    }
    mount_fields = {
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
    closure_mounts = closure.get("mounts") if isinstance(closure, dict) else None
    if (
        not isinstance(closure_mounts, list)
        or len(closure_mounts) != len(TRUSTED_SYSTEM_MOUNTS)
        or any(not isinstance(mount, dict) for mount in closure_mounts)
    ):
        raise PreparationError(f"{context} trusted system closure mounts differ")
    if (
        not isinstance(closure, dict)
        or set(closure) != closure_fields
        or closure.get("schema") != TRUSTED_SYSTEM_CLOSURE_SCHEMA
        or not is_lower_hex(closure.get("sha256"), SHA256)
        or not isinstance(closure.get("manifest_path"), str)
        or not Path(closure["manifest_path"]).is_absolute()
        or not isinstance(closure.get("entry_count"), int)
        or isinstance(closure.get("entry_count"), bool)
        or closure["entry_count"] < 3
        or not isinstance(closure.get("watch_count"), int)
        or isinstance(closure.get("watch_count"), bool)
        or closure["watch_count"] < 3
        or closure.get("mutation_events_absent") is not True
        or any(
            set(mount) != mount_fields
            or mount.get("guest_path") != guest_path
            or mount.get("host_path") != str(host_path)
            or mount.get("resolved_path") != str(host_path)
            or mount.get("trusted_root_owned_non_writable") is not True
            or mount.get("uid") != 0
            or not all(
                isinstance(mount.get(field), int)
                and not isinstance(mount.get(field), bool)
                for field in ("device", "gid", "inode", "permissions", "uid")
            )
            or mount.get("permissions", 0) & 0o022
            for mount, (host_path, guest_path) in zip(
                closure_mounts, TRUSTED_SYSTEM_MOUNTS, strict=True
            )
        )
    ):
        raise PreparationError(f"{context} trusted system closure differs")
    if replay_evidence:
        closure_evidence = evidence_snapshot(
            Path(closure["manifest_path"]),
            TRUSTED_SYSTEM_CLOSURE_SCHEMA,
            f"{context} trusted system closure evidence",
        )
        evidence_mounts = closure_evidence.value.get("mounts")
        if (
            not isinstance(evidence_mounts, list)
            or len(evidence_mounts) != len(TRUSTED_SYSTEM_MOUNTS)
            or any(not isinstance(mount, dict) for mount in evidence_mounts)
        ):
            raise PreparationError(
                f"{context} trusted system evidence mounts differ"
            )
        roots = tuple(
            path.resolve(strict=True) for path, _guest in TRUSTED_SYSTEM_MOUNTS
        )
        for expected, evidence_mount, binding_mount in zip(
            TRUSTED_SYSTEM_MOUNTS,
            evidence_mounts,
            closure_mounts,
            strict=True,
        ):
            host_path, guest_path = expected
            if (
                set(evidence_mount)
                != {"guest_path", "host_path", "resolved_path", "tree"}
                or evidence_mount.get("guest_path") != guest_path
                or evidence_mount.get("host_path") != str(host_path)
                or evidence_mount.get("resolved_path") != str(host_path)
            ):
                raise PreparationError(
                    f"{context} trusted system evidence mount differs"
                )
            tree = validate_recursive_manifest_value(
                evidence_mount.get("tree"),
                "system-" + guest_path.removeprefix("/").replace("/", "-"),
                f"{context} trusted system {guest_path}",
                trusted_system=True,
            )
            root_entry = tree["entries"][0]
            for field, entry_field in (
                ("device", "device"),
                ("gid", "gid"),
                ("inode", "inode"),
                ("permissions", "permissions"),
                ("uid", "uid"),
            ):
                if binding_mount[field] != root_entry[entry_field]:
                    raise PreparationError(
                        f"{context} trusted system root binding differs"
                    )
            if live_roots is not None:
                observed = resample_recursive_manifest(
                    host_path,
                    tree["role"],
                    f"{context} live trusted system {guest_path}",
                    allow_internal_symlinks=True,
                    hash_regular_contents=False,
                    trusted_system_roots=roots,
                )
                if observed != tree:
                    raise PreparationError(
                        f"{context} live trusted system {guest_path} differs"
                    )
        if (
            closure_evidence.sha256 != closure["sha256"]
            or sum(
                len(mount.get("tree", {}).get("entries", []))
                for mount in evidence_mounts
                if isinstance(mount, dict)
            )
            != closure["entry_count"]
            or sum(
                sum(
                    entry.get("file_type") == "directory"
                    for entry in mount.get("tree", {}).get("entries", [])
                )
                for mount in evidence_mounts or []
                if isinstance(mount, dict)
            )
            != closure["watch_count"]
        ):
            raise PreparationError(
                f"{context} trusted system closure evidence differs"
            )
    components = {
        name: value[name]
        for name in ("cargo_home", "toolchain", "trusted_system_closure")
    }
    if (
        not is_lower_hex(value.get("runtime_sha256"), SHA256)
        or value["runtime_sha256"] != semantic_runtime_sha256(components)
    ):
        raise PreparationError(f"{context} semantic runtime digest differs")
    return value


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


def parse_builder_local_canonical_object(
    payload: bytes, schema: str, context: str
) -> dict[str, Any]:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreparationError(f"{context} is not valid JSON") from error
    if not isinstance(value, dict) or payload != builder_local_canonical_json(value):
        raise PreparationError(
            f"{context} is not a builder-local ASCII-canonical JSON object"
        )
    if value.get("schema") != schema:
        raise PreparationError(f"{context} schema mismatch")
    return value


def _immutable_canonical_snapshot(
    path: Path,
    schema: str,
    context: str,
    *,
    builder_local: bool,
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
            or before.st_nlink != 1
        ):
            raise PreparationError(
                f"{context} is not an exact single-link 0444 regular file"
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
        "st_nlink",
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
        or any(
            getattr(current, field) != getattr(after, field)
            for field in stable_fields
        )
    ):
        raise PreparationError(
            f"{context} path is aliased or changed after snapshot"
        )
    value = (
        parse_builder_local_canonical_object(payload, schema, context)
        if builder_local
        else parse_canonical_object(payload, schema, context)
    )
    return CanonicalSnapshot(
        path=lexical,
        payload=payload,
        sha256=hash_bytes(payload),
        value=value,
        mode=stat.S_IMODE(after.st_mode),
        size=after.st_size,
        identity={
            "changed_ns": after.st_ctime_ns,
            "device": after.st_dev,
            "inode": after.st_ino,
            "link_count": after.st_nlink,
            "modified_ns": after.st_mtime_ns,
        },
    )


def immutable_canonical_snapshot(
    path: Path, schema: str, context: str
) -> CanonicalSnapshot:
    """Snapshot one reviewed/prepared UTF-8-canonical JSON authority."""

    return _immutable_canonical_snapshot(
        path, schema, context, builder_local=False
    )


def immutable_builder_local_canonical_snapshot(
    path: Path, schema: str, context: str
) -> CanonicalSnapshot:
    """Snapshot one current-child builder-local ASCII-canonical evidence file."""

    return _immutable_canonical_snapshot(
        path, schema, context, builder_local=True
    )


def source_review_input(snapshot: CanonicalSnapshot) -> dict[str, Any]:
    """Return the exact reviewer-facing immutable input record."""

    return {
        "identity": snapshot.identity,
        "mode": snapshot.mode,
        "path": str(snapshot.path),
        "schema": SOURCE_REVIEW_INPUT_SCHEMA,
        "sha256": snapshot.sha256,
        "size": snapshot.size,
    }


def require_same_snapshot(
    before: CanonicalSnapshot, after: CanonicalSnapshot, context: str
) -> None:
    if before != after:
        raise PreparationError(f"{context} changed across semantic validation")


def copy_snapshot(
    snapshot: CanonicalSnapshot, destination: Path, schema: str, context: str
) -> CanonicalSnapshot:
    atomic_write(destination, snapshot.payload, mode=0o444)
    copied = immutable_canonical_snapshot(destination, schema, context)
    if copied.payload != snapshot.payload or copied.sha256 != snapshot.sha256:
        raise PreparationError(f"{context} differs from reviewed input")
    return copied


def validate_identifier(value: Any, context: str) -> str:
    if not isinstance(value, str) or REVIEW_IDENTIFIER.fullmatch(value) is None:
        raise PreparationError(f"{context} is absent or noncanonical")
    return value


def parse_zoned_time(value: Any, context: str) -> datetime:
    if not isinstance(value, str) or ZONED_TIME.fullmatch(value) is None:
        raise PreparationError(f"{context} is not a canonical zoned time")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        timestamp = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise PreparationError(f"{context} is not ISO-8601") from error
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise PreparationError(f"{context} has no UTC offset")
    return timestamp


def exact_seal_event(value: Any, event_name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "author",
        "data",
        "event",
        "ts",
    }:
        raise PreparationError(f"Seal {event_name} event fields differ")
    validate_identifier(value.get("author"), f"Seal {event_name} author")
    if value.get("event") != event_name or not isinstance(value.get("data"), dict):
        raise PreparationError(f"Seal {event_name} event content differs")
    parse_zoned_time(value.get("ts"), f"Seal {event_name} timestamp")
    return value


def content_binding(snapshot: CanonicalSnapshot, schema: str) -> dict[str, Any]:
    if snapshot.value.get("schema") != schema:
        raise PreparationError("source-review content binding schema differs")
    return {"mode": 0o444, "schema": schema, "sha256": snapshot.sha256}


def local_binding(snapshot: CanonicalSnapshot) -> dict[str, Any]:
    return {
        "mode": 0o444,
        "path": str(snapshot.path),
        "sha256": snapshot.sha256,
    }


def release_compile_out_requirement(
    current_children: CanonicalSnapshot,
) -> dict[str, Any]:
    preapproval = current_children.value.get("release_compile_out")
    if not isinstance(preapproval, dict):
        raise PreparationError("current-child preapproval compile-out proof is absent")
    return {
        "binary_byte_identical": True,
        "cfg_test": False,
        "forbidden_hook_strings": list(FORBIDDEN_RELEASE_HOOK_STRINGS),
        "forbidden_hook_strings_absent": True,
        "ordinary_a_role": "published",
        "overlay_a_role": "proof_only",
        "preapproval_compile_out_sha256": hash_bytes(canonical_json(preapproval)),
        "product_overlay_sha256": CURRENT_PRODUCT_OVERLAY_SHA256,
        "proof_must_bind_enclosing_approval_sha256": True,
        "repeat_under_real_source_approval": True,
        "rustc_workspace_wrapper": "absent",
        "same_contract_nonce_lock_toolchain_sandbox": True,
        "schema": RELEASE_COMPILE_OUT_REQUIREMENT_SCHEMA,
        "status": "required",
        "symbol_inventory_byte_identical": True,
        "variant": "A",
    }


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
    try:
        metadata = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise PreparationError(f"required tool is unavailable: {path}") from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or resolved != path
    ):
        raise PreparationError(f"required tool is not a regular file: {path}")
    return path


def run_retained_probe(
    path: Path,
    descriptor: int,
    argv: list[str],
    environment: dict[str, str],
    context: str,
) -> bytes:
    """Execute one exact retained tool descriptor and return exact stdout bytes."""

    if not argv or argv[0] != str(path):
        raise PreparationError(f"{context} logical executable differs")
    completed = subprocess.run(
        argv,
        executable=f"/proc/self/fd/{descriptor}",
        pass_fds=(descriptor,),
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
        check=False,
    )
    if completed.returncode != 0 or completed.stderr or not completed.stdout:
        raise PreparationError(f"{context} retained probe failed")
    return completed.stdout


def toolchain_identity() -> dict[str, str]:
    cargo_home_input = Path(
        os.environ.get("CARGO_HOME", str(Path.home() / ".cargo"))
    ).absolute()
    rustup_home_input = Path(
        os.environ.get("RUSTUP_HOME", str(Path.home() / ".rustup"))
    ).absolute()
    cargo_home = cargo_home_input.resolve(strict=True)
    rustup_home = rustup_home_input.resolve(strict=True)
    if (
        cargo_home != cargo_home_input
        or rustup_home != rustup_home_input
        or not cargo_home.is_dir()
        or not rustup_home.is_dir()
    ):
        raise PreparationError("Cargo and rustup homes must be directories")
    rustup = checked_tool("rustup")
    bwrap = checked_tool("bwrap")
    git = checked_tool("git")
    bootstrap = bootstrap_environment(cargo_home, rustup_home)
    leases: dict[str, tuple[Path, int, bytes, dict[str, Any]]] = {}

    def retain(name: str, path: Path) -> None:
        descriptor, payload, binding, _full = open_retained_artifact(
            path, f"toolchain identity {name}"
        )
        key = (binding["identity"]["device"], binding["identity"]["inode"])
        if any(
            key
            == (
                other[3]["identity"]["device"],
                other[3]["identity"]["inode"],
            )
            for other in leases.values()
        ):
            os.close(descriptor)
            raise PreparationError(f"toolchain executable aliases: {name}")
        leases[name] = (path, descriptor, payload, binding)

    def verify_all(boundary: str) -> None:
        for name, (path, descriptor, payload, binding) in leases.items():
            verify_retained_artifact(
                descriptor,
                path,
                payload,
                binding,
                f"toolchain identity {name} at {boundary}",
            )

    try:
        retain("rustup", rustup)
        retain("bwrap", bwrap)
        retain("git", git)
        active_argv = [str(rustup), "show", "active-toolchain"]
        active_raw = run_retained_probe(
            rustup,
            leases["rustup"][1],
            active_argv,
            bootstrap,
            "rustup active toolchain",
        )
        verify_all("active-toolchain sample")
        try:
            active = active_raw.decode("utf-8").strip()
        except UnicodeDecodeError as error:
            raise PreparationError("rustup active toolchain is not UTF-8") from error
        toolchain = active.split(maxsplit=1)[0]
        if not toolchain or any(character.isspace() for character in toolchain):
            raise PreparationError("rustup active toolchain is invalid")

        which_outputs: dict[str, bytes] = {}
        for component in ("cargo", "rustc"):
            argv = [
                str(rustup),
                "which",
                component,
                "--toolchain",
                toolchain,
            ]
            raw = run_retained_probe(
                rustup,
                leases["rustup"][1],
                argv,
                bootstrap,
                f"rustup which {component}",
            )
            verify_all(f"rustup which {component}")
            which_outputs[component] = raw
            try:
                rendered = raw.decode("utf-8").strip()
            except UnicodeDecodeError as error:
                raise PreparationError(
                    f"rustup {component} target is not UTF-8"
                ) from error
            lexical = Path(rendered).absolute()
            resolved = lexical.resolve(strict=True)
            metadata = lexical.lstat()
            if (
                lexical != resolved
                or stat.S_ISLNK(metadata.st_mode)
                or not stat.S_ISREG(metadata.st_mode)
            ):
                raise PreparationError(
                    f"rustup {component} target is aliased or unsupported"
                )
            retain(component, lexical)

        cargo = leases["cargo"][0]
        rustc = leases["rustc"][0]
        if cargo.parent.parent != rustc.parent.parent:
            raise PreparationError("Cargo and rustc toolchain roots differ")
        version_env = {**bootstrap, "RUSTUP_TOOLCHAIN": toolchain}
        version_outputs: dict[str, bytes] = {}
        for component, path in (("cargo", cargo), ("rustc", rustc)):
            raw = run_retained_probe(
                path,
                leases[component][1],
                [str(path), "--version", "--verbose"],
                version_env,
                f"{component} verbose version",
            )
            verify_all(f"{component} version sample")
            version_outputs[component] = raw

        active_replay = run_retained_probe(
            rustup,
            leases["rustup"][1],
            active_argv,
            bootstrap,
            "rustup active toolchain replay",
        )
        if active_replay != active_raw:
            raise PreparationError("rustup active toolchain changed while sampled")
        for component, path in (("cargo", cargo), ("rustc", rustc)):
            which_replay = run_retained_probe(
                rustup,
                leases["rustup"][1],
                [
                    str(rustup),
                    "which",
                    component,
                    "--toolchain",
                    toolchain,
                ],
                bootstrap,
                f"rustup which {component} replay",
            )
            version_replay = run_retained_probe(
                path,
                leases[component][1],
                [str(path), "--version", "--verbose"],
                version_env,
                f"{component} verbose version replay",
            )
            if (
                which_replay != which_outputs[component]
                or version_replay != version_outputs[component]
            ):
                raise PreparationError(
                    f"{component} toolchain discovery changed while sampled"
                )
            verify_all(f"{component} discovery replay")
        cargo_version = version_outputs["cargo"].decode("utf-8").strip()
        rustc_version = version_outputs["rustc"].decode("utf-8").strip()
        verify_all("final toolchain identity")
        bindings = {name: lease[3] for name, lease in leases.items()}
    finally:
        for _path, descriptor, _payload, _binding in leases.values():
            os.close(descriptor)
    host_lines = [
        line.removeprefix("host: ")
        for line in rustc_version.splitlines()
        if line.startswith("host: ")
    ]
    if (
        len(host_lines) != 1
        or re.fullmatch(r"[A-Za-z0-9_-]+", host_lines[0]) is None
    ):
        raise PreparationError("rustc verbose version has no unique host triple")
    rust_lld = (
        cargo.parent.parent
        / "lib"
        / "rustlib"
        / host_lines[0]
        / "bin"
        / "rust-lld"
    )
    rust_lld_descriptor, rust_lld_payload, rust_lld_binding, _rust_lld_identity = (
        open_retained_artifact(rust_lld, "rust-lld toolchain identity")
    )
    try:
        if stat.S_IMODE(os.fstat(rust_lld_descriptor).st_mode) & 0o111 == 0:
            raise PreparationError("rust-lld toolchain identity is not executable")
        verify_retained_artifact(
            rust_lld_descriptor,
            rust_lld,
            rust_lld_payload,
            rust_lld_binding,
            "rust-lld toolchain identity",
        )
    finally:
        os.close(rust_lld_descriptor)
    return {
        "bwrap_path": str(bwrap),
        "bwrap_sha256": bindings["bwrap"]["sha256"],
        "cargo_home_path": str(cargo_home),
        "cargo_path": str(cargo),
        "cargo_sha256": bindings["cargo"]["sha256"],
        "cargo_version_verbose": cargo_version,
        "git_path": str(git),
        "git_sha256": bindings["git"]["sha256"],
        "rustc_path": str(rustc),
        "rustc_sha256": bindings["rustc"]["sha256"],
        "rustc_version_verbose": rustc_version,
        "rustc_host": host_lines[0],
        "rust_lld_path": str(rust_lld),
        "rust_lld_sha256": rust_lld_binding["sha256"],
        "rustup_home_path": str(rustup_home),
        "rustup_path": str(rustup),
        "rustup_sha256": bindings["rustup"]["sha256"],
        "rustup_toolchain": toolchain,
    }


def validate_toolchain(value: Any) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != TOOLCHAIN_FIELDS:
        raise PreparationError("toolchain identity fields differ")
    for field in (
        "bwrap_sha256", "cargo_sha256", "git_sha256", "rustc_sha256",
        "rust_lld_sha256", "rustup_sha256",
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


def sandboxed_cargo_environment(
    toolchain: dict[str, str], extra: dict[str, str]
) -> dict[str, str]:
    """Return the exact environment visible to every measurement build."""

    environment = frozen_cargo_environment(toolchain, extra)
    environment.update(
        {
            "CARGO_HOME": GUEST_CARGO_HOME,
            "GIT_CONFIG_GLOBAL": f"{GUEST_ROOT}/absent-gitconfig",
            "PATH": f"{GUEST_TOOLCHAIN_ROOT}/bin:/usr/bin:/bin",
            "RUSTC": GUEST_RUSTC,
            "RUSTUP_HOME": "/nonexistent",
        }
    )
    if set(environment) != FROZEN_CARGO_ENV_FIELDS | set(extra):
        raise PreparationError("sandboxed Cargo environment fields differ")
    return environment


def sandboxed_build_environment(
    toolchain: dict[str, str], extra: dict[str, str]
) -> dict[str, str]:
    """Add the fixed loader origin required by rustc with an empty guest /proc."""

    if "LD_ORIGIN_PATH" in extra:
        raise PreparationError("sandboxed build loader origin override")
    environment = sandboxed_cargo_environment(toolchain, extra)
    environment["LD_ORIGIN_PATH"] = GUEST_TOOLCHAIN_BIN
    expected = FROZEN_CARGO_ENV_FIELDS | set(extra) | {"LD_ORIGIN_PATH"}
    if set(environment) != expected:
        raise PreparationError("sandboxed build environment fields differ")
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


def sandboxed_cargo_config_search(
    source_root: Path, toolchain: dict[str, str]
) -> dict[str, Any]:
    """Describe exactly the Cargo config paths visible at fixed guest paths."""

    source_root = source_root.resolve(strict=True)
    cargo_home = Path(toolchain["cargo_home_path"]).resolve(strict=True)
    candidates: tuple[tuple[str, Path | None, bool], ...] = (
        (
            f"{GUEST_SOURCE}/.cargo/config.toml",
            source_root / ".cargo/config.toml",
            True,
        ),
        (f"{GUEST_SOURCE}/.cargo/config", source_root / ".cargo/config", True),
        (f"{GUEST_ROOT}/.cargo/config.toml", None, False),
        (f"{GUEST_ROOT}/.cargo/config", None, False),
        ("/.cargo/config.toml", None, False),
        ("/.cargo/config", None, False),
        (
            f"{GUEST_CARGO_HOME}/config.toml",
            cargo_home / "config.toml",
            True,
        ),
        (f"{GUEST_CARGO_HOME}/config", cargo_home / "config", True),
    )
    entries: list[dict[str, Any]] = []
    for guest_path, host_path, bind_empty_when_absent in candidates:
        if host_path is None:
            entries.append(
                {"path": guest_path, "sha256": None, "status": "absent"}
            )
            continue
        host_path = Path(os.path.abspath(host_path))
        if host_path.is_symlink():
            raise PreparationError(f"Cargo config may not be a symlink: {host_path}")
        if host_path.exists():
            if not host_path.is_file():
                raise PreparationError(f"Cargo config is not a file: {host_path}")
            entries.append(
                {
                    "path": guest_path,
                    "sha256": hash_file(host_path),
                    "status": "present",
                }
            )
        else:
            entries.append({
                "path": guest_path,
                "sha256": EMPTY_SHA256 if bind_empty_when_absent else None,
                "status": "present" if bind_empty_when_absent else "absent",
            })
    return {
        "cargo_home_path": GUEST_CARGO_HOME,
        "cwd": GUEST_SOURCE,
        "entries": entries,
        "schema": CARGO_CONFIG_SCHEMA,
    }


def write_sandboxed_cargo_config_search(
    path: Path, source_root: Path, toolchain: dict[str, str]
) -> dict[str, Any]:
    empty_path = sandboxed_empty_cargo_config_path(path)
    atomic_write(empty_path, b"", mode=0o444)
    atomic_write(
        path,
        canonical_json(sandboxed_cargo_config_search(source_root, toolchain)),
        mode=0o444,
    )
    return {"path": str(path.resolve()), "sha256": hash_file(path)}


def replay_sandboxed_cargo_config_search(
    binding: dict[str, str], source_root: Path, toolchain: dict[str, str]
) -> None:
    if not isinstance(binding, dict) or set(binding) != {"path", "sha256"}:
        raise PreparationError("sandboxed Cargo config-search binding fields differ")
    snapshot = immutable_canonical_snapshot(
        Path(binding["path"]),
        CARGO_CONFIG_SCHEMA,
        "sandboxed Cargo config-search manifest",
    )
    path = snapshot.path
    empty_path = sandboxed_empty_cargo_config_path(path)
    if (
        empty_path.is_symlink()
        or not empty_path.is_file()
        or stat.S_IMODE(empty_path.lstat().st_mode) != 0o444
        or hash_file(empty_path) != EMPTY_SHA256
    ):
        raise PreparationError("sandboxed empty Cargo config authority changed")
    if snapshot.sha256 != binding["sha256"]:
        raise PreparationError("sandboxed Cargo config-search manifest hash changed")
    if snapshot.value != sandboxed_cargo_config_search(source_root, toolchain):
        raise PreparationError("sandboxed Cargo config search changed")


def sandboxed_empty_cargo_config_path(manifest_path: Path) -> Path:
    return manifest_path.with_name(f"{manifest_path.name}.empty")


def sandboxed_config_host_paths(
    source_root: Path, toolchain: dict[str, str]
) -> dict[str, Path]:
    cargo_home = Path(toolchain["cargo_home_path"])
    return {
        f"{GUEST_SOURCE}/.cargo/config.toml": source_root / ".cargo/config.toml",
        f"{GUEST_SOURCE}/.cargo/config": source_root / ".cargo/config",
        f"{GUEST_CARGO_HOME}/config.toml": cargo_home / "config.toml",
        f"{GUEST_CARGO_HOME}/config": cargo_home / "config",
    }


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
    inherited_fds: tuple[int, ...] = (),
    executable: str | None = None,
    attest_passed_file_descriptors: bool = False,
) -> tuple[dict[str, Any], bytes, bytes]:
    if max_output_bytes <= 0:
        raise PreparationError("attested output limit must be positive")
    if timeout <= 0:
        raise PreparationError("attested wall timeout must be positive")
    if (
        any(type(descriptor) is not int or descriptor < 3 for descriptor in inherited_fds)
        or len(set(inherited_fds)) != len(inherited_fds)
    ):
        raise PreparationError("attested inherited descriptors are invalid")
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
                for descriptor in inherited_fds:
                    os.set_inheritable(descriptor, True)
                retained_descriptors = set(inherited_fds)
                for raw_descriptor in os.listdir("/proc/self/fd"):
                    if not raw_descriptor.isdecimal():
                        continue
                    descriptor = int(raw_descriptor)
                    if descriptor < 3 or descriptor in retained_descriptors:
                        continue
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
                os.chdir(cwd)
                if executable is None:
                    os.execvpe(argv[0], argv, env)
                else:
                    os.execve(executable, argv, env)
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
        if attest_passed_file_descriptors:
            child["passed_file_descriptors"] = len(inherited_fds)
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
    record, stdout = retained_git_run(repository, arguments, toolchain)
    if record["exit_status"] != 0:
        raise PreparationError(
            f"retained Git invocation failed: {record['stderr']}"
        )
    return stdout


def git_text(
    repository: Path,
    arguments: list[str],
    toolchain: dict[str, str],
) -> str:
    return git_bytes(repository, arguments, toolchain).decode().strip()


def retained_git_run(
    repository: Path,
    arguments: list[str],
    toolchain: dict[str, str],
    *,
    input_payload: bytes | None = None,
) -> tuple[dict[str, Any], bytes]:
    """Run exact reviewed Git bytes through one retained executable lease."""

    git_path = Path(toolchain["git_path"])
    descriptor, executable_payload, binding, _full = open_retained_artifact(
        git_path, "retained generic Git"
    )
    try:
        if binding["sha256"] != toolchain["git_sha256"]:
            raise PreparationError("retained generic Git hash differs")
        argv = [str(git_path), "-C", str(repository), *arguments]
        completed = subprocess.run(
            argv,
            executable=f"/proc/self/fd/{descriptor}",
            pass_fds=(descriptor,),
            cwd=repository,
            env=frozen_cargo_environment(toolchain),
            capture_output=True,
            input=input_payload,
            timeout=120,
        )
        record = {
            "argv": argv,
            "cwd": str(repository.resolve()),
            "exit_status": completed.returncode,
            "stdout_sha256": hash_bytes(completed.stdout),
            "stderr_sha256": hash_bytes(completed.stderr),
            "stdout": completed.stdout.decode(errors="replace"),
            "stderr": completed.stderr.decode(errors="replace"),
        }
        return record, completed.stdout
    finally:
        try:
            verify_retained_artifact(
                descriptor,
                git_path,
                executable_payload,
                binding,
                "retained generic Git",
            )
        finally:
            os.close(descriptor)


def _rust_literal_end(source: str, offset: int, context: str) -> int | None:
    """Return the end of one Rust string/character literal, if present."""

    character = source[offset]
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
            return end + len(closing)
    if character == '"':
        cursor = offset + 1
        while cursor < len(source):
            if source[cursor] == "\\":
                cursor += 2
            elif source[cursor] == '"':
                return cursor + 1
            else:
                cursor += 1
        raise PreparationError(f"{context} has an unterminated string")
    if character == "'":
        lifetime = (
            offset + 1 < len(source)
            and (source[offset + 1].isalnum() or source[offset + 1] == "_")
            and (offset + 2 >= len(source) or source[offset + 2] != "'")
        )
        if lifetime:
            return None
        cursor = offset + 1
        while cursor < len(source):
            if source[cursor] == "\\":
                cursor += 2
            elif source[cursor] == "'":
                return cursor + 1
            else:
                cursor += 1
        raise PreparationError(f"{context} has an unterminated character literal")
    return None


def strip_rust_comments(source: str, context: str) -> str:
    """Blank Rust comments while preserving literals, newlines, and offsets."""

    stripped = list(source)
    offset = 0
    while offset < len(source):
        literal_end = _rust_literal_end(source, offset, context)
        if literal_end is not None:
            offset = literal_end
            continue
        if source.startswith("//", offset):
            end = source.find("\n", offset + 2)
            end = len(source) if end < 0 else end
            for index in range(offset, end):
                stripped[index] = " "
            offset = end
            continue
        if source.startswith("/*", offset):
            comment_depth = 1
            stripped[offset] = stripped[offset + 1] = " "
            cursor = offset + 2
            while cursor < len(source) and comment_depth:
                if source.startswith("/*", cursor):
                    comment_depth += 1
                    stripped[cursor] = stripped[cursor + 1] = " "
                    cursor += 2
                elif source.startswith("*/", cursor):
                    comment_depth -= 1
                    stripped[cursor] = stripped[cursor + 1] = " "
                    cursor += 2
                else:
                    if source[cursor] != "\n":
                        stripped[cursor] = " "
                    cursor += 1
            if comment_depth:
                raise PreparationError(f"{context} has an unterminated comment")
            offset = cursor
            continue
        offset += 1
    return "".join(stripped)


def blank_rust_literals(source: str, context: str) -> str:
    """Blank Rust literals while preserving newlines and source offsets."""

    blanked = list(source)
    offset = 0
    while offset < len(source):
        literal_end = _rust_literal_end(source, offset, context)
        if literal_end is None:
            offset += 1
            continue
        for index in range(offset, literal_end):
            if source[index] != "\n":
                blanked[index] = " "
        offset = literal_end
    return "".join(blanked)


def blank_exact_fragments(
    source: str, fragments: tuple[str, ...], context: str
) -> str:
    """Blank unique required fragments while preserving newlines and offsets."""

    blanked = list(source)
    occupied: set[int] = set()
    for fragment in fragments:
        require_exact_fragment(source, fragment, context)
        start = source.index(fragment)
        for index in range(start, start + len(fragment)):
            if index in occupied:
                raise PreparationError(f"{context} fragments overlap")
            occupied.add(index)
            if source[index] != "\n":
                blanked[index] = " "
    return "".join(blanked)


def reject_rust_unprovable_conditionals(source: str, context: str) -> None:
    """Reject constant or compile-time conditionals in one attested Rust item."""

    for pattern in (
        r"\bif\s*(?:\(\s*)?(?:false|true)\b",
        r"\bif\s+cfg!\s*\(",
        r"#\s*\[\s*cfg(?:_attr)?\s*\(",
    ):
        if re.search(pattern, source):
            raise PreparationError(f"{context} contains an unprovable conditional")


def reject_rust_control_transfers(source: str, context: str) -> None:
    """Reject transfers forbidden by a straight-line attested Rust flow."""

    for pattern in (
        r"\b(?:return|break|continue)\b",
        r"\b(?:panic|unreachable|todo|unimplemented)\s*!",
        r"\bexit\s*\(",
    ):
        if re.search(pattern, source):
            raise PreparationError(f"{context} contains a control-flow transfer")


def reject_rust_shadowing_and_macros(
    source: str,
    context: str,
    *,
    allowed_ack_fragments: tuple[str, ...] = (),
) -> None:
    """Reject silent shadowing and non-whitelisted macros in attested Rust."""

    literal_blanked = blank_rust_literals(source, context)
    if re.search(r"\b(?:const|static)\b", literal_blanked):
        raise PreparationError(f"{context} contains a local const/static item")
    observed_macros = set(
        re.findall(
            r"\b([A-Za-z_][A-Za-z0-9_]*)\s*!\s*[({\[]",
            literal_blanked,
        )
    )
    unexpected_macros = observed_macros - {"assert", "assert_eq", "vec"}
    if unexpected_macros:
        raise PreparationError(
            f"{context} contains unapproved macros: {sorted(unexpected_macros)}"
        )
    ack_blanked = blank_exact_fragments(
        source,
        allowed_ack_fragments,
        f"{context} PERF_ACK_WIRE allowance",
    )
    ack_blanked = blank_rust_literals(ack_blanked, context)
    if re.search(r"\bPERF_ACK_WIRE\b", ack_blanked):
        raise PreparationError(f"{context} shadows or smuggles PERF_ACK_WIRE")


def rust_brace_depth_at(source: str, offset: int, context: str) -> int:
    """Return Rust brace depth at one code offset, ignoring literals."""

    if not 0 <= offset <= len(source):
        raise PreparationError(f"{context} offset is outside the source")
    depth = 0
    cursor = 0
    while cursor < offset:
        literal_end = _rust_literal_end(source, cursor, context)
        if literal_end is not None:
            if literal_end > offset:
                raise PreparationError(f"{context} target is inside a literal")
            cursor = literal_end
            continue
        if source.startswith("//", cursor) or source.startswith("/*", cursor):
            raise PreparationError(f"{context} source retains a comment")
        if source[cursor] == "{":
            depth += 1
        elif source[cursor] == "}":
            depth -= 1
            if depth < 0:
                raise PreparationError(f"{context} braces are unbalanced")
        cursor += 1
    return depth


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


def validate_pinned_shared_overlay_payload(
    name: str, payload: bytes, expected_sha256: str | None = None
) -> None:
    """Re-export exact raw shared-overlay binding with local error semantics."""

    validate_raw_shared_overlay_payload(
        name,
        payload,
        expected_sha256,
        error_type=PreparationError,
    )


def validate_shared_control_child_source(
    control: bytes | str,
    *,
    expected_sha256: str | None = None,
) -> None:
    """Hash-bind shared control bytes, then run diagnostic source checks."""

    payload = control if isinstance(control, bytes) else control.encode("utf-8")
    validate_pinned_shared_overlay_payload(
        "control.rs",
        payload,
        expected_sha256
        if expected_sha256 is not None
        else PINNED_SHARED_OVERLAY_SHA256["control.rs"],
    )
    try:
        control = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise PreparationError("shared control source is not UTF-8") from error
    # Everything below is diagnostic. The exact raw-byte hash above is the
    # binding layer; fragment checks cannot model Rust name resolution. The
    # residual rebinding limits are documented by Seal review cr-8evico.
    control = strip_rust_comments(control, "shared control source")
    control_without_literals = blank_rust_literals(control, "shared control source")
    if re.search(r"\bmacro_rules\s*!", control_without_literals):
        raise PreparationError("shared control source defines a local macro")
    for fragment, context in (
        ('const PERF_ACK_WIRE: &[u8; 5] = b"ack\\n\\0";', "perf ACK wire"),
        (
            'const PERF_ACK_LEDGER_ENTRY: &[u8; 4] = b"ack\\n";',
            "perf ACK ledger entry",
        ),
    ):
        require_exact_fragment(control, fragment, context)
        if rust_brace_depth_at(control, control.index(fragment), context) != 0:
            raise PreparationError(f"{context} is not a top-level binding")

    disable = rust_item(
        control, "fn disable_after_t1(", "shared perf disable control"
    )
    require_exact_signature(
        disable,
        "fn disable_after_t1(&mut self, nonce: &Nonce, t1_monotonic_ns: u64) {",
        "shared perf disable control",
    )
    available_else = """        let Self::Available { pipes, disable, .. } = self else {
            return;
        };"""
    require_exact_fragment(
        disable,
        available_else,
        "shared perf disable availability binding",
    )
    if rust_brace_depth_at(
        disable,
        disable.index(available_else),
        "shared perf disable availability binding",
    ) != 1:
        raise PreparationError("shared perf disable availability binding is nested")
    ack_array_fragment = "let mut ack = [0_u8; PERF_ACK_WIRE.len()];"
    ack_assert_fragment = (
        'assert_eq!(&ack, PERF_ACK_WIRE, "perf disable ACK differs");'
    )
    disable_order = (
        "let sent_monotonic_ns = monotonic_ns();",
        'pipes.command.write_all(b"disable\\n")',
        'pipes.command.flush().expect("flush perf disable")',
        ack_array_fragment,
        "pipes.ack.read_exact(&mut ack)",
        ack_assert_fragment,
        "let ack_received_monotonic_ns = monotonic_ns();",
        "ack_received_monotonic_ns > sent_monotonic_ns",
        ".write_all(PERF_ACK_LEDGER_ENTRY)",
        'pipes.ledger.flush().expect("flush perf disable ACK ledger")',
    )
    for marker in disable_order:
        if disable.count(marker) != 1:
            raise PreparationError(f"shared perf disable control differs: {marker}")
        if rust_brace_depth_at(disable, disable.index(marker), marker) != 1:
            raise PreparationError(
                f"shared perf disable control depth differs: {marker}"
            )
    if [disable.index(marker) for marker in disable_order] != sorted(
        disable.index(marker) for marker in disable_order
    ):
        raise PreparationError("shared perf disable control is reordered")
    disable_flow = list(
        blank_rust_literals(disable, "shared perf disable control")
    )
    allowed_start = disable.index(available_else)
    for index in range(allowed_start, allowed_start + len(available_else)):
        if disable_flow[index] != "\n":
            disable_flow[index] = " "
    disable_flow_text = "".join(disable_flow)
    # This attested function is straight-line except for the exact availability
    # let-else above. Any new transfer requires a deliberate validator update.
    reject_rust_unprovable_conditionals(
        disable_flow_text, "shared perf disable control"
    )
    reject_rust_control_transfers(disable_flow_text, "shared perf disable control")
    # Infinite loops, aborts, and opaque diverging calls are accepted-by-failure:
    # the runner timeout/nonzero-exit/output-cardinality guards expose them.
    # Shadowing and macro smuggling can complete with silently wrong evidence,
    # so they are rejected here after blanking only the two required ACK uses.
    reject_rust_shadowing_and_macros(
        disable,
        "shared perf disable control",
        allowed_ack_fragments=(ack_array_fragment, ack_assert_fragment),
    )

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
    measured_prefix = """        let mut fields = vec![
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
        ];"""
    perf_disable_insert = """        if let Some(perf_disable) = self.perf.measured_value(nonce) {
            fields.push(("perf_disable", perf_disable));
        }"""
    measured_suffix = """        fields.extend([
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
        ]);"""
    measured_order = (
        measured_prefix,
        perf_disable_insert,
        measured_suffix,
        "self.send(&canonical_object(&fields));",
        'command(&self.receive(), "release")',
        'assert_eq!(released, *nonce, "release nonce mismatch")',
    )
    measured_flow = blank_rust_literals(measured, "shared measured control")
    # This attested function has no legitimate transfer tokens. Any future
    # return/break/continue/panic/exit requires a deliberate validator update.
    reject_rust_unprovable_conditionals(measured_flow, "shared measured control")
    reject_rust_control_transfers(measured_flow, "shared measured control")
    # Loop/abort/opaque-call divergence fails loudly at the runner boundary.
    # ACK binding shadowing and macros are rejected because they can silently
    # alter exact bindings or return while preserving an apparently valid exit.
    reject_rust_shadowing_and_macros(measured, "shared measured control")
    for fragment in measured_order:
        require_exact_fragment(measured, fragment, "shared measured lexical flow")
        if rust_brace_depth_at(measured, measured.index(fragment), fragment) != 1:
            raise PreparationError(
                "shared measured lexical flow has nested required code"
            )
    if [measured.index(fragment) for fragment in measured_order] != sorted(
        measured.index(fragment) for fragment in measured_order
    ):
        raise PreparationError("shared measured lexical flow is reordered")


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


def validate_semantic_oracle_integration_sources(
    public: str, semantic: str
) -> None:
    """Keep C/D's frozen oracle exact while A reuses the same semantic core."""

    shared = rust_item(
        semantic,
        "pub async fn run_generation_neutral_semantic_oracle(",
        "shared generation-neutral semantic oracle",
    )
    for marker in (
        'const EVENT_NAME: &str = "asterism.rebaseline.event";',
        'fn name(&self) -> &\'static str { "asterism.rebaseline.rejected" }',
        'b"common-oracle/alpha/0"',
        'b"common-oracle/alpha/1"',
        'b"common-oracle/beta/0"',
        'b"common-oracle/alpha/2"',
        "Err(AppendError::Conflict {",
        "Err(AppendError::Backend(_))",
        "domain_events:  4",
        "fresh_streams:  2",
        "public_appends: 3",
    ):
        if semantic.count(marker) != 1:
            raise PreparationError(
                f"shared semantic oracle marker differs: {marker}"
            )
    if shared.count(".append(") != 5:
        raise PreparationError("shared semantic oracle append calls differ")
    if shared.count(".load::<OracleAggregate>(") != 3:
        raise PreparationError("shared semantic oracle load calls differ")
    if shared.count(".subscribe(None)") != 1:
        raise PreparationError("shared semantic oracle subscription differs")
    for forbidden in (
        ".append_batch(",
        ".append_batch_owned(",
        ".command::<",
        ".command_cached::<",
        ".load_cached::<",
        ".load_hot::<",
        ".with_cache_capacity(",
    ):
        if forbidden in shared:
            raise PreparationError(
                f"shared semantic oracle contains generation-specific path: {forbidden}"
            )

    oracle = rust_item(
        public,
        "fn run_common_public_oracle(",
        "public correctness oracle",
    )
    for marker in (
        '#[path = "asterism_rebaseline_shared/semantic_oracle.rs"]',
        "mod semantic_oracle;",
    ):
        if public.count(marker) != 1:
            raise PreparationError(
                f"public shared-oracle module marker differs: {marker}"
            )
    routing = (
        "let store = EventStore::new(backend).with_page_size(16);",
        "let observations = runtime.block_on(",
        "semantic_oracle::run_generation_neutral_semantic_oracle(&store)",
        """adapter::assert_oracle_accounting(
        &engine,
        observations.domain_events,
        observations.public_appends,
        observations.fresh_streams,
        false,
    );""",
    )
    for marker in routing:
        if oracle.count(marker) != 1:
            raise PreparationError(
                f"public shared-oracle integration marker differs: {marker}"
            )
    if [oracle.index(marker) for marker in routing] != sorted(
        oracle.index(marker) for marker in routing
    ):
        raise PreparationError("public shared-oracle integration is reordered")
    if ".with_cache_capacity(" in oracle:
        raise PreparationError("historical public oracle enabled A-only cache")
    if "common-oracle/alpha/0" in oracle:
        raise PreparationError("public oracle retained an inline semantic copy")


def run_current_static_tool(*arguments: str) -> dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, "-B", *arguments],
        cwd=HERE.parents[2],
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=120,
        check=False,
    )
    if completed.returncode != 0 or completed.stderr:
        raise PreparationError(
            "current correctness static authority failed: "
            f"rc={completed.returncode} stderr={completed.stderr!r}"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise PreparationError(
            "current correctness static authority output is not JSON"
        ) from error
    if not isinstance(value, dict):
        raise PreparationError(
            "current correctness static authority output is not an object"
        )
    return value


def validate_current_correctness_construction() -> None:
    child_self_test = run_current_static_tool(
        str(CURRENT_PREPARE_CHILDREN), "self-test"
    )
    if child_self_test != {
        "hostile_mutations_rejected": 37,
        "schema": "bn-2k0f-prepare-children-self-test-v1",
        "status": "ok",
    }:
        raise PreparationError("current child source self-test differs")

    hook_self_test = run_current_static_tool(
        str(CURRENT_PRODUCT_OVERLAY_VALIDATOR), "--self-test"
    )
    if (
        hook_self_test.get("schema")
        != "bn-xfw3-product-test-overlay-validator-v1"
        or hook_self_test.get("outcome") != "SELF_TEST_PASS"
        or hook_self_test.get("patch_sha256")
        != CURRENT_PRODUCT_OVERLAY_SHA256
        or not isinstance(hook_self_test.get("checks"), list)
        or len(hook_self_test["checks"]) != 17
    ):
        raise PreparationError("current product hook self-test differs")

    hook_validation = run_current_static_tool(
        str(CURRENT_PRODUCT_OVERLAY_VALIDATOR)
    )
    if (
        hook_validation.get("outcome") != "PASS"
        or hook_validation.get("patch_sha256")
        != CURRENT_PRODUCT_OVERLAY_SHA256
        or not isinstance(hook_validation.get("checks"), list)
        or len(hook_validation["checks"]) != 11
    ):
        raise PreparationError("current product hook validation differs")

    with tempfile.TemporaryDirectory(
        prefix="asterism-current-correctness-construction-"
    ) as temporary:
        output = Path(temporary) / "construction"
        manifest = run_current_static_tool(
            str(CURRENT_PREPARE_CHILDREN),
            "prepare",
            "--product-commit",
            CURRENT_PRODUCT_COMMIT,
            "--product-tree",
            CURRENT_PRODUCT_TREE,
            "--product-overlay",
            str(CURRENT_PRODUCT_OVERLAY),
            "--output",
            str(output),
        )
        authority = manifest.get("product_test_overlay_authority")
        if (
            manifest.get("product_commit") != CURRENT_PRODUCT_COMMIT
            or manifest.get("product_tree") != CURRENT_PRODUCT_TREE
            or not isinstance(authority, dict)
            or authority.get("sha256") != CURRENT_PRODUCT_OVERLAY_SHA256
            or authority.get("reviewed_commit")
            != CURRENT_PRODUCT_OVERLAY_REVIEWED_COMMIT
            or authority.get("checks") != hook_validation.get("checks")
            or manifest.get("build_contract")
            != {
                "cargo_locked": True,
                "cargo_offline": True,
                "correctness_only": True,
                "rustc_cfg": ["test"],
                "target_comm": "ast-rb-check",
            }
        ):
            raise PreparationError(
                "current correctness construction manifest differs"
            )
        if (output / "construction.json").read_bytes() != canonical_json(
            manifest
        ):
            raise PreparationError(
                "current correctness construction was not canonical"
            )


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
    sources = sorted(SHARED_SOURCE.glob("*.rs"))
    validate_pinned_shared_overlay_set(
        (source.name for source in sources),
        error_type=PreparationError,
    )
    entries = []
    for source in sources:
        payload = source.read_bytes()
        if source.name == "control.rs":
            validate_shared_control_child_source(payload)
        else:
            validate_pinned_shared_overlay_payload(
                source.name,
                payload,
                PINNED_SHARED_OVERLAY_SHA256[source.name],
            )
        entries.append(
            {
                "name": source.name,
                "sha256": hash_bytes(payload),
                "size": len(payload),
            }
        )
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
    *,
    evidence_label: str | None = None,
) -> dict[str, Any]:
    label = variant if evidence_label is None else evidence_label
    root = output / "materialized" / label
    archive = extract_archive(
        repository,
        claim["product_commit"],
        root,
        output / "archives" / f"source-{label}.tar",
        output / "manifests" / f"archive-{label}.json",
        toolchain,
    )
    overlay = inject_overlay(variant, root)
    overlay_path = output / "manifests" / f"overlay-{label}.json"
    atomic_json(overlay_path, overlay)
    return {
        "root": root,
        "archive": archive,
        "overlay": overlay,
        "overlay_manifest_path": overlay_path,
        "overlay_manifest_sha256": hash_file(overlay_path),
    }


def sandboxed_resolution_argv(
    descriptors: dict[str, int],
    system_descriptors: dict[str, int],
    config_descriptors: dict[str, int],
    cargo_arguments: list[str],
    bwrap_path: str,
) -> list[str]:
    if set(descriptors) != {
        "cargo",
        "cargo_home",
        "rustc",
        "source",
        "toolchain_root",
    }:
        raise PreparationError("resolution sandbox descriptors differ")
    expected_system = tuple(
        guest_path for _host_path, guest_path in TRUSTED_SYSTEM_MOUNTS
    )
    if tuple(system_descriptors) != expected_system:
        raise PreparationError("resolution system descriptors differ")
    if tuple(config_descriptors) != GUEST_BOUND_CONFIG_PATHS:
        raise PreparationError("resolution config descriptors differ")
    all_descriptors = (
        *descriptors.values(),
        *system_descriptors.values(),
        *config_descriptors.values(),
    )
    if (
        any(
            not isinstance(descriptor, int)
            or isinstance(descriptor, bool)
            or descriptor < 3
            for descriptor in all_descriptors
        )
        or len(all_descriptors) != len(set(all_descriptors))
    ):
        raise PreparationError("resolution sandbox descriptors alias")
    argv = [
        bwrap_path,
        "--die-with-parent",
        "--new-session",
        "--unshare-net",
        "--dir",
        "/usr",
    ]
    for guest_path, descriptor in system_descriptors.items():
        argv.extend(["--ro-bind-fd", str(descriptor), guest_path])
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
        GUEST_ROOT,
        "--bind-fd",
        str(descriptors["source"]),
        GUEST_SOURCE,
        "--ro-bind-fd",
        str(descriptors["toolchain_root"]),
        GUEST_TOOLCHAIN_ROOT,
        "--ro-bind-fd",
        str(descriptors["cargo"]),
        GUEST_CARGO,
        "--ro-bind-fd",
        str(descriptors["rustc"]),
        GUEST_RUSTC,
        "--overlay-src",
        f"/proc/self/fd/{descriptors['cargo_home']}",
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
        "--ro-bind-fd",
        str(config_descriptors[f"{GUEST_SOURCE}/.cargo/config.toml"]),
        f"{GUEST_SOURCE}/.cargo/config.toml",
        "--ro-bind-fd",
        str(config_descriptors[f"{GUEST_SOURCE}/.cargo/config"]),
        f"{GUEST_SOURCE}/.cargo/config",
        "--remount-ro",
        f"{GUEST_SOURCE}/.cargo",
        "--ro-bind-fd",
        str(config_descriptors[f"{GUEST_CARGO_HOME}/config.toml"]),
        f"{GUEST_CARGO_HOME}/config.toml",
        "--ro-bind-fd",
        str(config_descriptors[f"{GUEST_CARGO_HOME}/config"]),
        f"{GUEST_CARGO_HOME}/config",
        "--remount-ro",
        GUEST_CARGO_HOME,
        "--chdir",
        GUEST_SOURCE,
        GUEST_CARGO,
        *cargo_arguments,
        ]
    )
    return argv


def freeze_resolution_source(root: Path) -> None:
    """Freeze every source input while leaving only Cargo.lock's parent writable."""

    make_read_only(root)
    if stat.S_IMODE(root.stat().st_mode) != 0o755:
        root.chmod(0o755)


def stage_locks(repository: Path, output: Path) -> None:
    if output.exists() or output.is_symlink():
        raise PreparationError(f"output must be absent: {output}")
    admission = filesystem_admission(output.parent)
    output.mkdir(parents=True)
    toolchain = toolchain_identity()
    plan = load_plan(repository, toolchain)
    resolution_env = sandboxed_cargo_environment(toolchain, {})
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
        freeze_resolution_source(root)
        config_binding = write_sandboxed_cargo_config_search(
            output / "manifests" / f"cargo-config-{variant}.json",
            root,
            toolchain,
        )
        replay_sandboxed_cargo_config_search(config_binding, root, toolchain)
        historical = git_bytes(
            repository,
            ["show", f"{claim['lock']['commit']}:{claim['lock']['path']}"],
            toolchain,
        )
        lock_path = root / "Cargo.lock"
        current_attempt = None
        if variant in {"C", "D"}:
            atomic_write(lock_path, current_lock)
            replay_sandboxed_cargo_config_search(config_binding, root, toolchain)
            current_attempt = execute_sandboxed_resolution(
                root=root,
                toolchain=toolchain,
                environment=resolution_env,
                cargo_config_search=config_binding,
                cargo_arguments=[
                        "metadata",
                        "--locked",
                        "--offline",
                        "--format-version",
                        "1",
                        "--no-deps",
                    ],
                evidence_root=output / "manifests",
                label=f"current-lock-{variant}",
            )
            replay_sandboxed_cargo_config_search(config_binding, root, toolchain)
            if (
                current_attempt["exit_status"] != 0
                or current_attempt["lock_output"]["pre"]
                != current_attempt["lock_output"]["post"]
            ):
                diagnostics = output / "diagnostics"
                diagnostics.mkdir(mode=0o700)
                atomic_json(
                    diagnostics / f"current-lock-{variant}.json",
                    current_attempt,
                )
                raise PreparationError(
                    f"{variant} current-lock attempt failed or changed Cargo.lock: "
                    f"{current_attempt['stderr'].strip()}"
                )
            lock_path.unlink()
            replay_sandboxed_cargo_config_search(config_binding, root, toolchain)
            resolved = execute_sandboxed_resolution(
                root=root,
                toolchain=toolchain,
                environment=resolution_env,
                cargo_config_search=config_binding,
                cargo_arguments=["generate-lockfile", "--offline"],
                evidence_root=output / "manifests",
                label=f"resolved-lock-{variant}",
            )
            replay_sandboxed_cargo_config_search(config_binding, root, toolchain)
            if resolved["exit_status"] != 0 or not lock_path.is_file():
                raise PreparationError(f"{variant} offline lock resolution failed: {resolved['stderr']}")
            final_lock = lock_path.read_bytes()
        else:
            if hash_bytes(historical) != claim["lock"]["sha256"]:
                raise PreparationError(f"{variant} tracked lock changed")
            final_lock = historical
            resolved, resolved_payload = retained_git_run(
                repository,
                [
                    "show",
                    f"{claim['lock']['commit']}:{claim['lock']['path']}",
                ],
                toolchain,
            )
            resolved["cargo_config_search"] = config_binding
            resolved["environment"] = frozen_cargo_environment(toolchain)
            resolved["host_source_root"] = str(root.resolve())
            resolved["resolver_kind"] = "tracked_git_readback"
            resolved["toolchain"] = toolchain
            if (
                resolved["exit_status"] != 0
                or resolved_payload != historical
            ):
                raise PreparationError(f"{variant} tracked lock readback failed")
        replay_sandboxed_cargo_config_search(config_binding, root, toolchain)
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


def cargo_home_overlay_descriptor(argv: list[str], context: str) -> tuple[int, int]:
    if argv.count("--overlay-src") != 1 or argv.count("--tmp-overlay") != 1:
        raise PreparationError(f"{context} Cargo-home overlay cardinality differs")
    index = argv.index("--overlay-src")
    if argv[index : index + 4][2:] != ["--tmp-overlay", GUEST_CARGO_HOME]:
        raise PreparationError(f"{context} Cargo-home overlay topology differs")
    source = argv[index + 1] if index + 1 < len(argv) else ""
    prefix = "/proc/self/fd/"
    descriptor_text = source.removeprefix(prefix)
    if (
        not source.startswith(prefix)
        or not descriptor_text.isascii()
        or not descriptor_text.isdecimal()
        or len(descriptor_text) > 10
        or str(int(descriptor_text)) != descriptor_text
        or int(descriptor_text) < 3
    ):
        raise PreparationError(f"{context} Cargo-home overlay descriptor differs")
    return int(descriptor_text), index


def resolution_sandbox_descriptors(
    argv: Any, context: str
) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    """Extract only the descriptor layout accepted by the fixed resolver argv."""

    if (
        not isinstance(argv, list)
        or not argv
        or any(not isinstance(argument, str) for argument in argv)
    ):
        raise PreparationError(f"{context} argv is not an exact string list")
    expected_bindings = (
        *(
            (f"system:{guest_path}", "--ro-bind-fd", guest_path)
            for _host_path, guest_path in TRUSTED_SYSTEM_MOUNTS
        ),
        ("source", "--bind-fd", GUEST_SOURCE),
        ("toolchain_root", "--ro-bind-fd", GUEST_TOOLCHAIN_ROOT),
        ("cargo", "--ro-bind-fd", GUEST_CARGO),
        ("rustc", "--ro-bind-fd", GUEST_RUSTC),
        *(
            (f"config:{guest_path}", "--ro-bind-fd", guest_path)
            for guest_path in GUEST_BOUND_CONFIG_PATHS
        ),
    )
    descriptors: dict[str, int] = {}
    observed: list[tuple[str, str, str]] = []
    for index, argument in enumerate(argv):
        if argument not in {"--ro-bind-fd", "--bind-fd"}:
            continue
        if index + 2 >= len(argv):
            raise PreparationError(f"{context} descriptor binding is truncated")
        descriptor_text = argv[index + 1]
        if (
            not descriptor_text.isdecimal()
            or str(int(descriptor_text)) != descriptor_text
        ):
            raise PreparationError(f"{context} descriptor is not canonical")
        ordinal = len(observed)
        if ordinal >= len(expected_bindings):
            raise PreparationError(f"{context} has an extra descriptor binding")
        name, _option, _destination = expected_bindings[ordinal]
        observed.append((name, argument, argv[index + 2]))
        descriptors[name] = int(descriptor_text)
    if tuple(observed) != expected_bindings:
        raise PreparationError(f"{context} guest descriptor bindings differ")
    cargo_home_descriptor, _overlay_index = cargo_home_overlay_descriptor(
        argv, context
    )
    descriptors["cargo_home"] = cargo_home_descriptor
    if len(descriptors) != len(set(descriptors.values())):
        raise PreparationError(f"{context} descriptors alias")
    core = {
        name: descriptors[name]
        for name in ("cargo", "cargo_home", "rustc", "source", "toolchain_root")
    }
    system = {
        guest_path: descriptors[f"system:{guest_path}"]
        for _host_path, guest_path in TRUSTED_SYSTEM_MOUNTS
    }
    config = {
        guest_path: descriptors[f"config:{guest_path}"]
        for guest_path in GUEST_BOUND_CONFIG_PATHS
    }
    return core, system, config


def validate_resolver_output_hashes(record: dict[str, Any], context: str) -> None:
    for stream in ("stderr", "stdout"):
        payload = record.get(stream)
        digest = record.get(f"{stream}_sha256")
        if (
            not isinstance(payload, str)
            or not is_lower_hex(digest, SHA256)
            or hash_bytes(payload.encode("utf-8")) != digest
        ):
            raise PreparationError(f"{context} {stream} authority differs")


def validate_lock_manifest(
    repository: Path, locks: dict[str, Any], plan: dict[str, Any]
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
    expected_sandbox_environment = sandboxed_cargo_environment(toolchain, {})
    expected_git_environment = frozen_cargo_environment(toolchain)
    current_claim = plan["variants"]["A"]
    if (
        current_claim.get("product_commit")
        != current_claim.get("lock", {}).get("commit")
        or current_claim.get("lock", {}).get("path") != "Cargo.lock"
        or not is_lower_hex(current_claim.get("lock", {}).get("sha256"), SHA256)
    ):
        raise PreparationError("current Cargo.lock source-plan authority differs")
    current_lock_sha256 = current_claim["lock"]["sha256"]
    shared_manifest_hashes: set[str] = set()
    for variant in VARIANTS:
        claim = locks["variants"][variant]
        if not isinstance(claim, dict) or set(claim) != {
            "adapter_sha256",
            "archive_sha256",
            "current_lock_attempt",
            "current_to_final",
            "final_lock_path",
            "final_lock_sha256",
            "historical_lock",
            "historical_to_final",
            "overlay_manifest_path",
            "overlay_manifest_sha256",
            "resolver",
            "shared_manifest_sha256",
        }:
            raise PreparationError(f"{variant} lock-candidate fields differ")
        if (
            claim.get("historical_lock") != plan["variants"][variant]["lock"]
            or not is_lower_hex(claim.get("final_lock_sha256"), SHA256)
        ):
            raise PreparationError(f"{variant} lock-candidate authority differs")
        resolver = claim.get("resolver", {})
        current_attempt = claim.get("current_lock_attempt")
        if (variant in {"C", "D"}) != isinstance(current_attempt, dict):
            raise PreparationError(f"{variant} current-lock topology differs")
        if variant in {"A", "B"} and current_attempt is not None:
            raise PreparationError(f"{variant} unexpectedly has a current-lock attempt")
        final_lock_lexical = Path(str(claim.get("final_lock_path")))
        if not final_lock_lexical.is_absolute():
            raise PreparationError(f"{variant} lock path is not absolute")
        output_root = final_lock_lexical.parent.parent
        expected_lock_path = output_root / "locks" / f"Cargo-{variant}.lock"
        expected_overlay_path = (
            output_root / "manifests" / f"overlay-{variant}.json"
        )
        expected_config_path = (
            output_root / "manifests" / f"cargo-config-{variant}.json"
        )
        expected_source_root = (
            output_root / "materialized" / variant
        ).resolve(strict=True)
        if final_lock_lexical != expected_lock_path:
            raise PreparationError(f"{variant} lock path topology differs")
        records = [("generated", resolver)]
        if current_attempt is not None:
            records.append(("current", current_attempt))
        for role, record in records:
            expected_kind = (
                "tracked_git_readback"
                if variant in {"A", "B"}
                else "sandboxed_cargo_resolution"
            )
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
            sandboxed_fields = tracked_fields | {
                "execution_authority",
                "lock_output",
                "passed_file_descriptors",
                "semantic_input_authority",
            }
            expected_fields = (
                tracked_fields
                if expected_kind == "tracked_git_readback"
                else sandboxed_fields
            )
            if (
                not isinstance(record, dict)
                or set(record) != expected_fields
                or record.get("resolver_kind") != expected_kind
                or record.get("toolchain") != toolchain
                or record.get("environment")
                != (
                    expected_git_environment
                    if expected_kind == "tracked_git_readback"
                    else expected_sandbox_environment
                )
                or not isinstance(record.get("exit_status"), int)
                or isinstance(record.get("exit_status"), bool)
                or record.get("exit_status") != 0
            ):
                raise PreparationError(f"{variant} resolver record fields differ")
            validate_resolver_output_hashes(
                record, f"{variant} {role} resolver"
            )
            binding = record.get("cargo_config_search")
            if (
                not isinstance(binding, dict)
                or set(binding) != {"path", "sha256"}
                or binding.get("path") != str(expected_config_path)
            ):
                raise PreparationError(f"{variant} Cargo config binding is absent")
            load_canonical(Path(binding["path"]), CARGO_CONFIG_SCHEMA)
            host_source_root = Path(str(record.get("host_source_root")))
            if (
                record.get("host_source_root") != str(expected_source_root)
                or host_source_root.resolve(strict=True) != expected_source_root
            ):
                raise PreparationError(
                    f"{variant} resolver source-root authority differs"
                )
            replay_sandboxed_cargo_config_search(
                binding, host_source_root, toolchain
            )
            if expected_kind == "sandboxed_cargo_resolution":
                if record.get("cwd") != GUEST_SOURCE:
                    raise PreparationError(f"{variant} resolver cwd differs")
                execution_authority = record.get("execution_authority")
                if (
                    not isinstance(execution_authority, dict)
                    or execution_authority.get("path")
                    != toolchain["bwrap_path"]
                    or execution_authority.get("sha256")
                    != toolchain["bwrap_sha256"]
                ):
                    raise PreparationError(
                        f"{variant} resolver execution authority differs"
                    )
                replay_artifact_binding(
                    execution_authority,
                    f"{variant} {role} resolver retained bwrap",
                )
                descriptors, system_descriptors, config_descriptors = (
                    resolution_sandbox_descriptors(
                        record.get("argv"), f"{variant} {role} resolver"
                    )
                )
                cargo_arguments = (
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
                if record["argv"] != sandboxed_resolution_argv(
                    descriptors,
                    system_descriptors,
                    config_descriptors,
                    cargo_arguments,
                    toolchain["bwrap_path"],
                ):
                    raise PreparationError(
                        f"{variant} {role} resolver argv differs"
                    )
                if record.get("passed_file_descriptors") != 13:
                    raise PreparationError(
                        f"{variant} resolver descriptor count differs"
                    )
                authority = record["semantic_input_authority"]
                if (
                    not isinstance(authority, dict)
                    or authority.get("schema") != SEMANTIC_INPUT_AUTHORITY_SCHEMA
                    or authority.get("source", {}).get("role")
                    != "resolution_source_without_cargo_lock"
                ):
                    raise PreparationError(
                        f"{variant} resolver semantic authority differs"
                    )
                validate_semantic_input_authority(
                    authority,
                    f"{variant} resolver",
                    replay_evidence=True,
                    live_roots={
                        "cargo_home": Path(toolchain["cargo_home_path"]),
                        "source": host_source_root,
                        "toolchain": Path(toolchain["cargo_path"]).parent.parent,
                    },
                    source_role="resolution_source_without_cargo_lock",
                )
                lock_output = record.get("lock_output")
                if (
                    not isinstance(lock_output, dict)
                    or set(lock_output) != {"post", "pre"}
                    or any(
                        not isinstance(lock_output.get(boundary), dict)
                        or set(lock_output[boundary])
                        != {"path", "sha256", "status"}
                        for boundary in ("pre", "post")
                    )
                ):
                    raise PreparationError(
                        f"{variant} resolver lock-output authority differs"
                    )
                lock_path_string = str(expected_source_root / "Cargo.lock")
                if role == "current":
                    expected_boundary = {
                        "path": lock_path_string,
                        "sha256": current_lock_sha256,
                        "status": "present",
                    }
                    if (
                        lock_output["pre"] != expected_boundary
                        or lock_output["post"] != expected_boundary
                    ):
                        raise PreparationError(
                            f"{variant} current-lock replay differs"
                        )
                elif lock_output != {
                    "post": {
                        "path": lock_path_string,
                        "sha256": claim.get("final_lock_sha256"),
                        "status": "present",
                    },
                    "pre": {
                        "path": lock_path_string,
                        "sha256": None,
                        "status": "absent",
                    },
                }:
                    raise PreparationError(
                        f"{variant} generated lock authority differs"
                    )
            else:
                historical = claim.get("historical_lock")
                if (
                    role != "generated"
                    or not isinstance(historical, dict)
                    or set(historical) != {"commit", "path", "sha256"}
                    or record.get("argv")
                    != [
                        toolchain["git_path"],
                        "-C",
                        str(repository),
                        "show",
                        f"{historical['commit']}:{historical['path']}",
                    ]
                    or record.get("cwd") != str(repository.resolve())
                    or record.get("stdout_sha256") != historical["sha256"]
                    or record.get("stdout_sha256")
                    != claim.get("final_lock_sha256")
                    or record.get("stderr") != ""
                    or record.get("stderr_sha256") != EMPTY_SHA256
                ):
                    raise PreparationError(
                        f"{variant} tracked lock resolver authority differs"
                    )
        lock_path = Path(claim["final_lock_path"]).resolve(strict=True)
        if (
            lock_path != expected_lock_path
            or hash_file(lock_path) != claim.get("final_lock_sha256")
        ):
            raise PreparationError(f"{variant} lock candidate changed")
        overlay_path = Path(claim["overlay_manifest_path"]).resolve(strict=True)
        if (
            overlay_path != expected_overlay_path
            or hash_file(overlay_path) != claim.get("overlay_manifest_sha256")
        ):
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


def validate_current_children_authority(
    current_children: CanonicalSnapshot,
    tools_manifest: CanonicalSnapshot,
    lock_manifest: CanonicalSnapshot,
    lock_authority: CanonicalSnapshot,
    lock_review_bundle: CanonicalSnapshot,
) -> None:
    attestation = current_children.value
    expected_fields = {
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
    if set(attestation) != expected_fields or attestation.get("status") != "ok":
        raise PreparationError("current-child attestation fields/status differ")
    builds = attestation.get("builds")
    if not isinstance(builds, dict) or set(builds) != {
        "children",
        "hooked_release",
        "pristine_release",
    }:
        raise PreparationError("current-child build set differs")
    runtime_digests = set()
    toolchain_value = attestation.get("toolchain")
    if not isinstance(toolchain_value, dict):
        raise PreparationError("current-child toolchain is absent")
    toolchain_root = Path(str(toolchain_value.get("cargo_path"))).parent.parent
    cargo_home = Path(str(toolchain_value.get("cargo_home_path")))
    source_roots = {
        "children": current_children.path.parent / "materialized" / "children",
        "hooked_release": (
            current_children.path.parent / "materialized" / "hooked-release"
        ),
        "pristine_release": (
            current_children.path.parent / "materialized" / "pristine-release"
        ),
    }
    for name, build in builds.items():
        if not isinstance(build, dict):
            raise PreparationError(f"current-child build is not an object: {name}")
        authority = validate_semantic_input_authority(
            build.get("semantic_input_authority"),
            f"current-child {name} build",
            replay_evidence=True,
            builder_local_evidence=True,
            live_roots={
                "cargo_home": cargo_home,
                "source": source_roots[name],
                "toolchain": toolchain_root,
            },
        )
        runtime_digests.add(authority["runtime_sha256"])
    if len(runtime_digests) != 1:
        raise PreparationError("current-child runtime authorities differ")
    if (
        attestation.get("protocol") != PROTOCOL
        or attestation.get("protocol_sha256") != PROTOCOL_DOCUMENT_SHA256
        or attestation.get("product_commit") != CURRENT_PRODUCT_COMMIT
        or attestation.get("product_tree") != CURRENT_PRODUCT_TREE
        or attestation.get("tools_manifest_path") != str(tools_manifest.path)
        or attestation.get("tools_manifest_sha256") != tools_manifest.sha256
        or attestation.get("lock_manifest_sha256") != lock_manifest.sha256
        or attestation.get("review_bundle_sha256") != lock_review_bundle.sha256
    ):
        raise PreparationError("current-child attestation source binding differs")
    child_artifacts = attestation.get("artifacts")
    manifest_tools = tools_manifest.value["tools"]
    if (
        not isinstance(child_artifacts, dict)
        or set(child_artifacts) != {"correctness", "fault"}
        or any(
            child_artifacts.get(name) != manifest_tools.get(name)
            for name in child_artifacts
        )
    ):
        raise PreparationError("current-child artifact/tools bindings differ")
    if attestation.get("lock_authority") != lock_authority.value:
        raise PreparationError("current-child lock authority payload differs")
    authority_inputs = attestation.get("lock_authority_inputs")
    expected_inputs = {
        "authority": lock_authority,
        "lock_manifest": lock_manifest,
        "review_bundle": lock_review_bundle,
    }
    if not isinstance(authority_inputs, dict) or set(authority_inputs) != set(
        expected_inputs
    ):
        raise PreparationError("current-child lock authority input set differs")
    for name, snapshot in expected_inputs.items():
        expected = source_review_input(snapshot)
        expected.pop("schema")
        if authority_inputs.get(name) != expected:
            raise PreparationError(
                f"current-child lock authority input differs: {name}"
            )
    authority = lock_authority.value
    if (
        authority.get("status") != "approved"
        or authority.get("protocol") != PROTOCOL
        or authority.get("protocol_sha256") != PROTOCOL_DOCUMENT_SHA256
        or authority.get("review_sha256") != lock_review_bundle.sha256
        or not isinstance(authority.get("lock_manifest"), dict)
        or authority["lock_manifest"].get("sha256") != lock_manifest.sha256
        or not isinstance(authority.get("review_bundle"), dict)
        or authority["review_bundle"].get("sha256") != lock_review_bundle.sha256
    ):
        raise PreparationError("reviewed current lock authority crosslink differs")
    release_approval = attestation.get("release_compile_out_approval")
    if release_approval != {
        "final_integration_action": PREAPPROVAL_FINAL_ACTION,
        "source_approval_sha256": PREAPPROVAL_SOURCE_SENTINEL,
        "source_approval_status": "preapproval-sentinel-not-source-approved",
    }:
        raise PreparationError("current-child preapproval sentinel/action differs")
    overlay_authority = attestation.get("product_overlay_authority")
    overlay_patch = (
        overlay_authority.get("patch")
        if isinstance(overlay_authority, dict)
        else None
    )
    if (
        not isinstance(overlay_patch, dict)
        or overlay_patch.get("sha256") != CURRENT_PRODUCT_OVERLAY_SHA256
    ):
        raise PreparationError("current-child product overlay authority differs")
    compile_out = attestation.get("release_compile_out")
    if (
        not isinstance(compile_out, dict)
        or compile_out.get("preapproval_source_sentinel")
        != PREAPPROVAL_SOURCE_SENTINEL
        or compile_out.get("binary_byte_identical") is not True
        or compile_out.get("symbol_inventory_byte_identical") is not True
        or compile_out.get("forbidden_hook_strings")
        != list(FORBIDDEN_RELEASE_HOOK_STRINGS)
    ):
        raise PreparationError("current-child preapproval compile-out proof differs")
    if b"/asterism/preapproval-placeholder/" in tools_manifest.payload:
        raise PreparationError("final tools manifest retains a child placeholder")


def source_review_assertion(
    *,
    inputs: dict[str, CanonicalSnapshot],
    requirement: dict[str, Any],
    tooling_commit: str,
    tooling_tree: str,
) -> dict[str, Any]:
    if set(inputs) != SOURCE_REVIEW_INPUT_NAMES:
        raise PreparationError("source-review input set differs")
    return {
        "inputs": {
            name: source_review_input(inputs[name]) for name in sorted(inputs)
        },
        "open_findings": 0,
        "protocol": PROTOCOL,
        "protocol_sha256": PROTOCOL_DOCUMENT_SHA256,
        "release_compile_out_requirement": requirement,
        "schema": SOURCE_REVIEW_ASSERTION_SCHEMA,
        "status": "approved",
        "tooling_commit": tooling_commit,
        "tooling_tree": tooling_tree,
    }


def validate_source_review_bundle(
    bundle_snapshot: CanonicalSnapshot,
    expected_assertion: dict[str, Any],
) -> tuple[str, str]:
    bundle = bundle_snapshot.value
    if set(bundle) != {
        "assertion",
        "assertion_sha256",
        "review_created",
        "schema",
        "verdict",
    }:
        raise PreparationError("source-review bundle fields differ")
    assertion_sha256 = hash_bytes(canonical_json(expected_assertion))
    if (
        bundle.get("assertion") != expected_assertion
        or bundle.get("assertion_sha256") != assertion_sha256
    ):
        raise PreparationError("source-review assertion binding differs")
    created = exact_seal_event(bundle.get("review_created"), "ReviewCreated")
    created_data = created["data"]
    if set(created_data) != {
        "description",
        "initial_commit",
        "jj_change_id",
        "review_id",
        "scm_anchor",
        "scm_kind",
        "title",
    }:
        raise PreparationError("Seal ReviewCreated data fields differ")
    review_id = validate_identifier(created_data.get("review_id"), "Seal review id")
    commit = expected_assertion["tooling_commit"]
    detached_anchor = f"detached:{commit}"
    if (
        created_data.get("initial_commit") != commit
        or created_data.get("jj_change_id") != detached_anchor
        or created_data.get("scm_anchor") != detached_anchor
        or created_data.get("scm_kind") != "git"
        or not isinstance(created_data.get("title"), str)
        or not created_data["title"]
        or not isinstance(created_data.get("description"), str)
        or not created_data["description"]
    ):
        raise PreparationError("Seal ReviewCreated anchor/content differs")
    verdict = exact_seal_event(bundle.get("verdict"), "ReviewerVoted")
    expected_reason = (
        f"APPROVED assertion_sha256={assertion_sha256}; open_findings=0"
    )
    if set(verdict["data"]) != {"reason", "review_id", "vote"} or verdict[
        "data"
    ] != {"reason": expected_reason, "review_id": review_id, "vote": "lgtm"}:
        raise PreparationError("Seal ReviewerVoted verdict content differs")
    if parse_zoned_time(verdict["ts"], "Seal ReviewerVoted timestamp") < (
        parse_zoned_time(created["ts"], "Seal ReviewCreated timestamp")
    ):
        raise PreparationError("Seal verdict predates ReviewCreated")
    return review_id, verdict["ts"]


def resample_source_review_authority(
    inputs: dict[str, CanonicalSnapshot], bundle: CanonicalSnapshot
) -> None:
    resample_source_review_inputs(inputs)
    require_same_snapshot(
        bundle,
        immutable_canonical_snapshot(
            bundle.path,
            SOURCE_REVIEW_BUNDLE_SCHEMA,
            "resampled source-review bundle",
        ),
        "source-review bundle",
    )


def resample_source_review_inputs(
    inputs: dict[str, CanonicalSnapshot],
) -> None:
    if set(inputs) != SOURCE_REVIEW_INPUT_NAMES:
        raise PreparationError("source-review resample input set differs")
    for name, snapshot in inputs.items():
        schema = snapshot.value["schema"]
        require_same_snapshot(
            snapshot,
            immutable_canonical_snapshot(
                snapshot.path, schema, f"resampled source-review {name}"
            ),
            f"source-review {name}",
        )


def capture_source_review_assertion(
    repository: Path,
    *,
    current_children_path: Path,
    lock_manifest_path: Path,
    tools_path: Path,
    lock_authority_path: Path,
    lock_review_bundle_path: Path,
    toolchain: dict[str, str],
) -> tuple[dict[str, CanonicalSnapshot], dict[str, Any], dict[str, Any]]:
    """Capture and deeply validate the exact inputs a source reviewer approves."""

    specifications = {
        "current_children_attestation": (
            current_children_path,
            CURRENT_CHILDREN_ATTESTATION_SCHEMA,
        ),
        "lock_authority": (lock_authority_path, CURRENT_LOCK_AUTHORITY_SCHEMA),
        "lock_manifest": (lock_manifest_path, LOCK_SCHEMA),
        "lock_review_bundle": (
            lock_review_bundle_path,
            CURRENT_LOCK_REVIEW_BUNDLE_SCHEMA,
        ),
        "tools_manifest": (tools_path, "asterism-rebaseline-tools-v3"),
    }
    inputs = {
        name: immutable_canonical_snapshot(path, schema, f"source-review {name}")
        for name, (path, schema) in specifications.items()
    }
    identities = [
        (snapshot.identity["device"], snapshot.identity["inode"])
        for snapshot in inputs.values()
    ]
    if len(set(identities)) != len(identities):
        raise PreparationError("source-review inputs are not identity-disjoint")
    locks = inputs["lock_manifest"].value
    tools = validate_tools_manifest(tools_path)
    if tools != inputs["tools_manifest"].value:
        raise PreparationError("source-review tools snapshot changed during validation")
    plan = load_plan(repository, toolchain)
    validate_lock_manifest(repository, locks, plan)
    validate_current_children_authority(
        inputs["current_children_attestation"],
        inputs["tools_manifest"],
        inputs["lock_manifest"],
        inputs["lock_authority"],
        inputs["lock_review_bundle"],
    )
    commit, tree = tooling_identity(repository, toolchain)
    requirement = release_compile_out_requirement(
        inputs["current_children_attestation"]
    )
    assertion = source_review_assertion(
        inputs=inputs,
        requirement=requirement,
        tooling_commit=commit,
        tooling_tree=tree,
    )
    resample_source_review_inputs(inputs)
    return inputs, requirement, assertion


def validate_source_review_authority(
    repository: Path,
    *,
    bundle_path: Path,
    current_children_path: Path,
    lock_manifest_path: Path,
    tools_path: Path,
    lock_authority_path: Path,
    lock_review_bundle_path: Path,
    toolchain: dict[str, str],
) -> tuple[dict[str, CanonicalSnapshot], CanonicalSnapshot, dict[str, Any], str, str]:
    inputs, requirement, assertion = capture_source_review_assertion(
        repository,
        current_children_path=current_children_path,
        lock_manifest_path=lock_manifest_path,
        tools_path=tools_path,
        lock_authority_path=lock_authority_path,
        lock_review_bundle_path=lock_review_bundle_path,
        toolchain=toolchain,
    )
    bundle = immutable_canonical_snapshot(
        bundle_path, SOURCE_REVIEW_BUNDLE_SCHEMA, "source-review bundle"
    )
    identities = {
        (snapshot.identity["device"], snapshot.identity["inode"])
        for snapshot in inputs.values()
    }
    if (bundle.identity["device"], bundle.identity["inode"]) in set(identities):
        raise PreparationError("source-review bundle aliases a reviewed input")
    review_id, reviewed_at = validate_source_review_bundle(bundle, assertion)
    resample_source_review_authority(inputs, bundle)
    return inputs, bundle, requirement, review_id, reviewed_at


def validate_approval(
    repository: Path,
    approval_path: Path,
    lock_manifest_path: Path,
    tools_path: Path,
    source_review_bundle_path: Path,
    current_children_path: Path,
    lock_authority_path: Path,
    lock_review_bundle_path: Path,
    toolchain: dict[str, str],
) -> tuple[
    CanonicalSnapshot,
    dict[str, Any],
    dict[str, Any],
    dict[str, CanonicalSnapshot],
    CanonicalSnapshot,
]:
    approval_snapshot = immutable_canonical_snapshot(
        approval_path, APPROVAL_SCHEMA, "source approval"
    )
    approval = approval_snapshot.value
    source_inputs, source_bundle, requirement, review_id, reviewed_at = (
        validate_source_review_authority(
            repository,
            bundle_path=source_review_bundle_path,
            current_children_path=current_children_path,
            lock_manifest_path=lock_manifest_path,
            tools_path=tools_path,
            lock_authority_path=lock_authority_path,
            lock_review_bundle_path=lock_review_bundle_path,
            toolchain=toolchain,
        )
    )
    locks = source_inputs["lock_manifest"].value
    tools = source_inputs["tools_manifest"].value
    plan = load_plan(repository, toolchain)
    expected_top = {
        "schema", "protocol", "protocol_sha256", "status", "review_id", "reviewed_at",
        "comm_allowlist", "filesystem_admission", "toolchain", "tooling_commit", "tooling_tree",
        "shared_manifest_sha256", "source_review", "tools_manifest",
        "tools_manifest_sha256", "variants",
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
    if approval.get("review_id") != review_id or approval.get("reviewed_at") != reviewed_at:
        raise PreparationError("source approval review provenance was not Seal-derived")
    tools_sha256 = source_inputs["tools_manifest"].sha256
    if (
        approval.get("tools_manifest") != tools
        or approval.get("tools_manifest_sha256") != tools_sha256
        or tools_sha256 != hash_bytes(canonical_json(tools))
    ):
        raise PreparationError("source approval tools manifest mismatch")
    commit, tree = tooling_identity(repository, toolchain)
    if approval.get("tooling_commit") != commit or approval.get("tooling_tree") != tree:
        raise PreparationError("source approval tooling identity mismatch")
    source_review = approval.get("source_review")
    expected_source_review = {
        "assertion_sha256": source_bundle.value["assertion_sha256"],
        "bundle": content_binding(source_bundle, SOURCE_REVIEW_BUNDLE_SCHEMA),
        "current_children_attestation": content_binding(
            source_inputs["current_children_attestation"],
            CURRENT_CHILDREN_ATTESTATION_SCHEMA,
        ),
        "lock_authority": content_binding(
            source_inputs["lock_authority"], CURRENT_LOCK_AUTHORITY_SCHEMA
        ),
        "lock_review_bundle": content_binding(
            source_inputs["lock_review_bundle"],
            CURRENT_LOCK_REVIEW_BUNDLE_SCHEMA,
        ),
        "release_compile_out_requirement": requirement,
    }
    if source_review != expected_source_review:
        raise PreparationError("source approval reviewed-input binding differs")
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
    resample_source_review_authority(source_inputs, source_bundle)
    return approval_snapshot, locks, tools, source_inputs, source_bundle


def write_approval(
    repository: Path,
    lock_manifest_path: Path,
    tools_path: Path,
    output: Path,
    source_review_bundle_path: Path,
    current_children_path: Path,
    lock_authority_path: Path,
    lock_review_bundle_path: Path,
) -> None:
    if output.exists() or output.is_symlink():
        raise PreparationError(f"approval output must be absent: {output}")
    toolchain = toolchain_identity()
    source_inputs, source_bundle, requirement, review_id, reviewed_at = (
        validate_source_review_authority(
            repository,
            bundle_path=source_review_bundle_path,
            current_children_path=current_children_path,
            lock_manifest_path=lock_manifest_path,
            tools_path=tools_path,
            lock_authority_path=lock_authority_path,
            lock_review_bundle_path=lock_review_bundle_path,
            toolchain=toolchain,
        )
    )
    locks = source_inputs["lock_manifest"].value
    tools = source_inputs["tools_manifest"].value
    plan = load_plan(repository, toolchain)
    commit, tree = tooling_identity(repository, toolchain)
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
        "source_review": {
            "assertion_sha256": source_bundle.value["assertion_sha256"],
            "bundle": content_binding(
                source_bundle, SOURCE_REVIEW_BUNDLE_SCHEMA
            ),
            "current_children_attestation": content_binding(
                source_inputs["current_children_attestation"],
                CURRENT_CHILDREN_ATTESTATION_SCHEMA,
            ),
            "lock_authority": content_binding(
                source_inputs["lock_authority"], CURRENT_LOCK_AUTHORITY_SCHEMA
            ),
            "lock_review_bundle": content_binding(
                source_inputs["lock_review_bundle"],
                CURRENT_LOCK_REVIEW_BUNDLE_SCHEMA,
            ),
            "release_compile_out_requirement": requirement,
        },
        "status": "approved",
        "toolchain": locks["toolchain"],
        "tools_manifest": tools,
        "tools_manifest_sha256": source_inputs["tools_manifest"].sha256,
        "tooling_commit": commit,
        "tooling_tree": tree,
        "variants": variants,
    }
    resample_source_review_authority(source_inputs, source_bundle)
    atomic_write(output, canonical_json(approval), mode=0o444)


def make_read_only(root: Path) -> None:
    for path in sorted(root.rglob("*"), key=lambda value: len(value.parts), reverse=True):
        if path.is_dir():
            if stat.S_IMODE(path.stat().st_mode) != 0o555:
                path.chmod(0o555)
        elif path.is_file():
            target_mode = 0o555 if path.stat().st_mode & 0o111 else 0o444
            if stat.S_IMODE(path.stat().st_mode) != target_mode:
                path.chmod(target_mode)
        else:
            raise PreparationError(f"unsupported staged path {path}")
    if stat.S_IMODE(root.stat().st_mode) != 0o555:
        root.chmod(0o555)


def freeze_prepared_root(root: Path, claims: Path) -> None:
    claims = claims.resolve(strict=True)
    for path in sorted(root.rglob("*"), key=lambda value: len(value.parts), reverse=True):
        if path == claims or claims in path.parents:
            continue
        if path.is_symlink() or (not path.is_file() and not path.is_dir()):
            raise PreparationError(f"unsupported prepared path {path}")
        if path.is_dir():
            if stat.S_IMODE(path.stat().st_mode) != 0o555:
                path.chmod(0o555)
        else:
            target_mode = 0o555 if path.stat().st_mode & 0o111 else 0o444
            if stat.S_IMODE(path.stat().st_mode) != target_mode:
                path.chmod(target_mode)
    if stat.S_IMODE(claims.stat().st_mode) != 0o700:
        claims.chmod(0o700)
    if stat.S_IMODE(root.stat().st_mode) != 0o555:
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


def prepared_authority_manifest(root: Path) -> dict[str, Any]:
    """Snapshot every file and directory in the prepared authority tree."""

    root = root.resolve(strict=True)
    entries: list[dict[str, Any]] = []
    for path in [root, *sorted(root.rglob("*"))]:
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            raise PreparationError(f"prepared authority contains symlink {path}")
        entry: dict[str, Any] = {
            "kind": "directory" if stat.S_ISDIR(metadata.st_mode) else "file",
            "mode": f"{stat.S_IMODE(metadata.st_mode):04o}",
            "path": "." if path == root else path.relative_to(root).as_posix(),
        }
        if stat.S_ISREG(metadata.st_mode):
            entry.update({"bytes": metadata.st_size, "sha256": hash_file(path)})
        elif not stat.S_ISDIR(metadata.st_mode):
            raise PreparationError(f"unsupported prepared authority path {path}")
        entries.append(entry)
    return {
        "entries": entries,
        "protocol": PROTOCOL,
        "root": str(root),
        "schema": "bn-3hch-prepared-authority-manifest-v1",
    }


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
    environment = frozen_runtime_environment({"PYTHONDONTWRITEBYTECODE": "1"})
    for tool_name in ("evaluator_runtime", "terminal_verifier_runtime"):
        tool_path = Path(bound_tools[tool_name]["path"])
        descriptor, payload, observed, _full = open_retained_artifact(
            tool_path, f"prepared support import runtime {tool_name}"
        )
        try:
            if {
                "mode": observed["mode"],
                "path": observed["path"],
                "sha256": observed["sha256"],
            } != {
                "mode": bound_tools[tool_name]["executable_mode"],
                "path": bound_tools[tool_name]["path"],
                "sha256": bound_tools[tool_name]["sha256"],
            }:
                raise PreparationError(
                    f"prepared support import runtime binding differs: {tool_name}"
                )
            result = subprocess.run(
                # -I implies -E, discarding PYTHONDONTWRITEBYTECODE; -B is the
                # only way to keep the frozen support tree free of __pycache__.
                [str(tool_path), "-I", "-B", "-c", import_program],
                executable=f"/proc/self/fd/{descriptor}",
                pass_fds=(descriptor,),
                cwd=support_root,
                env=environment,
                capture_output=True,
                timeout=30,
            )
            if result.returncode != 0:
                raise PreparationError(
                    f"{tool_name} cannot import frozen evidence schema: "
                    f"{result.stderr.decode(errors='replace')}"
                )
        finally:
            try:
                verify_retained_artifact(
                    descriptor,
                    tool_path,
                    payload,
                    observed,
                    f"prepared support import runtime {tool_name}",
                )
            finally:
                os.close(descriptor)
        if file_manifest(support_root) != before:
            raise PreparationError(f"{tool_name} mutated frozen support files")


def sandboxed_build_argv(
    descriptors: dict[str, int],
    system_descriptors: dict[str, int],
    config_descriptors: dict[str, int],
    package: str,
    example: str,
    bwrap_path: str,
    rust_lld_guest_path: str,
) -> list[str]:
    expected_descriptors = {
        "cargo",
        "cargo_home",
        "dev_null",
        "rustc",
        "rust_lld",
        "source",
        "target",
        "toolchain_root",
    }
    if (
        set(descriptors) != expected_descriptors
        or any(
            isinstance(descriptor, bool)
            or not isinstance(descriptor, int)
            or descriptor < 3
            for descriptor in descriptors.values()
        )
        or len(set(descriptors.values())) != len(descriptors)
    ):
        raise PreparationError("release sandbox descriptors differ")
    expected_system_paths = tuple(
        guest_path for _host_path, guest_path in TRUSTED_SYSTEM_MOUNTS
    )
    if (
        tuple(system_descriptors) != expected_system_paths
        or any(
            isinstance(descriptor, bool)
            or not isinstance(descriptor, int)
            or descriptor < 3
            for descriptor in system_descriptors.values()
        )
        or len(set(system_descriptors.values())) != len(system_descriptors)
        or set(descriptors.values()) & set(system_descriptors.values())
    ):
        raise PreparationError("release trusted-system descriptors differ")
    if (
        tuple(config_descriptors) != GUEST_BOUND_CONFIG_PATHS
        or any(
            isinstance(descriptor, bool)
            or not isinstance(descriptor, int)
            or descriptor < 3
            for descriptor in config_descriptors.values()
        )
        or len(set(config_descriptors.values())) != len(config_descriptors)
        or set(descriptors.values()) & set(config_descriptors.values())
        or set(system_descriptors.values()) & set(config_descriptors.values())
    ):
        raise PreparationError("release sandbox config descriptors differ")
    argv = [
        bwrap_path,
        "--die-with-parent",
        "--new-session",
        "--unshare-net",
        "--dir",
        "/usr",
    ]
    for guest_path, descriptor in system_descriptors.items():
        argv.extend(["--ro-bind-fd", str(descriptor), guest_path])
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
        "--ro-bind-fd",
        str(descriptors["source"]),
        GUEST_SOURCE,
        "--bind-fd",
        str(descriptors["target"]),
        GUEST_TARGET,
        "--ro-bind-fd",
        str(descriptors["toolchain_root"]),
        GUEST_TOOLCHAIN_ROOT,
        "--ro-bind-fd",
        str(descriptors["cargo"]),
        GUEST_CARGO,
        "--ro-bind-fd",
        str(descriptors["rustc"]),
        GUEST_RUSTC,
        "--ro-bind-fd",
        str(descriptors["rust_lld"]),
        rust_lld_guest_path,
        "--overlay-src",
        f"/proc/self/fd/{descriptors['cargo_home']}",
        "--tmp-overlay",
        GUEST_CARGO_HOME,
        "--dir",
        f"{GUEST_SOURCE}/.cargo",
        "--tmpfs",
        f"{GUEST_SOURCE}/.cargo",
        "--ro-bind-fd",
        str(config_descriptors[f"{GUEST_SOURCE}/.cargo/config.toml"]),
        f"{GUEST_SOURCE}/.cargo/config.toml",
        "--ro-bind-fd",
        str(config_descriptors[f"{GUEST_SOURCE}/.cargo/config"]),
        f"{GUEST_SOURCE}/.cargo/config",
        "--remount-ro",
        f"{GUEST_SOURCE}/.cargo",
        "--ro-bind-fd",
        str(config_descriptors[f"{GUEST_CARGO_HOME}/config.toml"]),
        f"{GUEST_CARGO_HOME}/config.toml",
        "--ro-bind-fd",
        str(config_descriptors[f"{GUEST_CARGO_HOME}/config"]),
        f"{GUEST_CARGO_HOME}/config",
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
    )
    return argv


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


def compact_identity(metadata: os.stat_result) -> dict[str, int]:
    return {
        "changed_ns": metadata.st_ctime_ns,
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "link_count": metadata.st_nlink,
        "modified_ns": metadata.st_mtime_ns,
    }


def trusted_device_parent_chain(path: Path, context: str) -> list[dict[str, Any]]:
    current = path.absolute().parent
    chain: list[Path] = []
    while True:
        chain.append(current)
        if current == current.parent:
            break
        current = current.parent
    records = []
    for selected in reversed(chain):
        metadata = selected.lstat()
        if (
            stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != 0
            or stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            raise PreparationError(f"{context} trusted parent chain is mutable")
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


def device_identity(metadata: os.stat_result, path: Path) -> dict[str, Any]:
    return {
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


def open_retained_null_device(
    path: Path, context: str
) -> tuple[int, dict[str, Any]]:
    lexical = path.absolute()
    if lexical.resolve(strict=True) != lexical:
        raise PreparationError(f"{context} path differs")
    descriptor = os.open(
        lexical,
        os.O_RDWR | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        metadata = os.fstat(descriptor)
        identity = device_identity(metadata, lexical)
        if (
            not stat.S_ISCHR(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != 0o666
            or metadata.st_nlink != 1
            or os.major(metadata.st_rdev) != 1
            or os.minor(metadata.st_rdev) != 3
            or device_identity(lexical.lstat(), lexical) != identity
        ):
            raise PreparationError(f"{context} is not the exact null device")
        return descriptor, {
            "identity": identity,
            "parent_path_chain": trusted_device_parent_chain(lexical, context),
            "trusted_system": True,
        }
    except BaseException:
        os.close(descriptor)
        raise


def verify_retained_null_device(
    descriptor: int,
    path: Path,
    binding: dict[str, Any],
    context: str,
) -> None:
    lexical = path.absolute()
    if (
        device_identity(os.fstat(descriptor), lexical) != binding["identity"]
        or device_identity(lexical.lstat(), lexical) != binding["identity"]
        or trusted_device_parent_chain(lexical, context)
        != binding["parent_path_chain"]
        or binding.get("trusted_system") is not True
    ):
        raise PreparationError(f"{context} changed while retained")


def open_retained_artifact(
    path: Path, context: str
) -> tuple[int, bytes, dict[str, Any], dict[str, Any]]:
    lexical = path.absolute()
    descriptor = os.open(
        lexical, os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise PreparationError(f"{context} is not a single-link regular file")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(descriptor)
        fields = (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_nlink",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if any(getattr(before, field) != getattr(after, field) for field in fields):
            raise PreparationError(f"{context} changed while retained")
        payload = b"".join(chunks)
        resolved = lexical.resolve(strict=True)
        current = lexical.lstat()
        if (
            resolved != lexical
            or stat.S_ISLNK(current.st_mode)
            or any(
                getattr(current, field) != getattr(after, field)
                for field in fields
            )
            or len(payload) != after.st_size
        ):
            raise PreparationError(f"{context} path/identity changed while retained")
        digest = hash_bytes(payload)
        binding = {
            "identity": compact_identity(after),
            "mode": stat.S_IMODE(after.st_mode),
            "path": str(lexical),
            "sha256": digest,
            "size": after.st_size,
        }
        full_identity = {
            "bytes": after.st_size,
            "ctime_ns": after.st_ctime_ns,
            "device": after.st_dev,
            "inode": after.st_ino,
            "link_count": after.st_nlink,
            "mode": stat.S_IMODE(after.st_mode),
            "mtime_ns": after.st_mtime_ns,
            "path": str(lexical),
            "sha256": digest,
            "size": after.st_size,
        }
        os.lseek(descriptor, 0, os.SEEK_SET)
        return descriptor, payload, binding, full_identity
    except BaseException:
        os.close(descriptor)
        raise


def verify_retained_artifact(
    descriptor: int,
    path: Path,
    payload: bytes,
    binding: dict[str, Any],
    context: str,
) -> None:
    metadata = os.fstat(descriptor)
    if (
        compact_identity(metadata) != binding["identity"]
        or stat.S_IMODE(metadata.st_mode) != binding["mode"]
        or metadata.st_size != binding["size"]
    ):
        raise PreparationError(f"{context} descriptor identity changed")
    os.lseek(descriptor, 0, os.SEEK_SET)
    chunks: list[bytes] = []
    while True:
        chunk = os.read(descriptor, 1024 * 1024)
        if not chunk:
            break
        chunks.append(chunk)
    if b"".join(chunks) != payload or hash_bytes(payload) != binding["sha256"]:
        raise PreparationError(f"{context} descriptor bytes changed")
    current = path.lstat()
    if compact_identity(current) != binding["identity"]:
        raise PreparationError(f"{context} path identity changed")


def replay_artifact_binding(binding: dict[str, Any], context: str) -> None:
    path = Path(str(binding.get("path")))
    descriptor, payload, observed, _full = open_retained_artifact(path, context)
    try:
        if observed != binding:
            raise PreparationError(f"{context} live binding differs")
        verify_retained_artifact(descriptor, path, payload, binding, context)
    finally:
        os.close(descriptor)


def open_retained_directory(path: Path, context: str) -> tuple[int, dict[str, int]]:
    lexical = path.absolute()
    descriptor = os.open(
        lexical,
        os.O_RDONLY
        | os.O_CLOEXEC
        | os.O_DIRECTORY
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        metadata = os.fstat(descriptor)
        current = lexical.lstat()
        resolved = lexical.resolve(strict=True)
        identity = {
            "device": metadata.st_dev,
            "inode": metadata.st_ino,
            "link_count": metadata.st_nlink,
            "mode": stat.S_IMODE(metadata.st_mode),
        }
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(current.st_mode)
            or resolved != lexical
            or any(
                getattr(current, field) != getattr(metadata, field)
                for field in ("st_dev", "st_ino", "st_mode", "st_nlink")
            )
        ):
            raise PreparationError(f"{context} is not an exact retained directory")
        return descriptor, identity
    except BaseException:
        os.close(descriptor)
        raise


def verify_retained_directory(
    descriptor: int,
    path: Path,
    identity: dict[str, int],
    context: str,
    *,
    writable: bool = False,
) -> None:
    metadata = os.fstat(descriptor)
    current = path.lstat()
    observed = {
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "link_count": metadata.st_nlink,
        "mode": stat.S_IMODE(metadata.st_mode),
    }
    current_identity = {
        "device": current.st_dev,
        "inode": current.st_ino,
        "link_count": current.st_nlink,
        "mode": stat.S_IMODE(current.st_mode),
    }
    fields = ("device", "inode", "mode") if writable else tuple(identity)
    if any(
        observed[field] != identity[field]
        or current_identity[field] != identity[field]
        for field in fields
    ):
        raise PreparationError(f"{context} retained directory identity changed")


RECURSIVE_METADATA_FIELDS = (
    "st_dev",
    "st_ino",
    "st_mode",
    "st_nlink",
    "st_size",
    "st_mtime_ns",
    "st_ctime_ns",
)


def same_recursive_metadata(left: os.stat_result, right: os.stat_result) -> bool:
    return all(
        getattr(left, field) == getattr(right, field)
        for field in RECURSIVE_METADATA_FIELDS
    )


class RecursiveTreeAuthorityGuard:
    """Descriptor-bind and recursively watch one build-semantic input tree."""

    def __init__(
        self,
        path: Path,
        evidence_path: Path,
        role: str,
        context: str,
        *,
        allow_internal_symlinks: bool,
        hash_regular_contents: bool = True,
        trusted_system_roots: tuple[Path, ...] = (),
        excluded_relative_paths: tuple[str, ...] = (),
        allowed_mutation_paths: tuple[str, ...] = (),
        volatile_directory_metadata_paths: tuple[str, ...] = (),
    ) -> None:
        self.path = path
        self.evidence_path = evidence_path
        self.role = role
        self.context = context
        self.allow_internal_symlinks = allow_internal_symlinks
        self.hash_regular_contents = hash_regular_contents
        self.trusted_system_roots = trusted_system_roots
        self.excluded_relative_paths = frozenset(excluded_relative_paths)
        self.allowed_mutation_paths = frozenset(allowed_mutation_paths)
        self.volatile_directory_metadata_paths = frozenset(
            volatile_directory_metadata_paths
        )
        self.descriptor = -1
        self.identity: dict[str, int] | None = None
        self.inotify_descriptor = -1
        self.watch_descriptors: set[int] = set()
        self.watch_paths: dict[int, str] = {}
        self.evidence: tuple[int, bytes, dict[str, Any]] | None = None
        self.initial_manifest: dict[str, Any] | None = None
        self.binding: dict[str, Any] | None = None
        self.poisoned = False
        self.active = False

    def _initialize_inotify(self) -> None:
        library = ctypes.CDLL(None, use_errno=True)
        try:
            initialize = library.inotify_init1
            add_watch = library.inotify_add_watch
        except AttributeError as error:
            raise PreparationError(
                f"{self.context} inotify API is unavailable"
            ) from error
        initialize.argtypes = [ctypes.c_int]
        initialize.restype = ctypes.c_int
        add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        add_watch.restype = ctypes.c_int
        self.inotify_descriptor = initialize(os.O_CLOEXEC | os.O_NONBLOCK)
        if self.inotify_descriptor < 0:
            errno = ctypes.get_errno()
            raise PreparationError(
                f"{self.context} inotify initialization failed: {errno}"
            )
        self._inotify_add_watch = add_watch

    def _add_watch(self, descriptor: int, relative: str) -> None:
        watch = self._inotify_add_watch(
            self.inotify_descriptor,
            os.fsencode(f"/proc/self/fd/{descriptor}"),
            INOTIFY_MUTATION_MASK | 0x01000000,
        )
        if watch < 0:
            errno = ctypes.get_errno()
            raise PreparationError(f"{self.context} inotify watch failed: {errno}")
        self.watch_descriptors.add(watch)
        if watch in self.watch_paths and self.watch_paths[watch] != relative:
            raise PreparationError(f"{self.context} inotify watch aliases")
        self.watch_paths[watch] = relative

    def _drain(self, boundary: str) -> None:
        if self.poisoned or self.inotify_descriptor < 0:
            raise PreparationError(f"{self.context} mutation guard is poisoned")
        observed: list[tuple[int, int, int, int]] = []
        try:
            while True:
                try:
                    payload = os.read(self.inotify_descriptor, 1024 * 1024)
                except BlockingIOError:
                    break
                if not payload:
                    raise PreparationError(
                        f"{self.context} inotify queue closed at {boundary}"
                    )
                offset = 0
                while offset < len(payload):
                    if len(payload) - offset < INOTIFY_EVENT_HEADER.size:
                        raise PreparationError(
                            f"{self.context} malformed inotify header at {boundary}"
                        )
                    event = INOTIFY_EVENT_HEADER.unpack_from(payload, offset)
                    event_size = INOTIFY_EVENT_HEADER.size + event[3]
                    if event_size > len(payload) - offset:
                        raise PreparationError(
                            f"{self.context} malformed inotify event at {boundary}"
                        )
                    raw_name = payload[
                        offset + INOTIFY_EVENT_HEADER.size : offset + event_size
                    ].rstrip(b"\0")
                    try:
                        name = os.fsdecode(raw_name)
                    except UnicodeDecodeError:
                        name = "<invalid>"
                    parent = self.watch_paths.get(event[0], "<unknown>")
                    relative = (
                        parent
                        if not name
                        else name if parent == "." else f"{parent}/{name}"
                    )
                    always_fatal = event[1] & (
                        0x00002000 | 0x00004000 | 0x00008000
                    )
                    allowed = relative in self.allowed_mutation_paths or (
                        parent == "." and name.startswith(".Cargo.lock")
                    )
                    if always_fatal or not allowed:
                        observed.append(event)
                    offset += event_size
        except OSError as error:
            self.poisoned = True
            raise PreparationError(
                f"{self.context} inotify read failed at {boundary}"
            ) from error
        if observed:
            self.poisoned = True
            masks = sorted({event[1] for event in observed})
            raise PreparationError(
                f"{self.context} mutation events at {boundary}: {masks}"
            )

    def _trusted(self, metadata: os.stat_result, relative: str, *, symlink: bool) -> None:
        candidate = self.path if relative == "." else self.path / relative
        if self.trusted_system_roots and (
            metadata.st_uid != 0
            or (not symlink and stat.S_IMODE(metadata.st_mode) & 0o022)
            or (not symlink and os.access(candidate, os.W_OK))
        ):
            raise PreparationError(
                f"{self.context} trusted system entry is writable: {relative}"
            )

    def _record(
        self,
        metadata: os.stat_result,
        relative: str,
        kind: str,
        digest: str | None,
        target: str | None,
        symlink_scope: str | None = None,
    ) -> dict[str, Any]:
        record = {
            "changed_ns": metadata.st_ctime_ns,
            "device": metadata.st_dev,
            "file_type": kind,
            "gid": metadata.st_gid,
            "inode": metadata.st_ino,
            "link_count": metadata.st_nlink,
            "modified_ns": metadata.st_mtime_ns,
            "path": relative,
            "permissions": stat.S_IMODE(metadata.st_mode),
            "sha256": digest,
            "size": metadata.st_size,
            "symlink_target": target,
            "symlink_scope": symlink_scope,
            "uid": metadata.st_uid,
        }
        if kind == "directory" and relative in self.volatile_directory_metadata_paths:
            for field in ("changed_ns", "modified_ns", "permissions", "size"):
                record[field] = 0
        return record

    def _manifest(self, *, install_watches: bool) -> dict[str, Any]:
        if self.descriptor < 0:
            raise PreparationError(f"{self.context} root descriptor is absent")
        directory_flags = (
            os.O_RDONLY
            | os.O_DIRECTORY
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0)
        )
        root_descriptor = os.open(".", directory_flags, dir_fd=self.descriptor)
        entries: list[dict[str, Any]] = []

        def walk(descriptor: int, relative: str) -> None:
            before = os.fstat(descriptor)
            self._trusted(before, relative, symlink=False)
            if install_watches:
                self._add_watch(descriptor, relative)
            entries.append(self._record(before, relative, "directory", None, None))
            names = sorted(os.listdir(descriptor))
            if len(names) != len(set(names)):
                raise PreparationError(f"{self.context} names alias")
            for name in names:
                child_relative = name if relative == "." else f"{relative}/{name}"
                if child_relative in self.excluded_relative_paths:
                    continue
                selected = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
                if stat.S_ISDIR(selected.st_mode):
                    child = os.open(name, directory_flags, dir_fd=descriptor)
                    try:
                        if not same_recursive_metadata(selected, os.fstat(child)):
                            raise PreparationError(
                                f"{self.context} directory selection changed"
                            )
                        walk(child, child_relative)
                    finally:
                        os.close(child)
                elif stat.S_ISREG(selected.st_mode):
                    file_flags = (
                        os.O_RDONLY
                        if self.hash_regular_contents
                        else getattr(os, "O_PATH", 0)
                    )
                    if not self.hash_regular_contents and file_flags == 0:
                        raise PreparationError(
                            f"{self.context} metadata-only file descriptors are unavailable"
                        )
                    child = os.open(
                        name,
                        file_flags
                        | os.O_CLOEXEC
                        | getattr(os, "O_NOFOLLOW", 0),
                        dir_fd=descriptor,
                    )
                    try:
                        opened_before = os.fstat(child)
                        if not same_recursive_metadata(selected, opened_before):
                            raise PreparationError(
                                f"{self.context} file selection changed"
                            )
                        digest = None
                        if self.hash_regular_contents:
                            hasher = hashlib.sha256()
                            offset = 0
                            while offset < opened_before.st_size:
                                chunk = os.pread(
                                    child,
                                    min(
                                        1024 * 1024,
                                        opened_before.st_size - offset,
                                    ),
                                    offset,
                                )
                                if not chunk:
                                    raise PreparationError(
                                        f"{self.context} file read made no progress"
                                    )
                                hasher.update(chunk)
                                offset += len(chunk)
                            digest = hasher.hexdigest()
                        opened_after = os.fstat(child)
                        selected_after = os.stat(
                            name, dir_fd=descriptor, follow_symlinks=False
                        )
                        if (
                            not same_recursive_metadata(
                                opened_before, opened_after
                            )
                            or not same_recursive_metadata(
                                opened_after, selected_after
                            )
                        ):
                            raise PreparationError(
                                f"{self.context} file changed while snapshotting"
                            )
                    finally:
                        os.close(child)
                    self._trusted(selected, child_relative, symlink=False)
                    entries.append(
                        self._record(
                            selected, child_relative, "regular", digest, None
                        )
                    )
                elif stat.S_ISLNK(selected.st_mode):
                    if not self.allow_internal_symlinks:
                        raise PreparationError(
                            f"{self.context} contains a symlink: {child_relative}"
                        )
                    target = os.readlink(name, dir_fd=descriptor)
                    selected_after = os.stat(
                        name, dir_fd=descriptor, follow_symlinks=False
                    )
                    if not same_recursive_metadata(selected, selected_after):
                        raise PreparationError(
                            f"{self.context} symlink changed while snapshotting"
                        )
                    try:
                        scope = recursive_symlink_scope(
                            self.path,
                            child_relative,
                            target,
                            trusted_system=bool(self.trusted_system_roots),
                        )
                    except PreparationError as error:
                        raise PreparationError(
                            f"{self.context} {error}"
                        ) from error
                    self._trusted(selected, child_relative, symlink=True)
                    entries.append(
                        self._record(
                            selected,
                            child_relative,
                            "symlink",
                            hash_bytes(os.fsencode(target)),
                            target,
                            scope,
                        )
                    )
                else:
                    raise PreparationError(
                        f"{self.context} contains unsupported node: {child_relative}"
                    )
            after = os.fstat(descriptor)
            if not same_recursive_metadata(before, after):
                raise PreparationError(
                    f"{self.context} directory changed during traversal"
                )

        try:
            walk(root_descriptor, ".")
        finally:
            os.close(root_descriptor)
        hardlinks: dict[tuple[int, int], list[dict[str, Any]]] = {}
        for entry in entries:
            if entry["file_type"] == "regular":
                hardlinks.setdefault((entry["device"], entry["inode"]), []).append(
                    entry
                )
        for aliases in hardlinks.values():
            if (
                not self.trusted_system_roots
                and len(aliases) != aliases[0]["link_count"]
            ):
                raise PreparationError(
                    f"{self.context} hard link escapes the retained tree"
                )
        if install_watches:
            expected = sum(entry["file_type"] == "directory" for entry in entries)
            if len(self.watch_descriptors) != expected:
                raise PreparationError(
                    f"{self.context} recursive watch coverage differs"
                )
        entries.sort(key=lambda entry: (entry["path"] != ".", entry["path"]))
        return {
            "entries": entries,
            "role": self.role,
            "schema": RECURSIVE_TREE_AUTHORITY_SCHEMA,
        }

    def __enter__(self) -> RecursiveTreeAuthorityGuard:
        try:
            self.descriptor, self.identity = open_retained_directory(
                self.path, f"{self.context} root"
            )
            self._initialize_inotify()
            first = self._manifest(install_watches=True)
            self._drain("initial manifest")
            second = self._manifest(install_watches=False)
            self._drain("matching manifest")
            if first != second:
                raise PreparationError(f"{self.context} initial manifests differ")
            payload = canonical_json(second)
            if self.evidence_path.exists() or self.evidence_path.is_symlink():
                raise PreparationError(f"{self.context} evidence path is not fresh")
            atomic_write(self.evidence_path, payload, mode=0o444)
            evidence_descriptor, evidence_payload, evidence_binding, _full = (
                open_retained_artifact(
                    self.evidence_path, f"{self.context} manifest evidence"
                )
            )
            if evidence_payload != payload:
                os.close(evidence_descriptor)
                raise PreparationError(f"{self.context} evidence bytes differ")
            self.evidence = (
                evidence_descriptor,
                evidence_payload,
                evidence_binding,
            )
            self.initial_manifest = second
            self.binding = {
                "entry_count": len(second["entries"]),
                "equal_pre_post": False,
                "manifest_path": str(self.evidence_path.resolve()),
                "manifest_sha256": hash_bytes(payload),
                "mutation_events_absent": True,
                "role": self.role,
                "schema": RECURSIVE_TREE_AUTHORITY_SCHEMA,
                "watch_count": len(self.watch_descriptors),
            }
            self.active = True
        except Exception:
            self.close()
            raise
        return self

    @property
    def pass_fds(self) -> tuple[int, ...]:
        if not self.active or self.descriptor < 0:
            raise PreparationError(f"{self.context} recursive guard is inactive")
        return (self.descriptor,)

    def bwrap_bind(self, guest_path: str) -> tuple[str, str, str]:
        return "--ro-bind-fd", str(self.pass_fds[0]), guest_path

    def replay(self, boundary: str) -> dict[str, Any]:
        if (
            not self.active
            or self.initial_manifest is None
            or self.binding is None
            or self.identity is None
            or self.evidence is None
        ):
            raise PreparationError(f"{self.context} recursive guard is inactive")
        self._drain(f"{boundary} before replay")
        if self._manifest(install_watches=False) != self.initial_manifest:
            self.poisoned = True
            raise PreparationError(f"{self.context} recursive manifest changed")
        self._drain(f"{boundary} after replay")
        verify_retained_directory(
            self.descriptor, self.path, self.identity, f"{self.context} root"
        )
        verify_retained_artifact(
            self.evidence[0],
            self.evidence_path,
            self.evidence[1],
            self.evidence[2],
            f"{self.context} manifest evidence",
        )
        self.binding["equal_pre_post"] = True
        return dict(self.binding)

    def close(self) -> None:
        if self.evidence is not None:
            os.close(self.evidence[0])
            self.evidence = None
        if self.inotify_descriptor >= 0:
            os.close(self.inotify_descriptor)
            self.inotify_descriptor = -1
        if self.descriptor >= 0:
            os.close(self.descriptor)
            self.descriptor = -1
        self.active = False

    def __exit__(self, child_type: Any, child_error: Any, traceback: Any) -> bool:
        verification_error = None
        try:
            if self.active:
                self.replay("guard close")
        except PreparationError as error:
            verification_error = error
        finally:
            self.close()
        if verification_error is not None:
            raise verification_error
        return False


class TrustedSystemClosureGuard:
    """Retain the narrow, recursively immutable system build closure."""

    def __init__(self, evidence_root: Path, label: str) -> None:
        self.evidence_root = evidence_root
        self.label = label
        self.guards: list[tuple[str, RecursiveTreeAuthorityGuard]] = []
        self.evidence_path = evidence_root / f"{label}-system-closure.json"
        self.evidence: tuple[int, bytes, dict[str, Any]] | None = None
        self.binding: dict[str, Any] | None = None

    def __enter__(self) -> TrustedSystemClosureGuard:
        roots = tuple(path.resolve(strict=True) for path, _ in TRUSTED_SYSTEM_MOUNTS)
        if len(set(roots)) != len(roots):
            raise PreparationError("trusted system closure roots alias")
        manifests = []
        try:
            for (host_path, guest_path), root in zip(
                TRUSTED_SYSTEM_MOUNTS, roots, strict=True
            ):
                if host_path != root or not root.is_dir():
                    raise PreparationError(
                        f"trusted system mount is aliased: {host_path}"
                    )
                role = "system-" + guest_path.removeprefix("/").replace("/", "-")
                guard = RecursiveTreeAuthorityGuard(
                    root,
                    self.evidence_root / f"{self.label}-{role}.json",
                    role,
                    f"{self.label} trusted {guest_path}",
                    allow_internal_symlinks=True,
                    hash_regular_contents=False,
                    trusted_system_roots=roots,
                )
                guard.__enter__()
                self.guards.append((guest_path, guard))
                assert guard.initial_manifest is not None
                manifests.append(
                    {
                        "guest_path": guest_path,
                        "host_path": str(host_path),
                        "resolved_path": str(root),
                        "tree": guard.initial_manifest,
                    }
                )
            payload = canonical_json(
                {"mounts": manifests, "schema": TRUSTED_SYSTEM_CLOSURE_SCHEMA}
            )
            if self.evidence_path.exists() or self.evidence_path.is_symlink():
                raise PreparationError("trusted system closure evidence is not fresh")
            atomic_write(self.evidence_path, payload, mode=0o444)
            descriptor, observed_payload, observed_binding, _full = (
                open_retained_artifact(
                    self.evidence_path, "trusted system closure evidence"
                )
            )
            if observed_payload != payload:
                os.close(descriptor)
                raise PreparationError("trusted system closure evidence differs")
            self.evidence = (descriptor, observed_payload, observed_binding)
            self.binding = {
                "entry_count": sum(
                    len(item["tree"]["entries"]) for item in manifests
                ),
                "manifest_path": str(self.evidence_path.resolve()),
                "mounts": [
                    {
                        "device": item["tree"]["entries"][0]["device"],
                        "gid": item["tree"]["entries"][0]["gid"],
                        "guest_path": item["guest_path"],
                        "host_path": item["host_path"],
                        "inode": item["tree"]["entries"][0]["inode"],
                        "permissions": item["tree"]["entries"][0]["permissions"],
                        "resolved_path": item["resolved_path"],
                        "trusted_root_owned_non_writable": True,
                        "uid": item["tree"]["entries"][0]["uid"],
                    }
                    for item in manifests
                ],
                "mutation_events_absent": True,
                "schema": TRUSTED_SYSTEM_CLOSURE_SCHEMA,
                "sha256": hash_bytes(payload),
                "watch_count": sum(
                    len(guard.watch_descriptors) for _, guard in self.guards
                ),
            }
        except Exception:
            self.close()
            raise
        return self

    @property
    def pass_fds(self) -> tuple[int, ...]:
        descriptors = tuple(
            descriptor
            for _, guard in self.guards
            for descriptor in guard.pass_fds
        )
        if len(descriptors) != len(set(descriptors)):
            raise PreparationError("trusted system closure descriptors alias")
        return descriptors

    def bwrap_args(self) -> list[str]:
        arguments = ["--dir", "/usr"]
        for guest_path, guard in self.guards:
            arguments.extend(guard.bwrap_bind(guest_path))
        arguments.extend(
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
            ]
        )
        return arguments

    def replay(self, boundary: str) -> dict[str, Any]:
        for _, guard in self.guards:
            guard.replay(boundary)
        if self.binding is None or self.evidence is None:
            raise PreparationError("trusted system closure binding is absent")
        verify_retained_artifact(
            self.evidence[0],
            self.evidence_path,
            self.evidence[1],
            self.evidence[2],
            "trusted system closure evidence",
        )
        return dict(self.binding)

    def close(self) -> None:
        if self.evidence is not None:
            os.close(self.evidence[0])
            self.evidence = None
        while self.guards:
            _guest, guard = self.guards.pop()
            guard.__exit__(None, None, None)

    def __exit__(self, child_type: Any, child_error: Any, traceback: Any) -> bool:
        self.close()
        return False


def resolution_lock_binding(path: Path, context: str) -> dict[str, Any]:
    try:
        descriptor, payload, binding, _full = open_retained_artifact(path, context)
    except FileNotFoundError:
        return {"path": str(path), "sha256": None, "status": "absent"}
    try:
        verify_retained_artifact(descriptor, path, payload, binding, context)
        return {"path": str(path), "sha256": binding["sha256"], "status": "present"}
    finally:
        os.close(descriptor)


def execute_sandboxed_resolution(
    *,
    root: Path,
    toolchain: dict[str, str],
    environment: dict[str, str],
    cargo_config_search: dict[str, str],
    cargo_arguments: list[str],
    evidence_root: Path,
    label: str,
) -> dict[str, Any]:
    """Run one offline lock operation through retained, recursively guarded inputs."""

    cargo_path = Path(toolchain["cargo_path"])
    rustc_path = Path(toolchain["rustc_path"])
    toolchain_root = cargo_path.parent.parent
    if rustc_path.parent.parent != toolchain_root:
        raise PreparationError(f"{label} Cargo/rustc roots differ")
    replay_sandboxed_cargo_config_search(cargo_config_search, root, toolchain)
    config_snapshot = immutable_canonical_snapshot(
        Path(cargo_config_search["path"]),
        CARGO_CONFIG_SCHEMA,
        f"{label} Cargo config manifest",
    )
    config_entries = {entry["path"]: entry for entry in config_snapshot.value["entries"]}
    host_config_paths = sandboxed_config_host_paths(root, toolchain)
    empty_config = sandboxed_empty_cargo_config_path(config_snapshot.path)
    artifact_specs = {
        "bwrap": (Path(toolchain["bwrap_path"]), toolchain["bwrap_sha256"]),
        "cargo": (cargo_path, toolchain["cargo_sha256"]),
        "rustc": (rustc_path, toolchain["rustc_sha256"]),
    }
    for guest_path in GUEST_BOUND_CONFIG_PATHS:
        entry = config_entries.get(guest_path)
        if (
            not isinstance(entry, dict)
            or entry.get("status") != "present"
            or not is_lower_hex(entry.get("sha256"), SHA256)
        ):
            raise PreparationError(f"{label} Cargo config binding differs")
        host_path = host_config_paths[guest_path]
        artifact_specs[f"config:{guest_path}"] = (
            host_path if host_path.exists() else empty_config,
            entry["sha256"],
        )
    leases: dict[str, tuple[int, bytes, dict[str, Any]]] = {}
    authority_stack = ExitStack()
    lock_path = root / "Cargo.lock"
    lock_before = resolution_lock_binding(lock_path, f"{label} lock before")
    try:
        for name, (path, expected_sha256) in artifact_specs.items():
            descriptor, payload, binding, _full = open_retained_artifact(
                path, f"{label} retained {name}"
            )
            if binding["sha256"] != expected_sha256:
                os.close(descriptor)
                raise PreparationError(f"{label} retained {name} hash differs")
            leases[name] = (descriptor, payload, binding)
        source_guard = authority_stack.enter_context(
            RecursiveTreeAuthorityGuard(
                root,
                evidence_root / f"resolution-source-{label}.json",
                "resolution_source_without_cargo_lock",
                f"{label} resolution source",
                allow_internal_symlinks=False,
                excluded_relative_paths=("Cargo.lock",),
                allowed_mutation_paths=("Cargo.lock",),
                volatile_directory_metadata_paths=(".",),
            )
        )
        toolchain_guard = authority_stack.enter_context(
            RecursiveTreeAuthorityGuard(
                toolchain_root,
                evidence_root / f"resolution-toolchain-{label}.json",
                "toolchain",
                f"{label} resolution toolchain",
                allow_internal_symlinks=True,
            )
        )
        cargo_home_guard = authority_stack.enter_context(
            RecursiveTreeAuthorityGuard(
                Path(toolchain["cargo_home_path"]),
                evidence_root / f"resolution-cargo-home-{label}.json",
                "cargo_home",
                f"{label} resolution Cargo home",
                allow_internal_symlinks=True,
            )
        )
        system_guard = authority_stack.enter_context(
            TrustedSystemClosureGuard(evidence_root, f"resolution-{label}")
        )
        descriptors = {
            "cargo": leases["cargo"][0],
            "cargo_home": cargo_home_guard.pass_fds[0],
            "rustc": leases["rustc"][0],
            "source": source_guard.pass_fds[0],
            "toolchain_root": toolchain_guard.pass_fds[0],
        }
        system_descriptors = {
            guest_path: guard.pass_fds[0]
            for guest_path, guard in system_guard.guards
        }
        config_descriptors = {
            guest_path: leases[f"config:{guest_path}"][0]
            for guest_path in GUEST_BOUND_CONFIG_PATHS
        }
        argv = sandboxed_resolution_argv(
            descriptors,
            system_descriptors,
            config_descriptors,
            cargo_arguments,
            str(Path(toolchain["bwrap_path"])),
        )
        inherited_fds = tuple(lease[0] for lease in leases.values()) + (
            source_guard.pass_fds
            + toolchain_guard.pass_fds
            + cargo_home_guard.pass_fds
            + system_guard.pass_fds
        )
        if len(inherited_fds) != len(set(inherited_fds)):
            raise PreparationError(f"{label} inherited descriptors alias")
        source_guard.replay("pre-resolution launch")
        toolchain_guard.replay("pre-resolution launch")
        cargo_home_guard.replay("pre-resolution launch")
        system_guard.replay("pre-resolution launch")
        for name, (descriptor, payload, binding) in leases.items():
            verify_retained_artifact(
                descriptor,
                artifact_specs[name][0],
                payload,
                binding,
                f"{label} retained {name} before launch",
            )
        bwrap_descriptor = leases["bwrap"][0]
        try:
            completed = subprocess.run(
                argv,
                executable=f"/proc/self/fd/{bwrap_descriptor}",
                pass_fds=inherited_fds,
                cwd=root,
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=300,
                check=False,
            )
        finally:
            source_guard.replay("post-resolution boundary")
            toolchain_guard.replay("post-resolution boundary")
            cargo_home_guard.replay("post-resolution boundary")
            system_guard.replay("post-resolution boundary")
        replay_sandboxed_cargo_config_search(cargo_config_search, root, toolchain)
        lock_after = resolution_lock_binding(lock_path, f"{label} lock after")
        runtime_components = {
            "cargo_home": cargo_home_guard.binding,
            "toolchain": toolchain_guard.binding,
            "trusted_system_closure": system_guard.binding,
        }
        if any(value is None for value in runtime_components.values()):
            raise PreparationError(f"{label} runtime authority is absent")
        source_binding = dict(source_guard.binding or {})
        authority = {
            **runtime_components,
            "runtime_sha256": semantic_runtime_sha256(runtime_components),
            "schema": SEMANTIC_INPUT_AUTHORITY_SCHEMA,
            "source": source_binding,
        }
        return {
            "argv": argv,
            "cargo_config_search": cargo_config_search,
            "cwd": GUEST_SOURCE,
            "environment": environment,
            "exit_status": completed.returncode,
            "execution_authority": leases["bwrap"][2],
            "host_source_root": str(root.resolve()),
            "lock_output": {"post": lock_after, "pre": lock_before},
            "passed_file_descriptors": len(inherited_fds),
            "resolver_kind": "sandboxed_cargo_resolution",
            "semantic_input_authority": authority,
            "stderr": completed.stderr.decode("utf-8"),
            "stderr_sha256": hash_bytes(completed.stderr),
            "stdout": completed.stdout.decode("utf-8"),
            "stdout_sha256": hash_bytes(completed.stdout),
            "toolchain": toolchain,
        }
    finally:
        verification_error: BaseException | None = None
        for name, (descriptor, payload, binding) in leases.items():
            try:
                verify_retained_artifact(
                    descriptor,
                    artifact_specs[name][0],
                    payload,
                    binding,
                    f"{label} retained {name}",
                )
            except BaseException as error:
                if verification_error is None:
                    verification_error = error
            finally:
                os.close(descriptor)
        try:
            authority_stack.close()
        except BaseException as error:
            if verification_error is None:
                verification_error = error
        if verification_error is not None:
            raise verification_error


def execute_sandboxed_build(
    *,
    root: Path,
    target: Path,
    package: str,
    example: str,
    toolchain: dict[str, str],
    environment: dict[str, str],
    cargo_config_search: dict[str, str],
    output_path: Path,
    context: str,
) -> tuple[
    list[str],
    dict[str, Any],
    bytes,
    bytes,
    dict[str, Any],
    dict[str, Any],
]:
    """Execute one build through retained tools and fixed guest bindings."""

    artifact_specs = {
        "bwrap": (Path(toolchain["bwrap_path"]), toolchain["bwrap_sha256"]),
        "cargo": (Path(toolchain["cargo_path"]), toolchain["cargo_sha256"]),
        "rustc": (Path(toolchain["rustc_path"]), toolchain["rustc_sha256"]),
    }
    cargo = artifact_specs["cargo"][0]
    rustc = artifact_specs["rustc"][0]
    toolchain_root = cargo.parent.parent
    if rustc.parent.parent != toolchain_root:
        raise PreparationError(f"{context} Cargo/rustc toolchain roots differ")
    rustc_host = toolchain.get("rustc_host")
    if (
        not isinstance(rustc_host, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", rustc_host) is None
    ):
        raise PreparationError(f"{context} rustc host differs")
    rust_lld_path = Path(toolchain["rust_lld_path"])
    expected_rust_lld_path = (
        toolchain_root / "lib" / "rustlib" / rustc_host / "bin" / "rust-lld"
    )
    if rust_lld_path != expected_rust_lld_path:
        raise PreparationError(f"{context} rust-lld path differs")
    rust_lld_guest_path = (
        f"{GUEST_TOOLCHAIN_ROOT}/lib/rustlib/{rustc_host}/bin/gcc-ld/ld.lld"
    )
    artifact_specs["rust_lld"] = (rust_lld_path, toolchain["rust_lld_sha256"])
    rust_lld_relative = rust_lld_path.relative_to(toolchain_root).as_posix()
    directory_specs = {
        "target": target,
    }
    replay_sandboxed_cargo_config_search(
        cargo_config_search, root, toolchain
    )
    config_manifest_snapshot = immutable_canonical_snapshot(
        Path(cargo_config_search["path"]),
        CARGO_CONFIG_SCHEMA,
        f"{context} Cargo config manifest",
    )
    if config_manifest_snapshot.sha256 != cargo_config_search["sha256"]:
        raise PreparationError(f"{context} Cargo config manifest hash differs")
    config_manifest_path = config_manifest_snapshot.path
    config_manifest = config_manifest_snapshot.value
    config_entries = {
        entry["path"]: entry for entry in config_manifest["entries"]
    }
    host_config_paths = sandboxed_config_host_paths(root, toolchain)
    empty_config_path = sandboxed_empty_cargo_config_path(config_manifest_path)
    artifact_specs["cargo_config_manifest"] = (
        config_manifest_path,
        cargo_config_search["sha256"],
    )
    artifact_specs["cargo_config_empty"] = (
        empty_config_path,
        EMPTY_SHA256,
    )
    for guest_path in GUEST_BOUND_CONFIG_PATHS:
        entry = config_entries.get(guest_path)
        if (
            not isinstance(entry, dict)
            or entry.get("status") != "present"
            or not is_lower_hex(entry.get("sha256"), SHA256)
        ):
            raise PreparationError(
                f"{context} effective Cargo config binding differs: {guest_path}"
            )
        host_path = host_config_paths[guest_path]
        source_path = host_path if host_path.exists() else empty_config_path
        artifact_specs[f"config:{guest_path}"] = (
            source_path,
            entry["sha256"],
        )
    artifact_leases: dict[str, tuple[int, bytes, dict[str, Any]]] = {}
    directory_leases: dict[str, tuple[int, dict[str, int]]] = {}
    dev_null_descriptor = -1
    dev_null_binding: dict[str, Any] | None = None
    authority_stack = ExitStack()
    try:
        for name, (path, expected_sha256) in artifact_specs.items():
            descriptor, payload, binding, _full = open_retained_artifact(
                path, f"{context} retained {name}"
            )
            if binding["sha256"] != expected_sha256:
                os.close(descriptor)
                raise PreparationError(f"{context} retained {name} hash differs")
            artifact_leases[name] = (descriptor, payload, binding)
        for name, path in directory_specs.items():
            directory_leases[name] = open_retained_directory(
                path, f"{context} retained {name}"
            )
        dev_null_descriptor, dev_null_binding = open_retained_null_device(
            HOST_DEV_NULL, f"{context} retained null device"
        )
        evidence_root = output_path.parent.parent / "manifests"
        evidence_label = output_path.stem
        source_guard = authority_stack.enter_context(
            RecursiveTreeAuthorityGuard(
                root,
                evidence_root / f"semantic-source-{evidence_label}.json",
                "source",
                f"{context} semantic source tree",
                allow_internal_symlinks=False,
            )
        )
        toolchain_guard = authority_stack.enter_context(
            RecursiveTreeAuthorityGuard(
                toolchain_root,
                evidence_root / f"semantic-toolchain-{evidence_label}.json",
                "toolchain",
                f"{context} semantic toolchain tree",
                allow_internal_symlinks=True,
            )
        )
        if toolchain_guard.initial_manifest is None:
            raise PreparationError(f"{context} toolchain authority is absent")
        rust_lld_binding = artifact_leases["rust_lld"][2]
        rust_lld_entries = [
            entry
            for entry in toolchain_guard.initial_manifest["entries"]
            if entry.get("path") == rust_lld_relative
        ]
        rust_lld_metadata = os.fstat(artifact_leases["rust_lld"][0])
        expected_rust_lld_entry = {
            "changed_ns": rust_lld_metadata.st_ctime_ns,
            "device": rust_lld_metadata.st_dev,
            "file_type": "regular",
            "gid": rust_lld_metadata.st_gid,
            "inode": rust_lld_metadata.st_ino,
            "link_count": rust_lld_metadata.st_nlink,
            "modified_ns": rust_lld_metadata.st_mtime_ns,
            "path": rust_lld_relative,
            "permissions": stat.S_IMODE(rust_lld_metadata.st_mode),
            "sha256": rust_lld_binding["sha256"],
            "size": rust_lld_metadata.st_size,
            "symlink_target": None,
            "symlink_scope": None,
            "uid": rust_lld_metadata.st_uid,
        }
        if rust_lld_entries != [expected_rust_lld_entry]:
            raise PreparationError(
                f"{context} rust-lld differs from toolchain authority"
            )
        cargo_home_guard = authority_stack.enter_context(
            RecursiveTreeAuthorityGuard(
                Path(toolchain["cargo_home_path"]),
                evidence_root / f"semantic-cargo-home-{evidence_label}.json",
                "cargo_home",
                f"{context} semantic Cargo home",
                allow_internal_symlinks=True,
            )
        )
        system_guard = authority_stack.enter_context(
            TrustedSystemClosureGuard(evidence_root, evidence_label)
        )
        descriptors = {
            name: lease[0] for name, lease in directory_leases.items()
        }
        descriptors.update(
            {
                "cargo": artifact_leases["cargo"][0],
                "cargo_home": cargo_home_guard.pass_fds[0],
                "dev_null": dev_null_descriptor,
                "rustc": artifact_leases["rustc"][0],
                "rust_lld": artifact_leases["rust_lld"][0],
                "source": source_guard.pass_fds[0],
                "toolchain_root": toolchain_guard.pass_fds[0],
            }
        )
        system_descriptors = {
            guest_path: guard.pass_fds[0]
            for guest_path, guard in system_guard.guards
        }
        config_descriptors = {
            guest_path: artifact_leases[f"config:{guest_path}"][0]
            for guest_path in GUEST_BOUND_CONFIG_PATHS
        }
        argv = sandboxed_build_argv(
            descriptors,
            system_descriptors,
            config_descriptors,
            package,
            example,
            str(artifact_specs["bwrap"][0]),
            rust_lld_guest_path,
        )
        if argv[0] != artifact_leases["bwrap"][2]["path"]:
            raise PreparationError(f"{context} bwrap argv/lease path differs")
        verify_retained_artifact(
            artifact_leases["rust_lld"][0],
            rust_lld_path,
            artifact_leases["rust_lld"][1],
            rust_lld_binding,
            f"{context} prelaunch rust-lld",
        )
        verify_retained_null_device(
            dev_null_descriptor,
            HOST_DEV_NULL,
            dev_null_binding,
            f"{context} prelaunch null device",
        )
        inherited_artifacts = (
            "bwrap",
            "cargo",
            "rustc",
            "rust_lld",
            *(f"config:{guest_path}" for guest_path in GUEST_BOUND_CONFIG_PATHS),
        )
        inherited_fds = tuple(
            artifact_leases[name][0] for name in inherited_artifacts
        ) + tuple(lease[0] for lease in directory_leases.values()) + (
            (dev_null_descriptor,)
            + source_guard.pass_fds
            + toolchain_guard.pass_fds
            + cargo_home_guard.pass_fds
            + system_guard.pass_fds
        )
        if len(set(inherited_fds)) != len(inherited_fds):
            raise PreparationError(f"{context} inherited descriptors alias")
        replay_sandboxed_cargo_config_search(
            cargo_config_search, root, toolchain
        )
        source_guard.replay("pre-child launch")
        toolchain_guard.replay("pre-child launch")
        cargo_home_guard.replay("pre-child launch")
        system_guard.replay("pre-child launch")
        execution_path = f"/proc/self/fd/{artifact_leases['bwrap'][0]}"
        try:
            child, stdout, stderr = run_attested(
                argv,
                cwd=root,
                env=environment,
                output_path=output_path,
                raw_stdout=False,
                inherited_fds=inherited_fds,
                executable=execution_path,
                attest_passed_file_descriptors=True,
            )
        finally:
            source_guard.replay("post-child boundary")
            toolchain_guard.replay("post-child boundary")
            cargo_home_guard.replay("post-child boundary")
            system_guard.replay("post-child boundary")
        replay_sandboxed_cargo_config_search(
            cargo_config_search, root, toolchain
        )
        assert source_guard.binding is not None
        assert toolchain_guard.binding is not None
        assert cargo_home_guard.binding is not None
        assert system_guard.binding is not None
        runtime_components = {
            "cargo_home": cargo_home_guard.binding,
            "toolchain": toolchain_guard.binding,
            "trusted_system_closure": system_guard.binding,
        }
        semantic_input_authority = {
            **runtime_components,
            "runtime_sha256": semantic_runtime_sha256(runtime_components),
            "schema": SEMANTIC_INPUT_AUTHORITY_SCHEMA,
            "source": source_guard.binding,
        }
        execution_tools = {
            "bwrap": artifact_leases["bwrap"][2],
            "cargo": artifact_leases["cargo"][2],
            "dev_null": dev_null_binding,
            "rustc": artifact_leases["rustc"][2],
            "rust_lld": artifact_leases["rust_lld"][2],
            "toolchain_root": dict(toolchain_guard.identity or {}),
        }
        return (
            argv,
            child,
            stdout,
            stderr,
            semantic_input_authority,
            execution_tools,
        )
    finally:
        verification_error: BaseException | None = None
        for name, (descriptor, payload, binding) in artifact_leases.items():
            try:
                verify_retained_artifact(
                    descriptor,
                    artifact_specs[name][0],
                    payload,
                    binding,
                    f"{context} retained {name}",
                )
            except BaseException as error:
                if verification_error is None:
                    verification_error = error
            finally:
                os.close(descriptor)
        for name, (descriptor, identity) in directory_leases.items():
            try:
                verify_retained_directory(
                    descriptor,
                    directory_specs[name],
                    identity,
                    f"{context} retained {name}",
                    writable=name == "target",
                )
            except BaseException as error:
                if verification_error is None:
                    verification_error = error
            finally:
                os.close(descriptor)
        if dev_null_descriptor >= 0 and dev_null_binding is not None:
            try:
                verify_retained_null_device(
                    dev_null_descriptor,
                    HOST_DEV_NULL,
                    dev_null_binding,
                    f"{context} retained null device",
                )
            except BaseException as error:
                if verification_error is None:
                    verification_error = error
            finally:
                os.close(dev_null_descriptor)
        try:
            authority_stack.close()
        except BaseException as error:
            if verification_error is None:
                verification_error = error
        if verification_error is not None:
            raise verification_error


def apply_product_test_overlay(
    root: Path, toolchain: dict[str, str], overlay_payload: bytes
) -> None:
    if hash_bytes(overlay_payload) != CURRENT_PRODUCT_OVERLAY_SHA256:
        raise PreparationError("product test overlay changed before proof build")
    environment = frozen_cargo_environment(toolchain)
    git_path = Path(toolchain["git_path"])
    descriptor, payload, binding, _full = open_retained_artifact(
        git_path, "product overlay retained Git"
    )
    try:
        if binding["sha256"] != toolchain["git_sha256"]:
            raise PreparationError("product overlay retained Git hash differs")
        argv = [
            str(git_path),
            "-C",
            str(root),
            "apply",
            "--whitespace=nowarn",
            "-",
        ]
        execution_path = f"/proc/self/fd/{descriptor}"
        check = subprocess.run(
            [*argv[:4], "--check", *argv[4:]],
            executable=execution_path,
            pass_fds=(descriptor,),
            env=environment,
            capture_output=True,
            input=overlay_payload,
            timeout=120,
        )
        if check.returncode != 0 or check.stdout or check.stderr:
            raise PreparationError("product test overlay failed exact Git apply check")
        applied = subprocess.run(
            argv,
            executable=execution_path,
            pass_fds=(descriptor,),
            env=environment,
            capture_output=True,
            input=overlay_payload,
            timeout=120,
        )
        if applied.returncode != 0 or applied.stdout or applied.stderr:
            raise PreparationError("product test overlay application failed")
    finally:
        try:
            verify_retained_artifact(
                descriptor,
                git_path,
                payload,
                binding,
                "product overlay retained Git",
            )
        finally:
            os.close(descriptor)


def validated_execution_tools_sha256(
    execution_tools: Any,
    toolchain: Mapping[str, Any],
    *,
    live: bool,
) -> str:
    expected_names = {
        "bwrap", "cargo", "dev_null", "rustc", "rust_lld", "toolchain_root",
    }
    if not isinstance(execution_tools, dict) or set(execution_tools) != expected_names:
        raise PreparationError("release build execution tools differ")

    def json_integer(value: Any) -> bool:
        return isinstance(value, int) and not isinstance(value, bool)

    rustc_host = toolchain.get("rustc_host")
    if (
        not isinstance(rustc_host, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", rustc_host) is None
    ):
        raise PreparationError("release execution-tool rustc host differs")
    toolchain_root = Path(str(toolchain.get("cargo_path"))).parent.parent
    expected_rust_lld = (
        toolchain_root / "lib" / "rustlib" / rustc_host / "bin" / "rust-lld"
    )
    if Path(str(toolchain.get("rust_lld_path"))) != expected_rust_lld:
        raise PreparationError("release execution-tool rust-lld topology differs")
    for name in ("bwrap", "cargo", "rustc", "rust_lld"):
        binding = execution_tools[name]
        if (
            not isinstance(binding, dict)
            or set(binding) != {"identity", "mode", "path", "sha256", "size"}
            or binding["path"] != toolchain.get(f"{name}_path")
            or binding["sha256"] != toolchain.get(f"{name}_sha256")
            or not json_integer(binding["mode"])
            or not json_integer(binding["size"])
        ):
            raise PreparationError(f"release execution-tool {name} binding differs")
        identity = binding["identity"]
        if (
            not isinstance(identity, dict)
            or set(identity)
            != {"changed_ns", "device", "inode", "link_count", "modified_ns"}
            or any(not json_integer(identity[field]) for field in identity)
        ):
            raise PreparationError(f"release execution-tool {name} identity differs")
        if live:
            replay_artifact_binding(binding, f"release execution-tool {name}")

    dev_null = execution_tools["dev_null"]
    if (
        not isinstance(dev_null, dict)
        or set(dev_null) != {"identity", "parent_path_chain", "trusted_system"}
        or dev_null.get("trusted_system") is not True
    ):
        raise PreparationError("release execution-tool null-device binding differs")
    null_identity = dev_null["identity"]
    null_integer_fields = {
        "changed_ns", "device", "gid", "inode", "link_count", "major", "minor",
        "modified_ns", "permissions", "size", "type", "uid",
    }
    if (
        not isinstance(null_identity, dict)
        or set(null_identity) != {*null_integer_fields, "path"}
        or any(not json_integer(null_identity[field]) for field in null_integer_fields)
        or null_identity["path"] != str(HOST_DEV_NULL)
        or null_identity["gid"] != 0
        or null_identity["uid"] != 0
        or null_identity["link_count"] != 1
        or null_identity["major"] != 1
        or null_identity["minor"] != 3
        or null_identity["permissions"] != 0o666
        or null_identity["type"] != stat.S_IFCHR
    ):
        raise PreparationError("release execution-tool null-device identity differs")
    parent_chain = dev_null["parent_path_chain"]
    chain_integer_fields = {
        "changed_ns", "device", "gid", "inode", "link_count", "mode",
        "modified_ns", "size", "type", "uid",
    }
    if (
        not isinstance(parent_chain, list)
        or len(parent_chain) != 2
        or any(
            not isinstance(item, dict)
            or set(item) != {*chain_integer_fields, "path"}
            or any(not json_integer(item[field]) for field in chain_integer_fields)
            for item in parent_chain
        )
        or [item["path"] for item in parent_chain] != ["/", "/dev"]
    ):
        raise PreparationError("release execution-tool null-device chain differs")
    if live:
        null_descriptor, observed_null = open_retained_null_device(
            HOST_DEV_NULL, "release execution-tool null device"
        )
        try:
            if observed_null != dev_null:
                raise PreparationError("release execution-tool null device differs")
        finally:
            os.close(null_descriptor)

    root_binding = execution_tools["toolchain_root"]
    if (
        not isinstance(root_binding, dict)
        or set(root_binding) != {"device", "inode", "link_count", "mode"}
        or any(not json_integer(root_binding[field]) for field in root_binding)
    ):
        raise PreparationError("release execution-tool root identity differs")
    if live:
        root_descriptor, observed_root = open_retained_directory(
            toolchain_root, "release execution-tool root"
        )
        try:
            if observed_root != root_binding:
                raise PreparationError("release execution-tool root differs")
        finally:
            os.close(root_descriptor)
    return hash_bytes(canonical_json(execution_tools))


def normalized_sandbox_sha256(
    attestation: dict[str, Any], *, live_execution_tools: bool = True
) -> str:
    argv = attestation.get("build_argv")
    if (
        not isinstance(argv, list)
        or not argv
        or any(not isinstance(argument, str) for argument in argv)
    ):
        raise PreparationError("release build argv is not an exact string list")
    toolchain = attestation.get("toolchain")
    if (
        not isinstance(toolchain, dict)
        or not isinstance(toolchain.get("bwrap_path"), str)
        or not isinstance(toolchain.get("rustc_host"), str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", toolchain["rustc_host"])
        is None
    ):
        raise PreparationError("release sandbox toolchain authority is absent")
    rust_lld_guest_path = (
        f"{GUEST_TOOLCHAIN_ROOT}/lib/rustlib/{toolchain['rustc_host']}"
        "/bin/gcc-ld/ld.lld"
    )
    system_bindings = tuple(
        (f"system:{guest_path}", "--ro-bind-fd", guest_path)
        for _host_path, guest_path in TRUSTED_SYSTEM_MOUNTS
    )
    core_bindings = (
        ("source", "--ro-bind-fd", GUEST_SOURCE),
        ("target", "--bind-fd", GUEST_TARGET),
        ("toolchain_root", "--ro-bind-fd", GUEST_TOOLCHAIN_ROOT),
        ("cargo", "--ro-bind-fd", GUEST_CARGO),
        ("rustc", "--ro-bind-fd", GUEST_RUSTC),
        (
            "rust_lld",
            "--ro-bind-fd",
            rust_lld_guest_path,
        ),
    )
    config_bindings = tuple(
        (f"config:{guest_path}", "--ro-bind-fd", guest_path)
        for guest_path in GUEST_BOUND_CONFIG_PATHS
    )
    expected_bindings = (*system_bindings, *core_bindings, *config_bindings)
    observed_bindings: list[tuple[str, str, str]] = []
    descriptors: dict[str, int] = {}
    normalized = list(argv)
    for index, argument in enumerate(argv):
        if argument not in {"--ro-bind-fd", "--bind-fd"}:
            continue
        descriptor_text = argv[index + 1] if index + 1 < len(argv) else ""
        if (
            index + 2 >= len(argv)
            or not descriptor_text.isdecimal()
            or str(int(descriptor_text)) != descriptor_text
        ):
            raise PreparationError("release sandbox descriptor binding differs")
        destination = argv[index + 2]
        ordinal = len(observed_bindings)
        if ordinal >= len(expected_bindings):
            raise PreparationError("release sandbox has an extra descriptor binding")
        name, _expected_option, _expected_destination = expected_bindings[ordinal]
        observed_bindings.append((name, argument, destination))
        descriptors[name] = int(descriptor_text)
        normalized[index + 1] = f"$FD:{destination}"
    if tuple(observed_bindings) != expected_bindings:
        raise PreparationError("release sandbox guest bindings differ")
    dev_null_indexes = [
        index for index, argument in enumerate(argv) if argument == "--dev-bind"
    ]
    if len(dev_null_indexes) != 1:
        raise PreparationError("release sandbox null-device binding differs")
    dev_null_index = dev_null_indexes[0]
    dev_null_source = (
        argv[dev_null_index + 1] if dev_null_index + 2 < len(argv) else ""
    )
    dev_null_descriptor = (
        dev_null_source.removeprefix("/proc/self/fd/")
        if dev_null_source.startswith("/proc/self/fd/")
        else ""
    )
    if (
        not dev_null_descriptor.isdecimal()
        or str(int(dev_null_descriptor)) != dev_null_descriptor
        or int(dev_null_descriptor) < 3
        or argv[dev_null_index + 2] != "/dev/null"
    ):
        raise PreparationError("release sandbox null-device descriptor differs")
    descriptors["dev_null"] = int(dev_null_descriptor)
    normalized[dev_null_index + 1] = "$FD:/dev/null"
    cargo_home_descriptor, overlay_index = cargo_home_overlay_descriptor(
        argv, "release sandbox"
    )
    descriptors["cargo_home"] = cargo_home_descriptor
    if len(descriptors) != len(set(descriptors.values())):
        raise PreparationError("release sandbox descriptors alias")
    normalized[overlay_index + 1] = "$FD:cargo-home-overlay"
    core_descriptors = {
        name: descriptors[name] for name, _option, _path in core_bindings
    }
    core_descriptors["dev_null"] = descriptors["dev_null"]
    core_descriptors["cargo_home"] = cargo_home_descriptor
    system_descriptors = {
        path: descriptors[f"system:{path}"]
        for _host_path, path in TRUSTED_SYSTEM_MOUNTS
    }
    config_descriptors = {
        path: descriptors[f"config:{path}"] for path in GUEST_BOUND_CONFIG_PATHS
    }
    expected_argv = sandboxed_build_argv(
        core_descriptors,
        system_descriptors,
        config_descriptors,
        "mess-store",
        "asterism_rebaseline_public",
        toolchain["bwrap_path"],
        rust_lld_guest_path,
    )
    if argv != expected_argv:
        raise PreparationError("release sandbox argv differs from the exact contract")
    cargo_config = attestation.get("cargo_config_search")
    if (
        not isinstance(cargo_config, dict)
        or set(cargo_config) != {"path", "sha256"}
        or not is_lower_hex(cargo_config.get("sha256"), SHA256)
    ):
        raise PreparationError("release sandbox Cargo config authority differs")
    semantic_authority = validate_semantic_input_authority(
        attestation.get("semantic_input_authority"), "release build"
    )
    execution_tools = attestation.get("execution_tools")
    execution_tools_sha256 = validated_execution_tools_sha256(
        execution_tools, toolchain, live=live_execution_tools
    )
    return hash_bytes(
        canonical_json(
            {
                "argv": normalized,
                "cargo_config_search_sha256": cargo_config["sha256"],
                "execution_tools_sha256": execution_tools_sha256,
                "semantic_runtime_sha256": semantic_authority["runtime_sha256"],
            }
        )
    )


def release_build_record(
    *,
    role: str,
    artifact_role: str,
    attestation: dict[str, Any],
    contract: dict[str, Any],
    approval_sha256: str,
    live_execution_tools: bool = True,
) -> dict[str, Any]:
    record = {
        "artifact_role": artifact_role,
        "attestation": attestation,
        "attestation_sha256": hash_bytes(canonical_json(attestation)),
        "build_environment_sha256": hash_bytes(
            canonical_json(attestation["build_env"])
        ),
        "build_nonce": attestation["build_nonce"],
        "cargo_lock_sha256": attestation["cargo_lock_sha256"],
        "cfg_test": False,
        "contract_sha256": hash_bytes(canonical_json(contract)),
        "role": role,
        "rustc_workspace_wrapper": "absent",
        "sandbox_sha256": normalized_sandbox_sha256(
            attestation, live_execution_tools=live_execution_tools
        ),
        "source_approval_sha256": approval_sha256,
        "toolchain_sha256": hash_bytes(canonical_json(attestation["toolchain"])),
    }
    if (
        "RUSTC_WORKSPACE_WRAPPER" in attestation["build_env"]
        or "RUSTC_WRAPPER" in attestation["build_env"]
        or "RUSTFLAGS" in attestation["build_env"]
        or "CARGO_ENCODED_RUSTFLAGS" in attestation["build_env"]
    ):
        raise PreparationError(f"{role} release build enables a wrapper or cfg flags")
    return record


def build_product_overlay_a(
    *,
    repository: Path,
    output: Path,
    plan: dict[str, Any],
    approval: dict[str, Any],
    approval_sha256: str,
    locks: dict[str, Any],
    approved_toolchain: dict[str, str],
    nonce: str,
    contract: dict[str, Any],
    build_env: dict[str, str],
) -> tuple[dict[str, Any], Path, dict[str, Any]]:
    claim = plan["variants"]["A"]
    label = "A-product-overlay"
    staged = stage_variant(
        repository,
        output,
        "A",
        claim,
        approved_toolchain,
        evidence_label=label,
    )
    if staged["overlay_manifest_sha256"] != approval["variants"]["A"][
        "overlay_manifest_sha256"
    ]:
        raise PreparationError("proof-only A base overlay manifest differs")
    root = staged["root"]
    overlay_fd, overlay_payload, overlay_binding, _overlay_full = (
        open_retained_artifact(CURRENT_PRODUCT_OVERLAY, "product test overlay")
    )
    try:
        apply_product_test_overlay(root, approved_toolchain, overlay_payload)
        verify_retained_artifact(
            overlay_fd,
            CURRENT_PRODUCT_OVERLAY,
            overlay_payload,
            overlay_binding,
            "product test overlay",
        )
    finally:
        os.close(overlay_fd)
    config_binding = write_sandboxed_cargo_config_search(
        output / "manifests" / "cargo-config-build-A-product-overlay.json",
        root,
        approved_toolchain,
    )
    replay_sandboxed_cargo_config_search(config_binding, root, approved_toolchain)
    approved_lock = Path(locks["variants"]["A"]["final_lock_path"]).resolve(
        strict=True
    )
    lock_destination = root / "Cargo.lock"
    lock_destination.unlink(missing_ok=True)
    atomic_write(lock_destination, approved_lock.read_bytes(), mode=0o444)
    lock_pre_sha256 = hash_file(lock_destination)
    if lock_pre_sha256 != approval["variants"]["A"]["cargo_lock_sha256"]:
        raise PreparationError("proof-only A lock differs before build")
    make_read_only(root)
    source_before = file_manifest(root)
    materialized_manifest_path = (
        output / "manifests" / "materialized-A-product-overlay.json"
    )
    atomic_json(materialized_manifest_path, source_before)
    materialized_manifest_sha256 = hash_file(materialized_manifest_path)
    target = output / "targets" / "A-product-overlay"
    target_was_absent = not target.exists() and not target.is_symlink()
    if not target_was_absent:
        raise PreparationError("proof-only A target directory is not fresh")
    target.mkdir(parents=True)
    build_log_path = output / "logs" / "build-A-product-overlay.json"
    replay_sandboxed_cargo_config_search(config_binding, root, approved_toolchain)
    (
        build_argv,
        build_child,
        _stdout,
        _stderr,
        semantic_input_authority,
        execution_tools,
    ) = execute_sandboxed_build(
        root=root,
        target=target.resolve(),
        package="mess-store",
        example="asterism_rebaseline_public",
        toolchain=approved_toolchain,
        environment=build_env,
        cargo_config_search=config_binding,
        output_path=build_log_path,
        context="proof-only A build",
    )
    validate_attested_child(build_child, "proof-only A build")
    replay_sandboxed_cargo_config_search(config_binding, root, approved_toolchain)
    if toolchain_identity() != approved_toolchain:
        raise PreparationError("toolchain changed during proof-only A build")
    lock_post_sha256 = hash_file(lock_destination)
    if lock_post_sha256 != lock_pre_sha256 or file_manifest(root) != source_before:
        raise PreparationError("proof-only A source or lock changed during build")
    built = target / "release" / "examples" / "asterism_rebaseline_public"
    artifact_root = output / "artifacts" / "proof-only"
    artifact_root.mkdir(parents=True)
    binary = artifact_root / "ast-rb-a-product-overlay"
    atomic_write(binary, built.read_bytes(), mode=0o555)
    contract_env = frozen_runtime_environment(
        {"ASTERISM_REBASELINE_MODE": "contract"}
    )
    contract_output_path = output / "logs" / "contract-A-product-overlay.json"
    contract_child, contract_stdout, contract_stderr = run_attested(
        [str(binary.resolve())],
        cwd=artifact_root,
        env=contract_env,
        output_path=contract_output_path,
        raw_stdout=True,
        timeout=30,
    )
    validate_attested_child(contract_child, "proof-only A contract")
    if contract_stderr or contract_stdout != canonical_json(contract):
        raise PreparationError("proof-only A contract output differs")
    attestation = {
        "archive_manifest_path": str(
            staged["archive"]["archive_manifest_path"].resolve()
        ),
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
        "cargo_lock_sha256": lock_post_sha256,
        "cargo_config_search": config_binding,
        "contract_child": contract_child,
        "contract_output_path": str(contract_output_path.resolve()),
        "contract_output_sha256": hash_file(contract_output_path),
        "materialized_manifest_path": str(materialized_manifest_path.resolve()),
        "materialized_manifest_post_sha256": materialized_manifest_sha256,
        "materialized_manifest_pre_sha256": materialized_manifest_sha256,
        "materialized_manifest_sha256": materialized_manifest_sha256,
        "materialized_root": str(root.resolve()),
        "execution_tools": execution_tools,
        "overlay_manifest_path": str(staged["overlay_manifest_path"].resolve()),
        "overlay_manifest_sha256": staged["overlay_manifest_sha256"],
        "product_overlay_sha256": CURRENT_PRODUCT_OVERLAY_SHA256,
        "source_archive_bytes": staged["archive"]["archive_bytes"],
        "source_archive_path": str(staged["archive"]["archive_path"].resolve()),
        "source_archive_sha256": staged["archive"]["archive_sha256"],
        "source_commit": claim["product_commit"],
        "source_read_only": True,
        "source_tree": claim["product_tree"],
        "semantic_input_authority": semantic_input_authority,
        "target_dir": str(target.resolve()),
        "target_dir_was_absent": target_was_absent,
        "toolchain": approved_toolchain,
    }
    record = release_build_record(
        role="overlay_a",
        artifact_role="proof_only",
        attestation=attestation,
        contract=contract,
        approval_sha256=approval_sha256,
    )
    return record, binary, contract


def nm_inventory(
    *,
    nm_fd: int,
    nm_path: Path,
    binary_fd: int,
    output_path: Path,
    log_path: Path,
    repository: Path,
) -> tuple[dict[str, Any], dict[str, Any], bytes]:
    argv = [
        str(nm_path),
        "--defined-only",
        "--demangle=rust",
        "--format=posix",
        f"/proc/self/fd/{binary_fd}",
    ]
    child, stdout, stderr = run_attested(
        argv,
        cwd=repository,
        env=frozen_runtime_environment({}),
        output_path=log_path,
        raw_stdout=False,
        timeout=120,
        inherited_fds=(nm_fd, binary_fd),
        executable=f"/proc/self/fd/{nm_fd}",
    )
    validate_attested_child(child, f"nm inventory {output_path.name}")
    if stderr:
        raise PreparationError(f"nm inventory emitted stderr: {output_path.name}")
    atomic_write(output_path, stdout, mode=0o444)
    descriptor, payload, binding, _full = open_retained_artifact(
        output_path, f"symbol inventory {output_path.name}"
    )
    try:
        verify_retained_artifact(
            descriptor,
            output_path,
            payload,
            binding,
            f"symbol inventory {output_path.name}",
        )
    finally:
        os.close(descriptor)
    return binding, child, stdout


def validate_preapproval_nm_authority(value: Any) -> dict[str, Any]:
    """Validate and unwrap the trusted current-child nm authority record."""

    def json_integer(item: Any) -> bool:
        return isinstance(item, int) and not isinstance(item, bool)

    identity_fields = {
        "bytes", "ctime_ns", "device", "inode", "link_count", "mode",
        "mtime_ns", "path", "sha256", "size",
    }
    chain_fields = {
        "changed_ns", "device", "gid", "inode", "link_count", "mode",
        "modified_ns", "path", "size", "type", "uid",
    }
    if (
        not isinstance(value, dict)
        or set(value) != {"identity", "path_chain", "trusted_system"}
        or value["trusted_system"] is not True
    ):
        raise PreparationError("preapproval nm authority is absent")
    identity = value["identity"]
    if (
        not isinstance(identity, dict)
        or set(identity) != identity_fields
        or any(
            not json_integer(identity[field])
            for field in identity_fields - {"path", "sha256"}
        )
        or not isinstance(identity["path"], str)
        or not identity["path"]
        or not Path(identity["path"]).is_absolute()
        or not isinstance(identity["sha256"], str)
        or re.fullmatch(r"[0-9a-f]{64}", identity["sha256"]) is None
    ):
        raise PreparationError("preapproval nm authority identity differs")
    chain = value["path_chain"]
    if not isinstance(chain, list) or not chain:
        raise PreparationError("preapproval nm authority path chain differs")
    for item in chain:
        if (
            not isinstance(item, dict)
            or set(item) != chain_fields
            or any(
                not json_integer(item[field])
                for field in chain_fields - {"path"}
            )
            or not isinstance(item["path"], str)
            or not item["path"]
            or not Path(item["path"]).is_absolute()
        ):
            raise PreparationError("preapproval nm authority path chain differs")
    if chain[0]["path"] != "/" or chain[-1]["path"] != identity["path"]:
        raise PreparationError("preapproval nm authority path chain differs")
    return identity


def produce_release_compile_out(
    *,
    repository: Path,
    output: Path,
    approval: dict[str, Any],
    approval_sha256: str,
    current_children: CanonicalSnapshot,
    ordinary_build: dict[str, Any],
    ordinary_binary: Path,
    overlay_build: dict[str, Any],
    overlay_binary: Path,
) -> dict[str, Any]:
    common_fields = (
        "source_approval_sha256",
        "contract_sha256",
        "build_nonce",
        "cargo_lock_sha256",
        "toolchain_sha256",
        "build_environment_sha256",
        "sandbox_sha256",
        "cfg_test",
        "rustc_workspace_wrapper",
    )
    if any(
        ordinary_build[field] != overlay_build[field] for field in common_fields
    ):
        raise PreparationError("ordinary/proof-only A build authority differs")
    if (
        ordinary_build["role"] != "ordinary_a"
        or ordinary_build["artifact_role"] != "published"
        or overlay_build["role"] != "overlay_a"
        or overlay_build["artifact_role"] != "proof_only"
    ):
        raise PreparationError("release compile-out artifact roles differ")
    equivalence = {
        field: ordinary_build[field] for field in common_fields
    }
    equivalence.update(
        {"ordinary_a_role": "published", "overlay_a_role": "proof_only"}
    )
    ordinary_fd = overlay_fd = nm_fd = -1
    try:
        ordinary_fd, ordinary_payload, ordinary_binding, _ordinary_full = (
            open_retained_artifact(ordinary_binary, "ordinary published A")
        )
        overlay_fd, overlay_payload, overlay_binding, _overlay_full = (
            open_retained_artifact(overlay_binary, "proof-only product-overlay A")
        )
        if ordinary_payload != overlay_payload:
            raise PreparationError("ordinary/proof-only A binaries are not byte-identical")
        if (
            ordinary_binding["path"] == overlay_binding["path"]
            or ordinary_binding["identity"] == overlay_binding["identity"]
        ):
            raise PreparationError("ordinary/proof-only A binaries are not disjoint")
        encoded_tokens = [token.encode() for token in FORBIDDEN_RELEASE_HOOK_STRINGS]
        if any(token in ordinary_payload for token in encoded_tokens):
            raise PreparationError("release A binary retains a forbidden hook string")
        preapproval_nm = current_children.value["release_compile_out"].get("nm")
        preapproval_nm_identity = validate_preapproval_nm_authority(preapproval_nm)
        nm_path = Path(preapproval_nm_identity["path"])
        nm_fd, nm_payload, nm_binding, nm_full = open_retained_artifact(
            nm_path, "reviewed preapproval nm"
        )
        if nm_full != preapproval_nm_identity:
            raise PreparationError("real-approval nm differs from preapproval authority")
        ordinary_inventory, ordinary_nm, ordinary_symbols = nm_inventory(
            nm_fd=nm_fd,
            nm_path=nm_path,
            binary_fd=ordinary_fd,
            output_path=output / "manifests" / "symbols-ordinary-a.txt",
            log_path=output / "logs" / "nm-ordinary-a.json",
            repository=repository,
        )
        overlay_inventory, overlay_nm, overlay_symbols = nm_inventory(
            nm_fd=nm_fd,
            nm_path=nm_path,
            binary_fd=overlay_fd,
            output_path=output / "manifests" / "symbols-overlay-a.txt",
            log_path=output / "logs" / "nm-overlay-a.json",
            repository=repository,
        )
        if ordinary_symbols != overlay_symbols:
            raise PreparationError(
                "ordinary/proof-only A symbol inventories are not byte-identical"
            )
        if any(token in ordinary_symbols for token in encoded_tokens):
            raise PreparationError("release A symbols retain a forbidden hook string")
        verify_retained_artifact(
            ordinary_fd,
            ordinary_binary,
            ordinary_payload,
            ordinary_binding,
            "ordinary published A",
        )
        verify_retained_artifact(
            overlay_fd,
            overlay_binary,
            overlay_payload,
            overlay_binding,
            "proof-only product-overlay A",
        )
        verify_retained_artifact(
            nm_fd, nm_path, nm_payload, nm_binding, "reviewed preapproval nm"
        )
    finally:
        for descriptor in (ordinary_fd, overlay_fd, nm_fd):
            if descriptor >= 0:
                os.close(descriptor)
    requirement = approval["source_review"]["release_compile_out_requirement"]
    return {
        "binaries": {
            "ordinary_a": ordinary_binding,
            "overlay_a": overlay_binding,
        },
        "binary_byte_identical": True,
        "builds": {"ordinary_a": ordinary_build, "overlay_a": overlay_build},
        "current_children_attestation_sha256": current_children.sha256,
        "equivalence_contract": equivalence,
        "forbidden_hook_strings": list(FORBIDDEN_RELEASE_HOOK_STRINGS),
        "forbidden_hook_strings_absent": True,
        "nm": {
            "ordinary_a": ordinary_nm,
            "overlay_a": overlay_nm,
            "tool": nm_binding,
        },
        "product_overlay_sha256": CURRENT_PRODUCT_OVERLAY_SHA256,
        "protocol": PROTOCOL,
        "protocol_sha256": PROTOCOL_DOCUMENT_SHA256,
        "published_a_sha256": ordinary_binding["sha256"],
        "requirement_sha256": hash_bytes(canonical_json(requirement)),
        "schema": RELEASE_COMPILE_OUT_SCHEMA,
        "source_approval_sha256": approval_sha256,
        "status": "ok",
        "symbol_inventories": {
            "ordinary_a": ordinary_inventory,
            "overlay_a": overlay_inventory,
        },
        "symbol_inventory_byte_identical": True,
    }


def build(
    repository: Path, output: Path, approval_path: Path, lock_manifest_path: Path,
    tools_path: Path, source_review_bundle_path: Path,
    current_children_path: Path, lock_authority_path: Path,
    lock_review_bundle_path: Path,
) -> None:
    if output.exists() or output.is_symlink():
        raise PreparationError(f"output must be absent: {output}")
    admission = filesystem_admission(output.parent)
    current_toolchain = toolchain_identity()
    approval_snapshot, locks, tools, source_inputs, source_bundle = validate_approval(
        repository, approval_path, lock_manifest_path, tools_path,
        source_review_bundle_path, current_children_path, lock_authority_path,
        lock_review_bundle_path,
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
    source_binding_specs = {
        "bundle": (
            source_bundle,
            output / "bindings" / "source-review-bundle.json",
            SOURCE_REVIEW_BUNDLE_SCHEMA,
        ),
        "current_children_attestation": (
            source_inputs["current_children_attestation"],
            output / "bindings" / "current-children-attestation.json",
            CURRENT_CHILDREN_ATTESTATION_SCHEMA,
        ),
        "lock_authority": (
            source_inputs["lock_authority"],
            output / "bindings" / "lock-review-authority.json",
            CURRENT_LOCK_AUTHORITY_SCHEMA,
        ),
        "lock_review_bundle": (
            source_inputs["lock_review_bundle"],
            output / "bindings" / "lock-review-bundle.json",
            CURRENT_LOCK_REVIEW_BUNDLE_SCHEMA,
        ),
    }
    prepared_source_review = {
        name: local_binding(
            copy_snapshot(snapshot, destination, schema, f"copied source-review {name}")
        )
        for name, (snapshot, destination, schema) in source_binding_specs.items()
    }
    tools_manifest_sha256 = source_inputs["tools_manifest"].sha256
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
    release_compile_out: dict[str, Any] | None = None
    release_runtime_digests: set[str] = set()
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
        config_binding = write_sandboxed_cargo_config_search(
            output / "manifests" / f"cargo-config-build-{variant}.json",
            root,
            approved_toolchain,
        )
        replay_sandboxed_cargo_config_search(
            config_binding, root, approved_toolchain
        )
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
        build_env = sandboxed_build_environment(
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
        build_log_path = output / "logs" / f"build-{variant}.json"
        replay_sandboxed_cargo_config_search(
            config_binding, root, approved_toolchain
        )
        (
            build_argv,
            build_child,
            _build_stdout,
            build_stderr,
            semantic_input_authority,
            execution_tools,
        ) = (
            execute_sandboxed_build(
                root=root,
                target=target.resolve(),
                package=package,
                example=example,
                toolchain=approved_toolchain,
                environment=build_env,
                cargo_config_search=config_binding,
                output_path=build_log_path,
                context=f"{variant} build",
            )
        )
        validate_attested_child(build_child, f"{variant} build")
        release_runtime_digests.add(
            semantic_input_authority["runtime_sha256"]
        )
        replay_sandboxed_cargo_config_search(
            config_binding, root, approved_toolchain
        )
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
            "execution_tools": execution_tools,
            "overlay_manifest_path": str(staged["overlay_manifest_path"].resolve()),
            "overlay_manifest_sha256": staged["overlay_manifest_sha256"],
            "source_archive_bytes": staged["archive"]["archive_bytes"],
            "source_archive_path": str(staged["archive"]["archive_path"].resolve()),
            "source_archive_sha256": staged["archive"]["archive_sha256"],
            "source_commit": claim["product_commit"],
            "source_read_only": True,
            "source_tree": claim["product_tree"],
            "semantic_input_authority": semantic_input_authority,
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
        if variant == "A":
            ordinary_build = release_build_record(
                role="ordinary_a",
                artifact_role="published",
                attestation=attestation,
                contract=contract,
                approval_sha256=approval_sha256,
            )
            overlay_build, overlay_binary, overlay_contract = build_product_overlay_a(
                repository=repository,
                output=output,
                plan=plan,
                approval=approval,
                approval_sha256=approval_sha256,
                locks=locks,
                approved_toolchain=approved_toolchain,
                nonce=nonce,
                contract=contract,
                build_env=build_env,
            )
            if overlay_contract != contract:
                raise PreparationError("proof-only A contract differs from ordinary A")
            release_runtime_digests.add(
                overlay_build["attestation"]["semantic_input_authority"][
                    "runtime_sha256"
                ]
            )
            release_compile_out = produce_release_compile_out(
                repository=repository,
                output=output,
                approval=approval,
                approval_sha256=approval_sha256,
                current_children=source_inputs["current_children_attestation"],
                ordinary_build=ordinary_build,
                ordinary_binary=binary,
                overlay_build=overlay_build,
                overlay_binary=overlay_binary,
            )
    if len(release_runtime_digests) != 1:
        raise PreparationError(
            "release builds used different semantic runtime authority"
        )
    if release_compile_out is None:
        raise PreparationError("real-approval release compile-out proof was not produced")
    release_compile_out_path = output / "manifests" / "release-compile-out.json"
    atomic_write(
        release_compile_out_path,
        canonical_json(release_compile_out),
        mode=0o444,
    )
    release_compile_out_binding = {
        "mode": 0o444,
        "path": str(release_compile_out_path.resolve()),
        "sha256": hash_file(release_compile_out_path),
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
        "source_review": prepared_source_review,
        "release_compile_out": release_compile_out_binding,
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
    authority_before_import = prepared_authority_manifest(output)
    validate_support_import_immutability(support_root, bound_support, bound_tools)
    if prepared_authority_manifest(output) != authority_before_import:
        raise PreparationError("prepared authority changed during support imports")
    freeze_prepared_root(output, claims)
    frozen_authority = prepared_authority_manifest(output)
    validate_frozen_prepared_root(output, claims)
    for group_name in ("binaries", "symbol_inventories"):
        for role, binding in release_compile_out[group_name].items():
            replay_artifact_binding(
                binding, f"frozen release compile-out {group_name} {role}"
            )
    replay_artifact_binding(
        release_compile_out["nm"]["tool"],
        "frozen release compile-out nm authority",
    )
    frozen_proof = immutable_canonical_snapshot(
        release_compile_out_path,
        RELEASE_COMPILE_OUT_SCHEMA,
        "frozen release compile-out proof",
    )
    if (
        frozen_proof.value != release_compile_out
        or frozen_proof.sha256 != release_compile_out_binding["sha256"]
    ):
        raise PreparationError("frozen release compile-out proof changed")
    if prepared_authority_manifest(output) != frozen_authority:
        raise PreparationError("frozen prepared authority changed during final replay")


def preapproval_nm_authority_static_self_test() -> int:
    identity = {
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

    def chain_entry(path: str, ordinal: int) -> dict[str, Any]:
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
            "type": stat.S_IFDIR if path != identity["path"] else stat.S_IFREG,
            "uid": 0,
        }

    authority = {
        "identity": identity,
        "path_chain": [chain_entry("/", 1), chain_entry(identity["path"], 2)],
        "trusted_system": True,
    }
    if validate_preapproval_nm_authority(authority) != identity:
        raise AssertionError("preapproval nm wrapper positive fixture differs")

    def changed(callback: Any) -> dict[str, Any]:
        hostile = json.loads(json.dumps(authority))
        callback(hostile)
        return hostile

    hostiles = {
        "flat_legacy": dict(identity),
        "missing_wrapper_key": changed(lambda value: value.pop("path_chain")),
        "extra_wrapper_key": changed(lambda value: value.__setitem__("extra", True)),
        "missing_identity": changed(lambda value: value.pop("identity")),
        "wrong_identity_keys": changed(
            lambda value: value["identity"].pop("bytes")
        ),
        "trusted_system_false": changed(
            lambda value: value.__setitem__("trusted_system", False)
        ),
        "trusted_system_absent": changed(
            lambda value: value.pop("trusted_system")
        ),
        "path_null": changed(
            lambda value: value["identity"].__setitem__("path", None)
        ),
        "path_non_string": changed(
            lambda value: value["identity"].__setitem__("path", 7)
        ),
        "path_relative": changed(
            lambda value: value["identity"].__setitem__("path", "usr/bin/nm")
        ),
        "bool_integer": changed(
            lambda value: value["identity"].__setitem__("mode", True)
        ),
        "path_chain_last_mismatch": changed(
            lambda value: value["path_chain"][-1].__setitem__(
                "path", "/usr/bin/other"
            )
        ),
    }
    rejected = 0
    for name, hostile in hostiles.items():
        try:
            validate_preapproval_nm_authority(hostile)
        except PreparationError:
            rejected += 1
        else:
            raise AssertionError(f"hostile preapproval nm authority passed: {name}")
    return rejected


def canonical_encoding_boundary_static_self_test() -> int:
    """Prove the two canonical JSON boundaries do not accept each other."""

    rejected = 0

    def reject(callable_: Any, context: str) -> None:
        nonlocal rejected
        try:
            callable_()
        except PreparationError:
            rejected += 1
        else:
            raise AssertionError(
                f"hostile canonical encoding was accepted: {context}"
            )

    with tempfile.TemporaryDirectory(
        prefix="asterism-canonical-boundary-static-"
    ) as temporary:
        root = Path(temporary).resolve()
        boundary_schema = "bn-16gw-canonical-boundary-self-test-v1"
        boundary_value = {"path": "/evidence/\u00de", "schema": boundary_schema}
        utf8_payload = canonical_json(boundary_value)
        ascii_payload = builder_local_canonical_json(boundary_value)
        if (
            b"\xc3\x9e" not in utf8_payload
            or b"\\u00de" in utf8_payload
            or b"\\u00de" not in ascii_payload
            or b"\xc3\x9e" in ascii_payload
            or utf8_payload == ascii_payload
        ):
            raise AssertionError("non-ASCII canonical fixtures do not differ")
        reviewed_path = root / "reviewed-utf8.json"
        builder_path = root / "builder-ascii.json"
        atomic_write(reviewed_path, utf8_payload, mode=0o444)
        atomic_write(builder_path, ascii_payload, mode=0o444)
        if (
            immutable_canonical_snapshot(
                reviewed_path, boundary_schema, "reviewed UTF-8 fixture"
            ).value
            != boundary_value
            or immutable_builder_local_canonical_snapshot(
                builder_path, boundary_schema, "builder ASCII fixture"
            ).value
            != boundary_value
        ):
            raise AssertionError("canonical boundary positive fixture differs")
        reject(
            lambda: immutable_canonical_snapshot(
                builder_path, boundary_schema, "reviewed ASCII hostile"
            ),
            "ASCII escapes at reviewed/prepared UTF-8 boundary",
        )
        reject(
            lambda: immutable_builder_local_canonical_snapshot(
                reviewed_path, boundary_schema, "builder UTF-8 hostile"
            ),
            "UTF-8 bytes at builder-local ASCII boundary",
        )

        def entry(
            path: str, kind: str, ordinal: int, *, trusted_system: bool
        ) -> dict[str, Any]:
            return {
                "changed_ns": ordinal,
                "device": 1,
                "file_type": kind,
                "gid": 0,
                "inode": ordinal,
                "link_count": 1,
                "modified_ns": ordinal,
                "path": path,
                "permissions": 0o555 if kind == "directory" else 0o444,
                "sha256": (
                    None
                    if kind == "directory" or trusted_system
                    else EMPTY_SHA256
                ),
                "size": 0,
                "symlink_target": None,
                "symlink_scope": None,
                "uid": 0,
            }

        def recursive_manifest(
            role: str, *, trusted_system: bool
        ) -> dict[str, Any]:
            return {
                "entries": [
                    entry(".", "directory", 1, trusted_system=trusted_system),
                    entry(
                        "\u00de",
                        "regular",
                        2,
                        trusted_system=trusted_system,
                    ),
                ],
                "role": role,
                "schema": RECURSIVE_TREE_AUTHORITY_SCHEMA,
            }

        def write_evidence(
            name: str, value: dict[str, Any], *, builder_local: bool
        ) -> tuple[Path, bytes]:
            payload = (
                builder_local_canonical_json(value)
                if builder_local
                else canonical_json(value)
            )
            path = root / (
                f"{'builder' if builder_local else 'reviewed'}-{name}.json"
            )
            atomic_write(path, payload, mode=0o444)
            return path, payload

        def semantic_fixture(
            *, builder_local: bool
        ) -> tuple[dict[str, Any], dict[str, tuple[Path, bytes]]]:
            evidence: dict[str, tuple[Path, bytes]] = {}
            trees: dict[str, dict[str, Any]] = {}
            for name, role in (
                ("source", "source"),
                ("toolchain", "toolchain"),
                ("cargo_home", "cargo_home"),
            ):
                tree = recursive_manifest(role, trusted_system=False)
                trees[name] = tree
                evidence[name] = write_evidence(
                    name, tree, builder_local=builder_local
                )
            closure_mounts = []
            binding_mounts = []
            for ordinal, (host_path, guest_path) in enumerate(
                TRUSTED_SYSTEM_MOUNTS, start=1
            ):
                tree = recursive_manifest(
                    "system-" + guest_path.removeprefix("/").replace("/", "-"),
                    trusted_system=True,
                )
                closure_mounts.append(
                    {
                        "guest_path": guest_path,
                        "host_path": str(host_path),
                        "resolved_path": str(host_path),
                        "tree": tree,
                    }
                )
                root_entry = tree["entries"][0]
                binding_mounts.append(
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
            closure_value = {
                "mounts": closure_mounts,
                "schema": TRUSTED_SYSTEM_CLOSURE_SCHEMA,
            }
            evidence["trusted_system_closure"] = write_evidence(
                "trusted-system-closure",
                closure_value,
                builder_local=builder_local,
            )

            def tree_binding(name: str, role: str) -> dict[str, Any]:
                path, payload = evidence[name]
                tree = trees[name]
                return {
                    "entry_count": len(tree["entries"]),
                    "equal_pre_post": True,
                    "manifest_path": str(path),
                    "manifest_sha256": hash_bytes(payload),
                    "mutation_events_absent": True,
                    "role": role,
                    "schema": RECURSIVE_TREE_AUTHORITY_SCHEMA,
                    "watch_count": sum(
                        item["file_type"] == "directory"
                        for item in tree["entries"]
                    ),
                }

            closure_path, closure_payload = evidence["trusted_system_closure"]
            components = {
                "cargo_home": tree_binding("cargo_home", "cargo_home"),
                "toolchain": tree_binding("toolchain", "toolchain"),
                "trusted_system_closure": {
                    "entry_count": sum(
                        len(mount["tree"]["entries"])
                        for mount in closure_mounts
                    ),
                    "manifest_path": str(closure_path),
                    "mounts": binding_mounts,
                    "mutation_events_absent": True,
                    "schema": TRUSTED_SYSTEM_CLOSURE_SCHEMA,
                    "sha256": hash_bytes(closure_payload),
                    "watch_count": sum(
                        sum(
                            item["file_type"] == "directory"
                            for item in mount["tree"]["entries"]
                        )
                        for mount in closure_mounts
                    ),
                },
            }
            return (
                {
                    **components,
                    "runtime_sha256": semantic_runtime_sha256(components),
                    "schema": SEMANTIC_INPUT_AUTHORITY_SCHEMA,
                    "source": tree_binding("source", "source"),
                },
                evidence,
            )

        builder_authority, builder_evidence = semantic_fixture(
            builder_local=True
        )
        reviewed_authority, reviewed_evidence = semantic_fixture(
            builder_local=False
        )
        if (
            validate_semantic_input_authority(
                builder_authority,
                "builder-local non-ASCII semantic fixture",
                replay_evidence=True,
                builder_local_evidence=True,
            )
            != builder_authority
            or validate_semantic_input_authority(
                reviewed_authority,
                "reviewed UTF-8 non-ASCII semantic fixture",
                replay_evidence=True,
            )
            != reviewed_authority
        ):
            raise AssertionError("semantic canonical boundary positive fixture differs")

        def replace_evidence(
            authority: dict[str, Any],
            name: str,
            replacement: tuple[Path, bytes],
        ) -> dict[str, Any]:
            changed = json.loads(json.dumps(authority))
            path, payload = replacement
            if name == "trusted_system_closure":
                changed[name]["manifest_path"] = str(path)
                changed[name]["sha256"] = hash_bytes(payload)
            else:
                changed[name]["manifest_path"] = str(path)
                changed[name]["manifest_sha256"] = hash_bytes(payload)
            components = {
                component: changed[component]
                for component in (
                    "cargo_home",
                    "toolchain",
                    "trusted_system_closure",
                )
            }
            changed["runtime_sha256"] = semantic_runtime_sha256(components)
            return changed

        for name in (
            "source",
            "toolchain",
            "cargo_home",
            "trusted_system_closure",
        ):
            utf8_at_builder_boundary = replace_evidence(
                builder_authority, name, reviewed_evidence[name]
            )
            reject(
                lambda value=utf8_at_builder_boundary: (
                    validate_semantic_input_authority(
                        value,
                        "builder-local UTF-8 semantic hostile",
                        replay_evidence=True,
                        builder_local_evidence=True,
                    )
                ),
                f"UTF-8 {name} evidence at builder-local ASCII boundary",
            )
            ascii_at_reviewed_boundary = replace_evidence(
                reviewed_authority, name, builder_evidence[name]
            )
            reject(
                lambda value=ascii_at_reviewed_boundary: (
                    validate_semantic_input_authority(
                        value,
                        "reviewed ASCII semantic hostile",
                        replay_evidence=True,
                    )
                ),
                f"ASCII {name} evidence at reviewed UTF-8 boundary",
            )
    return rejected


def source_review_and_compile_out_static_self_test() -> int:
    """Reject forged review authority and release-equivalence drift statically."""

    rejected = 0

    def reject(callable_: Any, context: str) -> None:
        nonlocal rejected
        try:
            callable_()
        except (PreparationError, OSError, ValueError):
            rejected += 1
        else:
            raise AssertionError(f"hostile source-review mutation was accepted: {context}")

    with tempfile.TemporaryDirectory(prefix="asterism-source-review-static-") as temporary:
        root = Path(temporary).resolve()
        schemas = {
            "current_children_attestation": CURRENT_CHILDREN_ATTESTATION_SCHEMA,
            "lock_authority": CURRENT_LOCK_AUTHORITY_SCHEMA,
            "lock_manifest": LOCK_SCHEMA,
            "lock_review_bundle": CURRENT_LOCK_REVIEW_BUNDLE_SCHEMA,
            "tools_manifest": "asterism-rebaseline-tools-v3",
        }
        inputs: dict[str, CanonicalSnapshot] = {}
        for index, (name, schema) in enumerate(sorted(schemas.items())):
            value: dict[str, Any] = {"index": index, "schema": schema}
            if name == "current_children_attestation":
                value["release_compile_out"] = {
                    "binary_byte_identical": True,
                    "forbidden_hook_strings": list(FORBIDDEN_RELEASE_HOOK_STRINGS),
                    "preapproval_source_sentinel": PREAPPROVAL_SOURCE_SENTINEL,
                    "symbol_inventory_byte_identical": True,
                }
            path = root / f"{name}.json"
            atomic_write(path, canonical_json(value), mode=0o444)
            inputs[name] = immutable_canonical_snapshot(path, schema, name)
        requirement = release_compile_out_requirement(
            inputs["current_children_attestation"]
        )
        assert set(requirement) == {
            "binary_byte_identical",
            "cfg_test",
            "forbidden_hook_strings",
            "forbidden_hook_strings_absent",
            "ordinary_a_role",
            "overlay_a_role",
            "preapproval_compile_out_sha256",
            "product_overlay_sha256",
            "proof_must_bind_enclosing_approval_sha256",
            "repeat_under_real_source_approval",
            "rustc_workspace_wrapper",
            "same_contract_nonce_lock_toolchain_sandbox",
            "schema",
            "status",
            "symbol_inventory_byte_identical",
            "variant",
        }
        commit = "1" * GIT_OBJECT
        tree = "2" * GIT_OBJECT
        assertion = source_review_assertion(
            inputs=inputs,
            requirement=requirement,
            tooling_commit=commit,
            tooling_tree=tree,
        )
        assertion_sha256 = hash_bytes(canonical_json(assertion))
        review_id = "cr-bn-3hch-source"
        bundle_value = {
            "assertion": assertion,
            "assertion_sha256": assertion_sha256,
            "review_created": {
                "author": "mess-reviewer",
                "data": {
                    "description": "Review exact source authority.",
                    "initial_commit": commit,
                    "jj_change_id": f"detached:{commit}",
                    "review_id": review_id,
                    "scm_anchor": f"detached:{commit}",
                    "scm_kind": "git",
                    "title": "Exact source authority review",
                },
                "event": "ReviewCreated",
                "ts": "2026-07-16T20:00:00Z",
            },
            "schema": SOURCE_REVIEW_BUNDLE_SCHEMA,
            "verdict": {
                "author": "mess-reviewer",
                "data": {
                    "reason": (
                        "APPROVED assertion_sha256="
                        f"{assertion_sha256}; open_findings=0"
                    ),
                    "review_id": review_id,
                    "vote": "lgtm",
                },
                "event": "ReviewerVoted",
                "ts": "2026-07-16T20:01:00Z",
            },
        }
        bundle_path = root / "source-review-bundle.json"
        atomic_write(bundle_path, canonical_json(bundle_value), mode=0o444)
        bundle = immutable_canonical_snapshot(
            bundle_path, SOURCE_REVIEW_BUNDLE_SCHEMA, "static source-review bundle"
        )
        assert validate_source_review_bundle(bundle, assertion) == (
            review_id,
            "2026-07-16T20:01:00Z",
        )

        def changed_bundle(path: tuple[str, ...], value: Any) -> CanonicalSnapshot:
            changed = json.loads(json.dumps(bundle_value))
            target: dict[str, Any] = changed
            for part in path[:-1]:
                target = target[part]
            target[path[-1]] = value
            payload = canonical_json(changed)
            return replace(
                bundle,
                payload=payload,
                sha256=hash_bytes(payload),
                value=changed,
                size=len(payload),
            )

        hostile_bundles = (
            changed_bundle(("assertion", "open_findings"), 1),
            changed_bundle(("assertion", "status"), "blocked"),
            changed_bundle(("assertion_sha256",), "0" * SHA256),
            changed_bundle(("review_created", "author"), "bad author!"),
            changed_bundle(("review_created", "event"), "ReviewerVoted"),
            changed_bundle(("review_created", "ts"), "not-a-time"),
            changed_bundle(("review_created", "data", "initial_commit"), "3" * GIT_OBJECT),
            changed_bundle(("review_created", "data", "jj_change_id"), "detached:forged"),
            changed_bundle(("review_created", "data", "scm_anchor"), "detached:forged"),
            changed_bundle(("review_created", "data", "scm_kind"), "jj"),
            changed_bundle(("verdict", "data", "vote"), "request_changes"),
            changed_bundle(("verdict", "data", "review_id"), "cr-forged"),
            changed_bundle(("verdict", "data", "reason"), "APPROVED"),
            changed_bundle(("verdict", "ts"), "2026-07-16T19:59:00Z"),
        )
        for hostile in hostile_bundles:
            reject(
                lambda hostile=hostile: validate_source_review_bundle(
                    hostile, assertion
                ),
                "forged Seal bundle",
            )

        mutable = root / "mutable.json"
        atomic_write(
            mutable,
            canonical_json({"schema": SOURCE_REVIEW_BUNDLE_SCHEMA}),
            mode=0o644,
        )
        reject(
            lambda: immutable_canonical_snapshot(
                mutable, SOURCE_REVIEW_BUNDLE_SCHEMA, "mutable review"
            ),
            "mutable input",
        )
        symlink = root / "review-symlink.json"
        symlink.symlink_to(bundle_path)
        reject(
            lambda: immutable_canonical_snapshot(
                symlink, SOURCE_REVIEW_BUNDLE_SCHEMA, "symlink review"
            ),
            "symlink input",
        )
        hardlink = root / "review-hardlink.json"
        os.link(bundle_path, hardlink)
        reject(
            lambda: immutable_canonical_snapshot(
                bundle_path, SOURCE_REVIEW_BUNDLE_SCHEMA, "hard-linked review"
            ),
            "hard-linked input",
        )

    with tempfile.TemporaryDirectory(prefix="asterism-cargo-config-static-") as temporary:
        root = Path(temporary).resolve()
        source_root = root / "source"
        cargo_home = root / "cargo-home"
        (source_root / ".cargo").mkdir(parents=True)
        cargo_home.mkdir()
        config_toolchain = {"cargo_home_path": str(cargo_home)}
        config_authority = sandboxed_cargo_config_search(
            source_root, config_toolchain
        )
        assert config_authority["cwd"] == GUEST_SOURCE
        assert config_authority["cargo_home_path"] == GUEST_CARGO_HOME
        assert [entry["path"] for entry in config_authority["entries"]] == [
            f"{GUEST_SOURCE}/.cargo/config.toml",
            f"{GUEST_SOURCE}/.cargo/config",
            f"{GUEST_ROOT}/.cargo/config.toml",
            f"{GUEST_ROOT}/.cargo/config",
            "/.cargo/config.toml",
            "/.cargo/config",
            f"{GUEST_CARGO_HOME}/config.toml",
            f"{GUEST_CARGO_HOME}/config",
        ]
        effective = {
            entry["path"]: (entry["status"], entry["sha256"])
            for entry in config_authority["entries"]
        }
        assert all(
            effective[path] == ("present", EMPTY_SHA256)
            for path in GUEST_BOUND_CONFIG_PATHS
        )
        assert all(
            effective[path] == ("absent", None)
            for path in (
                f"{GUEST_ROOT}/.cargo/config.toml",
                f"{GUEST_ROOT}/.cargo/config",
                "/.cargo/config.toml",
                "/.cargo/config",
            )
        )
        config_manifest_path = root / "cargo-config.json"
        config_binding = write_sandboxed_cargo_config_search(
            config_manifest_path, source_root, config_toolchain
        )
        replay_sandboxed_cargo_config_search(
            config_binding, source_root, config_toolchain
        )
        assert hash_file(
            sandboxed_empty_cargo_config_path(config_manifest_path)
        ) == EMPTY_SHA256
        config_symlink = source_root / ".cargo/config.toml"
        config_symlink.symlink_to(cargo_home)
        reject(
            lambda: sandboxed_cargo_config_search(
                source_root, config_toolchain
            ),
            "sandboxed Cargo config symlink",
        )

    base_descriptors = {
        "source": 10,
        "target": 11,
        "toolchain_root": 12,
        "cargo": 13,
        "rustc": 14,
        "cargo_home": 15,
        "dev_null": 16,
        "rust_lld": 17,
    }
    base_system_descriptors = {
        guest_path: descriptor
        for (_host_path, guest_path), descriptor in zip(
            TRUSTED_SYSTEM_MOUNTS, range(18, 21), strict=True
        )
    }
    base_config_descriptors = {
        guest_path: descriptor
        for guest_path, descriptor in zip(
            GUEST_BOUND_CONFIG_PATHS, range(21, 25), strict=True
        )
    }
    def static_tree(role: str, digest: str, path: str) -> dict[str, Any]:
        return {
            "entry_count": 10,
            "equal_pre_post": True,
            "manifest_path": path,
            "manifest_sha256": digest,
            "mutation_events_absent": True,
            "role": role,
            "schema": RECURSIVE_TREE_AUTHORITY_SCHEMA,
            "watch_count": 3,
        }

    runtime_components = {
        "cargo_home": static_tree("cargo_home", "a" * SHA256, "/host/cargo.json"),
        "toolchain": static_tree("toolchain", "b" * SHA256, "/host/toolchain.json"),
        "trusted_system_closure": {
            "entry_count": 30,
            "manifest_path": "/host/system.json",
            "mounts": [
                {
                    "device": 1,
                    "gid": 0,
                    "guest_path": guest_path,
                    "host_path": guest_path,
                    "inode": index + 1,
                    "permissions": 0o755,
                    "resolved_path": guest_path,
                    "trusted_root_owned_non_writable": True,
                    "uid": 0,
                }
                for index, (_host_path, guest_path) in enumerate(
                    TRUSTED_SYSTEM_MOUNTS
                )
            ],
            "mutation_events_absent": True,
            "schema": TRUSTED_SYSTEM_CLOSURE_SCHEMA,
            "sha256": "c" * SHA256,
            "watch_count": 9,
        },
    }
    semantic_authority = {
        **runtime_components,
        "runtime_sha256": semantic_runtime_sha256(runtime_components),
        "schema": SEMANTIC_INPUT_AUTHORITY_SCHEMA,
        "source": static_tree("source", "d" * SHA256, "/host/source.json"),
    }
    assert validate_semantic_input_authority(
        semantic_authority, "static semantic authority"
    ) == semantic_authority
    scalar_mount = json.loads(json.dumps(semantic_authority))
    scalar_mount["trusted_system_closure"]["mounts"][0] = "forged"
    reject(
        lambda: validate_semantic_input_authority(
            scalar_mount, "static scalar closure mount"
        ),
        "scalar trusted-system closure mount",
    )
    short_mounts = json.loads(json.dumps(semantic_authority))
    short_mounts["trusted_system_closure"]["mounts"].pop()
    reject(
        lambda: validate_semantic_input_authority(
            short_mounts, "static short closure mounts"
        ),
        "short trusted-system closure mount list",
    )
    forged_mount_paths = json.loads(json.dumps(semantic_authority))
    forged_mount_paths["trusted_system_closure"]["mounts"][0][
        "host_path"
    ] = "/forged"
    forged_mount_paths["trusted_system_closure"]["mounts"][0][
        "resolved_path"
    ] = "/forged"
    reject(
        lambda: validate_semantic_input_authority(
            forged_mount_paths, "static forged closure mount path"
        ),
        "forged trusted-system closure mount path",
    )
    forged_runtime_components = {
        name: forged_mount_paths[name]
        for name in ("cargo_home", "toolchain", "trusted_system_closure")
    }
    reject(
        lambda: semantic_runtime_sha256(forged_runtime_components),
        "forged trusted-system runtime mount path",
    )
    resolution_descriptors = {
        name: descriptor
        for name, descriptor in base_descriptors.items()
        if name not in {"target", "dev_null", "rust_lld"}
    }
    resolution_argv = sandboxed_resolution_argv(
        resolution_descriptors,
        base_system_descriptors,
        base_config_descriptors,
        ["generate-lockfile", "--offline"],
        "/usr/bin/bwrap",
    )
    assert all(
        forbidden not in resolution_argv
        for forbidden in ("--dev", "--dev-bind", "--proc")
    )
    assert ["--dir", "/dev"] == resolution_argv[
        resolution_argv.index("/dev") - 1 : resolution_argv.index("/dev") + 1
    ]
    assert ["--dir", "/proc"] == resolution_argv[
        resolution_argv.index("/proc") - 1 : resolution_argv.index("/proc") + 1
    ]
    observed_resolution_descriptors = resolution_sandbox_descriptors(
        resolution_argv, "static resolution sandbox"
    )
    assert observed_resolution_descriptors == (
        resolution_descriptors,
        base_system_descriptors,
        base_config_descriptors,
    )
    extra_resolution_binding = [
        *resolution_argv,
        "--ro-bind-fd",
        "99",
        "/forged",
    ]
    reject(
        lambda: resolution_sandbox_descriptors(
            extra_resolution_binding, "static extra resolution binding"
        ),
        "extra resolution descriptor binding",
    )
    aliased_resolution_binding = list(resolution_argv)
    first_descriptor = aliased_resolution_binding.index("--ro-bind-fd") + 1
    source_descriptor = aliased_resolution_binding.index("--bind-fd") + 1
    aliased_resolution_binding[source_descriptor] = aliased_resolution_binding[
        first_descriptor
    ]
    reject(
        lambda: resolution_sandbox_descriptors(
            aliased_resolution_binding, "static aliased resolution binding"
        ),
        "aliased resolution descriptor binding",
    )
    overlay_source = resolution_argv.index("--overlay-src") + 1
    aliased_resolution_overlay = list(resolution_argv)
    aliased_resolution_overlay[overlay_source] = (
        f"/proc/self/fd/{resolution_argv[first_descriptor]}"
    )
    reject(
        lambda: resolution_sandbox_descriptors(
            aliased_resolution_overlay,
            "static aliased resolution Cargo-home overlay",
        ),
        "aliased resolution Cargo-home overlay descriptor",
    )
    noncanonical_resolution_overlay = list(resolution_argv)
    noncanonical_resolution_overlay[overlay_source] = "/proc/self/fd/0409"
    reject(
        lambda: resolution_sandbox_descriptors(
            noncanonical_resolution_overlay,
            "static noncanonical resolution Cargo-home overlay",
        ),
        "noncanonical resolution Cargo-home overlay descriptor",
    )
    oversized_resolution_overlay = list(resolution_argv)
    oversized_resolution_overlay[overlay_source] = "/proc/self/fd/12345678901"
    reject(
        lambda: resolution_sandbox_descriptors(
            oversized_resolution_overlay,
            "static oversized resolution Cargo-home overlay",
        ),
        "oversized resolution Cargo-home overlay descriptor",
    )
    wrong_resolution_overlay = list(resolution_argv)
    wrong_resolution_overlay[overlay_source + 2] = "/asterism/other-home"
    reject(
        lambda: resolution_sandbox_descriptors(
            wrong_resolution_overlay,
            "static wrong resolution Cargo-home overlay",
        ),
        "wrong resolution Cargo-home overlay destination",
    )
    def static_execution_file(path: str, digest: str, seed: int) -> dict[str, Any]:
        return {
            "identity": {
                "changed_ns": seed,
                "device": seed,
                "inode": seed,
                "link_count": 1,
                "modified_ns": seed,
            },
            "mode": 0o555,
            "path": path,
            "sha256": digest,
            "size": seed,
        }

    static_parent_chain = [
        {
            "changed_ns": seed,
            "device": seed,
            "gid": 0,
            "inode": seed,
            "link_count": 1,
            "mode": 0o755,
            "modified_ns": seed,
            "path": path,
            "size": seed,
            "type": stat.S_IFDIR,
            "uid": 0,
        }
        for seed, path in ((1, "/"), (2, "/dev"))
    ]
    base_attestation = {
        "build_argv": sandboxed_build_argv(
            base_descriptors,
            base_system_descriptors,
            base_config_descriptors,
            "mess-store",
            "asterism_rebaseline_public",
            "/usr/bin/bwrap",
            f"{GUEST_TOOLCHAIN_ROOT}/lib/rustlib/x86_64-unknown-linux-gnu/bin/gcc-ld/ld.lld",
        ),
        "build_env": {"ASTERISM_BUILD_SOURCE_APPROVAL_SHA256": "4" * SHA256},
        "build_nonce": "5" * SHA256,
        "cargo_config_search": {
            "path": "/host/cargo-config.json",
            "sha256": "8" * SHA256,
        },
        "cargo_lock_sha256": "6" * SHA256,
        "materialized_root": "/host/source",
        "semantic_input_authority": semantic_authority,
        "execution_tools": {
            "bwrap": static_execution_file("/usr/bin/bwrap", "1" * SHA256, 1),
            "cargo": static_execution_file(
                "/host/toolchain/bin/cargo", "2" * SHA256, 2
            ),
            "dev_null": {
                "identity": {
                    "changed_ns": 1,
                    "device": 1,
                    "gid": 0,
                    "inode": 1,
                    "link_count": 1,
                    "major": 1,
                    "minor": 3,
                    "modified_ns": 1,
                    "path": "/dev/null",
                    "permissions": 0o666,
                    "size": 0,
                    "type": stat.S_IFCHR,
                    "uid": 0,
                },
                "parent_path_chain": static_parent_chain,
                "trusted_system": True,
            },
            "rustc": static_execution_file(
                "/host/toolchain/bin/rustc", "3" * SHA256, 3
            ),
            "rust_lld": static_execution_file(
                "/host/toolchain/lib/rustlib/x86_64-unknown-linux-gnu/bin/rust-lld",
                "4" * SHA256,
                4,
            ),
            "toolchain_root": {
                "device": 1, "inode": 2, "link_count": 1, "mode": 0o755,
            },
        },
        "target_dir": "/host/target",
        "toolchain": {
            "bwrap_path": "/usr/bin/bwrap",
            "bwrap_sha256": "1" * SHA256,
            "cargo_path": "/host/toolchain/bin/cargo",
            "cargo_sha256": "2" * SHA256,
            "rustc_path": "/host/toolchain/bin/rustc",
            "rustc_sha256": "3" * SHA256,
            "rustc_host": "x86_64-unknown-linux-gnu",
            "rust_lld_path": (
                "/host/toolchain/lib/rustlib/x86_64-unknown-linux-gnu/bin/rust-lld"
            ),
            "rust_lld_sha256": "4" * SHA256,
        },
    }
    contract = {"schema": CONTRACT_SCHEMA, "source_approval_sha256": "4" * SHA256}
    ordinary = release_build_record(
        role="ordinary_a",
        artifact_role="published",
        attestation=base_attestation,
        contract=contract,
        approval_sha256="4" * SHA256,
        live_execution_tools=False,
    )
    relocated = json.loads(json.dumps(base_attestation))
    relocated["materialized_root"] = "/host/proof-source"
    relocated["target_dir"] = "/host/proof-target"
    for component in ("source", "cargo_home", "toolchain"):
        relocated["semantic_input_authority"][component]["manifest_path"] = (
            f"/relocated/{component}.json"
        )
    relocated["semantic_input_authority"]["trusted_system_closure"][
        "manifest_path"
    ] = "/relocated/system.json"
    relocated["build_argv"] = sandboxed_build_argv(
        {name: descriptor + 20 for name, descriptor in base_descriptors.items()},
        {
            name: descriptor + 20
            for name, descriptor in base_system_descriptors.items()
        },
        {
            name: descriptor + 20
            for name, descriptor in base_config_descriptors.items()
        },
        "mess-store",
        "asterism_rebaseline_public",
        "/usr/bin/bwrap",
        f"{GUEST_TOOLCHAIN_ROOT}/lib/rustlib/x86_64-unknown-linux-gnu/bin/gcc-ld/ld.lld",
    )
    overlay = release_build_record(
        role="overlay_a",
        artifact_role="proof_only",
        attestation=relocated,
        contract=contract,
        approval_sha256="4" * SHA256,
        live_execution_tools=False,
    )
    for field in (
        "source_approval_sha256",
        "contract_sha256",
        "build_nonce",
        "cargo_lock_sha256",
        "toolchain_sha256",
        "build_environment_sha256",
        "sandbox_sha256",
        "cfg_test",
        "rustc_workspace_wrapper",
    ):
        assert ordinary[field] == overlay[field]
    config_drift = json.loads(json.dumps(relocated))
    config_drift["cargo_config_search"]["sha256"] = "9" * SHA256
    assert release_build_record(
        role="overlay_a",
        artifact_role="proof_only",
        attestation=config_drift,
        contract=contract,
        approval_sha256="4" * SHA256,
        live_execution_tools=False,
    )["sandbox_sha256"] != ordinary["sandbox_sha256"]
    rejected += 1
    runtime_drift = json.loads(json.dumps(relocated))
    runtime_drift["semantic_input_authority"]["toolchain"][
        "manifest_sha256"
    ] = "e" * SHA256
    runtime_drift_components = {
        name: runtime_drift["semantic_input_authority"][name]
        for name in ("cargo_home", "toolchain", "trusted_system_closure")
    }
    runtime_drift["semantic_input_authority"]["runtime_sha256"] = (
        semantic_runtime_sha256(runtime_drift_components)
    )
    assert release_build_record(
        role="overlay_a",
        artifact_role="proof_only",
        attestation=runtime_drift,
        contract=contract,
        approval_sha256="4" * SHA256,
        live_execution_tools=False,
    )["sandbox_sha256"] != ordinary["sandbox_sha256"]
    rejected += 1
    for forbidden in (
        "RUSTFLAGS",
        "CARGO_ENCODED_RUSTFLAGS",
        "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
    ):
        hostile = json.loads(json.dumps(base_attestation))
        hostile["build_env"][forbidden] = "forged"
        reject(
            lambda hostile=hostile: release_build_record(
                role="ordinary_a",
                artifact_role="published",
                attestation=hostile,
                contract=contract,
                approval_sha256="4" * SHA256,
                live_execution_tools=False,
            ),
            f"release environment {forbidden}",
        )
    sandbox_drift = json.loads(json.dumps(relocated))
    sandbox_drift["build_argv"].append("--share-net")
    reject(
        lambda: release_build_record(
            role="overlay_a",
            artifact_role="proof_only",
            attestation=sandbox_drift,
            contract=contract,
            approval_sha256="4" * SHA256,
            live_execution_tools=False,
        ),
        "extra sandbox argument",
    )
    guest_path_drift = json.loads(json.dumps(relocated))
    guest_path_drift["build_argv"] = [
        "/asterism/proof-source" if item == GUEST_SOURCE else item
        for item in guest_path_drift["build_argv"]
    ]
    reject(
        lambda: release_build_record(
            role="overlay_a",
            artifact_role="proof_only",
            attestation=guest_path_drift,
            contract=contract,
            approval_sha256="4" * SHA256,
            live_execution_tools=False,
        ),
        "guest source path drift",
    )
    float_null_minor = json.loads(json.dumps(base_attestation))
    float_null_minor["execution_tools"]["dev_null"]["identity"]["minor"] = 3.0
    reject(
        lambda: release_build_record(
            role="ordinary_a",
            artifact_role="published",
            attestation=float_null_minor,
            contract=contract,
            approval_sha256="4" * SHA256,
            live_execution_tools=False,
        ),
        "float null-device identity",
    )
    rust_lld_binding_drift = json.loads(json.dumps(base_attestation))
    rust_lld_binding_drift["execution_tools"]["rust_lld"]["path"] = (
        rust_lld_binding_drift["toolchain"]["rustc_path"]
    )
    reject(
        lambda: release_build_record(
            role="ordinary_a",
            artifact_role="published",
            attestation=rust_lld_binding_drift,
            contract=contract,
            approval_sha256="4" * SHA256,
            live_execution_tools=False,
        ),
        "rust-lld execution binding drift",
    )
    path_like_rustc_host = json.loads(json.dumps(base_attestation))
    path_like_rustc_host["toolchain"]["rustc_host"] = "../escape"
    reject(
        lambda: release_build_record(
            role="ordinary_a",
            artifact_role="published",
            attestation=path_like_rustc_host,
            contract=contract,
            approval_sha256="4" * SHA256,
            live_execution_tools=False,
        ),
        "path-like rustc host",
    )
    duplicate_descriptors = dict(base_descriptors)
    duplicate_descriptors["rustc"] = duplicate_descriptors["cargo"]
    reject(
        lambda: sandboxed_build_argv(
            duplicate_descriptors,
            base_system_descriptors,
            base_config_descriptors,
            "mess-store",
            "asterism_rebaseline_public",
            "/usr/bin/bwrap",
            f"{GUEST_TOOLCHAIN_ROOT}/lib/rustlib/x86_64-unknown-linux-gnu/bin/gcc-ld/ld.lld",
        ),
        "duplicate sandbox descriptors",
    )
    low_descriptors = dict(base_descriptors)
    low_descriptors["source"] = 0
    reject(
        lambda: sandboxed_build_argv(
            low_descriptors,
            base_system_descriptors,
            base_config_descriptors,
            "mess-store",
            "asterism_rebaseline_public",
            "/usr/bin/bwrap",
            f"{GUEST_TOOLCHAIN_ROOT}/lib/rustlib/x86_64-unknown-linux-gnu/bin/gcc-ld/ld.lld",
        ),
        "non-passable sandbox descriptor",
    )
    duplicate_config_descriptors = dict(base_config_descriptors)
    duplicate_config_descriptors[GUEST_BOUND_CONFIG_PATHS[-1]] = (
        duplicate_config_descriptors[GUEST_BOUND_CONFIG_PATHS[0]]
    )
    reject(
        lambda: sandboxed_build_argv(
            base_descriptors,
            base_system_descriptors,
            duplicate_config_descriptors,
            "mess-store",
            "asterism_rebaseline_public",
            "/usr/bin/bwrap",
            f"{GUEST_TOOLCHAIN_ROOT}/lib/rustlib/x86_64-unknown-linux-gnu/bin/gcc-ld/ld.lld",
        ),
        "duplicate Cargo config descriptors",
    )
    aliased_config_descriptors = dict(base_config_descriptors)
    aliased_config_descriptors[GUEST_BOUND_CONFIG_PATHS[0]] = base_descriptors[
        "cargo"
    ]
    reject(
        lambda: sandboxed_build_argv(
            base_descriptors,
            base_system_descriptors,
            aliased_config_descriptors,
            "mess-store",
            "asterism_rebaseline_public",
            "/usr/bin/bwrap",
            f"{GUEST_TOOLCHAIN_ROOT}/lib/rustlib/x86_64-unknown-linux-gnu/bin/gcc-ld/ld.lld",
        ),
        "Cargo config/core descriptor alias",
    )
    assert rejected >= 29
    return rejected


def idempotent_freeze_static_self_test() -> None:
    evidence_schema = load_evidence_schema_self_test_module()
    assert (
        system_symlink_scope(Path("/usr/bin"), "tool", "/bin/sh")
        == "within_closure"
    )
    assert (
        system_symlink_scope(Path("/usr/bin"), "manual", "/usr/share/man")
        == "guest_inaccessible_external"
    )
    assert (
        system_symlink_scope(
            Path("/usr/lib"),
            "gcc/x86_64-pc-linux-gnu/15.3.0/libitm.so",
            "/usr/lib/libitm.so",
        )
        == "within_closure"
    )
    for target in (
        "/proc/self/status",
        "//proc/self/status",
        "//asterism/source",
        "/",
    ):
        try:
            system_symlink_scope(Path("/usr/bin"), "hostile", target)
        except PreparationError:
            pass
        else:
            raise PreparationError(
                "trusted system symlink reached guest-accessible authority"
            )
    with tempfile.TemporaryDirectory(prefix="bn-ecm1-prepare-freeze-") as temporary:
        temporary_root = Path(temporary).resolve(strict=True)
        ordering_root = temporary_root / "ordering"
        ordering_root.mkdir()
        (ordering_root / "a").mkdir()
        (ordering_root / "a-b").mkdir()
        (ordering_root / "crates" / "mess").mkdir(parents=True)
        (ordering_root / "crates" / "mess-bench").mkdir()
        atomic_write(ordering_root / "a" / "input", b"a\n", mode=0o444)
        atomic_write(ordering_root / "a-b" / "input", b"a-b\n", mode=0o444)
        atomic_write(
            ordering_root / "crates" / "mess" / "x", b"x\n", mode=0o444
        )
        atomic_write(
            ordering_root / "crates" / "mess-bench" / "y",
            b"y\n",
            mode=0o444,
        )
        with RecursiveTreeAuthorityGuard(
            ordering_root,
            temporary_root / "ordering.json",
            "source",
            "prefix-sibling ordering fixture",
            allow_internal_symlinks=False,
        ) as ordering_guard:
            assert ordering_guard.initial_manifest is not None
            ordering_manifest = ordering_guard.initial_manifest
            evidence_schema._validate_recursive_semantic_manifest(
                ordering_manifest,
                "source",
                "prefix-sibling ordering fixture",
                trusted_system=False,
            )
            ordering_paths = [
                entry["path"] for entry in ordering_manifest["entries"]
            ]
            if ordering_paths != [
                ".",
                "a",
                "a-b",
                "a-b/input",
                "a/input",
                "crates",
                "crates/mess",
                "crates/mess-bench",
                "crates/mess-bench/y",
                "crates/mess/x",
            ]:
                raise PreparationError(
                    "prefix-sibling recursive path order differs"
                )
        root = temporary_root / "source"
        root.mkdir()
        outside = temporary_root / "outside"
        outside.write_bytes(b"outside")
        if (
            recursive_symlink_scope(
                Path("/usr/lib"),
                "gcc/x86_64-pc-linux-gnu/15.3.0/libitm.so",
                "/usr/lib/libitm.so",
                trusted_system=True,
            )
            != "within_closure"
        ):
            raise PreparationError("trusted dangling symlink was rejected")
        for target in ("missing", "../outside"):
            try:
                recursive_symlink_scope(
                    root,
                    "link",
                    target,
                    trusted_system=False,
                )
            except PreparationError:
                pass
            else:
                raise PreparationError(
                    "nontrusted dangling or escaping symlink was accepted"
                )

        class FixtureTrustedTreeGuard(RecursiveTreeAuthorityGuard):
            def _trusted(
                self,
                metadata: os.stat_result,
                relative: str,
                *,
                symlink: bool,
            ) -> None:
                return None

        trusted_root = temporary_root / "trusted"
        trusted_root.mkdir()
        (trusted_root / "dangling").symlink_to("/usr/lib/libitm.so")
        with FixtureTrustedTreeGuard(
            trusted_root,
            temporary_root / "trusted.json",
            "system-usr-lib",
            "trusted dangling fixture",
            allow_internal_symlinks=True,
            hash_regular_contents=False,
            trusted_system_roots=(Path("/usr/lib"),),
        ) as trusted_guard:
            assert trusted_guard.initial_manifest is not None
            dangling = next(
                entry
                for entry in trusted_guard.initial_manifest["entries"]
                if entry["path"] == "dangling"
            )
            if dangling["symlink_scope"] != "within_closure":
                raise PreparationError(
                    "trusted guard rejected dangling closure symlink"
                )
        atomic_write(root / "input.rs", b"fn main() {}\n", mode=0o444)
        root.chmod(0o555)
        before = resample_recursive_manifest(
            root,
            "source",
            "prepare freeze before",
            allow_internal_symlinks=False,
            hash_regular_contents=True,
        )
        make_read_only(root)
        after = resample_recursive_manifest(
            root,
            "source",
            "prepare freeze after",
            allow_internal_symlinks=False,
            hash_regular_contents=True,
        )
        if before != after:
            raise PreparationError(
                "idempotent final freeze changed recursive evidence"
            )
        root.chmod(0o755)

        metadata_root = temporary_root / "metadata-only"
        metadata_root.mkdir()
        atomic_write(metadata_root / "unreadable", b"metadata only\n", mode=0o000)
        metadata_root.chmod(0o555)
        metadata_manifest = resample_recursive_manifest(
            metadata_root,
            "metadata_only",
            "metadata-only unreadable regular",
            allow_internal_symlinks=False,
            hash_regular_contents=False,
        )
        unreadable = next(
            entry
            for entry in metadata_manifest["entries"]
            if entry["path"] == "unreadable"
        )
        if unreadable["permissions"] != 0 or unreadable["sha256"] is not None:
            raise PreparationError(
                "metadata-only unreadable regular was not retained without content access"
            )


def support_import_bytecode_static_self_test() -> None:
    """Prove the isolated support import cannot write bytecode caches."""
    with tempfile.TemporaryDirectory() as scratch:
        root = Path(scratch) / "support"
        root.mkdir()
        (root / "evidence_schema.py").write_text("VALUE = 1\n", encoding="utf-8")
        before = file_manifest(root)
        import_program = (
            "import sys;"
            f"sys.path.insert(0,{json.dumps(str(root.resolve()))});"
            "import evidence_schema"
        )
        python = str(Path(sys.executable).resolve())
        environment = frozen_runtime_environment({"PYTHONDONTWRITEBYTECODE": "1"})
        result = subprocess.run(
            [python, "-I", "-B", "-c", import_program],
            cwd=root,
            env=environment,
            capture_output=True,
            timeout=30,
        )
        if result.returncode != 0:
            raise AssertionError(
                f"support import self-test failed: {result.stderr.decode(errors='replace')}"
            )
        if file_manifest(root) != before:
            raise AssertionError("support import with -B wrote bytecode cache")
        # Without -B the same import must mutate the tree: proves -B is
        # load-bearing rather than redundant with the environment binding.
        hostile = subprocess.run(
            [python, "-I", "-c", import_program],
            cwd=root,
            env=environment,
            capture_output=True,
            timeout=30,
        )
        if hostile.returncode != 0:
            raise AssertionError("hostile support import unexpectedly failed")
        if file_manifest(root) == before:
            raise AssertionError(
                "hostile -I-only import did not write bytecode; self-test is vacuous"
            )


def static_self_test() -> None:
    idempotent_freeze_static_self_test()
    support_import_bytecode_static_self_test()
    assert preapproval_nm_authority_static_self_test() == 12
    assert canonical_encoding_boundary_static_self_test() == 10
    source_review_and_compile_out_static_self_test()
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
    assert len(shared_manifest()["entries"]) == 8
    assert len({entry["sha256"] for entry in shared_manifest()["entries"]}) == 8
    assert hash_file(PLAN_PATH) == hash_bytes(PLAN_PATH.read_bytes())
    assert hash_file(PROTOCOL_DOCUMENT) == PROTOCOL_DOCUMENT_SHA256
    assert hash_file(HISTORICAL_BASELINE) == HISTORICAL_BASELINE_SHA256
    assert hash_file(CURRENT_PRODUCT_OVERLAY) == CURRENT_PRODUCT_OVERLAY_SHA256
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
    semantic_source = (SHARED_SOURCE / "semantic_oracle.rs").read_text()
    validate_correctness_oracle_control_source(public)
    validate_semantic_oracle_integration_sources(public, semantic_source)
    validate_current_correctness_construction()
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
    hostile_semantic_integrations = (
        (
            public,
            replace_exact_once(
                semantic_source,
                "domain_events:  4",
                "domain_events:  6",
                "shared semantic domain count mutation",
            ),
        ),
        (
            public,
            replace_exact_once(
                semantic_source,
                'const EVENT_NAME: &str = "asterism.rebaseline.event";',
                'const EVENT_NAME: &str = "asterism.rebaseline.oracle-event";',
                "shared semantic event identity mutation",
            ),
        ),
        (
            replace_exact_once(
                public,
                "EventStore::new(backend).with_page_size(16);",
                "EventStore::new(backend).with_page_size(16).with_cache_capacity(16);",
                "historical oracle cache mutation",
            ),
            semantic_source,
        ),
        (
            replace_exact_once(
                public,
                "semantic_oracle::run_generation_neutral_semantic_oracle(&store)",
                "semantic_oracle::run_generation_neutral_semantic_oracle(&group_store)",
                "historical oracle shared-call mutation",
            ),
            semantic_source,
        ),
        (
            replace_exact_once(
                public,
                """        observations.domain_events,
        observations.public_appends,
        observations.fresh_streams,""",
                """        observations.public_appends,
        observations.domain_events,
        observations.fresh_streams,""",
                "historical oracle accounting swap",
            ),
            semantic_source,
        ),
    )
    for hostile_public, hostile_semantic in hostile_semantic_integrations:
        try:
            validate_semantic_oracle_integration_sources(
                hostile_public, hostile_semantic
            )
        except PreparationError:
            pass
        else:
            raise AssertionError(
                "hostile shared semantic-oracle integration was accepted"
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
        "semantic_oracle::run_generation_neutral_semantic_oracle(&store)",
        "observations.domain_events",
        "observations.public_appends",
        "observations.fresh_streams",
        "adapter::assert_oracle_accounting(&group_engine, 1, 1, 1, true);",
        'assert_eq!(arguments[0], "--run-row");',
        'required("ASTERISM_REBASELINE_ROW_ORDINAL")',
        'required("ASTERISM_REBASELINE_CONFIG")',
    ):
        assert marker in public, f"public integration marker absent: {marker}"
    for marker in (
        "Err(AppendError::Conflict {",
        "Err(AppendError::Backend(_))",
        "domain_events:  4",
        "fresh_streams:  2",
        "public_appends: 3",
    ):
        assert marker in semantic_source, (
            f"shared semantic oracle marker absent: {marker}"
        )
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
    comment_fixture = """const WIRE: &[u8] = b"// literal /* bytes */"; // decoy
const WORDS: &str = r#"return break continue panic! exit("#;
/* outer comment
   /* nested comment */
*/
const LIVE: usize = 5;
"""
    stripped_fixture = strip_rust_comments(comment_fixture, "Rust comment fixture")
    assert len(stripped_fixture) == len(comment_fixture)
    assert stripped_fixture.count("\n") == comment_fixture.count("\n")
    assert 'b"// literal /* bytes */"' in stripped_fixture
    assert "decoy" not in stripped_fixture and "outer comment" not in stripped_fixture
    assert "const LIVE: usize = 5;" in stripped_fixture
    literal_blanked_fixture = blank_rust_literals(
        stripped_fixture, "Rust literal fixture"
    )
    reject_rust_control_transfers(literal_blanked_fixture, "Rust literal fixture")
    pinned_manifest = shared_manifest()
    assert {
        entry["name"]: entry["sha256"] for entry in pinned_manifest["entries"]
    } == PINNED_SHARED_OVERLAY_SHA256
    control_payload = (SHARED_SOURCE / "control.rs").read_bytes()
    validate_shared_control_child_source(control_payload)
    try:
        validate_shared_control_child_source(
            control_payload,
            expected_sha256="0" * 64,
        )
    except PreparationError:
        pass
    else:
        raise AssertionError("wrong shared control pin was accepted")
    control = control_payload.decode("utf-8")
    measured_signature = """pub fn measured_and_wait_release(
        &mut self,
        nonce: &Nonce,
        markers: MeasuredMarkers,
    ) {"""
    perf_disable_block = """        if let Some(perf_disable) = self.perf.measured_value(nonce) {
            fields.push(("perf_disable", perf_disable));
        }"""
    disable_ack_block = """        let mut ack = [0_u8; PERF_ACK_WIRE.len()];
        pipes.ack.read_exact(&mut ack).expect("read exact perf disable ACK");
        assert_eq!(&ack, PERF_ACK_WIRE, "perf disable ACK differs");"""
    relocated_disable = replace_exact_once(
        control,
        disable_ack_block,
        "",
        "remove live perf ACK block for dead relocation",
    )
    relocated_disable = replace_exact_once(
        relocated_disable,
        """        pipes.command.flush().expect("flush perf disable");

        let ack_received_monotonic_ns = monotonic_ns();""",
        f"""        pipes.command.flush().expect("flush perf disable");
        if true {{ return; }}
{disable_ack_block}
        let ack_received_monotonic_ns = monotonic_ns();""",
        "relocate sole perf ACK block after return",
    )
    hidden_macro_control = replace_exact_once(
        control,
        disable_ack_block,
        "        review_early_return!();\n" + disable_ack_block,
        "invoke hidden-return macro before perf ACK",
    )
    hidden_macro_control = (
        "macro_rules! review_early_return { () => { return; }; }\n"
        + hidden_macro_control
    )
    custom_read_exact_control = replace_exact_once(
        control,
        "use std::io::{BufRead as _, BufReader, Read as _, Write as _};",
        """use std::io::{BufRead as _, BufReader, Write as _};
trait ReviewReadExact {
    fn read_exact(&mut self, buffer: &mut [u8]) -> std::io::Result<()>;
}
impl ReviewReadExact for File {
    fn read_exact(&mut self, buffer: &mut [u8]) -> std::io::Result<()> {
        buffer.copy_from_slice(PERF_ACK_WIRE);
        Ok(())
    }
}""",
        "shared perf custom read_exact rebinding",
    )
    assert_alias_control = replace_exact_once(
        control,
        "use std::fmt::Write as _;",
        """use std::fmt::Write as _;
use std::debug_assert_eq as assert_eq;""",
        "shared perf assert_eq macro alias",
    )
    fabricated_clock_control = replace_exact_once(
        control,
        "        let sent_monotonic_ns = monotonic_ns();",
        """        let mut fabricated = t1_monotonic_ns;
        let mut monotonic_ns = || {
            fabricated += 1;
            fabricated
        };
        let sent_monotonic_ns = monotonic_ns();""",
        "shared perf monotonic clock closure shadow",
    )
    hostile_control_sources = (
        control.replace(
            '("context_sha256", json_string(&self.context_sha256)),\n',
            "",
            1,
        ),
        custom_read_exact_control,
        assert_alias_control,
        fabricated_clock_control,
        replace_exact_once(
            control,
            measured_signature,
            measured_signature + "\n        return;",
            "shared measured function-start return",
        ),
        replace_exact_once(
            control,
            measured_signature,
            measured_signature + "\n        if true { return; }",
            "shared measured constant-true return",
        ),
        replace_exact_once(
            control,
            '        pipes.command.write_all(b"disable\\n")',
            '        return;\n        pipes.command.write_all(b"disable\\n")',
            "shared perf disable mid-body return",
        ),
        replace_exact_once(
            control,
            perf_disable_block,
            "        return;\n" + perf_disable_block,
            "shared measured return before perf-disable insertion",
        ),
        relocated_disable,
        replace_exact_once(
            control,
            disable_ack_block,
            """        const PERF_ACK_WIRE: &[u8; 4] = b"ack\\n";
"""
            + disable_ack_block,
            "shared perf local-const ACK shadow",
        ),
        replace_exact_once(
            control,
            disable_ack_block,
            """        let PERF_ACK_WIRE: &[u8; 4] = b"ack\\n";
"""
            + disable_ack_block,
            "shared perf local-let ACK shadow",
        ),
        hidden_macro_control,
        replace_exact_once(
            control,
            'const PERF_ACK_WIRE: &[u8; 5] = b"ack\\n\\0";',
            'const PERF_ACK_WIRE: &[u8; 4] = b"ack\\n";',
            "shared perf four-byte ACK wire",
        ),
        replace_exact_once(
            control,
            'const PERF_ACK_WIRE: &[u8; 5] = b"ack\\n\\0";',
            """const PERF_ACK_WIRE: &[u8; 4] = b"ack\\n";
// const PERF_ACK_WIRE: &[u8; 5] = b"ack\\n\\0";""",
            "shared perf comment-only five-byte ACK decoy",
        ),
        replace_exact_once(
            control,
            """        let mut ack = [0_u8; PERF_ACK_WIRE.len()];
        pipes.ack.read_exact(&mut ack).expect("read exact perf disable ACK");
        assert_eq!(&ack, PERF_ACK_WIRE, "perf disable ACK differs");""",
            """        let _ack = PERF_ACK_WIRE;
        // let mut ack = [0_u8; PERF_ACK_WIRE.len()];
        // pipes.ack.read_exact(&mut ack).expect("read exact perf disable ACK");
        // assert_eq!(&ack, PERF_ACK_WIRE, "perf disable ACK differs");""",
            "shared perf comment-only read/assert decoy",
        ),
        replace_exact_once(
            control,
            """        if let Some(perf_disable) = self.perf.measured_value(nonce) {
            fields.push(("perf_disable", perf_disable));
        }""",
            """        if false {
        if let Some(perf_disable) = self.perf.measured_value(nonce) {
            fields.push(("perf_disable", perf_disable));
        }
        }""",
            "shared perf-disable if-false wrapper",
        ),
        replace_exact_once(
            control,
            ".write_all(PERF_ACK_LEDGER_ENTRY)",
            ".write_all(&ack)",
            "shared perf wire ACK ledger leak",
        ),
        replace_exact_once(
            control,
            """        pipes.command.write_all(b"disable\\n").expect("write perf disable");
        pipes.command.flush().expect("flush perf disable");
        let mut ack = [0_u8; PERF_ACK_WIRE.len()];
        pipes.ack.read_exact(&mut ack).expect("read exact perf disable ACK");""",
            """        let mut ack = [0_u8; PERF_ACK_WIRE.len()];
        pipes.ack.read_exact(&mut ack).expect("read exact perf disable ACK");
        pipes.command.write_all(b"disable\\n").expect("write perf disable");
        pipes.command.flush().expect("flush perf disable");""",
            "shared perf disable command/ACK reorder",
        ),
        replace_exact_once(
            control,
            "let ack_received_monotonic_ns = monotonic_ns();",
            "let ack_received_monotonic_ns = sent_monotonic_ns + 1;",
            "shared perf fabricated ACK timestamp",
        ),
        replace_exact_once(
            control,
            'fields.push(("perf_disable", perf_disable));',
            'fields.insert(0, ("perf_disable", perf_disable));',
            "shared perf-disable nonlexical insertion",
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
        'const PERF_ACK_WIRE: &[u8; 5] = b"ack\\n\\0";',
        'const PERF_ACK_LEDGER_ENTRY: &[u8; 4] = b"ack\\n";',
        'pipes.command.write_all(b"disable\\n")',
        "let mut ack = [0_u8; PERF_ACK_WIRE.len()]",
        "pipes.ack.read_exact(&mut ack)",
        'assert_eq!(&ack, PERF_ACK_WIRE, "perf disable ACK differs")',
        ".write_all(PERF_ACK_LEDGER_ENTRY)",
        'fields.push(("perf_disable", perf_disable));',
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
    production_source = preparer[
        : preparer.index("def source_review_and_compile_out_static_self_test(")
    ]
    for marker in (
        "approval_snapshot, locks, tools, source_inputs, source_bundle = validate_approval(",
        "approval_sha256 = approval_snapshot.sha256",
        "atomic_write(bound_approval_path, approval_snapshot.payload, mode=0o444)",
        '"tools_manifest": tools,',
        '"tools_manifest_sha256": source_inputs["tools_manifest"].sha256,',
        "validate_source_review_authority(",
        '"release_compile_out_requirement": requirement,',
        'role="ordinary_a",\n                artifact_role="published",',
        'role="overlay_a",\n        artifact_role="proof_only",',
        "produce_release_compile_out(",
        'output / "bindings" / "source-review-bundle.json"',
        'output / "bindings" / "current-children-attestation.json"',
        'output / "bindings" / "lock-review-authority.json"',
        'output / "bindings" / "lock-review-bundle.json"',
        'output / "manifests" / "release-compile-out.json"',
        '"source_review": prepared_source_review,',
        '"release_compile_out": release_compile_out_binding,',
        "replay_artifact_binding(",
        '"mode": 0o444,\n            "path": str(bound_tools_manifest_path.resolve()),',
        'marker_templates = validate_trace_path_marker_templates(',
        '"trace_path_marker_templates": marker_templates,',
        'validate_attested_child(build_child, f"{variant} build")',
        'validate_attested_child(contract_child, f"{variant} contract")',
        'locks = source_inputs["lock_manifest"].value',
        "build_env = sandboxed_build_environment(",
        "execute_sandboxed_build(",
        "execute_sandboxed_resolution(",
        'executable=f"/proc/self/fd/{bwrap_descriptor}"',
        'source_guard.replay("post-resolution boundary")',
        'toolchain_guard.replay("post-resolution boundary")',
        'cargo_home_guard.replay("post-resolution boundary")',
        'system_guard.replay("post-resolution boundary")',
        'if (variant in {"C", "D"}) != isinstance(current_attempt, dict):',
        'if variant in {"A", "B"} and current_attempt is not None:',
        'or set(record) != expected_fields',
        'authority = record["semantic_input_authority"]',
        'if argv[0] != artifact_leases["bwrap"][2]["path"]:',
        'execution_path = f"/proc/self/fd/{artifact_leases[\'bwrap\'][0]}"',
        "executable=execution_path,",
        "record, stdout = retained_git_run(repository, arguments, toolchain)",
        "resolved, resolved_payload = retained_git_run(",
        "config_descriptors = {",
        'artifact_specs[f"config:{guest_path}"]',
        "authority_before_import = prepared_authority_manifest(output)",
        "frozen_authority = prepared_authority_manifest(output)",
    ):
        assert marker in production_source, (
            f"preparer authority marker absent: {marker}"
        )
    build_source = production_source[
        production_source.index("def build(") :
    ]
    resolution_source = production_source[
        production_source.index("def sandboxed_resolution_argv(") :
        production_source.index("def stage_locks(")
    ]
    release_sandbox_source = production_source[
        production_source.index("def sandboxed_build_argv(") :
        production_source.index("def expected_contract(")
    ]
    semantic_digest_source = production_source[
        production_source.index("def semantic_runtime_sha256(") :
        production_source.index("def system_symlink_scope(")
    ]
    semantic_validation_source = production_source[
        production_source.index("def validate_semantic_input_authority(") :
        production_source.index("def hash_file(")
    ]
    for marker in (
        '"mounts",',
        'mount.get("guest_path") != guest_path',
        'mount.get("host_path") != str(host_path)',
        'mount.get("resolved_path") != str(host_path)',
        '"semantic runtime trusted-system mounts differ"',
    ):
        assert marker in semantic_digest_source
    for marker in (
        'mount.get("guest_path") != guest_path',
        'mount.get("host_path") != str(host_path)',
        'mount.get("resolved_path") != str(host_path)',
        "immutable_builder_local_canonical_snapshot",
        "builder_local_canonical_json if builder_local_evidence else canonical_json",
    ):
        assert marker in semantic_validation_source
    assert '"--ro-bind",\n        "/",\n        "/"' not in resolution_source
    assert '"--bind",\n        str(root)' not in resolution_source
    for forbidden in ('"--dev-bind"', '"--dev"', '"--proc"'):
        assert forbidden not in resolution_source
    assert '"--dir",\n        "/dev"' in resolution_source
    assert '"--dir",\n        "/proc"' in resolution_source
    for forbidden in ('"--dev"', '"--proc"'):
        assert forbidden not in release_sandbox_source
    assert release_sandbox_source.count('"--dev-bind"') == 1
    assert (
        '"--dev-bind",\n        f"/proc/self/fd/{descriptors[\'dev_null\']}",\n        "/dev/null"'
        in release_sandbox_source
    )
    assert 'str(descriptors["rust_lld"])' in release_sandbox_source
    assert "rust_lld_guest_path" in release_sandbox_source
    assert '"--dir",\n        "/dev"' in release_sandbox_source
    assert '"--dir",\n        "/proc"' in release_sandbox_source
    sandbox_environment_source = production_source[
        production_source.index("def sandboxed_cargo_environment(") :
        production_source.index("def frozen_runtime_environment(")
    ]
    assert (
        '"GIT_CONFIG_GLOBAL": f"{GUEST_ROOT}/absent-gitconfig"'
        in sandbox_environment_source
    )
    assert (
        'environment["LD_ORIGIN_PATH"] = GUEST_TOOLCHAIN_BIN'
        in sandbox_environment_source
    )
    assert "toolchain[\"cargo_path\"]" not in resolution_source
    assert '"--bind-fd"' in resolution_source
    assert '"--ro-bind-fd"' in resolution_source
    assert "GUEST_CARGO" in resolution_source
    assert "def run(\n" not in production_source
    for marker in (
        "def system_symlink_scope(root: Path, relative: str, target: str) -> str:",
        'for authority in ("/asterism", "/dev", "/proc", "/run", "/sys", "/tmp")',
        '"trusted system symlink reaches mutable guest authority"',
        '"symlink_scope": symlink_scope',
        'else getattr(os, "O_PATH", 0)',
        "metadata-only file descriptors are unavailable",
    ):
        assert marker in production_source
    toolchain_source = production_source[
        production_source.index("def checked_tool(") :
        production_source.index("def validate_toolchain(")
    ]
    identity_source = toolchain_source[
        toolchain_source.index("def toolchain_identity(") :
    ]
    assert "subprocess.run(" not in identity_source
    for marker in (
        'executable=f"/proc/self/fd/{descriptor}"',
        "pass_fds=(descriptor,)",
        'retain("rustup", rustup)',
        'retain("bwrap", bwrap)',
        'retain("git", git)',
        'for component in ("cargo", "rustc"):',
        "retain(component, lexical)",
        "if active_replay != active_raw:",
        "which_replay != which_outputs[component]",
        "version_replay != version_outputs[component]",
        'verify_all("final toolchain identity")',
    ):
        assert marker in toolchain_source
    for forbidden in (
        "hash_file(bwrap)",
        "hash_file(cargo)",
        "hash_file(git)",
        "hash_file(rustc)",
        "hash_file(rustup)",
    ):
        assert forbidden not in toolchain_source
    approval_validation_source = production_source[
        production_source.index("def validate_approval(") : production_source.index(
            "def write_approval("
        )
    ]
    lock_validation_source = production_source[
        production_source.index("def validate_lock_manifest(") :
        production_source.index("def validate_current_children_authority(")
    ]
    current_children_validation_source = production_source[
        production_source.index("def validate_current_children_authority(") :
        production_source.index("def source_review_assertion(")
    ]
    assert "builder_local_evidence=True" in current_children_validation_source
    assert "builder_local_evidence=True" not in lock_validation_source
    assert 'if "semantic_input_authority" in record:' not in lock_validation_source
    assert 'claim.get("current_lock_attempt")' in lock_validation_source
    assert '"sandboxed_cargo_resolution"' in lock_validation_source
    assert '"tracked_git_readback"' in lock_validation_source
    for marker in (
        "resolution_sandbox_descriptors(",
        "validate_resolver_output_hashes(",
        'record.get("exit_status") != 0',
        'record.get("host_source_root") != str(expected_source_root)',
        'record.get("passed_file_descriptors") != 13',
        "replay_artifact_binding(",
        '"generate-lockfile", "--offline"',
        '"metadata",',
        "current_lock_sha256",
    ):
        assert marker in lock_validation_source
    reviewed_snapshot_offset = approval_validation_source.index(
        "validate_source_review_authority("
    )
    assert approval_validation_source.index(
        'locks = source_inputs["lock_manifest"].value'
    ) > reviewed_snapshot_offset
    assert "load_canonical(lock_manifest_path" not in approval_validation_source
    generic_git_source = production_source[
        production_source.index("def git_bytes(") : production_source.index(
            "def rust_item("
        )
    ]
    assert "subprocess.run(" not in generic_git_source[
        : generic_git_source.index("def retained_git_run(")
    ]
    for marker in (
        "open_retained_artifact(",
        'binding["sha256"] != toolchain["git_sha256"]',
        'executable=f"/proc/self/fd/{descriptor}"',
        "pass_fds=(descriptor,)",
        "verify_retained_artifact(",
    ):
        assert marker in generic_git_source
    assert "hash_file(approval_path)" not in build_source
    assert "approval_path.read_bytes()" not in build_source
    assert "--review" + "-id" not in preparer
    assert "--reviewed" + "-at" not in preparer
    approval_source = production_source[
        production_source.index("def write_approval(") : production_source.index(
            "def make_read_only("
        )
    ]
    assert "release_compile_out_path" not in approval_source
    assert "release-compile-out.json" not in approval_source
    assert build_source.index("build_product_overlay_a(") < build_source.index(
        'source_tools = tools.get("tools")'
    )
    support_import_offset = build_source.rindex(
        "validate_support_import_immutability("
    )
    freeze_offset = build_source.rindex("freeze_prepared_root(")
    assert support_import_offset < freeze_offset
    assert "run_attested(" not in build_source[freeze_offset:]
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
    build_environment = sandboxed_build_environment(toolchain, {})
    assert build_environment["LD_ORIGIN_PATH"] == GUEST_TOOLCHAIN_BIN
    assert "LD_ORIGIN_PATH" not in sandboxed_cargo_environment(toolchain, {})
    try:
        sandboxed_build_environment(
            toolchain, {"LD_ORIGIN_PATH": "/host/toolchain/bin"}
        )
    except PreparationError:
        pass
    else:
        raise AssertionError("sandboxed build loader origin override was accepted")
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
    parser.add_argument("--source-review-bundle", type=Path)
    parser.add_argument("--current-children-attestation", type=Path)
    parser.add_argument("--lock-authority", type=Path)
    parser.add_argument("--lock-review-bundle", type=Path)
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
            "output",
            "lock_manifest",
            "tools",
            "source_review_bundle",
            "current_children_attestation",
            "lock_authority",
            "lock_review_bundle",
        ):
            if getattr(arguments, name) is None:
                parser.error(f"write-approval requires --{name.replace('_', '-')}")
        write_approval(
            repository,
            arguments.lock_manifest.resolve(strict=True),
            arguments.tools.resolve(strict=True),
            arguments.output.resolve(),
            arguments.source_review_bundle.resolve(strict=True),
            arguments.current_children_attestation.resolve(strict=True),
            arguments.lock_authority.resolve(strict=True),
            arguments.lock_review_bundle.resolve(strict=True),
        )
    else:
        for name in (
            "output",
            "approval",
            "lock_manifest",
            "tools",
            "source_review_bundle",
            "current_children_attestation",
            "lock_authority",
            "lock_review_bundle",
        ):
            if getattr(arguments, name) is None:
                parser.error(f"build requires --{name.replace('_', '-')}")
        build(
            repository, arguments.output.resolve(), arguments.approval.resolve(strict=True),
            arguments.lock_manifest.resolve(strict=True), arguments.tools.resolve(strict=True),
            arguments.source_review_bundle.resolve(strict=True),
            arguments.current_children_attestation.resolve(strict=True),
            arguments.lock_authority.resolve(strict=True),
            arguments.lock_review_bundle.resolve(strict=True),
        )


if __name__ == "__main__":
    try:
        main()
    except (OSError, subprocess.SubprocessError, PreparationError, ValueError, json.JSONDecodeError) as error:
        print(f"prepare-overlays: {error}", file=os.sys.stderr)
        raise SystemExit(2) from error
