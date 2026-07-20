#!/usr/bin/python3
"""Build and attest the pre-approval current correctness/fault children.

The build is deliberately separate from the A/B/C/D performance build.  It
consumes an independently reviewed lock authority, materializes exact frozen A
archives, applies the sole cfg(test)-only product overlay, and performs three
fresh offline/no-network builds:

* an ordinary pristine A release example; and
* the same A release example with the product overlay applied but no test cfg;
  then
* the correctness/fault children through the selective workspace wrapper.

The latter pair must be byte-identical and have identical hook-free symbol
inventories.  Only then are ``ast-rb-check`` and ``ast-rb-fault`` substituted
into an otherwise complete v3 tools manifest.  This module never runs a child,
test, benchmark, measurement, evaluator, or evidence runner.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import io
import json
import os
import re
import stat
import struct
import subprocess
import sys
import tarfile
import tempfile
import types
from contextlib import ExitStack
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any, Callable, Iterable, Mapping, Sequence


PROTOCOL = "bn-2l3n-asterism-rebaseline-v3"
PROTOCOL_SHA256 = "d9ee10b2cccdaf6428bf1419a8c2ee74d272e987dc3617a80b64ad2e9d7a18dd"
PRODUCT_COMMIT = "d644dc583dfe6a3d2cd07e71ce0212a323875ab4"
PRODUCT_TREE = "205d853905bdb648ee997900c6aef24a323aa380"
PRODUCT_LOCK_SHA256 = "9c24189940d9b43d7798c6680c8aeab6ddc270ef9b450390334d9327405cbea0"
PRODUCT_ENGINE_SHA256 = "c995c27d8fff3e1ddfffdb700dfc94160a99ea0c7fe731017d3f1db99d7b59e7"
EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
PRODUCT_OVERLAY_VALIDATOR_SCHEMA = "bn-xfw3-product-test-overlay-validator-v1"
ATTESTATION_SCHEMA = "bn-ecm1-current-children-build-v2"
CONSTRUCTION_SCHEMA = "bn-30fs-current-children-construction-v1"
SELF_TEST_SCHEMA = "bn-30fs-build-children-self-test-v1"
TOOLS_SCHEMA = "asterism-rebaseline-tools-v3"
LOCK_MANIFEST_SCHEMA = "asterism-rebaseline-lock-candidates-v3"
LOCK_AUTHORITY_SCHEMA = "bn-31gp-current-lock-authority-v1"
LOCK_VALIDATION_SCHEMA = "bn-31gp-current-lock-authority-validation-v1"
FAULT_VALIDATOR_SCHEMA = "bn-20be-current-fault-validator-v1"
STATIC_VALIDATOR_SCHEMA = "bn-30fs-build-children-validator-v1"
WRAPPER_RECEIPT_SCHEMA = "bn-30fs-rustc-workspace-wrapper-receipt-v1"
WRAPPER_TARGET_CRATE = "mess_store"
WRAPPER_TARGET_PACKAGE = "mess-store"
WRAPPER_TARGET_CRATE_TYPE = "lib"
WRAPPER_INJECTED_ARGUMENTS = (
    "--cfg",
    "test",
    "--allow",
    "explicit_builtin_cfgs_in_flags",
    "--cfg",
    "asterism_rebaseline_correctness",
    "--check-cfg",
    "cfg(asterism_rebaseline_correctness)",
)
FAULT_COMPILE_OUT_SCHEMA = "bn-2l3n-fault-compile-out-authority-v1"
PREAPPROVAL_SOURCE_SENTINEL = (
    "fa2acb626f303f8a65a16a6c8a1fd86b7e80cf48e092ae21a7308984ae790c94"
)

HERE = Path(__file__).resolve().parent
TOOLING = HERE.parent
if str(TOOLING) not in sys.path:
    sys.path.insert(0, str(TOOLING))
from overlay_pins import (  # noqa: E402
    PINNED_SHARED_OVERLAY_SHA256,
    validate_pinned_shared_overlay_payload,
    validate_pinned_shared_overlay_set,
)

SHARED = TOOLING / "overlay" / "shared"
PUBLIC = TOOLING / "overlay" / "public"
CORRECTNESS_SOURCE = HERE / "correctness.rs"
FAULT_SOURCE = HERE / "fault.rs"
FAULT_VALIDATOR = HERE / "validate_fault.py"
LOCK_AUTHORITY_VALIDATOR = HERE / "lock_authority.py"
PREPARE_OVERLAYS = TOOLING / "prepare_overlays.py"
OVERLAY_PINS = TOOLING / "overlay_pins.py"
PRODUCT_OVERLAY = HERE / "product-test-overlay.patch"
PRODUCT_OVERLAY_VALIDATOR = HERE / "validate_product_test_overlay.py"
WORKSPACE_WRAPPER = HERE / "rustc_workspace_wrapper.py"
STATIC_VALIDATOR = HERE / "validate_build_children.py"
EXPECTED_LIB_SOURCE = "crates/mess-store/src/lib.rs"
ENGINE_PATH = Path("crates/mess-store/src/engine.rs")
LOCK_PATH = Path("Cargo.lock")
CORRECTNESS_DESTINATION = Path(
    "crates/mess-store/examples/asterism_rebaseline_current_correctness.rs"
)
FAULT_DESTINATION = Path(
    "crates/mess-store/examples/asterism_rebaseline_current_fault.rs"
)
PUBLIC_DESTINATION = Path(
    "crates/mess-store/examples/asterism_rebaseline_public.rs"
)
ADAPTER_DESTINATION = Path(
    "crates/mess-store/examples/asterism_rebaseline_adapter.rs"
)
SHARED_DESTINATION = Path(
    "crates/mess-store/examples/asterism_rebaseline_shared"
)
SHARED_NAMES = (
    "allocation.rs",
    "contract.rs",
    "control.rs",
    "digest.rs",
    "schema.rs",
    "semantic_oracle.rs",
    "timing.rs",
    "workload.rs",
)

REQUIRED_TOOLS = {
    "correctness",
    "evaluator_runtime",
    "fault",
    "perf",
    "runner_runtime",
    "strace",
    "strace_launcher_runtime",
    "terminal_verifier_runtime",
}
CHILD_TOOLS = {"correctness", "fault"}
CHILD_PLACEHOLDER_BINDINGS = {
    "correctness": {
        "comm": "ast-rb-check",
        "executable_mode": 0o555,
        "path": "/asterism/preapproval-placeholder/ast-rb-check",
        "sha256": "a48e573b0cbd89a11ece523fbc79e7d6a54aa42fa417e913f70861c0846ed3d6",
    },
    "fault": {
        "comm": "ast-rb-fault",
        "executable_mode": 0o555,
        "path": "/asterism/preapproval-placeholder/ast-rb-fault",
        "sha256": "a9826b2a400813f9c0ab0b9a8e6998c2c40bf3fc6bee07495552e6d23a7c8367",
    },
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
FORBIDDEN_RELEASE_TOKENS = (
    b"TestEngineHook",
    b"TestEngineHooks",
    b"TestEngineFs",
    b"arm_test_hook",
    b"arm_test_owner_cohort",
    b"asterism_rebaseline_correctness",
)
MAX_CAPTURE_BYTES = 64 * 1024 * 1024
SYSTEM_PYTHON = Path("/usr/bin/python3").resolve(strict=True)
GUEST_ROOT = "/asterism"
GUEST_CARGO_HOME = f"{GUEST_ROOT}/cargo-home"
GUEST_TARGET = f"{GUEST_ROOT}/target"
GUEST_TOOLCHAIN_ROOT = f"{GUEST_ROOT}/toolchain"
GUEST_TOOLCHAIN_BIN = f"{GUEST_TOOLCHAIN_ROOT}/bin"
GUEST_CARGO = f"{GUEST_TOOLCHAIN_ROOT}/bin/cargo"
GUEST_RUSTC = f"{GUEST_TOOLCHAIN_ROOT}/bin/rustc"
HOST_DEV_NULL = Path("/dev/null")
CARGO_CONFIG_SEARCH_SCHEMA = "bn-30fs-build-cargo-config-search-v1"
SEMANTIC_INPUT_AUTHORITY_SCHEMA = "bn-ecm1-semantic-input-authority-v1"
RECURSIVE_TREE_AUTHORITY_SCHEMA = "bn-ecm1-recursive-tree-authority-v1"
TRUSTED_SYSTEM_CLOSURE_SCHEMA = "bn-ecm1-trusted-system-closure-v1"
CARGO_HOME_TREE_SCHEMA = RECURSIVE_TREE_AUTHORITY_SCHEMA
TRUSTED_SYSTEM_MOUNTS = (
    (Path("/usr/bin"), "/usr/bin"),
    (Path("/usr/lib"), "/usr/lib"),
    (Path("/usr/include"), "/usr/include"),
)
INOTIFY_EVENT_HEADER = struct.Struct("iIII")
INOTIFY_MUTATION_MASK = (
    0x00000002  # IN_MODIFY
    | 0x00000004  # IN_ATTRIB
    | 0x00000008  # IN_CLOSE_WRITE
    | 0x00000040  # IN_MOVED_FROM
    | 0x00000080  # IN_MOVED_TO
    | 0x00000100  # IN_CREATE
    | 0x00000200  # IN_DELETE
    | 0x00000400  # IN_DELETE_SELF
    | 0x00000800  # IN_MOVE_SELF
    | 0x00002000  # IN_UNMOUNT
    | 0x00004000  # IN_Q_OVERFLOW
    | 0x00008000  # IN_IGNORED
)
CARGO_CONFIG_GUEST_PATHS = (
    "/asterism/source/.cargo/config.toml",
    "/asterism/source/.cargo/config",
    "/asterism/.cargo/config.toml",
    "/asterism/.cargo/config",
    "/.cargo/config.toml",
    "/.cargo/config",
    f"{GUEST_CARGO_HOME}/config.toml",
    f"{GUEST_CARGO_HOME}/config",
)


class BuildError(RuntimeError):
    """An input, build transition, or attestation differs from authority."""


def load_evidence_schema_self_test_module() -> Any:
    """Load the real shared semantic-manifest validator for producer tests."""

    import importlib.util

    module_name = "_asterism_build_children_evidence_schema_self_test"
    spec = importlib.util.spec_from_file_location(
        module_name, TOOLING.parent / "evidence_schema.py"
    )
    if spec is None or spec.loader is None:
        raise BuildError("evidence-schema self-test module is unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode()


def authority_canonical_bytes(value: Any) -> bytes:
    """Canonical bytes for externally reviewed authority producers."""

    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def semantic_runtime_sha256(components: Mapping[str, Mapping[str, Any]]) -> str:
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
        raise BuildError("semantic runtime authority components differ")
    closure = components["trusted_system_closure"]
    mounts = closure.get("mounts")
    if (
        not isinstance(mounts, list)
        or len(mounts) != len(TRUSTED_SYSTEM_MOUNTS)
        or any(
            not isinstance(mount, Mapping)
            or mount.get("guest_path") != guest_path
            or mount.get("host_path") != str(host_path)
            or mount.get("resolved_path") != str(host_path)
            for mount, (host_path, guest_path) in zip(
                mounts, TRUSTED_SYSTEM_MOUNTS, strict=True
            )
        )
    ):
        raise BuildError("semantic runtime trusted-system mounts differ")
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
    return sha256_bytes(canonical_bytes(normalized))


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
        raise BuildError("trusted system symlink reaches mutable guest authority")
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
        raise BuildError("symlink target is unresolved") from error
    if resolved != root and root not in resolved.parents:
        raise BuildError("symlink escapes the retained root")
    return "within_root"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def lower_hex(value: Any, length: int) -> bool:
    return (
        isinstance(value, str)
        and len(value) == length
        and all(character in "0123456789abcdef" for character in value)
    )


def require_exact_file(
    path: Path,
    *,
    mode: int | None = None,
    exact_path: Path | None = None,
    context: str,
) -> Path:
    if not path.is_absolute():
        raise BuildError(f"{context} path is not absolute")
    try:
        metadata = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise BuildError(f"{context} is unavailable: {path}") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise BuildError(f"{context} is not an exact regular file")
    if resolved != path:
        raise BuildError(f"{context} path contains an alias")
    if exact_path is not None and resolved != exact_path:
        raise BuildError(f"{context} path differs from the reviewed location")
    if metadata.st_nlink != 1:
        raise BuildError(f"{context} must have exactly one hard link")
    if mode is not None and stat.S_IMODE(metadata.st_mode) != mode:
        raise BuildError(f"{context} mode differs")
    return resolved


def file_identity(path: Path, context: str) -> dict[str, Any]:
    exact = require_exact_file(path, context=context)
    before = exact.stat()
    digest = sha256_file(exact)
    after = exact.stat()
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
        raise BuildError(f"{context} changed while hashing")
    return {
        "bytes": before.st_size,
        "ctime_ns": before.st_ctime_ns,
        "device": before.st_dev,
        "inode": before.st_ino,
        "link_count": before.st_nlink,
        "mode": stat.S_IMODE(before.st_mode),
        "mtime_ns": before.st_mtime_ns,
        "path": str(exact),
        "sha256": digest,
        "size": before.st_size,
    }


def parse_canonical(payload: bytes, schema: str, context: str) -> dict[str, Any]:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BuildError(f"{context} is not JSON") from error
    if not isinstance(value, dict) or value.get("schema") != schema:
        raise BuildError(f"{context} schema differs")
    if payload != canonical_bytes(value):
        raise BuildError(f"{context} is not canonical JSON+LF")
    return value


def parse_external_canonical(
    payload: bytes, schema: str, context: str
) -> dict[str, Any]:
    """Parse the UTF-8 canonical form emitted by authority producers."""

    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BuildError(f"{context} is not JSON") from error
    if not isinstance(value, dict) or value.get("schema") != schema:
        raise BuildError(f"{context} schema differs")
    if payload != authority_canonical_bytes(value):
        raise BuildError(f"{context} is not authority-canonical JSON+LF")
    return value


def load_canonical(path: Path, schema: str, context: str) -> dict[str, Any]:
    require_exact_file(path, mode=0o444, context=context)
    return parse_external_canonical(path.read_bytes(), schema, context)


def validate_wrapper_receipt(
    payload: bytes,
    identity: Mapping[str, Any],
    *,
    build_nonce: str,
    pinned_rustc: str,
) -> dict[str, Any]:
    receipt = parse_canonical(
        payload, WRAPPER_RECEIPT_SCHEMA, "wrapper injection receipt"
    )
    expected_fields = {
        "build_nonce",
        "crate_name",
        "crate_type",
        "injected_arguments",
        "original_argv_sha256",
        "package",
        "rustc",
        "schema",
        "source",
    }
    if set(receipt) != expected_fields:
        raise BuildError("wrapper injection receipt fields differ")
    expected_values = {
        "build_nonce": build_nonce,
        "crate_name": WRAPPER_TARGET_CRATE,
        "crate_type": WRAPPER_TARGET_CRATE_TYPE,
        "injected_arguments": list(WRAPPER_INJECTED_ARGUMENTS),
        "package": WRAPPER_TARGET_PACKAGE,
        "rustc": pinned_rustc,
        "schema": WRAPPER_RECEIPT_SCHEMA,
        "source": EXPECTED_LIB_SOURCE,
    }
    if any(receipt.get(name) != value for name, value in expected_values.items()):
        raise BuildError("wrapper injection receipt targeting/injection differs")
    if not lower_hex(receipt.get("original_argv_sha256"), 64):
        raise BuildError("wrapper injection receipt argv digest differs")
    if (
        identity.get("mode") != 0o444
        or identity.get("link_count") != 1
        or identity.get("sha256") != sha256_bytes(payload)
        or identity.get("size") != len(payload)
    ):
        raise BuildError("wrapper injection receipt identity differs")
    return receipt


def write_new(path: Path, payload: bytes, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        path,
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0),
        mode,
    )
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise BuildError(f"write made no progress: {path}")
            view = view[written:]
        os.fsync(descriptor)
        os.fchmod(descriptor, mode)
    finally:
        os.close(descriptor)


def copy_new(source: Path, destination: Path, mode: int = 0o444) -> dict[str, Any]:
    identity = file_identity(source, f"copy source {source.name}")
    write_new(destination, source.read_bytes(), mode)
    if sha256_file(destination) != identity["sha256"]:
        raise BuildError(f"copied file differs: {destination}")
    return {
        "destination": destination.as_posix(),
        "mode": mode,
        "sha256": identity["sha256"],
        "source": str(source),
    }


def trusted_root_chain(path: Path, context: str) -> list[dict[str, Any]]:
    """Attest an exact root-owned, non-writable pathname chain."""

    exact = path.resolve(strict=True)
    if exact != path or not exact.is_absolute():
        raise BuildError(f"{context} trusted path is aliased")
    records = []
    current = Path("/")
    chain_paths = [current]
    for component in exact.parts[1:]:
        current = current / component
        chain_paths.append(current)
    for current in chain_paths:
        metadata = current.lstat()
        if (
            stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != 0
            or stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            raise BuildError(f"{context} trusted pathname chain is mutable")
        records.append(
            {
                "changed_ns": metadata.st_ctime_ns,
                "device": metadata.st_dev,
                "gid": metadata.st_gid,
                "inode": metadata.st_ino,
                "link_count": metadata.st_nlink,
                "mode": stat.S_IMODE(metadata.st_mode),
                "modified_ns": metadata.st_mtime_ns,
                "path": str(current),
                "size": metadata.st_size,
                "type": stat.S_IFMT(metadata.st_mode),
                "uid": metadata.st_uid,
            }
        )
    return records


class RetainedFile:
    """Retain, hash, and replay one exact regular file selection."""

    def __init__(
        self,
        path: Path,
        context: str,
        *,
        expected_sha256: str | None = None,
        require_executable: bool,
        trusted_system: bool = False,
        parent_descriptor: int | None = None,
        relative_name: str | None = None,
    ) -> None:
        self.path = path
        self.context = context
        self.expected_sha256 = expected_sha256
        self.require_executable = require_executable
        self.trusted_system = trusted_system
        self.parent_descriptor = parent_descriptor
        self.relative_name = relative_name
        if (parent_descriptor is None) != (relative_name is None):
            raise BuildError(f"{context} relative selection is incomplete")
        self.descriptor = -1
        self.payload = b""
        self.identity: dict[str, Any] | None = None
        self.chain: list[dict[str, Any]] | None = None

    def __enter__(self) -> RetainedFile:
        exact = require_exact_file(self.path, context=self.context)
        if self.trusted_system:
            self.chain = trusted_root_chain(exact, self.context)
        flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
        self.descriptor = os.open(
            self.relative_name if self.relative_name is not None else exact,
            flags,
            dir_fd=self.parent_descriptor,
        )
        try:
            self.payload, self.identity = snapshot_open_file(
                self.descriptor,
                exact,
                self.context,
                require_executable=self.require_executable,
            )
            if file_identity(exact, self.context) != self.identity:
                raise BuildError(f"{self.context} descriptor/path identity differs")
            self.verify_relative_selection()
            if (
                self.expected_sha256 is not None
                and self.identity["sha256"] != self.expected_sha256
            ):
                raise BuildError(f"{self.context} hash differs from authority")
        except Exception:
            self.close()
            raise
        return self

    @property
    def pass_fds(self) -> tuple[int, ...]:
        if self.descriptor < 0:
            raise BuildError(f"{self.context} retained file is inactive")
        return (self.descriptor,)

    @property
    def proc_path(self) -> str:
        return f"/proc/self/fd/{self.pass_fds[0]}"

    def rewind_for_bind_data(self) -> None:
        os.lseek(self.pass_fds[0], 0, os.SEEK_SET)

    def verify_relative_selection(self) -> None:
        if self.parent_descriptor is None or self.relative_name is None:
            return
        selected = os.stat(
            self.relative_name,
            dir_fd=self.parent_descriptor,
            follow_symlinks=False,
        )
        if not same_manifest_metadata(selected, os.fstat(self.pass_fds[0])):
            raise BuildError(f"{self.context} retained relative selection changed")

    def verify(self) -> None:
        if self.identity is None:
            raise BuildError(f"{self.context} retained identity is absent")
        payload, observed = snapshot_open_file(
            self.pass_fds[0],
            self.path,
            self.context,
            require_executable=self.require_executable,
        )
        if (
            payload != self.payload
            or observed != self.identity
            or file_identity(self.path, self.context) != self.identity
        ):
            raise BuildError(f"{self.context} changed across retained use")
        self.verify_relative_selection()
        if (
            self.trusted_system
            and trusted_root_chain(self.path, self.context) != self.chain
        ):
            raise BuildError(f"{self.context} trusted pathname chain changed")

    def record(self) -> dict[str, Any]:
        if self.identity is None:
            raise BuildError(f"{self.context} retained identity is absent")
        return {
            "identity": self.identity,
            "path_chain": self.chain,
            "trusted_system": self.trusted_system,
        }

    def close(self) -> None:
        if self.descriptor >= 0:
            os.close(self.descriptor)
            self.descriptor = -1

    def __exit__(self, child_type: Any, child_error: Any, traceback: Any) -> bool:
        verification_error = None
        try:
            self.verify()
        except BuildError as error:
            verification_error = error
        finally:
            self.close()
        if verification_error is not None:
            raise verification_error
        return False


class RetainedDevice:
    """Retain and attest the one writable character device needed by Cargo."""

    def __init__(self, path: Path, context: str) -> None:
        self.path = path
        self.context = context
        self.descriptor = -1
        self.identity: dict[str, Any] | None = None
        self.chain: list[dict[str, Any]] | None = None

    @staticmethod
    def _identity(metadata: os.stat_result, path: Path) -> dict[str, Any]:
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

    def _snapshot(self) -> dict[str, Any]:
        metadata = os.fstat(self.descriptor)
        identity = self._identity(metadata, self.path)
        if (
            not stat.S_ISCHR(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != 0o666
            or metadata.st_nlink != 1
            or os.major(metadata.st_rdev) != 1
            or os.minor(metadata.st_rdev) != 3
        ):
            raise BuildError(f"{self.context} is not the exact null device")
        return identity

    def __enter__(self) -> RetainedDevice:
        exact = self.path.resolve(strict=True)
        if exact != self.path:
            raise BuildError(f"{self.context} path differs")
        self.chain = trusted_root_chain(exact.parent, self.context)
        self.descriptor = os.open(
            exact,
            os.O_RDWR | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            self.identity = self._snapshot()
            if self._identity(exact.lstat(), exact) != self.identity:
                raise BuildError(f"{self.context} descriptor/path identity differs")
        except Exception:
            self.close()
            raise
        return self

    @property
    def pass_fds(self) -> tuple[int, ...]:
        if self.descriptor < 0:
            raise BuildError(f"{self.context} retained device is inactive")
        return (self.descriptor,)

    @property
    def proc_path(self) -> str:
        return f"/proc/self/fd/{self.pass_fds[0]}"

    def verify(self) -> None:
        if self.identity is None or self.chain is None:
            raise BuildError(f"{self.context} retained identity is absent")
        if (
            self._snapshot() != self.identity
            or self._identity(self.path.lstat(), self.path) != self.identity
            or trusted_root_chain(self.path.parent, self.context) != self.chain
        ):
            raise BuildError(f"{self.context} changed across retained use")

    def record(self) -> dict[str, Any]:
        if self.identity is None:
            raise BuildError(f"{self.context} retained identity is absent")
        return {
            "identity": self.identity,
            "parent_path_chain": self.chain,
            "trusted_system": True,
        }

    def close(self) -> None:
        if self.descriptor >= 0:
            os.close(self.descriptor)
            self.descriptor = -1

    def __exit__(self, child_type: Any, child_error: Any, traceback: Any) -> bool:
        verification_error = None
        try:
            self.verify()
        except BuildError as error:
            verification_error = error
        finally:
            self.close()
        if verification_error is not None:
            raise verification_error
        return False


def run_capture(
    argv: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str],
    timeout: int,
    pass_fds: tuple[int, ...] = (),
    execution_lease: RetainedFile,
    actual_argv: Sequence[str] | None = None,
    stdin_payload: bytes | None = None,
) -> tuple[dict[str, Any], bytes, bytes]:
    logical_argv = list(argv)
    if not logical_argv or logical_argv[0] != str(execution_lease.path):
        raise BuildError("logical command does not match retained executable")
    inherited = tuple(dict.fromkeys((*pass_fds, *execution_lease.pass_fds)))
    if len(inherited) != len(pass_fds) + len(execution_lease.pass_fds):
        raise BuildError("command inherited file descriptors alias")
    execution_lease.verify()
    try:
        completed = subprocess.run(
            list(actual_argv) if actual_argv is not None else logical_argv,
            executable=execution_lease.proc_path,
            cwd=cwd,
            env=dict(env),
            input=stdin_payload,
            stdin=subprocess.DEVNULL if stdin_payload is None else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            pass_fds=inherited,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as error:
        raise BuildError(f"command timed out: {argv[0]}") from error
    execution_lease.verify()
    stdout = completed.stdout
    stderr = completed.stderr
    if len(stdout) + len(stderr) > MAX_CAPTURE_BYTES:
        raise BuildError(f"command output exceeds {MAX_CAPTURE_BYTES} bytes")
    record = {
        "argv": logical_argv,
        "cwd": str(cwd.resolve()),
        "environment": dict(sorted(env.items())),
        "exit_status": completed.returncode,
        "execution_authority": execution_lease.record(),
        "passed_file_descriptors": len(inherited),
        "stderr_bytes": len(stderr),
        "stderr_sha256": sha256_bytes(stderr),
        "stdout_bytes": len(stdout),
        "stdout_sha256": sha256_bytes(stdout),
    }
    if stdin_payload is not None:
        record["stdin_bytes"] = len(stdin_payload)
        record["stdin_sha256"] = sha256_bytes(stdin_payload)
    return record, stdout, stderr


def run_logged(
    name: str,
    argv: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str],
    timeout: int,
    log_root: Path,
    pass_fds: tuple[int, ...] = (),
    execution_lease: RetainedFile,
    actual_argv: Sequence[str] | None = None,
    stdin_payload: bytes | None = None,
    boundary_replay: Callable[[], None] | None = None,
    require_quiet: bool = False,
) -> dict[str, Any]:
    try:
        record, stdout, stderr = run_capture(
            argv,
            cwd=cwd,
            env=env,
            timeout=timeout,
            pass_fds=pass_fds,
            execution_lease=execution_lease,
            actual_argv=actual_argv,
            stdin_payload=stdin_payload,
        )
    finally:
        if boundary_replay is not None:
            boundary_replay()
    log_path = log_root / f"{name}.json"
    write_new(log_path, canonical_bytes(record), 0o444)
    if record["exit_status"] != 0:
        raise BuildError(
            f"{name} failed with {record['exit_status']}: {stderr.decode(errors='replace')}"
        )
    if require_quiet and (stdout or stderr):
        raise BuildError(f"{name} produced unexpected output")
    return record


def validator_output(
    argv: Sequence[str],
    *,
    repository: Path,
    context: str,
    canonical_output: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if len(argv) < 3 or argv[1] != "-B":
        raise BuildError(f"{context} validator argv differs")
    script = require_exact_file(Path(argv[2]), context=f"{context} script")
    script_arguments = list(argv[3:])
    environment = {
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "TZ": "UTC",
    }
    bootstrap = (
        "import os,sys;"
        "fd=int(sys.argv[1]);filename=sys.argv[2];args=sys.argv[3:];"
        "source=os.fdopen(fd,'rb',closefd=False).read();"
        "sys.argv=[filename,*args];"
        "scope={'__name__':'__main__','__file__':filename,'__package__':None};"
        "exec(compile(source,filename,'exec'),scope,scope)"
    )
    logical_argv = [str(SYSTEM_PYTHON), "-I", "-B", str(script), *script_arguments]
    with ExitStack() as stack:
        python_lease = stack.enter_context(
            RetainedFile(
                SYSTEM_PYTHON,
                f"{context} Python interpreter",
                require_executable=True,
                trusted_system=True,
            )
        )
        script_lease = stack.enter_context(
            RetainedFile(
                script,
                f"{context} retained script",
                require_executable=False,
            )
        )
        actual_argv = [
            str(SYSTEM_PYTHON),
            "-I",
            "-B",
            "-c",
            bootstrap,
            str(script_lease.descriptor),
            str(script),
            *script_arguments,
        ]
        script_lease.rewind_for_bind_data()
        record, stdout, stderr = run_capture(
            logical_argv,
            cwd=repository,
            env=environment,
            timeout=120,
            pass_fds=script_lease.pass_fds,
            execution_lease=python_lease,
            actual_argv=actual_argv,
        )
        record["script_authority"] = script_lease.record()
    if record["exit_status"] != 0 or stderr:
        raise BuildError(
            f"{context} failed: rc={record['exit_status']} stderr={stderr!r}"
        )
    try:
        value = json.loads(stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BuildError(f"{context} output is not JSON") from error
    expected_output = (
        canonical_bytes(value)
        if canonical_output
        else (json.dumps(value, sort_keys=True) + "\n").encode()
    )
    if not isinstance(value, dict) or stdout != expected_output:
        raise BuildError(f"{context} output is not one canonical object")
    return value, record


def load_lock_authority_module() -> Any:
    validator = require_exact_file(
        LOCK_AUTHORITY_VALIDATOR,
        exact_path=LOCK_AUTHORITY_VALIDATOR,
        context="lock authority validator",
    )
    dependency = require_exact_file(
        PREPARE_OVERLAYS,
        exact_path=PREPARE_OVERLAYS,
        context="prepare-overlays lock dependency",
    )
    module_name = "asterism_current_lock_authority"
    if module_name in sys.modules or "prepare_overlays" in sys.modules:
        raise BuildError("lock authority module namespace is already occupied")
    with ExitStack() as stack:
        dependency_lease = stack.enter_context(
            RetainedFile(
                dependency,
                "retained prepare-overlays lock dependency",
                require_executable=False,
            )
        )
        validator_lease = stack.enter_context(
            RetainedFile(
                validator,
                "retained lock authority module",
                require_executable=False,
            )
        )
        overlays_module = types.ModuleType("prepare_overlays")
        overlays_module.__file__ = str(dependency)
        overlays_module.__package__ = None
        sys.modules["prepare_overlays"] = overlays_module
        module = types.ModuleType(module_name)
        module.__file__ = str(validator)
        module.__package__ = None
        sys.modules[module_name] = module
        try:
            exec(
                compile(
                    dependency_lease.payload,
                    str(dependency),
                    "exec",
                ),
                overlays_module.__dict__,
                overlays_module.__dict__,
            )
            exec(
                compile(
                    validator_lease.payload,
                    str(validator),
                    "exec",
                ),
                module.__dict__,
                module.__dict__,
            )
        except Exception:
            sys.modules.pop(module_name, None)
            sys.modules.pop("prepare_overlays", None)
            raise
    validate_authority = getattr(module, "validate_authority", None)
    validation_result = getattr(module, "validation_result", None)
    capture_tree = getattr(module, "capture_prebuild_materialized_tree", None)
    resample_admission = getattr(
        module, "resample_builder_filesystem_admission", None
    )
    if not all(
        callable(function)
        for function in (
            validate_authority,
            validation_result,
            capture_tree,
            resample_admission,
        )
    ):
        raise BuildError("lock authority Python API differs")
    return module


def validate_lock_authority(
    repository: Path,
    lock_manifest_path: Path,
    review_bundle_path: Path,
    authority_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], Any, Any]:
    module = load_lock_authority_module()
    validate_authority = module.validate_authority
    validation_result = module.validation_result
    lock_preview = load_canonical(
        lock_manifest_path, LOCK_MANIFEST_SCHEMA, "lock authority manifest preview"
    )
    authority_preview = load_canonical(
        authority_path, LOCK_AUTHORITY_SCHEMA, "lock authority preview"
    )
    review_preview = load_canonical(
        review_bundle_path,
        "bn-31gp-current-lock-review-bundle-v1",
        "lock review bundle preview",
    )
    embedded_manifest = authority_preview.get("lock_manifest")
    embedded_review = authority_preview.get("review_bundle")
    toolchain = lock_preview.get("toolchain")
    if (
        not isinstance(embedded_manifest, dict)
        or embedded_manifest.get("payload") != lock_preview
        or embedded_manifest.get("sha256") != sha256_file(lock_manifest_path)
        or not isinstance(embedded_review, dict)
        or embedded_review.get("payload") != review_preview
        or embedded_review.get("sha256") != sha256_file(review_bundle_path)
        or not isinstance(toolchain, dict)
        or authority_preview.get("toolchain") != toolchain
        or authority_preview.get("protocol") != lock_preview.get("protocol")
        or authority_preview.get("protocol_sha256")
        != lock_preview.get("protocol_sha256")
    ):
        raise BuildError("authority/manifest/review execution cross-binding differs")
    with ExitStack() as stack:
        leases = {
            name: stack.enter_context(
                RetainedFile(
                    Path(str(toolchain[f"{name}_path"])),
                    f"lock authority {name} executable",
                    expected_sha256=str(toolchain[f"{name}_sha256"]),
                    require_executable=True,
                    trusted_system=name in {"bwrap", "git", "rustup"},
                )
            )
            for name in ("bwrap", "cargo", "git", "rustc", "rustup")
        }
        git_lease = leases["git"]

        def descriptor_git(arguments: Sequence[str], context: str) -> str:
            record, stdout, stderr = run_capture(
                [str(git_lease.path), *arguments],
                cwd=repository,
                env=git_environment(),
                timeout=180,
                execution_lease=git_lease,
            )
            if record["exit_status"] != 0 or stderr:
                raise BuildError(f"{context} descriptor Git failed: {stderr!r}")
            return stdout.decode().strip()

        def current_context(lock_value: dict[str, Any]) -> Any:
            if lock_value != lock_preview:
                raise BuildError("semantic lock payload differs from cross-bound preview")
            if (
                lock_value.get("schema") != LOCK_MANIFEST_SCHEMA
                or lock_value.get("protocol") != PROTOCOL
                or lock_value.get("protocol_sha256") != PROTOCOL_SHA256
                or lock_value.get("toolchain") != toolchain
                or lock_value.get("filesystem_admission")
                != authority_preview.get("filesystem_admission")
            ):
                raise BuildError("semantic lock context differs")
            for lease in leases.values():
                lease.verify()
            commit = descriptor_git(["rev-parse", "HEAD"], "tooling commit")
            tree = descriptor_git(["rev-parse", "HEAD^{tree}"], "tooling tree")
            status = descriptor_git(["status", "--porcelain"], "tooling status")
            if (
                status
                or commit != authority_preview.get("tooling_commit")
                or tree != authority_preview.get("tooling_tree")
                or not lower_hex(commit, 40)
                or not lower_hex(tree, 40)
            ):
                raise BuildError("descriptor tooling identity differs from authority")
            return module.AuthorityContext(
                filesystem_admission=lock_value["filesystem_admission"],
                protocol_sha256=lock_value["protocol_sha256"],
                toolchain=toolchain,
                tooling_commit=commit,
                tooling_tree=tree,
            )

        def identity_revalidator(expected: Any) -> Any:
            observed = current_context(lock_preview)
            if observed != expected:
                raise BuildError("descriptor authority identity recheck differs")
            return observed

        validated = validate_authority(
            repository,
            lock_manifest_path,
            review_bundle_path,
            authority_path,
            semantic_validator=current_context,
            identity_revalidator=identity_revalidator,
        )
        execution_authority = {
            name: lease.record() for name, lease in sorted(leases.items())
        }
    value = validation_result(validated)
    if not isinstance(value, dict):
        raise BuildError("lock authority validation_result did not return an object")
    if canonical_bytes(value) != canonical_bytes(json.loads(canonical_bytes(value))):
        raise BuildError("lock authority validation_result is not canonicalizable")
    authority_snapshot = getattr(validated, "authority", None)
    lock_manifest_snapshot = getattr(validated, "lock_manifest", None)
    review_bundle_snapshot = getattr(validated, "review_bundle", None)
    expected_snapshot_paths = {
        "authority": authority_path,
        "lock_manifest": lock_manifest_path,
        "review_bundle": review_bundle_path,
    }
    for name, path in expected_snapshot_paths.items():
        snapshot = getattr(validated, name, None)
        if snapshot is None or getattr(snapshot, "path", None) != path:
            raise BuildError(f"validated {name} exact path differs")
        immutable_snapshot_record(snapshot, f"validated {name}")
    authority = authority_snapshot.value
    locks = lock_manifest_snapshot.value
    if not isinstance(authority, dict) or authority.get("schema") != LOCK_AUTHORITY_SCHEMA:
        raise BuildError("validated authority schema differs")
    if not isinstance(locks, dict) or locks.get("schema") != LOCK_MANIFEST_SCHEMA:
        raise BuildError("validated lock-manifest schema differs")
    expected = {
        "authority_sha256": authority_snapshot.sha256,
        "lock_manifest_sha256": lock_manifest_snapshot.sha256,
        "schema": LOCK_VALIDATION_SCHEMA,
        "status": "ok",
    }
    if value != expected:
        raise BuildError("lock authority validation output differs")
    return authority, locks, {
        "execution_authority": execution_authority,
        "result": value,
        "semantic_validator": "descriptor-cross-bound-authority-context-v1",
    }, validated, module


def immutable_snapshot_record(
    snapshot: Any, label: str, *, require_value: bool = True
) -> dict[str, Any]:
    required = {"path", "payload", "sha256", "mode", "identity", "value"}
    if not all(hasattr(snapshot, field) for field in required):
        raise BuildError(f"{label} immutable snapshot fields differ")
    path = snapshot.path
    payload = snapshot.payload
    if not isinstance(path, Path) or not isinstance(payload, bytes):
        raise BuildError(f"{label} snapshot path/payload types differ")
    exact = require_exact_file(path, mode=0o444, context=f"{label} immutable file")
    if (
        snapshot.mode != 0o444
        or snapshot.sha256 != sha256_bytes(payload)
        or sha256_file(exact) != snapshot.sha256
        or exact.read_bytes() != payload
        or (require_value and snapshot.value is None)
    ):
        raise BuildError(f"{label} immutable snapshot bytes/mode/value differ")
    identity = snapshot.identity
    canonical_identity = getattr(identity, "canonical", None)
    if not callable(canonical_identity):
        raise BuildError(f"{label} snapshot canonical identity is absent")
    metadata = exact.stat()
    expected_identity = {
        "changed_ns": metadata.st_ctime_ns,
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "link_count": metadata.st_nlink,
        "modified_ns": metadata.st_mtime_ns,
    }
    observed_identity = canonical_identity()
    if observed_identity != expected_identity:
        raise BuildError(f"{label} snapshot full identity changed")
    return {
        "identity": observed_identity,
        "mode": snapshot.mode,
        "path": str(exact),
        "sha256": snapshot.sha256,
        "size": len(payload),
    }


def validated_lock_records(validated: Any) -> dict[str, dict[str, Any]]:
    locks = getattr(validated, "locks", None)
    if not isinstance(locks, dict) or set(locks) != {"A", "C", "D"}:
        raise BuildError("ValidatedAuthority.locks must be the exact A/C/D set")
    records = {
        name: immutable_snapshot_record(
            snapshot,
            f"validated {name}",
            require_value=False,
        )
        for name, snapshot in sorted(locks.items())
    }
    paths = [record["path"] for record in records.values()]
    identities_seen = [
        (record["identity"]["device"], record["identity"]["inode"])
        for record in records.values()
    ]
    if len(set(paths)) != 3 or len(set(identities_seen)) != 3:
        raise BuildError("validated A/C/D lock candidates are not exact and disjoint")
    if records["A"]["sha256"] != PRODUCT_LOCK_SHA256:
        raise BuildError("validated A lock payload differs from frozen authority")
    return records


def validated_authority_records(validated: Any) -> dict[str, dict[str, Any]]:
    return {
        name: immutable_snapshot_record(
            getattr(validated, name, None), f"validated {name}"
        )
        for name in ("authority", "lock_manifest", "review_bundle")
    }


def validate_fault_authority(repository: Path) -> dict[str, Any]:
    source = require_exact_file(
        FAULT_SOURCE, exact_path=FAULT_SOURCE, context="current fault source"
    )
    validator = require_exact_file(
        FAULT_VALIDATOR,
        exact_path=FAULT_VALIDATOR,
        context="current fault validator",
    )
    source_before = file_identity(source, "current fault source before validation")
    validator_before = file_identity(
        validator, "current fault validator before validation"
    )
    outputs = []
    executions = []
    for arguments in ((), ("--self-test",)):
        value, execution = validator_output(
            [str(SYSTEM_PYTHON), "-B", str(validator), *arguments],
            repository=repository,
            context="current fault validator",
        )
        if value.get("schema") != FAULT_VALIDATOR_SCHEMA or value.get("status") != "ok":
            raise BuildError("current fault validator schema/status differs")
        checks = value.get("checks")
        hostile = value.get("hostile_mutations_rejected")
        if (
            not isinstance(checks, list)
            or not checks
            or any(not isinstance(item, str) or not item for item in checks)
            or len(checks) != len(set(checks))
            or not isinstance(hostile, int)
            or isinstance(hostile, bool)
            or hostile < 0
        ):
            raise BuildError("current fault validator checks/hostile count differ")
        outputs.append(value)
        executions.append(execution)
    if outputs[0]["checks"] != outputs[1]["checks"]:
        raise BuildError("current fault normal/self-test exact checks differ")
    if outputs[1]["hostile_mutations_rejected"] <= 0:
        raise BuildError("current fault self-test rejected no hostile mutations")
    source_after = file_identity(source, "current fault source after validation")
    validator_after = file_identity(
        validator, "current fault validator after validation"
    )
    if source_after != source_before or validator_after != validator_before:
        raise BuildError("current fault authority changed during validation")
    return {
        "executions": executions,
        "normal": outputs[0],
        "self_test": outputs[1],
        "source": source_before,
        "validator": validator_before,
    }


def validate_static_authority(repository: Path) -> dict[str, Any]:
    validator = require_exact_file(
        STATIC_VALIDATOR,
        exact_path=STATIC_VALIDATOR,
        context="build-children static validator",
    )
    validator_before = file_identity(
        validator, "build-children static validator before validation"
    )
    outputs = []
    executions = []
    for arguments in ((), ("--self-test",)):
        value, execution = validator_output(
            [str(SYSTEM_PYTHON), "-B", str(validator), *arguments],
            repository=repository,
            context="build-children static validator",
        )
        if (
            value.get("schema") != STATIC_VALIDATOR_SCHEMA
            or value.get("status") != "ok"
        ):
            raise BuildError("build-children static validator output differs")
        checks = value.get("checks")
        hostile = value.get("hostile_mutations_rejected")
        if (
            not isinstance(checks, list)
            or not checks
            or any(not isinstance(item, str) or not item for item in checks)
            or len(checks) != len(set(checks))
            or not isinstance(hostile, int)
            or isinstance(hostile, bool)
            or hostile < 0
        ):
            raise BuildError("build-children static validator fields differ")
        outputs.append(value)
        executions.append(execution)
    if outputs[0]["checks"] != outputs[1]["checks"]:
        raise BuildError("build-children static normal/self-test checks differ")
    if outputs[0]["hostile_mutations_rejected"] != 0:
        raise BuildError("build-children static normal hostile count differs")
    if outputs[1]["hostile_mutations_rejected"] <= 0:
        raise BuildError("build-children static self-test rejected no hostiles")
    validator_after = file_identity(
        validator, "build-children static validator after validation"
    )
    if validator_after != validator_before:
        raise BuildError("build-children static validator changed during validation")
    return {
        "executions": executions,
        "normal": outputs[0],
        "self_test": outputs[1],
        "validator": validator_before,
    }


def validate_product_overlay_authority(repository: Path) -> dict[str, Any]:
    patch = require_exact_file(
        PRODUCT_OVERLAY, exact_path=PRODUCT_OVERLAY, context="product test overlay"
    )
    validator = require_exact_file(
        PRODUCT_OVERLAY_VALIDATOR,
        exact_path=PRODUCT_OVERLAY_VALIDATOR,
        context="product test overlay validator",
    )
    patch_before = file_identity(patch, "product test overlay before validation")
    validator_before = file_identity(
        validator, "product test overlay validator before validation"
    )
    outputs = []
    executions = []
    for arguments in ((), ("--self-test",)):
        value, execution = validator_output(
            [str(SYSTEM_PYTHON), "-B", str(validator), *arguments],
            repository=repository,
            context="product test overlay validator",
            canonical_output=False,
        )
        if (
            set(value)
            != {"checks", "engine_sha256", "outcome", "patch_sha256", "schema"}
            or value.get("schema") != PRODUCT_OVERLAY_VALIDATOR_SCHEMA
            or value.get("engine_sha256") != PRODUCT_ENGINE_SHA256
            or value.get("patch_sha256") != patch_before["sha256"]
        ):
            raise BuildError("product test overlay validator authority differs")
        outputs.append(value)
        executions.append(execution)
    normal_checks = outputs[0].get("checks")
    self_checks = outputs[1].get("checks")
    required_normal_checks = {
        "exact_source_and_patch_applicability",
        "original_release_line_mapping_preserved",
        "exact_test_only_eof_tail_identity",
    }
    if (
        outputs[0].get("outcome") != "PASS"
        or outputs[1].get("outcome") != "SELF_TEST_PASS"
        or not isinstance(normal_checks, list)
        or not normal_checks
        or not required_normal_checks <= set(normal_checks)
        or len(normal_checks) != len(set(normal_checks))
        or any(not isinstance(item, str) or not item for item in normal_checks)
        or not isinstance(self_checks, list)
        or len(self_checks) <= 1
        or self_checks[0] != "canonical_overlay"
        or len(self_checks) != len(set(self_checks))
        or any(not isinstance(item, str) or not item for item in self_checks)
    ):
        raise BuildError("product test overlay normal/self-test checks differ")
    patch_after = file_identity(patch, "product test overlay after validation")
    validator_after = file_identity(
        validator, "product test overlay validator after validation"
    )
    if patch_after != patch_before or validator_after != validator_before:
        raise BuildError("product test overlay authority changed during validation")
    return {
        "executions": executions,
        "hostile_mutations_rejected": len(self_checks) - 1,
        "normal": outputs[0],
        "patch": patch_before,
        "self_test": outputs[1],
        "validator": validator_before,
    }


def validate_tools_manifest(
    path: Path, *, allow_child_placeholders: bool
) -> dict[str, Any]:
    manifest = load_canonical(path, TOOLS_SCHEMA, "base tools manifest")
    if set(manifest) != {"comm_allowlist", "schema", "support_files", "tools"}:
        raise BuildError("base tools manifest top-level fields differ")
    if manifest.get("comm_allowlist") != COMM_ALLOWLIST:
        raise BuildError("base tools manifest comm allowlist differs")
    tools = manifest.get("tools")
    support = manifest.get("support_files")
    if not isinstance(tools, dict) or set(tools) != REQUIRED_TOOLS:
        raise BuildError("base tools set differs")
    if not isinstance(support, dict) or set(support) != REQUIRED_SUPPORT_FILES:
        raise BuildError("base support set differs")
    observed: set[Path] = set()
    for name, binding in sorted(tools.items()):
        if not isinstance(binding, dict) or set(binding) != {
            "comm",
            "executable_mode",
            "path",
            "sha256",
        }:
            raise BuildError(f"base tool binding fields differ: {name}")
        if binding.get("comm") != REQUIRED_TOOL_COMMS[name]:
            raise BuildError(f"base tool comm differs: {name}")
        if binding.get("executable_mode") != 0o555:
            raise BuildError(f"base tool mode differs: {name}")
        if name in CHILD_TOOLS and allow_child_placeholders:
            if binding != CHILD_PLACEHOLDER_BINDINGS[name]:
                raise BuildError(f"base child placeholder differs: {name}")
            continue
        source = require_exact_file(
            Path(str(binding.get("path"))), mode=0o555, context=f"base tool {name}"
        )
        if sha256_file(source) != binding.get("sha256") or source in observed:
            raise BuildError(f"base tool hash/path reuse differs: {name}")
        observed.add(source)
    for name, binding in sorted(support.items()):
        if not isinstance(binding, dict) or set(binding) != {"mode", "path", "sha256"}:
            raise BuildError(f"support binding fields differ: {name}")
        if binding.get("mode") != 0o444:
            raise BuildError(f"support mode differs: {name}")
        source = require_exact_file(
            Path(str(binding.get("path"))), mode=0o444, context=f"support {name}"
        )
        if (
            source.name != REQUIRED_SUPPORT_BASENAMES[name]
            or sha256_file(source) != binding.get("sha256")
            or source in observed
        ):
            raise BuildError(f"support hash/path/basename differs: {name}")
        observed.add(source)
    return manifest


def validate_lock_manifest(locks: dict[str, Any]) -> dict[str, str]:
    if locks.get("protocol") != PROTOCOL or locks.get("protocol_sha256") != PROTOCOL_SHA256:
        raise BuildError("lock manifest protocol differs")
    variants = locks.get("variants")
    if not isinstance(variants, dict) or set(variants) != {"A", "B", "C", "D"}:
        raise BuildError("lock manifest variants differ")
    toolchain = locks.get("toolchain")
    if not isinstance(toolchain, dict) or set(toolchain) != TOOLCHAIN_FIELDS:
        raise BuildError("lock manifest toolchain fields differ")
    for field in ("bwrap", "cargo", "git", "rustc", "rustup"):
        path = require_exact_file(
            Path(str(toolchain[f"{field}_path"])), context=f"pinned {field}"
        )
        if sha256_file(path) != toolchain[f"{field}_sha256"]:
            raise BuildError(f"pinned {field} hash differs")
    rustc_host = toolchain.get("rustc_host")
    if (
        not isinstance(rustc_host, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", rustc_host) is None
    ):
        raise BuildError("lock manifest rustc host differs")
    rust_lld_path = require_exact_file(
        Path(str(toolchain["rust_lld_path"])), context="pinned rust-lld"
    )
    if (
        rust_lld_path
        != Path(str(toolchain["cargo_path"])).parent.parent
        / "lib"
        / "rustlib"
        / rustc_host
        / "bin"
        / "rust-lld"
        or sha256_file(rust_lld_path) != toolchain["rust_lld_sha256"]
    ):
        raise BuildError("pinned rust-lld authority differs")
    a = variants["A"]
    if not isinstance(a, dict) or a.get("final_lock_sha256") != PRODUCT_LOCK_SHA256:
        raise BuildError("A final lock authority differs")
    # The manifest path is review metadata only.  Materialization consumes the
    # independently validated ImmutableSnapshot payload, never this path.
    if not isinstance(a.get("final_lock_path"), str):
        raise BuildError("A final lock review path is absent")
    return toolchain


def reviewed_cargo_config_policy(
    locks: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], Path, Path]:
    try:
        binding = locks["variants"]["A"]["resolver"]["cargo_config_search"]
    except (KeyError, TypeError) as error:
        raise BuildError("reviewed A Cargo config binding is absent") from error
    if not isinstance(binding, dict) or set(binding) != {"path", "sha256"}:
        raise BuildError("reviewed A Cargo config binding fields differ")
    manifest_path = require_exact_file(
        Path(str(binding["path"])),
        mode=0o444,
        context="reviewed A Cargo config search manifest",
    )
    if sha256_file(manifest_path) != binding["sha256"]:
        raise BuildError("reviewed A Cargo config search hash differs")
    empty_path = require_exact_file(
        manifest_path.with_name(f"{manifest_path.name}.empty"),
        mode=0o444,
        context="reviewed empty Cargo config",
    )
    if empty_path.stat().st_size != 0 or sha256_file(empty_path) != EMPTY_SHA256:
        raise BuildError("reviewed empty Cargo config authority differs")
    recorded = load_canonical(
        manifest_path,
        "asterism-rebaseline-cargo-config-search-v3",
        "reviewed A Cargo config search",
    )
    if set(recorded) != {"cargo_home_path", "cwd", "entries", "schema"}:
        raise BuildError("reviewed A Cargo config search fields differ")
    entries = recorded.get("entries")
    if (
        not isinstance(entries, list)
        or len(entries) != len(CARGO_CONFIG_GUEST_PATHS)
        or any(
            not isinstance(entry, dict)
            or set(entry) != {"path", "sha256", "status"}
            for entry in entries
        )
        or recorded.get("cwd") != "/asterism/source"
        or recorded.get("cargo_home_path") != GUEST_CARGO_HOME
        or [entry.get("path") for entry in entries]
        != list(CARGO_CONFIG_GUEST_PATHS)
        or any(
            entry.get("status") != "absent" or entry.get("sha256") is not None
            for entry in entries[2:-2]
        )
    ):
        raise BuildError("reviewed A Cargo config search topology differs")
    translated = json.loads(json.dumps(entries))
    if any(
        set(entry) != {"path", "sha256", "status"}
        or entry["status"] not in {"present", "absent"}
        or (
            entry["status"] == "present"
            and not lower_hex(entry["sha256"], 64)
        )
        or (entry["status"] == "absent" and entry["sha256"] is not None)
        for entry in translated
    ) or any(
        entry["status"] != "present" or entry["sha256"] is None
        for entry in (*translated[:2], *translated[-2:])
    ):
        raise BuildError("reviewed A Cargo config entry policy differs")
    return translated, {
        "binding": binding,
        "identity": file_identity(manifest_path, "reviewed Cargo config manifest"),
        "recorded": recorded,
        "translated_entries": translated,
    }, manifest_path, empty_path


def canonical_archive_member(member: tarfile.TarInfo) -> tuple[str, tuple[str, ...]]:
    name = member.name
    path_name = name[:-1] if member.isdir() and name.endswith("/") else name
    parts = tuple(path_name.split("/"))
    if (
        not path_name
        or path_name.startswith("/")
        or "\\" in path_name
        or any(part in {"", ".", ".."} for part in parts)
        or "/".join(parts) != path_name
    ):
        raise BuildError(f"unsafe archive member {name!r}")
    return path_name, parts


def extract_archive_payload(archive: bytes, destination: Path) -> None:
    if destination.exists() or destination.is_symlink():
        raise BuildError(f"archive destination is not fresh: {destination}")
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as source:
        members = source.getmembers()
        validated: list[tuple[tarfile.TarInfo, tuple[str, ...]]] = []
        kinds: dict[str, str] = {}
        for member in members:
            canonical, parts = canonical_archive_member(member)
            if canonical in kinds:
                raise BuildError(f"duplicate archive member {canonical}")
            if not member.isdir() and not member.isfile():
                raise BuildError(f"unsupported archive member {member.name}")
            kinds[canonical] = "directory" if member.isdir() else "file"
            validated.append((member, parts))
        for canonical in kinds:
            parts = canonical.split("/")
            if any(
                kinds.get("/".join(parts[:index])) == "file"
                for index in range(1, len(parts))
            ):
                raise BuildError(f"archive member descends from file: {canonical}")
        destination.mkdir(parents=True)
        for member, parts in validated:
            target = destination.joinpath(*parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            extracted = source.extractfile(member)
            if extracted is None:
                raise BuildError(f"cannot extract archive member {member.name}")
            write_new(target, extracted.read(), 0o555 if member.mode & 0o111 else 0o444)


def git_environment() -> dict[str, str]:
    return {
        "GIT_CONFIG_COUNT": "0",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
        "TZ": "UTC",
    }


def local_git_bytes(
    git: Path,
    git_sha256: str,
    repository: Path,
    arguments: Sequence[str],
    context: str,
) -> bytes:
    with RetainedFile(
        git,
        f"{context} Git executable",
        expected_sha256=git_sha256,
        require_executable=True,
        trusted_system=True,
    ) as git_lease:
        record, stdout, stderr = run_capture(
            [str(git), *arguments],
            cwd=repository,
            env=git_environment(),
            timeout=180,
            execution_lease=git_lease,
        )
    if record["exit_status"] != 0 or stderr:
        raise BuildError(f"{context} failed: {stderr!r}")
    return stdout


def product_archive(
    git: Path, git_sha256: str, repository: Path
) -> tuple[bytes, dict[str, Any]]:
    object_type = local_git_bytes(
        git,
        git_sha256,
        repository,
        ["cat-file", "-t", PRODUCT_COMMIT],
        "product object type",
    ).decode().strip()
    tree = local_git_bytes(
        git,
        git_sha256,
        repository,
        ["rev-parse", f"{PRODUCT_COMMIT}^{{tree}}"],
        "product tree",
    ).decode().strip()
    if object_type != "commit" or tree != PRODUCT_TREE:
        raise BuildError("exact frozen product commit/tree are unavailable")
    archive = local_git_bytes(
        git,
        git_sha256,
        repository,
        ["archive", "--format=tar", PRODUCT_COMMIT],
        "product archive",
    )
    return archive, {
        "bytes": len(archive),
        "commit": PRODUCT_COMMIT,
        "sha256": sha256_bytes(archive),
        "tree": PRODUCT_TREE,
    }


MANIFEST_STAT_FIELDS = (
    "st_dev",
    "st_ino",
    "st_mode",
    "st_nlink",
    "st_size",
    "st_mtime_ns",
    "st_ctime_ns",
)


def same_manifest_metadata(left: os.stat_result, right: os.stat_result) -> bool:
    return all(
        getattr(left, field) == getattr(right, field)
        for field in MANIFEST_STAT_FIELDS
    )


def manifest_entry(root: Path, path: Path) -> dict[str, Any]:
    before = path.lstat()
    if stat.S_ISLNK(before.st_mode):
        raise BuildError(f"materialized tree contains a symlink: {path}")
    if stat.S_ISDIR(before.st_mode):
        file_type = "directory"
        digest = None
    elif stat.S_ISREG(before.st_mode):
        if before.st_nlink != 1:
            raise BuildError(f"materialized file has multiple hard links: {path}")
        file_type = "regular"
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            opened_before = os.fstat(descriptor)
            chunks = []
            while chunk := os.read(descriptor, 1024 * 1024):
                chunks.append(chunk)
            opened_after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        if (
            not same_manifest_metadata(before, opened_before)
            or not same_manifest_metadata(opened_before, opened_after)
        ):
            raise BuildError(f"materialized file changed while hashing: {path}")
        payload = b"".join(chunks)
        if len(payload) != opened_after.st_size:
            raise BuildError(f"materialized file size changed while hashing: {path}")
        digest = sha256_bytes(payload)
    else:
        raise BuildError(f"materialized tree contains an unsupported node: {path}")
    after = path.lstat()
    if not same_manifest_metadata(before, after):
        raise BuildError(f"materialized path changed while snapshotting: {path}")
    if path.resolve(strict=True) != path:
        raise BuildError(f"materialized path is aliased: {path}")
    return {
        "changed_ns": before.st_ctime_ns,
        "device": before.st_dev,
        "file_type": file_type,
        "inode": before.st_ino,
        "link_count": before.st_nlink,
        "modified_ns": before.st_mtime_ns,
        "path": "." if path == root else path.relative_to(root).as_posix(),
        "permissions": stat.S_IMODE(before.st_mode),
        "sha256": digest,
        "size": before.st_size,
    }


def manifest_pass(root: Path) -> list[dict[str, Any]]:
    root_before = root.lstat()
    if stat.S_ISLNK(root_before.st_mode) or not stat.S_ISDIR(root_before.st_mode):
        raise BuildError("materialized root is not one exact directory")
    paths = [root, *sorted(root.rglob("*"))]
    if len(paths) != len(set(paths)):
        raise BuildError("materialized traversal contains duplicate paths")
    entries = [manifest_entry(root, path) for path in paths]
    root_after = root.lstat()
    if not same_manifest_metadata(root_before, root_after):
        raise BuildError("materialized root changed during tree snapshot")
    return entries


def file_manifest(root: Path) -> dict[str, Any]:
    first = manifest_pass(root)
    second = manifest_pass(root)
    if first != second:
        raise BuildError("materialized tree changed between complete snapshots")
    return {"entries": first, "schema": "bn-30fs-file-manifest-v2"}


def make_read_only(root: Path) -> None:
    for path in sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        if path.is_symlink():
            raise BuildError(f"cannot freeze symlink: {path}")
        if path.is_dir():
            if stat.S_IMODE(path.stat().st_mode) != 0o555:
                path.chmod(0o555)
        elif path.is_file():
            executable = bool(stat.S_IMODE(path.stat().st_mode) & 0o111)
            desired = 0o555 if executable else 0o444
            if stat.S_IMODE(path.stat().st_mode) != desired:
                path.chmod(desired)
        else:
            raise BuildError(f"cannot freeze unsupported path: {path}")
    if stat.S_IMODE(root.stat().st_mode) != 0o555:
        root.chmod(0o555)


def install_lock(root: Path, lock_payload: bytes) -> None:
    destination = root / LOCK_PATH
    if destination.is_symlink() or not destination.is_file():
        raise BuildError("archived Cargo.lock is absent or unsupported")
    destination.unlink()
    write_new(destination, lock_payload, 0o444)
    if sha256_file(destination) != PRODUCT_LOCK_SHA256:
        raise BuildError("materialized Cargo.lock differs")


def pinned_shared_entries() -> list[dict[str, Any]]:
    """Validate the exact shared bytes used by every current-child build kind."""

    validate_pinned_shared_overlay_set(SHARED_NAMES, error_type=BuildError)
    entries = []
    for name in SHARED_NAMES:
        payload = (SHARED / name).read_bytes()
        validate_pinned_shared_overlay_payload(
            name,
            payload,
            error_type=BuildError,
        )
        entries.append(
            {"name": name, "sha256": sha256_bytes(payload), "size": len(payload)}
        )
    return entries


def inject_shared(root: Path) -> list[dict[str, Any]]:
    placements = []
    for entry in pinned_shared_entries():
        placement = copy_new(
            SHARED / entry["name"],
            root / SHARED_DESTINATION / entry["name"],
        )
        if placement["sha256"] != entry["sha256"]:
            raise BuildError(f"copied shared overlay changed: {entry['name']}")
        placements.append(placement)
    return placements


def inject_children(root: Path) -> list[dict[str, Any]]:
    return [
        copy_new(CORRECTNESS_SOURCE, root / CORRECTNESS_DESTINATION),
        copy_new(FAULT_SOURCE, root / FAULT_DESTINATION),
        *inject_shared(root),
    ]


def inject_public(root: Path) -> list[dict[str, Any]]:
    return [
        copy_new(PUBLIC / "main.rs", root / PUBLIC_DESTINATION),
        copy_new(PUBLIC / "adapters" / "current.rs", root / ADAPTER_DESTINATION),
        *inject_shared(root),
    ]


def apply_product_overlay(
    root: Path,
    *,
    git: Path,
    git_sha256: str,
    logs: Path,
    label: str,
) -> list[dict[str, Any]]:
    environment = git_environment()
    commands = []
    with ExitStack() as stack:
        git_lease = stack.enter_context(
            RetainedFile(
                git,
                f"{label} overlay Git executable",
                expected_sha256=git_sha256,
                require_executable=True,
                trusted_system=True,
            )
        )
        patch_lease = stack.enter_context(
            RetainedFile(
                PRODUCT_OVERLAY,
                f"{label} product overlay input",
                expected_sha256=sha256_file(PRODUCT_OVERLAY),
                require_executable=False,
            )
        )
        for action, extra in (
            ("check", ["--check"]),
            ("apply", []),
        ):
            argv = [
                str(git),
                "apply",
                "--no-index",
                *extra,
                "--whitespace=error-all",
                "-",
            ]
            commands.append(
                run_logged(
                    f"git-apply-{label}-{action}",
                    argv,
                    cwd=root,
                    env=environment,
                    timeout=120,
                    log_root=logs,
                    execution_lease=git_lease,
                    stdin_payload=patch_lease.payload,
                    require_quiet=True,
                )
            )
    if sha256_file(root / ENGINE_PATH) == PRODUCT_ENGINE_SHA256:
        raise BuildError("product overlay did not change the hooked engine source")
    return commands


def materialize(
    output: Path,
    archive: bytes,
    lock_payload: bytes,
    *,
    kind: str,
    git: Path,
    git_sha256: str,
    logs: Path,
) -> dict[str, Any]:
    root = output / "materialized" / kind
    extract_archive_payload(archive, root)
    if sha256_file(root / ENGINE_PATH) != PRODUCT_ENGINE_SHA256:
        raise BuildError(f"{kind} archived engine hash differs")
    install_lock(root, lock_payload)
    if kind == "children":
        placements = inject_children(root)
        apply_log = apply_product_overlay(
            root, git=git, git_sha256=git_sha256, logs=logs, label=kind
        )
    elif kind == "pristine-release":
        placements = inject_public(root)
        apply_log = []
    elif kind == "hooked-release":
        placements = inject_public(root)
        apply_log = apply_product_overlay(
            root, git=git, git_sha256=git_sha256, logs=logs, label=kind
        )
    else:
        raise BuildError(f"unknown materialization kind: {kind}")
    make_read_only(root)
    manifest = file_manifest(root)
    manifest_path = output / "manifests" / f"materialized-{kind}.json"
    write_new(manifest_path, canonical_bytes(manifest), 0o444)
    return {
        "apply": apply_log,
        "manifest": manifest,
        "manifest_path": str(manifest_path.resolve()),
        "manifest_sha256": sha256_file(manifest_path),
        "placements": placements,
        "root": root,
    }


def cargo_environment(
    toolchain: Mapping[str, str], extra: Mapping[str, str]
) -> dict[str, str]:
    path = "/usr/bin:/bin"
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
        # /proc stays empty, so glibc needs the attested executable origin to
        # expand rustc's $ORIGIN/../lib RUNPATH.
        "LD_ORIGIN_PATH": GUEST_TOOLCHAIN_BIN,
        "PATH": path,
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "RUSTC": GUEST_RUSTC,
        "RUSTUP_HOME": "/nonexistent",
        "RUSTUP_TOOLCHAIN": toolchain["rustup_toolchain"],
        "TZ": "UTC",
    }
    overlap = set(environment) & set(extra)
    if overlap:
        raise BuildError(f"Cargo environment override: {sorted(overlap)}")
    environment.update(extra)
    return environment


def sandboxed_build_argv(
    *,
    toolchain: Mapping[str, str],
    trusted_system_args: Sequence[str],
    source_ro_bind: Sequence[str],
    toolchain_ro_bind: Sequence[str],
    cargo_bind: Sequence[str],
    rustc_bind: Sequence[str],
    rust_lld_bind: Sequence[str],
    dev_null_source: str,
    python_bind: Sequence[str],
    cargo_config_args: Sequence[str],
    target_bind: Sequence[str],
    wrapper_bind: Sequence[str] | None,
    receipt_bind: Sequence[str] | None,
    examples: Sequence[str],
) -> list[str]:
    argv = [
        toolchain["bwrap_path"],
        "--die-with-parent",
        "--new-session",
        "--unshare-net",
        *trusted_system_args,
        "--dir",
        "/dev",
        "--dev-bind",
        dev_null_source,
        "/dev/null",
        "--dir",
        "/proc",
        "--tmpfs",
        "/tmp",
        "--tmpfs",
        "/asterism",
        *source_ro_bind,
        *toolchain_ro_bind,
        *cargo_bind,
        *rustc_bind,
        *rust_lld_bind,
        *python_bind,
        *cargo_config_args,
        *target_bind,
    ]
    if wrapper_bind is not None:
        if receipt_bind is None:
            raise BuildError("wrapper build has no receipt root")
        argv.extend([*wrapper_bind, *receipt_bind])
    elif receipt_bind is not None:
        raise BuildError("release build unexpectedly has a receipt root")
    argv.extend(
        [
            "--chdir",
            "/asterism/source",
            GUEST_CARGO,
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
    argv.extend(["--target-dir", "/asterism/target"])
    return argv


def release_contract_environment(
    repository: Path,
    git: Path,
    git_sha256: str,
    adapter_sha256: str,
    shared_sha256: str,
    build_nonce: str,
) -> dict[str, str]:
    commit = local_git_bytes(
        git, git_sha256, repository, ["rev-parse", "HEAD"], "tooling commit"
    ).decode().strip()
    tree = local_git_bytes(
        git,
        git_sha256,
        repository,
        ["rev-parse", "HEAD^{tree}"],
        "tooling tree",
    ).decode().strip()
    if not lower_hex(commit, 40) or not lower_hex(tree, 40):
        raise BuildError("tooling commit/tree identity differs")
    return {
        "ASTERISM_BUILD_ADAPTER_SHA256": adapter_sha256,
        "ASTERISM_BUILD_BINARY_KIND": "public",
        "ASTERISM_BUILD_NONCE": build_nonce,
        "ASTERISM_BUILD_CARGO_LOCK_SHA256": PRODUCT_LOCK_SHA256,
        "ASTERISM_BUILD_PRODUCT_COMMIT": PRODUCT_COMMIT,
        "ASTERISM_BUILD_PRODUCT_TREE": PRODUCT_TREE,
        "ASTERISM_BUILD_PROTOCOL": PROTOCOL,
        "ASTERISM_BUILD_PROTOCOL_SHA256": PROTOCOL_SHA256,
        "ASTERISM_BUILD_SHARED_MANIFEST_SHA256": shared_sha256,
        # These twins are proof-only, never published or executed.  Calling
        # the lock review a source approval would create a false approval
        # identity and a proof/approval cycle.  Final integration must repeat
        # this equality proof under the eventual real source approval.
        "ASTERISM_BUILD_SOURCE_APPROVAL_SHA256": PREAPPROVAL_SOURCE_SENTINEL,
        "ASTERISM_BUILD_TIMED_SURFACE": "public-event-store",
        "ASTERISM_BUILD_TOOLING_COMMIT": commit,
        "ASTERISM_BUILD_TOOLING_TREE": tree,
        "ASTERISM_BUILD_VARIANT": "A",
    }


def shared_manifest_sha256() -> str:
    value = {
        "entries": pinned_shared_entries(),
        "schema": "asterism-rebaseline-shared-v3",
    }
    return sha256_bytes(canonical_bytes(value))


def directory_identity(path: Path, context: str) -> dict[str, Any]:
    metadata = path.lstat()
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or path.resolve(strict=True) != path
    ):
        raise BuildError(f"{context} is not one exact directory")
    return {
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


def descriptor_directory_identity(
    descriptor: int, path: Path, context: str
) -> dict[str, Any]:
    metadata = os.fstat(descriptor)
    if not stat.S_ISDIR(metadata.st_mode):
        raise BuildError(f"{context} descriptor is not a directory")
    return {
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


class BoundBuildDirectory:
    """Retain one writable bwrap bind and its unchanged parent selection."""

    def __init__(self, path: Path, context: str) -> None:
        self.path = path
        self.context = context
        self.parent_descriptor = -1
        self.descriptor = -1
        self.parent_before: dict[str, Any] | None = None
        self.pre_bind: dict[str, Any] | None = None
        self.post_bind: dict[str, Any] | None = None
        self.active = False

    def __enter__(self) -> BoundBuildDirectory:
        o_path = getattr(os, "O_PATH", None)
        if not isinstance(o_path, int):
            raise BuildError(f"{self.context} platform lacks O_PATH")
        flags = o_path | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
        self.parent_before = directory_identity(
            self.path.parent, f"{self.context} parent"
        )
        self.pre_bind = directory_identity(self.path, self.context)
        try:
            self.parent_descriptor = os.open(self.path.parent, flags)
            self.descriptor = os.open(
                self.path.name,
                flags,
                dir_fd=self.parent_descriptor,
            )
            if (
                descriptor_directory_identity(
                    self.parent_descriptor,
                    self.path.parent,
                    f"{self.context} parent",
                )
                != self.parent_before
                or descriptor_directory_identity(
                    self.descriptor, self.path, self.context
                )
                != self.pre_bind
            ):
                raise BuildError(f"{self.context} descriptor/path identity differs")
        except Exception:
            self.close()
            raise
        self.active = True
        return self

    def require_active(self) -> None:
        if not self.active or self.descriptor < 0:
            raise BuildError(f"{self.context} bound-directory guard is not active")

    @property
    def pass_fds(self) -> tuple[int, ...]:
        self.require_active()
        return (self.descriptor,)

    @property
    def bind_source(self) -> str:
        self.require_active()
        return f"/proc/self/fd/{self.descriptor}"

    def bwrap_bind(
        self, destination: str, *, read_only: bool
    ) -> tuple[str, str, str]:
        canonical = PurePosixPath(destination)
        if (
            not canonical.is_absolute()
            or destination != str(canonical)
            or ".." in canonical.parts
        ):
            raise BuildError(f"{self.context} bwrap destination differs")
        return (
            "--ro-bind-fd" if read_only else "--bind-fd",
            str(self.descriptor),
            destination,
        )

    def verify(self) -> None:
        self.require_active()
        assert self.parent_before is not None
        assert self.pre_bind is not None
        if (
            directory_identity(self.path.parent, f"{self.context} parent")
            != self.parent_before
            or descriptor_directory_identity(
                self.parent_descriptor,
                self.path.parent,
                f"{self.context} parent",
            )
            != self.parent_before
        ):
            raise BuildError(f"{self.context} parent selection changed")
        retained = descriptor_directory_identity(
            self.descriptor, self.path, self.context
        )
        observed = directory_identity(self.path, self.context)
        selection_fields = ("device", "file_type", "inode", "permissions")
        if any(retained[field] != observed[field] for field in selection_fields):
            raise BuildError(f"{self.context} bound selection changed")
        self.post_bind = retained

    def close(self) -> None:
        for descriptor_name in ("descriptor", "parent_descriptor"):
            descriptor = getattr(self, descriptor_name)
            if descriptor >= 0:
                os.close(descriptor)
                setattr(self, descriptor_name, -1)
        self.active = False

    def __exit__(self, child_type: Any, child_error: Any, traceback: Any) -> bool:
        verification_error = None
        try:
            self.verify()
        except BuildError as error:
            verification_error = error
        finally:
            self.close()
        if verification_error is not None:
            raise verification_error
        return False


class RecursiveTreeAuthorityGuard:
    """Retain, recursively attest, and mutation-watch one semantic input tree."""

    def __init__(
        self,
        path: Path,
        evidence_path: Path,
        role: str,
        context: str,
        *,
        allow_internal_symlinks: bool,
        hash_regular_contents: bool = True,
        trusted_system_roots: Sequence[Path] = (),
    ) -> None:
        self.path = path
        self.evidence_path = evidence_path
        self.role = role
        self.context = context
        self.allow_internal_symlinks = allow_internal_symlinks
        self.hash_regular_contents = hash_regular_contents
        self.trusted_system_roots = tuple(trusted_system_roots)
        self.stack = ExitStack()
        self.root_guard: BoundBuildDirectory | None = None
        self.evidence: RetainedFile | None = None
        self.inotify_descriptor = -1
        self.watch_descriptors: set[int] = set()
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
            raise BuildError(f"{self.context} inotify API is unavailable") from error
        initialize.argtypes = [ctypes.c_int]
        initialize.restype = ctypes.c_int
        add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        add_watch.restype = ctypes.c_int
        descriptor = initialize(os.O_CLOEXEC | os.O_NONBLOCK)
        if descriptor < 0:
            errno = ctypes.get_errno()
            raise BuildError(f"{self.context} inotify initialization failed: {errno}")
        self.inotify_descriptor = descriptor
        self._inotify_add_watch = add_watch
        self.stack.callback(self._close_inotify)

    def _close_inotify(self) -> None:
        if self.inotify_descriptor >= 0:
            os.close(self.inotify_descriptor)
            self.inotify_descriptor = -1

    def _add_directory_watch(self, descriptor: int) -> None:
        watch = self._inotify_add_watch(
            self.inotify_descriptor,
            os.fsencode(f"/proc/self/fd/{descriptor}"),
            INOTIFY_MUTATION_MASK | 0x01000000,  # IN_ONLYDIR
        )
        if watch < 0:
            errno = ctypes.get_errno()
            raise BuildError(f"{self.context} inotify watch failed: {errno}")
        self.watch_descriptors.add(watch)

    def _drain_mutation_events(self, boundary: str) -> None:
        if self.poisoned or self.inotify_descriptor < 0:
            raise BuildError(f"{self.context} mutation guard is poisoned")
        observed: list[tuple[int, int, int, int]] = []
        try:
            while True:
                try:
                    payload = os.read(self.inotify_descriptor, 1024 * 1024)
                except BlockingIOError:
                    break
                if not payload:
                    raise BuildError(
                        f"{self.context} inotify queue closed at {boundary}"
                    )
                offset = 0
                while offset < len(payload):
                    if len(payload) - offset < INOTIFY_EVENT_HEADER.size:
                        raise BuildError(
                            f"{self.context} malformed inotify header at {boundary}"
                        )
                    event = INOTIFY_EVENT_HEADER.unpack_from(payload, offset)
                    event_size = INOTIFY_EVENT_HEADER.size + event[3]
                    if event_size > len(payload) - offset:
                        raise BuildError(
                            f"{self.context} malformed inotify event at {boundary}"
                        )
                    observed.append(event)
                    offset += event_size
        except OSError as error:
            self.poisoned = True
            raise BuildError(
                f"{self.context} inotify read failed at {boundary}"
            ) from error
        if observed:
            self.poisoned = True
            masks = sorted({event[1] for event in observed})
            raise BuildError(
                f"{self.context} mutation events at {boundary}: {masks}"
            )

    def _trusted_system_entry(
        self, metadata: os.stat_result, relative: str, *, symlink: bool = False
    ) -> None:
        if self.trusted_system_roots and (
            metadata.st_uid != 0
            or (not symlink and stat.S_IMODE(metadata.st_mode) & 0o022)
            or (
                not symlink
                and os.access(
                    self.path if relative == "." else self.path / relative,
                    os.W_OK,
                )
            )
        ):
            raise BuildError(
                f"{self.context} trusted system entry is writable: {relative}"
            )

    def _regular_entry(
        self,
        parent_descriptor: int,
        name: str,
        relative: str,
        selected: os.stat_result,
    ) -> dict[str, Any]:
        access_mode = (
            os.O_RDONLY
            if self.hash_regular_contents
            else getattr(os, "O_PATH", 0)
        )
        if not self.hash_regular_contents and access_mode == 0:
            raise BuildError(
                f"{self.context} metadata-only file descriptors are unavailable"
            )
        flags = access_mode | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(name, flags, dir_fd=parent_descriptor)
        try:
            opened_before = os.fstat(descriptor)
            if not same_manifest_metadata(selected, opened_before):
                raise BuildError(f"{self.context} file selection changed")
            digest = None
            if self.hash_regular_contents:
                hasher = hashlib.sha256()
                offset = 0
                while offset < opened_before.st_size:
                    chunk = os.pread(
                        descriptor,
                        min(1024 * 1024, opened_before.st_size - offset),
                        offset,
                    )
                    if not chunk:
                        raise BuildError(f"{self.context} file read made no progress")
                    hasher.update(chunk)
                    offset += len(chunk)
                digest = hasher.hexdigest()
            opened_after = os.fstat(descriptor)
            selected_after = os.stat(
                name, dir_fd=parent_descriptor, follow_symlinks=False
            )
            if (
                not same_manifest_metadata(opened_before, opened_after)
                or not same_manifest_metadata(opened_after, selected_after)
            ):
                raise BuildError(f"{self.context} file changed while snapshotting")
        finally:
            os.close(descriptor)
        self._trusted_system_entry(selected, relative)
        return self._entry_record(selected, relative, "regular", digest, None)

    def _entry_record(
        self,
        metadata: os.stat_result,
        relative: str,
        file_type: str,
        digest: str | None,
        symlink_target: str | None,
        symlink_scope: str | None = None,
    ) -> dict[str, Any]:
        return {
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

    def _symlink_entry(
        self,
        descriptor: int,
        name: str,
        relative: str,
        selected: os.stat_result,
    ) -> dict[str, Any]:
        if not self.allow_internal_symlinks:
            raise BuildError(f"{self.context} contains a symlink: {relative}")
        target = os.readlink(name, dir_fd=descriptor)
        selected_after = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
        if not same_manifest_metadata(selected, selected_after):
            raise BuildError(f"{self.context} symlink changed while snapshotting")
        try:
            scope = recursive_symlink_scope(
                self.path,
                relative,
                target,
                trusted_system=bool(self.trusted_system_roots),
            )
        except BuildError as error:
            raise BuildError(f"{self.context} {error}") from error
        self._trusted_system_entry(selected, relative, symlink=True)
        return self._entry_record(
            selected,
            relative,
            "symlink",
            sha256_bytes(os.fsencode(target)),
            target,
            scope,
        )

    def _manifest(self, *, install_watches: bool) -> dict[str, Any]:
        if self.root_guard is None:
            raise BuildError(f"{self.context} root guard is absent")
        directory_flags = (
            os.O_RDONLY
            | os.O_DIRECTORY
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0)
        )
        root_descriptor = os.open(
            ".", directory_flags, dir_fd=self.root_guard.descriptor
        )
        entries: list[dict[str, Any]] = []

        def walk(descriptor: int, relative: str) -> None:
            before = os.fstat(descriptor)
            self._trusted_system_entry(before, relative)
            if install_watches:
                self._add_directory_watch(descriptor)
            entries.append(
                self._entry_record(before, relative, "directory", None, None)
            )
            names = sorted(os.listdir(descriptor))
            if len(names) != len(set(names)):
                raise BuildError(f"{self.context} names alias")
            for name in names:
                child_relative = name if relative == "." else f"{relative}/{name}"
                selected = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
                if stat.S_ISDIR(selected.st_mode):
                    child = os.open(name, directory_flags, dir_fd=descriptor)
                    try:
                        if not same_manifest_metadata(selected, os.fstat(child)):
                            raise BuildError(
                                f"{self.context} directory selection changed"
                            )
                        walk(child, child_relative)
                    finally:
                        os.close(child)
                elif stat.S_ISREG(selected.st_mode):
                    entries.append(
                        self._regular_entry(
                            descriptor, name, child_relative, selected
                        )
                    )
                elif stat.S_ISLNK(selected.st_mode):
                    entries.append(
                        self._symlink_entry(
                            descriptor, name, child_relative, selected
                        )
                    )
                else:
                    raise BuildError(
                        f"{self.context} contains an unsupported node: {child_relative}"
                    )
            after = os.fstat(descriptor)
            if not same_manifest_metadata(before, after):
                raise BuildError(f"{self.context} directory changed during traversal")

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
                raise BuildError(f"{self.context} hard link escapes the retained tree")
        if install_watches:
            directory_count = sum(
                entry["file_type"] == "directory" for entry in entries
            )
            if len(self.watch_descriptors) != directory_count:
                raise BuildError(f"{self.context} recursive watch coverage differs")
        entries.sort(key=lambda entry: (entry["path"] != ".", entry["path"]))
        return {
            "entries": entries,
            "role": self.role,
            "schema": RECURSIVE_TREE_AUTHORITY_SCHEMA,
        }

    def __enter__(self) -> RecursiveTreeAuthorityGuard:
        try:
            self.root_guard = self.stack.enter_context(
                BoundBuildDirectory(self.path, f"{self.context} root")
            )
            self._initialize_inotify()
            first = self._manifest(install_watches=True)
            self._drain_mutation_events("initial manifest")
            second = self._manifest(install_watches=False)
            self._drain_mutation_events("matching manifest")
            if first != second:
                raise BuildError(f"{self.context} initial manifests differ")
            payload = canonical_bytes(second)
            write_new(self.evidence_path, payload, 0o444)
            self.evidence = self.stack.enter_context(
                RetainedFile(
                    self.evidence_path,
                    f"{self.context} manifest evidence",
                    expected_sha256=sha256_bytes(payload),
                    require_executable=False,
                )
            )
            self.initial_manifest = second
            self.binding = {
                "entry_count": len(second["entries"]),
                "equal_pre_post": False,
                "manifest_path": str(self.evidence_path.resolve()),
                "manifest_sha256": sha256_bytes(payload),
                "mutation_events_absent": True,
                "role": self.role,
                "schema": RECURSIVE_TREE_AUTHORITY_SCHEMA,
                "watch_count": len(self.watch_descriptors),
            }
            self.active = True
        except Exception:
            self.stack.close()
            raise
        return self

    @property
    def pass_fds(self) -> tuple[int, ...]:
        if not self.active or self.root_guard is None:
            raise BuildError(f"{self.context} recursive guard is inactive")
        return self.root_guard.pass_fds

    def bwrap_bind(self, destination: str) -> tuple[str, str, str]:
        if self.root_guard is None:
            raise BuildError(f"{self.context} recursive guard root is absent")
        return self.root_guard.bwrap_bind(destination, read_only=True)

    def replay(self, boundary: str) -> dict[str, Any]:
        if not self.active or self.initial_manifest is None or self.binding is None:
            raise BuildError(f"{self.context} recursive guard is inactive")
        self._drain_mutation_events(f"{boundary} before replay")
        observed = self._manifest(install_watches=False)
        if observed != self.initial_manifest:
            self.poisoned = True
            raise BuildError(f"{self.context} recursive manifest changed")
        self._drain_mutation_events(f"{boundary} after replay")
        if self.evidence is None:
            raise BuildError(f"{self.context} manifest evidence is absent")
        self.evidence.verify()
        self.binding["equal_pre_post"] = True
        return dict(self.binding)

    def __exit__(self, child_type: Any, child_error: Any, traceback: Any) -> bool:
        verification_error = None
        try:
            if self.active:
                self.replay("guard close")
        except BuildError as error:
            verification_error = error
        finally:
            self.active = False
            self.stack.close()
        if verification_error is not None:
            raise verification_error
        return False


class TrustedSystemClosureGuard:
    """Expose only recursively verified, immutable system build dependencies."""

    def __init__(self, evidence_root: Path, label: str) -> None:
        self.evidence_root = evidence_root
        self.label = label
        self.stack = ExitStack()
        self.guards: list[tuple[str, RecursiveTreeAuthorityGuard]] = []
        self.evidence: RetainedFile | None = None
        self.binding: dict[str, Any] | None = None

    def __enter__(self) -> TrustedSystemClosureGuard:
        roots = tuple(path.resolve(strict=True) for path, _ in TRUSTED_SYSTEM_MOUNTS)
        if len(set(roots)) != len(roots):
            raise BuildError("trusted system closure roots alias")
        manifests = []
        try:
            for (host_path, guest_path), root in zip(
                TRUSTED_SYSTEM_MOUNTS, roots, strict=True
            ):
                if host_path != root or not root.is_dir():
                    raise BuildError(f"trusted system mount is aliased: {host_path}")
                role = "system-" + guest_path.removeprefix("/").replace("/", "-")
                guard = self.stack.enter_context(
                    RecursiveTreeAuthorityGuard(
                        root,
                        self.evidence_root / f"{self.label}-{role}.json",
                        role,
                        f"{self.label} trusted {guest_path}",
                        allow_internal_symlinks=True,
                        hash_regular_contents=False,
                        trusted_system_roots=roots,
                    )
                )
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
            payload = canonical_bytes(
                {"mounts": manifests, "schema": TRUSTED_SYSTEM_CLOSURE_SCHEMA}
            )
            evidence_path = self.evidence_root / f"{self.label}-system-closure.json"
            write_new(evidence_path, payload, 0o444)
            self.evidence = self.stack.enter_context(
                RetainedFile(
                    evidence_path,
                    f"{self.label} trusted system closure evidence",
                    expected_sha256=sha256_bytes(payload),
                    require_executable=False,
                )
            )
            entry_count = sum(
                len(item["tree"]["entries"]) for item in manifests
            )
            watch_count = sum(len(guard.watch_descriptors) for _, guard in self.guards)
            self.binding = {
                "entry_count": entry_count,
                "manifest_path": str(evidence_path.resolve()),
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
                "sha256": sha256_bytes(payload),
                "watch_count": watch_count,
            }
        except Exception:
            self.stack.close()
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
            raise BuildError("trusted system closure descriptors alias")
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
            raise BuildError("trusted system closure binding is absent")
        self.evidence.verify()
        return dict(self.binding)

    def __exit__(self, child_type: Any, child_error: Any, traceback: Any) -> bool:
        self.stack.close()
        return False


class CargoConfigSearchGuard:
    """Freeze and attest the exact eight Cargo config selections in the guest."""

    RESERVED = ("config.toml", "config")

    def __init__(
        self,
        source_root: Path,
        cargo_home: Path,
        expected_entries: Sequence[Mapping[str, Any]],
        empty_config_path: Path,
        manifest_evidence_path: Path,
        context: str,
    ) -> None:
        self.source_root = source_root
        self.cargo_home = cargo_home
        self.expected_entries = list(expected_entries)
        self.empty_config_path = empty_config_path
        self.manifest_evidence_path = manifest_evidence_path
        self.context = context
        self.stack = ExitStack()
        self.directory_guards: dict[str, BoundBuildDirectory] = {}
        self.file_leases: dict[str, RetainedFile] = {}
        self.empty_leases: dict[str, RetainedFile] = {}
        self.preserved: dict[str, list[dict[str, Any]]] = {}
        self.pre_build: dict[str, Any] | None = None
        self.post_build: dict[str, Any] | None = None
        self.inotify_descriptor = -1
        self.watch_descriptors: set[int] = set()
        self.cargo_home_tree_binding: dict[str, Any] | None = None
        self.cargo_home_tree_evidence: RetainedFile | None = None
        self.poisoned = False
        self.active = False

    @staticmethod
    def _guest_path(origin: str, name: str) -> str:
        if origin == "source":
            base = "/asterism/source/.cargo"
        elif origin == "cargo-home":
            base = GUEST_CARGO_HOME
        else:
            raise BuildError(f"unknown Cargo config origin: {origin}")
        return f"{base}/{name}"

    def _capture_directory(self, origin: str, path: Path) -> None:
        guard = self.stack.enter_context(
            BoundBuildDirectory(path, f"{self.context} {origin} directory")
        )
        self.directory_guards[origin] = guard
        preserved = []
        enumeration_descriptor = os.open(
            ".",
            os.O_RDONLY
            | os.O_DIRECTORY
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=guard.descriptor,
        )
        try:
            names = sorted(os.listdir(enumeration_descriptor))
        finally:
            os.close(enumeration_descriptor)
        if len(names) != len(set(names)):
            raise BuildError(f"{self.context} {origin} entries alias")
        for name in names:
            if name in self.RESERVED:
                continue
            candidate = path / name
            metadata = os.stat(
                name,
                dir_fd=guard.descriptor,
                follow_symlinks=False,
            )
            if stat.S_ISLNK(metadata.st_mode):
                raise BuildError(f"{self.context} {origin} contains a symlink")
            key = f"{origin}:{name}"
            if stat.S_ISREG(metadata.st_mode):
                lease = self.stack.enter_context(
                    RetainedFile(
                        candidate,
                        f"{self.context} preserved {origin} file {name}",
                        require_executable=False,
                        parent_descriptor=guard.descriptor,
                        relative_name=name,
                    )
                )
                self.file_leases[key] = lease
                preserved.append(
                    {"identity": lease.identity, "name": name, "type": "regular"}
                )
            elif stat.S_ISDIR(metadata.st_mode):
                child = self.stack.enter_context(
                    BoundBuildDirectory(
                        candidate,
                        f"{self.context} preserved {origin} directory {name}",
                    )
                )
                self.directory_guards[key] = child
                preserved.append(
                    {"identity": child.pre_bind, "name": name, "type": "directory"}
                )
            else:
                raise BuildError(
                    f"{self.context} {origin} contains an unsupported entry"
                )
        self.preserved[origin] = preserved

    def _initialize_inotify(self) -> None:
        library = ctypes.CDLL(None, use_errno=True)
        try:
            initialize = library.inotify_init1
            add_watch = library.inotify_add_watch
        except AttributeError as error:
            raise BuildError(f"{self.context} inotify API is unavailable") from error
        initialize.argtypes = [ctypes.c_int]
        initialize.restype = ctypes.c_int
        add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        add_watch.restype = ctypes.c_int
        descriptor = initialize(os.O_CLOEXEC | os.O_NONBLOCK)
        if descriptor < 0:
            errno = ctypes.get_errno()
            raise BuildError(f"{self.context} inotify initialization failed: {errno}")
        self.inotify_descriptor = descriptor
        self._inotify_add_watch = add_watch
        self.stack.callback(self._close_inotify)

    def _close_inotify(self) -> None:
        if self.inotify_descriptor >= 0:
            os.close(self.inotify_descriptor)
            self.inotify_descriptor = -1

    def _add_directory_watch(self, descriptor: int) -> None:
        if self.inotify_descriptor < 0:
            raise BuildError(f"{self.context} inotify guard is inactive")
        watch = self._inotify_add_watch(
            self.inotify_descriptor,
            os.fsencode(f"/proc/self/fd/{descriptor}"),
            INOTIFY_MUTATION_MASK | 0x01000000,  # IN_ONLYDIR
        )
        if watch < 0:
            errno = ctypes.get_errno()
            raise BuildError(f"{self.context} inotify watch failed: {errno}")
        self.watch_descriptors.add(watch)

    def _drain_mutation_events(self, boundary: str) -> None:
        if self.poisoned or self.inotify_descriptor < 0:
            raise BuildError(f"{self.context} Cargo-home mutation guard is poisoned")
        observed = []
        try:
            while True:
                try:
                    payload = os.read(self.inotify_descriptor, 1024 * 1024)
                except BlockingIOError:
                    break
                if not payload:
                    raise BuildError(
                        f"{self.context} inotify queue closed at {boundary}"
                    )
                offset = 0
                while offset < len(payload):
                    if len(payload) - offset < INOTIFY_EVENT_HEADER.size:
                        raise BuildError(
                            f"{self.context} malformed inotify header at {boundary}"
                        )
                    watch, mask, cookie, name_length = INOTIFY_EVENT_HEADER.unpack_from(
                        payload, offset
                    )
                    event_size = INOTIFY_EVENT_HEADER.size + name_length
                    if event_size > len(payload) - offset:
                        raise BuildError(
                            f"{self.context} malformed inotify event at {boundary}"
                        )
                    observed.append((watch, mask, cookie, name_length))
                    offset += event_size
        except OSError as error:
            self.poisoned = True
            raise BuildError(
                f"{self.context} inotify read failed at {boundary}"
            ) from error
        if observed:
            self.poisoned = True
            masks = sorted({mask for _, mask, _, _ in observed})
            raise BuildError(
                f"{self.context} Cargo-home mutation events at {boundary}: {masks}"
            )

    def _regular_tree_entry(
        self,
        parent_descriptor: int,
        name: str,
        relative: str,
        selected: os.stat_result,
    ) -> dict[str, Any]:
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_descriptor,
        )
        try:
            opened_before = os.fstat(descriptor)
            if not same_manifest_metadata(selected, opened_before):
                raise BuildError(f"{self.context} Cargo-home file selection changed")
            offset = 0
            digest = hashlib.sha256()
            while offset < opened_before.st_size:
                chunk = os.pread(
                    descriptor,
                    min(1024 * 1024, opened_before.st_size - offset),
                    offset,
                )
                if not chunk:
                    raise BuildError(
                        f"{self.context} Cargo-home file read made no progress"
                    )
                digest.update(chunk)
                offset += len(chunk)
            opened_after = os.fstat(descriptor)
            selected_after = os.stat(
                name, dir_fd=parent_descriptor, follow_symlinks=False
            )
            if (
                offset != opened_before.st_size
                or not same_manifest_metadata(opened_before, opened_after)
                or not same_manifest_metadata(opened_after, selected_after)
            ):
                raise BuildError(f"{self.context} Cargo-home file changed while hashing")
        finally:
            os.close(descriptor)
        return {
            "changed_ns": selected.st_ctime_ns,
            "device": selected.st_dev,
            "file_type": "regular",
            "gid": selected.st_gid,
            "inode": selected.st_ino,
            "link_count": selected.st_nlink,
            "modified_ns": selected.st_mtime_ns,
            "path": relative,
            "permissions": stat.S_IMODE(selected.st_mode),
            "sha256": digest.hexdigest(),
            "size": selected.st_size,
            "symlink_target": None,
            "symlink_scope": None,
            "uid": selected.st_uid,
        }

    def _cargo_home_tree_manifest(self, *, install_watches: bool) -> dict[str, Any]:
        root_guard = self.directory_guards["cargo-home"]
        directory_flags = (
            os.O_RDONLY
            | os.O_DIRECTORY
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0)
        )
        root_descriptor = os.open(
            ".", directory_flags, dir_fd=root_guard.descriptor
        )
        entries: list[dict[str, Any]] = []

        def directory_entry(metadata: os.stat_result, relative: str) -> dict[str, Any]:
            return {
                "changed_ns": metadata.st_ctime_ns,
                "device": metadata.st_dev,
                "file_type": "directory",
                "gid": metadata.st_gid,
                "inode": metadata.st_ino,
                "link_count": metadata.st_nlink,
                "modified_ns": metadata.st_mtime_ns,
                "path": relative,
                "permissions": stat.S_IMODE(metadata.st_mode),
                "sha256": None,
                "size": metadata.st_size,
                "symlink_target": None,
                "symlink_scope": None,
                "uid": metadata.st_uid,
            }

        def walk(descriptor: int, relative: str) -> None:
            before = os.fstat(descriptor)
            if not stat.S_ISDIR(before.st_mode):
                raise BuildError(f"{self.context} Cargo-home directory changed type")
            if install_watches:
                self._add_directory_watch(descriptor)
            entries.append(directory_entry(before, relative))
            names = sorted(os.listdir(descriptor))
            if len(names) != len(set(names)):
                raise BuildError(f"{self.context} Cargo-home names alias")
            for name in names:
                child_relative = name if relative == "." else f"{relative}/{name}"
                selected = os.stat(
                    name, dir_fd=descriptor, follow_symlinks=False
                )
                if stat.S_ISDIR(selected.st_mode):
                    child = os.open(name, directory_flags, dir_fd=descriptor)
                    try:
                        if not same_manifest_metadata(selected, os.fstat(child)):
                            raise BuildError(
                                f"{self.context} Cargo-home directory selection changed"
                            )
                        walk(child, child_relative)
                    finally:
                        os.close(child)
                elif stat.S_ISREG(selected.st_mode):
                    entries.append(
                        self._regular_tree_entry(
                            descriptor, name, child_relative, selected
                        )
                    )
                elif stat.S_ISLNK(selected.st_mode):
                    target = os.readlink(name, dir_fd=descriptor)
                    selected_after = os.stat(
                        name, dir_fd=descriptor, follow_symlinks=False
                    )
                    if not same_manifest_metadata(selected, selected_after):
                        raise BuildError(
                            f"{self.context} Cargo-home symlink changed while reading"
                        )
                    resolved = (
                        self.cargo_home
                        / (Path(child_relative).parent)
                        / target
                    ).resolve(strict=True)
                    if resolved != self.cargo_home and self.cargo_home not in resolved.parents:
                        raise BuildError(
                            f"{self.context} Cargo-home symlink escapes the frozen root"
                        )
                    entries.append(
                        {
                            "changed_ns": selected.st_ctime_ns,
                            "device": selected.st_dev,
                            "file_type": "symlink",
                            "gid": selected.st_gid,
                            "inode": selected.st_ino,
                            "link_count": selected.st_nlink,
                            "modified_ns": selected.st_mtime_ns,
                            "path": child_relative,
                            "permissions": stat.S_IMODE(selected.st_mode),
                            "sha256": sha256_bytes(os.fsencode(target)),
                            "size": selected.st_size,
                            "symlink_target": target,
                            "symlink_scope": "within_root",
                            "uid": selected.st_uid,
                        }
                    )
                else:
                    raise BuildError(
                        f"{self.context} Cargo-home contains an unsupported node"
                    )
            after = os.fstat(descriptor)
            if not same_manifest_metadata(before, after):
                raise BuildError(
                    f"{self.context} Cargo-home directory changed during traversal"
                )

        try:
            walk(root_descriptor, ".")
        finally:
            os.close(root_descriptor)
        hardlinks: dict[tuple[int, int], list[dict[str, Any]]] = {}
        for entry in entries:
            if entry["file_type"] == "regular":
                hardlinks.setdefault(
                    (entry["device"], entry["inode"]), []
                ).append(entry)
        for aliases in hardlinks.values():
            if len(aliases) != aliases[0]["link_count"]:
                raise BuildError(
                    f"{self.context} Cargo-home hard link escapes the frozen tree"
                )
        if install_watches:
            directory_count = sum(
                entry["file_type"] == "directory" for entry in entries
            )
            if len(self.watch_descriptors) != directory_count:
                raise BuildError(f"{self.context} Cargo-home watch coverage differs")
        entries.sort(key=lambda entry: (entry["path"] != ".", entry["path"]))
        return {
            "entries": entries,
            "role": "cargo_home",
            "schema": CARGO_HOME_TREE_SCHEMA,
        }

    def _capture_cargo_home_tree_authority(self) -> None:
        self._initialize_inotify()
        first = self._cargo_home_tree_manifest(install_watches=True)
        self._drain_mutation_events("initial manifest")
        second = self._cargo_home_tree_manifest(install_watches=False)
        self._drain_mutation_events("matching manifest")
        if first != second:
            raise BuildError(f"{self.context} Cargo-home initial manifests differ")
        payload = canonical_bytes(second)
        write_new(self.manifest_evidence_path, payload, 0o444)
        evidence = self.stack.enter_context(
            RetainedFile(
                self.manifest_evidence_path,
                f"{self.context} Cargo-home tree evidence",
                expected_sha256=sha256_bytes(payload),
                require_executable=False,
            )
        )
        self.cargo_home_tree_evidence = evidence
        self.cargo_home_tree_binding = {
            "entry_count": len(second["entries"]),
            "equal_pre_post": True,
            "path": str(self.manifest_evidence_path.resolve()),
            "post_sha256": sha256_bytes(payload),
            "pre_sha256": sha256_bytes(payload),
            "watch_count": len(self.watch_descriptors),
        }

    def _replay_cargo_home_tree(self, boundary: str, *, deep: bool) -> None:
        self._drain_mutation_events(f"{boundary} before replay")
        if deep:
            manifest = self._cargo_home_tree_manifest(install_watches=False)
            payload = canonical_bytes(manifest)
            observed_sha256 = sha256_bytes(payload)
            assert self.cargo_home_tree_binding is not None
            if (
                observed_sha256 != self.cargo_home_tree_binding["pre_sha256"]
                or len(manifest["entries"])
                != self.cargo_home_tree_binding["entry_count"]
            ):
                self.poisoned = True
                raise BuildError(
                    f"{self.context} Cargo-home recursive content changed"
                )
            self.cargo_home_tree_binding["post_sha256"] = observed_sha256
            self.cargo_home_tree_binding["equal_pre_post"] = True
        self._drain_mutation_events(f"{boundary} after replay")
        if self.cargo_home_tree_evidence is None:
            raise BuildError(f"{self.context} Cargo-home evidence is absent")
        self.cargo_home_tree_evidence.verify()

    def __enter__(self) -> CargoConfigSearchGuard:
        if (
            len(self.expected_entries) != len(CARGO_CONFIG_GUEST_PATHS)
            or [entry.get("path") for entry in self.expected_entries]
            != list(CARGO_CONFIG_GUEST_PATHS)
        ):
            raise BuildError(f"{self.context} expected Cargo search order differs")
        try:
            self._capture_directory("source", self.source_root / ".cargo")
            self._capture_directory("cargo-home", self.cargo_home)
            self._capture_cargo_home_tree_authority()
            for origin, directory in (
                ("source", self.source_root / ".cargo"),
                ("cargo-home", self.cargo_home),
            ):
                for name in self.RESERVED:
                    key = f"{origin}:{name}"
                    candidate = directory / name
                    guest_path = self._guest_path(origin, name)
                    expected = next(
                        (
                            entry
                            for entry in self.expected_entries
                            if entry.get("path") == guest_path
                        ),
                        None,
                    )
                    if not isinstance(expected, Mapping):
                        raise BuildError(
                            f"{self.context} expected Cargo config is absent"
                        )
                    try:
                        metadata = os.stat(
                            name,
                            dir_fd=self.directory_guards[origin].descriptor,
                            follow_symlinks=False,
                        )
                    except FileNotFoundError:
                        if expected == {
                            "path": guest_path,
                            "sha256": EMPTY_SHA256,
                            "status": "present",
                        }:
                            lease = self.stack.enter_context(
                                RetainedFile(
                                    self.empty_config_path,
                                    f"{self.context} empty {origin} {name}",
                                    expected_sha256=EMPTY_SHA256,
                                    require_executable=False,
                                )
                            )
                            if (
                                lease.identity is None
                                or lease.identity["mode"] != 0o444
                                or lease.identity["size"] != 0
                            ):
                                raise BuildError(
                                    f"{self.context} empty Cargo config differs"
                                )
                            self.empty_leases[key] = lease
                        continue
                    if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(
                        metadata.st_mode
                    ):
                        raise BuildError(f"{self.context} config is not regular")
                    lease = self.stack.enter_context(
                        RetainedFile(
                            candidate,
                            f"{self.context} {origin} {name}",
                            require_executable=False,
                            parent_descriptor=self.directory_guards[
                                origin
                            ].descriptor,
                            relative_name=name,
                        )
                    )
                    self.file_leases[key] = lease
            self.active = True
            self.pre_build = self.replay(boundary="guard activation")
        except Exception:
            self.stack.close()
            self.active = False
            raise
        return self

    def _entry(self, guest_path: str, origin: str, name: str) -> dict[str, Any]:
        key = f"{origin}:{name}"
        lease = self.file_leases.get(key)
        if lease is None:
            empty_lease = self.empty_leases.get(key)
            if empty_lease is not None:
                guard = self.directory_guards[origin]
                try:
                    os.stat(name, dir_fd=guard.descriptor, follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    raise BuildError(
                        f"{self.context} empty-bound Cargo config appeared"
                    )
                empty_lease.verify()
                if (
                    empty_lease.identity is None
                    or empty_lease.identity["mode"] != 0o444
                    or empty_lease.identity["size"] != 0
                    or empty_lease.identity["sha256"] != EMPTY_SHA256
                ):
                    raise BuildError(f"{self.context} empty Cargo config changed")
                return {
                    "path": guest_path,
                    "sha256": EMPTY_SHA256,
                    "status": "present",
                }
            guard = self.directory_guards[origin]
            try:
                os.stat(name, dir_fd=guard.descriptor, follow_symlinks=False)
            except FileNotFoundError:
                return {"path": guest_path, "sha256": None, "status": "absent"}
            raise BuildError(f"{self.context} absent Cargo config appeared")
        lease.verify()
        assert lease.identity is not None
        return {
            "path": guest_path,
            "sha256": lease.identity["sha256"],
            "status": "present",
        }

    def replay(
        self, *, boundary: str = "replay", deep: bool = False
    ) -> dict[str, Any]:
        if not self.active:
            raise BuildError(f"{self.context} Cargo config guard is inactive")
        self._replay_cargo_home_tree(boundary, deep=deep)
        self.directory_guards["source"].verify()
        self.directory_guards["cargo-home"].verify()
        entries = [
            self._entry(CARGO_CONFIG_GUEST_PATHS[0], "source", "config.toml"),
            self._entry(CARGO_CONFIG_GUEST_PATHS[1], "source", "config"),
        ]
        entries.extend(
            {"path": path, "sha256": None, "status": "absent"}
            for path in CARGO_CONFIG_GUEST_PATHS[2:6]
        )
        entries.extend(
            [
                self._entry(CARGO_CONFIG_GUEST_PATHS[6], "cargo-home", "config.toml"),
                self._entry(CARGO_CONFIG_GUEST_PATHS[7], "cargo-home", "config"),
            ]
        )
        if entries != self.expected_entries:
            raise BuildError(f"{self.context} Cargo config selection differs")
        record = {
            "cargo_search": {
                "cargo_home_path": GUEST_CARGO_HOME,
                "cwd": "/asterism/source",
                "entries": entries,
                "schema": "asterism-rebaseline-cargo-config-search-v3",
            },
            "cargo_home_tree": dict(self.cargo_home_tree_binding or {}),
            "preserved_top_level_entries": self.preserved,
            "schema": CARGO_CONFIG_SEARCH_SCHEMA,
        }
        return record

    @property
    def pass_fds(self) -> tuple[int, ...]:
        descriptors = [
            *(guard.descriptor for guard in self.directory_guards.values()),
            *(lease.descriptor for lease in self.file_leases.values()),
            *(lease.descriptor for lease in self.empty_leases.values()),
        ]
        if not self.active or any(descriptor < 0 for descriptor in descriptors):
            raise BuildError(f"{self.context} Cargo config descriptors differ")
        if len(descriptors) != len(set(descriptors)):
            raise BuildError(f"{self.context} Cargo config descriptors alias")
        return tuple(descriptors)

    def prepare_bwrap_args(self) -> list[str]:
        """Rewind all data FDs immediately before bwrap consumes them."""

        self.replay(boundary="bwrap argument preparation")
        for lease in self.file_leases.values():
            lease.rewind_for_bind_data()
        for lease in self.empty_leases.values():
            lease.rewind_for_bind_data()
        arguments: list[str] = []
        source_guest = "/asterism/source/.cargo"
        arguments.extend(["--dir", source_guest, "--tmpfs", source_guest])
        for entry in self.preserved["source"]:
            key = f"source:{entry['name']}"
            destination = f"{source_guest}/{entry['name']}"
            if entry["type"] == "regular":
                arguments.extend(
                    [
                        "--ro-bind-data",
                        str(self.file_leases[key].descriptor),
                        destination,
                    ]
                )
            else:
                arguments.extend(
                    [
                        "--ro-bind-fd",
                        str(self.directory_guards[key].descriptor),
                        destination,
                    ]
                )
        for name in self.RESERVED:
            lease = self.file_leases.get(f"source:{name}")
            if lease is not None:
                arguments.extend(
                    ["--ro-bind-data", str(lease.descriptor), f"{source_guest}/{name}"]
                )
            else:
                empty_lease = self.empty_leases.get(f"source:{name}")
                if empty_lease is not None:
                    arguments.extend(
                        [
                            "--ro-bind-data",
                            str(empty_lease.descriptor),
                            f"{source_guest}/{name}",
                        ]
                    )
        arguments.extend(["--remount-ro", source_guest])

        arguments.extend(
            [
                "--overlay-src",
                f"/proc/self/fd/{self.directory_guards['cargo-home'].descriptor}",
                "--tmp-overlay",
                GUEST_CARGO_HOME,
            ]
        )
        for name in self.RESERVED:
            lease = self.file_leases.get(f"cargo-home:{name}")
            if lease is not None:
                arguments.extend(
                    [
                        "--ro-bind-data",
                        str(lease.descriptor),
                        f"{GUEST_CARGO_HOME}/{name}",
                    ]
                )
            else:
                empty_lease = self.empty_leases.get(f"cargo-home:{name}")
                if empty_lease is not None:
                    arguments.extend(
                        [
                            "--ro-bind-data",
                            str(empty_lease.descriptor),
                            f"{GUEST_CARGO_HOME}/{name}",
                        ]
                    )
        arguments.extend(["--remount-ro", GUEST_CARGO_HOME])
        arguments.extend(
            [
                "--dir",
                "/asterism/.cargo",
                "--tmpfs",
                "/asterism/.cargo",
                "--remount-ro",
                "/asterism/.cargo",
                "--dir",
                "/.cargo",
                "--tmpfs",
                "/.cargo",
                "--remount-ro",
                "/.cargo",
            ]
        )
        return arguments

    def __exit__(self, child_type: Any, child_error: Any, traceback: Any) -> bool:
        verification_error = None
        try:
            self.post_build = self.replay(boundary="guard exit")
            if self.post_build != self.pre_build:
                raise BuildError(f"{self.context} Cargo config changed during build")
        except BuildError as error:
            verification_error = error
        try:
            self.stack.close()
        finally:
            self.active = False
        if verification_error is not None:
            raise verification_error
        return False


def cargo_example_hardlink_aliases(
    directory_descriptor: int,
    name: str,
    metadata: os.stat_result,
    context: str,
) -> list[str]:
    if metadata.st_nlink != 2:
        raise BuildError(f"{context} Cargo hard-link count differs")
    enumeration_descriptor = os.open(
        ".",
        os.O_RDONLY
        | os.O_DIRECTORY
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0),
        dir_fd=directory_descriptor,
    )
    try:
        aliases = []
        for candidate in sorted(os.listdir(enumeration_descriptor)):
            observed = os.stat(
                candidate,
                dir_fd=enumeration_descriptor,
                follow_symlinks=False,
            )
            if (observed.st_dev, observed.st_ino) != (
                metadata.st_dev,
                metadata.st_ino,
            ):
                continue
            if not stat.S_ISREG(observed.st_mode) or not same_manifest_metadata(
                observed, metadata
            ):
                raise BuildError(f"{context} Cargo hard-link identity differs")
            aliases.append(candidate)
    finally:
        os.close(enumeration_descriptor)
    if len(aliases) != metadata.st_nlink:
        raise BuildError(f"{context} Cargo hard link escapes the target directory")
    hashed = [
        alias
        for alias in aliases
        if alias != name
        and re.fullmatch(rf"{re.escape(name)}-[0-9a-f]{{16}}", alias)
        is not None
    ]
    if len(aliases) != 2 or name not in aliases or len(hashed) != 1:
        raise BuildError(f"{context} Cargo hard-link aliases differ")
    return aliases


def read_bound_regular_file(
    root_descriptor: int,
    relative: Path,
    context: str,
    *,
    require_executable: bool,
    expected_link_count: int = 1,
    normalize_mode: int | None = None,
) -> tuple[bytes, dict[str, Any]]:
    if (
        not isinstance(expected_link_count, int)
        or isinstance(expected_link_count, bool)
        or expected_link_count not in {1, 2}
    ):
        raise BuildError(f"{context} expected hard-link count differs")
    if normalize_mode is not None and normalize_mode != 0o555:
        raise BuildError(f"{context} normalized mode differs")
    if (
        relative.is_absolute()
        or not relative.parts
        or any(part in {"", ".", ".."} for part in relative.parts)
    ):
        raise BuildError(f"{context} relative path differs")
    o_path = getattr(os, "O_PATH", None)
    if not isinstance(o_path, int):
        raise BuildError(f"{context} platform lacks O_PATH")
    directory_flags = (
        o_path | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    )
    directories = [os.dup(root_descriptor)]
    snapshots = [
        descriptor_directory_identity(
            directories[0], Path(f"/proc/self/fd/{root_descriptor}"), context
        )
    ]
    file_descriptor = -1
    try:
        for component in relative.parts[:-1]:
            child = os.open(
                component,
                directory_flags,
                dir_fd=directories[-1],
            )
            directories.append(child)
            snapshots.append(
                descriptor_directory_identity(
                    child,
                    Path(f"/proc/self/fd/{root_descriptor}")
                    / Path(*relative.parts[: len(directories) - 1]),
                    context,
                )
            )
        file_descriptor = os.open(
            relative.parts[-1],
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directories[-1],
        )
        opened_before = os.fstat(file_descriptor)
        aliases_before = None
        if expected_link_count == 2:
            aliases_before = cargo_example_hardlink_aliases(
                directories[-1],
                relative.parts[-1],
                opened_before,
                context,
            )
        if normalize_mode is not None:
            if (
                not stat.S_ISREG(opened_before.st_mode)
                or opened_before.st_nlink != expected_link_count
                or (require_executable and opened_before.st_mode & 0o111 == 0)
            ):
                raise BuildError(f"{context} pre-normalization identity differs")
            os.fchmod(file_descriptor, normalize_mode)
            normalized = os.fstat(file_descriptor)
            stable_fields = ("st_dev", "st_ino", "st_nlink", "st_size", "st_mtime_ns")
            if (
                any(
                    getattr(opened_before, field) != getattr(normalized, field)
                    for field in stable_fields
                )
                or stat.S_IMODE(normalized.st_mode) != normalize_mode
            ):
                raise BuildError(f"{context} mode normalization changed identity")
            opened_before = normalized
            if expected_link_count == 2:
                normalized_aliases = cargo_example_hardlink_aliases(
                    directories[-1],
                    relative.parts[-1],
                    opened_before,
                    context,
                )
                if normalized_aliases != aliases_before:
                    raise BuildError(
                        f"{context} Cargo hard-link aliases changed during normalization"
                    )
                aliases_before = normalized_aliases
        logical_path = Path(f"/proc/self/fd/{root_descriptor}") / relative
        payload, identity = snapshot_open_file(
            file_descriptor,
            logical_path,
            context,
            require_executable=require_executable,
            expected_link_count=expected_link_count,
        )
        relative_metadata = os.stat(
            relative.parts[-1],
            dir_fd=directories[-1],
            follow_symlinks=False,
        )
        opened_metadata = os.fstat(file_descriptor)
        if (
            not same_manifest_metadata(relative_metadata, opened_metadata)
            or identity["device"] != opened_metadata.st_dev
            or identity["inode"] != opened_metadata.st_ino
            or identity["link_count"] != opened_metadata.st_nlink
            or identity["mode"] != stat.S_IMODE(opened_metadata.st_mode)
            or identity["size"] != opened_metadata.st_size
            or identity["mtime_ns"] != opened_metadata.st_mtime_ns
            or identity["ctime_ns"] != opened_metadata.st_ctime_ns
        ):
            raise BuildError(f"{context} relative file selection changed")
        if expected_link_count == 2:
            aliases_after = cargo_example_hardlink_aliases(
                directories[-1],
                relative.parts[-1],
                opened_metadata,
                context,
            )
            if aliases_after != aliases_before:
                raise BuildError(f"{context} Cargo hard-link aliases changed")
        for descriptor, expected in zip(directories, snapshots, strict=True):
            if (
                descriptor_directory_identity(
                    descriptor, Path(expected["path"]), context
                )
                != expected
            ):
                raise BuildError(f"{context} directory chain changed while reading")
        return payload, identity
    finally:
        if file_descriptor >= 0:
            os.close(file_descriptor)
        for descriptor in reversed(directories):
            os.close(descriptor)


def copy_bound_artifact(
    root_descriptor: int,
    relative: Path,
    destination: Path,
) -> dict[str, Any]:
    payload, source_identity = read_bound_regular_file(
        root_descriptor,
        relative,
        f"built artifact {relative.name}",
        require_executable=True,
        expected_link_count=2,
        normalize_mode=0o555,
    )
    write_new(destination, payload, 0o555)
    binding = {
        "comm": destination.name,
        "executable_mode": 0o555,
        "path": str(destination.resolve()),
        "sha256": sha256_file(destination),
    }
    if (
        binding["sha256"] != source_identity["sha256"]
        or file_identity(destination, f"copied artifact {destination.name}")["mode"]
        != 0o555
    ):
        raise BuildError(f"copied artifact differs: {destination}")
    return {"binding": binding, "source": source_identity}


def cargo_target_artifact_paths(
    root_descriptor: int,
    target: Path,
    examples: Sequence[str],
    context: str,
) -> set[Path]:
    if target.resolve(strict=True) != target:
        raise BuildError(f"{context} target path differs")
    directory_flags = (
        getattr(os, "O_PATH", os.O_RDONLY)
        | os.O_DIRECTORY
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0)
    )
    directories = [os.dup(root_descriptor)]
    try:
        for component in ("release", "examples"):
            directories.append(
                os.open(component, directory_flags, dir_fd=directories[-1])
            )
        examples_descriptor = directories[-1]
        allowed = set()
        for example in examples:
            metadata = os.stat(
                example,
                dir_fd=examples_descriptor,
                follow_symlinks=False,
            )
            aliases = cargo_example_hardlink_aliases(
                examples_descriptor, example, metadata, context
            )
            allowed.update(
                Path("release") / "examples" / alias for alias in aliases
            )
        if len(allowed) != len(examples) * 2:
            raise BuildError(f"{context} Cargo artifact alias cardinality differs")
        return allowed
    finally:
        for descriptor in reversed(directories):
            os.close(descriptor)


def snapshot_cargo_target(
    root_descriptor: int,
    target: Path,
    context: str,
) -> tuple[
    dict[Path, os.stat_result],
    dict[Path, os.stat_result],
    dict[Path, tuple[str, ...]],
]:
    root_identity = descriptor_directory_identity(
        root_descriptor, target, context
    )
    live_identity = directory_identity(target, context)
    selection_fields = ("device", "file_type", "inode", "permissions")
    if any(
        root_identity[field] != live_identity[field]
        for field in selection_fields
    ):
        raise BuildError(f"{context} retained target selection differs")
    directory_flags = (
        os.O_RDONLY
        | os.O_DIRECTORY
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0)
    )
    root = os.open(".", directory_flags, dir_fd=root_descriptor)
    directories: dict[Path, os.stat_result] = {}
    regular_files: dict[Path, os.stat_result] = {}
    children: dict[Path, tuple[str, ...]] = {}

    def walk(descriptor: int, relative: Path) -> None:
        before = os.fstat(descriptor)
        if not stat.S_ISDIR(before.st_mode):
            raise BuildError(f"{context} Cargo target directory changed type")
        names = tuple(sorted(os.listdir(descriptor)))
        if len(names) != len(set(names)):
            raise BuildError(f"{context} Cargo target names alias")
        directories[relative] = before
        children[relative] = names
        for name in names:
            selected = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            child_relative = relative / name
            if stat.S_ISDIR(selected.st_mode):
                child = os.open(name, directory_flags, dir_fd=descriptor)
                try:
                    opened = os.fstat(child)
                    if not same_manifest_metadata(selected, opened):
                        raise BuildError(
                            f"{context} Cargo target directory selection changed"
                        )
                    walk(child, child_relative)
                    selected_after = os.stat(
                        name, dir_fd=descriptor, follow_symlinks=False
                    )
                    if not same_manifest_metadata(opened, selected_after):
                        raise BuildError(
                            f"{context} Cargo target directory changed during snapshot"
                        )
                finally:
                    os.close(child)
            elif stat.S_ISREG(selected.st_mode):
                regular_files[child_relative] = selected
            elif stat.S_ISLNK(selected.st_mode):
                raise BuildError(f"{context} Cargo target contains a symlink")
            else:
                raise BuildError(
                    f"{context} Cargo target contains an unsupported node"
                )
        if not same_manifest_metadata(before, os.fstat(descriptor)):
            raise BuildError(f"{context} Cargo target changed during snapshot")

    try:
        walk(root, Path())
    finally:
        os.close(root)
    hardlinks: dict[tuple[int, int], list[os.stat_result]] = {}
    for metadata in regular_files.values():
        hardlinks.setdefault((metadata.st_dev, metadata.st_ino), []).append(metadata)
    for aliases in hardlinks.values():
        if (
            len(aliases) != aliases[0].st_nlink
            or any(
                not same_manifest_metadata(aliases[0], alias)
                for alias in aliases[1:]
            )
        ):
            raise BuildError(f"{context} Cargo target hard link escapes the target")
    return directories, regular_files, children


def prune_cargo_target(
    root_descriptor: int,
    target: Path,
    examples: Sequence[str],
    context: str,
) -> None:
    allowed_files = cargo_target_artifact_paths(
        root_descriptor, target, examples, context
    )
    allowed_directories = {Path()}
    for path in allowed_files:
        allowed_directories.update(
            parent for parent in path.parents if parent != Path(".")
        )
    directories, regular_files, children = snapshot_cargo_target(
        root_descriptor, target, context
    )
    if not allowed_files <= set(regular_files):
        raise BuildError(f"{context} Cargo artifact is absent from target snapshot")
    directory_flags = (
        os.O_RDONLY
        | os.O_DIRECTORY
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0)
    )
    root = os.open(".", directory_flags, dir_fd=root_descriptor)

    def same_selection(
        observed: os.stat_result, expected: os.stat_result
    ) -> bool:
        return (
            observed.st_dev,
            observed.st_ino,
            stat.S_IFMT(observed.st_mode),
        ) == (
            expected.st_dev,
            expected.st_ino,
            stat.S_IFMT(expected.st_mode),
        )

    def prune(descriptor: int, relative: Path) -> None:
        names = tuple(sorted(os.listdir(descriptor)))
        if names != children.get(relative):
            raise BuildError(f"{context} Cargo target names changed before pruning")
        for name in names:
            child_relative = relative / name
            selected = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            if child_relative in directories:
                if not same_manifest_metadata(
                    selected, directories[child_relative]
                ):
                    raise BuildError(
                        f"{context} Cargo target directory changed before pruning"
                    )
                child = os.open(name, directory_flags, dir_fd=descriptor)
                try:
                    if not same_selection(os.fstat(child), selected):
                        raise BuildError(
                            f"{context} Cargo target directory selection changed"
                        )
                    prune(child, child_relative)
                    retained = os.fstat(child)
                    selected_after = os.stat(
                        name, dir_fd=descriptor, follow_symlinks=False
                    )
                    if not same_selection(retained, selected_after):
                        raise BuildError(
                            f"{context} Cargo target directory changed during pruning"
                        )
                finally:
                    os.close(child)
                if child_relative not in allowed_directories:
                    os.rmdir(name, dir_fd=descriptor)
            elif child_relative in regular_files:
                expected = regular_files[child_relative]
                stable_fields = (
                    "st_dev",
                    "st_ino",
                    "st_mode",
                    "st_size",
                    "st_mtime_ns",
                )
                if any(
                    getattr(selected, field) != getattr(expected, field)
                    for field in stable_fields
                ):
                    raise BuildError(
                        f"{context} Cargo target file changed before pruning"
                    )
                if child_relative not in allowed_files:
                    os.unlink(name, dir_fd=descriptor)
            else:
                raise BuildError(f"{context} Cargo target topology changed")

    try:
        if not same_manifest_metadata(os.fstat(root), directories[Path()]):
            raise BuildError(f"{context} Cargo target changed before pruning")
        prune(root, Path())
    finally:
        os.close(root)
    final_directories, final_files, _final_children = snapshot_cargo_target(
        root_descriptor, target, context
    )
    if (
        set(final_directories) != allowed_directories
        or set(final_files) != allowed_files
    ):
        raise BuildError(f"{context} pruned Cargo target topology differs")
    if any(
        not same_manifest_metadata(final_files[path], regular_files[path])
        for path in allowed_files
    ):
        raise BuildError(f"{context} Cargo artifact changed during pruning")
    if (
        cargo_target_artifact_paths(root_descriptor, target, examples, context)
        != allowed_files
    ):
        raise BuildError(f"{context} Cargo artifact aliases changed during pruning")


def verify_frozen_build_artifacts(build: Mapping[str, Any], context: str) -> None:
    target = Path(str(build["target"])).resolve(strict=True)
    argv = build["argv"]
    target_descriptors = [
        argv[index + 1]
        for index in range(len(argv) - 2)
        if argv[index] == "--bind-fd" and argv[index + 2] == GUEST_TARGET
    ]
    if len(target_descriptors) != 1 or not str(target_descriptors[0]).isdigit():
        raise BuildError(f"{context} frozen target descriptor differs")
    descriptor = os.open(
        target,
        getattr(os, "O_PATH", os.O_RDONLY)
        | os.O_DIRECTORY
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        target_before = descriptor_directory_identity(descriptor, target, context)
        recorded_target = build["binds"]["target"]["post"]
        stable_target_fields = (
            "device",
            "file_type",
            "inode",
            "link_count",
            "modified_ns",
            "path",
            "size",
        )
        if (
            any(
                target_before[field] != recorded_target[field]
                for field in stable_target_fields
            )
            or target_before["permissions"] != 0o555
        ):
            raise BuildError(f"{context} frozen target identity differs")
        artifacts = build["artifacts"]
        allowed_files = cargo_target_artifact_paths(
            descriptor, target, tuple(artifacts), context
        )
        frozen_directories, frozen_files, _children = snapshot_cargo_target(
            descriptor, target, context
        )
        expected_directories = {Path()}
        for path in allowed_files:
            expected_directories.update(
                parent for parent in path.parents if parent != Path(".")
            )
        if (
            set(frozen_directories) != expected_directories
            or set(frozen_files) != allowed_files
        ):
            raise BuildError(f"{context} frozen Cargo target topology differs")
        for _replay_pass in range(2):
            for example, artifact in artifacts.items():
                relative = Path("release") / "examples" / example
                payload, replayed_source = read_bound_regular_file(
                    descriptor,
                    relative,
                    f"{context} frozen Cargo artifact {example}",
                    require_executable=True,
                    expected_link_count=2,
                )
                recorded_source = artifact["source"]
                expected_recorded_path = (
                    f"/proc/self/fd/{target_descriptors[0]}/{relative.as_posix()}"
                )
                if recorded_source.get("path") != expected_recorded_path:
                    raise BuildError(f"{context} frozen Cargo artifact path differs")
                replayed_source["path"] = expected_recorded_path
                if replayed_source != recorded_source:
                    raise BuildError(f"{context} frozen Cargo artifact identity differs")
                binding = artifact["binding"]
                published = Path(str(binding["path"])).resolve(strict=True)
                published_descriptor, published_payload, published_identity = (
                    open_built_binary(
                        published, f"{context} frozen published artifact {example}"
                    )
                )
                try:
                    if (
                        set(binding)
                        != {"comm", "executable_mode", "path", "sha256"}
                        or binding["comm"] != published.name
                        or binding["executable_mode"] != 0o555
                        or binding["path"] != str(published)
                        or binding["sha256"] != published_identity["sha256"]
                        or published_identity["mode"] != 0o555
                        or published_identity["link_count"] != 1
                        or published_identity["sha256"]
                        != replayed_source["sha256"]
                        or published_payload != payload
                        or (
                            published_identity["device"],
                            published_identity["inode"],
                        )
                        == (replayed_source["device"], replayed_source["inode"])
                    ):
                        raise BuildError(
                            f"{context} frozen published artifact differs"
                        )
                finally:
                    os.close(published_descriptor)
        if descriptor_directory_identity(descriptor, target, context) != target_before:
            raise BuildError(f"{context} frozen target changed during replay")
    finally:
        os.close(descriptor)


def run_build(
    output: Path,
    materialized: dict[str, Any],
    *,
    kind: str,
    toolchain: Mapping[str, str],
    extra_environment: Mapping[str, str],
    lock_authority_module: Any,
    expected_lock: Any,
    reviewed_stage_admission: dict[str, Any],
    reviewed_cargo_config_entries: Sequence[Mapping[str, Any]],
    reviewed_cargo_config_empty: Path,
    wrapper: Path | None,
    examples: Sequence[str],
    artifact_destinations: Mapping[str, Path],
) -> dict[str, Any]:
    if set(artifact_destinations) != set(examples) or len(examples) != len(
        set(examples)
    ):
        raise BuildError(f"{kind} artifact destinations differ from examples")
    if len(set(artifact_destinations.values())) != len(artifact_destinations):
        raise BuildError(f"{kind} artifact destinations are not disjoint")
    target = output / "targets" / kind
    if target.exists() or target.is_symlink():
        raise BuildError(f"{kind} target is not fresh")
    target.mkdir(parents=True)
    receipt_root = None
    if wrapper is not None:
        receipt_root = output / "receipts" / kind
        receipt_root.mkdir(parents=True)
    environment = cargo_environment(toolchain, extra_environment)
    forbidden = {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"}
    if forbidden & set(environment):
        raise BuildError(f"{kind} environment contains global Rust flags")
    if wrapper is None and {
        "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
        "ASTERISM_REBASELINE_WRAPPER_RECEIPT",
    } & set(environment):
        raise BuildError(f"{kind} release environment contains a wrapper")
    filesystem_admission = (
        lock_authority_module.resample_builder_filesystem_admission(
            output.parent, reviewed_stage_admission
        )
    )
    guard_factory = lock_authority_module.capture_prebuild_materialized_tree
    wrapper_descriptor = -1
    wrapper_payload = None
    wrapper_identity = None
    receipt = None
    receipt_identity = None
    config_prebuild = None
    config_postbuild = None
    execution_tools = None
    toolchain_manifest_binding = None
    semantic_input_authority = None
    cargo_path = Path(toolchain["cargo_path"])
    rustc_path = Path(toolchain["rustc_path"])
    toolchain_root = cargo_path.parent.parent
    if rustc_path.parent.parent != toolchain_root:
        raise BuildError(f"{kind} Cargo/rustc toolchain roots differ")
    rustc_host = toolchain.get("rustc_host")
    if (
        not isinstance(rustc_host, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", rustc_host) is None
    ):
        raise BuildError(f"{kind} rustc host differs")
    rust_lld_path = Path(toolchain["rust_lld_path"])
    if rust_lld_path != (
        toolchain_root
        / "lib"
        / "rustlib"
        / rustc_host
        / "bin"
        / "rust-lld"
    ):
        raise BuildError(f"{kind} rust-lld path differs")
    guest_lld_path = (
        f"{GUEST_TOOLCHAIN_ROOT}/lib/rustlib/{rustc_host}/bin/gcc-ld/ld.lld"
    )
    rust_lld_relative = rust_lld_path.relative_to(toolchain_root).as_posix()
    with ExitStack() as stack:
        bwrap_lease = stack.enter_context(
            RetainedFile(
                Path(toolchain["bwrap_path"]),
                f"{kind} bwrap executable",
                expected_sha256=toolchain["bwrap_sha256"],
                require_executable=True,
                trusted_system=True,
            )
        )
        cargo_lease = stack.enter_context(
            RetainedFile(
                cargo_path,
                f"{kind} Cargo executable",
                expected_sha256=toolchain["cargo_sha256"],
                require_executable=True,
            )
        )
        rustc_lease = stack.enter_context(
            RetainedFile(
                rustc_path,
                f"{kind} rustc executable",
                expected_sha256=toolchain["rustc_sha256"],
                require_executable=True,
            )
        )
        rust_lld_lease = stack.enter_context(
            RetainedFile(
                rust_lld_path,
                f"{kind} real rust-lld executable",
                expected_sha256=toolchain["rust_lld_sha256"],
                require_executable=True,
            )
        )
        dev_null_lease = stack.enter_context(
            RetainedDevice(HOST_DEV_NULL, f"{kind} null device")
        )
        python_lease = stack.enter_context(
            RetainedFile(
                SYSTEM_PYTHON,
                f"{kind} wrapper Python interpreter",
                require_executable=True,
                trusted_system=True,
            )
        )
        target_guard = stack.enter_context(
            BoundBuildDirectory(target, f"{kind} target bind")
        )
        receipt_guard = None
        if receipt_root is not None:
            receipt_guard = stack.enter_context(
                BoundBuildDirectory(receipt_root, f"{kind} receipt bind")
            )
        guard = stack.enter_context(
            guard_factory(
                materialized["root"] / LOCK_PATH,
                expected_lock,
                materialized["root"],
                f"{kind} materialized source tree",
            )
        )
        source_tree_guard = stack.enter_context(
            RecursiveTreeAuthorityGuard(
                materialized["root"],
                output / "manifests" / f"semantic-source-{kind}.json",
                "source",
                f"{kind} semantic source tree",
                allow_internal_symlinks=False,
            )
        )
        toolchain_guard = stack.enter_context(
            RecursiveTreeAuthorityGuard(
                toolchain_root,
                output / "manifests" / f"semantic-toolchain-{kind}.json",
                "toolchain",
                f"{kind} semantic toolchain tree",
                allow_internal_symlinks=True,
            )
        )
        if toolchain_guard.initial_manifest is None or rust_lld_lease.identity is None:
            raise BuildError(f"{kind} rust-lld semantic authority is absent")
        rust_lld_entries = [
            entry
            for entry in toolchain_guard.initial_manifest["entries"]
            if entry.get("path") == rust_lld_relative
        ]
        rust_lld_metadata = os.fstat(rust_lld_lease.descriptor)
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
            "sha256": rust_lld_lease.identity["sha256"],
            "size": rust_lld_metadata.st_size,
            "symlink_target": None,
            "symlink_scope": None,
            "uid": rust_lld_metadata.st_uid,
        }
        if rust_lld_entries != [expected_rust_lld_entry]:
            raise BuildError(f"{kind} rust-lld differs from toolchain authority")
        config_guard = stack.enter_context(
            CargoConfigSearchGuard(
                materialized["root"],
                Path(toolchain["cargo_home_path"]),
                reviewed_cargo_config_entries,
                reviewed_cargo_config_empty,
                output / "manifests" / f"cargo-home-{kind}.json",
                f"{kind} Cargo config search",
            )
        )
        system_guard = stack.enter_context(
            TrustedSystemClosureGuard(output / "manifests", kind)
        )
        lock_before = immutable_snapshot_record(
            guard.pre_build,
            f"{kind} prebuild Cargo.lock",
            require_value=False,
        )
        source_ro_bind = guard.bwrap_ro_bind("/asterism/source")
        if (
            source_ro_bind
            != ("--ro-bind", guard.ro_bind_source, "/asterism/source")
            or not isinstance(guard.pass_fds, tuple)
            or len(guard.pass_fds) != 1
            or not isinstance(guard.pass_fds[0], int)
            or isinstance(guard.pass_fds[0], bool)
            or guard.pass_fds[0] < 0
            or guard.ro_bind_source != f"/proc/self/fd/{guard.pass_fds[0]}"
        ):
            raise BuildError(f"{kind} materialized tree guard interface differs")
        source_ro_bind = (
            "--ro-bind-fd",
            str(guard.pass_fds[0]),
            "/asterism/source",
        )
        target_bind = target_guard.bwrap_bind(
            "/asterism/target", read_only=False
        )
        toolchain_ro_bind = toolchain_guard.bwrap_bind(
            GUEST_TOOLCHAIN_ROOT
        )
        cargo_bind = ("--ro-bind-fd", str(cargo_lease.descriptor), GUEST_CARGO)
        rustc_bind = ("--ro-bind-fd", str(rustc_lease.descriptor), GUEST_RUSTC)
        rust_lld_bind = (
            "--ro-bind-fd",
            str(rust_lld_lease.descriptor),
            guest_lld_path,
        )
        python_bind = (
            "--ro-bind-fd",
            str(python_lease.descriptor),
            "/asterism/python3",
        )
        wrapper_bind = None
        receipt_bind = None
        inherited_descriptors = (
            guard.pass_fds
            + target_guard.pass_fds
            + toolchain_guard.pass_fds
            + config_guard.pass_fds
            + system_guard.pass_fds
            + cargo_lease.pass_fds
            + rustc_lease.pass_fds
            + rust_lld_lease.pass_fds
            + dev_null_lease.pass_fds
            + python_lease.pass_fds
        )
        if wrapper is not None:
            assert receipt_guard is not None
            wrapper_descriptor, wrapper_payload, wrapper_identity = open_built_binary(
                wrapper, f"{kind} copied workspace wrapper"
            )
            stack.callback(os.close, wrapper_descriptor)
            wrapper_bind = (
                "--ro-bind-fd",
                str(wrapper_descriptor),
                "/asterism/rustc_workspace_wrapper.py",
            )
            receipt_bind = receipt_guard.bwrap_bind(
                "/asterism/receipt", read_only=False
            )
            inherited_descriptors += (wrapper_descriptor, *receipt_guard.pass_fds)
        if len(set(inherited_descriptors)) != len(inherited_descriptors):
            raise BuildError(f"{kind} inherited bind descriptors alias")
        if os.fstat(source_tree_guard.pass_fds[0]).st_ino != os.fstat(
            guard.pass_fds[0]
        ).st_ino or os.fstat(source_tree_guard.pass_fds[0]).st_dev != os.fstat(
            guard.pass_fds[0]
        ).st_dev:
            raise BuildError(f"{kind} source authority descriptors differ")
        source_tree_guard.replay("pre-Cargo launch")
        toolchain_guard.replay("pre-Cargo launch")
        system_guard.replay("pre-Cargo launch")
        config_prebuild = config_guard.replay(boundary="pre-Cargo launch")
        rust_lld_lease.verify()
        dev_null_lease.verify()
        if rust_lld_entries != [expected_rust_lld_entry]:
            raise BuildError(f"{kind} rust-lld authority changed before launch")
        cargo_config_args = config_guard.prepare_bwrap_args()
        argv = sandboxed_build_argv(
            toolchain=toolchain,
            trusted_system_args=system_guard.bwrap_args(),
            source_ro_bind=source_ro_bind,
            toolchain_ro_bind=toolchain_ro_bind,
            cargo_bind=cargo_bind,
            rustc_bind=rustc_bind,
            rust_lld_bind=rust_lld_bind,
            dev_null_source=dev_null_lease.proc_path,
            python_bind=python_bind,
            cargo_config_args=cargo_config_args,
            target_bind=target_bind,
            wrapper_bind=wrapper_bind,
            receipt_bind=receipt_bind,
            examples=examples,
        )

        def replay_cargo_boundary() -> None:
            nonlocal config_postbuild
            config_postbuild = config_guard.replay(
                boundary="post-Cargo boundary", deep=True
            )
            if config_postbuild != config_prebuild:
                raise BuildError(
                    f"{kind} Cargo config changed across Cargo execution"
                )
            source_tree_guard.replay("post-Cargo boundary")
            toolchain_guard.replay("post-Cargo boundary")
            system_guard.replay("post-Cargo boundary")
            for lease in (
                bwrap_lease,
                cargo_lease,
                rustc_lease,
                rust_lld_lease,
                dev_null_lease,
                python_lease,
            ):
                lease.verify()

        record = run_logged(
            f"cargo-build-{kind}",
            argv,
            cwd=output,
            env=environment,
            timeout=3_600,
            log_root=output / "logs",
            pass_fds=inherited_descriptors,
            execution_lease=bwrap_lease,
            boundary_replay=replay_cargo_boundary,
        )
        execution_tools = {
            "bwrap": bwrap_lease.record(),
            "cargo": cargo_lease.record(),
            "dev_null": dev_null_lease.record(),
            "python": python_lease.record(),
            "rustc": rustc_lease.record(),
            "rust_lld": rust_lld_lease.record(),
            "toolchain_root": toolchain_guard.root_guard.pre_bind,
        }
        after = file_manifest(materialized["root"])
        if after != materialized["manifest"]:
            raise BuildError(f"{kind} source changed during build")
        artifacts = {
            example: copy_bound_artifact(
                target_guard.descriptor,
                Path("release") / "examples" / example,
                artifact_destinations[example],
            )
            for example in examples
        }
        prune_cargo_target(
            target_guard.descriptor,
            target,
            examples,
            f"{kind} post-build target pruning",
        )
        if wrapper is not None:
            assert receipt_guard is not None
            receipt_payload, receipt_identity = read_bound_regular_file(
                receipt_guard.descriptor,
                Path("injection.json"),
                "wrapper injection receipt",
                require_executable=False,
            )
            receipt = validate_wrapper_receipt(
                receipt_payload,
                receipt_identity,
                build_nonce=extra_environment[
                    "ASTERISM_REBASELINE_CHILD_BUILD_NONCE"
                ],
                pinned_rustc=GUEST_RUSTC,
            )
            assert wrapper_payload is not None
            assert wrapper_identity is not None
            verify_open_built_binary(
                wrapper_descriptor,
                wrapper,
                wrapper_payload,
                wrapper_identity,
                f"{kind} copied workspace wrapper",
            )
        assert source_tree_guard.binding is not None
        assert toolchain_guard.binding is not None
        assert system_guard.binding is not None
        cargo_home_tree = config_postbuild["cargo_home_tree"]
        cargo_home_authority = {
            "entry_count": cargo_home_tree["entry_count"],
            "equal_pre_post": cargo_home_tree["equal_pre_post"],
            "manifest_path": cargo_home_tree["path"],
            "manifest_sha256": cargo_home_tree["pre_sha256"],
            "mutation_events_absent": True,
            "role": "cargo_home",
            "schema": RECURSIVE_TREE_AUTHORITY_SCHEMA,
            "watch_count": cargo_home_tree["watch_count"],
        }
        runtime_components = {
            "cargo_home": cargo_home_authority,
            "toolchain": toolchain_guard.binding,
            "trusted_system_closure": system_guard.binding,
        }
        semantic_input_authority = {
            **runtime_components,
            "runtime_sha256": semantic_runtime_sha256(runtime_components),
            "schema": SEMANTIC_INPUT_AUTHORITY_SCHEMA,
            "source": source_tree_guard.binding,
        }
        toolchain_manifest_binding = {
            "entry_count": toolchain_guard.binding["entry_count"],
            "equal_pre_post": toolchain_guard.binding["equal_pre_post"],
            "path": toolchain_guard.binding["manifest_path"],
            "post_sha256": toolchain_guard.binding["manifest_sha256"],
            "pre_sha256": toolchain_guard.binding["manifest_sha256"],
        }
    lock_after = immutable_snapshot_record(
        guard.post_build,
        f"{kind} postbuild Cargo.lock",
        require_value=False,
    )
    if lock_after != lock_before:
        raise BuildError(f"{kind} Cargo.lock full identity changed during build")
    result = {
        "argv": argv,
        "environment": environment,
        "execution": record,
        "filesystem_admission": filesystem_admission,
        "cargo_config_prebuild": config_prebuild,
        "cargo_config_postbuild": config_postbuild,
        "execution_tools": execution_tools,
        "artifacts": artifacts,
        "binds": {
            "target": {
                "parent": target_guard.parent_before,
                "post": target_guard.post_bind,
                "pre": target_guard.pre_bind,
            }
        },
        "lock_prebuild": lock_before,
        "lock_postbuild": lock_after,
        "source_manifest_sha256": materialized["manifest_sha256"],
        "semantic_input_authority": semantic_input_authority,
        "toolchain_manifest": toolchain_manifest_binding,
        "target": str(target.resolve()),
        "target_was_absent": True,
    }
    if wrapper is not None:
        assert receipt_root is not None
        assert receipt is not None
        assert receipt_identity is not None
        assert receipt_guard is not None
        assert wrapper_identity is not None
        result["binds"]["receipt"] = {
            "parent": receipt_guard.parent_before,
            "post": receipt_guard.post_bind,
            "pre": receipt_guard.pre_bind,
        }
        result["wrapper_receipt"] = receipt
        result["wrapper_receipt_identity"] = receipt_identity
        result["wrapper_receipt_sha256"] = receipt_identity["sha256"]
        result["wrapper_input_identity"] = wrapper_identity
    return result


def snapshot_open_file(
    descriptor: int,
    path: Path,
    context: str,
    *,
    require_executable: bool,
    expected_link_count: int = 1,
) -> tuple[bytes, dict[str, Any]]:
    before = os.fstat(descriptor)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != expected_link_count
        or (require_executable and stat.S_IMODE(before.st_mode) & 0o111 == 0)
    ):
        raise BuildError(f"{context} descriptor is not one regular file")
    os.lseek(descriptor, 0, os.SEEK_SET)
    chunks = []
    while chunk := os.read(descriptor, 1024 * 1024):
        chunks.append(chunk)
    payload = b"".join(chunks)
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
        raise BuildError(f"{context} descriptor changed while reading")
    if len(payload) != before.st_size:
        raise BuildError(f"{context} descriptor size differs")
    return payload, {
        "bytes": before.st_size,
        "ctime_ns": before.st_ctime_ns,
        "device": before.st_dev,
        "inode": before.st_ino,
        "link_count": before.st_nlink,
        "mode": stat.S_IMODE(before.st_mode),
        "mtime_ns": before.st_mtime_ns,
        "path": str(path),
        "sha256": sha256_bytes(payload),
        "size": before.st_size,
    }


def snapshot_open_binary(
    descriptor: int, path: Path, context: str
) -> tuple[bytes, dict[str, Any]]:
    return snapshot_open_file(
        descriptor, path, context, require_executable=True
    )


def open_built_binary(path: Path, context: str) -> tuple[int, bytes, dict[str, Any]]:
    exact = require_exact_file(path, context=context)
    descriptor = os.open(
        exact,
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        payload, identity = snapshot_open_binary(descriptor, exact, context)
        if file_identity(exact, context) != identity:
            raise BuildError(f"{context} descriptor/path identity differs")
    except Exception:
        os.close(descriptor)
        raise
    return descriptor, payload, identity


def verify_open_built_binary(
    descriptor: int,
    path: Path,
    expected_payload: bytes,
    expected_identity: dict[str, Any],
    context: str,
) -> None:
    payload, identity = snapshot_open_binary(descriptor, path, context)
    if (
        payload != expected_payload
        or identity != expected_identity
        or file_identity(path, context) != expected_identity
    ):
        raise BuildError(f"{context} changed during release proof")


def nm_inventory(
    nm_lease: RetainedFile,
    binary_fd: int,
    output: Path,
    *,
    repository: Path,
) -> tuple[bytes, dict[str, Any]]:
    environment = {
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
        "TZ": "UTC",
    }
    binary_source = f"/proc/self/fd/{binary_fd}"
    record, stdout, stderr = run_capture(
        [
            str(nm_lease.path),
            "--defined-only",
            "--demangle=rust",
            "--format=posix",
            binary_source,
        ],
        cwd=repository,
        env=environment,
        timeout=120,
        pass_fds=(binary_fd,),
        execution_lease=nm_lease,
    )
    if record["exit_status"] != 0 or stderr:
        raise BuildError(f"nm inventory failed: {stderr!r}")
    write_new(output, stdout, 0o444)
    return stdout, record


def release_compile_out_proof(
    output: Path,
    pristine: Path,
    hooked: Path,
    *,
    repository: Path,
) -> dict[str, Any]:
    pristine_fd = -1
    hooked_fd = -1
    try:
        pristine_fd, pristine_bytes, pristine_identity = open_built_binary(
            pristine, "pristine release binary"
        )
        hooked_fd, hooked_bytes, hooked_identity = open_built_binary(
            hooked, "hooked release binary"
        )
        if pristine_bytes != hooked_bytes:
            raise BuildError("hooked release binary is not byte-identical to pristine A")
        digest = sha256_bytes(pristine_bytes)
        if pristine_identity["sha256"] != digest or hooked_identity["sha256"] != digest:
            raise BuildError("release SHA-256 equality differs")
        for token in FORBIDDEN_RELEASE_TOKENS:
            if token in pristine_bytes or token in hooked_bytes:
                raise BuildError(f"release binary contains hook string {token!r}")
        nm = Path("/usr/bin/nm").resolve(strict=True)
        with RetainedFile(
            nm,
            "pinned nm symbol-proof executable",
            require_executable=True,
            trusted_system=True,
        ) as nm_lease:
            nm_before = nm_lease.record()
            pristine_inventory, pristine_record = nm_inventory(
                nm_lease,
                pristine_fd,
                output / "manifests" / "symbols-pristine-release.txt",
                repository=repository,
            )
            hooked_inventory, hooked_record = nm_inventory(
                nm_lease,
                hooked_fd,
                output / "manifests" / "symbols-hooked-release.txt",
                repository=repository,
            )
            if pristine_inventory != hooked_inventory:
                raise BuildError("hooked/pristine release symbol inventories differ")
            for token in FORBIDDEN_RELEASE_TOKENS:
                if token in pristine_inventory:
                    raise BuildError(
                        f"release symbol inventory contains hook token {token!r}"
                    )
            nm_lease.verify()
            nm_after = nm_lease.record()
        if nm_after != nm_before:
            raise BuildError("nm identity changed during symbol proof")
        verify_open_built_binary(
            pristine_fd,
            pristine,
            pristine_bytes,
            pristine_identity,
            "pristine release binary",
        )
        verify_open_built_binary(
            hooked_fd,
            hooked,
            hooked_bytes,
            hooked_identity,
            "hooked release binary",
        )
    finally:
        if pristine_fd >= 0:
            os.close(pristine_fd)
        if hooked_fd >= 0:
            os.close(hooked_fd)
    symbol_absence = {
        "forbidden_hook_strings": [
            token.decode() for token in FORBIDDEN_RELEASE_TOKENS
        ],
        "hooked_inventory_sha256": sha256_bytes(hooked_inventory),
        "hooked_strings_absent": True,
        "pristine_inventory_sha256": sha256_bytes(pristine_inventory),
        "pristine_strings_absent": True,
        "schema": "bn-30fs-release-symbol-absence-v1",
    }
    return {
        "binary_byte_identical": True,
        "binary_sha256": digest,
        "forbidden_hook_strings": [token.decode() for token in FORBIDDEN_RELEASE_TOKENS],
        "hooked_nm": hooked_record,
        "hooked_binary": hooked_identity,
        "overlay_release_sha256": hooked_identity["sha256"],
        "nm": nm_before,
        "preapproval_source_sentinel": PREAPPROVAL_SOURCE_SENTINEL,
        "pristine_binary": pristine_identity,
        "pristine_sha256": pristine_identity["sha256"],
        "pristine_nm": pristine_record,
        "symbol_absence": symbol_absence,
        "symbol_absence_sha256": sha256_bytes(canonical_bytes(symbol_absence)),
        "symbol_inventory_byte_identical": True,
        "symbol_inventory_sha256": sha256_bytes(pristine_inventory),
    }


def input_paths(
    lock_manifest: Path, authority: Path, review_bundle: Path, tools: Path
) -> tuple[Path, ...]:
    return (
        CORRECTNESS_SOURCE,
        FAULT_SOURCE,
        FAULT_VALIDATOR,
        LOCK_AUTHORITY_VALIDATOR,
        PREPARE_OVERLAYS,
        OVERLAY_PINS,
        PRODUCT_OVERLAY,
        PRODUCT_OVERLAY_VALIDATOR,
        WORKSPACE_WRAPPER,
        STATIC_VALIDATOR,
        PUBLIC / "main.rs",
        PUBLIC / "adapters" / "current.rs",
        *(SHARED / name for name in SHARED_NAMES),
        lock_manifest,
        authority,
        review_bundle,
        tools,
    )


def identities(paths: Iterable[Path], label: str) -> list[dict[str, Any]]:
    records = [file_identity(path, f"{label} {path.name}") for path in paths]
    if len({record["path"] for record in records}) != len(records):
        raise BuildError(f"{label} input paths are not unique")
    if len({(record["device"], record["inode"]) for record in records}) != len(records):
        raise BuildError(f"{label} input file identities are not unique")
    return records


def toolchain_identities(toolchain: Mapping[str, str], label: str) -> list[dict[str, Any]]:
    return [
        file_identity(Path(toolchain[f"{name}_path"]), f"{label} {name}")
        for name in ("bwrap", "cargo", "git", "rustc", "rust_lld", "rustup")
    ]


def build(args: argparse.Namespace) -> None:
    repository = args.repository.resolve(strict=True)
    output = args.output.resolve()
    if output.exists() or output.is_symlink():
        raise BuildError(f"output must be absent: {output}")
    lock_manifest_path = require_exact_file(
        args.lock_manifest.resolve(strict=True),
        mode=0o444,
        context="v3 lock manifest",
    )
    authority_path = require_exact_file(
        args.lock_authority.resolve(strict=True),
        mode=0o444,
        context="current lock authority",
    )
    review_bundle_path = require_exact_file(
        args.review_bundle.resolve(strict=True),
        mode=0o444,
        context="current lock review bundle",
    )
    tools_path = require_exact_file(
        args.tools.resolve(strict=True), mode=0o444, context="base tools manifest"
    )

    static_authority = validate_static_authority(repository)
    product_overlay_authority = validate_product_overlay_authority(repository)
    fault_authority = validate_fault_authority(repository)
    authority, locks, lock_validation, validated, lock_authority_module = (
        validate_lock_authority(
            repository, lock_manifest_path, review_bundle_path, authority_path
        )
    )
    validated_authority_before = validated_authority_records(validated)
    validated_locks_before = validated_lock_records(validated)
    toolchain = validate_lock_manifest(locks)
    (
        cargo_config_entries,
        cargo_config_authority,
        cargo_config_manifest_path,
        cargo_config_empty_path,
    ) = reviewed_cargo_config_policy(locks)
    base_tools = validate_tools_manifest(tools_path, allow_child_placeholders=True)
    lock_payload = validated.locks["A"].payload
    if not isinstance(lock_payload, bytes) or sha256_bytes(lock_payload) != PRODUCT_LOCK_SHA256:
        raise BuildError("validated A lock payload is not the frozen exact bytes")
    tracked_inputs = (
        *input_paths(
            lock_manifest_path, authority_path, review_bundle_path, tools_path
        ),
        *(Path(record["path"]) for record in validated_locks_before.values()),
        cargo_config_manifest_path,
    )
    inputs_before = identities(tracked_inputs, "before")
    toolchain_before = toolchain_identities(toolchain, "before")

    output.mkdir(parents=True)
    (output / "logs").mkdir()
    (output / "manifests").mkdir()
    archive, archive_identity = product_archive(
        Path(toolchain["git_path"]), toolchain["git_sha256"], repository
    )
    write_new(output / "archives" / "source-A.tar", archive, 0o444)
    wrapper_copy = output / "inputs" / "rustc_workspace_wrapper.py"
    copy_new(WORKSPACE_WRAPPER, wrapper_copy, mode=0o555)

    materialized = {
        kind: materialize(
            output,
            archive,
            lock_payload,
            kind=kind,
            git=Path(toolchain["git_path"]),
            git_sha256=toolchain["git_sha256"],
            logs=output / "logs",
        )
        for kind in ("children", "pristine-release", "hooked-release")
    }
    construction = {
        "archive": archive_identity,
        "cargo_config_manifest_sha256": cargo_config_authority["identity"]["sha256"],
        "cargo_config_view_sha256": sha256_bytes(canonical_bytes(cargo_config_entries)),
        "kinds": {
            kind: {
                "manifest_sha256": value["manifest_sha256"],
                "placements": value["placements"],
            }
            for kind, value in materialized.items()
        },
        "lock_sha256": PRODUCT_LOCK_SHA256,
        "product_commit": PRODUCT_COMMIT,
        "product_overlay_sha256": sha256_file(PRODUCT_OVERLAY),
        "product_tree": PRODUCT_TREE,
        "protocol": PROTOCOL,
        "schema": CONSTRUCTION_SCHEMA,
    }
    construction_path = output / "manifests" / "current-children-construction.json"
    write_new(construction_path, canonical_bytes(construction), 0o444)
    build_nonce = sha256_bytes(canonical_bytes(construction))

    release_extra = release_contract_environment(
        repository,
        Path(toolchain["git_path"]),
        toolchain["git_sha256"],
        sha256_file(PUBLIC / "adapters" / "current.rs"),
        shared_manifest_sha256(),
        build_nonce,
    )
    if (
        release_extra["ASTERISM_BUILD_TOOLING_COMMIT"]
        != authority["tooling_commit"]
        or release_extra["ASTERISM_BUILD_TOOLING_TREE"]
        != authority["tooling_tree"]
    ):
        raise BuildError("release contract tooling identity differs from authority")
    reviewed_stage_admission = getattr(validated, "reviewed_stage_admission", None)
    if not isinstance(reviewed_stage_admission, dict):
        raise BuildError("validated reviewed_stage_admission differs")
    pristine_build = run_build(
        output,
        materialized["pristine-release"],
        kind="pristine-release",
        toolchain=toolchain,
        extra_environment=release_extra,
        lock_authority_module=lock_authority_module,
        expected_lock=validated.locks["A"],
        reviewed_stage_admission=reviewed_stage_admission,
        reviewed_cargo_config_entries=cargo_config_entries,
        reviewed_cargo_config_empty=cargo_config_empty_path,
        wrapper=None,
        examples=("asterism_rebaseline_public",),
        artifact_destinations={
            "asterism_rebaseline_public": (
                output / "artifacts" / "release" / "pristine-A"
            )
        },
    )
    hooked_build = run_build(
        output,
        materialized["hooked-release"],
        kind="hooked-release",
        toolchain=toolchain,
        extra_environment=release_extra,
        lock_authority_module=lock_authority_module,
        expected_lock=validated.locks["A"],
        reviewed_stage_admission=reviewed_stage_admission,
        reviewed_cargo_config_entries=cargo_config_entries,
        reviewed_cargo_config_empty=cargo_config_empty_path,
        wrapper=None,
        examples=("asterism_rebaseline_public",),
        artifact_destinations={
            "asterism_rebaseline_public": (
                output / "artifacts" / "release" / "hooked-A"
            )
        },
    )

    pristine_binary = Path(
        pristine_build["artifacts"]["asterism_rebaseline_public"]["binding"][
            "path"
        ]
    )
    hooked_binary = Path(
        hooked_build["artifacts"]["asterism_rebaseline_public"]["binding"][
            "path"
        ]
    )
    compile_out = release_compile_out_proof(
        output, pristine_binary, hooked_binary, repository=repository
    )
    if (
        compile_out["pristine_sha256"]
        != pristine_build["artifacts"]["asterism_rebaseline_public"]["binding"][
            "sha256"
        ]
        or compile_out["overlay_release_sha256"]
        != hooked_build["artifacts"]["asterism_rebaseline_public"]["binding"][
            "sha256"
        ]
    ):
        raise BuildError("release proof differs from descriptor-copied artifacts")
    child_extra = {
        "ASTERISM_FAULT_COMPILE_OUT_IDENTICAL": "true",
        "ASTERISM_FAULT_COMPILE_OUT_OVERLAY_RELEASE_SHA256": compile_out[
            "overlay_release_sha256"
        ],
        "ASTERISM_FAULT_COMPILE_OUT_PRISTINE_SHA256": compile_out[
            "pristine_sha256"
        ],
        "ASTERISM_FAULT_COMPILE_OUT_SCHEMA": FAULT_COMPILE_OUT_SCHEMA,
        "ASTERISM_FAULT_COMPILE_OUT_SYMBOL_ABSENCE_SHA256": compile_out[
            "symbol_absence_sha256"
        ],
        "ASTERISM_REBASELINE_CHILD_BUILD_NONCE": build_nonce,
        "ASTERISM_REBASELINE_EXPECTED_LIB_SOURCE": EXPECTED_LIB_SOURCE,
        "ASTERISM_REBASELINE_PINNED_RUSTC": GUEST_RUSTC,
        "ASTERISM_REBASELINE_WRAPPER_RECEIPT": "/asterism/receipt/injection.json",
        "RUSTC_WORKSPACE_WRAPPER": "/asterism/rustc_workspace_wrapper.py",
    }
    if len([name for name in child_extra if name.startswith("ASTERISM_FAULT_")]) != 5:
        raise BuildError("fault compile-out environment cardinality differs")
    child_build = run_build(
        output,
        materialized["children"],
        kind="children",
        toolchain=toolchain,
        extra_environment=child_extra,
        lock_authority_module=lock_authority_module,
        expected_lock=validated.locks["A"],
        reviewed_stage_admission=reviewed_stage_admission,
        reviewed_cargo_config_entries=cargo_config_entries,
        reviewed_cargo_config_empty=cargo_config_empty_path,
        wrapper=wrapper_copy,
        examples=(
            "asterism_rebaseline_current_correctness",
            "asterism_rebaseline_current_fault",
        ),
        artifact_destinations={
            "asterism_rebaseline_current_correctness": (
                output
                / "artifacts"
                / "tools"
                / REQUIRED_TOOL_COMMS["correctness"]
            ),
            "asterism_rebaseline_current_fault": (
                output / "artifacts" / "tools" / REQUIRED_TOOL_COMMS["fault"]
            ),
        },
    )
    if child_build["wrapper_receipt"].get("build_nonce") != build_nonce:
        raise BuildError("wrapper receipt build nonce differs")
    semantic_runtime_digests = {
        build["semantic_input_authority"]["runtime_sha256"]
        for build in (pristine_build, hooked_build, child_build)
    }
    if len(semantic_runtime_digests) != 1:
        raise BuildError("current builds used different semantic runtime authority")

    child_bindings = {
        "correctness": child_build["artifacts"][
            "asterism_rebaseline_current_correctness"
        ]["binding"],
        "fault": child_build["artifacts"]["asterism_rebaseline_current_fault"][
            "binding"
        ],
    }

    final_tools = json.loads(json.dumps(base_tools))
    final_tools["tools"].update(child_bindings)
    final_tools_path = output / "asterism-rebaseline-tools.json"
    write_new(
        final_tools_path, authority_canonical_bytes(final_tools), 0o444
    )
    if (
        validate_tools_manifest(
            final_tools_path, allow_child_placeholders=False
        )
        != final_tools
    ):
        raise BuildError("emitted tools manifest failed exact downstream validation")

    inputs_after = identities(tracked_inputs, "after")
    toolchain_after = toolchain_identities(toolchain, "after")
    if inputs_after != inputs_before:
        raise BuildError("source/authority inputs changed during child builds")
    if toolchain_after != toolchain_before:
        raise BuildError("toolchain changed during child builds")
    validated_locks_after = validated_lock_records(validated)
    if validated_locks_after != validated_locks_before:
        raise BuildError("validated A/C/D lock candidates changed during child builds")
    validated_authority_after = validated_authority_records(validated)
    if validated_authority_after != validated_authority_before:
        raise BuildError(
            "validated authority/manifest/review bundle changed during child builds"
        )

    attestation = {
        "artifacts": child_bindings,
        "build_nonce": build_nonce,
        "builds": {
            "children": child_build,
            "hooked_release": hooked_build,
            "pristine_release": pristine_build,
        },
        "construction_path": str(construction_path.resolve()),
        "construction_sha256": sha256_file(construction_path),
        "fault_authority": fault_authority,
        "inputs": inputs_before,
        "lock_authority": authority,
        "lock_authority_inputs": validated_authority_before,
        "lock_authority_validation": lock_validation,
        "lock_candidates": validated_locks_before,
        "cargo_config_authority": cargo_config_authority,
        "lock_manifest_sha256": sha256_file(lock_manifest_path),
        "product_commit": PRODUCT_COMMIT,
        "product_overlay_authority": product_overlay_authority,
        "product_tree": PRODUCT_TREE,
        "protocol": PROTOCOL,
        "protocol_sha256": PROTOCOL_SHA256,
        "release_compile_out": compile_out,
        "release_compile_out_approval": {
            "final_integration_action": (
                "repeat-release-equality-proof-under-real-source-approval"
            ),
            "source_approval_sha256": PREAPPROVAL_SOURCE_SENTINEL,
            "source_approval_status": "preapproval-sentinel-not-source-approved",
        },
        "prebuild_filesystem_admissions": {
            "children": child_build["filesystem_admission"],
            "hooked_release": hooked_build["filesystem_admission"],
            "pristine_release": pristine_build["filesystem_admission"],
        },
        "review_bundle_sha256": sha256_file(review_bundle_path),
        "schema": ATTESTATION_SCHEMA,
        "static_authority": static_authority,
        "status": "ok",
        "toolchain": toolchain,
        "toolchain_identities": toolchain_before,
        "tools_manifest_path": str(final_tools_path.resolve()),
        "tools_manifest_sha256": sha256_file(final_tools_path),
    }
    attestation_path = output / "current-children-attestation.json"
    write_new(
        attestation_path, authority_canonical_bytes(attestation), 0o444
    )
    make_read_only(output)
    verify_frozen_build_artifacts(
        pristine_build, "pristine release post-freeze replay"
    )
    verify_frozen_build_artifacts(
        hooked_build, "hooked release post-freeze replay"
    )
    verify_frozen_build_artifacts(child_build, "current children post-freeze replay")
    print(authority_canonical_bytes(attestation).decode("utf-8"), end="")


def self_test_cargo_example_hardlinks() -> dict[str, Any]:
    """Accept only Cargo's exact in-directory example hard-link layout."""

    example = "asterism_rebaseline_public"
    hashed = f"{example}-0123456789abcdef"
    payload = b"synthetic executable bytes\n"

    def fixture(
        root: Path,
        label: str,
        *,
        local_aliases: Sequence[str] = (),
        external_aliases: Sequence[str] = (),
        scratch_external_alias: bool = False,
        scratch_symlink: bool = False,
    ) -> dict[str, Any]:
        fixture_root = root / label
        target = fixture_root / "target"
        examples = target / "release" / "examples"
        examples.mkdir(parents=True)
        source = examples / example
        write_new(source, payload, 0o755)
        for alias in local_aliases:
            os.link(source, examples / alias)
        for alias in external_aliases:
            os.link(source, fixture_root / alias)
        scratch = target / "release" / "build" / "dependency-0123456789abcdef"
        scratch.mkdir(parents=True)
        scratch_binary = scratch / "build-script-build"
        write_new(scratch_binary, b"synthetic Cargo build script\n", 0o755)
        os.link(
            scratch_binary,
            scratch / "build_script_build-0123456789abcdef",
        )
        dependencies = target / "release" / "deps"
        dependencies.mkdir()
        write_new(dependencies / "dependency.rlib", b"synthetic rlib\n", 0o644)
        if scratch_external_alias:
            os.link(scratch_binary, fixture_root / "external-scratch-alias")
        if scratch_symlink:
            (target / "release" / "hostile-scratch-link").symlink_to(
                fixture_root
            )
        destination = fixture_root / "sealed"
        descriptor = os.open(
            target,
            getattr(os, "O_PATH", os.O_RDONLY)
            | os.O_DIRECTORY
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            artifact = copy_bound_artifact(
                descriptor,
                Path("release") / "examples" / example,
                destination,
            )
            prune_cargo_target(
                descriptor,
                target,
                (example,),
                f"self-test {label} Cargo target pruning",
            )
            target_post = directory_identity(
                target, f"self-test {label} post-build target"
            )
            make_read_only(target)
            return {
                "artifact": artifact,
                "build": {
                    "argv": ["--bind-fd", str(descriptor), GUEST_TARGET],
                    "artifacts": {example: artifact},
                    "binds": {"target": {"post": target_post}},
                    "target": str(target.resolve()),
                },
            }
        finally:
            os.close(descriptor)

    with tempfile.TemporaryDirectory(prefix="bn-1o85-cargo-hardlinks-") as temporary:
        root = Path(temporary).resolve(strict=True)
        try:
            paired = fixture(root, "paired", local_aliases=(hashed,))
            paired_artifact = paired["artifact"]
            if (
                paired_artifact["source"]["link_count"] != 2
                or paired_artifact["source"]["mode"] != 0o555
                or paired_artifact["binding"]["sha256"] != sha256_bytes(payload)
            ):
                raise BuildError("Cargo example hard-link acceptance differs")
            paired_target = root / "paired" / "target"
            expected_paths = {
                Path("release"),
                Path("release/examples"),
                Path("release/examples") / example,
                Path("release/examples") / hashed,
            }
            observed_paths = {
                path.relative_to(paired_target)
                for path in paired_target.rglob("*")
            }
            if observed_paths != expected_paths:
                raise BuildError("Cargo target scratch pruning differs")
            for alias in (example, hashed):
                alias_path = paired_target / "release" / "examples" / alias
                if stat.S_IMODE(alias_path.stat().st_mode) != 0o555:
                    raise BuildError("Cargo example hard-link normalization differs")
            paired_destination = root / "paired" / "sealed"
            paired_identity = file_identity(
                paired_destination, "self-test sealed Cargo artifact"
            )
            if (
                paired_identity["link_count"] != 1
                or paired_identity["mode"] != 0o555
            ):
                raise BuildError("sealed Cargo artifact is not one 0555 regular file")
            verify_frozen_build_artifacts(
                paired["build"], "self-test frozen Cargo artifact"
            )
            sealed_sha256 = sha256_file(paired_destination)
            paired_source = (
                paired_target / "release" / "examples" / example
            )
            paired_source.chmod(0o755)
            paired_source.write_bytes(b"hostile post-copy drift\n")
            paired_source.chmod(0o555)
            try:
                verify_frozen_build_artifacts(
                    paired["build"], "self-test drifted frozen Cargo artifact"
                )
            except BuildError:
                post_copy_drift_rejected = True
            else:
                post_copy_drift_rejected = False
            if (
                not post_copy_drift_rejected
                or sha256_file(paired_destination) != sealed_sha256
            ):
                raise BuildError("post-copy Cargo artifact drift was accepted")

            hostiles = (
                ("single", {}),
                ("wrong-name", {"local_aliases": (f"{example}-not-a-hash",)}),
                ("external-only", {"external_aliases": ("outside",)}),
                (
                    "extra-external",
                    {
                        "local_aliases": (hashed,),
                        "external_aliases": ("outside",),
                    },
                ),
                (
                    "scratch-external",
                    {
                        "local_aliases": (hashed,),
                        "scratch_external_alias": True,
                    },
                ),
                (
                    "scratch-symlink",
                    {"local_aliases": (hashed,), "scratch_symlink": True},
                ),
            )
            rejected = 0
            for label, arguments in hostiles:
                try:
                    fixture(root, label, **arguments)
                except BuildError:
                    rejected += 1
            if rejected != len(hostiles):
                raise BuildError(
                    "hostile Cargo example hard-link layout was accepted"
                )
        finally:
            for directory in sorted(
                (
                    path
                    for path in root.rglob("*")
                    if not path.is_symlink() and path.is_dir()
                ),
                key=lambda path: len(path.parts),
            ):
                directory.chmod(0o755)
    return {
        "hostile_layouts_rejected": rejected,
        "paired_layout_accepted": True,
        "post_copy_drift_rejected": True,
        "scratch_pruned": True,
        "sealed_single_link": True,
        "single_layout_rejected": True,
        "status": "ok",
    }


def self_test_cargo_config_guard() -> dict[str, Any]:
    """Exercise descriptor enumeration/replay without launching any process."""

    evidence_schema = load_evidence_schema_self_test_module()
    with tempfile.TemporaryDirectory(prefix="bn-30fs-cargo-config-") as temporary:
        root = Path(temporary).resolve(strict=True)
        source = root / "source"
        source_config = source / ".cargo" / "config.toml"
        cargo_home = root / "cargo-home"
        evidence_root = root / "evidence"
        source_config.parent.mkdir(parents=True)
        cargo_home.mkdir()
        evidence_root.mkdir()
        nested = cargo_home / "registry" / "cache"
        nested.mkdir(parents=True)
        (cargo_home / "a").mkdir()
        (cargo_home / "a-b").mkdir()
        (cargo_home / "crates" / "mess").mkdir(parents=True)
        (cargo_home / "crates" / "mess-bench").mkdir()
        dependency = nested / "dependency.crate"
        dependency_payload = b"reviewed dependency bytes\n"
        write_new(dependency, dependency_payload, 0o644)
        write_new(cargo_home / ".package-cache", b"", 0o444)
        write_new(cargo_home / "a" / "input", b"a\n", 0o644)
        write_new(cargo_home / "a-b" / "input", b"a-b\n", 0o644)
        write_new(cargo_home / "crates" / "mess" / "x", b"x\n", 0o644)
        write_new(
            cargo_home / "crates" / "mess-bench" / "y", b"y\n", 0o644
        )
        config_payload = b"[net]\noffline = true\n"
        write_new(source_config, config_payload, 0o444)
        empty_config = root / "reviewed-cargo-config.empty"
        write_new(empty_config, b"", 0o444)
        expected = [
            {
                "path": CARGO_CONFIG_GUEST_PATHS[0],
                "sha256": sha256_bytes(config_payload),
                "status": "present",
            },
            {
                "path": CARGO_CONFIG_GUEST_PATHS[1],
                "sha256": EMPTY_SHA256,
                "status": "present",
            },
            *(
                {"path": path, "sha256": None, "status": "absent"}
                for path in CARGO_CONFIG_GUEST_PATHS[2:6]
            ),
            *(
                {"path": path, "sha256": EMPTY_SHA256, "status": "present"}
                for path in CARGO_CONFIG_GUEST_PATHS[6:]
            ),
        ]
        with CargoConfigSearchGuard(
            source,
            cargo_home,
            expected,
            empty_config,
            evidence_root / "cargo-home-normal.json",
            "self-test Cargo config search",
        ) as guard:
            before = guard.replay(boundary="self-test pre")
            arguments = guard.prepare_bwrap_args()
            after = guard.replay(boundary="self-test post", deep=True)
            cargo_home_descriptor = guard.directory_guards[
                "cargo-home"
            ].descriptor
            cargo_home_overlay = [
                "--overlay-src",
                f"/proc/self/fd/{cargo_home_descriptor}",
                "--tmp-overlay",
                GUEST_CARGO_HOME,
            ]
            cargo_home_manifest = json.loads(
                (evidence_root / "cargo-home-normal.json").read_bytes()
            )
            evidence_schema._validate_recursive_semantic_manifest(
                cargo_home_manifest,
                "cargo_home",
                "self-test Cargo-home prefix-sibling ordering",
                trusted_system=False,
            )
            if (
                before != after
                or len(before["cargo_search"]["entries"]) != 8
                or before["cargo_home_tree"]["entry_count"] != 14
                or before["cargo_home_tree"]["watch_count"] != 8
                or arguments.count("--ro-bind-data") != 4
                or arguments.count("--ro-bind-fd") != 0
                or arguments.count("--overlay-src") != 1
                or arguments.count("--tmp-overlay") != 1
                or arguments.count("--remount-ro") != 4
                or sum(
                    arguments[index : index + len(cargo_home_overlay)]
                    == cargo_home_overlay
                    for index in range(len(arguments))
                )
                != 1
            ):
                raise BuildError("Cargo config guard self-test differs")
        nested_mutation_rejected = False
        try:
            with CargoConfigSearchGuard(
                source,
                cargo_home,
                expected,
                empty_config,
                evidence_root / "cargo-home-hostile.json",
                "self-test hostile Cargo-home tree",
            ) as hostile_guard:
                dependency.write_bytes(b"hostile dependency bytes\n")
                dependency.write_bytes(dependency_payload)
                hostile_guard.replay(
                    boundary="nested mutation/restore hostile", deep=True
                )
        except BuildError:
            nested_mutation_rejected = True
        if not nested_mutation_rejected:
            raise BuildError("Cargo-home nested mutation/restore was accepted")
        appeared_rejected = False
        try:
            with CargoConfigSearchGuard(
                source,
                cargo_home,
                expected,
                empty_config,
                evidence_root / "cargo-config-appeared.json",
                "self-test appearing Cargo config",
            ) as appeared_guard:
                write_new(source / ".cargo" / "config", b"", 0o444)
                appeared_guard.replay(boundary="appearing config hostile")
        except BuildError:
            appeared_rejected = True
        finally:
            appeared = source / ".cargo" / "config"
            if appeared.exists():
                appeared.unlink()
        if not appeared_rejected:
            raise BuildError("empty-bound Cargo config appearance was accepted")
        return {
            "empty_bound_appearance_rejected": True,
            "entries": 8,
            "nested_mutation_restore_rejected": True,
            "recursive_path_order_validated": True,
            "ro_bind_data": 4,
            "cargo_home_overlay": 1,
            "ro_bind_fd": 0,
            "schema": CARGO_CONFIG_SEARCH_SCHEMA,
            "status": "ok",
            "tree_entries": 14,
            "tree_watches": 8,
        }


def self_test_external_canonical_utf8() -> dict[str, Any]:
    """Accept upstream authority UTF-8 without changing local output bytes."""

    schema = "bn-ecm1-external-canonical-utf8-self-test-v1"
    value = {"label": "A — current", "schema": schema}
    local_canonical = canonical_bytes(value)
    authority_canonical = authority_canonical_bytes(value)
    if (
        b"\\u2014" not in local_canonical
        or b"\xe2\x80\x94" in local_canonical
        or b"\xe2\x80\x94" not in authority_canonical
        or b"\\u2014" in authority_canonical
        or local_canonical == authority_canonical
        or parse_canonical(
            local_canonical, schema, "local canonical self-test"
        )
        != value
    ):
        raise BuildError("local/external canonical boundary self-test differs")
    with tempfile.TemporaryDirectory(
        prefix="bn-ecm1-external-canonical-"
    ) as temporary:
        root = Path(temporary).resolve(strict=True)
        accepted_path = root / "authority-utf8.json"
        accepted_path.write_bytes(authority_canonical)
        accepted_path.chmod(0o444)
        if load_canonical(
            accepted_path, schema, "upstream authority UTF-8 self-test"
        ) != value:
            raise BuildError("upstream authority UTF-8 self-test differs")
        escaped_path = root / "authority-ascii-escaped.json"
        escaped_path.write_bytes(local_canonical)
        escaped_path.chmod(0o444)
        try:
            load_canonical(
                escaped_path,
                schema,
                "ASCII-escaped upstream authority hostile",
            )
        except BuildError:
            pass
        else:
            raise BuildError(
                "ASCII-escaped upstream authority alternate was accepted"
            )
    return {
        "ascii_escaped_alternate_rejected": True,
        "local_ascii_canonical_preserved": True,
        "schema": schema,
        "status": "ok",
        "upstream_utf8_canonical_accepted": True,
    }


def self_test_reviewed_cargo_config_policy() -> dict[str, Any]:
    """Reject the stale host-path policy and retain the reviewed guest view."""

    with tempfile.TemporaryDirectory(prefix="bn-2ld4-reviewed-config-") as temporary:
        root = Path(temporary).resolve(strict=True)
        entries = [
            *(
                {"path": path, "sha256": EMPTY_SHA256, "status": "present"}
                for path in CARGO_CONFIG_GUEST_PATHS[:2]
            ),
            *(
                {"path": path, "sha256": None, "status": "absent"}
                for path in CARGO_CONFIG_GUEST_PATHS[2:6]
            ),
            *(
                {"path": path, "sha256": EMPTY_SHA256, "status": "present"}
                for path in CARGO_CONFIG_GUEST_PATHS[6:]
            ),
        ]
        canonical = {
            "cargo_home_path": GUEST_CARGO_HOME,
            "cwd": "/asterism/source",
            "entries": entries,
            "schema": "asterism-rebaseline-cargo-config-search-v3",
        }

        def evaluate(label: str, value: Mapping[str, Any]) -> tuple[
            list[dict[str, Any]], dict[str, Any], Path, Path
        ]:
            path = root / f"{label}.json"
            write_new(path, canonical_bytes(value), 0o444)
            write_new(path.with_name(f"{path.name}.empty"), b"", 0o444)
            binding = {"path": str(path.resolve()), "sha256": sha256_file(path)}
            locks = {
                "variants": {"A": {"resolver": {"cargo_config_search": binding}}}
            }
            return reviewed_cargo_config_policy(locks)

        translated, authority, path, empty_path = evaluate("canonical", canonical)
        if (
            translated != entries
            or authority["recorded"] != canonical
            or authority["translated_entries"] != entries
            or authority["binding"]["path"] != str(path)
            or empty_path != path.with_name(f"{path.name}.empty")
        ):
            raise BuildError("reviewed Cargo config canonical guest policy differs")

        hostiles: list[tuple[str, dict[str, Any]]] = []
        wrong_cwd = json.loads(json.dumps(canonical))
        wrong_cwd["cwd"] = str(root / "source")
        hostiles.append(("host-cwd", wrong_cwd))
        wrong_home = json.loads(json.dumps(canonical))
        wrong_home["cargo_home_path"] = str(root / "cargo-home")
        hostiles.append(("host-cargo-home", wrong_home))
        wrong_order = json.loads(json.dumps(canonical))
        wrong_order["entries"][0], wrong_order["entries"][1] = (
            wrong_order["entries"][1],
            wrong_order["entries"][0],
        )
        hostiles.append(("entry-order", wrong_order))
        middle_present = json.loads(json.dumps(canonical))
        middle_present["entries"][2].update(
            {"sha256": EMPTY_SHA256, "status": "present"}
        )
        hostiles.append(("middle-present", middle_present))
        edge_absent = json.loads(json.dumps(canonical))
        edge_absent["entries"][7].update({"sha256": None, "status": "absent"})
        hostiles.append(("edge-absent", edge_absent))
        rejected = 0
        for label, value in hostiles:
            try:
                evaluate(label, value)
            except BuildError:
                rejected += 1
        if rejected != len(hostiles):
            raise BuildError("reviewed Cargo config hostile policy was accepted")
        return {
            "entries": len(entries),
            "hostile_mutations_rejected": rejected,
            "status": "ok",
        }


def self_test_semantic_runtime_authority() -> dict[str, Any]:
    """Prove path relocation is neutral while semantic drift changes the digest."""

    def tree(role: str, digest: str, path: str) -> dict[str, Any]:
        return {
            "entry_count": 7,
            "equal_pre_post": True,
            "manifest_path": path,
            "manifest_sha256": digest,
            "mutation_events_absent": True,
            "role": role,
            "schema": RECURSIVE_TREE_AUTHORITY_SCHEMA,
            "watch_count": 2,
        }

    components = {
        "cargo_home": tree("cargo_home", "a" * 64, "/one/cargo.json"),
        "toolchain": tree("toolchain", "b" * 64, "/one/toolchain.json"),
        "trusted_system_closure": {
            "entry_count": 21,
            "manifest_path": "/one/system.json",
            "mounts": [
                {
                    "device": 1,
                    "gid": 0,
                    "guest_path": guest_path,
                    "host_path": str(host_path),
                    "inode": index + 1,
                    "permissions": 0o755,
                    "resolved_path": str(host_path),
                    "trusted_root_owned_non_writable": True,
                    "uid": 0,
                }
                for index, (host_path, guest_path) in enumerate(
                    TRUSTED_SYSTEM_MOUNTS
                )
            ],
            "mutation_events_absent": True,
            "schema": TRUSTED_SYSTEM_CLOSURE_SCHEMA,
            "sha256": "c" * 64,
            "watch_count": 6,
        },
    }
    baseline = semantic_runtime_sha256(components)
    relocated = json.loads(json.dumps(components))
    for name in ("cargo_home", "toolchain", "trusted_system_closure"):
        relocated[name]["manifest_path"] = f"/two/{name}.json"
    if semantic_runtime_sha256(relocated) != baseline:
        raise BuildError("semantic runtime digest depends on evidence paths")
    drifted = json.loads(json.dumps(relocated))
    drifted["toolchain"]["manifest_sha256"] = "d" * 64
    if semantic_runtime_sha256(drifted) == baseline:
        raise BuildError("semantic runtime digest ignores manifest drift")
    drifted = json.loads(json.dumps(relocated))
    drifted["trusted_system_closure"]["entry_count"] += 1
    if semantic_runtime_sha256(drifted) == baseline:
        raise BuildError("semantic runtime digest ignores authority counts")
    drifted = json.loads(json.dumps(relocated))
    drifted["trusted_system_closure"]["mounts"][0]["host_path"] = "/forged"
    drifted["trusted_system_closure"]["mounts"][0]["resolved_path"] = "/forged"
    try:
        semantic_runtime_sha256(drifted)
    except BuildError:
        pass
    else:
        raise BuildError("semantic runtime accepts forged system mount paths")
    if (
        system_symlink_scope(Path("/usr/bin"), "tool", "/bin/sh")
        != "within_closure"
        or system_symlink_scope(Path("/usr/bin"), "manual", "/usr/share/man")
        != "guest_inaccessible_external"
        or system_symlink_scope(
            Path("/usr/lib"),
            "gcc/x86_64-pc-linux-gnu/15.3.0/libitm.so",
            "/usr/lib/libitm.so",
        )
        != "within_closure"
    ):
        raise BuildError("trusted system symlink namespace model differs")
    for target in (
        "/proc/self/status",
        "//proc/self/status",
        "//asterism/source",
        "/",
    ):
        try:
            system_symlink_scope(Path("/usr/bin"), "hostile", target)
        except BuildError:
            pass
        else:
            raise BuildError(
                "trusted system symlink reached guest-accessible authority"
            )
    with tempfile.TemporaryDirectory(prefix="bn-ecm1-build-symlink-") as temporary:
        temporary_root = Path(temporary).resolve(strict=True)
        root = temporary_root / "source"
        root.mkdir()
        (temporary_root / "outside").write_bytes(b"outside")
        if (
            recursive_symlink_scope(
                Path("/usr/lib"),
                "gcc/x86_64-pc-linux-gnu/15.3.0/libitm.so",
                "/usr/lib/libitm.so",
                trusted_system=True,
            )
            != "within_closure"
        ):
            raise BuildError("trusted dangling symlink was rejected")
        for target in ("missing", "../outside"):
            try:
                recursive_symlink_scope(
                    root,
                    "link",
                    target,
                    trusted_system=False,
                )
            except BuildError:
                pass
            else:
                raise BuildError(
                    "nontrusted dangling or escaping symlink was accepted"
                )
    return {
        "manifest_drift_changes_digest": True,
        "mount_path_drift_rejected": True,
        "path_relocation_neutral": True,
        "special_root_symlink_rejected": True,
        "schema": SEMANTIC_INPUT_AUTHORITY_SCHEMA,
        "status": "ok",
    }


def self_test_idempotent_freeze() -> dict[str, Any]:
    """Prove final output freezing cannot invalidate pre-frozen input evidence."""

    evidence_schema = load_evidence_schema_self_test_module()
    with tempfile.TemporaryDirectory(prefix="bn-ecm1-freeze-") as temporary:
        temporary_root = Path(temporary).resolve(strict=True)
        evidence_root = temporary_root / "evidence"
        evidence_root.mkdir()
        ordering_root = temporary_root / "ordering"
        ordering_root.mkdir()
        (ordering_root / "a").mkdir()
        (ordering_root / "a-b").mkdir()
        (ordering_root / "crates" / "mess").mkdir(parents=True)
        (ordering_root / "crates" / "mess-bench").mkdir()
        write_new(ordering_root / "a" / "input", b"a\n", 0o444)
        write_new(ordering_root / "a-b" / "input", b"a-b\n", 0o444)
        write_new(ordering_root / "crates" / "mess" / "x", b"x\n", 0o444)
        write_new(
            ordering_root / "crates" / "mess-bench" / "y", b"y\n", 0o444
        )
        with RecursiveTreeAuthorityGuard(
            ordering_root,
            evidence_root / "ordering.json",
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
                raise BuildError("prefix-sibling recursive path order differs")
        root = temporary_root / "source"
        root.mkdir()
        write_new(root / "input.rs", b"fn main() {}\n", 0o444)
        root.chmod(0o555)
        with RecursiveTreeAuthorityGuard(
            root,
            evidence_root / "before.json",
            "source",
            "idempotent freeze before",
            allow_internal_symlinks=False,
        ) as before_guard:
            assert before_guard.initial_manifest is not None
            before = before_guard.initial_manifest
        make_read_only(root)
        with RecursiveTreeAuthorityGuard(
            root,
            evidence_root / "after.json",
            "source",
            "idempotent freeze after",
            allow_internal_symlinks=False,
        ) as after_guard:
            assert after_guard.initial_manifest is not None
            after = after_guard.initial_manifest
        if before != after:
            raise BuildError("idempotent final freeze changed recursive evidence")
        root.chmod(0o755)

        metadata_root = temporary_root / "metadata-only"
        metadata_root.mkdir()
        write_new(metadata_root / "unreadable", b"metadata only\n", 0o000)
        metadata_root.chmod(0o555)
        metadata_hashing = False
        with RecursiveTreeAuthorityGuard(
            metadata_root,
            evidence_root / "metadata-only.json",
            "metadata_only",
            "metadata-only unreadable regular",
            allow_internal_symlinks=False,
            hash_regular_contents=metadata_hashing,
        ) as metadata_guard:
            assert metadata_guard.initial_manifest is not None
            unreadable = next(
                entry
                for entry in metadata_guard.initial_manifest["entries"]
                if entry["path"] == "unreadable"
            )
            if unreadable["permissions"] != 0 or unreadable["sha256"] is not None:
                raise BuildError(
                    "metadata-only unreadable regular required content access"
                )

        class FixtureTrustedTreeGuard(RecursiveTreeAuthorityGuard):
            def _trusted_system_entry(
                self,
                metadata: os.stat_result,
                relative: str,
                *,
                symlink: bool = False,
            ) -> None:
                return None

        trusted_root = temporary_root / "trusted"
        trusted_root.mkdir()
        (trusted_root / "dangling").symlink_to("/usr/lib/libitm.so")
        with FixtureTrustedTreeGuard(
            trusted_root,
            evidence_root / "trusted.json",
            "system-usr-lib",
            "trusted dangling fixture",
            allow_internal_symlinks=True,
            hash_regular_contents=metadata_hashing,
            trusted_system_roots=(Path("/usr/lib"),),
        ) as trusted_guard:
            assert trusted_guard.initial_manifest is not None
            dangling = next(
                entry
                for entry in trusted_guard.initial_manifest["entries"]
                if entry["path"] == "dangling"
            )
            if dangling["symlink_scope"] != "within_closure":
                raise BuildError("trusted guard rejected dangling closure symlink")
    return {
        "metadata_only_unreadable_retained": True,
        "prefix_sibling_order_validated": True,
        "recursive_manifest_unchanged": True,
        "status": "ok",
        "trusted_dangling_symlink_retained": True,
    }


def self_test_validated_lock_records(
    repository: Path, lock_module: Any
) -> dict[str, Any]:
    """Prove reviewed Cargo.lock snapshots remain raw immutable bytes."""

    product_lock = (repository / LOCK_PATH).read_bytes()
    if sha256_bytes(product_lock) != PRODUCT_LOCK_SHA256:
        raise BuildError("self-test product Cargo.lock differs from frozen authority")
    with tempfile.TemporaryDirectory(prefix="bn-30fs-validated-locks-") as temporary:
        root = Path(temporary).resolve(strict=True)
        payloads = {
            "A": product_lock,
            "C": b"# synthetic current lock C\n",
            "D": b"# synthetic current lock D\n",
        }
        snapshots: dict[str, Any] = {}
        for name, payload in payloads.items():
            path = root / f"Cargo-{name}.lock"
            write_new(path, payload, 0o444)
            snapshot = lock_module.snapshot_file(path, f"self-test {name} lock")
            if snapshot.value is not None:
                raise BuildError("raw Cargo.lock snapshot unexpectedly parsed a value")
            snapshots[name] = snapshot
        records = validated_lock_records(types.SimpleNamespace(locks=snapshots))
        if (
            set(records) != {"A", "C", "D"}
            or records["A"]["sha256"] != PRODUCT_LOCK_SHA256
            or any(record["mode"] != 0o444 for record in records.values())
        ):
            raise BuildError("validated raw Cargo.lock records differ")
    return {
        "raw_value_none_accepted": True,
        "records": sorted(records),
        "status": "ok",
    }


def self_test_shared_overlay_pins() -> dict[str, Any]:
    """Exercise real and hostile shared injection without compiling children."""

    global SHARED
    original_shared = SHARED
    with tempfile.TemporaryDirectory(prefix="asterism-shared-pin-") as directory:
        scratch = Path(directory)
        shared = scratch / "shared"
        shared.mkdir()
        for name in SHARED_NAMES:
            write_new(shared / name, (original_shared / name).read_bytes(), 0o444)
        try:
            SHARED = shared
            accepted_root = scratch / "accepted"
            (accepted_root / SHARED_DESTINATION).mkdir(parents=True)
            accepted = inject_shared(accepted_root)
            if len(accepted) != len(SHARED_NAMES):
                raise BuildError("pinned shared injection cardinality differs")
            (shared / "control.rs").chmod(0o644)
            with (shared / "control.rs").open("ab") as output:
                output.write(b"\n// hostile shared mutation\n")
            (shared / "control.rs").chmod(0o444)
            hostile_root = scratch / "hostile"
            (hostile_root / SHARED_DESTINATION).mkdir(parents=True)
            try:
                inject_shared(hostile_root)
            except BuildError:
                hostile_rejected = True
            else:
                raise BuildError("mutated shared injection was accepted")
        finally:
            SHARED = original_shared
    return {
        "accepted_placements": len(accepted),
        "hostile_rejected": hostile_rejected,
        "status": "ok",
    }


def self_test_cargo_environment() -> dict[str, Any]:
    """Exercise the exact guest-root environment before any real build."""

    environment = cargo_environment(
        {"rustup_toolchain": "self-test-toolchain"},
        {"ASTERISM_SELF_TEST": "1"},
    )
    expected = {
        "CARGO_HOME": f"{GUEST_ROOT}/cargo-home",
        "GIT_CONFIG_GLOBAL": f"{GUEST_ROOT}/absent-gitconfig",
        "LD_ORIGIN_PATH": f"{GUEST_ROOT}/toolchain/bin",
        "RUSTC": f"{GUEST_ROOT}/toolchain/bin/rustc",
        "RUSTUP_TOOLCHAIN": "self-test-toolchain",
        "ASTERISM_SELF_TEST": "1",
    }
    if any(environment.get(name) != value for name, value in expected.items()):
        raise BuildError("self-test Cargo guest environment differs")
    for hostile in (
        {"CARGO_HOME": "/hostile"},
        {"LD_ORIGIN_PATH": "/host/toolchain/bin"},
    ):
        try:
            cargo_environment(
                {"rustup_toolchain": "self-test-toolchain"}, hostile
            )
        except BuildError:
            pass
        else:
            raise BuildError("self-test Cargo environment override was accepted")
    return {
        "guest_root": GUEST_ROOT,
        "override_rejected": True,
        "status": "ok",
    }


def self_test(repository: Path) -> None:
    static = validate_static_authority(repository)
    overlay = validate_product_overlay_authority(repository)
    # These exact dependencies intentionally do not have fixtures here.  The
    # command must fail closed until their independently reviewed bones land.
    fault = validate_fault_authority(repository)
    lock_module = load_lock_authority_module()
    validated_lock_records_self_test = self_test_validated_lock_records(
        repository, lock_module
    )
    cargo_artifact_hardlinks = self_test_cargo_example_hardlinks()
    shared_overlay_pins = self_test_shared_overlay_pins()
    cargo_environment_self_test = self_test_cargo_environment()
    cargo_config_guard = self_test_cargo_config_guard()
    external_canonical_utf8 = self_test_external_canonical_utf8()
    reviewed_cargo_config = self_test_reviewed_cargo_config_policy()
    semantic_runtime = self_test_semantic_runtime_authority()
    idempotent_freeze = self_test_idempotent_freeze()
    print(
        canonical_bytes(
            {
                "fault_checks": fault["normal"]["checks"],
                "cargo_artifact_hardlinks": cargo_artifact_hardlinks,
                "cargo_environment": cargo_environment_self_test,
                "cargo_config_guard": cargo_config_guard,
                "external_canonical_utf8": external_canonical_utf8,
                "reviewed_cargo_config_policy": reviewed_cargo_config,
                "lock_api": sorted(
                    name
                    for name in (
                        "capture_prebuild_materialized_tree",
                        "resample_builder_filesystem_admission",
                        "validate_authority",
                        "validation_result",
                    )
                    if callable(getattr(lock_module, name, None))
                ),
                "validated_lock_records": validated_lock_records_self_test,
                "overlay_checks": overlay["normal"]["checks"],
                "idempotent_freeze": idempotent_freeze,
                "overlay_hostile_mutations_rejected": overlay[
                    "hostile_mutations_rejected"
                ],
                "schema": SELF_TEST_SCHEMA,
                "semantic_runtime_authority": semantic_runtime,
                "shared_overlay_pins": shared_overlay_pins,
                "static_hostile_mutations_rejected": static["self_test"][
                    "hostile_mutations_rejected"
                ],
                "status": "ok",
            }
        ).decode(),
        end="",
    )


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    self_test_parser = commands.add_parser("self-test")
    self_test_parser.add_argument("--repository", required=True, type=Path)
    build_parser = commands.add_parser("build")
    build_parser.add_argument("--repository", required=True, type=Path)
    build_parser.add_argument("--lock-authority", required=True, type=Path)
    build_parser.add_argument("--lock-manifest", required=True, type=Path)
    build_parser.add_argument("--review-bundle", required=True, type=Path)
    build_parser.add_argument("--tools", required=True, type=Path)
    build_parser.add_argument("--output", required=True, type=Path)
    return root


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "self-test":
            self_test(args.repository.resolve(strict=True))
        else:
            build(args)
    except (
        BuildError,
        AttributeError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
        subprocess.SubprocessError,
        tarfile.TarError,
    ) as error:
        print(f"build-children: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
