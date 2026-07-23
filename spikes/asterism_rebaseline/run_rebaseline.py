#!/usr/bin/env python3
"""Fail-closed orchestration for the frozen bn-2l3n rebaseline.

This file deliberately contains no benchmark implementation and no performance
decision logic.  It consumes reviewed, immutable executables; realizes the
frozen physical order; owns the host lease and evidence files; and hands a
complete attempt to the independently reviewed evaluator.

The lifecycle and negative tests are derived from the hardened bn-22it
``owned_append`` runner.  The important difference is the raw-output boundary:
an evidence child may write exactly one canonical JSON object to its private
stdout file, but it never receives a shared CSV path.  Only this runner, after
validation through ``evidence_schema.py``, may append a canonical CSV row.
"""

from __future__ import annotations

import argparse
import ctypes
import csv
import fcntl
import hashlib
import importlib.util
import json
import os
import pwd
import re
import secrets
import select
import signal
import socket
import stat
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable, NoReturn, Protocol


PROTOCOL = "bn-2l3n-asterism-rebaseline-v3"
PROTOCOL_SHA256 = "d9ee10b2cccdaf6428bf1419a8c2ee74d272e987dc3617a80b64ad2e9d7a18dd"
HISTORICAL_BASELINE_SHA256 = "b2801a056a711de7a8643c15af2eb7d12a04b6315e0dec40a7beacc53c5bde40"
PREPARED_SCHEMA = "bn-2l3n-prepared-artifacts-v3"
CLAIM_SCHEMA = "bn-2l3n-prepared-claim-v3"
CONFIG_SCHEMA = "bn-2l3n-config-v3"
PROVENANCE_SCHEMA = "bn-2l3n-provenance-v3"
MANIFEST_SCHEMA = "asterism-rebaseline-manifest-record-v3"
FAILURE_SCHEMA = "asterism-rebaseline-failure-v3"
CORRECTNESS_ONLY_SCHEMA = "bn-2l3n-correctness-only-v3"
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
RUNNER_COMM = "asterism-run"
ACCOUNT_HOME = Path(pwd.getpwuid(os.getuid()).pw_dir).resolve()
LOCK_PATH = ACCOUNT_HOME / ".cache/mess-bench/global-measurement.lock"
SCRATCH_ROOT = ACCOUNT_HOME / ".cache/mess-bench/asterism-rebaseline"
MIN_FREE_BYTES = 137_438_953_472
MIN_FREE_INODES = 1_000_000
REQUIRED_FILESYSTEM_TYPE = "ext4"
MAX_LOAD1 = 6.0
QUIET_TIMEOUT_SECONDS = 120
QUIET_POLL_SECONDS = 1
ROW_TIMEOUT_SECONDS = 1_800
C_PROFILE_CHILD_TIMEOUT_SECONDS = 120
C_ROLE_LIFETIME_CONTRACT = {
    "schema": "bn-2l3n-c-role-lifetime-v3",
    "blocking_thread_keep_alive_ns": 3_600_000_000_000,
    "maximum_profile_child_timeout_ns": C_PROFILE_CHILD_TIMEOUT_SECONDS
    * 1_000_000_000,
    "ready_to_measured_spawn_blocking_sites": 1,
    "ready_to_measured_other_thread_birth_sites": 0,
}
PROCESS_SETTLE_MS = 400
GROUP_SETTLE_MS = 4_000
VARIANTS = ("A", "B", "C", "D")
PUBLIC_VARIANTS = ("A", "C", "D")
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
WILLIAMS = (
    ("A", "B", "D", "C"),
    ("B", "C", "A", "D"),
    ("C", "D", "B", "A"),
    ("D", "A", "C", "B"),
)
PRIMARY_BPW = {
    "Process": {1: 40_000, 10: 12_500, 100: 2_500, 1000: 250},
    "Group": {1: 800, 10: 500, 100: 300, 1000: 100},
}
FAIRNESS_BPW = {
    "Process": {1: 5_000, 100: 500},
    "Group": {1: 200, 100: 200},
}
TRACK_CARDINALITY = {
    "primary": 512,
    "new_names": 64,
    "fairness": 64,
    "cpu_profiles": 64,
    "syscall_profiles": 64,
    "structural_traces": 15,
    "reopen": 9,
}
CSV_FILENAMES = {
    "primary": "primary.csv",
    "new_names": "new_names.csv",
    "fairness": "fairness.csv",
    "cpu_profiles": "cpu_profiles.csv",
    "syscall_profiles": "syscall_profiles.csv",
    "structural_traces": "structural_traces.csv",
    "reopen": "reopen.csv",
}
SHA256_RE = re.compile(r"[0-9a-f]{64}")
GIT_OBJECT_RE = re.compile(r"[0-9a-f]{40}")
PERF_EVENTS = ("cycles:u", "instructions:u", "task-clock:u", "context-switches:u")
PERF_ACK_WIRE = b"ack\n\0"
PERF_ACK_LEDGER_ENTRY = b"ack\n"
PERF_CHILD_FD_ENVIRONMENT = (
    "ASTERISM_REBASELINE_PERF_COMMAND_FD",
    "ASTERISM_REBASELINE_PERF_ACK_FD",
    "ASTERISM_REBASELINE_PERF_ACK_LEDGER_FD",
)
PERF_PERMISSION_ENVIRONMENT = "ASTERISM_REBASELINE_PERF_PERMISSION_RESULT"
TRACE_SYSCALLS = (
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
CURRENT_CHILDREN_ATTESTATION_SCHEMA = "bn-ecm1-current-children-build-v2"
CURRENT_BUILD_CARGO_CONFIG_SCHEMA = "bn-30fs-build-cargo-config-search-v1"
SEMANTIC_INPUT_AUTHORITY_SCHEMA = "bn-ecm1-semantic-input-authority-v1"
RECURSIVE_TREE_AUTHORITY_SCHEMA = "bn-ecm1-recursive-tree-authority-v1"
TRUSTED_SYSTEM_CLOSURE_SCHEMA = "bn-ecm1-trusted-system-closure-v1"
SEMANTIC_MANIFEST_TOTAL = 48
SEMANTIC_MANIFEST_COUNTS = {"current": 12, "release": 20, "resolver": 16}
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
SEMANTIC_CLOSURE_BINDING_FIELDS = {
    "entry_count",
    "manifest_path",
    "mounts",
    "mutation_events_absent",
    "schema",
    "sha256",
    "watch_count",
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
CURRENT_BUILD_CARGO_CONFIG_FIELDS = {
    "cargo_search",
    "cargo_home_tree",
    "preserved_top_level_entries",
    "schema",
}
CURRENT_BUILD_CARGO_SEARCH_FIELDS = {
    "cargo_home_path",
    "cwd",
    "entries",
    "schema",
}
CURRENT_BUILD_CARGO_HOME_TREE_FIELDS = {
    "entry_count",
    "equal_pre_post",
    "path",
    "post_sha256",
    "pre_sha256",
    "watch_count",
}
CURRENT_BUILD_PRESERVED_ENTRY_FIELDS = {"identity", "name", "type"}
CURRENT_BUILD_PRESERVED_REGULAR_IDENTITY_FIELDS = {
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
}
CURRENT_BUILD_PRESERVED_DIRECTORY_IDENTITY_FIELDS = {
    "changed_ns",
    "device",
    "file_type",
    "inode",
    "link_count",
    "modified_ns",
    "path",
    "permissions",
    "size",
}
SEMANTIC_TOOLCHAIN_FIELDS = {
    "bwrap_path",
    "bwrap_sha256",
    "cargo_home_path",
    "cargo_path",
    "cargo_sha256",
    "cargo_version_verbose",
    "git_path",
    "git_sha256",
    "rustc_host",
    "rustc_path",
    "rustc_sha256",
    "rustc_version_verbose",
    "rust_lld_path",
    "rust_lld_sha256",
    "rustup_home_path",
    "rustup_path",
    "rustup_sha256",
    "rustup_toolchain",
}
CURRENT_BUILD_FILE_IDENTITY_FIELDS = {
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
}
CURRENT_BUILD_TOOL_RECORD_FIELDS = {"identity", "path_chain", "trusted_system"}
CURRENT_BUILD_DEVICE_RECORD_FIELDS = {
    "identity",
    "parent_path_chain",
    "trusted_system",
}
CURRENT_BUILD_DEVICE_IDENTITY_FIELDS = {
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
}
CURRENT_BUILD_DIRECTORY_IDENTITY_FIELDS = {
    "changed_ns",
    "device",
    "file_type",
    "inode",
    "link_count",
    "modified_ns",
    "path",
    "permissions",
    "size",
}
PREPARED_EXECUTION_FILE_FIELDS = {"identity", "mode", "path", "sha256", "size"}
PREPARED_EXECUTION_FILE_IDENTITY_FIELDS = {
    "changed_ns",
    "device",
    "inode",
    "link_count",
    "modified_ns",
}
PREPARED_TOOLCHAIN_ROOT_FIELDS = {"device", "inode", "link_count", "mode"}
SEMANTIC_DESCRIPTOR_OPTIONS = frozenset(
    {"--ro-bind-fd", "--bind-fd", "--ro-bind-data", "--bind-data"}
)
TRUSTED_SYSTEM_MOUNTS = (
    ("/usr/bin", "/usr/bin"),
    ("/usr/lib", "/usr/lib"),
    ("/usr/include", "/usr/include"),
)
SEMANTIC_CONFIG_DESTINATIONS = (
    "/asterism/source/.cargo/config.toml",
    "/asterism/source/.cargo/config",
    "/asterism/cargo-home/config.toml",
    "/asterism/cargo-home/config",
)
SEMANTIC_CARGO_HOME = "/asterism/cargo-home"
SEMANTIC_CARGO_HOME_FD_PREFIX = "/proc/self/fd/"
SEMANTIC_TOOLCHAIN_BIN = "/asterism/toolchain/bin"
RELEASE_BUILD_DESTINATIONS = {
    "/usr/bin",
    "/usr/lib",
    "/usr/include",
    "/asterism/source",
    "/asterism/target",
    "/asterism/toolchain",
    "/asterism/toolchain/bin/cargo",
    "/asterism/toolchain/bin/rustc",
    *SEMANTIC_CONFIG_DESTINATIONS,
}
CURRENT_BUILD_FD_DESTINATIONS = {
    *(RELEASE_BUILD_DESTINATIONS - set(SEMANTIC_CONFIG_DESTINATIONS)),
    "/asterism/python3",
}
RESOLVER_DESTINATIONS = RELEASE_BUILD_DESTINATIONS - {"/asterism/target"}
RELEASE_BUILD_BASE_DESCRIPTOR_BINDINGS = (
    *(("--ro-bind-fd", guest) for _host, guest in TRUSTED_SYSTEM_MOUNTS),
    ("--ro-bind-fd", "/asterism/source"),
    ("--bind-fd", "/asterism/target"),
    ("--ro-bind-fd", "/asterism/toolchain"),
    ("--ro-bind-fd", "/asterism/toolchain/bin/cargo"),
    ("--ro-bind-fd", "/asterism/toolchain/bin/rustc"),
)
RESOLVER_DESCRIPTOR_BINDINGS = (
    *(("--ro-bind-fd", guest) for _host, guest in TRUSTED_SYSTEM_MOUNTS),
    ("--bind-fd", "/asterism/source"),
    ("--ro-bind-fd", "/asterism/toolchain"),
    ("--ro-bind-fd", "/asterism/toolchain/bin/cargo"),
    ("--ro-bind-fd", "/asterism/toolchain/bin/rustc"),
    *(("--ro-bind-fd", path) for path in SEMANTIC_CONFIG_DESTINATIONS),
)


class RunnerFailure(Exception):
    """A classified failure which must terminate the whole attempt."""

    def __init__(self, reason: str, *, exit_code: int = 20) -> None:
        super().__init__(reason)
        self.reason = reason
        self.exit_code = exit_code


class EvidenceSchema(Protocol):
    PROTOCOL: str
    CSV_FIELDS_BY_TRACK: dict[str, tuple[str, ...] | list[str]]
    ARTIFACT_FILE_MODE: int
    ARTIFACT_INVENTORY_FIELDS: tuple[str, ...]

    def canonical_json_bytes(self, value: Any) -> bytes: ...

    def prepared_authority_canonical_json_bytes(self, value: Any) -> bytes: ...

    def validate_child_records(
        self,
        kind: str,
        records: list[dict[str, Any]],
        context: dict[str, Any],
    ) -> Any: ...

    def encode_csv_row(
        self, track: str, row: dict[str, Any], write_header: bool
    ) -> bytes: ...

    def expected_order(
        self, config: dict[str, Any], track: str
    ) -> list[dict[str, Any]]: ...

    def validate_source_approval(self, approval: dict[str, Any]) -> None: ...

    def validate_prepared_artifacts(
        self,
        prepared: dict[str, Any],
        approval: dict[str, Any],
        prepared_path: Path,
    ) -> None: ...

    def validate_binary_contract(self, contract: dict[str, Any]) -> None: ...

    def expected_profile_contract(self) -> dict[str, Any]: ...

    def row_child_environment(self, **arguments: Any) -> dict[str, str]: ...

    def correctness_child_environment(self, **arguments: Any) -> dict[str, str]: ...


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json_bytes(value: Any) -> bytes:
    try:
        text = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise RunnerFailure(f"value is not canonical JSON: {error}") from error
    return text.encode("ascii") + b"\n"


def reviewed_authority_canonical_json_bytes(value: Any) -> bytes:
    """Return the UTF-8 canonical form emitted by reviewed authority tools."""

    try:
        text = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise RunnerFailure(
            f"value is not reviewed authority JSON: {error}"
        ) from error
    return text.encode("utf-8") + b"\n"


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fsync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write(path: Path, payload: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    )
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        fsync_dir(path.parent)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def atomic_json(path: Path, value: Any) -> None:
    atomic_write(path, canonical_json_bytes(value))


def atomic_create_json(path: Path, value: Any, *, mode: int = 0o600) -> None:
    """Publish canonical JSON once; never overwrite a prior consumption."""

    ready = path.with_name(
        f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}.ready"
    )
    atomic_write(ready, canonical_json_bytes(value), mode=mode)
    try:
        os.link(ready, path)
        fsync_dir(path.parent)
    except FileExistsError as error:
        raise RunnerFailure(f"single-use artifact already claimed: {path}") from error
    finally:
        try:
            ready.unlink()
            fsync_dir(path.parent)
        except FileNotFoundError:
            pass


def append_bytes(path: Path, payload: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        written = 0
        while written < len(payload):
            count = os.write(descriptor, payload[written:])
            if count <= 0:
                raise RunnerFailure(f"short append to {path}")
            written += count
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def append_jsonl(path: Path, value: Any) -> None:
    append_bytes(path, canonical_json_bytes(value))


def load_canonical_json(path: Path) -> dict[str, Any]:
    try:
        payload = path.read_bytes()
        value = json.loads(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RunnerFailure(f"cannot read JSON {path}: {error}", exit_code=2) from error
    if not isinstance(value, dict) or payload != canonical_json_bytes(value):
        raise RunnerFailure(f"{path} is not one canonical JSON object", exit_code=2)
    return value


def load_reviewed_authority_json(path: Path) -> dict[str, Any]:
    try:
        payload = path.read_bytes()
        value = json.loads(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RunnerFailure(
            f"cannot read reviewed authority JSON {path}: {error}",
            exit_code=2,
        ) from error
    if (
        not isinstance(value, dict)
        or payload != reviewed_authority_canonical_json_bytes(value)
    ):
        raise RunnerFailure(
            f"{path} is not one reviewed authority JSON object",
            exit_code=2,
        )
    return value


def require_exact_keys(value: dict[str, Any], expected: set[str], context: str) -> None:
    observed = set(value)
    if observed != expected:
        raise RunnerFailure(
            f"{context} keys differ: missing={sorted(expected - observed)} "
            f"extra={sorted(observed - expected)}",
            exit_code=2,
        )


def reject_superseded_authority(
    value: Any, context: str, path: tuple[str, ...] = ()
) -> None:
    """Reject nested v2/r5 identities before shared semantic validation."""

    if isinstance(value, dict):
        for key, item in value.items():
            reject_superseded_authority(item, context, (*path, str(key)))
        return
    if isinstance(value, list):
        for ordinal, item in enumerate(value):
            reject_superseded_authority(item, context, (*path, str(ordinal)))
        return
    if not isinstance(value, str):
        return
    # The v3 protocol pins the current-children attestation schema itself at
    # this -v2 identity (CURRENT_CHILDREN_ATTESTATION_SCHEMA, required exactly
    # during prepared validation), so it is current authority, not superseded.
    if value == CURRENT_CHILDREN_ATTESTATION_SCHEMA:
        return
    leaf = path[-1] if path else ""
    identity_field = leaf == "protocol" or leaf == "schema" or leaf.endswith(
        "_schema"
    )
    rebaseline_identity = value.startswith("bn-2l3n-") or value.startswith(
        "asterism-rebaseline-"
    )
    if (identity_field or rebaseline_identity) and (
        value.endswith("-v2") or value.endswith("-r5")
    ):
        rendered = ".".join(path) or "<root>"
        raise RunnerFailure(
            f"{context} contains superseded authority at {rendered}: {value!r}",
            exit_code=2,
        )


def call_shared_validator(
    schema: Any, name: str, context: str, *arguments: Any
) -> None:
    validator = getattr(schema, name, None)
    if not callable(validator):
        raise RunnerFailure(
            f"shared schema omits required deep validator {name}", exit_code=2
        )
    try:
        validator(*arguments)
    except Exception as error:
        raise RunnerFailure(
            f"{context} failed shared deep validation: "
            f"{error.__class__.__name__}: {error}",
            exit_code=2,
        ) from error


def set_process_comm(name: str) -> None:
    if len(name.encode()) > 15:
        raise RunnerFailure(f"Linux comm exceeds 15 bytes: {name!r}")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(15, name.encode(), 0, 0, 0) != 0:  # PR_SET_NAME
        error = ctypes.get_errno()
        raise RunnerFailure(f"cannot bind runner comm: errno={error}")


def verify_current_runtime(executable: Executable, expected_comm: str) -> None:
    proc_exe = (Path("/proc") / str(os.getpid()) / "exe").resolve(strict=True)
    if (
        proc_exe != executable.path
        or sha256(Path("/proc") / str(os.getpid()) / "exe") != executable.sha256
        or parse_proc_stat(os.getpid())["comm"] != expected_comm
    ):
        raise RunnerFailure("executed runner runtime binding differs", exit_code=2)


def parse_proc_stat(pid: int) -> dict[str, Any]:
    payload = (Path("/proc") / str(pid) / "stat").read_text()
    opened = payload.find("(")
    closed = payload.rfind(")")
    if opened < 0 or closed <= opened:
        raise OSError("malformed proc stat")
    fields = payload[closed + 2 :].split()
    if len(fields) <= 19:
        raise OSError("short proc stat")
    return {
        "pid": pid,
        "comm": payload[opened + 1 : closed],
        "state": fields[0],
        "ppid": int(fields[1]),
        "pgrp": int(fields[2]),
        "session": int(fields[3]),
        "starttime_ticks": int(fields[19]),
    }


def read_proc_cmdline(pid: int) -> list[str]:
    payload = (Path("/proc") / str(pid) / "cmdline").read_bytes()
    if not payload.endswith(b"\0"):
        raise RunnerFailure(f"process {pid} cmdline is not NUL-terminated")
    try:
        result = [item.decode("utf-8") for item in payload[:-1].split(b"\0")]
    except UnicodeDecodeError as error:
        raise RunnerFailure(f"process {pid} cmdline is not UTF-8") from error
    if not result or any(not item for item in result):
        raise RunnerFailure(f"process {pid} cmdline is empty or malformed")
    return result


def same_identity(left: dict[str, Any], right: dict[str, Any] | None) -> bool:
    return bool(
        right
        and left.get("pid") == right.get("pid")
        and left.get("starttime_ticks") == right.get("starttime_ticks")
    )


def set_child_parked(
    identity: dict[str, Any], parked: bool, *, timeout_seconds: float = 5.0
) -> dict[str, Any]:
    """Stop or continue one identity-bound benchmark process and prove the state.

    Reopen profiling samples ``/proc`` only while the subject is SIGSTOP-parked.
    The start-tick comparison prevents a recycled PID from satisfying the proof.
    """

    expected_states = {"T", "t"} if parked else set("RSDIW")
    try:
        os.kill(int(identity["pid"]), signal.SIGSTOP if parked else signal.SIGCONT)
    except ProcessLookupError as error:
        raise RunnerFailure("benchmark vanished while changing parked state") from error
    deadline = time.monotonic() + timeout_seconds
    observed: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        try:
            observed = parse_proc_stat(int(identity["pid"]))
        except (FileNotFoundError, ProcessLookupError) as error:
            raise RunnerFailure("benchmark vanished during parked-state proof") from error
        if not same_identity(identity, observed):
            raise RunnerFailure("benchmark identity changed during parked-state proof")
        if observed["state"] in expected_states:
            return {
                "pid": identity["pid"],
                "starttime_ticks": identity["starttime_ticks"],
                "requested": "parked" if parked else "running",
                "observed_state": observed["state"],
                "proved_monotonic_ns": time.monotonic_ns(),
            }
        time.sleep(0.001)
    raise RunnerFailure(
        "benchmark did not reach "
        f"{'parked' if parked else 'running'} state: {observed}"
    )


def process_group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def terminate_process_group(child: subprocess.Popen[Any]) -> int:
    if child.poll() is None:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait(timeout=10)
    else:
        child.wait()
    if process_group_exists(child.pid):
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + 10
        while process_group_exists(child.pid) and time.monotonic() < deadline:
            time.sleep(0.01)
    if process_group_exists(child.pid):
        raise RunnerFailure(f"child process group {child.pid} remains alive")
    return int(child.returncode if child.returncode is not None else 125)


def reject_orphan_process_group(
    child: subprocess.Popen[Any], context: str, *, exit_code: int = 20
) -> None:
    """Kill/reap a live session group, then reject the transition.

    ``Popen.poll()`` only describes the session leader. A helper can exit after
    leaving descendants in its process group, so cleanup must probe and kill
    the group even when the leader has already been waited.
    """

    if not process_group_exists(child.pid):
        return
    cleanup_error: BaseException | None = None
    try:
        terminate_process_group(child)
    except BaseException as error:
        cleanup_error = error
    group_absent = not process_group_exists(child.pid)
    if cleanup_error is not None or not group_absent:
        raise RunnerFailure(
            f"{context} left process group {child.pid}; cleanup failed: "
            f"{cleanup_error!r}, group_absent={group_absent}",
            exit_code=exit_code,
        ) from cleanup_error
    raise RunnerFailure(
        f"{context} left process group {child.pid}; exact group was killed and reaped",
        exit_code=exit_code,
    )


def csv_shape(path: Path, expected_fields: tuple[str, ...]) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False, "bytes": 0, "rows": 0, "complete": True}
    payload = path.read_bytes()
    complete = not payload or payload.endswith(b"\n")
    try:
        rows = list(csv.reader(payload.decode().splitlines()))
    except (UnicodeDecodeError, csv.Error) as error:
        raise RunnerFailure(f"malformed CSV {path}: {error}") from error
    fields = tuple(rows[0]) if rows else ()
    data = rows[1:] if rows else []
    uniform = all(len(row) == len(fields) for row in data)
    return {
        "exists": True,
        "bytes": len(payload),
        "rows": len(data),
        "fields": fields,
        "complete": complete and uniform and fields == expected_fields,
        "sha256": sha256_bytes(payload),
    }


def hash_sorted_cells(track: str, cells: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Version-independent deterministic permutation derived from the protocol."""

    def key(cell: dict[str, Any]) -> tuple[str, bytes]:
        payload = canonical_json_bytes(cell)
        digest = hashlib.sha256(
            PROTOCOL_SHA256.encode() + b"\0" + track.encode() + b"\0" + payload
        ).hexdigest()
        return digest, payload

    return sorted(cells, key=key)


def primary_cells() -> list[dict[str, Any]]:
    return hash_sorted_cells(
        "primary",
        [
            {
                "durability": durability,
                "payload_size": payload,
                "batch_size": batch,
                "writers": writers,
            }
            for durability in ("Process", "Group")
            for payload in (24, 250)
            for batch in (1, 10, 100, 1000)
            for writers in (1, 4)
        ],
    )


def new_name_cells() -> list[dict[str, Any]]:
    return hash_sorted_cells(
        "new_names",
        [
            {
                "durability": durability,
                "payload_size": 250,
                "batch_size": 1,
                "writers": writers,
            }
            for durability in ("Process", "Group")
            for writers in (1, 4)
        ],
    )


def fairness_cells() -> list[dict[str, Any]]:
    return hash_sorted_cells(
        "fairness",
        [
            {
                "durability": durability,
                "payload_size": 250,
                "batch_size": batch,
                "writers": 64,
            }
            for durability in ("Process", "Group")
            for batch in (1, 100)
        ],
    )


def sentinel_cells(track: str) -> list[dict[str, Any]]:
    return hash_sorted_cells(
        track,
        [
            {
                "durability": durability,
                "payload_size": 250,
                "batch_size": batch,
                "writers": 4,
            }
            for durability in ("Process", "Group")
            for batch in (1, 1000)
        ],
    )


def williams_rows(track: str, cells: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    ordinal = 0
    for block, variants in enumerate(WILLIAMS, start=1):
        traversal = list(enumerate(cells, start=1))
        if block in (2, 4):
            traversal.reverse()
        for cell_ordinal, cell in traversal:
            for slot, variant in enumerate(variants, start=1):
                ordinal += 1
                rows.append(
                    {
                        "track": track,
                        "ordinal": ordinal,
                        "block": block,
                        "cell_ordinal": cell_ordinal,
                        "slot": slot,
                        "variant": variant,
                        **cell,
                    }
                )
    return rows


def structural_trace_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for variant in PUBLIC_VARIANTS:
        for durability in ("Process", "Group"):
            for writers in (1, 4):
                rows.append(
                    {
                        "track": "structural_traces",
                        "row_ordinal": len(rows) + 1,
                        "trace_kind": "new_names",
                        "variant": variant,
                        "durability": durability,
                        "writers": writers,
                        "appends_per_writer": 8,
                    }
                )
    for variant in PUBLIC_VARIANTS:
        rows.append(
            {
                "track": "structural_traces",
                "row_ordinal": len(rows) + 1,
                "trace_kind": "reopen",
                "variant": variant,
                "durability": "Process",
                "writers": 1,
                "appends_per_writer": 0,
            }
        )
    return rows


def reopen_rows() -> list[dict[str, Any]]:
    latin = (("A", "C", "D"), ("C", "D", "A"), ("D", "A", "C"))
    rows: list[dict[str, Any]] = []
    for repetition, variants in enumerate(latin, start=1):
        for slot, variant in enumerate(variants, start=1):
            rows.append(
                {
                    "track": "reopen",
                    "row_ordinal": len(rows) + 1,
                    "latin_block": repetition,
                    "ordinal_in_block": slot,
                    "variant": variant,
                }
            )
    return rows


def expected_orders() -> dict[str, list[dict[str, Any]]]:
    return {
        "primary": williams_rows("primary", primary_cells()),
        "new_names": williams_rows("new_names", new_name_cells()),
        "fairness": williams_rows("fairness", fairness_cells()),
        "cpu_profiles": williams_rows(
            "cpu_profiles", sentinel_cells("cpu_profiles")
        ),
        "syscall_profiles": williams_rows(
            "syscall_profiles", sentinel_cells("syscall_profiles")
        ),
        "structural_traces": structural_trace_rows(),
        "reopen": reopen_rows(),
    }


@dataclass(frozen=True)
class Executable:
    name: str
    path: Path
    sha256: str
    mode: int
    comm: str


@dataclass(frozen=True)
class SupportFile:
    name: str
    path: Path
    sha256: str
    mode: int


@dataclass(frozen=True)
class Variant:
    name: str
    product_commit: str
    product_tree: str
    binary_kind: str
    timed_surface: str
    correctness_oracle_mode: bool
    executable: Executable
    contract: dict[str, Any]
    contract_argv: tuple[str, ...]
    contract_env: dict[str, str]
    evidence_argv: tuple[str, ...]
    evidence_env: dict[str, str]
    trace_path_marker_templates: dict[str, Any]


@dataclass(frozen=True)
class Prepared:
    schema: EvidenceSchema
    path: Path
    digest: str
    value: dict[str, Any]
    root: Path
    source_approval: Path
    source_approval_sha256: str
    source_approval_value: dict[str, Any]
    source_review_files: dict[str, SupportFile]
    release_compile_out: SupportFile
    release_compile_out_value: dict[str, Any]
    tools_manifest: SupportFile
    tools_manifest_value: dict[str, Any]
    claim_path: Path
    variants: dict[str, Variant]
    tools: dict[str, Executable]
    support_files: dict[str, SupportFile]
    inputs: dict[str, SupportFile]
    tracked_comm: frozenset[str]


def _semantic_runtime_sha256(authority: dict[str, Any]) -> str:
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
    closure = authority["trusted_system_closure"]
    normalized = {
        "cargo_home": {
            field: authority["cargo_home"].get(field) for field in tree_fields
        },
        "schema": SEMANTIC_INPUT_AUTHORITY_SCHEMA,
        "toolchain": {
            field: authority["toolchain"].get(field) for field in tree_fields
        },
        "trusted_system_closure": {
            field: closure.get(field) for field in closure_fields
        },
    }
    return sha256_bytes(canonical_json_bytes(normalized))


def _semantic_file_identity(metadata: os.stat_result) -> dict[str, int]:
    return {
        "changed_ns": metadata.st_ctime_ns,
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "link_count": metadata.st_nlink,
        "mode": stat.S_IMODE(metadata.st_mode),
        "modified_ns": metadata.st_mtime_ns,
        "size": metadata.st_size,
    }


def _read_retained_file(descriptor: int) -> bytes:
    chunks = []
    offset = 0
    while True:
        chunk = os.pread(descriptor, 1024 * 1024, offset)
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)
        offset += len(chunk)


def _sha256_retained_file(descriptor: int) -> str:
    digest = hashlib.sha256()
    offset = 0
    while True:
        chunk = os.pread(descriptor, 1024 * 1024, offset)
        if not chunk:
            return digest.hexdigest()
        digest.update(chunk)
        offset += len(chunk)


def _semantic_directory_identity(
    metadata: os.stat_result,
) -> tuple[int, int, int, int, int]:
    """Return substitution-stable identity for one retained path ancestor.

    Only fields that a path *substitution* would change are compared:
    st_dev+st_ino pin the exact ancestor inode (defeating symlink, bind-mount,
    rename-to-a-different-inode, or inode replacement of any ancestor), and
    st_mode+st_uid+st_gid catch permission/ownership tampering.

    st_nlink, st_size, st_mtime_ns and st_ctime_ns are deliberately excluded.
    They change as a byproduct of adding/removing *sibling* entries in a
    directory (the runner creating its own ``rehearsal-*`` output dir, or any
    unrelated process writing under a shared ancestor such as ``~/.cache``),
    which is not a substitution and must not fail-stop a run.  Including them
    made the retained-evidence recheck reach up into shared, externally-mutated
    cache parents and abort on activity unrelated to the evidence.  The
    retained manifest *file* remains bound by its content hash and its own
    single-link file identity; genuine ancestor substitution (symlink swap,
    a different inode, a mode/owner change) is still rejected by the fields
    above.
    """

    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
    )


def _open_semantic_manifest_no_follow(
    path: Path,
) -> tuple[
    list[int],
    list[tuple[int, int, int, int, int]],
    int,
]:
    """Open one absolute manifest through a descriptor-relative no-follow chain."""

    directory_flags = (
        os.O_RDONLY
        | os.O_DIRECTORY
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0)
    )
    file_flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    directories: list[int] = []
    identities: list[
        tuple[int, int, int, int, int]
    ] = []
    file_descriptor = -1
    try:
        current = os.open("/", directory_flags)
        directories.append(current)
        metadata = os.fstat(current)
        identities.append(_semantic_directory_identity(metadata))
        for component in path.parts[1:-1]:
            current = os.open(component, directory_flags, dir_fd=current)
            directories.append(current)
            metadata = os.fstat(current)
            if not stat.S_ISDIR(metadata.st_mode):
                raise OSError(f"semantic ancestor {component!r} is not a directory")
            identities.append(_semantic_directory_identity(metadata))
        if len(path.parts) < 2:
            raise OSError("semantic manifest path names no file")
        file_descriptor = os.open(path.name, file_flags, dir_fd=current)
        if not stat.S_ISREG(os.fstat(file_descriptor).st_mode):
            raise OSError("semantic manifest is not a regular file")
        return directories, identities, file_descriptor
    except BaseException:
        if file_descriptor >= 0:
            os.close(file_descriptor)
        for descriptor in reversed(directories):
            os.close(descriptor)
        raise


def _confirm_semantic_manifest_path(
    path: Path,
    directory_identities: list[
        tuple[int, int, int, int, int]
    ],
    expected_identity: dict[str, int],
) -> None:
    """Prove the lexical chain still names the retained physical file."""

    directory_flags = (
        os.O_RDONLY
        | os.O_DIRECTORY
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0)
    )
    verified_directories = [os.open("/", directory_flags)]
    try:
        current = verified_directories[0]
        metadata = os.fstat(current)
        if _semantic_directory_identity(metadata) != directory_identities[0]:
            raise OSError("semantic root directory identity changed")
        for index, component in enumerate(path.parts[1:-1], start=1):
            current = os.open(
                component,
                directory_flags,
                dir_fd=current,
            )
            verified_directories.append(current)
            metadata = os.fstat(current)
            if (
                _semantic_directory_identity(metadata)
                != directory_identities[index]
            ):
                raise OSError("semantic manifest ancestor was replaced")
        path_metadata = os.stat(path.name, dir_fd=current, follow_symlinks=False)
        if _semantic_file_identity(path_metadata) != expected_identity:
            raise OSError("semantic manifest path identity changed")
        if any(
            _semantic_directory_identity(os.fstat(descriptor)) != expected
            for descriptor, expected in zip(
                verified_directories,
                directory_identities,
                strict=True,
            )
        ):
            raise OSError("semantic manifest ancestor changed during confirmation")
    finally:
        for descriptor in reversed(verified_directories):
            os.close(descriptor)


def _load_canonical_support_authority(
    support: SupportFile,
    context: str,
) -> dict[str, Any]:
    """Read one reviewed authority through a stable no-follow path snapshot."""

    directories: list[int] = []
    descriptor = -1
    try:
        directories, directory_identities, descriptor = (
            _open_semantic_manifest_no_follow(support.path)
        )
        before = os.fstat(descriptor)
        identity = _semantic_file_identity(before)
        payload = _read_retained_file(descriptor)
        after = os.fstat(descriptor)
        _confirm_semantic_manifest_path(
            support.path,
            directory_identities,
            identity,
        )
        if any(
            _semantic_directory_identity(os.fstat(directory)) != expected
            for directory, expected in zip(
                directories,
                directory_identities,
                strict=True,
            )
        ):
            raise OSError("reviewed authority ancestor changed during read")
        value = json.loads(payload)
        if (
            _semantic_file_identity(after) != identity
            or stat.S_IMODE(before.st_mode) != support.mode
            or sha256_bytes(payload) != support.sha256
            or not isinstance(value, dict)
            or reviewed_authority_canonical_json_bytes(value) != payload
        ):
            raise RunnerFailure(f"{context} reviewed authority differs", exit_code=2)
        return value
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RunnerFailure(
            f"{context} reviewed authority is unavailable: {error}",
            exit_code=2,
        ) from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        for directory in reversed(directories):
            os.close(directory)


@dataclass
class RetainedSemanticManifest:
    """An open, immutable semantic-evidence file retained for the whole run."""

    context: str
    path: Path
    sha256: str
    descriptor: int
    value: dict[str, Any] | None
    identity: dict[str, int]

    def verify(self) -> None:
        if self.descriptor < 0:
            raise RunnerFailure(f"{self.context} retained descriptor is closed")
        directories: list[int] = []
        directory_identities: list[
            tuple[int, int, int, int, int]
        ] = []
        live_descriptor = -1
        try:
            directories, directory_identities, live_descriptor = (
                _open_semantic_manifest_no_follow(self.path)
            )
            live_metadata_before = os.fstat(live_descriptor)
            descriptor_metadata_before = os.fstat(self.descriptor)
            path_digest = _sha256_retained_file(live_descriptor)
            descriptor_digest = _sha256_retained_file(self.descriptor)
            live_metadata_after = os.fstat(live_descriptor)
            descriptor_metadata_after = os.fstat(self.descriptor)
            if any(
                _semantic_directory_identity(metadata) != expected
                for metadata, expected in zip(
                    (os.fstat(descriptor) for descriptor in directories),
                    directory_identities,
                    strict=True,
                )
            ):
                raise OSError("semantic manifest ancestor changed while retained")
            _confirm_semantic_manifest_path(
                self.path,
                directory_identities,
                self.identity,
            )
            if any(
                _semantic_directory_identity(os.fstat(descriptor)) != expected
                for descriptor, expected in zip(
                    directories,
                    directory_identities,
                    strict=True,
                )
            ):
                raise OSError("semantic manifest ancestor changed after confirmation")
        except OSError as error:
            raise RunnerFailure(
                f"{self.context} retained evidence is unavailable: {error}"
            ) from error
        finally:
            if live_descriptor >= 0:
                os.close(live_descriptor)
            for descriptor in reversed(directories):
                os.close(descriptor)
        if (
            _semantic_file_identity(live_metadata_before) != self.identity
            or _semantic_file_identity(live_metadata_after) != self.identity
            or _semantic_file_identity(descriptor_metadata_before) != self.identity
            or _semantic_file_identity(descriptor_metadata_after) != self.identity
            or path_digest != self.sha256
            or descriptor_digest != self.sha256
        ):
            raise RunnerFailure(f"{self.context} retained evidence changed")

    def close(self) -> None:
        if self.descriptor >= 0:
            descriptor = self.descriptor
            self.descriptor = -1
            os.close(descriptor)


def _final_verify_close_retained_manifests(
    manifests: Iterable[RetainedSemanticManifest],
    primary_error: BaseException,
) -> list[str]:
    """Final-verify and close partial lifetime state without replacing its error."""

    secondary_errors: list[str] = []
    for semantic_manifest in manifests:
        if semantic_manifest.descriptor < 0:
            secondary_errors.append(
                f"{semantic_manifest.context} final verification: "
                "retained descriptor was already closed"
            )
            continue
        try:
            semantic_manifest.verify()
        except BaseException as error:
            secondary_errors.append(
                f"{semantic_manifest.context} final verification: "
                f"{error.__class__.__name__}: {error}"
            )
        finally:
            try:
                semantic_manifest.close()
            except BaseException as error:
                secondary_errors.append(
                    f"{semantic_manifest.context} final close: "
                    f"{error.__class__.__name__}: {error}"
                )
    for secondary in secondary_errors:
        try:
            primary_error.add_note(f"secondary semantic lifetime failure: {secondary}")
        except BaseException:
            break
    return secondary_errors


def _validate_semantic_manifest_entry(
    entry: Any,
    *,
    index: int,
    trusted_system: bool,
    paths: set[str],
    context: str,
) -> str:
    if not isinstance(entry, dict) or set(entry) != SEMANTIC_MANIFEST_ENTRY_FIELDS:
        raise RunnerFailure(f"{context} manifest entry fields differ", exit_code=2)
    relative = entry.get("path")
    candidate = PurePosixPath(relative) if isinstance(relative, str) else None
    if (
        candidate is None
        or (
            relative != "."
            and (
                candidate.is_absolute()
                or str(candidate) != relative
                or any(part in {".", ".."} for part in candidate.parts)
            )
        )
        or relative in paths
        or (index == 0) != (relative == ".")
    ):
        raise RunnerFailure(f"{context} manifest path differs", exit_code=2)
    paths.add(relative)
    kind = entry.get("file_type")
    if kind not in {"directory", "regular", "symlink"} or any(
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
        raise RunnerFailure(f"{context} manifest metadata differs", exit_code=2)
    if kind == "directory":
        if any(
            entry.get(field) is not None
            for field in ("sha256", "symlink_target", "symlink_scope")
        ):
            raise RunnerFailure(f"{context} directory evidence differs", exit_code=2)
    elif kind == "regular":
        digest = entry.get("sha256")
        if (
            entry.get("symlink_target") is not None
            or entry.get("symlink_scope") is not None
            or (
                digest is not None
                if trusted_system
                else not isinstance(digest, str) or not SHA256_RE.fullmatch(digest)
            )
        ):
            raise RunnerFailure(
                f"{context} regular-file evidence differs", exit_code=2
            )
    elif (
        not isinstance(entry.get("symlink_target"), str)
        or not isinstance(entry.get("sha256"), str)
        or not SHA256_RE.fullmatch(entry["sha256"])
        or entry.get("symlink_scope")
        not in (
            {"within_closure", "guest_inaccessible_external"}
            if trusted_system
            else {"within_root"}
        )
    ):
        raise RunnerFailure(f"{context} symlink evidence differs", exit_code=2)
    if trusted_system and (
        entry["uid"] != 0 or (kind != "symlink" and entry["permissions"] & 0o022)
    ):
        raise RunnerFailure(f"{context} trusted-system policy differs", exit_code=2)
    return str(kind)


def _validate_recursive_semantic_manifest(
    value: Any,
    role: str,
    context: str,
    *,
    trusted_system: bool,
) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or set(value) != {"entries", "role", "schema"}
        or value.get("schema") != RECURSIVE_TREE_AUTHORITY_SCHEMA
        or value.get("role") != role
        or not isinstance(value.get("entries"), list)
        or not value["entries"]
    ):
        raise RunnerFailure(f"{context} recursive manifest fields differ", exit_code=2)
    paths: set[str] = set()
    directory_count = sum(
        _validate_semantic_manifest_entry(
            entry,
            index=index,
            trusted_system=trusted_system,
            paths=paths,
            context=context,
        )
        == "directory"
        for index, entry in enumerate(value["entries"])
    )
    if directory_count < 1:
        raise RunnerFailure(f"{context} has no directory evidence", exit_code=2)
    return value


def _retain_semantic_manifest(
    raw_path: Any,
    expected_sha256: Any,
    expected_schema: str,
    context: str,
    *,
    canonicalizer: Callable[[Any], bytes] = canonical_json_bytes,
) -> RetainedSemanticManifest:
    if not isinstance(raw_path, str) or str(Path(raw_path)) != raw_path:
        raise RunnerFailure(f"{context} evidence path is not canonical", exit_code=2)
    path = Path(raw_path)
    if not path.is_absolute():
        raise RunnerFailure(f"{context} evidence path is not absolute", exit_code=2)
    if not isinstance(expected_sha256, str) or not SHA256_RE.fullmatch(
        expected_sha256
    ):
        raise RunnerFailure(f"{context} evidence digest differs", exit_code=2)
    directories: list[int] = []
    descriptor = -1
    try:
        directories, directory_identities, descriptor = (
            _open_semantic_manifest_no_follow(path)
        )
        metadata_before = os.fstat(descriptor)
        identity = _semantic_file_identity(metadata_before)
        if (
            not stat.S_ISREG(metadata_before.st_mode)
            or stat.S_IMODE(metadata_before.st_mode) != 0o444
            or metadata_before.st_nlink != 1
        ):
            raise RunnerFailure(
                f"{context} evidence is not canonical single-link 0444",
                exit_code=2,
            )
        payload = _read_retained_file(descriptor)
        metadata_after = os.fstat(descriptor)
        if any(
            _semantic_directory_identity(os.fstat(directory)) != expected
            for directory, expected in zip(
                directories,
                directory_identities,
                strict=True,
            )
        ):
            raise OSError("semantic manifest ancestor changed during retention")
        _confirm_semantic_manifest_path(path, directory_identities, identity)
        if any(
            _semantic_directory_identity(os.fstat(directory)) != expected
            for directory, expected in zip(
                directories,
                directory_identities,
                strict=True,
            )
        ):
            raise OSError("semantic manifest ancestor changed after retention")
        value = json.loads(payload)
        if (
            _semantic_file_identity(metadata_after) != identity
            or sha256_bytes(payload) != expected_sha256
            or not isinstance(value, dict)
            or value.get("schema") != expected_schema
            or canonicalizer(value) != payload
        ):
            raise RunnerFailure(f"{context} evidence binding differs", exit_code=2)
        retained = RetainedSemanticManifest(
            context=context,
            path=path,
            sha256=expected_sha256,
            descriptor=descriptor,
            value=value,
            identity=identity,
        )
        descriptor = -1
        try:
            retained.verify()
        except BaseException:
            retained.close()
            raise
        return retained
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RunnerFailure(
            f"{context} evidence cannot be retained: {error}", exit_code=2
        ) from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        for directory in reversed(directories):
            os.close(directory)


def _semantic_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _semantic_toolchain(
    value: Any,
    context: str,
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != SEMANTIC_TOOLCHAIN_FIELDS:
        raise RunnerFailure(f"{context} toolchain fields differ", exit_code=2)
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
        raw_path = value[path_field]
        digest = value[digest_field]
        if (
            not isinstance(raw_path, str)
            or not Path(raw_path).is_absolute()
            or not isinstance(digest, str)
            or not SHA256_RE.fullmatch(digest)
        ):
            raise RunnerFailure(
                f"{context} {path_field} binding differs", exit_code=2
            )
        try:
            path = Path(raw_path).resolve(strict=True)
            metadata = path.lstat()
        except (OSError, RuntimeError) as error:
            raise RunnerFailure(
                f"{context} {path_field} cannot be resolved: {error}",
                exit_code=2,
            ) from error
        if (
            raw_path != str(path)
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o111 == 0
            or sha256(path) != digest
        ):
            raise RunnerFailure(
                f"{context} {path_field} live identity differs", exit_code=2
            )
        resolved[path_field] = path
    if (
        len(set(resolved.values())) != len(resolved)
        or len(
            {
                (path.stat().st_dev, path.stat().st_ino)
                for path in resolved.values()
            }
        )
        != len(resolved)
    ):
        raise RunnerFailure(
            f"{context} executable identities alias", exit_code=2
        )
    roots: dict[str, Path] = {}
    for field in ("cargo_home_path", "rustup_home_path"):
        raw_path = value[field]
        if not isinstance(raw_path, str) or not Path(raw_path).is_absolute():
            raise RunnerFailure(f"{context} {field} differs", exit_code=2)
        try:
            root = Path(raw_path).resolve(strict=True)
        except (OSError, RuntimeError) as error:
            raise RunnerFailure(
                f"{context} {field} cannot be resolved: {error}", exit_code=2
            ) from error
        if raw_path != str(root) or not root.is_dir():
            raise RunnerFailure(f"{context} {field} live root differs", exit_code=2)
        roots[field] = root
    rustc_host = value["rustc_host"]
    rustup_toolchain = value["rustup_toolchain"]
    host_lines = [
        line.removeprefix("host: ")
        for line in str(value["rustc_version_verbose"]).splitlines()
        if line.startswith("host: ")
    ]
    toolchain_root = (
        roots["rustup_home_path"] / "toolchains" / str(rustup_toolchain)
    )
    expected_rust_lld = (
        toolchain_root
        / "lib"
        / "rustlib"
        / str(rustc_host)
        / "bin"
        / "rust-lld"
    )
    if (
        not isinstance(rustc_host, str)
        or re.fullmatch(r"[A-Za-z0-9_-]+", rustc_host) is None
        or host_lines != [rustc_host]
        or not isinstance(rustup_toolchain, str)
        or re.fullmatch(r"[A-Za-z0-9_.-]+", rustup_toolchain) is None
        or rustup_toolchain in {".", ".."}
        or toolchain_root.resolve(strict=True) != toolchain_root
        or resolved["cargo_path"] != toolchain_root / "bin" / "cargo"
        or resolved["rustc_path"] != toolchain_root / "bin" / "rustc"
        or resolved["rust_lld_path"] != expected_rust_lld
        or any(
            not isinstance(value[field], str)
            or not value[field]
            or value[field] != value[field].strip()
            for field in ("cargo_version_verbose", "rustc_version_verbose")
        )
    ):
        raise RunnerFailure(
            f"{context} sampled Rust toolchain differs", exit_code=2
        )
    return value


def _semantic_current_file_identity(
    value: Any,
    context: str,
) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or set(value) != CURRENT_BUILD_FILE_IDENTITY_FIELDS
        or not isinstance(value.get("path"), str)
        or not Path(value["path"]).is_absolute()
        or not isinstance(value.get("sha256"), str)
        or not SHA256_RE.fullmatch(value["sha256"])
        or any(
            not _semantic_integer(value[field])
            for field in CURRENT_BUILD_FILE_IDENTITY_FIELDS
            - {"path", "sha256"}
        )
    ):
        raise RunnerFailure(f"{context} fields differ", exit_code=2)
    try:
        path = Path(value["path"]).resolve(strict=True)
        metadata = path.lstat()
    except (OSError, RuntimeError) as error:
        raise RunnerFailure(
            f"{context} cannot be resolved: {error}", exit_code=2
        ) from error
    expected = {
        "bytes": metadata.st_size,
        "ctime_ns": metadata.st_ctime_ns,
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "link_count": metadata.st_nlink,
        "mode": stat.S_IMODE(metadata.st_mode),
        "mtime_ns": metadata.st_mtime_ns,
        "path": str(path),
        "sha256": sha256(path),
        "size": metadata.st_size,
    }
    if (
        value != expected
        or value["path"] != str(path)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
    ):
        raise RunnerFailure(f"{context} live identity differs", exit_code=2)
    return value


def _semantic_path_chain(paths: list[Path]) -> list[dict[str, Any]]:
    records = []
    for path in paths:
        metadata = path.lstat()
        records.append(
            {
                "changed_ns": metadata.st_ctime_ns,
                "device": metadata.st_dev,
                "gid": metadata.st_gid,
                "inode": metadata.st_ino,
                "link_count": metadata.st_nlink,
                "mode": stat.S_IMODE(metadata.st_mode),
                "modified_ns": metadata.st_mtime_ns,
                "path": str(path),
                "size": metadata.st_size,
                "type": stat.S_IFMT(metadata.st_mode),
                "uid": metadata.st_uid,
            }
        )
    return records


def _semantic_null_device(value: Any, context: str) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or set(value) != CURRENT_BUILD_DEVICE_RECORD_FIELDS
        or value.get("trusted_system") is not True
    ):
        raise RunnerFailure(f"{context} fields differ", exit_code=2)
    identity = value.get("identity")
    path = Path("/dev/null")
    metadata = path.lstat()
    expected_identity = {
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
        not isinstance(identity, dict)
        or set(identity) != CURRENT_BUILD_DEVICE_IDENTITY_FIELDS
        or any(
            not _semantic_integer(identity[field])
            for field in CURRENT_BUILD_DEVICE_IDENTITY_FIELDS - {"path"}
        )
        or identity != expected_identity
        or not stat.S_ISCHR(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o666
        or metadata.st_nlink != 1
        or os.major(metadata.st_rdev) != 1
        or os.minor(metadata.st_rdev) != 3
        or value.get("parent_path_chain")
        != _semantic_path_chain([Path("/"), Path("/dev")])
    ):
        raise RunnerFailure(f"{context} live identity differs", exit_code=2)
    return value


def _semantic_current_tool(
    value: Any,
    expected_path: Path,
    expected_sha256: str | None,
    context: str,
    *,
    trusted: bool,
) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or set(value) != CURRENT_BUILD_TOOL_RECORD_FIELDS
        or value.get("trusted_system") is not trusted
    ):
        raise RunnerFailure(f"{context} fields differ", exit_code=2)
    identity = _semantic_current_file_identity(value.get("identity"), context)
    if (
        identity["path"] != str(expected_path)
        or (expected_sha256 is not None and identity["sha256"] != expected_sha256)
        or identity["mode"] & 0o111 == 0
    ):
        raise RunnerFailure(f"{context} binding differs", exit_code=2)
    chain = value.get("path_chain")
    if not trusted:
        if chain is not None:
            raise RunnerFailure(
                f"{context} unexpected trusted chain", exit_code=2
            )
        return value
    paths = [Path("/"), *list(expected_path.parents)[::-1][1:], expected_path]
    expected_chain = _semantic_path_chain(paths)
    if (
        chain != expected_chain
        or any(
            item["uid"] != 0 or item["mode"] & 0o022
            for item in expected_chain
        )
    ):
        raise RunnerFailure(f"{context} trusted chain differs", exit_code=2)
    return value


def _semantic_current_directory(
    value: Any,
    expected_path: Path,
    context: str,
) -> dict[str, Any]:
    metadata = expected_path.lstat()
    expected = {
        "changed_ns": metadata.st_ctime_ns,
        "device": metadata.st_dev,
        "file_type": stat.S_IFMT(metadata.st_mode),
        "inode": metadata.st_ino,
        "link_count": metadata.st_nlink,
        "modified_ns": metadata.st_mtime_ns,
        "path": str(expected_path),
        "permissions": stat.S_IMODE(metadata.st_mode),
        "size": metadata.st_size,
    }
    if (
        not isinstance(value, dict)
        or set(value) != CURRENT_BUILD_DIRECTORY_IDENTITY_FIELDS
        or any(
            not _semantic_integer(value[field])
            for field in CURRENT_BUILD_DIRECTORY_IDENTITY_FIELDS - {"path"}
        )
        or value != expected
        or not stat.S_ISDIR(metadata.st_mode)
    ):
        raise RunnerFailure(f"{context} live identity differs", exit_code=2)
    return value


def _semantic_current_execution_tools(
    value: Any,
    toolchain: dict[str, Any],
    context: str,
    canonicalizer: Callable[[Any], bytes],
) -> str:
    expected_names = {
        "bwrap",
        "cargo",
        "dev_null",
        "python",
        "rustc",
        "rust_lld",
        "toolchain_root",
    }
    if not isinstance(value, dict) or set(value) != expected_names:
        raise RunnerFailure(f"{context} fields differ", exit_code=2)
    for name, trusted in (
        ("bwrap", True),
        ("cargo", False),
        ("rustc", False),
        ("rust_lld", False),
    ):
        _semantic_current_tool(
            value[name],
            Path(toolchain[f"{name}_path"]),
            toolchain[f"{name}_sha256"],
            f"{context} {name}",
            trusted=trusted,
        )
    _semantic_current_tool(
        value["python"],
        Path("/usr/bin/python3").resolve(strict=True),
        None,
        f"{context} python",
        trusted=True,
    )
    _semantic_null_device(value["dev_null"], f"{context} dev-null")
    _semantic_current_directory(
        value["toolchain_root"],
        Path(toolchain["cargo_path"]).parent.parent,
        f"{context} toolchain root",
    )
    return sha256_bytes(canonicalizer(value))


def _semantic_prepared_execution_tools(
    value: Any,
    toolchain: dict[str, Any],
    context: str,
    canonicalizer: Callable[[Any], bytes],
) -> str:
    expected_names = {
        "bwrap",
        "cargo",
        "dev_null",
        "rustc",
        "rust_lld",
        "toolchain_root",
    }
    if not isinstance(value, dict) or set(value) != expected_names:
        raise RunnerFailure(f"{context} fields differ", exit_code=2)
    for name in ("bwrap", "cargo", "rustc", "rust_lld"):
        binding = value[name]
        expected_path = Path(toolchain[f"{name}_path"])
        metadata = expected_path.lstat()
        expected_identity = {
            "changed_ns": metadata.st_ctime_ns,
            "device": metadata.st_dev,
            "inode": metadata.st_ino,
            "link_count": metadata.st_nlink,
            "modified_ns": metadata.st_mtime_ns,
        }
        if (
            not isinstance(binding, dict)
            or set(binding) != PREPARED_EXECUTION_FILE_FIELDS
            or not isinstance(binding.get("identity"), dict)
            or set(binding["identity"])
            != PREPARED_EXECUTION_FILE_IDENTITY_FIELDS
            or any(
                not _semantic_integer(binding["identity"][field])
                for field in PREPARED_EXECUTION_FILE_IDENTITY_FIELDS
            )
            or any(
                not _semantic_integer(binding.get(field))
                for field in ("mode", "size")
            )
            or binding
            != {
                "identity": expected_identity,
                "mode": stat.S_IMODE(metadata.st_mode),
                "path": str(expected_path),
                "sha256": sha256(expected_path),
                "size": metadata.st_size,
            }
            or binding["sha256"] != toolchain[f"{name}_sha256"]
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o111 == 0
        ):
            raise RunnerFailure(
                f"{context} {name} live binding differs", exit_code=2
            )
    _semantic_null_device(value["dev_null"], f"{context} dev-null")
    root = Path(toolchain["cargo_path"]).parent.parent
    metadata = root.lstat()
    expected_root = {
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "link_count": metadata.st_nlink,
        "mode": stat.S_IMODE(metadata.st_mode),
    }
    if (
        not isinstance(value["toolchain_root"], dict)
        or set(value["toolchain_root"]) != PREPARED_TOOLCHAIN_ROOT_FIELDS
        or any(
            not _semantic_integer(value["toolchain_root"][field])
            for field in PREPARED_TOOLCHAIN_ROOT_FIELDS
        )
        or value["toolchain_root"] != expected_root
        or not stat.S_ISDIR(metadata.st_mode)
    ):
        raise RunnerFailure(
            f"{context} toolchain root live identity differs", exit_code=2
        )
    return sha256_bytes(canonicalizer(value))


def _semantic_prepared_build_child(value: Any, context: str) -> None:
    passed = (
        value.get("passed_file_descriptors")
        if isinstance(value, dict)
        else None
    )
    if not _semantic_integer(passed) or passed != 16:
        raise RunnerFailure(
            f"{context} exact inherited-FD count differs", exit_code=2
        )


def _semantic_rust_lld_guest(toolchain: dict[str, Any]) -> str:
    return (
        f"/asterism/toolchain/lib/rustlib/{toolchain['rustc_host']}"
        "/bin/gcc-ld/ld.lld"
    )


def _release_build_destinations(toolchain: dict[str, Any]) -> set[str]:
    return {
        *RELEASE_BUILD_DESTINATIONS,
        _semantic_rust_lld_guest(toolchain),
    }


def _release_build_descriptor_bindings(
    toolchain: dict[str, Any],
) -> tuple[tuple[str, str], ...]:
    return (
        *RELEASE_BUILD_BASE_DESCRIPTOR_BINDINGS,
        ("--ro-bind-fd", _semantic_rust_lld_guest(toolchain)),
        *(("--ro-bind-fd", path) for path in SEMANTIC_CONFIG_DESTINATIONS),
    )


def _current_build_expected_passed_file_descriptors(
    build: Any, context: str
) -> int:
    """Reproduce current/build_children.py's complete inherited-FD count."""

    if not isinstance(build, dict):
        raise RunnerFailure(f"{context} build is absent", exit_code=2)
    cargo_config = build.get("cargo_config_prebuild")
    if (
        not isinstance(cargo_config, dict)
        or set(cargo_config) != CURRENT_BUILD_CARGO_CONFIG_FIELDS
        or cargo_config.get("schema") != CURRENT_BUILD_CARGO_CONFIG_SCHEMA
    ):
        raise RunnerFailure(
            f"{context} Cargo config authority differs", exit_code=2
        )
    cargo_search = cargo_config.get("cargo_search")
    cargo_home_tree = cargo_config.get("cargo_home_tree")
    preserved = cargo_config.get("preserved_top_level_entries")
    if (
        not isinstance(cargo_search, dict)
        or set(cargo_search) != CURRENT_BUILD_CARGO_SEARCH_FIELDS
        or not isinstance(cargo_search.get("entries"), list)
        or len(cargo_search["entries"]) != 8
        or not isinstance(cargo_home_tree, dict)
        or set(cargo_home_tree) != CURRENT_BUILD_CARGO_HOME_TREE_FIELDS
        or not isinstance(preserved, dict)
        or set(preserved) != {"cargo-home", "source"}
    ):
        raise RunnerFailure(
            f"{context} Cargo config record shape differs", exit_code=2
        )

    for origin in ("cargo-home", "source"):
        entries = preserved[origin]
        if not isinstance(entries, list):
            raise RunnerFailure(
                f"{context} preserved {origin} entries differ", exit_code=2
            )
        names: list[str] = []
        for entry in entries:
            if (
                not isinstance(entry, dict)
                or set(entry) != CURRENT_BUILD_PRESERVED_ENTRY_FIELDS
                or not isinstance(entry.get("identity"), dict)
                or not isinstance(entry.get("name"), str)
                or entry.get("type") not in {"directory", "regular"}
            ):
                raise RunnerFailure(
                    f"{context} preserved {origin} entry differs", exit_code=2
                )
            name = entry["name"]
            selected = PurePosixPath(name)
            if (
                not name
                or len(selected.parts) != 1
                or name in {".", "..", "config", "config.toml"}
            ):
                raise RunnerFailure(
                    f"{context} preserved {origin} name differs", exit_code=2
                )
            identity_fields = (
                CURRENT_BUILD_PRESERVED_DIRECTORY_IDENTITY_FIELDS
                if entry["type"] == "directory"
                else CURRENT_BUILD_PRESERVED_REGULAR_IDENTITY_FIELDS
            )
            if set(entry["identity"]) != identity_fields:
                raise RunnerFailure(
                    f"{context} preserved {origin} identity differs", exit_code=2
                )
            names.append(name)
        if names != sorted(names) or len(names) != len(set(names)):
            raise RunnerFailure(
                f"{context} preserved {origin} order differs", exit_code=2
            )

    argv = build.get("argv")
    if not isinstance(argv, list) or not all(
        isinstance(argument, str) for argument in argv
    ):
        raise RunnerFailure(f"{context} sandbox argv differs", exit_code=2)
    argv_descriptor_bindings = sum(
        argument in SEMANTIC_DESCRIPTOR_OPTIONS for argument in argv
    )
    dev_null_bindings = sum(argument == "--dev-bind" for argument in argv)
    if dev_null_bindings != 1:
        raise RunnerFailure(
            f"{context} exact null-device binding differs", exit_code=2
        )
    cargo_home_entries = preserved["cargo-home"]
    # CargoConfigSearchGuard retains the unbound source .cargo root, the direct
    # Cargo-home overlay source, and every preserved Cargo-home child.
    # run_capture then adds the separately retained bwrap execution lease. This
    # preserves the intentional 18/20 release/child base plus one descriptor for
    # every additional retained Cargo-home child.
    return (
        argv_descriptor_bindings
        + dev_null_bindings
        + 3
        + len(cargo_home_entries)
    )


def _validate_semantic_sandbox(
    argv: Any,
    environment: Any,
    context: str,
    *,
    passed_file_descriptors: Any,
    expected_passed_file_descriptors: int | None,
    require_loader_origin: bool,
    require_passed_file_descriptors: bool = True,
    expected_fd_destinations: set[str],
    writable_fd_destinations: set[str],
    expected_data_destinations: set[str] | None = None,
    expected_descriptor_bindings: tuple[tuple[str, str], ...] | None = None,
    execution_tools: Any = None,
    execution_tools_kind: str | None = None,
    toolchain: dict[str, Any] | None = None,
    cargo_config_search_sha256: Any = None,
    semantic_runtime_sha256: Any = None,
    prepared_authority_canonicalizer: Callable[[Any], bytes] | None = None,
) -> str:
    if expected_data_destinations is None:
        expected_data_destinations = set()
    require_cargo_runtime = execution_tools_kind is not None
    if execution_tools_kind not in {None, "current", "prepared"}:
        raise RunnerFailure(
            f"{context} execution-tools kind differs", exit_code=2
        )
    if (
        not isinstance(argv, list)
        or not argv
        or not all(isinstance(argument, str) for argument in argv)
        or not isinstance(environment, dict)
        or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in environment.items()
        )
        or environment.get("RUSTUP_HOME") != "/nonexistent"
        or ("LD_ORIGIN_PATH" in environment) != require_loader_origin
        or (
            require_loader_origin
            and environment.get("LD_ORIGIN_PATH") != SEMANTIC_TOOLCHAIN_BIN
        )
        or (
            require_cargo_runtime
            and (
                toolchain is None
                or argv[0] != toolchain.get("bwrap_path")
            )
        )
    ):
        raise RunnerFailure(f"{context} sandbox environment differs", exit_code=2)
    forbidden_options = {"--dev", "--proc"}
    if forbidden_options & set(argv) or (
        not require_cargo_runtime and "--dev-bind" in argv
    ):
        raise RunnerFailure(f"{context} exposes legacy dev/proc", exit_code=2)
    triples = list(zip(argv, argv[1:], argv[2:]))
    if (
        sum(left == "--dir" and right == "/dev" for left, right in zip(argv, argv[1:]))
        != 1
        or sum(
            left == "--dir" and right == "/proc"
            for left, right in zip(argv, argv[1:])
        )
        != 1
        or any(
            option in {"--ro-bind", "--bind"} and source == "/" and target == "/"
            for option, source, target in triples
        )
        or any(
            argument in {"/dev", "/proc"}
            and (index == 0 or argv[index - 1] != "--dir")
            for index, argument in enumerate(argv)
        )
        or any(
            (
                argument.startswith("/dev/")
                and (
                    not require_cargo_runtime
                    or argument != "/dev/null"
                    or index < 2
                    or argv[index - 2] != "--dev-bind"
                )
            )
            or (
                argument.startswith("/proc/")
                and (
                    index == 0
                    or not argument.startswith(SEMANTIC_CARGO_HOME_FD_PREFIX)
                    or argv[index - 1] not in {"--overlay-src", "--dev-bind"}
                )
            )
            for index, argument in enumerate(argv)
        )
        or "/asterism/rustup-home" in argv
    ):
        raise RunnerFailure(f"{context} sandbox mount authority differs", exit_code=2)
    descriptor_bindings = []
    normalized = list(argv)
    dev_bind_indexes = [
        index for index, argument in enumerate(argv) if argument == "--dev-bind"
    ]
    dev_null_descriptor: int | None = None
    if require_cargo_runtime:
        if len(dev_bind_indexes) != 1:
            raise RunnerFailure(
                f"{context} null-device binding differs", exit_code=2
            )
        dev_index = dev_bind_indexes[0]
        segment = argv[dev_index : dev_index + 3]
        source = segment[1] if len(segment) == 3 else ""
        descriptor_text = (
            source.removeprefix(SEMANTIC_CARGO_HOME_FD_PREFIX)
            if source.startswith(SEMANTIC_CARGO_HOME_FD_PREFIX)
            else ""
        )
        if (
            len(segment) != 3
            or segment[2] != "/dev/null"
            or not descriptor_text.isascii()
            or not descriptor_text.isdecimal()
            or len(descriptor_text) > 10
            or str(int(descriptor_text)) != descriptor_text
            or int(descriptor_text) < 3
        ):
            raise RunnerFailure(
                f"{context} null-device binding differs", exit_code=2
            )
        dev_null_descriptor = int(descriptor_text)
        normalized[dev_index + 1] = "$FD:/dev/null"
        assert toolchain is not None
        fixed_prefix = [
            toolchain["bwrap_path"],
            "--die-with-parent",
            "--new-session",
            "--unshare-net",
            "--dir",
            "/usr",
        ]
        system_offset = len(fixed_prefix)
        for _host, guest in TRUSTED_SYSTEM_MOUNTS:
            segment = argv[system_offset : system_offset + 3]
            if (
                len(segment) != 3
                or segment[0] != "--ro-bind-fd"
                or segment[2] != guest
            ):
                raise RunnerFailure(
                    f"{context} trusted-system binding order differs",
                    exit_code=2,
                )
            system_offset += 3
        private_before_dev = [
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
        ]
        private_after_dev = [
            "--dir",
            "/proc",
            "--tmpfs",
            "/tmp",
            "--tmpfs",
            "/asterism",
        ]
        if (
            argv[: len(fixed_prefix)] != fixed_prefix
            or argv[
                system_offset : system_offset + len(private_before_dev)
            ]
            != private_before_dev
            or dev_index != system_offset + len(private_before_dev)
            or argv[dev_index + 3 : dev_index + 3 + len(private_after_dev)]
            != private_after_dev
        ):
            raise RunnerFailure(
                f"{context} null-device position differs", exit_code=2
            )
    elif dev_bind_indexes:
        raise RunnerFailure(
            f"{context} resolver gained a device binding", exit_code=2
        )
    for index, option in enumerate(argv):
        if option not in SEMANTIC_DESCRIPTOR_OPTIONS:
            continue
        if index + 2 >= len(argv) or not argv[index + 1].isdecimal():
            raise RunnerFailure(f"{context} sandbox descriptor differs", exit_code=2)
        descriptor = int(argv[index + 1])
        if descriptor < 3 or str(descriptor) != argv[index + 1]:
            raise RunnerFailure(f"{context} sandbox descriptor differs", exit_code=2)
        destination = argv[index + 2]
        descriptor_bindings.append((descriptor, destination, option, index))
        normalized[index + 1] = f"$FD:{destination}"
    overlay_indexes = [
        index for index, argument in enumerate(argv) if argument == "--overlay-src"
    ]
    if (
        len(overlay_indexes) != 1
        or argv.count("--tmp-overlay") != 1
        or "/asterism/cargo-home-lower" in argv
    ):
        raise RunnerFailure(
            f"{context} sandbox Cargo-home overlay differs", exit_code=2
        )
    overlay_index = overlay_indexes[0]
    overlay_source = (
        argv[overlay_index + 1] if overlay_index + 1 < len(argv) else ""
    )
    overlay_descriptor_text = (
        overlay_source.removeprefix(SEMANTIC_CARGO_HOME_FD_PREFIX)
        if overlay_source.startswith(SEMANTIC_CARGO_HOME_FD_PREFIX)
        else ""
    )
    if (
        overlay_index + 3 >= len(argv)
        or argv[overlay_index + 2 : overlay_index + 4]
        != ["--tmp-overlay", SEMANTIC_CARGO_HOME]
        or not overlay_descriptor_text.isascii()
        or not overlay_descriptor_text.isdecimal()
        or len(overlay_descriptor_text) > 10
        or str(int(overlay_descriptor_text)) != overlay_descriptor_text
        or int(overlay_descriptor_text) < 3
    ):
        raise RunnerFailure(
            f"{context} sandbox Cargo-home overlay differs", exit_code=2
        )
    overlay_descriptor = int(overlay_descriptor_text)
    normalized[overlay_index + 1] = "$FD:cargo-home-overlay"
    expected_descriptors = len(expected_fd_destinations) + len(
        expected_data_destinations
    )
    observed_descriptor_bindings = tuple(
        (option, path)
        for _descriptor, path, option, _index in descriptor_bindings
    )
    all_descriptors = [
        *(descriptor for descriptor, _path, _option, _index in descriptor_bindings),
        overlay_descriptor,
        *((dev_null_descriptor,) if dev_null_descriptor is not None else ()),
    ]
    cargo_home_config_indexes = [
        index
        for _descriptor, path, _option, index in descriptor_bindings
        if path.startswith(f"{SEMANTIC_CARGO_HOME}/")
    ]
    semantic_config_indexes = [
        index
        for _descriptor, path, _option, index in descriptor_bindings
        if path in SEMANTIC_CONFIG_DESTINATIONS
    ]
    overlay_anchor_path = (
        _semantic_rust_lld_guest(toolchain)
        if require_cargo_runtime and toolchain is not None
        else "/asterism/toolchain/bin/rustc"
    )
    overlay_anchor_indexes = [
        index
        for _descriptor, path, option, index in descriptor_bindings
        if path == overlay_anchor_path and option == "--ro-bind-fd"
    ]
    cargo_home_remount_indexes = [
        index
        for index in range(len(argv) - 1)
        if argv[index : index + 2] == ["--remount-ro", SEMANTIC_CARGO_HOME]
    ]
    cargo_child_indexes = [
        index
        for index, argument in enumerate(argv)
        if argument == "/asterism/toolchain/bin/cargo"
        and not any(
            binding_index + 2 == index
            for _descriptor, _path, _option, binding_index in descriptor_bindings
        )
    ]
    passed_topology_differs = False
    if require_passed_file_descriptors:
        passed_topology_differs = (
            expected_passed_file_descriptors is None
            or not _semantic_integer(passed_file_descriptors)
            or passed_file_descriptors != expected_passed_file_descriptors
        )
    if (
        len(descriptor_bindings) != expected_descriptors
        or len(all_descriptors)
        != expected_descriptors + 1 + int(require_cargo_runtime)
        or len(set(all_descriptors))
        != expected_descriptors + 1 + int(require_cargo_runtime)
        or len(
            {
                path
                for _descriptor, path, _option, _index in descriptor_bindings
            }
        )
        != expected_descriptors
        or {
            path
            for _descriptor, path, option, _index in descriptor_bindings
            if option in {"--ro-bind-fd", "--bind-fd"}
        }
        != expected_fd_destinations
        or {
            path
            for _descriptor, path, option, _index in descriptor_bindings
            if option in {"--ro-bind-data", "--bind-data"}
        }
        != expected_data_destinations
        or {
            path
            for _descriptor, path, option, _index in descriptor_bindings
            if option == "--bind-fd"
        }
        != writable_fd_destinations
        or any(
            option == "--bind-data"
            for _descriptor, _path, option, _index in descriptor_bindings
        )
        or (
            expected_descriptor_bindings is not None
            and observed_descriptor_bindings != expected_descriptor_bindings
        )
        or (
            expected_descriptor_bindings is not None
            and (
                len(overlay_anchor_indexes) != 1
                or overlay_index != overlay_anchor_indexes[0] + 3
                or any(index < overlay_index + 4 for index in semantic_config_indexes)
            )
        )
        or any(index < overlay_index + 4 for index in cargo_home_config_indexes)
        or len(cargo_home_remount_indexes) != 1
        or cargo_home_remount_indexes[0]
        <= max(cargo_home_config_indexes, default=overlay_index + 3)
        or len(cargo_child_indexes) != 1
        or cargo_child_indexes[0] <= cargo_home_remount_indexes[0] + 1
        or passed_topology_differs
    ):
        raise RunnerFailure(f"{context} sandbox descriptor topology differs", exit_code=2)
    if require_cargo_runtime and not callable(
        prepared_authority_canonicalizer
    ):
        raise RunnerFailure(
            f"{context} prepared canonicalizer is absent", exit_code=2
        )
    if execution_tools_kind == "current":
        if toolchain is None:
            raise RunnerFailure(f"{context} toolchain is absent", exit_code=2)
        execution_tools_sha256 = _semantic_current_execution_tools(
            execution_tools,
            toolchain,
            f"{context} execution tools",
            prepared_authority_canonicalizer,
        )
    elif execution_tools_kind == "prepared":
        if toolchain is None:
            raise RunnerFailure(f"{context} toolchain is absent", exit_code=2)
        execution_tools_sha256 = _semantic_prepared_execution_tools(
            execution_tools,
            toolchain,
            f"{context} execution tools",
            prepared_authority_canonicalizer,
        )
    else:
        execution_tools_sha256 = None
    if require_cargo_runtime and (
        not isinstance(cargo_config_search_sha256, str)
        or not SHA256_RE.fullmatch(cargo_config_search_sha256)
        or not isinstance(semantic_runtime_sha256, str)
        or not SHA256_RE.fullmatch(semantic_runtime_sha256)
    ):
        raise RunnerFailure(
            f"{context} sandbox hash inputs differ", exit_code=2
        )
    return sha256_bytes(
        (
            prepared_authority_canonicalizer
            if callable(prepared_authority_canonicalizer)
            else canonical_json_bytes
        )(
            {
                "argv": normalized,
                "cargo_config_search_sha256": cargo_config_search_sha256,
                "execution_tools_sha256": execution_tools_sha256,
                "semantic_runtime_sha256": semantic_runtime_sha256,
            }
        )
    )


def _semantic_authority_bindings(
    value: Any,
    context: str,
    *,
    source_role: str,
) -> list[tuple[str, str, str, str, str, dict[str, Any]]]:
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "cargo_home",
            "runtime_sha256",
            "schema",
            "source",
            "toolchain",
            "trusted_system_closure",
        }
        or value.get("schema") != SEMANTIC_INPUT_AUTHORITY_SCHEMA
    ):
        raise RunnerFailure(f"{context} semantic authority fields differ", exit_code=2)
    bindings: list[tuple[str, str, str, str, str, dict[str, Any]]] = []
    for name, role in (
        ("source", source_role),
        ("toolchain", "toolchain"),
        ("cargo_home", "cargo_home"),
    ):
        binding = value.get(name)
        if (
            not isinstance(binding, dict)
            or set(binding) != SEMANTIC_TREE_BINDING_FIELDS
            or binding.get("schema") != RECURSIVE_TREE_AUTHORITY_SCHEMA
            or binding.get("role") != role
            or not isinstance(binding.get("entry_count"), int)
            or isinstance(binding.get("entry_count"), bool)
            or binding["entry_count"] < 1
            or not isinstance(binding.get("watch_count"), int)
            or isinstance(binding.get("watch_count"), bool)
            or binding["watch_count"] < 1
            or binding.get("equal_pre_post") is not True
            or binding.get("mutation_events_absent") is not True
        ):
            raise RunnerFailure(
                f"{context} semantic {name} binding differs", exit_code=2
            )
        bindings.append(
            (
                str(binding.get("manifest_path")),
                str(binding.get("manifest_sha256")),
                RECURSIVE_TREE_AUTHORITY_SCHEMA,
                role,
                name,
                binding,
            )
        )
    closure = value.get("trusted_system_closure")
    mounts = closure.get("mounts") if isinstance(closure, dict) else None
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
    if (
        not isinstance(closure, dict)
        or set(closure) != SEMANTIC_CLOSURE_BINDING_FIELDS
        or closure.get("schema") != TRUSTED_SYSTEM_CLOSURE_SCHEMA
        or not isinstance(closure.get("entry_count"), int)
        or isinstance(closure.get("entry_count"), bool)
        or closure["entry_count"] < 3
        or not isinstance(closure.get("watch_count"), int)
        or isinstance(closure.get("watch_count"), bool)
        or closure["watch_count"] < 3
        or closure.get("mutation_events_absent") is not True
        or not isinstance(mounts, list)
        or len(mounts) != len(TRUSTED_SYSTEM_MOUNTS)
        or any(
            not isinstance(mount, dict)
            or set(mount) != mount_fields
            or mount.get("host_path") != host
            or mount.get("resolved_path") != host
            or mount.get("guest_path") != guest
            or mount.get("trusted_root_owned_non_writable") is not True
            or mount.get("uid") != 0
            or not all(
                isinstance(mount.get(field), int)
                and not isinstance(mount.get(field), bool)
                for field in ("device", "gid", "inode", "permissions", "uid")
            )
            or mount.get("permissions", 0) & 0o022
            for mount, (host, guest) in zip(mounts or [], TRUSTED_SYSTEM_MOUNTS, strict=True)
        )
    ):
        raise RunnerFailure(f"{context} trusted-system closure differs", exit_code=2)
    bindings.append(
        (
            str(closure.get("manifest_path")),
            str(closure.get("sha256")),
            TRUSTED_SYSTEM_CLOSURE_SCHEMA,
            "trusted_system_closure",
            "trusted_system_closure",
            closure,
        )
    )
    runtime_sha256 = value.get("runtime_sha256")
    if (
        not isinstance(runtime_sha256, str)
        or not SHA256_RE.fullmatch(runtime_sha256)
        or runtime_sha256 != _semantic_runtime_sha256(value)
    ):
        raise RunnerFailure(f"{context} semantic runtime digest differs", exit_code=2)
    return bindings


def _claim_semantic_physical_identity(
    seen: dict[tuple[int, int], str],
    snapshot: RetainedSemanticManifest,
) -> None:
    physical_identity = (
        snapshot.identity["device"],
        snapshot.identity["inode"],
    )
    prior_context = seen.get(physical_identity)
    if prior_context is not None:
        raise RunnerFailure(
            f"semantic manifest physical alias: {prior_context} and "
            f"{snapshot.context}",
            exit_code=2,
        )
    seen[physical_identity] = snapshot.context


def retain_prepared_semantic_manifests(
    prepared: Prepared,
) -> tuple[list[RetainedSemanticManifest], dict[str, int]]:
    """Project the exact 12 + 20 + 16 semantic evidence topology."""

    toolchain = _semantic_toolchain(
        prepared.value.get("toolchain"),
        "prepared release",
    )
    prepared_canonicalizer = getattr(
        prepared.schema,
        "prepared_authority_canonical_json_bytes",
        None,
    )
    if not callable(prepared_canonicalizer):
        raise RunnerFailure(
            "prepared authority canonicalizer is absent", exit_code=2
        )
    records: list[
        tuple[str, str, str, str, str, str, dict[str, Any]]
    ] = []
    runtime_component_names = (
        "cargo_home",
        "toolchain",
        "trusted_system_closure",
    )
    runtime_content_hasher = getattr(
        prepared.schema, "semantic_runtime_content_sha256", None
    )
    if not callable(runtime_content_hasher):
        raise RunnerFailure(
            "shared schema omits normalized semantic runtime hasher", exit_code=2
        )
    runtime_authorities: list[tuple[dict[str, Any], str]] = []
    runtime_manifest_bindings: dict[str, tuple[int, str]] = {}
    runtime_digests: set[str] = set()

    def add(
        section: str,
        authority: Any,
        context: str,
        *,
        source_role: str = "source",
    ) -> None:
        authority_bindings = _semantic_authority_bindings(
            authority, context, source_role=source_role
        )
        authority_index = len(runtime_authorities)
        runtime_authorities.append((authority, context))
        for path, digest, schema, role, component, binding in authority_bindings:
            records.append(
                (
                    section,
                    path,
                    digest,
                    schema,
                    role,
                    f"{context} {component}",
                    binding,
                )
            )
            if component in runtime_component_names:
                runtime_manifest_bindings[path] = (authority_index, component)

    current_file = prepared.source_review_files.get("current_children_attestation")
    if current_file is None:
        raise RunnerFailure("current-child semantic authority is absent", exit_code=2)
    current = _load_canonical_support_authority(
        current_file,
        "current-child semantic authority",
    )
    current_builds = current.get("builds")
    if (
        current.get("schema") != CURRENT_CHILDREN_ATTESTATION_SCHEMA
        or not isinstance(current_builds, dict)
        or set(current_builds) != {"children", "hooked_release", "pristine_release"}
        or current.get("toolchain") != toolchain
    ):
        raise RunnerFailure("current-child semantic topology differs", exit_code=2)
    identities = current.get("toolchain_identities")
    identity_names = ("bwrap", "cargo", "git", "rustc", "rust_lld", "rustup")
    if not isinstance(identities, list) or len(identities) != len(identity_names):
        raise RunnerFailure(
            "current-child exact toolchain identities differ", exit_code=2
        )
    for raw, identity_name in zip(identities, identity_names, strict=True):
        identity = _semantic_current_file_identity(
            raw,
            f"current-child toolchain identity {identity_name}",
        )
        if (
            identity["path"] != toolchain[f"{identity_name}_path"]
            or identity["sha256"] != toolchain[f"{identity_name}_sha256"]
        ):
            raise RunnerFailure(
                "current-child toolchain identity crosslink differs",
                exit_code=2,
            )
    current_build_destinations = {
        *CURRENT_BUILD_FD_DESTINATIONS,
        _semantic_rust_lld_guest(toolchain),
    }
    for name in ("children", "hooked_release", "pristine_release"):
        build = current_builds[name]
        if not isinstance(build, dict):
            raise RunnerFailure(f"current-child {name} build is absent", exit_code=2)
        execution = build.get("execution")
        passed = (
            execution.get("passed_file_descriptors")
            if isinstance(execution, dict)
            else None
        )
        expected_passed = _current_build_expected_passed_file_descriptors(
            build, f"current-child {name}"
        )
        current_authority = build.get("semantic_input_authority")
        _validate_semantic_sandbox(
            build.get("argv"),
            build.get("environment"),
            f"current-child {name}",
            passed_file_descriptors=passed,
            expected_passed_file_descriptors=expected_passed,
            require_loader_origin=True,
            expected_fd_destinations=(
                current_build_destinations
                | {"/asterism/rustc_workspace_wrapper.py", "/asterism/receipt"}
                if name == "children"
                else current_build_destinations
            ),
            writable_fd_destinations=(
                {"/asterism/target", "/asterism/receipt"}
                if name == "children"
                else {"/asterism/target"}
            ),
            expected_data_destinations=set(SEMANTIC_CONFIG_DESTINATIONS),
            execution_tools=build.get("execution_tools"),
            execution_tools_kind="current",
            toolchain=toolchain,
            cargo_config_search_sha256=sha256_bytes(
                canonical_json_bytes(build["cargo_config_prebuild"])
            ),
            semantic_runtime_sha256=(
                current_authority.get("runtime_sha256")
                if isinstance(current_authority, dict)
                else None
            ),
            prepared_authority_canonicalizer=prepared_canonicalizer,
        )
        add("current", current_authority, f"current-child {name}")

    prepared_variants = prepared.value.get("variants")
    if not isinstance(prepared_variants, dict) or set(prepared_variants) != set(VARIANTS):
        raise RunnerFailure("prepared release semantic topology differs", exit_code=2)
    release_authorities: dict[str, dict[str, Any]] = {}
    release_sandbox_sha256s: dict[str, str] = {}
    for name in VARIANTS:
        item = prepared_variants[name]
        attestation = item.get("attestation") if isinstance(item, dict) else None
        authority = (
            attestation.get("semantic_input_authority")
            if isinstance(attestation, dict)
            else None
        )
        if not isinstance(attestation, dict) or attestation.get("toolchain") != toolchain:
            raise RunnerFailure(
                f"release {name} toolchain authority differs", exit_code=2
            )
        _semantic_prepared_build_child(
            attestation.get("build_child"),
            f"release {name} build child",
        )
        release_sandbox_sha256s[name] = _validate_semantic_sandbox(
            attestation.get("build_argv") if isinstance(attestation, dict) else None,
            attestation.get("build_env") if isinstance(attestation, dict) else None,
            f"release {name}",
            passed_file_descriptors=None,
            expected_passed_file_descriptors=None,
            require_loader_origin=True,
            require_passed_file_descriptors=False,
            expected_fd_destinations=_release_build_destinations(toolchain),
            writable_fd_destinations={"/asterism/target"},
            expected_descriptor_bindings=_release_build_descriptor_bindings(
                toolchain
            ),
            execution_tools=attestation.get("execution_tools"),
            execution_tools_kind="prepared",
            toolchain=toolchain,
            cargo_config_search_sha256=attestation.get(
                "cargo_config_search", {}
            ).get("sha256"),
            semantic_runtime_sha256=authority.get("runtime_sha256")
            if isinstance(authority, dict)
            else None,
            prepared_authority_canonicalizer=prepared_canonicalizer,
        )
        if not isinstance(authority, dict):
            raise RunnerFailure(f"release {name} semantic authority is absent", exit_code=2)
        release_authorities[name] = authority
        add("release", authority, f"release {name}")
    proof_builds = prepared.release_compile_out_value.get("builds")
    if not isinstance(proof_builds, dict) or set(proof_builds) != {"ordinary_a", "overlay_a"}:
        raise RunnerFailure("release proof semantic topology differs", exit_code=2)
    ordinary = proof_builds["ordinary_a"]
    overlay = proof_builds["overlay_a"]
    ordinary_attestation = ordinary.get("attestation") if isinstance(ordinary, dict) else None
    overlay_attestation = overlay.get("attestation") if isinstance(overlay, dict) else None
    if (
        not isinstance(ordinary_attestation, dict)
        or ordinary_attestation.get("semantic_input_authority")
        != release_authorities["A"]
        or not isinstance(overlay_attestation, dict)
    ):
        raise RunnerFailure("release ordinary-A semantic alias differs", exit_code=2)
    if overlay_attestation.get("toolchain") != toolchain:
        raise RunnerFailure(
            "release overlay A toolchain authority differs", exit_code=2
        )
    _semantic_prepared_build_child(
        overlay_attestation.get("build_child"),
        "release overlay A build child",
    )
    overlay_sandbox_sha256 = _validate_semantic_sandbox(
        overlay_attestation.get("build_argv"),
        overlay_attestation.get("build_env"),
        "release overlay A",
        passed_file_descriptors=None,
        expected_passed_file_descriptors=None,
        require_loader_origin=True,
        require_passed_file_descriptors=False,
        expected_fd_destinations=_release_build_destinations(toolchain),
        writable_fd_destinations={"/asterism/target"},
        expected_descriptor_bindings=_release_build_descriptor_bindings(
            toolchain
        ),
        execution_tools=overlay_attestation.get("execution_tools"),
        execution_tools_kind="prepared",
        toolchain=toolchain,
        cargo_config_search_sha256=overlay_attestation.get(
            "cargo_config_search", {}
        ).get("sha256"),
        semantic_runtime_sha256=overlay_attestation.get(
            "semantic_input_authority", {}
        ).get("runtime_sha256"),
        prepared_authority_canonicalizer=prepared_canonicalizer,
    )
    ordinary_sandbox_sha256 = ordinary.get("sandbox_sha256")
    reviewed_overlay_sandbox_sha256 = overlay.get("sandbox_sha256")
    if (
        not isinstance(ordinary_sandbox_sha256, str)
        or not SHA256_RE.fullmatch(ordinary_sandbox_sha256)
        or ordinary_sandbox_sha256 != release_sandbox_sha256s["A"]
        or not isinstance(reviewed_overlay_sandbox_sha256, str)
        or not SHA256_RE.fullmatch(reviewed_overlay_sandbox_sha256)
        or reviewed_overlay_sandbox_sha256 != overlay_sandbox_sha256
    ):
        raise RunnerFailure(
            "release proof sandbox digest crosslink differs", exit_code=2
        )
    add(
        "release",
        overlay_attestation.get("semantic_input_authority"),
        "release overlay A",
    )

    lock_authority_file = prepared.source_review_files.get("lock_authority")
    if lock_authority_file is None:
        raise RunnerFailure("reviewed resolver authority is absent", exit_code=2)
    lock_authority = _load_canonical_support_authority(
        lock_authority_file,
        "resolver semantic authority",
    )
    lock_manifest = lock_authority.get("lock_manifest")
    lock_payload = (
        lock_manifest.get("payload") if isinstance(lock_manifest, dict) else None
    )
    resolver_variants = (
        lock_payload.get("variants") if isinstance(lock_payload, dict) else None
    )
    approval_variants = prepared.source_approval_value.get("variants")
    if (
        not isinstance(resolver_variants, dict)
        or set(resolver_variants) != set(VARIANTS)
        or not isinstance(approval_variants, dict)
        or set(approval_variants) != set(VARIANTS)
    ):
        raise RunnerFailure("resolver semantic topology differs", exit_code=2)
    for name in VARIANTS:
        item = resolver_variants[name]
        approval_item = approval_variants[name]
        if not isinstance(item, dict) or not isinstance(approval_item, dict):
            raise RunnerFailure(f"resolver {name} authority is absent", exit_code=2)
        final = item.get("resolver")
        current_attempt = item.get("current_lock_attempt")
        if (
            approval_item.get("lock_resolution") != final
            or approval_item.get("current_lock_attempt") != current_attempt
        ):
            raise RunnerFailure(
                f"resolver {name} approval/review authority differs",
                exit_code=2,
            )
        if name in {"A", "B"}:
            if (
                current_attempt is not None
                or not isinstance(final, dict)
                or final.get("resolver_kind") != "tracked_git_readback"
                or "semantic_input_authority" in final
            ):
                raise RunnerFailure(f"resolver {name} topology differs", exit_code=2)
            continue
        for role, record in (("current", current_attempt), ("generated", final)):
            if (
                not isinstance(record, dict)
                or record.get("resolver_kind") != "sandboxed_cargo_resolution"
            ):
                raise RunnerFailure(
                    f"resolver {name} {role} topology differs", exit_code=2
                )
            _validate_semantic_sandbox(
                record.get("argv"),
                record.get("environment"),
                f"resolver {name} {role}",
                passed_file_descriptors=record.get("passed_file_descriptors"),
                expected_passed_file_descriptors=13,
                require_loader_origin=False,
                expected_fd_destinations=RESOLVER_DESTINATIONS,
                writable_fd_destinations={"/asterism/source"},
                expected_descriptor_bindings=RESOLVER_DESCRIPTOR_BINDINGS,
            )
            add(
                "resolver",
                record.get("semantic_input_authority"),
                f"resolver {name} {role}",
                source_role="resolution_source_without_cargo_lock",
            )

    counts = {
        section: sum(record[0] == section for record in records)
        for section in SEMANTIC_MANIFEST_COUNTS
    }
    if counts != SEMANTIC_MANIFEST_COUNTS or len(records) != SEMANTIC_MANIFEST_TOTAL:
        raise RunnerFailure(
            f"semantic manifest topology differs: counts={counts}", exit_code=2
        )
    retained: list[RetainedSemanticManifest] = []
    seen_paths: dict[str, str] = {}
    seen_identities: dict[tuple[int, int], str] = {}
    runtime_manifests: dict[int, dict[str, dict[str, Any]]] = {}
    completed_runtime_authorities = 0
    try:
        for _section, path, digest, _schema, _role, _context, _binding in records:
            prior = seen_paths.get(path)
            if prior is not None:
                conflict = "conflicting digest" if prior != digest else "path alias"
                raise RunnerFailure(
                    f"semantic manifest {conflict}: {path}", exit_code=2
                )
            seen_paths[path] = digest
        for section, path, digest, schema, role, context, binding in records:
            snapshot = _retain_semantic_manifest(
                path,
                digest,
                schema,
                context,
                canonicalizer=(
                    canonical_json_bytes
                    if section == "current"
                    else reviewed_authority_canonical_json_bytes
                ),
            )
            retained.append(snapshot)
            _claim_semantic_physical_identity(seen_identities, snapshot)
            if snapshot.value is None:
                raise RunnerFailure(f"{context} retained evidence is absent", exit_code=2)
            if schema == RECURSIVE_TREE_AUTHORITY_SCHEMA:
                tree = _validate_recursive_semantic_manifest(
                    snapshot.value, role, context, trusted_system=False
                )
                if (
                    len(tree["entries"]) != binding["entry_count"]
                    or sum(
                        entry["file_type"] == "directory"
                        for entry in tree["entries"]
                    )
                    != binding["watch_count"]
                ):
                    raise RunnerFailure(
                        f"{context} evidence cardinality differs", exit_code=2
                    )
            else:
                closure = snapshot.value
                evidence_mounts = closure.get("mounts")
                if (
                    set(closure) != {"mounts", "schema"}
                    or not isinstance(evidence_mounts, list)
                    or len(evidence_mounts) != len(TRUSTED_SYSTEM_MOUNTS)
                ):
                    raise RunnerFailure(
                        f"{context} closure evidence fields differ", exit_code=2
                    )
                entry_count = 0
                watch_count = 0
                for evidence_mount, binding_mount, (host, guest) in zip(
                    evidence_mounts,
                    binding["mounts"],
                    TRUSTED_SYSTEM_MOUNTS,
                    strict=True,
                ):
                    if (
                        not isinstance(evidence_mount, dict)
                        or set(evidence_mount)
                        != {"guest_path", "host_path", "resolved_path", "tree"}
                        or evidence_mount.get("guest_path") != guest
                        or evidence_mount.get("host_path") != host
                        or evidence_mount.get("resolved_path") != host
                    ):
                        raise RunnerFailure(
                            f"{context} closure mount evidence differs", exit_code=2
                        )
                    tree = _validate_recursive_semantic_manifest(
                        evidence_mount.get("tree"),
                        "system-" + guest.removeprefix("/").replace("/", "-"),
                        f"{context} {guest}",
                        trusted_system=True,
                    )
                    root_entry = tree["entries"][0]
                    if any(
                        binding_mount[field] != root_entry[field]
                        for field in ("device", "gid", "inode", "permissions", "uid")
                    ):
                        raise RunnerFailure(
                            f"{context} closure root binding differs", exit_code=2
                        )
                    entry_count += len(tree["entries"])
                    watch_count += sum(
                        entry["file_type"] == "directory"
                        for entry in tree["entries"]
                    )
                if (
                    entry_count != binding["entry_count"]
                    or watch_count != binding["watch_count"]
                ):
                    raise RunnerFailure(
                        f"{context} closure cardinality differs", exit_code=2
                    )
            runtime_binding = runtime_manifest_bindings.get(path)
            if runtime_binding is not None:
                authority_index, component = runtime_binding
                manifests = runtime_manifests.setdefault(authority_index, {})
                if component in manifests:
                    raise RunnerFailure(
                        f"{context} semantic runtime component aliases", exit_code=2
                    )
                manifests[component] = snapshot.value
                if set(manifests) == set(runtime_component_names):
                    authority, authority_context = runtime_authorities[authority_index]
                    components = {
                        name: authority[name] for name in runtime_component_names
                    }
                    try:
                        content_digest = runtime_content_hasher(components, manifests)
                    except Exception as error:
                        raise RunnerFailure(
                            f"{authority_context} normalized semantic runtime "
                            f"validation failed: {error.__class__.__name__}: {error}",
                            exit_code=2,
                        ) from error
                    if (
                        not isinstance(content_digest, str)
                        or not SHA256_RE.fullmatch(content_digest)
                    ):
                        raise RunnerFailure(
                            f"{authority_context} normalized semantic runtime "
                            "digest differs",
                            exit_code=2,
                        )
                    runtime_digests.add(content_digest)
                    del runtime_manifests[authority_index]
                    completed_runtime_authorities += 1
            snapshot.value = None
        if (
            runtime_manifests
            or completed_runtime_authorities != len(runtime_authorities)
            or len(runtime_digests) != 1
        ):
            raise RunnerFailure(
                "semantic runtime content topology differs: "
                f"authorities={completed_runtime_authorities}/"
                f"{len(runtime_authorities)} runtime_digests={len(runtime_digests)}",
                exit_code=2,
            )
        if (
            len(retained) != SEMANTIC_MANIFEST_TOTAL
            or len(seen_identities) != SEMANTIC_MANIFEST_TOTAL
        ):
            raise RunnerFailure("semantic retained set is incomplete", exit_code=2)
        return retained, counts
    except BaseException as error:
        _final_verify_close_retained_manifests(retained, error)
        raise


def resolve_bound_file(
    root: Path, value: dict[str, Any], context: str, *, executable: bool
) -> Executable:
    require_exact_keys(value, {"path", "sha256", "executable_mode", "comm"}, context)
    lexical = Path(str(value["path"]))
    if not lexical.is_absolute():
        raise RunnerFailure(f"{context} path is not absolute", exit_code=2)
    metadata = lexical.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise RunnerFailure(f"{context} path is not a regular non-symlink", exit_code=2)
    path = lexical.resolve(strict=True)
    if root not in path.parents:
        raise RunnerFailure(f"{context} path escapes prepared root", exit_code=2)
    expected_sha = str(value["sha256"])
    expected_mode = int(value["executable_mode"])
    comm = str(value["comm"])
    observed_mode = stat.S_IMODE(path.stat().st_mode)
    if (
        not SHA256_RE.fullmatch(expected_sha)
        or sha256(path) != expected_sha
        or observed_mode != expected_mode
        or len(comm.encode()) > 15
        or (executable and not os.access(path, os.X_OK))
    ):
        raise RunnerFailure(f"{context} executable binding mismatch", exit_code=2)
    return Executable(context, path, expected_sha, expected_mode, comm)


def resolve_support_file(
    root: Path, value: dict[str, Any], context: str
) -> SupportFile:
    require_exact_keys(value, {"path", "sha256", "mode"}, context)
    lexical = Path(str(value["path"]))
    if not lexical.is_absolute():
        raise RunnerFailure(f"{context} path is not absolute", exit_code=2)
    metadata = lexical.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise RunnerFailure(f"{context} path is not a regular non-symlink", exit_code=2)
    path = lexical.resolve(strict=True)
    if root not in path.parents:
        raise RunnerFailure(f"{context} path escapes prepared root", exit_code=2)
    digest = str(value["sha256"])
    mode = int(value["mode"])
    if (
        not SHA256_RE.fullmatch(digest)
        or sha256(path) != digest
        or stat.S_IMODE(path.stat().st_mode) != mode
        or mode & 0o222
    ):
        raise RunnerFailure(f"{context} immutable binding differs", exit_code=2)
    return SupportFile(context, path, digest, mode)


def resolve_immutable_json_file(
    value: Path, context: str, *, root: Path | None = None, filename: str
) -> Path:
    lexical = value
    if root is not None and not lexical.is_absolute():
        raise RunnerFailure(f"{context} path is not absolute", exit_code=2)
    if not lexical.is_absolute():
        lexical = Path.cwd() / lexical
    try:
        metadata = lexical.lstat()
        path = lexical.resolve(strict=True)
    except OSError as error:
        raise RunnerFailure(f"{context} cannot be resolved: {error}", exit_code=2) from error
    if (
        lexical.name != filename
        or stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o444
    ):
        raise RunnerFailure(
            f"{context} must be a non-symlink regular {filename} with mode 0444",
            exit_code=2,
        )
    if root is not None and root != path.parent and root not in path.parents:
        raise RunnerFailure(f"{context} escapes prepared root", exit_code=2)
    return path


def validate_approved_tools_manifest(
    value: Any, claimed_sha256: Any, schema: Any
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RunnerFailure("source-approved tools manifest is not an object", exit_code=2)
    require_exact_keys(
        value,
        set(getattr(schema, "TOOLS_MANIFEST_FIELDS", ())),
        "source-approved tools manifest",
    )
    observed_sha256 = sha256_bytes(
        reviewed_authority_canonical_json_bytes(value)
    )
    if (
        value.get("schema")
        != getattr(schema, "TOOLS_MANIFEST_SCHEMA", None)
        or not isinstance(claimed_sha256, str)
        or not SHA256_RE.fullmatch(claimed_sha256)
        or claimed_sha256 != observed_sha256
    ):
        raise RunnerFailure(
            "source-approved tools manifest schema/digest differs", exit_code=2
        )
    expected_allowlist_function = getattr(schema, "expected_comm_allowlist", None)
    if (
        not callable(expected_allowlist_function)
        or value.get("comm_allowlist") != expected_allowlist_function()
    ):
        raise RunnerFailure(
            "source-approved tools manifest comm allowlist differs", exit_code=2
        )

    tools = value.get("tools")
    expected_tools = set(getattr(schema, "PREPARED_TOOL_NAMES", ()))
    expected_tool_comms = getattr(schema, "PREPARED_TOOL_COMMS", None)
    if (
        not isinstance(tools, dict)
        or set(tools) != expected_tools
        or not isinstance(expected_tool_comms, dict)
        or set(expected_tool_comms) != expected_tools
    ):
        raise RunnerFailure(
            "source-approved tool binding names differ", exit_code=2
        )
    for name, binding in tools.items():
        if not isinstance(binding, dict):
            raise RunnerFailure(
                f"source-approved tool {name} is not an object", exit_code=2
            )
        require_exact_keys(
            binding,
            set(getattr(schema, "TOOL_BINDING_FIELDS", ())),
            f"source-approved tool {name}",
        )
        source_path = Path(str(binding.get("path")))
        if (
            not source_path.is_absolute()
            or not isinstance(binding.get("sha256"), str)
            or not SHA256_RE.fullmatch(binding["sha256"])
            or binding.get("executable_mode") != 0o555
            or binding.get("comm") != expected_tool_comms[name]
        ):
            raise RunnerFailure(
                f"source-approved tool {name} claim differs", exit_code=2
            )

    support = value.get("support_files")
    expected_support = set(getattr(schema, "PREPARED_SUPPORT_FILE_NAMES", ()))
    if not isinstance(support, dict) or set(support) != expected_support:
        raise RunnerFailure(
            "source-approved support binding names differ", exit_code=2
        )
    for name, binding in support.items():
        if not isinstance(binding, dict):
            raise RunnerFailure(
                f"source-approved support {name} is not an object", exit_code=2
            )
        require_exact_keys(
            binding,
            set(getattr(schema, "SUPPORT_FILE_FIELDS", ())),
            f"source-approved support {name}",
        )
        source_path = Path(str(binding.get("path")))
        if (
            not source_path.is_absolute()
            or not isinstance(binding.get("sha256"), str)
            or not SHA256_RE.fullmatch(binding["sha256"])
            or binding.get("mode") != 0o444
        ):
            raise RunnerFailure(
                f"source-approved support {name} claim differs", exit_code=2
            )
    return value


def load_prepared(path: Path, schema: Any | None = None) -> Prepared:
    path = resolve_immutable_json_file(
        path, "prepared artifacts", filename="prepared-artifacts.json"
    )
    value = load_reviewed_authority_json(path)
    expected_prepared_fields = set(
        getattr(
            schema,
            "PREPARED_FIELDS",
            (
                "schema",
                "protocol",
                "protocol_sha256",
                "tooling_commit",
                "tooling_tree",
                "created_at",
                "created_monotonic_ns",
                "source_approval",
                "single_use_claim",
                "comm_allowlist",
                "tools",
                "build_order",
                "toolchain",
                "variants",
            ),
        )
    )
    require_exact_keys(value, expected_prepared_fields, "prepared artifacts")
    if (
        value["schema"] != PREPARED_SCHEMA
        or value["protocol"] != PROTOCOL
        or value["protocol_sha256"] != PROTOCOL_SHA256
    ):
        raise RunnerFailure("prepared protocol binding mismatch", exit_code=2)
    if (
        value.get("build_order") != list(VARIANTS)
        or isinstance(value.get("created_monotonic_ns"), bool)
        or not isinstance(value.get("created_monotonic_ns"), int)
        or value["created_monotonic_ns"] <= 0
        or not isinstance(value.get("toolchain"), dict)
    ):
        raise RunnerFailure("prepared build order/toolchain attestation differs", exit_code=2)
    if not GIT_OBJECT_RE.fullmatch(str(value["tooling_commit"])) or not GIT_OBJECT_RE.fullmatch(
        str(value["tooling_tree"])
    ):
        raise RunnerFailure("prepared tooling object IDs are invalid", exit_code=2)
    root = path.parent.resolve()
    support_value = value.get("support_files")
    expected_support = set(
        getattr(
            schema,
            "PREPARED_SUPPORT_FILE_NAMES",
            (
                "runner",
                "evaluator",
                "terminal_verifier",
                "evidence_schema",
                "profile_adapter",
                "strace_attach",
            ),
        )
    )
    if not isinstance(support_value, dict) or set(support_value) != expected_support:
        raise RunnerFailure("prepared support file names differ", exit_code=2)
    support_files = {
        name: resolve_support_file(root, binding, f"support-{name}")
        for name, binding in support_value.items()
    }
    support_parents = {binding.path.parent for binding in support_files.values()}
    if len(support_parents) != 1:
        raise RunnerFailure("prepared support files are not colocated", exit_code=2)
    support_directory = next(iter(support_parents))
    if stat.S_IMODE(support_directory.stat().st_mode) != 0o555:
        raise RunnerFailure("prepared support directory mode is not 0555", exit_code=2)
    expected_local_support = {
        "runner": Path(__file__).resolve(),
        "evidence_schema": Path(__file__).with_name("evidence_schema.py").resolve(),
        "profile_adapter": Path(__file__).with_name("profile_adapters.py").resolve(),
        "strace_attach": Path(__file__).with_name("strace_attach.py").resolve(),
        "evaluator": Path(__file__).with_name("evaluate.py").resolve(),
        "terminal_verifier": Path(__file__).with_name("verify_terminal.py").resolve(),
    }
    if expected_support != set(expected_local_support):
        raise RunnerFailure("shared support file names differ", exit_code=2)
    if any(
        support_files[name].path != expected_local_support[name]
        for name in expected_support
    ):
        raise RunnerFailure("executed runner support directory differs", exit_code=2)
    expected_support_entries = {
        binding.path.name for binding in support_files.values()
    }
    observed_support_entries = {entry.name for entry in support_directory.iterdir()}
    if (
        observed_support_entries != expected_support_entries
        or any(name.startswith(".") or name == "__pycache__" for name in observed_support_entries)
    ):
        raise RunnerFailure(
            "prepared support directory entries are not exact", exit_code=2
        )
    input_value = value.get("inputs")
    expected_inputs = {"historical_baseline", "protocol"}
    schema_input_names = set(
        getattr(schema, "PREPARED_INPUT_NAMES", tuple(sorted(expected_inputs)))
    )
    if schema_input_names != expected_inputs:
        raise RunnerFailure("shared prepared input names differ", exit_code=2)
    if not isinstance(input_value, dict) or set(input_value) != expected_inputs:
        raise RunnerFailure("prepared input names differ", exit_code=2)
    inputs = {
        name: resolve_support_file(root, binding, f"input-{name}")
        for name, binding in input_value.items()
    }
    exact_inputs = {
        "historical_baseline": (
            "BN-2SU-FINAL.csv",
            "inputs/BN-2SU-FINAL.csv",
            HISTORICAL_BASELINE_SHA256,
        ),
        "protocol": (
            "BN-2L3N-PROTOCOL.md",
            "BN-2L3N-PROTOCOL.md",
            PROTOCOL_SHA256,
        ),
    }
    if (
        getattr(schema, "PREPARED_INPUT_FILENAMES", None)
        != {name: value[0] for name, value in exact_inputs.items()}
        or getattr(schema, "PREPARED_INPUT_RELATIVE_PATHS", None)
        != {name: value[1] for name, value in exact_inputs.items()}
        or getattr(schema, "PREPARED_INPUT_SHA256", None)
        != {name: value[2] for name, value in exact_inputs.items()}
        or tuple(getattr(schema, "PREPARED_INPUT_FIELDS", ()))
        != ("path", "sha256", "mode")
    ):
        raise RunnerFailure("shared prepared input authority differs", exit_code=2)
    for name, (filename, relative_path, digest) in exact_inputs.items():
        binding = inputs[name]
        if (
            binding.path.name != filename
            or binding.path != (root / relative_path).resolve(strict=True)
            or binding.sha256 != digest
            or binding.mode != 0o444
        ):
            raise RunnerFailure(
                f"prepared input {name} binding differs", exit_code=2
            )
    source_review_value = value.get("source_review")
    expected_source_review_names = {
        "bundle",
        "current_children_attestation",
        "lock_authority",
        "lock_review_bundle",
    }
    shared_source_review_names = set(
        getattr(schema, "PREPARED_SOURCE_REVIEW_FIELDS", ())
    )
    shared_source_review_binding_fields = tuple(
        getattr(schema, "PREPARED_SOURCE_REVIEW_BINDING_FIELDS", ())
    )
    source_review_relative_paths = getattr(
        schema, "PREPARED_SOURCE_REVIEW_RELATIVE_PATHS", None
    )
    if (
        shared_source_review_names != expected_source_review_names
        or shared_source_review_binding_fields != ("path", "sha256", "mode")
        or not isinstance(source_review_relative_paths, dict)
        or set(source_review_relative_paths) != expected_source_review_names
    ):
        raise RunnerFailure(
            "shared prepared source-review authority differs", exit_code=2
        )
    if (
        not isinstance(source_review_value, dict)
        or set(source_review_value) != expected_source_review_names
    ):
        raise RunnerFailure("prepared source-review bindings differ", exit_code=2)
    source_review_files = {
        name: resolve_support_file(
            root, source_review_value[name], f"prepared-source-review-{name}"
        )
        for name in sorted(expected_source_review_names)
    }
    for name, binding in source_review_files.items():
        expected_path = (root / str(source_review_relative_paths[name])).resolve(
            strict=True
        )
        if binding.path != expected_path or binding.mode != 0o444:
            raise RunnerFailure(
                f"prepared source-review {name} binding differs", exit_code=2
            )

    release_compile_out_value = value.get("release_compile_out")
    if tuple(getattr(schema, "RELEASE_COMPILE_OUT_BINDING_FIELDS", ())) != (
        "path",
        "sha256",
        "mode",
    ):
        raise RunnerFailure(
            "shared release compile-out binding authority differs", exit_code=2
        )
    if not isinstance(release_compile_out_value, dict):
        raise RunnerFailure("prepared release compile-out binding is absent", exit_code=2)
    release_compile_out = resolve_support_file(
        root, release_compile_out_value, "prepared-release-compile-out"
    )
    expected_release_compile_out_path = (
        root / str(getattr(schema, "RELEASE_COMPILE_OUT_RELATIVE_PATH", ""))
    ).resolve(strict=True)
    if (
        release_compile_out.path != expected_release_compile_out_path
        or release_compile_out.mode != 0o444
    ):
        raise RunnerFailure(
            "prepared release compile-out path/mode differs", exit_code=2
        )
    release_compile_out_payload = load_reviewed_authority_json(
        release_compile_out.path
    )
    if (
        release_compile_out_payload.get("schema")
        != getattr(schema, "RELEASE_COMPILE_OUT_SCHEMA", None)
        or release_compile_out_payload.get("protocol") != PROTOCOL
        or release_compile_out_payload.get("protocol_sha256") != PROTOCOL_SHA256
        or release_compile_out_payload.get("status") != "ok"
    ):
        raise RunnerFailure(
            "prepared release compile-out identity differs", exit_code=2
        )
    approval_binding = value["source_approval"]
    require_exact_keys(approval_binding, {"path", "sha256"}, "source approval binding")
    source_approval = resolve_immutable_json_file(
        Path(str(approval_binding["path"])),
        "source approval",
        root=root,
        filename="source-approval.json",
    )
    if sha256(source_approval) != approval_binding["sha256"]:
        raise RunnerFailure("source approval binding mismatch", exit_code=2)
    source_approval_value = load_reviewed_authority_json(source_approval)
    reject_superseded_authority(source_approval_value, "source approval")
    call_shared_validator(
        schema,
        "validate_source_approval",
        "source approval",
        source_approval_value,
    )
    require_exact_keys(
        source_approval_value,
        set(getattr(schema, "SOURCE_APPROVAL_FIELDS", source_approval_value)),
        "source approval",
    )
    if (
        source_approval_value.get("schema")
        != getattr(schema, "SOURCE_APPROVAL_SCHEMA", "bn-2l3n-source-approval-v3")
        or source_approval_value.get("protocol") != PROTOCOL
        or source_approval_value.get("protocol_sha256") != PROTOCOL_SHA256
        or source_approval_value.get("status") != "approved"
    ):
        raise RunnerFailure("source approval is not an approved protocol binding", exit_code=2)
    if set(source_approval_value.get("variants", {})) != set(VARIANTS):
        raise RunnerFailure("source approval variant set differs", exit_code=2)
    for name, approval_variant in source_approval_value["variants"].items():
        require_exact_keys(
            approval_variant,
            set(
                getattr(
                    schema,
                    "SOURCE_APPROVAL_VARIANT_FIELDS",
                    approval_variant,
                )
            ),
            f"source approval variant {name}",
        )
        expected_source = VARIANT_SOURCE_BINDINGS[name]
        if (
            approval_variant.get("product_commit") != expected_source["commit"]
            or approval_variant.get("product_tree") != expected_source["tree"]
        ):
            raise RunnerFailure(
                f"source approval variant {name} product binding differs",
                exit_code=2,
            )
    approved_tools_manifest = validate_approved_tools_manifest(
        source_approval_value.get("tools_manifest"),
        source_approval_value.get("tools_manifest_sha256"),
        schema,
    )
    manifest_binding_value = value["tools_manifest"]
    if not isinstance(manifest_binding_value, dict):
        raise RunnerFailure("prepared tools manifest binding is not an object", exit_code=2)
    require_exact_keys(
        manifest_binding_value,
        set(getattr(schema, "TOOLS_MANIFEST_BINDING_FIELDS", ())),
        "prepared tools manifest binding",
    )
    tools_manifest = resolve_support_file(
        root, manifest_binding_value, "prepared-tools-manifest"
    )
    expected_manifest_path = (root / "bindings" / "tools-manifest.json").resolve(
        strict=True
    )
    if (
        tools_manifest.path != expected_manifest_path
        or tools_manifest.mode != 0o444
        or tools_manifest.sha256
        != source_approval_value.get("tools_manifest_sha256")
    ):
        raise RunnerFailure("prepared tools manifest binding differs", exit_code=2)
    tools_manifest_value = load_reviewed_authority_json(tools_manifest.path)
    if tools_manifest_value != approved_tools_manifest:
        raise RunnerFailure(
            "prepared tools manifest differs from source approval", exit_code=2
        )
    for name, support in support_files.items():
        approved = approved_tools_manifest["support_files"][name]
        if (support.sha256, support.mode) != (
            approved["sha256"],
            approved["mode"],
        ):
            raise RunnerFailure(
                f"prepared support {name} differs from source-approved claim",
                exit_code=2,
            )
    # Protocol v4: prepared artifacts are reusable across rehearsals and
    # declared runs.  The v3 single-use claim binding is still parsed for
    # structural compatibility with build-tool output, but the claim marker
    # is recorded in the run's own output directory (see claim_prepared),
    # never written into the prepared root, and a pre-existing v3 claims/
    # directory or consumed claim no longer blocks a run.
    claim_binding = value["single_use_claim"]
    require_exact_keys(claim_binding, {"path"}, "single-use claim")
    claim_lexical = Path(str(claim_binding["path"]))
    if not claim_lexical.is_absolute():
        raise RunnerFailure("single-use claim path is not absolute", exit_code=2)
    claim_path = claim_lexical.resolve()
    if stat.S_IMODE(root.stat().st_mode) != 0o555:
        raise RunnerFailure("prepared root is not read-only 0555", exit_code=2)
    if set(value["variants"]) != set(VARIANTS):
        raise RunnerFailure("prepared variant set differs from A/B/C/D", exit_code=2)
    variants: dict[str, Variant] = {}
    for name in VARIANTS:
        item = value["variants"][name]
        require_exact_keys(
            item,
            set(
                getattr(
                    schema,
                    "PREPARED_VARIANT_FIELDS",
                    (
                        "contract",
                        "binary",
                        "executable_mode",
                        "artifact_root",
                        "contract_argv",
                        "contract_env",
                        "comm",
                        "evidence_argv",
                        "evidence_env",
                        "attestation",
                    ),
                )
            ),
            f"variant {name}",
        )
        require_exact_keys(
            item["attestation"],
            set(getattr(schema, "PREPARED_ATTESTATION_FIELDS", item["attestation"])),
            f"variant {name} attestation",
        )
        require_exact_keys(item["binary"], {"path", "sha256"}, f"variant {name} binary")
        executable = resolve_bound_file(
            root,
            {
                **item["binary"],
                "executable_mode": item["executable_mode"],
                "comm": item["comm"],
            },
            f"variant-{name}",
            executable=True,
        )
        if executable.mode != 0o555:
            raise RunnerFailure(
                f"variant {name} executable mode is not immutable 0555", exit_code=2
            )
        expected_variant_comms = getattr(schema, "VARIANT_COMMS", None)
        if (
            not isinstance(expected_variant_comms, dict)
            or executable.comm != expected_variant_comms.get(name)
        ):
            raise RunnerFailure(
                f"variant {name} comm differs from shared authority", exit_code=2
            )
        contract = item["contract"]
        if not isinstance(contract, dict) or contract.get("variant") != name:
            raise RunnerFailure(f"variant {name} contract mismatch", exit_code=2)
        reject_superseded_authority(contract, f"variant {name} binary contract")
        call_shared_validator(
            schema,
            "validate_binary_contract",
            f"variant {name} binary contract",
            contract,
        )
        expected_source = VARIANT_SOURCE_BINDINGS[name]
        if (
            contract.get("protocol_sha256") != PROTOCOL_SHA256
            or contract.get("contract_mode") is not True
            or contract.get("rows_written") != 0
            or contract.get("product_commit") != expected_source["commit"]
            or contract.get("product_tree") != expected_source["tree"]
        ):
            raise RunnerFailure(f"variant {name} contract is incomplete", exit_code=2)
        expected_oracle_mode = name in PUBLIC_VARIANTS
        if (
            item.get("correctness_oracle_mode") is not expected_oracle_mode
            or contract.get("correctness_oracle_mode") is not expected_oracle_mode
            or source_approval_value["variants"][name].get(
                "correctness_oracle_mode"
            )
            is not expected_oracle_mode
        ):
            raise RunnerFailure(
                f"variant {name} correctness oracle capability differs",
                exit_code=2,
            )
        contract_argv = item["contract_argv"]
        evidence_argv = item["evidence_argv"]
        contract_env = item["contract_env"]
        evidence_env = item["evidence_env"]
        if (
            not isinstance(contract_argv, list)
            or len(contract_argv) != 1
            or not all(isinstance(arg, str) for arg in contract_argv)
            or not isinstance(evidence_argv, list)
            or len(evidence_argv) != 1
            or not all(isinstance(arg, str) for arg in evidence_argv)
            or not isinstance(contract_env, dict)
            or not all(
                isinstance(key, str) and isinstance(val, str)
                for key, val in contract_env.items()
            )
            or not isinstance(evidence_env, dict)
            or not all(
                isinstance(key, str) and isinstance(val, str)
                for key, val in evidence_env.items()
            )
        ):
            raise RunnerFailure(f"variant {name} argv/environment is malformed", exit_code=2)
        validate_trace_environment = getattr(
            schema, "validate_trace_marker_environment", None
        )
        if not callable(validate_trace_environment):
            raise RunnerFailure(
                "shared trace-marker environment authority is absent", exit_code=2
            )
        try:
            validate_trace_environment(name, evidence_env)
        except (TypeError, ValueError) as error:
            raise RunnerFailure(
                f"variant {name} trace-marker environment differs: {error}",
                exit_code=2,
            ) from error
        expected_trace_templates = getattr(
            schema, "expected_trace_path_marker_templates", None
        )
        if not callable(expected_trace_templates):
            raise RunnerFailure(
                "shared trace-marker template authority is absent", exit_code=2
            )
        try:
            frozen_trace_templates = expected_trace_templates(name)
        except (TypeError, ValueError) as error:
            raise RunnerFailure(
                f"variant {name} trace-marker templates are invalid: {error}",
                exit_code=2,
            ) from error
        if (
            item.get("trace_path_marker_templates") != frozen_trace_templates
            or source_approval_value["variants"][name].get(
                "trace_path_marker_templates"
            )
            != frozen_trace_templates
        ):
            raise RunnerFailure(
                f"variant {name} source/prepared trace-marker templates differ",
                exit_code=2,
            )
        artifact_root_lexical = Path(str(item["artifact_root"]))
        if not artifact_root_lexical.is_absolute():
            raise RunnerFailure(
                f"variant {name} artifact root is not absolute", exit_code=2
            )
        artifact_root = artifact_root_lexical.resolve(strict=True)
        if root not in artifact_root.parents and artifact_root != root:
            raise RunnerFailure(f"variant {name} artifact root escapes prepared root", exit_code=2)
        if contract_argv[0] != str(executable.path) or evidence_argv[0] != str(
            executable.path
        ):
            raise RunnerFailure(f"variant {name} argv does not execute bound binary", exit_code=2)
        variants[name] = Variant(
            name=name,
            product_commit=str(contract.get("product_commit", "")),
            product_tree=str(contract.get("product_tree", "")),
            binary_kind=str(contract.get("binary_kind", "")),
            timed_surface=str(contract.get("timed_surface", "")),
            correctness_oracle_mode=expected_oracle_mode,
            executable=executable,
            contract=contract,
            contract_argv=tuple(contract_argv),
            contract_env=contract_env,
            evidence_argv=tuple(evidence_argv),
            evidence_env=evidence_env,
            trace_path_marker_templates=json.loads(
                canonical_json_bytes(frozen_trace_templates)
            ),
        )
    tools: dict[str, Executable] = {}
    if not isinstance(value["tools"], dict):
        raise RunnerFailure("prepared tools are malformed", exit_code=2)
    for name, binding in value["tools"].items():
        tools[name] = resolve_bound_file(root, binding, f"tool-{name}", executable=True)
        if tools[name].mode != 0o555:
            raise RunnerFailure(f"tool {name} mode is not immutable 0555", exit_code=2)
    expected_tools = set(getattr(schema, "PREPARED_TOOL_NAMES", tools))
    if set(tools) != expected_tools:
        raise RunnerFailure(
            f"prepared tool names differ: missing={sorted(expected_tools - set(tools))} "
            f"extra={sorted(set(tools) - expected_tools)}",
            exit_code=2,
        )
    expected_tool_comms = getattr(schema, "PREPARED_TOOL_COMMS", None)
    if not isinstance(expected_tool_comms, dict) or set(expected_tool_comms) != set(
        tools
    ):
        raise RunnerFailure("shared tool comm authority differs", exit_code=2)
    for name, executable in tools.items():
        if executable.comm != expected_tool_comms[name]:
            raise RunnerFailure(
                f"tool {name} comm differs from shared authority", exit_code=2
            )
        approved = approved_tools_manifest["tools"][name]
        if (executable.sha256, executable.mode, executable.comm) != (
            approved["sha256"],
            approved["executable_mode"],
            approved["comm"],
        ):
            raise RunnerFailure(
                f"prepared tool {name} differs from source-approved claim",
                exit_code=2,
            )
    comm_allowlist = value["comm_allowlist"]
    expected_allowlist_function = getattr(schema, "expected_comm_allowlist", None)
    if not callable(expected_allowlist_function):
        raise RunnerFailure("shared comm allowlist authority is absent", exit_code=2)
    expected_allowlist = expected_allowlist_function()
    if (
        not isinstance(comm_allowlist, list)
        or not all(
            isinstance(item, str) and 0 < len(item.encode()) <= 15
            for item in comm_allowlist
        )
        or comm_allowlist != sorted(set(comm_allowlist))
        or comm_allowlist != expected_allowlist
        or source_approval_value.get("comm_allowlist") != expected_allowlist
        or approved_tools_manifest.get("comm_allowlist") != expected_allowlist
    ):
        raise RunnerFailure(
            "prepared/source-approved comm allowlist differs from shared authority",
            exit_code=2,
        )
    reject_superseded_authority(value, "prepared artifacts")
    call_shared_validator(
        schema,
        "validate_prepared_artifacts",
        "prepared artifacts",
        value,
        source_approval_value,
        path,
    )
    return Prepared(
        schema=schema,
        path=path,
        digest=sha256(path),
        value=value,
        root=root,
        source_approval=source_approval,
        source_approval_sha256=str(approval_binding["sha256"]),
        source_approval_value=source_approval_value,
        source_review_files=source_review_files,
        release_compile_out=release_compile_out,
        release_compile_out_value=release_compile_out_payload,
        tools_manifest=tools_manifest,
        tools_manifest_value=tools_manifest_value,
        claim_path=claim_path,
        variants=variants,
        tools=tools,
        support_files=support_files,
        inputs=inputs,
        tracked_comm=frozenset(comm_allowlist),
    )


def load_schema() -> EvidenceSchema:
    path = Path(__file__).with_name("evidence_schema.py").resolve()
    if not path.is_file():
        raise RunnerFailure(f"evidence schema is absent: {path}", exit_code=2)
    spec = importlib.util.spec_from_file_location("asterism_evidence_schema", path)
    if spec is None or spec.loader is None:
        raise RunnerFailure(f"cannot load evidence schema: {path}", exit_code=2)
    module = importlib.util.module_from_spec(spec)
    previous = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(spec.name, None)
        raise
    finally:
        sys.dont_write_bytecode = previous
    expected_bindings = {
        "PROTOCOL": PROTOCOL,
        "PREPARED_ARTIFACTS_SCHEMA": PREPARED_SCHEMA,
        "CONFIG_SCHEMA": CONFIG_SCHEMA,
        "PROVENANCE_SCHEMA": PROVENANCE_SCHEMA,
        "REQUIRED_FILESYSTEM_TYPE": REQUIRED_FILESYSTEM_TYPE,
        "MIN_FREE_BYTES": MIN_FREE_BYTES,
        "MIN_FREE_INODES": MIN_FREE_INODES,
    }
    for field, expected in expected_bindings.items():
        if getattr(module, field, None) != expected:
            raise RunnerFailure(
                f"evidence schema {field} differs from runner binding",
                exit_code=2,
            )
    if getattr(module, "VARIANT_SOURCE_BINDINGS", None) != VARIANT_SOURCE_BINDINGS:
        raise RunnerFailure(
            "evidence schema variant source bindings differ from runner binding",
            exit_code=2,
        )
    fairness_diagnostics = {
        "queue_depth",
        "queue_bytes",
        "group_width_distribution",
        "adaptive_group_width_target",
        "oldest_queued_age_ns",
    }
    raw_fields = getattr(module, "RAW_POINT_FIELDS_BY_TRACK", {}).get(
        "fairness", ()
    )
    csv_fields = getattr(module, "CSV_FIELDS_BY_TRACK", {}).get("fairness", ())
    if (
        getattr(module, "NOT_AVAILABLE", None) != "not_available"
        or not fairness_diagnostics.issubset(set(raw_fields))
        or not fairness_diagnostics.issubset(set(csv_fields))
    ):
        raise RunnerFailure(
            "fairness unavailable diagnostic authority differs", exit_code=2
        )
    expected_trace_logs = {
        "A": ({"kind": "file_prefix", "path": "log/seg-"},),
        "B": ({"kind": "exact", "path": "segment-1.log"},),
        "C": ({"kind": "file_prefix", "path": "log/seg-"},),
        "D": ({"kind": "file_prefix", "path": "log/seg-"},),
    }
    expected_trace_metadata = {
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
    if (
        getattr(module, "TRACE_LOG_PATH_MARKERS", None) != expected_trace_logs
        or getattr(module, "TRACE_METADATA_PATH_MARKERS", None)
        != expected_trace_metadata
        or not callable(getattr(module, "validate_trace_marker_environment", None))
        or not callable(getattr(module, "expected_trace_path_marker_templates", None))
        or not callable(getattr(module, "resolved_trace_path_markers", None))
    ):
        raise RunnerFailure("trace-marker authority differs", exit_code=2)
    expected_profile_preflight_fields = (
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
    if (
        getattr(module, "PROFILE_PREFLIGHT_SCHEMA", None)
        != "bn-2l3n-profile-preflight-v3"
        or tuple(getattr(module, "PROFILE_PREFLIGHT_FIELDS", ()))
        != expected_profile_preflight_fields
        or getattr(module, "SCHEDSTAT_DECISION_MULTIPLIER", None) != 20
        or not callable(getattr(module, "expected_profile_contract", None))
        or tuple(getattr(module, "ARTIFACT_INVENTORY_FIELDS", ()))
        != ("path", "bytes", "sha256", "mode")
        or getattr(module, "ARTIFACT_FILE_MODE", None) != 0o444
        or not callable(getattr(module, "artifact_inventory", None))
        or not callable(getattr(module, "row_child_environment", None))
        or not callable(getattr(module, "correctness_child_environment", None))
    ):
        raise RunnerFailure("profile preflight schema authority differs", exit_code=2)
    return module


def config_for(prepared: Prepared, schema: EvidenceSchema) -> dict[str, Any]:
    seed_material = (
        PROTOCOL_SHA256 + "\0" + prepared.source_approval_sha256
    ).encode()
    profile_binding = prepared.support_files.get("profile_adapter")
    profile_path = (
        profile_binding.path
        if profile_binding is not None
        else Path(__file__).with_name("profile_adapters.py").resolve()
    )
    profile_sha = sha256(profile_path) if profile_path.is_file() else "0" * 64
    approval = prepared.source_approval_value
    return {
        "schema": CONFIG_SCHEMA,
        "protocol": PROTOCOL,
        "protocol_sha256": PROTOCOL_SHA256,
        "rehearsal": False,
        "approved": approval.get("status") == "approved",
        "review_id": approval.get("review_id"),
        "tooling_commit": prepared.value["tooling_commit"],
        "tooling_tree": prepared.value["tooling_tree"],
        "attempt_nonce": None,
        "seed_sha256": sha256_bytes(seed_material),
        "cell_orders": {
            "primary": primary_cells(),
            "new_names": new_name_cells(),
            "fairness": fairness_cells(),
            "cpu_profiles": sentinel_cells("cpu_profiles"),
            "syscall_profiles": sentinel_cells("syscall_profiles"),
        },
        "variant_sources": {
            name: {
                "commit": variant.product_commit,
                "tree": variant.product_tree,
            }
            for name, variant in prepared.variants.items()
        },
        "lock_hashes": {
            name: variant.contract.get("cargo_lock_sha256")
            for name, variant in prepared.variants.items()
        },
        "resource_limits": {
            "free_bytes": MIN_FREE_BYTES,
            "free_inodes": MIN_FREE_INODES,
            "load1_milli": int(MAX_LOAD1 * 1_000),
            "quiet_wait_seconds": QUIET_TIMEOUT_SECONDS,
        },
        "settle_ms": {"Process": PROCESS_SETTLE_MS, "Group": GROUP_SETTLE_MS},
        "argv_templates": {
            track: [
                "{binary}",
                "--run-row",
                "--track",
                "{track}",
                "--row-ordinal",
                "{row_ordinal}",
                "--config",
                "{config}",
            ]
            for track in TRACK_CARDINALITY
        },
        "smoke_transitions": [],
        "correctness_cases": schema.correctness_descriptors(),
        "correctness_execution": [],
        "profile_contract_sha256": profile_sha,
    }


def syncfs(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.syncfs(descriptor) != 0:
            error = ctypes.get_errno()
            raise RunnerFailure(f"syncfs({path}) failed: errno={error}")
    finally:
        os.close(descriptor)


def tree_manifest(root: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            raise RunnerFailure(f"corpus contains symlink: {relative}")
        if stat.S_ISDIR(metadata.st_mode):
            entries.append(
                {
                    "path": relative,
                    "kind": "directory",
                    "mode": stat.S_IMODE(metadata.st_mode),
                }
            )
        elif stat.S_ISREG(metadata.st_mode):
            entries.append(
                {
                    "path": relative,
                    "kind": "file",
                    "mode": stat.S_IMODE(metadata.st_mode),
                    "bytes": metadata.st_size,
                    "sha256": sha256(path),
                }
            )
        else:
            raise RunnerFailure(f"corpus contains non-file: {relative}")
    return entries


def descriptor_tree_manifest(
    root: Path,
    *,
    root_mode: int,
    directory_mode: int,
    file_mode: int,
) -> list[dict[str, Any]]:
    """Snapshot an exact corpus tree through stable no-follow descriptors."""

    lexical_root = Path(root)
    if (
        not lexical_root.is_absolute()
        or any(part in {"", ".", ".."} for part in lexical_root.parts[1:])
        or len(lexical_root.parts) < 2
    ):
        raise RunnerFailure(f"corpus authority root is not lexical: {root}")
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    file_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    chain: list[int] = []
    chain_identities: list[tuple[int, int]] = []
    entries: list[dict[str, Any]] = []
    file_identities: set[tuple[int, int]] = set()

    def stat_identity(info: os.stat_result) -> tuple[int, ...]:
        return (
            info.st_dev,
            info.st_ino,
            info.st_mode,
            info.st_nlink,
            info.st_size,
            info.st_mtime_ns,
            info.st_ctime_ns,
        )

    def require_identity(
        before: os.stat_result, opened: os.stat_result, context: str
    ) -> None:
        if stat_identity(before) != stat_identity(opened):
            raise RunnerFailure(
                f"corpus authority entry changed while opening: {context}"
            )

    def walk(directory_fd: int, relative: Path, expected_mode: int) -> None:
        directory_before = os.fstat(directory_fd)
        label = relative.as_posix() if relative.parts else "."
        if (
            not stat.S_ISDIR(directory_before.st_mode)
            or stat.S_IMODE(directory_before.st_mode) != expected_mode
        ):
            raise RunnerFailure(f"corpus authority directory mode differs: {label}")
        names = sorted(os.listdir(directory_fd))
        for name in names:
            child_relative = relative / name
            child_label = child_relative.as_posix()
            before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if stat.S_ISLNK(before.st_mode):
                raise RunnerFailure(
                    f"corpus authority contains symlink: {child_label}"
                )
            if stat.S_ISDIR(before.st_mode):
                child_fd = os.open(name, directory_flags, dir_fd=directory_fd)
                try:
                    opened = os.fstat(child_fd)
                    require_identity(before, opened, child_label)
                    if stat.S_IMODE(opened.st_mode) != directory_mode:
                        raise RunnerFailure(
                            f"corpus authority directory mode differs: {child_label}"
                        )
                    entries.append(
                        {
                            "path": child_label,
                            "kind": "directory",
                            "mode": directory_mode,
                        }
                    )
                    walk(child_fd, child_relative, directory_mode)
                    if stat_identity(opened) != stat_identity(os.fstat(child_fd)):
                        raise RunnerFailure(
                            "corpus authority directory changed during snapshot: "
                            f"{child_label}"
                        )
                finally:
                    os.close(child_fd)
            elif stat.S_ISREG(before.st_mode):
                if (
                    before.st_nlink != 1
                    or stat.S_IMODE(before.st_mode) != file_mode
                ):
                    raise RunnerFailure(
                        f"corpus authority file mode/link differs: {child_label}"
                    )
                file_fd = os.open(name, file_flags, dir_fd=directory_fd)
                try:
                    opened = os.fstat(file_fd)
                    require_identity(before, opened, child_label)
                    digest = hashlib.sha256()
                    byte_count = 0
                    while True:
                        chunk = os.read(file_fd, 1024 * 1024)
                        if not chunk:
                            break
                        digest.update(chunk)
                        byte_count += len(chunk)
                    if (
                        stat_identity(opened) != stat_identity(os.fstat(file_fd))
                        or byte_count != opened.st_size
                    ):
                        raise RunnerFailure(
                            "corpus authority file changed during snapshot: "
                            f"{child_label}"
                        )
                finally:
                    os.close(file_fd)
                identity = (opened.st_dev, opened.st_ino)
                if identity in file_identities:
                    raise RunnerFailure(
                        f"corpus authority file identity repeats: {child_label}"
                    )
                file_identities.add(identity)
                entries.append(
                    {
                        "path": child_label,
                        "kind": "file",
                        "mode": file_mode,
                        "bytes": byte_count,
                        "sha256": digest.hexdigest(),
                    }
                )
            else:
                raise RunnerFailure(
                    f"corpus authority contains non-file: {child_label}"
                )
            path_after = os.stat(
                name, dir_fd=directory_fd, follow_symlinks=False
            )
            if stat_identity(before) != stat_identity(path_after):
                raise RunnerFailure(
                    f"corpus authority entry changed during snapshot: {child_label}"
                )
        if sorted(os.listdir(directory_fd)) != names:
            raise RunnerFailure(
                f"corpus authority names changed during snapshot: {label}"
            )
        if stat_identity(directory_before) != stat_identity(os.fstat(directory_fd)):
            raise RunnerFailure(
                f"corpus authority directory changed during snapshot: {label}"
            )

    try:
        current = os.open("/", directory_flags)
        chain.append(current)
        root_info = os.fstat(current)
        chain_identities.append((root_info.st_dev, root_info.st_ino))
        for component in lexical_root.parts[1:]:
            before = os.stat(component, dir_fd=current, follow_symlinks=False)
            if stat.S_ISLNK(before.st_mode) or not stat.S_ISDIR(before.st_mode):
                raise RunnerFailure(
                    f"corpus authority path component differs: {component}"
                )
            next_fd = os.open(component, directory_flags, dir_fd=current)
            try:
                opened = os.fstat(next_fd)
                require_identity(before, opened, str(lexical_root))
            except BaseException:
                os.close(next_fd)
                raise
            chain.append(next_fd)
            current = next_fd
            chain_identities.append((opened.st_dev, opened.st_ino))
        walk(current, Path(), root_mode)

        verify = os.open("/", directory_flags)
        try:
            if (os.fstat(verify).st_dev, os.fstat(verify).st_ino) != chain_identities[0]:
                raise RunnerFailure("corpus authority filesystem root changed")
            for index, component in enumerate(lexical_root.parts[1:], start=1):
                before = os.stat(
                    component, dir_fd=verify, follow_symlinks=False
                )
                next_fd = os.open(component, directory_flags, dir_fd=verify)
                try:
                    opened = os.fstat(next_fd)
                    require_identity(before, opened, str(lexical_root))
                except BaseException:
                    os.close(next_fd)
                    raise
                previous_fd = verify
                verify = next_fd
                os.close(previous_fd)
                if (opened.st_dev, opened.st_ino) != chain_identities[index]:
                    raise RunnerFailure("corpus authority path identity changed")
        finally:
            os.close(verify)
    finally:
        for descriptor in reversed(chain):
            os.close(descriptor)
    entries.sort(key=lambda item: Path(str(item["path"])))
    if not any(item["kind"] == "file" for item in entries):
        raise RunnerFailure("corpus authority tree contains no regular file")
    return entries


def make_tree_read_only(root: Path) -> None:
    for path in sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        metadata = path.lstat()
        if stat.S_ISREG(metadata.st_mode):
            path.chmod(0o444)
        elif stat.S_ISDIR(metadata.st_mode):
            path.chmod(0o555)
        else:
            raise RunnerFailure(f"unsupported corpus entry: {path}")
    root.chmod(0o555)


def _open_exclusive(path: Path, mode: int = 0o600) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    return os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)


def _write_all(descriptor: int, payload: bytes, context: str) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise RunnerFailure(f"{context} write made no progress")
        view = view[written:]


def create_sealable_memfd(name: str) -> int:
    """Create one Linux memfd on Python builds that omit os.memfd_create."""

    native = getattr(os, "memfd_create", None)
    flags = MFD_CLOEXEC | MFD_ALLOW_SEALING
    if callable(native):
        return int(native(name, flags))
    libc = ctypes.CDLL(None, use_errno=True)
    function = getattr(libc, "memfd_create", None)
    if function is None:
        raise RunnerFailure("Linux memfd_create is unavailable", exit_code=2)
    function.argtypes = (ctypes.c_char_p, ctypes.c_uint)
    function.restype = ctypes.c_int
    descriptor = int(function(name.encode("utf-8"), flags))
    if descriptor < 0:
        error = ctypes.get_errno()
        raise RunnerFailure(
            f"memfd_create failed: errno={error}", exit_code=30
        )
    return descriptor


def _open_canonical_artifact(path: Path, context: str) -> int:
    path_text = str(path)
    if (
        not path.is_absolute()
        or path_text.startswith("//")
        or ".." in path.parts
        or str(Path(path_text)) != path_text
    ):
        raise RunnerFailure(f"{context} path is not canonical absolute")
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
        raise RunnerFailure(f"cannot open {context}: {error}") from error
    os.close(parent)
    return descriptor


def _finalize_immutable_artifact(path: Path, context: str) -> None:
    descriptor = _open_canonical_artifact(path, context)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise RunnerFailure(f"{context} is not a regular file")
        os.fsync(descriptor)
        os.fchmod(descriptor, 0o444)
        os.fsync(descriptor)
        if stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o444:
            raise RunnerFailure(f"{context} did not finalize as 0444")
    finally:
        os.close(descriptor)


def _immutable_artifact_binding(path: Path, context: str) -> dict[str, Any]:
    """Snapshot one finalized artifact through one no-follow descriptor."""

    path_text = str(path)
    descriptor = _open_canonical_artifact(path, context)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or stat.S_IMODE(before.st_mode) != 0o444:
            raise RunnerFailure(f"{context} is not one immutable 0444 file")
        digest = hashlib.sha256()
        size = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
        after = os.fstat(descriptor)
        identity = lambda value: (
            value.st_dev,
            value.st_ino,
            value.st_size,
            value.st_mtime_ns,
            stat.S_IMODE(value.st_mode),
        )
        if identity(before) != identity(after) or size != before.st_size:
            raise RunnerFailure(f"{context} changed during its one-fd snapshot")
    finally:
        os.close(descriptor)
    return {
        "path": path_text,
        "sha256": digest.hexdigest(),
        "bytes": size,
        "mode": 0o444,
    }


def _wait_exact_process(
    process: subprocess.Popen[Any], executable: Executable, timeout: float = 5.0
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RunnerFailure(
                f"process {executable.name} exited before binding: {process.returncode}"
            )
        try:
            observed = parse_proc_stat(process.pid)
            proc_exe = Path("/proc") / str(process.pid) / "exe"
            observed_exe_sha256 = sha256(proc_exe)
            last = observed
        except (FileNotFoundError, OSError):
            time.sleep(0.001)
            continue
        if (
            observed["comm"] == executable.comm
            and observed_exe_sha256 == executable.sha256
        ):
            return observed
        time.sleep(0.001)
    raise RunnerFailure(
        f"process {executable.name} binding did not stabilize: {last}"
    )


def _read_ack_line(descriptor: int, timeout: float = 5.0) -> bytes:
    """Read one exact ACK frame emitted by the pinned perf binary."""

    deadline = time.monotonic() + timeout
    payload = bytearray()
    while len(payload) < len(PERF_ACK_WIRE):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RunnerFailure("perf control acknowledgement timed out")
        ready, _, _ = select.select([descriptor], [], [], remaining)
        if not ready:
            raise RunnerFailure("perf control acknowledgement timed out")
        chunk = os.read(descriptor, len(PERF_ACK_WIRE) - len(payload))
        if not chunk:
            raise RunnerFailure("perf control acknowledgement closed early")
        payload.extend(chunk)
    if bytes(payload) != PERF_ACK_WIRE:
        raise RunnerFailure(f"perf control acknowledgement differs: {bytes(payload)!r}")
    return bytes(payload)


class RunnerOwnedProfileSession:
    """Own one exact perf or strace helper around one benchmark child."""

    def __init__(
        self,
        *,
        plan: Any,
        output_dir: Path,
        ordinal: int,
        tools: dict[str, Executable],
        support_files: dict[str, SupportFile],
        profile_adapter: Any,
        permission_result: str,
    ) -> None:
        self.plan = plan
        self.track = str(plan.track or plan.context.get("smoke_target"))
        self.output_dir = output_dir
        self.ordinal = ordinal
        self.tools = tools
        self.support_files = support_files
        self.profile_adapter = profile_adapter
        self.permission_result = permission_result
        self.child_identity: dict[str, Any] | None = None
        self.child_control_fd: int | None = None
        self.process: subprocess.Popen[Any] | None = None
        self.helper_identity: dict[str, Any] | None = None
        self.helper_argv: tuple[str, ...] = ()
        self.helper_kind = "perf" if self.track == "cpu_profiles" else "strace"
        self.perf_available = self.permission_result.startswith("available;")
        base = output_dir / "profiles" / f"{ordinal:05d}-{self.track}"
        self.raw_path = base.with_suffix(".perf.csv" if self.helper_kind == "perf" else ".strace")
        self.stderr_path = base.with_suffix(".stderr")
        self.ack_path = base.with_suffix(".perf.ack")
        self.control_write: int | None = None
        self.ack_read: int | None = None
        self.helper_control_read: int | None = None
        self.helper_ack_write: int | None = None
        self.ack_ledger_fd: int | None = None
        self.phase_events: dict[str, dict[str, Any]] = {}
        self.perf_control_events: list[dict[str, Any]] = []
        self.start_nonce: str | None = None
        self.finished = False
        self.phase_names: list[str] = []
        if self.helper_kind == "strace":
            self._spawn_waiting_tracer()
        elif self.perf_available:
            self._prepare_perf_channels()

    def _prepare_perf_channels(self) -> None:
        if any(
            descriptor is not None
            for descriptor in (
                self.control_write,
                self.ack_read,
                self.helper_control_read,
                self.helper_ack_write,
                self.ack_ledger_fd,
            )
        ):
            raise RunnerFailure("perf channels were prepared twice")
        descriptors: list[int] = []
        try:
            helper_control_read, child_control_write = os.pipe()
            descriptors.extend((helper_control_read, child_control_write))
            child_ack_read, helper_ack_write = os.pipe()
            descriptors.extend((child_ack_read, helper_ack_write))
            ledger = _open_exclusive(self.ack_path)
        except BaseException:
            for descriptor in descriptors:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
            raise
        self.helper_control_read = helper_control_read
        self.control_write = child_control_write
        self.ack_read = child_ack_read
        self.helper_ack_write = helper_ack_write
        self.ack_ledger_fd = ledger

    def _spawn_waiting_tracer(self) -> None:
        runtime = self.tools["strace_launcher_runtime"]
        launcher = self.support_files["strace_attach"]
        tracer = self.tools["strace"]
        command_read_value, command_write_value = os.pipe()
        command_read: int | None = command_read_value
        command_write: int | None = command_write_value
        stderr_fd: int | None = None
        process: subprocess.Popen[Any] | None = None
        argv = (
            str(runtime.path),
            str(launcher.path),
            "--command-fd",
            str(command_read),
            "--strace",
            str(tracer.path),
            "--trace-output",
            str(self.raw_path),
            "--trace-set",
            ",".join(TRACE_SYSCALLS),
            "--waiting-comm",
            runtime.comm,
        )
        try:
            stderr_fd = _open_exclusive(self.stderr_path)
            process = subprocess.Popen(
                argv,
                executable=str(runtime.path),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=stderr_fd,
                env={"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "TZ": "UTC"},
                pass_fds=(command_read,),
                start_new_session=True,
            )
            os.close(command_read)
            command_read = None
            os.close(stderr_fd)
            stderr_fd = None
            helper_identity = _wait_exact_process(process, runtime)
        except BaseException as error:
            cleanup_errors: list[str] = []
            for label, descriptor in (
                ("command read", command_read),
                ("command write", command_write),
                ("stderr", stderr_fd),
            ):
                if descriptor is None:
                    continue
                try:
                    os.close(descriptor)
                except OSError as cleanup_error:
                    cleanup_errors.append(f"close {label}: {cleanup_error!r}")
            if process is not None:
                try:
                    terminate_process_group(process)
                except BaseException as cleanup_error:
                    cleanup_errors.append(
                        f"terminate strace launcher: {cleanup_error!r}"
                    )
            if cleanup_errors:
                raise RunnerFailure(
                    "cannot initialize strace launcher; cleanup failures: "
                    f"{cleanup_errors}",
                    exit_code=30,
                ) from error
            raise
        if process is None or command_write is None:
            raise RunnerFailure("strace launcher initialization lost ownership")
        self.process = process
        self.control_write = command_write
        self.helper_argv = argv
        self.helper_identity = helper_identity

    def environment_overrides(self) -> dict[str, str]:
        if self.helper_kind == "strace":
            if self.helper_identity is None:
                return {}
            return {"ASTERISM_REBASELINE_PTRACER_PID": str(self.helper_identity["pid"])}
        result = {PERF_PERMISSION_ENVIRONMENT: self.permission_result}
        if not self.perf_available:
            return result
        if self.control_write is None or self.ack_read is None or self.ack_ledger_fd is None:
            raise RunnerFailure("perf child descriptors are absent")
        result.update(
            {
                PERF_CHILD_FD_ENVIRONMENT[0]: str(self.control_write),
                PERF_CHILD_FD_ENVIRONMENT[1]: str(self.ack_read),
                PERF_CHILD_FD_ENVIRONMENT[2]: str(self.ack_ledger_fd),
            }
        )
        return result

    def child_pass_fds(self) -> tuple[int, ...]:
        if self.helper_kind != "perf" or not self.perf_available:
            return ()
        if self.control_write is None or self.ack_read is None or self.ack_ledger_fd is None:
            raise RunnerFailure("perf inherited descriptors are absent")
        return (self.control_write, self.ack_read, self.ack_ledger_fd)

    def bind_child(self, identity: dict[str, Any], *, control_fd: int) -> None:
        if isinstance(control_fd, bool) or not isinstance(control_fd, int) or control_fd <= 0:
            raise RunnerFailure("profile child control descriptor is invalid")
        self.child_identity = identity
        self.child_control_fd = control_fd

    def _spawn_perf(self) -> None:
        if self.child_identity is None:
            raise RunnerFailure("perf session lacks a bound child")
        if self.helper_control_read is None or self.helper_ack_write is None:
            raise RunnerFailure("perf helper descriptors are absent")
        perf = self.tools["perf"]
        control_read = self.helper_control_read
        ack_write = self.helper_ack_write
        owned: dict[str, int | None] = {
            "control read": control_read,
            "ack write": ack_write,
            "stat": None,
            "stderr": None,
        }
        process: subprocess.Popen[Any] | None = None
        argv: tuple[str, ...] = ()

        def close_owned(name: str, session_attribute: str | None = None) -> None:
            descriptor = owned[name]
            if descriptor is None:
                return
            os.close(descriptor)
            owned[name] = None
            if (
                session_attribute is not None
                and getattr(self, session_attribute) == descriptor
            ):
                setattr(self, session_attribute, None)

        try:
            owned["stat"] = _open_exclusive(self.raw_path)
            owned["stderr"] = _open_exclusive(self.stderr_path)
            stat_fd = owned["stat"]
            stderr_fd = owned["stderr"]
            if stat_fd is None or stderr_fd is None:
                raise RunnerFailure("perf artifact descriptors are absent")
            argv = (
                str(perf.path),
                "stat",
                "-x,",
                "--no-big-num",
                "--inherit",
                "--delay=-1",
                "--event",
                ",".join(PERF_EVENTS),
                "--control",
                f"fd:{control_read},{ack_write}",
                "--log-fd",
                str(stat_fd),
                "--pid",
                str(self.child_identity["pid"]),
            )
            process = subprocess.Popen(
                argv,
                executable=str(perf.path),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=stderr_fd,
                env={"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "TZ": "UTC"},
                pass_fds=(control_read, ack_write, stat_fd),
                start_new_session=True,
            )
            close_owned("control read", "helper_control_read")
            close_owned("ack write", "helper_ack_write")
            close_owned("stat")
            close_owned("stderr")
            helper_identity = _wait_exact_process(process, perf)
            # A stable, live helper while its tracee is parked proves perf opened
            # the target before runtime thread births.  Any permission/open error
            # exits immediately and is observed here.
            deadline = time.monotonic() + 0.4
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RunnerFailure(
                        f"perf attach failed before start: {process.returncode}"
                    )
                time.sleep(0.005)
        except BaseException as error:
            cleanup_errors: list[str] = []
            for name, session_attribute in (
                ("control read", "helper_control_read"),
                ("ack write", "helper_ack_write"),
                ("stat", None),
                ("stderr", None),
            ):
                try:
                    close_owned(name, session_attribute)
                except OSError as cleanup_error:
                    cleanup_errors.append(f"close {name}: {cleanup_error!r}")
            if process is not None:
                try:
                    terminate_process_group(process)
                except BaseException as cleanup_error:
                    # Keep the live helper reachable so the enclosing session's
                    # abort path can retry cleanup instead of orphaning it.
                    self.process = process
                    self.helper_argv = argv
                    cleanup_errors.append(f"terminate perf: {cleanup_error!r}")
            if cleanup_errors:
                raise RunnerFailure(
                    "cannot initialize perf helper; cleanup failures: "
                    f"{cleanup_errors}",
                    exit_code=30,
                ) from error
            raise
        if process is None or not argv:
            raise RunnerFailure("perf helper initialization lost ownership")
        self.process = process
        self.helper_argv = argv
        self.helper_identity = helper_identity

    def _attach_strace(self) -> None:
        if (
            self.child_identity is None
            or self.process is None
            or self.control_write is None
        ):
            raise RunnerFailure("strace session lacks launcher/child identity")
        os.write(
            self.control_write,
            canonical_json_bytes(
                {
                    "pid": self.child_identity["pid"],
                    "starttime_ticks": self.child_identity["starttime_ticks"],
                }
            ),
        )
        os.close(self.control_write)
        self.control_write = None
        tracer = self.tools["strace"]
        self.helper_identity = _wait_exact_process(self.process, tracer)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            status = (Path("/proc") / str(self.child_identity["pid"]) / "status").read_text()
            match = re.search(r"^TracerPid:\s+(\d+)$", status, re.MULTILINE)
            if match and int(match.group(1)) == self.process.pid:
                return
            if self.process.poll() is not None:
                raise RunnerFailure(f"strace attach failed: {self.process.returncode}")
            time.sleep(0.001)
        raise RunnerFailure("strace did not become the exact benchmark tracer")

    def helper_identities(self) -> list[dict[str, Any]]:
        return [self.helper_identity] if self.helper_identity is not None else []

    def capture_phase(self, phase_name: str, event: dict[str, Any]) -> None:
        self.phase_names.append(phase_name)
        if phase_name in self.phase_events:
            raise RunnerFailure(f"duplicate profile phase event {phase_name}")
        self.phase_events[phase_name] = json.loads(canonical_json_bytes(event))
        if phase_name == "boot":
            if self.helper_kind == "perf":
                if self.perf_available:
                    self._spawn_perf()
            else:
                self._attach_strace()
        if phase_name == "measured" and self.helper_kind == "perf":
            disable = event.get("perf_disable")
            if not self.perf_available:
                if disable is not None:
                    raise RunnerFailure("unavailable perf child emitted disable evidence")
            else:
                expected = {
                    "command",
                    "nonce",
                    "sent_monotonic_ns",
                    "ack",
                    "ack_received_monotonic_ns",
                }
                if (
                    not isinstance(disable, dict)
                    or set(disable) != expected
                    or disable.get("command") != "disable"
                    or disable.get("nonce") != self.start_nonce
                    or disable.get("ack") != "ack"
                    or isinstance(disable.get("sent_monotonic_ns"), bool)
                    or not isinstance(disable.get("sent_monotonic_ns"), int)
                    or isinstance(disable.get("ack_received_monotonic_ns"), bool)
                    or not isinstance(disable.get("ack_received_monotonic_ns"), int)
                    or disable["sent_monotonic_ns"] < event.get("t1_monotonic_ns", -1)
                    or disable["ack_received_monotonic_ns"]
                    <= disable["sent_monotonic_ns"]
                    or disable["ack_received_monotonic_ns"]
                    > event.get("counter_end_monotonic_ns", -1)
                ):
                    raise RunnerFailure("child perf disable evidence differs")
                self.perf_control_events.append(
                    json.loads(canonical_json_bytes(disable))
                )
        if self.process is not None and self.process.poll() is not None:
            raise RunnerFailure(
                f"{self.helper_kind} exited during {phase_name}: {self.process.returncode}"
            )

    def begin(self, *, start_nonce: str) -> None:
        if not SHA256_RE.fullmatch(start_nonce):
            raise RunnerFailure("profile start nonce is malformed")
        if self.start_nonce is not None:
            raise RunnerFailure("profile window began twice")
        self.start_nonce = start_nonce
        if self.helper_kind == "perf" and self.perf_available:
            if (
                self.control_write is None
                or self.ack_read is None
                or self.ack_ledger_fd is None
            ):
                raise RunnerFailure("perf control descriptors are absent")
            sent = time.monotonic_ns()
            _write_all(self.control_write, b"enable\n", "perf enable command")
            wire_ack = _read_ack_line(self.ack_read)
            received = time.monotonic_ns()
            # The pinned perf wire frame includes a trailing NUL.  The reviewed
            # shared-offset ledger format intentionally stores only ``ack\n``.
            ledger_ack = wire_ack[:-1]
            if ledger_ack != PERF_ACK_LEDGER_ENTRY:
                raise RunnerFailure("perf ledger acknowledgement differs")
            _write_all(self.ack_ledger_fd, ledger_ack, "perf runner ACK ledger")
            os.fsync(self.ack_ledger_fd)
            self.perf_control_events.append(
                {
                    "command": "enable",
                    "nonce": start_nonce,
                    "sent_monotonic_ns": sent,
                    "ack": ledger_ack.rstrip(b"\n").decode("ascii"),
                    "ack_received_monotonic_ns": received,
                }
            )
            os.close(self.control_write)
            os.close(self.ack_read)
            os.close(self.ack_ledger_fd)
            self.control_write = None
            self.ack_read = None
            self.ack_ledger_fd = None

    def end(self, *, end_event: dict[str, Any]) -> None:
        if end_event.get("phase") not in {"opened", "measured"}:
            raise RunnerFailure("profile end event is not a frozen boundary")

    def _wait_helper(self) -> tuple[int, dict[str, Any]]:
        if self.process is None or self.helper_identity is None:
            raise RunnerFailure("profile helper was never spawned")
        if self.control_write is not None:
            os.close(self.control_write)
            self.control_write = None
        try:
            status = self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            terminate_process_group(self.process)
            raise RunnerFailure(f"{self.helper_kind} did not exit with its tracee")
        reject_orphan_process_group(
            self.process, f"{self.helper_kind} profile helper"
        )
        record = {
            "kind": self.helper_kind,
            "identity": self.helper_identity,
            "argv": list(self.helper_argv),
            "exit_status": status,
            "waited_pid": self.process.pid,
            "process_group_absent": True,
        }
        if status != 0:
            raise RunnerFailure(f"profile helper reaping failed: {record}")
        return status, record

    def abort(self) -> None:
        """Best-effort fail-stop cleanup which never masks the primary error."""

        for descriptor_name in (
            "control_write",
            "ack_read",
            "helper_control_read",
            "helper_ack_write",
            "ack_ledger_fd",
        ):
            descriptor = getattr(self, descriptor_name)
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
                setattr(self, descriptor_name, None)
        if self.process is not None:
            try:
                terminate_process_group(self.process)
            except (OSError, subprocess.TimeoutExpired, RunnerFailure):
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                except OSError:
                    pass
                try:
                    self.process.wait(timeout=2)
                except (OSError, subprocess.TimeoutExpired):
                    pass

    def finish(self) -> dict[str, Any]:
        if self.finished:
            raise RunnerFailure("profile tool session was consumed twice")
        self.finished = True
        if self.helper_kind == "perf" and not self.perf_available:
            return {
                "inputs": {
                    "perf_permission": self.permission_result,
                    "perf_control_events": [],
                    "perf_raw_artifacts": {},
                },
                "helper_records": [],
            }
        _, helper_record = self._wait_helper()
        if self.ack_read is not None:
            os.close(self.ack_read)
            self.ack_read = None
        finalized = [self.raw_path, self.stderr_path]
        if self.helper_kind == "perf":
            finalized.append(self.ack_path)
        for path in finalized:
            _finalize_immutable_artifact(path, f"profile artifact {path.name}")
        fsync_dir(self.raw_path.parent)
        if self.helper_kind == "perf":
            if len(self.perf_control_events) != 2:
                raise RunnerFailure("perf control event ledger cardinality differs")
            stat_binding = _immutable_artifact_binding(
                self.raw_path, "perf stat artifact"
            )
            ack_binding = _immutable_artifact_binding(
                self.ack_path, "perf ACK ledger"
            )
            if (
                ack_binding["bytes"] != len(b"ack\nack\n")
                or ack_binding["sha256"] != sha256_bytes(b"ack\nack\n")
            ):
                raise RunnerFailure("perf shared-offset ACK ledger differs")
            inputs = {
                "perf_permission": self.permission_result,
                "perf_control_events": json.loads(
                    canonical_json_bytes(self.perf_control_events)
                ),
                "perf_raw_artifacts": {
                    "stat": stat_binding,
                    "ack": ack_binding,
                },
            }
        else:
            markers = self.plan.context.get("trace_path_markers")
            if markers is None:
                markers = self.plan.context.get("variant_trace_path_markers")
            if (
                not isinstance(markers, dict)
                or set(markers) != {"log", "metadata"}
                or not all(
                    isinstance(value, list)
                    and all(
                        isinstance(item, dict)
                        and set(item) == {"kind", "path"}
                        and item.get("kind")
                        in {"exact", "file_prefix", "directory_prefix"}
                        and isinstance(item.get("path"), str)
                        and item["path"].startswith("/")
                        for item in value
                    )
                    for value in markers.values()
                )
                or not markers["log"] + markers["metadata"]
            ):
                raise RunnerFailure("source-approved trace path markers are absent")
            ready_event = self.phase_events.get("ready")
            measured_event = self.phase_events.get("measured")
            if ready_event is None or measured_event is None or self.child_identity is None:
                raise RunnerFailure("strace exact ready/measured boundary is absent")
            if self.child_control_fd is None:
                raise RunnerFailure("strace child control descriptor is absent")
            inputs = {
                "trace_raw_artifact": _immutable_artifact_binding(
                    self.raw_path, "strace raw artifact"
                ),
                "log_path_markers": json.loads(
                    canonical_json_bytes(markers["log"])
                ),
                "metadata_path_markers": json.loads(
                    canonical_json_bytes(markers["metadata"])
                ),
            }
        return {"inputs": inputs, "helper_records": [helper_record]}


class RunnerOwnedProfileTools:
    REQUIRED = frozenset(
        {"perf", "strace", "strace_launcher_runtime"}
    )

    def __init__(self) -> None:
        self.permission_result: str | None = None

    def preflight(
        self,
        *,
        prepared: Prepared,
        output_dir: Path,
        profile_adapter: Any,
    ) -> dict[str, Any]:
        missing = self.REQUIRED - set(prepared.tools)
        if missing:
            raise RunnerFailure(
                f"prepared profile tools omit {sorted(missing)} before row zero"
            )
        perf = prepared.tools["perf"]
        tracer = prepared.tools["strace"]
        commands: list[dict[str, Any]] = []
        for name, argv in (
            ("perf_version", (str(perf.path), "--version")),
            ("strace_version", (str(tracer.path), "--version")),
        ):
            completed = subprocess.run(
                argv,
                check=False,
                capture_output=True,
                env={"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "TZ": "UTC"},
            )
            commands.append(
                {
                    "name": name,
                    "argv": list(argv),
                    "exit_status": completed.returncode,
                    "stdout": completed.stdout.decode("utf-8", "backslashreplace"),
                    "stderr": completed.stderr.decode("utf-8", "backslashreplace"),
                }
            )
            if completed.returncode != 0:
                raise RunnerFailure(f"{name} failed before row zero")
        perf_probe_path = output_dir / "profiles" / "perf-permission-probe.csv"
        perf_probe_stderr = output_dir / "profiles" / "perf-permission-probe.stderr"
        stderr_fd = _open_exclusive(perf_probe_stderr)
        stat_fd = _open_exclusive(perf_probe_path)
        probe_argv = (
            str(perf.path),
            "stat",
            "-x,",
            "--no-big-num",
            "--event",
            ",".join(PERF_EVENTS),
            "--timeout",
            "100",
            "--log-fd",
            str(stat_fd),
            "--pid",
            str(os.getpid()),
        )
        try:
            probe = subprocess.run(
                probe_argv,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=stderr_fd,
                pass_fds=(stat_fd,),
                env={"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "TZ": "UTC"},
            )
        finally:
            os.close(stat_fd)
            os.close(stderr_fd)
        paranoid = Path("/proc/sys/kernel/perf_event_paranoid").read_text().strip()
        if probe.returncode == 0:
            self.permission_result = (
                f"available;perf_event_paranoid={paranoid};scope=user-only"
            )
        else:
            self.permission_result = (
                "not_available;"
                f"perf_event_paranoid={paranoid};scope=user-only;exit_status={probe.returncode}"
            )
        for path in (perf_probe_path, perf_probe_stderr):
            path.chmod(0o444)
        value = {
            "schema": "bn-2l3n-profile-tools-preflight-v3",
            "protocol": PROTOCOL,
            "protocol_sha256": PROTOCOL_SHA256,
            "commands": commands,
            "perf_probe_argv": list(probe_argv),
            "perf_probe_exit_status": probe.returncode,
            "perf_permission": self.permission_result,
            "perf_probe_path": str(perf_probe_path.resolve()),
            "perf_probe_sha256": sha256(perf_probe_path),
            "perf_probe_stderr_path": str(perf_probe_stderr.resolve()),
            "perf_probe_stderr_sha256": sha256(perf_probe_stderr),
            "ptrace_scope": Path("/proc/sys/kernel/yama/ptrace_scope").read_text().strip(),
            "events": list(PERF_EVENTS),
            "trace_syscalls": list(TRACE_SYSCALLS),
            "trace_boundary_policy": (
                "exact-child-pid-control-fd-full-ready-measured-frames"
            ),
        }
        # The unavailable path is retained and normalized without fabricating
        # counter bytes or acknowledgements; adapter support is checked now.
        if probe.returncode != 0:
            profile_adapter.perf_profile_inputs(
                "", "", self.permission_result, control_events=[]
            )
        return value

    def prepare_child(
        self,
        *,
        plan: Any,
        output_dir: Path,
        ordinal: int,
        tools: dict[str, Executable],
        support_files: dict[str, SupportFile],
        profile_adapter: Any,
    ) -> RunnerOwnedProfileSession:
        if self.permission_result is None:
            raise RunnerFailure("profile tool preflight was not consumed")
        return RunnerOwnedProfileSession(
            plan=plan,
            output_dir=output_dir,
            ordinal=ordinal,
            tools=tools,
            support_files=support_files,
            profile_adapter=profile_adapter,
            permission_result=self.permission_result,
        )


@dataclass(frozen=True)
class ChildPlan:
    kind: str
    context: dict[str, Any]
    executable: Executable
    argv: tuple[str, ...]
    environment: dict[str, str]
    expected_records: int
    track: str | None
    store_path: Path | None
    require_store_after: bool
    timeout_seconds: int
    environment_ordinal: int | None = None
    store_absent_before: bool = True
    allowed_exit_statuses: tuple[int, ...] = (0,)
    # Whether the child speaks the control-socket handshake. Measured children
    # (variant binaries, correctness/fault) do; the runtime-role tool smokes
    # (evaluator/terminal_verifier), which only emit a canonical JSON line on
    # stdout, do not, and must run uncontrolled like the contract smokes.
    controlled: bool = True


class RebaselineRunner:
    def __init__(
        self,
        prepared: Prepared,
        output: Path,
        schema: EvidenceSchema,
        *,
        lock_path: Path = LOCK_PATH,
        scratch_root: Path = SCRATCH_ROOT,
        minimum_free_bytes: int = MIN_FREE_BYTES,
        minimum_free_inodes: int = MIN_FREE_INODES,
        maximum_load1: float = MAX_LOAD1,
        quiet_timeout_seconds: int = QUIET_TIMEOUT_SECONDS,
        quiet_poll_seconds: float = QUIET_POLL_SECONDS,
        row_timeout_seconds: int = ROW_TIMEOUT_SECONDS,
        load_reader: Callable[[], float] | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        profile_factory: Any | None = None,
        require_profile_factory: bool = True,
        schema_path: Path | None = None,
        profile_tool_driver: Any | None = None,
    ) -> None:
        self.prepared = prepared
        self.output = output
        self.schema = schema
        self.lock_path = lock_path
        self.scratch_root = scratch_root
        self.minimum_free_bytes = minimum_free_bytes
        self.minimum_free_inodes = minimum_free_inodes
        self.maximum_load1 = maximum_load1
        self.quiet_timeout_seconds = quiet_timeout_seconds
        self.quiet_poll_seconds = quiet_poll_seconds
        self.row_timeout_seconds = row_timeout_seconds
        self.load_reader = load_reader or self._read_load1
        self.sleep = sleep
        self.monotonic = monotonic
        self.profile_factory = profile_factory
        self.profile_adapter_module: Any | None = None
        self.profile_preflight: dict[str, Any] | None = None
        self.profile_tool_driver = profile_tool_driver
        self.require_profile_factory = require_profile_factory
        self.schema_path = (
            schema_path.resolve()
            if schema_path is not None
            else Path(__file__).with_name("evidence_schema.py").resolve()
        )
        self.phase = "constructed"
        self.attempt_nonce = secrets.token_hex(32)
        self.runner_identity = parse_proc_stat(os.getpid())
        self.runner_cmdline = read_proc_cmdline(os.getpid())
        self.active_child: dict[str, Any] | None = None
        self.active_executable: Executable | None = None
        self.active_helpers: list[dict[str, Any]] = []
        self.active_evaluator_process: subprocess.Popen[Any] | None = None
        self.active_evaluator_authority_fd: int | None = None
        self.lease_handle: Any | None = None
        self.lease: dict[str, Any] | None = None
        self.claim: dict[str, Any] | None = None
        self.run_claim_path: Path | None = None
        self.declaration_sha256: str | None = None
        # Protocol v4 §3: an output directory whose final component starts
        # with "rehearsal-" marks a rehearsal run — rows are non-evidence,
        # no DECLARED.txt or coordination attestation is required, and
        # quiet-guard misses warn instead of fail-stopping.
        self.rehearsal = output.name.startswith("rehearsal-")
        self.next_transition: dict[str, Any] | None = None
        self.guard_count = 0
        self.child_count = 0
        self.resource_count = 0
        self.correctness_observations: dict[
            tuple[str, str, str, str, str], dict[str, Any]
        ] = {}
        self.correctness_boundedness: dict[str, dict[str, Any]] = {}
        # This execution-time authority is deliberately never serialized into
        # config/provenance/artifacts.  It is sealed into a one-shot memfd only
        # at the real evaluator boundary.
        self.corpus_execution_authority: list[dict[str, Any]] = []
        self.semantic_manifests: list[RetainedSemanticManifest] = []
        self.semantic_manifest_counts: dict[str, int] = {}
        try:
            self.semantic_manifests, self.semantic_manifest_counts = (
                retain_prepared_semantic_manifests(prepared)
            )
            self._initialize_after_semantic_retention()
        except BaseException as error:
            secondary_errors = _final_verify_close_retained_manifests(
                self.semantic_manifests,
                error,
            )
            for secondary in secondary_errors:
                self._report_secondary_failure(
                    "runner construction semantic finalization",
                    RunnerFailure(secondary),
                )
            raise

    def _initialize_after_semantic_retention(self) -> None:
        """Finish fallible construction under explicit retained-FD cleanup."""

        self.frozen_files = self._frozen_file_bindings()
        self.config = config_for(self.prepared, self.schema)
        self.config["attempt_nonce"] = self.attempt_nonce
        # Protocol v4 §3: the run mode is stamped into config (and mirrored
        # into provenance) so every downstream consumer can distinguish
        # rehearsal rows from evidence without consulting directory names.
        self.config["rehearsal"] = self.rehearsal
        self.config_path = self.output / "config.json"
        self.provenance_path = self.output / "provenance.json"
        self.guard_manifest = self.output / "guard-manifest.jsonl"
        self.child_manifest = self.output / "child-manifest.jsonl"
        self.raw_manifest = self.output / "raw-manifest.json"
        self.resource_manifest = self.output / "resource-manifest.jsonl"
        self.correctness_manifest = self.output / "correctness-manifest.jsonl"
        self.auxiliary_manifest = self.output / "auxiliary-manifest.jsonl"
        self.lease_manifest = self.output / "lease-manifest.jsonl"
        self.attempt_scratch = self.scratch_root / "attempts" / self.attempt_nonce
        self.runtime_home = self.attempt_scratch / "home"
        self.initial_filesystem: dict[str, Any] | None = None
        self.initial_free_bytes: int | None = None
        self.initial_free_inodes: int | None = None
        self.prepared_tree_snapshot = (
            self._prepared_tree_state() if self.prepared.inputs else None
        )
        self.csv_paths = {
            track: self.output / filename for track, filename in CSV_FILENAMES.items()
        }
        transition_plans = (
            [*self.contract_plans(), *self.smoke_plans()]
            if self.prepared.tools
            else []
        )
        self.config["smoke_transitions"] = [
            {
                "id": (
                    f"contract-{plan.context['variant']}"
                    if plan.kind == "contract"
                    else str(plan.context["smoke_target"])
                    + "-"
                    + str(plan.context.get("variant", "tooling"))
                ),
                # This is the child context identity, including tooling smoke
                # children that execute against variant A's evidence surface.
                "variant": str(plan.context["variant"]),
                "argv": list(plan.argv),
            }
            for plan in transition_plans
        ]
        if self.prepared.tools:
            for smoke_id in (
                "smoke_reopen_seed",
                "smoke_reopen",
                "smoke_structural_reopen",
            ):
                self.config["smoke_transitions"].append(
                    {
                        "id": smoke_id,
                        "variant": "A",
                        "argv": list(self.prepared.variants["A"].evidence_argv),
                    }
                )
        if {"correctness", "fault"}.issubset(self.prepared.tools):
            self._refresh_correctness_execution()

    def __del__(self) -> None:
        for semantic_manifest in getattr(self, "semantic_manifests", ()):
            try:
                semantic_manifest.close()
            except BaseException:
                pass

    @staticmethod
    def _report_secondary_failure(context: str, error: BaseException) -> None:
        """Best-effort diagnostics which can never replace the primary outcome."""

        try:
            print(
                f"secondary failure during {context}: "
                f"{error.__class__.__name__}: {error}",
                file=sys.stderr,
                flush=True,
            )
        except BaseException:
            pass

    @staticmethod
    def _emit_primary_failure(failure: RunnerFailure) -> None:
        """Best-effort primary visibility when the durable log cannot publish."""

        print(f"fail-stop: {failure.reason}", flush=True)

    def publish_frozen_inputs(self) -> None:
        self._verify_prepared_tree()
        self._verify_prepared_input_metadata()
        bindings = (
            (self.prepared.path, self.output / "prepared-artifacts.json"),
            (self.prepared.source_approval, self.output / "source-approval.json"),
            (
                self.prepared.inputs["protocol"].path,
                self.output / "BN-2L3N-PROTOCOL.md",
            ),
            (
                self.prepared.inputs["historical_baseline"].path,
                self.output / "BN-2SU-FINAL.csv",
            ),
        )
        for source, destination in bindings:
            if destination.exists() or destination.is_symlink():
                raise RunnerFailure(f"frozen input destination is not fresh: {destination}")
            payload = source.read_bytes()
            exact_digest = {
                "BN-2L3N-PROTOCOL.md": PROTOCOL_SHA256,
                "BN-2SU-FINAL.csv": HISTORICAL_BASELINE_SHA256,
            }.get(destination.name)
            if exact_digest is not None and sha256_bytes(payload) != exact_digest:
                raise RunnerFailure(f"prepared input {destination.name} hash differs")
            atomic_write(destination, payload, mode=0o444)
            if sha256(destination) != sha256(source):
                raise RunnerFailure(f"frozen input copy differs: {source}")
            self.frozen_files[str(destination.resolve())] = sha256(destination)
        self._verify_prepared_input_metadata()
        self._verify_prepared_tree()

    def _load_profile_adapter(self) -> Any | None:
        if self.profile_adapter_module is not None:
            return self.profile_adapter_module
        if self.profile_factory is None:
            support = self.prepared.support_files.get("profile_adapter")
            path = (
                support.path
                if support is not None
                else Path(__file__).with_name("profile_adapters.py").resolve()
            )
            if not path.is_file():
                if self.require_profile_factory:
                    raise RunnerFailure(f"profile adapter is absent: {path}")
                return None
            spec = importlib.util.spec_from_file_location("asterism_profile_adapters", path)
            if spec is None or spec.loader is None:
                raise RunnerFailure(f"cannot load profile adapter: {path}")
            module = importlib.util.module_from_spec(spec)
            previous = sys.dont_write_bytecode
            sys.dont_write_bytecode = True
            sys.modules[spec.name] = module
            try:
                spec.loader.exec_module(module)
            except BaseException:
                sys.modules.pop(spec.name, None)
                raise
            finally:
                sys.dont_write_bytecode = previous
            factory = module.ProfileCoordinator
            self.profile_adapter_module = module
            self.profile_factory = factory
            self.frozen_files[str(path)] = sha256(path)
            return module
        return self.profile_factory

    @staticmethod
    def _profile_tool_binding(executable: Executable) -> dict[str, Any]:
        return {
            "path": str(executable.path),
            "sha256": executable.sha256,
            "executable_mode": executable.mode,
            "comm": executable.comm,
        }

    def _profile_authority(
        self,
        plan: ChildPlan,
        identity: dict[str, Any],
        *,
        child_ordinal: int,
        context_sha256: str,
        control_fd: int,
    ) -> dict[str, Any]:
        profile_track = plan.track or plan.context.get("profile_smoke_track")
        if profile_track is None:
            raise RunnerFailure("profile authority track is absent")
        variant_name = str(plan.context.get("variant", ""))
        variant = self.prepared.variants.get(variant_name)
        if variant is None or variant.executable != plan.executable:
            raise RunnerFailure("profile authority variant/executable differs")
        provider = self._load_profile_adapter()
        authority_schema = getattr(provider, "AUTHORITY_SCHEMA", None)
        if authority_schema != "bn-2l3n-profile-authority-v3":
            raise RunnerFailure("profile authority schema differs")
        required_tool_names = (
            ("perf",)
            if profile_track == "cpu_profiles"
            else ("strace", "strace_launcher_runtime")
            if profile_track in {"syscall_profiles", "structural_traces"}
            else ()
        )
        missing_tools = set(required_tool_names) - set(self.prepared.tools)
        if missing_tools:
            raise RunnerFailure(
                f"profile authority omits tools {sorted(missing_tools)}"
            )
        adapter = self.prepared.support_files.get("profile_adapter")
        if adapter is None:
            raise RunnerFailure("profile adapter binding is absent")
        if profile_track == "cpu_profiles":
            perf_permission_result = getattr(
                self.profile_tool_driver, "permission_result", None
            )
            if not isinstance(perf_permission_result, str):
                raise RunnerFailure("profile perf permission authority is absent")
        else:
            perf_permission_result = "not_applicable"
        row_ordinal = plan.context.get("row_ordinal", "not_applicable")
        attempt_prepared = (self.output / "prepared-artifacts.json").resolve(
            strict=True
        )
        attempt_approval = (self.output / "source-approval.json").resolve(
            strict=True
        )
        if (
            sha256(attempt_prepared) != self.prepared.digest
            or sha256(attempt_approval) != self.prepared.source_approval_sha256
        ):
            raise RunnerFailure("attempt profile authority copies differ")
        authority = {
            "schema": authority_schema,
            "protocol": PROTOCOL,
            "protocol_sha256": PROTOCOL_SHA256,
            "attempt_nonce": self.attempt_nonce,
            "child_ordinal": child_ordinal,
            "row_ordinal": row_ordinal,
            "context_sha256": context_sha256,
            "prepared_artifacts_path": str(attempt_prepared),
            "prepared_artifacts_sha256": self.prepared.digest,
            "source_approval_path": str(attempt_approval),
            "source_approval_sha256": self.prepared.source_approval_sha256,
            "profile_adapter_path": str(adapter.path),
            "profile_adapter_sha256": adapter.sha256,
            "profile_tools": {
                name: self._profile_tool_binding(self.prepared.tools[name])
                for name in required_tool_names
            },
            "perf_permission_result": perf_permission_result,
            "variant": variant_name,
            "source_commit": variant.product_commit,
            "source_tree": variant.product_tree,
            "track": str(profile_track),
            "executable_path": str(plan.executable.path),
            "executable_sha256": plan.executable.sha256,
            "executable_mode": plan.executable.mode,
            "executable_comm": plan.executable.comm,
            "child_pid": identity["pid"],
            "child_start_ticks": identity["starttime_ticks"],
            "control_fd": control_fd,
        }
        expected_fields = set(getattr(provider, "_AUTHORITY_FIELDS", ()))
        if not expected_fields or set(authority) != expected_fields:
            raise RunnerFailure(
                "profile authority fields differ: "
                f"missing={sorted(expected_fields - set(authority))} "
                f"extra={sorted(set(authority) - expected_fields)}"
            )
        return json.loads(canonical_json_bytes(authority))

    def _profile_for(
        self, plan: ChildPlan, child_pid: int, authority: dict[str, Any]
    ) -> Any | None:
        profile_track = plan.track or plan.context.get("profile_smoke_track")
        if profile_track is None:
            return None
        provider = self._load_profile_adapter()
        if provider is None:
            return None
        factory = self.profile_factory
        return factory.for_child(
            child_pid,
            str(plan.context["variant"]),
            str(profile_track),
            authority=authority,
            context=plan.context,
        )

    def prepare_profile_preflight(self) -> None:
        self.phase = "profile_preflight"
        self.assert_host_admission_stable()
        provider = self._load_profile_adapter()
        if provider is None:
            raise RunnerFailure("profile preflight adapter is unavailable")
        function = getattr(provider, "preflight_profile_contract", None)
        contract_function = getattr(provider, "profile_contract", None)
        if not callable(function) or not callable(contract_function):
            raise RunnerFailure("profile adapter omits preflight_profile_contract")
        value = function()
        contract = contract_function()
        expected_contract_function = getattr(
            self.schema, "expected_profile_contract", None
        )
        expected_fields = {
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
        }
        if not isinstance(value, dict) or set(value) != expected_fields:
            raise RunnerFailure("profile preflight did not return the exact object")
        if not isinstance(contract, dict) or not callable(expected_contract_function):
            raise RunnerFailure("shared profile adapter contract authority is absent")
        expected_contract = expected_contract_function()
        if not isinstance(expected_contract, dict) or contract != expected_contract:
            raise RunnerFailure("profile adapter contract differs from shared authority")
        samples = value.get("samples_ns")
        if (
            not isinstance(samples, list)
            or len(samples) < 3
            or any(
                not isinstance(item, int) or isinstance(item, bool) or item < 0
                for item in samples
            )
        ):
            raise RunnerFailure("profile preflight samples are invalid")
        increments = [
            after - before
            for before, after in zip(samples, samples[1:])
            if after > before
        ]
        if any(after < before for before, after in zip(samples, samples[1:])) or not increments:
            raise RunnerFailure("profile preflight samples do not prove a resolution")
        increment = min(increments)
        multiplier = getattr(provider, "SCHEDSTAT_DECISION_MULTIPLIER", None)
        contract_sha256 = sha256_bytes(canonical_json_bytes(contract))
        expected = {
            "schema": getattr(provider, "PREFLIGHT_SCHEMA", None),
            "protocol": PROTOCOL,
            "protocol_sha256": PROTOCOL_SHA256,
            "profile_contract_sha256": contract_sha256,
            "source": "/proc/<pid>/task/<native-tid>/schedstat:first-field",
            "helper": "adapter-owned-cpu-bound-native-thread",
            "samples_ns": samples,
            "minimum_nonzero_increment_ns": increment,
            "decision_multiplier": multiplier,
            "decision_floor_ns": increment * multiplier if isinstance(multiplier, int) else None,
        }
        if (
            value != expected
            or contract.get("schema") != getattr(provider, "PROFILE_SCHEMA", None)
            or contract.get("protocol") != PROTOCOL
            or contract.get("protocol_sha256") != PROTOCOL_SHA256
            or contract.get("authority_schema")
            != getattr(provider, "AUTHORITY_SCHEMA", None)
            or contract.get("variant_source_bindings") != VARIANT_SOURCE_BINDINGS
            or contract.get("schedstat_decision_multiplier") != multiplier
            or contract.get("c_role_lifetime_contract")
            != C_ROLE_LIFETIME_CONTRACT
            or contract.get("perf_disable_owner")
            != "child-at-t1-before-measured-serialization"
            or contract.get("perf_child_environment")
            != {
                "cpu_all": [PERF_PERMISSION_ENVIRONMENT],
                "cpu_available_only": list(PERF_CHILD_FD_ENVIRONMENT),
                "non_cpu": [],
            }
            or contract.get("perf_ack_ledger")
            != {
                "ownership": "one-shared-offset",
                "artifact_mode": 0o444,
                "exact_bytes_utf8": "ack\nack\n",
                "exact_bytes": len(b"ack\nack\n"),
                "sha256": sha256_bytes(b"ack\nack\n"),
            }
            or contract.get("profile_inputs_persistence")
            != {
                "payload": "child.profile_tool_inputs",
                "sha256": "child.profile_tool_inputs_sha256",
                "raw_artifacts": (
                    "one-fd-nofollow-0444-sha256-and-byte-length"
                ),
            }
            or contract.get("trace_path_marker_schema")
            != {
                "fields": ["kind", "path"],
                "kinds": ["exact", "file_prefix", "directory_prefix"],
                "path_authority": "canonical-absolute",
                "empty_family_allowed": True,
                "combined_empty_allowed": False,
                "overlap_allowed": False,
            }
        ):
            raise RunnerFailure("profile preflight/contract authority differs")
        path = self.output / "profile-contract.json"
        atomic_json(path, value)
        self.profile_preflight = value
        self.config["profile_contract_sha256"] = sha256(path)
        if self.profile_tool_driver is not None:
            self.assert_host_admission_stable()
            self.snapshot_processes(
                "profile-tools-preflight-pre",
                enforce_resources=True,
                publish_manifest=False,
                snapshot_path=self.output / "profile-tools-preflight-pre.json",
            )
            tool_value = self.profile_tool_driver.preflight(
                prepared=self.prepared,
                output_dir=self.output,
                profile_adapter=provider,
            )
            tool_path = self.output / "profile-tools-preflight.json"
            atomic_json(tool_path, tool_value)
            self.frozen_files[str(tool_path.resolve())] = sha256(tool_path)
            self.snapshot_processes(
                "profile-tools-preflight-post",
                publish_manifest=False,
                snapshot_path=self.output / "profile-tools-preflight-post.json",
            )

    def _profile_inputs(
        self, track: str, tool_inputs: dict[str, Any]
    ) -> dict[str, Any]:
        resolution = (
            self.profile_preflight.get("minimum_nonzero_increment_ns")
            if self.profile_preflight is not None
            else None
        )
        if isinstance(resolution, bool) or not isinstance(resolution, int) or resolution <= 0:
            raise RunnerFailure("profile schedstat resolution authority is absent")
        if track in {"primary", "new_names", "fairness"}:
            value = {"schedstat_resolution_ns": resolution}
        elif track == "reopen":
            value = {}
        elif track == "cpu_profiles":
            value = {"schedstat_resolution_ns": resolution, **tool_inputs}
        elif track in {"syscall_profiles", "structural_traces"}:
            value = dict(tool_inputs)
        else:
            raise RunnerFailure(f"unknown profile input track {track!r}")
        detached = json.loads(canonical_json_bytes(value))
        if not isinstance(detached, dict):
            raise AssertionError("profile inputs did not detach as an object")
        return detached

    def _profile_fields(
        self,
        track: str,
        rich_result: dict[str, Any] | None,
        raw_point: dict[str, Any],
        control_events: list[dict[str, Any]],
        profile_inputs: dict[str, Any],
        authority: dict[str, Any],
    ) -> dict[str, Any]:
        expected = set(getattr(self.schema, "PROFILE_FIELDS_BY_TRACK")[track])
        provider = self.profile_adapter_module or self.profile_factory
        function = getattr(provider, "profile_fields", None)
        if function is None:
            if expected:
                raise RunnerFailure("profile adapter does not expose profile_fields")
            return {}
        fields = function(
            track,
            rich_result,
            raw_point=raw_point,
            control_events=control_events,
            authority=authority,
            profile_inputs=profile_inputs,
        )
        if not isinstance(fields, dict) or set(fields) != expected:
            raise RunnerFailure(
                f"profile adapter fields differ for {track}: "
                f"missing={sorted(expected - set(fields or {}))} "
                f"extra={sorted(set(fields or {}) - expected)}"
            )
        return fields

    def _frozen_file_bindings(self) -> dict[str, str]:
        values = {
            str(self.prepared.path): self.prepared.digest,
            str(self.prepared.source_approval): self.prepared.source_approval_sha256,
            str(self.prepared.release_compile_out.path): (
                self.prepared.release_compile_out.sha256
            ),
            str(self.prepared.tools_manifest.path): self.prepared.tools_manifest.sha256,
            str(Path(__file__).resolve()): sha256(Path(__file__).resolve()),
            str(self.schema_path): sha256(self.schema_path),
        }
        for variant in self.prepared.variants.values():
            values[str(variant.executable.path)] = variant.executable.sha256
        for tool in self.prepared.tools.values():
            values[str(tool.path)] = tool.sha256
        for support in self.prepared.support_files.values():
            values[str(support.path)] = support.sha256
        for input_file in self.prepared.inputs.values():
            values[str(input_file.path)] = input_file.sha256
        for source_review_file in self.prepared.source_review_files.values():
            values[str(source_review_file.path)] = source_review_file.sha256
        for semantic_manifest in self.semantic_manifests:
            prior = values.get(str(semantic_manifest.path))
            if prior is not None:
                conflict = (
                    "conflicting hashes" if prior != semantic_manifest.sha256 else "path alias"
                )
                raise RunnerFailure(
                    f"semantic manifest has {conflict}: {semantic_manifest.path}"
                )
            values[str(semantic_manifest.path)] = semantic_manifest.sha256
        for path, digest, _mode, _identity, _label in (
            self._release_compile_out_file_bindings()
        ):
            prior = values.get(str(path))
            if prior is not None and prior != digest:
                raise RunnerFailure(
                    f"release compile-out path has conflicting hashes: {path}"
                )
            values[str(path)] = digest
        return values

    def _release_compile_out_file_bindings(
        self,
    ) -> list[tuple[Path, str, int, dict[str, Any] | None, str]]:
        """Project every proof-referenced file into the runner's frozen set."""

        proof = self.prepared.release_compile_out_value
        binaries = proof.get("binaries")
        inventories = proof.get("symbol_inventories")
        nm = proof.get("nm")
        builds = proof.get("builds")
        if not all(
            isinstance(value, dict)
            for value in (binaries, inventories, nm, builds)
        ):
            # Synthetic runner fixtures predate the final proof shape. Real
            # prepared inputs already passed the shared exact schema validator.
            return []
        records: list[tuple[Path, str, int, dict[str, Any] | None, str]] = []
        for group_name, group, expected_mode in (
            ("binary", binaries, 0o555),
            ("symbol inventory", inventories, 0o444),
        ):
            for role in ("ordinary_a", "overlay_a"):
                binding = group.get(role)
                if not isinstance(binding, dict):
                    raise RunnerFailure(
                        f"release compile-out {group_name} {role} is absent"
                    )
                records.append(
                    (
                        Path(str(binding.get("path"))),
                        str(binding.get("sha256")),
                        expected_mode,
                        binding.get("identity")
                        if isinstance(binding.get("identity"), dict)
                        else None,
                        f"release compile-out {group_name} {role}",
                    )
                )
        nm_tool = nm.get("tool")
        if not isinstance(nm_tool, dict) or not isinstance(nm_tool.get("mode"), int):
            raise RunnerFailure("release compile-out nm tool is absent")
        records.append(
            (
                Path(str(nm_tool.get("path"))),
                str(nm_tool.get("sha256")),
                int(nm_tool["mode"]),
                nm_tool.get("identity")
                if isinstance(nm_tool.get("identity"), dict)
                else None,
                "release compile-out nm tool",
            )
        )
        for role in ("ordinary_a", "overlay_a"):
            child = nm.get(role)
            if not isinstance(child, dict):
                raise RunnerFailure(f"release compile-out nm child {role} is absent")
            records.append(
                (
                    Path(str(child.get("output_path"))),
                    str(child.get("output_sha256")),
                    0o444,
                    None,
                    f"release compile-out nm log {role}",
                )
            )
        attestation_file_fields = (
            ("source_archive_path", "source_archive_sha256"),
            ("archive_manifest_path", "archive_manifest_sha256"),
            ("overlay_manifest_path", "overlay_manifest_sha256"),
            ("materialized_manifest_path", "materialized_manifest_sha256"),
            ("cargo_lock_path", "cargo_lock_sha256"),
            ("build_log_path", "build_log_sha256"),
            ("contract_output_path", "contract_output_sha256"),
        )
        for role in ("ordinary_a", "overlay_a"):
            build = builds.get(role)
            attestation = (
                build.get("attestation") if isinstance(build, dict) else None
            )
            if not isinstance(attestation, dict):
                raise RunnerFailure(
                    f"release compile-out build attestation {role} is absent"
                )
            for path_field, digest_field in attestation_file_fields:
                records.append(
                    (
                        Path(str(attestation.get(path_field))),
                        str(attestation.get(digest_field)),
                        0o444,
                        None,
                        (
                            "release compile-out build "
                            f"{role} {path_field.removesuffix('_path')}"
                        ),
                    )
                )
            cargo_config = attestation.get("cargo_config_search")
            if not isinstance(cargo_config, dict):
                raise RunnerFailure(
                    f"release compile-out Cargo config authority {role} is absent"
                )
            records.append(
                (
                    Path(str(cargo_config.get("path"))),
                    str(cargo_config.get("sha256")),
                    0o444,
                    None,
                    f"release compile-out build {role} Cargo config search",
                )
            )
            cargo_config_path = Path(str(cargo_config.get("path")))
            records.append(
                (
                    cargo_config_path.with_name(
                        f"{cargo_config_path.name}.empty"
                    ),
                    hashlib.sha256(b"").hexdigest(),
                    0o444,
                    None,
                    f"release compile-out build {role} empty Cargo config",
                )
            )
        return records

    @staticmethod
    def _read_load1() -> float:
        try:
            return float(Path("/proc/loadavg").read_text().split()[0])
        except (OSError, ValueError, IndexError) as error:
            raise RunnerFailure(f"cannot read numeric load1: {error}") from error

    def log(self, message: str) -> None:
        append_bytes(self.output / "run.log", f"{now()} {self.phase} {message}\n".encode())
        print(message, flush=True)

    def verify_frozen(self) -> None:
        self.phase = "frozen_recheck"
        self.verify_semantic_manifests()
        self._verify_prepared_input_metadata()
        semantic_paths = {
            str(semantic_manifest.path)
            for semantic_manifest in self.semantic_manifests
        }
        for raw_path, expected in self.frozen_files.items():
            if raw_path in semantic_paths:
                continue
            path = Path(raw_path)
            if not path.is_file() or sha256(path) != expected:
                raise RunnerFailure(f"frozen artifact changed: {path}")
        if self.run_claim_path is not None and self.claim is not None:
            metadata = self.run_claim_path.lstat()
            if (
                stat.S_ISLNK(metadata.st_mode)
                or not stat.S_ISREG(metadata.st_mode)
                or stat.S_IMODE(metadata.st_mode) != 0o444
                or load_canonical_json(self.run_claim_path) != self.claim
            ):
                raise RunnerFailure("run claim record changed")

    def verify_semantic_manifests(self) -> None:
        """Recheck the exact retained semantic set without unrelated I/O."""

        if (
            self.semantic_manifest_counts != SEMANTIC_MANIFEST_COUNTS
            or len(self.semantic_manifests) != SEMANTIC_MANIFEST_TOTAL
        ):
            raise RunnerFailure("semantic manifest lifetime set is incomplete")
        for semantic_manifest in self.semantic_manifests:
            semantic_manifest.verify()

    def _verify_prepared_input_metadata(self) -> None:
        bindings: list[tuple[Path, str, int]] = [
            (self.prepared.path, "prepared artifacts", 0o444),
            (self.prepared.source_approval, "source approval", 0o444),
            (
                self.prepared.release_compile_out.path,
                "release compile-out proof",
                self.prepared.release_compile_out.mode,
            ),
            (
                self.prepared.tools_manifest.path,
                "prepared tools manifest",
                self.prepared.tools_manifest.mode,
            ),
        ]
        bindings.extend(
            (variant.executable.path, f"variant {name}", variant.executable.mode)
            for name, variant in self.prepared.variants.items()
        )
        bindings.extend(
            (executable.path, f"tool {name}", executable.mode)
            for name, executable in self.prepared.tools.items()
        )
        bindings.extend(
            (support.path, f"support {name}", support.mode)
            for name, support in self.prepared.support_files.items()
        )
        bindings.extend(
            (input_file.path, f"input {name}", input_file.mode)
            for name, input_file in self.prepared.inputs.items()
        )
        bindings.extend(
            (
                source_review_file.path,
                f"source review {name}",
                source_review_file.mode,
            )
            for name, source_review_file in self.prepared.source_review_files.items()
        )
        proof_bindings = self._release_compile_out_file_bindings()
        bindings.extend(
            (path, label, mode)
            for path, _digest, mode, _identity, label in proof_bindings
        )
        for path, label, expected_mode in bindings:
            try:
                metadata = path.lstat()
            except OSError as error:
                raise RunnerFailure(f"{label} metadata is unavailable: {error}") from error
            if (
                stat.S_ISLNK(metadata.st_mode)
                or not stat.S_ISREG(metadata.st_mode)
                or stat.S_IMODE(metadata.st_mode) != expected_mode
            ):
                raise RunnerFailure(f"{label} immutable metadata changed")
        for path, _digest, _mode, identity, label in proof_bindings:
            if identity is None:
                continue
            metadata = path.lstat()
            observed_identity = {
                "changed_ns": metadata.st_ctime_ns,
                "device": metadata.st_dev,
                "inode": metadata.st_ino,
                "link_count": metadata.st_nlink,
                "modified_ns": metadata.st_mtime_ns,
            }
            if identity != observed_identity or metadata.st_nlink != 1:
                raise RunnerFailure(f"{label} immutable identity changed")
        if self.prepared.inputs:
            # Protocol v4: the prepared root stays read-only and reusable; the
            # v3 claims/ subdirectory is no longer required or written.
            if stat.S_IMODE(self.prepared.root.stat().st_mode) != 0o555:
                raise RunnerFailure("prepared root is not read-only 0555")

    def _prepared_tree_state(self) -> str:
        root = self.prepared.root.resolve(strict=True)
        # Protocol v4: a v3-built prepared root may still carry a writable
        # 0700 claims/ directory; a v4-built root has none.  When present it
        # is recorded by mode only (its contents are ignored), so a stray
        # claim can neither perturb the tree hash nor trip the writable-entry
        # guard.  When absent, the whole tree is ordinary read-only entries.
        claims_parent = self.prepared.claim_path.parent
        claims = (
            claims_parent.resolve(strict=True)
            if claims_parent.is_dir() and claims_parent.name == "claims"
            else None
        )
        entries: list[dict[str, Any]] = []
        paths = [root, *sorted(root.rglob("*"))]
        for path in paths:
            resolved = path.resolve(strict=False)
            if claims is not None and path != claims and claims in resolved.parents:
                continue
            relative = "." if path == root else path.relative_to(root).as_posix()
            metadata = path.lstat()
            if path == claims:
                entries.append(
                    {
                        "path": relative,
                        "kind": "claims-directory",
                        "mode": stat.S_IMODE(metadata.st_mode),
                    }
                )
                continue
            if path.name == "__pycache__" or path.suffix in {".pyc", ".pyo"}:
                raise RunnerFailure(f"prepared tree contains Python cache: {relative}")
            if stat.S_ISLNK(metadata.st_mode):
                raise RunnerFailure(f"prepared tree contains symlink: {relative}")
            if stat.S_ISDIR(metadata.st_mode):
                kind = "directory"
            elif stat.S_ISREG(metadata.st_mode):
                kind = "file"
            else:
                raise RunnerFailure(
                    f"prepared tree contains unsupported entry: {relative}"
                )
            mode = stat.S_IMODE(metadata.st_mode)
            if mode & 0o222:
                raise RunnerFailure(f"prepared tree entry is writable: {relative}")
            entries.append(
                {
                    "path": relative,
                    "kind": kind,
                    "mode": mode,
                    "bytes": metadata.st_size,
                    "mtime_ns": metadata.st_mtime_ns,
                }
            )
        return sha256_bytes(canonical_json_bytes(entries))

    def _verify_prepared_tree(self) -> None:
        if self.prepared_tree_snapshot is None:
            return
        if self._prepared_tree_state() != self.prepared_tree_snapshot:
            raise RunnerFailure("prepared immutable tree changed")

    def _admit_run_mode(self) -> None:
        """Protocol v4 §3-4: admit the output directory for its run mode.

        Rehearsal (output name starts with "rehearsal-"): the directory must
        be fresh; the runner creates it.  Accepted: the operator pre-creates
        the directory containing exactly DECLARED.txt, a regular file whose
        lines are exactly ``label=<64-hex sha256>`` for label ``runner`` and
        one per variant (``A``..``D``), each hash matching this runner
        script and that variant's reviewed binary.  The declaration is
        parsed strictly (no substring matching, no comments, no extra
        labels) and its exact bytes are frozen into the run's authority so a
        post-admission edit — of a regular file or a symlink target — is
        detected by verify_frozen.  Every declared run, including failures,
        is reportable attempt history (§4), so the declaration is never
        runner-created.
        """

        self.phase = "run_mode_admission"
        if self.rehearsal:
            self.output.mkdir(parents=False, exist_ok=False)
            return
        if not self.output.is_dir():
            raise RunnerFailure(
                "accepted run requires an operator-created output directory "
                f"containing DECLARED.txt: {self.output}",
                exit_code=2,
            )
        entries = sorted(entry.name for entry in self.output.iterdir())
        if entries != ["DECLARED.txt"]:
            raise RunnerFailure(
                "accepted run output must contain exactly DECLARED.txt, "
                f"found: {entries}",
                exit_code=2,
            )
        declared_path = self.output / "DECLARED.txt"
        metadata = declared_path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise RunnerFailure(
                "DECLARED.txt must be a single-link regular file "
                "(no symlink, no hardlink)",
                exit_code=2,
            )
        declared_bytes = declared_path.read_bytes()
        try:
            declaration = declared_bytes.decode("ascii")
        except UnicodeDecodeError as error:
            raise RunnerFailure("DECLARED.txt is not ASCII", exit_code=2) from error
        required_digests = {
            "runner": sha256(Path(__file__).resolve()),
            **{
                name: variant.executable.sha256
                for name, variant in self.prepared.variants.items()
            },
        }
        declared_digests: dict[str, str] = {}
        for lineno, raw_line in enumerate(declaration.splitlines(), start=1):
            line = raw_line.strip()
            if not line:
                continue
            label, separator, value = line.partition("=")
            label = label.strip()
            value = value.strip()
            if separator != "=" or label not in required_digests:
                raise RunnerFailure(
                    f"DECLARED.txt line {lineno} is not a recognized "
                    "'<label>=<sha256>' declaration",
                    exit_code=2,
                )
            if label in declared_digests:
                raise RunnerFailure(
                    f"DECLARED.txt declares {label!r} more than once",
                    exit_code=2,
                )
            if not SHA256_RE.fullmatch(value):
                raise RunnerFailure(
                    f"DECLARED.txt {label!r} is not a 64-hex SHA-256",
                    exit_code=2,
                )
            declared_digests[label] = value
        if declared_digests != required_digests:
            raise RunnerFailure(
                "DECLARED.txt digests do not exactly match the runner and "
                "reviewed variant binaries",
                exit_code=2,
            )
        # Freeze the exact declaration bytes so any post-admission edit
        # (regular file or symlink target) is caught before row zero.
        self.declaration_sha256 = sha256_bytes(declared_bytes)
        self.frozen_files[str(declared_path.resolve(strict=True))] = (
            self.declaration_sha256
        )

    def acquire_lease(self) -> None:
        self.phase = "lease_acquire"
        # Protocol v4 §3-4: rehearsal runs need no coordination attestation
        # (their rows are non-evidence); accepted runs require the operator's
        # explicit host-quiet confirmation.  Both modes take the lease so a
        # rehearsal can never overlap an accepted run.
        if (
            not self.rehearsal
            and os.environ.get("MESS_BENCH_COORDINATION_CONFIRMED") != "true"
        ):
            raise RunnerFailure(
                "MESS_BENCH_COORDINATION_CONFIRMED must be true after host coordination"
            )
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.lock_path.open("a+b", buffering=0)
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            handle.close()
            raise RunnerFailure(f"global measurement lease is held: {self.lock_path}") from error
        metadata = os.fstat(handle.fileno())
        self.lease_handle = handle
        proof = self._proc_locks_proof(metadata.st_dev, metadata.st_ino)
        second = self.lock_path.open("a+b", buffering=0)
        try:
            try:
                fcntl.flock(second.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                second_lock_failed = True
            else:
                second_lock_failed = False
                fcntl.flock(second.fileno(), fcntl.LOCK_UN)
        finally:
            second.close()
        if not second_lock_failed:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
            self.lease_handle = None
            raise RunnerFailure("second exclusive measurement lock unexpectedly succeeded")
        self.lease = {
            "path": str(self.lock_path.resolve()),
            "device": metadata.st_dev,
            "inode": metadata.st_ino,
            "holder_pid": self.runner_identity["pid"],
            "holder_start_ticks": self.runner_identity["starttime_ticks"],
            "holder_uid": os.getuid(),
            "hostname": socket.gethostname(),
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "nonce": secrets.token_hex(32),
            "acquired_at": now(),
            "acquired_monotonic_ns": time.monotonic_ns(),
            "proc_locks_proof": proof,
            "second_exclusive_failed": True,
        }
        append_jsonl(self.lease_manifest, self.lease)

    @staticmethod
    def _proc_locks_proof(device: int, inode: int) -> str:
        major = os.major(device)
        minor = os.minor(device)
        suffix = f"{major:02x}:{minor:02x}:{inode}"
        matches = [line for line in Path("/proc/locks").read_text().splitlines() if suffix in line]
        if len(matches) != 1 or "FLOCK" not in matches[0] or "WRITE" not in matches[0]:
            raise RunnerFailure(f"cannot prove unique exclusive flock in /proc/locks: {matches}")
        return matches[0]

    def release_lease(self, terminal: str) -> None:
        if self.lease_handle is None:
            return
        handle = self.lease_handle
        self.lease_handle = None
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
        value = {
            "schema": getattr(
                self.schema, "LEASE_RELEASE_SCHEMA", "bn-2l3n-lease-release-v3"
            ),
            "protocol": PROTOCOL,
            "event": "released",
            "attempt_nonce": self.attempt_nonce,
            "lease_nonce": self.lease["nonce"] if self.lease else None,
            "lease_path": self.lease["path"] if self.lease else None,
            "lease_device": self.lease["device"] if self.lease else None,
            "lease_inode": self.lease["inode"] if self.lease else None,
            "outcome": terminal,
            "released_at": now(),
            "released_monotonic_ns": time.monotonic_ns(),
        }
        atomic_json(self.output / "lease-release.json", value)

    def claim_prepared(self) -> None:
        if self.lease is None:
            raise RunnerFailure("cannot claim prepared artifacts without lease")
        self.phase = "claim_prepared"
        self.claim = {
            "schema": CLAIM_SCHEMA,
            "protocol": PROTOCOL,
            "prepared_artifacts_path": str(self.prepared.path),
            "prepared_artifacts_sha256": self.prepared.digest,
            "output_dir": str(self.output.resolve()),
            "attempt_nonce": self.attempt_nonce,
            "lease_nonce": self.lease["nonce"],
            "claimed_at": now(),
            "claimed_monotonic_ns": time.monotonic_ns(),
        }
        # Protocol v4: the claim record lives in the run's own output
        # directory.  The prepared root stays untouched and reusable; the
        # lease (not the claim) provides mutual exclusion between runs.
        claim_path = self.output / "run-claim.json"
        atomic_create_json(claim_path, self.claim, mode=0o444)
        self.run_claim_path = claim_path
        self.frozen_files[str(claim_path.resolve(strict=True))] = sha256(claim_path)

    def wait_for_quiet(self) -> list[dict[str, Any]]:
        self.phase = "quiet_wait"
        deadline = self.monotonic() + self.quiet_timeout_seconds
        samples: list[dict[str, Any]] = []
        while True:
            load1 = self.load_reader()
            samples.append({"at": now(), "monotonic_ns": time.monotonic_ns(), "load1": load1})
            if load1 < self.maximum_load1:
                return samples
            if self.monotonic() >= deadline:
                if self.rehearsal:
                    # Protocol v4 §3: rehearsal rows are non-evidence, so a
                    # noisy host is recorded, warned about, and tolerated.
                    samples.append(
                        {
                            "at": now(),
                            "monotonic_ns": time.monotonic_ns(),
                            "load1": load1,
                            "rehearsal_quiet_waiver": True,
                        }
                    )
                    self.log(
                        f"rehearsal: load1={load1} stayed at or above "
                        f"{self.maximum_load1}; continuing (non-evidence rows)"
                    )
                    return samples
                raise RunnerFailure(
                    f"load1={load1} did not fall below {self.maximum_load1} "
                    f"within {self.quiet_timeout_seconds} seconds"
                )
            self.sleep(self.quiet_poll_seconds)

    def resource_guard(self, label: str, absent_paths: Iterable[Path] = ()) -> dict[str, Any]:
        self.phase = f"resource_guard_{label}"
        self.assert_host_admission_stable()
        for path in absent_paths:
            if path.exists() or path.is_symlink():
                raise RunnerFailure(f"fresh identity already exists: {path}")
        sample = os.statvfs(self.scratch_root)
        free_bytes = sample.f_bavail * sample.f_frsize
        free_inodes = sample.f_favail
        load1 = self.load_reader()
        value = {
            "schema": "asterism-rebaseline-resource-sample-v3",
            "protocol": PROTOCOL,
            "ordinal": self.resource_count + 1,
            "label": label,
            "at": now(),
            "monotonic_ns": time.monotonic_ns(),
            "scratch_root": str(self.scratch_root.resolve()),
            "free_bytes": free_bytes,
            "free_inodes": free_inodes,
            "minimum_free_bytes": self.minimum_free_bytes,
            "minimum_free_inodes": self.minimum_free_inodes,
            "load1": load1,
            "maximum_load1_exclusive": self.maximum_load1,
            "absent_paths": [str(path) for path in absent_paths],
        }
        append_jsonl(self.resource_manifest, value)
        self.resource_count += 1
        if free_bytes < self.minimum_free_bytes or free_inodes < self.minimum_free_inodes:
            raise RunnerFailure(
                f"resource floor failed: free_bytes={free_bytes} free_inodes={free_inodes}"
            )
        if load1 >= self.maximum_load1:
            raise RunnerFailure(f"load changed after quiet wait: load1={load1}")
        return value

    def _read_guard_process(self, pid: int) -> tuple[dict[str, Any], list[str], bool]:
        try:
            record = parse_proc_stat(pid)
        except FileNotFoundError:
            return {"pid": pid}, [], False
        except (OSError, ValueError) as error:
            return {"pid": pid}, [f"stat:{error.__class__.__name__}:{error}"], True
        if (
            record["comm"] not in self.prepared.tracked_comm
            and not same_identity(record, self.runner_identity)
            and not same_identity(record, self.active_child)
            and not any(same_identity(record, helper) for helper in self.active_helpers)
        ):
            return record, [], True
        proc = Path("/proc") / str(pid)
        errors: list[str] = []
        try:
            record["uid"] = proc.stat().st_uid
        except OSError as error:
            errors.append(f"uid:{error.__class__.__name__}:{error}")
        try:
            record["exe"] = str((proc / "exe").resolve(strict=True))
            record["exe_sha256"] = sha256(proc / "exe")
        except OSError as error:
            errors.append(f"exe:{error.__class__.__name__}:{error}")
        try:
            record["cmdline"] = (
                (proc / "cmdline")
                .read_bytes()
                .replace(b"\0", b" ")
                .decode("utf-8", "backslashreplace")
                .rstrip()
            )
        except OSError as error:
            errors.append(f"cmdline:{error.__class__.__name__}:{error}")
        return record, errors, True

    def _allowed_group_member(self, record: dict[str, Any]) -> bool:
        candidates = [
            *(variant.executable for variant in self.prepared.variants.values()),
            *self.prepared.tools.values(),
        ]
        exact_helper = any(same_identity(record, helper) for helper in self.active_helpers)
        same_child_group = bool(
            self.active_child is not None
            and record.get("pgrp") == self.active_child.get("pgrp")
        )
        return (exact_helper or same_child_group) and any(
            record.get("comm") == executable.comm
            and record.get("exe_sha256") == executable.sha256
            for executable in candidates
        )

    def snapshot_processes(
        self,
        label: str,
        *,
        enforce_resources: bool = False,
        publish_manifest: bool = True,
        snapshot_path: Path | None = None,
    ) -> dict[str, Any]:
        """Scan exact approved comm values without command-line substring matching."""

        self.phase = f"process_guard_{label}"
        self.assert_host_admission_stable()
        started_at = now()
        started_monotonic_ns = time.monotonic_ns()
        records: list[dict[str, Any]] = []
        preidentity_vanished: list[int] = []
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            pid = int(entry.name)
            record, errors, observed = self._read_guard_process(pid)
            if not observed:
                preidentity_vanished.append(pid)
                continue
            if record.get("comm") not in self.prepared.tracked_comm and not errors:
                continue
            if errors:
                classification = "vanished_unresolved"
            elif same_identity(record, self.runner_identity):
                classification = "runner"
            elif same_identity(record, self.active_child):
                classification = "current_child"
            elif self._allowed_group_member(record):
                classification = "declared_helper"
            else:
                classification = "foreign_unexplained"
            records.append(
                {
                    **record,
                    "read_errors": errors,
                    "classification": classification,
                    "observed_at": now(),
                }
            )
        bad = [
            record
            for record in records
            if record["classification"] in {"vanished_unresolved", "foreign_unexplained"}
        ]
        if self.active_child is not None:
            child_matches = [
                record for record in records if same_identity(record, self.active_child)
            ]
            if (
                len(child_matches) != 1
                or self.active_executable is None
                or child_matches[0].get("comm") != self.active_executable.comm
                or child_matches[0].get("exe_sha256")
                != self.active_executable.sha256
            ):
                bad.append(
                    {
                        **self.active_child,
                        "classification": "current_child_binding_mismatch",
                        "read_errors": [],
                        "observed": child_matches,
                    }
                )
        resource = os.statvfs(self.scratch_root)
        final_resource = {
            "load1": self.load_reader(),
            "free_bytes": resource.f_bavail * resource.f_frsize,
            "free_inodes": resource.f_favail,
            "enforced": enforce_resources,
        }
        if enforce_resources and (
            final_resource["load1"] >= self.maximum_load1
            or final_resource["free_bytes"] < self.minimum_free_bytes
            or final_resource["free_inodes"] < self.minimum_free_inodes
        ):
            bad.append(
                {
                    "pid": None,
                    "comm": None,
                    "classification": "final_resource_guard_failed",
                    "read_errors": [],
                    "resource": final_resource,
                }
            )
        snapshot = {
            "schema": self.schema.GUARD_SCHEMA,
            "protocol": PROTOCOL,
            "ordinal": self.guard_count + 1,
            "label": label,
            "tracked_comm": sorted(self.prepared.tracked_comm),
            "runner": self.runner_identity,
            "active_child": self.active_child,
            "active_helpers": self.active_helpers,
            "records": records,
            "final_resource": final_resource,
            "preidentity_vanished": preidentity_vanished,
            "verdict": "fail" if bad else "pass",
            "started_at": started_at,
            "started_monotonic_ns": started_monotonic_ns,
            "completed_at": now(),
            "completed_monotonic_ns": time.monotonic_ns(),
        }
        path = snapshot_path or (
            self.output / "guards" / f"{self.guard_count + 1:05d}-{label}.json"
        )
        atomic_json(path, snapshot)
        require_exact_keys(
            snapshot,
            set(self.schema.GUARD_SNAPSHOT_FIELDS),
            "process guard snapshot",
        )
        binding = {
            "schema": self.schema.GUARD_BINDING_SCHEMA,
            "protocol": PROTOCOL,
            "kind": "process_guard",
            "ordinal": self.guard_count + 1,
            "label": label,
            "path": str(path.resolve()),
            "sha256": sha256(path),
            "verdict": snapshot["verdict"],
            "started_monotonic_ns": started_monotonic_ns,
            "completed_monotonic_ns": snapshot["completed_monotonic_ns"],
        }
        require_exact_keys(
            binding,
            set(self.schema.GUARD_BINDING_FIELDS),
            "process guard binding",
        )
        if publish_manifest:
            append_jsonl(self.guard_manifest, binding)
            self.guard_count += 1
        if bad:
            raise RunnerFailure(
                f"process guard {label} found unexplained/unresolved activity: "
                f"{[{k: item.get(k) for k in ('pid','comm','starttime_ticks','classification','read_errors')} for item in bad]}"
            )
        return snapshot

    @staticmethod
    def _parse_canonical_lines(path: Path, expected: int) -> list[dict[str, Any]]:
        payload = path.read_bytes()
        lines = payload.splitlines(keepends=True)
        if len(lines) != expected:
            raise RunnerFailure(
                f"raw child output cardinality {len(lines)} != {expected}: {path}"
            )
        records: list[dict[str, Any]] = []
        for ordinal, line in enumerate(lines, start=1):
            try:
                value = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise RunnerFailure(f"raw output line {ordinal} is not JSON: {error}") from error
            if not isinstance(value, dict) or line != canonical_json_bytes(value):
                raise RunnerFailure(f"raw output line {ordinal} is not canonical JSON+LF")
            records.append(value)
        return records

    @staticmethod
    def _socket_json_read(control: socket.socket, phase: str) -> dict[str, Any]:
        chunks = bytearray()
        while True:
            byte = control.recv(1)
            if not byte:
                raise RunnerFailure(f"control socket closed before {phase}")
            chunks.extend(byte)
            if byte == b"\n":
                break
            if len(chunks) > 1024 * 1024:
                raise RunnerFailure(f"control message exceeds 1 MiB before {phase}")
        try:
            value = json.loads(bytes(chunks))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RunnerFailure(f"control {phase} is not JSON: {error}") from error
        if not isinstance(value, dict) or bytes(chunks) != canonical_json_bytes(value):
            raise RunnerFailure(f"control {phase} is not canonical JSON+LF")
        return value

    @staticmethod
    def _socket_json_write(control: socket.socket, value: dict[str, Any]) -> None:
        control.sendall(canonical_json_bytes(value))

    def _reap_probe(self, identity: dict[str, Any]) -> dict[str, Any]:
        try:
            observed = parse_proc_stat(identity["pid"])
        except FileNotFoundError:
            return {
                "status": "absent",
                "pid": identity["pid"],
                "start_ticks": identity["starttime_ticks"],
            }
        except OSError as error:
            raise RunnerFailure(f"cannot prove child reaped: {error}") from error
        if same_identity(observed, identity):
            raise RunnerFailure(f"child {identity['pid']} was waited but remains present")
        return {
            "status": "pid_reused",
            "pid": identity["pid"],
            "start_ticks": identity["starttime_ticks"],
        }

    def _base_environment(self) -> dict[str, str]:
        return {
            "HOME": str(self.runtime_home.resolve()),
            "PATH": "/usr/bin:/bin",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "TZ": "UTC",
        }

    def _assert_controlled_child_environment(
        self,
        plan: ChildPlan,
        environment: dict[str, str],
        *,
        physical_ordinal: int,
        context_sha256: str,
        control_fd: int,
    ) -> None:
        if plan.track is not None:
            function = getattr(self.schema, "row_child_environment", None)
            if not callable(function):
                raise RunnerFailure("shared row child environment authority is absent")
            ptracer_text = environment.get("ASTERISM_REBASELINE_PTRACER_PID")
            ptracer_pid = int(ptracer_text) if ptracer_text is not None else None
            expected = function(
                scratch_root=self.scratch_root,
                attempt_nonce=self.attempt_nonce,
                output_dir=self.output,
                config_path=self.config_path,
                physical_ordinal=physical_ordinal,
                track=plan.track,
                identity={
                    "row_ordinal": plan.context.get("row_ordinal"),
                    "variant": plan.context.get("variant"),
                },
                context=plan.context,
                context_sha256=context_sha256,
                control_fd=str(control_fd),
                ptracer_pid=ptracer_pid,
                perf_permission_result=environment.get(
                    PERF_PERMISSION_ENVIRONMENT
                ),
                perf_command_fd=environment.get(PERF_CHILD_FD_ENVIRONMENT[0]),
                perf_ack_fd=environment.get(PERF_CHILD_FD_ENVIRONMENT[1]),
                perf_ack_ledger_fd=environment.get(
                    PERF_CHILD_FD_ENVIRONMENT[2]
                ),
            )
        elif plan.kind in {"correctness", "fault"}:
            function = getattr(self.schema, "correctness_child_environment", None)
            if not callable(function) or plan.environment_ordinal is None:
                raise RunnerFailure(
                    "shared correctness child environment authority is absent"
                )
            expected = function(
                scratch_root=self.scratch_root,
                attempt_nonce=self.attempt_nonce,
                output_dir=self.output,
                physical_ordinal=physical_ordinal,
                variant=str(plan.context["variant"]),
                phase=str(plan.context["phase"]),
                suite=str(plan.context["suite"]),
                phase_ordinal=plan.environment_ordinal,
                context_sha256=context_sha256,
                control_fd=str(control_fd),
            )
        else:
            return
        if environment != expected:
            raise RunnerFailure(
                "controlled child environment differs from shared authority: "
                f"missing={sorted(set(expected) - set(environment))} "
                f"extra={sorted(set(environment) - set(expected))}"
            )

    def _fresh_store(self, track: str, ordinal: int, variant: str) -> Path:
        identity = sha256_bytes(
            f"{self.attempt_nonce}\0{track}\0{ordinal}\0{variant}".encode()
        )[:20]
        return self.attempt_scratch / "stores" / f"{track}-{ordinal:05d}-{variant}-{identity}"

    def _variant_plan(
        self,
        context: dict[str, Any],
        *,
        mode: str | None = None,
        track: str | None = None,
        controlled: bool = True,
        store_path: Path | None = None,
        require_store_after: bool = True,
        store_absent_before: bool = True,
        expected_records: int = 1,
    ) -> ChildPlan:
        variant = self.prepared.variants[str(context["variant"])]
        selected_mode = mode or str(context["track"])
        environment = dict(variant.evidence_env)
        marker_keys = {
            "log": "ASTERISM_REBASELINE_LOG_PATH_MARKERS",
            "metadata": "ASTERISM_REBASELINE_METADATA_PATH_MARKERS",
        }
        marker_templates = {
            family: environment.pop(key, None) for family, key in marker_keys.items()
        }
        environment.update(
            {
                "ASTERISM_REBASELINE_MODE": selected_mode,
                "ASTERISM_REBASELINE_VARIANT": variant.name,
            }
        )
        if selected_mode == "smoke":
            smoke_target = context.get("smoke_target")
            if not isinstance(smoke_target, str) or not smoke_target:
                raise RunnerFailure("variant smoke target is absent")
            environment["ASTERISM_REBASELINE_SMOKE_TARGET"] = smoke_target
        dynamic = {
            "ASTERISM_DURABILITY": context.get("durability"),
            "ASTERISM_PAYLOAD_BYTES": context.get("payload_size"),
            "ASTERISM_BATCH": context.get("batch_size"),
            "ASTERISM_WRITERS": context.get("writers"),
            "ASTERISM_BATCHES_PER_WRITER": context.get("batches_per_writer"),
            "ASTERISM_TRACE_KIND": context.get("trace_kind"),
            "ASTERISM_EXPECTED_DOMAIN_EVENTS": context.get(
                "expected_domain_events"
            ),
            "ASTERISM_EXPECTED_VISIBLE_EVENTS": context.get(
                "expected_visible_events"
            ),
            "ASTERISM_EXPECTED_LOG_EVENTS": context.get("expected_log_events"),
            "ASTERISM_EXPECTED_LOGICAL_DIGEST": context.get(
                "expected_logical_digest"
            ),
            "ASTERISM_EXPECTED_REGISTRY_HEAD_DIGEST": context.get(
                "expected_registry_head_digest"
            ),
        }
        environment.update(
            {key: str(value) for key, value in dynamic.items() if value is not None}
        )
        argv = variant.evidence_argv
        if track is not None:
            row_ordinal = context.get("row_ordinal")
            if not isinstance(row_ordinal, int) or isinstance(row_ordinal, bool):
                raise RunnerFailure(f"{track} row ordinal is absent")
            environment.update(
                {
                    "ASTERISM_REBASELINE_CONFIG": str(self.config_path.resolve()),
                    "ASTERISM_REBASELINE_ROW_ORDINAL": str(row_ordinal),
                }
            )
            argv = (
                *variant.evidence_argv,
                "--run-row",
                "--track",
                track,
                "--row-ordinal",
                str(row_ordinal),
                "--config",
                str(self.config_path.resolve()),
            )
        if track in {"syscall_profiles", "structural_traces"} or context.get(
            "smoke_target"
        ) in {"syscall_profiles", "structural_traces"}:
            if store_path is None or not store_path.is_absolute():
                raise RunnerFailure("trace plan lacks an absolute store identity")
            relative_markers: dict[str, list[dict[str, str]]] = {}
            for family in marker_keys:
                encoded = marker_templates[family]
                if encoded is None:
                    raise RunnerFailure(
                        f"variant {variant.name} omits reviewed {family} path markers"
                    )
                try:
                    values = json.loads(encoded)
                except json.JSONDecodeError as error:
                    raise RunnerFailure(
                        f"variant {variant.name} {family} path markers are invalid JSON"
                    ) from error
                expected_values = variant.trace_path_marker_templates.get(family)
                if not isinstance(expected_values, list) or values != expected_values:
                    raise RunnerFailure(
                        f"variant {variant.name} {family} path markers differ from source authority"
                    )
                relative_markers[family] = values
            resolver = getattr(self.schema, "resolved_trace_path_markers", None)
            if not callable(resolver):
                raise RunnerFailure("shared trace marker resolver is absent")
            try:
                markers = resolver(store_path, variant.name)
            except (TypeError, ValueError) as error:
                raise RunnerFailure(
                    f"variant {variant.name} trace marker resolution failed: {error}"
                ) from error
            if (
                variant.trace_path_marker_templates.get("root_environment")
                != "ASTERISM_REBASELINE_STORE"
                or relative_markers
                != {
                    family: variant.trace_path_marker_templates[family]
                    for family in marker_keys
                }
                or not isinstance(markers, dict)
                or set(markers) != set(marker_keys)
                or not markers["log"]
                or (variant.name != "B" and not markers["metadata"])
            ):
                raise RunnerFailure(
                    f"variant {variant.name} path marker families are incomplete"
                )
            context["variant_trace_path_markers"] = markers
        profile_track = track or context.get("profile_smoke_track")
        if profile_track is None and context.get("smoke_target") in {
            "cpu_profiles",
            "syscall_profiles",
            "structural_traces",
        }:
            profile_track = context["smoke_target"]
        timeout_seconds = self.row_timeout_seconds
        if variant.name == "C" and profile_track is not None:
            timeout_seconds = min(timeout_seconds, C_PROFILE_CHILD_TIMEOUT_SECONDS)
        return ChildPlan(
            kind=selected_mode,
            context=context,
            executable=variant.executable,
            argv=argv,
            environment=environment,
            expected_records=expected_records,
            track=track,
            store_path=store_path,
            require_store_after=require_store_after,
            timeout_seconds=timeout_seconds,
            store_absent_before=store_absent_before,
        )

    def contract_plans(self) -> list[ChildPlan]:
        plans = []
        for name in VARIANTS:
            variant = self.prepared.variants[name]
            plans.append(
                ChildPlan(
                    kind="contract",
                    context={"transition": "contract", "variant": name},
                    executable=variant.executable,
                    argv=variant.contract_argv,
                    environment=variant.contract_env,
                    expected_records=1,
                    track=None,
                    store_path=None,
                    require_store_after=False,
                    timeout_seconds=30,
                )
            )
        return plans

    def smoke_plans(self) -> list[ChildPlan]:
        """One real runner-path smoke for every evidence subprocess shape."""

        unique: list[tuple[str, str]] = []
        for track in TRACK_CARDINALITY:
            rows = self.schema.expected_order(self.config, track)
            for context in rows:
                coordinate = (track, context["variant"])
                if coordinate not in unique:
                    unique.append(coordinate)
        plans: list[ChildPlan] = []
        for smoke_ordinal, (track, variant_name) in enumerate(unique, start=1):
            context = {
                "transition": "smoke",
                "smoke_ordinal": smoke_ordinal,
                "smoke_target": track,
                "variant": variant_name,
                "durability": "Process",
            }
            store = self._fresh_store("smoke", smoke_ordinal, variant_name)
            plans.append(
                self._variant_plan(
                    context,
                    mode="smoke",
                    track=None,
                    store_path=store,
                    require_store_after=True,
                )
            )
        for tool_name in ("correctness", "fault"):
            tool = self.prepared.tools.get(tool_name)
            if tool is None:
                raise RunnerFailure(f"prepared artifacts omit required tool {tool_name}")
            context = {
                "transition": "smoke",
                "smoke_ordinal": len(plans) + 1,
                "smoke_target": tool_name,
                "variant": "A",
                "durability": "Process",
            }
            plans.append(
                ChildPlan(
                    kind="smoke",
                    context=context,
                    executable=tool,
                    argv=(str(tool.path), "--smoke"),
                    environment={
                        "ASTERISM_REBASELINE_MODE": "smoke",
                        "ASTERISM_REBASELINE_SMOKE_TARGET": tool_name,
                    },
                    expected_records=1,
                    track=None,
                    store_path=self._fresh_store("smoke", len(plans) + 1, "A"),
                    require_store_after=True,
                    timeout_seconds=120,
                )
            )
        for role, runtime_name, support_name in (
            ("evaluator", "evaluator_runtime", "evaluator"),
            (
                "terminal_verifier",
                "terminal_verifier_runtime",
                "terminal_verifier",
            ),
        ):
            runtime = self.prepared.tools.get(runtime_name)
            support = self.prepared.support_files.get(support_name)
            if runtime is None or support is None:
                raise RunnerFailure(f"prepared artifacts omit required {role} role")
            plans.append(
                ChildPlan(
                    kind="smoke",
                    context={
                        "transition": "smoke",
                        "smoke_ordinal": len(plans) + 1,
                        "smoke_target": role,
                        "variant": "A",
                        "durability": "Process",
                    },
                    executable=runtime,
                    argv=(str(runtime.path), str(support.path), "--smoke"),
                    environment={
                        "ASTERISM_REBASELINE_MODE": "smoke",
                        "ASTERISM_REBASELINE_SMOKE_TARGET": role,
                    },
                    expected_records=1,
                    track=None,
                    store_path=None,
                    require_store_after=False,
                    timeout_seconds=120,
                    controlled=False,
                )
            )
        return plans

    def _run_specialized_reopen_smokes(self) -> None:
        """Exercise the truthful pre-open transition and traced reopen pre-row."""

        variant = self.prepared.variants["A"]
        source = self.attempt_scratch / "smoke-reopen" / "archive-source"
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
        seed_plan = self._variant_plan(
            seed_context,
            mode="smoke_reopen_seed",
            track=None,
            store_path=source,
            require_store_after=True,
        )
        seed_record = self.run_child(seed_plan, controlled=True)
        seed = self._parse_canonical_lines(Path(seed_record["raw_path"]), 1)[0]
        expected_seed_keys = {
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
            set(seed) != expected_seed_keys
            or seed.get("schema")
            != "bn-2l3n-overlay-smoke-reopen-seed-v3"
            or seed.get("protocol") != PROTOCOL
            or seed.get("protocol_sha256") != PROTOCOL_SHA256
            or seed.get("variant") != "A"
            or seed.get("domain_events") != 1
            or not SHA256_RE.fullmatch(str(seed.get("logical_digest")))
            or not SHA256_RE.fullmatch(str(seed.get("registry_head_digest")))
        ):
            raise RunnerFailure("specialized reopen smoke seed differs")
        entries = tree_manifest(source)
        content = self._content_manifest(entries)
        manifest = {
            "content_sha256": sha256_bytes(canonical_json_bytes(content)),
        }
        make_tree_read_only(source)
        if self._content_manifest(tree_manifest(source)) != content:
            raise RunnerFailure("specialized reopen smoke archive replay differs")
        self._register_corpus_execution_tree(
            source,
            role="specialized-source",
            track="specialized_reopen",
            row_ordinal=0,
            variant="A",
            read_only=True,
        )
        corpus = {
            "root": source,
            "manifest": manifest,
            "content": content,
            "seed_observations": seed,
        }
        for index, (mode, smoke_id, profile_track, tool_target) in enumerate(
            (
                ("smoke_reopen", "smoke_reopen", "reopen", None),
                (
                    "smoke_structural_reopen",
                    "smoke_structural_reopen",
                    "structural_traces",
                    "structural_traces",
                ),
            ),
            start=1,
        ):
            store, copy_context = self.materialize_corpus(
                corpus, "smoke-reopen", index, "A"
            )
            context = {
                "transition": "smoke",
                "smoke_id": smoke_id,
                "smoke_target": tool_target or smoke_id,
                "profile_smoke_track": profile_track,
                "reopen_order": True,
                "trace_kind": "reopen" if tool_target else None,
                "variant": "A",
                "durability": "Process",
                **copy_context,
                "expected_domain_events": seed["domain_events"],
                "expected_visible_events": seed["visible_events"],
                "expected_log_events": seed["log_events"],
                "expected_logical_digest": seed["logical_digest"],
                "expected_registry_head_digest": seed["registry_head_digest"],
            }
            plan = self._variant_plan(
                context,
                mode=mode,
                track=None,
                store_path=store,
                require_store_after=True,
                store_absent_before=False,
            )
            self.run_child(plan, controlled=True)

    def correctness_plans(self, phase: str) -> list[ChildPlan]:
        if phase not in {"pre", "post"}:
            raise RunnerFailure(f"invalid correctness phase {phase}")
        correctness = self.prepared.tools.get("correctness")
        fault = self.prepared.tools.get("fault")
        if correctness is None or fault is None:
            raise RunnerFailure("prepared correctness/fault executables are required")
        approved = [
            descriptor
            for descriptor in self.config["correctness_cases"]
            if descriptor["phase"] in ({"oracle", "pre"} if phase == "pre" else {"post"})
        ]
        grouped: dict[tuple[str, str, str, str], list[dict[str, str]]] = {}
        for descriptor in approved:
            group_key = (
                descriptor["variant"],
                descriptor["phase"],
                descriptor["suite"],
                descriptor["kind"],
            )
            grouped.setdefault(group_key, []).append(descriptor)
        plans: list[ChildPlan] = []
        for (name, child_phase, suite, kind), descriptors in grouped.items():
            if suite == "common-public-oracle":
                if name not in {"C", "D"} or kind != "correctness":
                    raise RunnerFailure("historical correctness authority differs")
                variant = self.prepared.variants[name]
                if variant.correctness_oracle_mode is not True:
                    raise RunnerFailure(
                        f"variant {name} lacks bound correctness oracle capability"
                    )
                executable = variant.executable
                mode = "correctness_oracle"
                mode_flag = "--correctness-oracle"
            elif suite == "current-product" and name == "A" and kind == "correctness":
                executable = correctness
                mode = "correctness"
                mode_flag = "--correctness"
            elif suite == "current-fault" and name == "A" and kind == "fault":
                executable = fault
                mode = "fault"
                mode_flag = "--fault"
            else:
                raise RunnerFailure(
                    f"correctness executable authority differs: {name}/{suite}/{kind}"
                )
            context = {
                "transition": kind,
                "phase": child_phase,
                "variant": name,
                "suite": suite,
                "expected_cases": [
                    {
                        "id": descriptor["id"],
                        "classification": descriptor["classification"],
                    }
                    for descriptor in descriptors
                ],
                "durability": "Group" if kind == "fault" else "Process",
            }
            environment_ordinal = len(plans) + 1
            plans.append(
                ChildPlan(
                    kind=kind,
                    context=context,
                    executable=executable,
                    argv=(
                        str(executable.path),
                        mode_flag,
                        "--protocol",
                        PROTOCOL,
                        "--attempt-nonce",
                        self.attempt_nonce,
                        "--variant",
                        name,
                        "--phase",
                        child_phase,
                        "--suite",
                        suite,
                    ),
                    environment={
                        "ASTERISM_REBASELINE_MODE": mode,
                        "ASTERISM_REBASELINE_ATTEMPT_NONCE": self.attempt_nonce,
                        "ASTERISM_REBASELINE_PHASE": child_phase,
                        "ASTERISM_REBASELINE_PROTOCOL": PROTOCOL,
                        "ASTERISM_REBASELINE_SUITE": suite,
                        "ASTERISM_REBASELINE_VARIANT": name,
                    },
                    expected_records=1,
                    track=None,
                    store_path=self._fresh_store(
                        f"correctness-{phase}", environment_ordinal, name
                    ),
                    require_store_after=True,
                    timeout_seconds=3_600 if kind == "fault" else 1_800,
                    environment_ordinal=environment_ordinal,
                )
            )
        expected_groups = 4 if phase == "pre" else 2
        if len(plans) != expected_groups:
            raise RunnerFailure(
                f"correctness {phase} grouping differs: {len(plans)} != {expected_groups}"
            )
        return plans

    @staticmethod
    def _correctness_execution_record(plan: ChildPlan) -> dict[str, Any]:
        return {
            "variant": plan.context["variant"],
            "phase": plan.context["phase"],
            "suite": plan.context["suite"],
            "kind": plan.kind,
            "executable_path": str(plan.executable.path),
            "executable_sha256": plan.executable.sha256,
            "executable_mode": plan.executable.mode,
            "executable_comm": plan.executable.comm,
            "argv": list(plan.argv),
            "environment": dict(plan.environment),
        }

    def _refresh_correctness_execution(self) -> None:
        plans = [
            *self.correctness_plans("pre"),
            *self.correctness_plans("post"),
        ]
        records = [
            self._correctness_execution_record(plan) for plan in plans
        ]
        expected_groups = list(
            getattr(
                self.schema,
                "CORRECTNESS_GROUPS",
                (
                    ("C", "oracle", "common-public-oracle", "correctness"),
                    ("D", "oracle", "common-public-oracle", "correctness"),
                    ("A", "pre", "current-product", "correctness"),
                    ("A", "pre", "current-fault", "fault"),
                    ("A", "post", "current-product", "correctness"),
                    ("A", "post", "current-fault", "fault"),
                ),
            )
        )
        if [
            tuple(record[field] for field in ("variant", "phase", "suite", "kind"))
            for record in records
        ] != expected_groups:
            raise RunnerFailure("correctness execution group order differs")
        schema_contract = getattr(self.schema, "correctness_execution_contract", None)
        if schema_contract is not None:
            expected = schema_contract(self.prepared.value, self.attempt_nonce)
            if records != expected:
                raise RunnerFailure("correctness execution differs from shared schema")
        self.config["correctness_execution"] = records

    @staticmethod
    def _content_manifest(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {key: value for key, value in entry.items() if key != "mode"}
            for entry in entries
        ]

    def _expected_corpus_execution_records(
        self, *, correctness_only: bool
    ) -> list[dict[str, Any]]:
        """Return the exact physical corpus plan without consulting config."""

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

        append(
            "specialized-source",
            "specialized_reopen",
            0,
            "A",
            self.attempt_scratch / "smoke-reopen" / "archive-source",
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
                self._fresh_store(
                    "smoke-reopen-corpus", row_ordinal, "A"
                ),
                read_only=False,
            )
        if correctness_only:
            return records
        for variant in PUBLIC_VARIANTS:
            append(
                "full-source",
                "reopen_seed",
                0,
                variant,
                self.attempt_scratch
                / "corpora"
                / f"{variant}-archive-source",
                read_only=True,
            )
        reopen_variants = (
            "A",
            "C",
            "D",
            "C",
            "D",
            "A",
            "D",
            "A",
            "C",
        )
        for row_ordinal, variant in enumerate(reopen_variants, start=1):
            append(
                "full-copy",
                "reopen",
                row_ordinal,
                variant,
                self._fresh_store("reopen-corpus", row_ordinal, variant),
                read_only=False,
            )
        for row_ordinal, variant in zip(
            (13, 14, 15), PUBLIC_VARIANTS, strict=True
        ):
            append(
                "full-copy",
                "structural_traces",
                row_ordinal,
                variant,
                self._fresh_store(
                    "structural_traces-corpus", row_ordinal, variant
                ),
                read_only=False,
            )
        return records

    def _register_corpus_execution_tree(
        self,
        root: Path,
        *,
        role: str,
        track: str,
        row_ordinal: int,
        variant: str,
        read_only: bool,
    ) -> None:
        expected_records = self._expected_corpus_execution_records(
            correctness_only=False
        )
        ordinal = len(self.corpus_execution_authority) + 1
        if ordinal > len(expected_records):
            raise RunnerFailure("corpus execution authority has an extra tree")
        expected = expected_records[ordinal - 1]
        binding = {
            "ordinal": ordinal,
            "role": role,
            "track": track,
            "row_ordinal": row_ordinal,
            "variant": variant,
            "root": str(root),
            "root_mode": 0o555 if read_only else 0o755,
            "directory_mode": 0o555 if read_only else 0o755,
            "file_mode": 0o444 if read_only else 0o644,
        }
        if binding != expected:
            raise RunnerFailure(
                "corpus execution authority physical plan differs: "
                f"observed={binding!r} expected={expected!r}"
            )
        entries = descriptor_tree_manifest(
            root,
            root_mode=binding["root_mode"],
            directory_mode=binding["directory_mode"],
            file_mode=binding["file_mode"],
        )
        manifest_bytes = canonical_json_bytes(entries)
        self.corpus_execution_authority.append(
            {
                **binding,
                "entries": entries,
                "tree_sha256": sha256_bytes(manifest_bytes),
            }
        )

    @staticmethod
    def _validate_corpus_execution_record(record: dict[str, Any]) -> None:
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
        if type(record) is not dict or set(record) != record_fields:
            raise RunnerFailure(
                "corpus execution authority record fields differ"
            )
        for field in (
            "ordinal",
            "row_ordinal",
            "root_mode",
            "directory_mode",
            "file_mode",
        ):
            if type(record[field]) is not int:
                raise RunnerFailure(
                    f"corpus execution authority {field} type differs"
                )
        for field in ("role", "track", "variant", "root", "tree_sha256"):
            if type(record[field]) is not str:
                raise RunnerFailure(
                    f"corpus execution authority {field} type differs"
                )
        entries = record["entries"]
        if type(entries) is not list or not entries:
            raise RunnerFailure("corpus execution authority entries differ")
        paths: list[str] = []
        for entry in entries:
            if type(entry) is not dict:
                raise RunnerFailure(
                    "corpus execution authority entry is not an object"
                )
            kind = entry.get("kind")
            if type(kind) is not str:
                raise RunnerFailure(
                    "corpus execution authority entry kind type differs"
                )
            expected_fields = (
                {"path", "kind", "mode"}
                if kind == "directory"
                else {"path", "kind", "mode", "bytes", "sha256"}
                if kind == "file"
                else set()
            )
            if not expected_fields or set(entry) != expected_fields:
                raise RunnerFailure(
                    "corpus execution authority entry fields differ"
                )
            raw_path = entry["path"]
            if type(raw_path) is not str:
                raise RunnerFailure(
                    "corpus execution authority entry path is not text"
                )
            relative = Path(raw_path)
            if (
                not raw_path
                or relative.is_absolute()
                or relative.as_posix() != raw_path
                or any(part in {"", ".", ".."} for part in relative.parts)
            ):
                raise RunnerFailure(
                    "corpus execution authority entry path differs"
                )
            paths.append(raw_path)
            expected_mode = (
                record["directory_mode"]
                if kind == "directory"
                else record["file_mode"]
            )
            if type(entry["mode"]) is not int or entry["mode"] != expected_mode:
                raise RunnerFailure(
                    "corpus execution authority entry mode differs"
                )
            if kind == "file" and (
                type(entry["bytes"]) is not int
                or entry["bytes"] < 0
                or type(entry["sha256"]) is not str
                or not SHA256_RE.fullmatch(entry["sha256"])
            ):
                raise RunnerFailure(
                    "corpus execution authority file content fields differ"
                )
        if (
            len(paths) != len(set(paths))
            or paths != [
                str(entry["path"])
                for entry in sorted(
                    entries, key=lambda item: Path(str(item["path"]))
                )
            ]
        ):
            raise RunnerFailure(
                "corpus execution authority entry path order differs"
            )
        manifest_bytes = canonical_json_bytes(entries)
        if (
            not SHA256_RE.fullmatch(record["tree_sha256"])
            or sha256_bytes(manifest_bytes) != record["tree_sha256"]
        ):
            raise RunnerFailure(
                "corpus execution authority manifest hash differs"
            )
        live_entries = descriptor_tree_manifest(
            Path(str(record["root"])),
            root_mode=int(record["root_mode"]),
            directory_mode=int(record["directory_mode"]),
            file_mode=int(record["file_mode"]),
        )
        if live_entries != entries:
            raise RunnerFailure(
                "corpus execution authority differs from its live tree"
            )

    def _corpus_execution_authority_payload(
        self, *, correctness_only: bool
    ) -> bytes:
        if type(correctness_only) is not bool:
            raise RunnerFailure(
                "corpus execution authority correctness-only type differs"
            )
        expected = self._expected_corpus_execution_records(
            correctness_only=correctness_only
        )
        try:
            observed = [
                {
                    key: record[key]
                    for key in (
                        "ordinal",
                        "role",
                        "track",
                        "row_ordinal",
                        "variant",
                        "root",
                        "root_mode",
                        "directory_mode",
                        "file_mode",
                    )
                }
                for record in self.corpus_execution_authority
            ]
        except (KeyError, TypeError) as error:
            raise RunnerFailure(
                "corpus execution authority record binding differs"
            ) from error
        if observed != expected:
            raise RunnerFailure(
                "corpus execution authority is partial or rebound"
            )
        for record in self.corpus_execution_authority:
            self._validate_corpus_execution_record(record)
        return canonical_json_bytes(
            {
                "schema": CORPUS_EXECUTION_AUTHORITY_SCHEMA,
                "protocol": PROTOCOL,
                "protocol_sha256": PROTOCOL_SHA256,
                "attempt_nonce": self.attempt_nonce,
                "correctness_only": correctness_only,
                "records": self.corpus_execution_authority,
            }
        )

    @staticmethod
    def _verify_sealed_corpus_authority_fd(
        descriptor: int, expected_payload: bytes
    ) -> None:
        before = os.fstat(descriptor)
        seals = fcntl.fcntl(descriptor, F_GET_SEALS)
        payload = os.pread(descriptor, before.st_size, 0)
        after = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size != len(expected_payload)
            or payload != expected_payload
            or seals != CORPUS_EXECUTION_AUTHORITY_SEALS
            or (
                before.st_dev,
                before.st_ino,
                before.st_mode,
                before.st_nlink,
                before.st_size,
                before.st_mtime_ns,
                before.st_ctime_ns,
            )
            != (
                after.st_dev,
                after.st_ino,
                after.st_mode,
                after.st_nlink,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            )
        ):
            raise RunnerFailure("sealed corpus execution authority differs")
        try:
            decoded = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RunnerFailure(
                "sealed corpus execution authority is invalid JSON"
            ) from error
        if canonical_json_bytes(decoded) != payload:
            raise RunnerFailure(
                "sealed corpus execution authority is not canonical"
            )

    def _seal_corpus_execution_authority(
        self, *, correctness_only: bool
    ) -> int:
        payload = self._corpus_execution_authority_payload(
            correctness_only=correctness_only
        )
        descriptor = create_sealable_memfd(
            "asterism-corpus-execution-authority"
        )
        try:
            _write_all(
                descriptor, payload, "corpus execution authority memfd"
            )
            fcntl.fcntl(
                descriptor,
                F_ADD_SEALS,
                CORPUS_EXECUTION_AUTHORITY_SEALS,
            )
            self._verify_sealed_corpus_authority_fd(descriptor, payload)
        except BaseException:
            os.close(descriptor)
            raise
        return descriptor

    def seed_reopen_corpora(self) -> dict[str, dict[str, Any]]:
        corpora: dict[str, dict[str, Any]] = {}
        for ordinal, name in enumerate(PUBLIC_VARIANTS, start=1):
            root = self.attempt_scratch / "corpora" / f"{name}-archive-source"
            context = {
                "transition": "reopen_seed",
                "ordinal": ordinal,
                "variant": name,
                "domain_events": 2_000_000,
                "streams": 1_000,
                "batch": 10,
                "payload": 64,
                "segment_bytes": 8 * 1024 * 1024,
                "durability": "Process",
            }
            plan = self._variant_plan(
                context,
                mode="reopen_seed",
                track=None,
                store_path=root,
                require_store_after=True,
            )
            record = self.run_child(plan, controlled=True)
            seed = self._parse_canonical_lines(Path(record["raw_path"]), 1)[0]
            require_exact_keys(
                seed,
                {
                    "schema",
                    "protocol",
                    "protocol_sha256",
                    "variant",
                    "domain_events",
                    "visible_events",
                    "log_events",
                    "logical_digest",
                    "registry_head_digest",
                },
                f"reopen seed {name}",
            )
            if (
                seed["schema"] != "bn-2l3n-reopen-seed-v3"
                or seed["protocol"] != PROTOCOL
                or seed["protocol_sha256"] != PROTOCOL_SHA256
                or seed["variant"] != name
                or any(
                    isinstance(seed[field], bool)
                    or not isinstance(seed[field], int)
                    or seed[field] < 0
                    for field in ("domain_events", "visible_events", "log_events")
                )
                or not SHA256_RE.fullmatch(str(seed["logical_digest"]))
                or not SHA256_RE.fullmatch(str(seed["registry_head_digest"]))
            ):
                raise RunnerFailure(f"reopen seed evidence is invalid for {name}")
            if seed["domain_events"] != context["domain_events"]:
                raise RunnerFailure(f"reopen seed domain count differs for {name}")
            entries = tree_manifest(root)
            content = self._content_manifest(entries)
            manifest_path = self.output / "corpora" / f"{name}-manifest.json"
            manifest = {
                "schema": "asterism-rebaseline-corpus-manifest-v3",
                "protocol": PROTOCOL,
                "variant": name,
                "root": str(root.resolve()),
                "entries": entries,
                "content_sha256": sha256_bytes(canonical_json_bytes(content)),
                "seed_observations": seed,
                "verified_at": now(),
            }
            atomic_json(manifest_path, manifest)
            make_tree_read_only(root)
            replay = self._content_manifest(tree_manifest(root))
            if replay != content:
                raise RunnerFailure(f"read-only corpus replay differs for {name}")
            self._register_corpus_execution_tree(
                root,
                role="full-source",
                track="reopen_seed",
                row_ordinal=0,
                variant=name,
                read_only=True,
            )
            corpora[name] = {
                "root": root,
                "manifest": manifest,
                "manifest_path": manifest_path,
                "manifest_sha256": sha256(manifest_path),
                "content": content,
                "seed_observations": seed,
            }
        return corpora

    @staticmethod
    def _make_tree_writable(root: Path) -> None:
        root.chmod(0o755)
        for path in sorted(root.rglob("*")):
            if path.is_dir():
                path.chmod(0o755)
            elif path.is_file():
                path.chmod(0o644)

    def materialize_corpus(
        self,
        corpus: dict[str, Any],
        track: str,
        ordinal: int,
        variant: str,
    ) -> tuple[Path, dict[str, Any]]:
        source = corpus["root"]
        if self._content_manifest(tree_manifest(source)) != corpus["content"]:
            raise RunnerFailure(f"immutable corpus source changed for {variant}")
        destination = self._fresh_store(f"{track}-corpus", ordinal, variant)
        if destination.exists() or destination.is_symlink():
            raise RunnerFailure(f"corpus materialization is not fresh: {destination}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, destination, copy_function=shutil.copy2)
        copy_content = self._content_manifest(tree_manifest(destination))
        if copy_content != corpus["content"]:
            raise RunnerFailure(f"materialized corpus differs for {variant}")
        syncfs(destination)
        self._make_tree_writable(destination)
        if track == "smoke-reopen":
            authority_role = "specialized-copy"
            authority_track = (
                "smoke_reopen"
                if ordinal == 1
                else "smoke_structural_reopen"
            )
        elif track in {"reopen", "structural_traces"}:
            authority_role = "full-copy"
            authority_track = track
        else:
            raise RunnerFailure(
                f"corpus materialization track lacks authority: {track}"
            )
        self._register_corpus_execution_tree(
            destination,
            role=authority_role,
            track=authority_track,
            row_ordinal=ordinal,
            variant=variant,
            read_only=False,
        )
        content_sha = sha256_bytes(canonical_json_bytes(copy_content))
        return destination, {
            "archive_manifest_sha256": corpus["manifest"]["content_sha256"],
            "copy_manifest_sha256": content_sha,
            "copy_id": destination.name,
            "copy_verified_read_only": True,
        }

    def evidence_plans(
        self, track: str, corpora: dict[str, dict[str, Any]]
    ) -> Iterable[ChildPlan]:
        for identity in self.schema.expected_order(self.config, track):
            context = {"track": track, **identity}
            if track in {"primary", "cpu_profiles", "syscall_profiles"}:
                context["batches_per_writer"] = PRIMARY_BPW[
                    str(context["durability"])
                ][int(context["batch_size"])]
            elif track == "new_names":
                context["batches_per_writer"] = (
                    4_000 if context["durability"] == "Process" else 1_000
                )
            elif track == "fairness":
                context["batches_per_writer"] = FAIRNESS_BPW[
                    str(context["durability"])
                ][int(context["batch_size"])]
            variant = str(context["variant"])
            if track == "reopen":
                store, corpus_context = self.materialize_corpus(
                    corpora[variant], track, int(context["row_ordinal"]), variant
                )
                context.update(corpus_context)
                seed = corpora[variant]["seed_observations"]
                context.update(
                    {
                        "expected_domain_events": seed["domain_events"],
                        "expected_visible_events": seed["visible_events"],
                        "expected_log_events": seed["log_events"],
                        "expected_logical_digest": seed["logical_digest"],
                        "expected_registry_head_digest": seed[
                            "registry_head_digest"
                        ],
                    }
                )
                require_store = True
                store_absent_before = False
            elif track == "structural_traces" and context.get("trace_kind") == "reopen":
                store, corpus_context = self.materialize_corpus(
                    corpora[variant], track, int(context["row_ordinal"]), variant
                )
                context.update(corpus_context)
                seed = corpora[variant]["seed_observations"]
                context.update(
                    {
                        "expected_domain_events": seed["domain_events"],
                        "expected_visible_events": seed["visible_events"],
                        "expected_log_events": seed["log_events"],
                        "expected_logical_digest": seed["logical_digest"],
                        "expected_registry_head_digest": seed[
                            "registry_head_digest"
                        ],
                    }
                )
                require_store = True
                store_absent_before = False
            else:
                store = self._fresh_store(track, int(context["row_ordinal"]), variant)
                require_store = True
                store_absent_before = True
            yield self._variant_plan(
                context,
                track=track,
                store_path=store,
                require_store_after=require_store,
                store_absent_before=store_absent_before,
            )

    def _artifact_inventory(self, *, require_immutable: bool = False) -> list[dict[str, Any]]:
        if require_immutable:
            function = getattr(self.schema, "artifact_inventory", None)
            if not callable(function):
                raise RunnerFailure("shared artifact inventory authority is absent")
            try:
                inventory, _snapshots = function(self.output)
            except (OSError, ValueError) as error:
                raise RunnerFailure(
                    f"immutable artifact inventory failed: {error}"
                ) from error
            expected_fields = set(self.schema.ARTIFACT_INVENTORY_FIELDS)
            if any(set(item) != expected_fields for item in inventory):
                raise RunnerFailure("shared artifact inventory fields differ")
            return inventory
        inventory = []
        for path in sorted(self.output.rglob("*")):
            relative = path.relative_to(self.output)
            metadata = path.lstat()
            if any(part.startswith(".") for part in relative.parts):
                raise RunnerFailure(f"hidden artifact exists before freeze: {relative}")
            if stat.S_ISLNK(metadata.st_mode):
                raise RunnerFailure(f"artifact symlink exists before freeze: {relative}")
            if stat.S_ISDIR(metadata.st_mode):
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise RunnerFailure(f"non-regular artifact exists before freeze: {relative}")
            item = {
                "path": relative.as_posix(),
                "bytes": metadata.st_size,
                "sha256": sha256(path),
                "mode": stat.S_IMODE(metadata.st_mode),
            }
            expected_fields = set(
                getattr(
                    self.schema,
                    "ARTIFACT_INVENTORY_FIELDS",
                    ("path", "bytes", "sha256", "mode"),
                )
            )
            if set(item) != expected_fields:
                raise RunnerFailure("artifact inventory fields differ from shared authority")
            inventory.append(item)
        return inventory

    def _publish_sha256sums(self, inventory: list[dict[str, Any]]) -> Path:
        sums_path = self.output / "SHA256SUMS"
        forbidden = {
            "SHA256SUMS",
            "terminal-pre-release.json",
            "lease-release.json",
            "terminal.json",
            "terminal-verification.json",
            "terminal_pre_release.json",
            "lease_release.json",
        }
        paths = [str(item["path"]) for item in inventory]
        if paths != sorted(paths) or len(paths) != len(set(paths)):
            raise RunnerFailure("nonterminal artifact inventory is not uniquely sorted")
        if forbidden.intersection(paths) or any(
            (self.output / name).exists() or (self.output / name).is_symlink()
            for name in forbidden
        ):
            raise RunnerFailure("terminal/SHA artifact exists before SHA256SUMS freeze")
        required = {"REPORT.md", "result.json", "evaluator-transition.json"}
        if not required.issubset(paths):
            raise RunnerFailure(
                f"nonterminal inventory misses evaluator artifacts: {sorted(required - set(paths))}"
            )
        payload = b"".join(
            f"{item['sha256']}  {item['path']}\n".encode("utf-8")
            for item in inventory
        )
        atomic_write(sums_path, payload, mode=0o444)
        return sums_path

    @staticmethod
    def _decode_mountinfo_field(value: str) -> str:
        replacements = {
            "040": " ",
            "011": "\t",
            "012": "\n",
            "134": "\\",
        }
        return re.sub(
            r"\\(040|011|012|134)",
            lambda match: replacements[match.group(1)],
            value,
        )

    def _filesystem_identity(self) -> dict[str, Any]:
        resolved = self.scratch_root.resolve()
        chosen: dict[str, Any] | None = None

        for line in Path("/proc/self/mountinfo").read_text().splitlines():
            before, separator, after = line.partition(" - ")
            if not separator:
                continue
            fields = before.split()
            tail = after.split()
            if len(fields) < 6 or len(tail) < 3:
                continue
            target = Path(self._decode_mountinfo_field(fields[4]))
            try:
                resolved.relative_to(target)
            except ValueError:
                continue
            candidate = {
                "mount_id": fields[0],
                "parent_mount_id": fields[1],
                "device": fields[2],
                "root": self._decode_mountinfo_field(fields[3]),
                "target": str(target),
                "mount_options": self._decode_mountinfo_field(fields[5]),
                "filesystem_type": tail[0],
                "source": self._decode_mountinfo_field(tail[1]),
                "super_options": self._decode_mountinfo_field(tail[2]),
            }
            if chosen is None or len(candidate["target"]) > len(chosen["target"]):
                chosen = candidate
        if chosen is None:
            raise RunnerFailure(f"cannot resolve mount identity for {resolved}")
        return chosen

    def _validate_host_admission(
        self,
        filesystem: dict[str, Any],
        free_bytes: int,
        free_inodes: int,
    ) -> None:
        require_exact_keys(
            filesystem,
            set(self.schema.PROVENANCE_FILESYSTEM_FIELDS),
            "measurement filesystem",
        )
        if filesystem.get("filesystem_type") != REQUIRED_FILESYSTEM_TYPE:
            raise RunnerFailure(
                "measurement filesystem differs: "
                f"{filesystem.get('filesystem_type')} != {REQUIRED_FILESYSTEM_TYPE}"
            )
        if (
            isinstance(free_bytes, bool)
            or not isinstance(free_bytes, int)
            or free_bytes < self.minimum_free_bytes
        ):
            raise RunnerFailure(
                f"initial free bytes {free_bytes!r} below {self.minimum_free_bytes}"
            )
        if (
            isinstance(free_inodes, bool)
            or not isinstance(free_inodes, int)
            or free_inodes < self.minimum_free_inodes
        ):
            raise RunnerFailure(
                f"initial free inodes {free_inodes!r} below {self.minimum_free_inodes}"
            )

    def host_resource_preflight(self) -> None:
        self.phase = "host_resource_preflight"
        filesystem = self._filesystem_identity()
        resource = os.statvfs(self.scratch_root)
        free_bytes = resource.f_bavail * resource.f_frsize
        free_inodes = resource.f_favail
        self._validate_host_admission(filesystem, free_bytes, free_inodes)
        self.initial_filesystem = filesystem
        self.initial_free_bytes = free_bytes
        self.initial_free_inodes = free_inodes

    def assert_host_admission_stable(self) -> None:
        if (
            self.initial_filesystem is None
            or self.initial_free_bytes is None
            or self.initial_free_inodes is None
        ):
            raise RunnerFailure("initial host/resource admission is absent")
        filesystem = self._filesystem_identity()
        if filesystem != self.initial_filesystem:
            raise RunnerFailure("measurement filesystem identity changed after admission")
        resource = os.statvfs(self.scratch_root)
        self._validate_host_admission(
            filesystem,
            resource.f_bavail * resource.f_frsize,
            resource.f_favail,
        )

    @staticmethod
    def _parse_cpu_list(value: str) -> list[int]:
        result: set[int] = set()
        for item in value.strip().split(","):
            if not item:
                continue
            start, separator, end = item.partition("-")
            try:
                first = int(start)
                last = int(end) if separator else first
            except ValueError as error:
                raise RunnerFailure(f"invalid Linux CPU list: {value!r}") from error
            if first < 0 or last < first:
                raise RunnerFailure(f"invalid Linux CPU range: {item!r}")
            result.update(range(first, last + 1))
        if not result:
            raise RunnerFailure("Linux online CPU list is empty")
        return sorted(result)

    @classmethod
    def _cpu_topology(cls) -> dict[str, int]:
        cpu_root = Path("/sys/devices/system/cpu")
        cpus = sorted(
            int(path.name[3:])
            for path in cpu_root.iterdir()
            if re.fullmatch(r"cpu[0-9]+", path.name) and path.is_dir()
        )
        if not cpus:
            raise RunnerFailure("sysfs CPU topology is empty")
        packages: set[int] = set()
        cores: set[tuple[int, int]] = set()
        for cpu in cpus:
            topology = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
            try:
                package = int((topology / "physical_package_id").read_text().strip())
                core = int((topology / "core_id").read_text().strip())
            except (OSError, ValueError) as error:
                raise RunnerFailure(f"cannot read CPU {cpu} topology: {error}") from error
            key = (package, core)
            packages.add(package)
            cores.add(key)
        if len(cpus) % len(cores):
            raise RunnerFailure(
                f"logical CPU/core topology is not exactly divisible: {len(cpus)}/{len(cores)}"
            )
        return {
            "logical_cpus": len(cpus),
            "physical_packages": len(packages),
            "cores": len(cores),
            "threads_per_core": len(cpus) // len(cores),
        }

    @staticmethod
    def _cpu_model() -> str:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            key, separator, value = line.partition(":")
            if separator and key.strip() == "model name":
                model = value.strip()
                if model:
                    return model
        raise RunnerFailure("cannot identify CPU model")

    @staticmethod
    def _cpu_governors() -> dict[str, str]:
        result: dict[str, str] = {}
        root = Path("/sys/devices/system/cpu/cpufreq")
        for policy in sorted(root.glob("policy*")):
            governor = policy / "scaling_governor"
            if governor.is_file():
                result[str(policy.resolve())] = governor.read_text().strip()
        if not result:
            raise RunnerFailure("CPU governor policies are unavailable")
        return result

    @staticmethod
    def _turbo_state() -> dict[str, str]:
        def read(path: str) -> str:
            candidate = Path(path)
            return candidate.read_text().strip() if candidate.is_file() else "not_available"

        return {
            "intel_pstate_no_turbo": read(
                "/sys/devices/system/cpu/intel_pstate/no_turbo"
            ),
            "cpufreq_boost": read("/sys/devices/system/cpu/cpufreq/boost"),
        }

    @staticmethod
    def _memory_bytes() -> int:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        if pages <= 0 or page_size <= 0:
            raise RunnerFailure("sysconf physical memory is unavailable")
        return pages * page_size

    @staticmethod
    def _kernel() -> str:
        return " ".join(os.uname())

    @staticmethod
    def _scheduler_state(
        filesystem: dict[str, Any], *, sys_dev_root: Path = Path("/sys/dev/block")
    ) -> dict[str, str]:
        logical = str(filesystem["device"])
        source = str(filesystem["source"])
        link = sys_dev_root / logical
        try:
            subject = link.resolve(strict=True)
        except OSError as error:
            if source.startswith("/dev/"):
                raise RunnerFailure(
                    f"cannot resolve scheduler device {logical} for {source}: {error}"
                ) from error
            return {
                "logical_device": logical,
                "base_device": "not_available",
                "scheduler_path": "not_available",
                "scheduler_value": "not_available",
            }
        for candidate in (subject, *subject.parents):
            scheduler_path = candidate / "queue" / "scheduler"
            if not scheduler_path.is_file():
                continue
            value = scheduler_path.read_text().strip()
            if not value:
                raise RunnerFailure(f"scheduler file is empty: {scheduler_path}")
            return {
                "logical_device": logical,
                "base_device": candidate.name,
                "scheduler_path": str(scheduler_path.resolve(strict=True)),
                "scheduler_value": value,
            }
        if source.startswith("/dev/"):
            raise RunnerFailure(
                f"cannot find parent block scheduler for {source} ({logical})"
            )
        return {
            "logical_device": logical,
            "base_device": "not_available",
            "scheduler_path": "not_available",
            "scheduler_value": "not_available",
        }

    def write_initial_provenance(self) -> None:
        self.phase = "provenance_initial"
        self.assert_host_admission_stable()
        require_exact_keys(
            self.config,
            set(self.schema.CONFIG_FIELDS),
            "runner config",
        )
        atomic_json(self.config_path, self.config)
        for track in TRACK_CARDINALITY:
            schema_order = self.schema.expected_order(self.config, track)
            if len(schema_order) != TRACK_CARDINALITY[track]:
                raise RunnerFailure(f"{track} cardinality differs before row zero")
        if (
            self.initial_filesystem is None
            or self.initial_free_bytes is None
            or self.initial_free_inodes is None
        ):
            raise RunnerFailure("initial host/resource admission is absent")
        filesystem = self.initial_filesystem
        topology = self._cpu_topology()
        cpu_count = os.cpu_count()
        if cpu_count is None or cpu_count <= 0:
            raise RunnerFailure("logical CPU count is unavailable")
        evaluator = self.prepared.support_files["evaluator"]
        verifier = self.prepared.support_files["terminal_verifier"]
        correctness = self.prepared.tools["correctness"]
        schema_path = self.prepared.support_files["evidence_schema"].path
        profile_path = self.prepared.support_files["profile_adapter"].path
        self.provenance = {
            "schema": PROVENANCE_SCHEMA,
            "protocol": PROTOCOL,
            "protocol_sha256": PROTOCOL_SHA256,
            "evidence_mode": "admission",
            "rehearsal": self.rehearsal,
            "declaration_sha256": self.declaration_sha256,
            "attempt_nonce": self.attempt_nonce,
            "output_dir": str(self.output.resolve()),
            "output_dir_absent_before": self.rehearsal,
            "source_approval_path": str(
                (self.output / "source-approval.json").resolve()
            ),
            "source_approval_sha256": self.prepared.source_approval_sha256,
            "config_path": str(self.config_path.resolve()),
            "config_sha256": sha256(self.config_path),
            "prepared_artifacts_path": str(
                (self.output / "prepared-artifacts.json").resolve()
            ),
            "prepared_artifacts_sha256": self.prepared.digest,
            "runner_path": str(Path(__file__).resolve()),
            "runner_sha256": sha256(Path(__file__).resolve()),
            "evaluator_path": str(evaluator.path),
            "evaluator_sha256": evaluator.sha256,
            "terminal_verifier_path": str(verifier.path),
            "terminal_verifier_sha256": verifier.sha256,
            "schema_path": str(schema_path),
            "schema_sha256": sha256(schema_path),
            "profile_adapter_path": str(profile_path),
            "profile_adapter_sha256": sha256(profile_path),
            "profile_contract_path": str(
                (self.output / "profile-contract.json").resolve()
            ),
            "profile_contract_sha256": sha256(
                self.output / "profile-contract.json"
            ),
            "correctness_path": str((self.output / "correctness.json").resolve()),
            "correctness_sha256": None,
            "correctness_executable_path": str(correctness.path),
            "correctness_executable_sha256": correctness.sha256,
            "csv_artifacts": {},
            "raw_manifest_path": str(self.raw_manifest.resolve()),
            "raw_manifest_sha256": None,
            "guard_manifest_path": str(self.guard_manifest.resolve()),
            "guard_manifest_sha256": None,
            "child_manifest_path": str(self.child_manifest.resolve()),
            "child_manifest_sha256": None,
            "lease": self.lease,
            "host": {
                "runner": self.runner_identity,
                "hostname": socket.gethostname(),
                "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                "kernel": self._kernel(),
                "cpu_model": self._cpu_model(),
                "cpu_topology": topology,
                "governors": self._cpu_governors(),
                "turbo": self._turbo_state(),
                "affinity": sorted(os.sched_getaffinity(0)),
                "page_size": os.sysconf("SC_PAGE_SIZE"),
                "cpu_count": cpu_count,
                "memory_bytes": self._memory_bytes(),
                "filesystem": filesystem,
                "scheduler": self._scheduler_state(filesystem),
                "uid": os.getuid(),
                "scratch_root": str(self.scratch_root.resolve()),
                "scratch_free_bytes_initial": self.initial_free_bytes,
                "scratch_free_inodes_initial": self.initial_free_inodes,
                "scratch_free_bytes_final": None,
                "scratch_free_inodes_final": None,
                "tracked_comm": sorted(self.prepared.tracked_comm),
                "frozen_files": self.frozen_files,
                "guard_records": None,
                "child_records": None,
                "resource_manifest_path": str(self.resource_manifest.resolve()),
                "resource_manifest_sha256": None,
                "correctness_manifest_path": str(
                    self.correctness_manifest.resolve()
                ),
                "correctness_manifest_sha256": None,
            },
            "started_at": now(),
            "started_monotonic_ns": time.monotonic_ns(),
            "completed_at": None,
            "completed_monotonic_ns": None,
            "partial": True,
            "failure_absent": True,
        }
        require_exact_keys(
            self.provenance,
            set(self.schema.PROVENANCE_FIELDS),
            "initial provenance",
        )
        require_exact_keys(
            self.provenance["host"],
            set(self.schema.PROVENANCE_HOST_FIELDS),
            "initial provenance host",
        )
        atomic_json(self.provenance_path, self.provenance)

    def finalize_provenance(self, *, correctness_only: bool = False) -> None:
        self.phase = "provenance_final"
        self.assert_host_admission_stable()
        self.verify_frozen()
        csv_bindings: dict[str, Any] = {}
        if correctness_only:
            present = [
                path
                for path in self.csv_paths.values()
                if path.exists() or path.is_symlink()
            ]
            if present or not (self.output / "correctness-only.json").is_file():
                raise RunnerFailure(
                    "correctness-only provenance has timing CSVs or lacks its marker"
                )
        else:
            for track, expected_rows in TRACK_CARDINALITY.items():
                path = self.csv_paths[track]
                fields = tuple(self.schema.CSV_FIELDS_BY_TRACK[track])
                shape = csv_shape(path, fields)
                if not shape["complete"] or shape["rows"] != expected_rows:
                    raise RunnerFailure(f"final {track} CSV shape differs: {shape}")
                csv_bindings[track] = {
                    "path": str(path.resolve()),
                    "sha256": sha256(path),
                    "bytes": shape["bytes"],
                    "rows": shape["rows"],
                    "columns": len(fields),
                }
        observed = os.statvfs(self.scratch_root)
        self.provenance["evidence_mode"] = (
            "correctness-only" if correctness_only else "admission"
        )
        self.provenance["csv_artifacts"] = csv_bindings
        correctness_path = self.output / "correctness.json"
        if not correctness_path.is_file():
            raise RunnerFailure("correctness aggregate is absent")
        self.provenance["correctness_sha256"] = sha256(correctness_path)
        self.provenance["raw_manifest_sha256"] = sha256(self.raw_manifest)
        self.provenance["guard_manifest_sha256"] = sha256(self.guard_manifest)
        self.provenance["child_manifest_sha256"] = sha256(self.child_manifest)
        self.provenance["host"].update(
            {
                "scratch_free_bytes_final": observed.f_bavail * observed.f_frsize,
                "scratch_free_inodes_final": observed.f_favail,
                "guard_records": self.guard_count,
                "child_records": self.child_count,
                "resource_manifest_path": str(self.resource_manifest.resolve()),
                "resource_manifest_sha256": sha256(self.resource_manifest),
                "correctness_manifest_path": str(self.correctness_manifest.resolve()),
                "correctness_manifest_sha256": sha256(self.correctness_manifest),
            }
        )
        self.provenance["completed_at"] = now()
        self.provenance["completed_monotonic_ns"] = time.monotonic_ns()
        self.provenance["partial"] = False
        require_exact_keys(
            self.provenance["host"],
            set(self.schema.PROVENANCE_HOST_FIELDS),
            "final provenance host",
        )
        require_exact_keys(
            self.provenance,
            set(self.schema.PROVENANCE_FIELDS),
            "final provenance",
        )
        atomic_json(self.provenance_path, self.provenance)

    def _run_contract_smoke(self) -> None:
        if self.profile_tool_driver is None:
            raise RunnerFailure(
                "runner-owned perf/strace driver is absent before row zero"
            )
        self.phase = "contract_smoke"
        for plan in self.contract_plans():
            record = self.run_child(plan, controlled=False)
            output = self._parse_canonical_lines(Path(record["raw_path"]), 1)[0]
            expected = self.prepared.variants[str(plan.context["variant"])].contract
            if output != expected:
                raise RunnerFailure(
                    f"variant {plan.context['variant']} contract readback differs"
                )
        self.phase = "transition_smoke"
        for plan in self.smoke_plans():
            self.run_child(plan, controlled=plan.controlled)
        self.phase = "specialized_reopen_smoke"
        self._run_specialized_reopen_smokes()

    @staticmethod
    def _correctness_key(
        descriptor: dict[str, Any],
    ) -> tuple[str, str, str, str, str]:
        return (
            str(descriptor["id"]),
            str(descriptor["variant"]),
            str(descriptor["phase"]),
            str(descriptor["suite"]),
            str(descriptor["kind"]),
        )

    def _validate_correctness_child(
        self,
        plan: ChildPlan,
        value: dict[str, Any],
        record: dict[str, Any],
    ) -> dict[str, Any]:
        if plan.kind not in {"correctness", "fault"}:
            raise RunnerFailure("non-correctness child reached correctness validation")
        execution_matches = [
            item
            for item in self.config.get("correctness_execution", [])
            if item.get("variant") == plan.context.get("variant")
            and item.get("phase") == plan.context.get("phase")
            and item.get("suite") == plan.context.get("suite")
            and item.get("kind") == plan.kind
        ]
        if len(execution_matches) != 1:
            raise RunnerFailure("correctness execution authority is absent or ambiguous")
        execution = execution_matches[0]
        execution_fields = set(
            getattr(
                self.schema,
                "CORRECTNESS_EXECUTION_FIELDS",
                (
                    "variant",
                    "phase",
                    "suite",
                    "kind",
                    "executable_path",
                    "executable_sha256",
                    "executable_mode",
                    "executable_comm",
                    "argv",
                    "environment",
                ),
            )
        )
        require_exact_keys(execution, execution_fields, "correctness execution")
        if execution != self._correctness_execution_record(plan):
            raise RunnerFailure("correctness plan differs from config authority")
        record_binding = {
            "executable_path": record.get("executable_path"),
            "executable_sha256": record.get("executable_sha256"),
            "executable_mode": record.get("executable_mode"),
            "executable_comm": record.get("executable_comm"),
            "argv": record.get("argv"),
        }
        if record_binding != {
            field: execution[field]
            for field in (
                "executable_path",
                "executable_sha256",
                "executable_mode",
                "executable_comm",
                "argv",
            )
        }:
            raise RunnerFailure("correctness child executable/argv binding differs")
        child_environment = record.get("environment")
        if not isinstance(child_environment, dict) or any(
            child_environment.get(key) != expected
            for key, expected in execution["environment"].items()
        ):
            raise RunnerFailure("correctness child environment binding differs")
        require_exact_keys(
            value,
            set(self.schema.CORRECTNESS_CHILD_FIELDS),
            "correctness child",
        )
        expected_identity = {
            "schema": self.schema.CORRECTNESS_CHILD_SCHEMA,
            "protocol": PROTOCOL,
            "attempt_nonce": self.attempt_nonce,
            "variant": plan.context["variant"],
            "phase": plan.context["phase"],
            "suite": plan.context["suite"],
            "harness_sound": True,
        }
        for field, expected in expected_identity.items():
            if value.get(field) != expected:
                raise RunnerFailure(
                    f"correctness child {field} differs: {value.get(field)!r} != {expected!r}"
                )
        if record["stderr_bytes"] != 0:
            raise RunnerFailure("correctness child wrote stderr")
        suite = str(plan.context["suite"])
        expected_bounds = (
            dict(self.schema.CORRECTNESS_EXPECTED_BOUNDEDNESS)
            if plan.context["variant"] == "A" and suite == "current-fault"
            else None
        )
        if value.get("boundedness") != expected_bounds:
            raise RunnerFailure("correctness child boundedness differs")
        if expected_bounds is not None:
            require_exact_keys(
                value["boundedness"],
                set(self.schema.CORRECTNESS_BOUNDEDNESS_FIELDS),
                "correctness child boundedness",
            )
            prior = self.correctness_boundedness.get(str(plan.context["phase"]))
            if prior is not None:
                raise RunnerFailure("duplicate correctness boundedness phase")
            other_phase = "post" if plan.context["phase"] == "pre" else "pre"
            other = self.correctness_boundedness.get(other_phase)
            if other is not None and canonical_json_bytes(other) != canonical_json_bytes(
                expected_bounds
            ):
                raise RunnerFailure("correctness fault pre/post boundedness differs")

        cases = value.get("cases")
        expected_cases = plan.context["expected_cases"]
        if not isinstance(cases, list) or len(cases) != len(expected_cases):
            raise RunnerFailure("correctness child case cardinality differs")
        local: list[tuple[tuple[str, str, str, str, str], dict[str, Any]]] = []
        child_case_keys: set[tuple[str, str, str, str, str]] = set()
        approved_by_id = {
            descriptor["id"]: descriptor
            for descriptor in self.config["correctness_cases"]
            if descriptor["variant"] == plan.context["variant"]
            and descriptor["phase"] == plan.context["phase"]
            and descriptor["suite"] == suite
            and descriptor["kind"] == plan.kind
        }
        if len(approved_by_id) != len(expected_cases):
            raise RunnerFailure("approved correctness partition cardinality differs")
        for ordinal, (case, expected_case) in enumerate(
            zip(cases, expected_cases, strict=True), start=1
        ):
            if not isinstance(case, dict):
                raise RunnerFailure(f"correctness child case {ordinal} is not an object")
            require_exact_keys(
                case,
                set(self.schema.CORRECTNESS_CHILD_CASE_FIELDS),
                f"correctness child case {ordinal}",
            )
            if (
                case.get("id") != expected_case["id"]
                or case.get("classification") != expected_case["classification"]
                or case.get("status") not in {"PASS", "FAIL"}
            ):
                raise RunnerFailure(f"correctness child case {ordinal} differs")
            approved = approved_by_id.get(case["id"])
            if approved is None or approved["classification"] != case["classification"]:
                raise RunnerFailure(f"correctness child case {ordinal} partition differs")
            key = self._correctness_key(approved)
            if key in child_case_keys or key in self.correctness_observations:
                raise RunnerFailure(f"duplicate correctness case descriptor: {key}")
            child_case_keys.add(key)
            aggregate_case = {
                **approved,
                "status": case["status"],
                "child_ordinal": record["ordinal"],
                "output_path": record["raw_path"],
                "output_sha256": record["raw_sha256"],
            }
            require_exact_keys(
                aggregate_case,
                set(self.schema.CORRECTNESS_AGGREGATE_CASE_FIELDS),
                f"correctness aggregate case {ordinal}",
            )
            local.append((key, aggregate_case))

        if expected_bounds is not None:
            self.correctness_boundedness[str(plan.context["phase"])] = expected_bounds
        for key, aggregate_case in local:
            self.correctness_observations[key] = aggregate_case
        return value

    def _publish_correctness_aggregate(self) -> None:
        path = self.output / "correctness.json"
        if path.exists() or path.is_symlink():
            raise RunnerFailure("correctness aggregate destination is not fresh")
        approved = self.config["correctness_cases"]
        cases: list[dict[str, Any]] = []
        expected_keys: set[tuple[str, str, str, str, str]] = set()
        for descriptor in approved:
            key = self._correctness_key(descriptor)
            if key in expected_keys:
                raise RunnerFailure(f"approved correctness descriptor is duplicated: {key}")
            expected_keys.add(key)
            observation = self.correctness_observations.get(key)
            if observation is None:
                raise RunnerFailure(f"approved correctness descriptor is missing: {key}")
            cases.append(observation)
        if set(self.correctness_observations) != expected_keys:
            raise RunnerFailure("unapproved correctness observations are present")
        pre_bounds = self.correctness_boundedness.get("pre")
        post_bounds = self.correctness_boundedness.get("post")
        if (
            pre_bounds is None
            or post_bounds is None
            or canonical_json_bytes(pre_bounds) != canonical_json_bytes(post_bounds)
        ):
            raise RunnerFailure("correctness fault bounds are absent or differ")
        aggregate = {
            "schema": self.schema.CORRECTNESS_SCHEMA,
            "protocol": PROTOCOL,
            "attempt_nonce": self.attempt_nonce,
            "harness_sound": True,
            "boundedness": pre_bounds,
            "cases": cases,
        }
        require_exact_keys(
            aggregate,
            set(self.schema.CORRECTNESS_AGGREGATE_FIELDS),
            "correctness aggregate",
        )
        atomic_create_json(path, aggregate, mode=0o444)
        fsync_dir(path.parent)

    def _run_correctness(self, phase: str) -> None:
        self.phase = f"correctness_{phase}"
        for plan in self.correctness_plans(phase):
            self.run_child(plan, controlled=True)
        expected = {
            self._correctness_key(descriptor)
            for descriptor in self.config["correctness_cases"]
            if descriptor["phase"] in ({"oracle", "pre"} if phase == "pre" else {"post"})
        }
        observed = {
            key
            for key in self.correctness_observations
            if key[2] in ({"oracle", "pre"} if phase == "pre" else {"post"})
        }
        if observed != expected:
            raise RunnerFailure(f"correctness {phase} coverage differs")
        if phase == "post":
            self._publish_correctness_aggregate()

    def _pre_correctness_failures(
        self,
    ) -> tuple[list[str], list[dict[str, str]]]:
        current = sorted(
            {
                observation["id"]
                for observation in self.correctness_observations.values()
                if observation["variant"] == "A"
                and observation["phase"] == "pre"
                and observation["status"] == "FAIL"
            }
        )
        historical = sorted(
            (
                {
                    "variant": observation["variant"],
                    "phase": observation["phase"],
                    "id": observation["id"],
                }
                for observation in self.correctness_observations.values()
                if observation["variant"] in {"C", "D"}
                and observation["phase"] == "oracle"
                and observation["status"] == "FAIL"
            ),
            key=lambda item: (item["variant"], item["phase"], item["id"]),
        )
        return current, historical

    def _post_current_failure_ids(self) -> list[str]:
        return sorted(
            {
                observation["id"]
                for observation in self.correctness_observations.values()
                if observation["variant"] == "A"
                and observation["phase"] == "post"
                and observation["status"] == "FAIL"
            }
        )

    def _publish_correctness_only_marker(
        self,
        current_pre_failed_case_ids: list[str],
        current_post_failed_case_ids: list[str],
        historical_failed_cases: list[dict[str, str]],
    ) -> None:
        if (
            current_pre_failed_case_ids
            != sorted(set(current_pre_failed_case_ids))
            or current_post_failed_case_ids
            != sorted(set(current_post_failed_case_ids))
            or historical_failed_cases
            != sorted(
                historical_failed_cases,
                key=lambda item: (
                    item.get("variant", ""),
                    item.get("phase", ""),
                    item.get("id", ""),
                ),
            )
            or len(
                {
                    (item.get("variant"), item.get("phase"), item.get("id"))
                    for item in historical_failed_cases
                }
            )
            != len(historical_failed_cases)
            or not current_pre_failed_case_ids
            and not historical_failed_cases
        ):
            raise RunnerFailure("correctness-only failure identities are not exact")
        for item in historical_failed_cases:
            require_exact_keys(
                item,
                set(
                    getattr(
                        self.schema,
                        "CORRECTNESS_ONLY_HISTORICAL_FAILURE_FIELDS",
                        ("variant", "phase", "id"),
                    )
                ),
                "correctness-only historical failure",
            )
        child_records = (
            [
                json.loads(line)
                for line in self.child_manifest.read_text().splitlines()
            ]
            if self.child_manifest.is_file()
            else []
        )
        timing_children = [
            record
            for record in child_records
            if record.get("kind") in TRACK_CARDINALITY
        ]
        present_csv = [
            path for path in self.csv_paths.values() if path.exists() or path.is_symlink()
        ]
        if timing_children or present_csv:
            raise RunnerFailure(
                "correctness-only outcome was requested after timing evidence existed"
            )
        marker = {
            "schema": CORRECTNESS_ONLY_SCHEMA,
            "protocol": PROTOCOL,
            "attempt_nonce": self.attempt_nonce,
            "trigger": (
                "mixed"
                if current_pre_failed_case_ids and historical_failed_cases
                else (
                    "current"
                    if current_pre_failed_case_ids
                    else "historical"
                )
            ),
            "current_pre_failed_case_ids": current_pre_failed_case_ids,
            "current_post_failed_case_ids": current_post_failed_case_ids,
            "historical_failed_cases": historical_failed_cases,
            "timing_child_records": 0,
            "created_at": now(),
            "created_monotonic_ns": time.monotonic_ns(),
        }
        require_exact_keys(
            marker,
            set(
                getattr(
                    self.schema,
                    "CORRECTNESS_ONLY_FIELDS",
                    tuple(marker),
                )
            ),
            "correctness-only marker",
        )
        if getattr(
            self.schema, "CORRECTNESS_ONLY_SCHEMA", CORRECTNESS_ONLY_SCHEMA
        ) != CORRECTNESS_ONLY_SCHEMA:
            raise RunnerFailure("shared correctness-only schema differs")
        path = self.output / "correctness-only.json"
        atomic_write(path, canonical_json_bytes(marker), mode=0o444)
        self.frozen_files[str(path.resolve())] = sha256(path)

    def _run_matrix(self, track: str, corpora: dict[str, dict[str, Any]]) -> None:
        self.phase = track
        expected = self.schema.expected_order(self.config, track)
        if len(expected) != TRACK_CARDINALITY[track]:
            raise RunnerFailure(f"physical order changed before {track}")
        for plan in self.evidence_plans(track, corpora):
            self.run_child(plan, controlled=True)

    def _lease_held_proof(self) -> dict[str, Any]:
        if self.lease_handle is None or self.lease is None:
            raise RunnerFailure("measurement lease is not held")
        metadata = os.fstat(self.lease_handle.fileno())
        if (
            metadata.st_dev != self.lease["device"]
            or metadata.st_ino != self.lease["inode"]
        ):
            raise RunnerFailure("held lease identity changed")
        second = self.lock_path.open("a+b", buffering=0)
        try:
            try:
                fcntl.flock(second.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                second_failed = True
            else:
                second_failed = False
                fcntl.flock(second.fileno(), fcntl.LOCK_UN)
        finally:
            second.close()
        if not second_failed:
            raise RunnerFailure("lease reproof second exclusive lock succeeded")
        return {
            "path": self.lease["path"],
            "device": metadata.st_dev,
            "inode": metadata.st_ino,
            "holder_pid": self.runner_identity["pid"],
            "holder_start_ticks": self.runner_identity["starttime_ticks"],
            "nonce": self.lease["nonce"],
            "proc_locks_proof": self._proc_locks_proof(
                metadata.st_dev, metadata.st_ino
            ),
            "second_exclusive_failed": True,
            "observed_at": now(),
            "observed_monotonic_ns": time.monotonic_ns(),
        }

    @staticmethod
    def _close_descriptors_with_retry(
        descriptors: dict[str, int | None]
    ) -> list[str]:
        pending = dict(descriptors)
        failures: dict[str, BaseException] = {}
        for _attempt in range(2):
            failures = {}
            for label, descriptor in tuple(pending.items()):
                if descriptor is None:
                    pending.pop(label, None)
                    continue
                try:
                    os.close(descriptor)
                except OSError as error:
                    if error.errno == 9:
                        pending.pop(label, None)
                    else:
                        failures[label] = error
                else:
                    pending.pop(label, None)
            if not pending:
                break
        return [f"close {label}: {failures[label]!r}" for label in pending]

    def _abort_owned_evaluator(self) -> None:
        cleanup_errors: list[str] = []
        descriptor = self.active_evaluator_authority_fd
        if descriptor is not None:
            cleanup_errors.extend(
                self._close_descriptors_with_retry({"authority": descriptor})
            )
            if not cleanup_errors:
                self.active_evaluator_authority_fd = None
        process = self.active_evaluator_process
        group_present = (
            process is not None and process_group_exists(process.pid)
        )
        if process is not None and (process.poll() is None or group_present):
            last_error: BaseException | None = None
            for _attempt in range(2):
                try:
                    terminate_process_group(process)
                except BaseException as error:
                    last_error = error
                else:
                    last_error = None
                    break
            if last_error is not None:
                cleanup_errors.append(
                    f"terminate evaluator: {last_error!r}"
                )
        if process is None or (
            process.poll() is not None
            and not process_group_exists(process.pid)
        ):
            self.active_evaluator_process = None
        if cleanup_errors:
            raise RunnerFailure(
                f"cannot release evaluator ownership: {cleanup_errors}",
                exit_code=30,
            )

    def _spawn_evaluator_process(
        self,
        argv: tuple[str, ...],
        evaluator: Executable,
        stdout_fd: int,
        stderr_fd: int,
        authority_fd: int,
    ) -> subprocess.Popen[Any]:
        self.active_evaluator_authority_fd = authority_fd
        process: subprocess.Popen[Any] | None = None
        try:
            process = subprocess.Popen(
                argv,
                executable=str(evaluator.path),
                stdin=subprocess.DEVNULL,
                stdout=stdout_fd,
                stderr=stderr_fd,
                env={
                    "LANG": "C.UTF-8",
                    "LC_ALL": "C.UTF-8",
                    "TZ": "UTC",
                    "ASTERISM_REBASELINE_MODE": "evaluate",
                    CORPUS_EXECUTION_AUTHORITY_FD_ENVIRONMENT: str(
                        authority_fd
                    ),
                },
                pass_fds=(authority_fd,),
                start_new_session=True,
            )
            self.active_evaluator_process = process
        except BaseException as error:
            cleanup_errors = self._close_descriptors_with_retry(
                {
                    "corpus authority": authority_fd,
                    "stdout": stdout_fd,
                    "stderr": stderr_fd,
                }
            )
            if not cleanup_errors:
                self.active_evaluator_authority_fd = None
            if cleanup_errors:
                raise RunnerFailure(
                    "cannot spawn evaluator; cleanup failures: "
                    f"{cleanup_errors}",
                    exit_code=30,
                ) from error
            raise
        cleanup_errors = self._close_descriptors_with_retry(
            {
                "corpus authority": authority_fd,
                "stdout": stdout_fd,
                "stderr": stderr_fd,
            }
        )
        if not cleanup_errors:
            self.active_evaluator_authority_fd = None
        if cleanup_errors:
            try:
                self._abort_owned_evaluator()
            except BaseException as cleanup_error:
                cleanup_errors.append(f"abort evaluator: {cleanup_error!r}")
            raise RunnerFailure(
                "cannot release evaluator parent descriptors: "
                f"{cleanup_errors}",
                exit_code=30,
            )
        return process

    def _invoke_evaluator(
        self, pre_guard: dict[str, Any], *, correctness_only: bool = False
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        self._verify_prepared_tree()
        self._verify_prepared_input_metadata()
        evaluator = self.prepared.tools.get("evaluator_runtime")
        script = self.prepared.support_files.get("evaluator")
        if evaluator is None or script is None:
            raise RunnerFailure("prepared evaluator is absent")
        if pre_guard.get("label") != "pre-evaluator" or pre_guard.get(
            "verdict"
        ) != "pass":
            raise RunnerFailure("final measurement guard differs")
        pre_path = (
            self.output
            / "guards"
            / f"{pre_guard['ordinal']:05d}-pre-evaluator.json"
        )
        pre_binding = {
            "path": str(pre_path.resolve()),
            "sha256": sha256(pre_path),
        }
        stdout_path = self.output / "evaluator-transition.stdout"
        stderr_path = self.output / "evaluator-transition.stderr"
        stdout_fd: int | None = None
        stderr_fd: int | None = None
        argv = (
            str(evaluator.path),
            str(script.path),
            (
                "--evaluate-correctness-only"
                if correctness_only
                else "--evaluate"
            ),
            str(self.output.resolve()),
        )
        try:
            stdout_fd = _open_exclusive(stdout_path)
            stderr_fd = _open_exclusive(stderr_path)
            authority_fd = self._seal_corpus_execution_authority(
                correctness_only=correctness_only
            )
        except BaseException:
            cleanup_errors = self._close_descriptors_with_retry(
                {"stdout": stdout_fd, "stderr": stderr_fd}
            )
            if cleanup_errors:
                raise RunnerFailure(
                    "cannot prepare evaluator descriptors; cleanup failures: "
                    f"{cleanup_errors}",
                    exit_code=30,
                )
            raise
        if stdout_fd is None or stderr_fd is None:
            raise RunnerFailure("evaluator output descriptors are absent")
        started_at = now()
        started_monotonic_ns = time.monotonic_ns()
        try:
            process = self._spawn_evaluator_process(
                argv,
                evaluator,
                stdout_fd,
                stderr_fd,
                authority_fd,
            )
            identity = _wait_exact_process(process, evaluator)
            try:
                exit_status = process.wait(timeout=600)
                timed_out = False
            except subprocess.TimeoutExpired:
                timed_out = True
                exit_status = terminate_process_group(process)
            reaping = self._reap_probe(identity)
            group_absent = not process_group_exists(identity["pgrp"])
            if not group_absent:
                terminate_process_group(process)
                raise RunnerFailure("evaluator left a process group")
        except BaseException:
            # The spawn helper closes every descriptor on Popen failure; this
            # retry also covers a signal/error immediately after a live spawn.
            self._abort_owned_evaluator()
            raise
        self.active_evaluator_process = None
        completed_at = now()
        completed_monotonic_ns = time.monotonic_ns()
        stdout_path.chmod(0o444)
        stderr_path.chmod(0o444)
        if stderr_path.stat().st_size != 0:
            raise RunnerFailure("evaluator wrote stderr")
        result = self._parse_canonical_lines(stdout_path, 1)[0]
        if result.get("outcome") not in {"ADMIT", "NARROW", "REVERT", "INCONCLUSIVE"}:
            raise RunnerFailure(f"evaluator returned invalid outcome: {result}")
        expected_status = {
            "ADMIT": 0,
            "NARROW": 10,
            "REVERT": 11,
            "INCONCLUSIVE": 20,
        }[result["outcome"]]
        if exit_status != expected_status or timed_out or reaping["status"] != "absent":
            raise RunnerFailure("evaluator outcome/exit status binding differs")
        result_path = self.output / "result.json"
        if not result_path.is_file() or result_path.read_bytes() != stdout_path.read_bytes():
            raise RunnerFailure("evaluator stdout/result publication differs")
        report_path = self.output / "REPORT.md"
        if (
            not report_path.is_file()
            or report_path.stat().st_size == 0
            or stat.S_IMODE(report_path.stat().st_mode) != 0o444
            or stat.S_IMODE(result_path.stat().st_mode) != 0o444
        ):
            raise RunnerFailure("evaluator report/result publication differs")
        if any(
            (self.output / name).exists() or (self.output / name).is_symlink()
            for name in (
                "SHA256SUMS",
                "terminal-pre-release.json",
                "lease-release.json",
                "terminal.json",
                "terminal-verification.json",
                "terminal_pre_release.json",
                "lease_release.json",
            )
        ):
            raise RunnerFailure("terminal artifact exists before evaluator transition")
        post_path = self.output / "post-evaluator-guard.json"
        post_snapshot = self.snapshot_processes(
            "post-evaluator",
            publish_manifest=False,
            snapshot_path=post_path,
        )
        child = {
            "runtime": {
                "path": str(evaluator.path),
                "sha256": evaluator.sha256,
                "mode": evaluator.mode,
                "comm": evaluator.comm,
            },
            "support": {
                "path": str(script.path),
                "sha256": script.sha256,
                "mode": script.mode,
            },
            "argv": list(argv),
            "environment": {
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "TZ": "UTC",
                "ASTERISM_REBASELINE_MODE": "evaluate",
            },
            "identity": identity,
            "started_at": started_at,
            "started_monotonic_ns": started_monotonic_ns,
            "completed_at": completed_at,
            "completed_monotonic_ns": completed_monotonic_ns,
            "exit_status": exit_status,
            "waited_pid": process.pid,
            "timed_out": timed_out,
            "terminated_by_runner": timed_out,
            "interrupted": None,
            "reaping": reaping,
            "process_group_absent": group_absent,
            "orphan_process_group_detected": False,
            "stdout": {
                "path": str(stdout_path.resolve()),
                "sha256": sha256(stdout_path),
                "bytes": stdout_path.stat().st_size,
                "mode": stat.S_IMODE(stdout_path.stat().st_mode),
            },
            "stderr": {
                "path": str(stderr_path.resolve()),
                "sha256": sha256(stderr_path),
                "bytes": stderr_path.stat().st_size,
                "mode": stat.S_IMODE(stderr_path.stat().st_mode),
            },
        }
        transition = {
            "schema": getattr(
                self.schema,
                "EVALUATOR_TRANSITION_SCHEMA",
                "bn-2l3n-evaluator-transition-v3",
            ),
            "protocol": PROTOCOL,
            "attempt_nonce": self.attempt_nonce,
            "pre_guard": pre_binding,
            "child": child,
            "post_snapshot": {
                "path": str(post_path.resolve()),
                "sha256": sha256(post_path),
            },
            "lease_held": self._lease_held_proof(),
            "completed_at": now(),
            "completed_monotonic_ns": time.monotonic_ns(),
        }
        expected_transition = set(
            getattr(self.schema, "EVALUATOR_TRANSITION_FIELDS", transition)
        )
        if set(transition) != expected_transition:
            raise RunnerFailure("evaluator transition fields differ")
        if set(child) != set(
            getattr(self.schema, "EVALUATOR_TRANSITION_CHILD_FIELDS", child)
        ):
            raise RunnerFailure("evaluator transition child fields differ")
        if post_snapshot.get("label") != "post-evaluator" or post_snapshot.get(
            "verdict"
        ) != "pass":
            raise RunnerFailure("post-evaluator snapshot differs")
        transition_path = self.output / "evaluator-transition.json"
        atomic_json(transition_path, transition)
        return transition, result

    def publish_terminal(
        self, evaluator_transition: dict[str, Any], result: dict[str, Any]
    ) -> None:
        self._verify_prepared_tree()
        self._verify_prepared_input_metadata()
        self.phase = "terminal_pre_release"
        inventory = self._artifact_inventory(require_immutable=True)
        sums_path = self._publish_sha256sums(inventory)
        evaluator_transition_path = self.output / "evaluator-transition.json"
        completed_at = now()
        completed_monotonic_ns = time.monotonic_ns()
        pre_release = {
            "schema": getattr(
                self.schema,
                "TERMINAL_PRE_RELEASE_SCHEMA",
                "bn-2l3n-terminal-pre-release-v3",
            ),
            "protocol": PROTOCOL,
            "attempt_nonce": self.attempt_nonce,
            "outcome": result["outcome"],
            "evaluator_exit": evaluator_transition["child"]["exit_status"],
            "result_path": str((self.output / "result.json").resolve()),
            "result_sha256": sha256(self.output / "result.json"),
            "provenance_path": str(self.provenance_path.resolve()),
            "provenance_sha256": sha256(self.provenance_path),
            "report_path": str((self.output / "REPORT.md").resolve()),
            "report_sha256": sha256(self.output / "REPORT.md"),
            "sha256sums_path": str(sums_path.resolve()),
            "sha256sums_sha256": sha256(sums_path),
            "artifact_inventory": inventory,
            "evaluator_transition": {
                "path": str(evaluator_transition_path.resolve()),
                "sha256": sha256(evaluator_transition_path),
            },
            "guard_manifest_path": str(self.guard_manifest.resolve()),
            "guard_manifest_sha256": sha256(self.guard_manifest),
            "guard_manifest_records": self.guard_count,
            "child_manifest_path": str(self.child_manifest.resolve()),
            "child_manifest_sha256": sha256(self.child_manifest),
            "child_manifest_records": self.child_count,
            "lease": self.lease,
            "completed_at": completed_at,
            "completed_monotonic_ns": completed_monotonic_ns,
        }
        expected_pre = set(getattr(self.schema, "TERMINAL_PRE_RELEASE_FIELDS", pre_release))
        if set(pre_release) != expected_pre:
            raise RunnerFailure("terminal pre-release fields differ", exit_code=30)
        pre_path = self.output / "terminal-pre-release.json"
        atomic_json(pre_path, pre_release)
        self.release_lease(result["outcome"])
        release_path = self.output / "lease-release.json"
        runtime = self.prepared.tools["runner_runtime"]
        support = self.prepared.support_files["runner"]
        terminal = {
            "schema": getattr(self.schema, "TERMINAL_SCHEMA", "bn-2l3n-terminal-v3"),
            "protocol": PROTOCOL,
            "attempt_nonce": self.attempt_nonce,
            "outcome": result["outcome"],
            "terminal_pre_release_path": str(pre_path.resolve()),
            "terminal_pre_release_sha256": sha256(pre_path),
            "lease_release_path": str(release_path.resolve()),
            "lease_release_sha256": sha256(release_path),
            "result_path": str((self.output / "result.json").resolve()),
            "result_sha256": sha256(self.output / "result.json"),
            "provenance_path": str(self.provenance_path.resolve()),
            "provenance_sha256": sha256(self.provenance_path),
            "sha256sums_path": str((self.output / "SHA256SUMS").resolve()),
            "sha256sums_sha256": sha256(self.output / "SHA256SUMS"),
            "artifact_inventory_sha256": sha256_bytes(canonical_json_bytes(inventory)),
            "runner": {
                "identity": self.runner_identity,
                "runtime": {
                    "path": str(runtime.path),
                    "sha256": runtime.sha256,
                    "mode": runtime.mode,
                    "comm": runtime.comm,
                },
                "support": {
                    "path": str(support.path),
                    "sha256": support.sha256,
                    "mode": support.mode,
                },
                "cmdline": self.runner_cmdline,
            },
            "terminal_published_at": now(),
            "terminal_published_monotonic_ns": time.monotonic_ns(),
        }
        if self.runner_cmdline != read_proc_cmdline(os.getpid()):
            raise RunnerFailure("runner cmdline changed before terminal publication")
        require_exact_keys(
            terminal["runner"],
            set(self.schema.TERMINAL_RUNNER_FIELDS),
            "terminal runner",
        )
        require_exact_keys(
            terminal["runner"]["runtime"],
            set(self.schema.TERMINAL_RUNTIME_FIELDS),
            "terminal runner runtime",
        )
        require_exact_keys(
            terminal["runner"]["support"],
            set(self.schema.TERMINAL_SUPPORT_FIELDS),
            "terminal runner support",
        )
        expected_terminal = set(getattr(self.schema, "TERMINAL_FIELDS", terminal))
        if set(terminal) != expected_terminal:
            raise RunnerFailure("terminal fields differ", exit_code=30)
        atomic_json(self.output / "terminal.json", terminal)
        verifier = self.prepared.tools.get("terminal_verifier_runtime")
        verifier_script = self.prepared.support_files.get("terminal_verifier")
        if verifier is None or verifier_script is None:
            raise RunnerFailure("prepared terminal verifier is absent", exit_code=30)
        argv = (
            str(verifier.path),
            str(verifier_script.path),
            "--verify",
            str(self.output.resolve()),
        )
        process = subprocess.Popen(
            argv,
            executable=str(verifier.path),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "TZ": "UTC"},
            start_new_session=True,
        )
        try:
            _wait_exact_process(process, verifier)
        except BaseException as error:
            terminate_process_group(process)
            raise RunnerFailure(
                f"cannot bind terminal verifier identity: {error}", exit_code=30
            ) from error
        try:
            stdout, stderr = process.communicate(timeout=300)
        except subprocess.TimeoutExpired as error:
            terminate_process_group(process)
            raise RunnerFailure("terminal verifier timed out", exit_code=30) from error
        reject_orphan_process_group(
            process, "terminal verifier", exit_code=30
        )
        if (
            process.returncode != 0
            or stderr
        ):
            raise RunnerFailure(
                f"terminal verifier failed rc={process.returncode} stderr={stderr!r}",
                exit_code=30,
            )
        try:
            verification = json.loads(stdout)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RunnerFailure("terminal verifier stdout is not JSON", exit_code=30) from error
        verification_path = self.output / "terminal-verification.json"
        if (
            not isinstance(verification, dict)
            or stdout != canonical_json_bytes(verification)
            or not verification_path.is_file()
            or verification_path.read_bytes() != stdout
            or verification.get("outcome") != "TERMINAL_VERIFIED"
        ):
            raise RunnerFailure(
                f"terminal verifier did not verify attempt: {verification}",
                exit_code=30,
            )

    def write_failure(self, failure: RunnerFailure) -> None:
        value = {
            "schema": FAILURE_SCHEMA,
            "protocol": PROTOCOL,
            "outcome": "INCONCLUSIVE",
            "phase": self.phase,
            "next_transition": self.next_transition,
            "reason": failure.reason,
            "exit_code": failure.exit_code,
            "runner": self.runner_identity,
            "active_child": self.active_child,
            "lease": self.lease,
            "claim_path": (
                str(self.run_claim_path)
                if self.claim and self.run_claim_path is not None
                else None
            ),
            "claim_sha256": (
                sha256(self.run_claim_path)
                if self.claim
                and self.run_claim_path is not None
                and self.run_claim_path.is_file()
                else None
            ),
            "failed_at": now(),
            "failed_monotonic_ns": time.monotonic_ns(),
            "artifact_inventory": self._artifact_inventory(),
        }
        atomic_json(self.output / "failure.json", value)

    def close_semantic_manifests(self) -> None:
        errors: list[str] = []
        for semantic_manifest in getattr(self, "semantic_manifests", ()):
            try:
                semantic_manifest.close()
            except BaseException as error:
                errors.append(
                    f"{semantic_manifest.context}: "
                    f"{error.__class__.__name__}: {error}"
                )
        if errors:
            raise RunnerFailure(
                "semantic manifest descriptor cleanup failed: " + "; ".join(errors)
            )

    def finish_semantic_lifetime(
        self,
        primary_failure: RunnerFailure | None = None,
    ) -> RunnerFailure | None:
        """Reverify then close every retained FD, preserving any primary failure."""

        if not any(
            semantic_manifest.descriptor >= 0
            for semantic_manifest in getattr(self, "semantic_manifests", ())
        ):
            return primary_failure
        boundary_failure: RunnerFailure | None = None
        manifests = list(getattr(self, "semantic_manifests", ()))
        if (
            getattr(self, "semantic_manifest_counts", None)
            != SEMANTIC_MANIFEST_COUNTS
            or len(manifests) != SEMANTIC_MANIFEST_TOTAL
        ):
            boundary_failure = RunnerFailure(
                "semantic manifest lifetime set is incomplete"
            )
        for semantic_manifest in manifests:
            try:
                semantic_manifest.verify()
            except RunnerFailure as error:
                if boundary_failure is None:
                    boundary_failure = error
            except BaseException as error:
                if boundary_failure is None:
                    boundary_failure = RunnerFailure(
                        "unhandled semantic lifetime recheck failure: "
                        f"{error.__class__.__name__}: {error}",
                        exit_code=30,
                    )
            finally:
                try:
                    semantic_manifest.close()
                except BaseException as error:
                    if boundary_failure is None:
                        boundary_failure = RunnerFailure(
                            "semantic manifest descriptor cleanup failed: "
                            f"{semantic_manifest.context}: "
                            f"{error.__class__.__name__}: {error}"
                        )
        if boundary_failure is None:
            return primary_failure
        if primary_failure is None:
            return boundary_failure
        return RunnerFailure(
            f"{primary_failure.reason}; semantic lifetime boundary failed: "
            f"{boundary_failure.reason}",
            exit_code=primary_failure.exit_code,
        )

    def run(self) -> int:
        try:
            return self._run_with_retained_semantic_authority()
        finally:
            try:
                self.close_semantic_manifests()
            except BaseException as cleanup_error:
                self._report_secondary_failure(
                    "final semantic cleanup",
                    cleanup_error,
                )

    def _publish_runner_failure(self, failure: RunnerFailure) -> int:
        """Attempt all failure side effects without replacing the primary failure."""

        actions: tuple[tuple[str, Callable[[], None]], ...] = (
            ("failure artifact publication", lambda: self.write_failure(failure)),
            ("inconclusive lease release", lambda: self.release_lease("INCONCLUSIVE")),
            ("failure log publication", lambda: self.log(f"fail-stop: {failure.reason}")),
        )
        log_published = False
        for context, action in actions:
            try:
                action()
                if context == "failure log publication":
                    log_published = True
            except BaseException as error:
                self._report_secondary_failure(context, error)
        if not log_published:
            try:
                self._emit_primary_failure(failure)
            except BaseException:
                pass
        return failure.exit_code

    def _run_with_retained_semantic_authority(self) -> int:
        def interrupted(signum: int, _frame: Any) -> None:
            raise RunnerFailure(f"runner received signal {signum}")

        try:
            signal.signal(signal.SIGINT, interrupted)
            signal.signal(signal.SIGTERM, interrupted)
            self._admit_run_mode()
            self.scratch_root.mkdir(parents=True, exist_ok=True)
            self.attempt_scratch.mkdir(parents=True, exist_ok=False)
            self.runtime_home.mkdir(mode=0o700)
            if self.rehearsal:
                (self.output / "REHEARSAL").write_text(
                    "rows in this directory are rehearsal output and are "
                    "non-evidence by construction (protocol v4 §3)\n"
                )
            self.acquire_lease()
            self.host_resource_preflight()
            self.publish_frozen_inputs()
            self.claim_prepared()
            self.assert_host_admission_stable()
            self.prepare_profile_preflight()
            self.write_initial_provenance()
            self._run_contract_smoke()
            self._run_correctness("pre")
            current_pre_failures, historical_failed_cases = (
                self._pre_correctness_failures()
            )
            if current_pre_failures or historical_failed_cases:
                self._run_correctness("post")
                self._publish_correctness_only_marker(
                    current_pre_failures,
                    self._post_current_failure_ids(),
                    historical_failed_cases,
                )
                pre_evaluator = self.snapshot_processes(
                    "pre-evaluator", enforce_resources=True
                )
                self.finalize_provenance(correctness_only=True)
                evaluator_transition, result = self._invoke_evaluator(
                    pre_evaluator, correctness_only=True
                )
                boundary_failure = self.finish_semantic_lifetime()
                if boundary_failure is not None:
                    raise boundary_failure
                self.publish_terminal(evaluator_transition, result)
                return 0
            corpora = self.seed_reopen_corpora()
            for track in (
                "primary",
                "new_names",
                "fairness",
                "cpu_profiles",
                "syscall_profiles",
                "reopen",
                "structural_traces",
            ):
                self._run_matrix(track, corpora)
            self._run_correctness("post")
            pre_evaluator = self.snapshot_processes(
                "pre-evaluator", enforce_resources=True
            )
            self.finalize_provenance()
            evaluator_transition, result = self._invoke_evaluator(pre_evaluator)
            boundary_failure = self.finish_semantic_lifetime()
            if boundary_failure is not None:
                raise boundary_failure
            self.publish_terminal(evaluator_transition, result)
            return 0
        except RunnerFailure as failure:
            failure = self.finish_semantic_lifetime(failure) or failure
            return self._publish_runner_failure(failure)
        except BaseException as error:
            failure = RunnerFailure(
                f"unhandled runner failure: {error.__class__.__name__}: {error}",
                exit_code=30,
            )
            failure = self.finish_semantic_lifetime(failure) or failure
            return self._publish_runner_failure(failure)


    def _settle(self, durability: str | None, label: str) -> list[dict[str, Any]]:
        milliseconds = GROUP_SETTLE_MS if durability == "Group" else PROCESS_SETTLE_MS
        deadline = self.monotonic() + milliseconds / 1000
        samples: list[dict[str, Any]] = []
        while True:
            sample = os.statvfs(self.scratch_root)
            samples.append(
                {
                    "at": now(),
                    "monotonic_ns": time.monotonic_ns(),
                    "load1": self.load_reader(),
                    "free_bytes": sample.f_bavail * sample.f_frsize,
                    "free_inodes": sample.f_favail,
                }
            )
            remaining = deadline - self.monotonic()
            if remaining <= 0:
                break
            self.sleep(min(1.0, remaining))
        append_jsonl(
            self.resource_manifest,
            {
                "schema": "asterism-rebaseline-settle-v3",
                "protocol": PROTOCOL,
                "label": label,
                "durability": durability,
                "settle_ms": milliseconds,
                "samples": samples,
            },
        )
        return samples

    def _append_csv(self, track: str, row: dict[str, Any]) -> dict[str, Any]:
        path = self.csv_paths[track]
        fields = tuple(self.schema.CSV_FIELDS_BY_TRACK[track])
        before = csv_shape(path, fields)
        before_bytes = before["bytes"]
        before_prefix = sha256(path) if path.exists() else sha256_bytes(b"")
        payload = self.schema.encode_csv_row(track, row, not path.exists())
        if isinstance(payload, str):
            payload = payload.encode("ascii")
        if not isinstance(payload, bytes) or not payload.endswith(b"\n"):
            raise RunnerFailure(f"schema emitted non-canonical CSV bytes for {track}")
        append_bytes(path, payload)
        after = csv_shape(path, fields)
        if (
            not after["complete"]
            or after["rows"] != before["rows"] + 1
            or before_prefix != (sha256_bytes(path.read_bytes()[:before_bytes]))
        ):
            raise RunnerFailure(f"CSV append boundary failed for {track}")
        return {
            "path": str(path.resolve()),
            "bytes_before": before_bytes,
            "bytes_after": after["bytes"],
            "rows_before": before["rows"],
            "rows_after": after["rows"],
            "prefix_sha256_before": before_prefix,
            "prefix_sha256_after": sha256_bytes(path.read_bytes()[:before_bytes]),
            "sha256_after": after["sha256"],
        }

    def _runner_context(
        self, plan: ChildPlan, control_events: list[dict[str, Any]]
    ) -> dict[str, Any]:
        if plan.track is None:
            raise RunnerFailure("runner context requested for non-row child")
        by_phase = {
            str(event.get("phase")): event
            for event in control_events
            if isinstance(event, dict)
            and "command" not in event
            and isinstance(event.get("phase"), str)
        }
        track = plan.track
        context = plan.context
        schema_name = getattr(self.schema, "ROW_SCHEMAS")[track]
        if track in {"primary", "new_names", "fairness"}:
            ready = by_phase.get("ready", {})
            measured = by_phase.get("measured", {})
            value = {
                "schema": schema_name,
                "attempt_nonce": self.attempt_nonce,
                "row_ordinal": context["row_ordinal"],
                "block": context["block"],
                "cell_ordinal": context["cell_ordinal"],
                "store_id": plan.store_path.name if plan.store_path else "not_applicable",
                "store_absent_before": True,
                "ready_monotonic_ns": ready.get("ready_monotonic_ns"),
                "counter_start_monotonic_ns": ready.get("counter_start_monotonic_ns"),
                "t0_monotonic_ns": measured.get("t0_monotonic_ns"),
                "release_monotonic_ns": measured.get("release_monotonic_ns"),
                "last_completion_monotonic_ns": measured.get(
                    "last_completion_monotonic_ns"
                ),
                "t1_monotonic_ns": measured.get("t1_monotonic_ns"),
                "counter_end_monotonic_ns": measured.get("counter_end_monotonic_ns"),
            }
        elif track == "reopen":
            boot = by_phase.get("boot", {})
            runtime = by_phase.get("runtime", {})
            ready = by_phase.get("ready", {})
            opened = by_phase.get("opened", {})
            measured = by_phase.get("measured", {})
            start = next(
                (
                    event
                    for event in control_events
                    if isinstance(event, dict) and event.get("command") == "start"
                ),
                {},
            )
            release = next(
                (
                    event
                    for event in reversed(control_events)
                    if isinstance(event, dict) and event.get("command") == "release"
                ),
                {},
            )
            value = {
                "schema": schema_name,
                "attempt_nonce": self.attempt_nonce,
                "row_ordinal": context["row_ordinal"],
                "latin_block": context["latin_block"],
                "ordinal_in_block": context["ordinal_in_block"],
                "archive_manifest_sha256": context["archive_manifest_sha256"],
                "copy_manifest_sha256": context["copy_manifest_sha256"],
                "copy_id": context["copy_id"],
                "copy_absent_before": True,
                "copy_verified_read_only": context["copy_verified_read_only"],
                "syncfs_complete": True,
                "cache_state": "warm-from-materialization",
                "boot_monotonic_ns": boot.get("_runner_received_monotonic_ns"),
                "runtime_monotonic_ns": runtime.get("_runner_received_monotonic_ns"),
                "ready_monotonic_ns": ready.get("ready_monotonic_ns"),
                "start_sent_monotonic_ns": start.get("_runner_sent_monotonic_ns"),
                "open_start_monotonic_ns": opened.get("open_start_monotonic_ns"),
                "opened_monotonic_ns": opened.get("opened_monotonic_ns"),
                "measured_monotonic_ns": measured.get(
                    "_runner_received_monotonic_ns"
                ),
                "release_monotonic_ns": release.get("_runner_sent_monotonic_ns"),
            }
        elif track in {"cpu_profiles", "syscall_profiles"}:
            value = {
                "schema": schema_name,
                "attempt_nonce": self.attempt_nonce,
                "row_ordinal": context["row_ordinal"],
                "block": context["block"],
                "cell_ordinal": context["cell_ordinal"],
                "profile_timing_discarded": True,
            }
        elif track == "structural_traces":
            value = {
                "schema": schema_name,
                "attempt_nonce": self.attempt_nonce,
                "row_ordinal": context["row_ordinal"],
                "profile_timing_discarded": True,
            }
        else:
            raise RunnerFailure(f"unknown runner context track {track}")
        expected = set(getattr(self.schema, "RUNNER_CONTEXT_FIELDS_BY_TRACK")[track])
        if set(value) != expected:
            raise RunnerFailure(
                f"runner context fields differ for {track}: "
                f"missing={sorted(expected - set(value))} extra={sorted(set(value) - expected)}"
            )
        return value

    def run_child(self, plan: ChildPlan, *, controlled: bool) -> dict[str, Any]:
        """Execute once, reap the process group, then publish at most one row."""

        self.next_transition = plan.context
        ordinal = self.child_count + 1
        profile_tool_track = plan.track or plan.context.get("smoke_target")
        profile_track = plan.track or plan.context.get("profile_smoke_track")
        timeout_profile_track = profile_track
        if timeout_profile_track is None and profile_tool_track in {
            "cpu_profiles",
            "syscall_profiles",
            "structural_traces",
        }:
            timeout_profile_track = profile_tool_track
        if (
            plan.context.get("variant") == "C"
            and timeout_profile_track is not None
            and plan.timeout_seconds > C_PROFILE_CHILD_TIMEOUT_SECONDS
        ):
            raise RunnerFailure("C profile child timeout exceeds 120-second contract")
        needs_profile_tool = profile_tool_track in {
            "cpu_profiles",
            "syscall_profiles",
            "structural_traces",
        }
        if needs_profile_tool and self.profile_tool_driver is None:
            raise RunnerFailure(
                f"runner-owned profile tool driver is absent for {profile_tool_track}"
            )
        raw_path = self.output / "raw" / plan.kind / f"{ordinal:05d}.json"
        stderr_path = self.output / "raw" / plan.kind / f"{ordinal:05d}.stderr"
        context_path = self.output / "contexts" / f"{ordinal:05d}.json"
        active_path = self.output / "active-child.json"
        absent = [raw_path, stderr_path, context_path]
        if plan.store_path is not None and plan.store_absent_before:
            absent.append(plan.store_path)
        elif plan.store_path is not None and not plan.store_path.is_dir():
            raise RunnerFailure(
                f"pre-materialized child store is absent: {plan.store_path}"
            )
        for path in absent:
            if path.exists() or path.is_symlink():
                raise RunnerFailure(f"child output/store identity is not fresh: {path}")
        atomic_json(context_path, plan.context)
        context_sha = sha256(context_path)
        environment = self._base_environment()
        environment.update(plan.environment)
        environment.update(
            {
                "ASTERISM_REBASELINE_PROTOCOL": PROTOCOL,
                "ASTERISM_REBASELINE_PROTOCOL_SHA256": PROTOCOL_SHA256,
                "ASTERISM_REBASELINE_CONTEXT": str(context_path.resolve()),
                "ASTERISM_REBASELINE_CONTEXT_SHA256": context_sha,
                "ASTERISM_CONTEXT_SHA256": context_sha,
            }
        )
        if plan.store_path is not None:
            environment["ASTERISM_REBASELINE_STORE"] = str(plan.store_path.resolve())
        control_parent: socket.socket | None = None
        control_child: socket.socket | None = None
        child_control_fd: int | None = None
        self.wait_for_quiet()
        self.verify_frozen()
        self.resource_guard(
            f"{ordinal:05d}-{plan.kind}-pre",
            [path for path in absent if path != context_path],
        )
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        stderr_path.parent.mkdir(parents=True, exist_ok=True)
        stdout_fd = os.open(raw_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            stderr_fd = os.open(
                stderr_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
            )
        except BaseException:
            os.close(stdout_fd)
            raise
        owned_parent_fds: dict[str, int | None] = {
            "stdout": stdout_fd,
            "stderr": stderr_fd,
        }
        owned_sockets: dict[str, socket.socket | None] = {
            "parent": None,
            "child": None,
        }

        def close_owned_fd(name: str) -> None:
            descriptor = owned_parent_fds[name]
            if descriptor is None:
                return
            os.close(descriptor)
            owned_parent_fds[name] = None

        def close_owned_socket(name: str) -> None:
            owned = owned_sockets[name]
            if owned is None:
                return
            owned.close()
            owned_sockets[name] = None

        def clear_active_marker() -> None:
            if active_path.is_file() or active_path.is_symlink():
                active_path.unlink()
                fsync_dir(active_path.parent)

        def cleanup_actions(
            actions: Iterable[tuple[str, Callable[[], Any]]],
        ) -> list[str]:
            pending = list(actions)
            latest: dict[str, str] = {}
            for _attempt in range(2):
                retry: list[tuple[str, Callable[[], Any]]] = []
                for label, action in pending:
                    try:
                        action()
                        latest.pop(label, None)
                    except BaseException as cleanup_error:
                        latest[label] = f"{label}: {cleanup_error!r}"
                        retry.append((label, action))
                if not retry:
                    return []
                pending = retry
            return [latest[label] for label, _action in pending]

        profile_tool_session: RunnerOwnedProfileSession | None = None
        profile_child_fds: tuple[int, ...] = ()
        try:
            if controlled:
                control_parent, control_child = socket.socketpair(
                    socket.AF_UNIX, socket.SOCK_STREAM
                )
                owned_sockets["parent"] = control_parent
                owned_sockets["child"] = control_child
                control_parent.settimeout(plan.timeout_seconds)
                child_control_fd = control_child.fileno()
                environment["ASTERISM_REBASELINE_CONTROL_FD"] = str(
                    child_control_fd
                )
            if needs_profile_tool:
                provider = self._load_profile_adapter()
                if provider is None:
                    raise RunnerFailure("profile adapter is absent for external tool session")
                profile_tool_session = self.profile_tool_driver.prepare_child(
                    plan=plan,
                    output_dir=self.output,
                    ordinal=ordinal,
                    tools=self.prepared.tools,
                    support_files=self.prepared.support_files,
                    profile_adapter=provider,
                )
                environment.update(profile_tool_session.environment_overrides())
                profile_child_fds = profile_tool_session.child_pass_fds()
                self.active_helpers = profile_tool_session.helper_identities()
            if controlled:
                if child_control_fd is None:
                    raise RunnerFailure("controlled child descriptor is absent")
                self._assert_controlled_child_environment(
                    plan,
                    environment,
                    physical_ordinal=ordinal,
                    context_sha256=context_sha,
                    control_fd=child_control_fd,
                )
            pre_guard = self.snapshot_processes(
                f"{ordinal:05d}-{plan.kind}-pre", enforce_resources=True
            )
        except BaseException as error:
            cleanup_errors = cleanup_actions(
                (
                    (
                        "abort profile tool session",
                        lambda: (
                            profile_tool_session.abort()
                            if profile_tool_session is not None
                            else None
                        ),
                    ),
                    ("close stdout", lambda: close_owned_fd("stdout")),
                    ("close stderr", lambda: close_owned_fd("stderr")),
                    (
                        "close parent control socket",
                        lambda: close_owned_socket("parent"),
                    ),
                    (
                        "close child control socket",
                        lambda: close_owned_socket("child"),
                    ),
                )
            )
            self.active_helpers = []
            if cleanup_errors:
                raise RunnerFailure(
                    f"cannot prepare {plan.kind}; cleanup failures: {cleanup_errors}",
                    exit_code=30,
                ) from error
            raise
        started_at = now()
        started_monotonic_ns = time.monotonic_ns()
        interrupted: BaseException | None = None
        terminated = False
        timed_out = False
        blocked = {signal.SIGINT, signal.SIGTERM}
        previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, blocked)
        try:
            child = subprocess.Popen(
                list(plan.argv),
                stdout=stdout_fd,
                stderr=stderr_fd,
                env=environment,
                start_new_session=True,
                pass_fds=(
                    ((child_control_fd,) if child_control_fd is not None else ())
                    + profile_child_fds
                ),
            )
        except BaseException as error:
            cleanup_errors = cleanup_actions(
                (
                    (
                        "abort profile tool session",
                        lambda: (
                            profile_tool_session.abort()
                            if profile_tool_session is not None
                            else None
                        ),
                    ),
                    ("close stdout", lambda: close_owned_fd("stdout")),
                    ("close stderr", lambda: close_owned_fd("stderr")),
                    (
                        "close parent control socket",
                        lambda: close_owned_socket("parent"),
                    ),
                    (
                        "close child control socket",
                        lambda: close_owned_socket("child"),
                    ),
                    (
                        "restore signal mask",
                        lambda: signal.pthread_sigmask(
                            signal.SIG_SETMASK, previous_mask
                        ),
                    ),
                )
            )
            self.active_helpers = []
            detail = f"cannot spawn {plan.kind}: {error!r}"
            if cleanup_errors:
                detail += f"; cleanup failures: {cleanup_errors}"
            raise RunnerFailure(detail, exit_code=30) from error

        def terminate_owned_child() -> int:
            failures: list[str] = []
            for _attempt in range(2):
                try:
                    return terminate_process_group(child)
                except BaseException as cleanup_error:
                    failures.append(repr(cleanup_error))
            raise RunnerFailure(
                f"cannot terminate child process group after retry: {failures}",
                exit_code=30,
            )

        def fail_post_spawn(stage: str, error: BaseException) -> NoReturn:
            cleanup_errors = cleanup_actions(
                (
                    (
                        "terminate child process group",
                        terminate_owned_child,
                    ),
                    (
                        "abort profile tool session",
                        lambda: (
                            profile_tool_session.abort()
                            if profile_tool_session is not None
                            else None
                        ),
                    ),
                    ("close stdout", lambda: close_owned_fd("stdout")),
                    ("close stderr", lambda: close_owned_fd("stderr")),
                    (
                        "close parent control socket",
                        lambda: close_owned_socket("parent"),
                    ),
                    (
                        "close child control socket",
                        lambda: close_owned_socket("child"),
                    ),
                    ("clear active child marker", clear_active_marker),
                    (
                        "restore signal mask",
                        lambda: signal.pthread_sigmask(
                            signal.SIG_SETMASK, previous_mask
                        ),
                    ),
                )
            )
            self.active_child = None
            self.active_executable = None
            self.active_helpers = []
            exit_code = (
                error.exit_code
                if isinstance(error, RunnerFailure) and not cleanup_errors
                else 30
            )
            detail = f"cannot {stage}: {error!r}"
            if cleanup_errors:
                detail += f"; cleanup failures: {cleanup_errors}"
            raise RunnerFailure(detail, exit_code=exit_code) from error

        try:
            close_owned_fd("stdout")
            close_owned_fd("stderr")
            close_owned_socket("child")
        except BaseException as error:
            fail_post_spawn("release parent child descriptors", error)

        try:
            identity = _wait_exact_process(child, plan.executable)
        except BaseException as error:
            fail_post_spawn("bind child identity", error)
        self.active_child = identity
        self.active_executable = plan.executable
        try:
            if profile_tool_session is not None:
                if child_control_fd is None:
                    raise RunnerFailure("profile child control descriptor is absent")
                profile_tool_session.bind_child(identity, control_fd=child_control_fd)
            profile_authority: dict[str, Any] | None = None
            if controlled and profile_track is not None:
                if child_control_fd is None:
                    raise RunnerFailure("controlled profile descriptor is absent")
                profile_authority = self._profile_authority(
                    plan,
                    identity,
                    child_ordinal=ordinal,
                    context_sha256=context_sha,
                    control_fd=child_control_fd,
                )
                profile = self._profile_for(plan, child.pid, profile_authority)
            else:
                profile = None
        except BaseException as error:
            fail_post_spawn("establish profile authority", error)
        try:
            atomic_json(
                active_path,
                {
                    "schema": "asterism-rebaseline-active-child-v3",
                    "protocol": PROTOCOL,
                    "runner": self.runner_identity,
                    "child": identity,
                    "kind": plan.kind,
                    "argv": list(plan.argv),
                    "environment_overrides": environment,
                    "context_path": str(context_path.resolve()),
                    "context_sha256": context_sha,
                    "published_at": now(),
                    "published_monotonic_ns": time.monotonic_ns(),
                },
            )
        except BaseException as error:
            fail_post_spawn("publish active child marker", error)
        control_events: list[dict[str, Any]] = []
        profile_events: list[dict[str, Any]] = []
        parked_state_proofs: list[dict[str, Any]] = []
        try:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
            if control_parent is not None:
                reopen_order = plan.track == "reopen" or (
                    plan.track == "structural_traces"
                    and plan.context.get("trace_kind") == "reopen"
                ) or plan.context.get("reopen_order") is True
                phase_nonce: str | None = None
                before_start = (
                    ("boot", "runtime", "ready")
                    if reopen_order
                    else ("boot", "runtime", "opened", "ready")
                )
                for phase_name in before_start:
                    phase_event = self._socket_json_read(control_parent, phase_name)
                    expected_phase_keys = (
                        {"context_sha256", "phase", "protocol_sha256", "variant"}
                        if phase_name == "boot"
                        else (
                            {
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
                            }
                            if phase_name == "ready"
                            else {"nonce", "phase"}
                        )
                    )
                    if (
                        set(phase_event) != expected_phase_keys
                        or
                        phase_event.get("phase") != phase_name
                        or (
                            phase_name == "boot"
                            and (
                                phase_event.get("context_sha256") != context_sha
                                or phase_event.get("protocol_sha256") != PROTOCOL_SHA256
                                or phase_event.get("variant")
                                != plan.context.get("variant")
                            )
                        )
                        or (
                            phase_name != "boot"
                            and phase_event.get("nonce") != phase_nonce
                        )
                        or (
                            phase_name == "ready"
                            and (
                                phase_event.get("context_sha256") != context_sha
                                or phase_event.get("protocol_sha256") != PROTOCOL_SHA256
                                or phase_event.get("variant")
                                != plan.context.get("variant")
                            )
                        )
                    ):
                        raise RunnerFailure(
                            f"invalid {phase_name} handshake: {phase_event}"
                        )
                    phase_event["_runner_received_monotonic_ns"] = time.monotonic_ns()
                    control_events.append(phase_event)
                    parked_for_capture = (
                        phase_name == "boot" and needs_profile_tool
                    ) or (phase_name == "ready" and reopen_order)
                    if parked_for_capture:
                        parked_state_proofs.append(
                            {"phase": phase_name, **set_child_parked(identity, True)}
                        )
                    if profile is not None:
                        profile_events.append(
                            {
                                "phase": phase_name,
                                "snapshot": profile.capture_phase(phase_name).to_json(),
                            }
                        )
                    if profile_tool_session is not None:
                        profile_tool_session.capture_phase(phase_name, phase_event)
                        self.active_helpers = profile_tool_session.helper_identities()
                    self.snapshot_processes(
                        f"{ordinal:05d}-{plan.kind}-{phase_name}",
                        publish_manifest=False,
                        snapshot_path=(
                            self.output
                            / "phase-guards"
                            / f"{ordinal:05d}-{plan.kind}-{phase_name}.json"
                        ),
                    )
                    if parked_for_capture and not (
                        reopen_order and phase_name == "ready"
                    ):
                        parked_state_proofs.append(
                            {"phase": phase_name, **set_child_parked(identity, False)}
                        )
                    if phase_name != "ready":
                        phase_nonce = secrets.token_hex(32)
                        acknowledgement = {
                            "command": "continue",
                            "phase": phase_name,
                            "nonce": phase_nonce,
                        }
                        self._socket_json_write(control_parent, acknowledgement)
                        acknowledgement["_runner_sent_monotonic_ns"] = time.monotonic_ns()
                        control_events.append(acknowledgement)
                start_nonce = secrets.token_hex(32)
                if profile is not None:
                    profile_events.append(
                        {"phase": "begin", "snapshot": profile.begin().to_json()}
                    )
                if profile_tool_session is not None:
                    profile_tool_session.begin(start_nonce=start_nonce)
                if reopen_order:
                    # Both the ready phase and the profile window start are
                    # sampled while SIGSTOP-parked.  Continue immediately
                    # before publishing the one timed-open release.
                    parked_state_proofs.append(
                        {"phase": "ready", **set_child_parked(identity, False)}
                    )
                start = {"command": "start", "nonce": start_nonce}
                self._socket_json_write(control_parent, start)
                start["_runner_sent_monotonic_ns"] = time.monotonic_ns()
                control_events.append(start)
                measured_nonce = start_nonce
                if reopen_order:
                    opened = self._socket_json_read(control_parent, "opened")
                    if (
                        set(opened)
                        != {
                            "nonce",
                            "opened_monotonic_ns",
                            "open_start_monotonic_ns",
                            "phase",
                        }
                        or opened.get("phase") != "opened"
                        or opened.get("nonce") != start_nonce
                    ):
                        raise RunnerFailure(f"invalid opened handshake: {opened}")
                    opened["_runner_received_monotonic_ns"] = time.monotonic_ns()
                    control_events.append(opened)
                    parked_state_proofs.append(
                        {"phase": "opened", **set_child_parked(identity, True)}
                    )
                    if profile is not None:
                        profile_events.append(
                            {
                                "phase": "opened",
                                "snapshot": profile.capture_phase("opened").to_json(),
                            }
                        )
                        profile_events.append(
                            {"phase": "end", "snapshot": profile.end().to_json()}
                        )
                    if profile_tool_session is not None:
                        profile_tool_session.capture_phase("opened", opened)
                        profile_tool_session.end(end_event=opened)
                    self.snapshot_processes(
                        f"{ordinal:05d}-{plan.kind}-opened",
                        publish_manifest=False,
                        snapshot_path=(
                            self.output
                            / "phase-guards"
                            / f"{ordinal:05d}-{plan.kind}-opened.json"
                        ),
                    )
                    parked_state_proofs.append(
                        {"phase": "opened", **set_child_parked(identity, False)}
                    )
                    measured_nonce = secrets.token_hex(32)
                    opened_ack = {
                        "command": "continue",
                        "phase": "opened",
                        "nonce": measured_nonce,
                    }
                    self._socket_json_write(control_parent, opened_ack)
                    opened_ack["_runner_sent_monotonic_ns"] = time.monotonic_ns()
                    control_events.append(opened_ack)
                measured = self._socket_json_read(control_parent, "measured")
                expected_measured_keys = {
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
                }
                if profile_tool_track == "cpu_profiles":
                    expected_measured_keys.add("perf_disable")
                if (
                    set(measured) != expected_measured_keys
                    or measured.get("phase") != "measured"
                    or measured.get("nonce") != measured_nonce
                ):
                    raise RunnerFailure(f"invalid measured handshake: {measured}")
                measured["_runner_received_monotonic_ns"] = time.monotonic_ns()
                control_events.append(measured)
                if profile is not None:
                    if reopen_order:
                        profile_events.append(
                            {
                                "phase": "measured",
                                "snapshot": profile.capture_phase("measured").to_json(),
                            }
                        )
                    else:
                        profile_events.append(
                            {"phase": "end", "snapshot": profile.end().to_json()}
                        )
                if profile_tool_session is not None:
                    profile_tool_session.capture_phase("measured", measured)
                    if not reopen_order:
                        profile_tool_session.end(end_event=measured)
                self.snapshot_processes(
                    f"{ordinal:05d}-{plan.kind}-measured",
                    publish_manifest=False,
                    snapshot_path=(
                        self.output
                        / "phase-guards"
                        / f"{ordinal:05d}-{plan.kind}-measured.json"
                    ),
                )
                release = {"command": "release", "nonce": measured_nonce}
                self._socket_json_write(control_parent, release)
                release["_runner_sent_monotonic_ns"] = time.monotonic_ns()
                control_events.append(release)
            rc = child.wait(timeout=plan.timeout_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            terminated = True
            try:
                terminate_owned_child()
                rc = 124
            except BaseException as cleanup_error:
                interrupted = RunnerFailure(
                    f"{plan.kind} timed out; cleanup failed with {cleanup_error!r}",
                    exit_code=30,
                )
                rc = 125
        except BaseException as error:
            interrupted = error
            terminated = True
            try:
                rc = terminate_owned_child()
            except BaseException as cleanup_error:
                interrupted = RunnerFailure(
                    f"{plan.kind} failed with {error!r}; cleanup failed with {cleanup_error!r}",
                    exit_code=30,
                )
                rc = 125
        finally:
            final_cleanup_errors = cleanup_actions(
                (
                    (
                        "close parent control socket",
                        lambda: close_owned_socket("parent"),
                    ),
                    (
                        "restore signal mask",
                        lambda: signal.pthread_sigmask(
                            signal.SIG_SETMASK, previous_mask
                        ),
                    ),
                )
            )
            if final_cleanup_errors:
                cleanup_failure = RunnerFailure(
                    f"{plan.kind} final cleanup failed: {final_cleanup_errors}",
                    exit_code=30,
                )
                if interrupted is not None:
                    cleanup_failure = RunnerFailure(
                        f"{plan.kind} failed with {interrupted!r}; "
                        f"final cleanup failed: {final_cleanup_errors}",
                        exit_code=30,
                    )
                interrupted = cleanup_failure
        try:
            reaping = self._reap_probe(identity)
        except BaseException as error:
            reaping = {
                "status": "unproven",
                "pid": identity["pid"],
                "start_ticks": identity["starttime_ticks"],
            }
            interrupted = interrupted or error
        orphan_group_detected = process_group_exists(identity["pgrp"])
        if orphan_group_detected:
            terminated = True
            try:
                terminate_owned_child()
            except BaseException as cleanup_error:
                interrupted = RunnerFailure(
                    f"{plan.kind} left an orphan process group; cleanup failed "
                    f"with {cleanup_error!r}",
                    exit_code=30,
                )
        group_absent = not process_group_exists(identity["pgrp"])
        try:
            profile_tool_evidence = (
                profile_tool_session.finish()
                if profile_tool_session is not None
                else {"inputs": {}, "helper_records": []}
            )
            if (
                not isinstance(profile_tool_evidence, dict)
                or set(profile_tool_evidence)
                != {"inputs", "helper_records"}
                or not isinstance(profile_tool_evidence["inputs"], dict)
                or not isinstance(profile_tool_evidence["helper_records"], list)
            ):
                raise RunnerFailure("runner-owned profile tool evidence is malformed")
        except BaseException as error:
            if profile_tool_session is not None:
                profile_tool_session.abort()
            profile_tool_evidence = {
                "inputs": {},
                "helper_records": [],
            }
            interrupted = interrupted or error
        try:
            active_path.unlink()
            fsync_dir(active_path.parent)
        except BaseException as error:
            interrupted = interrupted or RunnerFailure(
                f"cannot clear active child marker: {error}"
            )
        self.active_child = None
        self.active_executable = None
        self.active_helpers = []
        completed_at = now()
        completed_monotonic_ns = time.monotonic_ns()
        raw_sha = sha256(raw_path)
        stderr_sha = sha256(stderr_path)
        raw_bytes = raw_path.stat().st_size
        stderr_bytes = stderr_path.stat().st_size
        raw_path.chmod(0o444)
        stderr_path.chmod(0o444)
        raw_mode_after = stat.S_IMODE(raw_path.stat().st_mode)
        stderr_mode_after = stat.S_IMODE(stderr_path.stat().st_mode)
        tool_profile_inputs = profile_tool_evidence["inputs"]
        persisted_profile_track = plan.track or plan.context.get(
            "profile_smoke_track"
        )
        if persisted_profile_track is None and profile_tool_track in {
            "cpu_profiles",
            "syscall_profiles",
            "structural_traces",
        }:
            persisted_profile_track = profile_tool_track
        profile_inputs = (
            self._profile_inputs(str(persisted_profile_track), tool_profile_inputs)
            if persisted_profile_track is not None and interrupted is None
            else {}
        )
        if profile is not None and interrupted is None and rc == 0:
            profile_rich_result = profile.finish()
        else:
            profile_rich_result = None
        record: dict[str, Any] = {
            "schema": getattr(self.schema, "CHILD_SCHEMA", "bn-2l3n-child-v3"),
            "protocol": PROTOCOL,
            "ordinal": ordinal,
            "kind": plan.kind,
            "context": plan.context,
            "context_sha256": context_sha,
            "argv": list(plan.argv),
            "environment": environment,
            "executable_path": str(plan.executable.path),
            "executable_sha256": plan.executable.sha256,
            "executable_mode": plan.executable.mode,
            "executable_comm": plan.executable.comm,
            "identity": identity,
            "waited_pid": child.pid,
            "started_at": started_at,
            "started_monotonic_ns": started_monotonic_ns,
            "completed_at": completed_at,
            "completed_monotonic_ns": completed_monotonic_ns,
            "exit_status": rc,
            "timed_out": timed_out,
            "terminated_by_runner": terminated,
            "interrupted": (
                f"{interrupted.__class__.__name__}: {interrupted}" if interrupted else None
            ),
            "reaping": reaping,
            "process_group_absent": group_absent,
            "orphan_process_group_detected": orphan_group_detected,
            "control_events": control_events,
            "control_events_sha256": sha256_bytes(canonical_json_bytes(control_events)),
            "profile_events": profile_events,
            "profile_events_sha256": sha256_bytes(canonical_json_bytes(profile_events)),
            "parked_state_proofs": parked_state_proofs,
            "profile_rich_result": profile_rich_result,
            "runner_context": None,
            "runner_context_sha256": None,
            "profile_result": None,
            "profile_result_sha256": None,
            "profile_contract_sha256": self.config["profile_contract_sha256"],
            "profile_tool_inputs": profile_inputs,
            "profile_tool_inputs_sha256": sha256_bytes(
                self.schema.canonical_json_bytes(profile_inputs)
            ),
            "profile_tool_helper_records": profile_tool_evidence["helper_records"],
            "raw_path": str(raw_path.resolve()),
            "raw_sha256": raw_sha,
            "raw_bytes": raw_bytes,
            "raw_mode_after": raw_mode_after,
            "stderr_path": str(stderr_path.resolve()),
            "stderr_sha256": stderr_sha,
            "stderr_bytes": stderr_bytes,
            "stderr_mode_after": stderr_mode_after,
            "expected_records": plan.expected_records,
            "combined_row_sha256": None,
            "csv_append": None,
            "guard_pre_ordinal": pre_guard["ordinal"],
            "guard_post_ordinal": None,
            "validation_error": None,
        }

        def publish_child_record() -> None:
            expected_fields = set(getattr(self.schema, "CHILD_FIELDS", ()))
            if expected_fields and set(record) != expected_fields:
                raise RunnerFailure(
                    "child record fields differ: "
                    f"missing={sorted(expected_fields - set(record))} "
                    f"extra={sorted(set(record) - expected_fields)}"
                )
            append_jsonl(self.child_manifest, record)

        raw_binding = {
            "schema": self.schema.RAW_BINDING_SCHEMA,
            "protocol": PROTOCOL,
            "child_ordinal": ordinal,
            "kind": plan.kind,
            "context_sha256": context_sha,
            "raw_path": str(raw_path.resolve()),
            "raw_sha256": raw_sha,
            "raw_bytes": raw_bytes,
            "expected_records": plan.expected_records,
            "stderr_path": str(stderr_path.resolve()),
            "stderr_sha256": stderr_sha,
        }
        require_exact_keys(
            raw_binding,
            set(self.schema.RAW_BINDING_FIELDS),
            "raw manifest binding",
        )
        append_jsonl(self.raw_manifest, raw_binding)
        try:
            post_guard = self.snapshot_processes(f"{ordinal:05d}-{plan.kind}-post")
            record["guard_post_ordinal"] = post_guard["ordinal"]
        except RunnerFailure as guard_failure:
            if interrupted is None:
                interrupted = guard_failure
            else:
                interrupted = RunnerFailure(f"{interrupted}; post guard: {guard_failure}")
        if interrupted is not None:
            publish_child_record()
            self.child_count += 1
            if isinstance(interrupted, RunnerFailure):
                raise interrupted
            raise RunnerFailure(f"child interrupted: {interrupted}")
        if rc not in plan.allowed_exit_statuses or orphan_group_detected or not group_absent:
            publish_child_record()
            self.child_count += 1
            raise RunnerFailure(
                f"{plan.kind} exited {rc}, orphan_group_detected={orphan_group_detected}, "
                f"process_group_absent={group_absent}"
            )
        try:
            records = self._parse_canonical_lines(raw_path, plan.expected_records)
            validated: Any = records[0] if len(records) == 1 else records
            if plan.track is not None:
                if profile_authority is None:
                    raise RunnerFailure("row profile authority is absent")
                profile_result = self._profile_fields(
                    plan.track,
                    profile_rich_result,
                    records[0],
                    control_events,
                    profile_inputs,
                    profile_authority,
                )
                record["profile_result"] = profile_result
                record["profile_result_sha256"] = sha256_bytes(
                    self.schema.canonical_json_bytes(profile_result)
                )
                validation_context = self._runner_context(plan, control_events)
                record["runner_context"] = validation_context
                record["runner_context_sha256"] = sha256_bytes(
                    self.schema.canonical_json_bytes(validation_context)
                )
                validated = self.schema.validate_child_records(
                    plan.track,
                    records,
                    f"{plan.track} row {plan.context.get('row_ordinal')}",
                    runner_context=validation_context,
                    profile_result=profile_result,
                )
                if not isinstance(validated, dict):
                    raise RunnerFailure(f"schema did not return one row for {plan.track}")
                record["combined_row_sha256"] = sha256_bytes(
                    self.schema.canonical_json_bytes(validated)
                )
                record["csv_append"] = self._append_csv(plan.track, validated)
            else:
                record["combined_row_sha256"] = None
                if plan.kind in {"correctness", "fault"}:
                    if len(records) != 1 or not isinstance(records[0], dict):
                        raise RunnerFailure("correctness child did not emit one object")
                    validated_child = self._validate_correctness_child(
                        plan, records[0], record
                    )
                    append_jsonl(self.correctness_manifest, validated_child)
                else:
                    for value in records:
                        append_jsonl(self.auxiliary_manifest, value)
            if plan.require_store_after and (
                plan.store_path is None or not plan.store_path.is_dir()
            ):
                raise RunnerFailure(f"{plan.kind} did not create its fresh store")
        except BaseException as error:
            record["validation_error"] = f"{error.__class__.__name__}: {error}"
            raw_path.chmod(0o444)
            stderr_path.chmod(0o444)
            record["raw_mode_after"] = stat.S_IMODE(raw_path.stat().st_mode)
            record["stderr_mode_after"] = stat.S_IMODE(stderr_path.stat().st_mode)
            publish_child_record()
            self.child_count += 1
            if isinstance(error, RunnerFailure):
                raise
            raise RunnerFailure(record["validation_error"]) from error
        raw_path.chmod(0o444)
        stderr_path.chmod(0o444)
        record["raw_mode_after"] = stat.S_IMODE(raw_path.stat().st_mode)
        record["stderr_mode_after"] = stat.S_IMODE(stderr_path.stat().st_mode)
        publish_child_record()
        self.child_count += 1
        self._settle(plan.context.get("durability"), f"{ordinal:05d}-{plan.kind}")
        return record


class _FakeSnapshot:
    def __init__(self, phase: str) -> None:
        self.phase = phase

    def to_json(self) -> dict[str, Any]:
        return {"phase": self.phase}


class _FakeProfile:
    def capture_phase(self, phase: str) -> _FakeSnapshot:
        return _FakeSnapshot(phase)

    def begin(self) -> _FakeSnapshot:
        return _FakeSnapshot("begin")

    def end(self) -> _FakeSnapshot:
        return _FakeSnapshot("end")

    def finish(self) -> dict[str, Any]:
        return {"schedstat_resolution_ns": 1, "status": "complete"}


class _FakeProfiles:
    PROFILE_SCHEMA = "bn-2l3n-profile-adapters-v3"
    PREFLIGHT_SCHEMA = "bn-2l3n-profile-preflight-v3"
    AUTHORITY_SCHEMA = "bn-2l3n-profile-authority-v3"
    SCHEDSTAT_DECISION_MULTIPLIER = 20
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

    @staticmethod
    def for_child(
        _child_pid: int,
        _variant: str,
        _track: str,
        *,
        authority: dict[str, Any],
        context: dict[str, Any],
    ) -> _FakeProfile:
        if not context or set(authority) != _FakeProfiles._AUTHORITY_FIELDS:
            raise ValueError("missing fake profile context/authority")
        return _FakeProfile()

    @staticmethod
    def profile_fields(
        _track: str,
        _result: dict[str, Any] | None,
        **_inputs: Any,
    ) -> dict[str, Any]:
        return {}

    @staticmethod
    def preflight_profile_contract() -> dict[str, Any]:
        contract = _FakeProfiles.profile_contract()
        return {
            "schema": _FakeProfiles.PREFLIGHT_SCHEMA,
            "protocol": PROTOCOL,
            "protocol_sha256": PROTOCOL_SHA256,
            "profile_contract_sha256": sha256_bytes(canonical_json_bytes(contract)),
            "source": "/proc/<pid>/task/<native-tid>/schedstat:first-field",
            "helper": "adapter-owned-cpu-bound-native-thread",
            "samples_ns": [0, 1, 2],
            "minimum_nonzero_increment_ns": 1,
            "decision_multiplier": _FakeProfiles.SCHEDSTAT_DECISION_MULTIPLIER,
            "decision_floor_ns": _FakeProfiles.SCHEDSTAT_DECISION_MULTIPLIER,
        }

    @staticmethod
    def profile_contract() -> dict[str, Any]:
        return _FakeSchema.expected_profile_contract()

    @staticmethod
    def perf_profile_inputs(
        stat_payload: str,
        ack_payload: str,
        permission: str,
        *,
        control_events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if (
            stat_payload
            or ack_payload
            or control_events
            or not permission.startswith("not_available;")
        ):
            raise ValueError("fake unavailable perf boundary differs")
        return {
            "perf_permission": permission,
            "perf_control_acknowledged": False,
            "perf_counters": [],
            "perf_control_events": [],
            "perf_stat_sha256": sha256_bytes(b""),
            "perf_stat_bytes": 0,
            "perf_ack_sha256": sha256_bytes(b""),
            "perf_ack_bytes": 0,
        }


class _FakeSchema:
    PROTOCOL = PROTOCOL
    ARTIFACT_FILE_MODE = 0o444
    ARTIFACT_INVENTORY_FIELDS = ("path", "bytes", "sha256", "mode")
    TOOLS_MANIFEST_SCHEMA = "asterism-rebaseline-tools-v3"
    TOOLS_MANIFEST_FIELDS = (
        "schema",
        "comm_allowlist",
        "tools",
        "support_files",
    )
    TOOL_BINDING_FIELDS = ("path", "sha256", "executable_mode", "comm")
    SUPPORT_FILE_FIELDS = ("path", "sha256", "mode")
    PREPARED_TOOL_NAMES: tuple[str, ...] = ()
    PREPARED_SUPPORT_FILE_NAMES: tuple[str, ...] = ()
    PREPARED_TOOL_COMMS: dict[str, str] = {}
    PROVENANCE_FILESYSTEM_FIELDS = (
        "mount_id",
        "parent_mount_id",
        "device",
        "root",
        "target",
        "mount_options",
        "filesystem_type",
        "source",
        "super_options",
    )
    RAW_BINDING_SCHEMA = "fake-raw-binding-v3"
    RAW_BINDING_FIELDS = (
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
    GUARD_SCHEMA = "fake-guard-v3"
    GUARD_BINDING_SCHEMA = "fake-guard-binding-v3"
    GUARD_SNAPSHOT_FIELDS = (
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
    GUARD_BINDING_FIELDS = (
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
    CORRECTNESS_SCHEMA = "fake-correctness-v1"
    CORRECTNESS_CHILD_SCHEMA = "bn-2l3n-correctness-child-v3"
    CORRECTNESS_CHILD_FIELDS = (
        "schema",
        "protocol",
        "attempt_nonce",
        "variant",
        "phase",
        "suite",
        "harness_sound",
        "boundedness",
        "cases",
    )
    CORRECTNESS_CHILD_CASE_FIELDS = ("id", "classification", "status")
    CORRECTNESS_BOUNDEDNESS_FIELDS = (
        "owner_ring_intents",
        "group_byte_bound_proven",
        "group_time_bound_proven",
        "waiter_reservations_after",
        "byte_reservations_after",
    )
    CORRECTNESS_EXPECTED_BOUNDEDNESS = {
        "owner_ring_intents": 1024,
        "group_byte_bound_proven": True,
        "group_time_bound_proven": True,
        "waiter_reservations_after": 0,
        "byte_reservations_after": 0,
    }
    CORRECTNESS_AGGREGATE_FIELDS = (
        "schema",
        "protocol",
        "attempt_nonce",
        "harness_sound",
        "boundedness",
        "cases",
    )
    CORRECTNESS_AGGREGATE_CASE_FIELDS = (
        "id",
        "variant",
        "phase",
        "suite",
        "kind",
        "classification",
        "status",
        "child_ordinal",
        "output_path",
        "output_sha256",
    )
    CORRECTNESS_CASE_IDS = ("fake-product", "fake-fault")
    ROW_SCHEMAS = {"primary": "fake-row-v1"}
    CSV_FIELDS_BY_TRACK = {"primary": ("schema", "value")}
    PROFILE_FIELDS_BY_TRACK = {"primary": ()}
    RUNNER_CONTEXT_FIELDS_BY_TRACK = {
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
        )
    }

    @staticmethod
    def expected_comm_allowlist() -> list[str]:
        return []

    @staticmethod
    def expected_profile_contract() -> dict[str, Any]:
        return {
            "schema": _FakeProfiles.PROFILE_SCHEMA,
            "protocol": PROTOCOL,
            "protocol_sha256": PROTOCOL_SHA256,
            "authority_schema": _FakeProfiles.AUTHORITY_SCHEMA,
            "c_role_lifetime_contract": C_ROLE_LIFETIME_CONTRACT,
            "variant_source_bindings": VARIANT_SOURCE_BINDINGS,
            "schedstat_decision_multiplier": 20,
            "perf_events": list(PERF_EVENTS),
            "perf_event_scope_policy": "exact-user-only",
            "syscall_events": list(TRACE_SYSCALLS),
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
                "sha256": sha256_bytes(b"ack\nack\n"),
            },
            "perf_child_environment": {
                "cpu_all": [PERF_PERMISSION_ENVIRONMENT],
                "cpu_available_only": list(PERF_CHILD_FD_ENVIRONMENT),
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
                    "producer-runtime:boot-to-runtime-births:comm=tokio-runtime-w",
                    "spawn_blocking-publication:ready-to-measured-births:comm=tokio-runtime-w",
                ],
                "D": ["owner:comm=mess-flat-owner"],
            },
            "open_helper_comms": [
                "fjall:worker",
                "mess-engine-rol",
                "mess-sealer",
            ],
            "profile_field_inputs": {
                "primary,new_names,fairness": ["schedstat_resolution_ns"],
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

    @staticmethod
    def correctness_descriptors() -> list[dict[str, str]]:
        result = [
            {
                "id": "public-common-oracle",
                "variant": variant,
                "phase": "oracle",
                "suite": "common-public-oracle",
                "kind": "correctness",
                "classification": "historical-oracle",
            }
            for variant in ("C", "D")
        ]
        for phase in ("pre", "post"):
            result.extend(
                (
                    {
                        "id": "fake-product",
                        "variant": "A",
                        "phase": phase,
                        "suite": "current-product",
                        "kind": "correctness",
                        "classification": "correctness",
                    },
                    {
                        "id": "fake-fault",
                        "variant": "A",
                        "phase": phase,
                        "suite": "current-fault",
                        "kind": "fault",
                        "classification": "boundedness",
                    },
                )
            )
        return result

    @staticmethod
    def canonical_json_bytes(value: Any) -> bytes:
        return canonical_json_bytes(value)

    @staticmethod
    def prepared_authority_canonical_json_bytes(value: Any) -> bytes:
        return reviewed_authority_canonical_json_bytes(value)

    @staticmethod
    def validate_child_records(
        kind: str,
        records: list[dict[str, Any]],
        _context: str,
        *,
        runner_context: dict[str, Any],
        profile_result: dict[str, Any],
    ) -> dict[str, Any]:
        if kind != "primary" or len(records) != 1:
            raise ValueError("fake child shape mismatch")
        if set(runner_context) != set(_FakeSchema.RUNNER_CONTEXT_FIELDS_BY_TRACK[kind]):
            raise ValueError("fake runner context mismatch")
        if profile_result != {} or records[0] != {"schema": "fake-raw-v1", "value": 7}:
            raise ValueError("fake raw/profile mismatch")
        return {"schema": "fake-row-v1", "value": 7}

    @staticmethod
    def encode_csv_row(
        track: str, row: dict[str, Any], write_header: bool = False
    ) -> str:
        if track != "primary" or row != {"schema": "fake-row-v1", "value": 7}:
            raise ValueError("fake row mismatch")
        return ("schema,value\n" if write_header else "") + "fake-row-v1,7\n"

    @staticmethod
    def expected_order(_config: dict[str, Any], track: str) -> list[dict[str, Any]]:
        if track != "primary":
            return []
        return []

    @staticmethod
    def row_child_environment(
        *,
        scratch_root: Path,
        attempt_nonce: str,
        output_dir: Path,
        config_path: Path,
        physical_ordinal: int,
        track: str,
        identity: dict[str, Any],
        context: dict[str, Any],
        context_sha256: str,
        control_fd: str,
        ptracer_pid: int | None = None,
        perf_permission_result: str | None = None,
        perf_command_fd: str | None = None,
        perf_ack_fd: str | None = None,
        perf_ack_ledger_fd: str | None = None,
    ) -> dict[str, str]:
        variant = str(identity["variant"])
        row_ordinal = int(identity["row_ordinal"])
        dynamic = {
            "ASTERISM_DURABILITY": context.get("durability"),
            "ASTERISM_PAYLOAD_BYTES": context.get("payload_size"),
            "ASTERISM_BATCH": context.get("batch_size"),
            "ASTERISM_WRITERS": context.get("writers"),
            "ASTERISM_BATCHES_PER_WRITER": context.get("batches_per_writer"),
            "ASTERISM_TRACE_KIND": context.get("trace_kind"),
            "ASTERISM_EXPECTED_DOMAIN_EVENTS": context.get("expected_domain_events"),
            "ASTERISM_EXPECTED_VISIBLE_EVENTS": context.get("expected_visible_events"),
            "ASTERISM_EXPECTED_LOG_EVENTS": context.get("expected_log_events"),
            "ASTERISM_EXPECTED_LOGICAL_DIGEST": context.get("expected_logical_digest"),
            "ASTERISM_EXPECTED_REGISTRY_HEAD_DIGEST": context.get(
                "expected_registry_head_digest"
            ),
        }
        store_track = (
            f"{track}-corpus"
            if track == "reopen"
            or (track == "structural_traces" and context.get("trace_kind") == "reopen")
            else track
        )
        store_identity = sha256_bytes(
            f"{attempt_nonce}\0{store_track}\0{row_ordinal}\0{variant}".encode()
        )[:20]
        result = {
            "HOME": str(scratch_root / "attempts" / attempt_nonce / "home"),
            "PATH": "/usr/bin:/bin",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "TZ": "UTC",
            "ASTERISM_REBASELINE_MODE": track,
            "ASTERISM_REBASELINE_VARIANT": variant,
            "ASTERISM_REBASELINE_CONFIG": str(config_path),
            "ASTERISM_REBASELINE_ROW_ORDINAL": str(row_ordinal),
            "ASTERISM_REBASELINE_PROTOCOL": PROTOCOL,
            "ASTERISM_REBASELINE_PROTOCOL_SHA256": PROTOCOL_SHA256,
            "ASTERISM_REBASELINE_CONTEXT": str(
                output_dir / "contexts" / f"{physical_ordinal:05d}.json"
            ),
            "ASTERISM_REBASELINE_CONTEXT_SHA256": context_sha256,
            "ASTERISM_CONTEXT_SHA256": context_sha256,
            "ASTERISM_REBASELINE_STORE": str(
                scratch_root
                / "attempts"
                / attempt_nonce
                / "stores"
                / f"{store_track}-{row_ordinal:05d}-{variant}-{store_identity}"
            ),
            "ASTERISM_REBASELINE_CONTROL_FD": control_fd,
            **{key: str(value) for key, value in dynamic.items() if value is not None},
        }
        if ptracer_pid is not None:
            result["ASTERISM_REBASELINE_PTRACER_PID"] = str(ptracer_pid)
        if perf_permission_result is not None:
            result[PERF_PERMISSION_ENVIRONMENT] = perf_permission_result
        for key, value in zip(
            PERF_CHILD_FD_ENVIRONMENT,
            (perf_command_fd, perf_ack_fd, perf_ack_ledger_fd),
            strict=True,
        ):
            if value is not None:
                result[key] = value
        return result

    @staticmethod
    def correctness_child_environment(**_arguments: Any) -> dict[str, str]:
        raise AssertionError("fake correctness children are not executed by runner self-test")


class _AdvancingClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        self.value += 1.0
        return self.value


def _write_fake_child(path: Path) -> None:
    payload = r'''#!/usr/bin/env python3
import ctypes, hashlib, json, os, pathlib, socket, subprocess, sys, time
ctypes.CDLL(None).prctl(15, b"ast-fake", 0, 0, 0)
def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
def send(sock, value):
    sock.sendall(canonical(value))
def recv(sock):
    data = bytearray()
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            raise SystemExit(91)
        data.extend(chunk)
    return json.loads(data)
context_path = pathlib.Path(os.environ["ASTERISM_REBASELINE_CONTEXT"])
context_sha = hashlib.sha256(context_path.read_bytes()).hexdigest()
row_ordinal = json.loads(context_path.read_bytes())["row_ordinal"]
variant = os.environ["ASTERISM_REBASELINE_VARIANT"]
control = socket.socket(fileno=int(os.environ["ASTERISM_REBASELINE_CONTROL_FD"]))
bad = row_ordinal == 2
boot = {"context_sha256": context_sha, "phase": "boot", "protocol_sha256": os.environ["ASTERISM_REBASELINE_PROTOCOL_SHA256"], "variant": variant}
if bad:
    boot["unexpected"] = True
send(control, boot)
ack = recv(control)
send(control, {"nonce": ack["nonce"], "phase": "runtime"})
ack = recv(control)
send(control, {"nonce": ack["nonce"], "phase": "opened"})
ack = recv(control)
base = time.monotonic_ns()
send(control, {"allocated_bytes_start": 100, "allocation_calls_start": 10, "context_sha256": context_sha, "counter_start_monotonic_ns": base + 1, "nonce": ack["nonce"], "phase": "ready", "process_system_cpu_start_ns": 20, "process_user_cpu_start_ns": 30, "protocol_sha256": os.environ["ASTERISM_REBASELINE_PROTOCOL_SHA256"], "ready_monotonic_ns": base, "variant": variant})
start = recv(control)
pathlib.Path(os.environ["ASTERISM_REBASELINE_STORE"]).mkdir(parents=True)
send(control, {"allocated_bytes_end": 120, "allocation_calls_end": 12, "counter_end_monotonic_ns": base + 7, "last_completion_monotonic_ns": base + 5, "nonce": start["nonce"], "phase": "measured", "process_system_cpu_end_ns": 22, "process_user_cpu_end_ns": 34, "release_monotonic_ns": base + 3, "t0_monotonic_ns": base + 2, "t1_monotonic_ns": base + 6})
recv(control)
if row_ordinal == 4:
    subprocess.Popen(["/bin/sleep", "30"])
sys.stdout.buffer.write(canonical({"schema": "fake-raw-v1", "value": 7}))
if row_ordinal == 30:
    sys.stdout.buffer.write(canonical({"schema": "fake-raw-v1", "value": 8}))
sys.stdout.buffer.flush()
raise SystemExit(9 if row_ordinal == 3 else 0)
'''
    atomic_write(path, payload.encode(), mode=0o755)


def _fixture_semantic_tree(
    role: str,
    seed: int,
    *,
    trusted: bool = False,
    non_ascii_entry: bool = False,
) -> dict[str, Any]:
    entries = [
        {
            "changed_ns": seed,
            "device": 100 + seed,
            "file_type": "directory",
            "gid": 0,
            "inode": 1_000 + seed,
            "link_count": 2,
            "modified_ns": seed,
            "path": ".",
            "permissions": 0o555 if trusted else 0o755,
            "sha256": None,
            "size": 0,
            "symlink_scope": None,
            "symlink_target": None,
            "uid": 0 if trusted else os.getuid(),
        }
    ]
    if non_ascii_entry:
        entries.append(
            {
                "changed_ns": seed + 1,
                "device": 100 + seed,
                "file_type": "regular",
                "gid": 0,
                "inode": 2_000 + seed,
                "link_count": 1,
                "modified_ns": seed + 1,
                "path": "Þfoo.go",
                "permissions": 0o444,
                "sha256": "f" * 64,
                "size": 1,
                "symlink_scope": None,
                "symlink_target": None,
                "uid": 0 if trusted else os.getuid(),
            }
        )
    return {
        "entries": entries,
        "role": role,
        "schema": RECURSIVE_TREE_AUTHORITY_SCHEMA,
    }


def _fixture_semantic_authority(
    root: Path,
    label: str,
    seed: int,
    *,
    source_role: str = "source",
    prepared_authority: bool = False,
) -> dict[str, Any]:
    canonicalizer = (
        reviewed_authority_canonical_json_bytes
        if prepared_authority
        else canonical_json_bytes
    )
    bindings: dict[str, dict[str, Any]] = {}
    for offset, (component, role) in enumerate(
        (
            ("source", source_role),
            ("toolchain", "toolchain"),
            ("cargo_home", "cargo_home"),
        )
    ):
        component_seed = seed if component == "source" else 500 + offset
        value = _fixture_semantic_tree(
            role,
            component_seed,
            non_ascii_entry=component == "cargo_home",
        )
        path = root / f"{label}-{component}.json"
        atomic_write(path, canonicalizer(value))
        path.chmod(0o444)
        bindings[component] = {
            "entry_count": len(value["entries"]),
            "equal_pre_post": True,
            "manifest_path": str(path.resolve()),
            "manifest_sha256": sha256(path),
            "mutation_events_absent": True,
            "role": role,
            "schema": RECURSIVE_TREE_AUTHORITY_SCHEMA,
            "watch_count": 1,
        }
    evidence_mounts = []
    binding_mounts = []
    for offset, (host, guest) in enumerate(TRUSTED_SYSTEM_MOUNTS, start=3):
        role = "system-" + guest.removeprefix("/").replace("/", "-")
        tree = _fixture_semantic_tree(role, 700 + offset, trusted=True)
        evidence_mounts.append(
            {
                "guest_path": guest,
                "host_path": host,
                "resolved_path": host,
                "tree": tree,
            }
        )
        entry = tree["entries"][0]
        binding_mounts.append(
            {
                "device": entry["device"],
                "gid": entry["gid"],
                "guest_path": guest,
                "host_path": host,
                "inode": entry["inode"],
                "permissions": entry["permissions"],
                "resolved_path": host,
                "trusted_root_owned_non_writable": True,
                "uid": entry["uid"],
            }
        )
    closure_value = {
        "mounts": evidence_mounts,
        "schema": TRUSTED_SYSTEM_CLOSURE_SCHEMA,
    }
    closure_path = root / f"{label}-trusted-system-closure.json"
    atomic_write(closure_path, canonicalizer(closure_value))
    closure_path.chmod(0o444)
    closure = {
        "entry_count": 3,
        "manifest_path": str(closure_path.resolve()),
        "mounts": binding_mounts,
        "mutation_events_absent": True,
        "schema": TRUSTED_SYSTEM_CLOSURE_SCHEMA,
        "sha256": sha256(closure_path),
        "watch_count": 3,
    }
    authority = {
        **bindings,
        "runtime_sha256": "0" * 64,
        "schema": SEMANTIC_INPUT_AUTHORITY_SCHEMA,
        "trusted_system_closure": closure,
    }
    authority["runtime_sha256"] = _semantic_runtime_sha256(authority)
    return authority


def _fixture_sandbox_argv(
    destinations: set[str],
    *,
    writable_destinations: set[str],
    data_destinations: set[str] | None = None,
    descriptor_bindings: tuple[tuple[str, str], ...] | None = None,
    toolchain: dict[str, Any] | None = None,
) -> list[str]:
    if data_destinations is None:
        data_destinations = set()
    bindings = descriptor_bindings or (
        *(
            ("--ro-bind-fd", guest)
            for _host, guest in TRUSTED_SYSTEM_MOUNTS
            if guest in destinations
        ),
        *(
            (
                "--bind-fd"
                if destination in writable_destinations
                else "--ro-bind-fd",
                destination,
            )
            for destination in sorted(
                destinations
                - {guest for _host, guest in TRUSTED_SYSTEM_MOUNTS}
            )
        ),
    )
    if (
        {destination for _option, destination in bindings} != destinations
        or {
            destination
            for option, destination in bindings
            if option == "--bind-fd"
        }
        != writable_destinations
    ):
        raise AssertionError("fixture descriptor binding topology differs")
    if toolchain is not None:
        system_bindings = tuple(
            ("--ro-bind-fd", guest) for _host, guest in TRUSTED_SYSTEM_MOUNTS
        )
        if bindings[: len(system_bindings)] != system_bindings:
            raise AssertionError("fixture trusted-system binding order differs")
        argv = [
            toolchain["bwrap_path"],
            "--die-with-parent",
            "--new-session",
            "--unshare-net",
            "--dir",
            "/usr",
        ]
        descriptor = 10
        for option, destination in system_bindings:
            argv.extend([option, str(descriptor), destination])
            descriptor += 1
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
                f"{SEMANTIC_CARGO_HOME_FD_PREFIX}9",
                "/dev/null",
                "--dir",
                "/proc",
                "--tmpfs",
                "/tmp",
                "--tmpfs",
                "/asterism",
            ]
        )
        remaining_bindings = bindings[len(system_bindings) :]
    else:
        argv = [
            "/usr/bin/bwrap",
            "--die-with-parent",
            "--new-session",
            "--unshare-net",
            "--dir",
            "/dev",
            "--dir",
            "/proc",
            "--tmpfs",
            "/tmp",
        ]
        descriptor = 10
        remaining_bindings = bindings
    source_data_destinations = tuple(
        destination
        for destination in SEMANTIC_CONFIG_DESTINATIONS[:2]
        if destination in data_destinations
    )
    cargo_home_data_destinations = tuple(
        destination
        for destination in SEMANTIC_CONFIG_DESTINATIONS[2:]
        if destination in data_destinations
    )
    other_data_destinations = tuple(
        sorted(data_destinations - set(SEMANTIC_CONFIG_DESTINATIONS))
    )

    def append_data_bindings(destinations: tuple[str, ...]) -> None:
        nonlocal descriptor
        for destination in destinations:
            argv.extend(["--ro-bind-data", str(descriptor), destination])
            descriptor += 1

    overlay_inserted = False
    for option, destination in remaining_bindings:
        argv.extend(
            [
                option,
                str(descriptor),
                destination,
            ]
        )
        descriptor += 1
        overlay_anchor = (
            _semantic_rust_lld_guest(toolchain)
            if toolchain is not None
            else "/asterism/toolchain/bin/rustc"
        )
        if not overlay_inserted and destination == overlay_anchor:
            append_data_bindings(source_data_destinations)
            argv.extend(
                [
                    "--overlay-src",
                    f"{SEMANTIC_CARGO_HOME_FD_PREFIX}{descriptor}",
                    "--tmp-overlay",
                    SEMANTIC_CARGO_HOME,
                ]
            )
            descriptor += 1
            append_data_bindings(cargo_home_data_destinations)
            overlay_inserted = True
    if not overlay_inserted:
        append_data_bindings(source_data_destinations)
        argv.extend(
            [
                "--overlay-src",
                f"{SEMANTIC_CARGO_HOME_FD_PREFIX}{descriptor}",
                "--tmp-overlay",
                SEMANTIC_CARGO_HOME,
            ]
        )
        descriptor += 1
        append_data_bindings(cargo_home_data_destinations)
    append_data_bindings(other_data_destinations)
    argv.extend(
        [
            "--remount-ro",
            SEMANTIC_CARGO_HOME,
            "--chdir",
            "/asterism/source",
            "/asterism/toolchain/bin/cargo",
        ]
    )
    return argv


def _fixture_current_cargo_config(
    preserved_cargo_home: tuple[dict[str, Any], ...] = (),
) -> dict[str, Any]:
    guest_paths = (
        "/asterism/source/.cargo/config.toml",
        "/asterism/source/.cargo/config",
        "/asterism/.cargo/config.toml",
        "/asterism/.cargo/config",
        "/.cargo/config.toml",
        "/.cargo/config",
        "/asterism/cargo-home/config.toml",
        "/asterism/cargo-home/config",
    )
    search_entries = [
        {
            "path": path,
            "sha256": "a" * 64 if index == 0 else None,
            "status": "present" if index == 0 else "absent",
        }
        for index, path in enumerate(guest_paths)
    ]
    cargo_home_entry_count = 1 + len(preserved_cargo_home)
    cargo_home_watch_count = 1 + sum(
        entry.get("type") == "directory" for entry in preserved_cargo_home
    )
    return {
        "cargo_search": {
            "cargo_home_path": "/asterism/cargo-home",
            "cwd": "/asterism/source",
            "entries": search_entries,
            "schema": "asterism-rebaseline-cargo-config-search-v3",
        },
        "cargo_home_tree": {
            "entry_count": cargo_home_entry_count,
            "equal_pre_post": True,
            "path": "/fixture/manifests/cargo-home.json",
            "post_sha256": "b" * 64,
            "pre_sha256": "b" * 64,
            "watch_count": cargo_home_watch_count,
        },
        "preserved_top_level_entries": {
            "cargo-home": list(preserved_cargo_home),
            "source": [],
        },
        "schema": CURRENT_BUILD_CARGO_CONFIG_SCHEMA,
    }


def _fixture_preserved_cargo_home_directory(name: str, ordinal: int) -> dict[str, Any]:
    return {
        "identity": {
            "changed_ns": ordinal,
            "device": 1,
            "file_type": stat.S_IFDIR,
            "inode": ordinal,
            "link_count": 2,
            "modified_ns": ordinal,
            "path": f"/fixture/cargo-home/{name}",
            "permissions": 0o755,
            "size": 4096,
        },
        "name": name,
        "type": "directory",
    }


def _fixture_build_sandbox(
    authority: dict[str, Any],
    destinations: set[str],
    *,
    writable_destinations: set[str],
    data_destinations: set[str] | None = None,
    passed_file_descriptors: int | None = None,
    include_passed_file_descriptors: bool = True,
    require_loader_origin: bool = True,
    descriptor_bindings: tuple[tuple[str, str], ...] | None = None,
    execution_tools: dict[str, Any] | None = None,
    toolchain: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if data_destinations is None:
        data_destinations = set()
    cargo_config = _fixture_current_cargo_config()
    return {
        "argv": _fixture_sandbox_argv(
            destinations,
            writable_destinations=writable_destinations,
            data_destinations=data_destinations,
            descriptor_bindings=descriptor_bindings,
            toolchain=toolchain,
        ),
        "environment": {
            **(
                {"LD_ORIGIN_PATH": SEMANTIC_TOOLCHAIN_BIN}
                if require_loader_origin
                else {}
            ),
            "RUSTUP_HOME": "/nonexistent",
        },
        "cargo_config_prebuild": cargo_config,
        "cargo_config_postbuild": json.loads(canonical_json_bytes(cargo_config)),
        "execution": {
            "passed_file_descriptors": (
                (
                    len(destinations)
                    + len(data_destinations)
                    + 2
                    + int(toolchain is not None)
                )
                if passed_file_descriptors is None
                else passed_file_descriptors
            )
        }
        if include_passed_file_descriptors
        else {"passed_file_descriptors": 16},
        "execution_tools": execution_tools,
        "semantic_input_authority": authority,
        "toolchain": toolchain,
    }


def _fixture_prepared(root: Path, executable: Path) -> Prepared:
    digest = sha256(executable)
    bound = Executable("fake", executable, digest, stat.S_IMODE(executable.stat().st_mode), "ast-fake")
    rustc_host = "x86_64-unknown-linux-gnu"
    rustup_toolchain = "fixture-toolchain"
    rustup_home = root / "fixture-rustup-home"
    toolchain_root = rustup_home / "toolchains" / rustup_toolchain
    cargo_home = root / "fixture-cargo-home"
    cargo_path = toolchain_root / "bin" / "cargo"
    rustc_path = toolchain_root / "bin" / "rustc"
    rust_lld_path = (
        toolchain_root
        / "lib"
        / "rustlib"
        / rustc_host
        / "bin"
        / "rust-lld"
    )
    required_system_tools = {
        name: shutil.which(name) for name in ("bwrap", "git", "rustup")
    }
    if any(path is None for path in required_system_tools.values()):
        raise RunnerFailure(
            "semantic fixture system tools are absent", exit_code=2
        )
    bwrap_path = Path(str(required_system_tools["bwrap"])).resolve(strict=True)
    git_path = Path(str(required_system_tools["git"])).resolve(strict=True)
    rustup_path = Path(str(required_system_tools["rustup"])).resolve(strict=True)
    for directory in (
        cargo_home,
        cargo_path.parent,
        rust_lld_path.parent,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    for path, payload in (
        (cargo_path, b"fixture cargo\n"),
        (rustc_path, b"fixture rustc\n"),
        (rust_lld_path, b"fixture rust-lld\n"),
    ):
        atomic_write(path, payload, mode=0o555)
    toolchain = {
        "bwrap_path": str(bwrap_path.resolve()),
        "bwrap_sha256": sha256(bwrap_path),
        "cargo_home_path": str(cargo_home.resolve()),
        "cargo_path": str(cargo_path.resolve()),
        "cargo_sha256": sha256(cargo_path),
        "cargo_version_verbose": "cargo 1.0.0 (fixture)",
        "git_path": str(git_path.resolve()),
        "git_sha256": sha256(git_path),
        "rustc_host": rustc_host,
        "rustc_path": str(rustc_path.resolve()),
        "rustc_sha256": sha256(rustc_path),
        "rustc_version_verbose": (
            f"rustc 1.0.0 (fixture)\nhost: {rustc_host}"
        ),
        "rust_lld_path": str(rust_lld_path.resolve()),
        "rust_lld_sha256": sha256(rust_lld_path),
        "rustup_home_path": str(rustup_home.resolve()),
        "rustup_path": str(rustup_path.resolve()),
        "rustup_sha256": sha256(rustup_path),
        "rustup_toolchain": rustup_toolchain,
    }

    def fixture_current_file_identity(path: Path) -> dict[str, Any]:
        exact = path.resolve(strict=True)
        metadata = exact.lstat()
        return {
            "bytes": metadata.st_size,
            "ctime_ns": metadata.st_ctime_ns,
            "device": metadata.st_dev,
            "inode": metadata.st_ino,
            "link_count": metadata.st_nlink,
            "mode": stat.S_IMODE(metadata.st_mode),
            "mtime_ns": metadata.st_mtime_ns,
            "path": str(exact),
            "sha256": sha256(exact),
            "size": metadata.st_size,
        }

    def fixture_current_tool(path: Path, *, trusted: bool) -> dict[str, Any]:
        exact = path.resolve(strict=True)
        paths = [Path("/"), *list(exact.parents)[::-1][1:], exact]
        return {
            "identity": fixture_current_file_identity(exact),
            "path_chain": _semantic_path_chain(paths) if trusted else None,
            "trusted_system": trusted,
        }

    def fixture_null_device() -> dict[str, Any]:
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
            "parent_path_chain": _semantic_path_chain(
                [Path("/"), Path("/dev")]
            ),
            "trusted_system": True,
        }

    def fixture_current_directory(path: Path) -> dict[str, Any]:
        exact = path.resolve(strict=True)
        metadata = exact.lstat()
        return {
            "changed_ns": metadata.st_ctime_ns,
            "device": metadata.st_dev,
            "file_type": stat.S_IFMT(metadata.st_mode),
            "inode": metadata.st_ino,
            "link_count": metadata.st_nlink,
            "modified_ns": metadata.st_mtime_ns,
            "path": str(exact),
            "permissions": stat.S_IMODE(metadata.st_mode),
            "size": metadata.st_size,
        }

    def fixture_current_execution_tools() -> dict[str, Any]:
        system_python = Path("/usr/bin/python3").resolve(strict=True)
        return {
            "bwrap": fixture_current_tool(bwrap_path, trusted=True),
            "cargo": fixture_current_tool(cargo_path, trusted=False),
            "dev_null": fixture_null_device(),
            "python": fixture_current_tool(system_python, trusted=True),
            "rustc": fixture_current_tool(rustc_path, trusted=False),
            "rust_lld": fixture_current_tool(rust_lld_path, trusted=False),
            "toolchain_root": fixture_current_directory(toolchain_root),
        }

    def fixture_execution_file(path: Path) -> dict[str, Any]:
        exact = path.resolve(strict=True)
        metadata = exact.lstat()
        return {
            "identity": {
                "changed_ns": metadata.st_ctime_ns,
                "device": metadata.st_dev,
                "inode": metadata.st_ino,
                "link_count": metadata.st_nlink,
                "modified_ns": metadata.st_mtime_ns,
            },
            "mode": stat.S_IMODE(metadata.st_mode),
            "path": str(exact),
            "sha256": sha256(exact),
            "size": metadata.st_size,
        }

    def fixture_prepared_execution_tools() -> dict[str, Any]:
        metadata = toolchain_root.lstat()
        return {
            "bwrap": fixture_execution_file(bwrap_path),
            "cargo": fixture_execution_file(cargo_path),
            "dev_null": fixture_null_device(),
            "rustc": fixture_execution_file(rustc_path),
            "rust_lld": fixture_execution_file(rust_lld_path),
            "toolchain_root": {
                "device": metadata.st_dev,
                "inode": metadata.st_ino,
                "link_count": metadata.st_nlink,
                "mode": stat.S_IMODE(metadata.st_mode),
            },
        }
    variants = {
        name: Variant(
            name=name,
            product_commit=(str(index + 1) * 40)[:40],
            product_tree=(str(index + 5) * 40)[:40],
            binary_kind="bare" if name == "B" else "public",
            timed_surface="raw-numeric" if name == "B" else "public-event-store",
            correctness_oracle_mode=name in PUBLIC_VARIANTS,
            executable=bound,
            contract={
                "variant": name,
                "cargo_lock_sha256": "a" * 64,
            },
            contract_argv=(str(executable),),
            contract_env={},
            evidence_argv=(str(executable), str(root / "fake-child.py")),
            evidence_env={},
            trace_path_marker_templates={
                "root_environment": "ASTERISM_REBASELINE_STORE",
                "log": [],
                "metadata": [],
            },
        )
        for index, name in enumerate(VARIANTS)
    }
    manifest = root / "prepared-artifacts.json"
    approval = root / "source-approval.json"
    tools_manifest_value = {
        "schema": "asterism-rebaseline-tools-v3",
        "comm_allowlist": [],
        "tools": {},
        "support_files": {},
    }
    bindings = root / "bindings"
    bindings.mkdir()
    tools_manifest = bindings / "tools-manifest.json"
    source_review_paths = {
        "bundle": bindings / "source-review-bundle.json",
        "current_children_attestation": (
            bindings / "current-children-attestation.json"
        ),
        "lock_authority": bindings / "lock-review-authority.json",
        "lock_review_bundle": bindings / "lock-review-bundle.json",
    }
    semantic_root = root / "semantic-manifests"
    semantic_root.mkdir()
    authorities: dict[str, dict[str, Any]] = {}
    seed = 1
    for label in (
        "current-children",
        "current-hooked-release",
        "current-pristine-release",
        "release-A",
        "release-B",
        "release-C",
        "release-D",
        "release-overlay-A",
    ):
        authorities[label] = _fixture_semantic_authority(
            semantic_root,
            label,
            seed,
            prepared_authority=label.startswith("release-"),
        )
        seed += 10
    for variant in ("C", "D"):
        for role in ("current", "generated"):
            label = f"resolver-{variant}-{role}"
            authorities[label] = _fixture_semantic_authority(
                semantic_root,
                label,
                seed,
                source_role="resolution_source_without_cargo_lock",
                prepared_authority=True,
            )
            seed += 10
    current_builds = {}
    current_build_destinations = {
        *CURRENT_BUILD_FD_DESTINATIONS,
        _semantic_rust_lld_guest(toolchain),
    }
    for name, label, destinations, writable, passed in (
        (
            "children",
            "current-children",
            current_build_destinations
            | {"/asterism/rustc_workspace_wrapper.py", "/asterism/receipt"},
            {"/asterism/target", "/asterism/receipt"},
            20,
        ),
        (
            "hooked_release",
            "current-hooked-release",
            current_build_destinations,
            {"/asterism/target"},
            18,
        ),
        (
            "pristine_release",
            "current-pristine-release",
            current_build_destinations,
            {"/asterism/target"},
            18,
        ),
    ):
        current_builds[name] = _fixture_build_sandbox(
            authorities[label],
            destinations,
            writable_destinations=writable,
            data_destinations=set(SEMANTIC_CONFIG_DESTINATIONS),
            passed_file_descriptors=passed,
            execution_tools=fixture_current_execution_tools(),
            toolchain=toolchain,
        )
    approval_variants: dict[str, dict[str, Any]] = {}
    lock_variants: dict[str, dict[str, Any]] = {}
    for name in VARIANTS:
        if name in {"A", "B"}:
            tracked = {"resolver_kind": "tracked_git_readback"}
            approval_variants[name] = {
                "current_lock_attempt": None,
                "lock_resolution": tracked,
            }
            lock_variants[name] = {
                "current_lock_attempt": None,
                "resolver": json.loads(canonical_json_bytes(tracked)),
            }
            continue
        records = {}
        for role in ("current", "generated"):
            sandbox = _fixture_build_sandbox(
                authorities[f"resolver-{name}-{role}"],
                RESOLVER_DESTINATIONS,
                writable_destinations={"/asterism/source"},
                require_loader_origin=False,
                descriptor_bindings=RESOLVER_DESCRIPTOR_BINDINGS,
            )
            records[role] = {
                "argv": sandbox["argv"],
                "environment": sandbox["environment"],
                "passed_file_descriptors": sandbox["execution"][
                    "passed_file_descriptors"
                ],
                "resolver_kind": "sandboxed_cargo_resolution",
                "semantic_input_authority": sandbox["semantic_input_authority"],
            }
        approval_variants[name] = {
            "current_lock_attempt": records["current"],
            "lock_resolution": records["generated"],
        }
        lock_variants[name] = {
            "current_lock_attempt": json.loads(
                canonical_json_bytes(records["current"])
            ),
            "resolver": json.loads(canonical_json_bytes(records["generated"])),
        }
    source_review_values = {
        "bundle": {"schema": "bn-3hch-source-review-bundle-v1"},
        "current_children_attestation": {
            "builds": current_builds,
            "schema": CURRENT_CHILDREN_ATTESTATION_SCHEMA,
            "toolchain": toolchain,
            "toolchain_identities": [
                fixture_current_file_identity(
                    Path(toolchain[f"{name}_path"])
                )
                for name in (
                    "bwrap",
                    "cargo",
                    "git",
                    "rustc",
                    "rust_lld",
                    "rustup",
                )
            ],
        },
        "lock_authority": {
            "lock_manifest": {
                "payload": {"variants": lock_variants},
            },
            "schema": "bn-31gp-current-lock-authority-v1",
        },
        "lock_review_bundle": {
            "schema": "bn-31gp-current-lock-review-bundle-v1"
        },
    }
    for name, source_review_path in source_review_paths.items():
        atomic_json(source_review_path, source_review_values[name])
        source_review_path.chmod(0o444)
    manifests = root / "manifests"
    manifests.mkdir()
    release_attestations = {}
    for name in VARIANTS:
        sandbox = _fixture_build_sandbox(
            authorities[f"release-{name}"],
            _release_build_destinations(toolchain),
            writable_destinations={"/asterism/target"},
            include_passed_file_descriptors=False,
            descriptor_bindings=_release_build_descriptor_bindings(toolchain),
            execution_tools=fixture_prepared_execution_tools(),
            toolchain=toolchain,
        )
        release_attestations[name] = {
            "build_argv": sandbox["argv"],
            "build_child": sandbox["execution"],
            "build_env": sandbox["environment"],
            "cargo_config_search": {
                "path": "/fixture/empty-cargo-config",
                "sha256": sha256_bytes(b""),
            },
            "execution_tools": sandbox["execution_tools"],
            "semantic_input_authority": sandbox["semantic_input_authority"],
            "toolchain": toolchain,
        }
    overlay_sandbox = _fixture_build_sandbox(
        authorities["release-overlay-A"],
        _release_build_destinations(toolchain),
        writable_destinations={"/asterism/target"},
        include_passed_file_descriptors=False,
        descriptor_bindings=_release_build_descriptor_bindings(toolchain),
        execution_tools=fixture_prepared_execution_tools(),
        toolchain=toolchain,
    )
    overlay_attestation = {
        "build_argv": overlay_sandbox["argv"],
        "build_child": overlay_sandbox["execution"],
        "build_env": overlay_sandbox["environment"],
        "cargo_config_search": {
            "path": "/fixture/empty-cargo-config",
            "sha256": sha256_bytes(b""),
        },
        "execution_tools": overlay_sandbox["execution_tools"],
        "semantic_input_authority": overlay_sandbox["semantic_input_authority"],
        "toolchain": toolchain,
    }

    def fixture_release_sandbox_sha256(attestation: dict[str, Any]) -> str:
        return _validate_semantic_sandbox(
            attestation["build_argv"],
            attestation["build_env"],
            "fixture release",
            passed_file_descriptors=None,
            expected_passed_file_descriptors=None,
            require_loader_origin=True,
            require_passed_file_descriptors=False,
            expected_fd_destinations=_release_build_destinations(toolchain),
            writable_fd_destinations={"/asterism/target"},
            expected_descriptor_bindings=_release_build_descriptor_bindings(
                toolchain
            ),
            execution_tools=attestation["execution_tools"],
            execution_tools_kind="prepared",
            toolchain=toolchain,
            cargo_config_search_sha256=attestation["cargo_config_search"][
                "sha256"
            ],
            semantic_runtime_sha256=attestation[
                "semantic_input_authority"
            ]["runtime_sha256"],
            prepared_authority_canonicalizer=(
                reviewed_authority_canonical_json_bytes
            ),
        )

    release_compile_out_path = manifests / "release-compile-out.json"
    release_compile_out_value = {
        "builds": {
            "ordinary_a": {
                "attestation": release_attestations["A"],
                "sandbox_sha256": fixture_release_sandbox_sha256(
                    release_attestations["A"]
                ),
            },
            "overlay_a": {
                "attestation": overlay_attestation,
                "sandbox_sha256": fixture_release_sandbox_sha256(
                    overlay_attestation
                ),
            },
        },
        "protocol": PROTOCOL,
        "protocol_sha256": PROTOCOL_SHA256,
        "schema": "bn-3hch-release-compile-out-v1",
        "status": "ok",
    }
    atomic_json(release_compile_out_path, release_compile_out_value)
    release_compile_out_path.chmod(0o444)
    source_approval_value = {
        "review_id": "fixture-review",
        "status": "approved",
        "variants": approval_variants,
    }
    atomic_json(tools_manifest, tools_manifest_value)
    atomic_json(approval, source_approval_value)
    atomic_json(manifest, {"fixture": True})
    tools_manifest.chmod(0o444)
    bindings.chmod(0o555)
    manifests.chmod(0o555)
    approval.chmod(0o444)
    manifest.chmod(0o444)
    semantic_root.chmod(0o555)
    prepared_value = {
        "toolchain": toolchain,
        "tooling_commit": "1" * 40,
        "tooling_tree": "2" * 40,
        "variants": {
            name: {"attestation": release_attestations[name]} for name in VARIANTS
        },
    }
    fixture_schema = _FakeSchema()
    fixture_schema.semantic_runtime_content_sha256 = (
        load_schema().semantic_runtime_content_sha256
    )
    return Prepared(
        schema=fixture_schema,
        path=manifest,
        digest=sha256(manifest),
        value=prepared_value,
        root=root,
        source_approval=approval,
        source_approval_sha256=sha256(approval),
        source_approval_value=source_approval_value,
        source_review_files={
            name: SupportFile(
                f"prepared-source-review-{name}",
                source_review_path,
                sha256(source_review_path),
                0o444,
            )
            for name, source_review_path in source_review_paths.items()
        },
        release_compile_out=SupportFile(
            "prepared-release-compile-out",
            release_compile_out_path,
            sha256(release_compile_out_path),
            0o444,
        ),
        release_compile_out_value=release_compile_out_value,
        tools_manifest=SupportFile(
            "prepared-tools-manifest",
            tools_manifest,
            sha256(tools_manifest),
            0o444,
        ),
        tools_manifest_value=tools_manifest_value,
        claim_path=root / "consumption.json",
        variants=variants,
        tools={},
        support_files={
            "profile_adapter": SupportFile(
                "profile-adapter", manifest, sha256(manifest), 0o444
            )
        },
        inputs={},
        tracked_comm=frozenset({RUNNER_COMM, "ast-fake"}),
    )


def _detached(value: Any) -> Any:
    return json.loads(canonical_json_bytes(value))


def _fixture_with_release_a_mutation(
    prepared: Prepared, mutation: Callable[[dict[str, Any]], None]
) -> Prepared:
    value = _detached(prepared.value)
    proof = _detached(prepared.release_compile_out_value)
    attestation = value["variants"]["A"]["attestation"]
    mutation(attestation)
    proof["builds"]["ordinary_a"]["attestation"] = _detached(attestation)
    return replace(
        prepared,
        value=value,
        release_compile_out_value=proof,
    )


def _semantic_authority_static_checks(
    root: Path, prepared: Prepared
) -> tuple[dict[str, bool], dict[str, str]]:
    checks: dict[str, bool] = {}
    details: dict[str, str] = {}
    hostile = root / "hostile-semantic"
    hostile.mkdir()

    unicode_value = {
        "label": "A — reviewed",
        "schema": "bn-ecm1-runner-reviewed-authority-utf8-self-test-v1",
    }
    unicode_path = hostile / "reviewed-authority-utf8.json"
    atomic_write(
        unicode_path,
        reviewed_authority_canonical_json_bytes(unicode_value),
        mode=0o444,
    )
    unicode_support = SupportFile(
        "reviewed-authority-utf8-self-test",
        unicode_path,
        sha256(unicode_path),
        0o444,
    )
    checks["reviewed_authority_utf8_non_ascii_accepted"] = (
        _load_canonical_support_authority(
            unicode_support, "reviewed authority UTF-8 self-test"
        )
        == unicode_value
        and load_reviewed_authority_json(unicode_path) == unicode_value
    )
    schema_unicode_bytes = (
        prepared.schema.prepared_authority_canonical_json_bytes(unicode_value)
    )
    checks["prepared_sandbox_hash_uses_schema_utf8_canonicalization"] = (
        schema_unicode_bytes
        == reviewed_authority_canonical_json_bytes(unicode_value)
        and schema_unicode_bytes != canonical_json_bytes(unicode_value)
    )
    details["prepared_sandbox_hash_uses_schema_utf8_canonicalization"] = (
        f"schema_bytes={len(schema_unicode_bytes)}"
    )
    ascii_escaped_path = hostile / "reviewed-authority-ascii-escaped.json"
    atomic_write(
        ascii_escaped_path, canonical_json_bytes(unicode_value), mode=0o444
    )
    ascii_escaped_support = SupportFile(
        "reviewed-authority-ascii-escaped-hostile",
        ascii_escaped_path,
        sha256(ascii_escaped_path),
        0o444,
    )
    try:
        _load_canonical_support_authority(
            ascii_escaped_support,
            "ASCII-escaped reviewed authority hostile",
        )
    except RunnerFailure as error:
        checks["reviewed_authority_ascii_escaped_rejected"] = True
        details["reviewed_authority_ascii_escaped_rejected"] = error.reason
    else:
        checks["reviewed_authority_ascii_escaped_rejected"] = False
        details["reviewed_authority_ascii_escaped_rejected"] = (
            "ASCII-escaped reviewed authority was accepted"
        )
    try:
        load_reviewed_authority_json(ascii_escaped_path)
    except RunnerFailure as error:
        checks["reviewed_authority_file_ascii_escaped_rejected"] = True
        details["reviewed_authority_file_ascii_escaped_rejected"] = error.reason
    else:
        checks["reviewed_authority_file_ascii_escaped_rejected"] = False
        details["reviewed_authority_file_ascii_escaped_rejected"] = (
            "ASCII-escaped reviewed authority file was accepted"
        )

    semantic_unicode_value = {
        "label": "source — reviewed",
        "schema": RECURSIVE_TREE_AUTHORITY_SCHEMA,
    }
    semantic_unicode_path = hostile / "reviewed-semantic-utf8.json"
    atomic_write(
        semantic_unicode_path,
        reviewed_authority_canonical_json_bytes(semantic_unicode_value),
        mode=0o444,
    )
    semantic_unicode_snapshot = _retain_semantic_manifest(
        str(semantic_unicode_path),
        sha256(semantic_unicode_path),
        RECURSIVE_TREE_AUTHORITY_SCHEMA,
        "reviewed semantic UTF-8 self-test",
        canonicalizer=reviewed_authority_canonical_json_bytes,
    )
    try:
        checks["reviewed_semantic_utf8_non_ascii_accepted"] = (
            semantic_unicode_snapshot.value == semantic_unicode_value
        )
    finally:
        semantic_unicode_snapshot.close()
    semantic_ascii_path = hostile / "reviewed-semantic-ascii-escaped.json"
    atomic_write(
        semantic_ascii_path,
        canonical_json_bytes(semantic_unicode_value),
        mode=0o444,
    )
    try:
        _retain_semantic_manifest(
            str(semantic_ascii_path),
            sha256(semantic_ascii_path),
            RECURSIVE_TREE_AUTHORITY_SCHEMA,
            "ASCII-escaped reviewed semantic hostile",
            canonicalizer=reviewed_authority_canonical_json_bytes,
        )
    except RunnerFailure as error:
        checks["reviewed_semantic_ascii_escaped_rejected"] = True
        details["reviewed_semantic_ascii_escaped_rejected"] = error.reason
    else:
        checks["reviewed_semantic_ascii_escaped_rejected"] = False
        details["reviewed_semantic_ascii_escaped_rejected"] = (
            "ASCII-escaped reviewed semantic evidence was accepted"
        )

    def rejected(name: str, candidate: Prepared) -> None:
        snapshots: list[RetainedSemanticManifest] = []
        try:
            snapshots, _counts = retain_prepared_semantic_manifests(candidate)
        except RunnerFailure as error:
            checks[name] = True
            details[name] = error.reason
        else:
            checks[name] = False
            details[name] = "hostile semantic authority was accepted"
        finally:
            for snapshot in snapshots:
                snapshot.close()

    snapshots, counts = retain_prepared_semantic_manifests(prepared)
    try:
        checks["exact_12_20_16_topology"] = (
            counts == SEMANTIC_MANIFEST_COUNTS
            and len(snapshots) == SEMANTIC_MANIFEST_TOTAL
        )
        checks["all_48_retained_and_rechecked"] = all(
            snapshot.verify() is None and snapshot.descriptor >= 0
            for snapshot in snapshots
        )
        release_children = [
            prepared.value["variants"][name]["attestation"]["build_child"]
            for name in VARIANTS
        ] + [
            prepared.release_compile_out_value["builds"][proof]["attestation"][
                "build_child"
            ]
            for proof in ("ordinary_a", "overlay_a")
        ]
        checks["release_child_exact_passed_fd_count"] = all(
            _semantic_integer(child.get("passed_file_descriptors"))
            and child["passed_file_descriptors"] == 16
            for child in release_children
        )
        current_fixture = load_reviewed_authority_json(
            prepared.source_review_files["current_children_attestation"].path
        )
        checks["current_exact_release_and_child_fd_counts"] = {
            name: current_fixture["builds"][name]["execution"][
                "passed_file_descriptors"
            ]
            for name in ("children", "hooked_release", "pristine_release")
        } == {
            "children": 20,
            "hooked_release": 18,
            "pristine_release": 18,
        }
        resolver_records = [
            prepared.source_approval_value["variants"][variant][field]
            for variant in ("C", "D")
            for field in ("current_lock_attempt", "lock_resolution")
        ]
        checks["resolver_exact_13_fd_private_authority"] = all(
            record["passed_file_descriptors"] == 13
            and not isinstance(record["passed_file_descriptors"], bool)
            and "--dev-bind" not in record["argv"]
            and "LD_ORIGIN_PATH" not in record["environment"]
            and not any("rust-lld" in argument for argument in record["argv"])
            for record in resolver_records
        )
        identities: dict[tuple[int, int], str] = {}
        _claim_semantic_physical_identity(identities, snapshots[0])
        physical_alias = replace(
            snapshots[1],
            context="hostile physical alias",
            identity=dict(snapshots[0].identity),
        )
        try:
            _claim_semantic_physical_identity(identities, physical_alias)
        except RunnerFailure as error:
            checks["physical_identity_alias_rejected"] = True
            details["physical_identity_alias_rejected"] = error.reason
        else:
            checks["physical_identity_alias_rejected"] = False
            details["physical_identity_alias_rejected"] = (
                "duplicate retained device/inode was accepted"
            )
    finally:
        for snapshot in snapshots:
            snapshot.close()
    checks["all_48_closed_at_lifetime_boundary"] = all(
        snapshot.descriptor == -1 for snapshot in snapshots
    )

    hostile_proof = _detached(prepared.release_compile_out_value)
    hostile_proof["builds"]["ordinary_a"]["sandbox_sha256"] = "0" * 64
    rejected(
        "release_reviewed_sandbox_digest_crosslink_rejected",
        replace(prepared, release_compile_out_value=hostile_proof),
    )

    def authority(attestation: dict[str, Any]) -> dict[str, Any]:
        return attestation["semantic_input_authority"]

    rejected(
        "missing_manifest_rejected",
        _fixture_with_release_a_mutation(
            prepared,
            lambda attestation: authority(attestation)["source"].__setitem__(
                "manifest_path", str((hostile / "missing.json").resolve())
            ),
        ),
    )

    def swap_source_toolchain(attestation: dict[str, Any]) -> None:
        value = authority(attestation)
        value["source"], value["toolchain"] = value["toolchain"], value["source"]
        value["runtime_sha256"] = _semantic_runtime_sha256(value)

    rejected(
        "swapped_source_toolchain_rejected",
        _fixture_with_release_a_mutation(prepared, swap_source_toolchain),
    )

    original_source = Path(
        prepared.value["variants"]["A"]["attestation"][
            "semantic_input_authority"
        ]["source"]["manifest_path"]
    )

    def bind_release_source(attestation: dict[str, Any], path: Path) -> None:
        binding = authority(attestation)["source"]
        binding["manifest_path"] = str(path.absolute())
        binding["manifest_sha256"] = sha256_bytes(path.read_bytes())

    symlink_path = hostile / "symlink.json"
    symlink_path.symlink_to(original_source)
    rejected(
        "symlink_manifest_rejected",
        _fixture_with_release_a_mutation(
            prepared, lambda attestation: bind_release_source(attestation, symlink_path)
        ),
    )
    symlink_path.unlink()

    hardlink_path = hostile / "hardlink.json"
    os.link(original_source, hardlink_path)
    try:
        rejected(
            "hardlink_manifest_rejected",
            _fixture_with_release_a_mutation(
                prepared,
                lambda attestation: bind_release_source(attestation, hardlink_path),
            ),
        )
    finally:
        hardlink_path.unlink()

    writable_path = hostile / "writable.json"
    atomic_write(writable_path, original_source.read_bytes(), mode=0o644)
    rejected(
        "writable_manifest_rejected",
        _fixture_with_release_a_mutation(
            prepared, lambda attestation: bind_release_source(attestation, writable_path)
        ),
    )

    toolchain_binding = prepared.value["variants"]["A"]["attestation"][
        "semantic_input_authority"
    ]["toolchain"]

    def collide_source_path(attestation: dict[str, Any], *, digest: str) -> None:
        binding = authority(attestation)["source"]
        binding["manifest_path"] = toolchain_binding["manifest_path"]
        binding["manifest_sha256"] = digest

    rejected(
        "path_alias_rejected",
        _fixture_with_release_a_mutation(
            prepared,
            lambda attestation: collide_source_path(
                attestation, digest=toolchain_binding["manifest_sha256"]
            ),
        ),
    )
    rejected(
        "path_digest_conflict_rejected",
        _fixture_with_release_a_mutation(
            prepared,
            lambda attestation: collide_source_path(
                attestation, digest=authority(attestation)["source"]["manifest_sha256"]
            ),
        ),
    )

    approval = _detached(prepared.source_approval_value)
    del approval["variants"]["C"]["current_lock_attempt"][
        "semantic_input_authority"
    ]
    rejected(
        "resolver_omission_rejected",
        replace(prepared, source_approval_value=approval),
    )
    approval = _detached(prepared.source_approval_value)
    approval["variants"]["C"]["lock_resolution"][
        "passed_file_descriptors"
    ] = 12
    rejected(
        "resolver_approval_review_divergence_rejected",
        replace(prepared, source_approval_value=approval),
    )
    approval = _detached(prepared.source_approval_value)
    approval["variants"]["A"]["current_lock_attempt"] = approval["variants"][
        "C"
    ]["current_lock_attempt"]
    rejected(
        "resolver_topology_rejected",
        replace(prepared, source_approval_value=approval),
    )

    def legacy_root(attestation: dict[str, Any]) -> None:
        attestation["build_argv"][1:1] = ["--ro-bind", "/", "/"]

    rejected(
        "legacy_root_bind_rejected",
        _fixture_with_release_a_mutation(prepared, legacy_root),
    )

    def legacy_dev(attestation: dict[str, Any]) -> None:
        attestation["build_argv"][1:1] = ["--dev", "/dev"]

    rejected(
        "legacy_dev_bind_rejected",
        _fixture_with_release_a_mutation(prepared, legacy_dev),
    )

    def legacy_proc(attestation: dict[str, Any]) -> None:
        attestation["build_argv"][1:1] = ["--proc", "/proc"]

    rejected(
        "legacy_proc_bind_rejected",
        _fixture_with_release_a_mutation(prepared, legacy_proc),
    )
    rejected(
        "release_passed_fd_count_drift_rejected",
        _fixture_with_release_a_mutation(
            prepared,
            lambda attestation: attestation["build_child"].__setitem__(
                "passed_file_descriptors", 15
            ),
        ),
    )
    rejected(
        "release_passed_fd_bool_rejected",
        _fixture_with_release_a_mutation(
            prepared,
            lambda attestation: attestation["build_child"].__setitem__(
                "passed_file_descriptors", True
            ),
        ),
    )

    def remove_release_dev_null(attestation: dict[str, Any]) -> None:
        argv = attestation["build_argv"]
        index = argv.index("--dev-bind")
        del argv[index : index + 3]

    rejected(
        "release_dev_null_omission_rejected",
        _fixture_with_release_a_mutation(prepared, remove_release_dev_null),
    )

    def move_release_dev_null(attestation: dict[str, Any]) -> None:
        argv = attestation["build_argv"]
        index = argv.index("--dev-bind")
        binding = argv[index : index + 3]
        del argv[index : index + 3]
        proc_target = argv.index("/proc")
        argv[proc_target + 1 : proc_target + 1] = binding

    rejected(
        "release_dev_null_position_rejected",
        _fixture_with_release_a_mutation(prepared, move_release_dev_null),
    )

    def redirect_release_rust_lld(attestation: dict[str, Any]) -> None:
        argv = attestation["build_argv"]
        destination = _semantic_rust_lld_guest(attestation["toolchain"])
        index = argv.index(destination)
        argv[index] = "/asterism/toolchain/bin/rust-lld"

    rejected(
        "release_rust_lld_destination_rejected",
        _fixture_with_release_a_mutation(prepared, redirect_release_rust_lld),
    )
    rejected(
        "release_execution_tools_omission_rejected",
        _fixture_with_release_a_mutation(
            prepared,
            lambda attestation: attestation["execution_tools"].pop("rust_lld"),
        ),
    )

    def drift_release_null_device(attestation: dict[str, Any]) -> None:
        attestation["execution_tools"]["dev_null"]["identity"]["minor"] = 4

    rejected(
        "release_null_device_identity_rejected",
        _fixture_with_release_a_mutation(prepared, drift_release_null_device),
    )
    rejected(
        "legacy_rustup_home_rejected",
        _fixture_with_release_a_mutation(
            prepared,
            lambda attestation: attestation["build_env"].__setitem__(
                "RUSTUP_HOME", "/asterism/rustup-home"
            ),
        ),
    )
    def reorder_release_bindings(attestation: dict[str, Any]) -> None:
        argv = attestation["build_argv"]
        starts = [
            index
            for index, argument in enumerate(argv)
            if argument in {"--ro-bind-fd", "--bind-fd"}
        ]
        first, second = starts[:2]
        argv[first : first + 3], argv[second : second + 3] = (
            argv[second : second + 3],
            argv[first : first + 3],
        )

    rejected(
        "release_bind_descriptor_order_rejected",
        _fixture_with_release_a_mutation(prepared, reorder_release_bindings),
    )

    def release_overlay_index(attestation: dict[str, Any]) -> int:
        indexes = [
            index
            for index, argument in enumerate(attestation["build_argv"])
            if argument == "--overlay-src"
        ]
        if len(indexes) != 1:
            raise AssertionError("release fixture overlay topology differs")
        return indexes[0]

    def reopen_release_overlay_by_host_path(attestation: dict[str, Any]) -> None:
        index = release_overlay_index(attestation)
        attestation["build_argv"][index + 1] = "/fixture/cargo-home"

    rejected(
        "release_overlay_host_path_fallback_rejected",
        _fixture_with_release_a_mutation(
            prepared, reopen_release_overlay_by_host_path
        ),
    )

    def restore_release_direct_root_bind(attestation: dict[str, Any]) -> None:
        index = release_overlay_index(attestation)
        descriptor = attestation["build_argv"][index + 1].removeprefix(
            SEMANTIC_CARGO_HOME_FD_PREFIX
        )
        attestation["build_argv"][index : index + 4] = [
            "--ro-bind-fd",
            descriptor,
            SEMANTIC_CARGO_HOME,
        ]

    rejected(
        "release_legacy_direct_cargo_home_bind_rejected",
        _fixture_with_release_a_mutation(
            prepared, restore_release_direct_root_bind
        ),
    )

    def insert_release_lower_bind(attestation: dict[str, Any]) -> None:
        index = release_overlay_index(attestation)
        descriptor = attestation["build_argv"][index + 1].removeprefix(
            SEMANTIC_CARGO_HOME_FD_PREFIX
        )
        attestation["build_argv"][index:index] = [
            "--ro-bind-fd",
            descriptor,
            "/asterism/cargo-home-lower",
        ]

    rejected(
        "release_cargo_home_lower_path_rejected",
        _fixture_with_release_a_mutation(prepared, insert_release_lower_bind),
    )

    def alias_release_overlay_descriptor(attestation: dict[str, Any]) -> None:
        argv = attestation["build_argv"]
        first_binding = next(
            index
            for index, argument in enumerate(argv)
            if argument in SEMANTIC_DESCRIPTOR_OPTIONS
        )
        overlay = release_overlay_index(attestation)
        argv[overlay + 1] = (
            f"{SEMANTIC_CARGO_HOME_FD_PREFIX}{argv[first_binding + 1]}"
        )

    rejected(
        "release_overlay_descriptor_alias_rejected",
        _fixture_with_release_a_mutation(
            prepared, alias_release_overlay_descriptor
        ),
    )

    def move_release_overlay_after_configs(attestation: dict[str, Any]) -> None:
        argv = attestation["build_argv"]
        index = release_overlay_index(attestation)
        overlay = argv[index : index + 4]
        del argv[index : index + 4]
        remount = next(
            candidate
            for candidate in range(len(argv) - 1)
            if argv[candidate : candidate + 2]
            == ["--remount-ro", SEMANTIC_CARGO_HOME]
        )
        argv[remount:remount] = overlay

    rejected(
        "release_overlay_after_configs_rejected",
        _fixture_with_release_a_mutation(
            prepared, move_release_overlay_after_configs
        ),
    )

    def remove_release_cargo_home_remount(attestation: dict[str, Any]) -> None:
        argv = attestation["build_argv"]
        remount = next(
            index
            for index in range(len(argv) - 1)
            if argv[index : index + 2]
            == ["--remount-ro", SEMANTIC_CARGO_HOME]
        )
        del argv[remount : remount + 2]

    rejected(
        "release_cargo_home_remount_omission_rejected",
        _fixture_with_release_a_mutation(
            prepared, remove_release_cargo_home_remount
        ),
    )

    release_attestation = prepared.value["variants"]["A"]["attestation"]

    rejected(
        "release_loader_origin_missing_rejected",
        _fixture_with_release_a_mutation(
            prepared,
            lambda attestation: attestation["build_env"].pop("LD_ORIGIN_PATH"),
        ),
    )
    rejected(
        "release_loader_origin_host_path_rejected",
        _fixture_with_release_a_mutation(
            prepared,
            lambda attestation: attestation["build_env"].__setitem__(
                "LD_ORIGIN_PATH", "/host/toolchain/bin"
            ),
        ),
    )

    def release_sandbox_digest(argv: list[str]) -> str:
        release_toolchain = release_attestation["toolchain"]
        return _validate_semantic_sandbox(
            argv,
            release_attestation["build_env"],
            "release normalization self-test",
            passed_file_descriptors=None,
            expected_passed_file_descriptors=None,
            require_loader_origin=True,
            require_passed_file_descriptors=False,
            expected_fd_destinations=_release_build_destinations(
                release_toolchain
            ),
            writable_fd_destinations={"/asterism/target"},
            expected_descriptor_bindings=_release_build_descriptor_bindings(
                release_toolchain
            ),
            execution_tools=release_attestation["execution_tools"],
            execution_tools_kind="prepared",
            toolchain=release_toolchain,
            cargo_config_search_sha256=release_attestation[
                "cargo_config_search"
            ]["sha256"],
            semantic_runtime_sha256=release_attestation[
                "semantic_input_authority"
            ]["runtime_sha256"],
            prepared_authority_canonicalizer=(
                prepared.schema.prepared_authority_canonical_json_bytes
            ),
        )

    original_release_argv = _detached(release_attestation["build_argv"])
    shifted_release_argv = _detached(original_release_argv)
    for index, argument in enumerate(shifted_release_argv):
        if argument in SEMANTIC_DESCRIPTOR_OPTIONS:
            shifted_release_argv[index + 1] = str(
                int(shifted_release_argv[index + 1]) + 100
            )
        elif argument == "--dev-bind":
            descriptor = shifted_release_argv[index + 1].removeprefix(
                SEMANTIC_CARGO_HOME_FD_PREFIX
            )
            shifted_release_argv[index + 1] = (
                f"{SEMANTIC_CARGO_HOME_FD_PREFIX}{int(descriptor) + 100}"
            )
        elif argument == "--overlay-src":
            descriptor = shifted_release_argv[index + 1].removeprefix(
                SEMANTIC_CARGO_HOME_FD_PREFIX
            )
            shifted_release_argv[index + 1] = (
                f"{SEMANTIC_CARGO_HOME_FD_PREFIX}{int(descriptor) + 100}"
            )
    checks["release_overlay_fd_normalization_stable"] = (
        release_sandbox_digest(original_release_argv)
        == release_sandbox_digest(shifted_release_argv)
    )
    details["release_overlay_fd_normalization_stable"] = (
        "descriptor-renumbered sandbox digest is stable"
        if checks["release_overlay_fd_normalization_stable"]
        else "descriptor-renumbered sandbox digest differs"
    )

    resolver_record = prepared.source_approval_value["variants"]["C"][
        "current_lock_attempt"
    ]
    try:
        _validate_semantic_sandbox(
            resolver_record["argv"],
            resolver_record["environment"],
            "resolver hostile float descriptor count",
            passed_file_descriptors=13.0,
            expected_passed_file_descriptors=13,
            require_loader_origin=False,
            expected_fd_destinations=RESOLVER_DESTINATIONS,
            writable_fd_destinations={"/asterism/source"},
            expected_descriptor_bindings=RESOLVER_DESCRIPTOR_BINDINGS,
        )
    except RunnerFailure as error:
        checks["resolver_passed_fd_float_rejected"] = True
        details["resolver_passed_fd_float_rejected"] = error.reason
    else:
        checks["resolver_passed_fd_float_rejected"] = False
        details["resolver_passed_fd_float_rejected"] = (
            "resolver float descriptor count was accepted"
        )

    hostile_resolver_argv = _detached(resolver_record["argv"])
    resolver_overlay = hostile_resolver_argv.index("--overlay-src")
    hostile_resolver_argv[resolver_overlay + 1] = "/fixture/cargo-home"
    try:
        _validate_semantic_sandbox(
            hostile_resolver_argv,
            resolver_record["environment"],
            "resolver hostile host fallback",
            passed_file_descriptors=resolver_record[
                "passed_file_descriptors"
            ],
            expected_passed_file_descriptors=13,
            require_loader_origin=False,
            expected_fd_destinations=RESOLVER_DESTINATIONS,
            writable_fd_destinations={"/asterism/source"},
            expected_descriptor_bindings=RESOLVER_DESCRIPTOR_BINDINGS,
        )
    except RunnerFailure as error:
        checks["resolver_overlay_host_path_fallback_rejected"] = True
        details["resolver_overlay_host_path_fallback_rejected"] = error.reason
    else:
        checks["resolver_overlay_host_path_fallback_rejected"] = False
        details["resolver_overlay_host_path_fallback_rejected"] = (
            "resolver host path fallback was accepted"
        )

    hostile_resolver_environment = dict(resolver_record["environment"])
    hostile_resolver_environment["LD_ORIGIN_PATH"] = SEMANTIC_TOOLCHAIN_BIN
    try:
        _validate_semantic_sandbox(
            resolver_record["argv"],
            hostile_resolver_environment,
            "resolver hostile loader origin",
            passed_file_descriptors=resolver_record["passed_file_descriptors"],
            expected_passed_file_descriptors=13,
            require_loader_origin=False,
            expected_fd_destinations=RESOLVER_DESTINATIONS,
            writable_fd_destinations={"/asterism/source"},
            expected_descriptor_bindings=RESOLVER_DESCRIPTOR_BINDINGS,
        )
    except RunnerFailure as error:
        checks["resolver_loader_origin_rejected"] = True
        details["resolver_loader_origin_rejected"] = error.reason
    else:
        checks["resolver_loader_origin_rejected"] = False
        details["resolver_loader_origin_rejected"] = (
            "resolver loader origin was accepted"
        )

    hostile_resolver_dev = _detached(resolver_record["argv"])
    hostile_resolver_dev[5:5] = [
        "--dev-bind",
        f"{SEMANTIC_CARGO_HOME_FD_PREFIX}999",
        "/dev/null",
    ]
    try:
        _validate_semantic_sandbox(
            hostile_resolver_dev,
            resolver_record["environment"],
            "resolver hostile device bind",
            passed_file_descriptors=resolver_record["passed_file_descriptors"],
            expected_passed_file_descriptors=13,
            require_loader_origin=False,
            expected_fd_destinations=RESOLVER_DESTINATIONS,
            writable_fd_destinations={"/asterism/source"},
            expected_descriptor_bindings=RESOLVER_DESCRIPTOR_BINDINGS,
        )
    except RunnerFailure as error:
        checks["resolver_device_bind_rejected"] = True
        details["resolver_device_bind_rejected"] = error.reason
    else:
        checks["resolver_device_bind_rejected"] = False
        details["resolver_device_bind_rejected"] = (
            "resolver device binding was accepted"
        )

    hostile_resolver_lld = _detached(resolver_record["argv"])
    rustc_index = hostile_resolver_lld.index(
        "/asterism/toolchain/bin/rustc"
    )
    hostile_resolver_lld[rustc_index + 1 : rustc_index + 1] = [
        "--ro-bind-fd",
        "998",
        "/asterism/toolchain/lib/rustlib/host/bin/gcc-ld/ld.lld",
    ]
    try:
        _validate_semantic_sandbox(
            hostile_resolver_lld,
            resolver_record["environment"],
            "resolver hostile rust-lld bind",
            passed_file_descriptors=resolver_record["passed_file_descriptors"],
            expected_passed_file_descriptors=13,
            require_loader_origin=False,
            expected_fd_destinations=RESOLVER_DESTINATIONS,
            writable_fd_destinations={"/asterism/source"},
            expected_descriptor_bindings=RESOLVER_DESCRIPTOR_BINDINGS,
        )
    except RunnerFailure as error:
        checks["resolver_rust_lld_bind_rejected"] = True
        details["resolver_rust_lld_bind_rejected"] = error.reason
    else:
        checks["resolver_rust_lld_bind_rejected"] = False
        details["resolver_rust_lld_bind_rejected"] = (
            "resolver rust-lld binding was accepted"
        )

    post_path = hostile / "post-start.json"
    atomic_write(post_path, original_source.read_bytes(), mode=0o444)
    post_prepared = _fixture_with_release_a_mutation(
        prepared, lambda attestation: bind_release_source(attestation, post_path)
    )
    post_snapshots, _post_counts = retain_prepared_semantic_manifests(post_prepared)
    try:
        post_path.chmod(0o644)
        post_path.write_bytes(b"{}\n")
        try:
            for snapshot in post_snapshots:
                snapshot.verify()
        except RunnerFailure as error:
            checks["post_start_mutation_rejected"] = True
            details["post_start_mutation_rejected"] = error.reason
        else:
            checks["post_start_mutation_rejected"] = False
            details["post_start_mutation_rejected"] = "post-start chmod was accepted"
    finally:
        post_path.chmod(0o444)
        for snapshot in post_snapshots:
            snapshot.close()

    initial_root = hostile / "initial-retention-transient"
    initial_root.mkdir()
    initial_ancestor = initial_root / "live"
    initial_ancestor.mkdir()
    initial_path = initial_ancestor / "manifest.json"
    atomic_write(initial_path, original_source.read_bytes(), mode=0o444)
    initial_away = initial_root / "away"
    original_retained_read = globals()["_read_retained_file"]
    initial_swapped = False

    def initial_transient_swap_restore(descriptor: int) -> bytes:
        nonlocal initial_swapped
        payload = original_retained_read(descriptor)
        if not initial_swapped:
            initial_ancestor.rename(initial_away)
            initial_away.rename(initial_ancestor)
            initial_swapped = True
        return payload

    globals()["_read_retained_file"] = initial_transient_swap_restore
    initial_snapshot: RetainedSemanticManifest | None = None
    try:
        initial_snapshot = _retain_semantic_manifest(
            str(initial_path),
            sha256(initial_path),
            RECURSIVE_TREE_AUTHORITY_SCHEMA,
            "hostile initial retention transient swap",
        )
    except RunnerFailure as error:
        # Rename-away-and-restore returns the ancestor to the *same* inode with
        # the same content, which is not a substitution; substitution-only
        # ancestor identity tolerates it (genuine substitution is covered by
        # ancestor_symlink_swap_rejected and path_swap_during_recheck_rejected).
        checks["initial_retention_transient_ancestor_restore_tolerated"] = False
        details["initial_retention_transient_ancestor_restore_tolerated"] = (
            f"identical-inode ancestor restore was rejected: {error.reason}"
        )
    else:
        checks["initial_retention_transient_ancestor_restore_tolerated"] = (
            initial_swapped
        )
        details["initial_retention_transient_ancestor_restore_tolerated"] = (
            "identical-inode ancestor restore tolerated"
            if initial_swapped
            else "swap probe did not fire"
        )
    finally:
        globals()["_read_retained_file"] = original_retained_read
        if initial_snapshot is not None:
            initial_snapshot.close()

    ancestor_root = hostile / "ancestor-live"
    ancestor_root.mkdir()
    ancestor_path = ancestor_root / "manifest.json"
    atomic_write(ancestor_path, original_source.read_bytes(), mode=0o444)
    ancestor_snapshot = _retain_semantic_manifest(
        str(ancestor_path),
        sha256(ancestor_path),
        RECURSIVE_TREE_AUTHORITY_SCHEMA,
        "hostile ancestor swap",
    )
    ancestor_target = hostile / "ancestor-target"
    ancestor_root.rename(ancestor_target)
    ancestor_root.symlink_to(ancestor_target, target_is_directory=True)
    try:
        ancestor_snapshot.verify()
    except RunnerFailure as error:
        checks["ancestor_symlink_swap_rejected"] = True
        details["ancestor_symlink_swap_rejected"] = error.reason
    else:
        checks["ancestor_symlink_swap_rejected"] = False
        details["ancestor_symlink_swap_rejected"] = "ancestor symlink was followed"
    finally:
        ancestor_snapshot.close()

    gap_root = hostile / "path-gap"
    gap_root.mkdir()
    gap_path = gap_root / "manifest.json"
    gap_replacement = gap_root / "replacement.json"
    gap_retired = gap_root / "retired.json"
    gap_payload = original_source.read_bytes()
    atomic_write(gap_path, gap_payload, mode=0o444)
    atomic_write(gap_replacement, gap_payload, mode=0o444)
    gap_snapshot = _retain_semantic_manifest(
        str(gap_path),
        sha256(gap_path),
        RECURSIVE_TREE_AUTHORITY_SCHEMA,
        "hostile path recheck gap",
    )
    original_retained_hash = globals()["_sha256_retained_file"]
    swapped_during_hash = False

    def swap_after_retained_hash(descriptor: int) -> str:
        nonlocal swapped_during_hash
        digest = original_retained_hash(descriptor)
        if descriptor == gap_snapshot.descriptor and not swapped_during_hash:
            gap_path.rename(gap_retired)
            gap_replacement.rename(gap_path)
            swapped_during_hash = True
        return digest

    globals()["_sha256_retained_file"] = swap_after_retained_hash
    try:
        gap_snapshot.verify()
    except RunnerFailure as error:
        checks["path_swap_during_recheck_rejected"] = swapped_during_hash
        details["path_swap_during_recheck_rejected"] = error.reason
    else:
        checks["path_swap_during_recheck_rejected"] = False
        details["path_swap_during_recheck_rejected"] = (
            "post-hash lexical path replacement was accepted"
        )
    finally:
        globals()["_sha256_retained_file"] = original_retained_hash
        gap_snapshot.close()

    transient_root = hostile / "transient-ancestor"
    transient_root.mkdir()
    transient_ancestor = transient_root / "live"
    transient_ancestor.mkdir()
    transient_path = transient_ancestor / "manifest.json"
    atomic_write(transient_path, gap_payload, mode=0o444)
    transient_snapshot = _retain_semantic_manifest(
        str(transient_path),
        sha256(transient_path),
        RECURSIVE_TREE_AUTHORITY_SCHEMA,
        "hostile transient ancestor swap",
    )
    transient_away = transient_root / "away"
    original_retained_hash = globals()["_sha256_retained_file"]
    transient_swapped = False

    def transient_swap_restore_after_hash(descriptor: int) -> str:
        nonlocal transient_swapped
        digest = original_retained_hash(descriptor)
        if descriptor == transient_snapshot.descriptor and not transient_swapped:
            transient_ancestor.rename(transient_away)
            transient_away.rename(transient_ancestor)
            transient_swapped = True
        return digest

    globals()["_sha256_retained_file"] = transient_swap_restore_after_hash
    try:
        transient_snapshot.verify()
    except RunnerFailure as error:
        # Same-inode rename-away/restore during the recheck is not a
        # substitution and is tolerated under substitution-only identity.
        checks["transient_ancestor_restore_tolerated"] = False
        details["transient_ancestor_restore_tolerated"] = (
            f"identical-inode ancestor restore was rejected: {error.reason}"
        )
    else:
        checks["transient_ancestor_restore_tolerated"] = transient_swapped
        details["transient_ancestor_restore_tolerated"] = (
            "identical-inode ancestor restore tolerated"
            if transient_swapped
            else "swap probe did not fire"
        )
    finally:
        globals()["_sha256_retained_file"] = original_retained_hash
        transient_snapshot.close()

    reviewed_root = hostile / "reviewed-transient"
    reviewed_root.mkdir()
    reviewed_ancestor = reviewed_root / "live"
    reviewed_ancestor.mkdir()
    reviewed_path = reviewed_ancestor / "authority.json"
    atomic_json(reviewed_path, {"schema": "hostile-reviewed-authority-v1"})
    reviewed_path.chmod(0o444)
    reviewed_support = SupportFile(
        "hostile-reviewed-authority",
        reviewed_path,
        sha256(reviewed_path),
        0o444,
    )
    reviewed_away = reviewed_root / "away"
    original_retained_read = globals()["_read_retained_file"]
    reviewed_swapped = False

    def reviewed_transient_swap_restore(descriptor: int) -> bytes:
        nonlocal reviewed_swapped
        payload = original_retained_read(descriptor)
        if not reviewed_swapped:
            reviewed_ancestor.rename(reviewed_away)
            reviewed_away.rename(reviewed_ancestor)
            reviewed_swapped = True
        return payload

    globals()["_read_retained_file"] = reviewed_transient_swap_restore
    try:
        _load_canonical_support_authority(
            reviewed_support,
            "hostile reviewed transient swap",
        )
    except RunnerFailure as error:
        # Same-inode rename-away/restore of a reviewed-authority ancestor is
        # not a substitution and is tolerated under substitution-only identity.
        checks["reviewed_authority_transient_ancestor_restore_tolerated"] = False
        details["reviewed_authority_transient_ancestor_restore_tolerated"] = (
            f"identical-inode ancestor restore was rejected: {error.reason}"
        )
    else:
        checks["reviewed_authority_transient_ancestor_restore_tolerated"] = (
            reviewed_swapped
        )
        details["reviewed_authority_transient_ancestor_restore_tolerated"] = (
            "identical-inode ancestor restore tolerated"
            if reviewed_swapped
            else "swap probe did not fire"
        )
    finally:
        globals()["_read_retained_file"] = original_retained_read

    constructor_snapshots: list[RetainedSemanticManifest] = []
    constructor_secondary: list[str] = []
    constructor_mutated_path: Path | None = None
    constructor_original_payload: bytes | None = None

    class FailingSemanticRunner(RebaselineRunner):
        def _initialize_after_semantic_retention(self) -> None:
            nonlocal constructor_mutated_path, constructor_original_payload
            constructor_snapshots.extend(self.semantic_manifests)
            constructor_mutated_path = self.semantic_manifests[0].path
            constructor_original_payload = constructor_mutated_path.read_bytes()
            constructor_mutated_path.chmod(0o644)
            constructor_mutated_path.write_bytes(b"{}\n")
            raise RunnerFailure("synthetic constructor failure")

        def _report_secondary_failure(
            self,
            context: str,
            error: BaseException,
        ) -> None:
            constructor_secondary.append(f"{context}: {error}")

    try:
        FailingSemanticRunner(
            prepared,
            root / "unused-constructor-output",
            object(),  # type: ignore[arg-type]
            scratch_root=root / "unused-constructor-scratch",
        )
    except RunnerFailure as error:
        notes = list(getattr(error, "__notes__", ()))
        checks["constructor_failure_closes_all_48"] = (
            error.reason == "synthetic constructor failure"
            and len(constructor_snapshots) == SEMANTIC_MANIFEST_TOTAL
            and all(snapshot.descriptor == -1 for snapshot in constructor_snapshots)
            and any("final verification" in note for note in notes)
            and any("retained evidence" in item for item in constructor_secondary)
        )
        details["constructor_failure_closes_all_48"] = (
            f"primary={error.reason!r} notes={notes!r} "
            f"secondary={constructor_secondary!r}"
        )
    else:
        checks["constructor_failure_closes_all_48"] = False
        details["constructor_failure_closes_all_48"] = (
            "synthetic constructor failure was not raised"
        )
    finally:
        if constructor_mutated_path is not None and constructor_original_payload is not None:
            constructor_mutated_path.write_bytes(constructor_original_payload)
            constructor_mutated_path.chmod(0o444)

    original_retain_manifest = globals()["_retain_semantic_manifest"]
    partial_snapshots: list[RetainedSemanticManifest] = []
    partial_original_payload: bytes | None = None
    partial_calls = 0

    def fail_after_mutating_partial_retention(
        raw_path: Any,
        expected_sha256: Any,
        expected_schema: str,
        context: str,
        *,
        canonicalizer: Callable[[Any], bytes] = canonical_json_bytes,
    ) -> RetainedSemanticManifest:
        nonlocal partial_calls, partial_original_payload
        partial_calls += 1
        if partial_calls == 2:
            raise RunnerFailure("synthetic partial retention failure", exit_code=26)
        snapshot = original_retain_manifest(
            raw_path,
            expected_sha256,
            expected_schema,
            context,
            canonicalizer=canonicalizer,
        )
        if partial_calls == 1:
            partial_snapshots.append(snapshot)
            partial_original_payload = snapshot.path.read_bytes()
            snapshot.path.chmod(0o644)
            snapshot.path.write_bytes(b"{}\n")
        return snapshot

    globals()["_retain_semantic_manifest"] = fail_after_mutating_partial_retention
    try:
        retain_prepared_semantic_manifests(prepared)
    except RunnerFailure as error:
        notes = list(getattr(error, "__notes__", ()))
        checks["partial_retention_failure_final_verifies_and_closes"] = (
            error.reason == "synthetic partial retention failure"
            and error.exit_code == 26
            and len(partial_snapshots) == 1
            and partial_snapshots[0].descriptor == -1
            and any("final verification" in note for note in notes)
        )
        details["partial_retention_failure_final_verifies_and_closes"] = (
            f"primary={error.reason!r} exit={error.exit_code} notes={notes!r}"
        )
    else:
        checks["partial_retention_failure_final_verifies_and_closes"] = False
        details["partial_retention_failure_final_verifies_and_closes"] = (
            "synthetic partial retention failure was accepted"
        )
    finally:
        globals()["_retain_semantic_manifest"] = original_retain_manifest
        if partial_snapshots and partial_original_payload is not None:
            partial_snapshots[0].path.write_bytes(partial_original_payload)
            partial_snapshots[0].path.chmod(0o444)
            partial_snapshots[0].close()

    early_output = root / "early-output-that-must-remain-absent"
    publication_runner = object.__new__(RebaselineRunner)
    publication_runner.output = early_output
    publication_attempts: list[str] = []

    def publication_failure(label: str) -> NoReturn:
        publication_attempts.append(label)
        raise OSError(f"synthetic {label} failure")

    publication_runner.write_failure = lambda _failure: publication_failure("write")
    publication_runner.release_lease = lambda _outcome: publication_failure("lease")
    publication_runner.log = lambda _message: publication_failure("log")
    publication_runner._report_secondary_failure = (
        lambda context, _error: publication_attempts.append(f"reported:{context}")
    )
    publication_runner._emit_primary_failure = (
        lambda failure: publication_attempts.append(f"fallback:{failure.reason}")
    )
    early_failure = RunnerFailure("synthetic early pre-output failure", exit_code=24)
    early_code = publication_runner._publish_runner_failure(early_failure)
    checks["early_pre_output_secondary_failures_preserve_primary"] = (
        not early_output.exists()
        and early_failure.reason == "synthetic early pre-output failure"
        and early_failure.exit_code == 24
        and early_code == 24
        and publication_attempts
        == [
            "write",
            "reported:failure artifact publication",
            "lease",
            "reported:inconclusive lease release",
            "log",
            "reported:failure log publication",
            "fallback:synthetic early pre-output failure",
        ]
    )
    details["early_pre_output_secondary_failures_preserve_primary"] = (
        f"reason={early_failure.reason!r} exit={early_code} "
        f"attempts={publication_attempts!r}"
    )

    close_path = hostile / "close-idempotence.json"
    atomic_write(close_path, gap_payload, mode=0o444)
    close_snapshot = _retain_semantic_manifest(
        str(close_path),
        sha256(close_path),
        RECURSIVE_TREE_AUTHORITY_SCHEMA,
        "hostile close failure",
    )
    close_descriptor = close_snapshot.descriptor
    original_os_close = os.close
    close_attempts = 0

    def failing_os_close(descriptor: int) -> None:
        nonlocal close_attempts
        if descriptor == close_descriptor:
            close_attempts += 1
            raise OSError("synthetic close failure")
        original_os_close(descriptor)

    os.close = failing_os_close
    first_close_failed = False
    try:
        try:
            close_snapshot.close()
        except OSError:
            first_close_failed = True
        close_snapshot.close()
    finally:
        os.close = original_os_close
        original_os_close(close_descriptor)
    checks["descriptor_close_invalidates_before_error"] = (
        first_close_failed
        and close_attempts == 1
        and close_snapshot.descriptor == -1
    )
    details["descriptor_close_invalidates_before_error"] = (
        f"first_failed={first_close_failed} attempts={close_attempts} "
        f"descriptor={close_snapshot.descriptor}"
    )

    second_close_runner = object.__new__(RebaselineRunner)
    secondary_cleanup: list[str] = []
    second_close_runner._run_with_retained_semantic_authority = lambda: 25
    second_close_runner.close_semantic_manifests = lambda: publication_failure(
        "second-close"
    )
    second_close_runner._report_secondary_failure = (
        lambda context, _error: secondary_cleanup.append(context)
    )
    second_close_code = second_close_runner.run()
    checks["second_close_failure_does_not_override_exit"] = (
        second_close_code == 25
        and publication_attempts[-1] == "second-close"
        and secondary_cleanup == ["final semantic cleanup"]
    )
    details["second_close_failure_does_not_override_exit"] = (
        f"exit={second_close_code} cleanup={secondary_cleanup!r}"
    )

    boundary_snapshots, boundary_counts = retain_prepared_semantic_manifests(prepared)
    original_source_payload = original_source.read_bytes()
    original_source.chmod(0o644)
    original_source.write_bytes(b"{}\n")
    boundary_runner = object.__new__(RebaselineRunner)
    boundary_runner.semantic_manifests = boundary_snapshots
    boundary_runner.semantic_manifest_counts = boundary_counts
    primary = RunnerFailure("synthetic primary failure", exit_code=23)
    try:
        combined = boundary_runner.finish_semantic_lifetime(primary)
        checks["final_boundary_rechecks_closes_and_preserves_primary"] = (
            isinstance(combined, RunnerFailure)
            and "synthetic primary failure" in combined.reason
            and "semantic lifetime boundary failed" in combined.reason
            and combined.exit_code == 23
            and all(snapshot.descriptor == -1 for snapshot in boundary_snapshots)
        )
        details["final_boundary_rechecks_closes_and_preserves_primary"] = (
            combined.reason if combined is not None else "boundary failure was absent"
        )
    finally:
        original_source.write_bytes(original_source_payload)
        original_source.chmod(0o444)
        for snapshot in boundary_snapshots:
            snapshot.close()

    current = _detached(
        load_reviewed_authority_json(
            prepared.source_review_files["current_children_attestation"].path
        )
    )
    preserved_current = _detached(current)
    preserved_build = preserved_current["builds"]["children"]
    preserved_build["cargo_config_prebuild"] = _fixture_current_cargo_config(
        (_fixture_preserved_cargo_home_directory("registry", 101),)
    )
    preserved_build["execution"]["passed_file_descriptors"] += 1
    preserved_path = hostile / "current-preserved-cargo-home.json"
    atomic_json(preserved_path, preserved_current)
    preserved_path.chmod(0o444)
    source_review_files = dict(prepared.source_review_files)
    source_review_files["current_children_attestation"] = SupportFile(
        "prepared-source-review-current_children_attestation",
        preserved_path,
        sha256(preserved_path),
        0o444,
    )
    preserved_snapshots: list[RetainedSemanticManifest] = []
    try:
        preserved_snapshots, preserved_counts = retain_prepared_semantic_manifests(
            replace(prepared, source_review_files=source_review_files)
        )
        checks["current_child_preserved_cargo_home_fd_count_accepted"] = (
            preserved_counts == SEMANTIC_MANIFEST_COUNTS
            and len(preserved_snapshots) == SEMANTIC_MANIFEST_TOTAL
        )
        details["current_child_preserved_cargo_home_fd_count_accepted"] = (
            f"passed={preserved_build['execution']['passed_file_descriptors']}"
        )
    except RunnerFailure as error:
        checks["current_child_preserved_cargo_home_fd_count_accepted"] = False
        details["current_child_preserved_cargo_home_fd_count_accepted"] = error.reason
    finally:
        for snapshot in preserved_snapshots:
            snapshot.close()
    checks["current_dynamic_fd_formula_base_18_20_plus_preserved"] = (
        checks["current_child_preserved_cargo_home_fd_count_accepted"]
        and preserved_build["execution"]["passed_file_descriptors"] == 21
    )
    details["current_dynamic_fd_formula_base_18_20_plus_preserved"] = (
        "release_base=18 child_base=20 preserved_children=1 child_passed=21"
    )

    count_drift_current = _detached(current)
    count_drift_current["builds"]["children"]["execution"][
        "passed_file_descriptors"
    ] += 1
    count_drift_path = hostile / "current-passed-fd-count-drift.json"
    atomic_json(count_drift_path, count_drift_current)
    count_drift_path.chmod(0o444)
    source_review_files = dict(prepared.source_review_files)
    source_review_files["current_children_attestation"] = SupportFile(
        "prepared-source-review-current_children_attestation",
        count_drift_path,
        sha256(count_drift_path),
        0o444,
    )
    rejected(
        "current_child_exact_passed_fd_count_rejected",
        replace(prepared, source_review_files=source_review_files),
    )

    bool_count_current = _detached(current)
    bool_count_current["builds"]["children"]["execution"][
        "passed_file_descriptors"
    ] = True
    bool_count_path = hostile / "current-passed-fd-bool.json"
    atomic_json(bool_count_path, bool_count_current)
    bool_count_path.chmod(0o444)
    source_review_files = dict(prepared.source_review_files)
    source_review_files["current_children_attestation"] = SupportFile(
        "prepared-source-review-current_children_attestation",
        bool_count_path,
        sha256(bool_count_path),
        0o444,
    )
    rejected(
        "current_child_passed_fd_bool_rejected",
        replace(prepared, source_review_files=source_review_files),
    )

    float_count_current = _detached(current)
    float_count_current["builds"]["children"]["execution"][
        "passed_file_descriptors"
    ] = 17.0
    float_count_path = hostile / "current-passed-fd-float.json"
    atomic_json(float_count_path, float_count_current)
    float_count_path.chmod(0o444)
    source_review_files = dict(prepared.source_review_files)
    source_review_files["current_children_attestation"] = SupportFile(
        "prepared-source-review-current_children_attestation",
        float_count_path,
        sha256(float_count_path),
        0o444,
    )
    rejected(
        "current_child_passed_fd_float_rejected",
        replace(prepared, source_review_files=source_review_files),
    )

    identity_drift_current = _detached(current)
    identity_drift_current["toolchain_identities"].pop()
    identity_drift_path = hostile / "current-toolchain-identities-drift.json"
    atomic_json(identity_drift_path, identity_drift_current)
    identity_drift_path.chmod(0o444)
    source_review_files = dict(prepared.source_review_files)
    source_review_files["current_children_attestation"] = SupportFile(
        "prepared-source-review-current_children_attestation",
        identity_drift_path,
        sha256(identity_drift_path),
        0o444,
    )
    rejected(
        "current_six_toolchain_identities_rejected",
        replace(prepared, source_review_files=source_review_files),
    )

    current_null_drift = _detached(current)
    current_null_drift["builds"]["children"]["execution_tools"]["dev_null"][
        "identity"
    ]["minor"] = 4
    current_null_path = hostile / "current-null-device-drift.json"
    atomic_json(current_null_path, current_null_drift)
    current_null_path.chmod(0o444)
    source_review_files = dict(prepared.source_review_files)
    source_review_files["current_children_attestation"] = SupportFile(
        "prepared-source-review-current_children_attestation",
        current_null_path,
        sha256(current_null_path),
        0o444,
    )
    rejected(
        "current_null_device_identity_rejected",
        replace(prepared, source_review_files=source_review_files),
    )

    current_null_position = _detached(current)
    current_null_argv = current_null_position["builds"]["children"]["argv"]
    current_null_index = current_null_argv.index("--dev-bind")
    current_null_binding = current_null_argv[
        current_null_index : current_null_index + 3
    ]
    del current_null_argv[current_null_index : current_null_index + 3]
    current_proc_target = current_null_argv.index("/proc")
    current_null_argv[
        current_proc_target + 1 : current_proc_target + 1
    ] = current_null_binding
    current_null_position_path = hostile / "current-null-device-position.json"
    atomic_json(current_null_position_path, current_null_position)
    current_null_position_path.chmod(0o444)
    source_review_files = dict(prepared.source_review_files)
    source_review_files["current_children_attestation"] = SupportFile(
        "prepared-source-review-current_children_attestation",
        current_null_position_path,
        sha256(current_null_position_path),
        0o444,
    )
    rejected(
        "current_null_device_position_rejected",
        replace(prepared, source_review_files=source_review_files),
    )

    current_python_drift = _detached(current)
    current_python_drift["builds"]["children"]["execution_tools"]["python"] = (
        _detached(
            current_python_drift["builds"]["children"]["execution_tools"][
                "bwrap"
            ]
        )
    )
    current_python_path = hostile / "current-python-self-authority.json"
    atomic_json(current_python_path, current_python_drift)
    current_python_path.chmod(0o444)
    source_review_files = dict(prepared.source_review_files)
    source_review_files["current_children_attestation"] = SupportFile(
        "prepared-source-review-current_children_attestation",
        current_python_path,
        sha256(current_python_path),
        0o444,
    )
    rejected(
        "current_python_exact_usr_bin_python3_rejected",
        replace(prepared, source_review_files=source_review_files),
    )

    malformed_config_current = _detached(current)
    malformed_config_current["builds"]["children"]["cargo_config_prebuild"][
        "preserved_top_level_entries"
    ]["cargo-home"] = {}
    malformed_config_path = hostile / "current-malformed-cargo-config.json"
    atomic_json(malformed_config_path, malformed_config_current)
    malformed_config_path.chmod(0o444)
    source_review_files = dict(prepared.source_review_files)
    source_review_files["current_children_attestation"] = SupportFile(
        "prepared-source-review-current_children_attestation",
        malformed_config_path,
        sha256(malformed_config_path),
        0o444,
    )
    rejected(
        "current_child_malformed_cargo_config_rejected",
        replace(prepared, source_review_files=source_review_files),
    )

    current["schema"] = "bn-30fs-current-children-build-v1"
    legacy_current_path = hostile / "legacy-current-children.json"
    atomic_json(legacy_current_path, current)
    legacy_current_path.chmod(0o444)
    source_review_files = dict(prepared.source_review_files)
    source_review_files["current_children_attestation"] = SupportFile(
        "prepared-source-review-current_children_attestation",
        legacy_current_path,
        sha256(legacy_current_path),
        0o444,
    )
    rejected(
        "legacy_current_child_v1_rejected",
        replace(prepared, source_review_files=source_review_files),
    )
    return checks, details


def run_semantic_authority_self_test(root: Path) -> int:
    if root.exists() or root.is_symlink():
        print(f"refusing non-fresh semantic self-test directory {root}", file=sys.stderr)
        return 2
    root.mkdir(parents=True)
    prepared = _fixture_prepared(root, Path(sys.executable).resolve())
    checks, details = _semantic_authority_static_checks(root, prepared)
    report = {
        "schema": "bn-32de-runner-semantic-authority-self-test-v1",
        "outcome": "SELF_TEST_PASS" if all(checks.values()) else "SELF_TEST_FAILED",
        "checks": checks,
        "details": details,
        "check_count": len(checks),
    }
    atomic_json(root / "semantic-authority-self-test.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if all(checks.values()) else 30


def run_self_test(root: Path) -> int:
    if root.exists() or root.is_symlink():
        print(f"refusing non-fresh self-test directory {root}", file=sys.stderr)
        return 2
    root.mkdir(parents=True)
    set_process_comm(RUNNER_COMM)
    fake_child = root / "fake-child.py"
    _write_fake_child(fake_child)
    python = Path(sys.executable).resolve()
    prepared = _fixture_prepared(root, python)
    semantic_checks, semantic_details = _semantic_authority_static_checks(
        root, prepared
    )
    manifest_authority_accepted = False
    manifest_digest_rejected = False
    manifest_shape_rejected = False
    try:
        manifest_authority_accepted = (
            validate_approved_tools_manifest(
                prepared.tools_manifest_value,
                prepared.tools_manifest.sha256,
                _FakeSchema(),
            )
            == prepared.tools_manifest_value
        )
    except RunnerFailure:
        manifest_authority_accepted = False
    try:
        validate_approved_tools_manifest(
            prepared.tools_manifest_value, "0" * 64, _FakeSchema()
        )
    except RunnerFailure:
        manifest_digest_rejected = True
    try:
        validate_approved_tools_manifest(
            {**prepared.tools_manifest_value, "unexpected": True},
            prepared.tools_manifest.sha256,
            _FakeSchema(),
        )
    except RunnerFailure:
        manifest_shape_rejected = True
    schema_file = root / "fake-schema.py"
    atomic_write(schema_file, b"# self-test schema identity\n")
    scratch = root / "scratch"
    scratch.mkdir()
    output = root / "attempt"
    clock = _AdvancingClock()
    runner = RebaselineRunner(
        prepared,
        output,
        _FakeSchema(),
        lock_path=root / "global.lock",
        scratch_root=scratch,
        minimum_free_bytes=0,
        minimum_free_inodes=0,
        maximum_load1=6.0,
        quiet_timeout_seconds=2,
        quiet_poll_seconds=1,
        load_reader=lambda: 0.1,
        sleep=lambda _seconds: None,
        monotonic=clock,
        profile_factory=_FakeProfiles,
        schema_path=schema_file,
    )
    output.mkdir()
    runner.attempt_scratch.mkdir(parents=True)
    runner.runtime_home.mkdir(mode=0o700)
    checks: dict[str, bool] = {}
    details: dict[str, Any] = {}
    checks.update(semantic_checks)
    details.update(semantic_details)

    ack_read, ack_write = os.pipe()
    try:
        _write_all(
            ack_write,
            PERF_ACK_WIRE + PERF_ACK_WIRE,
            "self-test perf ACK frames",
        )
        first_ack = _read_ack_line(ack_read, timeout=0.1)
        second_ack = _read_ack_line(ack_read, timeout=0.1)
        os.set_blocking(ack_read, False)
        try:
            os.read(ack_read, 1)
        except BlockingIOError:
            ack_pipe_empty = True
        else:
            ack_pipe_empty = False
        checks["perf_ack_exact_five_byte_frames_consumed"] = (
            first_ack == PERF_ACK_WIRE
            and second_ack == PERF_ACK_WIRE
            and ack_pipe_empty
        )
    finally:
        os.close(ack_read)
        os.close(ack_write)

    def perf_ack_rejected(payload: bytes) -> bool:
        read_descriptor, write_descriptor = os.pipe()
        try:
            _write_all(write_descriptor, payload, "hostile self-test perf ACK")
            os.close(write_descriptor)
            write_descriptor = -1
            try:
                _read_ack_line(read_descriptor, timeout=0.1)
            except RunnerFailure:
                return True
            return False
        finally:
            os.close(read_descriptor)
            if write_descriptor >= 0:
                os.close(write_descriptor)

    checks["perf_ack_four_byte_frame_rejected"] = perf_ack_rejected(
        PERF_ACK_LEDGER_ENTRY
    )
    checks["perf_ack_malformed_five_byte_frame_rejected"] = perf_ack_rejected(
        b"ack\nX"
    )

    def comm_identities(name: str) -> set[tuple[int, int]]:
        identities: set[tuple[int, int]] = set()
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            try:
                identity = parse_proc_stat(int(entry.name))
            except (FileNotFoundError, OSError, ValueError):
                continue
            if identity.get("comm") == name:
                identities.add((identity["pid"], identity["starttime_ticks"]))
        return identities

    def descriptors_are_closed(descriptors: Iterable[int]) -> bool:
        for descriptor in descriptors:
            try:
                os.fstat(descriptor)
            except OSError:
                continue
            return False
        return True

    expected_corpus_records = runner._expected_corpus_execution_records(
        correctness_only=False
    )
    for descriptor in expected_corpus_records:
        corpus_root = Path(descriptor["root"])
        nested = corpus_root / "nested"
        nested.mkdir(parents=True)
        atomic_write(
            nested / "corpus.bin",
            f"authority-{descriptor['ordinal']}\n".encode(),
        )
        if descriptor["role"].endswith("source"):
            make_tree_read_only(corpus_root)
        else:
            runner._make_tree_writable(corpus_root)
        runner._register_corpus_execution_tree(
            corpus_root,
            role=str(descriptor["role"]),
            track=str(descriptor["track"]),
            row_ordinal=int(descriptor["row_ordinal"]),
            variant=str(descriptor["variant"]),
            read_only=descriptor["role"].endswith("source"),
        )

    full_authority = list(runner.corpus_execution_authority)
    full_payload = runner._corpus_execution_authority_payload(
        correctness_only=False
    )
    full_value = json.loads(full_payload)
    checks["corpus_authority_exact_specialized_full_coverage"] = (
        len(expected_corpus_records) == 18
        and len(full_value["records"]) == 18
        and [record["ordinal"] for record in full_value["records"]]
        == list(range(1, 19))
        and [record["role"] for record in full_value["records"]].count(
            "specialized-source"
        )
        == 1
        and [record["role"] for record in full_value["records"]].count(
            "specialized-copy"
        )
        == 2
        and [record["role"] for record in full_value["records"]].count(
            "full-source"
        )
        == 3
        and [record["role"] for record in full_value["records"]].count(
            "full-copy"
        )
        == 12
        and all(
            record["root_mode"]
            == (0o555 if record["role"].endswith("source") else 0o755)
            and record["file_mode"]
            == (0o444 if record["role"].endswith("source") else 0o644)
            and all("mode" in entry for entry in record["entries"])
            for record in full_value["records"]
        )
    )
    sealed_fd = runner._seal_corpus_execution_authority(correctness_only=False)
    try:
        checks["corpus_authority_exact_seals_and_replay"] = (
            fcntl.fcntl(sealed_fd, F_GET_SEALS)
            == CORPUS_EXECUTION_AUTHORITY_SEALS
            and os.pread(sealed_fd, len(full_payload), 0) == full_payload
        )
    finally:
        os.close(sealed_fd)

    unsealed_fd = create_sealable_memfd("asterism-unsealed-negative")
    try:
        _write_all(unsealed_fd, full_payload, "unsealed authority fixture")
        try:
            runner._verify_sealed_corpus_authority_fd(
                unsealed_fd, full_payload
            )
        except RunnerFailure:
            checks["corpus_authority_unsealed_rejected"] = True
        else:
            checks["corpus_authority_unsealed_rejected"] = False
    finally:
        os.close(unsealed_fd)

    runner.corpus_execution_authority = full_authority[:-1]
    try:
        runner._corpus_execution_authority_payload(correctness_only=False)
    except RunnerFailure:
        checks["corpus_authority_partial_rejected"] = True
    else:
        checks["corpus_authority_partial_rejected"] = False
    runner.corpus_execution_authority = [
        dict(record) for record in full_authority
    ]
    runner.corpus_execution_authority[0]["root"] += "-rebound"
    try:
        runner._corpus_execution_authority_payload(correctness_only=False)
    except RunnerFailure:
        checks["corpus_authority_rebound_rejected"] = True
    else:
        checks["corpus_authority_rebound_rejected"] = False
    runner.corpus_execution_authority = full_authority

    def coherent_entry_mutation_rejected(
        mutate: Callable[[list[dict[str, Any]]], None]
    ) -> bool:
        forged = json.loads(canonical_json_bytes(full_authority))
        entries = forged[1]["entries"]
        mutate(entries)
        forged[1]["tree_sha256"] = sha256_bytes(
            canonical_json_bytes(entries)
        )
        runner.corpus_execution_authority = forged
        try:
            runner._corpus_execution_authority_payload(
                correctness_only=False
            )
        except RunnerFailure:
            return True
        return False

    def file_entry(entries: list[dict[str, Any]]) -> dict[str, Any]:
        return next(entry for entry in entries if entry.get("kind") == "file")

    checks["corpus_authority_coherent_mode_hash_rejected"] = (
        coherent_entry_mutation_rejected(
            lambda entries: file_entry(entries).__setitem__("mode", 0o600)
        )
    )
    checks["corpus_authority_coherent_content_hash_rejected"] = (
        coherent_entry_mutation_rejected(
            lambda entries: file_entry(entries).__setitem__(
                "sha256", "0" * 64
            )
        )
        and coherent_entry_mutation_rejected(
            lambda entries: file_entry(entries).__setitem__(
                "bytes", int(file_entry(entries)["bytes"]) + 1
            )
        )
    )
    checks["corpus_authority_coherent_file_fields_rejected"] = (
        coherent_entry_mutation_rejected(
            lambda entries: file_entry(entries).__setitem__(
                "unexpected", True
            )
        )
    )
    checks["corpus_authority_coherent_file_path_rejected"] = (
        coherent_entry_mutation_rejected(
            lambda entries: file_entry(entries).__setitem__(
                "path", "nested/rebound.bin"
            )
        )
    )
    checks["corpus_authority_coherent_entry_order_rejected"] = (
        coherent_entry_mutation_rejected(lambda entries: entries.reverse())
    )

    def binding_type_mutation_rejected(field: str, value: Any) -> bool:
        forged = json.loads(canonical_json_bytes(full_authority))
        forged[0][field] = value
        runner.corpus_execution_authority = forged
        try:
            runner._corpus_execution_authority_payload(
                correctness_only=False
            )
        except RunnerFailure:
            return True
        return False

    integer_binding_fields = (
        "ordinal",
        "row_ordinal",
        "root_mode",
        "directory_mode",
        "file_mode",
    )
    checks["corpus_authority_binding_bool_types_rejected"] = all(
        binding_type_mutation_rejected(
            field, bool(full_authority[0][field])
        )
        for field in integer_binding_fields
    )
    checks["corpus_authority_binding_float_types_rejected"] = all(
        binding_type_mutation_rejected(
            field, float(full_authority[0][field])
        )
        for field in integer_binding_fields
    )

    class TextSubclass(str):
        pass

    checks["corpus_authority_binding_string_types_rejected"] = all(
        binding_type_mutation_rejected(
            field, TextSubclass(str(full_authority[0][field]))
        )
        for field in ("role", "track", "variant", "root", "tree_sha256")
    )

    top_level_types_rejected = True
    for drifted in (0, 1, 0.0, 1.0, None, "false"):
        runner.corpus_execution_authority = (
            full_authority[:3] if bool(drifted) else full_authority
        )
        try:
            runner._corpus_execution_authority_payload(
                correctness_only=drifted  # type: ignore[arg-type]
            )
        except RunnerFailure:
            continue
        top_level_types_rejected = False
    checks["corpus_authority_top_level_types_rejected"] = (
        top_level_types_rejected
    )
    runner.corpus_execution_authority = full_authority

    copy_file = (
        Path(full_authority[1]["root"])
        / str(full_authority[1]["entries"][-1]["path"])
    )
    copy_file.chmod(0o600)
    try:
        descriptor_tree_manifest(
            Path(full_authority[1]["root"]),
            root_mode=0o755,
            directory_mode=0o755,
            file_mode=0o644,
        )
    except RunnerFailure:
        checks["corpus_authority_copy_mode_mutation_rejected"] = True
    else:
        checks["corpus_authority_copy_mode_mutation_rejected"] = False
    finally:
        copy_file.chmod(0o644)

    def open_descriptor_set() -> set[int]:
        result: set[int] = set()
        for descriptor in range(3, 256):
            try:
                fcntl.fcntl(descriptor, fcntl.F_GETFD)
            except OSError:
                continue
            result.add(descriptor)
        return result

    race_root = Path(full_authority[1]["root"])
    descriptors_before_race = open_descriptor_set()
    original_stat = os.stat
    identity_race_injected = False

    class RacedStat:
        def __init__(self, observed: os.stat_result) -> None:
            self.observed = observed
            self.st_ino = observed.st_ino + 1

        def __getattr__(self, name: str) -> Any:
            return getattr(self.observed, name)

    def inject_ancestor_identity_race(
        path: Any, *arguments: Any, **keywords: Any
    ) -> os.stat_result:
        nonlocal identity_race_injected
        observed = original_stat(path, *arguments, **keywords)
        if (
            not identity_race_injected
            and keywords.get("dir_fd") is not None
            and keywords.get("follow_symlinks") is False
        ):
            identity_race_injected = True
            return RacedStat(observed)  # type: ignore[return-value]
        return observed

    os.stat = inject_ancestor_identity_race  # type: ignore[assignment]
    try:
        try:
            descriptor_tree_manifest(
                race_root,
                root_mode=0o755,
                directory_mode=0o755,
                file_mode=0o644,
            )
        except RunnerFailure:
            identity_race_rejected = True
        else:
            identity_race_rejected = False
    finally:
        os.stat = original_stat  # type: ignore[assignment]
    checks["corpus_authority_ancestor_race_fd_cleanup"] = (
        identity_race_injected
        and identity_race_rejected
        and open_descriptor_set() == descriptors_before_race
    )

    runner.corpus_execution_authority = full_authority[:3]
    correctness_payload = runner._corpus_execution_authority_payload(
        correctness_only=True
    )
    checks["corpus_authority_correctness_only_specialized"] = (
        len(json.loads(correctness_payload)["records"]) == 3
        and json.loads(correctness_payload)["correctness_only"] is True
    )
    runner.corpus_execution_authority = full_authority

    popen_capture: dict[str, Any] = {}
    original_popen = subprocess.Popen
    stdout_probe = _open_exclusive(root / "evaluator-spawn.stdout")
    stderr_probe = _open_exclusive(root / "evaluator-spawn.stderr")
    authority_probe = runner._seal_corpus_execution_authority(
        correctness_only=False
    )
    spawn_descriptors = (stdout_probe, stderr_probe, authority_probe)

    def failing_evaluator_popen(
        _argv: tuple[str, ...], **arguments: Any
    ) -> NoReturn:
        inherited = tuple(arguments.get("pass_fds", ()))
        environment = arguments.get("env", {})
        popen_capture.update(
            {
                "pass_fds": inherited,
                "environment": dict(environment),
                "authority_live": (
                    len(inherited) == 1
                    and os.fstat(inherited[0]).st_size == len(full_payload)
                ),
            }
        )
        raise OSError("synthetic evaluator Popen failure")

    subprocess.Popen = failing_evaluator_popen  # type: ignore[assignment]
    try:
        try:
            runner._spawn_evaluator_process(
                (str(python), "fake-evaluator.py", "--evaluate", str(output)),
                prepared.variants["A"].executable,
                stdout_probe,
                stderr_probe,
                authority_probe,
            )
        except OSError:
            popen_failed = True
        else:
            popen_failed = False
    finally:
        subprocess.Popen = original_popen  # type: ignore[assignment]
    checks["corpus_authority_popen_failure_cleanup"] = (
        popen_failed
        and descriptors_are_closed(spawn_descriptors)
        and runner.active_evaluator_authority_fd is None
        and runner.active_evaluator_process is None
    )
    checks["corpus_authority_evaluator_only_inheritance"] = (
        popen_capture.get("pass_fds") == (authority_probe,)
        and popen_capture.get("authority_live") is True
        and popen_capture.get("environment", {}).get(
            CORPUS_EXECUTION_AUTHORITY_FD_ENVIRONMENT
        )
        == str(authority_probe)
        and CORPUS_EXECUTION_AUTHORITY_FD_ENVIRONMENT not in runner.config
        and CORPUS_EXECUTION_AUTHORITY_FD_ENVIRONMENT
        not in full_value
    )

    class DeepValidatorFixture:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def validate_source_approval(self, approval: dict[str, Any]) -> None:
            if approval != {
                "schema": "bn-2l3n-source-approval-v3",
                "protocol": PROTOCOL,
            }:
                raise ValueError("fixture source approval is incomplete")
            self.calls.append("source")

        def validate_prepared_artifacts(
            self,
            value: dict[str, Any],
            approval: dict[str, Any],
            prepared_path: Path,
        ) -> None:
            if (
                value != {"schema": PREPARED_SCHEMA}
                or approval.get("protocol") != PROTOCOL
                or prepared_path.name != "prepared-artifacts.json"
            ):
                raise ValueError("fixture prepared authority is incomplete")
            self.calls.append("prepared")

        def validate_binary_contract(self, contract: dict[str, Any]) -> None:
            if contract != {
                "schema": "bn-2l3n-binary-contract-v3",
                "protocol": PROTOCOL,
            }:
                raise ValueError("fixture binary contract is incomplete")
            self.calls.append("contract")

    def leader_exited_group_cleanup(context: str) -> bool:
        leader = subprocess.Popen(
            (
                str(python),
                "-c",
                (
                    "import subprocess,sys;"
                    "subprocess.Popen(['/bin/sleep','30'],stdin=subprocess.DEVNULL,"
                    "stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);"
                    "print('ready',flush=True);sys.stdin.buffer.read(1)"
                ),
            ),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        try:
            if leader.stdout is None or leader.stdout.readline() != b"ready\n":
                return False
            if leader.stdin is None:
                return False
            leader.stdin.write(b"x")
            leader.stdin.close()
            leader.wait(timeout=5)
            deadline = time.monotonic() + 2
            while not process_group_exists(leader.pid) and time.monotonic() < deadline:
                time.sleep(0.001)
            if not process_group_exists(leader.pid):
                return False
            try:
                reject_orphan_process_group(leader, context, exit_code=30)
            except RunnerFailure as error:
                return (
                    "exact group was killed and reaped" in error.reason
                    and not process_group_exists(leader.pid)
                )
            return False
        finally:
            if process_group_exists(leader.pid):
                try:
                    terminate_process_group(leader)
                except RunnerFailure:
                    pass

    checks["source_approved_tools_manifest"] = all(
        (
            manifest_authority_accepted,
            manifest_digest_rejected,
            manifest_shape_rejected,
        )
    )
    checks["leader_exited_profile_group_killed"] = leader_exited_group_cleanup(
        "self-test profile helper"
    )
    checks["leader_exited_terminal_group_killed"] = leader_exited_group_cleanup(
        "self-test terminal verifier"
    )
    deep = DeepValidatorFixture()
    deep_approval = {
        "schema": "bn-2l3n-source-approval-v3",
        "protocol": PROTOCOL,
    }
    deep_prepared = {"schema": PREPARED_SCHEMA}
    deep_contract = {
        "schema": "bn-2l3n-binary-contract-v3",
        "protocol": PROTOCOL,
    }
    try:
        reject_superseded_authority(deep_approval, "self-test approval")
        reject_superseded_authority(deep_prepared, "self-test prepared")
        reject_superseded_authority(deep_contract, "self-test contract")
        call_shared_validator(
            deep, "validate_source_approval", "self-test approval", deep_approval
        )
        call_shared_validator(
            deep,
            "validate_prepared_artifacts",
            "self-test prepared",
            deep_prepared,
            deep_approval,
            prepared.path,
        )
        call_shared_validator(
            deep, "validate_binary_contract", "self-test contract", deep_contract
        )
    except RunnerFailure as error:
        details["shared_deep_authority_positive"] = error.reason
        checks["shared_deep_authority_positive"] = False
    else:
        checks["shared_deep_authority_positive"] = deep.calls == [
            "source",
            "prepared",
            "contract",
        ]
    for label, identity in (
        ("nested_v2_authority_rejected", "bn-2l3n-source-approval-v2"),
        ("nested_r5_authority_rejected", "asterism-rebaseline-lock-candidates-r5"),
    ):
        try:
            reject_superseded_authority(
                {"nested": {"schema": identity}}, label
            )
        except RunnerFailure:
            checks[label] = True
        else:
            checks[label] = False
    try:
        reject_superseded_authority(
            {
                "source_review": {
                    "current_children_attestation": {
                        "schema": CURRENT_CHILDREN_ATTESTATION_SCHEMA,
                    },
                },
            },
            "pinned attestation schema accepted",
        )
    except RunnerFailure:
        checks["pinned_attestation_schema_accepted"] = False
    else:
        checks["pinned_attestation_schema_accepted"] = True
    try:
        call_shared_validator(
            deep,
            "validate_binary_contract",
            "incomplete binary contract",
            {"schema": "bn-2l3n-binary-contract-v3"},
        )
    except RunnerFailure:
        checks["incomplete_binary_contract_rejected"] = True
    else:
        checks["incomplete_binary_contract_rejected"] = False
    try:
        call_shared_validator(
            object(),
            "validate_source_approval",
            "missing shared source validator",
            deep_approval,
        )
    except RunnerFailure:
        checks["shared_deep_validators_required"] = True
    else:
        checks["shared_deep_validators_required"] = False

    fake_filesystem = {
        "mount_id": "42",
        "parent_mount_id": "1",
        "device": "259:3",
        "root": "/",
        "target": str(scratch.resolve()),
        "mount_options": "rw,relatime",
        "filesystem_type": REQUIRED_FILESYSTEM_TYPE,
        "source": "/dev/nvme0n1p3",
        "super_options": "rw",
    }
    runner._filesystem_identity = lambda: dict(fake_filesystem)  # type: ignore[method-assign]
    runner.host_resource_preflight()

    checks["mountinfo_escape_decoder"] = (
        runner._decode_mountinfo_field(r"a\040b\011c\012d\134e")
        == "a b\tc\nd\\e"
    )
    fake_sys_dev = root / "fake-sys-dev-block"
    fake_subject = (
        root
        / "fake-sys-devices"
        / "pci0000:00"
        / "nvme"
        / "nvme0"
        / "nvme0n1"
        / "nvme0n1p3"
    )
    fake_scheduler = fake_subject.parent / "queue" / "scheduler"
    fake_subject.mkdir(parents=True)
    atomic_write(fake_scheduler, b"[none] mq-deadline\n", mode=0o444)
    fake_sys_dev.mkdir(parents=True)
    (fake_sys_dev / "259:3").symlink_to(fake_subject, target_is_directory=True)
    checks["partition_parent_scheduler"] = runner._scheduler_state(
        fake_filesystem, sys_dev_root=fake_sys_dev
    ) == {
        "logical_device": "259:3",
        "base_device": "nvme0n1",
        "scheduler_path": str(fake_scheduler.resolve()),
        "scheduler_value": "[none] mq-deadline",
    }

    admitted = True
    wrong_filesystem_rejected = False
    bytes_floor_rejected = False
    inode_floor_rejected = False
    old_minimum_bytes = runner.minimum_free_bytes
    old_minimum_inodes = runner.minimum_free_inodes
    runner.minimum_free_bytes = MIN_FREE_BYTES
    runner.minimum_free_inodes = MIN_FREE_INODES
    try:
        runner._validate_host_admission(
            fake_filesystem, MIN_FREE_BYTES, MIN_FREE_INODES
        )
        for label, filesystem, free_bytes, free_inodes in (
            (
                "filesystem",
                {**fake_filesystem, "filesystem_type": "tmpfs"},
                MIN_FREE_BYTES,
                MIN_FREE_INODES,
            ),
            ("bytes", fake_filesystem, MIN_FREE_BYTES - 1, MIN_FREE_INODES),
            ("inodes", fake_filesystem, MIN_FREE_BYTES, MIN_FREE_INODES - 1),
        ):
            try:
                runner._validate_host_admission(filesystem, free_bytes, free_inodes)
            except RunnerFailure:
                if label == "filesystem":
                    wrong_filesystem_rejected = True
                elif label == "bytes":
                    bytes_floor_rejected = True
                else:
                    inode_floor_rejected = True
            else:
                admitted = False
    except RunnerFailure:
        admitted = False
    finally:
        runner.minimum_free_bytes = old_minimum_bytes
        runner.minimum_free_inodes = old_minimum_inodes
    checks["host_admission_boundaries"] = all(
        (
            admitted,
            wrong_filesystem_rejected,
            bytes_floor_rejected,
            inode_floor_rejected,
        )
    )
    checks["host_admission_precedes_claim_and_child"] = (
        not prepared.claim_path.exists()
        and not prepared.claim_path.is_symlink()
        and runner.claim is None
        and runner.child_count == 0
    )

    immutable_inputs_accepted = False
    mutable_input_rejected = False
    symlink_input_rejected = False
    try:
        immutable_inputs_accepted = (
            resolve_immutable_json_file(
                prepared.path,
                "self-test prepared",
                filename="prepared-artifacts.json",
            )
            == prepared.path.resolve()
            and resolve_immutable_json_file(
                prepared.source_approval,
                "self-test approval",
                root=root.resolve(),
                filename="source-approval.json",
            )
            == prepared.source_approval.resolve()
        )
    except RunnerFailure:
        immutable_inputs_accepted = False
    mutable_dir = root / "mutable-input"
    mutable_manifest = mutable_dir / "prepared-artifacts.json"
    atomic_json(mutable_manifest, {"mutable": True})
    try:
        resolve_immutable_json_file(
            mutable_manifest, "mutable prepared", filename="prepared-artifacts.json"
        )
    except RunnerFailure:
        mutable_input_rejected = True
    symlink_dir = root / "symlink-input"
    symlink_dir.mkdir()
    symlink_manifest = symlink_dir / "prepared-artifacts.json"
    symlink_manifest.symlink_to(prepared.path)
    try:
        resolve_immutable_json_file(
            symlink_manifest, "symlink prepared", filename="prepared-artifacts.json"
        )
    except RunnerFailure:
        symlink_input_rejected = True
    checks["immutable_prepared_inputs"] = all(
        (immutable_inputs_accepted, mutable_input_rejected, symlink_input_rejected)
    )

    relocated_root = root / "relocated-prepared"
    relocated_root.mkdir()
    relocated_manifest = relocated_root / "prepared-artifacts.json"
    relocated_approval = relocated_root / "source-approval.json"
    atomic_write(relocated_manifest, prepared.path.read_bytes(), mode=0o444)
    atomic_write(
        relocated_approval, prepared.source_approval.read_bytes(), mode=0o444
    )
    relocated_bindings = relocated_root / "bindings"
    relocated_bindings.mkdir()
    relocated_tools_manifest = relocated_bindings / "tools-manifest.json"
    atomic_write(
        relocated_tools_manifest,
        prepared.tools_manifest.path.read_bytes(),
        mode=0o444,
    )
    relocated_source_review_files = {}
    for name, source_review_file in prepared.source_review_files.items():
        relocated_source_review_path = (
            relocated_bindings / source_review_file.path.name
        )
        atomic_write(
            relocated_source_review_path,
            source_review_file.path.read_bytes(),
            mode=0o444,
        )
        relocated_source_review_files[name] = SupportFile(
            f"prepared-source-review-{name}",
            relocated_source_review_path,
            sha256(relocated_source_review_path),
            0o444,
        )
    relocated_manifests = relocated_root / "manifests"
    relocated_manifests.mkdir()
    relocated_release_compile_out = (
        relocated_manifests / "release-compile-out.json"
    )
    atomic_write(
        relocated_release_compile_out,
        prepared.release_compile_out.path.read_bytes(),
        mode=0o444,
    )
    relocated_protocol = relocated_root / "BN-2L3N-PROTOCOL.md"
    protocol_source = Path(__file__).with_name("BN-2L3N-PROTOCOL.md")
    atomic_write(relocated_protocol, protocol_source.read_bytes(), mode=0o444)
    relocated_historical = relocated_root / "inputs" / "BN-2SU-FINAL.csv"
    historical_source = (
        Path(__file__).parents[1] / "baseline_matrix" / "BN-2SU-FINAL.csv"
    )
    atomic_write(
        relocated_historical, historical_source.read_bytes(), mode=0o444
    )
    relocated_claims = relocated_root / "claims"
    relocated_claims.mkdir(mode=0o700)
    relocated_bindings.chmod(0o555)
    relocated_manifests.chmod(0o555)
    relocated_historical.parent.chmod(0o555)
    relocated_root.chmod(0o555)
    relocated_inputs = {
        "protocol": SupportFile(
            "input-protocol",
            relocated_protocol,
            sha256(relocated_protocol),
            0o444,
        ),
        "historical_baseline": SupportFile(
            "input-historical-baseline",
            relocated_historical,
            sha256(relocated_historical),
            0o444,
        ),
    }
    relocated_prepared = replace(
        prepared,
        path=relocated_manifest,
        digest=sha256(relocated_manifest),
        root=relocated_root,
        source_approval=relocated_approval,
        source_approval_sha256=sha256(relocated_approval),
        tools_manifest=SupportFile(
            "prepared-tools-manifest",
            relocated_tools_manifest,
            sha256(relocated_tools_manifest),
            0o444,
        ),
        source_review_files=relocated_source_review_files,
        release_compile_out=SupportFile(
            "prepared-release-compile-out",
            relocated_release_compile_out,
            sha256(relocated_release_compile_out),
            0o444,
        ),
        claim_path=relocated_claims / "single-use-claim.json",
        inputs=relocated_inputs,
    )
    relocated_output = root / "relocated-attempt"
    relocated_output.mkdir()
    saved_prepared = runner.prepared
    saved_output = runner.output
    saved_frozen = runner.frozen_files
    saved_tree_snapshot = runner.prepared_tree_snapshot
    try:
        runner.prepared = relocated_prepared
        runner.output = relocated_output
        runner.frozen_files = runner._frozen_file_bindings()
        runner.prepared_tree_snapshot = runner._prepared_tree_state()
        runner.publish_frozen_inputs()
        checks["relocated_protocol_and_baseline"] = all(
            (
                sha256(relocated_output / "BN-2L3N-PROTOCOL.md")
                == PROTOCOL_SHA256,
                sha256(relocated_output / "BN-2SU-FINAL.csv")
                == HISTORICAL_BASELINE_SHA256,
                stat.S_IMODE(
                    (relocated_output / "BN-2L3N-PROTOCOL.md").stat().st_mode
                )
                == 0o444,
                stat.S_IMODE(
                    (relocated_output / "BN-2SU-FINAL.csv").stat().st_mode
                )
                == 0o444,
                str((relocated_output / "BN-2L3N-PROTOCOL.md").resolve())
                in runner.frozen_files,
                str((relocated_output / "BN-2SU-FINAL.csv").resolve())
                in runner.frozen_files,
                str(relocated_tools_manifest.resolve()) in runner.frozen_files,
                str(relocated_release_compile_out.resolve())
                in runner.frozen_files,
                all(
                    str(source_review_file.path.resolve()) in runner.frozen_files
                    for source_review_file in (
                        relocated_source_review_files.values()
                    )
                ),
            )
        )
    except RunnerFailure as error:
        details["relocated_protocol_and_baseline"] = error.reason
        checks["relocated_protocol_and_baseline"] = False
    finally:
        runner.prepared = saved_prepared
        runner.output = saved_output
        runner.frozen_files = saved_frozen
        runner.prepared_tree_snapshot = saved_tree_snapshot

    orders = expected_orders()
    checks["frozen_cardinalities"] = all(
        len(orders[track]) == count for track, count in TRACK_CARDINALITY.items()
    )
    primary = orders["primary"]
    checks["williams_and_reversal"] = all(
        tuple(
            row["variant"]
            for row in primary
            if row["block"] == block
            and row["cell_ordinal"] == 1
        )[:4]
        == WILLIAMS[block - 1]
        for block in range(1, 5)
    ) and primary[128]["cell_ordinal"] == 32

    try:
        canonical_json_bytes({"bad": float("nan")})
    except RunnerFailure:
        checks["nan_rejected"] = True
    else:
        checks["nan_rejected"] = False
    try:
        canonical_json_bytes({"bad": float("inf")})
    except RunnerFailure:
        checks["infinity_rejected"] = True
    else:
        checks["infinity_rejected"] = False

    try:
        resolve_support_file(
            root,
            {"path": str(fake_child), "sha256": "0" * 64, "mode": 0o755},
            "negative-support",
        )
    except RunnerFailure:
        checks["support_hash_negative"] = True
    else:
        checks["support_hash_negative"] = False

    wrong_runtime = Executable(
        "negative-runtime",
        python,
        "0" * 64,
        stat.S_IMODE(python.stat().st_mode),
        RUNNER_COMM,
    )
    try:
        verify_current_runtime(wrong_runtime, RUNNER_COMM)
    except RunnerFailure:
        checks["runtime_binding_negative"] = True
    else:
        checks["runtime_binding_negative"] = False

    unavailable_plan = ChildPlan(
        kind="cpu_profiles",
        context={"variant": "A"},
        executable=prepared.variants["A"].executable,
        argv=prepared.variants["A"].evidence_argv,
        environment={},
        expected_records=1,
        track="cpu_profiles",
        store_path=None,
        require_store_after=False,
        timeout_seconds=1,
    )
    unavailable_session = RunnerOwnedProfileSession(
        plan=unavailable_plan,
        output_dir=output,
        ordinal=99_999,
        tools={},
        support_files={},
        profile_adapter=_FakeProfiles,
        permission_result="not_available;perf_event_paranoid=3;scope=user-only;exit_status=255",
    )
    unavailable_evidence = unavailable_session.finish()
    checks["unavailable_perf_skips_spawn"] = (
        unavailable_session.process is None
        and unavailable_evidence["helper_records"] == []
        and unavailable_evidence["inputs"]
        == {
            "perf_permission": (
                "not_available;perf_event_paranoid=3;scope=user-only;exit_status=255"
            ),
            "perf_control_events": [],
            "perf_raw_artifacts": {},
        }
        and unavailable_session.environment_overrides()
        == {
            PERF_PERMISSION_ENVIRONMENT: (
                "not_available;perf_event_paranoid=3;scope=user-only;exit_status=255"
            )
        }
        and unavailable_session.child_pass_fds() == ()
    )
    original_pipe = os.pipe
    first_perf_pipe: list[int] = []
    pipe_calls = 0

    def fail_second_perf_pipe() -> tuple[int, int]:
        nonlocal pipe_calls
        pipe_calls += 1
        if pipe_calls == 2:
            raise OSError("synthetic second pipe failure")
        descriptors = original_pipe()
        first_perf_pipe.extend(descriptors)
        return descriptors

    os.pipe = fail_second_perf_pipe
    try:
        RunnerOwnedProfileSession(
            plan=unavailable_plan,
            output_dir=output,
            ordinal=99_996,
            tools={},
            support_files={},
            profile_adapter=_FakeProfiles,
            permission_result="available;perf_event_paranoid=2;scope=user-only",
        )
    except OSError:
        checks["perf_constructor_partial_pipe_cleanup"] = (
            len(first_perf_pipe) == 2
            and descriptors_are_closed(first_perf_pipe)
        )
    else:
        checks["perf_constructor_partial_pipe_cleanup"] = False
    finally:
        os.pipe = original_pipe

    perf_cleanup_matrix: dict[str, dict[str, Any]] = {}
    original_open_exclusive = globals()["_open_exclusive"]
    original_popen = subprocess.Popen
    original_close = os.close
    original_wait_exact = globals()["_wait_exact_process"]
    original_terminate_process_group = globals()["terminate_process_group"]

    class FakePerfProcess:
        def __init__(self, pid: int) -> None:
            self.pid = pid
            self.returncode: int | None = None
            self.terminated = False

        def poll(self) -> int | None:
            return self.returncode

    for case_ordinal, stage in enumerate(
        (
            "stat_open",
            "stderr_open",
            "popen",
            "first_close",
            "second_close",
            "close_and_terminate",
            "identity",
        ),
        start=99_980,
    ):
        session = RunnerOwnedProfileSession(
            plan=unavailable_plan,
            output_dir=output,
            ordinal=case_ordinal,
            tools={"perf": prepared.variants["A"].executable},
            support_files={},
            profile_adapter=_FakeProfiles,
            permission_result="available;perf_event_paranoid=2;scope=user-only",
        )
        session.bind_child({"pid": os.getpid()}, control_fd=123)
        tracked_descriptors = [
            descriptor
            for descriptor in (
                session.helper_control_read,
                session.helper_ack_write,
            )
            if descriptor is not None
        ]
        opened_descriptors: list[int] = []
        fake_processes: list[FakePerfProcess] = []
        injection = {"open_calls": 0, "close_calls": 0, "close_failed": False}

        def inject_perf_open(path: Path) -> int:
            injection["open_calls"] += 1
            if stage == "stat_open" and injection["open_calls"] == 1:
                raise OSError("synthetic perf stat open failure")
            if stage == "stderr_open" and injection["open_calls"] == 2:
                raise OSError("synthetic perf stderr open failure")
            descriptor = original_open_exclusive(path)
            opened_descriptors.append(descriptor)
            return descriptor

        def inject_perf_popen(*_args: Any, **_kwargs: Any) -> FakePerfProcess:
            if stage == "popen":
                raise OSError("synthetic perf spawn failure")
            process = FakePerfProcess(900_000 + case_ordinal)
            fake_processes.append(process)
            return process

        def inject_perf_close(descriptor: int) -> None:
            injection["close_calls"] += 1
            fail_at = (
                1 if stage in {"first_close", "close_and_terminate"} else 2
            )
            if (
                stage in {"first_close", "second_close", "close_and_terminate"}
                and injection["close_calls"] == fail_at
                and not injection["close_failed"]
            ):
                injection["close_failed"] = True
                raise OSError(f"synthetic perf close {fail_at} failure")
            original_close(descriptor)

        def inject_perf_wait(
            _process: subprocess.Popen[Any],
            _executable: Executable,
            timeout: float = 5.0,
        ) -> dict[str, Any]:
            del timeout
            if stage == "identity":
                raise RunnerFailure("synthetic perf identity failure")
            return {
                "pid": fake_processes[-1].pid,
                "starttime_ticks": 1,
                "comm": "perf",
            }

        def record_perf_termination(process: FakePerfProcess) -> None:
            if stage == "close_and_terminate":
                raise OSError("synthetic perf termination failure")
            process.terminated = True
            process.returncode = -signal.SIGTERM

        globals()["_open_exclusive"] = inject_perf_open
        subprocess.Popen = inject_perf_popen  # type: ignore[method-assign]
        os.close = inject_perf_close
        globals()["_wait_exact_process"] = inject_perf_wait
        globals()["terminate_process_group"] = record_perf_termination
        rejected = False
        try:
            session._spawn_perf()
        except (OSError, RunnerFailure):
            rejected = True
        finally:
            globals()["_open_exclusive"] = original_open_exclusive
            subprocess.Popen = original_popen
            os.close = original_close
            globals()["_wait_exact_process"] = original_wait_exact
            globals()["terminate_process_group"] = original_terminate_process_group
        all_tracked = tracked_descriptors + opened_descriptors
        perf_cleanup_matrix[stage] = {
            "rejected": rejected,
            "descriptors_closed": descriptors_are_closed(all_tracked),
            "helper_descriptors_released": (
                session.helper_control_read is None
                and session.helper_ack_write is None
            ),
            "processes_terminated_or_owned": all(
                process.terminated or session.process is process
                for process in fake_processes
            ),
            "process_state_safe": (
                session.process is fake_processes[-1]
                if stage == "close_and_terminate" and fake_processes
                else session.process is None
            ),
        }
        if stage == "close_and_terminate" and fake_processes:
            fake_processes[-1].terminated = True
            fake_processes[-1].returncode = -signal.SIGKILL
            session.process = None
        session.abort()
        for descriptor in all_tracked:
            try:
                original_close(descriptor)
            except OSError:
                pass
    details["perf_spawn_cleanup_matrix"] = perf_cleanup_matrix
    checks["perf_spawn_cleanup_matrix"] = all(
        all(case.values()) for case in perf_cleanup_matrix.values()
    )

    bound_correctness = prepared.variants["A"].executable
    runner.prepared = replace(
        prepared,
        tools={"correctness": bound_correctness, "fault": bound_correctness},
    )
    runner._refresh_correctness_execution()

    def correctness_fixture(
        descriptor: dict[str, str], ordinal: int = 1
    ) -> tuple[ChildPlan, dict[str, Any], dict[str, Any]]:
        plan = next(
            plan
            for phase in ("pre", "post")
            for plan in runner.correctness_plans(phase)
            if plan.context["variant"] == descriptor["variant"]
            and plan.context["phase"] == descriptor["phase"]
            and plan.context["suite"] == descriptor["suite"]
            and plan.kind == descriptor["kind"]
        )
        value = {
            "schema": _FakeSchema.CORRECTNESS_CHILD_SCHEMA,
            "protocol": PROTOCOL,
            "attempt_nonce": runner.attempt_nonce,
            "variant": descriptor["variant"],
            "phase": descriptor["phase"],
            "suite": descriptor["suite"],
            "harness_sound": True,
            "boundedness": (
                dict(_FakeSchema.CORRECTNESS_EXPECTED_BOUNDEDNESS)
                if descriptor["suite"] == "current-fault"
                else None
            ),
            "cases": [
                {
                    "id": descriptor["id"],
                    "classification": descriptor["classification"],
                    "status": "PASS",
                }
            ],
        }
        record = {
            "ordinal": ordinal,
            "raw_path": str((root / f"correctness-{ordinal}.json").resolve()),
            "raw_sha256": f"{ordinal % 10}" * 64,
            "stderr_bytes": 0,
            "executable_path": str(plan.executable.path),
            "executable_sha256": plan.executable.sha256,
            "executable_mode": plan.executable.mode,
            "executable_comm": plan.executable.comm,
            "argv": list(plan.argv),
            "environment": dict(plan.environment),
        }
        return plan, value, record

    def reset_correctness() -> None:
        runner.correctness_observations.clear()
        runner.correctness_boundedness.clear()

    descriptors = runner.config["correctness_cases"]
    product_descriptor = next(
        item
        for item in descriptors
        if item["phase"] == "pre" and item["suite"] == "current-product"
    )
    fault_descriptor = next(
        item
        for item in descriptors
        if item["phase"] == "pre" and item["suite"] == "current-fault"
    )

    negative_mutations: dict[str, Callable[[ChildPlan, dict[str, Any], dict[str, Any]], None]] = {
        "correctness_missing_case_rejected": lambda _plan, value, _record: value.update(
            {"cases": []}
        ),
        "correctness_wrong_partition_rejected": lambda _plan, value, _record: value[
            "cases"
        ][0].update({"id": "fake-fault", "classification": "boundedness"}),
        "correctness_false_harness_rejected": lambda _plan, value, _record: value.update(
            {"harness_sound": False}
        ),
        "correctness_stderr_rejected": lambda _plan, _value, record: record.update(
            {"stderr_bytes": 1}
        ),
    }
    for label, mutate in negative_mutations.items():
        reset_correctness()
        plan, value, record = correctness_fixture(product_descriptor)
        mutate(plan, value, record)
        try:
            runner._validate_correctness_child(plan, value, record)
        except RunnerFailure:
            checks[label] = True
        else:
            checks[label] = False

    reset_correctness()
    fault_plan, fault_value, fault_record = correctness_fixture(fault_descriptor)
    fault_value["boundedness"]["owner_ring_intents"] = 1023
    try:
        runner._validate_correctness_child(fault_plan, fault_value, fault_record)
    except RunnerFailure:
        checks["correctness_bounds_mismatch_rejected"] = True
    else:
        checks["correctness_bounds_mismatch_rejected"] = False

    reset_correctness()
    wrong_kind_plan, wrong_kind_value, wrong_kind_record = correctness_fixture(
        fault_descriptor
    )
    wrong_kind_plan = ChildPlan(
        **{**wrong_kind_plan.__dict__, "kind": "correctness"}
    )
    try:
        runner._validate_correctness_child(
            wrong_kind_plan, wrong_kind_value, wrong_kind_record
        )
    except RunnerFailure:
        checks["correctness_fault_kind_rejected"] = True
    else:
        checks["correctness_fault_kind_rejected"] = False

    reset_correctness()
    duplicate_plan, duplicate_value, duplicate_record = correctness_fixture(
        product_descriptor
    )
    runner._validate_correctness_child(
        duplicate_plan, duplicate_value, duplicate_record
    )
    try:
        runner._validate_correctness_child(
            duplicate_plan, duplicate_value, duplicate_record
        )
    except RunnerFailure:
        checks["correctness_duplicate_rejected"] = True
    else:
        checks["correctness_duplicate_rejected"] = False

    noncanonical_path = root / "noncanonical-correctness.json"
    atomic_write(noncanonical_path, b'{"schema": "not-canonical"}\n')
    try:
        runner._parse_canonical_lines(noncanonical_path, 1)
    except RunnerFailure:
        checks["correctness_noncanonical_rejected"] = True
    else:
        checks["correctness_noncanonical_rejected"] = False

    reset_correctness()
    for ordinal, descriptor in enumerate(descriptors, start=1):
        plan, value, record = correctness_fixture(descriptor, ordinal)
        runner._validate_correctness_child(plan, value, record)
    runner._publish_correctness_aggregate()
    aggregate = load_canonical_json(output / "correctness.json")
    checks["correctness_aggregate_bound_and_ordered"] = (
        aggregate["boundedness"] == _FakeSchema.CORRECTNESS_EXPECTED_BOUNDEDNESS
        and [
            {
                key: case[key]
                for key in (
                    "id",
                    "variant",
                    "phase",
                    "suite",
                    "kind",
                    "classification",
                )
            }
            for case in aggregate["cases"]
        ]
        == descriptors
        and stat.S_IMODE((output / "correctness.json").stat().st_mode) == 0o444
    )
    pre_failure = next(
        observation
        for observation in runner.correctness_observations.values()
        if observation["variant"] == "A" and observation["phase"] == "pre"
    )
    historical_oracle = next(
        observation
        for observation in runner.correctness_observations.values()
        if observation["variant"] in {"C", "D"}
    )
    post_failure = next(
        observation
        for observation in runner.correctness_observations.values()
        if observation["variant"] == "A"
        and observation["phase"] == "post"
        and observation["id"] == pre_failure["id"]
    )
    pre_failure["status"] = "FAIL"
    expected_early_failure_ids = [pre_failure["id"]]
    historical_oracle["status"] = "FAIL"
    observed_current, observed_historical = runner._pre_correctness_failures()
    checks["historical_failure_forces_early_inconclusive"] = (
        observed_current == expected_early_failure_ids
        and observed_historical
        == [
            {
                "variant": historical_oracle["variant"],
                "phase": "oracle",
                "id": historical_oracle["id"],
            }
        ]
    )
    historical_oracle["status"] = "PASS"
    post_failure["status"] = "FAIL"
    observed_pre_failures, observed_historical = (
        runner._pre_correctness_failures()
    )
    observed_post_failures = runner._post_current_failure_ids()
    runner._publish_correctness_only_marker(
        observed_pre_failures,
        observed_post_failures,
        observed_historical,
    )
    marker = load_canonical_json(output / "correctness-only.json")
    checks["correctness_only_marker_before_timing"] = (
        observed_pre_failures == expected_early_failure_ids
        and observed_post_failures == expected_early_failure_ids
        and marker["current_pre_failed_case_ids"]
        == expected_early_failure_ids
        and marker["current_post_failed_case_ids"]
        == expected_early_failure_ids
        and marker["historical_failed_cases"] == []
        and marker["timing_child_records"] == 0
        and marker["trigger"] == "current"
        and stat.S_IMODE((output / "correctness-only.json").stat().st_mode)
        == 0o444
    )
    pre_failure["status"] = "PASS"
    post_failure["status"] = "PASS"

    tracer_path = Path(shutil.which("strace") or "")
    launcher_path = Path(__file__).with_name("strace_attach.py").resolve()
    attach_session: RunnerOwnedProfileSession | None = None
    tracee: subprocess.Popen[Any] | None = None
    try:
        if not tracer_path.is_file():
            raise RunnerFailure("strace is absent from synthetic attach test")
        trace_plan = ChildPlan(
            kind="smoke",
            context={
                "variant": "A",
                "smoke_target": "syscall_profiles",
                "variant_trace_path_markers": {
                    "log": [
                        {
                            "kind": "directory_prefix",
                            "path": str(output / "trace-log") + "/",
                        }
                    ],
                    "metadata": [],
                },
            },
            executable=prepared.variants["A"].executable,
            argv=prepared.variants["A"].evidence_argv,
            environment={},
            expected_records=1,
            track=None,
            store_path=None,
            require_store_after=False,
            timeout_seconds=5,
        )
        trace_tools = {
            "strace_launcher_runtime": Executable(
                "strace-launcher-runtime",
                python,
                sha256(python),
                stat.S_IMODE(python.stat().st_mode),
                "ast-trace-wait",
            ),
            "strace": Executable(
                "strace",
                tracer_path.resolve(),
                sha256(tracer_path.resolve()),
                stat.S_IMODE(tracer_path.resolve().stat().st_mode),
                "strace",
            ),
        }
        trace_support = {
            "strace_attach": SupportFile(
                "strace-attach",
                launcher_path,
                sha256(launcher_path),
                stat.S_IMODE(launcher_path.stat().st_mode),
            )
        }
        original_open_exclusive = globals()["_open_exclusive"]
        open_failure_pipes: list[int] = []

        def record_open_failure_pipe() -> tuple[int, int]:
            descriptors = original_pipe()
            open_failure_pipes.extend(descriptors)
            return descriptors

        os.pipe = record_open_failure_pipe
        globals()["_open_exclusive"] = lambda _path: (_ for _ in ()).throw(
            OSError("synthetic tracer stderr open failure")
        )
        try:
            RunnerOwnedProfileSession(
                plan=trace_plan,
                output_dir=output,
                ordinal=99_995,
                tools=trace_tools,
                support_files=trace_support,
                profile_adapter=_FakeProfiles,
                permission_result="not_applicable",
            )
        except OSError:
            checks["tracer_constructor_open_cleanup"] = (
                len(open_failure_pipes) == 2
                and descriptors_are_closed(open_failure_pipes)
            )
        else:
            checks["tracer_constructor_open_cleanup"] = False
        finally:
            os.pipe = original_pipe
            globals()["_open_exclusive"] = original_open_exclusive

        original_popen = subprocess.Popen
        popen_failure_descriptors: list[int] = []

        def record_popen_failure_pipe() -> tuple[int, int]:
            descriptors = original_pipe()
            popen_failure_descriptors.extend(descriptors)
            return descriptors

        def record_popen_failure_open(path: Path) -> int:
            descriptor = original_open_exclusive(path)
            popen_failure_descriptors.append(descriptor)
            return descriptor

        os.pipe = record_popen_failure_pipe
        globals()["_open_exclusive"] = record_popen_failure_open
        subprocess.Popen = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                OSError("synthetic tracer spawn failure")
            )
        )
        try:
            RunnerOwnedProfileSession(
                plan=trace_plan,
                output_dir=output,
                ordinal=99_994,
                tools=trace_tools,
                support_files=trace_support,
                profile_adapter=_FakeProfiles,
                permission_result="not_applicable",
            )
        except OSError:
            checks["tracer_constructor_spawn_cleanup"] = (
                len(popen_failure_descriptors) == 3
                and descriptors_are_closed(popen_failure_descriptors)
            )
        else:
            checks["tracer_constructor_spawn_cleanup"] = False
        finally:
            subprocess.Popen = original_popen
            os.pipe = original_pipe
            globals()["_open_exclusive"] = original_open_exclusive

        original_wait_exact = globals()["_wait_exact_process"]
        wait_failure_pipes: list[int] = []
        wait_failure_processes: list[subprocess.Popen[Any]] = []

        def record_wait_failure_pipe() -> tuple[int, int]:
            descriptors = original_pipe()
            wait_failure_pipes.extend(descriptors)
            return descriptors

        def reject_waiting_tracer(
            process: subprocess.Popen[Any],
            _executable: Executable,
            timeout: float = 5.0,
        ) -> dict[str, Any]:
            del timeout
            wait_failure_processes.append(process)
            raise RunnerFailure("synthetic tracer identity failure")

        os.pipe = record_wait_failure_pipe
        globals()["_wait_exact_process"] = reject_waiting_tracer
        try:
            RunnerOwnedProfileSession(
                plan=trace_plan,
                output_dir=output,
                ordinal=99_993,
                tools=trace_tools,
                support_files=trace_support,
                profile_adapter=_FakeProfiles,
                permission_result="not_applicable",
            )
        except RunnerFailure as error:
            identity_cleanup = {
                "pipe_count": len(wait_failure_pipes),
                "pipes_closed": descriptors_are_closed(wait_failure_pipes),
                "process_count": len(wait_failure_processes),
                "process_reaped": bool(
                    wait_failure_processes
                    and wait_failure_processes[0].poll() is not None
                ),
                "group_absent": bool(
                    wait_failure_processes
                    and not process_group_exists(wait_failure_processes[0].pid)
                ),
                "error": repr(error),
            }
            details["tracer_constructor_identity_cleanup"] = identity_cleanup
            checks["tracer_constructor_identity_cleanup"] = all(
                value
                for key, value in identity_cleanup.items()
                if key != "error"
            )
        else:
            checks["tracer_constructor_identity_cleanup"] = False
        finally:
            globals()["_wait_exact_process"] = original_wait_exact
            os.pipe = original_pipe

        attach_session = RunnerOwnedProfileSession(
            plan=trace_plan,
            output_dir=output,
            ordinal=99_998,
            tools=trace_tools,
            support_files=trace_support,
            profile_adapter=_FakeProfiles,
            permission_result="available;perf_event_paranoid=2;scope=user-only",
        )
        ptracer_pid = attach_session.environment_overrides()[
            "ASTERISM_REBASELINE_PTRACER_PID"
        ]
        tracee_code = (
            "import ctypes,os,time;"
            "libc=ctypes.CDLL(None,use_errno=True);"
            "libc.prctl(15,b'ast-fake',0,0,0);"
            "pid=int(os.environ['ASTERISM_REBASELINE_PTRACER_PID']);"
            "assert libc.prctl(0x59616d61,pid,0,0,0)==0;"
            "print('ready',flush=True);time.sleep(0.2)"
        )
        tracee = subprocess.Popen(
            (str(python), "-c", tracee_code),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={
                **os.environ,
                "ASTERISM_REBASELINE_PTRACER_PID": ptracer_pid,
            },
            start_new_session=True,
        )
        if tracee.stdout is None or tracee.stdout.readline() != "ready\n":
            raise RunnerFailure("synthetic tracee did not authorize launcher")
        tracee_identity = parse_proc_stat(tracee.pid)
        set_child_parked(tracee_identity, True)
        attach_session.bind_child(tracee_identity, control_fd=123)
        attach_session.capture_phase("boot", {})
        attached_identity = attach_session.helper_identities()[0]
        set_child_parked(tracee_identity, False)
        stdout, stderr = tracee.communicate(timeout=5)
        _, helper_record = attach_session._wait_helper()
        attach_session.finished = True
        checks["exact_pid_strace_attach"] = (
            tracee.returncode == 0
            and stdout == ""
            and stderr == ""
            and attached_identity["comm"] == "strace"
            and helper_record["exit_status"] == 0
            and helper_record["process_group_absent"] is True
        )
    except BaseException as error:
        details["exact_pid_strace_attach"] = repr(error)
        checks["exact_pid_strace_attach"] = False
    finally:
        if tracee is not None and tracee.poll() is None:
            terminate_process_group(tracee)
        if attach_session is not None:
            attach_session.abort()

    previous_coordination = os.environ.get("MESS_BENCH_COORDINATION_CONFIRMED")
    os.environ["MESS_BENCH_COORDINATION_CONFIRMED"] = "true"
    try:
        runner.acquire_lease()
        checks["lease_proc_proof"] = bool(
            runner.lease
            and runner.lease["second_exclusive_failed"]
            and "FLOCK" in runner.lease["proc_locks_proof"]
        )
        runner.host_resource_preflight()
        # The compact runner fixture intentionally omits the immutable prepared
        # input tree exercised above.  Still publish the two attempt-local
        # authority copies consumed by profile replay so this path uses the
        # same bindings as a production attempt.
        for source, destination in (
            (prepared.path, output / "prepared-artifacts.json"),
            (prepared.source_approval, output / "source-approval.json"),
        ):
            atomic_write(destination, source.read_bytes(), mode=0o444)
            runner.frozen_files[str(destination.resolve())] = sha256(destination)
        runner.claim_prepared()
        # Protocol v4: the claim is recorded in the run's own output
        # directory as run-claim.json (mode 0444) and the prepared root is
        # never written to, so prepared artifacts stay reusable across
        # rehearsals and declared runs.
        run_claim = output / "run-claim.json"
        checks["run_claim_in_output"] = (
            runner.run_claim_path == run_claim
            and run_claim.is_file()
            and stat.S_IMODE(run_claim.stat().st_mode) == 0o444
            and not prepared.claim_path.exists()
            and load_canonical_json(run_claim).get("prepared_artifacts_path")
            == str(prepared.path)
            and load_canonical_json(run_claim).get("prepared_artifacts_sha256")
            == prepared.digest
        )
        runner.prepare_profile_preflight()

        # Protocol v4 §3-4: exercise the run-mode admission gate in every
        # mode without a full measurement run.
        def _admission_runner(out: Path) -> "RebaselineRunner":
            scratch = root / f"admit-scratch-{out.name}"
            scratch.mkdir()
            return RebaselineRunner(
                prepared,
                out,
                _FakeSchema(),
                lock_path=root / f"admit-{out.name}.lock",
                scratch_root=scratch,
                minimum_free_bytes=0,
                minimum_free_inodes=0,
                maximum_load1=6.0,
                profile_factory=runner.profile_factory,
                require_profile_factory=False,
            )

        runner_hash = sha256(Path(__file__).resolve())
        variant_hashes = {
            name: variant.executable.sha256
            for name, variant in prepared.variants.items()
        }

        def _faithful_declaration() -> str:
            return f"runner={runner_hash}\n" + "".join(
                f"{name}={digest}\n"
                for name, digest in sorted(variant_hashes.items())
            )

        def _declared_dir(name: str, text: str | None) -> Path:
            out = root / name
            out.mkdir()
            if text is not None:
                (out / "DECLARED.txt").write_text(text)
            return out

        def _rejects(out: Path) -> bool:
            try:
                _admission_runner(out)._admit_run_mode()
            except RunnerFailure:
                return True
            return False

        # Rehearsal: fresh dir is created, marked, and needs no coordination.
        rehearsal_out = root / "rehearsal-admit"
        rehearsal_runner = _admission_runner(rehearsal_out)
        rehearsal_runner._admit_run_mode()
        checks["v4_rehearsal_admits_and_marks"] = (
            rehearsal_runner.rehearsal and rehearsal_out.is_dir()
        )

        checks["v4_accepted_requires_declared_dir"] = _rejects(
            root / "accepted-missing"
        )
        accepted_stray = _declared_dir("accepted-stray", _faithful_declaration())
        (accepted_stray / "stowaway").write_text("x")
        checks["v4_accepted_rejects_stray_file"] = _rejects(accepted_stray)
        checks["v4_accepted_rejects_wrong_digest"] = _rejects(
            _declared_dir(
                "accepted-wrong",
                f"runner={runner_hash}\n"
                + "".join(f"{n}={'0' * 64}\n" for n in sorted(variant_hashes)),
            )
        )
        # New hostile classes from cr-1e7dil: a correct digest embedded in a
        # larger token, a negating comment, and a symlinked declaration must
        # all be rejected by strict per-line parsing and the regular-file
        # requirement.
        checks["v4_accepted_rejects_embedded_token"] = _rejects(
            _declared_dir(
                "accepted-embedded",
                f"runner=X{runner_hash}\n"
                + "".join(
                    f"{n}={d}\n" for n, d in sorted(variant_hashes.items())
                ),
            )
        )
        checks["v4_accepted_rejects_negating_comment"] = _rejects(
            _declared_dir(
                "accepted-comment",
                f"runner={'0' * 64}\n"
                + "".join(f"{n}={'0' * 64}\n" for n in sorted(variant_hashes))
                + f"# real runner={runner_hash}\n",
            )
        )
        symlink_dir = root / "accepted-symlink"
        symlink_dir.mkdir()
        real_declaration = root / "outside-declaration.txt"
        real_declaration.write_text(_faithful_declaration())
        (symlink_dir / "DECLARED.txt").symlink_to(real_declaration)
        checks["v4_accepted_rejects_symlink_declaration"] = _rejects(symlink_dir)

        accepted_ok = _declared_dir("accepted-ok", _faithful_declaration())
        ok_runner = _admission_runner(accepted_ok)
        try:
            ok_runner._admit_run_mode()
        except RunnerFailure as error:
            checks["v4_accepted_admits_faithful_declaration"] = False
            details["v4_accepted_admits_faithful_declaration"] = error.reason
        else:
            checks["v4_accepted_admits_faithful_declaration"] = (
                ok_runner.declaration_sha256 is not None
                and bool(SHA256_RE.fullmatch(ok_runner.declaration_sha256))
                and str((accepted_ok / "DECLARED.txt").resolve())
                in ok_runner.frozen_files
            )

        cleanup_output = root / "post-spawn-cleanup-attempt"
        cleanup_scratch = root / "post-spawn-cleanup-scratch"
        cleanup_output.mkdir()
        cleanup_scratch.mkdir()
        cleanup_runner = RebaselineRunner(
            prepared,
            cleanup_output,
            _FakeSchema(),
            lock_path=root / "post-spawn-cleanup.lock",
            scratch_root=cleanup_scratch,
            minimum_free_bytes=0,
            minimum_free_inodes=0,
            maximum_load1=6.0,
            quiet_timeout_seconds=2,
            quiet_poll_seconds=1,
            load_reader=lambda: 0.1,
            sleep=lambda _seconds: None,
            monotonic=_AdvancingClock(),
            profile_factory=_FakeProfiles,
            schema_path=schema_file,
        )
        cleanup_runner.attempt_scratch.mkdir(parents=True)
        cleanup_runner.runtime_home.mkdir(mode=0o700)
        cleanup_runner._filesystem_identity = lambda: {  # type: ignore[method-assign]
            **fake_filesystem,
            "target": str(cleanup_scratch.resolve()),
        }
        cleanup_runner.host_resource_preflight()
        for source, destination in (
            (prepared.path, cleanup_output / "prepared-artifacts.json"),
            (prepared.source_approval, cleanup_output / "source-approval.json"),
        ):
            atomic_write(destination, source.read_bytes(), mode=0o444)
            cleanup_runner.frozen_files[str(destination.resolve())] = sha256(
                destination
            )

        wait_failure_plan = cleanup_runner._variant_plan(
            {
                "track": "primary",
                "row_ordinal": 6,
                "block": 1,
                "cell_ordinal": 1,
                "variant": "A",
                "durability": "Process",
                "payload_size": 24,
                "batch_size": 1,
                "writers": 1,
                "batches_per_writer": 1,
            },
            track="primary",
            store_path=cleanup_runner._fresh_store("primary", 6, "A"),
        )
        original_wait_for_quiet = cleanup_runner.wait_for_quiet
        original_socketpair = socket.socketpair
        wait_failure_sockets: list[socket.socket] = []

        def record_wait_failure_socketpair(
            *args: Any, **kwargs: Any
        ) -> tuple[socket.socket, socket.socket]:
            pair = original_socketpair(*args, **kwargs)
            wait_failure_sockets.extend(pair)
            return pair

        cleanup_runner.wait_for_quiet = lambda: (_ for _ in ()).throw(  # type: ignore[method-assign]
            RunnerFailure("synthetic immediate quiet-wait failure")
        )
        socket.socketpair = record_wait_failure_socketpair
        try:
            cleanup_runner.run_child(wait_failure_plan, controlled=True)
        except RunnerFailure:
            checks["pre_guard_socket_ownership"] = (
                not wait_failure_sockets
                or descriptors_are_closed(
                    owned.fileno() for owned in wait_failure_sockets
                )
            )
        else:
            checks["pre_guard_socket_ownership"] = False
        finally:
            cleanup_runner.wait_for_quiet = original_wait_for_quiet  # type: ignore[method-assign]
            socket.socketpair = original_socketpair
            for owned in wait_failure_sockets:
                owned.close()
            failed_context = cleanup_output / "contexts" / "00001.json"
            if failed_context.is_file():
                failed_context.unlink()

        def hostile_cleanup_runner(
            label: str, row_ordinal: int
        ) -> tuple[RebaselineRunner, ChildPlan]:
            case_output = root / f"{label}-attempt"
            case_scratch = root / f"{label}-scratch"
            case_output.mkdir()
            case_scratch.mkdir()
            case_runner = RebaselineRunner(
                prepared,
                case_output,
                _FakeSchema(),
                lock_path=root / f"{label}.lock",
                scratch_root=case_scratch,
                minimum_free_bytes=0,
                minimum_free_inodes=0,
                maximum_load1=6.0,
                quiet_timeout_seconds=2,
                quiet_poll_seconds=1,
                load_reader=lambda: 0.1,
                sleep=lambda _seconds: None,
                monotonic=_AdvancingClock(),
                profile_factory=_FakeProfiles,
                schema_path=schema_file,
            )
            case_runner.attempt_scratch.mkdir(parents=True)
            case_runner.runtime_home.mkdir(mode=0o700)
            case_runner._filesystem_identity = lambda: {  # type: ignore[method-assign]
                **fake_filesystem,
                "target": str(case_scratch.resolve()),
            }
            case_runner.host_resource_preflight()
            for source, destination in (
                (prepared.path, case_output / "prepared-artifacts.json"),
                (prepared.source_approval, case_output / "source-approval.json"),
            ):
                atomic_write(destination, source.read_bytes(), mode=0o444)
                case_runner.frozen_files[str(destination.resolve())] = sha256(
                    destination
                )
            case_runner.prepare_profile_preflight()
            case_plan = case_runner._variant_plan(
                {
                    "track": "primary",
                    "row_ordinal": row_ordinal,
                    "block": 1,
                    "cell_ordinal": 1,
                    "variant": "A",
                    "durability": "Process",
                    "payload_size": 24,
                    "batch_size": 1,
                    "writers": 1,
                    "batches_per_writer": 1,
                },
                track="primary",
                store_path=case_runner._fresh_store(
                    "primary", row_ordinal, "A"
                ),
            )
            return case_runner, case_plan

        close_runner, close_plan = hostile_cleanup_runner(
            "hostile-close-terminate", 7
        )
        original_popen = subprocess.Popen
        original_terminate_process_group = globals()["terminate_process_group"]
        original_close = os.close
        spawned_children: list[subprocess.Popen[Any]] = []
        child_parent_descriptors: list[int] = []
        close_armed = False
        close_failed = False
        terminate_calls = 0

        def arm_child_close(*args: Any, **kwargs: Any) -> subprocess.Popen[Any]:
            nonlocal close_armed
            process = original_popen(*args, **kwargs)
            spawned_children.append(process)
            child_parent_descriptors.extend((kwargs["stdout"], kwargs["stderr"]))
            close_armed = True
            return process

        def fail_child_close_once(descriptor: int) -> None:
            nonlocal close_failed
            if close_armed and not close_failed:
                close_failed = True
                raise OSError("synthetic post-spawn descriptor close failure")
            original_close(descriptor)

        def fail_child_termination_once(process: subprocess.Popen[Any]) -> int:
            nonlocal terminate_calls
            terminate_calls += 1
            if terminate_calls == 1:
                raise OSError("synthetic child termination failure")
            return original_terminate_process_group(process)

        mask_before_hostile = signal.pthread_sigmask(signal.SIG_BLOCK, set())
        subprocess.Popen = arm_child_close  # type: ignore[method-assign]
        os.close = fail_child_close_once
        globals()["terminate_process_group"] = fail_child_termination_once
        try:
            close_runner.run_child(close_plan, controlled=True)
        except RunnerFailure as error:
            mask_after_hostile = signal.pthread_sigmask(signal.SIG_BLOCK, set())
            details["run_child_close_terminate_cleanup"] = repr(error)
            checks["run_child_close_terminate_cleanup"] = all(
                (
                    close_failed,
                    terminate_calls == 2,
                    descriptors_are_closed(child_parent_descriptors),
                    mask_after_hostile == mask_before_hostile,
                    close_runner.active_child is None,
                    close_runner.active_executable is None,
                    all(
                        process.poll() is not None
                        and not process_group_exists(process.pid)
                        for process in spawned_children
                    ),
                )
            )
        else:
            checks["run_child_close_terminate_cleanup"] = False
        finally:
            subprocess.Popen = original_popen
            os.close = original_close
            globals()["terminate_process_group"] = original_terminate_process_group

        final_runner, final_plan = hostile_cleanup_runner(
            "hostile-final-cleanup", 8
        )
        original_socketpair = socket.socketpair
        original_sigmask = signal.pthread_sigmask
        final_parent_wrappers: list[Any] = []
        sigmask_restore_calls = 0
        sigmask_failed = False

        class CloseOnceSocket:
            def __init__(self, inner: socket.socket) -> None:
                self.inner = inner
                self.close_calls = 0
                self.failed = False

            def __getattr__(self, name: str) -> Any:
                return getattr(self.inner, name)

            def close(self) -> None:
                self.close_calls += 1
                if not self.failed:
                    self.failed = True
                    raise OSError("synthetic final socket close failure")
                self.inner.close()

        def hostile_final_socketpair(
            *args: Any, **kwargs: Any
        ) -> tuple[Any, socket.socket]:
            parent, child_socket = original_socketpair(*args, **kwargs)
            wrapped = CloseOnceSocket(parent)
            final_parent_wrappers.append(wrapped)
            return wrapped, child_socket

        def hostile_final_sigmask(how: int, mask: Any) -> set[signal.Signals]:
            nonlocal sigmask_restore_calls, sigmask_failed
            if how == signal.SIG_SETMASK:
                sigmask_restore_calls += 1
                if sigmask_restore_calls == 2 and not sigmask_failed:
                    sigmask_failed = True
                    raise OSError("synthetic final signal-mask restore failure")
            return original_sigmask(how, mask)

        mask_before_final = original_sigmask(signal.SIG_BLOCK, set())
        socket.socketpair = hostile_final_socketpair
        signal.pthread_sigmask = hostile_final_sigmask
        try:
            final_record = final_runner.run_child(final_plan, controlled=True)
            mask_after_final = original_sigmask(signal.SIG_BLOCK, set())
            checks["run_child_final_cleanup_retry"] = all(
                (
                    final_record["exit_status"] == 0,
                    sigmask_failed,
                    sigmask_restore_calls == 3,
                    mask_after_final == mask_before_final,
                    len(final_parent_wrappers) == 1,
                    final_parent_wrappers[0].close_calls == 2,
                    final_parent_wrappers[0].inner.fileno() == -1,
                    final_runner.active_child is None,
                )
            )
        except BaseException as error:
            details["run_child_final_cleanup_retry"] = repr(error)
            checks["run_child_final_cleanup_retry"] = False
        finally:
            socket.socketpair = original_socketpair
            signal.pthread_sigmask = original_sigmask
            for wrapped in final_parent_wrappers:
                if wrapped.inner.fileno() != -1:
                    wrapped.inner.close()

        (cleanup_output / "active-child.json").mkdir()
        mask_before = signal.pthread_sigmask(signal.SIG_BLOCK, set())
        fake_processes_before = comm_identities("ast-fake")
        cleanup_plan = cleanup_runner._variant_plan(
            {
                "track": "primary",
                "row_ordinal": 5,
                "block": 1,
                "cell_ordinal": 1,
                "variant": "A",
                "durability": "Process",
                "payload_size": 24,
                "batch_size": 1,
                "writers": 1,
                "batches_per_writer": 1,
            },
            track="primary",
            store_path=cleanup_runner._fresh_store("primary", 5, "A"),
        )
        try:
            cleanup_runner.run_child(cleanup_plan, controlled=True)
        except RunnerFailure as error:
            mask_after = signal.pthread_sigmask(signal.SIG_BLOCK, set())
            details["post_spawn_publication_cleanup"] = repr(error)
            checks["post_spawn_publication_cleanup"] = all(
                (
                    "publish active child marker" in error.reason,
                    cleanup_runner.active_child is None,
                    cleanup_runner.active_executable is None,
                    cleanup_runner.active_helpers == [],
                    mask_after == mask_before,
                    comm_identities("ast-fake") == fake_processes_before,
                )
            )
        else:
            checks["post_spawn_publication_cleanup"] = False
        finally:
            shutil.rmtree(cleanup_output)
            shutil.rmtree(cleanup_scratch)

        context = {
            "track": "primary",
            "row_ordinal": 1,
            "block": 1,
            "cell_ordinal": 1,
            "variant": "A",
            "durability": "Process",
            "payload_size": 24,
            "batch_size": 1,
            "writers": 1,
            "batches_per_writer": 1,
        }
        plan = runner._variant_plan(
            context,
            track="primary",
            store_path=runner._fresh_store("primary", 1, "A"),
        )
        record = runner.run_child(plan, controlled=True)
        checks["synthetic_state_machine"] = all(
            (
                record["exit_status"] == 0,
                record["process_group_absent"],
                record["reaping"]["status"] in {"absent", "pid_reused"},
                len(record["control_events"]) == 10,
                record["csv_append"]["rows_after"] == 1,
                record["raw_mode_after"] == 0o444,
            )
        )

        bad = runner._variant_plan(
            {**context, "row_ordinal": 2},
            track="primary",
            store_path=runner._fresh_store("primary", 2, "A"),
        )
        try:
            runner.run_child(bad, controlled=True)
        except RunnerFailure as error:
            details["bad_phase_fail_stop"] = repr(error)
            checks["bad_phase_fail_stop"] = runner.active_child is None
        else:
            checks["bad_phase_fail_stop"] = False

        nonzero = runner._variant_plan(
            {**context, "row_ordinal": 3},
            track="primary",
            store_path=runner._fresh_store("primary", 3, "A"),
        )
        try:
            runner.run_child(nonzero, controlled=True)
        except RunnerFailure as error:
            details["nonzero_fail_stop"] = repr(error)
            checks["nonzero_fail_stop"] = runner.active_child is None
        else:
            checks["nonzero_fail_stop"] = False

        extra = runner._variant_plan(
            {**context, "row_ordinal": 30},
            track="primary",
            store_path=runner._fresh_store("primary", 30, "A"),
        )
        try:
            runner.run_child(extra, controlled=True)
        except RunnerFailure as error:
            details["extra_raw_record_rejected"] = repr(error)
            extra_record = json.loads(
                runner.child_manifest.read_text().splitlines()[-1]
            )
            checks["extra_raw_record_rejected"] = (
                "cardinality" in str(extra_record.get("validation_error"))
                and extra_record.get("process_group_absent") is True
            )
        else:
            checks["extra_raw_record_rejected"] = False

        leaked = runner._variant_plan(
            {**context, "row_ordinal": 4},
            track="primary",
            store_path=runner._fresh_store("primary", 4, "A"),
        )
        try:
            runner.run_child(leaked, controlled=True)
        except RunnerFailure as error:
            details["leaked_group_killed"] = repr(error)
            last_child = json.loads(
                runner.child_manifest.read_text().splitlines()[-1]
            )
            checks["leaked_group_killed"] = (
                last_child.get("orphan_process_group_detected") is True
                and last_child.get("process_group_absent") is True
                and last_child.get("terminated_by_runner") is True
            )
        else:
            checks["leaked_group_killed"] = False

        stale = scratch / "already-exists"
        stale.mkdir()
        try:
            runner.resource_guard("stale", [stale])
        except RunnerFailure:
            checks["fresh_identity_negative"] = True
        else:
            checks["fresh_identity_negative"] = False

        original_floor = runner.minimum_free_bytes
        runner.minimum_free_bytes = 1 << 100
        try:
            runner.resource_guard("space-floor")
        except RunnerFailure:
            checks["free_space_negative"] = True
        else:
            checks["free_space_negative"] = False
        runner.minimum_free_bytes = original_floor

        old_reader = runner.load_reader
        old_timeout = runner.quiet_timeout_seconds
        runner.load_reader = lambda: 7.0
        runner.quiet_timeout_seconds = 1
        try:
            runner.wait_for_quiet()
        except RunnerFailure:
            checks["load_timeout_negative"] = True
        else:
            checks["load_timeout_negative"] = False
        runner.load_reader = old_reader
        runner.quiet_timeout_seconds = old_timeout

        guard_records = [
            json.loads(line) for line in runner.guard_manifest.read_text().splitlines()
        ]
        checks["guard_manifest_exact_child_pairs"] = (
            len(guard_records) == 2 * runner.child_count
            and all(
                record["ordinal"] == ordinal
                and record["label"].endswith("-pre" if ordinal % 2 else "-post")
                for ordinal, record in enumerate(guard_records, start=1)
            )
        )

        foreign = subprocess.Popen(
            [
                str(python),
                "-c",
                "import ctypes,time;ctypes.CDLL(None).prctl(15,b'ast-fake',0,0,0);time.sleep(30)",
            ]
        )
        foreign_deadline = time.monotonic() + 2
        while time.monotonic() < foreign_deadline:
            try:
                if parse_proc_stat(foreign.pid)["comm"] == "ast-fake":
                    break
            except FileNotFoundError:
                break
            time.sleep(0.01)
        try:
            runner.snapshot_processes("foreign-negative")
        except RunnerFailure:
            checks["foreign_process_negative"] = True
        else:
            checks["foreign_process_negative"] = False
        finally:
            foreign.terminate()
            foreign.wait(timeout=10)
    finally:
        runner.release_lease("SELF_TEST")
        if previous_coordination is None:
            os.environ.pop("MESS_BENCH_COORDINATION_CONFIRMED", None)
        else:
            os.environ["MESS_BENCH_COORDINATION_CONFIRMED"] = previous_coordination

    terminal_fixture = root / "terminal-filename-fixture"
    terminal_fixture.mkdir()
    original_output = runner.output
    runner.output = terminal_fixture
    try:
        atomic_write(terminal_fixture / "REPORT.md", b"# synthetic report\n", mode=0o444)
        atomic_json(terminal_fixture / "result.json", {"outcome": "ADMIT"})
        hidden = terminal_fixture / ".leftover.tmp"
        atomic_write(hidden, b"temporary")
        try:
            runner._artifact_inventory()
        except RunnerFailure:
            checks["hidden_artifact_rejected"] = True
        else:
            checks["hidden_artifact_rejected"] = False
        hidden.unlink()
        symlink = terminal_fixture / "result-alias.json"
        symlink.symlink_to(terminal_fixture / "result.json")
        try:
            runner._artifact_inventory()
        except RunnerFailure:
            checks["artifact_symlink_rejected"] = True
        else:
            checks["artifact_symlink_rejected"] = False
        symlink.unlink()
        fsync_dir(terminal_fixture)
        try:
            runner._publish_sha256sums(runner._artifact_inventory())
        except RunnerFailure:
            checks["sha_requires_evaluator_transition"] = True
        else:
            checks["sha_requires_evaluator_transition"] = False
        atomic_json(
            terminal_fixture / "evaluator-transition.json",
            {"schema": "synthetic-transition-v3"},
        )
        underscore_alias = terminal_fixture / "terminal_pre_release.json"
        atomic_json(underscore_alias, {"forbidden": True})
        try:
            runner._publish_sha256sums(runner._artifact_inventory())
        except RunnerFailure:
            checks["terminal_underscore_alias_rejected"] = True
        else:
            checks["terminal_underscore_alias_rejected"] = False
        underscore_alias.unlink()
        fsync_dir(terminal_fixture)
        inventory = runner._artifact_inventory()
        sums_path = runner._publish_sha256sums(inventory)
        expected_sums = b"".join(
            f"{item['sha256']}  {item['path']}\n".encode("utf-8")
            for item in inventory
        )
        checks["sha_sorted_nonterminal_inventory"] = (
            sums_path.name == "SHA256SUMS"
            and sums_path.read_bytes() == expected_sums
            and stat.S_IMODE(sums_path.stat().st_mode) == 0o444
            and not (terminal_fixture / "terminal_pre_release.json").exists()
            and not (terminal_fixture / "lease_release.json").exists()
        )
    finally:
        runner.output = original_output

    runner.close_semantic_manifests()
    report = {
        "schema": "bn-2l3n-runner-self-test-v3",
        "protocol": PROTOCOL,
        "outcome": "SELF_TEST_PASS" if all(checks.values()) else "SELF_TEST_FAILED",
        "checks": checks,
        "details": details,
        "check_count": len(checks),
        "runner_path": str(Path(__file__).resolve()),
        "runner_sha256": sha256(Path(__file__).resolve()),
        "completed_at": now(),
    }
    atomic_json(root / "self-test.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if all(checks.values()) else 30


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="run the frozen bn-2l3n four-variant rebaseline"
    )
    parser.add_argument("prepared_artifacts", type=Path)
    parser.add_argument("output_dir", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    if raw[:1] == ["--semantic-authority-self-test"]:
        if len(raw) != 2:
            print(
                "usage: run_rebaseline.sh --semantic-authority-self-test <fresh-dir>",
                file=sys.stderr,
            )
            return 2
        try:
            return run_semantic_authority_self_test(Path(raw[1]).resolve())
        except BaseException as error:
            print(
                f"semantic authority self-test failed internally: {error}",
                file=sys.stderr,
            )
            return 30
    if raw[:1] == ["--self-test"]:
        if len(raw) != 2:
            print("usage: run_rebaseline.sh --self-test <fresh-dir>", file=sys.stderr)
            return 2
        try:
            return run_self_test(Path(raw[1]).resolve())
        except BaseException as error:
            print(f"runner self-test failed internally: {error}", file=sys.stderr)
            return 30
    args = parse_args(raw)
    try:
        set_process_comm(RUNNER_COMM)
        schema_path = Path(__file__).with_name("evidence_schema.py").resolve()
        schema = load_schema()
        prepared = load_prepared(args.prepared_artifacts, schema)
        # Protocol v4 §6: the executing script is bound by recording its
        # hash in provenance (and, for accepted runs, matching DECLARED.txt)
        # rather than by requiring a specific frozen inode and interpreter.
        # verify_current_runtime's exe-identity fail-stop is retired.
        approval = prepared.source_approval_value
        if (
            approval.get("tooling_commit") != prepared.value["tooling_commit"]
            or approval.get("tooling_tree") != prepared.value["tooling_tree"]
        ):
            raise RunnerFailure("prepared tooling approval binding differs", exit_code=2)
        required_tools = {
            "correctness",
            "fault",
            *RunnerOwnedProfileTools.REQUIRED,
            "runner_runtime",
            "evaluator_runtime",
            "terminal_verifier_runtime",
        }
        if not required_tools.issubset(prepared.tools):
            raise RunnerFailure(
                f"prepared tools omit {sorted(required_tools - set(prepared.tools))}",
                exit_code=2,
            )
        output = args.output_dir.resolve()
        scratch = SCRATCH_ROOT.resolve()
        # Protocol v4 §3-4: rehearsal outputs must be fresh (runner-created);
        # accepted outputs are operator-created and hold only DECLARED.txt.
        # Full admission happens in RebaselineRunner._admit_run_mode.
        if output.is_symlink():
            raise RunnerFailure(f"output directory is a symlink: {output}", exit_code=2)
        if output.name.startswith("rehearsal-") and output.exists():
            raise RunnerFailure(
                f"rehearsal output directory is not fresh: {output}", exit_code=2
            )
        if output == prepared.root or prepared.root in output.parents:
            raise RunnerFailure("output is inside prepared artifacts", exit_code=2)
        try:
            output.relative_to(scratch)
        except ValueError as error:
            raise RunnerFailure(
                f"output must be under {scratch}: {output}", exit_code=2
            ) from error
        output.parent.mkdir(parents=True, exist_ok=True)
        runner = RebaselineRunner(
            prepared,
            output,
            schema,
            schema_path=schema_path,
            profile_tool_driver=RunnerOwnedProfileTools(),
        )
        return runner.run()
    except RunnerFailure as error:
        print(f"fail-stop: {error.reason}", file=sys.stderr)
        return error.exit_code
    except (OSError, subprocess.SubprocessError) as error:
        print(f"preflight failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
