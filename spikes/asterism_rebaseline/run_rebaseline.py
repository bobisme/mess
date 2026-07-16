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
from pathlib import Path
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
    path: Path
    digest: str
    value: dict[str, Any]
    root: Path
    source_approval: Path
    source_approval_sha256: str
    source_approval_value: dict[str, Any]
    tools_manifest: SupportFile
    tools_manifest_value: dict[str, Any]
    claim_path: Path
    variants: dict[str, Variant]
    tools: dict[str, Executable]
    support_files: dict[str, SupportFile]
    inputs: dict[str, SupportFile]
    tracked_comm: frozenset[str]


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
    observed_sha256 = sha256_bytes(canonical_json_bytes(value))
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
    value = load_canonical_json(path)
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
    source_approval_value = load_canonical_json(source_approval)
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
    tools_manifest_value = load_canonical_json(tools_manifest.path)
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
    claim_binding = value["single_use_claim"]
    require_exact_keys(claim_binding, {"path"}, "single-use claim")
    claim_lexical = Path(str(claim_binding["path"]))
    if not claim_lexical.is_absolute():
        raise RunnerFailure("single-use claim path is not absolute", exit_code=2)
    claim_path = claim_lexical.resolve()
    claims_directory = claim_path.parent
    if (
        claims_directory.name != "claims"
        or claims_directory.parent != root
        or not claims_directory.is_dir()
        or stat.S_IMODE(claims_directory.stat().st_mode) != 0o700
        or stat.S_IMODE(root.stat().st_mode) != 0o555
        or root not in claim_path.parents
        or claim_path.exists()
        or claim_path.is_symlink()
    ):
        raise RunnerFailure(
            "prepared claims/root layout differs or artifacts are already consumed",
            exit_code=2,
        )
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
        path=path,
        digest=sha256(path),
        value=value,
        root=root,
        source_approval=source_approval,
        source_approval_sha256=str(approval_binding["sha256"]),
        source_approval_value=source_approval_value,
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
    ready, _, _ = select.select([descriptor], [], [], timeout)
    if not ready:
        raise RunnerFailure("perf control acknowledgement timed out")
    payload = bytearray()
    while not payload.endswith(b"\n"):
        chunk = os.read(descriptor, 1)
        if not chunk:
            raise RunnerFailure("perf control acknowledgement closed early")
        payload.extend(chunk)
        if len(payload) > 16:
            raise RunnerFailure("perf control acknowledgement is oversized")
    if bytes(payload) != b"ack\n":
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
            ack = _read_ack_line(self.ack_read)
            received = time.monotonic_ns()
            _write_all(self.ack_ledger_fd, ack, "perf runner ACK ledger")
            os.fsync(self.ack_ledger_fd)
            self.perf_control_events.append(
                {
                    "command": "enable",
                    "nonce": start_nonce,
                    "sent_monotonic_ns": sent,
                    "ack": ack.rstrip(b"\n").decode("ascii"),
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
        self.frozen_files = self._frozen_file_bindings()
        self.config = config_for(prepared, schema)
        self.config["attempt_nonce"] = self.attempt_nonce
        self.config_path = output / "config.json"
        self.provenance_path = output / "provenance.json"
        self.guard_manifest = output / "guard-manifest.jsonl"
        self.child_manifest = output / "child-manifest.jsonl"
        self.raw_manifest = output / "raw-manifest.json"
        self.resource_manifest = output / "resource-manifest.jsonl"
        self.correctness_manifest = output / "correctness-manifest.jsonl"
        self.auxiliary_manifest = output / "auxiliary-manifest.jsonl"
        self.lease_manifest = output / "lease-manifest.jsonl"
        self.attempt_scratch = scratch_root / "attempts" / self.attempt_nonce
        self.runtime_home = self.attempt_scratch / "home"
        self.initial_filesystem: dict[str, Any] | None = None
        self.initial_free_bytes: int | None = None
        self.initial_free_inodes: int | None = None
        self.prepared_tree_snapshot = (
            self._prepared_tree_state() if self.prepared.inputs else None
        )
        self.csv_paths = {
            track: output / filename for track, filename in CSV_FILENAMES.items()
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
        return values

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
        self._verify_prepared_input_metadata()
        for raw_path, expected in self.frozen_files.items():
            path = Path(raw_path)
            if not path.is_file() or sha256(path) != expected:
                raise RunnerFailure(f"frozen artifact changed: {path}")
        if self.prepared.claim_path.is_file() and self.claim is not None:
            metadata = self.prepared.claim_path.lstat()
            if (
                stat.S_ISLNK(metadata.st_mode)
                or not stat.S_ISREG(metadata.st_mode)
                or stat.S_IMODE(metadata.st_mode) != 0o444
                or load_canonical_json(self.prepared.claim_path) != self.claim
            ):
                raise RunnerFailure("single-use claim changed")

    def _verify_prepared_input_metadata(self) -> None:
        bindings: list[tuple[Path, str, int]] = [
            (self.prepared.path, "prepared artifacts", 0o444),
            (self.prepared.source_approval, "source approval", 0o444),
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
        if self.prepared.inputs:
            claims_directory = self.prepared.claim_path.parent
            if (
                stat.S_IMODE(self.prepared.root.stat().st_mode) != 0o555
                or claims_directory.name != "claims"
                or claims_directory.parent != self.prepared.root
                or stat.S_IMODE(claims_directory.stat().st_mode) != 0o700
            ):
                raise RunnerFailure("prepared root/claims immutable layout changed")

    def _prepared_tree_state(self) -> str:
        root = self.prepared.root.resolve(strict=True)
        claims = self.prepared.claim_path.parent.resolve(strict=True)
        entries: list[dict[str, Any]] = []
        paths = [root, *sorted(root.rglob("*"))]
        for path in paths:
            resolved = path.resolve(strict=False)
            if path != claims and claims in resolved.parents:
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

    def acquire_lease(self) -> None:
        self.phase = "lease_acquire"
        if os.environ.get("MESS_BENCH_COORDINATION_CONFIRMED") != "true":
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
        atomic_create_json(self.prepared.claim_path, self.claim, mode=0o444)
        self.frozen_files[str(self.prepared.claim_path.resolve(strict=True))] = sha256(
            self.prepared.claim_path
        )

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
            "attempt_nonce": self.attempt_nonce,
            "output_dir": str(self.output.resolve()),
            "output_dir_absent_before": True,
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
            self.run_child(plan, controlled=True)
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
            "claim_path": str(self.prepared.claim_path) if self.claim else None,
            "claim_sha256": (
                sha256(self.prepared.claim_path)
                if self.claim and self.prepared.claim_path.is_file()
                else None
            ),
            "failed_at": now(),
            "failed_monotonic_ns": time.monotonic_ns(),
            "artifact_inventory": self._artifact_inventory(),
        }
        atomic_json(self.output / "failure.json", value)

    def run(self) -> int:
        def interrupted(signum: int, _frame: Any) -> None:
            raise RunnerFailure(f"runner received signal {signum}")

        signal.signal(signal.SIGINT, interrupted)
        signal.signal(signal.SIGTERM, interrupted)
        try:
            self.output.mkdir(parents=False, exist_ok=False)
            self.scratch_root.mkdir(parents=True, exist_ok=True)
            self.attempt_scratch.mkdir(parents=True, exist_ok=False)
            self.runtime_home.mkdir(mode=0o700)
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
            self.publish_terminal(evaluator_transition, result)
            return 0
        except RunnerFailure as failure:
            try:
                self.write_failure(failure)
            finally:
                self.release_lease("INCONCLUSIVE")
            self.log(f"fail-stop: {failure.reason}")
            return failure.exit_code
        except BaseException as error:
            failure = RunnerFailure(
                f"unhandled runner failure: {error.__class__.__name__}: {error}",
                exit_code=30,
            )
            try:
                self.write_failure(failure)
            finally:
                self.release_lease("INCONCLUSIVE")
            self.log(f"fail-stop: {failure.reason}")
            return failure.exit_code


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


def _fixture_prepared(root: Path, executable: Path) -> Prepared:
    digest = sha256(executable)
    bound = Executable("fake", executable, digest, stat.S_IMODE(executable.stat().st_mode), "ast-fake")
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
    atomic_json(tools_manifest, tools_manifest_value)
    atomic_json(approval, {"status": "approved", "review_id": "fixture-review"})
    atomic_json(manifest, {"fixture": True})
    tools_manifest.chmod(0o444)
    bindings.chmod(0o555)
    approval.chmod(0o444)
    manifest.chmod(0o444)
    return Prepared(
        path=manifest,
        digest=sha256(manifest),
        value={"tooling_commit": "1" * 40, "tooling_tree": "2" * 40},
        root=root,
        source_approval=approval,
        source_approval_sha256=sha256(approval),
        source_approval_value={"status": "approved", "review_id": "fixture-review"},
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
        try:
            atomic_create_json(prepared.claim_path, {"again": True})
        except RunnerFailure:
            checks["single_use_claim"] = (
                stat.S_IMODE(prepared.claim_path.stat().st_mode) == 0o444
                and load_canonical_json(prepared.claim_path).get(
                    "prepared_artifacts_path"
                )
                == str(prepared.path)
                and load_canonical_json(prepared.claim_path).get(
                    "prepared_artifacts_sha256"
                )
                == prepared.digest
            )
        else:
            checks["single_use_claim"] = False
        runner.prepare_profile_preflight()

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
        runtime = prepared.tools.get("runner_runtime")
        if runtime is None:
            raise RunnerFailure("prepared runner runtime is absent", exit_code=2)
        verify_current_runtime(runtime, RUNNER_COMM)
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
        if output.exists() or output.is_symlink():
            raise RunnerFailure(f"output directory is not fresh: {output}", exit_code=2)
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
