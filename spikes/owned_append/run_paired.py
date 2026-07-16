#!/usr/bin/env python3
"""Hardened Process-only owned-append measurement orchestrator.

The shell entry point is intentionally a tiny, frozen wrapper.  This module
owns process identities, the cooperative host lease, durable evidence, and the
one-way evaluator transition.  It never builds either benchmark artifact.
"""

from __future__ import annotations

import argparse
import csv
import fcntl
import hashlib
import importlib.util
import io
import json
import os
import re
import secrets
import shlex
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


PROTOCOL = "bn-22it-process-owned-v1"
PAIR_SCHEMA = "bn-22it-prepared-pair-v2"
ATTESTATION_SCHEMA = "bn-22it-build-attestation-v2"
PAIR_CLAIM_SCHEMA = "bn-22it-pair-consumption-v1"
FORBIDDEN_COMM = frozenset(
    {"cargo", "rustc", "cc", "ld", "collect2", "owned_append_be"}
)
BATCHES = (1, 10, 100, 1000)
CYCLES = range(1, 6)
PROCESS_BPW = {1: 40_000, 10: 12_500, 100: 2_500, 1000: 250}
EXPECTED_GUARDS = 161
EXPECTED_CHILDREN = 80
LOCK_PATH = Path.home() / ".cache/mess-bench/global-measurement.lock"
CSV_FIELDS = (
    "variant",
    "source",
    "binary_sha256",
    "cycle",
    "slot",
    "mode",
    "batch",
    "writers",
    "bpw",
    "payload",
    "events",
    "ev_s",
    "p50_us",
    "p99_us",
    "allocs",
    "alloc_bytes",
    "allocs_per_event",
    "alloc_bytes_per_event",
    "owned_batches",
    "owned_records",
    "owned_payload_bytes",
    "borrowed_batches",
    "borrowed_records",
    "copied_records",
    "copied_bytes",
    "batches",
    "groups",
    "fsyncs",
    "fsync_p99_ns",
    "fsync_degraded",
    "pre_load1",
    "post_load1",
)


class RunnerFailure(Exception):
    """A classified, evidence-bearing runner failure."""

    def __init__(
        self,
        reason: str,
        *,
        exit_code: int = 20,
        child_rc: int | None = None,
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.exit_code = exit_code
        self.child_rc = child_rc


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_prefix(path: Path | None, length: int) -> str:
    digest = hashlib.sha256()
    if path is None or length == 0:
        return digest.hexdigest()
    with path.open("rb") as handle:
        remaining = length
        while remaining:
            chunk = handle.read(min(remaining, 1024 * 1024))
            if not chunk:
                raise RunnerFailure(
                    f"cannot hash {length}-byte prefix of {path}: short read"
                )
            digest.update(chunk)
            remaining -= len(chunk)
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
    payload = canonical_json(value)
    atomic_write(path, payload)


def atomic_create_json(path: Path, value: Any) -> None:
    """Atomically publish canonical JSON only when the destination is absent."""

    ready = path.with_name(
        f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}.ready"
    )
    atomic_write(ready, canonical_json(value))
    try:
        os.link(ready, path)
        fsync_dir(path.parent)
    except FileExistsError as error:
        raise RunnerFailure(f"single-use artifact already exists: {path}") from error
    finally:
        try:
            ready.unlink()
            fsync_dir(path.parent)
        except FileNotFoundError:
            pass


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode() + b"\n"


def load_canonical_object(path: Path) -> dict[str, Any]:
    try:
        payload = path.read_bytes()
        value = json.loads(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RunnerFailure(f"cannot read JSON {path}: {error}", exit_code=2) from error
    if not isinstance(value, dict) or payload != canonical_json(value):
        raise RunnerFailure(f"{path} is not a canonical JSON object", exit_code=2)
    return value


def load_canonical_json(path: Path, schema: str) -> dict[str, Any]:
    value = load_canonical_object(path)
    if value.get("schema") != schema or value.get("protocol") != PROTOCOL:
        raise RunnerFailure(f"{path} schema/protocol mismatch", exit_code=2)
    return value


def parse_aware_timestamp(value: Any, context: str) -> datetime:
    try:
        observed = datetime.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise RunnerFailure(f"{context} timestamp is invalid", exit_code=2) from error
    if observed.tzinfo is None:
        raise RunnerFailure(f"{context} timestamp lacks timezone", exit_code=2)
    return observed


def require_exact_keys(
    value: dict[str, Any], expected: set[str], context: str
) -> None:
    observed = set(value)
    if observed != expected:
        raise RunnerFailure(
            f"{context} keys differ: missing={sorted(expected - observed)} "
            f"extra={sorted(observed - expected)}",
            exit_code=2,
        )


def validate_completed_child_record(
    record: Any,
    *,
    context: str,
    argv: list[str],
    cwd: Path,
    output_path: Path,
) -> None:
    if not isinstance(record, dict):
        raise RunnerFailure(f"{context} is not an object", exit_code=2)
    require_exact_keys(
        record,
        {
            "argv",
            "cwd",
            "pid",
            "starttime",
            "waited_pid",
            "started_at",
            "completed_at",
            "started_monotonic_ns",
            "completed_monotonic_ns",
            "exit_status",
            "timed_out",
            "terminated_by_runner",
            "reaping",
            "process_group_absent",
            "output_path",
            "output_sha256",
        },
        context,
    )
    started = parse_aware_timestamp(record.get("started_at"), f"{context} start")
    completed = parse_aware_timestamp(
        record.get("completed_at"), f"{context} completion"
    )
    pid = record.get("pid")
    starttime = record.get("starttime")
    started_ns = record.get("started_monotonic_ns")
    completed_ns = record.get("completed_monotonic_ns")
    reaping = record.get("reaping")
    valid_reaping = bool(
        isinstance(reaping, dict)
        and reaping.get("status") in {"absent", "pid_reused"}
        and reaping.get("pid") == pid
        and reaping.get("starttime") == starttime
    )
    checks = (
        (record.get("argv") == argv, "argv"),
        (record.get("cwd") == str(cwd), "cwd"),
        (isinstance(pid, int) and pid > 0, "pid"),
        (isinstance(starttime, int) and starttime > 0, "starttime"),
        (record.get("waited_pid") == pid, "waited PID"),
        (completed >= started, "wall chronology"),
        (
            isinstance(started_ns, int)
            and isinstance(completed_ns, int)
            and started_ns > 0
            and completed_ns >= started_ns,
            "monotonic chronology",
        ),
        (record.get("exit_status") == 0, "exit status"),
        (record.get("timed_out") is False, "timeout"),
        (record.get("terminated_by_runner") is False, "termination"),
        (record.get("process_group_absent") is True, "process group"),
        (valid_reaping, "reaping"),
        (record.get("output_path") == str(output_path), "output path"),
        (record.get("output_sha256") == sha256(output_path), "output hash"),
    )
    for valid, label in checks:
        if not valid:
            raise RunnerFailure(f"{context} {label} mismatch", exit_code=2)


def load_prepare_module() -> Any:
    path = Path(__file__).with_name("prepare_paired.py").resolve()
    spec = importlib.util.spec_from_file_location("bn22_prepare", path)
    if spec is None or spec.loader is None:
        raise RunnerFailure(f"cannot load prepare implementation {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    previous = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = previous
    return module


def append_jsonl(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode() + b"\n"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def jsonl_records(path: Path) -> int:
    with path.open("rb") as handle:
        return sum(1 for line in handle if line.strip())


def command_output(argv: list[str], cwd: Path | None = None) -> str:
    result = subprocess.run(
        argv,
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result.stdout.strip()


def process_identity(pid: int) -> dict[str, Any]:
    proc = Path("/proc") / str(pid)
    stat = (proc / "stat").read_text()
    opened = stat.find("(")
    close = stat.rfind(")")
    if opened < 0 or close <= opened:
        raise OSError(f"malformed {proc / 'stat'}")
    rest = stat[close + 2 :].split()
    if len(rest) <= 19:
        raise OSError(f"short {proc / 'stat'}")
    return {
        "pid": pid,
        "comm": stat[opened + 1 : close],
        "state": rest[0],
        "ppid": int(rest[1]),
        "starttime_ticks": int(rest[19]),
    }


def read_process(pid: int) -> tuple[dict[str, Any], list[str], bool]:
    proc = Path("/proc") / str(pid)
    record: dict[str, Any] = {"pid": pid}
    errors: list[str] = []
    try:
        identity = process_identity(pid)
        record.update(identity)
    except FileNotFoundError as error:
        # The PID listing alone is not an observation of process identity.  If
        # stat vanished before establishing comm, there is no forbidden-name
        # match to classify.
        record["preidentity_error"] = (
            f"stat:{error.__class__.__name__}:{error}"
        )
        return record, errors, False
    except (OSError, ValueError) as error:
        errors.append(f"stat:{error.__class__.__name__}:{error}")
        return record, errors, True
    if record["comm"] not in FORBIDDEN_COMM:
        return record, errors, True
    readers = (
        ("uid", lambda: proc.stat().st_uid),
        (
            "cmdline",
            lambda: (proc / "cmdline")
            .read_bytes()
            .replace(b"\0", b" ")
            .decode("utf-8", "backslashreplace")
            .rstrip(),
        ),
        ("exe", lambda: str((proc / "exe").readlink())),
    )
    for field, reader in readers:
        try:
            record[field] = reader()
        except OSError as error:
            errors.append(f"{field}:{error.__class__.__name__}:{error}")
    return record, errors, True


def same_identity(record: dict[str, Any], wanted: dict[str, Any] | None) -> bool:
    return bool(
        wanted
        and record.get("pid") == wanted.get("pid")
        and record.get("starttime_ticks") == wanted.get("starttime_ticks")
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
    """Terminate the child's whole session, wait, and prove the group is gone."""

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
    return int(child.returncode if child.returncode is not None else 124)


def classify_process(
    record: dict[str, Any],
    errors: list[str],
    runner: dict[str, Any],
    current_child: dict[str, Any] | None,
    helpers: Iterable[dict[str, Any]],
) -> str:
    if errors:
        return "vanished_unresolved"
    if same_identity(record, runner):
        return "runner"
    if same_identity(record, current_child):
        return "current_child"
    if any(same_identity(record, helper) for helper in helpers):
        return "expected_helper"
    return "forbidden_unexplained"


def physical_order() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    ordinal = 0
    for batch in BATCHES:
        for cycle in CYCLES:
            variants = (
                ("control", "candidate", "candidate", "control")
                if cycle % 2 == 1
                else ("candidate", "control", "control", "candidate")
            )
            for slot, variant in enumerate(variants, start=1):
                ordinal += 1
                rows.append(
                    {
                        "ordinal": ordinal,
                        "variant": variant,
                        "mode": "process",
                        "batch": batch,
                        "cycle": cycle,
                        "slot": slot,
                    }
                )
    return rows


def physical_order_sha256() -> str:
    payload = json.dumps(
        physical_order(), sort_keys=True, separators=(",", ":")
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def cpu_topology_sha256() -> str:
    """Hash a locale-stable CPU summary and per-CPU topology allowlist."""

    environment = os.environ.copy()
    environment["LC_ALL"] = "C"

    def observe(arguments: list[str]) -> dict[str, Any]:
        result = subprocess.run(
            arguments,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
            env=environment,
        )
        return json.loads(result.stdout)

    summary = observe(["lscpu", "-J"])
    allowed = {
        "Architecture:",
        "CPU op-mode(s):",
        "Address sizes:",
        "Byte Order:",
        "CPU(s):",
        "On-line CPU(s) list:",
        "Vendor ID:",
        "Model name:",
        "CPU family:",
        "Model:",
        "Thread(s) per core:",
        "Core(s) per socket:",
        "Socket(s):",
        "Stepping:",
        "L1d cache:",
        "L1i cache:",
        "L2 cache:",
        "L3 cache:",
        "NUMA node(s):",
    }
    numa_field = re.compile(r"NUMA node[0-9]+ CPU\(s\):")
    stable_summary = sorted(
        (
            {"field": entry.get("field"), "data": entry.get("data")}
            for entry in summary.get("lscpu", [])
            if entry.get("field") in allowed
            or numa_field.fullmatch(str(entry.get("field", "")))
        ),
        key=lambda entry: str(entry["field"]),
    )
    per_cpu = observe(
        ["lscpu", "-J", "-e=CPU,NODE,SOCKET,CORE,ONLINE"]
    )
    cpus = sorted(
        per_cpu.get("cpus", []),
        key=lambda entry: (
            int(entry["cpu"]),
            int(entry["node"]),
            int(entry["socket"]),
            int(entry["core"]),
            bool(entry["online"]),
        ),
    )
    payload = json.dumps(
        {"summary": stable_summary, "cpus": cpus},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def csv_shape(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "exists": False,
            "bytes": 0,
            "data_rows": 0,
            "columns": 0,
            "complete": True,
        }
    data = path.read_bytes()
    complete = not data or data.endswith(b"\n")
    try:
        text = data.decode("utf-8")
        parsed = list(csv.reader(text.splitlines()))
    except (UnicodeDecodeError, csv.Error) as error:
        raise RunnerFailure(f"malformed CSV while counting rows: {error}")
    if not parsed:
        fields: tuple[str, ...] = ()
        rows: list[list[str]] = []
    else:
        fields = tuple(parsed[0])
        rows = parsed[1:]
    uniform = all(len(row) == len(fields) for row in rows)
    return {
        "exists": True,
        "bytes": len(data),
        "data_rows": len(rows),
        "columns": len(fields),
        "complete": complete and uniform,
        "fields": fields,
    }


def bind_evidence(
    csv_path: Path,
    manifests: dict[str, tuple[Path, int]],
    *,
    expected_rows: int,
) -> dict[str, str]:
    """Shared fail-closed finalizer for fixture and admission evidence."""

    shape = csv_shape(csv_path)
    if (
        shape["data_rows"] != expected_rows
        or shape["columns"] != len(CSV_FIELDS)
        or not shape["complete"]
    ):
        raise RunnerFailure(f"final CSV shape is invalid: {shape}")
    values = {
        "paired_csv_path": str(csv_path.resolve()),
        "paired_csv_sha256": sha256(csv_path),
        "paired_csv_bytes": str(shape["bytes"]),
        "paired_csv_data_rows": str(shape["data_rows"]),
        "paired_csv_columns": str(shape["columns"]),
    }
    for label, (path, expected_records) in manifests.items():
        observed = jsonl_records(path)
        if observed != expected_records:
            raise RunnerFailure(
                f"{label} records {observed} != {expected_records}"
            )
        values[f"{label}_path"] = str(path.resolve())
        values[f"{label}_sha256"] = sha256(path)
        values[f"{label}_records"] = str(expected_records)
    return values


def write_fixture_evidence(root: Path) -> tuple[Path, Path, Path]:
    """Create non-admissible evidence for the exact evaluator transition."""

    root.mkdir(parents=True, exist_ok=False)
    csv_path = root / "paired.csv"
    provenance_path = root / "provenance.txt"
    result_path = root / "result.json"
    control_source = "1" * 40
    candidate_source = "2" * 40
    control_binary = "a" * 64
    candidate_binary = "b" * 64
    text = io.StringIO(newline="")
    writer = csv.DictWriter(text, fieldnames=CSV_FIELDS, lineterminator="\n")
    writer.writeheader()
    for coordinate in physical_order():
        variant = coordinate["variant"]
        batch = int(coordinate["batch"])
        bpw = PROCESS_BPW[batch]
        appends = 4 * bpw
        events = batch * appends
        candidate = variant == "candidate"
        copied_records = 0 if candidate or batch > 10 else events
        writer.writerow(
            {
                "variant": variant,
                "source": candidate_source if candidate else control_source,
                "binary_sha256": candidate_binary if candidate else control_binary,
                "cycle": coordinate["cycle"],
                "slot": coordinate["slot"],
                "mode": "process",
                "batch": batch,
                "writers": 4,
                "bpw": bpw,
                "payload": 250,
                "events": events,
                "ev_s": 120 if candidate else 100,
                "p50_us": 45 if candidate else 50,
                "p99_us": 90 if candidate else 100,
                "allocs": events * (8 if candidate else 10),
                "alloc_bytes": events * (80 if candidate else 100),
                "allocs_per_event": (
                    "8.0000" if candidate else "10.0000"
                ),
                "alloc_bytes_per_event": (
                    "80.00" if candidate else "100.00"
                ),
                "owned_batches": appends if candidate else 0,
                "owned_records": events if candidate else 0,
                "owned_payload_bytes": events * 250 if candidate else 0,
                "borrowed_batches": 0 if candidate else appends,
                "borrowed_records": 0 if candidate else events,
                "copied_records": copied_records,
                "copied_bytes": copied_records * (250 + len("bench.event")),
                "batches": appends,
                "groups": 0,
                "fsyncs": 0,
                "fsync_p99_ns": 0,
                "fsync_degraded": "false",
                "pre_load1": 0.1,
                "post_load1": 0.1,
            }
        )
    atomic_write(csv_path, text.getvalue().encode())

    marker = {
        "protocol": PROTOCOL,
        "fixture_marker": "bn-22it-tooling-fixture-v1",
        "status": "PASS",
    }
    manifests = {
        "lease_event": root / "lease_event.jsonl",
        "guard_manifest": root / "guard_manifest.jsonl",
        "child_manifest": root / "child_manifest.jsonl",
        "smoke_manifest": root / "smoke_manifest.jsonl",
    }
    for label, path in manifests.items():
        append_jsonl(path, {**marker, "kind": label})
    values = {
        "protocol": PROTOCOL,
        "evidence_mode": "fixture",
        **bind_evidence(
            csv_path,
            {label: (path, 1) for label, path in manifests.items()},
            expected_rows=EXPECTED_CHILDREN,
        ),
    }
    atomic_write(
        provenance_path,
        ("\n".join(f"{key}={value}" for key, value in values.items()) + "\n").encode(),
    )
    return csv_path, provenance_path, result_path


@dataclass(frozen=True)
class FrozenVariant:
    name: str
    binary: Path
    source: str
    tree: str
    root: Path
    materialized_root: Path
    harness: Path
    cargo_lock: Path
    target_dir: Path
    binary_sha256: str
    harness_sha256: str
    cargo_lock_sha256: str
    diff_manifest_sha256: str
    patch_sha256: str
    build_attestation: Path
    build_attestation_sha256: str
    build_nonce: str
    build_log: Path
    build_log_sha256: str
    source_archive: Path
    source_archive_sha256: str
    tree_manifest: Path
    tree_manifest_sha256: str
    materialized_manifest: Path
    materialized_manifest_sha256: str
    sandbox: Path
    sandbox_sha256: str
    rustc: str
    cargo: str
    contract: dict[str, Any]


@dataclass(frozen=True)
class PreparedContext:
    pair_path: Path
    pair_sha256: str
    pair: dict[str, Any]
    approval_path: Path
    approval_sha256: str
    approval: dict[str, Any]
    prepare_runner: Path
    prepare_orchestrator: Path
    lease_event: Path
    pre_release: Path
    release: Path
    terminal: Path
    failure: Path


def inspect_prepared_pair(
    pair_path: Path,
) -> tuple[FrozenVariant, FrozenVariant, PreparedContext]:
    pair_path = pair_path.resolve(strict=True)
    pair = load_canonical_json(pair_path, PAIR_SCHEMA)
    require_exact_keys(
        pair,
        {
            "schema",
            "protocol",
            "created_at",
            "created_monotonic_ns",
            "tooling_source",
            "tooling_tree",
            "source_approval_path",
            "source_approval_sha256",
            "prepare_runner_path",
            "prepare_runner_sha256",
            "prepare_orchestrator_path",
            "prepare_orchestrator_sha256",
            "lease_event_path",
            "lease_event_sha256",
            "pre_release_path",
            "pre_release_sha256",
            "release_path",
            "release_sha256",
            "terminal_path",
            "terminal_sha256",
            "failure_path",
            "failure_absent",
            "attestations",
        },
        "prepared pair",
    )
    parse_aware_timestamp(pair.get("created_at"), "prepared pair creation")
    if not isinstance(pair.get("created_monotonic_ns"), int):
        raise RunnerFailure("prepared pair monotonic time is invalid", exit_code=2)
    if set(pair.get("attestations", {})) != {"control", "candidate"}:
        raise RunnerFailure("prepared pair attestation set mismatch", exit_code=2)
    prepare_root = pair_path.parent
    prepare_runner = Path(pair["prepare_runner_path"]).resolve(strict=True)
    prepare_orchestrator = Path(
        pair["prepare_orchestrator_path"]
    ).resolve(strict=True)
    expected_runner = Path(__file__).with_name("prepare_paired.sh").resolve()
    expected_orchestrator = Path(__file__).with_name("prepare_paired.py").resolve()
    for observed, expected, key in (
        (prepare_runner, expected_runner, "prepare_runner_sha256"),
        (prepare_orchestrator, expected_orchestrator, "prepare_orchestrator_sha256"),
    ):
        if observed != expected or sha256(observed) != pair.get(key):
            raise RunnerFailure(f"prepared pair {key} mismatch", exit_code=2)
    approval_path = Path(pair["source_approval_path"]).resolve(strict=True)
    if sha256(approval_path) != pair.get("source_approval_sha256"):
        raise RunnerFailure("prepared source-approval hash mismatch", exit_code=2)
    approval = load_canonical_json(
        approval_path, "bn-22it-source-approval-v1"
    )
    lease_event = Path(pair["lease_event_path"]).resolve(strict=True)
    if (
        prepare_root not in lease_event.parents
        or sha256(lease_event) != pair.get("lease_event_sha256")
    ):
        raise RunnerFailure("prepared lease-event binding mismatch", exit_code=2)
    lease = load_canonical_object(lease_event)
    require_exact_keys(
        lease,
        {
            "protocol",
            "event",
            "path",
            "device",
            "inode",
            "holder",
            "uid",
            "hostname",
            "boot_id",
            "nonce",
            "acquired_at",
            "acquired_monotonic_ns",
        },
        "prepare lease event",
    )
    if lease.get("protocol") != PROTOCOL or lease.get("event") != "prepare_acquired":
        raise RunnerFailure("prepare lease event mismatch", exit_code=2)
    parse_aware_timestamp(lease.get("acquired_at"), "prepare lease acquisition")

    lifecycle_paths = {}
    for label in ("pre_release", "release", "terminal"):
        path = Path(pair[f"{label}_path"]).resolve(strict=True)
        if prepare_root not in path.parents or sha256(path) != pair[f"{label}_sha256"]:
            raise RunnerFailure(f"prepared {label} binding mismatch", exit_code=2)
        lifecycle_paths[label] = path
    failure = Path(pair["failure_path"]).resolve()
    if (
        prepare_root not in failure.parents
        or pair.get("failure_absent") is not True
        or failure.exists()
        or failure.is_symlink()
    ):
        raise RunnerFailure("prepared failure-absence proof mismatch", exit_code=2)
    pre_release = load_canonical_json(
        lifecycle_paths["pre_release"], "bn-22it-prepare-pre-release-v1"
    )
    release = load_canonical_json(
        lifecycle_paths["release"], "bn-22it-prepare-release-v1"
    )
    terminal = load_canonical_json(
        lifecycle_paths["terminal"], "bn-22it-prepare-terminal-v1"
    )
    require_exact_keys(
        pre_release,
        {
            "schema",
            "protocol",
            "created_at",
            "created_monotonic_ns",
            "tooling_source",
            "tooling_tree",
            "source_approval_path",
            "source_approval_sha256",
            "prepare_runner_path",
            "prepare_runner_sha256",
            "prepare_orchestrator_path",
            "prepare_orchestrator_sha256",
            "lease_event_path",
            "lease_event_sha256",
            "attestations",
            "failure_path",
            "failure_absent",
            "lease_held",
        },
        "prepare pre-release",
    )
    require_exact_keys(
        release,
        {
            "schema",
            "protocol",
            "event",
            "released_at",
            "released_monotonic_ns",
            "lease_nonce",
            "pre_release_path",
            "pre_release_sha256",
        },
        "prepare release",
    )
    require_exact_keys(
        terminal,
        {
            "schema",
            "protocol",
            "outcome",
            "completed_at",
            "completed_monotonic_ns",
            "pre_release_path",
            "pre_release_sha256",
            "release_path",
            "release_sha256",
            "failure_path",
            "failure_absent",
        },
        "prepare terminal",
    )
    if (
        pre_release.get("source_approval_path") != str(approval_path)
        or pre_release.get("source_approval_sha256") != sha256(approval_path)
        or pre_release.get("prepare_runner_path") != str(prepare_runner)
        or pre_release.get("prepare_runner_sha256")
        != pair.get("prepare_runner_sha256")
        or pre_release.get("prepare_orchestrator_path")
        != str(prepare_orchestrator)
        or pre_release.get("prepare_orchestrator_sha256")
        != pair.get("prepare_orchestrator_sha256")
        or pre_release.get("lease_event_path") != str(lease_event)
        or pre_release.get("lease_event_sha256") != sha256(lease_event)
        or pre_release.get("attestations") != pair.get("attestations")
        or pre_release.get("failure_path") != str(failure)
        or pre_release.get("failure_absent") is not True
        or pre_release.get("lease_held") is not True
        or release.get("event") != "prepare_released"
        or release.get("lease_nonce") != lease.get("nonce")
        or release.get("pre_release_path") != str(lifecycle_paths["pre_release"])
        or release.get("pre_release_sha256") != sha256(lifecycle_paths["pre_release"])
        or terminal.get("outcome") != "PREPARED"
        or terminal.get("pre_release_path") != str(lifecycle_paths["pre_release"])
        or terminal.get("pre_release_sha256") != sha256(lifecycle_paths["pre_release"])
        or terminal.get("release_path") != str(lifecycle_paths["release"])
        or terminal.get("release_sha256") != sha256(lifecycle_paths["release"])
        or terminal.get("failure_path") != str(failure)
        or terminal.get("failure_absent") is not True
    ):
        raise RunnerFailure("prepared release lifecycle mismatch", exit_code=2)
    lease_ns = lease.get("acquired_monotonic_ns")
    pre_ns = pre_release.get("created_monotonic_ns")
    release_ns = release.get("released_monotonic_ns")
    terminal_ns = terminal.get("completed_monotonic_ns")
    pair_ns = pair.get("created_monotonic_ns")
    if (
        not all(
            isinstance(value, int) and not isinstance(value, bool)
            for value in (
                lease_ns,
                pre_ns,
                release_ns,
                terminal_ns,
                pair_ns,
            )
        )
        or not (lease_ns <= pre_ns <= release_ns <= terminal_ns <= pair_ns)
    ):
        raise RunnerFailure("prepared lifecycle chronology mismatch", exit_code=2)

    tooling_root = Path(__file__).resolve().parents[2]
    tooling_source = command_output(
        ["git", "-C", str(tooling_root), "rev-parse", "HEAD"]
    )
    tooling_tree = command_output(
        ["git", "-C", str(tooling_root), "rev-parse", "HEAD^{tree}"]
    )
    if (
        pair.get("tooling_source") != tooling_source
        or pair.get("tooling_tree") != tooling_tree
        or approval.get("baseline")
        != {"source": tooling_source, "tree": tooling_tree}
        or pre_release.get("tooling_source") != tooling_source
        or pre_release.get("tooling_tree") != tooling_tree
    ):
        raise RunnerFailure("prepared tooling baseline mismatch", exit_code=2)

    attestations: dict[str, tuple[Path, dict[str, Any]]] = {}
    roots: dict[str, Path] = {}
    for name in ("control", "candidate"):
        binding = pair.get("attestations", {}).get(name, {})
        path = Path(binding.get("path", "")).resolve(strict=True)
        if prepare_root not in path.parents or sha256(path) != binding.get("sha256"):
            raise RunnerFailure(f"{name} attestation binding mismatch", exit_code=2)
        attestation = load_canonical_json(path, ATTESTATION_SCHEMA)
        require_exact_keys(
            attestation,
            {
                "schema",
                "protocol",
                "variant",
                "baseline_source",
                "baseline_tree",
                "source_commit",
                "source_tree",
                "source_root",
                "materialized_root",
                "git_archive_argv",
                "source_archive_path",
                "source_archive_sha256",
                "tree_manifest_path",
                "tree_manifest_sha256",
                "materialized_manifest_path",
                "materialized_manifest_sha256",
                "materialized_manifest_pre_sha256",
                "materialized_manifest_post_sha256",
                "source_read_only",
                "gitlinks_present",
                "source_approval_sha256",
                "build_nonce",
                "target_dir",
                "target_dir_was_absent",
                "binary_path",
                "binary_sha256",
                "harness_path",
                "harness_sha256",
                "cargo_lock_path",
                "cargo_lock_sha256",
                "diff_manifest_path",
                "diff_manifest_sha256",
                "patch_path",
                "patch_sha256",
                "build_log_path",
                "build_log_sha256",
                "cargo_argv",
                "sandbox_path",
                "sandbox_sha256",
                "build_argv",
                "contract_argv",
                "build_started_at",
                "build_completed_at",
                "build_child",
                "rustc",
                "cargo",
                "contract_output_path",
                "contract_output_sha256",
                "contract_child",
                "contract",
                "prepare_runner_path",
                "prepare_runner_sha256",
                "prepare_orchestrator_path",
                "prepare_orchestrator_sha256",
            },
            f"{name} attestation",
        )
        if attestation.get("variant") != name:
            raise RunnerFailure(f"{name} attestation variant mismatch", exit_code=2)
        root = Path(attestation["source_root"]).resolve(strict=True)
        roots[name] = root
        attestations[name] = (path, attestation)

    preparation = load_prepare_module()
    try:
        validated_approval, observed_sources = preparation.validate_approval(
            approval_path, roots
        )
    except BaseException as error:
        raise RunnerFailure(
            f"prepared source approval no longer validates: {error}", exit_code=2
        ) from error
    if validated_approval != approval:
        raise RunnerFailure("prepared approval readback changed", exit_code=2)

    variants: list[FrozenVariant] = []
    preparation_child_sequence: list[int] = []
    for name in ("control", "candidate"):
        path, attestation = attestations[name]
        source = observed_sources[name]
        binary = Path(attestation["binary_path"]).resolve(strict=True)
        materialized_root = Path(
            attestation["materialized_root"]
        ).resolve(strict=True)
        materialized_root_stat = materialized_root.lstat()
        harness = Path(attestation["harness_path"]).resolve(strict=True)
        cargo_lock = Path(attestation["cargo_lock_path"]).resolve(strict=True)
        build_log = Path(attestation["build_log_path"]).resolve(strict=True)
        contract_output = Path(
            attestation["contract_output_path"]
        ).resolve(strict=True)
        diff_manifest = Path(attestation["diff_manifest_path"]).resolve(strict=True)
        patch = Path(attestation["patch_path"]).resolve(strict=True)
        source_archive = Path(
            attestation["source_archive_path"]
        ).resolve(strict=True)
        tree_manifest = Path(
            attestation["tree_manifest_path"]
        ).resolve(strict=True)
        materialized_manifest = Path(
            attestation["materialized_manifest_path"]
        ).resolve(strict=True)
        sandbox = Path(attestation["sandbox_path"]).resolve(strict=True)
        target_dir = Path(attestation["target_dir"]).resolve(strict=True)
        expected_binary = target_dir / "release/examples/owned_append_bench"
        cargo_argv = [
            "cargo",
            "build",
            "--locked",
            "--release",
            "-p",
            "mess-store",
            "--example",
            "owned_append_bench",
            "--target-dir",
            str(target_dir),
        ]
        sandbox_prefix = [
            str(sandbox),
            "--die-with-parent",
            "--ro-bind",
            "/",
            "/",
            "--proc",
            "/proc",
            "--dev-bind",
            "/dev",
            "/dev",
            "--tmpfs",
            "/tmp",
            "--bind",
            str(target_dir),
            str(target_dir),
            "--chdir",
            str(materialized_root),
        ]
        build_argv = [*sandbox_prefix, *cargo_argv]
        contract_argv = [*sandbox_prefix, str(binary)]
        tree_entries = preparation.git_tree_entries(
            roots[name], source["source"]
        )
        expected_tree_manifest = preparation.source_manifest(
            name, source, None, tree_entries
        )
        expected_materialized_manifest = preparation.source_manifest(
            name,
            source,
            materialized_root,
            preparation.filesystem_entries(materialized_root, tree_entries),
        )
        if load_canonical_object(tree_manifest) != expected_tree_manifest:
            raise RunnerFailure(f"{name} tree manifest mismatch", exit_code=2)
        if (
            load_canonical_object(materialized_manifest)
            != expected_materialized_manifest
        ):
            raise RunnerFailure(
                f"{name} materialized manifest mismatch", exit_code=2
            )
        archive_bytes = preparation.git_bytes(
            roots[name], ["archive", "--format=tar", source["source"]]
        )
        materialized_digest = hashlib.sha256(
            canonical_json(expected_materialized_manifest)
        ).hexdigest()
        expected_contract = {
            "protocol": PROTOCOL,
            "contract_mode": True,
            "csv_written": False,
            "baseline_source": approval["baseline"]["source"],
            "baseline_tree": approval["baseline"]["tree"],
            "source_commit": source["source"],
            "source_tree": source["tree"],
            "harness_sha256": approval["common"]["harness_sha256"],
            "cargo_lock_sha256": approval["common"]["cargo_lock_sha256"],
            "source_approval_sha256": pair["source_approval_sha256"],
            "build_nonce": attestation["build_nonce"],
        }
        try:
            contract_readback = json.loads(contract_output.read_bytes())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RunnerFailure(
                f"{name} contract readback is invalid", exit_code=2
            ) from error
        build_started = parse_aware_timestamp(
            attestation.get("build_started_at"), f"{name} build start"
        )
        build_completed = parse_aware_timestamp(
            attestation.get("build_completed_at"), f"{name} build completion"
        )
        checks = (
            (binary == expected_binary, "binary path"),
            (prepare_root in target_dir.parents, "target path"),
            (
                attestation.get("source_root") == str(roots[name]),
                "source root",
            ),
            (
                prepare_root in materialized_root.parents,
                "materialized root path",
            ),
            (
                stat.S_ISDIR(materialized_root_stat.st_mode)
                and stat.S_IMODE(materialized_root_stat.st_mode) == 0o555,
                "materialized root read-only mode",
            ),
            (
                harness == materialized_root
                / "crates/mess-store/examples/owned_append_bench.rs",
                "materialized harness path",
            ),
            (
                cargo_lock == materialized_root / "Cargo.lock",
                "materialized Cargo.lock path",
            ),
            (os.access(binary, os.X_OK), "binary executable"),
            (sha256(binary) == attestation.get("binary_sha256"), "binary hash"),
            (sha256(harness) == attestation.get("harness_sha256"), "harness hash"),
            (
                sha256(cargo_lock) == attestation.get("cargo_lock_sha256"),
                "Cargo.lock hash",
            ),
            (
                sha256(build_log) == attestation.get("build_log_sha256"),
                "build log hash",
            ),
            (
                attestation.get("git_archive_argv")
                == [
                    "git",
                    "-C",
                    str(roots[name]),
                    "archive",
                    "--format=tar",
                    source["source"],
                ],
                "Git archive argv",
            ),
            (source_archive.read_bytes() == archive_bytes, "source archive"),
            (
                sha256(source_archive)
                == attestation.get("source_archive_sha256"),
                "source archive hash",
            ),
            (
                sha256(tree_manifest)
                == attestation.get("tree_manifest_sha256"),
                "tree manifest hash",
            ),
            (
                sha256(materialized_manifest)
                == attestation.get("materialized_manifest_sha256"),
                "materialized manifest hash",
            ),
            (
                attestation.get("materialized_manifest_pre_sha256")
                == materialized_digest
                and attestation.get("materialized_manifest_post_sha256")
                == materialized_digest,
                "materialized manifest replay",
            ),
            (attestation.get("source_read_only") is True, "read-only source"),
            (attestation.get("gitlinks_present") is False, "gitlink absence"),
            (os.access(sandbox, os.X_OK), "sandbox executable"),
            (sha256(sandbox) == attestation.get("sandbox_sha256"), "sandbox hash"),
            (
                prepare_root in diff_manifest.parents
                and diff_manifest.read_bytes() == source["diff_bytes"],
                "diff manifest artifact",
            ),
            (
                prepare_root in patch.parents
                and patch.read_bytes() == source["patch_bytes"],
                "patch artifact",
            ),
            (
                sha256(contract_output)
                == attestation.get("contract_output_sha256"),
                "contract output hash",
            ),
            (
                attestation.get("source_commit") == source["source"],
                "source commit",
            ),
            (attestation.get("source_tree") == source["tree"], "source tree"),
            (
                attestation.get("source_approval_sha256")
                == pair["source_approval_sha256"],
                "approval hash",
            ),
            (attestation.get("target_dir_was_absent") is True, "fresh target"),
            (
                isinstance(attestation.get("build_nonce"), str)
                and re.fullmatch(r"[0-9a-f]{64}", attestation["build_nonce"])
                is not None,
                "build nonce",
            ),
            (attestation.get("cargo_argv") == cargo_argv, "Cargo argv"),
            (attestation.get("build_argv") == build_argv, "build argv"),
            (attestation.get("contract_argv") == contract_argv, "contract argv"),
            (build_completed >= build_started, "build chronology"),
            (
                isinstance(attestation.get("rustc"), str)
                and bool(attestation["rustc"]),
                "rustc identity",
            ),
            (
                isinstance(attestation.get("cargo"), str)
                and bool(attestation["cargo"]),
                "Cargo identity",
            ),
            (attestation.get("contract") == expected_contract, "contract"),
            (contract_readback == expected_contract, "contract readback"),
            (
                attestation.get("baseline_source")
                == approval["baseline"]["source"],
                "baseline source",
            ),
            (
                attestation.get("baseline_tree") == approval["baseline"]["tree"],
                "baseline tree",
            ),
            (
                attestation.get("prepare_runner_path") == str(prepare_runner)
                and attestation.get("prepare_runner_sha256")
                == pair["prepare_runner_sha256"],
                "prepare runner",
            ),
            (
                attestation.get("prepare_orchestrator_path")
                == str(prepare_orchestrator)
                and attestation.get("prepare_orchestrator_sha256")
                == pair["prepare_orchestrator_sha256"],
                "prepare orchestrator",
            ),
            (
                attestation.get("diff_manifest_sha256")
                == approval["variants"][name]["diff_manifest_sha256"],
                "diff manifest",
            ),
            (
                attestation.get("patch_sha256")
                == approval["variants"][name]["patch_sha256"],
                "patch hash",
            ),
        )
        for valid, label in checks:
            if not valid:
                raise RunnerFailure(
                    f"{name} prepared {label} mismatch", exit_code=2
                )
        validate_completed_child_record(
            attestation["build_child"],
            context=f"{name} build child",
            argv=build_argv,
            cwd=materialized_root,
            output_path=build_log,
        )
        validate_completed_child_record(
            attestation["contract_child"],
            context=f"{name} contract child",
            argv=contract_argv,
            cwd=materialized_root,
            output_path=contract_output,
        )
        child_points = [
            attestation["build_child"]["started_monotonic_ns"],
            attestation["build_child"]["completed_monotonic_ns"],
            attestation["contract_child"]["started_monotonic_ns"],
            attestation["contract_child"]["completed_monotonic_ns"],
        ]
        if not all(
            isinstance(point, int) and not isinstance(point, bool)
            for point in child_points
        ):
            raise RunnerFailure(
                f"{name} build/contract monotonic time is invalid",
                exit_code=2,
            )
        preparation_child_sequence.extend(child_points)
        variants.append(
            FrozenVariant(
                name=name,
                binary=binary,
                source=source["source"],
                tree=source["tree"],
                root=roots[name],
                materialized_root=materialized_root,
                harness=harness,
                cargo_lock=cargo_lock,
                target_dir=target_dir,
                binary_sha256=attestation["binary_sha256"],
                harness_sha256=attestation["harness_sha256"],
                cargo_lock_sha256=attestation["cargo_lock_sha256"],
                diff_manifest_sha256=attestation["diff_manifest_sha256"],
                patch_sha256=attestation["patch_sha256"],
                build_attestation=path,
                build_attestation_sha256=sha256(path),
                build_nonce=attestation["build_nonce"],
                build_log=build_log,
                build_log_sha256=attestation["build_log_sha256"],
                source_archive=source_archive,
                source_archive_sha256=attestation["source_archive_sha256"],
                tree_manifest=tree_manifest,
                tree_manifest_sha256=attestation["tree_manifest_sha256"],
                materialized_manifest=materialized_manifest,
                materialized_manifest_sha256=attestation[
                    "materialized_manifest_sha256"
                ],
                sandbox=sandbox,
                sandbox_sha256=attestation["sandbox_sha256"],
                rustc=attestation["rustc"],
                cargo=attestation["cargo"],
                contract=attestation["contract"],
            )
        )
    if preparation_child_sequence != sorted(preparation_child_sequence):
        raise RunnerFailure(
            "control/candidate builds or contract checks overlap/reorder",
            exit_code=2,
        )
    if (
        preparation_child_sequence[0] < lease_ns
        or pre_ns < preparation_child_sequence[-1]
    ):
        raise RunnerFailure(
            "prepared children are outside the preparation lease",
            exit_code=2,
        )
    if variants[0].build_nonce == variants[1].build_nonce:
        raise RunnerFailure("prepared build nonces are identical", exit_code=2)
    if (variants[0].rustc, variants[0].cargo) != (
        variants[1].rustc,
        variants[1].cargo,
    ):
        raise RunnerFailure("prepared toolchains differ", exit_code=2)
    context = PreparedContext(
        pair_path=pair_path,
        pair_sha256=sha256(pair_path),
        pair=pair,
        approval_path=approval_path,
        approval_sha256=sha256(approval_path),
        approval=approval,
        prepare_runner=prepare_runner,
        prepare_orchestrator=prepare_orchestrator,
        lease_event=lease_event,
        pre_release=lifecycle_paths["pre_release"],
        release=lifecycle_paths["release"],
        terminal=lifecycle_paths["terminal"],
        failure=failure,
    )
    return variants[0], variants[1], context


class MeasurementRunner:
    def __init__(
        self,
        control: FrozenVariant,
        candidate: FrozenVariant,
        output_dir: Path,
        wrapper: Path,
        orchestrator: Path,
        evaluator: Path,
        lock_path: Path = LOCK_PATH,
        prepared: PreparedContext | None = None,
    ) -> None:
        self.control = control
        self.candidate = candidate
        self.output_dir = output_dir
        self.wrapper = wrapper
        self.orchestrator = orchestrator
        self.evaluator = evaluator
        self.lock_path = lock_path
        self.prepared = prepared
        self.pair_claim_path = (
            prepared.pair_path.parent / "consumption.json"
            if prepared is not None
            else output_dir / "fixture-consumption.json"
        )
        self.pair_claim: dict[str, Any] | None = None
        self.csv_path = output_dir / "paired.csv"
        self.provenance_path = output_dir / "provenance.txt"
        self.guard_manifest = output_dir / "guard_manifest.jsonl"
        self.child_manifest = output_dir / "child_manifest.jsonl"
        self.smoke_manifest = output_dir / "smoke_manifest.jsonl"
        self.lease_event = output_dir / "lease_event.jsonl"
        self.result_path = output_dir / "result.json"
        self.phase = "created"
        self.phase_before_failure = self.phase
        self.next_row: dict[str, Any] | None = physical_order()[0]
        self.active_child: dict[str, Any] | None = None
        self.latest_snapshot: str | None = None
        self.runner_identity = process_identity(os.getpid())
        self.lease: dict[str, Any] | None = None
        self.lease_handle: Any = None
        self.guard_count = 0
        self.child_count = 0
        self.frozen_hashes = {
            "runner_sha256": sha256(wrapper),
            "orchestrator_sha256": sha256(orchestrator),
            "evaluator_sha256": sha256(evaluator),
            "prepare_runner_sha256": sha256(
                Path(__file__).with_name("prepare_paired.sh")
            ),
            "prepare_orchestrator_sha256": sha256(
                Path(__file__).with_name("prepare_paired.py")
            ),
        }

    def log(self, message: str) -> None:
        line = f"{now()} {self.phase} {message}\n".encode()
        path = self.output_dir / "run.log"
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(descriptor, line)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        print(message, flush=True)

    def claim_prepared_pair(self) -> None:
        if self.prepared is None or self.lease is None:
            raise RunnerFailure("cannot claim pair without preparation and lease")
        self.phase = "claim_prepared_pair"
        claim = {
            "schema": PAIR_CLAIM_SCHEMA,
            "protocol": PROTOCOL,
            "claimed_at": now(),
            "claimed_monotonic_ns": time.monotonic_ns(),
            "pair_path": str(self.prepared.pair_path),
            "pair_sha256": self.prepared.pair_sha256,
            "output_dir": str(self.output_dir.resolve()),
            "lease_nonce": self.lease["lease_nonce"],
            "lease_path": self.lease["lease_path"],
            "lease_device": self.lease["lease_device"],
            "lease_inode": self.lease["lease_inode"],
            "lease_holder_pid": self.lease["lease_holder_pid"],
            "lease_holder_starttime": self.lease[
                "lease_holder_starttime"
            ],
            "lease_boot_id": self.lease["lease_boot_id"],
        }
        atomic_create_json(self.pair_claim_path, claim)
        self.pair_claim = claim

    def acquire_lease(self) -> None:
        self.phase = "lease_acquire"
        if os.environ.get("MESS_BENCH_COORDINATION_CONFIRMED") != "true":
            raise RunnerFailure(
                "set MESS_BENCH_COORDINATION_CONFIRMED=true only after "
                "confirming cooperating sibling work is quiet"
            )
        lock_path = self.lock_path.expanduser().resolve()
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = lock_path.open("a+b", buffering=0)
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            handle.close()
            raise RunnerFailure(
                f"cooperative global measurement lease is held: {lock_path}"
            ) from error
        stat = os.fstat(handle.fileno())
        boot_path = Path("/proc/sys/kernel/random/boot_id")
        boot_id = boot_path.read_text().strip()
        self.lease_handle = handle
        self.lease = {
            "protocol": PROTOCOL,
            "event": "acquired",
            "lease_path": str(lock_path),
            "lease_device": stat.st_dev,
            "lease_inode": stat.st_ino,
            "lease_holder_pid": self.runner_identity["pid"],
            "lease_holder_starttime": self.runner_identity["starttime_ticks"],
            "lease_holder_uid": os.getuid(),
            "lease_hostname": socket.gethostname(),
            "lease_boot_id": boot_id,
            "lease_nonce": secrets.token_hex(32),
            "lease_acquired_at": now(),
        }
        append_jsonl(self.lease_event, self.lease)
        self.log(f"acquired cooperative lease {lock_path}")

    def release_lease(self, terminal: str) -> dict[str, Any] | None:
        if self.lease_handle is None:
            return None
        event = {
            "protocol": PROTOCOL,
            "event": "released",
            "nonce": self.lease["lease_nonce"] if self.lease else None,
            "terminal": terminal,
        }
        handle = self.lease_handle
        self.lease_handle = None
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
        event["released_at"] = now()
        atomic_json(self.output_dir / "lease_release.json", event)
        return event

    def verify_frozen(self) -> None:
        self.phase = "frozen_recheck"
        for variant in (self.control, self.candidate):
            try:
                head = command_output(
                    ["git", "-C", str(variant.root), "rev-parse", "HEAD"]
                )
                tree = command_output(
                    ["git", "-C", str(variant.root), "rev-parse", "HEAD^{tree}"]
                )
                dirty = command_output(
                    ["git", "-C", str(variant.root), "status", "--porcelain"]
                )
            except (OSError, subprocess.SubprocessError) as error:
                raise RunnerFailure(
                    f"cannot recheck {variant.name} worktree: {error}"
                ) from error
            checks = (
                (head == variant.source, f"{variant.name} HEAD"),
                (tree == variant.tree, f"{variant.name} tree"),
                (not dirty, f"{variant.name} worktree"),
                (
                    sha256(variant.binary) == variant.binary_sha256,
                    f"{variant.name} binary",
                ),
                (
                    sha256(variant.harness) == variant.harness_sha256,
                    f"{variant.name} harness",
                ),
                (
                    sha256(variant.cargo_lock) == variant.cargo_lock_sha256,
                    f"{variant.name} Cargo.lock",
                ),
                (
                    sha256(variant.build_attestation)
                    == variant.build_attestation_sha256,
                    f"{variant.name} build attestation",
                ),
                (
                    sha256(variant.build_log) == variant.build_log_sha256,
                    f"{variant.name} build log",
                ),
                (
                    sha256(variant.source_archive)
                    == variant.source_archive_sha256,
                    f"{variant.name} source archive",
                ),
                (
                    sha256(variant.tree_manifest)
                    == variant.tree_manifest_sha256,
                    f"{variant.name} tree manifest",
                ),
                (
                    sha256(variant.materialized_manifest)
                    == variant.materialized_manifest_sha256,
                    f"{variant.name} materialized manifest",
                ),
                (
                    sha256(variant.sandbox) == variant.sandbox_sha256,
                    f"{variant.name} sandbox",
                ),
            )
            for valid, label in checks:
                if not valid:
                    raise RunnerFailure(f"frozen provenance changed: {label}")
        paths = (
            (self.wrapper, "runner", "runner_sha256"),
            (self.orchestrator, "orchestrator", "orchestrator_sha256"),
            (self.evaluator, "evaluator", "evaluator_sha256"),
            (
                Path(__file__).with_name("prepare_paired.sh"),
                "prepare runner",
                "prepare_runner_sha256",
            ),
            (
                Path(__file__).with_name("prepare_paired.py"),
                "prepare orchestrator",
                "prepare_orchestrator_sha256",
            ),
        )
        for path, label, key in paths:
            if sha256(path) != self.frozen_hashes[key]:
                raise RunnerFailure(f"frozen provenance changed: {label}")
        if self.prepared is not None:
            if sha256(self.prepared.pair_path) != self.prepared.pair_sha256:
                raise RunnerFailure("frozen prepared-pair manifest changed")
            if (
                sha256(self.prepared.approval_path)
                != self.prepared.approval_sha256
            ):
                raise RunnerFailure("frozen source-approval manifest changed")
            if sha256(self.prepared.lease_event) != self.prepared.pair.get(
                "lease_event_sha256"
            ):
                raise RunnerFailure("frozen prepare lease event changed")
            for label, path in (
                ("pre_release", self.prepared.pre_release),
                ("release", self.prepared.release),
                ("terminal", self.prepared.terminal),
            ):
                if sha256(path) != self.prepared.pair.get(f"{label}_sha256"):
                    raise RunnerFailure(f"frozen prepare {label} changed")
            if self.prepared.failure.exists() or self.prepared.failure.is_symlink():
                raise RunnerFailure("frozen preparation failure appeared")
            if self.pair_claim is not None:
                if load_canonical_json(
                    self.pair_claim_path, PAIR_CLAIM_SCHEMA
                ) != self.pair_claim:
                    raise RunnerFailure("single-use pair claim changed")

    def verify_reviewed_sources(self) -> None:
        if self.prepared is None:
            raise RunnerFailure("missing prepared source context")
        control, candidate, context = inspect_prepared_pair(
            self.prepared.pair_path
        )
        if control != self.control or candidate != self.candidate:
            raise RunnerFailure("reviewed source/build readback changed")
        if context.pair_sha256 != self.prepared.pair_sha256:
            raise RunnerFailure("prepared-pair readback hash changed")

    def wait_for_load(self) -> None:
        self.phase = "load_cooldown"
        deadline = time.monotonic() + 120
        while True:
            try:
                load = float(Path("/proc/loadavg").read_text().split()[0])
            except (OSError, ValueError, IndexError) as error:
                raise RunnerFailure(f"cannot read numeric load1: {error}") from error
            if load < 6.0:
                return
            if time.monotonic() >= deadline:
                raise RunnerFailure(
                    f"load1={load} did not fall below 6.0 in 120 seconds"
                )
            self.log(f"waiting for load1={load} to fall below 6.0")
            time.sleep(2)

    def snapshot_processes(
        self,
        label: str,
        *,
        manifest: Path | None,
        directory: Path | None = None,
    ) -> dict[str, Any]:
        self.phase = f"guard_{label}"
        scan_started_at = now()
        scan_started_monotonic_ns = time.monotonic_ns()
        entries: list[dict[str, Any]] = []
        preidentity_vanished: list[dict[str, Any]] = []
        pids: list[int] = []
        for item in Path("/proc").iterdir():
            if item.name.isdigit():
                pids.append(int(item.name))
        for pid in sorted(pids):
            record, errors, observed = read_process(pid)
            if not observed:
                preidentity_vanished.append(
                    {
                        "pid": pid,
                        "observed_at": now(),
                        "read_error": record.get("preidentity_error"),
                    }
                )
                continue
            comm = record.get("comm")
            if comm not in FORBIDDEN_COMM and not errors:
                continue
            classification = classify_process(
                record,
                errors,
                self.runner_identity,
                self.active_child,
                (),
            )
            record.update(
                {
                    "observed_at": now(),
                    "read_errors": errors,
                    "classification": classification,
                }
            )
            entries.append(record)
        bad = [
            entry
            for entry in entries
            if entry["classification"]
            in {"forbidden_unexplained", "vanished_unresolved"}
        ]
        snapshot = {
            "protocol": PROTOCOL,
            "label": label,
            "started_at": scan_started_at,
            "started_monotonic_ns": scan_started_monotonic_ns,
            "runner": self.runner_identity,
            "active_child": self.active_child,
            "forbidden_comm": sorted(FORBIDDEN_COMM),
            "entries": entries,
            "preidentity_vanished": preidentity_vanished,
            "verdict": "fail" if bad else "pass",
            "completed_at": now(),
            "completed_monotonic_ns": time.monotonic_ns(),
        }
        target_dir = directory or self.output_dir / "guards"
        target_dir.mkdir(parents=True, exist_ok=True)
        if manifest == self.guard_manifest:
            ordinal = self.guard_count + 1
        else:
            ordinal = len(list(target_dir.glob("*.json"))) + 1
        path = target_dir / f"{ordinal:03d}-{label}.json"
        atomic_json(path, snapshot)
        publication_sha256 = sha256(path)
        published_snapshot = {
            **snapshot,
            "publication_path": str(path.resolve()),
            "publication_sha256": publication_sha256,
            "publication": "atomic",
        }
        self.latest_snapshot = str(path)
        if manifest is not None:
            append_jsonl(
                manifest,
                {
                    "protocol": PROTOCOL,
                    "ordinal": ordinal,
                    "label": label,
                    "publication": "atomic",
                    "verdict": snapshot["verdict"],
                    "matches": entries,
                    "preidentity_vanished": len(preidentity_vanished),
                    "runner": snapshot["runner"],
                    "active_child": snapshot["active_child"],
                    "forbidden_comm": snapshot["forbidden_comm"],
                    "started_at": snapshot["started_at"],
                    "completed_at": snapshot["completed_at"],
                    "started_monotonic_ns": snapshot[
                        "started_monotonic_ns"
                    ],
                    "completed_monotonic_ns": snapshot[
                        "completed_monotonic_ns"
                    ],
                    "path": str(path.resolve()),
                    "sha256": publication_sha256,
                },
            )
            if manifest == self.guard_manifest:
                self.guard_count += 1
        if bad:
            identities = [
                {
                    "pid": item.get("pid"),
                    "comm": item.get("comm"),
                    "starttime_ticks": item.get("starttime_ticks"),
                    "classification": item.get("classification"),
                    "read_errors": item.get("read_errors"),
                }
                for item in bad
            ]
            raise RunnerFailure(
                f"process guard {label} found unresolved activity: {identities}"
            )
        return published_snapshot

    def reap_probe(self, identity: dict[str, Any]) -> dict[str, Any]:
        try:
            observed = process_identity(identity["pid"])
        except FileNotFoundError:
            return {
                "status": "absent",
                "pid": identity["pid"],
                "starttime": identity["starttime_ticks"],
            }
        except OSError as error:
            raise RunnerFailure(
                f"cannot prove child {identity['pid']} reaped: {error}"
            ) from error
        if same_identity(observed, identity):
            raise RunnerFailure(
                f"prior_child_not_reaped pid={identity['pid']} "
                f"starttime={identity['starttime_ticks']}"
            )
        return {
            "status": "pid_reused",
            "pid": identity["pid"],
            "starttime": identity["starttime_ticks"],
            "observed_starttime": observed["starttime_ticks"],
            "observed": observed,
        }

    def run_child(
        self,
        *,
        kind: str,
        argv: list[str],
        environment: dict[str, str],
        output_path: Path,
        timeout: int,
        csv_path: Path | None,
        manifest: Path | None,
        expected_row_delta: int,
        context: dict[str, Any],
        allowed_exit_statuses: frozenset[int] = frozenset({0}),
        active_publication: Path | None = None,
    ) -> dict[str, Any]:
        self.phase = f"child_spawn_{kind}"
        before = csv_shape(csv_path) if csv_path else None
        prefix_before = sha256_prefix(
            csv_path, before["bytes"] if before is not None else 0
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        started_at = now()
        started_monotonic_ns = time.monotonic_ns()
        interrupted: BaseException | None = None
        terminated_by_runner = False
        with output_path.open("wb") as output:
            blocked = {signal.SIGINT, signal.SIGTERM}
            previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, blocked)
            try:
                child = subprocess.Popen(
                    argv,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    env=environment,
                    start_new_session=True,
                )
            except OSError as error:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
                raise RunnerFailure(f"cannot spawn {kind}: {error}") from error
            try:
                identity = process_identity(child.pid)
            except (OSError, ValueError) as error:
                try:
                    terminate_process_group(child)
                finally:
                    signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
                raise RunnerFailure(
                    f"cannot capture exact {kind} child identity: {error}"
                ) from error
            self.active_child = identity
            try:
                publication = (
                    active_publication
                    if active_publication is not None
                    else self.output_dir / "active-child.json"
                )
                atomic_json(
                    publication,
                    {
                        "protocol": PROTOCOL,
                        "runner": self.runner_identity,
                        "child": identity,
                        "kind": kind,
                        "argv": argv,
                        "published_at": now(),
                        "published_monotonic_ns": time.monotonic_ns(),
                    },
                )
            except BaseException:
                try:
                    terminate_process_group(child)
                finally:
                    self.active_child = None
                    signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
                raise
            timed_out = False
            try:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
                rc = child.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                terminated_by_runner = True
                rc = terminate_process_group(child)
                rc = 124
            except BaseException as error:
                interrupted = error
                terminated_by_runner = True
                try:
                    rc = terminate_process_group(child)
                except BaseException as cleanup_error:
                    interrupted = RunnerFailure(
                        f"{kind} interrupted by {error!r}; cleanup failed: "
                        f"{cleanup_error!r}",
                        exit_code=30,
                    )
                    rc = 125
            finally:
                output.flush()
                os.fsync(output.fileno())
        reaping = self.reap_probe(identity)
        process_group_absent = not process_group_exists(identity["pid"])
        self.active_child = None
        after = csv_shape(csv_path) if csv_path else None
        prefix_after = sha256_prefix(
            csv_path, after["bytes"] if after is not None else 0
        )
        row_delta = (
            after["data_rows"] - before["data_rows"]
            if before is not None and after is not None
            else None
        )
        byte_delta = (
            after["bytes"] - before["bytes"]
            if before is not None and after is not None
            else None
        )
        record = {
            "protocol": PROTOCOL,
            "kind": kind,
            "context": context,
            "ordinal": context.get("ordinal"),
            "mode": context.get("mode"),
            "batch": context.get("batch"),
            "cycle": context.get("cycle"),
            "slot": context.get("slot"),
            "variant": context.get("variant"),
            "argv": argv,
            "started_at": started_at,
            "started_monotonic_ns": started_monotonic_ns,
            "completed_at": now(),
            "completed_monotonic_ns": time.monotonic_ns(),
            "identity": identity,
            "pid": identity["pid"],
            "starttime": identity["starttime_ticks"],
            "waited_pid": child.pid,
            "binary_sha256": environment.get("OWNED_APPEND_BINARY_SHA256"),
            "exit_status": rc,
            "timed_out": timed_out,
            "terminated_by_runner": terminated_by_runner,
            "interrupted": (
                f"{interrupted.__class__.__name__}: {interrupted}"
                if interrupted is not None
                else None
            ),
            "reaping": reaping,
            "process_group_absent": process_group_absent,
            "output_path": str(output_path.resolve()),
            "output_sha256": sha256(output_path),
            "csv_before": before,
            "csv_after": after,
            "csv_byte_delta": byte_delta,
            "csv_row_delta": row_delta,
            "csv_rows_before": (
                before["data_rows"] if before is not None else None
            ),
            "csv_rows_after": (
                after["data_rows"] if after is not None else None
            ),
            "csv_bytes_before": before["bytes"] if before is not None else None,
            "csv_bytes_after": after["bytes"] if after is not None else None,
            "csv_prefix_sha256_before": prefix_before,
            "csv_prefix_sha256_after": prefix_after,
        }
        if manifest is not None:
            append_jsonl(manifest, record)
            if manifest == self.child_manifest:
                self.child_count += 1
        if interrupted is not None:
            raise interrupted
        if not process_group_absent:
            terminate_process_group(child)
            raise RunnerFailure(
                f"{kind} left process group {identity['pid']} alive"
            )
        if rc not in allowed_exit_statuses:
            raise RunnerFailure(
                f"{kind} child exited {rc}", child_rc=rc
            )
        if csv_path is not None and row_delta != expected_row_delta:
            raise RunnerFailure(
                f"{kind} appended {row_delta} CSV rows, expected "
                f"{expected_row_delta}"
            )
        if after is not None:
            if not after["complete"]:
                raise RunnerFailure(f"{kind} left a partial or ragged CSV")
            if after["fields"] != CSV_FIELDS:
                raise RunnerFailure(
                    f"{kind} CSV schema changed: {after['fields']}"
                )
            if expected_row_delta and (byte_delta is None or byte_delta <= 0):
                raise RunnerFailure(f"{kind} did not append positive CSV bytes")
        return record

    def run_contract_smoke(self) -> None:
        self.phase = "real_binary_contract_smoke"
        smoke_root = self.output_dir / "smoke"
        smoke_guards = smoke_root / "guards"
        records: list[dict[str, Any]] = []
        guard_records: list[dict[str, Any]] = []
        for variant in (self.control, self.candidate):
            self.wait_for_load()
            self.verify_frozen()
            guard_records.append(
                self.snapshot_processes(
                    f"{variant.name}-pre",
                    manifest=None,
                    directory=smoke_guards,
                )
            )
            output = smoke_root / f"{variant.name}.log"
            environment = os.environ.copy()
            environment["OWNED_APPEND_CONTRACT_MODE"] = PROTOCOL
            environment["OWNED_APPEND_BINARY_SHA256"] = variant.binary_sha256
            record = self.run_child(
                kind=f"{variant.name}_contract",
                argv=[str(variant.binary)],
                environment=environment,
                output_path=output,
                timeout=30,
                csv_path=None,
                manifest=None,
                expected_row_delta=0,
                context={"variant": variant.name},
            )
            guard_records.append(
                self.snapshot_processes(
                    f"{variant.name}-post",
                    manifest=None,
                    directory=smoke_guards,
                )
            )
            try:
                contract = json.loads(output.read_text())
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
                raise RunnerFailure(
                    f"{variant.name} contract mode output is invalid: {error}"
                ) from error
            expected = variant.contract
            if contract != expected:
                raise RunnerFailure(
                    f"{variant.name} contract mode {contract!r} != {expected!r}"
                )
            records.append(record)
        fixture_csv, fixture_provenance, fixture_result = write_fixture_evidence(
            smoke_root / "evaluator-fixture"
        )
        self.verify_frozen()
        guard_records.append(
            self.snapshot_processes(
                "evaluator-pre",
                manifest=None,
                directory=smoke_guards,
            )
        )
        evaluator_record = self.run_child(
            kind="fixture_evaluator",
            argv=[
                str(self.evaluator),
                "--fixture",
                str(fixture_csv),
                str(fixture_provenance),
                str(fixture_result),
            ],
            environment=os.environ.copy(),
            output_path=smoke_root / "evaluator.log",
            timeout=120,
            csv_path=None,
            manifest=None,
            expected_row_delta=0,
            context={"evidence_mode": "fixture"},
        )
        try:
            fixture_outcome = json.loads(fixture_result.read_text())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RunnerFailure(
                f"fixture evaluator result is unreadable: {error}"
            ) from error
        if fixture_outcome.get("outcome") != "FIXTURE_PASS":
            raise RunnerFailure(
                f"fixture evaluator did not pass: {fixture_outcome!r}"
            )
        append_jsonl(
            self.smoke_manifest,
            {
                "protocol": PROTOCOL,
                "status": "PASS",
                "completed_at": now(),
                "guards": [
                    {
                        "label": guard["label"],
                        "path": guard["publication_path"],
                        "sha256": guard["publication_sha256"],
                        "publication": guard["publication"],
                        "verdict": guard["verdict"],
                        "preidentity_vanished": len(
                            guard["preidentity_vanished"]
                        ),
                        "started_at": guard["started_at"],
                        "completed_at": guard["completed_at"],
                        "started_monotonic_ns": guard[
                            "started_monotonic_ns"
                        ],
                        "completed_monotonic_ns": guard[
                            "completed_monotonic_ns"
                        ],
                    }
                    for guard in guard_records
                ],
                "children": {
                    "control": {
                        "binary_sha256": self.control.binary_sha256,
                        "contract_mode": True,
                        "argv": records[0]["argv"],
                        "pid": records[0]["pid"],
                        "starttime": records[0]["starttime"],
                        "waited_pid": records[0]["waited_pid"],
                        "exit_status": records[0]["exit_status"],
                        "timed_out": records[0]["timed_out"],
                        "reaping": records[0]["reaping"],
                        "started_at": records[0]["started_at"],
                        "completed_at": records[0]["completed_at"],
                        "started_monotonic_ns": records[0][
                            "started_monotonic_ns"
                        ],
                        "completed_monotonic_ns": records[0][
                            "completed_monotonic_ns"
                        ],
                        "process_group_absent": records[0][
                            "process_group_absent"
                        ],
                        "output_path": records[0]["output_path"],
                        "output_sha256": records[0]["output_sha256"],
                        "contract": self.control.contract,
                    },
                    "candidate": {
                        "binary_sha256": self.candidate.binary_sha256,
                        "contract_mode": True,
                        "argv": records[1]["argv"],
                        "pid": records[1]["pid"],
                        "starttime": records[1]["starttime"],
                        "waited_pid": records[1]["waited_pid"],
                        "exit_status": records[1]["exit_status"],
                        "timed_out": records[1]["timed_out"],
                        "reaping": records[1]["reaping"],
                        "started_at": records[1]["started_at"],
                        "completed_at": records[1]["completed_at"],
                        "started_monotonic_ns": records[1][
                            "started_monotonic_ns"
                        ],
                        "completed_monotonic_ns": records[1][
                            "completed_monotonic_ns"
                        ],
                        "process_group_absent": records[1][
                            "process_group_absent"
                        ],
                        "output_path": records[1]["output_path"],
                        "output_sha256": records[1]["output_sha256"],
                        "contract": self.candidate.contract,
                    },
                },
                "evaluator": {
                    "argv": evaluator_record["argv"],
                    "pid": evaluator_record["pid"],
                    "starttime": evaluator_record["starttime"],
                    "waited_pid": evaluator_record["waited_pid"],
                    "exit_status": evaluator_record["exit_status"],
                    "timed_out": evaluator_record["timed_out"],
                    "reaping": evaluator_record["reaping"],
                    "started_at": evaluator_record["started_at"],
                    "completed_at": evaluator_record["completed_at"],
                    "started_monotonic_ns": evaluator_record[
                        "started_monotonic_ns"
                    ],
                    "completed_monotonic_ns": evaluator_record[
                        "completed_monotonic_ns"
                    ],
                    "process_group_absent": evaluator_record[
                        "process_group_absent"
                    ],
                    "output_path": evaluator_record["output_path"],
                    "output_sha256": evaluator_record["output_sha256"],
                    "outcome": fixture_outcome["outcome"],
                    "result_path": str(fixture_result.resolve()),
                    "result_sha256": sha256(fixture_result),
                },
            },
        )

    def write_initial_provenance(self) -> None:
        self.phase = "provenance_initial"
        if self.prepared is None:
            raise RunnerFailure("admission requires a prepared-pair manifest")
        if self.pair_claim is None or not self.pair_claim_path.is_file():
            raise RunnerFailure("admission requires a durable single-use pair claim")
        bench_dir = Path(
            os.environ.get(
                "MESS_BENCH_DIR", str(Path.home() / ".cache/mess-bench")
            )
        ).expanduser().resolve()
        bench_dir.mkdir(parents=True, exist_ok=True)
        filesystem_source = command_output(
            ["findmnt", "-n", "-o", "SOURCE", "-T", str(bench_dir)]
        )
        filesystem_type = command_output(
            ["findmnt", "-n", "-o", "FSTYPE", "-T", str(bench_dir)]
        )
        filesystem_target = command_output(
            ["findmnt", "-n", "-o", "TARGET", "-T", str(bench_dir)]
        )
        if filesystem_type != "ext4":
            raise RunnerFailure(
                f"benchmark directory filesystem is {filesystem_type}, expected ext4"
            )
        filesystem = os.statvfs(bench_dir)
        cpu_online_path = Path("/sys/devices/system/cpu/online")
        cpu_online = cpu_online_path.read_text().strip()
        rustc = command_output(["rustc", "-Vv"]).replace("\n", "\\n")
        cargo = command_output(["cargo", "-V"])
        for variant in (self.control, self.candidate):
            if (rustc, cargo) != (variant.rustc, variant.cargo):
                raise RunnerFailure(
                    f"live toolchain differs from {variant.name} build attestation"
                )
        values = {
            "protocol": PROTOCOL,
            "evidence_mode": "admission",
            "control_source": self.control.source,
            "candidate_source": self.candidate.source,
            "control_tree": self.control.tree,
            "candidate_tree": self.candidate.tree,
            "control_dirty": "false",
            "candidate_dirty": "false",
            "control_root": str(self.control.root),
            "candidate_root": str(self.candidate.root),
            "control_binary_path": str(self.control.binary),
            "candidate_binary_path": str(self.candidate.binary),
            "control_harness_path": str(self.control.harness),
            "candidate_harness_path": str(self.candidate.harness),
            "control_cargo_lock_path": str(self.control.cargo_lock),
            "candidate_cargo_lock_path": str(self.candidate.cargo_lock),
            "runner_path": str(self.wrapper),
            "orchestrator_path": str(self.orchestrator),
            "evaluator_path": str(self.evaluator),
            "control_harness_sha256": self.control.harness_sha256,
            "candidate_harness_sha256": self.candidate.harness_sha256,
            "control_binary_sha256": self.control.binary_sha256,
            "candidate_binary_sha256": self.candidate.binary_sha256,
            "control_cargo_lock_sha256": self.control.cargo_lock_sha256,
            "candidate_cargo_lock_sha256": self.candidate.cargo_lock_sha256,
            "baseline_source": self.prepared.approval["baseline"]["source"],
            "baseline_tree": self.prepared.approval["baseline"]["tree"],
            "tooling_source": self.prepared.pair["tooling_source"],
            "tooling_tree": self.prepared.pair["tooling_tree"],
            "source_approval_path": str(self.prepared.approval_path),
            "source_approval_sha256": self.prepared.approval_sha256,
            "source_approval_schema": self.prepared.approval["schema"],
            "source_review_id": self.prepared.approval["review_id"],
            "source_review_status": self.prepared.approval["status"],
            "prepare_manifest_path": str(self.prepared.pair_path),
            "prepare_manifest_sha256": self.prepared.pair_sha256,
            "prepare_pre_release_path": str(self.prepared.pre_release),
            "prepare_pre_release_sha256": self.prepared.pair[
                "pre_release_sha256"
            ],
            "prepare_release_path": str(self.prepared.release),
            "prepare_release_sha256": self.prepared.pair["release_sha256"],
            "prepare_terminal_path": str(self.prepared.terminal),
            "prepare_terminal_sha256": self.prepared.pair["terminal_sha256"],
            "prepare_failure_path": str(self.prepared.failure),
            "prepare_failure_absent": "true",
            "pair_claim_path": str(self.pair_claim_path),
            "pair_claim_sha256": sha256(self.pair_claim_path),
            "pair_claim_schema": PAIR_CLAIM_SCHEMA,
            "prepare_runner_path": str(self.prepared.prepare_runner),
            "prepare_orchestrator_path": str(
                self.prepared.prepare_orchestrator
            ),
            **self.frozen_hashes,
            "started_at": now(),
            "command": shlex.join(sys.argv),
            "kernel": command_output(["uname", "-a"]),
            "rustc": rustc,
            "cargo": cargo,
            "page_size": str(os.sysconf("SC_PAGE_SIZE")),
            "physical_order_sha256": physical_order_sha256(),
            "bench_dir": str(bench_dir),
            "filesystem_source": filesystem_source,
            "filesystem_type": filesystem_type,
            "filesystem_target": filesystem_target,
            "filesystem_free_bytes": str(
                filesystem.f_bavail * filesystem.f_frsize
            ),
            "filesystem_total_bytes": str(
                filesystem.f_blocks * filesystem.f_frsize
            ),
            "cpu_count": str(os.cpu_count() or 0),
            "cpu_online": cpu_online,
            "cpu_topology_sha256": cpu_topology_sha256(),
        }
        for variant in (self.control, self.candidate):
            prefix = variant.name
            values.update(
                {
                    f"{prefix}_diff_manifest_sha256": (
                        variant.diff_manifest_sha256
                    ),
                    f"{prefix}_patch_sha256": variant.patch_sha256,
                    f"{prefix}_build_attestation_path": str(
                        variant.build_attestation
                    ),
                    f"{prefix}_build_attestation_sha256": (
                        variant.build_attestation_sha256
                    ),
                    f"{prefix}_build_nonce": variant.build_nonce,
                    f"{prefix}_build_log_path": str(variant.build_log),
                    f"{prefix}_build_log_sha256": variant.build_log_sha256,
                    f"{prefix}_target_dir": str(variant.target_dir),
                    f"{prefix}_materialized_root": str(
                        variant.materialized_root
                    ),
                    f"{prefix}_source_archive_path": str(
                        variant.source_archive
                    ),
                    f"{prefix}_source_archive_sha256": (
                        variant.source_archive_sha256
                    ),
                    f"{prefix}_tree_manifest_path": str(
                        variant.tree_manifest
                    ),
                    f"{prefix}_tree_manifest_sha256": (
                        variant.tree_manifest_sha256
                    ),
                    f"{prefix}_materialized_manifest_path": str(
                        variant.materialized_manifest
                    ),
                    f"{prefix}_materialized_manifest_sha256": (
                        variant.materialized_manifest_sha256
                    ),
                    f"{prefix}_sandbox_path": str(variant.sandbox),
                    f"{prefix}_sandbox_sha256": variant.sandbox_sha256,
                }
            )
        governors = []
        for path in sorted(
            Path("/sys/devices/system/cpu").glob("cpu*/cpufreq/scaling_governor")
        ):
            try:
                governors.append(path.read_text().strip())
            except OSError:
                governors.append("unreadable")
        values["governors"] = ",".join(governors)
        self.provenance_values = values
        self._flush_provenance()

    def _flush_provenance(self) -> None:
        lines = [f"{key}={value}" for key, value in self.provenance_values.items()]
        atomic_write(self.provenance_path, ("\n".join(lines) + "\n").encode())

    def finalize_provenance(self) -> None:
        self.phase = "provenance_final"
        if self.lease is None:
            raise RunnerFailure("cannot finalize without an acquired lease")
        bound = bind_evidence(
            self.csv_path,
            {
                "guard_manifest": (self.guard_manifest, EXPECTED_GUARDS),
                "child_manifest": (self.child_manifest, EXPECTED_CHILDREN),
                "smoke_manifest": (self.smoke_manifest, 1),
                "lease_event": (self.lease_event, 1),
            },
            expected_rows=EXPECTED_CHILDREN,
        )
        self.provenance_values.update(
            {
                **bound,
                "lease_path": self.lease["lease_path"],
                "lease_device": str(self.lease["lease_device"]),
                "lease_inode": str(self.lease["lease_inode"]),
                "lease_holder_pid": str(self.lease["lease_holder_pid"]),
                "lease_holder_starttime": str(
                    self.lease["lease_holder_starttime"]
                ),
                "lease_holder_uid": str(self.lease["lease_holder_uid"]),
                "lease_hostname": self.lease["lease_hostname"],
                "lease_acquired_at": self.lease["lease_acquired_at"],
                "lease_nonce": self.lease["lease_nonce"],
                "lease_boot_id": self.lease["lease_boot_id"],
                "coordination_confirmed": "true",
            }
        )
        self._flush_provenance()

    def invoke_evaluator(self) -> tuple[int, dict[str, Any], dict[str, Any]]:
        self.phase = "pre_evaluator_guard"
        self.wait_for_load()
        self.verify_reviewed_sources()
        self.verify_frozen()
        self.snapshot_processes("pre-evaluator", manifest=self.guard_manifest)
        self.finalize_provenance()
        self.phase = "evaluator"
        evaluator_record = self.run_child(
            kind="evaluator",
            argv=[
                str(self.evaluator),
                str(self.csv_path),
                str(self.provenance_path),
                str(self.result_path),
            ],
            environment=os.environ.copy(),
            output_path=self.output_dir / "evaluation.log",
            timeout=120,
            csv_path=None,
            manifest=None,
            expected_row_delta=0,
            context={"evidence_mode": "admission"},
            allowed_exit_statuses=frozenset({0, 10, 20, 30}),
        )
        evaluator_rc = int(evaluator_record["exit_status"])
        if not self.result_path.is_file():
            raise RunnerFailure(
                f"evaluator exited {evaluator_rc} without result.json",
                exit_code=30,
                child_rc=evaluator_rc,
            )
        try:
            outcome = json.loads(self.result_path.read_text())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RunnerFailure(
                f"cannot read evaluator result: {error}", exit_code=30
            ) from error
        expected_exit = {"ADOPT": 0, "DECLINED": 10}.get(outcome.get("outcome"))
        if expected_exit is None or evaluator_rc != expected_exit:
            raise RunnerFailure(
                f"evaluator result/exit mismatch: outcome={outcome.get('outcome')!r} "
                f"rc={evaluator_rc}",
                exit_code=30 if evaluator_rc == 30 else 20,
                child_rc=evaluator_rc,
            )
        return evaluator_rc, evaluator_record, outcome

    def publish_terminal(
        self,
        outcome: str,
        evaluator_rc: int,
        evaluator_record: dict[str, Any],
        *,
        verify: bool = True,
    ) -> int:
        """Publish under-lease proof, release honestly, then bind the release."""

        self.phase = "terminal_pre_release"
        pre_release = {
            "protocol": PROTOCOL,
            "outcome": outcome,
            "evaluator_exit": evaluator_rc,
            "completed_at": now(),
            "result_path": str(self.result_path.resolve()),
            "result_sha256": sha256(self.result_path),
            "provenance_sha256": sha256(self.provenance_path),
            "pair_claim_path": str(self.pair_claim_path.resolve()),
            "pair_claim_sha256": sha256(self.pair_claim_path),
            "evaluator_child": evaluator_record,
            "artifact_inventory": self.inventory(),
        }
        pre_release_path = self.output_dir / "terminal_pre_release.json"
        atomic_json(pre_release_path, pre_release)
        release_event = self.release_lease(outcome)
        release_path = self.output_dir / "lease_release.json"
        terminal = {
            **pre_release,
            "terminal_pre_release_path": str(pre_release_path.resolve()),
            "terminal_pre_release_sha256": sha256(pre_release_path),
            "lease_release_path": str(release_path.resolve()),
            "lease_release_sha256": sha256(release_path),
            "lease_release": release_event,
            "terminal_published_at": now(),
        }
        try:
            atomic_json(self.output_dir / "terminal.json", terminal)
            if verify:
                self.verify_terminal()
        except BaseException as error:
            atomic_json(
                self.output_dir / "post_release_failure.json",
                {
                    "protocol": PROTOCOL,
                    "outcome": "INCONCLUSIVE_FATAL",
                    "phase": "terminal_post_release",
                    "reason": f"{error.__class__.__name__}: {error}",
                    "failed_at": now(),
                    "pre_release_sha256": sha256(pre_release_path),
                    "lease_release_sha256": sha256(release_path),
                },
            )
            return 30
        return evaluator_rc

    def verify_terminal(self) -> None:
        self.phase = "terminal_verification"
        verification_path = self.output_dir / "terminal-verification.json"
        if verification_path.exists() or verification_path.is_symlink():
            raise RunnerFailure("terminal verification output already exists")
        record = self.run_child(
            kind="terminal_verifier",
            argv=[
                str(self.evaluator),
                "--verify-terminal",
                str(self.output_dir.resolve()),
                str(verification_path.resolve()),
            ],
            environment=os.environ.copy(),
            output_path=self.output_dir / "terminal-verifier.log",
            timeout=120,
            csv_path=None,
            manifest=None,
            expected_row_delta=0,
            context={"evidence_mode": "terminal-verification"},
        )
        if record["exit_status"] != 0 or not verification_path.is_file():
            raise RunnerFailure("terminal verifier did not publish success")
        try:
            verification = json.loads(verification_path.read_bytes())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RunnerFailure("terminal verification is unreadable") from error
        require_exact_keys(
            verification,
            {
                "schema",
                "protocol",
                "outcome",
                "verified_at",
                "output_dir",
                "terminal_path",
                "terminal_sha256",
                "release_path",
                "release_sha256",
                "pre_release_path",
                "pre_release_sha256",
                "result_path",
                "result_sha256",
                "provenance_path",
                "provenance_sha256",
                "pair_claim_path",
                "pair_claim_sha256",
                "errors",
            },
            "terminal verification",
        )
        parse_aware_timestamp(
            verification.get("verified_at"), "terminal verification"
        )
        expected_bindings = {
            "output_dir": str(self.output_dir.resolve()),
            "terminal_path": str((self.output_dir / "terminal.json").resolve()),
            "terminal_sha256": sha256(self.output_dir / "terminal.json"),
            "release_path": str(
                (self.output_dir / "lease_release.json").resolve()
            ),
            "release_sha256": sha256(self.output_dir / "lease_release.json"),
            "pre_release_path": str(
                (self.output_dir / "terminal_pre_release.json").resolve()
            ),
            "pre_release_sha256": sha256(
                self.output_dir / "terminal_pre_release.json"
            ),
            "result_path": str(self.result_path.resolve()),
            "result_sha256": sha256(self.result_path),
            "provenance_path": str(self.provenance_path.resolve()),
            "provenance_sha256": sha256(self.provenance_path),
            "pair_claim_path": str(self.pair_claim_path.resolve()),
            "pair_claim_sha256": sha256(self.pair_claim_path),
        }
        if (
            verification.get("schema")
            != "bn-22it-terminal-verification-v1"
            or verification.get("protocol") != PROTOCOL
            or verification.get("outcome") != "TERMINAL_VERIFIED"
            or verification.get("errors") != []
            or any(
                verification.get(field) != expected
                for field, expected in expected_bindings.items()
            )
        ):
            raise RunnerFailure(
                f"terminal verifier did not pass: {verification!r}"
            )

    def inventory(self) -> list[dict[str, Any]]:
        items = []
        for path in sorted(self.output_dir.rglob("*")):
            if not path.is_file() or path.name.startswith("."):
                continue
            if path.name in {"failure.json", "terminal.json", "lease_release.json"}:
                continue
            try:
                item = {
                    "path": str(path.relative_to(self.output_dir)),
                    "bytes": path.stat().st_size,
                    "sha256": sha256(path),
                }
                if path.suffix == ".jsonl":
                    item["records"] = jsonl_records(path)
                elif path.suffix == ".csv":
                    shape = csv_shape(path)
                    item["data_rows"] = shape["data_rows"]
                    item["columns"] = shape["columns"]
                    item["complete"] = shape["complete"]
                items.append(item)
            except (OSError, RunnerFailure):
                continue
        return items

    def write_failure(self, failure: RunnerFailure) -> None:
        self.phase = "failure"
        value = {
            "protocol": PROTOCOL,
            "outcome": (
                "INCONCLUSIVE_FATAL"
                if failure.exit_code == 30
                else "INCONCLUSIVE_INFRASTRUCTURE"
            ),
            "failed_at": now(),
            "phase": self.phase_before_failure,
            "next_row": self.next_row,
            "runner": self.runner_identity,
            "active_child": self.active_child,
            "lease": self.lease,
            "latest_guard_snapshot": self.latest_snapshot,
            "reason": failure.reason,
            "exit_code": failure.exit_code,
            "child_rc": failure.child_rc,
            "pair_claim_path": (
                str(self.pair_claim_path) if self.pair_claim is not None else None
            ),
            "pair_claim_sha256": (
                sha256(self.pair_claim_path)
                if self.pair_claim is not None and self.pair_claim_path.is_file()
                else None
            ),
            "artifact_inventory": self.inventory(),
        }
        atomic_json(self.output_dir / "failure.json", value)

    def run(self) -> int:
        def signal_failure(signum: int, _frame: Any) -> None:
            raise RunnerFailure(f"runner received signal {signum}")

        signal.signal(signal.SIGINT, signal_failure)
        signal.signal(signal.SIGTERM, signal_failure)
        try:
            self.acquire_lease()
            self.claim_prepared_pair()
            self.write_initial_provenance()
            self.verify_reviewed_sources()
            self.verify_frozen()
            self.run_contract_smoke()
            for row in physical_order():
                self.next_row = row
                variant = (
                    self.control if row["variant"] == "control" else self.candidate
                )
                self.wait_for_load()
                self.verify_frozen()
                self.snapshot_processes(
                    f"row-{row['ordinal']:03d}-pre", manifest=self.guard_manifest
                )
                environment = os.environ.copy()
                environment.update(
                    {
                        "OWNED_APPEND_VARIANT": variant.name,
                        "OWNED_APPEND_MODE": "process",
                        "OWNED_APPEND_BATCH": str(row["batch"]),
                        "OWNED_APPEND_CYCLE": str(row["cycle"]),
                        "OWNED_APPEND_SLOT": str(row["slot"]),
                        "OWNED_APPEND_SOURCE": variant.source,
                        "OWNED_APPEND_BINARY_SHA256": variant.binary_sha256,
                        "OWNED_APPEND_CSV": str(self.csv_path),
                    }
                )
                child_failure: RunnerFailure | None = None
                try:
                    self.run_child(
                        kind="benchmark",
                        argv=[str(variant.binary)],
                        environment=environment,
                        output_path=self.output_dir
                        / "rows"
                        / f"{row['ordinal']:03d}-{variant.name}.log",
                        timeout=int(
                            os.environ.get("MESS_BENCH_ROW_TIMEOUT", "1800")
                        ),
                        csv_path=self.csv_path,
                        manifest=self.child_manifest,
                        expected_row_delta=1,
                        context=row,
                    )
                except RunnerFailure as failure:
                    child_failure = failure
                post_failure: RunnerFailure | None = None
                try:
                    self.snapshot_processes(
                        f"row-{row['ordinal']:03d}-post",
                        manifest=self.guard_manifest,
                    )
                except RunnerFailure as failure:
                    post_failure = failure
                if child_failure is not None:
                    if post_failure is not None:
                        child_failure.reason += (
                            f"; post-child guard also failed: {post_failure.reason}"
                        )
                    raise child_failure
                if post_failure is not None:
                    raise post_failure
                self.log(
                    f"completed row {row['ordinal']}/{EXPECTED_CHILDREN} "
                    f"{variant.name} b{row['batch']} c{row['cycle']} s{row['slot']}"
                )
            self.next_row = None
            rc, evaluator_record, evaluator_outcome = self.invoke_evaluator()
            return self.publish_terminal(
                evaluator_outcome["outcome"], rc, evaluator_record
            )
        except RunnerFailure as failure:
            self.phase_before_failure = self.phase
            self.write_failure(failure)
            self.release_lease("failure")
            self.log(f"fail-stop: {failure.reason}")
            return failure.exit_code
        except BaseException as error:
            failure = RunnerFailure(
                f"unhandled runner failure: {error.__class__.__name__}: {error}",
                exit_code=30,
            )
            self.phase_before_failure = self.phase
            self.write_failure(failure)
            self.release_lease("failure")
            self.log(f"fail-stop: {failure.reason}")
            return failure.exit_code


def ensure_distinct(control: FrozenVariant, candidate: FrozenVariant) -> None:
    if control.source == candidate.source:
        raise RunnerFailure("control and candidate commits are identical", exit_code=2)
    if control.binary_sha256 == candidate.binary_sha256:
        raise RunnerFailure("control and candidate binaries are identical", exit_code=2)
    if control.harness_sha256 != candidate.harness_sha256:
        raise RunnerFailure("control and candidate harnesses differ", exit_code=2)
    if control.cargo_lock_sha256 != candidate.cargo_lock_sha256:
        raise RunnerFailure("control and candidate Cargo.lock files differ", exit_code=2)


def replace_provenance(path: Path, transform: Any) -> None:
    lines = path.read_text().splitlines()
    updated = transform(lines)
    atomic_write(path, ("\n".join(updated) + "\n").encode())


def fixture_variant(root: Path) -> FrozenVariant:
    orchestrator = Path(__file__).resolve()
    true_binary = Path("/bin/true").resolve()
    return FrozenVariant(
        name="fixture",
        binary=true_binary,
        source="0" * 40,
        tree="0" * 40,
        root=root,
        materialized_root=root,
        harness=orchestrator,
        cargo_lock=orchestrator,
        target_dir=root,
        binary_sha256=sha256(true_binary),
        harness_sha256=sha256(orchestrator),
        cargo_lock_sha256=sha256(orchestrator),
        diff_manifest_sha256="0" * 64,
        patch_sha256="0" * 64,
        build_attestation=orchestrator,
        build_attestation_sha256=sha256(orchestrator),
        build_nonce="fixture",
        build_log=orchestrator,
        build_log_sha256=sha256(orchestrator),
        source_archive=orchestrator,
        source_archive_sha256=sha256(orchestrator),
        tree_manifest=orchestrator,
        tree_manifest_sha256=sha256(orchestrator),
        materialized_manifest=orchestrator,
        materialized_manifest_sha256=sha256(orchestrator),
        sandbox=true_binary,
        sandbox_sha256=sha256(true_binary),
        rustc="fixture",
        cargo="fixture",
        contract={"protocol": PROTOCOL, "contract_mode": True},
    )


def write_prepared_fixture(source_root: Path, output: Path) -> Path:
    preparation = load_prepare_module()
    approval_path = source_root / "source-approval.json"
    approval = load_canonical_json(approval_path, "bn-22it-source-approval-v1")
    roots = {
        name: source_root / f"{name}-root"
        for name in ("control", "candidate")
    }
    _, sources = preparation.validate_approval(approval_path, roots)
    output.mkdir()
    tooling_root = Path(__file__).resolve().parents[2]
    tooling_source = command_output(
        ["git", "-C", str(tooling_root), "rev-parse", "HEAD"]
    )
    tooling_tree = command_output(
        ["git", "-C", str(tooling_root), "rev-parse", "HEAD^{tree}"]
    )
    if approval["baseline"] != {
        "source": tooling_source,
        "tree": tooling_tree,
    }:
        raise RunnerFailure("fixture approval does not anchor tooling baseline")
    prepare_runner = Path(__file__).with_name("prepare_paired.sh").resolve()
    prepare_orchestrator = Path(__file__).with_name("prepare_paired.py").resolve()
    sandbox = Path(shutil.which("bwrap") or "").resolve(strict=True)
    attestations = {}
    for index, name in enumerate(("control", "candidate"), start=1):
        materialized = preparation.materialize_source(
            name, roots[name], sources[name], output
        )
        nonce = hashlib.sha256(
            f"bn-22it-prepared-fixture-{name}".encode()
        ).hexdigest()
        target = output / "targets" / f"{name}-{nonce}"
        binary = target / "release/examples/owned_append_bench"
        binary.parent.mkdir(parents=True)
        atomic_write(
            binary,
            f"#!/bin/sh\n# fixture {index}\nexit 0\n".encode(),
            mode=0o700,
        )
        build_log = output / "logs" / f"{name}-build.log"
        atomic_write(build_log, f"fixture build {name}\n".encode())
        diff_path = output / "sources" / f"{name}-diff.json"
        patch_path = output / "sources" / f"{name}.patch"
        atomic_write(diff_path, sources[name]["diff_bytes"])
        atomic_write(patch_path, sources[name]["patch_bytes"])
        contract = {
            "protocol": PROTOCOL,
            "baseline_source": approval["baseline"]["source"],
            "baseline_tree": approval["baseline"]["tree"],
            "source_commit": sources[name]["source"],
            "source_tree": sources[name]["tree"],
            "harness_sha256": approval["common"]["harness_sha256"],
            "cargo_lock_sha256": approval["common"]["cargo_lock_sha256"],
            "source_approval_sha256": sha256(approval_path),
            "build_nonce": nonce,
            "contract_mode": True,
            "csv_written": False,
        }
        contract_path = output / "logs" / f"{name}-contract.log"
        atomic_write(contract_path, canonical_json(contract))
        cargo_argv = [
            "cargo",
            "build",
            "--locked",
            "--release",
            "-p",
            "mess-store",
            "--example",
            "owned_append_bench",
            "--target-dir",
            str(target.resolve()),
        ]
        sandbox_prefix = [
            str(sandbox),
            "--die-with-parent",
            "--ro-bind",
            "/",
            "/",
            "--proc",
            "/proc",
            "--dev-bind",
            "/dev",
            "/dev",
            "--tmpfs",
            "/tmp",
            "--bind",
            str(target.resolve()),
            str(target.resolve()),
            "--chdir",
            str(materialized["materialized_root"].resolve()),
        ]
        build_argv = [*sandbox_prefix, *cargo_argv]
        contract_argv = [*sandbox_prefix, str(binary)]

        def fixture_lifecycle(
            argv: list[str], output_path: Path, ordinal: int
        ) -> dict[str, Any]:
            pid = 900_000 + index * 10 + ordinal
            starttime = 10_000 + index * 10 + ordinal
            started_ns = 2_000_000 + index * 100_000 + ordinal * 100
            return {
                "argv": argv,
                "cwd": str(materialized["materialized_root"].resolve()),
                "pid": pid,
                "starttime": starttime,
                "waited_pid": pid,
                "started_at": "2026-01-01T00:00:00+00:00",
                "completed_at": "2026-01-01T00:00:01+00:00",
                "started_monotonic_ns": started_ns,
                "completed_monotonic_ns": started_ns + 1,
                "exit_status": 0,
                "timed_out": False,
                "terminated_by_runner": False,
                "reaping": {
                    "status": "absent",
                    "pid": pid,
                    "starttime": starttime,
                },
                "process_group_absent": True,
                "output_path": str(output_path.resolve()),
                "output_sha256": sha256(output_path),
            }

        build_lifecycle = fixture_lifecycle(build_argv, build_log, 1)
        contract_lifecycle = fixture_lifecycle(contract_argv, contract_path, 2)
        materialized_digest = materialized["materialized_manifest_sha256"]
        attestation = {
            "schema": ATTESTATION_SCHEMA,
            "protocol": PROTOCOL,
            "variant": name,
            "baseline_source": approval["baseline"]["source"],
            "baseline_tree": approval["baseline"]["tree"],
            "source_commit": sources[name]["source"],
            "source_tree": sources[name]["tree"],
            "source_root": str(roots[name].resolve()),
            "materialized_root": str(materialized["materialized_root"].resolve()),
            "git_archive_argv": materialized["git_archive_argv"],
            "source_archive_path": str(
                materialized["source_archive_path"].resolve()
            ),
            "source_archive_sha256": materialized["source_archive_sha256"],
            "tree_manifest_path": str(
                materialized["tree_manifest_path"].resolve()
            ),
            "tree_manifest_sha256": materialized["tree_manifest_sha256"],
            "materialized_manifest_path": str(
                materialized["materialized_manifest_path"].resolve()
            ),
            "materialized_manifest_sha256": materialized_digest,
            "materialized_manifest_pre_sha256": materialized_digest,
            "materialized_manifest_post_sha256": materialized_digest,
            "source_read_only": True,
            "gitlinks_present": False,
            "source_approval_sha256": sha256(approval_path),
            "build_nonce": nonce,
            "target_dir": str(target.resolve()),
            "target_dir_was_absent": True,
            "binary_path": str(binary.resolve()),
            "binary_sha256": sha256(binary),
            "harness_path": str(
                (materialized["materialized_root"] / preparation.HARNESS_RELATIVE).resolve()
            ),
            "harness_sha256": sha256(
                materialized["materialized_root"] / preparation.HARNESS_RELATIVE
            ),
            "cargo_lock_path": str(
                (materialized["materialized_root"] / "Cargo.lock").resolve()
            ),
            "cargo_lock_sha256": sha256(
                materialized["materialized_root"] / "Cargo.lock"
            ),
            "diff_manifest_path": str(diff_path.resolve()),
            "diff_manifest_sha256": sha256(diff_path),
            "patch_path": str(patch_path.resolve()),
            "patch_sha256": sha256(patch_path),
            "build_log_path": str(build_log.resolve()),
            "build_log_sha256": sha256(build_log),
            "cargo_argv": cargo_argv,
            "sandbox_path": str(sandbox),
            "sandbox_sha256": sha256(sandbox),
            "build_argv": build_argv,
            "contract_argv": contract_argv,
            "build_started_at": now(),
            "build_completed_at": now(),
            "build_child": build_lifecycle,
            "rustc": "fixture",
            "cargo": "fixture",
            "contract_output_path": str(contract_path.resolve()),
            "contract_output_sha256": sha256(contract_path),
            "contract_child": contract_lifecycle,
            "contract": contract,
            "prepare_runner_path": str(prepare_runner),
            "prepare_runner_sha256": sha256(prepare_runner),
            "prepare_orchestrator_path": str(prepare_orchestrator),
            "prepare_orchestrator_sha256": sha256(prepare_orchestrator),
        }
        attestation_path = output / "attestations" / f"{name}.json"
        atomic_json(attestation_path, attestation)
        attestations[name] = {
            "path": str(attestation_path.resolve()),
            "sha256": sha256(attestation_path),
        }
    lease_path = output / "lease_event.json"
    fixture_identity = process_identity(os.getpid())
    lock_path = LOCK_PATH.resolve()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.touch(exist_ok=True)
    lock_stat = lock_path.stat()
    atomic_json(
        lease_path,
        {
            "protocol": PROTOCOL,
            "event": "prepare_acquired",
            "path": str(lock_path),
            "device": lock_stat.st_dev,
            "inode": lock_stat.st_ino,
            "holder": {
                "pid": fixture_identity["pid"],
                "comm": fixture_identity["comm"],
                "state": fixture_identity["state"],
                "ppid": fixture_identity["ppid"],
                "starttime": fixture_identity["starttime_ticks"],
            },
            "uid": os.getuid(),
            "hostname": socket.gethostname(),
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "nonce": hashlib.sha256(b"bn-22it-prepare-fixture-lease").hexdigest(),
            "acquired_at": now(),
            "acquired_monotonic_ns": 1_000_000,
        },
    )
    failure_path = output / "failure.json"
    pre_release = {
        "schema": "bn-22it-prepare-pre-release-v1",
        "protocol": PROTOCOL,
        "created_at": now(),
        "created_monotonic_ns": 4_000_000,
        "tooling_source": tooling_source,
        "tooling_tree": tooling_tree,
        "source_approval_path": str(approval_path.resolve()),
        "source_approval_sha256": sha256(approval_path),
        "prepare_runner_path": str(prepare_runner),
        "prepare_runner_sha256": sha256(prepare_runner),
        "prepare_orchestrator_path": str(prepare_orchestrator),
        "prepare_orchestrator_sha256": sha256(prepare_orchestrator),
        "lease_event_path": str(lease_path.resolve()),
        "lease_event_sha256": sha256(lease_path),
        "attestations": attestations,
        "failure_path": str(failure_path.resolve()),
        "failure_absent": True,
        "lease_held": True,
    }
    pre_release_path = output / "prepare_pre_release.json"
    atomic_json(pre_release_path, pre_release)
    release = {
        "schema": "bn-22it-prepare-release-v1",
        "protocol": PROTOCOL,
        "event": "prepare_released",
        "released_at": now(),
        "released_monotonic_ns": 4_000_001,
        "lease_nonce": hashlib.sha256(b"bn-22it-prepare-fixture-lease").hexdigest(),
        "pre_release_path": str(pre_release_path.resolve()),
        "pre_release_sha256": sha256(pre_release_path),
    }
    release_path = output / "lease_release.json"
    atomic_json(release_path, release)
    terminal = {
        "schema": "bn-22it-prepare-terminal-v1",
        "protocol": PROTOCOL,
        "outcome": "PREPARED",
        "completed_at": now(),
        "completed_monotonic_ns": 4_000_002,
        "pre_release_path": str(pre_release_path.resolve()),
        "pre_release_sha256": sha256(pre_release_path),
        "release_path": str(release_path.resolve()),
        "release_sha256": sha256(release_path),
        "failure_path": str(failure_path.resolve()),
        "failure_absent": True,
    }
    terminal_path = output / "prepare_terminal.json"
    atomic_json(terminal_path, terminal)
    pair = {
        "schema": PAIR_SCHEMA,
        "protocol": PROTOCOL,
        "created_at": now(),
        "created_monotonic_ns": 4_000_003,
        "tooling_source": tooling_source,
        "tooling_tree": tooling_tree,
        "source_approval_path": str(approval_path.resolve()),
        "source_approval_sha256": sha256(approval_path),
        "prepare_runner_path": str(prepare_runner),
        "prepare_runner_sha256": sha256(prepare_runner),
        "prepare_orchestrator_path": str(prepare_orchestrator),
        "prepare_orchestrator_sha256": sha256(prepare_orchestrator),
        "lease_event_path": str(lease_path.resolve()),
        "lease_event_sha256": sha256(lease_path),
        "pre_release_path": str(pre_release_path.resolve()),
        "pre_release_sha256": sha256(pre_release_path),
        "release_path": str(release_path.resolve()),
        "release_sha256": sha256(release_path),
        "terminal_path": str(terminal_path.resolve()),
        "terminal_sha256": sha256(terminal_path),
        "failure_path": str(failure_path.resolve()),
        "failure_absent": True,
        "attestations": attestations,
    }
    pair_path = output / "prepared-pair.json"
    atomic_json(pair_path, pair)
    return pair_path


def run_signal_worker(root: Path, evaluator: Path) -> int:
    root.mkdir(parents=True, exist_ok=False)
    os.environ["MESS_BENCH_COORDINATION_CONFIRMED"] = "true"
    dummy = fixture_variant(root)
    runner = MeasurementRunner(
        dummy,
        dummy,
        root,
        Path(__file__).with_name("run_paired.sh").resolve(),
        Path(__file__).resolve(),
        evaluator,
        lock_path=root / "signal.lock",
    )

    def interrupted(signum: int, _frame: Any) -> None:
        raise RunnerFailure(f"signal worker received {signum}")

    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    try:
        runner.acquire_lease()
        runner.phase = "signal_child"
        runner.run_child(
            kind="signal_fixture",
            argv=["/bin/sleep", "30"],
            environment=os.environ.copy(),
            output_path=root / "signal-child.log",
            timeout=60,
            csv_path=None,
            manifest=runner.child_manifest,
            expected_row_delta=0,
            context={"evidence_mode": "signal-fixture"},
            active_publication=root / "active-child.json",
        )
        raise RunnerFailure("signal worker child unexpectedly completed")
    except RunnerFailure as failure:
        runner.phase_before_failure = runner.phase
        runner.write_failure(failure)
        runner.release_lease("failure")
        return 20


def run_tooling_self_test(root: Path, evaluator: Path) -> int:
    """Exercise the evaluator transition and required fail-closed branches."""

    root = root.resolve()
    if root.exists() or root.is_symlink():
        print(f"refusing non-fresh self-test directory {root}", file=sys.stderr)
        return 2
    root.parent.mkdir(parents=True, exist_ok=True)
    root.mkdir()
    checks: dict[str, bool] = {}
    details: dict[str, Any] = {}

    single_use_path = root / "single-use-pair-claim.json"
    atomic_create_json(
        single_use_path,
        {"schema": PAIR_CLAIM_SCHEMA, "protocol": PROTOCOL, "fixture": True},
    )
    try:
        atomic_create_json(
            single_use_path,
            {
                "schema": PAIR_CLAIM_SCHEMA,
                "protocol": PROTOCOL,
                "fixture": "reuse",
            },
        )
    except RunnerFailure:
        checks["single_use_pair_reuse_rejected"] = True
    else:
        checks["single_use_pair_reuse_rejected"] = False

    # Reconstruct and validate the reviewed-source/preparation chain without
    # invoking Cargo.  The prepare self-test creates two real divergent Git
    # histories; the runner fixture then binds synthetic executables to those
    # exact commits, trees, patches, and immutable manifests.
    prepare_self_test_root = root / "prepare-self-test"
    prepare_orchestrator = Path(__file__).with_name("prepare_paired.py").resolve()
    prepare_unit = subprocess.run(
        [
            sys.executable,
            str(prepare_orchestrator),
            "--self-test",
            str(prepare_self_test_root),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    prepare_report_path = prepare_self_test_root / "self-test.json"
    prepare_report = (
        json.loads(prepare_report_path.read_text())
        if prepare_report_path.is_file()
        else {}
    )
    checks["prepare_source_self_test"] = (
        prepare_unit.returncode == 0
        and prepare_report.get("outcome") == "SELF_TEST_PASS"
    )
    if not checks["prepare_source_self_test"]:
        details["prepare_source_self_test"] = {
            "rc": prepare_unit.returncode,
            "stdout": prepare_unit.stdout,
            "stderr": prepare_unit.stderr,
            "report": prepare_report,
        }

    prepared_fixture_root = root / "prepared-fixture"
    try:
        prepared_pair_path = write_prepared_fixture(
            prepare_self_test_root, prepared_fixture_root
        )
        prepared_control, prepared_candidate, prepared_context = (
            inspect_prepared_pair(prepared_pair_path)
        )
        ensure_distinct(prepared_control, prepared_candidate)
        checks["prepared_pair_round_trip"] = all(
            (
                prepared_control.source
                == prepare_report.get("control_source"),
                prepared_candidate.source
                == prepare_report.get("candidate_source"),
                prepared_control.tree != prepared_candidate.tree,
                prepared_context.pair_sha256 == sha256(prepared_pair_path),
                prepared_context.approval_sha256
                == sha256(prepare_self_test_root / "source-approval.json"),
            )
        )
        fixture_paths = {
            "pair": prepared_pair_path,
            "pre_release": prepared_context.pre_release,
            "release": prepared_context.release,
            "terminal": prepared_context.terminal,
            "control_attestation": prepared_control.build_attestation,
            "candidate_attestation": prepared_candidate.build_attestation,
        }
        fixture_payloads = {
            name: path.read_bytes() for name, path in fixture_paths.items()
        }

        def restore_prepared_fixture() -> None:
            for name, path in fixture_paths.items():
                atomic_write(path, fixture_payloads[name])
            prepared_control.materialized_root.chmod(0o555)
            prepared_candidate.materialized_root.chmod(0o555)

        def rebind_prepared_fixture() -> None:
            pair_value = load_canonical_object(prepared_pair_path)
            for variant in ("control", "candidate"):
                attestation_path = fixture_paths[f"{variant}_attestation"]
                pair_value["attestations"][variant]["sha256"] = sha256(
                    attestation_path
                )
            pre_release_value = load_canonical_object(
                prepared_context.pre_release
            )
            pre_release_value["attestations"] = pair_value["attestations"]
            atomic_json(prepared_context.pre_release, pre_release_value)
            release_value = load_canonical_object(prepared_context.release)
            release_value["pre_release_sha256"] = sha256(
                prepared_context.pre_release
            )
            atomic_json(prepared_context.release, release_value)
            terminal_value = load_canonical_object(prepared_context.terminal)
            terminal_value["pre_release_sha256"] = sha256(
                prepared_context.pre_release
            )
            terminal_value["release_sha256"] = sha256(
                prepared_context.release
            )
            atomic_json(prepared_context.terminal, terminal_value)
            pair_value["pre_release_sha256"] = sha256(
                prepared_context.pre_release
            )
            pair_value["release_sha256"] = sha256(prepared_context.release)
            pair_value["terminal_sha256"] = sha256(prepared_context.terminal)
            atomic_json(prepared_pair_path, pair_value)

        def prepared_mutation_rejected(mutation: Any) -> bool:
            restore_prepared_fixture()
            try:
                mutation()
                inspect_prepared_pair(prepared_pair_path)
            except (RunnerFailure, OSError, KeyError, TypeError, ValueError):
                return True
            finally:
                restore_prepared_fixture()
            return False

        mutated_pair = dict(prepared_context.pair)
        mutated_pair["source_approval_sha256"] = "0" * 64
        mutated_pair_path = prepared_fixture_root / "mutated-pair.json"
        atomic_json(mutated_pair_path, mutated_pair)
        try:
            inspect_prepared_pair(mutated_pair_path)
        except (RunnerFailure, OSError, KeyError, TypeError, ValueError):
            checks["prepared_pair_mutation_rejected"] = True
        else:
            checks["prepared_pair_mutation_rejected"] = False

        checks["materialized_root_mode_drift_rejected"] = (
            prepared_mutation_rejected(
                lambda: prepared_control.materialized_root.chmod(0o755)
            )
        )

        def add_lifecycle_key() -> None:
            value = load_canonical_object(prepared_context.pre_release)
            value["unexpected"] = True
            atomic_json(prepared_context.pre_release, value)
            rebind_prepared_fixture()

        checks["extra_lifecycle_key_rejected"] = prepared_mutation_rejected(
            add_lifecycle_key
        )

        def remove_lifecycle_key() -> None:
            value = load_canonical_object(prepared_context.terminal)
            del value["failure_absent"]
            atomic_json(prepared_context.terminal, value)
            rebind_prepared_fixture()

        checks["missing_lifecycle_key_rejected"] = (
            prepared_mutation_rejected(remove_lifecycle_key)
        )

        def overlap_preparation_children() -> None:
            control_attestation = load_canonical_object(
                prepared_control.build_attestation
            )
            candidate_attestation = load_canonical_object(
                prepared_candidate.build_attestation
            )
            control_contract_completed = control_attestation["contract_child"][
                "completed_monotonic_ns"
            ]
            candidate_attestation["build_child"][
                "started_monotonic_ns"
            ] = control_contract_completed - 2
            candidate_attestation["build_child"][
                "completed_monotonic_ns"
            ] = control_contract_completed - 1
            atomic_json(
                prepared_candidate.build_attestation,
                candidate_attestation,
            )
            rebind_prepared_fixture()

        checks["overlapping_preparation_children_rejected"] = (
            prepared_mutation_rejected(overlap_preparation_children)
        )
    except (RunnerFailure, OSError, KeyError, TypeError, ValueError) as error:
        checks["prepared_pair_round_trip"] = False
        checks["prepared_pair_mutation_rejected"] = False
        checks["materialized_root_mode_drift_rejected"] = False
        checks["extra_lifecycle_key_rejected"] = False
        checks["missing_lifecycle_key_rejected"] = False
        checks["overlapping_preparation_children_rejected"] = False
        details["prepared_pair_round_trip"] = (
            f"{error.__class__.__name__}: {error}"
        )

    # Traverse the same lifecycle primitives as admission with a harmless
    # direct child and a private cooperative lease.  This is transition proof,
    # never benchmark evidence.
    wrapper = Path(__file__).with_name("run_paired.sh").resolve()
    orchestrator = Path(__file__).resolve()
    true_binary = Path("/bin/true").resolve()
    dummy = fixture_variant(root)
    coordination_before = os.environ.get("MESS_BENCH_COORDINATION_CONFIRMED")
    os.environ["MESS_BENCH_COORDINATION_CONFIRMED"] = "true"
    lifecycle_root = root / "shared-lifecycle"
    lifecycle_root.mkdir()
    lifecycle = MeasurementRunner(
        dummy,
        dummy,
        lifecycle_root,
        wrapper,
        orchestrator,
        evaluator,
        lock_path=root / "shared-lifecycle.lock",
    )
    try:
        lifecycle.acquire_lease()
        pre_guard = lifecycle.snapshot_processes(
            "fixture-pre", manifest=None, directory=lifecycle_root / "guards"
        )
        child_record = lifecycle.run_child(
            kind="fixture_child",
            argv=[str(true_binary)],
            environment={
                **os.environ,
                "OWNED_APPEND_BINARY_SHA256": sha256(true_binary),
            },
            output_path=lifecycle_root / "fixture-child.log",
            timeout=30,
            csv_path=None,
            manifest=None,
            expected_row_delta=0,
            context={"evidence_mode": "fixture"},
        )
        post_guard = lifecycle.snapshot_processes(
            "fixture-post", manifest=None, directory=lifecycle_root / "guards"
        )
        atomic_json(
            lifecycle.result_path,
            {"protocol": PROTOCOL, "outcome": "FIXTURE_PASS"},
        )
        atomic_write(
            lifecycle.provenance_path,
            f"protocol={PROTOCOL}\nevidence_mode=fixture\n".encode(),
        )
        lifecycle.pair_claim = {
            "schema": PAIR_CLAIM_SCHEMA,
            "protocol": PROTOCOL,
            "fixture": True,
        }
        atomic_json(lifecycle.pair_claim_path, lifecycle.pair_claim)
        terminal_rc = lifecycle.publish_terminal(
            "FIXTURE_PASS", 0, child_record, verify=False
        )
        checks["shared_lifecycle_transition"] = all(
            (
                pre_guard["verdict"] == "pass",
                post_guard["verdict"] == "pass",
                child_record["exit_status"] == 0,
                child_record["waited_pid"] == child_record["pid"],
                child_record["reaping"]["status"] in {"absent", "pid_reused"},
                terminal_rc == 0,
                (lifecycle_root / "terminal_pre_release.json").is_file(),
                (lifecycle_root / "lease_release.json").is_file(),
                (lifecycle_root / "terminal.json").is_file(),
            )
        )
    except RunnerFailure as failure:
        lifecycle.phase_before_failure = lifecycle.phase
        lifecycle.write_failure(failure)
        lifecycle.release_lease("failure")
        checks["shared_lifecycle_transition"] = False
        details["shared_lifecycle_transition"] = failure.reason

    # Exercise the runner side of the automatic post-release verification
    # interface with an independent, deterministic verifier.  This proves the
    # runner invokes the frozen CLI, reaps it, and rejects anything except the
    # exact bound result; evaluator-side verifier coverage is separate.
    verifier_root = root / "terminal-verifier-interface"
    verifier_root.mkdir()
    verifier = verifier_root / "verifier.py"
    atomic_write(
        verifier,
        (
            "#!/usr/bin/env python3\n"
            "import hashlib, json, os, sys, tempfile\n"
            "from datetime import datetime, timezone\n"
            "from pathlib import Path\n"
            "def digest(path):\n"
            "    return hashlib.sha256(path.read_bytes()).hexdigest()\n"
            "if len(sys.argv) != 4 or sys.argv[1] != '--verify-terminal':\n"
            "    raise SystemExit(2)\n"
            "root = Path(sys.argv[2]).resolve()\n"
            "target = Path(sys.argv[3]).resolve()\n"
            "paths = {\n"
            "    'terminal': root / 'terminal.json',\n"
            "    'release': root / 'lease_release.json',\n"
            "    'pre_release': root / 'terminal_pre_release.json',\n"
            "    'result': root / 'result.json',\n"
            "    'provenance': root / 'provenance.txt',\n"
            "    'pair_claim': root / 'fixture-consumption.json',\n"
            "}\n"
            "value = {\n"
            "    'schema': 'bn-22it-terminal-verification-v1',\n"
            "    'protocol': 'bn-22it-process-owned-v1',\n"
            "    'outcome': 'TERMINAL_VERIFIED',\n"
            "    'verified_at': datetime.now(timezone.utc).isoformat(),\n"
            "    'output_dir': str(root),\n"
            "    'terminal_path': str(paths['terminal']),\n"
            "    'terminal_sha256': digest(paths['terminal']),\n"
            "    'release_path': str(paths['release']),\n"
            "    'release_sha256': digest(paths['release']),\n"
            "    'pre_release_path': str(paths['pre_release']),\n"
            "    'pre_release_sha256': digest(paths['pre_release']),\n"
            "    'result_path': str(paths['result']),\n"
            "    'result_sha256': digest(paths['result']),\n"
            "    'provenance_path': str(paths['provenance']),\n"
            "    'provenance_sha256': digest(paths['provenance']),\n"
            "    'pair_claim_path': str(paths['pair_claim']),\n"
            "    'pair_claim_sha256': digest(paths['pair_claim']),\n"
            "    'errors': [],\n"
            "}\n"
            "target.parent.mkdir(parents=True, exist_ok=True)\n"
            "fd, temporary = tempfile.mkstemp(dir=target.parent, prefix='.verify.')\n"
            "try:\n"
            "    with os.fdopen(fd, 'w') as handle:\n"
            "        json.dump(value, handle, sort_keys=True, separators=(',', ':'))\n"
            "        handle.write('\\n')\n"
            "        handle.flush()\n"
            "        os.fsync(handle.fileno())\n"
            "    os.replace(temporary, target)\n"
            "finally:\n"
            "    try:\n"
            "        os.unlink(temporary)\n"
            "    except FileNotFoundError:\n"
            "        pass\n"
        ).encode(),
        mode=0o755,
    )
    verifier_runner = MeasurementRunner(
        dummy,
        dummy,
        verifier_root,
        wrapper,
        orchestrator,
        verifier,
        lock_path=root / "terminal-verifier-interface.lock",
    )
    for path, value in (
        (verifier_runner.result_path, {"outcome": "FIXTURE_PASS"}),
        (verifier_root / "terminal_pre_release.json", {"stage": "pre"}),
        (verifier_root / "lease_release.json", {"stage": "release"}),
        (verifier_root / "terminal.json", {"stage": "terminal"}),
        (verifier_runner.pair_claim_path, {"stage": "claim"}),
    ):
        atomic_json(path, value)
    atomic_write(verifier_runner.provenance_path, b"fixture=true\n")
    try:
        verifier_runner.verify_terminal()
        verification = json.loads(
            (verifier_root / "terminal-verification.json").read_text()
        )
        checks["automatic_terminal_verifier_interface"] = all(
            (
                verification.get("outcome") == "TERMINAL_VERIFIED",
                verification.get("errors") == [],
                (verifier_root / "terminal-verifier.log").is_file(),
                (verifier_root / "active-child.json").is_file(),
                verifier_runner.active_child is None,
            )
        )
    except (RunnerFailure, OSError, ValueError) as error:
        checks["automatic_terminal_verifier_interface"] = False
        details["automatic_terminal_verifier_interface"] = (
            f"{error.__class__.__name__}: {error}"
        )

    failure_root = root / "shared-failure"
    failure_root.mkdir()
    failure_lifecycle = MeasurementRunner(
        dummy,
        dummy,
        failure_root,
        wrapper,
        orchestrator,
        evaluator,
        lock_path=root / "shared-failure.lock",
    )
    failure_lifecycle.acquire_lease()
    failure_lifecycle.phase = "fixture_forced_failure"
    failure_lifecycle.phase_before_failure = failure_lifecycle.phase
    forced = RunnerFailure("fixture forced fail-stop")
    failure_lifecycle.write_failure(forced)
    failure_lifecycle.release_lease("failure")
    failure_payload = json.loads(
        (failure_root / "failure.json").read_text()
    )
    checks["shared_failure_publication"] = (
        failure_payload.get("phase") == "fixture_forced_failure"
        and failure_payload.get("reason") == "fixture forced fail-stop"
        and (failure_root / "lease_release.json").is_file()
    )

    signal_root = root / "signal-injection"
    signal_log = root / "signal-worker.log"
    with signal_log.open("wb") as output:
        signal_worker = subprocess.Popen(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--signal-worker",
                str(signal_root),
            ],
            stdout=output,
            stderr=subprocess.STDOUT,
            env=os.environ.copy(),
        )
        active_path = signal_root / "active-child.json"
        deadline = time.monotonic() + 10
        while not active_path.is_file() and time.monotonic() < deadline:
            if signal_worker.poll() is not None:
                break
            time.sleep(0.01)
        if active_path.is_file():
            active = json.loads(active_path.read_text())
            os.kill(signal_worker.pid, signal.SIGTERM)
            signal_rc = signal_worker.wait(timeout=20)
            child_pid = int(active["child"]["pid"])
            try:
                child_after = process_identity(child_pid)
            except FileNotFoundError:
                child_after = None
            child_gone = child_after is None or child_after["starttime_ticks"] != int(
                active["child"]["starttime_ticks"]
            )
        else:
            signal_worker.terminate()
            signal_rc = signal_worker.wait(timeout=10)
            child_gone = False
    signal_failure = signal_root / "failure.json"
    signal_release = signal_root / "lease_release.json"
    signal_manifest = signal_root / "child_manifest.jsonl"
    signal_artifacts = [signal_failure, signal_release, signal_manifest]
    before_delay = {
        str(path): sha256(path) for path in signal_artifacts if path.is_file()
    }
    time.sleep(0.1)
    after_delay = {
        str(path): sha256(path) for path in signal_artifacts if path.is_file()
    }
    signal_record = (
        json.loads(signal_manifest.read_text().splitlines()[0])
        if signal_manifest.is_file()
        else {}
    )
    checks["live_child_signal_cleanup"] = all(
        (
            signal_rc == 20,
            child_gone,
            signal_failure.is_file(),
            signal_release.is_file(),
            signal_record.get("terminated_by_runner") is True,
            signal_record.get("process_group_absent") is True,
            signal_record.get("reaping", {}).get("status")
            in {"absent", "pid_reused"},
            before_delay == after_delay,
        )
    )
    if coordination_before is None:
        os.environ.pop("MESS_BENCH_COORDINATION_CONFIRMED", None)
    else:
        os.environ["MESS_BENCH_COORDINATION_CONFIRMED"] = coordination_before

    evaluator_unit = root / "evaluator-self-test.json"
    unit = subprocess.run(
        [str(evaluator), "--self-test", str(evaluator_unit)],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    checks["evaluator_self_test"] = (
        unit.returncode == 0
        and evaluator_unit.is_file()
        and json.loads(evaluator_unit.read_text()).get("outcome")
        == "SELF_TEST_PASS"
    )

    fixture_root = root / "fixture-pass"
    fixture_csv, fixture_provenance, fixture_result = write_fixture_evidence(
        fixture_root
    )
    passed = subprocess.run(
        [
            str(evaluator),
            "--fixture",
            str(fixture_csv),
            str(fixture_provenance),
            str(fixture_result),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    checks["exact_fixture_transition"] = (
        passed.returncode == 0
        and fixture_result.is_file()
        and json.loads(fixture_result.read_text()).get("outcome")
        == "FIXTURE_PASS"
    )

    def invalid_case(
        name: str, mutation: Any
    ) -> tuple[bool, dict[str, Any]]:
        case_root = root / name
        csv_path, provenance_path, result_path = write_fixture_evidence(case_root)
        mutation(csv_path, provenance_path, case_root)
        result = subprocess.run(
            [
                str(evaluator),
                "--fixture",
                str(csv_path),
                str(provenance_path),
                str(result_path),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
        payload = json.loads(result_path.read_text()) if result_path.exists() else {}
        return (
            result.returncode == 20
            and payload.get("outcome") == "FIXTURE_INVALID",
            {
                "rc": result.returncode,
                "outcome": payload.get("outcome"),
                "errors": payload.get("errors", []),
            },
        )

    checks["missing_final_csv_binding"], details["missing_final_csv_binding"] = (
        invalid_case(
            "missing-final-binding",
            lambda _csv, provenance, _root: replace_provenance(
                provenance,
                lambda lines: [
                    line
                    for line in lines
                    if not line.startswith("paired_csv_sha256=")
                ],
            ),
        )
    )
    checks["wrong_cardinality"], details["wrong_cardinality"] = invalid_case(
        "wrong-cardinality",
        lambda _csv, provenance, _root: replace_provenance(
            provenance,
            lambda lines: [
                "guard_manifest_records=2"
                if line.startswith("guard_manifest_records=")
                else line
                for line in lines
            ],
        ),
    )

    def wrong_order(csv_path: Path, provenance: Path, _root: Path) -> None:
        lines = csv_path.read_text().splitlines()
        lines[1], lines[2] = lines[2], lines[1]
        atomic_write(csv_path, ("\n".join(lines) + "\n").encode())
        digest = sha256(csv_path)
        replace_provenance(
            provenance,
            lambda current: [
                f"paired_csv_sha256={digest}"
                if line.startswith("paired_csv_sha256=")
                else line
                for line in current
            ],
        )

    checks["wrong_physical_order"], details["wrong_physical_order"] = invalid_case(
        "wrong-order", wrong_order
    )
    checks["missing_manifest_binding"], details["missing_manifest_binding"] = (
        invalid_case(
            "missing-manifest-binding",
            lambda _csv, provenance, _root: replace_provenance(
                provenance,
                lambda lines: [
                    line
                    for line in lines
                    if not line.startswith("child_manifest_path=")
                ],
            ),
        )
    )

    lock_path = root / "lease.lock"
    first = lock_path.open("a+b", buffering=0)
    second = lock_path.open("a+b", buffering=0)
    try:
        fcntl.flock(first.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            fcntl.flock(second.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            checks["held_lease_rejected"] = True
        else:
            checks["held_lease_rejected"] = False
    finally:
        fcntl.flock(first.fileno(), fcntl.LOCK_UN)
        first.close()
        second.close()

    sleeper = subprocess.Popen(["/bin/sleep", "30"])
    sleeper_identity = process_identity(sleeper.pid)
    checks["unreaped_child_detected"] = same_identity(
        process_identity(sleeper.pid), sleeper_identity
    )
    sleeper.terminate()
    sleeper.wait(timeout=10)
    try:
        after_sleep = process_identity(sleeper.pid)
    except FileNotFoundError:
        after_sleep = None
    checks["reaped_child_absent"] = (
        after_sleep is None or not same_identity(after_sleep, sleeper_identity)
    )
    checks["unidentified_process_rejected"] = (
        classify_process(
            {"pid": 999999, "starttime_ticks": 1, "comm": "cargo"},
            [],
            process_identity(os.getpid()),
            None,
            (),
        )
        == "forbidden_unexplained"
    )
    try:
        subprocess.run(
            [str(root / "missing-evaluator")], check=False, timeout=1
        )
    except OSError:
        checks["missing_evaluator_rejected"] = True
    else:
        checks["missing_evaluator_rejected"] = False
    false_result = subprocess.run(["/bin/false"], check=False, timeout=10)
    checks["nonzero_evaluator_rejected"] = false_result.returncode != 0

    repository_root = Path(__file__).resolve().parents[2]
    try:
        repository_head = command_output(
            ["git", "-C", str(repository_root), "rev-parse", "HEAD"]
        )
        repository_tree = command_output(
            ["git", "-C", str(repository_root), "rev-parse", "HEAD^{tree}"]
        )
    except (OSError, subprocess.SubprocessError):
        repository_head = None
        repository_tree = None
    tool_paths = {
        "runner": Path(__file__).with_name("run_paired.sh").resolve(),
        "orchestrator": Path(__file__).resolve(),
        "evaluator": evaluator.resolve(),
        "prepare_runner": Path(__file__)
        .with_name("prepare_paired.sh")
        .resolve(),
        "prepare_orchestrator": prepare_orchestrator,
    }
    report = {
        "schema": "bn-22it-runner-self-test-v2",
        "protocol": PROTOCOL,
        "outcome": "SELF_TEST_PASS" if all(checks.values()) else "SELF_TEST_FAILED",
        "completed_at": now(),
        "repository_head": repository_head,
        "repository_tree": repository_tree,
        "tool_hashes": {
            name: {"path": str(path), "sha256": sha256(path)}
            for name, path in tool_paths.items()
        },
        "check_count": len(checks),
        "checks": checks,
        "details": details,
        "fixture_result_sha256": (
            sha256(fixture_result) if fixture_result.exists() else None
        ),
    }
    atomic_json(root / "self-test.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if all(checks.values()) else 30


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="run the frozen bn-22it Process-only comparison"
    )
    parser.add_argument("prepared_pair", type=Path)
    parser.add_argument("output_dir", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    if raw[:1] == ["--signal-worker"]:
        if len(raw) != 2:
            return 2
        evaluator = Path(__file__).with_name("evaluate.py").resolve()
        return run_signal_worker(Path(raw[1]).resolve(), evaluator)
    if raw[:1] == ["--self-test"]:
        if len(raw) != 2:
            print("usage: run_paired.sh --self-test <fresh-output-dir>", file=sys.stderr)
            return 2
        evaluator = Path(__file__).with_name("evaluate.py").resolve()
        try:
            return run_tooling_self_test(Path(raw[1]), evaluator)
        except BaseException as error:
            print(f"self-test failed internally: {error}", file=sys.stderr)
            return 30
    args = parse_args(raw)
    wrapper = Path(__file__).with_name("run_paired.sh").resolve()
    orchestrator = Path(__file__).resolve()
    evaluator = Path(__file__).with_name("evaluate.py").resolve()
    try:
        tooling_root = Path(__file__).resolve().parents[2]
        if command_output(
            ["git", "-C", str(tooling_root), "status", "--porcelain"]
        ):
            raise RunnerFailure("tooling checkout is dirty", exit_code=2)
        control, candidate, prepared = inspect_prepared_pair(args.prepared_pair)
        ensure_distinct(control, candidate)
        if not evaluator.is_file() or not os.access(evaluator, os.X_OK):
            raise RunnerFailure(f"evaluator is not executable: {evaluator}", exit_code=2)
        output = args.output_dir.expanduser().resolve()
        if output.exists() or output.is_symlink():
            raise RunnerFailure(
                f"refusing non-fresh output directory {output}", exit_code=2
            )
        for variant in (control, candidate):
            if output == variant.root or variant.root in output.parents:
                raise RunnerFailure(
                    "output directory must be outside frozen source workspaces",
                    exit_code=2,
                )
        if output == prepared.pair_path.parent or prepared.pair_path.parent in output.parents:
            raise RunnerFailure(
                "output directory must be outside prepared artifact directory",
                exit_code=2,
            )
        output.parent.mkdir(parents=True, exist_ok=True)
        output.mkdir()
        try:
            runner = MeasurementRunner(
                control,
                candidate,
                output,
                wrapper,
                orchestrator,
                evaluator,
                prepared=prepared,
            )
        except BaseException as error:
            atomic_json(
                output / "failure.json",
                {
                    "protocol": PROTOCOL,
                    "outcome": "INCONCLUSIVE_FATAL",
                    "phase": "runner_constructor",
                    "failed_at": now(),
                    "reason": f"{error.__class__.__name__}: {error}",
                },
            )
            raise
        return runner.run()
    except RunnerFailure as failure:
        print(f"fail-stop: {failure.reason}", file=sys.stderr)
        return failure.exit_code
    except (OSError, subprocess.SubprocessError) as error:
        print(f"preflight failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
