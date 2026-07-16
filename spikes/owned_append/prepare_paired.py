#!/usr/bin/env python3
"""Prepare fresh, source-attested bn-22it release artifacts.

This stage is deliberately separate from timed measurement.  It accepts a
reviewed source-approval manifest, replays every approved source proof, builds
sequentially into never-before-existing target directories, and reads a
compile-time identity back from each binary before publishing a prepared pair.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import io
import json
import os
import re
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tarfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROTOCOL = "bn-22it-process-owned-v1"
APPROVAL_SCHEMA = "bn-22it-source-approval-v1"
ATTESTATION_SCHEMA = "bn-22it-build-attestation-v2"
PAIR_SCHEMA = "bn-22it-prepared-pair-v2"
MATERIALIZATION_SCHEMA = "bn-22it-source-manifest-v1"
PRE_RELEASE_SCHEMA = "bn-22it-prepare-pre-release-v1"
RELEASE_SCHEMA = "bn-22it-prepare-release-v1"
TERMINAL_SCHEMA = "bn-22it-prepare-terminal-v1"
LOCK_PATH = Path.home() / ".cache/mess-bench/global-measurement.lock"
VARIANTS = ("control", "candidate")
HARNESS_RELATIVE = Path("crates/mess-store/examples/owned_append_bench.rs")
GIT_OBJECT = re.compile(r"[0-9a-f]{40}")
SHA256 = re.compile(r"[0-9a-f]{64}")


class PrepareFailure(Exception):
    pass


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode() + b"\n"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


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
    atomic_write(path, canonical_json(value))


def load_canonical_json(path: Path, schema: str) -> dict[str, Any]:
    try:
        payload = path.read_bytes()
        value = json.loads(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PrepareFailure(f"cannot read JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise PrepareFailure(f"{path} is not a JSON object")
    if payload != canonical_json(value):
        raise PrepareFailure(f"{path} is not canonical JSON")
    if value.get("schema") != schema or value.get("protocol") != PROTOCOL:
        raise PrepareFailure(f"{path} schema/protocol mismatch")
    return value


def require_exact_keys(
    value: dict[str, Any], expected: set[str], context: str
) -> None:
    observed = set(value)
    if observed != expected:
        raise PrepareFailure(
            f"{context} keys differ: missing={sorted(expected - observed)} "
            f"extra={sorted(observed - expected)}"
        )


def git_bytes(root: Path, arguments: list[str]) -> bytes:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), *arguments],
            check=True,
            capture_output=True,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise PrepareFailure(
            f"git {' '.join(arguments)} failed for {root}: {error}"
        ) from error
    return result.stdout


def git_text(root: Path, arguments: list[str]) -> str:
    return git_bytes(root, arguments).decode().strip()


def tooling_identity(*, require_clean: bool) -> dict[str, Any]:
    root = Path(__file__).resolve().parents[2]
    source = git_text(root, ["rev-parse", "HEAD"])
    tree = git_text(root, ["rev-parse", "HEAD^{tree}"])
    dirty = git_text(root, ["status", "--porcelain"])
    if require_clean and dirty:
        raise PrepareFailure("tooling checkout is dirty")
    return {"root": root, "source": source, "tree": tree, "dirty": bool(dirty)}


def canonical_changes(root: Path, baseline: str, source: str) -> list[dict[str, str]]:
    payload = git_bytes(
        root,
        [
            "diff",
            "--name-status",
            "--no-renames",
            "-z",
            f"{baseline}..{source}",
        ],
    )
    tokens = payload.split(b"\0")
    if tokens and not tokens[-1]:
        tokens.pop()
    if len(tokens) % 2:
        raise PrepareFailure("malformed git name-status output")
    changes = []
    for index in range(0, len(tokens), 2):
        try:
            status = tokens[index].decode("ascii")
            path = tokens[index + 1].decode("utf-8")
        except UnicodeDecodeError as error:
            raise PrepareFailure(f"non-canonical diff path/status: {error}") from error
        if status not in {"A", "M", "D"}:
            raise PrepareFailure(f"unsupported diff status {status!r} for {path!r}")
        if not path or path.startswith("/") or "\0" in path:
            raise PrepareFailure(f"unsafe diff path {path!r}")
        changes.append({"status": status, "path": path})
    return sorted(changes, key=lambda item: (item["path"], item["status"]))


def canonical_patch(
    root: Path,
    baseline: str,
    source: str,
    changes: list[dict[str, str]],
) -> bytes:
    paths = [change["path"] for change in changes]
    return git_bytes(
        root,
        [
            "diff",
            "--binary",
            "--full-index",
            "--no-ext-diff",
            "--no-renames",
            f"{baseline}..{source}",
            "--",
            *paths,
        ],
    )


def git_tree_entries(root: Path, source: str) -> list[dict[str, Any]]:
    payload = git_bytes(root, ["ls-tree", "-r", "-z", source])
    metadata_entries = []
    for raw in payload.split(b"\0"):
        if not raw:
            continue
        try:
            metadata, path_raw = raw.split(b"\t", 1)
            mode_raw, kind_raw, object_raw = metadata.split(b" ", 2)
            mode = mode_raw.decode("ascii")
            kind = kind_raw.decode("ascii")
            object_id = object_raw.decode("ascii")
            path = path_raw.decode("utf-8")
        except (ValueError, UnicodeDecodeError) as error:
            raise PrepareFailure(f"malformed git tree entry: {error}") from error
        if kind != "blob" or mode not in {"100644", "100755"}:
            raise PrepareFailure(
                f"unsupported source entry {mode} {kind} {path!r}; "
                "symlinks and gitlinks are forbidden"
            )
        metadata_entries.append(
            {
                "path": path,
                "mode": mode,
                "object_id": object_id,
            }
        )
    metadata_entries.sort(key=lambda item: item["path"])
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "cat-file", "--batch"],
            input=b"".join(
                f"{entry['object_id']}\n".encode() for entry in metadata_entries
            ),
            check=True,
            capture_output=True,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise PrepareFailure(f"git cat-file batch failed: {error}") from error
    stream = io.BytesIO(result.stdout)
    entries = []
    for metadata_entry in metadata_entries:
        header = stream.readline().rstrip(b"\n").split()
        if len(header) != 3 or header[1] != b"blob":
            raise PrepareFailure("malformed git cat-file batch header")
        try:
            size = int(header[2])
        except ValueError as error:
            raise PrepareFailure("invalid git cat-file blob size") from error
        blob = stream.read(size)
        if len(blob) != size or stream.read(1) != b"\n":
            raise PrepareFailure("truncated git cat-file batch payload")
        entries.append(
            {
                **metadata_entry,
                "size": size,
                "sha256": sha256_bytes(blob),
            }
        )
    if stream.read():
        raise PrepareFailure("extra git cat-file batch output")
    return entries


def source_manifest(
    variant: str,
    source: dict[str, Any],
    root: Path | None,
    entries: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema": MATERIALIZATION_SCHEMA,
        "protocol": PROTOCOL,
        "variant": variant,
        "source_commit": source["source"],
        "source_tree": source["tree"],
        "root": str(root.resolve()) if root is not None else None,
        "entries": entries,
    }


def filesystem_entries(
    root: Path, expected: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    expected_by_path = {entry["path"]: entry for entry in expected}
    observed_files: dict[str, Path] = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            raise PrepareFailure(f"materialized source contains symlink {relative}")
        if stat.S_ISDIR(metadata.st_mode):
            if metadata.st_mode & 0o222:
                raise PrepareFailure(f"materialized directory is writable: {relative}")
            continue
        if not stat.S_ISREG(metadata.st_mode):
            raise PrepareFailure(f"materialized source has non-file {relative}")
        observed_files[relative] = path
    if set(observed_files) != set(expected_by_path):
        raise PrepareFailure("materialized source file set differs from Git tree")
    entries = []
    for relative in sorted(observed_files):
        path = observed_files[relative]
        metadata = path.lstat()
        expected_entry = expected_by_path[relative]
        wanted_permissions = 0o555 if expected_entry["mode"] == "100755" else 0o444
        if stat.S_IMODE(metadata.st_mode) != wanted_permissions:
            raise PrepareFailure(f"materialized mode differs for {relative}")
        payload = path.read_bytes()
        git_object = hashlib.sha1(
            f"blob {len(payload)}\0".encode() + payload
        ).hexdigest()
        if git_object != expected_entry["object_id"]:
            raise PrepareFailure(f"materialized Git object differs for {relative}")
        entries.append(
            {
                **expected_entry,
                "size": len(payload),
                "sha256": sha256_bytes(payload),
            }
        )
    return entries


def materialize_source(
    variant: str,
    root: Path,
    source: dict[str, Any],
    output: Path,
) -> dict[str, Any]:
    tree_entries = git_tree_entries(root, source["source"])
    tree_manifest = source_manifest(variant, source, None, tree_entries)
    tree_path = output / "sources" / f"{variant}-tree.json"
    atomic_json(tree_path, tree_manifest)
    archive_argv = ["git", "-C", str(root), "archive", "--format=tar", source["source"]]
    archive = git_bytes(root, ["archive", "--format=tar", source["source"]])
    archive_path = output / "sources" / f"{variant}-source.tar"
    atomic_write(archive_path, archive)
    materialized_root = output / "materialized" / f"{variant}-{source['source']}"
    if materialized_root.exists() or materialized_root.is_symlink():
        raise PrepareFailure(f"materialized root is not fresh: {materialized_root}")
    materialized_root.mkdir(parents=True)
    expected_by_path = {entry["path"]: entry for entry in tree_entries}
    seen: set[str] = set()
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as archive_file:
        for member in archive_file.getmembers():
            name = member.name.rstrip("/")
            if not name:
                continue
            parts = Path(name).parts
            if member.name.startswith("/") or ".." in parts:
                raise PrepareFailure(f"unsafe archive path {member.name!r}")
            target = materialized_root.joinpath(*parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            if not member.isfile() or name not in expected_by_path or name in seen:
                raise PrepareFailure(f"unsupported archive entry {member.name!r}")
            extracted = archive_file.extractfile(member)
            if extracted is None:
                raise PrepareFailure(f"cannot extract archive entry {member.name!r}")
            target.parent.mkdir(parents=True, exist_ok=True)
            wanted = 0o555 if expected_by_path[name]["mode"] == "100755" else 0o444
            atomic_write(target, extracted.read(), mode=wanted)
            seen.add(name)
    if seen != set(expected_by_path):
        raise PrepareFailure("archive contents differ from Git tree")
    for directory in sorted(
        (path for path in materialized_root.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    ):
        directory.chmod(0o555)
    materialized_root.chmod(0o555)
    entries = filesystem_entries(materialized_root, tree_entries)
    manifest = source_manifest(variant, source, materialized_root, entries)
    manifest_path = output / "sources" / f"{variant}-materialized.json"
    atomic_json(manifest_path, manifest)
    return {
        "source_root": root,
        "materialized_root": materialized_root,
        "git_archive_argv": archive_argv,
        "source_archive_path": archive_path,
        "source_archive_sha256": sha256(archive_path),
        "tree_manifest_path": tree_path,
        "tree_manifest_sha256": sha256(tree_path),
        "materialized_manifest_path": manifest_path,
        "materialized_manifest_sha256": sha256(manifest_path),
        "materialized_manifest_pre_sha256": sha256_bytes(canonical_json(manifest)),
        "tree_entries": tree_entries,
    }


def replay_materialized_source(
    variant: str, source: dict[str, Any], materialized: dict[str, Any]
) -> str:
    entries = filesystem_entries(
        materialized["materialized_root"], materialized["tree_entries"]
    )
    manifest = source_manifest(
        variant,
        source,
        materialized["materialized_root"],
        entries,
    )
    return sha256_bytes(canonical_json(manifest))


def process_identity(pid: int) -> dict[str, Any]:
    proc = Path("/proc") / str(pid)
    stat = (proc / "stat").read_text()
    opened = stat.find("(")
    closed = stat.rfind(")")
    if opened < 0 or closed <= opened:
        raise OSError(f"malformed {proc / 'stat'}")
    fields = stat[closed + 2 :].split()
    if len(fields) <= 19:
        raise OSError(f"short {proc / 'stat'}")
    return {
        "pid": pid,
        "comm": stat[opened + 1 : closed],
        "state": fields[0],
        "ppid": int(fields[1]),
        "starttime": int(fields[19]),
    }


def process_group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def terminate_group(child: subprocess.Popen[Any]) -> int:
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
        raise PrepareFailure(f"process group {child.pid} remains alive")
    return int(child.returncode if child.returncode is not None else 125)


def run_child(
    argv: list[str],
    *,
    cwd: Path,
    environment: dict[str, str],
    output_path: Path,
    active_publication: Path,
    timeout: int,
) -> dict[str, Any]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    started_at = now()
    started_monotonic_ns = time.monotonic_ns()
    interrupted: BaseException | None = None
    terminated = False
    with output_path.open("wb") as output:
        blocked = {signal.SIGINT, signal.SIGTERM}
        previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, blocked)
        try:
            child = subprocess.Popen(
                argv,
                cwd=cwd,
                env=environment,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except OSError as error:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
            raise PrepareFailure(f"cannot spawn child {argv}: {error}") from error
        try:
            identity = process_identity(child.pid)
        except (OSError, ValueError) as error:
            try:
                terminate_group(child)
            except BaseException as cleanup_error:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
                raise PrepareFailure(
                    f"cannot identify child {argv}: {error}; cleanup failed: "
                    f"{cleanup_error}"
                ) from error
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
            raise PrepareFailure(f"cannot identify child {argv}: {error}") from error
        try:
            atomic_json(
                active_publication,
                {
                    "protocol": PROTOCOL,
                    "child": identity,
                    "argv": argv,
                    "cwd": str(cwd.resolve()),
                    "published_at": now(),
                    "published_monotonic_ns": time.monotonic_ns(),
                },
            )
        except BaseException:
            try:
                terminate_group(child)
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
            raise
        timed_out = False
        try:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
            exit_status = child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            terminated = True
            terminate_group(child)
            exit_status = 124
        except BaseException as error:
            interrupted = error
            terminated = True
            exit_status = terminate_group(child)
        finally:
            output.flush()
            os.fsync(output.fileno())
    try:
        observed = process_identity(identity["pid"])
    except FileNotFoundError:
        reaping = {
            "status": "absent",
            "pid": identity["pid"],
            "starttime": identity["starttime"],
        }
    else:
        if observed["starttime"] == identity["starttime"]:
            raise PrepareFailure(f"child {identity['pid']} was not reaped")
        reaping = {
            "status": "pid_reused",
            "pid": identity["pid"],
            "starttime": identity["starttime"],
            "observed_starttime": observed["starttime"],
        }
    record = {
        "argv": argv,
        "cwd": str(cwd.resolve()),
        "pid": identity["pid"],
        "starttime": identity["starttime"],
        "waited_pid": child.pid,
        "started_at": started_at,
        "completed_at": now(),
        "started_monotonic_ns": started_monotonic_ns,
        "completed_monotonic_ns": time.monotonic_ns(),
        "exit_status": exit_status,
        "timed_out": timed_out,
        "terminated_by_runner": terminated,
        "reaping": reaping,
        "process_group_absent": not process_group_exists(child.pid),
        "output_path": str(output_path.resolve()),
        "output_sha256": sha256(output_path),
    }
    if interrupted is not None:
        raise interrupted
    if exit_status != 0:
        raise PrepareFailure(f"child exited {exit_status}: {argv}")
    if not record["process_group_absent"]:
        terminate_group(child)
        raise PrepareFailure(f"child left process group {child.pid} alive")
    return record


def validate_approval(
    path: Path,
    roots: dict[str, Path],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    approval = load_canonical_json(path, APPROVAL_SCHEMA)
    require_exact_keys(
        approval,
        {
            "schema",
            "protocol",
            "status",
            "review_id",
            "reviewed_at",
            "baseline",
            "common",
            "variants",
        },
        "source approval",
    )
    if approval.get("status") != "approved":
        raise PrepareFailure("source approval status is not approved")
    if not isinstance(approval.get("review_id"), str) or not approval.get(
        "review_id"
    ):
        raise PrepareFailure("source approval lacks review identity/time")
    reviewed_at = approval.get("reviewed_at")
    try:
        reviewed_timestamp = datetime.fromisoformat(reviewed_at)
    except (TypeError, ValueError) as error:
        raise PrepareFailure("source approval review time is invalid") from error
    if reviewed_timestamp.tzinfo is None:
        raise PrepareFailure("source approval review time lacks timezone")
    baseline = approval.get("baseline")
    common = approval.get("common")
    variants = approval.get("variants")
    if not all(isinstance(value, dict) for value in (baseline, common, variants)):
        raise PrepareFailure("source approval sections are malformed")
    require_exact_keys(baseline, {"source", "tree"}, "approval baseline")
    require_exact_keys(
        common, {"harness_sha256", "cargo_lock_sha256"}, "approval common"
    )
    require_exact_keys(variants, set(VARIANTS), "approval variants")
    baseline_source = str(baseline.get("source", ""))
    baseline_tree = str(baseline.get("tree", ""))
    if not GIT_OBJECT.fullmatch(baseline_source) or not GIT_OBJECT.fullmatch(
        baseline_tree
    ):
        raise PrepareFailure("approval baseline source/tree are not full object IDs")
    for field in ("harness_sha256", "cargo_lock_sha256"):
        if not SHA256.fullmatch(str(common.get(field, ""))):
            raise PrepareFailure(f"approval common {field} is not SHA-256")
    observed: dict[str, dict[str, Any]] = {}
    for variant in VARIANTS:
        root = roots[variant]
        claim = variants.get(variant)
        if not isinstance(claim, dict):
            raise PrepareFailure(f"approval lacks {variant}")
        require_exact_keys(
            claim,
            {
                "source",
                "tree",
                "diff_manifest_sha256",
                "patch_sha256",
                "allowed_changes",
            },
            f"approval {variant}",
        )
        source = str(claim.get("source", ""))
        tree = str(claim.get("tree", ""))
        if not GIT_OBJECT.fullmatch(source) or not GIT_OBJECT.fullmatch(tree):
            raise PrepareFailure(f"{variant} source/tree are not full object IDs")
        for field in ("diff_manifest_sha256", "patch_sha256"):
            if not SHA256.fullmatch(str(claim.get(field, ""))):
                raise PrepareFailure(f"{variant} {field} is not SHA-256")
        if git_text(root, ["rev-parse", "HEAD"]) != source:
            raise PrepareFailure(f"{variant} HEAD does not match approval")
        if git_text(root, ["status", "--porcelain"]):
            raise PrepareFailure(f"{variant} worktree is dirty")
        if git_text(root, ["rev-parse", "HEAD^{tree}"]) != tree:
            raise PrepareFailure(f"{variant} tree does not match approval")
        if git_text(root, ["rev-parse", f"{baseline_source}^{{tree}}"]) != baseline_tree:
            raise PrepareFailure(f"{variant} baseline tree mismatch")
        changes = canonical_changes(root, baseline_source, source)
        allowed = claim.get("allowed_changes")
        if allowed != changes:
            raise PrepareFailure(f"{variant} live changes differ from approval")
        diff_bytes = canonical_json(changes)
        patch_bytes = canonical_patch(root, baseline_source, source, changes)
        if sha256_bytes(diff_bytes) != claim.get("diff_manifest_sha256"):
            raise PrepareFailure(f"{variant} diff-manifest hash mismatch")
        if sha256_bytes(patch_bytes) != claim.get("patch_sha256"):
            raise PrepareFailure(f"{variant} patch hash mismatch")
        harness = root / HARNESS_RELATIVE
        cargo_lock = root / "Cargo.lock"
        if sha256(harness) != common.get("harness_sha256"):
            raise PrepareFailure(f"{variant} harness differs from approval")
        if sha256(cargo_lock) != common.get("cargo_lock_sha256"):
            raise PrepareFailure(f"{variant} Cargo.lock differs from approval")
        observed[variant] = {
            "source": source,
            "tree": tree,
            "changes": changes,
            "diff_bytes": diff_bytes,
            "patch_bytes": patch_bytes,
            "harness": harness,
            "cargo_lock": cargo_lock,
        }
    merge_base = git_text(
        roots["control"],
        [
            "merge-base",
            str(variants["control"]["source"]),
            str(variants["candidate"]["source"]),
        ],
    )
    if merge_base != baseline_source:
        raise PrepareFailure(f"source merge-base {merge_base} != {baseline_source}")
    if observed["control"]["source"] == observed["candidate"]["source"]:
        raise PrepareFailure("control and candidate source commits are identical")
    return approval, observed


def build_variant(
    variant: str,
    root: Path,
    approval: dict[str, Any],
    approval_sha: str,
    source: dict[str, Any],
    materialized: dict[str, Any],
    output: Path,
    prepare_wrapper: Path,
    prepare_orchestrator: Path,
) -> tuple[Path, dict[str, Any]]:
    nonce = secrets.token_hex(32)
    target_dir = output / "targets" / f"{variant}-{nonce}"
    if target_dir.exists() or target_dir.is_symlink():
        raise PrepareFailure(f"target directory is not fresh: {target_dir}")
    target_dir.mkdir(parents=True)
    environment = os.environ.copy()
    environment.pop("CARGO_TARGET_DIR", None)
    compile_identity = {
        "protocol": PROTOCOL,
        "baseline_source": approval["baseline"]["source"],
        "baseline_tree": approval["baseline"]["tree"],
        "source_commit": source["source"],
        "source_tree": source["tree"],
        "harness_sha256": approval["common"]["harness_sha256"],
        "cargo_lock_sha256": approval["common"]["cargo_lock_sha256"],
        "source_approval_sha256": approval_sha,
        "build_nonce": nonce,
    }
    for key, value in compile_identity.items():
        environment[f"OWNED_APPEND_BUILD_{key.upper()}"] = str(value)
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
    sandbox_value = shutil.which("bwrap")
    if sandbox_value is None:
        raise PrepareFailure("bubblewrap is required for read-only source builds")
    sandbox = Path(sandbox_value).resolve(strict=True)
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
        str(materialized["materialized_root"]),
    ]
    build_argv = [*sandbox_prefix, *cargo_argv]
    build_log = output / "logs" / f"{variant}-build.log"
    build_started_at = now()
    build_child = run_child(
        build_argv,
        cwd=materialized["materialized_root"],
        environment=environment,
        output_path=build_log,
        active_publication=output / "active-child.json",
        timeout=3600,
    )
    build_completed_at = now()
    binary = target_dir / "release/examples/owned_append_bench"
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise PrepareFailure(f"build did not produce {binary}")
    contract_log = output / "logs" / f"{variant}-contract.log"
    contract_environment = os.environ.copy()
    contract_environment["OWNED_APPEND_CONTRACT_MODE"] = PROTOCOL
    contract_argv = [*sandbox_prefix, str(binary)]
    contract_child = run_child(
        contract_argv,
        cwd=materialized["materialized_root"],
        environment=contract_environment,
        output_path=contract_log,
        active_publication=output / "active-child.json",
        timeout=30,
    )
    try:
        contract = json.loads(contract_log.read_text())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PrepareFailure(f"invalid {variant} contract output: {error}") from error
    expected_contract = {
        **compile_identity,
        "contract_mode": True,
        "csv_written": False,
    }
    if contract != expected_contract:
        raise PrepareFailure(
            f"{variant} embedded contract {contract!r} != {expected_contract!r}"
        )
    post_manifest_sha256 = replay_materialized_source(
        variant, source, materialized
    )
    if post_manifest_sha256 != materialized["materialized_manifest_sha256"]:
        raise PrepareFailure(f"{variant} source materialization changed during build")
    diff_path = output / "sources" / f"{variant}-diff.json"
    patch_path = output / "sources" / f"{variant}.patch"
    atomic_write(diff_path, source["diff_bytes"])
    atomic_write(patch_path, source["patch_bytes"])
    attestation = {
        "schema": ATTESTATION_SCHEMA,
        "protocol": PROTOCOL,
        "variant": variant,
        "baseline_source": approval["baseline"]["source"],
        "baseline_tree": approval["baseline"]["tree"],
        "source_commit": source["source"],
        "source_tree": source["tree"],
        "source_root": str(root.resolve()),
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
        "materialized_manifest_sha256": materialized[
            "materialized_manifest_sha256"
        ],
        "materialized_manifest_pre_sha256": materialized[
            "materialized_manifest_pre_sha256"
        ],
        "materialized_manifest_post_sha256": post_manifest_sha256,
        "source_read_only": True,
        "gitlinks_present": False,
        "source_approval_sha256": approval_sha,
        "build_nonce": nonce,
        "target_dir": str(target_dir.resolve()),
        "target_dir_was_absent": True,
        "binary_path": str(binary.resolve()),
        "binary_sha256": sha256(binary),
        "harness_path": str(
            (materialized["materialized_root"] / HARNESS_RELATIVE).resolve()
        ),
        "harness_sha256": sha256(
            materialized["materialized_root"] / HARNESS_RELATIVE
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
        "build_started_at": build_started_at,
        "build_completed_at": build_completed_at,
        "build_child": build_child,
        "rustc": subprocess.run(
            ["rustc", "-Vv"], check=True, capture_output=True, text=True
        ).stdout.strip().replace("\n", "\\n"),
        "cargo": subprocess.run(
            ["cargo", "-V"], check=True, capture_output=True, text=True
        ).stdout.strip(),
        "contract_output_path": str(contract_log.resolve()),
        "contract_output_sha256": sha256(contract_log),
        "contract_child": contract_child,
        "contract": contract,
        "prepare_runner_path": str(prepare_wrapper.resolve()),
        "prepare_runner_sha256": sha256(prepare_wrapper),
        "prepare_orchestrator_path": str(prepare_orchestrator.resolve()),
        "prepare_orchestrator_sha256": sha256(prepare_orchestrator),
    }
    attestation_path = output / "attestations" / f"{variant}.json"
    atomic_json(attestation_path, attestation)
    return attestation_path, attestation


def self_test(root: Path) -> int:
    if root.exists() or root.is_symlink():
        print(f"refusing non-fresh self-test directory {root}", file=sys.stderr)
        return 2
    root.mkdir(parents=True)
    repository = root / "repository"
    tooling = tooling_identity(require_clean=False)
    subprocess.run(
        ["git", "clone", "-q", str(tooling["root"]), str(repository)],
        check=True,
        capture_output=True,
        timeout=60,
    )

    def git(arguments: list[str], cwd: Path = repository) -> str:
        result = subprocess.run(
            ["git", *arguments],
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    git(["config", "user.email", "bn-22it@example.invalid"])
    git(["config", "user.name", "bn-22it self-test"])
    git(["checkout", "-q", tooling["source"]])
    baseline_source = tooling["source"]
    baseline_tree = tooling["tree"]
    git(["checkout", "-q", "-b", "control"])
    harness = repository / HARNESS_RELATIVE
    harness.parent.mkdir(parents=True, exist_ok=True)
    atomic_write(harness, b"fn main() {}\n")
    atomic_write(repository / "Cargo.lock", b"# fixture\n")
    atomic_write(repository / "control.txt", b"control\n")
    git(["add", "."])
    git(["add", "-f", "Cargo.lock"])
    git(["commit", "-q", "-m", "control"])
    control_source = git(["rev-parse", "HEAD"])
    git(["checkout", "-q", "-b", "candidate", baseline_source])
    harness.parent.mkdir(parents=True, exist_ok=True)
    atomic_write(harness, b"fn main() {}\n")
    atomic_write(repository / "Cargo.lock", b"# fixture\n")
    atomic_write(repository / "candidate.txt", b"candidate\n")
    git(["add", "."])
    git(["add", "-f", "Cargo.lock"])
    git(["commit", "-q", "-m", "candidate"])
    candidate_source = git(["rev-parse", "HEAD"])

    roots = {}
    variants = {}
    for name, source in (
        ("control", control_source),
        ("candidate", candidate_source),
    ):
        clone = root / f"{name}-root"
        git(["clone", "-q", str(repository), str(clone)], cwd=root)
        git(["checkout", "-q", source], cwd=clone)
        roots[name] = clone
        changes = canonical_changes(clone, baseline_source, source)
        variants[name] = {
            "source": source,
            "tree": git_text(clone, ["rev-parse", "HEAD^{tree}"]),
            "diff_manifest_sha256": sha256_bytes(canonical_json(changes)),
            "patch_sha256": sha256_bytes(
                canonical_patch(clone, baseline_source, source, changes)
            ),
            "allowed_changes": changes,
        }
    approval = {
        "schema": APPROVAL_SCHEMA,
        "protocol": PROTOCOL,
        "status": "approved",
        "review_id": "fixture-review",
        "reviewed_at": now(),
        "baseline": {"source": baseline_source, "tree": baseline_tree},
        "common": {
            "harness_sha256": sha256(roots["control"] / HARNESS_RELATIVE),
            "cargo_lock_sha256": sha256(roots["control"] / "Cargo.lock"),
        },
        "variants": variants,
    }
    approval_path = root / "source-approval.json"
    atomic_json(approval_path, approval)
    observed, sources = validate_approval(approval_path, roots)
    checks = {
        "canonical_approval": observed == approval,
        "merge_base_and_trees": all(
            sources[name]["source"] == variants[name]["source"]
            and sources[name]["tree"] == variants[name]["tree"]
            for name in VARIANTS
        ),
    }
    materialization_output = root / "materialization-output"
    materialization_output.mkdir()
    materialized = materialize_source(
        "control", roots["control"], sources["control"], materialization_output
    )
    replay_digest = replay_materialized_source(
        "control", sources["control"], materialized
    )
    checks["exact_read_only_materialization"] = (
        replay_digest == materialized["materialized_manifest_sha256"]
        and replay_digest == materialized["materialized_manifest_pre_sha256"]
    )
    mutation_target = materialized["materialized_root"] / "control.txt"
    mutation_target.chmod(0o644)
    mutation_target.write_bytes(b"transient mutation\n")
    mutation_target.chmod(0o444)
    try:
        replay_materialized_source("control", sources["control"], materialized)
    except PrepareFailure:
        checks["materialized_mutation_rejected"] = True
    else:
        checks["materialized_mutation_rejected"] = False
    mutated = json.loads(approval_path.read_text())
    mutated["variants"]["candidate"]["patch_sha256"] = "0" * 64
    mutated_path = root / "mutated-approval.json"
    atomic_json(mutated_path, mutated)
    try:
        validate_approval(mutated_path, roots)
    except PrepareFailure:
        checks["patch_mutation_rejected"] = True
    else:
        checks["patch_mutation_rejected"] = False

    noncanonical_path = root / "noncanonical-approval.json"
    atomic_write(noncanonical_path, b" " + canonical_json(approval))
    try:
        validate_approval(noncanonical_path, roots)
    except PrepareFailure:
        checks["noncanonical_approval_rejected"] = True
    else:
        checks["noncanonical_approval_rejected"] = False

    allowlist_mutation = json.loads(json.dumps(approval))
    allowlist_mutation["variants"]["candidate"]["allowed_changes"] = []
    allowlist_path = root / "allowlist-mutation.json"
    atomic_json(allowlist_path, allowlist_mutation)
    try:
        validate_approval(allowlist_path, roots)
    except PrepareFailure:
        checks["allowlist_mutation_rejected"] = True
    else:
        checks["allowlist_mutation_rejected"] = False

    common_mutation = json.loads(json.dumps(approval))
    common_mutation["common"]["harness_sha256"] = "0" * 64
    common_path = root / "common-mutation.json"
    atomic_json(common_path, common_mutation)
    try:
        validate_approval(common_path, roots)
    except PrepareFailure:
        checks["common_input_mutation_rejected"] = True
    else:
        checks["common_input_mutation_rejected"] = False

    dirty_marker = roots["control"] / "untracked-self-test-marker"
    atomic_write(dirty_marker, b"dirty\n")
    try:
        validate_approval(approval_path, roots)
    except PrepareFailure:
        checks["dirty_source_rejected"] = True
    else:
        checks["dirty_source_rejected"] = False
    dirty_marker.unlink()

    malformed = json.loads(json.dumps(approval))
    malformed["unexpected"] = True
    malformed_path = root / "malformed-approval.json"
    atomic_json(malformed_path, malformed)
    try:
        validate_approval(malformed_path, roots)
    except PrepareFailure:
        checks["unexpected_schema_key_rejected"] = True
    else:
        checks["unexpected_schema_key_rejected"] = False
    report = {
        "schema": "bn-22it-prepare-self-test-v1",
        "protocol": PROTOCOL,
        "outcome": "SELF_TEST_PASS" if all(checks.values()) else "SELF_TEST_FAILED",
        "checks": checks,
        "baseline_source": baseline_source,
        "baseline_tree": baseline_tree,
        "control_source": control_source,
        "candidate_source": candidate_source,
        "check_count": len(checks),
        "prepare_runner_sha256": sha256(
            Path(__file__).with_name("prepare_paired.sh")
        ),
        "prepare_orchestrator_sha256": sha256(Path(__file__).resolve()),
        "completed_at": now(),
    }
    atomic_json(root / "self-test.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if all(checks.values()) else 30


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("source_approval", type=Path)
    parser.add_argument("control_root", type=Path)
    parser.add_argument("candidate_root", type=Path)
    parser.add_argument("output_dir", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    if raw[:1] == ["--self-test"]:
        if len(raw) != 2:
            return 2
        return self_test(Path(raw[1]).resolve())
    args = parse_args(raw)
    approval_path = args.source_approval.resolve()
    roots = {
        "control": args.control_root.resolve(),
        "candidate": args.candidate_root.resolve(),
    }
    output = args.output_dir.expanduser().resolve()
    if output.exists() or output.is_symlink():
        print(f"refusing non-fresh output directory {output}", file=sys.stderr)
        return 2
    if os.environ.get("MESS_BENCH_COORDINATION_CONFIRMED") != "true":
        print("MESS_BENCH_COORDINATION_CONFIRMED must be true", file=sys.stderr)
        return 20

    def interrupted(signum: int, _frame: Any) -> None:
        raise PrepareFailure(f"prepare received signal {signum}")

    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    phase = "preflight"
    failure_published = False
    lease_acquired = False
    lease_released = False
    lease_event: dict[str, Any] | None = None
    lease_handle: Any | None = None
    try:
        for root in roots.values():
            if output == root or root in output.parents:
                raise PrepareFailure(
                    "prepare output must be outside source workspaces"
                )
        output.parent.mkdir(parents=True, exist_ok=True)
        output.mkdir()
        lock_path = LOCK_PATH.resolve()
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lease_handle = lock_path.open("a+b", buffering=0)
        try:
            fcntl.flock(
                lease_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB
            )
        except BlockingIOError as error:
            lease_handle.close()
            raise PrepareFailure(f"measurement lease is held: {lock_path}") from error
        lease_acquired = True
        lock_stat = os.fstat(lease_handle.fileno())
        lease_event = {
            "protocol": PROTOCOL,
            "event": "prepare_acquired",
            "path": str(lock_path),
            "device": lock_stat.st_dev,
            "inode": lock_stat.st_ino,
            "holder": process_identity(os.getpid()),
            "uid": os.getuid(),
            "hostname": socket.gethostname(),
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "nonce": secrets.token_hex(32),
            "acquired_at": now(),
            "acquired_monotonic_ns": time.monotonic_ns(),
        }
        lease_path = output / "lease_event.json"
        atomic_json(lease_path, lease_event)
        prepare_wrapper = Path(__file__).with_name("prepare_paired.sh").resolve()
        prepare_orchestrator = Path(__file__).resolve()
        attestations: dict[str, dict[str, str]] = {}
        try:
            phase = "validate_reviewed_sources"
            approval, sources = validate_approval(approval_path, roots)
            tooling = tooling_identity(require_clean=True)
            if approval["baseline"] != {
                "source": tooling["source"],
                "tree": tooling["tree"],
            }:
                raise PrepareFailure(
                    "approval baseline is not the immutable tooling checkout"
                )
            approval_sha = sha256(approval_path)
            for variant in VARIANTS:
                phase = f"materialize_{variant}"
                materialized = materialize_source(
                    variant, roots[variant], sources[variant], output
                )
                phase = f"build_{variant}"
                path, _ = build_variant(
                    variant,
                    roots[variant],
                    approval,
                    approval_sha,
                    sources[variant],
                    materialized,
                    output,
                    prepare_wrapper,
                    prepare_orchestrator,
                )
                attestations[variant] = {
                    "path": str(path.resolve()),
                    "sha256": sha256(path),
                }
                validate_approval(approval_path, roots)
            phase = "publish_pre_release"
            failure_path = output / "failure.json"
            if failure_path.exists() or failure_path.is_symlink():
                raise PrepareFailure("failure artifact exists before pre-release")
            pre_release = {
                "schema": PRE_RELEASE_SCHEMA,
                "protocol": PROTOCOL,
                "created_at": now(),
                "created_monotonic_ns": time.monotonic_ns(),
                "tooling_source": tooling["source"],
                "tooling_tree": tooling["tree"],
                "source_approval_path": str(approval_path),
                "source_approval_sha256": approval_sha,
                "prepare_runner_path": str(prepare_wrapper),
                "prepare_runner_sha256": sha256(prepare_wrapper),
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
        except BaseException as error:
            failure_path = output / "failure.json"
            atomic_json(
                failure_path,
                {
                    "protocol": PROTOCOL,
                    "outcome": "INCONCLUSIVE_PREPARATION",
                    "failed_at": now(),
                    "phase": phase,
                    "reason": f"{error.__class__.__name__}: {error}",
                    "lease_held_during_publication": True,
                    "lease_event_path": str(lease_path.resolve()),
                    "lease_event_sha256": sha256(lease_path),
                },
            )
            failure_published = True
            raise
        finally:
            fcntl.flock(lease_handle.fileno(), fcntl.LOCK_UN)
            lease_handle.close()
            lease_released = True
        phase = "publish_release"
        release = {
            "schema": RELEASE_SCHEMA,
            "protocol": PROTOCOL,
            "event": "prepare_released",
            "released_at": now(),
            "released_monotonic_ns": time.monotonic_ns(),
            "lease_nonce": lease_event["nonce"],
            "pre_release_path": str(pre_release_path.resolve()),
            "pre_release_sha256": sha256(pre_release_path),
        }
        release_path = output / "lease_release.json"
        atomic_json(release_path, release)
        phase = "publish_terminal"
        terminal = {
            "schema": TERMINAL_SCHEMA,
            "protocol": PROTOCOL,
            "outcome": "PREPARED",
            "completed_at": now(),
            "completed_monotonic_ns": time.monotonic_ns(),
            "pre_release_path": str(pre_release_path.resolve()),
            "pre_release_sha256": sha256(pre_release_path),
            "release_path": str(release_path.resolve()),
            "release_sha256": sha256(release_path),
            "failure_path": str((output / "failure.json").resolve()),
            "failure_absent": True,
        }
        terminal_path = output / "prepare_terminal.json"
        atomic_json(terminal_path, terminal)
        phase = "publish_prepared_pair"
        pair = {
            "schema": PAIR_SCHEMA,
            "protocol": PROTOCOL,
            "created_at": now(),
            "created_monotonic_ns": time.monotonic_ns(),
            "tooling_source": tooling["source"],
            "tooling_tree": tooling["tree"],
            "source_approval_path": str(approval_path),
            "source_approval_sha256": approval_sha,
            "prepare_runner_path": str(prepare_wrapper),
            "prepare_runner_sha256": sha256(prepare_wrapper),
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
            "failure_path": str((output / "failure.json").resolve()),
            "failure_absent": True,
            "attestations": attestations,
        }
        pair_path = output / "prepared-pair.json"
        atomic_json(pair_path, pair)
        print(pair_path)
        return 0
    except BaseException as error:
        if output.is_dir() and not failure_published:
            atomic_json(
                output / "failure.json",
                {
                    "protocol": PROTOCOL,
                    "outcome": "INCONCLUSIVE_PREPARATION",
                    "failed_at": now(),
                    "phase": phase,
                    "reason": f"{error.__class__.__name__}: {error}",
                    "lease_held_during_publication": (
                        lease_acquired and not lease_released
                    ),
                },
            )
            failure_published = True
        if lease_acquired and not lease_released and lease_handle is not None:
            try:
                fcntl.flock(lease_handle.fileno(), fcntl.LOCK_UN)
            finally:
                lease_handle.close()
                lease_released = True
        if (
            output.is_dir()
            and lease_acquired
            and lease_released
            and lease_event is not None
            and not (output / "lease_release.json").exists()
        ):
            atomic_json(
                output / "lease_release.json",
                {
                    "protocol": PROTOCOL,
                    "event": "prepare_released_after_failure",
                    "released_at": now(),
                    "lease_nonce": lease_event["nonce"],
                    "failure_path": str((output / "failure.json").resolve()),
                    "failure_sha256": sha256(output / "failure.json"),
                },
            )
        print(f"prepare fail-stop: {error}", file=sys.stderr)
        return 20


if __name__ == "__main__":
    raise SystemExit(main())
