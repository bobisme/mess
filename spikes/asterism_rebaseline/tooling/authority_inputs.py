#!/usr/bin/env python3
"""Produce the immutable operational inputs for the v3 authority workflow.

This module only packages already-frozen tools, assertions, and exact Seal
events.  It never resolves dependencies, invokes Cargo/rustc, builds product
code, runs correctness children, or emits measurement evidence.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import stat
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


HERE = Path(__file__).resolve().parent
CURRENT = HERE / "current"
for module_root in (HERE, CURRENT):
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

import prepare_overlays as overlays  # noqa: E402
import build_children as children  # noqa: E402
import lock_authority as locks  # noqa: E402


BASE_TOOLS_RESULT_SCHEMA = "bn-znj5-base-tools-write-v1"
LOCK_ASSERTION_RESULT_SCHEMA = "bn-znj5-lock-assertion-write-v1"
LOCK_BUNDLE_RESULT_SCHEMA = "bn-znj5-lock-review-bundle-write-v1"
SOURCE_ASSERTION_RESULT_SCHEMA = "bn-znj5-source-assertion-write-v1"
SOURCE_BUNDLE_RESULT_SCHEMA = "bn-znj5-source-review-bundle-write-v1"
SELF_TEST_SCHEMA = "bn-znj5-authority-inputs-self-test-v1"
TOOLS_FILENAME = "asterism-rebaseline-tools.json"
SHA256 = 64


class InputError(RuntimeError):
    """An operational authority input is ambiguous, mutable, or stale."""


@dataclass(frozen=True)
class FileSnapshot:
    path: Path
    payload: bytes
    identity: tuple[int, int, int, int, int, int, int]

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.payload).hexdigest()


def canonical_json(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()
        + b"\n"
    )


def exact_repository(path: Path) -> Path:
    repository = path.resolve(strict=True)
    producer_repository = HERE.parents[2].resolve(strict=True)
    if repository != producer_repository:
        raise InputError(
            "authority producer and reviewed repository are different checkouts"
        )
    return repository


def require_external_output(repository: Path, output: Path, context: str) -> None:
    candidate = output.resolve()
    if candidate == repository or repository in candidate.parents:
        raise InputError(f"{context} must be outside the reviewed repository")


def parse_canonical_object(payload: bytes, context: str) -> dict[str, Any]:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InputError(f"{context} is not valid JSON") from error
    if not isinstance(value, dict) or canonical_json(value) != payload:
        raise InputError(f"{context} is not one canonical JSON object")
    return value


def identity(metadata: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def snapshot_regular(
    path: Path,
    context: str,
    *,
    required_mode: int | None = None,
    require_single_link: bool = True,
) -> FileSnapshot:
    lexical = path
    if not lexical.is_absolute():
        raise InputError(f"{context} path is not absolute")
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lexical, flags)
    except OSError as error:
        raise InputError(f"{context} cannot be opened exactly") from error
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or (require_single_link and before.st_nlink != 1)
            or (
                required_mode is not None
                and stat.S_IMODE(before.st_mode) != required_mode
            )
        ):
            raise InputError(f"{context} is not an unaliased exact regular file")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    observed = lexical.lstat()
    if identity(before) != identity(after) or identity(after) != identity(observed):
        raise InputError(f"{context} changed while being read")
    if lexical.resolve(strict=True) != lexical:
        raise InputError(f"{context} path is not canonical")
    return FileSnapshot(lexical, b"".join(chunks), identity(after))


def require_same_snapshot(
    expected: FileSnapshot, observed: FileSnapshot, context: str
) -> None:
    if expected != observed:
        raise InputError(f"{context} changed across the authority boundary")


def write_new(path: Path, payload: bytes, mode: int) -> None:
    if path.exists() or path.is_symlink():
        raise InputError(f"output must be absent: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        children.write_new(path, payload, mode)
    except children.BuildError as error:
        raise InputError(str(error)) from error


def copy_new(source: Path, destination: Path, mode: int) -> None:
    snapshot = snapshot_regular(
        source.resolve(strict=True),
        f"copy source {source}",
        require_single_link=False,
    )
    write_new(destination, snapshot.payload, mode)
    copied = snapshot_regular(
        destination.resolve(strict=True),
        f"copied destination {destination}",
        required_mode=mode,
    )
    if copied.payload != snapshot.payload:
        raise InputError(f"copied destination differs: {destination}")


def runtime_smoke(path: Path, expected_comm: str) -> None:
    program = (
        "import json,os,pathlib;"
        "print(json.dumps({'comm':pathlib.Path('/proc/self/comm').read_text().strip(),"
        "'exe':str(pathlib.Path('/proc/self/exe').resolve())},sort_keys=True,separators=(',',':')))"
    )
    completed = subprocess.run(
        [str(path), "-I", "-B", "-c", program],
        executable=str(path),
        cwd=path.parent,
        env={
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PYTHONDONTWRITEBYTECODE": "1",
            "TZ": "UTC",
        },
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=15,
        check=False,
    )
    try:
        observed = json.loads(completed.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InputError(f"runtime smoke output differs: {expected_comm}") from error
    if (
        completed.returncode != 0
        or completed.stderr
        or observed
        != {"comm": expected_comm, "exe": str(path.resolve(strict=True))}
    ):
        raise InputError(f"runtime copy cannot realize its identity: {expected_comm}")


def base_tools_value(
    tool_paths: dict[str, Path], support_paths: dict[str, Path]
) -> dict[str, Any]:
    if set(tool_paths) != children.REQUIRED_TOOLS - children.CHILD_TOOLS:
        raise InputError("base tool source set differs")
    if set(support_paths) != children.REQUIRED_SUPPORT_FILES:
        raise InputError("base support source set differs")
    tools: dict[str, Any] = copy.deepcopy(children.CHILD_PLACEHOLDER_BINDINGS)
    for name, path in sorted(tool_paths.items()):
        snapshot = snapshot_regular(
            path.resolve(strict=True), f"base tool {name}", required_mode=0o555
        )
        tools[name] = {
            "comm": children.REQUIRED_TOOL_COMMS[name],
            "executable_mode": 0o555,
            "path": str(snapshot.path),
            "sha256": snapshot.sha256,
        }
    support: dict[str, Any] = {}
    for name, path in sorted(support_paths.items()):
        snapshot = snapshot_regular(
            path.resolve(strict=True), f"base support {name}", required_mode=0o444
        )
        if snapshot.path.name != children.REQUIRED_SUPPORT_BASENAMES[name]:
            raise InputError(f"base support basename differs: {name}")
        support[name] = {
            "mode": 0o444,
            "path": str(snapshot.path),
            "sha256": snapshot.sha256,
        }
    return {
        "comm_allowlist": children.COMM_ALLOWLIST,
        "schema": children.TOOLS_SCHEMA,
        "support_files": support,
        "tools": tools,
    }


def write_base_tools(
    repository: Path,
    output: Path,
    *,
    perf_source: Path,
    strace_source: Path,
) -> Path:
    repository = exact_repository(repository)
    require_external_output(repository, output, "base-tools output")
    if output.exists() or output.is_symlink():
        raise InputError(f"base-tools output must be absent: {output}")
    toolchain = overlays.toolchain_identity()
    tooling_commit, _tooling_tree = overlays.tooling_identity(repository, toolchain)
    output.mkdir(parents=False, exist_ok=False)
    runtime_source = children.SYSTEM_PYTHON
    tool_paths: dict[str, Path] = {}
    support_paths: dict[str, Path] = {}
    try:
        tools_root = output / "tools"
        support_root = output / "support"
        tools_root.mkdir()
        support_root.mkdir()
        for name in sorted(children.REQUIRED_TOOLS - children.CHILD_TOOLS):
            destination = tools_root / children.REQUIRED_TOOL_COMMS[name]
            source = (
                perf_source
                if name == "perf"
                else strace_source
                if name == "strace"
                else runtime_source
            )
            copy_new(source.resolve(strict=True), destination, 0o555)
            tool_paths[name] = destination.resolve(strict=True)
            if name not in {"perf", "strace"}:
                runtime_smoke(tool_paths[name], children.REQUIRED_TOOL_COMMS[name])
        support_source_root = repository / "spikes" / "asterism_rebaseline"
        for name in sorted(children.REQUIRED_SUPPORT_FILES):
            source = support_source_root / children.REQUIRED_SUPPORT_BASENAMES[name]
            relative = source.relative_to(repository).as_posix()
            committed = overlays.git_bytes(
                repository, ["show", f"{tooling_commit}:{relative}"], toolchain
            )
            observed = snapshot_regular(
                source, f"committed support source {name}"
            )
            if observed.payload != committed:
                raise InputError(f"support source differs from tooling commit: {name}")
            destination = support_root / children.REQUIRED_SUPPORT_BASENAMES[name]
            write_new(destination, observed.payload, 0o444)
            copied = snapshot_regular(
                destination.resolve(strict=True),
                f"copied committed support {name}",
                required_mode=0o444,
            )
            if copied.payload != committed:
                raise InputError(f"copied support differs from tooling commit: {name}")
            support_paths[name] = destination.resolve(strict=True)
        value = base_tools_value(tool_paths, support_paths)
        manifest = output / TOOLS_FILENAME
        write_new(manifest, canonical_json(value), 0o444)
        if children.validate_tools_manifest(
            manifest.resolve(strict=True), allow_child_placeholders=True
        ) != value:
            raise InputError("base tools failed exact downstream validation")
        children.make_read_only(output)
        if children.validate_tools_manifest(
            manifest.resolve(strict=True), allow_child_placeholders=True
        ) != value:
            raise InputError("frozen base tools failed exact downstream validation")
        overlays.tooling_identity(repository, toolchain)
        if children.validate_tools_manifest(
            manifest.resolve(strict=True), allow_child_placeholders=True
        ) != value:
            raise InputError("terminal base tools validation differs")
        return manifest.resolve(strict=True)
    except BaseException:
        # The partially written, uniquely named root is retained diagnostically.
        raise


def capture_lock_assertion(
    repository: Path, lock_manifest_path: Path
) -> tuple[
    locks.ImmutableSnapshot,
    dict[str, locks.ImmutableSnapshot],
    locks.AuthorityContext,
    dict[str, Any],
]:
    repository = locks.exact_repository(exact_repository(repository))
    validator: Callable[[dict[str, Any]], locks.AuthorityContext] = (
        lambda value: locks.production_context(repository, value)
    )
    manifest, lock_inputs, context = locks.capture_validated_locks(
        lock_manifest_path, validator
    )
    assertion = locks.review_assertion(manifest, lock_inputs, context)
    final_manifest, final_inputs, final_context = locks.capture_validated_locks(
        lock_manifest_path, validator
    )
    locks.require_same_snapshot(manifest, final_manifest, "lock assertion manifest")
    locks.require_same_locks(lock_inputs, final_inputs)
    locks.require_same_context(context, final_context, "lock assertion context")
    if locks.review_assertion(final_manifest, final_inputs, final_context) != assertion:
        raise InputError("lock assertion changed while being captured")
    return final_manifest, final_inputs, final_context, assertion


def write_lock_assertion(
    repository: Path, lock_manifest_path: Path, output: Path
) -> dict[str, Any]:
    repository = exact_repository(repository)
    require_external_output(repository, output, "lock assertion output")
    _manifest, _lock_inputs, _context, assertion = capture_lock_assertion(
        repository, lock_manifest_path
    )
    write_new(output, canonical_json(assertion), 0o444)
    snapshot = snapshot_regular(
        output.resolve(strict=True), "lock review assertion", required_mode=0o444
    )
    if parse_canonical_object(snapshot.payload, "lock review assertion") != assertion:
        raise InputError("published lock assertion differs")
    _final_manifest, _final_inputs, _final_context, final_assertion = (
        capture_lock_assertion(repository, lock_manifest_path)
    )
    if final_assertion != assertion:
        raise InputError("lock assertion changed after publication")
    require_same_snapshot(
        snapshot,
        snapshot_regular(
            snapshot.path,
            "terminal lock review assertion",
            required_mode=0o444,
        ),
        "lock review assertion",
    )
    return assertion


def parse_seal_events(snapshot: FileSnapshot) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for ordinal, line in enumerate(snapshot.payload.splitlines(), 1):
        if not line:
            raise InputError(f"Seal event log contains an empty line: {ordinal}")
        try:
            value = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise InputError(f"Seal event log line is invalid: {ordinal}") from error
        if not isinstance(value, dict):
            raise InputError(f"Seal event log line is not an object: {ordinal}")
        events.append(value)
    if not events:
        raise InputError("Seal event log is empty")
    return events


def select_seal_events(
    events: list[dict[str, Any]], assertion_sha256: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    event_names = tuple(event.get("event") for event in events)
    if event_names not in {
        ("ReviewCreated", "ReviewerVoted", "ReviewApproved"),
        (
            "ReviewCreated",
            "ReviewersRequested",
            "ReviewerVoted",
            "ReviewApproved",
        ),
    }:
        raise InputError("Seal review lifecycle is not one terminal approval")
    created = locks.exact_seal_event(events[0], "ReviewCreated")
    review_id = created["data"].get("review_id")
    vote_index = 1
    if event_names[1] == "ReviewersRequested":
        requested = locks.exact_seal_event(events[1], "ReviewersRequested")
        reviewers = requested["data"].get("reviewers")
        if (
            set(requested["data"]) != {"review_id", "reviewers"}
            or requested["data"].get("review_id") != review_id
            or not isinstance(reviewers, list)
            or not reviewers
            or any(not isinstance(reviewer, str) or not reviewer for reviewer in reviewers)
            or len(reviewers) != len(set(reviewers))
        ):
            raise InputError("Seal reviewer request differs")
        vote_index = 2
    vote = locks.exact_seal_event(events[vote_index], "ReviewerVoted")
    approved = locks.exact_seal_event(events[vote_index + 1], "ReviewApproved")
    reason = f"APPROVED assertion_sha256={assertion_sha256}; open_findings=0"
    if vote["data"] != {
        "reason": reason,
        "review_id": review_id,
        "vote": "lgtm",
    }:
        raise InputError("Seal log does not contain one exact approval verdict")
    if set(approved["data"]) != {"review_id"} or approved["data"].get(
        "review_id"
    ) != review_id:
        raise InputError("Seal ReviewApproved differs")
    timestamps = [
        locks.parse_zoned_time(event["ts"], f"Seal {event['event']} timestamp")
        for event in events
    ]
    if timestamps != sorted(timestamps):
        raise InputError("Seal review events are not chronologically ordered")
    return created, vote


def assertion_snapshot(path: Path, context: str) -> tuple[FileSnapshot, dict[str, Any]]:
    snapshot = snapshot_regular(path.absolute(), context, required_mode=0o444)
    return snapshot, parse_canonical_object(snapshot.payload, context)


def seal_log_snapshot(path: Path) -> tuple[FileSnapshot, list[dict[str, Any]]]:
    snapshot = snapshot_regular(path.absolute(), "Seal review event log")
    return snapshot, parse_seal_events(snapshot)


def write_lock_bundle(
    repository: Path,
    lock_manifest_path: Path,
    assertion_path: Path,
    seal_events_path: Path,
    output: Path,
) -> dict[str, Any]:
    repository = exact_repository(repository)
    require_external_output(repository, output, "lock review bundle output")
    manifest, lock_inputs, context, assertion = capture_lock_assertion(
        repository, lock_manifest_path
    )
    reviewed, reviewed_value = assertion_snapshot(
        assertion_path, "reviewed lock assertion"
    )
    if reviewed_value != assertion:
        raise InputError("reviewed lock assertion is stale")
    seal_log, events = seal_log_snapshot(seal_events_path)
    if seal_log.identity == reviewed.identity:
        raise InputError("Seal log aliases the reviewed assertion")
    assertion_sha256 = hashlib.sha256(canonical_json(assertion)).hexdigest()
    created, verdict = select_seal_events(events, assertion_sha256)
    bundle_value = {
        "assertion": assertion,
        "assertion_sha256": assertion_sha256,
        "review_created": created,
        "schema": locks.REVIEW_BUNDLE_SCHEMA,
        "verdict": verdict,
    }
    write_new(output, canonical_json(bundle_value), 0o444)
    bundle = locks.snapshot_file(
        output.resolve(strict=True),
        "published lock review bundle",
        schema=locks.REVIEW_BUNDLE_SCHEMA,
    )
    locks.validate_review_bundle(bundle, manifest, lock_inputs, context)
    require_same_snapshot(
        reviewed,
        snapshot_regular(
            reviewed.path, "resampled reviewed lock assertion", required_mode=0o444
        ),
        "reviewed lock assertion",
    )
    require_same_snapshot(
        seal_log,
        snapshot_regular(seal_log.path, "resampled Seal review event log"),
        "Seal review event log",
    )
    final_manifest, final_inputs, final_context, final_assertion = (
        capture_lock_assertion(repository, lock_manifest_path)
    )
    locks.require_same_snapshot(manifest, final_manifest, "lock bundle manifest")
    locks.require_same_locks(lock_inputs, final_inputs)
    locks.require_same_context(context, final_context, "lock bundle context")
    if final_assertion != assertion:
        raise InputError("lock bundle assertion changed after publication")
    require_same_snapshot(
        reviewed,
        snapshot_regular(
            reviewed.path,
            "terminal reviewed lock assertion",
            required_mode=0o444,
        ),
        "reviewed lock assertion",
    )
    require_same_snapshot(
        seal_log,
        snapshot_regular(seal_log.path, "terminal lock Seal event log"),
        "lock Seal event log",
    )
    final_bundle = locks.snapshot_file(
        bundle.path,
        "terminal lock review bundle",
        schema=locks.REVIEW_BUNDLE_SCHEMA,
    )
    locks.require_same_snapshot(bundle, final_bundle, "lock review bundle")
    return bundle_value


def capture_source_assertion(
    repository: Path,
    *,
    current_children_path: Path,
    lock_manifest_path: Path,
    tools_path: Path,
    lock_authority_path: Path,
    lock_review_bundle_path: Path,
) -> tuple[dict[str, overlays.CanonicalSnapshot], dict[str, Any], dict[str, Any]]:
    toolchain = overlays.toolchain_identity()
    inputs, requirement, assertion = overlays.capture_source_review_assertion(
        exact_repository(repository),
        current_children_path=current_children_path,
        lock_manifest_path=lock_manifest_path,
        tools_path=tools_path,
        lock_authority_path=lock_authority_path,
        lock_review_bundle_path=lock_review_bundle_path,
        toolchain=toolchain,
    )
    if overlays.toolchain_identity() != toolchain:
        raise InputError("toolchain changed while capturing source assertion")
    return inputs, requirement, assertion


def write_source_assertion(
    repository: Path,
    *,
    current_children_path: Path,
    lock_manifest_path: Path,
    tools_path: Path,
    lock_authority_path: Path,
    lock_review_bundle_path: Path,
    output: Path,
) -> dict[str, Any]:
    repository = exact_repository(repository)
    require_external_output(repository, output, "source assertion output")
    inputs, _requirement, assertion = capture_source_assertion(
        repository,
        current_children_path=current_children_path,
        lock_manifest_path=lock_manifest_path,
        tools_path=tools_path,
        lock_authority_path=lock_authority_path,
        lock_review_bundle_path=lock_review_bundle_path,
    )
    write_new(output, canonical_json(assertion), 0o444)
    published = overlays.immutable_canonical_snapshot(
        output.resolve(strict=True),
        overlays.SOURCE_REVIEW_ASSERTION_SCHEMA,
        "published source review assertion",
    )
    if published.value != assertion:
        raise InputError("published source assertion differs")
    overlays.resample_source_review_inputs(inputs)
    final_inputs, _final_requirement, final_assertion = capture_source_assertion(
        repository,
        current_children_path=current_children_path,
        lock_manifest_path=lock_manifest_path,
        tools_path=tools_path,
        lock_authority_path=lock_authority_path,
        lock_review_bundle_path=lock_review_bundle_path,
    )
    if final_assertion != assertion:
        raise InputError("source assertion changed after publication")
    overlays.resample_source_review_inputs(final_inputs)
    overlays.require_same_snapshot(
        published,
        overlays.immutable_canonical_snapshot(
            published.path,
            overlays.SOURCE_REVIEW_ASSERTION_SCHEMA,
            "terminal source review assertion",
        ),
        "source review assertion",
    )
    return assertion


def write_source_bundle(
    repository: Path,
    *,
    current_children_path: Path,
    lock_manifest_path: Path,
    tools_path: Path,
    lock_authority_path: Path,
    lock_review_bundle_path: Path,
    assertion_path: Path,
    seal_events_path: Path,
    output: Path,
) -> dict[str, Any]:
    repository = exact_repository(repository)
    require_external_output(repository, output, "source review bundle output")
    inputs, _requirement, assertion = capture_source_assertion(
        repository,
        current_children_path=current_children_path,
        lock_manifest_path=lock_manifest_path,
        tools_path=tools_path,
        lock_authority_path=lock_authority_path,
        lock_review_bundle_path=lock_review_bundle_path,
    )
    reviewed = overlays.immutable_canonical_snapshot(
        assertion_path.absolute(),
        overlays.SOURCE_REVIEW_ASSERTION_SCHEMA,
        "reviewed source assertion",
    )
    if reviewed.value != assertion:
        raise InputError("reviewed source assertion is stale")
    seal_log, events = seal_log_snapshot(seal_events_path)
    if seal_log.identity[:2] == (
        reviewed.identity["device"], reviewed.identity["inode"]
    ):
        raise InputError("Seal log aliases the reviewed source assertion")
    assertion_sha256 = hashlib.sha256(canonical_json(assertion)).hexdigest()
    created, verdict = select_seal_events(events, assertion_sha256)
    bundle_value = {
        "assertion": assertion,
        "assertion_sha256": assertion_sha256,
        "review_created": created,
        "schema": overlays.SOURCE_REVIEW_BUNDLE_SCHEMA,
        "verdict": verdict,
    }
    write_new(output, canonical_json(bundle_value), 0o444)
    bundle = overlays.immutable_canonical_snapshot(
        output.resolve(strict=True),
        overlays.SOURCE_REVIEW_BUNDLE_SCHEMA,
        "published source review bundle",
    )
    overlays.validate_source_review_bundle(bundle, assertion)
    overlays.resample_source_review_authority(inputs, bundle)
    overlays.require_same_snapshot(
        reviewed,
        overlays.immutable_canonical_snapshot(
            reviewed.path,
            overlays.SOURCE_REVIEW_ASSERTION_SCHEMA,
            "resampled reviewed source assertion",
        ),
        "reviewed source assertion",
    )
    require_same_snapshot(
        seal_log,
        snapshot_regular(seal_log.path, "resampled source Seal event log"),
        "source Seal event log",
    )
    final_inputs, _final_requirement, final_assertion = capture_source_assertion(
        repository,
        current_children_path=current_children_path,
        lock_manifest_path=lock_manifest_path,
        tools_path=tools_path,
        lock_authority_path=lock_authority_path,
        lock_review_bundle_path=lock_review_bundle_path,
    )
    if final_assertion != assertion:
        raise InputError("source assertion changed after bundle publication")
    overlays.resample_source_review_inputs(final_inputs)
    overlays.require_same_snapshot(
        reviewed,
        overlays.immutable_canonical_snapshot(
            reviewed.path,
            overlays.SOURCE_REVIEW_ASSERTION_SCHEMA,
            "terminal reviewed source assertion",
        ),
        "reviewed source assertion",
    )
    require_same_snapshot(
        seal_log,
        snapshot_regular(seal_log.path, "terminal source Seal event log"),
        "source Seal event log",
    )
    overlays.require_same_snapshot(
        bundle,
        overlays.immutable_canonical_snapshot(
            bundle.path,
            overlays.SOURCE_REVIEW_BUNDLE_SCHEMA,
            "terminal source review bundle",
        ),
        "source review bundle",
    )
    return bundle_value


def self_test() -> None:
    rejected = 0

    def reject(action: Callable[[], Any]) -> None:
        nonlocal rejected
        try:
            action()
        except (InputError, children.BuildError, locks.AuthorityError):
            rejected += 1
        else:
            raise AssertionError("hostile authority input was accepted")

    with tempfile.TemporaryDirectory(prefix="bn-znj5-authority-inputs-") as temporary:
        root = Path(temporary).resolve(strict=True)
        reject(lambda: exact_repository(root))
        reject(
            lambda: require_external_output(
                root, root / "inside.json", "fixture output"
            )
        )
        runtime_root = root / "runtime-smoke"
        runtime_root.mkdir()
        for name in sorted(
            (children.REQUIRED_TOOLS - children.CHILD_TOOLS) - {"perf", "strace"}
        ):
            runtime = runtime_root / children.REQUIRED_TOOL_COMMS[name]
            copy_new(children.SYSTEM_PYTHON, runtime, 0o555)
            runtime_smoke(
                runtime.resolve(strict=True), children.REQUIRED_TOOL_COMMS[name]
            )
        tool_paths: dict[str, Path] = {}
        support_paths: dict[str, Path] = {}
        for name in sorted(children.REQUIRED_TOOLS - children.CHILD_TOOLS):
            path = root / f"tool-{name}"
            write_new(path, f"tool:{name}\n".encode(), 0o555)
            tool_paths[name] = path.resolve(strict=True)
        for name in sorted(children.REQUIRED_SUPPORT_FILES):
            path = root / children.REQUIRED_SUPPORT_BASENAMES[name]
            write_new(path, f"support:{name}\n".encode(), 0o444)
            support_paths[name] = path.resolve(strict=True)
        tools_value = base_tools_value(tool_paths, support_paths)
        manifest = root / "tools.json"
        write_new(manifest, canonical_json(tools_value), 0o444)
        children.validate_tools_manifest(
            manifest.resolve(strict=True), allow_child_placeholders=True
        )
        drifted_tools = dict(tool_paths)
        drifted_tools.pop("perf")
        reject(lambda: base_tools_value(drifted_tools, support_paths))

        assertion = {"status": "approved"}
        assertion_sha256 = hashlib.sha256(canonical_json(assertion)).hexdigest()
        review_id = "cr-fixture"
        created = {
            "author": "reviewer",
            "data": {
                "description": "fixture",
                "initial_commit": "a" * 40,
                "jj_change_id": f"detached:{'a' * 40}",
                "review_id": review_id,
                "scm_anchor": f"detached:{'a' * 40}",
                "scm_kind": "git",
                "title": "fixture",
            },
            "event": "ReviewCreated",
            "ts": "2026-07-17T00:00:00Z",
        }
        verdict = {
            "author": "reviewer",
            "data": {
                "reason": (
                    "APPROVED assertion_sha256="
                    f"{assertion_sha256}; open_findings=0"
                ),
                "review_id": review_id,
                "vote": "lgtm",
            },
            "event": "ReviewerVoted",
            "ts": "2026-07-17T00:01:00Z",
        }
        approved = {
            "author": "reviewer",
            "data": {"review_id": review_id},
            "event": "ReviewApproved",
            "ts": "2026-07-17T00:01:01Z",
        }
        if select_seal_events([created, verdict, approved], assertion_sha256) != (
            created,
            verdict,
        ):
            raise AssertionError("canonical Seal events were not selected")
        reject(lambda: select_seal_events([created, verdict], assertion_sha256))
        reject(
            lambda: select_seal_events(
                [created, verdict, copy.deepcopy(verdict), approved],
                assertion_sha256,
            )
        )
        wrong_vote = copy.deepcopy(verdict)
        wrong_vote["data"]["reason"] = "approved"
        reject(
            lambda: select_seal_events(
                [created, wrong_vote, approved], assertion_sha256
            )
        )
        abandoned = {
            "author": "reviewer",
            "data": {"reason": "revoked", "review_id": review_id},
            "event": "ReviewAbandoned",
            "ts": "2026-07-17T00:02:00Z",
        }
        reject(
            lambda: select_seal_events(
                [created, verdict, approved, abandoned], assertion_sha256
            )
        )
        unresolved = {
            "author": "reviewer",
            "data": {
                "commit_hash": "a" * 40,
                "file_path": "file",
                "review_id": review_id,
                "selection": {"type": "Line", "line": 1},
                "thread_id": "th-fixture",
            },
            "event": "ThreadCreated",
            "ts": "2026-07-17T00:00:30Z",
        }
        reject(
            lambda: select_seal_events(
                [created, unresolved, verdict, approved], assertion_sha256
            )
        )

        event_path = root / "events.jsonl"
        write_new(
            event_path,
            b"".join(
                json.dumps(event, separators=(",", ":")).encode() + b"\n"
                for event in (created, verdict, approved)
            ),
            0o644,
        )
        event_snapshot, parsed_events = seal_log_snapshot(event_path)
        if parsed_events != [created, verdict, approved]:
            raise AssertionError("Seal event log parse differs")
        alias = root / "events-hardlink.jsonl"
        os.link(event_path, alias)
        reject(lambda: seal_log_snapshot(alias))
        alias.unlink()
        event_snapshot, _parsed_events = seal_log_snapshot(event_path)
        symlink = root / "events-symlink.jsonl"
        symlink.symlink_to(event_path)
        reject(lambda: seal_log_snapshot(symlink))
        write_new(root / "already.json", b"{}\n", 0o444)
        reject(lambda: write_new(root / "already.json", b"{}\n", 0o444))
        require_same_snapshot(
            event_snapshot,
            snapshot_regular(event_path.resolve(strict=True), "stable event log"),
            "stable event log",
        )

    print(
        canonical_json(
            {
                "hostile_inputs_rejected": rejected,
                "schema": SELF_TEST_SCHEMA,
                "status": "ok",
            }
        ).decode(),
        end="",
    )


def add_source_inputs(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repository", required=True, type=Path)
    parser.add_argument("--current-children-attestation", required=True, type=Path)
    parser.add_argument("--lock-manifest", required=True, type=Path)
    parser.add_argument("--tools", required=True, type=Path)
    parser.add_argument("--lock-authority", required=True, type=Path)
    parser.add_argument("--lock-review-bundle", required=True, type=Path)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("self-test")

    base = commands.add_parser("write-base-tools")
    base.add_argument("--repository", required=True, type=Path)
    base.add_argument("--perf", default=Path("/usr/bin/perf"), type=Path)
    base.add_argument("--strace", default=Path("/usr/bin/strace"), type=Path)
    base.add_argument("--output", required=True, type=Path)

    lock_assertion = commands.add_parser("write-lock-review-assertion")
    lock_assertion.add_argument("--repository", required=True, type=Path)
    lock_assertion.add_argument("--lock-manifest", required=True, type=Path)
    lock_assertion.add_argument("--output", required=True, type=Path)

    lock_bundle = commands.add_parser("write-lock-review-bundle")
    lock_bundle.add_argument("--repository", required=True, type=Path)
    lock_bundle.add_argument("--lock-manifest", required=True, type=Path)
    lock_bundle.add_argument("--assertion", required=True, type=Path)
    lock_bundle.add_argument("--seal-events", required=True, type=Path)
    lock_bundle.add_argument("--output", required=True, type=Path)

    source_assertion = commands.add_parser("write-source-review-assertion")
    add_source_inputs(source_assertion)
    source_assertion.add_argument("--output", required=True, type=Path)

    source_bundle = commands.add_parser("write-source-review-bundle")
    add_source_inputs(source_bundle)
    source_bundle.add_argument("--assertion", required=True, type=Path)
    source_bundle.add_argument("--seal-events", required=True, type=Path)
    source_bundle.add_argument("--output", required=True, type=Path)
    return root


def source_arguments(arguments: argparse.Namespace) -> dict[str, Path]:
    return {
        "current_children_path": arguments.current_children_attestation.absolute(),
        "lock_manifest_path": arguments.lock_manifest.absolute(),
        "tools_path": arguments.tools.absolute(),
        "lock_authority_path": arguments.lock_authority.absolute(),
        "lock_review_bundle_path": arguments.lock_review_bundle.absolute(),
    }


def result(schema: str, path: Path, value: dict[str, Any]) -> None:
    payload = canonical_json(value)
    print(
        canonical_json(
            {
                "path": str(path.resolve(strict=True)),
                "schema": schema,
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        ).decode(),
        end="",
    )


def main() -> int:
    arguments = parser().parse_args()
    try:
        if arguments.command == "self-test":
            self_test()
        elif arguments.command == "write-base-tools":
            manifest = write_base_tools(
                arguments.repository,
                arguments.output.resolve(),
                perf_source=arguments.perf,
                strace_source=arguments.strace,
            )
            value = children.validate_tools_manifest(
                manifest, allow_child_placeholders=True
            )
            result(BASE_TOOLS_RESULT_SCHEMA, manifest, value)
        elif arguments.command == "write-lock-review-assertion":
            value = write_lock_assertion(
                arguments.repository,
                arguments.lock_manifest.absolute(),
                arguments.output.resolve(),
            )
            result(LOCK_ASSERTION_RESULT_SCHEMA, arguments.output, value)
        elif arguments.command == "write-lock-review-bundle":
            value = write_lock_bundle(
                arguments.repository,
                arguments.lock_manifest.absolute(),
                arguments.assertion.absolute(),
                arguments.seal_events.absolute(),
                arguments.output.resolve(),
            )
            result(LOCK_BUNDLE_RESULT_SCHEMA, arguments.output, value)
        elif arguments.command == "write-source-review-assertion":
            value = write_source_assertion(
                arguments.repository,
                **source_arguments(arguments),
                output=arguments.output.resolve(),
            )
            result(SOURCE_ASSERTION_RESULT_SCHEMA, arguments.output, value)
        elif arguments.command == "write-source-review-bundle":
            value = write_source_bundle(
                arguments.repository,
                **source_arguments(arguments),
                assertion_path=arguments.assertion.absolute(),
                seal_events_path=arguments.seal_events.absolute(),
                output=arguments.output.resolve(),
            )
            result(SOURCE_BUNDLE_RESULT_SCHEMA, arguments.output, value)
        else:
            raise AssertionError(f"unknown command {arguments.command}")
    except (
        InputError,
        children.BuildError,
        locks.AuthorityError,
        overlays.PreparationError,
        OSError,
        subprocess.SubprocessError,
        ValueError,
        json.JSONDecodeError,
    ) as error:
        print(f"authority-inputs: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
