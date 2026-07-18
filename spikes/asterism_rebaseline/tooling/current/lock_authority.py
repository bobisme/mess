#!/usr/bin/env python3
"""Bind reviewed v3 lock candidates before any correctness child build.

This authority is deliberately narrower than the final source approval.  It
freezes the resolution-only ``lock-candidates.json`` result, the A/C/D lock
inputs consumed by public correctness children, and one reviewer-produced
Seal verdict/assertion bundle.  It never stages locks, invokes Cargo or rustc,
builds a child, or produces measurement evidence.

The normal ``write`` and ``validate`` paths delegate semantic validation to
``prepare_overlays.load_plan`` and ``prepare_overlays.validate_lock_manifest``.
Every file crossing this boundary is also read through one descriptor, checked
before and after the read, required to be an unaliased 0444 regular file, and
re-snapshotted after the slower repository/toolchain validation.  Builders
must consume the exact canonical hash binding printed by ``validate``, retain
a descriptor-bound read-only materialized tree and its full directory chain
across each build, and resample live filesystem admission immediately before
each build.
The admission frozen here is evidence of the reviewed staging event, not a
substitute for that live builder admission.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Callable


HERE = Path(__file__).resolve().parent
TOOLING = HERE.parent
if str(TOOLING) not in sys.path:
    sys.path.insert(0, str(TOOLING))

import prepare_overlays as overlays  # noqa: E402


AUTHORITY_SCHEMA = "bn-31gp-current-lock-authority-v1"
REVIEW_BUNDLE_SCHEMA = "bn-31gp-current-lock-review-bundle-v1"
VALIDATION_SCHEMA = "bn-31gp-current-lock-authority-validation-v1"
WRITE_RESULT_SCHEMA = "bn-31gp-current-lock-authority-write-v1"
SELF_TEST_SCHEMA = "bn-31gp-current-lock-authority-self-test-v1"
LOCK_SCHEMA = "asterism-rebaseline-lock-candidates-v3"
PROTOCOL = "bn-2l3n-asterism-rebaseline-v3"
LOCK_VARIANTS = ("A", "C", "D")
EXACT_MODE = 0o444
SHA256_LENGTH = 64
GIT_OBJECT_LENGTH = 40
REVIEW_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@+\-]{0,255}\Z")
ZONED_TIME = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    r"(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})\Z"
)


class AuthorityError(RuntimeError):
    """The lock-review authority is absent, stale, mutable, or inconsistent."""


@dataclass(frozen=True)
class FileIdentity:
    device: int
    inode: int
    link_count: int
    modified_ns: int
    changed_ns: int

    def canonical(self) -> dict[str, int]:
        return {
            "changed_ns": self.changed_ns,
            "device": self.device,
            "inode": self.inode,
            "link_count": self.link_count,
            "modified_ns": self.modified_ns,
        }


@dataclass(frozen=True)
class ImmutableSnapshot:
    path: Path
    payload: bytes
    sha256: str
    mode: int
    identity: FileIdentity
    value: dict[str, Any] | None = None

    @property
    def size(self) -> int:
        return len(self.payload)

    def binding(self) -> dict[str, Any]:
        return {
            "identity": self.identity.canonical(),
            "mode": self.mode,
            "path": str(self.path),
            "sha256": self.sha256,
            "size": self.size,
        }


@dataclass(frozen=True)
class DirectoryIdentity:
    device: int
    inode: int
    mode: int
    link_count: int
    size: int
    modified_ns: int
    changed_ns: int


@dataclass(frozen=True)
class DirectorySnapshot:
    path: Path
    identity: DirectoryIdentity


@dataclass(frozen=True)
class AuthorityContext:
    filesystem_admission: dict[str, Any]
    protocol_sha256: str
    toolchain: dict[str, Any]
    tooling_commit: str
    tooling_tree: str


@dataclass(frozen=True)
class ValidatedAuthority:
    authority: ImmutableSnapshot
    lock_manifest: ImmutableSnapshot
    locks: dict[str, ImmutableSnapshot]
    review_bundle: ImmutableSnapshot
    reviewed_stage_admission: dict[str, Any]


SemanticValidator = Callable[[dict[str, Any]], AuthorityContext]
IdentityRevalidator = Callable[[AuthorityContext], AuthorityContext]
AfterRead = Callable[[], None]


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


def sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def is_lower_hex(value: Any, length: int) -> bool:
    return (
        isinstance(value, str)
        and len(value) == length
        and all(character in "0123456789abcdef" for character in value)
    )


def parse_canonical(payload: bytes, schema: str, context: str) -> dict[str, Any]:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AuthorityError(f"{context} is not valid JSON") from error
    if not isinstance(value, dict) or canonical_json(value) != payload:
        raise AuthorityError(f"{context} is not a canonical JSON object")
    if value.get("schema") != schema:
        raise AuthorityError(f"{context} schema differs from exact authority")
    return value


def _identity(metadata: os.stat_result) -> FileIdentity:
    return FileIdentity(
        device=metadata.st_dev,
        inode=metadata.st_ino,
        link_count=metadata.st_nlink,
        modified_ns=metadata.st_mtime_ns,
        changed_ns=metadata.st_ctime_ns,
    )


def snapshot_file(
    path: Path,
    context: str,
    *,
    schema: str | None = None,
    after_read: AfterRead | None = None,
) -> ImmutableSnapshot:
    """Read one exact immutable file once and reject aliases or read races."""

    if not path.is_absolute():
        raise AuthorityError(f"{context} path is not absolute")
    lexical = path
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lexical, flags)
    except OSError as error:
        raise AuthorityError(
            f"{context} cannot be opened without following links"
        ) from error
    before: os.stat_result
    after: os.stat_result
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != EXACT_MODE
            or before.st_nlink != 1
        ):
            raise AuthorityError(
                f"{context} is not an unaliased exact 0444 regular file"
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
    if any(
        getattr(before, field) != getattr(after, field)
        for field in stable_fields
    ):
        raise AuthorityError(f"{context} changed while it was read")
    payload = b"".join(chunks)
    if len(payload) != after.st_size:
        raise AuthorityError(f"{context} size changed while it was read")
    if after_read is not None:
        after_read()
    try:
        resolved = lexical.resolve(strict=True)
        current = lexical.lstat()
    except OSError as error:
        raise AuthorityError(f"{context} path changed after its read") from error
    if (
        lexical != resolved
        or stat.S_ISLNK(current.st_mode)
        or any(
            getattr(current, field) != getattr(after, field)
            for field in stable_fields
        )
    ):
        raise AuthorityError(
            f"{context} path, mode, or identity changed after its read"
        )
    value = parse_canonical(payload, schema, context) if schema else None
    return ImmutableSnapshot(
        path=lexical,
        payload=payload,
        sha256=sha256(payload),
        mode=stat.S_IMODE(after.st_mode),
        identity=_identity(after),
        value=value,
    )


def require_same_snapshot(
    before: ImmutableSnapshot,
    after: ImmutableSnapshot,
    context: str,
) -> None:
    if before != after:
        raise AuthorityError(f"{context} changed across semantic validation")


def snapshot_identity(snapshot: ImmutableSnapshot) -> tuple[int, int]:
    return snapshot.identity.device, snapshot.identity.inode


def require_disjoint_lock_inputs(
    snapshots: dict[str, ImmutableSnapshot],
    other_inputs: tuple[ImmutableSnapshot, ...],
) -> None:
    """Reject logical variants or authority inputs backed by one file."""

    if set(snapshots) != set(LOCK_VARIANTS):
        raise AuthorityError("lock snapshot set differs")
    paths = [snapshots[variant].path for variant in LOCK_VARIANTS]
    identities = [
        snapshot_identity(snapshots[variant]) for variant in LOCK_VARIANTS
    ]
    if len(set(paths)) != len(paths):
        raise AuthorityError("A/C/D lock candidate paths are not distinct")
    if len(set(identities)) != len(identities):
        raise AuthorityError("A/C/D lock candidate identities are not distinct")
    other_paths = {snapshot.path for snapshot in other_inputs}
    other_identities = {snapshot_identity(snapshot) for snapshot in other_inputs}
    if set(paths) & other_paths or set(identities) & other_identities:
        raise AuthorityError("lock candidate aliases an authority input")


def snapshot_lock_inputs(
    lock_manifest: ImmutableSnapshot,
    *,
    disjoint_inputs: tuple[ImmutableSnapshot, ...] = (),
    disjoint_paths: tuple[Path, ...] = (),
) -> dict[str, ImmutableSnapshot]:
    value = lock_manifest.value
    if not isinstance(value, dict):
        raise AuthorityError("lock manifest was not parsed")
    variants = value.get("variants")
    if not isinstance(variants, dict) or not set(LOCK_VARIANTS).issubset(variants):
        raise AuthorityError("lock manifest lacks exact A/C/D candidates")
    snapshots: dict[str, ImmutableSnapshot] = {}
    forbidden_paths = {lock_manifest.path, *disjoint_paths}
    forbidden_paths.update(snapshot.path for snapshot in disjoint_inputs)
    for variant in LOCK_VARIANTS:
        claim = variants.get(variant)
        if not isinstance(claim, dict):
            raise AuthorityError(f"{variant} lock claim is not an object")
        path_value = claim.get("final_lock_path")
        expected_sha256 = claim.get("final_lock_sha256")
        if not isinstance(path_value, str) or not is_lower_hex(
            expected_sha256, SHA256_LENGTH
        ):
            raise AuthorityError(f"{variant} lock path/hash binding is invalid")
        expected_path = (
            lock_manifest.path.parent / "locks" / f"Cargo-{variant}.lock"
        )
        if path_value != str(expected_path) or expected_path in forbidden_paths:
            raise AuthorityError(f"{variant} lock candidate path is not exact")
        snapshot = snapshot_file(expected_path, f"{variant} lock candidate")
        if snapshot.sha256 != expected_sha256:
            raise AuthorityError(f"{variant} lock candidate payload differs")
        snapshots[variant] = snapshot
    require_disjoint_lock_inputs(
        snapshots,
        (lock_manifest, *disjoint_inputs),
    )
    return snapshots


def require_same_locks(
    before: dict[str, ImmutableSnapshot],
    after: dict[str, ImmutableSnapshot],
) -> None:
    if set(before) != set(LOCK_VARIANTS) or set(after) != set(LOCK_VARIANTS):
        raise AuthorityError("lock snapshot set differs")
    for variant in LOCK_VARIANTS:
        require_same_snapshot(
            before[variant], after[variant], f"{variant} lock candidate"
        )


def validate_identifier(value: Any, context: str) -> str:
    if not isinstance(value, str) or REVIEW_ID.fullmatch(value) is None:
        raise AuthorityError(f"{context} is absent or noncanonical")
    return value


def parse_zoned_time(value: Any, context: str) -> datetime:
    if not isinstance(value, str) or ZONED_TIME.fullmatch(value) is None:
        raise AuthorityError(f"{context} is not a canonical zoned time")
    normalized = (
        value[:-1] + "+00:00"
        if value.endswith("Z")
        else value
    )
    try:
        timestamp = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise AuthorityError(f"{context} is not ISO-8601") from error
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise AuthorityError(f"{context} has no UTC offset")
    return timestamp


def exact_repository(path: Path) -> Path:
    if not path.is_absolute():
        raise AuthorityError("repository path is not absolute")
    try:
        resolved = path.resolve(strict=True)
        metadata = path.lstat()
    except OSError as error:
        raise AuthorityError("repository path is unavailable") from error
    if path != resolved or stat.S_ISLNK(metadata.st_mode) or not path.is_dir():
        raise AuthorityError("repository path is aliased or not a directory")
    return path


def production_context(
    repository: Path,
    lock_value: dict[str, Any],
) -> AuthorityContext:
    """Delegate the semantic lock review to the frozen v3 tooling authority."""

    if overlays.LOCK_SCHEMA != LOCK_SCHEMA or overlays.PROTOCOL != PROTOCOL:
        raise AuthorityError("prepare_overlays v3 identity differs")
    toolchain = overlays.toolchain_identity()
    if lock_value.get("toolchain") != toolchain:
        raise AuthorityError("lock manifest toolchain differs from current toolchain")
    plan = overlays.load_plan(repository, toolchain)
    overlays.validate_lock_manifest(repository, lock_value, plan)
    tooling_commit, tooling_tree = overlays.tooling_identity(repository, toolchain)
    if not is_lower_hex(tooling_commit, GIT_OBJECT_LENGTH) or not is_lower_hex(
        tooling_tree, GIT_OBJECT_LENGTH
    ):
        raise AuthorityError("tooling Git identity is invalid")
    filesystem_admission = lock_value.get("filesystem_admission")
    if not isinstance(filesystem_admission, dict):
        raise AuthorityError("lock manifest filesystem admission is absent")
    return AuthorityContext(
        filesystem_admission=filesystem_admission,
        protocol_sha256=plan["protocol_sha256"],
        toolchain=toolchain,
        tooling_commit=tooling_commit,
        tooling_tree=tooling_tree,
    )


def production_identity_recheck(
    repository: Path,
    expected: AuthorityContext,
) -> AuthorityContext:
    """Freshly sample exact toolchain and clean Git identity without slow work."""

    toolchain = overlays.toolchain_identity()
    if toolchain != expected.toolchain:
        raise AuthorityError("toolchain changed at authority publication boundary")
    tooling_commit, tooling_tree = overlays.tooling_identity(repository, toolchain)
    observed = replace(
        expected,
        toolchain=toolchain,
        tooling_commit=tooling_commit,
        tooling_tree=tooling_tree,
    )
    require_same_context(expected, observed, "fresh identity recheck")
    return observed


def lock_manifest_binding(snapshot: ImmutableSnapshot) -> dict[str, Any]:
    if snapshot.value is None:
        raise AuthorityError("lock manifest snapshot has no canonical payload")
    binding = snapshot.binding()
    binding.update(
        {
            "payload": snapshot.value,
            "schema": LOCK_SCHEMA,
        }
    )
    return binding


def lock_input_bindings(
    snapshots: dict[str, ImmutableSnapshot],
) -> dict[str, dict[str, Any]]:
    if set(snapshots) != set(LOCK_VARIANTS):
        raise AuthorityError("lock input set differs from exact A/C/D")
    return {
        variant: snapshots[variant].binding() for variant in LOCK_VARIANTS
    }


def review_assertion(
    lock_manifest: ImmutableSnapshot,
    locks: dict[str, ImmutableSnapshot],
    context: AuthorityContext,
) -> dict[str, Any]:
    """Return the exact structured claim a reviewer must approve."""

    return {
        "filesystem_admission": context.filesystem_admission,
        "lock_inputs": lock_input_bindings(locks),
        "lock_manifest": lock_manifest.binding(),
        "open_findings": 0,
        "protocol": PROTOCOL,
        "protocol_sha256": context.protocol_sha256,
        "status": "approved",
        "toolchain": context.toolchain,
        "tooling_commit": context.tooling_commit,
        "tooling_tree": context.tooling_tree,
    }


def exact_seal_event(value: Any, event_name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "author",
        "data",
        "event",
        "ts",
    }:
        raise AuthorityError(f"Seal {event_name} event fields differ")
    # Seal author names are operational attribution on this host, not a
    # cryptographic actor identity.  Exact event content and digest binding
    # are therefore the enforceable local boundary.
    validate_identifier(value.get("author"), f"Seal {event_name} author")
    if value.get("event") != event_name or not isinstance(value.get("data"), dict):
        raise AuthorityError(f"Seal {event_name} event content differs")
    parse_zoned_time(value.get("ts"), f"Seal {event_name} timestamp")
    return value


def validate_review_bundle(
    review_bundle: ImmutableSnapshot,
    lock_manifest: ImmutableSnapshot,
    locks: dict[str, ImmutableSnapshot],
    context: AuthorityContext,
) -> tuple[str, str]:
    """Validate one frozen Seal anchor, verdict, and structured assertion."""

    bundle = review_bundle.value
    if not isinstance(bundle, dict) or set(bundle) != {
        "assertion",
        "assertion_sha256",
        "review_created",
        "schema",
        "verdict",
    }:
        raise AuthorityError("review bundle fields differ")
    assertion = review_assertion(lock_manifest, locks, context)
    assertion_sha256 = sha256(canonical_json(assertion))
    if (
        bundle.get("assertion") != assertion
        or bundle.get("assertion_sha256") != assertion_sha256
    ):
        raise AuthorityError("review bundle assertion binding differs")

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
        raise AuthorityError("Seal ReviewCreated data fields differ")
    review_id = validate_identifier(
        created_data.get("review_id"), "Seal review id"
    )
    detached_anchor = f"detached:{context.tooling_commit}"
    if (
        created_data.get("initial_commit") != context.tooling_commit
        or created_data.get("jj_change_id") != detached_anchor
        or created_data.get("scm_anchor") != detached_anchor
        or created_data.get("scm_kind") != "git"
        or not isinstance(created_data.get("title"), str)
        or not created_data["title"]
        or not isinstance(created_data.get("description"), str)
        or not created_data["description"]
    ):
        raise AuthorityError("Seal ReviewCreated anchor/content differs")

    verdict = exact_seal_event(bundle.get("verdict"), "ReviewerVoted")
    verdict_data = verdict["data"]
    expected_reason = (
        f"APPROVED assertion_sha256={assertion_sha256}; open_findings=0"
    )
    if set(verdict_data) != {"reason", "review_id", "vote"} or (
        verdict_data.get("review_id") != review_id
        or verdict_data.get("vote") != "lgtm"
        or verdict_data.get("reason") != expected_reason
    ):
        raise AuthorityError("Seal ReviewerVoted verdict content differs")
    created_at = parse_zoned_time(
        created.get("ts"), "Seal ReviewCreated timestamp"
    )
    reviewed_at = verdict["ts"]
    if parse_zoned_time(
        reviewed_at, "Seal ReviewerVoted timestamp"
    ) < created_at:
        raise AuthorityError("Seal verdict predates ReviewCreated")
    return review_id, reviewed_at


def review_bundle_binding(snapshot: ImmutableSnapshot) -> dict[str, Any]:
    if snapshot.value is None:
        raise AuthorityError("review bundle has no canonical payload")
    binding = snapshot.binding()
    binding.update(
        {
            "payload": snapshot.value,
            "schema": REVIEW_BUNDLE_SCHEMA,
        }
    )
    return binding


def authority_value(
    lock_manifest: ImmutableSnapshot,
    locks: dict[str, ImmutableSnapshot],
    context: AuthorityContext,
    review_bundle: ImmutableSnapshot,
) -> dict[str, Any]:
    review_id, reviewed_at = validate_review_bundle(
        review_bundle,
        lock_manifest,
        locks,
        context,
    )
    value = lock_manifest.value
    if not isinstance(value, dict):
        raise AuthorityError("lock manifest has no canonical object")
    if value.get("toolchain") != context.toolchain:
        raise AuthorityError("validated toolchain differs from lock payload")
    if value.get("filesystem_admission") != context.filesystem_admission:
        raise AuthorityError("validated filesystem admission differs from lock payload")
    return {
        "filesystem_admission": context.filesystem_admission,
        "lock_inputs": lock_input_bindings(locks),
        "lock_manifest": lock_manifest_binding(lock_manifest),
        "protocol": PROTOCOL,
        "protocol_sha256": context.protocol_sha256,
        "review_bundle": review_bundle_binding(review_bundle),
        "review_id": review_id,
        "review_sha256": review_bundle.sha256,
        "reviewed_at": reviewed_at,
        "schema": AUTHORITY_SCHEMA,
        "status": "approved",
        "toolchain": context.toolchain,
        "tooling_commit": context.tooling_commit,
        "tooling_tree": context.tooling_tree,
    }


def validate_authority_value(
    authority: ImmutableSnapshot,
    lock_manifest: ImmutableSnapshot,
    locks: dict[str, ImmutableSnapshot],
    context: AuthorityContext,
    review_bundle: ImmutableSnapshot,
) -> None:
    value = authority.value
    if not isinstance(value, dict):
        raise AuthorityError("lock-review authority has no canonical object")
    exact_fields = {
        "filesystem_admission",
        "lock_inputs",
        "lock_manifest",
        "protocol",
        "protocol_sha256",
        "review_bundle",
        "review_id",
        "review_sha256",
        "reviewed_at",
        "schema",
        "status",
        "toolchain",
        "tooling_commit",
        "tooling_tree",
    }
    if set(value) != exact_fields or value.get("status") != "approved":
        raise AuthorityError("lock-review authority fields/status differ")
    expected = authority_value(
        lock_manifest,
        locks,
        context,
        review_bundle,
    )
    if value != expected:
        raise AuthorityError("lock-review authority binding differs")


def capture_validated_locks(
    lock_manifest_path: Path,
    semantic_validator: SemanticValidator,
    *,
    disjoint_inputs: tuple[ImmutableSnapshot, ...] = (),
    disjoint_paths: tuple[Path, ...] = (),
) -> tuple[ImmutableSnapshot, dict[str, ImmutableSnapshot], AuthorityContext]:
    before_manifest = snapshot_file(
        lock_manifest_path,
        "lock candidates",
        schema=LOCK_SCHEMA,
    )
    before_locks = snapshot_lock_inputs(
        before_manifest,
        disjoint_inputs=disjoint_inputs,
        disjoint_paths=disjoint_paths,
    )
    if before_manifest.value is None:
        raise AuthorityError("lock candidates have no canonical payload")
    context = semantic_validator(before_manifest.value)
    after_manifest = snapshot_file(
        lock_manifest_path,
        "lock candidates",
        schema=LOCK_SCHEMA,
    )
    after_locks = snapshot_lock_inputs(
        after_manifest,
        disjoint_inputs=disjoint_inputs,
        disjoint_paths=disjoint_paths,
    )
    require_same_snapshot(
        before_manifest, after_manifest, "lock candidates"
    )
    require_same_locks(before_locks, after_locks)
    return after_manifest, after_locks, context


def require_same_context(
    expected: AuthorityContext,
    observed: AuthorityContext,
    context: str,
) -> None:
    if observed != expected:
        raise AuthorityError(f"{context} toolchain/tooling context changed")


def exact_new_output(path: Path) -> Path:
    if not path.is_absolute():
        raise AuthorityError("authority output path is not absolute")
    parent = path.parent
    try:
        resolved_parent = parent.resolve(strict=True)
        parent_metadata = parent.lstat()
    except OSError as error:
        raise AuthorityError("authority output parent is unavailable") from error
    if (
        parent != resolved_parent
        or stat.S_ISLNK(parent_metadata.st_mode)
        or not stat.S_ISDIR(parent_metadata.st_mode)
    ):
        raise AuthorityError("authority output parent is aliased")
    if path.exists() or path.is_symlink():
        raise AuthorityError("authority output must be absent")
    return path


def write_new_canonical(path: Path, value: dict[str, Any]) -> ImmutableSnapshot:
    path = exact_new_output(path)
    parent = path.parent
    payload = canonical_json(value)
    descriptor = -1
    created = False
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
            0o600,
        )
        created = True
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise AuthorityError("authority write made no progress")
            view = view[written:]
        os.fsync(descriptor)
        os.fchmod(descriptor, EXACT_MODE)
        os.fsync(descriptor)
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
            descriptor = -1
        if created:
            path.unlink(missing_ok=True)
        raise
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    directory_descriptor = os.open(parent, os.O_RDONLY | os.O_CLOEXEC)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)
    return snapshot_file(path, "lock-review authority", schema=AUTHORITY_SCHEMA)


def write_authority(
    repository: Path,
    lock_manifest_path: Path,
    review_bundle_path: Path,
    output: Path,
    *,
    semantic_validator: SemanticValidator | None = None,
    identity_revalidator: IdentityRevalidator | None = None,
) -> ImmutableSnapshot:
    repository = exact_repository(repository)
    output = exact_new_output(output)
    validator = semantic_validator or (
        lambda lock_value: production_context(repository, lock_value)
    )
    identity_validator = identity_revalidator or (
        (lambda expected: production_identity_recheck(repository, expected))
        if semantic_validator is None
        else (lambda expected: expected)
    )
    review_bundle = snapshot_file(
        review_bundle_path,
        "Seal review bundle",
        schema=REVIEW_BUNDLE_SCHEMA,
    )
    lock_manifest, locks, context = capture_validated_locks(
        lock_manifest_path,
        validator,
        disjoint_inputs=(review_bundle,),
        disjoint_paths=(output,),
    )
    post_semantic_review = snapshot_file(
        review_bundle_path,
        "Seal review bundle",
        schema=REVIEW_BUNDLE_SCHEMA,
    )
    require_same_snapshot(
        review_bundle,
        post_semantic_review,
        "Seal review bundle",
    )
    post_semantic_context = validator(lock_manifest.value or {})
    require_same_context(
        context,
        post_semantic_context,
        "post-semantic-validation",
    )
    post_semantic_identity = identity_validator(post_semantic_context)
    require_same_context(
        post_semantic_context,
        post_semantic_identity,
        "post-semantic identity recheck",
    )
    validate_review_bundle(
        post_semantic_review,
        lock_manifest,
        locks,
        post_semantic_identity,
    )

    publication_manifest = snapshot_file(
        lock_manifest_path,
        "lock candidates",
        schema=LOCK_SCHEMA,
    )
    publication_review = snapshot_file(
        review_bundle_path,
        "Seal review bundle",
        schema=REVIEW_BUNDLE_SCHEMA,
    )
    publication_locks = snapshot_lock_inputs(
        publication_manifest,
        disjoint_inputs=(publication_review,),
        disjoint_paths=(output,),
    )
    require_same_snapshot(
        lock_manifest,
        publication_manifest,
        "lock candidates",
    )
    require_same_snapshot(
        post_semantic_review,
        publication_review,
        "Seal review bundle",
    )
    require_same_locks(locks, publication_locks)
    value = authority_value(
        publication_manifest,
        publication_locks,
        post_semantic_identity,
        publication_review,
    )
    publication_context = identity_validator(post_semantic_identity)
    require_same_context(
        post_semantic_identity,
        publication_context,
        "pre-publication",
    )
    authority = write_new_canonical(output, value)

    final_manifest = snapshot_file(
        lock_manifest_path,
        "lock candidates",
        schema=LOCK_SCHEMA,
    )
    final_review = snapshot_file(
        review_bundle_path,
        "Seal review bundle",
        schema=REVIEW_BUNDLE_SCHEMA,
    )
    final_locks = snapshot_lock_inputs(
        final_manifest,
        disjoint_inputs=(final_review, authority),
    )
    require_same_snapshot(
        publication_manifest,
        final_manifest,
        "lock candidates",
    )
    require_same_snapshot(
        publication_review,
        final_review,
        "Seal review bundle",
    )
    require_same_locks(publication_locks, final_locks)
    validate_authority_value(
        authority,
        final_manifest,
        final_locks,
        publication_context,
        final_review,
    )
    return authority


def validate_authority(
    repository: Path,
    lock_manifest_path: Path,
    review_bundle_path: Path,
    authority_path: Path,
    *,
    semantic_validator: SemanticValidator | None = None,
    identity_revalidator: IdentityRevalidator | None = None,
) -> ValidatedAuthority:
    repository = exact_repository(repository)
    before_authority = snapshot_file(
        authority_path,
        "lock-review authority",
        schema=AUTHORITY_SCHEMA,
    )
    before_review = snapshot_file(
        review_bundle_path,
        "Seal review bundle",
        schema=REVIEW_BUNDLE_SCHEMA,
    )
    validator = semantic_validator or (
        lambda lock_value: production_context(repository, lock_value)
    )
    identity_validator = identity_revalidator or (
        (lambda expected: production_identity_recheck(repository, expected))
        if semantic_validator is None
        else (lambda expected: expected)
    )
    lock_manifest, locks, context = capture_validated_locks(
        lock_manifest_path,
        validator,
        disjoint_inputs=(before_review, before_authority),
    )
    after_authority = snapshot_file(
        authority_path,
        "lock-review authority",
        schema=AUTHORITY_SCHEMA,
    )
    require_same_snapshot(
        before_authority, after_authority, "lock-review authority"
    )
    after_review = snapshot_file(
        review_bundle_path,
        "Seal review bundle",
        schema=REVIEW_BUNDLE_SCHEMA,
    )
    require_same_snapshot(before_review, after_review, "Seal review bundle")
    current_context = validator(lock_manifest.value or {})
    require_same_context(context, current_context, "authority validation")
    current_identity = identity_validator(current_context)
    require_same_context(
        current_context,
        current_identity,
        "authority validation identity recheck",
    )
    validate_authority_value(
        after_authority,
        lock_manifest,
        locks,
        current_identity,
        after_review,
    )
    return ValidatedAuthority(
        authority=after_authority,
        lock_manifest=lock_manifest,
        locks=locks,
        review_bundle=after_review,
        reviewed_stage_admission=dict(current_identity.filesystem_admission),
    )


def require_reviewed_payload(
    observed: ImmutableSnapshot,
    expected: ImmutableSnapshot,
    context: str,
) -> None:
    if (
        observed.payload,
        observed.sha256,
        observed.size,
        observed.mode,
    ) != (
        expected.payload,
        expected.sha256,
        expected.size,
        expected.mode,
    ):
        raise AuthorityError(f"{context} differs from reviewed lock payload")


def _directory_identity(metadata: os.stat_result) -> DirectoryIdentity:
    return DirectoryIdentity(
        device=metadata.st_dev,
        inode=metadata.st_ino,
        mode=metadata.st_mode,
        link_count=metadata.st_nlink,
        size=metadata.st_size,
        modified_ns=metadata.st_mtime_ns,
        changed_ns=metadata.st_ctime_ns,
    )


def _require_directory_snapshot(
    path: Path,
    descriptor: int,
    context: str,
) -> DirectorySnapshot:
    """Bind one canonical, permission-read-only directory to its descriptor."""

    try:
        retained = os.fstat(descriptor)
        current = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise AuthorityError(f"{context} directory is unavailable") from error
    if (
        path != resolved
        or not stat.S_ISDIR(retained.st_mode)
        or not stat.S_ISDIR(current.st_mode)
        or stat.S_IMODE(retained.st_mode) & 0o222
        or _directory_identity(current) != _directory_identity(retained)
    ):
        raise AuthorityError(
            f"{context} directory is aliased, writable, or descriptor-divergent"
        )
    return DirectorySnapshot(path=path, identity=_directory_identity(retained))


def _require_same_directory(
    observed: os.stat_result,
    expected: DirectorySnapshot,
    context: str,
) -> None:
    if _directory_identity(observed) != expected.identity:
        raise AuthorityError(f"{context} directory identity changed")


def _snapshot_materialized_lock(
    parent_descriptor: int,
    name: str,
    path: Path,
    context: str,
) -> ImmutableSnapshot:
    """Read the lock relative to the retained parent, then bind its live path."""

    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(name, flags, dir_fd=parent_descriptor)
    except OSError as error:
        raise AuthorityError(
            f"{context} cannot be opened from retained directory"
        ) from error
    before: os.stat_result
    after: os.stat_result
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != EXACT_MODE
            or before.st_nlink != 1
        ):
            raise AuthorityError(
                f"{context} is not an unaliased exact 0444 regular file"
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
    if any(
        getattr(before, field) != getattr(after, field)
        for field in stable_fields
    ):
        raise AuthorityError(f"{context} changed while it was read")
    payload = b"".join(chunks)
    if len(payload) != after.st_size:
        raise AuthorityError(f"{context} size changed while it was read")
    try:
        relative = os.stat(
            name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        absolute = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise AuthorityError(f"{context} path changed after its read") from error
    if (
        path != resolved
        or stat.S_ISLNK(relative.st_mode)
        or any(
            getattr(metadata, field) != getattr(after, field)
            for metadata in (relative, absolute)
            for field in stable_fields
        )
    ):
        raise AuthorityError(
            f"{context} path, mode, or identity changed after its read"
        )
    return ImmutableSnapshot(
        path=path,
        payload=payload,
        sha256=sha256(payload),
        mode=stat.S_IMODE(after.st_mode),
        identity=_identity(after),
    )


class MaterializedTreeGuard:
    """Retain the exact tree selected for a read-only bwrap child build."""

    def __init__(
        self,
        path: Path,
        expected: ImmutableSnapshot,
        isolated_tree: Path,
        context: str,
    ) -> None:
        self._path = path
        self._expected = expected
        self._isolated_tree = isolated_tree
        self._context = context
        self._descriptors: list[int] = []
        self._directories: list[DirectorySnapshot] = []
        self._pre_build: ImmutableSnapshot | None = None
        self._post_build: ImmutableSnapshot | None = None
        self._active = False
        self._closed = False

    def _require_active(self) -> None:
        if not self._active or self._closed:
            raise AuthorityError(
                f"{self._context} materialized-tree guard is not active"
            )

    @property
    def pre_build(self) -> ImmutableSnapshot:
        if self._pre_build is None:
            raise AuthorityError(
                f"{self._context} pre-build snapshot is not captured"
            )
        return self._pre_build

    @property
    def post_build(self) -> ImmutableSnapshot:
        if self._active or self._post_build is None:
            raise AuthorityError(
                f"{self._context} post-build snapshot is not verified"
            )
        return self._post_build

    @property
    def pass_fds(self) -> tuple[int, ...]:
        self._require_active()
        return (self._descriptors[0],)

    @property
    def ro_bind_source(self) -> str:
        self._require_active()
        descriptor = self._descriptors[0]
        source = f"/proc/self/fd/{descriptor}"
        try:
            observed = os.stat(source)
        except OSError as error:
            raise AuthorityError(
                f"{self._context} retained tree fd is unavailable"
            ) from error
        _require_same_directory(
            observed,
            self._directories[0],
            self._context,
        )
        return source

    def bwrap_ro_bind(self, destination: str | Path) -> tuple[str, str, str]:
        source = self.ro_bind_source
        rendered = str(destination)
        canonical = PurePosixPath(rendered)
        if (
            not canonical.is_absolute()
            or rendered != str(canonical)
            or ".." in canonical.parts
        ):
            raise AuthorityError(
                f"{self._context} bwrap destination is not canonical absolute"
            )
        return "--ro-bind", source, rendered

    def _capture(self) -> None:
        if self._active or self._closed:
            raise AuthorityError(
                f"{self._context} materialized-tree guard cannot be reused"
            )
        path = self._path
        isolated_tree = self._isolated_tree
        if not path.is_absolute() or not isolated_tree.is_absolute():
            raise AuthorityError(
                f"{self._context} isolated-tree paths are not absolute"
            )
        try:
            tree_resolved = isolated_tree.resolve(strict=True)
            path_resolved = path.resolve(strict=True)
            relative = path.relative_to(isolated_tree)
        except (OSError, ValueError) as error:
            raise AuthorityError(
                f"{self._context} isolated tree is unavailable"
            ) from error
        if (
            isolated_tree != tree_resolved
            or path != path_resolved
            or not relative.parts
            or relative == Path(".")
        ):
            raise AuthorityError(
                f"{self._context} lock is aliased or outside isolated tree"
            )
        o_path = getattr(os, "O_PATH", None)
        if not isinstance(o_path, int):
            raise AuthorityError(
                f"{self._context} platform lacks descriptor-bound O_PATH"
            )
        directory_flags = (
            o_path
            | os.O_DIRECTORY
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            tree_descriptor = os.open(isolated_tree, directory_flags)
            self._descriptors.append(tree_descriptor)
            current_path = isolated_tree
            self._directories.append(
                _require_directory_snapshot(
                    current_path,
                    tree_descriptor,
                    self._context,
                )
            )
            for component in relative.parts[:-1]:
                child_descriptor = os.open(
                    component,
                    directory_flags,
                    dir_fd=self._descriptors[-1],
                )
                self._descriptors.append(child_descriptor)
                current_path /= component
                self._directories.append(
                    _require_directory_snapshot(
                        current_path,
                        child_descriptor,
                        self._context,
                    )
                )
            observed = _snapshot_materialized_lock(
                self._descriptors[-1],
                relative.parts[-1],
                path,
                self._context,
            )
            require_reviewed_payload(observed, self._expected, self._context)
            self._verify_directories()
            self._pre_build = observed
            self._active = True
        except (AuthorityError, OSError) as error:
            self._close()
            self._closed = True
            if isinstance(error, AuthorityError):
                raise
            raise AuthorityError(
                f"{self._context} directory chain cannot be retained"
            ) from error

    def _verify_directories(self) -> None:
        for index, (descriptor, expected) in enumerate(
            zip(self._descriptors, self._directories, strict=True)
        ):
            try:
                retained = os.fstat(descriptor)
                absolute = expected.path.lstat()
                resolved = expected.path.resolve(strict=True)
                relative = (
                    absolute
                    if index == 0
                    else os.stat(
                        expected.path.name,
                        dir_fd=self._descriptors[index - 1],
                        follow_symlinks=False,
                    )
                )
            except OSError as error:
                raise AuthorityError(
                    f"{self._context} directory chain is unavailable"
                ) from error
            if expected.path != resolved:
                raise AuthorityError(
                    f"{self._context} directory chain became aliased"
                )
            for metadata in (retained, absolute, relative):
                _require_same_directory(metadata, expected, self._context)

    def _verify_post_build(self) -> ImmutableSnapshot:
        self._require_active()
        self._verify_directories()
        observed = _snapshot_materialized_lock(
            self._descriptors[-1],
            self._path.name,
            self._path,
            self._context,
        )
        require_reviewed_payload(observed, self._expected, self._context)
        if observed != self.pre_build:
            raise AuthorityError(
                f"{self._context} lock changed across child build"
            )
        self._verify_directories()
        return observed

    def _close(self) -> None:
        while self._descriptors:
            os.close(self._descriptors.pop())

    def __enter__(self) -> MaterializedTreeGuard:
        self._capture()
        return self

    def __exit__(
        self,
        child_type: type[BaseException] | None,
        child_error: BaseException | None,
        child_traceback: Any,
    ) -> bool:
        verification_error: AuthorityError | None = None
        try:
            self._post_build = self._verify_post_build()
        except AuthorityError as error:
            verification_error = error
        finally:
            self._close()
            self._active = False
            self._closed = True
        if verification_error is not None:
            if child_error is not None:
                raise AuthorityError(
                    f"{self._context} post-build verification failed after "
                    f"child error {child_error!r}"
                ) from verification_error
            raise verification_error
        return False

    def __del__(self) -> None:
        try:
            self._close()
        except OSError:
            pass


def capture_prebuild_materialized_tree(
    path: Path,
    expected: ImmutableSnapshot,
    isolated_tree: Path,
    context: str,
) -> MaterializedTreeGuard:
    """Return the single-use context manager required around one child build."""

    return MaterializedTreeGuard(path, expected, isolated_tree, context)


def resample_builder_filesystem_admission(
    path: Path,
    reviewed_stage_admission: dict[str, Any],
) -> dict[str, Any]:
    """Resample live admission immediately before, never during, a build."""

    if not isinstance(reviewed_stage_admission, dict):
        raise AuthorityError("reviewed stage admission is not an object")
    observed = overlays.filesystem_admission(path)
    policy_fields = {
        "filesystem",
        "minimum_available_bytes",
        "minimum_available_inodes",
        "schema",
    }
    if {
        field: observed.get(field) for field in policy_fields
    } != {
        field: reviewed_stage_admission.get(field) for field in policy_fields
    }:
        raise AuthorityError("live builder admission policy differs from review")
    return observed


def validation_result(validated: ValidatedAuthority) -> dict[str, str]:
    """Return the exact canonical interface consumed by the child builder."""

    return {
        "authority_sha256": validated.authority.sha256,
        "lock_manifest_sha256": validated.lock_manifest.sha256,
        "schema": VALIDATION_SCHEMA,
        "status": "ok",
    }


def _fixture_write(path: Path, payload: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise AssertionError("fixture write made no progress")
            view = view[written:]
        os.fchmod(descriptor, EXACT_MODE)
    finally:
        os.close(descriptor)


def _mutated_snapshot(
    root: Path,
    name: str,
    value: dict[str, Any],
    schema: str,
) -> ImmutableSnapshot:
    path = root / name
    _fixture_write(path, canonical_json(value))
    return snapshot_file(path, name, schema=schema)


def _production_context_validator_boundary_self_test(repository: Path) -> None:
    """Exercise the live validator signature and production forwarding boundary."""

    try:
        overlays.validate_lock_manifest(repository, {}, {})
    except overlays.PreparationError:
        pass
    except TypeError as error:
        raise AssertionError(
            "current lock validator signature differs from production boundary"
        ) from error
    else:
        raise AssertionError("empty lock manifest unexpectedly passed validation")

    toolchain = {
        "cargo_path": "/toolchain/bin/cargo",
        "rustc_path": "/toolchain/bin/rustc",
        "rustup_toolchain": "1.97.0-x86_64-unknown-linux-gnu",
    }
    plan = {"protocol_sha256": "1" * SHA256_LENGTH}
    admission = {"schema": "fixture-filesystem-admission"}
    lock_value = {
        "filesystem_admission": admission,
        "toolchain": toolchain,
    }
    calls: list[tuple[Any, ...]] = []
    original_toolchain_identity = overlays.toolchain_identity
    original_load_plan = overlays.load_plan
    original_validate_lock_manifest = overlays.validate_lock_manifest
    original_tooling_identity = overlays.tooling_identity

    def fixture_toolchain_identity() -> dict[str, str]:
        calls.append(("toolchain_identity",))
        return toolchain

    def fixture_load_plan(
        observed_repository: Path,
        observed_toolchain: dict[str, Any],
    ) -> dict[str, Any]:
        calls.append(("load_plan", observed_repository, observed_toolchain))
        return plan

    def fixture_validate_lock_manifest(
        observed_repository: Path,
        observed_locks: dict[str, Any],
        observed_plan: dict[str, Any],
    ) -> None:
        calls.append(
            (
                "validate_lock_manifest",
                observed_repository,
                observed_locks,
                observed_plan,
            )
        )

    def fixture_tooling_identity(
        observed_repository: Path,
        observed_toolchain: dict[str, Any],
    ) -> tuple[str, str]:
        calls.append(("tooling_identity", observed_repository, observed_toolchain))
        return "2" * GIT_OBJECT_LENGTH, "3" * GIT_OBJECT_LENGTH

    try:
        overlays.toolchain_identity = fixture_toolchain_identity
        overlays.load_plan = fixture_load_plan
        overlays.validate_lock_manifest = fixture_validate_lock_manifest
        overlays.tooling_identity = fixture_tooling_identity
        context = production_context(repository, lock_value)
    finally:
        overlays.toolchain_identity = original_toolchain_identity
        overlays.load_plan = original_load_plan
        overlays.validate_lock_manifest = original_validate_lock_manifest
        overlays.tooling_identity = original_tooling_identity

    if calls != [
        ("toolchain_identity",),
        ("load_plan", repository, toolchain),
        ("validate_lock_manifest", repository, lock_value, plan),
        ("tooling_identity", repository, toolchain),
    ]:
        raise AssertionError("production lock validator call boundary differs")
    if context != AuthorityContext(
        filesystem_admission=admission,
        protocol_sha256=plan["protocol_sha256"],
        toolchain=toolchain,
        tooling_commit="2" * GIT_OBJECT_LENGTH,
        tooling_tree="3" * GIT_OBJECT_LENGTH,
    ):
        raise AssertionError("production lock validator context differs")


def self_test() -> None:
    hostile = 0

    def expect_rejected(action: Callable[[], Any]) -> None:
        nonlocal hostile
        try:
            action()
        except (AuthorityError, overlays.PreparationError):
            hostile += 1
            return
        raise AssertionError("hostile lock authority mutation was accepted")

    with tempfile.TemporaryDirectory(prefix="bn-31gp-lock-authority-") as raw:
        root = Path(raw).resolve()
        _production_context_validator_boundary_self_test(root)
        locks_directory = root / "locks"
        locks_directory.mkdir()
        variants: dict[str, Any] = {}
        for variant in ("A", "B", "C", "D"):
            payload = f"# lock {variant}\nversion = 4\n".encode()
            path = locks_directory / f"Cargo-{variant}.lock"
            _fixture_write(path, payload)
            variants[variant] = {
                "final_lock_path": str(path),
                "final_lock_sha256": sha256(payload),
            }
        toolchain = {
            "cargo_path": "/toolchain/bin/cargo",
            "rustc_path": "/toolchain/bin/rustc",
            "rustup_toolchain": "1.97.0-x86_64-unknown-linux-gnu",
        }
        admission = {
            "available_bytes": 256 * 1024**3,
            "available_inodes": 2_000_000,
            "filesystem": "ext4",
            "minimum_available_bytes": 128 * 1024**3,
            "minimum_available_inodes": 1_000_000,
            "schema": "asterism-rebaseline-filesystem-admission-v3",
        }
        protocol_sha256 = "1" * SHA256_LENGTH
        context = AuthorityContext(
            filesystem_admission=admission,
            protocol_sha256=protocol_sha256,
            toolchain=toolchain,
            tooling_commit="2" * GIT_OBJECT_LENGTH,
            tooling_tree="3" * GIT_OBJECT_LENGTH,
        )
        lock_value = {
            "filesystem_admission": admission,
            "protocol": PROTOCOL,
            "protocol_sha256": protocol_sha256,
            "schema": LOCK_SCHEMA,
            "toolchain": toolchain,
            "variants": variants,
        }
        lock_manifest_path = root / "lock-candidates.json"
        _fixture_write(lock_manifest_path, canonical_json(lock_value))
        lock_manifest = snapshot_file(
            lock_manifest_path, "fixture lock candidates", schema=LOCK_SCHEMA
        )
        locks = snapshot_lock_inputs(lock_manifest)
        assertion = review_assertion(lock_manifest, locks, context)
        assertion_sha256 = sha256(canonical_json(assertion))
        review_id = "cr-review-31gp"
        created_at = "2026-07-16T17:59:00.000000000Z"
        reviewed_at = "2026-07-16T18:00:00.000000000Z"
        bundle_value = {
            "assertion": assertion,
            "assertion_sha256": assertion_sha256,
            "review_created": {
                "author": "mess-reviewer",
                "data": {
                    "description": "Review exact lock authority inputs.",
                    "initial_commit": context.tooling_commit,
                    "jj_change_id": f"detached:{context.tooling_commit}",
                    "review_id": review_id,
                    "scm_anchor": f"detached:{context.tooling_commit}",
                    "scm_kind": "git",
                    "title": "Exact lock authority review",
                },
                "event": "ReviewCreated",
                "ts": created_at,
            },
            "schema": REVIEW_BUNDLE_SCHEMA,
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
                "ts": reviewed_at,
            },
        }
        review_bundle_path = root / "seal-review-bundle.json"
        _fixture_write(review_bundle_path, canonical_json(bundle_value))
        review_bundle = snapshot_file(
            review_bundle_path,
            "fixture Seal review bundle",
            schema=REVIEW_BUNDLE_SCHEMA,
        )
        validate_review_bundle(
            review_bundle,
            lock_manifest,
            locks,
            context,
        )
        base_value = authority_value(
            lock_manifest,
            locks,
            context,
            review_bundle,
        )
        authority_path = root / "lock-review-authority.json"
        _fixture_write(authority_path, canonical_json(base_value))
        authority = snapshot_file(
            authority_path, "fixture authority", schema=AUTHORITY_SCHEMA
        )
        validate_authority_value(
            authority,
            lock_manifest,
            locks,
            context,
            review_bundle,
        )
        validated = validate_authority(
            root,
            lock_manifest_path,
            review_bundle_path,
            authority_path,
            semantic_validator=lambda _value: context,
        )
        if validation_result(validated) != {
            "authority_sha256": authority.sha256,
            "lock_manifest_sha256": lock_manifest.sha256,
            "schema": VALIDATION_SCHEMA,
            "status": "ok",
        }:
            raise AssertionError("builder validation interface differs")
        written = write_authority(
            root,
            lock_manifest_path,
            review_bundle_path,
            root / "written-authority.json",
            semantic_validator=lambda _value: context,
        )
        if written.mode != EXACT_MODE:
            raise AssertionError("written authority mode differs")

        def changed(path: tuple[str, ...], replacement: Any) -> dict[str, Any]:
            value = json.loads(json.dumps(base_value))
            cursor: Any = value
            for component in path[:-1]:
                cursor = cursor[component]
            cursor[path[-1]] = replacement
            return value

        stale_authority = dict(base_value)
        stale_authority["schema"] = "bn-31gp-current-lock-authority-v0"
        stale_authority_path = root / "stale-authority.json"
        _fixture_write(stale_authority_path, canonical_json(stale_authority))
        expect_rejected(
            lambda: snapshot_file(
                stale_authority_path,
                "stale authority",
                schema=AUTHORITY_SCHEMA,
            )
        )

        stale_locks = dict(lock_value)
        stale_locks["schema"] = "asterism-rebaseline-lock-candidates-v2"
        stale_locks_path = root / "stale-locks.json"
        _fixture_write(stale_locks_path, canonical_json(stale_locks))
        expect_rejected(
            lambda: snapshot_file(
                stale_locks_path, "stale locks", schema=LOCK_SCHEMA
            )
        )
        stale_locks_v1 = dict(lock_value)
        stale_locks_v1["schema"] = "asterism-rebaseline-lock-candidates-v1"
        stale_locks_v1_path = root / "stale-locks-v1.json"
        _fixture_write(stale_locks_v1_path, canonical_json(stale_locks_v1))
        expect_rejected(
            lambda: snapshot_file(
                stale_locks_v1_path, "stale v1 locks", schema=LOCK_SCHEMA
            )
        )

        mutations = (
            (("status",), "pending"),
            (("protocol",), "bn-2l3n-asterism-rebaseline-v2"),
            (("protocol_sha256",), "4" * SHA256_LENGTH),
            (("toolchain", "rustc_path"), "/tmp/rustc"),
            (("filesystem_admission", "filesystem"), "tmpfs"),
            (("tooling_commit",), "5" * GIT_OBJECT_LENGTH),
            (("tooling_tree",), "6" * GIT_OBJECT_LENGTH),
            (("review_id",), ""),
            (("review_id",), "review id with spaces"),
            (("reviewed_at",), "2026-07-16T14:00:00"),
            (("lock_manifest", "path"), "/tmp/lock-candidates.json"),
            (("lock_manifest", "sha256"), "7" * SHA256_LENGTH),
            (("lock_manifest", "mode"), 0o644),
            (("lock_manifest", "identity", "inode"), 1),
            (("lock_manifest", "schema"), "asterism-rebaseline-lock-candidates-v2"),
            (("lock_manifest", "payload", "protocol"), "mutated"),
            (("lock_inputs", "A", "path"), "/tmp/Cargo-A.lock"),
            (("lock_inputs", "C", "sha256"), "8" * SHA256_LENGTH),
            (("lock_inputs", "D", "mode"), 0o644),
            (("lock_inputs", "A", "identity", "device"), 999),
        )
        for index, (path, replacement) in enumerate(mutations):
            mutated = _mutated_snapshot(
                root,
                f"mutated-{index}.json",
                changed(path, replacement),
                AUTHORITY_SCHEMA,
            )
            expect_rejected(
                lambda mutated=mutated: validate_authority_value(
                    mutated,
                    lock_manifest,
                    locks,
                    context,
                    review_bundle,
                )
            )

        missing_variant = json.loads(json.dumps(base_value))
        del missing_variant["lock_inputs"]["D"]
        missing_variant_snapshot = _mutated_snapshot(
            root,
            "missing-variant.json",
            missing_variant,
            AUTHORITY_SCHEMA,
        )
        expect_rejected(
            lambda: validate_authority_value(
                missing_variant_snapshot,
                lock_manifest,
                locks,
                context,
                review_bundle,
            )
        )

        extra_field = dict(base_value)
        extra_field["unreviewed"] = True
        extra_field_snapshot = _mutated_snapshot(
            root,
            "extra-field.json",
            extra_field,
            AUTHORITY_SCHEMA,
        )
        expect_rejected(
            lambda: validate_authority_value(
                extra_field_snapshot,
                lock_manifest,
                locks,
                context,
                review_bundle,
            )
        )

        changed_review = _mutated_snapshot(
            root,
            "changed-review.json",
            changed(("review_id",), "review/cr-other"),
            AUTHORITY_SCHEMA,
        )
        expect_rejected(
            lambda: validate_authority_value(
                changed_review,
                lock_manifest,
                locks,
                context,
                review_bundle,
            )
        )
        changed_review_time = _mutated_snapshot(
            root,
            "changed-review-time.json",
            changed(("reviewed_at",), "2026-07-16T15:00:00-04:00"),
            AUTHORITY_SCHEMA,
        )
        expect_rejected(
            lambda: validate_authority_value(
                changed_review_time,
                lock_manifest,
                locks,
                context,
                review_bundle,
            )
        )

        def changed_bundle(
            path: tuple[str, ...], replacement: Any, name: str
        ) -> ImmutableSnapshot:
            value = json.loads(json.dumps(bundle_value))
            cursor: Any = value
            for component in path[:-1]:
                cursor = cursor[component]
            cursor[path[-1]] = replacement
            return _mutated_snapshot(
                root,
                name,
                value,
                REVIEW_BUNDLE_SCHEMA,
            )

        wrong_anchor = changed_bundle(
            ("review_created", "data", "initial_commit"),
            "a" * GIT_OBJECT_LENGTH,
            "wrong-review-anchor.json",
        )
        expect_rejected(
            lambda: validate_review_bundle(
                wrong_anchor,
                lock_manifest,
                locks,
                context,
            )
        )
        blocking_verdict = changed_bundle(
            ("verdict", "data", "vote"),
            "block",
            "blocking-review-verdict.json",
        )
        expect_rejected(
            lambda: validate_review_bundle(
                blocking_verdict,
                lock_manifest,
                locks,
                context,
            )
        )
        open_finding = changed_bundle(
            ("assertion", "open_findings"),
            1,
            "open-finding-review.json",
        )
        expect_rejected(
            lambda: validate_review_bundle(
                open_finding,
                lock_manifest,
                locks,
                context,
            )
        )

        changed_stage = root / "changed-stage"
        changed_stage.mkdir()
        changed_stage_locks = changed_stage / "locks"
        changed_stage_locks.mkdir()
        changed_lock_value = json.loads(json.dumps(lock_value))
        changed_lock_value["created_at"] = "stale-review-manifest"
        for variant in ("A", "B", "C", "D"):
            source = locks_directory / f"Cargo-{variant}.lock"
            destination = changed_stage_locks / f"Cargo-{variant}.lock"
            _fixture_write(destination, source.read_bytes())
            changed_lock_value["variants"][variant]["final_lock_path"] = str(
                destination
            )
        changed_manifest = _mutated_snapshot(
            changed_stage,
            "lock-candidates.json",
            changed_lock_value,
            LOCK_SCHEMA,
        )
        changed_manifest_locks = snapshot_lock_inputs(changed_manifest)
        expect_rejected(
            lambda: validate_review_bundle(
                review_bundle,
                changed_manifest,
                changed_manifest_locks,
                context,
            )
        )

        changed_tooling = replace(
            context,
            tooling_commit="a" * GIT_OBJECT_LENGTH,
        )
        changed_toolchain = replace(
            context,
            toolchain={**toolchain, "rustc_path": "/changed/rustc"},
        )
        stale_admission = replace(
            context,
            filesystem_admission={**admission, "available_bytes": 1},
        )
        for changed_context in (
            changed_tooling,
            changed_toolchain,
            stale_admission,
        ):
            expect_rejected(
                lambda changed_context=changed_context: validate_review_bundle(
                    review_bundle,
                    lock_manifest,
                    locks,
                    changed_context,
                )
            )

        fabricated_admission_value = json.loads(json.dumps(bundle_value))
        fabricated_admission_value["assertion"]["filesystem_admission"] = {
            **admission,
            "available_bytes": 1,
        }
        fabricated_digest = sha256(
            canonical_json(fabricated_admission_value["assertion"])
        )
        fabricated_admission_value["assertion_sha256"] = fabricated_digest
        fabricated_admission_value["verdict"]["data"]["reason"] = (
            f"APPROVED assertion_sha256={fabricated_digest}; open_findings=0"
        )
        fabricated_admission = _mutated_snapshot(
            root,
            "fabricated-admission-review.json",
            fabricated_admission_value,
            REVIEW_BUNDLE_SCHEMA,
        )
        expect_rejected(
            lambda: validate_review_bundle(
                fabricated_admission,
                lock_manifest,
                locks,
                context,
            )
        )

        wrong_filename_value = json.loads(json.dumps(lock_value))
        wrong_filename_value["variants"]["A"]["final_lock_path"] = str(
            locks_directory / "Cargo-wrong.lock"
        )
        wrong_filename_manifest = _mutated_snapshot(
            root,
            "wrong-filename-locks.json",
            wrong_filename_value,
            LOCK_SCHEMA,
        )
        expect_rejected(lambda: snapshot_lock_inputs(wrong_filename_manifest))

        duplicate_path_value = json.loads(json.dumps(lock_value))
        duplicate_path_value["variants"]["C"]["final_lock_path"] = str(
            locks_directory / "Cargo-A.lock"
        )
        duplicate_path_manifest = _mutated_snapshot(
            root,
            "duplicate-path-locks.json",
            duplicate_path_value,
            LOCK_SCHEMA,
        )
        expect_rejected(lambda: snapshot_lock_inputs(duplicate_path_manifest))

        duplicate_inode_locks = dict(locks)
        duplicate_inode_locks["C"] = replace(
            locks["C"], identity=locks["A"].identity
        )
        expect_rejected(
            lambda: require_disjoint_lock_inputs(
                duplicate_inode_locks,
                (lock_manifest, review_bundle),
            )
        )
        cross_input_locks = dict(locks)
        cross_input_locks["D"] = replace(
            locks["D"], identity=review_bundle.identity
        )
        expect_rejected(
            lambda: require_disjoint_lock_inputs(
                cross_input_locks,
                (lock_manifest, review_bundle),
            )
        )

        mode_path = root / "bad-mode.lock"
        _fixture_write(mode_path, b"mode\n")
        os.chmod(mode_path, 0o644)
        expect_rejected(lambda: snapshot_file(mode_path, "bad-mode lock"))

        symlink_path = root / "aliased.lock"
        symlink_path.symlink_to(locks_directory / "Cargo-A.lock")
        expect_rejected(lambda: snapshot_file(symlink_path, "symlink lock"))

        hardlink_path = root / "hardlinked.lock"
        os.link(locks_directory / "Cargo-C.lock", hardlink_path)
        expect_rejected(
            lambda: snapshot_file(
                locks_directory / "Cargo-C.lock",
                "hardlinked candidate",
            )
        )
        hardlink_path.unlink()

        race_path = root / "race.lock"
        replacement_path = root / "race-replacement.lock"
        _fixture_write(race_path, b"before\n")
        _fixture_write(replacement_path, b"after\n")
        expect_rejected(
            lambda: snapshot_file(
                race_path,
                "racing candidate",
                after_read=lambda: os.replace(replacement_path, race_path),
            )
        )

        toctou_lock_path = root / "toctou-lock-candidates.json"
        _fixture_write(toctou_lock_path, canonical_json(lock_value))

        def mutate_during_semantic(_value: dict[str, Any]) -> AuthorityContext:
            mutated = json.loads(json.dumps(lock_value))
            mutated["protocol_sha256"] = "9" * SHA256_LENGTH
            os.chmod(toctou_lock_path, 0o600)
            with toctou_lock_path.open("wb") as handle:
                handle.write(canonical_json(mutated))
            os.chmod(toctou_lock_path, EXACT_MODE)
            return context

        expect_rejected(
            lambda: capture_validated_locks(
                toctou_lock_path, mutate_during_semantic
            )
        )

        post_semantic_contexts = iter((context, changed_toolchain))
        expect_rejected(
            lambda: write_authority(
                root,
                lock_manifest_path,
                review_bundle_path,
                root / "changed-after-semantic-authority.json",
                semantic_validator=lambda _value: next(
                    post_semantic_contexts
                ),
            )
        )
        publication_identities = iter((context, changed_tooling))
        expect_rejected(
            lambda: write_authority(
                root,
                lock_manifest_path,
                review_bundle_path,
                root / "changed-before-publication-authority.json",
                semantic_validator=lambda _value: context,
                identity_revalidator=lambda _expected: next(
                    publication_identities
                ),
            )
        )

        isolated_tree = root / "isolated-tree"
        isolated_tree.mkdir()
        materialized_directory = isolated_tree / "nested" / "source"
        materialized_directory.mkdir(parents=True)
        materialized_path = materialized_directory / "Cargo.lock"
        _fixture_write(materialized_path, locks["A"].payload)
        os.chmod(materialized_directory, 0o555)
        os.chmod(materialized_directory.parent, 0o555)
        os.chmod(isolated_tree, 0o555)
        guard = capture_prebuild_materialized_tree(
            materialized_path,
            locks["A"],
            isolated_tree,
            "A materialized tree",
        )
        expect_rejected(lambda: guard.pass_fds)
        with guard as active_guard:
            pre_build = active_guard.pre_build
            if active_guard.pass_fds != (
                int(active_guard.ro_bind_source.rsplit("/", 1)[1]),
            ):
                raise AssertionError("guard fd source and pass_fds differ")
            if active_guard.bwrap_ro_bind("/asterism/source") != (
                "--ro-bind",
                active_guard.ro_bind_source,
                "/asterism/source",
            ):
                raise AssertionError("guard bwrap read-only bind differs")
            if (
                Path(active_guard.ro_bind_source)
                / "nested"
                / "source"
                / "Cargo.lock"
            ).read_bytes() != locks["A"].payload:
                raise AssertionError("retained tree does not expose reviewed A")
            expect_rejected(lambda: active_guard.post_build)
            expect_rejected(lambda: active_guard.bwrap_ro_bind("relative"))
        if guard.post_build != pre_build:
            raise AssertionError("normal post-build snapshot differs")
        expect_rejected(lambda: guard.pass_fds)

        def swap_read_restore_tree() -> None:
            reviewed_tree = root / "isolated-tree-reviewed"
            with capture_prebuild_materialized_tree(
                materialized_path,
                locks["A"],
                isolated_tree,
                "swapped/restored materialized tree",
            ) as retained:
                retained_lock = (
                    Path(retained.ro_bind_source)
                    / "nested"
                    / "source"
                    / "Cargo.lock"
                )
                isolated_tree.rename(reviewed_tree)
                try:
                    materialized_directory.mkdir(parents=True)
                    _fixture_write(materialized_path, locks["C"].payload)
                    os.chmod(materialized_directory, 0o555)
                    os.chmod(materialized_directory.parent, 0o555)
                    os.chmod(isolated_tree, 0o555)
                    if materialized_path.read_bytes() != locks["C"].payload:
                        raise AssertionError("hostile absolute tree lacks C")
                    if retained_lock.read_bytes() != locks["A"].payload:
                        raise AssertionError("retained tree stopped selecting A")
                finally:
                    if isolated_tree.exists():
                        os.chmod(materialized_directory, 0o755)
                        os.chmod(materialized_directory.parent, 0o755)
                        os.chmod(isolated_tree, 0o755)
                        materialized_path.unlink()
                        materialized_directory.rmdir()
                        materialized_directory.parent.rmdir()
                        isolated_tree.rmdir()
                    reviewed_tree.rename(isolated_tree)

        expect_rejected(swap_read_restore_tree)

        writable_tree = root / "writable-isolated-tree"
        writable_tree.mkdir()
        writable_lock = writable_tree / "Cargo.lock"
        _fixture_write(writable_lock, locks["A"].payload)
        expect_rejected(
            lambda: (
                capture_prebuild_materialized_tree(
                    writable_lock,
                    locks["A"],
                    writable_tree,
                    "writable materialized tree",
                ).__enter__()
            )
        )
        os.chmod(materialized_directory, 0o755)
        os.chmod(materialized_directory.parent, 0o755)
        os.chmod(isolated_tree, 0o755)

        validation_arguments = vars(
            parser().parse_args(
                [
                    "validate",
                    "--repository",
                    str(root),
                    "--lock-manifest",
                    str(lock_manifest_path),
                    "--review-bundle",
                    str(review_bundle_path),
                    "--authority",
                    str(authority_path),
                ]
            )
        )
        if set(validation_arguments) != {
            "authority",
            "command",
            "lock_manifest",
            "repository",
            "review_bundle",
        }:
            raise AssertionError("builder validate CLI fields differ")
        write_arguments = vars(
            parser().parse_args(
                [
                    "write",
                    "--repository",
                    str(root),
                    "--lock-manifest",
                    str(lock_manifest_path),
                    "--review-bundle",
                    str(review_bundle_path),
                    "--output",
                    str(root / "cli-authority.json"),
                ]
            )
        )
        if set(write_arguments) != {
            "command",
            "lock_manifest",
            "output",
            "repository",
            "review_bundle",
        }:
            raise AssertionError("authority write CLI fields differ")

    print(
        canonical_json(
            {
                "hostile_mutations_rejected": hostile,
                "schema": SELF_TEST_SCHEMA,
                "status": "ok",
            }
        ).decode(),
        end="",
    )


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("self-test")

    write = commands.add_parser("write")
    write.add_argument("--repository", required=True, type=Path)
    write.add_argument("--lock-manifest", required=True, type=Path)
    write.add_argument("--review-bundle", required=True, type=Path)
    write.add_argument("--output", required=True, type=Path)

    validate = commands.add_parser("validate")
    validate.add_argument("--repository", required=True, type=Path)
    validate.add_argument("--lock-manifest", required=True, type=Path)
    validate.add_argument("--review-bundle", required=True, type=Path)
    validate.add_argument("--authority", required=True, type=Path)
    return root


def main() -> int:
    arguments = parser().parse_args()
    try:
        if arguments.command == "self-test":
            self_test()
        elif arguments.command == "write":
            snapshot = write_authority(
                arguments.repository,
                arguments.lock_manifest,
                arguments.review_bundle,
                arguments.output,
            )
            print(
                canonical_json(
                    {
                        "authority_path": str(snapshot.path),
                        "authority_sha256": snapshot.sha256,
                        "schema": WRITE_RESULT_SCHEMA,
                    }
                ).decode(),
                end="",
            )
        elif arguments.command == "validate":
            validated = validate_authority(
                arguments.repository,
                arguments.lock_manifest,
                arguments.review_bundle,
                arguments.authority,
            )
            print(
                canonical_json(validation_result(validated)).decode(),
                end="",
            )
        else:
            raise AssertionError(f"unknown command {arguments.command}")
    except (AuthorityError, overlays.PreparationError, OSError) as error:
        print(f"lock-authority: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
