#!/usr/bin/env python3
"""Validate and materialize deterministic current correctness-child inputs.

This tool is deliberately build-neutral: it binds exact source bytes and a
reviewed cfg(test)-only product overlay into a fresh construction directory.
It never invokes Cargo, rustc, a child, or the measurement runner.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from validate_product_test_overlay import (
    ValidationError as ProductOverlayValidationError,
    validate_patch as validate_product_overlay_patch,
)


PROTOCOL = "bn-2l3n-asterism-rebaseline-v3"
SCHEMA = "bn-2k0f-current-child-construction-v1"
CORRECTNESS_DESTINATION = (
    "crates/mess-store/examples/asterism_rebaseline_current_correctness.rs"
)
SHARED_DESTINATION = "crates/mess-store/examples/asterism_rebaseline_shared"
PRODUCT_OVERLAY_DESTINATION = "product-test-overlay.patch"
PRODUCT_OVERLAY = Path(__file__).resolve().parent / PRODUCT_OVERLAY_DESTINATION
EXACT_PRODUCT_COMMIT = "d644dc583dfe6a3d2cd07e71ce0212a323875ab4"
EXACT_PRODUCT_TREE = "205d853905bdb648ee997900c6aef24a323aa380"
EXACT_PRODUCT_OVERLAY_REVIEWED_COMMIT = (
    "86027c98605d9ea01c3e385b0702741723d5a538"
)
EXACT_PRODUCT_OVERLAY_SHA256 = (
    "db060c902d7d1a2664dcaea44525adac727b1bef1de32bb01b33be2561143a39"
)
EXACT_CASES = (
    ("public-ordinary-append-command-cache-read-subscribe", "correctness"),
    ("same-stream-exact-race", "correctness"),
    ("registry-first-use-ordered-failure-unit", "correctness"),
    ("error-ordering", "correctness"),
    ("borrowed-owned-mixed-order-and-type", "correctness"),
    ("two-live-rolls", "roll-recovery"),
    ("clean-repeated-active-tail-sealed-recovery", "roll-recovery"),
)
SHARED_NAMES = (
    "allocation.rs",
    "control.rs",
    "digest.rs",
    "schema.rs",
    "semantic_oracle.rs",
)


class PreparationError(RuntimeError):
    """The source authority or deterministic construction differs."""


@dataclass(frozen=True)
class FileIdentity:
    """Fields that must remain stable around and after a descriptor read."""

    device: int
    inode: int
    file_type: int
    permissions: int
    link_count: int
    size: int
    mtime_ns: int
    ctime_ns: int

    @classmethod
    def from_stat(cls, value: os.stat_result) -> FileIdentity:
        return cls(
            device=value.st_dev,
            inode=value.st_ino,
            file_type=stat.S_IFMT(value.st_mode),
            permissions=stat.S_IMODE(value.st_mode),
            link_count=value.st_nlink,
            size=value.st_size,
            mtime_ns=value.st_mtime_ns,
            ctime_ns=value.st_ctime_ns,
        )


@dataclass(frozen=True)
class SourceSnapshot:
    """One immutable read used for validation, hashing, and copying."""

    name: str
    path: Path
    destination: str | None
    payload: bytes
    identity: FileIdentity

    @property
    def sha256(self) -> str:
        return sha256_bytes(self.payload)


HERE = Path(__file__).resolve().parent
TOOLING = HERE.parent
SHARED = TOOLING / "overlay" / "shared"
PUBLIC_MAIN = TOOLING / "overlay" / "public" / "main.rs"
CORRECTNESS = HERE / "correctness.rs"


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode()


def validate_identity(identity: FileIdentity, context: str) -> None:
    if identity.file_type != stat.S_IFREG:
        raise PreparationError(f"{context} must be a regular file")
    if identity.link_count != 1:
        raise PreparationError(f"{context} must have exactly one hard link")
    if identity.size < 0:
        raise PreparationError(f"{context} has an invalid size")


def read_descriptor_exact(descriptor: int, size: int, context: str) -> bytes:
    chunks: list[bytes] = []
    offset = 0
    while offset < size:
        chunk = os.pread(descriptor, min(1024 * 1024, size - offset), offset)
        if not chunk:
            raise PreparationError(f"{context} shortened during snapshot")
        chunks.append(chunk)
        offset += len(chunk)
    if os.pread(descriptor, 1, size):
        raise PreparationError(f"{context} grew during snapshot")
    return b"".join(chunks)


def snapshot_source(
    name: str,
    source: Path,
    destination: str | None,
    *,
    expected_path: Path | None = None,
) -> SourceSnapshot:
    if not source.is_absolute():
        raise PreparationError(f"source path must be absolute: {source}")
    try:
        resolved_path = source.resolve(strict=True)
    except OSError as error:
        raise PreparationError(f"source is unavailable: {source}") from error
    if source != resolved_path:
        raise PreparationError(f"source path contains a symlink or alias: {source}")
    if expected_path is not None and resolved_path != expected_path:
        raise PreparationError(f"source path differs from reviewed input: {source}")

    descriptor = os.open(
        resolved_path,
        os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
    )
    try:
        before = FileIdentity.from_stat(os.fstat(descriptor))
        validate_identity(before, name)
        payload = read_descriptor_exact(descriptor, before.size, name)
        after = FileIdentity.from_stat(os.fstat(descriptor))
    finally:
        os.close(descriptor)
    if before != after:
        raise PreparationError(f"{name} changed during descriptor snapshot")
    if len(payload) != before.size:
        raise PreparationError(f"{name} snapshot size differs")
    path_identity = FileIdentity.from_stat(os.stat(resolved_path, follow_symlinks=False))
    if path_identity != before:
        raise PreparationError(f"{name} path changed during descriptor snapshot")
    if source.resolve(strict=True) != resolved_path:
        raise PreparationError(f"{name} resolved path changed during snapshot")
    return SourceSnapshot(name, resolved_path, destination, payload, before)


def capture_sources(product_overlay: Path) -> tuple[SourceSnapshot, ...]:
    if product_overlay.name != PRODUCT_OVERLAY_DESTINATION:
        raise PreparationError(
            "product test overlay basename must be product-test-overlay.patch"
        )
    snapshots = [
        snapshot_source(
            "correctness.rs",
            CORRECTNESS,
            CORRECTNESS_DESTINATION,
        )
    ]
    snapshots.extend(
        snapshot_source(
            name,
            SHARED / name,
            f"{SHARED_DESTINATION}/{name}",
        )
        for name in SHARED_NAMES
    )
    snapshots.extend(
        (
            snapshot_source(
                "public/main.rs",
                PUBLIC_MAIN,
                None,
            ),
            snapshot_source(
                PRODUCT_OVERLAY_DESTINATION,
                product_overlay,
                PRODUCT_OVERLAY_DESTINATION,
                expected_path=PRODUCT_OVERLAY,
            ),
        )
    )
    result = tuple(snapshots)
    validate_snapshot_set(result)
    return result


def validate_snapshot_set(snapshots: tuple[SourceSnapshot, ...]) -> None:
    if len(snapshots) != len(SHARED_NAMES) + 3:
        raise PreparationError("source snapshot cardinality differs")
    names: set[str] = set()
    paths: set[Path] = set()
    file_identities: set[tuple[int, int]] = set()
    destinations: set[str] = set()
    for snapshot in snapshots:
        validate_identity(snapshot.identity, snapshot.name)
        if len(snapshot.payload) != snapshot.identity.size:
            raise PreparationError(f"{snapshot.name} frozen size differs")
        if snapshot.name in names:
            raise PreparationError(f"duplicate source name: {snapshot.name}")
        names.add(snapshot.name)
        if snapshot.path in paths:
            raise PreparationError(f"duplicate source path: {snapshot.path}")
        paths.add(snapshot.path)
        file_identity = (snapshot.identity.device, snapshot.identity.inode)
        if file_identity in file_identities:
            raise PreparationError(f"duplicate source file identity: {snapshot.name}")
        file_identities.add(file_identity)
        if snapshot.destination is None:
            continue
        destination = Path(snapshot.destination)
        if destination.is_absolute() or ".." in destination.parts:
            raise PreparationError(
                f"source destination escapes construction: {snapshot.destination}"
            )
        if snapshot.destination in destinations:
            raise PreparationError(
                f"duplicate source destination: {snapshot.destination}"
            )
        destinations.add(snapshot.destination)


def snapshot_named(
    snapshots: tuple[SourceSnapshot, ...], name: str
) -> SourceSnapshot:
    matches = tuple(snapshot for snapshot in snapshots if snapshot.name == name)
    if len(matches) != 1:
        raise PreparationError(f"source snapshot identity differs: {name}")
    return matches[0]


def decode_snapshot(snapshot: SourceSnapshot) -> str:
    try:
        return snapshot.payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise PreparationError(f"source is not UTF-8: {snapshot.name}") from error


def require_identity_matches(
    snapshot: SourceSnapshot, current: FileIdentity
) -> None:
    if current != snapshot.identity:
        raise PreparationError(
            f"source identity changed before publication: {snapshot.name}"
        )


def revalidate_source(snapshot: SourceSnapshot) -> None:
    try:
        if snapshot.path.resolve(strict=True) != snapshot.path:
            raise PreparationError(
                f"source resolved path changed before publication: {snapshot.name}"
            )
        current = FileIdentity.from_stat(
            os.stat(snapshot.path, follow_symlinks=False)
        )
    except OSError as error:
        raise PreparationError(
            f"source disappeared before publication: {snapshot.name}"
        ) from error
    require_identity_matches(snapshot, current)


def revalidate_sources(snapshots: tuple[SourceSnapshot, ...]) -> None:
    for snapshot in snapshots:
        revalidate_source(snapshot)


def require_order(source: str, tokens: tuple[str, ...], context: str) -> None:
    offset = 0
    for token in tokens:
        found = source.find(token, offset)
        if found < 0:
            raise PreparationError(f"{context} lacks ordered token: {token}")
        offset = found + len(token)


def extract_between(source: str, start: str, end: str, context: str) -> str:
    left = source.find(start)
    if left < 0:
        raise PreparationError(f"{context} lacks start marker")
    right = source.find(end, left + len(start))
    if right < 0:
        raise PreparationError(f"{context} lacks end marker")
    return source[left:right]


def validate_semantic_oracle(source: str) -> None:
    require_order(
        source,
        (
            "pub async fn run_generation_neutral_semantic_oracle(",
            "store: &EventStore<FjallSnapshotBackend<LogEngine>>",
            ".append(",
            ".load::<OracleAggregate>(",
            ".subscribe(None)",
            "SemanticOracleObservations {",
            "domain_events:  4",
            "fresh_streams:  2",
            "public_appends: 3",
        ),
        "shared semantic oracle",
    )
    if ".append_batch(" in source or ".append_batch_owned(" in source:
        raise PreparationError(
            "shared semantic oracle bypasses the EventStore public composition"
        )
    if source.count("run_generation_neutral_semantic_oracle") != 1:
        raise PreparationError("shared semantic oracle function is duplicated")
    if source.count(
        'const EVENT_NAME: &str = "asterism.rebaseline.event";'
    ) != 1:
        raise PreparationError("shared semantic oracle event identity differs")
    if source.count(
        'fn name(&self) -> &\'static str { "asterism.rebaseline.rejected" }'
    ) != 1:
        raise PreparationError("shared semantic oracle rejected-event identity differs")
    for forbidden in (
        ".command::<",
        ".command_cached::<",
        ".load_cached::<",
        ".load_hot::<",
        ".with_cache_capacity(",
        '"oracle-command"',
        '"oracle-cache"',
    ):
        if forbidden in source:
            raise PreparationError(
                f"shared semantic oracle contains A-only behavior: {forbidden}"
            )
    require_order(
        source,
        (
            'b"common-oracle/alpha/0"',
            'b"common-oracle/alpha/1"',
            'b"common-oracle/beta/0"',
            'b"common-oracle/alpha/2"',
        ),
        "shared semantic oracle payload identity",
    )
    if source.count('b"common-oracle/') != 4:
        raise PreparationError("shared semantic oracle payload cardinality differs")
    if source.count(".append(") != 5:
        raise PreparationError("shared semantic oracle append cardinality differs")
    if source.count(".load::<OracleAggregate>(") != 3:
        raise PreparationError("shared semantic oracle load cardinality differs")
    if source.count(".subscribe(None)") != 1:
        raise PreparationError("shared semantic oracle subscription differs")
    compact = normalized_rust(source)
    for phrase in (
        "common oracle rejection",
        "records .windows(2)",
        "expected: Version::At(0)",
        "actual: Version::At(2)",
    ):
        if phrase not in compact:
            raise PreparationError(f"shared semantic oracle lacks {phrase!r}")


def validate_public_main(source: str) -> None:
    compact = normalized_rust(source)
    require_order(
        compact,
        (
            '#[path = "asterism_rebaseline_shared/semantic_oracle.rs"]',
            "mod semantic_oracle;",
            "fn run_common_public_oracle(root: PathBuf)",
            "let engine = LogEngine::open_with(",
            "let backend =",
            "FjallSnapshotBackend::open(engine.clone()",
            "let store = EventStore::new(backend)",
            ".with_page_size(16)",
            ";",
            "semantic_oracle::run_generation_neutral_semantic_oracle(&store)",
            "adapter::assert_oracle_accounting(",
            "observations.domain_events",
            "observations.public_appends",
            "observations.fresh_streams",
        ),
        "public oracle composition",
    )
    if source.count("run_generation_neutral_semantic_oracle(&store)") != 1:
        raise PreparationError("public main does not call the shared oracle exactly once")
    body = extract_between(
        source,
        "fn run_common_public_oracle(root: PathBuf)",
        "fn emit_correctness_oracle(",
        "public oracle body",
    )
    if "common-oracle/alpha/0" in body or "oracle alpha initial append" in body:
        raise PreparationError("public main retained an inline semantic oracle copy")
    if ".with_cache_capacity(" in body:
        raise PreparationError("historical public oracle enabled an A-only cache")
    if body.count("EventStore::new(backend).with_page_size(16)") != 1:
        raise PreparationError("historical public oracle store configuration differs")
    for exact_output in (
        '("classification", json_string("historical-oracle"))',
        '("id", json_string("public-common-oracle"))',
        '("suite", json_string("common-public-oracle"))',
    ):
        if source.count(exact_output) != 1:
            raise PreparationError(
                f"historical public oracle output identity differs: {exact_output}"
            )


def normalized_rust(source: str) -> str:
    return " ".join(source.split())


def validate_correctness(source: str, *, allow_pending_hook: bool = True) -> None:
    cases_body = extract_between(
        source, "const CASES:", "];", "current correctness case table"
    )
    observed_cases = tuple(
        re.findall(
            r'\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*,?\s*\)',
            cases_body,
        )
    )
    if observed_cases != EXACT_CASES:
        raise PreparationError("current correctness exact case IDs/order differ")

    require_order(
        source,
        (
            "let mut control = Control::connect();",
            "let boot_nonce = control.boot();",
            "let runtime_nonce = control.runtime(&boot_nonce);",
            "let public_engine = LogEngine::open_with(",
            "let public_backend = FjallSnapshotBackend::open(",
            "let public_store = EventStore::new(public_backend)",
            "let opened_nonce = control.opened(&runtime_nonce);",
            "let start_nonce = control.ready_and_wait_start(",
            "case_public_composition(&public_store, &public_engine).await;",
            "case_same_stream_exact_race(&public_store).await;",
            "case_registry_first_use_ordered_failure_unit(",
            "case_error_ordering(",
            "case_borrowed_owned_mixed_order_and_type(",
            "case_two_live_rolls(",
            "case_clean_repeated_active_tail_sealed_recovery(",
            "control.measured_and_wait_release(",
            "drop(public_store);",
            "drop(public_engine);",
            "runtime.shutdown_timeout(",
            "Invocation::Correctness(args) => emit_result(&args),",
            "Invocation::Smoke => emit_smoke_result(),",
        ),
        "current correctness control/case/output sequence",
    )
    if source.count("control.measured_and_wait_release(") != 1:
        raise PreparationError("current correctness measured transition differs")
    if source.count("Invocation::Correctness(args) => emit_result(&args),") != 1:
        raise PreparationError("current correctness output cardinality differs")
    require_order(
        source,
        (
            'assert_eq!(arguments[0], "--correctness");',
            'assert_eq!(arguments[1], "--protocol");',
            'assert_eq!(arguments[3], "--attempt-nonce");',
            'assert_eq!(arguments[5], "--variant");',
            'assert_eq!(arguments[7], "--phase");',
            'assert_eq!(arguments[9], "--suite");',
            'assert_eq!(arguments[10], "current-product");',
        ),
        "current correctness argv",
    )
    expected_binding_counts = {
        "ASTERISM_REBASELINE_MODE": 2,
        "ASTERISM_REBASELINE_ATTEMPT_NONCE": 1,
        "ASTERISM_REBASELINE_PHASE": 1,
        "ASTERISM_REBASELINE_PROTOCOL": 2,
        "ASTERISM_REBASELINE_SUITE": 1,
        "ASTERISM_REBASELINE_VARIANT": 1,
    }
    for binding, expected_count in expected_binding_counts.items():
        if source.count(f'required("{binding}")') != expected_count:
            raise PreparationError(f"current correctness env binding differs: {binding}")
    public_case = extract_between(
        source,
        "async fn case_public_composition(",
        "async fn case_current_command_cache_extension(",
        "current public composition case",
    )
    if public_case.count("run_generation_neutral_semantic_oracle(store).await") != 1:
        raise PreparationError("A case 1 does not call the exact shared oracle once")
    if ".append_batch(" in public_case or ".append_batch_owned(" in public_case:
        raise PreparationError("A case 1 bypasses the public EventStore composition")
    require_order(
        public_case,
        (
            "run_generation_neutral_semantic_oracle(store).await",
            "domain_events:  4",
            "fresh_streams:  2",
            "public_appends: 3",
            "let current_store = EventStore::new(store.backend().clone())",
            ".with_page_size(16)",
            ".with_cache_capacity(16);",
            "case_current_command_cache_extension(&current_store).await",
            "current.domain_events + current.fresh_streams + 1",
            "current.public_appends + current.fresh_streams",
        ),
        "A shared-then-current case 1 composition",
    )
    if source.count(".with_cache_capacity(16)") != 1:
        raise PreparationError("A-only cache construction cardinality differs")
    current_extension = extract_between(
        source,
        "async fn case_current_command_cache_extension(",
        "async fn case_same_stream_exact_race(",
        "A-only command/cache extension",
    )
    require_order(
        current_extension,
        (
            "store.cache().is_enabled()",
            ".command::<CurrentAggregate, _>(",
            ".command_cached::<CurrentAggregate, _>(",
            ".load::<CurrentAggregate>(",
            ".load_hot::<CurrentAggregate>(",
            "store.cache().get::<CurrentAggregate>(",
            '.head("current-command")',
            '.head("current-cache")',
            ".read_global(None, 16)",
            "store.await_past(last_cursor)",
            "store.watermark()",
            "store.subscribe(Some(command_cursor))",
            "domain_events:  3",
            "fresh_streams:  2",
            "public_appends: 3",
        ),
        "A-only command/cache extension",
    )
    if current_extension.count(".command_cached::<CurrentAggregate, _>(") != 2:
        raise PreparationError("A-only cached-command cardinality differs")

    pending = "HOOK_CONTRACT_PENDING" in source
    if pending:
        if not allow_pending_hook:
            raise PreparationError("registry hook contract remains pending")
        hook_case = extract_between(
            source,
            "async fn case_registry_first_use_ordered_failure_unit(",
            "async fn case_error_ordering(",
            "pending registry hook case",
        )
        if "panic!(" not in hook_case:
            raise PreparationError("pending registry hook case could claim PASS")
    elif "registry ordered-failure hook contract is not integrated" in source:
        raise PreparationError("registry hook pending marker was partially removed")
    else:
        hook_case = extract_between(
            source,
            "async fn case_registry_first_use_ordered_failure_unit(",
            "async fn case_error_ordering(",
            "registry ordered-failure case",
        )
        require_order(
            hook_case,
            (
                "let engine = LogEngine::open(root)",
                '"registry-hot-predecessor"',
                "let cohort = engine.arm_test_owner_cohort(2);",
                "TestEngineHook::PwriteEio {",
                "after_successful_pwrites: 1",
                "cohort.wait_until_admitted(1);",
                '"registry-fresh-after-predecessor"',
                "cohort.wait_until_admitted(2);",
                "write_fault.wait_until_reached();",
                "write_fault.release();",
                "hot predecessor pwrite must succeed",
                "failed registry unit must not publish a stream id",
                "reopen after registry-unit EIO",
                "fresh stream retry must register and commit",
                "second registry-failure reopen",
            ),
            "registry ordered-failure hook binding",
        )
        if "TestEngineHook::PwriteEio" not in source:
            raise PreparationError("registry ordered-failure hook type is absent")

    result = extract_between(source, "fn emit_result(", "fn main()", "result emitter")
    require_order(
        result,
        (
            '("attempt_nonce", json_string(&args.attempt_nonce))',
            '("boundedness", "null".to_owned())',
            '("cases", cases_json())',
            '("harness_sound", json_bool(true))',
            '("phase", json_string(&args.phase))',
            '("protocol", json_string(contract::PROTOCOL))',
            '("schema", json_string("bn-2l3n-correctness-child-v3"))',
            '("suite", json_string("current-product"))',
            '("variant", json_string(contract::VARIANT))',
        ),
        "canonical correctness output fields",
    )


def validate_sources(
    snapshots: tuple[SourceSnapshot, ...],
    *,
    allow_pending_hook: bool = True,
) -> None:
    validate_snapshot_set(snapshots)
    validate_semantic_oracle(
        decode_snapshot(snapshot_named(snapshots, "semantic_oracle.rs"))
    )
    validate_public_main(decode_snapshot(snapshot_named(snapshots, "public/main.rs")))
    validate_correctness(
        decode_snapshot(snapshot_named(snapshots, "correctness.rs")),
        allow_pending_hook=allow_pending_hook,
    )


def validate_product_authority_values(
    product_commit: str,
    product_tree: str,
    overlay_payload: bytes,
) -> list[str]:
    if product_commit != EXACT_PRODUCT_COMMIT:
        raise PreparationError("current A product commit differs")
    if product_tree != EXACT_PRODUCT_TREE:
        raise PreparationError("current A product tree differs")
    if sha256_bytes(overlay_payload) != EXACT_PRODUCT_OVERLAY_SHA256:
        raise PreparationError("current product test overlay hash differs")
    try:
        overlay_text = overlay_payload.decode()
        checks = validate_product_overlay_patch(overlay_text)
    except (UnicodeDecodeError, ProductOverlayValidationError) as error:
        raise PreparationError("current product test overlay authority failed") from error
    if len(checks) != 11 or len(set(checks)) != 11:
        raise PreparationError("current product test overlay check set differs")
    return checks


def validate_product_authority_snapshot(
    product_commit: str,
    product_tree: str,
    product_overlay: SourceSnapshot,
) -> list[str]:
    if (
        product_overlay.name != PRODUCT_OVERLAY_DESTINATION
        or product_overlay.destination != PRODUCT_OVERLAY_DESTINATION
    ):
        raise PreparationError(
            "product test overlay snapshot identity differs"
        )
    if product_overlay.path != PRODUCT_OVERLAY:
        raise PreparationError("product test overlay path differs from reviewed input")
    return validate_product_authority_values(
        product_commit,
        product_tree,
        product_overlay.payload,
    )


def input_record(source: SourceSnapshot) -> dict[str, Any]:
    if source.destination is None:
        raise PreparationError(f"validation-only source cannot be copied: {source.name}")
    return {
        "destination": source.destination,
        "mode": 0o444,
        "sha256": source.sha256,
        "size": len(source.payload),
        "source": source.name,
    }


def construction_manifest(
    product_commit: str,
    product_tree: str,
    snapshots: tuple[SourceSnapshot, ...],
    *,
    allow_pending_hook: bool = False,
) -> dict[str, Any]:
    validate_sources(snapshots, allow_pending_hook=allow_pending_hook)
    product_overlay = snapshot_named(snapshots, PRODUCT_OVERLAY_DESTINATION)
    overlay_checks = validate_product_authority_snapshot(
        product_commit, product_tree, product_overlay
    )
    inputs = [input_record(snapshot) for snapshot in snapshots if snapshot.destination]
    inputs.sort(key=lambda item: item["destination"])
    overlay_input = next(
        item for item in inputs if item["destination"] == PRODUCT_OVERLAY_DESTINATION
    )
    if overlay_input["sha256"] != EXACT_PRODUCT_OVERLAY_SHA256:
        raise PreparationError("overlay authority does not match copied input")
    return {
        "build_contract": {
            "cargo_locked": True,
            "cargo_offline": True,
            "correctness_only": True,
            "rustc_cfg": ["test"],
            "target_comm": "ast-rb-check",
        },
        "inputs": inputs,
        "product_commit": product_commit,
        "product_test_overlay_authority": {
            "checks": overlay_checks,
            "reviewed_commit": EXACT_PRODUCT_OVERLAY_REVIEWED_COMMIT,
            "sha256": EXACT_PRODUCT_OVERLAY_SHA256,
        },
        "product_tree": product_tree,
        "protocol": PROTOCOL,
        "schema": SCHEMA,
    }


def write_new(path: Path, payload: bytes, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        raise
    os.chmod(path, mode)


def validate_destination(
    path: Path,
    expected_payload: bytes,
    seen_identities: set[tuple[int, int]],
    source_identities: set[tuple[int, int]],
) -> SourceSnapshot:
    destination = snapshot_source(
        f"construction/{path.name}",
        path,
        None,
        expected_path=path,
    )
    if destination.identity.permissions != 0o444:
        raise PreparationError(f"construction destination mode differs: {path}")
    if destination.payload != expected_payload:
        raise PreparationError(f"copied input differs: {path}")
    if destination.identity.size != len(expected_payload):
        raise PreparationError(f"construction destination size differs: {path}")
    file_identity = (destination.identity.device, destination.identity.inode)
    if file_identity in source_identities:
        raise PreparationError(f"construction destination aliases a source: {path}")
    if file_identity in seen_identities:
        raise PreparationError(f"construction destination identity is reused: {path}")
    seen_identities.add(file_identity)
    return destination


def prepare(args: argparse.Namespace) -> None:
    output = args.output.resolve()
    if output.exists() or output.is_symlink():
        raise PreparationError("construction output must be absent")
    snapshots = capture_sources(args.product_overlay)
    manifest = construction_manifest(
        args.product_commit,
        args.product_tree,
        snapshots,
    )
    sources = {
        snapshot.name: snapshot
        for snapshot in snapshots
        if snapshot.destination is not None
    }
    output.mkdir(parents=True, mode=0o755)
    source_identities = {
        (snapshot.identity.device, snapshot.identity.inode)
        for snapshot in snapshots
    }
    destination_identities: set[tuple[int, int]] = set()
    destination_snapshots: list[SourceSnapshot] = []
    for item in manifest["inputs"]:
        source = sources[item["source"]]
        destination = output / item["destination"]
        write_new(destination, source.payload, 0o444)
        destination_snapshots.append(
            validate_destination(
                destination,
                source.payload,
                destination_identities,
                source_identities,
            )
        )
    revalidate_sources(snapshots)
    revalidate_sources(tuple(destination_snapshots))
    manifest_bytes = canonical_bytes(manifest)
    write_new(output / "construction.json", manifest_bytes, 0o444)
    print(manifest_bytes.decode(), end="")


def mutate_once(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise AssertionError(f"hostile source token is not unique: {old}")
    return source.replace(old, new, 1)


def expect_rejected(validator: Any, source: str, old: str, new: str) -> None:
    mutated = mutate_once(source, old, new)
    try:
        validator(mutated)
    except PreparationError:
        return
    raise AssertionError(f"hostile mutation was accepted: {old} -> {new}")


def expect_preparation_error(action: Any, context: str) -> None:
    try:
        action()
    except PreparationError:
        return
    raise AssertionError(f"hostile construction was accepted: {context}")


def self_test() -> None:
    snapshots = capture_sources(PRODUCT_OVERLAY)
    manifest = construction_manifest(
        EXACT_PRODUCT_COMMIT,
        EXACT_PRODUCT_TREE,
        snapshots,
        allow_pending_hook=True,
    )
    overlay_snapshot = snapshot_named(snapshots, PRODUCT_OVERLAY_DESTINATION)
    overlay_payload = overlay_snapshot.payload
    if overlay_snapshot.sha256 != EXACT_PRODUCT_OVERLAY_SHA256:
        raise AssertionError("reviewed overlay snapshot hash differs")
    overlay_input = next(
        item
        for item in manifest["inputs"]
        if item["destination"] == PRODUCT_OVERLAY_DESTINATION
    )
    if overlay_input["sha256"] != EXACT_PRODUCT_OVERLAY_SHA256:
        raise AssertionError("reviewed overlay authority and copied input differ")
    semantic = decode_snapshot(snapshot_named(snapshots, "semantic_oracle.rs"))
    public = decode_snapshot(snapshot_named(snapshots, "public/main.rs"))
    correctness = decode_snapshot(snapshot_named(snapshots, "correctness.rs"))

    expect_rejected(
        validate_semantic_oracle,
        semantic,
        '.append(\n            "oracle-alpha",\n            Version::NoStream,',
        '.append_batch(\n            "oracle-alpha",\n            Version::NoStream,',
    )
    expect_rejected(
        validate_semantic_oracle,
        semantic,
        "domain_events:  4,",
        "domain_events:  6,",
    )
    expect_rejected(
        validate_semantic_oracle,
        semantic,
        "fresh_streams:  2,",
        "fresh_streams:  4,",
    )
    expect_rejected(
        validate_semantic_oracle,
        semantic,
        "public_appends: 3,",
        "public_appends: 5,",
    )
    expect_rejected(
        validate_semantic_oracle,
        semantic,
        'const EVENT_NAME: &str = "asterism.rebaseline.event";',
        'const EVENT_NAME: &str = "asterism.rebaseline.oracle-event";',
    )
    expect_rejected(
        validate_semantic_oracle,
        semantic,
        'fn name(&self) -> &\'static str { "asterism.rebaseline.rejected" }',
        'fn name(&self) -> &\'static str { "asterism.rebaseline.oracle-rejected" }',
    )
    expect_rejected(
        validate_semantic_oracle,
        semantic,
        '.load::<OracleAggregate>("oracle-alpha")',
        '.command::<OracleAggregate, _>("oracle-alpha")',
    )
    expect_rejected(
        validate_semantic_oracle,
        semantic,
        "    let payloads = [",
        "    let _cache = store.clone().with_cache_capacity(16);\n    let payloads = [",
    )
    expect_rejected(
        validate_public_main,
        public,
        "semantic_oracle::run_generation_neutral_semantic_oracle(&store)",
        "semantic_oracle::run_generation_neutral_semantic_oracle(&group_store)",
    )
    expect_rejected(
        validate_public_main,
        public,
        "EventStore::new(backend).with_page_size(16)",
        "EventStore::new(group_backend).with_page_size(16)",
    )
    expect_rejected(
        validate_public_main,
        public,
        "EventStore::new(backend).with_page_size(16)",
        "EventStore::new(backend).with_page_size(16).with_cache_capacity(16)",
    )
    expect_rejected(
        validate_public_main,
        public,
        "EventStore::new(backend).with_page_size(16)",
        "EventStore::new(backend).with_page_size(32)",
    )
    expect_rejected(
        validate_public_main,
        public,
        '("id", json_string("public-common-oracle"))',
        '("id", json_string("public-expanded-oracle"))',
    )
    expect_rejected(
        validate_correctness,
        correctness,
        '("same-stream-exact-race", "correctness")',
        '("error-ordering", "correctness")',
    )
    expect_rejected(
        validate_correctness,
        correctness,
        "let boot_nonce = control.boot();",
        "let boot_nonce = control.runtime(&boot_nonce);",
    )
    expect_rejected(
        validate_correctness,
        correctness,
        "case_public_composition(&public_store, &public_engine).await;",
        "case_same_stream_exact_race(&public_store).await;",
    )
    expect_rejected(
        validate_correctness,
        correctness,
        "control.measured_and_wait_release(",
        (
            "Invocation::Correctness(args) => emit_result(&args),\n"
            "    control.measured_and_wait_release("
        ),
    )
    expect_rejected(
        validate_correctness,
        correctness,
        "run_generation_neutral_semantic_oracle(store).await",
        "run_generation_neutral_semantic_oracle(&EventStore::new(engine.clone())).await",
    )
    expect_rejected(
        validate_correctness,
        correctness,
        "case_current_command_cache_extension(&current_store).await",
        "CurrentExtensionObservations { domain_events: 3, fresh_streams: 2, public_appends: 3 }",
    )
    expect_rejected(
        validate_correctness,
        correctness,
        '.load_hot::<CurrentAggregate>("current-cache")',
        '.load_cached::<CurrentAggregate>("current-cache")',
    )
    expect_rejected(
        validate_correctness,
        correctness,
        '.command_cached::<CurrentAggregate, _>(\n            "current-cache",\n            CurrentCommand(7),',
        '.command::<CurrentAggregate, _>(\n            "current-cache",\n            CurrentCommand(7),',
    )
    expect_rejected(
        validate_correctness,
        correctness,
        "after_successful_pwrites: 1,",
        "after_successful_pwrites: 0,",
    )
    expect_rejected(
        validate_correctness,
        correctness,
        "cohort.wait_until_admitted(1);",
        "cohort.wait_until_admitted(2);",
    )
    expect_rejected(
        validate_correctness,
        correctness,
        '("boundedness", "null".to_owned()),',
        '("boundedness", cases_json()),',
    )
    for product_commit, product_tree, payload in (
        ("0" * 40, EXACT_PRODUCT_TREE, overlay_payload),
        (EXACT_PRODUCT_COMMIT, "0" * 40, overlay_payload),
        (
            EXACT_PRODUCT_COMMIT,
            EXACT_PRODUCT_TREE,
            overlay_payload.replace(b"PwriteEio", b"PwriteGone", 1),
        ),
    ):
        try:
            validate_product_authority_values(
                product_commit, product_tree, payload
            )
        except PreparationError:
            continue
        raise AssertionError("hostile product/overlay authority was accepted")

    mutated_overlay = replace(
        overlay_snapshot,
        payload=overlay_payload.replace(b"PwriteEio", b"PwriteBad", 1),
    )
    mutated_snapshots = tuple(
        mutated_overlay if snapshot is overlay_snapshot else snapshot
        for snapshot in snapshots
    )
    expect_preparation_error(
        lambda: construction_manifest(
            EXACT_PRODUCT_COMMIT,
            EXACT_PRODUCT_TREE,
            mutated_snapshots,
            allow_pending_hook=True,
        ),
        "overlay changed between validation and manifest construction",
    )
    expect_preparation_error(
        lambda: require_identity_matches(
            overlay_snapshot,
            replace(
                overlay_snapshot.identity,
                ctime_ns=overlay_snapshot.identity.ctime_ns + 1,
            ),
        ),
        "source identity changed before publication",
    )
    destination_snapshot = replace(
        overlay_snapshot,
        name="construction/product-test-overlay.patch",
    )
    expect_preparation_error(
        lambda: require_identity_matches(
            destination_snapshot,
            replace(
                destination_snapshot.identity,
                inode=destination_snapshot.identity.inode + 1,
            ),
        ),
        "destination identity changed before publication",
    )
    linked_overlay = replace(
        overlay_snapshot,
        identity=replace(overlay_snapshot.identity, link_count=2),
    )
    expect_preparation_error(
        lambda: validate_snapshot_set(
            tuple(
                linked_overlay if snapshot is overlay_snapshot else snapshot
                for snapshot in snapshots
            )
        ),
        "hard-linked source",
    )
    public_snapshot = snapshot_named(snapshots, "public/main.rs")
    duplicate_inode_public = replace(
        public_snapshot,
        identity=replace(
            public_snapshot.identity,
            device=overlay_snapshot.identity.device,
            inode=overlay_snapshot.identity.inode,
        ),
    )
    expect_preparation_error(
        lambda: validate_snapshot_set(
            tuple(
                duplicate_inode_public if snapshot is public_snapshot else snapshot
                for snapshot in snapshots
            )
        ),
        "duplicate source file identity",
    )
    semantic_snapshot = snapshot_named(snapshots, "semantic_oracle.rs")
    duplicate_destination_semantic = replace(
        semantic_snapshot,
        destination=CORRECTNESS_DESTINATION,
    )
    expect_preparation_error(
        lambda: validate_snapshot_set(
            tuple(
                duplicate_destination_semantic
                if snapshot is semantic_snapshot
                else snapshot
                for snapshot in snapshots
            )
        ),
        "duplicate construction destination",
    )
    duplicate_path_public = replace(public_snapshot, path=overlay_snapshot.path)
    expect_preparation_error(
        lambda: validate_snapshot_set(
            tuple(
                duplicate_path_public if snapshot is public_snapshot else snapshot
                for snapshot in snapshots
            )
        ),
        "duplicate source path alias",
    )
    expect_preparation_error(
        lambda: capture_sources(
            Path(os.path.relpath(PRODUCT_OVERLAY, Path.cwd()))
        ),
        "relative reviewed overlay path",
    )
    expect_preparation_error(
        lambda: capture_sources(
            PRODUCT_OVERLAY.parent
            / ".."
            / PRODUCT_OVERLAY.parent.name
            / PRODUCT_OVERLAY.name
        ),
        "lexical parent-directory overlay alias",
    )
    print(
        canonical_bytes(
            {
                "hostile_mutations_rejected": 36,
                "schema": "bn-2k0f-prepare-children-self-test-v1",
                "status": "ok",
            }
        ).decode(),
        end="",
    )


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    subcommands = root.add_subparsers(dest="command", required=True)
    subcommands.add_parser("self-test")
    prepare_parser = subcommands.add_parser("prepare")
    prepare_parser.add_argument("--product-commit", required=True)
    prepare_parser.add_argument("--product-tree", required=True)
    prepare_parser.add_argument("--product-overlay", required=True, type=Path)
    prepare_parser.add_argument("--output", required=True, type=Path)
    return root


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "self-test":
            self_test()
        else:
            prepare(args)
    except (OSError, PreparationError, ValueError) as error:
        print(f"prepare_children: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
