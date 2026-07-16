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
import sys
from pathlib import Path
from typing import Any


PROTOCOL = "bn-2l3n-asterism-rebaseline-v3"
SCHEMA = "bn-2k0f-current-child-construction-v1"
CORRECTNESS_DESTINATION = (
    "crates/mess-store/examples/asterism_rebaseline_current_correctness.rs"
)
SHARED_DESTINATION = "crates/mess-store/examples/asterism_rebaseline_shared"
PRODUCT_OVERLAY_DESTINATION = "product-test-overlay.patch"
LOWER_HEX = frozenset("0123456789abcdef")
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


def exact_hex(value: str, length: int) -> bool:
    return len(value) == length and set(value) <= LOWER_HEX


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
            "emit_result(&args);",
        ),
        "current correctness control/case/output sequence",
    )
    if source.count("control.measured_and_wait_release(") != 1:
        raise PreparationError("current correctness measured transition differs")
    if source.count("emit_result(&args);") != 1:
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
    for binding in (
        "ASTERISM_REBASELINE_MODE",
        "ASTERISM_REBASELINE_ATTEMPT_NONCE",
        "ASTERISM_REBASELINE_PHASE",
        "ASTERISM_REBASELINE_PROTOCOL",
        "ASTERISM_REBASELINE_SUITE",
        "ASTERISM_REBASELINE_VARIANT",
    ):
        if source.count(f'required("{binding}")') != 1:
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


def validate_sources(*, allow_pending_hook: bool = True) -> None:
    validate_semantic_oracle((SHARED / "semantic_oracle.rs").read_text())
    validate_public_main(PUBLIC_MAIN.read_text())
    validate_correctness(CORRECTNESS.read_text(), allow_pending_hook=allow_pending_hook)


def input_record(source: Path, destination: str) -> dict[str, Any]:
    payload = source.read_bytes()
    return {
        "destination": destination,
        "mode": 0o444,
        "sha256": sha256_bytes(payload),
        "size": len(payload),
        "source": source.name,
    }


def construction_manifest(
    product_commit: str,
    product_tree: str,
    product_overlay: Path,
) -> dict[str, Any]:
    if not exact_hex(product_commit, 40) or not exact_hex(product_tree, 40):
        raise PreparationError("product commit/tree must be 40 lowercase hex")
    if product_overlay.name != PRODUCT_OVERLAY_DESTINATION:
        raise PreparationError(
            "product test overlay basename must be product-test-overlay.patch"
        )
    if not product_overlay.is_file() or product_overlay.is_symlink():
        raise PreparationError("product test overlay must be one regular file")
    inputs = [input_record(CORRECTNESS, CORRECTNESS_DESTINATION)]
    inputs.extend(
        input_record(SHARED / name, f"{SHARED_DESTINATION}/{name}")
        for name in SHARED_NAMES
    )
    inputs.append(input_record(product_overlay, PRODUCT_OVERLAY_DESTINATION))
    inputs.sort(key=lambda item: item["destination"])
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


def prepare(args: argparse.Namespace) -> None:
    validate_sources(allow_pending_hook=False)
    output = args.output.resolve()
    if output.exists() or output.is_symlink():
        raise PreparationError("construction output must be absent")
    manifest = construction_manifest(
        args.product_commit, args.product_tree, args.product_overlay.resolve()
    )
    sources = {CORRECTNESS.name: CORRECTNESS}
    sources.update({name: SHARED / name for name in SHARED_NAMES})
    sources[args.product_overlay.name] = args.product_overlay.resolve()
    output.mkdir(parents=True, mode=0o755)
    for item in manifest["inputs"]:
        source = sources[item["source"]]
        payload = source.read_bytes()
        if sha256_bytes(payload) != item["sha256"]:
            raise PreparationError(f"input changed during copy: {source}")
        destination = output / item["destination"]
        write_new(destination, payload, 0o444)
        if sha256_bytes(destination.read_bytes()) != item["sha256"]:
            raise PreparationError(f"copied input differs: {destination}")
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


def self_test() -> None:
    validate_sources(allow_pending_hook=True)
    semantic = (SHARED / "semantic_oracle.rs").read_text()
    public = PUBLIC_MAIN.read_text()
    correctness = CORRECTNESS.read_text()

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
        "emit_result(&args);\n    control.measured_and_wait_release(",
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
    print(
        canonical_bytes(
            {
                "hostile_mutations_rejected": 24,
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
