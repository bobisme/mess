#!/usr/bin/env python3
"""Static authority for the exact A current-fault child.

Normal mode::

    python3 validate_fault.py

Self-test mode (the same source validation plus deterministic hostile-source
mutations)::

    python3 validate_fault.py --self-test

Both modes emit one canonical JSON object with schema
``bn-20be-current-fault-validator-v1``.  This validator never builds or runs
the Rust child.  In particular, it documents and binds the builder contract
for the compile-out case: the builder must set these compile-time variables
for *every* fault-child compilation, and the Rust ``env!`` calls make any
missing value a hard compile error::

    ASTERISM_FAULT_COMPILE_OUT_SCHEMA
    ASTERISM_FAULT_COMPILE_OUT_IDENTICAL
    ASTERISM_FAULT_COMPILE_OUT_PRISTINE_SHA256
    ASTERISM_FAULT_COMPILE_OUT_OVERLAY_RELEASE_SHA256
    ASTERISM_FAULT_COMPILE_OUT_SYMBOL_ABSENCE_SHA256

The schema value is ``bn-2l3n-fault-compile-out-authority-v1``; ``IDENTICAL``
is the literal ``true``; both release-binary SHA-256 values are lowercase and
equal; and the lowercase symbol-absence SHA-256 binds an independently
captured release-symbol scan.  Supplying arbitrary values is not evidence: the
later builder/approval stage owns and hashes the pristine build, overlay
release build, and scan artifact.  This source only consumes that exact
builder authority and refuses to claim PASS without it.
"""

from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path
from typing import Callable, Final


SCHEMA: Final = "bn-20be-current-fault-validator-v1"
SOURCE: Final = Path(__file__).with_name("fault.rs")

CASE_SPECS: Final = (
    ("cancel-before-admission", "cancellation"),
    ("cancel-after-ownership", "cancellation"),
    ("kill-pre-write", "durability"),
    ("kill-partial-write", "durability"),
    ("kill-post-write-pre-barrier", "durability"),
    ("kill-post-barrier-pre-publication", "durability"),
    ("kill-post-publication-pre-completion", "durability"),
    ("short-write", "durability"),
    ("write-error", "durability"),
    ("fdatasync-error", "durability"),
    ("torn-truncated-tail", "roll-recovery"),
    ("invalid-marker-crc", "roll-recovery"),
    ("corrupt-registry-record", "roll-recovery"),
    ("refuted-corrupt-sidecar", "roll-recovery"),
    ("uncertain-persistence-poison", "poison"),
    ("acknowledged-group-survives-reopen", "durability"),
    ("owner-ring-intent-bound", "boundedness"),
    ("group-byte-time-bounds", "boundedness"),
    ("zero-reservations-after-cancel-complete", "boundedness"),
    ("fault-hook-compiles-out-binary-identical", "harness"),
)

BOUNDS: Final = {
    "owner_ring_intents": 1024,
    "group_byte_bound_proven": True,
    "group_time_bound_proven": True,
    "waiter_reservations_after": 0,
    "byte_reservations_after": 0,
}

COMPILE_ENV: Final = (
    "ASTERISM_FAULT_COMPILE_OUT_SCHEMA",
    "ASTERISM_FAULT_COMPILE_OUT_IDENTICAL",
    "ASTERISM_FAULT_COMPILE_OUT_PRISTINE_SHA256",
    "ASTERISM_FAULT_COMPILE_OUT_OVERLAY_RELEASE_SHA256",
    "ASTERISM_FAULT_COMPILE_OUT_SYMBOL_ABSENCE_SHA256",
)

CHECKS: Final = (
    "exact-case-partition-and-order",
    "fault-and-smoke-invocation-contracts",
    "shared-control-lifecycle",
    "pass-after-execution-only",
    "reviewed-hook-and-real-sigkill-semantics",
    "injected-io-and-poison-semantics",
    "on-disk-corruption-and-recovery-semantics",
    "measured-boundedness-object",
    "compile-out-builder-authority",
    "canonical-fault-and-smoke-output",
)


class ValidationError(RuntimeError):
    """The Rust source no longer has the approved fail-closed shape."""


def fail(message: str) -> None:
    raise ValidationError(message)


def require(source: str, token: str, label: str, *, count: int | None = None) -> None:
    observed = source.count(token)
    if observed == 0 or (count is not None and observed != count):
        suffix = "" if count is None else f" (observed {observed}, expected {count})"
        fail(f"{label} differs{suffix}")


def require_order(source: str, tokens: tuple[str, ...], label: str) -> None:
    positions: list[int] = []
    cursor = 0
    for token in tokens:
        position = source.find(token, cursor)
        if position < 0:
            fail(f"{label} omits {token!r}")
        positions.append(position)
        cursor = position + len(token)
    if positions != sorted(positions) or len(set(positions)) != len(positions):
        fail(f"{label} order differs")


def function_block(source: str, name: str) -> str:
    match = re.search(rf"(?m)^(?:async )?fn {re.escape(name)}\b", source)
    if match is None:
        fail(f"function {name} is absent")
    next_match = re.search(r"(?m)^(?:async )?fn [a-zA-Z0-9_]+\b", source[match.end() :])
    end = len(source) if next_match is None else match.end() + next_match.start()
    return source[match.start() : end]


def exact_case_partition(source: str) -> None:
    match = re.search(
        r"const CASE_SPECS: \[CaseSpec; 20\] = \[(.*?)\n\];",
        source,
        re.DOTALL,
    )
    if match is None:
        fail("CASE_SPECS declaration differs")
    observed = tuple(
        re.findall(
            r'CaseSpec\s*\{\s*id:\s*"([^"]+)",\s*classification:\s*"([^"]+)"\s*,?\s*\}',
            match.group(1),
            re.DOTALL,
        )
    )
    if observed != CASE_SPECS:
        fail(f"current-fault partition differs: {observed!r}")
    if len({case_id for case_id, _ in observed}) != len(observed):
        fail("current-fault partition contains duplicate ids")

    run = function_block(source, "run_cases")
    roots = tuple(f'case-{ordinal:02d}-' for ordinal in range(1, 20))
    require_order(run, roots, "executed current-fault cases")
    require_order(
        run,
        (
            "KillPoint::PreWrite",
            "KillPoint::PartialWrite",
            "KillPoint::PostWritePreBarrier",
            "KillPoint::PostBarrierPrePublication",
            "KillPoint::PostPublicationPreCompletion",
        ),
        "real crash phases",
    )
    require(run, "passes.push(case_fault_hook_compiles_out_binary_identical());", "harness case execution", count=1)
    require(run, "assert_eq!(passes.len(), CASE_SPECS.len());", "case cardinality assertion", count=1)
    require(run, "assert_eq!(observed.spec, expected, \"fault case order differs\");", "runtime case order assertion", count=1)
    kill_ids = {
        "kill-pre-write",
        "kill-partial-write",
        "kill-post-write-pre-barrier",
        "kill-post-barrier-pre-publication",
        "kill-post-publication-pre-completion",
    }
    multiline_ids = {"zero-reservations-after-cancel-complete"}
    for case_id, classification in CASE_SPECS:
        if case_id in kill_ids or case_id in multiline_ids:
            continue
        require(
            source,
            f'CasePass::new("{case_id}", "{classification}")',
            f"executed PASS token for {case_id}",
            count=1,
        )
    require(
        function_block(source, "run_kill_case"),
        'CasePass::new(point.id(), "durability")',
        "executed PASS token for the five bound kill points",
        count=1,
    )
    zero = function_block(source, "case_zero_reservations_after_cancel_complete")
    require(zero, '"zero-reservations-after-cancel-complete"', "executed zero-reservation id", count=1)
    require(zero, '"boundedness"', "executed zero-reservation classification", count=1)


def invocation_contracts(source: str) -> None:
    block = function_block(source, "invocation")
    for token in (
        'if arguments == ["--smoke"]',
        'required("ASTERISM_REBASELINE_MODE"), "smoke"',
        'required("ASTERISM_REBASELINE_SMOKE_TARGET"), "fault"',
        'required("ASTERISM_REBASELINE_PROTOCOL"), contract::PROTOCOL',
        'assert_eq!(arguments.len(), 11, "current-fault argv differs")',
        'assert_eq!(arguments[0], "--fault")',
        'assert_eq!(arguments[10], "current-fault")',
        'required("ASTERISM_REBASELINE_MODE"), "fault"',
        'required("ASTERISM_REBASELINE_ATTEMPT_NONCE"), attempt_nonce',
        'required("ASTERISM_REBASELINE_PHASE"), phase',
        'required("ASTERISM_REBASELINE_SUITE"), "current-fault"',
        'required("ASTERISM_REBASELINE_VARIANT"), contract::VARIANT',
    ):
        require(block, token, f"invocation contract {token}")

    main = function_block(source, "main")
    require(main, "let invocation = invocation();", "single invocation parse", count=1)
    require(main, "control::validate_perf_environment_mode(mode);", "mode-aware perf environment validation", count=1)
    require(main, "assert!(!root.exists(), \"fault store root must be absent\");", "fresh smoke/fault store", count=1)
    require(main, "let evidence = runtime.block_on(run_cases(&root, &worker_nonce));", "shared full smoke/fault execution", count=1)
    require_order(
        main,
        (
            "let invocation = invocation();",
            "let mut control = Control::connect();",
            "make_control_close_on_exec();",
            "let boot_nonce = control.boot();",
            "let runtime_nonce = control.runtime(&boot_nonce);",
            "let control_engine = LogEngine::open_with(",
            "let opened_nonce = control.opened(&runtime_nonce);",
            "let start_nonce = control.ready_and_wait_start(",
            "let evidence = runtime.block_on(run_cases(&root, &worker_nonce));",
            "control.measured_and_wait_release(",
            "runtime.shutdown_timeout(Duration::from_secs(30));",
            "Invocation::Fault(args) => emit_fault(&args, &evidence)",
            "Invocation::Smoke => emit_smoke(&evidence)",
        ),
        "shared Control lifecycle",
    )


def pass_after_execution(source: str) -> None:
    require(function_block(source, "cases_json"), 'json_string("PASS")', "case PASS serialization", count=1)
    require(source, "CASE_SPECS.contains(&spec)", "approved PASS construction", count=1)
    require(source, "passes:      Vec<CasePass>", "executed evidence storage", count=1)
    require(source, "cases_json(&evidence.passes)", "output from executed evidence", count=2)
    if re.search(r"CaseSpec\s*\{[^}]*status", source, re.DOTALL):
        fail("static case specification carries an unexecuted status")
    if '"status", json_string("PASS")' in function_block(source, "run_cases"):
        fail("run_cases fabricates PASS before returning evidence")


def hook_and_sigkill_semantics(source: str) -> None:
    worker_hook_block = function_block(source, "worker_hook")
    for token in (
        "KillPoint::PreWrite => TestEngineHook::Pause",
        "point:         TestEngineHookPoint::PrePwrite",
        "KillPoint::PartialWrite => TestEngineHook::PartialRealWrite",
        "after_successful_pwrites: 0",
        "after_bytes:              8",
        "KillPoint::PostWritePreBarrier => TestEngineHook::Pause",
        "point:         TestEngineHookPoint::PreFdatasync",
        "KillPoint::PostBarrierPrePublication => TestEngineHook::Pause",
        "point:         TestEngineHookPoint::PostFdatasyncPrePublication",
        "KillPoint::PostPublicationPreCompletion => TestEngineHook::Pause",
        "point:         TestEngineHookPoint::PostPublicationPreCompletion",
    ):
        require(worker_hook_block, token, f"reviewed worker hook {token}")

    worker = function_block(source, "fault_worker")
    for token in (
        "std::env::current_exe",
        "--fault-worker",
        "hook.wait_until_reached();",
        "assert!(!append.is_finished()",
        "write_ready_marker(&ready, point, &nonce);",
        "std::future::pending::<()>().await;",
    ):
        # current_exe/--fault-worker live in spawn_fault_worker, handled below.
        if token in {"std::env::current_exe", "--fault-worker"}:
            continue
        require(worker, token, f"worker rendezvous {token}")
    spawn = function_block(source, "spawn_fault_worker")
    require(spawn, "std::env::current_exe()", "self-exec worker", count=1)
    require(spawn, '.arg("--fault-worker")', "worker route", count=1)
    require(spawn, ".stdout(Stdio::null())", "worker stdout isolation", count=1)
    require(spawn, ".stderr(Stdio::null())", "worker stderr isolation", count=1)

    kill = function_block(source, "run_kill_case")
    for token in (
        "await_worker_ready(&mut child, &ready, point, nonce);",
        'child.kill().expect("SIGKILL fault worker");',
        "assert_eq!(status.signal(), Some(9)",
        "LogEngine::open_with(&store, options(Durability::Os))",
        "KillPoint::PreWrite | KillPoint::PartialWrite",
        "KillPoint::PostWritePreBarrier => {}",
        "KillPoint::PostBarrierPrePublication",
        "| KillPoint::PostPublicationPreCompletion",
    ):
        require(kill, token, f"real SIGKILL outcome {token}")

    cloexec = function_block(source, "make_control_close_on_exec")
    for token in (
        'required("ASTERISM_REBASELINE_CONTROL_FD")',
        "F_GETFD",
        "F_SETFD",
        "FD_CLOEXEC",
        "fcntl(fd, F_SETFD, flags | FD_CLOEXEC)",
    ):
        require(cloexec, token, f"worker control-fd isolation {token}")


def injected_io_and_poison(source: str) -> None:
    expectations = {
        "case_short_write": (
            "TestEngineHook::PwriteZero",
            "hook.wait_until_reached();",
            "assert_backend_error(result);",
            'Version::At(0)',
        ),
        "case_write_error": (
            "TestEngineHook::PwriteEio",
            "hook.wait_until_reached();",
            "assert_backend_error(result);",
            'Version::At(0)',
        ),
        "case_fdatasync_error": (
            "TestEngineHook::FdatasyncEio",
            "hook.wait_until_reached();",
            "assert_backend_error(result);",
            "engine.metrics().degraded_poisoned",
            "Version::At(0) | Version::At(1)",
        ),
        "case_uncertain_persistence_poison": (
            "TestEngineHook::FdatasyncEio",
            "assert_backend_error(uncertain);\n    assert!(engine.metrics().degraded_poisoned);",
            'b"never-retry-barrier"',
            'b"durable-prefix"',
        ),
    }
    for function, tokens in expectations.items():
        block = function_block(source, function)
        for token in tokens:
            require(block, token, f"{function} semantic {token}")


def corruption_and_recovery(source: str) -> None:
    torn = function_block(source, "case_torn_truncated_tail")
    for token in (
        "tail.offset + tail.total_len - 1",
        ".set_len(torn_len)",
        "metadata(&path).unwrap().len() < before.safe_offset",
        "assert_eq!(observed.len(), 1)",
        'assert_eq!(observed[0].data, b"prefix")',
    ):
        require(torn, token, f"torn-tail semantic {token}")

    marker = function_block(source, "case_invalid_marker_crc")
    for token in (
        "tail.offset + tail.total_len - 1",
        "mutate_byte_sync(&path, crc_echo_byte);",
        "assert_eq!(after.accepted.len() + 1, before.accepted.len())",
        "assert_eq!(after.safe_offset, tail.offset)",
    ):
        require(marker, token, f"invalid-marker semantic {token}")

    registry = function_block(source, "case_corrupt_registry_record")
    for token in (
        ".find(|batch| batch.stream_id == 0)",
        "image[payload_offset] = 0xFF;",
        "let repaired_crc = batch_crc(&image[start..end]);",
        "HEADER_CRC_OFF",
        "image[end - 4..end].copy_from_slice",
        "assert_eq!(after.accepted.len(), before.accepted.len())",
        ".expect(\"CRC-valid corrupt registry record must refuse open\")",
    ):
        require(registry, token, f"registry-corruption semantic {token}")

    sidecar = function_block(source, "case_refuted_corrupt_sidecar")
    for token in (
        'first_sidecar(&store, "pcol")',
        "mutate_byte_sync(&pcol, length / 2);",
        'expect("reopen with corrupt refuted sidecar")',
        'expect("raw-log fallback after corrupt pcol")',
        "assert_eq!(observed.len(), expected.len())",
    ):
        require(sidecar, token, f"sidecar-refutation semantic {token}")

    group = function_block(source, "case_acknowledged_group_survives_reopen")
    require(group, "Durability::group_default()", "Group acknowledgement durability", count=2)
    require(group, 'expect("acknowledged Group append")', "positive Group acknowledgement")
    require(group, 'expect("reopen acknowledged Group store")', "Group reopen")
    require(group, "assert_eq!(observed.len(), expected.len())", "Group corpus recovery")


def boundedness_contract(source: str) -> None:
    require(source, "const OWNER_RING_INTENTS: usize = 1_024;", "fixed owner-ring authority", count=1)
    owner = function_block(source, "case_owner_ring_intent_bound")
    for token in (
        "OWNER_RING_INTENTS + 1",
        "cohort.wait_until_admitted(OWNER_RING_INTENTS + 1);",
        "engine.metrics().owner_intent_slots_in_use",
        "assert_eq!(observed_bound, OWNER_RING_INTENTS);",
        "assert_zero_reservations(&engine);",
    ):
        require(owner, token, f"owner-ring bound {token}")

    cancel = function_block(source, "case_cancel_before_admission")
    for token in (
        "metrics.owner_intent_slots_in_use == OWNER_RING_INTENTS",
        "metrics.owner_intent_bytes_in_use > baseline_bytes",
        "candidate.abort();",
        "expect_err(\"cancelled candidate returned\")",
        ".is_cancelled()",
        "metrics.owner_intent_bytes_in_use == baseline_bytes",
    ):
        require(cancel, token, f"pre-admission reservation semantic {token}")

    group = function_block(source, "case_group_byte_time_bounds")
    for token in (
        "const TIME_TRAINING_COHORT: usize = 4;",
        "const TIME_INCOMPLETE_COHORT: usize = 2;",
        "const TIME_MAX_DELAY: Duration = Duration::from_millis(200);",
        "const TIME_ADMISSION_CAP: Duration = Duration::from_millis(50);",
        "const TIME_DEADLINE_TOLERANCE: Duration = Duration::from_millis(150);",
        "max_delay: Duration::from_secs(30)",
        "max_bytes: 1",
        "let byte_cohort = byte_engine.arm_test_owner_cohort(2);",
        "byte_cohort.wait_until_admitted(2);",
        "Group byte bound failed to close before 30-second time bound",
        "let byte_groups = byte_after.groups - byte_before.groups;",
        "let byte_batches = byte_after.batches - byte_before.batches;",
        "byte_groups == 2",
        "byte_batches == 2",
        "max_delay: TIME_MAX_DELAY",
        "max_bytes: u64::MAX",
        "byte_after.fsync.count - byte_before.fsync.count == byte_groups",
        "time_engine.arm_test_owner_cohort(TIME_TRAINING_COHORT)",
        "training_cohort.wait_until_admitted(TIME_TRAINING_COHORT);",
        "training_batches == TIME_TRAINING_COHORT as u64",
        "training_groups == 1",
        "training_fsyncs == training_groups",
        "TIME_INCOMPLETE_COHORT < TIME_TRAINING_COHORT",
        "time_engine.arm_test_owner_cohort(TIME_INCOMPLETE_COHORT)",
        "incomplete_cohort.wait_until_admitted(TIME_INCOMPLETE_COHORT);",
        "time_admission_elapsed <= TIME_ADMISSION_CAP",
        "&& time_admission_bounded",
        "point:         TestEngineHookPoint::PreFdatasync",
        "deadline_hook.wait_until_reached();",
        "let time_elapsed_cap = TIME_MAX_DELAY + TIME_DEADLINE_TOLERANCE;",
        "time_deadline_elapsed >= TIME_MAX_DELAY",
        "time_deadline_elapsed <= time_elapsed_cap",
        "&& time_deadline_bounded",
        "deadline_hook.release();",
        "time_batches == TIME_INCOMPLETE_COHORT as u64",
        "time_groups == 1",
        "time_fsyncs == time_groups",
    ):
        require(group, token, f"Group bound semantic {token}")
    require_order(
        group,
        (
            "let training_before = time_engine.metrics().commit;",
            "time_engine.arm_test_owner_cohort(TIME_TRAINING_COHORT)",
            "training_cohort.wait_until_admitted(TIME_TRAINING_COHORT);",
            "let training_after = time_engine.metrics().commit;",
            "let time_causal_setup = TIME_INCOMPLETE_COHORT",
            'assert!(time_causal_setup, "adaptive target training differs");',
            "let time_before = time_engine.metrics().commit;",
            "time_engine.arm_test_owner_cohort(TIME_INCOMPLETE_COHORT)",
            "let time_started = Instant::now();",
            "incomplete_cohort.wait_until_admitted(TIME_INCOMPLETE_COHORT);",
            "let time_admission_elapsed = time_started.elapsed();",
            "deadline_hook.wait_until_reached();",
            "let time_deadline_elapsed = time_started.elapsed();",
            "let time_elapsed_cap = TIME_MAX_DELAY",
            "deadline_hook.release();",
            "let time_after = time_engine.metrics().commit;",
            "let group_time_bound_proven = time_causal_setup",
            '"incomplete adaptive cohort did not close at configured max_delay"',
        ),
        "Group max-delay causal proof",
    )

    zero = function_block(source, "case_zero_reservations_after_cancel_complete")
    for token in (
        "zero-reservations success path",
        "Err(AppendError::Conflict { .. })",
        "let empty: [RecordToAppend; 0] = [];",
        'append_batch("zero-reservations", Version::At(0), &empty)',
        '.expect("empty no-op at current version");',
        "empty_outcome.version,\n        Version::At(0)",
        '"empty no-op must return the unchanged current version"',
        "TestEngineHook::PwriteEio",
        "PostPublicationPreCompletion",
        "abandoned.abort();",
        "sentinel after abandoned receiver",
        "metrics.owner_intent_slots_in_use",
        "metrics.owner_intent_bytes_in_use",
    ):
        require(zero, token, f"zero-reservation terminal path {token}")
    require_order(
        zero,
        (
            "let empty: [RecordToAppend; 0] = [];",
            "let empty_outcome = engine",
            '.expect("empty no-op at current version");',
            "empty_outcome.version,",
            '"empty no-op must return the unchanged current version"',
            "assert_zero_reservations(&engine);",
        ),
        "empty no-op current-version reservation release",
    )

    run = function_block(source, "run_cases")
    exact = (
        "owner_ring_intents:        1_024",
        "group_byte_bound_proven:   true",
        "group_time_bound_proven:   true",
        "waiter_reservations_after: 0",
        "byte_reservations_after:   0",
    )
    for token in exact:
        require(run, token, f"exact boundedness {token}", count=1)
    output = function_block(source, "boundedness_json")
    for field in sorted(BOUNDS):
        require(output, f'"{field}"', f"boundedness output field {field}", count=1)


def compile_out_contract(source: str) -> None:
    if "option_env!(\"ASTERISM_FAULT_COMPILE_OUT_" in source:
        fail("compile-out authority became optional")
    for name in COMPILE_ENV:
        require(source, f'env!("{name}")', f"compile-time authority {name}", count=1)
    block = function_block(source, "case_fault_hook_compiles_out_binary_identical")
    for token in (
        '"bn-2l3n-fault-compile-out-authority-v1"',
        'assert_eq!(compile_out_authority::IDENTICAL, "true")',
        "lower_hex(compile_out_authority::PRISTINE_SHA256, 64)",
        "compile_out_authority::OVERLAY_RELEASE_SHA256",
        "compile_out_authority::SYMBOL_ABSENCE_SHA256",
        "compile_out_authority::PRISTINE_SHA256,\n        compile_out_authority::OVERLAY_RELEASE_SHA256",
    ):
        require(block, token, f"compile-out authority semantic {token}")


def output_contract(source: str) -> None:
    fault = function_block(source, "emit_fault")
    for token in (
        '("attempt_nonce", json_string(&args.attempt_nonce))',
        '("boundedness", boundedness_json(evidence.boundedness))',
        '("cases", cases_json(&evidence.passes))',
        '("harness_sound", json_bool(true))',
        '("phase", json_string(&args.phase))',
        '("protocol", json_string(contract::PROTOCOL))',
        '("schema", json_string("bn-2l3n-correctness-child-v3"))',
        '("suite", json_string("current-fault"))',
        '("variant", json_string(contract::VARIANT))',
    ):
        require(fault, token, f"fault output {token}", count=1)

    smoke = function_block(source, "emit_smoke")
    for token in (
        '("boundedness", boundedness_json(evidence.boundedness))',
        '("cases", cases_json(&evidence.passes))',
        '("harness_sound", json_bool(true))',
        '("protocol", json_string(contract::PROTOCOL))',
        '("schema", json_string("bn-2l3n-smoke-v3"))',
        '("smoke_target", json_string("fault"))',
        '("status", json_string("PASS"))',
        '("variant", json_string(contract::VARIANT))',
    ):
        require(smoke, token, f"smoke output {token}", count=1)

    require(source, "println!(", "stdout publication", count=2)


def validate(source: str) -> None:
    if "\r" in source or not source.endswith("\n"):
        fail("fault.rs text normalization differs")
    exact_case_partition(source)
    invocation_contracts(source)
    pass_after_execution(source)
    hook_and_sigkill_semantics(source)
    injected_io_and_poison(source)
    corruption_and_recovery(source)
    boundedness_contract(source)
    compile_out_contract(source)
    output_contract(source)


Mutation = tuple[str, Callable[[str], str]]


def replace_once(old: str, new: str) -> Callable[[str], str]:
    def mutate(source: str) -> str:
        if source.count(old) != 1:
            fail(f"self-test mutation target is not unique: {old!r}")
        return source.replace(old, new, 1)

    return mutate


HOSTILE_MUTATIONS: Final[tuple[Mutation, ...]] = (
    ("case-id", replace_once('id:             "cancel-before-admission"', 'id:             "cancel-before-queue"')),
    ("classification", replace_once('classification: "poison"', 'classification: "durability"')),
    ("smoke-target", replace_once('required("ASTERISM_REBASELINE_SMOKE_TARGET"), "fault"', 'required("ASTERISM_REBASELINE_SMOKE_TARGET"), "correctness"')),
    ("fault-suite", replace_once('assert_eq!(arguments[10], "current-fault")', 'assert_eq!(arguments[10], "current-product")')),
    ("smoke-schema", replace_once('("schema", json_string("bn-2l3n-smoke-v3"))', '("schema", json_string("bn-2l3n-smoke-v2"))')),
    ("control-open", replace_once("let mut control = Control::connect();", "let mut control = unsafe_control();")),
    ("control-cloexec", replace_once("fcntl(fd, F_SETFD, flags | FD_CLOEXEC)", "fcntl(fd, F_SETFD, flags)")),
    ("smoke-bypass", replace_once("let evidence = runtime.block_on(run_cases(&root, &worker_nonce));", "let evidence = assumed_evidence();")),
    ("kill-method", replace_once('child.kill().expect("SIGKILL fault worker");', 'child.wait().expect("wait fault worker");')),
    ("kill-signal", replace_once("assert_eq!(status.signal(), Some(9)", "assert_eq!(status.signal(), Some(15)")),
    ("partial-prefix", replace_once("after_bytes:              8", "after_bytes:              0")),
    ("pre-barrier-hook", replace_once("KillPoint::PostWritePreBarrier => TestEngineHook::Pause {\n            point:         TestEngineHookPoint::PreFdatasync", "KillPoint::PostWritePreBarrier => TestEngineHook::Pause {\n            point:         TestEngineHookPoint::PrePwrite")),
    ("post-barrier-hook", replace_once("point:         TestEngineHookPoint::PostFdatasyncPrePublication", "point:         TestEngineHookPoint::PreFdatasync")),
    ("short-write-hook", replace_once("let hook = engine.arm_test_hook(TestEngineHook::PwriteZero", "let hook = engine.arm_test_hook(TestEngineHook::PwriteEio")),
    ("torn-boundary", replace_once("let torn_len = tail.offset + tail.total_len - 1;", "let torn_len = tail.offset + tail.total_len;")),
    ("marker-boundary", replace_once("let crc_echo_byte = tail.offset + tail.total_len - 1;", "let crc_echo_byte = tail.offset + tail.total_len;")),
    ("registry-byte", replace_once("image[payload_offset] = 0xFF;", "image[payload_offset] = image[payload_offset];")),
    ("registry-crc", replace_once("let repaired_crc = batch_crc(&image[start..end]);", "let repaired_crc = 0;")),
    ("sidecar-authority", replace_once('let pcol = first_sidecar(&store, "pcol");', 'let pcol = first_sidecar(&store, "pidx");')),
    ("poison-latch", replace_once("assert_backend_error(uncertain);\n    assert!(engine.metrics().degraded_poisoned);", "assert_backend_error(uncertain);\n    assert!(!engine.metrics().degraded_poisoned);")),
    ("group-durability", replace_once('expect("acknowledged Group append")', 'expect("assumed Group append")')),
    ("owner-ring", replace_once("const OWNER_RING_INTENTS: usize = 1_024;", "const OWNER_RING_INTENTS: usize = 1_023;")),
    ("group-byte-bound", replace_once("max_bytes: 1,", "max_bytes: 2,")),
    ("group-time-delay", replace_once("const TIME_MAX_DELAY: Duration = Duration::from_millis(200);", "const TIME_MAX_DELAY: Duration = Duration::from_secs(2);")),
    ("group-time-setup", replace_once("const TIME_TRAINING_COHORT: usize = 4;", "const TIME_TRAINING_COHORT: usize = 1;")),
    ("group-time-admission-cause", replace_once("&& time_admission_bounded", "&& true")),
    ("group-time-deadline-cause", replace_once("&& time_deadline_bounded", "&& true")),
    ("group-time-lower-bound-removed", replace_once("time_deadline_elapsed >= TIME_MAX_DELAY", "time_deadline_elapsed >= Duration::ZERO")),
    ("group-time-cap-removed", replace_once("time_deadline_elapsed <= time_elapsed_cap", "time_deadline_elapsed <= Duration::from_secs(5)")),
    ("group-time-cap-loosened", replace_once("const TIME_DEADLINE_TOLERANCE: Duration = Duration::from_millis(150);", "const TIME_DEADLINE_TOLERANCE: Duration = Duration::from_secs(5);")),
    ("empty-outcome-flipped", replace_once('.expect("empty no-op at current version");', '.expect_err("empty append unexpectedly succeeded");')),
    ("empty-version-flipped", replace_once("empty_outcome.version,\n        Version::At(0)", "empty_outcome.version,\n        Version::At(1)")),
    ("zero-reservation", replace_once("waiter_reservations_after: 0,", "waiter_reservations_after: 1,")),
    ("compile-schema-env", replace_once('env!("ASTERISM_FAULT_COMPILE_OUT_SCHEMA")', 'option_env!("ASTERISM_FAULT_COMPILE_OUT_SCHEMA").unwrap_or("assumed")')),
    ("compile-identical", replace_once('assert_eq!(compile_out_authority::IDENTICAL, "true")', 'assert_ne!(compile_out_authority::IDENTICAL, "true")')),
    ("compile-digest-equality", replace_once("compile_out_authority::PRISTINE_SHA256,\n        compile_out_authority::OVERLAY_RELEASE_SHA256", "compile_out_authority::PRISTINE_SHA256,\n        compile_out_authority::PRISTINE_SHA256")),
    ("pass-source", replace_once('(\"attempt_nonce\", json_string(&args.attempt_nonce)),\n            (\"boundedness\", boundedness_json(evidence.boundedness)),\n            (\"cases\", cases_json(&evidence.passes))', '(\"attempt_nonce\", json_string(&args.attempt_nonce)),\n            (\"boundedness\", boundedness_json(evidence.boundedness)),\n            (\"cases\", cases_json(&CASE_SPECS))')),
    ("fault-harness-sound", replace_once('(\"harness_sound\", json_bool(true)),\n            (\"phase\"', '(\"harness_sound\", json_bool(false)),\n            (\"phase\"')),
)


def run_hostile_mutations(source: str) -> int:
    rejected = 0
    for label, mutate in HOSTILE_MUTATIONS:
        hostile = mutate(source)
        try:
            validate(hostile)
        except ValidationError:
            rejected += 1
        else:
            fail(f"hostile mutation escaped validation: {label}")
    return rejected


def canonical(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ) + "\n"


def main(argv: list[str]) -> int:
    if argv not in ([], ["--self-test"]):
        raise SystemExit("usage: validate_fault.py [--self-test]")
    # Parsing this validator itself is part of --self-test's self-containment
    # check and harmless in normal mode; it writes no cache or other artifact.
    ast.parse(Path(__file__).read_text(encoding="utf-8"), filename=__file__)
    source = SOURCE.read_text(encoding="utf-8")
    validate(source)
    rejected = run_hostile_mutations(source)
    result = {
        "schema": SCHEMA,
        "status": "ok",
        "checks": list(CHECKS),
        "hostile_mutations_rejected": rejected,
    }
    sys.stdout.write(canonical(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
