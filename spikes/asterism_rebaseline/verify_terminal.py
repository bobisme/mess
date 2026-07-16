#!/usr/bin/env python3
"""Post-release terminal verifier for the bn-2l3n evidence chain.

This executable intentionally does not import the evaluator.  It verifies the
immutable result and the runner's release/publication chain after the global
measurement lease has been released.
"""

from __future__ import annotations

import hashlib
import fcntl
import json
import os
import re
import stat
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

import evidence_schema as schema


EXIT_VERIFIED = 0
EXIT_USAGE = 2
EXIT_INVALID = 20
EXIT_INTERNAL = 30
OUTCOME_EXIT = {"ADMIT": 0, "NARROW": 10, "REVERT": 11, "INCONCLUSIVE": 20}
EXCLUDED_INVENTORY = {
    "SHA256SUMS",
    "terminal-pre-release.json",
    "lease-release.json",
    "terminal.json",
    "terminal-verification.json",
}
COMMON_REQUIRED_INVENTORY = {
    "BN-2L3N-PROTOCOL.md",
    "BN-2SU-FINAL.csv",
    "REPORT.md",
    "config.json",
    "provenance.json",
    "source-approval.json",
    "prepared-artifacts.json",
    "profile-contract.json",
    "correctness.json",
    "raw-manifest.json",
    "guard-manifest.jsonl",
    "child-manifest.jsonl",
    "result.json",
    "evaluator-transition.json",
}
FULL_REQUIRED_INVENTORY = {
    *COMMON_REQUIRED_INVENTORY,
    *schema.CSV_FILENAMES.values(),
}
CORRECTNESS_ONLY_REQUIRED_INVENTORY = {
    *COMMON_REQUIRED_INVENTORY,
    "correctness-only.json",
}
PRE_RELEASE_FIELDS = set(schema.TERMINAL_PRE_RELEASE_FIELDS)
EVALUATOR_CHILD_FIELDS = set(schema.CHILD_FIELDS)
RELEASE_FIELDS = set(schema.LEASE_RELEASE_FIELDS)
TERMINAL_FIELDS = set(schema.TERMINAL_FIELDS)
GUEST_ROOT = "/asterism"
GUEST_SOURCE = f"{GUEST_ROOT}/source"
GUEST_TARGET = f"{GUEST_ROOT}/target"
GUEST_TOOLCHAIN_ROOT = f"{GUEST_ROOT}/toolchain"
GUEST_CARGO = f"{GUEST_TOOLCHAIN_ROOT}/bin/cargo"
GUEST_RUSTC = f"{GUEST_TOOLCHAIN_ROOT}/bin/rustc"
GUEST_CARGO_HOME = f"{GUEST_ROOT}/cargo-home"
GUEST_RUSTUP_HOME = f"{GUEST_ROOT}/rustup-home"
GUEST_BOUND_CONFIG_PATHS = (
    f"{GUEST_SOURCE}/.cargo/config.toml",
    f"{GUEST_SOURCE}/.cargo/config",
    f"{GUEST_CARGO_HOME}/config.toml",
    f"{GUEST_CARGO_HOME}/config",
)
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
_BOUND_SNAPSHOTS: dict[Path, schema.FileSnapshot] = {}


def canonical_json_bytes(value: Any) -> bytes:
    return schema.canonical_json_bytes(value)


def sha256_file(path: Path) -> str:
    lexical = Path(path)
    snapshot = _BOUND_SNAPSHOTS.get(lexical)
    if snapshot is None:
        snapshot = schema.snapshot_regular_file(lexical)
        _BOUND_SNAPSHOTS[lexical] = snapshot
    return snapshot.sha256


def parse_timestamp(value: Any, context: str, errors: list[str]) -> datetime | None:
    if not isinstance(value, str):
        errors.append(f"{context} is not text")
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        errors.append(f"{context} invalid: {error}")
        return None
    if parsed.tzinfo is None:
        errors.append(f"{context} has no timezone")
        return None
    return parsed


def require_keys(value: Any, expected: set[str], context: str, errors: list[str]) -> bool:
    if not isinstance(value, dict):
        errors.append(f"{context} is not an object")
        return False
    if set(value) != expected:
        errors.append(
            f"{context} keys are not exact; missing={sorted(expected - set(value))}, "
            f"extra={sorted(set(value) - expected)}"
        )
        return False
    return True


def read_object(
    path: Path | schema.FileSnapshot, context: str, errors: list[str]
) -> dict[str, Any] | None:
    try:
        if isinstance(path, schema.FileSnapshot):
            snapshot = path
        else:
            snapshot = _BOUND_SNAPSHOTS.get(path)
            if snapshot is None:
                snapshot = schema.snapshot_regular_file(path, expected_mode=0o444)
                _BOUND_SNAPSHOTS[path] = snapshot
        return schema.parse_canonical_json_object(snapshot.data, context)
    except (OSError, ValueError) as error:
        errors.append(str(error))
        return None


def bound_file(
    path_value: Any,
    digest: Any,
    expected: Path,
    context: str,
    errors: list[str],
    *,
    expected_mode: int = 0o444,
) -> schema.FileSnapshot | None:
    if not isinstance(path_value, str) or Path(path_value) != expected:
        errors.append(f"{context} path is not exact")
        return None
    if not isinstance(digest, str) or len(digest) != 64:
        errors.append(f"{context} hash is invalid")
        return None
    try:
        snapshot = _BOUND_SNAPSHOTS.get(expected)
        if snapshot is None:
            snapshot = schema.snapshot_regular_file(
                expected, expected_mode=expected_mode
            )
            _BOUND_SNAPSHOTS[expected] = snapshot
        elif snapshot.mode != expected_mode:
            raise OSError(
                f"mode {snapshot.mode:#06o} differs from exact {expected_mode:#06o}"
            )
    except (OSError, ValueError) as error:
        errors.append(f"cannot snapshot {context}: {error}")
        return None
    if snapshot.sha256 != digest:
        errors.append(f"{context} hash mismatch")
    return snapshot


def read_jsonl(
    path: Path | schema.FileSnapshot, context: str, errors: list[str]
) -> list[dict[str, Any]]:
    try:
        if isinstance(path, schema.FileSnapshot):
            snapshot = path
        else:
            snapshot = _BOUND_SNAPSHOTS.get(path)
            if snapshot is None:
                snapshot = schema.snapshot_regular_file(path, expected_mode=0o444)
                _BOUND_SNAPSHOTS[path] = snapshot
        data = snapshot.data
    except (OSError, ValueError) as error:
        errors.append(f"cannot read {context}: {error}")
        return []
    if data and not data.endswith(b"\n"):
        errors.append(f"{context} lacks final LF")
    records = []
    for ordinal, line in enumerate(data.splitlines(keepends=True), start=1):
        try:
            records.append(schema.parse_canonical_json_object(line, f"{context} line {ordinal}"))
        except ValueError as error:
            errors.append(str(error))
    return records


_TERMINAL_REVIEW_IDENTIFIER = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._:/@+\-]{0,255}\Z"
)
_TERMINAL_ZONED_TIME = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    r"(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})\Z"
)


def terminal_is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def terminal_is_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def terminal_authority_object(
    value: Any,
    fields: Any,
    context: str,
    errors: list[str],
) -> Mapping[str, Any] | None:
    if not isinstance(value, Mapping) or set(value) != set(fields):
        errors.append(f"{context} fields are not exact")
        return None
    return value


def terminal_authority_timestamp(
    value: Any, context: str, errors: list[str]
) -> datetime | None:
    if not isinstance(value, str) or _TERMINAL_ZONED_TIME.fullmatch(value) is None:
        errors.append(f"{context} is not an exact zoned timestamp")
        return None
    return parse_timestamp(value, context, errors)


def validate_terminal_release_requirement(
    value: Any, errors: list[str]
) -> Mapping[str, Any] | None:
    requirement = terminal_authority_object(
        value,
        schema.RELEASE_COMPILE_OUT_REQUIREMENT_FIELDS,
        "terminal release compile-out requirement",
        errors,
    )
    if requirement is None:
        return None
    exact = {
        "schema": schema.RELEASE_COMPILE_OUT_REQUIREMENT_SCHEMA,
        "status": "required",
        "variant": "A",
        "proof_must_bind_enclosing_approval_sha256": True,
        "repeat_under_real_source_approval": True,
        "same_contract_nonce_lock_toolchain_sandbox": True,
        "cfg_test": False,
        "rustc_workspace_wrapper": "absent",
        "ordinary_a_role": "published",
        "overlay_a_role": "proof_only",
        "binary_byte_identical": True,
        "symbol_inventory_byte_identical": True,
        "forbidden_hook_strings": list(
            schema.RELEASE_COMPILE_OUT_FORBIDDEN_HOOK_STRINGS
        ),
        "forbidden_hook_strings_absent": True,
    }
    if any(requirement.get(field) != expected for field, expected in exact.items()):
        errors.append("terminal release compile-out requirement policy differs")
    for field in ("product_overlay_sha256", "preapproval_compile_out_sha256"):
        if not terminal_is_sha256(requirement.get(field)):
            errors.append(f"terminal release compile-out requirement {field} is invalid")
    return requirement


def validate_terminal_source_input(
    value: Any, name: str, errors: list[str]
) -> Mapping[str, Any] | None:
    context = f"terminal source-review input {name}"
    binding = terminal_authority_object(
        value, schema.SOURCE_REVIEW_INPUT_FIELDS, context, errors
    )
    if binding is None:
        return None
    path = binding.get("path")
    if (
        binding.get("schema") != schema.SOURCE_REVIEW_INPUT_SCHEMA
        or not isinstance(path, str)
        or not Path(path).is_absolute()
        or not terminal_is_sha256(binding.get("sha256"))
        or not terminal_is_integer(binding.get("size"))
        or binding.get("size", 0) <= 0
        or binding.get("mode") != schema.ARTIFACT_FILE_MODE
    ):
        errors.append(f"{context} immutable binding differs")
    identity = terminal_authority_object(
        binding.get("identity"),
        schema.SOURCE_REVIEW_IDENTITY_FIELDS,
        f"{context} identity",
        errors,
    )
    if identity is not None:
        if any(
            not terminal_is_integer(identity.get(field)) or identity.get(field, -1) < 0
            for field in schema.SOURCE_REVIEW_IDENTITY_FIELDS
        ) or identity.get("device", 0) <= 0 or identity.get("inode", 0) <= 0:
            errors.append(f"{context} identity values are invalid")
        if identity.get("link_count") != 1:
            errors.append(f"{context} identity link count differs")
    return binding


def validate_terminal_source_review_semantics(
    approval: Mapping[str, Any],
    bundle: Mapping[str, Any],
    current_children: Mapping[str, Any],
    lock_authority: Mapping[str, Any],
    lock_review_bundle: Mapping[str, Any],
    errors: list[str],
) -> Mapping[str, Any] | None:
    """Replay reviewed source authority without calling shared semantic validators."""

    source_review = terminal_authority_object(
        approval.get("source_review"),
        schema.SOURCE_REVIEW_FIELDS,
        "terminal source review",
        errors,
    )
    if source_review is None:
        return None
    requirement = validate_terminal_release_requirement(
        source_review.get("release_compile_out_requirement"), errors
    )
    if not terminal_is_sha256(source_review.get("assertion_sha256")):
        errors.append("terminal source-review assertion hash is invalid")
    for name, expected_schema in schema.SOURCE_REVIEW_CONTENT_SCHEMAS.items():
        binding = terminal_authority_object(
            source_review.get(name),
            schema.SOURCE_REVIEW_CONTENT_BINDING_FIELDS,
            f"terminal source review {name}",
            errors,
        )
        if binding is not None and (
            binding.get("schema") != expected_schema
            or binding.get("mode") != schema.ARTIFACT_FILE_MODE
            or not terminal_is_sha256(binding.get("sha256"))
        ):
            errors.append(f"terminal source review {name} binding differs")

    bundle_value = terminal_authority_object(
        bundle,
        schema.SOURCE_REVIEW_BUNDLE_FIELDS,
        "terminal source-review bundle",
        errors,
    )
    if bundle_value is None:
        return None
    assertion = terminal_authority_object(
        bundle_value.get("assertion"),
        schema.SOURCE_REVIEW_ASSERTION_FIELDS,
        "terminal source-review assertion",
        errors,
    )
    if assertion is None:
        return None
    assertion_sha256 = hashlib.sha256(canonical_json_bytes(assertion)).hexdigest()
    if (
        bundle_value.get("schema") != schema.SOURCE_REVIEW_BUNDLE_SCHEMA
        or bundle_value.get("assertion_sha256") != assertion_sha256
        or source_review.get("assertion_sha256") != assertion_sha256
    ):
        errors.append("terminal source-review assertion digest binding differs")
    exact_assertion = {
        "schema": schema.SOURCE_REVIEW_ASSERTION_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "status": "approved",
        "open_findings": 0,
        "tooling_commit": approval.get("tooling_commit"),
        "tooling_tree": approval.get("tooling_tree"),
        "release_compile_out_requirement": requirement,
    }
    if any(assertion.get(field) != expected for field, expected in exact_assertion.items()):
        errors.append("terminal source-review assertion authority differs")
    inputs = assertion.get("inputs")
    if not isinstance(inputs, Mapping) or set(inputs) != set(
        schema.SOURCE_REVIEW_INPUT_NAMES
    ):
        errors.append("terminal source-review assertion input names differ")
        return None
    validated_inputs = {
        name: validate_terminal_source_input(inputs[name], name, errors)
        for name in schema.SOURCE_REVIEW_INPUT_NAMES
    }
    if any(binding is None for binding in validated_inputs.values()):
        return None
    paths = [
        binding.get("path")
        for binding in validated_inputs.values()
        if isinstance(binding, Mapping)
    ]
    identities = [
        (binding.get("identity", {}).get("device"), binding.get("identity", {}).get("inode"))
        for binding in validated_inputs.values()
        if isinstance(binding, Mapping)
    ]
    if (
        len(paths) != len(schema.SOURCE_REVIEW_INPUT_NAMES)
        or len(set(paths)) != len(paths)
        or len(set(identities)) != len(identities)
    ):
        errors.append("terminal source-review inputs are not physically disjoint")

    created = terminal_authority_object(
        bundle_value.get("review_created"),
        schema.SOURCE_REVIEW_SEAL_EVENT_FIELDS,
        "terminal source-review ReviewCreated",
        errors,
    )
    verdict = terminal_authority_object(
        bundle_value.get("verdict"),
        schema.SOURCE_REVIEW_SEAL_EVENT_FIELDS,
        "terminal source-review ReviewerVoted",
        errors,
    )
    created_at = reviewed_at = None
    review_id = approval.get("review_id")
    if created is not None:
        created_data = terminal_authority_object(
            created.get("data"),
            schema.SOURCE_REVIEW_CREATED_DATA_FIELDS,
            "terminal source-review ReviewCreated data",
            errors,
        )
        author = created.get("author")
        detached = f"detached:{approval.get('tooling_commit')}"
        if (
            created.get("event") != "ReviewCreated"
            or not isinstance(author, str)
            or _TERMINAL_REVIEW_IDENTIFIER.fullmatch(author) is None
            or created_data is None
            or created_data.get("review_id") != review_id
            or created_data.get("initial_commit") != approval.get("tooling_commit")
            or created_data.get("jj_change_id") != detached
            or created_data.get("scm_anchor") != detached
            or created_data.get("scm_kind") != "git"
            or not isinstance(created_data.get("title"), str)
            or not created_data.get("title")
            or not isinstance(created_data.get("description"), str)
            or not created_data.get("description")
        ):
            errors.append("terminal source-review ReviewCreated authority differs")
        created_at = terminal_authority_timestamp(
            created.get("ts"), "terminal source-review created time", errors
        )
    if verdict is not None:
        verdict_data = terminal_authority_object(
            verdict.get("data"),
            schema.SOURCE_REVIEW_VERDICT_DATA_FIELDS,
            "terminal source-review ReviewerVoted data",
            errors,
        )
        author = verdict.get("author")
        expected_reason = (
            f"APPROVED assertion_sha256={assertion_sha256}; open_findings=0"
        )
        if (
            verdict.get("event") != "ReviewerVoted"
            or not isinstance(author, str)
            or _TERMINAL_REVIEW_IDENTIFIER.fullmatch(author) is None
            or verdict_data
            != {"reason": expected_reason, "review_id": review_id, "vote": "lgtm"}
        ):
            errors.append("terminal source-review ReviewerVoted authority differs")
        reviewed_at = terminal_authority_timestamp(
            verdict.get("ts"), "terminal source-review verdict time", errors
        )
        if approval.get("reviewed_at") != verdict.get("ts"):
            errors.append("terminal source approval time is not Seal-derived")
    if created_at is not None and reviewed_at is not None and reviewed_at < created_at:
        errors.append("terminal source-review verdict predates ReviewCreated")

    expected_hashes = {
        "current_children_attestation": source_review.get(
            "current_children_attestation", {}
        ).get("sha256"),
        "lock_authority": source_review.get("lock_authority", {}).get("sha256"),
        "lock_review_bundle": source_review.get("lock_review_bundle", {}).get(
            "sha256"
        ),
        "tools_manifest": approval.get("tools_manifest_sha256"),
    }
    for name, expected in expected_hashes.items():
        if inputs[name].get("sha256") != expected:
            errors.append(f"terminal source-review input {name} hash differs")

    stripped_inputs = {
        "authority": {
            field: inputs["lock_authority"][field]
            for field in schema.SOURCE_REVIEW_INPUT_FIELDS
            if field != "schema"
        },
        "lock_manifest": {
            field: inputs["lock_manifest"][field]
            for field in schema.SOURCE_REVIEW_INPUT_FIELDS
            if field != "schema"
        },
        "review_bundle": {
            field: inputs["lock_review_bundle"][field]
            for field in schema.SOURCE_REVIEW_INPUT_FIELDS
            if field != "schema"
        },
    }
    if (
        current_children.get("schema")
        != schema.SOURCE_REVIEW_CONTENT_SCHEMAS["current_children_attestation"]
        or current_children.get("protocol") != schema.PROTOCOL
        or current_children.get("protocol_sha256") != schema.PROTOCOL_SHA256
        or current_children.get("status") != "ok"
        or current_children.get("tools_manifest_sha256")
        != inputs["tools_manifest"].get("sha256")
        or current_children.get("lock_manifest_sha256")
        != inputs["lock_manifest"].get("sha256")
        or current_children.get("review_bundle_sha256")
        != inputs["lock_review_bundle"].get("sha256")
        or current_children.get("lock_authority_inputs") != stripped_inputs
        or current_children.get("lock_authority") != lock_authority
    ):
        errors.append("terminal current-child reviewed input crosslinks differ")
    lock_manifest = lock_authority.get("lock_manifest")
    bound_lock_review = lock_authority.get("review_bundle")
    if (
        lock_authority.get("schema")
        != schema.SOURCE_REVIEW_CONTENT_SCHEMAS["lock_authority"]
        or lock_authority.get("status") != "approved"
        or lock_authority.get("protocol") != schema.PROTOCOL
        or lock_authority.get("protocol_sha256") != schema.PROTOCOL_SHA256
        or lock_authority.get("review_sha256")
        != inputs["lock_review_bundle"].get("sha256")
        or not isinstance(lock_manifest, Mapping)
        or lock_manifest.get("sha256") != inputs["lock_manifest"].get("sha256")
        or lock_manifest.get("schema") != "asterism-rebaseline-lock-candidates-v3"
        or not isinstance(lock_manifest.get("payload"), Mapping)
        or lock_manifest.get("payload", {}).get("schema")
        != "asterism-rebaseline-lock-candidates-v3"
        or not isinstance(bound_lock_review, Mapping)
        or bound_lock_review.get("sha256")
        != inputs["lock_review_bundle"].get("sha256")
        or bound_lock_review.get("schema")
        != schema.SOURCE_REVIEW_CONTENT_SCHEMAS["lock_review_bundle"]
        or bound_lock_review.get("payload") != lock_review_bundle
        or lock_review_bundle.get("schema")
        != schema.SOURCE_REVIEW_CONTENT_SCHEMAS["lock_review_bundle"]
    ):
        errors.append("terminal prepared lock authority differs from source review")

    sentinel = "fa2acb626f303f8a65a16a6c8a1fd86b7e80cf48e092ae21a7308984ae790c94"
    expected_approval = {
        "final_integration_action": (
            "repeat-release-equality-proof-under-real-source-approval"
        ),
        "source_approval_sha256": sentinel,
        "source_approval_status": "preapproval-sentinel-not-source-approved",
    }
    preapproval = current_children.get("release_compile_out")
    overlay_authority = current_children.get("product_overlay_authority")
    patch = (
        overlay_authority.get("patch")
        if isinstance(overlay_authority, Mapping)
        else None
    )
    if (
        current_children.get("release_compile_out_approval") != expected_approval
        or not isinstance(preapproval, Mapping)
        or requirement is None
        or hashlib.sha256(canonical_json_bytes(preapproval)).hexdigest()
        != requirement.get("preapproval_compile_out_sha256")
        or not isinstance(patch, Mapping)
        or patch.get("sha256") != requirement.get("product_overlay_sha256")
    ):
        errors.append("terminal current-child preapproval authority differs")
    approved_tools = approval.get("tools_manifest", {}).get("tools", {})
    artifacts = current_children.get("artifacts")
    if not isinstance(artifacts, Mapping) or any(
        artifacts.get(name) != approved_tools.get(name)
        for name in ("correctness", "fault")
    ):
        errors.append("terminal current-child tool artifacts differ")
    if b"/asterism/preapproval-placeholder/" in canonical_json_bytes(
        approval.get("tools_manifest", {})
    ):
        errors.append("terminal approved tools retain a preapproval placeholder")
    return assertion


def proc_identity(pid: int) -> dict[str, Any]:
    payload = (Path("/proc") / str(pid) / "stat").read_text()
    closed = payload.rfind(")")
    opened = payload.find("(")
    fields = payload[closed + 2 :].split()
    if opened < 0 or closed <= opened or len(fields) <= 19:
        raise ValueError("malformed proc stat")
    return {
        "pid": pid,
        "comm": payload[opened + 1 : closed],
        "state": fields[0],
        "ppid": int(fields[1]),
        "pgrp": int(fields[2]),
        "session": int(fields[3]),
        "starttime_ticks": int(fields[19]),
    }


def runner_cmdline_matches(
    recorded: Any,
    runtime_path: Any,
    support_path: Any,
    observed: list[bytes] | None = None,
) -> bool:
    """Validate the complete runner argv, optionally against live /proc bytes."""

    if (
        not isinstance(recorded, list)
        or len(recorded) < 2
        or not all(isinstance(item, str) and item for item in recorded)
        or recorded[:2] != [runtime_path, support_path]
    ):
        return False
    return observed is None or observed == [item.encode() for item in recorded]


def validate_live_terminal_invocation(
    output_dir: Path,
    prepared: Mapping[str, Any] | None,
    terminal: Mapping[str, Any] | None,
    errors: list[str],
) -> None:
    if prepared is None or terminal is None:
        errors.append("cannot bind live terminal verifier without prepared/terminal records")
        return
    tools = prepared.get("tools", {})
    support_files = prepared.get("support_files", {})
    runtime = tools.get("terminal_verifier_runtime", {})
    support = support_files.get("terminal_verifier", {})
    if set(runtime) != set(schema.TOOL_BINDING_FIELDS):
        errors.append("terminal verifier runtime binding fields are not exact")
        return
    if set(support) != set(schema.SUPPORT_FILE_FIELDS):
        errors.append("terminal verifier support binding fields are not exact")
        return
    runtime_path = Path(str(runtime.get("path", "")))
    support_path = Path(str(support.get("path", "")))
    bound_file(
        runtime.get("path"), runtime.get("sha256"), runtime_path,
        "live terminal runtime", errors, expected_mode=0o555,
    )
    bound_file(support.get("path"), support.get("sha256"), support_path, "live terminal support", errors)
    try:
        if Path("/proc/self/exe").resolve(strict=True) != runtime_path.resolve(strict=True):
            errors.append("live terminal /proc/self/exe differs from prepared runtime")
        if stat.S_IMODE(runtime_path.stat().st_mode) != runtime.get("executable_mode"):
            errors.append("live terminal runtime mode differs")
        if Path("/proc/self/comm").read_text().strip() != runtime.get("comm"):
            errors.append("live terminal comm differs from prepared runtime")
        if Path(__file__).resolve(strict=True) != support_path.resolve(strict=True):
            errors.append("live terminal script differs from prepared support")
        if stat.S_IMODE(support_path.stat().st_mode) != support.get("mode"):
            errors.append("live terminal support mode differs")
        cmdline = Path("/proc/self/cmdline").read_bytes().rstrip(b"\0").split(b"\0")
        expected = [
            str(runtime_path).encode(), str(support_path).encode(), b"--verify",
            str(output_dir).encode(),
        ]
        if cmdline != expected:
            errors.append("live terminal cmdline is not exact")
    except OSError as error:
        errors.append(f"cannot replay live terminal identity: {error}")

    runner = terminal.get("runner")
    if not require_keys(runner, set(schema.TERMINAL_RUNNER_FIELDS), "terminal runner", errors):
        return
    identity = runner.get("identity")
    runner_runtime = runner.get("runtime")
    runner_support = runner.get("support")
    if not require_keys(identity, set(schema.PROCESS_IDENTITY_FIELDS), "terminal runner identity", errors):
        return
    require_keys(runner_runtime, set(schema.TERMINAL_RUNTIME_FIELDS), "terminal runner runtime", errors)
    require_keys(runner_support, set(schema.TERMINAL_SUPPORT_FIELDS), "terminal runner support", errors)
    prepared_runtime = tools.get("runner_runtime", {})
    prepared_support = support_files.get("runner", {})
    if runner_runtime != {
        "path": prepared_runtime.get("path"), "sha256": prepared_runtime.get("sha256"),
        "mode": prepared_runtime.get("executable_mode"), "comm": prepared_runtime.get("comm"),
    }:
        errors.append("terminal runner runtime differs from prepared binding")
    if runner_support != prepared_support:
        errors.append("terminal runner support differs from prepared binding")
    parent = os.getppid()
    try:
        live = proc_identity(parent)
        if (live.get("pid"), live.get("starttime_ticks"), live.get("comm")) != (
            identity.get("pid"), identity.get("starttime_ticks"), identity.get("comm")
        ):
            errors.append("live parent runner PID/start/comm differs from terminal")
        parent_exe = (Path("/proc") / str(parent) / "exe").resolve(strict=True)
        if parent_exe != Path(str(runner_runtime.get("path"))).resolve(strict=True):
            errors.append("live parent runner runtime path differs")
        if sha256_file(parent_exe) != runner_runtime.get("sha256"):
            errors.append("live parent runner runtime hash differs")
        parent_cmdline = (Path("/proc") / str(parent) / "cmdline").read_bytes().rstrip(b"\0").split(b"\0")
        recorded_cmdline = runner.get("cmdline")
        if not runner_cmdline_matches(
            recorded_cmdline,
            runner_runtime.get("path"),
            runner_support.get("path"),
            parent_cmdline,
        ):
            errors.append("live parent runner cmdline differs from exact terminal record")
    except (OSError, ValueError) as error:
        errors.append(f"cannot replay live parent runner identity: {error}")


def current_inventory(output_dir: Path, errors: list[str]) -> list[dict[str, Any]]:
    try:
        entries, snapshots = schema.artifact_inventory(
            output_dir, excluded_names=EXCLUDED_INVENTORY
        )
    except (OSError, ValueError) as error:
        errors.append(f"cannot enumerate terminal inventory: {error}")
        return []
    for relative, snapshot in snapshots.items():
        path = output_dir / relative
        previous = _BOUND_SNAPSHOTS.get(path)
        if previous is not None and (
            previous.device,
            previous.inode,
            previous.sha256,
            previous.size,
            previous.mode,
        ) != (
            snapshot.device,
            snapshot.inode,
            snapshot.sha256,
            snapshot.size,
            snapshot.mode,
        ):
            errors.append(
                f"terminal artifact {relative} changed after its semantic snapshot"
            )
            continue
        _BOUND_SNAPSHOTS[path] = snapshot
    return entries


def validate_terminal_release_cargo_config(
    attestation: Mapping[str, Any], context: str, errors: list[str]
) -> str | None:
    binding = terminal_authority_object(
        attestation.get("cargo_config_search"),
        schema.FILE_BINDING_FIELDS,
        f"{context} Cargo config binding",
        errors,
    )
    if binding is None or not terminal_is_sha256(binding.get("sha256")):
        errors.append(f"{context} Cargo config manifest hash is invalid")
        return None
    manifest_path = Path(str(binding.get("path", "")))
    snapshot = bound_file(
        binding.get("path"),
        binding.get("sha256"),
        manifest_path,
        f"{context} Cargo config manifest",
        errors,
    )
    manifest = (
        read_object(snapshot, f"{context} Cargo config manifest", errors)
        if snapshot is not None
        else None
    )
    if not isinstance(manifest, Mapping) or not require_keys(
        manifest,
        set(schema.CARGO_CONFIG_SEARCH_FIELDS),
        f"{context} Cargo config manifest",
        errors,
    ):
        return None
    if (
        manifest.get("schema") != schema.CARGO_CONFIG_SEARCH_SCHEMA
        or manifest.get("cwd") != GUEST_SOURCE
        or manifest.get("cargo_home_path") != GUEST_CARGO_HOME
    ):
        errors.append(f"{context} Cargo config guest identity differs")
    try:
        source_root = Path(str(attestation.get("materialized_root"))).resolve(
            strict=True
        )
        toolchain = attestation.get("toolchain")
        if not isinstance(toolchain, Mapping) or not isinstance(
            toolchain.get("cargo_home_path"), str
        ):
            raise ValueError("Cargo home authority is absent")
        cargo_home = Path(toolchain["cargo_home_path"]).resolve(strict=True)
    except (OSError, TypeError, ValueError) as error:
        errors.append(f"{context} Cargo config host roots differ: {error}")
        return None
    candidates: tuple[tuple[str, Path | None], ...] = (
        (f"{GUEST_SOURCE}/.cargo/config.toml", source_root / ".cargo/config.toml"),
        (f"{GUEST_SOURCE}/.cargo/config", source_root / ".cargo/config"),
        (f"{GUEST_ROOT}/.cargo/config.toml", None),
        (f"{GUEST_ROOT}/.cargo/config", None),
        ("/.cargo/config.toml", None),
        ("/.cargo/config", None),
        (f"{GUEST_CARGO_HOME}/config.toml", cargo_home / "config.toml"),
        (f"{GUEST_CARGO_HOME}/config", cargo_home / "config"),
    )
    entries = manifest.get("entries")
    if not isinstance(entries, list) or len(entries) != len(candidates):
        errors.append(f"{context} Cargo config candidate cardinality differs")
        return None
    for ordinal, (entry_value, (guest_path, host_path)) in enumerate(
        zip(entries, candidates, strict=True), start=1
    ):
        entry = terminal_authority_object(
            entry_value,
            schema.CARGO_CONFIG_SEARCH_ENTRY_FIELDS,
            f"{context} Cargo config entry {ordinal}",
            errors,
        )
        if entry is None:
            continue
        if entry.get("path") != guest_path:
            errors.append(f"{context} Cargo config guest path/order differs")
        if host_path is None:
            if entry.get("status") != "absent" or entry.get("sha256") is not None:
                errors.append(f"{context} private Cargo config path is not absent")
            continue
        if host_path.is_symlink():
            errors.append(f"{context} Cargo config host input is a symlink")
            continue
        expected_sha256 = EMPTY_SHA256
        if host_path.exists():
            try:
                host_snapshot = schema.snapshot_regular_file(
                    host_path, expected_mode=None
                )
                _BOUND_SNAPSHOTS[host_path] = host_snapshot
                expected_sha256 = host_snapshot.sha256
            except (OSError, ValueError) as error:
                errors.append(f"{context} cannot snapshot Cargo config: {error}")
                continue
        if (
            entry.get("status") != "present"
            or entry.get("sha256") != expected_sha256
        ):
            errors.append(f"{context} effective Cargo config bytes differ")
    empty_path = manifest_path.with_name(f"{manifest_path.name}.empty")
    empty_snapshot = bound_file(
        str(empty_path),
        EMPTY_SHA256,
        empty_path,
        f"{context} empty Cargo config authority",
        errors,
    )
    if empty_snapshot is not None and empty_snapshot.size != 0:
        errors.append(f"{context} empty Cargo config authority is nonempty")
    return str(binding.get("sha256"))


def validate_terminal_release_sandbox(
    attestation: Mapping[str, Any], context: str, errors: list[str]
) -> str | None:
    argv = attestation.get("build_argv")
    toolchain = attestation.get("toolchain")
    if (
        not isinstance(argv, list)
        or any(not isinstance(argument, str) for argument in argv)
        or not isinstance(toolchain, Mapping)
        or not isinstance(toolchain.get("bwrap_path"), str)
    ):
        errors.append(f"{context} sandbox authority is invalid")
        return None
    prefix = [
        toolchain["bwrap_path"],
        "--die-with-parent",
        "--new-session",
        "--unshare-net",
        "--ro-bind",
        "/",
        "/",
        "--dev-bind",
        "/dev",
        "/dev",
        "--proc",
        "/proc",
        "--tmpfs",
        "/tmp",
        "--tmpfs",
        GUEST_ROOT,
        "--dir",
        f"{GUEST_ROOT}/.cargo",
        "--tmpfs",
        f"{GUEST_ROOT}/.cargo",
        "--remount-ro",
        f"{GUEST_ROOT}/.cargo",
        "--dir",
        "/.cargo",
        "--tmpfs",
        "/.cargo",
        "--remount-ro",
        "/.cargo",
    ]
    core_bindings = (
        ("--ro-bind-fd", GUEST_SOURCE),
        ("--bind-fd", GUEST_TARGET),
        ("--ro-bind-fd", GUEST_TOOLCHAIN_ROOT),
        ("--ro-bind-fd", GUEST_CARGO),
        ("--ro-bind-fd", GUEST_RUSTC),
        ("--ro-bind-fd", GUEST_CARGO_HOME),
        ("--ro-bind-fd", GUEST_RUSTUP_HOME),
    )
    source_config = tuple(
        ("--ro-bind-fd", path) for path in GUEST_BOUND_CONFIG_PATHS[:2]
    )
    cargo_home_config = tuple(
        ("--ro-bind-fd", path) for path in GUEST_BOUND_CONFIG_PATHS[2:]
    )
    middle = ["--dir", f"{GUEST_SOURCE}/.cargo", "--tmpfs", f"{GUEST_SOURCE}/.cargo"]
    source_remount = ["--remount-ro", f"{GUEST_SOURCE}/.cargo"]
    cargo_home_remount = ["--remount-ro", GUEST_CARGO_HOME]
    suffix = [
        "--chdir",
        GUEST_SOURCE,
        GUEST_CARGO,
        "build",
        "--locked",
        "--offline",
        "--release",
        "-p",
        "mess-store",
        "--example",
        "asterism_rebaseline_public",
        "--target-dir",
        GUEST_TARGET,
    ]
    expected_length = (
        len(prefix)
        + 3 * len(core_bindings)
        + len(middle)
        + 3 * len(source_config)
        + len(source_remount)
        + 3 * len(cargo_home_config)
        + len(cargo_home_remount)
        + len(suffix)
    )
    if len(argv) != expected_length or argv[: len(prefix)] != prefix:
        errors.append(f"{context} sandbox prefix/cardinality differs")
        return None
    descriptors: list[str] = []
    normalized = list(argv)
    offset = len(prefix)

    def consume(bindings: Any, current: int) -> int | None:
        for operation, destination in bindings:
            segment = argv[current : current + 3]
            descriptor = segment[1] if len(segment) == 3 else ""
            if (
                len(segment) != 3
                or segment[0] != operation
                or segment[2] != destination
                or not descriptor.isascii()
                or not descriptor.isdecimal()
                or len(descriptor) > 10
                or str(int(descriptor)) != descriptor
                or int(descriptor) < 3
            ):
                errors.append(f"{context} sandbox binding differs: {destination}")
                return None
            descriptors.append(descriptor)
            normalized[current + 1] = f"$FD:{destination}"
            current += 3
        return current

    offset = consume(core_bindings, offset)
    if offset is None:
        return None
    if argv[offset : offset + len(middle)] != middle:
        errors.append(f"{context} private source Cargo config mount differs")
        return None
    offset += len(middle)
    offset = consume(source_config, offset)
    if offset is None:
        return None
    if argv[offset : offset + len(source_remount)] != source_remount:
        errors.append(f"{context} source Cargo config remount differs")
        return None
    offset += len(source_remount)
    offset = consume(cargo_home_config, offset)
    if offset is None:
        return None
    if argv[offset : offset + len(cargo_home_remount)] != cargo_home_remount:
        errors.append(f"{context} Cargo-home config remount differs")
        return None
    offset += len(cargo_home_remount)
    if len(set(descriptors)) != len(descriptors) or argv[offset:] != suffix:
        errors.append(f"{context} sandbox descriptors/command differ")
    if any(
        not isinstance(attestation.get(field), str) or attestation.get(field) in argv
        for field in ("materialized_root", "target_dir")
    ):
        errors.append(f"{context} sandbox exposes a mutable host path")
    build_child = attestation.get("build_child")
    if not isinstance(build_child, Mapping) or build_child.get("argv") != argv:
        errors.append(f"{context} child argv differs from sandbox authority")
    cargo_config_sha256 = validate_terminal_release_cargo_config(
        attestation, context, errors
    )
    if cargo_config_sha256 is None:
        return None
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "argv": normalized,
                "cargo_config_search_sha256": cargo_config_sha256,
            }
        )
    ).hexdigest()


def validate_terminal_release_file(
    value: Any,
    context: str,
    errors: list[str],
    *,
    expected_mode: int,
) -> tuple[Mapping[str, Any], schema.FileSnapshot] | None:
    record = terminal_authority_object(
        value, schema.RELEASE_COMPILE_OUT_FILE_FIELDS, context, errors
    )
    if record is None:
        return None
    path_value = record.get("path")
    if (
        not isinstance(path_value, str)
        or not Path(path_value).is_absolute()
        or not terminal_is_sha256(record.get("sha256"))
        or not terminal_is_integer(record.get("size"))
        or record.get("size", 0) <= 0
        or record.get("mode") != expected_mode
    ):
        errors.append(f"{context} file binding differs")
        return None
    path = Path(path_value)
    snapshot = bound_file(
        path_value,
        record.get("sha256"),
        path,
        context,
        errors,
        expected_mode=expected_mode,
    )
    if snapshot is None:
        return None
    expected_identity = {
        "changed_ns": snapshot._stat.st_ctime_ns,
        "device": snapshot.device,
        "inode": snapshot.inode,
        "link_count": snapshot._stat.st_nlink,
        "modified_ns": snapshot._stat.st_mtime_ns,
    }
    if (
        record.get("size") != snapshot.size
        or record.get("identity") != expected_identity
        or snapshot._stat.st_nlink != 1
    ):
        errors.append(f"{context} live size/identity differs")
    return record, snapshot


def validate_terminal_nm_child(
    value: Any,
    name: str,
    nm_tool: Mapping[str, Any],
    inventory: schema.FileSnapshot,
    errors: list[str],
) -> None:
    context = f"terminal release nm child {name}"
    child = terminal_authority_object(
        value, schema.RELEASE_COMPILE_OUT_NM_CHILD_FIELDS, context, errors
    )
    if child is None:
        return
    argv = child.get("argv")
    reaping = child.get("reaping")
    expected_prefix = [
        nm_tool.get("path"),
        "--defined-only",
        "--demangle=rust",
        "--format=posix",
    ]
    integer_fields = (
        "pid",
        "start_ticks",
        "waited_pid",
        "started_monotonic_ns",
        "completed_monotonic_ns",
    )
    if (
        child.get("exit_status") != 0
        or child.get("timed_out") is not False
        or child.get("process_group_absent") is not True
        or child.get("waited_pid") != child.get("pid")
        or not isinstance(argv, list)
        or len(argv) != 5
        or argv[:4] != expected_prefix
        or not isinstance(argv[4], str)
        or re.fullmatch(r"/proc/self/fd/[0-9]+", argv[4]) is None
        or reaping
        != {
            "pid": child.get("pid"),
            "start_ticks": child.get("start_ticks"),
            "status": "absent",
        }
        or any(
            not terminal_is_integer(child.get(field)) or child.get(field, 0) <= 0
            for field in integer_fields
        )
        or child.get("completed_monotonic_ns", 0)
        < child.get("started_monotonic_ns", 0)
    ):
        errors.append(f"{context} did not complete exactly")
    started_at = terminal_authority_timestamp(
        child.get("started_at"), f"{context} start", errors
    )
    completed_at = terminal_authority_timestamp(
        child.get("completed_at"), f"{context} completion", errors
    )
    if started_at is not None and completed_at is not None and completed_at < started_at:
        errors.append(f"{context} wall chronology differs")
    output_path = Path(str(child.get("output_path", "")))
    output_snapshot = bound_file(
        child.get("output_path"),
        child.get("output_sha256"),
        output_path,
        f"{context} output",
        errors,
    )
    output = (
        read_object(output_snapshot, f"{context} output", errors)
        if output_snapshot is not None
        else None
    )
    stdout = output.get("stdout") if isinstance(output, Mapping) else None
    stderr = output.get("stderr") if isinstance(output, Mapping) else None
    if (
        not isinstance(output, Mapping)
        or set(output)
        != {"exit_status", "stderr", "stderr_sha256", "stdout", "stdout_sha256"}
        or output.get("exit_status") != 0
        or not isinstance(stdout, str)
        or not isinstance(stderr, str)
        or stderr != ""
        or output.get("stderr_sha256") != hashlib.sha256(b"").hexdigest()
        or output.get("stdout_sha256")
        != hashlib.sha256(stdout.encode() if isinstance(stdout, str) else b"").hexdigest()
    ):
        errors.append(f"{context} output authority differs")
    if isinstance(stdout, str) and inventory.data != stdout.encode():
        errors.append(f"{context} stdout does not derive symbol inventory")


def validate_terminal_release_proof_semantics(
    proof: Mapping[str, Any],
    prepared: Mapping[str, Any],
    approval: Mapping[str, Any],
    current_children: Mapping[str, Any],
    config: Mapping[str, Any] | None,
    errors: list[str],
) -> str | None:
    """Independently replay the real-approval compile-out proof."""

    value = terminal_authority_object(
        proof,
        schema.RELEASE_COMPILE_OUT_FIELDS,
        "terminal release compile-out proof",
        errors,
    )
    if value is None:
        return None
    approval_sha256 = hashlib.sha256(canonical_json_bytes(approval)).hexdigest()
    requirement = approval.get("source_review", {}).get(
        "release_compile_out_requirement"
    )
    requirement_sha256 = (
        hashlib.sha256(canonical_json_bytes(requirement)).hexdigest()
        if isinstance(requirement, Mapping)
        else None
    )
    current_sha256 = hashlib.sha256(canonical_json_bytes(current_children)).hexdigest()
    exact = {
        "schema": schema.RELEASE_COMPILE_OUT_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "status": "ok",
        "source_approval_sha256": approval_sha256,
        "requirement_sha256": requirement_sha256,
        "current_children_attestation_sha256": current_sha256,
        "product_overlay_sha256": (
            requirement.get("product_overlay_sha256")
            if isinstance(requirement, Mapping)
            else None
        ),
        "forbidden_hook_strings": list(
            schema.RELEASE_COMPILE_OUT_FORBIDDEN_HOOK_STRINGS
        ),
        "binary_byte_identical": True,
        "symbol_inventory_byte_identical": True,
        "forbidden_hook_strings_absent": True,
    }
    if any(value.get(field) != expected for field, expected in exact.items()):
        errors.append("terminal release compile-out top-level authority differs")

    equivalence = terminal_authority_object(
        value.get("equivalence_contract"),
        schema.RELEASE_COMPILE_OUT_EQUIVALENCE_CONTRACT_FIELDS,
        "terminal release equivalence contract",
        errors,
    )
    if equivalence is None:
        return None
    if (
        equivalence.get("source_approval_sha256") != approval_sha256
        or equivalence.get("cfg_test") is not False
        or equivalence.get("rustc_workspace_wrapper") != "absent"
        or equivalence.get("ordinary_a_role") != "published"
        or equivalence.get("overlay_a_role") != "proof_only"
    ):
        errors.append("terminal release equivalence policy differs")
    for field in (
        "contract_sha256",
        "build_nonce",
        "cargo_lock_sha256",
        "toolchain_sha256",
        "build_environment_sha256",
        "sandbox_sha256",
    ):
        if not terminal_is_sha256(equivalence.get(field)):
            errors.append(f"terminal release equivalence {field} is invalid")

    builds = value.get("builds")
    if not isinstance(builds, Mapping) or set(builds) != set(
        schema.RELEASE_COMPILE_OUT_BUILD_NAMES
    ):
        errors.append("terminal release build names differ")
        return None
    prepared_a = prepared.get("variants", {}).get("A", {})
    prepared_attestation = prepared_a.get("attestation")
    common_fields = (
        "source_approval_sha256",
        "contract_sha256",
        "build_nonce",
        "cargo_lock_sha256",
        "toolchain_sha256",
        "build_environment_sha256",
        "sandbox_sha256",
        "cfg_test",
        "rustc_workspace_wrapper",
    )
    for name in schema.RELEASE_COMPILE_OUT_BUILD_NAMES:
        context = f"terminal release build {name}"
        build = terminal_authority_object(
            builds[name], schema.RELEASE_COMPILE_OUT_BUILD_FIELDS, context, errors
        )
        if build is None:
            continue
        attestation_fields = (
            schema.RELEASE_COMPILE_OUT_ORDINARY_ATTESTATION_FIELDS
            if name == "ordinary_a"
            else schema.RELEASE_COMPILE_OUT_OVERLAY_ATTESTATION_FIELDS
        )
        attestation = terminal_authority_object(
            build.get("attestation"),
            attestation_fields,
            f"{context} attestation",
            errors,
        )
        if attestation is None:
            continue
        expected_role = "published" if name == "ordinary_a" else "proof_only"
        if (
            build.get("role") != name
            or build.get("artifact_role") != expected_role
            or any(build.get(field) != equivalence.get(field) for field in common_fields)
            or build.get("attestation_sha256")
            != hashlib.sha256(canonical_json_bytes(attestation)).hexdigest()
        ):
            errors.append(f"{context} equivalence binding differs")
        build_env = attestation.get("build_env")
        toolchain = attestation.get("toolchain")
        sandbox_sha256 = validate_terminal_release_sandbox(
            attestation, context, errors
        )
        if (
            not isinstance(build_env, Mapping)
            or not isinstance(toolchain, Mapping)
            or attestation.get("build_nonce") != build.get("build_nonce")
            or attestation.get("cargo_lock_sha256") != build.get("cargo_lock_sha256")
            or hashlib.sha256(canonical_json_bytes(toolchain)).hexdigest()
            != build.get("toolchain_sha256")
            or hashlib.sha256(canonical_json_bytes(build_env)).hexdigest()
            != build.get("build_environment_sha256")
            or sandbox_sha256 != build.get("sandbox_sha256")
            or build_env.get("ASTERISM_BUILD_SOURCE_APPROVAL_SHA256")
            != approval_sha256
            or build_env.get("CARGO_HOME") != GUEST_CARGO_HOME
            or build_env.get("RUSTC") != GUEST_RUSTC
            or build_env.get("RUSTUP_HOME") != GUEST_RUSTUP_HOME
            or build_env.get("PATH")
            != f"{GUEST_TOOLCHAIN_ROOT}/bin:/usr/bin:/bin"
            or any(
                field in build_env
                for field in (
                    "RUSTC_WORKSPACE_WRAPPER",
                    "RUSTC_WRAPPER",
                    "RUSTFLAGS",
                    "CARGO_ENCODED_RUSTFLAGS",
                )
            )
        ):
            errors.append(f"{context} embedded authority differs")
        if name == "ordinary_a" and attestation != prepared_attestation:
            errors.append("terminal ordinary A attestation differs from prepared A")
        if isinstance(prepared_attestation, Mapping) and (
            attestation.get("build_env") != prepared_attestation.get("build_env")
            or attestation.get("toolchain") != prepared_attestation.get("toolchain")
        ):
            errors.append(f"{context} environment/toolchain differs from prepared A")
        contract_path = Path(str(attestation.get("contract_output_path", "")))
        contract_snapshot = bound_file(
            attestation.get("contract_output_path"),
            attestation.get("contract_output_sha256"),
            contract_path,
            f"{context} contract output",
            errors,
        )
        contract = (
            read_object(contract_snapshot, f"{context} contract output", errors)
            if contract_snapshot is not None
            else None
        )
        if (
            contract != prepared_a.get("contract")
            or not isinstance(contract, Mapping)
            or hashlib.sha256(canonical_json_bytes(contract)).hexdigest()
            != build.get("contract_sha256")
        ):
            errors.append(f"{context} contract replay differs")
        if name == "ordinary_a" and "product_overlay_sha256" in attestation:
            errors.append("terminal ordinary A contains product-overlay authority")
        if name == "overlay_a" and (
            not isinstance(requirement, Mapping)
            or attestation.get("product_overlay_sha256")
            != requirement.get("product_overlay_sha256")
        ):
            errors.append("terminal proof-only overlay A authority differs")

    binaries = value.get("binaries")
    inventories = value.get("symbol_inventories")
    if not isinstance(binaries, Mapping) or set(binaries) != set(
        schema.RELEASE_COMPILE_OUT_BUILD_NAMES
    ):
        errors.append("terminal release binary names differ")
        return None
    if not isinstance(inventories, Mapping) or set(inventories) != set(
        schema.RELEASE_COMPILE_OUT_BUILD_NAMES
    ):
        errors.append("terminal release inventory names differ")
        return None
    binary_records: dict[str, tuple[Mapping[str, Any], schema.FileSnapshot]] = {}
    inventory_records: dict[str, tuple[Mapping[str, Any], schema.FileSnapshot]] = {}
    for name in schema.RELEASE_COMPILE_OUT_BUILD_NAMES:
        binary = validate_terminal_release_file(
            binaries[name], f"terminal release binary {name}", errors,
            expected_mode=0o555,
        )
        inventory = validate_terminal_release_file(
            inventories[name], f"terminal release inventory {name}", errors,
            expected_mode=0o444,
        )
        if binary is not None:
            binary_records[name] = binary
        if inventory is not None:
            inventory_records[name] = inventory
    for records, context in (
        (binary_records, "terminal release binaries"),
        (inventory_records, "terminal release inventories"),
    ):
        if set(records) != set(schema.RELEASE_COMPILE_OUT_BUILD_NAMES):
            continue
        first_record, first_snapshot = records["ordinary_a"]
        second_record, second_snapshot = records["overlay_a"]
        if (
            first_snapshot.data != second_snapshot.data
            or first_record.get("sha256") != second_record.get("sha256")
            or first_record.get("size") != second_record.get("size")
            or first_record.get("path") == second_record.get("path")
            or (first_snapshot.device, first_snapshot.inode)
            == (second_snapshot.device, second_snapshot.inode)
        ):
            errors.append(f"{context} are not equal physically-disjoint files")
    ordinary_binary = binary_records.get("ordinary_a")
    overlay_binary = binary_records.get("overlay_a")
    if ordinary_binary is not None and (
        value.get("published_a_sha256") != ordinary_binary[0].get("sha256")
        or value.get("published_a_sha256")
        != prepared_a.get("binary", {}).get("sha256")
        or ordinary_binary[0].get("path") != prepared_a.get("binary", {}).get("path")
    ):
        errors.append("terminal published ordinary A binding differs")
    for records in (binary_records, inventory_records):
        for _name, (_record, snapshot) in records.items():
            for forbidden in schema.RELEASE_COMPILE_OUT_FORBIDDEN_HOOK_STRINGS:
                if forbidden.encode() in snapshot.data:
                    errors.append(f"terminal release artifact contains {forbidden}")

    nm = terminal_authority_object(
        value.get("nm"), schema.RELEASE_COMPILE_OUT_NM_FIELDS,
        "terminal release nm proof", errors,
    )
    if nm is not None:
        preapproval = current_children.get("release_compile_out")
        preapproval_nm = (
            preapproval.get("nm") if isinstance(preapproval, Mapping) else None
        )
        expected_mode = (
            preapproval_nm.get("mode")
            if isinstance(preapproval_nm, Mapping)
            and terminal_is_integer(preapproval_nm.get("mode"))
            else -1
        )
        nm_tool_result = validate_terminal_release_file(
            nm.get("tool"), "terminal release nm tool", errors,
            expected_mode=expected_mode,
        )
        if isinstance(preapproval_nm, Mapping) and nm_tool_result is not None:
            expected_tool = {
                "identity": {
                    "changed_ns": preapproval_nm.get("ctime_ns"),
                    "device": preapproval_nm.get("device"),
                    "inode": preapproval_nm.get("inode"),
                    "link_count": preapproval_nm.get("link_count"),
                    "modified_ns": preapproval_nm.get("mtime_ns"),
                },
                "mode": preapproval_nm.get("mode"),
                "path": preapproval_nm.get("path"),
                "sha256": preapproval_nm.get("sha256"),
                "size": preapproval_nm.get("size"),
            }
            if nm_tool_result[0] != expected_tool:
                errors.append("terminal release nm tool differs from preapproval")
            for name in schema.RELEASE_COMPILE_OUT_BUILD_NAMES:
                inventory_result = inventory_records.get(name)
                if inventory_result is not None:
                    validate_terminal_nm_child(
                        nm.get(name), name, nm_tool_result[0], inventory_result[1], errors
                    )
        else:
            errors.append("terminal release preapproval nm authority is absent")

    overlay_path = (
        overlay_binary[0].get("path") if overlay_binary is not None else None
    )
    if not isinstance(overlay_path, str) or not overlay_path:
        errors.append("terminal proof-only overlay A path is absent")
        return None

    def reaches_overlay(item: Any) -> bool:
        if isinstance(item, Mapping):
            return any(reaches_overlay(child) for child in item.values())
        if isinstance(item, (list, tuple)):
            return any(reaches_overlay(child) for child in item)
        return item == overlay_path

    if any(
        reaches_overlay(prepared.get(field))
        for field in ("variants", "tools", "support_files")
    ):
        errors.append("terminal proof-only overlay A is published/reachable")
    if isinstance(config, Mapping) and any(
        reaches_overlay(config.get(field))
        for field in ("smoke_transitions", "correctness_execution", "argv_templates")
    ):
        errors.append("terminal proof-only overlay A is config-reachable")
    return overlay_path


def validate_terminal_local_source_release(
    prepared: Mapping[str, Any],
    approval: Mapping[str, Any],
    prepared_path: Path,
    config: Mapping[str, Any] | None,
    errors: list[str],
) -> str | None:
    """Independently replay prepared source review and release authority."""

    try:
        root = prepared_path.parent.resolve(strict=True)
    except OSError as error:
        errors.append(f"terminal local prepared root is invalid: {error}")
        return None
    if (
        prepared.get("schema") != schema.PREPARED_ARTIFACTS_SCHEMA
        or prepared.get("protocol") != schema.PROTOCOL
        or prepared.get("protocol_sha256") != schema.PROTOCOL_SHA256
        or prepared.get("tooling_commit") != approval.get("tooling_commit")
        or prepared.get("tooling_tree") != approval.get("tooling_tree")
        or prepared.get("build_order") != list(schema.VARIANTS)
    ):
        errors.append("terminal local prepared identity/order differs")
    approval_binding = terminal_authority_object(
        prepared.get("source_approval"),
        schema.FILE_BINDING_FIELDS,
        "terminal local prepared source approval",
        errors,
    )
    expected_approval = root.joinpath(*schema.PREPARED_SOURCE_APPROVAL_RELATIVE_PATH)
    if approval_binding is not None:
        approval_snapshot = bound_file(
            approval_binding.get("path"),
            approval_binding.get("sha256"),
            expected_approval,
            "terminal local source approval copy",
            errors,
        )
        copied_approval = (
            read_object(
                approval_snapshot, "terminal local source approval copy", errors
            )
            if approval_snapshot is not None
            else None
        )
        if copied_approval != approval:
            errors.append("terminal local source approval copy differs")

    review_bindings = terminal_authority_object(
        prepared.get("source_review"),
        schema.PREPARED_SOURCE_REVIEW_FIELDS,
        "terminal local prepared source review",
        errors,
    )
    if review_bindings is None:
        return None
    snapshots: dict[str, schema.FileSnapshot] = {}
    payloads: dict[str, Mapping[str, Any]] = {}
    source_review = approval.get("source_review")
    if not isinstance(source_review, Mapping):
        errors.append("terminal local source-review approval is absent")
        return None
    for name, relative in schema.PREPARED_SOURCE_REVIEW_RELATIVE_PATHS.items():
        context = f"terminal local prepared source review {name}"
        binding = terminal_authority_object(
            review_bindings.get(name),
            schema.PREPARED_SOURCE_REVIEW_BINDING_FIELDS,
            context,
            errors,
        )
        if binding is None:
            continue
        expected_path = root / relative
        if binding.get("mode") != schema.ARTIFACT_FILE_MODE:
            errors.append(f"{context} mode differs")
        snapshot = bound_file(
            binding.get("path"),
            binding.get("sha256"),
            expected_path,
            context,
            errors,
        )
        payload = (
            read_object(snapshot, context, errors) if snapshot is not None else None
        )
        approved_binding = source_review.get(name)
        if (
            not isinstance(approved_binding, Mapping)
            or snapshot is None
            or snapshot.sha256 != approved_binding.get("sha256")
            or not isinstance(payload, Mapping)
            or payload.get("schema") != approved_binding.get("schema")
        ):
            errors.append(f"{context} differs from source approval")
            continue
        snapshots[name] = snapshot
        payloads[name] = payload
    if set(payloads) != set(schema.PREPARED_SOURCE_REVIEW_FIELDS):
        errors.append("terminal local prepared source-review copies are incomplete")
        return None
    assertion = validate_terminal_source_review_semantics(
        approval,
        payloads["bundle"],
        payloads["current_children_attestation"],
        payloads["lock_authority"],
        payloads["lock_review_bundle"],
        errors,
    )
    if assertion is None:
        return None
    inputs = assertion.get("inputs")
    if isinstance(inputs, Mapping):
        for name in (
            "current_children_attestation",
            "lock_authority",
            "lock_review_bundle",
        ):
            if snapshots[name].sha256 != inputs.get(name, {}).get("sha256"):
                errors.append(f"terminal local prepared source review {name} differs")
        if inputs.get("tools_manifest", {}).get("sha256") != approval.get(
            "tools_manifest_sha256"
        ):
            errors.append("terminal local reviewed tools manifest differs")

    proof_binding = terminal_authority_object(
        prepared.get("release_compile_out"),
        schema.RELEASE_COMPILE_OUT_BINDING_FIELDS,
        "terminal local release compile-out binding",
        errors,
    )
    if proof_binding is None:
        return None
    if proof_binding.get("mode") != schema.ARTIFACT_FILE_MODE:
        errors.append("terminal local release compile-out mode differs")
    proof_path = root / schema.RELEASE_COMPILE_OUT_RELATIVE_PATH
    proof_snapshot = bound_file(
        proof_binding.get("path"),
        proof_binding.get("sha256"),
        proof_path,
        "terminal local release compile-out proof",
        errors,
    )
    proof = (
        read_object(proof_snapshot, "terminal local release compile-out proof", errors)
        if proof_snapshot is not None
        else None
    )
    if not isinstance(proof, Mapping):
        return None
    return validate_terminal_release_proof_semantics(
        proof,
        prepared,
        approval,
        payloads["current_children_attestation"],
        config,
        errors,
    )


def validate_terminal_tools_authority(
    output_dir: Path,
    prepared: Mapping[str, Any] | None,
    errors: list[str],
) -> str | None:
    prepared_path = output_dir / "prepared-artifacts.json"
    approval_path = output_dir / "source-approval.json"
    approval = read_object(
        approval_path, "terminal source approval", errors
    )
    provenance = read_object(
        output_dir / "provenance.json", "terminal authority provenance", errors
    )
    if not isinstance(prepared, Mapping) or not isinstance(approval, Mapping):
        errors.append("terminal tools authority is unavailable")
        return
    if not require_keys(
        approval,
        set(schema.SOURCE_APPROVAL_FIELDS),
        "terminal source approval",
        errors,
    ) or not require_keys(
        prepared,
        set(schema.PREPARED_FIELDS),
        "terminal prepared artifacts",
        errors,
    ):
        return
    if (
        approval.get("schema") != schema.SOURCE_APPROVAL_SCHEMA
        or approval.get("protocol") != schema.PROTOCOL
        or approval.get("protocol_sha256") != schema.PROTOCOL_SHA256
        or approval.get("status") != "approved"
        or prepared.get("schema") != schema.PREPARED_ARTIFACTS_SCHEMA
        or prepared.get("protocol") != schema.PROTOCOL
        or prepared.get("protocol_sha256") != schema.PROTOCOL_SHA256
    ):
        errors.append("terminal source/prepared identity differs")
    try:
        schema.validate_source_approval(dict(approval))
    except (OSError, ValueError) as error:
        errors.append(f"terminal shared source approval authority replay: {error}")

    prepared_root: Path | None = None
    original_prepared_snapshot: schema.FileSnapshot | None = None
    attempt_prepared_snapshot = _BOUND_SNAPSHOTS.get(prepared_path)
    attempt_approval_snapshot = _BOUND_SNAPSHOTS.get(approval_path)
    claim_binding = prepared.get("single_use_claim")
    claim: Mapping[str, Any] | None = None
    if require_keys(
        claim_binding,
        {"path"},
        "terminal prepared single-use claim binding",
        errors,
    ):
        claim_path = Path(str(claim_binding.get("path")))
        claim_value = read_object(
            claim_path, "terminal prepared single-use claim", errors
        )
        claim = claim_value if isinstance(claim_value, Mapping) else None
        try:
            claims_directory = claim_path.parent.resolve(strict=True)
            prepared_root = claims_directory.parent.resolve(strict=True)
            if (
                claims_directory.name != "claims"
                or claim_path != claims_directory / "single-use-claim.json"
                or stat.S_IMODE(claims_directory.stat().st_mode) != 0o700
                or stat.S_IMODE(prepared_root.stat().st_mode) != 0o555
            ):
                errors.append("terminal prepared claim/root layout differs")
        except OSError as error:
            errors.append(f"terminal prepared claim/root replay failed: {error}")
    if claim is not None and require_keys(
        claim,
        set(schema.PREPARED_CLAIM_FIELDS),
        "terminal prepared single-use claim",
        errors,
    ):
        expected_original_prepared = (
            prepared_root / "prepared-artifacts.json"
            if prepared_root is not None
            else None
        )
        if (
            claim.get("schema") != schema.PREPARED_CLAIM_SCHEMA
            or claim.get("protocol") != schema.PROTOCOL
            or expected_original_prepared is None
            or claim.get("prepared_artifacts_path")
            != str(expected_original_prepared)
        ):
            errors.append("terminal claim original prepared path differs")
        else:
            original_prepared_snapshot = bound_file(
                claim.get("prepared_artifacts_path"),
                claim.get("prepared_artifacts_sha256"),
                expected_original_prepared,
                "terminal original prepared artifacts",
                errors,
            )
            if (
                original_prepared_snapshot is not None
                and attempt_prepared_snapshot is not None
                and original_prepared_snapshot.data
                != attempt_prepared_snapshot.data
            ):
                errors.append("terminal attempt/original prepared bytes differ")
            if (
                original_prepared_snapshot is not None
                and attempt_prepared_snapshot is not None
                and (
                    original_prepared_snapshot.device,
                    original_prepared_snapshot.inode,
                )
                == (
                    attempt_prepared_snapshot.device,
                    attempt_prepared_snapshot.inode,
                )
            ):
                errors.append(
                    "terminal attempt/original prepared are hardlink aliases"
                )
            if (
                original_prepared_snapshot is not None
                and read_object(
                    original_prepared_snapshot,
                    "terminal original prepared artifacts",
                    errors,
                )
                != prepared
            ):
                errors.append("terminal attempt/original prepared objects differ")
        attempt_nonce = (
            provenance.get("attempt_nonce")
            if isinstance(provenance, Mapping)
            else None
        )
        lease = (
            provenance.get("lease")
            if isinstance(provenance, Mapping)
            and isinstance(provenance.get("lease"), Mapping)
            else None
        )
        if claim.get("output_dir") != str(output_dir):
            errors.append("terminal claim output differs")
        if claim.get("attempt_nonce") != attempt_nonce:
            errors.append("terminal claim attempt nonce differs")
        if lease is None or claim.get("lease_nonce") != lease.get("nonce"):
            errors.append("terminal claim lease nonce differs")
        prepared_created_at = parse_timestamp(
            prepared.get("created_at"),
            "terminal prepared created_at",
            errors,
        )
        lease_acquired_at = (
            parse_timestamp(
                lease.get("acquired_at"),
                "terminal claim lease acquired_at",
                errors,
            )
            if lease is not None
            else None
        )
        claimed_at = parse_timestamp(
            claim.get("claimed_at"), "terminal claim claimed_at", errors
        )
        chronology = (
            prepared.get("created_monotonic_ns"),
            lease.get("acquired_monotonic_ns") if lease is not None else None,
            claim.get("claimed_monotonic_ns"),
        )
        if (
            prepared_created_at is None
            or lease_acquired_at is None
            or claimed_at is None
            or not prepared_created_at <= lease_acquired_at <= claimed_at
            or not all(
                isinstance(value, int)
                and not isinstance(value, bool)
                and value > 0
                for value in chronology
            )
            or not chronology[0] < chronology[1] < chronology[2]
        ):
            errors.append("terminal claim chronology differs")

    local_overlay_path: str | None = None
    if original_prepared_snapshot is not None:
        try:
            schema.validate_prepared_artifacts(
                dict(prepared), dict(approval), original_prepared_snapshot.path
            )
        except (OSError, ValueError) as error:
            errors.append(
                "terminal shared source-review/release authority replay: "
                f"{error}"
            )
        config = read_object(
            output_dir / "config.json", "terminal local authority config", errors
        )
        local_overlay_path = validate_terminal_local_source_release(
            prepared,
            approval,
            original_prepared_snapshot.path,
            config,
            errors,
        )

    approval_binding = prepared.get("source_approval")
    if not require_keys(
        approval_binding,
        {"path", "sha256"},
        "terminal prepared source approval binding",
        errors,
    ):
        original_approval_snapshot = None
    else:
        expected_original_approval = (
            prepared_root.joinpath(*schema.PREPARED_SOURCE_APPROVAL_RELATIVE_PATH)
            if prepared_root is not None
            else None
        )
        original_approval_path = Path(str(approval_binding.get("path")))
        if (
            expected_original_approval is None
            or original_approval_path != expected_original_approval
        ):
            errors.append("terminal original source approval path differs")
        original_approval_snapshot = bound_file(
            approval_binding.get("path"),
            approval_binding.get("sha256"),
            original_approval_path,
            "terminal original source approval",
            errors,
        )
        if approval_binding.get("sha256") != sha256_file(approval_path):
            errors.append("terminal attempt/original source approval hash differs")
        if (
            original_approval_snapshot is not None
            and attempt_approval_snapshot is not None
            and original_approval_snapshot.data != attempt_approval_snapshot.data
        ):
            errors.append("terminal attempt/original source approval bytes differ")
        if (
            original_approval_snapshot is not None
            and attempt_approval_snapshot is not None
            and (
                original_approval_snapshot.device,
                original_approval_snapshot.inode,
            )
            == (
                attempt_approval_snapshot.device,
                attempt_approval_snapshot.inode,
            )
        ):
            errors.append(
                "terminal attempt/original source approval are hardlink aliases"
            )
        if (
            original_approval_snapshot is not None
            and read_object(
                original_approval_snapshot,
                "terminal original source approval",
                errors,
            )
            != approval
        ):
            errors.append("terminal attempt/original source approval objects differ")
    approved_variants = approval.get("variants")
    prepared_variants = prepared.get("variants")
    if (
        not isinstance(approved_variants, Mapping)
        or not isinstance(prepared_variants, Mapping)
        or set(approved_variants) != set(schema.VARIANTS)
        or set(prepared_variants) != set(schema.VARIANTS)
    ):
        errors.append("terminal source/prepared variant names differ")
    else:
        for variant in schema.VARIANTS:
            source_variant = approved_variants[variant]
            prepared_variant = prepared_variants[variant]
            source_context = f"terminal source variant {variant}"
            prepared_context = f"terminal prepared variant {variant}"
            if not require_keys(
                source_variant,
                set(schema.SOURCE_APPROVAL_VARIANT_FIELDS),
                source_context,
                errors,
            ) or not require_keys(
                prepared_variant,
                set(schema.PREPARED_VARIANT_FIELDS),
                prepared_context,
                errors,
            ):
                continue
            expected_lifetime: Any = (
                schema.PROFILE_C_ROLE_LIFETIME_CONTRACT
                if variant == "C"
                else "not_applicable"
            )
            expected_templates = schema.expected_trace_path_marker_templates(
                variant
            )
            if (
                source_variant.get("profile_role_lifetime") != expected_lifetime
                or prepared_variant.get("contract", {}).get(
                    "profile_role_lifetime"
                )
                != expected_lifetime
            ):
                errors.append(f"terminal variant {variant} role lifetime differs")
            if (
                source_variant.get("trace_path_marker_templates")
                != expected_templates
                or prepared_variant.get("trace_path_marker_templates")
                != expected_templates
                or prepared_variant.get("evidence_env")
                != schema.expected_trace_marker_environment(variant)
            ):
                errors.append(f"terminal variant {variant} trace templates differ")
            try:
                schema.validate_binary_contract(prepared_variant.get("contract", {}))
            except (KeyError, TypeError, ValueError) as error:
                errors.append(f"{prepared_context} binary contract invalid: {error}")
            binding = schema.VARIANT_SOURCE_BINDINGS[variant]
            contract = prepared_variant.get("contract", {})
            if (
                source_variant.get("product_commit") != binding["commit"]
                or source_variant.get("product_tree") != binding["tree"]
                or contract.get("product_commit") != binding["commit"]
                or contract.get("product_tree") != binding["tree"]
            ):
                errors.append(f"terminal variant {variant} source binding differs")
    manifest = approval.get("tools_manifest")
    claimed_sha256 = approval.get("tools_manifest_sha256")
    if not require_keys(
        manifest, set(schema.TOOLS_MANIFEST_FIELDS),
        "terminal approved tools manifest", errors,
    ):
        return
    observed_sha256 = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    if claimed_sha256 != observed_sha256:
        errors.append("terminal approved tools manifest canonical digest mismatch")
    if (
        manifest.get("schema") != schema.TOOLS_MANIFEST_SCHEMA
        or manifest.get("comm_allowlist") != prepared.get("comm_allowlist")
    ):
        errors.append("terminal approved tools manifest identity/allowlist mismatch")
    binding = prepared.get("tools_manifest")
    if not require_keys(
        binding, set(schema.TOOLS_MANIFEST_BINDING_FIELDS),
        "terminal prepared tools manifest binding", errors,
    ):
        return
    bound_path = Path(str(binding.get("path", "")))
    if binding.get("sha256") != claimed_sha256 or binding.get("mode") != 0o444:
        errors.append("terminal prepared tools manifest hash/mode claim mismatch")
    bound_snapshot = bound_file(
        binding.get("path"), binding.get("sha256"), bound_path,
        "terminal prepared tools manifest", errors,
    )
    if bound_snapshot is None:
        return
    if bound_path.parent.name != "bindings":
        errors.append("terminal prepared tools manifest file binding mismatch")
    if read_object(bound_snapshot, "terminal prepared tools manifest", errors) != manifest:
        errors.append("terminal prepared tools manifest differs from approval")
    for collection, fields in (
        ("tools", ("sha256", "executable_mode", "comm")),
        ("support_files", ("sha256", "mode")),
    ):
        approved_items = manifest.get(collection)
        prepared_items = prepared.get(collection)
        if (
            not isinstance(approved_items, Mapping)
            or not isinstance(prepared_items, Mapping)
            or set(approved_items) != set(prepared_items)
        ):
            errors.append(f"terminal prepared {collection} names differ from approval")
            continue
        for name, prepared_item in prepared_items.items():
            approved_item = approved_items.get(name, {})
            if any(prepared_item.get(field) != approved_item.get(field) for field in fields):
                errors.append(
                    f"terminal prepared {collection} {name} differs from approved claim"
                )
            item_path = Path(str(prepared_item.get("path", "")))
            bound_file(
                prepared_item.get("path"),
                prepared_item.get("sha256"),
                item_path,
                f"terminal prepared {collection} {name}",
                errors,
                expected_mode=(
                    0o555
                    if collection == "tools"
                    else schema.ARTIFACT_FILE_MODE
                ),
            )

    proof_binding = prepared.get("release_compile_out")
    if not isinstance(proof_binding, Mapping) or prepared_root is None:
        return None
    proof_path = prepared_root / schema.RELEASE_COMPILE_OUT_RELATIVE_PATH
    proof_snapshot = bound_file(
        proof_binding.get("path"),
        proof_binding.get("sha256"),
        proof_path,
        "terminal release compile-out proof",
        errors,
    )
    proof = (
        read_object(proof_snapshot, "terminal release compile-out proof", errors)
        if proof_snapshot is not None
        else None
    )
    overlay_path = (
        proof.get("binaries", {}).get("overlay_a", {}).get("path")
        if isinstance(proof, Mapping)
        else None
    )
    if not isinstance(overlay_path, str) or not overlay_path:
        errors.append("terminal proof-only overlay A path is absent")
        return None
    if local_overlay_path is not None and local_overlay_path != overlay_path:
        errors.append("terminal local/shared proof-only overlay paths differ")
    return local_overlay_path or overlay_path


def validate_terminal_proof_only_child_reachability(
    children: list[dict[str, Any]],
    overlay_path: str | None,
    errors: list[str],
) -> None:
    if overlay_path is None:
        return

    def reaches_overlay(value: Any) -> bool:
        if isinstance(value, Mapping):
            return any(reaches_overlay(item) for item in value.values())
        if isinstance(value, (list, tuple)):
            return any(reaches_overlay(item) for item in value)
        return value == overlay_path

    if any(reaches_overlay(child) for child in children):
        errors.append(
            "terminal proof-only overlay A is reachable from the completed child manifest"
        )


def validate_terminal_profile_contract(
    output_dir: Path, errors: list[str]
) -> None:
    value = read_object(
        output_dir / "profile-contract.json", "terminal profile contract", errors
    )
    if not require_keys(
        value,
        set(schema.PROFILE_PREFLIGHT_FIELDS),
        "terminal profile contract",
        errors,
    ):
        return
    exact = {
        "schema": schema.PROFILE_PREFLIGHT_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "profile_contract_sha256": schema.expected_profile_contract_sha256(),
        "source": "/proc/<pid>/task/<native-tid>/schedstat:first-field",
        "helper": "adapter-owned-cpu-bound-native-thread",
        "decision_multiplier": schema.SCHEDSTAT_DECISION_MULTIPLIER,
    }
    for field, expected in exact.items():
        if value.get(field) != expected:
            errors.append(f"terminal profile contract {field} mismatch")
    samples = value.get("samples_ns")
    if (
        not isinstance(samples, list)
        or len(samples) < 3
        or any(
            isinstance(item, bool) or not isinstance(item, int) or item < 0
            for item in samples
        )
        or any(after < before for before, after in zip(samples, samples[1:]))
    ):
        errors.append("terminal profile contract samples are invalid")
        return
    increments = [
        after - before
        for before, after in zip(samples, samples[1:])
        if after > before
    ]
    if not increments:
        errors.append("terminal profile contract lacks a nonzero increment")
        return
    resolution = min(increments)
    if value.get("minimum_nonzero_increment_ns") != resolution:
        errors.append("terminal profile contract resolution differs")
    if (
        value.get("decision_floor_ns")
        != resolution * schema.SCHEDSTAT_DECISION_MULTIPLIER
    ):
        errors.append("terminal profile contract decision floor differs")


def validate_terminal_profile_artifact(
    value: Any,
    output_dir: Path,
    context: str,
    errors: list[str],
) -> schema.FileSnapshot | None:
    if not require_keys(
        value,
        set(schema.PROFILE_RAW_ARTIFACT_BINDING_FIELDS),
        context,
        errors,
    ):
        return None
    path_value = value.get("path")
    path = Path(path_value) if isinstance(path_value, str) else Path()
    if (
        not isinstance(path_value, str)
        or not path.is_absolute()
        or path_value.startswith("//")
        or ".." in path.parts
        or str(path) != path_value
    ):
        errors.append(f"{context} path is not canonical absolute")
        return None
    try:
        path.relative_to(output_dir)
    except ValueError:
        errors.append(f"{context} escapes terminal output")
        return None
    snapshot = bound_file(
        value.get("path"),
        value.get("sha256"),
        path,
        context,
        errors,
        expected_mode=0o444,
    )
    if value.get("mode") != 0o444:
        errors.append(f"{context} mode authority differs")
    if snapshot is not None and value.get("bytes") != len(snapshot.data):
        errors.append(f"{context} byte length differs")
    return snapshot


def validate_terminal_result_artifacts(
    result: Mapping[str, Any] | None,
    output_dir: Path,
    errors: list[str],
    *,
    correctness_only: bool,
) -> None:
    """Bind immutable evaluator inputs before delegating evaluator-only semantics.

    Transition plan semantics are owned by the prepared-bound evaluator.  The
    terminal independently captures every result artifact here, then separately
    binds the canonical result bytes to the waited evaluator transition.
    """

    if not isinstance(result, Mapping):
        return
    names = schema.expected_result_artifact_names(
        correctness_only=correctness_only
    )
    artifacts = result.get("artifacts")
    if not require_keys(
        artifacts, set(names), "evaluation result artifacts", errors
    ):
        return
    for name in names:
        context = f"evaluation result artifact {name}"
        binding = artifacts[name]
        if not require_keys(binding, {"sha256", "bytes"}, context, errors):
            continue
        path = output_dir / name
        try:
            snapshot = _BOUND_SNAPSHOTS.get(path)
            if snapshot is None:
                snapshot = schema.snapshot_regular_file(
                    path, expected_mode=schema.ARTIFACT_FILE_MODE
                )
                _BOUND_SNAPSHOTS[path] = snapshot
        except (OSError, ValueError) as error:
            errors.append(f"cannot snapshot {context}: {error}")
            continue
        expected = {"sha256": snapshot.sha256, "bytes": snapshot.size}
        if binding != expected:
            errors.append(f"{context} differs from captured bytes")


def validate_terminal_profile_rich_authority(
    record: Mapping[str, Any],
    track: str,
    child_context: Mapping[str, Any],
    inputs: Mapping[str, Any],
    output_dir: Path,
    prepared: Mapping[str, Any] | None,
    approval: Mapping[str, Any] | None,
    attempt_nonce: Any,
    errors: list[str],
    *,
    context: str,
) -> None:
    """Project the evaluator's exact retained rich/profile authority checks."""

    rich = record.get("profile_rich_result")
    if not require_keys(
        rich,
        set(schema.PROFILE_RICH_RESULT_FIELDS),
        f"{context} rich profile result",
        errors,
    ):
        return
    authority = rich.get("authority")
    if not require_keys(
        authority,
        set(schema.PROFILE_AUTHORITY_FIELDS),
        f"{context} profile authority",
        errors,
    ):
        return
    identity = record.get("identity")
    identity = identity if isinstance(identity, Mapping) else {}
    environment = record.get("environment")
    environment = environment if isinstance(environment, Mapping) else {}
    control_fd_text = environment.get("ASTERISM_REBASELINE_CONTROL_FD")
    control_fd = (
        int(control_fd_text)
        if isinstance(control_fd_text, str)
        and control_fd_text.isascii()
        and control_fd_text.isdecimal()
        and int(control_fd_text) >= 3
        and str(int(control_fd_text)) == control_fd_text
        else None
    )
    variant = child_context.get("variant")
    source = schema.VARIANT_SOURCE_BINDINGS.get(str(variant))
    prepared_tools = prepared.get("tools") if isinstance(prepared, Mapping) else None
    expected_tool_names = (
        {"perf"}
        if track == "cpu_profiles"
        else {"strace", "strace_launcher_runtime"}
        if track in {"syscall_profiles", "structural_traces"}
        else set()
    )
    expected_tools = (
        {name: prepared_tools.get(name) for name in expected_tool_names}
        if isinstance(prepared_tools, Mapping)
        else None
    )
    support = (
        prepared.get("support_files")
        if isinstance(prepared, Mapping)
        else None
    )
    adapter = (
        support.get("profile_adapter") if isinstance(support, Mapping) else None
    )
    prepared_path = output_dir / "prepared-artifacts.json"
    approval_path = output_dir / "source-approval.json"
    exact = {
        "schema": schema.PROFILE_AUTHORITY_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "attempt_nonce": attempt_nonce,
        "child_ordinal": record.get("ordinal"),
        "row_ordinal": child_context.get("row_ordinal"),
        "context_sha256": record.get("context_sha256"),
        "prepared_artifacts_path": str(prepared_path),
        "prepared_artifacts_sha256": sha256_file(prepared_path),
        "source_approval_path": str(approval_path),
        "source_approval_sha256": sha256_file(approval_path),
        "profile_adapter_path": (
            adapter.get("path") if isinstance(adapter, Mapping) else None
        ),
        "profile_adapter_sha256": (
            adapter.get("sha256") if isinstance(adapter, Mapping) else None
        ),
        "profile_tools": expected_tools,
        "perf_permission_result": (
            inputs.get("perf_permission")
            if track == "cpu_profiles"
            else "not_applicable"
        ),
        "variant": variant,
        "source_commit": source.get("commit") if source is not None else None,
        "source_tree": source.get("tree") if source is not None else None,
        "track": track,
        "executable_path": record.get("executable_path"),
        "executable_sha256": record.get("executable_sha256"),
        "executable_mode": record.get("executable_mode"),
        "executable_comm": record.get("executable_comm"),
        "child_pid": identity.get("pid"),
        "child_start_ticks": identity.get("starttime_ticks"),
        "control_fd": control_fd,
    }
    for field, expected in exact.items():
        if authority.get(field) != expected:
            errors.append(f"{context} profile authority {field} differs")
    if (
        rich.get("schema") != schema.PROFILE_ADAPTER_SCHEMA
        or rich.get("protocol") != schema.PROTOCOL
        or rich.get("variant") != variant
        or rich.get("track") != track
        or rich.get("context") != child_context
    ):
        errors.append(f"{context} rich profile identity/context differs")
    if not isinstance(approval, Mapping):
        errors.append(f"{context} source approval is unavailable")


def terminal_profile_track(record: Mapping[str, Any]) -> str | None:
    context = record.get("context")
    return schema.profile_tool_track_for_child(
        record.get("kind"), context if isinstance(context, Mapping) else None
    )


def validate_terminal_child_projection(
    records: list[dict[str, Any]],
    output_dir: Path,
    errors: list[str],
    *,
    correctness_only: bool,
) -> None:
    """Replay terminal-critical child/profile provenance independently."""

    if correctness_only:
        return
    prepared = read_object(
        output_dir / "prepared-artifacts.json",
        "terminal child prepared artifacts",
        errors,
    )
    approval = read_object(
        output_dir / "source-approval.json",
        "terminal child source approval",
        errors,
    )
    provenance = read_object(
        output_dir / "provenance.json", "terminal child provenance", errors
    )
    attempt_nonce = provenance.get("attempt_nonce") if provenance else None
    try:
        profile_contract_sha256 = sha256_file(output_dir / "profile-contract.json")
    except (OSError, ValueError) as error:
        errors.append(f"cannot snapshot terminal profile contract: {error}")
        profile_contract_sha256 = None
    for ordinal, record in enumerate(records, start=1):
        context = f"terminal child {ordinal}"
        if not require_keys(record, EVALUATOR_CHILD_FIELDS, context, errors):
            continue
        if (
            record.get("schema") != schema.CHILD_SCHEMA
            or record.get("protocol") != schema.PROTOCOL
            or record.get("ordinal") != ordinal
        ):
            errors.append(f"{context} identity differs")
        child_context = record.get("context")
        if not isinstance(child_context, dict) or hashlib.sha256(
            canonical_json_bytes(child_context)
        ).hexdigest() != record.get("context_sha256"):
            errors.append(f"{context} context/hash differs")
            child_context = {}
        environment = record.get("environment")
        if not isinstance(environment, dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in (environment or {}).items()
        ):
            errors.append(f"{context} environment is invalid")
            environment = {}
        forbidden_markers = {
            "ASTERISM_REBASELINE_LOG_PATH_MARKERS",
            "ASTERISM_REBASELINE_METADATA_PATH_MARKERS",
        }
        if forbidden_markers & set(environment):
            errors.append(f"{context} inherited source-only trace marker templates")
        for field, hash_field in (
            ("control_events", "control_events_sha256"),
            ("profile_events", "profile_events_sha256"),
        ):
            payload = record.get(field)
            if not isinstance(payload, list) or hashlib.sha256(
                canonical_json_bytes(payload)
            ).hexdigest() != record.get(hash_field):
                errors.append(f"{context} {field} hash differs")
        row_child = record.get("kind") in schema.TRACK_EXECUTION_ORDER
        profile_result = record.get("profile_result")
        if row_child:
            if not isinstance(profile_result, dict) or hashlib.sha256(
                canonical_json_bytes(profile_result)
            ).hexdigest() != record.get("profile_result_sha256"):
                errors.append(f"{context} profile result/hash differs")
        elif (
            profile_result is not None
            or record.get("profile_result_sha256") is not None
        ):
            errors.append(f"{context} non-row profile result/hash is not null")
        if record.get("profile_contract_sha256") != profile_contract_sha256:
            errors.append(f"{context} profile contract hash differs")
        inputs = record.get("profile_tool_inputs")
        if not isinstance(inputs, dict):
            errors.append(f"{context} profile tool inputs are not an object")
            inputs = {}
        if hashlib.sha256(canonical_json_bytes(inputs)).hexdigest() != record.get(
            "profile_tool_inputs_sha256"
        ):
            errors.append(f"{context} profile tool input hash differs")
        track = terminal_profile_track(record)
        perf_environment = {
            name
            for name in environment
            if name.startswith("ASTERISM_REBASELINE_PERF_")
        }
        if track is None:
            if inputs:
                errors.append(f"{context} non-profile inputs are not empty")
            if perf_environment:
                errors.append(f"{context} non-profile child has perf environment")
            continue
        expected = set(schema.PROFILE_TOOL_INPUT_FIELDS_BY_TRACK[track])
        if set(inputs) != expected:
            errors.append(
                f"{context} {track} input keys differ; "
                f"missing={sorted(expected - set(inputs))} "
                f"extra={sorted(set(inputs) - expected)}"
            )
            continue
        if track != "cpu_profiles" and perf_environment:
            errors.append(f"{context} non-CPU child has perf environment")
        if record.get("kind") in schema.PROFILE_TOOL_INPUT_FIELDS_BY_TRACK:
            validate_terminal_profile_rich_authority(
                record,
                track,
                child_context,
                inputs,
                output_dir,
                prepared,
                approval,
                attempt_nonce,
                errors,
                context=context,
            )
        if track in {"primary", "new_names", "fairness", "cpu_profiles"}:
            resolution = inputs.get("schedstat_resolution_ns")
            if (
                isinstance(resolution, bool)
                or not isinstance(resolution, int)
                or resolution <= 0
            ):
                errors.append(f"{context} schedstat resolution is invalid")
        if track == "cpu_profiles":
            permission = inputs.get("perf_permission")
            try:
                status = schema.profile_perf_permission_status(permission)
            except ValueError as error:
                errors.append(f"{context} perf permission invalid: {error}")
                status = None
            if environment.get("ASTERISM_REBASELINE_PERF_PERMISSION_RESULT") != permission:
                errors.append(f"{context} perf permission/environment differs")
            events = inputs.get("perf_control_events")
            artifacts = inputs.get("perf_raw_artifacts")
            if not isinstance(events, list) or not isinstance(artifacts, dict):
                errors.append(f"{context} perf events/artifacts types differ")
                continue
            fd_names = (
                "ASTERISM_REBASELINE_PERF_COMMAND_FD",
                "ASTERISM_REBASELINE_PERF_ACK_FD",
                "ASTERISM_REBASELINE_PERF_ACK_LEDGER_FD",
            )
            control_events = record.get("control_events", [])
            start = next(
                (
                    event
                    for event in control_events
                    if isinstance(event, dict) and event.get("command") == "start"
                ),
                None,
            )
            measured = next(
                (
                    event
                    for event in control_events
                    if isinstance(event, dict) and event.get("phase") == "measured"
                ),
                None,
            )
            start_nonce = start.get("nonce") if isinstance(start, dict) else None
            if (
                not isinstance(start_nonce, str)
                or len(start_nonce) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in start_nonce
                )
                or not isinstance(measured, dict)
                or measured.get("nonce") != start_nonce
            ):
                errors.append(f"{context} CPU control nonce projection differs")
            if status == "available":
                expected_perf_environment = {
                    "ASTERISM_REBASELINE_PERF_PERMISSION_RESULT",
                    *fd_names,
                }
                if perf_environment != expected_perf_environment:
                    errors.append(f"{context} available perf environment differs")
                if len(events) != 2 or set(artifacts) != {"stat", "ack"}:
                    errors.append(f"{context} available perf evidence is incomplete")
                    continue
                rendered_fds = [environment.get(name) for name in fd_names]
                control_fd = environment.get("ASTERISM_REBASELINE_CONTROL_FD")
                if (
                    any(
                        not isinstance(value, str)
                        or not value.isascii()
                        or not value.isdecimal()
                        or int(value) < 3
                        or str(int(value)) != value
                        for value in rendered_fds
                    )
                    or len({control_fd, *rendered_fds}) != 4
                ):
                    errors.append(f"{context} inherited perf descriptors differ")
                normalized_events: list[dict[str, Any]] = []
                for index, (event, command) in enumerate(
                    zip(events, ("enable", "disable"), strict=True)
                ):
                    if not require_keys(
                        event,
                        {
                            "command",
                            "nonce",
                            "sent_monotonic_ns",
                            "ack",
                            "ack_received_monotonic_ns",
                        },
                        f"{context} perf event {index}",
                        errors,
                    ):
                        continue
                    if (
                        event.get("command") != command
                        or event.get("ack") != "ack"
                        or not isinstance(event.get("nonce"), str)
                        or len(event["nonce"]) != 64
                        or any(
                            character not in "0123456789abcdef"
                            for character in event["nonce"]
                        )
                        or not all(
                            isinstance(event.get(field), int)
                            and not isinstance(event.get(field), bool)
                            and event[field] >= 0
                            for field in (
                                "sent_monotonic_ns",
                                "ack_received_monotonic_ns",
                            )
                        )
                        or event.get("ack_received_monotonic_ns", 0)
                        <= event.get("sent_monotonic_ns", 0)
                    ):
                        errors.append(f"{context} perf event {index} differs")
                    normalized_events.append(event)
                if len(normalized_events) == 2 and (
                    normalized_events[0].get("nonce")
                    != normalized_events[1].get("nonce")
                    or normalized_events[1].get("sent_monotonic_ns", -1)
                    <= normalized_events[0].get("ack_received_monotonic_ns", -1)
                ):
                    errors.append(f"{context} perf control sequence differs")
                ready = next(
                    (
                        event
                        for event in record.get("control_events", [])
                        if isinstance(event, dict) and event.get("phase") == "ready"
                    ),
                    None,
                )
                if (
                    len(normalized_events) != 2
                    or not isinstance(measured, dict)
                    or measured.get("perf_disable") != normalized_events[1]
                ):
                    errors.append(f"{context} child/perf disable projection differs")
                lifecycle = (
                    ready.get("_runner_received_monotonic_ns")
                    if isinstance(ready, dict)
                    else None,
                    normalized_events[0].get("sent_monotonic_ns")
                    if len(normalized_events) == 2
                    else None,
                    normalized_events[0].get("ack_received_monotonic_ns")
                    if len(normalized_events) == 2
                    else None,
                    start.get("_runner_sent_monotonic_ns")
                    if isinstance(start, dict)
                    else None,
                    measured.get("t1_monotonic_ns")
                    if isinstance(measured, dict)
                    else None,
                    normalized_events[1].get("sent_monotonic_ns")
                    if len(normalized_events) == 2
                    else None,
                    normalized_events[1].get("ack_received_monotonic_ns")
                    if len(normalized_events) == 2
                    else None,
                    measured.get("counter_end_monotonic_ns")
                    if isinstance(measured, dict)
                    else None,
                    measured.get("_runner_received_monotonic_ns")
                    if isinstance(measured, dict)
                    else None,
                )
                if (
                    not all(
                        isinstance(value, int)
                        and not isinstance(value, bool)
                        and value >= 0
                        for value in lifecycle
                    )
                    or not (
                        lifecycle[0]
                        <= lifecycle[1]
                        < lifecycle[2]
                        < lifecycle[3]
                        <= lifecycle[4]
                        <= lifecycle[5]
                        < lifecycle[6]
                        <= lifecycle[7]
                        <= lifecycle[8]
                    )
                    or not isinstance(start, dict)
                    or start.get("nonce") != normalized_events[0].get("nonce")
                ):
                    errors.append(f"{context} perf control/lifecycle projection differs")
                stat_snapshot = validate_terminal_profile_artifact(
                    artifacts.get("stat"), output_dir, f"{context} perf stat", errors
                )
                ack_snapshot = validate_terminal_profile_artifact(
                    artifacts.get("ack"), output_dir, f"{context} perf ACK", errors
                )
                if stat_snapshot is not None and not stat_snapshot.data:
                    errors.append(f"{context} perf stat is empty")
                if ack_snapshot is not None and ack_snapshot.data != b"ack\nack\n":
                    errors.append(f"{context} perf ACK bytes differ")
            elif status == "not_available":
                if perf_environment != {
                    "ASTERISM_REBASELINE_PERF_PERMISSION_RESULT"
                }:
                    errors.append(f"{context} unavailable perf environment differs")
                if events or artifacts or any(name in environment for name in fd_names):
                    errors.append(f"{context} unavailable perf evidence is contradictory")
                if not isinstance(measured, dict) or measured.get("perf_disable") is not None:
                    errors.append(f"{context} unavailable perf disable is not null")
        elif track in {"syscall_profiles", "structural_traces"}:
            validate_terminal_profile_artifact(
                inputs.get("trace_raw_artifact"),
                output_dir,
                f"{context} strace raw",
                errors,
            )
            markers = {
                "log": inputs.get("log_path_markers"),
                "metadata": inputs.get("metadata_path_markers"),
            }
            authority = child_context.get("trace_path_markers")
            if authority is None:
                authority = child_context.get("variant_trace_path_markers")
            if markers != authority:
                errors.append(f"{context} trace input/context markers differ")
            store = environment.get("ASTERISM_REBASELINE_STORE")
            variant = child_context.get("variant")
            try:
                expected_markers = schema.resolved_trace_path_markers(
                    Path(str(store)), str(variant)
                )
            except (KeyError, ValueError) as error:
                errors.append(f"{context} cannot resolve trace markers: {error}")
            else:
                if markers != expected_markers:
                    errors.append(f"{context} trace markers differ from source templates")


def validate_correctness_only_terminal(
    output_dir: Path,
    marker: Mapping[str, Any] | None,
    correctness: Mapping[str, Any] | None,
    result: Mapping[str, Any] | None,
    provenance: Mapping[str, Any] | None,
    children: list[dict[str, Any]],
    errors: list[str],
) -> None:
    if not require_keys(
        marker,
        set(schema.CORRECTNESS_ONLY_FIELDS),
        "terminal correctness-only marker",
        errors,
    ):
        return
    attempt_nonce = provenance.get("attempt_nonce") if provenance else None
    if (
        marker.get("schema") != schema.CORRECTNESS_ONLY_SCHEMA
        or marker.get("protocol") != schema.PROTOCOL
        or marker.get("attempt_nonce") != attempt_nonce
    ):
        errors.append("terminal correctness-only marker identity mismatch")
    if marker.get("trigger") not in {"current", "historical", "mixed"}:
        errors.append("terminal correctness-only marker trigger invalid")
    parse_timestamp(marker.get("created_at"), "terminal correctness-only created_at", errors)
    if (
        not isinstance(marker.get("created_monotonic_ns"), int)
        or isinstance(marker.get("created_monotonic_ns"), bool)
        or marker["created_monotonic_ns"] <= 0
    ):
        errors.append("terminal correctness-only monotonic timestamp invalid")

    def exact_ids(value: Any, context: str) -> list[str]:
        if (
            not isinstance(value, list)
            or not all(isinstance(item, str) and item for item in value)
            or value != sorted(set(value))
        ):
            errors.append(f"{context} is not an exact sorted unique string list")
            return []
        return value

    marker_pre = exact_ids(
        marker.get("current_pre_failed_case_ids"),
        "terminal current pre failed IDs",
    )
    marker_post = exact_ids(
        marker.get("current_post_failed_case_ids"),
        "terminal current post failed IDs",
    )
    marker_historical = marker.get("historical_failed_cases")
    if not isinstance(marker_historical, list):
        errors.append("terminal historical failed cases is not a list")
        marker_historical = []
    else:
        for ordinal, failure in enumerate(marker_historical, start=1):
            if not require_keys(
                failure,
                set(schema.CORRECTNESS_ONLY_HISTORICAL_FAILURE_FIELDS),
                f"terminal historical failure {ordinal}",
                errors,
            ):
                continue
            if failure.get("variant") not in {"C", "D"} or failure.get("phase") != "oracle":
                errors.append(f"terminal historical failure {ordinal} identity invalid")
        if marker_historical != sorted(
            marker_historical,
            key=lambda item: (
                str(item.get("variant")), str(item.get("phase")), str(item.get("id"))
            ),
        ):
            errors.append("terminal historical failures are not exactly sorted")

    if not require_keys(
        correctness,
        set(schema.CORRECTNESS_AGGREGATE_FIELDS),
        "terminal correctness aggregate",
        errors,
    ):
        correctness = {}
    if (
        correctness.get("schema") != schema.CORRECTNESS_SCHEMA
        or correctness.get("protocol") != schema.PROTOCOL
        or correctness.get("attempt_nonce") != attempt_nonce
        or correctness.get("harness_sound") is not True
    ):
        errors.append("terminal correctness aggregate identity/harness mismatch")
    aggregate_bounds = correctness.get("boundedness")
    if not require_keys(
        aggregate_bounds,
        set(schema.CORRECTNESS_BOUNDEDNESS_FIELDS),
        "terminal correctness aggregate boundedness",
        errors,
    ):
        aggregate_bounds = None
    if aggregate_bounds != schema.CORRECTNESS_EXPECTED_BOUNDEDNESS:
        errors.append("terminal correctness aggregate boundedness differs from exact authority")

    descriptors = schema.correctness_descriptors()
    cases = correctness.get("cases", []) if isinstance(correctness, Mapping) else []
    if not isinstance(cases, list) or len(cases) != len(descriptors):
        errors.append("terminal correctness aggregate case cardinality mismatch")
        cases = []
    correctness_children = [
        child for child in children if child.get("kind") in {"correctness", "fault"}
    ]
    observed_groups = [
        (
            child.get("context", {}).get("variant"),
            child.get("context", {}).get("phase"),
            child.get("context", {}).get("suite"),
            child.get("kind"),
        )
        for child in correctness_children
    ]
    if observed_groups != list(schema.CORRECTNESS_GROUPS):
        errors.append("terminal correctness child group order/cardinality mismatch")
    child_by_group = dict(zip(observed_groups, correctness_children))
    child_raw_by_group: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for group, child in child_by_group.items():
        if not isinstance(child, dict):
            errors.append(f"terminal correctness child {group} is not an object")
            continue
        for field in ("ordinal", "kind", "context", "raw_path", "raw_sha256"):
            if field not in child:
                errors.append(f"terminal correctness child {group} lacks {field}")
        raw_path = Path(str(child.get("raw_path", "")))
        raw = bound_file(
            child.get("raw_path"), child.get("raw_sha256"), raw_path,
            f"terminal correctness child {group} raw", errors,
        )
        if raw is None:
            continue
        try:
            raw.path.relative_to(output_dir)
        except ValueError as error:
            errors.append(f"terminal correctness child {group} raw escapes result: {error}")
            continue
        raw_record = read_object(raw, f"terminal correctness child {group} raw", errors)
        if raw_record is None or not require_keys(
            raw_record,
            set(schema.CORRECTNESS_CHILD_FIELDS),
            f"terminal correctness child {group} raw",
            errors,
        ):
            continue
        if (
            raw_record.get("schema") != schema.CORRECTNESS_CHILD_SCHEMA
            or raw_record.get("protocol") != schema.PROTOCOL
            or raw_record.get("attempt_nonce") != attempt_nonce
            or raw_record.get("harness_sound") is not True
            or (
                raw_record.get("variant"), raw_record.get("phase"),
                raw_record.get("suite"), child.get("kind")
            ) != group
        ):
            errors.append(f"terminal correctness child {group} raw identity mismatch")
        expected_child_cases = [
            descriptor
            for descriptor in descriptors
            if tuple(
                descriptor[field] for field in ("variant", "phase", "suite", "kind")
            ) == group
        ]
        raw_cases = raw_record.get("cases")
        if not isinstance(raw_cases, list) or len(raw_cases) != len(expected_child_cases):
            errors.append(f"terminal correctness child {group} case cardinality mismatch")
            raw_cases = []
        for raw_case, descriptor in zip(raw_cases, expected_child_cases):
            if not require_keys(
                raw_case,
                set(schema.CORRECTNESS_CHILD_CASE_FIELDS),
                f"terminal correctness child {group} case",
                errors,
            ):
                continue
            if raw_case != {
                "id": descriptor["id"],
                "classification": descriptor["classification"],
                "status": raw_case.get("status"),
            } or raw_case.get("status") not in {"PASS", "FAIL"}:
                errors.append(f"terminal correctness child {group} case mismatch")
        child_bounds = raw_record.get("boundedness")
        if group[0] == "A" and group[2] == "current-fault":
            require_keys(
                child_bounds,
                set(schema.CORRECTNESS_BOUNDEDNESS_FIELDS),
                f"terminal correctness child {group} boundedness",
                errors,
            )
            if child_bounds != schema.CORRECTNESS_EXPECTED_BOUNDEDNESS:
                errors.append(
                    f"terminal correctness child {group} boundedness differs from exact authority"
                )
        elif child_bounds is not None:
            errors.append(f"terminal correctness child {group} unexpected boundedness")
        child_raw_by_group[group] = raw_record

    for case, descriptor in zip(cases, descriptors):
        if not require_keys(
            case,
            set(schema.CORRECTNESS_AGGREGATE_CASE_FIELDS),
            "terminal correctness aggregate case",
            errors,
        ):
            continue
        for field, expected in descriptor.items():
            if case.get(field) != expected:
                errors.append(f"terminal correctness aggregate case {field} mismatch")
        if case.get("status") not in {"PASS", "FAIL"}:
            errors.append("terminal correctness aggregate case status invalid")
        group = tuple(
            descriptor[field] for field in ("variant", "phase", "suite", "kind")
        )
        child = child_by_group.get(group, {})
        raw_record = child_raw_by_group.get(group, {})
        child_case = next(
            (
                item
                for item in raw_record.get("cases", [])
                if isinstance(item, Mapping) and item.get("id") == descriptor["id"]
            ),
            None,
        )
        if (
            case.get("child_ordinal") != child.get("ordinal")
            or case.get("output_path") != child.get("raw_path")
            or case.get("output_sha256") != child.get("raw_sha256")
            or child_case is None
            or case.get("status") != child_case.get("status")
        ):
            errors.append("terminal correctness aggregate case child binding mismatch")
    derived_pre = sorted(
        {
            str(case.get("id"))
            for case in cases
            if isinstance(case, Mapping)
            and case.get("variant") == "A"
            and case.get("phase") == "pre"
            and case.get("status") == "FAIL"
        }
    )
    derived_post = sorted(
        {
            str(case.get("id"))
            for case in cases
            if isinstance(case, Mapping)
            and case.get("variant") == "A"
            and case.get("phase") == "post"
            and case.get("status") == "FAIL"
        }
    )
    derived_historical = sorted(
        [
            {
                "variant": str(case.get("variant")),
                "phase": str(case.get("phase")),
                "id": str(case.get("id")),
            }
            for case in cases
            if isinstance(case, Mapping)
            and case.get("variant") in {"C", "D"}
            and case.get("phase") == "oracle"
            and case.get("status") == "FAIL"
        ],
        key=lambda item: (item["variant"], item["phase"], item["id"]),
    )
    if (marker_pre, marker_post, marker_historical) != (
        derived_pre, derived_post, derived_historical
    ):
        errors.append("terminal correctness-only marker differs from correctness aggregate")
    expected_trigger = (
        "mixed"
        if derived_pre and derived_historical
        else "current"
        if derived_pre
        else "historical"
        if derived_historical
        else None
    )
    if marker.get("trigger") != expected_trigger:
        errors.append("terminal correctness-only marker trigger differs from aggregate")
    timing_children = sum(
        child.get("kind") in schema.TRACK_EXECUTION_ORDER for child in children
    )
    if marker.get("timing_child_records") != 0 or timing_children != 0:
        errors.append("terminal correctness-only chain contains timing children")
    pre_fault = child_raw_by_group.get(
        ("A", "pre", "current-fault", "fault"), {}
    ).get("boundedness")
    post_fault = child_raw_by_group.get(
        ("A", "post", "current-fault", "fault"), {}
    ).get("boundedness")
    bounds_reproduced = (
        pre_fault == schema.CORRECTNESS_EXPECTED_BOUNDEDNESS
        and post_fault == schema.CORRECTNESS_EXPECTED_BOUNDEDNESS
    )
    expected_outcome = (
        "REVERT"
        if (
            derived_pre
            and derived_pre == derived_post
            and not derived_historical
            and bounds_reproduced
        )
        else "INCONCLUSIVE"
    )
    if result is not None and result.get("outcome") != expected_outcome:
        errors.append("terminal correctness-only result differs from rebound failures")
    report_data = (
        result.get("summary", {}).get("report_data", {})
        if isinstance(result, Mapping)
        else {}
    )
    if (
        not isinstance(report_data, Mapping)
        or report_data.get("correctness_only") is not True
        or report_data.get("timing_rows") != 0
        or report_data.get("current_pre_failed_case_ids") != derived_pre
        or report_data.get("current_post_failed_case_ids") != derived_post
        or report_data.get("historical_failed_cases") != derived_historical
        or report_data.get("bounds_reproduced") is not bounds_reproduced
    ):
        errors.append("terminal correctness-only report data differs from rebound failures")


def validate_evaluator_transition(
    transition: Any,
    output_dir: Path,
    result: Mapping[str, Any] | None,
    prepared: Mapping[str, Any] | None,
    pre_guard: Mapping[str, Any] | None,
    lease: Mapping[str, Any] | None,
    attempt_nonce: Any,
    errors: list[str],
) -> int | None:
    if not require_keys(
        transition, set(schema.EVALUATOR_TRANSITION_FIELDS),
        "evaluator transition", errors,
    ):
        return None
    if transition.get("schema") != schema.EVALUATOR_TRANSITION_SCHEMA or transition.get("protocol") != schema.PROTOCOL:
        errors.append("evaluator transition identity mismatch")
    if transition.get("attempt_nonce") != attempt_nonce:
        errors.append("evaluator transition attempt nonce mismatch")
    if transition.get("pre_guard") != pre_guard:
        errors.append("evaluator transition pre-guard differs from final measurement guard")
    child = transition.get("child")
    if not require_keys(
        child, set(schema.EVALUATOR_TRANSITION_CHILD_FIELDS),
        "evaluator transition child", errors,
    ):
        return None
    tools = prepared.get("tools", {}) if prepared else {}
    support = prepared.get("support_files", {}) if prepared else {}
    runtime = tools.get("evaluator_runtime", {})
    evaluator_binding = support.get("evaluator", {})
    evaluator = Path(str(evaluator_binding.get("path", "missing")))
    runtime_path = Path(str(runtime.get("path", "missing")))
    evaluator_mode = (
        "--evaluate-correctness-only"
        if result is not None and result.get("evidence_mode") == "correctness-only"
        else "--evaluate"
    )
    expected_argv = [str(runtime_path), str(evaluator), evaluator_mode, str(output_dir)]
    if child.get("argv") != expected_argv:
        errors.append("terminal evaluator argv mismatch")
    expected_runtime = {
        "path": runtime.get("path"), "sha256": runtime.get("sha256"),
        "mode": runtime.get("executable_mode"), "comm": runtime.get("comm"),
    }
    if child.get("runtime") != expected_runtime:
        errors.append("evaluator transition runtime differs from prepared binding")
    if child.get("support") != evaluator_binding:
        errors.append("evaluator transition support differs from prepared binding")
    bound_file(
        runtime.get("path"), runtime.get("sha256"), runtime_path,
        "terminal evaluator runtime", errors, expected_mode=0o555,
    )
    bound_file(
        evaluator_binding.get("path"), evaluator_binding.get("sha256"), evaluator,
        "terminal evaluator support", errors,
    )
    if result is not None and (
        evaluator_binding.get("path") != result.get("evaluator_path")
        or evaluator_binding.get("sha256") != result.get("evaluator_sha256")
    ):
        errors.append("terminal evaluator support differs from result")
    identity = child.get("identity")
    if not require_keys(identity, set(schema.PROCESS_IDENTITY_FIELDS), "terminal evaluator identity", errors):
        identity = {}
    for field in ("started_monotonic_ns", "completed_monotonic_ns", "waited_pid"):
        if not isinstance(child.get(field), int) or isinstance(child.get(field), bool) or child[field] <= 0:
            errors.append(f"terminal evaluator {field} invalid")
    if child.get("waited_pid") != identity.get("pid"):
        errors.append("terminal evaluator was not explicitly waited")
    expected_exit = OUTCOME_EXIT.get(result.get("outcome")) if result else None
    if child.get("exit_status") != expected_exit:
        errors.append("terminal evaluator exit differs from decision outcome")
    if (
        child.get("timed_out") is not False
        or child.get("terminated_by_runner") is not False
        or child.get("interrupted") is not None
        or child.get("process_group_absent") is not True
        or child.get("orphan_process_group_detected") is not False
        or child.get("validation_error") is not None
    ):
        errors.append("terminal evaluator timeout/process-group proof failed")
    if child.get("completed_monotonic_ns", 0) < child.get("started_monotonic_ns", 0):
        errors.append("terminal evaluator monotonic chronology invalid")
    parse_timestamp(child.get("started_at"), "terminal evaluator started_at", errors)
    parse_timestamp(child.get("completed_at"), "terminal evaluator completed_at", errors)
    reaping = child.get("reaping")
    if not require_keys(reaping, set(schema.REAPING_FIELDS), "terminal evaluator reaping", errors):
        reaping = None
    if reaping is not None and (
        reaping.get("pid"), reaping.get("start_ticks"), reaping.get("status")
    ) != (identity.get("pid"), identity.get("starttime_ticks"), "absent"):
        errors.append("terminal evaluator reaping identity mismatch")
    stdout = child.get("stdout")
    stderr = child.get("stderr")
    evaluator_output_snapshots: dict[str, schema.FileSnapshot] = {}
    for name, binding in (("stdout", stdout), ("stderr", stderr)):
        if not require_keys(binding, set(schema.EVALUATOR_TRANSITION_FILE_FIELDS), f"evaluator {name}", errors):
            continue
        path = Path(str(binding.get("path", "")))
        snapshot = bound_file(
            binding.get("path"), binding.get("sha256"), path,
            f"evaluator {name}", errors,
        )
        if snapshot is not None:
            evaluator_output_snapshots[name] = snapshot
        try:
            path.relative_to(output_dir)
            if (
                snapshot is None
                or snapshot.size != binding.get("bytes")
                or snapshot.mode != binding.get("mode")
                or binding.get("mode") != 0o444
            ):
                errors.append(f"evaluator {name} byte/mode binding mismatch")
        except ValueError as error:
            errors.append(f"evaluator {name} escapes result or cannot be read: {error}")
    if result is not None:
        snapshot = evaluator_output_snapshots.get("stdout")
        if snapshot is None or snapshot.data != canonical_json_bytes(result):
            errors.append("terminal evaluator stdout differs from canonical result")
    snapshot = evaluator_output_snapshots.get("stderr")
    if snapshot is None or snapshot.data != b"":
        errors.append("terminal evaluator stderr is not empty")
    if child.get("environment") != schema.EVALUATOR_TRANSITION_ENV:
        errors.append("terminal evaluator environment differs from frozen map")

    post_binding = transition.get("post_snapshot")
    post: dict[str, Any] | None = None
    if require_keys(post_binding, set(schema.EVALUATOR_TRANSITION_BINDING_FIELDS), "evaluator post snapshot binding", errors):
        post_path = Path(str(post_binding.get("path", "")))
        post_snapshot = bound_file(
            post_binding.get("path"), post_binding.get("sha256"), post_path,
            "evaluator post snapshot", errors,
        )
        if post_snapshot is not None:
            post = read_object(post_snapshot, "evaluator post snapshot", errors)
    if post is None or not require_keys(post, set(schema.GUARD_SNAPSHOT_FIELDS), "evaluator post snapshot", errors):
        post = None
    elif (
        post.get("schema") != schema.GUARD_SCHEMA
        or post.get("protocol") != schema.PROTOCOL
        or post.get("label") != "post-evaluator"
        or post.get("active_child") is not None
        or post.get("active_helpers") != []
        or post.get("verdict") != "pass"
    ):
        errors.append("evaluator post snapshot is not a clean passing boundary")

    held = transition.get("lease_held")
    if require_keys(held, set(schema.LEASE_HELD_PROOF_FIELDS), "evaluator lease-held proof", errors):
        expected_held = {
            "path": (lease or {}).get("path"), "device": (lease or {}).get("device"),
            "inode": (lease or {}).get("inode"), "holder_pid": (lease or {}).get("holder_pid"),
            "holder_start_ticks": (lease or {}).get("holder_start_ticks"),
            "nonce": (lease or {}).get("nonce"),
        }
        for field, expected in expected_held.items():
            if held.get(field) != expected:
                errors.append(f"evaluator lease-held proof {field} mismatch")
        if not isinstance(held.get("proc_locks_proof"), str) or not held["proc_locks_proof"] or held.get("second_exclusive_failed") is not True:
            errors.append("evaluator lease-held proof is incomplete")
        parse_timestamp(held.get("observed_at"), "evaluator lease-held observed_at", errors)

    pre_ns = pre_guard.get("completed_monotonic_ns") if pre_guard else None
    child_start = child.get("started_monotonic_ns")
    child_end = child.get("completed_monotonic_ns")
    post_start = post.get("started_monotonic_ns") if post else None
    post_end = post.get("completed_monotonic_ns") if post else None
    held_ns = held.get("observed_monotonic_ns") if isinstance(held, dict) else None
    completed = transition.get("completed_monotonic_ns")
    chronology = (pre_ns, child_start, child_end, post_start, post_end, held_ns, completed)
    if not all(isinstance(value, int) and not isinstance(value, bool) for value in chronology) or list(chronology) != sorted(chronology):
        errors.append("evaluator transition chronology is invalid")
    parse_timestamp(transition.get("completed_at"), "evaluator transition completed_at", errors)
    return completed if isinstance(completed, int) else None


def verify(output_dir: Path, *, publish: bool) -> tuple[dict[str, Any], int]:
    _BOUND_SNAPSHOTS.clear()
    errors: list[str] = []
    try:
        output_dir = output_dir.resolve(strict=True)
    except OSError as error:
        output_dir = output_dir.resolve()
        errors.append(f"cannot resolve output directory: {error}")
    verification_path = output_dir / "terminal-verification.json"
    if publish and verification_path.exists():
        errors.append("terminal-verification.json already exists")
    if (output_dir / "failure.json").exists():
        errors.append("failure.json exists at terminal verification")
    pre_path = output_dir / "terminal-pre-release.json"
    release_path = output_dir / "lease-release.json"
    terminal_path = output_dir / "terminal.json"
    result_path = output_dir / "result.json"
    provenance_path = output_dir / "provenance.json"
    sums_path = output_dir / "SHA256SUMS"
    pre = read_object(pre_path, "terminal pre-release", errors)
    release = read_object(release_path, "lease release", errors)
    terminal = read_object(terminal_path, "terminal", errors)
    result = read_object(result_path, "result", errors)
    provenance = read_object(provenance_path, "provenance", errors)
    prepared = read_object(output_dir / "prepared-artifacts.json", "prepared artifacts", errors)
    correctness = read_object(output_dir / "correctness.json", "correctness", errors)
    proof_only_overlay_path = validate_terminal_tools_authority(
        output_dir, prepared, errors
    )
    validate_terminal_profile_contract(output_dir, errors)
    if publish:
        validate_live_terminal_invocation(output_dir, prepared, terminal, errors)

    if pre is not None:
        require_keys(pre, PRE_RELEASE_FIELDS, "terminal pre-release", errors)
    if release is not None:
        require_keys(release, RELEASE_FIELDS, "lease release", errors)
    if terminal is not None:
        require_keys(terminal, TERMINAL_FIELDS, "terminal", errors)
    if result is not None:
        expected_result_fields = {
            "schema", "protocol", "evidence_mode", "outcome", "exit_code", "evidence_valid",
            "matrix_complete", "errors", "gate_failures", "gates", "summary", "artifacts",
            "evaluated_at", "evaluator_path", "evaluator_sha256",
        }
        require_keys(result, expected_result_fields, "evaluation result", errors)
        evidence_mode = result.get("evidence_mode")
        if (
            result.get("schema") != schema.RESULT_SCHEMA
            or result.get("protocol") != schema.PROTOCOL
            or evidence_mode not in {"admission", "correctness-only"}
        ):
            errors.append("evaluation result identity mismatch")
        outcome = result.get("outcome")
        if outcome not in OUTCOME_EXIT or result.get("exit_code") != OUTCOME_EXIT.get(outcome):
            errors.append("evaluation result outcome/exit mismatch")
        expected_matrix = evidence_mode == "admission"
        if (
            result.get("evidence_valid") is not True
            or result.get("matrix_complete") is not expected_matrix
            or result.get("errors") != []
        ):
            errors.append("evaluation result is not complete valid evidence")
        if provenance is not None and provenance.get("evidence_mode") != evidence_mode:
            errors.append("evaluation result mode differs from provenance")
    outcome = result.get("outcome") if result else None
    attempt_nonce = provenance.get("attempt_nonce") if provenance else None

    final_guard_completed_ns: int | None = None
    guards = read_jsonl(output_dir / "guard-manifest.jsonl", "terminal guard manifest", errors)
    if guards:
        final_guard = guards[-1]
        if (
            set(final_guard) != set(schema.GUARD_BINDING_FIELDS)
            or final_guard.get("schema") != schema.GUARD_BINDING_SCHEMA
            or final_guard.get("kind") != "process_guard"
            or final_guard.get("verdict") != "pass"
            or final_guard.get("label") != "pre-evaluator"
        ):
            errors.append("terminal final guard is not the passing pre-evaluator binding")
        snapshot_path = Path(str(final_guard.get("path", "")))
        guard_snapshot = bound_file(
            final_guard.get("path"), final_guard.get("sha256"), snapshot_path,
            "terminal final guard snapshot", errors,
        )
        snapshot = (
            read_object(guard_snapshot, "terminal final guard snapshot", errors)
            if guard_snapshot is not None
            else None
        )
        if snapshot is None or set(snapshot) != set(schema.GUARD_SNAPSHOT_FIELDS) or snapshot.get("active_child") is not None or snapshot.get("active_helpers") != [] or snapshot.get("verdict") != "pass":
            errors.append("terminal final guard snapshot is not clean")
        value = final_guard.get("completed_monotonic_ns")
        if isinstance(value, int) and not isinstance(value, bool):
            final_guard_completed_ns = value
        else:
            errors.append("terminal final guard completion is invalid")
    children = read_jsonl(output_dir / "child-manifest.jsonl", "terminal child manifest", errors)
    validate_terminal_proof_only_child_reachability(
        children, proof_only_overlay_path, errors
    )
    correctness_only = result is not None and result.get("evidence_mode") == "correctness-only"
    validate_terminal_child_projection(
        children,
        output_dir,
        errors,
        correctness_only=correctness_only,
    )
    validate_terminal_result_artifacts(
        result,
        output_dir,
        errors,
        correctness_only=correctness_only,
    )
    correctness_only_path = output_dir / "correctness-only.json"
    if correctness_only:
        marker = read_object(
            correctness_only_path, "terminal correctness-only marker", errors
        )
        marker_snapshot = _BOUND_SNAPSHOTS.get(correctness_only_path)
        if marker_snapshot is None or marker_snapshot.mode != 0o444:
            errors.append("terminal correctness-only marker mode/type invalid")
        validate_correctness_only_terminal(
            output_dir, marker, correctness, result, provenance, children, errors
        )
    elif correctness_only_path.exists() or correctness_only_path.is_symlink():
        errors.append("full-matrix terminal contains correctness-only marker")

    evaluator_completed_ns: int | None = None
    if pre is not None:
        exact_pre = {
            "schema": schema.TERMINAL_PRE_RELEASE_SCHEMA,
            "protocol": schema.PROTOCOL,
            "attempt_nonce": attempt_nonce,
            "outcome": outcome,
            "evaluator_exit": OUTCOME_EXIT.get(outcome),
            "result_path": str(result_path),
            "result_sha256": sha256_file(result_path) if result_path.exists() else None,
            "provenance_path": str(provenance_path),
            "provenance_sha256": sha256_file(provenance_path) if provenance_path.exists() else None,
            "report_path": str(output_dir / "REPORT.md"),
            "report_sha256": sha256_file(output_dir / "REPORT.md") if (output_dir / "REPORT.md").exists() else None,
            "sha256sums_path": str(sums_path),
            "sha256sums_sha256": sha256_file(sums_path) if sums_path.exists() else None,
            "guard_manifest_path": str(output_dir / "guard-manifest.jsonl"),
            "guard_manifest_sha256": sha256_file(output_dir / "guard-manifest.jsonl") if (output_dir / "guard-manifest.jsonl").exists() else None,
            "guard_manifest_records": len(guards),
            "child_manifest_path": str(output_dir / "child-manifest.jsonl"),
            "child_manifest_sha256": sha256_file(output_dir / "child-manifest.jsonl") if (output_dir / "child-manifest.jsonl").exists() else None,
            "child_manifest_records": len(children),
        }
        for field, expected in exact_pre.items():
            if pre.get(field) != expected:
                errors.append(f"terminal pre-release {field} mismatch")
        transition_binding = pre.get("evaluator_transition")
        transition: dict[str, Any] | None = None
        if require_keys(
            transition_binding, set(schema.EVALUATOR_TRANSITION_BINDING_FIELDS),
            "terminal evaluator transition binding", errors,
        ):
            transition_path = output_dir / "evaluator-transition.json"
            transition_snapshot = bound_file(
                transition_binding.get("path"), transition_binding.get("sha256"),
                transition_path, "terminal evaluator transition", errors,
            )
            if transition_snapshot is not None:
                transition = read_object(
                    transition_snapshot, "terminal evaluator transition", errors
                )
        evaluator_completed_ns = validate_evaluator_transition(
            transition, output_dir, result, prepared, final_guard,
            provenance.get("lease") if provenance else None, attempt_nonce, errors,
        )
        if evaluator_completed_ns is not None and pre.get("completed_monotonic_ns", 0) < evaluator_completed_ns:
            errors.append("terminal pre-release predates evaluator completion")
        parse_timestamp(pre.get("completed_at"), "terminal pre-release completed_at", errors)

    inventory = current_inventory(output_dir, errors)
    inventory_paths = {entry["path"] for entry in inventory}
    required_inventory = (
        CORRECTNESS_ONLY_REQUIRED_INVENTORY
        if correctness_only
        else FULL_REQUIRED_INVENTORY
    )
    if not required_inventory <= inventory_paths:
        errors.append(
            "terminal inventory misses required files: "
            f"{sorted(required_inventory - inventory_paths)}"
        )
    timing_csvs = set(schema.CSV_FILENAMES.values()) & inventory_paths
    if correctness_only and timing_csvs:
        errors.append(
            f"correctness-only terminal contains timing CSVs: {sorted(timing_csvs)}"
        )
    if pre is not None and pre.get("artifact_inventory") != inventory:
        errors.append("terminal artifact inventory differs from current immutable files")
    expected_sums = b"".join(
        f"{entry['sha256']}  {entry['path']}\n".encode("utf-8") for entry in inventory
    )
    try:
        sums_snapshot = _BOUND_SNAPSHOTS.get(sums_path)
        if sums_snapshot is None:
            sums_snapshot = schema.snapshot_regular_file(
                sums_path, expected_mode=0o444
            )
            _BOUND_SNAPSHOTS[sums_path] = sums_snapshot
        if sums_snapshot.data != expected_sums:
            errors.append("SHA256SUMS bytes are not exact sorted inventory")
    except (OSError, ValueError) as error:
        errors.append(f"cannot read SHA256SUMS: {error}")

    lease = provenance.get("lease") if provenance else None
    if pre is not None and pre.get("lease") != lease:
        errors.append("terminal pre-release lease differs from provenance")
    if release is not None and isinstance(lease, dict):
        expected_release = {
            "schema": schema.LEASE_RELEASE_SCHEMA,
            "protocol": schema.PROTOCOL,
            "event": "released",
            "attempt_nonce": attempt_nonce,
            "lease_nonce": lease.get("nonce"),
            "lease_path": lease.get("path"),
            "lease_device": lease.get("device"),
            "lease_inode": lease.get("inode"),
            "outcome": outcome,
        }
        for field, expected in expected_release.items():
            if release.get(field) != expected:
                errors.append(f"lease release {field} mismatch")
        parse_timestamp(release.get("released_at"), "lease released_at", errors)
        if publish:
            try:
                lease_path = Path(str(release.get("lease_path"))).resolve(strict=True)
                info = lease_path.stat()
                if (info.st_dev, info.st_ino) != (
                    release.get("lease_device"), release.get("lease_inode")
                ):
                    errors.append("released lease device/inode differs from live lock file")
                descriptor = os.open(lease_path, os.O_RDWR | os.O_CLOEXEC)
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    os.close(descriptor)
            except (OSError, BlockingIOError) as error:
                errors.append(f"released lease is still held or unavailable: {error}")
    if terminal is not None:
        prepared_tools = prepared.get("tools", {}) if prepared else {}
        prepared_support = prepared.get("support_files", {}) if prepared else {}
        runner_runtime = prepared_tools.get("runner_runtime", {})
        terminal_runner = terminal.get("runner", {})
        recorded_cmdline = terminal_runner.get("cmdline") if isinstance(terminal_runner, dict) else None
        if not runner_cmdline_matches(
            recorded_cmdline,
            runner_runtime.get("path"),
            prepared_support.get("runner", {}).get("path"),
        ):
            errors.append("terminal runner cmdline is not a valid prepared invocation")
        expected_runner = {
            "identity": provenance.get("host", {}).get("runner") if provenance else None,
            "runtime": {
                "path": runner_runtime.get("path"),
                "sha256": runner_runtime.get("sha256"),
                "mode": runner_runtime.get("executable_mode"),
                "comm": runner_runtime.get("comm"),
            },
            "support": prepared_support.get("runner"),
            "cmdline": recorded_cmdline,
        }
        expected_terminal = {
            "schema": schema.TERMINAL_SCHEMA,
            "protocol": schema.PROTOCOL,
            "attempt_nonce": attempt_nonce,
            "outcome": outcome,
            "terminal_pre_release_path": str(pre_path),
            "terminal_pre_release_sha256": sha256_file(pre_path) if pre_path.exists() else None,
            "lease_release_path": str(release_path),
            "lease_release_sha256": sha256_file(release_path) if release_path.exists() else None,
            "result_path": str(result_path),
            "result_sha256": sha256_file(result_path) if result_path.exists() else None,
            "provenance_path": str(provenance_path),
            "provenance_sha256": sha256_file(provenance_path) if provenance_path.exists() else None,
            "sha256sums_path": str(sums_path),
            "sha256sums_sha256": sha256_file(sums_path) if sums_path.exists() else None,
            "artifact_inventory_sha256": hashlib.sha256(canonical_json_bytes(inventory)).hexdigest(),
            "runner": expected_runner,
        }
        for field, expected in expected_terminal.items():
            if terminal.get(field) != expected:
                errors.append(f"terminal {field} mismatch")
        parse_timestamp(terminal.get("terminal_published_at"), "terminal published_at", errors)
    pre_ns = pre.get("completed_monotonic_ns") if pre else None
    release_ns = release.get("released_monotonic_ns") if release else None
    terminal_ns = terminal.get("terminal_published_monotonic_ns") if terminal else None
    if not all(isinstance(value, int) and not isinstance(value, bool) for value in (pre_ns, release_ns, terminal_ns)) or not pre_ns <= release_ns <= terminal_ns:
        errors.append("terminal pre-release/release/publication chronology invalid")

    verification = {
        "schema": schema.TERMINAL_VERIFICATION_SCHEMA,
        "protocol": schema.PROTOCOL,
        "outcome": "TERMINAL_VERIFIED" if not errors else "TERMINAL_INVALID",
        "decision_outcome": outcome,
        "output_dir": str(output_dir),
        "terminal_path": str(terminal_path),
        "terminal_sha256": sha256_file(terminal_path) if terminal_path.exists() else "",
        "result_path": str(result_path),
        "result_sha256": sha256_file(result_path) if result_path.exists() else "",
        "provenance_path": str(provenance_path),
        "provenance_sha256": sha256_file(provenance_path) if provenance_path.exists() else "",
        "errors": errors,
        "verified_at": datetime.now(UTC).isoformat(),
    }
    if publish:
        try:
            descriptor = os.open(
                verification_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
                0o444,
            )
            try:
                data = canonical_json_bytes(verification)
                offset = 0
                while offset < len(data):
                    offset += os.write(descriptor, data[offset:])
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except OSError as error:
            errors.append(f"cannot publish terminal verification: {error}")
            return verification, EXIT_INTERNAL
    return verification, EXIT_VERIFIED if not errors else EXIT_INVALID


def write_fixture(path: Path, data: bytes, mode: int = 0o444) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.chmod(0o644)
    path.write_bytes(data)
    path.chmod(mode)


def write_fixture_json(path: Path, value: Mapping[str, Any], mode: int = 0o444) -> None:
    write_fixture(path, canonical_json_bytes(value), mode)


def build_terminal_fixture_v3(
    root: Path,
    *,
    correctness_only: bool = False,
    historical_failure: bool = False,
) -> Path:
    """Build the post-evaluator chain without mutating measurement manifests."""

    output = root / "terminal-result-v3"
    output.mkdir()
    tooling_root = root / "prepared-tooling"
    attempt_nonce = hashlib.sha256(b"terminal-fixture-attempt").hexdigest()
    lease_nonce = hashlib.sha256(b"terminal-fixture-lease").hexdigest()
    required_inventory = (
        CORRECTNESS_ONLY_REQUIRED_INVENTORY
        if correctness_only
        else FULL_REQUIRED_INVENTORY
    )
    for name in required_inventory:
        path = output / name
        if path.suffix == ".json":
            write_fixture_json(path, {"fixture": name})
        else:
            write_fixture(path, f"fixture {name}\n".encode())
    write_fixture_json(
        output / "profile-contract.json",
        {
            "schema": schema.PROFILE_PREFLIGHT_SCHEMA,
            "protocol": schema.PROTOCOL,
            "protocol_sha256": schema.PROTOCOL_SHA256,
            "profile_contract_sha256": schema.expected_profile_contract_sha256(),
            "source": "/proc/<pid>/task/<native-tid>/schedstat:first-field",
            "helper": "adapter-owned-cpu-bound-native-thread",
            "samples_ns": [0, 1, 2],
            "minimum_nonzero_increment_ns": 1,
            "decision_multiplier": schema.SCHEDSTAT_DECISION_MULTIPLIER,
            "decision_floor_ns": schema.SCHEDSTAT_DECISION_MULTIPLIER,
        },
    )

    support: dict[str, dict[str, Any]] = {}
    for name in schema.PREPARED_SUPPORT_FILE_NAMES:
        path = tooling_root / "support" / f"{name}.py"
        write_fixture(path, f"fixture support {name}\n".encode())
        support[name] = {"path": str(path), "sha256": sha256_file(path), "mode": 0o444}
    comms = {
        "runner_runtime": "ast-runner", "evaluator_runtime": "ast-evaluator",
        "terminal_verifier_runtime": "ast-terminal", "strace_launcher_runtime": "ast-strace-py",
        "perf": "ast-perf", "strace": "ast-strace", "correctness": "ast-correct",
        "fault": "ast-fault",
    }
    tools: dict[str, dict[str, Any]] = {}
    for name in schema.PREPARED_TOOL_NAMES:
        path = tooling_root / "executables" / name
        write_fixture(path, f"fixture executable {name}\n".encode(), 0o555)
        tools[name] = {
            "path": str(path), "sha256": sha256_file(path),
            "executable_mode": 0o555, "comm": comms[name],
        }
    tools_manifest = {
        "schema": schema.TOOLS_MANIFEST_SCHEMA,
        "comm_allowlist": schema.expected_comm_allowlist(),
        "tools": tools,
        "support_files": support,
    }
    tools_manifest_sha256 = hashlib.sha256(
        canonical_json_bytes(tools_manifest)
    ).hexdigest()
    tools_manifest_path = tooling_root / "bindings" / "tools-manifest.json"
    write_fixture_json(tools_manifest_path, tools_manifest)
    fake_commit = "1" * 40
    fake_tree = "2" * 40
    terminal_cargo_home = tooling_root / "cargo-home"
    terminal_cargo_home.mkdir()
    write_fixture(terminal_cargo_home / "config.toml", b"")
    terminal_toolchain = {
        "bwrap_path": "/fixture/bwrap",
        "cargo_home_path": str(terminal_cargo_home.resolve()),
    }
    review_id = "cr-terminal-fixture"
    review_time = "2026-07-15T00:00:00+00:00"
    reviewed_root = tooling_root / "reviewed-source"
    lock_manifest_path = reviewed_root / "lock-manifest.json"
    lock_review_path = reviewed_root / "lock-review-bundle.json"
    write_fixture_json(
        lock_manifest_path,
        {"schema": "asterism-rebaseline-lock-candidates-v3"},
    )
    write_fixture_json(
        lock_review_path,
        {"schema": schema.SOURCE_REVIEW_CONTENT_SCHEMAS["lock_review_bundle"]},
    )

    def source_review_input(path: Path) -> dict[str, Any]:
        metadata = path.stat()
        return {
            "identity": {
                "changed_ns": metadata.st_ctime_ns,
                "device": metadata.st_dev,
                "inode": metadata.st_ino,
                "link_count": metadata.st_nlink,
                "modified_ns": metadata.st_mtime_ns,
            },
            "mode": stat.S_IMODE(metadata.st_mode),
            "path": str(path.resolve()),
            "schema": schema.SOURCE_REVIEW_INPUT_SCHEMA,
            "sha256": sha256_file(path),
            "size": metadata.st_size,
        }

    lock_manifest_input = source_review_input(lock_manifest_path)
    lock_review_input = source_review_input(lock_review_path)
    lock_authority = {
        "lock_manifest": {
            "payload": {
                "schema": "asterism-rebaseline-lock-candidates-v3"
            },
            "schema": "asterism-rebaseline-lock-candidates-v3",
            "sha256": lock_manifest_input["sha256"],
        },
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "review_bundle": {
            "payload": {
                "schema": schema.SOURCE_REVIEW_CONTENT_SCHEMAS[
                    "lock_review_bundle"
                ]
            },
            "schema": schema.SOURCE_REVIEW_CONTENT_SCHEMAS[
                "lock_review_bundle"
            ],
            "sha256": lock_review_input["sha256"],
        },
        "review_sha256": lock_review_input["sha256"],
        "schema": schema.SOURCE_REVIEW_CONTENT_SCHEMAS["lock_authority"],
        "status": "approved",
        "tooling_commit": fake_commit,
        "tooling_tree": fake_tree,
    }
    lock_authority_path = reviewed_root / "lock-authority.json"
    write_fixture_json(lock_authority_path, lock_authority)
    lock_authority_input = source_review_input(lock_authority_path)
    nm_path = reviewed_root / "nm"
    write_fixture(nm_path, b"fixture reviewed nm\n", 0o555)
    nm_metadata = nm_path.stat()
    preapproval = {
        "binary_byte_identical": True,
        "forbidden_hook_strings": list(
            schema.RELEASE_COMPILE_OUT_FORBIDDEN_HOOK_STRINGS
        ),
        "nm": {
            "ctime_ns": nm_metadata.st_ctime_ns,
            "device": nm_metadata.st_dev,
            "inode": nm_metadata.st_ino,
            "link_count": nm_metadata.st_nlink,
            "mode": stat.S_IMODE(nm_metadata.st_mode),
            "mtime_ns": nm_metadata.st_mtime_ns,
            "path": str(nm_path.resolve()),
            "sha256": sha256_file(nm_path),
            "size": nm_metadata.st_size,
        },
        "preapproval_source_sentinel": (
            "fa2acb626f303f8a65a16a6c8a1fd86b7e80cf48e092ae21a7308984ae790c94"
        ),
        "symbol_inventory_byte_identical": True,
    }
    product_overlay_sha256 = hashlib.sha256(
        b"terminal fixture product overlay"
    ).hexdigest()
    current_children = {
        "artifacts": {
            name: tools_manifest["tools"][name]
            for name in ("correctness", "fault")
        },
        "lock_authority": lock_authority,
        "lock_authority_inputs": {
            "authority": {
                key: value
                for key, value in lock_authority_input.items()
                if key != "schema"
            },
            "lock_manifest": {
                key: value
                for key, value in lock_manifest_input.items()
                if key != "schema"
            },
            "review_bundle": {
                key: value
                for key, value in lock_review_input.items()
                if key != "schema"
            },
        },
        "lock_manifest_sha256": lock_manifest_input["sha256"],
        "product_overlay_authority": {
            "patch": {"sha256": product_overlay_sha256}
        },
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "release_compile_out": preapproval,
        "release_compile_out_approval": {
            "final_integration_action": (
                "repeat-release-equality-proof-under-real-source-approval"
            ),
            "source_approval_sha256": (
                "fa2acb626f303f8a65a16a6c8a1fd86b7e80cf48e092ae21a7308984ae790c94"
            ),
            "source_approval_status": "preapproval-sentinel-not-source-approved",
        },
        "review_bundle_sha256": lock_review_input["sha256"],
        "schema": schema.SOURCE_REVIEW_CONTENT_SCHEMAS[
            "current_children_attestation"
        ],
        "status": "ok",
        "tools_manifest_sha256": tools_manifest_sha256,
    }
    current_children_path = reviewed_root / "current-children-attestation.json"
    write_fixture_json(current_children_path, current_children)
    source_inputs = {
        "current_children_attestation": source_review_input(current_children_path),
        "lock_authority": lock_authority_input,
        "lock_manifest": lock_manifest_input,
        "lock_review_bundle": lock_review_input,
        "tools_manifest": source_review_input(tools_manifest_path),
    }
    requirement = {
        "binary_byte_identical": True,
        "cfg_test": False,
        "forbidden_hook_strings": list(
            schema.RELEASE_COMPILE_OUT_FORBIDDEN_HOOK_STRINGS
        ),
        "forbidden_hook_strings_absent": True,
        "ordinary_a_role": "published",
        "overlay_a_role": "proof_only",
        "preapproval_compile_out_sha256": hashlib.sha256(
            canonical_json_bytes(preapproval)
        ).hexdigest(),
        "product_overlay_sha256": product_overlay_sha256,
        "proof_must_bind_enclosing_approval_sha256": True,
        "repeat_under_real_source_approval": True,
        "rustc_workspace_wrapper": "absent",
        "same_contract_nonce_lock_toolchain_sandbox": True,
        "schema": schema.RELEASE_COMPILE_OUT_REQUIREMENT_SCHEMA,
        "status": "required",
        "symbol_inventory_byte_identical": True,
        "variant": "A",
    }
    assertion = {
        "inputs": source_inputs,
        "open_findings": 0,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "release_compile_out_requirement": requirement,
        "schema": schema.SOURCE_REVIEW_ASSERTION_SCHEMA,
        "status": "approved",
        "tooling_commit": fake_commit,
        "tooling_tree": fake_tree,
    }
    assertion_sha256 = hashlib.sha256(
        canonical_json_bytes(assertion)
    ).hexdigest()
    bundle = {
        "assertion": assertion,
        "assertion_sha256": assertion_sha256,
        "review_created": {
            "author": "terminal-reviewer",
            "data": {
                "description": "Synthetic terminal source authority",
                "initial_commit": fake_commit,
                "jj_change_id": f"detached:{fake_commit}",
                "review_id": review_id,
                "scm_anchor": f"detached:{fake_commit}",
                "scm_kind": "git",
                "title": "Synthetic terminal source authority",
            },
            "event": "ReviewCreated",
            "ts": review_time,
        },
        "schema": schema.SOURCE_REVIEW_BUNDLE_SCHEMA,
        "verdict": {
            "author": "terminal-reviewer",
            "data": {
                "reason": (
                    f"APPROVED assertion_sha256={assertion_sha256}; open_findings=0"
                ),
                "review_id": review_id,
                "vote": "lgtm",
            },
            "event": "ReviewerVoted",
            "ts": review_time,
        },
    }
    source_bundle_path = reviewed_root / "source-review-bundle.json"
    write_fixture_json(source_bundle_path, bundle)
    prepared_source_review: dict[str, Any] = {}
    for name, source, filename in (
        ("bundle", source_bundle_path, "source-review-bundle.json"),
        (
            "current_children_attestation",
            current_children_path,
            "current-children-attestation.json",
        ),
        ("lock_authority", lock_authority_path, "lock-review-authority.json"),
        ("lock_review_bundle", lock_review_path, "lock-review-bundle.json"),
    ):
        destination = tooling_root / "bindings" / filename
        write_fixture(destination, source.read_bytes(), 0o444)
        prepared_source_review[name] = {
            "mode": 0o444,
            "path": str(destination.resolve()),
            "sha256": sha256_file(destination),
        }
    source_review = {
        "assertion_sha256": assertion_sha256,
        "bundle": {
            "mode": 0o444,
            "schema": schema.SOURCE_REVIEW_BUNDLE_SCHEMA,
            "sha256": sha256_file(source_bundle_path),
        },
        "current_children_attestation": {
            "mode": 0o444,
            "schema": schema.SOURCE_REVIEW_CONTENT_SCHEMAS[
                "current_children_attestation"
            ],
            "sha256": source_inputs["current_children_attestation"]["sha256"],
        },
        "lock_authority": {
            "mode": 0o444,
            "schema": schema.SOURCE_REVIEW_CONTENT_SCHEMAS["lock_authority"],
            "sha256": lock_authority_input["sha256"],
        },
        "lock_review_bundle": {
            "mode": 0o444,
            "schema": schema.SOURCE_REVIEW_CONTENT_SCHEMAS[
                "lock_review_bundle"
            ],
            "sha256": lock_review_input["sha256"],
        },
        "release_compile_out_requirement": requirement,
    }
    source_variants: dict[str, dict[str, Any]] = {}
    for variant in schema.VARIANTS:
        binding = schema.VARIANT_SOURCE_BINDINGS[variant]
        source_variants[variant] = {
            "product_commit": binding["commit"],
            "product_tree": binding["tree"],
            "binary_kind": "bare" if variant == "B" else "public",
            "timed_surface": "raw-numeric" if variant == "B" else "public-event-store",
            "adapter_sha256": hashlib.sha256(
                f"terminal-adapter-{variant}".encode()
            ).hexdigest(),
            "cargo_lock_sha256": hashlib.sha256(
                f"terminal-lock-{variant}".encode()
            ).hexdigest(),
            "overlay_manifest_sha256": hashlib.sha256(
                f"terminal-overlay-{variant}".encode()
            ).hexdigest(),
            "allowed_overlay_paths": ["fixture/main.rs"],
            "lock_resolution": {},
            "current_lock_attempt": None,
            "correctness_oracle_mode": variant != "B",
            "profile_role_lifetime": (
                schema.PROFILE_C_ROLE_LIFETIME_CONTRACT
                if variant == "C"
                else "not_applicable"
            ),
            "trace_path_marker_templates": (
                schema.expected_trace_path_marker_templates(variant)
            ),
        }
    approval = {
        "schema": schema.SOURCE_APPROVAL_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "status": "approved",
        "review_id": review_id,
        "reviewed_at": review_time,
        "tooling_commit": fake_commit,
        "tooling_tree": fake_tree,
        "toolchain": terminal_toolchain,
        "shared_manifest_sha256": hashlib.sha256(b"terminal-shared").hexdigest(),
        "tools_manifest": tools_manifest,
        "tools_manifest_sha256": tools_manifest_sha256,
        "filesystem_admission": {},
        "comm_allowlist": schema.expected_comm_allowlist(),
        "source_review": source_review,
        "variants": source_variants,
    }
    original_approval_path = tooling_root.joinpath(
        *schema.PREPARED_SOURCE_APPROVAL_RELATIVE_PATH
    )
    write_fixture_json(original_approval_path, approval)
    original_approval_path.parent.chmod(0o555)
    approval_sha256 = sha256_file(original_approval_path)
    approval_path = output / "source-approval.json"
    write_fixture(approval_path, original_approval_path.read_bytes())
    prepared_variants: dict[str, dict[str, Any]] = {}
    for variant in schema.VARIANTS:
        binary = tooling_root / "variants" / variant / "rebaseline-bench"
        write_fixture(binary, f"fixture binary {variant}\n".encode(), 0o555)
        source_variant = source_variants[variant]
        contract = {
            "schema": schema.BINARY_CONTRACT_SCHEMA,
            "protocol": schema.PROTOCOL,
            "protocol_sha256": schema.PROTOCOL_SHA256,
            "tooling_commit": fake_commit,
            "tooling_tree": fake_tree,
            "variant": variant,
            "product_commit": source_variant["product_commit"],
            "product_tree": source_variant["product_tree"],
            "adapter_sha256": source_variant["adapter_sha256"],
            "shared_manifest_sha256": approval["shared_manifest_sha256"],
            "cargo_lock_sha256": source_variant["cargo_lock_sha256"],
            "source_approval_sha256": approval_sha256,
            "build_nonce": hashlib.sha256(
                f"terminal-build-{variant}".encode()
            ).hexdigest(),
            "binary_kind": source_variant["binary_kind"],
            "timed_surface": source_variant["timed_surface"],
            "correctness_oracle_mode": variant != "B",
            "profile_role_lifetime": source_variant["profile_role_lifetime"],
            "contract_mode": True,
            "rows_written": 0,
        }
        prepared_variants[variant] = {
            "contract": contract,
            "binary": {"path": str(binary), "sha256": sha256_file(binary)},
            "executable_mode": 0o555,
            "artifact_root": str(binary.parent),
            "contract_argv": [str(binary), "--contract"],
            "contract_env": {},
            "comm": schema.VARIANT_COMMS[variant],
            "evidence_argv": [str(binary)],
            "evidence_env": schema.expected_trace_marker_environment(variant),
            "trace_path_marker_templates": source_variant[
                "trace_path_marker_templates"
            ],
            "correctness_oracle_mode": variant != "B",
            "attestation": {},
        }

    def release_file_record(path: Path) -> dict[str, Any]:
        metadata = path.stat()
        return {
            "identity": {
                "changed_ns": metadata.st_ctime_ns,
                "device": metadata.st_dev,
                "inode": metadata.st_ino,
                "link_count": metadata.st_nlink,
                "modified_ns": metadata.st_mtime_ns,
            },
            "mode": stat.S_IMODE(metadata.st_mode),
            "path": str(path.resolve()),
            "sha256": sha256_file(path),
            "size": metadata.st_size,
        }

    ordinary_binary = Path(prepared_variants["A"]["binary"]["path"])
    overlay_binary = tooling_root / "proof-only" / "rebaseline-bench-overlay"
    write_fixture(overlay_binary, ordinary_binary.read_bytes(), 0o555)
    symbols = b"terminal_fixture_symbol T 0\n"
    ordinary_inventory = tooling_root / "manifests" / "symbols-ordinary-a.txt"
    overlay_inventory = tooling_root / "manifests" / "symbols-overlay-a.txt"
    write_fixture(ordinary_inventory, symbols)
    write_fixture(overlay_inventory, symbols)
    contract_path = tooling_root / "manifests" / "release-contract-a.json"
    write_fixture_json(contract_path, prepared_variants["A"]["contract"])
    fixture_hash = hashlib.sha256(b"terminal fixture field").hexdigest()
    materialized_path = tooling_root / "materialized" / "A"
    materialized_path.mkdir(parents=True)
    materialized_root = str(materialized_path.resolve())
    target_dir = str((tooling_root / "targets" / "A").resolve())
    descriptor_bindings = (
        ("--ro-bind-fd", "401", GUEST_SOURCE),
        ("--bind-fd", "402", GUEST_TARGET),
        ("--ro-bind-fd", "403", GUEST_TOOLCHAIN_ROOT),
        ("--ro-bind-fd", "404", GUEST_CARGO),
        ("--ro-bind-fd", "405", GUEST_RUSTC),
        ("--ro-bind-fd", "406", GUEST_CARGO_HOME),
        ("--ro-bind-fd", "407", GUEST_RUSTUP_HOME),
    )
    config_descriptor_bindings = tuple(
        ("--ro-bind-fd", str(408 + offset), guest_path)
        for offset, guest_path in enumerate(GUEST_BOUND_CONFIG_PATHS)
    )
    release_build_argv = [
        "/fixture/bwrap",
        "--die-with-parent",
        "--new-session",
        "--unshare-net",
        "--ro-bind",
        "/",
        "/",
        "--dev-bind",
        "/dev",
        "/dev",
        "--proc",
        "/proc",
        "--tmpfs",
        "/tmp",
        "--tmpfs",
        GUEST_ROOT,
        "--dir",
        f"{GUEST_ROOT}/.cargo",
        "--tmpfs",
        f"{GUEST_ROOT}/.cargo",
        "--remount-ro",
        f"{GUEST_ROOT}/.cargo",
        "--dir",
        "/.cargo",
        "--tmpfs",
        "/.cargo",
        "--remount-ro",
        "/.cargo",
        *(argument for binding in descriptor_bindings for argument in binding),
        "--dir",
        f"{GUEST_SOURCE}/.cargo",
        "--tmpfs",
        f"{GUEST_SOURCE}/.cargo",
        *(
            argument
            for binding in config_descriptor_bindings[:2]
            for argument in binding
        ),
        "--remount-ro",
        f"{GUEST_SOURCE}/.cargo",
        *(
            argument
            for binding in config_descriptor_bindings[2:]
            for argument in binding
        ),
        "--remount-ro",
        GUEST_CARGO_HOME,
        "--chdir",
        GUEST_SOURCE,
        GUEST_CARGO,
        "build",
        "--locked",
        "--offline",
        "--release",
        "-p",
        "mess-store",
        "--example",
        "asterism_rebaseline_public",
        "--target-dir",
        GUEST_TARGET,
    ]
    cargo_config_manifest = {
        "cargo_home_path": GUEST_CARGO_HOME,
        "cwd": GUEST_SOURCE,
        "entries": [
            {
                "path": f"{GUEST_SOURCE}/.cargo/config.toml",
                "sha256": EMPTY_SHA256,
                "status": "present",
            },
            {
                "path": f"{GUEST_SOURCE}/.cargo/config",
                "sha256": EMPTY_SHA256,
                "status": "present",
            },
            {
                "path": f"{GUEST_ROOT}/.cargo/config.toml",
                "sha256": None,
                "status": "absent",
            },
            {
                "path": f"{GUEST_ROOT}/.cargo/config",
                "sha256": None,
                "status": "absent",
            },
            {"path": "/.cargo/config.toml", "sha256": None, "status": "absent"},
            {"path": "/.cargo/config", "sha256": None, "status": "absent"},
            {
                "path": f"{GUEST_CARGO_HOME}/config.toml",
                "sha256": sha256_file(terminal_cargo_home / "config.toml"),
                "status": "present",
            },
            {
                "path": f"{GUEST_CARGO_HOME}/config",
                "sha256": EMPTY_SHA256,
                "status": "present",
            },
        ],
        "schema": schema.CARGO_CONFIG_SEARCH_SCHEMA,
    }
    ordinary_config_path = tooling_root / "manifests" / "cargo-config-A.json"
    write_fixture(
        ordinary_config_path.with_name(f"{ordinary_config_path.name}.empty"),
        b"",
    )
    write_fixture_json(ordinary_config_path, cargo_config_manifest)
    ordinary_config_binding = {
        "path": str(ordinary_config_path.resolve()),
        "sha256": sha256_file(ordinary_config_path),
    }
    ordinary_attestation: dict[str, Any] = {
        field: fixture_hash for field in schema.PREPARED_ATTESTATION_FIELDS
    }
    ordinary_attestation.update(
        {
            "archive_manifest_path": str(tooling_root / "archive-manifest.json"),
            "build_argv": release_build_argv,
            "build_child": {"argv": release_build_argv},
            "build_env": {
                "ASTERISM_BUILD_SOURCE_APPROVAL_SHA256": approval_sha256,
                "CARGO_HOME": GUEST_CARGO_HOME,
                "PATH": f"{GUEST_TOOLCHAIN_ROOT}/bin:/usr/bin:/bin",
                "RUSTC": GUEST_RUSTC,
                "RUSTUP_HOME": GUEST_RUSTUP_HOME,
            },
            "build_completed_at": "2026-07-15T00:00:01+00:00",
            "build_completed_monotonic_ns": 20,
            "build_nonce": prepared_variants["A"]["contract"]["build_nonce"],
            "build_started_at": "2026-07-15T00:00:00+00:00",
            "build_started_monotonic_ns": 10,
            "cargo_config_search": ordinary_config_binding,
            "cargo_lock_path": str(tooling_root / "Cargo.lock"),
            "cargo_lock_post_sha256": source_variants["A"]["cargo_lock_sha256"],
            "cargo_lock_pre_sha256": source_variants["A"]["cargo_lock_sha256"],
            "cargo_lock_sha256": source_variants["A"]["cargo_lock_sha256"],
            "contract_child": {},
            "contract_output_path": str(contract_path.resolve()),
            "contract_output_sha256": sha256_file(contract_path),
            "materialized_manifest_path": str(
                tooling_root / "materialized-manifest.json"
            ),
            "materialized_root": materialized_root,
            "overlay_manifest_path": str(tooling_root / "overlay-manifest.json"),
            "source_archive_bytes": 1,
            "source_archive_path": str(tooling_root / "source.tar"),
            "source_commit": source_variants["A"]["product_commit"],
            "source_read_only": True,
            "source_tree": source_variants["A"]["product_tree"],
            "target_dir": target_dir,
            "target_dir_was_absent": True,
            "toolchain": approval["toolchain"],
        }
    )
    overlay_attestation = dict(ordinary_attestation)
    overlay_config_path = (
        tooling_root / "manifests" / "cargo-config-A-product-overlay.json"
    )
    write_fixture(
        overlay_config_path.with_name(f"{overlay_config_path.name}.empty"),
        b"",
    )
    write_fixture(overlay_config_path, ordinary_config_path.read_bytes())
    overlay_attestation["cargo_config_search"] = {
        "path": str(overlay_config_path.resolve()),
        "sha256": sha256_file(overlay_config_path),
    }
    overlay_attestation["product_overlay_sha256"] = product_overlay_sha256
    prepared_variants["A"]["attestation"] = ordinary_attestation
    normalized_sandbox = list(release_build_argv)
    for index, argument in enumerate(release_build_argv):
        if argument in {"--ro-bind-fd", "--bind-fd"}:
            normalized_sandbox[index + 1] = (
                f"$FD:{release_build_argv[index + 2]}"
            )
    equivalence = {
        "build_environment_sha256": hashlib.sha256(
            canonical_json_bytes(ordinary_attestation["build_env"])
        ).hexdigest(),
        "build_nonce": ordinary_attestation["build_nonce"],
        "cargo_lock_sha256": ordinary_attestation["cargo_lock_sha256"],
        "cfg_test": False,
        "contract_sha256": hashlib.sha256(
            canonical_json_bytes(prepared_variants["A"]["contract"])
        ).hexdigest(),
        "ordinary_a_role": "published",
        "overlay_a_role": "proof_only",
        "rustc_workspace_wrapper": "absent",
        "sandbox_sha256": hashlib.sha256(
            canonical_json_bytes(
                {
                    "argv": normalized_sandbox,
                    "cargo_config_search_sha256": ordinary_config_binding[
                        "sha256"
                    ],
                }
            )
        ).hexdigest(),
        "source_approval_sha256": approval_sha256,
        "toolchain_sha256": hashlib.sha256(
            canonical_json_bytes(ordinary_attestation["toolchain"])
        ).hexdigest(),
    }

    def release_build(
        name: str, artifact_role: str, attestation: Mapping[str, Any]
    ) -> dict[str, Any]:
        return {
            "artifact_role": artifact_role,
            "attestation": attestation,
            "attestation_sha256": hashlib.sha256(
                canonical_json_bytes(attestation)
            ).hexdigest(),
            "build_environment_sha256": equivalence["build_environment_sha256"],
            "build_nonce": equivalence["build_nonce"],
            "cargo_lock_sha256": equivalence["cargo_lock_sha256"],
            "cfg_test": False,
            "contract_sha256": equivalence["contract_sha256"],
            "role": name,
            "rustc_workspace_wrapper": "absent",
            "sandbox_sha256": equivalence["sandbox_sha256"],
            "source_approval_sha256": approval_sha256,
            "toolchain_sha256": equivalence["toolchain_sha256"],
        }

    nm_tool = {
        "identity": {
            "changed_ns": preapproval["nm"]["ctime_ns"],
            "device": preapproval["nm"]["device"],
            "inode": preapproval["nm"]["inode"],
            "link_count": preapproval["nm"]["link_count"],
            "modified_ns": preapproval["nm"]["mtime_ns"],
        },
        "mode": preapproval["nm"]["mode"],
        "path": preapproval["nm"]["path"],
        "sha256": preapproval["nm"]["sha256"],
        "size": preapproval["nm"]["size"],
    }

    def nm_child(name: str, ordinal: int) -> dict[str, Any]:
        output_path = tooling_root / "logs" / f"nm-{name}.json"
        write_fixture_json(
            output_path,
            {
                "exit_status": 0,
                "stderr": "",
                "stderr_sha256": hashlib.sha256(b"").hexdigest(),
                "stdout": symbols.decode(),
                "stdout_sha256": hashlib.sha256(symbols).hexdigest(),
            },
        )
        pid = 50_000 + ordinal
        start_ticks = 60_000 + ordinal
        return {
            "argv": [
                str(nm_path.resolve()),
                "--defined-only",
                "--demangle=rust",
                "--format=posix",
                f"/proc/self/fd/{700 + ordinal}",
            ],
            "completed_at": "2026-07-15T00:00:01+00:00",
            "completed_monotonic_ns": 20_000 + ordinal,
            "cwd": str(root.resolve()),
            "exit_status": 0,
            "output_path": str(output_path.resolve()),
            "output_sha256": sha256_file(output_path),
            "pid": pid,
            "process_group_absent": True,
            "reaping": {
                "pid": pid,
                "start_ticks": start_ticks,
                "status": "absent",
            },
            "start_ticks": start_ticks,
            "started_at": "2026-07-15T00:00:00+00:00",
            "started_monotonic_ns": 10_000 + ordinal,
            "timed_out": False,
            "waited_pid": pid,
        }

    release_compile_out = {
        "binaries": {
            "ordinary_a": release_file_record(ordinary_binary),
            "overlay_a": release_file_record(overlay_binary),
        },
        "binary_byte_identical": True,
        "builds": {
            "ordinary_a": release_build(
                "ordinary_a", "published", ordinary_attestation
            ),
            "overlay_a": release_build(
                "overlay_a", "proof_only", overlay_attestation
            ),
        },
        "current_children_attestation_sha256": source_inputs[
            "current_children_attestation"
        ]["sha256"],
        "equivalence_contract": equivalence,
        "forbidden_hook_strings": list(
            schema.RELEASE_COMPILE_OUT_FORBIDDEN_HOOK_STRINGS
        ),
        "forbidden_hook_strings_absent": True,
        "nm": {
            "ordinary_a": nm_child("ordinary-a", 1),
            "overlay_a": nm_child("overlay-a", 2),
            "tool": nm_tool,
        },
        "product_overlay_sha256": product_overlay_sha256,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "published_a_sha256": sha256_file(ordinary_binary),
        "requirement_sha256": hashlib.sha256(
            canonical_json_bytes(requirement)
        ).hexdigest(),
        "schema": schema.RELEASE_COMPILE_OUT_SCHEMA,
        "source_approval_sha256": approval_sha256,
        "status": "ok",
        "symbol_inventories": {
            "ordinary_a": release_file_record(ordinary_inventory),
            "overlay_a": release_file_record(overlay_inventory),
        },
        "symbol_inventory_byte_identical": True,
    }
    release_compile_out_path = (
        tooling_root / schema.RELEASE_COMPILE_OUT_RELATIVE_PATH
    )
    write_fixture_json(release_compile_out_path, release_compile_out)
    release_compile_out_binding = {
        "mode": 0o444,
        "path": str(release_compile_out_path.resolve()),
        "sha256": sha256_file(release_compile_out_path),
    }
    prepared = {
        "schema": schema.PREPARED_ARTIFACTS_SCHEMA,
        "protocol": schema.PROTOCOL,
        "protocol_sha256": schema.PROTOCOL_SHA256,
        "tooling_commit": fake_commit,
        "tooling_tree": fake_tree,
        "created_at": "2026-07-15T00:00:01+00:00",
        "created_monotonic_ns": 10,
        "source_approval": {
            "path": str(original_approval_path),
            "sha256": approval_sha256,
        },
        "single_use_claim": {
            "path": str(tooling_root / "claims" / "single-use-claim.json")
        },
        "source_review": prepared_source_review,
        "release_compile_out": release_compile_out_binding,
        "comm_allowlist": schema.expected_comm_allowlist(),
        "tools": tools,
        "support_files": support,
        "tools_manifest": {
            "path": str(tools_manifest_path),
            "sha256": tools_manifest_sha256,
            "mode": 0o444,
        },
        "inputs": {},
        "filesystem_admission": {},
        "build_order": list(schema.VARIANTS),
        "toolchain": terminal_toolchain,
        "variants": prepared_variants,
    }
    original_prepared_path = tooling_root / "prepared-artifacts.json"
    write_fixture_json(original_prepared_path, prepared)
    prepared_path = output / "prepared-artifacts.json"
    write_fixture(prepared_path, original_prepared_path.read_bytes())
    claims_directory = tooling_root / "claims"
    claims_directory.mkdir()
    claims_directory.chmod(0o700)
    tooling_root.chmod(0o555)
    write_fixture_json(
        claims_directory / "single-use-claim.json",
        {
            "schema": schema.PREPARED_CLAIM_SCHEMA,
            "protocol": schema.PROTOCOL,
            "prepared_artifacts_path": str(original_prepared_path),
            "prepared_artifacts_sha256": sha256_file(original_prepared_path),
            "output_dir": str(output),
            "attempt_nonce": attempt_nonce,
            "lease_nonce": lease_nonce,
            "claimed_at": "2026-07-15T00:00:03+00:00",
            "claimed_monotonic_ns": 30,
        },
    )
    evaluator = Path(support["evaluator"]["path"])
    current_case = schema.CORRECTNESS_CASE_IDS[0]
    decision_outcome = "INCONCLUSIVE" if historical_failure else (
        "REVERT" if correctness_only else "ADMIT"
    )
    decision_exit = OUTCOME_EXIT[decision_outcome]
    evidence_mode = "correctness-only" if correctness_only else "admission"
    terminal_children: list[dict[str, Any]] = []
    if not correctness_only:
        def raw_binding(path: Path) -> dict[str, Any]:
            return {
                "path": str(path),
                "sha256": sha256_file(path),
                "bytes": path.stat().st_size,
                "mode": 0o444,
            }

        def profile_child(
            ordinal: int,
            track: str,
            context: dict[str, Any],
            environment: dict[str, str],
            profile_inputs: dict[str, Any],
            control_events: list[dict[str, Any]],
        ) -> dict[str, Any]:
            raw_path = output / "raw" / track / f"{ordinal:05d}.json"
            stderr_path = output / "raw" / track / f"{ordinal:05d}.stderr"
            write_fixture_json(raw_path, {"fixture": track})
            write_fixture(stderr_path, b"")
            variant = str(context["variant"])
            binary = prepared_variants[variant]["binary"]
            empty_hash = hashlib.sha256(canonical_json_bytes({})).hexdigest()
            context_sha256 = hashlib.sha256(
                canonical_json_bytes(context)
            ).hexdigest()
            identity = {
                "pid": 10_000 + ordinal,
                "comm": schema.VARIANT_COMMS[variant],
                "state": "S",
                "ppid": 1,
                "pgrp": 10_000 + ordinal,
                "session": 10_000 + ordinal,
                "starttime_ticks": 20_000 + ordinal,
            }
            expected_tool_names = (
                {"perf"}
                if track == "cpu_profiles"
                else {"strace", "strace_launcher_runtime"}
                if track in {"syscall_profiles", "structural_traces"}
                else set()
            )
            control_fd = int(environment["ASTERISM_REBASELINE_CONTROL_FD"])
            source = schema.VARIANT_SOURCE_BINDINGS[variant]
            authority = {
                "schema": schema.PROFILE_AUTHORITY_SCHEMA,
                "protocol": schema.PROTOCOL,
                "protocol_sha256": schema.PROTOCOL_SHA256,
                "attempt_nonce": attempt_nonce,
                "child_ordinal": ordinal,
                "row_ordinal": context["row_ordinal"],
                "context_sha256": context_sha256,
                "prepared_artifacts_path": str(prepared_path),
                "prepared_artifacts_sha256": sha256_file(prepared_path),
                "source_approval_path": str(approval_path),
                "source_approval_sha256": approval_sha256,
                "profile_adapter_path": support["profile_adapter"]["path"],
                "profile_adapter_sha256": support["profile_adapter"]["sha256"],
                "profile_tools": {
                    name: tools[name] for name in expected_tool_names
                },
                "perf_permission_result": (
                    profile_inputs["perf_permission"]
                    if track == "cpu_profiles"
                    else "not_applicable"
                ),
                "variant": variant,
                "source_commit": source["commit"],
                "source_tree": source["tree"],
                "track": track,
                "executable_path": binary["path"],
                "executable_sha256": binary["sha256"],
                "executable_mode": 0o555,
                "executable_comm": schema.VARIANT_COMMS[variant],
                "child_pid": identity["pid"],
                "child_start_ticks": identity["starttime_ticks"],
                "control_fd": control_fd,
            }
            rich = {
                "schema": schema.PROFILE_ADAPTER_SCHEMA,
                "protocol": schema.PROTOCOL,
                "authority": authority,
                "variant": variant,
                "track": track,
                "context": context,
                "process": {},
                "roles": [],
                "phase_snapshots": [],
                "unattributed_births": [],
            }
            return {
                "schema": schema.CHILD_SCHEMA,
                "protocol": schema.PROTOCOL,
                "ordinal": ordinal,
                "kind": track,
                "context": context,
                "context_sha256": context_sha256,
                "argv": [binary["path"]],
                "environment": environment,
                "executable_path": binary["path"],
                "executable_sha256": binary["sha256"],
                "executable_mode": 0o555,
                "executable_comm": schema.VARIANT_COMMS[variant],
                "identity": identity,
                "waited_pid": 10_000 + ordinal,
                "started_at": "2026-07-15T00:00:00+00:00",
                "started_monotonic_ns": 20 + ordinal * 10,
                "completed_at": "2026-07-15T00:00:01+00:00",
                "completed_monotonic_ns": 25 + ordinal * 10,
                "exit_status": 0,
                "timed_out": False,
                "terminated_by_runner": False,
                "interrupted": None,
                "reaping": {
                    "pid": 10_000 + ordinal,
                    "start_ticks": 20_000 + ordinal,
                    "status": "absent",
                },
                "process_group_absent": True,
                "orphan_process_group_detected": False,
                "control_events": control_events,
                "control_events_sha256": hashlib.sha256(
                    canonical_json_bytes(control_events)
                ).hexdigest(),
                "profile_events": [],
                "profile_events_sha256": hashlib.sha256(
                    canonical_json_bytes([])
                ).hexdigest(),
                "parked_state_proofs": [],
                "profile_rich_result": rich,
                "runner_context": {},
                "runner_context_sha256": empty_hash,
                "profile_result": {},
                "profile_result_sha256": empty_hash,
                "profile_contract_sha256": sha256_file(
                    output / "profile-contract.json"
                ),
                "profile_tool_inputs": profile_inputs,
                "profile_tool_inputs_sha256": hashlib.sha256(
                    canonical_json_bytes(profile_inputs)
                ).hexdigest(),
                "profile_tool_helper_records": [],
                "raw_path": str(raw_path),
                "raw_sha256": sha256_file(raw_path),
                "raw_bytes": raw_path.stat().st_size,
                "raw_mode_after": 0o444,
                "stderr_path": str(stderr_path),
                "stderr_sha256": sha256_file(stderr_path),
                "stderr_bytes": 0,
                "stderr_mode_after": 0o444,
                "expected_records": 1,
                "combined_row_sha256": empty_hash,
                "csv_append": {},
                "guard_pre_ordinal": 1,
                "guard_post_ordinal": 2,
                "validation_error": None,
            }

        nonce = hashlib.sha256(b"terminal-perf-nonce").hexdigest()
        perf_events = [
            {
                "command": command,
                "nonce": nonce,
                "sent_monotonic_ns": sent,
                "ack": "ack",
                "ack_received_monotonic_ns": sent + 1,
            }
            for command, sent in (("enable", 31), ("disable", 41))
        ]
        perf_stat = output / "profiles" / "terminal.perf.csv"
        perf_ack = output / "profiles" / "terminal.perf.ack"
        write_fixture(perf_stat, b"1,,cycles,1,100.00\n")
        write_fixture(perf_ack, b"ack\nack\n")
        cpu_context = {"variant": "A", "row_ordinal": 1}
        cpu_environment = {
            "ASTERISM_REBASELINE_STORE": str(output / "stores" / "cpu"),
            "ASTERISM_REBASELINE_CONTROL_FD": "3",
            "ASTERISM_REBASELINE_PERF_PERMISSION_RESULT": (
                "available;perf_event_paranoid=2;scope=user-only"
            ),
            "ASTERISM_REBASELINE_PERF_COMMAND_FD": "4",
            "ASTERISM_REBASELINE_PERF_ACK_FD": "5",
            "ASTERISM_REBASELINE_PERF_ACK_LEDGER_FD": "6",
        }
        cpu_inputs = {
            "schedstat_resolution_ns": 1,
            "perf_permission": cpu_environment[
                "ASTERISM_REBASELINE_PERF_PERMISSION_RESULT"
            ],
            "perf_control_events": perf_events,
            "perf_raw_artifacts": {
                "stat": raw_binding(perf_stat),
                "ack": raw_binding(perf_ack),
            },
        }
        cpu_control = [
            {
                "phase": "ready",
                "_runner_received_monotonic_ns": 30,
            },
            {
                "command": "start",
                "nonce": nonce,
                "_runner_sent_monotonic_ns": 33,
            },
            {
                "phase": "measured",
                "nonce": nonce,
                "t1_monotonic_ns": 40,
                "counter_end_monotonic_ns": 43,
                "_runner_received_monotonic_ns": 44,
                "perf_disable": perf_events[1],
            }
        ]
        terminal_children.append(
            profile_child(
                1,
                "cpu_profiles",
                cpu_context,
                cpu_environment,
                cpu_inputs,
                cpu_control,
            )
        )
        trace_store = output / "stores" / "trace"
        trace_context = {
            "variant": "A",
            "row_ordinal": 1,
            "variant_trace_path_markers": schema.resolved_trace_path_markers(
                trace_store, "A"
            ),
        }
        trace_path = output / "profiles" / "terminal.strace"
        write_fixture(trace_path, b"fixture strace evidence\n")
        trace_inputs = {
            "trace_raw_artifact": raw_binding(trace_path),
            "log_path_markers": trace_context["variant_trace_path_markers"][
                "log"
            ],
            "metadata_path_markers": trace_context[
                "variant_trace_path_markers"
            ]["metadata"],
        }
        terminal_children.append(
            profile_child(
                2,
                "syscall_profiles",
                trace_context,
                {
                    "ASTERISM_REBASELINE_STORE": str(trace_store),
                    "ASTERISM_REBASELINE_CONTROL_FD": "3",
                },
                trace_inputs,
                [],
            )
        )

        def non_row_child(
            ordinal: int,
            kind: str,
            context: dict[str, Any],
            argv: list[str],
        ) -> dict[str, Any]:
            record = json.loads(json.dumps(terminal_children[0]))
            raw_path = output / "raw" / kind / f"{ordinal:05d}.json"
            stderr_path = output / "raw" / kind / f"{ordinal:05d}.stderr"
            write_fixture_json(raw_path, {"fixture": kind})
            write_fixture(stderr_path, b"")
            context_sha256 = hashlib.sha256(
                canonical_json_bytes(context)
            ).hexdigest()
            pid = 10_000 + ordinal
            start_ticks = 20_000 + ordinal
            record.update(
                {
                    "ordinal": ordinal,
                    "kind": kind,
                    "context": context,
                    "context_sha256": context_sha256,
                    "argv": argv,
                    "environment": {"ASTERISM_REBASELINE_MODE": kind},
                    "waited_pid": pid,
                    "started_monotonic_ns": 20 + ordinal * 10,
                    "completed_monotonic_ns": 25 + ordinal * 10,
                    "control_events": [],
                    "control_events_sha256": hashlib.sha256(
                        canonical_json_bytes([])
                    ).hexdigest(),
                    "profile_events": [],
                    "profile_events_sha256": hashlib.sha256(
                        canonical_json_bytes([])
                    ).hexdigest(),
                    "profile_rich_result": None,
                    "runner_context": None,
                    "runner_context_sha256": None,
                    "profile_result": None,
                    "profile_result_sha256": None,
                    "profile_tool_inputs": {},
                    "profile_tool_inputs_sha256": hashlib.sha256(
                        canonical_json_bytes({})
                    ).hexdigest(),
                    "profile_tool_helper_records": [],
                    "raw_path": str(raw_path),
                    "raw_sha256": sha256_file(raw_path),
                    "raw_bytes": raw_path.stat().st_size,
                    "stderr_path": str(stderr_path),
                    "stderr_sha256": sha256_file(stderr_path),
                    "stderr_bytes": 0,
                    "combined_row_sha256": None,
                    "csv_append": None,
                }
            )
            record["identity"].update(
                {"pid": pid, "pgrp": pid, "session": pid, "starttime_ticks": start_ticks}
            )
            record["reaping"] = {
                "pid": pid,
                "start_ticks": start_ticks,
                "status": "absent",
            }
            return record

        binary = prepared_variants["A"]["binary"]["path"]
        terminal_children.extend(
            (
                non_row_child(
                    3,
                    "contract",
                    {"transition": "contract", "variant": "A"},
                    [binary, "--contract"],
                ),
                non_row_child(
                    4,
                    "smoke",
                    {
                        "transition": "smoke",
                        "smoke_target": "primary",
                        "variant": "A",
                    },
                    [binary, "--smoke"],
                ),
                non_row_child(
                    5,
                    "correctness",
                    {
                        "transition": "correctness",
                        "suite": "current",
                        "variant": "A",
                        "phase": "pre",
                    },
                    [binary, "--correctness-oracle"],
                ),
            )
        )
        write_fixture(
            output / "child-manifest.jsonl",
            b"".join(
                canonical_json_bytes(child) for child in terminal_children
            ),
        )
    correctness_children: list[dict[str, Any]] = []
    if correctness_only:
        descriptors = schema.correctness_descriptors()
        group_bindings: dict[tuple[str, str, str, str], tuple[int, Path]] = {}
        for ordinal, group in enumerate(schema.CORRECTNESS_GROUPS, start=1):
            variant, phase, suite, kind = group
            group_descriptors = [
                descriptor
                for descriptor in descriptors
                if tuple(
                    descriptor[field]
                    for field in ("variant", "phase", "suite", "kind")
                ) == group
            ]
            raw_cases = []
            for descriptor in group_descriptors:
                failed = (
                    historical_failure and variant == "C"
                ) or (
                    not historical_failure
                    and variant == "A"
                    and phase in {"pre", "post"}
                    and descriptor["id"] == current_case
                )
                raw_cases.append(
                    {
                        "id": descriptor["id"],
                        "classification": descriptor["classification"],
                        "status": "FAIL" if failed else "PASS",
                    }
                )
            raw = {
                "schema": schema.CORRECTNESS_CHILD_SCHEMA,
                "protocol": schema.PROTOCOL,
                "attempt_nonce": attempt_nonce,
                "variant": variant,
                "phase": phase,
                "suite": suite,
                "harness_sound": True,
                "boundedness": (
                    schema.CORRECTNESS_EXPECTED_BOUNDEDNESS
                    if variant == "A" and suite == "current-fault"
                    else None
                ),
                "cases": raw_cases,
            }
            raw_path = output / "raw" / kind / f"{ordinal:05d}.json"
            write_fixture_json(raw_path, raw)
            child = {
                "ordinal": ordinal,
                "kind": kind,
                "context": {
                    "transition": "correctness",
                    "suite": suite,
                    "variant": variant,
                    "phase": phase,
                },
                "raw_path": str(raw_path),
                "raw_sha256": sha256_file(raw_path),
            }
            correctness_children.append(child)
            group_bindings[group] = (ordinal, raw_path)
        aggregate_cases = []
        for descriptor in descriptors:
            group = tuple(
                descriptor[field] for field in ("variant", "phase", "suite", "kind")
            )
            ordinal, raw_path = group_bindings[group]
            raw = json.loads(raw_path.read_bytes())
            raw_case = next(
                case for case in raw["cases"] if case["id"] == descriptor["id"]
            )
            aggregate_cases.append(
                {
                    **descriptor,
                    "status": raw_case["status"],
                    "child_ordinal": ordinal,
                    "output_path": str(raw_path),
                    "output_sha256": sha256_file(raw_path),
                }
            )
        correctness_value = {
            "schema": schema.CORRECTNESS_SCHEMA,
            "protocol": schema.PROTOCOL,
            "attempt_nonce": attempt_nonce,
            "harness_sound": True,
            "boundedness": schema.CORRECTNESS_EXPECTED_BOUNDEDNESS,
            "cases": aggregate_cases,
        }
        write_fixture_json(output / "correctness.json", correctness_value)
        marker = {
            "schema": schema.CORRECTNESS_ONLY_SCHEMA,
            "protocol": schema.PROTOCOL,
            "attempt_nonce": attempt_nonce,
            "trigger": "historical" if historical_failure else "current",
            "current_pre_failed_case_ids": [] if historical_failure else [current_case],
            "current_post_failed_case_ids": [] if historical_failure else [current_case],
            "historical_failed_cases": (
                [
                    {
                        "variant": "C",
                        "phase": "oracle",
                        "id": "public-common-oracle",
                    }
                ]
                if historical_failure
                else []
            ),
            "timing_child_records": 0,
            "created_at": "2026-07-15T02:00:00+00:00",
            "created_monotonic_ns": 105,
        }
        write_fixture_json(output / "correctness-only.json", marker)
        write_fixture(
            output / "child-manifest.jsonl",
            b"".join(canonical_json_bytes(child) for child in correctness_children),
        )
        terminal_children = correctness_children
    result = {
        "schema": schema.RESULT_SCHEMA, "protocol": schema.PROTOCOL,
        "evidence_mode": evidence_mode, "outcome": decision_outcome,
        "exit_code": decision_exit, "evidence_valid": True,
        "matrix_complete": not correctness_only, "errors": [],
        "gate_failures": [], "gates": [], "summary": {
            "report_data": (
                {
                    "correctness_only": True,
                    "timing_rows": 0,
                    "current_pre_failed_case_ids": marker["current_pre_failed_case_ids"],
                    "current_post_failed_case_ids": marker["current_post_failed_case_ids"],
                    "historical_failed_cases": marker["historical_failed_cases"],
                    "bounds_reproduced": True,
                }
                if correctness_only
                else {}
            )
        }, "artifacts": {},
        "evaluated_at": "2026-07-15T02:00:00+00:00",
        "evaluator_path": str(evaluator), "evaluator_sha256": sha256_file(evaluator),
    }
    write_fixture_json(output / "result.json", result)
    stdout_path = output / "terminal-chain" / "evaluator.stdout"
    stderr_path = output / "terminal-chain" / "evaluator.stderr"
    write_fixture(stdout_path, canonical_json_bytes(result))
    write_fixture(stderr_path, b"")
    lease = {
        "path": str(Path.home() / ".cache/mess-bench/global-measurement.lock"),
        "device": 11, "inode": 12, "holder_pid": 101,
        "holder_start_ticks": 202, "nonce": lease_nonce,
        "acquired_at": "2026-07-15T00:00:02+00:00",
        "acquired_monotonic_ns": 20,
    }
    runner_identity = {
        "pid": 101, "comm": comms["runner_runtime"], "state": "R", "ppid": 1,
        "pgrp": 101, "session": 101, "starttime_ticks": 202,
    }
    provenance = {
        "protocol": schema.PROTOCOL, "attempt_nonce": attempt_nonce,
        "evidence_mode": evidence_mode,
        "lease": lease, "host": {"runner": runner_identity},
    }
    write_fixture_json(output / "provenance.json", provenance)
    runner_process = {
        **runner_identity, "uid": os.getuid(), "exe": tools["runner_runtime"]["path"],
        "exe_sha256": tools["runner_runtime"]["sha256"], "cmdline": "fixture runner",
        "read_errors": [], "classification": "runner",
        "observed_at": "2026-07-15T02:00:00+00:00",
    }

    def snapshot(label: str, ordinal: int, start: int, end: int) -> dict[str, Any]:
        return {
            "schema": schema.GUARD_SCHEMA, "protocol": schema.PROTOCOL,
            "ordinal": ordinal, "label": label, "tracked_comm": sorted(comms.values()),
            "runner": runner_identity, "active_child": None, "active_helpers": [],
            "records": [runner_process], "final_resource": {
                "load1": 0.0, "free_bytes": 200_000_000_000,
                "free_inodes": 2_000_000, "enforced": True,
            },
            "preidentity_vanished": [], "verdict": "pass",
            "started_at": "2026-07-15T02:00:00+00:00", "started_monotonic_ns": start,
            "completed_at": "2026-07-15T02:00:00+00:00", "completed_monotonic_ns": end,
        }

    pre_snapshot = snapshot("pre-evaluator", 1, 90, 100)
    pre_snapshot_path = output / "guards" / "00001-pre-evaluator.json"
    write_fixture_json(pre_snapshot_path, pre_snapshot)
    guard = {
        "schema": schema.GUARD_BINDING_SCHEMA, "protocol": schema.PROTOCOL,
        "kind": "process_guard", "ordinal": 1, "label": "pre-evaluator",
        "path": str(pre_snapshot_path), "sha256": sha256_file(pre_snapshot_path),
        "verdict": "pass", "started_monotonic_ns": 90, "completed_monotonic_ns": 100,
    }
    write_fixture(output / "guard-manifest.jsonl", canonical_json_bytes(guard))
    post_snapshot = snapshot("post-evaluator", 2, 121, 122)
    post_path = output / "terminal-chain" / "post-evaluator.json"
    write_fixture_json(post_path, post_snapshot)
    result["artifacts"] = {
        name: {
            "sha256": sha256_file(output / name),
            "bytes": (output / name).stat().st_size,
        }
        for name in schema.expected_result_artifact_names(
            correctness_only=correctness_only
        )
    }
    write_fixture_json(output / "result.json", result)
    write_fixture(stdout_path, canonical_json_bytes(result))
    evaluator_identity = {
        "pid": 303, "comm": comms["evaluator_runtime"], "state": "R", "ppid": 101,
        "pgrp": 303, "session": 303, "starttime_ticks": 404,
    }
    transition = {
        "schema": schema.EVALUATOR_TRANSITION_SCHEMA, "protocol": schema.PROTOCOL,
        "attempt_nonce": attempt_nonce, "pre_guard": guard,
        "child": {
            "argv": [
                tools["evaluator_runtime"]["path"],
                support["evaluator"]["path"],
                "--evaluate-correctness-only" if correctness_only else "--evaluate",
                str(output),
            ],
            "environment": dict(schema.EVALUATOR_TRANSITION_ENV),
            "runtime": {
                "path": tools["evaluator_runtime"]["path"], "sha256": tools["evaluator_runtime"]["sha256"],
                "mode": 0o555, "comm": tools["evaluator_runtime"]["comm"],
            },
            "support": support["evaluator"], "identity": evaluator_identity,
            "waited_pid": 303, "started_at": "2026-07-15T02:00:00+00:00",
            "started_monotonic_ns": 110, "completed_at": "2026-07-15T02:00:00+00:00",
            "completed_monotonic_ns": 120, "exit_status": decision_exit,
            "timed_out": False,
            "terminated_by_runner": False, "interrupted": None,
            "reaping": {"pid": 303, "start_ticks": 404, "status": "absent"},
            "process_group_absent": True, "orphan_process_group_detected": False,
            "stdout": {"path": str(stdout_path), "sha256": sha256_file(stdout_path), "bytes": stdout_path.stat().st_size, "mode": 0o444},
            "stderr": {"path": str(stderr_path), "sha256": sha256_file(stderr_path), "bytes": 0, "mode": 0o444},
        },
        "post_snapshot": {"path": str(post_path), "sha256": sha256_file(post_path)},
        "lease_held": {
            "path": lease["path"], "device": 11, "inode": 12, "holder_pid": 101,
            "holder_start_ticks": 202, "nonce": lease_nonce,
            "proc_locks_proof": "fixture exclusive lock", "second_exclusive_failed": True,
            "observed_at": "2026-07-15T02:00:00+00:00", "observed_monotonic_ns": 123,
        },
        "completed_at": "2026-07-15T02:00:00+00:00", "completed_monotonic_ns": 124,
    }
    transition_path = output / "evaluator-transition.json"
    write_fixture_json(transition_path, transition)
    inventory = current_inventory(output, [])
    write_fixture(
        output / "SHA256SUMS",
        b"".join(f"{item['sha256']}  {item['path']}\n".encode() for item in inventory),
    )
    pre = {
        "schema": schema.TERMINAL_PRE_RELEASE_SCHEMA, "protocol": schema.PROTOCOL,
        "attempt_nonce": attempt_nonce, "outcome": decision_outcome,
        "evaluator_exit": decision_exit,
        "result_path": str(output / "result.json"), "result_sha256": sha256_file(output / "result.json"),
        "provenance_path": str(output / "provenance.json"), "provenance_sha256": sha256_file(output / "provenance.json"),
        "report_path": str(output / "REPORT.md"), "report_sha256": sha256_file(output / "REPORT.md"),
        "sha256sums_path": str(output / "SHA256SUMS"), "sha256sums_sha256": sha256_file(output / "SHA256SUMS"),
        "artifact_inventory": inventory,
        "evaluator_transition": {"path": str(transition_path), "sha256": sha256_file(transition_path)},
        "guard_manifest_path": str(output / "guard-manifest.jsonl"), "guard_manifest_sha256": sha256_file(output / "guard-manifest.jsonl"),
        "guard_manifest_records": 1,
        "child_manifest_path": str(output / "child-manifest.jsonl"), "child_manifest_sha256": sha256_file(output / "child-manifest.jsonl"),
        "child_manifest_records": len(terminal_children), "lease": lease,
        "completed_at": "2026-07-15T02:00:00+00:00", "completed_monotonic_ns": 130,
    }
    pre_path = output / "terminal-pre-release.json"
    write_fixture_json(pre_path, pre)
    release = {
        "schema": schema.LEASE_RELEASE_SCHEMA, "protocol": schema.PROTOCOL,
        "event": "released", "attempt_nonce": attempt_nonce, "lease_nonce": lease_nonce,
        "lease_path": lease["path"], "lease_device": 11, "lease_inode": 12,
        "outcome": decision_outcome,
        "released_at": "2026-07-15T02:00:00+00:00",
        "released_monotonic_ns": 140,
    }
    release_path = output / "lease-release.json"
    write_fixture_json(release_path, release)
    terminal = {
        "schema": schema.TERMINAL_SCHEMA, "protocol": schema.PROTOCOL,
        "attempt_nonce": attempt_nonce, "outcome": decision_outcome,
        "terminal_pre_release_path": str(pre_path), "terminal_pre_release_sha256": sha256_file(pre_path),
        "lease_release_path": str(release_path), "lease_release_sha256": sha256_file(release_path),
        "result_path": str(output / "result.json"), "result_sha256": sha256_file(output / "result.json"),
        "provenance_path": str(output / "provenance.json"), "provenance_sha256": sha256_file(output / "provenance.json"),
        "sha256sums_path": str(output / "SHA256SUMS"), "sha256sums_sha256": sha256_file(output / "SHA256SUMS"),
        "artifact_inventory_sha256": hashlib.sha256(canonical_json_bytes(inventory)).hexdigest(),
        "runner": {
            "identity": runner_identity,
            "runtime": {"path": tools["runner_runtime"]["path"], "sha256": tools["runner_runtime"]["sha256"], "mode": 0o555, "comm": tools["runner_runtime"]["comm"]},
            "support": support["runner"],
            "cmdline": [
                tools["runner_runtime"]["path"], support["runner"]["path"],
                "--prepared-artifacts", str(output / "prepared-artifacts.json"),
                "--output", str(output),
            ],
        },
        "terminal_published_at": "2026-07-15T02:00:00+00:00",
        "terminal_published_monotonic_ns": 150,
    }
    write_fixture_json(output / "terminal.json", terminal)
    return output


def self_test() -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def check(name: str, action: Any) -> None:
        try:
            passed = bool(action())
            detail = ""
        except Exception as error:
            passed = False
            detail = repr(error)
        checks.append({"name": name, "pass": passed, "detail": detail})

    with tempfile.TemporaryDirectory(prefix="bn-2l3n-terminal-selftest-") as temp:
        output = build_terminal_fixture_v3(Path(temp))
        positive, rc = verify(output, publish=False)
        checks.append(
            {
                "name": "terminal-positive-chain",
                "pass": rc == 0 and positive["outcome"] == "TERMINAL_VERIFIED",
                "detail": "" if rc == 0 else repr(positive["errors"][:20]),
            }
        )
        attempt_prepared_fixture = json.loads(
            (output / "prepared-artifacts.json").read_bytes()
        )
        claim_fixture_path = Path(
            attempt_prepared_fixture["single_use_claim"]["path"]
        )
        claim_fixture = json.loads(claim_fixture_path.read_bytes())
        original_prepared_fixture_path = Path(
            claim_fixture["prepared_artifacts_path"]
        )
        original_approval_fixture_path = Path(
            attempt_prepared_fixture["source_approval"]["path"]
        )
        check(
            "terminal-producer-real-original-and-attempt-authority-copies",
            lambda: (
                original_prepared_fixture_path
                != output / "prepared-artifacts.json"
                and original_approval_fixture_path
                != output / "source-approval.json"
                and original_prepared_fixture_path.read_bytes()
                == (output / "prepared-artifacts.json").read_bytes()
                and original_approval_fixture_path.read_bytes()
                == (output / "source-approval.json").read_bytes()
                and original_approval_fixture_path.relative_to(
                    original_prepared_fixture_path.parent
                ).parts
                == schema.PREPARED_SOURCE_APPROVAL_RELATIVE_PATH
            ),
        )
        check(
            "terminal-producer-manifest-source-approval-binding-layout",
            lambda: (
                attempt_prepared_fixture["source_approval"]["path"]
                == str(
                    original_prepared_fixture_path.parent.joinpath(
                        *schema.PREPARED_SOURCE_APPROVAL_RELATIVE_PATH
                    )
                )
                and stat.S_IMODE(
                    original_approval_fixture_path.parent.stat().st_mode
                )
                == 0o555
            ),
        )

        def terminal_authority_file_mutation(
            path: Path,
            mutator: Any,
            expected_error: str,
        ) -> bool:
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                value = json.loads(original)
                mutator(value)
                write_fixture_json(path, value, mode)
                _BOUND_SNAPSHOTS.clear()
                result, mutation_rc = verify(output, publish=False)
                return mutation_rc == EXIT_INVALID and any(
                    expected_error in error for error in result["errors"]
                )
            finally:
                write_fixture(path, original, mode)
                _BOUND_SNAPSHOTS.clear()

        check(
            "terminal-mutation-source-review-seal-binding",
            lambda: terminal_authority_file_mutation(
                Path(
                    attempt_prepared_fixture["source_review"]["bundle"]["path"]
                ),
                lambda value: value["verdict"]["data"].__setitem__(
                    "vote", "reject"
                ),
                "prepared source review bundle bytes differ from binding",
            ),
        )
        check(
            "terminal-mutation-release-proof-binding",
            lambda: terminal_authority_file_mutation(
                Path(attempt_prepared_fixture["release_compile_out"]["path"]),
                lambda value: value.__setitem__("status", "forged"),
                "prepared release compile-out proof bytes differ from binding",
            ),
        )

        approval_fixture = json.loads(original_approval_fixture_path.read_bytes())
        bundle_fixture = json.loads(
            Path(attempt_prepared_fixture["source_review"]["bundle"]["path"])
            .read_bytes()
        )
        current_children_fixture = json.loads(
            Path(
                attempt_prepared_fixture["source_review"][
                    "current_children_attestation"
                ]["path"]
            ).read_bytes()
        )
        lock_authority_fixture = json.loads(
            Path(
                attempt_prepared_fixture["source_review"]["lock_authority"]["path"]
            ).read_bytes()
        )
        lock_review_fixture = json.loads(
            Path(
                attempt_prepared_fixture["source_review"]["lock_review_bundle"][
                    "path"
                ]
            ).read_bytes()
        )
        proof_fixture = json.loads(
            Path(attempt_prepared_fixture["release_compile_out"]["path"])
            .read_bytes()
        )
        config_fixture = json.loads((output / "config.json").read_bytes())

        def reseal_source_bundle(
            approval_value: dict[str, Any], bundle_value: dict[str, Any]
        ) -> None:
            assertion_sha256 = hashlib.sha256(
                canonical_json_bytes(bundle_value["assertion"])
            ).hexdigest()
            bundle_value["assertion_sha256"] = assertion_sha256
            approval_value["source_review"]["assertion_sha256"] = assertion_sha256
            bundle_value["verdict"]["data"]["reason"] = (
                f"APPROVED assertion_sha256={assertion_sha256}; open_findings=0"
            )

        def terminal_local_source_mutation(
            mutator: Any, expected_error: str
        ) -> bool:
            approval_value = json.loads(json.dumps(approval_fixture))
            bundle_value = json.loads(json.dumps(bundle_fixture))
            current_value = json.loads(json.dumps(current_children_fixture))
            lock_value = json.loads(json.dumps(lock_authority_fixture))
            lock_review_value = json.loads(json.dumps(lock_review_fixture))
            mutator(
                approval_value,
                bundle_value,
                current_value,
                lock_value,
                lock_review_value,
            )
            semantic_errors: list[str] = []
            validate_terminal_source_review_semantics(
                approval_value,
                bundle_value,
                current_value,
                lock_value,
                lock_review_value,
                semantic_errors,
            )
            return any(expected_error in error for error in semantic_errors)

        def alias_reviewed_inputs(
            approval_value: dict[str, Any],
            bundle_value: dict[str, Any],
            _current: dict[str, Any],
            _lock: dict[str, Any],
            _lock_review: dict[str, Any],
        ) -> None:
            inputs = bundle_value["assertion"]["inputs"]
            inputs["lock_manifest"]["path"] = inputs["lock_authority"]["path"]
            inputs["lock_manifest"]["identity"] = json.loads(
                json.dumps(inputs["lock_authority"]["identity"])
            )
            reseal_source_bundle(approval_value, bundle_value)

        check(
            "terminal-local-rejects-reviewed-input-alias",
            lambda: terminal_local_source_mutation(
                alias_reviewed_inputs, "inputs are not physically disjoint"
            ),
        )

        def scalar_reviewed_input(
            approval_value: dict[str, Any],
            bundle_value: dict[str, Any],
            _current: dict[str, Any],
            _lock: dict[str, Any],
            _lock_review: dict[str, Any],
        ) -> None:
            bundle_value["assertion"]["inputs"]["lock_authority"] = "malformed"
            reseal_source_bundle(approval_value, bundle_value)

        check(
            "terminal-local-rejects-scalar-reviewed-input",
            lambda: terminal_local_source_mutation(
                scalar_reviewed_input,
                "source-review input lock_authority fields are not exact",
            ),
        )
        check(
            "terminal-local-rejects-seal-semantic-vote",
            lambda: terminal_local_source_mutation(
                lambda _approval, bundle, _current, _lock, _review: bundle[
                    "verdict"
                ]["data"].__setitem__("vote", "reject"),
                "ReviewerVoted authority differs",
            ),
        )
        check(
            "terminal-local-rejects-embedded-lock-review-payload",
            lambda: terminal_local_source_mutation(
                lambda _approval, _bundle, _current, lock, _review: lock[
                    "review_bundle"
                ].__setitem__("payload", {"schema": "forged"}),
                "prepared lock authority differs",
            ),
        )
        check(
            "terminal-local-rejects-preapproval-semantic-crosslink",
            lambda: terminal_local_source_mutation(
                lambda _approval, _bundle, current, _lock, _review: current[
                    "release_compile_out_approval"
                ].__setitem__("source_approval_status", "source-approved"),
                "preapproval authority differs",
            ),
        )

        def terminal_local_proof_mutation(
            mutator: Any, expected_error: str
        ) -> bool:
            proof_value = json.loads(json.dumps(proof_fixture))
            prepared_value = json.loads(json.dumps(attempt_prepared_fixture))
            config_value = json.loads(json.dumps(config_fixture))
            mutator(proof_value, prepared_value, config_value)
            semantic_errors: list[str] = []
            _BOUND_SNAPSHOTS.clear()
            validate_terminal_release_proof_semantics(
                proof_value,
                prepared_value,
                approval_fixture,
                current_children_fixture,
                config_value,
                semantic_errors,
            )
            _BOUND_SNAPSHOTS.clear()
            return any(expected_error in error for error in semantic_errors)

        def mutate_overlay_attestation_nonce(
            proof_value: dict[str, Any],
            _prepared: dict[str, Any],
            _config: dict[str, Any],
        ) -> None:
            build = proof_value["builds"]["overlay_a"]
            build["attestation"]["build_nonce"] = "0" * 64
            build["attestation_sha256"] = hashlib.sha256(
                canonical_json_bytes(build["attestation"])
            ).hexdigest()

        check(
            "terminal-local-rejects-rehashed-overlay-attestation-nonce",
            lambda: terminal_local_proof_mutation(
                mutate_overlay_attestation_nonce, "embedded authority differs"
            ),
        )
        check(
            "terminal-local-rejects-inventory-identity-alias",
            lambda: terminal_local_proof_mutation(
                lambda proof, _prepared, _config: proof["symbol_inventories"].__setitem__(
                    "overlay_a",
                    json.loads(json.dumps(proof["symbol_inventories"]["ordinary_a"])),
                ),
                "inventories are not equal physically-disjoint files",
            ),
        )
        check(
            "terminal-local-rejects-nm-argv-semantic-mutation",
            lambda: terminal_local_proof_mutation(
                lambda proof, _prepared, _config: proof["nm"]["ordinary_a"][
                    "argv"
                ].__setitem__(1, "--undefined-only"),
                "nm child ordinary_a did not complete exactly",
            ),
        )
        check(
            "terminal-local-rejects-published-a-rebound",
            lambda: terminal_local_proof_mutation(
                lambda proof, _prepared, _config: proof.__setitem__(
                    "published_a_sha256", "0" * 64
                ),
                "published ordinary A binding differs",
            ),
        )
        check(
            "terminal-local-rejects-config-reachable-proof-twin",
            lambda: terminal_local_proof_mutation(
                lambda proof, _prepared, config: config.__setitem__(
                    "argv_templates", [proof["binaries"]["overlay_a"]["path"]]
                ),
                "proof-only overlay A is config-reachable",
            ),
        )

        projection_children = read_jsonl(
            output / "child-manifest.jsonl",
            "terminal projection fixture children",
            [],
        )

        def terminal_completed_child_twin_rejected() -> bool:
            proof = json.loads(
                Path(
                    attempt_prepared_fixture["release_compile_out"]["path"]
                ).read_bytes()
            )
            hostile_children = json.loads(json.dumps(projection_children))
            hostile_children[-1]["argv"].append(
                proof["binaries"]["overlay_a"]["path"]
            )
            reachability_errors: list[str] = []
            validate_terminal_proof_only_child_reachability(
                hostile_children,
                proof["binaries"]["overlay_a"]["path"],
                reachability_errors,
            )
            return any(
                "completed child manifest" in error
                for error in reachability_errors
            )

        check(
            "terminal-mutation-release-twin-final-child-reachable",
            terminal_completed_child_twin_rejected,
        )
        check(
            "terminal-full-fixture-covers-null-non-row-results",
            lambda: {
                record.get("kind")
                for record in projection_children
                if record.get("profile_result") is None
                and record.get("profile_result_sha256") is None
            }
            >= {"contract", "smoke", "correctness"},
        )
        check(
            "terminal-ordinary-smoke-target-is-not-profile-authority",
            lambda: terminal_profile_track(
                next(
                    record
                    for record in projection_children
                    if record.get("kind") == "smoke"
                )
            )
            is None,
        )

        def projection_mutation(mutator: Any, expected_error: str) -> bool:
            records = json.loads(json.dumps(projection_children))
            mutator(records)
            projection_errors: list[str] = []
            _BOUND_SNAPSHOTS.clear()
            validate_terminal_child_projection(
                records,
                output,
                projection_errors,
                correctness_only=False,
            )
            return any(expected_error in error for error in projection_errors)

        check(
            "terminal-projection-rejects-profile-input-hash-mutation",
            lambda: projection_mutation(
                lambda records: records[0]["profile_tool_inputs"].__setitem__(
                    "schedstat_resolution_ns", 2
                ),
                "profile tool input hash differs",
            ),
        )
        check(
            "terminal-projection-rejects-old-raw-artifact-list",
            lambda: projection_mutation(
                lambda records: records[0].__setitem__(
                    "profile_tool_raw_artifacts", []
                ),
                "keys are not exact",
            ),
        )

        def mutate_trace_authority(records: list[dict[str, Any]]) -> None:
            record = records[1]
            markers = record["context"]["variant_trace_path_markers"]
            markers["log"][0]["kind"] = "exact"
            record["profile_tool_inputs"]["log_path_markers"] = markers["log"]
            record["context_sha256"] = hashlib.sha256(
                canonical_json_bytes(record["context"])
            ).hexdigest()
            record["profile_tool_inputs_sha256"] = hashlib.sha256(
                canonical_json_bytes(record["profile_tool_inputs"])
            ).hexdigest()

        check(
            "terminal-projection-rejects-typed-trace-authority-mutation",
            lambda: projection_mutation(
                mutate_trace_authority,
                "trace markers differ from source templates",
            ),
        )

        def mutate_perf_disable(records: list[dict[str, Any]]) -> None:
            record = records[0]
            measured = next(
                event
                for event in record["control_events"]
                if event.get("phase") == "measured"
            )
            measured["perf_disable"]["ack"] = "bad"
            record["control_events_sha256"] = hashlib.sha256(
                canonical_json_bytes(record["control_events"])
            ).hexdigest()

        check(
            "terminal-projection-rejects-child-perf-disable-mutation",
            lambda: projection_mutation(
                mutate_perf_disable,
                "child/perf disable projection differs",
            ),
        )

        def mutate_perf_nonce(records: list[dict[str, Any]]) -> None:
            record = records[0]
            events = record["profile_tool_inputs"]["perf_control_events"]
            for event in events:
                event["nonce"] = "z" * 64
            start = next(
                event
                for event in record["control_events"]
                if event.get("command") == "start"
            )
            measured = next(
                event
                for event in record["control_events"]
                if event.get("phase") == "measured"
            )
            start["nonce"] = "z" * 64
            measured["perf_disable"] = dict(events[1])
            record["profile_tool_inputs_sha256"] = hashlib.sha256(
                canonical_json_bytes(record["profile_tool_inputs"])
            ).hexdigest()
            record["control_events_sha256"] = hashlib.sha256(
                canonical_json_bytes(record["control_events"])
            ).hexdigest()

        check(
            "terminal-projection-rejects-nonhex-perf-nonce",
            lambda: projection_mutation(
                mutate_perf_nonce,
                "perf event 0 differs",
            ),
        )

        def mutate_perf_order(records: list[dict[str, Any]]) -> None:
            record = records[0]
            disable = record["profile_tool_inputs"]["perf_control_events"][1]
            disable["sent_monotonic_ns"] = 32
            disable["ack_received_monotonic_ns"] = 33
            measured = next(
                event
                for event in record["control_events"]
                if event.get("phase") == "measured"
            )
            measured["perf_disable"] = dict(disable)
            record["profile_tool_inputs_sha256"] = hashlib.sha256(
                canonical_json_bytes(record["profile_tool_inputs"])
            ).hexdigest()
            record["control_events_sha256"] = hashlib.sha256(
                canonical_json_bytes(record["control_events"])
            ).hexdigest()

        check(
            "terminal-projection-rejects-perf-command-order",
            lambda: projection_mutation(
                mutate_perf_order,
                "perf control sequence differs",
            ),
        )

        def mutate_perf_t1_bound(records: list[dict[str, Any]]) -> None:
            record = records[0]
            measured = next(
                event
                for event in record["control_events"]
                if event.get("phase") == "measured"
            )
            measured["t1_monotonic_ns"] = 42
            record["control_events_sha256"] = hashlib.sha256(
                canonical_json_bytes(record["control_events"])
            ).hexdigest()

        check(
            "terminal-projection-rejects-perf-t1-bound",
            lambda: projection_mutation(
                mutate_perf_t1_bound,
                "perf control/lifecycle projection differs",
            ),
        )

        check(
            "terminal-projection-rejects-non-cpu-perf-environment",
            lambda: projection_mutation(
                lambda records: records[1]["environment"].__setitem__(
                    "ASTERISM_REBASELINE_PERF_PERMISSION_RESULT",
                    "available;perf_event_paranoid=2;scope=user-only",
                ),
                "non-CPU child has perf environment",
            ),
        )

        check(
            "terminal-projection-rejects-profile-authority-mutation",
            lambda: projection_mutation(
                lambda records: records[0]["profile_rich_result"][
                    "authority"
                ].__setitem__("source_tree", "0" * 40),
                "profile authority source_tree differs",
            ),
        )

        check(
            "terminal-projection-rejects-profile-result-hash-mutation",
            lambda: projection_mutation(
                lambda records: records[0].__setitem__(
                    "profile_result", {"forged": True}
                ),
                "profile result/hash differs",
            ),
        )

        def mutate_non_row_profile_result(records: list[dict[str, Any]]) -> None:
            record = next(
                item for item in records if item.get("kind") == "contract"
            )
            record["profile_result"] = {}
            record["profile_result_sha256"] = hashlib.sha256(
                canonical_json_bytes({})
            ).hexdigest()

        check(
            "terminal-projection-rejects-non-row-profile-result",
            lambda: projection_mutation(
                mutate_non_row_profile_result,
                "non-row profile result/hash is not null",
            ),
        )

        def mutate_measured_nonce(records: list[dict[str, Any]]) -> None:
            record = records[0]
            measured = next(
                event
                for event in record["control_events"]
                if event.get("phase") == "measured"
            )
            measured["nonce"] = "0" * 64
            record["control_events_sha256"] = hashlib.sha256(
                canonical_json_bytes(record["control_events"])
            ).hexdigest()

        check(
            "terminal-projection-rejects-measured-nonce-mutation",
            lambda: projection_mutation(
                mutate_measured_nonce,
                "CPU control nonce projection differs",
            ),
        )

        def result_artifact_mutation() -> bool:
            path = output / "child-manifest.jsonl"
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                path.chmod(0o644)
                path.write_bytes(original + b"{}\n")
                path.chmod(mode)
                result_value = json.loads((output / "result.json").read_bytes())
                artifact_errors: list[str] = []
                _BOUND_SNAPSHOTS.clear()
                validate_terminal_result_artifacts(
                    result_value,
                    output,
                    artifact_errors,
                    correctness_only=False,
                )
                return any(
                    "artifact child-manifest.jsonl differs" in error
                    for error in artifact_errors
                )
            finally:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)
                _BOUND_SNAPSHOTS.clear()

        check(
            "terminal-result-binds-profile-child-manifest",
            result_artifact_mutation,
        )

        def transition_context_delegation_mutation() -> bool:
            path = output / "child-manifest.jsonl"
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                records = [json.loads(line) for line in original.splitlines()]
                transition = next(
                    record
                    for record in records
                    if record.get("kind") == "contract"
                )
                transition["context"]["transition"] = "forged"
                transition["context_sha256"] = hashlib.sha256(
                    canonical_json_bytes(transition["context"])
                ).hexdigest()
                path.chmod(0o644)
                path.write_bytes(
                    b"".join(canonical_json_bytes(record) for record in records)
                )
                path.chmod(mode)
                result_value = json.loads((output / "result.json").read_bytes())
                artifact_errors: list[str] = []
                _BOUND_SNAPSHOTS.clear()
                validate_terminal_result_artifacts(
                    result_value,
                    output,
                    artifact_errors,
                    correctness_only=False,
                )
                return any(
                    "artifact child-manifest.jsonl differs" in error
                    for error in artifact_errors
                )
            finally:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)
                _BOUND_SNAPSHOTS.clear()

        check(
            "terminal-delegates-transition-semantics-only-through-bound-evaluator-result",
            transition_context_delegation_mutation,
        )

        def mutate_json(path: Path, mutator: Any) -> bool:
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                value = json.loads(original)
                mutator(value)
                path.chmod(0o644)
                path.write_bytes(canonical_json_bytes(value))
                path.chmod(mode)
                result, code = verify(output, publish=False)
                return code == EXIT_INVALID and result["outcome"] == "TERMINAL_INVALID"
            finally:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)

        def mutate_bytes(path: Path, data: bytes) -> bool:
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                path.chmod(0o644)
                path.write_bytes(data)
                path.chmod(mode)
                result, code = verify(output, publish=False)
                return code == EXIT_INVALID and result["outcome"] == "TERMINAL_INVALID"
            finally:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)

        def mutate_json_expect(
            path: Path, mutator: Any, expected_error: str
        ) -> bool:
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                value = json.loads(original)
                mutator(value)
                path.chmod(0o644)
                path.write_bytes(canonical_json_bytes(value))
                path.chmod(mode)
                result, code = verify(output, publish=False)
                return (
                    code == EXIT_INVALID
                    and any(
                        expected_error in error for error in result["errors"]
                    )
                )
            finally:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)

        def hardlink_alias_expect(
            original_path: Path,
            attempt_path: Path,
            expected_error: str,
        ) -> bool:
            attempt_bytes = attempt_path.read_bytes()
            attempt_mode = stat.S_IMODE(attempt_path.stat().st_mode)
            try:
                attempt_path.unlink()
                os.link(original_path, attempt_path)
                result, code = verify(output, publish=False)
                return code == EXIT_INVALID and any(
                    expected_error in error for error in result["errors"]
                )
            finally:
                if attempt_path.exists():
                    attempt_path.unlink()
                write_fixture(attempt_path, attempt_bytes, attempt_mode)

        def root_level_source_approval_rejected() -> bool:
            prepared_root = original_prepared_fixture_path.parent
            root_level_approval = prepared_root / "source-approval.json"
            attempt_prepared_path = output / "prepared-artifacts.json"
            original_prepared_bytes = original_prepared_fixture_path.read_bytes()
            attempt_prepared_bytes = attempt_prepared_path.read_bytes()
            claim_bytes = claim_fixture_path.read_bytes()
            root_mode = stat.S_IMODE(prepared_root.stat().st_mode)
            try:
                prepared_root.chmod(0o755)
                write_fixture(
                    root_level_approval,
                    original_approval_fixture_path.read_bytes(),
                )
                prepared_root.chmod(root_mode)
                value = json.loads(original_prepared_bytes)
                value["source_approval"]["path"] = str(root_level_approval)
                rebound = canonical_json_bytes(value)
                for path in (original_prepared_fixture_path, attempt_prepared_path):
                    path.chmod(0o644)
                    path.write_bytes(rebound)
                    path.chmod(0o444)
                claim = json.loads(claim_bytes)
                claim["prepared_artifacts_sha256"] = hashlib.sha256(
                    rebound
                ).hexdigest()
                claim_fixture_path.chmod(0o644)
                claim_fixture_path.write_bytes(canonical_json_bytes(claim))
                claim_fixture_path.chmod(0o444)
                result, code = verify(output, publish=False)
                return code == EXIT_INVALID and any(
                    "terminal original source approval path differs" in error
                    for error in result["errors"]
                )
            finally:
                for path, payload in (
                    (original_prepared_fixture_path, original_prepared_bytes),
                    (attempt_prepared_path, attempt_prepared_bytes),
                    (claim_fixture_path, claim_bytes),
                ):
                    path.chmod(0o644)
                    path.write_bytes(payload)
                    path.chmod(0o444)
                prepared_root.chmod(0o755)
                if root_level_approval.exists():
                    root_level_approval.unlink()
                prepared_root.chmod(root_mode)

        check(
            "terminal-mutation-attempt-original-prepared-hardlink-alias",
            lambda: hardlink_alias_expect(
                original_prepared_fixture_path,
                output / "prepared-artifacts.json",
                "terminal attempt/original prepared are hardlink aliases",
            ),
        )
        check(
            "terminal-mutation-attempt-original-source-hardlink-alias",
            lambda: hardlink_alias_expect(
                original_approval_fixture_path,
                output / "source-approval.json",
                "terminal attempt/original source approval are hardlink aliases",
            ),
        )
        check(
            "terminal-mutation-root-level-source-approval-layout-rejected",
            root_level_source_approval_rejected,
        )

        check(
            "terminal-mutation-attempt-original-prepared-divergence",
            lambda: mutate_json_expect(
                output / "prepared-artifacts.json",
                lambda value: value.__setitem__("created_monotonic_ns", 11),
                "terminal attempt/original prepared bytes differ",
            ),
        )
        check(
            "terminal-mutation-claim-rejects-attempt-prepared-path",
            lambda: mutate_json_expect(
                claim_fixture_path,
                lambda value: value.__setitem__(
                    "prepared_artifacts_path",
                    str(output / "prepared-artifacts.json"),
                ),
                "terminal claim original prepared path differs",
            ),
        )
        check(
            "terminal-mutation-attempt-original-source-divergence",
            lambda: mutate_json_expect(
                original_approval_fixture_path,
                lambda value: value.__setitem__("review_id", "cr-forged"),
                "terminal original source approval hash mismatch",
            ),
        )
        for field, value, expected_error in (
            ("output_dir", "/tmp/forged-output", "terminal claim output differs"),
            (
                "attempt_nonce",
                "0" * 64,
                "terminal claim attempt nonce differs",
            ),
            ("lease_nonce", "0" * 64, "terminal claim lease nonce differs"),
            (
                "claimed_at",
                "2020-01-01T00:00:00+00:00",
                "terminal claim chronology differs",
            ),
            ("claimed_monotonic_ns", 1, "terminal claim chronology differs"),
        ):
            check(
                f"terminal-mutation-claim-{field.replace('_', '-')}",
                lambda field=field, value=value, expected_error=expected_error: (
                    mutate_json_expect(
                        claim_fixture_path,
                        lambda claim: claim.__setitem__(field, value),
                        expected_error,
                    )
                ),
            )

        def mutate_mode(path: Path, mode: int) -> bool:
            original = stat.S_IMODE(path.stat().st_mode)
            try:
                path.chmod(mode)
                result, code = verify(output, publish=False)
                return code == EXIT_INVALID and result["outcome"] == "TERMINAL_INVALID"
            finally:
                path.chmod(original)

        def inject_unsafe_entry(kind: str) -> bool:
            path = output / f"unsafe-{kind}"
            target = output / "REPORT.md"
            try:
                if kind == "symlink":
                    path.symlink_to(target.name)
                elif kind == "symlink-dir":
                    path.symlink_to((output / "raw").name, target_is_directory=True)
                elif kind == "hidden":
                    path = output / ".hidden-artifact"
                    write_fixture(path, b"hidden\n")
                elif kind == "nonregular":
                    os.mkfifo(path, 0o444)
                else:
                    raise AssertionError(kind)
                result, code = verify(output, publish=False)
                return code == EXIT_INVALID and result["outcome"] == "TERMINAL_INVALID"
            finally:
                if path.exists() or path.is_symlink():
                    if not path.is_symlink() and stat.S_ISREG(path.lstat().st_mode):
                        path.chmod(0o644)
                    path.unlink()

        check(
            "terminal-mutation-result",
            lambda: mutate_json(output / "result.json", lambda value: value.__setitem__("outcome", "NARROW")),
        )
        check(
            "terminal-rejects-v2-result-schema",
            lambda: mutate_json(
                output / "result.json",
                lambda value: value.__setitem__(
                    "schema", "bn-2l3n-evaluation-result-v2"
                ),
            ),
        )
        check(
            "terminal-mutation-evaluator-argv",
            lambda: mutate_json(
                output / "evaluator-transition.json",
                lambda value: value["child"].__setitem__("argv", ["manual"]),
            ),
        )
        check(
            "terminal-mutation-evaluator-environment-injected-key",
            lambda: mutate_json(
                output / "evaluator-transition.json",
                lambda value: value["child"]["environment"].__setitem__(
                    "UNAPPROVED", "1"
                ),
            ),
        )
        check(
            "terminal-mutation-lease-nonce",
            lambda: mutate_json(output / "lease-release.json", lambda value: value.__setitem__("lease_nonce", "0" * 64)),
        )
        check(
            "terminal-mutation-publication-chronology",
            lambda: mutate_json(output / "terminal.json", lambda value: value.__setitem__("terminal_published_monotonic_ns", 1)),
        )
        check(
            "terminal-mutation-sha256sums",
            lambda: mutate_bytes(output / "SHA256SUMS", b"0" * 64 + b"  fake\n"),
        )
        check(
            "terminal-mutation-report",
            lambda: mutate_bytes(output / "REPORT.md", b"mutated report\n"),
        )
        check(
            "terminal-rejects-artifact-mode-change",
            lambda: mutate_mode(output / "REPORT.md", 0o644),
        )
        check(
            "terminal-rejects-artifact-symlink",
            lambda: inject_unsafe_entry("symlink"),
        )
        check(
            "terminal-rejects-artifact-symlink-directory",
            lambda: inject_unsafe_entry("symlink-dir"),
        )
        check(
            "terminal-rejects-hidden-artifact",
            lambda: inject_unsafe_entry("hidden"),
        )
        check(
            "terminal-rejects-nonregular-artifact",
            lambda: inject_unsafe_entry("nonregular"),
        )
        check(
            "terminal-mutation-approved-tools-manifest",
            lambda: mutate_json(
                output / "source-approval.json",
                lambda value: value["tools_manifest"]["tools"]["perf"].__setitem__(
                    "comm", "unapproved"
                ),
            ),
        )
        check(
            "terminal-mutation-source-role-lifetime",
            lambda: mutate_json(
                output / "source-approval.json",
                lambda value: value["variants"]["C"].__setitem__(
                    "profile_role_lifetime", "not_applicable"
                ),
            ),
        )
        check(
            "terminal-mutation-prepared-role-lifetime",
            lambda: mutate_json(
                output / "prepared-artifacts.json",
                lambda value: value["variants"]["C"]["contract"].__setitem__(
                    "profile_role_lifetime", "not_applicable"
                ),
            ),
        )
        check(
            "terminal-mutation-typed-trace-template",
            lambda: mutate_json(
                output / "source-approval.json",
                lambda value: value["variants"]["A"][
                    "trace_path_marker_templates"
                ]["log"][0].__setitem__("kind", "exact"),
            ),
        )
        check(
            "terminal-mutation-final-guard",
            lambda: mutate_bytes(
                output / "guard-manifest.jsonl",
                canonical_json_bytes(
                    {
                        "label": "pre-evaluator",
                        "verdict": "fail",
                        "active_child": None,
                        "completed_monotonic_ns": 100,
                    }
                ),
            ),
        )
        check(
            "terminal-mutation-nonfinite-rejected",
            lambda: _rejects_nonfinite(),
        )

        def inject_underscore_alias(alias: str, canonical: str) -> bool:
            path = output / alias
            try:
                write_fixture(path, (output / canonical).read_bytes())
                result, code = verify(output, publish=False)
                return code == EXIT_INVALID and result["outcome"] == "TERMINAL_INVALID"
            finally:
                if path.exists():
                    path.chmod(0o644)
                    path.unlink()

        check(
            "terminal-rejects-underscore-pre-release-alias",
            lambda: inject_underscore_alias(
                "terminal_pre_release.json", "terminal-pre-release.json"
            ),
        )
        check(
            "terminal-rejects-underscore-lease-release-alias",
            lambda: inject_underscore_alias(
                "lease_release.json", "lease-release.json"
            ),
        )

        terminal = json.loads((output / "terminal.json").read_bytes())
        recorded = terminal["runner"]["cmdline"]
        runtime = terminal["runner"]["runtime"]["path"]
        support = terminal["runner"]["support"]["path"]
        observed = [item.encode() for item in recorded]
        check(
            "terminal-rejects-runner-cmdline-tail-mutation",
            lambda: not runner_cmdline_matches(
                [*recorded, "--unapproved-tail"], runtime, support, observed
            ),
        )

        early_root = Path(temp) / "correctness-only-current"
        early_root.mkdir()
        early_output = build_terminal_fixture_v3(
            early_root, correctness_only=True
        )
        early_result, early_rc = verify(early_output, publish=False)
        checks.append(
            {
                "name": "terminal-correctness-only-revert-chain",
                "pass": (
                    early_rc == EXIT_VERIFIED
                    and early_result["outcome"] == "TERMINAL_VERIFIED"
                    and early_result["decision_outcome"] == "REVERT"
                ),
                "detail": repr(early_result["errors"][:20]),
            }
        )

        historical_root = Path(temp) / "correctness-only-historical"
        historical_root.mkdir()
        historical_output = build_terminal_fixture_v3(
            historical_root,
            correctness_only=True,
            historical_failure=True,
        )
        historical_result, historical_rc = verify(
            historical_output, publish=False
        )
        checks.append(
            {
                "name": "terminal-correctness-only-historical-inconclusive-chain",
                "pass": (
                    historical_rc == EXIT_VERIFIED
                    and historical_result["outcome"] == "TERMINAL_VERIFIED"
                    and historical_result["decision_outcome"] == "INCONCLUSIVE"
                ),
                "detail": repr(historical_result["errors"][:20]),
            }
        )

        def mutate_early_json(path: Path, mutator: Any) -> bool:
            original = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            try:
                value = json.loads(original)
                mutator(value)
                path.chmod(0o644)
                path.write_bytes(canonical_json_bytes(value))
                path.chmod(mode)
                result, code = verify(early_output, publish=False)
                return code == EXIT_INVALID and result["outcome"] == "TERMINAL_INVALID"
            finally:
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(mode)

        check(
            "terminal-correctness-only-marker-mutation",
            lambda: mutate_early_json(
                early_output / "correctness-only.json",
                lambda value: value.__setitem__("trigger", "historical"),
            ),
        )
        def mutate_current_aggregate(value: dict[str, Any]) -> None:
            case = next(
                item
                for item in value["cases"]
                if item["variant"] == "A"
                and item["phase"] == "pre"
                and item["id"] == schema.CORRECTNESS_CASE_IDS[0]
            )
            case["status"] = "PASS"

        check(
            "terminal-correctness-only-aggregate-mutation",
            lambda: mutate_early_json(
                early_output / "correctness.json", mutate_current_aggregate
            ),
        )
        early_children = read_jsonl(
            early_output / "child-manifest.jsonl",
            "terminal self-test early children",
            [],
        )
        early_fault = next(
            child
            for child in early_children
            if child.get("kind") == "fault"
            and child.get("context", {}).get("phase") == "pre"
        )
        check(
            "terminal-correctness-only-boundedness-mutation",
            lambda: mutate_early_json(
                Path(early_fault["raw_path"]),
                lambda value: value["boundedness"].__setitem__(
                    "owner_ring_intents", 1023
                ),
            ),
        )

        def inject_timing_csv() -> bool:
            path = early_output / schema.CSV_FILENAMES["primary"]
            try:
                write_fixture(path, b"injected timing evidence\n")
                result, code = verify(early_output, publish=False)
                return code == EXIT_INVALID and result["outcome"] == "TERMINAL_INVALID"
            finally:
                if path.exists():
                    path.chmod(0o644)
                    path.unlink()

        check("terminal-correctness-only-rejects-timing-csv", inject_timing_csv)
    passed = all(item["pass"] for item in checks)
    return {
        "schema": "bn-2l3n-terminal-self-test-v3",
        "protocol": schema.PROTOCOL,
        "outcome": "SELF_TEST_PASS" if passed else "SELF_TEST_FAIL",
        "checks": checks,
    }


def _rejects_nonfinite() -> bool:
    try:
        canonical_json_bytes({"value": float("nan")})
    except ValueError:
        return True
    return False


def usage() -> int:
    print(f"usage: {Path(sys.argv[0]).name} --smoke | --self-test | --verify OUTPUT_DIR", file=sys.stderr)
    return EXIT_USAGE


def main() -> int:
    if sys.argv[1:] == ["--smoke"]:
        sys.stdout.buffer.write(canonical_json_bytes({
            "schema": "bn-2l3n-terminal-verifier-smoke-v3",
            "protocol": schema.PROTOCOL,
            "status": "PASS",
        }))
        return 0
    if sys.argv[1:] == ["--self-test"]:
        result = self_test()
        sys.stdout.buffer.write(canonical_json_bytes(result))
        return 0 if result["outcome"] == "SELF_TEST_PASS" else EXIT_INTERNAL
    if len(sys.argv) == 3 and sys.argv[1] == "--verify":
        result, code = verify(Path(sys.argv[2]), publish=True)
        sys.stdout.buffer.write(canonical_json_bytes(result))
        return code
    return usage()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as error:
        print(f"internal terminal verifier failure: {error!r}", file=sys.stderr)
        raise SystemExit(EXIT_INTERNAL)
