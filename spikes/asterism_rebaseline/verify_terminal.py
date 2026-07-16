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


def validate_terminal_tools_authority(
    output_dir: Path,
    prepared: Mapping[str, Any] | None,
    errors: list[str],
) -> None:
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

    prepared_root: Path | None = None
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
    validate_terminal_tools_authority(output_dir, prepared, errors)
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
        "comm_allowlist": sorted(comms.values()),
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
        "review_id": "cr-terminal-fixture",
        "reviewed_at": "2026-07-15T00:00:00+00:00",
        "tooling_commit": fake_commit,
        "tooling_tree": fake_tree,
        "toolchain": {},
        "shared_manifest_sha256": hashlib.sha256(b"terminal-shared").hexdigest(),
        "tools_manifest": tools_manifest,
        "tools_manifest_sha256": tools_manifest_sha256,
        "filesystem_admission": {},
        "comm_allowlist": sorted(comms.values()),
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
        "comm_allowlist": sorted(comms.values()),
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
        "toolchain": {},
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

        projection_children = read_jsonl(
            output / "child-manifest.jsonl",
            "terminal projection fixture children",
            [],
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
