#!/usr/bin/env python3
"""Hostile static validator for the exact current-product test overlay.

This script never applies the overlay, compiles Rust, runs product code, or
collects evidence.  It reconstructs the patched source in memory and rejects
drift in the frozen test-only hook contract.  The caller separately runs
``git apply --check`` as the Git implementation compatibility check.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


SCHEMA = "bn-xfw3-product-test-overlay-validator-v1"
ENGINE_PATH = Path("crates/mess-store/src/engine.rs")
ENGINE_SHA256 = "c995c27d8fff3e1ddfffdb700dfc94160a99ea0c7fe731017d3f1db99d7b59e7"
CORRECTNESS_CFG = "asterism_rebaseline_correctness"
HERE = Path(__file__).resolve().parent
REPOSITORY = HERE.parents[3]
PATCH_PATH = HERE / "product-test-overlay.patch"


class ValidationError(RuntimeError):
    """One stable rejection category plus a human-readable explanation."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class Hunk:
    old_start: int
    old_count: int
    new_start: int
    new_count: int
    lines: tuple[str, ...]


HUNK_HEADER = re.compile(
    r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(?: .*)?$"
)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fail(code: str, message: str) -> None:
    raise ValidationError(code, message)


def replace_once(text: str, old: str, new: str) -> str:
    if text.count(old) != 1:
        raise AssertionError(f"hostile mutation target is not unique: {old!r}")
    return text.replace(old, new, 1)


def parse_patch(patch_text: str) -> tuple[Path, list[Hunk]]:
    old_headers = re.findall(r"^--- a/(.+)$", patch_text, re.MULTILINE)
    new_headers = re.findall(r"^\+\+\+ b/(.+)$", patch_text, re.MULTILINE)
    diff_headers = re.findall(
        r"^diff --git a/(.+) b/(.+)$", patch_text, re.MULTILINE
    )
    if old_headers != [ENGINE_PATH.as_posix()]:
        fail("compatibility", f"old patch paths differ: {old_headers!r}")
    if new_headers != [ENGINE_PATH.as_posix()]:
        fail("compatibility", f"new patch paths differ: {new_headers!r}")
    if diff_headers != [(ENGINE_PATH.as_posix(), ENGINE_PATH.as_posix())]:
        fail("compatibility", f"diff paths differ: {diff_headers!r}")

    lines = patch_text.splitlines(keepends=True)
    hunks: list[Hunk] = []
    index = 0
    while index < len(lines):
        header = HUNK_HEADER.match(lines[index].rstrip("\n"))
        if header is None:
            index += 1
            continue
        old_start = int(header.group(1))
        old_count = int(header.group(2) or "1")
        new_start = int(header.group(3))
        new_count = int(header.group(4) or "1")
        index += 1
        body: list[str] = []
        while index < len(lines):
            line = lines[index]
            if HUNK_HEADER.match(line.rstrip("\n")) or line.startswith(
                "diff --git "
            ):
                break
            if line.startswith((" ", "+", "-", "\\")):
                body.append(line)
                index += 1
                continue
            break
        hunks.append(Hunk(old_start, old_count, new_start, new_count, tuple(body)))
    if not hunks:
        fail("compatibility", "patch has no unified hunks")
    return ENGINE_PATH, hunks


def apply_in_memory(base: str, hunks: list[Hunk]) -> str:
    source = base.splitlines(keepends=True)
    output: list[str] = []
    cursor = 0
    new_cursor = 0
    for hunk in hunks:
        # Unified diff anchors a zero-length side *after* its numbered line;
        # a non-empty side starts on the numbered line itself.
        old_index = (
            hunk.old_start if hunk.old_count == 0 else hunk.old_start - 1
        )
        if old_index < cursor:
            fail("compatibility", "overlapping or out-of-order hunks")
        output.extend(source[cursor:old_index])
        new_cursor += old_index - cursor
        expected_new = (
            hunk.new_start if hunk.new_count == 0 else hunk.new_start - 1
        )
        if new_cursor != expected_new:
            fail("compatibility", "new hunk coordinate differs")
        cursor = old_index
        consumed = 0
        produced = 0
        for raw in hunk.lines:
            if raw.startswith("\\"):
                continue
            marker, payload = raw[0], raw[1:]
            if marker in (" ", "-"):
                if cursor >= len(source) or source[cursor] != payload:
                    fail(
                        "compatibility",
                        f"hunk context differs at source line {cursor + 1}",
                    )
                cursor += 1
                consumed += 1
            if marker in (" ", "+"):
                output.append(payload)
                produced += 1
                new_cursor += 1
        if consumed != hunk.old_count or produced != hunk.new_count:
            fail("compatibility", "hunk line counts differ")
    output.extend(source[cursor:])
    return "".join(output)


def require(text: str, needle: str, code: str) -> None:
    if needle not in text:
        fail(code, f"required marker absent: {needle!r}")


def require_once(text: str, needle: str, code: str) -> None:
    count = text.count(needle)
    if count != 1:
        fail(code, f"marker cardinality is {count}, expected 1: {needle!r}")


def function_slice(source: str, start: str, end: str, code: str) -> str:
    begin = source.find(start)
    finish = source.find(end, begin + len(start))
    if begin < 0 or finish < 0:
        fail(code, f"function bounds absent: {start!r} .. {end!r}")
    return source[begin:finish]


def validate_cfg_dominance(source: str, patch_text: str) -> None:
    item_markers = (
        "#[cfg(test)]\n#[derive(Debug, Clone, Copy, PartialEq, Eq)]\npub enum TestEngineHookPoint {",
        "#[cfg(test)]\n#[derive(Debug, Clone, Copy, PartialEq, Eq)]\npub enum TestEngineHook {",
        "#[cfg(test)]\n#[derive(Default)]\nstruct TestEngineHooks",
        "#[cfg(test)]\n#[derive(Default)]\nstruct TestEngineHookState",
        "#[cfg(test)]\nenum TestPwriteAction",
        "#[cfg(test)]\nenum TestFdatasyncAction",
        "#[cfg(test)]\nimpl TestEngineHooks",
        "#[cfg(test)]\npub struct TestEngineHookGuard",
        "#[cfg(test)]\nimpl TestEngineHookGuard",
        "#[cfg(test)]\nimpl Drop for TestEngineHookGuard",
        "#[cfg(test)]\n#[derive(Clone)]\nstruct TestEngineFs",
        "#[cfg(test)]\nimpl LogFs for TestEngineFs",
        "#[cfg(test)]\n#[derive(Clone)]\nstruct TestEngineFile",
        "#[cfg(test)]\nimpl FileHandle for TestEngineFile",
    )
    for marker in item_markers:
        require_once(source, marker, "cfg_test_dominance")

    local_markers = (
        "#[cfg(test)]\n    test_hooks:                      Arc<TestEngineHooks>",
        "#[cfg(test)]\n    test_hooks:           Arc<TestEngineHooks>",
        "// owner join below, so a dropped guard/engine cannot strand shutdown.\n        #[cfg(test)]\n        self.test_hooks.",
        "#[cfg(test)]\n            self.test_hooks.rendezvous(",
        "#[cfg(test)]\n        let test_hooks = Arc::new(TestEngineHooks::default());",
        "#[cfg(test)]\n        let writer = {",
        "#[cfg(test)]\n            test_hooks: Arc::clone(&test_hooks),",
        "#[cfg(test)]\n                test_hooks,",
        "#[cfg(test)]\n    pub fn arm_test_hook",
        "#[cfg(test)]\n    pub fn arm_test_owner_cohort",
    )
    for marker in local_markers:
        require_once(source, marker, "cfg_test_dominance")
    require_once(
        source,
        "#[cfg(all(test, not(miri), not(asterism_rebaseline_correctness)))]\n"
        "mod append_gate_tests;",
        "cfg_test_dominance",
    )
    require_once(
        source,
        "#[cfg(all(test, not(asterism_rebaseline_correctness)))]\n"
        "mod seal_skip_tests {",
        "cfg_test_dominance",
    )
    if source.count(CORRECTNESS_CFG) != 2 or patch_text.count(CORRECTNESS_CFG) != 2:
        fail(
            "cfg_test_dominance",
            "correctness cfg may only suppress the two dev-only test modules",
        )
    if "cfg(feature" in patch_text or "Cargo.toml" in patch_text:
        fail("cfg_test_dominance", "overlay introduces a normal feature surface")


def validate_named_sites(source: str) -> None:
    expected_counts = {
        "TestEngineHookPoint::Admission": 1,
        "TestEngineHookPoint::PrePwrite": 1,
        "TestEngineHookPoint::PreFdatasync": 1,
        "TestEngineHookPoint::PostFdatasyncPrePublication": 1,
        "TestEngineHookPoint::PostPublicationPreCompletion": 2,
    }
    for marker, expected in expected_counts.items():
        count = source.count(marker)
        if count != expected:
            fail("named_call_sites", f"{marker} count {count}, expected {expected}")

    pwrite = function_slice(
        source,
        "    fn pwrite(&self, off: u64, buf: &[u8])",
        "    fn pread(&self, off: u64, buf: &mut [u8])",
        "named_call_sites",
    )
    pwrite_order = (
        "TestEngineHookPoint::PrePwrite",
        "self.hooks.pwrite_action(buf.len())",
        "self.inner.pwrite(off, &buf[..after_bytes])",
        "self.hooks.reach_and_pause(generation)",
        ".pwrite(off + written as u64, &buf[written..])",
    )
    offsets = [pwrite.find(marker) for marker in pwrite_order]
    if -1 in offsets or offsets != sorted(offsets):
        fail("named_call_sites", "partial real-write call-site order differs")

    sync = function_slice(
        source,
        "    fn fdatasync(&self) -> std::io::Result<()> {",
        "    fn len(&self) -> std::io::Result<u64>",
        "named_call_sites",
    )
    sync_order = (
        "TestEngineHookPoint::PreFdatasync",
        "self.hooks.fdatasync_action()",
        "self.inner.fdatasync()",
        "TestEngineHookPoint::PostFdatasyncPrePublication",
    )
    offsets = [sync.find(marker) for marker in sync_order]
    if -1 in offsets or offsets != sorted(offsets):
        fail("named_call_sites", "fdatasync call-site order differs")

    run = function_slice(source, "    fn run(mut self,", "\n    }\n}\n\nfn expect_acked_log", "named_call_sites")
    run_order = (
        "rx.blocking_recv()",
        "TestEngineHookPoint::Admission",
        "wait_until_cohort_admitted()",
        "self.gather(&mut rx, first)",
    )
    offsets = [run.find(marker) for marker in run_order]
    if -1 in offsets or offsets != sorted(offsets):
        fail("named_call_sites", "admission call-site order differs")

    retire = function_slice(
        source, "    fn retire_domain(", "    fn process_registry(", "named_call_sites"
    )
    post = retire.rfind("TestEngineHookPoint::PostPublicationPreCompletion")
    if not (retire.rfind("publish_batch(") < post < retire.rfind("completion.finish(result)")):
        fail("named_call_sites", "domain publication/completion order differs")
    registry = function_slice(
        source, "    fn process_registry(", "    fn refresh_status(", "named_call_sites"
    )
    post = registry.find("TestEngineHookPoint::PostPublicationPreCompletion")
    if not (registry.rfind("publish_batch(") < post < registry.rfind("completion.finish(Ok(")):
        fail("named_call_sites", "registry publication/completion order differs")


def validate_one_shot(source: str) -> None:
    for marker in (
        "generation:              u64",
        "armed:                   Option<TestEngineHook>",
        "claimed_generation:      Option<u64>",
        "last_reached_generation: u64",
        "state.armed = Some(hook);",
        "state.generation = state.generation.wrapping_add(1).max(1);",
        "state.armed = None;",
        "state.claimed_generation = Some(generation);",
    ):
        require(source, marker, "one_shot_countdowns")
    require_once(source, "state.armed = Some(hook);", "one_shot_countdowns")
    if source.count("state.armed = None;") != 9:
        fail("one_shot_countdowns", "one-shot consumption/disarm sites differ")
    for helper in ("completed_real_pwrite", "completed_real_fdatasync"):
        body = function_slice(source, f"    fn {helper}(", "\n    }", "one_shot_countdowns")
        require(body, "if !succeeded {\n            return;\n        }", "one_shot_countdowns")
        require(body, "saturating_sub(1)", "one_shot_countdowns")
    require(
        source,
        "TestEngineHook::PwriteEio {\n                after_successful_pwrites,\n            }) => {\n                if *after_successful_pwrites > 0",
        "one_shot_countdowns",
    )


def validate_after_open(source: str) -> None:
    open_body = function_slice(
        source,
        "    pub fn open_with(",
        "    /// Rehydrate the record book",
        "after_open_arming",
    )
    require(open_body, "let test_hooks = Arc::new(TestEngineHooks::default());", "after_open_arming")
    require(open_body, "let fs = TestEngineFs {", "after_open_arming")
    if ".arm(" in open_body or "TestEngineHook::" in open_body:
        fail("after_open_arming", "hook can be armed during engine open")
    arm = function_slice(
        source,
        "    pub fn arm_test_hook(&self, hook: TestEngineHook)",
        "    /// Cohort a known number",
        "after_open_arming",
    )
    require(arm, "self.inner.test_hooks.arm(hook)", "after_open_arming")


def validate_unwind(source: str) -> None:
    guard_drop = function_slice(
        source,
        "impl Drop for TestEngineHookGuard",
        "\n}\n\n#[cfg(test)]\n#[derive(Clone)]\nstruct TestEngineFs",
        "unwind_safety",
    )
    require(guard_drop, "self.hooks.disarm(self.generation)", "unwind_safety")
    inner_drop = function_slice(
        source,
        "impl Drop for Inner",
        "/// Poll cadence + per-segment wait bound",
        "unwind_safety",
    )
    disarm = inner_drop.find("self.test_hooks.disarm_all();")
    join = inner_drop.find("join.join()")
    if disarm < 0 or join < 0 or disarm > join:
        fail("unwind_safety", "engine disarm must precede owner join")
    for marker in (
        "state.armed = None;",
        "state.claimed_generation = None;",
        "state.paused_generation = None;",
        "self.ready.notify_all();",
    ):
        require(
            function_slice(source, "    fn disarm_all(&self)", "\n    }", "unwind_safety"),
            marker,
            "unwind_safety",
        )


def validate_faults(source: str) -> None:
    enum_body = function_slice(
        source, "pub enum TestEngineHook {", "\n}\n\n#[cfg(test)]\n#[derive(Default)]", "fault_variants"
    )
    for variant in ("PartialRealWrite", "PwriteZero", "PwriteEio", "FdatasyncEio"):
        require_once(enum_body, variant, "fault_variants")
    require(enum_body, "after_successful_pwrites: usize", "fault_variants")
    require(enum_body, "after_successful_fdatasyncs: usize", "fault_variants")
    require(source, "Ok(0)", "fault_variants")
    if source.count("Err(std::io::Error::from_raw_os_error(5))") != 2:
        fail("fault_variants", "pwrite/fdatasync EIO injection differs")
    require(source, "self.inner.pwrite(off, &buf[..after_bytes])", "fault_variants")


def normal_command_sources(overrides: Mapping[Path, str] | None = None) -> dict[Path, str]:
    """Return only runtime launch boundaries for normal timed children.

    Build/review/schema sources intentionally name the forbidden test hooks in
    order to prove their absence and must not be confused with executable
    launch inputs. Prepared-artifact validation separately proves that the
    proof-only overlay twin is unreachable from published variants and tools.
    """

    overrides = overrides or {}
    selected: dict[Path, str] = {}
    for relative in (
        Path("spikes/asterism_rebaseline/run_rebaseline.py"),
        Path("spikes/asterism_rebaseline/run_rebaseline.sh"),
        Path("spikes/asterism_rebaseline/strace_attach.py"),
    ):
        path = REPOSITORY / relative
        if not path.is_file():
            fail("normal_perf_isolation", f"normal command source is absent: {relative}")
        selected[relative] = overrides.get(relative, path.read_text())
    for relative, text in overrides.items():
        selected[relative] = text
    return selected


def validate_normal_isolation(overrides: Mapping[Path, str] | None = None) -> None:
    forbidden = (
        "--cfg test",
        "--cfg=test",
        "arm_test_hook",
        "arm_test_owner_cohort",
        "TestEngineHook",
        "product-test-overlay.patch",
    )
    for path, text in normal_command_sources(overrides).items():
        for token in forbidden:
            if token in text:
                fail("normal_perf_isolation", f"{path}: forbidden normal input {token!r}")


def validate_patch(
    patch_text: str,
    *,
    normal_overrides: Mapping[Path, str] | None = None,
) -> list[str]:
    base_bytes = (REPOSITORY / ENGINE_PATH).read_bytes()
    if sha256(base_bytes) != ENGINE_SHA256:
        fail("compatibility", "exact current engine source SHA-256 differs")
    _, hunks = parse_patch(patch_text)
    source = apply_in_memory(base_bytes.decode(), hunks)
    validate_cfg_dominance(source, patch_text)
    validate_named_sites(source)
    validate_one_shot(source)
    validate_after_open(source)
    validate_unwind(source)
    validate_faults(source)
    validate_normal_isolation(normal_overrides)
    return [
        "exact_source_and_patch_applicability",
        "cfg_test_dominance",
        "dev_only_test_modules_excluded_from_child_dependency_unit",
        "named_call_sites_and_order",
        "generation_bound_one_shot_countdowns",
        "after_open_arming_only",
        "unwind_disarm_and_release",
        "fault_variants",
        "normal_perf_command_isolation",
    ]


def expect_rejection(
    name: str,
    patch_text: str,
    expected: str,
    *,
    overrides: Mapping[Path, str] | None = None,
) -> str:
    try:
        validate_patch(patch_text, normal_overrides=overrides)
    except ValidationError as error:
        if error.code != expected:
            raise AssertionError(
                f"{name}: expected {expected}, observed {error.code}: {error}"
            ) from error
        return name
    raise AssertionError(f"{name}: hostile mutation was accepted")


def self_test(patch_text: str) -> list[str]:
    validate_patch(patch_text)
    passed = ["canonical_overlay"]
    passed.append(
        expect_rejection(
            "missing_cfg_test",
            replace_once(
                patch_text,
                "+#[cfg(test)]\n+#[derive(Debug, Clone, Copy, PartialEq, Eq)]\n+pub enum TestEngineHookPoint",
                "+// hostile missing cfg(test)\n+#[derive(Debug, Clone, Copy, PartialEq, Eq)]\n+pub enum TestEngineHookPoint",
            ),
            "cfg_test_dominance",
        )
    )
    passed.append(
        expect_rejection(
            "missing_append_gate_negative_guard",
            replace_once(
                patch_text,
                "+#[cfg(all(test, not(miri), not(asterism_rebaseline_correctness)))]",
                "+#[cfg(all(test, not(miri)))]",
            ),
            "cfg_test_dominance",
        )
    )
    passed.append(
        expect_rejection(
            "missing_seal_skip_negative_guard",
            replace_once(
                patch_text,
                "+#[cfg(all(test, not(asterism_rebaseline_correctness)))]",
                "+#[cfg(test)]",
            ),
            "cfg_test_dominance",
        )
    )
    passed.append(
        expect_rejection(
            "wrong_admission_site",
            replace_once(
                patch_text,
                "+            self.test_hooks.rendezvous(TestEngineHookPoint::Admission);",
                "+            self.test_hooks.rendezvous(TestEngineHookPoint::PrePwrite);",
            ),
            "named_call_sites",
        )
    )
    passed.append(
        expect_rejection(
            "non_one_shot_fault",
            replace_once(
                patch_text,
                "+                    state.armed = None;\n+                    state.claimed_generation = Some(generation);\n+                    TestPwriteAction::Eio(generation)",
                "+                    // hostile: leave the hook armed\n+                    state.claimed_generation = Some(generation);\n+                    TestPwriteAction::Eio(generation)",
            ),
            "one_shot_countdowns",
        )
    )
    passed.append(
        expect_rejection(
            "arming_during_open",
            replace_once(
                patch_text,
                "+        let test_hooks = Arc::new(TestEngineHooks::default());",
                "+        let test_hooks = Arc::new(TestEngineHooks::default()); // hostile .arm(",
            ),
            "after_open_arming",
        )
    )
    passed.append(
        expect_rejection(
            "missing_unwind_disarm",
            replace_once(
                patch_text,
                "+        self.test_hooks.disarm_all();",
                "+        self.test_hooks.release(0); // hostile missing global disarm",
            ),
            "unwind_safety",
        )
    )
    passed.append(
        expect_rejection(
            "missing_fault_variant",
            replace_once(patch_text, "+    PwriteZero {", "+    PwriteGone {"),
            "fault_variants",
        )
    )
    sample = Path("spikes/asterism_rebaseline/run_rebaseline.sh")
    passed.append(
        expect_rejection(
            "normal_command_test_cfg",
            patch_text,
            "normal_perf_isolation",
            overrides={sample: normal_command_sources()[sample] + "\n--cfg test\n"},
        )
    )
    passed.append(
        expect_rejection(
            "inapplicable_context",
            replace_once(
                patch_text,
                "@@ -402,3 +402,428 @@",
                "@@ -403,3 +402,428 @@",
            ),
            "compatibility",
        )
    )
    return passed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    patch_bytes = PATCH_PATH.read_bytes()
    patch_text = patch_bytes.decode()
    checks = self_test(patch_text) if args.self_test else validate_patch(patch_text)
    print(
        json.dumps(
            {
                "schema": SCHEMA,
                "outcome": "SELF_TEST_PASS" if args.self_test else "PASS",
                "engine_sha256": ENGINE_SHA256,
                "patch_sha256": sha256(patch_bytes),
                "checks": checks,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
