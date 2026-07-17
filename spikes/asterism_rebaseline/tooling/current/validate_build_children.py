#!/usr/bin/env python3
"""Hostile static validator for the pre-approval current-child builder.

This validator parses and inspects source only.  It never imports the future
fault/lock authorities, invokes Cargo/rustc, applies the product patch, or runs
a product/child/runner.  Its mutations prove the reviewed build boundaries
fail closed before the independently reviewed dependencies are available.
"""

from __future__ import annotations

import argparse
import ast
import importlib.util
import json
from pathlib import Path
from typing import Any, Callable


SCHEMA = "bn-30fs-build-children-validator-v1"
HERE = Path(__file__).resolve().parent
BUILDER = HERE / "build_children.py"
WRAPPER = HERE / "rustc_workspace_wrapper.py"
CORRECTNESS = HERE / "correctness.rs"
OVERLAY = HERE / "product-test-overlay.patch"
OVERLAY_VALIDATOR = HERE / "validate_product_test_overlay.py"
CORRECTNESS_CFG = "asterism_rebaseline_correctness"
CASES = (
    "public-ordinary-append-command-cache-read-subscribe",
    "same-stream-exact-race",
    "registry-first-use-ordered-failure-unit",
    "error-ordering",
    "borrowed-owned-mixed-order-and-type",
    "two-live-rolls",
    "clean-repeated-active-tail-sealed-recovery",
)
CARGO_CONFIG_GUEST_MARKERS = (
    '"/asterism/source/.cargo/config.toml"',
    '"/asterism/source/.cargo/config"',
    '"/asterism/.cargo/config.toml"',
    '"/asterism/.cargo/config"',
    '"/.cargo/config.toml"',
    '"/.cargo/config"',
    'f"{GUEST_CARGO_HOME}/config.toml"',
    'f"{GUEST_CARGO_HOME}/config"',
)


class ValidationError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def fail(code: str, message: str) -> None:
    raise ValidationError(code, message)


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode()


def require_once(source: str, marker: str, code: str) -> None:
    count = source.count(marker)
    if count != 1:
        fail(code, f"marker cardinality {count}, expected 1: {marker!r}")


def require_order(source: str, markers: tuple[str, ...], code: str) -> None:
    offsets = [source.find(marker) for marker in markers]
    if -1 in offsets or offsets != sorted(offsets) or len(offsets) != len(set(offsets)):
        fail(code, f"marker order differs: {markers!r}")


def slice_between(source: str, start: str, end: str, code: str) -> str:
    begin = source.find(start)
    finish = source.find(end, begin + len(start))
    if begin < 0 or finish < 0:
        fail(code, f"source bounds absent: {start!r} .. {end!r}")
    return source[begin:finish]


def parse_python(source: str, name: str, code: str) -> None:
    try:
        ast.parse(source, filename=name)
    except SyntaxError as error:
        fail(code, f"{name} is not valid Python: {error}")


def validate_wrapper(source: str) -> list[str]:
    parse_python(source, "rustc_workspace_wrapper.py", "wrapper_ast")
    if "subprocess" in source or "shell=True" in source:
        fail("wrapper_exec", "wrapper may only replace itself with pinned rustc")
    for marker in (
        "#!/asterism/python3",
        'TARGET_CRATE = "mess_store"',
        'TARGET_PACKAGE = "mess-store"',
        'TARGET_CRATE_TYPE = "lib"',
        'CORRECTNESS_CFG = "asterism_rebaseline_correctness"',
        '"--cfg",\n    "test"',
        '"--allow",\n    "explicit_builtin_cfgs_in_flags"',
        '"--cfg",\n    CORRECTNESS_CFG',
        '"--check-cfg",\n    f"cfg({CORRECTNESS_CFG})"',
        "if names_target != sources_target:",
        'if environment.get("CARGO_PKG_NAME") != TARGET_PACKAGE:',
        'if crate_types != (TARGET_CRATE_TYPE,):',
        'if "--test" in arguments:',
        "os.O_EXCL",
        "os.fchmod(descriptor, 0o444)",
        "stat.S_IMODE(metadata.st_mode) != 0o444",
        "os.execv(plan.rustc, (plan.rustc, *plan.arguments))",
    ):
        require_once(source, marker, "wrapper_contract")
    if source.count("write_receipt(") != 2:
        fail("wrapper_receipt", "receipt definition/call cardinality differs")
    require_order(
        source,
        (
            "plan = plan_invocation(sys.argv[1:], os.environ)",
            "write_receipt(Path(os.environ[RECEIPT_ENV]), plan.receipt)",
            "os.execv(plan.rustc, (plan.rustc, *plan.arguments))",
        ),
        "wrapper_receipt",
    )
    return [
        "wrapper_exact_workspace_unit_selection",
        "wrapper_exact_cfg_injection",
        "wrapper_single_use_receipt_before_exec",
    ]


def load_wrapper_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "bn30fs_static_wrapper", WRAPPER
    )
    if spec is None or spec.loader is None:
        fail("wrapper_semantics", "cannot load wrapper module")
    module = importlib.util.module_from_spec(spec)
    # Dataclass decoration resolves the defining module through sys.modules.
    import sys

    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def validate_wrapper_semantics() -> list[str]:
    wrapper = load_wrapper_module()
    rustc = "/pinned/toolchain/bin/rustc"
    source = "crates/mess-store/src/lib.rs"
    environment = {
        wrapper.PINNED_RUSTC_ENV: rustc,
        wrapper.EXPECTED_SOURCE_ENV: source,
        wrapper.RECEIPT_ENV: "/receipt/injection.json",
        wrapper.BUILD_NONCE_ENV: "a" * 64,
        "CARGO_PKG_NAME": "mess-store",
    }
    target = [
        rustc,
        "--crate-name",
        "mess_store",
        source,
        "--crate-type",
        "lib",
    ]
    planned = wrapper.plan_invocation(target, environment)
    if not planned.inject or planned.arguments[-8:] != wrapper.INJECTED_ARGUMENTS:
        fail("wrapper_semantics", "target invocation does not receive exact cfg arguments")
    other = [rustc, "--crate-name", "mess_core", "crates/mess-core/src/lib.rs", "--crate-type", "lib"]
    passthrough = wrapper.plan_invocation(other, environment)
    if passthrough.inject or passthrough.arguments != tuple(other[1:]):
        fail("wrapper_semantics", "non-target invocation is not unchanged")
    rejected = 0
    hostiles = (
        (["/wrong/rustc", *target[1:]], environment),
        ([rustc, "--crate-name", "mess_store", "wrong.rs", "--crate-type", "lib"], environment),
        ([rustc, "--crate-name", "mess_store", source, "--crate-type", "bin"], environment),
        ([*target, "--cfg", "test"], environment),
        (target, {**environment, "CARGO_PKG_NAME": "wrong"}),
    )
    for argv, hostile_environment in hostiles:
        try:
            wrapper.plan_invocation(argv, hostile_environment)
        except wrapper.WrapperError:
            rejected += 1
    if rejected != len(hostiles):
        fail("wrapper_semantics", "wrapper accepted a hostile invocation")
    return ["wrapper_semantic_target_and_passthrough_probes"]


def validate_builder(source: str) -> list[str]:
    parse_python(source, "build_children.py", "builder_ast")
    require_once(source, "#!/usr/bin/python3", "builder_contract")
    for forbidden in (
        "import prepare_overlays",
        "from prepare_overlays",
        "import prepare_children",
        "from prepare_children",
        "shell=True",
    ):
        if forbidden in source:
            fail("builder_scope", f"builder reaches forbidden integration surface: {forbidden}")
    repeatable_authority_markers = {
        "initialize = library.inotify_init1",
        "add_watch = library.inotify_add_watch",
        'os.fsencode(f"/proc/self/fd/{descriptor}")',
        "self.watch_descriptors.add(watch)",
        "self._add_directory_watch(descriptor)",
        "def _drain_mutation_events(self, boundary: str) -> None:",
        "if observed:\n            self.poisoned = True",
        'f"{self.context} malformed inotify header at {boundary}"',
        'f"{self.context} malformed inotify event at {boundary}"',
        "if len(self.watch_descriptors) != directory_count:",
        'self._drain_mutation_events("initial manifest")',
        'self._drain_mutation_events("matching manifest")',
        '"watch_count": len(self.watch_descriptors)',
        "chunk = os.pread(",
    }
    for marker in (
        'LOCK_AUTHORITY_SCHEMA = "bn-31gp-current-lock-authority-v1"',
        'LOCK_VALIDATION_SCHEMA = "bn-31gp-current-lock-authority-validation-v1"',
        'FAULT_VALIDATOR_SCHEMA = "bn-20be-current-fault-validator-v1"',
        'LOCK_AUTHORITY_VALIDATOR = HERE / "lock_authority.py"',
        'PREPARE_OVERLAYS = TOOLING / "prepare_overlays.py"',
        "LOCK_AUTHORITY_VALIDATOR,\n        PREPARE_OVERLAYS,",
        'FAULT_SOURCE = HERE / "fault.rs"',
        'FAULT_VALIDATOR = HERE / "validate_fault.py"',
        'validate_authority = getattr(module, "validate_authority", None)',
        'validation_result = getattr(module, "validation_result", None)',
        'capture_tree = getattr(module, "capture_prebuild_materialized_tree", None)',
        'module, "resample_builder_filesystem_admission", None',
        "semantic_validator=current_context,",
        "identity_revalidator=identity_revalidator,",
        "repository,\n            lock_manifest_path,\n            review_bundle_path,\n            authority_path,",
        "value = validation_result(validated)",
        'canonical_identity = getattr(identity, "canonical", None)',
        'build_parser.add_argument("--review-bundle", required=True, type=Path)',
        'if not isinstance(locks, dict) or set(locks) != {"A", "C", "D"}:',
        'lock_payload = validated.locks["A"].payload',
        'for kind in ("children", "pristine-release", "hooked-release")',
        '"--unshare-net"',
        '"--locked"',
        '"--offline"',
        '"--tmpfs",\n        "/asterism"',
        'source_ro_bind=source_ro_bind',
        '"apply",\n                "--no-index"',
        '"--check"',
        '"--whitespace=error-all"',
        "guard_factory = lock_authority_module.capture_prebuild_materialized_tree",
        'source_ro_bind = guard.bwrap_ro_bind("/asterism/source")',
        'guard.ro_bind_source != f"/proc/self/fd/{guard.pass_fds[0]}"',
        "pass_fds=inherited_descriptors",
        "guard.pass_fds\n            + target_guard.pass_fds",
        "inherited_descriptors += (wrapper_descriptor, *receipt_guard.pass_fds)",
        "class BoundBuildDirectory:",
        'raise BuildError(f"{self.context} parent selection changed")',
        'target_guard.bwrap_bind(\n            "/asterism/target", read_only=False',
        '"--ro-bind-fd",\n                str(wrapper_descriptor),',
        'receipt_guard.bwrap_bind(\n                "/asterism/receipt", read_only=False',
        "example: copy_bound_artifact(",
        "receipt_payload, receipt_identity = read_bound_regular_file(",
        "cwd=output,",
        "lock_authority_module.resample_builder_filesystem_admission(",
        '"prebuild_filesystem_admissions": {',
        '"children": child_build["filesystem_admission"]',
        '"hooked_release": hooked_build["filesystem_admission"]',
        '"pristine_release": pristine_build["filesystem_admission"]',
        'wrapper=wrapper_copy,\n        examples=(',
        'kind="pristine-release"',
        'kind="hooked-release"',
        'if pristine_bytes != hooked_bytes:',
        'Path("/usr/bin/nm").resolve(strict=True)',
        '"--demangle=rust",\n            "--format=posix",',
        'binary_source = f"/proc/self/fd/{binary_fd}"',
        "pass_fds=(binary_fd,)",
        "if token in pristine_bytes or token in hooked_bytes:",
        'raise BuildError("release proof differs from descriptor-copied artifacts")',
        "product_overlay_authority = validate_product_overlay_authority(repository)",
        'canonical_output=False',
        '"hostile_mutations_rejected": len(self_checks) - 1',
        'final_tools["tools"].update(child_bindings)',
        "if name in CHILD_TOOLS and allow_child_placeholders:",
        "if binding != CHILD_PLACEHOLDER_BINDINGS[name]:",
        '"path": "/asterism/preapproval-placeholder/ast-rb-check"',
        '"path": "/asterism/preapproval-placeholder/ast-rb-fault"',
        "def validate_wrapper_receipt(",
        '"original_argv_sha256",',
        '"injected_arguments": list(WRAPPER_INJECTED_ARGUMENTS)',
        '"crate_name": WRAPPER_TARGET_CRATE',
        '"crate_type": WRAPPER_TARGET_CRATE_TYPE',
        '"package": WRAPPER_TARGET_PACKAGE',
        '"rustc": pinned_rustc',
        '"source": EXPECTED_LIB_SOURCE',
        'if not lower_hex(receipt.get("original_argv_sha256"), 64):',
        'identity.get("mode") != 0o444',
        'identity.get("link_count") != 1',
        'identity.get("sha256") != sha256_bytes(payload)',
        'identity.get("size") != len(payload)',
        "receipt = validate_wrapper_receipt(",
        "base_tools = validate_tools_manifest(tools_path, allow_child_placeholders=True)",
        "final_tools_path, allow_child_placeholders=False",
        "validated_locks_after = validated_lock_records(validated)",
        "if validated_locks_after != validated_locks_before:",
        "validated_authority_after = validated_authority_records(validated)",
        "if validated_authority_after != validated_authority_before:",
        "if inputs_after != inputs_before:",
        "if toolchain_after != toolchain_before:",
        '"source_approval_status": "preapproval-sentinel-not-source-approved"',
        "RuntimeError,",
        "class RetainedFile:",
        "executable=execution_lease.proc_path",
        "inherited = tuple(dict.fromkeys((*pass_fds, *execution_lease.pass_fds)))",
        "def trusted_root_chain(",
        'SYSTEM_PYTHON = Path("/usr/bin/python3").resolve(strict=True)',
        '"exec(compile(source,filename,\'exec\'),scope,scope)"',
        'stdin_payload=patch_lease.payload',
        '"semantic_validator": "descriptor-cross-bound-authority-context-v1"',
        'or embedded_manifest.get("payload") != lock_preview',
        'or embedded_review.get("payload") != review_preview',
        'commit = descriptor_git(["rev-parse", "HEAD"]',
        "class CargoConfigSearchGuard:",
        'dir_fd=guard.descriptor,\n        )\n        try:\n            names = sorted(os.listdir(enumeration_descriptor))',
        "parent_descriptor=guard.descriptor,",
        'CARGO_CONFIG_SEARCH_SCHEMA = "bn-30fs-build-cargo-config-search-v1"',
        'CARGO_HOME_TREE_SCHEMA = RECURSIVE_TREE_AUTHORITY_SCHEMA',
        "| 0x00004000  # IN_Q_OVERFLOW",
        "| 0x00008000  # IN_IGNORED",
        "initialize = library.inotify_init1",
        "add_watch = library.inotify_add_watch",
        'os.fsencode(f"/proc/self/fd/{descriptor}")',
        "self.watch_descriptors.add(watch)",
        "self._add_directory_watch(descriptor)",
        "def _drain_mutation_events(self, boundary: str) -> None:",
        'f"{self.context} malformed inotify header at {boundary}"',
        'f"{self.context} malformed inotify event at {boundary}"',
        "if observed:\n            self.poisoned = True",
        "def _cargo_home_tree_manifest(self, *, install_watches: bool)",
        "chunk = os.pread(",
        'if resolved != self.cargo_home and self.cargo_home not in resolved.parents:\n'
        '                        raise BuildError(\n'
        '                            f"{self.context} Cargo-home symlink escapes the frozen root"',
        'if len(aliases) != aliases[0]["link_count"]:\n'
        '                raise BuildError(\n'
        '                    f"{self.context} Cargo-home hard link escapes the frozen tree"',
        "if len(self.watch_descriptors) != directory_count:",
        'self._drain_mutation_events("initial manifest")',
        'self._drain_mutation_events("matching manifest")',
        'if first != second:\n            raise BuildError(f"{self.context} Cargo-home initial manifests differ")',
        "write_new(self.manifest_evidence_path, payload, 0o444)",
        "self.cargo_home_tree_evidence = evidence",
        '"cargo_home_tree": dict(self.cargo_home_tree_binding or {})',
        'observed_sha256 != self.cargo_home_tree_binding["pre_sha256"]',
        '"watch_count": len(self.watch_descriptors)',
        'str(self.directory_guards["cargo-home"].descriptor)',
        'output / "manifests" / f"cargo-home-{kind}.json"',
        'boundary="post-Cargo boundary", deep=True',
        "dependency.write_bytes(b\"hostile dependency bytes\\n\")",
        "dependency.write_bytes(dependency_payload)",
        '"nested_mutation_restore_rejected": True',
        '"/asterism/source/.cargo/config.toml"',
        'f"{GUEST_CARGO_HOME}/config"',
        "for lease in self.file_leases.values():\n            lease.rewind_for_bind_data()",
        'arguments.extend(["--remount-ro", source_guest])',
        'arguments.extend(["--remount-ro", GUEST_CARGO_HOME])',
        '"--tmpfs",\n                "/asterism/.cargo"',
        '"--tmpfs",\n                "/.cargo"',
        '"CARGO_HOME": GUEST_CARGO_HOME',
        '"RUSTC": GUEST_RUSTC',
        'GUEST_CARGO = f"{GUEST_TOOLCHAIN_ROOT}/bin/cargo"',
        'cargo_bind = ("--ro-bind-fd", str(cargo_lease.descriptor), GUEST_CARGO)',
        'rustc_bind = ("--ro-bind-fd", str(rustc_lease.descriptor), GUEST_RUSTC)',
        'ATTESTATION_SCHEMA = "bn-ecm1-current-children-build-v2"',
        'SEMANTIC_INPUT_AUTHORITY_SCHEMA = "bn-ecm1-semantic-input-authority-v1"',
        'RECURSIVE_TREE_AUTHORITY_SCHEMA = "bn-ecm1-recursive-tree-authority-v1"',
        'TRUSTED_SYSTEM_CLOSURE_SCHEMA = "bn-ecm1-trusted-system-closure-v1"',
        "def system_symlink_scope(root: Path, relative: str, target: str) -> str:",
        'for authority in ("/asterism", "/dev", "/proc", "/run", "/sys", "/tmp")',
        'raise BuildError("trusted system symlink reaches mutable guest authority")',
        '"symlink_scope": symlink_scope',
        '"special_root_symlink_rejected": True',
        'class RecursiveTreeAuthorityGuard:',
        'class TrustedSystemClosureGuard:',
        'hash_regular_contents=False,',
        'trusted_system_roots=roots,',
        '"--symlink",\n                "usr/bin",\n                "/bin"',
        '"--symlink",\n                "usr/lib",\n                "/lib"',
        '"--symlink",\n                "usr/lib",\n                "/lib64"',
        'source_tree_guard.replay("post-Cargo boundary")',
        'toolchain_guard.replay("post-Cargo boundary")',
        'system_guard.replay("post-Cargo boundary")',
        'if observed != self.initial_manifest:\n            self.poisoned = True',
        '"semantic_input_authority": semantic_input_authority',
        '"runtime_sha256": semantic_runtime_sha256(runtime_components)',
        'if len(semantic_runtime_digests) != 1:',
        '"toolchain_manifest": toolchain_manifest_binding',
        "def self_test_cargo_config_guard() -> dict[str, Any]:",
        "cargo_config_guard = self_test_cargo_config_guard()",
        '"cargo_config_guard": cargo_config_guard',
        '"empty_bound_appearance_rejected": True',
        "def self_test_reviewed_cargo_config_policy() -> dict[str, Any]:",
        "reviewed_cargo_config = self_test_reviewed_cargo_config_policy()",
        '"reviewed_cargo_config_policy": reviewed_cargo_config',
        'semantic_runtime = self_test_semantic_runtime_authority()',
        '"semantic_runtime_authority": semantic_runtime',
        '"mount_path_drift_rejected": True',
        'config_postbuild = config_guard.replay(',
        'if config_postbuild != config_prebuild:',
        '"ASTERISM_REBASELINE_PINNED_RUSTC": GUEST_RUSTC',
        'pinned_rustc=GUEST_RUSTC',
        '"--ro-bind-fd" if read_only else "--bind-fd"',
        "execution_lease=bwrap_lease,",
        "boundary_replay=replay_cargo_boundary,",
        "execution_lease=nm_lease,",
        "execution_lease=python_lease,",
        "script_lease.rewind_for_bind_data()",
        'overlays_module = types.ModuleType("prepare_overlays")',
        'compile(\n                    dependency_lease.payload,',
        'compile(\n                    validator_lease.payload,',
        "make_read_only(output)",
    ):
        if marker in repeatable_authority_markers:
            if source.count(marker) < 2:
                fail("builder_contract", f"recursive authority marker absent: {marker}")
        else:
            require_once(source, marker, "builder_contract")
    if "Path(str(a.get(\"final_lock_path\")))" in source or "lock_path.read_bytes()" in source:
        fail("builder_lock_authority", "builder materializes from an unvalidated manifest path")
    sandbox = slice_between(
        source, "def sandboxed_build_argv(", "def directory_identity(",
        "builder_system_closure",
    )
    if (
        '"--ro-bind",\n        "/",\n        "/"' in sandbox
        or '"--ro-bind", "/", "/"' in sandbox
    ):
        fail("builder_system_closure", "builder exposes the whole host root")
    if (
        '"--dev-bind"' in sandbox
        or '"--dev"' in sandbox
        or '"--proc"' in sandbox
        or '"--dir",\n        "/dev"' not in sandbox
        or '"--dir",\n        "/proc"' not in sandbox
    ):
        fail(
            "builder_system_closure",
            "builder exposes a device or proc interface instead of empty directories",
        )
    if '"/etc"' in sandbox or 'Path("/etc")' in source:
        fail("builder_system_closure", "builder exposes unreviewed /etc authority")
    cargo_environment_source = slice_between(
        source, "def cargo_environment(", "def sandboxed_build_argv(",
        "builder_system_closure",
    )
    if (
        '"GIT_CONFIG_GLOBAL": f"{GUEST_ROOT}/absent-gitconfig"'
        not in cargo_environment_source
        or '"GIT_CONFIG_GLOBAL": "/dev/null"' in cargo_environment_source
    ):
        fail(
            "builder_system_closure",
            "sandboxed Git config reaches a guest device path",
        )
    runtime_digest = slice_between(
        source, "def semantic_runtime_sha256(", "def sha256_file(",
        "builder_semantic_runtime",
    )
    if "manifest_path" in runtime_digest or '"source"' in runtime_digest:
        fail(
            "builder_semantic_runtime",
            "runtime equality includes relocatable/source-only authority",
        )
    for marker in (
        '"mounts",',
        'mount.get("guest_path") != guest_path',
        'mount.get("host_path") != str(host_path)',
        'mount.get("resolved_path") != str(host_path)',
        'raise BuildError("semantic runtime trusted-system mounts differ")',
    ):
        require_once(runtime_digest, marker, "builder_semantic_runtime")
    freeze_source = slice_between(
        source, "def make_read_only(", "def install_lock(",
        "builder_idempotent_freeze",
    )
    for marker in (
        "if stat.S_IMODE(path.stat().st_mode) != 0o555:",
        "if stat.S_IMODE(path.stat().st_mode) != desired:",
        "if stat.S_IMODE(root.stat().st_mode) != 0o555:",
    ):
        require_once(freeze_source, marker, "builder_idempotent_freeze")
    for marker in (
        '(Path("/usr/bin"), "/usr/bin")',
        '(Path("/usr/lib"), "/usr/lib")',
        '(Path("/usr/include"), "/usr/include")',
        "metadata.st_uid != 0",
        "stat.S_IMODE(metadata.st_mode) & 0o022",
        "os.access(",
        'self.evidence_root / f"{self.label}-system-closure.json"',
        '"entry_count": entry_count',
        '"mutation_events_absent": True',
        '"watch_count": watch_count',
        "def self_test_idempotent_freeze() -> dict[str, Any]:",
        'evidence_root = temporary_root / "evidence"',
        'evidence_root / "before.json"',
        'evidence_root / "after.json"',
        'raise BuildError("idempotent final freeze changed recursive evidence")',
    ):
        if marker not in source:
            fail("builder_system_closure", f"system/freeze marker absent: {marker}")
    if source.count("run_build(") != 4:
        fail("builder_build_count", "run_build definition/call cardinality differs")
    if source.count("materialize(") != 2:
        fail("builder_build_count", "materialize definition/call cardinality differs")
    if source.count("verify_open_built_binary(") != 4:
        fail("builder_release_identity", "release binary verification differs")
    if source.count("require_value=False") != 2:
        fail("builder_lock_guard", "materialized lock snapshots require parsed values")
    if source.count("reviewed_stage_admission=reviewed_stage_admission") != 3:
        fail("builder_admission", "each of the three builds needs fresh admission")
    if source.count(
        "reviewed_cargo_config_entries=cargo_config_entries"
    ) != 3:
        fail("builder_cargo_config", "each build needs exact Cargo config authority")
    if source.count(
        "reviewed_cargo_config_empty=cargo_config_empty_path"
    ) != 3:
        fail("builder_cargo_config", "each build needs reviewed empty config authority")
    if source.count('"--ro-bind-data"') != 6:
        fail("builder_cargo_config", "Cargo config/data view must use exact-byte FD binds")
    if source.count('record["stdin_bytes"] = len(stdin_payload)') != 1:
        fail("builder_execution_lease", "stdin payload evidence cardinality differs")
    if source.count("self.verify_relative_selection()") != 2:
        fail("builder_execution_lease", "retained relative file replay differs")
    run_capture = slice_between(
        source, "def run_capture(", "def run_logged(", "builder_execution_lease"
    )
    for marker in (
        "execution_lease: RetainedFile,",
        "executable=execution_lease.proc_path",
        "pass_fds=inherited",
        '"execution_authority": execution_lease.record()',
    ):
        require_once(run_capture, marker, "builder_execution_lease")
    if run_capture.count("execution_lease.verify()") != 2:
        fail("builder_execution_lease", "executable lease needs immediate pre/post replay")
    run_logged = slice_between(
        source, "def run_logged(", "def validator_output(", "builder_boundary_replay"
    )
    for marker in (
        "boundary_replay: Callable[[], None] | None = None,",
        "finally:\n        if boundary_replay is not None:\n            boundary_replay()",
    ):
        require_once(run_logged, marker, "builder_boundary_replay")
    release_proof = slice_between(
        source,
        "def release_compile_out_proof(",
        "def input_paths(",
        "builder_nm_lease_scope",
    )
    require_once(
        release_proof,
        "            nm_lease.verify()\n            nm_after = nm_lease.record()",
        "builder_nm_lease_scope",
    )
    require_order(
        release_proof,
        (
            "with RetainedFile(",
            "hooked_inventory, hooked_record = nm_inventory(",
            "            nm_lease.verify()",
            "        if nm_after != nm_before:",
        ),
        "builder_nm_lease_scope",
    )
    cargo_guard = slice_between(
        source,
        "class CargoConfigSearchGuard:",
        "def read_bound_regular_file(",
        "builder_cargo_config",
    )
    cargo_policy = slice_between(
        source,
        "def reviewed_cargo_config_policy(",
        "def canonical_archive_member(",
        "builder_cargo_config",
    )
    for marker in (
        'recorded.get("cwd") != "/asterism/source"',
        'recorded.get("cargo_home_path") != GUEST_CARGO_HOME',
        '!= list(CARGO_CONFIG_GUEST_PATHS)',
        'manifest_path.with_name(f"{manifest_path.name}.empty")',
        'sha256_file(empty_path) != EMPTY_SHA256',
        "translated = json.loads(json.dumps(entries))",
        'entry["status"] != "present" or entry["sha256"] is None',
    ):
        require_once(cargo_policy, marker, "builder_cargo_config")
    for marker in CARGO_CONFIG_GUEST_MARKERS:
        require_once(source, marker, "builder_cargo_config")
    for marker in (
        "len(self.expected_entries) != len(CARGO_CONFIG_GUEST_PATHS)",
        "self.empty_leases: dict[str, RetainedFile] = {}",
        "if expected == {",
        "empty_lease = self.empty_leases.get(key)",
        'f"{self.context} empty-bound Cargo config appeared"',
        "*(lease.descriptor for lease in self.empty_leases.values())",
        'self.pre_build = self.replay(boundary="guard activation")',
        'self.post_build = self.replay(boundary="guard exit")',
        "if self.post_build != self.pre_build:",
        '"--ro-bind-data"',
        '"--ro-bind-fd"',
    ):
        if marker in {'"--ro-bind-data"', '"--ro-bind-fd"'}:
            if marker not in cargo_guard:
                fail("builder_cargo_config", f"Cargo view marker absent: {marker}")
        else:
            require_once(cargo_guard, marker, "builder_cargo_config")
    if cargo_guard.count("lease.rewind_for_bind_data()") != 2:
        fail("builder_cargo_config", "actual/empty Cargo config rewinds differ")
    static_authority = slice_between(
        source,
        "def validate_static_authority(",
        "def validate_product_overlay_authority(",
        "builder_static_authority",
    )
    for marker in (
        'for arguments in ((), ("--self-test",)):',
        'if outputs[0]["checks"] != outputs[1]["checks"]:',
        'if outputs[0]["hostile_mutations_rejected"] != 0:',
        'if outputs[1]["hostile_mutations_rejected"] <= 0:',
    ):
        require_once(static_authority, marker, "builder_static_authority")
    overlay_authority = slice_between(
        source,
        "def validate_product_overlay_authority(",
        "def validate_tools_manifest(",
        "builder_overlay_authority",
    )
    for marker in (
        'for arguments in ((), ("--self-test",)):',
        'value.get("schema") != PRODUCT_OVERLAY_VALIDATOR_SCHEMA',
        'value.get("patch_sha256") != patch_before["sha256"]',
        'outputs[0].get("outcome") != "PASS"',
        'outputs[1].get("outcome") != "SELF_TEST_PASS"',
        'self_checks[0] != "canonical_overlay"',
        'patch_after != patch_before or validator_after != validator_before',
    ):
        require_once(overlay_authority, marker, "builder_overlay_authority")
    tree = ast.parse(source, filename="build_children.py")
    child_assignments = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "child_extra"
            for target in node.targets
        )
    ]
    if len(child_assignments) != 1 or not isinstance(child_assignments[0].value, ast.Dict):
        fail("builder_fault_environment", "child fault environment is not one literal")
    fault_environment = {
        key.value: value
        for key, value in zip(
            child_assignments[0].value.keys,
            child_assignments[0].value.values,
            strict=True,
        )
        if isinstance(key, ast.Constant)
        and isinstance(key.value, str)
        and key.value.startswith("ASTERISM_FAULT_")
    }
    expected_fault_keys = {
        "ASTERISM_FAULT_COMPILE_OUT_IDENTICAL",
        "ASTERISM_FAULT_COMPILE_OUT_OVERLAY_RELEASE_SHA256",
        "ASTERISM_FAULT_COMPILE_OUT_PRISTINE_SHA256",
        "ASTERISM_FAULT_COMPILE_OUT_SCHEMA",
        "ASTERISM_FAULT_COMPILE_OUT_SYMBOL_ABSENCE_SHA256",
    }
    if set(fault_environment) != expected_fault_keys:
        fail("builder_fault_environment", "fault compile-out environment differs")
    expected_values = {
        "ASTERISM_FAULT_COMPILE_OUT_IDENTICAL": ast.Constant(value="true"),
        "ASTERISM_FAULT_COMPILE_OUT_OVERLAY_RELEASE_SHA256": ast.Subscript(
            value=ast.Name(id="compile_out", ctx=ast.Load()),
            slice=ast.Constant(value="overlay_release_sha256"),
            ctx=ast.Load(),
        ),
        "ASTERISM_FAULT_COMPILE_OUT_PRISTINE_SHA256": ast.Subscript(
            value=ast.Name(id="compile_out", ctx=ast.Load()),
            slice=ast.Constant(value="pristine_sha256"),
            ctx=ast.Load(),
        ),
        "ASTERISM_FAULT_COMPILE_OUT_SCHEMA": ast.Name(
            id="FAULT_COMPILE_OUT_SCHEMA", ctx=ast.Load()
        ),
        "ASTERISM_FAULT_COMPILE_OUT_SYMBOL_ABSENCE_SHA256": ast.Subscript(
            value=ast.Name(id="compile_out", ctx=ast.Load()),
            slice=ast.Constant(value="symbol_absence_sha256"),
            ctx=ast.Load(),
        ),
    }
    if any(
        ast.dump(fault_environment[name], include_attributes=False)
        != ast.dump(expected_values[name], include_attributes=False)
        for name in sorted(expected_fault_keys)
    ):
        fail("builder_fault_environment", "fault compile-out values differ")
    require_order(
        source,
        (
            "static_authority = validate_static_authority(repository)",
            "product_overlay_authority = validate_product_overlay_authority(repository)",
            "fault_authority = validate_fault_authority(repository)",
            "validate_lock_authority(\n            repository, lock_manifest_path, review_bundle_path, authority_path",
            "validated_authority_before = validated_authority_records(validated)",
            "validated_locks_before = validated_lock_records(validated)",
            "inputs_before = identities(tracked_inputs, \"before\")",
            "materialized = {",
            "pristine_build = run_build(",
            "hooked_build = run_build(",
            "compile_out = release_compile_out_proof(",
            "child_extra = {",
            "child_build = run_build(",
            "final_tools[\"tools\"].update(child_bindings)",
            "validated_locks_after = validated_lock_records(validated)",
            "validated_authority_after = validated_authority_records(validated)",
            "attestation = {",
            "make_read_only(output)",
        ),
        "builder_sequence",
    )
    run_build = slice_between(source, "def run_build(", "def snapshot_open_file(", "builder_lock_identity")
    require_order(
        run_build,
        (
            'target = output / "targets" / kind',
            "target.mkdir(parents=True)",
            "filesystem_admission = (",
            "lock_authority_module.resample_builder_filesystem_admission(",
            "guard_factory = lock_authority_module.capture_prebuild_materialized_tree",
            "with ExitStack() as stack:",
            "BoundBuildDirectory(target",
            "\n        guard = stack.enter_context(\n            guard_factory(",
            "source_tree_guard = stack.enter_context(",
            "toolchain_guard = stack.enter_context(",
            "system_guard = stack.enter_context(",
            'source_ro_bind = guard.bwrap_ro_bind("/asterism/source")',
            "target_bind = target_guard.bwrap_bind(",
            '"--ro-bind-fd",\n                str(wrapper_descriptor),',
            "def replay_cargo_boundary() -> None:",
            "config_postbuild = config_guard.replay(",
            'source_tree_guard.replay("post-Cargo boundary")',
            'toolchain_guard.replay("post-Cargo boundary")',
            'system_guard.replay("post-Cargo boundary")',
            "record = run_logged(",
            "boundary_replay=replay_cargo_boundary,",
            "after = file_manifest(materialized[\"root\"])",
            "artifacts = {",
            "receipt_payload, receipt_identity = read_bound_regular_file(",
            "lock_after = immutable_snapshot_record(",
        ),
        "builder_lock_identity",
    )
    materialize = slice_between(source, "def materialize(", "def cargo_environment(", "builder_read_only")
    require_order(
        materialize,
        (
            "extract_archive_payload(archive, root)",
            "install_lock(root, lock_payload)",
            "apply_product_overlay(\n            root",
            "make_read_only(root)",
            "manifest = file_manifest(root)",
        ),
        "builder_read_only",
    )
    manifest = slice_between(
        source, "def manifest_entry(", "def make_read_only(", "builder_manifest"
    )
    if ".is_dir()" in manifest or ".is_file()" in manifest:
        fail("builder_manifest", "manifest follows a path before lstat classification")
    for marker in (
        "before = path.lstat()",
        "if stat.S_ISLNK(before.st_mode):",
        "if stat.S_ISDIR(before.st_mode):",
        "elif stat.S_ISREG(before.st_mode):",
        'getattr(os, "O_NOFOLLOW", 0)',
        "opened_before = os.fstat(descriptor)",
        "opened_after = os.fstat(descriptor)",
        "after = path.lstat()",
        '"file_type": file_type',
        '"link_count": before.st_nlink',
        '"modified_ns": before.st_mtime_ns',
        '"changed_ns": before.st_ctime_ns',
        'paths = [root, *sorted(root.rglob("*"))]',
        "first = manifest_pass(root)",
        "second = manifest_pass(root)",
        "if first != second:",
        '"schema": "bn-30fs-file-manifest-v2"',
    ):
        require_once(manifest, marker, "builder_manifest")
    return [
        "builder_future_authority_interfaces",
        "builder_validated_lock_payload_and_full_identity",
        "builder_three_fresh_offline_sandboxes",
        "builder_real_overlay_check_and_apply",
        "builder_live_filesystem_admission",
        "builder_lock_authority_materialized_lock_helpers",
        "builder_exact_fault_compile_out_environment",
        "builder_release_byte_symbol_string_compile_out",
        "builder_release_proof_precedes_cfg_child",
        "builder_downstream_tools_schema_preserved",
        "builder_inputs_toolchain_and_outputs_immutable",
        "builder_full_tree_identity_manifest",
        "builder_descriptor_bound_writable_binds_and_artifacts",
        "builder_overlay_and_static_hostile_authorities",
        "builder_descriptor_executable_selection_and_replay",
        "builder_cross_bound_lock_authority_context",
        "builder_exact_eight_entry_guest_cargo_config_view",
        "builder_retained_toolchain_tree_and_cargo_rustc",
        "builder_exact_wrapper_receipt_consumer",
        "builder_normalized_authority_failures",
        "builder_descriptor_cargo_config_guard_probe",
        "builder_recursive_cargo_home_freeze_authority",
        "builder_recursive_semantic_input_authority",
        "builder_narrow_trusted_system_closure",
        "builder_path_free_runtime_equality",
        "builder_idempotent_final_freeze",
    ]


def validate_correctness(source: str) -> list[str]:
    case_table = slice_between(
        source, "const CASES:", "];", "correctness_cases"
    )
    for case in CASES:
        require_once(case_table, f'("{case}",', "correctness_cases")
    for marker in (
        'if arguments == ["--smoke"] {',
        'assert_eq!(required("ASTERISM_REBASELINE_MODE"), "smoke");',
        'required("ASTERISM_REBASELINE_SMOKE_TARGET")',
        'Invocation::Correctness(correctness_args())',
        'Invocation::Correctness(_) => "correctness"',
        'Invocation::Smoke => "smoke"',
        'json_string("bn-2l3n-smoke-v3")',
        '("smoke_target", json_string("correctness"))',
        "Invocation::Correctness(args) => emit_result(&args)",
        "Invocation::Smoke => emit_smoke_result()",
    ):
        require_once(source, marker, "correctness_smoke")
    if source.count('required("ASTERISM_REBASELINE_PROTOCOL")') != 2:
        fail("correctness_smoke", "correctness/smoke protocol bindings differ")
    main = slice_between(source, "fn main()", "\n}", "correctness_control")
    if "return" in main:
        fail("correctness_control", "smoke may not bypass the common control path")
    require_order(
        main,
        (
            "let invocation = invocation();",
            "let mut control = Control::connect();",
            "let boot_nonce = control.boot();",
            "let runtime_nonce = control.runtime(&boot_nonce);",
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
            "match invocation {",
        ),
        "correctness_control",
    )
    smoke = slice_between(source, "fn emit_smoke_result()", "fn main()", "correctness_smoke_output")
    require_once(smoke, '("status", json_string("PASS"))', "correctness_smoke_output")
    for forbidden in (
        "t0_monotonic_ns",
        "t1_monotonic_ns",
        "allocated_bytes",
        "allocation_calls",
        "process_user_cpu",
        "process_system_cpu",
    ):
        if forbidden in smoke:
            fail("correctness_smoke_output", f"smoke result contains measurement field {forbidden}")
    return [
        "correctness_smoke_exact_argv_environment",
        "correctness_smoke_same_seven_case_control_path",
        "correctness_smoke_aux_output_has_no_measurement_fields",
    ]


def validate_overlay(patch: str, validator: str) -> list[str]:
    for marker in (
        "+#[cfg(all(test, not(miri), not(asterism_rebaseline_correctness)))]\n mod append_gate_tests;",
        "+#[cfg(all(test, not(asterism_rebaseline_correctness)))]\n mod seal_skip_tests {",
    ):
        require_once(patch, marker, "overlay_negative_guards")
    if patch.count(CORRECTNESS_CFG) != 2:
        fail("overlay_negative_guards", "correctness cfg escapes the two negative guards")
    parse_python(validator, "validate_product_test_overlay.py", "overlay_validator")
    for marker in (
        'CORRECTNESS_CFG = "asterism_rebaseline_correctness"',
        'if source.count(CORRECTNESS_CFG) != 2 or patch_text.count(CORRECTNESS_CFG) != 2:',
        '"missing_append_gate_negative_guard"',
        '"missing_seal_skip_negative_guard"',
        '"dev_only_test_modules_excluded_from_child_dependency_unit"',
    ):
        require_once(validator, marker, "overlay_validator")
    return ["overlay_two_exact_dev_test_negative_guards"]


def validate_all(
    builder: str,
    wrapper: str,
    correctness: str,
    patch: str,
    overlay_validator: str,
    *,
    semantic_wrapper: bool = True,
) -> list[str]:
    checks = []
    checks.extend(validate_builder(builder))
    checks.extend(validate_wrapper(wrapper))
    if semantic_wrapper:
        checks.extend(validate_wrapper_semantics())
    checks.extend(validate_correctness(correctness))
    checks.extend(validate_overlay(patch, overlay_validator))
    if len(checks) != len(set(checks)):
        fail("check_identity", "static check identities are duplicated")
    return checks


def replace_once(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise AssertionError(f"hostile mutation target is not unique: {old!r}")
    return source.replace(old, new, 1)


def expect_rejection(
    name: str,
    expected: str,
    callback: Callable[[], Any],
) -> str:
    try:
        callback()
    except ValidationError as error:
        if error.code != expected:
            raise AssertionError(
                f"{name}: expected {expected}, observed {error.code}: {error}"
            ) from error
        return name
    raise AssertionError(f"{name}: hostile mutation was accepted")


def self_test(
    builder: str,
    wrapper: str,
    correctness: str,
    patch: str,
    overlay_validator: str,
) -> list[str]:
    validate_all(builder, wrapper, correctness, patch, overlay_validator)
    passed = ["canonical_sources"]

    def reject_builder(name: str, old: str, new: str, code: str) -> None:
        if builder.count(old) != 1 and name.startswith("builder_cargo_home_"):
            start = builder.index("class CargoConfigSearchGuard:")
            end = builder.index("def read_bound_regular_file(", start)
            scoped = replace_once(builder[start:end], old, new)
            mutated = builder[:start] + scoped + builder[end:]
        else:
            mutated = replace_once(builder, old, new)
        passed.append(expect_rejection(name, code, lambda: validate_builder(mutated)))

    def reject_wrapper(name: str, old: str, new: str, code: str) -> None:
        mutated = replace_once(wrapper, old, new)
        passed.append(expect_rejection(name, code, lambda: validate_wrapper(mutated)))

    def reject_correctness(name: str, old: str, new: str, code: str) -> None:
        mutated = replace_once(correctness, old, new)
        passed.append(expect_rejection(name, code, lambda: validate_correctness(mutated)))

    reject_wrapper("wrapper_global_selection", "if names_target != sources_target:", "if names_target == sources_target:", "wrapper_contract")
    reject_wrapper(
        "wrapper_unretained_python_shebang",
        "#!/asterism/python3",
        "#!/usr/bin/python3",
        "wrapper_contract",
    )
    reject_wrapper("wrapper_no_exclusive_receipt", "os.O_EXCL", "os.O_TRUNC", "wrapper_contract")
    reject_wrapper(
        "wrapper_receipt_not_chmod_0444",
        "os.fchmod(descriptor, 0o444)",
        "os.fchmod(descriptor, 0o666)",
        "wrapper_contract",
    )
    reject_wrapper("wrapper_no_test_cfg", '"--cfg",\n    "test"', '"--cfg",\n    "debug_assertions"', "wrapper_contract")
    reject_wrapper("wrapper_no_builtin_cfg_allow", '"--allow",\n    "explicit_builtin_cfgs_in_flags"', '"--allow",\n    "warnings"', "wrapper_contract")
    reject_wrapper("wrapper_no_private_marker", '"--cfg",\n    CORRECTNESS_CFG', '"--cfg",\n    "other"', "wrapper_contract")
    reject_wrapper("wrapper_no_check_cfg", '"--check-cfg",\n    f"cfg({CORRECTNESS_CFG})"', '"--check-cfg",\n    "cfg(other)"', "wrapper_contract")
    reject_wrapper("wrapper_spawns_rustc", "os.execv(plan.rustc", "os.spawnv(os.P_WAIT, plan.rustc", "wrapper_contract")
    reject_wrapper("wrapper_receipt_after_exec", "write_receipt(Path(os.environ[RECEIPT_ENV]), plan.receipt)", "pass  # hostile missing receipt", "wrapper_receipt")

    reject_builder("builder_network_shared", '"--unshare-net"', '"--share-net"', "builder_contract")
    reject_builder(
        "builder_env_selected_interpreter",
        "#!/usr/bin/python3",
        "#!/usr/bin/env python3",
        "builder_contract",
    )
    reject_builder("builder_unlocked", '"--locked"', '"--frozen-no"', "builder_contract")
    reject_builder("builder_online", '"--offline"', '"--online"', "builder_contract")
    reject_builder(
        "builder_executes_mutable_path",
        "executable=execution_lease.proc_path",
        "executable=logical_argv[0]",
        "builder_contract",
    )
    reject_builder(
        "builder_executable_fd_not_inherited",
        "inherited = tuple(dict.fromkeys((*pass_fds, *execution_lease.pass_fds)))",
        "inherited = tuple(dict.fromkeys(pass_fds))",
        "builder_contract",
    )
    reject_builder(
        "builder_executable_no_post_replay",
        "execution_lease.verify()\n    stdout = completed.stdout",
        "# hostile: no post-exec lease replay\n    stdout = completed.stdout",
        "builder_execution_lease",
    )
    reject_builder(
        "builder_lock_uses_default_nested_runner",
        "semantic_validator=current_context,",
        "semantic_validator=None,",
        "builder_contract",
    )
    reject_builder(
        "builder_lock_dependency_reopened_by_path",
        "dependency_lease.payload,\n                    str(dependency),",
        "dependency.read_bytes(),\n                    str(dependency),",
        "builder_contract",
    )
    reject_builder(
        "builder_lock_module_reopened_by_path",
        "validator_lease.payload,\n                    str(validator),",
        "validator.read_bytes(),\n                    str(validator),",
        "builder_contract",
    )
    reject_builder(
        "builder_prepare_dependency_not_tracked",
        "LOCK_AUTHORITY_VALIDATOR,\n        PREPARE_OVERLAYS,",
        "LOCK_AUTHORITY_VALIDATOR,\n        # hostile omitted dependency",
        "builder_contract",
    )
    reject_builder(
        "builder_lock_omits_identity_revalidator",
        "identity_revalidator=identity_revalidator,",
        "identity_revalidator=None,",
        "builder_contract",
    )
    reject_builder(
        "builder_lock_manifest_not_cross_bound",
        'or embedded_manifest.get("payload") != lock_preview',
        'or embedded_manifest.get("payload") == lock_preview',
        "builder_contract",
    )
    reject_builder(
        "builder_lock_review_not_cross_bound",
        'or embedded_review.get("payload") != review_preview',
        'or embedded_review.get("payload") == review_preview',
        "builder_contract",
    )
    reject_builder(
        "builder_python_validator_without_lease",
        "execution_lease=python_lease,",
        "execution_lease=script_lease,",
        "builder_contract",
    )
    reject_builder(
        "builder_python_validator_fd_at_eof",
        "script_lease.rewind_for_bind_data()",
        "pass  # hostile validator source FD left at EOF",
        "builder_contract",
    )
    reject_builder(
        "builder_git_patch_reopened_by_path",
        "stdin_payload=patch_lease.payload",
        "stdin_payload=None",
        "builder_contract",
    )
    reject_builder(
        "builder_bwrap_without_lease",
        "execution_lease=bwrap_lease,",
        "execution_lease=cargo_lease,",
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_boundary_replay_not_wired",
        "boundary_replay=replay_cargo_boundary,",
        "boundary_replay=None,",
        "builder_contract",
    )
    reject_builder(
        "builder_boundary_replay_not_on_failure",
        "finally:\n        if boundary_replay is not None:\n            boundary_replay()",
        "except BaseException:\n        raise\n    else:\n        if boundary_replay is not None:\n            boundary_replay()  # hostile success-only replay",
        "builder_boundary_replay",
    )
    reject_builder(
        "builder_nm_without_lease",
        "execution_lease=nm_lease,",
        "execution_lease=None,",
        "builder_contract",
    )
    reject_builder(
        "builder_nm_verified_after_lease_close",
        "            nm_lease.verify()\n            nm_after = nm_lease.record()",
        "        nm_lease.verify()\n        nm_after = nm_lease.record()",
        "builder_nm_lease_scope",
    )
    reject_builder(
        "builder_host_cargo_home",
        '"CARGO_HOME": GUEST_CARGO_HOME',
        '"CARGO_HOME": toolchain["cargo_home_path"]',
        "builder_contract",
    )
    reject_builder(
        "builder_host_rustc_path",
        '"RUSTC": GUEST_RUSTC',
        '"RUSTC": toolchain["rustc_path"]',
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_not_fd_mounted",
        'cargo_bind = ("--ro-bind-fd", str(cargo_lease.descriptor), GUEST_CARGO)',
        'cargo_bind = ("--ro-bind", str(cargo_path), GUEST_CARGO)',
        "builder_contract",
    )
    reject_builder(
        "builder_rustc_not_fd_mounted",
        'rustc_bind = ("--ro-bind-fd", str(rustc_lease.descriptor), GUEST_RUSTC)',
        'rustc_bind = ("--ro-bind", str(rustc_path), GUEST_RUSTC)',
        "builder_contract",
    )
    reject_builder(
        "builder_bound_directory_reopens_path",
        '"--ro-bind-fd" if read_only else "--bind-fd"',
        '"--ro-bind" if read_only else "--bind"',
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_config_fd_not_rewound",
        "for lease in self.file_leases.values():\n            lease.rewind_for_bind_data()",
        "for lease in self.file_leases.values():\n            pass  # hostile bind-data offset left at EOF",
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_top_level_enumerated_by_path",
        "names = sorted(os.listdir(enumeration_descriptor))",
        "names = sorted(os.listdir(path))",
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_regular_entry_opened_by_path",
        "parent_descriptor=guard.descriptor,",
        "parent_descriptor=None,",
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_home_overflow_not_fatal",
        "| 0x00004000  # IN_Q_OVERFLOW",
        "| 0x00000000  # hostile ignored queue overflow",
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_home_ignored_watch_not_fatal",
        "| 0x00008000  # IN_IGNORED",
        "| 0x00000000  # hostile ignored watch removal",
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_home_events_accepted",
        "if observed:\n            self.poisoned = True",
        "if False:\n            self.poisoned = True",
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_home_watch_not_inode_bound",
        'os.fsencode(f"/proc/self/fd/{descriptor}")',
        "os.fsencode(str(self.cargo_home))",
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_home_directory_unwatched",
        "self._add_directory_watch(descriptor)",
        "pass  # hostile recursive directory left unwatched",
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_home_manifest_not_double_captured",
        'if first != second:\n            raise BuildError(f"{self.context} Cargo-home initial manifests differ")',
        'if False:\n            raise BuildError(f"{self.context} Cargo-home initial manifests differ")',
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_home_not_deep_replayed_post_child",
        'boundary="post-Cargo boundary", deep=True',
        'boundary="post-Cargo boundary", deep=False',
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_home_reopened_in_bwrap",
        'str(self.directory_guards["cargo-home"].descriptor)',
        "str(self.cargo_home)",
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_home_content_hash_unbound",
        'observed_sha256 != self.cargo_home_tree_binding["pre_sha256"]',
        "observed_sha256 != observed_sha256",
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_home_external_hardlink_accepted",
        'if len(aliases) != aliases[0]["link_count"]:',
        "if False:",
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_home_escaping_symlink_accepted",
        "if resolved != self.cargo_home and self.cargo_home not in resolved.parents:",
        "if False:",
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_home_evidence_not_retained",
        "self.cargo_home_tree_evidence = evidence",
        "self.cargo_home_tree_evidence = None",
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_home_nested_restore_probe_removed",
        "dependency.write_bytes(b\"hostile dependency bytes\\n\")",
        "dependency.write_bytes(dependency_payload)",
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_config_guard_probe_not_run",
        "cargo_config_guard = self_test_cargo_config_guard()",
        'cargo_config_guard = {"status": "ok"}',
        "builder_contract",
    )
    reject_builder(
        "builder_reviewed_cargo_config_policy_probe_not_run",
        "reviewed_cargo_config = self_test_reviewed_cargo_config_policy()",
        'reviewed_cargo_config = {"status": "ok"}',
        "builder_contract",
    )
    reject_builder(
        "builder_reviewed_cargo_config_reinterprets_guest_cwd",
        'recorded.get("cwd") != "/asterism/source"',
        'recorded.get("cwd") == recorded.get("cwd")',
        "builder_cargo_config",
    )
    reject_builder(
        "builder_reviewed_cargo_config_reinterprets_guest_home",
        'recorded.get("cargo_home_path") != GUEST_CARGO_HOME',
        'recorded.get("cargo_home_path") == recorded.get("cargo_home_path")',
        "builder_cargo_config",
    )
    reject_builder(
        "builder_reviewed_cargo_config_translates_again",
        "translated = json.loads(json.dumps(entries))",
        "translated = []  # hostile discarded reviewed guest view",
        "builder_cargo_config",
    )
    reject_builder(
        "builder_reviewed_empty_cargo_config_unbound",
        "if expected == {",
        "if False and expected == {",
        "builder_cargo_config",
    )
    reject_builder(
        "builder_empty_bound_cargo_config_appearance_accepted",
        'raise BuildError(\n                        f"{self.context} empty-bound Cargo config appeared"\n                    )',
        "pass  # hostile accepts a path that appeared after empty binding",
        "builder_cargo_config",
    )
    reject_builder(
        "builder_cargo_config_not_exact_bytes",
        '"--ro-bind-data", str(lease.descriptor), f"{source_guest}/{name}"',
        '"--ro-bind", lease.proc_path, f"{source_guest}/{name}"',
        "builder_cargo_config",
    )
    reject_builder(
        "builder_cargo_config_missing_root_candidate",
        '"/.cargo/config.toml"',
        '"/.cargo/other.toml"',
        "builder_cargo_config",
    )
    reject_builder(
        "builder_cargo_config_missing_asterism_candidate",
        '"/asterism/.cargo/config.toml"',
        '"/asterism/.cargo/other.toml"',
        "builder_cargo_config",
    )
    reject_builder(
        "builder_cargo_config_accepts_cached_only",
        'config_postbuild = config_guard.replay(\n                boundary="post-Cargo boundary", deep=True\n            )',
        "config_postbuild = config_prebuild  # hostile cached-only boundary",
        "builder_contract",
    )
    reject_builder(
        "builder_cargo_config_accepts_change",
        "if config_postbuild != config_prebuild:",
        "if False:",
        "builder_contract",
    )
    reject_builder(
        "builder_toolchain_no_post_manifest",
        'toolchain_guard.replay("post-Cargo boundary")',
        'toolchain_guard.replay("pre-Cargo launch")  # hostile cached replay',
        "builder_contract",
    )
    reject_builder(
        "builder_toolchain_accepts_change",
        "if observed != self.initial_manifest:\n            self.poisoned = True",
        "if False:\n            self.poisoned = True",
        "builder_contract",
    )
    reject_builder(
        "builder_whole_host_root_reintroduced",
        "        *trusted_system_args,\n        \"--dir\",\n        \"/dev\",",
        "        *trusted_system_args,\n        \"--ro-bind\", \"/\", \"/\",\n        \"--dir\",\n        \"/dev\",",
        "builder_system_closure",
    )
    reject_builder(
        "builder_devtmpfs_reintroduced",
        '"--dir",\n        "/dev",',
        '"--dev",\n        "/dev",',
        "builder_system_closure",
    )
    reject_builder(
        "builder_procfs_reintroduced",
        '"--dir",\n        "/proc",',
        '"--proc",\n        "/proc",',
        "builder_system_closure",
    )
    reject_builder(
        "builder_system_closure_broadened_to_etc",
        '(Path("/usr/include"), "/usr/include")',
        '(Path("/etc"), "/etc")',
        "builder_system_closure",
    )
    reject_builder(
        "builder_system_post_replay_removed",
        'system_guard.replay("post-Cargo boundary")',
        'system_guard.replay("pre-Cargo launch")  # hostile cached replay',
        "builder_contract",
    )
    reject_builder(
        "builder_runtime_digest_binds_paths",
        '"mutation_events_absent",\n    )\n    if set(components)',
        '"mutation_events_absent",\n        "manifest_path",\n    )\n    if set(components)',
        "builder_semantic_runtime",
    )
    reject_builder(
        "builder_runtime_accepts_forged_system_mount",
        'mount.get("host_path") != str(host_path)',
        'mount.get("host_path") == mount.get("host_path")',
        "builder_semantic_runtime",
    )
    reject_builder(
        "builder_idempotent_freeze_removed",
        "if stat.S_IMODE(path.stat().st_mode) != desired:\n                path.chmod(desired)",
        "path.chmod(desired)  # hostile unconditional final freeze",
        "builder_idempotent_freeze",
    )
    reject_builder(
        "builder_idempotent_evidence_mutates_selected_parent",
        'evidence_root / "before.json"',
        'temporary_root / "before.json"',
        "builder_system_closure",
    )
    reject_builder(
        "builder_wrapper_guest_rustc_unbound",
        '"ASTERISM_REBASELINE_PINNED_RUSTC": GUEST_RUSTC',
        '"ASTERISM_REBASELINE_PINNED_RUSTC": toolchain["rustc_path"]',
        "builder_contract",
    )
    reject_builder(
        "builder_review_bundle_not_validated",
        "repository,\n            lock_manifest_path,\n            review_bundle_path,\n            authority_path,",
        "repository,\n            lock_manifest_path,\n            authority_path,\n            authority_path,",
        "builder_contract",
    )
    reject_builder("builder_manifest_lock_payload", 'lock_payload = validated.locks["A"].payload', 'lock_payload = Path(locks["variants"]["A"]["final_lock_path"]).read_bytes()', "builder_contract")
    reject_builder("builder_wrong_validated_lock", 'lock_payload = validated.locks["A"].payload', 'lock_payload = validated.locks["C"].payload', "builder_contract")
    reject_builder(
        "builder_no_materialized_tree_guard",
        "guard_factory = lock_authority_module.capture_prebuild_materialized_tree",
        "guard_factory = lock_authority_module.capture_unverified_materialized_tree",
        "builder_contract",
    )
    reject_builder(
        "builder_guard_snapshot_requires_json_value",
        'guard.pre_build,\n            f"{kind} prebuild Cargo.lock",\n            require_value=False,',
        'guard.pre_build,\n            f"{kind} prebuild Cargo.lock",\n            require_value=True,',
        "builder_lock_guard",
    )
    reject_builder(
        "builder_guard_path_instead_of_retained_fd",
        'source_ro_bind = guard.bwrap_ro_bind("/asterism/source")',
        'source_ro_bind = ("--ro-bind", str(materialized["root"]), "/asterism/source")',
        "builder_contract",
    )
    reject_builder(
        "builder_guard_fd_not_passed",
        "pass_fds=inherited_descriptors",
        "pass_fds=()",
        "builder_contract",
    )
    reject_builder(
        "builder_target_fd_not_inherited",
        "guard.pass_fds\n            + target_guard.pass_fds",
        "guard.pass_fds\n            + ()  # hostile omitted target FD",
        "builder_contract",
    )
    reject_builder(
        "builder_wrapper_receipt_fds_not_inherited",
        "inherited_descriptors += (wrapper_descriptor, *receipt_guard.pass_fds)",
        "inherited_descriptors += (wrapper_descriptor,)",
        "builder_contract",
    )
    reject_builder(
        "builder_guard_source_not_fd_bound",
        'guard.ro_bind_source != f"/proc/self/fd/{guard.pass_fds[0]}"',
        'guard.ro_bind_source != str(materialized["root"])',
        "builder_contract",
    )
    reject_builder(
        "builder_guarded_source_used_as_launch_cwd",
        "cwd=output,",
        'cwd=materialized["root"],',
        "builder_contract",
    )
    reject_builder(
        "builder_no_live_admission",
        "lock_authority_module.resample_builder_filesystem_admission(",
        "lock_authority_module.trust_reviewed_filesystem_admission(",
        "builder_contract",
    )
    reject_builder(
        "builder_admissions_not_aggregated",
        '"prebuild_filesystem_admissions": {',
        '"prebuild_filesystem_admission": {',
        "builder_contract",
    )
    reject_builder(
        "builder_manifest_symlink_checked_after_type",
        "before = path.lstat()\n    if stat.S_ISLNK(before.st_mode):",
        "before = path.lstat()\n    if stat.S_ISDIR(before.st_mode):",
        "builder_manifest",
    )
    reject_builder(
        "builder_manifest_omits_ctime",
        '"changed_ns": before.st_ctime_ns',
        '"changed_ns": 0',
        "builder_manifest",
    )
    reject_builder(
        "builder_manifest_accepts_swap_restore",
        'if first != second:\n        raise BuildError("materialized tree changed between complete snapshots")',
        'if False:\n        raise BuildError("materialized tree changed between complete snapshots")',
        "builder_manifest",
    )
    reject_builder(
        "builder_target_bind_uses_path",
        'target_bind = target_guard.bwrap_bind(\n            "/asterism/target", read_only=False\n        )',
        'target_bind = ("--bind", str(target), "/asterism/target")  # hostile',
        "builder_contract",
    )
    reject_builder(
        "builder_bound_parent_not_verified",
        'raise BuildError(f"{self.context} parent selection changed")',
        'pass  # hostile parent selection accepted',
        "builder_contract",
    )
    reject_builder(
        "builder_wrapper_bind_uses_path",
        '"--ro-bind-fd",\n                str(wrapper_descriptor),',
        '"--ro-bind",\n                str(wrapper),',
        "builder_contract",
    )
    reject_builder(
        "builder_receipt_bind_uses_path",
        'receipt_guard.bwrap_bind(\n                "/asterism/receipt", read_only=False\n            )',
        '("--bind", str(receipt_root), "/asterism/receipt")',
        "builder_contract",
    )
    reject_builder(
        "builder_artifact_copy_uses_target_path",
        "example: copy_bound_artifact(",
        "example: copy_artifact(",
        "builder_contract",
    )
    reject_builder(
        "builder_receipt_fields_not_exact",
        '"original_argv_sha256",',
        '"unreviewed_field",',
        "builder_contract",
    )
    reject_builder(
        "builder_receipt_crate_not_bound",
        '"crate_name": WRAPPER_TARGET_CRATE',
        '"crate_name": "other"',
        "builder_contract",
    )
    reject_builder(
        "builder_receipt_package_not_bound",
        '"package": WRAPPER_TARGET_PACKAGE',
        '"package": "other"',
        "builder_contract",
    )
    reject_builder(
        "builder_receipt_source_not_bound",
        '"source": EXPECTED_LIB_SOURCE',
        '"source": "other.rs"',
        "builder_contract",
    )
    reject_builder(
        "builder_receipt_type_not_bound",
        '"crate_type": WRAPPER_TARGET_CRATE_TYPE',
        '"crate_type": "bin"',
        "builder_contract",
    )
    reject_builder(
        "builder_receipt_args_not_bound",
        '"injected_arguments": list(WRAPPER_INJECTED_ARGUMENTS)',
        '"injected_arguments": []',
        "builder_contract",
    )
    reject_builder(
        "builder_receipt_rustc_not_bound",
        '"rustc": pinned_rustc',
        '"rustc": receipt.get("rustc")',
        "builder_contract",
    )
    reject_builder(
        "builder_receipt_hash_not_lowerhex",
        'if not lower_hex(receipt.get("original_argv_sha256"), 64):',
        'if False:',
        "builder_contract",
    )
    reject_builder(
        "builder_receipt_mode_not_0444",
        'identity.get("mode") != 0o444',
        'identity.get("mode") != 0o666',
        "builder_contract",
    )
    reject_builder(
        "builder_receipt_bytes_not_bound",
        'identity.get("sha256") != sha256_bytes(payload)',
        'identity.get("sha256") != receipt.get("sha256")',
        "builder_contract",
    )
    reject_builder(
        "builder_receipt_size_not_bound",
        'identity.get("size") != len(payload)',
        'identity.get("size") < 0',
        "builder_contract",
    )
    reject_builder("builder_no_release_byte_equality", "if pristine_bytes != hooked_bytes:", "if pristine_bytes == hooked_bytes:", "builder_contract")
    reject_builder(
        "builder_release_proof_not_bound_to_copied_artifacts",
        'raise BuildError("release proof differs from descriptor-copied artifacts")',
        'pass  # hostile proof/copy mismatch accepted',
        "builder_contract",
    )
    reject_builder("builder_unpinned_nm", 'Path("/usr/bin/nm")', 'Path("nm")', "builder_contract")
    reject_builder(
        "builder_nm_not_descriptor_bound",
        'binary_source = f"/proc/self/fd/{binary_fd}"',
        'binary_source = str(output)',
        "builder_contract",
    )
    reject_builder(
        "builder_nm_descriptor_not_inherited",
        "pass_fds=(binary_fd,)",
        "pass_fds=()",
        "builder_contract",
    )
    reject_builder(
        "builder_fault_identical_false",
        '"ASTERISM_FAULT_COMPILE_OUT_IDENTICAL": "true"',
        '"ASTERISM_FAULT_COMPILE_OUT_IDENTICAL": "false"',
        "builder_fault_environment",
    )
    reject_builder(
        "builder_fault_pristine_uses_overlay",
        '"ASTERISM_FAULT_COMPILE_OUT_PRISTINE_SHA256": compile_out[\n            "pristine_sha256"\n        ]',
        '"ASTERISM_FAULT_COMPILE_OUT_PRISTINE_SHA256": compile_out[\n            "overlay_release_sha256"\n        ]',
        "builder_fault_environment",
    )
    reject_builder(
        "builder_fault_symbol_proof_unbound",
        '"ASTERISM_FAULT_COMPILE_OUT_SYMBOL_ABSENCE_SHA256": compile_out[\n            "symbol_absence_sha256"\n        ]',
        '"ASTERISM_FAULT_COMPILE_OUT_SYMBOL_ABSENCE_SHA256": compile_out[\n            "binary_sha256"\n        ]',
        "builder_fault_environment",
    )
    reject_builder(
        "builder_release_proof_not_before_child",
        "compile_out = release_compile_out_proof(",
        "proof = release_compile_out_proof(",
        "builder_sequence",
    )
    reject_builder("builder_no_tool_replacement", 'final_tools["tools"].update(child_bindings)', 'final_tools.update(child_bindings)', "builder_contract")
    reject_builder(
        "builder_unrestricted_child_placeholder",
        "if binding != CHILD_PLACEHOLDER_BINDINGS[name]:",
        "if False:",
        "builder_contract",
    )
    reject_builder(
        "builder_overlay_authority_not_run",
        "product_overlay_authority = validate_product_overlay_authority(repository)",
        "product_overlay_authority = {}",
        "builder_contract",
    )
    reject_builder(
        "builder_static_self_test_not_run",
        'if outputs[1]["hostile_mutations_rejected"] <= 0:\n        raise BuildError("build-children static self-test rejected no hostiles")',
        'if outputs[1]["hostile_mutations_rejected"] < 0:\n        raise BuildError("build-children static self-test rejected no hostiles")',
        "builder_static_authority",
    )
    reject_builder(
        "builder_final_manifest_accepts_child_placeholders",
        "final_tools_path, allow_child_placeholders=False",
        "final_tools_path, allow_child_placeholders=True",
        "builder_contract",
    )
    reject_builder("builder_no_final_lock_revalidation", "if validated_locks_after != validated_locks_before:", "if False:", "builder_contract")
    reject_builder(
        "builder_no_final_authority_revalidation",
        "if validated_authority_after != validated_authority_before:",
        "if False:",
        "builder_contract",
    )
    reject_builder("builder_writable_output", "make_read_only(output)", "pass  # hostile writable output", "builder_contract")
    reject_builder(
        "builder_authority_error_not_normalized",
        "RuntimeError,",
        "ArithmeticError,",
        "builder_contract",
    )

    reject_correctness("smoke_argv_broadened", 'if arguments == ["--smoke"] {', 'if arguments.first().map(String::as_str) == Some("--smoke") {', "correctness_smoke")
    reject_correctness("smoke_wrong_target", '("smoke_target", json_string("correctness"))', '("smoke_target", json_string("fault"))', "correctness_smoke")
    reject_correctness("smoke_case_omitted", '("same-stream-exact-race", "correctness"),', '("same-stream-race", "correctness"),', "correctness_cases")
    reject_correctness("smoke_measurement_output", "fn emit_smoke_result() {", "fn emit_smoke_result() { // allocated_bytes", "correctness_smoke_output")
    reject_correctness("smoke_output_bypass", "match invocation {", "return match invocation {", "correctness_control")

    mutated_patch = replace_once(
        patch,
        "+#[cfg(all(test, not(miri), not(asterism_rebaseline_correctness)))]",
        "+#[cfg(all(test, not(miri)))]",
    )
    passed.append(expect_rejection("overlay_append_gate_enabled", "overlay_negative_guards", lambda: validate_overlay(mutated_patch, overlay_validator)))
    mutated_validator = replace_once(
        overlay_validator,
        '"missing_seal_skip_negative_guard"',
        '"missing_other_guard"',
    )
    passed.append(expect_rejection("overlay_hostile_guard_missing", "overlay_validator", lambda: validate_overlay(patch, mutated_validator)))
    return passed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    builder = BUILDER.read_text()
    wrapper = WRAPPER.read_text()
    correctness = CORRECTNESS.read_text()
    patch = OVERLAY.read_text()
    overlay_validator = OVERLAY_VALIDATOR.read_text()
    if args.self_test:
        hostile = self_test(builder, wrapper, correctness, patch, overlay_validator)
        checks = validate_all(builder, wrapper, correctness, patch, overlay_validator)
        rejected = len(hostile) - 1
    else:
        checks = validate_all(builder, wrapper, correctness, patch, overlay_validator)
        rejected = 0
    print(
        canonical_bytes(
            {
                "checks": checks,
                "hostile_mutations_rejected": rejected,
                "schema": SCHEMA,
                "status": "ok",
            }
        ).decode(),
        end="",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
