#!/asterism/python3
"""Fail-closed cfg(test) injection for the current correctness/fault children.

Cargo invokes this file through ``RUSTC_WORKSPACE_WRAPPER``.  Every workspace
rustc invocation except the exact ``mess_store`` library unit is passed to the
pinned compiler unchanged.  The one selected unit receives the reviewed
cfg(test)-only child marker and atomically publishes a single-use receipt.

This wrapper is never valid for a performance or release build.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence


SCHEMA = "bn-30fs-rustc-workspace-wrapper-receipt-v1"
TARGET_CRATE = "mess_store"
TARGET_PACKAGE = "mess-store"
TARGET_CRATE_TYPE = "lib"
CORRECTNESS_CFG = "asterism_rebaseline_correctness"
INJECTED_ARGUMENTS = (
    "--cfg",
    "test",
    "--allow",
    "explicit_builtin_cfgs_in_flags",
    "--cfg",
    CORRECTNESS_CFG,
    "--check-cfg",
    f"cfg({CORRECTNESS_CFG})",
)
PINNED_RUSTC_ENV = "ASTERISM_REBASELINE_PINNED_RUSTC"
EXPECTED_SOURCE_ENV = "ASTERISM_REBASELINE_EXPECTED_LIB_SOURCE"
RECEIPT_ENV = "ASTERISM_REBASELINE_WRAPPER_RECEIPT"
BUILD_NONCE_ENV = "ASTERISM_REBASELINE_CHILD_BUILD_NONCE"


class WrapperError(RuntimeError):
    """The Cargo/rustc invocation differs from the reviewed child contract."""


@dataclass(frozen=True)
class InvocationPlan:
    rustc: str
    arguments: tuple[str, ...]
    inject: bool
    receipt: dict[str, object] | None


def canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode()


def lower_hex(value: str, length: int) -> bool:
    return len(value) == length and all(character in "0123456789abcdef" for character in value)


def argument_values(arguments: Sequence[str], option: str) -> tuple[str, ...]:
    """Return exact values for ``--option value`` and ``--option=value``."""

    values: list[str] = []
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        if argument == option:
            if index + 1 >= len(arguments):
                raise WrapperError(f"missing value after {option}")
            values.append(arguments[index + 1])
            index += 2
            continue
        prefix = f"{option}="
        if argument.startswith(prefix):
            value = argument[len(prefix) :]
            if not value:
                raise WrapperError(f"empty value for {option}")
            values.append(value)
        index += 1
    return tuple(values)


def source_arguments(arguments: Sequence[str]) -> tuple[str, ...]:
    """Locate Rust input paths without accepting an ambiguous target source."""

    sources = []
    for argument in arguments:
        if argument.endswith(".rs") and not argument.startswith("--"):
            sources.append(argument)
    return tuple(sources)


def plan_invocation(
    argv: Sequence[str], environment: Mapping[str, str]
) -> InvocationPlan:
    if len(argv) < 2:
        raise WrapperError("wrapper requires a rustc path and arguments")
    rustc = argv[0]
    arguments = tuple(argv[1:])
    pinned_rustc = environment.get(PINNED_RUSTC_ENV)
    if not pinned_rustc or not Path(pinned_rustc).is_absolute():
        raise WrapperError("pinned rustc path is absent or not absolute")
    if rustc != pinned_rustc:
        raise WrapperError("Cargo supplied a rustc path different from the pinned compiler")

    crate_names = argument_values(arguments, "--crate-name")
    crate_types = argument_values(arguments, "--crate-type")
    sources = source_arguments(arguments)
    expected_source = environment.get(EXPECTED_SOURCE_ENV)
    if not expected_source or Path(expected_source).is_absolute():
        raise WrapperError("expected library source must be one canonical relative path")

    names_target = TARGET_CRATE in crate_names
    sources_target = expected_source in sources
    if names_target != sources_target:
        raise WrapperError("mess_store crate/source selection is ambiguous")
    if not names_target:
        return InvocationPlan(rustc, arguments, False, None)

    if crate_names != (TARGET_CRATE,):
        raise WrapperError("target crate-name cardinality differs")
    if sources != (expected_source,):
        raise WrapperError("target source cardinality or spelling differs")
    if crate_types != (TARGET_CRATE_TYPE,):
        raise WrapperError("target crate type is not the exact library unit")
    if "--test" in arguments:
        raise WrapperError("target library unexpectedly uses a test harness")
    if environment.get("CARGO_PKG_NAME") != TARGET_PACKAGE:
        raise WrapperError("target Cargo package identity differs")
    cfg_values = argument_values(arguments, "--cfg")
    if "test" in cfg_values or CORRECTNESS_CFG in cfg_values:
        raise WrapperError("target invocation already contains a child cfg")

    receipt_path = environment.get(RECEIPT_ENV)
    if not receipt_path or not Path(receipt_path).is_absolute():
        raise WrapperError("wrapper receipt path is absent or not absolute")
    nonce = environment.get(BUILD_NONCE_ENV, "")
    if not lower_hex(nonce, 64):
        raise WrapperError("child build nonce is not 64 lowercase hex characters")
    original = (rustc, *arguments)
    receipt: dict[str, object] = {
        "build_nonce": nonce,
        "crate_name": TARGET_CRATE,
        "crate_type": TARGET_CRATE_TYPE,
        "injected_arguments": list(INJECTED_ARGUMENTS),
        "original_argv_sha256": hashlib.sha256(
            canonical_bytes(list(original))
        ).hexdigest(),
        "package": TARGET_PACKAGE,
        "rustc": rustc,
        "schema": SCHEMA,
        "source": expected_source,
    }
    return InvocationPlan(
        rustc,
        (*arguments, *INJECTED_ARGUMENTS),
        True,
        receipt,
    )


def write_receipt(path: Path, receipt: dict[str, object]) -> None:
    if not path.is_absolute() or path.parent.resolve(strict=True) != path.parent:
        raise WrapperError("wrapper receipt parent is not one exact directory")
    descriptor = os.open(
        path,
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0),
        0o444,
    )
    try:
        payload = canonical_bytes(receipt)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise WrapperError("wrapper receipt write made no progress")
            view = view[written:]
        os.fsync(descriptor)
        os.fchmod(descriptor, 0o444)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o444
        ):
            raise WrapperError("wrapper receipt is not one exact 0444 regular file")
    finally:
        os.close(descriptor)


def main() -> int:
    try:
        plan = plan_invocation(sys.argv[1:], os.environ)
        if plan.inject:
            assert plan.receipt is not None
            write_receipt(Path(os.environ[RECEIPT_ENV]), plan.receipt)
        os.execv(plan.rustc, (plan.rustc, *plan.arguments))
    except (OSError, WrapperError, ValueError) as error:
        print(f"rustc-workspace-wrapper: {error}", file=sys.stderr)
        return 86
    raise AssertionError("os.execv returned")


if __name__ == "__main__":
    raise SystemExit(main())
