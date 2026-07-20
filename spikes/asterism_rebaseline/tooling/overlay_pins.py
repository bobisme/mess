#!/usr/bin/env python3
"""Exact raw-byte pins for shared Asterism overlay sources."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable


PINNED_SHARED_OVERLAY_SHA256 = {
    "allocation.rs": "9a84a7be0f8feb23c8b94e393a0b6c29d7056a213d5ab312376026a5a35ab21f",
    "contract.rs": "30a3c3c260686690ee5fa0e2b00ea2a866ef0f3069615dac706397a40db99c12",
    "control.rs": "1abd791f7bb9cd3d32e9fbf4c0bdb2172af891e0aa1c9ca402dd2c9e965d6d8f",
    "digest.rs": "fbddf7563ce01012585d9858f58d47ab9bdf9bd78fec45104c4d9dfac02599ef",
    "schema.rs": "91676e539b32347466b423405536e49745e8dd307defdd9464de8f7a83127445",
    "semantic_oracle.rs": "4484a2e053ed081b7ed49d8614e86cef979d2f3031d0d49d8848f38daed2f3be",
    "timing.rs": "9e234a7240818b7740f3c3d5233dcafe31ac506ca617eb010909d06dc06458af",
    "workload.rs": "9fe6fbbbefc3d4ccf72cfa877162ab5bd130f7e14efcd85ff8c96883be13e535",
}


def validate_pinned_shared_overlay_set(
    names: Iterable[str], *, error_type: type[Exception] = RuntimeError
) -> None:
    """Require the complete pinned shared-overlay source set."""

    if set(names) != set(PINNED_SHARED_OVERLAY_SHA256):
        raise error_type("shared overlay source set differs from pinned set")


def validate_pinned_shared_overlay_payload(
    name: str,
    payload: bytes,
    expected_sha256: str | None = None,
    *,
    error_type: type[Exception] = RuntimeError,
) -> None:
    """Require one exact raw shared-overlay payload."""

    expected = (
        PINNED_SHARED_OVERLAY_SHA256.get(name)
        if expected_sha256 is None
        else expected_sha256
    )
    if expected is None or hashlib.sha256(payload).hexdigest() != expected:
        raise error_type(
            f"shared overlay source hash differs from pinned value: {name}; "
            "if this edit is intentional, update PINNED_SHARED_OVERLAY_SHA256 "
            "in the same commit — both changes will appear in the same review"
        )
