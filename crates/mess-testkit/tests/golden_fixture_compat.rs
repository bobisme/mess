//! Acceptance test 2 (bn-3ui): the generated fixture-compat golden test.
//!
//! Bytes written by older code must decode *forever*. The committed fixture
//! pins old payloads; the golden decodes those exact bytes with the current
//! type. Renaming a struct field without an upcaster breaks the old payload
//! (msgpack-named keys by field name), and the check fails loudly instead of
//! silently mis-decoding at runtime (see the `spikes/codec_bakeoff` evolution
//! matrix: a rename with no upcaster is a loud decode error).
//!
//! The happy path (committed bytes still decode under `AccountEvent`) stays
//! green; `rename_without_upcaster_fails_fixture_compat` drives the failure
//! path directly and asserts the actionable error.

use mess_derive::Event;
use mess_testkit::{FixtureError, assert_fixture_compat};

/// The event as originally written. The committed fixture pins its bytes.
#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "account", version = 1)]
enum AccountEvent {
    Opened { owner: String },
    Deposited { amount: i64 },
    Withdrawn { amount: i64 },
}

/// A later refactor renames `owner` -> `account_owner` with **no upcaster**.
/// The wire name is unchanged (`account.opened`), but old committed payloads
/// carry the map key `"owner"`, which has no home in `{ account_owner }`.
#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "account", version = 1)]
enum AccountEventRenamed {
    Opened { account_owner: String },
    Deposited { amount: i64 },
    Withdrawn { amount: i64 },
}

fn samples() -> Vec<AccountEvent> {
    vec![
        AccountEvent::Opened { owner: "alice".into() },
        AccountEvent::Deposited { amount: 100 },
        AccountEvent::Withdrawn { amount: 40 },
    ]
}

const COMPAT_FIXTURE: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/account.compat");

// The generated golden: committed old bytes still decode under the current
// `AccountEvent`. Stays green. This is the *only* writer of COMPAT_FIXTURE
// under `UPDATE_FIXTURES` — every other test here is read-only
// (`assert_fixture_compat`) so regeneration can never be raced or clobbered by
// an off-nominal sample.
mess_testkit::fixture_compat_golden!(
    account_event_compat_golden,
    AccountEvent,
    COMPAT_FIXTURE,
    samples(),
);

/// Happy path proven directly: the committed bytes decode and still mean the
/// same thing under the unchanged type. Read-only, so it never regenerates.
#[test]
fn fixture_compat_passes_against_committed_bytes() {
    assert_fixture_compat::<AccountEvent>(COMPAT_FIXTURE, &samples())
        .expect("committed old bytes must still decode under AccountEvent");
}

/// The failure path: feed the committed `owner` bytes to the field-renamed
/// type. The old payload's `"owner"` key no longer maps to `account_owner`, so
/// decode fails loudly with the actionable "add an upcaster" message.
#[test]
fn rename_without_upcaster_fails_fixture_compat() {
    // Live samples of the renamed type — same wire names, so the label set
    // still matches the committed fixture and the check reaches the decode
    // step against the *committed* (old-shape) bytes.
    let renamed = vec![
        AccountEventRenamed::Opened { account_owner: "alice".into() },
        AccountEventRenamed::Deposited { amount: 100 },
        AccountEventRenamed::Withdrawn { amount: 40 },
    ];

    // Read-only: this must exercise the decode failure against the *committed*
    // old bytes, never (under `UPDATE_FIXTURES`) overwrite them with the
    // renamed-shape sample.
    let err = assert_fixture_compat::<AccountEventRenamed>(COMPAT_FIXTURE, &renamed)
        .expect_err("renaming a field with no upcaster must break old bytes");

    assert!(
        matches!(err, FixtureError::CompatDecodeFailed { ref wire_name, .. }
            if wire_name == "account.opened"),
        "expected CompatDecodeFailed on account.opened, got {err:?}"
    );
    let msg = err.to_string();
    assert!(msg.contains("FIXTURE-COMPAT BROKEN"), "message was: {msg}");
    assert!(msg.contains("upcaster"), "message was: {msg}");
}
