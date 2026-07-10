//! Acceptance test 1 (bn-3ui): the generated fold-drift golden test.
//!
//! A pinned fixture of events folds to a pinned expected state. Changing
//! `apply` semantics without bumping `#[aggregate(fold_version = N)]` trips
//! the golden with a "bump `fold_version` or fix your fold" message (D4; spec
//! `05-fold-certificates.md` §9).
//!
//! Two triggers are wired here:
//!   - `--features drift` flips the aggregate's `apply` (mirrors the
//!     `spikes/fold_cert` cfg-flagged demo), so the generated golden below
//!     FAILS with the bump message. The default suite stays green.
//!   - `fold_drift_failure_path_reports_bump_message` drives the check function
//!     *directly* against a deliberately-drifted fold and asserts the
//!     actionable error — so the failure path is proven in the default config
//!     that CI runs.

use mess_derive::{Aggregate, Event};
use mess_testkit::{FixtureError, assert_fold_drift, fold};

#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "account", version = 1)]
enum AccountEvent {
    Opened { owner: String },
    Deposited { amount: i64 },
    Withdrawn { amount: i64 },
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Aggregate)]
#[aggregate(event = AccountEvent, fold_version = 1)]
struct Account {
    open:        bool,
    balance:     i64,
    withdrawals: u32,
}

impl Account {
    fn apply(&mut self, event: &AccountEvent) {
        match event {
            AccountEvent::Opened { .. } => self.open = true,
            AccountEvent::Deposited { amount } => self.balance += amount,
            AccountEvent::Withdrawn { amount } => {
                self.balance -= amount;
                self.withdrawals += 1;
                // Deliberate cfg-flagged semantic drift (mirrors
                // spikes/fold_cert): withdrawals now also charge a fee. Same
                // fold_version -> the golden below fails.
                #[cfg(feature = "drift")]
                {
                    self.balance -= 1;
                }
            }
        }
    }
}

fn fixture_events() -> Vec<AccountEvent> {
    vec![
        AccountEvent::Opened { owner: "alice".into() },
        AccountEvent::Deposited { amount: 100 },
        AccountEvent::Deposited { amount: 50 },
        AccountEvent::Withdrawn { amount: 30 },
        AccountEvent::Withdrawn { amount: 20 },
        AccountEvent::Deposited { amount: 7 },
    ]
}

const FOLD_FIXTURE: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/account.fold");

// The generated golden. Passes in the default config against the committed
// fixture; with `--features drift` the fold changes and this FAILS with the
// bump-or-fix message. This is the *only* writer of FOLD_FIXTURE under
// `UPDATE_FIXTURES` — every other test here is read-only (`assert_fold_drift`)
// so regeneration can never be raced or clobbered by a drifted / bumped state.
mess_testkit::fold_drift_golden!(
    account_fold_drift_golden,
    Account,
    FOLD_FIXTURE,
    fixture_events(),
);

/// The failure path, proven directly and unconditionally (runs in the default
/// config CI uses): fold the same fixture events with a *drifted* fold and
/// assert the check reports the actionable bump message.
#[test]
fn fold_drift_failure_path_reports_bump_message() {
    let events = fixture_events();
    // A hand-drifted fold: identical to `Account::apply` except a withdrawal
    // fee is charged — i.e. an edited apply() with no fold_version bump.
    let mut drifted = Account::default();
    for e in &events {
        match e {
            AccountEvent::Opened { .. } => drifted.open = true,
            AccountEvent::Deposited { amount } => drifted.balance += amount,
            AccountEvent::Withdrawn { amount } => {
                drifted.balance -= amount;
                drifted.withdrawals += 1;
                drifted.balance -= 1; // the drift
            }
        }
    }

    // Read-only: never regenerate FOLD_FIXTURE with the drifted state.
    let err = assert_fold_drift(
        "Account",
        FOLD_FIXTURE,
        Account::FOLD_VERSION,
        &format!("{drifted:?}"),
    )
    .expect_err("a drifted fold must not match the pinned fixture");

    assert!(
        matches!(err, FixtureError::FoldDrift { .. }),
        "expected FoldDrift, got {err:?}"
    );
    let msg = err.to_string();
    assert!(msg.contains("FOLD DRIFT DETECTED"), "message was: {msg}");
    assert!(msg.contains("bump"), "message was: {msg}");
    assert!(msg.contains("fold_version"), "message was: {msg}");
}

/// The happy path, proven directly (default config only — under
/// `--features drift` the generated golden above already covers the failure).
#[cfg(not(feature = "drift"))]
#[test]
fn fold_drift_golden_matches_pinned_state() {
    let state = fold::<Account>(&fixture_events());
    // Read-only re-proof of the happy path; the macro golden above owns
    // regeneration of FOLD_FIXTURE.
    assert_fold_drift(
        "Account",
        FOLD_FIXTURE,
        Account::FOLD_VERSION,
        &format!("{state:?}"),
    )
    .expect("pinned fixture must still match the current fold");
}

/// A legitimate `fold_version` bump must regenerate the fixture, not silently
/// reuse the stale one: a version mismatch is its own actionable error.
#[test]
fn fold_version_bump_requires_regeneration() {
    let state = fold::<Account>(&fixture_events());
    // Read-only: never regenerate FOLD_FIXTURE pinned to the fake version 999.
    let err = assert_fold_drift(
        "Account",
        FOLD_FIXTURE,
        999, // pretend the aggregate bumped to fold_version = 999
        &format!("{state:?}"),
    )
    .expect_err("a version mismatch must be surfaced");
    assert!(
        matches!(
            err,
            FixtureError::FoldVersionChanged { pinned: 1, current: 999, .. }
        ),
        "got {err:?}"
    );
    assert!(err.to_string().contains("regenerate"), "{err}");
}
