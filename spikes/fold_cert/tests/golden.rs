//! D4 fold-drift golden test prototype.
//!
//! In the real system, `#[aggregate(fold_version = N)]` GENERATES this test:
//! fixture events + expected folded state committed to the repo. If apply()
//! semantics change while fold_version stays the same, the test fails with a
//! message telling the human to bump fold_version (or fix the fold).
//!
//! Demonstration:
//!   cargo test --test golden                    -> passes (FOLD_VERSION 1 OK)
//!   cargo test --test golden --features drift   -> FAILS with the bump message
//! (`drift` simulates a developer editing apply() to charge a withdrawal fee
//! without bumping FOLD_VERSION.)

use fold_cert::*;

/// Golden fixture pinned when FOLD_VERSION was set to 1. NEVER regenerate
/// these constants without bumping FOLD_VERSION — that is the whole point.
const PINNED_FOLD_VERSION: u32 = 1;

#[test]
fn fold_drift_golden() {
    assert_eq!(
        Account::FOLD_VERSION, PINNED_FOLD_VERSION,
        "FOLD_VERSION changed: regenerate the golden fixture constants \
         (new fixtures pinned to the new version) — this is the legitimate path."
    );

    let mut state = Account::init();
    for e in golden_fixture_events() {
        state.apply(&e);
    }

    let drifted = state.balance != GOLDEN_EXPECTED_BALANCE
        || state.tx_count != GOLDEN_EXPECTED_TX_COUNT
        || hex(&blob_hash(&state.to_bytes())) != GOLDEN_EXPECTED_STATE_HASH;

    assert!(
        !drifted,
        "FOLD DRIFT DETECTED for Account (fold_version = {v}):\n\
         fixture fold produced balance={b} tx_count={t} state_hash={h}\n\
         expected                balance={eb} tx_count={et} state_hash={eh}\n\
         \n\
         apply() semantics changed but fold_version did not.\n\
         => bump `FOLD_VERSION` (invalidating existing snapshots) or fix your fold.",
        v = Account::FOLD_VERSION,
        b = state.balance,
        t = state.tx_count,
        h = hex(&blob_hash(&state.to_bytes())),
        eb = GOLDEN_EXPECTED_BALANCE,
        et = GOLDEN_EXPECTED_TX_COUNT,
        eh = GOLDEN_EXPECTED_STATE_HASH,
    );
}

fn hex(h: &Hash) -> String {
    h.iter().map(|b| format!("{b:02x}")).collect()
}
