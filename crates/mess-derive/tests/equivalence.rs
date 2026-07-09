//! Behavioral equivalence: the `spikes/dx_api` bank-account example
//! rewritten with `#[derive(Event)]` / `#[derive(Aggregate)]` only, proven
//! to behave identically to the hand-written ground truth on the same
//! fixtures — encode/decode round-trips, wire-name stability, the
//! variant-reorder property, and the given-when-then decide/apply rules.
//!
//! The store-backed halves of the spike's suite (RocksDB via `mess_db`)
//! are out of scope here: this crate deliberately never depends on a
//! backend. What is exercised is exactly the surface the derives generate.

use mess_core::{Decide, Event};
use mess_derive::{Aggregate, Event};

// ---------------------------------------------------------------------------
// Domain rewritten with derives only. Compare against
// spikes/dx_api/tests/bank_account.rs: every `impl Event` / `impl Aggregate`
// block there is replaced by a derive here.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "account", version = 1)]
enum AccountEvent {
    Opened { owner: String },
    Deposited { amount: i64 },
    Withdrawn { amount: i64 },
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Aggregate)]
#[aggregate(event = AccountEvent)]
struct Account {
    open: bool,
    balance: i64,
}

impl Account {
    fn apply(&mut self, event: &AccountEvent) {
        match event {
            AccountEvent::Opened { .. } => self.open = true,
            AccountEvent::Deposited { amount } => self.balance += amount,
            AccountEvent::Withdrawn { amount } => self.balance -= amount,
        }
    }
}

/// The aggregate's own rejection type — the production replacement for the
/// spike's stringly `DomainError`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Rejected(String);

impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "command rejected: {}", self.0)
    }
}

impl std::error::Error for Rejected {}

#[derive(Debug, Clone)]
struct Open {
    owner: String,
}

#[derive(Debug, Clone, Copy)]
struct Deposit {
    amount: i64,
}

#[derive(Debug, Clone, Copy)]
struct Withdraw {
    amount: i64,
}

impl Decide<Open> for Account {
    type Rejection = Rejected;

    fn decide(&self, cmd: Open) -> Result<Vec<AccountEvent>, Rejected> {
        if self.open {
            return Err(Rejected("account is already open".into()));
        }
        Ok(vec![AccountEvent::Opened { owner: cmd.owner }])
    }
}

impl Decide<Deposit> for Account {
    type Rejection = Rejected;

    fn decide(&self, cmd: Deposit) -> Result<Vec<AccountEvent>, Rejected> {
        if !self.open {
            return Err(Rejected("account is not open".into()));
        }
        if cmd.amount <= 0 {
            return Err(Rejected("deposit must be positive".into()));
        }
        Ok(vec![AccountEvent::Deposited { amount: cmd.amount }])
    }
}

impl Decide<Withdraw> for Account {
    type Rejection = Rejected;

    fn decide(&self, cmd: Withdraw) -> Result<Vec<AccountEvent>, Rejected> {
        if !self.open {
            return Err(Rejected("account is not open".into()));
        }
        if cmd.amount > self.balance {
            return Err(Rejected(format!(
                "insufficient funds: balance is {}, requested {}",
                self.balance, cmd.amount
            )));
        }
        Ok(vec![AccountEvent::Withdrawn { amount: cmd.amount }])
    }
}

// A tiny in-memory given-when-then kit, mirroring the spike's testkit but
// over the production `Decide::Rejection` seam.
fn replay(events: &[AccountEvent]) -> Account {
    let mut state = Account::default();
    for e in events {
        state.apply(e);
    }
    state
}

// ---------------------------------------------------------------------------
// Wire-name equivalence: the derive must produce exactly the hand-written
// names from the spike.
// ---------------------------------------------------------------------------

#[test]
fn wire_names_match_ground_truth() {
    assert_eq!(
        AccountEvent::Opened { owner: "a".into() }.name(),
        "account.opened"
    );
    assert_eq!(AccountEvent::Deposited { amount: 1 }.name(), "account.deposited");
    assert_eq!(AccountEvent::Withdrawn { amount: 1 }.name(), "account.withdrawn");

    assert_eq!(
        AccountEvent::EVENT_NAMES,
        ["account.opened", "account.deposited", "account.withdrawn"]
    );
    assert_eq!(AccountEvent::SCHEMA_VERSION, 1);
    assert_eq!(AccountEvent::EVENT_NAME_PREFIX, "account");
}

// ---------------------------------------------------------------------------
// Encode / decode round-trips through the codec layer.
// ---------------------------------------------------------------------------

#[test]
fn encode_decode_round_trips_every_variant() {
    let events = [
        AccountEvent::Opened { owner: "alice".into() },
        AccountEvent::Deposited { amount: 100 },
        AccountEvent::Withdrawn { amount: 40 },
    ];
    for ev in &events {
        let bytes = ev.encode().unwrap();
        let back = AccountEvent::decode(ev.name(), &bytes).unwrap();
        assert_eq!(&back, ev);
    }
}

#[test]
fn decode_rejects_unknown_name() {
    let bytes = AccountEvent::Opened { owner: "alice".into() }.encode().unwrap();
    let err = AccountEvent::decode("account.frobnicated", &bytes).unwrap_err();
    assert!(
        matches!(err, mess_core::CodecError::UnknownEventName(ref n) if n == "account.frobnicated"),
        "got: {err:?}"
    );
}

#[test]
fn payload_carries_only_variant_fields_not_the_enum_tag() {
    // The manual spike impl serialized the whole tagged enum; the derive
    // serializes only the variant's fields. Prove the payload does NOT
    // embed the Rust variant name "Opened" — dispatch lives entirely in the
    // stored wire name.
    let bytes = AccountEvent::Opened { owner: "alice".into() }.encode().unwrap();
    let as_text = String::from_utf8_lossy(&bytes);
    assert!(
        !as_text.contains("Opened"),
        "payload leaked the variant tag: {as_text:?}"
    );
    assert!(as_text.contains("owner"), "expected the field name in payload");
}

// ---------------------------------------------------------------------------
// THE reorder property: wire names are stable under variant reorder.
//
// Two enums with identical variants declared in a DIFFERENT order both carry
// `#[event(name = "account")]`. Bytes written by one decode correctly under
// the other because dispatch keys on the stored name string, never on the
// variant index or serde's internal tagging. An index/position-keyed codec
// would silently swap variants here.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "account", version = 1)]
enum AccountEventReordered {
    // Deliberately reversed relative to `AccountEvent`.
    Withdrawn { amount: i64 },
    Deposited { amount: i64 },
    Opened { owner: String },
}

#[test]
fn wire_names_stable_under_variant_reorder() {
    let original = [
        AccountEvent::Opened { owner: "alice".into() },
        AccountEvent::Deposited { amount: 100 },
        AccountEvent::Withdrawn { amount: 40 },
    ];
    let expected = [
        AccountEventReordered::Opened { owner: "alice".into() },
        AccountEventReordered::Deposited { amount: 100 },
        AccountEventReordered::Withdrawn { amount: 40 },
    ];

    for (orig, want) in original.iter().zip(expected.iter()) {
        // Names are identical across the two orderings.
        assert_eq!(orig.name(), want.name());
        // Bytes from the original enum decode to the semantically equal
        // variant of the reordered enum — no index-based swap.
        let bytes = orig.encode().unwrap();
        let got =
            AccountEventReordered::decode(orig.name(), &bytes).unwrap();
        assert_eq!(&got, want);
    }
}

// ---------------------------------------------------------------------------
// Given-when-then: the aggregate rules behave like the spike's.
// ---------------------------------------------------------------------------

#[test]
fn gwt_open_emits_opened() {
    let state = replay(&[]);
    assert_eq!(
        state.decide(Open { owner: "alice".into() }).unwrap(),
        [AccountEvent::Opened { owner: "alice".into() }]
    );
}

#[test]
fn gwt_cannot_open_twice() {
    let state = replay(&[AccountEvent::Opened { owner: "alice".into() }]);
    assert_eq!(
        state.decide(Open { owner: "bob".into() }).unwrap_err(),
        Rejected("account is already open".into())
    );
}

#[test]
fn gwt_deposit_emits_deposited() {
    let state = replay(&[AccountEvent::Opened { owner: "alice".into() }]);
    assert_eq!(
        state.decide(Deposit { amount: 100 }).unwrap(),
        [AccountEvent::Deposited { amount: 100 }]
    );
}

#[test]
fn gwt_withdraw_within_balance() {
    let state = replay(&[
        AccountEvent::Opened { owner: "alice".into() },
        AccountEvent::Deposited { amount: 100 },
    ]);
    assert_eq!(
        state.decide(Withdraw { amount: 40 }).unwrap(),
        [AccountEvent::Withdrawn { amount: 40 }]
    );
}

#[test]
fn gwt_overdraw_is_rejected() {
    let state = replay(&[
        AccountEvent::Opened { owner: "alice".into() },
        AccountEvent::Deposited { amount: 50 },
        AccountEvent::Withdrawn { amount: 20 },
    ]);
    assert_eq!(
        state.decide(Withdraw { amount: 100 }).unwrap_err(),
        Rejected("insufficient funds: balance is 30, requested 100".into())
    );
}

#[test]
fn gwt_cannot_deposit_before_open() {
    let state = replay(&[]);
    assert_eq!(
        state.decide(Deposit { amount: 10 }).unwrap_err(),
        Rejected("account is not open".into())
    );
}

#[test]
fn full_apply_fold_matches_expected_state() {
    let state = replay(&[
        AccountEvent::Opened { owner: "alice".into() },
        AccountEvent::Deposited { amount: 100 },
        AccountEvent::Deposited { amount: 50 },
        AccountEvent::Withdrawn { amount: 30 },
    ]);
    assert_eq!(state, Account { open: true, balance: 120 });
}

// ---------------------------------------------------------------------------
// Schema fingerprint: per-variant, stable, and shape-sensitive.
// ---------------------------------------------------------------------------

#[test]
fn schema_fingerprint_is_per_variant_and_stable() {
    let opened = AccountEvent::Opened { owner: "a".into() };
    let deposited = AccountEvent::Deposited { amount: 1 };
    let withdrawn = AccountEvent::Withdrawn { amount: 1 };

    // Distinct per variant.
    assert_ne!(opened.schema_fingerprint(), deposited.schema_fingerprint());
    assert_ne!(deposited.schema_fingerprint(), withdrawn.schema_fingerprint());

    // Independent of the field values.
    assert_eq!(
        opened.schema_fingerprint(),
        AccountEvent::Opened { owner: "zzz".into() }.schema_fingerprint()
    );

    // Stable across variant reorder: same name + version + shape.
    assert_eq!(
        opened.schema_fingerprint(),
        AccountEventReordered::Opened { owner: "a".into() }.schema_fingerprint()
    );
}

// A unit / tuple variant enum to prove those variant shapes also derive.
// Note: the enum itself is NOT serde-(de)serializable — only its fields
// need to be, since the derive serializes per-variant payloads.
#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "session", version = 2)]
enum SessionEvent {
    Started,
    Renewed(u64),
    Ended { reason: String },
}

#[test]
fn unit_and_tuple_variants_round_trip() {
    for ev in [
        SessionEvent::Started,
        SessionEvent::Renewed(42),
        SessionEvent::Ended { reason: "logout".into() },
    ] {
        let bytes = ev.encode().unwrap();
        let back = SessionEvent::decode(ev.name(), &bytes).unwrap();
        assert_eq!(back, ev);
    }
    assert_eq!(SessionEvent::Started.name(), "session.started");
    assert_eq!(SessionEvent::Renewed(1).name(), "session.renewed");
    assert_eq!(
        SessionEvent::Ended { reason: "x".into() }.name(),
        "session.ended"
    );
    assert_eq!(SessionEvent::SCHEMA_VERSION, 2);
}
