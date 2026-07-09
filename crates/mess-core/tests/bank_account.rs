//! Acceptance test: the bank-account example from the north star, ported from
//! `spikes/dx_api/tests/bank_account.rs` onto `mess-core`'s production traits.
//!
//! Every impl below is what `#[derive(Event)]` / `#[derive(Aggregate)]`
//! (bn-hy7) would generate. The only substantive change from the spike is that
//! the domain rejection is now a *typed* enum ([`AccountError`]) named through
//! [`Decide::Rejection`], instead of the spike's stringly-typed `DomainError`.
//!
//! The store-backed half of the spike test (real RocksDB via the `mess_db`
//! actor) is intentionally *not* ported: `mess-core` has no backend
//! dependency. This exercises the pure vocabulary — `Event` codec round-trips,
//! infallible `apply`, and typed `decide` — through a small given/when/then
//! harness.

use std::fmt;

use mess_core::{Aggregate, CodecError, Decide, Event};

// ---------------------------------------------------------------------------
// Domain: what a user of the library writes today (all manual).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum AccountEvent {
    Opened { owner: String },
    Deposited { amount: i64 },
    Withdrawn { amount: i64 },
}

impl Event for AccountEvent {
    fn name(&self) -> &'static str {
        match self {
            AccountEvent::Opened { .. } => "account.opened",
            AccountEvent::Deposited { .. } => "account.deposited",
            AccountEvent::Withdrawn { .. } => "account.withdrawn",
        }
    }

    // A hand-rolled wire codec keeps the crate dependency-free. The variant is
    // carried by `name()`, so the payload holds only the fields. A real event
    // would use the registry codec (docs/spec/04-registry.md); the shape of
    // the trait is identical either way.
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(match self {
            AccountEvent::Opened { owner } => owner.as_bytes().to_vec(),
            AccountEvent::Deposited { amount }
            | AccountEvent::Withdrawn { amount } => {
                amount.to_le_bytes().to_vec()
            }
        })
    }

    fn decode(name: &str, data: &[u8]) -> Result<Self, CodecError> {
        let decode_amount = |bytes: &[u8]| -> Result<i64, CodecError> {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| {
                CodecError::Decode {
                    event_name: name.to_string(),
                    source: "expected 8 payload bytes".to_string(),
                }
            })?;
            Ok(i64::from_le_bytes(arr))
        };
        match name {
            "account.opened" => {
                let owner =
                    String::from_utf8(data.to_vec()).map_err(|e| {
                        CodecError::Decode {
                            event_name: name.to_string(),
                            source: e.to_string(),
                        }
                    })?;
                Ok(AccountEvent::Opened { owner })
            }
            "account.deposited" => {
                Ok(AccountEvent::Deposited { amount: decode_amount(data)? })
            }
            "account.withdrawn" => {
                Ok(AccountEvent::Withdrawn { amount: decode_amount(data)? })
            }
            other => Err(CodecError::UnknownEventName(other.to_string())),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Account {
    open: bool,
    balance: i64,
}

impl Aggregate for Account {
    type Event = AccountEvent;

    fn apply(&mut self, event: &AccountEvent) {
        match event {
            AccountEvent::Opened { .. } => self.open = true,
            AccountEvent::Deposited { amount } => self.balance += amount,
            AccountEvent::Withdrawn { amount } => self.balance -= amount,
        }
    }
}

/// The aggregate's typed rejection — the production replacement for the spike's
/// stringly `DomainError`. One enum, named by every `Decide` impl below.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AccountError {
    AlreadyOpen,
    NotOpen,
    NonPositiveDeposit,
    InsufficientFunds { balance: i64, requested: i64 },
}

impl fmt::Display for AccountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AccountError::AlreadyOpen => write!(f, "account is already open"),
            AccountError::NotOpen => write!(f, "account is not open"),
            AccountError::NonPositiveDeposit => {
                write!(f, "deposit must be positive")
            }
            AccountError::InsufficientFunds { balance, requested } => write!(
                f,
                "insufficient funds: balance is {balance}, requested \
                 {requested}"
            ),
        }
    }
}

impl std::error::Error for AccountError {}

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
    type Rejection = AccountError;

    fn decide(&self, cmd: Open) -> Result<Vec<AccountEvent>, AccountError> {
        if self.open {
            return Err(AccountError::AlreadyOpen);
        }
        Ok(vec![AccountEvent::Opened { owner: cmd.owner }])
    }
}

impl Decide<Deposit> for Account {
    type Rejection = AccountError;

    fn decide(&self, cmd: Deposit) -> Result<Vec<AccountEvent>, AccountError> {
        if !self.open {
            return Err(AccountError::NotOpen);
        }
        if cmd.amount <= 0 {
            return Err(AccountError::NonPositiveDeposit);
        }
        Ok(vec![AccountEvent::Deposited { amount: cmd.amount }])
    }
}

impl Decide<Withdraw> for Account {
    type Rejection = AccountError;

    fn decide(&self, cmd: Withdraw) -> Result<Vec<AccountEvent>, AccountError> {
        if !self.open {
            return Err(AccountError::NotOpen);
        }
        if cmd.amount > self.balance {
            return Err(AccountError::InsufficientFunds {
                balance: self.balance,
                requested: cmd.amount,
            });
        }
        Ok(vec![AccountEvent::Withdrawn { amount: cmd.amount }])
    }
}

// ---------------------------------------------------------------------------
// A minimal given/when/then harness over the pure traits. (The full test kit
// is a separate crate; this is just enough to drive the acceptance test.)
// ---------------------------------------------------------------------------

struct AggregateTest<A: Aggregate> {
    given: Vec<A::Event>,
}

impl<A: Aggregate> AggregateTest<A> {
    fn given(events: impl IntoIterator<Item = A::Event>) -> Self {
        Self { given: events.into_iter().collect() }
    }

    fn when<C>(self, cmd: C) -> WhenOutcome<A, C>
    where
        A: Decide<C>,
    {
        let mut state = A::default();
        for event in &self.given {
            state.apply(event);
        }
        WhenOutcome { result: state.decide(cmd) }
    }
}

struct WhenOutcome<A, C>
where
    A: Decide<C>,
{
    result: Result<Vec<A::Event>, <A as Decide<C>>::Rejection>,
}

impl<A, C> WhenOutcome<A, C>
where
    A: Decide<C>,
    A::Event: fmt::Debug + PartialEq,
    <A as Decide<C>>::Rejection: fmt::Debug + PartialEq,
{
    #[track_caller]
    fn then_events(self, expected: impl IntoIterator<Item = A::Event>) {
        let expected: Vec<A::Event> = expected.into_iter().collect();
        match self.result {
            Ok(actual) => assert_eq!(
                actual, expected,
                "emitted events did not match\nexpected: {expected:#?}\n\
                 actual:   {actual:#?}"
            ),
            Err(err) => panic!(
                "expected the command to emit events, but it was \
                 rejected\nexpected events: {expected:#?}\nrejection:       \
                 {err:?}"
            ),
        }
    }

    #[track_caller]
    fn then_error(self, expected: <A as Decide<C>>::Rejection) {
        match self.result {
            Ok(events) => panic!(
                "expected the command to be rejected, but it emitted \
                 events\nexpected rejection: {expected:?}\nemitted events: \
                 {events:#?}"
            ),
            Err(actual) => assert_eq!(
                actual, expected,
                "command was rejected, but with the wrong error"
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// Given/when/then tests: pure in-memory, no store, no async.
// ---------------------------------------------------------------------------

#[test]
fn gwt_open_emits_opened() {
    AggregateTest::<Account>::given([])
        .when(Open { owner: "alice".into() })
        .then_events([AccountEvent::Opened { owner: "alice".into() }]);
}

#[test]
fn gwt_cannot_open_twice() {
    AggregateTest::<Account>::given([AccountEvent::Opened {
        owner: "alice".into(),
    }])
    .when(Open { owner: "bob".into() })
    .then_error(AccountError::AlreadyOpen);
}

#[test]
fn gwt_deposit_emits_deposited() {
    AggregateTest::<Account>::given([AccountEvent::Opened {
        owner: "alice".into(),
    }])
    .when(Deposit { amount: 100 })
    .then_events([AccountEvent::Deposited { amount: 100 }]);
}

#[test]
fn gwt_withdraw_within_balance() {
    AggregateTest::<Account>::given([
        AccountEvent::Opened { owner: "alice".into() },
        AccountEvent::Deposited { amount: 100 },
    ])
    .when(Withdraw { amount: 40 })
    .then_events([AccountEvent::Withdrawn { amount: 40 }]);
}

#[test]
fn gwt_overdraw_is_rejected() {
    AggregateTest::<Account>::given([
        AccountEvent::Opened { owner: "alice".into() },
        AccountEvent::Deposited { amount: 50 },
        AccountEvent::Withdrawn { amount: 20 },
    ])
    .when(Withdraw { amount: 100 })
    .then_error(AccountError::InsufficientFunds {
        balance: 30,
        requested: 100,
    });
}

#[test]
fn gwt_cannot_deposit_before_open() {
    AggregateTest::<Account>::given([])
        .when(Deposit { amount: 10 })
        .then_error(AccountError::NotOpen);
}

// ---------------------------------------------------------------------------
// Event codec round-trips through the `Event` trait.
// ---------------------------------------------------------------------------

#[test]
fn event_codec_round_trips() {
    let events = [
        AccountEvent::Opened { owner: "alice".into() },
        AccountEvent::Deposited { amount: 100 },
        AccountEvent::Withdrawn { amount: -42 },
    ];
    for event in events {
        let bytes = event.encode().unwrap();
        let back = AccountEvent::decode(event.name(), &bytes).unwrap();
        assert_eq!(back, event);
    }
}

#[test]
fn decode_rejects_unknown_event_name() {
    let err = AccountEvent::decode("account.frozen", &[]).unwrap_err();
    assert_eq!(
        err,
        CodecError::UnknownEventName("account.frozen".to_string())
    );
    assert_eq!(err.to_string(), "unknown event name \"account.frozen\"");
}

#[test]
fn overdraw_rejection_renders_like_the_spike() {
    // The spike asserted this exact string; the typed error must still render
    // it, so existing operator-facing messages are preserved.
    let err = AccountError::InsufficientFunds { balance: 120, requested: 1000 };
    assert_eq!(
        err.to_string(),
        "insufficient funds: balance is 120, requested 1000"
    );
}
