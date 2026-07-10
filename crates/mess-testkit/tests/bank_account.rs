//! Acceptance test: the bank-account example from the north star, run
//! through `mess-testkit`'s own Given-When-Then kit.
//!
//! Domain impls are hand-written and copied minimally from
//! `spikes/dx_api/tests/bank_account.rs` / `mess-core/tests/bank_account.rs`
//! — what `#[derive(Event)]` / `#[derive(Aggregate)]` (bn-hy7) would
//! generate. This file exercises `mess-testkit` itself: `AggregateTest`,
//! `then_events`, and both `then_error` forms (exact value and
//! [`mess_testkit::matching`] predicate), store-free.

use std::fmt;

use mess_core::{Aggregate, CodecError, Decide, Event};
use mess_testkit::{AggregateTest, matching};

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
            let arr: [u8; 8] =
                bytes.try_into().map_err(|_| CodecError::Decode {
                    event_name: name.to_string(),
                    source:     "expected 8 payload bytes".to_string(),
                })?;
            Ok(i64::from_le_bytes(arr))
        };
        match name {
            "account.opened" => {
                let owner = String::from_utf8(data.to_vec()).map_err(|e| {
                    CodecError::Decode {
                        event_name: name.to_string(),
                        source:     e.to_string(),
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
    open:    bool,
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

/// The aggregate's typed rejection, named through every `Decide` impl
/// below via `Decide::Rejection` — the production replacement for the
/// spike's stringly `DomainError`.
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
                balance:   self.balance,
                requested: cmd.amount,
            });
        }
        Ok(vec![AccountEvent::Withdrawn { amount: cmd.amount }])
    }
}

// ---------------------------------------------------------------------------
// Given-When-Then tests: pure in-memory, no store, no async — this is
// `mess-testkit`'s own API under test.
// ---------------------------------------------------------------------------

#[test]
fn gwt_open_emits_opened() {
    AggregateTest::<Account>::given_no_events()
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
        balance:   30,
        requested: 100,
    });
}

#[test]
fn gwt_cannot_deposit_before_open() {
    AggregateTest::<Account>::given_no_events()
        .when(Deposit { amount: 10 })
        .then_error(AccountError::NotOpen);
}

/// `then_error` also accepts a labeled predicate via `matching`, for
/// asserting on a rejection's shape without pinning down every field (here,
/// the exact overdrawn balance/requested amount don't matter).
#[test]
fn gwt_overdraw_matches_by_predicate() {
    AggregateTest::<Account>::given([
        AccountEvent::Opened { owner: "alice".into() },
        AccountEvent::Deposited { amount: 50 },
    ])
    .when(Withdraw { amount: 1_000 })
    .then_error(matching("insufficient funds", |e| {
        matches!(e, AccountError::InsufficientFunds { .. })
    }));
}

/// Prove the kit panics with a readable positional diff on mismatch.
#[test]
fn gwt_mismatch_panics_with_diff() {
    let panic = std::panic::catch_unwind(|| {
        AggregateTest::<Account>::given([AccountEvent::Opened {
            owner: "alice".into(),
        }])
        .when(Deposit { amount: 100 })
        .then_events([AccountEvent::Deposited { amount: 999 }]);
    })
    .expect_err("should have panicked");
    let msg = panic.downcast_ref::<String>().unwrap();
    assert!(msg.contains("MISMATCH"), "diff missing from panic: {msg}");
    assert!(msg.contains("expected: Deposited { amount: 999 }"));
    assert!(msg.contains("actual:   Deposited { amount: 100 }"));
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
