//! Given-When-Then tests for the bank-account domain via `mess-testkit`'s
//! store-free [`AggregateTest`]: no store, no async, no I/O — just
//! `given` events folded, a command run through `Decide::decide`, and an
//! assertion on the outcome.
//!
//! Run with:
//!
//! ```sh
//! cargo test -p bank
//! ```

use bank::{Account, AccountError, AccountEvent, Deposit, Open, Withdraw};
use mess_testkit::{AggregateTest, matching};

#[test]
fn opening_emits_opened() {
    AggregateTest::<Account>::given_no_events()
        .when(Open { owner: "alice".into() })
        .then_events([AccountEvent::Opened { owner: "alice".into() }]);
}

#[test]
fn cannot_open_twice() {
    AggregateTest::<Account>::given([AccountEvent::Opened {
        owner: "alice".into(),
    }])
    .when(Open { owner: "bob".into() })
    .then_error(AccountError::AlreadyOpen);
}

#[test]
fn deposit_emits_deposited() {
    AggregateTest::<Account>::given([AccountEvent::Opened {
        owner: "alice".into(),
    }])
    .when(Deposit { amount: 100 })
    .then_events([AccountEvent::Deposited { amount: 100 }]);
}

#[test]
fn cannot_deposit_before_open() {
    AggregateTest::<Account>::given_no_events()
        .when(Deposit { amount: 10 })
        .then_error(AccountError::NotOpen);
}

#[test]
fn deposit_must_be_positive() {
    AggregateTest::<Account>::given([AccountEvent::Opened {
        owner: "alice".into(),
    }])
    .when(Deposit { amount: 0 })
    .then_error(AccountError::NonPositiveDeposit);
}

#[test]
fn withdraw_within_balance() {
    AggregateTest::<Account>::given([
        AccountEvent::Opened { owner: "alice".into() },
        AccountEvent::Deposited { amount: 100 },
    ])
    .when(Withdraw { amount: 40 })
    .then_events([AccountEvent::Withdrawn { amount: 40 }]);
}

#[test]
fn overdraw_is_rejected_with_exact_amounts() {
    AggregateTest::<Account>::given([
        AccountEvent::Opened { owner: "alice".into() },
        AccountEvent::Deposited { amount: 50 },
        AccountEvent::Withdrawn { amount: 20 },
    ])
    .when(Withdraw { amount: 100 })
    .then_error(AccountError::InsufficientFunds { balance: 30, requested: 100 });
}

/// `then_error` also accepts a labeled predicate via `matching`, for
/// asserting on a rejection's shape without pinning down every field.
#[test]
fn overdraw_matches_by_predicate() {
    AggregateTest::<Account>::given([
        AccountEvent::Opened { owner: "alice".into() },
        AccountEvent::Deposited { amount: 50 },
    ])
    .when(Withdraw { amount: 1_000 })
    .then_error(matching("insufficient funds", |e| {
        matches!(e, AccountError::InsufficientFunds { .. })
    }));
}

#[test]
fn event_codec_round_trips() {
    use mess_core::Event;

    let events = [
        AccountEvent::Opened { owner: "alice".into() },
        AccountEvent::Deposited { amount: 100 },
        AccountEvent::Withdrawn { amount: 40 },
    ];
    for event in events {
        let bytes = event.encode().unwrap();
        let back = AccountEvent::decode(event.name(), &bytes).unwrap();
        assert_eq!(back, event);
    }
}
