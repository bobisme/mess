//! Bank-account domain: the mess v1 API showcase.
//!
//! This crate is `examples/bank`. `examples/bank.rs` runs it end-to-end
//! against a [`mess_store::EventStore`]; `tests/gwt.rs` exercises the same
//! domain store-free through `mess-testkit`'s Given-When-Then kit.
//!
//! Ported from `spikes/dx_api/tests/bank_account.rs` (the original
//! hand-written spike) — every `impl Event` / `impl Aggregate` block there
//! is replaced here by a derive.
//!
//! # Concepts, in the order a newcomer meets them
//!
//! 1. **`#[derive(Event)]`** — the wire vocabulary a stream can hold. Each
//!    enum variant becomes one dotted wire name; the derive generates
//!    `encode`/`decode` so you never hand-write codec plumbing.
//! 2. **`#[derive(Aggregate)]`** — the folded read-model. The derive wires
//!    the trait; you write one inherent `apply` method, which is the ONLY
//!    place an event is allowed to mutate state.
//! 3. **`Decide<Command>`** — one command handler per command type: pure,
//!    synchronous, no I/O, no knowledge of storage or retries.
//! 4. **A typed `Rejection`** per aggregate — the "why not" a command was
//!    refused, matched exhaustively by callers instead of parsed from a
//!    string.

use mess_core::Decide;
use mess_derive::{Aggregate, Event};

// ---------------------------------------------------------------------------
// 1. Events: the wire vocabulary.
// ---------------------------------------------------------------------------

/// Every fact that can happen to a bank account.
///
/// `#[event(name = "account", version = 1)]` gives the wire-name prefix and
/// schema version; `#[derive(Event)]` turns `Opened` into the stored name
/// `"account.opened"`, `Deposited` into `"account.deposited"`, and so on.
/// Dispatch on decode keys on that stored *name*, never on the variant's
/// declaration order, so reordering variants below cannot silently swap
/// what a stored byte string decodes to.
#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "account", version = 1)]
pub enum AccountEvent {
    Opened { owner: String },
    Deposited { amount: i64 },
    Withdrawn { amount: i64 },
}

// ---------------------------------------------------------------------------
// 2. The aggregate: folded state plus the one place events become state.
// ---------------------------------------------------------------------------

/// The read-model folded from one account's event stream.
///
/// `#[aggregate(event = AccountEvent)]` tells the derive which event type
/// this aggregate folds; the generated `mess_core::Aggregate::apply` just
/// forwards to the inherent `apply` below.
#[derive(Debug, Default, Clone, PartialEq, Eq, Aggregate)]
#[aggregate(event = AccountEvent)]
pub struct Account {
    pub open: bool,
    pub balance: i64,
}

impl Account {
    /// Fold one event into state. Infallible by construction: by the time
    /// an event is in the log, a prior `decide` call already proved it was
    /// legal — `apply` never rejects, never validates, just mutates.
    pub fn apply(&mut self, event: &AccountEvent) {
        match event {
            AccountEvent::Opened { .. } => self.open = true,
            AccountEvent::Deposited { amount } => self.balance += amount,
            AccountEvent::Withdrawn { amount } => self.balance -= amount,
        }
    }
}

// ---------------------------------------------------------------------------
// 3 & 4. Commands, the typed rejection, and one `Decide` impl per command.
// ---------------------------------------------------------------------------

/// Every way a command against [`Account`] can be refused.
///
/// This is [`Decide::Rejection`] — a typed business-rule outcome, distinct
/// from a store/backend failure (`mess_store::StoreError`) and distinct
/// from conflict-exhaustion (`mess_core::CommandError::Conflict`, raised
/// when `EventStore::command`'s optimistic retry budget runs out). Callers
/// match on this exhaustively instead of parsing an error string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountError {
    AlreadyOpen,
    NotOpen,
    NonPositiveDeposit,
    InsufficientFunds { balance: i64, requested: i64 },
}

impl std::fmt::Display for AccountError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccountError::AlreadyOpen => write!(f, "account is already open"),
            AccountError::NotOpen => write!(f, "account is not open"),
            AccountError::NonPositiveDeposit => {
                write!(f, "deposit must be positive")
            }
            AccountError::InsufficientFunds { balance, requested } => write!(
                f,
                "insufficient funds: balance is {balance}, requested {requested}"
            ),
        }
    }
}

impl std::error::Error for AccountError {}

/// Open a new account. Commands are plain structs — never stored, never
/// put on the wire; only the events `decide` returns cross into the log.
#[derive(Debug, Clone)]
pub struct Open {
    pub owner: String,
}

#[derive(Debug, Clone, Copy)]
pub struct Deposit {
    pub amount: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct Withdraw {
    pub amount: i64,
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
