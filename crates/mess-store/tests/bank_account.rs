//! The bank-account example ported from `spikes/dx_api`, now driven against
//! the in-memory [`MockBackend`] instead of the RocksDB actor. The headline
//! is `concurrent_commands_on_one_stream_all_succeed`: 8 writers × 25 deposits
//! on one hot stream must converge to an exact balance via optimistic retry.

use std::convert::Infallible;
use std::fmt;
use std::sync::Arc;

use mess_core::{Aggregate, CodecError, Decide, Event};
use mess_store::backend::{
    AppendError, Appended, Backend, RecordToAppend, StoredRecord,
};
use mess_store::{CommandError, EventStore, MockBackend, RetryPolicy, Version};

// ---------------------------------------------------------------------------
// Domain (all hand-written — the shape a `#[derive]` layer would generate).
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

    // The event name disambiguates the variant, so each payload is just its
    // own field(s) — no tag byte needed. Deliberately serde-free to keep the
    // crate's dependency surface minimal.
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
        let amount = || -> Result<i64, CodecError> {
            let bytes: [u8; 8] =
                data.try_into().map_err(|_| CodecError::Decode {
                    event_name: name.to_string(),
                    source: format!(
                        "expected 8 payload bytes, got {}",
                        data.len()
                    ),
                })?;
            Ok(i64::from_le_bytes(bytes))
        };
        match name {
            "account.opened" => Ok(AccountEvent::Opened {
                owner: String::from_utf8(data.to_vec()).map_err(|e| {
                    CodecError::Decode {
                        event_name: name.to_string(),
                        source: e.to_string(),
                    }
                })?,
            }),
            "account.deposited" => {
                Ok(AccountEvent::Deposited { amount: amount()? })
            }
            "account.withdrawn" => {
                Ok(AccountEvent::Withdrawn { amount: amount()? })
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

/// The aggregate's typed rejection (the `R` in `CommandError<R, S>`).
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
                "insufficient funds: balance is {balance}, requested {requested}"
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

fn store() -> EventStore<MockBackend> {
    EventStore::new(MockBackend::new())
}

// ---------------------------------------------------------------------------
// End-to-end against the mock.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bank_account_end_to_end() {
    let store = store();

    store
        .command::<Account, _>("account-123", Open { owner: "alice".into() })
        .await
        .unwrap();
    store
        .command::<Account, _>("account-123", Deposit { amount: 100 })
        .await
        .unwrap();
    store
        .command::<Account, _>("account-123", Deposit { amount: 50 })
        .await
        .unwrap();
    let commit = store
        .command::<Account, _>("account-123", Withdraw { amount: 30 })
        .await
        .unwrap();
    assert_eq!(commit.version, Version::At(3));
    assert_eq!(commit.events_appended, 1);
    assert_eq!(commit.attempts, 1);

    // Overdraw is rejected by the domain, not the store.
    let err = store
        .command::<Account, _>("account-123", Withdraw { amount: 1_000 })
        .await
        .unwrap_err();
    match err {
        CommandError::Domain(AccountError::InsufficientFunds {
            balance,
            requested,
        }) => {
            assert_eq!(balance, 120);
            assert_eq!(requested, 1_000);
        }
        other => panic!("expected a domain rejection, got: {other}"),
    }

    // Rebuild from the log.
    let loaded = store.load::<Account>("account-123").await.unwrap();
    assert!(loaded.state.open);
    assert_eq!(loaded.state.balance, 120);
    assert_eq!(loaded.version, Version::At(3));
    assert_eq!(loaded.events_replayed, 4);

    // A different stream is untouched.
    let other = store.load::<Account>("account-999").await.unwrap();
    assert_eq!(other.version, Version::NoStream);
    assert_eq!(other.state, Account::default());
}

#[tokio::test]
async fn append_enforces_expected_version() {
    let store = store();
    let stream = "account-ev";

    let events = [AccountEvent::Opened { owner: "alice".into() }];
    store.append(stream, Version::NoStream, &events).await.unwrap();

    // Same expectation again must conflict.
    let err =
        store.append(stream, Version::NoStream, &events).await.unwrap_err();
    assert!(matches!(err, AppendError::Conflict { .. }), "got: {err}");

    // A stale exact version must conflict too.
    let more = [
        AccountEvent::Deposited { amount: 1 },
        AccountEvent::Deposited { amount: 2 },
    ];
    let commit = store.append(stream, Version::At(0), &more).await.unwrap();
    assert_eq!(commit.version, Version::At(2));
    let err = store.append(stream, Version::At(1), &more).await.unwrap_err();
    assert!(matches!(err, AppendError::Conflict { .. }), "got: {err}");
}

/// The paged replay loop must cross many pages with no ceiling: write 2_500
/// events, then load with a tiny page size and get every one back.
#[tokio::test]
async fn load_pages_across_many_pages() {
    let store = store().with_page_size(7);
    let stream = "account-long";

    store
        .command::<Account, _>(stream, Open { owner: "alice".into() })
        .await
        .unwrap();
    for _ in 0..2_500 {
        store
            .command::<Account, _>(stream, Deposit { amount: 1 })
            .await
            .unwrap();
    }

    let loaded = store.load::<Account>(stream).await.unwrap();
    assert_eq!(loaded.state.balance, 2_500);
    assert_eq!(loaded.events_replayed, 2_501);
    assert_eq!(loaded.version, Version::At(2_500));
}

// ---------------------------------------------------------------------------
// Conflict exhaustion: distinct typed variant carrying the attempt count.
// ---------------------------------------------------------------------------

/// A backend whose `append_batch` always signals a version conflict — a
/// deterministic way to drive `command` to budget exhaustion.
#[derive(Clone, Default)]
struct AlwaysConflict;

impl Backend for AlwaysConflict {
    type Error = Infallible;

    async fn head(&self, _stream_id: &str) -> Result<Version, Self::Error> {
        Ok(Version::NoStream)
    }

    async fn read_stream(
        &self,
        _stream_id: &str,
        _after: Version,
        _limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        Ok(Vec::new())
    }

    async fn read_global(
        &self,
        _after: Option<u64>,
        _limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        Ok(Vec::new())
    }

    async fn append_batch(
        &self,
        _stream_id: &str,
        expected: Version,
        _records: &[RecordToAppend],
    ) -> Result<Appended, AppendError<Self::Error>> {
        Err(AppendError::Conflict { expected, actual: Version::At(999) })
    }
}

#[tokio::test]
async fn command_exhausts_retries_with_typed_conflict() {
    let store = EventStore::new(AlwaysConflict)
        .with_retry_policy(RetryPolicy::no_backoff(5));
    let stream = "account-doomed";

    let err = store
        .command::<Account, _>(stream, Open { owner: "alice".into() })
        .await
        .unwrap_err();

    match err {
        CommandError::Conflict { stream: s, attempts } => {
            assert_eq!(s, stream);
            assert_eq!(attempts, 5, "must exhaust exactly the budget");
        }
        other => panic!("expected conflict exhaustion, got: {other}"),
    }
}

// ---------------------------------------------------------------------------
// THE concurrency test: 8 writers × 25 deposits on ONE stream.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_commands_on_one_stream_all_succeed() {
    const TASKS: usize = 8;
    const DEPOSITS_PER_TASK: usize = 25;
    const AMOUNT: i64 = 7;

    // Default policy: budget 64 (> the spike's observed worst case of 24) with
    // real jittered backoff.
    let store = Arc::new(store());
    let stream = "account-hot";

    store
        .command::<Account, _>(stream, Open { owner: "everyone".into() })
        .await
        .unwrap();

    let mut handles = Vec::new();
    for task in 0..TASKS {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            let mut attempts_total = 0u32;
            let mut attempts_max = 0u32;
            for _ in 0..DEPOSITS_PER_TASK {
                let commit = store
                    .command::<Account, _>(stream, Deposit { amount: AMOUNT })
                    .await
                    .unwrap_or_else(|e| {
                        panic!("task {task}: command failed: {e}")
                    });
                attempts_total += commit.attempts;
                attempts_max = attempts_max.max(commit.attempts);
            }
            (attempts_total, attempts_max)
        }));
    }

    let mut attempts_total = 0u32;
    let mut attempts_max = 0u32;
    for handle in handles {
        let (total, max) = handle.await.unwrap();
        attempts_total += total;
        attempts_max = attempts_max.max(max);
    }

    let commands = (TASKS * DEPOSITS_PER_TASK) as u32;
    println!(
        "concurrency: {commands} commands took {attempts_total} attempts \
         (max {attempts_max} for a single command)"
    );
    // Every command is at least one attempt; genuine contention pushes the
    // total above the command count.
    assert!(attempts_total >= commands);
    assert!(
        attempts_total > commands,
        "expected real contention (retries) on one hot stream, but no command \
         ever retried — total attempts {attempts_total} == commands {commands}"
    );

    let loaded = store.load::<Account>(stream).await.unwrap();
    assert_eq!(
        loaded.state.balance,
        (TASKS * DEPOSITS_PER_TASK) as i64 * AMOUNT,
        "final balance must be exact — no lost updates, no duplicates"
    );
    assert_eq!(loaded.events_replayed, TASKS * DEPOSITS_PER_TASK + 1);
    assert_eq!(loaded.version, Version::At((TASKS * DEPOSITS_PER_TASK) as u64));
}
