//! Demonstration: the bank-account example from the north star
//! (notes/mess-research/09_implementation_plan.md), running against the
//! real mess_db RocksDB actor backend.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use dx_api::store::{CommandError, EventStore, Version};
use dx_api::testkit::AggregateTest;
use dx_api::{Aggregate, CodecError, Decide, DomainError, Event};

// ---------------------------------------------------------------------------
// Domain: what a user of the library writes today (all manual; every impl
// below is what `#[derive(Event)]` / `#[derive(Aggregate)]` would generate).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
        serde_json::to_vec(self).map_err(|e| CodecError::Encode(e.to_string()))
    }

    fn decode(name: &str, data: &[u8]) -> Result<Self, CodecError> {
        // A derive would dispatch per-variant on `name`; this manual impl
        // just validates the name and lets serde's enum tagging carry the
        // variant.
        match name {
            "account.opened" | "account.deposited" | "account.withdrawn" => {
                serde_json::from_slice(data).map_err(|e| CodecError::Decode {
                    event_name: name.to_string(),
                    source: e.to_string(),
                })
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
    fn decide(&self, cmd: Open) -> Result<Vec<AccountEvent>, DomainError> {
        if self.open {
            return Err(DomainError::new("account is already open"));
        }
        Ok(vec![AccountEvent::Opened { owner: cmd.owner }])
    }
}

impl Decide<Deposit> for Account {
    fn decide(&self, cmd: Deposit) -> Result<Vec<AccountEvent>, DomainError> {
        if !self.open {
            return Err(DomainError::new("account is not open"));
        }
        if cmd.amount <= 0 {
            return Err(DomainError::new("deposit must be positive"));
        }
        Ok(vec![AccountEvent::Deposited { amount: cmd.amount }])
    }
}

impl Decide<Withdraw> for Account {
    fn decide(&self, cmd: Withdraw) -> Result<Vec<AccountEvent>, DomainError> {
        if !self.open {
            return Err(DomainError::new("account is not open"));
        }
        if cmd.amount > self.balance {
            return Err(DomainError::new(format!(
                "insufficient funds: balance is {}, requested {}",
                self.balance, cmd.amount
            )));
        }
        Ok(vec![AccountEvent::Withdrawn { amount: cmd.amount }])
    }
}

// ---------------------------------------------------------------------------
// Given-When-Then tests: pure in-memory, no store, no async.
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
    .then_error(DomainError::new("account is already open"));
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
    .then_error(DomainError::new(
        "insufficient funds: balance is 30, requested 100",
    ));
}

#[test]
fn gwt_cannot_deposit_before_open() {
    AggregateTest::<Account>::given([])
        .when(Deposit { amount: 10 })
        .then_error(DomainError::new("account is not open"));
}

/// Prove the kit panics with a readable diff on mismatch.
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
// Store tests: real RocksDB in a temp dir, through the mess_db actor.
// ---------------------------------------------------------------------------

/// Minimal tempdir guard (avoids an extra dependency). Removal is
/// best-effort; on Linux unlinking files still held open by RocksDB is fine.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new() -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "dx_api_spike_{}_{}_{n}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn bank_account_end_to_end() {
    let tmp = TempDir::new();
    let store = EventStore::open(&tmp.0).unwrap();

    // The north-star calling code:
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

    // Overdraw is rejected by the domain, not the store.
    let err = store
        .command::<Account, _>("account-123", Withdraw { amount: 1_000 })
        .await
        .unwrap_err();
    match err {
        CommandError::Domain(e) => {
            assert_eq!(
                e,
                DomainError::new(
                    "insufficient funds: balance is 120, requested 1000"
                )
            );
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
    let tmp = TempDir::new();
    let store = EventStore::open(&tmp.0).unwrap();
    let stream = "account-ev";

    let events = [AccountEvent::Opened { owner: "alice".into() }];
    store.append(stream, Version::NoStream, &events).await.unwrap();

    // Same expectation again must conflict.
    let err =
        store.append(stream, Version::NoStream, &events).await.unwrap_err();
    assert!(err.is_version_conflict(), "got: {err}");

    // Stale exact version must conflict too.
    let more = [
        AccountEvent::Deposited { amount: 1 },
        AccountEvent::Deposited { amount: 2 },
    ];
    let commit = store.append(stream, Version::At(0), &more).await.unwrap();
    assert_eq!(commit.version, Version::At(2));
    let err = store.append(stream, Version::At(1), &more).await.unwrap_err();
    assert!(err.is_version_conflict(), "got: {err}");
}

#[tokio::test]
async fn command_exhausts_retries_with_typed_error() {
    let tmp = TempDir::new();
    let store = EventStore::open(&tmp.0).unwrap().with_max_attempts(3);
    let stream = "account-contended";

    store
        .command::<Account, _>(stream, Open { owner: "alice".into() })
        .await
        .unwrap();

    // Saboteur: keeps advancing the stream while we issue commands with a
    // retry budget of 1, so our commands should quickly lose a race and
    // surface the typed Conflict error.
    let store2 = store.clone().with_max_attempts(1);
    let saboteur = {
        let store = store.clone();
        let stream = stream.to_string();
        tokio::spawn(async move {
            for _ in 0..200 {
                let _ = store
                    .command::<Account, _>(&stream, Deposit { amount: 1 })
                    .await;
            }
        })
    };

    // With a budget of 1 against 200 racing writes, at least one of our
    // attempts should hit a conflict; retry until we observe it (bounded).
    let mut saw_conflict = false;
    for _ in 0..200 {
        match store2
            .command::<Account, _>(stream, Deposit { amount: 1 })
            .await
        {
            Err(CommandError::Conflict { stream: s, attempts }) => {
                assert_eq!(s, stream);
                assert_eq!(attempts, 1);
                saw_conflict = true;
                break;
            }
            Ok(_) => {}
            Err(other) => panic!("unexpected error: {other}"),
        }
    }
    saboteur.await.unwrap();
    assert!(
        saw_conflict,
        "never observed a Conflict error with max_attempts=1 against a \
         racing writer"
    );
}

/// THE concurrency test: 8 tasks hammer ONE stream through `command()`.
/// Every deposit must eventually succeed via optimistic retry, and the
/// final balance must be exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_commands_on_one_stream_all_succeed() {
    const TASKS: usize = 8;
    const DEPOSITS_PER_TASK: usize = 25;
    const AMOUNT: i64 = 7;

    let tmp = TempDir::new();
    let store =
        Arc::new(EventStore::open(&tmp.0).unwrap().with_max_attempts(64));
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
    assert!(attempts_total >= commands);

    let loaded = store.load::<Account>(stream).await.unwrap();
    assert_eq!(
        loaded.state.balance,
        (TASKS * DEPOSITS_PER_TASK) as i64 * AMOUNT,
        "final balance must be exact — no lost updates, no duplicates"
    );
    assert_eq!(loaded.events_replayed, TASKS * DEPOSITS_PER_TASK + 1);
    assert_eq!(loaded.version, Version::At((TASKS * DEPOSITS_PER_TASK) as u64));
}
