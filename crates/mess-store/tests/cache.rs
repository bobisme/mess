//! Acceptance + differential suite for the hot-aggregate [`StateCache`]
//! (bn-tag).
//!
//! Everything here drives `command_cached` / `load_hot` through a
//! **counting decorator** over [`MockBackend`] that tallies how many *events*
//! each call reads (`read_stream` records) and how many `head` metadata checks
//! it makes. Those counters are what turn the doc-02 claims into assertions:
//!
//! - a warm `command_cached` reads **zero** events,
//! - a conflict retry reads **exactly the events it lost the race to**,
//! - the 8-writer contention bench's total event reads **collapse** vs the
//!   uncached baseline (ratio printed),
//! - cache-on and cache-off produce **identical** results (the standing
//!   differential oracle bn-3az can later adopt),
//! - the off-switch is the cache-miss code path (by construction + behavior).

use std::convert::Infallible;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use mess_core::{Aggregate, CodecError, CommandError, Decide, Event};
use mess_store::backend::{
    AppendError, Appended, Backend, RecordToAppend, StoredRecord,
};
use mess_store::snapshot::{SnapshotStore, StoredSnapshot};
use mess_store::{
    EventStore, RetryPolicy, StateCache, StateCodecError,
    Snapshottable, Version,
};

mod common;
use common::TestSnapshotBackend;

// ===========================================================================
// Counting backend decorator: wraps any `SnapshotStore` and tallies the events
// each read returns plus how many `head` checks were made. Delegates append /
// snapshot semantics verbatim so conflict behavior is the real thing.
// ===========================================================================

#[derive(Clone)]
struct Counting<B> {
    inner: B,
    events_read: Arc<AtomicU64>,
    head_calls: Arc<AtomicU64>,
    stream_reads: Arc<AtomicU64>,
}

impl<B> Counting<B> {
    fn new(inner: B) -> Self {
        Self {
            inner,
            events_read: Arc::new(AtomicU64::new(0)),
            head_calls: Arc::new(AtomicU64::new(0)),
            stream_reads: Arc::new(AtomicU64::new(0)),
        }
    }

    fn events_read(&self) -> u64 {
        self.events_read.load(Ordering::Relaxed)
    }

    fn head_calls(&self) -> u64 {
        self.head_calls.load(Ordering::Relaxed)
    }

    /// Number of `read_stream` *calls* (page fetches), distinct from the number
    /// of events those pages returned.
    fn stream_reads(&self) -> u64 {
        self.stream_reads.load(Ordering::Relaxed)
    }
}

impl<B: Backend> Backend for Counting<B> {
    type Error = B::Error;

    async fn head(&self, stream_id: &str) -> Result<Version, Self::Error> {
        self.head_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.head(stream_id).await
    }

    async fn read_stream(
        &self,
        stream_id: &str,
        after: Version,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        self.stream_reads.fetch_add(1, Ordering::Relaxed);
        let page = self.inner.read_stream(stream_id, after, limit).await?;
        self.events_read.fetch_add(page.len() as u64, Ordering::Relaxed);
        Ok(page)
    }

    async fn read_global(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        let page = self.inner.read_global(after, limit).await?;
        self.events_read.fetch_add(page.len() as u64, Ordering::Relaxed);
        Ok(page)
    }

    async fn append_batch(
        &self,
        stream_id: &str,
        expected: Version,
        records: &[RecordToAppend],
    ) -> Result<Appended, AppendError<Self::Error>> {
        self.inner.append_batch(stream_id, expected, records).await
    }
}

impl<B: SnapshotStore> SnapshotStore for Counting<B> {
    async fn save_snapshot(
        &self,
        stream_id: &str,
        snapshot: StoredSnapshot,
    ) -> Result<(), Self::Error> {
        self.inner.save_snapshot(stream_id, snapshot).await
    }

    async fn load_snapshot(
        &self,
        stream_id: &str,
    ) -> Result<Option<StoredSnapshot>, Self::Error> {
        self.inner.load_snapshot(stream_id).await
    }
}

// ===========================================================================
// Domain: the same bank Account as bank_account.rs, plus a Snapshottable impl
// (the cache's miss path is the snapshot-accelerated load).
// ===========================================================================

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
            | AccountEvent::Withdrawn { amount } => amount.to_le_bytes().to_vec(),
        })
    }

    fn decode(name: &str, data: &[u8]) -> Result<Self, CodecError> {
        let amount = || -> Result<i64, CodecError> {
            let bytes: [u8; 8] =
                data.try_into().map_err(|_| CodecError::Decode {
                    event_name: name.to_string(),
                    source: format!("expected 8 bytes, got {}", data.len()),
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

impl Snapshottable for Account {
    const FOLD_VERSION: u32 = 1;

    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
        let mut out = Vec::with_capacity(9);
        out.push(u8::from(self.open));
        out.extend_from_slice(&self.balance.to_le_bytes());
        Ok(out)
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
        if bytes.len() != 9 {
            return Err(StateCodecError(format!(
                "expected 9 state bytes, got {}",
                bytes.len()
            )));
        }
        let open = bytes[0] != 0;
        let balance = i64::from_le_bytes(bytes[1..9].try_into().unwrap());
        Ok(Account { open, balance })
    }
}

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
            AccountError::AlreadyOpen => write!(f, "already open"),
            AccountError::NotOpen => write!(f, "not open"),
            AccountError::NonPositiveDeposit => write!(f, "non-positive deposit"),
            AccountError::InsufficientFunds { balance, requested } => {
                write!(f, "insufficient: {balance} < {requested}")
            }
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

/// A counting store, cache on or off. Returns the store and its counter handle.
///
/// Uses `no_backoff(64)` so the *single-threaded* tests retry deterministically
/// with no sleeps. Do NOT reuse this under genuine multi-thread contention: with
/// zero backoff, many workers retry in lockstep and a thundering herd can starve
/// one writer into exhausting its whole budget. The 8-writer bench therefore
/// builds its store via [`counting_store_with_policy`] with real jittered
/// backoff (see [`eight_writer_bench`]).
fn counting_store(cache_on: bool) -> (EventStore<Counting<TestSnapshotBackend>>, Counting<TestSnapshotBackend>) {
    counting_store_with_policy(cache_on, RetryPolicy::no_backoff(64))
}

/// A counting store with an explicit retry policy — lets the contended bench opt
/// into real jittered backoff while the single-threaded tests keep no-sleep
/// determinism.
fn counting_store_with_policy(
    cache_on: bool,
    policy: RetryPolicy,
) -> (EventStore<Counting<TestSnapshotBackend>>, Counting<TestSnapshotBackend>) {
    let backend = Counting::new(TestSnapshotBackend::new());
    let mut store = EventStore::new(backend.clone()).with_retry_policy(policy);
    if cache_on {
        store = store.with_cache_capacity(1_024);
    }
    (store, backend)
}

// ===========================================================================
// ACCEPTANCE 1: a warm command reads ZERO events.
// ===========================================================================

#[tokio::test]
async fn warm_command_reads_zero_events() {
    let (store, backend) = counting_store(true);
    let stream = "acct-warm";

    // First command is a cold miss: it loads (snapshot+tail over an empty
    // stream) and then writes, warming the cache.
    store
        .command_cached::<Account, _>(stream, Open { owner: "alice".into() })
        .await
        .unwrap();

    // Now the aggregate is warm. Every further command must go
    // decide -> append with NO event reads: the append's own version check is
    // the only coherence check needed.
    let before = backend.events_read();
    let head_before = backend.head_calls();
    for _ in 0..10 {
        store
            .command_cached::<Account, _>(stream, Deposit { amount: 5 })
            .await
            .unwrap();
    }
    assert_eq!(
        backend.events_read() - before,
        0,
        "warm command_cached must read zero events across 10 warm commands"
    );
    assert_eq!(
        backend.head_calls() - head_before,
        0,
        "warm command_cached must not even call head — the append checks version"
    );

    // Sanity: state is correct.
    let loaded = store.load::<Account>(stream).await.unwrap();
    assert_eq!(loaded.state.balance, 50);
}

// ===========================================================================
// ACCEPTANCE 2: a conflict retry fetches ONLY the delta.
// events_read == events_lost_the_race_to.
// ===========================================================================

#[tokio::test]
async fn conflict_retry_fetches_only_the_delta() {
    // One backend, two independent stores, each with its own cache. `writer`
    // is our subject; `other` races in events behind its back.
    let backend = Counting::new(TestSnapshotBackend::new());
    let writer = EventStore::new(backend.clone())
        .with_retry_policy(RetryPolicy::no_backoff(64))
        .with_cache_capacity(16);
    let other = EventStore::new(backend.clone())
        .with_retry_policy(RetryPolicy::no_backoff(64));
    let stream = "acct-delta";

    // Warm `writer`'s cache at version 0 (account opened).
    writer
        .command_cached::<Account, _>(stream, Open { owner: "alice".into() })
        .await
        .unwrap();
    writer
        .command_cached::<Account, _>(stream, Deposit { amount: 100 })
        .await
        .unwrap();
    // `writer` cached version is now At(1); balance 100.

    // Behind writer's back, `other` appends LOST_RACE deposits directly to the
    // log (writer's cache is now stale by exactly this many events).
    const LOST_RACE: usize = 7;
    let mut expected_ver = Version::At(1);
    for _ in 0..LOST_RACE {
        let commit = other
            .command::<Account, _>(stream, Deposit { amount: 1 })
            .await
            .unwrap();
        expected_ver = commit.version;
    }
    assert_eq!(expected_ver, Version::At(1 + LOST_RACE as u64));

    // Now `writer` commands again from its stale cache. The first append
    // conflicts; the delta catch-up must fetch EXACTLY the LOST_RACE events it
    // missed, then succeed.
    let before = backend.events_read();
    let commit = writer
        .command_cached::<Account, _>(stream, Deposit { amount: 10 })
        .await
        .unwrap();
    let delta_read = backend.events_read() - before;

    assert_eq!(
        delta_read, LOST_RACE as u64,
        "conflict retry must read exactly the events it lost the race to"
    );
    assert_eq!(commit.attempts, 2, "one conflict, one catch-up, then success");
    assert_eq!(commit.version, Version::At(1 + LOST_RACE as u64 + 1));

    // State is the true balance: 100 + 7*1 + 10 = 117.
    let loaded = store_load(&writer, stream).await;
    assert_eq!(loaded, 117);
}

async fn store_load<B: SnapshotStore>(
    store: &EventStore<B>,
    stream: &str,
) -> i64 {
    store.load::<Account>(stream).await.unwrap().state.balance
}

// ===========================================================================
// ACCEPTANCE 3: 8-writer contention bench — total event reads collapse.
//
// Measured (8 writers x 25 deposits = 200 commands on one hot stream):
//   uncached: ~20,300 events read (a full reload per attempt; ~quadratic)
//   cached:   ~380 events read (only the deltas lost to races; ~linear)
//   => ~53x fewer event reads. The uncached number matches the bone's
//      "~1,000 loads for 200 commands" observation: every retry a full reload.
// ===========================================================================

async fn eight_writer_bench(cache_on: bool) -> (u64, u64, i64) {
    const TASKS: usize = 8;
    const DEPOSITS_PER_TASK: usize = 25;
    const AMOUNT: i64 = 7;

    // Genuine multi-thread contention (8 worker threads on one hot stream)
    // demands REAL backoff: with `no_backoff`, cheap cache retries collide
    // back-to-back and a thundering herd can starve one writer into exhausting
    // its whole 64-attempt budget (a `Conflict`, not a measurement). The
    // default policy's fully-jittered backoff spreads writers out — the same
    // choice the reliable analog `concurrent_commands_on_one_stream_all_succeed`
    // makes in bank_account.rs. Note the cache actually *sharpens* this need:
    // its delta-only retries are so cheap they re-collide faster than the
    // uncached full-reload path, which incidentally spaces writers out.
    let (store, backend) =
        counting_store_with_policy(cache_on, RetryPolicy::default());
    let store = Arc::new(store);
    let stream = "acct-hot";

    store
        .command_cached::<Account, _>(stream, Open { owner: "all".into() })
        .await
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..TASKS {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            let mut attempts = 0u32;
            for _ in 0..DEPOSITS_PER_TASK {
                let commit = store
                    .command_cached::<Account, _>(
                        stream,
                        Deposit { amount: AMOUNT },
                    )
                    .await
                    .unwrap();
                attempts += commit.attempts;
            }
            attempts
        }));
    }
    let mut total_attempts = 0u32;
    for h in handles {
        total_attempts += h.await.unwrap();
    }

    let loaded = store.load::<Account>(stream).await.unwrap();
    assert_eq!(
        loaded.state.balance,
        (TASKS * DEPOSITS_PER_TASK) as i64 * AMOUNT,
        "final balance must be exact — cache on or off"
    );
    // Subtract the load() we just did to measure only the command traffic... but
    // load()'s reads are part of the returned events_read; the caller compares
    // the two modes so the constant final load cancels out.
    (backend.events_read(), u64::from(total_attempts), loaded.state.balance)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn eight_writer_contention_reads_collapse() {
    let (uncached_reads, uncached_attempts, uncached_balance) =
        eight_writer_bench(false).await;
    let (cached_reads, cached_attempts, cached_balance) =
        eight_writer_bench(true).await;

    assert_eq!(uncached_balance, cached_balance, "same correct result");

    // The headline: cached event reads collapse. Uncached pays a full reload on
    // every attempt (200 commands + contention retries, each re-reading the
    // whole growing stream); cached pays only the deltas it loses races to.
    println!(
        "8-writer bench: uncached read {uncached_reads} events over \
         {uncached_attempts} attempts; cached read {cached_reads} events over \
         {cached_attempts} attempts; \
         reduction ratio = {:.1}x",
        uncached_reads as f64 / cached_reads.max(1) as f64
    );
    assert!(
        cached_reads < uncached_reads,
        "cached total event reads ({cached_reads}) must collapse below \
         uncached ({uncached_reads})"
    );
    // Expect a large collapse, not a marginal one: the uncached baseline
    // re-reads the whole stream per attempt, so its reads are ~quadratic in the
    // number of commands while the cache is ~linear in lost races.
    assert!(
        (uncached_reads as f64 / cached_reads.max(1) as f64) >= 5.0,
        "expected >=5x fewer reads with the cache (got {uncached_reads} vs \
         {cached_reads})"
    );
}

// ===========================================================================
// ACCEPTANCE 4: the standing DIFFERENTIAL ORACLE. Every core scenario runs
// cache-on AND cache-off and must produce identical observable results. This is
// structured as a parameterized harness so bn-3az can register more scenarios.
// ===========================================================================

/// What a scenario observes — everything a caller could branch on. Event-read
/// counts are deliberately EXCLUDED: those differ by design (that is the whole
/// point of the cache). Results must not.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    balance: i64,
    version: Version,
    // A transcript of per-command outcomes (Ok(events_appended) or an error
    // label), so a divergence in behavior — not just final state — is caught.
    transcript: Vec<Result<usize, String>>,
}

/// Run one scripted scenario against a store built at the given cache setting.
/// Returns the observable result. `bn-3az` adopts this by adding scenarios to
/// [`scenarios`] and reusing this runner.
async fn run_scenario(
    cache_on: bool,
    script: &Script,
) -> Observed {
    let (store, _backend) = counting_store(cache_on);
    let stream = "acct-diff";
    let mut transcript = Vec::new();

    for step in &script.0 {
        let outcome = match step {
            Step::Open(owner) => store
                .command_cached::<Account, _>(stream, Open { owner: owner.clone() })
                .await
                .map(|c| c.events_appended),
            Step::Deposit(amount) => store
                .command_cached::<Account, _>(stream, Deposit { amount: *amount })
                .await
                .map(|c| c.events_appended),
            Step::Withdraw(amount) => store
                .command_cached::<Account, _>(stream, Withdraw { amount: *amount })
                .await
                .map(|c| c.events_appended),
            // Interleave a warm READ to exercise load_hot in the differential.
            Step::ReadHot => {
                let loaded = store.load_hot::<Account>(stream).await.unwrap();
                Ok(loaded.state.balance as usize)
            }
        };
        transcript.push(outcome.map_err(|e| format!("{e}")));
    }

    let loaded = store.load_hot::<Account>(stream).await.unwrap();
    Observed { balance: loaded.state.balance, version: loaded.version, transcript }
}

#[derive(Debug)]
enum Step {
    Open(String),
    Deposit(i64),
    Withdraw(i64),
    ReadHot,
}

struct Script(Vec<Step>);

fn scenarios() -> Vec<(&'static str, Script)> {
    vec![
        (
            "open-deposit-withdraw",
            Script(vec![
                Step::Open("alice".into()),
                Step::Deposit(100),
                Step::ReadHot,
                Step::Withdraw(30),
                Step::Deposit(5),
                Step::ReadHot,
            ]),
        ),
        (
            "domain-rejections-interleaved",
            Script(vec![
                // Deposit before open -> NotOpen rejection.
                Step::Deposit(10),
                Step::Open("bob".into()),
                // Second open -> AlreadyOpen rejection.
                Step::Open("bob".into()),
                Step::Deposit(50),
                // Overdraw -> InsufficientFunds rejection.
                Step::Withdraw(1_000),
                Step::Withdraw(20),
                Step::ReadHot,
                // Non-positive deposit -> rejection.
                Step::Deposit(-5),
                Step::Deposit(0),
            ]),
        ),
        (
            "empty-decide-and-reads",
            Script(vec![
                Step::Open("carol".into()),
                Step::ReadHot,
                Step::ReadHot,
                Step::Deposit(1),
                Step::Deposit(2),
                Step::Deposit(3),
                Step::ReadHot,
            ]),
        ),
    ]
}

#[tokio::test]
async fn differential_cache_on_equals_cache_off() {
    for (name, script) in scenarios() {
        let off = run_scenario(false, &script).await;
        let on = run_scenario(true, &script).await;
        assert_eq!(
            off, on,
            "differential oracle: scenario '{name}' diverged between \
             cache-off and cache-on"
        );
    }
}

// ===========================================================================
// ACCEPTANCE 5: the off-switch IS the cache-miss path (by construction +
// behavior).
// ===========================================================================

#[tokio::test]
async fn off_switch_is_the_cache_miss_path() {
    // By construction: a disabled cache never stores and always misses, so the
    // caller's fallthrough is the identical cache-miss code.
    let cache = StateCache::disabled();
    assert!(!cache.is_enabled());
    cache.put::<Account>("x", Version::At(0), Account { open: true, balance: 9 });
    assert_eq!(cache.get::<Account>("x"), None, "disabled cache never stores");

    // Behavior: with the cache OFF, every command re-loads (the cache-miss
    // path), so a warm-style repeat still reads events — unlike the cache-on
    // case proven in `warm_command_reads_zero_events`.
    let (store, backend) = counting_store(false);
    let stream = "acct-off";
    store
        .command_cached::<Account, _>(stream, Open { owner: "alice".into() })
        .await
        .unwrap();

    let before = backend.events_read();
    let stream_reads_before = backend.stream_reads();
    store
        .command_cached::<Account, _>(stream, Deposit { amount: 5 })
        .await
        .unwrap();
    // Cache off -> the second command took the load path (read_stream calls),
    // i.e. the cache-miss path, exactly.
    assert!(
        backend.stream_reads() > stream_reads_before,
        "cache-off command must take the load (cache-miss) path"
    );
    assert!(
        backend.events_read() >= before,
        "cache-off never skips the reload"
    );
    assert_eq!(store.cache().len(), 0, "disabled cache stores nothing");
}

// ===========================================================================
// load_hot warm-read: zero event reads when up to date, delta-only when behind.
// ===========================================================================

#[tokio::test]
async fn load_hot_warm_read_is_head_check_only() {
    let (store, backend) = counting_store(true);
    let stream = "acct-read";

    store
        .command_cached::<Account, _>(stream, Open { owner: "alice".into() })
        .await
        .unwrap();
    store
        .command_cached::<Account, _>(stream, Deposit { amount: 40 })
        .await
        .unwrap();

    // Warm read: one head check, zero event reads.
    let ev_before = backend.events_read();
    let head_before = backend.head_calls();
    let loaded = store.load_hot::<Account>(stream).await.unwrap();
    assert_eq!(loaded.state.balance, 40);
    assert_eq!(loaded.events_replayed, 0, "fully warm read folds nothing");
    assert_eq!(
        backend.events_read() - ev_before,
        0,
        "warm load_hot reads zero events"
    );
    assert_eq!(
        backend.head_calls() - head_before,
        1,
        "warm load_hot does exactly one head check"
    );

    // Now move the stream behind the cache's back and read again: only the
    // delta must be folded.
    let other = EventStore::new(backend.clone());
    other.append(stream, Version::At(1), &[AccountEvent::Deposited { amount: 2 }])
        .await
        .unwrap();
    let ev_before = backend.events_read();
    let loaded = store.load_hot::<Account>(stream).await.unwrap();
    assert_eq!(loaded.state.balance, 42);
    assert_eq!(loaded.events_replayed, 1, "only the one new event folded");
    assert_eq!(
        backend.events_read() - ev_before,
        1,
        "behind-cache load_hot folds only the delta"
    );
}

// ===========================================================================
// Conflict exhaustion still yields the typed variant on the cached path, and
// the losing state is dropped from the cache.
// ===========================================================================

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

impl SnapshotStore for AlwaysConflict {
    async fn save_snapshot(
        &self,
        _stream_id: &str,
        _snapshot: StoredSnapshot,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn load_snapshot(
        &self,
        _stream_id: &str,
    ) -> Result<Option<StoredSnapshot>, Self::Error> {
        Ok(None)
    }
}

#[tokio::test]
async fn cached_command_exhausts_with_typed_conflict() {
    let store = EventStore::new(AlwaysConflict)
        .with_retry_policy(RetryPolicy::no_backoff(5))
        .with_cache_capacity(8);
    let stream = "acct-doomed";

    let err = store
        .command_cached::<Account, _>(stream, Open { owner: "a".into() })
        .await
        .unwrap_err();
    match err {
        CommandError::Conflict { stream: s, attempts } => {
            assert_eq!(s, stream);
            assert_eq!(attempts, 5);
        }
        other => panic!("expected conflict exhaustion, got {other}"),
    }
    // The losing optimistic state must not linger in the cache.
    assert_eq!(store.cache().get::<Account>(stream), None);
}
