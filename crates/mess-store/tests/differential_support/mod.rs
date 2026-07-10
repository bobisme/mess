//! bn-3az — the store-level differential harness support module.
//!
//! Random sequences of PUBLIC [`EventStore`] operations
//! (`command`/`append`/`load`/`load_cached`/`load_hot`/`save_snapshot`, plus a
//! simulated crash-recover-reopen) are executed **op by op** against BOTH the
//! real [`EventStore<MockBackend>`] and a trivially-correct in-memory
//! [`Model`] built from `HashMap<stream, Vec<Event>>` + naive fold. Every
//! return value — commit shape, error variant, folded state — is compared
//! after each op; the first divergence aborts with the seed and the minimal
//! failing op prefix (see [`run_sequence`]).
//!
//! # Design
//!
//! - **Domain**: the same bank-account aggregate as `cache.rs` (duplicated here
//!   — separate test binaries can't share a `tests/` module without a `#[path]`
//!   mod, and this one intentionally stands alone).
//! - **RNG**: a dependency-free splitmix64 generator (mirrors
//!   `mess_log::runtime::Rng`'s rationale: no external `rand` crate pulled into
//!   `mess-store` just for test-side sequence generation).
//! - **The model IS the oracle.** [`Model::append`]'s expected-version check is
//!   the ONLY place a "conflict" is decided; when a generated op wants a
//!   deliberately stale expected version, the stale value is resolved from the
//!   model's own tracked version (never the real store's) so a bug in the real
//!   store's version bookkeeping shows up as a genuine divergence rather than
//!   being absorbed into the resolution itself.
//! - **`command`'s `Conflict`-exhaustion variant is out of scope here.** This
//!   harness drives one sequential actor with no concurrency, and
//!   `command`/`command_cached` always reload immediately before deciding —
//!   there is no window for a real race, so `CommandError::Conflict` cannot
//!   arise from a `command` call in this harness (a divergence into that
//!   variant would itself be caught and reported, it just never legitimately
//!   fires). That variant IS covered: `cache.rs`'s
//!   `cached_command_exhausts_with_typed_conflict` exercises conflict
//!   exhaustion with a backend that always refuses. `AppendError::Conflict`
//!   (the raw, no-retry primitive `append` speaks) IS fully covered here via
//!   deliberately stale expected versions.
//! - **Crash-recover-reopen fidelity.** [`MockBackend`] is in-memory with no
//!   fault injection, so there is nothing to lose: "crash" is modeled as "acked
//!   state survives," exactly as the bone specifies. Concretely,
//!   `Op::CrashReopen` rebuilds a fresh `EventStore` over the SAME backend
//!   handle (dropping any hot-aggregate cache, as a real process restart would)
//!   while the model does nothing (its state IS the durable state). Real
//!   torn-write / partial-batch / power-loss recovery is the DST harness's job
//!   at the log layer (`crates/mess-log/tests/dst_scenarios.rs`
//!   `crates/mess-log/tests/crash_harness.rs`); this harness only proves that
//!   the store facade keeps serving correct results across a handle rebuild
//!   over already-durable state, and that the throwaway snapshot keyspace's
//!   persistence-across-reopen behaves as documented (`mock.rs`: wiped only
//!   when the backend itself is dropped).
//! - **`subscribe` is NOT exercised.** `EventStore` (this crate) has no
//!   `subscribe` method to drive — the subscription runtime lives one layer
//!   down in `mess-log` and is out of this crate's public API surface, so it is
//!   not "cheaply drivable against the mock" as the bone's optional clause
//!   allows for. Left out; not a gap in this harness's scope.
//! - **`events_replayed` and `attempts` are excluded from the compared
//!   `Outcome`** — both are implementation/perf details, not correctness, the
//!   same exclusion `cache.rs`'s own differential oracle makes for
//!   `events_read`. `events_replayed`'s exclusion is obvious (paging strategy
//!   is not observable-correctness). `attempts`' exclusion was a DIVERGENCE
//!   THIS HARNESS FOUND during development, not an a-priori design choice: the
//!   first version of this file compared `attempts` too, on the theory that a
//!   single-actor sequential harness has no concurrency, so every commit should
//!   need exactly one attempt. That is true for `append` and for
//!   `command`/`command_cached` in a vacuum — but `Op::AppendRaw` writes
//!   directly to a stream, bypassing the store's hot-aggregate cache entirely.
//!   When cache is ON and a later `command_cached` targets the SAME stream, its
//!   cached version is now genuinely stale (the raw append moved the stream out
//!   from under it) — the append conflicts, the delta catch-up fires, and
//!   `attempts == 2`. That's real, correct `command_cached` behavior (exactly
//!   what `cache.rs`'s `conflict_retry_fetches_only_the_delta` proves
//!   deliberately with two handles), not a bug — this harness produces the SAME
//!   effect for free with a single handle, because `AppendRaw` plays the role
//!   of "someone else moved the stream." The naive model has no cache to go
//!   stale, so its `attempts` is always 1; comparing the field was comparing an
//!   implementation detail, not a behavioral contract. Fixed by dropping
//!   `attempts` from `Outcome` entirely (state/version/error-shape is what the
//!   contract promises).

use std::collections::HashMap;
use std::fmt;

use mess_core::{Aggregate, CodecError, CommandError, Decide, Event};
use mess_store::backend::AppendError;
use mess_store::snapshot::{Snapshottable, StateCodecError};
use mess_store::{EventStore, MockBackend, StoreError, Version};

// ===========================================================================
// Dependency-free seeded PRNG (splitmix64). Same construction as
// `mess_log::runtime::Rng`; duplicated rather than depending on `mess-log`
// from `mess-store`'s dev-deps, keeping this crate's dependency graph
// unchanged.
// ===========================================================================

pub struct Rng {
    state: u64,
}

impl Rng {
    #[must_use]
    pub fn new(seed: u64) -> Self {
        // splitmix64 must never be seeded with 0 (0 is a fine seed for the
        // generator itself, but keep the same non-zero convention as
        // `mess_log::runtime::Rng` for consistency across the codebase).
        Rng { state: if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed } }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, n)`. `n` must be non-zero.
    pub fn below(&mut self, n: u64) -> u64 {
        debug_assert!(n > 0);
        self.next_u64() % n
    }

    pub fn chance(&mut self, p: f64) -> bool {
        (self.next_u64() as f64) / (u64::MAX as f64) < p
    }
}

// ===========================================================================
// Domain: a bank Account, same shape as cache.rs's (this test binary is
// independent, so it is duplicated rather than shared).
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountEvent {
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
        let amount = || -> Result<i64, CodecError> {
            let bytes: [u8; 8] =
                data.try_into().map_err(|_| CodecError::Decode {
                    event_name: name.to_string(),
                    source:     format!("expected 8 bytes, got {}", data.len()),
                })?;
            Ok(i64::from_le_bytes(bytes))
        };
        match name {
            "account.opened" => Ok(AccountEvent::Opened {
                owner: String::from_utf8(data.to_vec()).map_err(|e| {
                    CodecError::Decode {
                        event_name: name.to_string(),
                        source:     e.to_string(),
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
pub struct Account {
    pub open:    bool,
    pub balance: i64,
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
pub enum AccountError {
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
            AccountError::NonPositiveDeposit => {
                write!(f, "non-positive deposit")
            }
            AccountError::InsufficientFunds { balance, requested } => {
                write!(f, "insufficient: {balance} < {requested}")
            }
        }
    }
}

impl std::error::Error for AccountError {}

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
                balance:   self.balance,
                requested: cmd.amount,
            });
        }
        Ok(vec![AccountEvent::Withdrawn { amount: cmd.amount }])
    }
}

// ===========================================================================
// The model: a trivially-correct in-memory reference. `HashMap<stream,
// Vec<Event>>` plus naive fold — no snapshot acceleration, no paging.
// `load_cached` is modeled identically to `load` because that
// byte-identical-result equivalence is `EventStore`'s own documented contract
// (proven separately by `snapshot_law.rs`); this harness exists to catch a
// REAL implementation violating that contract, not to re-derive it.
//
// `command_cached`/`load_hot`, in contrast, get a DEDICATED mirror
// (`Model::command_cached`/`Model::load_hot` below, backed by `Model::hot`) —
// NOT modeled as identical to the uncached path. This is itself a divergence
// this harness found during development (see the module docs' "found during
// development" note above `Outcome`): a naive "cache-on behaves identically
// to cache-off" model is WRONG, because `Op::AppendRaw` can move a stream
// behind the hot cache's back, and — critically — a `decide` REJECTION using
// stale cached state returns that stale rejection WITHOUT ever touching the
// backend (there is no read or write on that path to discover the staleness).
// That is real, faithful `command_cached` behavior (`store.rs`'s own doc
// comment: "the version is proven by the append itself" — a promise that only
// covers the success path), so the model mirrors `StateCache`'s exact
// semantics (`cache.rs`: `get` clones without removing, `put` writes through
// on success or an empty decide, `invalidate` drops on conflict exhaustion)
// rather than pretending the hot path is cache-transparent.
// ===========================================================================

#[derive(Debug, Default, Clone)]
pub struct Model {
    streams:    HashMap<String, Vec<AccountEvent>>,
    /// Global position counter, dense across every stream — mirrors
    /// `MockBackend::Inner::global`'s assignment exactly (0-based, assigned in
    /// append order) so `last_global_position` is comparable byte-for-byte.
    global_len: u64,
    /// Mirrors `EventStore`'s `StateCache`: one warm `(version, state)` entry
    /// per stream. Only consulted/mutated by [`Model::command_cached`] /
    /// [`Model::load_hot`] — i.e. only when the caller is driving the
    /// cache-on configuration; a run with cache off never touches this field,
    /// matching a real disabled `StateCache` (always empty, by construction).
    hot:        HashMap<String, (Version, Account)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelCommit {
    pub version:         Version,
    pub events_appended: usize,
    pub last_global:     Option<u64>,
    pub attempts:        u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelConflict {
    pub expected: Version,
    pub actual:   Version,
}

impl Model {
    #[must_use]
    pub fn version(&self, stream: &str) -> Version {
        match self.streams.get(stream) {
            Some(events) if !events.is_empty() => {
                Version::At((events.len() - 1) as u64)
            }
            _ => Version::NoStream,
        }
    }

    #[must_use]
    pub fn state(&self, stream: &str) -> Account {
        let mut account = Account::default();
        if let Some(events) = self.streams.get(stream) {
            for event in events {
                account.apply(event);
            }
        }
        account
    }

    #[must_use]
    pub fn load(&self, stream: &str) -> (Account, Version) {
        (self.state(stream), self.version(stream))
    }

    /// The naive fold's append primitive: exact expected-version check, then
    /// commit — the same contract `Backend::append_batch` promises.
    pub fn append(
        &mut self,
        stream: &str,
        expected: Version,
        events: &[AccountEvent],
    ) -> Result<ModelCommit, ModelConflict> {
        let actual = self.version(stream);
        if actual != expected {
            return Err(ModelConflict { expected, actual });
        }
        if events.is_empty() {
            return Ok(ModelCommit {
                version:         actual,
                events_appended: 0,
                last_global:     None,
                attempts:        1,
            });
        }
        let entry = self.streams.entry(stream.to_string()).or_default();
        entry.extend(events.iter().cloned());
        self.global_len += events.len() as u64;
        let last_global = self.global_len - 1;
        let version = Version::At((entry.len() - 1) as u64);
        Ok(ModelCommit {
            version,
            events_appended: events.len(),
            last_global: Some(last_global),
            attempts: 1,
        })
    }

    /// `load -> decide -> append` with NO retry loop: this harness drives one
    /// sequential actor, so nothing can move the stream between the decide
    /// and the append below — a conflict here would mean the model itself
    /// mis-tracked a version, which would be its own bug, not a real one to
    /// simulate. See the module docs for why `command`'s `Conflict`-exhaustion
    /// variant is out of this harness's scope entirely.
    pub fn command<C>(
        &mut self,
        stream: &str,
        cmd: C,
    ) -> Result<ModelCommit, AccountError>
    where
        Account: Decide<C, Rejection = AccountError>,
    {
        let state = self.state(stream);
        let events = state.decide(cmd)?;
        let expected = self.version(stream);
        Ok(self.append(stream, expected, &events).expect(
            "model command is single-actor sequential; it cannot conflict",
        ))
    }

    /// Fold every event strictly after `from` (up to the current head) into
    /// `state` — the model's equivalent of `EventStore::replay_tail`. Always
    /// lands exactly on the current head, since nothing else can move the
    /// model between this call and its caller (single-threaded).
    fn replay_tail(
        &self,
        stream: &str,
        mut state: Account,
        from: Version,
    ) -> (Account, Version) {
        if let Some(events) = self.streams.get(stream) {
            let start = from.next_position() as usize;
            for event in events.iter().skip(start) {
                state.apply(event);
            }
        }
        (state, self.version(stream))
    }

    /// Mirrors `EventStore::command_cached`: warm hit -> `decide -> append`
    /// with no read; on a version conflict (the stream moved behind the
    /// cache's back — here, via `Op::AppendRaw`), fold ONLY the delta and
    /// retry. See the module docs above [`Model`] for why this is a
    /// dedicated mirror rather than delegating to [`Model::command`].
    pub fn command_cached<C: Clone>(
        &mut self,
        stream: &str,
        cmd: C,
    ) -> Result<ModelCommit, AccountError>
    where
        Account: Decide<C, Rejection = AccountError>,
    {
        let mut current = self.hot.get(stream).cloned();
        loop {
            let (version, state) = match current.take() {
                Some(warm) => warm,
                None => {
                    let (state, version) = self.load(stream);
                    (version, state)
                }
            };
            // A rejection returns HERE, with the cache untouched — exactly
            // `StateCache::get`'s non-removing semantics: the stale entry
            // (if any) is left exactly as it was.
            let events = state.decide(cmd.clone())?;
            if events.is_empty() {
                self.hot.insert(stream.to_string(), (version, state));
                return Ok(ModelCommit {
                    version,
                    events_appended: 0,
                    last_global: None,
                    attempts: 1,
                });
            }
            match self.append(stream, version, &events) {
                Ok(commit) => {
                    let mut folded = state;
                    for event in &events {
                        folded.apply(event);
                    }
                    self.hot
                        .insert(stream.to_string(), (commit.version, folded));
                    return Ok(commit);
                }
                Err(_conflict) => {
                    // Delta catch-up: fold only what moved since `version`,
                    // then retry. In this single-threaded harness the catch-up
                    // always lands exactly on the current head, so the retry
                    // always succeeds next iteration (mirrors the real store;
                    // conflict-EXHAUSTION cannot arise here — see module docs).
                    let (caught, caught_version) =
                        self.replay_tail(stream, state, version);
                    self.hot.insert(
                        stream.to_string(),
                        (caught_version, caught.clone()),
                    );
                    current = Some((caught_version, caught));
                }
            }
        }
    }

    /// Mirrors `EventStore::load_hot`: warm + up to date -> return as-is
    /// (zero fold); warm + behind -> delta catch-up; miss -> full load, then
    /// warm the cache.
    pub fn load_hot(&mut self, stream: &str) -> (Account, Version) {
        if let Some((version, state)) = self.hot.get(stream).cloned() {
            let head = self.version(stream);
            if head == version {
                return (state, version);
            }
            let (caught, caught_version) =
                self.replay_tail(stream, state, version);
            self.hot
                .insert(stream.to_string(), (caught_version, caught.clone()));
            return (caught, caught_version);
        }
        let (state, version) = self.load(stream);
        self.hot.insert(stream.to_string(), (version, state.clone()));
        (state, version)
    }

    /// Simulate a crash-recover-reopen: the durable `streams`/`global_len`
    /// survive (this IS the durable state — nothing to lose), but a fresh
    /// `EventStore` handle starts with an empty hot cache.
    pub fn crash_reopen(&mut self) { self.hot.clear(); }
}

// ===========================================================================
// The op vocabulary + seeded generator.
// ===========================================================================

#[derive(Debug, Clone)]
pub enum Op {
    Open {
        stream: usize,
        owner:  String,
    },
    Deposit {
        stream: usize,
        amount: i64,
    },
    Withdraw {
        stream: usize,
        amount: i64,
    },
    /// Direct `append` of raw (not `decide`d) events — the only op that can
    /// deliberately choose a STALE expected version, to exercise
    /// `AppendError::Conflict`.
    AppendRaw {
        stream: usize,
        events: Vec<AccountEvent>,
        stale:  bool,
    },
    /// `load` (cache off) / `load_hot` (cache on).
    Load {
        stream: usize,
    },
    /// Always `load_cached` (the snapshot-accelerated path), independent of
    /// the store's hot-cache setting.
    LoadCached {
        stream: usize,
    },
    SaveSnapshot {
        stream: usize,
    },
    /// Simulate a crash-recover-reopen: rebuild the `EventStore` handle over
    /// the same (surviving) backend. See the module docs' fidelity note.
    CrashReopen,
}

fn random_event(rng: &mut Rng) -> AccountEvent {
    match rng.below(3) {
        0 => AccountEvent::Opened { owner: format!("raw-{}", rng.below(9)) },
        1 => AccountEvent::Deposited { amount: (rng.below(100) as i64) + 1 },
        _ => AccountEvent::Withdrawn { amount: (rng.below(100) as i64) + 1 },
    }
}

/// Generate `n_ops` ops over `n_streams` streams from `seed`. Deterministic:
/// same seed -> same `Vec<Op>`, always.
#[must_use]
pub fn plan_ops(seed: u64, n_ops: usize, n_streams: usize) -> Vec<Op> {
    let mut rng = Rng::new(seed);
    let mut ops = Vec::with_capacity(n_ops);
    for _ in 0..n_ops {
        let stream = rng.below(n_streams as u64) as usize;
        // Weighted op mix: mostly domain commands (so most sequences build up
        // real balances), a healthy slice of raw appends (conflict coverage),
        // reads across all three read paths, occasional snapshots, and rare
        // crash-reopens (~5%, but over ~40 ops/sequence almost every sequence
        // hits at least one).
        let op = match rng.below(100) {
            0..=14 => {
                Op::Open { stream, owner: format!("owner-{}", rng.below(5)) }
            }
            // Deliberately includes some <=0 amounts (~1 in 11) to exercise
            // the `NonPositiveDeposit` rejection.
            15..=39 => {
                Op::Deposit { stream, amount: (rng.below(220) as i64) - 20 }
            }
            40..=59 => {
                Op::Withdraw { stream, amount: (rng.below(160) as i64) - 10 }
            }
            60..=69 => {
                let n = 1 + rng.below(3);
                let events = (0..n).map(|_| random_event(&mut rng)).collect();
                Op::AppendRaw { stream, events, stale: rng.chance(0.5) }
            }
            70..=79 => Op::Load { stream },
            80..=89 => Op::LoadCached { stream },
            90..=94 => Op::SaveSnapshot { stream },
            _ => Op::CrashReopen,
        };
        ops.push(op);
    }
    ops
}

/// Resolve `AppendRaw`'s stale flag against the model's ground-truth version:
/// `stale` picks a value guaranteed different from `correct`.
fn resolve_expected(correct: Version, stale: bool) -> Version {
    if !stale {
        return correct;
    }
    match correct {
        Version::NoStream => Version::At(0),
        Version::At(_) => Version::NoStream,
    }
}

// ===========================================================================
// Observable outcome: everything a caller could branch on, MINUS the two
// implementation/perf details `events_replayed` and a successful commit's
// `attempts` (see the module docs' "found during development" note for why
// `attempts` is excluded specifically). `CommandConflict`'s `attempts` is kept
// — that field is the whole POINT of the conflict-exhaustion variant, not an
// incidental retry count on an otherwise-successful commit.
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Commit {
        version:         Version,
        events_appended: usize,
        last_global:     Option<u64>,
    },
    Conflict {
        expected: Version,
        actual:   Version,
    },
    CommandConflict {
        attempts: u32,
    },
    Domain(AccountError),
    Loaded {
        state:   Account,
        version: Version,
    },
    SnapshotOk,
    Reopened,
}

fn map_command_result<E: std::error::Error>(
    res: Result<mess_store::Commit, CommandError<AccountError, StoreError<E>>>,
) -> Outcome {
    match res {
        Ok(c) => Outcome::Commit {
            version:         c.version,
            events_appended: c.events_appended,
            last_global:     c.last_global_position,
        },
        Err(CommandError::Domain(e)) => Outcome::Domain(e),
        Err(CommandError::Conflict { attempts, .. }) => {
            Outcome::CommandConflict { attempts }
        }
        Err(CommandError::Store(e)) => {
            unreachable!(
                "MockBackend is Infallible; store error impossible: {e}"
            )
        }
    }
}

fn map_append_result<E: std::error::Error>(
    res: Result<mess_store::Commit, AppendError<StoreError<E>>>,
) -> Outcome {
    match res {
        Ok(c) => Outcome::Commit {
            version:         c.version,
            events_appended: c.events_appended,
            last_global:     c.last_global_position,
        },
        Err(AppendError::Conflict { expected, actual }) => {
            Outcome::Conflict { expected, actual }
        }
        Err(AppendError::Backend(e)) => {
            unreachable!(
                "MockBackend is Infallible; store error impossible: {e}"
            )
        }
    }
}

// ===========================================================================
// The two executors: apply one op to the model / to the real store.
// ===========================================================================

fn apply_model(
    model: &mut Model,
    cache_on: bool,
    streams: &[String],
    op: &Op,
    resolved_expected: Option<Version>,
) -> Outcome {
    fn commit_outcome(c: ModelCommit) -> Outcome {
        Outcome::Commit {
            version:         c.version,
            events_appended: c.events_appended,
            last_global:     c.last_global,
        }
    }

    match op {
        Op::Open { stream, owner } => {
            let name = &streams[*stream];
            let res = if cache_on {
                model.command_cached(name, Open { owner: owner.clone() })
            } else {
                model.command(name, Open { owner: owner.clone() })
            };
            res.map_or_else(Outcome::Domain, commit_outcome)
        }
        Op::Deposit { stream, amount } => {
            let name = &streams[*stream];
            let res = if cache_on {
                model.command_cached(name, Deposit { amount: *amount })
            } else {
                model.command(name, Deposit { amount: *amount })
            };
            res.map_or_else(Outcome::Domain, commit_outcome)
        }
        Op::Withdraw { stream, amount } => {
            let name = &streams[*stream];
            let res = if cache_on {
                model.command_cached(name, Withdraw { amount: *amount })
            } else {
                model.command(name, Withdraw { amount: *amount })
            };
            res.map_or_else(Outcome::Domain, commit_outcome)
        }
        Op::AppendRaw { stream, events, .. } => {
            let name = &streams[*stream];
            let expected = resolved_expected
                .expect("AppendRaw always resolves an expected version");
            match model.append(name, expected, events) {
                Ok(c) => commit_outcome(c),
                Err(e) => Outcome::Conflict {
                    expected: e.expected,
                    actual:   e.actual,
                },
            }
        }
        Op::Load { stream } => {
            let name = &streams[*stream];
            let (state, version) =
                if cache_on { model.load_hot(name) } else { model.load(name) };
            Outcome::Loaded { state, version }
        }
        Op::LoadCached { stream } => {
            let name = &streams[*stream];
            let (state, version) = model.load(name);
            Outcome::Loaded { state, version }
        }
        Op::SaveSnapshot { .. } => Outcome::SnapshotOk,
        Op::CrashReopen => {
            model.crash_reopen();
            Outcome::Reopened
        }
    }
}

async fn apply_real<B: mess_store::snapshot::SnapshotStore + Clone>(
    store: &mut EventStore<B>,
    cache_on: bool,
    streams: &[String],
    op: &Op,
    resolved_expected: Option<Version>,
) -> Outcome {
    match op {
        Op::Open { stream, owner } => {
            let name = &streams[*stream];
            let res = if cache_on {
                store
                    .command_cached::<Account, _>(
                        name,
                        Open { owner: owner.clone() },
                    )
                    .await
            } else {
                store
                    .command::<Account, _>(name, Open { owner: owner.clone() })
                    .await
            };
            map_command_result(res)
        }
        Op::Deposit { stream, amount } => {
            let name = &streams[*stream];
            let res = if cache_on {
                store
                    .command_cached::<Account, _>(
                        name,
                        Deposit { amount: *amount },
                    )
                    .await
            } else {
                store
                    .command::<Account, _>(name, Deposit { amount: *amount })
                    .await
            };
            map_command_result(res)
        }
        Op::Withdraw { stream, amount } => {
            let name = &streams[*stream];
            let res = if cache_on {
                store
                    .command_cached::<Account, _>(
                        name,
                        Withdraw { amount: *amount },
                    )
                    .await
            } else {
                store
                    .command::<Account, _>(name, Withdraw { amount: *amount })
                    .await
            };
            map_command_result(res)
        }
        Op::AppendRaw { stream, events, .. } => {
            let name = &streams[*stream];
            let expected = resolved_expected
                .expect("AppendRaw always resolves an expected version");
            let res =
                store.append::<AccountEvent>(name, expected, events).await;
            map_append_result(res)
        }
        Op::Load { stream } => {
            let name = &streams[*stream];
            let loaded = if cache_on {
                store.load_hot::<Account>(name).await
            } else {
                store.load::<Account>(name).await
            }
            .expect("MockBackend is Infallible; load cannot fail");
            Outcome::Loaded { state: loaded.state, version: loaded.version }
        }
        Op::LoadCached { stream } => {
            let name = &streams[*stream];
            let loaded = store
                .load_cached::<Account>(name)
                .await
                .expect("MockBackend is Infallible; load cannot fail");
            Outcome::Loaded { state: loaded.state, version: loaded.version }
        }
        Op::SaveSnapshot { stream } => {
            let name = &streams[*stream];
            store
                .save_snapshot::<Account>(name)
                .await
                .expect("MockBackend is Infallible; save_snapshot cannot fail");
            Outcome::SnapshotOk
        }
        Op::CrashReopen => {
            // A genuine reopen consumes the backend (dropping the engine and
            // releasing its lock), so it cannot run behind `&mut store` here —
            // the driver loop ([`run_sequence_with`]) intercepts `CrashReopen`
            // and reopens the backend + store itself.
            unreachable!(
                "CrashReopen is handled by the run_sequence_with driver loop"
            )
        }
    }
}

/// Build a fresh [`EventStore`] over `backend` (a clone), cache on iff
/// `cache_on`.
fn build_store<B: mess_store::snapshot::SnapshotStore + Clone>(
    backend: &B,
    cache_on: bool,
) -> EventStore<B> {
    let mut store = EventStore::new(backend.clone());
    if cache_on {
        store = store.with_cache_capacity(64);
    }
    store
}

/// The genuine crash-reopen: drop the current store (releasing its backend
/// handle) FIRST, then reopen the backend over its own durable state, then
/// rebuild the store. Returns the reopened `(store, backend)`.
fn crash_reopen<B>(
    store: EventStore<B>,
    backend: B,
    cache_on: bool,
) -> (EventStore<B>, B)
where
    B: mess_store::snapshot::SnapshotStore + Clone + crate::common::Reopen,
{
    drop(store);
    let backend = backend.reopen();
    (build_store(&backend, cache_on), backend)
}

// ===========================================================================
// The differential driver.
// ===========================================================================

/// A single-op divergence, printable with the seed and the minimal failing
/// prefix (every op up to and including the divergent one).
#[derive(Debug)]
pub struct DivergenceReport {
    pub seed:          u64,
    pub cache_on:      bool,
    pub index:         usize,
    pub op_prefix:     Vec<Op>,
    pub model_outcome: Outcome,
    pub real_outcome:  Outcome,
}

impl fmt::Display for DivergenceReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "differential divergence: seed={} cache_on={} at op index {} (of \
             {} ops in the prefix)",
            self.seed,
            self.cache_on,
            self.index,
            self.op_prefix.len()
        )?;
        writeln!(f, "  model outcome: {:?}", self.model_outcome)?;
        writeln!(f, "  real  outcome: {:?}", self.real_outcome)?;
        writeln!(f, "  minimal failing op prefix:")?;
        for (i, op) in self.op_prefix.iter().enumerate() {
            writeln!(f, "    [{i}] {op:?}")?;
        }
        Ok(())
    }
}

/// Generate `n_ops` ops over `n_streams` streams from `seed`, then execute
/// them op-by-op against BOTH a fresh [`Model`] and a fresh
/// `EventStore<MockBackend>` (cache on iff `cache_on`), comparing after every
/// op. Returns `Ok(())` iff every op's outcome matched; on the FIRST
/// divergence, returns a [`DivergenceReport`] carrying the seed and the
/// minimal failing op prefix — this IS the harness's shrinking story (see
/// `harness_reports_the_first_divergent_op` in `differential_model.rs` for a
/// direct proof the mechanism finds the true minimal index).
pub async fn run_sequence(
    seed: u64,
    cache_on: bool,
    n_ops: usize,
    n_streams: usize,
) -> Result<(), DivergenceReport> {
    // The interim in-memory backend: kept for differential testing (bn-20b).
    run_sequence_with(MockBackend::new(), seed, cache_on, n_ops, n_streams)
        .await
}

/// The same differential sequence, driven against an arbitrary supplied
/// backend (bn-20b: run the identical Phase-1/2 differential suite against the
/// composed production engine as well as the interim `MockBackend`).
pub async fn run_sequence_with<B>(
    backend: B,
    seed: u64,
    cache_on: bool,
    n_ops: usize,
    n_streams: usize,
) -> Result<(), DivergenceReport>
where
    B: mess_store::snapshot::SnapshotStore + Clone + crate::common::Reopen,
{
    let plan = plan_ops(seed, n_ops, n_streams);
    let streams: Vec<String> =
        (0..n_streams).map(|i| format!("acct-{i}")).collect();

    let mut model = Model::default();
    let mut backend = backend;
    let mut store = build_store(&backend, cache_on);

    for (idx, op) in plan.iter().enumerate() {
        let resolved_expected = match op {
            Op::AppendRaw { stream, stale, .. } => {
                let correct = model.version(&streams[*stream]);
                Some(resolve_expected(correct, *stale))
            }
            _ => None,
        };

        let model_outcome =
            apply_model(&mut model, cache_on, &streams, op, resolved_expected);
        let real_outcome = if matches!(op, Op::CrashReopen) {
            // Genuine crash-reopen: drop the store + engine and re-open the
            // durable directory fresh (for the composed engine; a no-op handle
            // for the in-memory mock). The book is rehydrated from the log.
            let (s, b) = crash_reopen(store, backend, cache_on);
            store = s;
            backend = b;
            Outcome::Reopened
        } else {
            apply_real(&mut store, cache_on, &streams, op, resolved_expected)
                .await
        };

        if model_outcome != real_outcome {
            return Err(DivergenceReport {
                seed,
                cache_on,
                index: idx,
                op_prefix: plan[..=idx].to_vec(),
                model_outcome,
                real_outcome,
            });
        }
    }
    Ok(())
}

/// The index of the first differing element between two equal-length slices,
/// or `None` if they match (or `Some(min_len)` if the lengths differ). This is
/// the pure shrink-to-minimal-prefix logic `run_sequence` uses inline;
/// exposed separately so `differential_model.rs` can unit-test it directly
/// against a synthetic divergence (proving it finds the TRUE minimal index)
/// without needing to plant a bug in production code to do so.
#[must_use]
pub fn first_divergence<T: PartialEq>(a: &[T], b: &[T]) -> Option<usize> {
    let min_len = a.len().min(b.len());
    for i in 0..min_len {
        if a[i] != b[i] {
            return Some(i);
        }
    }
    if a.len() != b.len() { Some(min_len) } else { None }
}
