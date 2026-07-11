# mess

> **⚠️ Under heavy development.** APIs, on-disk formats, and crate boundaries
> all change without notice, and there are no migration guarantees between
> revisions yet. Read the code and the specs, not just this page.

An embedded event store for Rust: an append-only, crash-recoverable log with
event-sourcing ergonomics on top. You define events, aggregates, and command
handlers; `mess` gives you durable appends, optimistic concurrency, replay,
live subscriptions, snapshots, and an operational CLI — in-process, over a
directory, no server.

```rust
use mess_store::{EventStore, LogEngine};

let store = EventStore::new(LogEngine::open(dir)?);

// load -> decide -> append, with bounded jittered retry on write races.
store.command::<Account, _>("account-alice", Open { owner: "alice".into() }).await?;
store.command::<Account, _>("account-alice", Deposit { amount: 100 }).await?;

// Business rules reject with the aggregate's own typed error:
match store.command::<Account, _>("account-alice", Withdraw { amount: 1_000 }).await {
    Err(CommandError::Domain(AccountError::InsufficientFunds { .. })) => { /* ... */ }
    other => { /* ... */ }
}
```

The domain side is three concepts, mostly derived:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "account", version = 1)]
enum AccountEvent { Opened { owner: String }, Deposited { amount: u64 }, /* ... */ }

#[derive(Debug, Default, Clone, Aggregate)]  // you write one `apply` method
#[aggregate(event = AccountEvent)]
struct Account { balance: u64, open: bool }

impl Decide<Deposit> for Account {           // pure, sync, no I/O
    type Rejection = AccountError;
    fn decide(&self, cmd: Deposit) -> Result<Vec<AccountEvent>, AccountError> { /* ... */ }
}
```

## Try it

```sh
just demo          # seeds a deterministic twitter clone and serves it on :3000
cargo run -p bank --example bank            # smallest end-to-end walkthrough
```

The flagship example is [`examples/social`](examples/social) — a working,
server-rendered twitter clone (posting, deleting, following, liking) with its
own README covering the domain model, projections with checkpointed resume,
a rebuild-from-log proof, seed tiers up to ~55k events, and an ops tour with
real CLI output and measured numbers. It exists both to show the API and to
find its rough edges; most of the store's recent DX features were extracted
from building it.

## What's inside

| Crate | Role |
|---|---|
| `mess-core` | The trait vocabulary: `Event`, `Aggregate`, `Decide`, typed `CommandError`, `Actor` |
| `mess-derive` | `#[derive(Event)]` / `#[derive(Aggregate)]` — wire names, codecs, fold wiring |
| `mess-log` | The durable log: segmented append, group commit, background sealing, recovery, watermarks |
| `mess-index` | Hot + sealed indexes (BinaryFuse16 membership, columnar payloads), registry / meta tables (fjall) |
| `mess-store` | `EventStore` facade over a `Backend`: `load` / `append` / `command` / `command_cached`, state cache, snapshots, subscriptions; `LogEngine` composes log + index into the production backend |
| `mess-cli` | `mess inspect / doctor / verify (--repair) / backup / restore / retention` against a store directory |
| `mess-testkit` | Given-When-Then aggregate testing, self-sweeping real-fs temp dirs |
| `mess-bench` / `mess-soak` | Performance envelope + regression ratchet; long-running crash/soak harness |

(`mess`, `mess_db`, and `mess_ecs` are retired earlier iterations kept for
reference; new code should not depend on them.)

## Design posture

- **The log is the truth.** Aggregates, read models, caches, and snapshots are
  all derived state and can be rebuilt from the log byte-for-byte — the social
  example proves this on every run of its `--rebuild` mode.
- **Durability is explicit.** Three modes (`Process` / `Os` / `Group`) with
  documented crash contracts ([`docs/spec/03-durability.md`](docs/spec/03-durability.md));
  fsync latency is measured and alarmed at runtime, and write barriers only
  exist where the selected contract requires them.
- **Crash-safety is tested, not asserted.** The commit/recovery kernel has a
  formal model ([`docs/spec/formal-model-commit-recovery.md`](docs/spec/formal-model-commit-recovery.md)),
  and CI runs miri, loom, fuzzing, deterministic-simulation, torn-write,
  SIGKILL, and differential-model harnesses plus a performance-regression
  ratchet ([`docs/verification.md`](docs/verification.md)).
- **Accelerators need evidence.** Performance features are admitted by
  benchmark, and refusals are recorded with their numbers
  ([`docs/perf/`](docs/perf), [`spikes/`](spikes)).
- **One stream = one consistency boundary.** Cross-aggregate invariants use
  accept-and-reconcile or sagas
  ([`docs/cross-aggregate-invariants.md`](docs/cross-aggregate-invariants.md));
  unbounded crowds (likes, follows) belong in per-relationship streams, not in
  aggregate state — see the social README's "at scale" section for the
  measured why.

## Performance, roughly

Numbers move week to week (see the caveat up top) and are workstation
measurements, not promises: ~9 bytes/event on disk after columnar compression;
warm-path commands (cached fold, no reads) in the tens of microseconds;
snapshot-accelerated cold loads instead of full replays; new-stream creation
no longer pays a per-stream fsync outside the durable modes. Methodology and
current figures live in [`docs/perf/envelope.md`](docs/perf/envelope.md),
[`docs/perf/bulk-writes.md`](docs/perf/bulk-writes.md), and the social README.

## Development

```sh
cargo test -p <crate>            # suites are scoped per crate
cargo +nightly fmt --all         # rustfmt config uses nightly options
cargo clippy --workspace --all-targets -- -D warnings
```

Real-filesystem test suites create their stores through
`mess_testkit::sweeping_temp_dir` (self-cleaning, namespaced under
`$TMPDIR/mess-tests/`); see [`docs/testing.md`](docs/testing.md). Design docs
live in [`docs/`](docs), format and recovery specs in
[`docs/spec/`](docs/spec), and research notes with raw benchmark data in
[`spikes/`](spikes) and [`notes/`](notes).
