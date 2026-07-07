# Spike report: north-star DX on the existing RocksDB actor backend

Date: 2026-07-07. Crate: `spikes/dx_api` (standalone, opts out of the root
workspace). Everything runs against the real `mess_db` RocksDB actor
(`ActorHandle` / `put_message` / `fetch_messages`) — no in-memory fakes except
the Given-When-Then kit, which is deliberately store-free.

**Verdict: the north-star API works end-to-end on the existing backend.**
`store.command::<Account, _>(id, Withdraw { amount }).await?` with bounded
optimistic retry survives 8 concurrent writers on one stream with an exact
final balance. But the spike surfaced one real bug and a pile of API warts in
`mess_db` — list below.

## 1. What the calling code looks like

Library layer (`src/lib.rs`, `src/store.rs`, `src/testkit.rs`): plain traits,
no proc macros.

Domain code a user writes today (from `tests/bank_account.rs`, abridged —
every impl here is what derives would generate):

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    fn encode(&self) -> Result<Vec<u8>, CodecError> { /* serde_json */ }
    fn decode(name: &str, data: &[u8]) -> Result<Self, CodecError> { /* dispatch on name */ }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Account { open: bool, balance: i64 }

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
```

Usage — this is the actual test code, verbatim:

```rust
let store = EventStore::open(&tmp.0).unwrap();

store.command::<Account, _>("account-123", Open { owner: "alice".into() }).await?;
store.command::<Account, _>("account-123", Deposit { amount: 100 }).await?;
store.command::<Account, _>("account-123", Withdraw { amount: 30 }).await?;

// Overdraw rejected by the domain, not the store:
let err = store
    .command::<Account, _>("account-123", Withdraw { amount: 1_000 })
    .await
    .unwrap_err();
// err == CommandError::Domain("insufficient funds: balance is 120, requested 1000")

let loaded = store.load::<Account>("account-123").await?;
// loaded.state.balance == 120, loaded.version == Version::At(3)
```

Given-When-Then kit (pure in-memory, no store, no async):

```rust
AggregateTest::<Account>::given([
    AccountEvent::Opened { owner: "alice".into() },
    AccountEvent::Deposited { amount: 50 },
    AccountEvent::Withdrawn { amount: 20 },
])
.when(Withdraw { amount: 100 })
.then_error(DomainError::new("insufficient funds: balance is 30, requested 100"));
```

Mismatches panic with a positional diff (`gwt_mismatch_panics_with_diff`
asserts on the diff text):

```text
emitted events did not match
  [0] MISMATCH
        expected: Deposited { amount: 999 }
        actual:   Deposited { amount: 100 }
expected 1 event(s), got 1
```

## 2. Real test output (`cargo test --release`)

```text
     Running tests/bank_account.rs (target/release/deps/bank_account-74e9426a5634c2e2)

running 11 tests
test gwt_deposit_emits_deposited ... ok
test gwt_cannot_deposit_before_open ... ok
test gwt_cannot_open_twice ... ok
test gwt_open_emits_opened ... ok
test gwt_mismatch_panics_with_diff ... ok
test gwt_withdraw_within_balance ... ok
test gwt_overdraw_is_rejected ... ok
test append_enforces_expected_version ... ok
test bank_account_end_to_end ... ok
test command_exhausts_retries_with_typed_error ... ok
test concurrent_commands_on_one_stream_all_succeed ... ok

test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.13s
```

### The concurrency test (the point of the spike)

`concurrent_commands_on_one_stream_all_succeed`: 8 tokio tasks (multi-thread
runtime, 8 workers) each issue 25 `Deposit` commands against ONE stream
through `command()`. All 200 must succeed via optimistic retry; final balance
must be exact (`8 * 25 * 7 = 1400`); event count and stream version are also
asserted. Retry stats over six release runs (`--nocapture`):

```text
concurrency: 200 commands took 952 attempts (max 22 for a single command)
concurrency: 200 commands took 953 attempts (max 20 for a single command)
concurrency: 200 commands took 1006 attempts (max 20 for a single command)
concurrency: 200 commands took 1016 attempts (max 17 for a single command)
concurrency: 200 commands took 1001 attempts (max 23 for a single command)
concurrency: 200 commands took 979 attempts (max 24 for a single command)
```

So ~5x write amplification under 8-way contention, and a worst single command
needed 24 attempts. Two takeaways: (a) the retry loop genuinely works against
the real actor — zero lost updates, exact balance every run; (b) a default
retry budget of 16 (my first guess) is NOT enough at this contention level —
the test uses 64. A production `command()` wants jittered backoff and/or
server-side (actor-side) conditional append instead of client-side
load-decide-append loops. `command_exhausts_retries_with_typed_error` proves
exhaustion surfaces as typed `CommandError::Conflict { stream, attempts }`,
not a panic or a mystery DB error.

## 3. Friction found in the current mess_db API (every wart)

### Bug (confirmed empirically)

1. **`CACHED_GLOBAL` is a process-wide `static mut AtomicU64`**
   (`mess_db/src/rocks/write.rs:18`). The last-global-position cache is
   shared across *all DB instances in the process*. `examples/global_leak.rs`
   demonstrates it:

   ```text
   db1 6th append: global position = Some(6)
   db2 (fresh, empty db) FIRST append: global position = Some(7) (expected 1)
   ```

   A fresh, empty database starts numbering after whatever another database
   in the same process wrote. Any multi-store process (tests!) gets corrupt
   global ordering. It's also `static mut` with shared references — the
   compiler emits 11 `static_mut_refs` / rust-2024 UB warnings on every
   build. The cache must live inside `DB`.

### Concurrency / write-path warts

2. **No atomic multi-event append.** The actor writes one message per
   request, so appending N events is N chained expected-version writes
   (`EventStore::append` does exactly this). A concurrent writer landing
   between event 2 and 3 of a batch leaves a *partial append* on disk and a
   conflict error for the rest. `decide` returning multiple events is the
   normal case in event sourcing; this needs a batch write request (this is
   exactly D2 batch framing from doc 12, but the Phase-2 backend needs it
   too).
3. **No `ExpectedVersion::Any`.** `expected_stream_position: None` means
   "stream must be empty", `Some(v)` means "last message is exactly at v".
   There is no unconditional append at all — every writer is forced into
   read-before-write even when it doesn't care about versions.
4. **Expected-version convention is "position of the LAST message".**
   Off-by-one bait: empty stream = `None`, one event = `Some(Sequential(0))`.
   The spike had to wrap it in `Version::{NoStream, At(u64)}` to stay sane.
   A `NoStream / Exact(n) / Any` enum belongs in mess_db itself.
5. **`StreamPos::Relaxed` write path is `todo!()`** (`rocks/write.rs:160`) —
   and because it's inside the actor task, sending a Relaxed expected
   position doesn't return an error, it **panics the actor and kills the
   whole store**; every later call then hits the `unwrap`/`RecvError` paths.
   The bit-flag encoding also leaks: `Position.stream` must be unwrapped with
   `.position()` and re-wrapped as `Sequential` by every caller. Doc 12
   Phase 2 already says "decide fate of StreamPos bit-flag encoding and the
   unwired HLC clock" — confirmed, decide it.

### Read-path warts

6. **The actor ignores the stream-position option for stream reads.**
   `svc.rs` `handle_req` destructures `stream_pos` and never uses it — you
   cannot read a stream from a position through the actor. Combined with the
   hard `LIMIT_MAX = 10_000`, an aggregate with >10k events **cannot be fully
   loaded** (and there is no pagination to work around it). This also blocks
   snapshot-plus-tail replay, which Phase 1 needs (`load` = snapshot + tail).
   The spike's `load()` carries a warning comment and just uses LIMIT_MAX.
7. **The actor ignores the stream filter for global reads** — the
   `RequestBody::GetGlobalMessages { stream, .. }` field exists and a
   filtered `Fetch` impl exists in `rocks/read.rs`, but `handle_req` drops
   the filter on the floor. Silent wrong results rather than unimplemented.
8. **`fetch_messages` returns `Result<Vec<Result<OwnedMessage>>>`** — a
   Result of Results; every caller writes the same double-unwrap loop.
9. **`fetch_messages` does `recv.await.unwrap()`** (`svc.rs:254`) — if the
   actor died (see wart 5), readers *panic* instead of getting an error.
   `put_message` gets this right (`recv.await?`); the two paths disagree.
10. **`run_actor` does `handle_req(req).await.unwrap()`** — any handler error
    kills the actor loop silently. Nothing restarts it; the store just goes
    dark.

### Type / ergonomics warts

11. **`WriteMessage.id: ident::Id` where `ident` is a git-only dependency
    that mess_db does not re-export.** Any consumer outside the workspace
    must add the identical `{ git = "..." }` line to construct a write at
    all. Re-export it (or generate ids inside the store).
12. **`metadata: Cow<'a, [u8]>` is not `Option`** on write, but reads return
    `Option<Vec<u8>>` (empty ⇒ `None`). Asymmetric; the spike writes `b""`.
13. **`Error` is stringly and non-comparable**: `WrongStreamPosition` carries
    `stream: String` and flag-stripped `Option<u64>`s; the enum is neither
    `Clone` nor `PartialEq`, so matching is the only option (fine) but
    conflict detection is `matches!` on a pattern rather than a method or a
    dedicated conflict type.
14. **`ActorHandle::new` spawns inside the constructor** — requires an
    ambient tokio runtime (constructor in sync context = silent panic), and
    there's no graceful shutdown: `kill()` cancels the token but queued
    requests then hit the panicking paths; dropping the handle leaks the
    in-flight queue. No flush/close.
15. **`GetMessages` typestate builder is clever but partial** — only some
    state combinations have `From<...> for RequestBody` impls, and as warts
    6–7 show, the ones that compile aren't all honored. The type-level
    ceremony gives an illusion of coverage the actor doesn't back up.

### Build/tooling friction (environmental, but real)

16. `librocksdb-sys 0.11 (RocksDB 8.1.1)` does not compile under GCC 13+
    (missing `#include <cstdint>`); the spike sets
    `CXXFLAGS = "-include cstdint"` in `.cargo/config.toml [env]`.
17. The repo root `.cargo/config.toml` hard-requires clang + `lld`
    (`-fuse-ld=lld`, `-Wl,--no-rosegment`); lld is not installed on this
    machine, and cargo *merges* parent rustflags (a child config can't remove
    them), so the spike ships an `ld-shim.sh` that strips the lld flags.
    Neither of these is a mess_db API problem, but anyone cloning the repo
    fresh hits both.

## 4. What a derive layer needs to eliminate

The manual code in `tests/bank_account.rs` is exactly the derive target.
Line-for-line:

- **`#[derive(Event)]`** on the event enum must generate: per-variant stable
  names from `#[event(name = "account.opened", version = 1)]` attrs (the
  manual impl's `name()` match), `encode` that serializes **variant payload
  only** (the spike serializes the whole enum via serde's tagging, which
  makes the stored `message_type` redundant with the JSON tag — a derive
  should make the name the *only* discriminator), `decode` dispatching on the
  stored name (the manual impl's name-list match), and the schema-version +
  upcaster hooks from Phase 1. Codec choice belongs here too (doc 12:
  postcard alone rejected for payloads).
- **`#[derive(Aggregate)]`** must generate: the `type Event` binding, stream
  category naming (`#[aggregate(stream = "account")]` -> `"account-{id}"` —
  the spike passes raw stream strings everywhere, which is error-prone),
  `fold_version = N` plus the **golden fold-drift test** (doc 12 D4), and
  eventually snapshot wiring.
- **`Decide` ergonomics**: the retry loop forces `C: Clone` because `decide`
  consumes the command. Either the trait takes `&C` (my recommendation — the
  spike note is in `store.rs`) or the derive generates the clone. Also:
  `DomainError(String)` should become an associated
  `type Rejection: Error` so domains keep typed errors; the string version
  was spike expedience.
- **Nothing in the test kit needs a derive.** `AggregateTest` already works
  generically off the plain traits; a derive would only add nicer test names
  and the generated fold-drift fixtures.
- **Not derive work, but prerequisite backend work** exposed by this spike,
  in priority order: fix wart 1 (the static), add atomic multi-event
  conditional append (wart 2), add `ExpectedVersion::{Any, NoStream,
  Exact}` (warts 3–4), honor read positions + pagination (wart 6), and make
  the actor error instead of panic (warts 5, 9, 10). The DX layer above is
  thin and worked on the first try; the sharp edges are all below it.

## Files

- `src/lib.rs` — `Event`, `Aggregate`, `Decide<C>`, `DomainError`,
  `CodecError`
- `src/store.rs` — `EventStore` (`load` / `append` / `command` with bounded
  optimistic retry), `Version`, `Loaded`, `Commit` (incl. `attempts`),
  `StoreError`, `CommandError`
- `src/testkit.rs` — `AggregateTest::given(..).when(..).then_events(..)` /
  `.then_error(..)` with positional diff panics
- `tests/bank_account.rs` — GWT tests, end-to-end bank account on a tempdir
  RocksDB, expected-version enforcement, typed retry exhaustion, and the
  8-task concurrency test
- `examples/global_leak.rs` — reproduces the `CACHED_GLOBAL` cross-database
  bug
- `.cargo/config.toml`, `ld-shim.sh` — machine workarounds (warts 16–17)
