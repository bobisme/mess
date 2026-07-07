# 09 — Implementation plan

## North star

```rust
store
    .command(AccountId::new("acct_123"), Withdraw { amount: Money::usd(50) })
    .await?;
```

Under the hood:

```text
load latest snapshot
replay tail
run decide
append events with expected version
update indexes
publish subscriptions
maybe snapshot
```

## Phase 0 — Rename the mental model

Avoid “ECS.” Use event-sourcing terms.

```text
Entity        -> AggregateId / StreamId
Component     -> Aggregate / Projection
ComponentStore -> AggregateStore
EventDB       -> EventStore
ApplyEvents   -> Apply
```

This matters. ECS suggests game-style component storage. The product is event-sourced domain state.

## Phase 1 — API-first prototype on current backend

Keep RocksDB/redb simple. Prove DX.

### Traits

```rust
pub trait Event: Sized + Send + Sync + 'static {
    const NAME: &'static str;
    const VERSION: u32;
}

pub trait EventCodec<E> {
    fn encode(event: &E) -> Result<Bytes>;
    fn decode(bytes: &[u8]) -> Result<E>;
}

pub trait Aggregate: Default + Send + Sync + 'static {
    type Event: Event;
    fn apply(&mut self, event: &Self::Event);
}

pub trait Decide<C>: Aggregate {
    fn decide(&self, command: C) -> Result<Vec<Self::Event>>;
}
```

### Store API

```rust
impl EventStore {
    pub async fn load<A: Aggregate>(&self, id: impl Into<StreamId>) -> Result<Loaded<A>>;

    pub async fn append<E: Event>(
        &self,
        stream: StreamId,
        expected: ExpectedVersion,
        events: impl IntoIterator<Item = E>,
    ) -> Result<Commit>;

    pub async fn command<A, C>(&self, id: StreamId, command: C) -> Result<Commit>
    where
        A: Aggregate + Decide<C>;
}
```

### Generated derives

```rust
#[derive(Event, Serialize, Deserialize)]
#[event(name = "account.opened", version = 1)]
struct AccountOpened { ... }

#[derive(Aggregate)]
#[aggregate(stream = "account")]
struct Account { ... }
```

## Phase 2 — Correct single-node semantics

Before custom storage, nail semantics.

Required behavior:

```text
append expected NoStream succeeds only once
append expected Exact(v) succeeds only at v
append with duplicate message_id is idempotent within dedupe window
read_stream returns exactly ordered events
read_global returns exactly committed global order
subscription resume from cursor never skips committed event
snapshot + tail equals full replay
```

Property tests:

```text
random append/read interleavings
wrong expected version cases
snapshot at random prefix + tail replay
crash injection around every commit step
idempotency under retry
```

## Phase 3 — Storage abstraction

Introduce traits without changing high-level API.

```rust
trait SegmentLog {
    fn append(&mut self, batch: EncodedBatch) -> Result<CommitRegion>;
    fn read(&self, ptr: EventPtr) -> Result<Frame>;
    fn scan(&self, cursor: LogCursor) -> Result<FrameStream>;
}

trait MetadataIndex {
    fn prepare_append(&self, batch: AppendIntent) -> Result<PreparedAppend>;
    fn commit_append(&mut self, commit: PreparedCommit) -> Result<()>;
    fn stream_ptrs(&self, stream: StreamId, from: u64) -> Result<PtrStream>;
    fn snapshot_head(&self, stream: StreamId) -> Result<Option<SnapshotRef>>;
}
```

## Phase 4 — Meridian v1 storage

Implement:

```text
events/*.seg
snapshots/*.snap
manifest
metadata index backend
```

### Segment file

```text
SegmentHeader
Frame*
SegmentFooter? // for sealed segments only
```

Active segment can lack footer until sealed.

### Metadata tables

```text
stream_head
snapshot_head
stream_ptr_block
global_seek_index
category_ptr_block
dedupe_recent
projection_checkpoint
```

### Commit protocol

Choose one and test brutally:

```text
A. data first, metadata second
B. metadata pending, data, metadata visible
```

Recommendation: A.

## Phase 5 — Snapshots

Implement:

```rust
store.save_snapshot::<A>(stream_id).await?;
store.load::<A>(stream_id).await?; // uses snapshot automatically
```

Basic policy:

```text
snapshot every N events or M bytes
manual opt-in per aggregate
```

Add fold hashes from the start even if verification is optional.

## Phase 6 — Subscriptions/projections

API:

```rust
store
    .subscribe_category::<PostEvent>("post")
    .from_checkpoint("visible-posts")
    .run(|event, ctx| async move { ... })
    .await?;
```

Checkpoint contract:

```text
handler success -> checkpoint after event
handler error -> do not advance
restart -> resume from checkpoint
```

Support:

```text
global subscription
category subscription
stream subscription
pull batches
backpressure
```

## Phase 7 — Sealed segment accelerators

Start with:

```text
segment summaries
sparse offset table
CRC footer
```

Then:

```text
Binary Fuse/Ribbon filters
stream/category local directories
optional learned offset models
```

Keep them optional and rebuildable.

## Phase 8 — Benchmark suite

Datasets:

```text
uniform stream distribution
Zipf stream distribution
hot aggregate/cold aggregate mix
large payloads vs tiny payloads
many categories
long-tail snapshots
projection catch-up
```

Metrics:

```text
append throughput by batch size
append p50/p95/p99 by durability mode
read_global MB/s
read_stream events/s by stream density
load_aggregate p50/p95/p99 by tail length
snapshot lookup latency
projection catch-up events/s
index size per event
payload write amplification
recovery time after crash
```

Compare:

```text
RocksDB payload duplicated baseline
RocksDB pointer-index + custom log
redb pointer-index + custom log
Fjall pointer-index + custom log
```

## Phase 9 — Crash/fault harness

You need this before claiming anything.

Fault points:

```text
before frame write
during frame write
after frame write before fsync
after fsync before metadata commit
during metadata commit
after metadata commit before ack
after ack
```

Assertions:

```text
no committed event lost under promised durability
no uncommitted event visible
indexes match log after recovery
snapshot_head never points to invalid snapshot
dedupe semantics preserved across retry
```

Implementation trick:

```text
inject failpoints into every storage step
run model checker / proptest-style random operation sequences
open/close/recover repeatedly
compare against in-memory reference model
```

## Phase 10 — Operational DX

CLI:

```text
mess doctor PATH
mess inspect PATH
mess dump-stream PATH account-123
mess verify PATH --full
mess rebuild-index PATH
mess snapshot-stats PATH
mess retention explain PATH
mess bench PATH
```

Metrics:

```text
append latency by durability mode
fsync latency
active segment size/age
oldest subscriber cursor
oldest projection checkpoint
snapshot tail length histogram
stream replay random-read count
index cache hit rate
segment filter false-positive estimate
```

## Hard no’s

Do not start with:

```text
distributed consensus
multi-region replication
custom async I/O engine
learned indexes in hot path
complex retention/scavenge
projection compiler
```

Do start with:

```text
excellent aggregate API
exact append/read semantics
custom immutable segment log
snapshot_head + tail replay
crash tests
benchmarks
```

## Suggested first milestone

A demo that feels like this:

```rust
#[derive(Event, Serialize, Deserialize)]
enum AccountEvent {
    Opened { owner: UserId },
    Deposited { amount: Money },
    Withdrawn { amount: Money },
}

#[derive(Default, Aggregate)]
#[aggregate(event = AccountEvent, stream = "account")]
struct Account {
    balance: Money,
}

impl Decide<Withdraw> for Account {
    fn decide(&self, cmd: Withdraw) -> Result<Vec<AccountEvent>> {
        ensure!(self.balance >= cmd.amount);
        Ok(vec![AccountEvent::Withdrawn { amount: cmd.amount }])
    }
}

store.command::<Account, _>(account_id, Withdraw { amount }).await?;
```

And a storage test proving:

```text
snapshot + replay tail == full replay
across 10,000 randomized crash/recovery cases
```

That combination is the project’s soul: lovable DX backed by serious storage correctness.

