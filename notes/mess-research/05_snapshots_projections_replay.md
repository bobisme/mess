# 05 — Snapshots, projections, and replay

## Requirement

The target requirement is:

> Instantly retrieve the latest snapshot and the remaining unsnapshotted messages.

This should be an explicit storage primitive, not a convention.

## Snapshot tables

```text
snapshot_head:
  stream_id -> SnapshotRef

snapshot_by_version:
  stream_id + version -> SnapshotRef

snapshot_blob_index:
  snapshot_id -> BlobPtr
```

```rust
struct SnapshotRef {
    stream_id: StreamId,
    stream_version: u64,
    global_position: u64,
    snapshot_id: SnapshotId,
    blob_ptr: BlobPtr,
    codec: CodecId,
    schema_version: u32,
    state_hash: [u8; 32],
    fold_hash: [u8; 32],
    created_at_unix_nanos: u64,
}
```

Load:

```text
ref = snapshot_head[stream_id]       // O(1)
state = read_snapshot(ref.blob_ptr)  // one blob read
tail = read_stream(stream_id, ref.stream_version + 1)
state' = fold(state, tail)
```

## Snapshot as fold checkpoint

Given:

```text
E* = event sequence monoid
S  = state space
apply: S × E -> S
fold: S × E* -> S
```

The snapshot at stream version `v` stores:

```text
snapshot.state = fold(initial_state, events[0..v])
```

Then for any suffix `events[v+1..n]`:

```text
fold(snapshot.state, events[v+1..n])
= fold(initial_state, events[0..n])
```

because folding is associative over sequence concatenation.

This is the core validity proof for snapshot + tail replay.

## Fold certificate

Store a rolling hash chain per stream:

```text
fold_hash[-1] = H("mess-stream", stream_id)
fold_hash[i]  = H(fold_hash[i-1], frame_hash[i], stream_version=i)
```

Snapshot stores `fold_hash[v]`.

On load, the engine can verify:

```text
snapshot.fold_hash == stream_index.fold_hash_at(version)
```

Then tail replay continues:

```text
h = snapshot.fold_hash
for event in tail:
  h = H(h, event.frame_hash, event.stream_version)
```

This gives a cheap proof that the snapshot corresponds to exactly the committed prefix through version `v`.

## Snapshot write path

Snapshot writes should not block event appends.

```text
1. aggregate/projection loads/replays to version v
2. snapshot policy decides to snapshot
3. snapshot blob written asynchronously
4. validate stream_head.version >= v
5. insert snapshot_by_version
6. compare-and-swap snapshot_head if old.version < v
```

Rules:

```text
snapshot_head only advances monotonically
snapshot version must correspond to committed event
snapshot corruption must not poison snapshot_head permanently
older snapshots retained by policy
```

## Snapshot policy

Do not snapshot every N events blindly. Use cost.

```rust
struct SnapshotPolicy {
    min_events_since_snapshot: u64,
    min_bytes_since_snapshot: u64,
    max_replay_latency: Duration,
    hotness_factor: f64,
}
```

Useful trigger:

```text
expected_replay_cost(stream) = tail_events * avg_apply_cost + tail_bytes / read_bandwidth
snapshot if expected_replay_cost > target
```

Hot aggregates should snapshot more aggressively than cold aggregates.

## Adaptive snapshot scheduling

Borrow the access-pattern idea from FASTER’s HybridLog: hot records receive special treatment while cold data remains immutable/cold [FASTER](https://www.microsoft.com/en-us/research/publication/faster-a-concurrent-key-value-store-with-in-place-updates/).

For Mess:

```text
hot stream:
  keep aggregate state cache
  snapshot frequently enough to cap replay

warm stream:
  snapshot based on tail length/bytes

cold stream:
  snapshot only on explicit demand or retention boundary
```

## Projection model

A projection is just a fold over one or more streams/categories:

```rust
trait Projection {
    type Event;
    type State;

    fn apply(&mut self, event: Self::Event) -> Result<()>;
    fn checkpoint(&self) -> Cursor;
}
```

Store checkpoints:

```text
projection_checkpoint:
  projection_name -> {
    cursor,
    state_ref,
    input_frontier,
    output_version,
    fold_hash
  }
```

## Projection consistency

Kurrent/EventStore projections are useful but warn about write amplification: system projections can produce extra events per appended event, multiplying writes [Kurrent projections](https://docs.kurrent.io/server/v25.0/features/projections/). This is a caution for Mess.

Prefer:

```text
projection state as external/read-model database
projection checkpoint in Mess
optional projection-output events only when explicitly requested
```

Do not automatically emit projection events for everything.

## Incremental view maintenance

DBSP gives a general model for incremental view maintenance over rich stream computations, including relational queries, grouping, aggregation, recursion, and streaming aggregation [DBSP](https://arxiv.org/abs/2203.16684).

This is not a v1 implementation requirement. But it suggests a future “projection compiler” path:

```text
event category stream -> typed relation changes -> incremental view -> checkpoint
```

The DX dream:

```rust
#[projection(input = "post")]
fn visible_posts(events: Stream<PostEvent>) -> Table<PostId, VisiblePost> {
    ...
}
```

The storage engine only needs reliable ordered inputs and checkpoints.

## Replay planner

Replay should be adaptive:

```text
aggregate load:
  snapshot + stream tail

projection catch-up:
  category index scan from checkpoint

global subscription:
  segment scan from cursor

cold restore:
  segment scan + index rebuild
```

Planner hints:

```text
if reading many events from same segment:
  sort/coalesce reads by segment offset

if tail is tiny:
  random ptr reads are fine

if stream is dense in recent active segment:
  scan active segment and filter stream_id

if category is dense globally:
  global scan may beat category pointer chasing
```

## Snapshot compaction

Retention policy:

```text
keep_latest: 1..N
keep_every: duration/version interval
keep_until: all projections beyond snapshot.version
```

Do not delete an event segment if it contains events needed by:

```text
oldest projection checkpoint
oldest active subscriber cursor
snapshot rebuild/fallback policy
replication follower cursor
retention policy
```

Expose blockers:

```text
mess retention explain
  segment 00042 retained because projection visible_posts at cursor 1234
```

## Snapshot corruption recovery

```text
load latest snapshot
if checksum/hash fails:
  mark snapshot suspect
  load previous snapshot
  replay from previous
  optionally rewrite latest snapshot
```

No single snapshot should be a hard dependency if the event log is retained.

## Recommended v1 snapshot feature set

```text
snapshot_head exact table
snapshot_by_version exact table
snapshot blob segments
CRC + fold_hash
monotonic head update
manual snapshot API
basic policy: every N events or M bytes
```

v2:

```text
adaptive policy by measured replay cost
background snapshot worker
projection state snapshots
retention-aware snapshot compaction
fold certificate verification API
```

