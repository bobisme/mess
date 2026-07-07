# 08 — Novel mechanisms worth exploring

This file contains candidate mechanisms that are not merely “use paper X.” Some are straightforward compositions; some may be genuinely novel enough to test and possibly publish if the results are strong.

## 1. Meridian segments

### Idea

A segment is not just a log file. It is a sealed algebraic object:

```text
Segment = Frames
        + Offset model
        + Stream/category summaries
        + Static membership filters
        + Optional perfect-hash directories
        + Rolling integrity root
```

Active segments optimize appends. Sealed segments optimize reads.

### Why it matters

Most systems pick one physical shape. Meridian uses lifecycle stages:

```text
active:
  append-only, minimal index work

sealing:
  build expensive static accelerators once

sealed:
  mmap/read-optimized, compact, immutable

retained/cold:
  filters + sparse indexes, payload maybe tiered
```

### Validity

Because sealed accelerators are derived only from committed frames and are rebuildable, they cannot affect correctness if exact fallback exists.

```text
if accelerator says maybe -> verify exact frame/index
if accelerator missing/corrupt -> rebuild or fallback scan
```

## 2. Fold certificates

### Idea

Every stream has a rolling fold hash:

```text
h[-1] = H("mess", stream_id)
h[i]  = H(h[i-1], frame_hash[i], stream_version=i)
```

A snapshot stores:

```text
stream_id
stream_version
state_hash
fold_hash
```

### Load proof

To load from snapshot at version `v` and tail `v+1..n`, verify:

```text
snapshot.fold_hash == stream_index.fold_hash(v)
```

Then replay tail. The snapshot is proven to summarize the exact event prefix.

### Why it matters for DX

Expose:

```rust
let account = store.load_verified::<Account>(id).await?;
```

This gives users confidence that snapshots are not stale/mismatched/corrupt.

### Validity proof

Let:

```text
events = prefix ++ suffix
snapshot.state = fold(s0, prefix)
```

By fold associativity:

```text
fold(s0, events)
= fold(s0, prefix ++ suffix)
= fold(fold(s0, prefix), suffix)
= fold(snapshot.state, suffix)
```

The hash certificate proves the chosen `prefix` is the committed prefix through `v`.

## 3. Algebra-driven stream modes

### Idea

Make stream ordering a type-level/domain-level choice:

```rust
#[aggregate(strict)]
struct BankAccount;

#[projection(monotonic)]
struct LikesByPost;

#[aggregate(relaxed, crdt = "PNCounter")]
struct ViewCounter;
```

Strict streams use expected-version serialization. Relaxed streams require declared laws:

```text
commutative
associative
idempotent or causally ordered
invariant-confluent
```

### Why it matters

This creates a future distributed path without pretending all domain state can be coordination-free. CALM and invariant confluence provide the theory: monotonic programs can be coordination-free, and invariant confluence tells when coordination is necessary for application invariants [CALM](https://arxiv.org/abs/1901.01930), [Coordination Avoidance](https://arxiv.org/abs/1402.2237).

### Validity

For unordered application, require:

```text
apply(apply(s,a),b) = apply(apply(s,b),a)
```

For idempotent replay/dedupe:

```text
apply(apply(s,a),a) = apply(s,a)
```

For CRDT state:

```text
state forms join-semilattice
updates inflate state
merge is least upper bound
```

Those are checkable with generated property tests, and sometimes mechanically provable.

## 4. Cursor frontiers from day one

### Idea

Use a semilattice cursor even in single-node mode.

```rust
struct Frontier {
    positions: SmallVec<[(ShardId, Position); 1]>,
}
```

Single-node frontier is one pair.

Merge:

```text
join(F,G)[shard] = max(F[shard], G[shard])
```

### Why it matters

Consumer checkpoints, projection progress, replication acknowledgments, and future shard subscriptions all become the same abstraction.

### Validity

Pointwise max is associative, commutative, and idempotent. Therefore checkpoint merges are deterministic and safe under retries.

## 5. Seal-time perfect hash stream directories

### Idea

For each sealed segment, build:

```text
stream_filter: approximate membership
stream_mph: stream_id -> local directory slot
stream_directory[slot] -> compressed local pointer list
```

Stream replay can then skip global stream-index pointer blocks for dense old segments.

### Why it matters

Global stream indexes are good for point navigation. But long replay may benefit from segment-local locality:

```text
find which segments contain stream
read segment-local pointer list
read event frames in segment order
```

This trades small seal-time build cost for faster cold replay.

### Validity

The filter can only skip when guaranteed no false negatives. The perfect hash directory must verify stored key/fingerprint before use. Fallback to global stream index on corruption/mismatch.

## 6. Cost-based replay planner

### Idea

Use stats to pick replay strategy:

```text
stream tail length
events per segment
pointer locality
payload bytes
cache residency
filter selectivity
```

Plans:

```text
PtrChase:
  direct read each EventPtr

SegmentCoalesce:
  group EventPtrs by segment and offset order

SegmentScanFilter:
  scan candidate segments and filter stream/category

SnapshotOnly:
  if no tail events
```

### Why it matters

The fastest plan depends on distribution. A heavy stream with events clustered in segments is different from a sparse stream spread across years.

### Validity

All plans produce the same exact event set ordered by stream_version/global_pos. Planner can be tested by differential comparison against exact stream index.

## 7. Snapshot feedback controller

### Idea

Snapshot policy should be control-theoretic:

```text
target aggregate load p95 <= X ms
observe replay tail cost
increase/decrease snapshot frequency by stream hotness
```

Pseudo:

```text
error = observed_load_p95 - target
snapshot_interval = clamp(snapshot_interval * exp(-k * error), min, max)
```

### Why it matters

Static “snapshot every 100 events” is dumb. Some events are tiny, some apply slowly, some aggregates are hot.

### Validity

Snapshot frequency affects performance, not correctness. Correctness remains guaranteed by snapshot + tail replay.

## 8. Retention-aware fold certificates

### Idea

If old events are deleted after snapshot compaction, store a cryptographic certificate for the retained state boundary.

```text
retention_boundary:
  stream_id
  version
  fold_hash
  snapshot_ref
```

Then later the system can prove:

```text
state at boundary corresponds to historical prefix, even if prefix events are gone
```

### Why it matters

Event sourcing often says “never delete events,” then reality shows up. This gives a principled retention story.

### Validity caveat

This proves continuity/integrity, not semantic correctness of the snapshot implementation. Semantic correctness still requires trusted fold code, tests, and possibly reproducible snapshot builds before deletion.

## 9. Projection lineage fingerprints

### Idea

Each projection row can optionally store a compact lineage fingerprint:

```text
row_hash = H(previous_row_hash, event.frame_hash, operation_id)
```

This is not full provenance, but enough to debug:

```text
which projection version produced this?
which event range influenced this?
can I detect drift after replay?
```

### Why it matters

DX for projections usually sucks. “Why does this read model look wrong?” is a common pain.

### Validity

Fingerprints detect mismatch; they do not explain everything. Pair with optional debug mode that records full event IDs for sampled rows.

## 10. Static learned offset model with exact fallback

### Idea

For sealed segment global positions, store a RadixSpline/PGM model:

```text
model(global_pos) -> predicted offset-table index ± ε
```

Exact fallback searches within ε.

### Why it matters

Offset tables can be smaller than one offset per event while preserving bounded seek cost.

### Validity

Learned model does not decide correctness. It narrows search. Exact frame global_pos verification completes lookup.

## The most promising bundle

Build these together:

```text
Meridian segments
fold certificates
snapshot_head table
cursor frontiers
sealed segment filters
cost-based replay planner
```

Defer:

```text
relaxed CRDT streams
perfect hash directories
learned offset models
projection lineage fingerprints
retention certificates
```

The first bundle is implementable and immediately useful. The deferred set is research-y and should be protected by benchmarks/proofs.

