# 04 — Indexes, cursors, and filters

## Indexing principle

Indexes should answer navigation questions, not own event bytes.

```text
global cursor -> next frame
stream/version -> EventPtr
category/global -> EventPtr
message_id -> commit ref
stream -> latest snapshot
projection -> checkpoint cursor
```

## Global index

The global log is physically sequential. Optimize global replay as a scan, not as a lookup per event.

Use sparse seek metadata:

```text
segment directory:
  segment_id -> { min_global, max_global, file, base_offset }

offset table per segment:
  every K frames or every K bytes -> byte offset
```

Lookup:

```text
seek_global(pos):
  segment = upper_bound(segment.min_global <= pos)
  offset_hint = segment.offset_table.floor(pos)
  scan frames until global_pos == pos
```

For dense global positions, a segment-local offset array can be direct:

```text
offsets[global_pos - segment_base_global] -> relative byte offset
```

This is compact if event counts per segment are not massive. Use varint/delta encoding.

## Stream index

Stream replay is the hard path because one stream’s events are interleaved across global segments.

Use block-indexed event pointers:

```text
key = stream_id || block_no
value = PtrBlock {
  base_version,
  entries: [EventPtrDelta; N]
}
```

`N = 128, 256, 512` should be benchmarked.

### Compression

Most pointers in a block will often be near each other if a stream has bursts. Store deltas:

```rust
struct EventPtrDelta {
    segment_delta: i16,
    offset_delta: u32,
    len: u32,
    global_delta: u32,
}
```

Fallback to full pointer for outliers.

## Category index

Eventide-style stream categories are too valuable to fake with global scan filters.

```text
category: post
streams:
  post-123
  post-456
  post-789
```

Category subscribers want:

```text
read all post events after global cursor C
```

Index:

```text
category_id + category_block_no -> CategoryPtrBlock
```

Where entries are sorted by global position.

```rust
struct CategoryEntry {
    global_pos_delta: u64,
    stream_id: u128,
    stream_version: u64,
    ptr: EventPtrDelta,
}
```

## Opaque cursors

Do not expose physical segment offsets as a stable API. Return opaque cursors.

```rust
struct CursorV1 {
    kind: CursorKind,
    epoch: u64,
    shard: u32,
    global_pos: u64,
    segment_id: u64,
    offset: u64,
    fence_hash: u64,
}
```

Encoded as base64/URL-safe bytes.

### Why opaque?

Today:

```text
single node -> global_pos is enough
```

Tomorrow:

```text
partitioned cluster -> global_pos may be per-shard
compaction -> physical offset may move
retention -> cursor may expire
format migration -> cursor schema changes
```

Opaque cursors preserve upgrade paths.

## Cursor algebra

Single-node cursor order:

```text
c1 <= c2 iff c1.global_pos <= c2.global_pos
```

Distributed/sharded future:

```rust
type Frontier = BTreeMap<ShardId, GlobalPos>;

join(F, G)[s] = max(F[s], G[s])
```

This forms a join-semilattice:

```text
associative: join(a, join(b,c)) = join(join(a,b), c)
commutative: join(a,b) = join(b,a)
idempotent:  join(a,a) = a
```

That matters for consumer checkpoints: merging progress from concurrent workers should be mathematically safe.

## Causality clocks

Lamport clocks give a partial-order-compatible scalar: if `a happened-before b`, then `clock(a) < clock(b)`, but not the converse. Vector clocks capture causality more precisely but cost O(number_of_participants). Recent tree clocks reduce overhead by making join/copy proportional to modified entries and show 2–3× speedups in happens-before computations in benchmarks [Tree clocks](https://arxiv.org/abs/2201.06325).

For Mess:

```text
v1 strict streams:
  stream version is enough

future relaxed/causal streams:
  dotted version vectors or tree-clock-inspired frontiers
```

Do not store full vector clocks per event unless the user opts into causal/relaxed semantics.

## Segment filters

Filters answer: “Can this segment/block contain something I care about?”

Use them only to skip work. False positives are okay. False negatives are correctness bugs.

Targets:

```text
stream_id membership in segment
category_id membership in segment
event_type_id membership in segment
message_id dedupe membership
```

### Active segment

Use dynamic filters if needed:

```text
cuckoo filter
blocked bloom
hash set if small
```

Cuckoo filters support adding/removing dynamically and can use less space than Bloom filters for moderately low false-positive rates [Cuckoo Filter](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf).

### Sealed segment

Use static filters:

```text
Binary Fuse
Ribbon
Xor/Zor
```

Ribbon filters target static sets and can get below 10% overhead above the information-theoretic lower bound at some CPU cost [Ribbon filters](https://arxiv.org/abs/2103.02515). Binary Fuse filters are reported within 13% of the storage lower bound without sacrificing query speed and can be constructed over 2× faster than xor filters [Binary Fuse filters](https://arxiv.org/abs/2201.01174).

## Learned indexes

Learned indexes model key -> position in sorted data. Kraska et al. frame B-trees as models mapping keys to positions and report learned models up to 70% faster than cache-optimized B-trees with order-of-magnitude memory savings on several real datasets [The Case for Learned Index Structures](https://arxiv.org/abs/1712.01208).

For Mess, the best target is immutable sorted arrays:

```text
sealed segment global positions
sealed segment stream_id directory
sealed category pointer blocks
```

PGM-index is especially interesting because it provides guaranteed I/O-optimal queries and learns an optimal number of linear models [PGM-index](https://arxiv.org/abs/1910.06169).

RadixSpline is attractive because it is single-pass, error-bounded, and designed for sorted arrays; the paper explicitly calls out write-once/read-many settings such as LSM files [RadixSpline](https://arxiv.org/abs/2004.14541).

## Perfect hashing

For immutable segment-local lookup:

```text
stream_id -> directory entry
message_id -> frame pointer
snapshot_id -> blob pointer
```

Perfect hashes can beat trees because sealed segments have fixed key sets. CompassDB’s TPH result suggests SSD-era point lookup can be CPU/index dominated, and perfect hashing can dramatically reduce lookup overhead [CompassDB](https://arxiv.org/abs/2406.18099).

Do not force perfect hashing onto range scans. Pair it with sorted arrays/sparse indexes.

## Recommended index stack

### Always-on metadata

```text
stream_head:       exact KV
snapshot_head:     exact KV
projection_cursor: exact KV
dedupe_recent:     exact KV + retention window
```

### Active segment

```text
in-memory stream head cache
append-only pointer block builder
simple hash maps for active segment summaries
```

### Sealed segment

```text
segment footer:
  min/max global
  frame count
  offset table
  stream/category/event_type filters
  optional static stream directory
  optional learned/sparse index
```

### Query planner

Even event stores need a tiny planner:

```text
read_stream(stream, from):
  if tail small -> pointer reads
  if pointers cluster by segment -> coalesce by segment
  if stream dense in segment -> sequential segment scan with stream filter

read_category(category, from):
  if category index exists -> pointer blocks
  else -> global scan + filter, with warning metric
```

## Correctness rule

No approximate structure may decide existence negatively unless it is built from committed data and is guaranteed no-false-negative.

Filters can only say:

```text
no: definitely absent
maybe: check exact index/log
```

