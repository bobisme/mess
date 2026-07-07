# 07 — Probabilistic and learned structures

## Rule zero

Approximate structures are accelerators only.

```text
They may cause extra work.
They must never cause missing committed events.
```

That means:

```text
false positive: OK
false negative: bug/corruption
```

## Membership filters

### Bloom filters

Classic approximate membership: compact, no false negatives if constructed correctly, false positives possible.

Problem: standard Bloom filters do not support deletion without variants like counting Bloom filters.

### Cuckoo filters

Cuckoo filters are useful for dynamic sets. The Cuckoo Filter paper reports support for add/remove, high performance, and lower space than Bloom filters when target false-positive rate is below about 3% [Cuckoo Filter](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf).

Use for:

```text
active segment stream/category membership
recent dedupe IDs
mutable in-memory “maybe touched streams”
```

### Ribbon filters

Ribbon filters target static sets and can approach the information-theoretic lower bound for false-positive filters. The paper states filters need at least `log2(1/f)` bits/key and that Ribbon can get below 10% overhead in many cases, even below 1% in an experimental load-balanced design [Ribbon filter](https://arxiv.org/abs/2103.02515).

Use for:

```text
sealed segment stream_id filter
sealed segment category_id filter
sealed segment event_type filter
```

### Binary Fuse filters

Binary Fuse filters are static filters reported within 13% of the storage lower bound without sacrificing query speed, and construction can be more than twice as fast as xor filters [Binary Fuse filters](https://arxiv.org/abs/2201.01174).

Use for sealed segments if implementation complexity is acceptable.

## Static segment filter layout

```text
segment footer:
  stream_filter_ref
  category_filter_ref
  event_type_filter_ref
  message_id_filter_ref
```

Query:

```text
if !stream_filter.maybe_contains(stream_id):
  skip segment
else:
  consult exact stream index or scan candidate block
```

## Count-min sketch and heavy hitters

Use sketches for adaptive optimization, not correctness:

```text
stream hotness
category hotness
event type frequency
snapshot scheduling
cache admission
```

A Count-Min Sketch can estimate frequencies cheaply but overestimates due to collisions. That is fine for “snapshot maybe sooner” and bad for “delete data.”

## HyperLogLog

Use for approximate cardinality:

```text
streams touched per segment
unique aggregates per category
projection fanout estimation
```

Again: metrics/planning only.

## Learned indexes

### The core idea

Learned indexes model the mapping from key to position in sorted data. The original learned-index paper frames B-trees as models that map lookup keys to positions and shows potential speed/memory improvements [Learned Index Structures](https://arxiv.org/abs/1712.01208).

### Why event stores are a good target

Sealed segments are immutable and sorted by global position. Many segment-local arrays are write-once/read-many:

```text
global_pos -> frame offset
stream_id -> stream directory position
category_id -> category directory position
snapshot_version -> snapshot blob offset
```

This is exactly where learned indexes are least scary: no online updates.

## PGM-index

PGM-index gives learned indexing with guarantees: I/O-optimal query operations and an optimal number of linear models [PGM-index](https://arxiv.org/abs/1910.06169).

Candidate use:

```text
segment global_pos -> offset table position
category global_pos -> category pointer block position
stream_id sorted directory -> directory entry
```

## RadixSpline

RadixSpline is single-pass, error-bounded, and competitive in lookup performance/size. It is designed for sorted arrays and the paper explicitly points to write-once/read-many structures such as LSM files [RadixSpline](https://arxiv.org/abs/2004.14541).

Candidate use:

```text
build while sealing segment:
  sorted keys stream_id/category/global_pos -> directory positions
```

The single-pass property is important because segment sealing should not become compaction hell.

## Perfect hashes

Perfect hashing maps a fixed set of keys to unique slots. Minimal perfect hashing maps `N` keys to `N` slots.

Use when:

```text
key set is immutable
lookup is point query
range scans are not required or separately indexed
```

Ideal segment-local targets:

```text
stream_id -> directory entry
message_id -> offset
snapshot_id -> blob pointer
```

CompassDB’s use of two-tier perfect hashing and reported compact indexes are strong evidence that perfect hashes are practical on SSD-era KV workloads [CompassDB](https://arxiv.org/abs/2406.18099).

## Learned filters?

Learned filters replace parts of Bloom-like structures with classifiers plus backup structures. They are interesting but probably not worth v1 complexity.

Reason:

```text
Ribbon/Binary Fuse already give extremely compact static filters with simple correctness stories.
```

Use learned range indexes before learned filters.

## Segment sealing pipeline

On active segment close:

```text
1. finalize frame high watermark
2. sort/build stream directory
3. build category directory
4. build offset table
5. build static filters
6. optionally build RadixSpline/PGM/perfect hash indexes
7. write segment footer atomically
8. publish segment sealed
```

The active segment path stays simple. Expensive static structures are built off the hot append path.

## Probabilistic dedupe

Dedupe must be exact inside the dedupe window if exactly-once append semantics are promised.

Use two layers:

```text
exact recent_dedupe table:
  message_id -> commit_ref

probabilistic maybe table:
  filters for old sealed segments to avoid exact lookup/scans
```

Do not let a probabilistic filter reject an append as duplicate. It can only trigger exact verification.

## Novel hybrid: Filter-of-directories

For each sealed segment:

```text
stream_filter says maybe stream exists
stream_perfect_hash maps stream_id -> stream_dir_entry
stream_dir_entry gives compressed pointer blocks for that stream within segment
```

This avoids a global pointer-block lookup when a stream has many events within a segment.

Read path:

```text
for segment in candidate_segments:
  if filter.no(stream): continue
  dir = perfect_hash.lookup(stream)
  read local ptr list
```

This may be faster for long stream replay than fetching one global stream-index block per 256 events.

## What to benchmark

```text
filter memory per segment
filter build time at segment seal
false-positive rate under real stream/category distributions
stream replay latency with/without segment filters
learned index size vs sparse offset table
perfect hash lookup latency vs BTree/hash map
CPU cost under p99 load
```

## Recommendation

v1:

```text
exact indexes only
simple segment summaries
maybe Bloom/Cuckoo for active dedupe/cache hints
```

v2:

```text
Binary Fuse or Ribbon filters for sealed segments
sparse offset tables
```

v3:

```text
RadixSpline/PGM for sealed sorted arrays
perfect hash stream directories
query planner using filter/selectivity stats
```

