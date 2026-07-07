# 02 — Storage engine landscape

## Decision table

| Candidate | Use it for | Avoid using it for | Why |
|---|---|---|---|
| Custom segmented log | canonical events, snapshots | complex mutable indexes | natural fit for append/read/replay |
| redb | metadata, stream heads, snapshot heads, pointer blocks | huge write-heavy LSM-like ingestion | pure Rust, ACID, MVCC, COW B-trees |
| Fjall | metadata/indexes with LSM-ish write load | canonical large payloads unless KV separation helps | safe Rust, LSM, range/prefix iteration, keyspaces |
| RocksDB | pragmatic high-performance index backend | pure-Rust story, minimal ops surface | battle-tested, atomic batches, CFs, tuning cost |
| SQLite | ultra-boring deployment, admin tooling | pure-Rust goal, high-throughput append log | operationally excellent, not physically ideal |
| “all custom” | maximum performance/learning | v0/v1 unless you want to debug storage for months | highest upside and highest blast radius |

## redb

redb is attractive for metadata/indexes because it is pure Rust, embedded, ACID, stores data in copy-on-write B-trees, supports MVCC with concurrent readers and one writer, and is crash-safe by default [redb docs](https://docs.rs/redb/latest/redb/).

This maps cleanly to:

```text
stream_head
snapshot_head
stream registry
category registry
projection checkpoints
small pointer-block records
manifest state
```

Potential issue: event-ingest throughput may bottleneck on a single writer transaction if pointer indexes are updated very frequently. That is tolerable initially because the event segment writer is also single-writer in v1. Benchmark before assuming.

## Fjall

Fjall is an embeddable log-structured KV store in safe Rust. It offers LSM-style range/prefix iteration, keyspaces analogous to RocksDB column families, cross-keyspace atomic semantics, optional serializable transactions, and optional KV separation [Fjall docs](https://docs.rs/fjall/latest/fjall/).

Good fit:

```text
stream_index as keyspace
category_index as keyspace
snapshot_head as keyspace
consumer_checkpoint as keyspace
```

Why it matters: if pointer-index writes dominate, an LSM-like metadata layer may beat a COW B-tree. But it reintroduces background maintenance, write amplification, and tuning.

## RocksDB

RocksDB remains the pragmatic baseline. It supports atomic multi-key writes with `WriteBatch`, and transactions add conflict checking beyond RocksDB’s default atomicity for multiple keys [RocksDB transactions](https://github.com/facebook/rocksdb/wiki/Transactions). Column families logically partition records while supporting atomic writes across CFs and a consistent view across them [RocksDB column families](https://github.com/facebook/rocksdb/wiki/Column-Families).

Use RocksDB if the first objective is getting correctness and performance quickly.

But do not store event payloads twice in RocksDB. Store pointers.

```text
bad:
  global_cf: full payload
  stream_cf: full payload again

good:
  event segments: full payload once
  global_cf: EventPtr
  stream_cf: PtrBlock
```

## WiscKey: key/value separation

WiscKey’s central move is key/value separation: keep keys in the LSM and values in a separate log, thereby reducing compaction amplification. The paper reports that separating keys from values made WiscKey much faster than LevelDB for loads/lookups and faster than both LevelDB and RocksDB across YCSB workloads [WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf).

The event-store adaptation is stronger:

```text
WiscKey:
  LSM stores keys -> pointer to value log

Mess/Meridian:
  metadata index stores stream/version/category -> pointer to immutable event frame
```

Because events are immutable and mostly not point-updated, we do not need value-log garbage collection for overwritten values. We only need retention/scavenge for expired streams/snapshots.

## FASTER: hybrid log and hot-path shaping

FASTER combines a cache-optimized concurrent hash index with a HybridLog spanning memory and storage. It allows hot records to be updated in-place while cold records use read-copy-update, shaping the log around access patterns [FASTER](https://www.microsoft.com/en-us/research/publication/faster-a-concurrent-key-value-store-with-in-place-updates/).

For Mess, do **not** mutate event frames in place. But steal the idea for caches:

```text
hot aggregate state:
  in-memory mutable cache

cold aggregate state:
  snapshot + tail replay

hot stream head/index blocks:
  in memory

sealed segment data:
  immutable, read-only, compact indexed
```

The pattern is “mutable hot cache, immutable cold truth.”

## CompassDB: perfect hashes for immutable pieces

CompassDB argues that SSDs shift bottlenecks from I/O to CPU and that LSM compaction/read/write amplification are bottlenecks. It uses two-tier perfect hashing with compact in-memory indexes; the paper reports ~6 bytes/key average index cost, O(1) lookup, and 2.5–4× throughput over RocksDB on YCSB [CompassDB](https://arxiv.org/abs/2406.18099).

This is highly relevant for sealed event segments:

```text
active segment:
  append-only mutable writer + simple in-memory maps

sealed segment:
  build static perfect hash / learned index / filters
  mmap or load compact directory
```

Perfect hashing is a good fit for immutable/sealed sets:

```text
segment-local stream_id -> pointer-list directory
segment-local message_id -> frame offset
segment-local event_type/category -> range blocks
```

Do not use perfect hashing for the active writer path unless you want rehash churn.

## Cascade Log: reference-stable handles

Cascade Log proposes reference-stable handles over tiered append sequences, with operations for resolving handles and ranges even as data moves across tiers [Cascade Log](https://arxiv.org/abs/2606.05467).

This is useful as inspiration for compaction/retention:

```text
EventPtr should stay valid until its retention epoch is gone.
Cursor should survive segment sealing and index rebuilding.
SnapshotRef should survive snapshot compaction via an indirection table or tombstone/fallback chain.
```

For Mess, this suggests separating:

```text
logical references:
  global_pos, stream/version, snapshot id

physical references:
  segment_id, offset, len
```

Logical refs are client-visible. Physical refs are engine-internal and may be rewritten under compaction.

## NVM / persistent memory: interesting, not v1

Recent NVM research such as FlintKV targets durable linearizable storage on byte-addressable non-volatile memory and reports throughput gains over prior engines [FlintKV](https://arxiv.org/abs/2607.02401). This is useful as a future direction, but commodity deployment should assume NVMe SSDs and filesystems first.

## Recommended backend strategy

### v0

Use one backend only:

```text
RocksDB or redb stores events and indexes
```

Purpose: API, semantics, tests.

### v1

Introduce custom segments:

```text
event payloads -> custom segments
metadata/indexes -> redb/Fjall/RocksDB
```

Purpose: storage shape correctness and performance.

### v2

Seal-time static indexes:

```text
sealed segment -> filters + learned/sparse/perfect indexes
active segment -> simple append and in-memory maps
```

Purpose: reduce memory and random read cost.

### v3

Pluggable index backends:

```rust
trait IndexBackend {
    fn get_stream_head(...);
    fn get_snapshot_head(...);
    fn write_commit_batch(...);
    fn range_ptr_blocks(...);
}
```

Purpose: compare redb/Fjall/RocksDB/custom without changing API.

