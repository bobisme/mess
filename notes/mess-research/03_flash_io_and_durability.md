# 03 — Flash, I/O, and durability

## The physical workload

Mess has two dominant I/O modes:

```text
append:
  small-to-medium frames batched into large sequential writes

replay:
  global replay = sequential scan
  stream replay = pointer-guided reads, sometimes random, sometimes segment-local
```

This maps well to modern SSDs if the engine avoids unnecessary rewriting.

## Flash facts that matter

### 1. Sequential append still matters

SSDs have much better random I/O than disks, but writes still interact with internal erase blocks, FTL placement, garbage collection, and write amplification.

F2FS exists precisely because NAND flash benefits from flash-aware layout. The Linux docs describe F2FS as log-structured and designed to exploit NAND flash characteristics while addressing wandering-tree and cleaning overhead problems [F2FS docs](https://docs.kernel.org/filesystems/f2fs.html).

Relevant lessons:

```text
write hot and cold data separately
batch writes
avoid rewriting cold data
keep metadata update propagation bounded
make cleaning predictable
```

F2FS explicitly supports hot/warm/cold active logs and tries to align layout with flash operational units [F2FS docs](https://docs.kernel.org/filesystems/f2fs.html).

### 2. Zoned Namespace SSDs reveal the truth

ZNS SSDs expose zones that can be read in any order but must be written sequentially. The specification goal is to let the host align writes with device geometry, improving placement, throughput, QoS, and capacity [ZNS docs](https://zonedstorage.io/docs/introduction/zns).

A future Mess segment engine maps naturally to ZNS:

```text
segment == zone-like append region
seal segment == close zone
retention delete == reset zone
```

Do not require ZNS for v1. But if the file format already thinks in segments, a ZNS backend later is straightforward.

### 3. fsync is a product decision, not an implementation detail

JetStream’s docs are a useful warning: file streams flush writes synchronously, but by default they do not immediately `fsync`; the default sync interval is 2 minutes, while `sync_interval: always` is strongest and slowest [NATS JetStream docs](https://docs.nats.io/nats-concepts/jetstream).

Expose durability modes explicitly:

```rust
enum Durability {
    Buffered,          // OS page cache; fastest; crash may lose recent commits
    GroupCommit,       // fdatasync every interval/batch
    SyncOnCommit,      // fdatasync per commit/batch
    SyncOnAck,         // ack only after data + index boundary durable
}
```

Developer experience improves when users can say:

```rust
Store::open(path)
    .durability(Durability::GroupCommit { max_lag: Duration::from_millis(5) })
```

instead of discovering the policy in documentation footnotes.

## io_uring

io_uring lets applications and kernel share submission/completion rings, avoiding some syscalls and memory copies [io_uring paper](https://kernel.dk/io_uring.pdf). It supports linked operations and drain semantics for sequencing data writes before integrity operations, but drain can stall unrelated work if overused.

Recent DBMS work warns that io_uring does not automatically improve database performance; benefits require careful use of registered buffers, fixed files, batching, and avoiding naive integration [High-Performance DBMSs with io_uring](https://arxiv.org/abs/2512.04859).

Recommendation:

```text
v1:
  use normal pwritev/readv + fdatasync

v2:
  add optional io_uring backend behind trait

v3:
  registered buffers, fixed files, linked write+sync chains, separate rings for data/index/sync
```

Do not let io_uring infect the core correctness model.

## Segment sizing

Segment size should balance:

```text
large enough:
  efficient sequential I/O
  amortized static index/filter cost
  fewer file descriptors

small enough:
  quick recovery scan
  cheap retention deletion
  manageable static index build
  low worst-case corruption impact
```

Starting values to test:

```text
64 MiB
256 MiB
1 GiB
```

For each segment, store:

```text
segment header
frame stream
periodic frame-offset table
segment footer:
  commit high watermark
  frame count
  min/max global_pos
  stream/category summaries
  rolling hash root
  static filters/indexes or pointers to sidecar files
```

## Write barriers and commit protocol

A robust commit sequence:

```text
1. write data frames to active segment
2. write commit marker or update segment-local high watermark
3. fdatasync segment according to durability policy
4. update index metadata transaction
5. fdatasync index metadata according to durability policy
6. publish ack/cursor
```

Alternative:

```text
1. update index first with pending commit epoch
2. write data frames
3. sync data
4. flip commit epoch visible
```

The first model is easier to reason about if event log is authority.

## Recovery

Recovery should be deterministic and boring:

```text
open manifest
read last durable commit epoch
scan active segment from known safe offset
validate frame magic/len/header_crc/payload_crc
stop at first invalid/incomplete frame
rebuild missing index entries
truncate tail beyond safe offset if configured
```

Rules:

```text
bytes after first invalid frame are garbage
index entries pointing beyond commit boundary are ignored
snapshot_head must never point to a corrupt snapshot
if latest snapshot is corrupt, fall back to previous snapshot or full replay
```

## Checksums

Use two levels:

```text
frame CRC32C:
  cheap corruption detection for normal reads

cryptographic chain/Merkle root:
  optional integrity/audit/fold proof
```

CRC32C is for accidental corruption. Cryptographic hashes are for tamper evidence and fold certificates.

## mmap vs read

mmap is attractive for segment reads and static indexes. But mmap has page-fault tail latency and less explicit backpressure. Start with explicit reads; add mmap for sealed segment indexes and optionally for global replay.

Suggested split:

```text
active segment:
  pwrite/read, no mmap

sealed segment static metadata:
  mmap OK

large replay payload:
  readv/preadv first, mmap benchmarked later
```

## Hot/cold split

Borrow F2FS’s hot/warm/cold idea:

```text
hot:
  active event segment
  active stream index block cache
  recent snapshots
  stream heads

warm:
  recent sealed segments
  recent pointer blocks

cold:
  old segments
  old snapshots
  static filters only until needed
```

The engine can expose this operationally:

```text
mess inspect storage
  active segment age/size
  dirty index bytes
  oldest uncheckpointed projection
  retention-blocked segments
  fsync p50/p95/p99
  segment read locality
```

## What to measure

Do not benchmark only throughput. Measure:

```text
append p50/p95/p99 latency by durability mode
fsync p50/p95/p99
replay MB/s global sequential
stream replay events/s for random stream distribution
aggregate load latency: snapshot hit + tail length distribution
index write amplification
segment write amplification
crash recovery time by active segment size
retention deletion time
```

## Recommendation

Implement the first custom engine with:

```text
append-only segment files
explicit durability modes
CRC per frame
commit high watermark
metadata transaction in redb/Fjall/RocksDB
background segment sealing
no io_uring initially
```

Then benchmark io_uring and direct I/O only after the file format and failure semantics are locked.

