# 01 — Architecture thesis

## Summary

The store should be physically designed around this invariant:

> Event payload bytes are immutable and stored once. Everything else is an index, cache, checkpoint, filter, or proof over those bytes.

That means the canonical object is not a RocksDB key/value pair. It is an event frame in an append-only segment.

```text
mess/
  manifest
  events/
    0000000000000000.seg
    0000000001000000.seg
    0000000002000000.seg
  snapshots/
    0000000000000000.snap
    0000000001000000.snap
  index/
    meta.redb | meta.fjall | meta.rocks
```

The desired read paths are:

```text
read_global(cursor):
  cursor -> segment/offset -> sequential frame scan

read_stream(stream, version):
  stream index -> event pointers -> frame reads

load_aggregate(stream):
  snapshot_head[stream] -> snapshot blob
  stream index from snapshot.version + 1 -> tail event pointers
  apply tail
```

## Why not “just RocksDB”?

RocksDB is a strong general-purpose embedded LSM tree. It supports atomic multi-key updates via `WriteBatch`, and column families can logically partition data while sharing a WAL for atomic writes across column families [RocksDB transactions](https://github.com/facebook/rocksdb/wiki/Transactions), [RocksDB column families](https://github.com/facebook/rocksdb/wiki/Column-Families).

But immutable event payloads are a specialized workload:

```text
append once
read sequentially many times
never update
delete/compact only by retention policy
serve cursor-based subscribers
serve aggregate replay from snapshot + suffix
```

An LSM tree’s strengths are not free. WiscKey’s core observation is directly relevant: LSM designs suffer I/O amplification because compaction repeatedly reads, sorts, and rewrites data; WiscKey separates keys from values so the LSM stores only keys and values live in a log [WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf). For an event store, we can go further: store event bytes in a log from the start and index only pointers.

## Proposed engine: Meridian Log

I’ll name the architecture “Meridian Log” in this research pack. The name is just a handle.

```text
Meridian Log = append-only event segments
             + pointer indexes
             + O(1) snapshot heads
             + fold certificates
             + sealed-segment static accelerators
```

### Event frame

```rust
struct FrameHeader {
    magic: u32,
    format_version: u16,
    header_len: u16,
    total_len: u32,

    global_pos: u64,
    stream_id: u128,
    stream_version: u64,
    category_id: u64,
    event_type_id: u64,

    metadata_len: u32,
    data_len: u32,

    prev_stream_hash: [u8; 32],
    frame_hash: [u8; 32],
    header_crc32c: u32,
    payload_crc32c: u32,
}
```

Payload follows the header:

```text
metadata bytes
data bytes
optional extension blocks
```

A frame carries enough information to rebuild all indexes by scanning the segments.

### Pointers

```rust
struct EventPtr {
    segment_id: u64,
    offset: u64,
    len: u32,
    frame_hash_prefix: u64,
}
```

Indexes store `EventPtr`, not duplicated payload bytes.

### Indexes

```text
global_index:
  global_pos -> EventPtr

stream_index:
  stream_id + block_no -> PtrBlock<EventPtr>

category_index:
  category_id + block_no -> PtrBlock<(global_pos_delta, EventPtrDelta)>

stream_head:
  stream_id -> { latest_version, latest_global_pos, latest_event_ptr }

snapshot_head:
  stream_id -> SnapshotRef

dedupe:
  message_id -> CommitRef

projection_checkpoint:
  projection_name -> Cursor
```

A pointer block should amortize metadata overhead:

```rust
struct PtrBlock<const N: usize = 256> {
    stream_id: u128,
    base_version: u64,
    base_global_pos: u64,
    ptrs: [CompressedEventPtr; N],
    count: u16,
    block_crc32c: u32,
}
```

## The critical invariants

### I1. Event immutability

Once a committed frame is visible, its bytes never change.

### I2. Segment append order

Within one writer shard, committed global positions are strictly increasing.

### I3. Stream version order

For strict streams:

```text
append(stream, expected = v) succeeds only if stream_head.version == v
new event version = v + 1
```

For stream creation:

```text
expected = None succeeds only if no stream_head exists
new event version = 0
```

### I4. Atomic commit visibility

A commit is visible only if both are true:

```text
1. frame bytes are durable enough for the selected durability mode
2. manifest/index commit boundary has advanced
```

Tail garbage beyond the commit boundary is ignored during recovery.

### I5. Index rebuildability

Every persistent index can be destroyed and rebuilt from committed event frames plus committed snapshot blobs.

This is the safety valve that lets the storage engine be aggressive.

## Write path

```text
append_batch(stream, events, expected_version):
  1. resolve stream_id/category_id/event_type_id
  2. validate dedupe IDs
  3. validate expected stream version
  4. reserve global position range
  5. encode frames into active segment buffer
  6. write frames
  7. optionally fdatasync/group-fsync
  8. atomically update metadata index:
       global_index
       stream_index
       category_index
       stream_head
       dedupe
  9. publish cursor to subscribers
```

## Read path

### Global subscription

The fastest path is not a KV range scan. It is sequential frame scan.

```text
cursor -> {segment_id, offset, global_pos}
while wanted:
  read next frame
  verify len/checksum if needed
  yield frame
```

The global index is mostly for seek/resume, not the hot sequential path.

### Stream replay

```text
read_stream(stream_id, from_version):
  block = stream_index[stream_id, from_version / BLOCK_SIZE]
  ptrs = block.ptrs[from_version % BLOCK_SIZE..]
  read pointed frames
```

For small tails, issue vectored/random reads. For long stream replays, detect locality and switch to segment-order gather.

### Aggregate load

```text
load(stream):
  snap = snapshot_head[stream]
  state = decode_snapshot(snap.ptr)
  events = read_stream(stream, snap.version + 1)
  return fold(state, events)
```

This makes “latest snapshot + unsnapshotted messages” an O(1) metadata lookup plus a suffix scan.

## DX-oriented architecture

The store should expose high-level domain APIs over the low-level engine.

```rust
#[derive(Event)]
enum AccountEvent {
    Opened { owner: UserId },
    Deposited { amount: Money },
    Withdrawn { amount: Money },
}

#[derive(Aggregate)]
struct Account {
    balance: Money,
}

impl Decide<Withdraw> for Account {
    fn decide(&self, cmd: Withdraw) -> Result<Vec<AccountEvent>> {
        ensure!(self.balance >= cmd.amount);
        Ok(vec![AccountEvent::Withdrawn { amount: cmd.amount }])
    }
}
```

The ideal application code:

```rust
store
    .command(AccountId::new("acct_123"), Withdraw { amount })
    .await?;
```

The generated/hidden storage sequence:

```text
load snapshot -> replay tail -> decide -> append with expected version -> update snapshot policy/projection hooks
```

## Where this likely wins

Compared to payload-in-LSM designs:

1. No duplicated payload bytes across global and stream indexes.
2. Sequential global replay is a file scan, not iterator over key/value records.
3. Snapshots are first-class pointers, not conventions embedded in streams.
4. Segment files are copy/backup/replication-friendly.
5. Static sealed segments can get aggressive indexes/filters without affecting the active writer path.

## Where this can fail

1. Index crash consistency becomes your responsibility.
2. Random stream replay can become pointer-chasing if streams are interleaved heavily.
3. Snapshot corruption requires fallback strategy.
4. Segment GC/retention is a whole subsystem.
5. Filesystem and fsync behavior will dominate tail latency if you promise strong durability.

## Design stance

Start with a boring metadata engine — redb, Fjall, or RocksDB — but keep the canonical event log custom and stable.

The replaceable boundary:

```rust
trait EventLog {
    fn append_frames(&mut self, batch: EncodedBatch) -> Result<CommitRegion>;
    fn read_ptr(&self, ptr: EventPtr) -> Result<EventFrame>;
    fn scan_from(&self, cursor: LogCursor) -> Result<FrameStream>;
}

trait EventIndex {
    fn stream_head(&self, stream: StreamId) -> Result<Option<StreamHead>>;
    fn latest_snapshot(&self, stream: StreamId) -> Result<Option<SnapshotRef>>;
    fn stream_ptrs_from(&self, stream: StreamId, version: u64) -> Result<PtrStream>;
    fn commit(&mut self, index_batch: IndexBatch) -> Result<()>;
}
```

