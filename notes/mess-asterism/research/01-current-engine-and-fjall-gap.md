# Research 01: current Mess engine, Fjall’s role, and the actual remaining gap

**Repository inspected:** `bobisme/mess` at `43e4aca0192f01bb47670627f41182bca182759e`  
**Purpose:** identify the costs a custom engine can actually remove, without blaming Fjall for work it does not perform.

## 1. Executive finding

The current Mess engine has already moved the high-volume event and pointer paths out of Fjall:

```text
canonical payloads    -> mess-log segments
active pointers       -> in-memory ActiveIndex
sealed pointers       -> .pidx sidecar
sealed payloads       -> .pcol sidecar
segment membership    -> static filter sidecar
exact metadata        -> Fjall MetaStore
```

Accordingly, a replacement generic KV engine can improve only a fraction of the composed path. The larger opportunity is to remove the boundary between the canonical log and derived current state.

The highest-value changes, in order, are:

1. stop reconstructing and retaining the all-history `Book`;
2. make one committer own validation and publication, removing the per-append synchronization chain;
3. derive heads/registry/dedupe from canonical capsule bytes, removing the post-commit Fjall batch;
4. install a log-anchored kernel checkpoint so open is not event-count proportional;
5. replace immutable-segment `HashMap` reconstruction with a packed static directory only if a measured candidate wins.

## 2. What Fjall currently does

The `MetaStore` owns these keyspaces:

| keyspace | logical operation | authoritative? | current persistence |
|---|---|---:|---|
| `stream_heads` | stream ID -> version/global position | no; derived from log | Fjall buffered journal |
| `snapshot_heads` | stream ID -> snapshot pointer | intended rebuildable | Fjall buffered journal |
| `checkpoints` | projection ID -> position | operational state | Fjall buffered journal |
| `dedupe` | stream+key -> position/sequence | derived if keys are in log | Fjall buffered journal |
| `dedupe_order` | sequence -> primary key | implementation index | Fjall buffered journal |
| `hw` | per-table high-water | implementation index | same batch as rows |
| `stream_names` | numeric ID -> string | **currently authoritative** | explicitly `SyncAll` on first use |
| `type_names` | numeric ID -> string | **currently authoritative** | explicitly `SyncAll` on first use |

The implementation batches all normal metadata changes per commit group. That is already the correct way to use an LSM here; it avoids the earlier per-event insert pathology.

Warm point-lookups in the repository’s microbenchmark are approximately 0.49 µs for stream heads and 0.55 µs for dedupe. These are good generic-KV numbers. A direct dense table can plausibly be an order of magnitude faster, but that alone will not materially change durable append throughput when `fdatasync` dominates.

## 3. The current append critical path

The composed `LogEngine::append_batch` performs the following logical stages.

```text
A. lock Book, intern stream name
B. write new stream name to Fjall if needed
C. acquire one of 256 async append-gate shards
D. lock Book, validate expected version, intern event types, build EventInput list
E. write new type names to Fjall if needed
F. if a new name/type was written: spawn_blocking(MetaStore::persist / SyncAll)
G. spawn_blocking:
     G1. await durable committer append
     G2. wait for position-ordered PublishSequencer turn
     G3. lock Book and append payload/name/type objects and stream position rows
     G4. update ActiveIndex
     G5. build and apply Fjall CommitGroup for stream head
     G6. advance readable watermark
H. release per-stream append gate and return
```

The durability discipline is careful. A new name is persisted before the event that refers to it can be durable. A cancelled async future cannot strand an assigned global position because the append and publish tail run in a non-cancellable blocking task. Cross-stream acknowledgments are re-ordered through a `Condvar` sequencer before they touch dense in-memory structures.

The same correctness requirements can be met with less machinery if the durable committer itself owns validation and state publication:

```text
one owner:
  dequeue -> validate -> assign -> encode -> write -> sync -> apply -> publish -> complete
```

This removes the need to repair out-of-order completion after the fact.

## 4. The `Book` is the dominant architectural debt

The `Book` retains:

- stream-name -> numeric-ID hash map;
- numeric-ID -> stream-name vector;
- event-type-name -> numeric-ID hash map;
- numeric-ID -> event-type-name vector;
- one heap-backed payload object per event;
- one per-stream vector of global positions;
- one head hash map.

Every read materializes owned strings and byte vectors from this structure. Every append copies payload data into it. Every open scans accepted batches, decodes each event payload, resolves names, and repopulates it densely.

This duplicates data already present in:

- active log capsules;
- sealed columnar payload blocks;
- sealed pointer blocks;
- registry tables.

The repository’s measured open-with-rehydration path processes roughly 3.48 million events/s on a one-million-event corpus. This is fast for a full decode, but it scales linearly forever. At 100 million events, simple extrapolation is roughly 29 seconds before cold-device effects and allocator/RSS pressure. That extrapolation is **derived, not measured**, and exists only to show why the asymptotic shape matters.

### Recommended replacement

```text
all-history payload mirror -> remove
active payload reads        -> raw log + bounded capsule cache
sealed payload reads        -> .pcol / SealPack + bounded block cache
stream navigation           -> active microblocks + sealed packed pointers
head                         -> dense direct table
names                        -> registry base + active overlay
```

The old API can materialize `StoredRecord`; the engine-native API should return block-backed views.

## 5. ActiveIndex: good direction, remaining lock/hash costs

The active index uses 64 sharded `RwLock<HashMap<u64, Vec<StreamEntry>>>` structures plus a global `RwLock<Vec<GlobalEntry>>` and an atomic applied watermark. This was a rational choice after rejecting copy-on-write snapshots and left-right duplication. It takes index persistence off the append path and has a clean release/acquire visibility rule.

The custom-kernel opportunity comes from two additional facts:

1. stream IDs are dense, so the first hash lookup can be direct-addressed;
2. active entries are append-only and the writer is singular, so per-stream vectors can be fixed microblocks with release-published counts.

A microblock arena may remove hash computation, shard locks, vector growth/reallocation, and the duplicate global-entry vector. This is a hypothesis; it must beat the current `parking_lot` implementation under real readers.

## 6. Sealed sidecars: excellent data, suboptimal runtime shape

The current pointer sidecar is compact on disk but opens by:

1. reading the entire file;
2. validating the CRC;
3. parsing every 56-byte directory entry;
4. inserting every stream ID and entry into a Rust `HashMap`;
5. retaining a separate ascending stream-ID vector.

This makes a sealed immutable set pay generic mutable-hash overhead and rebuild time. The existing D10 spike compared `HashMap`, sorted vector, hand-rolled FKS, and Fjall. At one million keys, the current `HashMap` won: about 62 ns/hit and 60.6 bytes/key; FKS was slower and larger; Fjall was around 1.87 µs/hit.

That result should be treated as a valuable negative result:

```text
rejected: naive FKS / generic “perfect hash will be faster” claim
not yet tested: bitmap+rank, partitioned Elias–Fano, PtrHash,
                cache-line k-PHF, Ribbon retrieval with exact verification
```

A density-adaptive directory is worth one controlled tournament, not a roadmap dependency.

## 7. Dedupe: where LSM semantics are especially mismatched

The current exact window needs:

- a primary row keyed by stream and arbitrary dedupe bytes;
- a monotonically sequenced order row;
- a read-before-write to find an overwritten sequence;
- deletion of stale order entries;
- eviction of oldest primary and order entries;
- LSM tombstones and eventual compaction for rows that logically expire as a group.

The semantic requirement is simpler: “has this exact key appeared within the last `W` global positions?” The canonical capsule can retain the full key once. A resident/frozen fingerprint index can identify candidates, exact key bytes can be compared from the capsule, and old coarse epochs can be discarded whole. This is a natural custom-structure win because deletion order is already monotone.

## 8. Snapshot path: second Fjall and file-per-blob

The production snapshot wrapper opens another `MetaStore`, writes one blob file at:

```text
blobs/<stream-id>/<version>.blob
```

then advances a Fjall head. The blob-first/head-second order is correct and a lost head degrades to replay. However:

- one file per snapshot version creates metadata and directory overhead;
- the snapshot wrapper uses an interim FNV stream-ID namespace and a side mapping;
- snapshot heads participate in another generic KV database;
- the path is separate from the engine’s runtime interner.

Append-only snapshot packs plus `SnapshotInstalled` control capsules remove these mismatches while preserving blob-first installation.

## 9. Cost classification

| cost | current cause | proposed removal | expected effect |
|---|---|---|---|
| new-name extra barrier | names authoritative in Fjall | registration in same capsule | stronger atomicity; lower first-write latency |
| normal post-commit KV work | `MetaStore::apply_group` | derive/apply resident capsule effect | less CPU/allocation; no lag state |
| all-history open scan | rebuild `Book` | checkpoint + SegmentEffects | asymptotic startup change |
| payload duplicate/RSS | `Book.payloads` | block-native reads + bounded cache | major memory reduction |
| publish ordering | independent append tasks complete out of order | one state-kernel owner | fewer locks/condvars/tasks |
| active stream hash/lock | sharded map-of-vectors | direct table + microblocks | lower read/write CPU if spike wins |
| dedupe tombstones | mutable exact KV + order index | exact epoch index | lower write amp and simpler expiry |
| sealed map rebuild | `HashMap` per sidecar | packed static directory | lower RSS/open cost if spike wins |
| sidecar install states | `.pidx`, `.filter`, `.pcol` | one SealPack | simpler durability/ops |

## 10. What not to change yet

Do not rewrite these until a profile says otherwise:

- batch CRC and marker semantics;
- the existing single-file group committer;
- 256 MiB segment sizing;
- columnar sealed payload format;
- BinaryFuse negative filters;
- packed pointer blocks and skip tables;
- explicit `pread`-style read policy;
- the single-writer-process lock;
- EIO poisoning;
- subscription catch-up/live protocol.

The new kernel should first remove duplicate state and generic metadata work around these proven components.

## 11. Source map

- [`mess-index/src/meta/mod.rs`](https://github.com/bobisme/mess/blob/43e4aca0192f01bb47670627f41182bca182759e/crates/mess-index/src/meta/mod.rs)
- [`mess-index/src/active.rs`](https://github.com/bobisme/mess/blob/43e4aca0192f01bb47670627f41182bca182759e/crates/mess-index/src/active.rs)
- [`mess-index/src/sealed/segment.rs`](https://github.com/bobisme/mess/blob/43e4aca0192f01bb47670627f41182bca182759e/crates/mess-index/src/sealed/segment.rs)
- [`mess-store/src/engine.rs`](https://github.com/bobisme/mess/blob/43e4aca0192f01bb47670627f41182bca182759e/crates/mess-store/src/engine.rs)
- [`mess-store/src/fjall_snapshot.rs`](https://github.com/bobisme/mess/blob/43e4aca0192f01bb47670627f41182bca182759e/crates/mess-store/src/fjall_snapshot.rs)
- [`docs/perf/envelope.md`](https://github.com/bobisme/mess/blob/43e4aca0192f01bb47670627f41182bca182759e/docs/perf/envelope.md)
- [`docs/perf/experiments-d10.md`](https://github.com/bobisme/mess/blob/43e4aca0192f01bb47670627f41182bca182759e/docs/perf/experiments-d10.md)
