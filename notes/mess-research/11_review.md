# 11 — Review of the research pack

Reviewed: 2026-07-07. Scope: docs 00–10 checked against the actual repo code and every citation
verified against the live web.

## Verdict

The architecture is sound and the phased plan is good. The core thesis — canonical event bytes
once in append-only segments, everything else a rebuildable accelerator — is correct for this
workload, and the discipline (io_uring/CRDTs/learned structures kept out of v1, DX before custom
storage) is right.

The pack has:

- 2 bibliography errors (wrong arXiv IDs; the papers themselves are real)
- 2 correctness holes in the storage design (commit authority, batch atomicity)
- 1 overclaimed mechanism (fold certificates) plus a spec gap in its storage
- 1 broken invariant (index rebuildability vs. interned IDs)
- 1 self-inflicted write-amplification problem (pointer-block RMW)
- several unaddressed operational realities (active-segment readers, process locking, fsync failure)
- several high-value omissions, the biggest being compression and schema evolution

None of these invalidate the plan. All are fixable in the design phase, and most fixes make the
design simpler, not more complex.

## 1. Citation audit

Every reference was verified against arxiv.org / the cited docs. Result: 15 of 17 correct,
2 cited with wrong arXiv IDs, none fabricated.

| Reference | Verdict |
|---|---|
| WiscKey, FASTER, CompassDB, Cuckoo, Xor, Ribbon, Binary Fuse, PGM-index, RadixSpline, DBSP, CALM, Coordination Avoidance, Tree Clocks (2201.06325), CRDT overview (1805.06358), ZNS characterization (2310.19094), io_uring DBMS (2512.04859), FlintKV (2607.02401), Cascade Log (2606.05467), F2FS/ZNS/JetStream/RocksDB/redb/Fjall/Kurrent docs | verified as cited |
| ZOR filters | wrong ID — cited 2601.04843 (a soft-matter physics paper); real paper is "ZOR filters: fast and smaller than fuse filters" (Limasset), **arXiv:2602.03525** |
| RadixStringSpline | wrong ID — cited 2104.11346 (unrelated); real paper is "Bounding the Last Mile: Efficient Learned String Indexing" (Spector, Kipf, et al.), **arXiv:2111.14905** |

Credibility caveat: "The Cascade Log" (Alpay & Sarioglu, June 2026) exists but has no venue, no
independent citations, and the first author is associated with a high volume of
auto-generated-looking arXiv submissions. The idea borrowed from it (logical vs. physical
references surviving compaction) is sound on its own merits — cite it as inspiration, don't lean
on its claims. FlintKV, by contrast, is from established distributed-systems researchers
(Royal Holloway / Surrey).

Fixes to apply to `10_bibliography.md`:

```text
ZOR filters:          https://arxiv.org/abs/2602.03525
RadixStringSpline:    https://arxiv.org/abs/2111.14905
Cascade Log:          keep, add "unreviewed preprint, treat claims skeptically"
```

## 2. Correctness holes

### 2.1 Commit-visibility authority is contradictory

Invariant I4 (`01_architecture_thesis.md`) says a commit is visible only when the
manifest/index commit boundary has advanced. The recovery procedure
(`03_flash_io_and_durability.md`) scans the log past that boundary and "rebuilds missing index
entries" — which makes the log the authority. Both cannot be true. And if every index is
rebuildable from the log (invariant I5), the index *cannot* be the commit authority, because a
destroyed-and-rebuilt index would resurrect exactly the entries I4 says are invisible.

Resolution (recommended): **the log, with commit markers, is the sole authority.**

```text
committed  = frame is inside a durably-written, marker-terminated batch region
indexes    = pure caches; recovery may rebuild them freely from committed frames
I4 rewrite = "visible iff the frame's batch commit marker is durable per the
              selected durability mode"
```

This also removes the awkward "index entries pointing beyond commit boundary are ignored" rule —
there is no boundary to compare against except the log's own.

### 2.2 Multi-event batch atomicity is unhandled

`append_batch` writes N frames, each individually CRC-valid. A crash mid-batch leaves a valid
*prefix* of the batch on disk, and the recovery rule "stop at first invalid frame" will commit
half a batch — violating expected-version append atomicity.

Fix: batch framing.

```text
BatchHeader   { frame_count, total_len, batch_crc, ... }
Frame*        { compact per-event subheader + payload }
CommitMarker  { batch_crc echo }   // or total_len in header checked at recovery
```

Recovery accepts a batch only if its terminator/length checks out; otherwise the whole batch is
tail garbage. Bonus: batch framing lets per-batch-constant fields (stream_id, category_id,
timestamps) live once in the batch header, addressing the frame-overhead problem in §2.5.

### 2.3 Fold certificates prove less than the DX implies

`load_verified::<Account>()` (`08_novel_mechanisms.md` §2) verifies the snapshot summarizes the
correct *event prefix*. It cannot verify the snapshot state was computed by *correct fold code*:
a snapshot produced by last month's buggy `apply()` verifies fine. Doc 08 admits this for
retention certificates (§8) but §2 oversells it.

Fix: include a **fold-logic fingerprint** in the certificate and the verification check:

```rust
struct SnapshotRef {
    // ... existing fields ...
    fold_fingerprint: u64,  // hash of (aggregate type, schema_version, apply-logic version)
}
```

Bump the fingerprint whenever `apply()` semantics change; mismatched snapshots are invalidated
and rebuilt by replay. This is also the missing **snapshot-invalidation-on-deploy story** — the
pack never says what happens to existing snapshots when aggregate code changes.

Spec gap in the same mechanism: the chain `h[i] = H(h[i-1], frame_hash[i], i)` is defined, but
`stream_index.fold_hash_at(v)` is given no storage — as written it is either an O(v) recompute
or 32 bytes/event of index. Fix by defining `FrameHeader.prev_stream_hash` to *be* the chain
value `h[i-1]` (currently ambiguous — it reads as "previous frame's hash"). Then `h[v]` is
computable from frame v alone, and no index storage is needed.

Related nit: `frame_hash` is a field of the header it presumably covers. The spec must define
hash/CRC coverage explicitly (hash covers header-with-hash-fields-zeroed + payload, or
header-prefix + payload).

### 2.4 The ID registry breaks invariant I5

Frames carry `stream_id: u128`, `category_id: u64`, `event_type_id: u64` — interned IDs. I5
claims all indexes are rebuildable "by scanning the segments," but the name→ID registry is
itself an index: lose it and every frame is anonymous. The current repo uses structured string
stream names (`category-id` parsing in `mess_ecs/src/streams.rs`); the pack never bridges the two.

Options, best first:

```text
A. registry is event-sourced into the log itself (a system stream:
   "$registry" carrying name<->id assignments). Rebuild = replay it.
B. first-use frames embed the name inline (extension block); later
   frames carry only the ID.
C. IDs are hashes of names — no registry, but no reverse lookup and
   collision policy required. Not recommended for stream_id semantics.
```

Option A keeps I5 intact with zero per-frame cost after first use, and gives you a free audit
trail of stream/category/type creation.

### 2.5 Frame header overhead

The proposed `FrameHeader` is ~140 fixed bytes including two 32-byte hashes — more than 2×
write amplification for small events, in a design whose pitch is avoiding amplification.

Fixes (compose with §2.2 batch framing):

```text
per-batch-constant fields -> BatchHeader, once per batch
crypto chain              -> opt-in per stream/category
varint/delta subheader fields where cheap
```

### 2.6 Pointer-block RMW reintroduces write amplification

`PtrBlock` with N=256 entries means a read-modify-write of a KB-scale value in redb/Fjall
*per append* (same for category blocks). That is precisely the metadata write amplification the
pack criticizes LSM compaction for.

Fix — align with the pack's own active/sealed lifecycle:

```text
active window:  cheap per-event pointer keys (stream_id+version -> EventPtr),
                or an in-memory block builder whose tail is rebuildable from
                the log on recovery
seal time:      consolidate per-event keys into packed PtrBlocks, delete the
                per-event keys
```

Also: most real streams are short (a handful of events per aggregate). Fixed 256-entry blocks
are bloat for the common case; blocks must be variable-length, sized to what exists.

### 2.7 Unaddressed operational realities

```text
active-segment readers:
  subscribers scan the active segment while the writer appends. Needs a
  shared in-memory committed watermark; readers never scan past it. Torn
  reads of in-flight frames must be impossible by construction, not by CRC luck.

process model:
  embedded store => lock file / single-writer-process enforcement, and a
  documented answer for "two processes opened the same dir."

fsync failure (fsyncgate):
  an EIO from fsync/fdatasync is fatal for that file — page cache state is
  unknowable afterward; never retry-and-continue (the PostgreSQL lesson).
  The durability section is silent on this.

dedupe window:
  "exactly-once within window" needs the window defined (time? global-pos
  span?) and the behavior on retry-after-expiry stated (duplicate accepted).
```

## 3. Overstated or unnecessary parts

### 3.1 Learned indexes over global_pos are ornamental

Within a segment, global positions are dense and monotone by construction (single writer). A
delta-encoded offset array or two-level table is smaller and faster than PGM/RadixSpline for
that mapping — doc 04 even notes the dense-array option, then doc 07 recommends learned models
anyway. Learned indexes earn their keep on skewed key distributions; the segment-local sorted
sets that are actually skewed (stream directories) are hashed key sets where perfect hashing
already wins. Demote learned indexes from "v3 roadmap" to "benchmark curiosity."

### 3.2 Prior art undersold

"Meridian Log" is essentially Kafka's segment + sparse-index layout, plus EventStoreDB/Kurrent
chunk files, plus WiscKey pointer separation. That is a strength — the shape is battle-proven —
but the pack cites none of the three as storage-format prior art. Add:

```text
Kafka log segments + sparse .index/.timeindex files
EventStoreDB chunk file format (chunks, scavenge, $all order)
message-db (Eventide's Postgres store) for API-shape prior art
```

## 4. High-value omissions

### 4.1 Compression (the biggest one)

A storage-performance research pack with zero words on compression. Event payloads are small,
repetitive, and category-homogeneous — ideal for dictionary compression:

```text
active segment:  optional cheap per-frame compression (or none)
seal time:       recompress payload blocks with per-category zstd dictionaries
expected win:    3-10x on JSON-ish payloads — larger than everything in the
                 filter/learned-index sections combined
```

Design consequence *now*: the frame format needs a codec field and compressed-block framing from
day one, even if implementation is v2. Note the current repo stores sqlite payloads as JSON text
and rocks payloads as postcard — both uncompressed.

### 4.2 Schema evolution / upcasting

The #1 real-world event-sourcing DX pain, and the pack gives it one passing mention
(`Event::VERSION`). Needed:

```text
- a codec position: postcard (current repo choice) is NOT evolution-tolerant —
  field reorder/rename silently decodes garbage. Payloads need a
  self-describing or tagged format, or a strict schema-registry discipline.
- upcaster pipeline: decode old version -> upcast chain -> current type
- schema fingerprint per event type stored in frames/registry
- derive-generated compatibility tests (old fixture bytes must decode forever)
```

### 4.3 Optimistic retry in `command()`

The north-star API will hit `WrongStreamPosition` under any concurrency. The obvious DX win:

```rust
store.command(id, cmd).await?         // internally: load -> decide -> append,
                                      // on version conflict: reload and retry
                                      // with bounded policy (attempts, backoff)
```

Absent from the plan; belongs in Phase 1.

### 4.4 Given-When-Then test kit

Generated from the `Aggregate`/`Decide` derives:

```rust
AggregateTest::<Account>::given([Opened { .. }, Deposited { .. }])
    .when(Withdraw { amount })
    .then([Withdrawn { amount }]);
```

Cheap to build, the single most-loved DX feature of mature ES frameworks, and it pairs directly
with doc 06's "event laws as property tests" idea. Belongs in Phase 1–2.

### 4.5 Rust ecosystem landscape

No survey of `cqrs-es`, `disintegrate`, `thalo`, message-db clients, or the Kurrent Rust client.
Needed both for DX inspiration and to substantiate the "wedge" claim in `00_README.md`.

### 4.6 Crash-harness tooling, concretely

Doc 09's failpoint plan is right; name the tools:

```text
fail crate            (failpoints)
deterministic sim     (turmoil / madsim-style, or hand-rolled sim-fs layer)
ALICE-style checks    (filesystem op reordering between barriers)
```

## 5. Integration notes with the existing repo

The pack's repo observations checked out against the code (payload duplication across the
`global`/`stream` CFs confirmed in `mess_db/src/rocks/record.rs`; `StreamPos`
Sequential/Relaxed confirmed in `mess_db/src/lib.rs`). Two things Phase 0 should decide
explicitly rather than silently drop:

```text
StreamPos 1-bit Sequential/Relaxed encoding:
  either maps onto doc 06's strict/relaxed stream modes or is retired.
  The new design's plain u64 stream_version conflicts with the current
  shifted encoding.

HLC clock (rocks/clock.rs — built, tested, unwired):
  either becomes the `ord`/causal field for future relaxed streams or is
  deleted. Don't carry it as dead weight.

known code debts the rewrite obsoletes but shouldn't replicate:
  static mut CACHED_GLOBAL in rocks/write.rs (unsound with multiple DBs)
  actor handle_req unwrap() panic path in svc.rs
  Relaxed writes todo!()
```

## 6. What is right and should not be touched

```text
- payload-once + pointer-index separation (WiscKey adaptation)
- snapshot_head as an O(1) first-class primitive
- fold-associativity snapshot law (mathematically correct as stated)
- opaque cursors; frontier-as-join-semilattice checkpoints
- active/sealing/sealed segment lifecycle
- filters may only say no/maybe, never a false negative
- phase ordering: DX first -> exact semantics -> custom storage -> accelerators
- the "hard no" list in doc 09
```

## 7. Amended recommendation summary

Adopt the pack's plan with these deltas:

```text
design phase (before Phase 4):
  D1. log-with-commit-markers is the sole commit authority; indexes are caches
  D2. batch framing: BatchHeader + subheaders + terminator (atomicity + overhead)
  D3. registry event-sourced into the log (preserve I5)
  D4. codec field + compression framing in the frame format
  D5. prev_stream_hash defined as the fold-chain value; hash coverage specified
  D6. fold-logic fingerprint in SnapshotRef; snapshot invalidation on change

phase 1 additions:
  P1. command() bounded retry on version conflict
  P2. given-when-then test kit from derives
  P3. codec/evolution position + upcaster pipeline design

phase 4 changes:
  P4. per-event ptr keys in active window; consolidate to PtrBlocks at seal
  P5. variable-length pointer blocks
  P6. committed-watermark protocol for active-segment readers; lock file
  P7. fsync-EIO-is-fatal policy

phase 7 changes:
  P8. per-category zstd dictionary recompression at seal (promote above
      filters in priority)
  P9. demote learned offset models to benchmark-only

docs:
  B1. fix ZOR filter id -> 2602.03525; RadixStringSpline -> 2111.14905
  B2. annotate Cascade Log as unreviewed preprint
  B3. add Kafka / EventStoreDB chunks / message-db as prior art
```
