# 12 — Convergence: agreed design after review

Recorded: 2026-07-07. This document captures the resolution of the exchange between the research
pack (docs 00–10), the review (doc 11), and the author's response to that review. Where this
document conflicts with earlier docs, this document wins. It is the authoritative statement of
what to build.

Updated 2026-07-07: four spikes validated the quantitative bets in this document — see
[13_spike_results.md](./13_spike_results.md). Corrections from the spikes are folded in below
and marked "(spike)".

## Debate status

```text
review findings adopted wholesale:
  log-as-commit-authority, batch framing, fold certificate scope,
  event-sourced registry, active-path RMW elimination, compression in the
  frame format, schema evolution in Phase 1, bibliography fixes

review positions refined by author pushback (accepted):
  learned indexes demoted not deleted
  fold fingerprint = explicit semantic version, not code hash
  registry requires bootstrap rules

author refinements accepted with one guard each:
  durability mode enum        (+ cursor-regression as a typed error)
  fsync-EIO poisoning policy  (+ degraded-read caveat)
  process locking à la RocksDB (no changes)

author's revised roadmap corrected:
  crash/fault harness restored into Phase 3 (it had been dropped)
  benchmarks restored as exit gates on Phases 3–5
```

## D1. Commit authority

**The event log, with commit markers, is the sole commit authority.**

```text
committed := frame lies inside a durable, marker-terminated batch in the log
indexes   := rebuildable caches; recovery may rebuild them freely
manifest  := segment/catalog metadata, not truth
```

Recovery:

```text
scan segments forward from last known-good offset
accept a batch only if its terminator/length validates
discard the incomplete tail batch (all-or-nothing)
rebuild any missing index entries from accepted batches
```

Invariant I4 from doc 01 is rewritten accordingly. The "index entries pointing beyond commit
boundary" rule is deleted — there is no boundary other than the log's own.

## D2. Batch framing (atomicity + overhead)

The on-disk append unit is a batch, not a frame:

```text
BatchHeader
  magic, format_version
  batch_id
  frame_count
  total_len
  first_global_pos
  stream_id / category refs (batch-constant fields hoisted here)
  batch_crc_or_hash
  flags

EventSubframe*                 // compact: deltas against batch header
  event_type_id
  codec_id, compression_id
  schema_fingerprint
  uncompressed_len, compressed_len
  metadata_len, data_len
  payload

CommitMarker
  magic
  total_len echo               // makes scan termination unambiguous
  batch_crc_or_hash echo
```

Rules:

```text
no marker => no batch (torn marker == discarded batch, by design)
hash/CRC coverage is specified exactly: hash fields zeroed during hashing
crypto chain (prev_stream_hash) is opt-in per stream/category
prev_stream_hash IS the fold-chain value h[i-1], not merely the previous
  frame's hash — so h[v] is computable from frame v alone
```

Batch acceptance rules (spike — crash_log A1–A8; 12,000 randomized crash cases):

```text
A1 MANDATORY: marker validity alone is NOT sufficient acceptance — a stale
   but CRC-valid old batch in recycled disk space after the last good batch
   would resurrect deleted data (demonstrated). Recovery must also check
   contiguity: first_global_pos == expected next position. Add a segment
   epoch / prev-batch link to BatchHeader as defense in depth.
A2: total_len has a sanity cap and an exact definition (whole on-disk
    batch, header through marker).
A3: CommitMarker bytes are inside CRC coverage (echo field zeroed while
    hashing) — otherwise a corrupted length echo can pass.
A4: the full-batch CRC is load-bearing against block-write reordering
    (marker persisting before frames); magic + length echo alone is unsafe.
A5: empty batches (frame_count == 0) are forbidden.
A7: segment boundaries align with batch boundaries; recovery scans from the
    segment start; checkpoint offsets are advisory-only, never a second
    commit authority.
A8: batches never span segments.
```

## D3. Registry is event-sourced into the log

Interned IDs (`stream_id`, `category_id`, `event_type_id`) are assigned by the single writer at
append time and recorded in a system stream, preserving invariant I5 (everything rebuildable by
scanning the log).

```text
$registry stream:
  StreamRegistered    { id, name, category, codec, schema, ... }
  EventTypeRegistered { id, name, schema_fingerprint, codec, ... }
  CategoryRegistered  { id, name }
  NameAliased         { id, new_name }      // rename = alias event, never mutation
```

Bootstrap rules:

```text
reserved IDs:
  stream_id     0 = $registry
  event_type_id 0 = RegistryEventV1
  category_id   0 = $system
  codec_id      0 = bootstrap codec

codec 0 is FROZEN FOREVER. Registry evolution happens by adding new registry
event types, never by changing codec 0 — otherwise the bootstrap problem
recurs one level up.

IDs are never reassigned.

registry compaction: none in v1. The registry is tiny (one event per
stream/type/category ever created) and append-only. If ever needed, the
mechanism is a registry snapshot with a fold certificate — machinery that
will exist by then.
```

## D4. Fold certificates: explicit version + generated drift test

`SnapshotRef` carries:

```rust
struct SnapshotRef {
    stream_id: StreamId,
    stream_version: u64,
    snapshot_ptr: BlobPtr,
    event_prefix_hash: Hash256,   // chain value h[v] — proves prefix identity
    state_hash: Hash256,          // blob integrity
    fold_version: u32,            // EXPLICIT semantic version, human-bumped
}
```

Resolution of the code-hash vs. explicit-version debate:

```text
mechanism: explicit #[aggregate(fold_version = N)] — code hashing is brittle
           (dep bumps, inlining, macro output churn would spuriously
           invalidate every snapshot)

guard:     the derive generates a FOLD-DRIFT GOLDEN TEST — fixture events
           plus expected folded state committed to the repo. If apply()
           semantics change, the test fails with "bump fold_version or fix
           your fold." Explicit version for the mechanism, generated test
           for the human failure mode (forgetting to bump).
```

Snapshots with a mismatched `fold_version` are invalidated and rebuilt by replay. This is the
snapshot-invalidation-on-deploy story.

Scope stays honest: the certificate proves the snapshot summarizes the exact committed prefix;
it does not prove the fold code was correct. `load_verified` docs must say so.

## D5. Active/sealed index lifecycle (no RMW on the hot path)

```text
active:
  append log batch
  cheap per-event index entries (stream_id+version -> EventPtr) or an
  append-only mini-index; hot heads in memory; tail rebuildable from log

seal:
  consolidate per-event entries into packed, VARIABLE-LENGTH pointer blocks
  build filters
  train per-category zstd dictionaries; recompress payload blocks
  build static accelerators
  delete the per-event entries
```

Fixed 256-entry blocks are rejected: most streams are short; blocks are sized to what exists.

Measured (spike — ptr_index, 500k Zipf appends): RMW block maintenance costs **55× the logical
write volume** of per-event entries and is 2.8–3.4× slower, worst on the hottest streams;
packed blocks replay hot streams 78–95× faster and use ~3× less disk. Both halves of the
lifecycle above are confirmed. **Backend pick: fjall for the active index** (264k vs. redb's
34k appends/s at journal-buffered durability, which is legitimate here because the active index
is rebuildable from the log); redb only draws even under forced fsync-per-commit.

## D6. Compression

Table stakes, not exotic. Frame format carries `codec_id`, `compression_id`,
`uncompressed_len`, `compressed_len` from v1 (see D2) even though heavy compression lands at
seal time:

```text
v1: format fields present; per-frame compression optional/off
v2: per-category (or per-event-type) zstd dictionaries trained at seal;
    sealed payload blocks recompressed
measured win (spike): 3–6x on realistic-variance JSON — still larger than
    the entire filter/learned-index program. 10x is unreachable at L3.
```

Format decisions (spike — compression):

```text
sealed segments: block compression (~128 events) with per-category 16 KiB
  dictionaries and a dict_id in the block header. At block granularity the
  dictionary adds only 0-6% (a block is its own dictionary) but costs
  nothing to keep for consistency with the hot tail.
hot unsealed tail: per-event WITH dictionary (2.8-3.5x, 0.3-1.1 µs point
  decode). Per-event WITHOUT a dictionary is rejected — 1.2-1.4x, pointless.
point reads on sealed blocks cost 20-90 µs (whole-block decompress):
  front them with a small decompressed-block cache.
```

## D7. Durability modes and their consequences

```rust
enum Durability {
    Process,                              // page cache accepted; survives process crash only
    Os,                                   // fdatasync OK; survives OS crash per fs/device contract
    Group { max_delay: Duration, max_bytes: u64 },  // grouped sync boundary
}
```

Log-as-authority collapses the old data-durable/index-durable distinction: **marker durable =
ack**. One fewer state; keep it.

Required consequence the spec must own: under `Process` (and inside a `Group` window),
visibility precedes durability, so a subscriber can hold a cursor pointing past the post-crash
log end. This is a **typed, documented error**, not undefined behavior:

```text
CursorRegressed { cursor, log_end }  // "log ends before your cursor; resubscribe from log_end"
```

Write-side mirror (spike — crash_log A6): recovery can legitimately surface batches that were
fully written but never acknowledged (observed in 171 of 12,000 crash runs). A crashed client's
retry will interact with these; the dedupe-window spec must own this case explicitly rather
than leaving it emergent.

## D8. fsync-EIO poisoning policy

```text
on fdatasync/fsync EIO:
  mark store poisoned; reject all future writes
  reads may continue in an explicit DEGRADED state — callers are told,
  because post-EIO the page cache can serve pages that never reached disk,
  so reads may return data that will not survive restart (same
  cursor-regression hazard as D7)
  require process restart; recovery scan defines truth
  never retry-and-continue (the PostgreSQL fsyncgate lesson)
```

## D9. Process model

Copied from boring databases (RocksDB documents the same rule):

```text
one writer process per store directory, enforced by an OS lock file
many threads within that process
server mode: the server process owns the dir; clients speak a protocol
no cleverness
```

## D10. Learned indexes: demoted, not deleted

```text
global_pos -> offset within a segment:  NO — positions are dense/monotone by
    construction; a delta-encoded offset array is direct addressing
stream-name / registry string directories: MAYBE — benchmark
    RadixStringSpline (arXiv:2111.14905) against perfect hashing at seal time
hot append path: NEVER
```

Research note in doc 07 stays; roadmap commitment is removed. Benchmarks earn admission.

## Revised roadmap (final)

The author's reordering is adopted with one correction: **the crash/fault harness had been
dropped from the revised phases entirely. It is restored into Phase 3 as a co-requisite, and
benchmarks become exit gates on Phases 3–5.** The original pack itself named
"snapshot + replay tail == full replay across 10,000 randomized crash/recovery cases" as the
project's soul; the harness is the proof, the recovery scanner is merely the code.

```text
Phase 0 — Semantics spec
  event log is truth (D1); batch atomicity (D2); expected-version behavior;
  durability modes + cursor regression (D7); cursor semantics;
  registry bootstrap (D3)

Phase 1 — DX first
  Aggregate / Event / Decide traits
  command() with bounded optimistic retry on version conflict
    (spike: default budget must exceed 16 attempts with jittered backoff —
    24 attempts observed under just 8 concurrent writers; longer term,
    actor/store-side conditional append removes the race entirely)
  Given-When-Then test kit (generated from derives)
  schema version + upcaster model; codec position
    (postcard alone rejected for payloads: not evolution-tolerant)
  fold-drift golden tests (D4)
  snapshots as a public API concept

Phase 2 — Boring backend
  existing RocksDB/redb backend; prove API ergonomics; examples
  decide fate of StreamPos bit-flag encoding and the unwired HLC clock
    (map onto strict/relaxed modes or retire — no silent carry)

Phase 3 — Custom log  [co-requisite: crash harness]
  segmented append-only log; batch framing + commit markers (D2)
  recovery scanner; active committed watermark for readers
  single-process lock (D9); fsync poisoning (D8)
  HARNESS: failpoints at every step of the batch protocol
    (header written / frames partial / marker written / marker synced),
    randomized crash/recovery runs, differential check against an
    in-memory reference model — from the FIRST commit of this phase
  tooling: fail crate; deterministic sim (turmoil/madsim-style or sim-fs);
    ALICE-style op-reordering checks
  EXIT GATE: 10,000 randomized crash/recovery cases green + append/replay
    benchmarks vs. Phase 2 backend

Phase 4 — Index/cache engine
  per-event active indexes -> sealed variable-length pointer blocks (D5)
  active index backend: fjall at journal-buffered durability (spike pick)
  snapshot_head; registry replay; projection checkpoints
  EXIT GATE: benchmark pointer-block design vs. per-event-key baseline
    (spike ptr_index answered this for the strategy — 55x write amp for
    RMW — re-verify on the real implementation, not just the model)

Phase 5 — Verification/compression
  fold hashes + fold_version enforcement (D4)
  zstd dictionaries at seal (D6); fixture compatibility tests
  EXIT GATE: compression ratio + seal-time cost measured on realistic
    category-homogeneous payloads

Phase 6 — Accelerators
  filters (Ribbon / Binary Fuse; ZOR filters — arXiv:2602.03525 — worth a look)
  perfect hash / string directory experiments
  learned indexes only if benchmarks earn them (D10)
```

## Open items (not blocking)

```text
- dedupe window: define extent (time vs. global-pos span) and
  retry-after-expiry behavior (duplicate accepted, documented)
- ecosystem survey (cqrs-es, disintegrate, thalo, message-db, Kurrent client)
  for DX comparison — informs Phase 1, does not gate it
- prior-art citations to add: Kafka segment + sparse index files,
  EventStoreDB chunk format, message-db
- Cascade Log (arXiv:2606.05467): cite as inspiration only; unreviewed
  preprint, author associated with high-volume auto-generated submissions
```

## One-paragraph summary

Canonical log is truth; batch markers give atomicity; the registry is itself event-sourced with
frozen bootstrap codec; the active path avoids read-modify-write; the sealed path does
consolidation, compression, and indexing; snapshots are O(1), prefix-proven, and invalidatable
via explicit fold versions backed by generated drift tests; DX gets command retry, upcasting,
and test kits in Phase 1; and none of it ships without the crash harness green. That is the
version worth building.
