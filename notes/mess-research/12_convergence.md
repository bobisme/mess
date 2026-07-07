# 12 — Convergence: agreed design after review

Recorded: 2026-07-07. This document captures the resolution of the exchange between the research
pack (docs 00–10), the review (doc 11), and the author's response to that review. Where this
document conflicts with earlier docs, this document wins. It is the authoritative statement of
what to build.

Updated 2026-07-07: four spikes validated the quantitative bets in this document — see
[13_spike_results.md](./13_spike_results.md). Corrections from the spikes are folded in below
and marked "(spike)".

Updated 2026-07-07 (round 2): three further spikes — the composed vertical slice, the
torn-write/reordering harness, and the subscription-handoff protocol — see
[14_spike_results_round2.md](./14_spike_results_round2.md). They added rules A9–A12 to D2,
amended D5/D6/D7, and added D11.

Updated 2026-07-07 (round 3): four final spikes — seal pipeline, codec bake-off, fold
certificates, recovery at scale — see [15_spike_results_round3.md](./15_spike_results_round3.md).
They settled the codec (MessagePack named + upcasters), the segment size (256 MiB), flipped the
vertical-slice losses post-seal, and marked D4 design-incomplete pending G6/G10.

Updated 2026-07-07 (round 4, performance): four measurement-driven optimization spikes — see
[16_spike_results_round4.md](./16_spike_results_round4.md) for the measured performance
envelope. They finalized the D7 group-commit design, made the in-memory-until-seal active
index the D5 default, and replaced D6's default sealed tier with columnar shredding.

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
  schema_version: u16          // beside event_type_id — together they are the
                               // upcaster dispatch key (spike: codec_bakeoff)
  codec_id, compression_id
  dict_id: u16                 // 0 = none; dictionaries registered durably (D3)
  uncompressed_len, compressed_len
  metadata_len, data_len
  payload                      // domain payloads: codec_id 1 = MessagePack
                               // named-field mode; postcard only for frozen
                               // internal structures, never payloads

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

Additional rules (spike — torn_write; 24,000 sector-reordering crash cases, ALICE-style):

```text
A9 MANDATORY: the segment epoch / generation in BatchHeader is upgraded from
    defense-in-depth to REQUIRED before any segment recycling. Demonstrated:
    a recycled segment holding a stale prior-generation batch at a coincident
    first_global_pos, with zero new sectors persisted (legal under
    reordering), passes every other check including A1 contiguity and
    resurrects deleted data.
A10: recovery must NEVER resynchronize past a hole — the scan stops at the
    first invalid batch, full stop, even if fully-valid batches exist beyond
    it (the harness left 1,248 valid "resync bait" batches past stop points).
A11: BatchHeader does NOT require single-sector alignment; safety under
    straddled/torn headers comes from the A2 length cap + CRC, not layout.
    Stated to preempt alignment "optimizations".
A12: no CRC-off recovery fast path may ever exist. With the batch-CRC check
    disabled, 348 corrupt batches were wrongly accepted across 24,000 cases;
    in 7.8% of cases the CRC was the only rejecting check (header + marker
    sectors persisted, a frame sector did not — structurally invisible
    without it). A4 is empirically confirmed.
```

Segment size and recovery (spike — recovery_scale, 31 GiB measured):

```text
segment size: 256 MiB. Full recovery depends only on total log size
  (~1.1 s/GiB cold, regardless of segment size); the last-segment-only fast
  path depends only on active-segment size (256 MiB -> 0.32 s, flat as the
  log grows). 1 GiB crosses the 1 s startup line; 64 MiB buys nothing.
R1: recovery scan is CPU-bound (~1.3 GiB/s/core) — parallel per-segment
    scans are permitted; sealed footers provide the A1 contiguity seeds.
R2: the advisory manifest caches segment footers (fast path is otherwise
    O(#sealed segments) preads).
R3: the segment footer carries the A9 epoch.
R4: CRC/hash coverage is split AROUND the checksum fields — recovery never
    copies a batch to zero fields before verifying.
CRC32C vs BLAKE3 at recovery: ~1,350 vs 590 MiB/s — mandatory crypto would
  slow recovery 2.1–2.3×; the opt-in chain stands confirmed.
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

compression dictionaries are registry objects (spike — codec_bakeoff):
  DictRegistered { dict_id, category/event_type scope, dict_bytes_ref }.
  A dict_id referenced by any live frame must never be deleted.
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

Validation and open design items (spike — fold_cert; all 13 attack scenarios detected):

```text
measured: ~165 ns/event append overhead (~3% of the composed budget);
  load_verified 0.7 µs + ~0.5 µs/tail-event; 1M-event full verify in 504 ms.
MANDATORY: the genesis hash includes stream_id — without it the cross-stream
  snapshot-confusion attack demonstrably succeeds. Unify the genesis formula
  (docs 08 and 12 currently disagree) and specify byte-exact encodings.
BLOCKING G6: the trusted head anchor must live OUTSIDE the rebuildable index
  (segment footer or manifest) — a head rebuilt from the log anchors
  truncation detection to nothing.
BLOCKING G10: where h[i-1] physically lives in the batch format is undecided;
  per-frame materialization costs ~25% storage on small events. Likely
  answer: materialize per batch, recompute within a batch.
also: version-0/empty-prefix snapshots need a defined representation;
  retention/compaction must preserve the frames a verification reads (or
  convert to a retention certificate per doc 08 §8).
```

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

Composed-slice corrections (spike — vertical_slice, 1M events end-to-end):

```text
- active-index inserts MUST be batched per commit, not issued per event:
  10 synchronous fjall inserts per batch made the index — not the log — the
  append bottleneck (composed buffered path 175k vs. RocksDB's 532k). (F1)
  [round-4 correction: the log alone does ~689k ev/s, not ~224k — the index
  tax was ~3/4 of baseline capacity]
- until seal-time packed blocks + read coalescing exist, stream replay via
  per-event pointer chasing LOSES to an LSM prefix scan by 1.7× (one pread
  per pointer vs. locality). Packed blocks (78-95× hot-replay win, above)
  and segment-order pread coalescing (F7) are what close this — they are
  required for the thesis, not optional acceleration.
```

Round-4 finalization (spike — perf_append): **the active index lives in memory and is
persisted only at seal by a background sealer thread** — this is now the default, not the
endgame. Measured: composed buffered append 1.71M ev/s at 4 writers (~3× RocksDB's 557k),
p99 batch latency 32 µs, ~0 hot-path allocations. Write-path architecture: dedicated
appender/actor (F8a) — it beats position-range reservation at realistic batch sizes and
matches the existing svc.rs shape; range reservation is the bulk-load path. Consequence
(extends F6): recovery rebuilds the active index for EVERY segment lacking a sealed footer,
not just the last one — async sealing means a rolled-but-unsealed segment is normal.

Seal verdict (spike — seal_pipeline, real 256 MiB segment, 1M events): **both vertical-slice
losses flip.** Sealed stream replay 2.72M ev/s (1.55× over the RocksDB target; hottest stream
7.40M, 4.2×); sealed disk 44.0 B/event all-in vs. RocksDB's 232.6 (5.3×); global replay 6.40M
ev/s. Seal cost: **1.50 s single-threaded per 256 MiB segment**, peak RSS ≈ the segment buffer —
comfortably a background task. Filters: **BinaryFuse16 is the default** (0.002% measured FPR at
3.61 B/key ≈ 26 KiB/segment, ~4 ns/query); BinaryFuse8 if memory ever matters (0.372% FPR at
1.32 B/key). Caveats to carry: pointer blocks want an intra-block skip table (cached point-read
p99 is linear varint-seek on hot streams); the decompressed-block cache earns 48% on replay but
~3% on random point reads.

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

Status upgrade (spike — vertical_slice): compression is **load-bearing for the thesis, not a
v2 luxury**. The uncompressed v1 slice LOSES the disk-footprint comparison to RocksDB by 1.31×
(232.6 vs. 305.5 B/event) because SST block compression beats payload deduplication on raw
JSON. With the measured 3–6× sealed-block compression applied, the comparison flips to roughly
a 2.5–3× win for the custom log. Sealed-segment recompression moves up the roadmap accordingly.

Format decisions (spike — compression; REVISED by round-4 perf_compress):

```text
DEFAULT SEALED TIER (round 4): COLUMNAR SHREDDING — per event type, msgpack
  fields split into per-path columns (delta/varint int columns, interned
  structural skeletons), 128-event blocks, zstd-9, NO dictionaries.
  Measured: 30.8 B/event at 2.89M ev/s replay; +42-47% ratio over row
  blocks at every block size; byte-exact reassembly proven on 1M events
  (0 mismatches); point reads FASTER than row (only needed columns
  decompress). Row blocks remain the fallback for unshreddable payloads.
DICTIONARIES DEMOTED (round 4): obsolete under columnar; kept at 16 KiB
  only for the row-format fallback and the hot unsealed tail (per-event
  WITHOUT a dictionary stays rejected — 1.2-1.4×, pointless).
OPTIONAL ARCHIVE TIER: columnar/2048-event blocks/zstd-19 -> 26.5 B/event
  for segments past the replay SLA (replay drops to 0.48M ev/s).
FORMAT: block index carries a 1-bit columnar/row flag; skeleton tables
  (~2 KiB/segment) join sealed metadata; zstd-22 ≡ zstd-19 here (skip it);
  no lz4 tier.
point reads on sealed blocks: front with a small decompressed-block cache
  (48% hit on replay; useless for random point reads — those rely on
  columnar's partial decompress + the intra-block skip table).
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

Group-commit amendment (spike — vertical_slice, F2): fixed-delay `Group { max_delay }` is
**strictly worse than sync-per-batch at low writer concurrency** — measured on both engines:
1 ms ≈ 4k ev/s (a tie with sync-per-batch), 5 ms ≈ 2.7k, 25 ms ≈ 950. The window must close
early when every in-flight writer is already waiting on it (`max_delay` is the cap, not the
target); with that rule, group commit degrades gracefully to sync-per-batch when there is
nothing to group.

Final group-commit design (round 4 — perf_group_commit; every point crash-verified):

```text
single segment file; single committer thread:
  gather the group -> assign positions centrally -> ONE coalesced write ->
  one fdatasync -> advance the position-ordered durable watermark -> ack.
  (Centralized gathering also fixes a measured convoy-split race in
  decentralized early-close: 5,070 vs ~2,400 events/barrier.)
window: early-close (all in-flight writers pending) + max_bytes volume
  close + max_delay cap. Confirmed: beats sync-per-batch even at 4 writers
  (+93% at half the latency).
measured: 121k durable ev/s @ p50 2.8 ms (4 writers × 100); 363k @ p50
  14 ms (512 × 10); knee at ~1.5 MB/group; ceiling is device write+flush
  bandwidth (~150-200 MB/s), not fsync rate.
O_DSYNC coalesced writes: 2.3-4.4× at high concurrency with tighter tails
  (FUA pays per byte; FLUSH pays for the whole dirty cache) — offered as a
  device-verified config option; fdatasync is the default.
striping across files: REJECTED for v1 — parallel flush capacity is real
  (~4× at 8 files) but global-order acks couple latency to the slowest
  stripe and lose end-to-end. Deterministic recovery-merge was implemented
  and crash-proven; revisit only for a multi-device tier.
never hold the append path across the fsync barrier.
ops: fdatasync on a near-full consumer SSD degrades ~50× under sustained
  load (measured 3.3 ms -> 150+ ms) — the runtime fsync-latency metric
  (doc 09) is mandatory, and the store should surface it loudly.
```

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

## D11. Subscription handoff (catch-up → live)

Added from the sub_handoff spike (5,600 randomized scenarios, 10,617 subscriber sequences
verified gapless and duplicate-free; full protocol in `spikes/sub_handoff/REPORT.md`).

A subscription created at cursor `c` delivers exactly the committed positions `c+1, c+2, …` in
order — no gaps, no duplicates — regardless of concurrent appends, consumer speed, or repeated
falls from live back to catch-up.

```text
sources:
  history   = paged read_from(cursor, limit), serving only positions <= the
              D7 committed watermark. AUTHORITATIVE.
  live feed = bounded per-subscriber buffer fed by the committing writer.
              An optimization; carries ZERO correctness weight.

writer obligation (the invariant everything rests on):
  for every committed position p: advance the committed watermark to >= p
  BEFORE offering p to any live buffer, and live-feed order == position
  order. A batch's positions become visible together: watermark to batch
  end, then publish the batch's positions in order. A refactor that
  publishes outside the commit critical section breaks gapless delivery
  undetectably.

protocol:
  states: CatchUp -> Switching -> Live; {Switching, Live} --overflow--> CatchUp
  subscribe(c):   attach to live feed FIRST, then CatchUp with last := c
  CatchUp:        page read_from(last); deliver; empty page -> Switching
  Switching/Live: next live event p:
                    p <= last   -> drop  (overlap dedupe — MUST be <=, not ==:
                                          post-regression the buffer holds
                                          arbitrarily stale positions)
                    p == last+1 -> deliver (Switching becomes Live)
                    overflow    -> back to CatchUp from last
```

Consequences:

```text
- overflow is lossy-but-loud: dropping a slow subscriber's buffered events
  is required (bounded memory); dropping them silently is forbidden.
- no flapping is possible: a persistently slow subscriber takes one
  overflow and settles in CatchUp; the switch condition (empty page) is
  itself the proof of having caught up.
- live-buffer sizing tracks COMMIT BATCH SIZE, not throughput: one burst
  larger than the buffer sends even a fast subscriber through history.
- expose watermark - cursor as the lag metric; the protocol tolerates
  unbounded lag silently (bounded memory, unbounded delivery debt).
- CursorRegressed (D7) propagates to the consumer; it must not be silently
  absorbed by auto-resubscribe — delivered-but-revoked history is an
  application-level fact.
- rejected alternative: catch-up-first-then-subscribe lost events in
  291/300 seeded races; kept as an executable counterexample in the spike.
```

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
  schema version + upcaster model; codec DECIDED (spike — codec_bakeoff):
    codec_id 1 = MessagePack named-field mode; postcard/bincode/msgpack-
    compact disqualified (silent-wrong decodes on same-typed field reorder
    and enum-variant reorder); with 16 KiB dictionaries msgpack-named is
    23% smaller than raw postcard, so evolvability is free
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
  active-index writes batched per commit (vertical-slice F1)
  active index backend: fjall at journal-buffered durability (spike pick)
  snapshot_head; registry replay; projection checkpoints
  subscription runtime per D11 (validated protocol; property tests come with it)
  EXIT GATE: benchmark pointer-block design vs. per-event-key baseline
    (spike ptr_index answered this for the strategy — 55x write amp for
    RMW — re-verify on the real implementation, not just the model)

Phase 5 — Verification/compression
  fold hashes + fold_version enforcement (D4)
    PREREQ: resolve G6 (durable head anchor) and G10 (chain storage layout)
    retention preserves certification frames
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

## Measured performance envelope (round 4; Ryzen 3900X, consumer NVMe)

```text
durable append:   121k ev/s @ p50 2.8 ms (4 writers) … 363k @ p50 14 ms (512)
buffered append:  1.0M (1 writer) / 1.7M (4) / 2.0M (12) ev/s — ~3× RocksDB
sealed replay:    95M ev/s global warm (49M cold); 24.6M across 1k streams
point reads:      0.36 µs p50 / 0.79 µs p99 cached
seal:             0.40 s per 256 MiB segment (~0.22 s compute)
recovery:         0.32-0.38 s/GiB cold (device ceiling); 0.036 warm;
                  fast path ~0.3 s regardless of log size
sealed disk:      30.8 B/event (columnar default tier)
```

These are v1 acceptance targets, not aspirations — each traces to a profiled, checksum-gated
implementation in `spikes/perf_*`.

## One-paragraph summary

Canonical log is truth; batch markers give atomicity; the registry is itself event-sourced with
frozen bootstrap codec; the active path avoids read-modify-write; the sealed path does
consolidation, compression, and indexing; snapshots are O(1), prefix-proven, and invalidatable
via explicit fold versions backed by generated drift tests; DX gets command retry, upcasting,
and test kits in Phase 1; and none of it ships without the crash harness green. That is the
version worth building.
