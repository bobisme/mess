# Spike C — open_without_book (bn-2ib)

**Production-change spike** (unlike the frozen side-crate spikes): the code
lives in the engine (`crates/mess-store/src/engine.rs` + small additive APIs
in `mess-log`/`mess-index`); this REPORT documents the hypothesis, method, and
measured result. Merge candidacy is gated on the correctness + performance
gates below.

## Hypothesis

The composed engine's public API can be served from log/`.pcol` blocks with
bounded caches — no all-history in-process payload mirror (the `Book`) — with:
zero old payload decodes on open, <=50% of the Book baseline's peak RSS on a
2M+ corpus, hot repeated loads within 10% of current, replay within the
existing envelope gates, and fewer append allocations/event.

## What changed (summary for the reviewer)

- **`Book` shrank to interners + heads** (O(streams + types), not O(events)):
  `payloads` and `stream_events` are gone; no payload byte lives in the
  engine outside the bounded caches.
- **Real `EventPtr`s**: `mess-log`'s `AppendOutcome::Acked` now carries the
  batch's durable `(segment_id, offset)` (the writer `Receipt`'s placement),
  and the publish step stamps it into the `ActiveIndex` — previously live
  entries carried pseudo pointers (`segment_id = 1`, `offset = global_pos`)
  that were never dereferenced.
- **Block-native reads**: a bounded, bytes-weighted decoded-capsule cache
  (`BlockReader`, `quick_cache` — already a workspace dep) keyed by
  `(segment_id, offset)`. Miss = one header `pread` + one batch `pread` +
  the recovery scanner's byte layer (A4/A12 CRC always —
  `scanner::accepted_batch_at`, new additive API) + one frame-decode into an
  immutable arena. Identity (stream/gp/version/frame-count) is cross-checked
  against the resolving index entry; any mismatch falls back to a
  locate-by-scan of the segment, which **bulk-warms every batch of the
  scanned segment into the capsule cache under its legacy pseudo key**
  (review F3), so a sequential legacy replay pays ~one scan per cache window
  rather than one scan per batch. This is what keeps **pre-bn-2ib stores
  with pseudo-pointer sealed sidecars readable** — correct always, and
  amortized while the cache can hold the warm (with the cache disabled the
  old scan-per-batch worst case is the floor; `mess rebuild-index` upgrades
  legacy sidecars to real pointers and is the real fix). The v3 golden
  exercises exactly this path.
- **Sealed payload bytes from `.pcol`**: when the covering sealed segment has
  a payload sidecar, the capsule's payload arena is reassembled through the
  new `SealedPayloadIndex::reassemble_range` (block-granular, decodes only
  covering blocks); any `.pcol` error or coverage gap falls back
  byte-identically to the raw frames (verify-on-seal proved equality at seal
  time; the log stays truth).
- **Recovery is O(unsealed bytes)**: fully-sealed non-head segments whose
  sidecar coverage is contiguous (own header base == running watermark, next
  header base == coverage end) are not read beyond their 52-byte header —
  heads come from the sidecar directory, the watermark from its event count.
  Unsealed segments (and always the live head) get a batch-metadata scan
  (`recover_segment`; CRC yes, frame decode **no**). Chain-on stores still
  scan + fold everything (spec 05 §6 requires every payload).
- **Seal pipeline re-sourced (review V2, mandatory)**: the roll-sealer gates
  readiness on `ActiveIndex::applied_end` **and the published read
  watermark** (previously the Book's length) and builds both the pointer
  sidecar and the `.pcol` payloads by **reading the rolled raw segment
  back** (`seal_input_from_segment`). Sealed sidecars now carry real byte
  offsets. `seal_active` likewise re-reads the live head clamped to the
  published watermark — incidentally fixing its post-roll behavior (it
  assumed segment 1 / base 0) and the reopen-after-`seal_active` tail loss
  (positions past the sidecar's coverage are now hot-seeded on recover).
- **Subscriptions/watermark semantics unchanged**: the published read
  watermark advances exactly where it used to (end of publish, in turn
  order); `total_events()`/metrics read it instead of the Book length.

### Known deviation / finding for the design docs

`StoredRecord.message_type` requires each event's `event_type_id`, and **no
sealed sidecar stores type ids** (`.pidx` = positions/offsets, `.pcol` =
payload bytes only). So a cold read still decodes the raw batch once (cached)
for frame identities, and takes payload bytes from `.pcol`. Until a SealPack
(or capsule format) carries per-event type ids, sealed reads cannot be served
from sidecars alone. Flagged for design.md §14 / the SealPack consolidation
(the bn-23x/bn-2gg dependents).

## Method

Harness: `crates/mess-store/examples/owb_bench.rs` (public API only, so the
identical binary measures both builds; one phase per process so `VmHWM` is
per-phase peak RSS; counting global allocator for allocs/event).

Corpus: 2,000,000 events, 1,000 streams, 10-event batches, ~64 B MessagePack
payloads, 8 MiB segments -> 24 segments, 23 sealed (with `.pcol`),
`Durability::Process`.

```
cargo run -p mess-store --release --example owb_bench -- seed   <dir> 2000000 1000 10 64 8
cargo run -p mess-store --release --example owb_bench -- open   <dir>   # x3
cargo run -p mess-store --release --example owb_bench -- hot    <dir> 200
cargo run -p mess-store --release --example owb_bench -- replay <dir>
cargo run -p mess-store --release --example owb_bench -- point  <dir> 20000
cargo bench -p mess-store --bench engine_envelope                        # envelope gates
```

Baseline measured at workspace commit `558cf11d` (engine untouched); "after"
measured in the same session on the same corpus shape and same host.

## Results (Measured)

All rows Measured on this host. Baseline = pre-bn-2ib engine built from
`558cf11d` in a side worktree; both binaries run the identical harness,
interleaved in the same session (same background load) on same-shape corpora.

### 2M events, 1,000 streams, 10-event batches, ~64 B payloads, 24 segments (23 sealed + `.pcol`)

| metric | Book baseline | block-native | gate | verdict |
|---|---|---|---|---|
| reopen wall (best of 3, warm cache) | 2.449 s | 1.947 s | report | 20% faster |
| reopen peak RSS (VmHWM) | 420,736 KiB | 104,804 KiB | <= 50% of base | **PASS (25%)** |
| open payload frame decodes (23/24 segments sealed) | 2,000,000 (every event) | **0** (`recover_payload_decodes()`, asserted in tests) | zero | **PASS** |
| hot repeated load (2,000-event stream, µs/load) | 758 | 385 | <= +10% | **PASS (2.0x faster)** |
| cold global replay (ev/s, paged 4096) | 5.74 M | 1.80 M | see envelope row | see note |
| cold stream replay, all 1k streams (ev/s) | 3.62 M | 1.69 M | see envelope row | see note |
| cold random point read (µs) | 0.38 | 13.1 | report | see note |
| append allocs/event (2M-event seed loop) | 13.99 | 12.42 | decrease | **PASS (-11%)** |
| append throughput (seed, ev/s, same load) | 139,290 | 143,695 | report | +3% |
| seed peak RSS (VmHWM) | 533,900 KiB | 218,428 KiB | report | 41% of base |

### 10M events, 5,000 streams, 59 sealed segments (~1.2 GB store)

| metric | Book baseline | block-native | verdict |
|---|---|---|---|
| reopen wall (warm) | 5.91 s | 3.91 s | 34% faster |
| reopen peak RSS (VmHWM) | 1,872,388 KiB | 302,648 KiB | **16% of baseline — PASS** |
| hot repeated load (µs/load) | 1,078 | 425 | 2.5x faster |
| cold global / stream replay (ev/s) | 5.00 M / 2.45 M | 0.90 M / 1.06 M | see note |

### Envelope gates (`cargo bench -p mess-store --bench engine_envelope`, release, same session)

| gate | baseline (`558cf11d`) | block-native | floor | verdict |
|---|---|---|---|---|
| buffered append | 2.72 M ev/s | 3.07 M ev/s | >= 1.00 M | **PASS** (and faster: Book pushes left the publish tail) |
| sealed stream replay | 3.42 M ev/s | 4.55 M ev/s | >= 2.50 M | **PASS** (and faster) |
| recovery fast path | 148 µs | 128 µs | <= 0.5 s | **PASS** |

**Cold-replay note (the honest cost).** On a deliberately harsh corpus —
10-event batches, EVERY read cold — full-history replay through the bounded
caches is 2-5x slower than reading an unbounded in-RAM payload mirror, and a
cold random point read pays ~13 µs (pointer-directory + block decode) vs an
array index. That is the price of O(1) memory: the baseline "wins" these rows
by holding all 630 MB of payloads resident (1.9 GB RSS). The formal replay
gate — the envelope bench's sealed-replay floor — passes with 1.8x headroom
and got FASTER, because envelope-shaped batches (5,000 events) amortize the
per-batch pread/CRC that dominates the 10-event-batch corpus. Repeated
(cache-warm) reads are 2-2.5x faster than the Book across every corpus.

## Correctness gates

| gate | result |
|---|---|
| full workspace test suite (`MESS_SNAPSHOT_LAW_ITERS=25`) | PASS (one pre-existing timing-sensitive perf test, `mess-log early_close_beats_sync_per_batch_at_4_writers`, is flaky under parallel load and passes in isolation) |
| differential byte-identity (`crates/mess-store/tests/engine_block_reads.rs`): live mixed hot/sealed, reopened, reopened+appended — every `read_global`/`read_stream` page shape + `head` vs a full oracle | **PASS** |
| seal-with-no-Book: `.pcol` reassembly byte-identical vs raw frames (verify-on-seal ships and runs on every seal; the differential test reads all sealed history back byte-exact); pointer/version/global-position identity via the read APIs | **PASS** (semantic gate; byte-identical `.pidx` vs the OLD path is intentionally not met — offsets are now real, see below) |
| truncated/corrupt `.pcol` -> open skips it (CRC) / read falls back to raw, byte-identical | **PASS** |
| bounded cache: 2 KiB budget (evicts every read) and budget 0 (disabled) return identical results | **PASS** |
| legacy pseudo-pointer sidecars (v3/v4 goldens, `mess-cli tests/golden.rs`) open + replay byte-exact via the locate-by-scan fallback | **PASS** |
| `mess rebuild-index` byte-equal rebuild | **PASS** (updated: rebuilds now write the batch's real byte offset, matching the new sealer; `crates/mess-cli/src/rebuild.rs`) |

## Sidecar format note (reviewer attention)

Sealed `.pidx` sidecars written by this engine now carry the batch's **real
byte offset** where the old engine wrote the global position as a pseudo
offset. Same encoding, different values — old sidecars still parse and are
served through the identity-checked locate-by-scan fallback (one segment scan
per first miss, then cached), and `mess rebuild-index` upgrades them. New
sidecars are what the block-native read path dereferences directly.

## Two implementation findings worth keeping

1. **quick_cache shard admission**: an item heavier than its SHARD's hot
   budget is silently never admitted. The sealed global-batch directory
   (one multi-hundred-KiB item per segment) had to be an UNSHARDED cache or
   every cold global page re-decoded a segment's pointer blocks (~400 µs
   per point read, 30x). Any future bytes-weighted cache of large objects
   must set `shards(1)` or size shards explicitly.
2. **`.pcol` block decode amplification**: reassembling per batch decodes a
   whole 128-event block per overlapping batch — a 10-event-batch corpus
   pays ~12.8x. Fixed with a bounded decoded-block cache
   (`BlockReader::pcol_blocks`); sequential global replay now decodes each
   block ~once. Scattered per-stream cold replay still pays amplification
   when the working set exceeds the block-cache budget — a SealPack that
   co-locates a stream's payloads (design.md Phase 5) is the real fix.

## Adversarial-review findings addressed (FIX-FIRST round)

- **F1 (blocking, confirmed): stale decoded-`.pcol`-block cache across
  re-seals.** A segment can be sealed twice with grown coverage
  (`seal_active` twice, or `seal_active` then the roll-sealer re-sealing the
  same segment); a decoded partial tail block cached under the first
  sidecar disagreed with the second sidecar's block map — integer underflow
  panic in `pcol_range` while holding the book mutex (reviewer repro
  reproduced, then confirmed fixed). Fixed twice over: (1) `SealedStore`
  now stamps a monotone **install generation** per segment
  (`get_with_gen`/`segments_with_gens`), and both derived caches —
  decoded `.pcol` blocks AND sealed global batch directories (F6) — key by
  it, so a re-seal starts a fresh cache lineage; (2) `pcol_range` now
  cross-checks every cached block against THIS sidecar's block entry
  (first_event/n_events/coverage) and returns a typed
  `PayloadError::Corrupt` instead of clamping — the caller falls back to
  the raw frames. Regression tests
  (`reseal_after_growth_serves_fresh_pcol_blocks`, modeled on the reviewer
  repro, plus `roll_reseal_after_seal_active_serves_fresh_pcol_blocks`)
  were verified to reproduce the exact original panic with both fixes
  reverted and pass with either in place.
- **F2: sidecar durable before segment data.** The seal makes the sidecar
  durable strictly before the footer's whole-file fsync, so a crash between
  the two could leave a CRC-valid sidecar covering bytes that never reached
  the device. `load_sealed` now installs a sidecar only when the segment
  carries a **valid footer trailer that cross-checks** (segment_id,
  base_pos, end_pos == coverage end); anything else — including on-demand
  `seal_active` sidecars over the live head, which never have a footer —
  becomes a *pending candidate* that `recover` installs only after its scan
  proves the durable committed prefix reaches the claimed coverage end. A
  refuted candidate is not installed; the segment is served from the log.
  Regression test: `sidecar_durable_before_data_is_not_trusted_on_reopen`
  (footer + tail bytes stripped, sidecar left) — the surviving prefix reads
  back byte-identical and dense, nothing fabricated.
- **F3: legacy-store replay cliff** — fixed via the locate-by-scan bulk
  warm described above; the earlier REPORT claim ("one segment read" per
  miss) was wrong and has been corrected.
- **F4 (ride-along, fixed): `head()` vs index visibility.** The publish
  step now updates the interner head strictly AFTER
  `ActiveIndex::apply_committed`, so a concurrent `head()` can never name a
  version `read_stream` (which now resolves through the index) cannot yet
  serve. Pre-bn-2ib both lived under one book-lock section; this restores
  that ordering guarantee in the new shape.
- **F5 (REPORT correction):** the earlier "additive API" wording was wrong
  in Rust terms — `AppendOutcome::Acked` gained fields and `EngineError`
  gained a variant, both **source-breaking** for exhaustive
  matches/constructions outside this workspace (all in-repo consumers were
  updated in the same commits). Neither enum is `#[non_exhaustive]`, and no
  enum in this workspace uses that attribute; adopting it is left as a
  deliberate style decision for the lead rather than snuck in here.
- **F6: `global_dirs` cache invalidation** — same missing-generation bug
  class as F1 (masked in-process by active-preferred routing); covered by
  the same generation-key fix.

All performance gates were re-measured after this fix round and are
unchanged-to-better (Measured, same host/corpus: open 1.66 s / 106 MiB VmHWM,
hot 397 µs/load, cold replay 1.80 M / 1.68 M ev/s, point 13.4 µs; envelope
append 3.39 M ev/s, sealed replay 4.85 M ev/s, recovery fast 119 µs — all
PASS). Affected suites re-run green after the fixes: mess-store + mess-index
+ mess-cli (45 test binaries, 0 failures, snapshot_law capped at 25 iters)
plus mess-log crash/sigkill harnesses; `engine_block_reads` now carries the
three review regression tests, and the F1 test was verified to reproduce the
reviewer's exact underflow panic with both halves of the fix reverted.

## Verdict

**PASS — merge candidate.** All mandatory gates green: zero open decodes,
RSS 25% (2M) / 16% (10M) of the Book baseline, hot loads 2x faster (gate was
"no more than 10% slower"), envelope replay/append gates pass and improved,
allocations/event down 11%, byte-identity proven differentially incl. cache
starvation and `.pcol` corruption, and the seal pipeline no longer touches
any in-process mirror. The known regression is fully-cold fine-batched
replay/point reads vs an unbounded in-RAM mirror (documented above, formal
gates unaffected); the SealPack phase addresses it structurally.

## Machine

AMD Ryzen 9 3900X (24 threads), 64 GB RAM, consumer NVMe, ext4 `$HOME`.
Desktop background load ~2 cores (no competing cargo/rustc during
measurement). Date: 2026-07-11.
