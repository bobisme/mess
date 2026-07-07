# 15 — Spike results, round 3

Recorded: 2026-07-07. Rounds 1–2 ([13](./13_spike_results.md), [14](./14_spike_results_round2.md))
validated the components and the composed slice. Round 3 closed the remaining unspiked areas:
the Phase 1 codec decision, the fold-certificate mechanism, the seal pass (carrying the round's
biggest question: does sealing flip the vertical-slice losses?), and recovery at scale. All
numbers measured on this machine; full details in each spike's `REPORT.md`.

## Scorecard

| Spike | Question | Verdict |
|---|---|---|
| `spikes/seal_pipeline` | Does sealing flip the vertical-slice stream-replay and disk losses? | **Both columns flip** — stream replay 1.55× *over* the RocksDB target, disk 5.3× *below* it; seal costs 1.5 s per 256 MiB segment |
| `spikes/codec_bakeoff` | Which payload codec, and does the upcaster model work? | **MessagePack named-field mode** — the only fast evolution-safe option; dictionaries make it smaller than raw postcard; upcaster chain proven with byte fixtures |
| `spikes/fold_cert` | Is the fold-certificate mechanism sound and affordable? | **Yes at runtime (~165 ns/event), but the spec is not implementable as written** — 10 gaps, 2 blocking |
| `spikes/recovery_scale` | What segment size, and what does recovery really cost? | **256 MiB** — fast-path recovery 0.32 s flat regardless of log size; full rebuild ~1.1 s/GiB regardless of segment size |

## 1. seal_pipeline — the seal pass flips the thesis

Input: a real 258.5 MiB batch-framed segment (1M events, ~271 B/event, 7,411 streams Zipf 1.1,
4 categories). Seal pass: consolidate per-event index → variable-length varint-delta pointer
blocks; BinaryFuse filters; per-category 16 KiB zstd dictionaries; recompress into ~128-event
blocks; sealed artifacts + footer.

| metric | before seal | after seal | reference target |
|---|---|---|---|
| stream replay (1,000 random) | 1.61M ev/s (ptr-chase) | **2.72M ev/s** | RocksDB 1.75M → **beaten 1.55×** |
| hottest-stream replay | 1.66M ev/s | **7.40M ev/s** | 4.2× over target |
| disk, all-in | 271 B/event | **44.0 B/event** | RocksDB 232.6 → **5.3× smaller** |
| global replay | 4.24M ev/s | 6.40M ev/s | |

```text
seal cost: 1.50 s single-threaded for 256 MiB / 1M events
  (read 149 ms, dict training 351 ms, recompress+write 767 ms,
   consolidate 20 ms, filters 0.5 ms, metadata+fsync 172 ms)
peak RSS ≈ the input segment buffer (376 MiB)
payload compression 6.0× overall (4.9–7.3× per category) — top of the D6
  band, helped by seal-time (category, stream, version) clustering
mechanism isolation: packing/coalescing alone (uncompressed) = 9.1M ev/s
correctness: all 7,411 streams byte-identical across the seal (0 mismatches)
```

Filters, measured: BinaryFuse8 = 0.372% FPR at 1.32 B/key; **BinaryFuse16 = 0.002% FPR at
3.61 B/key (~26 KiB/segment), ~4 ns/query, zero false negatives → the default.**

Honest caveats recorded: cached point-read p99 is linear varint-seek within a block on hot
streams (wants a small skip table); a small decompressed-block LRU earns 48% on replay but only
3.3% on random point reads — point-read-heavy workloads need a different answer (per-event-with-
dict hot tail, per round 1).

## 2. codec_bakeoff — Phase 1 decision settled

**`codec_id 1` = MessagePack, named-field mode (`rmp_serde::to_vec_named`).**

The disqualifier — the evolution matrix (9 schema-change scenarios per codec):

```text
postcard / bincode / msgpack-compact:  SILENT-WRONG on same-typed field
  reorder (width/height decode swapped, no error); postcard/bincode also
  decode Created(9) as Deleted(9) on enum-variant reorder — a different
  business event, silently.
json / cbor / msgpack-named:           ZERO silent-wrong cells. Additive
  changes, removal, reorder, variant add/reorder, int widening all correct;
  rename fails loudly (= the upcaster trigger).
note: the trap is SAME-TYPED adjacent field swaps — differently-typed swaps
  error by luck, which is how this bug class hides.
```

Size — dictionaries make evolvability free:

```text
raw:            msgpack-named 280.5 B/event vs postcard 94.2 (3.0×)
16 KiB dict:    msgpack-named 72.6 B/event — 23% SMALLER than raw postcard
vs dict'd postcard: +10–12 B/event premium (51.1 vs 40.6 at 128-ev blocks)
postcard REGRESSES under per-event zstd on small events (~13 B frame overhead)
```

Speed: msgpack-named is the fastest evolution-safe codec (1.82M encode / 1.39M decode ev/s
single-threaded; 1.5× json decode, 2.2× cbor). Nowhere near the fsync-batched log's bottleneck.

Upcaster prototype: `StoredEvent(event_name, schema_version, codec_id, payload)` →
version-matched decode → one-hop `Upcast` impls (V1→V2→V3 with a real rename and unit change) →
macro-generated transitive dispatch. Committed V1/V2 byte fixtures decode to exact V3 values;
unknown version/codec fail loudly.

Frame-format consequences (folded into D2/D3):

```text
schema_version: u16 PER EVENT, beside event_type_id (the upcaster dispatch key)
dict_id: u16 beside compression_id (0 = none); dictionaries stored durably
  via the registry (D3)
zstd magicless framing (reclaims ~13 B/event of header)
postcard remains fine for frozen internal frame structures — never payloads
```

## 3. fold_cert — mechanism validated, spec must be revised

Measured (1M events, 250 B payloads, BLAKE3):

```text
append: 5.71M ev/s raw → 1.51M ev/s with full chain (~165 ns/event added;
  ~3% of the composed append budget)
load_verified: 0.7 µs (empty tail) … ~0.5 µs per tail event
full-chain verify of 1M events: 504 ms (1.98M ev/s)
```

All 13 attack scenarios detected (corrupt blob, wrong-version claim, tail/last-frame/prefix
tamper, truncation ×2, reorder, cross-stream snapshot, forged header, fold_version mismatch).
Counterfactual proven: without stream_id in the genesis hash, the cross-stream snapshot attack
SUCCEEDS — stream_id-in-genesis is mandatory, not defense in depth.

Both D2 verification paths implemented: Path A recomputes `h[v]` from frame v alone (needed
when the tail is empty; requires recomputing frame_hash — spec was silent); Path B compares
`frame[v+1].prev_stream_hash` (one memcmp; only sound combined with tail replay).

10 spec gaps found; the two blocking ones:

```text
G6 (biggest): the trusted head anchor is unspecified. Truncation detection
  anchors to the stream head — but if the head record is rebuilt from the
  log during recovery (as I5 encourages), the anchor anchors to nothing.
  The head chain value needs an existence guarantee OUTSIDE the rebuildable
  index: segment footer or manifest.
G10 (dominates real cost): where h[i-1] physically lives in the BATCH format
  is undecided. Two 32 B hashes per ~250 B frame ≈ 25% storage overhead if
  materialized per frame; per-batch materialization + intra-batch recompute
  is the likely answer. This decision, not CPU, is the mechanism's price.
```

Others: docs 08/12 disagree on the genesis formula; no byte-exact hash encodings anywhere;
version-0/empty-prefix snapshot representation undefined; compaction can delete both frames a
verification needs (retention must preserve certification frames or convert to a retention
certificate per doc 08 §8).

Verdict: keep — every modeled attack short of head-record compromise is detected at negligible
runtime cost — but D4 is design-incomplete until G6/G10 are specified.

## 4. recovery_scale — segment size decided

31 GiB generated and measured (1/4/10 GiB logs × 64/256/1024 MiB segments), then self-cleaned.

```text
FULL recovery depends ONLY on total log size (<8% spread across segment
  sizes): ~1.1 s/GiB cold, ~0.76 s/GiB warm. 10 GiB ≈ 11 s cold.
  Segment sizing cannot rescue full-rebuild time.
LAST-SEGMENT-ONLY (A7/F5 fast path) depends ONLY on active-segment size:
  64 MiB → 0.10 s | 256 MiB → 0.32 s | 1 GiB → 1.16 s cold
  — flat as the log grows.
DECISION: 256 MiB. (1 GiB crosses the 1 s startup line; 64 MiB quadruples
  file/footer counts for speed nothing needs.)
CRC32C vs BLAKE3 recovery scan: ~1,350 vs 590 MiB/s → mandatory crypto
  would be 2.1–2.3× slower; opt-in chain (D2) confirmed empirically.
SIGKILL of a real child writer ×5: torn tails detected and truncated
  exactly; durable prefix intact; idempotent re-recovery.
```

New spec items (folded into D2):

```text
R1: recovery scan is CPU-bound (~1.3 GiB/s/core) — spec permits parallel
    per-segment scans; sealed footers provide the A1 contiguity seeds.
R2: fast path is O(#sealed segments) footer preads (~0.25 ms each cold);
    the advisory manifest caches footers.
R3: the segment footer carries the A9 epoch.
R4: hash AROUND the CRC fields (split coverage), never copy-and-zero the
    batch to verify it.
```

## What changed in doc 12 as a result

```text
D2:  per-event schema_version + dict_id fields; 256 MiB segment size;
     R1–R4 recovery amendments
D3:  compression dictionaries registered durably (dict_id -> registry)
D4:  validated with numbers; G6 (durable head anchor) and G10 (chain
     storage layout) added as blocking design items; genesis formula to be
     unified and byte-exact, stream_id mandatory in genesis
D5:  seal verdict recorded — packed blocks + coalescing + block decompress
     beat the RocksDB stream-replay target 1.55×; BinaryFuse16 default
D6:  sealed all-in 44 B/event (5.3× below RocksDB); 6.0× payload compression
Phase 1: codec decision settled (msgpack named + upcaster model)
Phase 5: retention must preserve certification frames
```

## Standing verdict after three rounds

Eleven spikes. The thesis is now measured end-to-end in both states: pre-seal the custom engine
trades wins with RocksDB; post-seal it wins every storage column — stream replay 1.55×, hot
streams 4.2×, global replay 3.5×, disk 5.3× — for 1.5 s of background work per 256 MiB segment.
The correctness story spans crash, reordering, subscription handoff, certificate attacks, and
real SIGKILLs. The remaining design debt is enumerated, not open-ended: G6/G10 on fold
certificates, F4/F8 from the vertical slice, and the Phase 0 semantics spec that now has
every number it needs. There is nothing left that a spike answers better than building v1.
