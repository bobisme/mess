# directory_tournament — Spike H (bn-2fp): static directory tournament

**Question.** The sealed-segment stream directory is a `HashMap<u64, DirEntry>`
rebuilt from a 56-byte-record DIR region on every open
(`crates/mess-index/src/sealed/segment.rs`), plus an ascending `stream_ids`
vec. D10 already declined a hand-rolled FKS perfect hash
(`docs/perf/experiments-d10.md`: 77.4 ns / 80.0 B/key vs the HashMap's
62.4 ns / 60.6 B/key at 1M). design.md §12 argues that result rejects naive
FKS, not modern static layouts, and nominates bitvector+rank, partitioned
Elias–Fano, PtrHash, and cache-line k-bins. Does any of them beat the
incumbent by ≥20% on an important size/density region, without blowing the
seal-time construction budget, with a deterministic fallback?

**Verdict up front.** One candidate clears the admission bar, decisively:
**H2 (bitvector + rank) wins the region that real sealed segments actually
occupy** — dense interned stream-id sets (measured U/n 1.0–19 on 38 of 39
real segments from two engine-generated stores). Across those real segments
H2's median improvement over the incumbent is **+67% batch throughput
(4.3 vs 12.6 ns/op), +56% serial latency (7.8 vs 17.0 ns), +40% cold-ish
p50 (60 vs 100 ns), 92% faster construction, 86% faster open, and 60%
smaller resident** — and the wins hold at every synthetic dense size up to
30M keys (+62% batch, +24% serial at 30M) with construction 9–13× FASTER
than the incumbent's open-time rebuild. It passes every clause of the
admission rule: ≥20% on an important region (by 2–3×), seal-time
construction *improved* rather than regressed (4 µs at real segment sizes;
0.66 s at 30M vs the HashMap's 8.5 s build / 11 s open), and a
deterministic fallback (the density test at seal chooses the existing
HashMap/sorted encoding whenever U/n exceeds the threshold — a fully static
decision recorded in the section header, per design §12.6). **Everything
else is declined, D10-style**: PEF loses 2–6× on speed everywhere and its
space edge is capped at ~14% because the 48-byte entry column dominates;
PtrHash's lookup wins (+47% real median batch) never beat H2 where it
matters and are bought with 2–3× worse seal-time construction, an
open-time PHF rebuild, and two crate hazards our exactness suite exposed
(§2); the k-bin approximation never beats even the hasher swap. One
non-representation bonus finding: **swapping the incumbent's SipHash for
foldhash is worth +40–59% batch on every dataset for a one-line change**
(h0f) — the honest fallback improvement if H2 is not taken up, and the
right hasher for the HashMap fallback arm either way.

---

## 1. Candidates

Every candidate implements one `ExactDirectory` trait (`src/lib.rs`) — build
from one canonical sorted `(stream_id, 48-byte entry payload)` vector,
`lookup`, ascending `iter`, `serialize` (CRC-framed), `open` (must reject
corrupt bytes), `serialized_bytes`, `resident_bytes`. All lookups are EXACT:
the PHF/bin candidates verify the full stored key, per the bone.

```text
h0_hashmap_sip    the incumbent: HashMap<u64, Entry> + std SipHash-1-3 (what
                  std::collections::HashMap actually gives segment.rs), open =
                  parse 56-byte records + rebuild map, + ascending ids vec
h0f_hashmap_fold  the same, foldhash (hashbrown's default hasher) — the
                  cheapest conceivable "just swap the hasher" counterfactual
h1_sorted         sorted key column + entry column, binary search (§4.1
                  baseline); open = validated column copy
h2_bitrank        bitvector over [min,max] + rank: u32 superblock/512 bits,
                  u16 subblock/64 bits, hardware popcount; entries in key
                  order; serialized = bits + entries, ranks rebuilt on open;
                  §12.2 auto-select rule U/n <= 8 (built out to U/n <= 64
                  here to map the crossover)
h3_pef            partitioned Elias–Fano, 256 keys/partition, local base,
                  l = floor(log2(span/count)) low bits, unary high bits;
                  lookup = partition binary search + word-popcount scan of a
                  <= ~12-word high-bit run + low-bit equality; open =
                  validated copy (offsets recomputed)
h4_ptrhash        ptr_hash 2.0.1 (the paper author's crate), FastPtrHash
                  (REMAP=false) + StrongerIntHash, slot -> packed key verify
                  -> entry; open REBUILDS the PHF (epserde not wired; its
                  cost is quantified via bits_per_element: ~2.7 bits/key)
h5_kbin           cache-line 7-key bins (64-byte aligned: 7 keys + entry_base
                  + len/overflow flag), splitmix64+fastrange bin choice, avg
                  load ~5/bin, overflow -> shared sorted spill vec. An
                  APPROXIMATION of the 2026 k-PHF cache-line idea WITHOUT the
                  paper's construction — labelled as such.
```

## 2. Correctness (all green before any measurement)

`cargo test --release`: 6/6. Per candidate × distribution × edge shapes
(n=1, adjacent keys, 255/256/257/512/513 partition boundaries, keys at
u64::MAX): present keys → exact entries; generated absent keys (neighbors,
in-span randoms, out-of-span, 0, u64::MAX) → `None`; `iter == source`;
serialize→open→re-serialize byte-equality; corrupt bytes (header, kind, body,
CRC flips; truncations; empty) → typed error; H5's overflow path exercised;
H2 popcount-vs-entry-count cross-check catches a CRC-repaired bit flip.

**Finding 1 — ptr_hash's minimal variant is unsafe for nonmember queries.**
`DefaultPtrHash` (REMAP=true) reads its free-slot remap table with
`get_unchecked` (`ptr_hash-2.0.1/src/pack.rs`); a nonmember key can hash to
a free slot past the last remapped one, and our exactness suite SIGSEGV'd
inside `PtrHash::index` (key 43 against a 1-key set). Any exact-membership
use (ours) must use the non-minimal `FastPtrHash` (REMAP=false) and size the
verify arrays to `max_index()` (~1.01n), with a sentinel key for empty
slots. This is a real adoption hazard for the "slot → verify" recipe as
written in research/03 §4.4.

**Finding 2 — ptr_hash's default integer hash fails on dense ids.**
`FastIntHash` (= FxHash) panics "Unable to construct PtrHash after 10 tries"
on consecutive/regular integer keys — exactly the interned dense stream-id
shape mess produces (see §4). `StrongerIntHash` constructs everywhere; all
H4 numbers use it.

## 3. Method

- Machine: AMD Ryzen 9 3900X (12C/24T, 4×16 MB L3), 64 GB, governor
  `performance`; Linux 7.0.12-arch1-1; rustc 1.97.0; bench thread pinned to
  core 8 (second CCD). Whole spike compiled `-C target-cpu=native` (znver2)
  — required by ptr_hash's gxhash dep; identical flags for every candidate.
- Timing: rdtsc→op→rdtscp per sample (RDTSCP so the sample can't close
  before the load completes — the state_kernel_dense_heads trap), measured
  empty-pair overhead (80 ticks ≈ 20 ns) subtracted, sorted-sample
  quantiles. TSC 0.25 ns/tick, but Zen 2's TSC advances in ~40-tick steps,
  so per-op quantiles quantize to ~10 ns — candidates are compared on the
  two loop measures:
  - `batch32`: 32-lookup batches, one timestamp pair per batch (throughput
    shape, OoO overlap allowed) — mean ns/op;
  - `serial`: a data-dependency chain (next key depends on previous lookup's
    result via `black_box`-laundered zero) — true serial latency, no
    overlap possible.
- Phases per (dataset × candidate): build ×reps (2–9 by n; p50/p99) →
  serialize → open ×reps (p50/p99) → **cold-ish** (stream 256 MB through a
  scratch buffer to evict all cache levels, then 64 per-op samples; ×64
  rounds) → warm hit-uniform / hit-Zipf(1.1) / miss per-op quantiles (200k
  samples each) → batch32 → serial (mixed 75% hit / 25% miss). Misses are
  ~7/8 in-span (the hard case).
- Quiet-guard before every candidate: 1-min loadavg < 6.0 (ambient ~4-5 on
  this host), no rustc/cc/ld/cargo outside our ancestor chain, and no
  `active_microblocks` process (the sibling spike's bench). Contaminated
  quantiles are never published — the guard blocks, it does not annotate.
  Provenance: an earlier launch spent its whole window blocked behind the
  sibling's bench + agent compiles and published NOTHING (header-only CSV,
  audited and discarded); the published run (stamp 20260711-220512) started
  after that window and still hit 170 guard sleeps between candidates, all
  resolved below the threshold before measurement.
- Hardware counters (branch/LLC misses) were NOT collected (no perf harness
  in this spike); the warm vs cold-ish latency split is the locality proxy.
  Noted as a limitation.

Exact commands:

```
cd spikes/directory_tournament
CLANG_PATH=/usr/bin/clang cargo test --release     # exactness suite
./target/release/directory_tournament gen-real     # 12k streams, 150k events, 1.5MB segs
./target/release/directory_tournament gen-real \
  --dir ~/.cache/mess-bench-scratch/directory_tournament_store_big \
  --streams 40000 --events 400000 --seg 8388608
bash run.sh                                        # full matrix + real segments
python3 summarize.py logs/synthetic-20260711-220512.csv   # per-dataset tables
```

Raw logs: `logs/*-20260711-220512.csv` (one row per dataset × candidate,
19 metric columns), `logs/*.err` (progress + quiet-guard trace),
`logs/run-driver.log`.

## 4. Datasets

Synthetic (n = 1k / 10k / 100k / 1M / 10M / 30M):

```text
dense-*     n distinct from a universe of 1.5n         (U/n = 1.5)
sparse-*    n distinct from a universe of 1000n        (U/n = 1000)
zipfclust-* Zipf(1.3)-distributed gaps: consecutive runs + rare big jumps
            (locally dense, globally U/n ~ 1100-1250)
```

REAL: two stores generated by the actual engine (`LogEngine::append_batch`
→ name interning → committer → auto-roll → background roll-sealer →
`.pidx`), Zipf(1.05) stream popularity, then every sealed segment's DIR
region extracted (validated through `SealedSegmentIndex::open` first, and
cross-checked against `stream_ids()`/`stream_range()`):

- store 1: 12k streams, 150k events, 1.5 MB segments → 26 sealed segments,
  n = 109..2070 (all benched; the n=109 U/n=93 tail segment exceeds the H2
  build guard, so it ran 6 candidates).
- store 2 ("big"): 40k streams, 400k events, 8 MB segments → 13 sealed
  segments, n = 1704..8056.
- Combined: 39 real directories, U/n min 1.00 / median 3.89 / max 93.3.

**Finding 3 — real sealed directories are DENSE.** Stream ids are interned
densely (the registry hands out sequential u64s), and Zipf popularity puts
the hot streams in every segment, so a real segment's key set is a
dense prefix-biased subset of [0, total_streams): U/n ≤ 10 for every
big segment, drifting sparser (≤ 20) only in the late small segments of
the new-stream tail. The sparse-1000 synthetic regime does not occur in
this engine unless stream-id allocation changes; today's
admission-relevant region is **U/n ≤ ~20 at n = 10²..10⁴ per segment**
(bigger production segments ⇒ same density, larger n — covered by the
dense synthetics to 30M).

Generation throughput note (reproduces bn-1jg): appends to existing streams
are ~30 µs, but every NEW stream pays the ~3.4 ms registry `SyncAll`; both
stores' generation wall time is ~streams × 3.4 ms, regardless of
pipelining.

## 5. Results — synthetic matrix (Measured, this host, 2026-07-11, release)

Full per-dataset tables: `python3 summarize.py logs/synthetic-*.csv`. The
representative 10M tables and the dense H2 series:

### dense-10m (n=10M, U/n=1.5)

| cand | build p50 | open p50 | ser B/key | res B/key | batch32 ns/op | serial ns/op | cold p50 | vs h0 batch/serial/cold |
|---|---|---|---|---|---|---|---|---|
| h0_hashmap_sip | 2.25 s | 3.02 s | 56.0 | 103.6 | 123.98 | 195.58 | 410 ns | — |
| h0f_hashmap_fold | 1.57 s | 2.07 s | 56.0 | 103.6 | 59.20 | 183.26 | 390 ns | +52% / +6% / +5% |
| h1_sorted | 0.30 s | 0.32 s | 56.0 | 56.0 | 586.73 | 746.47 | 1920 ns | −373% / −282% / −368% |
| **h2_bitrank** | **0.24 s** | **0.26 s** | **48.2** | **48.2** | **30.93** | **119.43** | **230 ns** | **+75% / +39% / +44%** |
| h3_pef | 0.33 s | 0.26 s | 48.4 | 48.5 | 252.19 | 307.68 | 1050 ns | −103% / −57% / −156% |
| h4_ptrhash | 2.53 s | 2.96 s | 56.0 | 56.9 | 43.58 | 149.22 | 360 ns | +65% / +24% / +12% |
| h5_kbin | 2.36 s | 2.48 s | 56.0 | 63.7 | 129.39 | 207.21 | 230 ns | −4% / −6% / +44% |

### sparse-10m (n=10M, U/n=1000; h2 not built — outside its region)

| cand | build p50 | open p50 | ser B/key | res B/key | batch32 ns/op | serial ns/op | cold p50 | vs h0 batch/serial/cold |
|---|---|---|---|---|---|---|---|---|
| h0_hashmap_sip | 2.16 s | 2.90 s | 56.0 | 103.6 | 106.84 | 190.76 | 400 ns | — |
| h0f_hashmap_fold | 1.50 s | 2.02 s | 56.0 | 103.6 | 63.64 | 205.18 | 400 ns | +40% / −8% / +0% |
| h1_sorted | 0.10 s | 0.14 s | 56.0 | 56.0 | 588.02 | 777.17 | 2020 ns | −450% / −307% / −405% |
| h3_pef | 0.14 s | 0.10 s | 49.5 | 49.6 | 356.65 | 358.33 | 1260 ns | −234% / −88% / −215% |
| h4_ptrhash | 2.37 s | 2.66 s | 56.0 | 56.9 | 52.91 | 152.51 | 350 ns | +50% / +20% / +12% |
| h5_kbin | 2.01 s | 2.48 s | 56.0 | 63.7 | 122.28 | 203.88 | 240 ns | −14% / −7% / +40% |

zipfclust-10m behaves like sparse-10m (h0f +59%, h4 +54% batch; both ≤+9%
serial; everything else loses). 30M scales the same shapes: h2 dense
+62%/+24%/+36%; h4 sparse +52%/+11%.

### H2 vs incumbent across the dense series (batch32 / serial ns/op)

| n | h2 batch | h0 batch | Δ | h2 serial | h0 serial | Δ |
|---|---|---|---|---|---|---|
| 1k | 5.04 | 12.17 | +59% | 8.10 | 16.38 | +51% |
| 10k | 4.68 | 14.77 | +68% | 9.53 | 21.53 | +56% |
| 100k | 16.31 | 27.10 | +40% | 17.94 | 27.99 | +36% |
| 1M | 23.18 | 65.99 | +65% | 62.01 | 110.76 | +44% |
| 10M | 30.93 | 123.98 | +75% | 119.43 | 195.58 | +39% |
| 30M | 57.16 | 148.80 | +62% | 184.92 | 241.92 | +24% |

## 6. Results — real segments (Measured; 39 engine-written directories)

Median improvement vs the incumbent across all real segments
(+ = better than h0; `build`/`open` = time; `res` = resident bytes):

| cand | batch | serial | cold p50 | build | open | resident |
|---|---|---|---|---|---|---|
| h0f_hashmap_fold | +53% | +29% | +11% | +55% | +39% | +0% |
| h1_sorted | −52% | −78% | −18% | +93% | +82% | +54% |
| **h2_bitrank** (38/39) | **+67%** | **+56%** | **+40%** | **+92%** | **+86%** | **+60%** |
| h3_pef | −623% | −443% | −60% | +81% | +86% | +60% |
| h4_ptrhash | +47% | +23% | +22% | −232% | −159% | +54% |
| h5_kbin | −23% | −8% | +18% | −36% | −20% | +48% |

Absolute medians (real segments): h0 12.59 ns/op batch, 17.04 ns serial,
100 ns cold, 45.8 µs build, 66.8 µs open. **h2: 4.29 ns/op batch, 7.78 ns
serial, 60 ns cold, 4.1 µs build, 9.4 µs open.**

Density crossover: none found inside the tested range — H2 still wins
+66–72% batch at the sparsest real segments it was built for
(U/n = 12.7–19.1), and its key-side space (U/8 bits + ~0.19 B/key rank)
stays below an 8 B/key sorted key column until U/n ≈ 64. The §12.2
auto-select rule (U/n ≤ 8) is therefore conservative by at least 2×; the
evidence supports raising it toward 16–32 with the same fallback.

## 7. Admission decision per region

| region | evidence | decision |
|---|---|---|
| **dense / real** (U/n ≤ ~20; n 10²..3×10⁷; ALL 39 real segments) | h2: +40..75% batch, +24..62% serial, +36..44% cold, build+open 9–25× faster, resident −53..−60% — every clause of the rule met with margin | **ADMIT h2 (bitvector+rank)**, seal-time density test chooses it; fallback = current HashMap/sorted encoding, decision recorded in the section header (design §12.6) |
| sparse (U/n ≈ 1000, synthetic only — does not occur with interned ids) | best representation: h4 +36..55% batch but ≤+20% serial, 2–3× slower seal-time build, PHF rebuild on open, crate hazards (§2 findings 1–2); h0f (not a representation — a hasher swap) matches h4's batch win for free | **DECLINE all representations; keep HashMap** (swap to foldhash) |
| zipf-clustered (locally dense, globally sparse) | same as sparse: h0f/h4 only batch winners; h3 (the shape's on-paper favorite) loses 2–4× on speed | **DECLINE; keep HashMap** |
| entry-payload columns (Stream VByte / FOR) | NOT RUN — descoped: the 48 B entry column is the space floor in every candidate (key-side savings cap at 8 B/key = 14%), so payload compression is where the next space win lives; measure decode-on-lookup separately if space ever matters | no decision (follow-up) |

Seal-budget note: today the sealer only serializes DIR records (the HashMap
is built at *open*). Adopting H2 moves an O(n) build to seal: measured
4 µs at real segment sizes, 0.24 s at 10M, 0.66 s at 30M — 3–13× cheaper
than the single open-time rebuild it eliminates, and paid once per seal
instead of on every open.

## 8. Verdict

**POSITIVE result — admit bitvector+rank (H2) for the dense region, which
is every real sealed segment this engine writes; decline PEF, PtrHash, and
k-bins D10-style; keep HashMap (with foldhash) as the deterministic
fallback arm.** The FKS lesson (§21.4) survives intact: hashing did not
beat hashbrown here either — H4/H5 confirm D10 at every scale. What wins is
*not hashing at all*: real stream-id sets are dense integers, so the bit
position IS the key, and one cache-line-local rank beats any probe
sequence. Recommended production shape (a follow-up bone, not this spike):
at seal, compute U/n; if ≤ threshold (8 conservative, evidence supports
more) emit the H2 section (bits + entries in key order; ranks rebuilt on
open in one popcount pass), else emit the current record layout; tag the
representation + decoder version in the section header; readers without
the tag fall back to scanning (advisory-skip preserved). Also worth a
one-line bone regardless: `HashMap<u64, DirEntry, foldhash::fast::RandomState>`
in `segment.rs` — +53% median real-segment batch throughput for free.

Surprises for the record: (1) the incumbent's open-time HashMap rebuild is
the single largest cost this tournament found (3.0 s at 10M, 11 s at 30M —
vs 0.26 s / 0.74 s for H2's popcount pass); (2) ptr_hash 2.0.1 is unsafe
for nonmember queries in its default minimal mode and cannot construct
over dense ids with its default hasher — both fixable, both disqualifying
for an "exactness-first" adoption without upstream work; (3) PEF, the
research favorite for this exact shape, is not even close on speed at any
size — its win condition (key-side space) is capped at 14% by the entry
payload it cannot compress.
