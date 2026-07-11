# epoch_dedupe — Spike G (bn-2j5): exact epoch dedupe tournament

**Question.** Can exact dedupe eliminate mutable-KV write/delete work without
regressing latency — does the epoch design (design.md §13, research/03 §7) beat
a Fjall primary+order implementation on latency, write traffic, and footprint
while staying EXACTLY correct under forced fingerprint collisions, window
boundaries, epoch straddling, and checkpoint-loss rebuild?

**Semantic model under test.** `WindowByGlobalPosition { span: W }`: a key
committed at position `p` is a duplicate at durable end `w` iff `p >= w - W`.
Full keys are stored ONCE in a simulated capsule arena (a `Vec<u8>` blob store
standing in for the log); every index stores only fingerprint + position +
arena ptr, and every fingerprint hit verifies full key bytes from the arena
before declaring a duplicate.

**The baseline is SYNTHETIC (review/11 V1).** Production dedupe is dormant:
`LogEngine` never populates or queries the Fjall `dedupe`/`dedupe_order`
partitions (per-append `CommitGroup` carries `stream_heads` only,
engine.rs:1921-1932; the `DEFAULT_DEDUPE_CAPACITY` FIFO at meta/mod.rs:321-347
has zero callers). G0 here is a best-effort component baseline built for this
comparison — the dormant `MetaStore` shape adapted from entry-count capacity to
position-span eviction. This spike proves a NEW feature; gates are the absolute
latency/write-amp/footprint targets plus the >=5x-Fjall relative escape hatch,
not §21.8's retired "slower than Fjall by >20%" migration criterion.

**Verdict up front.** **Adopt G2 (hashbrown active table + sorted frozen
epochs + BinaryFuse16 negative filter per epoch).** Exactness is proven
everywhere the harness looked, including a zero-bit fingerprint where every
key collides. G2 whole-window miss is p50 100-130 ns at a full 1M-position
window — 12.6x the synthetic Fjall baseline; <= 80 ns only at <= 25% fill (the
intentionally extreme target is missed at full fill, and per research/05 §11
that does not kill epoching because everything else wins big). Footprint is
**0.18x G0 at 16 B keys and 0.06x at 256 B** (gate <= 0.5x); steady-state
insert throughput through a turning-over window is **4.1 M/s vs G0's
0.117 M/s (35x)** with **zero per-key deletes vs G0's 2.0 M** on the same
workload; Zipf-weighted hit checks are p50 60-100 ns (G0: 0.9-1.2 us). G1 (no
filters) is eliminated: without a negative filter a full-window miss
binary-searches every frozen epoch (p50 1.11 us, only 1.5x G0). G3
(Iceberg-style active) ties G2 within noise on probes and wins slightly on
raw insert throughput, but adds a second table shape for no decisive benefit
at this scale — keep it in the back pocket for a future concurrent-writer
active table (its fixed-bin layout is stabler under concurrency). The one
gate G2 misses besides the extreme 80 ns number: uniform-random hits landing
in the OLDEST live epoch are p50 410-510 ns (> 300 ns) — dominated by
binary-search cache misses over the 2 MB fingerprint array plus the arena
verify; Zipf-realistic hit traffic passes with 3-5x margin, and every hit
scenario is 4-11x faster than G0. An Eytzinger/interleaved layout or
per-epoch candidate router (the optional k-PHF/Ribbon idea) is the known
follow-up if worst-case hit latency ever matters.

---

## 1. Candidates

```text
G0  fjall 3.1.6 primary+order      (scope||key) -> (position u64, seq u64); order:
    (synthetic baseline)           seq BE -> (position, pk). One journal-buffered
                                   batch per commit group (64 keys in benches, 1-4 in
                                   differential tests; durability None = I5 contract),
                                   read-before-write drops the stale order row on
                                   overwrite, position-window FIFO eviction pops front
                                   rows (2 per-key deletes each). Mirrors the dormant
                                   MetaStore shape (crates/mess-index/src/meta/mod.rs
                                   apply_group ~:321-347, dedupe_lookup).
G1  hashbrown active + sorted      active: HashMap<u128 fp, SmallVec<(pos, ptr)>>
    frozen epochs                  (identity hasher over the keyed fp). Freeze: sort
                                   into SoA (fp, pos, ptr) arrays; query: binary
                                   search + equal-fingerprint run scan. No filter.
G2  G1 active + BinaryFuse16       As G1 plus a BinaryFuse16 negative filter per
    per frozen epoch               frozen epoch over deduplicated fp64 folds — same
                                   xorf crate/filter as the sealed segment filters
                                   (bn-1i7).
G3  Iceberg-style active + G2      active: 8-way fixed bins (SoA within bin, bin =
    frozen                         fp64 & mask, ~75% target load) + rare spill map
                                   for full bins; frozen identical to G2.
```

Epoch mechanics (G1-G3): epochs are `W/8` positions. An insert crossing into
the next epoch range freezes the active table (sort by (fp, pos), build
filter, push onto a deque). Whole epochs are dropped when
`max_position < w - W`; an epoch straddling the boundary is retained and its
stale entries are rejected by the exact position check at query time. There
are NO per-key deletes anywhere in G1-G3 — no tombstones, no order rows, no
read-before-write. This is structural, not behavioral: neither active-table
type exposes a per-row remove operation, and reclamation is
`VecDeque::pop_front` of whole epochs (see `deletes_issued()` — a constant 0
by construction, confirmed 0 after 2M steady-state inserts with 7 epoch
drops).

Fingerprints: keyed xxh3-128 (per-store seed; stands in for the BLAKE3/AES
derivation of design.md §13.2), width-maskable for forced-collision tests
(0 bits = every key fingerprints identically). The fp is only a router: all
equal-fingerprint candidates are retained and full-key-compared against the
arena.

A miss (the hot append-path case) never touches the arena/log at 128-bit
width unless a real fingerprint collision occurs — the negative path is pure
index: active probe + one BinaryFuse16 lookup per live frozen epoch.

## 2. Correctness — exact differential vs reference (HARD GATE): PASS

Reference model: `BTreeMap<(scope, full key) -> Vec<position>>` + `VecDeque`
window pruning (`src/reference.rs`). `cargo test --release`: **15/15 pass**.

| test | candidates | shape |
|---|---|---|
| randomized 100k ops | G0 (group=1), G1, G2, G3 | mixed new-key / re-check / retry-recommit ops, key lens 0-4 KiB, Global + 4 stream scopes, random position strides, span 4096 -> ~200 full window turnovers; every check compared against the reference |
| randomized 20k, grouped | G0 (group=4) | multi-key commit groups; `check` flushes the pending group |
| forced collisions, 0-bit fp | G1, G2, G3 | EVERY key fingerprints to 0; exactness must come entirely from arena full-key verification (no false negatives, no false positives) |
| forced collisions, 8-bit fp | G1, G2, G3 | 256 fingerprints across thousands of keys: heavy partial collisions in buckets, bins, runs, and filters |
| boundary exactness | all four | duplicate at `w = p+W-1` and exactly at `w = p+W`; gone at `w = p+W+1`; retry after expiry re-arms the window; durable end advances monotonically via filler traffic |
| epoch straddling | G1, G2 | boundary lands mid-epoch: the epoch is RETAINED (`oldest_live_epoch_range` asserted), its stale side answers None via the position check, its live side is still found; fully-expired epochs dropped wholesale with zero per-key deletes |
| rebuild from arena | G1, G2, G3, G0 | checkpoint-loss case (design §13.5): drop ALL index state, re-insert the arena suffix with `position >= w - W`, identical answers on every key ever seen |
| key lengths + scope separation | all four | lens {0, 1, 2, 8, 16, 255, 1024, 4096}; identical bytes invisible across Global/Stream scopes |
| adversarial same-prefix | all four | 4 KiB keys sharing a 4088-byte prefix, at 128-bit and forced 16-bit widths |

**Bug found by the harness — in the baseline, not the epoch design.** The
first G0 implementation reproduced the dormant MetaStore batch shape
faithfully and produced FALSE NEGATIVES under position-window eviction: when
a commit group re-inserts a key whose previous position has expired, the
eviction loop (which reads only committed rows) can see the key's stale order
row at the FIFO front and remove the primary row that the same batch is
overwriting — fjall batches apply last-write-wins per key, so the eviction's
remove clobbers the fresh insert. The dormant code avoids this class only via
its "capacity exceeds any single commit group" slack; position-window
eviction has no such slack, so G0 needed an explicit same-batch pk guard
(`group_seqs` in src/g0.rs). Recorded because it is precisely the mutable-KV
coupling (read-before-write + cross-table FIFO + in-batch ordering) that the
epoch design deletes wholesale.

## 3. Method

Environment (Measured): AMD Ryzen 9 3900X (24 threads, governor
`performance`), bench thread pinned to core 20 (second CCD, away from other
spikes' 1-5 pins); rustc 1.97.0 (2d8144b78 2026-07-07), `--release`,
`lto = "thin"`; Linux 7.0.12-arch1-1; fjall 3.1.6 (repo pin), hashbrown
0.15.5, xorf 0.12 (BinaryFuse16, as the sealed filters), xxhash-rust 0.8.16,
smallvec 1.15; disk for G0: nvme0n1p3 ext4 (~78% full), journal-buffered, no
fsync (I5) — same disk setup as the other spikes. TSC 0.25 ns/tick, empty
rdtsc->rdtscp overhead 80 ticks (subtracted).

Hygiene (per the state_kernel_dense_heads lesson): every per-op sample opens
with `rdtsc` and CLOSES with `rdtscp` (serializing-enough; plain rdtsc
flatters single-load paths with 0 ns p50s); quantiles come from full sorted
sample arrays (research/05 §15.2), never averages. 200k samples/scenario +
10k warmup; probe keys pre-generated into flat buffers so the measured op is
hash+probe+verify, not keygen. Every measured phase is preceded by a
competing-load guard (no rustc/cc/ld anywhere, no cargo/bench outside our own
ancestor chain, load1 < 6.0; sleep-and-retry) — during this run the sibling
spike's build storms (load 16-35) were correctly held out (669 guard holds
logged); the guard's load floor is 6.0 rather than ~1 because the host
carries a persistent ~4-5 ambient load from unrelated user processes on other
cores. Each (candidate, keylen, fill) config runs in a FRESH subprocess (heap
isolation; a wedged G0 cannot poison the matrix). Memory is measured in fresh
processes as RSS delta across index population, with the arena built FIRST so
the log-stand-in is excluded from both sides.

Workloads: W = 1M positions, epoch span 125k; key lens 16 B and 256 B; fills
25/75/100% of the window (250k/750k/1M live keys, one per position; at 100%
fill: 7 frozen epochs + 1 full active epoch — steady state). Scenarios:
active-only miss (`check_active_only`), whole-window miss (absent key, full
path), hit-in-active (uniform), hit-in-oldest-live-epoch (uniform), Zipf
(s = 1.1, recency-ranked) hits, insert throughput, epoch-close (freeze) cost,
resident + serialized footprint, and a steady-state phase (2W = 2M inserts
through the W window — the phase where G0 pays delete traffic and the epoch
designs drop whole epochs). G0 measured warm.

Caveats: (a) G0 insert numbers use 64-key commit groups; per-op batches would
be slower. (b) G0 "resident" includes fjall memtables/caches; "serialized" is
its on-disk directory after a 3 s settle. (c) Populate loops include keygen
(~20-40 ns/key, identical across candidates). (d) The arena is RAM-resident:
hit numbers include the full-key verify read from RAM; cold-arena I/O is
excluded by the gate's definition and never occurs on the miss path at
128-bit width. (e) One G0 `steady` subprocess wedged in fjall shutdown after
printing its results (futex wait, single thread remaining) and was killed
manually; results unaffected. (f) `insert` for G1-G3 includes inline freeze
cost at epoch boundaries (production would freeze off the append owner).

## 4. Results (Measured)

### 4.1 Whole-window miss — the hot append-path check (ns, p50 / p99)

| keylen | fill | G0 | G1 | G2 | G3 |
|---|---|---|---|---|---|
| 16 | 25% | 1090 / 1830 | 130 / 250 | **40 / 150** | 50 / 100 |
| 16 | 75% | 940 / 1610 | 700 / 1130 | **80 / 180** | 90 / 150 |
| 16 | 100% | 1560 / 2480 | 980 / 1800 | **100 / 230** | 110 / 170 |
| 256 | 25% | 790 / 1280 | 170 / 300 | **70 / 180** | 90 / 210 |
| 256 | 75% | 1600 / 2870 | 730 / 1140 | **110 / 400** | 130 / 240 |
| 256 | 100% | 1640 / 2480 | 1110 / 1750 | **130 / 280** | 150 / 320 |

### 4.2 Active-table-only miss (ns, p50 / p99)

G0 has no active/frozen split — its window_miss above IS the single fjall
point read.

| keylen | fill | G1 | G2 | G3 |
|---|---|---|---|---|
| 16 | 25/75/100% | 20 / 120 | 20-30 / 120-130 | 30 / 110 |
| 256 | 25/75/100% | 60 / 170-200 | 60 / 180-190 | 60 / 150-240 |

### 4.3 Hits (ns, p50 / p99)

hit in active epoch (uniform):

| keylen | fill | G0 | G1 | G2 | G3 |
|---|---|---|---|---|---|
| 16 | 100% | 2200 / 4230 | 80 / 410 | 90 / 420 | 70 / 300 |
| 256 | 100% | 1960 / 3700 | 250 / 630 | 250 / 600 | 240 / 610 |

hit in OLDEST live epoch (uniform — worst case):

| keylen | fill | G0 | G1 | G2 | G3 |
|---|---|---|---|---|---|
| 16 | 25% | 1890 / 3770 | 310 / 570 | 310 / 520 | 350 / 650 |
| 16 | 100% | 4090 / 7240 | 1350 / 2060 | 410 / 700 | 440 / 760 |
| 256 | 100% | 5710 / 11950 | 1570 / 2420 | 510 / 910 | 600 / 1020 |

Zipf(1.1) recency-weighted hits (realistic retry mix):

| keylen | fill | G0 | G1 | G2 | G3 |
|---|---|---|---|---|---|
| 16 | 100% | 1170 / 5080 | 60 / 1640 | 60 / 1430 | 60 / 1450 |
| 256 | 100% | 1220 / 104970 | 100 / 2250 | 100 / 1940 | 110 / 1940 |

(G0's 105 us p99 spike at 256 B / 100% is LSM background work interfering
with reads — exactly the coupling the epoch design removes.)

### 4.4 Insert throughput (populate, M ops/s; G0 batched 64/commit group)

| keylen | fill | G0 | G1 | G2 | G3 |
|---|---|---|---|---|---|
| 16 | 25% | 0.38 | 4.28 | 4.56 | 5.95 |
| 16 | 75% | 0.24 | 5.10 | 4.20 | 5.19 |
| 16 | 100% | 0.23 | 4.94 | 4.05 | 4.76 |
| 256 | 25% | 0.20 | 2.29 | 2.12 | 2.42 |
| 256 | 75% | 0.13 | 2.22 | 2.08 | 2.32 |
| 256 | 100% | 0.12 | 2.22 | 2.06 | 2.26 |

### 4.5 Steady state — 2W = 2M inserts through the W window (keylen 16)

| cand | M inserts/s | ns/insert | per-key deletes | write traffic |
|---|---|---|---|---|
| G0 | 0.117 | 8566 | 1,999,998 | 197 MB batch bytes; 318 MB on disk |
| G1 | 5.11 | 196 | **0** | 15 freezes (67 ns/key), 7 whole epochs dropped |
| G2 | 4.11 | 244 | **0** | 15 freezes (114 ns/key), 7 whole epochs dropped |
| G3 | 5.15 | 194 | **0** | 15 freezes (102 ns/key), 7 whole epochs dropped |

### 4.6 Epoch-close (freeze) cost, fill = 100%

| cand | freezes | mean ms / 125k-key epoch | max ms | ns/key |
|---|---|---|---|---|
| G1 | 7 | 9.2-9.3 | 11.2 | 73-75 |
| G2 | 7 | 15.0 | 17.9 | 120 |
| G3 | 7 | 13.8-14.1 | 16.9 | 111-112 |

Freeze = sort + (G2/G3) BinaryFuse16 build; it runs inline on the insert that
crosses the epoch boundary in this spike. Production should freeze off the
append owner — a 15 ms stall per 125k positions would otherwise be the
worst-case insert outlier.

### 4.7 Footprint (fresh process; arena/log excluded from both sides)

| keylen | fill | cand | RSS delta MiB | serialized MiB | (RSS+ser)/live key B |
|---|---|---|---|---|---|
| 16 | 100% | G0 | 311.1 | 118.6 (disk) | 450.6 |
| 16 | 100% | G1 | 43.7 | 30.5 | 77.8 |
| 16 | 100% | G2 | 45.3 | 32.5 | 81.6 |
| 16 | 100% | G3 | 42.1 | 32.5 | 78.2 |
| 256 | 100% | G0 | 334.0 | 976.3 (disk) | 1373.9 |
| 256 | 100% | G2 | 45.3 | 32.5 | 81.6 |

Epoch-candidate footprint is key-length-INDEPENDENT (fingerprint+pos+ptr =
32 B/entry + ~2.1 B/key filter); G0 stores full keys in both LSM tables.
Lower fills, all in bench_all.log, tell the same story (e.g. 16 B / 25%:
G0 584.5 B/key vs G1-G3 99-116 B/key).

## 5. Derived

| metric (worst across keylens, fill = 100%) | G1 | G2 | G3 |
|---|---|---|---|
| window-miss p50 vs G0 | 1.5x | **12.6x** | 10.9x |
| steady insert vs G0 | 43.8x | 35.2x | 44.1x |
| footprint vs G0 (16 B / 256 B keys) | 0.17x / 0.06x | 0.18x / 0.06x | 0.17x / 0.06x |
| serialized B/live key | 32.0 | 34.1 | 34.1 |

## 6. Gates

| gate | G1 | G2 | G3 |
|---|---|---|---|
| exact equality with reference everywhere, incl. forced collisions [hard] | **PASS** | **PASS** | **PASS** |
| active miss p50 <= 100 ns | **PASS** (60 ns worst) | **PASS** (60 ns) | **PASS** (60 ns) |
| whole-window miss <= 80 ns OR >= 5x G0 | **FAIL** (1110 ns; 1.5x) | **PASS** via 12.6x (<= 80 ns only at <= 25% fill; 100-130 ns at full window) | **PASS** via 10.9x |
| hit <= 300 ns warm | FAIL worst case (oldest-epoch uniform 1350-1570 ns) | **PARTIAL** — Zipf 60-100 ns and active-hit 90-250 ns PASS; oldest-epoch uniform 410-510 ns FAIL | PARTIAL (same shape, 440-600 ns) |
| zero per-key delete/tombstone writes (structural) | **PASS** (no remove API; 0 after 2M steady ops; G0: 2.0M) | **PASS** | **PASS** |
| resident + serialized <= 0.5x G0 | **PASS** (0.17x/0.06x) | **PASS** (0.18x/0.06x) | **PASS** (0.17x/0.06x) |

The 80 ns whole-window target is intentionally extreme (research/05 §11): G2
meets it at <= 25% fill and lands at 100-130 ns at a full window — while
write-amp (zero deletes; 34 B/key serialized vs G0's ~159 B/key steady disk
traffic), composed insert throughput (35x), and footprint (5.5-16x) all
clearly win, which is exactly the trade the plan says does not kill epoching.
The honest miss is worst-case single-epoch binary search on uniform
oldest-epoch hits (~410-510 ns); an Eytzinger layout or a per-epoch candidate
router (k-PHF/Ribbon — the optional G4 this spike did not need to build) is
the known fix if a real workload ever makes uniform-aged retries hot.

## 7. Design constraints the adopting bone must carry (review D3/D5)

**D3 — retention invariant (MUST-ADOPT).** The exact-verification path
(`canonical_key(candidate.ptr)`) and the rebuild-from-arena recovery path
both dereference canonical key bytes in the log. Retention must treat the
dedupe window as a floor:

```text
window_start = watermark.saturating_sub(W)

a segment containing canonical dedupe keys is deletable only when:
    segment.end_position <= window_start
```

Otherwise retention silently corrupts dedupe (dangling capsule ptrs on the
verify path) and breaks design §13.5's checkpoint-loss rebuild guarantee —
this spike's `rebuild_from_arena` test is exactly the recovery that invariant
protects. Silent-corruption class (S=5, D=5); add to the risk register per
review/11 D3.

**D5 — batch-level key is a NEW API decision.** v4 allows at most one
`DedupeKeyV1` per user capsule, covering the whole batch. Since production
dedupe was never wired (V1), this is not a narrowing of an existing contract —
it is a new API decision: batch/capsule-level idempotency. Per-event
idempotency requires one capsule per event or a future vector-valued control
record. Document it as such when the feature ships.

## 8. Commands

```bash
cd spikes/epoch_dedupe
CLANG_PATH=/usr/bin/clang cargo test --release        # differential suite (15 tests)
CLANG_PATH=/usr/bin/clang cargo build --release
./target/release/bench all                            # full matrix -> RESULT/MEM/STEADY lines
./target/release/bench lat  g2 16 100                 # one latency config
./target/release/bench mem  g2 16 100                 # fresh-process RSS/serialized
./target/release/bench steady g0 16                   # 2W inserts through a W window
./target/release/bench info                           # calibration + host line
```

Raw data for every table: `bench_all.log` (committed alongside this report).
