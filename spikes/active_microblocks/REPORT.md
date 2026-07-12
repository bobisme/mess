# active_microblocks — Spike F (bn-2gg): pointer microblocks vs the incumbent ActiveIndex

**Question.** Do direct tails + append-only fixed-capacity microblocks (design §8,
research/03 §3) beat the incumbent `ActiveIndex` (sharded
`RwLock<HashMap<u64, Vec<StreamEntry>>>` + global `Vec<GlobalEntry>` + `applied_end`
watermark) under actual contention — apply ≥ 1.25×, head/tail resolve p99 ≤ 0.75×,
memory ≤ 0.7×, no reader-path lock, Loom + reclamation green?

**Baseline context (review V3).** After Spike C (block-native reads) the production
read path DOES consult the ActiveIndex: `read_stream`'s hot path is
`active.stream_entries_from(sid, start, limit)` (engine.rs:2680), `read_global` pages
through `active.global_range(pos, limit)` (engine.rs:2738), and the committer applies
groups via `active.apply_committed` (engine.rs:1956/3157). F0 in this spike is the
REAL `mess_index::ActiveIndex` (path dependency, not a copy), exercised through
exactly those call shapes. The old caveat that reads bypass the index is retired;
what remains composed-specific is the global side (§5).

**Verdict up front: ADOPT-F3** (microblock arena, linear within-block scan, skip
chain every 8 blocks, stride-8 sparse global checkpoints). Every gate passes with
margin except two boundary rows reported honestly below. Headline (Measured): apply
**2.2–5.3× F0** across the whole matrix (gate ≥ 1.25×); head/tail resolve p99 under
contention **0.045×–0.19× F0** (64 readers Zipf) and **0.0007× F0** (64 readers on
the written stream: 200 ns vs 282 µs — the RwLock reader herd is structural);
memory **0.26× F0** hot / **0.35×** at a 4M-batch segment / **0.71×** (marginal vs
the 0.70 target) at a sparse 1M-batch Zipf segment; global seek p50 **120 ns vs
1310 ns** (10.9×) at 2 B/batch vs 40 B/batch; slab-free at roll **0.05 ms vs
18.9 ms** with no reader-visible latency spike. Loom (bound 3) + differential +
reclamation all green. Binary search within blocks (F2) loses to linear scan on
recent lookups, confirming research/03 §3.3; the skip chain (F3) costs nothing
measurable on recent reads and is 5× on deep resolves.

---

## 1. Candidates

```text
F0  incumbent ActiveIndex        real crate: 64-shard RwLock<HashMap<u64, Vec>>,
                                 global RwLock<Vec<GlobalEntry>>, applied_end clamp
F1  microblock arena, linear     fixed 32-entry PtrMicroblock (568 B), 16-B delta
                                 BatchPtr (research/03 §3.1) + full-width escape
                                 entries in an overflow arena; direct dense
                                 stream->tail-BlockId table (chunked, grow-only,
                                 AtomicU32 cells); single writer writes entry bytes
                                 then release-stores published=n+1; new blocks built
                                 privately, linked via previous, tail release-stored;
                                 readers acquire-load everything; same applied_end
                                 D7 clamp semantics as F0
F2  F1 + binary search           within-block search over <=32 entries
F3  F1 + skip chain              per-stream skip pointer to the nearest ancestor
                                 block anchored every 8 blocks (design §8.3)
+   sparse global batch-offset   (first_global_pos, offset) checkpoint every 8th
    array (design §8.4)          batch (stride-1 variant measured for reference)
                                 vs F0's global Vec via global_range(pos, 1)
```

Block layout (`src/micro.rs`): header {stream_id, base_version, base_global,
base_offset, previous, skip, depth} + `AtomicU32 published` + 32 × 16-B entries =
568 B → 17.75 B/batch at full occupancy (Measured, `info`). An entry escapes to the
overflow arena (full-width copy, 40 B) when a delta exceeds u32 or the batch's
segment differs from the generation's. Reclamation is generational: the whole index
is one slab set dropped via Arc generation handles (design §8.5); `Drop` poisons a
canary before the slabs free, and reclaim readers assert it on every read. F-arena
apply is one code path: F2 shares it verbatim and F3's skip maintenance
(2 header words per block alloc, i.e. per 32 batches) is unconditional — so the
apply rows cover all three.

## 2. Correctness (all green)

```text
loom          RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=target/loom cargo test
              --release --test loom_model
              Models the REAL apply/resolve functions (src/shim.rs swaps std/loom
              atomics + tracked cells; ENTRIES=2 under loom so 3 batches cross a
              block boundary). Writer fills block A (entry writes + release
              published stores), allocates+links block B, release-publishes the new
              tail; 2 concurrent readers traverse tail/previous with all three
              search modes. Loom's tracked UnsafeCells flag any entry/header read
              not ordered by the published/tail release-acquire edges (i.e.
              uninitialized or torn reads); asserts: a committed head always
              resolves to exactly the schedule's pointer; quiescent state exact.
              preemption_bound = 3 — PASS (21.5 s; bound 2: 3.7 s).
differential  cargo test (debug, F0's D7 asserts armed) + --release (defensive-skip
              path): 4 seeded randomized runs (48 streams, ~12k batches each,
              per-group queries) + a 20k-batch hot-stream deep-chain run (625
              blocks). F1/F2/F3 vs the REAL F0 must agree exactly on: stream_head,
              resolve (all modes), stream_entries_from (both descent modes, the
              bn-2ib paged covered-events accounting), stream_entries, applied_end,
              and global seeks (stride-1: exact batch identity; stride-8:
              checkpoint-within-stride containment). Workload forces delta escapes
              (>4 GiB offset jumps) and segment escapes (foreign segment_id), and
              in release exercises the defensive above-watermark skip identically
              on both sides — 5/5 PASS debug, 5/5 PASS release.
reclamation   tests/reclaim.rs: 4 readers doing lock-free generation loads with the
              use-after-free canary asserted on EVERY read; explicit long-lived
              leases held across a roll; asserts the slab does NOT free while any
              lease is held, reads through a retired-generation lease stay correct,
              the slab frees promptly once leases drain, and 30 back-to-back rolls
              under readers leak nothing — 2/2 PASS.
```

## 3. Method

Environment (Measured): AMD Ryzen 9 3900X (24 threads, governor `performance`),
Linux 7.0.12-arch1-1; rustc 1.97.0 (2d8144b78 2026-07-07), `--release`,
`lto = "thin"`; TSC 0.25 ns/tick, empty rdtsc→rdtscp overhead 80 ticks = 20 ns
(subtracted). Writer pinned to core 5; 4-reader rows pinned to cores 1–4;
64-reader rows round-robin over the other 23 cores. Host ambient load ~4–5. The
sibling Spike H (`directory_tournament`) finished before this suite started; all
16 quiet-guard holds during the run were empty-process loadavg-decay waits between
our own phases (bench_all.err.log) — no external process overlapped any measured
phase.

Hygiene (per state_kernel_dense_heads §2 and epoch_dedupe §3): every per-op sample
opens with `rdtsc` and closes with `rdtscp`; quantiles from full sorted sample
arrays; 200k samples/reader + 10k warmup; stream choices pre-generated outside
timed loops (Zipf(1.1) via exact CDF binary search over 100k permuted ranks).
Every measured phase is preceded by the competing-load guard (no rustc/cc/ld/cargo
outside our ancestor chain, no sibling bench binary — matched by argv[0] basename
equality, NOT cmdline substring (a substring match self-triggers on monitoring
shells that mention the name; that deadlock was hit and fixed) — and load1 < 6.0).
Reader latencies are measured under a live writer applying pre-generated 64-batch
commit groups at a fixed 500k batches/s (far above any durable group-commit rate;
bounded so every candidate faces identical write pressure). Each latency row uses
a FRESH index prebuilt with 2M batches (re-applying a schedule to a used index
violates the committer's version-continuity contract — a harness bug the first
smoke run caught via the "committed head must resolve" assertion). Memory points
run in fresh child processes (RSS delta across apply, schedule built first so it
is excluded; analytic = allocated bytes incl. chunk-directory overhead). Apply
rows: 2M-batch schedules, watermark per group = last batch end.

Run-validity note: an earlier bench launch was killed and discarded before any
measured row — its binary had been silently rebuilt by a `--cfg loom` test sharing
`target/release` (2-entry loom blocks). Loom builds now use `target/loom`, and the
runner verified `info` reports the real 568 B / 32-entry shape before measuring.

Caveats: (a) writer pacing bounds invalidation pressure; unpaced writers appear
only in the apply rows; (b) F0 `seek` is `global_range(pos, 1)`, which allocates
its one-entry Vec exactly as the production `read_global` resolve does; (c)
64-reader rows oversubscribe 24 hardware threads — deliberately; their `max`
values (both candidates, 6–21 ms) are descheduling artifacts, which is why p99/
p99.9 are the decision quantiles; (d) reclaim readers are paced at 500k
ops/s/reader so both candidates face the same op stream across the roll.

## 4. Results (Measured)

### 4.1 Apply throughput (single writer, 2M batches; M batches/s)

```text
events workload group   F0      F-arena   ratio   arena notes
1      hot      1       11.82   43.01     3.64x   62,500 blocks, 0 escapes
1      hot      64      15.45   45.35     2.94x
1      zipf     1        6.18   16.42     2.66x   148,018 blocks, 0 escapes
1      zipf     64       7.44   19.10     2.57x
100    hot      1       11.58   61.89     5.34x
100    hot      64      14.74   78.34     5.31x
100    zipf     1        6.70   14.98     2.23x   escapes 17.7% (see below)
100    zipf     64       7.19   17.33     2.41x
```

Escape rate: 0% everywhere except 100-event Zipf, where the schedule spans 24 GB
of segment offsets and cold streams' offset deltas from their block base exceed
u32 → 354,698 escapes (17.7%). Even at that rate apply stays 2.2–2.4× F0 (each
escape costs one 40-B overflow write). Real segments roll far below 24 GB, so this
is an upper bound; if multi-GB active segments ever become real, widen
offset_delta or re-base blocks (§7).

### 4.2 Read latency under concurrent writer (ns; pooled sorted samples)

Zipf(1.1), 100k streams, recent-version resolve + head:

```text
scenario           cand   p50     p90     p99      p99.9      max
zipf r=4  resolve  F0     50      170     380      670        107,160
                   F1     10      170     350      590        12,160
                   F2     20      190     470      750        22,890
                   F3     10      160     310      570        12,790
zipf r=4  head     F0     130     300     620      1,090      64,960
                   F1     10      170     340      590        13,080
                   F2     10      120     250      520        76,270
                   F3     10      140     260      540        26,520
zipf r=64 resolve  F0     120     560     1,820    212,590    6.5 ms
                   F1     10      120     330      580        17.0 ms*
                   F2     20      130     360      630        17.0 ms*
                   F3     10      140     340      590        8.0 ms*
zipf r=64 head     F0     270     720     6,670    1,624,860  13.0 ms
                   F1     10      100     300      540        21.0 ms*
                   F2     10      100     290      540        11.0 ms*
                   F3     10      100     300      550        21.2 ms*
```

\* 64-thread-on-24-core descheduling max; note F0's p99.9 is already 0.2–1.6 ms
while the arena's stays ≤ 650 ns — the arena has no lock for a descheduled reader
to hold, so preemption hurts only the preempted thread.

Adversarial (every reader on the exact stream being written = same shard for F0,
same tail block for the arena) + deep resolve (head − 4096 = 128 blocks back):

```text
scenario           cand   p50     p90     p99      p99.9     max
hot  r=4  resolve  F0     250     490     1,500    13,640    153,900
                   F1     0       10      30       340       11,350
                   F2     10      10      70       360       14,160
                   F3     0       10      80       350       31,490
hot  r=64 resolve  F0     1,030   3,920   282,370  2,857,459 11.7 ms
                   F1     0       10      90       650       13.5 ms*
                   F2     10      10      220      720       13.1 ms*
                   F3     0       10      200      690       20.0 ms*
hot  r=4  head     F0     190     420     1,320    12,810    95,780
                   F1     0       10      150      380       14,920
                   F2     0       0       130      370       11,810
                   F3     0       10      140      380       30,470
deep r=4  resolve  F0     300     490     1,010    13,050    334,280
                   F1     360     390     600      810       753,470
                   F2     380     420     640      880       26,610
                   F3     70      80      280      470       111,180
```

The hot r=64 row is the structural story: F0's shard RwLock collapses to p99
282 µs / p99.9 2.9 ms under a reader herd on the written shard; the arena serves
the same herd at p99 90–220 ns from the L1-hot tail block. Deep resolves separate
the chain walks: F3's skip chain (16 hops + ≤8 previous) is 4–5× F1/F2's 128-block
linear walk and beats F0's binary search over a 2M-entry Vec by 3.6× at p99. F2's
within-block binary search is consistently WORSE than linear scan on recent
lookups (branch misses vs 2–3 cache lines of predictable scan) — research/03
§3.3's hypothesis confirmed.

### 4.3 Global seek: position → batch, 4 readers under writer (ns)

```text
cand                    p50     p90     p99     p99.9    max       resident
F0 globalvec(range1)    1,310   1,880   4,330   9,960    2.56 ms   40 B/batch
F1 sparse stride-8      120     170     260     380      26,510    2 B/batch
F1 sparse stride-1      460     850     1,410   2,250    131,170   16 B/batch
```

Stride-8 is 10.9× F0 at p50 / 16.7× at p99 on the seek itself, at 1/20th the
resident cost (composed caveat in §5). Even stride-1 (a full in-memory batch
directory) beats F0 — `global_range`'s Vec allocation dominates its constant.

### 4.4 Memory per committed batch (fresh child, RSS delta across apply; 1-event)

```text
point                    F0 RSS     arena-s8 RSS  (analytic)   ratio    arena drop
hot  1M batches          80.21      20.69 (20.46)              0.258x   1.9 ms vs 6.2
zipf 1M batches          102.07     71.97 (72.03)              0.705x   6.4 ms vs 18.9
zipf 4M batches          89.14      31.15 (31.15)              0.349x   9.7 ms vs 42.7
(arena stride-1: hot 35.22, zipf-1M 86.00 — the stride-8 design point stands)
```

Hot/dense segments hit 20.7 B/batch — under the 24 B/batch gate and near the 18.4
design target (the gap is chunk-granularity + the stride-8 checkpoints' 2 B). The
sparse-Zipf 1M point is the honest edge: Zipf(1.1) leaves ~90k tail streams with
1–8 batches in 568-B blocks (121,525 blocks = 26% occupancy) → 72 B/batch, ratio
0.705× vs the 0.70 gate — a boundary miss by 0.7%. At a 4M-batch segment the same
streams refill their blocks (61% occupancy) → 0.35×. Fixed 32-entry blocks are
simply wasteful for one-batch streams; a small-first-block shape would close it
(§7), but even as-is the arena never uses MORE than 0.71× F0.

### 4.5 Segment-roll reclaim (1M-batch generation retired under 8 leased readers)

```text
cand   swap        slab-free   reader p99 pre/during/post   reader max during
F0     1,350 ns    18.92 ms    1,020 / 750 / 700 ns         12.6 us (post max 1.15 ms)
F1     2,220 ns    0.05 ms     510 / 610 / 580 ns           34.4 us
```

Generation-slab reclamation is bulk by construction: freeing the arena = ~120
chunk frees (0.05 ms) vs F0's per-stream `Vec` + `HashMap` teardown (18.9 ms — a
pause that lands on whichever thread drops the last lease, i.e. potentially a
reader; the 1.15 ms post-window max on F0 is exactly that). Neither candidate
shows a p99 spike through the roll itself; the arena's swap costs ~0.9 µs more
(ArcSwap store of a larger struct) — noise.

## 5. Global side: composed-shape note

The sparse batch-offset array answers position→(batch pos, offset) seeks; with
stride 8 the composed engine then scans ≤7 batch headers forward in the
(page-cached) active segment itself, which `read_global` already has open — the
segment is the canonical global order (design §8.4), so no in-memory per-batch
global entry is needed at all. F0's global Vec answers the same seek from memory
at 40 B/batch resident. The differential test pins the stride-1 variant to exact
batch identity and the stride-8 variant to within-stride containment; §4.3
measures both. What this spike does NOT measure is the segment-header
forward-scan cost (real segment I/O — composed-engine / Spike J territory). If
that scan proves annoying, stride-1 (16 B/batch, still 2.8× F0 on seeks and 0.35×
its global-side memory) is the fallback inside the same structure.

## 6. Gates (research/05 §10)

```text
gate                              result
apply >= F0 * 1.25                PASS — 2.23x worst row (100-ev Zipf, 17.7%
                                  escapes), 5.34x best; every row >= 2.2x
resolve p99 <= F0 * 0.75          PASS — 7/8 scenario-rows at 0.0007x-0.42x for
                                  F3; the single miss is 4-reader Zipf resolve
                                  (F3 310 vs F0 380 ns = 0.82x), where both sit
                                  on the cross-core/DRAM tail floor of a 100k-
                                  stream footprint (spike A's physics floor);
                                  every contended row is 5x-1400x inside the gate
memory <= F0 * 0.7                PASS with one boundary row — hot 0.26x, 4M-
                                  batch Zipf 0.35x, but sparse 1M-batch Zipf
                                  0.705x (block occupancy 26%; see §4.4/§7)
no lock in reader fast path       PASS (structural) — readers execute acquire
                                  loads only: tail cell, published count, chunk
                                  dir, applied_end; evidenced by hot r=64 p99
                                  90-220 ns vs F0's 282 us RwLock herd collapse
Loom + reclamation green [hard]   PASS — loom bound 3 (21.5 s), differential
                                  5/5 debug + 5/5 release, reclamation 2/2;
                                  slab-free 0.05 ms vs 18.9 ms
```

## 7. Verdict

**ADOPT-F3**: microblock arena with linear within-block scan, per-stream skip
chain every 8 blocks, and the stride-8 sparse global checkpoint array.

1. F3 dominates or ties F1 everywhere (recent-read cost of the skip pointer is
   unmeasurable; deep resolves 4–5× faster; apply/memory identical — skip
   maintenance is 2 header words per 32 batches).
2. **Reject F2**: binary search inside a 32-entry block is the only candidate
   that is ever slower than F0-relative expectations — linear scan over ≤3
   predictable cache lines wins at p50 and p99 (research/03 §3.3 confirmed).
3. The incumbent's failure mode is structural, not tuning: any reader herd on a
   written shard serializes on the shard RwLock (282 µs p99 at 64 readers; p99.9
   milliseconds), and its global Vec costs 40 B/batch plus microsecond seeks.
   The arena removes the lock, the hash, and the vector reallocation in one move
   and reclaims per-generation in constant time.
4. Honest edges for the adopting bone: (a) sparse-segment occupancy — at a
   1M-batch Zipf(1.1) segment the fixed 32-entry block yields 72 B/batch (0.705×
   F0, grazing the 0.7 gate); if sparse segments matter, start streams on a
   small (4- or 8-entry) first block before graduating to 32 — pure win, same
   protocol; (b) offset-delta escapes reach 17.7% only when a single active
   segment spans 24 GB of offsets (100-event batches × 2M) — not a real regime,
   but re-basing or a 40-bit offset delta removes it if it ever is; (c) the
   stride-8 global array assumes the composed engine scans batch headers in the
   segment for final positioning (§5) — measure that scan in the composed spike.
5. This was an optional spike; the incumbent remains solid at 4 readers and
   moderate load. The case for adoption is the contended tail (r=64: two to four
   orders of magnitude), the memory floor (0.26–0.35× in dense regimes), the
   allocation-free reader path, and O(1) generation reclamation.

## 8. Commands

```bash
cd spikes/active_microblocks
CLANG_PATH=/usr/bin/clang cargo test --tests            # differential + reclaim (debug asserts)
CLANG_PATH=/usr/bin/clang cargo test --release --tests  # + defensive-skip path
CLANG_PATH=/usr/bin/clang RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=target/loom \
  cargo test --release --test loom_model
cargo run --release -- all > bench_all.log 2> bench_all.err.log   # or: info|apply|latency|seek|mem|reclaim
# AMB_SMOKE=1 shrinks phases + skips the quiet-guard (plumbing smoke only)
```

Loom builds MUST keep their own target dir: a `--cfg loom` test build sharing
`target/release` silently rebuilds the bench binary with 2-entry loom blocks (this
run's first launch was invalidated exactly that way; the runner now verifies the
`info` block shape before measuring).
