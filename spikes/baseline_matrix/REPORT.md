# baseline_matrix — Spike 0 (bn-u1k): matched baseline lock-in (baseline-gen2)

**Purpose** (research/05 §2 locked baselines, §15 measurement discipline;
review C4/S7). Re-lock the matched **bare-log vs composed-`LogEngine`**
throughput/latency matrix on the CURRENT engine, so every downstream Asterism
gate (Spike B's 85%-of-bare kill line, Spike J's composed decision, the
name-barrier baselines) measures against numbers taken *after* the three
merges Spike B's (gen1) workspace pre-dated:

1. **bn-34o** — registry name-persist coalesced into the committer group window
   (one meta `SyncAll` per group of new names, not one per name).
2. **bn-2cj** — the new-name durability barrier is gated on the engine
   `Durability` mode: `Process` buffers new names to the page cache with **no
   fsync**; `Os`/`Group` keep the `SyncAll`, coalesced.
3. **bn-2ib / Spike C** (`f0ab89e9`) — the all-history `Book` payload mirror is
   gone: block-native reads, header-only recovery, watermark-gated seal.

**This is baseline-gen2, post bn-34o/bn-2cj/bn-2ib, workspace base commit
`f0ab89e9`.** Cite it as the anchor for Spike-B/J-style gates going forward.

---

## Verdict up front

- **Process, composed/bare ratio is dominated by per-append fixed overhead and
  therefore rises monotonically with batch size** — from ~17% at batch 1 (4
  writers) to **85-95% at batch 1000**. The composed engine adds a roughly
  constant **~15 us/append** (4-writer) on top of bare's ~3 us; at batch 1 that
  is 5-6x the whole cost, at batch 1000 it is amortized away. **The per-cell
  ratio is the number to gate on, never a single headline** (review S7).
- **The bn-2cj/bn-34o name-barrier win is large and confirmed.** A 100%-new-
  stream append under `Process` is now **34.4 us/new-stream** (1 writer) vs the
  old **3.4344 ms/new-stream** (`spikes/seed_profile`) — a **~100x reduction**;
  the per-new-stream fsync that was 98.8% of that cost is simply not issued
  under `Process` anymore. Under `Group`, concurrent new streams coalesce
  (bn-34o): 4-writer new-name is **1.12 ms/new-stream** vs a naive ~2.5 ms if
  each rode its own barrier.
- **Group-mode parity holds at 1 writer and at large batch (90-103% of bare)**
  but **breaks to 49-57% at 4 writers with small per-writer batch counts** —
  the composed committer splits the group convoy (~2x the barrier count bare
  achieves at the same shape). This is the one anomaly; see §5. It reproduced
  across all 3 reps and is a genuine gen2 fact, but it warrants a dedicated
  high-concurrency (16-64 writer) confirmation before it is treated as a locked
  floor, because gen1's 4-writer Group cells reported parity.
- **Reopen on a 2M-event corpus: 1.67 s wall, 105 MB peak RSS** — matches
  Spike C's post-Book block-native number (104,804 KiB), i.e. the RSS win held.

---

## 1. Environment (Measured)

- **CPU:** AMD Ryzen 9 3900X (12C/24T), governor `performance`.
- **Kernel:** Linux 7.0.12-arch1-1. **rustc** 1.97.0 (2d8144b78 2026-07-07),
  `--release`, `lto = "thin"`, `debug = true`.
- **Disk:** `/dev/nvme0n1p3` **ext4**, `/home` 78% full (281 GB free).
- **Scratch:** `$HOME/.cache/mess-bench` (real ext4 via
  `mess_testkit::temp_dir_in`, self-sweeping) — **never tmpfs** (`fdatasync`
  there is a no-op and durable numbers would be a lie).
- **Workspace base commit:** `f0ab89e9` (post bn-2ib Spike C).
- **Ambient load:** this host carries a persistent ~4-5 background load from
  unrelated user processes (`spikes/epoch_dedupe/REPORT.md` §3); the quiet-guard
  floor is therefore **load1 < 6.0** (not ~1), plus **no rustc/cc/ld/cargo**
  outside our own ancestor chain. Every measured run held for quiet first; the
  Group cells logged dozens of guard holds at load 6.0-6.8 during this session.
- **Date:** 2026-07-11.

## 2. Method

Two engines, identical driver, matched and interleaved **A/B B/A A/B** (3 reps
each, research/05 §15.1), fresh store per run, settle before each run (400 ms
Process / 4 s Group):

- **bare** — the raw `mess-log` committer + appender: numeric stream ids, no
  registry, no validation, direct producer->committer->producer. The lower bound.
- **log** — the current composed `mess_store::LogEngine::append_batch`
  (registry interning, dedupe window, active index, block-native reads).

Both drivers: one Tokio task (log) / one appender (bare) per writer, one stream
per writer (0%-new matrix) with driver-tracked exact versions so no append ever
conflicts. **Every log run asserts `total_events() == events written`** (the
logical result check); every bare run reports the committer's own fsync count.
p50/p99 are over all per-writer samples after a 10% per-writer warm-up drop.
Median-of-3 **and** best-of-3 are reported separately (§15.2). Raw per-run rows:
`baseline_results.csv`; full logs: `matrix_run.log`, `newname_run.log`.

Reproduce:
```
cargo run --release -- matrix     # the 32-cell matched matrix
cargo run --release -- newname    # the 100%-new-stream name-barrier scenario
# reopen / RSS (2M corpus), existing Spike-C harness:
cargo run -p mess-store --release --example owb_bench -- seed <dir> 2000000 1000 10 64 8
cargo run -p mess-store --release --example owb_bench -- open <dir>   # x3, quiet-guarded
```

Batches-per-writer were tuned per (batch, mode) to keep each run a few hundred
ms (Process) or a bounded barrier count (Group): Process bpw
40000/12500/2500/250 for batch 1/10/100/1000; Group bpw 800/500/300/100.

---

## 3. The matched matrix — median ev/s (best-of-3 in the CSV)

`ratio` = composed `log` median / `bare` median. **Gate on the ratio in the
matching cell.** All rows Measured, 2026-07-11, one session.

### 3.1 Process (no barrier)

| payload | batch | writers | bare med ev/s | log med ev/s | ratio | ratio best | log p99 us |
|--------:|------:|--------:|--------------:|-------------:|------:|-----------:|-----------:|
| 24 B  | 1    | 1 | 86,086    | 30,333    | 35.2% | 33.6% | 81 |
| 24 B  | 1    | 4 | 397,648   | 67,280    | 16.9% | 16.9% | 121 |
| 24 B  | 10   | 1 | 883,570   | 297,870   | 33.7% | 34.6% | 60 |
| 24 B  | 10   | 4 | 3,008,728 | 668,240   | 22.2% | 22.1% | 106 |
| 24 B  | 100  | 1 | 3,844,026 | 1,630,243 | 42.4% | 44.0% | 85 |
| 24 B  | 100  | 4 | 8,900,379 | 1,995,791 | 22.4% | 24.1% | 475 |
| 24 B  | 1000 | 1 | 3,782,045 | 3,086,667 | 81.6% | 88.4% | 345 |
| 24 B  | 1000 | 4 | 4,021,471 | 3,802,737 | 94.6% | 93.5% | 1,528 |
| 250 B | 1    | 1 | 96,586    | 32,712    | 33.9% | 36.2% | 54 |
| 250 B | 1    | 4 | 361,327   | 65,362    | 18.1% | 17.8% | 121 |
| 250 B | 10   | 1 | 699,181   | 261,454   | 37.4% | 35.3% | 72 |
| 250 B | 10   | 4 | 1,934,753 | 580,569   | 30.0% | 30.8% | 187 |
| 250 B | 100  | 1 | 3,076,159 | 1,118,169 | 36.3% | 34.8% | 143 |
| 250 B | 100  | 4 | 4,376,313 | 1,680,185 | 38.4% | 38.6% | 658 |
| 250 B | 1000 | 1 | 2,800,872 | 2,136,894 | 76.3% | 76.7% | — |
| 250 B | 1000 | 4 | 3,266,049 | 2,803,607 | 85.8% | 86.4% | 2,647 |

**Process 4-writer row (the Spike-B gate row), by batch size:**

| batch | 24 B | 250 B |
|------:|-----:|------:|
| 1     | 16.9% | 18.1% |
| 10    | 22.2% | 30.0% |
| 100   | 22.4% | 38.4% |
| 1000  | 94.6% | 85.8% |

The composed engine only reaches the historical 85%-of-bare line at **batch
1000**. At batches 1-100 the ~15 us/append fixed cost (index + registry probe +
async handoff) dominates — bare's per-append is ~3 us (4w). This is the same
overhead Spike B diagnosed as thread-handoff/API-serialization, not validation.

### 3.2 Group (durable, `Durability::group_default()`)

| payload | batch | writers | bare med ev/s | log med ev/s | ratio | bare fsyncs | log fsyncs | log mean fsync us |
|--------:|------:|--------:|--------------:|-------------:|------:|------------:|-----------:|------------------:|
| 24 B  | 1    | 1 | 373     | 365     | 97.9% | 800 | 800 | 2,670 |
| 24 B  | 1    | 4 | 1,493   | 1,415   | 94.8% | 801 | 805 | 2,693 |
| 24 B  | 10   | 1 | 4,004   | 3,917   | 97.8% | 500 | 500 | 2,499 |
| 24 B  | 10   | 4 | 14,951  | 8,096   | 54.2% | 526 | 1,000 | 2,691 |
| 24 B  | 100  | 1 | 39,509  | 39,166  | 99.1% | 300 | 300 | 2,451 |
| 24 B  | 100  | 4 | 132,363 | 122,099 | 92.2% | 302 | 310 | 2,921 |
| 24 B  | 1000 | 1 | 354,305 | 346,873 | 97.9% | 100 | 100 | 2,520 |
| 24 B  | 1000 | 4 | 1,239,427 | 612,472 | 49.4% | 100 | 199 | 3,345 |
| 250 B | 1    | 1 | 404     | 381     | 94.3% | 800 | 800 | 2,491 |
| 250 B | 1    | 4 | 1,422   | 1,287   | 90.5% | 801 | 1,600* | 2,831 |
| 250 B | 10   | 1 | 3,817   | 3,904   | 102.3% | 500 | 500 | 2,471 |
| 250 B | 10   | 4 | 13,530  | 6,655   | 49.2% | 501 | 1,000 | 3,027 |
| 250 B | 100  | 1 | 38,745  | 37,116  | 95.8% | 300 | 300 | 2,582 |
| 250 B | 100  | 4 | 145,224 | 132,154 | 91.0% | ~301 | ~307 | 2,900 |
| 250 B | 1000 | 1 | 213,053 | 219,572 | 103.1% | 100 | 100 | 3,874 |
| 250 B | 1000 | 4 | 548,927 | 388,701 | 70.8% | 100 | 200 | 4,729 |

`*` one rep split badly (1,600 barriers); the median cell still reflects it.
Mean fsync latency sat at **2.4-4.7 ms** all run (settled device); the barrier
count — not throughput — is what the ratio tracks. **Where the composed engine
hits the same barrier count as bare (1w all batches; 4w at batch 100) it is at
parity. Where it splits the convoy (4w at batch 1/10/1000, small per-writer
counts) it pays ~2x barriers and drops to ~49-71%.** See §5.

---

## 4. Name-barrier baseline (100% new streams) — the bn-34o/bn-2cj re-lock

Batch of 1 event per brand-new interned stream (the seeder shape), 250 B
payload. **The per-new-stream latency is `us/app`** (one new stream per append).
Compared against `spikes/seed_profile`'s **3.4344 ms/new-stream** (measured
under `Process`, workspace `43e4aca0`, pre-bn-2cj).

| scenario | writers | bare us/new-stream | log us/new-stream | log fsyncs / new-streams | vs old 3.4344 ms |
|----------|--------:|-------------------:|------------------:|--------------------------|------------------|
| Process | 1 | 11.5 | 34.4 | 0 fsync | ~100x faster |
| Process | 4 | 2.9  | 18.6 | 0 fsync | ~185x faster |
| Group   | 1 | 2,517 | 3,406 | 1000 / 1000 (~1.0 barrier/name) | ~same order; no coalescing at 1 writer |
| Group   | 4 | 634  | 1,116 | ~1002 / 4000 (~0.25 barrier/name) | coalesced ~3x (bn-34o) |

Reading:
- **Process new-stream cost collapsed from 3.4344 ms to 34.4 us (1w) / 18.6 us
  (4w)** — bn-2cj removed the per-new-name `SyncAll` under `Process` (the log
  itself runs no barrier there, so a per-name fsync would be strictly stronger
  than the operator asked). The 98.8%-dominant fsync from `seed_profile` H1 is
  simply gone in `Process`. This is the single biggest gen2 shift and the one
  Spike J's seeder-shaped composed numbers must anchor to.
- **Group at 1 writer is still ~1 barrier/new-stream** (3.41 ms; sequential, so
  nothing piles up to share a fsync — matches `seed_profile`'s ~0.99
  barriers/new-name note). The composed engine adds ~0.9 ms over bare here (its
  meta `SyncAll` is small/fast at ~0.85 ms mean, but it is an *extra* serialized
  barrier on top of the event `fdatasync`).
- **Group at 4 writers coalesces (bn-34o):** ~1002 barriers for 4000 new streams
  -> 1.12 ms/new-stream, vs the ~2.5 ms it would cost if each rode its own
  barrier. Concurrency is what feeds the coalescing window.

## 5. The one anomaly — 4-writer Group convoy split

At **4 writers with small per-writer batch counts** (Group, batch 1/10/1000,
bpw <= 500), the composed engine records **~2x the barriers bare achieves at the
identical shape** (e.g. 24 B x10 x4w: log 956-1000 fsyncs vs bare 526; 250 B x
1000 x4w: log 200 vs bare 100), roughly halving durable throughput -> **49-71%**
of bare. At **1 writer** and at **batch 100** the barrier counts match and
parity holds (90-103%).

- Spike B's *gen1* report showed the composed `log` at **~95-107% of bare** in
  its 4-writer Group cells (bpw 500, same shape). Gen2 does not. Either one of
  the three merges perturbed the committer's group-window early-close at
  moderate concurrency, or gen1's short 4w Group runs happened to catch clean
  convoys. It reproduced across all 3 gen2 reps here, so it is not pure noise.
- Spike B *also* documented this exact failure mode (perf_group_commit H1's
  convoy split) and had to **rebuild** a D7 early-close in its flat owner to get
  ~500 barriers; the current committer evidently does not hold the window as
  tightly at 4 writers / low bpw. Spike B's own 64-writer `b1probe` saw parity,
  consistent with "more concurrency keeps the window full."
- **Recommendation for the lead:** treat the 4-writer Group small-batch cells as
  *provisional*. Before locking them as a durable floor, run a dedicated
  16/32/64-writer Group confirmation (the window-fill regime) and, if the split
  persists there too, file it as a committer coalescing regression against
  bn-34o/bn-2ib.

## 6. Reopen + peak RSS on a 2M-event corpus (post-C)

Corpus: 2,000,000 events, 1,000 streams, 10-event batches, ~64 B MessagePack
payloads, 8 MiB segments -> 24 segments (23 sealed + `.pcol`), `Process`. Harness
`crates/mess-store/examples/owb_bench` (public API; one phase per process so
`VmHWM` is that phase's peak RSS). Reopen quiet-guarded, best/median of 3.

| metric | gen2 (this run) | Spike C post-Book (open_without_book) | note |
|--------|----------------:|--------------------------------------:|------|
| reopen wall (best of 3, warm) | 1.667 s | 1.947 s | header-only recovery |
| reopen wall (median of 3) | 1.670 s | — | (1.667 / 1.706 / 1.670) |
| reopen peak RSS (VmHWM) | 105,176 KiB | 104,804 KiB | block-native RSS held |
| total_events on open (logical check) | 2,000,000 | 2,000,000 | PASS |
| sealed segments | 23 | 23 | — |
| seed peak RSS | 182,880 KiB | 218,428 KiB | — |
| seed allocs/event | 12.42 | 12.42 | identical |

The Book-removal RSS win from Spike C is intact: reopening a 2M corpus costs
~105 MB peak RSS (vs the ~420 MB Book baseline Spike C measured), and open
decodes zero old payload frames.

---

## 7. Deltas vs Spike B gen1 (matching Process 4-writer cells)

| cell (Process, 4w) | gen1 bare | gen1 log (ratio) | gen2 bare | gen2 log (ratio) |
|--------------------|----------:|-----------------:|----------:|-----------------:|
| 24 B x 10  | 2,959,586 | 520,056 (18%)  | 3,008,728 | 668,240 (22.2%) |
| 24 B x 100 | 9,521,221 | 1,956,646 (21%) | 8,900,379 | 1,995,791 (22.4%) |
| 250 B x 10 | 1,921,743 | 554,454 (29%)  | 1,934,753 | 580,569 (30.0%) |
| 250 B x 100| 4,638,734 | 1,480,965 (32%) | 4,376,313 | 1,680,185 (38.4%) |

Composed Process throughput is **modestly up** (18->22%, 32->38%) — consistent
with bn-2ib removing per-append Book bookkeeping. Bare is within +/-7% (same
host, same committer). **The only matched pair Spike B trusted (4x10 = 84% in
its envelope framing) was a Group/durable comparison; in gen2 the matched 4x10
Group cell dropped to 54% — that regression (§5) is the headline change a gate
author must not miss.**

---

## 8. How to use these numbers (anchor note)

- **Cite the cell, not a headline.** Every gate compares one (payload, batch,
  writers, durability) cell of §3/§4 against a candidate in the *same* cell.
  Per-batch-size ratios are mandatory (review S7): the Process composed/bare
  ratio ranges 17%->95% across batch size alone.
- **Spike-B-style ">=85% of bare, Process" gate:** the gen2 baseline meets it
  only at batch 1000 (85-95%); batches 1-100 sit at 17-42%. A candidate is
  judged against the bare figure in its cell here, at the batch size it targets.
- **Spike-J composed decision / seeder-shaped work:** anchor new-stream cost to
  §4 — **34 us/new-stream (Process)**, not the retired 3.43 ms. Sustained-append
  composed throughput anchors to §3.1.
- **Durable/Group gates:** use §3.2, but note §5 — 1-writer and batch-100 cells
  are trustworthy parity; the 4-writer small-batch cells are provisional pending
  a high-concurrency confirmation.
- **Reopen/RSS regression tests:** §6 — 2M corpus reopens in ~1.7 s at ~105 MB
  VmHWM; a future change that inflates either is a resource-gate failure.
- **Re-lock trigger:** any change touching the committer group window, the
  registry name-persist hook (bn-34o), the durability gating (bn-2cj), or the
  read/recovery path (bn-2ib) invalidates the matching cells here — re-run
  `matrix` + `newname` and bump the generation label.
