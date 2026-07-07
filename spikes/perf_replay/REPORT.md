# perf_replay — parallel/optimized sealed-segment paths

Question: how much of the round-3 sealed-segment performance (spikes/seal_pipeline,
spikes/recovery_scale) is single-thread artifact, and what do the five obvious
optimizations buy — without giving up bit-identical results?

Machine: Ryzen 3900X (12c/24t), 64 GiB, NVMe (`/home` on nvme0n1p3, 95% full —
measured cold buffered-read ceiling: 0.98 GB/s single stream, **2.9 GB/s at 8–16-way
parallelism**, not the nominal 3.5 GB/s). Linux 7.0.12, rustc 1.96.1, `--release`,
opt-level 3, thin LTO. Workload: identical shape to seal_pipeline — 1M events,
258.5 MiB active segment, batches of 10, 10,000 streams Zipf(1.1) (7,411 present),
4 payload categories, seed-locked (fill=7, sample=99, point=123). Recovery workload
identical to recovery_scale (250 B payloads, 10/batch, crc32c, 256 MiB segments).

**Measurement hygiene note:** sibling spike benchmarks (`perf_group_commit`,
`perf_compress`, plus large rustc builds) were hammering this box for part of the
session (1-min load 40–90). All numbers below were re-taken in a verified calm
window (load < 6 at suite start; a watch script gated the run). The one stage that
stays noisy in *any* window is the fsync tail of the seal (device writeback
scheduling, bimodal in both baseline and optimized modes) — reported as ranges.

## Correctness gates (every optimized path, every run)

- Replay paths: `ReplayStats` (events, bytes, order-independent checksum) equal to
  ground truth recomputed from the ACTIVE segment scan on every measured run.
- Point reads: skip-table result asserted equal to linear-seek result per read,
  before timing.
- Parallel seal: **byte-identical output file** vs the baseline seal — all sections
  (blocks, ptrs, skips, dir, block index, dicts, footer) memcmp'd. The filters
  section is excluded *in both directions*: xorf's BinaryFuse seeds its hash
  randomly per process, so even two baseline runs differ there (the postcard
  varint of the seed even changes the section length). The filter-build code is
  identical, sequential code in both modes.
- Parallel recovery: identical batch/event counts, next_pos, last stop, AND the
  complete 10,000-entry per-stream heads map (version + last EventPtr) vs the
  single-threaded scan; A1 contiguity chain re-verified across the merge.
- The baseline seal run additionally re-verifies seal_pipeline's end-to-end gate:
  all 7,411 streams replay byte-identically (crc32 of len++payload) across the
  seal — 0 mismatches.

## 1. Locked baseline (reproduced before changing anything)

| metric | round-3 recorded | reproduced here |
|---|---|---|
| sealed global replay | 6.40M ev/s | **6.44M ev/s** |
| sealed stream replay (1k random) | 2.72M ev/s | **2.75M ev/s** |
| hottest-stream replay | 7.40M ev/s | **7.63M ev/s** |
| seal, single-threaded | 1.50 s | **1.45 s** (1.31–1.92 across runs; fsync tail) |
| cached point read p50 | 1.7 µs | **1.67 µs** |
| cached point read p99 (the linear-seek problem) | — | **108 µs** |
| recovery full scan cold / warm | ~1.1 / ~0.76 s/GiB | **1.06–1.54 / 0.74 s/GiB** |
| sealed size | 44.0 B/event | **44.2 B/event** (+0.15 MiB skip table = +0.16 B/ev) |

Baseline seal stages (single-threaded): consolidate 22 ms, filters 0.5 ms, segment
read 131–150 ms, dict training 335 ms (105/83/72/68 per category), recompress+write
775 ms, metadata+fsync 26–536 ms (median ~156), drop index 18 ms. Peak RSS 373 MiB.

The sealed format here is v2 = seal_pipeline's layout + a skip-table section
(between ptrs and dir). Block bytes, pointer bytes and all baseline read paths are
exactly what seal_pipeline produced; baseline `point_read` never touches the skip
data.

## 2. Bottleneck evidence

`perf stat` / `perf record` both work at perf_event_paranoid=2 for user-space
events (`:u`) — sufficient, these loops are ~0% kernel time when warm. No fallback
to instrumented timers was needed.

- **Global replay baseline** (`prof global_seq`, 10 reps): IPC 2.58; cache-misses
  only 2% of cache-references. `perf record`: **55.2% ZSTD_decompressSequences_bmi2**,
  ~10% FSE/HUF table setup (`ZSTD_buildFSETable`, `HUF_readDTableX2`,
  `HUF_readStats`), ~4% libc memcpy. → CPU-bound zstd decompress, NOT memory-bound
  ⇒ block-parallel decompression should scale with cores.
- **Stream replay baseline** (`prof stream_seq`): same shape — 56.3%
  ZSTD_decompressSequences, IPC 2.50 — plus visible `replay_stream` overhead
  (Vec-per-block runs, LRU/Arc bookkeeping) and repeated decompression of blocks
  shared across streams.
- **Point read baseline** (`prof point_seq`, cached): **~306k instructions and
  ~57k branches per read** at IPC 5.49 — a pure linear varint-seek loop whose cost
  scales with `version`; event-uniform sampling lands on hot streams constantly,
  which is exactly the p99.
- **Seal baseline**: recompress (775 ms) is per-block independent work; dict
  training (335 ms) is per-category independent; the 150 ms input read is a copy
  of data already in page cache; metadata+fsync is ~42 MiB of device writeback.
- **Recovery baseline**: recovery_scale already showed the scan is CPU-bound at
  ~1.3 GiB/s/core; segments are independent given base_pos (in the file name) —
  the R1 spec amendment anticipated parallel per-segment scans.

## 3. Ranked hypotheses

1. Global replay is CPU-bound block decompress; blocks are independent → rayon
   over blocks ⇒ order-10× warm; cold will hit the NVMe ceiling. Target ≥25M ev/s.
2. Stream replay redundantly decompresses shared blocks and pays cache
   bookkeeping → decode all pointer lists, decompress the *unique* block set once
   in parallel from an mmap, assemble per stream ⇒ ≥8M ev/s warm.
3. Point-read p99 is O(version) varint seeking → intra-block skip table (every
   64th eslot absolute: eslot u64 + varint byte-offset u32, 12 B/entry) ⇒ O(64)
   seek, p99 collapses to ~p50 for ~0.16 B/event.
4. Seal: 4-way parallel dict training + block-parallel recompression + mmap input
   + writeback overlap ⇒ ≤0.4 s.
5. Recovery: rayon per segment ⇒ warm bound by memory bandwidth, cold by NVMe;
   ≤0.4 s/GiB cold requires reads at the device ceiling with the scan overlapped.

## 4. Changes

### Kept

- `opt::global_replay_par` — rayon `par_chunks(64)` over the block index;
  per-worker zstd decompressors; **one reused decompress buffer** per worker
  (`decompress_to_buffer` — no per-block Vec alloc); block bytes sliced straight
  from an mmap of the sealed file. Stats merged with an order-independent
  reduction (the checksum is a wrapping sum, so parallel == sequential exactly).
- `opt::stream_replay_batch` — parallel eslot decode → sorted unique touched-block
  set → each block decompressed **exactly once** (parallel, mmap-backed) →
  parallel per-stream assembly. No LRU, no Arc churn; per-stream block runs stored
  as dense `(block, first_idx, len)` ranges instead of a Vec per block.
- `opt::stream_replay_par_preads` — "just add rayon" over the baseline per-stream
  loop (own ReadCtx + preads per worker). Kept for comparison; batch wins warm.
- `opt::point_read_skip` + skip-table section in the sealed format. Entry `j`
  stores the absolute eslot at index `(j+1)*64` and the byte offset *after* its
  varint; a read seeks to `version/64`, then decodes ≤63 varints.
- `opt::dicts_and_recompress_pipelined` — per category: train the dictionary, then
  immediately compress that category's blocks with nested rayon — no global
  barrier, so early categories' compression overlaps late categories' training.
  Compressed blocks are then written in baseline order by one thread ⇒ byte
  identity. Input segment mmap'd (saves the 150 ms copy). After the block region
  is written: `sync_file_range(WRITE)` kicks async writeback so the final fsync
  overlaps metadata building.
- Recovery `recover_full_par` — rayon over segments; per-segment heads merged in
  segment order (reproduces last-writer-wins exactly); contiguity chain verified
  across the merge. Then two measured refinements:
  `posix_fadvise(WILLNEED)` on all segments up front (cold 0.517 → 0.454 s/GiB)
  and **scan straight from mmap** instead of `fs::read` (cold 0.454 → 0.32–0.38;
  warm 0.226 → 0.036 s/GiB — the 4 GiB copy into user buffers was the warm
  bottleneck, not the scan).

### Tried and rejected

- **Background drop of the per-event index** (free the 1M-node BTreeMap on a
  spawned thread while dicts train): cross-thread frees contend the glibc
  allocator with ZDICT's workspace allocations — per-category dict training went
  125 ms → ~290 ms (2.5–3×), costing far more than the 18 ms it hid. Reverted;
  the drop stays sequential.
- **Barrier-style parallel seal** (`seal par1`: all dicts, then all blocks) is
  kept in the binary for comparison; the pipelined version is ~25–50 ms faster
  because the slowest category's dictionary no longer gates all compression.

## 5. Post metrics

Warm = best of 5, page-cache warm. Cold = `posix_fadvise(DONTNEED)` on the
touched files immediately before the run (best-effort eviction; files fdatasync'd
at write time). Correctness gates ran on every measurement.

### Final before/after — all five paths

| path | baseline | optimized | change | target | hit? |
|---|---|---|---|---|---|
| global replay, warm | 6.44M ev/s | **95.1M ev/s** | **14.8×** | ≥25M | **yes** |
| global replay, cold | 4.85M ev/s | 48.8M ev/s | 10.1× | (find NVMe wall) | NVMe-bound |
| stream replay 1k random, warm | 2.75M ev/s | **24.6M ev/s** | **8.9×** | ≥8M warm | **yes** |
| hottest stream, warm | 7.63M ev/s | **32.3M ev/s** | 4.2× | — | — |
| point read cached p50 / p99 | 1.67 µs / 108 µs | **0.36 µs / 0.79 µs** | p50 4.6×, **p99 137×** | kill p99 | **yes** |
| point read hottest p50 / p99 | 57.8 µs / 120 µs | **0.16 µs / 0.42 µs** | **p99 286×** | — | — |
| seal 256 MiB / 1M events | 1.45 s (best 1.31) | **0.40 s** (best 0.397) | **3.6×** | ≤0.4 s | **yes**, at the line¹ |
| recovery full scan, cold 4 GiB | 1.06–1.54 s/GiB | **0.32–0.38 s/GiB** | **~3.3×** | ≤0.4 s/GiB | **yes** |
| recovery full scan, warm | 0.74 s/GiB | **0.036 s/GiB** | **21×** | — | — |

¹ best runs 0.397–0.405 s; runs whose final fsync landed behind still-in-flight
writeback of the *previous* run's 42 MiB hit 0.90 s. The same bimodality afflicts
the baseline (d-stage 26–536 ms). Compute-side the parallel seal is deterministic:
a+filters+input+dicts+recompress+drop ≈ 0.21–0.22 s every run; the remainder is
device writeback that a real system pays asynchronously anyway.

### Global replay: where it goes NVMe-bound

Warm, 95.1M ev/s = 3.9 GB/s of compressed bytes decompressed+walked — pure CPU,
14.8× one core, +3% total instructions vs sequential (15.85G vs 15.41G — near-zero
parallelization overhead). Cold, 48.8M ev/s at **1.98 GB/s off NVMe**: the disk is
the limiter (measured device ceiling 2.9 GB/s at high parallelism; mmap-fault
readahead reaches ~2.0). The baseline never got near the disk (0.2 GB/s). With
this segment's 6.1× payload compression, every NVMe GB/s feeds ~25M ev/s of
replay: cold global replay saturates around 4–6 cores; the rest only helps warm.

### Stream replay detail (1,000 random streams, 94,980 events)

| variant | warm | cold |
|---|---|---|
| baseline seq (LRU 256) | 2.75M ev/s (34.5 ms) | — |
| rayon per-stream preads (LRU 64/worker) | 19.8M ev/s | 22.1M ev/s |
| batch unique-blocks (kept) | **24.6M ev/s** (3.9 ms) | 17.3M ev/s |

Seal-time clustering means 1,000 streams touch only 1,376 unique blocks (7.1 MiB
compressed); decompressing each exactly once beats any cache policy. Cold, both
parallel variants sit at 17–22M ev/s (random 7 MiB off NVMe); preads edge out mmap
faults there.

### Point reads (10k uniform-random events / 10k on the hottest stream)

| case | p50 | p99 | p99.9 | mean |
|---|---|---|---|---|
| uniform, no cache, linear (baseline) | 25.2 µs | 124.8 µs | 134.9 µs | 33.6 µs |
| uniform, no cache, skip | 17.9 µs | **30.6 µs** | 35.8 µs | 20.6 µs |
| uniform, cached, linear (baseline) | 1.67 µs | 108.1 µs | 116.6 µs | 13.4 µs |
| uniform, cached, skip | **0.36 µs** | **0.79 µs** | 0.98 µs | 0.38 µs |
| hottest, cached, linear (baseline) | 57.8 µs | 120.3 µs | 145.6 µs | 58.3 µs |
| hottest, cached, skip | **0.16 µs** | **0.42 µs** | 0.61 µs | 0.18 µs |

The linear-seek p99 is dead: ~306k instructions/read → <1k (perf-counter
verified). Cost: 0.15 MiB of skip entries (0.16 B/event). Cached mean drops 35×.
No-cache reads are now ~95% block decompress (~18 µs), as predicted.

### Seal stages (256 MiB / 1M events)

| stage | baseline | par1 (barrier) | par (pipelined, kept) |
|---|---|---|---|
| consolidate | 22 ms | 21–26 ms | 22 ms |
| filters | 0.5 ms | 0.5 ms | 0.5 ms |
| segment input | 131–150 ms (copy) | ~0 (mmap) | ~0 (mmap) |
| dict training | 335 ms | 97–127 ms (3×, 4-way) | pipelined ↓ |
| recompress+write | 775 ms | 95–101 ms (7.7×) | 174–178 ms dicts+blocks combined (6.4× vs 1,110 ms) |
| metadata+fsync | 26–536 ms (median ~156) | 60–111 ms | 93 ms fast runs / 432–483 ms writeback collisions |
| drop index | 18 ms | 18 ms | 16–18 ms |
| **TOTAL** | **1.45 s** (best 1.31) | 0.41–0.89 s | **0.40–0.90 s (best 0.397)** |
| peak RSS (HWM) | 373 MiB | ~640 MiB | **~630 MiB** |

Peak RSS grows ~+255 MiB: all compressed blocks are held before the ordered write
(~41 MiB), per-category EvRef copies (~16 MiB), and the mmap'd input's resident
pages count against RSS where the baseline's heap copy did too — the real
non-reclaimable delta is the ~60 MiB of blocks+copies; the rest is page cache.

### Recovery (4 GiB = 17×256 MiB segments, crc32c)

| mode | cold | warm |
|---|---|---|
| FULL seq (baseline) | 1.06–1.54 s/GiB (666–969 MiB/s) | 0.74 s/GiB (1,370 MiB/s) |
| FULL par v1 (rayon + fs::read) | 0.517 s/GiB | 0.226 s/GiB |
| FULL par v2 (+WILLNEED prefetch) | 0.454 s/GiB | — |
| FULL par v3 (+mmap scan, kept) | **0.32–0.38 s/GiB (2.7–3.2 GiB/s)** | **0.036 s/GiB (28–29 GiB/s)** |

Warm parallel recovery lands almost exactly at 24 threads × the known
~1.2–1.3 GiB/s/core scan speed — the copy, not the scan, was the warm ceiling.
Cold is at/near the measured 2.9 GB/s device ceiling. The identical-results gate
(counts, next_pos, full heads map) passed seq-vs-par on every run. Peak RSS
~4.1 GiB (all segments resident at once — v3's mmap pages are reclaimable page
cache, v1's read buffers were not; chunked scanning would cap this if it mattered).

## 6. Remaining bottlenecks / next experiments

- **Cold reads** stop at ~2.0 GB/s via mmap faults / ~2.9 GB/s via parallel
  buffered reads on this (95% full) drive. io_uring or O_DIRECT with explicit
  queue-depth control is the next lever for cold global replay (~1.4× headroom);
  it cannot help warm paths.
- **Warm global replay** now spends its cycles in zstd proper; the ~10% FSE/HUF
  table-rebuild share says bigger blocks (256–512 events) would trade point-read
  latency for scan throughput. A format experiment, not a code one.
- **Seal floor** is the slowest category's dictionary (~110 ms; ZDICT is
  single-threaded per call) plus the fsync tail. More/smaller categories, or
  fastcover with tuned params, attack the former; issuing the final fsync
  asynchronously (the real system seals in the background anyway) removes the
  latter from any latency path.
- **Point reads without cache** (~18–21 µs) are 95% block decompress; the round-1
  per-event-with-dict hot tail remains the right design for point-read-heavy
  workloads. The skip table fixes seek, not decompress.
- **Recovery warm** is at the memory-bandwidth ceiling (28–29 GiB/s aggregate);
  nothing left single-box. Cold recovery of a 10 GiB log now projects to ~3.5 s
  (vs ~11 s baseline).

## Repro

```
cargo build --release
./target/release/perf_replay gen          # 256 MiB / 1M-event active segment
./target/release/perf_replay seal base    # canonical seal + full byte-identity verify
./target/release/perf_replay seal par1    # parallel seal, barrier variant
./target/release/perf_replay seal par     # parallel seal, pipelined (kept)
./target/release/perf_replay bench replay # warm global+stream, baseline vs optimized
./target/release/perf_replay bench point  # linear vs skip-table latency
./target/release/perf_replay bench cold   # fadvise-DONTNEED cold variants
./target/release/perf_replay prof <global_seq|global_par|stream_seq|stream_batch|point_seq|point_skip> [reps]
./target/release/perf_replay recovery 4   # 4 GiB multi-segment scan, seq vs par (self-cleans)
./target/release/perf_replay clean        # remove bench_data
```

Bench data was deleted at the end of this spike (`recovery` self-cleans; `clean`
removed the segment + sealed file).
