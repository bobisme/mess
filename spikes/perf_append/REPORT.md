# perf_append — profile-guided optimization of the composed append path

**Question**: can the composed Engine A (custom segment log + pointer index, vertical_slice's
"Meridian slice") beat RocksDB at buffered appends — its one decisive remaining win?
**Target**: >= 532k ev/s (Engine B's recorded number); stretch goal 1M ev/s.

**Verdict: yes, 3.1x over the target.** Final composed engine (`actor-pipe`): **1.71M ev/s at
4 writers** (workload of record), 998k at 1 writer, 2.05M at 12 — vs RocksDB reproduced at
557k/342k/724k on the same machine, same day, same binary. Both goals cleared at every writer
count. The change that mattered most: **taking the index write off the append critical path
entirely** — in-memory active index + a background sealer thread that persists index entries at
segment seal (the D5 endgame + seal_pipeline design). Everything else (F1 batching, actor
sequencing, group encode) was worth 2-3x combined; this one flip was worth another ~4.7x.

Workload (identical to vertical_slice, same seed/rng/payload generator): 1M events, ~257 B
JSON payloads, 10k streams Zipf(1.1), batches of 10, BUFFERED durability. Machine: Ryzen 9
3900X (12C/24T), NVMe ext4, rustc 1.96.1, release + thin LTO. The host ran unrelated
concurrent work during parts of the sweep, so every config was run 3x and the best wall time
is reported (per-rep numbers were printed alongside; spread was typically <15%, occasionally
2x under interference bursts).

Run: `cargo run --release` (full progression + gates), `cargo run --release -- bench <variant>
<writers> [batch] [events]` (single run, for perf wrapping), `-- ceiling` (microbenches).

## 1. Baseline (locked)

| config | measured here | recorded in round 2 |
|---|---|---|
| composed Engine A, tokio driver, 4 writers | **185k ev/s** | 175k ev/s |
| composed Engine A, thread driver, 4 writers | 183k ev/s | — |
| log only (index writes off), 4 writers | 689k ev/s | "~224k" (F1 note) |
| RocksDB (Engine B shape), 4 writers | **557k ev/s** | 532k ev/s |

Baseline and target reproduce within 5-6%. One correction to the round-2 note: the bare log
is *not* a ~224k device — with the index off it does ~689k ev/s in this harness even under the
global mutex. The index tax was even larger than F1 estimated: **the composed baseline spends
roughly 3/4 of its capacity on per-event fjall inserts.**

Baseline latency at 4 writers: p50 159us, p99 386us per 10-event batch.

## 2. Bottleneck evidence (baseline, 4 writers, 1M events)

`perf stat` (whole process; subtract the deterministic workload-gen run: 0.95s, 5.9G instr):

```text
baseline:  wall 6.73s   user 3.37s   sys 7.20s     <- SYS 2.1x USER
           cycles:u 12.6G   instr:u 15.0G (IPC 1.19)
           cache-misses 156M   branch-misses 39M
```

`strace -c` (counts are exact; timings inflated by strace):

```text
write:  1,200,668 calls   = 12 per batch: 10 fjall journal writes (ptr inserts)
                            + 1 head insert + 1 log write   <- F1, measured
futex:  1,838,032 calls   = global engine mutex + fjall journal mutex churn
```

`perf record` top user-space symbols (workload gen excluded by symbol):

```text
7.9%  std mutex lock_contended            <- F8: global position/log mutex
8.5%  crossbeam_skiplist search/insert    <- fjall memtable, per-event inserts
~4%   fjall keyspace insert / journal write_raw / table writer / compaction
0.6%  crc32fast                           <- CRC is a non-issue
```

The append path itself (encode + CRC + buffered write) is cheap; the engine was spending its
time putting 11 tiny keys per batch through an LSM journal, one syscall each, under contention.

## 3. Ranked hypotheses (post-profiling order)

1. **F1 — per-event fjall index writes dominate.** Fix: one fjall WriteBatch per commit;
   endgame: in-memory active index, fjall written only at seal. (Confirmed, biggest single
   family of wins.)
2. **Seal stall — found by measurement at 1M scale.** At ~260 MB the log rolls once; the roll
   inline-pays fdatasync(256 MiB) + a ~0.9M-entry index dump + durable persist, together
   ~1.4s stop-the-world. Every in-mem-index variant was capped at 310-500k by this single
   event. Fix: background sealer thread (the seal_pipeline design). (Confirmed, largest
   lever overall.)
3. **F8 — global mutex sequencing.** (a) dedicated appender thread with MPSC handoff
   (mess_db actor shape), (b) position-range reservation + reorder-before-write with
   parallel encode. (Both implemented; matters only after 1+2 are fixed.)
4. **Encode cost / syscalls per batch.** Group-encode all drained requests into one reusable
   contiguous buffer, single write() per drain, zero steady-state allocation. (Real but
   secondary at batch=10; large at batch>=100.)
5. **CRC.** crc32fast 14.0 GB/s vs hardware crc32c 19.8 GB/s — both <2% of the final budget;
   keep the existing crc32 format (A12 forbids dropping it, and there is no need).

## 4. Changes and progression (composed throughput, ev/s, best of 3)

Every variant passed all three correctness gates at every writer count (section 6).

| variant | change on top of previous | 1 writer | 4 writers | 12 writers |
|---|---|---|---|---|
| `baseline` | vertical_slice Engine A as-is | 214k | 183k | 203k |
| `mutex-batched` | F1: one fjall WriteBatch per commit | 396k | 475k | 554k |
| `mutex-inmem` | index in memory, dump at seal (inline) | 498k | 365k | 472k |
| `actor-naive` | F8a: appender thread, per-batch encode+write, fjall batch per drain | 376k | 374k | 581k |
| `actor-group` | + group encode, 1 write()/drain | 310k | 362k | 407k |
| `actor-inmem` | + in-mem index (seal inline) | 315k | 363k | 409k |
| **`actor-pipe`** | **+ seal on background sealer thread** | **998k** | **1,710k** | **2,045k** |
| `reserve` | F8b: pos-range reservation, parallel encode+CRC, reorder-before-write, in-mem index (seal inline) | 318k | 687k | 628k |
| `reserve-pipe` | F8b + background sealer | 815k | 1,592k | 1,668k |
| `rocks` (target) | Engine B, same machine/day | 342k | 557k | 724k |

Reading the table:

- **F1 alone (mutex-batched) is a 2.6x win** (183k -> 475k at 4w) — the minimal spec-level fix
  already recovers most of the gap to RocksDB. It also collapses sys time from 7.20s to 1.97s
  (perf stat below).
- **Every in-mem-index variant without the async sealer is a trap**: the ~1.4s inline seal
  stall at the segment roll caps 1M-event runs at 310-500k regardless of sequencing. This is
  invisible at <256 MiB scale (the 100k-event smoke runs hit 1.3-2.0M ev/s — no roll, no
  stall) and is why hypothesis 2 only emerged from full-scale measurement.
- **The sealer flip is worth 4.7x** (actor-inmem 363k -> actor-pipe 1,710k at 4w). p99 batch
  latency drops to 32us because no append ever waits on fjall or fdatasync.
- **F8a vs F8b**: at batch=10 the actor wins slightly (1.71M vs 1.59M at 4w) — encode+CRC of a
  2.7 KB batch (~400ns) is too cheap to be worth the reservation/reorder machinery. F8b wins
  when encode dominates: at batch=1000 reserve-pipe hits 9.3M ev/s vs actor-pipe's 5.0M,
  because four writers CRC in parallel while the sequencer only writes. Verdict for the
  design: **the actor (mess_db shape) is the right default; keep F8b in the back pocket for
  huge batches.**
- Writer scaling of the final variant: 998k / 1,710k / 2,045k at 1/4/12 — beyond ~4 writers
  the appender thread itself is the serial section (see remaining bottlenecks).

Supporting perf stat for the top two hypotheses:

```text
                     wall     user    sys     instr:u   cache-misses
baseline             6.73s    3.37s   7.20s   15.0G     156M    <- H1: syscall/LSM dominated
mutex-batched (F1)   4.11s    2.46s   1.97s   14.5G      88M    <- sys collapses 3.7x
actor-pipe (final)   (load 0.58s) 2.14s 1.97s 13.9G      41M    <- H2: seal off critical path
  (bench-process totals include the constant 0.95s/5.9G workload generation)
```

## 5. Post metrics (final variant vs baseline, 4 writers)

```text
                        baseline            actor-pipe          change
throughput              183k ev/s           1,710k ev/s         9.3x
batch latency p50/p99   159us / 386us       19.2us / 31.9us     8x / 12x
write() syscalls        1,200,668           45,336              26x fewer (0.045/event)
futex calls             1,838,032           207,059             9x fewer
allocations/event       1.42                1.05*               (*seal-dump fjall allocs;
                                                                 steady-state hot path ~0.1)
disk (log + index)      259 + 32 MiB        259 + 58 MiB        see index-footprint note
vs RocksDB (557k)       0.33x               3.07x               target 532k: PASSED
                                                                stretch 1M: PASSED (all w)
```

RocksDB for reference allocates 19.4/event and its p99 at 4 writers is 100us.

### Batch-size sensitivity (4 writers, pipe variants)

| events/append | actor-pipe | reserve-pipe | note |
|---|---|---|---|
| 10 | 1.72M | 1.58M | workload of record; one seal in-window |
| 100 | 4.60M | 2.18M (noisy rep) | log fits under 256 MiB -> **no roll/seal in-window** |
| 1000 | 5.00M | 9.30M | ditto; encode parallelism (F8b) wins big |

Saturation estimate: at batch=1000 reserve-pipe writes the 253 MB log in ~0.11s, roughly
2.3 GB/s through write(), against a measured 10.8 GB/s streaming memcpy — the buffered-write
path (copy-to-page-cache + kernel bookkeeping) tops out around a quarter of memory bandwidth.
Single-thread ceiling (encode+CRC+write, no channels/index, inline roll sync): 0.7-1.9M ev/s
depending on group size and interference — the composed 4-writer engine sits essentially at
the ceiling of one appender thread, which is the point.

## 6. Correctness gates (run after every variant, every writer count)

1. **Full-log scan** with the byte-for-byte unchanged vertical_slice recovery scanner: every
   batch CRC+marker valid, A1 contiguity across all segments, event count, commutative global
   checksum, and per-stream **order-sensitive FNV checksums** all equal to ground truth
   computed from the workload itself.
2. **Index read path**: per-stream (count, FNV) via the variant's own index (fjall prefix
   scan / in-mem ptrs -> pread) on a 200-stream sample (100 hottest + spread), equal to
   ground truth.
3. **Recovery gate**: reopen the directory with the baseline recovery path (load fjall index,
   scan last segment, repair), then re-run gates 1+2. In-mem-index variants recovered with
   ~1,300 tail batches repaired — the rebuild-from-log-tail semantics doing exactly what D5
   intends. Clean logs showed zero torn bytes everywhere.

RocksDB runs were verified through their own global iterator + stream prefix scans against the
same ground truth. Zero gate failures across the entire spike.

## 7. Remaining bottlenecks & carry-forward notes

- **Single appender thread is the next wall** (F8a): 12 writers only add 20% over 4. The
  appender does encode+CRC+write serially; reserve-pipe shows parallel encode recovers this
  when batches are large. A hybrid (actor that offloads CRC/encode for large batches) is the
  obvious next experiment if >2M ev/s at small batches ever matters.
- **Depth-1 acks**: writers wait for each batch's ack before submitting the next (baseline
  semantics, kept for comparability). Per-writer pipelining would raise all numbers further.
- **Async seal weakens "scan only the last segment"** (F6): between roll and sealer
  completion, a crash requires scanning one extra segment (an index watermark tells recovery
  how far back). This is the seal_pipeline design's known trade; recovery-gate runs pass
  because finalize drains the sealer. Must be folded into the D5/F6 wording.
- **Reserve caveat**: shard index entries are published before the log write completes
  (reversed D1 order) — fine for a buffered spike, needs post-write publication in a real
  implementation.
- **Index disk footprint**: seal-time dumps land 58-92 MiB in fjall vs 32 MiB for per-event
  inserts — large write batches at rest carry journal write-amplification until memtable
  rotation catches up. Cold-storage accounting, not a hot-path cost; moot once D5's packed
  pointer blocks replace this representation at seal anyway.
- **Segment size interacts with benchmarks**: batch>=100 workloads fit 1M events under one
  256 MiB segment (framing overhead shrinks), so no seal occurs in-window — those sweep rows
  are partly measuring "no seal happened". Sustained load pays one background seal (~1.4s of
  sealer work) per ~150ms of appending at 1.7M ev/s; the pipeline absorbed the one seal in
  these runs, but a sustained multi-segment soak should confirm the sealer keeps up (or
  seals need to shrink — packed blocks do exactly that).
- **CRC**: no change recommended; crc32c-hw is 1.4x faster if ever needed, but a format
  change buys <2% at current throughput (and A12 forbids removing the CRC).

## Files

- `src/seglog.rs` — vertical_slice log format + UNCHANGED scanner; adds `batch_len`,
  `encode_batch_into` (reusable-buffer group encode), `roll_nosync`.
- `src/engine_mutex.rs` — baseline + F1 variants (index-mode knob).
- `src/engine_actor.rs` — F8a actor engine (write-mode, index-mode, async-seal knobs).
- `src/engine_reserve.rs` — F8b reservation + reorder engine (async-seal knob).
- `src/engine_rocks.rs` — Engine B reproduction (buffered path).
- `src/verify.rs` — ground-truth checksums + the three gates.
- `src/alloc_count.rs` — global allocation counter.

Bench data is cleaned up by the harness itself (`bench_data/` removed after runs).
