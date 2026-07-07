# vertical_slice — composing the pieces: custom log + pointer index vs. RocksDB-twice

Spike question (the project's core thesis, end-to-end): does a custom append-only segment
log holding payload bytes **once** plus a fjall pointer index (Engine A, "Meridian slice",
per D1/D2/D5/D7 of `notes/mess-research/12_convergence.md`) beat the current mess_db shape —
payloads duplicated into two RocksDB column families (Engine B)?

**Short answer: not yet, and not everywhere.** Engine A wins global replay decisively
(2.1x) and ties on every durable-append mode (both are fsync-bound). Engine B wins buffered
append throughput (3.0x), stream replay (1.7x), and — the surprise — **bytes on disk (1.3x
smaller), despite storing every payload twice**, because RocksDB compresses its SSTs and the
v1 log format stores payloads raw. The thesis survives only together with its roadmap
companions: D6 seal-time compression is *required* for the disk claim, and D5 packed pointer
blocks are *required* for the stream-replay claim. The composition friction found while
wiring the pieces together (§7) is the other half of the deliverable.

Prior spikes validated each piece in isolation (`crash_log`, `ptr_index`, `compression`,
`dx_api` — see `notes/mess-research/13_spike_results.md`); this spike is the first time the
pieces run as one system against the incumbent.

## 1. Setup

```text
machine   AMD Ryzen 9 3900X, Samsung 970 EVO Plus 2TB NVMe (measured fdatasync ~5 ms
          under this workload — this single number shapes every durable result below),
          Linux 7.0.12-arch1-1
build     cargo 1.96.1, --release, lto=thin
          CXXFLAGS="-include cstdint" (librocksdb-sys on GCC 16)
workload  1,000,000 events (250k where noted), batches of 10 events to one stream,
          ~257 B mean JSON-ish payloads, 10,000 streams Zipf(s=1.1) => 7,404 distinct,
          hottest stream 152,210 events; 4 concurrent tokio writer tasks, each stream
          owned by one writer (stream % 4); identical seeded sequence for both engines
verify    after every load: full global replay count == events appended; payload
          checksums identical across engines (379976762); stream replay counts checked
          against the generator's per-stream totals
```

**Engine A** — segment log (crash_log's D2 framing, format v2: `stream_id` +
`first_stream_version` hoisted into the batch header; A1 contiguity check; A5/A7/A8
enforced; 256 MiB segments, fdatasync-sealed on roll) + fjall 3 index: per-event key
`stream_id||version -> EventPtr{segment_id, offset, len}` (16 B + 20 B) plus a stream-head
key, written after the log append, at fjall's default journal-buffered durability; fjall
persisted (SyncAll) at each segment roll so recovery only ever scans the last segment.

**Engine B** — rocksdb 0.21 (same pin as mess_db), two CFs `global` / `stream` with
mess_db's options (`create_*`, `increase_parallelism`), record shapes copied from
`mess_db/src/rocks/record.rs` (postcard-serialized, 26-char id string, stream name,
message type, payload, metadata) — full payload in both records; 10 events = 20 puts in
one WriteBatch. Both engines use the same in-memory head cache for expected-version
checks, isolating the storage path.

## 2. Append throughput and latency (per engine x durability)

| config | events | wall | events/s | batch p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|
| A-buffered (no fsync)          | 1,000,000 | 5.72 s   | **174,893** | 161 us   | 302 us  | 405 us  |
| A-sync-per-batch (fdatasync)   | 1,000,000 | 259.8 s  | 3,849   | 5.32 ms  | 13.0 ms | 15.2 ms |
| A-group-1ms                    | 1,000,000 | 264.6 s  | 3,779   | 7.22 ms  | 9.46 ms | 14.4 ms |
| A-group-5ms                    | 1,000,000 | 377.1 s  | 2,652   | 11.0 ms  | 12.3 ms | 14.5 ms |
| A-group-25ms                   | 250,000¹  | 264.3 s  | 946     | 31.1 ms  | 32.7 ms | 34.6 ms |
| B-nosync (WAL, no fsync)       | 1,000,000 | 1.88 s   | **531,602** | 67 us    | 85 us   | 104 us  |
| B-sync-per-batch (sync=true)   | 1,000,000 | 271.8 s  | 3,680   | 5.30 ms  | 13.4 ms | 17.2 ms |
| B-group-1ms (flush_wal task)   | 1,000,000 | 240.4 s  | 4,159   | 6.96 ms  | 8.68 ms | 12.5 ms |
| B-group-5ms                    | 1,000,000 | 368.7 s  | 2,712   | 10.9 ms  | 12.3 ms | 14.6 ms |
| B-group-25ms                   | 250,000¹  | 260.6 s  | 959     | 30.9 ms  | 31.9 ms | 34.2 ms |

¹ reduced from 1M per the spike spec (1M would take >10 min at this rate; throughput and
latency are rates, comparable across counts).

Readings:

- **Buffered: B wins 3.0x.** RocksDB's memtable+WAL path absorbs 20 puts/batch at 531k
  events/s. Engine A's ceiling is NOT the log (the crash child appends ~224k ev/s on one
  thread including index writes) — the cost is dominated by 10 individual fjall `insert`
  calls per batch plus a per-batch write syscall + CRC pass. ptr_index measured fjall at
  ~264k standalone inserts/s; composed, that budget is the bottleneck. See friction F1.
- **Every durable mode is an fsync benchmark, and the engines tie** (3.7–4.2k ev/s, batch
  p50 ≈ one 5 ms fdatasync). Log-once vs LSM-twice does not move durable append
  throughput at this batch size.
- Engine A's sync-per-batch gets natural grouping for free: 4 writers fdatasync
  concurrently and each sync covers whatever was written before it (p95 ≈ 13 ms = queued
  behind two other syncs). RocksDB does the same internally with sync=true.

## 3. Group commit curve (max_delay = 1 / 5 / 25 ms)

| max_delay | A events/s | A p50 / p99 | B events/s | B p50 / p99 |
|---|---:|---|---:|---|
| 1 ms  | 3,779 | 7.2 ms / 14.4 ms | 4,159 | 7.0 ms / 12.5 ms |
| 5 ms  | 2,652 | 11.0 ms / 14.5 ms | 2,712 | 10.9 ms / 14.6 ms |
| 25 ms | 946   | 31.1 ms / 34.6 ms | 959   | 30.9 ms / 34.2 ms |

The curve is the same for both engines because it is arithmetic, not engineering:
**throughput ≈ writers x batch_size / (max_delay + fsync_cost)**. With only 4 writers,
all in-flight batches are already pending when the window opens; waiting the full
`max_delay` collects nothing extra and simply adds latency. Group{1ms} ≈ sync-per-batch;
Group{25ms} is 4x *worse*. Group commit as specified (D7 `Group { max_delay }`) only pays
when writer concurrency >> group size — the window must close early when every in-flight
writer is already waiting (see friction F2). This is a real spec correction for D7, the
same class of finding as crash_log's A1.

## 4. Bytes on disk (after load, `du`-style block accounting)

| engine | total | per event | breakdown |
|---|---:|---:|---|
| A (1M events) | 291.3 MiB | **305.5 B/ev** | log 259.4 MiB (272 B/ev raw+framing) + fjall index 32.0 MiB (33.5 B/ev) |
| B (1M events) | 221.8 MiB | **232.6 B/ev** | 2 CFs, snappy-compressed SSTs; 221.2 MiB after full manual compaction |

Logical bytes tell the story the thesis expected: A writes ~272 B/event of log + 36 B of
index entries; B writes ~640 B/event of records (payload twice + id strings + names +
keys). **But B compresses ~2.8x at the SST level and A stores payloads raw (v1 format:
compression fields present, compression off), so B lands 1.31x smaller on disk.** For
250-byte JSON events, an uncompressed store-once loses to a compressed store-twice.
The `compression` spike measured 2.8–3.5x for per-event-with-dict and 3.4–6.3x for sealed
blocks on this kind of payload — applied to the log, A's footprint would drop to roughly
75–95 MiB (~85–110 B/ev), decisively below B. Conclusion: **D6 is not a Phase-5 luxury;
it is load-bearing for the thesis' disk claim.**

## 5. Replay (warm page cache; identical checksums verify both engines return the same bytes)

Global replay — full scan, 1M events (best of 3, all reps within 2%):

| engine | mechanism | throughput | events/s |
|---|---|---:|---:|
| A | sequential segment scan + frame parse | 1,059 MB/s (physical file bytes) | **3,892,049** |
| B | global CF iterator + postcard decode | 604 MB/s (logical KV bytes) | 1,847,080 |

**A wins global replay 2.1x** — a flat file read at NVMe/page-cache speed with CRC-checked
frame parsing beats LSM iteration. This is the replay/projection-rebuild path, the
operation event stores live and die by, and the strongest confirmed piece of the thesis.

Stream replay — 1,000 random non-empty streams (seeded), full replay each, 161,870 events:

| engine | mechanism | events/s | per-stream p50 | p99 | max |
|---|---|---:|---:|---:|---:|
| A | fjall ptr lookup -> pread per event | 1,051,405 | 25.8 us | 1.13 ms | 64.0 ms |
| B | stream CF prefix scan | **1,753,626** | 18.7 us | 0.82 ms | 35.6 ms |

**B wins stream replay 1.7x.** One `pread` per event through the pointer index cannot beat
a prefix scan over locality-clustered duplicated records — locality is exactly what the
duplication buys. The gap is worst on the hottest streams (max = the 152k-event stream:
64 ms vs 36 ms). This is precisely the gap D5's seal-time packed pointer blocks exist to
close (ptr_index measured 78–95x on block-packed replay), but the active-path slice alone
does not close it. Note the absolute numbers are fine for OLTP aggregate loads (p50 26 us
for a full stream) — B is ahead, A is not pathological.

## 6. Recovery / open time (Engine A)

| scenario | open time | scanned | outcome |
|---|---:|---:|---|
| clean close (1M events, tail segment 3.4 MiB) | **30 ms** | 3.4 MiB / 1,298 batches | repaired 0, truncated 0 |
| SIGKILL mid-write, run 0 (558,310 events survive) | 1.78 s | 144.7 MiB / 55,831 batches | repaired 1 batch, stop EndOfLog |
| SIGKILL mid-write, run 1 (560,630 events survive) | 1.80 s | 145.3 MiB / 56,063 batches | **torn tail: 2,715 B truncated**, stop IncompleteBatch |
| SIGKILL mid-write, run 2 (560,100 events survive) | 1.77 s | 145.2 MiB / 56,010 batches | repaired 1 batch, stop EndOfLog |

After every crash: full global re-scan confirms the recovered log is contiguous and equals
`next_global_pos`; 25 sampled streams' index entries resolve through the pointer index to
the correct payloads (spot checks pass in all runs). The crash child used split writes
(header+frames, then marker) to widen the torn-batch window; run 1 caught a genuinely torn
batch and the scanner truncated exactly it — D1/D2 behaving as crash_log promised, now on a
real filesystem instead of a simulated one.

Costs worth naming: crash-recovery scanning runs at ~85 MB/s (vs 1 GB/s pure scan) because
recovery does a per-batch index point-get + head write; worst case (full 256 MiB segment,
~1M events) extrapolates to ~3 s. Clean open is 30 ms because segments are sealed with
fsync + index persist at roll, so only the tail segment is ever scanned (A7 honored:
that is an invariant, not a checkpoint file).

## 7. Design friction discovered while composing (the second deliverable)

- **F1 — The per-event active index is Engine A's append bottleneck, not the log.**
  10 synchronous fjall inserts per batch cost more than framing+writing the batch itself
  (175k composed vs ~530k ev/s for B's single WriteBatch). D5's "cheap per-event entries"
  are cheap per *operation* but the operation count is per-event while the log pays
  per-batch. The index write path needs the same batching the log has (fjall write batch /
  one packed key-range insert per batch, or a small in-memory tail index flushed
  asynchronously — the tail is rebuildable from the log anyway, D1 pays rent here).
- **F2 — D7's `Group { max_delay }` semantics are wrong at low writer concurrency** (§3).
  The window must close as soon as every in-flight writer is pending (or track
  outstanding-writer count); a fixed delay window only subtracts. Needs a spec amendment
  the same way A1 amended D2.
- **F3 — An uncompressed log forfeits the disk claim** (§4). The v1 "fields present,
  compression off" format loses to snappy'd SSTs on realistic JSON. Either per-event-with-
  dict compression moves into the hot tail from day one, or the thesis' disk win waits for
  seal-time D6 and interim numbers will look embarrassing next to RocksDB.
- **F4 — Segment metadata has no home.** The scanner needs each segment's expected first
  global position (A1 contiguity seed). This spike smuggled it through the filename
  (`seg-{id}-{base_pos}.log`); a real implementation needs a fsync'd segment header +
  manifest, and that manifest must itself stay advisory (A7) or it becomes a second
  commit authority.
- **F5 — "Scan only the last segment" is an invariant chain, not a default.** It holds
  only because: roll fdatasyncs the old segment -> then persists the index -> then the new
  segment accepts writes. Kill-safety of the fjall journal covered everything else in
  testing (repaired <= 1 batch per crash), but an OS-crash (not testable with SIGKILL)
  could lose index entries for *earlier* segments while their heads survive — heads and
  ptrs for streams untouched by the tail segment can then silently point past the log.
  The full design needs an index low-watermark ("index durable through segment N") written
  at roll, and recovery must distrust anything after it.
- **F6 — Recovery-time index verification wants a watermark.** Re-verifying 56k batches
  against fjall at open costs 1.8 s per 145 MiB (~20x slower than the pure scan). Fine at
  256 MiB segment scale, but it argues for an advisory (never authoritative) per-segment
  "index verified through offset X" checkpoint to make crash reopen ~O(tail-since-flush).
- **F7 — Pointer-chased stream reads need coalescing.** Sequential events of a stream are
  usually adjacent in the same segment (same batch => contiguous bytes); the reader should
  merge adjacent EventPtrs into one pread instead of one syscall per event. Cheap to add,
  probably closes much of the 1.7x stream-replay gap before packed blocks even land.
- **F8 — Position assignment forces a global mutex around the log append** (positions are
  dense by construction). Fine at these rates (the mutex never showed up next to fsync and
  fjall), but it means "many threads within that process" (D9) serialize at the log; the
  actor/single-writer model the roadmap already assumes is the honest architecture.
- Byte-accounting caveat, for fairness: part of B's logical bloat (id strings, stream-name
  strings, type strings in every record) is mess_db's shape, not RocksDB's fault; Engine A
  never stores them because D3 interning is assumed. The on-disk comparison in §4 embeds
  both differences — that is what "current mess_db approach" means, but a leaner Rocks
  schema could shrink B further.

## 8. Verdict

```text
where Engine A wins:
  global replay        2.1x  (3.89M vs 1.85M ev/s; 1.06 GB/s flat scan)  — the thesis' core
  clean open           30 ms; crash recovery 1.8 s / 145 MiB with provable truncation of
                       exactly the torn batch and index self-repair (Engine B's recovery
                       was not measured here; RocksDB's is its own WAL replay)
  durable appends      tie (both fsync-bound at ~3.7-4.2k ev/s, p50 ~5.3 ms)

where RocksDB wins:
  buffered appends     3.0x  (532k vs 175k ev/s)   — A's per-event index inserts (F1)
  stream replay        1.7x  (1.75M vs 1.05M ev/s) — duplication buys locality (F7/D5 gap)
  bytes on disk        1.31x (232.6 vs 305.5 B/ev) — compression beats deduplication (F3)
```

The composed v1 slice does **not** beat RocksDB across the board — said plainly. But the
losses map one-to-one onto components the convergence doc already schedules (D6 compression:
would flip §4 to a ~2.5–3x A win; D5 packed blocks + F7 coalescing: closes §5b; F1 index
batching: recoverable, the log itself sustains >220k ev/s single-threaded), while A's wins
sit exactly where an event store earns its keep (replay-everything, provable crash
semantics) and are not reachable by Engine B without abandoning its shape. The thesis
survives as a roadmap bet, not as a v1 fact; F1–F6 are the price list, and F2 is a
mandatory D7 spec amendment discovered only because the pieces were composed.

## 9. Reproduce

```bash
cd spikes/vertical_slice
CXXFLAGS="-include cstdint" cargo build --release   # GCC 16 / librocksdb-sys
./target/release/vertical_slice            # everything (~25 min, fsync-bound)
./target/release/vertical_slice load-a     # or per phase: load-a | load-b | replays | recovery
```

Raw logs from the recorded run are the source of every number above. `bench_data/` keeps
the A-buffered (292 MiB) and B-nosync (222 MiB) stores for replay reruns; delete freely.

```text
crate layout: src/seglog.rs   log format+writer+scanner (adapted from spikes/crash_log)
              src/engine_a.rs Meridian slice (log + fjall ptrs/heads, 3 durability modes)
              src/engine_b.rs RocksDB baseline (mess_db record shapes, 3 durability modes)
              src/workload.rs seeded generator; src/main.rs drivers/measurement/crash child
```
