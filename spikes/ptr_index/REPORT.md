# ptr_index spike — quantifying D5 (per-event active index vs RMW pointer blocks)

Tests the claim in `notes/mess-research/12_convergence.md` D5: maintaining packed
pointer blocks via read-modify-write on every append reintroduces write
amplification, so the active path should use cheap per-event index entries and
consolidate into packed blocks only at segment seal time.

## Setup

- Machine: AMD Ryzen 9 3900X, Samsung 970 EVO Plus 2TB NVMe, ext4, Linux 7.0.12-arch1-1
- Toolchain: rustc 1.96.1, `redb 4.1.0`, `fjall 3.1.6`, built `--release` with `-Ctarget-cpu=native`
- Value indexed: `EventPtr { segment_id: u64, offset: u64, len: u32 }` = 20 bytes
- Strategy A (PER-EVENT): key `stream_id(8B BE) || version(8B BE)` -> 20B EventPtr
- Strategy B (RMW BLOCK): key `stream_id(8B BE) || block_no(8B BE)` -> packed block of up to
  256 EventPtrs; every append reads the current block, appends 20B, writes the whole block back
- Workload: **500,000 appends**, 10,000 streams, Zipf(s=1.1) stream choice, seed 42 —
  identical sequence for every combo. 9,885 distinct streams used; hottest stream got
  **75,659 events** (296 blocks); most streams got a handful.
- Commit granularity: 100 appends per transaction/batch (group commit).
- Durability (as noted per backend):
  - **redb**: default `Durability::Immediate` — fsync on every commit (5,000 fsyncs/run).
  - **fjall (default)**: every insert appends to the journal and persists with
    `PersistMode::Buffer` (write to OS buffers, **no fsync**; survives process crash, not
    power loss). One final `persist(SyncAll)` before the clock stops.
  - **fjall-sync**: `persist(PersistMode::SyncAll)` after each 100-append batch — the
    apples-to-apples durability match for redb's default.
- On-disk size = allocated bytes (`st_blocks`, du-style); fjall measured after flushing the
  memtable to sstables, and includes its journal.
- Read benches run in-process immediately after the writes (warm page cache); hot-stream
  latency is the mean of 50 full scans.

Run it: `RUSTFLAGS="-Ctarget-cpu=native" cargo run --release -- [n_appends]`
(the env var sidesteps the repo-root cargo config that demands lld, which is not installed;
see `.cargo/config.toml` here).

## Results (500,000 appends, measured 2026-07-07)

| combo | wall time | appends/s | logical bytes written | RMW read-back | on-disk | hot replay (75,659 ev) | cold replay (1,000 streams) |
|---|---|---|---|---|---|---|---|
| redb / A per-event | 14.92 s | 33,515 | 17.17 MiB | — | 39.85 MiB | 5.98 ms (79 ns/ev) | 2.74 ms (2.7 µs/stream) |
| redb / B rmw-block | 49.92 s | 10,016 | **942.03 MiB** | 932.50 MiB | 16.55 MiB | 63 µs (0.8 ns/ev) | 0.59 ms (0.6 µs/stream) |
| fjall / A per-event | 1.89 s | **264,354** | 17.17 MiB | — | 55.58 MiB | 22.29 ms (295 ns/ev) | 8.92 ms (8.9 µs/stream) |
| fjall / B rmw-block | 5.33 s | 93,779 | **942.03 MiB** | 932.50 MiB | 16.41 MiB | 285 µs (3.8 ns/ev) | 2.62 ms (2.6 µs/stream) |
| fjall-sync / A per-event | 19.92 s | 25,103 | 17.17 MiB | — | 55.58 MiB | 39.03 ms (516 ns/ev) | 9.34 ms (9.3 µs/stream) |
| fjall-sync / B rmw-block | 53.39 s | 9,365 | **942.03 MiB** | 932.50 MiB | 16.41 MiB | 291 µs (3.8 ns/ev) | 2.44 ms (2.4 µs/stream) |

Cold set = 1,000 uniformly sampled non-empty streams outside the top-100 (18,605 events
total, ~19 events/stream).

### Write amplification, B vs A

| backend | logical bytes (B/A) | wall time (B/A) | disk (B/A) |
|---|---|---|---|
| redb | **54.9x** | 3.35x | 0.42x |
| fjall (default) | **54.9x** | 2.82x | 0.30x |
| fjall-sync | **54.9x** | 2.68x | 0.30x |

Strategy B moved **1.83 GiB** through the backend API (read-back + write-back) to index
**9.5 MiB** of pointers. Average block written per append: **98.0 entries (~3.9 KiB
read+written per append)** — and this grows with stream depth: the same benchmark at
20,000 appends averaged 61.7 entries/append (34.7x amp). Zipf means the hot streams that
dominate append volume are exactly the ones with the deepest (most expensive) blocks; the
per-append RMW cost keeps growing with stream depth until the block caps at 256 entries
(~5 KiB write + ~5 KiB read per append, steady state).

### Read path

Packed blocks win reads decisively, as expected:

- Hot stream (75,659 events): redb 63 µs (B) vs 5.98 ms (A) — **~95x**; fjall 285 µs vs
  22.3 ms — **~78x**.
- Cold streams: redb 0.6 µs vs 2.7 µs per stream (~4.6x); fjall 2.6 µs vs 8.9 µs (~3.4x).
- Disk is also ~2.5–3.4x smaller with blocks (16-byte key amortized over up to 256 entries
  instead of paid per entry, fewer tree/sst entries).

But even the *worst* per-event replay is 22–39 ms for a 75k-event stream and ~9 µs for a
typical stream — entirely acceptable for an *active segment* whose contents are bounded
and short-lived before sealing.

## Verdict on D5

**Confirmed, with numbers.** The RMW-block strategy costs ~55x logical write
amplification and 2.7–3.4x wall time on the append path for this workload, and the
amplification grows with stream depth — the penalty lands hardest precisely on the
hottest streams. Meanwhile everything blocks are good at (fast ordered replay, compact
storage) is a *read-time and seal-time* property, not something that needs to be
maintained per-append. Per-event entries on the active path + one-shot consolidation into
packed, variable-length blocks at seal time captures both ends: A's append cost (36
logical bytes/event, flat) and B's read performance/footprint for sealed data. That is
exactly the D5 design. The doc's rejection of fixed 256-entry blocks is also supported:
at seal time you know each stream's true length, so blocks can be sized to what exists
instead of averaging half-empty.

### redb or fjall for the active index?

**fjall.** At its default durability it sustained **264k appends/s** vs redb's **33.5k**
(~8x) on per-event writes — an LSM absorbs small sorted inserts via memtable+journal,
while redb's copy-on-write B-tree pays page rewrites plus an fsync per commit. The
durability asymmetry is the point, not a flaw: the active index is rebuildable from the
event log (per D5, "tail rebuildable from log"), so it does not need its own fsync — the
log's fsync is the durability boundary. If you do force matched fsync-per-batch,
fjall-sync (25.1k/s) and redb (33.5k/s) are comparable, with redb slightly ahead and
also ~2–4x faster on the read path (79 ns/ev vs 295 ns/ev hot-scan) — so redb remains a
fine choice if the index must be independently durable, or if a single-file store with no
background threads is preferred. For the hot append path as designed (index as a
rebuildable cache over the log), fjall's write absorption is the better fit.

### Caveats

- "Logical bytes" is measured at the backend API boundary; physical amplification
  differs per engine (redb CoW page writes; fjall compaction rewrites) — but the API-level
  number is the part the strategy choice controls, and it is the same 54.9x on both.
- Reads are warm-cache and in-process; cold-cache IO would favor packed blocks even more
  (1 block read vs N key lookups), which again argues for blocks *at seal time*, not on
  the append path.
- Single-threaded writer; both engines would shift under concurrency, but the
  amplification ratio would not.
