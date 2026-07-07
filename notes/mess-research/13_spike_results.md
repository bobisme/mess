# 13 — Spike results

Recorded: 2026-07-07. Four spikes were run to test the quantitative bets in
[12_convergence.md](./12_convergence.md) before committing to the build. All spike code is
throwaway, lives under `spikes/` (each a standalone crate opted out of the workspace), and every
number below was measured on this machine (Ryzen 9 3900X, NVMe). Full details and raw output are
in each spike's `REPORT.md`.

## Scorecard

| Spike | Question | Verdict |
|---|---|---|
| `spikes/crash_log` | Do D1/D2 (log authority + batch framing) survive crash injection? | **Holds** — 12,000 randomized cases, 0 acked batches lost, 0 partial batches visible. Found one mandatory spec addition (A1) |
| `spikes/compression` | Does D6's 3–10× compression claim hold? | **Holds at 3–6×** — for sealed blocks; per-event-without-dict is pointless |
| `spikes/ptr_index` | Does D5's RMW write-amplification claim hold? | **Holds decisively** — 55× logical write amp for RMW blocks; fjall picked for the active index |
| `spikes/dx_api` | Does the north-star API work on the existing backend? | **Works end-to-end** — retry loop proven under real contention; produced a friction list and found one real bug (fixed, see §5) |

Every bet survived, each with a sharpening. The corrections below have been folded back into
doc 12.

## 1. crash_log — batch framing + recovery

Implementation: BatchHeader / subframes / CommitMarker per D2, recovery scanner per D1, a
fault-injecting writer (crash at any byte offset or protocol step, optional loss of un-fsynced
tail), 19 deterministic edge-case tests plus a seeded randomized loop.

```text
randomized crash loop: 12,000 iterations, 10,505 with an injected crash
15,067 acked batches verified intact and in order
0 partial batches ever visible
0 acked batches lost
171 unacked-but-complete batches surfaced by recovery (allowed; see A6)
recovery idempotent; post-recovery appends contiguous in every iteration
```

Spec issues found (A1–A8, detailed in `spikes/crash_log/REPORT.md`):

```text
A1 (MANDATORY): "marker validates" is insufficient — a stale but fully
    CRC-valid old batch in recycled disk space directly after the last good
    batch is accepted and resurrects deleted data (demonstrated in a test).
    Fix adopted: scan-time contiguity check (first_global_pos must equal the
    expected next position), plus a segment epoch / prev-batch link in
    BatchHeader as defense in depth.
A2: total_len needs a sanity cap and an exact definition (whole on-disk
    batch, header through marker).
A3: CommitMarker bytes must be inside CRC coverage (echo field zeroed
    during hashing), else a corrupted length echo can pass.
A4: the full-batch CRC is load-bearing against block-write reordering
    (marker persisting before frames); magic + length echo alone is unsafe.
    Real reordering needs the Phase 3 ALICE-style harness.
A5: empty batches (frame_count == 0) break the contiguity guard — forbid.
A6: recovery can surface fully-written-but-unacked batches (171/12,000
    observed) — the write-side mirror of D7's CursorRegressed. The spec and
    the dedupe window must own this case explicitly.
A7: scan-from-zero per segment is fine iff segment boundaries align with
    batch boundaries; checkpoint offsets are advisory-only (never a second
    commit authority).
A8: batches never span segments.
```

## 2. compression — per-category zstd dictionaries

Setup: 4 synthetic categories of realistic-variance JSON (transactions 348 B, social 591 B,
orders 433 B, IoT 198 B), 45k eval events/category, zstd level 3, dictionaries trained on 5k
held-out samples, 128-event blocks.

| strategy | transactions | social | orders | iot |
|---|---|---|---|---|
| per-event, no dict | 1.25× | 1.38× | 1.35× | 1.18× |
| per-event, 16 KiB dict | 2.79× | 2.84× | 3.43× | 3.49× |
| block-128, no dict | 3.66× | 3.27× | 4.45× | 6.11× |
| block-128 + dict (best) | 3.70× | 3.44× | 4.58× | 6.32× |

Findings:

```text
- D6's "3-10x" corrected to 3-6x (sealed blocks). 10x is unreachable on
  realistic-variance JSON at L3.
- per-event WITHOUT a dictionary is pointless (1.2-1.4x) — no "just zstd
  each frame" shortcut.
- the dictionary matters for per-event (1.3x -> ~3x, and 2-5x faster
  decode) but adds only 0-6% on top of 128-event blocks: a block is
  effectively its own dictionary.
- random-read cost: per-event+dict point-decodes in 0.3-1.1 µs; block-128
  costs 20-90 µs (whole-block decompress) — wants a small
  decompressed-block cache. Scans amortize blocks to 0.2-0.7 µs/event at
  0.9-1.4 GB/s.
- 16 KiB dictionaries suffice (64 KiB tied); training 60-225 ms on 5k samples.
```

Adopted format decision: sealed-block compression (~128 events) with per-category 16 KiB
dictionaries and a `dict_id` in the block header; per-event-with-dict only for the unsealed hot
tail; decompressed-block cache in front of sealed point reads.

## 3. ptr_index — active-index maintenance strategy

Setup: 500k appends, 10k streams, Zipf s≈1.1 (hottest stream 75,659 events), 100 appends per
commit, EventPtr ≈ 20 B, strategies A (per-event key) vs B (RMW 256-entry block), on redb and
fjall.

| metric | per-event (A) | RMW block (B) |
|---|---|---|
| logical bytes through backend | 17.2 MiB | 942 MiB written + 933 MiB read back = **54.9× write amp** |
| avg block read+written per append (B) | — | 98 entries ≈ 3.9 KiB, growing with stream depth |
| redb appends/s | 33.5k | 10.0k (3.35× slower) |
| fjall appends/s (default durability) | 264k | 94k (2.8× slower) |
| fjall appends/s (fsync per batch) | 25.1k | 9.4k |
| hot-stream replay (redb) | 5.98 ms | 63 µs (**78–95× faster for blocks**) |
| disk footprint | baseline | ~3× smaller |

Verdict — D5 confirmed on both ends: RMW pays ~55× write amp on the append path, worst on
exactly the hottest streams; packed blocks win replay and footprint, which are read/seal-time
properties. **Per-event active entries + seal-time consolidation into variable-length blocks
gets both.**

Backend pick: **fjall for the active index** — 8× redb's append throughput at its default
journal-buffered durability, which is legitimate for this table because the active index is
rebuildable from the log (invariant I5 paying rent). redb only draws even under forced
fsync-per-commit, where it is 2–4× faster on reads.

## 4. dx_api — north-star API on the existing backend

`store.command::<Account, _>("account-123", Withdraw { amount: 30 }).await?` works end-to-end
against the real RocksDB actor: load → decide → append-with-expected-version, plain traits
(`Event` / `Aggregate` / `Decide<C>`), typed `CommandError::{Domain, Conflict, Store}`, and a
store-free Given-When-Then kit with positional diffs. 11 tests pass.

The convincing test: 8 tokio tasks × 25 deposits against ONE stream through `command()` —
exact final balance every run; 200 commands took ~950–1,020 attempts (one command needed 24
tries), so the retry loop is real, not decorative.

Consequences adopted:

```text
- default retry budget must exceed 16 attempts and use jittered backoff
  (observed max: 24 under only 8 writers); longer term, actor-side
  conditional append removes the race entirely.
```

Friction found in current mess_db (full list in `spikes/dx_api/REPORT.md`):

```text
1. CACHED_GLOBAL process-wide static mut     -> real bug; FIXED, see §5
2. no atomic multi-event append              -> multi-event decide results
   are N chained CAS writes; torn appends possible on the interim backend.
   Known limitation for Phase 2; solved properly by D2 batches in Phase 3.
3. no ExpectedVersion::Any                   -> every append forces
   read-before-write; expected-version convention is off-by-one bait
4. read path can't paginate (LIMIT_MAX 10k, stream_pos option ignored)
   -> blocks snapshot+tail replay for >10k-event aggregates
5. panic-prone actor: todo!() on Relaxed writes kills the store;
   run_actor unwraps; fetch_messages unwraps a dead-actor recv
6. ident::Id is a git-only dep and not re-exported, but required to
   construct any write
```

## 5. Code fix landed: CACHED_GLOBAL

The spike-confirmed bug — `static mut CACHED_GLOBAL: AtomicU64` in `mess_db/src/rocks/write.rs`
leaking the last global position across independent DB instances in one process (a fresh empty
DB's first append landed at global position 7) — is fixed:

```text
- cache moved into the DB struct (mess_db/src/rocks/db.rs: cached_global: AtomicU64)
- cache advance moved into write_records with fetch_max, which also fixes a
  second latent bug: the async write path read through the cache but never
  updated it, so warm-cache consecutive async writes could compute the same
  next-global and silently overwrite a record
- unsafe blocks and the static_mut_refs warnings are gone
- regression test added (global_position_does_not_leak_across_db_instances);
  all 37 mess_db unit tests pass; the spike's global_leak example now prints
  "db2 FIRST append: global position = Some(1)"
```

Pre-existing, unrelated: `mess_db` doctests in `read.rs` fail (they reference a long-renamed
`ReadMessages` type) — not touched by this fix.

## 6. Environment notes

```text
- repo root .cargo/config.toml forces -fuse-ld=lld; lld is NOT installed on
  this machine. Each spike carries a local workaround (.cargo/config.toml
  override or RUSTFLAGS="-Ctarget-cpu=native" env, which replaces the
  inherited flags). To build the workspace: install lld, or drop the flag.
- librocksdb-sys needs CXXFLAGS="-include cstdint" on modern GCC (16.x here).
- spikes/ptr_index/bench_data (201 MB) was deleted after the run; spike
  target/ dirs remain and are gitignored — safe to delete for space.
```

## 7. What changed in doc 12 as a result

```text
D2: batch acceptance rules A1-A8 added (contiguity check mandatory;
    segment epoch / prev-batch link; total_len cap; marker in CRC coverage;
    empty batches forbidden; batches never span segments)
D5: measured 55x write-amp figure recorded; fjall selected for the active
    index at relaxed durability
D6: claim corrected to 3-6x; sealed-block + per-category 16 KiB dict +
    dict_id format decision recorded; per-event-no-dict rejected
D7: A6 (recovered-but-unacked batches) added as the write-side mirror of
    CursorRegressed
Phase 1: retry budget guidance (jittered, >16 attempts) added
```
