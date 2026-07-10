# perfect_hash_directory spike — bn-1hl experiment 1

Benchmarks the sealed stream-directory lookup (doc 12 D10: "stream-name / registry
string directories: MAYBE — benchmark ... perfect hashing", elaborated by bn-1hl into
"benchmark BinaryFuse16/fjall vs a minimal perfect hash vs sorted+binary-search") — the
exact `stream_id -> pointer` lookup that sits behind
`crates/mess-index/src/sealed/filter.rs`'s `BinaryFuse16` membership pre-filter.

This file is a project deliverable (spike convention: every `spikes/*` directory carries
a verdict writeup, per `spikes/README.md`), not a conversational summary.

## What's compared

1. `hashmap` — `std::collections::HashMap<u64, V>`, exactly what
   `SealedSegmentIndex::dir` (`crates/mess-index/src/sealed/segment.rs`) uses today,
   built once at sidecar-parse time.
2. `sorted` — a sorted `Vec<(u64, V)>` + `binary_search_by_key`, the bone's baseline.
3. `mph` — a hand-rolled fixed-seed FKS two-level perfect hash (`src/mph.rs`, ~150
   lines, no external MPH crate: `boomphf` is not a workspace dependency and the bone
   says avoid adding a heavyweight one for a benchmark-gated experiment). Worst-case
   O(1) lookup: two `splitmix64`-family mixes and two array reads, with an explicit
   key-equality check (this is an exact structure, not a filter — an absent key must
   never return `Some`, verified by a test over 50,000 absent probes).
4. `fjall` — the production on-disk KV backend (`fjall::Database`/`Keyspace`, 3.1.6,
   `PersistMode::SyncAll`), the exact API `crates/mess-index/src/meta/mod.rs`'s
   `MetaStore` uses for the stream-head registry — the bone's explicit "fjall lookup
   path" comparison point, run against real fs (never tmpfs).

`V = Ptr { a: u64, b: u64, c: u64 }` (24 bytes) — sized like a real `EventPtr` plus a
little range info, not the full 56-byte `DirEntry`, so the memory comparison is about
indexing overhead, not payload size (identical payload size across all four structures).

Keys: `n` distinct pseudo-random `u64`s (xorshift64, fixed seed per `n`) — stream ids are
pre-hashed in production, so random keys are the representative case (sequential keys
would flatter sorted-array binary search's cache behavior unrealistically).

## Method

- Machine: this workspace's host (AMD Ryzen 9 3900X, 24 threads), `/home` on
  `nvme0n1p3` (Samsung 970 EVO Plus 2TB, ext4, 86% full at run time).
- Sizes: 1,000 / 100,000 / 1,000,000 streams (the bone's "realistic directory sizes").
- 200,000 probes each for hits (sampled with replacement from the real key set) and
  misses (freshly generated keys checked disjoint from the key set).
- `fjall` writes to `$TMPDIR` (set to `$HOME/.cache/mess-bench-scratch`, real ext4, not
  tmpfs) so on-disk sizes and read-path timings reflect a real filesystem, not page-cache
  tmpfs shortcuts.
- Memory: `hashmap` = `capacity() * (1 control byte + sizeof(K,V))` (hashbrown's
  `RawTable` layout); `sorted` = `len * sizeof((K,V))` exactly; `mph` =
  `Mph::approx_bytes()` (bucket metadata + both slot arrays); `fjall` = `du`-style
  allocated bytes under the scratch dir after a full `persist(SyncAll)`.
- Run: `TMPDIR=$HOME/.cache/mess-bench-scratch cargo run --release` (or `cargo test
  --release` for the `mph` correctness properties).

## Results (release, this host, 2026-07-10)

| n | structure | build | hit (ns) | miss (ns) | bytes/key |
|---|---|---|---|---|---|
| 1,000 | hashmap | 0.1 ms | 9.0 | 8.0 | 59.1 |
| 1,000 | sorted | 0.0 ms | 15.2 | 13.2 | 32.0 |
| 1,000 | mph | 0.2 ms | 21.4 | 19.9 | 78.7 |
| 1,000 | fjall | 12.5 ms | 286.1 | 245.8 | 67,112.4 |
| 100,000 | hashmap | 4.0 ms | 20.3 | 12.6 | 37.8 |
| 100,000 | sorted | 1.3 ms | 62.1 | 59.5 | 32.0 |
| 100,000 | mph | 25.8 ms | 32.0 | 21.4 | 80.1 |
| 100,000 | fjall | 185.1 ms | 705.1 | 624.1 | 671.1 |
| 1,000,000 | hashmap | 81.5 ms | 62.4 | 16.4 | 60.6 |
| 1,000,000 | sorted | 16.9 ms | 381.3 | 310.0 | 32.0 |
| 1,000,000 | mph | 415.3 ms | 77.4 | 50.4 | 80.0 |
| 1,000,000 | fjall | 1,844.1 ms | 1,865.2 | 570.1 | 107.4 |

The MPH's slot-table blowup was measured at 1.96-2.00x the key count across all
three sizes (matches the FKS construction's theoretical ~2x expectation for
`sum(bucket_len^2) <= 4n` with Poisson(1)-ish bucket occupancy — see `src/mph.rs` docs).

`fjall`'s `bytes/key` at n=1,000 (67 KB/key!) is journal/SST fixed-cost overhead that
hasn't amortized yet at that scale, not a real per-key cost — it drops to 107 B/key by
1M keys. Not a fair comparison to the in-memory structures' bytes/key at small n; included
for completeness since the bone names fjall explicitly.

## Verdict: DECLINE the hand-rolled MPH

At every tested size, `HashMap` — what the sealed directory already uses — beats the
MPH on both axes: faster hits (20-62 ns vs 32-77 ns), faster misses, and less memory
(38-61 B/key vs 79-80 B/key). The FKS construction's O(1) worst-case lookup does not
translate into a real win here: two `splitmix64` mixes plus two cache-line-scattered
array reads costs more than `hashbrown`'s SIMD-probed open addressing already provides,
and the 2x slot-table blowup (inherent to a "no collisions ever" static perfect hash at
this bucket-sizing scheme) costs more memory than a load-factor-0.875 hash table.
Building is also 5-20x slower than `HashMap::insert` in a loop. There is no metric on
which the MPH wins — a clean decline per D10 ("accelerators admitted only by evidence").

`sorted` + binary search loses on lookup latency at every size (1.7-6x slower hits than
`HashMap`, growing with `n` as expected for O(log n) vs O(1) amortized) but is the most
memory-compact exact structure (32 B/key flat, vs `HashMap`'s 38-61 B/key
capacity-dependent overhead) and has the fastest build (no hashing, just a sort) — a
real trade-off, not a strict loss, but not enough to displace `HashMap` for this
lookup-dominated read path.

`fjall` costs 30-280x more per lookup than `HashMap` at every size (expected: real
`mmap`+LSM I/O path vs an in-process hash table) — it is doing a fundamentally different
job (durable, rebuild-free-of-the-log persistence for the *registry*, not a rebuildable
per-segment sidecar) and is not a candidate replacement for the sealed directory; it
already IS mess-index's chosen backend for the piece of the system that actually needs
persistence (`MetaStore`, see `crates/mess-index/src/meta/mod.rs`), which is the correct
job split — no change indicated there either.

Net: keep `HashMap<u64, DirEntry>` for the sealed directory, unchanged. No production
code touched by this experiment.

## Correctness

`src/mph.rs`'s `exact_membership_no_false_hits` test builds a 5,000-key table and checks
zero false positives across 50,000 sampled absent keys (an MPH, unlike the BinaryFuse16
filter it would sit behind, is an exact structure — any false positive would be a
correctness bug, not a tunable FPR). `empty_build` covers the zero-key edge case.
`cargo test --release` (2 tests) and `cargo clippy --all-targets -- -D warnings` both
pass clean in this spike.
