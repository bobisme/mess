# seal_pipeline — the D5/D6 seal pass, prototyped and measured

**Question.** The vertical-slice spike showed that BEFORE sealing, the custom engine loses
stream replay 1.7× (per-event pointer chase vs. RocksDB prefix scan: 1.05M vs. 1.75M ev/s) and
disk 1.31× (uncompressed vs. SST compression: 305.5 vs. 232.6 B/event). Does the seal pass —
packed pointer blocks + per-category zstd dictionaries + ~128-event block recompression +
binary-fuse filters — flip both columns, and what does sealing cost?

**Verdict.** **Both columns flip, decisively, and the seal is cheap.**

- **Stream replay: flipped.** Sealed-path replay of 1,000 random streams runs **2.7M ev/s**
  (hottest stream **7.4M ev/s**) vs. 1.61M ev/s for the pre-seal per-event chase on the same
  data and the 1.75M ev/s RocksDB target — **1.55× over the target, 1.7× over the chase**
  (hottest: 4.2× / 4.5×).
- **Disk: flipped.** Sealed segment is **44.0 B/event**, all artifacts included — **5.3× less
  than RocksDB's 232.6 B/event** and 6.2× less than the active segment (271.0 B/event).
- **Seal cost: background-task cheap.** **1.4–1.5 s single-threaded for a 258 MiB segment
  (~175 MB/s)**, peak RSS ≈ input segment + ~100 MiB. At the vertical-slice durable append
  ceiling (~4k ev/s), a segment fills in ~4 minutes; sealing it costs ~1.5 s of one core —
  under 1% duty cycle.

Run: `cargo run --release` (~15 s, writes and then deletes `bench_data/`, ~300 MiB transient;
`--keep` to keep the artifacts). All numbers below from a representative run on this machine
(same box as the vertical-slice figures); a repeat run agreed within ~2%.

## Setup

One active segment filled exactly like the vertical-slice workload: 1M events in batches of
10, 10,000 streams Zipf(1.1), seeded ~250 B JSON-ish payloads with realistic variance
(measured avg 256.0 B), 4 categories derived from `stream % 4`, each with a distinct event
shape (AccountCredited / OrderPlaced / ProfileUpdated / SensorReading). Segment writer is the
batch-framed v2 format reused from `vertical_slice/src/seglog.rs` (54 B BatchHeader +
subframes + 16 B CommitMarker). Result: **258.5 MiB active segment**, 7,411 streams present,
hottest stream = 150,690 events.

The per-event active index — the seal INPUT, not the subject — is an in-memory
`BTreeMap<(stream, version), EvRef>` stand-in for the fjall keyspace. Note the consequence:
our pre-seal pointer-chase baseline (1.61M ev/s) is *faster* than the vertical slice's
fjall-backed 1.05M ev/s, because the per-pointer lookup is free here and only the
one-`pread`-per-event cost remains. The sealed path therefore beats an **upper bound** of the
real pre-seal baseline.

### Sealed layout (the design choices this spike makes)

- Payloads are rewritten **clustered by (category, stream, version)**: blocks are
  category-homogeneous (so per-category dictionaries apply cleanly, D6) and a stream's events
  are contiguous — the same locality RocksDB buys with stream-prefixed keys and compaction.
  Global-position-order replay stays the job of the retained log segment (D1: the log is
  truth); sealed artifacts serve stream reads.
- Every event gets a sealed slot `eslot = block_id * 128 + index_in_block`; the per-stream
  packed pointer block is `varint(first eslot) + varint deltas` (with clustering the deltas
  are all 1 ≈ 1 B/event; the encoding stays general for non-clustered layouts).
- Uncompressed block = `(count+1) × u32 offsets ++ payloads`, compressed with the category's
  16 KiB dictionary at zstd L3; block index maps block → file offset (the offset table).
- Sealed file: blocks ++ pointer blocks ++ stream dir ++ block index ++ dicts ++ 4 filters
  (postcard-serialized xorf) ++ fixed footer.

## Seal cost (258.5 MiB segment, 1M events, single thread)

| stage | time |
|---|---|
| a. consolidate 1M per-event index entries → per-stream runs | 20 ms |
| b. filters (BinaryFuse8 + BinaryFuse16, streams + categories) | 0.5 ms |
| c1. read active segment back | 149 ms |
| c2. train 4 × 16 KiB zstd dicts (5k strided samples each) | 351 ms (68–105 ms each) |
| c3. recompress into 7,813 ~128-event blocks + stream to disk | 767 ms (318 MB/s payload in) |
| d. pointer blocks + dir + block index + dicts + filters + fsync | 172 ms |
| e. drop per-event entries | 17 ms |
| **total** | **1.50 s (172 MB/s of segment)** |

Peak memory: RSS high-water **376 MiB** vs. 73 MiB after fill — i.e. seal peak ≈ the 258 MiB
input segment buffer (`fs::read`; a streaming/mmap reader would shrink this) + ~45 MiB output
buffering + bookkeeping. Dict training is the biggest non-I/O line and is trivially
parallelizable per category, as is block compression.

## Disk: before / after

| representation | size | B/event | vs. RocksDB 232.6 B/ev |
|---|---|---|---|
| active segment (uncompressed, framed) | 258.5 MiB | 271.0 | loses 1.17× (slice w/ fjall index: 305.5, loses 1.31×) |
| **sealed total (all artifacts)** | **42.0 MiB** | **44.02** | **wins 5.3×** |

Sealed breakdown (B/event): payload blocks 42.60, pointer blocks 1.015, stream dir 0.148,
block index 0.148, dicts + filters + footer ≈ 0.10. Payload compression overall **6.0×**
(category detail: account 4.9×, order 7.1×, user 6.1×, sensor 7.3×) — at the top of the 3–6×
band D6 predicted, helped by stream-clustering putting near-identical JSON adjacent.
The vertical-slice extrapolation ("~2.5–3× win after compression") was conservative.

## Stream replay: before / after (same data, page-cache warm)

1,000 distinct uniform-random present streams (94,980 events) and the hottest stream
(150,690 events), identical stream sets before and after; replay results cross-checked
byte-identical via checksums.

| path | 1,000 random streams | hottest stream |
|---|---|---|
| pre-seal per-event pointer chase (1 pread/event) | 1.61M ev/s | 1.66M ev/s |
| packed pointers + coalesced preads on **uncompressed** segment | 9.12M ev/s | 11.50M ev/s |
| **sealed blocks (coalesced preads + dict decompress + 256-block LRU)** | **2.72M ev/s** | **7.40M ev/s** |
| vertical-slice references | slice chase 1.05M / RocksDB 1.75M | — |

- **The sealed path beats the RocksDB prefix-scan target (1.75M ev/s) by 1.55× on random
  streams and 4.2× on the hottest stream** — and beats the same-data pointer chase 1.7× /
  4.5×. D5's claim that packed blocks + coalescing are "required for the thesis" is confirmed
  in the favorable direction.
- Random-stream detail: 918 preads for 94,980 events (~103 events per I/O vs. 1 per I/O
  pre-seal); 8.3 MiB compressed read for 24 MiB of payload delivered.
- The intermediate row isolates the mechanism: **packing/coalescing alone is worth ~5.7×**;
  block decompression then costs ~3.4× of that back on cold random streams but buys the 6×
  disk win. If a workload ever needs it, "packed but uncompressed" is a legitimate
  per-category seal policy the format already supports (dict/flag per block).
- Zipf note: hot streams replay *faster* than cold ones here (7.4M vs 2.7M ev/s) because a
  hot stream's blocks are dense with its own events — the opposite of the pre-seal shape,
  where the hot stream was the worst case (round-1 ptr_index measured RMW worst on hot
  streams).

## Global replay after seal (sequential block decompress scan)

| path | ev/s | MB/s |
|---|---|---|
| active segment scan + frame parse (measured here) | 4.24M | 1,096 (file bytes) |
| **sealed sequential block scan** | **6.40M** | **260 compressed / 1,563 payload** |
| vertical-slice slice figure | 3.89M | 1,059 |

Global replay *improves* 1.5× despite decompression: 6× fewer file bytes per event dominates.
(Order note: the sealed scan delivers category/stream-major order. Position-order global
replay remains the retained log segment's job per D1 — or costs a 4 B/event pos→eslot
permutation table if it must ever run off sealed blocks alone.)

## Filters (xorf BinaryFuse8 / BinaryFuse16)

Empirical FPR on 100,000 absent stream ids; zero false negatives on all present keys.

| filter | keys | size | bytes/key | bits/key | measured FPR | ns/query |
|---|---|---|---|---|---|---|
| stream BinaryFuse8 | 7,411 | 9,746 B | 1.32 | 10.5 | **0.372%** (372/100k) | ~4 |
| stream BinaryFuse16 | 7,411 | 26,728 B | 3.61 | 28.9 | **0.002%** (2/100k) | ~4 |
| category BinaryFuse8 | 4 | 38 B | — | — | 0.387% | — |
| category BinaryFuse16 | 4 | 78 B | — | — | 0.002% | — |

Both match their theoretical rates (≈2⁻⁸ / ≈2⁻¹⁶). Bits/key runs ~15% above the asymptotic
figure at this small key count (binary-fuse construction overhead shrinks with n). Both are
noise in the size budget (0.01–0.03 B/event); **BinaryFuse16 is the right default** — 26 KiB
per segment for a 200× lower FPR means essentially zero wasted segment probes in a
many-segment store. Category filters are trivially worth carrying (38–78 B).

## Point reads by (stream, version) through the sealed path

10,000 uniform-random events (event-weighted, so hot streams dominate the sample):

| mode | mean | p50 | p99 |
|---|---|---|---|
| cold (no cache: pread + whole-block dict decompress every read) | 34.7 µs | 25.8 µs | 126 µs |
| cached (block already decompressed) | 13.4 µs | **1.7 µs** | 107 µs |
| Zipf mix through 256-block LRU (3.3% hit rate) | 34.6 µs | — | — |

Cold cost sits in the 20–90 µs whole-block-decompress band round 1 predicted; the cache
removes it (p50 1.7 µs). Two honest findings:

- **The cached p99 (~107 µs) is varint-seek, not decompression**: point read decodes the
  stream's delta list linearly to reach version v, and the hottest stream has 150k entries.
  Fix when it matters: a skip table every ~1k entries or run-length encoding (with clustering,
  a whole stream collapses to ~1 run) — both stay within the format.
- **A 256-block LRU is useless against a uniform/Zipf point-read mix over a full segment**
  (3.3% hit rate — the working set is thousands of blocks). The decompressed-block cache earns
  its keep on replay locality (48% hit rate during the all-streams verify pass), not on random
  point reads; size it for replay, not lookup.

## Correctness

Every present stream (7,411 streams, all 1M events) replayed through the sealed path and
compared against the active segment as crc32 over `(len ++ payload)` per event, in version
order: **0 mismatches**. Benchmark checksums also agree exactly between the pre-seal chase,
the packed-uncompressed path, and the sealed path on the sampled streams. The verify pass
costs 0.22 s (it is nearly a sequential scan thanks to clustering).

## Frictions / notes for the real implementation

1. **Seal input read is 10% of seal time** — `fs::read` of the whole segment is fine at 256
   MiB but sets peak RSS; stream or mmap in the real thing.
2. **eslot addressing depends on "blocks short only at category boundaries"** — the invariant
   `idx < 128` must survive any future block-sizing cleverness, or eslot needs to become an
   explicit (block, idx) pair.
3. **Pointer-block point reads are linear** in stream length (see above); replay is
   unaffected.
4. **The pre-seal baseline here flatters the enemy**: with the real fjall index the chase is
   ~1.05M ev/s, so the true sealed-vs-active win is larger than measured.
5. **Category = `stream % 4` is idealized**; real category skew changes dictionary quality and
   per-category block counts, not the mechanism. Dict training on 5k samples took <110 ms per
   category — affordable to redo per segment, so cross-segment payload drift is a non-issue.
6. Filters are serialized via `postcard` + xorf's serde support; a hand-rolled header would
   drop the dependency.

## Bottom line

Sealing does exactly what D5/D6 promised, with margin. Stream replay goes from **losing 1.7×**
to **beating the RocksDB target 1.55–4.2×**; disk goes from **losing 1.31×** to **winning
5.3×**; global replay improves a further 1.5×; and the pass costs **~1.5 s per 256 MiB
segment on one core** (<1% duty cycle at the durable-append ceiling) with peak memory ≈ one
segment. The remaining risks are engineering bookkeeping (eslot invariant, point-read seek,
streaming the input), not physics. Seal-time recompression + packed pointer blocks should stay
exactly where doc 12 now has them: load-bearing, Phase 4/5.
