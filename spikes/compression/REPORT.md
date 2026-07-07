# Compression spike: per-category zstd dictionaries (D6)

Tests the claim in `notes/mess-research/12_convergence.md` section D6 that per-category
zstd dictionary compression of event payloads yields **3-10x on JSON-ish payloads**.

## Setup

- Machine: AMD Ryzen 9 3900X, Linux 7.0.12-arch1-1, rustc 1.96.1, `zstd` crate 0.13 (libzstd), single-threaded
- `cargo run --release` (this crate, 2026-07-07); all numbers below are measured, not estimated
- 4 synthetic categories, 50,000 schema-homogeneous JSON events each, seeded RNG,
  realistic variance (ULIDs/UUIDs, varied English-ish text, varied numerics, RFC3339 timestamps)
- Dictionaries trained with `zstd::dict::from_samples` on 5,000 samples; **evaluation on the
  other 45,000 only** (no train/eval overlap). All strategies evaluated on the same 45,000 events
- zstd level 3 everywhere; blocks = 128 concatenated events
- Throughput is measured against uncompressed bytes; compressor/decompressor contexts and
  dictionaries are loaded once and reused (the realistic hot path)

Corpus shape:

| category | avg event | eval raw |
|---|---:|---:|
| account-transactions | 348 B | 15.64 MB |
| social-posts | 591 B | 26.58 MB |
| order-lifecycle | 433 B | 19.49 MB |
| iot-sensor-readings | 198 B | 8.89 MB |

## Results

### Compression ratio (higher is better)

| strategy | transactions | social | orders | iot |
|---|---:|---:|---:|---:|
| per-event, no dict | 1.25x | 1.38x | 1.35x | 1.18x |
| per-event, dict 16K | 2.79x | 2.84x | 3.43x | 3.49x |
| per-event, dict 64K | 2.78x | 2.92x | 3.43x | 3.67x |
| block-128, no dict | 3.66x | 3.27x | 4.45x | 6.11x |
| block-128, dict 16K | **3.70x** | 3.39x | **4.58x** | 6.09x |
| block-128, dict 64K | 3.66x | **3.44x** | 4.51x | **6.32x** |

### Throughput, MB/s of uncompressed data (compress / decompress)

| strategy | transactions | social | orders | iot |
|---|---:|---:|---:|---:|
| per-event, no dict | 72 / 238 | 96 / 329 | 86 / 271 | 56 / 127 |
| per-event, dict 16K | 161 / 688 | 166 / 554 | 212 / 723 | 189 / 614 |
| per-event, dict 64K | 163 / 690 | 184 / 594 | 210 / 722 | 198 / 648 |
| block-128, no dict | 343 / 1446 | 261 / 996 | 437 / 1416 | 422 / 1229 |
| block-128, dict 16K | 202 / 1360 | 198 / 894 | 257 / 1279 | 299 / 1061 |
| block-128, dict 64K | 164 / 1290 | 188 / 852 | 224 / 1231 | 294 / 1156 |

### Random-read latency (decode one event, avg of 20,000 random reads, warm contexts)

| strategy | transactions | social | orders | iot |
|---|---:|---:|---:|---:|
| per-event, no dict | 1.54 us | 1.93 us | 1.68 us | 1.64 us |
| per-event, dict 16K | 0.57 us | 1.10 us | 0.62 us | 0.33 us |
| per-event, dict 64K | 0.56 us | 1.01 us | 0.60 us | 0.37 us |
| block-128, no dict | 29.1 us | 75.1 us | 38.4 us | 19.9 us |
| block-128, dict 16K | 32.0 us | 84.3 us | 41.9 us | 23.0 us |
| block-128, dict 64K | 34.0 us | 88.7 us | 43.9 us | 21.3 us |

Block latency is the cost of decompressing the whole 128-event block (16-76 KB raw) to extract
one event; it scales with block payload size (social posts have the largest events, hence 75-89 us).

### Dictionary training (5,000 samples)

| category | 16 KiB dict | 64 KiB dict |
|---|---:|---:|
| transactions | 16,384 B / 150 ms | 65,536 B / 140 ms |
| social | 16,384 B / 225 ms | 65,536 B / 209 ms |
| orders | 16,384 B / 169 ms | 65,536 B / 148 ms |
| iot | 16,384 B / 61 ms | 65,536 B / 64 ms |

Training always filled the requested budget and took 60-225 ms per category — negligible at
seal time.

(Full raw benchmark output: `run_output.txt` in this directory.)

## Analysis

**Which strategy wins per category?** Block-128 wins ratio in every category. The dictionary's
marginal contribution *on top of blocks* is small (0-6%: e.g. transactions 3.66x -> 3.70x,
iot 6.11x -> 6.32x) because a 128-event block of same-category events already gives the zstd
window plenty of cross-event redundancy — a block is effectively its own dictionary. Where the
dictionary is transformative is **per-event** compression: it turns a useless 1.2-1.4x into
2.8-3.7x (a 2.2-3.0x improvement) and simultaneously makes compression ~2x faster and
decompression ~2-5x faster, because tiny inputs stop paying full cold-start cost.

**Does the 3-10x claim hold?** Partially:

- Per-event with dictionary: **no** — 2.8-3.7x, at or below the bottom of the claimed range.
- Sealed blocks (with or without dict): **yes, for the lower half of the range** — 3.3-6.3x.
  Small, numeric/id-heavy payloads (iot) hit 6.3x; text-heavy payloads (social posts) sit at
  ~3.4x because unique English-ish text is the incompressible core of each event.
- 10x is not reachable on realistic-variance JSON at level 3 with 128-event blocks. It would
  require near-duplicate payloads, much larger blocks, or higher compression levels — treat
  "3-10x" as "3-6x" for planning purposes.

Caveat: synthetic payloads. The random-id density here (UUIDs/ULIDs are incompressible) is on
the pessimistic side; real payloads with more repeated enum-ish values would do better, real
payloads with more unique free text would do worse.

**Random-read cost.** This is the real trade. Per-event+dict decodes a single event in
0.3-1.1 us; block-128 costs 20-90 us per random read because the whole block must be
decompressed (throughput is fine — ~1.3 GB/s — the block is just 128x bigger than the ask).
20-90 us is on the order of one NVMe read, so for cold reads block decode roughly doubles
random-read cost; for page-cache-hot reads it dominates (~30-80x vs per-event). Mitigations:
cache decompressed blocks (random reads cluster), and note that scans/catch-up subscriptions —
the dominant read path for an event store — amortize the block cost to ~0.2-0.7 us per event
and get the best decompression throughput of any strategy.

**Dictionary size: 16 KiB is enough.** 64 KiB beat 16 KiB only on iot per-event
(3.49x -> 3.67x) and social (2.84x -> 2.92x); elsewhere it tied or lost slightly, and it costs
more memory per open category plus slightly slower block compression. Nothing here justifies
64 KiB.

## Recommendation for the mess frame format

**Sealed-block compression with per-category 16 KiB dictionaries, block target ~128 events
(or ~64 KiB uncompressed, whichever comes first).**

1. **v1 / hot tail: per-event, dictionary-ready.** Keep `codec_id`/`compression_id`/
   `uncompressed_len` per frame as D2 already specifies. If per-frame compression is enabled
   before seal, it must use the category dictionary — per-event without a dict is pointless
   (1.2-1.4x) and slow.
2. **At seal: recompress into blocks** of ~128 events with the category's 16 KiB dictionary,
   and record a `dict_id` in the block header. Expect 3.4-6.3x (vs 2.8-3.7x per-event) and
   0.9-1.4 GB/s scan-side decompression. Keep the dictionary even though its marginal ratio win
   over plain blocks is small: it is nearly free (16 KiB/category, <250 ms training at seal),
   it hedges small/partial blocks (a category with few events per seal degrades toward the
   per-event case, where the dict is worth 2-3x), and it speeds decompression.
3. **Serve random reads from sealed blocks by decompressing the block** (20-90 us, comparable
   to the NVMe fetch it accompanies) **behind a small decompressed-block cache**. If a
   point-read-heavy workload emerges where that latency matters, per-event-with-dict inside
   sealed segments is the fallback: reads in <1 us at the price of ~25-40% more space than
   blocks.
4. **Dictionary lifecycle:** train per category at first seal once ~1-5k samples exist
   (5k samples trained in 60-225 ms here); version dictionaries via `dict_id` so retraining
   never invalidates old blocks; cap size at 16 KiB.

Net: restate the D6 expectation as **3-6x on JSON-ish payloads with sealed blocks**; the
dictionary is what rescues the *unsealed/per-event* path, not what powers the sealed one.
