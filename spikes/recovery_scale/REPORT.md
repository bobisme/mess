# recovery_scale — recovery time at real scale, segment-size validation

Round-1 crash testing (crash_log, torn_write) proved the batch format safe on small files.
This spike produces the recovery-**time** numbers at real sizes that the doc-03 segment-size
guidance (64 MiB / 256 MiB / 1 GiB candidates) was waiting on.

Format: the D2 batch-framed segment format reused from `spikes/vertical_slice/src/seglog.rs`
(format v2, 54-byte BatchHeader, 16-byte CommitMarker, A1 contiguity check, A2 length cap,
A5 no-empty-batches), with two spike additions:

- a parameterized batch checksum (crc32fast IEEE / hardware CRC32C / BLAKE3-truncated),
  same A3/A4 coverage rule (whole batch, both checksum fields zeroed while hashing);
- a 40-byte checksummed **SegmentFooter** written when a segment seals, so the A7/F5
  "trust sealed segments, scan only the active one" fast path has something to trust.
  Footers are advisory: FULL recovery still scans and CRC-validates every batch and
  cross-checks the footer counts (D1 — the log is the only authority).

Workload: ~250 B payloads, batches of 10 (2,650 B/batch on disk), 10,000 streams,
10 GiB per segment-size configuration (smaller totals measured as prefixes of the same
files), generated/measured/deleted per configuration — peak disk ≈ 11 GiB, well under
the 25 GiB budget. Everything single-threaded.

Run: `cargo run --release` (~7 minutes; `RS_QUICK=1` for a small smoke run).
All generated data under `bench_data/` was deleted by the run itself; nothing to clean up.

Hardware: Ryzen 9 3900X, Samsung 970 EVO Plus 2 TB NVMe, ext4, 64 GiB RAM,
Linux 7.0.12-arch1-1, rustc 1.96.1.

## Cold/warm methodology

`echo 3 > drop_caches` needs root, so cold cache is approximated with
`posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED)` over every segment file immediately
before the cold run. All segment files are `fdatasync`'d at generation/seal time, so
their pages are clean and DONTNEED actually evicts them. Warm runs re-read the same
files immediately afterwards (best of 2–3 reported). The consistent cold/warm split
(~950 vs ~1,330 MiB/s) confirms eviction works; the gap is modest because the scan is
CPU-bound, not disk-bound (see spec issue 1).

Sanity checks (assertions, all green): recovered batch/event counts equal
generation-side per-segment ground truth for every configuration; footer counts equal
scan counts on every sealed segment; re-recovery is idempotent; the fast path agrees
with full recovery.

## Timing matrix

FULL = read + CRC-validate every batch of every segment, rebuilding an in-memory index
skeleton (per-stream head version + last EventPtr, HashMap). LAST-SEG = A7/F5 fast
path: one 40-byte footer pread per sealed segment, full scan of one **completely full**
segment as the worst-case active segment. Checksum: hardware CRC32C.

"Total" is nominal; "scanned" is the actual byte count (prefixes round **up** to a
segment boundary, which is why e.g. the 1 GiB @ 1 GiB-segment row scanned 2 segments).

| total | seg size | mode | scanned | cold | cold MiB/s | warm | warm MiB/s |
|---|---|---|---|---|---|---|---|
| 1 GiB | 64 MiB | FULL | 1.06 GiB | 1.07 s | 1013 | 0.82 s | 1331 |
| 4 GiB | 64 MiB | FULL | 4.06 GiB | 4.15 s | 1003 | 3.11 s | 1338 |
| 10 GiB | 64 MiB | FULL | 10.0 GiB | 11.02 s | 929 | 7.58 s | 1351 |
| 1 GiB | 256 MiB | FULL | 1.25 GiB | 1.36 s | 943 | 0.94 s | 1358 |
| 4 GiB | 256 MiB | FULL | 4.25 GiB | 4.57 s | 952 | 3.39 s | 1285 |
| 10 GiB | 256 MiB | FULL | 10.0 GiB | 11.04 s | 927 | 7.66 s | 1336 |
| 1 GiB | 1 GiB | FULL | 2.0 GiB | 2.14 s | 957 | 1.58 s | 1300 |
| 4 GiB | 1 GiB | FULL | 5.0 GiB | 5.45 s | 940 | 3.95 s | 1295 |
| 10 GiB | 1 GiB | FULL | 10.0 GiB | 11.20 s | 915 | 7.90 s | 1296 |
| 1 GiB | 64 MiB | LAST-SEG | 64 MiB + 16 footers | 65 ms | 991 | 48 ms | 1327 |
| 4 GiB | 64 MiB | LAST-SEG | 64 MiB + 64 footers | 93 ms | 686 | 50 ms | 1268 |
| 10 GiB | 64 MiB | LAST-SEG | 64 MiB + 159 footers | 102 ms | 628 | 48 ms | 1328 |
| 1 GiB | 256 MiB | LAST-SEG | 256 MiB + 4 footers | 290 ms | 884 | 197 ms | 1302 |
| 4 GiB | 256 MiB | LAST-SEG | 256 MiB + 16 footers | 291 ms | 880 | 197 ms | 1300 |
| 10 GiB | 256 MiB | LAST-SEG | 256 MiB + 39 footers | 322 ms | 796 | 204 ms | 1253 |
| 1 GiB | 1 GiB | LAST-SEG | 1 GiB + 1 footer | 1.09 s | 942 | 0.82 s | 1252 |
| 4 GiB | 1 GiB | LAST-SEG | 1 GiB + 4 footers | 1.14 s | 900 | 0.79 s | 1298 |
| 10 GiB | 1 GiB | LAST-SEG | 1 GiB + 9 footers | 1.16 s | 884 | 0.80 s | 1283 |

Per 10 GiB: 4,051,856 batches / 40,518,560 events; 161 / 41 / 11 segment files at
64 MiB / 256 MiB / 1 GiB. Generation ran at 450–580 MiB/s (buffered, CRC at encode).

Readings:

- **FULL recovery is a property of total log size only.** ~1.1 s/GiB cold,
  ~0.76 s/GiB warm, flat across segment sizes (spread < 8%). Segment size buys you
  *nothing* on the full-rebuild path; it only matters through the fast path,
  retention granularity, and file counts.
- **LAST-SEG recovery is a property of active-segment size only** — and it is the
  number that gates startup after a normal crash: 64 MiB → ~0.1 s, 256 MiB → ~0.3 s,
  1 GiB → ~1.2 s cold, in each case nearly flat in total log size.
- Footer preads cost ~0.25 ms each cold (visible in the 64 MiB column: 65 → 102 ms
  as sealed count goes 16 → 159). Extrapolated, the fast path's O(#segments) preads
  dominate at large logs — see spec issue 2.

## CRC32C vs BLAKE3 (mandatory crypto chain cost)

FULL recovery of 1 GiB at 256 MiB segments, checksum used both at write and scan time
(BLAKE3 truncated to the 4-byte field — this measures compute cost, not a proposed format):

| checksum | cold | cold MiB/s | warm | warm MiB/s |
|---|---|---|---|---|
| crc32-ieee (crc32fast) | 1.03 s | 995 | 0.78 s | 1308 |
| crc32c (hw) | 0.98 s | 1043 | 0.76 s | 1356 |
| blake3 (truncated) | 2.08 s | 493 | 1.74 s | 590 |

- crc32fast vs crc32c: within 4% — either is fine; both are effectively free relative
  to the rest of the scan.
- **BLAKE3 costs 2.1× cold / 2.3× warm** end-to-end recovery time on these 2.6 KiB
  batches (short-input regime, single-threaded). A *mandatory* per-batch crypto hash
  would turn the 10 GiB full rebuild from ~11 s into ~23 s and the 256 MiB active-segment
  scan from ~0.3 s into ~0.7 s. D2's "crypto chain is opt-in per stream/category" is
  empirically the right call; verifying the chain can also be deferred/backgrounded
  since CRC alone already gates acceptance (A12 stays mandatory regardless).

## SIGKILL realism

A child process (`recovery_scale child <dir> <seg_bytes>`) appends batches to 64 MiB
segments with each batch as two `write()` calls (header+frames, then marker — seglog's
`split_writes`), `fdatasync` every 64 batches, then durably records its acknowledged
counters in a status file (write-tmp + rename). The parent SIGKILLs it at random points
and recovers. Note SIGKILL cannot tear a single in-flight `write()` (the syscall
completes in the kernel), so the child widens the between-writes window with a 3 ms
sleep every 24 batches; without that, a random kill lands between batches essentially
every time.

| kill after | written | durable (acked) batches | recovered batches | tail stop | tail cut | FULL rec | LAST-SEG rec |
|---|---|---|---|---|---|---|---|
| 400 ms | 4.2 MiB | 1,664 | 1,680 | IncompleteBatch | 2,634 B | 3.5 ms | 0.6 ms |
| 900 ms | 9.6 MiB | 3,776 | 3,816 | IncompleteBatch | 2,634 B | 10.9 ms | 3.0 ms |
| 1500 ms | 15.9 MiB | 6,208 | 6,272 | EndOfLog | 0 | 15.6 ms | 7.3 ms |
| 2200 ms | 24.4 MiB | 9,664 | 9,672 | IncompleteBatch | 2,634 B | 23.6 ms | 10.1 ms |
| 3000 ms | 34.6 MiB | 13,632 | 13,696 | EndOfLog | 0 | 33.0 ms | 25.8 ms |

- Torn tails observed and handled: 2,634 bytes = exactly one marker-less batch
  (header + frames, no 16-byte marker), rejected as `IncompleteBatch`, truncated at
  `safe_offset`; post-truncation re-recovery is byte-identical and ends `EndOfLog`
  (asserted every run).
- The durably-acknowledged prefix survived every kill (recovered ≥ acked, asserted).
  Recovered > acked is expected under SIGKILL: unsynced page-cache writes survive
  *process* death; only OS/power failure loses them, and that regime is covered by
  torn_write's sector-reordering simulation, not by SIGKILL.
- Recovery of a partially filled 64 MiB active segment is single-digit-to-low-tens of
  ms — consistent with the matrix (recovery cost tracks actual tail bytes).

## Concrete guidance

**Recommended segment size: 256 MiB** (doc-03's middle candidate, and what
vertical_slice already uses).

- Measured worst case at 256 MiB: **~0.32 s cold (~0.20 s warm) last-segment recovery
  on a 10 GiB log** — comfortably inside an interactive startup budget, and flat as the
  log grows (modulo footer preads, issue 2).
- 64 MiB buys 3× faster crash recovery (~0.1 s) that nothing currently needs, at the
  cost of 4× the files/footers/manifest entries (161 files per 10 GiB; 16k per TiB —
  the fast path's footer preads alone would reach seconds at TiB scale) and 4× more
  frequent seal/roll work.
- 1 GiB crosses the one-second line (1.09–1.16 s cold) for every crash recovery, has
  the coarsest retention-delete and worst-case-corruption granularity, and provides
  zero benefit on the full-rebuild path in exchange.

**Do the doc-03 candidates survive contact?** Yes, all three are viable *given the
sealed-footer fast path exists*: even 1 GiB segments recover in ~1.2 s. But the
implicit doc-03 assumption that segment size matters for "quick recovery scan" is
**only** true of the fast path — full recovery (index lost/distrusted) is ~1.1 s/GiB
cold **regardless of segment size**, so segment sizing cannot rescue full-rebuild time.

### New spec issues

1. **Recovery scan is CPU-bound, single-core, at ~1.3 GiB/s warm / ~0.95 GiB/s cold**
   (NVMe sequential read is ~3× faster than the scan consumes it). A 100 GiB log means
   ~2 minutes of full rebuild. Segments are independently scannable — each segment's
   A1 seed (`base_pos`) is available from its predecessor's footer/name — so the spec
   should permit parallel per-segment scan + stitch (with A9 epoch checks) before
   full-rebuild time matters in production.
2. **The A7/F5 fast path is O(#sealed segments) footer preads** (~0.25 ms each cold):
   ~4 s for a 1 TiB log at 64 MiB segments, ~1 s at 256 MiB — eventually dominating the
   active-segment scan. The manifest (already planned, advisory-only per A7) should
   cache sealed-segment footers so startup is O(active segment + manifest read), with
   footers remaining the fallback authority-of-convenience and the log the real one.
3. **The segment footer must be spec'd** (doc 03 lists one; seglog v2 has none): it
   needs its own checksum (done here), the A9 segment epoch, and it does not remove
   F5's index low-watermark requirement — footers prove log contents, not index
   durability.
4. **Keep the crypto chain opt-in (D2 confirmed with numbers):** mandatory BLAKE3
   ≈ 2.2× recovery wall time on realistic 2.6 KiB batches. If a stream opts in,
   consider verifying the chain lazily/in the background; CRC + marker + A1 already
   gate acceptance.
5. **CRC choice is a non-issue for recovery:** crc32c (hw) ≈ crc32fast (SIMD) within
   4%. Pick CRC32C for its ecosystem pedigree; A12 (no CRC-off fast path) costs
   nothing measurable relative to disk.
6. **seglog.rs scanner nit:** it copies every batch into a temp `Vec` to zero the
   checksum fields before hashing. At spike scale that was invisible; at 10 GiB it is
   ~10 GiB of extra alloc+memcpy. Hash the three spans around the zeroed fields
   instead (done in this spike's scanner — `batch_sum` in `src/main.rs`).
7. **Measurement footnote:** segments hold slightly *less* than their nominal size
   (roll happens when the next batch would overflow), so "N GiB total" prefixes round
   up by one segment; throughput numbers are computed from actual bytes scanned.
