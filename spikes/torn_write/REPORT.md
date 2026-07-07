# Spike report: batch acceptance under block-write reordering (A4)

Spike validating `notes/mess-research/12_convergence.md` D2 acceptance rule A4: *"the
full-batch CRC is load-bearing against block-write reordering (marker persisting before
frames); magic + length echo alone is unsafe."* The predecessor spike
(`spikes/crash_log/`) tested crash-at-byte-offset (prefix truncation) failures and
explicitly punted on reordering. This spike tests it. Standalone crate, opted out of the
repo workspace. Throwaway code; the verdict below is the deliverable.

## Fault model

ALICE-style simulated block device (`SectorDisk` in `src/lib.rs`) under the same batch
format, writer protocol, and recovery scanner as crash_log (BatchHeader 38 B / subframes /
CommitMarker 16 B with total_len + CRC echoes; CRC covers the whole batch with both CRC
fields zeroed; A1 contiguity check in the scanner):

- The device is a fixed-capacity, preallocated segment image with a caller-chosen
  **background** — zeros, random garbage, or a **stale previous generation of the log**
  (recycled space; real filesystems do not zero freed blocks).
- Writes are decomposed into **sectors** (512 B and 4096 B both tested). Un-fsynced sector
  writes accumulate in a pending set; nothing is durable until fsync.
- **fsync = barrier**: all pending sectors become durable, in order. `append+fsync`
  returning == durability acknowledged (ack).
- **Crash**: an **arbitrary random subset** of the pending sector writes is applied to the
  durable image — each pending sector independently persisted or not, so a later sector
  (the marker's) can persist while an earlier one (a frame's) does not. This is write
  reordering. Optionally one applied sector is **torn**: only a byte-prefix of its new
  content persisted, the rest keeping the previous durable bytes. The resulting image goes
  to the recovery scanner.

Model limits: fsync is assumed to be a true barrier (no volatile-write-cache/FUA lies);
sector persistence is all-or-nothing except for the single modeled tear.

## (a) Full validation — production config

Acceptance = header magic/version/length sane (A2 cap) + marker magic + total_len echo +
CRC echo + full-batch CRC verifies + subframes tile exactly + A1 position contiguity.
Scan stops at the first failure; no resynchronization.

**24,000 randomized cases** (6 configs: {512, 4096} x {zeros, garbage, stale-gen} x 4,000
seeded iterations; 1-4 batches per case, 1-4 events of 0..1.5x-sector random bytes, random
acked prefix, optional mid-append byte tear of the last batch, each pending sector
persisted with p=0.5, torn sector with p=0.3), asserting on every case:

- every acked batch recovered intact, in order, at its exact offset — **0 lost**;
- every accepted batch byte-exact equals a planned batch, extras only ever the
  fully-written unacked ones in order (the A6 duplicate-side outcome) — **0 partial,
  reordered-hole, stale, or corrupt batches accepted**;
- recovery idempotent (re-scan identical) — **0 violations**.

Plus **11 deterministic adversarial cases** (all pass): marker sector persisted with zero
frame sectors; header+marker persisted with the middle frame sector missing; stale
previous-generation bytes occupying the missing sector; torn marker sector (two cut
points); torn header sector; hole followed by a fully-persisted valid batch; recycled
segment with a coincident-position stale batch (see A9); header straddling a sector
boundary with only the first sector persisted; acked batches under boundary-sector tear;
unacked-but-fully-persisted batch surfacing; empty segment.

```text
$ cargo test --release  (2026-07-07)
running 11 tests ... test result: ok. 11 passed; 0 failed
running 1 test  ... test result: ok. 1 passed; 0 failed  (matrix, 1.25 s)

sector        bg |   cases acked-ok  unacked crc-last  resync | weak-wrong wrong-case weak>full
   512     Zeros |    4000     5030      442      274     200 |         56         56        63
   512   Garbage |    4000     4961      461      265     214 |         52         50        52
   512  StaleGen |    4000     4894      447      393     194 |         57         56        57
  4096     Zeros |    4000     4975      491      253     212 |         58         58        58
  4096   Garbage |    4000     5139      506      267     216 |         58         57        57
  4096  StaleGen |    4000     4874      526      423     212 |         67         64        64
TOTAL: 24000 cases | 29873 acked batches verified intact/in-order | 2873 unacked-but-complete
surfaced (allowed, A6) | full validation: 0 partial/corrupt/stale accepted, 0 acked lost |
CRC was the last line of defense in 1875 cases | 1248 resync-bait batches past stop |
WEAK validation wrongly accepted 348 corrupt batches in 341 cases
```

## (b) Differential — weakened validation (A4 proof)

Same 24,000 crash images re-scanned with the batch CRC **disabled**: marker check reduced
to magic + total_len echo (no CRC-echo comparison, no CRC verify). Subframe tiling and the
A1 contiguity check were deliberately **kept**, so the differential isolates exactly what
the CRC buys.

- **348 corrupt batches wrongly accepted, across 341 of 24,000 cases (~1.4%)** — every
  config produced failures. Accepted "batches" contained background garbage, zeros, or
  resurrected stale-generation bytes in place of un-persisted frame sectors, under a
  pristine, fully self-consistent CommitMarker.
- The deterministic headline case (`reorder_middle_frame_sector_missing_...`): header
  sector + marker sector persisted, middle payload sector not. Full validation stops with
  `BadCrc`; weak validation **accepts** a batch whose middle 512 bytes are raw garbage.
- The danger is larger than 348 suggests: in **1,875 cases (7.8%)** the full scanner's stop
  reason was `BadCrc` — i.e. the marker was completely self-consistent (magic + length echo
  + even the CRC echo matching the header field, since header and marker sectors both
  persisted) and the whole-batch CRC verify was the **only** rejecting check. Weak
  validation escaped ~80% of those through luck: the hole happened to hit a subframe header
  and garbage failed to tile. A hole confined to payload bytes is structurally invisible —
  only the CRC sees it (see A12).

**A4 is confirmed and quantified: the full-batch CRC echo is load-bearing, not
belt-and-braces. Magic + length echo (even plus tiling and contiguity) accepts corrupt
batches at ~1.4% of crashes in this workload.**

## (c)/(d) Findings and probes

- **Marker-only persistence is harmless; header+marker is the killer.** A marker sector
  persisting with no header sector fails at header magic in any mode. The dangerous
  reordering is header + marker persisted, frames missing — reachable whenever a batch
  spans >= 3 sectors, and for 2-sector batches when the hole is payload-only.
- **Does BatchHeader need to fit in one sector? No.** Batches are not sector-aligned, so
  headers straddle boundaries constantly (deterministic test: straddled header with only
  its first 12 bytes persisted; randomized matrix hits this continuously). A torn/straddled
  header survives partially with valid magic/version but garbage `total_len`; the A2 sanity
  cap bounds the read and the marker/CRC checks reject. No alignment requirement is needed
  — but note the safety comes from A2 + A4, not from any sector geometry (A11).
- **Can a valid batch legitimately appear after a hole? Physically yes, logically never
  acceptable.** Under reordering, batch *i+1*'s sectors can all persist while batch *i*'s
  do not; the later batch is then fully intact and internally valid on disk (proved by
  scanning it in isolation). In 24,000 cases there were **1,248** such "resync bait"
  batches sitting past the scan stop point. The scan must stop permanently at the first
  invalid batch; any scanner that resynchronizes to the next magic resurrects them and
  breaks prefix consistency (A10).
- **4 KiB vs 512 B sector size changes no outcome.** All invariants hold and the
  differential fires at similar rates in both (payloads were scaled with sector size).
  Directionally, larger sectors make more small batches sector-atomic (less exposure) but
  make each hole 8x bigger; neither property is a defense. Acceptance rules must not assume
  a sector size (they don't).
- **Stale-generation background is the nastiest case.** `crc-last` is ~50% higher under
  stale-gen backgrounds (393/423 vs ~265): recycled bytes are structured and
  magic-bearing, so more corrupt candidates get all the way to the CRC check before dying.
  And weak validation there resurrects *old committed data* into new batches, which is a
  privacy/correctness failure, not just noise.

## New spec issues (continuing crash_log's A1-A8)

- **A9 — Recycled segments defeat even full validation; the segment epoch must be
  MANDATORY, not defense-in-depth.** Deterministic test
  `recycled_segment_stale_batch_at_coincident_position_passes_full_validation`: a recycled
  segment holds a stale prior-generation batch at offset 0 whose `first_global_pos`
  coincides with what a fresh scan expects (a segment reused for the same position range
  after an unclean rollback, or segment-file recycling without restamping). The new write's
  sectors all fail to persist — routine under reordering, since zero-of-N pending sectors
  is a legal crash outcome. Full validation (magic + echoes + CRC + A1 contiguity)
  **accepts the stale batch** and resurrects old data. Nothing in A1-A8 catches this. Fix:
  a segment epoch/generation stamp in `BatchHeader` checked at scan time, or never recycle
  segment files/space without zeroing or restamping the head. A1's "defense in depth"
  wording should be upgraded to a requirement wherever segments are recycled.
- **A10 — Recovery must never resynchronize past a hole; say so explicitly.** D1's recovery
  pseudocode ("scan segments forward ... accept a batch only if its terminator/length
  validates, discard the incomplete tail") does not forbid skip-ahead heuristics. Under
  reordering, fully-valid-looking batches routinely sit past the first invalid one
  (1,248 in 24,000 cases here); accepting any of them creates a committed-history hole. The
  spec should state: the scan stop is terminal, and everything at/after the stop offset is
  dead space to be truncated/overwritten. (The A1 contiguity check alone does NOT
  substitute — stop-at-first-failure is the actual invariant.)
- **A11 — No sector-alignment requirement, and none should be added implicitly.** Nothing
  in the format needs the header (or marker) to fit in one sector; correctness under
  straddling/tearing comes from the A2 `total_len` cap plus the A4 CRC. The spec should
  record this so nobody "optimizes" acceptance by trusting a header just because it looked
  sector-atomic, and so the A2 cap is understood as load-bearing input validation (it is
  what bounds the read triggered by a half-persisted header's garbage `total_len`).
- **A12 — Structural checks are luck, not protection; no substitute for the CRC.**
  Quantified: of 1,875 marker-self-consistent corrupt candidates, subframe tiling happened
  to reject ~80% (garbage where a subframe header should be), and 348 sailed through. A
  hole that lands entirely inside payload bytes is invisible to every structural check by
  construction. The spec should not permit any acceptance mode that treats
  "frames parse + marker echoes lengths" as validation — e.g. no fast-path recovery mode,
  no "CRC off for performance" knob on the scan side.

## Verdict

**D2's acceptance rules are sufficient under write reordering — if and only if the
full-batch CRC verify is treated as mandatory (A4) and one gap is closed (A9).** Across
24,000 randomized sector-reordering crashes plus 11 deterministic adversarial cases, full
validation lost zero acked batches and accepted zero partial, corrupt, reordered-hole, or
(position-distinct) stale batches, idempotently, at both 512 B and 4 KiB sector sizes. The
claim under test is proven concretely: with only the marker's magic + length echo, 348
corrupt batches were accepted across 341 crashes (~1.4%), and in 7.8% of crashes the CRC
was the only check standing between a pristine-looking marker and garbage frames. The one
scenario that defeats even full validation is the recycled-segment coincident-position
stale batch (A9) — reachable under reordering with nothing new persisting — which requires
a segment epoch in the header (or no-recycling-without-restamping) before Phase 3
segmentation lands. A10-A12 are wording obligations so nobody weakens the scanner later.

## Files

- `src/lib.rs` — format/writer/scanner (adapted from spikes/crash_log with a
  Full/Weak validation switch), `SectorDisk` fault model, deterministic adversarial tests
- `tests/matrix.rs` — 24,000-case randomized reordering matrix + differential counters
- `.cargo/config.toml` — local override: repo root config forces `-fuse-ld=lld`, not
  installed on this machine
