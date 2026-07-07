# Spike report: crash safety of the batch-framed append-only log (D1/D2)

Spike validating `notes/mess-research/12_convergence.md` sections D1 (log with commit markers
is the sole commit authority) and D2 (batch framing). Standalone crate, opted out of the repo
workspace. Throwaway code; the verdict below is the deliverable.

## What was built

- **On-disk format** (`src/lib.rs`): `BatchHeader { magic, format_version, frame_count,
  batch_id, total_len, first_global_pos, batch_crc }` (38 bytes) + `EventSubframe*
  { event_type_id, data_len, payload }` + `CommitMarker { magic, total_len echo, batch_crc
  echo }` (16 bytes). Little-endian, crc32fast. CRC covers the whole batch (header through
  marker) with both CRC fields zeroed during hashing, per D2's "hash fields zeroed" rule.
- **Writer**: `Log::append_batch(&mut self, events: &[Vec<u8>]) -> Result<CommitInfo, Crashed>`
  — encode, write, fsync; `Ok` == durability acknowledged. Writes go through a `FaultWriter`
  that models file + page cache (written vs. fsynced watermarks) and can be configured to
  crash at any absolute byte offset (keeping a torn prefix) or inside an fsync call
  ("marker written, durability never acked"). `crash()` materializes the surviving bytes:
  everything up to the fsync watermark survives; the unsynced tail survives to a random
  length and can optionally be scribbled with garbage (torn-sector simulation).
- **Recovery scanner** (`scan`): forward scan from a given offset; a batch is accepted only if
  header magic/version/length are sane, the CommitMarker validates (magic + total_len echo +
  CRC echo), the CRC verifies over the whole batch, the subframes exactly tile the batch, and
  `first_global_pos` is contiguous with the previous accepted batch. Stops at the first
  failure and reports recovered batches, the safe truncation offset, and next
  batch_id/global_pos. All-or-nothing per batch.

## Test results (`cargo test --release`, 2026-07-07)

**19 deterministic edge cases** — torn header, partial frames, frames-without-marker, torn
marker, marker CRC-echo mismatch, marker length-echo mismatch, payload corruption under an
intact marker, garbage after a valid marker (both longer and shorter than a header), valid
batch then torn second batch, stale-but-CRC-valid batch after a valid marker, version
mismatch, insane total_len, empty file, plus protocol-step crashes injected through the
FaultWriter (mid-header / mid-frames / mid-marker / after-marker-before-fsync /
after-fsync). All pass.

**1 randomized crash-loop test** — 12,000 seeded iterations, each: 1–5 batches of 1–4 events
(0–64 bytes) written to a fault-injected log; crash at a uniformly random byte offset (25% of
the time snapped to a batch boundary to hammer the fsync-loss path); un-fsynced tail randomly
dropped (70%) and/or scrambled (30%); surviving bytes round-tripped through a real tempfile;
recover; assert acked-batches-recovered-intact-in-order, no-partial-batch-visible,
re-recovery idempotence (same bytes and truncated-to-safe-offset both), and post-recovery
appends continuing with contiguous global positions. Passes in ~0.2 s.

```text
running 19 tests
test tests::crash_after_fsync ... ok
test tests::crash_after_marker_before_fsync ... ok
test tests::crash_mid_marker ... ok
test tests::crash_mid_frames ... ok
test tests::empty_file ... ok
test tests::crash_mid_header ... ok
test tests::full_frames_but_no_marker ... ok
test tests::garbage_after_valid_marker ... ok
test tests::header_but_partial_frames ... ok
test tests::insane_total_len_stops_scan ... ok
test tests::marker_crc_echo_mismatch ... ok
test tests::marker_len_echo_mismatch ... ok
test tests::payload_corruption_caught_by_crc ... ok
test tests::stale_valid_batch_after_marker_stops_scan ... ok
test tests::torn_header ... ok
test tests::torn_marker ... ok
test tests::two_valid_batches ... ok
test tests::valid_then_torn_second_batch ... ok
test tests::version_mismatch_stops_scan ... ok

test result: ok. 19 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

running 1 test
test randomized_crash_recovery_loop ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.23s
```

Stats line from the loop (`-- --nocapture`):

```text
randomized crash loop: 12000 iterations, 10505 with an injected crash,
15067 acked batches verified, 171 unacked-but-complete batches surfaced (allowed),
0 partial batches visible, 0 acked batches lost
```

Note on the 171: a batch that was fully written but whose fsync was lost may legitimately
surface after recovery. That is correct under "marker durable = ack" (D7): the client was
never acked, so recovering it is a permitted duplicate-side outcome, and it is always a
complete, prefix-consistent batch. The spec should state this explicitly (see A6).

## Ambiguities / gaps found in the spec (the valuable part)

- **A1 — Stale CRC-valid data needs a continuity check; the spec text doesn't require one.**
  D1 says "accept a batch only if its terminator/length validates." That is insufficient: a
  recycled segment (or a file truncated by a previous recovery whose old tail bytes still sit
  on disk past the logical end — real filesystems do not zero freed blocks) can contain a
  *stale but fully CRC-valid* batch immediately after the last good one. Marker validation
  alone would accept it and resurrect old data. The spike closes this with a
  `first_global_pos == expected` contiguity check (test
  `stale_valid_batch_after_marker_stops_scan` demonstrates the attack). The spec should
  either (a) mandate the position-contiguity rule during scan, or (b) add a `prev_batch`
  link / segment epoch to `BatchHeader`. Option (a) is free — the fields already exist — but
  only works while positions are dense and monotone (which D10 already asserts). A physical
  segment epoch would additionally protect scan-from-a-checkpoint in a recycled segment where
  the stale batch coincidentally has the expected position (possible if a segment is reused
  for the same position range after an unclean rollback); worth deciding explicitly.
- **A2 — `total_len` needs a sanity bound and an exact definition.** A corrupted `total_len`
  that is huge must not drive a giant read/allocation before the marker check can fail; the
  spike caps it at `MAX_BATCH_LEN` (1 MiB) and treats out-of-range as scan-stop. Also the
  spec never says what `total_len` includes; the spike defines it as the whole on-disk batch
  (header + subframes + marker), which is what makes "marker at `off + total_len - 16`"
  unambiguous. Pin both down.
- **A3 — CRC coverage of the marker itself.** D2 says "hash fields zeroed during hashing" but
  does not say whether the CommitMarker bytes are inside CRC coverage. The spike includes
  them (with the echo field zeroed). This matters: if the marker were outside coverage, a
  corrupted `total_len` echo could still pass CRC. Covering the marker also means the marker
  cannot be validated without reading the whole batch — fine, since recovery reads it anyway.
- **A4 — Write reordering, not just prefix loss.** The spike simulates prefix truncation plus
  scrambling of the unsynced tail. On real hardware, a multi-block batch can persist
  *out of order* (marker block before a middle frame block). Magic + length echo alone would
  accept such a batch; the full-batch CRC is what makes marker-first reordering safe. So the
  CRC echo is load-bearing, not belt-and-braces — the spec should say the marker is only
  trustworthy in combination with the CRC verify. The planned ALICE-style reordering harness
  (Phase 3) is the right place to test this for real; this spike's scramble mode is a weak
  proxy.
- **A5 — Empty batches.** Nothing in D2 forbids `frame_count == 0`. An empty batch advances
  `batch_id` but not `first_global_pos`, which makes the A1 contiguity check unable to
  distinguish one stale empty batch from a fresh one. The spike forbids empty batches;
  the spec should too (or give them a purpose and a sequence rule).
- **A6 — Recovery may surface unacked batches; say so.** Fully-written-but-unacked batches
  (crash between marker write and fsync ack, page cache flushed anyway) reappear after
  recovery. This is the mirror image of D7's `CursorRegressed` and interacts with the open
  dedupe-window item: a client that retries after a timeout can create a duplicate. Not a
  bug, but the spec should own it the way D7 owns cursor regression.
- **A7 — Scan-from-zero vs. checkpoint.** Scan-from-zero is O(file) and fine per segment;
  with segmentation (Phase 3) the unit of scanning is the last non-sealed segment, so no
  checkpoint machinery is needed *if* segment boundaries always coincide with batch
  boundaries — the spec should state that alignment rule. Any "last known-good offset"
  checkpoint must be advisory only (re-verified by the scan), never trusted, or it becomes a
  second commit authority and violates D1.
- **A8 — Batch larger than remaining segment space.** D2 is silent. With `MAX_BATCH_LEN` <<
  segment size the answer is "roll to a new segment; batches never span segments," but that
  needs to be written down, plus what a scanner does with trailing free space (expect zeros?
  stop at first non-magic? The spike stops at the first invalid header, which handles both).

## Verdict

**D1/D2 hold up.** The framing protocol — header with length + CRC, subframes, commit marker
echoing length and CRC, all-or-nothing recovery at the first invalid batch — survived 19
adversarial deterministic cases and 12,000 randomized crash/recovery iterations with zero
acked-batch losses and zero partial batches visible, including torn writes at every protocol
step, loss of the un-fsynced tail, and corruption of surviving unsynced bytes. Recovery is
idempotent and appends resume cleanly from the recovered state.

One real gap: as written, D1's "terminator/length validates" acceptance rule is not
sufficient — it accepts stale CRC-valid batches from recycled disk space (A1). The fix is
cheap (enforce `first_global_pos` contiguity during scan, and/or add a segment epoch), but it
must be added to the spec before Phase 3; everything else found is pin-it-down wording
(A2/A3/A5/A7/A8) or documentation of already-implied behavior (A4/A6).

## Files

- `src/lib.rs` — format, FaultWriter, writer, recovery scanner, deterministic tests
- `tests/crash_loop.rs` — randomized crash loop + idempotence + post-recovery append
- `.cargo/config.toml` — local override: repo root config forces `-fuse-ld=lld`, not
  installed on this machine
