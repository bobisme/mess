//! bn-303 — Z1 dedupe-absorption check: a resurfaced unacked batch must be
//! absorbed as an A6 duplicate, not double-counted or double-delivered.
//!
//! `docs/spec/02-recovery.md` §3.1 (Z1) and
//! `docs/spec/formal-model-commit-recovery.md`'s finding Z1 establish a
//! spec-**legal** sequence, found by walking the abstract model
//! (`crate::model::tests::a10_dead_bait_can_legally_resurface_after_rewrite`):
//!
//!   1. Two batches are written back-to-back, un-fsynced: `b0` (the segment's
//!      next slot) and `b1` (the "bait", one slot further on).
//!   2. Crash 1 persists `b1` in full but loses `b0` entirely (a legal
//!      sector-reordering outcome — un-fsynced sectors survive independently
//!      and in arbitrary order). Recovery scans, hits the `b0` hole, and stops
//!      dead (A10): `b1`'s bytes sit on disk but are NOT part of the committed
//!      prefix — dead space.
//!   3. The writer resumes at the recovered `safe_offset` and rewrites the hole
//!      with a same-shaped batch (a client's idempotent retry of the same
//!      logical append), and it is fsynced — genuinely acked this time.
//!   4. Crash 2 loses nothing new (the rewrite was already durable).
//!   5. A second recovery now finds the rewrite AND `b1` both byte-valid,
//!      position-contiguous, and current-epoch: `b1` **resurfaces** and is
//!      legally accepted (A6 — an unacked-but-complete batch MAY surface).
//!
//! This file drives that exact sequence through the REAL production types —
//! [`SegmentWriter`]/[`SegmentWriter::resume`] for the writes, [`SimFs`]'s
//! sector-reorder crash medium (the same mechanism
//! `a10_sector_reorder_hole_must_not_resync` in `recovery_scanner.rs` uses)
//! for the two crashes, and [`recover_segment`] for both recoveries — not
//! the abstract model. It then checks the three things the bone asks for:
//!
//! 1. **No double-count.** [`Recovery::stream_heads`] — the scanner's own
//!    per-stream head fold, computed the same way a Book/index rehydration
//!    would (§2.3 `scan_batches`) — reflects exactly the two accepted batches'
//!    contribution: neither under- (the bait silently dropped) nor over-counted
//!    (the bait counted twice, or double-added on top of an unrelated batch).
//! 2. **Exactly-once delivery.** [`ReadView::read_committed`] (the real
//!    reader/subscription-history seam, `docs/spec/06-subscriptions.md` §3)
//!    over the final image serves each accepted position, and each `(stream_id,
//!    stream_version)` pair, exactly once.
//! 3. **The boundary condition.**
//!    [`z1_bait_stays_dead_when_rewrite_shape_differs`] is the negative
//!    control: mess-log itself has no time/capacity-bounded dedupe window (that
//!    lives in `mess-index`'s meta store, downstream of mess-log and out of
//!    this bone's scope — see the worker report). The "window" A6/Z1 actually
//!    turn on at this layer is purely the position/epoch realignment condition
//!    (Z1's own wording: "dead space is dead ... only until the positions line
//!    up again"). This test proves the resurfacing is conditional on exactly
//!    that: a rewrite of a DIFFERENT shape never re-aligns the bait's position,
//!    and the bait correctly stays dead forever.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use mess_log::encode::Subframe;
use mess_log::format::SEGMENT_HEADER_LEN;
use mess_log::reader::ReadView;
use mess_log::runtime::{CrashPlan, Fault, SectorPlan, SimFs};
use mess_log::scanner::{
    ScanStop, recover_segment, recover_segment_with_image,
};
use mess_log::watermark::Watermark;
use mess_log::writer::{BatchSpec, ResumeParams, SegmentParams, SegmentWriter};

const SECTOR: usize = 512;
const STREAM: u64 = 7;
const CATEGORY: u64 = 100 + STREAM;

/// Write `b0` (destined to be lost) and `b1` (the bait, destined to survive)
/// as two un-fsynced, sector-disjoint batches, then crash such that only
/// `b1`'s sectors persist. Returns the fs/path plus both batches' receipts.
fn build_hole_and_bait(
    b0_payload: &[u8],
    bait_payload: &[u8],
) -> (SimFs, &'static Path, mess_log::writer::Receipt, mess_log::writer::Receipt)
{
    let fs = SimFs::new(Fault::Sector { sector_size: SECTOR });
    let path = Path::new("seg");
    let mut w =
        SegmentWriter::create(&fs, path, SegmentParams::new(1, 0, 1, 0))
            .unwrap();

    let r0 = w
        .append(&BatchSpec {
            stream_id:            STREAM,
            category_id:          CATEGORY,
            first_stream_version: 0,
            crypto_chain:         None,
            subframes:            &[Subframe::plain(0x11, 0, 0, b0_payload)],
        })
        .unwrap();
    let r1 = w
        .append(&BatchSpec {
            stream_id:            STREAM,
            category_id:          CATEGORY,
            first_stream_version: 1,
            crypto_chain:         None,
            subframes:            &[Subframe::plain(0x11, 0, 0, bait_payload)],
        })
        .unwrap();

    // Persist every sector spanning b1's byte range (including the sector it
    // shares with b0's tail, if any — b0 still fails to byte-validate, since
    // its magic/header lives in the EARLIER sectors that stay unpersisted);
    // b0's own sectors are left pending and revert to background (all-zero,
    // never durable) — the model's `PartFate::Keep` for b0, `PartFate::Persist`
    // for b1.
    let b1_start = r1.offset as usize;
    let b1_end = (r1.offset + r1.total_len) as usize;
    let first = b1_start / SECTOR;
    let last = (b1_end - 1) / SECTOR;
    let persist: Vec<usize> = (first..=last).collect();
    assert!(
        (r0.offset as usize) / SECTOR < first,
        "sanity: b0's header sector must be strictly before b1's persisted \
         range"
    );
    fs.crash(path, CrashPlan::Sector(SectorPlan { persist, tear: None }))
        .unwrap();

    (fs, path, r0, r1)
}

/// Core Z1 replay through the real writer/scanner: build the two-crash
/// sequence, confirm the bait resurfaces on the second recovery, and check
/// the dedupe-absorption properties the bone asks for.
#[test]
fn z1_double_crash_resurfaces_bait_and_is_absorbed_without_double_count() {
    let b0_payload = vec![0xA0u8; 1200]; // the client's original (lost) attempt
    let bait_payload = vec![0xB1u8; 1200]; // the bait: fully persisted, never acked
    let (fs, path, r0, r1) = build_hole_and_bait(&b0_payload, &bait_payload);

    // ---- First recovery ("process life 2"): must stop dead at the hole. ----
    let rec1 = recover_segment(&fs, path).unwrap();
    assert!(
        rec1.accepted.is_empty(),
        "life-2 recovery must stop at the b0 hole"
    );
    assert_eq!(rec1.safe_offset, SEGMENT_HEADER_LEN as u64);
    assert_eq!(
        rec1.next_pos, 0,
        "nothing committed yet — stream head must not move"
    );
    assert!(
        rec1.stream_heads.is_empty(),
        "no stream head bump for a dead-on-arrival scan"
    );
    assert_ne!(
        rec1.stop,
        ScanStop::EndOfSegment,
        "the hole is a real stop, not a clean tail"
    );

    // ---- The writer resumes and rewrites the hole: a same-shaped retry of
    // ---- the same logical append (same stream, same expected version,
    // DIFFERENT bytes — proving this is a genuinely new write, not b0's
    // surviving content). It is fsynced: truly acked this time.
    let retry_payload = vec![0xC2u8; 1200];
    let mut w2 = SegmentWriter::resume(
        &fs,
        path,
        ResumeParams {
            segment_id:    1,
            base_pos:      0,
            epoch:         1,
            segment_size:  mess_log::format::SEGMENT_SIZE,
            write_off:     rec1.safe_offset,
            next_batch_id: rec1.next_batch_id,
            next_pos:      rec1.next_pos,
            batch_count:   0,
            event_count:   0,
        },
    )
    .unwrap();
    let r0b = w2
        .append(&BatchSpec {
            stream_id:            STREAM,
            category_id:          CATEGORY,
            first_stream_version: 0,
            crypto_chain:         None,
            subframes:            &[Subframe::plain(
                0x11,
                0,
                0,
                &retry_payload,
            )],
        })
        .unwrap();
    w2.sync().unwrap();

    assert_eq!(
        r0b.offset, r0.offset,
        "the rewrite lands exactly at the hole's old offset"
    );
    assert_eq!(
        r0b.total_len, r0.total_len,
        "same shape is what re-aligns the bait's position (Z1's own condition)"
    );

    // ---- Crash 2 ("process life 3"): nothing pending — already durable. ----
    fs.crash(
        path,
        CrashPlan::Sector(SectorPlan { persist: vec![], tear: None }),
    )
    .unwrap();

    // ---- Second recovery: the resurfaced-bait shape. ----
    let (rec2, image) = recover_segment_with_image(&fs, path).unwrap();
    assert_eq!(
        rec2.stop,
        ScanStop::EndOfSegment,
        "clean tail: both slots now byte-valid"
    );
    assert_eq!(rec2.accepted.len(), 2, "rewrite + resurfaced bait");

    let rewrite = &rec2.accepted[0];
    let bait = &rec2.accepted[1];
    assert_eq!(rewrite.first_global_pos, 0);
    assert_eq!(rewrite.first_stream_version, 0);
    assert_eq!(bait.first_global_pos, 1);
    assert_eq!(bait.first_stream_version, 1);
    assert_eq!(
        bait.offset, r1.offset,
        "the bait is byte-identical to the original b1 write"
    );

    // Prove it is genuinely the ORIGINAL bait content that resurfaced (not
    // some new write coincidentally matching shape), and that the rewrite
    // carries the RETRY's bytes, not b0's original (lost) content.
    let rewrite_frame = rewrite.frames(&image).unwrap().next().unwrap();
    assert_eq!(rewrite_frame.payload, retry_payload.as_slice());
    assert_ne!(rewrite_frame.payload, b0_payload.as_slice());
    let bait_frame = bait.frames(&image).unwrap().next().unwrap();
    assert_eq!(bait_frame.payload, bait_payload.as_slice());

    // ---- (1) No double-count: the scanner's own per-stream head fold ----
    // (the same accept-time fold a Book/index rehydration performs) must
    // show exactly one contiguous 2-event run for the stream — not the bait
    // silently dropped (head stuck at 0) and not double-applied.
    assert_eq!(rec2.stream_heads.len(), 1);
    assert_eq!(
        rec2.stream_heads[&STREAM], 1,
        "head == last_stream_version, events 0 and 1"
    );
    let total_events: u64 =
        rec2.accepted.iter().map(|b| u64::from(b.frame_count)).sum();
    assert_eq!(
        total_events, 2,
        "exactly two events total — no loss, no duplication"
    );

    // Every accepted batch's (stream_id, stream_version) must be unique —
    // this is the sharpest form of "not double-counted": if the bait had
    // resurfaced ON TOP OF (rather than genuinely extending) the rewrite,
    // this map would show a version with count 2.
    let mut version_counts: HashMap<(u64, u64), u32> = HashMap::new();
    for b in &rec2.accepted {
        for k in 0..u64::from(b.frame_count) {
            *version_counts
                .entry((b.stream_id, b.first_stream_version + k))
                .or_insert(0) += 1;
        }
    }
    assert_eq!(version_counts.len(), 2);
    assert!(
        version_counts.values().all(|&c| c == 1),
        "no (stream, version) counted twice"
    );

    // ---- (2) A reader sees each batch exactly once. ----
    // Drive the real reader seam: a watermark set to the fully-recovered
    // durable end, and `ReadView::read_committed` — the same primitive
    // `docs/spec/06-subscriptions.md`'s history source and a subscription's
    // catch-up phase are built on.
    let watermark = Watermark::new(rec2.next_pos);
    let view = ReadView::new(fs.clone(), path, watermark);
    let committed = view.read_committed().unwrap();
    assert_eq!(committed.len(), 2);
    assert_eq!(committed.event_count(), 2);

    let mut seen_positions: HashSet<u64> = HashSet::new();
    let mut seen_versions: HashSet<(u64, u64)> = HashSet::new();
    for b in &committed.batches {
        for k in 0..u64::from(b.frame_count) {
            assert!(
                seen_positions.insert(b.first_global_pos + k),
                "global position {} delivered more than once",
                b.first_global_pos + k
            );
            assert!(
                seen_versions.insert((b.stream_id, b.first_stream_version + k)),
                "stream version delivered more than once"
            );
        }
    }
    assert_eq!(seen_positions, HashSet::from([0, 1]));

    // Re-reading (idempotence, formal-model property 4 / §1) must reproduce
    // exactly the same delivered set — a second "subscriber" catching up
    // from scratch over the same durable image sees the identical result,
    // not a fresh duplicate.
    let committed_again = view.read_committed().unwrap();
    assert_eq!(
        committed_again, committed,
        "re-read of the same durable image is idempotent"
    );
}

/// Negative control for the "boundary" the bone asks about (point 3).
/// mess-log has no time/capacity-bounded dedupe window of its own (that
/// mechanism — `mess-index`'s meta-store dedupe FIFO — lives downstream and
/// is out of this bone's scope). What actually gates Z1 resurfacing at this
/// layer is the position/epoch realignment condition its own wording states:
/// "dead space is dead ... only until the positions line up again." This
/// test is the "outside the window" side: a rewrite of a DIFFERENT shape
/// (2 events instead of 1) never realigns the bait's position, so the bait
/// stays dead across the second recovery too — resurfacing is not automatic
/// on every rewrite, only on a position/epoch-coincident one.
#[test]
fn z1_bait_stays_dead_when_rewrite_shape_differs() {
    let b0_payload = vec![0xA0u8; 1200];
    let bait_payload = vec![0xB1u8; 1200];
    let (fs, path, r0, r1) = build_hole_and_bait(&b0_payload, &bait_payload);

    let rec1 = recover_segment(&fs, path).unwrap();
    assert!(rec1.accepted.is_empty());

    // Rewrite the hole with a DIFFERENTLY SHAPED batch: two events instead
    // of one, so the writer's next position becomes 2, not 1 — no longer
    // coincident with the bait's stamped `first_global_pos == 1`.
    let mut w2 = SegmentWriter::resume(
        &fs,
        path,
        ResumeParams {
            segment_id:    1,
            base_pos:      0,
            epoch:         1,
            segment_size:  mess_log::format::SEGMENT_SIZE,
            write_off:     rec1.safe_offset,
            next_batch_id: rec1.next_batch_id,
            next_pos:      rec1.next_pos,
            batch_count:   0,
            event_count:   0,
        },
    )
    .unwrap();
    let retry_payload_a = vec![0xC2u8; 600];
    let retry_payload_b = vec![0xC3u8; 600];
    w2.append(&BatchSpec {
        stream_id:            STREAM,
        category_id:          CATEGORY,
        first_stream_version: 0,
        crypto_chain:         None,
        subframes:            &[
            Subframe::plain(0x11, 0, 0, &retry_payload_a),
            Subframe::plain(0x11, 0, 0, &retry_payload_b),
        ],
    })
    .unwrap();
    w2.sync().unwrap();

    fs.crash(
        path,
        CrashPlan::Sector(SectorPlan { persist: vec![], tear: None }),
    )
    .unwrap();

    let rec2 = recover_segment(&fs, path).unwrap();
    // The rewrite's total_len now differs from b0's original slot size (one
    // extra subframe header), so its bytes overlap into what was b1's byte
    // range — the bait's header magic/echo/CRC no longer sits where the
    // scanner expects a batch to start. A1/A3/A4 correctly reject it: the
    // bait stays dead, and the recovered prefix is exactly the rewrite's own
    // two events — not the rewrite's two PLUS the bait's one.
    assert!(
        rec2.accepted.iter().all(|b| b.offset != r1.offset),
        "bait must not resurface when the rewrite does not realign its \
         position"
    );
    assert_eq!(
        rec2.accepted.len(),
        1,
        "only the (2-event) rewrite is accepted"
    );
    assert_eq!(rec2.accepted[0].frame_count, 2);
    assert_eq!(rec2.stream_heads.len(), 1);
    assert_eq!(
        rec2.stream_heads[&STREAM], 1,
        "head reflects the rewrite's own 2 events (0,1), not a 3rd resurfaced \
         one"
    );
    let total_events: u64 =
        rec2.accepted.iter().map(|b| u64::from(b.frame_count)).sum();
    assert_eq!(
        total_events, 2,
        "the bait must not silently add a phantom 3rd event"
    );
    let _ = r0;
}
