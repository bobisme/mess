//! bn-2di (review F4) — an ordered unit fails **atomically**.
//!
//! [`Appender::submit_ordered`] exists so `mess-store` can put a `$registry`
//! registration into the log *ahead of* the batch that first references the ids
//! it mints. File order alone is not enough for that: if the registration can
//! be REJECTED while the domain batch behind it is ACCEPTED, the log ends up
//! holding events whose `stream_id`/`event_type_id` no `*Registered` record
//! covers — a store that never opens again, reached from an ordinary `ENOSPC`.
//!
//! So a non-`Acked` outcome on batch *i* of a unit must abort every LATER batch
//! of that unit: never encoded, never written, no global position consumed,
//! failed with [`AppendError::UnitAborted`]. These tests force exactly that,
//! two ways — a `SegmentFull` the roller cannot rescue, and an injected
//! disk-full at the roll's preallocation (`StoreFull`) — and prove from the
//! **durable bytes** (the recovery scanner, not the acks) that the second batch
//! is not in the log.

use std::path::Path;

use mess_log::committer::{
    AppendError, AppendOutcome, AppendRequest, Committer, Durability,
    EventInput, Roller,
};
use mess_log::runtime::{EnospcSite, Runtime, SimFs, SimRuntime};
use mess_log::scanner::recover_segment;
use mess_log::writer::{SegmentParams, SegmentWriter};

/// One batch of `n` events of `payload_len` bytes each on `stream_id`.
fn req(
    stream_id: u64,
    first_stream_version: u64,
    n: usize,
    payload_len: usize,
) -> AppendRequest {
    AppendRequest {
        stream_id,
        category_id: 7,
        first_stream_version,
        events: (0..n)
            .map(|i| EventInput::plain(1, 0, 0, vec![i as u8; payload_len]))
            .collect(),
    }
}

fn params(
    segment_id: u64,
    base_pos: u64,
    epoch: u64,
    size: u64,
) -> SegmentParams {
    let mut p = SegmentParams::new(segment_id, base_pos, epoch, 0);
    p.segment_size = size;
    p
}

/// The unit's FIRST batch cannot fit an empty segment (`SegmentFull`, bn-u6o:
/// no amount of rolling helps, so it is surfaced rather than retried). The
/// SECOND batch is tiny and would fit easily — and must nevertheless never be
/// written, because it was ordered *behind* a batch that failed.
///
/// Without the unit rule this is precisely the `$registry` corruption: the
/// registration is rejected, the domain batch lands, and the log now references
/// ids that were never registered.
#[test]
fn first_batch_segment_full_aborts_the_rest_of_the_unit() {
    let rt = SimRuntime::new(3);
    let fs = rt.fs();
    let path = Path::new("/seg-unit-full");
    // A 16 KiB segment: the 32 KiB first batch can never fit, the 64-byte
    // second one always could.
    let writer =
        SegmentWriter::create(&fs, path, params(1, 0, 1, 16 * 1024)).unwrap();

    let outcomes = rt.block_on(async {
        let c = Committer::spawn(&rt, writer, Durability::Os);
        let out = c
            .appender()
            .submit_ordered(vec![
                req(0, 0, 1, 32 * 1024), // bigger than the whole segment
                req(9, 0, 1, 64),        // trivially small
            ])
            .expect("the unit pre-flights clean; it fails at the WRITE")
            .wait()
            .await;
        c.shutdown().await;
        out
    });

    assert!(
        matches!(outcomes[0], Err(AppendError::SegmentFull { .. })),
        "batch 0 must fail SegmentFull, got {:?}",
        outcomes[0]
    );
    assert!(
        matches!(outcomes[1], Err(AppendError::UnitAborted)),
        "batch 1 must be aborted with the unit, got {:?}",
        outcomes[1]
    );

    // The durable proof: the log holds NOTHING. The second batch was not
    // written, and it consumed no global position.
    let rec = recover_segment(&fs, path).unwrap();
    assert_eq!(rec.batch_count(), 0, "an aborted unit writes no bytes");
    assert_eq!(rec.next_pos, 0, "an aborted unit consumes no position");
}

/// Same rule, reached through the disk-full path the reviewer traced: the
/// first batch does not fit the *remaining* space, the roller tries to open a
/// fresh segment, preallocation hits `ENOSPC` → `StoreFull`. The second batch
/// of the unit is small enough that it WOULD still fit the current segment's
/// tail — the exact shape that makes this reachable without any bug in the
/// caller — and must still never be written.
#[test]
fn first_batch_store_full_aborts_the_rest_of_the_unit() {
    let rt = SimRuntime::new(5);
    let fs: SimFs = rt.fs();
    let seg1 = Path::new("/seg-unit-a");
    let seg2 = Path::new("/seg-unit-b");
    let writer =
        SegmentWriter::create(&fs, seg1, params(1, 0, 1, 4096)).unwrap();

    let (tx, _rx) = std::sync::mpsc::channel();
    let roller = Roller::new(
        |id| {
            if id == 2 {
                seg2.to_path_buf()
            } else {
                Path::new("/seg-unit-other").to_path_buf()
            }
        },
        tx,
    );

    // Disk-full at the NEXT segment's preallocation: the roll that a
    // `SegmentFull` triggers cannot succeed.
    fs.inject_enospc(seg2, EnospcSite::Allocate);

    let outcomes = rt.block_on(async {
        let c = Committer::spawn_with_roll(&rt, writer, Durability::Os, roller);
        // Fill most of the 4 KiB segment first, so the unit's first batch
        // overflows it (and the tiny second batch would still fit the tail).
        let filled = c.append(req(1, 0, 1, 3000)).await.unwrap();
        assert!(matches!(filled, AppendOutcome::Acked { .. }));

        let out = c
            .appender()
            .submit_ordered(vec![
                req(0, 0, 1, 1500), // overflows the tail → roll → ENOSPC
                req(9, 0, 1, 16),   // would still fit the tail
            ])
            .expect("pre-flight is clean")
            .wait()
            .await;
        c.shutdown().await;
        out
    });

    assert!(
        matches!(outcomes[0], Err(AppendError::StoreFull)),
        "batch 0 must fail StoreFull, got {:?}",
        outcomes[0]
    );
    assert!(
        matches!(outcomes[1], Err(AppendError::UnitAborted)),
        "batch 1 must be aborted with the unit, got {:?}",
        outcomes[1]
    );

    // The durable proof: only the pre-fill batch is in the log. Batch 1 fit,
    // but was never offered to the writer.
    let rec = recover_segment(&fs, seg1).unwrap();
    assert_eq!(rec.batch_count(), 1, "only the pre-fill batch is durable");
    assert_eq!(rec.accepted[0].stream_id, 1);
    assert_eq!(rec.next_pos, 1, "the aborted unit consumed no position");
}

/// The happy path is unchanged: a unit that commits gets dense, ordered
/// positions — batch 0 strictly before batch 1, in ONE group.
#[test]
fn a_committing_unit_keeps_file_order_and_dense_positions() {
    let rt = SimRuntime::new(7);
    let fs = rt.fs();
    let path = Path::new("/seg-unit-ok");
    let writer =
        SegmentWriter::create(&fs, path, params(1, 0, 1, 64 * 1024)).unwrap();

    let outcomes = rt.block_on(async {
        let c = Committer::spawn(&rt, writer, Durability::group_default());
        let out = c
            .appender()
            .submit_ordered(vec![req(0, 0, 2, 32), req(9, 0, 3, 32)])
            .expect("pre-flight clean")
            .wait()
            .await;
        c.shutdown().await;
        out
    });

    let AppendOutcome::Acked { first_position: a0, last_position: a1, .. } =
        outcomes[0].as_ref().expect("batch 0 acked")
    else {
        panic!("batch 0 must be Acked: {:?}", outcomes[0]);
    };
    let AppendOutcome::Acked { first_position: b0, .. } =
        outcomes[1].as_ref().expect("batch 1 acked")
    else {
        panic!("batch 1 must be Acked: {:?}", outcomes[1]);
    };
    assert_eq!((*a0, *a1), (0, 1), "the registration-shaped batch goes first");
    assert_eq!(*b0, 2, "the dependent batch follows it densely");

    let rec = recover_segment(&fs, path).unwrap();
    assert_eq!(rec.batch_count(), 2);
    assert_eq!(rec.accepted[0].stream_id, 0, "unit order == file order");
    assert_eq!(rec.accepted[1].stream_id, 9);
}

/// A one-batch unit is not a unit: nothing can be aborted, and the ordinary
/// error surfaces unchanged (no `UnitAborted` leaks into single-batch callers).
#[test]
fn a_single_batch_unit_reports_its_own_error() {
    let rt = SimRuntime::new(11);
    let fs = rt.fs();
    let path = Path::new("/seg-unit-one");
    let writer =
        SegmentWriter::create(&fs, path, params(1, 0, 1, 16 * 1024)).unwrap();

    let outcomes = rt.block_on(async {
        let c = Committer::spawn(&rt, writer, Durability::Os);
        let out = c
            .appender()
            .submit_ordered(vec![req(0, 0, 1, 32 * 1024)])
            .expect("pre-flight clean")
            .wait()
            .await;
        c.shutdown().await;
        out
    });
    assert!(
        matches!(outcomes[0], Err(AppendError::SegmentFull { .. })),
        "got {:?}",
        outcomes[0]
    );
}
