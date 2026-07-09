//! Rebuild + differential invariant tests (bn-25d acceptance): the active
//! index recovered from crashed segment files (F6) equals the one built
//! incrementally by the live committer, and a kill -9 with two unsealed
//! segments rebuilds both with exact heads.
//!
//! All segment I/O goes through the sim [`Fs`] (the same fault fs the log's own
//! adversarial suite uses). These build/read real segment files, so they are
//! `#[cfg_attr(miri, ignore)]` per the bone's real-fs/thread-test rule.

use std::collections::BTreeMap;
use std::path::PathBuf;

use mess_index::{ActiveIndex, BatchEntry, EventPtr, rebuild};
use mess_log::encode::Subframe;
use mess_log::runtime::{Fs, FileHandle, OpenOpts, Runtime, SimRuntime};
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter};

/// A batch in a test plan: `n` events for `stream_id` starting at
/// `first_version`.
#[derive(Clone, Copy)]
struct PlannedBatch {
    stream_id: u64,
    first_version: u64,
    n: usize,
}

fn pb(stream_id: u64, first_version: u64, n: usize) -> PlannedBatch {
    PlannedBatch { stream_id, first_version, n }
}

/// Write one **unsealed** segment file (create + append + close; no seal), and
/// return the [`BatchEntry`]s a correct index would hold for it. `base_pos` is
/// the segment's A1 seed; the returned `next_pos` continues the chain.
fn write_unsealed_segment<F: Fs>(
    fs: &F,
    path: &std::path::Path,
    segment_id: u64,
    base_pos: u64,
    epoch: u64,
    prev_epoch: u64,
    batches: &[PlannedBatch],
) -> (Vec<BatchEntry>, u64) {
    let params = SegmentParams {
        prev_segment_epoch: prev_epoch,
        ..SegmentParams::new(segment_id, base_pos, epoch, prev_epoch)
    };
    let mut writer = SegmentWriter::create(fs, path, params).expect("create segment");
    let mut entries = Vec::new();
    for b in batches {
        // Distinct payloads; content is irrelevant to pointer indexing.
        let payloads: Vec<Vec<u8>> =
            (0..b.n).map(|i| vec![(b.stream_id as u8) ^ (i as u8) ^ 0x5A; 16]).collect();
        let subframes: Vec<Subframe> =
            payloads.iter().map(|p| Subframe::plain(1, 1, 0, p)).collect();
        let spec = BatchSpec {
            stream_id: b.stream_id,
            category_id: 0,
            first_stream_version: b.first_version,
            crypto_chain: None,
            subframes: &subframes,
        };
        let receipt = writer.append(&spec).expect("append batch");
        entries.push(BatchEntry {
            stream_id: b.stream_id,
            first_stream_version: b.first_version,
            frame_count: receipt.frame_count,
            first_global_pos: receipt.first_global_pos,
            ptr: EventPtr { segment_id, offset: receipt.offset },
        });
    }
    let summary = writer.close().expect("close (unsealed)");
    (entries, summary.end_pos)
}

/// The incrementally-built reference: apply each segment's batches with that
/// segment's durable end as the watermark, exactly as the committer would.
fn incremental_index(segments: &[(Vec<BatchEntry>, u64)]) -> ActiveIndex {
    let idx = ActiveIndex::new();
    for (entries, next_pos) in segments {
        idx.apply_committed(*next_pos, entries);
    }
    idx
}

/// Ground-truth per-stream head version (last version) from a flat batch list.
fn ground_truth_heads(batches: &[BatchEntry]) -> BTreeMap<u64, u64> {
    let mut heads = BTreeMap::new();
    for b in batches {
        let last = b.first_stream_version + u64::from(b.frame_count) - 1;
        let e = heads.entry(b.stream_id).or_insert(last);
        *e = (*e).max(last);
    }
    heads
}

#[test]
#[cfg_attr(miri, ignore)]
fn rebuild_of_single_unsealed_segment_equals_incremental() {
    let rt = SimRuntime::new(1);
    let fs = rt.fs();
    let p: PathBuf = PathBuf::from("seg-0001.log");
    let plan = [pb(10, 0, 3), pb(20, 0, 1), pb(10, 3, 2), pb(30, 0, 4), pb(20, 1, 2)];
    let seg = write_unsealed_segment(&fs, &p, 1, 0, 1, 0, &plan);

    let reference = incremental_index(std::slice::from_ref(&seg));
    let (rebuilt, report) = rebuild(&fs, &[p]).expect("rebuild");

    assert_eq!(report.unsealed_rebuilt, vec![1]);
    assert!(report.sealed_skipped.is_empty());
    assert_eq!(rebuilt.snapshot(), reference.snapshot());
}

/// Acceptance: kill -9 with two unsealed segments → both rebuilt, heads exact.
#[test]
#[cfg_attr(miri, ignore)]
fn kill9_two_unsealed_segments_both_rebuilt_heads_exact() {
    let rt = SimRuntime::new(7);
    let fs = rt.fs();
    let p1 = PathBuf::from("seg-0001.log");
    let p2 = PathBuf::from("seg-0002.log");

    // Segment 1: streams 10, 20, 30 interleaved.
    let plan1 = [pb(10, 0, 4), pb(20, 0, 2), pb(30, 0, 1), pb(10, 4, 3)];
    let seg1 = write_unsealed_segment(&fs, &p1, 1, 0, 1, 0, &plan1);
    let base2 = seg1.1;

    // Segment 2: continues stream 10, adds stream 40 — a rolled-but-unsealed
    // segment (F6), normal under async sealing.
    let plan2 = [pb(10, 7, 2), pb(40, 0, 5), pb(20, 2, 1)];
    let seg2 = write_unsealed_segment(&fs, &p2, 2, base2, 2, 1, &plan2);

    let (rebuilt, report) = rebuild(&fs, &[p1, p2]).expect("rebuild");
    assert_eq!(report.unsealed_rebuilt, vec![1, 2], "both segments rebuilt");
    assert!(report.sealed_skipped.is_empty());

    // Heads exact against ground truth over BOTH segments.
    let mut all = seg1.0.clone();
    all.extend(seg2.0.clone());
    let heads = ground_truth_heads(&all);
    for (&stream, &head) in &heads {
        assert_eq!(rebuilt.stream_head(stream), Some(head), "stream {stream} head");
    }
    // Stream 10 spans both segments: head is the last version in segment 2.
    assert_eq!(rebuilt.stream_head(10), Some(8)); // versions 0..=6 then 7,8
    // A resolve for a cross-segment version points into the right segment.
    assert_eq!(rebuilt.resolve(10, 8).map(|p| p.segment_id), Some(2));
    assert_eq!(rebuilt.resolve(10, 6).map(|p| p.segment_id), Some(1));

    // Full differential: rebuilt == incrementally built.
    let reference = incremental_index(&[seg1, seg2]);
    assert_eq!(rebuilt.snapshot(), reference.snapshot());
}

/// A torn tail (crash mid-append) drops exactly the un-committed batch: rebuild
/// equals the index built from the *accepted* prefix only — the D1 rule that a
/// batch with a broken CRC was never committed.
#[test]
#[cfg_attr(miri, ignore)]
fn torn_tail_rebuild_equals_accepted_prefix() {
    let rt = SimRuntime::new(99);
    let fs = rt.fs();
    let p = PathBuf::from("seg-0001.log");
    let plan = [pb(10, 0, 2), pb(20, 0, 3), pb(10, 2, 4)];
    let seg = write_unsealed_segment(&fs, &p, 1, 0, 1, 0, &plan);

    // Corrupt one byte inside the LAST batch's header (within CRC coverage) and
    // make it durable — the scanner rejects it (BadCrc), so it is not
    // committed. Everything before it is untouched.
    let last = *seg.0.last().unwrap();
    let corrupt_off = last.ptr.offset + 40; // inside the 72-byte BatchHeader
    {
        let f = fs.open(&p, OpenOpts::create_rw()).unwrap();
        let mut byte = [0u8; 1];
        f.pread(corrupt_off, &mut byte).unwrap();
        byte[0] ^= 0xFF;
        f.pwrite(corrupt_off, &byte).unwrap();
        f.fdatasync().unwrap();
    }

    let (rebuilt, report) = rebuild(&fs, &[p]).expect("rebuild");
    assert_eq!(report.unsealed_rebuilt, vec![1]);

    // Reference: only the accepted prefix (all but the torn last batch).
    let accepted: Vec<BatchEntry> = seg.0[..seg.0.len() - 1].to_vec();
    let accepted_next = accepted.last().map(|b| b.end_pos()).unwrap_or(0);
    let reference = ActiveIndex::new();
    reference.apply_committed(accepted_next, &accepted);

    assert_eq!(rebuilt.snapshot(), reference.snapshot());
    // Stream 10's second batch (versions 2..=5) is gone; head is back at 1.
    assert_eq!(rebuilt.stream_head(10), Some(1));
    assert_eq!(rebuilt.resolve(10, 5), None);
}

/// A sealed segment is skipped (its packed index blocks are bn-20e — the
/// documented seam); an unsealed successor is still rebuilt, and the position
/// chain is threaded across the sealed one.
#[test]
#[cfg_attr(miri, ignore)]
fn sealed_segment_skipped_unsealed_successor_rebuilt() {
    let rt = SimRuntime::new(3);
    let fs = rt.fs();
    let p1 = PathBuf::from("seg-0001.log");
    let p2 = PathBuf::from("seg-0002.log");

    // Segment 1: SEALED (write a footer trailer).
    let params1 = SegmentParams::new(1, 0, 1, 0);
    let mut w1 = SegmentWriter::create(&fs, &p1, params1).unwrap();
    for b in [pb(10, 0, 3), pb(20, 0, 2)] {
        let payloads: Vec<Vec<u8>> = (0..b.n).map(|_| vec![1u8; 8]).collect();
        let subs: Vec<Subframe> = payloads.iter().map(|p| Subframe::plain(1, 1, 0, p)).collect();
        w1.append(&BatchSpec {
            stream_id: b.stream_id,
            category_id: 0,
            first_stream_version: b.first_version,
            crypto_chain: None,
            subframes: &subs,
        })
        .unwrap();
    }
    let summary1 = w1.seal().expect("seal segment 1");

    // Segment 2: unsealed, continues stream 10.
    let plan2 = [pb(10, 3, 2), pb(30, 0, 1)];
    let seg2 = write_unsealed_segment(&fs, &p2, 2, summary1.end_pos, 2, 1, &plan2);

    let (rebuilt, report) = rebuild(&fs, &[p1, p2]).expect("rebuild");
    assert_eq!(report.sealed_skipped, vec![1], "segment 1 sealed → skipped");
    assert_eq!(report.unsealed_rebuilt, vec![2]);
    assert_eq!(report.next_pos, seg2.1);

    // Only the unsealed segment's events are in the active index (seam).
    assert_eq!(rebuilt.stream_head(30), Some(0));
    // Stream 10's version 3 lives in the unsealed segment → resolvable.
    assert_eq!(rebuilt.resolve(10, 3).map(|p| p.segment_id), Some(2));
    // Stream 20 exists ONLY in the sealed segment → not in the active index
    // (bn-20e resolves it via the sealed index blocks).
    assert_eq!(rebuilt.stream_head(20), None);
    // Segment 2's first batch begins where the sealed segment ended (A1 chain).
    assert!(rebuilt.resolve(10, 3).is_some());
    assert_eq!(seg2.0[0].first_global_pos, summary1.end_pos);
}
