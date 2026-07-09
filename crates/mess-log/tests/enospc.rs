//! ENOSPC (disk-full) discipline for the segment writer (bn-36y,
//! `docs/spec/03-durability.md` §2.6).
//!
//! Every case injects disk-full at one site class through the sim fs
//! [`EnospcSite`] point and asserts the writer's response is a **typed** error
//! and, crucially, that **no committed data is corrupted**: after each injected
//! failure the recovery scanner reconstructs exactly the committed prefix.
//!
//! The three site classes:
//! - `Allocate` — segment preallocation at roll: the ONE point disk-full is
//!   designed to strike. Fails the triggering append with `StoreFull`, leaves
//!   no partial segment.
//! - `Fdatasync` — the durability barrier: unknowable durable state → the store
//!   is poisoned (`StorePoisoned`), the D8 shape.
//! - `Pwrite` — a positioned batch write: adversarial (preallocation should
//!   prevent it), but even so the committed prefix survives intact.
//!
//! All cases run on the in-memory sim fs, so they stay in the Miri lane (no
//! `#[cfg_attr(miri, ignore)]`); a single real-fs case proves the
//! `FALLOC_FL_KEEP_SIZE` semantics and IS excluded from the Miri lane.

use std::path::Path;

use mess_log::encode::Subframe;
use mess_log::format::*;
use mess_log::runtime::{
    CrashPlan, EnospcSite, Fault, FileHandle, Fs, OpenOpts, RealRuntime, Runtime, SectorPlan,
    SimFs,
};
use mess_log::scanner::{recover_segment, ScanStop};
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter, WriteError};

/// Append one small single-frame batch through `w`.
fn append_one<F: Fs>(
    w: &mut SegmentWriter<F>,
    stream_id: u64,
    version: u64,
    payload: &[u8],
) -> Result<(), WriteError> {
    let sfs = [Subframe::plain(0x11, 0, 0, payload)];
    w.append(&BatchSpec {
        stream_id,
        category_id: 100 + stream_id,
        first_stream_version: version,
        crypto_chain: None,
        subframes: &sfs,
    })
    .map(|_| ())
}

/// A modest segment (big enough for the tiny batches here; small enough that
/// the sim never grows a large buffer — sim `allocate` is a no-op anyway).
fn params(segment_id: u64, base_pos: u64, epoch: u64) -> SegmentParams {
    let mut p = SegmentParams::new(segment_id, base_pos, epoch, 0);
    p.segment_size = 64 * 1024;
    p
}

// ---------------------------------------------------------------------------
// (1) fallocate-at-roll: disk-full strikes at preallocation.
// ---------------------------------------------------------------------------

/// Disk-full injected at `allocate` during `create` yields a typed `StoreFull`,
/// and the never-headered husk is removed — no partial segment left behind.
#[test]
fn allocate_enospc_at_create_yields_store_full_and_no_husk() {
    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new("/seg-create-full");
    fs.inject_enospc(path, EnospcSite::Allocate);

    let err = match SegmentWriter::create(&fs, path, params(1, 0, 1)) {
        Ok(_) => panic!("create must fail with StoreFull when preallocation hits ENOSPC"),
        Err(e) => e,
    };
    assert!(
        matches!(err, WriteError::StoreFull { requested } if requested == 64 * 1024),
        "expected StoreFull, got {err:?}"
    );
    // The husk was removed: the path does not exist (create cleaned up).
    assert!(
        fs.open(path, OpenOpts::read_only()).is_err(),
        "a failed preallocation must leave no segment file behind"
    );
}

/// A roll whose new-segment preallocation hits ENOSPC fails with `StoreFull`,
/// leaves no partial new segment, and — the load-bearing property — the OLD
/// segment's committed prefix recovers cleanly and completely.
#[test]
fn roll_enospc_yields_store_full_old_segment_recovers() {
    let fs = SimFs::new(Fault::SECTOR_512);
    let seg0 = Path::new("/seg-roll-a");
    let seg1 = Path::new("/seg-roll-b");

    let mut w = SegmentWriter::create(&fs, seg0, params(1, 0, 1)).unwrap();
    append_one(&mut w, 0, 0, b"alpha").unwrap();
    append_one(&mut w, 0, 1, b"bravo").unwrap();
    let end_pos = w.next_pos();

    // Arm ENOSPC on the NEXT segment's preallocation, then roll.
    fs.inject_enospc(seg1, EnospcSite::Allocate);
    let err = match w.roll(seg1, 2, 2, 0) {
        Ok(_) => panic!("roll must fail with StoreFull when preallocation hits ENOSPC"),
        Err(e) => e,
    };
    assert!(matches!(err, WriteError::StoreFull { .. }), "roll must surface StoreFull, got {err:?}");

    // No partial new segment: seg1 was created empty then removed.
    assert!(fs.open(seg1, OpenOpts::read_only()).is_err(), "no partial new segment");

    // The old segment stayed durable and readable: recover its committed prefix.
    let rec = recover_segment(&fs, seg0).unwrap();
    assert_eq!(rec.batch_count(), 2, "both committed batches recover");
    assert_eq!(rec.next_pos, end_pos);
    assert_eq!(rec.stop, ScanStop::EndOfSegment, "clean tail, no corruption");
}

// ---------------------------------------------------------------------------
// (2) barrier ENOSPC: the D8 poisoning path shape.
// ---------------------------------------------------------------------------

/// A barrier (`fdatasync`) ENOSPC poisons the store: `sync` fails, the writer
/// reports poisoned, every subsequent append fails fast with `StorePoisoned`,
/// and after the crash that drops the unsynced batch the committed prefix
/// (everything durable BEFORE the poisoning barrier) recovers cleanly.
#[test]
fn barrier_enospc_poisons_store_committed_prefix_recovers() {
    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new("/seg-poison");

    let mut w = SegmentWriter::create(&fs, path, params(1, 0, 1)).unwrap();
    // Batch 0 is committed (a real barrier).
    append_one(&mut w, 0, 0, b"alpha").unwrap();
    w.sync().unwrap();

    // Batch 1 is written, but its covering barrier hits ENOSPC.
    fs.inject_enospc(path, EnospcSite::Fdatasync);
    append_one(&mut w, 0, 1, b"bravo").unwrap();
    let sync_err = w.sync().unwrap_err();
    assert_eq!(sync_err.raw_os_error(), Some(libc::ENOSPC), "barrier failed with ENOSPC");
    assert!(w.is_poisoned(), "an ENOSPC barrier must poison the store");

    // Every subsequent write fails fast with the typed poison error.
    let after = append_one(&mut w, 0, 2, b"charlie").unwrap_err();
    assert!(matches!(after, WriteError::StorePoisoned), "post-poison append: {after:?}");
    let after_sync = w.sync().unwrap_err();
    assert_eq!(after_sync.kind(), std::io::ErrorKind::Other, "post-poison sync fails fast");

    drop(w);

    // Power loss: the unsynced batch 1 is dropped (persist nothing new).
    fs.crash(path, CrashPlan::Sector(SectorPlan { persist: vec![], tear: None })).unwrap();

    let rec = recover_segment(&fs, path).unwrap();
    assert_eq!(rec.batch_count(), 1, "only the pre-poison committed batch survives");
    assert_eq!(rec.next_pos, 1);
    // The unsynced batch 1 reached the page cache before the barrier failed, so
    // the crash reverts its region to durable zeros; the scanner stops at that
    // garbage the moment the committed prefix ends — the committed data is
    // uncorrupted and the resume point is exactly its end (A10 safe offset).
    assert_eq!(rec.stop, ScanStop::BadMagic, "garbage tail rejected past the committed prefix");
    let b0 = rec.accepted[0];
    assert_eq!(rec.safe_offset, b0.offset + b0.total_len, "resume at the committed prefix boundary");
}

// ---------------------------------------------------------------------------
// (3) pwrite ENOSPC: even a mid-commit write fault cannot corrupt the prefix.
// ---------------------------------------------------------------------------

/// Disk-full injected at a positioned batch write fails the append with a typed
/// I/O error and lands no partial bytes; the previously committed prefix
/// recovers exactly.
#[test]
fn pwrite_enospc_fails_append_no_corruption() {
    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new("/seg-pwrite-full");

    let mut w = SegmentWriter::create(&fs, path, params(1, 0, 1)).unwrap();
    append_one(&mut w, 0, 0, b"alpha").unwrap();
    w.sync().unwrap();

    // The next batch's pwrite hits ENOSPC → no bytes land.
    fs.inject_enospc(path, EnospcSite::Pwrite);
    let err = append_one(&mut w, 0, 1, b"bravo").unwrap_err();
    match err {
        WriteError::Io(e) => assert_eq!(e.raw_os_error(), Some(libc::ENOSPC)),
        other => panic!("expected an ENOSPC Io error, got {other:?}"),
    }

    drop(w);
    fs.crash(path, CrashPlan::Sector(SectorPlan { persist: vec![], tear: None })).unwrap();

    let rec = recover_segment(&fs, path).unwrap();
    assert_eq!(rec.batch_count(), 1, "committed prefix intact");
    assert_eq!(rec.next_pos, 1);
    assert_eq!(rec.stop, ScanStop::EndOfSegment);
}

// ---------------------------------------------------------------------------
// (4) real fs: preallocation reserves blocks WITHOUT extending st_size.
// ---------------------------------------------------------------------------

/// On the real runtime, `create` preallocates the full segment via
/// `fallocate(FALLOC_FL_KEEP_SIZE)`: blocks are reserved but the logical length
/// stays at the written content (header only, here), so `len()` and recovery
/// are unaffected. bn-25j: real fs is excluded from the Miri lane (Miri blocks
/// `open`).
#[test]
#[cfg_attr(miri, ignore)]
fn real_preallocation_keeps_size() {
    let dir = std::env::temp_dir();
    let path = dir.join(format!("mess-log-enospc-keepsize-{}.seg", std::process::id()));
    struct Rm(std::path::PathBuf);
    impl Drop for Rm {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let _rm = Rm(path.clone());

    let rt = RealRuntime::new();
    let mut p = SegmentParams::new(1, 0, 1, 0);
    p.segment_size = 8 * 1024 * 1024; // 8 MiB reservation
    let mut w = SegmentWriter::create(&rt.fs(), &path, p).unwrap();
    append_one(&mut w, 0, 0, b"alpha").unwrap();
    let sum = w.close().unwrap();

    // Reservation did NOT extend the logical length: len() == written content,
    // not 8 MiB. If create had used posix_fallocate (extends size) this would
    // be 8 MiB and the assertion would fail.
    let f = rt.fs().open(&path, OpenOpts::read_only()).unwrap();
    assert_eq!(f.len().unwrap(), sum.content_len, "KEEP_SIZE: st_size tracks content, not the reservation");
    assert_eq!(sum.content_len, SEGMENT_HEADER_LEN as u64 + append_len(b"alpha"));
}

/// On-disk length of a single-frame batch carrying `payload` (header + one
/// subframe + marker), computed via the encoder so the assertion is exact.
fn append_len(payload: &[u8]) -> u64 {
    use mess_log::encode::{BatchEncoder, BatchInput};
    let sfs = [Subframe::plain(0x11, 0, 0, payload)];
    BatchEncoder::total_len(&BatchInput {
        segment_epoch: 1,
        batch_id: 0,
        first_global_pos: 0,
        stream_id: 0,
        category_id: 100,
        first_stream_version: 0,
        crypto_chain: None,
        subframes: &sfs,
    })
    .unwrap()
}
