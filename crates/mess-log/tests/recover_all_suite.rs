//! Conformance suite for whole-log recovery (bn-2en), spec 02 §8.
//!
//! Coverage:
//! - §8.4 full-recovery / fast-recovery equivalence on the same corpus,
//!   including torn tails, as a seeded property sweep;
//! - R2 advisory-manifest harmlessness: identical committed prefix with the
//!   manifest present, absent, corrupt, or stale;
//! - §8.1 cross-segment stitch: A1 position-gap and A9 epoch-chain breaks are
//!   typed [`StitchError`]s; a torn active tail stops recovery cleanly (A10);
//! - R1 parallelism: parallel per-segment recovery == serial (on the real
//!   runtime; the sim always runs serial for determinism).
//!
//! Corpora are built with the real [`SegmentWriter`]/sealer through the [`Fs`]
//! seam, so the bytes are current spec-v3.

use std::path::{Path, PathBuf};

use mess_log::encode::Subframe;
use mess_log::format::*;
use mess_log::manifest::{self, Manifest};
use mess_log::recover_all::{
    RecoverOptions, RecoveryMode, RecoverySource, SegmentFile, StitchError,
    manifest_entries, recover_whole_log,
};
use mess_log::runtime::{Fault, FileHandle, Fs, OpenOpts, SimFs};
use mess_log::sealer::SegmentCatalogEntry;
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter};

// ---------------------------------------------------------------------------
// A tiny seeded RNG (xorshift) so the property sweep is deterministic without
// pulling in a dev-dependency.
// ---------------------------------------------------------------------------
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self { Rng(seed | 1) }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 { self.next_u64() % n }
}

// ---------------------------------------------------------------------------
// Corpus builder — a chain of sealed segments + an active tail, over any Fs.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum ActiveTail {
    /// A clean unsealed segment (writer closed, no footer).
    Clean,
    /// An unsealed segment with a mid-tail batch corrupted (a torn tail).
    Torn,
    /// A header-only active segment (no batches).
    Empty,
    /// No active tail: the last segment is sealed too.
    AllSealed,
}

struct Corpus {
    fs:   SimFs,
    segs: Vec<SegmentFile>,
}

fn seg_path(i: usize) -> PathBuf { PathBuf::from(format!("log/seg-{i:04}")) }

/// Append `n` batches (1..=3 events each, driven by `rng`) to `w`, returning
/// each batch's byte offset (for targeted corruption).
fn append_batches(
    w: &mut SegmentWriter<SimFs>,
    n: usize,
    rng: &mut Rng,
) -> Vec<u64> {
    let mut offsets = Vec::new();
    let payload = [0xABu8; 16];
    for k in 0..n {
        let events = 1 + (rng.below(3) as usize);
        let subs: Vec<Subframe> = (0..events)
            .map(|_| Subframe::plain(0x11, 0, 0, &payload))
            .collect();
        let r = w
            .append(&BatchSpec {
                stream_id:            1,
                category_id:          101,
                first_stream_version: (k * 4) as u64,
                crypto_chain:         None,
                subframes:            &subs,
            })
            .unwrap();
        offsets.push(r.offset);
    }
    offsets
}

/// Flip one durable byte at absolute `off` in `path` through the Fs.
fn flip<F: Fs>(fs: &F, path: &Path, off: u64) {
    let f = fs.open(path, OpenOpts::create_rw()).unwrap();
    let mut b = [0u8; 1];
    f.pread(off, &mut b).unwrap();
    b[0] ^= 0xFF;
    f.pwrite(off, &b).unwrap();
    f.fdatasync().unwrap();
}

/// Build a log: `n_sealed` sealed segments followed by an active tail of the
/// given kind. Epochs increase by 1 per segment starting at 10; segment ids
/// start at 1. Batch counts are drawn from `rng`.
fn build(n_sealed: usize, active: ActiveTail, rng: &mut Rng) -> Corpus {
    let fs = SimFs::new(Fault::SECTOR_512);
    let mut segs = Vec::new();
    let epoch0 = 10u64;

    // First segment.
    let mut w = SegmentWriter::create(
        &fs,
        &seg_path(0),
        SegmentParams::new(1, 0, epoch0, 0),
    )
    .unwrap();
    segs.push(SegmentFile::new(1, seg_path(0)));
    append_batches(&mut w, 1 + rng.below(4) as usize, rng);

    // Roll+seal to make `n_sealed` sealed segments; the first is sealed by the
    // first roll.
    for i in 1..=n_sealed {
        let next_id = (i + 1) as u64;
        let next_epoch = epoch0 + i as u64;
        w = w.roll_sealed(&seg_path(i), next_id, next_epoch, 0).unwrap();
        segs.push(SegmentFile::new(next_id, seg_path(i)));
        // The just-opened segment gets batches unless it will be the active
        // tail we shape below (it is the last one).
        if i < n_sealed || active != ActiveTail::Empty {
            append_batches(&mut w, 1 + rng.below(4) as usize, rng);
        }
    }
    // At this point `w` is the last-opened segment. If we asked for n_sealed
    // sealed segments and an active tail, the last-opened is the active tail.

    let last_path = seg_path(n_sealed);
    match active {
        ActiveTail::AllSealed => {
            w.seal().unwrap();
        }
        ActiveTail::Clean => {
            w.close().unwrap();
        }
        ActiveTail::Empty => {
            // No batches were appended to the last segment (guard above); just
            // close it unsealed.
            w.close().unwrap();
        }
        ActiveTail::Torn => {
            let offs = {
                // Ensure the active tail has at least 3 batches so a mid-tail
                // corruption leaves valid batches beyond the hole (A10 bait).
                append_batches(&mut w, 2, rng)
            };
            w.sync().unwrap();
            // Corrupt an earlier batch's header CRC so the scan stops before
            // the tail; the batches after it become dead space
            // (A10).
            let target = offs[0] + HEADER_CRC_OFF as u64;
            w.close().unwrap();
            flip(&fs, &last_path, target);
        }
    }

    Corpus { fs, segs }
}

// ---------------------------------------------------------------------------
// §8.4 equivalence
// ---------------------------------------------------------------------------

fn assert_full_fast_equiv(c: &Corpus) {
    let full = recover_whole_log(&c.fs, &c.segs, None, RecoverOptions::full())
        .unwrap();
    let fast = recover_whole_log(&c.fs, &c.segs, None, RecoverOptions::fast())
        .unwrap();
    assert!(
        full.prefix_eq(&fast),
        "§8.4 full/fast disagree:\n full={full:#?}\n fast={fast:#?}"
    );
}

#[test]
fn full_fast_equivalence_over_shapes() {
    let mut rng = Rng::new(0xF00D);
    for &active in &[
        ActiveTail::Clean,
        ActiveTail::Torn,
        ActiveTail::Empty,
        ActiveTail::AllSealed,
    ] {
        for n_sealed in 0..=4 {
            let c = build(n_sealed, active, &mut rng);
            assert_full_fast_equiv(&c);
        }
    }
}

#[test]
fn full_fast_equivalence_property_sweep() {
    // Seeded DST-style sweep (§8.4 conformance): many random corpora incl.
    // torn tails, full == fast every time.
    let mut rng = Rng::new(0x2E_u64.wrapping_mul(0x9E37_79B9));
    for _ in 0..200 {
        let n_sealed = rng.below(5) as usize;
        let active = match rng.below(4) {
            0 => ActiveTail::Clean,
            1 => ActiveTail::Torn,
            2 => ActiveTail::Empty,
            _ => ActiveTail::AllSealed,
        };
        let c = build(n_sealed, active, &mut rng);
        assert_full_fast_equiv(&c);
    }
}

// ---------------------------------------------------------------------------
// R2 manifest harmlessness: present / absent / corrupt / stale
// ---------------------------------------------------------------------------

#[test]
fn manifest_present_absent_corrupt_equivalent() {
    let mut rng = Rng::new(0xBEEF);
    for &active in &[ActiveTail::Clean, ActiveTail::Torn, ActiveTail::AllSealed]
    {
        let c = build(3, active, &mut rng);

        // Baseline: no manifest.
        let base =
            recover_whole_log(&c.fs, &c.segs, None, RecoverOptions::fast())
                .unwrap();
        let full =
            recover_whole_log(&c.fs, &c.segs, None, RecoverOptions::full())
                .unwrap();
        assert!(base.prefix_eq(&full));

        // Build the manifest from the recovered sealed catalog and persist it.
        let entries = manifest_entries(&base);
        let mpath = Path::new("log/manifest");
        manifest::write_manifest(&c.fs, mpath, &entries).unwrap();
        let m = manifest::read_manifest(&c.fs, mpath)
            .unwrap()
            .expect("just wrote it");

        // Present: identical committed prefix (and it actually saved trailer
        // preads by trusting the coherent cache).
        let with =
            recover_whole_log(&c.fs, &c.segs, Some(&m), RecoverOptions::fast())
                .unwrap();
        assert!(with.prefix_eq(&base), "present manifest changed the result");

        // Corrupt the manifest *file*: flip a byte inside its CRC coverage.
        flip(&c.fs, mpath, manifest::MANIFEST_HEADER_LEN as u64);
        let corrupt = manifest::read_manifest(&c.fs, mpath).unwrap();
        assert!(
            corrupt.is_none(),
            "a corrupt manifest decodes to None (treated absent)"
        );
        // Recovery with the (now absent) manifest still equals the baseline.
        let after = recover_whole_log(
            &c.fs,
            &c.segs,
            corrupt.as_ref(),
            RecoverOptions::fast(),
        )
        .unwrap();
        assert!(after.prefix_eq(&base));
    }
}

#[test]
fn stale_manifest_entry_is_rejected_and_falls_back() {
    let mut rng = Rng::new(0x5747);
    let c = build(3, ActiveTail::Clean, &mut rng);
    let base = recover_whole_log(&c.fs, &c.segs, None, RecoverOptions::fast())
        .unwrap();
    let full = recover_whole_log(&c.fs, &c.segs, None, RecoverOptions::full())
        .unwrap();

    // Craft a STALE manifest: a plausible, internally-consistent entry for
    // segment 2 whose epoch/base_pos no longer match the on-disk header (as if
    // the segment had been recycled since the manifest was written). The
    // header cross-check MUST reject it, so recovery falls back to the trailer
    // and still produces the authoritative prefix.
    let stale = Manifest::new(vec![SegmentCatalogEntry {
        segment_id:  2,
        epoch:       999,  // wrong generation
        base_pos:    7777, // wrong position
        end_pos:     7777 + 3,
        batch_count: 99,
        event_count: 3,
        ext_offset:  SEGMENT_HEADER_LEN as u64,
        ext_len:     0,
        ext_crc:     0,
    }]);

    let recovered =
        recover_whole_log(&c.fs, &c.segs, Some(&stale), RecoverOptions::fast())
            .unwrap();
    assert!(
        recovered.prefix_eq(&base) && recovered.prefix_eq(&full),
        "a stale manifest entry must be rejected and fall back to the trailer"
    );
    // And the trusted count must be the real one, not the stale 99.
    let seg2 = recovered.segments.iter().find(|s| s.segment_id == 2).unwrap();
    assert_ne!(seg2.batch_count, 99);
}

#[test]
fn manifest_roundtrips_through_fs() {
    let mut rng = Rng::new(0x1234);
    let c = build(2, ActiveTail::AllSealed, &mut rng);
    let base = recover_whole_log(&c.fs, &c.segs, None, RecoverOptions::fast())
        .unwrap();
    let entries = manifest_entries(&base);
    assert!(!entries.is_empty(), "an all-sealed log has cacheable segments");
    let mpath = Path::new("log/manifest");
    manifest::write_manifest(&c.fs, mpath, &entries).unwrap();
    let m = manifest::read_manifest(&c.fs, mpath).unwrap().unwrap();
    assert_eq!(m.entries(), entries.as_slice());
}

#[test]
fn missing_manifest_is_none() {
    let fs = SimFs::new(Fault::SECTOR_512);
    assert!(
        manifest::read_manifest(&fs, Path::new("nope/absent"))
            .unwrap()
            .is_none()
    );
}

// ---------------------------------------------------------------------------
// §8.1 cross-segment stitch
// ---------------------------------------------------------------------------

/// Build two internally-valid segments with a chosen (id, base_pos, epoch) for
/// the second, so the stitch can be exercised directly.
fn two_segments(seg2_base_pos: u64, seg2_epoch: u64) -> Corpus {
    let fs = SimFs::new(Fault::SECTOR_512);
    let p = [0xAAu8; 8];
    let subs = [Subframe::plain(0x11, 0, 0, &p)];

    // Segment 1: id 1, base_pos 0, epoch 10, one event ⇒ end_pos 1.
    let mut w1 = SegmentWriter::create(
        &fs,
        &seg_path(0),
        SegmentParams::new(1, 0, 10, 0),
    )
    .unwrap();
    w1.append(&BatchSpec {
        stream_id:            1,
        category_id:          101,
        first_stream_version: 0,
        crypto_chain:         None,
        subframes:            &subs,
    })
    .unwrap();
    w1.seal().unwrap();

    // Segment 2: built directly with the caller's base_pos / epoch.
    let mut w2 = SegmentWriter::create(
        &fs,
        &seg_path(1),
        SegmentParams::new(2, seg2_base_pos, seg2_epoch, 10),
    )
    .unwrap();
    w2.append(&BatchSpec {
        stream_id:            1,
        category_id:          101,
        first_stream_version: 0,
        crypto_chain:         None,
        subframes:            &subs,
    })
    .unwrap();
    w2.close().unwrap();

    Corpus {
        fs,
        segs: vec![
            SegmentFile::new(1, seg_path(0)),
            SegmentFile::new(2, seg_path(1)),
        ],
    }
}

#[test]
fn stitch_position_gap_fails_recovery() {
    // Segment 1 ends at pos 1; segment 2 claims base_pos 5 ⇒ A1 break.
    let c = two_segments(5, 11);
    for opts in [RecoverOptions::full(), RecoverOptions::fast()] {
        let err = recover_whole_log(&c.fs, &c.segs, None, opts).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("does not continue"),
            "expected PositionGap, got {err:?}"
        );
    }
}

#[test]
fn stitch_epoch_regression_fails_recovery() {
    // Segment 2 continues the position (base_pos 1) but its epoch (9) does not
    // exceed segment 1's (10) ⇒ A9 chain break.
    let c = two_segments(1, 9);
    for opts in [RecoverOptions::full(), RecoverOptions::fast()] {
        let err = recover_whole_log(&c.fs, &c.segs, None, opts).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("does not exceed"),
            "expected EpochChainBroken, got {err:?}"
        );
    }
}

#[test]
fn stitch_error_variants_are_typed() {
    let gap = StitchError::PositionGap {
        at_segment_id:     2,
        expected_base_pos: 1,
        found_base_pos:    5,
    };
    let ep = StitchError::EpochChainBroken {
        at_segment_id: 2,
        prev_epoch:    10,
        found_epoch:   9,
    };
    assert_ne!(gap, ep);
}

#[test]
fn torn_active_tail_recovers_prefix_and_marks_active() {
    let mut rng = Rng::new(0x707);
    let c = build(2, ActiveTail::Torn, &mut rng);
    let full = recover_whole_log(&c.fs, &c.segs, None, RecoverOptions::full())
        .unwrap();
    let fast = recover_whole_log(&c.fs, &c.segs, None, RecoverOptions::fast())
        .unwrap();
    assert!(full.prefix_eq(&fast));
    // The last (torn) segment is the active one; it is the last live segment.
    assert_eq!(
        full.active_segment_id,
        Some(full.segments.last().unwrap().segment_id)
    );
    assert_eq!(full.segments.last().unwrap().source, RecoverySource::Scan);
}

#[test]
fn corrupt_interior_footer_keeps_later_segments() {
    // A media corruption of an already-sealed *interior* segment's footer CRC
    // must NOT drop the valid segments that follow it: the body scans clean and
    // ends at the (still-magic) footer, so the segment is complete and its
    // successor is stitched normally. This is the completeness distinction —
    // "seal present, footer corrupt" is complete, not a torn tail (§8.3
    // decision: an invalid trailer is treated as unsealed and fully scanned).
    let mut rng = Rng::new(0xC0FFEE);
    let c = build(3, ActiveTail::AllSealed, &mut rng);
    let clean_full =
        recover_whole_log(&c.fs, &c.segs, None, RecoverOptions::full())
            .unwrap();

    // Corrupt segment 1's footer_crc (last 4 bytes of its file are the
    // footer_crc at trailer offset 96; the trailer magic stays intact).
    let seg1 = &c.segs[0].path;
    let len = c.fs.open(seg1, OpenOpts::read_only()).unwrap().len().unwrap();
    flip(&c.fs, seg1, len - 4);

    let full = recover_whole_log(&c.fs, &c.segs, None, RecoverOptions::full())
        .unwrap();
    let fast = recover_whole_log(&c.fs, &c.segs, None, RecoverOptions::fast())
        .unwrap();
    assert!(
        full.prefix_eq(&fast),
        "full/fast still agree with a corrupt interior footer"
    );
    assert_eq!(
        full.total_batches, clean_full.total_batches,
        "a corrupt footer costs no committed batch — the body is rescanned, \
         successors kept"
    );
    assert_eq!(
        full.segments.len(),
        clean_full.segments.len(),
        "no segment dropped"
    );
}

#[test]
fn empty_log_recovers_to_zero_state() {
    let fs = SimFs::new(Fault::SECTOR_512);
    let whole =
        recover_whole_log(&fs, &[], None, RecoverOptions::full()).unwrap();
    assert_eq!(whole.segments.len(), 0);
    assert_eq!(whole.next_pos, 0);
    assert_eq!(whole.next_epoch, 0);
    assert_eq!(whole.active_segment_id, None);
    assert_eq!(whole.total_batches, 0);
}

#[test]
fn all_sealed_log_has_no_active_segment() {
    let mut rng = Rng::new(0x9999);
    let c = build(3, ActiveTail::AllSealed, &mut rng);
    let fast = recover_whole_log(&c.fs, &c.segs, None, RecoverOptions::fast())
        .unwrap();
    assert_eq!(
        fast.active_segment_id, None,
        "every segment sealed ⇒ next append opens a new one"
    );
    assert_eq!(fast.next_batch_id, 0);
    assert!(fast.segments.iter().all(|s| s.source == RecoverySource::Footer));
}

// ---------------------------------------------------------------------------
// R1 parallelism — on the real runtime (real threads + real fs).
// ---------------------------------------------------------------------------

mod real_parallel {
    use mess_log::runtime::Runtime;
    use mess_log::runtime::real::RealFs;

    use super::*;

    /// Build a chain of `n_sealed` sealed segments + a clean unsealed tail on
    /// the real fs under `dir`, returning the ordered segment files.
    fn build_real(dir: &Path, n_sealed: usize) -> Vec<SegmentFile> {
        let fs = RealFs;
        let p = |i: usize| dir.join(format!("seg-{i:04}"));
        let payload = [0xCDu8; 24];
        let subs = [
            Subframe::plain(0x11, 0, 0, &payload),
            Subframe::plain(0x11, 0, 0, &payload),
        ];
        let mut segs = Vec::new();

        let mut w =
            SegmentWriter::create(&fs, &p(0), SegmentParams::new(1, 0, 10, 0))
                .unwrap();
        segs.push(SegmentFile::new(1, p(0)));
        let spec = |v: u64| BatchSpec {
            stream_id:            1,
            category_id:          101,
            first_stream_version: v,
            crypto_chain:         None,
            subframes:            &subs,
        };
        w.append(&spec(0)).unwrap();
        w.append(&spec(2)).unwrap();
        for i in 1..=n_sealed {
            let id = (i + 1) as u64;
            w = w.roll_sealed(&p(i), id, 10 + i as u64, 0).unwrap();
            segs.push(SegmentFile::new(id, p(i)));
            w.append(&spec(0)).unwrap();
            w.append(&spec(2)).unwrap();
        }
        w.close().unwrap();
        segs
    }

    // bn-2en: real fs + real threads; Miri isolation blocks `open`, and the
    // parallel path spawns OS threads — excluded from the Miri lane.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn parallel_equals_serial_full_and_fast() {
        let dir = mess_testkit::sweeping_temp_dir("recover-all-parallel");
        let fs = RealFs;
        let segs = build_real(dir.path(), 5);

        for mode in [RecoveryMode::Full, RecoveryMode::Fast] {
            let serial = recover_whole_log(
                &fs,
                &segs,
                None,
                RecoverOptions { mode, parallel: false },
            )
            .unwrap();
            let parallel = recover_whole_log(
                &fs,
                &segs,
                None,
                RecoverOptions { mode, parallel: true },
            )
            .unwrap();
            assert_eq!(
                serial, parallel,
                "R1: parallel recovery must equal serial ({mode:?})"
            );
        }

        // And full == fast on the real corpus too (§8.4).
        let full = recover_whole_log(
            &fs,
            &segs,
            None,
            RecoverOptions::full().parallel(),
        )
        .unwrap();
        let fast = recover_whole_log(
            &fs,
            &segs,
            None,
            RecoverOptions::fast().parallel(),
        )
        .unwrap();
        assert!(full.prefix_eq(&fast));

        // The runtime is available for spawning too (sanity that RealRuntime is
        // the parallel host); not used directly here since the orchestrator
        // owns its own thread::scope fan-out.
        let _ = mess_log::runtime::RealRuntime::new().fs();
    }
}

#[test]
fn unsorted_input_is_stitched_in_id_order() {
    let mut rng = Rng::new(0x4242);
    let c = build(3, ActiveTail::Clean, &mut rng);
    let ordered =
        recover_whole_log(&c.fs, &c.segs, None, RecoverOptions::full())
            .unwrap();
    // Reverse the input; the result must be identical (stitched by id).
    let mut rev = c.segs.clone();
    rev.reverse();
    let shuffled =
        recover_whole_log(&c.fs, &rev, None, RecoverOptions::full()).unwrap();
    assert!(ordered.prefix_eq(&shuffled));
    assert_eq!(ordered, shuffled);
}
