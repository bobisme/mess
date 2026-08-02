//! Crash and contract tests for the pack snapshot sidecar.
//!
//! These drive [`Sidecar`] directly rather than through
//! [`PackSnapshotBackend`](super::PackSnapshotBackend), because the storage
//! core is what has the crash contract; the trait adapter is pure translation
//! and is covered by `tests/pack_snapshot.rs`'s parity suite.
//!
//! # The crash model, stated honestly
//!
//! Two distinct models are used, and neither is a real power cut:
//!
//! 1. **Injected abort at a named durability boundary.** A test-only hook
//!    returns an error immediately *before* one named step (`root:sync`,
//!    `roll:rename`, `prune:unlink`, …). The sidecar is then dropped and
//!    reopened from whatever is on disk. This faithfully models a *process*
//!    crash at that instant: every byte written so far is in the page cache and
//!    will reach the disk, and no cleanup code runs.
//! 2. **Byte-level mutilation.** For the failures a process crash cannot
//!    produce — a half-written frame, a partially persisted footer, a root
//!    naming a pack that is gone, a flipped bit inside a committed record — the
//!    test edits the directory directly and then reopens.
//!
//! What is *not* covered here: a real `SIGKILL` of a separate process, and
//! true power-loss reordering of unsynced writes (that needs a
//! fault-injecting filesystem; `mess-log`'s `SimFs`/`CrashFs` has one, but it
//! is wired to the log's own `Fs` trait rather than `std::fs`, so reusing it
//! would mean routing the sidecar through that abstraction — noted as future
//! work).

use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use mess_testkit::SweepingTempDir;

use super::format::{
    self, Record, SaveMode, TRUST_UNVERIFIED_CACHE, decode_root, encode_frame,
    encode_record, parse_pack_name, parse_root_name, root_name,
};
use super::sidecar::{FaultHook, PackSidecarError, Sidecar, SidecarOptions};
use crate::snapshot::{
    SnapshotCompatibility, SnapshotCoverage, SnapshotSaveOutcome,
    SnapshotScanDiagnostic, StableSnapshotId,
};

fn dir(name: &str) -> SweepingTempDir {
    mess_testkit::sweeping_temp_dir(&format!("pack-snapshot-{name}"))
}

/// The identity nearly every test in this file writes under.
fn compat(fold: u32) -> SnapshotCompatibility {
    SnapshotCompatibility {
        aggregate_schema_id: StableSnapshotId::new("pack.test"),
        fold_version:        fold,
        codec_id:            StableSnapshotId::new("pack.test"),
        codec_version:       1,
    }
}

fn rec(stream: &str, version: u64, state: &[u8]) -> Record {
    Record {
        stream_name:   stream.to_owned(),
        compatibility: compat(1),
        coverage:      SnapshotCoverage::Through(version),
        trust_mode:    TRUST_UNVERIFIED_CACHE,
        state:         state.to_vec(),
    }
}

/// Test shorthand: resolve a head under the default identity, as an `Option`.
trait LoadDefaultIdentity {
    fn load1(&self, stream: &str) -> Option<Record>;
}

impl LoadDefaultIdentity for Sidecar {
    fn load1(&self, stream: &str) -> Option<Record> {
        self.load(stream, compat(1)).hit()
    }
}

fn durable() -> SidecarOptions {
    SidecarOptions { mode: SaveMode::Durable, ..Default::default() }
}

/// A hook that aborts the first time `step` is reached.
fn fail_at(step: &'static str) -> FaultHook {
    Arc::new(move |s: &str| {
        if s == step {
            Err(std::io::Error::other(format!("injected crash at {s}")))
        } else {
            Ok(())
        }
    })
}

fn open_faulty(
    path: &std::path::Path,
    options: SidecarOptions,
    step: &'static str,
) -> Sidecar {
    Sidecar::open_writer_inner(path, options, Some(fail_at(step)))
        .expect("open with fault hook")
}

/// Every file in the directory, with its length — the fixture the read-only
/// tests compare against.
fn listing(path: &std::path::Path) -> Vec<(String, u64)> {
    let mut v: Vec<(String, u64)> = std::fs::read_dir(path)
        .expect("read_dir")
        .map(|e| {
            let e = e.expect("entry");
            (
                e.file_name().to_string_lossy().into_owned(),
                e.metadata().expect("metadata").len(),
            )
        })
        .collect();
    v.sort();
    v
}

fn pack_path(
    path: &std::path::Path,
    sealed: bool,
) -> Option<std::path::PathBuf> {
    std::fs::read_dir(path)
        .ok()?
        .filter_map(Result::ok)
        .find(|e| {
            e.file_name()
                .to_str()
                .and_then(parse_pack_name)
                .is_some_and(|(_, _, s)| s == sealed)
        })
        .map(|e| e.path())
}

fn root_generations(path: &std::path::Path) -> Vec<u64> {
    let mut v: Vec<u64> = std::fs::read_dir(path)
        .expect("read_dir")
        .filter_map(Result::ok)
        .filter_map(|e| {
            e.file_name().to_str().and_then(parse_root_name).map(|(_, g)| g)
        })
        .collect();
    v.sort_unstable();
    v
}

// ---------------------------------------------------------------------------
// Writer lock / read-only readers
// ---------------------------------------------------------------------------

#[test]
#[cfg_attr(miri, ignore)]
fn a_second_writer_fails_loudly() {
    let d = dir("second-writer");
    let _first =
        Sidecar::open_writer(d.path(), SidecarOptions::default()).expect("1st");
    let err = Sidecar::open_writer(d.path(), SidecarOptions::default())
        .expect_err("a second writer must fail, not silently share");
    assert!(matches!(err, PackSidecarError::Lock(_)), "got {err:?}");
}

#[test]
#[cfg_attr(miri, ignore)]
fn clones_share_one_serialized_writer_owner() {
    // The single writer owner is the shared `Sidecar`; every clone of the
    // backend holds the same `Arc`, so two "writers" in one process are the
    // same owner and serialize rather than racing.
    let d = dir("shared-owner");
    let s = Arc::new(
        Sidecar::open_writer(d.path(), SidecarOptions::default())
            .expect("open"),
    );
    let a = Arc::clone(&s);
    let b = Arc::clone(&s);
    let ta = std::thread::spawn(move || {
        for i in 0..50u64 {
            a.save(&rec("alpha", i, &[i as u8])).expect("save alpha");
        }
    });
    let tb = std::thread::spawn(move || {
        for i in 0..50u64 {
            b.save(&rec("beta", i, &[i as u8])).expect("save beta");
        }
    });
    ta.join().expect("alpha thread");
    tb.join().expect("beta thread");
    assert_eq!(
        s.load1("alpha").expect("alpha").coverage,
        SnapshotCoverage::Through(49)
    );
    assert_eq!(
        s.load1("beta").expect("beta").coverage,
        SnapshotCoverage::Through(49)
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn a_reader_never_mutates_the_directory_and_takes_no_lock() {
    let d = dir("read-only");
    {
        let w = Sidecar::open_writer(d.path(), durable()).expect("writer");
        w.save(&rec("s1", 3, b"state")).expect("save");
    }
    let before = listing(d.path());

    // A live writer plus a reader must coexist.
    let _writer = Sidecar::open_writer(d.path(), durable()).expect("writer");
    let r = Sidecar::open_reader(d.path());
    assert_eq!(r.load1("s1").expect("reader sees the head").state, b"state");
    let err = r.save(&rec("s1", 4, b"nope")).expect_err("reader cannot write");
    assert!(matches!(err, PackSidecarError::ReadOnly(_)), "got {err:?}");
    assert!(r.roll_now().is_err(), "reader cannot roll");
    assert!(r.prune_roots_now().is_err(), "reader cannot prune");

    drop(r);
    assert_eq!(
        listing(d.path()),
        before,
        "an offline reader must not create, truncate, repair, rename or \
         delete anything"
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn a_reader_on_a_missing_or_empty_sidecar_simply_has_no_heads() {
    let d = dir("reader-empty");
    let missing = d.path().join("not-here");
    let r = Sidecar::open_reader(&missing);
    assert_eq!(r.head_count(), 0);
    assert!(r.load1("anything").is_none());
    assert!(!missing.exists(), "opening a reader must not create the dir");

    std::fs::create_dir_all(&missing).expect("mkdir");
    let r = Sidecar::open_reader(&missing);
    assert!(r.load1("anything").is_none());
    assert_eq!(listing(&missing), Vec::new(), "still nothing created");
}

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

#[test]
#[cfg_attr(miri, ignore)]
fn identity_is_created_once_and_reused_across_reopen() {
    let d = dir("identity");
    let first = {
        let s = Sidecar::open_writer(d.path(), durable()).expect("open");
        s.save(&rec("s", 0, b"x")).expect("save");
        s.uuid()
    };
    let s = Sidecar::open_writer(d.path(), durable()).expect("reopen");
    assert_eq!(s.uuid(), first, "the store UUID is persisted, not reminted");
    assert_eq!(s.load1("s").expect("head survives").state, b"x");
}

#[test]
#[cfg_attr(miri, ignore)]
fn a_corrupt_identity_starts_a_new_namespace_and_never_reuses_ids() {
    let d = dir("identity-corrupt");
    let old_uuid = {
        let s = Sidecar::open_writer(d.path(), durable()).expect("open");
        s.save(&rec("s", 0, b"x")).expect("save");
        s.uuid()
    };
    let old_gens = root_generations(d.path());
    assert!(!old_gens.is_empty());
    let old_max = *old_gens.iter().max().expect("a generation");

    // Mangle IDENTITY.
    let id = d.path().join(format::IDENTITY_FILE);
    let mut raw = std::fs::read(&id).expect("read identity");
    raw[12] ^= 0xFF;
    std::fs::write(&id, &raw).expect("write identity");

    let s = Sidecar::open_writer(d.path(), durable()).expect("reopen");
    assert_ne!(s.uuid(), old_uuid, "a corrupt identity mints a new namespace");
    assert_eq!(s.head_count(), 0, "old artifacts are ignored, not adopted");
    s.save(&rec("s", 5, b"y")).expect("save under the new namespace");

    // Generations and pack sequences continue past everything the directory
    // has ever used, so a resurrected old descriptor can never alias a new
    // one (the anti-ABA rule).
    let new_gens: Vec<u64> = root_generations(d.path())
        .into_iter()
        .filter(|g| !old_gens.contains(g))
        .collect();
    assert!(!new_gens.is_empty());
    assert!(
        new_gens.iter().all(|g| *g > old_max),
        "new generations {new_gens:?} must exceed old {old_gens:?}"
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn ids_are_never_reused_even_after_every_root_is_deleted() {
    // The anti-ABA rule cannot rest on "highest number still on disk": delete
    // every root descriptor and a naive counter rewinds, letting a new root
    // reuse a generation a cached descriptor already saw. The durable
    // reservation in IDENTITY is what prevents that.
    let d = dir("id-reuse");
    let (uuid, used) = {
        let s = Sidecar::open_writer(d.path(), durable()).expect("open");
        for i in 0..8u64 {
            s.save(&rec("s", i, &[i as u8])).expect("save");
        }
        (s.uuid(), root_generations(d.path()))
    };
    assert!(used.len() >= 2);
    let highest_used = *used.iter().max().expect("a generation");

    for g in &used {
        std::fs::remove_file(d.path().join(root_name(uuid, *g)))
            .expect("delete root");
    }
    assert_eq!(root_generations(d.path()), Vec::<u64>::new());

    let s = Sidecar::open_writer(d.path(), durable()).expect("reopen");
    assert_eq!(s.uuid(), uuid, "the namespace is intact — only roots went");
    assert_eq!(s.head_count(), 0, "no root => no heads => replay");
    s.save(&rec("s", 99, b"fresh")).expect("save");
    let fresh = root_generations(d.path());
    assert!(
        fresh.iter().all(|g| *g > highest_used),
        "new generations {fresh:?} must all exceed the deleted {highest_used}"
    );
}

// ---------------------------------------------------------------------------
// Basic semantics
// ---------------------------------------------------------------------------

#[test]
#[cfg_attr(miri, ignore)]
fn save_load_and_reopen_are_idempotent() {
    let d = dir("roundtrip");
    {
        let s = Sidecar::open_writer(d.path(), durable()).expect("open");
        for i in 0..20u64 {
            s.save(&rec(&format!("stream-{i}"), i, &[i as u8; 16]))
                .expect("save");
        }
        assert_eq!(s.head_count(), 20);
    }
    // Reopening twice in a row must produce the same answer both times.
    for round in 0..2 {
        let s = Sidecar::open_writer(d.path(), durable()).expect("reopen");
        assert_eq!(s.head_count(), 20, "round {round}");
        for i in 0..20u64 {
            let r = s.load1(&format!("stream-{i}")).expect("head");
            assert_eq!(r.state, vec![i as u8; 16]);
            assert_eq!(r.coverage, SnapshotCoverage::Through(i));
            assert_eq!(r.stream_name, format!("stream-{i}"));
        }
        assert_eq!(s.stream_names().len(), 20);
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn records_are_self_describing_so_no_reverse_name_map_is_needed() {
    let d = dir("self-describing");
    {
        let s = Sidecar::open_writer(d.path(), durable()).expect("open");
        s.save(&rec("orders/42", 1, b"a")).expect("save");
        s.save(&rec("users/7", 2, b"b")).expect("save");
    }
    let r = Sidecar::open_reader(d.path());
    assert_eq!(
        r.stream_names(),
        vec!["orders/42".to_string(), "users/7".to_string(),]
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn coverage_never_regresses_and_identical_saves_are_idempotent() {
    let d = dir("coverage");
    let s = Sidecar::open_writer(d.path(), durable()).expect("open");
    s.save(&rec("s", 5, b"five")).expect("save");
    let after_first = s.metrics.records_written.load(Ordering::Relaxed);

    // Lower coverage must not become the head.
    assert_eq!(
        s.save(&rec("s", 2, b"two")).expect("save"),
        SnapshotSaveOutcome::CoverageRegressed {
            current: SnapshotCoverage::Through(5),
        }
    );
    assert_eq!(s.load1("s").expect("head").state, b"five");
    assert_eq!(s.metrics.coverage_regressions.load(Ordering::Relaxed), 1);
    assert_eq!(s.metrics.records_written.load(Ordering::Relaxed), after_first);

    // Byte-identical re-save at the same coverage writes nothing.
    assert_eq!(
        s.save(&rec("s", 5, b"five")).expect("save"),
        SnapshotSaveOutcome::Idempotent
    );
    assert_eq!(s.metrics.idempotent_saves.load(Ordering::Relaxed), 1);
    assert_eq!(s.metrics.records_written.load(Ordering::Relaxed), after_first);

    // Higher coverage replaces.
    assert_eq!(
        s.save(&rec("s", 9, b"nine")).expect("save"),
        SnapshotSaveOutcome::Published
    );
    assert_eq!(
        s.load1("s").expect("head").coverage,
        SnapshotCoverage::Through(9)
    );

    // The empty prefix is strictly below Through(0) and must not displace it.
    let mut empty = rec("t", 0, b"init");
    empty.coverage = SnapshotCoverage::Empty;
    s.save(&empty).expect("save empty");
    assert_eq!(s.load1("t").expect("head").coverage, SnapshotCoverage::Empty);
    s.save(&rec("t", 0, b"zero")).expect("save through(0)");
    assert_eq!(
        s.load1("t").expect("head").coverage,
        SnapshotCoverage::Through(0),
        "Through(0) outranks Empty"
    );
    assert_eq!(
        s.save(&empty).expect("save empty again"),
        SnapshotSaveOutcome::CoverageRegressed {
            current: SnapshotCoverage::Through(0),
        },
        "Empty must not regress a Through(0) head"
    );
    assert_eq!(
        s.load1("t").expect("head").coverage,
        SnapshotCoverage::Through(0)
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn equal_coverage_with_different_state_is_a_conflict_the_current_head_wins() {
    // ADR 0002 §1 rule 3. Two records claiming to fold the same prefix of the
    // same stream under the same identity to different states cannot both be
    // right; the store keeps the older evidence and reports the disagreement
    // rather than alternating between two answers.
    let d = dir("equal-coverage-conflict");
    let s = Sidecar::open_writer(d.path(), durable()).expect("open");
    s.save(&rec("s", 5, b"five")).expect("save");
    let after_first = s.metrics.records_written.load(Ordering::Relaxed);

    assert_eq!(
        s.save(&rec("s", 5, b"FIVE")).expect("save"),
        SnapshotSaveOutcome::Conflict {
            coverage: SnapshotCoverage::Through(5),
        }
    );
    assert_eq!(
        s.load1("s").expect("head").state,
        b"five",
        "the valid current record remains current"
    );
    assert_eq!(s.metrics.conflicts.load(Ordering::Relaxed), 1);
    assert_eq!(
        s.metrics.records_written.load(Ordering::Relaxed),
        after_first,
        "a refused save writes nothing at all"
    );

    // ... and it survives a reopen: nothing was written to be recovered.
    drop(s);
    let s = Sidecar::open_writer(d.path(), durable()).expect("reopen");
    assert_eq!(s.load1("s").expect("head").state, b"five");
}

#[test]
#[cfg_attr(miri, ignore)]
fn an_unreadable_head_at_equal_coverage_is_repaired_not_refused() {
    // ADR 0002 §1 rule 1: validating the current record *before* superseding
    // it is what lets a corrupt head be healed without weakening rule 3.
    let d = dir("equal-coverage-repair");
    let s = Sidecar::open_writer(d.path(), durable()).expect("open");
    s.save(&rec("s", 5, b"five")).expect("save");

    // Bit rot under a live writer: the committed record's bytes go bad in
    // place, same length, so the root still names a frame that is simply no
    // longer the one it named. (Corrupting across a reopen instead would be a
    // torn tail, which the writer truncates — a different contract.)
    let pack = pack_path(d.path(), false).expect("active pack");
    let mut bytes = std::fs::read(&pack).expect("read pack");
    // Locate the state by its full byte pattern: the pack header carries 16
    // random identity bytes, and a single-byte search would sometimes hit one
    // of those instead (a header a reader never validates — a flaky no-op).
    let at =
        bytes.windows(4).position(|w| w == b"five").expect("the state bytes");
    bytes[at] = b'F';
    std::fs::write(&pack, &bytes).expect("write pack");

    assert!(s.load1("s").is_none(), "a corrupt head is a miss");
    assert_eq!(
        s.save(&rec("s", 5, b"five")).expect("save"),
        SnapshotSaveOutcome::Repaired
    );
    assert_eq!(s.load1("s").expect("head").state, b"five");
    assert_eq!(s.metrics.repairs.load(Ordering::Relaxed), 1);
}

#[test]
#[cfg_attr(miri, ignore)]
fn a_different_identity_gets_its_own_head_and_never_hides_another() {
    // ADR 0002 §1: stream name plus compatibility is the complete key, so a
    // deploy's new identity does not overwrite the old one — and a *late* save
    // under the old identity cannot hide or delete the new one.
    let d = dir("identity-keys");
    let s = Sidecar::open_writer(d.path(), durable()).expect("open");
    s.save(&rec("s", 5, b"v1")).expect("save");

    let mut bumped = rec("s", 5, b"v2");
    bumped.compatibility = compat(2);
    s.save(&bumped).expect("save bumped");

    assert_eq!(s.load1("s").expect("v1 head").state, b"v1");
    assert_eq!(s.load("s", compat(2)).hit().expect("v2 head").state, b"v2");
    assert_eq!(s.head_count(), 2, "one head per identity");
    assert_eq!(s.stream_names(), vec!["s".to_string()], "one stream");

    // A late save under the OLD identity at a HIGHER coverage advances only
    // its own head.
    s.save(&rec("s", 9, b"v1-late")).expect("late save");
    assert_eq!(s.load1("s").expect("v1 head").state, b"v1-late");
    assert_eq!(
        s.load("s", compat(2)).hit().expect("v2 head").state,
        b"v2",
        "the newer identity is untouched"
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn a_miss_says_whether_the_stream_has_a_foreign_identity() {
    use crate::snapshot::{SnapshotLookup, SnapshotMiss};

    let d = dir("miss-reasons");
    let s = Sidecar::open_writer(d.path(), durable()).expect("open");
    assert_eq!(
        s.load("nothing", compat(1)),
        SnapshotLookup::Miss(SnapshotMiss::Absent)
    );

    s.save(&rec("s", 5, b"v1")).expect("save");
    assert_eq!(
        s.load("s", compat(2)),
        SnapshotLookup::Miss(SnapshotMiss::Incompatible { stored: compat(1) }),
        "a post-deploy load must be able to say why it missed"
    );
}

// ---------------------------------------------------------------------------
// Rolling and sealed-pack immutability
// ---------------------------------------------------------------------------

#[test]
#[cfg_attr(miri, ignore)]
fn rolling_seals_packs_that_are_then_never_appended() {
    let d = dir("roll-seal");
    let opts = SidecarOptions {
        mode: SaveMode::Durable,
        max_pack_bytes: 512,
        ..Default::default()
    };
    let s = Sidecar::open_writer(d.path(), opts).expect("open");
    for i in 0..40u64 {
        s.save(&rec(&format!("s{i}"), i, &[i as u8; 64])).expect("save");
    }
    assert!(
        s.metrics.packs_rolled.load(Ordering::Relaxed) >= 3,
        "the workload must actually roll"
    );

    let is_sealed =
        |n: &str| parse_pack_name(n).is_some_and(|(_, _, sealed)| sealed);
    // Freeze the sealed packs' sizes, then keep writing.
    let sealed_before: Vec<(String, u64)> =
        listing(d.path()).into_iter().filter(|(n, _)| is_sealed(n)).collect();
    assert!(sealed_before.len() >= 3);
    for i in 40..80u64 {
        s.save(&rec(&format!("s{i}"), i, &[i as u8; 64])).expect("save");
    }
    let sealed_after: Vec<(String, u64)> = listing(d.path())
        .into_iter()
        .filter(|(n, _)| is_sealed(n))
        .filter(|(n, _)| sealed_before.iter().any(|(b, _)| b == n))
        .collect();
    assert_eq!(
        sealed_before, sealed_after,
        "a sealed pack is immutable: never appended, never rewritten"
    );

    drop(s);
    let r = Sidecar::open_reader(d.path());
    for i in 0..80u64 {
        assert_eq!(
            r.load1(&format!("s{i}")).expect("head").state,
            vec![i as u8; 64],
            "head {i} must survive multi-pack reopen"
        );
    }
}

// ---------------------------------------------------------------------------
// Torn tails and corruption
// ---------------------------------------------------------------------------

#[test]
#[cfg_attr(miri, ignore)]
fn a_torn_active_tail_is_truncated_at_the_last_valid_frame() {
    let d = dir("torn-tail");
    {
        let s = Sidecar::open_writer(d.path(), durable()).expect("open");
        s.save(&rec("keep", 1, b"kept")).expect("save");
    }
    let pack = pack_path(d.path(), false).expect("an active pack");
    let good_len = std::fs::metadata(&pack).expect("meta").len();

    // A torn append: the first 60% of a well-formed frame, then — to prove
    // the scan stops at the FIRST invalid frame rather than resynchronizing —
    // a complete, perfectly valid frame after it.
    let torn = encode_frame(&encode_record(&rec("torn", 2, b"half")));
    let complete = encode_frame(&encode_record(&rec("after", 3, b"whole")));
    {
        let mut f = OpenOptions::new().append(true).open(&pack).expect("open");
        f.write_all(&torn[..torn.len() * 3 / 5]).expect("torn write");
        f.write_all(&complete).expect("valid write");
    }
    assert!(std::fs::metadata(&pack).expect("meta").len() > good_len);

    let s = Sidecar::open_writer(d.path(), durable()).expect("reopen");
    assert_eq!(
        std::fs::metadata(&pack).expect("meta").len(),
        good_len,
        "the writer truncates to the last valid frame end"
    );
    assert!(s.metrics.tail_truncated_bytes.load(Ordering::Relaxed) > 0);
    assert_eq!(s.load1("keep").expect("head").state, b"kept");
    assert!(s.load1("after").is_none(), "an unreachable frame is not a head");

    // And the writer can keep appending after the truncation.
    s.save(&rec("next", 4, b"more")).expect("save after recovery");
    assert_eq!(s.load1("next").expect("head").state, b"more");
}

#[test]
#[cfg_attr(miri, ignore)]
fn a_corrupt_frame_mid_pack_is_a_miss_never_a_wrong_answer() {
    let d = dir("corrupt-frame");
    {
        let s = Sidecar::open_writer(d.path(), durable()).expect("open");
        s.save(&rec("victim", 1, b"AAAAAAAAAAAAAAAA")).expect("save");
        s.save(&rec("bystander", 2, b"BBBBBBBBBBBBBBBB")).expect("save");
    }
    let pack = pack_path(d.path(), false).expect("active pack");
    let mut raw = std::fs::read(&pack).expect("read");
    // Flip a byte inside the first record's payload.
    let at = raw
        .windows(4)
        .position(|w| w == b"AAAA")
        .expect("find the victim payload");
    raw[at] = b'Z';
    std::fs::write(&pack, &raw).expect("write");

    let r = Sidecar::open_reader(d.path());
    assert!(
        r.load1("victim").is_none(),
        "a CRC-broken record is a miss, so the caller replays"
    );
    assert_eq!(
        r.load1("bystander").expect("intact head").state,
        b"BBBBBBBBBBBBBBBB",
        "one corrupt record must not take down its neighbours"
    );
    assert!(r.metrics.degraded_loads.load(Ordering::Relaxed) >= 1);
}

#[test]
#[cfg_attr(miri, ignore)]
fn a_stale_root_naming_a_missing_pack_falls_back_to_an_older_root() {
    let d = dir("stale-root");
    let opts = SidecarOptions {
        mode: SaveMode::Durable,
        max_pack_bytes: 256,
        // Keep every root so the fallback chain is long enough to test.
        prune_roots_at: usize::MAX,
        ..Default::default()
    };
    {
        let s = Sidecar::open_writer(d.path(), opts).expect("open");
        for i in 0..12u64 {
            s.save(&rec(&format!("s{i}"), i, &[i as u8; 48])).expect("save");
        }
        assert!(s.metrics.packs_rolled.load(Ordering::Relaxed) >= 1);
    }

    // Delete the newest sealed pack. Every root that names it is now
    // unresolvable; open must walk back to one that is.
    let gens = root_generations(d.path());
    assert!(gens.len() > 2);
    let mut sealed: Vec<std::path::PathBuf> = std::fs::read_dir(d.path())
        .expect("read_dir")
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_name()
                .to_str()
                .and_then(parse_pack_name)
                .is_some_and(|(_, _, s)| s)
        })
        .map(|e| e.path())
        .collect();
    sealed.sort();
    let doomed = sealed.last().expect("a sealed pack").clone();
    let doomed_seq = parse_pack_name(
        doomed.file_name().and_then(|n| n.to_str()).expect("name"),
    )
    .expect("parse")
    .1;
    std::fs::remove_file(&doomed).expect("delete a pack");

    let r = Sidecar::open_reader(d.path());
    assert!(
        r.metrics.roots_rejected.load(Ordering::Relaxed) > 0,
        "at least one root must have been rejected as unresolvable"
    );
    // Whatever root was selected, every head it publishes must resolve, and
    // none may point at the pack we deleted.
    assert!(r.head_count() > 0, "the fallback must find a usable root");
    for name in r.stream_names() {
        assert!(
            r.load1(&name).is_some(),
            "selected root must be independently resolvable ({name})"
        );
    }
    let selected: Vec<u64> = gens
        .iter()
        .rev()
        .filter_map(|g| {
            std::fs::read(d.path().join(root_name(r.uuid(), *g))).ok()
        })
        .filter_map(|raw| decode_root(&raw))
        .filter(|root| {
            !root.entries.iter().any(|e| e.pack_seq == doomed_seq)
                && root.active_pack != Some(doomed_seq)
        })
        .map(|root| root.generation)
        .collect();
    assert!(
        !selected.is_empty(),
        "the fixture must leave at least one resolvable older root"
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn every_root_corrupt_degrades_to_no_snapshots_not_an_error() {
    let d = dir("all-roots-corrupt");
    let uuid = {
        let s = Sidecar::open_writer(d.path(), durable()).expect("open");
        s.save(&rec("s", 1, b"x")).expect("save");
        s.save(&rec("s", 2, b"y")).expect("save");
        s.uuid()
    };
    for g in root_generations(d.path()) {
        let p = d.path().join(root_name(uuid, g));
        let mut raw = std::fs::read(&p).expect("read root");
        let n = raw.len();
        raw[n / 2] ^= 0xFF;
        std::fs::write(&p, &raw).expect("write root");
    }

    let r = Sidecar::open_reader(d.path());
    assert_eq!(r.head_count(), 0, "no usable root => no heads");
    assert!(r.load1("s").is_none());

    // The writer must also open cleanly and start publishing again.
    let w = Sidecar::open_writer(d.path(), durable()).expect("writer opens");
    assert_eq!(w.head_count(), 0);
    w.save(&rec("s", 3, b"z")).expect("save");
    assert_eq!(w.load1("s").expect("head").state, b"z");
}

#[test]
#[cfg_attr(miri, ignore)]
fn deleting_the_whole_sidecar_degrades_to_no_heads() {
    let d = dir("nuked");
    let inner = d.path().join("snap");
    {
        let s = Sidecar::open_writer(&inner, durable()).expect("open");
        s.save(&rec("s", 1, b"x")).expect("save");
    }
    std::fs::remove_dir_all(&inner).expect("nuke");
    let r = Sidecar::open_reader(&inner);
    assert!(r.load1("s").is_none());
    let w = Sidecar::open_writer(&inner, durable()).expect("recreates");
    assert_eq!(w.head_count(), 0);
    w.save(&rec("s", 1, b"x")).expect("save");
    assert_eq!(w.load1("s").expect("head").state, b"x");
}

#[test]
#[cfg_attr(miri, ignore)]
fn reserved_tmp_names_are_never_discovery_candidates() {
    let d = dir("tmp-names");
    let uuid = {
        let s = Sidecar::open_writer(d.path(), durable()).expect("open");
        s.save(&rec("s", 1, b"real")).expect("save");
        s.uuid()
    };
    // Plant a staging file for a *higher* generation full of lies. If
    // discovery ever accepted a `.tmp` name it would win the descending scan.
    let liar = d.path().join(format!(
        "{}{}",
        root_name(uuid, u64::MAX - 1),
        format::TMP_SUFFIX
    ));
    std::fs::write(&liar, b"not even a root descriptor").expect("plant");
    // And a `.tmp` pack, which must not be resolvable or appendable either.
    let liar_pack = d.path().join(format!(
        "{}{}",
        format::pack_name(uuid, u64::MAX - 1, false),
        format::TMP_SUFFIX
    ));
    std::fs::write(&liar_pack, b"junk").expect("plant");

    let r = Sidecar::open_reader(d.path());
    assert_eq!(r.load1("s").expect("real head").state, b"real");
    drop(r);
    let w = Sidecar::open_writer(d.path(), durable()).expect("writer opens");
    assert_eq!(w.load1("s").expect("real head").state, b"real");
    w.save(&rec("s", 2, b"more")).expect("save");
    assert!(liar.exists() && liar_pack.exists(), "and nothing was cleaned up");
}

// ---------------------------------------------------------------------------
// Crash at every durability boundary
// ---------------------------------------------------------------------------

/// After a crash at `step`, the sidecar must reopen with: the pre-crash head
/// intact, the in-flight head either fully present or fully absent, and a
/// writer that can immediately publish again.
fn assert_crash_recovers(
    label: &str,
    step: &'static str,
    options: SidecarOptions,
    prepare: impl Fn(&Sidecar),
) {
    let d = dir(label);
    {
        let s = Sidecar::open_writer(d.path(), options).expect("open");
        prepare(&s);
    }
    {
        let s = open_faulty(d.path(), options, step);
        let err = s
            .save(&rec("during", 2, b"in-flight"))
            .expect_err("the injected step must fail");
        assert!(
            format!("{err}").contains("injected"),
            "expected the injected failure, got {err}"
        );
        // Dropping without any further cleanup models the process dying here.
    }

    let s =
        Sidecar::open_writer(d.path(), options).expect("reopen after crash");
    assert_eq!(
        s.load1("before").map(|r| r.state),
        Some(b"committed".to_vec()),
        "[{step}] a head published before the crash must survive"
    );
    if let Some(r) = s.load1("during") {
        assert_eq!(
            r.state,
            b"in-flight".to_vec(),
            "[{step}] an in-flight head is all-or-nothing, never partial"
        );
    }
    // And the sidecar is immediately usable again.
    s.save(&rec("after", 99, b"recovered")).expect("save after recovery");
    assert_eq!(s.load1("after").expect("head").state, b"recovered");
    drop(s);
    let r = Sidecar::open_reader(d.path());
    assert_eq!(r.load1("after").expect("head").state, b"recovered");
}

#[test]
#[cfg_attr(miri, ignore)]
fn crash_at_every_record_and_root_step_recovers() {
    for step in [
        "record:append",
        "record:sync",
        "pack:dirsync",
        "root:write",
        "root:sync",
        "root:rename",
        "root:dirsync",
    ] {
        assert_crash_recovers(
            &format!("crash-{}", step.replace(':', "-")),
            step,
            durable(),
            |s| {
                s.save(&rec("before", 1, b"committed")).expect("prepare");
            },
        );
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn crash_at_every_roll_step_recovers() {
    let opts = SidecarOptions {
        mode: SaveMode::Durable,
        max_pack_bytes: 200,
        ..Default::default()
    };
    for step in [
        "roll:index",
        "roll:sync",
        "roll:rename",
        "roll:dirsync",
        "roll:newpack",
    ] {
        assert_crash_recovers(
            &format!("crash-{}", step.replace(':', "-")),
            step,
            opts,
            |s| {
                // Fill the pack past its limit so the next save must roll.
                s.save(&rec("before", 1, b"committed")).expect("prepare");
                s.save(&rec("filler", 1, &[0u8; 256])).expect("prepare");
            },
        );
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn crash_at_every_prune_step_keeps_at_least_two_roots() {
    let opts = SidecarOptions {
        mode: SaveMode::Durable,
        retain_roots: 2,
        prune_roots_at: 4,
        ..Default::default()
    };
    for step in ["prune:presync", "prune:unlink", "prune:dirsync"] {
        let d = dir(&format!("crash-{}", step.replace(':', "-")));
        {
            let s = Sidecar::open_writer(d.path(), opts).expect("open");
            for i in 0..4u64 {
                s.save(&rec("s", i, &[i as u8])).expect("save");
            }
        }
        {
            let s = open_faulty(d.path(), opts, step);
            // The 5th save trips the prune threshold; the prune then aborts.
            let _ = s.save(&rec("s", 9, b"nine"));
        }
        let gens = root_generations(d.path());
        assert!(
            gens.len() >= 2,
            "[{step}] at least two complete roots must remain, saw {gens:?}"
        );
        let s = Sidecar::open_writer(d.path(), opts).expect("reopen");
        assert!(s.load1("s").is_some(), "[{step}] a head must still resolve");
        s.save(&rec("s", 10, b"ten")).expect("still writable");
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn pruning_retains_at_least_two_roots_and_keeps_loading() {
    let d = dir("prune");
    let opts = SidecarOptions {
        mode: SaveMode::Durable,
        retain_roots: 2,
        prune_roots_at: 4,
        ..Default::default()
    };
    let s = Sidecar::open_writer(d.path(), opts).expect("open");
    for i in 0..30u64 {
        s.save(&rec("s", i, &[i as u8])).expect("save");
    }
    let gens = root_generations(d.path());
    assert!(gens.len() >= 2, "at least two complete roots, saw {gens:?}");
    assert!(gens.len() <= 5, "and pruning actually happened: {gens:?}");
    assert!(s.metrics.roots_pruned.load(Ordering::Relaxed) > 0);
    drop(s);
    assert_eq!(
        Sidecar::open_reader(d.path()).load1("s").expect("head").coverage,
        SnapshotCoverage::Through(29)
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn a_partially_persisted_footer_recovers_by_truncation() {
    // The injected-abort model cannot lose already-written bytes, so this is
    // the byte-level companion: crash during the roll's footer sync, then
    // discard the unsynced tail as a power cut would.
    let d = dir("partial-footer");
    let opts = SidecarOptions {
        mode: SaveMode::Durable,
        max_pack_bytes: 200,
        ..Default::default()
    };
    {
        let s = Sidecar::open_writer(d.path(), opts).expect("open");
        s.save(&rec("before", 1, b"committed")).expect("save");
        s.save(&rec("filler", 1, &[0u8; 256])).expect("save");
    }
    let pack = pack_path(d.path(), false).expect("active pack");
    let before_len = std::fs::metadata(&pack).expect("meta").len();
    {
        let s = open_faulty(d.path(), opts, "roll:sync");
        let _ = s.save(&rec("during", 2, b"in-flight"));
    }
    // The index+footer bytes were written but never synced: drop half of them.
    let after_len = std::fs::metadata(&pack).expect("meta").len();
    assert!(after_len > before_len, "the roll appended index+footer");
    let f = OpenOptions::new().write(true).open(&pack).expect("open pack");
    f.set_len(before_len + (after_len - before_len) / 2).expect("truncate");
    drop(f);

    let s = Sidecar::open_writer(d.path(), opts).expect("reopen");
    assert_eq!(
        std::fs::metadata(&pack).expect("meta").len(),
        before_len,
        "an incomplete footer is truncated back to the last valid frame"
    );
    assert_eq!(s.load1("before").expect("head").state, b"committed");
    s.save(&rec("after", 3, b"recovered")).expect("still writable");
}

// ---------------------------------------------------------------------------
// Durability modes
// ---------------------------------------------------------------------------

#[test]
#[cfg_attr(miri, ignore)]
fn buffered_issues_no_barriers_and_durable_does() {
    let d = dir("buffered");
    let s = Sidecar::open_writer(d.path(), SidecarOptions::default())
        .expect("open");
    let baseline = s.metrics.barriers.load(Ordering::Relaxed);
    for i in 0..10u64 {
        s.save(&rec("s", i, &[i as u8])).expect("buffered save");
    }
    assert_eq!(
        s.metrics.barriers.load(Ordering::Relaxed),
        baseline,
        "a Buffered save is a discardable cache write: no fsync at all"
    );
    // …yet it is still ordered and atomically visible.
    assert_eq!(
        s.load1("s").expect("head").coverage,
        SnapshotCoverage::Through(9)
    );
    drop(s);
    assert_eq!(
        Sidecar::open_reader(d.path()).load1("s").expect("head").coverage,
        SnapshotCoverage::Through(9)
    );

    let d2 = dir("durable");
    let s = Sidecar::open_writer(d2.path(), durable()).expect("open");
    let baseline = s.metrics.barriers.load(Ordering::Relaxed);
    s.save(&rec("s", 0, b"x")).expect("durable save");
    assert!(
        s.metrics.barriers.load(Ordering::Relaxed) >= baseline + 3,
        "a Durable save syncs the pack, the root file and the directory"
    );
    assert!(s.metrics.promoted_bytes.load(Ordering::Relaxed) > 0);
}

#[test]
#[cfg_attr(miri, ignore)]
fn a_durable_save_promotes_inherited_buffered_records() {
    // Unrelated streams written under Buffered are part of the new root's
    // closure, so the Durable save must sweep their bytes in too.
    let d = dir("durable-closure");
    let s = Sidecar::open_writer(d.path(), SidecarOptions::default())
        .expect("open");
    // (The identity file is written durably at open, so measure from there.)
    let baseline = s.metrics.barriers.load(Ordering::Relaxed);
    for i in 0..5u64 {
        s.save(&rec(&format!("buffered-{i}"), i, &[i as u8; 32]))
            .expect("buffered save");
    }
    assert_eq!(s.metrics.barriers.load(Ordering::Relaxed), baseline);
    s.save_with_mode(&rec("durable", 0, b"d"), SaveMode::Durable)
        .expect("durable save");
    let promoted = s.metrics.promoted_bytes.load(Ordering::Relaxed);
    assert!(
        promoted > 5 * 32,
        "promotion must cover the inherited Buffered records, saw {promoted}"
    );

    // The published root records the mode it was acknowledged under, so a
    // Buffered root can never be mistaken for a Durable one after restart.
    let uuid = s.uuid();
    let modes: Vec<SaveMode> = root_generations(d.path())
        .iter()
        .filter_map(|g| std::fs::read(d.path().join(root_name(uuid, *g))).ok())
        .filter_map(|raw| decode_root(&raw))
        .map(|r| r.mode)
        .collect();
    assert!(modes.contains(&SaveMode::Buffered));
    assert!(modes.contains(&SaveMode::Durable));
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

#[test]
#[cfg_attr(miri, ignore)]
fn readers_pread_committed_ranges_while_the_writer_appends() {
    let d = dir("concurrent");
    let s = Arc::new(
        Sidecar::open_writer(
            d.path(),
            SidecarOptions { max_pack_bytes: 4096, ..Default::default() },
        )
        .expect("open"),
    );
    s.save(&rec("hot", 0, &[0u8; 128])).expect("seed");

    let reader = Arc::clone(&s);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_r = Arc::clone(&stop);
    let t = std::thread::spawn(move || {
        let mut seen = 0u64;
        while !stop_r.load(Ordering::Relaxed) {
            if let Some(r) = reader.load1("hot") {
                // Every observation must be an internally consistent record,
                // never a shred of one.
                assert_eq!(r.state.len(), 128);
                assert!(r.state.iter().all(|b| *b == r.state[0]));
                assert_eq!(
                    u64::from(r.state[0]),
                    r.coverage.covered_version().unwrap_or(0) % 251
                );
                seen += 1;
            }
        }
        seen
    });
    for i in 1..500u64 {
        s.save(&rec("hot", i, &[(i % 251) as u8; 128])).expect("save");
    }
    stop.store(true, Ordering::Relaxed);
    let seen = t.join().expect("reader thread");
    assert!(seen > 0, "the reader must have observed something");
    assert_eq!(s.metrics.degraded_loads.load(Ordering::Relaxed), 0);
}

// ---------------------------------------------------------------------------
// Bounded administrative scan (ADR 0002 §1, "Bounded copy-on-write discovery")
// ---------------------------------------------------------------------------

/// Walk every page of one pin, asserting the page contract as it goes.
fn drain_scan(
    s: &Sidecar,
    pin: &crate::snapshot::PinnedSnapshotRoot,
    limit: u32,
) -> (Vec<crate::snapshot::SnapshotScanEntry>, Vec<SnapshotScanDiagnostic>) {
    let limit = std::num::NonZeroU32::new(limit).expect("nonzero");
    let mut all = Vec::new();
    let mut diags = Vec::new();
    let mut cursor = None;
    let mut pages = 0;
    loop {
        let page = s.scan(pin, cursor.as_ref(), limit);
        assert!(
            page.entries.len() <= limit.get() as usize,
            "a page must never exceed its limit"
        );
        diags.push(page.diagnostic);
        all.extend(page.entries);
        pages += 1;
        assert!(pages < 10_000, "pagination must terminate");
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    (all, diags)
}

#[test]
#[cfg_attr(miri, ignore)]
fn a_bounded_scan_pages_every_head_exactly_once_in_key_order() {
    // The scale shape: many heads, small pages, no page holding more than its
    // limit, and the concatenation being the complete set in key order.
    let d = dir("scan-scale");
    let s = Sidecar::open_writer(d.path(), SidecarOptions::default())
        .expect("open");
    const HEADS: u64 = 2_000;
    for i in 0..HEADS {
        s.save(&rec(&format!("stream-{i:06}"), i, &[i as u8; 8]))
            .expect("save");
    }

    let pin = s.pin_root().expect("a published root");
    let (entries, diags) = drain_scan(&s, &pin, 64);
    assert_eq!(entries.len() as u64, HEADS, "every head, exactly once");
    assert!(
        diags.iter().all(|d| *d == SnapshotScanDiagnostic::Complete),
        "an intact sidecar scans clean: {diags:?}"
    );
    assert!(
        entries.windows(2).all(|w| w[0].key < w[1].key),
        "strictly ascending key order, so pages neither overlap nor skip"
    );
    assert_eq!(entries[0].key.stream_id, "stream-000000");
    assert_eq!(entries[0].coverage, SnapshotCoverage::Through(0));
    assert_eq!(entries[0].key.compatibility, compat(1));
    assert_eq!(entries[0].state_len, 8);
    assert_eq!(
        entries[0].trust,
        crate::snapshot::SnapshotTrust::UnverifiedCache
    );

    // The limit is capped, not merely trusted: an unbounded request yields at
    // most the cap, and the cap is below the head count here on purpose.
    let huge = std::num::NonZeroU32::new(u32::MAX).expect("nonzero");
    let page = s.scan(&pin, None, huge);
    assert_eq!(
        page.entries.len() as u64,
        u64::from(crate::snapshot::MAX_SNAPSHOT_SCAN_LIMIT).min(HEADS),
        "an unbounded request is clamped, never honoured"
    );
    assert!(
        (page.entries.len() as u64) < HEADS
            || HEADS <= u64::from(crate::snapshot::MAX_SNAPSHOT_SCAN_LIMIT)
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn one_stream_under_two_identities_is_two_scan_entries() {
    let d = dir("scan-identities");
    let s = Sidecar::open_writer(d.path(), SidecarOptions::default())
        .expect("open");
    s.save(&rec("s", 1, b"v1")).expect("save");
    let mut v2 = rec("s", 1, b"v2");
    v2.compatibility = compat(2);
    s.save(&v2).expect("save");

    let pin = s.pin_root().expect("root");
    let (entries, _) = drain_scan(&s, &pin, 8);
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|e| e.key.stream_id == "s"));
    assert_eq!(entries[0].key.compatibility, compat(1));
    assert_eq!(entries[1].key.compatibility, compat(2));
}

#[test]
#[cfg_attr(miri, ignore)]
fn a_cursor_from_another_root_is_rejected_not_reinterpreted() {
    let d1 = dir("scan-root-a");
    let s1 =
        Sidecar::open_writer(d1.path(), SidecarOptions::default()).expect("a");
    for i in 0..4u64 {
        s1.save(&rec(&format!("s{i}"), i, b"x")).expect("save");
    }
    let pin1 = s1.pin_root().expect("root");
    let page = s1.scan(&pin1, None, std::num::NonZeroU32::new(2).unwrap());
    let cursor = page.next_cursor.expect("more pages");

    // A different store entirely.
    let d2 = dir("scan-root-b");
    let s2 =
        Sidecar::open_writer(d2.path(), SidecarOptions::default()).expect("b");
    s2.save(&rec("s0", 0, b"x")).expect("save");
    let pin2 = s2.pin_root().expect("root");
    assert_eq!(
        s2.scan(&pin2, Some(&cursor), std::num::NonZeroU32::new(2).unwrap())
            .diagnostic,
        SnapshotScanDiagnostic::Rejected,
        "a foreign cursor names a key that may mean something else here"
    );
    assert_eq!(
        s1.scan(&pin2, None, std::num::NonZeroU32::new(2).unwrap()).diagnostic,
        SnapshotScanDiagnostic::Rejected,
        "so is a foreign pin"
    );

    // The same store, but a generation later: the old pin no longer names the
    // served root, so a destructive caller fails closed.
    s1.save(&rec("s9", 9, b"new")).expect("save");
    assert_eq!(
        s1.scan(&pin1, None, std::num::NonZeroU32::new(2).unwrap()).diagnostic,
        SnapshotScanDiagnostic::Rejected,
        "a stale pin is rejected, not silently upgraded"
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn a_corrupt_head_makes_its_page_partial_and_is_skipped_never_faked() {
    let d = dir("scan-partial");
    {
        let s = Sidecar::open_writer(d.path(), durable()).expect("open");
        s.save(&rec("a", 1, b"AAAAAAAAAAAAAAAA")).expect("save");
        s.save(&rec("b", 2, b"BBBBBBBBBBBBBBBB")).expect("save");
    }
    // Corrupt exactly one committed record's bytes.
    let pack = pack_path(d.path(), false).expect("active pack");
    let mut bytes = std::fs::read(&pack).expect("read");
    let at = bytes
        .windows(16)
        .position(|w| w == b"AAAAAAAAAAAAAAAA")
        .expect("the victim's state bytes");
    bytes[at] = b'Z';
    std::fs::write(&pack, &bytes).expect("write");

    let r = Sidecar::open_reader(d.path());
    let pin = r.pin_root().expect("root");
    let (entries, diags) = drain_scan(&r, &pin, 8);
    assert_eq!(entries.len(), 1, "the intact head is still reported");
    assert_eq!(entries[0].key.stream_id, "b");
    assert!(
        diags.contains(&SnapshotScanDiagnostic::Partial { unresolved: 1 }),
        "the damaged head is counted, not fabricated: {diags:?}"
    );
    assert!(
        !diags.contains(&SnapshotScanDiagnostic::Complete),
        "and the page must not claim to be complete"
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn a_pin_holds_its_view_while_the_writer_publishes_over_it() {
    // The lease: a scan walking a pinned root sees exactly that root's heads,
    // however many generations land on top of it.
    let d = dir("scan-lease");
    let s = Sidecar::open_writer(d.path(), SidecarOptions::default())
        .expect("open");
    for i in 0..8u64 {
        s.save(&rec(&format!("s{i}"), i, b"x")).expect("save");
    }
    let pin = s.pin_root().expect("root");
    let first = s.scan(&pin, None, std::num::NonZeroU32::new(4).unwrap());
    assert_eq!(first.entries.len(), 4);

    // Publish more heads mid-scan. The pin is now stale, which is exactly the
    // fail-closed signal — and the immutable view it leased is still alive.
    for i in 8..16u64 {
        s.save(&rec(&format!("s{i}"), i, b"x")).expect("save");
    }
    assert_eq!(
        s.scan(
            &pin,
            first.next_cursor.as_ref(),
            std::num::NonZeroU32::new(4).unwrap()
        )
        .diagnostic,
        SnapshotScanDiagnostic::Rejected
    );

    // Re-pinning sees the new generation and the complete set.
    let pin = s.pin_root().expect("root");
    let (entries, diags) = drain_scan(&s, &pin, 4);
    assert_eq!(entries.len(), 16);
    assert!(diags.iter().all(|d| *d == SnapshotScanDiagnostic::Complete));
}

#[test]
#[cfg_attr(miri, ignore)]
fn a_store_with_no_published_root_has_nothing_to_pin() {
    let d = dir("scan-empty");
    let s = Sidecar::open_writer(d.path(), SidecarOptions::default())
        .expect("open");
    assert!(s.pin_root().is_none(), "no root, nothing to enumerate");
    assert!(Sidecar::open_reader(d.path()).pin_root().is_none());
}
