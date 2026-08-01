//! bn-30u — the **sealed-index candidate lifecycle**: a sidecar the engine
//! cannot admit must not sit on disk being re-refuted forever.
//!
//! Before this bone a footerless or otherwise refuted candidate stayed exactly
//! where it was, was re-read and re-judged on every single reopen, and its
//! rolled raw segment was never re-queued for sealing. The store served every
//! byte correctly from the raw log — no data was ever at risk — but it could
//! never climb back onto the sealed path. One crash in a millisecond-wide
//! window permanently demoted a segment to raw scan.
//!
//! These tests pin the whole lifecycle end to end, against a REAL rolled store
//! (no mocks, no fault-injecting filesystem): each of the four refutation
//! shapes, the durable quarantine, the interrupted-cleanup crash points, the
//! "evaluated at most once" property across repeated reopens, and the
//! successful re-seal that puts the segment back on the sealed path — with the
//! full read surface asserted byte-exact at every step, because the whole
//! justification for throwing a candidate away is that the log is authority.
//!
//! # How a refuted candidate is manufactured
//!
//! Realistically, not by mutilating the log:
//!
//! - **unparsable** — overwrite the `.pidx`/`.seal` with garbage (a truncated
//!   or torn artifact under its real name);
//! - **coverage-unproven** — forge a structurally valid sidecar claiming far
//!   more events than the segment holds (the sidecar-before-data crash: the
//!   sidecar's fsync landed, the covered tail bytes did not);
//! - **identity-mismatch** — forge one whose `base_pos` contradicts the segment
//!   header;
//! - **orphan** — leave a valid candidate for a segment that has no `.log`.
//!
//! The last three go through the *pending* path automatically: the engine
//! cross-checks the candidate against the segment's footer trailer, and a
//! forged `base_pos`/`event_count` fails that cross-check, so the candidate is
//! exactly the footerless-pending shape the recovery scan must judge.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use mess_index::sealed::{SealBatch, SealInput, SealStream, encode_sidecar};
use mess_store::backend::{Backend, RecordToAppend, StoredRecord};
use mess_store::{EngineOptions, LogEngine, RefutationReason, Version};

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

/// Sized so a 16 KiB active segment rolls at least three times: the tests need
/// a live head, one rolled segment to damage, and other rolled segments that
/// must stay untouched.
const STREAMS: usize = 8;
const PER: usize = 45;

fn rec(message_type: &str, data: &[u8]) -> RecordToAppend {
    RecordToAppend {
        message_type: message_type.to_string(),
        data:         data.to_vec(),
    }
}

/// A tiny active segment so a few hundred small batches span several segments
/// and the background sealer produces real rolled+sealed segments.
fn rolling_opts() -> EngineOptions {
    EngineOptions {
        segment_size: 16 * 1024,
        // loose-sidecar coverage — this mode must keep working forever;
        // bn-ccx1
        seal_pack: false,
        ..EngineOptions::default()
    }
}

fn payload(s: usize, i: usize) -> Vec<u8> {
    format!("s{s:03}-e{i:04}").into_bytes()
}

/// Build a multi-segment store and drain its sealer, so `sealed/` holds a real
/// `.pidx`/`.filter`/`.pcol` trio (or `.seal` pack) per rolled segment and
/// every rolled `.log` carries its footer trailer. Returns the full global
/// read as the byte-exact baseline every later assertion compares against.
async fn build_rolled_store(
    store: &Path,
    opts: EngineOptions,
) -> Vec<StoredRecord> {
    let engine = LogEngine::open_with(store, opts).expect("open fresh");
    for s in 0..STREAMS {
        let name = format!("stream-{s}");
        let mut expected = Version::NoStream;
        for i in 0..PER {
            let out = engine
                .append_batch(&name, expected, &[rec("ev", &payload(s, i))])
                .await
                .expect("append");
            expected = out.version;
        }
    }
    let baseline =
        engine.read_global(None, STREAMS * PER * 2).await.expect("baseline");
    assert_eq!(baseline.len(), STREAMS * PER);
    drop(engine); // drains the sealer: sidecars + footers durable
    baseline
}

/// Every read path agrees with the baseline, byte for byte. The point of
/// throwing a candidate away is that the log is authority — so every test
/// re-proves it, not just the ones about reads.
async fn assert_reads_match(engine: &LogEngine, baseline: &[StoredRecord]) {
    let g =
        engine.read_global(None, STREAMS * PER * 2).await.expect("global read");
    assert_eq!(g.len(), baseline.len(), "global event count");
    for (got, want) in g.iter().zip(baseline) {
        assert_eq!(got.stream_id, want.stream_id, "stream_id");
        assert_eq!(got.message_type, want.message_type, "message_type");
        assert_eq!(got.data, want.data, "payload bytes");
        assert_eq!(got.stream_position, want.stream_position, "stream_pos");
        assert_eq!(got.global_position, want.global_position, "global_pos");
    }
    for s in 0..STREAMS {
        let name = format!("stream-{s}");
        let evs = engine
            .read_stream(&name, Version::NoStream, PER * 2)
            .await
            .expect("stream read");
        assert_eq!(evs.len(), PER, "{name}: every event served");
        for (i, e) in evs.iter().enumerate() {
            assert_eq!(e.data, payload(s, i), "{name} event {i}: byte-exact");
        }
        assert_eq!(
            engine.head(&name).await.expect("head"),
            Version::At((PER - 1) as u64),
            "{name}: head survives"
        );
    }
}

// ---------------------------------------------------------------------------
// Store layout helpers
// ---------------------------------------------------------------------------

fn sealed_dir(store: &Path) -> PathBuf { store.join("sealed") }

fn candidate_path(store: &Path, seg_id: u64, ext: &str) -> PathBuf {
    sealed_dir(store).join(format!("seg-{seg_id:020}.{ext}"))
}

fn quarantined_path(store: &Path, seg_id: u64, ext: &str) -> PathBuf {
    sealed_dir(store).join(format!("seg-{seg_id:020}.{ext}.refuted"))
}

fn count_ext(dir: &Path, ext: &str) -> usize {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .filter(|e| {
                    e.path().extension().and_then(|x| x.to_str()) == Some(ext)
                })
                .count()
        })
        .unwrap_or(0)
}

/// Segment ids with a `seg-<id>.log` on disk, ascending. The last is the live
/// head; everything before it is rolled and therefore re-sealable.
fn segment_ids(store: &Path) -> Vec<u64> {
    let mut ids: Vec<u64> = std::fs::read_dir(store)
        .expect("store dir")
        .flatten()
        .filter_map(|e| {
            let name = e.file_name();
            let name = name.to_str()?;
            name.strip_prefix("seg-")?.strip_suffix(".log")?.parse().ok()
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// A rolled (non-head) segment id that really does have a sealed candidate on
/// disk — the one the tests damage.
fn a_rolled_sealed_segment(store: &Path, ext: &str) -> u64 {
    let ids = segment_ids(store);
    assert!(
        ids.len() >= 2,
        "the corpus must have rolled at least once: {ids:?}"
    );
    let head = *ids.last().expect("non-empty");
    ids.into_iter()
        .filter(|&id| id != head)
        .find(|&id| candidate_path(store, id, ext).exists())
        .unwrap_or_else(|| panic!("no rolled segment carries a .{ext}"))
}

/// A structurally valid sidecar image whose header says whatever we want it
/// to. Its pointers are deliberately for a stream the corpus never uses, so if
/// this were ever wrongly ADMITTED the read assertions would fail loudly
/// rather than the test passing on a technicality.
fn forged_sidecar(segment_id: u64, base_pos: u64, frame_count: u32) -> Vec<u8> {
    encode_sidecar(&SealInput {
        segment_id,
        base_pos,
        streams: vec![SealStream {
            stream_id: 424_242,
            batches:   vec![SealBatch {
                first_version: 0,
                frame_count,
                first_global_pos: base_pos,
                offset: 52,
            }],
        }],
        payloads: None,
        event_type_ids: None,
    })
}

/// bn-11g: the SealPack identity `seg`'s footer NAMES (spec 01 §3.3.3), or
/// `None` for a footer that names none (unsealed, or the legacy policy).
fn named_pack_identity(store: &Path, seg: u64) -> Option<[u8; 32]> {
    use mess_log::footer_ext::decode_extension;
    use mess_log::runtime::real::RealFs;
    use mess_log::sealer::{read_extension, read_trailer};

    let log = store.join(format!("seg-{seg:08}.log"));
    let cat = read_trailer(&RealFs, &log).ok().flatten()?;
    if !cat.names_seal_pack() {
        return None;
    }
    let ext = read_extension(&RealFs, &log, &cat).ok().flatten()?;
    decode_extension(&ext).pack_identity.map(|n| n.identity)
}

/// bn-11g: what the pack on disk actually hashes to — derived independently of
/// the footer, which is the point of the comparison.
fn pack_identity_on_disk(store: &Path, seg: u64) -> Option<[u8; 32]> {
    mess_index::sealed::SealedSegmentIndex::open_pack_eager(&candidate_path(
        store, seg, "seal",
    ))
    .ok()?
    .pack_identity()
    .map(|i| *i.as_bytes())
}

/// Wait for the background re-seal to put a fresh candidate back under its
/// real name. Bounded; the sealer gate is already satisfied at open (recovery
/// seeded both the hot index and the published watermark), so this normally
/// returns on the first poll.
fn await_candidate(path: &Path) -> bool {
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    false
}

// ---------------------------------------------------------------------------
// The four refutation shapes
// ---------------------------------------------------------------------------

/// **Truncated / corrupt candidate.** A `.pidx` that fails its CRC is refuted
/// at load, quarantined out of the candidate namespace, and its rolled segment
/// re-queued for a fresh seal — while every read still comes back byte-exact
/// from the raw log.
#[tokio::test]
async fn a_corrupt_candidate_is_refuted_quarantined_and_resealed() {
    let d = mess_testkit::sweeping_temp_dir("cand-corrupt-refuted-resealed");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, rolling_opts()).await;

    let victim = a_rolled_sealed_segment(&store, "pidx");
    let sealed_before = count_ext(&sealed_dir(&store), "pidx");
    std::fs::write(
        candidate_path(&store, victim, "pidx"),
        b"truncated garbage - fails magic and CRC",
    )
    .expect("corrupt the candidate");

    // ---- Open 1: refute + quarantine + enqueue. ----
    let engine =
        LogEngine::open_with(&store, rolling_opts()).expect("reopen 1");
    let health = engine.sealed_candidate_health();
    assert_eq!(health.refuted(), 1, "exactly one candidate refuted");
    assert_eq!(health.refutations[0].segment_id, victim);
    assert_eq!(
        health.refutations[0].reason,
        RefutationReason::Unparsable,
        "a CRC failure is an unparsable candidate"
    );
    assert!(health.refutations[0].quarantined, "the rename must land");
    assert_eq!(health.quarantine_fails, 0);
    assert_eq!(
        health.pending_reseal,
        vec![victim],
        "the rolled raw segment is re-queued exactly once"
    );
    assert_eq!(
        health.last_refutation().as_deref(),
        Some(format!("segment {victim}: unparsable").as_str()),
        "the reason string an operator reads first"
    );

    // The same counters are on the Copy metrics surface.
    let m = engine.metrics();
    assert_eq!(m.sealed_candidates_refuted, 1);
    assert_eq!(m.sealed_candidates_quarantined, 1);
    assert_eq!(m.sealed_candidate_quarantine_failures, 0);
    assert_eq!(m.sealed_reseals_enqueued, 1);

    // The candidate really left the candidate namespace, with its derived
    // siblings, and the raw log is untouched.
    for ext in ["pidx", "filter", "pcol"] {
        assert!(
            !candidate_path(&store, victim, ext).exists(),
            "the .{ext} must not remain a candidate"
        );
    }
    assert!(quarantined_path(&store, victim, "pidx").exists());
    assert!(store.join(format!("seg-{victim:08}.log")).exists());

    // A refuted candidate never blocks a read.
    assert_reads_match(&engine, &baseline).await;

    // ---- The enqueued re-seal lands. ----
    assert!(
        await_candidate(&candidate_path(&store, victim, "pidx")),
        "the background sealer must write a fresh candidate"
    );
    drop(engine);
    assert_eq!(
        count_ext(&sealed_dir(&store), "pidx"),
        sealed_before,
        "the sealed tier is whole again"
    );

    // ---- Open 2: nothing left to refute; the segment is admitted. ----
    let engine =
        LogEngine::open_with(&store, rolling_opts()).expect("reopen 2");
    let health = engine.sealed_candidate_health();
    assert_eq!(
        health.refuted(),
        0,
        "the quarantined bytes are never re-judged"
    );
    assert_eq!(
        health.pending_reseal,
        Vec::<u64>::new(),
        "an admitted segment is owed nothing"
    );
    assert_eq!(
        engine.sealed_segment_count(),
        sealed_before,
        "every rolled segment is served from the sealed tier again"
    );
    assert_reads_match(&engine, &baseline).await;
}

/// **Coverage-unproven.** The sidecar-before-data crash: a structurally valid
/// candidate claims coverage the segment's durable committed prefix does not
/// reach. The recovery scan refutes it, and — critically — does NOT install
/// it, so no read is ever answered from a pointer into bytes that never landed.
#[tokio::test]
async fn a_candidate_claiming_undurable_coverage_is_refuted() {
    let d = mess_testkit::sweeping_temp_dir("cand-coverage-unproven");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, rolling_opts()).await;

    let victim = a_rolled_sealed_segment(&store, "pidx");
    let real = mess_index::sealed::SealedSegmentIndex::open(&candidate_path(
        &store, victim, "pidx",
    ))
    .expect("read the real candidate");
    let base = real.base_pos();
    // Same identity, wildly more events than the segment holds: the footer
    // cross-check fails (so it becomes a pending candidate) and the scan then
    // cannot prove the coverage end is durable.
    std::fs::write(
        candidate_path(&store, victim, "pidx"),
        forged_sidecar(victim, base, 1_000_000),
    )
    .expect("forge");

    let engine = LogEngine::open_with(&store, rolling_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    assert_eq!(health.refuted(), 1);
    assert_eq!(health.refutations[0].segment_id, victim);
    assert_eq!(
        health.refutations[0].reason,
        RefutationReason::CoverageUnproven
    );
    assert!(health.refutations[0].quarantined);
    assert_eq!(health.pending_reseal, vec![victim]);
    assert!(quarantined_path(&store, victim, "pidx").exists());
    assert_reads_match(&engine, &baseline).await;

    assert!(await_candidate(&candidate_path(&store, victim, "pidx")));
    drop(engine);
    let engine =
        LogEngine::open_with(&store, rolling_opts()).expect("reopen 2");
    assert_eq!(engine.sealed_candidate_health().refuted(), 0);
    assert_reads_match(&engine, &baseline).await;
}

/// **Identity mismatch.** A candidate whose `base_pos` contradicts its
/// segment's own header describes a different segment; nothing about it can be
/// trusted, so it is refuted before its coverage is even considered.
#[tokio::test]
async fn an_identity_mismatched_candidate_is_refuted() {
    let d = mess_testkit::sweeping_temp_dir("cand-identity-mismatch");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, rolling_opts()).await;

    let victim = a_rolled_sealed_segment(&store, "pidx");
    let real = mess_index::sealed::SealedSegmentIndex::open(&candidate_path(
        &store, victim, "pidx",
    ))
    .expect("read the real candidate");
    // One event, one position off: coverage is trivially reachable, so only
    // the identity check can fire.
    std::fs::write(
        candidate_path(&store, victim, "pidx"),
        forged_sidecar(victim, real.base_pos() + 1, 1),
    )
    .expect("forge");

    let engine = LogEngine::open_with(&store, rolling_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    assert_eq!(health.refuted(), 1);
    assert_eq!(
        health.refutations[0].reason,
        RefutationReason::IdentityMismatch
    );
    assert!(health.refutations[0].quarantined);
    assert_eq!(health.pending_reseal, vec![victim]);
    assert_reads_match(&engine, &baseline).await;

    assert!(await_candidate(&candidate_path(&store, victim, "pidx")));
    drop(engine);
    let engine =
        LogEngine::open_with(&store, rolling_opts()).expect("reopen 2");
    assert_eq!(engine.sealed_candidate_health().refuted(), 0);
    assert_reads_match(&engine, &baseline).await;
}

/// **Orphan.** A candidate for a segment that has no `.log` can never be
/// confirmed by any future scan, so it is refuted rather than left to be
/// re-parsed forever. It has no raw segment to re-seal, so nothing is queued.
#[tokio::test]
async fn an_orphan_candidate_is_refuted_and_queues_no_reseal() {
    let d = mess_testkit::sweeping_temp_dir("cand-orphan");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, rolling_opts()).await;

    const GHOST: u64 = 4_242;
    assert!(!store.join(format!("seg-{GHOST:08}.log")).exists());
    std::fs::write(
        candidate_path(&store, GHOST, "pidx"),
        forged_sidecar(GHOST, 0, 1),
    )
    .expect("plant the orphan");

    let engine = LogEngine::open_with(&store, rolling_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    assert_eq!(health.refuted(), 1);
    assert_eq!(health.refutations[0].segment_id, GHOST);
    assert_eq!(health.refutations[0].reason, RefutationReason::Orphan);
    assert!(health.refutations[0].quarantined);
    assert_eq!(
        health.pending_reseal,
        Vec::<u64>::new(),
        "there is no raw segment to seal"
    );
    assert!(!candidate_path(&store, GHOST, "pidx").exists());
    assert!(quarantined_path(&store, GHOST, "pidx").exists());
    assert_reads_match(&engine, &baseline).await;
    drop(engine);

    // And it is gone for good: the next open sees nothing to judge.
    let engine =
        LogEngine::open_with(&store, rolling_opts()).expect("reopen 2");
    assert_eq!(engine.sealed_candidate_health().refuted(), 0);
}

// ---------------------------------------------------------------------------
// Crash windows
// ---------------------------------------------------------------------------

/// **Kill point: after the quarantine, before the re-seal.** This is the crash
/// the whole design turns on. The candidate is gone, so there is nothing left
/// to refute — a re-seal trigger keyed on "a candidate was refuted this open"
/// would strand the segment on raw scan forever. The durable `*.refuted`
/// marker is what carries the intent across the crash.
#[tokio::test]
async fn a_crash_between_quarantine_and_reseal_still_converges() {
    let d = mess_testkit::sweeping_temp_dir("cand-crash-after-quarantine");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, rolling_opts()).await;

    let victim = a_rolled_sealed_segment(&store, "pidx");
    let sealed_before = count_ext(&sealed_dir(&store), "pidx");
    // Exactly the on-disk state a crash right after the rename leaves.
    for ext in ["filter", "pcol", "reg", "pidx"] {
        let from = candidate_path(&store, victim, ext);
        if from.exists() {
            std::fs::rename(&from, quarantined_path(&store, victim, ext))
                .expect("simulate the completed quarantine");
        }
    }

    let engine = LogEngine::open_with(&store, rolling_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    assert_eq!(
        health.refuted(),
        0,
        "there is no candidate left to refute — only the marker"
    );
    assert_eq!(
        health.pending_reseal,
        vec![victim],
        "the durable marker is what re-queues the seal"
    );
    assert_reads_match(&engine, &baseline).await;

    assert!(await_candidate(&candidate_path(&store, victim, "pidx")));
    drop(engine);
    assert_eq!(count_ext(&sealed_dir(&store), "pidx"), sealed_before);

    let engine =
        LogEngine::open_with(&store, rolling_opts()).expect("reopen 2");
    assert_eq!(engine.sealed_candidate_health().refuted(), 0);
    assert_eq!(
        engine.sealed_candidate_health().pending_reseal,
        Vec::<u64>::new(),
        "the marker is inert once the segment is admitted again"
    );
    assert_eq!(engine.sealed_segment_count(), sealed_before);
    assert_reads_match(&engine, &baseline).await;
}

/// **Kill point: after the fresh sidecar is durable, before its footer.** The
/// seal writes the sidecar strictly before it finalizes the segment footer, so
/// this window leaves a candidate the scan CAN confirm — the segment is served
/// cold again — but that is not the converged state: without a footer the
/// segment is re-scanned in full on every open forever. The marker is still
/// there and the segment is still not footer-verified, so it is enqueued once
/// more and the footer finally lands.
#[tokio::test]
async fn a_crash_before_the_footer_converges_to_a_footer_verified_segment() {
    // `mess-log`'s fixed trailer: removing exactly this many trailing bytes is
    // precisely the "footer never landed" state, and leaves the committed
    // batch bytes untouched.
    const TRAILER: u64 = mess_log::format::SEGMENT_TRAILER_LEN as u64;

    let d = mess_testkit::sweeping_temp_dir("cand-crash-before-footer");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, rolling_opts()).await;

    let victim = a_rolled_sealed_segment(&store, "pidx");
    let sealed_before = count_ext(&sealed_dir(&store), "pidx");
    // The marker an earlier refutation left...
    std::fs::write(quarantined_path(&store, victim, "pidx"), b"refuted bytes")
        .expect("marker");
    // ...and the interrupted re-seal: the sidecar is durable, the footer never
    // reached the device.
    let log = store.join(format!("seg-{victim:08}.log"));
    let len = std::fs::metadata(&log).expect("stat").len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&log)
        .expect("open log")
        .set_len(len - TRAILER)
        .expect("drop the trailer");

    let engine = LogEngine::open_with(&store, rolling_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    assert_eq!(
        health.refuted(),
        0,
        "the fresh candidate is confirmable — it must NOT be refuted"
    );
    assert_eq!(
        health.pending_reseal,
        vec![victim],
        "confirmed but footerless: still owed the footer"
    );
    assert_eq!(
        engine.sealed_segment_count(),
        sealed_before,
        "and served cold in the meantime"
    );
    assert_reads_match(&engine, &baseline).await;
    drop(engine);

    assert_eq!(
        std::fs::metadata(&log).expect("stat").len(),
        len,
        "the re-seal wrote the footer back"
    );

    // Converged: footer-verified, nothing owed, nothing refuted — and it stays
    // that way.
    for round in 0..2 {
        let engine = LogEngine::open_with(&store, rolling_opts())
            .unwrap_or_else(|e| panic!("reopen {round}: {e}"));
        let health = engine.sealed_candidate_health();
        assert_eq!(health.refuted(), 0, "round {round}");
        assert_eq!(
            health.pending_reseal,
            Vec::<u64>::new(),
            "round {round}: the marker is inert once admitted"
        );
        assert_eq!(engine.sealed_segment_count(), sealed_before);
        assert_reads_match(&engine, &baseline).await;
    }
}

/// **Kill point: midway through the quarantine.** The rename order is siblings
/// first, primary last, precisely so a crash in the middle leaves the primary
/// candidate in place: the next open re-classifies it, re-refutes it, and
/// finishes moving the family. A stale `.filter` can never survive to be
/// re-attached to a later re-seal of the same segment.
#[tokio::test]
async fn a_crash_midway_through_the_quarantine_finishes_it() {
    let d = mess_testkit::sweeping_temp_dir("cand-crash-mid-quarantine");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, rolling_opts()).await;

    let victim = a_rolled_sealed_segment(&store, "pidx");
    std::fs::write(
        candidate_path(&store, victim, "pidx"),
        b"torn candidate under its real name",
    )
    .expect("corrupt");
    // The crash: the first sibling moved, the primary did not.
    let filter = candidate_path(&store, victim, "filter");
    let had_filter = filter.exists();
    if had_filter {
        std::fs::rename(&filter, quarantined_path(&store, victim, "filter"))
            .expect("partial quarantine");
    }

    let engine = LogEngine::open_with(&store, rolling_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    assert_eq!(health.refuted(), 1, "the primary was still classifiable");
    assert_eq!(health.refutations[0].reason, RefutationReason::Unparsable);
    assert!(health.refutations[0].quarantined, "the rest of the family moved");
    assert_eq!(health.pending_reseal, vec![victim]);
    for ext in ["pidx", "filter", "pcol"] {
        assert!(
            !candidate_path(&store, victim, ext).exists(),
            "the .{ext} did not survive the resumed quarantine"
        );
    }
    assert_reads_match(&engine, &baseline).await;

    assert!(await_candidate(&candidate_path(&store, victim, "pidx")));
    drop(engine);
    let engine =
        LogEngine::open_with(&store, rolling_opts()).expect("reopen 2");
    assert_eq!(engine.sealed_candidate_health().refuted(), 0);
    assert_reads_match(&engine, &baseline).await;
}

/// **Repeated reopen.** The property the bone exists for: a refuted candidate
/// is evaluated **at most once**, the quarantine slot count never grows with
/// the number of reopens, and the store converges to a successful re-seal and
/// then stays there.
#[tokio::test]
async fn repeated_reopen_judges_a_candidate_once_and_then_converges() {
    let d = mess_testkit::sweeping_temp_dir("cand-repeated-reopen");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, rolling_opts()).await;

    let victim = a_rolled_sealed_segment(&store, "pidx");
    let sealed_before = count_ext(&sealed_dir(&store), "pidx");
    std::fs::write(candidate_path(&store, victim, "pidx"), b"garbage")
        .expect("corrupt");

    let mut refuted_per_open = Vec::new();
    let mut enqueued_per_open = Vec::new();
    let mut quarantine_files = Vec::new();
    for round in 0..5 {
        let engine = LogEngine::open_with(&store, rolling_opts())
            .unwrap_or_else(|e| panic!("reopen {round}: {e}"));
        let health = engine.sealed_candidate_health();
        refuted_per_open.push(health.refuted());
        enqueued_per_open.push(health.reseals_enqueued());
        assert_reads_match(&engine, &baseline).await;
        if round == 0 {
            assert!(await_candidate(&candidate_path(&store, victim, "pidx")));
        }
        drop(engine);
        quarantine_files.push(count_ext(&sealed_dir(&store), "refuted"));
    }

    assert_eq!(
        refuted_per_open,
        vec![1, 0, 0, 0, 0],
        "the candidate is judged on the first open and never again"
    );
    assert_eq!(
        enqueued_per_open,
        vec![1, 0, 0, 0, 0],
        "one re-seal job, not one per reopen"
    );
    assert!(
        quarantine_files[0] > 0,
        "the refuted bytes were preserved, not deleted"
    );
    assert_eq!(
        quarantine_files,
        vec![quarantine_files[0]; 5],
        "the quarantine slot count is bounded, not per-reopen"
    );

    let engine = LogEngine::open_with(&store, rolling_opts()).expect("final");
    assert_eq!(
        engine.sealed_segment_count(),
        sealed_before,
        "converged: the segment is back on the sealed path"
    );
    assert_reads_match(&engine, &baseline).await;
}

/// A **healthy** store must pay nothing for any of this: no refutations, no
/// re-seal jobs, and — the part that matters for open cost — no rolled segment
/// dragged back through the sealer just because it exists.
#[tokio::test]
async fn a_healthy_store_refutes_nothing_and_enqueues_nothing() {
    let d = mess_testkit::sweeping_temp_dir("cand-healthy-store");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, rolling_opts()).await;

    for round in 0..3 {
        let engine = LogEngine::open_with(&store, rolling_opts())
            .unwrap_or_else(|e| panic!("reopen {round}: {e}"));
        let m = engine.metrics();
        assert_eq!(m.sealed_candidates_refuted, 0, "round {round}");
        assert_eq!(m.sealed_candidates_quarantined, 0, "round {round}");
        assert_eq!(m.sealed_candidate_quarantine_failures, 0, "round {round}");
        assert_eq!(m.sealed_reseals_enqueued, 0, "round {round}");
        assert!(engine.sealed_segment_count() > 0, "round {round}: cold tier");
        assert_reads_match(&engine, &baseline).await;
    }
    assert_eq!(
        count_ext(&sealed_dir(&store), "refuted"),
        0,
        "a healthy store never quarantines anything"
    );
}

/// A candidate over the **live head** (the on-demand `seal_active` shape) is
/// quarantined when refuted, but must never be handed to the roll sealer: that
/// would write a footer trailer onto a segment still being appended to.
#[tokio::test]
async fn a_refuted_head_candidate_is_quarantined_but_never_footer_finalized() {
    let d = mess_testkit::sweeping_temp_dir("cand-head-not-finalized");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, rolling_opts()).await;

    let head = *segment_ids(&store).last().expect("a head");
    let head_log = store.join(format!("seg-{head:08}.log"));
    let head_len_before = std::fs::metadata(&head_log).expect("stat").len();
    // A forged candidate over the live head: no footer to cross-check, and its
    // coverage claim is unreachable.
    std::fs::write(
        candidate_path(&store, head, "pidx"),
        forged_sidecar(head, 0, 1_000_000),
    )
    .expect("plant a head candidate");

    let engine = LogEngine::open_with(&store, rolling_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    assert_eq!(health.refuted(), 1);
    assert_eq!(health.refutations[0].segment_id, head);
    assert!(health.refutations[0].quarantined);
    assert_eq!(
        health.pending_reseal,
        Vec::<u64>::new(),
        "the live head is never re-queued: the sealer would footer it"
    );
    assert_reads_match(&engine, &baseline).await;

    // Still appendable, and still no trailer bolted onto the live segment.
    engine
        .append_batch(
            "stream-0",
            Version::At((PER - 1) as u64),
            &[rec("ev", b"after")],
        )
        .await
        .expect("the head is still live");
    drop(engine);
    assert!(
        std::fs::metadata(&head_log).expect("stat").len() > head_len_before,
        "the head kept growing rather than being sealed shut"
    );
}

/// The consolidated **SealPack** (`.seal`) candidate takes the identical
/// lifecycle — one self-contained file, so its quarantine family is just
/// itself, and a `.seal` quarantine can never collide with a `.pidx` one.
///
/// **bn-11g extension.** The re-seal this lifecycle produces is a genuinely
/// NEW pack (rebuilt from the log, not a copy of the refuted bytes), so the
/// footer it writes must name *that* pack's identity — spec 01 §3.3.3. Without
/// that, a converged store would carry a footer naming a pack that no longer
/// exists and would refute itself on every subsequent open, turning bn-30u's
/// convergence into a permanent loop. The assertions below pin the whole
/// before/after: the original footer names the original pack, and the
/// converged footer names the converged one.
#[tokio::test]
async fn a_corrupt_seal_pack_candidate_takes_the_same_lifecycle() {
    fn pack_opts() -> EngineOptions {
        EngineOptions { seal_pack: true, ..rolling_opts() }
    }

    let d = mess_testkit::sweeping_temp_dir("cand-seal-pack");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, pack_opts()).await;
    assert!(count_ext(&sealed_dir(&store), "seal") >= 1, "packs were written");

    let victim = a_rolled_sealed_segment(&store, "seal");
    let packs_before = count_ext(&sealed_dir(&store), "seal");

    // bn-11g: before the damage, the footer names exactly the pack on disk.
    let named_before = named_pack_identity(&store, victim)
        .expect("a pack-mode seal names its pack");
    assert_eq!(
        Some(named_before),
        pack_identity_on_disk(&store, victim),
        "the footer names the pack that is actually there"
    );

    std::fs::write(
        candidate_path(&store, victim, "seal"),
        b"a pack whose whole-pack hash cannot possibly verify",
    )
    .expect("corrupt the pack");

    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    assert_eq!(health.refuted(), 1);
    assert_eq!(health.refutations[0].segment_id, victim);
    assert_eq!(health.refutations[0].reason, RefutationReason::Unparsable);
    assert!(health.refutations[0].quarantined);
    assert_eq!(health.pending_reseal, vec![victim]);
    assert!(quarantined_path(&store, victim, "seal").exists());
    assert_reads_match(&engine, &baseline).await;

    assert!(await_candidate(&candidate_path(&store, victim, "seal")));
    drop(engine);
    assert_eq!(count_ext(&sealed_dir(&store), "seal"), packs_before);

    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen 2");
    assert_eq!(engine.sealed_candidate_health().refuted(), 0);
    assert_eq!(engine.sealed_segment_count(), packs_before);
    assert_reads_match(&engine, &baseline).await;

    // bn-11g: the converged footer names the CONVERGED pack. It is a different
    // pack from the one that was refuted — rebuilt from the log — so a footer
    // left pointing at the old identity would refute this store forever.
    let named_after = named_pack_identity(&store, victim)
        .expect("the re-seal names its pack");
    assert_eq!(
        Some(named_after),
        pack_identity_on_disk(&store, victim),
        "the re-sealed footer names the re-sealed pack"
    );
    assert_ne!(
        named_after, named_before,
        "the re-seal is a new pack, so the name had to be rewritten with it"
    );
}

/// Several candidates refuted in one open: each is judged once, each is
/// quarantined, each rolled segment is queued exactly once, and the health
/// record names every one of them.
#[tokio::test]
async fn several_refuted_candidates_in_one_open_all_converge() {
    let d = mess_testkit::sweeping_temp_dir("cand-many-in-one-open");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, rolling_opts()).await;

    let ids = segment_ids(&store);
    let head = *ids.last().expect("head");
    let victims: Vec<u64> = ids
        .iter()
        .copied()
        .filter(|&id| id != head && candidate_path(&store, id, "pidx").exists())
        .collect();
    assert!(victims.len() >= 2, "need several rolled candidates: {ids:?}");
    let sealed_before = count_ext(&sealed_dir(&store), "pidx");
    for &v in &victims {
        std::fs::write(candidate_path(&store, v, "pidx"), b"nope")
            .expect("corrupt");
    }

    let engine = LogEngine::open_with(&store, rolling_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    assert_eq!(health.refuted(), victims.len() as u64);
    assert_eq!(health.quarantined(), victims.len() as u64);
    let named: HashSet<u64> =
        health.refutations.iter().map(|r| r.segment_id).collect();
    assert_eq!(named, victims.iter().copied().collect::<HashSet<_>>());
    let queued: HashSet<u64> = health.pending_reseal.iter().copied().collect();
    assert_eq!(queued, named, "one job per refuted rolled segment");
    assert_reads_match(&engine, &baseline).await;

    for &v in &victims {
        assert!(await_candidate(&candidate_path(&store, v, "pidx")));
    }
    drop(engine);

    let engine =
        LogEngine::open_with(&store, rolling_opts()).expect("reopen 2");
    assert_eq!(engine.sealed_candidate_health().refuted(), 0);
    assert_eq!(engine.sealed_segment_count(), sealed_before);
    assert_reads_match(&engine, &baseline).await;
}
