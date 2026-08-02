//! bn-3qh0 — the **offline re-seal request**: how a segment whose pack is
//! simply *gone* climbs back onto the sealed path.
//!
//! bn-30u made the engine converge from every candidate it can *judge*: a
//! pack that is corrupt, stale, or substituted is refuted, quarantined, and
//! re-sealed. The one state it cannot see is the artifact that is not there.
//! With no candidate nothing is refuted, so no marker is written, so
//! `owed_reseal` stays empty and the rolled segment is never re-queued — it is
//! served correctly from the log forever and never regains its cold tier.
//! `never_reseals_a_deleted_pack_on_its_own` pins that as the deliberate
//! baseline, because it is exactly what
//! [`withdraw_sealed_index`](mess_store::withdraw_sealed_index) exists to fix
//! and what `mess rebuild-index` calls it for.
//!
//! Everything here runs against a REAL rolled store with a REAL sealer: no
//! mocks, no fault-injecting filesystem. Every test re-reads the whole store
//! byte-exact at every step, because the entire justification for withdrawing
//! a sealed index is that the log is authority.

use std::path::{Path, PathBuf};

use mess_store::backend::{Backend, RecordToAppend, StoredRecord};
use mess_store::{EngineOptions, LogEngine, Version, withdraw_sealed_index};

const STREAMS: usize = 8;
const PER: usize = 45;

fn rec(message_type: &str, data: &[u8]) -> RecordToAppend {
    RecordToAppend {
        message_type: message_type.to_string(),
        data:         data.to_vec(),
    }
}

fn payload(s: usize, i: usize) -> Vec<u8> {
    format!("s{s:03}-e{i:04}").into_bytes()
}

/// A tiny active segment so a few hundred small batches roll several times and
/// the background sealer produces real rolled, pack-sealed segments.
fn pack_opts() -> EngineOptions {
    EngineOptions {
        segment_size: 16 * 1024,
        seal_pack: true,
        ..EngineOptions::default()
    }
}

fn sealed_dir(store: &Path) -> PathBuf { store.join("sealed") }

fn pack_path(store: &Path, seg_id: u64) -> PathBuf {
    sealed_dir(store).join(format!("seg-{seg_id:020}.seal"))
}

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

/// A rolled (non-head) segment that really carries a `.seal` — the one the
/// tests take away. The live head is excluded on purpose: it is still being
/// appended to and is never re-queued (see `sealed_candidate`'s module docs).
fn a_rolled_pack_segment(store: &Path) -> u64 {
    let ids = segment_ids(store);
    assert!(ids.len() >= 2, "the corpus must have rolled: {ids:?}");
    let head = *ids.last().expect("non-empty");
    ids.iter()
        .copied()
        .filter(|&id| id != head)
        .find(|&id| pack_path(store, id).exists())
        .expect("a rolled segment carries a .seal")
}

/// bn-11g: the identity the segment's footer NAMES, or `None` for a footer
/// that names none.
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

/// What the pack on disk actually hashes to — derived independently of the
/// footer, which is the whole point of comparing the two.
fn pack_identity_on_disk(store: &Path, seg: u64) -> Option<[u8; 32]> {
    mess_index::sealed::SealedSegmentIndex::open_pack_eager(&pack_path(
        store, seg,
    ))
    .ok()?
    .pack_identity()
    .map(|i| *i.as_bytes())
}

async fn build_rolled_pack_store(store: &Path) -> Vec<StoredRecord> {
    let engine = LogEngine::open_with(store, pack_opts()).expect("open fresh");
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
    drop(engine); // drains the sealer: packs + footers durable
    baseline
}

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
    }
}

// ---------------------------------------------------------------------------

/// **The baseline the API exists for.** A rolled segment whose `.seal` was
/// deleted is served correctly from the log and is *never* re-sealed by the
/// engine alone, however many times the store is reopened: there is no
/// candidate, so nothing is refuted, so nothing is owed.
#[tokio::test]
async fn never_reseals_a_deleted_pack_on_its_own() {
    let d = mess_testkit::sweeping_temp_dir("offline-reseal-baseline");
    let store = d.path().join("store");
    let baseline = build_rolled_pack_store(&store).await;

    let victim = a_rolled_pack_segment(&store);
    std::fs::remove_file(pack_path(&store, victim)).expect("delete the pack");

    for round in 0..3 {
        let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen");
        let health = engine.sealed_candidate_health();
        assert_eq!(health.refuted(), 0, "round {round}: nothing to refute");
        assert_eq!(
            health.pending_reseal,
            Vec::<u64>::new(),
            "round {round}: and therefore nothing re-queued"
        );
        assert_reads_match(&engine, &baseline).await;
        drop(engine);
        assert!(
            !pack_path(&store, victim).exists(),
            "round {round}: the pack stays gone — the segment is permanently \
             demoted to raw scan without an explicit request"
        );
    }
}

/// The fix: an offline withdrawal over a segment with **no artifact at all**
/// puts the durable request in the quarantine slot, and the next open rebuilds
/// the pack from the log and re-finalizes the footer to name the fresh one.
#[tokio::test]
async fn a_withdrawn_absent_pack_is_rebuilt_at_the_next_open() {
    let d = mess_testkit::sweeping_temp_dir("offline-reseal-absent");
    let store = d.path().join("store");
    let baseline = build_rolled_pack_store(&store).await;

    let victim = a_rolled_pack_segment(&store);
    let identity_before =
        pack_identity_on_disk(&store, victim).expect("identity");
    assert_eq!(
        named_pack_identity(&store, victim),
        Some(identity_before),
        "the healthy store binds footer to pack (bn-11g)"
    );
    std::fs::remove_file(pack_path(&store, victim)).expect("delete the pack");

    let w = withdraw_sealed_index(
        &sealed_dir(&store),
        victim,
        "pack-named-but-absent",
    )
    .expect("withdraw");
    assert_eq!(w.as_str(), "marked", "nothing existed to preserve");
    assert!(w.marker.exists());
    // Nothing outside `sealed/` moved: the footer still names the pack that
    // is not there, which is a miss and not an error.
    assert_eq!(named_pack_identity(&store, victim), Some(identity_before));

    // ---- The next open converges. ----
    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen");
    assert_eq!(
        engine.sealed_candidate_health().pending_reseal,
        vec![victim],
        "the request re-queued exactly this segment"
    );
    assert_reads_match(&engine, &baseline).await;
    drop(engine); // joins the sealer

    let back = pack_path(&store, victim);
    assert!(back.exists(), "the pack is back at {}", back.display());
    let identity_after =
        pack_identity_on_disk(&store, victim).expect("identity after");
    assert_eq!(
        named_pack_identity(&store, victim),
        Some(identity_after),
        "the footer was re-finalized to name the pack now on disk"
    );

    // ---- And it is admitted, with no oscillation. ----
    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen 2");
    let health = engine.sealed_candidate_health();
    assert_eq!(health.refuted(), 0, "the fresh pack is not refuted");
    assert_eq!(
        health.pending_reseal,
        Vec::<u64>::new(),
        "the request is inert once the segment is admitted again"
    );
    assert_reads_match(&engine, &baseline).await;
}

/// A withdrawal over a pack that IS on disk preserves its bytes in the slot
/// (quarantine renames, never deletes) and still converges.
#[tokio::test]
async fn a_withdrawn_present_pack_is_preserved_and_rebuilt() {
    let d = mess_testkit::sweeping_temp_dir("offline-reseal-present");
    let store = d.path().join("store");
    let baseline = build_rolled_pack_store(&store).await;

    let victim = a_rolled_pack_segment(&store);
    let bytes_before =
        std::fs::read(pack_path(&store, victim)).expect("pack bytes");

    let w = withdraw_sealed_index(&sealed_dir(&store), victim, "forced")
        .expect("withdraw");
    assert_eq!(w.as_str(), "quarantined");
    assert!(!pack_path(&store, victim).exists(), "left the namespace");
    assert_eq!(
        std::fs::read(&w.marker).expect("read slot"),
        bytes_before,
        "the withdrawn pack is preserved, not deleted"
    );

    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen");
    assert_eq!(engine.sealed_candidate_health().pending_reseal, vec![victim]);
    assert_reads_match(&engine, &baseline).await;
    drop(engine);
    assert!(pack_path(&store, victim).exists(), "re-sealed");

    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen 2");
    assert_eq!(engine.sealed_candidate_health().refuted(), 0);
    assert_reads_match(&engine, &baseline).await;
}

/// Crash between step 1 (the durable marker) and step 2 (the rename): the
/// pack is still healthy and still in the candidate namespace. The next open
/// admits it and the request goes inert — no re-seal, no refutation, no
/// oscillation — and re-running the withdrawal finishes the job.
#[tokio::test]
async fn a_request_over_a_still_healthy_pack_is_inert() {
    let d = mess_testkit::sweeping_temp_dir("offline-reseal-inert");
    let store = d.path().join("store");
    let baseline = build_rolled_pack_store(&store).await;

    let victim = a_rolled_pack_segment(&store);
    let identity = pack_identity_on_disk(&store, victim).expect("identity");
    // Step 1 only — the crash window.
    std::fs::write(
        sealed_dir(&store).join(format!("seg-{victim:020}.seal.refuted")),
        format!("{}\nsegment={victim}\n", mess_store::WITHDRAWAL_MARKER_BANNER),
    )
    .expect("marker");

    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    assert_eq!(health.refuted(), 0, "the healthy pack is still admitted");
    assert_eq!(
        health.pending_reseal,
        Vec::<u64>::new(),
        "an admitted segment is never re-queued, marker or no marker"
    );
    assert_reads_match(&engine, &baseline).await;
    drop(engine);
    assert_eq!(
        pack_identity_on_disk(&store, victim),
        Some(identity),
        "and the pack was not touched"
    );

    // Re-running the withdrawal completes it, without clobbering the marker.
    let w = withdraw_sealed_index(&sealed_dir(&store), victim, "forced")
        .expect("resume");
    assert!(!w.created, "the marker was already there");
    assert!(w.quarantined.is_some(), "and now the pack moved too");
    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen 2");
    assert_eq!(engine.sealed_candidate_health().pending_reseal, vec![victim]);
    assert_reads_match(&engine, &baseline).await;
    drop(engine);
    assert!(pack_path(&store, victim).exists(), "re-sealed");
}

/// Repeating the whole cycle converges and never grows the sealed directory:
/// one quarantine slot per (segment, kind), whatever happened before.
#[tokio::test]
async fn repeated_withdrawals_stay_bounded() {
    let d = mess_testkit::sweeping_temp_dir("offline-reseal-bounded");
    let store = d.path().join("store");
    let baseline = build_rolled_pack_store(&store).await;
    let victim = a_rolled_pack_segment(&store);

    let count_refuted = || {
        std::fs::read_dir(sealed_dir(&store))
            .expect("read_dir")
            .flatten()
            .filter(|e| {
                e.path().extension().and_then(|x| x.to_str()) == Some("refuted")
            })
            .count()
    };

    for round in 0..3 {
        withdraw_sealed_index(&sealed_dir(&store), victim, "forced")
            .expect("withdraw");
        assert_eq!(count_refuted(), 1, "round {round}: exactly one slot");
        let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen");
        assert_reads_match(&engine, &baseline).await;
        drop(engine);
        assert!(
            pack_path(&store, victim).exists(),
            "round {round}: re-sealed again"
        );
        assert_eq!(count_refuted(), 1, "round {round}: still one slot");
    }
}
