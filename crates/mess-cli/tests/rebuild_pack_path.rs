//! bn-3qh0 acceptance: `mess rebuild-index` on **pack-sealed** segments.
//!
//! bn-1w4h taught the command to refuse writing a refutable `.pidx` over a
//! pack-sealed segment, and bn-ccx1 then made pack sealing the engine default —
//! so on any freshly written store `rebuild-index` did nothing at all. These
//! tests pin the closure of that regression end to end against a REAL rolled,
//! pack-sealed store: run the command, reopen the engine, and prove the pack
//! is back, footer-bound, with every read byte-exact at every step.
//!
//! The refusal itself is deliberately still here (`a_healthy_pack_is_refused`)
//! — bn-1w4h's judgement about a *working* pack has not changed. What changed
//! is that a broken one is now repaired instead of merely reported.

use std::path::{Path, PathBuf};

use mess_cli::rebuild::{self, RebuildOptions};
use mess_cli::report::{Report, Severity};
use mess_cli::store;
use mess_store::backend::{Backend, RecordToAppend, StoredRecord};
use mess_store::{EngineOptions, LogEngine, Version};

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

/// Small segments so the corpus really rolls: the tests need a rolled,
/// pack-sealed, footered segment (the live head is never re-queued).
fn pack_opts() -> EngineOptions {
    EngineOptions {
        segment_size: 16 * 1024,
        seal_pack: true,
        ..EngineOptions::default()
    }
}

async fn build_rolled_pack_store(dir: &Path) -> Vec<StoredRecord> {
    let engine = LogEngine::open_with(dir, pack_opts()).expect("open fresh");
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

/// Reopen and prove every read path still agrees with the baseline, byte for
/// byte. The whole justification for withdrawing a sealed index is that the
/// log is authority, so every test re-proves it.
async fn assert_store_reads_match(dir: &Path, baseline: &[StoredRecord]) {
    let engine = LogEngine::open_with(dir, pack_opts()).expect("reopen");
    let g =
        engine.read_global(None, STREAMS * PER * 2).await.expect("global read");
    assert_eq!(g.len(), baseline.len(), "global event count");
    for (got, want) in g.iter().zip(baseline) {
        assert_eq!(got.stream_id, want.stream_id);
        assert_eq!(got.message_type, want.message_type);
        assert_eq!(got.data, want.data, "payload bytes");
        assert_eq!(got.global_position, want.global_position);
    }
    for s in 0..STREAMS {
        let name = format!("stream-{s}");
        let evs = engine
            .read_stream(&name, Version::NoStream, PER * 2)
            .await
            .expect("stream read");
        assert_eq!(evs.len(), PER, "{name}: every event served");
        for (i, e) in evs.iter().enumerate() {
            assert_eq!(e.data, payload(s, i), "{name} event {i}");
        }
    }
    drop(engine); // also drains any re-seal this open enqueued
}

fn segment_ids(dir: &Path) -> Vec<u64> {
    store::discover_segments(dir).iter().map(|s| s.segment_id).collect()
}

/// A rolled (non-head) segment that carries a `.seal`. The live head is
/// excluded: it is still being appended to and is never re-queued.
fn a_rolled_pack_segment(dir: &Path) -> u64 {
    let ids = segment_ids(dir);
    assert!(ids.len() >= 2, "the corpus must have rolled: {ids:?}");
    let head = *ids.last().expect("non-empty");
    ids.iter()
        .copied()
        .filter(|&id| id != head)
        .find(|&id| store::seal_path(dir, id).exists())
        .expect("a rolled segment carries a .seal")
}

fn marker_path(dir: &Path, seg: u64) -> PathBuf {
    dir.join("sealed").join(format!("seg-{seg:020}.seal.refuted"))
}

fn pack_identity(dir: &Path, seg: u64) -> Option<String> {
    mess_index::sealed::SealedSegmentIndex::open_pack_eager(&store::seal_path(
        dir, seg,
    ))
    .ok()?
    .pack_identity()
    .map(|i| i.hex())
}

/// bn-11g: the identity the segment footer names.
fn named_identity(dir: &Path, seg: u64) -> Option<String> {
    use mess_log::footer_ext::{decode_extension, hex32};
    use mess_log::runtime::real::RealFs;
    use mess_log::sealer::{read_extension, read_trailer};

    let log = store::log_path(dir, seg);
    let cat = read_trailer(&RealFs, &log).ok().flatten()?;
    if !cat.names_seal_pack() {
        return None;
    }
    let ext = read_extension(&RealFs, &log, &cat).ok().flatten()?;
    decode_extension(&ext).pack_identity.map(|n| hex32(&n.identity))
}

fn finding<'a>(
    report: &'a Report,
    kind: &str,
) -> Option<&'a mess_cli::report::Finding> {
    report.findings.iter().find(|f| f.kind == kind)
}

fn all_log_bytes(dir: &Path) -> Vec<(u64, Vec<u8>)> {
    store::discover_segments(dir)
        .iter()
        .map(|s| (s.segment_id, std::fs::read(&s.log_path).expect("read .log")))
        .collect()
}

// ---------------------------------------------------------------------------

/// **The regression, closed.** A rolled segment whose `.seal` was lost gets a
/// durable re-seal request from `rebuild-index`, and the next engine open puts
/// a fresh, footer-bound pack back on disk. Before bn-3qh0 the command
/// reported `pack-sealed-segment-skipped` and the segment stayed demoted to
/// raw scan forever.
#[tokio::test]
async fn a_lost_pack_is_rebuilt_via_the_next_open() {
    let d = mess_testkit::sweeping_temp_dir("cli-rebuild-pack-lost");
    let dir = d.path();
    let baseline = build_rolled_pack_store(dir).await;

    let victim = a_rolled_pack_segment(dir);
    let named_before =
        named_identity(dir, victim).expect("footer names a pack");
    std::fs::remove_file(store::seal_path(dir, victim)).expect("lose the pack");

    let report = rebuild::run(dir, &RebuildOptions::default());
    assert_eq!(report.exit_code(), 0, "{:#?}", report.findings);

    let f =
        finding(&report, "reseal-pending").expect("the request is reported");
    assert_eq!(f.severity, Severity::Warn);
    assert_eq!(f.fields["pack_state"], "pack-named-but-absent");
    assert_eq!(f.fields["fallback"], "raw-segment-scan");
    assert!(
        f.message.contains("NEXT ENGINE OPEN"),
        "the deferred completion must be explicit: {}",
        f.message
    );
    let row = report
        .collection
        .iter()
        .find(|r| r["segment_id"] == victim)
        .expect("a row for the segment");
    assert_eq!(row["shape"], "seal-pack");
    assert_eq!(row["withdrawal"], "marked", "nothing existed to preserve");
    assert_eq!(row["written"], true);
    assert!(marker_path(dir, victim).exists(), "the request is durable");

    // Offline, before any open: the segment still recovers wholly from the log,
    // and the footer still names the pack that is not there (a miss, not an
    // error — the withdrawal touched nothing outside sealed/).
    let scan =
        mess_cli::scan::scan_segment(victim, &store::log_path(dir, victim))
            .expect("scan");
    assert!(scan.batch_count() > 0, "the raw log still serves the segment");
    assert_eq!(
        named_identity(dir, victim).as_deref(),
        Some(named_before.as_str())
    );

    // ---- The next open completes the rebuild. ----
    assert_store_reads_match(dir, &baseline).await;
    let back = store::seal_path(dir, victim);
    assert!(back.exists(), "the pack is back: {}", back.display());
    let fresh =
        pack_identity(dir, victim).expect("a rebuilt pack has identity");
    assert_eq!(
        named_identity(dir, victim).as_deref(),
        Some(fresh.as_str()),
        "the footer was re-finalized to name the pack now on disk"
    );

    // And the store is healthy again: nothing to rebuild, nothing refuted.
    let after = rebuild::run(dir, &RebuildOptions::default());
    assert!(finding(&after, "pack-sealed-segment-skipped").is_some());
    assert!(finding(&after, "reseal-pending").is_none());
}

/// `--reseal` finishes the job inside the one invocation: the command exits
/// with the pack already back on disk and reports its fresh identity.
#[tokio::test]
async fn reseal_flag_finishes_the_rebuild_in_one_command() {
    let d = mess_testkit::sweeping_temp_dir("cli-rebuild-pack-reseal");
    let dir = d.path();
    let baseline = build_rolled_pack_store(dir).await;

    let victim = a_rolled_pack_segment(dir);
    std::fs::remove_file(store::seal_path(dir, victim)).expect("lose the pack");

    let report = rebuild::run(
        dir,
        &RebuildOptions { reseal: true, ..Default::default() },
    );
    assert_eq!(report.exit_code(), 0, "{:#?}", report.findings);

    let f = finding(&report, "pack-rebuilt").expect("the rebuild is reported");
    assert_eq!(f.severity, Severity::Ok);
    assert_eq!(f.fields["segment_id"], victim);
    assert!(
        finding(&report, "reseal-pending").is_none(),
        "nothing is pending once this run completed it"
    );

    // The pack really is on disk when the command returns — no reopen needed.
    let fresh = pack_identity(dir, victim).expect("pack back");
    assert_eq!(f.fields["identity"], fresh);
    assert_eq!(
        named_identity(dir, victim).as_deref(),
        Some(fresh.as_str()),
        "and the footer names it"
    );

    assert_store_reads_match(dir, &baseline).await;
}

/// bn-1w4h preserved: a pack that opens and matches the identity its footer
/// names is left completely alone. `--force` is the explicit opt-in that
/// withdraws it anyway.
#[tokio::test]
async fn a_healthy_pack_is_refused_and_force_withdraws_it() {
    let d = mess_testkit::sweeping_temp_dir("cli-rebuild-pack-healthy");
    let dir = d.path();
    let baseline = build_rolled_pack_store(dir).await;
    let victim = a_rolled_pack_segment(dir);
    let before = std::fs::read(store::seal_path(dir, victim)).expect("pack");

    // Default: refuse, write nothing.
    let report = rebuild::run(dir, &RebuildOptions::default());
    let f = finding(&report, "pack-sealed-segment-skipped").expect("refused");
    assert_eq!(f.fields["pack_state"], "pack-healthy");
    assert!(f.message.contains("Re-seal"), "{}", f.message);
    assert!(f.message.contains("--force"), "the escape hatch: {}", f.message);
    assert!(
        !report.collection.iter().any(|r| r["shape"] == "seal-pack"),
        "no pack was touched: {:#?}",
        report.collection
    );
    assert!(!marker_path(dir, victim).exists(), "no request was recorded");
    assert_eq!(
        std::fs::read(store::seal_path(dir, victim)).expect("pack"),
        before,
        "the pack itself is untouched"
    );
    assert!(!store::pidx_path(dir, victim).exists(), "and no legacy sidecar");

    // --force: withdraw it, preserving the bytes.
    let report = rebuild::run(
        dir,
        &RebuildOptions { force: true, ..Default::default() },
    );
    let row = report
        .collection
        .iter()
        .find(|r| r["segment_id"] == victim)
        .expect("a row");
    assert_eq!(row["reason"], "forced");
    assert_eq!(row["withdrawal"], "quarantined");
    assert!(!store::seal_path(dir, victim).exists());
    assert_eq!(
        std::fs::read(marker_path(dir, victim)).expect("slot"),
        before,
        "quarantine renames, never deletes"
    );

    assert_store_reads_match(dir, &baseline).await;
    assert!(store::seal_path(dir, victim).exists(), "and it is re-sealed");
}

/// A corrupt pack is withdrawn without `--force` — the engine would refute and
/// quarantine it at the next open anyway; doing it here makes the repair
/// explicit and lets `--reseal` converge in one command.
#[tokio::test]
async fn a_corrupt_pack_is_withdrawn_and_preserved() {
    let d = mess_testkit::sweeping_temp_dir("cli-rebuild-pack-corrupt");
    let dir = d.path();
    let baseline = build_rolled_pack_store(dir).await;
    let victim = a_rolled_pack_segment(dir);
    std::fs::write(store::seal_path(dir, victim), b"not a pack at all")
        .expect("corrupt");

    let report = rebuild::run(
        dir,
        &RebuildOptions { reseal: true, ..Default::default() },
    );
    assert_eq!(report.exit_code(), 0, "{:#?}", report.findings);
    let row = report
        .collection
        .iter()
        .find(|r| r["segment_id"] == victim)
        .expect("a row");
    assert_eq!(row["pack_state"], "pack-unreadable");
    assert_eq!(row["withdrawal"], "quarantined");
    assert_eq!(
        std::fs::read(marker_path(dir, victim)).expect("slot"),
        b"not a pack at all",
        "the corrupt bytes are preserved as evidence"
    );
    assert!(finding(&report, "pack-rebuilt").is_some(), "and re-sealed");
    assert_store_reads_match(dir, &baseline).await;
}

/// Mid-migration shape: a segment carrying BOTH a `.seal` and a legacy
/// `.pidx`. Withdrawing the pack must take the sidecar with it — while the
/// pack served, bn-3of's dual read never opened the sidecar, but the moment
/// the pack leaves the candidate namespace the sidecar becomes the primary
/// candidate, and an admitted sidecar is exactly what suppresses the re-seal.
#[tokio::test]
async fn a_shadowed_sidecar_is_withdrawn_with_its_pack() {
    let d = mess_testkit::sweeping_temp_dir("cli-rebuild-pack-shadowed");
    let dir = d.path();
    let baseline = build_rolled_pack_store(dir).await;
    let victim = a_rolled_pack_segment(dir);

    // Plant a structurally valid legacy sidecar alongside the pack.
    let scan =
        mess_cli::scan::scan_segment(victim, &store::log_path(dir, victim))
            .expect("scan");
    let header = scan.recovery.header.as_ref().expect("header");
    let bytes = rebuild::rebuild_sidecar_bytes(
        header.segment_id,
        header.base_pos,
        &scan.recovery.accepted,
    );
    let pidx = store::pidx_path(dir, victim);
    std::fs::write(&pidx, &bytes).expect("plant the sidecar");

    let report = rebuild::run(
        dir,
        &RebuildOptions { force: true, ..Default::default() },
    );
    let row = report
        .collection
        .iter()
        .find(|r| r["segment_id"] == victim && r["shape"] == "seal-pack")
        .expect("a row");
    assert_eq!(row["pidx_quarantined"], true);
    assert!(!pidx.exists(), "the sidecar left the candidate namespace");
    assert!(
        pidx.with_file_name(format!("seg-{victim:020}.pidx.refuted")).exists(),
        "and landed in its own quarantine slot, distinct from the pack's"
    );

    assert_store_reads_match(dir, &baseline).await;
    assert!(store::seal_path(dir, victim).exists(), "the pack was rebuilt");
}

/// The pack path never touches a `.log` byte. The footer is the engine's to
/// re-finalize; an offline tool that rewrote it would be mutating the sole
/// authority.
#[tokio::test]
async fn the_offline_pack_path_never_writes_to_the_log() {
    let d = mess_testkit::sweeping_temp_dir("cli-rebuild-pack-log-intact");
    let dir = d.path();
    let _ = build_rolled_pack_store(dir).await;
    let victim = a_rolled_pack_segment(dir);
    std::fs::remove_file(store::seal_path(dir, victim)).expect("lose the pack");

    let before = all_log_bytes(dir);
    let report = rebuild::run(
        dir,
        &RebuildOptions { force: true, ..Default::default() },
    );
    assert_eq!(report.exit_code(), 0, "{:#?}", report.findings);
    assert_eq!(all_log_bytes(dir), before, "every .log byte is unchanged");
}

/// `--dry-run` reports the withdrawals it would make and writes nothing.
#[tokio::test]
async fn dry_run_records_nothing() {
    let d = mess_testkit::sweeping_temp_dir("cli-rebuild-pack-dry");
    let dir = d.path();
    let _ = build_rolled_pack_store(dir).await;
    let victim = a_rolled_pack_segment(dir);
    std::fs::remove_file(store::seal_path(dir, victim)).expect("lose the pack");

    let report = rebuild::run(
        dir,
        &RebuildOptions { dry_run: true, reseal: true, ..Default::default() },
    );
    let row = report
        .collection
        .iter()
        .find(|r| r["segment_id"] == victim)
        .expect("a row");
    assert_eq!(row["action"], "withdraw-sealed-index");
    assert_eq!(row["written"], false);
    assert!(!marker_path(dir, victim).exists(), "nothing was written");
    assert!(
        finding(&report, "pack-rebuilt").is_none(),
        "--reseal is inert under --dry-run"
    );
}

/// Idempotent: running the command repeatedly converges to the same on-disk
/// state and never grows the sealed directory.
#[tokio::test]
async fn the_pack_path_is_idempotent() {
    let d = mess_testkit::sweeping_temp_dir("cli-rebuild-pack-idempotent");
    let dir = d.path();
    let baseline = build_rolled_pack_store(dir).await;
    let victim = a_rolled_pack_segment(dir);
    std::fs::remove_file(store::seal_path(dir, victim)).expect("lose the pack");

    let count_refuted = || {
        std::fs::read_dir(dir.join("sealed"))
            .expect("read_dir")
            .flatten()
            .filter(|e| {
                e.path().extension().and_then(|x| x.to_str()) == Some("refuted")
            })
            .count()
    };

    for round in 0..3 {
        let report = rebuild::run(dir, &RebuildOptions::default());
        assert_eq!(report.exit_code(), 0, "round {round}");
        assert_eq!(count_refuted(), 1, "round {round}: exactly one slot");
    }
    assert_store_reads_match(dir, &baseline).await;
    assert!(store::seal_path(dir, victim).exists(), "still converges");
    assert_eq!(count_refuted(), 1, "and the slot count never grew");
}
