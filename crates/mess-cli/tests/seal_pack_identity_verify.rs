//! bn-11g acceptance: `mess verify` reports the segment footer's **SealPack
//! identity** binding — expected vs observed, and the fallback a reader would
//! take (spec 01 §3.3.3).
//!
//! `verify` is the offline half of the trust chain: the engine's `load_sealed`
//! makes the same decision at open, but an operator needs to learn *before* a
//! reopen that a segment is about to lose its cold tier, and why. Every `Error`
//! below corresponds to a candidate the next open would refuse and quarantine.
//! None of them is data loss — the `fallback` field says so explicitly, and the
//! raw log answers every read either way (D1).

use std::path::Path;

use mess_cli::report::{Finding, Report, Severity};
use mess_cli::verify::{self, VerifyOptions};
use mess_cli::{scan, store};
use mess_index::sealed::SealedSegmentIndex;
use mess_log::footer_ext::{
    SealPackIdentity, SealSummary, encode_sealed_footer,
};
use mess_log::format::{EXT_SECTION_HDR_LEN, SEAL_PACK_IDENTITY_HDRDIR_BLAKE3};
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EngineOptions, LogEngine, Version};
use serde_json::json;

const SEG_ID: u64 = 1;

fn tmp() -> mess_testkit::SweepingTempDir {
    mess_testkit::sweeping_temp_dir("cli-seal-pack-identity")
}

/// A one-segment pack-mode store: append a handful of batches, then
/// `seal_active` so `sealed/seg-1.seal` exists. Leaves the segment footerless —
/// the footer is written by the helpers below, which is exactly the knob these
/// tests need.
fn build_pack_corpus(dir: &Path, n_batches: u64) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let engine = LogEngine::open_with(
            dir,
            EngineOptions {
                segment_size: 1 << 20,
                seal_pack: true,
                ..EngineOptions::default()
            },
        )
        .expect("open engine");
        for i in 0..n_batches {
            let expected =
                if i == 0 { Version::NoStream } else { Version::At(i - 1) };
            engine
                .append_batch(
                    "acct-1",
                    expected,
                    &[RecordToAppend {
                        message_type: "account.happened".into(),
                        data:         format!("event-{i}").into_bytes(),
                    }],
                )
                .await
                .expect("append");
        }
        engine.seal_active().expect("seal");
    });
}

/// The identity the pack on disk hashes to — what a reader derives.
fn pack_identity(dir: &Path) -> [u8; 32] {
    *SealedSegmentIndex::open_pack_eager(&store::seal_path(dir, SEG_ID))
        .expect("pack opens")
        .pack_identity()
        .expect("a pack carries an identity")
        .as_bytes()
}

/// Write the segment's footer. `pack` is the identity to name, or `None` for
/// the legacy (pre-bn-11g) footer that names nothing.
fn write_footer(dir: &Path, pack: Option<[u8; 32]>) {
    use std::io::{Seek, SeekFrom, Write};

    let log = store::log_path(dir, SEG_ID);
    let s = scan::scan_segment(SEG_ID, &log).expect("scan");
    let content_len = s.recovery.safe_offset;
    let summary = SealSummary {
        segment_id: SEG_ID,
        epoch: s.epoch().expect("epoch"),
        base_pos: s.base_pos().expect("base_pos"),
        batch_count: s.batch_count() as u64,
        event_count: s.event_count(),
        content_len,
    };
    let identity = pack.map(|identity| SealPackIdentity {
        identity_kind: SEAL_PACK_IDENTITY_HDRDIR_BLAKE3,
        pack_format_version: mess_index::sealed::PACK_FORMAT_VERSION,
        segment_id: SEG_ID,
        identity,
    });
    let (footer, _) =
        encode_sealed_footer(&summary, &[], &[], identity.as_ref());

    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&log)
        .expect("open log");
    f.set_len(content_len).expect("truncate to content");
    f.seek(SeekFrom::Start(content_len)).expect("seek");
    f.write_all(&footer).expect("write footer");
    f.sync_all().expect("sync");
}

fn run(dir: &Path) -> Report { verify::run(dir, &VerifyOptions::default()) }

fn finding<'a>(report: &'a Report, kind: &str) -> Option<&'a Finding> {
    report.findings.iter().find(|f| f.kind == kind)
}

fn field(f: &Finding, key: &str) -> serde_json::Value {
    f.fields.get(key).cloned().unwrap_or(serde_json::Value::Null)
}

fn hex(id: &[u8; 32]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

// ---------------------------------------------------------------------------

/// A correctly bound segment: `verify` reports the match, with expected and
/// observed both present so a machine consumer can diff them, and the report
/// stays clean.
#[test]
fn a_matching_identity_is_reported_ok() {
    let d = tmp();
    build_pack_corpus(d.path(), 6);
    let id = pack_identity(d.path());
    write_footer(d.path(), Some(id));

    let report = run(d.path());
    let f = finding(&report, "seal-pack-identity-verified")
        .expect("a verified binding is reported");
    assert_eq!(f.severity, Severity::Ok);
    assert_eq!(field(f, "expected_identity"), json!(hex(&id)));
    assert_eq!(field(f, "observed_identity"), json!(hex(&id)));
    assert!(report.worst() < Severity::Error, "clean store");
    assert_eq!(report.exit_code(), 0);
}

/// **Expected vs observed on a substituted pack.** The pack is replaced by a
/// same-coverage forgery; `verify` names both identities and the fallback, and
/// forces a non-zero exit.
#[test]
fn a_mismatched_identity_reports_expected_and_observed() {
    let d = tmp();
    build_pack_corpus(d.path(), 6);
    let real = pack_identity(d.path());
    write_footer(d.path(), Some(real));

    // Rebuild the pack with every batch offset shifted: identical coverage,
    // different content, different identity.
    let pack = SealedSegmentIndex::open_pack_eager(&store::seal_path(
        d.path(),
        SEG_ID,
    ))
    .expect("pack opens");
    let streams: Vec<mess_index::sealed::SealStream> = pack
        .stream_ids()
        .iter()
        .map(|&sid| mess_index::sealed::SealStream {
            stream_id: sid,
            batches:   pack
                .stream_entries(sid)
                .expect("entries")
                .into_iter()
                .map(|e| mess_index::sealed::SealBatch {
                    first_version:    e.first_version,
                    frame_count:      e.frame_count,
                    first_global_pos: e.first_global_pos,
                    offset:           e.ptr.offset + 8,
                })
                .collect(),
        })
        .collect();
    let forged =
        mess_index::sealed::encode_pack(&mess_index::sealed::PackInput {
            segment_id:     pack.segment_id(),
            base_pos:       pack.base_pos(),
            streams:        &streams,
            event_type_ids: &[],
            filter:         None,
            payload_bytes:  None,
            registry_delta: None,
        });
    std::fs::write(store::seal_path(d.path(), SEG_ID), &forged).expect("plant");
    let observed = pack_identity(d.path());
    assert_ne!(observed, real, "the forgery is a different pack");

    let report = run(d.path());
    let f = finding(&report, "seal-pack-identity-mismatch")
        .expect("the mismatch is reported");
    assert_eq!(f.severity, Severity::Error);
    assert_eq!(field(f, "expected_identity"), json!(hex(&real)));
    assert_eq!(field(f, "observed_identity"), json!(hex(&observed)));
    assert_eq!(field(f, "fallback"), json!("raw-segment-scan"));
    assert_ne!(report.exit_code(), 0, "a broken binding must fail the exit");
}

/// A footer naming a pack that is not on disk: expected is named, observed is
/// explicitly null.
#[test]
fn a_missing_pack_reports_the_expected_identity_and_a_null_observed() {
    let d = tmp();
    build_pack_corpus(d.path(), 6);
    let id = pack_identity(d.path());
    write_footer(d.path(), Some(id));
    std::fs::remove_file(store::seal_path(d.path(), SEG_ID)).expect("remove");

    let report = run(d.path());
    let f = finding(&report, "seal-pack-missing").expect("reported");
    assert_eq!(f.severity, Severity::Error);
    assert_eq!(field(f, "expected_identity"), json!(hex(&id)));
    assert_eq!(field(f, "observed_identity"), serde_json::Value::Null);
    assert_eq!(field(f, "fallback"), json!("raw-segment-scan"));
}

/// **Identity-bit corruption.** The flag says a pack was named; the extension
/// no longer verifies. `verify` says the name is unresolvable — it must NOT
/// report the legacy "unnamed" advisory, which would tell an operator the
/// opposite of the truth.
#[test]
fn a_corrupt_identity_reports_unresolvable_not_unnamed() {
    use std::io::{Read, Seek, SeekFrom, Write};

    let d = tmp();
    build_pack_corpus(d.path(), 6);
    let id = pack_identity(d.path());
    write_footer(d.path(), Some(id));

    let log = store::log_path(d.path(), SEG_ID);
    let s = scan::scan_segment(SEG_ID, &log).expect("scan");
    let cat = s.trailer.expect("sealed");
    let at = cat.ext_offset + EXT_SECTION_HDR_LEN as u64 + 16;
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&log)
        .expect("open");
    let mut b = [0u8; 1];
    f.seek(SeekFrom::Start(at)).unwrap();
    f.read_exact(&mut b).unwrap();
    b[0] ^= 0x01;
    f.seek(SeekFrom::Start(at)).unwrap();
    f.write_all(&b).unwrap();
    f.sync_all().unwrap();
    drop(f);

    let report = run(d.path());
    let f =
        finding(&report, "seal-pack-identity-unresolvable").expect("reported");
    assert_eq!(f.severity, Severity::Error);
    assert_eq!(field(f, "fallback"), json!("raw-segment-scan"));
    assert!(
        finding(&report, "seal-pack-unnamed").is_none(),
        "a damaged name must never be reported as 'no name was written'"
    );
    assert_ne!(report.exit_code(), 0);
}

/// **The legacy compatibility policy, surfaced.** A footer that names nothing
/// is not an error — it is the documented D-FMT-10 policy — but an operator can
/// see which segments are still trusted on coverage alone, and that re-sealing
/// closes it. A `Warn`, so it never fails an exit code on its own.
#[test]
fn a_legacy_footer_is_reported_as_unnamed_but_not_an_error() {
    let d = tmp();
    build_pack_corpus(d.path(), 6);
    write_footer(d.path(), None);

    let report = run(d.path());
    let f = finding(&report, "seal-pack-unnamed").expect("reported");
    assert_eq!(f.severity, Severity::Warn);
    assert!(
        f.message.contains("coverage alone"),
        "the message must name the residual exposure: {}",
        f.message
    );
    assert!(finding(&report, "seal-pack-identity-mismatch").is_none());
    assert!(finding(&report, "seal-pack-identity-unresolvable").is_none());
    assert_eq!(report.exit_code(), 0, "the policy is not a failure");
}

/// An unsealed segment has no footer, so there is no installation record to
/// check and nothing to report — `verify` stays silent rather than inventing a
/// finding for the live head.
#[test]
fn an_unsealed_segment_reports_no_identity_finding() {
    let d = tmp();
    build_pack_corpus(d.path(), 6); // seal_active writes no footer

    let report = run(d.path());
    for kind in [
        "seal-pack-identity-verified",
        "seal-pack-identity-mismatch",
        "seal-pack-identity-unresolvable",
        "seal-pack-missing",
        "seal-pack-unnamed",
    ] {
        assert!(finding(&report, kind).is_none(), "unexpected {kind}");
    }
}
