//! `bn-11ba`: `mess doctor`'s authority section explains which state is
//! canonical and which is discardable acceleration — and its per-segment
//! inventory **flips** when an accelerator is deleted or corrupted.
//!
//! The load-bearing property is not that the section renders. It is that no
//! surface ever presents an accelerator as authoritative, and that the state
//! it reports tracks the bytes on disk rather than a hardcoded optimism.
#![cfg(not(miri))]

use mess_cli::doctor::{self, DoctorOptions};
use mess_cli::format::{self, Format};
use serde_json::Value;

mod common;

fn run_json(dir: &std::path::Path) -> Value {
    let report = doctor::run(dir, &DoctorOptions::default());
    serde_json::from_str(&format::render(&report, Format::Json))
        .expect("doctor json")
}

fn authority(json: &Value) -> &Value { &json["authority"] }

fn segment_row(json: &Value, segment_id: u64) -> &Value {
    authority(json)["segments"]
        .as_array()
        .expect("segments array")
        .iter()
        .find(|r| r["segment_id"] == segment_id)
        .unwrap_or_else(|| panic!("no authority row for segment {segment_id}"))
}

fn finding<'a>(json: &'a Value, kind: &str) -> &'a Value {
    json["findings"]
        .as_array()
        .expect("findings")
        .iter()
        .find(|f| f["kind"] == kind)
        .unwrap_or_else(|| {
            panic!("no `{kind}` finding: {:#}", json["findings"])
        })
}

// ---------------------------------------------------------------------------
// The classification itself
// ---------------------------------------------------------------------------

/// The section names exactly two canonical sources and classifies every other
/// artifact as discardable — with, for each, what the engine does on loss.
/// A report that listed an artifact without saying what losing it costs is
/// the failure this check exists to prevent.
#[test]
fn the_authority_section_classifies_canonical_and_discardable() {
    let d = mess_testkit::sweeping_temp_dir("cli-authority-classification");
    common::build_corpus(d.path(), 8);

    let json = run_json(d.path());
    let a = authority(&json);

    assert_eq!(a["scope"], "offline");
    // ADR 0003 declined v4; v3 is the only canonical log version.
    assert_eq!(a["log_format_version"], 3);

    let canonical = a["canonical"].as_array().expect("canonical array");
    let names: Vec<&str> =
        canonical.iter().map(|c| c["name"].as_str().unwrap()).collect();
    assert_eq!(
        names,
        vec!["seg-*.log", "$registry"],
        "exactly the two canonical sources ADR 0002 names"
    );
    for c in canonical {
        assert_eq!(c["role"], "canonical");
    }

    let accelerators = a["accelerators"].as_array().expect("accelerators");
    assert!(!accelerators.is_empty());
    for c in accelerators {
        assert_eq!(
            c["role"], "discardable-accelerator",
            "an accelerator must never be reported as authoritative: {c:#}"
        );
        assert!(
            c["on_loss"].as_str().is_some_and(|s| !s.is_empty()),
            "{}: an operator must be told what losing it costs",
            c["name"]
        );
    }
    let acc_names: Vec<&str> =
        accelerators.iter().map(|c| c["name"].as_str().unwrap()).collect();
    for want in [".seal", ".pidx", ".pcol", ".filter", ".reg", ".par"] {
        assert!(acc_names.contains(&want), "{want} must be classified");
    }

    // The advisory that says this is a file-state view, not live counters.
    let advice = json["advice"].as_array().expect("advice");
    assert!(
        advice.iter().any(|x| x["type"] == "authority-scope"),
        "the offline/in-process split must be stated: {advice:#?}"
    );
}

/// A healthy loose-sidecar store reports its segment as served by the `.pidx`
/// family, with the pack absent (not degraded — a store written in
/// compatibility mode has no pack to be missing).
#[test]
fn a_healthy_loose_sidecar_store_reports_the_pidx_as_serving() {
    let d = mess_testkit::sweeping_temp_dir("cli-authority-loose-healthy");
    common::build_corpus(d.path(), 8);

    let json = run_json(d.path());
    let row = segment_row(&json, common::SEG_ID);
    assert_eq!(row["serving"], "pidx");
    assert_eq!(row["serving_role"], "discardable-accelerator");
    assert_eq!(row["canonical_source"], "seg-*.log");
    assert_eq!(row["artifacts"][".pidx"], "present");
    assert_eq!(row["artifacts"][".seal"], "absent");
    assert_eq!(row["artifacts"][".pcol"], "present");
    assert_eq!(row["artifacts"][".filter"], "present");
    assert!(row["pack_identity"].is_null(), "a sidecar has no pack identity");
    assert!(
        row["dir_codec_name"].as_str().is_some(),
        "the serving artifact's directory codec is named: {row:#}"
    );
    assert!(
        row["fallback"].as_str().is_some_and(|s| s.contains("not data")),
        "the fallback line must say losing it costs time, not data: {row:#}"
    );

    let f = finding(&json, "authority-accelerators");
    assert_eq!(f["severity"], "ok");
    assert_eq!(f["artifacts_degraded"], 0);
    assert_eq!(f["served_by_log_scan"], 0);
    assert_eq!(authority(&json)["summary"]["served_by_loose_sidecar"], 1);
    assert_eq!(authority(&json)["summary"]["served_by_seal_pack"], 0);
}

/// A healthy pack-sealed store reports the pack as serving, with its
/// identity. The `.pcol`/`.filter`/`.reg` siblings read "absent" because a
/// pack carries that content as sections rather than as separate files —
/// correct, not a fault, which is why they do not raise the severity.
#[test]
fn a_healthy_pack_sealed_store_reports_the_pack_as_serving() {
    let d = mess_testkit::sweeping_temp_dir("cli-authority-pack-healthy");
    common::build_sealed_pack_store(d.path(), 8);

    let json = run_json(d.path());
    let row = segment_row(&json, common::SEG_ID);
    assert_eq!(row["serving"], "seal-pack");
    assert_eq!(row["artifacts"][".seal"], "present");
    assert_eq!(row["artifacts"][".pidx"], "absent");
    let identity = row["pack_identity"].as_str().expect("pack identity");
    assert_eq!(identity.len(), 64, "32 bytes of lowercase hex");
    assert!(identity.chars().all(|c| c.is_ascii_hexdigit()));

    let f = finding(&json, "authority-accelerators");
    assert_eq!(f["severity"], "ok");
    assert_eq!(authority(&json)["summary"]["served_by_seal_pack"], 1);
}

// ---------------------------------------------------------------------------
// Fault injection
// ---------------------------------------------------------------------------

/// **Delete the sidecar.** The row flips to `log-scan`, the artifact reads
/// `absent`, the summary counts it, and the finding drops to `info` — the
/// store is fine, it is just unaccelerated.
///
/// bn-3m62: the segment is footer-sealed first. `log-scan` now means "a SEALED
/// segment lost its index and is owed a re-seal"; a segment with no footer is
/// `unsealed` instead, because having no sealed index is its normal state and
/// nothing is owed for it. This test is about the former, so it seals.
#[test]
fn deleting_the_sidecar_flips_the_row_to_a_log_scan() {
    let d = mess_testkit::sweeping_temp_dir("cli-authority-sidecar-deleted");
    common::build_corpus(d.path(), 8);
    common::seal_log_trailer(d.path());
    assert_eq!(
        segment_row(&run_json(d.path()), common::SEG_ID)["serving"],
        "pidx",
        "precondition: the sidecar serves before the fault"
    );

    std::fs::remove_file(common::pidx(d.path())).expect("delete .pidx");

    let json = run_json(d.path());
    let row = segment_row(&json, common::SEG_ID);
    assert_eq!(row["serving"], "log-scan");
    assert_eq!(row["artifacts"][".pidx"], "absent");
    assert!(row["pack_identity"].is_null());
    assert!(
        row["fallback"]
            .as_str()
            .is_some_and(|s| s.contains("the log is the authority")),
        "the fallback must name the canonical source: {row:#}"
    );

    let f = finding(&json, "authority-accelerators");
    assert_eq!(f["severity"], "info", "unaccelerated is not a fault");
    assert_eq!(f["served_by_log_scan"], 1);
    assert_eq!(f["artifacts_degraded"], 0);
    assert_eq!(authority(&json)["summary"]["served_by_log_scan"], 1);
}

/// **Corrupt the sidecar.** Present-but-unusable is a different state from
/// absent, and it is the one worth a warning: those bytes exist and the
/// engine would refuse them.
#[test]
fn a_corrupt_sidecar_reads_degraded_not_absent() {
    let d = mess_testkit::sweeping_temp_dir("cli-authority-sidecar-corrupt");
    common::build_corpus(d.path(), 8);
    // bn-3m62: seal the footer so `serving` reports the sealed-but-unindexed
    // state (`log-scan`) rather than the unsealed one — see the test above.
    common::seal_log_trailer(d.path());
    let pidx = common::pidx(d.path());
    let bytes = std::fs::read(&pidx).expect("read");
    std::fs::write(&pidx, &bytes[..bytes.len() / 2]).expect("truncate");

    let json = run_json(d.path());
    let row = segment_row(&json, common::SEG_ID);
    assert_eq!(
        row["artifacts"][".pidx"], "degraded",
        "bytes on disk that the engine would refuse are not `absent`"
    );
    assert_eq!(row["serving"], "log-scan");

    let f = finding(&json, "authority-accelerators");
    assert_eq!(f["severity"], "warn");
    assert_eq!(f["artifacts_degraded"], 1);
    assert!(
        f["message"]
            .as_str()
            .is_some_and(|m| m.contains("the log is canonical")),
        "the warning must still say reads are safe: {}",
        f["message"]
    );
}

/// **Corrupt the pack.** Same story for the SealPack shape, so an operator
/// reading the section does not have to know which sealing mode wrote the
/// store.
#[test]
fn a_corrupt_seal_pack_reads_degraded() {
    let d = mess_testkit::sweeping_temp_dir("cli-authority-pack-corrupt");
    common::build_sealed_pack_store(d.path(), 8);
    let seal = common::pidx(d.path()).with_extension("seal");
    let bytes = std::fs::read(&seal).expect("read pack");
    std::fs::write(&seal, &bytes[..bytes.len() / 2]).expect("truncate");

    let json = run_json(d.path());
    let row = segment_row(&json, common::SEG_ID);
    assert_eq!(row["artifacts"][".seal"], "degraded");
    assert_eq!(row["serving"], "log-scan");
    assert!(row["pack_identity"].is_null(), "a refused pack names nothing");
    assert_eq!(finding(&json, "authority-accelerators")["severity"], "warn");
}

/// **Both shapes present.** bn-3of's dual read prefers the pack, so the
/// sidecar under a healthy pack is `shadowed` — inert, not broken, and not
/// counted as a fault.
#[test]
fn a_sidecar_under_a_healthy_pack_reads_shadowed() {
    let d = mess_testkit::sweeping_temp_dir("cli-authority-shadowed");
    common::build_corpus(d.path(), 8); // writes the .pidx family
    let pidx = common::pidx(d.path());
    let saved = std::fs::read(&pidx).expect("read sidecar");
    // Rebuild the same store as a pack, then put the sidecar back beside it.
    let d2 = mess_testkit::sweeping_temp_dir("cli-authority-shadowed-pack");
    common::build_sealed_pack_store(d2.path(), 8);
    std::fs::write(common::pidx(d2.path()), saved).expect("restore sidecar");

    let json = run_json(d2.path());
    let row = segment_row(&json, common::SEG_ID);
    assert_eq!(row["serving"], "seal-pack");
    assert_eq!(
        row["artifacts"][".pidx"], "shadowed",
        "an inert sidecar is neither present-and-serving nor a fault"
    );
    let f = finding(&json, "authority-accelerators");
    assert_eq!(f["severity"], "ok");
    assert_eq!(f["artifacts_degraded"], 0);
}

// ---------------------------------------------------------------------------
// Rendering + cardinality
// ---------------------------------------------------------------------------

/// The section is visible in `text` (the piped/agent default), not only in
/// `json`. An `extra` section that renders only under `--format json` is
/// invisible to the audience the CLI conventions make the default.
#[test]
fn the_authority_section_renders_in_text_output() {
    let d = mess_testkit::sweeping_temp_dir("cli-authority-text");
    common::build_corpus(d.path(), 8);

    let report = doctor::run(d.path(), &DoctorOptions::default());
    let text = format::render(&report, Format::Text);
    assert!(text.contains("authority"), "no authority section:\n{text}");
    assert!(
        text.contains("discardable-accelerator"),
        "the classification must reach the text render:\n{text}"
    );
    assert!(
        text.contains("authority-accelerators"),
        "the summary finding must reach the text render:\n{text}"
    );
}

/// One row per segment, and no stream/type/batch identity anywhere in the
/// section — the cardinality bound the surface promises.
#[test]
fn the_section_has_one_row_per_segment_and_no_unbounded_labels() {
    let d = mess_testkit::sweeping_temp_dir("cli-authority-cardinality");
    common::build_corpus(d.path(), 64);

    let json = run_json(d.path());
    let rows = authority(&json)["segments"].as_array().expect("segments");
    assert_eq!(rows.len(), 1, "the corpus is one segment");
    assert_eq!(authority(&json)["summary"]["segments"], 1);

    // Every row key is a fixed name; the artifacts map is keyed by extension,
    // never by a stream or type.
    for row in rows {
        let obj = row.as_object().expect("row object");
        for key in obj.keys() {
            assert!(
                !key.contains("stream") || key == "stream_count",
                "unbounded label leaked into an authority row: {key}"
            );
        }
        let arts = row["artifacts"].as_object().expect("artifacts");
        for key in arts.keys() {
            assert!(
                key.starts_with('.'),
                "artifacts must be keyed by extension, got {key}"
            );
        }
    }
}
