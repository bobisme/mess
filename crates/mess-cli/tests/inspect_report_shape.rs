//! bn-1yz: `mess inspect`'s piped/agent-default `text` format renders
//! `Report::extra` (dir, lock, metrics, registry, stream_heads) instead of
//! silently dropping it; `--format pretty`/`text` stream_heads resolve
//! interned ids to names and truncate at a sane default at app scale
//! (`--all-streams` shows everything, and `--format json` is always
//! complete); `--stream` accepts a name as well as the interned numeric id;
//! and the JSON schema's field names do not depend on whether a live writer
//! holds the store — `registry` is always present with the same shape,
//! degraded explicitly via `registry.available` rather than by omitting
//! fields.
//!
//! bn-fj34: the locked case is now driven by the **real** D9 store lock (a live
//! `LogEngine`), not by holding a second metadata store's directory lock. That
//! store is deleted, and with it the last part of this report that a live
//! writer could make unavailable — see the lock-state test below.
#![cfg(not(miri))]

use std::collections::BTreeSet;

use mess_cli::format::{self, Format};
use mess_cli::inspect::{self, InspectOptions};
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{LogEngine, Version};
use serde_json::Value;

fn rec(t: &str, d: &[u8]) -> RecordToAppend {
    RecordToAppend { message_type: t.to_string(), data: d.to_vec() }
}

fn opts() -> InspectOptions { InspectOptions::default() }

/// Build a small real store with `n_streams` named streams (`user-0`,
/// `user-1`, ...), one event each. The engine (and its D9 lock) is dropped
/// before returning; stream names are made durable synchronously on first
/// use (bn-150), so a fresh read-only open sees them.
async fn build_named_corpus(dir: &std::path::Path, n_streams: usize) {
    let engine = LogEngine::open(dir).expect("open");
    for i in 0..n_streams {
        engine
            .append_batch(
                &format!("user-{i}"),
                Version::NoStream,
                &[rec("Created", format!("u{i}").as_bytes())],
            )
            .await
            .unwrap();
    }
}

fn json_of(report: &mess_cli::report::Report) -> Value {
    serde_json::from_str(&format::render(report, Format::Json)).unwrap()
}

// ---------------------------------------------------------------------------
// 1. text drops Report::extra
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn text_format_renders_extra_sections() {
    let dir = mess_testkit::sweeping_temp_dir(
        "cli-inspect-shape-text-format-renders-extra",
    );
    build_named_corpus(dir.path(), 3).await;

    let report = inspect::run(dir.path(), &opts());
    let text = format::render(&report, Format::Text);

    for needle in ["dir:", "lock:", "metrics", "registry", "stream_heads"] {
        assert!(
            text.contains(needle),
            "text output missing {needle:?} section:\n{text}"
        );
    }
    // A named stream must be visible BY NAME in the text render, not just as
    // an interned integer — proves name resolution reached the text path too.
    assert!(
        text.contains("user-0"),
        "expected a resolved stream name in text output:\n{text}"
    );

    // pretty must render the same sections (it dumped raw JSON before this
    // fix — same underlying renderer as text now).
    let pretty = format::render(&report, Format::Pretty);
    for needle in ["dir:", "lock:", "registry", "stream_heads", "user-0"] {
        assert!(
            pretty.contains(needle),
            "pretty output missing {needle:?} section:\n{pretty}"
        );
    }
}

// ---------------------------------------------------------------------------
// 2. stream_heads name resolution + app-scale truncation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn stream_heads_resolve_names_and_truncate_by_default() {
    let dir = mess_testkit::sweeping_temp_dir(
        "cli-inspect-shape-stream-heads-resolve-names",
    );
    let n = inspect::DEFAULT_STREAM_HEADS_LIMIT + 5;
    build_named_corpus(dir.path(), n).await;

    let report = inspect::run(dir.path(), &opts());

    // json is always complete, with every head's name resolved.
    let json = json_of(&report);
    let heads = json["stream_heads"].as_array().expect("stream_heads array");
    // `bn-2di`: `$registry` (stream 0) is a real stream in the log and carries
    // a real head, so it appears here alongside the user streams —
    // deliberately. An operator inspecting a store SHOULD be able to see
    // how big its registry is; it is hidden only from the
    // application-facing `read_global`.
    assert_eq!(
        heads.len(),
        n + 1,
        "json stream_heads must be complete (n user streams + $registry), not \
         truncated"
    );
    assert!(
        heads.iter().all(|h| h["name"].is_string()),
        "every head resolved a name: {heads:?}"
    );

    // text/pretty truncate by default with a "... and K more" line, and stop
    // short of dumping all `n` rows.
    let text = format::render(&report, Format::Text);
    assert!(
        text.contains("more"),
        "text stream_heads must truncate at app scale:\n{text}"
    );
    let pretty = format::render(&report, Format::Pretty);
    assert!(
        pretty.contains("more"),
        "pretty stream_heads must truncate at app scale:\n{pretty}"
    );

    // --all-streams disables the render-time cap (json was never capped).
    let all = InspectOptions { all_streams: true, ..InspectOptions::default() };
    let report_all = inspect::run(dir.path(), &all);
    let text_all = format::render(&report_all, Format::Text);
    assert!(
        !text_all.contains("more"),
        "--all-streams must show every head:\n{text_all}"
    );
    for i in 0..n {
        assert!(
            text_all.contains(&format!("user-{i}")),
            "--all-streams text must include user-{i}:\n{text_all}"
        );
    }
}

// ---------------------------------------------------------------------------
// --stream accepts a name as well as the interned numeric id
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn stream_filter_accepts_name_or_id() {
    let dir = mess_testkit::sweeping_temp_dir(
        "cli-inspect-shape-stream-filter-accepts-name",
    );
    build_named_corpus(dir.path(), 4).await;

    let by_name = InspectOptions {
        stream: Some("user-2".to_string()),
        ..InspectOptions::default()
    };
    let report = inspect::run(dir.path(), &by_name);
    let json = json_of(&report);
    let heads = json["stream_heads"].as_array().unwrap();
    assert_eq!(
        heads.len(),
        1,
        "name filter must match exactly one stream: {heads:?}"
    );
    assert_eq!(heads[0]["name"], "user-2");

    let sid = heads[0]["stream_id"].as_u64().unwrap();
    let by_id = InspectOptions {
        stream: Some(sid.to_string()),
        ..InspectOptions::default()
    };
    let report2 = inspect::run(dir.path(), &by_id);
    let json2 = json_of(&report2);
    let heads2 = json2["stream_heads"].as_array().unwrap();
    assert_eq!(heads2.len(), 1);
    assert_eq!(heads2[0]["stream_id"], sid);
    assert_eq!(heads2[0]["name"], "user-2");
}

// ---------------------------------------------------------------------------
// 3. field-name stability across lock state
// ---------------------------------------------------------------------------

/// The JSON schema's field names must not depend on whether a live writer
/// holds the store. bn-fj34: the locked case holds the **real** D9 store lock
/// by keeping a `LogEngine` open, which is what a running app holds — the
/// previous simulation (a second metadata-store open contending on that
/// store's own directory lock) is not reproducible because the store is gone.
#[tokio::test(flavor = "multi_thread")]
async fn json_field_names_are_lock_state_independent() {
    let dir = mess_testkit::sweeping_temp_dir(
        "cli-inspect-shape-json-field-names-are",
    );
    build_named_corpus(dir.path(), 3).await;

    let free = json_of(&inspect::run(dir.path(), &opts()));

    // Hold the store's own single-writer lock, exactly as a live app does.
    let _held = LogEngine::open(dir.path()).expect("hold the store lock");

    let locked = json_of(&inspect::run(dir.path(), &opts()));

    fn keys(v: &Value) -> BTreeSet<String> {
        v.as_object().unwrap().keys().cloned().collect()
    }

    assert_eq!(
        keys(&free),
        keys(&locked),
        "top-level envelope field names must not depend on lock state"
    );
    assert_eq!(
        keys(&free["registry"]),
        keys(&locked["registry"]),
        "registry field names must not depend on lock state"
    );

    // `bn-2di`: the registry FOLD needs no lock — names come out of the log's
    // `$registry` stream, which `inspect` reads straight from the segment
    // bytes. So a live writer does not degrade the name report at all: it
    // resolves identically locked and free.
    assert_eq!(locked["registry"]["available"], true);
    assert_eq!(free["registry"]["available"], true);
    assert_eq!(
        locked["registry"]["stream_names"], free["registry"]["stream_names"],
        "names fold out of the log identically, locked or not"
    );
    assert!(!locked["registry"]["stream_names"].as_array().unwrap().is_empty());

    // `bn-fj34`: and NEITHER does the snapshot half any more. It used to be
    // the one part of this report a live writer could take away (the metadata
    // store's exclusive open failed => `snapshots_available: false` plus a
    // `registry-unavailable` advisory). The pack sidecar reader takes no lock
    // and cannot fail, so both runs agree — this corpus persists no snapshots,
    // so both report an empty set, and neither reports it as unavailable.
    assert_eq!(locked["registry"]["snapshots_available"], true);
    assert_eq!(free["registry"]["snapshots_available"], true);
    assert_eq!(
        locked["registry"]["snapshots"], free["registry"]["snapshots"],
        "the snapshot half is lock-independent too"
    );
    for (label, v) in [("free", &free), ("locked", &locked)] {
        let advice = v["advice"].as_array().unwrap();
        assert!(
            !advice.iter().any(|a| a["type"] == "registry-unavailable"),
            "the deleted degradation must never reappear ({label}): {advice:?}"
        );
    }

    // stream_heads are recovered straight from the log and stay available
    // regardless of the store lock — and, since bn-2di, so are their names.
    let locked_heads = locked["stream_heads"].as_array().unwrap();
    assert_eq!(
        locked_heads.len(),
        4,
        "stream heads recovered from the log even under a live writer (3 user \
         streams + $registry)"
    );
    // `bn-2di`: EVERY name resolves under a live writer — the user streams'
    // from the log's `$registry` (which `inspect` folds straight out of the
    // segment bytes, needing no lock), and the reserved `$registry` stream
    // itself from spec text (REG1: the reserved ids are named by the
    // specification, not by any record, which is what makes bootstrap
    // non-circular).
    for h in locked_heads {
        assert!(
            h["name"].is_string(),
            "every stream name must resolve from the log, lock or no lock: \
             {h:?}"
        );
    }
    assert!(
        locked_heads.iter().any(|h| h["name"] == "$registry"),
        "the reserved stream must be named by spec text"
    );

    let free_heads = free["stream_heads"].as_array().unwrap();
    assert!(free_heads.iter().all(|h| h["name"].is_string()));
}

// ---------------------------------------------------------------------------
// bn-we9x: the §12.6 directory chooser's decision is visible per segment
// ---------------------------------------------------------------------------

/// Seal a real store and report which `STREAM_DIRECTORY` codec each segment's
/// directory used. The chooser emits whichever image is smaller, so a store
/// with many interned (hence dense) stream ids seals `bitrank` while a
/// single-stream segment seals `sorted` — and `inspect` must show the
/// difference, because "which representation did this segment get" is exactly
/// the question an operator cannot otherwise answer about a sealed artifact.
#[tokio::test(flavor = "multi_thread")]
async fn dir_codec_is_reported_per_sealed_segment() {
    use mess_store::engine::EngineOptions;

    async fn seal_store(dir: &std::path::Path, n_streams: usize) {
        let opts = EngineOptions {
            segment_size: 1 << 20,
            // bn-3of consolidated `.seal` pack — the path the §12.6 chooser
            // actually runs on.
            seal_pack: true,
            ..EngineOptions::default()
        };
        let engine = LogEngine::open_with(dir, opts).expect("open");
        for i in 0..n_streams {
            engine
                .append_batch(
                    &format!("user-{i}"),
                    Version::NoStream,
                    &[rec("Created", format!("u{i}").as_bytes())],
                )
                .await
                .unwrap();
        }
        engine.seal_active().expect("seal");
    }

    fn codec_of(dir: &std::path::Path) -> Value {
        let json = json_of(&inspect::run(dir, &opts()));
        let rows = json["segments"].as_array().expect("segments rows");
        assert_eq!(rows.len(), 1, "expected one segment: {rows:?}");
        rows[0]["dir_codec"].clone()
    }

    // Many streams -> a dense interned id universe -> the bitvector beats the
    // 8-byte-per-key column it replaces.
    let dense = mess_testkit::sweeping_temp_dir("cli-inspect-dir-codec-dense");
    seal_store(dense.path(), 64).await;
    assert_eq!(codec_of(dense.path()), "bitrank");

    // One user stream (plus `$registry`) -> the bitvector cannot pay for its
    // own 16-byte header -> the always-correct sorted fallback.
    let tiny = mess_testkit::sweeping_temp_dir("cli-inspect-dir-codec-tiny");
    seal_store(tiny.path(), 1).await;
    assert_eq!(codec_of(tiny.path()), "sorted");

    // An unsealed segment has no directory at all: `null`, not a guess.
    let live = mess_testkit::sweeping_temp_dir("cli-inspect-dir-codec-live");
    build_named_corpus(live.path(), 3).await;
    let json = json_of(&inspect::run(live.path(), &opts()));
    let rows = json["segments"].as_array().unwrap();
    assert!(rows.iter().all(|r| r["dir_codec"].is_null()), "{rows:?}");
}

// ---------------------------------------------------------------------------
// bn-1w4h: which sealed artifact actually serves a segment
// ---------------------------------------------------------------------------

/// `has_pidx`/`has_seal` are raw presence bits; `sealed_artifact` names the one
/// a READER would use, applying bn-3of's dual-read preference. An operator
/// staring at a mid-migration store needs that answer, and `inspect` is the
/// surface that describes on-disk shape.
#[tokio::test(flavor = "multi_thread")]
async fn sealed_artifact_names_the_serving_shape() {
    use mess_store::engine::EngineOptions;

    async fn seal_store(dir: &std::path::Path, pack: bool) {
        let opts = EngineOptions {
            segment_size: 1 << 20,
            seal_pack: pack,
            ..EngineOptions::default()
        };
        let engine = LogEngine::open_with(dir, opts).expect("open");
        engine
            .append_batch("acct-1", Version::NoStream, &[rec("Created", b"a")])
            .await
            .unwrap();
        engine.seal_active().expect("seal");
    }

    fn artifact_of(dir: &std::path::Path) -> Value {
        let json = json_of(&inspect::run(dir, &opts()));
        let rows = json["segments"].as_array().expect("segments rows");
        assert_eq!(rows.len(), 1, "expected one segment: {rows:?}");
        rows[0]["sealed_artifact"].clone()
    }

    let packed = mess_testkit::sweeping_temp_dir("cli-inspect-artifact-pack");
    seal_store(packed.path(), true).await;
    assert_eq!(artifact_of(packed.path()), "seal-pack");

    let loose = mess_testkit::sweeping_temp_dir("cli-inspect-artifact-pidx");
    seal_store(loose.path(), false).await;
    assert_eq!(artifact_of(loose.path()), "pidx");

    // A `.pidx` planted next to the pack is shadowed: the pack still serves.
    std::fs::copy(
        mess_cli::store::pidx_path(loose.path(), 1),
        mess_cli::store::pidx_path(packed.path(), 1),
    )
    .expect("plant .pidx");
    assert_eq!(artifact_of(packed.path()), "seal-pack");

    // An unsealed head has no sealed artifact at all.
    let live = mess_testkit::sweeping_temp_dir("cli-inspect-artifact-live");
    build_named_corpus(live.path(), 2).await;
    let json = json_of(&inspect::run(live.path(), &opts()));
    let rows = json["segments"].as_array().unwrap();
    assert!(rows.iter().all(|r| r["sealed_artifact"] == "none"), "{rows:?}");
}
