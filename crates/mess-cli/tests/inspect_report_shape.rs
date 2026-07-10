//! bn-1yz: `mess inspect`'s piped/agent-default `text` format renders
//! `Report::extra` (dir, lock, metrics, registry, stream_heads) instead of
//! silently dropping it; `--format pretty`/`text` stream_heads resolve
//! interned ids to names and truncate at a sane default at app scale
//! (`--all-streams` shows everything, and `--format json` is always
//! complete); `--stream` accepts a name as well as the interned numeric id;
//! and the JSON schema's field names do not depend on whether a live writer
//! holds the metadata store's lock — `registry` is always present with the
//! same shape, degraded explicitly via `registry.available` rather than by
//! omitting fields.
#![cfg(not(miri))]

use std::collections::BTreeSet;

use mess_cli::format::{self, Format};
use mess_cli::inspect::{self, InspectOptions};
use mess_cli::store;
use mess_index::meta::MetaStore;
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{LogEngine, Version};
use serde_json::Value;

fn rec(t: &str, d: &[u8]) -> RecordToAppend {
    RecordToAppend { message_type: t.to_string(), data: d.to_vec() }
}

fn opts() -> InspectOptions {
    InspectOptions::default()
}

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
    let dir = tempfile::tempdir().unwrap();
    build_named_corpus(dir.path(), 3).await;

    let report = inspect::run(dir.path(), &opts());
    let text = format::render(&report, Format::Text);

    for needle in ["dir:", "lock:", "metrics", "registry", "stream_heads"] {
        assert!(text.contains(needle), "text output missing {needle:?} section:\n{text}");
    }
    // A named stream must be visible BY NAME in the text render, not just as
    // an interned integer — proves name resolution reached the text path too.
    assert!(text.contains("user-0"), "expected a resolved stream name in text output:\n{text}");

    // pretty must render the same sections (it dumped raw JSON before this
    // fix — same underlying renderer as text now).
    let pretty = format::render(&report, Format::Pretty);
    for needle in ["dir:", "lock:", "registry", "stream_heads", "user-0"] {
        assert!(pretty.contains(needle), "pretty output missing {needle:?} section:\n{pretty}");
    }
}

// ---------------------------------------------------------------------------
// 2. stream_heads name resolution + app-scale truncation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn stream_heads_resolve_names_and_truncate_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let n = inspect::DEFAULT_STREAM_HEADS_LIMIT + 5;
    build_named_corpus(dir.path(), n).await;

    let report = inspect::run(dir.path(), &opts());

    // json is always complete, with every head's name resolved.
    let json = json_of(&report);
    let heads = json["stream_heads"].as_array().expect("stream_heads array");
    assert_eq!(heads.len(), n, "json stream_heads must be complete, not truncated");
    assert!(heads.iter().all(|h| h["name"].is_string()), "every head resolved a name: {heads:?}");

    // text/pretty truncate by default with a "... and K more" line, and stop
    // short of dumping all `n` rows.
    let text = format::render(&report, Format::Text);
    assert!(text.contains("more"), "text stream_heads must truncate at app scale:\n{text}");
    let pretty = format::render(&report, Format::Pretty);
    assert!(pretty.contains("more"), "pretty stream_heads must truncate at app scale:\n{pretty}");

    // --all-streams disables the render-time cap (json was never capped).
    let all = InspectOptions { all_streams: true, ..InspectOptions::default() };
    let report_all = inspect::run(dir.path(), &all);
    let text_all = format::render(&report_all, Format::Text);
    assert!(!text_all.contains("more"), "--all-streams must show every head:\n{text_all}");
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
    let dir = tempfile::tempdir().unwrap();
    build_named_corpus(dir.path(), 4).await;

    let by_name = InspectOptions { stream: Some("user-2".to_string()), ..InspectOptions::default() };
    let report = inspect::run(dir.path(), &by_name);
    let json = json_of(&report);
    let heads = json["stream_heads"].as_array().unwrap();
    assert_eq!(heads.len(), 1, "name filter must match exactly one stream: {heads:?}");
    assert_eq!(heads[0]["name"], "user-2");

    let sid = heads[0]["stream_id"].as_u64().unwrap();
    let by_id = InspectOptions { stream: Some(sid.to_string()), ..InspectOptions::default() };
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
/// holds the metadata store's lock. Simulate the locked case exactly as the
/// bone suggests: hold the fjall metadata lock in-process. A second
/// `MetaStore::open` on the same directory contends on the same OS advisory
/// file lock (`std::fs::File::try_lock`, per-open-file-description, so it
/// blocks even a second open from this same process) that a live writer
/// would hold, so `inspect`'s internal `metaread::read` degrades exactly as
/// it would against a real live-locked store.
#[tokio::test(flavor = "multi_thread")]
async fn json_field_names_are_lock_state_independent() {
    let dir = tempfile::tempdir().unwrap();
    build_named_corpus(dir.path(), 3).await;

    let free = json_of(&inspect::run(dir.path(), &opts()));

    // Hold the metadata store's own lock, simulating a live writer.
    let meta_dir = store::meta_dir(dir.path());
    let _held = MetaStore::open(&meta_dir).expect("hold meta lock");

    let locked = json_of(&inspect::run(dir.path(), &opts()));

    fn keys(v: &Value) -> BTreeSet<String> {
        v.as_object().unwrap().keys().cloned().collect()
    }

    assert_eq!(keys(&free), keys(&locked), "top-level envelope field names must not depend on lock state");
    assert_eq!(
        keys(&free["registry"]),
        keys(&locked["registry"]),
        "registry field names must not depend on lock state"
    );

    // The degraded case says so explicitly rather than omitting fields.
    assert_eq!(locked["registry"]["available"], false);
    assert!(locked["registry"]["stream_names"].as_array().unwrap().is_empty());
    assert_eq!(free["registry"]["available"], true);
    assert!(!free["registry"]["stream_names"].as_array().unwrap().is_empty());

    let advice = locked["advice"].as_array().unwrap();
    assert!(
        advice.iter().any(|a| a["type"] == "registry-unavailable"),
        "locked run must advise that the registry is unavailable: {advice:?}"
    );

    // stream_heads are recovered straight from the log and stay available
    // regardless of the meta lock — just without resolved names.
    let locked_heads = locked["stream_heads"].as_array().unwrap();
    assert_eq!(locked_heads.len(), 3, "stream heads recovered from the log even when meta is locked");
    assert!(locked_heads.iter().all(|h| h["name"].is_null()));

    let free_heads = free["stream_heads"].as_array().unwrap();
    assert!(free_heads.iter().all(|h| h["name"].is_string()));
}
