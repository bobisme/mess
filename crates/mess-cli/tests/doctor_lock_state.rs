//! bn-ve0: `mess doctor`'s `fold_version` check needs the fjall metadata
//! store, which a live writer holds under its own exclusive lock. Against a
//! live-locked store the finding must:
//!
//! 1. Say WHY (a live writer holds the metadata store's lock) and WHAT to do
//!    (stop the writer, or run doctor against a backup) — not just surface the
//!    raw `FjallError: Locked` string.
//! 2. Stay `info` severity (not an error — exit code untouched).
//! 3. Not change the JSON envelope's top-level field names vs. the unlocked
//!    run, and every other check must keep working exactly as it does when the
//!    store is free.
//!
//! Locked case is simulated exactly as `inspect`'s bn-1yz tests do (see
//! `tests/inspect_report_shape.rs`): hold the metadata store's own fjall
//! lock in-process via a second `MetaStore::open` on the same directory,
//! which contends on the same OS advisory file lock a live writer would
//! hold.
#![cfg(not(miri))]

use std::collections::BTreeSet;

use mess_cli::doctor::{self, DoctorOptions};
use mess_cli::format::{self, Format};
use mess_cli::store;
use mess_index::meta::MetaStore;
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{LogEngine, Version};
use serde_json::Value;

fn rec(t: &str, d: &[u8]) -> RecordToAppend {
    RecordToAppend { message_type: t.to_string(), data: d.to_vec() }
}

fn opts() -> DoctorOptions { DoctorOptions::default() }

/// Build a small real store with a couple of streams, then drop the engine
/// (and its D9 lock) before returning.
async fn build_corpus(dir: &std::path::Path) {
    let engine = LogEngine::open(dir).expect("open");
    for i in 0..2 {
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

fn keys(v: &Value) -> BTreeSet<String> {
    v.as_object().unwrap().keys().cloned().collect()
}

fn findings_of(v: &Value) -> &Vec<Value> { v["findings"].as_array().unwrap() }

fn finding<'a>(v: &'a Value, kind: &str) -> Option<&'a Value> {
    findings_of(v).iter().find(|f| f["kind"] == kind)
}

#[tokio::test(flavor = "multi_thread")]
async fn fold_version_finding_explains_lock_and_says_what_to_do() {
    let dir = tempfile::tempdir().unwrap();
    build_corpus(dir.path()).await;

    // Hold the metadata store's own lock, simulating a live writer.
    let meta_dir = store::meta_dir(dir.path());
    let _held = MetaStore::open(&meta_dir).expect("hold meta lock");

    let locked = json_of(&doctor::run(dir.path(), &opts()));

    let f = finding(&locked, "meta-store-locked")
        .expect("locked run must produce a meta-store-locked finding");
    assert_eq!(f["severity"], "info", "locking is expected, not an error");
    assert_eq!(f["check"], "fold-version");

    let msg = f["message"].as_str().unwrap();
    // Says WHY.
    assert!(
        msg.contains("live writer") && msg.contains("lock"),
        "message must explain a live writer holds the lock: {msg:?}"
    );
    // Says WHAT to do.
    assert!(
        msg.contains("stop the writer") || msg.contains("backup"),
        "message must say how to get the full check: {msg:?}"
    );
    // No longer just the raw fjall error string.
    assert!(
        !msg.contains("FjallError"),
        "message must not surface the raw FjallError text: {msg:?}"
    );
    // The raw reason is still available in structured form for anyone who
    // wants it, just not as the human-facing message.
    assert!(f["reason"].as_str().unwrap().contains("Locked"));

    // A locked-store run is still a healthy exit: this was never a failure.
    assert!(
        locked["summary"]["worst"] == "info"
            || locked["summary"]["worst"] == "ok"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn old_registry_unavailable_kind_is_reserved_for_non_lock_failures() {
    // A fresh directory with no metadata store at all (never opened by an
    // engine) is a *different* failure than a live-writer lock: `doctor`
    // must not claim a lock is held when the real reason is "no store here
    // yet".
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("seg")).ok();
    // No engine ever opened this directory, so store::discover_segments
    // finds nothing and metaread::read finds no `meta/` dir either.
    let report = doctor::run(dir.path(), &opts());
    let json = json_of(&report);

    if let Some(f) = finding(&json, "registry-unavailable") {
        assert_eq!(f["check"], "fold-version");
        assert_ne!(
            f["kind"], "meta-store-locked",
            "an absent store must not be reported as locked"
        );
    }
    // Whichever kind fired, it must never be `meta-store-locked` for an
    // absent (never-written) store.
    assert!(finding(&json, "meta-store-locked").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn json_envelope_field_names_are_lock_state_independent() {
    let dir = tempfile::tempdir().unwrap();
    build_corpus(dir.path()).await;

    let free = json_of(&doctor::run(dir.path(), &opts()));

    let meta_dir = store::meta_dir(dir.path());
    let _held = MetaStore::open(&meta_dir).expect("hold meta lock");

    let locked = json_of(&doctor::run(dir.path(), &opts()));

    assert_eq!(
        keys(&free),
        keys(&locked),
        "top-level envelope field names must not depend on lock state"
    );
    // `fold_versions` in particular used to vanish entirely on a locked run
    // (the early `return` in `check_fold_version` skipped `report.set`) —
    // now it stays present, just empty.
    assert!(free.get("fold_versions").is_some());
    assert!(locked.get("fold_versions").is_some());
    assert_eq!(locked["fold_versions"], serde_json::json!([]));

    // Every other check keeps behaving exactly as it does unlocked: the
    // segment/epoch checks don't touch the meta store at all.
    assert!(
        finding(&free, "unsealed-head").is_some(),
        "segment checks unaffected by lock state (free)"
    );
    assert!(
        finding(&locked, "unsealed-head").is_some(),
        "segment checks unaffected by lock state (locked)"
    );
    assert!(
        finding(&free, "fsync-ok").is_some(),
        "fsync check unaffected by lock state (free)"
    );
    assert!(
        finding(&locked, "fsync-ok").is_some(),
        "fsync check unaffected by lock state (locked)"
    );

    // The lock-held finding surfaces the D9 store lock separately from the
    // fjall-meta-lock finding — the two are related but distinct locks in
    // this simulated scenario (the D9 lock is free here; only the fjall
    // meta lock is held), and the finding wording must not conflate them.
    let lock_finding = finding(&locked, "lock-free").expect(
        "D9 store lock is free in this scenario (only fjall meta is held)",
    );
    assert_eq!(lock_finding["check"], "lock");
}
