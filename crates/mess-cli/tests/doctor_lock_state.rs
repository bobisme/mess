//! `mess doctor` against a store held by a **live writer**.
//!
//! # What this file used to pin, and why it changed (bn-ve0 -> bn-fj34)
//!
//! bn-ve0 wrote this suite around a degradation: `doctor`'s `fold_version`
//! check read a `<dir>/meta` key-value store whose open took an exclusive
//! directory lock with no read-only or secondary mode, so against a running app
//! the check could not run at all. The best available behaviour was to explain
//! itself — an `info`-severity `meta-store-locked` finding that said WHY (a
//! live writer holds it) and WHAT to do (stop the writer, or run against a
//! backup) instead of surfacing a raw error string or failing the command.
//!
//! bn-3l8n moved app snapshots to the lock-free pack sidecar and bn-fj34
//! deleted the store that produced the degradation. The finding is not
//! unreachable — it does not exist, and neither does the code path that built
//! it. So this suite now pins the **stronger** property that replaced it:
//!
//! 1. the `fold_version` check RUNS against a live-locked store and sees every
//!    app snapshot — the thing bn-ve0 could only apologise for;
//! 2. `meta-store-locked` is gone and never comes back;
//! 3. the one remaining live-writer degradation (the `$registry` fold, which
//!    genuinely needs the engine and therefore the D9 store lock) stays
//!    `info`-severity and leaves the exit code healthy;
//! 4. the JSON envelope's top-level field names still do not depend on lock
//!    state.
//!
//! The lock here is the **real** one: a live `LogEngine` (plus its pack sidecar
//! writer) held open across the `doctor` run, exactly as a running app holds it
//! — no simulation.
#![cfg(not(miri))]

use std::collections::BTreeSet;

use mess_cli::doctor::{self, DoctorOptions};
use mess_cli::format::{self, Format};
use mess_core::{Aggregate, CodecError, Decide, Event};
use mess_store::{
    EventStore, LogEngine, PackSnapshotBackend, SnapshotPolicy, Snapshottable,
    StableSnapshotId, StateCodecError,
};
use serde_json::Value;

// ---------------------------------------------------------------------------
// A tiny snapshottable counter (mirrors `doctor_app_snapshots.rs`).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct Added(i64);

impl Event for Added {
    fn name(&self) -> &'static str { "counter.added" }

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.to_le_bytes().to_vec())
    }

    fn decode(name: &str, data: &[u8]) -> Result<Self, CodecError> {
        let b: [u8; 8] = data.try_into().map_err(|_| CodecError::Decode {
            event_name: name.to_string(),
            source:     format!("expected 8 bytes, got {}", data.len()),
        })?;
        Ok(Added(i64::from_le_bytes(b)))
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Counter {
    total: i64,
}

impl Aggregate for Counter {
    type Event = Added;

    fn apply(&mut self, e: &Added) {
        self.total = self.total.wrapping_add(e.0);
    }
}

impl Snapshottable for Counter {
    const AGGREGATE_SCHEMA_ID: StableSnapshotId =
        StableSnapshotId::new("mess-cli.test.lock-counter");
    const FOLD_VERSION: u32 = 1;

    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
        Ok(self.total.to_le_bytes().to_vec())
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
        let b: [u8; 8] = bytes.try_into().map_err(|_| {
            StateCodecError(format!("expected 8 bytes, got {}", bytes.len()))
        })?;
        Ok(Counter { total: i64::from_le_bytes(b) })
    }
}

#[derive(Debug)]
struct Never;

impl std::fmt::Display for Never {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "counter never rejects")
    }
}

impl std::error::Error for Never {}

#[derive(Clone)]
struct Add(i64);

impl Decide<Add> for Counter {
    type Rejection = Never;

    fn decide(&self, cmd: Add) -> Result<Vec<Added>, Never> {
        Ok(vec![Added(cmd.0)])
    }
}

type Store = EventStore<PackSnapshotBackend<LogEngine>>;

fn opts() -> DoctorOptions { DoctorOptions::default() }

fn open_app_store(dir: &std::path::Path) -> Store {
    let engine = LogEngine::open(dir).expect("open engine");
    let backend = PackSnapshotBackend::open(
        engine,
        mess_cli::store::snapshot_pack_dir(dir),
    )
    .expect("open snapshot backend");
    EventStore::new(backend)
        .with_cache_capacity(64)
        .with_snapshot_policy(SnapshotPolicy::every_n_events(EVERY_N))
}

/// Drive an app-style store that persists snapshots, and **return it still
/// open** so its D9 store lock and sidecar writer lock stay held.
async fn build_live_app_store(dir: &std::path::Path) -> Store {
    let store = open_app_store(dir);
    for s in 0..STREAMS {
        let stream = format!("counter-{s}");
        for _ in 0..EVERY_N {
            store
                .command_cached::<Counter, Add>(&stream, Add(1))
                .await
                .expect("command");
        }
    }
    store
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

const STREAMS: usize = 4;
const EVERY_N: u64 = 3;

/// **The bn-fj34 headline.** The fold-version check no longer needs the store
/// to be stopped: with a live writer holding both the engine and the sidecar,
/// `doctor` still reads every persisted app snapshot and reports on it.
#[tokio::test(flavor = "multi_thread")]
async fn fold_version_check_runs_against_a_live_writer() {
    let dir = mess_testkit::sweeping_temp_dir("cli-doctor-lock-fold-live");
    let _live = build_live_app_store(dir.path()).await; // held open on purpose

    let json = json_of(&doctor::run(
        dir.path(),
        &DoctorOptions { expect_fold_version: Some(1) },
    ));

    // The check RAN, and it was not vacuous: it saw the live app's snapshots.
    let current = finding(&json, "fold-version-current").expect(
        "the fold-version check must run against a LIVE store (this is the \
         degradation bn-fj34 removed)",
    );
    assert_eq!(current["severity"], "ok");
    assert_eq!(json["fold_versions"], serde_json::json!([1]));
    assert!(
        finding(&json, "no-snapshots").is_none(),
        "a live store with persisted snapshots is not an empty live set"
    );

    // The degraded findings are gone, not merely quiet.
    assert!(finding(&json, "meta-store-locked").is_none());
    assert!(finding(&json, "registry-unavailable").is_none());

    // The one remaining live-writer degradation: the $registry fold needs the
    // engine, so it says so — at `info`, without failing the command.
    let reg = finding(&json, "registry-store-locked")
        .expect("the $registry fold still needs exclusive access");
    assert_eq!(
        reg["severity"], "info",
        "a live writer is expected, not a fault"
    );
    assert!(
        json["summary"]["worst"] == "info" || json["summary"]["worst"] == "ok",
        "a locked-store run is still a healthy exit: {:?}",
        json["summary"]
    );
}

/// The JSON envelope's top-level field names must not depend on lock state,
/// and every non-registry check must behave identically either way.
#[tokio::test(flavor = "multi_thread")]
async fn json_envelope_field_names_are_lock_state_independent() {
    let dir = mess_testkit::sweeping_temp_dir(
        "cli-doctor-lock-json-envelope-field-names",
    );
    {
        let _closed = build_live_app_store(dir.path()).await;
    } // dropped: locks released.

    let free = json_of(&doctor::run(dir.path(), &opts()));

    let _live = build_live_app_store(dir.path()).await; // held open.
    let locked = json_of(&doctor::run(dir.path(), &opts()));

    assert_eq!(
        keys(&free),
        keys(&locked),
        "top-level envelope field names must not depend on lock state"
    );

    // `fold_versions` is present in BOTH — and, since bn-fj34, populated in
    // both. It used to vanish (bn-1yz) and then to be present-but-empty on a
    // locked run (bn-ve0); now the locked run is just as informative.
    assert_eq!(free["fold_versions"], serde_json::json!([1]));
    assert_eq!(
        locked["fold_versions"],
        serde_json::json!([1]),
        "a live writer no longer hides the app's fold versions"
    );

    // Every check that reads the segment files keeps behaving identically.
    for (label, v) in [("free", &free), ("locked", &locked)] {
        assert!(
            finding(v, "fsync-ok").is_some(),
            "fsync check unaffected by lock state ({label})"
        );
        assert!(
            finding(v, "meta-store-locked").is_none(),
            "the deleted degradation must never reappear ({label})"
        );
    }

    // The D9 store lock itself is what differs, and doctor reports it plainly.
    assert!(finding(&free, "lock-free").is_some());
    assert!(
        finding(&locked, "lock-held").is_some()
            || finding(&locked, "lock-free").is_none(),
        "a live writer must not be reported as a free lock: {:?}",
        findings_of(&locked)
    );
}

/// An absent/never-written store is a *different* state from a live-locked one,
/// and neither is reported as a metadata-store failure: with no snapshots to
/// inspect the fold-version check reports the honest OK `no-snapshots`.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_store_reports_no_snapshots_not_an_unavailability() {
    let dir =
        mess_testkit::sweeping_temp_dir("cli-doctor-lock-old-registry-unavail");
    std::fs::create_dir_all(dir.path().join("seg")).ok();

    let json = json_of(&doctor::run(dir.path(), &opts()));

    let f = finding(&json, "no-snapshots")
        .expect("no sidecar at all => genuinely nothing to drift");
    assert_eq!(f["severity"], "ok");
    assert_eq!(json["fold_versions"], serde_json::json!([]));
    assert!(finding(&json, "meta-store-locked").is_none());
    assert!(
        finding(&json, "registry-unavailable").is_none(),
        "an absent sidecar is an empty answer, not an unavailable one"
    );
}
