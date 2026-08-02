//! bn-3dy acceptance: `mess doctor`'s fold-version drift check fires on a REAL
//! app store for the first time.
//!
//! Before this bone the check was structurally vacuous: an app persists its
//! snapshots into its own sidecar keyed by interim FNV stream ids, while `mess
//! doctor` read only the engine's own `<dir>/meta` and correlated by the engine
//! interner's ids — two id spaces that never intersected, so `doctor` saw
//! `no live snapshots` no matter what. And nothing persisted snapshots on the
//! warm path anyway. This test proves both halves are fixed end-to-end:
//!
//! 1. an app-style store (`PackSnapshotBackend<LogEngine>`) writing through
//!    `command_cached` under an opt-in [`SnapshotPolicy`] actually persists
//!    snapshots, and
//! 2. `doctor` (run in-process the way the other doctor tests do) reports the
//!    fold-version check as NON-vacuous — it sees every persisted snapshot, all
//!    current — and then FLAGS the drift once a second fold_version lands.
//!
//! bn-3l8n: the sidecar is the pack sidecar at
//! [`store::snapshot_pack_dir`](mess_cli::store::snapshot_pack_dir), and the
//! store is opened at exactly that path so the app and the CLI agree on the
//! convention (this test is the one that would break if they drifted apart).
//!
//! Hits the real filesystem, so it is `miri`-ignored.
#![cfg(not(miri))]

use mess_cli::doctor::{self, DoctorOptions};
use mess_cli::format::{self, Format};
use mess_cli::metaread;
use mess_core::{Aggregate, CodecError, Decide, Event};
use mess_store::{
    EventStore, LogEngine, PackSnapshotBackend, SnapshotPolicy, Snapshottable,
    StableSnapshotId, StateCodecError,
};
use serde_json::Value;

// ---------------------------------------------------------------------------
// A tiny snapshottable counter with an `Add` command (one event per command).
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
        StableSnapshotId::new("mess-cli.test.app-counter");
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

/// The SAME aggregate over the SAME blob, but a bumped `FOLD_VERSION` — the
/// "deploy a new fold" story that must show up as drift.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct CounterV2(Counter);

impl Aggregate for CounterV2 {
    type Event = Added;

    fn apply(&mut self, e: &Added) { self.0.apply(e); }
}

impl Snapshottable for CounterV2 {
    // Same aggregate, bumped fold: the schema id is shared on purpose.
    const AGGREGATE_SCHEMA_ID: StableSnapshotId =
        StableSnapshotId::new("mess-cli.test.app-counter");
    const FOLD_VERSION: u32 = 2;

    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
        self.0.encode_state()
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
        Counter::decode_state(bytes).map(CounterV2)
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

impl Decide<Add> for CounterV2 {
    type Rejection = Never;

    fn decide(&self, cmd: Add) -> Result<Vec<Added>, Never> {
        Ok(vec![Added(cmd.0)])
    }
}

type Store = EventStore<PackSnapshotBackend<LogEngine>>;

/// Open the app-style warm-write store: `EventStore` over
/// `PackSnapshotBackend<LogEngine>`, cache on, snapshots persisted under
/// `policy`. The snapshot sidecar is `mess_cli::store::snapshot_pack_dir(dir)`
/// — the exact location `metaread`/`doctor` look for app snapshots.
fn open_app_store(dir: &std::path::Path, policy: SnapshotPolicy) -> Store {
    let engine = LogEngine::open(dir).expect("open engine");
    let backend = PackSnapshotBackend::open(
        engine,
        mess_cli::store::snapshot_pack_dir(dir),
    )
    .expect("open snapshot backend");
    EventStore::new(backend)
        .with_cache_capacity(64)
        .with_snapshot_policy(policy)
}

fn json_of(report: &mess_cli::report::Report) -> Value {
    serde_json::from_str(&format::render(report, Format::Json)).unwrap()
}

fn finding<'a>(v: &'a Value, kind: &str) -> Option<&'a Value> {
    v["findings"].as_array().unwrap().iter().find(|f| f["kind"] == kind)
}

const STREAMS: usize = 4;
const EVERY_N: u64 = 3;

#[tokio::test(flavor = "multi_thread")]
async fn doctor_fold_version_check_fires_on_a_real_app_store() {
    let dir = mess_testkit::sweeping_temp_dir("cli-doctor-app-snap");

    // ---- Phase 1: a running app persists snapshots under the policy. ----
    {
        let store =
            open_app_store(dir.path(), SnapshotPolicy::every_n_events(EVERY_N));
        for s in 0..STREAMS {
            let stream = format!("counter-{s}");
            // EVERY_N commands => the event count crosses the first N-boundary,
            // so `command_cached` persists exactly one snapshot per stream from
            // the folded state it already holds (no extra replay).
            for _ in 0..EVERY_N {
                store
                    .command_cached::<Counter, Add>(&stream, Add(1))
                    .await
                    .expect("command");
            }
        }
        // No flush call: a pack save installs its root by atomic rename
        // before it returns, so a fresh reader sees it as soon as the writer
        // lock is released. (Buffered mode promises nothing about power loss —
        // which is the same discardable contract the fjall head buffer had.)
    } // store dropped: engine + sidecar writer locks released.

    // The fold-version check's data source now SEES every app snapshot — the
    // id spaces are unified (sidecar heads joined to their stream names).
    let facts = metaread::read(dir.path());
    assert_eq!(
        facts.snapshots.len(),
        STREAMS,
        "doctor's metaread must see every persisted app snapshot (was 0 — \
         structurally vacuous — before this bone)"
    );
    assert!(
        facts.snapshots.iter().all(|s| s.fold_version == 1),
        "every phase-1 snapshot carries fold_version 1"
    );

    // doctor, with the operator's expected fold: NON-vacuous and all current.
    let json = json_of(&doctor::run(
        dir.path(),
        &DoctorOptions { expect_fold_version: Some(1) },
    ));
    let current = finding(&json, "fold-version-current")
        .expect("check must report the snapshots as current, not vacuous");
    assert_eq!(current["severity"], "ok");
    assert!(
        finding(&json, "no-snapshots").is_none(),
        "the check is no longer vacuous: it has real snapshots to inspect"
    );
    assert_eq!(json["fold_versions"], serde_json::json!([1]));

    // doctor with no expectation: a single consistent fold, still non-vacuous.
    let consistent =
        json_of(&doctor::run(dir.path(), &DoctorOptions::default()));
    assert!(
        finding(&consistent, "fold-version-consistent").is_some(),
        "a single-fold live set reads as consistent, not vacuous"
    );

    // ---- Phase 2: deploy bumps a fold_version -> the check FLAGS drift. ----
    {
        let store =
            open_app_store(dir.path(), SnapshotPolicy::every_n_events(EVERY_N));
        for _ in 0..EVERY_N {
            store
                .command_cached::<CounterV2, Add>("counter-v2", Add(1))
                .await
                .expect("v2 command");
        }
    }

    // No expectation: the live set now spans fold_versions {1, 2} -> drift.
    let drift = json_of(&doctor::run(dir.path(), &DoctorOptions::default()));
    let f = finding(&drift, "fold-version-drift")
        .expect("a bumped fold_version must surface as drift");
    assert_eq!(f["severity"], "warn");
    let folds = drift["fold_versions"].as_array().unwrap();
    assert!(
        folds.contains(&serde_json::json!(1))
            && folds.contains(&serde_json::json!(2)),
        "the live set spans both folds: {folds:?}"
    );

    // With an explicit expectation the stale-fold snapshots are flagged too.
    let expect2 = json_of(&doctor::run(
        dir.path(),
        &DoctorOptions { expect_fold_version: Some(2) },
    ));
    let ef = finding(&expect2, "fold-version-drift")
        .expect("snapshots not at the expected fold must drift");
    assert_eq!(ef["severity"], "warn");
}

/// The default policy is OFF: a warm-write app that does not opt in persists no
/// snapshots, so nothing changes for existing users and `doctor` correctly
/// reports `no-snapshots` (genuinely nothing to drift — not a vacuous check).
#[tokio::test(flavor = "multi_thread")]
async fn default_policy_persists_nothing_and_check_stays_ok() {
    let dir = mess_testkit::sweeping_temp_dir("cli-doctor-app-drift");
    {
        let store = open_app_store(dir.path(), SnapshotPolicy::never());
        for s in 0..STREAMS {
            let stream = format!("counter-{s}");
            for _ in 0..(EVERY_N * 3) {
                store
                    .command_cached::<Counter, Add>(&stream, Add(1))
                    .await
                    .expect("command");
            }
        }
    }

    let facts = metaread::read(dir.path());
    assert!(
        facts.snapshots.is_empty(),
        "default (never) policy must persist no snapshots"
    );

    let json = json_of(&doctor::run(dir.path(), &DoctorOptions::default()));
    let f = finding(&json, "no-snapshots")
        .expect("no persisted snapshots => the OK no-snapshots finding");
    assert_eq!(f["severity"], "ok");
}
