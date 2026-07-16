//! Exact A current-product correctness child for the Asterism rebaseline.
//!
//! This source is materialized as a current `mess-store` example together with
//! the reviewed cfg(test)-only product overlay. It is correctness authority,
//! never a timed surface.

#[path = "asterism_rebaseline_shared/allocation.rs"]
mod allocation;
#[path = "asterism_rebaseline_shared/control.rs"]
mod control;
#[path = "asterism_rebaseline_shared/digest.rs"]
mod digest;
#[path = "asterism_rebaseline_shared/schema.rs"]
mod schema;
#[path = "asterism_rebaseline_shared/semantic_oracle.rs"]
mod semantic_oracle;

mod contract {
    pub const PROTOCOL: &str = "bn-2l3n-asterism-rebaseline-v3";
    pub const PROTOCOL_SHA256: &str =
        "d9ee10b2cccdaf6428bf1419a8c2ee74d272e987dc3617a80b64ad2e9d7a18dd";
    pub const VARIANT: &str = "A";
}

use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use allocation::{AllocationSnapshot, CountingAllocator};
use control::{Control, MeasuredMarkers, cpu_snapshot, monotonic_ns};
use mess_core::{Aggregate, CodecError, Decide, Event};
use mess_store::backend::{Backend, OwnedAppendBatch, RecordToAppend};
use mess_store::engine::TestEngineHook;
use mess_store::{
    AppendError, Durability, EngineOptions, EventStore, FjallSnapshotBackend,
    LogEngine, Snapshottable, StateCodecError, Version,
};
use schema::{canonical_object, json_bool, json_string};
use semantic_oracle::OracleEvent;

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const CASES: [(&str, &str); 7] = [
    ("public-ordinary-append-command-cache-read-subscribe", "correctness"),
    ("same-stream-exact-race", "correctness"),
    ("registry-first-use-ordered-failure-unit", "correctness"),
    ("error-ordering", "correctness"),
    ("borrowed-owned-mixed-order-and-type", "correctness"),
    ("two-live-rolls", "roll-recovery"),
    ("clean-repeated-active-tail-sealed-recovery", "roll-recovery"),
];

struct CorrectnessArgs {
    attempt_nonce: String,
    phase:         String,
}

enum Invocation {
    Correctness(CorrectnessArgs),
    Smoke,
}

const CURRENT_EVENT_NAME: &str = "asterism.rebaseline.current-event";

#[derive(Clone, Debug, Eq, PartialEq)]
struct CurrentEvent(u64);

impl Event for CurrentEvent {
    fn name(&self) -> &'static str { CURRENT_EVENT_NAME }

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.to_le_bytes().to_vec())
    }

    fn decode(name: &str, bytes: &[u8]) -> Result<Self, CodecError> {
        if name != CURRENT_EVENT_NAME {
            return Err(CodecError::UnknownEventName(name.to_owned()));
        }
        let bytes: [u8; 8] =
            bytes.try_into().map_err(|_| CodecError::Decode {
                event_name: name.to_owned(),
                source:     "current correctness event must be eight bytes"
                    .into(),
            })?;
        Ok(Self(u64::from_le_bytes(bytes)))
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct CurrentAggregate {
    events: u64,
    total:  u64,
}

impl Aggregate for CurrentAggregate {
    type Event = CurrentEvent;

    fn apply(&mut self, event: &Self::Event) {
        self.events += 1;
        self.total += event.0;
    }
}

#[derive(Clone, Copy, Debug)]
struct CurrentCommand(u64);

impl Decide<CurrentCommand> for CurrentAggregate {
    type Rejection = Infallible;

    fn decide(
        &self,
        command: CurrentCommand,
    ) -> Result<Vec<Self::Event>, Self::Rejection> {
        Ok(vec![CurrentEvent(command.0)])
    }
}

impl Snapshottable for CurrentAggregate {
    const FOLD_VERSION: u32 = 1;

    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
        let mut bytes = Vec::with_capacity(16);
        bytes.extend_from_slice(&self.events.to_le_bytes());
        bytes.extend_from_slice(&self.total.to_le_bytes());
        Ok(bytes)
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
        if bytes.len() != 16 {
            return Err(StateCodecError(
                "current correctness state must be sixteen bytes".to_owned(),
            ));
        }
        let events =
            u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes"));
        let total = u64::from_le_bytes(bytes[8..].try_into().expect("8 bytes"));
        Ok(Self { events, total })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CurrentExtensionObservations {
    domain_events:  u64,
    fresh_streams:  u64,
    public_appends: u64,
}

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing {name}"))
}

fn lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn invocation() -> Invocation {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments == ["--smoke"] {
        assert_eq!(required("ASTERISM_REBASELINE_MODE"), "smoke");
        assert_eq!(required("ASTERISM_REBASELINE_SMOKE_TARGET"), "correctness",);
        assert_eq!(
            required("ASTERISM_REBASELINE_PROTOCOL"),
            contract::PROTOCOL
        );
        Invocation::Smoke
    } else {
        Invocation::Correctness(correctness_args())
    }
}

fn correctness_args() -> CorrectnessArgs {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    assert_eq!(arguments.len(), 11, "current-product correctness argv differs");
    assert_eq!(arguments[0], "--correctness");
    assert_eq!(arguments[1], "--protocol");
    assert_eq!(arguments[2], contract::PROTOCOL);
    assert_eq!(arguments[3], "--attempt-nonce");
    assert_eq!(arguments[5], "--variant");
    assert_eq!(arguments[6], contract::VARIANT);
    assert_eq!(arguments[7], "--phase");
    assert!(matches!(arguments[8].as_str(), "pre" | "post"));
    assert_eq!(arguments[9], "--suite");
    assert_eq!(arguments[10], "current-product");
    let attempt_nonce = arguments[4].clone();
    let phase = arguments[8].clone();
    assert!(lower_hex(&attempt_nonce, 64), "invalid correctness attempt nonce");
    assert_eq!(required("ASTERISM_REBASELINE_MODE"), "correctness");
    assert_eq!(required("ASTERISM_REBASELINE_ATTEMPT_NONCE"), attempt_nonce);
    assert_eq!(required("ASTERISM_REBASELINE_PHASE"), phase);
    assert_eq!(required("ASTERISM_REBASELINE_PROTOCOL"), contract::PROTOCOL);
    assert_eq!(required("ASTERISM_REBASELINE_SUITE"), "current-product");
    assert_eq!(required("ASTERISM_REBASELINE_VARIANT"), contract::VARIANT);
    CorrectnessArgs { attempt_nonce, phase }
}

fn process_options(segment_size: u64) -> EngineOptions {
    EngineOptions {
        durability: Durability::Process,
        segment_size,
        ..Default::default()
    }
}

fn record(message_type: &str, data: impl Into<Vec<u8>>) -> RecordToAppend {
    RecordToAppend {
        message_type: message_type.to_owned(),
        data:         data.into(),
    }
}

fn segment_count(root: &Path) -> usize {
    std::fs::read_dir(root)
        .expect("read correctness segment directory")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry.path().extension().and_then(|value| value.to_str())
                == Some("log")
        })
        .count()
}

async fn case_public_composition(
    store: &EventStore<FjallSnapshotBackend<LogEngine>>,
    engine: &LogEngine,
) {
    let shared =
        semantic_oracle::run_generation_neutral_semantic_oracle(store).await;
    assert_eq!(
        shared,
        semantic_oracle::SemanticOracleObservations {
            domain_events:  4,
            fresh_streams:  2,
            public_appends: 3,
        },
    );
    let shared_metrics = engine.metrics();
    let high_water = engine.total_events() as u64;
    assert_eq!(shared_metrics.total_events, high_water);
    assert_eq!(shared_metrics.durable_watermark, high_water);
    assert_eq!(
        high_water,
        shared.domain_events + shared.fresh_streams + 1,
        "A semantic oracle high-water differs from domain + streams + type",
    );
    assert_eq!(
        shared_metrics.commit.batches,
        shared.public_appends + shared.fresh_streams,
    );

    let current_store = EventStore::new(store.backend().clone())
        .with_page_size(16)
        .with_cache_capacity(16);
    let current = case_current_command_cache_extension(&current_store).await;
    let current_metrics = engine.metrics();
    let current_high_water = engine.total_events() as u64;
    assert_eq!(current_metrics.total_events, current_high_water);
    assert_eq!(current_metrics.durable_watermark, current_high_water);
    assert_eq!(
        current_high_water
            .checked_sub(high_water)
            .expect("A current extension high-water regressed"),
        current.domain_events + current.fresh_streams + 1,
        "A-only extension high-water differs from domain + streams + type",
    );
    assert_eq!(
        current_metrics
            .commit
            .batches
            .checked_sub(shared_metrics.commit.batches)
            .expect("A current extension batch count regressed"),
        current.public_appends + current.fresh_streams,
    );
}

async fn case_current_command_cache_extension(
    store: &EventStore<FjallSnapshotBackend<LogEngine>>,
) -> CurrentExtensionObservations {
    assert!(store.cache().is_enabled(), "A-only cache must be enabled");
    assert!(store.cache().is_empty(), "A-only cache must begin empty");

    let commanded = store
        .command::<CurrentAggregate, _>("current-command", CurrentCommand(5))
        .await
        .expect("A-only ordinary command");
    assert_eq!(
        (commanded.events_appended, commanded.version),
        (1, Version::At(0))
    );
    let cached_first = store
        .command_cached::<CurrentAggregate, _>(
            "current-cache",
            CurrentCommand(7),
        )
        .await
        .expect("A-only cold cached command");
    assert_eq!(
        (cached_first.events_appended, cached_first.version),
        (1, Version::At(0))
    );
    let cached_second = store
        .command_cached::<CurrentAggregate, _>(
            "current-cache",
            CurrentCommand(11),
        )
        .await
        .expect("A-only warm cached command");
    assert_eq!(
        (cached_second.events_appended, cached_second.version),
        (1, Version::At(1))
    );

    let command_load = store
        .load::<CurrentAggregate>("current-command")
        .await
        .expect("A-only ordinary command load");
    assert_eq!(
        (command_load.events_replayed, command_load.version),
        (1, Version::At(0))
    );
    assert_eq!(command_load.state, CurrentAggregate { events: 1, total: 5 });
    let hot = store
        .load_hot::<CurrentAggregate>("current-cache")
        .await
        .expect("A-only hot load");
    assert_eq!((hot.events_replayed, hot.version), (0, Version::At(1)));
    assert_eq!(hot.state, CurrentAggregate { events: 2, total: 18 });
    assert_eq!(store.cache().len(), 1);
    assert_eq!(
        store.cache().get::<CurrentAggregate>("current-cache"),
        Some((Version::At(1), CurrentAggregate { events: 2, total: 18 }))
    );
    assert_eq!(
        store
            .backend()
            .head("current-command")
            .await
            .expect("A-only command head"),
        Version::At(0),
    );
    assert_eq!(
        store.backend().head("current-cache").await.expect("A-only cache head"),
        Version::At(1),
    );

    let global = store
        .backend()
        .read_global(None, 16)
        .await
        .expect("A-only global read");
    assert_eq!(global.len(), 7, "four shared + three A-only events");
    assert!(
        global
            .windows(2)
            .all(|pair| pair[0].global_position < pair[1].global_position)
    );
    let expected = [
        ("current-command", 0, 5u64),
        ("current-cache", 0, 7u64),
        ("current-cache", 1, 11u64),
    ];
    for (record, (stream, position, value)) in global[4..].iter().zip(expected)
    {
        assert_eq!(record.stream_id, stream);
        assert_eq!(record.stream_position, position);
        assert_eq!(record.message_type, CURRENT_EVENT_NAME);
        assert_eq!(record.data, value.to_le_bytes());
    }

    let command_cursor =
        commanded.last_global_position.expect("A-only command global cursor");
    let last_cursor = cached_second
        .last_global_position
        .expect("A-only cached command global cursor");
    store.await_past(last_cursor).await.expect("A-only publication barrier");
    assert!(store.watermark().await.expect("A-only watermark") > last_cursor);
    let mut subscription = store.subscribe(Some(command_cursor));
    let records =
        subscription.next_batch().await.expect("A-only subscription history");
    assert_eq!(records.len(), 3);
    assert_eq!(records, global[4..]);

    CurrentExtensionObservations {
        domain_events:  3,
        fresh_streams:  2,
        public_appends: 3,
    }
}

async fn case_same_stream_exact_race(
    store: &EventStore<FjallSnapshotBackend<LogEngine>>,
) {
    let seeded = store
        .append(
            "current-exact-race",
            Version::NoStream,
            &[OracleEvent::new(b"seed".to_vec())],
        )
        .await
        .expect("seed exact race");
    assert_eq!(seeded.version, Version::At(0));

    const CONTENDERS: usize = 32;
    let start = Arc::new(tokio::sync::Barrier::new(CONTENDERS + 1));
    let mut tasks = Vec::with_capacity(CONTENDERS);
    for contender in 0..CONTENDERS {
        let start = Arc::clone(&start);
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            start.wait().await;
            store
                .append(
                    "current-exact-race",
                    Version::At(0),
                    &[OracleEvent::new(
                        (contender as u64).to_le_bytes().to_vec(),
                    )],
                )
                .await
        }));
    }
    start.wait().await;
    let mut wins = 0;
    let mut conflicts = 0;
    for task in tasks {
        match task.await.expect("exact race contender panicked") {
            Ok(commit) => {
                wins += 1;
                assert_eq!(commit.version, Version::At(1));
            }
            Err(AppendError::Conflict { expected, actual }) => {
                conflicts += 1;
                assert_eq!(expected, Version::At(0));
                assert_eq!(actual, Version::At(1));
            }
            Err(other) => panic!("unexpected exact-race error: {other}"),
        }
    }
    assert_eq!(wins, 1);
    assert_eq!(conflicts, CONTENDERS - 1);
    let loaded = store
        .load::<semantic_oracle::OracleAggregate>("current-exact-race")
        .await
        .expect("load exact race");
    assert_eq!((loaded.events_replayed, loaded.version), (2, Version::At(1)));
}

async fn case_registry_first_use_ordered_failure_unit(root: &Path) {
    let engine = LogEngine::open(root).expect("open registry-failure store");
    let store = EventStore::new(engine.clone());
    let seeded = store
        .append(
            "registry-hot-predecessor",
            Version::NoStream,
            &[OracleEvent::new(b"seed".to_vec())],
        )
        .await
        .expect("seed hot predecessor and shared event type");
    assert_eq!(seeded.version, Version::At(0));

    // Both submissions enter one owner cohort. The first post-arm pwrite is
    // the already-registered hot predecessor and must reach the real file.
    // The next pwrite is the fresh stream's registry unit; inject EIO there.
    let cohort = engine.arm_test_owner_cohort(2);
    let write_fault = engine.arm_test_hook(TestEngineHook::PwriteEio {
        after_successful_pwrites: 1,
    });
    let predecessor_store = store.clone();
    let predecessor = tokio::spawn(async move {
        predecessor_store
            .append(
                "registry-hot-predecessor",
                Version::At(0),
                &[OracleEvent::new(b"hot-predecessor".to_vec())],
            )
            .await
    });
    cohort.wait_until_admitted(1);
    let fresh_store = store.clone();
    let fresh = tokio::spawn(async move {
        fresh_store
            .append(
                "registry-fresh-after-predecessor",
                Version::NoStream,
                &[OracleEvent::new(b"fresh-must-not-land".to_vec())],
            )
            .await
    });
    cohort.wait_until_admitted(2);
    write_fault.wait_until_reached();
    write_fault.release();

    let predecessor = predecessor
        .await
        .expect("hot predecessor task panicked")
        .expect("hot predecessor pwrite must succeed");
    assert_eq!(predecessor.version, Version::At(1));
    let fresh = fresh.await.expect("fresh registry task panicked");
    assert!(matches!(fresh, Err(AppendError::Backend(_))));
    assert_eq!(
        engine.stream_id_of("registry-fresh-after-predecessor"),
        None,
        "failed registry unit must not publish a stream id",
    );
    assert_eq!(
        engine
            .head("registry-fresh-after-predecessor")
            .await
            .expect("fresh stream head after registry failure"),
        Version::NoStream,
    );
    drop(write_fault);
    drop(cohort);
    drop(store);
    drop(engine);

    let engine = LogEngine::open(root).expect("reopen after registry-unit EIO");
    let store = EventStore::new(engine.clone());
    let hot = store
        .load::<semantic_oracle::OracleAggregate>("registry-hot-predecessor")
        .await
        .expect("load hot predecessor after reopen");
    assert_eq!((hot.events_replayed, hot.version), (2, Version::At(1)));
    let fresh = store
        .append(
            "registry-fresh-after-predecessor",
            Version::NoStream,
            &[OracleEvent::new(b"fresh-retry".to_vec())],
        )
        .await
        .expect("fresh stream retry must register and commit");
    assert_eq!(fresh.version, Version::At(0));
    drop(store);
    drop(engine);

    let engine = LogEngine::open(root).expect("second registry-failure reopen");
    let records = engine
        .read_stream("registry-fresh-after-predecessor", Version::NoStream, 2)
        .await
        .expect("read retried fresh stream");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].data, b"fresh-retry");
}

async fn case_error_ordering(root: &Path) {
    let engine = LogEngine::open(root).expect("open error-ordering store");
    engine
        .append_batch(
            "error-ordering-stream",
            Version::NoStream,
            &[record("error-ordering.type", vec![1])],
        )
        .await
        .expect("prime error-ordering stream");
    let oversized = OwnedAppendBatch::from_records(vec![record(
        "error-ordering.type",
        vec![0; 64 * 1024 * 1024],
    )]);
    let error = engine
        .append_batch_owned(
            "error-ordering-stream",
            Version::NoStream,
            oversized,
        )
        .await
        .expect_err("stale oversized owned append must conflict first");
    assert!(matches!(
        error,
        AppendError::Conflict {
            expected: Version::NoStream,
            actual:   Version::At(0),
        }
    ));
}

async fn case_borrowed_owned_mixed_order_and_type(root: &Path) {
    let engine = LogEngine::open(root).expect("open borrowed-owned store");
    let borrowed = vec![
        record("mixed.a", vec![1, 2]),
        record("mixed.b", vec![3]),
        record("mixed.a", vec![4, 5]),
    ];
    engine
        .append_batch("mixed-stream", Version::NoStream, &borrowed)
        .await
        .expect("borrowed mixed append");
    let owned = vec![
        record("mixed.b", vec![6]),
        record("mixed.c", Vec::new()),
        record("mixed.a", vec![7, 8, 9]),
    ];
    engine
        .append_batch_owned(
            "mixed-stream",
            Version::At(2),
            OwnedAppendBatch::from_records(owned.clone()),
        )
        .await
        .expect("owned mixed append");
    let observed = engine
        .read_stream("mixed-stream", Version::NoStream, 16)
        .await
        .expect("read mixed stream");
    let expected: Vec<_> = borrowed.iter().chain(&owned).collect();
    assert_eq!(observed.len(), expected.len());
    for (position, (actual, expected)) in
        observed.iter().zip(expected).enumerate()
    {
        assert_eq!(actual.stream_position, position as u64);
        assert_eq!(actual.message_type, expected.message_type);
        assert_eq!(actual.data, expected.data);
    }
}

async fn case_two_live_rolls(root: &Path) {
    let engine = LogEngine::open_with(root, process_options(8 * 1024))
        .expect("open live-roll store");
    let mut expected = Version::NoStream;
    let mut payloads = Vec::new();
    for ordinal in 0..256u64 {
        let mut payload = ordinal.to_le_bytes().to_vec();
        payload.resize(1_024, (ordinal % 251) as u8);
        let commit = engine
            .append_batch(
                "two-live-rolls",
                expected,
                &[record("live-roll.type", payload.clone())],
            )
            .await
            .expect("append live-roll corpus");
        expected = commit.version;
        payloads.push(payload);
        if segment_count(root) >= 3 {
            break;
        }
    }
    assert!(segment_count(root) >= 3, "corpus did not force two live rolls");
    let observed = engine
        .read_stream("two-live-rolls", Version::NoStream, payloads.len() + 1)
        .await
        .expect("read live-roll corpus");
    assert_eq!(observed.len(), payloads.len());
    for (ordinal, (record, payload)) in
        observed.iter().zip(&payloads).enumerate()
    {
        assert_eq!(record.stream_position, ordinal as u64);
        assert_eq!(&record.data, payload);
    }
}

async fn assert_recovery_corpus(engine: &LogEngine, payloads: &[Vec<u8>]) {
    let records = engine
        .read_stream("repeated-recovery", Version::NoStream, payloads.len() + 1)
        .await
        .expect("read recovery corpus");
    assert_eq!(records.len(), payloads.len());
    for (ordinal, (record, payload)) in records.iter().zip(payloads).enumerate()
    {
        assert_eq!(record.stream_position, ordinal as u64);
        assert_eq!(&record.data, payload);
    }
}

async fn case_clean_repeated_active_tail_sealed_recovery(root: &Path) {
    let options = process_options(16 * 1024);
    let mut payloads = Vec::new();
    {
        let engine = LogEngine::open_with(root, options.clone())
            .expect("open recovery store");
        let mut expected = Version::NoStream;
        for ordinal in 0..18u64 {
            let mut payload = ordinal.to_le_bytes().to_vec();
            payload.resize(512, (ordinal % 251) as u8);
            let commit = engine
                .append_batch(
                    "repeated-recovery",
                    expected,
                    &[record("recovery.type", payload.clone())],
                )
                .await
                .expect("append sealed recovery prefix");
            expected = commit.version;
            payloads.push(payload);
        }
        engine.seal_active().expect("seal recovery prefix");
        assert!(engine.sealed_segment_count() > 0);
        for ordinal in 18..24u64 {
            let mut payload = ordinal.to_le_bytes().to_vec();
            payload.resize(512, (ordinal % 251) as u8);
            let commit = engine
                .append_batch(
                    "repeated-recovery",
                    expected,
                    &[record("recovery.type", payload.clone())],
                )
                .await
                .expect("append active recovery tail");
            expected = commit.version;
            payloads.push(payload);
        }
        assert_recovery_corpus(&engine, &payloads).await;
    }
    for cycle in 0..3 {
        let engine = LogEngine::open_with(root, options.clone())
            .unwrap_or_else(|error| panic!("recovery cycle {cycle}: {error}"));
        assert!(
            engine.sealed_segment_count() > 0,
            "recovery cycle {cycle}: sealed tier absent",
        );
        assert_recovery_corpus(&engine, &payloads).await;
    }
}

fn cases_json() -> String {
    let cases: Vec<String> = CASES
        .iter()
        .map(|(id, classification)| {
            canonical_object(&[
                ("classification", json_string(classification)),
                ("id", json_string(id)),
                ("status", json_string("PASS")),
            ])
        })
        .collect();
    format!("[{}]", cases.join(","))
}

fn emit_result(args: &CorrectnessArgs) {
    println!(
        "{}",
        canonical_object(&[
            ("attempt_nonce", json_string(&args.attempt_nonce)),
            ("boundedness", "null".to_owned()),
            ("cases", cases_json()),
            ("harness_sound", json_bool(true)),
            ("phase", json_string(&args.phase)),
            ("protocol", json_string(contract::PROTOCOL)),
            ("schema", json_string("bn-2l3n-correctness-child-v3")),
            ("suite", json_string("current-product")),
            ("variant", json_string(contract::VARIANT)),
        ])
    );
}

fn emit_smoke_result() {
    println!(
        "{}",
        canonical_object(&[
            ("cases", cases_json()),
            ("harness_sound", json_bool(true)),
            ("protocol", json_string(contract::PROTOCOL)),
            ("schema", json_string("bn-2l3n-smoke-v3"),),
            ("smoke_target", json_string("correctness")),
            ("status", json_string("PASS")),
            ("variant", json_string(contract::VARIANT)),
        ])
    );
}

fn main() {
    let invocation = invocation();
    let mode = match &invocation {
        Invocation::Correctness(_) => "correctness",
        Invocation::Smoke => "smoke",
    };
    control::validate_perf_environment_mode(mode);
    control::authorize_ptracer_from_env();
    let root = PathBuf::from(required("ASTERISM_REBASELINE_STORE"));
    assert!(!root.exists(), "correctness store root must be absent");

    let mut control = Control::connect();
    let boot_nonce = control.boot();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_keep_alive(std::time::Duration::from_secs(3_600))
        .enable_all()
        .build()
        .expect("create correctness runtime");
    let runtime_nonce = control.runtime(&boot_nonce);
    let public_engine = LogEngine::open_with(
        root.join("case-01-public/log"),
        process_options(8 * 1024 * 1024),
    )
    .expect("open public-composition engine");
    let public_backend = FjallSnapshotBackend::open(
        public_engine.clone(),
        root.join("case-01-public/snapshots"),
    )
    .expect("open public-composition snapshot wrapper");
    let public_store = EventStore::new(public_backend).with_page_size(16);
    let opened_nonce = control.opened(&runtime_nonce);
    let ready_monotonic_ns = monotonic_ns();
    let alloc_before: AllocationSnapshot = allocation::snapshot();
    let cpu_before = cpu_snapshot();
    let counter_start_monotonic_ns = monotonic_ns();
    let start_nonce = control.ready_and_wait_start(
        &opened_nonce,
        alloc_before.calls,
        alloc_before.bytes,
        cpu_before.user_ns,
        cpu_before.system_ns,
        ready_monotonic_ns,
        counter_start_monotonic_ns,
    );
    let t0_monotonic_ns = monotonic_ns();
    let release_monotonic_ns = monotonic_ns();
    runtime.block_on(async {
        case_public_composition(&public_store, &public_engine).await;
        case_same_stream_exact_race(&public_store).await;
        case_registry_first_use_ordered_failure_unit(
            &root.join("case-03-registry-failure"),
        )
        .await;
        case_error_ordering(&root.join("case-04-error-ordering")).await;
        case_borrowed_owned_mixed_order_and_type(
            &root.join("case-05-borrowed-owned"),
        )
        .await;
        case_two_live_rolls(&root.join("case-06-two-live-rolls")).await;
        case_clean_repeated_active_tail_sealed_recovery(
            &root.join("case-07-repeated-recovery"),
        )
        .await;
    });
    let last_completion_monotonic_ns = monotonic_ns();
    let t1_monotonic_ns = monotonic_ns();
    let alloc_after = allocation::snapshot();
    let cpu_after = cpu_snapshot();
    let counter_end_monotonic_ns = monotonic_ns();
    control.measured_and_wait_release(
        &start_nonce,
        MeasuredMarkers {
            allocation_calls_end: alloc_after.calls,
            allocated_bytes_end: alloc_after.bytes,
            counter_end_monotonic_ns,
            last_completion_monotonic_ns,
            release_monotonic_ns,
            process_system_cpu_end_ns: cpu_after.system_ns,
            process_user_cpu_end_ns: cpu_after.user_ns,
            t0_monotonic_ns,
            t1_monotonic_ns,
        },
    );
    drop(public_store);
    drop(public_engine);
    runtime.shutdown_timeout(std::time::Duration::from_secs(30));
    match invocation {
        Invocation::Correctness(args) => emit_result(&args),
        Invocation::Smoke => emit_smoke_result(),
    }
}
