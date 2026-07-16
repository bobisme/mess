//! Common public A/C/D measurement overlay for the bn-2l3n rebaseline.
//!
//! This file is byte-identical in all three product archives. Only the
//! reviewed `asterism_rebaseline_adapter.rs` differs by generation.

#[path = "asterism_rebaseline_adapter.rs"]
mod adapter;
#[path = "asterism_rebaseline_shared/allocation.rs"]
mod allocation;
#[path = "asterism_rebaseline_shared/contract.rs"]
mod contract;
#[path = "asterism_rebaseline_shared/control.rs"]
mod control;
#[path = "asterism_rebaseline_shared/digest.rs"]
mod digest;
#[path = "asterism_rebaseline_shared/schema.rs"]
mod schema;
#[path = "asterism_rebaseline_shared/timing.rs"]
mod timing;
#[path = "asterism_rebaseline_shared/workload.rs"]
mod workload;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use allocation::{AllocationSnapshot, CountingAllocator};
use control::{Control, MeasuredMarkers, cpu_snapshot, monotonic_ns};
use digest::LogicalDigest;
use mess_core::{Aggregate, CodecError, Event};
use mess_store::{
    AppendError, Durability, EngineOptions, EventStore, FjallSnapshotBackend,
    LogEngine, Version,
};
use schema::{
    canonical_object, json_available, json_bool, json_string, json_u64,
};
use timing::{
    FairnessInput, StartGate, fairness_ppb, nearest_rank, post_warmup,
};
use workload::{DurabilityKind, Workload, assert_payload_contract, payload_bytes};

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const BINARY_KIND: &str = "public";
const TIMED_SURFACE: &str = "public-event-store";

#[derive(Clone, Debug)]
struct BenchEvent {
    payload: Vec<u8>,
}

#[derive(Clone, Debug)]
struct RejectedEvent;

impl Event for BenchEvent {
    fn name(&self) -> &'static str { "asterism.rebaseline.event" }

    fn encode(&self) -> Result<Vec<u8>, CodecError> { Ok(self.payload.clone()) }

    fn decode(name: &str, bytes: &[u8]) -> Result<Self, CodecError> {
        if name != "asterism.rebaseline.event" {
            return Err(CodecError::UnknownEventName(name.to_owned()));
        }
        Ok(Self { payload: bytes.to_vec() })
    }
}

impl Event for RejectedEvent {
    fn name(&self) -> &'static str { "asterism.rebaseline.rejected" }

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Err(CodecError::Encode("common oracle rejection".to_owned()))
    }

    fn decode(_name: &str, _bytes: &[u8]) -> Result<Self, CodecError> {
        unreachable!("the rejected oracle event is never durable")
    }
}

struct WriterResult {
    completed:               u64,
    completion_monotonic_ns: u64,
    cursor_monotone:         bool,
    digest:                  LogicalDigest,
    latencies:               Vec<u64>,
}

struct WriterSample {
    completed_appends: u64,
    completed_events:  u64,
    elapsed_ns:        u64,
    max_ns:            u64,
    p50_ns:            u64,
    p99_ns:            u64,
    writer:            u64,
}

struct PointResult {
    allocations:               AllocationSnapshot,
    appends:                   u64,
    borrowed_batches:          Option<u64>,
    borrowed_payload_bytes:    Option<u64>,
    borrowed_records:          Option<u64>,
    copied_bytes:              Option<u64>,
    copied_records:            Option<u64>,
    digest:                    u64,
    domain_events:             u64,
    fsync_count:               u64,
    fsync_degraded:            bool,
    fsync_max_ns:              u64,
    fsync_p50_ns:              u64,
    fsync_p95_ns:              u64,
    fsync_p99_ns:              u64,
    fsync_total_ns:            u64,
    groups:                    u64,
    log_events:                u64,
    latency_samples:           u64,
    owned_batches:             Option<u64>,
    owned_payload_bytes:       Option<u64>,
    owned_records:             Option<u64>,
    opaque_cursor_monotone:    bool,
    p50_ns:                    u64,
    p99_ns:                    u64,
    max_ns:                    u64,
    path_label:                &'static str,
    payload_bytes:             u64,
    wall_ns:                   u64,
    waiter_reservations_after: Option<u64>,
    byte_reservations_after:   Option<u64>,
    writer_samples:            Vec<WriterSample>,
}

struct ReopenResult {
    domain_events:            u64,
    log_events:               u64,
    logical_digest:           String,
    recovery_payload_decodes: u64,
    registry_head_digest:     String,
    visible_events:           u64,
    wall_ns:                  u64,
}

#[derive(Default)]
struct CorpusAggregate {
    digest: LogicalDigest,
    events: u64,
}

impl Aggregate for CorpusAggregate {
    type Event = BenchEvent;

    fn apply(&mut self, event: &Self::Event) {
        self.events += 1;
        self.digest.update_bytes(&event.payload);
    }
}

struct CorpusVerification {
    domain_events:        u64,
    logical_digest:       String,
    registry_head_digest: String,
}

struct CorrectnessOracleArgs {
    attempt_nonce: String,
}

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing {name}"))
}

fn correctness_oracle_args() -> CorrectnessOracleArgs {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    assert_eq!(arguments.len(), 11, "correctness-oracle argv differs");
    assert_eq!(arguments[0], "--correctness-oracle");
    assert_eq!(arguments[1], "--protocol");
    assert_eq!(arguments[2], contract::PROTOCOL);
    assert_eq!(arguments[3], "--attempt-nonce");
    assert_eq!(arguments[5], "--variant");
    assert_eq!(arguments[6], contract::VARIANT);
    assert_eq!(arguments[7], "--phase");
    assert_eq!(arguments[8], "oracle");
    assert_eq!(arguments[9], "--suite");
    assert_eq!(arguments[10], "common-public-oracle");
    let attempt_nonce = arguments[4].clone();
    assert!(
        attempt_nonce.len() == 64
            && attempt_nonce.bytes().all(|byte| {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
            }),
        "invalid correctness attempt nonce"
    );
    assert_eq!(required("ASTERISM_REBASELINE_ATTEMPT_NONCE"), attempt_nonce);
    assert_eq!(required("ASTERISM_REBASELINE_PHASE"), "oracle");
    assert_eq!(required("ASTERISM_REBASELINE_PROTOCOL"), contract::PROTOCOL);
    assert_eq!(required("ASTERISM_REBASELINE_SUITE"), "common-public-oracle");
    assert_eq!(required("ASTERISM_REBASELINE_VARIANT"), contract::VARIANT);
    CorrectnessOracleArgs { attempt_nonce }
}

fn validate_row_args(track: &str) {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    assert_eq!(arguments.len(), 7, "row argv differs");
    assert_eq!(arguments[0], "--run-row");
    assert_eq!(arguments[1], "--track");
    assert_eq!(arguments[2], track);
    assert_eq!(arguments[3], "--row-ordinal");
    let ordinal: u64 = arguments[4].parse().expect("invalid row ordinal");
    assert!(ordinal > 0 && ordinal.to_string() == arguments[4]);
    assert_eq!(arguments[5], "--config");
    assert_eq!(required("ASTERISM_REBASELINE_ROW_ORDINAL"), arguments[4]);
    assert_eq!(required("ASTERISM_REBASELINE_CONFIG"), arguments[6]);
}

fn validate_empty_args() {
    assert!(
        std::env::args().nth(1).is_none(),
        "mode does not accept command-line arguments"
    );
}

fn parse_usize(name: &str) -> usize {
    required(name).parse().unwrap_or_else(|_| panic!("invalid {name}"))
}

fn configured_workload(track: &str) -> Workload {
    let durability = DurabilityKind::parse(&required("ASTERISM_DURABILITY"));
    let payload_bytes = parse_usize("ASTERISM_PAYLOAD_BYTES");
    let batch = parse_usize("ASTERISM_BATCH");
    let writers = parse_usize("ASTERISM_WRITERS");
    let batches_per_writer = parse_usize("ASTERISM_BATCHES_PER_WRITER");
    assert!(
        payload_bytes > 0 && batch > 0 && writers > 0 && batches_per_writer > 0
    );
    let new_names = match track {
        "new_names" => true,
        "structural_traces" => required("ASTERISM_TRACE_KIND") == "new_names",
        _ => false,
    };
    let workload = Workload {
        durability,
        payload_bytes,
        batch,
        writers,
        batches_per_writer,
        new_names,
    };
    if track == "primary" {
        assert_eq!(
            workload,
            Workload::primary(durability, payload_bytes, batch, writers)
        );
    }
    if track == "new_names" {
        assert_eq!((payload_bytes, batch), (250, 1));
        assert!(matches!(writers, 1 | 4));
        assert_eq!(
            batches_per_writer,
            if durability == DurabilityKind::Process { 4_000 } else { 1_000 }
        );
    }
    if track == "fairness" {
        assert_eq!((payload_bytes, writers), (250, 64));
        assert!(matches!(batch, 1 | 100));
    }
    workload
}

fn run_point(
    workload: Workload,
    root: PathBuf,
    warm_rounds: usize,
) -> PointResult {
    assert!(!root.exists(), "store path must be absent");
    let mut control = Control::connect();
    let boot_nonce = control.boot();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workload.writers.clamp(2, 8))
        .thread_keep_alive(Duration::from_secs(3_600))
        .enable_all()
        .build()
        .expect("create runtime");
    let runtime_nonce = control.runtime(&boot_nonce);
    let durability = match workload.durability {
        DurabilityKind::Process => Durability::Process,
        DurabilityKind::Group => Durability::group_default(),
    };
    let engine = LogEngine::open_with(
        root.join("log"),
        EngineOptions {
            durability,
            segment_size: workload.segment_size(),
            ..Default::default()
        },
    )
    .expect("open LogEngine");
    let backend =
        FjallSnapshotBackend::open(engine.clone(), root.join("snapshots"))
            .expect("open production snapshot wrapper");
    let store = EventStore::new(backend);
    let opened_nonce = control.opened(&runtime_nonce);
    let payload = Arc::new(payload_bytes(workload.payload_bytes));
    let gate = Arc::new(StartGate::default());

    let mut initial_heads = vec![Version::NoStream; workload.writers];
    for _ in 0..warm_rounds {
        initial_heads = runtime.block_on(async {
            let mut joins = Vec::with_capacity(workload.writers);
            for (writer, expected) in initial_heads.iter().copied().enumerate()
            {
                let payload = Arc::clone(&payload);
                let store = store.clone();
                joins.push(tokio::spawn(async move {
                    let events: Vec<BenchEvent> = (0..workload.batch)
                        .map(|_| BenchEvent {
                            payload: payload.as_ref().clone(),
                        })
                        .collect();
                    store
                        .append(
                            &workload.stream_name(writer, 0),
                            expected,
                            &events,
                        )
                        .await
                        .expect("fairness warm append")
                        .version
                }));
            }
            let mut output = Vec::with_capacity(joins.len());
            for join in joins {
                output.push(join.await.expect("warm writer task"));
            }
            output
        });
    }

    let joins = runtime.block_on(async {
        let mut joins = Vec::with_capacity(workload.writers);
        for (writer, initial_expected) in initial_heads.into_iter().enumerate()
        {
            let gate = Arc::clone(&gate);
            let payload = Arc::clone(&payload);
            let store = store.clone();
            joins.push(tokio::spawn(async move {
                let stable_name = (!workload.new_names)
                    .then(|| workload.stream_name(writer, 0));
                let mut expected = initial_expected;
                let mut latencies =
                    Vec::with_capacity(workload.batches_per_writer);
                let mut cursor_monotone = true;
                let mut previous_cursor = None;
                let mut digest = LogicalDigest::default();
                gate.arrive_and_wait().await;
                for append in 0..workload.batches_per_writer {
                    let started = Instant::now();
                    let events: Vec<BenchEvent> = (0..workload.batch)
                        .map(|_| BenchEvent {
                            payload: payload.as_ref().clone(),
                        })
                        .collect();
                    let new_name;
                    let stream = if let Some(name) = &stable_name {
                        name.as_str()
                    } else {
                        new_name = workload.stream_name(writer, append);
                        &new_name
                    };
                    let commit = store
                        .append(stream, expected, &events)
                        .await
                        .expect("public append");
                    latencies.push(started.elapsed().as_nanos() as u64);
                    assert_eq!(commit.events_appended, workload.batch);
                    let cursor = commit
                        .last_global_position
                        .expect("nonempty append has a global cursor");
                    cursor_monotone &=
                        previous_cursor.is_none_or(|prior| cursor > prior);
                    previous_cursor = Some(cursor);
                    expected = if workload.new_names {
                        Version::NoStream
                    } else {
                        commit.version
                    };
                    digest.committed_append(
                        writer as u64,
                        append as u64,
                        workload.stream_ordinal(writer, append),
                        workload.batch as u64,
                        payload.as_ref(),
                    );
                }
                WriterResult {
                    completed: workload.batches_per_writer as u64,
                    completion_monotonic_ns: monotonic_ns(),
                    cursor_monotone,
                    digest,
                    latencies,
                }
            }));
        }
        joins
    });
    let mut writer_results = Vec::with_capacity(joins.len());

    gate.wait_until_ready(workload.writers);
    let ready_monotonic_ns = monotonic_ns();
    let alloc_before = allocation::snapshot();
    let input_before = adapter::input_snapshot(&engine);
    let commit_before = engine.metrics().commit;
    let cpu_before = cpu_snapshot();
    let counter_start_monotonic_ns = monotonic_ns();
    let control_nonce = control.ready_and_wait_start(
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
    gate.start();
    let writers = runtime.block_on(async {
        for join in joins {
            writer_results.push(join.await.expect("writer task"));
        }
        writer_results
    });
    let t1_monotonic_ns = monotonic_ns();
    control.disable_perf_after_t1(&control_nonce, t1_monotonic_ns);
    let alloc_after = allocation::snapshot();
    let cpu_after = cpu_snapshot();
    let counter_end_monotonic_ns = monotonic_ns();
    let commit_after = engine.metrics().commit;
    let input_after = adapter::input_snapshot(&engine);
    let last_completion_monotonic_ns = writers
        .iter()
        .map(|writer| writer.completion_monotonic_ns)
        .max()
        .expect("writer completion");
    control.measured_and_wait_release(
        &control_nonce,
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

    if warm_rounds == 0 {
        assert_eq!(commit_before.batches, 0, "fresh store has prior batches");
        assert_eq!(commit_before.groups, 0, "fresh store has prior groups");
        assert_eq!(
            commit_before.fsync.count, 0,
            "fresh store has prior barriers"
        );
    }
    assert!(
        writers.iter().all(
            |writer| writer.completed == workload.batches_per_writer as u64
        )
    );
    let mut digest = LogicalDigest::default();
    let mut per_writer = Vec::with_capacity(writers.len());
    let mut writer_samples = Vec::with_capacity(writers.len());
    let opaque_cursor_monotone =
        writers.iter().all(|writer| writer.cursor_monotone);
    for (writer_index, writer) in writers.into_iter().enumerate() {
        digest = digest.combine(writer.digest);
        let elapsed_ns = writer.completion_monotonic_ns - release_monotonic_ns;
        let mut sorted = writer.latencies.clone();
        sorted.sort_unstable();
        writer_samples.push(WriterSample {
            completed_appends: writer.completed,
            completed_events: writer.completed * workload.batch as u64,
            elapsed_ns,
            max_ns: *sorted.last().expect("writer latency samples"),
            p50_ns: nearest_rank(&sorted, 50, 100),
            p99_ns: nearest_rank(&sorted, 99, 100),
            writer: writer_index as u64,
        });
        per_writer.push(writer.latencies);
    }
    let latencies = if warm_rounds == 0 {
        post_warmup(per_writer)
    } else {
        let mut merged: Vec<u64> = per_writer.into_iter().flatten().collect();
        merged.sort_unstable();
        merged
    };
    let inputs = adapter::input_delta(input_before, input_after, workload);
    let path_label = match workload.durability {
        DurabilityKind::Process => adapter::PATH_LABEL_PROCESS,
        DurabilityKind::Group => adapter::PATH_LABEL_GROUP,
    };
    let log_events = commit_after
        .events
        .checked_sub(commit_before.events)
        .expect("commit events regressed");
    let log_batches = commit_after
        .batches
        .checked_sub(commit_before.batches)
        .expect("commit batches regressed");
    assert!(
        log_batches >= workload.appends(),
        "log batches omit a successful public submission"
    );
    let groups = commit_after
        .groups
        .checked_sub(commit_before.groups)
        .expect("group count regressed");
    let measured_fsync_count = commit_after
        .fsync
        .count
        .checked_sub(commit_before.fsync.count)
        .expect("fsync count regressed");
    assert_eq!(
        measured_fsync_count, groups,
        "one successful barrier must cover each measured commit group"
    );
    let fsync_count = if warm_rounds == 0 {
        measured_fsync_count
    } else {
        commit_after.fsync.count
    };
    drop(store);
    drop(engine);
    runtime.shutdown_timeout(std::time::Duration::from_secs(10));

    PointResult {
        allocations: alloc_after.delta_from(alloc_before),
        appends: workload.appends(),
        borrowed_batches: inputs.borrowed_batches,
        borrowed_payload_bytes: inputs.borrowed_payload_bytes,
        borrowed_records: inputs.borrowed_records,
        copied_bytes: inputs.copied_bytes,
        copied_records: inputs.copied_records,
        digest: digest.value(),
        domain_events: workload.events(),
        fsync_count,
        fsync_degraded: commit_after.fsync_degraded,
        fsync_max_ns: commit_after.fsync.max_nanos,
        fsync_p50_ns: commit_after.fsync.p50_nanos,
        fsync_p95_ns: commit_after.fsync.p95_nanos,
        fsync_p99_ns: commit_after.fsync.p99_nanos,
        fsync_total_ns: commit_after
            .fsync
            .mean_nanos
            .saturating_mul(fsync_count),
        groups,
        log_events,
        latency_samples: latencies.len() as u64,
        owned_batches: inputs.owned_batches,
        owned_payload_bytes: inputs.owned_payload_bytes,
        owned_records: inputs.owned_records,
        opaque_cursor_monotone,
        p50_ns: nearest_rank(&latencies, 50, 100),
        p99_ns: nearest_rank(&latencies, 99, 100),
        max_ns: *latencies.last().expect("latency samples"),
        path_label,
        payload_bytes: workload.payload_total(),
        wall_ns: t1_monotonic_ns - t0_monotonic_ns,
        waiter_reservations_after: inputs.waiter_reservations_after,
        byte_reservations_after: inputs.byte_reservations_after,
        writer_samples,
    }
}

fn corpus_stream(index: usize) -> String {
    format!("asterism-corpus-{index:04}")
}

fn verify_corpus(
    runtime: &tokio::runtime::Runtime,
    store: &EventStore<FjallSnapshotBackend<LogEngine>>,
    streams: usize,
    events_per_stream: u64,
) -> CorpusVerification {
    runtime.block_on(async {
        let mut domain_events = 0_u64;
        let mut logical_digest = LogicalDigest::default();
        let mut registry_head_digest = LogicalDigest::default();
        for stream in 0..streams {
            let loaded = store
                .load::<CorpusAggregate>(&corpus_stream(stream))
                .await
                .expect("verify corpus stream");
            assert_eq!(loaded.events_replayed as u64, events_per_stream);
            assert_eq!(loaded.state.events, events_per_stream);
            let position =
                loaded.version.position().expect("corpus stream has a head");
            assert_eq!(position + 1, events_per_stream);
            domain_events += loaded.state.events;
            logical_digest = logical_digest.combine(loaded.state.digest);
            registry_head_digest.update_u64(stream as u64);
            registry_head_digest.update_u64(position);
        }
        assert_eq!(domain_events, streams as u64 * events_per_stream);
        CorpusVerification {
            domain_events,
            logical_digest: logical_digest.canonical_hex(),
            registry_head_digest: registry_head_digest.canonical_hex(),
        }
    })
}

fn run_reopen_seed(
    root: PathBuf,
    streams: usize,
    batches_per_stream: usize,
    batch: usize,
    workers: usize,
    output_schema: &str,
) {
    assert!(streams > 0 && batches_per_stream > 0 && batch > 0 && workers > 0);
    assert!(!root.exists(), "reopen seed path must be absent");
    let mut control = Control::connect();
    let boot_nonce = control.boot();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers.clamp(2, 8))
        .thread_keep_alive(Duration::from_secs(3_600))
        .enable_all()
        .build()
        .expect("create seed runtime");
    let runtime_nonce = control.runtime(&boot_nonce);
    let engine = LogEngine::open_with(
        root.join("log"),
        EngineOptions {
            durability: Durability::Process,
            segment_size: 8 * 1024 * 1024,
            ..Default::default()
        },
    )
    .expect("open seed LogEngine");
    let backend =
        FjallSnapshotBackend::open(engine.clone(), root.join("snapshots"))
            .expect("open seed production snapshot wrapper");
    let store = EventStore::new(backend);
    let opened_nonce = control.opened(&runtime_nonce);
    let payload = Arc::new(payload_bytes(64));
    let ready_monotonic_ns = monotonic_ns();
    let alloc_before = allocation::snapshot();
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
        let mut joins = Vec::with_capacity(workers);
        for worker in 0..workers {
            let payload = Arc::clone(&payload);
            let store = store.clone();
            joins.push(tokio::spawn(async move {
                for stream in (worker..streams).step_by(workers) {
                    let name = corpus_stream(stream);
                    let mut expected = Version::NoStream;
                    for _ in 0..batches_per_stream {
                        let events: Vec<BenchEvent> = (0..batch)
                            .map(|_| BenchEvent {
                                payload: payload.as_ref().clone(),
                            })
                            .collect();
                        expected = store
                            .append(&name, expected, &events)
                            .await
                            .expect("seed corpus append")
                            .version;
                    }
                }
            }));
        }
        for join in joins {
            join.await.expect("seed writer task");
        }
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
    let verified = verify_corpus(
        &runtime,
        &store,
        streams,
        (batches_per_stream * batch) as u64,
    );
    let log_events = engine.total_events() as u64;
    adapter::assert_reopen_seed_accounting(
        &engine,
        verified.domain_events,
        streams as u64,
    );
    drop(store);
    drop(engine);
    runtime.shutdown_timeout(std::time::Duration::from_secs(30));
    let fields = [
        ("domain_events", json_u64(verified.domain_events)),
        ("log_events", json_u64(log_events)),
        ("logical_digest", json_string(&verified.logical_digest)),
        ("protocol", json_string(contract::PROTOCOL)),
        ("protocol_sha256", json_string(contract::PROTOCOL_SHA256)),
        ("registry_head_digest", json_string(&verified.registry_head_digest)),
        ("schema", json_string(output_schema)),
        ("variant", json_string(contract::VARIANT)),
        ("visible_events", json_u64(verified.domain_events)),
    ];
    println!("{}", canonical_object(&fields));
}

fn run_reopen(
    root: PathBuf,
    streams: usize,
    events_per_stream: u64,
) -> ReopenResult {
    assert!(root.is_dir(), "reopen corpus path must exist");
    let mut control = Control::connect();
    let boot_nonce = control.boot();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_keep_alive(Duration::from_secs(3_600))
        .enable_all()
        .build()
        .expect("create reopen runtime");
    let runtime_nonce = control.runtime(&boot_nonce);
    let ready_monotonic_ns = monotonic_ns();
    let alloc_before = allocation::snapshot();
    let cpu_before = cpu_snapshot();
    let counter_start_monotonic_ns = monotonic_ns();
    let start_nonce = control.ready_and_wait_start(
        &runtime_nonce,
        alloc_before.calls,
        alloc_before.bytes,
        cpu_before.user_ns,
        cpu_before.system_ns,
        ready_monotonic_ns,
        counter_start_monotonic_ns,
    );
    let open_start_monotonic_ns = monotonic_ns();
    let engine = LogEngine::open_with(
        root.join("log"),
        EngineOptions {
            durability: Durability::Process,
            segment_size: 8 * 1024 * 1024,
            ..Default::default()
        },
    )
    .expect("reopen LogEngine");
    let backend =
        FjallSnapshotBackend::open(engine.clone(), root.join("snapshots"))
            .expect("reopen production snapshot wrapper");
    let store = EventStore::new(backend);
    let opened_monotonic_ns = monotonic_ns();
    // Freeze every internal end marker at successful open, before correctness
    // observations. `opened_after_start` then blocks while the parent captures
    // and closes the external post-open profile window. Keep this order in
    // lockstep with prepare_overlays.py's source-order regression.
    let alloc_after = allocation::snapshot();
    let cpu_after = cpu_snapshot();
    let counter_end_monotonic_ns = monotonic_ns();
    let measured_nonce = control.opened_after_start(
        &start_nonce,
        open_start_monotonic_ns,
        opened_monotonic_ns,
    );
    // The parent has resumed us after retaining its immediate post-open
    // snapshot. These observations are correctness-only and intentionally
    // outside wall, allocation, CPU, and external profile windows.
    let log_events = engine.total_events() as u64;
    let recovery_payload_decodes = engine.recover_payload_decodes();
    control.measured_and_wait_release(
        &measured_nonce,
        MeasuredMarkers {
            allocation_calls_end: alloc_after.calls,
            allocated_bytes_end: alloc_after.bytes,
            counter_end_monotonic_ns,
            last_completion_monotonic_ns: opened_monotonic_ns,
            release_monotonic_ns: open_start_monotonic_ns,
            process_system_cpu_end_ns: cpu_after.system_ns,
            process_user_cpu_end_ns: cpu_after.user_ns,
            t0_monotonic_ns: open_start_monotonic_ns,
            t1_monotonic_ns: opened_monotonic_ns,
        },
    );

    // The parent has now retained its immediate post-open profile snapshot
    // and released the child. Corpus verification is deliberately outside
    // both the open wall and the external profile window.
    let domain_events: u64 = required("ASTERISM_EXPECTED_DOMAIN_EVENTS")
        .parse()
        .expect("invalid expected domain events");
    let visible_events: u64 = required("ASTERISM_EXPECTED_VISIBLE_EVENTS")
        .parse()
        .expect("invalid expected visible events");
    let expected_log_events: u64 = required("ASTERISM_EXPECTED_LOG_EVENTS")
        .parse()
        .expect("invalid expected log events");
    assert_eq!(domain_events, streams as u64 * events_per_stream);
    assert_eq!(visible_events, domain_events);
    assert_eq!(
        log_events, expected_log_events,
        "reopen log event count mismatch"
    );
    let logical_digest = required("ASTERISM_EXPECTED_LOGICAL_DIGEST");
    let registry_head_digest =
        required("ASTERISM_EXPECTED_REGISTRY_HEAD_DIGEST");
    assert!(!logical_digest.is_empty() && !registry_head_digest.is_empty());
    let verified = verify_corpus(&runtime, &store, streams, events_per_stream);
    assert_eq!(verified.domain_events, domain_events);
    assert_eq!(
        verified.logical_digest, logical_digest,
        "reopen logical digest mismatch"
    );
    assert_eq!(
        verified.registry_head_digest, registry_head_digest,
        "reopen registry/head digest mismatch"
    );
    drop(store);
    drop(engine);
    runtime.shutdown_timeout(std::time::Duration::from_secs(10));
    ReopenResult {
        domain_events,
        log_events,
        logical_digest,
        recovery_payload_decodes,
        registry_head_digest,
        visible_events,
        wall_ns: opened_monotonic_ns - open_start_monotonic_ns,
    }
}

fn emit_reopen(track: &str, result: ReopenResult) {
    if track == "structural_traces" {
        let fields = [
            ("appends_per_writer", json_u64(0)),
            ("barrier_count", json_u64(0)),
            ("durability", json_string("Process")),
            ("group_count", json_u64(0)),
            ("protocol", json_string(contract::PROTOCOL)),
            ("protocol_sha256", json_string(contract::PROTOCOL_SHA256)),
            ("schema", json_string("bn-2l3n-point-v3")),
            ("trace_kind", json_string("reopen")),
            ("track", json_string(track)),
            ("variant", json_string(contract::VARIANT)),
            ("writers", json_u64(1)),
        ];
        println!("{}", canonical_object(&fields));
        return;
    }
    let fields = [
        ("domain_events", json_u64(result.domain_events)),
        ("log_events", json_u64(result.log_events)),
        ("logical_digest", json_string(&result.logical_digest)),
        ("protocol", json_string(contract::PROTOCOL)),
        ("protocol_sha256", json_string(contract::PROTOCOL_SHA256)),
        ("recovery_payload_decodes", json_u64(result.recovery_payload_decodes)),
        ("registry_head_digest", json_string(&result.registry_head_digest)),
        ("schema", json_string("bn-2l3n-point-v3")),
        ("track", json_string("reopen")),
        ("variant", json_string(contract::VARIANT)),
        ("visible_events", json_u64(result.visible_events)),
        ("wall_ns", json_u64(result.wall_ns)),
    ];
    println!("{}", canonical_object(&fields));
}

fn emit_smoke_reopen(smoke_target: &str, result: ReopenResult) {
    let fields = [
        ("domain_events", json_u64(result.domain_events)),
        ("log_events", json_u64(result.log_events)),
        ("logical_digest", json_string(&result.logical_digest)),
        ("protocol", json_string(contract::PROTOCOL)),
        ("protocol_sha256", json_string(contract::PROTOCOL_SHA256)),
        ("registry_head_digest", json_string(&result.registry_head_digest)),
        ("schema", json_string("bn-2l3n-overlay-smoke-reopen-v3")),
        ("smoke_target", json_string(smoke_target)),
        ("status", json_string("ok")),
        ("variant", json_string(contract::VARIANT)),
        ("visible_events", json_u64(result.visible_events)),
    ];
    println!("{}", canonical_object(&fields));
}

fn writer_samples_json(samples: &[WriterSample]) -> String {
    let values: Vec<String> = samples
        .iter()
        .map(|sample| {
            canonical_object(&[
                ("completed_appends", json_u64(sample.completed_appends)),
                ("completed_events", json_u64(sample.completed_events)),
                ("elapsed_ns", json_u64(sample.elapsed_ns)),
                ("max_ns", json_u64(sample.max_ns)),
                ("p50_ns", json_u64(sample.p50_ns)),
                ("p99_ns", json_u64(sample.p99_ns)),
                ("writer", json_u64(sample.writer)),
            ])
        })
        .collect();
    format!("[{}]", values.join(","))
}

fn emit_point(track: &str, workload: Workload, row: PointResult) {
    if matches!(track, "cpu_profiles" | "syscall_profiles") {
        let fields = [
            ("appends", json_u64(row.appends)),
            ("batch_size", json_u64(workload.batch as u64)),
            (
                "batches_per_writer",
                json_u64(workload.batches_per_writer as u64),
            ),
            ("domain_events", json_u64(row.domain_events)),
            ("durability", json_string(workload.durability.label())),
            ("payload_size", json_u64(workload.payload_bytes as u64)),
            ("protocol", json_string(contract::PROTOCOL)),
            ("protocol_sha256", json_string(contract::PROTOCOL_SHA256)),
            ("schema", json_string("bn-2l3n-point-v3")),
            ("track", json_string(track)),
            ("variant", json_string(contract::VARIANT)),
            ("writers", json_u64(workload.writers as u64)),
        ];
        println!("{}", canonical_object(&fields));
        return;
    }
    if track == "structural_traces" {
        let fields = [
            (
                "appends_per_writer",
                json_u64(workload.batches_per_writer as u64),
            ),
            ("barrier_count", json_u64(row.groups)),
            ("durability", json_string(workload.durability.label())),
            ("group_count", json_u64(row.groups)),
            ("protocol", json_string(contract::PROTOCOL)),
            ("protocol_sha256", json_string(contract::PROTOCOL_SHA256)),
            ("schema", json_string("bn-2l3n-point-v3")),
            ("trace_kind", json_string(&required("ASTERISM_TRACE_KIND"))),
            ("track", json_string(track)),
            ("variant", json_string(contract::VARIANT)),
            ("writers", json_u64(workload.writers as u64)),
        ];
        println!("{}", canonical_object(&fields));
        return;
    }
    let group = workload.durability == DurabilityKind::Group;
    let control_events = row
        .log_events
        .checked_sub(row.domain_events)
        .expect("log events below domain events");
    let mut fields = vec![
        ("accepted_batches", json_u64(row.appends)),
        ("allocated_bytes", json_u64(row.allocations.bytes)),
        ("allocation_calls", json_u64(row.allocations.calls)),
        ("appends", json_u64(row.appends)),
        ("barrier_count", json_u64(row.groups)),
        ("batch_size", json_u64(workload.batch as u64)),
        ("batches_per_writer", json_u64(workload.batches_per_writer as u64)),
        ("borrowed_batches", json_available(row.borrowed_batches)),
        ("borrowed_payload_bytes", json_available(row.borrowed_payload_bytes)),
        ("borrowed_records", json_available(row.borrowed_records)),
        ("conflicts", json_u64(0)),
        ("control_events", json_u64(control_events)),
        ("defensive_copy_bytes", json_available(row.copied_bytes)),
        ("defensive_copy_records", json_available(row.copied_records)),
        ("domain_events", json_u64(row.domain_events)),
        ("durability", json_string(workload.durability.label())),
        ("durability_degraded", json_bool(row.fsync_degraded)),
        ("fsync_count", json_u64(row.fsync_count)),
        ("fsync_max_ns", json_u64(row.fsync_max_ns)),
        ("fsync_p50_ns", json_u64(row.fsync_p50_ns)),
        ("fsync_p95_ns", json_u64(row.fsync_p95_ns)),
        ("fsync_p99_ns", json_u64(row.fsync_p99_ns)),
        ("fsync_total_ns", json_u64(row.fsync_total_ns)),
        ("group_batches", json_u64(if group { row.appends } else { 0 })),
        ("group_count", json_u64(row.groups)),
        ("group_events", json_u64(if group { row.domain_events } else { 0 })),
        ("host_write_bytes", json_string("not_available")),
        ("latency_max_ns", json_u64(row.max_ns)),
        ("latency_p50_ns", json_u64(row.p50_ns)),
        ("latency_p99_ns", json_u64(row.p99_ns)),
        ("latency_samples", json_u64(row.latency_samples)),
        ("log_events", json_u64(row.log_events)),
        ("logical_digest", json_string(&format!("{:016x}", row.digest))),
        ("owned_batches", json_available(row.owned_batches)),
        ("owned_payload_bytes", json_available(row.owned_payload_bytes)),
        ("owned_records", json_available(row.owned_records)),
        ("path_label", json_string(row.path_label)),
        ("payload_bytes", json_u64(row.payload_bytes)),
        ("payload_size", json_u64(workload.payload_bytes as u64)),
        ("protocol", json_string(contract::PROTOCOL)),
        ("protocol_sha256", json_string(contract::PROTOCOL_SHA256)),
        ("schema", json_string("bn-2l3n-point-v3")),
        ("sync_family_calls", json_string("not_available")),
        ("track", json_string(track)),
        ("variant", json_string(contract::VARIANT)),
        ("visible_events", json_u64(row.domain_events)),
        ("wall_ns", json_u64(row.wall_ns)),
        ("write_like_calls", json_string("not_available")),
        ("writers", json_u64(workload.writers as u64)),
    ];
    if track == "new_names" {
        fields.extend([
            ("distinct_streams", json_u64(row.appends)),
            ("opaque_cursor_monotone", json_bool(row.opaque_cursor_monotone)),
            ("registry_events", json_u64(control_events)),
        ]);
    }
    if track == "fairness" {
        let fairness: Vec<FairnessInput> = row
            .writer_samples
            .iter()
            .map(|sample| FairnessInput {
                completed_events: sample.completed_events,
                elapsed_ns:       sample.elapsed_ns,
                p99_ns:           sample.p99_ns,
            })
            .collect();
        let (jain, min_rate, max_p99) = fairness_ppb(&fairness);
        fields.extend([
            (
                "adaptive_group_width_target",
                json_string("not_available"),
            ),
            (
                "byte_reservations_after",
                json_available(row.byte_reservations_after),
            ),
            ("counter_snapshot_after_warm", json_bool(true)),
            ("fsync_histogram_includes_warm", json_bool(true)),
            ("group_width_distribution", json_string("not_available")),
            ("jain_ppb", json_u64(jain)),
            ("max_to_median_p99_ppb", json_u64(max_p99)),
            ("min_to_median_rate_ppb", json_u64(min_rate)),
            ("oldest_queued_age_ns", json_string("not_available")),
            ("queue_bytes", json_string("not_available")),
            ("queue_depth", json_string("not_available")),
            (
                "waiter_reservations_after",
                json_available(row.waiter_reservations_after),
            ),
            ("warm_names_established", json_bool(true)),
            ("warm_rounds", json_u64(4)),
            ("warm_writers", json_u64(64)),
            ("writer_samples_json", writer_samples_json(&row.writer_samples)),
        ]);
    }
    fields.sort_unstable_by_key(|field| field.0);
    println!("{}", canonical_object(&fields));
}

fn run_common_public_oracle(root: PathBuf) {
    assert!(!root.exists(), "correctness oracle store path must be absent");
    assert!(matches!(contract::VARIANT, "A" | "C" | "D"));
    let mut control = Control::connect();
    let boot_nonce = control.boot();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create correctness oracle runtime");
    let runtime_nonce = control.runtime(&boot_nonce);
    let engine = LogEngine::open_with(
        root.join("log"),
        EngineOptions {
            durability: Durability::Process,
            segment_size: 8 * 1024 * 1024,
            ..Default::default()
        },
    )
    .expect("open correctness oracle LogEngine");
    let backend =
        FjallSnapshotBackend::open(engine.clone(), root.join("snapshots"))
            .expect("open correctness oracle production snapshot wrapper");
    let store = EventStore::new(backend).with_page_size(16);
    let opened_nonce = control.opened(&runtime_nonce);
    let ready_monotonic_ns = monotonic_ns();
    let alloc_before = allocation::snapshot();
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
        let payloads = [
            b"common-oracle/alpha/0".to_vec(),
            b"common-oracle/alpha/1".to_vec(),
            b"common-oracle/beta/0".to_vec(),
            b"common-oracle/alpha/2".to_vec(),
        ];
        let first = store
            .append(
                "oracle-alpha",
                Version::NoStream,
                &[
                    BenchEvent { payload: payloads[0].clone() },
                    BenchEvent { payload: payloads[1].clone() },
                ],
            )
            .await
            .expect("oracle alpha initial append");
        assert_eq!((first.events_appended, first.version), (2, Version::At(1)));
        let second = store
            .append(
                "oracle-beta",
                Version::NoStream,
                &[BenchEvent { payload: payloads[2].clone() }],
            )
            .await
            .expect("oracle beta append");
        assert_eq!(
            (second.events_appended, second.version),
            (1, Version::At(0))
        );
        let third = store
            .append(
                "oracle-alpha",
                Version::At(1),
                &[BenchEvent { payload: payloads[3].clone() }],
            )
            .await
            .expect("oracle alpha continuation append");
        assert_eq!((third.events_appended, third.version), (1, Version::At(2)));
        let conflict = store
            .append(
                "oracle-alpha",
                Version::At(0),
                &[BenchEvent { payload: b"must-not-land".to_vec() }],
            )
            .await;
        assert!(matches!(
            conflict,
            Err(AppendError::Conflict {
                expected: Version::At(0),
                actual:   Version::At(2),
            })
        ));
        let common_error = store
            .append("oracle-error", Version::NoStream, &[RejectedEvent])
            .await;
        assert!(matches!(common_error, Err(AppendError::Backend(_))));
        let cursors = [
            first.last_global_position.expect("first oracle cursor"),
            second.last_global_position.expect("second oracle cursor"),
            third.last_global_position.expect("third oracle cursor"),
        ];
        assert!(cursors.windows(2).all(|pair| pair[0] < pair[1]));

        let alpha = store
            .load::<CorpusAggregate>("oracle-alpha")
            .await
            .expect("load oracle alpha");
        let beta = store
            .load::<CorpusAggregate>("oracle-beta")
            .await
            .expect("load oracle beta");
        let rejected = store
            .load::<CorpusAggregate>("oracle-error")
            .await
            .expect("load rejected oracle stream");
        let mut expected_alpha = LogicalDigest::default();
        for payload in [&payloads[0], &payloads[1], &payloads[3]] {
            expected_alpha.update_bytes(payload);
        }
        let mut expected_beta = LogicalDigest::default();
        expected_beta.update_bytes(&payloads[2]);
        assert_eq!((alpha.events_replayed, alpha.version), (3, Version::At(2)));
        assert_eq!((beta.events_replayed, beta.version), (1, Version::At(0)));
        assert_eq!(alpha.state.events, 3);
        assert_eq!(alpha.state.digest, expected_alpha);
        assert_eq!(beta.state.events, 1);
        assert_eq!(beta.state.digest, expected_beta);
        assert_eq!(
            (rejected.events_replayed, rejected.version),
            (0, Version::NoStream)
        );

        let mut subscription = store.subscribe(None);
        let records = subscription
            .next_batch()
            .await
            .expect("read oracle subscription history");
        assert_eq!(records.len(), 4);
        let expected = [
            ("oracle-alpha", 0, &payloads[0]),
            ("oracle-alpha", 1, &payloads[1]),
            ("oracle-beta", 0, &payloads[2]),
            ("oracle-alpha", 2, &payloads[3]),
        ];
        for (record, (stream, position, payload)) in
            records.iter().zip(expected)
        {
            assert_eq!(record.stream_id, stream);
            assert_eq!(record.stream_position, position);
            assert_eq!(record.message_type, "asterism.rebaseline.event");
            assert_eq!(&record.data, payload);
        }
        assert!(
            records.windows(2).all(|pair| {
                pair[0].global_position < pair[1].global_position
            })
        );
        let mut observed = LogicalDigest::default();
        let mut expected_digest = LogicalDigest::default();
        for record in &records {
            observed.update_bytes(&record.data);
        }
        for payload in &payloads {
            expected_digest.update_bytes(payload);
        }
        assert_eq!(observed, expected_digest);
    });
    adapter::assert_oracle_accounting(&engine, 4, 3, 2, false);
    drop(store);
    drop(engine);

    let group_engine = LogEngine::open_with(
        root.join("group-log"),
        EngineOptions {
            durability: Durability::group_default(),
            segment_size: 8 * 1024 * 1024,
            ..Default::default()
        },
    )
    .expect("open Group correctness oracle LogEngine");
    let group_backend = FjallSnapshotBackend::open(
        group_engine.clone(),
        root.join("group-snapshots"),
    )
    .expect("open Group correctness oracle production snapshot wrapper");
    let group_store = EventStore::new(group_backend);
    runtime.block_on(async {
        let commit = group_store
            .append(
                "oracle-group",
                Version::NoStream,
                &[BenchEvent {
                    payload: b"common-oracle/group-barrier".to_vec(),
                }],
            )
            .await
            .expect("Group oracle append");
        assert_eq!(
            (commit.events_appended, commit.version),
            (1, Version::At(0))
        );
        let loaded = group_store
            .load::<CorpusAggregate>("oracle-group")
            .await
            .expect("load Group oracle stream");
        assert_eq!(
            (loaded.events_replayed, loaded.version),
            (1, Version::At(0))
        );
    });
    adapter::assert_oracle_accounting(&group_engine, 1, 1, 1, true);
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
    drop(group_store);
    drop(group_engine);
    runtime.shutdown_timeout(std::time::Duration::from_secs(30));
}

fn emit_correctness_oracle(args: CorrectnessOracleArgs) {
    run_common_public_oracle(PathBuf::from(required(
        "ASTERISM_REBASELINE_STORE",
    )));
    let case = canonical_object(&[
        ("classification", json_string("historical-oracle")),
        ("id", json_string("public-common-oracle")),
        ("status", json_string("PASS")),
    ]);
    let fields = [
        ("attempt_nonce", json_string(&args.attempt_nonce)),
        ("boundedness", "null".to_owned()),
        ("cases", format!("[{case}]")),
        ("harness_sound", json_bool(true)),
        ("phase", json_string("oracle")),
        ("protocol", json_string(contract::PROTOCOL)),
        ("schema", json_string("bn-2l3n-correctness-child-v3")),
        ("suite", json_string("common-public-oracle")),
        ("variant", json_string(contract::VARIANT)),
    ];
    println!("{}", canonical_object(&fields));
}

fn self_test() {
    let workload = Workload::primary(DurabilityKind::Process, 250, 100, 4);
    assert_eq!(workload.batches_per_writer, 2_500);
    assert_eq!(workload.events(), 1_000_000);
    assert_payload_contract();
    let mut values = vec![9, 1, 3, 7, 5];
    values.sort_unstable();
    assert_eq!(nearest_rank(&values, 50, 100), 5);
    let mut fairness = vec![
        FairnessInput {
            completed_events: 100,
            elapsed_ns:       1_000,
            p99_ns:           10,
        };
        64
    ];
    assert_eq!(
        fairness_ppb(&fairness),
        (1_000_000_000, 1_000_000_000, 1_000_000_000)
    );
    fairness[0].elapsed_ns = 2_000;
    fairness[0].p99_ns = 20;
    let mutated = fairness_ppb(&fairness);
    assert!(mutated.0 < 1_000_000_000 && mutated.1 < 1_000_000_000);
    assert!(mutated.2 > 1_000_000_000);
    println!(
        "{{\"schema\":\"bn-2l3n-overlay-self-test-v3\",\"status\":\"ok\"}}"
    );
}

fn main() {
    let mode = required("ASTERISM_REBASELINE_MODE");
    control::validate_perf_environment_mode(&mode);
    control::authorize_ptracer_from_env();
    match mode.as_str() {
        "contract" => {
            validate_empty_args();
            contract::emit(BINARY_KIND, TIMED_SURFACE);
        }
        "correctness_oracle" => {
            contract::validate(BINARY_KIND, TIMED_SURFACE);
            emit_correctness_oracle(correctness_oracle_args());
        }
        "self-test" => {
            validate_empty_args();
            contract::validate(BINARY_KIND, TIMED_SURFACE);
            self_test();
        }
        "smoke" => {
            validate_empty_args();
            contract::validate(BINARY_KIND, TIMED_SURFACE);
            let workload = Workload {
                durability:         DurabilityKind::Process,
                payload_bytes:      24,
                batch:              1,
                writers:            1,
                batches_per_writer: 1,
                new_names:          false,
            };
            let root = PathBuf::from(required("ASTERISM_REBASELINE_STORE"));
            let row = run_point(workload, root, 0);
            assert_eq!((row.appends, row.domain_events), (1, 1));
            let fields = [
                ("protocol", json_string(contract::PROTOCOL)),
                ("protocol_sha256", json_string(contract::PROTOCOL_SHA256)),
                ("schema", json_string("bn-2l3n-overlay-smoke-v3")),
                (
                    "smoke_target",
                    json_string(&required("ASTERISM_REBASELINE_SMOKE_TARGET")),
                ),
                ("status", json_string("ok")),
                ("variant", json_string(contract::VARIANT)),
            ];
            println!("{}", canonical_object(&fields));
        }
        "reopen_seed" => {
            validate_empty_args();
            contract::validate(BINARY_KIND, TIMED_SURFACE);
            let root = PathBuf::from(required("ASTERISM_REBASELINE_STORE"));
            run_reopen_seed(root, 1_000, 200, 10, 8, "bn-2l3n-reopen-seed-v3");
        }
        "smoke_reopen_seed" => {
            validate_empty_args();
            contract::validate(BINARY_KIND, TIMED_SURFACE);
            let root = PathBuf::from(required("ASTERISM_REBASELINE_STORE"));
            run_reopen_seed(
                root,
                1,
                1,
                1,
                1,
                "bn-2l3n-overlay-smoke-reopen-seed-v3",
            );
        }
        "smoke_reopen" | "smoke_structural_reopen" => {
            validate_empty_args();
            contract::validate(BINARY_KIND, TIMED_SURFACE);
            let root = PathBuf::from(required("ASTERISM_REBASELINE_STORE"));
            emit_smoke_reopen(&mode, run_reopen(root, 1, 1));
        }
        "primary" | "new_names" | "fairness" | "cpu_profiles"
        | "syscall_profiles" | "structural_traces" => {
            contract::validate(BINARY_KIND, TIMED_SURFACE);
            validate_row_args(&mode);
            if mode == "structural_traces"
                && required("ASTERISM_TRACE_KIND") == "reopen"
            {
                let root = PathBuf::from(required("ASTERISM_REBASELINE_STORE"));
                emit_reopen(&mode, run_reopen(root, 1_000, 2_000));
                return;
            }
            let workload = configured_workload(&mode);
            let root = PathBuf::from(required("ASTERISM_REBASELINE_STORE"));
            let warm_rounds = usize::from(mode == "fairness") * 4;
            emit_point(&mode, workload, run_point(workload, root, warm_rounds));
        }
        "reopen" => {
            contract::validate(BINARY_KIND, TIMED_SURFACE);
            validate_row_args("reopen");
            let root = PathBuf::from(required("ASTERISM_REBASELINE_STORE"));
            emit_reopen("reopen", run_reopen(root, 1_000, 2_000));
        }
        mode => panic!("unsupported ASTERISM_REBASELINE_MODE={mode}"),
    }
}
