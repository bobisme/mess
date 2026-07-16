//! Bare B measurement overlay for the bn-2l3n rebaseline.

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
use std::time::Instant;

use allocation::{AllocationSnapshot, CountingAllocator};
use control::{Control, MeasuredMarkers, cpu_snapshot, monotonic_ns};
use digest::LogicalDigest;
use mess_log::committer::{AppendRequest, Committer, Durability, EventInput};
use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::writer::{SegmentParams, SegmentWriter};
use schema::{canonical_object, json_bool, json_string, json_u64};
use timing::{
    FairnessInput, StartGate, fairness_ppb, nearest_rank, post_warmup,
};
use workload::{DurabilityKind, Workload, assert_payload_contract, payload_bytes};

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const BINARY_KIND: &str = "bare";
const TIMED_SURFACE: &str = "raw-numeric";

struct WriterResult {
    completed:               u64,
    completion_monotonic_ns: u64,
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
    allocations:     AllocationSnapshot,
    appends:         u64,
    digest:          u64,
    domain_events:   u64,
    fsync_count:     u64,
    fsync_degraded:  bool,
    fsync_max_ns:    u64,
    fsync_p50_ns:    u64,
    fsync_p95_ns:    u64,
    fsync_p99_ns:    u64,
    fsync_total_ns:  u64,
    groups:          u64,
    latency_samples: u64,
    max_ns:          u64,
    p50_ns:          u64,
    p99_ns:          u64,
    payload_bytes:   u64,
    wall_ns:         u64,
    writer_samples:  Vec<WriterSample>,
}

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing {name}"))
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
    std::fs::create_dir_all(&root).expect("create store root");
    let runtime = RealRuntime::new();
    let runtime_nonce = control.runtime(&boot_nonce);
    let fs = runtime.fs();
    let mut params = SegmentParams::new(1, 0, 1, 0);
    params.segment_size = workload.segment_size();
    let writer = SegmentWriter::create(&fs, root.join("segment-1.log"), params)
        .expect("create bare segment");
    let durability = match workload.durability {
        DurabilityKind::Process => Durability::Process,
        DurabilityKind::Group => Durability::group_default(),
    };
    let committer = Committer::spawn(&runtime, writer, durability);
    let opened_nonce = control.opened(&runtime_nonce);
    let payload = Arc::new(payload_bytes(workload.payload_bytes));
    let gate = Arc::new(StartGate::default());

    let mut initial_versions = vec![0_u64; workload.writers];
    for _ in 0..warm_rounds {
        let mut warm_joins = Vec::with_capacity(workload.writers);
        for (writer, first_stream_version) in
            initial_versions.iter().copied().enumerate()
        {
            let appender = committer.appender();
            let payload = Arc::clone(&payload);
            warm_joins.push(runtime.spawn(async move {
                let events: Vec<EventInput> = (0..workload.batch)
                    .map(|_| {
                        EventInput::plain(1, 0, 0, payload.as_ref().clone())
                    })
                    .collect();
                appender
                    .append(AppendRequest {
                        stream_id: workload.stream_ordinal(writer, 0),
                        category_id: 0,
                        first_stream_version,
                        events,
                    })
                    .await
                    .expect("fairness warm bare append");
            }));
        }
        runtime.block_on(async {
            for join in warm_joins {
                join.await;
            }
        });
        for version in &mut initial_versions {
            *version += workload.batch as u64;
        }
    }

    let mut joins = Vec::with_capacity(workload.writers);
    for (writer, initial_version) in initial_versions.into_iter().enumerate() {
        let appender = committer.appender();
        let gate = Arc::clone(&gate);
        let payload = Arc::clone(&payload);
        joins.push(runtime.spawn(async move {
            let mut version = initial_version;
            let mut latencies = Vec::with_capacity(workload.batches_per_writer);
            let mut digest = LogicalDigest::default();
            gate.arrive_and_wait().await;
            for append in 0..workload.batches_per_writer {
                let stream_id = workload.stream_ordinal(writer, append);
                let first_stream_version =
                    if workload.new_names { 0 } else { version };
                let events: Vec<EventInput> = (0..workload.batch)
                    .map(|_| {
                        EventInput::plain(1, 0, 0, payload.as_ref().clone())
                    })
                    .collect();
                let request = AppendRequest {
                    stream_id,
                    category_id: 0,
                    first_stream_version,
                    events,
                };
                // The protocol deliberately excludes raw input construction
                // from B's append latency while retaining it in
                // wall/allocation.
                let started = Instant::now();
                appender.append(request).await.expect("bare append");
                latencies.push(started.elapsed().as_nanos() as u64);
                version += workload.batch as u64;
                digest.committed_append(
                    writer as u64,
                    append as u64,
                    stream_id,
                    workload.batch as u64,
                    payload.as_ref(),
                );
            }
            WriterResult {
                completed: workload.batches_per_writer as u64,
                completion_monotonic_ns: monotonic_ns(),
                digest,
                latencies,
            }
        }));
    }
    let mut writer_results = Vec::with_capacity(joins.len());

    gate.wait_until_ready(workload.writers);
    let ready_monotonic_ns = monotonic_ns();
    let alloc_before = allocation::snapshot();
    let commit_before = committer.metrics();
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
            writer_results.push(join.await);
        }
        writer_results
    });
    let t1_monotonic_ns = monotonic_ns();
    control.disable_perf_after_t1(&control_nonce, t1_monotonic_ns);
    let alloc_after = allocation::snapshot();
    let cpu_after = cpu_snapshot();
    let counter_end_monotonic_ns = monotonic_ns();
    let commit_after = committer.metrics();
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
    let committed_batches = commit_after
        .batches
        .checked_sub(commit_before.batches)
        .expect("commit batches regressed");
    assert_eq!(
        committed_batches,
        workload.appends(),
        "bare committer batch count differs from successful submissions"
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
    runtime.block_on(committer.shutdown());
    PointResult {
        allocations: alloc_after.delta_from(alloc_before),
        appends: workload.appends(),
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
        latency_samples: latencies.len() as u64,
        max_ns: *latencies.last().expect("latency samples"),
        p50_ns: nearest_rank(&latencies, 50, 100),
        p99_ns: nearest_rank(&latencies, 99, 100),
        payload_bytes: workload.payload_total(),
        wall_ns: t1_monotonic_ns - t0_monotonic_ns,
        writer_samples,
    }
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
    let group = workload.durability == DurabilityKind::Group;
    let mut fields = vec![
        ("accepted_batches", json_u64(row.appends)),
        ("allocated_bytes", json_u64(row.allocations.bytes)),
        ("allocation_calls", json_u64(row.allocations.calls)),
        ("appends", json_u64(row.appends)),
        ("barrier_count", json_u64(row.groups)),
        ("batch_size", json_u64(workload.batch as u64)),
        ("batches_per_writer", json_u64(workload.batches_per_writer as u64)),
        ("borrowed_batches", json_u64(0)),
        ("borrowed_payload_bytes", json_u64(0)),
        ("borrowed_records", json_u64(0)),
        ("conflicts", json_u64(0)),
        ("control_events", json_u64(0)),
        ("defensive_copy_bytes", json_u64(0)),
        ("defensive_copy_records", json_u64(0)),
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
        ("log_events", json_u64(row.domain_events)),
        ("logical_digest", json_string(&format!("{:016x}", row.digest))),
        ("owned_batches", json_u64(0)),
        ("owned_payload_bytes", json_u64(0)),
        ("owned_records", json_u64(0)),
        ("path_label", json_string(adapter::PATH_LABEL)),
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
            // The bare raw-numeric surface returns no public opaque cursor.
            ("opaque_cursor_monotone", json_bool(false)),
            ("registry_events", json_u64(0)),
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
            ("byte_reservations_after", json_string("not_available")),
            ("counter_snapshot_after_warm", json_bool(true)),
            ("fsync_histogram_includes_warm", json_bool(true)),
            ("group_width_distribution", json_string("not_available")),
            ("jain_ppb", json_u64(jain)),
            ("max_to_median_p99_ppb", json_u64(max_p99)),
            ("min_to_median_rate_ppb", json_u64(min_rate)),
            ("oldest_queued_age_ns", json_string("not_available")),
            ("queue_bytes", json_string("not_available")),
            ("queue_depth", json_string("not_available")),
            ("waiter_reservations_after", json_string("not_available")),
            ("warm_names_established", json_bool(true)),
            ("warm_rounds", json_u64(4)),
            ("warm_writers", json_u64(64)),
            ("writer_samples_json", writer_samples_json(&row.writer_samples)),
        ]);
    }
    fields.sort_unstable_by_key(|field| field.0);
    println!("{}", canonical_object(&fields));
}

fn self_test() {
    let workload = Workload::primary(DurabilityKind::Group, 24, 10, 1);
    assert_eq!(workload.batches_per_writer, 500);
    assert_eq!(workload.events(), 5_000);
    assert_payload_contract();
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
        "primary" | "new_names" | "fairness" | "cpu_profiles"
        | "syscall_profiles" => {
            contract::validate(BINARY_KIND, TIMED_SURFACE);
            validate_row_args(&mode);
            let workload = configured_workload(&mode);
            let root = PathBuf::from(required("ASTERISM_REBASELINE_STORE"));
            let warm_rounds = usize::from(mode == "fairness") * 4;
            emit_point(&mode, workload, run_point(workload, root, warm_rounds));
        }
        mode => panic!("unsupported ASTERISM_REBASELINE_MODE={mode}"),
    }
}
