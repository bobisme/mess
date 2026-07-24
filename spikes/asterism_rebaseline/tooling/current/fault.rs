//! Exact A current-fault child for the Asterism rebaseline.
//!
//! This source is materialized as a current `mess-store` example together
//! with the reviewed cfg(test)-only product overlay.  Every reported PASS is
//! produced only after its named case has executed.  The five crash windows
//! use a self-exec worker that is stopped at a real product hook and killed by
//! the parent with SIGKILL; the corruption cases mutate real store bytes.
//!
//! The final harness case consumes builder authority at compile time.  A
//! builder must provide all five `ASTERISM_FAULT_COMPILE_OUT_*` variables used
//! by `compile_out_authority` below.  Missing authority is a compilation
//! error, not an assumed PASS.  The pristine and overlay release digests must
//! be identical, while the symbol-scan digest binds the builder's independent
//! absence check for the hook API/symbol set.

#[path = "asterism_rebaseline_shared/allocation.rs"]
mod allocation;
#[path = "asterism_rebaseline_shared/control.rs"]
mod control;
#[path = "asterism_rebaseline_shared/schema.rs"]
mod schema;

mod contract {
    pub const PROTOCOL: &str = "bn-2l3n-asterism-rebaseline-v3";
    pub const PROTOCOL_SHA256: &str =
        "d9ee10b2cccdaf6428bf1419a8c2ee74d272e987dc3617a80b64ad2e9d7a18dd";
    pub const VARIANT: &str = "A";
}

mod compile_out_authority {
    pub const SCHEMA: &str = env!("ASTERISM_FAULT_COMPILE_OUT_SCHEMA");
    pub const IDENTICAL: &str = env!("ASTERISM_FAULT_COMPILE_OUT_IDENTICAL");
    pub const PRISTINE_SHA256: &str =
        env!("ASTERISM_FAULT_COMPILE_OUT_PRISTINE_SHA256");
    pub const OVERLAY_RELEASE_SHA256: &str =
        env!("ASTERISM_FAULT_COMPILE_OUT_OVERLAY_RELEASE_SHA256");
    pub const SYMBOL_ABSENCE_SHA256: &str =
        env!("ASTERISM_FAULT_COMPILE_OUT_SYMBOL_ABSENCE_SHA256");
}

use std::ffi::c_int;
use std::fs::{File, OpenOptions};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use allocation::{AllocationSnapshot, CountingAllocator};
use control::{Control, MeasuredMarkers, cpu_snapshot, monotonic_ns};
use mess_log::crc::batch_crc;
use mess_log::format::HEADER_CRC_OFF;
use mess_log::scanner::{Recovery, scan_image};
use mess_store::backend::{AppendError, Backend, RecordToAppend};
use mess_store::engine::{TestEngineHook, TestEngineHookPoint};
use mess_store::{Durability, EngineOptions, LogEngine, Version};
use schema::{canonical_object, json_bool, json_string, json_u64};

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const OWNER_RING_INTENTS: usize = 1_024;
const WORKER_NONCE_ENV: &str = "ASTERISM_REBASELINE_FAULT_WORKER_NONCE";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CaseSpec {
    id:             &'static str,
    classification: &'static str,
}

const CASE_SPECS: [CaseSpec; 20] = [
    CaseSpec {
        id:             "cancel-before-admission",
        classification: "cancellation",
    },
    CaseSpec {
        id:             "cancel-after-ownership",
        classification: "cancellation",
    },
    CaseSpec { id: "kill-pre-write", classification: "durability" },
    CaseSpec {
        id:             "kill-partial-write",
        classification: "durability",
    },
    CaseSpec {
        id:             "kill-post-write-pre-barrier",
        classification: "durability",
    },
    CaseSpec {
        id:             "kill-post-barrier-pre-publication",
        classification: "durability",
    },
    CaseSpec {
        id:             "kill-post-publication-pre-completion",
        classification: "durability",
    },
    CaseSpec { id: "short-write", classification: "durability" },
    CaseSpec { id: "write-error", classification: "durability" },
    CaseSpec {
        id:             "fdatasync-error",
        classification: "durability",
    },
    CaseSpec {
        id:             "torn-truncated-tail",
        classification: "roll-recovery",
    },
    CaseSpec {
        id:             "invalid-marker-crc",
        classification: "roll-recovery",
    },
    CaseSpec {
        id:             "corrupt-registry-record",
        classification: "roll-recovery",
    },
    CaseSpec {
        id:             "refuted-corrupt-sidecar",
        classification: "roll-recovery",
    },
    CaseSpec {
        id:             "uncertain-persistence-poison",
        classification: "poison",
    },
    CaseSpec {
        id:             "acknowledged-group-survives-reopen",
        classification: "durability",
    },
    CaseSpec {
        id:             "owner-ring-intent-bound",
        classification: "boundedness",
    },
    CaseSpec {
        id:             "group-byte-time-bounds",
        classification: "boundedness",
    },
    CaseSpec {
        id:             "zero-reservations-after-cancel-complete",
        classification: "boundedness",
    },
    CaseSpec {
        id:             "fault-hook-compiles-out-binary-identical",
        classification: "harness",
    },
];

#[derive(Debug)]
struct CasePass {
    spec: CaseSpec,
}

impl CasePass {
    fn new(id: &'static str, classification: &'static str) -> Self {
        let spec = CaseSpec { id, classification };
        assert!(CASE_SPECS.contains(&spec), "unapproved current-fault case");
        Self { spec }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct BoundednessEvidence {
    owner_ring_intents:        usize,
    group_byte_bound_proven:   bool,
    group_time_bound_proven:   bool,
    waiter_reservations_after: usize,
    byte_reservations_after:   usize,
}

struct FaultEvidence {
    passes:      Vec<CasePass>,
    boundedness: BoundednessEvidence,
}

#[derive(Debug)]
struct FaultArgs {
    attempt_nonce: String,
    phase:         String,
}

#[derive(Debug)]
enum Invocation {
    Fault(FaultArgs),
    Smoke,
}

#[derive(Clone, Copy, Debug)]
enum KillPoint {
    PreWrite,
    PartialWrite,
    PostWritePreBarrier,
    PostBarrierPrePublication,
    PostPublicationPreCompletion,
}

impl KillPoint {
    fn id(self) -> &'static str {
        match self {
            KillPoint::PreWrite => "kill-pre-write",
            KillPoint::PartialWrite => "kill-partial-write",
            KillPoint::PostWritePreBarrier => "kill-post-write-pre-barrier",
            KillPoint::PostBarrierPrePublication => {
                "kill-post-barrier-pre-publication"
            }
            KillPoint::PostPublicationPreCompletion => {
                "kill-post-publication-pre-completion"
            }
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "kill-pre-write" => Self::PreWrite,
            "kill-partial-write" => Self::PartialWrite,
            "kill-post-write-pre-barrier" => Self::PostWritePreBarrier,
            "kill-post-barrier-pre-publication" => {
                Self::PostBarrierPrePublication
            }
            "kill-post-publication-pre-completion" => {
                Self::PostPublicationPreCompletion
            }
            _ => panic!("unknown fault worker case: {value}"),
        }
    }
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
        assert_eq!(required("ASTERISM_REBASELINE_SMOKE_TARGET"), "fault");
        assert_eq!(
            required("ASTERISM_REBASELINE_PROTOCOL"),
            contract::PROTOCOL
        );
        return Invocation::Smoke;
    }

    assert_eq!(arguments.len(), 11, "current-fault argv differs");
    assert_eq!(arguments[0], "--fault");
    assert_eq!(arguments[1], "--protocol");
    assert_eq!(arguments[2], contract::PROTOCOL);
    assert_eq!(arguments[3], "--attempt-nonce");
    assert_eq!(arguments[5], "--variant");
    assert_eq!(arguments[6], contract::VARIANT);
    assert_eq!(arguments[7], "--phase");
    assert!(matches!(arguments[8].as_str(), "pre" | "post"));
    assert_eq!(arguments[9], "--suite");
    assert_eq!(arguments[10], "current-fault");
    let attempt_nonce = arguments[4].clone();
    let phase = arguments[8].clone();
    assert!(lower_hex(&attempt_nonce, 64), "invalid fault attempt nonce");
    assert_eq!(required("ASTERISM_REBASELINE_MODE"), "fault");
    assert_eq!(required("ASTERISM_REBASELINE_ATTEMPT_NONCE"), attempt_nonce);
    assert_eq!(required("ASTERISM_REBASELINE_PHASE"), phase);
    assert_eq!(required("ASTERISM_REBASELINE_PROTOCOL"), contract::PROTOCOL);
    assert_eq!(required("ASTERISM_REBASELINE_SUITE"), "current-fault");
    assert_eq!(required("ASTERISM_REBASELINE_VARIANT"), contract::VARIANT);
    Invocation::Fault(FaultArgs { attempt_nonce, phase })
}

fn options(durability: Durability) -> EngineOptions {
    EngineOptions { durability, ..Default::default() }
}

fn small_segment_options(durability: Durability) -> EngineOptions {
    EngineOptions { durability, segment_size: 64 * 1024, ..Default::default() }
}

fn record(message_type: &str, data: impl Into<Vec<u8>>) -> RecordToAppend {
    RecordToAppend {
        message_type: message_type.to_owned(),
        data:         data.into(),
    }
}

fn assert_backend_error(
    value: Result<
        mess_store::backend::Appended,
        AppendError<mess_store::EngineError>,
    >,
) {
    assert!(matches!(value, Err(AppendError::Backend(_))));
}

fn assert_zero_reservations(engine: &LogEngine) {
    let metrics = engine.metrics();
    assert_eq!(metrics.owner_intent_slots_in_use, 0);
    assert_eq!(metrics.owner_intent_bytes_in_use, 0);
}

async fn wait_for_metrics(
    engine: &LogEngine,
    predicate: impl Fn(mess_store::EngineMetrics) -> bool,
    message: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if predicate(engine.metrics()) {
            return;
        }
        assert!(Instant::now() < deadline, "{message}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn case_cancel_before_admission(root: &Path) -> CasePass {
    let engine = LogEngine::open_with(root, options(Durability::Process))
        .expect("open cancellation-before-admission store");
    engine
        .append_batch(
            "cancel-before-admission",
            Version::NoStream,
            &[record("fault.hot", b"prime".to_vec())],
        )
        .await
        .expect("prime cancellation-before-admission stream");

    let cohort = engine.arm_test_owner_cohort(OWNER_RING_INTENTS + 1);
    let pause = engine.arm_test_hook(TestEngineHook::Pause {
        point:         TestEngineHookPoint::Admission,
        after_reaches: 0,
    });
    let mut occupants = Vec::with_capacity(OWNER_RING_INTENTS + 1);
    for ordinal in 0..=OWNER_RING_INTENTS {
        let engine = engine.clone();
        occupants.push(tokio::spawn(async move {
            engine
                .append_batch(
                    "cancel-before-admission",
                    Version::At(0),
                    &[record("fault.hot", ordinal.to_le_bytes().to_vec())],
                )
                .await
        }));
    }
    cohort.wait_until_admitted(OWNER_RING_INTENTS + 1);
    pause.wait_until_reached();
    let full = engine.metrics();
    assert_eq!(full.owner_intent_slots_in_use, OWNER_RING_INTENTS);
    let baseline_bytes = full.owner_intent_bytes_in_use;

    let candidate_engine = engine.clone();
    let candidate = tokio::spawn(async move {
        candidate_engine
            .append_batch(
                "cancel-before-admission",
                Version::At(0),
                &[record("fault.hot", vec![0xA5; 1024 * 1024])],
            )
            .await
    });
    wait_for_metrics(
        &engine,
        |metrics| {
            metrics.owner_intent_slots_in_use == OWNER_RING_INTENTS
                && metrics.owner_intent_bytes_in_use > baseline_bytes
        },
        "candidate never acquired byte permits before queue admission",
    )
    .await;
    candidate.abort();
    assert!(
        candidate
            .await
            .expect_err("cancelled candidate returned")
            .is_cancelled()
    );
    wait_for_metrics(
        &engine,
        |metrics| {
            metrics.owner_intent_slots_in_use == OWNER_RING_INTENTS
                && metrics.owner_intent_bytes_in_use == baseline_bytes
        },
        "pre-admission cancellation retained byte permits",
    )
    .await;

    pause.release();
    drop(pause);
    drop(cohort);
    let mut successes = 0;
    let mut conflicts = 0;
    for occupant in occupants {
        match occupant.await.expect("owner-ring occupant panicked") {
            Ok(appended) => {
                successes += 1;
                assert_eq!(appended.version, Version::At(1));
            }
            Err(AppendError::Conflict {
                expected: Version::At(0),
                actual: Version::At(1),
            }) => conflicts += 1,
            other => panic!("unexpected owner-ring outcome: {other:?}"),
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(conflicts, OWNER_RING_INTENTS);
    assert_zero_reservations(&engine);
    CasePass::new("cancel-before-admission", "cancellation")
}

async fn case_cancel_after_ownership(root: &Path) -> CasePass {
    let engine =
        LogEngine::open_with(root, options(Durability::group_default()))
            .expect("open cancellation-after-ownership store");
    let pause = engine.arm_test_hook(TestEngineHook::Pause {
        point:         TestEngineHookPoint::PostPublicationPreCompletion,
        after_reaches: 0,
    });
    let append_engine = engine.clone();
    let abandoned = tokio::spawn(async move {
        append_engine
            .append_batch(
                "cancel-after-fresh-stream",
                Version::NoStream,
                &[record("fault.fresh-type", b"accepted".to_vec())],
            )
            .await
    });
    pause.wait_until_reached();
    assert!(!abandoned.is_finished(), "append completed before hook release");
    abandoned.abort();
    assert!(
        abandoned.await.expect_err("abandoned append returned").is_cancelled()
    );
    pause.release();
    drop(pause);

    let sentinel = engine
        .append_batch(
            "cancel-after-fresh-stream",
            Version::At(0),
            &[record("fault.fresh-type", b"sentinel".to_vec())],
        )
        .await
        .expect("sentinel after abandoned receiver");
    assert_eq!(sentinel.version, Version::At(1));
    let observed = engine
        .read_stream("cancel-after-fresh-stream", Version::NoStream, 3)
        .await
        .expect("read cancellation-after-ownership stream");
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0].data, b"accepted");
    assert_eq!(observed[1].data, b"sentinel");
    assert_zero_reservations(&engine);
    CasePass::new("cancel-after-ownership", "cancellation")
}

fn worker_hook(point: KillPoint) -> TestEngineHook {
    match point {
        KillPoint::PreWrite => TestEngineHook::Pause {
            point:         TestEngineHookPoint::PrePwrite,
            after_reaches: 0,
        },
        KillPoint::PartialWrite => TestEngineHook::PartialRealWrite {
            after_successful_pwrites: 0,
            after_bytes:              8,
        },
        KillPoint::PostWritePreBarrier => TestEngineHook::Pause {
            point:         TestEngineHookPoint::PreFdatasync,
            after_reaches: 0,
        },
        KillPoint::PostBarrierPrePublication => TestEngineHook::Pause {
            point:         TestEngineHookPoint::PostFdatasyncPrePublication,
            after_reaches: 0,
        },
        KillPoint::PostPublicationPreCompletion => TestEngineHook::Pause {
            point:         TestEngineHookPoint::PostPublicationPreCompletion,
            after_reaches: 0,
        },
    }
}

fn worker_args() -> Option<(KillPoint, PathBuf, PathBuf, String)> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments.first().map(String::as_str) != Some("--fault-worker") {
        return None;
    }
    assert_eq!(arguments.len(), 9, "fault worker argv differs");
    assert_eq!(arguments[1], "--case");
    assert_eq!(arguments[3], "--store");
    assert_eq!(arguments[5], "--ready");
    assert_eq!(arguments[7], "--nonce");
    let nonce = arguments[8].clone();
    assert!(lower_hex(&nonce, 64), "invalid fault worker nonce");
    assert_eq!(required(WORKER_NONCE_ENV), nonce);
    Some((
        KillPoint::parse(&arguments[2]),
        PathBuf::from(&arguments[4]),
        PathBuf::from(&arguments[6]),
        nonce,
    ))
}

fn write_ready_marker(path: &Path, point: KillPoint, nonce: &str) {
    let expected = format!("case={}\nnonce={}\n", point.id(), nonce);
    // Publish atomically: the parent polls `path.is_file()` and immediately
    // reads it, so it must never observe the marker after creation but before
    // its contents land.  Write+fsync a temp file, then rename it into place.
    let tmp = path.with_extension("marker-tmp");
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .expect("create fault worker ready marker temp");
        file.write_all(expected.as_bytes()).expect("write fault worker marker");
        file.sync_all().expect("sync fault worker marker");
    }
    std::fs::rename(&tmp, path).expect("publish fault worker ready marker");
}

fn fault_worker(
    point: KillPoint,
    store: PathBuf,
    ready: PathBuf,
    nonce: String,
) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("create fault worker runtime");
    runtime.block_on(async move {
        let engine = LogEngine::open_with(&store, options(Durability::Os))
            .expect("fault worker open");
        let hook = engine.arm_test_hook(worker_hook(point));
        let append_engine = engine.clone();
        let append = tokio::spawn(async move {
            append_engine
                .append_batch(
                    "kill-hot-stream",
                    Version::At(0),
                    &[record("fault.kill", point.id().as_bytes().to_vec())],
                )
                .await
        });
        tokio::task::yield_now().await;
        hook.wait_until_reached();
        assert!(!append.is_finished(), "fault worker append completed early");
        write_ready_marker(&ready, point, &nonce);
        std::future::pending::<()>().await;
    });
}

fn spawn_fault_worker(
    point: KillPoint,
    store: &Path,
    ready: &Path,
    nonce: &str,
) -> Child {
    Command::new(std::env::current_exe().expect("resolve fault executable"))
        .arg("--fault-worker")
        .arg("--case")
        .arg(point.id())
        .arg("--store")
        .arg(store)
        .arg("--ready")
        .arg(ready)
        .arg("--nonce")
        .arg(nonce)
        .env(WORKER_NONCE_ENV, nonce)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn fault worker")
}

fn await_worker_ready(
    child: &mut Child,
    ready: &Path,
    point: KillPoint,
    nonce: &str,
) {
    let expected = format!("case={}\nnonce={}\n", point.id(), nonce);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            child.try_wait().expect("poll fault worker").is_none(),
            "fault worker exited before ready"
        );
        if ready.is_file() {
            let mut observed = String::new();
            File::open(ready)
                .expect("open fault worker marker")
                .read_to_string(&mut observed)
                .expect("read fault worker marker");
            assert_eq!(observed, expected, "fault worker marker differs");
            return;
        }
        assert!(Instant::now() < deadline, "fault worker never became ready");
        std::thread::sleep(Duration::from_millis(1));
    }
}

async fn run_kill_case(root: &Path, point: KillPoint, nonce: &str) -> CasePass {
    let store = root.join("store");
    let ready = root.join("worker-ready");
    {
        let engine = LogEngine::open_with(&store, options(Durability::Os))
            .expect("open kill-case seed store");
        engine
            .append_batch(
                "kill-hot-stream",
                Version::NoStream,
                &[record("fault.kill", b"prime".to_vec())],
            )
            .await
            .expect("prime kill-case stream");
    }

    let mut child = spawn_fault_worker(point, &store, &ready, nonce);
    await_worker_ready(&mut child, &ready, point, nonce);
    child.kill().expect("SIGKILL fault worker");
    let status = child.wait().expect("wait SIGKILL fault worker");
    assert_eq!(status.signal(), Some(9), "fault worker was not SIGKILLed");

    let engine = LogEngine::open_with(&store, options(Durability::Os))
        .expect("reopen after worker SIGKILL");
    let observed = engine
        .read_stream("kill-hot-stream", Version::NoStream, 3)
        .await
        .expect("read kill-case recovery");
    assert!((1..=2).contains(&observed.len()));
    assert_eq!(observed[0].data, b"prime");
    if observed.len() == 2 {
        assert_eq!(observed[1].data, point.id().as_bytes());
    }
    match point {
        KillPoint::PreWrite | KillPoint::PartialWrite => {
            assert_eq!(
                observed.len(),
                1,
                "pre-commit crash published an event"
            );
        }
        KillPoint::PostWritePreBarrier => {}
        KillPoint::PostBarrierPrePublication
        | KillPoint::PostPublicationPreCompletion => {
            assert_eq!(observed.len(), 2, "durable crash point lost its event");
        }
    }
    CasePass::new(point.id(), "durability")
}

async fn case_short_write(root: &Path) -> CasePass {
    let engine = LogEngine::open_with(root, options(Durability::Process))
        .expect("open short-write store");
    engine
        .append_batch(
            "short-write",
            Version::NoStream,
            &[record("fault.injected", b"prime".to_vec())],
        )
        .await
        .expect("prime short-write store");
    let hook = engine.arm_test_hook(TestEngineHook::PwriteZero {
        after_successful_pwrites: 0,
    });
    let result = engine
        .append_batch(
            "short-write",
            Version::At(0),
            &[record("fault.injected", b"must-not-ack".to_vec())],
        )
        .await;
    hook.wait_until_reached();
    assert_backend_error(result);
    drop(hook);
    assert_zero_reservations(&engine);
    drop(engine);
    let engine = LogEngine::open_with(root, options(Durability::Process))
        .expect("reopen after short write");
    assert_eq!(engine.head("short-write").await.unwrap(), Version::At(0));
    CasePass::new("short-write", "durability")
}

async fn case_write_error(root: &Path) -> CasePass {
    let engine = LogEngine::open_with(root, options(Durability::Process))
        .expect("open write-error store");
    engine
        .append_batch(
            "write-error",
            Version::NoStream,
            &[record("fault.injected", b"prime".to_vec())],
        )
        .await
        .expect("prime write-error store");
    let hook = engine.arm_test_hook(TestEngineHook::PwriteEio {
        after_successful_pwrites: 0,
    });
    let result = engine
        .append_batch(
            "write-error",
            Version::At(0),
            &[record("fault.injected", b"must-not-ack".to_vec())],
        )
        .await;
    hook.wait_until_reached();
    assert_backend_error(result);
    drop(hook);
    assert_zero_reservations(&engine);
    drop(engine);
    let engine = LogEngine::open_with(root, options(Durability::Process))
        .expect("reopen after write EIO");
    assert_eq!(engine.head("write-error").await.unwrap(), Version::At(0));
    CasePass::new("write-error", "durability")
}

async fn case_fdatasync_error(root: &Path) -> CasePass {
    let engine = LogEngine::open_with(root, options(Durability::Os))
        .expect("open fdatasync-error store");
    engine
        .append_batch(
            "fdatasync-error",
            Version::NoStream,
            &[record("fault.injected", b"prime".to_vec())],
        )
        .await
        .expect("prime fdatasync-error store");
    let hook = engine.arm_test_hook(TestEngineHook::FdatasyncEio {
        after_successful_fdatasyncs: 0,
    });
    let result = engine
        .append_batch(
            "fdatasync-error",
            Version::At(0),
            &[record("fault.injected", b"indeterminate".to_vec())],
        )
        .await;
    hook.wait_until_reached();
    assert_backend_error(result);
    assert!(engine.metrics().degraded_poisoned);
    let later = engine
        .append_batch(
            "fdatasync-error",
            Version::At(0),
            &[record("fault.injected", b"must-fail-fast".to_vec())],
        )
        .await;
    assert_backend_error(later);
    drop(hook);
    assert_zero_reservations(&engine);
    drop(engine);
    let engine = LogEngine::open_with(root, options(Durability::Os))
        .expect("reopen after fdatasync EIO");
    assert!(matches!(
        engine.head("fdatasync-error").await.unwrap(),
        Version::At(0) | Version::At(1)
    ));
    CasePass::new("fdatasync-error", "durability")
}

fn log_path(store: &Path) -> PathBuf {
    let mut logs: Vec<PathBuf> = std::fs::read_dir(store)
        .expect("read store directory")
        .map(|entry| entry.expect("read store entry").path())
        .filter(|path| {
            path.extension().and_then(|value| value.to_str()) == Some("log")
        })
        .collect();
    logs.sort();
    assert_eq!(logs.len(), 1, "corruption fixture must have one segment");
    logs.pop().unwrap()
}

fn scan_disk(path: &Path) -> (Recovery, Vec<u8>) {
    let bytes = std::fs::read(path).expect("read segment image");
    let recovery = scan_image(&bytes, None);
    (recovery, bytes)
}

async fn seed_two_event_tail(store: &Path, stream: &str) {
    let engine =
        LogEngine::open_with(store, small_segment_options(Durability::Process))
            .expect("open tail-corruption fixture");
    engine
        .append_batch(
            stream,
            Version::NoStream,
            &[record("fault.corruption", b"prefix".to_vec())],
        )
        .await
        .expect("append corruption prefix");
    engine
        .append_batch(
            stream,
            Version::At(0),
            &[record("fault.corruption", b"tail".to_vec())],
        )
        .await
        .expect("append corruption tail");
}

async fn case_torn_truncated_tail(root: &Path) -> CasePass {
    let store = root.join("store");
    seed_two_event_tail(&store, "torn-tail").await;
    let path = log_path(&store);
    let (before, _) = scan_disk(&path);
    let tail = before.accepted.last().expect("tail batch");
    let torn_len = tail.offset + tail.total_len - 1;
    OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open tail for truncation")
        .set_len(torn_len)
        .expect("truncate committed tail");
    assert!(std::fs::metadata(&path).unwrap().len() < before.safe_offset);
    let engine = LogEngine::open_with(
        &store,
        small_segment_options(Durability::Process),
    )
    .expect("reopen torn tail");
    let observed = engine
        .read_stream("torn-tail", Version::NoStream, 3)
        .await
        .expect("read torn-tail recovery");
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].data, b"prefix");
    CasePass::new("torn-truncated-tail", "roll-recovery")
}

fn mutate_byte_sync(path: &Path, offset: u64) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open mutation target");
    file.seek(SeekFrom::Start(offset)).expect("seek mutation target");
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).expect("read mutation byte");
    byte[0] ^= 0xFF;
    file.seek(SeekFrom::Start(offset)).expect("rewind mutation target");
    file.write_all(&byte).expect("write mutation byte");
    file.sync_all().expect("sync mutation byte");
}

async fn case_invalid_marker_crc(root: &Path) -> CasePass {
    let store = root.join("store");
    seed_two_event_tail(&store, "invalid-marker").await;
    let path = log_path(&store);
    let (before, _) = scan_disk(&path);
    let tail = before.accepted.last().expect("marker tail batch");
    let crc_echo_byte = tail.offset + tail.total_len - 1;
    mutate_byte_sync(&path, crc_echo_byte);
    let (after, _) = scan_disk(&path);
    assert_eq!(after.accepted.len() + 1, before.accepted.len());
    assert_eq!(after.safe_offset, tail.offset);
    let engine = LogEngine::open_with(
        &store,
        small_segment_options(Durability::Process),
    )
    .expect("reopen invalid marker/CRC");
    let observed = engine
        .read_stream("invalid-marker", Version::NoStream, 3)
        .await
        .expect("read invalid-marker recovery");
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].data, b"prefix");
    CasePass::new("invalid-marker-crc", "roll-recovery")
}

async fn case_corrupt_registry_record(root: &Path) -> CasePass {
    let store = root.join("store");
    {
        let engine = LogEngine::open_with(
            &store,
            small_segment_options(Durability::Process),
        )
        .expect("open registry-corruption fixture");
        engine
            .append_batch(
                "registry-corruption",
                Version::NoStream,
                &[record("fault.registry", b"domain".to_vec())],
            )
            .await
            .expect("append registry-corruption fixture");
    }
    let path = log_path(&store);
    let (before, mut image) = scan_disk(&path);
    let registry = before
        .accepted
        .iter()
        .find(|batch| batch.stream_id == 0)
        .copied()
        .expect("registry batch absent");
    let payload_offset = {
        let frame = registry
            .frames(&image)
            .expect("decode registry batch")
            .next()
            .expect("registry frame absent");
        frame.payload.as_ptr() as usize - image.as_ptr() as usize
    };
    image[payload_offset] = 0xFF;
    let start = registry.offset as usize;
    let end = start + registry.total_len as usize;
    let repaired_crc = batch_crc(&image[start..end]);
    image[start + HEADER_CRC_OFF..start + HEADER_CRC_OFF + 4]
        .copy_from_slice(&repaired_crc.to_le_bytes());
    image[end - 4..end].copy_from_slice(&repaired_crc.to_le_bytes());
    let after = scan_image(&image, None);
    assert_eq!(after.accepted.len(), before.accepted.len());
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&path)
        .expect("open registry image for rewrite");
    file.write_all(&image).expect("rewrite corrupt registry image");
    file.sync_all().expect("sync corrupt registry image");
    drop(file);
    let error = LogEngine::open_with(
        &store,
        small_segment_options(Durability::Process),
    )
    .err()
    .expect("CRC-valid corrupt registry record must refuse open");
    let message = error.to_string().to_lowercase();
    assert!(
        message.contains("registry"),
        "unexpected registry refusal: {error}"
    );
    CasePass::new("corrupt-registry-record", "roll-recovery")
}

fn first_sidecar(store: &Path, extension: &str) -> PathBuf {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(store.join("sealed"))
        .expect("read sealed directory")
        .map(|entry| entry.expect("read sealed entry").path())
        .filter(|path| {
            path.extension().and_then(|value| value.to_str()) == Some(extension)
        })
        .collect();
    paths.sort();
    paths.into_iter().next().expect("expected sealed sidecar")
}

async fn case_refuted_corrupt_sidecar(root: &Path) -> CasePass {
    let store = root.join("store");
    let expected: Vec<Vec<u8>> = (0..256u64)
        .map(|ordinal| {
            let mut payload = ordinal.to_le_bytes().to_vec();
            payload.resize(128, ordinal as u8);
            payload
        })
        .collect();
    {
        let engine = LogEngine::open_with(
            &store,
            small_segment_options(Durability::Process),
        )
        .expect("open sidecar fixture");
        let mut version = Version::NoStream;
        for payload in &expected {
            version = engine
                .append_batch(
                    "sidecar-stream",
                    version,
                    &[record("fault.sidecar", payload.clone())],
                )
                .await
                .expect("append sidecar fixture")
                .version;
        }
        engine.seal_active().expect("seal sidecar fixture");
        assert!(engine.sealed_segment_count() > 0);
    }
    let pcol = first_sidecar(&store, "pcol");
    let length = std::fs::metadata(&pcol).unwrap().len();
    assert!(length > 8, "pcol fixture is too short");
    mutate_byte_sync(&pcol, length / 2);
    let engine = LogEngine::open_with(
        &store,
        small_segment_options(Durability::Process),
    )
    .expect("reopen with corrupt refuted sidecar");
    let observed = engine
        .read_stream("sidecar-stream", Version::NoStream, expected.len() + 1)
        .await
        .expect("raw-log fallback after corrupt pcol");
    assert_eq!(observed.len(), expected.len());
    for (record, payload) in observed.iter().zip(expected) {
        assert_eq!(record.data, payload);
    }
    CasePass::new("refuted-corrupt-sidecar", "roll-recovery")
}

async fn case_uncertain_persistence_poison(root: &Path) -> CasePass {
    let engine =
        LogEngine::open_with(root, options(Durability::group_default()))
            .expect("open poison store");
    engine
        .append_batch(
            "poison-stream",
            Version::NoStream,
            &[record("fault.poison", b"durable-prefix".to_vec())],
        )
        .await
        .expect("append poison prefix");
    let hook = engine.arm_test_hook(TestEngineHook::FdatasyncEio {
        after_successful_fdatasyncs: 0,
    });
    let uncertain = engine
        .append_batch(
            "poison-stream",
            Version::At(0),
            &[record("fault.poison", b"uncertain".to_vec())],
        )
        .await;
    hook.wait_until_reached();
    assert_backend_error(uncertain);
    assert!(engine.metrics().degraded_poisoned);
    for _ in 0..2 {
        assert_backend_error(
            engine
                .append_batch(
                    "poison-stream",
                    Version::At(0),
                    &[record("fault.poison", b"never-retry-barrier".to_vec())],
                )
                .await,
        );
        assert!(engine.metrics().degraded_poisoned);
    }
    let readable = engine
        .read_stream("poison-stream", Version::NoStream, 3)
        .await
        .expect("degraded reads remain available");
    assert_eq!(readable[0].data, b"durable-prefix");
    drop(hook);
    assert_zero_reservations(&engine);
    CasePass::new("uncertain-persistence-poison", "poison")
}

async fn case_acknowledged_group_survives_reopen(root: &Path) -> CasePass {
    let expected: Vec<Vec<u8>> =
        (0..32u64).map(|ordinal| ordinal.to_le_bytes().to_vec()).collect();
    {
        let engine =
            LogEngine::open_with(root, options(Durability::group_default()))
                .expect("open acknowledged Group store");
        let mut version = Version::NoStream;
        for payload in &expected {
            version = engine
                .append_batch(
                    "acknowledged-group",
                    version,
                    &[record("fault.group-ack", payload.clone())],
                )
                .await
                .expect("acknowledged Group append")
                .version;
        }
        assert_eq!(version, Version::At(expected.len() as u64 - 1));
    }
    let engine =
        LogEngine::open_with(root, options(Durability::group_default()))
            .expect("reopen acknowledged Group store");
    let observed = engine
        .read_stream(
            "acknowledged-group",
            Version::NoStream,
            expected.len() + 1,
        )
        .await
        .expect("read acknowledged Group corpus");
    assert_eq!(observed.len(), expected.len());
    for (record, payload) in observed.iter().zip(expected) {
        assert_eq!(record.data, payload);
    }
    CasePass::new("acknowledged-group-survives-reopen", "durability")
}

async fn case_owner_ring_intent_bound(root: &Path) -> (CasePass, usize) {
    let engine = LogEngine::open_with(root, options(Durability::Process))
        .expect("open owner-ring-bound store");
    engine
        .append_batch(
            "owner-ring-bound",
            Version::NoStream,
            &[record("fault.bound", b"prime".to_vec())],
        )
        .await
        .expect("prime owner-ring-bound store");
    let cohort = engine.arm_test_owner_cohort(OWNER_RING_INTENTS + 1);
    let pause = engine.arm_test_hook(TestEngineHook::Pause {
        point:         TestEngineHookPoint::Admission,
        after_reaches: 0,
    });
    let mut tasks = Vec::new();
    for ordinal in 0..=OWNER_RING_INTENTS {
        let engine = engine.clone();
        tasks.push(tokio::spawn(async move {
            engine
                .append_batch(
                    "owner-ring-bound",
                    Version::At(0),
                    &[record("fault.bound", ordinal.to_le_bytes().to_vec())],
                )
                .await
        }));
    }
    cohort.wait_until_admitted(OWNER_RING_INTENTS + 1);
    pause.wait_until_reached();
    let observed_bound = engine.metrics().owner_intent_slots_in_use;
    assert_eq!(observed_bound, OWNER_RING_INTENTS);
    pause.release();
    drop(pause);
    drop(cohort);
    for task in tasks {
        let result = task.await.expect("owner-ring-bound task panicked");
        assert!(matches!(result, Ok(_) | Err(AppendError::Conflict { .. })));
    }
    assert_zero_reservations(&engine);
    (CasePass::new("owner-ring-intent-bound", "boundedness"), observed_bound)
}

async fn case_group_byte_time_bounds(root: &Path) -> (CasePass, bool, bool) {
    const TIME_TRAINING_COHORT: usize = 4;
    const TIME_INCOMPLETE_COHORT: usize = 2;
    const TIME_MAX_DELAY: Duration = Duration::from_millis(200);
    const TIME_ADMISSION_CAP: Duration = Duration::from_millis(50);
    const TIME_DEADLINE_TOLERANCE: Duration = Duration::from_millis(150);

    let byte_engine = LogEngine::open_with(
        root.join("byte"),
        options(Durability::Group {
            max_delay: Duration::from_secs(30),
            max_bytes: 1,
        }),
    )
    .expect("open Group byte-bound store");
    for stream in ["group-byte-a", "group-byte-b"] {
        byte_engine
            .append_batch(
                stream,
                Version::NoStream,
                &[record("fault.group-bound", b"prime".to_vec())],
            )
            .await
            .expect("prime Group byte-bound stream");
    }
    let byte_before = byte_engine.metrics().commit;
    let byte_cohort = byte_engine.arm_test_owner_cohort(2);
    let byte_a_engine = byte_engine.clone();
    let byte_a = tokio::spawn(async move {
        byte_a_engine
            .append_batch(
                "group-byte-a",
                Version::At(0),
                &[record("fault.group-bound", vec![7; 4096])],
            )
            .await
    });
    let byte_b_engine = byte_engine.clone();
    let byte_b = tokio::spawn(async move {
        byte_b_engine
            .append_batch(
                "group-byte-b",
                Version::At(0),
                &[record("fault.group-bound", vec![8; 4096])],
            )
            .await
    });
    byte_cohort.wait_until_admitted(2);
    tokio::time::timeout(Duration::from_secs(5), async {
        byte_a
            .await
            .expect("Group byte-a task panicked")
            .expect("Group byte-a");
        byte_b
            .await
            .expect("Group byte-b task panicked")
            .expect("Group byte-b");
    })
    .await
    .expect("Group byte bound failed to close before 30-second time bound");
    drop(byte_cohort);
    let byte_after = byte_engine.metrics().commit;
    let byte_groups = byte_after.groups - byte_before.groups;
    let byte_batches = byte_after.batches - byte_before.batches;
    let group_byte_bound_proven = byte_groups == 2
        && byte_batches == 2
        && byte_after.fsync.count - byte_before.fsync.count == byte_groups;
    assert!(group_byte_bound_proven);

    let time_engine = LogEngine::open_with(
        root.join("time"),
        options(Durability::Group {
            max_delay: TIME_MAX_DELAY,
            max_bytes: u64::MAX,
        }),
    )
    .expect("open Group time-bound store");
    for ordinal in 0..TIME_TRAINING_COHORT {
        time_engine
            .append_batch(
                &format!("group-time-bound-{ordinal}"),
                Version::NoStream,
                &[record("fault.group-bound", b"prime".to_vec())],
            )
            .await
            .expect("prime Group time-bound stream");
    }

    // A complete four-intent cohort deterministically leaves FlatOwner's
    // adaptive target at four.  The next two-intent cohort is therefore below
    // target: once its bounded admission completes, the target=1 early-close
    // path cannot explain its closure.
    let training_before = time_engine.metrics().commit;
    let training_cohort =
        time_engine.arm_test_owner_cohort(TIME_TRAINING_COHORT);
    let mut training_tasks = Vec::with_capacity(TIME_TRAINING_COHORT);
    for ordinal in 0..TIME_TRAINING_COHORT {
        let engine = time_engine.clone();
        training_tasks.push(tokio::spawn(async move {
            engine
                .append_batch(
                    &format!("group-time-bound-{ordinal}"),
                    Version::At(0),
                    &[record("fault.group-bound", b"train".to_vec())],
                )
                .await
        }));
    }
    training_cohort.wait_until_admitted(TIME_TRAINING_COHORT);
    tokio::time::timeout(Duration::from_secs(5), async {
        for task in training_tasks {
            task.await
                .expect("Group time training task panicked")
                .expect("Group time training append");
        }
    })
    .await
    .expect("Group time training cohort failed to complete");
    drop(training_cohort);
    let training_after = time_engine.metrics().commit;
    let training_batches = training_after.batches - training_before.batches;
    let training_groups = training_after.groups - training_before.groups;
    let training_fsyncs =
        training_after.fsync.count - training_before.fsync.count;
    let time_causal_setup = TIME_INCOMPLETE_COHORT < TIME_TRAINING_COHORT
        && training_batches == TIME_TRAINING_COHORT as u64
        && training_groups == 1
        && training_fsyncs == training_groups;
    assert!(time_causal_setup, "adaptive target training differs");

    let time_before = time_engine.metrics().commit;
    let deadline_hook = time_engine.arm_test_hook(TestEngineHook::Pause {
        point:         TestEngineHookPoint::PreFdatasync,
        after_reaches: 0,
    });
    let incomplete_cohort =
        time_engine.arm_test_owner_cohort(TIME_INCOMPLETE_COHORT);
    let time_started = Instant::now();
    let mut time_tasks = Vec::with_capacity(TIME_INCOMPLETE_COHORT);
    for ordinal in 0..TIME_INCOMPLETE_COHORT {
        let engine = time_engine.clone();
        time_tasks.push(tokio::spawn(async move {
            engine
                .append_batch(
                    &format!("group-time-bound-{ordinal}"),
                    Version::At(1),
                    &[record("fault.group-bound", b"deadline".to_vec())],
                )
                .await
        }));
    }
    incomplete_cohort.wait_until_admitted(TIME_INCOMPLETE_COHORT);
    let time_admission_elapsed = time_started.elapsed();
    let time_admission_bounded = time_admission_elapsed <= TIME_ADMISSION_CAP;
    assert!(
        time_admission_bounded,
        "incomplete cohort admission cannot explain deadline elapsed"
    );
    // PreFdatasync is the first boundary after gather closes and excludes the
    // barrier's device latency.  With target four and only two visible
    // intents, reaching it inside this explicit window proves max_delay was
    // the close cause rather than target=1 grace or a broad child timeout.
    deadline_hook.wait_until_reached();
    let time_deadline_elapsed = time_started.elapsed();
    let time_elapsed_cap = TIME_MAX_DELAY + TIME_DEADLINE_TOLERANCE;
    let time_deadline_bounded = time_deadline_elapsed >= TIME_MAX_DELAY
        && time_deadline_elapsed <= time_elapsed_cap;
    assert!(
        time_deadline_bounded,
        "incomplete cohort did not reach barrier at configured max_delay"
    );
    deadline_hook.release();
    drop(deadline_hook);
    tokio::time::timeout(Duration::from_secs(5), async {
        for task in time_tasks {
            task.await
                .expect("Group time-bound task panicked")
                .expect("Group time-bound append");
        }
    })
    .await
    .expect("Group time bound failed to complete");
    drop(incomplete_cohort);
    let time_after = time_engine.metrics().commit;
    let time_batches = time_after.batches - time_before.batches;
    let time_groups = time_after.groups - time_before.groups;
    let time_fsyncs = time_after.fsync.count - time_before.fsync.count;
    let group_time_bound_proven = time_causal_setup
        && time_admission_bounded
        && time_deadline_bounded
        && time_batches == TIME_INCOMPLETE_COHORT as u64
        && time_groups == 1
        && time_fsyncs == time_groups;
    assert!(
        group_time_bound_proven,
        "incomplete adaptive cohort did not close at configured max_delay"
    );
    (
        CasePass::new("group-byte-time-bounds", "boundedness"),
        group_byte_bound_proven,
        group_time_bound_proven,
    )
}

async fn case_zero_reservations_after_cancel_complete(
    root: &Path,
) -> (CasePass, usize, usize) {
    let engine = LogEngine::open_with(root, options(Durability::Process))
        .expect("open zero-reservations store");
    engine
        .append_batch(
            "zero-reservations",
            Version::NoStream,
            &[record("fault.zero", b"prime".to_vec())],
        )
        .await
        .expect("zero-reservations success path");
    assert_zero_reservations(&engine);

    let conflict = engine
        .append_batch(
            "zero-reservations",
            Version::NoStream,
            &[record("fault.zero", b"conflict".to_vec())],
        )
        .await;
    assert!(matches!(conflict, Err(AppendError::Conflict { .. })));
    assert_zero_reservations(&engine);

    let empty: [RecordToAppend; 0] = [];
    let empty_outcome = engine
        .append_batch("zero-reservations", Version::At(0), &empty)
        .await
        .expect("empty no-op at current version");
    assert_eq!(
        empty_outcome.version,
        Version::At(0),
        "empty no-op must return the unchanged current version"
    );
    assert_zero_reservations(&engine);

    let hook = engine.arm_test_hook(TestEngineHook::PwriteEio {
        after_successful_pwrites: 0,
    });
    let backend_error = engine
        .append_batch(
            "zero-reservations",
            Version::At(0),
            &[record("fault.zero", b"backend-error".to_vec())],
        )
        .await;
    hook.wait_until_reached();
    assert_backend_error(backend_error);
    drop(hook);
    assert_zero_reservations(&engine);

    let pause = engine.arm_test_hook(TestEngineHook::Pause {
        point:         TestEngineHookPoint::PostPublicationPreCompletion,
        after_reaches: 0,
    });
    let append_engine = engine.clone();
    let abandoned = tokio::spawn(async move {
        append_engine
            .append_batch(
                "zero-reservations",
                Version::At(0),
                &[record("fault.zero", b"abandoned".to_vec())],
            )
            .await
    });
    pause.wait_until_reached();
    abandoned.abort();
    assert!(
        abandoned
            .await
            .expect_err("abandoned receiver returned")
            .is_cancelled()
    );
    pause.release();
    drop(pause);
    engine
        .append_batch(
            "zero-reservations",
            Version::At(1),
            &[record("fault.zero", b"sentinel".to_vec())],
        )
        .await
        .expect("sentinel after abandoned receiver");
    let metrics = engine.metrics();
    assert_eq!(metrics.owner_intent_slots_in_use, 0);
    assert_eq!(metrics.owner_intent_bytes_in_use, 0);
    (
        CasePass::new("zero-reservations-after-cancel-complete", "boundedness"),
        metrics.owner_intent_slots_in_use,
        metrics.owner_intent_bytes_in_use,
    )
}

fn case_fault_hook_compiles_out_binary_identical() -> CasePass {
    assert_eq!(
        compile_out_authority::SCHEMA,
        "bn-2l3n-fault-compile-out-authority-v1"
    );
    assert_eq!(compile_out_authority::IDENTICAL, "true");
    assert!(lower_hex(compile_out_authority::PRISTINE_SHA256, 64));
    assert!(lower_hex(compile_out_authority::OVERLAY_RELEASE_SHA256, 64));
    assert!(lower_hex(compile_out_authority::SYMBOL_ABSENCE_SHA256, 64));
    assert_eq!(
        compile_out_authority::PRISTINE_SHA256,
        compile_out_authority::OVERLAY_RELEASE_SHA256,
        "release overlay binary differs from pristine binary"
    );
    CasePass::new("fault-hook-compiles-out-binary-identical", "harness")
}

async fn run_cases(root: &Path, worker_nonce: &str) -> FaultEvidence {
    let mut passes = Vec::with_capacity(CASE_SPECS.len());
    passes.push(
        case_cancel_before_admission(&root.join("case-01-cancel-before")).await,
    );
    passes.push(
        case_cancel_after_ownership(&root.join("case-02-cancel-after")).await,
    );
    passes.push(
        run_kill_case(
            &root.join("case-03-kill-pre-write"),
            KillPoint::PreWrite,
            worker_nonce,
        )
        .await,
    );
    passes.push(
        run_kill_case(
            &root.join("case-04-kill-partial"),
            KillPoint::PartialWrite,
            worker_nonce,
        )
        .await,
    );
    passes.push(
        run_kill_case(
            &root.join("case-05-kill-pre-barrier"),
            KillPoint::PostWritePreBarrier,
            worker_nonce,
        )
        .await,
    );
    passes.push(
        run_kill_case(
            &root.join("case-06-kill-post-barrier"),
            KillPoint::PostBarrierPrePublication,
            worker_nonce,
        )
        .await,
    );
    passes.push(
        run_kill_case(
            &root.join("case-07-kill-post-publication"),
            KillPoint::PostPublicationPreCompletion,
            worker_nonce,
        )
        .await,
    );
    passes.push(case_short_write(&root.join("case-08-short-write")).await);
    passes.push(case_write_error(&root.join("case-09-write-error")).await);
    passes.push(
        case_fdatasync_error(&root.join("case-10-fdatasync-error")).await,
    );
    passes
        .push(case_torn_truncated_tail(&root.join("case-11-torn-tail")).await);
    passes
        .push(case_invalid_marker_crc(&root.join("case-12-marker-crc")).await);
    passes.push(
        case_corrupt_registry_record(&root.join("case-13-registry")).await,
    );
    passes.push(
        case_refuted_corrupt_sidecar(&root.join("case-14-sidecar")).await,
    );
    passes.push(
        case_uncertain_persistence_poison(&root.join("case-15-poison")).await,
    );
    passes.push(
        case_acknowledged_group_survives_reopen(
            &root.join("case-16-group-reopen"),
        )
        .await,
    );
    let (owner_ring, owner_ring_intents) =
        case_owner_ring_intent_bound(&root.join("case-17-owner-ring")).await;
    passes.push(owner_ring);
    let (group_bounds, group_byte_bound_proven, group_time_bound_proven) =
        case_group_byte_time_bounds(&root.join("case-18-group-bounds")).await;
    passes.push(group_bounds);
    let (zero, waiter_reservations_after, byte_reservations_after) =
        case_zero_reservations_after_cancel_complete(
            &root.join("case-19-zero-reservations"),
        )
        .await;
    passes.push(zero);
    passes.push(case_fault_hook_compiles_out_binary_identical());

    assert_eq!(passes.len(), CASE_SPECS.len());
    for (observed, expected) in passes.iter().zip(CASE_SPECS) {
        assert_eq!(observed.spec, expected, "fault case order differs");
    }
    let boundedness = BoundednessEvidence {
        owner_ring_intents,
        group_byte_bound_proven,
        group_time_bound_proven,
        waiter_reservations_after,
        byte_reservations_after,
    };
    assert_eq!(
        boundedness,
        BoundednessEvidence {
            owner_ring_intents:        1_024,
            group_byte_bound_proven:   true,
            group_time_bound_proven:   true,
            waiter_reservations_after: 0,
            byte_reservations_after:   0,
        }
    );
    FaultEvidence { passes, boundedness }
}

fn cases_json(passes: &[CasePass]) -> String {
    let cases: Vec<String> = passes
        .iter()
        .map(|pass| {
            canonical_object(&[
                ("classification", json_string(pass.spec.classification)),
                ("id", json_string(pass.spec.id)),
                ("status", json_string("PASS")),
            ])
        })
        .collect();
    format!("[{}]", cases.join(","))
}

fn boundedness_json(bounds: BoundednessEvidence) -> String {
    canonical_object(&[
        (
            "byte_reservations_after",
            json_u64(bounds.byte_reservations_after as u64),
        ),
        ("group_byte_bound_proven", json_bool(bounds.group_byte_bound_proven)),
        ("group_time_bound_proven", json_bool(bounds.group_time_bound_proven)),
        ("owner_ring_intents", json_u64(bounds.owner_ring_intents as u64)),
        (
            "waiter_reservations_after",
            json_u64(bounds.waiter_reservations_after as u64),
        ),
    ])
}

fn emit_fault(args: &FaultArgs, evidence: &FaultEvidence) {
    println!(
        "{}",
        canonical_object(&[
            ("attempt_nonce", json_string(&args.attempt_nonce)),
            ("boundedness", boundedness_json(evidence.boundedness)),
            ("cases", cases_json(&evidence.passes)),
            ("harness_sound", json_bool(true)),
            ("phase", json_string(&args.phase)),
            ("protocol", json_string(contract::PROTOCOL)),
            ("schema", json_string("bn-2l3n-correctness-child-v3")),
            ("suite", json_string("current-fault")),
            ("variant", json_string(contract::VARIANT)),
        ])
    );
}

fn emit_smoke(evidence: &FaultEvidence) {
    println!(
        "{}",
        canonical_object(&[
            ("boundedness", boundedness_json(evidence.boundedness)),
            ("cases", cases_json(&evidence.passes)),
            ("harness_sound", json_bool(true)),
            ("protocol", json_string(contract::PROTOCOL)),
            ("schema", json_string("bn-2l3n-smoke-v3")),
            ("smoke_target", json_string("fault")),
            ("status", json_string("PASS")),
            ("variant", json_string(contract::VARIANT)),
        ])
    );
}

unsafe extern "C" {
    fn fcntl(fd: c_int, command: c_int, ...) -> c_int;
}

fn make_control_close_on_exec() {
    const F_GETFD: c_int = 1;
    const F_SETFD: c_int = 2;
    const FD_CLOEXEC: c_int = 1;
    let text = required("ASTERISM_REBASELINE_CONTROL_FD");
    let fd: c_int = text.parse().expect("invalid control fd");
    assert!(fd >= 3 && fd.to_string() == text, "noncanonical control fd");
    // SAFETY: fcntl accepts this live inherited descriptor and integer
    // commands.  CLOEXEC does not close it in this process; it only prevents
    // self-exec fault workers from retaining the runner's authority socket.
    let flags = unsafe { fcntl(fd, F_GETFD) };
    assert!(flags >= 0, "fcntl(F_GETFD) failed");
    let status = unsafe { fcntl(fd, F_SETFD, flags | FD_CLOEXEC) };
    assert_eq!(status, 0, "fcntl(F_SETFD, FD_CLOEXEC) failed");
}

fn main() {
    if let Some((point, store, ready, nonce)) = worker_args() {
        fault_worker(point, store, ready, nonce);
        return;
    }

    let invocation = invocation();
    let mode = match invocation {
        Invocation::Fault(_) => "fault",
        Invocation::Smoke => "smoke",
    };
    control::validate_perf_environment_mode(mode);
    control::authorize_ptracer_from_env();
    let root = PathBuf::from(required("ASTERISM_REBASELINE_STORE"));
    assert!(!root.exists(), "fault store root must be absent");

    let mut control = Control::connect();
    make_control_close_on_exec();
    let boot_nonce = control.boot();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_keep_alive(Duration::from_secs(3_600))
        .enable_all()
        .build()
        .expect("create fault runtime");
    let runtime_nonce = control.runtime(&boot_nonce);
    let control_engine = LogEngine::open_with(
        root.join("case-00-control-open"),
        options(Durability::group_default()),
    )
    .expect("open controlled fault engine");
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
    let worker_nonce = required("ASTERISM_REBASELINE_CONTEXT_SHA256");
    assert!(lower_hex(&worker_nonce, 64), "invalid worker authority nonce");
    let evidence = runtime.block_on(run_cases(&root, &worker_nonce));
    let last_completion_monotonic_ns = monotonic_ns();
    let t1_monotonic_ns = monotonic_ns();
    let alloc_after = allocation::snapshot();
    let cpu_after = cpu_snapshot();
    let counter_end_monotonic_ns = monotonic_ns();
    let release_monotonic_ns = monotonic_ns();
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
    drop(control_engine);
    runtime.shutdown_timeout(Duration::from_secs(30));
    match invocation {
        Invocation::Fault(args) => emit_fault(&args, &evidence),
        Invocation::Smoke => emit_smoke(&evidence),
    }
}
