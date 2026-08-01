//! Canonical Process-only owned-append admission harness for `bn-22it`.
//!
//! Control and candidate compile this exact source. The external hardened
//! runner owns row order, exclusivity, process lifecycle, and evidence; this
//! binary owns the public-facade workload, exact per-row counters, and the
//! compile-time source identity contract.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mess_core::{CodecError, Event};
use mess_log::committer::Durability;
use mess_store::{
    EngineOptions, EventStore, LogEngine, PackSnapshotBackend, Version,
};
use tokio::sync::Barrier;

const PROTOCOL: &str = "bn-22it-process-owned-v1";

// Ordinary workspace test discovery compiles examples without the sealed
// preparer's build identity. Keep that compile path available, but embed an
// invalid empty sentinel: every executable path calls validate_build_identity
// before emitting a contract, running work, or writing CSV, so an unattested
// binary still fails closed at runtime.
macro_rules! build_identity {
    ($name:literal) => {
        match option_env!($name) {
            Some(value) => value,
            None => "",
        }
    };
}

const BUILD_PROTOCOL: &str = build_identity!("OWNED_APPEND_BUILD_PROTOCOL");
const BUILD_BASELINE_SOURCE: &str =
    build_identity!("OWNED_APPEND_BUILD_BASELINE_SOURCE");
const BUILD_BASELINE_TREE: &str =
    build_identity!("OWNED_APPEND_BUILD_BASELINE_TREE");
const BUILD_SOURCE_COMMIT: &str =
    build_identity!("OWNED_APPEND_BUILD_SOURCE_COMMIT");
const BUILD_SOURCE_TREE: &str =
    build_identity!("OWNED_APPEND_BUILD_SOURCE_TREE");
const BUILD_HARNESS_SHA256: &str =
    build_identity!("OWNED_APPEND_BUILD_HARNESS_SHA256");
const BUILD_CARGO_LOCK_SHA256: &str =
    build_identity!("OWNED_APPEND_BUILD_CARGO_LOCK_SHA256");
const BUILD_SOURCE_APPROVAL_SHA256: &str =
    build_identity!("OWNED_APPEND_BUILD_SOURCE_APPROVAL_SHA256");
const BUILD_NONCE: &str = build_identity!("OWNED_APPEND_BUILD_BUILD_NONCE");

struct CountingAlloc;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(
        &self,
        ptr: *mut u8,
        layout: Layout,
        new_size: usize,
    ) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

#[derive(Debug, Clone)]
struct BenchEvent {
    payload: Vec<u8>,
}

impl Event for BenchEvent {
    fn name(&self) -> &'static str { "bench.event" }

    fn encode(&self) -> Result<Vec<u8>, CodecError> { Ok(self.payload.clone()) }

    fn decode(name: &str, bytes: &[u8]) -> Result<Self, CodecError> {
        if name != "bench.event" {
            return Err(CodecError::UnknownEventName(name.to_owned()));
        }
        Ok(Self { payload: bytes.to_vec() })
    }
}

#[derive(Debug, Clone, Copy)]
struct Workload {
    batch:   usize,
    writers: usize,
    bpw:     usize,
    payload: usize,
}

impl Workload {
    fn events(self) -> u64 { (self.batch * self.writers * self.bpw) as u64 }

    fn appends(self) -> u64 { (self.writers * self.bpw) as u64 }
}

#[derive(Debug, Clone, Copy)]
struct ResultRow {
    ev_s:                f64,
    p50_us:              f64,
    p99_us:              f64,
    allocs:              u64,
    alloc_bytes:         u64,
    owned_batches:       u64,
    owned_records:       u64,
    owned_payload_bytes: u64,
    borrowed_batches:    u64,
    borrowed_records:    u64,
    copied_records:      u64,
    copied_bytes:        u64,
    batches:             u64,
    groups:              u64,
    fsyncs:              u64,
    fsync_p99_ns:        u64,
    fsync_degraded:      bool,
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_build_identity() {
    assert_eq!(BUILD_PROTOCOL, PROTOCOL, "embedded protocol mismatch");
    for (label, value) in [
        ("baseline source", BUILD_BASELINE_SOURCE),
        ("baseline tree", BUILD_BASELINE_TREE),
        ("source commit", BUILD_SOURCE_COMMIT),
        ("source tree", BUILD_SOURCE_TREE),
    ] {
        assert!(is_lower_hex(value, 40), "invalid embedded {label}");
    }
    for (label, value) in [
        ("harness sha256", BUILD_HARNESS_SHA256),
        ("Cargo.lock sha256", BUILD_CARGO_LOCK_SHA256),
        ("source approval sha256", BUILD_SOURCE_APPROVAL_SHA256),
        ("build nonce", BUILD_NONCE),
    ] {
        assert!(is_lower_hex(value, 64), "invalid embedded {label}");
    }
}

fn emit_contract() {
    validate_build_identity();
    println!(
        concat!(
            "{{\"protocol\":\"{}\",",
            "\"baseline_source\":\"{}\",",
            "\"baseline_tree\":\"{}\",",
            "\"source_commit\":\"{}\",",
            "\"source_tree\":\"{}\",",
            "\"harness_sha256\":\"{}\",",
            "\"cargo_lock_sha256\":\"{}\",",
            "\"source_approval_sha256\":\"{}\",",
            "\"build_nonce\":\"{}\",",
            "\"contract_mode\":true,\"csv_written\":false}}"
        ),
        BUILD_PROTOCOL,
        BUILD_BASELINE_SOURCE,
        BUILD_BASELINE_TREE,
        BUILD_SOURCE_COMMIT,
        BUILD_SOURCE_TREE,
        BUILD_HARNESS_SHA256,
        BUILD_CARGO_LOCK_SHA256,
        BUILD_SOURCE_APPROVAL_SHA256,
        BUILD_NONCE,
    );
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    let i = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[i] as f64 / 1_000.0
}

fn scratch_dir() -> mess_testkit::SweepingTempDir {
    let root = std::env::var_os("MESS_BENCH_DIR").map_or_else(
        || {
            PathBuf::from(std::env::var("HOME").expect("HOME"))
                .join(".cache/mess-bench")
        },
        PathBuf::from,
    );
    std::fs::create_dir_all(&root).expect("create benchmark root");
    mess_testkit::temp_dir_in(&root, "owned-append")
}

fn run(wl: Workload) -> ResultRow {
    let scratch = scratch_dir();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(wl.writers.clamp(2, 8))
        .enable_all()
        .build()
        .expect("runtime");
    let engine = LogEngine::open_with(
        scratch.path().join("store"),
        EngineOptions {
            durability: Durability::Process,
            segment_size: 512 * 1024 * 1024,
            ..Default::default()
        },
    )
    .expect("open");
    let backend = PackSnapshotBackend::open(
        engine.clone(),
        scratch.path().join("snapshots"),
    )
    .expect("open production snapshot wrapper");
    let store = EventStore::new(backend);
    let batch = vec![BenchEvent { payload: vec![0xA5; wl.payload] }; wl.batch];

    // Warm the streams and event-type registry outside the allocation sample.
    let mut heads = vec![Version::NoStream; wl.writers];
    rt.block_on(async {
        for (writer, head) in heads.iter_mut().enumerate() {
            *head = store
                .append(
                    &format!("bench-{writer}"),
                    Version::NoStream,
                    &batch[..1],
                )
                .await
                .expect("warm append")
                .version;
        }
    });

    // Build task, stream, batch-clone, and latency-vector scaffolding before
    // the allocation snapshot. Both task populations then rendezvous at a
    // start gate, leaving the allocator delta focused on the public append
    // path rather than benchmark setup.
    let ready = Arc::new(Barrier::new(wl.writers + 1));
    let start = Arc::new(Barrier::new(wl.writers + 1));
    let joins = rt.block_on(async {
        let mut joins = Vec::with_capacity(wl.writers);
        for (writer, mut expected) in heads.into_iter().enumerate() {
            let store = store.clone();
            let batch = batch.clone();
            let ready = Arc::clone(&ready);
            let start = Arc::clone(&start);
            joins.push(tokio::spawn(async move {
                let stream = format!("bench-{writer}");
                let mut lats = Vec::with_capacity(wl.bpw);
                ready.wait().await;
                start.wait().await;
                for _ in 0..wl.bpw {
                    let t = Instant::now();
                    let commit = store
                        .append(&stream, expected, &batch)
                        .await
                        .expect("append");
                    lats.push(t.elapsed().as_nanos() as u64);
                    expected = commit.version;
                }
                lats
            }));
        }
        ready.wait().await;
        joins
    });

    let a0 = ALLOCS.load(Ordering::Relaxed);
    let b0 = ALLOC_BYTES.load(Ordering::Relaxed);
    let input0 = engine.append_input_metrics();
    let commit0 = engine.metrics().commit;
    let started = Instant::now();
    let mut lats = rt.block_on(async {
        start.wait().await;
        let mut all = Vec::with_capacity(wl.appends() as usize);
        for join in joins {
            all.extend(join.await.expect("writer task"));
        }
        all
    });
    let elapsed = started.elapsed();
    let allocs = ALLOCS.load(Ordering::Relaxed) - a0;
    let alloc_bytes = ALLOC_BYTES.load(Ordering::Relaxed) - b0;
    lats.sort_unstable();
    let commit1 = engine.metrics().commit;
    let input1 = engine.append_input_metrics();
    assert_eq!(
        engine.total_events(),
        wl.events() as usize + wl.writers + 1 + wl.writers,
        "domain events plus warm events plus stream/type registry records",
    );
    drop(store);
    drop(engine);
    rt.shutdown_timeout(Duration::from_secs(10));
    ResultRow {
        ev_s: wl.events() as f64 / elapsed.as_secs_f64(),
        p50_us: percentile(&lats, 0.50),
        p99_us: percentile(&lats, 0.99),
        allocs,
        alloc_bytes,
        owned_batches: input1.owned_batches - input0.owned_batches,
        owned_records: input1.owned_records - input0.owned_records,
        owned_payload_bytes: input1.owned_payload_bytes
            - input0.owned_payload_bytes,
        borrowed_batches: input1.borrowed_batches - input0.borrowed_batches,
        borrowed_records: input1.borrowed_records - input0.borrowed_records,
        copied_records: input1.copied_records - input0.copied_records,
        copied_bytes: input1.copied_bytes - input0.copied_bytes,
        batches: commit1.batches - commit0.batches,
        groups: commit1.groups - commit0.groups,
        fsyncs: commit1.fsync.count - commit0.fsync.count,
        fsync_p99_ns: commit1.fsync.p99_nanos,
        fsync_degraded: commit1.fsync_degraded,
    }
}

fn bpw(batch: usize) -> usize {
    match batch {
        1 => 40_000,
        10 => 12_500,
        100 => 2_500,
        1000 => 250,
        _ => panic!("OWNED_APPEND_BATCH must be 1, 10, 100, or 1000"),
    }
}

fn assert_variant_path(variant: &str, row: ResultRow, wl: Workload) {
    match variant {
        "candidate" => {
            assert_eq!(row.owned_batches, wl.appends());
            assert_eq!(row.owned_records, wl.events());
            assert_eq!(
                row.owned_payload_bytes,
                wl.events() * wl.payload as u64
            );
            assert_eq!(row.borrowed_batches, 0);
            assert_eq!(row.borrowed_records, 0);
            assert_eq!(row.copied_records, 0);
            assert_eq!(row.copied_bytes, 0);
        }
        "control" => {
            assert_eq!(row.owned_batches, 0);
            assert_eq!(row.owned_records, 0);
            assert_eq!(row.owned_payload_bytes, 0);
            assert_eq!(row.borrowed_batches, wl.appends());
            assert_eq!(row.borrowed_records, wl.events());
            // With the locked 250-byte payload, b1/b10 take the small-batch
            // clone path and b100/b1000 cross the 16-KiB preparation threshold.
            let copied_records = if wl.batch <= 10 { wl.events() } else { 0 };
            assert_eq!(row.copied_records, copied_records);
            assert_eq!(
                row.copied_bytes,
                copied_records * (wl.payload + "bench.event".len()) as u64
            );
        }
        _ => panic!("OWNED_APPEND_VARIANT must be control or candidate"),
    }
}

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"))
}

fn expected_variant(cycle: u8, slot: u8) -> &'static str {
    let control = if cycle % 2 == 1 {
        slot == 1 || slot == 4
    } else {
        slot == 2 || slot == 3
    };
    if control { "control" } else { "candidate" }
}

fn main() {
    if let Ok(mode) = std::env::var("OWNED_APPEND_CONTRACT_MODE") {
        assert_eq!(mode, PROTOCOL, "unsupported contract mode");
        emit_contract();
        return;
    }
    validate_build_identity();

    let variant = required("OWNED_APPEND_VARIANT");
    assert!(
        matches!(variant.as_str(), "control" | "candidate"),
        "OWNED_APPEND_VARIANT must be control or candidate"
    );
    let mode = required("OWNED_APPEND_MODE");
    assert_eq!(mode, "process", "bn-22it admits Process rows only");
    let batch: usize = required("OWNED_APPEND_BATCH")
        .parse()
        .expect("OWNED_APPEND_BATCH must be numeric");
    let cycle: u8 = required("OWNED_APPEND_CYCLE")
        .parse()
        .expect("OWNED_APPEND_CYCLE must be numeric");
    assert!((1..=5).contains(&cycle), "OWNED_APPEND_CYCLE must be 1..=5");
    let slot: u8 = required("OWNED_APPEND_SLOT")
        .parse()
        .expect("OWNED_APPEND_SLOT must be numeric");
    assert!((1..=4).contains(&slot), "OWNED_APPEND_SLOT must be 1..=4");
    assert_eq!(
        variant,
        expected_variant(cycle, slot),
        "variant does not match the frozen ABBA/BAAB schedule"
    );
    let source = required("OWNED_APPEND_SOURCE");
    assert_eq!(source, BUILD_SOURCE_COMMIT, "runtime source mismatch");
    let binary_sha = required("OWNED_APPEND_BINARY_SHA256");
    assert!(is_lower_hex(&binary_sha, 64), "invalid runtime binary SHA-256");
    let csv_path = PathBuf::from(required("OWNED_APPEND_CSV"));

    let wl = Workload { batch, writers: 4, bpw: bpw(batch), payload: 250 };
    let pre_load = load1();
    let row = run(wl);
    let post_load = load1();

    assert_variant_path(&variant, row, wl);
    assert_eq!(row.batches, wl.appends());
    assert_eq!(row.groups, 0);
    assert_eq!(row.fsyncs, 0);
    assert_eq!(row.fsync_p99_ns, 0);
    assert!(!row.fsync_degraded);

    let fresh = !csv_path.exists();
    let mut csv = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&csv_path)
        .expect("open CSV");
    if fresh {
        writeln!(
            csv,
            "variant,source,binary_sha256,cycle,slot,mode,batch,writers,bpw,\
             payload,events,ev_s,p50_us,p99_us,allocs,alloc_bytes,\
             allocs_per_event,alloc_bytes_per_event,owned_batches,\
             owned_records,owned_payload_bytes,borrowed_batches,\
             borrowed_records,copied_records,copied_bytes,batches,groups,\
             fsyncs,fsync_p99_ns,fsync_degraded,pre_load1,post_load1"
        )
        .expect("write CSV header");
    }
    writeln!(
        csv,
        "{variant},{source},{binary_sha},{cycle},{slot},{mode},{batch},{},{},\
         {},{},{:.0},{:.1},{:.1},{},{},{:.4},{:.2},{},{},{},{},{},{},{},{},{},\
         {},{},{},{pre_load},{post_load}",
        wl.writers,
        wl.bpw,
        wl.payload,
        wl.events(),
        row.ev_s,
        row.p50_us,
        row.p99_us,
        row.allocs,
        row.alloc_bytes,
        row.allocs as f64 / wl.events() as f64,
        row.alloc_bytes as f64 / wl.events() as f64,
        row.owned_batches,
        row.owned_records,
        row.owned_payload_bytes,
        row.borrowed_batches,
        row.borrowed_records,
        row.copied_records,
        row.copied_bytes,
        row.batches,
        row.groups,
        row.fsyncs,
        row.fsync_p99_ns,
        row.fsync_degraded,
    )
    .expect("write one CSV row");
    println!(
        "{variant} cycle={cycle} slot={slot} process batch={batch} ev/s={:.0} \
         p99={:.1}us alloc/event={:.4} bytes/event={:.1} copied={}/{} \
         groups/fsyncs={}/{} fsync_p99={}ns degraded={} \
         load={pre_load}->{post_load}",
        row.ev_s,
        row.p99_us,
        row.allocs as f64 / wl.events() as f64,
        row.alloc_bytes as f64 / wl.events() as f64,
        row.copied_records,
        row.copied_bytes,
        row.groups,
        row.fsyncs,
        row.fsync_p99_ns,
        row.fsync_degraded,
    );
}

fn load1() -> f64 {
    std::fs::read_to_string("/proc/loadavg")
        .expect("loadavg")
        .split_whitespace()
        .next()
        .expect("load1")
        .parse()
        .expect("numeric load1")
}
