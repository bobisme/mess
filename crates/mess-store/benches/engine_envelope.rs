//! bn-20b Phase-4 exit-gate bench: the composed `mess-log` + `mess-index`
//! engine (`mess_store::LogEngine`) measured against the three envelope gates.
//!
//!   1. buffered append          >= 1_000_000 ev/s  (full engine append path)
//!   2. sealed stream replay     >= 2_500_000 ev/s  (EventStore load, sealed
//!      corpus)
//!   3. recovery fast path        <= 0.5 s          (recover_whole_log +
//!      manifest)
//!
//! Release + real ext4 scratch (`$HOME/.cache/...`, never /tmp tmpfs). Run:
//!   CLANG_PATH=/usr/bin/clang cargo bench -p mess-store --bench
//! engine_envelope

use std::path::PathBuf;
use std::time::Instant;

use mess_log::committer::Durability;
use mess_log::encode::Subframe;
use mess_log::manifest::{Manifest, build_manifest, decode_manifest};
use mess_log::recover_all::{
    RecoverOptions, SegmentFile, manifest_entries, recover_whole_log,
};
use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter};
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EngineOptions, EventStore, LogEngine, Version};

fn scratch_root() -> PathBuf {
    let home = std::env::var("HOME").expect("HOME");
    let root = PathBuf::from(home)
        .join(".cache")
        .join("mess-bn20b-bench")
        .join(format!("{}", std::process::id()));
    std::fs::create_dir_all(&root).expect("create scratch");
    root
}

fn rec(i: usize) -> RecordToAppend {
    RecordToAppend {
        message_type: "account.deposited".to_string(),
        data:         (i as u64).to_le_bytes().to_vec(),
    }
}

// ---------------------------------------------------------------------------
// Gate 1: buffered append through the full composed engine.
// ---------------------------------------------------------------------------
fn bench_buffered_append(root: &std::path::Path) -> f64 {
    const BATCHES: usize = 400;
    const PER_BATCH: usize = 5_000;
    const TOTAL: usize = BATCHES * PER_BATCH; // 2_000_000 events

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let engine = LogEngine::open_with(
        root.join("append"),
        EngineOptions {
            durability: Durability::Process,
            segment_size: 2 * 1024 * 1024 * 1024,
            ..Default::default()
        },
    )
    .expect("open");

    let batch: Vec<RecordToAppend> = (0..PER_BATCH).map(rec).collect();

    let started = Instant::now();
    rt.block_on(async {
        let mut expected = Version::NoStream;
        for _ in 0..BATCHES {
            let out = engine
                .append_batch("bench-stream", expected, &batch)
                .await
                .expect("append");
            expected = out.version;
        }
    });
    let elapsed = started.elapsed();
    let per_sec = TOTAL as f64 / elapsed.as_secs_f64();
    println!(
        "  gate 1 buffered append : {TOTAL} ev in {:?}  => {:.2} M ev/s",
        elapsed,
        per_sec / 1e6
    );
    per_sec
}

// ---------------------------------------------------------------------------
// Gate 2: sealed stream replay through EventStore load of a sealed corpus.
// ---------------------------------------------------------------------------
fn bench_sealed_replay(root: &std::path::Path) -> f64 {
    const BATCHES: usize = 400;
    const PER_BATCH: usize = 5_000;
    const TOTAL: usize = BATCHES * PER_BATCH; // 2_000_000 events

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let engine = LogEngine::open_with(
        root.join("sealed"),
        EngineOptions {
            durability: Durability::Process,
            segment_size: 2 * 1024 * 1024 * 1024,
            ..Default::default()
        },
    )
    .expect("open");

    let batch: Vec<RecordToAppend> = (0..PER_BATCH).map(rec).collect();
    rt.block_on(async {
        let mut expected = Version::NoStream;
        for _ in 0..BATCHES {
            let out = engine
                .append_batch("sealed-stream", expected, &batch)
                .await
                .expect("append");
            expected = out.version;
        }
    });

    // Seal the corpus: reads now route through the sealed ReplaySet cold path.
    engine.seal_active().expect("seal");

    let store = EventStore::new(engine).with_page_size(TOTAL + 1);

    let started = Instant::now();
    let records = rt.block_on(async {
        store
            .backend()
            .read_stream("sealed-stream", Version::NoStream, TOTAL + 1)
            .await
            .expect("read sealed")
    });
    let elapsed = started.elapsed();
    assert_eq!(records.len(), TOTAL, "sealed replay returned wrong count");
    let per_sec = TOTAL as f64 / elapsed.as_secs_f64();
    println!(
        "  gate 2 sealed replay   : {TOTAL} ev in {:?}  => {:.2} M ev/s",
        elapsed,
        per_sec / 1e6
    );
    per_sec
}

// ---------------------------------------------------------------------------
// Gate 3: recovery fast path on a realistic sealed corpus (recover_all +
// manifest). Pure mess-log.
// ---------------------------------------------------------------------------
fn bench_recovery(root: &std::path::Path) -> f64 {
    const SEGMENTS: u64 = 32;
    const BATCHES_PER_SEG: usize = 200;
    const EVENTS_PER_BATCH: u32 = 500; // 32 * 200 * 500 = 3.2M events

    let dir = root.join("recover");
    std::fs::create_dir_all(&dir).expect("mkdir");
    let rt = RealRuntime::new();
    let fs = rt.fs();

    let seg_path = |id: u64| dir.join(format!("seg-{id:08}.log"));
    let payload = vec![0u8; 24];
    let subframes: Vec<Subframe> = (0..EVENTS_PER_BATCH)
        .map(|_| Subframe::plain(1, 0, 0, &payload))
        .collect();

    let mut segs: Vec<SegmentFile> = Vec::new();
    let epoch0 = 1u64;
    let mut w = SegmentWriter::create(
        &fs,
        &seg_path(1),
        SegmentParams::new(1, 0, epoch0, 0),
    )
    .expect("create seg 1");
    segs.push(SegmentFile::new(1, seg_path(1)));

    let append_seg = |w: &mut SegmentWriter<_>, stream: u64| {
        let mut version = 0u64;
        for _ in 0..BATCHES_PER_SEG {
            let spec = BatchSpec {
                stream_id:            stream,
                category_id:          0,
                first_stream_version: version,
                crypto_chain:         None,
                subframes:            &subframes,
            };
            w.append(&spec).expect("append batch");
            version += u64::from(EVENTS_PER_BATCH);
        }
    };
    append_seg(&mut w, 1);
    for id in 2..=SEGMENTS {
        w = w
            .roll_sealed(&seg_path(id), id, epoch0 + id - 1, 0)
            .expect("roll_sealed");
        segs.push(SegmentFile::new(id, seg_path(id)));
        append_seg(&mut w, id);
    }
    w.seal().expect("seal tail");

    let total_events =
        SEGMENTS as usize * BATCHES_PER_SEG * EVENTS_PER_BATCH as usize;

    let whole = recover_whole_log(&fs, &segs, None, RecoverOptions::full())
        .expect("full recover");
    let manifest = Manifest::new(manifest_entries(&whole));
    let manifest_bytes = build_manifest(manifest.entries());
    let manifest =
        decode_manifest(&manifest_bytes).expect("manifest roundtrip");

    let started = Instant::now();
    let recovered =
        recover_whole_log(&fs, &segs, Some(&manifest), RecoverOptions::fast())
            .expect("fast recover");
    let elapsed = started.elapsed();
    assert_eq!(recovered.total_events as usize, total_events);
    println!(
        "  gate 3 recovery fast   : {SEGMENTS} segs / {total_events} ev \
         recovered in {:?}",
        elapsed
    );
    elapsed.as_secs_f64()
}

fn main() {
    let root = scratch_root();
    println!("bn-20b engine envelope bench (scratch: {})", root.display());

    let append = bench_buffered_append(&root);
    let replay = bench_sealed_replay(&root);
    let recover = bench_recovery(&root);

    let _ = std::fs::remove_dir_all(&root);

    println!("\n  gate results vs envelope:");
    let g1 = append >= 1_000_000.0;
    let g2 = replay >= 2_500_000.0;
    let g3 = recover <= 0.5;
    println!(
        "    buffered append  {:>8.2} M ev/s   (>= 1.00)   {}",
        append / 1e6,
        if g1 { "PASS" } else { "FAIL" }
    );
    println!(
        "    sealed replay    {:>8.2} M ev/s   (>= 2.50)   {}",
        replay / 1e6,
        if g2 { "PASS" } else { "FAIL" }
    );
    println!(
        "    recovery fast    {:>8.3} s        (<= 0.50)   {}",
        recover,
        if g3 { "PASS" } else { "FAIL" }
    );
    if !(g1 && g2 && g3) {
        std::process::exit(1);
    }
}
