//! bn-3l0 — the engine now emits the real on-disk fold chain (`crypto_chain`,
//! spec 05 §6) when opted in, and `mess verify --full` (bn-3h0) exercises it on
//! a REAL `LogEngine`-written store.
//!
//! Four properties are pinned here, all against the production `LogEngine`
//! append path (not `build_cert`'s in-memory construction):
//!
//! 1. **Chain off is byte-identical.** With `chain: false` (the default) the
//!    segment bytes carry no `crypto_chain` (flag bit 0 unset) and are
//!    byte-for-byte reproducible — turning the feature ON is the only thing
//!    that changes the on-disk bytes. This is the engine-level equivalent of
//!    the bn-1d0 golden byte-pinning.
//! 2. **Chain on materializes a real, verifiable chain.** Every batch carries
//!    `crypto_chain`; `mess verify --full` recomputes it green.
//! 3. **Crash-reopen continues the chain.** Write chained, drop the engine
//!    (crash), reopen (heads rehydrated from the recovered frames), append more
//!    — the whole stream's on-disk chain still verifies from genesis
//!    (`load_verified` over a cert reconstructed from the on-disk bytes), and
//!    `mess verify --full` is green across the reopen boundary.
//! 4. **A CRC-repaired tamper is caught.** Editing a committed payload at rest
//!    and repairing the batch CRC (both the header and the marker echo) sails
//!    past the structural scan and the CRC — but `mess verify --full` catches
//!    it as a `fold-chain-break` and forces a non-zero exit. This is the whole
//!    reason the on-disk chain exists.

use std::path::Path;

use mess_cli::store::log_path;
use mess_cli::verify::{self, VerifyOptions};
use mess_log::certificates::{
    Aggregate, BatchRec, FrameRec, StreamCert, load_verified,
};
use mess_log::crc::batch_crc;
use mess_log::format::{
    CHAIN_LEN, HEADER_CRC_OFF, HEADER_LEN, SUBFRAME_HDR_LEN,
};
use mess_log::runtime::real::RealFs;
use mess_log::scanner::{AcceptedBatch, recover_segment_with_image};
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EngineOptions, LogEngine, Version};

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

/// Engine options with a large single active segment (no rolls — the whole
/// stream stays in `seg-00000001.log`, so the fold-chain continuity check spans
/// the entire stream, including across a crash-reopen boundary).
fn opts(chain: bool) -> EngineOptions {
    EngineOptions { chain, ..EngineOptions::default() }
}

/// A deterministic payload for stream position `v` (no wall-clock).
fn payload(v: u64) -> Vec<u8> {
    let mut d = Vec::with_capacity(24);
    d.extend_from_slice(b"chain-evt-");
    d.extend_from_slice(&v.to_le_bytes());
    d.extend_from_slice(&(v.wrapping_mul(2654435761) & 0xFFFF).to_le_bytes());
    d
}

fn rec(v: u64) -> RecordToAppend {
    RecordToAppend { message_type: "Ev".to_string(), data: payload(v) }
}

/// Append `stream` in the given batch-size run, starting at version `start`.
/// Returns the new head version (one past the last appended).
async fn append_batches(
    engine: &LogEngine,
    stream: &str,
    start: u64,
    batch_sizes: &[u64],
) -> u64 {
    let mut expected =
        if start == 0 { Version::NoStream } else { Version::At(start - 1) };
    let mut v = start;
    for &n in batch_sizes {
        let recs: Vec<RecordToAppend> = (v..v + n).map(rec).collect();
        let out =
            engine.append_batch(stream, expected, &recs).await.expect("append");
        expected = out.version;
        v += n;
    }
    v
}

/// A tiny byte-sum aggregate — works over arbitrary payloads.
struct SumAgg {
    sum:   u64,
    count: u64,
}

impl Aggregate for SumAgg {
    const FOLD_VERSION: u32 = 1;

    fn init() -> Self { SumAgg { sum: 0, count: 0 } }

    fn apply(&mut self, payload: &[u8]) {
        self.sum = self
            .sum
            .wrapping_add(payload.iter().map(|&b| u64::from(b)).sum::<u64>());
        self.count += 1;
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut o = Vec::with_capacity(16);
        o.extend_from_slice(&self.sum.to_le_bytes());
        o.extend_from_slice(&self.count.to_le_bytes());
        o
    }

    fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.len() != 16 {
            return None;
        }
        Some(SumAgg {
            sum:   u64::from_le_bytes(b[0..8].try_into().ok()?),
            count: u64::from_le_bytes(b[8..16].try_into().ok()?),
        })
    }
}

/// Reconstruct a [`StreamCert`] for one stream **straight from the on-disk
/// segment bytes** of the active segment: read each accepted batch's stored
/// `crypto_chain` (offset [`HEADER_LEN`]) and its committed payloads, so
/// [`load_verified`] validates the real on-disk chain (not an in-memory
/// reconstruction). `head_anchor` is `None` — the per-batch continuity check is
/// the on-disk witness we want.
fn cert_from_active_segment(dir: &Path, stream_id: u64) -> StreamCert {
    let log = log_path(dir, 1);
    let (recovery, image) =
        recover_segment_with_image(&RealFs, &log).expect("recover segment");
    let mut batches: Vec<&AcceptedBatch> =
        recovery.accepted.iter().filter(|b| b.stream_id == stream_id).collect();
    batches.sort_by_key(|b| b.first_stream_version);

    let mut recs = Vec::new();
    for b in batches {
        assert!(
            b.has_crypto_chain,
            "chain-enabled stream must carry crypto_chain on disk"
        );
        let off = b.offset as usize;
        let crypto_chain = image
            [off + HEADER_LEN..off + HEADER_LEN + CHAIN_LEN]
            .try_into()
            .expect("chain slot is CHAIN_LEN bytes");
        let frames: Vec<FrameRec> = b
            .frames(&image)
            .expect("materialize frames")
            .enumerate()
            .map(|(k, f)| FrameRec {
                version: b.first_stream_version + k as u64,
                payload: f.payload.to_vec(),
            })
            .collect();
        recs.push(BatchRec {
            base_version: b.first_stream_version,
            crypto_chain,
            frames,
        });
    }
    StreamCert {
        stream_id,
        batches: recs,
        head_anchor: None,
        snapshot_anchors: Vec::new(),
    }
}

// ===========================================================================
// Overhead: chained vs unchained append throughput (bn-3l0 scope item 6).
// Run explicitly:  TMPDIR=$HOME/.cache/mess-test-tmp \
//   cargo test -p mess-cli --release --test chain_verify -- --ignored
// --nocapture chain_overhead
// ===========================================================================

#[tokio::test]
#[ignore = "throughput measurement: run explicitly with --release --nocapture"]
async fn chain_overhead_report() {
    const BATCHES: u64 = 2_000;
    const BATCH: u64 = 16; // events per batch
    let total = BATCHES * BATCH;

    let run = |chain: bool| async move {
        let dir = tempfile::tempdir().unwrap();
        let engine =
            LogEngine::open_with(dir.path(), opts(chain)).expect("open");
        let sizes = vec![BATCH; BATCHES as usize];
        let t0 = std::time::Instant::now();
        append_batches(&engine, "acct", 0, &sizes).await;
        let dt = t0.elapsed();
        drop(engine);
        dt
    };

    // Warm once (page-cache / allocator), then measure.
    let _ = run(false).await;
    let off = run(false).await;
    let on = run(true).await;

    let eps = |dt: std::time::Duration| total as f64 / dt.as_secs_f64();
    let off_eps = eps(off);
    let on_eps = eps(on);
    eprintln!(
        "chain overhead ({total} events, {BATCH}/batch, Process durability):"
    );
    eprintln!("  chain OFF: {off:?}  ->  {off_eps:.0} events/s");
    eprintln!("  chain ON : {on:?}  ->  {on_eps:.0} events/s");
    eprintln!(
        "  overhead : {:.2}% throughput reduction",
        (1.0 - on_eps / off_eps) * 100.0
    );
}

/// The interned stream id the engine assigned to the first stream it saw. The
/// interner is dense from 1 (see `Book::intern_stream`), so a store with a
/// single stream uses id 1.
const FIRST_STREAM_ID: u64 = 1;

// ===========================================================================
// 1. Chain OFF is byte-identical; chain ON changes the bytes (and only that).
// ===========================================================================

#[tokio::test]
async fn chain_off_is_byte_identical_and_chain_on_differs() {
    let batch_sizes = [3u64, 1, 2, 4, 1];

    // Two independent chain-OFF stores over the identical corpus.
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    for dir in [a.path(), b.path()] {
        let engine = LogEngine::open_with(dir, opts(false)).expect("open");
        append_batches(&engine, "acct", 0, &batch_sizes).await;
        drop(engine);
    }
    let bytes_a = std::fs::read(log_path(a.path(), 1)).expect("read seg a");
    let bytes_b = std::fs::read(log_path(b.path(), 1)).expect("read seg b");
    assert_eq!(
        bytes_a, bytes_b,
        "chain-off segment bytes must be byte-identical run-to-run"
    );

    // Every batch is flagless (no crypto_chain) with chain OFF.
    let (rec_off, _img) =
        recover_segment_with_image(&RealFs, &log_path(a.path(), 1)).unwrap();
    assert!(!rec_off.accepted.is_empty());
    assert!(
        rec_off.accepted.iter().all(|batch| !batch.has_crypto_chain),
        "chain-off store must not set flag bit 0 on any batch"
    );

    // The same corpus with chain ON: bytes differ, every batch carries the
    // chain, and verify --full is green over the real on-disk chain.
    let c = tempfile::tempdir().unwrap();
    let engine =
        LogEngine::open_with(c.path(), opts(true)).expect("open chained");
    append_batches(&engine, "acct", 0, &batch_sizes).await;
    drop(engine);

    let bytes_c = std::fs::read(log_path(c.path(), 1)).expect("read seg c");
    assert_ne!(
        bytes_a, bytes_c,
        "chain-on must change the on-disk bytes (the +32-byte slot)"
    );
    assert!(
        bytes_c.len() > bytes_a.len(),
        "chain-on segment is larger by the chain slots"
    );

    let (rec_on, _img) =
        recover_segment_with_image(&RealFs, &log_path(c.path(), 1)).unwrap();
    assert!(
        rec_on.accepted.iter().all(|batch| batch.has_crypto_chain),
        "chain-on store must set flag bit 0 on every batch"
    );

    let report = verify::run(
        c.path(),
        &VerifyOptions { full: true, ..Default::default() },
    );
    assert_eq!(
        report.exit_code(),
        0,
        "verify --full on honest chained store:\n{}",
        report.to_pretty()
    );
}

// ===========================================================================
// 3. Crash-reopen rehydrates the heads; an append after reopen continues the
//    chain; the whole stream verifies from genesis.
// ===========================================================================

#[tokio::test]
async fn reopen_continues_chain_and_verifies_whole_stream() {
    let dir = tempfile::tempdir().unwrap();

    // Write chained, then "crash" by dropping the engine.
    {
        let engine =
            LogEngine::open_with(dir.path(), opts(true)).expect("open");
        let head = append_batches(&engine, "acct", 0, &[3, 2, 4]).await;
        assert_eq!(head, 9);
        drop(engine);
    }

    // Reopen: recovery folds the 9 recovered frames back into the stream's
    // head. Append two more batches — they must continue the chain from h[8].
    {
        let engine =
            LogEngine::open_with(dir.path(), opts(true)).expect("reopen");
        let head = append_batches(&engine, "acct", 9, &[2, 3]).await;
        assert_eq!(head, 14);
        drop(engine);
    }

    // The whole on-disk chain (pre- + post-reopen batches) verifies from
    // genesis.
    let cert = cert_from_active_segment(dir.path(), FIRST_STREAM_ID);
    assert_eq!(cert.committed_count(), 14, "every event retained in the cert");
    let out = load_verified::<SumAgg>(&cert, None)
        .expect("load_verified green over whole stream");
    assert_eq!(out.state.count, 14, "verified aggregate saw every event");

    // And `mess verify --full` is green across the reopen boundary.
    let report = verify::run(
        dir.path(),
        &VerifyOptions { full: true, ..Default::default() },
    );
    assert_eq!(
        report.exit_code(),
        0,
        "verify --full must pass across the reopen boundary:\n{}",
        report.to_pretty()
    );
}

// ===========================================================================
// 4. A CRC-repaired payload tamper on a REAL engine store is caught by `mess
//    verify --full`.
// ===========================================================================

#[tokio::test]
async fn full_verify_catches_crc_repaired_tamper() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine =
            LogEngine::open_with(dir.path(), opts(true)).expect("open");
        // Six single-event batches so an interior batch has both a predecessor
        // (continuity check) and a successor.
        append_batches(&engine, "acct", 0, &[1, 1, 1, 1, 1, 1]).await;
        drop(engine);
    }

    let log = log_path(dir.path(), 1);
    let mut image = std::fs::read(&log).expect("read seg");

    // Locate an interior chain-enabled batch and flip a byte of its first
    // frame's payload, then repair the batch CRC in BOTH checksum fields so the
    // structural scan + CRC still accept it.
    let target = {
        let (recovery, _img) =
            recover_segment_with_image(&RealFs, &log).unwrap();
        let mut chained: Vec<AcceptedBatch> = recovery
            .accepted
            .into_iter()
            .filter(|b| b.has_crypto_chain)
            .collect();
        chained.sort_by_key(|b| b.first_stream_version);
        assert!(chained.len() >= 6, "expected 6 chained batches");
        chained[3]
    };
    let payload_off =
        target.offset as usize + HEADER_LEN + CHAIN_LEN + SUBFRAME_HDR_LEN;
    image[payload_off] ^= 0xFF;
    // Recompute the CRC over the tampered batch and write it into the header
    // `batch_crc` [68,72) and the trailing marker echo.
    let off = target.offset as usize;
    let end = off + target.total_len as usize;
    let crc = batch_crc(&image[off..end]);
    image[off + HEADER_CRC_OFF..off + HEADER_CRC_OFF + 4]
        .copy_from_slice(&crc.to_le_bytes());
    image[end - 4..end].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&log, &image).expect("write tampered seg");

    // The structural scan + CRC accept the tampered batch...
    let plain = verify::run(
        dir.path(),
        &VerifyOptions { full: false, ..Default::default() },
    );
    assert_eq!(
        plain.exit_code(),
        0,
        "structural scan cannot see a CRC-repaired tamper"
    );

    // ...but the fold chain catches it under --full.
    let full = verify::run(
        dir.path(),
        &VerifyOptions { full: true, ..Default::default() },
    );
    assert_ne!(
        full.exit_code(),
        0,
        "verify --full must catch the CRC-repaired tamper"
    );
    assert!(
        full.to_pretty().contains("fold-chain-break"),
        "expected a fold-chain-break finding, got:\n{}",
        full.to_pretty()
    );
}
