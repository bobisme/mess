//! bn-3h0 acceptance: `mess verify --full` must catch a CRC-repaired
//! fold-chain tamper.
//!
//! Mirrors the construction in `mess-log/tests/crash_verify.rs` (Case D): a
//! chained corpus is written with the real [`SegmentWriter`]/[`ChainHead`]
//! machinery (real per-batch `crypto_chain`, spec 05 §6.2), then a payload
//! byte inside a non-final batch is tampered and the batch's `batch_crc` is
//! recomputed into BOTH the header field and the marker echo — the adversary
//! who edits a payload at rest and repairs the CRC. The structural scan
//! (`verify` without `--full`) has nothing left to catch: the CRC is valid
//! again. Only `--full`'s fold-chain recompute (spec 05 §3, §6) sees the
//! divergence.

use std::path::Path;

use mess_cli::report::Severity;
use mess_cli::store;
use mess_cli::verify::{self, VerifyOptions};
use mess_log::crc::batch_crc;
use mess_log::encode::Subframe;
use mess_log::fold_chain::{ChainHead, Hash};
use mess_log::format::{
    CHAIN_LEN, HEADER_CRC_OFF, HEADER_LEN, SUBFRAME_HDR_LEN,
};
use mess_log::runtime::real::RealFs;
use mess_log::scanner::{AcceptedBatch, scan_image};
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter};

const SEG_ID: u64 = 1;
const STREAM_ID: u64 = 42;

fn tmp() -> tempfile::TempDir { tempfile::tempdir().expect("tempdir") }

/// A 32-byte payload: tag byte + u64 amount + filler (mirrors crash_verify.rs).
fn ev(tag: u8, amount: u64) -> Vec<u8> {
    let mut p = vec![0u8; 32];
    p[0] = tag;
    p[1..9].copy_from_slice(&amount.to_le_bytes());
    p
}

fn workload(n: u64) -> Vec<Vec<u8>> {
    (0..n).map(|i| ev((i % 3 == 2) as u8, 10 + i)).collect()
}

/// Write `payloads` as chained batches of `batch_size` (real `SegmentWriter`,
/// real `crypto_chain`) to `dir`'s active segment, over the real fs.
fn write_chained_corpus(dir: &Path, payloads: &[Vec<u8>], batch_size: usize) {
    let path = store::log_path(dir, SEG_ID);
    let mut w = SegmentWriter::create(
        &RealFs,
        &path,
        SegmentParams::new(SEG_ID, 0, 1, 0),
    )
    .unwrap();

    let mut head = ChainHead::genesis(STREAM_ID);
    let mut version = 0u64;
    for chunk in payloads.chunks(batch_size) {
        let entry: Hash = head.entry();
        let subs: Vec<Subframe> =
            chunk.iter().map(|p| Subframe::plain(1, 0, 0, p)).collect();
        w.append(&BatchSpec {
            stream_id:            STREAM_ID,
            category_id:          0,
            first_stream_version: version,
            crypto_chain:         Some(&entry),
            subframes:            &subs,
        })
        .unwrap();
        for p in chunk {
            head.absorb(p);
            version += 1;
        }
    }
    w.sync().unwrap();
    w.close().unwrap();
}

/// Recompute a batch's CRC over its (tampered) bytes and write it into BOTH
/// checksum fields — the header `batch_crc` and the marker echo — so the
/// scanner accepts the tampered batch (mirrors crash_verify.rs).
fn refresh_batch_crc(image: &mut [u8], ab: &AcceptedBatch) {
    let off = ab.offset as usize;
    let end = off + ab.total_len as usize;
    let crc = batch_crc(&image[off..end]);
    image[off + HEADER_CRC_OFF..off + HEADER_CRC_OFF + 4]
        .copy_from_slice(&crc.to_le_bytes());
    image[end - 4..end].copy_from_slice(&crc.to_le_bytes());
}

/// Tamper one payload byte in the batch at `batch_index` (0-based, commit
/// order) and repair its CRC in place, on disk.
fn tamper_batch_payload_crc_repaired(dir: &Path, batch_index: usize) {
    let path = store::log_path(dir, SEG_ID);
    let mut image = std::fs::read(&path).expect("read log");
    let rec = scan_image(&image, None);
    let target = rec.accepted[batch_index];
    assert!(target.has_crypto_chain, "target batch must carry crypto_chain");

    let first_payload_off =
        target.offset as usize + HEADER_LEN + CHAIN_LEN + SUBFRAME_HDR_LEN;
    image[first_payload_off] ^= 0xFF;
    refresh_batch_crc(&mut image, &target);

    std::fs::write(&path, &image).expect("write tampered log");
}

/// A clean chained corpus verifies clean under `--full`: no error findings,
/// and an explicit `fold-chain-verified` finding proves the chain was
/// actually recomputed (not silently skipped).
#[test]
fn clean_chained_corpus_verifies_full_clean() {
    let d = tmp();
    write_chained_corpus(d.path(), &workload(50), 10);

    let report =
        verify::run(d.path(), &VerifyOptions { full: true, repair: false });
    assert_eq!(
        report.exit_code(),
        0,
        "clean chained corpus must exit 0: {:#?}",
        report.findings
    );
    assert!(
        !report.findings.iter().any(|f| f.severity == Severity::Error),
        "clean chained corpus must have no error findings: {:#?}",
        report.findings
    );
    assert!(
        report.findings.iter().any(|f| f.kind == "fold-chain-verified"),
        "expected a fold-chain-verified finding, got: {:#?}",
        report.findings
    );
}

/// The core acceptance case: a CRC-repaired tail-batch payload tamper (batch
/// index 3 of 5, mirroring crash_verify.rs Case D). Structural verify (no
/// `--full`) is CRC-clean and exits 0; `--full` recomputes the fold chain and
/// exits non-zero with a `fold-chain-break` finding.
#[test]
fn crc_repaired_tamper_passes_structural_but_fails_full() {
    let d = tmp();
    write_chained_corpus(d.path(), &workload(50), 10);
    tamper_batch_payload_crc_repaired(d.path(), 3);

    // Structural (no --full): the batch CRC and marker are self-consistent
    // again, so the byte-layer scan sees nothing wrong.
    let structural =
        verify::run(d.path(), &VerifyOptions { full: false, repair: false });
    assert_eq!(
        structural.exit_code(),
        0,
        "CRC-repaired tamper must be invisible to structural verify: {:#?}",
        structural.findings
    );
    assert!(
        !structural.findings.iter().any(|f| f.severity == Severity::Error),
        "structural verify must have no error findings on a CRC-repaired \
         tamper: {:#?}",
        structural.findings
    );

    // --full: the fold chain catches what the CRC could not.
    let full =
        verify::run(d.path(), &VerifyOptions { full: true, repair: false });
    assert_ne!(
        full.exit_code(),
        0,
        "fold-chain tamper must exit non-zero under --full"
    );
    assert!(
        full.findings
            .iter()
            .any(|f| f.severity == Severity::Error
                && f.kind == "fold-chain-break"),
        "expected a fold-chain-break finding under --full, got: {:#?}",
        full.findings
    );
}
