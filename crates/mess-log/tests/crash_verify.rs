//! bn-1l6 — Phase 5 exit-gate: `load_verified` round-trips **through the real
//! crash-recovery path**.
//!
//! Every other fold-certificate test (`fold_cert_attacks`, the `certificates`
//! unit suite) builds its `StreamCert` in memory from a payload list. This
//! suite instead drives the **production** append + recovery machinery — real
//! [`BatchEncoder`]/[`SegmentWriter`] bytes carrying the real per-batch
//! `crypto_chain` (§6.2), crashed by truncation/tamper, then recovered by the
//! production [`scan_image`] scanner — and reconstructs the verifier's
//! `StreamCert` from the **recovered on-disk prefix**. Only then does it run
//! [`load_verified`]. This proves the D4 certificate and the D1 recovery
//! contract compose: after a crash, verified load over the recovered prefix
//! behaves exactly as the spec (§7) requires.
//!
//! The durable head anchor `A(S)` (§5, Tier 1) is captured **before** the crash
//! — it stands in for the sealed-footer `StreamHeadTable` that lives outside
//! the truncatable tail. That is what lets Case C detect truncation the
//! recovered (self-consistent, shortened) log cannot detect on its own (§5.1).
//!
//! Cases:
//!   A  honest full recovery, batch-boundary snapshot  -> ok  (Path A + Path B)
//!   B  honest full recovery, mid-batch snapshot        -> ok  (first-partial)
//!   C  torn tail (truncation) + trusted full anchor    -> HeadMismatch
//!   D  tail payload tamper with a RECOMPUTED CRC (the
//!      scanner accepts it; the fold chain must not)     -> ChainBreakPrev /
//! HeadMismatch   E  prefix frame-v payload tamper (Path A operand)   ->
//! PrefixHashMismatch{FromFrameV}   F  both certification frames compacted +
//! Path-C      retention anchor                                 -> ok via Path
//! C, then reject on tamper

use std::path::Path;

use mess_log::certificates::{
    Aggregate, BatchRec, FrameRec, StreamCert, VerifyError, VerifyPath,
    build_cert, load_verified, take_snapshot,
};
use mess_log::crc::batch_crc;
use mess_log::encode::Subframe;
use mess_log::fold_chain::ChainHead;
use mess_log::fold_chain::Hash;
use mess_log::footer_ext::SnapshotAnchor;
use mess_log::format::{CHAIN_LEN, HEADER_CRC_OFF, HEADER_LEN};
use mess_log::runtime::{Fault, FileHandle, Fs, OpenOpts, SimFs};
use mess_log::scanner::{AcceptedBatch, scan_image};
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter};

// --- Reference aggregate (toy bank account, mirrors the fold_cert suite) -----

#[derive(Clone, Debug, PartialEq, Eq)]
struct Account {
    balance:  i64,
    tx_count: u64,
}
impl Aggregate for Account {
    const FOLD_VERSION: u32 = 1;

    fn init() -> Self { Account { balance: 0, tx_count: 0 } }

    fn apply(&mut self, payload: &[u8]) {
        let tag = payload[0];
        let amount = u64::from_le_bytes(payload[1..9].try_into().unwrap());
        match tag {
            0 => self.balance += amount as i64,
            1 => self.balance -= amount as i64,
            _ => {}
        }
        self.tx_count += 1;
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut o = Vec::with_capacity(16);
        o.extend_from_slice(&self.balance.to_le_bytes());
        o.extend_from_slice(&self.tx_count.to_le_bytes());
        o
    }

    fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.len() != 16 {
            return None;
        }
        Some(Account {
            balance:  i64::from_le_bytes(b[0..8].try_into().ok()?),
            tx_count: u64::from_le_bytes(b[8..16].try_into().ok()?),
        })
    }
}

/// A 32-byte payload: tag byte + u64 amount + filler.
fn ev(tag: u8, amount: u64) -> Vec<u8> {
    let mut p = vec![0u8; 32];
    p[0] = tag;
    p[1..9].copy_from_slice(&amount.to_le_bytes());
    p
}

fn workload(n: u64) -> Vec<Vec<u8>> {
    (0..n).map(|i| ev((i % 3 == 2) as u8, 10 + i)).collect()
}

const STREAM_ID: u64 = 42;
const SEG_PATH: &str = "log/seg-0000";

/// Write `payloads` as chained batches of `batch_size` (real SegmentWriter,
/// real `crypto_chain`) into a fresh SimFs, then read back the durable image
/// through the production scanner's read path. Returns the on-disk image.
fn write_chained_log(payloads: &[Vec<u8>], batch_size: usize) -> Vec<u8> {
    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new(SEG_PATH);
    let mut w =
        SegmentWriter::create(&fs, path, SegmentParams::new(1, 0, 7, 0))
            .unwrap();

    // The append-side running chain: each batch's crypto_chain is the head
    // ENTERING that batch (h[base-1]); the first batch's is the genesis.
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

    // Read the whole durable image back through the Fs (what the scanner
    // reads).
    let f = fs.open(path, OpenOpts::read_only()).unwrap();
    let len = f.len().unwrap() as usize;
    let mut buf = vec![0u8; len];
    let mut filled = 0;
    while filled < len {
        let n = f.pread(filled as u64, &mut buf[filled..]).unwrap();
        if n == 0 {
            break;
        }
        filled += n;
    }
    buf.truncate(filled);
    buf
}

/// Reconstruct the verifier's `StreamCert` from a recovered segment image using
/// ONLY the production scanner. Each accepted batch's stored `crypto_chain` is
/// read from its on-disk offset (§6.2) and its payloads from the frame iterator
/// (`AcceptedBatch::frames`). `anchor` is the durable Tier-1 head anchor (§5),
/// supplied out of band (it does not live in the truncatable tail).
fn cert_from_recovered(
    image: &[u8],
    anchor: Option<(u64, Hash)>,
    snapshot_anchors: Vec<SnapshotAnchor>,
) -> StreamCert {
    let rec = scan_image(image, None);
    let mut batches = Vec::new();
    for ab in &rec.accepted {
        if ab.stream_id != STREAM_ID {
            continue;
        }
        batches.push(recovered_batch(ab, image));
    }
    StreamCert {
        stream_id: STREAM_ID,
        batches,
        head_anchor: anchor,
        snapshot_anchors,
    }
}

fn recovered_batch(ab: &AcceptedBatch, image: &[u8]) -> BatchRec {
    assert!(
        ab.has_crypto_chain,
        "chain-enabled stream must carry crypto_chain"
    );
    let off = ab.offset as usize;
    let crypto_chain: Hash = image
        [off + HEADER_LEN..off + HEADER_LEN + CHAIN_LEN]
        .try_into()
        .unwrap();
    let mut frames = Vec::new();
    for (k, fr) in ab.frames(image).unwrap().enumerate() {
        frames.push(FrameRec {
            version: ab.first_stream_version + k as u64,
            payload: fr.payload.to_vec(),
        });
    }
    BatchRec { base_version: ab.first_stream_version, crypto_chain, frames }
}

/// Recompute a batch's CRC over its (possibly tampered) bytes and write it into
/// BOTH checksum fields — the header `batch_crc` [68,72) and the marker echo
/// [total_len-4, total_len) — so the production scanner accepts the tampered
/// batch. This is the adversary who edits a payload at rest AND fixes the CRC:
/// exactly what the fold chain (not the CRC) exists to catch (spec §1.2).
fn refresh_batch_crc(image: &mut [u8], ab: &AcceptedBatch) {
    let off = ab.offset as usize;
    let end = off + ab.total_len as usize;
    let crc = batch_crc(&image[off..end]);
    image[off + HEADER_CRC_OFF..off + HEADER_CRC_OFF + 4]
        .copy_from_slice(&crc.to_le_bytes());
    image[end - 4..end].copy_from_slice(&crc.to_le_bytes());
}

// ===========================================================================
// Case A/B — honest verified load over the recovered prefix.
// ===========================================================================

#[test]
fn recovered_prefix_verifies_at_batch_boundary() {
    let payloads = workload(50);
    let honest = build_cert(STREAM_ID, &payloads, 10, None);
    let image = write_chained_log(&payloads, 10);
    let cert = cert_from_recovered(&image, honest.head_anchor, Vec::new());

    // The recovered on-disk chain is byte-for-byte the in-memory reference:
    // proves the real crypto_chain round-trips through append + scanner.
    assert_eq!(
        cert.batches, honest.batches,
        "recovered chain != reference chain"
    );
    assert_eq!(cert.committed_count(), 50);

    let (r, blob) = take_snapshot::<Account>(&cert, 19); // batch boundary
    let out = load_verified::<Account>(&cert, Some((&r, &blob))).unwrap();
    assert!(!out.rebuilt_by_replay);
    assert_eq!(out.tail_len, 30);
    assert!(out.paths_used.contains(&VerifyPath::FromFrameV));
    assert!(out.paths_used.contains(&VerifyPath::FromFrameVPlus1));
    // State equals a full verified replay of the recovered log.
    let full = load_verified::<Account>(&cert, None).unwrap();
    assert_eq!(out.state, full.state);
}

#[test]
fn recovered_prefix_verifies_mid_batch() {
    let payloads = workload(50);
    let honest = build_cert(STREAM_ID, &payloads, 10, None);
    let image = write_chained_log(&payloads, 10);
    let cert = cert_from_recovered(&image, honest.head_anchor, Vec::new());

    let (r, blob) = take_snapshot::<Account>(&cert, 25); // mid-batch [20..=29]
    let out = load_verified::<Account>(&cert, Some((&r, &blob))).unwrap();
    assert!(!out.rebuilt_by_replay);
    assert_eq!(out.tail_len, 24); // frames 26..=49
    let full = load_verified::<Account>(&cert, None).unwrap();
    assert_eq!(out.state, full.state);
}

// ===========================================================================
// Case C — truncation. The tail is torn off after the crash; the trusted head
// anchor (captured pre-crash, living outside the tail) exposes the loss.
// ===========================================================================

#[test]
fn truncated_tail_is_caught_by_the_durable_head_anchor() {
    let payloads = workload(50);
    // Trusted anchor for the FULL stream (as a sealed footer would hold, §5).
    let full = build_cert(STREAM_ID, &payloads, 10, None);
    let trusted_anchor = full.head_anchor; // (49, h[49])

    let image = write_chained_log(&payloads, 10);

    // Crash: tear the tail so only the first 3 batches (30 events) survive.
    // Find the 4th batch's offset via the scanner and truncate there.
    let rec = scan_image(&image, None);
    assert_eq!(rec.accepted.len(), 5);
    let cut = rec.accepted[3].offset as usize;
    let torn = &image[..cut];

    // The recovered prefix self-certifies as a valid 30-event log; only the
    // out-of-band trusted anchor knows the true head is 49.
    let recovered = scan_image(torn, None);
    assert_eq!(recovered.accepted.len(), 3);
    let cert = cert_from_recovered(torn, trusted_anchor, Vec::new());
    assert_eq!(cert.last_retained_version(), Some(29));

    // Snapshot at v=19 (retained). Prefix + tail replay succeed over the
    // recovered frames, but the final chain value is h[29] != trusted h[49].
    let (r, blob) = take_snapshot::<Account>(&cert, 19);
    let err = load_verified::<Account>(&cert, Some((&r, &blob))).unwrap_err();
    assert!(
        matches!(err, VerifyError::HeadMismatch { .. }),
        "truncation must surface as HeadMismatch, got {err:?}"
    );

    // Sanity: with the anchor that matches the truncated prefix (the self-
    // certifying, undetectable case), the same load succeeds — proving the
    // anchor is the sole truncation witness.
    let self_cert = cert_from_recovered(
        torn,
        recovered_anchor(&payloads[..30]),
        Vec::new(),
    );
    let out = load_verified::<Account>(&self_cert, Some((&r, &blob))).unwrap();
    assert!(!out.rebuilt_by_replay);
    assert_eq!(out.tail_len, 10);
}

/// The honest head anchor for a prefix (what a sealer would have frozen at the
/// point the prefix was the whole committed log).
fn recovered_anchor(prefix: &[Vec<u8>]) -> Option<(u64, Hash)> {
    build_cert(STREAM_ID, prefix, 10, None).head_anchor
}

// ===========================================================================
// Case D — a tail payload tamper WITH a repaired CRC. The scanner (CRC-only)
// accepts it; the fold chain must reject it (the whole reason Phase 5 exists).
// ===========================================================================

#[test]
fn crc_valid_tail_tamper_is_caught_by_the_chain() {
    let payloads = workload(50);
    let honest = build_cert(STREAM_ID, &payloads, 10, None);
    let mut image = write_chained_log(&payloads, 10);

    // Tamper a payload byte in batch 3 (versions 30..=39), then REPAIR the CRC
    // so the scanner cannot tell. Locate the batch and its first frame payload.
    let rec = scan_image(&image, None);
    let target = rec.accepted[3];
    let first_payload_off = target.offset as usize
        + HEADER_LEN
        + CHAIN_LEN
        + mess_log::format::SUBFRAME_HDR_LEN;
    image[first_payload_off] ^= 0xFF;
    refresh_batch_crc(&mut image, &target);

    // The scanner still accepts every batch (CRC is valid again).
    let rec2 = scan_image(&image, None);
    assert_eq!(
        rec2.accepted.len(),
        5,
        "repaired CRC must keep the scanner happy"
    );

    let cert = cert_from_recovered(&image, honest.head_anchor, Vec::new());

    // Snapshot at v=19 (before the tampered batch). Tail replay walks the
    // tampered batch 3; the divergence surfaces at batch 4's crypto_chain
    // boundary (batch-granular, §7.0) as ChainBreakPrev, or at the head anchor.
    let (r, blob) = take_snapshot::<Account>(&cert, 19);
    let err = load_verified::<Account>(&cert, Some((&r, &blob))).unwrap_err();
    match err {
        VerifyError::ChainBreakPrev { at_version } => {
            assert_eq!(at_version, 40)
        }
        VerifyError::HeadMismatch { .. } => {}
        other => panic!(
            "CRC-valid tamper must be caught by the chain, got {other:?}"
        ),
    }
}

// ===========================================================================
// Case E — a prefix frame-v payload tamper (Path A's operand). Post-recovery
// the verifier's untrusted view has frame v rewritten; Path A recomputes h[v]
// from the payload and rejects.
// ===========================================================================

#[test]
fn prefix_frame_tamper_fails_path_a() {
    let payloads = workload(50);
    let honest = build_cert(STREAM_ID, &payloads, 10, None);
    let image = write_chained_log(&payloads, 10);
    let mut cert = cert_from_recovered(&image, honest.head_anchor, Vec::new());

    // Honest snapshot at v=25, then rewrite frame 25's payload in the recovered
    // view (batch [20..=29], index 5).
    let (r, blob) = take_snapshot::<Account>(&cert, 25);
    let b = cert.batches.iter_mut().find(|b| b.base_version == 20).unwrap();
    b.frames[5].payload[0] ^= 0xFF;

    let err = load_verified::<Account>(&cert, Some((&r, &blob))).unwrap_err();
    assert_eq!(
        err,
        VerifyError::PrefixHashMismatch { path: VerifyPath::FromFrameV }
    );
}

// ===========================================================================
// Case F — the empty-tail retention case (§7.1 Path C / §8.2). The snapshot is
// at head, and both certification frames (frame v's payload, frame v+1 which
// does not exist) have been compacted away. Only the durable footer
// `SnapshotAnchor` (Path C) can certify the prefix; the head anchor closes it.
// ===========================================================================

#[test]
fn path_c_retention_anchor_over_recovered_prefix() {
    let payloads = workload(50);
    let recovered = cert_from_recovered(
        &write_chained_log(&payloads, 10),
        None,
        Vec::new(),
    );
    let full = build_cert(STREAM_ID, &payloads, 10, None);

    // Snapshot at v=49 == head: h[49] is the reference event_prefix_hash, and
    // the tail is empty. (Taken over the recovered log to prove the value the
    // footer would have frozen matches the recovered chain.)
    let (r, blob) = take_snapshot::<Account>(&recovered, 49);
    let h_v = r.event_prefix_hash;

    // Full compaction: no frames retained at all. Path A (frame 49) and Path B
    // (frame 50, nonexistent) are both unavailable; only Path C remains.
    let cert = StreamCert {
        stream_id:        STREAM_ID,
        batches:          Vec::new(),
        head_anchor:      full.head_anchor, // durable (49, h[49])
        snapshot_anchors: vec![SnapshotAnchor {
            stream_id:  STREAM_ID,
            version:    49,
            chain_hash: h_v,
        }],
    };

    let out = load_verified::<Account>(&cert, Some((&r, &blob))).unwrap();
    assert!(!out.rebuilt_by_replay);
    assert_eq!(out.paths_used, vec![VerifyPath::FromRetentionAnchor]);
    assert_eq!(out.tail_len, 0); // empty tail

    // Corrupt the retention anchor -> Path C rejects.
    let mut bad = cert.clone();
    bad.snapshot_anchors[0].chain_hash[0] ^= 0xFF;
    let err = load_verified::<Account>(&bad, Some((&r, &blob))).unwrap_err();
    assert_eq!(
        err,
        VerifyError::PrefixHashMismatch {
            path: VerifyPath::FromRetentionAnchor,
        }
    );
}

// ===========================================================================
// Verify throughput bench (spec §7.2). ev/s of load_verified over a large
// recovered tail. Run:
//   cargo test -p mess-log --release --test crash_verify
// verify_throughput_bench \     -- --ignored --nocapture
// ===========================================================================

#[test]
#[ignore = "perf bench; run explicitly with --release --ignored --nocapture"]
fn verify_throughput_bench() {
    use std::time::Instant;
    const N: u64 = 1_000_000;
    let payloads = workload(N);
    let cert = build_cert(STREAM_ID, &payloads, 100, None);

    // Snapshot at v=0: the tail is the whole 1M-event stream, so load_verified
    // pays the full prefix cert + tail replay + head anchor.
    let (r, blob) = take_snapshot::<Account>(&cert, 0);

    // Warmup.
    let _ = load_verified::<Account>(&cert, Some((&r, &blob))).unwrap();

    let mut best = f64::INFINITY;
    for _ in 0..3 {
        let t = Instant::now();
        let out = load_verified::<Account>(&cert, Some((&r, &blob))).unwrap();
        let dt = t.elapsed().as_secs_f64();
        assert_eq!(out.tail_len, N - 1);
        best = best.min(dt);
    }
    eprintln!("=== load_verified throughput (1M-event tail, batch 100) ===");
    eprintln!(
        "  verify    {:.2} M ev/s ({:.1} ms best-of-3)",
        N as f64 / best / 1e6,
        best * 1e3
    );
    eprintln!("  per-event {:.1} ns/ev", best * 1e9 / N as f64);
}
