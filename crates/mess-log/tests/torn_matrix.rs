//! Torn-write / sector-reorder conformance matrix (bn-py3): the A4
//! conformance bar promised by `spikes/torn_write/REPORT.md`, ported to run
//! against the **production** write/commit/recovery path instead of the
//! spike's standalone format.
//!
//! Every case: build workloads with the real [`SegmentWriter`] (the
//! production write path, D1) — append a random-prefix "acked" set of
//! batches each individually synced, then more batches left un-synced — onto
//! the sim [`SimFs`] sector-reordering fault medium
//! ([`Fault::Sector`], ported from the spike's `SectorDisk`), crash-randomize
//! (arbitrary pending-sector persistence + optional torn sector), recover
//! with the production [`recover_segment`], and assert:
//!
//! - **acked-implies-recovered**: every synced batch is in the accepted
//!   prefix, byte-exact, at its exact offset;
//! - **never-accept-uncommitted**: EVERY accepted batch (acked or the A6
//!   surfaced-unacked-but-fully-persisted kind) is byte-exact one of the
//!   batches that was actually written — never partial, reordered-hole,
//!   stale-generation, or otherwise corrupt;
//! - recovery is idempotent.
//!
//! Matrix: sector size `{512 B, 4096 B}` x background `{zeros, garbage,
//! stale-prior-generation}` = 6 configs. [`torn_matrix_fast`] runs ~1k cases
//! (unconditional gate, `cargo test -p mess-log`); [`torn_matrix_full`] runs
//! the full >=20,000-case sweep (`#[ignore]`, nightly CI —
//! `.github/workflows/torn-matrix.yml`).
//!
//! # Committer note
//!
//! The production **committer** (`commit_group` in `src/committer.rs`) is a
//! private function: for one gathered group it (a) calls
//! [`SegmentWriter::append`] once per batch, then (b) issues exactly one
//! [`SegmentWriter::sync`] barrier covering the group (`Durability::Os`/
//! `Group`), or (c) no barrier at all (`Durability::Process`). This harness
//! calls those same two public writer primitives in that same
//! append*-then-one-sync shape directly (sync after each "acked" batch,
//! matching `Os`'s sync-per-batch/group-of-one; no sync for the un-synced
//! tail, matching `Process`) — durability-identical to what `commit_group`
//! does, without needing crate-internal access. `committer.rs`'s own crash
//! test (`acks_strictly_after_covering_barrier`) exercises the async
//! gather/channel layer on top of this same writer contract; that layer
//! decides *how batches are grouped*, not *what bytes land durably*, which
//! is exactly the piece this matrix stresses.
//!
//! # Differential mode (A4 proof)
//!
//! Wired into both entry points below ([`torn_matrix_fast`] /
//! [`torn_matrix_full`]) via `run_case`'s part (b): every case's crashed
//! image is scanned a second time with [`scan_weak`], and both assert the
//! aggregate wrongly-accepted count is `> 0`.
//!
//! The production scanner has exactly one batch-decode path
//! ([`scanner`]'s private `decode_batch`) and it **always** computes the A4
//! CRC — no CRC-off switch exists there (A12), and this file adds none. To
//! demonstrate the CRC is load-bearing rather than belt-and-braces, this
//! file's own `decode_weak` **duplicates** the byte-decode step (magic /
//! version / A2 length / marker magic+length-echo / subframe tiling) with
//! the CRC verify and the marker's CRC echo comparison **omitted**, then
//! feeds the result through the exact same production acceptance kernel
//! ([`AcceptState`]) every full-validation pass uses — so the differential
//! isolates precisely what the CRC buys, the same methodology
//! `spikes/torn_write/REPORT.md` §(b) used.

use std::path::Path;

use mess_log::acceptance::{AcceptState, Candidate, CandidateStatus, Step};
use mess_log::encode::{BatchEncoder, BatchInput, Subframe};
use mess_log::format::*;
use mess_log::runtime::{FileHandle, Fault, Fs, OpenOpts, Rng, SimFs};
use mess_log::scanner::{recover_segment, ScanStop};
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter};

// ---------------------------------------------------------------------------
// Local duplicates of `format.rs`'s field offsets.
//
// `format.rs` deliberately keeps these `pub(crate)` (only `HEADER_LEN`,
// `HEADER_CRC_OFF`, `MARKER_LEN`, magics, and the size bounds are `pub`), so
// an external test crate cannot import them. Duplicated here ONLY for the
// differential's `decode_weak` (§ "Differential mode" above); values pinned
// to `docs/spec/01-log-format.md` §4.2/§4.3/§4.5 and cross-checked against
// production byte-for-byte by `weak_decoder_offsets_match_production`.
// ---------------------------------------------------------------------------

const BH_MAGIC_OFF: usize = 0;
const BH_FORMAT_VERSION_OFF: usize = 4;
const BH_FLAGS_OFF: usize = 6;
const BH_FRAME_COUNT_OFF: usize = 8;
const BH_BATCH_ID_OFF: usize = 12;
const BH_TOTAL_LEN_OFF: usize = 20;
const BH_FIRST_GLOBAL_POS_OFF: usize = 28;
const BH_SEGMENT_EPOCH_OFF: usize = 36;

const CM_MAGIC_OFF: usize = 0;
const CM_TOTAL_LEN_ECHO_OFF: usize = 4;

const SF_COMPRESSED_LEN_OFF: usize = 16;

#[inline]
fn rd_u16(d: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(d[o..o + 2].try_into().unwrap())
}
#[inline]
fn rd_u32(d: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(d[o..o + 4].try_into().unwrap())
}
#[inline]
fn rd_u64(d: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(d[o..o + 8].try_into().unwrap())
}

// ---------------------------------------------------------------------------
// Weak (CRC-off) decode + scan — the A4 differential. Test-only; the
// production scanner never grows this path (A12).
// ---------------------------------------------------------------------------

struct WeakDecoded {
    offset: u64,
    epoch: u64,
    batch_id: u64,
    first_global_pos: u64,
    frame_count: u32,
    total_len: u64,
}

/// `decode_batch`'s byte-decode (§2.1 steps 1-6 of `docs/spec/02-recovery.md`)
/// with the A4 full-batch CRC verify and the marker's CRC echo comparison
/// **omitted** — magic + length echo only, exactly the spike's `Validation::Weak`.
fn decode_weak(img: &[u8], off: usize) -> Result<WeakDecoded, ()> {
    let rem = img.len().checked_sub(off).ok_or(())?;
    if rem < HEADER_LEN {
        return Err(());
    }
    if rd_u32(img, off + BH_MAGIC_OFF) != HEADER_MAGIC {
        return Err(());
    }
    if rd_u16(img, off + BH_FORMAT_VERSION_OFF) != FORMAT_VERSION {
        return Err(());
    }
    let flags = rd_u16(img, off + BH_FLAGS_OFF);
    if flags & !FLAGS_KNOWN_MASK != 0 {
        return Err(());
    }
    let has_chain = flags & FLAG_CRYPTO_CHAIN != 0;
    let frame_count = rd_u32(img, off + BH_FRAME_COUNT_OFF);
    let batch_id = rd_u64(img, off + BH_BATCH_ID_OFF);
    let total_len = rd_u64(img, off + BH_TOTAL_LEN_OFF);
    let first_global_pos = rd_u64(img, off + BH_FIRST_GLOBAL_POS_OFF);
    let epoch = rd_u64(img, off + BH_SEGMENT_EPOCH_OFF);

    if !(MIN_BATCH_LEN..=MAX_BATCH_LEN).contains(&total_len) {
        return Err(()); // A2
    }
    if total_len > rem as u64 {
        return Err(());
    }
    let total_len_usize = total_len as usize;
    let batch = &img[off..off + total_len_usize];

    // WEAK marker check: magic + total_len echo ONLY. No CRC-echo compare,
    // no full-batch CRC verify — the A4 differential.
    let m = total_len_usize - MARKER_LEN;
    if rd_u32(batch, m + CM_MAGIC_OFF) != MARKER_MAGIC {
        return Err(());
    }
    if rd_u64(batch, m + CM_TOTAL_LEN_ECHO_OFF) != total_len {
        return Err(());
    }

    if !subframes_tile_weak(batch, frame_count, has_chain) {
        return Err(());
    }

    Ok(WeakDecoded { offset: off as u64, epoch, batch_id, first_global_pos, frame_count, total_len })
}

/// Duplicate of `scanner::subframes_tile` (structural check only — kept in
/// the weak path too, so the differential isolates the CRC, not tiling).
fn subframes_tile_weak(batch: &[u8], frame_count: u32, has_crypto_chain: bool) -> bool {
    let frames_end = batch.len() - MARKER_LEN;
    let mut p = HEADER_LEN + if has_crypto_chain { CHAIN_LEN } else { 0 };
    if p > frames_end {
        return false;
    }
    for _ in 0..frame_count {
        if p + SUBFRAME_HDR_LEN > frames_end {
            return false;
        }
        let compressed_len = rd_u32(batch, p + SF_COMPRESSED_LEN_OFF) as usize;
        let next = p + SUBFRAME_HDR_LEN + compressed_len;
        if next > frames_end {
            return false;
        }
        p = next;
    }
    p == frames_end
}

/// Duplicate of `scanner::scan_batches`'s accept loop (§2.3): every
/// accept/stop decision routed through the SAME production
/// [`AcceptState::step`] the full-validation scan uses. Only the byte-decode
/// underneath (`decode_weak` above) is weaker.
fn scan_weak(img: &[u8], epoch: u64, base_pos: u64) -> Vec<WeakDecoded> {
    let mut state = AcceptState::new(epoch, base_pos);
    let mut off = SEGMENT_HEADER_LEN;
    let mut out = Vec::new();
    loop {
        if off >= img.len() {
            break;
        }
        let decoded = decode_weak(img, off);
        let status = match &decoded {
            Ok(d) => CandidateStatus::ByteValid(Candidate {
                epoch: d.epoch,
                batch_id: d.batch_id,
                first_global_pos: d.first_global_pos,
                frame_count: d.frame_count,
            }),
            Err(()) => CandidateStatus::ByteInvalid,
        };
        match state.step(status) {
            Step::Accept(_) => {
                let d = decoded.expect("ByteValid status always carries a successful decode");
                off += d.total_len as usize;
                out.push(d);
            }
            Step::Stopped(_) => break,
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

fn rand_bytes(rng: &mut Rng, len: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(len + 8);
    while v.len() < len {
        v.extend_from_slice(&rng.next_u64().to_le_bytes());
    }
    v.truncate(len);
    v
}

fn read_all(fs: &SimFs, path: &Path) -> Vec<u8> {
    let f = fs.open(path, OpenOpts::read_only()).expect("reopen");
    let len = f.len().unwrap() as usize;
    let mut buf = vec![0u8; len];
    let n = f.pread(0, &mut buf).unwrap();
    buf.truncate(n);
    buf
}

/// A recycled-segment background (A9): raw batches (no `SegmentHeader`,
/// which the real writer's `create` always overwrites+syncs at offset 0
/// regardless of background) from an OLD generation (`epoch = 1`) at a
/// position range disjoint from the fresh segment's `[0, ..)`, padded to
/// `capacity` — the coincident-position case is covered separately by
/// `acceptance.rs`'s own deterministic A9 unit test.
fn stale_background(rng: &mut Rng, capacity: usize) -> Vec<u8> {
    let mut img = Vec::with_capacity(capacity);
    let mut enc = BatchEncoder::new();
    let mut pos = 1_000_000u64;
    let mut id = 500u64;
    loop {
        let n_sub = 1 + rng.below(3) as usize;
        let payloads: Vec<Vec<u8>> = (0..n_sub)
            .map(|_| {
                let len = rng.below(200) as usize;
                rand_bytes(rng, len)
            })
            .collect();
        let sfs: Vec<Subframe> = payloads.iter().map(|p| Subframe::plain(0x22, 0, 0, p)).collect();
        let input = BatchInput {
            segment_epoch: 1,
            batch_id: id,
            first_global_pos: pos,
            stream_id: 9,
            category_id: 9,
            first_stream_version: pos,
            crypto_chain: None,
            subframes: &sfs,
        };
        let Ok(bytes) = enc.encode(&input) else { break };
        if img.len() + bytes.len() > capacity {
            break;
        }
        img.extend_from_slice(bytes);
        pos += n_sub as u64;
        id += 1;
    }
    img.resize(capacity, 0xEE);
    img
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Bg {
    Zeros,
    Garbage,
    StaleGen,
}

#[derive(Default)]
struct Stats {
    cases: u64,
    acked_verified: u64,
    unacked_surfaced: u64,
    crc_was_last_line_of_defense: u64,
    weak_wrong_batches: u64,
    weak_wrong_cases: u64,
}

/// One matrix case: plan `1..=4` batches, ack (append+sync) a random prefix,
/// leave the rest un-synced, crash-randomize the pending sectors (+ optional
/// torn sector), recover through the production scanner, and check both the
/// full-validation invariants and the weak-validation differential on the
/// SAME crashed image.
fn run_case(cfg_idx: u64, seed: u64, sector: usize, bg: Bg, tear_prob: f64, stats: &mut Stats) {
    let mut rng = Rng::new(seed);
    stats.cases += 1;

    let k = 1 + rng.below(4) as usize;
    let epoch = 2 + (seed % 997); // always > the stale-gen background's epoch (1)
    let base_pos = 0u64;

    // Plan: precompute each batch's exact on-disk total_len (pure, via the
    // real encoder) so the background capacity can be sized before the
    // fs/writer exist.
    let mut pos = base_pos;
    let mut payloads_per_batch: Vec<Vec<Vec<u8>>> = Vec::with_capacity(k);
    let mut planned_len: Vec<u64> = Vec::with_capacity(k);
    for id in 0..k as u64 {
        let n_sub = 1 + rng.below(4) as usize;
        let payloads: Vec<Vec<u8>> = (0..n_sub)
            .map(|_| {
                let len = rng.below(sector as u64 + sector as u64 / 2 + 1) as usize;
                rand_bytes(&mut rng, len)
            })
            .collect();
        let sfs: Vec<Subframe> = payloads.iter().map(|p| Subframe::plain(0x11, 0, 0, p)).collect();
        let input = BatchInput {
            segment_epoch: epoch,
            batch_id: id,
            first_global_pos: pos,
            stream_id: 0,
            category_id: 0,
            first_stream_version: pos,
            crypto_chain: None,
            subframes: &sfs,
        };
        planned_len.push(BatchEncoder::total_len(&input).expect("encodable"));
        pos += n_sub as u64;
        payloads_per_batch.push(payloads);
    }
    let content_len: u64 = planned_len.iter().sum();
    let capacity =
        ((SEGMENT_HEADER_LEN as u64 + content_len + 2 * sector as u64).div_ceil(sector as u64)
            * sector as u64) as usize;

    let background = match bg {
        Bg::Zeros => vec![0u8; capacity],
        Bg::Garbage => rand_bytes(&mut rng, capacity),
        Bg::StaleGen => stale_background(&mut rng, capacity),
    };

    let fault = Fault::Sector { sector_size: sector };
    let fs = SimFs::new(fault);
    let path = Path::new("torn.seg");
    fs.seed(path, fault, background);

    let mut w = SegmentWriter::create(&fs, path, SegmentParams::new(1, base_pos, epoch, 0)).unwrap();

    let acked = rng.below(k as u64 + 1) as usize;
    let mut offsets = Vec::with_capacity(k);
    for (idx, payloads) in payloads_per_batch.iter().enumerate() {
        let sfs: Vec<Subframe> = payloads.iter().map(|p| Subframe::plain(0x11, 0, 0, p)).collect();
        let spec = BatchSpec {
            stream_id: 0,
            category_id: 0,
            first_stream_version: w.next_pos(),
            crypto_chain: None,
            subframes: &sfs,
        };
        let r = w.append(&spec).unwrap();
        offsets.push(r.offset);
        if idx < acked {
            w.sync().unwrap(); // matches commit_group's barrier for an acked group of one
        }
        // else: written, un-synced — matches commit_group's Process-mode
        // (no barrier) shape for the tail.
    }

    let clean_image = read_all(&fs, path);

    fs.crash_random(path, &mut rng, tear_prob).unwrap();

    let crashed_image = read_all(&fs, path);
    let rec = recover_segment(&fs, path).unwrap();

    // ---- (a) full-validation invariants ---------------------------------
    assert!(
        rec.accepted.len() >= acked,
        "cfg{cfg_idx} seed{seed}: LOST ACKED BATCHES: {} < {} (stop={:?})",
        rec.accepted.len(),
        acked,
        rec.stop
    );
    assert!(
        rec.accepted.len() <= k,
        "cfg{cfg_idx} seed{seed}: accepted more batches than were ever written"
    );
    for (i, ab) in rec.accepted.iter().enumerate() {
        assert_eq!(ab.batch_id, i as u64, "cfg{cfg_idx} seed{seed}: batch_id");
        assert_eq!(ab.offset, offsets[i], "cfg{cfg_idx} seed{seed}: offset");
        assert_eq!(ab.total_len, planned_len[i], "cfg{cfg_idx} seed{seed}: total_len");
        let a = ab.offset as usize;
        let b = a + ab.total_len as usize;
        assert_eq!(
            &crashed_image[a..b],
            &clean_image[a..b],
            "cfg{cfg_idx} seed{seed}: accepted batch {i} bytes differ from what was written \
             -- a corrupt/partial/stale batch was accepted"
        );
    }
    stats.acked_verified += acked as u64;
    stats.unacked_surfaced += (rec.accepted.len() - acked) as u64;
    if rec.stop == ScanStop::BadCrc {
        stats.crc_was_last_line_of_defense += 1;
    }

    // idempotence
    let again = recover_segment(&fs, path).unwrap();
    assert_eq!(rec, again, "cfg{cfg_idx} seed{seed}: recovery not idempotent");

    // ---- (b) weak-validation differential, SAME crashed image -----------
    let header = rec.header.expect("segment header is always durable (writer syncs it at create)");
    let weak = scan_weak(&crashed_image, header.epoch, header.base_pos);
    let mut wrong_here = 0u64;
    for (i, wd) in weak.iter().enumerate() {
        let byte_ok = i < k
            && wd.batch_id == i as u64
            && wd.offset == offsets[i]
            && wd.total_len == planned_len[i]
            && {
                let a = offsets[i] as usize;
                let b = a + wd.total_len as usize;
                b <= crashed_image.len() && b <= clean_image.len() && crashed_image[a..b] == clean_image[a..b]
            };
        if !byte_ok {
            wrong_here += 1;
        }
    }
    stats.weak_wrong_batches += wrong_here;
    if wrong_here > 0 {
        stats.weak_wrong_cases += 1;
    }
}

fn configs() -> [(usize, Bg); 6] {
    [
        (512, Bg::Zeros),
        (512, Bg::Garbage),
        (512, Bg::StaleGen),
        (4096, Bg::Zeros),
        (4096, Bg::Garbage),
        (4096, Bg::StaleGen),
    ]
}

const MASTER_SEED: u64 = 0x7042_D15C_5EED;
const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

/// `strict_per_config`: additionally assert the differential fires in EVERY
/// config individually, not just in aggregate. At ~1.4% of cases wrongly
/// accepted (see `spikes/torn_write/REPORT.md`), that per-config assertion
/// needs several thousand iterations/config to not be a coin flip; the fast
/// profile instead relies on the aggregate `weak_wrong_batches > 0` check
/// (its own caller), which is overwhelmingly robust already at ~1k cases.
fn run_matrix(iters_per_config: u64, strict_per_config: bool) -> Stats {
    let mut grand = Stats::default();
    for (idx, (sector, bg)) in configs().iter().enumerate() {
        let mut s = Stats::default();
        for i in 0..iters_per_config {
            let seed = MASTER_SEED ^ ((idx as u64) << 56) ^ i.wrapping_mul(GOLDEN);
            run_case(idx as u64, seed, *sector, *bg, 0.3, &mut s);
        }
        if strict_per_config {
            assert!(
                s.weak_wrong_batches > 0,
                "config ({sector}, {bg:?}): weakened validation never failed -- differential broken"
            );
        }
        grand.cases += s.cases;
        grand.acked_verified += s.acked_verified;
        grand.unacked_surfaced += s.unacked_surfaced;
        grand.crc_was_last_line_of_defense += s.crc_was_last_line_of_defense;
        grand.weak_wrong_batches += s.weak_wrong_batches;
        grand.weak_wrong_cases += s.weak_wrong_cases;
    }
    grand
}

// ---------------------------------------------------------------------------
// The two entry points
// ---------------------------------------------------------------------------

/// Fast profile: ~1k cases (170/config x 6 configs), part of the default
/// `cargo test -p mess-log` gate.
///
/// `#[cfg_attr(miri, ignore)]`: not a real-fs/real-thread test (everything
/// runs on the in-memory [`SimFs`]) — the miri.yml convention's usual reason
/// to exclude — but 1,020 full writer-append+crash+scan cycles, each
/// including several real `crc32c` (SSE4.2-intrinsic-dispatching) calls, is
/// squarely the "hot-path timing"/heavy-loop class miri.yml already excludes:
/// measured locally, `torn_matrix_fast` alone did not finish inside 180s
/// under `cargo +nightly miri test` (versus ~0.1s natively), which would blow
/// out the documented "~20s for mess-log" Miri-lane budget on every PR.
#[test]
#[cfg_attr(miri, ignore = "1k-case matrix: too slow under Miri's interpreter, see doc comment")]
fn torn_matrix_fast() {
    let g = run_matrix(170, false);
    assert_eq!(g.cases, 1020);
    println!(
        "torn_matrix_fast: {} cases | {} acked verified | {} unacked surfaced (A6) | \
         CRC last line of defense in {} | weak wrongly accepted {} batches in {} cases",
        g.cases, g.acked_verified, g.unacked_surfaced, g.crc_was_last_line_of_defense,
        g.weak_wrong_batches, g.weak_wrong_cases
    );
    assert!(g.weak_wrong_batches > 0, "differential failed to demonstrate A4");
}

/// Full A4 conformance bar: >=20,000 cases (4,000/config x 6 configs, mirroring
/// `spikes/torn_write`'s original sweep). `#[ignore]`d — run explicitly, or via
/// the nightly `.github/workflows/torn-matrix.yml`.
///
/// Run once locally (2026-07-08, debug build, `cargo test -p mess-log --test
/// torn_matrix -- --ignored torn_matrix_full`): **24,000 cases in ~1.72s**
/// (release: ~0.95s) — 30,016 acked batches verified intact/in-order, 3,031
/// unacked-but-complete surfaced (A6), the CRC was the only rejecting check
/// in 1,876 cases, and the weak-validation differential wrongly accepted
/// 398 corrupt batches across 392 of 24,000 cases (~1.63%) — every one of
/// the 6 configs individually demonstrated the differential (`strict_per_config`
/// below). Consistent with `spikes/torn_write/REPORT.md`'s original 24,000-case
/// sweep (348 wrongly accepted in 341 cases, ~1.4%).
// Already unconditionally `#[ignore]`d (never runs under plain `cargo test`,
// nor `cargo miri test`, without `--ignored`); no separate `cfg_attr(miri,
// ignore)` is needed on top — see `torn_matrix_fast`'s doc comment for why
// the matrix is Miri-prohibitive if it were ever run with `--ignored` there.
#[test]
#[ignore = "full A4 matrix: run explicitly or via nightly CI (torn-matrix.yml)"]
fn torn_matrix_full() {
    let start = std::time::Instant::now();
    let g = run_matrix(4_000, true);
    let elapsed = start.elapsed();
    assert_eq!(g.cases, 24_000);
    println!(
        "torn_matrix_full: {} cases in {elapsed:?} | {} acked verified | {} unacked surfaced (A6) | \
         CRC last line of defense in {} | weak wrongly accepted {} batches in {} cases",
        g.cases, g.acked_verified, g.unacked_surfaced, g.crc_was_last_line_of_defense,
        g.weak_wrong_batches, g.weak_wrong_cases
    );
    assert!(g.weak_wrong_batches > 0, "differential failed to demonstrate A4");
}

/// Pins the local `decode_weak` offsets (duplicated because `format.rs` keeps
/// them `pub(crate)`) against production: on a clean, uncrashed, single-batch
/// image, the weak decoder's fields must exactly match what
/// [`recover_segment`] reports.
#[test]
fn weak_decoder_offsets_match_production_on_clean_image() {
    let fault = Fault::SECTOR_512;
    let fs = SimFs::new(fault);
    let path = Path::new("pin.seg");
    let mut w = SegmentWriter::create(&fs, path, SegmentParams::new(1, 10, 7, 0)).unwrap();
    let payload = vec![0xABu8; 37];
    let sfs = [Subframe::plain(0x11, 0, 0, &payload), Subframe::plain(0x11, 0, 0, &payload)];
    let spec = BatchSpec {
        stream_id: 3,
        category_id: 4,
        first_stream_version: 10,
        crypto_chain: None,
        subframes: &sfs,
    };
    let receipt = w.append(&spec).unwrap();
    w.sync().unwrap();

    let rec = recover_segment(&fs, path).unwrap();
    assert_eq!(rec.accepted.len(), 1);
    let production = rec.accepted[0];

    let img = read_all(&fs, path);
    let weak = decode_weak(&img, SEGMENT_HEADER_LEN).expect("decodes");
    assert_eq!(weak.epoch, production.segment_epoch);
    assert_eq!(weak.batch_id, production.batch_id);
    assert_eq!(weak.first_global_pos, production.first_global_pos);
    assert_eq!(weak.frame_count, production.frame_count);
    assert_eq!(weak.total_len, production.total_len);
    assert_eq!(receipt.offset, SEGMENT_HEADER_LEN as u64);
}
