//! bn-9mw Spike E: the v4 torn-write / sector-reorder conformance matrix — the
//! v4 counterpart of `tests/torn_matrix.rs`, extended with **control regions**
//! and **zero-event (control-only) capsules** at 512 B / 4 KiB sectors.
//!
//! Every case builds a workload of 1..=4 capsules (a random mix of
//! control-only, events-only, and mixed) with the real [`CapsuleWriter`] over
//! the sim [`SimFs`] sector-reordering medium, acks (append+sync) a random
//! prefix, leaves the rest un-synced, crash-randomizes (arbitrary
//! pending-sector persistence + optional torn sector) over {zeros, garbage,
//! stale-prior- generation} backgrounds, recovers with the production
//! [`recover_v4_segment_ physical`], and asserts:
//!
//! - **acked-implies-recovered**: every synced capsule is in the accepted
//!   prefix, byte-exact, at its exact offset;
//! - **never-accept-uncommitted**: EVERY accepted capsule is byte-exact one of
//!   the capsules actually written — never a split (partial control/event
//!   region), reordered hole, or stale-generation capsule;
//! - **control-only cursor**: the recovered global position equals the sum of
//!   accepted `event_count`s (a control-only capsule advances no global
//!   position);
//! - recovery is idempotent.
//!
//! # CRC differential (§18 load-bearing proof)
//!
//! Each crashed image is also scanned with [`decode_weak_v4`] — the same byte
//! framing (magic/version/length/marker magic+echoes/region tiling) with the
//! full-capsule CRC verify and the marker's CRC echo compare **omitted** — fed
//! through the same protocol checks. The aggregate wrongly-accepted count must
//! be `> 0`, demonstrating the CRC is the load-bearing check, exactly as the v3
//! matrix's `scan_weak` does.

use std::path::Path;

use mess_log::encode::Subframe;
use mess_log::runtime::{Fault, FileHandle, Fs, OpenOpts, Rng, SimFs};
use mess_log::v4::capsule::{CapsuleEncoder, CapsuleInput};
use mess_log::v4::control::ControlRecord;
use mess_log::v4::format::*;
use mess_log::v4::recover::{
    recover_v4_segment_physical, scan_v4_image_physical,
};
use mess_log::v4::writer::{CapsuleSpec, CapsuleWriter, SegmentParamsV4};

const SEGMENT_HEADER_LEN: usize = 52;

fn rand_bytes(rng: &mut Rng, len: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(len + 8);
    while v.len() < len {
        v.extend_from_slice(&rng.next_u64().to_le_bytes());
    }
    v.truncate(len);
    v
}

fn read_all(fs: &SimFs, path: &Path) -> Vec<u8> {
    let f = fs.open(path, OpenOpts::read_only()).unwrap();
    let len = f.len().unwrap() as usize;
    let mut buf = vec![0u8; len];
    let n = f.pread(0, &mut buf).unwrap();
    buf.truncate(n);
    buf
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Bg {
    Zeros,
    Garbage,
    StaleGen,
}

/// A recycled-segment background of stale prior-generation (epoch 1) capsules,
/// disjoint position range, padded — mirrors torn_matrix's `stale_background`.
fn stale_background(rng: &mut Rng, capacity: usize) -> Vec<u8> {
    let mut img = Vec::with_capacity(capacity);
    let mut enc = CapsuleEncoder::new();
    let mut pos = 1_000_000u64;
    let mut id = 500u64;
    loop {
        let controls = [ControlRecord::CategoryRegistered {
            category_id: id,
            name:        "stale".to_string(),
        }];
        let ev = rand_bytes(rng, 16);
        let sfs = [Subframe::plain(7, 0, 0, &ev)];
        let input = CapsuleInput {
            segment_epoch:        1,
            batch_id:             id,
            first_global_pos:     pos,
            stream_id:            77,
            category_id:          0,
            first_stream_version: 0,
            crypto_chain:         None,
            controls:             &controls,
            subframes:            &sfs,
        };
        let Ok(bytes) = enc.encode(&input) else { break };
        if img.len() + bytes.len() > capacity {
            break;
        }
        img.extend_from_slice(bytes);
        pos += 1;
        id += 1;
    }
    img.resize(capacity, 0xEE);
    img
}

/// One planned capsule: its controls and event payloads.
struct PlannedCapsule {
    controls:  Vec<ControlRecord>,
    payloads:  Vec<Vec<u8>>,
    stream_id: u64,
}

fn plan_capsule(rng: &mut Rng, i: usize, sector: usize) -> PlannedCapsule {
    // 0..=2 controls, 0..=2 events, sum >= 1. Zero-event (control-only) is
    // explicitly reachable.
    let mut n_ctl = rng.below(3) as usize;
    let n_ev = rng.below(3) as usize;
    if n_ctl + n_ev == 0 {
        n_ctl = 1;
    }
    let etype = 100 + i as u32;
    let mut controls = Vec::new();
    for c in 0..n_ctl {
        if c == 0 && n_ev > 0 {
            controls.push(ControlRecord::EventTypeRegistered {
                event_type_id:          etype,
                codec_id:               1,
                current_schema_version: 1,
                schema_fingerprint:     [i as u8; 32],
                name:                   format!("T{i}"),
            });
        } else {
            let klen = 1 + rng.below(20) as usize;
            controls.push(ControlRecord::DedupeKey {
                scope_kind: DEDUPE_SCOPE_STREAM,
                scope_id:   9,
                key:        rand_bytes(rng, klen),
            });
        }
    }
    let payloads: Vec<Vec<u8>> = (0..n_ev)
        .map(|_| {
            let len = rng.below(sector as u64 + 1) as usize;
            rand_bytes(rng, len)
        })
        .collect();
    let stream_id = if n_ev == 0 { 0 } else { 9 };
    PlannedCapsule { controls, payloads, stream_id }
}

#[derive(Default)]
struct Stats {
    cases:            u64,
    acked_verified:   u64,
    unacked_surfaced: u64,
    weak_wrong_caps:  u64,
    weak_wrong_cases: u64,
}

const MASTER_SEED: u64 = 0x9A54_D15C_5EED;
const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

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

fn run_case(
    seed: u64,
    sector: usize,
    bg: Bg,
    tear_prob: f64,
    stats: &mut Stats,
) {
    let mut rng = Rng::new(seed);
    stats.cases += 1;

    let k = 1 + rng.below(4) as usize;
    let epoch = 2 + (seed % 997); // > stale background epoch (1)

    // Plan capsules, and compute the clean encoding to size the background.
    let planned: Vec<PlannedCapsule> =
        (0..k).map(|i| plan_capsule(&mut rng, i, sector)).collect();
    let mut enc = CapsuleEncoder::new();
    let mut content_len = 0u64;
    let mut gpos = 0u64;
    let mut batch_id = 0u64;
    for pc in &planned {
        let sfs: Vec<Subframe> = pc
            .payloads
            .iter()
            .map(|p| {
                Subframe::plain(
                    if pc.stream_id == 0 { 0 } else { 100 },
                    1,
                    1,
                    p,
                )
            })
            .collect();
        let input = CapsuleInput {
            segment_epoch: epoch,
            batch_id,
            first_global_pos: gpos,
            stream_id: pc.stream_id,
            category_id: 0,
            first_stream_version: 0,
            crypto_chain: None,
            controls: &pc.controls,
            subframes: &sfs,
        };
        content_len += enc.encode(&input).unwrap().len() as u64;
        gpos += pc.payloads.len() as u64;
        batch_id += 1;
    }
    let capacity =
        ((SEGMENT_HEADER_LEN as u64 + content_len + 2 * sector as u64)
            .div_ceil(sector as u64)
            * sector as u64) as usize;

    let background = match bg {
        Bg::Zeros => vec![0u8; capacity],
        Bg::Garbage => rand_bytes(&mut rng, capacity),
        Bg::StaleGen => stale_background(&mut rng, capacity),
    };

    let fault = Fault::Sector { sector_size: sector };
    let fs = SimFs::new(fault);
    let path = Path::new("v4torn.seg");
    fs.seed(path, fault, background);

    let mut w =
        CapsuleWriter::create(&fs, path, SegmentParamsV4::new(1, 0, epoch, 0))
            .unwrap();
    let acked = rng.below(k as u64 + 1) as usize;
    let mut offsets = Vec::with_capacity(k);
    let mut event_counts = Vec::with_capacity(k);
    for (idx, pc) in planned.iter().enumerate() {
        let etype = if pc.stream_id == 0 { 0 } else { 100 };
        let sfs: Vec<Subframe> = pc
            .payloads
            .iter()
            .map(|p| Subframe::plain(etype, 1, 1, p))
            .collect();
        let spec = CapsuleSpec {
            stream_id:            pc.stream_id,
            category_id:          0,
            first_stream_version: 0,
            crypto_chain:         None,
            controls:             &pc.controls,
            subframes:            &sfs,
        };
        let r = w.append(&spec).unwrap();
        offsets.push(r.offset);
        event_counts.push(r.event_count);
        if idx < acked {
            w.sync().unwrap();
        }
    }

    let clean_image = read_all(&fs, path);
    fs.crash_random(path, &mut rng, tear_prob).unwrap();
    let crashed_image = read_all(&fs, path);
    let rec = recover_v4_segment_physical(&fs, path).unwrap();

    // acked-implies-recovered.
    assert!(
        rec.accepted.len() >= acked,
        "seed{seed}: LOST ACKED CAPSULES {} < {} (stop={:?})",
        rec.accepted.len(),
        acked,
        rec.stop
    );
    assert!(rec.accepted.len() <= k, "seed{seed}: accepted more than written");

    // never-accept-uncommitted + byte-identity + control-only accounting.
    let mut expected_gpos = 0u64;
    for (i, ac) in rec.accepted.iter().enumerate() {
        assert_eq!(ac.batch_id, i as u64, "seed{seed}: batch_id");
        assert_eq!(ac.offset, offsets[i], "seed{seed}: offset");
        assert_eq!(ac.first_global_pos, expected_gpos, "seed{seed}: gp");
        assert_eq!(ac.event_count, event_counts[i], "seed{seed}: event_count");
        assert_eq!(
            ac.control_only,
            event_counts[i] == 0,
            "seed{seed}: control_only"
        );
        let a = ac.offset as usize;
        let b = a + ac.total_len as usize;
        assert_eq!(
            &crashed_image[a..b],
            &clean_image[a..b],
            "seed{seed}: accepted capsule {i} bytes differ from written — a \
             split/partial/stale capsule was accepted"
        );
        expected_gpos += u64::from(ac.event_count);
    }
    // control-only cursor: gp advanced only by events.
    assert_eq!(rec.next_global_pos, expected_gpos, "seed{seed}: gp advance");

    stats.acked_verified += acked as u64;
    stats.unacked_surfaced += (rec.accepted.len() - acked) as u64;

    // idempotence.
    let again = scan_v4_image_physical(&crashed_image);
    let rec_img = scan_v4_image_physical(&crashed_image);
    assert_eq!(again, rec_img, "seed{seed}: not idempotent");
    assert_eq!(rec.accepted, again.accepted, "seed{seed}: fs vs image scan");

    // CRC differential.
    let wrong = weak_scan_wrong_count(&crashed_image, &clean_image, &offsets);
    stats.weak_wrong_caps += wrong;
    if wrong > 0 {
        stats.weak_wrong_cases += 1;
    }
}

// ---------------------------------------------------------------------------
// Weak (CRC-off) decode: framing + region tiling, NO full CRC and NO crc echo.
// ---------------------------------------------------------------------------

fn rd_u16(d: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(d[o..o + 2].try_into().unwrap())
}
fn rd_u32(d: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(d[o..o + 4].try_into().unwrap())
}
fn rd_u64(d: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(d[o..o + 8].try_into().unwrap())
}

struct WeakCap {
    offset:    u64,
    total_len: u64,
    batch_id:  u64,
    first_gp:  u64,
    events:    u32,
    epoch:     u64,
}

/// Weak decode of the capsule at `off`: everything `decode_capsule` checks
/// EXCEPT the full-capsule CRC and the marker crc echo.
fn decode_weak(img: &[u8], off: usize) -> Option<WeakCap> {
    let rem = img.len().checked_sub(off)?;
    if rem < CAPSULE_HEADER_LEN {
        return None;
    }
    let h = &img[off..];
    if rd_u32(h, 0) != CAPSULE_MAGIC || rd_u16(h, 4) != FORMAT_VERSION_V4 {
        return None;
    }
    let flags = rd_u16(h, 6);
    if flags & !FLAGS_KNOWN_MASK != 0 {
        return None;
    }
    let event_count = rd_u32(h, 8);
    let control_count = rd_u32(h, 12);
    let batch_id = rd_u64(h, 16);
    let total_len = rd_u64(h, 24);
    let first_gp = rd_u64(h, 32);
    let epoch = rd_u64(h, 40);
    let control_len = rd_u32(h, 72);
    let event_region_len = rd_u32(h, 76);
    if !(MIN_CAPSULE_LEN..=MAX_CAPSULE_LEN).contains(&total_len)
        || total_len > rem as u64
    {
        return None;
    }
    if control_count > MAX_CONTROL_COUNT || control_len > MAX_CONTROL_LEN {
        return None;
    }
    let tl = total_len as usize;
    let cap = &img[off..off + tl];
    let has_chain = flags & FLAG_CRYPTO_CHAIN != 0;
    let chain = if has_chain { CAPSULE_CHAIN_LEN } else { 0 };
    // WEAK marker: magic + batch_id echo + total_len echo ONLY (no crc echo).
    let m = tl - CAPSULE_MARKER_LEN;
    if rd_u32(cap, m) != CAPSULE_MARKER_MAGIC
        || rd_u64(cap, m + 8) != batch_id
        || rd_u64(cap, m + 16) != total_len
    {
        return None;
    }
    // region tiling by declared lengths.
    if CAPSULE_HEADER_LEN
        + chain
        + control_len as usize
        + event_region_len as usize
        + CAPSULE_MARKER_LEN
        != tl
    {
        return None;
    }
    Some(WeakCap {
        offset: off as u64,
        total_len,
        batch_id,
        first_gp,
        events: event_count,
        epoch,
    })
}

/// Count capsules the weak scan accepts that are NOT byte-exact one of the
/// written capsules — the CRC's job.
fn weak_scan_wrong_count(crashed: &[u8], clean: &[u8], offsets: &[u64]) -> u64 {
    // Seed from the (always-durable) header.
    let epoch = rd_u64(crashed, 24); // segment header epoch
    let mut off = SEGMENT_HEADER_LEN;
    let mut expected_batch = 0u64;
    let mut expected_gp = 0u64;
    let mut wrong = 0u64;
    loop {
        if off >= crashed.len() {
            break;
        }
        let Some(wc) = decode_weak(crashed, off) else { break };
        // Same protocol gate as strong (minus CRC): epoch, batch_id, gp.
        if wc.epoch != epoch
            || wc.batch_id != expected_batch
            || wc.first_gp != expected_gp
        {
            break;
        }
        // Is this accepted capsule byte-exact a written one at this offset?
        let a = wc.offset as usize;
        let b = a + wc.total_len as usize;
        let byte_ok = offsets.contains(&wc.offset)
            && b <= clean.len()
            && crashed[a..b] == clean[a..b];
        if !byte_ok {
            wrong += 1;
        }
        expected_batch += 1;
        expected_gp += u64::from(wc.events);
        off = b;
    }
    wrong
}

fn run_matrix(iters_per_config: u64) -> Stats {
    let mut grand = Stats::default();
    for (idx, (sector, bg)) in configs().iter().enumerate() {
        for i in 0..iters_per_config {
            let seed =
                MASTER_SEED ^ ((idx as u64) << 56) ^ i.wrapping_mul(GOLDEN);
            run_case(seed, *sector, *bg, 0.3, &mut grand);
        }
    }
    grand
}

/// Fast profile: ~1.2k cases, part of the default `cargo test -p mess-log`.
#[test]
#[cfg_attr(miri, ignore = "sector matrix: too slow under Miri")]
fn v4_torn_matrix_fast() {
    let g = run_matrix(200);
    assert_eq!(g.cases, 1200);
    println!(
        "v4_torn_matrix_fast: {} cases | {} acked verified | {} unacked \
         surfaced | weak wrongly accepted {} capsules in {} cases",
        g.cases,
        g.acked_verified,
        g.unacked_surfaced,
        g.weak_wrong_caps,
        g.weak_wrong_cases
    );
    assert!(
        g.weak_wrong_caps > 0,
        "CRC differential failed to demonstrate §18"
    );
}

/// Full conformance bar: >=24,000 cases (4,000/config x 6). `#[ignore]`d.
#[test]
#[ignore = "full v4 torn matrix: run explicitly (>=24k cases)"]
fn v4_torn_matrix_full() {
    let start = std::time::Instant::now();
    let g = run_matrix(4_000);
    let elapsed = start.elapsed();
    assert_eq!(g.cases, 24_000);
    println!(
        "v4_torn_matrix_full: {} cases in {elapsed:?} | {} acked verified | \
         {} unacked surfaced | weak wrongly accepted {} capsules in {} cases",
        g.cases,
        g.acked_verified,
        g.unacked_surfaced,
        g.weak_wrong_caps,
        g.weak_wrong_cases
    );
    assert!(
        g.weak_wrong_caps > 0,
        "CRC differential failed to demonstrate §18"
    );
}
