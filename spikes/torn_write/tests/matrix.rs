//! Randomized reordering matrix (deliverable items 3a + 3b).
//!
//! Configs: sector size {512, 4096} x background {zeros, garbage, stale
//! previous-generation log image} x 4000 seeded iterations = 24,000 cases.
//!
//! Each case: plan 1-4 batches; ack (append+fsync) a random prefix of them;
//! write the rest un-fsynced, possibly tearing off only a byte-prefix of the
//! last one; crash by persisting an ARBITRARY RANDOM SUBSET of the pending
//! sectors (reordering) plus an optional torn sector; recover.
//!
//! (a) FULL validation invariants, asserted on every case:
//!     - every acked batch recovered intact, in order, at its offset
//!     - any extra recovered batch is byte-exact one of the fully-written
//!       unacked batches, in order (A6) — never partial, reordered-hole,
//!       stale, or corrupt
//!     - recovery is idempotent
//! (b) WEAK validation (marker magic + length echo, no CRC) on the SAME
//!     image: count wrongly-accepted batches — the A4 differential.
//! Also counted: cases where ONLY the CRC stopped acceptance (marker fully
//! self-consistent, stop == BadCrc), and "resync bait" — fully-intact valid
//! batches sitting past the scan stop point, which a resynchronizing scanner
//! would wrongly resurrect.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use torn_write::*;

const MASTER_SEED: u64 = 0x7042_D15C_5EED;
const ITERS_PER_CONFIG: u64 = 4_000;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Bg {
    Zeros,
    Garbage,
    StaleGen,
}

struct Planned {
    batch_id: u64,
    first_pos: u64,
    events: Vec<Vec<u8>>,
    bytes: Vec<u8>,
    offset: usize,
    /// false => only a byte-prefix was ever written (mid-append crash)
    complete: bool,
}

fn random_events(rng: &mut StdRng, sector: usize) -> Vec<Vec<u8>> {
    let n = rng.gen_range(1..=4);
    (0..n)
        .map(|_| {
            let len = rng.gen_range(0..=sector + sector / 2); // batches span sectors
            (0..len).map(|_| rng.gen()).collect()
        })
        .collect()
}

fn make_background(rng: &mut StdRng, bg: Bg, capacity: usize, sector: usize) -> Vec<u8> {
    match bg {
        Bg::Zeros => vec![0u8; capacity],
        Bg::Garbage => (0..capacity).map(|_| rng.gen()).collect(),
        Bg::StaleGen => {
            // A previous generation of the log occupying the recycled space.
            // Positions are drawn from a disjoint range so the A1 contiguity
            // check applies; the coincident-position case is a deterministic
            // test (A9) in src/lib.rs.
            let mut img = Vec::with_capacity(capacity);
            let mut id = 1000u64;
            let mut pos = 1_000_000u64;
            while img.len() + MIN_BATCH_LEN < capacity {
                let evs = random_events(rng, sector);
                let b = encode_batch(id, pos, &evs);
                if img.len() + b.len() > capacity {
                    break;
                }
                img.extend_from_slice(&b);
                id += 1;
                pos += evs.len() as u64;
            }
            img.resize(capacity, 0xEE);
            img
        }
    }
}

#[derive(Default)]
struct Stats {
    cases: u64,
    acked_verified: u64,
    unacked_surfaced: u64,
    crc_was_last_line_of_defense: u64, // full scan stop == BadCrc
    resync_bait: u64,                  // intact valid batches past the stop point
    weak_wrong_batches: u64,           // weak-accepted batches that are corrupt/false
    weak_wrong_cases: u64,             // cases with >= 1 such batch
    weak_extra_over_full: u64,         // cases where weak accepted more than full
}

fn run_config(cfg_idx: u64, sector: usize, bg: Bg, stats: &mut Stats) {
    for i in 0..ITERS_PER_CONFIG {
        let mut rng = StdRng::seed_from_u64(
            MASTER_SEED ^ (cfg_idx << 56) ^ i.wrapping_mul(0x9E37_79B9_7F4A_7C15),
        );
        stats.cases += 1;

        // Plan the batches.
        let k = rng.gen_range(1..=4);
        let mut planned: Vec<Planned> = Vec::with_capacity(k);
        let mut pos = 0u64;
        let mut off = 0usize;
        for id in 0..k as u64 {
            let events = random_events(&mut rng, sector);
            let bytes = encode_batch(id, pos, &events);
            let len = bytes.len();
            planned.push(Planned {
                batch_id: id,
                first_pos: pos,
                events,
                bytes,
                offset: off,
                complete: true,
            });
            pos += planned.last().unwrap().events.len() as u64;
            off += len;
        }
        let total = off;
        let capacity = (total + 2 * sector).div_ceil(sector) * sector;
        let background = make_background(&mut rng, bg, capacity, sector);
        let mut disk = SectorDisk::new(sector, background);

        // Ack a random prefix; write the rest without fsync; maybe tear the
        // byte stream mid-append of the final batch.
        let acked = rng.gen_range(0..=k);
        for p in planned.iter().take(acked) {
            disk.append(&p.bytes);
            disk.fsync(); // barrier: acked
        }
        for (idx, p) in planned.iter_mut().enumerate().skip(acked) {
            let is_last = idx == k - 1;
            if is_last && rng.gen_bool(0.3) {
                let cut = rng.gen_range(0..p.bytes.len());
                disk.append(&p.bytes[..cut]);
                p.complete = false;
            } else {
                disk.append(&p.bytes);
            }
        }

        // Crash: arbitrary subset of pending sectors persists (reordering),
        // optional torn sector.
        let img = disk.crash_random(&mut rng, 0.3);

        // ---- (a) FULL validation invariants --------------------------------
        let full = scan(&img, 0, 0, Validation::Full);
        assert!(
            full.batches.len() >= acked,
            "cfg{cfg_idx} iter {i}: LOST ACKED BATCHES: {} < {} (stop={:?})",
            full.batches.len(),
            acked,
            full.stop
        );
        for (r, p) in full.batches.iter().zip(planned.iter()) {
            // every accepted batch — acked prefix AND any extras — must be
            // byte-exact the planned batch at that index (extras per A6).
            assert!(p.complete, "cfg{cfg_idx} iter {i}: accepted a partially-written batch");
            assert_eq!(r.batch_id, p.batch_id, "cfg{cfg_idx} iter {i}: batch_id");
            assert_eq!(r.first_global_pos, p.first_pos, "cfg{cfg_idx} iter {i}: position");
            assert_eq!(r.offset as usize, p.offset, "cfg{cfg_idx} iter {i}: offset");
            let payloads: Vec<&Vec<u8>> = r.events.iter().map(|(_, pl)| pl).collect();
            let expected: Vec<&Vec<u8>> = p.events.iter().collect();
            assert_eq!(payloads, expected, "cfg{cfg_idx} iter {i}: payload corruption accepted");
        }
        assert!(
            full.batches.len() <= planned.len(),
            "cfg{cfg_idx} iter {i}: accepted more batches than were ever written"
        );
        stats.acked_verified += acked as u64;
        stats.unacked_surfaced += (full.batches.len() - acked) as u64;
        if full.stop == StopReason::BadCrc {
            stats.crc_was_last_line_of_defense += 1;
        }

        // idempotence
        let again = scan(&img, 0, 0, Validation::Full);
        assert_eq!(full, again, "cfg{cfg_idx} iter {i}: recovery not idempotent");

        // resync bait: a fully-intact valid batch past the stop point (a
        // scanner that skips to the next magic would resurrect it => holes)
        for p in &planned {
            if p.complete
                && p.offset as u64 >= full.safe_offset
                && !(p.offset as u64 == full.safe_offset && full.stop == StopReason::EndOfLog)
                && img[p.offset..p.offset + p.bytes.len()] == p.bytes[..]
            {
                stats.resync_bait += 1;
            }
        }

        // ---- (b) WEAK validation differential ------------------------------
        let weak = scan(&img, 0, 0, Validation::Weak);
        let mut wrong_here = 0u64;
        for (idx, r) in weak.batches.iter().enumerate() {
            let ok = idx < planned.len() && {
                let p = &planned[idx];
                p.complete
                    && r.batch_id == p.batch_id
                    && r.first_global_pos == p.first_pos
                    && r.offset as usize == p.offset
                    && r.events.iter().map(|(_, pl)| pl).eq(p.events.iter())
            };
            if !ok {
                wrong_here += 1;
            }
        }
        stats.weak_wrong_batches += wrong_here;
        if wrong_here > 0 {
            stats.weak_wrong_cases += 1;
        }
        if weak.batches.len() > full.batches.len() {
            stats.weak_extra_over_full += 1;
        }
    }
}

#[test]
fn reordering_matrix() {
    let configs: Vec<(usize, Bg)> = vec![
        (512, Bg::Zeros),
        (512, Bg::Garbage),
        (512, Bg::StaleGen),
        (4096, Bg::Zeros),
        (4096, Bg::Garbage),
        (4096, Bg::StaleGen),
    ];

    let mut grand = Stats::default();
    println!();
    println!(
        "{:>6} {:>9} | {:>7} {:>8} {:>8} {:>8} {:>7} | {:>10} {:>10} {:>9}",
        "sector",
        "bg",
        "cases",
        "acked-ok",
        "unacked",
        "crc-last",
        "resync",
        "weak-wrong",
        "wrong-case",
        "weak>full"
    );
    for (idx, (sector, bg)) in configs.iter().enumerate() {
        let mut s = Stats::default();
        run_config(idx as u64, *sector, *bg, &mut s);
        println!(
            "{:>6} {:>9} | {:>7} {:>8} {:>8} {:>8} {:>7} | {:>10} {:>10} {:>9}",
            sector,
            format!("{bg:?}"),
            s.cases,
            s.acked_verified,
            s.unacked_surfaced,
            s.crc_was_last_line_of_defense,
            s.resync_bait,
            s.weak_wrong_batches,
            s.weak_wrong_cases,
            s.weak_extra_over_full
        );
        grand.cases += s.cases;
        grand.acked_verified += s.acked_verified;
        grand.unacked_surfaced += s.unacked_surfaced;
        grand.crc_was_last_line_of_defense += s.crc_was_last_line_of_defense;
        grand.resync_bait += s.resync_bait;
        grand.weak_wrong_batches += s.weak_wrong_batches;
        grand.weak_wrong_cases += s.weak_wrong_cases;
        grand.weak_extra_over_full += s.weak_extra_over_full;

        // The differential must show up in every config: batches span sectors
        // in all of them, so marker-before-frames reordering is reachable.
        assert!(
            s.weak_wrong_batches > 0,
            "config ({sector}, {bg:?}): weakened validation never failed — differential broken"
        );
    }
    println!(
        "TOTAL: {} cases | {} acked batches verified intact/in-order | \
         {} unacked-but-complete surfaced (allowed, A6) | \
         full validation: 0 partial/corrupt/stale accepted, 0 acked lost | \
         CRC was the last line of defense in {} cases | \
         {} resync-bait batches past stop | \
         WEAK validation wrongly accepted {} corrupt batches in {} cases",
        grand.cases,
        grand.acked_verified,
        grand.unacked_surfaced,
        grand.crc_was_last_line_of_defense,
        grand.resync_bait,
        grand.weak_wrong_batches,
        grand.weak_wrong_cases,
    );
    assert!(grand.weak_wrong_batches > 0, "differential failed to demonstrate A4");
}
