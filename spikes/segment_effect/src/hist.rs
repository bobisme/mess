//! Randomized capsule-history generator + invalidity injectors.
//!
//! Corpus distribution (documented for the REPORT): per-history capsule
//! count is tiered so the 100k-history suite finishes —
//! 97% uniform in [100, 1_000], 2.9% uniform in (1_000, 10_000],
//! 0.1% uniform in (10_000, 100_000] (expected ≈ 600 capsules/history).
//! Stream POOL size is log-uniform in [1_000, 100_000] (touched streams are
//! naturally fewer on small histories); 30% of user batches target a hot
//! subset of ≤64 streams so multi-capsule streams exercise path
//! composition. Segment boundaries are random (16..=512 capsules).
//! Capsule mix: 85% user batches (1..=8 events), 5% registrations (with
//! occasional idempotent re-registration), 3% snapshots, 3% projection
//! checkpoints, 3% dedupe keys, 1% allocator advances. Dedupe span W ∈
//! {1k, 10k, 100k} positions, epoch span W/8.

use crate::model::{Capsule, Log};

/// splitmix64 — deterministic, seedable, dependency-free.
#[derive(Clone)]
pub struct Rng(pub u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed.wrapping_add(0x9e37_79b9_7f4a_7c15))
    }

    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `[lo, hi]`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }

    pub fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
}

pub struct History {
    pub log: Log,
    /// Streams the generator used (dense ids 0..pool).
    pub stream_pool: u64,
}

/// Tiered capsule count (see module docs).
pub fn tiered_count(rng: &mut Rng) -> u64 {
    let roll = rng.next() % 1000;
    if roll < 970 {
        rng.range(100, 1_000)
    } else if roll < 999 {
        rng.range(1_001, 10_000)
    } else {
        rng.range(10_001, 100_000)
    }
}

/// Generate one VALID history.
pub fn generate(seed: u64) -> History {
    let mut rng = Rng::new(seed);
    let n = tiered_count(&mut rng);
    generate_sized(seed ^ 0xabcd, n)
}

/// Generate one VALID history with an explicit capsule count (bench use).
pub fn generate_sized(seed: u64, n: u64) -> History {
    let mut rng = Rng::new(seed);
    // Log-uniform pool in [1k, 100k]: 10^(3 + u*2).
    let u = (rng.next() % 10_000) as f64 / 10_000.0;
    let pool = (1000.0 * 10f64.powf(u * 2.0)) as u64;
    generate_with(seed ^ 0x5eed, n, pool)
}

/// Generate one VALID history with explicit capsule count and stream pool.
pub fn generate_with(seed: u64, n: u64, pool: u64) -> History {
    let mut rng = Rng::new(seed);
    let pool = pool.max(4);
    let hot: u64 = pool.min(64);
    let dedupe_span = [1_000u64, 10_000, 100_000][(rng.next() % 3) as usize];

    let mut heads: hashbrown::HashMap<u64, u64> = hashbrown::HashMap::new();
    let mut touched: Vec<u64> = Vec::new(); // streams with >=1 event
    let mut registered: Vec<(u64, u64)> = Vec::new(); // (name, id)
    let mut next_name: u64 = 1;
    let mut alloc: hashbrown::HashMap<u32, u64> = hashbrown::HashMap::new();
    let mut pos: u64 = 0;
    let mut capsules: Vec<Capsule> = Vec::with_capacity(n as usize);

    while (capsules.len() as u64) < n {
        let roll = rng.next() % 100;
        if roll < 85 {
            // User batch. 30% hot subset -> multi-capsule streams.
            let sid = if rng.chance(30) {
                rng.range(0, hot - 1)
            } else {
                rng.range(0, pool - 1)
            };
            let count = rng.range(1, 8) as u32;
            let first = heads.get(&sid).copied().unwrap_or(0);
            capsules.push(Capsule::UserBatch {
                stream_id: sid,
                first_version: first,
                event_count: count,
                first_global_pos: pos,
            });
            if first == 0 {
                touched.push(sid);
            }
            heads.insert(sid, first + u64::from(count));
            pos += u64::from(count);
        } else if roll < 90 {
            // Registration: new unique pair, or (10%) an idempotent
            // re-registration of an existing pair.
            if !registered.is_empty() && rng.chance(10) {
                let &(name, id) =
                    &registered[(rng.next() as usize) % registered.len()];
                capsules.push(Capsule::StreamRegistered { name, stream_id: id });
            } else {
                let name = next_name;
                next_name += 1;
                // ids disjoint from names (names 1.., ids offset by 1<<40)
                let id = (1u64 << 40) + name;
                registered.push((name, id));
                capsules.push(Capsule::StreamRegistered { name, stream_id: id });
            }
        } else if roll < 93 {
            // Snapshot on a stream with events.
            if touched.is_empty() {
                continue;
            }
            let sid = touched[(rng.next() as usize) % touched.len()];
            let head = heads[&sid];
            capsules.push(Capsule::SnapshotInstalled {
                stream_id: sid,
                version: rng.range(1, head),
                snapshot_ref: rng.next(),
            });
        } else if roll < 96 {
            capsules.push(Capsule::ProjectionCheckpoint {
                projection_id: (rng.next() % 4) as u32,
                shard: (rng.next() % 4) as u32,
                position: rng.range(0, pos.max(1)),
            });
        } else if roll < 99 {
            capsules.push(Capsule::DedupeKey {
                fingerprint: rng.next(),
                position: pos,
            });
        } else {
            let slot = (rng.next() % 4) as u32;
            let cur = alloc.get(&slot).copied().unwrap_or(0);
            let value = cur + rng.range(1, 100);
            alloc.insert(slot, value);
            capsules.push(Capsule::AllocatorSet { slot, value });
        }
    }

    // Random segment boundaries: 16..=512 capsules per segment.
    let mut boundaries = Vec::new();
    let mut at = 0usize;
    loop {
        at += rng.range(16, 512) as usize;
        if at >= capsules.len() {
            break;
        }
        boundaries.push(at);
    }

    History {
        log: Log::seal(capsules, &boundaries, dedupe_span),
        stream_pool: pool,
    }
}

// ---------------------------------------------------------------------------
// Invalidity injectors — each returns the mutated capsule index, or None if
// the history had no viable mutation site. The mutated log is RESEALED so
// its anchors are self-consistent: rejection must come from semantic
// validation, never from a conveniently broken anchor chain.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub enum Injection {
    /// Head continuity gap/overlap: perturb a `first_version`.
    HeadGapOverlap,
    /// Same name, different id (or same id, different name).
    RegistryConflict,
    /// Allocator moves backwards.
    AllocatorRegression,
    /// Global position gap/overlap: perturb a `first_global_pos`.
    PositionSkew,
}

pub fn inject(h: &History, seed: u64, what: Injection) -> Option<(Log, usize)> {
    let mut rng = Rng::new(seed);
    let mut capsules = h.log.capsules.clone();
    let boundaries: Vec<usize> =
        h.log.segments.iter().skip(1).map(|s| s.lo).collect();
    let idx = match what {
        Injection::HeadGapOverlap => {
            let sites: Vec<usize> = capsules
                .iter()
                .enumerate()
                .filter(|(_, c)| matches!(c, Capsule::UserBatch { .. }))
                .map(|(i, _)| i)
                .collect();
            if sites.is_empty() {
                return None;
            }
            let i = sites[(rng.next() as usize) % sites.len()];
            if let Capsule::UserBatch { first_version, .. } = &mut capsules[i] {
                let delta = rng.range(1, 5);
                if rng.chance(50) {
                    *first_version += delta; // gap
                } else {
                    *first_version =
                        first_version.saturating_sub(delta).wrapping_sub(
                            u64::from(*first_version == 0),
                        ); // overlap (or underflow-huge = still a mismatch)
                }
            }
            i
        }
        Injection::RegistryConflict => {
            let sites: Vec<usize> = capsules
                .iter()
                .enumerate()
                .filter(|(_, c)| matches!(c, Capsule::StreamRegistered { .. }))
                .map(|(i, _)| i)
                .collect();
            if sites.is_empty() {
                return None;
            }
            let i = sites[(rng.next() as usize) % sites.len()];
            let (name, stream_id) = match capsules[i] {
                Capsule::StreamRegistered { name, stream_id } => (name, stream_id),
                _ => unreachable!(),
            };
            // Insert a conflicting registration somewhere at or after i.
            let at = i + 1 + (rng.next() as usize) % (capsules.len() - i);
            let conflict = if rng.chance(50) {
                Capsule::StreamRegistered { name, stream_id: stream_id ^ 1 }
            } else {
                Capsule::StreamRegistered { name: name ^ (1 << 20), stream_id }
            };
            capsules.insert(at, conflict);
            // Positions of user batches after the insertion are unchanged
            // (controls consume nothing), so the history is otherwise valid.
            at
        }
        Injection::AllocatorRegression => {
            let sites: Vec<usize> = capsules
                .iter()
                .enumerate()
                .filter(|(_, c)| matches!(c, Capsule::AllocatorSet { .. }))
                .map(|(i, _)| i)
                .collect();
            if sites.is_empty() {
                return None;
            }
            let i = sites[(rng.next() as usize) % sites.len()];
            let (slot, value) = match capsules[i] {
                Capsule::AllocatorSet { slot, value } => (slot, value),
                _ => unreachable!(),
            };
            if value == 0 {
                return None;
            }
            let at = i + 1 + (rng.next() as usize) % (capsules.len() - i);
            capsules.insert(
                at,
                Capsule::AllocatorSet { slot, value: value - 1 },
            );
            at
        }
        Injection::PositionSkew => {
            let sites: Vec<usize> = capsules
                .iter()
                .enumerate()
                .filter(|(_, c)| matches!(c, Capsule::UserBatch { .. }))
                .map(|(i, _)| i)
                .collect();
            if sites.is_empty() {
                return None;
            }
            let i = sites[(rng.next() as usize) % sites.len()];
            if let Capsule::UserBatch { first_global_pos, .. } = &mut capsules[i]
            {
                *first_global_pos = first_global_pos.wrapping_add(rng.range(1, 9));
            }
            i
        }
    };
    // Insertion-type injections shift every later boundary by one.
    let inserted = capsules.len() > h.log.capsules.len();
    let boundaries: Vec<usize> = boundaries
        .into_iter()
        .map(|b| if inserted && b > idx { b + 1 } else { b })
        .collect();
    Some((Log::seal(capsules, &boundaries, h.log.dedupe_span), idx))
}
