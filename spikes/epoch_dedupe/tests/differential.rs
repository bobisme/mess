//! Exact differential correctness: every candidate must agree EVERYWHERE
//! with the boring reference model (VecDeque + BTreeMap over
//! (scope, full key) -> positions) — the spike's hard gate.
//!
//! Run with `cargo test --release` (the randomized runs are slow in debug).

use std::path::PathBuf;

use epoch_dedupe::arena::Arena;
use epoch_dedupe::epoch::{EpochDedupe, HashActive, IcebergActive};
use epoch_dedupe::g0::FjallDedupe;
use epoch_dedupe::reference::RefModel;
use epoch_dedupe::{DedupeIndex, Fingerprinter, Rng, Scope};

fn tmpdir(tag: &str) -> PathBuf {
    let d = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("ed_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

// ------------------------------------------------------------- harness

struct Harness<I: DedupeIndex> {
    arena: Arena,
    refm: RefModel,
    idx: I,
    next_pos: u64,
    rng: Rng,
    pool: Vec<(Scope, Vec<u8>)>,
    checks: u64,
}

impl<I: DedupeIndex> Harness<I> {
    fn new(idx: I, span: u64, seed: u64) -> Self {
        Harness {
            arena: Arena::new(),
            refm: RefModel::new(span),
            idx,
            next_pos: 0,
            rng: Rng::new(seed),
            pool: Vec::new(),
            checks: 0,
        }
    }

    fn random_scope(&mut self) -> Scope {
        if self.rng.below(10) < 3 { Scope::Global } else { Scope::Stream(self.rng.below(4)) }
    }

    fn random_key(&mut self) -> Vec<u8> {
        let len = match self.rng.below(100) {
            0..2 => 0,
            2..72 => 1 + self.rng.below(32) as usize,
            72..92 => 32 + self.rng.below(268) as usize,
            92..95 => 4096,
            _ => 1 + self.rng.below(4096) as usize,
        };
        (0..len).map(|_| self.rng.next_u64() as u8).collect()
    }

    /// Compare candidate vs reference at the current durable end.
    fn check(&mut self, scope: Scope, key: &[u8]) {
        let w = self.next_pos;
        let got = self.idx.check(scope, key, w, &self.arena);
        let want = self.refm.check(scope, key, w);
        assert_eq!(
            got, want,
            "divergence at w={w} scope={scope:?} keylen={} check#{}",
            key.len(),
            self.checks
        );
        self.checks += 1;
        self.refm.prune(w);
    }

    fn insert(&mut self, scope: Scope, key: &[u8]) {
        let pos = self.next_pos;
        let ptr = self.arena.append(scope, key, pos);
        self.idx.insert(scope, key, pos, ptr);
        self.refm.insert(scope, key, pos);
        // Dedupe records are sparse among ordinary events: advance the
        // durable end by a random stride.
        self.next_pos = pos + 1 + self.rng.below(8);
    }

    fn run(&mut self, ops: usize) {
        for _ in 0..ops {
            match self.rng.below(100) {
                // New (usually) key: check-then-insert, the append path.
                0..50 => {
                    let scope = self.random_scope();
                    let key = self.random_key();
                    self.check(scope, &key);
                    self.insert(scope, &key);
                    self.pool.push((scope, key));
                }
                // Re-check an old key at a random age.
                50..75 if !self.pool.is_empty() => {
                    let i = self.rng.below(self.pool.len() as u64) as usize;
                    let (scope, key) = self.pool[i].clone();
                    self.check(scope, &key);
                }
                // Retry: an old key re-committed at a new position (the
                // unacked-but-recovered / expired-retry shape).
                _ if !self.pool.is_empty() => {
                    let i = self.rng.below(self.pool.len() as u64) as usize;
                    let (scope, key) = self.pool[i].clone();
                    self.check(scope, &key);
                    self.insert(scope, &key);
                }
                _ => {}
            }
        }
        // Final audit: every key ever seen, at the final durable end.
        for i in 0..self.pool.len() {
            let (scope, key) = self.pool[i].clone();
            self.check(scope, &key);
        }
    }
}

fn fpr(bits: u32) -> Fingerprinter {
    Fingerprinter { seed: 0xFEED_FACE, bits }
}

const SPAN: u64 = 4096;

// ------------------------------------------------- randomized 100k ops

#[test]
fn randomized_100k_g1() {
    let mut h = Harness::new(EpochDedupe::<HashActive>::new(SPAN, fpr(128), false), SPAN, 11);
    h.run(100_000);
    assert_eq!(h.idx.deletes_issued(), 0);
    assert!(h.idx.epochs_dropped() > 100, "window turnover exercised");
}

#[test]
fn randomized_100k_g2() {
    let mut h = Harness::new(EpochDedupe::<HashActive>::new(SPAN, fpr(128), true), SPAN, 22);
    h.run(100_000);
    assert_eq!(h.idx.deletes_issued(), 0);
}

#[test]
fn randomized_100k_g3() {
    let mut h = Harness::new(EpochDedupe::<IcebergActive>::new(SPAN, fpr(128), true), SPAN, 33);
    h.run(100_000);
    assert_eq!(h.idx.deletes_issued(), 0);
}

#[test]
fn randomized_100k_g0() {
    let idx = FjallDedupe::open(tmpdir("rand_g0"), SPAN, 1, true);
    let mut h = Harness::new(idx, SPAN, 44);
    h.run(100_000);
    // The synthetic baseline DOES pay per-key deletes — that asymmetry is
    // the point of the tournament.
    assert!(h.idx.deletes_issued() > 0);
}

#[test]
fn randomized_20k_g0_grouped() {
    // Commit groups >1: check() flushes the pending group first.
    let idx = FjallDedupe::open(tmpdir("rand_g0_grp"), SPAN, 4, true);
    let mut h = Harness::new(idx, SPAN, 55);
    h.run(20_000);
}

// ------------------------------------------- forced fingerprint collisions

#[test]
fn forced_collisions_zero_bits_g1() {
    // bits=0: EVERY key fingerprints to 0 — the index must fall back to
    // full-key arena verification for every probe. No false negatives, no
    // false positives.
    let mut h = Harness::new(EpochDedupe::<HashActive>::new(1024, fpr(0), false), 1024, 66);
    h.run(20_000);
}

#[test]
fn forced_collisions_zero_bits_g2() {
    let mut h = Harness::new(EpochDedupe::<HashActive>::new(1024, fpr(0), true), 1024, 77);
    h.run(20_000);
}

#[test]
fn forced_collisions_zero_bits_g3() {
    let mut h = Harness::new(EpochDedupe::<IcebergActive>::new(1024, fpr(0), true), 1024, 88);
    h.run(20_000);
}

#[test]
fn forced_collisions_8_bits_all_epoch_candidates() {
    // 256 distinct fingerprints across thousands of keys: heavy partial
    // collisions in every structure (hash buckets, bins, filters, runs).
    let mut h = Harness::new(EpochDedupe::<HashActive>::new(2048, fpr(8), false), 2048, 99);
    h.run(30_000);
    let mut h = Harness::new(EpochDedupe::<HashActive>::new(2048, fpr(8), true), 2048, 111);
    h.run(30_000);
    let mut h = Harness::new(EpochDedupe::<IcebergActive>::new(2048, fpr(8), true), 2048, 222);
    h.run(30_000);
}

// --------------------------------------------------- boundary exactness

/// A key committed at p is a duplicate at durable end w iff p >= w - W:
/// probe one position before, exactly at, and one after the boundary, with
/// the durable end advancing MONOTONICALLY (as it does in reality) via
/// filler traffic so epochs freeze / the FIFO slides between probes.
fn boundary_case<I: DedupeIndex>(mut idx: I, span: u64) {
    let mut arena = Arena::new();
    let key = b"boundary-key";
    let p = 100u64;
    let ptr = arena.append(Scope::Global, key, p);
    idx.insert(Scope::Global, key, p, ptr);

    fn advance<I: DedupeIndex>(idx: &mut I, arena: &mut Arena, from: &mut u64, upto: u64) {
        while *from < upto {
            let f = from.to_be_bytes();
            let fptr = arena.append(Scope::Stream(9), &f, *from);
            idx.insert(Scope::Stream(9), &f, *from, fptr);
            *from += 3;
        }
        idx.flush();
    }
    let mut fill = p + 1;

    // w = p + span - 1 -> lo = p - 1 -> duplicate (before boundary).
    advance(&mut idx, &mut arena, &mut fill, p + span - 1);
    assert_eq!(idx.check(Scope::Global, key, p + span - 1, &arena), Some(p), "before boundary");
    // w = p + span     -> lo = p     -> STILL a duplicate (p >= lo).
    advance(&mut idx, &mut arena, &mut fill, p + span);
    assert_eq!(idx.check(Scope::Global, key, p + span, &arena), Some(p), "exactly at boundary");
    // w = p + span + 1 -> lo = p + 1 -> expired; a retry may append.
    advance(&mut idx, &mut arena, &mut fill, p + span + 1);
    assert_eq!(idx.check(Scope::Global, key, p + span + 1, &arena), None, "after boundary");

    // And a retry after expiry re-arms the window at the new position.
    let p2 = fill + 10;
    let ptr2 = arena.append(Scope::Global, key, p2);
    idx.insert(Scope::Global, key, p2, ptr2);
    idx.flush();
    assert_eq!(idx.check(Scope::Global, key, p2 + 1, &arena), Some(p2), "retry re-arms");
}

#[test]
fn boundary_exactness_all_candidates() {
    let span = 1000;
    boundary_case(EpochDedupe::<HashActive>::new(span, fpr(128), false), span);
    boundary_case(EpochDedupe::<HashActive>::new(span, fpr(128), true), span);
    boundary_case(EpochDedupe::<IcebergActive>::new(span, fpr(128), true), span);
    boundary_case(FjallDedupe::open(tmpdir("boundary"), span, 1, true), span);
}

// ------------------------------------------------ epoch straddles boundary

#[test]
fn epoch_straddling_boundary_rejected_by_position_not_eviction() {
    let span = 800u64; // epoch span = 100
    for filt in [false, true] {
        let mut idx = EpochDedupe::<HashActive>::new(span, fpr(128), filt);
        let mut arena = Arena::new();
        for p in 0..1050u64 {
            let k = p.to_le_bytes();
            let ptr = arena.append(Scope::Global, &k, p);
            idx.insert(Scope::Global, &k, p, ptr);
        }
        let w = 1050u64; // lo = 250: epoch [200,300) STRADDLES the boundary
        let (fmin, fmax) = idx.oldest_live_epoch_range().unwrap();
        assert!(fmin < 250 && fmax >= 250, "straddling epoch retained: [{fmin},{fmax}]");
        // Stale side of the retained epoch: rejected by the position check.
        assert_eq!(idx.check(Scope::Global, &249u64.to_le_bytes(), w, &arena), None);
        assert_eq!(idx.check(Scope::Global, &200u64.to_le_bytes(), w, &arena), None);
        // Live side of the SAME epoch: still found.
        assert_eq!(idx.check(Scope::Global, &250u64.to_le_bytes(), w, &arena), Some(250));
        assert_eq!(idx.check(Scope::Global, &299u64.to_le_bytes(), w, &arena), Some(299));
        // Fully-expired epochs were reclaimed wholesale, with zero per-key
        // deletes.
        assert!(idx.epochs_dropped() > 0);
        assert_eq!(idx.deletes_issued(), 0);
    }
}

// -------------------------------------------------- rebuild from arena

/// Checkpoint-loss case (design.md §13.5): drop ALL index state, rebuild
/// from the (position, key) arena suffix within the window, and get
/// identical answers. Requires the review-D3 retention invariant (the log
/// suffix covering the window must still exist).
#[test]
fn rebuild_from_arena_epoch_candidates() {
    for (filt, iceberg) in [(false, false), (true, false), (true, true)] {
        let span = 2048u64;
        let seed = 0xD00D + filt as u64 + 2 * iceberg as u64;
        if iceberg {
            let idx = EpochDedupe::<IcebergActive>::new(span, fpr(128), filt);
            let mut h = Harness::new(idx, span, seed);
            h.run(15_000);
            let w = h.next_pos;
            let rebuilt = EpochDedupe::<IcebergActive>::rebuild_from_arena(
                &h.arena,
                w,
                span,
                fpr(128),
                filt,
            );
            audit_rebuild(rebuilt, &mut h, w);
        } else {
            let idx = EpochDedupe::<HashActive>::new(span, fpr(128), filt);
            let mut h = Harness::new(idx, span, seed);
            h.run(15_000);
            let w = h.next_pos;
            let rebuilt =
                EpochDedupe::<HashActive>::rebuild_from_arena(&h.arena, w, span, fpr(128), filt);
            audit_rebuild(rebuilt, &mut h, w);
        }
    }
}

fn audit_rebuild<I: DedupeIndex, J: DedupeIndex>(mut rebuilt: J, h: &mut Harness<I>, w: u64) {
    for i in 0..h.pool.len() {
        let (scope, key) = h.pool[i].clone();
        let want = h.refm.check(scope, &key, w);
        let orig = h.idx.check(scope, &key, w, &h.arena);
        let got = rebuilt.check(scope, &key, w, &h.arena);
        assert_eq!(orig, want, "original diverged");
        assert_eq!(got, want, "rebuilt diverged for keylen={}", key.len());
    }
}

#[test]
fn rebuild_from_arena_g0() {
    let span = 2048u64;
    let idx = FjallDedupe::open(tmpdir("rebuild_g0_a"), span, 1, true);
    let mut h = Harness::new(idx, span, 0xBEEF);
    h.run(8_000);
    let w = h.next_pos;
    let mut rebuilt = FjallDedupe::open(tmpdir("rebuild_g0_b"), span, 1, true);
    let lo = w.saturating_sub(span);
    for (ptr, pos, scope, key) in h.arena.iter() {
        if pos >= lo {
            rebuilt.insert(scope, key, pos, ptr);
        }
    }
    rebuilt.flush();
    audit_rebuild(rebuilt, &mut h, w);
}

// ------------------------------------------- key lengths, scopes, prefixes

#[test]
fn key_lengths_0_to_4k_and_scope_separation() {
    let span = 10_000u64;
    fn run(idx: &mut dyn DynIdx) {
        let mut arena = Arena::new();
        let lens = [0usize, 1, 2, 8, 16, 255, 1024, 4096];
        let mut pos = 0u64;
        for (i, &len) in lens.iter().enumerate() {
            let key = vec![0xA5u8 ^ i as u8; len];
            let ptr = arena.append(Scope::Stream(7), &key, pos);
            idx.insert_dyn(Scope::Stream(7), &key, pos, ptr);
            pos += 10;
        }
        idx.flush_dyn();
        let w = pos + 5;
        for (i, &len) in lens.iter().enumerate() {
            let key = vec![0xA5u8 ^ i as u8; len];
            // Present in its own scope...
            assert_eq!(
                idx.check_dyn(Scope::Stream(7), &key, w, &arena),
                Some(i as u64 * 10),
                "len={len}"
            );
            // ...and INVISIBLE from every other scope, same bytes.
            assert_eq!(idx.check_dyn(Scope::Global, &key, w, &arena), None);
            assert_eq!(idx.check_dyn(Scope::Stream(8), &key, w, &arena), None);
        }
    }
    run(&mut EpochDedupe::<HashActive>::new(span, fpr(128), false));
    run(&mut EpochDedupe::<HashActive>::new(span, fpr(128), true));
    run(&mut EpochDedupe::<IcebergActive>::new(span, fpr(128), true));
    run(&mut FjallDedupe::open(tmpdir("lens"), span, 1, true));
}

/// Object-safe shim so the length/scope test can drive every candidate with
/// one closure (DedupeIndex has generic-free methods but `impl Trait` args
/// elsewhere keep it non-dyn-safe-free; this avoids duplicating the body).
trait DynIdx {
    fn insert_dyn(&mut self, s: Scope, k: &[u8], p: u64, ptr: u64);
    fn check_dyn(&mut self, s: Scope, k: &[u8], w: u64, a: &Arena) -> Option<u64>;
    fn flush_dyn(&mut self);
}

impl<I: DedupeIndex> DynIdx for I {
    fn insert_dyn(&mut self, s: Scope, k: &[u8], p: u64, ptr: u64) {
        self.insert(s, k, p, ptr)
    }
    fn check_dyn(&mut self, s: Scope, k: &[u8], w: u64, a: &Arena) -> Option<u64> {
        self.check(s, k, w, a)
    }
    fn flush_dyn(&mut self) {
        self.flush()
    }
}

#[test]
fn adversarial_same_prefix_keys() {
    // 4 KiB keys sharing a 4088-byte prefix, differing only in the tail:
    // fingerprinting must not truncate, and full-key verification must
    // compare to the end. Run at full width and at forced 16-bit width.
    let span = 100_000u64;
    for bits in [128u32, 16] {
        for iceberg in [false, true] {
            let mut arena = Arena::new();
            let prefix = vec![0x42u8; 4088];
            let mk = |i: u64| {
                let mut k = prefix.clone();
                k.extend_from_slice(&i.to_le_bytes());
                k
            };
            let run = |idx: &mut dyn DynIdx, arena: &mut Arena| {
                for i in 0..200u64 {
                    let k = mk(i);
                    let ptr = arena.append(Scope::Global, &k, i);
                    idx.insert_dyn(Scope::Global, &k, i, ptr);
                }
                idx.flush_dyn();
                let w = 250u64;
                for i in 0..200u64 {
                    assert_eq!(idx.check_dyn(Scope::Global, &mk(i), w, arena), Some(i));
                }
                for i in 200..400u64 {
                    assert_eq!(idx.check_dyn(Scope::Global, &mk(i), w, arena), None);
                }
            };
            if iceberg {
                run(&mut EpochDedupe::<IcebergActive>::new(span, fpr(bits), true), &mut arena);
            } else {
                run(&mut EpochDedupe::<HashActive>::new(span, fpr(bits), true), &mut arena);
            }
        }
    }
    // G0 stores full keys directly; prefix adversaries stress the LSM
    // comparator instead. Once, at full width.
    let mut arena = Arena::new();
    let mut idx = FjallDedupe::open(tmpdir("prefix_g0"), span, 1, true);
    let prefix = vec![0x42u8; 4088];
    for i in 0..100u64 {
        let mut k = prefix.clone();
        k.extend_from_slice(&i.to_le_bytes());
        let ptr = arena.append(Scope::Global, &k, i);
        idx.insert(Scope::Global, &k, i, ptr);
    }
    idx.flush();
    for i in 0..100u64 {
        let mut k = prefix.clone();
        k.extend_from_slice(&i.to_le_bytes());
        assert_eq!(idx.check(Scope::Global, &k, 150, &arena), Some(i));
    }
}
