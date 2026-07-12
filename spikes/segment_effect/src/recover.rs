//! The recovery variants of research/05 §8. All must produce identical
//! canonical state digests on valid histories.
//!
//! ```text
//! 1 full sequential capsule-scan fold      (the boring-BTreeMap oracle)
//! 2 per-segment effect build + sequential ordered apply (through the
//!   canonical encode/decode roundtrip)
//! 3 parallel effect build + ordered tree reduce (operands NEVER permuted)
//! 4 full checkpoint + suffix fold
//! 5 incremental dirty-page checkpoints + suffix fold
//! 6 corrupt/missing effect -> rescan that segment only, rest from effects
//! 7 corrupt/missing page/manifest -> older checkpoint or effects
//! ```

use hashbrown::HashSet;

use crate::builder::{BuildErr, build_effect};
use crate::hist::{self, Rng};
use crate::checkpoint::{self, Dir, MemDir, Manifest};
use crate::effect::{ApplyErr, ComposeErr, DecodeErr, SegmentEffect, compose};
use crate::kernel::KernelState;
use crate::model::{Invalid, Log};
use crate::oracle::OracleState;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoverErr {
    Fold { at: u64, why: Invalid },
    Build { segment: u64, err: BuildErr },
    Compose(ComposeErr),
    Apply { segment: u64, err: ApplyErr },
    Decode(DecodeErr),
    NoCheckpoint,
}

/// Ordered applier with effect-identity replay dedupe (research/02 §12):
/// re-applying an ALREADY-APPLIED effect is an idempotent no-op; applying
/// an unknown effect that lands behind the cursor is `⊥`.
#[derive(Default)]
pub struct OrderedApplier {
    seen: HashSet<[u8; 32]>,
}

impl OrderedApplier {
    /// Returns `Ok(true)` if applied, `Ok(false)` if skipped as a known
    /// duplicate.
    pub fn apply(
        &mut self,
        st: &mut KernelState,
        e: &SegmentEffect,
    ) -> Result<bool, ApplyErr> {
        let h = e.hash();
        if e.last.idx <= st.cursor.idx {
            if self.seen.contains(&h) {
                return Ok(false);
            }
            return Err(ApplyErr::CursorMismatch {
                expected: st.cursor,
                got: e.first,
            });
        }
        st.apply_effect(e)?;
        self.seen.insert(h);
        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// Variant 1: the oracle
// ---------------------------------------------------------------------------

pub fn v1_oracle(log: &Log) -> Result<[u8; 32], RecoverErr> {
    let mut st = OracleState::new(log.dedupe_span);
    st.fold(&log.capsules)
        .map_err(|(i, why)| RecoverErr::Fold { at: i as u64, why })?;
    Ok(st.digest())
}

// ---------------------------------------------------------------------------
// Effect building
// ---------------------------------------------------------------------------

/// Build every segment's effect sequentially.
pub fn build_all(log: &Log) -> Result<Vec<SegmentEffect>, RecoverErr> {
    log.segments
        .iter()
        .enumerate()
        .map(|(i, meta)| {
            build_effect(
                log.segment_capsules(i),
                meta,
                log.dedupe_span,
                log.epoch_span,
            )
            .map_err(|err| RecoverErr::Build { segment: meta.segment_id, err })
        })
        .collect()
}

/// Build every segment's effect on `threads` OS threads (map phase —
/// independent per segment, research/02 §10.1). Order is preserved by
/// chunk position, never by completion time.
pub fn build_all_parallel(
    log: &Log,
    threads: usize,
) -> Result<Vec<SegmentEffect>, RecoverErr> {
    let n = log.segments.len();
    let threads = threads.max(1).min(n.max(1));
    let chunk = n.div_ceil(threads);
    let results: Vec<Result<Vec<SegmentEffect>, RecoverErr>> =
        std::thread::scope(|s| {
            let mut handles = Vec::new();
            for t in 0..threads {
                let lo = t * chunk;
                let hi = ((t + 1) * chunk).min(n);
                handles.push(s.spawn(move || {
                    let mut out = Vec::with_capacity(hi - lo);
                    for i in lo..hi {
                        let meta = &log.segments[i];
                        out.push(
                            build_effect(
                                log.segment_capsules(i),
                                meta,
                                log.dedupe_span,
                                log.epoch_span,
                            )
                            .map_err(|err| RecoverErr::Build {
                                segment: meta.segment_id,
                                err,
                            })?,
                        );
                    }
                    Ok(out)
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
    let mut out = Vec::with_capacity(n);
    for r in results {
        out.extend(r?);
    }
    Ok(out)
}

/// Balanced ordered tree reduction. Splits by POSITION, so operands are
/// never permuted (right-biased override is not commutative).
pub fn reduce_ordered_tree(
    effects: &[SegmentEffect],
) -> Result<SegmentEffect, ComposeErr> {
    assert!(!effects.is_empty());
    if effects.len() == 1 {
        return Ok(effects[0].clone());
    }
    let mid = effects.len() / 2;
    let a = reduce_ordered_tree(&effects[..mid])?;
    let b = reduce_ordered_tree(&effects[mid..])?;
    compose(&a, &b)
}

/// Parallel ordered tree reduction: contiguous chunks reduce on separate
/// threads, then the per-chunk partials compose left-to-right.
pub fn reduce_ordered_parallel(
    effects: &[SegmentEffect],
    threads: usize,
) -> Result<SegmentEffect, ComposeErr> {
    assert!(!effects.is_empty());
    let n = effects.len();
    let threads = threads.max(1).min(n);
    let chunk = n.div_ceil(threads);
    let partials: Vec<Result<SegmentEffect, ComposeErr>> =
        std::thread::scope(|s| {
            let mut handles = Vec::new();
            for t in 0..threads {
                let lo = t * chunk;
                let hi = ((t + 1) * chunk).min(n);
                if lo >= hi {
                    continue;
                }
                handles
                    .push(s.spawn(move || reduce_ordered_tree(&effects[lo..hi])));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
    let mut acc: Option<SegmentEffect> = None;
    for p in partials {
        let p = p?;
        acc = Some(match acc {
            None => p,
            Some(a) => compose(&a, &p)?,
        });
    }
    Ok(acc.unwrap())
}

// ---------------------------------------------------------------------------
// Variant 2: sequential effect apply (through the codec roundtrip)
// ---------------------------------------------------------------------------

pub fn v2_effects_sequential(log: &Log) -> Result<[u8; 32], RecoverErr> {
    let effects = build_all(log)?;
    let mut st = KernelState::new(log.dedupe_span, log.epoch_span);
    let mut ap = OrderedApplier::default();
    for e in &effects {
        // Exercise the canonical encoding: what recovery would read from a
        // SealPack sidecar.
        let bytes = e.encode();
        let d = SegmentEffect::decode(&bytes).map_err(RecoverErr::Decode)?;
        debug_assert_eq!(&d, e);
        ap.apply(&mut st, &d).map_err(|err| RecoverErr::Apply {
            segment: e.first_segment,
            err,
        })?;
    }
    Ok(st.digest())
}

// ---------------------------------------------------------------------------
// Variant 3: parallel build + ordered tree reduce
// ---------------------------------------------------------------------------

pub fn v3_effects_parallel(
    log: &Log,
    threads: usize,
) -> Result<[u8; 32], RecoverErr> {
    let effects = build_all_parallel(log, threads)?;
    if effects.is_empty() {
        let st = KernelState::new(log.dedupe_span, log.epoch_span);
        return Ok(st.digest());
    }
    let one = reduce_ordered_parallel(&effects, threads)
        .map_err(RecoverErr::Compose)?;
    let mut st = KernelState::new(log.dedupe_span, log.epoch_span);
    st.apply_effect(&one).map_err(|err| RecoverErr::Apply {
        segment: one.first_segment,
        err,
    })?;
    Ok(st.digest())
}

// ---------------------------------------------------------------------------
// Variants 4/5: checkpoint + suffix fold
// ---------------------------------------------------------------------------

/// Fold capsules up to segment boundary `seg_cut` (exclusive segment index),
/// install a FULL checkpoint into `dir`, then forget the state, §10.4-open
/// it back and fold the suffix.
pub fn v4_checkpoint_full(
    log: &Log,
    dir: &mut MemDir,
    seg_cut: usize,
) -> Result<[u8; 32], RecoverErr> {
    let cut = seg_cut.min(log.segments.len());
    let upto = if cut == 0 { 0 } else { log.segments[cut - 1].hi };
    let mut st = KernelState::new(log.dedupe_span, log.epoch_span);
    st.fold(&log.capsules[..upto])
        .map_err(|(i, why)| RecoverErr::Fold { at: i as u64, why })?;
    checkpoint::install(dir, &st, None).expect("install (no injection)");
    drop(st);
    open_and_fold_suffix(dir, log)
}

/// Open the best checkpoint in `dir` and fold the capsule suffix after it.
pub fn open_and_fold_suffix(
    dir: &impl Dir,
    log: &Log,
) -> Result<[u8; 32], RecoverErr> {
    let (mut st, m) = checkpoint::open(dir, log).ok_or(RecoverErr::NoCheckpoint)?;
    debug_assert_eq!(st.cursor, m.cursor);
    let suffix = &log.capsules[m.cursor.idx as usize..];
    st.fold(suffix).map_err(|(i, why)| RecoverErr::Fold {
        at: m.cursor.idx + i as u64,
        why,
    })?;
    Ok(st.digest())
}

/// Multiple checkpoints: a full one at the first boundary, then INCREMENTAL
/// dirty-page checkpoints at each later boundary, then open + suffix fold.
/// Returns the digest plus per-install written-blob-bytes (the
/// proportionality measurement).
pub fn v5_checkpoint_incremental(
    log: &Log,
    dir: &mut MemDir,
    seg_cuts: &[usize],
) -> Result<([u8; 32], Vec<u64>), RecoverErr> {
    let mut st = KernelState::new(log.dedupe_span, log.epoch_span);
    let mut folded = 0usize;
    let mut prev: Option<Manifest> = None;
    let mut written = Vec::new();
    for &cut in seg_cuts {
        let cut = cut.min(log.segments.len());
        if cut == 0 {
            continue;
        }
        let upto = log.segments[cut - 1].hi;
        if upto < folded {
            continue;
        }
        st.fold(&log.capsules[folded..upto])
            .map_err(|(i, why)| RecoverErr::Fold { at: (folded + i) as u64, why })?;
        folded = upto;
        let inst = checkpoint::install(dir, &st, prev.as_ref())
            .expect("install (no injection)");
        written.push(inst.blob_bytes_written);
        prev = Some(inst.manifest);
        st.clear_dirty();
    }
    drop(st);
    let digest = open_and_fold_suffix(dir, log)?;
    Ok((digest, written))
}

// ---------------------------------------------------------------------------
// Variant 6: corrupt/missing effect -> rescan that segment only
// ---------------------------------------------------------------------------

/// Serialized-effect store where entry `victim` is corrupted (byte flip) or
/// missing; recovery decodes each effect, and on ANY decode failure or
/// missing sidecar rebuilds that one segment's effect by scanning its
/// capsules. Returns the digest plus how many segments were rescanned.
pub fn v6_effect_fallback(
    log: &Log,
    victim: usize,
    remove: bool,
) -> Result<([u8; 32], usize), RecoverErr> {
    let effects = build_all(log)?;
    let mut sidecars: Vec<Option<Vec<u8>>> =
        effects.iter().map(|e| Some(e.encode())).collect();
    if !sidecars.is_empty() {
        let v = victim % sidecars.len();
        if remove {
            sidecars[v] = None;
        } else if let Some(bytes) = &mut sidecars[v] {
            let mid = bytes.len() / 2;
            bytes[mid] ^= 0x40;
        }
    }
    let mut st = KernelState::new(log.dedupe_span, log.epoch_span);
    let mut ap = OrderedApplier::default();
    let mut rescanned = 0usize;
    for (i, sc) in sidecars.iter().enumerate() {
        let decoded = sc.as_ref().and_then(|b| SegmentEffect::decode(b).ok());
        let e = match decoded {
            Some(e) => e,
            None => {
                // Local repair (research/02 §15): scan ONLY this segment.
                rescanned += 1;
                build_effect(
                    log.segment_capsules(i),
                    &log.segments[i],
                    log.dedupe_span,
                    log.epoch_span,
                )
                .map_err(|err| RecoverErr::Build {
                    segment: log.segments[i].segment_id,
                    err,
                })?
            }
        };
        ap.apply(&mut st, &e).map_err(|err| RecoverErr::Apply {
            segment: log.segments[i].segment_id,
            err,
        })?;
    }
    Ok((st.digest(), rescanned))
}

// ---------------------------------------------------------------------------
// Variant 7: corrupt/missing page or manifest -> older checkpoint / effects
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub enum PageCorruption {
    /// Flip a byte in some referenced page blob of the NEWEST manifest.
    WrongPageBytes,
    /// Remove some referenced page blob of the newest manifest.
    MissingPage,
    /// Flip a byte inside the newest manifest file itself.
    TornManifest,
}

/// Install checkpoints at two boundaries, corrupt the newest one as
/// requested, then open. §10.4 must select the OLDER manifest (or nothing),
/// and the digest must still match the oracle. Returns the digest and the
/// cursor idx of the manifest actually used (None = fell back to effects).
pub fn v7_page_fallback(
    log: &Log,
    dir: &mut MemDir,
    cut_old: usize,
    cut_new: usize,
    what: PageCorruption,
) -> Result<([u8; 32], Option<u64>), RecoverErr> {
    let mut st = KernelState::new(log.dedupe_span, log.epoch_span);
    let mut folded = 0usize;
    let mut installed: Vec<Manifest> = Vec::new();
    for &cut in &[cut_old, cut_new] {
        let cut = cut.min(log.segments.len());
        if cut == 0 {
            continue;
        }
        let upto = log.segments[cut - 1].hi;
        if upto < folded {
            continue;
        }
        st.fold(&log.capsules[folded..upto])
            .map_err(|(i, why)| RecoverErr::Fold { at: (folded + i) as u64, why })?;
        folded = upto;
        let inst = checkpoint::install(dir, &st, installed.last())
            .expect("install");
        st.clear_dirty();
        installed.push(inst.manifest);
    }
    drop(st);
    let page_file = |h: &[u8; 32]| {
        let mut s = String::from("page-");
        for b in h {
            s.push_str(&format!("{b:02x}"));
        }
        s.push_str(".page");
        s
    };
    if let Some(m) = installed.last() {
        match what {
            PageCorruption::WrongPageBytes | PageCorruption::MissingPage => {
                // Prefer a page the older manifest does NOT also reference
                // (so the older one can survive as the fallback); if every
                // page is shared, take the first — then the fallback is the
                // effects path.
                let old_pages: hashbrown::HashSet<[u8; 32]> = installed
                    .iter()
                    .rev()
                    .skip(1)
                    .flat_map(|om| om.pages.iter().map(|&(_, _, h)| h))
                    .collect();
                let h = m
                    .pages
                    .iter()
                    .map(|&(_, _, h)| h)
                    .find(|h| !old_pages.contains(h))
                    .or_else(|| m.pages.first().map(|&(_, _, h)| h))
                    .expect("manifest has pages");
                let name = page_file(&h);
                match what {
                    PageCorruption::MissingPage => {
                        let _ = dir.remove(&name);
                    }
                    _ => {
                        if let Some(mut bytes) = dir.read(&name) {
                            let mid = bytes.len() / 2;
                            bytes[mid] ^= 0x01;
                            dir.write_file(&name, &bytes).unwrap();
                        }
                    }
                }
            }
            PageCorruption::TornManifest => {
                let name = m.name();
                if let Some(mut bytes) = dir.read(&name) {
                    let mid = bytes.len() / 2;
                    bytes[mid] ^= 0x01;
                    dir.write_file(&name, &bytes).unwrap();
                }
            }
        }
    }
    match checkpoint::open(dir, log) {
        Some((mut st, m)) => {
            let suffix = &log.capsules[m.cursor.idx as usize..];
            st.fold(suffix).map_err(|(i, why)| RecoverErr::Fold {
                at: m.cursor.idx + i as u64,
                why,
            })?;
            Ok((st.digest(), Some(m.cursor.idx)))
        }
        None => {
            // No usable checkpoint at all: fall back to effects.
            let d = v2_effects_sequential(log)?;
            Ok((d, None))
        }
    }
}

// ---------------------------------------------------------------------------
// The differential check: one seeded history through ALL SEVEN variants
// ---------------------------------------------------------------------------

/// Run all seven recovery variants on one seeded VALID history; panic on
/// any digest divergence. Returns `((v6 rescans, v7 effects-fallbacks),
/// capsule count)`. This is the corpus gate's unit of work, shared by the
/// bench driver and the test suite.
pub fn differential_check(seed: u64) -> ((u64, u64), u64) {
    let h = hist::generate(seed);
    let log = &h.log;
    let n_caps = log.capsules.len() as u64;
    let mut rng = Rng::new(seed ^ 0xc0ffee);
    let nseg = log.segments.len();

    let d1 = v1_oracle(log).expect("v1 oracle rejected a valid history");
    let d2 = v2_effects_sequential(log).expect("v2");
    assert_eq!(d1, d2, "seed {seed}: v2 != oracle");
    let d3 = v3_effects_parallel(log, 2).expect("v3");
    assert_eq!(d1, d3, "seed {seed}: v3 != oracle");

    // v4: full checkpoint at a random boundary, suffix fold.
    let cut = (rng.next() as usize) % (nseg + 1);
    let mut dir = MemDir::new();
    let d4 = v4_checkpoint_full(log, &mut dir, cut).expect("v4");
    assert_eq!(d1, d4, "seed {seed}: v4 != oracle (cut {cut})");

    // v5: incremental checkpoints at 2-3 ascending nonzero boundaries
    // (a genesis-cut checkpoint is v4's job).
    let mut cuts: Vec<usize> = (0..rng.range(2, 3))
        .map(|_| 1 + (rng.next() as usize) % nseg)
        .collect();
    cuts.sort_unstable();
    let mut dir = MemDir::new();
    let (d5, _) = v5_checkpoint_incremental(log, &mut dir, &cuts).expect("v5");
    assert_eq!(d1, d5, "seed {seed}: v5 != oracle (cuts {cuts:?})");

    // v6: corrupt or remove one effect sidecar -> rescan that segment only.
    let victim = (rng.next() as usize) % nseg.max(1);
    let remove = rng.chance(50);
    let (d6, rescanned) = v6_effect_fallback(log, victim, remove).expect("v6");
    assert_eq!(d1, d6, "seed {seed}: v6 != oracle");
    assert_eq!(rescanned, 1, "seed {seed}: v6 must rescan exactly the victim");

    // v7: corrupt the newest checkpoint page/manifest -> older manifest or
    // effects fallback.
    let mut cuts = [
        (rng.next() as usize) % (nseg + 1),
        (rng.next() as usize) % (nseg + 1),
    ];
    cuts.sort_unstable();
    let what = match rng.next() % 3 {
        0 => PageCorruption::WrongPageBytes,
        1 => PageCorruption::MissingPage,
        _ => PageCorruption::TornManifest,
    };
    let mut dir = MemDir::new();
    let (d7, used) =
        v7_page_fallback(log, &mut dir, cuts[0], cuts[1], what).expect("v7");
    assert_eq!(d1, d7, "seed {seed}: v7 != oracle ({what:?})");
    let fell_back = u64::from(used.is_none());

    ((rescanned as u64, fell_back), n_caps)
}
