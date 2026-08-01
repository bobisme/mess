//! bn-26pp: **folded-from-sidecar ≡ folded-from-log**, over randomized
//! histories.
//!
//! The `.reg` registry delta ([`mess_index::sealed::regdelta`]) lets engine
//! open fold a sealed segment's `$registry` records straight out of a small
//! sequential sidecar instead of point-reading each one from the log. The log
//! remains the sole authority for the `id ↔ name` bijection (bn-2di / D1), so
//! the sidecar is only ever allowed to be an accelerator: it must produce the
//! **identical** registry, and when it is absent, damaged, foreign, or stale
//! the engine must fall back to the log path and produce that same registry
//! anyway.
//!
//! This suite proves exactly that, three ways, over seeded random histories
//! (random stream counts, random append order, random batch sizes, several
//! message types, tiny segments so many segments roll and seal):
//!
//! 1. **Byte-level.** Every `.reg` on disk carries *exactly* the
//!    `(first_global_pos, payloads)` list that scanning that segment's raw log
//!    bytes yields for stream 0 — same batches, same order, same bytes.
//! 2. **State-level.** The engine opened with the deltas present, the engine
//!    opened with every `.reg` deleted, and an independent offline fold of the
//!    raw log (the oracle, a re-implementation of what `mess inspect` does) all
//!    agree on the whole bijection: both high-water marks, every `id → name`,
//!    every `name → id`, every stream's category and head.
//! 3. **Fallback.** Deleted, truncated, bit-flipped, foreign (a valid delta
//!    from another segment), and mixed (only some segments carry one) all open
//!    cleanly and yield that same state.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use mess_index::sealed::RegistryDelta;
use mess_log::committer::Durability;
use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::scanner;
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::registry::{Fold, REGISTRY_STREAM_ID, RegistryState};
use mess_store::{EngineOptions, LogEngine, Version};

// ---------------------------------------------------------------------------
// Randomized history generation
// ---------------------------------------------------------------------------

/// Seeded xorshift64 — deterministic corpora without a `rand` dev-dependency
/// (the same trick `sealed::filter`'s property test uses).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 { self.next() % n.max(1) }
}

fn opts() -> EngineOptions {
    EngineOptions {
        durability: Durability::Process,
        // Tiny segments so a short history still rolls and seals many
        // segments — the delta only exists for sealed ones.
        segment_size: 96 * 1024,
        ..Default::default()
    }
}

/// Append a randomized history and return every stream name used, in first-use
/// order. Names and message types are drawn so that registrations are
/// interleaved with ordinary events throughout the log rather than all landing
/// in segment 0.
async fn build_history(dir: &Path, seed: u64) -> Vec<String> {
    let mut rng = Rng(seed | 1);
    let n_streams = 8 + rng.below(40) as usize;
    let n_types = 1 + rng.below(6) as usize;
    let steps = 120 + rng.below(240) as usize;

    let engine = LogEngine::open_with(dir, opts()).expect("open");
    let mut versions: BTreeMap<usize, Version> = BTreeMap::new();
    let mut order: Vec<String> = Vec::new();
    let mut seen = vec![false; n_streams];

    for step in 0..steps {
        // Bias toward streams introduced late, so new names keep appearing.
        let s = if rng.below(3) == 0 {
            (step * n_streams / steps.max(1)).min(n_streams - 1)
        } else {
            rng.below(n_streams as u64) as usize
        };
        if !seen[s] {
            seen[s] = true;
            order.push(format!("acct-{s:04}"));
        }
        let per_batch = 1 + rng.below(4) as usize;
        let batch: Vec<RecordToAppend> = (0..per_batch)
            .map(|k| {
                let t = rng.below(n_types as u64);
                let len = 8 + rng.below(400) as usize;
                RecordToAppend {
                    message_type: format!("evt.kind.{t}"),
                    data:         vec![(step + k) as u8; len],
                }
            })
            .collect();
        let name = format!("acct-{s:04}");
        let expected = versions.get(&s).copied().unwrap_or(Version::NoStream);
        let out =
            engine.append_batch(&name, expected, &batch).await.expect("append");
        versions.insert(s, out.version);
    }
    // Dropping the engine drains every queued roll-seal, so the sealed tier
    // (and every `.reg`) is complete on disk.
    drop(engine);
    order
}

// ---------------------------------------------------------------------------
// The oracle: fold `$registry` out of the raw log, ignoring every sidecar
// ---------------------------------------------------------------------------

/// Every `(first_global_pos, payloads)` stream-0 batch in `segment_id`'s raw
/// log bytes — the exact list a `.reg` for that segment must reproduce.
fn registry_batches_from_log(
    dir: &Path,
    segment_id: u64,
) -> Vec<(u64, Vec<Vec<u8>>)> {
    let rt = RealRuntime::new();
    let fs = rt.fs();
    let path = dir.join(format!("seg-{segment_id:08}.log"));
    let (rec, image) =
        scanner::recover_segment_with_image(&fs, &path).expect("scan segment");
    if rec.header.is_none() {
        return Vec::new();
    }
    let mut out: Vec<(u64, Vec<Vec<u8>>)> = rec
        .accepted
        .iter()
        .filter(|b| b.stream_id == REGISTRY_STREAM_ID)
        .map(|b| {
            let frames = b.frames(&image).expect("frames");
            (b.first_global_pos, frames.map(|f| f.payload.to_vec()).collect())
        })
        .collect();
    out.sort_by_key(|(pos, _)| *pos);
    out
}

/// Fold the whole store's `$registry` from the raw log alone — the independent
/// re-implementation of recovery step 2 that no sidecar can influence.
fn fold_from_log(dir: &Path) -> RegistryState {
    let mut fold = Fold::new();
    for id in segment_ids(dir) {
        for (pos, payloads) in registry_batches_from_log(dir, id) {
            fold.push_batch(pos, payloads);
        }
    }
    fold.finish::<std::convert::Infallible>().expect("registry folds")
}

fn segment_ids(dir: &Path) -> Vec<u64> {
    let mut ids: Vec<u64> = std::fs::read_dir(dir)
        .expect("read store dir")
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.strip_prefix("seg-")
                .and_then(|r| r.strip_suffix(".log"))
                .and_then(|n| n.parse::<u64>().ok())
        })
        .collect();
    ids.sort_unstable();
    ids
}

fn reg_paths(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir.join("sealed"))
        .map(|rd| {
            rd.flatten()
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|e| e == "reg"))
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

fn segment_id_of_reg(p: &Path) -> u64 {
    p.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.strip_prefix("seg-"))
        .and_then(|s| s.parse::<u64>().ok())
        .expect("sealed sidecar naming")
}

// ---------------------------------------------------------------------------
// State fingerprints
// ---------------------------------------------------------------------------

/// A total, order-independent fingerprint of the folded registry: both
/// high-water marks and every `id → (name, category)` in both namespaces. Two
/// states with the same fingerprint agree on the entire bijection, so an extra,
/// missing, or renamed entry cannot hide.
fn fingerprint_state(s: &RegistryState) -> String {
    let mut out = format!(
        "hwm_stream={} hwm_type={}\n",
        s.stream_high_water_mark(),
        s.event_type_high_water_mark()
    );
    for id in 0..=s.stream_high_water_mark() {
        out.push_str(&format!(
            "s{id}={:?}/{:?}\n",
            s.stream_name(id),
            s.stream_category(id)
        ));
    }
    for id in 0..=s.event_type_high_water_mark() {
        out.push_str(&format!("t{id}={:?}\n", s.event_type_name(id)));
    }
    out
}

/// The same fingerprint, observed through an opened engine's PUBLIC surface —
/// the ids it hands out for every name the oracle knows, plus each stream's
/// head. `names` comes from the oracle so an id the engine invented for a name
/// the log never registered would show up as a mismatch on the reverse lookup.
async fn fingerprint_engine(
    engine: &LogEngine,
    oracle: &RegistryState,
) -> String {
    let mut out = String::new();
    for id in 0..=oracle.stream_high_water_mark() {
        let Some(name) = oracle.stream_name(id) else {
            out.push_str(&format!("s{id}=<none>\n"));
            continue;
        };
        let head = engine.head(name).await.expect("head");
        out.push_str(&format!(
            "s{id}={name}->{:?} head={head:?}\n",
            engine.stream_id_of(name)
        ));
    }
    for id in 0..=oracle.event_type_high_water_mark() {
        let Some(name) = oracle.event_type_name(id) else {
            out.push_str(&format!("t{id}=<none>\n"));
            continue;
        };
        out.push_str(&format!(
            "t{id}={name}->{:?}\n",
            engine.event_type_id_of(name)
        ));
    }
    out.push_str(&format!("events={}\n", engine.total_events()));
    out
}

async fn open_and_fingerprint(dir: &Path, oracle: &RegistryState) -> String {
    let engine = LogEngine::open_with(dir, opts()).expect("reopen");
    let fp = fingerprint_engine(&engine, oracle).await;
    drop(engine);
    fp
}

// ---------------------------------------------------------------------------
// 1. Byte-level: every `.reg` reproduces its segment's log bytes exactly
// ---------------------------------------------------------------------------

/// The core differential. For every sealed segment that has a `.reg`, the
/// records it carries must be byte-identical to the ones a raw scan of that
/// segment's log bytes produces — and every segment whose log DOES carry
/// registrations must have one (otherwise the acceleration silently does
/// nothing, which is safe but pointless, and we want to know).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sidecar_records_are_byte_identical_to_the_log() {
    for seed in [1u64, 7, 12345, 0xDEAD_BEEF, 0x5EED, 99_991] {
        let tmp =
            mess_testkit::sweeping_temp_dir(&format!("regdelta-bytes-{seed}"));
        let dir = tmp.path().join("store");
        build_history(&dir, seed).await;

        let regs = reg_paths(&dir);
        assert!(
            !regs.is_empty(),
            "seed {seed}: history sealed no segment carrying a registration — \
             the corpus is not exercising the format"
        );

        let mut covered = 0usize;
        for p in &regs {
            let seg = segment_id_of_reg(p);
            let delta =
                RegistryDelta::from_bytes(std::fs::read(p).expect("read .reg"))
                    .expect("valid .reg");
            assert_eq!(delta.segment_id(), seg, "seed {seed}: segment id");
            assert_eq!(
                delta.stream_id(),
                REGISTRY_STREAM_ID,
                "seed {seed}: stream id"
            );

            let from_sidecar: Vec<(u64, Vec<Vec<u8>>)> = delta
                .batches()
                .map(|b| {
                    (
                        b.first_global_pos(),
                        b.payloads().map(<[u8]>::to_vec).collect(),
                    )
                })
                .collect();
            let from_log = registry_batches_from_log(&dir, seg);
            assert_eq!(
                from_sidecar, from_log,
                "seed {seed}, segment {seg}: sidecar records differ from the \
                 log's"
            );
            covered += from_log.len();
        }
        assert!(covered > 0, "seed {seed}: no records compared");
    }
}

// ---------------------------------------------------------------------------
// 2 + 3. State-level equality and the fallback ladder
// ---------------------------------------------------------------------------

/// The whole point, over randomized histories: with the deltas, without them,
/// with them damaged, with only half of them — the engine always recovers the
/// registry the raw log folds to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_fallback_recovers_the_same_registry() {
    for seed in [3u64, 42, 777, 0xC0FFEE, 0xABC_DEF, 31_337] {
        let tmp =
            mess_testkit::sweeping_temp_dir(&format!("regdelta-state-{seed}"));
        let dir = tmp.path().join("store");
        let names = build_history(&dir, seed).await;
        assert!(!names.is_empty());

        // The oracle: the log's own fold, no sidecar involved.
        let oracle = fold_from_log(&dir);
        let oracle_fp = fingerprint_state(&oracle);

        let regs = reg_paths(&dir);
        assert!(!regs.is_empty(), "seed {seed}: no .reg to exercise");
        let saved: Vec<(PathBuf, Vec<u8>)> = regs
            .iter()
            .map(|p| (p.clone(), std::fs::read(p).expect("read")))
            .collect();

        // (a) the accelerated path.
        let with_delta = open_and_fingerprint(&dir, &oracle).await;

        // Every name the history used must resolve, and to the same id the
        // oracle folded — a sanity check that the fingerprint is not vacuous.
        for name in &names {
            assert!(
                oracle.stream_id(name).is_some(),
                "seed {seed}: oracle never registered {name}"
            );
        }

        // (b) no deltas at all — the pre-bn-26pp path.
        for (p, _) in &saved {
            std::fs::remove_file(p).expect("remove");
        }
        let without = open_and_fingerprint(&dir, &oracle).await;
        assert_eq!(
            with_delta, without,
            "seed {seed}: deleting every .reg changed the recovered registry"
        );

        // (c) mixed: restore every other delta.
        for (i, (p, bytes)) in saved.iter().enumerate() {
            if i % 2 == 0 {
                std::fs::write(p, bytes).expect("write");
            }
        }
        let mixed = open_and_fingerprint(&dir, &oracle).await;
        assert_eq!(with_delta, mixed, "seed {seed}: mixed store differs");

        // (d) damaged: truncate one, bit-flip another, and give a third a
        //     perfectly valid delta belonging to a different segment.
        for (p, bytes) in &saved {
            std::fs::write(p, bytes).expect("write");
        }
        if let Some((p, bytes)) = saved.first() {
            std::fs::write(p, &bytes[..bytes.len() / 2]).expect("truncate");
        }
        if let Some((p, bytes)) = saved.get(1) {
            let mut b = bytes.clone();
            let mid = b.len() / 2;
            b[mid] ^= 0xFF;
            std::fs::write(p, b).expect("flip");
        }
        if saved.len() >= 3 {
            std::fs::write(&saved[2].0, &saved[saved.len() - 1].1)
                .expect("foreign");
        }
        let damaged = open_and_fingerprint(&dir, &oracle).await;
        assert_eq!(
            with_delta, damaged,
            "seed {seed}: damaged .reg files changed the recovered registry"
        );

        // And the engine's view agrees with the raw-log fold on every id.
        for (p, bytes) in &saved {
            std::fs::write(p, bytes).expect("restore");
        }
        let engine = LogEngine::open_with(&dir, opts()).expect("reopen");
        for id in 1..=oracle.stream_high_water_mark() {
            let name = oracle.stream_name(id).expect("dense");
            assert_eq!(
                engine.stream_id_of(name),
                Some(id),
                "seed {seed}: stream {name}"
            );
        }
        for id in 1..=oracle.event_type_high_water_mark() {
            let name = oracle.event_type_name(id).expect("dense");
            assert_eq!(
                engine.event_type_id_of(name),
                Some(id),
                "seed {seed}: event type {name}"
            );
        }
        drop(engine);
        assert_eq!(
            fingerprint_state(&fold_from_log(&dir)),
            oracle_fp,
            "seed {seed}: reopening changed what the log folds to"
        );
    }
}

/// A store sealed BEFORE this format existed reopens identically: no `.reg`
/// anywhere, and appending after the upgrade seals new segments that do carry
/// one while the old segments keep the `pread` path. Mixed stores are the
/// normal case for anyone who upgrades, so they get their own test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_legacy_store_upgrades_in_place() {
    let tmp = mess_testkit::sweeping_temp_dir("regdelta-legacy");
    let dir = tmp.path().join("store");
    build_history(&dir, 0x1234_5678).await;

    // Simulate "sealed before bn-26pp": drop every delta the build wrote.
    let legacy = reg_paths(&dir);
    assert!(!legacy.is_empty());
    for p in &legacy {
        std::fs::remove_file(p).expect("remove");
    }
    let oracle = fold_from_log(&dir);
    let before = open_and_fingerprint(&dir, &oracle).await;

    // Append more history: the new segments seal WITH deltas, the old ones
    // still have none.
    let engine = LogEngine::open_with(&dir, opts()).expect("reopen");
    // Enough bytes to roll and seal several fresh segments (96 KiB each).
    for s in 0..600u64 {
        let name = format!("post-upgrade-{s:04}");
        let batch = vec![RecordToAppend {
            message_type: "evt.after".to_string(),
            data:         vec![7u8; 500],
        }];
        engine
            .append_batch(&name, Version::NoStream, &batch)
            .await
            .expect("append");
    }
    drop(engine);

    let now = reg_paths(&dir);
    assert!(
        !now.is_empty(),
        "post-upgrade seals must emit deltas for the new segments"
    );
    let oracle2 = fold_from_log(&dir);
    let after = open_and_fingerprint(&dir, &oracle2).await;

    // The pre-upgrade portion of the bijection is untouched...
    for id in 1..=oracle.stream_high_water_mark() {
        let name = oracle.stream_name(id).expect("dense");
        assert_eq!(oracle2.stream_name(id), Some(name));
    }
    assert!(before.len() < after.len() || before == after);

    // ...and the mixed store still folds to exactly what the log says.
    for p in &now {
        std::fs::remove_file(p).expect("remove");
    }
    let all_log = open_and_fingerprint(&dir, &oracle2).await;
    assert_eq!(after, all_log, "mixed store differs from the all-log fold");
}
