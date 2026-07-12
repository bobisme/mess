//! `gen-real`: generate an ACTUAL store with the current engine — appends
//! across many streams with Zipf popularity through
//! `mess_store::LogEngine::append_batch` (the full production path: name
//! interning, per-stream append gate, committer, auto-roll), with a small
//! `segment_size` so segments roll repeatedly and the background roll-sealer
//! writes real `.pidx` sidecars. `seal_active()` at the end seals the final
//! live head too.
//!
//! Throughput note (spikes/seed_profile, bn-1jg): every NEW stream pays a
//! ~3.4 ms fjall `SyncAll` for the name→id registry persist, serialized on
//! one MetaStore — so generation wall time is ~streams * 3.4 ms no matter
//! how it is pipelined. Defaults (12k streams) take ~40-60 s once; the store
//! is kept on real fs (never tmpfs) and reused by later `bench --real` runs.

use std::path::Path;

use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{Durability, EngineOptions, LogEngine, Version};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rand_distr::{Distribution, Zipf};

pub struct GenConfig {
    pub streams:      usize,
    pub events:       usize,
    pub segment_size: u64,
    pub seed:         u64,
}

impl Default for GenConfig {
    fn default() -> Self {
        GenConfig {
            streams:      12_000,
            events:       150_000,
            // ~150k events * ~100 B (payload + frame overhead) ≈ 15 MB;
            // 1.5 MB segments ⇒ ~10 rolled+sealed segments of varied n.
            segment_size: 3 * 512 * 1024,
            seed:         42,
        }
    }
}

pub fn generate(dir: &Path, cfg: &GenConfig) {
    assert!(
        !dir.exists() || std::fs::read_dir(dir).map(|mut d| d.next().is_none()).unwrap_or(true),
        "refusing to generate into non-empty {} — delete it first",
        dir.display()
    );
    std::fs::create_dir_all(dir).unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    let engine = LogEngine::open_with(
        dir,
        EngineOptions {
            durability: Durability::Process,
            segment_size: cfg.segment_size,
            ..Default::default()
        },
    )
    .expect("open LogEngine");

    // Zipf(1.05) stream popularity over `streams` distinct streams: hot
    // streams recur in every segment, cold ones scatter — the real
    // benchmark-workload shape (research/05 §12 datasets).
    let mut rng = StdRng::seed_from_u64(cfg.seed);
    let zipf = Zipf::new(cfg.streams as f64, 1.05).unwrap();
    // Popularity rank -> stream index permutation, so hot streams are not
    // the lexicographically-first names.
    let mut perm: Vec<u32> = (0..cfg.streams as u32).collect();
    for i in (1..perm.len()).rev() {
        perm.swap(i, rng.random_range(0..=i));
    }
    let seq: Vec<u32> = (0..cfg.events)
        .map(|_| {
            let rank = (zipf.sample(&mut rng) as usize - 1).min(cfg.streams - 1);
            perm[rank]
        })
        .collect();

    // Shard streams over concurrent appender tasks (per-stream order
    // preserved: a stream lives on exactly one task).
    const TASKS: usize = 8;
    let mut shards: Vec<Vec<u32>> = vec![Vec::new(); TASKS];
    for &s in &seq {
        shards[s as usize % TASKS].push(s);
    }

    let t0 = std::time::Instant::now();
    rt.block_on(async {
        let mut joins = Vec::new();
        for (t, shard) in shards.into_iter().enumerate() {
            let engine = engine.clone();
            joins.push(tokio::spawn(async move {
                let mut versions: std::collections::HashMap<u32, Version> =
                    std::collections::HashMap::new();
                let mut prng = StdRng::seed_from_u64(t as u64);
                for s in shard {
                    let expected =
                        *versions.get(&s).unwrap_or(&Version::NoStream);
                    let nrec = prng.random_range(1..=3u32);
                    let recs: Vec<RecordToAppend> = (0..nrec)
                        .map(|r| RecordToAppend {
                            message_type: format!("ev.t{}", r % 4),
                            data:         vec![
                                (s % 251) as u8;
                                prng.random_range(24..96)
                            ],
                        })
                        .collect();
                    let out = engine
                        .append_batch(&format!("stream-{s}"), expected, &recs)
                        .await
                        .expect("append");
                    versions.insert(s, out.version);
                }
            }));
        }
        for j in joins {
            j.await.expect("appender task");
        }
    });
    let append_s = t0.elapsed().as_secs_f64();

    // Seal the final live head so its directory joins the corpus.
    engine.seal_active().expect("seal_active");
    let m = engine.metrics();
    eprintln!(
        "[gen-real] appended in {append_s:.1}s: {} events, {} sealed segments (+1 head), seals={} skipped={}",
        engine.total_events(),
        engine.sealed_segment_count(),
        m.seals,
        m.seals_skipped,
    );
    drop(engine); // drains queued roll seals
    rt.shutdown_timeout(std::time::Duration::from_secs(10));

    let n_pidx = std::fs::read_dir(dir.join("sealed"))
        .unwrap()
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "pidx"))
        .count();
    eprintln!("[gen-real] {} .pidx sidecars under {}/sealed", n_pidx, dir.display());
}
