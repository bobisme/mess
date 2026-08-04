//! `chatter bench` — five defined cells, process-timed, emitted as JSON on
//! stdout so an A/B harness can consume the output directly.
//!
//! # This is an honest example, not a shadow benchmark suite
//!
//! Read this before quoting a number from it.
//!
//! - **Process-level timing only.** Every cell is an
//!   [`Instant`]-around-the-whole-operation measurement of a public API call.
//!   There are no engine-internal hooks, no private counters, no sampling —
//!   nothing here can drift away from what an application would actually
//!   observe, because it *is* what an application observes.
//! - **One sample per cell, one process.** No warmup, no repetition, no
//!   statistics. Cells run in a fixed order in a single process, so later cells
//!   see caches (page cache, the engine's sealed block and capsule caches)
//!   warmed by earlier ones. That is realistic for an application and useless
//!   for a microbenchmark; treat these as *shape* numbers.
//! - **`mess-bench` is the real instrument.** It has floors, ABBA ordering,
//!   null controls, and repetition. `chatter bench` exists so a human can seed
//!   a realistic corpus and see whether the shape moved, and so an A/B harness
//!   can diff two JSON blobs.
//! - **Release builds only.** A debug build inflates these numbers several
//!   fold. The emitted JSON carries a `profile` field precisely so a debug
//!   number can never be mistaken for a release one.
//!
//! # The cells
//!
//! | name | what it times |
//! |------|---------------|
//! | `seed` | generating the whole corpus through the warm write path |
//! | `cold_reopen` | opening the store from cold: sealed tier install, registry recovery, active-segment resume |
//! | `scroll_back` | paging backward through the deepest channel — sealed history reads through the payload accelerator |
//! | `tail_catch_up` | one subscriber draining the whole global log from position 0 |
//! | `checkpoint_resume` | building the read model from a checkpoint versus from position 0 |

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::projections::Projections;
use crate::scrollback::ScrollBack;
use crate::seed::{self, SeedConfig};
use crate::store_backend::{
    OpenError, SealedCensus, checkpoint_path, create_store, open_store,
    sealed_census,
};
use crate::tail::Tailer;

/// How the bench was run and where.
#[derive(Debug, Clone)]
pub struct BenchConfig {
    /// Store directory. Wiped first (the bench always seeds a fresh corpus —
    /// timing a seed onto an existing store would measure the wrong thing).
    pub dir:              PathBuf,
    /// The corpus to seed.
    pub seed:             SeedConfig,
    /// How many backward pages the `scroll_back` cell reads.
    pub scroll_pages:     usize,
    /// Events per backward page.
    pub scroll_page_size: usize,
    /// Records per `read_global_page` call in the `tail_catch_up` cell.
    pub tail_page_size:   usize,
}

impl BenchConfig {
    /// A bench over `dir` at the demo scale.
    #[must_use]
    pub fn new(dir: PathBuf, seed: SeedConfig) -> Self {
        Self {
            dir,
            seed,
            scroll_pages: 20,
            scroll_page_size: 50,
            tail_page_size: 512,
        }
    }
}

/// One timed cell.
#[derive(Debug, Clone, Serialize)]
pub struct Cell {
    /// The cell's stable name — `seed`, `cold_reopen`, `scroll_back`,
    /// `tail_catch_up`, `checkpoint_resume`.
    pub name:   &'static str,
    /// Wall-clock milliseconds for the whole operation.
    pub ms:     f64,
    /// One line a human can read without the JSON.
    pub note:   String,
    /// Cell-specific numbers (counts, derived rates, sub-timings).
    pub detail: serde_json::Value,
}

/// The full bench result — what `chatter bench` prints.
#[derive(Debug, Clone, Serialize)]
pub struct BenchReport {
    /// Always `"chatter-bench"`, so a consumer can recognise the blob.
    pub tool:    &'static str,
    /// `"release"` or `"debug"`. **Only release numbers are meaningful** — see
    /// the module docs.
    pub profile: &'static str,
    pub seed:    u64,
    pub store:   StoreFacts,
    pub corpus:  CorpusFacts,
    pub cells:   Vec<Cell>,
}

/// The store the cells ran against.
#[derive(Debug, Clone, Serialize)]
pub struct StoreFacts {
    pub dir:           String,
    pub segment_bytes: u64,
    pub seal_pack:     bool,
    /// The sealed-tier census taken after the cold reopen — the numbers that
    /// prove this corpus actually reached the sealed tier.
    pub census:        SealedCensus,
}

/// The corpus that was seeded.
#[derive(Debug, Clone, Serialize)]
pub struct CorpusFacts {
    pub users:              usize,
    pub channels:           usize,
    pub messages:           usize,
    pub reactions:          usize,
    pub events:             usize,
    pub deepest_channel:    usize,
    pub shallowest_channel: usize,
}

impl BenchReport {
    /// The JSON blob, pretty-printed for a human and still machine-parseable.
    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("bench report serializes")
    }

    /// Look a cell up by name.
    #[must_use]
    pub fn cell(&self, name: &str) -> Option<&Cell> {
        self.cells.iter().find(|c| c.name == name)
    }
}

/// The five cell names this bench always emits, in order.
pub const CELL_NAMES: [&str; 5] = [
    "seed",
    "cold_reopen",
    "scroll_back",
    "tail_catch_up",
    "checkpoint_resume",
];

fn ms(d: Duration) -> f64 { d.as_secs_f64() * 1000.0 }

fn rate(count: u64, d: Duration) -> f64 {
    let secs = d.as_secs_f64();
    if secs > 0.0 { count as f64 / secs } else { 0.0 }
}

/// Run every cell against a freshly-seeded store at [`BenchConfig::dir`].
///
/// The directory is **removed first**: the seed cell times generating a corpus
/// into an empty log, which is the only thing it can honestly time.
pub async fn run(cfg: &BenchConfig) -> Result<BenchReport, OpenError> {
    if cfg.dir.exists() {
        std::fs::remove_dir_all(&cfg.dir)
            .map_err(|e| OpenError::Config(e.to_string()))?;
    }
    let mut cells: Vec<Cell> = Vec::new();

    // --- cell: seed --------------------------------------------------------
    let store = create_store(&cfg.dir, cfg.seed.store_config())?;
    let t = Instant::now();
    let seeded = seed::generate(&store, &cfg.seed).await;
    let seed_elapsed = t.elapsed();
    cells.push(Cell {
        name:   "seed",
        ms:     ms(seed_elapsed),
        note:   format!(
            "{} events through the warm write path",
            seeded.events()
        ),
        detail: serde_json::json!({
            "events": seeded.events(),
            "messages": seeded.messages,
            "reactions": seeded.reactions,
            "events_per_sec": rate(seeded.events() as u64, seed_elapsed),
            "concurrency": cfg.seed.concurrency,
        }),
    });
    // Drop the writer: the store lock is process-exclusive, and the next cell
    // is precisely "open it again from cold".
    drop(store);

    // --- cell: cold_reopen -------------------------------------------------
    let t = Instant::now();
    let store = open_store(&cfg.dir)?;
    let cold_reopen = t.elapsed();
    let census = sealed_census(&store, &cfg.dir);
    cells.push(Cell {
        name:   "cold_reopen",
        ms:     ms(cold_reopen),
        note:   format!(
            "reopened a store with {} sealed segment(s) and {} published \
             event(s)",
            census.sealed_segments, census.published_events
        ),
        detail: serde_json::json!({
            "sealed_segments": census.sealed_segments,
            "segment_files": census.segment_files,
            "published_events": census.published_events,
            "bytes": census.bytes,
        }),
    });

    // --- cell: scroll_back -------------------------------------------------
    // The deepest channel is known from the plan, so this cell needs no
    // projection built first — it reads the log directly, which is the point.
    let mut pages = 0usize;
    let mut records = 0u64;
    let mut messages = 0u64;
    let t = Instant::now();
    if let Some(channel) = seeded.deepest_channel_id {
        let mut back = ScrollBack::open(&store, channel, cfg.scroll_page_size)
            .await
            .map_err(|e| OpenError::Log(e.to_string()))?;
        while pages < cfg.scroll_pages && back.has_more() {
            let page = back
                .next_page()
                .await
                .map_err(|e| OpenError::Log(e.to_string()))?;
            pages += 1;
            records += page.records as u64;
            messages += page.messages.len() as u64;
        }
    }
    let scroll = t.elapsed();
    cells.push(Cell {
        name:   "scroll_back",
        ms:     ms(scroll),
        note:   format!(
            "{pages} backward page(s) of {} through {} ({messages} messages, \
             {records} records)",
            cfg.scroll_page_size,
            seeded.deepest_channel_slug.as_deref().unwrap_or("(no channel)"),
        ),
        detail: serde_json::json!({
            "pages": pages,
            "page_size": cfg.scroll_page_size,
            "records": records,
            "messages": messages,
            "channel": seeded.deepest_channel_slug,
            "channel_depth": seeded.deepest_channel,
            "records_per_sec": rate(records, scroll),
        }),
    });

    // --- cell: tail_catch_up ----------------------------------------------
    let mut tail = Tailer::from(&store, 0, cfg.tail_page_size);
    let t = Instant::now();
    let delivered = tail
        .catch_up(|_| {})
        .await
        .map_err(|e| OpenError::Log(e.to_string()))?;
    let tail_elapsed = t.elapsed();
    cells.push(Cell {
        name:   "tail_catch_up",
        ms:     ms(tail_elapsed),
        note:   format!("one subscriber drained {delivered} record(s) from 0"),
        detail: serde_json::json!({
            "records": delivered,
            "page_size": cfg.tail_page_size,
            "records_per_sec": rate(delivered, tail_elapsed),
            "frontier": tail.position(),
        }),
    });

    // --- cell: checkpoint_resume ------------------------------------------
    // Build once from 0 (no checkpoint yet), persist a checkpoint, then build
    // again — the second build is the cell's headline number.
    let ckpt = checkpoint_path(&cfg.dir);
    let _ = std::fs::remove_file(&ckpt);
    let t = Instant::now();
    let cold = Projections::with_checkpoint_cadence(
        &store,
        &ckpt,
        u64::MAX,
        Duration::from_secs(24 * 60 * 60),
    )
    .await;
    let from0 = t.elapsed();
    let applied = cold.applied_position();
    cold.checkpoint_now()
        .await
        .map_err(|e| OpenError::Config(e.to_string()))?;
    drop(cold);

    let t = Instant::now();
    let warm = Projections::with_checkpoint_cadence(
        &store,
        &ckpt,
        u64::MAX,
        Duration::from_secs(24 * 60 * 60),
    )
    .await;
    let resume = t.elapsed();
    let resumed_from = warm.resumed_from();
    let cards = warm.cardinalities().await;
    let ckpt_bytes = std::fs::metadata(&ckpt).map(|m| m.len()).unwrap_or(0);
    drop(warm);

    cells.push(Cell {
        name:   "checkpoint_resume",
        ms:     ms(resume),
        note:   format!(
            "read model resumed from position {resumed_from} (a from-0 \
             rebuild of the same log took {:.1} ms)",
            ms(from0)
        ),
        detail: serde_json::json!({
            "from0_ms": ms(from0),
            "resume_ms": ms(resume),
            "speedup": if resume.as_secs_f64() > 0.0 {
                from0.as_secs_f64() / resume.as_secs_f64()
            } else {
                0.0
            },
            "resumed_from": resumed_from,
            "applied": applied,
            "checkpoint_bytes": ckpt_bytes,
            "users": cards.users,
            "channels": cards.channels,
            "messages": cards.messages,
            "reactions": cards.reactions,
        }),
    });

    Ok(BenchReport {
        tool: "chatter-bench",
        profile: if cfg!(debug_assertions) { "debug" } else { "release" },
        seed: cfg.seed.seed,
        store: StoreFacts {
            dir: cfg.dir.display().to_string(),
            segment_bytes: cfg.seed.segment_bytes,
            seal_pack: cfg.seed.seal_pack,
            census,
        },
        corpus: CorpusFacts {
            users:              seeded.users,
            channels:           seeded.channels,
            messages:           seeded.messages,
            reactions:          seeded.reactions,
            events:             seeded.events(),
            deepest_channel:    seeded.deepest_channel,
            shallowest_channel: seeded.shallowest_channel,
        },
        cells,
    })
}

/// Where `chatter bench` puts its scratch store when `--dir` is not given.
#[must_use]
pub fn default_bench_dir() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    Path::new(&home).join(".cache/mess-chatter-bench/store")
}
