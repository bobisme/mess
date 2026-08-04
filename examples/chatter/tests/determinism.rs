//! Same `--seed`, same corpus.
//!
//! Two halves, matching what [`chatter::seed`]'s docs promise:
//!
//! - **Sequential (`concurrency == 1`)**: the global log is **byte-identical**
//!   run to run — same stream ids, same wire message types, same encoded
//!   payloads, in the same order. That pins the generated [`Id`]s themselves,
//!   not just the counts.
//! - **Pipelined (`concurrency > 1`)**: the global *interleaving* is free to
//!   differ, but every stream's own event sequence is identical, because a
//!   channel is one stream and its actions are never fanned out. Equal totals,
//!   equal per-stream bytes.
//!
//! The second half is the one that matters at `--scale large`, where sequential
//! seeding would take far too long — and it is exactly the property a message
//! ordinal depends on.

use std::collections::BTreeMap;

use chatter::seed::{self, SeedConfig};
use chatter::store_backend::{create_store, open_store};
use mess_store::{Backend, StoredRecord};
use mess_testkit::sweeping_temp_dir;

/// Every record in the store's global log, in order.
async fn global_log(dir: &std::path::Path) -> Vec<StoredRecord> {
    let store = open_store(dir).expect("open store");
    let log = store
        .backend()
        .read_global(None, 5_000_000)
        .await
        .expect("read the global log");
    drop(store);
    log
}

/// A `stream_id -> [(message_type, payload)]` view: each stream's own event
/// sequence, independent of the global interleaving.
fn per_stream(
    log: &[StoredRecord],
) -> BTreeMap<String, Vec<(String, Vec<u8>)>> {
    let mut map: BTreeMap<String, Vec<(String, Vec<u8>)>> = BTreeMap::new();
    for rec in log {
        map.entry(rec.stream_id.clone())
            .or_default()
            .push((rec.message_type.clone(), rec.data.clone()));
    }
    map
}

/// Seed `cfg` into a fresh store and return its directory (the temp guard is
/// returned too and must outlive it).
async fn seed_store(
    tag: &str,
    cfg: &SeedConfig,
) -> (mess_testkit::SweepingTempDir, std::path::PathBuf) {
    let t = sweeping_temp_dir(tag);
    let dir = t.path().join("store");
    let store = create_store(&dir, cfg.store_config()).expect("create store");
    seed::generate(&store, cfg).await;
    drop(store);
    (t, dir)
}

#[tokio::test]
async fn the_same_seed_produces_a_byte_identical_log_when_sequential() {
    let cfg = SeedConfig { concurrency: 1, ..SeedConfig::tiny(1337) };
    let (_ta, da) = seed_store("chatter-det-a", &cfg).await;
    let (_tb, db) = seed_store("chatter-det-b", &cfg).await;

    let log_a = global_log(&da).await;
    let log_b = global_log(&db).await;

    assert!(!log_a.is_empty(), "fixture sanity");
    assert_eq!(log_a.len(), log_b.len(), "same event count");
    for (a, b) in log_a.iter().zip(log_b.iter()) {
        assert_eq!(a.stream_id, b.stream_id, "same stream, same order");
        assert_eq!(a.message_type, b.message_type);
        assert_eq!(a.data, b.data, "byte-identical payloads");
        assert_eq!(a.stream_position, b.stream_position);
    }
}

#[tokio::test]
async fn a_different_seed_produces_a_different_corpus() {
    // The control for the test above: if the seed were being ignored, the
    // byte-identity assertion would pass vacuously.
    let (_ta, da) =
        seed_store("chatter-det-seed-a", &SeedConfig::tiny(1)).await;
    let (_tb, db) =
        seed_store("chatter-det-seed-b", &SeedConfig::tiny(2)).await;
    let log_a = global_log(&da).await;
    let log_b = global_log(&db).await;
    assert_ne!(
        log_a.iter().map(|r| r.data.clone()).collect::<Vec<_>>(),
        log_b.iter().map(|r| r.data.clone()).collect::<Vec<_>>(),
        "two different seeds must not produce the same payload sequence"
    );
}

#[tokio::test]
async fn pipelining_preserves_every_stream_and_the_totals() {
    let sequential = SeedConfig { concurrency: 1, ..SeedConfig::tiny(77) };
    let pipelined = SeedConfig { concurrency: 8, ..sequential.clone() };

    let (_ts, ds) = seed_store("chatter-det-seq", &sequential).await;
    let (_tp, dp) = seed_store("chatter-det-pipe", &pipelined).await;

    let log_s = global_log(&ds).await;
    let log_p = global_log(&dp).await;
    assert_eq!(log_s.len(), log_p.len(), "same total event count");

    let by_stream_s = per_stream(&log_s);
    let by_stream_p = per_stream(&log_p);
    assert_eq!(
        by_stream_s.len(),
        by_stream_p.len(),
        "the same set of streams exists either way"
    );
    assert_eq!(
        by_stream_s, by_stream_p,
        "per-stream event sequences must be identical regardless of execution \
         concurrency — this is what a message ordinal depends on"
    );
}

#[tokio::test]
async fn two_pipelined_runs_of_one_seed_agree_per_stream() {
    // The property `--scale large` actually relies on: pipelined execution is
    // reproducible per stream even though the global interleaving is not.
    let cfg = SeedConfig { concurrency: 8, ..SeedConfig::tiny(4242) };
    let (_ta, da) = seed_store("chatter-det-pipe-a", &cfg).await;
    let (_tb, db) = seed_store("chatter-det-pipe-b", &cfg).await;
    let log_a = global_log(&da).await;
    let log_b = global_log(&db).await;
    assert_eq!(log_a.len(), log_b.len());
    assert_eq!(per_stream(&log_a), per_stream(&log_b));
}
