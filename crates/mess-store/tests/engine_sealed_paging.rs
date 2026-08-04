//! bn-1u6p: the sealed branch of `read_stream` now seeks.
//!
//! Before this bone the sealed branch resolved the WHOLE stream on every call
//! — every batch of every sealed segment into a `BTreeMap`, unioned with the
//! entire hot tail — and then threw the pre-cursor prefix away. This suite is
//! the correctness gate on replacing that with a bounded seek + two-way merge:
//!
//! 1. **Bounded-resolve equivalence.** An exhaustive sweep of every cursor from
//!    before the stream to past its head, at several page sizes, against a full
//!    in-test oracle — byte-identical [`StoredRecord`]s or nothing.
//!    Batch-straddling cursors are the off-by-one that would silently drop
//!    records, so the corpus uses multi-event batches and the sweep hits every
//!    intra-batch offset.
//! 2. **Seal-handoff dedupe.** `seal_active` seals a prefix of the still-live
//!    head segment, so the same batches exist in the sealed sidecar AND the
//!    active index (sealed eviction is logical). Pages straddling that boundary
//!    must deliver each event exactly once. This is the bone's named risk: it
//!    used to ride on the `BTreeMap`'s insert order.
//! 3. **Structural boundedness.** Not wall-clock: the block cache's miss
//!    counter proves a deep-cursor page decodes only the pointer blocks it
//!    needs instead of every segment's.

use mess_log::committer::Durability;
use mess_store::backend::{Backend, RecordToAppend, StoredRecord};
use mess_store::{EngineOptions, LogEngine, Version};

/// One appended record the oracle remembers.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Shadow {
    stream:  String,
    typ:     String,
    data:    Vec<u8>,
    version: u64,
    global:  u64,
}

impl Shadow {
    fn assert_eq_record(&self, r: &StoredRecord, ctx: &str) {
        assert_eq!(r.stream_id, self.stream, "{ctx}: stream_id");
        assert_eq!(r.message_type, self.typ, "{ctx}: message_type");
        assert_eq!(r.data, self.data, "{ctx}: data");
        assert_eq!(r.stream_position, self.version, "{ctx}: stream_position");
        assert_eq!(r.global_position, self.global, "{ctx}: global_position");
    }
}

fn opts(segment_size: u64) -> EngineOptions {
    EngineOptions {
        durability: Durability::Process,
        segment_size,
        capsule_cache_budget_bytes: 64 << 20,
        seal_pack: false,
        ..Default::default()
    }
}

/// Fat enough (~400 B) that a 16 KiB segment holds only a few dozen events —
/// so a few hundred appends really do span many sealed segments without the
/// exhaustive cursor sweep having to grow with them.
fn payload(i: u64) -> Vec<u8> {
    let mut v = i.to_le_bytes().to_vec();
    v.extend(std::iter::repeat_n((i % 251) as u8, 384 + (i % 41) as usize));
    v
}

/// Append `batches` batches of `per_batch` events to `stream`, recording each
/// record in `oracle`. Resumes from the engine's live head.
async fn seed(
    engine: &LogEngine,
    oracle: &mut Vec<Shadow>,
    stream: &str,
    batches: u64,
    per_batch: u64,
) {
    let mut head = engine.head(stream).await.expect("head");
    let mut seq = oracle.iter().filter(|s| s.stream == stream).count() as u64;
    for _ in 0..batches {
        let recs: Vec<RecordToAppend> = (0..per_batch)
            .map(|k| RecordToAppend {
                message_type: if (seq + k).is_multiple_of(3) {
                    "msg.a".to_string()
                } else {
                    "msg.b".to_string()
                },
                data:         payload(seq + k),
            })
            .collect();
        let out =
            engine.append_batch(stream, head, &recs).await.expect("append");
        head = out.version;
        let first_global = out.last_global_position + 1 - recs.len() as u64;
        for (k, r) in recs.iter().enumerate() {
            oracle.push(Shadow {
                stream:  stream.to_string(),
                typ:     r.message_type.clone(),
                data:    r.data.clone(),
                version: seq + k as u64,
                global:  first_global + k as u64,
            });
        }
        seq += per_batch;
    }
}

/// Wait until the background roll-sealer has installed at least `want` sealed
/// segments (bounded).
fn await_seals(engine: &LogEngine, want: usize) {
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(30);
    while engine.sealed_segment_count() < want {
        assert!(
            std::time::Instant::now() < deadline,
            "sealer never installed {want} segments (got {})",
            engine.sealed_segment_count()
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// The oracle's view of one stream, version-ordered.
fn stream_view<'a>(oracle: &'a [Shadow], stream: &str) -> Vec<&'a Shadow> {
    let mut v: Vec<&Shadow> =
        oracle.iter().filter(|s| s.stream == stream).collect();
    v.sort_by_key(|s| s.version);
    v
}

/// EVERY cursor from `NoStream` through one past the head, at every page size
/// in `limits`, compared record-for-record against the oracle. Cheap on a
/// small corpus and the only way to be sure no cursor/limit pair silently
/// drops or duplicates a record — including cursors interior to a batch.
async fn assert_cursor_sweep(
    engine: &LogEngine,
    oracle: &[Shadow],
    stream: &str,
    limits: &[usize],
    ctx: &str,
) {
    let recs = stream_view(oracle, stream);
    let head = recs.len() as u64;
    assert!(head > 0, "{ctx}: empty oracle for {stream}");

    for &limit in limits {
        // `after`: NoStream, then At(0)..=At(head + 1) — one past the head, so
        // the "read positioned at/after the head returns nothing" case is
        // covered too.
        let cursors: Vec<Version> = std::iter::once(Version::NoStream)
            .chain((0..=head + 1).map(Version::At))
            .collect();
        for after in cursors {
            let start = after.next_position() as usize;
            let got = engine
                .read_stream(stream, after, limit)
                .await
                .expect("read_stream");
            let want_len = recs.len().saturating_sub(start).min(limit);
            assert_eq!(
                got.len(),
                want_len,
                "{ctx}: {stream} after={after:?} limit={limit} page length"
            );
            for (k, g) in got.iter().enumerate() {
                recs[start + k].assert_eq_record(
                    g,
                    &format!("{ctx}: {stream} after={after:?} limit={limit}"),
                );
            }
            // Exactly-once, in order, within the page.
            for w in got.windows(2) {
                assert_eq!(
                    w[1].stream_position,
                    w[0].stream_position + 1,
                    "{ctx}: {stream} after={after:?} limit={limit} must be \
                     dense and duplicate-free"
                );
            }
        }
    }
}

/// Page the whole stream from zero at `page`, exactly as `EventStore::load`
/// does, and assert the concatenation is the oracle — the paging loop is where
/// a per-page off-by-one shows up as a lost or repeated record.
async fn assert_full_paging(
    engine: &LogEngine,
    oracle: &[Shadow],
    stream: &str,
    page: usize,
    ctx: &str,
) {
    let recs = stream_view(oracle, stream);
    let mut after = Version::NoStream;
    let mut idx = 0usize;
    loop {
        let got =
            engine.read_stream(stream, after, page).await.expect("read_stream");
        for g in &got {
            recs[idx].assert_eq_record(
                g,
                &format!("{ctx}: {stream} full paging at {idx}"),
            );
            idx += 1;
        }
        if got.len() < page {
            break;
        }
        after = Version::At(got.last().unwrap().stream_position);
    }
    assert_eq!(idx, recs.len(), "{ctx}: {stream} paging must serve everything");
}

/// (1) Bounded-resolve equivalence over a multi-segment sealed stream plus a
/// hot tail, live and across a reopen, with the block cache enabled, tiny, and
/// off. Batches are 7 events wide so cursors land inside batches, at their
/// first version, and at their last.
#[test]
fn sealed_paging_is_byte_identical_across_every_cursor_and_limit() {
    let tmp = mess_testkit::sweeping_temp_dir("sealed-paging");
    let dir = tmp.path().join("store");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut oracle: Vec<Shadow> = Vec::new();
    // 16 KiB segments force several rolls, so `deep` really does span
    // multiple sealed segments while the head segment stays hot.
    let engine = LogEngine::open_with(&dir, opts(16 * 1024)).expect("open");
    rt.block_on(seed(&engine, &mut oracle, "deep", 60, 7));
    // A shallow companion stream, so the sweep also covers a stream whose
    // sealed coverage is a single segment.
    rt.block_on(seed(&engine, &mut oracle, "shallow", 3, 7));
    await_seals(&engine, 3);
    // More appends AFTER the seals: the stream now has sealed segments and a
    // genuine hot tail, which is the merge's real input shape.
    rt.block_on(seed(&engine, &mut oracle, "deep", 10, 7));

    assert!(
        engine.sealed_segment_count() >= 3,
        "need a multi-segment sealed stream, got {}",
        engine.sealed_segment_count()
    );

    let limits = [1usize, 2, 7, 8, 33, 1000];
    rt.block_on(assert_cursor_sweep(&engine, &oracle, "deep", &limits, "live"));
    rt.block_on(assert_cursor_sweep(
        &engine, &oracle, "shallow", &limits, "live",
    ));
    rt.block_on(assert_full_paging(&engine, &oracle, "deep", 13, "live"));
    // An unknown stream and a stream read past its head stay empty.
    assert!(
        rt.block_on(engine.read_stream("nope", Version::NoStream, 10))
            .unwrap()
            .is_empty()
    );
    drop(engine);

    // Reopen: the sealed history is served purely from the sidecars and the
    // hot tail is whatever the recovery scan rebuilt.
    let engine = LogEngine::open_with(&dir, opts(16 * 1024)).expect("reopen");
    rt.block_on(assert_cursor_sweep(
        &engine, &oracle, "deep", &limits, "reopened",
    ));
    rt.block_on(assert_full_paging(&engine, &oracle, "deep", 13, "reopened"));
    drop(engine);

    // Block cache off (budget 0): the seek must take the identical
    // decode-every-block-fresh path and return the identical bytes.
    let mut o = opts(16 * 1024);
    o.block_cache_budget_bytes = 0;
    let engine = LogEngine::open_with(&dir, o).expect("reopen no block cache");
    rt.block_on(assert_cursor_sweep(
        &engine,
        &oracle,
        "deep",
        &limits,
        "no-block-cache",
    ));
    drop(engine);

    // A block cache far too small to hold the stream: eviction on every read
    // cannot change results.
    let mut o = opts(16 * 1024);
    o.block_cache_budget_bytes = 512;
    let engine = LogEngine::open_with(&dir, o).expect("reopen tiny cache");
    rt.block_on(assert_cursor_sweep(
        &engine,
        &oracle,
        "deep",
        &limits,
        "tiny-block-cache",
    ));
}

/// (2) THE NAMED RISK. `seal_active` seals a prefix of the live head segment,
/// so its batches are in the sealed sidecar *and* still in the active index.
/// Every page straddling that handoff boundary must deliver each event exactly
/// once, in order — which the old code got from a `BTreeMap` keyed by
/// `first_version` and the new code gets from the merge's sealed-wins tie
/// break.
#[test]
fn seal_handoff_window_delivers_every_event_exactly_once() {
    let tmp = mess_testkit::sweeping_temp_dir("sealed-handoff");
    let dir = tmp.path().join("store");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut oracle: Vec<Shadow> = Vec::new();
    // One large segment: nothing ROLLS, so nothing is evicted from the active
    // index — `seal_active` is what creates the overlap.
    let engine = LogEngine::open_with(&dir, opts(64 << 20)).expect("open");
    rt.block_on(seed(&engine, &mut oracle, "hand", 40, 5));
    engine.seal_active().expect("seal_active");
    assert_eq!(engine.sealed_segment_count(), 1);
    // Versions 0..=199 are now in BOTH tiers. Append past the coverage so the
    // stream also has batches that exist ONLY in the active index.
    rt.block_on(seed(&engine, &mut oracle, "hand", 20, 5));

    let recs = stream_view(&oracle, "hand");
    assert_eq!(recs.len(), 300);

    // Full sweep across the handoff: every cursor, several page sizes.
    let limits = [1usize, 5, 6, 17, 300];
    rt.block_on(assert_cursor_sweep(
        &engine,
        &oracle,
        "hand",
        &limits,
        "handoff live",
    ));

    // Pages deliberately centred on the boundary (v199/v200): the window where
    // a batch present in both tiers meets one present in only the hot tail.
    for after in [Version::At(190), Version::At(198), Version::At(199)] {
        for limit in [1usize, 3, 5, 10, 25] {
            let got = rt
                .block_on(engine.read_stream("hand", after, limit))
                .expect("read across handoff");
            let start = after.next_position() as usize;
            assert_eq!(got.len(), recs.len().saturating_sub(start).min(limit));
            let mut seen = std::collections::BTreeSet::new();
            for (k, g) in got.iter().enumerate() {
                assert!(
                    seen.insert(g.stream_position),
                    "duplicate v{} across the handoff (after={after:?}, \
                     limit={limit})",
                    g.stream_position
                );
                recs[start + k].assert_eq_record(g, "handoff page");
            }
        }
    }

    // And the whole stream paged from zero, which is what `EventStore::load`
    // does: exactly 300 records, no repeats.
    rt.block_on(assert_full_paging(&engine, &oracle, "hand", 7, "handoff"));

    // Across a reopen the same window is served entirely from the sidecar
    // plus the recovered tail.
    drop(engine);
    let engine = LogEngine::open_with(&dir, opts(64 << 20)).expect("reopen");
    rt.block_on(assert_cursor_sweep(
        &engine,
        &oracle,
        "hand",
        &limits,
        "handoff reopened",
    ));
}

/// (3) Structural boundedness through an observable the engine already
/// reports: `EngineMetrics::cache_misses` counts pointer-block decodes. A page
/// taken at a deep cursor must decode a bounded number of blocks — not one per
/// sealed segment, which is what the whole-stream resolve did.
#[test]
fn deep_cursor_page_decodes_only_the_blocks_it_needs() {
    let tmp = mess_testkit::sweeping_temp_dir("sealed-bounded");
    let dir = tmp.path().join("store");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut oracle: Vec<Shadow> = Vec::new();
    let engine = LogEngine::open_with(&dir, opts(16 * 1024)).expect("open");
    rt.block_on(seed(&engine, &mut oracle, "deep", 200, 5));
    await_seals(&engine, 6);
    drop(engine);

    // Fresh open ⇒ cold block cache, and `cache_misses` starts at 0. The
    // segment count is taken HERE: the background sealer keeps installing
    // after `await_seals`' floor is met, and the reopen admits every sidecar
    // on disk, so this is the number of blocks the unbounded resolve would
    // have decoded.
    let engine = LogEngine::open_with(&dir, opts(16 * 1024)).expect("reopen");
    let segments = engine.sealed_segment_count();
    assert!(segments >= 6, "need several sealed segments, got {segments}");
    let base = engine.metrics().cache_misses;
    assert_eq!(base, 0, "a fresh open must not have decoded any block");

    let recs = stream_view(&oracle, "deep");
    let head = recs.len() as u64;

    // One page at a cursor near the head. The seek must skip every earlier
    // segment on its directory head alone.
    let got = rt
        .block_on(engine.read_stream("deep", Version::At(head - 10), 5))
        .expect("deep page");
    assert_eq!(got.len(), 5);
    let decoded = engine.metrics().cache_misses - base;
    assert!(
        decoded <= 2,
        "a page at the head of a {segments}-segment sealed stream took \
         {decoded} block-cache misses; the seek must decode at most the \
         segment holding the cursor (and, if the page runs off its end, the \
         next). The unbounded resolve took one per segment."
    );

    // A page from zero is equally bounded: it must not walk the whole stream
    // to serve the first few records.
    drop(engine);
    let engine = LogEngine::open_with(&dir, opts(16 * 1024)).expect("reopen 2");
    let base = engine.metrics().cache_misses;
    let got = rt
        .block_on(engine.read_stream("deep", Version::NoStream, 5))
        .expect("first page");
    assert_eq!(got.len(), 5);
    let decoded = engine.metrics().cache_misses - base;
    assert!(
        decoded <= 2,
        "the FIRST page of a {segments}-segment sealed stream took {decoded} \
         block-cache misses"
    );

    // Whole-stream paging still reads the whole history — boundedness is per
    // call, not a refusal to serve it.
    rt.block_on(assert_full_paging(&engine, &oracle, "deep", 64, "bounded"));
}
