//! bn-2ib (Spike C, open_without_book): differential byte-identity suite for
//! the block-native read paths.
//!
//! Builds a mixed hot/sealed corpus (small segments force live rolls, so the
//! background sealer produces real sealed `.pidx`/`.pcol` sidecars while the
//! head segment stays hot) alongside a full in-test oracle of every appended
//! record, then proves every read API returns **byte-identical**
//! [`StoredRecord`] sequences against that oracle:
//!
//! - live (hot + sealed mix), across `read_global` and `read_stream` page
//!   shapes and `head`;
//! - after a reopen (sealed history served purely from the sidecars — and with
//!   ZERO payload frame decodes during open, the bn-2ib gate);
//! - with a tiny capsule cache (evictions on every read) and with the cache
//!   disabled — eviction/absence cannot change results;
//! - with a corrupt and a truncated `.pcol` (falls back to the raw log,
//!   byte-identically — the log stays truth);
//! - across an on-demand `seal_active` of a still-growing head segment (reads
//!   straddle the sidecar's coverage boundary).

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

/// A shreddable MessagePack map (columnar `.pcol` blocks) for even seeds, a
/// raw binary run (row-fallback blocks) for odd — so sealed segments carry
/// MIXED block kinds through the one read path.
fn payload(i: u64) -> Vec<u8> {
    if i.is_multiple_of(2) {
        let mut m = vec![0x82]; // fixmap(2)
        m.push(0xA3);
        m.extend_from_slice(b"seq");
        m.push(0xCF);
        m.extend_from_slice(&i.to_be_bytes());
        m.push(0xA4);
        m.extend_from_slice(b"kind");
        m.push(0xA4);
        m.extend_from_slice(b"demo");
        m
    } else {
        let mut v = vec![0xFF, 0x00];
        v.extend_from_slice(&i.to_le_bytes());
        v.extend(std::iter::repeat_n((i % 251) as u8, (i % 37) as usize));
        v
    }
}

fn opts(segment_size: u64, capsule_budget: u64) -> EngineOptions {
    EngineOptions {
        durability: Durability::Process,
        segment_size,
        capsule_cache_budget_bytes: capsule_budget,
        // loose-sidecar coverage — this mode must keep working forever;
        // bn-ccx1
        seal_pack: false,
        ..Default::default()
    }
}

/// Append `total` events round-robin over `streams` streams in batches of
/// `per_batch`, recording every record in the oracle.
async fn seed(
    engine: &LogEngine,
    oracle: &mut Vec<Shadow>,
    streams: usize,
    total: u64,
    per_batch: u64,
) {
    // Resume from the engine's live heads (a second seeding pass after a
    // reopen continues each stream where it left off).
    let mut heads: Vec<Version> = Vec::with_capacity(streams);
    let mut versions: Vec<u64> = Vec::with_capacity(streams);
    for s in 0..streams {
        let h = engine.head(&format!("acct-{s:04}")).await.expect("head");
        heads.push(h);
        versions.push(match h {
            Version::At(v) => v + 1,
            _ => 0,
        });
    }
    // `seq` numbers the USER events (it seeds the payloads and type names, so
    // it must stay dense and reproducible). The oracle's `global`, by
    // contrast, is taken from the engine's OWN ack — `bn-2di`: the
    // log-derived `$registry` consumes a global position per name it
    // registers, so global positions are no longer "the index of the Nth
    // user event" and an oracle that assumed so would be asserting a
    // falsehood. Deriving them from the ack is also simply more faithful:
    // it tests what the engine actually reports.
    let mut seq = oracle.len() as u64;
    let batches = total / per_batch;
    for b in 0..batches {
        let s = (b as usize) % streams;
        let name = format!("acct-{s:04}");
        let recs: Vec<RecordToAppend> = (0..per_batch)
            .map(|k| RecordToAppend {
                message_type: if (seq + k).is_multiple_of(3) {
                    "acct.opened".to_string()
                } else {
                    "acct.deposited".to_string()
                },
                data:         payload(seq + k),
            })
            .collect();
        let out =
            engine.append_batch(&name, heads[s], &recs).await.expect("append");
        heads[s] = out.version;
        // The batch's events occupy [last - (n-1) ..= last].
        let first_global = out.last_global_position + 1 - recs.len() as u64;
        for (k, r) in recs.iter().enumerate() {
            oracle.push(Shadow {
                stream:  name.clone(),
                typ:     r.message_type.clone(),
                data:    r.data.clone(),
                version: versions[s],
                global:  first_global + k as u64,
            });
            versions[s] += 1;
            seq += 1;
        }
    }
}

/// The number of LOG events the oracle implies (`bn-2di`).
///
/// `LogEngine::total_events()` is the durable watermark — it counts every event
/// in the log, and that now includes the `$registry` records the engine writes
/// when it first sees a stream name or an event-type name. The oracle only
/// records USER events, so the two differ by exactly the number of
/// registrations. Rather than hard-code that, derive it: the highest global
/// position the oracle knows, plus one, IS the log's event count (the last
/// append is always a user event, never a registration — a registration is only
/// ever written *ahead of* the batch that uses it).
fn implied_total_events(oracle: &[Shadow]) -> usize {
    oracle.last().map_or(0, |s| s.global as usize + 1)
}

/// Every read API vs the oracle: paged `read_global` (several page sizes and
/// `after` starts), paged `read_stream` per stream (several `after` starts +
/// limits), and `head`.
async fn assert_identical(engine: &LogEngine, oracle: &[Shadow], ctx: &str) {
    // read_global, full paging at two page sizes + a mid-log start.
    //
    // `bn-2di`: the oracle INDEX and the global POSITION are no longer the same
    // number (the `$registry` records the engine writes consume positions but
    // are never delivered), so the mid-log start is expressed as an oracle
    // index and converted to the `after` cursor through the oracle's own
    // recorded position. Conflating the two would silently mis-align every
    // assertion past the first registration.
    for (page, start_idx) in
        [(97usize, 0usize), (512, 0), (64, oracle.len() / 2)]
    {
        let mut idx = start_idx;
        let mut after = if start_idx == 0 {
            None
        } else {
            Some(oracle[start_idx].global - 1)
        };
        loop {
            let got =
                engine.read_global(after, page).await.expect("read_global");
            let want = &oracle
                [idx.min(oracle.len())..(idx + got.len()).min(oracle.len())];
            assert_eq!(
                got.len(),
                want.len().min(page),
                "{ctx}: global page len at idx {idx}"
            );
            for (g, w) in got.iter().zip(want) {
                w.assert_eq_record(g, &format!("{ctx}: read_global idx {idx}"));
            }
            if got.len() < page {
                let served = idx + got.len();
                assert_eq!(
                    served,
                    oracle.len(),
                    "{ctx}: global paging must serve everything"
                );
                break;
            }
            idx += got.len();
            after = Some(got.last().unwrap().global_position);
        }
    }

    // Per-stream oracle views.
    let mut by_stream: std::collections::BTreeMap<&str, Vec<&Shadow>> =
        std::collections::BTreeMap::new();
    for s in oracle {
        by_stream.entry(s.stream.as_str()).or_default().push(s);
    }
    for (name, recs) in &by_stream {
        // head
        let head = engine.head(name).await.expect("head");
        assert_eq!(
            head,
            Version::At(recs.last().unwrap().version),
            "{ctx}: head of {name}"
        );
        // full paged replay + a mid-stream start + a tail start
        let starts = [
            Version::NoStream,
            Version::At(recs.len() as u64 / 2),
            Version::At(recs.len() as u64 - 1),
        ];
        for start in starts {
            let mut after = start;
            let mut idx = after.next_position() as usize;
            loop {
                let got = engine
                    .read_stream(name, after, 33)
                    .await
                    .expect("read_stream");
                for (g, w) in got.iter().zip(&recs[idx..]) {
                    w.assert_eq_record(
                        g,
                        &format!("{ctx}: read_stream {name} v{idx}"),
                    );
                }
                if got.len() < 33 {
                    assert_eq!(
                        idx + got.len(),
                        recs.len(),
                        "{ctx}: {name} paging must serve to the head"
                    );
                    break;
                }
                idx += got.len();
                after = Version::At(got.last().unwrap().stream_position);
            }
        }
    }
    // Unknown stream stays empty/NoStream.
    assert_eq!(engine.head("no-such-stream").await.unwrap(), Version::NoStream);
    assert!(
        engine
            .read_stream("no-such-stream", Version::NoStream, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

/// Wait until the background roll-sealer has installed at least `want`
/// sealed segments (bounded).
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

#[test]
fn block_reads_are_byte_identical_live_reopened_and_cache_starved() {
    let tmp = mess_testkit::sweeping_temp_dir("owb-diff");
    let dir = tmp.path().join("store");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut oracle: Vec<Shadow> = Vec::new();

    // Small segments (64 KiB) force many live rolls → real sealed sidecars
    // with `.pcol` payload blocks, while the head stays hot.
    let engine =
        LogEngine::open_with(&dir, opts(64 * 1024, 64 << 20)).expect("open");
    rt.block_on(seed(&engine, &mut oracle, 16, 4000, 5));
    await_seals(&engine, 3);
    assert_eq!(engine.total_events(), implied_total_events(&oracle));
    rt.block_on(assert_identical(&engine, &oracle, "live"));
    drop(engine);

    // Reopen: sealed history served from the sidecars; the open must decode
    // ZERO payload frames (the bn-2ib gate observable).
    let engine =
        LogEngine::open_with(&dir, opts(64 * 1024, 64 << 20)).expect("reopen");
    assert_eq!(
        engine.recover_payload_decodes(),
        0,
        "chain-off open must decode zero payload frames"
    );
    assert_eq!(
        engine.total_events(),
        implied_total_events(&oracle),
        "reopen watermark"
    );
    rt.block_on(assert_identical(&engine, &oracle, "reopened"));

    // Appends after reopen continue the dense positions; reads still match.
    rt.block_on(seed(&engine, &mut oracle, 16, 400, 5));
    rt.block_on(assert_identical(&engine, &oracle, "reopened+appended"));
    drop(engine);

    // Cache-starved reopen: a capsule budget so small every read evicts.
    // Eviction cannot change results.
    let engine = LogEngine::open_with(&dir, opts(64 * 1024, 2048))
        .expect("reopen tiny cache");
    rt.block_on(assert_identical(&engine, &oracle, "tiny-cache"));
    drop(engine);

    // Cache disabled entirely (budget 0): identical results again.
    let engine = LogEngine::open_with(&dir, opts(64 * 1024, 0))
        .expect("reopen no cache");
    rt.block_on(assert_identical(&engine, &oracle, "no-cache"));
    drop(engine);

    // Corrupt every `.pcol` (flip a middle byte): open skips the torn
    // sidecar (CRC) or the read's reassembly fails — either way reads fall
    // back to the raw log, byte-identically.
    let sealed_dir = dir.join("sealed");
    let mut flipped = 0;
    for entry in std::fs::read_dir(&sealed_dir).unwrap().flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("pcol") {
            let mut bytes = std::fs::read(&path).unwrap();
            let mid = bytes.len() / 2;
            bytes[mid] ^= 0xFF;
            std::fs::write(&path, &bytes).unwrap();
            flipped += 1;
            if flipped == 2 {
                break; // two corrupted + the rest healthy: a mixed tier
            }
        }
    }
    assert!(flipped > 0, "fixture must corrupt at least one .pcol");
    let engine = LogEngine::open_with(&dir, opts(64 * 1024, 64 << 20))
        .expect("reopen with corrupt .pcol");
    rt.block_on(assert_identical(&engine, &oracle, "corrupt-pcol"));
    drop(engine);

    // Truncate one `.pcol` to a husk: same story.
    for entry in std::fs::read_dir(&sealed_dir).unwrap().flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("pcol") {
            let bytes = std::fs::read(&path).unwrap();
            std::fs::write(&path, &bytes[..bytes.len() / 3]).unwrap();
            break;
        }
    }
    let engine = LogEngine::open_with(&dir, opts(64 * 1024, 64 << 20))
        .expect("reopen with truncated .pcol");
    rt.block_on(assert_identical(&engine, &oracle, "truncated-pcol"));
}

/// bn-2ib review F1 regression (the confirmed stale-`.pcol`-block repro): a
/// segment RE-sealed with grown coverage must never serve payloads through a
/// decoded block cached under the previous sidecar's block map.
///
/// Shape (the reviewer's exact sequence): 600 events in 10-event batches ->
/// `seal_active` (block 4 of the `.pcol` is a PARTIAL tail block, events
/// [512,600)) -> read [512,600) to cache that partial block (with a tiny
/// capsule budget so the publish warms for later batches are evicted) ->
/// append 3,000 more -> `seal_active` again (block 4 is now a FULL [512,640)
/// block) -> read [630,640). Pre-fix this underflowed in `pcol_range` while
/// holding the book mutex (panic + poisoned engine); the install-generation
/// cache keys make the second seal's blocks a distinct cache lineage.
#[test]
fn reseal_after_growth_serves_fresh_pcol_blocks() {
    let tmp = mess_testkit::sweeping_temp_dir("owb-reseal");
    let dir = tmp.path().join("store");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut oracle: Vec<Shadow> = Vec::new();
    // One big segment; tiny budget: capsule warms from the appends are
    // evicted fast, so the step-6 read genuinely misses into decode_capsule.
    let engine =
        LogEngine::open_with(&dir, opts(64 << 20, 16 * 1024)).expect("open");
    rt.block_on(seed(&engine, &mut oracle, 1, 600, 10));
    engine.seal_active().expect("seal 1");
    assert_eq!(engine.sealed_segment_count(), 1);

    // Churn the capsule cache, then warm the decoded-pcol-block cache with
    // the first seal's PARTIAL tail block.
    rt.block_on(async {
        let mut after = Version::NoStream;
        for _ in 0..8 {
            let page = engine
                .read_stream("acct-0000", after, 64)
                .await
                .expect("churn");
            after = Version::At(page.last().unwrap().stream_position);
        }
        let page = engine
            .read_stream("acct-0000", Version::At(511), 88)
            .await
            .expect("warm partial tail block");
        assert_eq!(page.len(), 88);
        for (k, r) in page.iter().enumerate() {
            oracle[512 + k].assert_eq_record(r, "warm read");
        }
    });

    // Grow the segment past the first seal's coverage and re-seal.
    rt.block_on(seed(&engine, &mut oracle, 1, 3000, 10));
    engine.seal_active().expect("seal 2");

    // The poisoned read: [630,640) lives in the (now full) block 4 of the
    // SECOND sidecar. Must be byte-identical, not a panic or stale bytes.
    rt.block_on(async {
        let page = engine
            .read_stream("acct-0000", Version::At(629), 10)
            .await
            .expect("read across re-sealed block");
        assert_eq!(page.len(), 10);
        for (k, r) in page.iter().enumerate() {
            oracle[630 + k].assert_eq_record(r, "post-reseal read");
        }
    });
    // And the whole store still reads back exactly.
    rt.block_on(assert_identical(&engine, &oracle, "post-reseal"));
}

/// bn-2ib review F1, roll-sealer variant: `seal_active` a still-filling head
/// (partial footerless sidecar, generation 1), warm its partial tail block,
/// then keep appending until the segment ROLLS and the background roll-sealer
/// re-seals the same segment id with full coverage + footer (generation 2).
/// Reads across the old coverage boundary must be byte-identical.
#[test]
fn roll_reseal_after_seal_active_serves_fresh_pcol_blocks() {
    let tmp = mess_testkit::sweeping_temp_dir("owb-roll-reseal");
    let dir = tmp.path().join("store");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut oracle: Vec<Shadow> = Vec::new();
    let engine =
        LogEngine::open_with(&dir, opts(64 * 1024, 16 * 1024)).expect("open");
    rt.block_on(seed(&engine, &mut oracle, 2, 300, 10));
    let seals_before = engine.metrics().seals;
    engine.seal_active().expect("seal_active over filling head");
    assert_eq!(engine.sealed_segment_count(), 1);

    // Warm the partial tail of the seal_active sidecar's `.pcol`.
    rt.block_on(async {
        let page = engine
            .read_stream("acct-0000", Version::At(100), 50)
            .await
            .expect("warm");
        assert!(!page.is_empty());
    });

    // Fill until the head rolls; the roll-sealer then RE-seals segment 1
    // fully (bumping its install generation) plus later segments.
    rt.block_on(seed(&engine, &mut oracle, 2, 3000, 10));
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(30);
    while engine.metrics().seals < seals_before + 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "roll-sealer never re-sealed (seals={})",
            engine.metrics().seals
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    rt.block_on(assert_identical(&engine, &oracle, "post-roll-reseal"));
}

/// bn-2ib review F2 regression: a sidecar that became durable BEFORE the
/// segment's data/footer fsync (the seal pipeline's crash window) must not
/// be trusted on reopen. Simulated by rolling + sealing a segment, then
/// truncating its `.log` (stripping the footer and a chunk of committed
/// tail bytes) while leaving the CRC-valid `.pidx`/`.pcol` in place: the
/// reopen must refuse the sidecar (no valid footer, scan refutes coverage),
/// serve the surviving prefix byte-identically from the log, and never
/// fabricate the lost positions.
#[test]
fn sidecar_durable_before_data_is_not_trusted_on_reopen() {
    let tmp = mess_testkit::sweeping_temp_dir("owb-torn-seal");
    let dir = tmp.path().join("store");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut oracle: Vec<Shadow> = Vec::new();
    {
        let engine = LogEngine::open_with(&dir, opts(64 * 1024, 64 << 20))
            .expect("open");
        rt.block_on(seed(&engine, &mut oracle, 4, 2000, 5));
        await_seals(&engine, 1); // at least seg-1 rolled + sealed (footered)
    }

    // Crash-window damage: strip seg-1's footer + tail bytes, keep sidecars.
    let seg1 = dir.join("seg-00000001.log");
    let len = std::fs::metadata(&seg1).unwrap().len();
    let f = std::fs::OpenOptions::new().write(true).open(&seg1).unwrap();
    f.set_len(len - 100 - 4096).unwrap(); // trailer (100 B) + ~a few batches
    drop(f);

    let engine = LogEngine::open_with(&dir, opts(64 * 1024, 64 << 20))
        .expect("reopen after torn seal");
    // The damaged segment's sidecar must NOT be serving lost positions: the
    // surviving prefix reads back byte-identically and densely, and the read
    // stops at the hole instead of fabricating.
    let full = rt
        .block_on(engine.read_global(None, oracle.len() + 10))
        .expect("read surviving prefix");
    assert!(full.len() < oracle.len(), "some tail of seg-1 was genuinely lost");
    assert!(!full.is_empty(), "the surviving prefix is served");
    for (i, r) in full.iter().enumerate() {
        oracle[i].assert_eq_record(r, "torn-seal surviving prefix");
        // The surviving prefix is a prefix of the ORACLE, in order, with each
        // record at the exact global position the engine acked it at (bn-2di:
        // those positions are not `i`, because `$registry` records sit between
        // them — but they are still exactly what was acked, so nothing was
        // fabricated or shifted).
        assert_eq!(
            r.global_position, oracle[i].global,
            "no fabrication: each surviving record keeps its acked position"
        );
    }
}

/// An on-demand `seal_active` of a still-growing head segment: the sidecar
/// covers a prefix; reads must straddle the coverage boundary byte-exactly
/// (prefix via `.pcol`, tail via the raw log / hot index), live and across
/// a reopen.
#[test]
fn seal_active_partial_coverage_straddles_byte_identically() {
    let tmp = mess_testkit::sweeping_temp_dir("owb-seal-active");
    let dir = tmp.path().join("store");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut oracle: Vec<Shadow> = Vec::new();
    // One big segment: no rolls; seal_active seals the head's prefix.
    let engine =
        LogEngine::open_with(&dir, opts(64 << 20, 64 << 20)).expect("open");
    rt.block_on(seed(&engine, &mut oracle, 4, 600, 3));
    engine.seal_active().expect("seal_active");
    assert_eq!(engine.sealed_segment_count(), 1);

    // Keep appending past the sealed coverage.
    rt.block_on(seed(&engine, &mut oracle, 4, 300, 3));
    rt.block_on(assert_identical(&engine, &oracle, "seal_active live"));
    drop(engine);

    let engine =
        LogEngine::open_with(&dir, opts(64 << 20, 64 << 20)).expect("reopen");
    assert_eq!(engine.recover_payload_decodes(), 0);
    assert_eq!(
        engine.total_events(),
        implied_total_events(&oracle),
        "no tail lost across reopen"
    );
    rt.block_on(assert_identical(&engine, &oracle, "seal_active reopened"));
}
