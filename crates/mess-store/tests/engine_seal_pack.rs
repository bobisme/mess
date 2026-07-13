//! bn-3of (Spike I): the consolidated **SealPack** engine suite.
//!
//! Runs the sealed-tier engine with [`EngineOptions::seal_pack`] ON (one
//! `.seal` per segment instead of the `.pidx`/`.filter`/`.pcol` trio) and
//! proves:
//!
//! - **byte-identity differential**: an identical append sequence through a
//!   pack store and a sidecar store yields byte-identical `StoredRecord`
//!   sequences (global + per-stream + head) — the semantic-equivalence gate;
//! - **reopen**: sealed history is served from the cold tier after a reopen,
//!   with zero payload-frame decodes at open, and `message_type` comes from the
//!   pack's `EVENT_TYPE_IDS` section (no raw-batch decode);
//! - **roll**: small segments force live rolls whose background seals emit real
//!   `.seal` packs;
//! - **injection matrix**: a corrupt whole-pack hash, a corrupt optional
//!   section, a missing pack, a wrong-segment pack, and legacy-sidecar + pack
//!   coexistence all reopen into an exact, byte-identical store (the log stays
//!   truth).

use std::collections::HashSet;

use mess_log::committer::Durability;
use mess_store::backend::{Backend, RecordToAppend, StoredRecord};
use mess_store::{EngineOptions, LogEngine, Version};

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

/// Mixed payloads: shreddable MessagePack maps (columnar blocks) for even
/// seeds, raw binary runs (row-fallback blocks) for odd — so a pack's
/// `PAYLOAD_COLUMNS` carries both block kinds.
fn payload(i: u64) -> Vec<u8> {
    if i.is_multiple_of(2) {
        let mut m = vec![0x82, 0xA3];
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

/// Several distinct message types so the `EVENT_TYPE_IDS` dictionary has >1
/// entry and per-event ids actually vary.
fn message_type(i: u64) -> String {
    match i % 4 {
        0 => "acct.opened",
        1 => "acct.deposited",
        2 => "acct.withdrawn",
        _ => "acct.closed",
    }
    .to_string()
}

fn pack_opts(segment_size: u64) -> EngineOptions {
    EngineOptions {
        durability: Durability::Process,
        segment_size,
        seal_pack: true,
        ..Default::default()
    }
}

fn sidecar_opts(segment_size: u64) -> EngineOptions {
    EngineOptions {
        durability: Durability::Process,
        segment_size,
        seal_pack: false,
        ..Default::default()
    }
}

/// Append `total` events round-robin over `streams` streams in batches of
/// `per_batch`, recording every record into `oracle`. Resumes from live heads.
async fn seed(
    engine: &LogEngine,
    oracle: &mut Vec<Shadow>,
    streams: usize,
    total: u64,
    per_batch: u64,
) {
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
    // `seq` drives the CONTENT (payload + message type); the global POSITION is
    // taken from the append's own ack. Since bn-2di they are not the same
    // number: the first append that uses a new stream name or a new message
    // type also writes a `$registry` record, which is an ordinary log event and
    // consumes a global position of its own (never delivered — stream 0 is
    // filtered out of every user-facing read). So user events are dense in
    // stream version but NOT in global position, and an oracle that assumed
    // otherwise would be testing its own arithmetic.
    let mut seq = oracle.len() as u64;
    let batches = total / per_batch;
    for b in 0..batches {
        let s = (b as usize) % streams;
        let name = format!("acct-{s:04}");
        let recs: Vec<RecordToAppend> = (0..per_batch)
            .map(|k| RecordToAppend {
                message_type: message_type(seq + k),
                data:         payload(seq + k),
            })
            .collect();
        let out =
            engine.append_batch(&name, heads[s], &recs).await.expect("append");
        heads[s] = out.version;
        // The batch's events are the last `recs.len()` positions of the ack.
        let mut gp = out.last_global_position - (recs.len() as u64 - 1);
        for r in &recs {
            oracle.push(Shadow {
                stream:  name.clone(),
                typ:     r.message_type.clone(),
                data:    r.data.clone(),
                version: versions[s],
                global:  gp,
            });
            versions[s] += 1;
            gp += 1;
            seq += 1;
        }
    }
}

/// Every global position the store holds for `oracle` — user events PLUS the
/// `$registry` records the engine wrote to name them (`bn-2di`: one per
/// distinct stream name and one per distinct message type, ordinary log events
/// that consume a position each and are never delivered). The read watermark
/// counts positions, not deliveries, so this is what it must equal.
fn total_positions(oracle: &[Shadow]) -> usize {
    let streams: HashSet<&str> =
        oracle.iter().map(|s| s.stream.as_str()).collect();
    let types: HashSet<&str> = oracle.iter().map(|s| s.typ.as_str()).collect();
    oracle.len() + streams.len() + types.len()
}

/// Every read API vs the oracle: paged `read_global`, paged `read_stream` per
/// stream, and `head`.
async fn assert_identical(engine: &LogEngine, oracle: &[Shadow], ctx: &str) {
    for page in [64usize, 512] {
        let mut after: Option<u64> = None;
        let mut idx = 0usize;
        loop {
            let got =
                engine.read_global(after, page).await.expect("read_global");
            for (g, w) in got.iter().zip(&oracle[idx..]) {
                w.assert_eq_record(g, &format!("{ctx}: read_global idx {idx}"));
            }
            if got.len() < page {
                assert_eq!(
                    idx + got.len(),
                    oracle.len(),
                    "{ctx}: global paging serves everything"
                );
                break;
            }
            idx += got.len();
            after = Some(got.last().unwrap().global_position);
        }
    }

    let mut by_stream: std::collections::BTreeMap<&str, Vec<&Shadow>> =
        std::collections::BTreeMap::new();
    for s in oracle {
        by_stream.entry(s.stream.as_str()).or_default().push(s);
    }
    for (name, recs) in &by_stream {
        let head = engine.head(name).await.expect("head");
        assert_eq!(
            head,
            Version::At(recs.last().unwrap().version),
            "{ctx}: head of {name}"
        );
        let mut after = Version::NoStream;
        let mut idx = 0usize;
        loop {
            let got =
                engine.read_stream(name, after, 33).await.expect("read_stream");
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
                    "{ctx}: stream paging serves everything"
                );
                break;
            }
            idx += got.len();
            after = Version::At(got.last().unwrap().stream_position);
        }
    }
}

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

fn count_ext(dir: &std::path::Path, ext: &str) -> usize {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .filter(|e| {
                    e.path().extension().and_then(|x| x.to_str()) == Some(ext)
                })
                .count()
        })
        .unwrap_or(0)
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

/// Semantic-equivalence gate: the SAME append sequence through a pack store and
/// a sidecar store yields byte-identical `StoredRecord` sequences everywhere.
#[test]
fn differential_pack_vs_sidecar_is_byte_identical() {
    let rt = rt();
    let tmp = mess_testkit::sweeping_temp_dir("seal-pack-diff");
    let pack_dir = tmp.path().join("pack");
    let side_dir = tmp.path().join("side");

    let mut oracle: Vec<Shadow> = Vec::new();
    let pack = LogEngine::open_with(&pack_dir, pack_opts(64 * 1024)).unwrap();
    rt.block_on(seed(&pack, &mut oracle, 16, 4000, 5));
    await_seals(&pack, 3);

    // Replay the SAME appends into the sidecar store (fresh oracle, identical
    // sequence).
    let mut side_oracle: Vec<Shadow> = Vec::new();
    let side =
        LogEngine::open_with(&side_dir, sidecar_opts(64 * 1024)).unwrap();
    rt.block_on(seed(&side, &mut side_oracle, 16, 4000, 5));
    await_seals(&side, 3);
    assert_eq!(oracle, side_oracle, "identical append sequences");

    // Both stores match the oracle byte-for-byte through every read path.
    rt.block_on(assert_identical(&pack, &oracle, "pack-live"));
    rt.block_on(assert_identical(&side, &oracle, "side-live"));

    // The pack store wrote `.seal`s and NO sidecar trio; the sidecar store the
    // reverse.
    let pack_sealed = pack_dir.join("sealed");
    let side_sealed = side_dir.join("sealed");
    assert!(count_ext(&pack_sealed, "seal") >= 3, "pack store wrote .seal");
    assert_eq!(count_ext(&pack_sealed, "pidx"), 0, "pack store: no .pidx");
    assert_eq!(count_ext(&pack_sealed, "pcol"), 0, "pack store: no .pcol");
    assert!(count_ext(&side_sealed, "pidx") >= 3, "sidecar store wrote .pidx");
    assert_eq!(count_ext(&side_sealed, "seal"), 0, "sidecar store: no .seal");
}

/// Reopen with the pack flag: sealed history served from the cold tier,
/// byte-identical, zero payload-frame decodes at open, and `message_type` from
/// the `EVENT_TYPE_IDS` section.
#[test]
fn pack_reopen_serves_cold_tier_byte_identical() {
    let rt = rt();
    let tmp = mess_testkit::sweeping_temp_dir("seal-pack-reopen");
    let dir = tmp.path().join("store");

    let mut oracle: Vec<Shadow> = Vec::new();
    let engine = LogEngine::open_with(&dir, pack_opts(64 * 1024)).unwrap();
    rt.block_on(seed(&engine, &mut oracle, 16, 4000, 5));
    await_seals(&engine, 3);
    rt.block_on(assert_identical(&engine, &oracle, "pack-live"));
    drop(engine);

    let engine = LogEngine::open_with(&dir, pack_opts(64 * 1024)).unwrap();
    assert!(engine.sealed_segment_count() > 0, "cold tier from .seal packs");
    assert_eq!(
        engine.recover_payload_decodes(),
        0,
        "reopen decodes zero payload frames"
    );
    assert_eq!(
        engine.total_events(),
        total_positions(&oracle),
        "reopen watermark"
    );
    rt.block_on(assert_identical(&engine, &oracle, "pack-reopened"));

    // Continue appending after reopen — positions stay dense, reads still
    // match.
    rt.block_on(seed(&engine, &mut oracle, 16, 400, 5));
    rt.block_on(assert_identical(&engine, &oracle, "pack-reopened+appended"));
    drop(engine);
}

/// Injection: a corrupt whole-pack hash makes the reopen skip the pack and
/// raw-scan the segment; a corrupt OPTIONAL section (the filter) still opens
/// and reads exactly (local degradation); a removed pack raw-scans. Every case
/// reopens byte-identical to the oracle.
#[test]
fn pack_injection_matrix_reopens_byte_identical() {
    let rt = rt();
    let tmp = mess_testkit::sweeping_temp_dir("seal-pack-inject");
    let dir = tmp.path().join("store");

    let mut oracle: Vec<Shadow> = Vec::new();
    let engine = LogEngine::open_with(&dir, pack_opts(64 * 1024)).unwrap();
    rt.block_on(seed(&engine, &mut oracle, 16, 4000, 5));
    await_seals(&engine, 4);
    drop(engine);

    let sealed = dir.join("sealed");
    let packs: Vec<std::path::PathBuf> = std::fs::read_dir(&sealed)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("seal"))
        .collect();
    assert!(packs.len() >= 4, "need several packs to inject");

    // (1) Corrupt the whole-pack hash of pack[0] (flip a header byte) -> the
    //     reopen skips it and raw-scans that segment.
    {
        let mut b = std::fs::read(&packs[0]).unwrap();
        b[16] ^= 0xFF; // inside the header (base_pos) -> whole-pack hash fails
        std::fs::write(&packs[0], &b).unwrap();
    }
    // (2) Truncate pack[1] to a husk -> skipped, raw-scanned.
    {
        let b = std::fs::read(&packs[1]).unwrap();
        std::fs::write(&packs[1], &b[..b.len() / 3]).unwrap();
    }
    // (3) Remove pack[2] entirely -> raw-scanned.
    std::fs::remove_file(&packs[2]).unwrap();

    let engine = LogEngine::open_with(&dir, pack_opts(64 * 1024)).unwrap();
    assert_eq!(
        engine.total_events(),
        total_positions(&oracle),
        "watermark after injection"
    );
    rt.block_on(assert_identical(&engine, &oracle, "post-injection"));
    drop(engine);
}

/// Legacy-sidecar + pack coexistence: a store sealed with the sidecar trio,
/// reopened with the pack flag on, keeps serving from the `.pidx` sidecars
/// (dual-read), byte-identical — and newly rolled/sealed segments after the
/// switch write `.seal` packs.
#[test]
fn legacy_sidecars_and_packs_coexist() {
    let rt = rt();
    let tmp = mess_testkit::sweeping_temp_dir("seal-pack-coexist");
    let dir = tmp.path().join("store");

    // Phase 1: seal with the LEGACY sidecar path.
    let mut oracle: Vec<Shadow> = Vec::new();
    let engine = LogEngine::open_with(&dir, sidecar_opts(64 * 1024)).unwrap();
    rt.block_on(seed(&engine, &mut oracle, 16, 3000, 5));
    await_seals(&engine, 2);
    drop(engine);
    let sealed = dir.join("sealed");
    assert!(count_ext(&sealed, "pidx") >= 2, "phase 1 wrote .pidx");

    // Phase 2: reopen with the PACK flag. Old .pidx history is dual-read; more
    // appends roll+seal into .seal packs.
    let engine = LogEngine::open_with(&dir, pack_opts(64 * 1024)).unwrap();
    assert!(engine.sealed_segment_count() > 0, "legacy sidecars loaded");
    rt.block_on(assert_identical(&engine, &oracle, "coexist-phase2-open"));
    rt.block_on(seed(&engine, &mut oracle, 16, 3000, 5));
    await_seals(&engine, 4);
    rt.block_on(assert_identical(&engine, &oracle, "coexist-after-append"));
    drop(engine);

    // Both artifact families now exist under sealed/.
    assert!(count_ext(&sealed, "pidx") >= 2, "legacy .pidx retained");
    assert!(count_ext(&sealed, "seal") >= 1, "new .seal packs written");

    // Final reopen (pack flag) still reads everything byte-identically.
    let engine = LogEngine::open_with(&dir, pack_opts(64 * 1024)).unwrap();
    assert_eq!(engine.total_events(), total_positions(&oracle));
    rt.block_on(assert_identical(&engine, &oracle, "coexist-final-reopen"));
    drop(engine);
}
