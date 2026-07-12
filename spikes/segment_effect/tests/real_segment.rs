//! Real-segment integration (review/11 C3): generate a store with the
//! CURRENT engine, scan the segments with the PRODUCTION scanner, build
//! head-only SegmentEffects from real `AcceptedBatch` headers, and assert
//! effect-fold == full-scan head state.
//!
//! Heads component ONLY: production v3 segments carry no control records
//! and (per C3) their extension regions are EMPTY, so registry/snapshot/
//! frontier/dedupe components are exercised on the synthetic model, not
//! here. Structurally, this test never touches event payloads — effects
//! are built purely from batch headers (`AcceptedBatch::frames` is never
//! called), which is the "no payload decode for metadata recovery" gate
//! against real bytes.

use std::collections::BTreeMap;

use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::scanner;
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EngineOptions, LogEngine, Version};
use segment_effect::effect::{HeadTransition, SegmentEffect};
use segment_effect::kernel::KernelState;
use segment_effect::model::{Capsule, Cursor, GENESIS_ANCHOR, chain_anchor};
use segment_effect::recover::reduce_ordered_tree;

fn scratch() -> mess_testkit::SweepingTempDir {
    let home = std::env::var("HOME").expect("HOME");
    let root = std::path::PathBuf::from(home).join(".cache/mess-bench");
    mess_testkit::temp_dir_in(&root, "segeff-real")
}

/// Build a head-only effect for one REAL segment from its accepted batch
/// headers (stream_id, first_stream_version, frame_count) — no payload
/// decode, no extension-region assumptions.
fn effect_from_batches(
    segment_ordinal: u64,
    start: Cursor,
    start_anchor: [u8; 32],
    batches: &[scanner::AcceptedBatch],
) -> SegmentEffect {
    let mut heads: BTreeMap<u64, HeadTransition> = BTreeMap::new();
    let mut cursor = start;
    let mut anchor = start_anchor;
    for b in batches {
        match heads.get_mut(&b.stream_id) {
            Some(t) => {
                assert_eq!(
                    t.last_head, b.first_stream_version,
                    "intra-segment continuity from real headers"
                );
                t.last_head =
                    b.first_stream_version + u64::from(b.frame_count);
            }
            None => {
                heads.insert(
                    b.stream_id,
                    HeadTransition {
                        first_prior: b.first_stream_version,
                        last_head: b.first_stream_version
                            + u64::from(b.frame_count),
                    },
                );
            }
        }
        // Anchor chain over the LOGICAL capsule identity of the real batch
        // (the model's UserBatch canonical bytes).
        let c = Capsule::UserBatch {
            stream_id: b.stream_id,
            first_version: b.first_stream_version,
            event_count: b.frame_count,
            first_global_pos: b.first_global_pos,
        };
        anchor = chain_anchor(&anchor, &c);
        cursor.idx += 1;
        cursor.pos += u64::from(b.frame_count);
    }
    SegmentEffect {
        first_segment: segment_ordinal,
        last_segment: segment_ordinal,
        epoch: segment_ordinal + 1,
        first: start,
        last: cursor,
        first_anchor: start_anchor,
        last_anchor: anchor,
        dedupe_span: 0,
        heads,
        snapshots: BTreeMap::new(),
        frontiers: BTreeMap::new(),
        registry: BTreeMap::new(),
        alloc: BTreeMap::new(),
        dedupe: Vec::new(),
    }
}

#[test]
fn real_segments_effect_fold_equals_full_scan() {
    let scratch = scratch();
    let store_dir = scratch.path().join("store");
    // Small segments so a modest append volume rolls several times.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let engine = LogEngine::open_with(
        store_dir.clone(),
        EngineOptions { segment_size: 1024 * 1024, ..Default::default() },
    )
    .expect("open engine");
    let n_streams = 23u64;
    let appends = 2500u64;
    let payload = vec![0xabu8; 512];
    rt.block_on(async {
        let mut vers: Vec<Version> = vec![Version::NoStream; n_streams as usize];
        for i in 0..appends {
            let s = (i * 7 + i / 11) % n_streams; // uneven mix, revisits
            let stream = format!("acct-{s}");
            let records: Vec<RecordToAppend> = (0..1 + (i % 4))
                .map(|_| RecordToAppend {
                    message_type: "ev.t".to_string(),
                    data: payload.clone(),
                })
                .collect();
            let out = engine
                .append_batch(&stream, vers[s as usize], &records)
                .await
                .expect("append");
            vers[s as usize] = out.version;
        }
    });
    let total_events = engine.total_events() as u64;
    drop(engine);
    rt.shutdown_timeout(std::time::Duration::from_secs(10));

    // Enumerate segment files and scan them with the PRODUCTION scanner.
    let mut seg_paths: Vec<std::path::PathBuf> = std::fs::read_dir(&store_dir)
        .expect("store dir")
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("seg-") && n.ends_with(".log"))
                .unwrap_or(false)
        })
        .collect();
    seg_paths.sort();
    assert!(
        seg_paths.len() >= 3,
        "want several real segments, got {} (raise appends or shrink \
         segment_size)",
        seg_paths.len()
    );

    let fsrt = RealRuntime::new();
    let fs = fsrt.fs();
    let mut effects: Vec<SegmentEffect> = Vec::new();
    let mut oracle_heads: BTreeMap<u64, u64> = BTreeMap::new();
    let mut cursor = Cursor::default();
    let mut anchor = GENESIS_ANCHOR;
    let mut scanned_batches = 0usize;
    for (ord, path) in seg_paths.iter().enumerate() {
        // recover_segment_with_image is the production entry (bn-20b); we
        // take the Recovery and NEVER call .frames() on the image — heads
        // come from batch headers alone.
        let (rec, _image) =
            scanner::recover_segment_with_image(&fs, path).expect("scan");
        assert!(
            rec.header.is_some(),
            "segment header must validate: {path:?}"
        );
        // Oracle: fold accepted batch headers directly (independent path).
        for b in &rec.accepted {
            let prior = oracle_heads.get(&b.stream_id).copied().unwrap_or(0);
            assert_eq!(
                prior, b.first_stream_version,
                "real log continuity check (oracle side)"
            );
            oracle_heads.insert(
                b.stream_id,
                b.first_stream_version + u64::from(b.frame_count),
            );
        }
        // And the production scanner's own per-segment head map must agree
        // with the effect's last_head for every stream it reports.
        let e = effect_from_batches(ord as u64, cursor, anchor, &rec.accepted);
        for (&sid, &last_ver) in &rec.stream_heads {
            let t = e.heads.get(&sid).expect("scanner head implies touched");
            assert_eq!(
                t.last_head,
                last_ver + 1,
                "scanner last_stream_version vs effect head count"
            );
        }
        scanned_batches += rec.accepted.len();
        cursor = e.last;
        anchor = e.last_anchor;
        effects.push(e);
    }
    assert_eq!(cursor.pos, total_events, "scanned events == engine watermark");
    assert!(scanned_batches >= appends as usize, "all appends scanned");

    // Effect fold: ordered tree reduce + one apply == oracle fold.
    let one = reduce_ordered_tree(&effects).expect("real effects compose");
    let mut st = KernelState::new(0, 1);
    st.apply_effect(&one).expect("apply composed real effect");
    let effect_heads: BTreeMap<u64, u64> = st
        .heads
        .iter()
        .enumerate()
        .filter(|&(_, &c)| c != 0)
        .map(|(i, &c)| (i as u64, c))
        .collect();
    assert_eq!(
        effect_heads, oracle_heads,
        "real-segment effect fold != full-scan head state"
    );

    // Sequential per-segment apply agrees too.
    let mut st2 = KernelState::new(0, 1);
    for e in &effects {
        st2.apply_effect(e).expect("sequential apply");
    }
    assert_eq!(st2.digest(), st.digest());
}
