//! bn-3dr regression: repeated drop→reopen cycles on ONE store never make
//! recovery *gain* events (no double-replay, no phantom/duplicate across the
//! roll/seal boundary).
//!
//! This is the engine-side guarantee that lets the soak harness treat a
//! post-reopen `engine_total > shadow_total` as a *harness* concern (spec-02 A6
//! unacked tail), not an engine bug: for a workload that awaits every append to
//! a definite result before the next (so nothing is ever submitted-but-unacked
//! at drop time), recovery must reproduce EXACTLY the acked set — never a
//! duplicate of an acked event (Z1-family double-replay) and never a fabricated
//! one. The test drives many resume-in-place cycles over a tiny segment (so each
//! cycle rolls and seals), reconciling the whole global order byte-for-byte
//! against an in-test shadow at every reopen.

use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EngineOptions, LogEngine, Version};

fn opts() -> EngineOptions {
    EngineOptions { segment_size: 8 * 1024, ..EngineOptions::default() }
}

#[tokio::test]
async fn many_reopen_cycles_never_gain_or_duplicate_events() {
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("s");
    let streams = 16usize;
    let mut heads = vec![0u64; streams]; // next stream position per stream
    let mut have = vec![false; streams];
    // The shadow: the exact acked global order as (stream_idx, stream_pos, payload).
    let mut global: Vec<(usize, u64, Vec<u8>)> = Vec::new();
    let mut nonce: u64 = 0;

    for cycle in 0..80u64 {
        let engine = LogEngine::open_with(&store, opts()).unwrap();

        // At every reopen the engine must reproduce EXACTLY the acked set — same
        // count, dense positions, byte-exact payloads (no phantom, no duplicate).
        assert_eq!(
            engine.total_events() as usize,
            global.len(),
            "cycle {cycle}: total_events gained/lost vs acked shadow"
        );
        let g = engine.read_global(None, global.len() + 16).await.unwrap();
        assert_eq!(g.len(), global.len(), "cycle {cycle}: global length");
        for (i, r) in g.iter().enumerate() {
            assert_eq!(r.global_position, i as u64, "cycle {cycle}: dense global position");
            assert_eq!(r.data, global[i].2, "cycle {cycle} gp {i}: payload drift/duplicate");
        }

        // Append a deterministic burst (varying size lands the drop at many roll
        // offsets across cycles). Every append is awaited to a definite result,
        // so nothing is ever submitted-but-unacked at the drop below.
        let burst = 20 + (cycle as usize * 7) % 40;
        for _ in 0..burst {
            let s = (nonce as usize) % streams;
            let expected = if have[s] { Version::At(heads[s] - 1) } else { Version::NoStream };
            let batch = 1 + (nonce as usize % 4);
            let mut recs = Vec::new();
            let mut payloads = Vec::new();
            for k in 0..batch {
                let mut d = Vec::new();
                d.extend_from_slice(&nonce.to_le_bytes());
                d.extend_from_slice(&(k as u64).to_le_bytes());
                recs.push(RecordToAppend { message_type: "ev".into(), data: d.clone() });
                payloads.push(d);
                nonce += 1;
            }
            let out = engine
                .append_batch(&format!("stream-{s:05}"), expected, &recs)
                .await
                .unwrap_or_else(|e| panic!("cycle {cycle}: unexpected append error {e:?}"));
            let first_sp = if have[s] { heads[s] } else { 0 };
            for (k, p) in payloads.into_iter().enumerate() {
                global.push((s, first_sp + k as u64, p));
            }
            have[s] = true;
            heads[s] = match out.version {
                Version::At(v) => v + 1,
                Version::NoStream => 0,
            };
        }
        // Graceful crash: drop drains the committer + sealer, then next loop
        // reopens the same dir and reconciles against the shadow above.
        drop(engine);
    }

    let engine = LogEngine::open_with(&store, opts()).unwrap();
    assert_eq!(engine.total_events() as usize, global.len(), "final: total_events");
    assert!(
        engine.sealed_segment_count() > 0,
        "the test must have rolled and sealed at least once"
    );
}
