//! The `fold_cert` spike's negative suite (bn-1d0), ported against the **real**
//! per-batch crypto chain and the batch-granular verification of
//! `docs/spec/05-fold-certificates.md` §7/§7.0.
//!
//! Every modeled attack MUST be detected. Where the spike promised
//! frame-precise errors (`ChainBreakFrameHash{i}`, `ChainBreakPrev{i+1}`) these
//! are **regenerated** for the batch-granular surface (§7.0, §11): the G10
//! layout (§6.2) stores no per-frame hash, so a tail payload tamper localizes
//! to the *batch* — surfaced as `ChainBreakPrev{next batch base}` or, for the
//! final tail batch, `HeadMismatch`. The cross-stream attack (attack 6) MUST be
//! caught by the genesis `stream_id` binding (§3.1); the counterfactual (6b)
//! proves the binding is load-bearing by removing it and watching the same
//! attack succeed.

use mess_log::certificates::*;
use mess_log::fold_chain::{Hash, advance, blob_hash};

// --- Reference aggregate (toy bank account, mirrors the spike) --------------

#[derive(Clone, Debug, PartialEq, Eq)]
struct Account {
    balance:  i64,
    tx_count: u64,
}
impl Aggregate for Account {
    const FOLD_VERSION: u32 = 1;

    fn init() -> Self { Account { balance: 0, tx_count: 0 } }

    fn apply(&mut self, payload: &[u8]) {
        let tag = payload[0];
        let amount = u64::from_le_bytes(payload[1..9].try_into().unwrap());
        match tag {
            0 => self.balance += amount as i64,
            1 => self.balance -= amount as i64,
            _ => {}
        }
        self.tx_count += 1;
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut o = Vec::with_capacity(16);
        o.extend_from_slice(&self.balance.to_le_bytes());
        o.extend_from_slice(&self.tx_count.to_le_bytes());
        o
    }

    fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.len() != 16 {
            return None;
        }
        Some(Account {
            balance:  i64::from_le_bytes(b[0..8].try_into().ok()?),
            tx_count: u64::from_le_bytes(b[8..16].try_into().ok()?),
        })
    }
}

fn ev(tag: u8, amount: u64) -> Vec<u8> {
    let mut p = vec![0u8; 32];
    p[0] = tag;
    p[1..9].copy_from_slice(&amount.to_le_bytes());
    p
}

fn payloads(n: u64) -> Vec<Vec<u8>> {
    (0..n).map(|i| ev((i % 3 == 2) as u8, 10 + i)).collect()
}

/// A stream of `n` events, `batch_size`-event batches, real bound chain.
fn stream(id: u64, n: u64, batch_size: usize) -> StreamCert {
    build_cert(id, &payloads(n), batch_size, None)
}

// --- 1. Corrupted snapshot blob -> StateHashMismatch ------------------------

#[test]
fn corrupted_snapshot_blob_detected() {
    let s = stream(1, 30, 10);
    let (r, mut blob) = take_snapshot::<Account>(&s, 19);
    blob[3] ^= 0xFF; // flip a byte in the blob
    let err = load_verified::<Account>(&s, Some((&r, &blob))).unwrap_err();
    assert_eq!(err, VerifyError::StateHashMismatch);
}

// --- 2. Snapshot claiming the wrong version -> Path A rejects ---------------

#[test]
fn snapshot_claiming_wrong_version_detected() {
    let s = stream(2, 30, 10);
    let (mut r, blob) = take_snapshot::<Account>(&s, 19);
    r.stream_version = 25; // lie: state/hash are for v=19
    let err = load_verified::<Account>(&s, Some((&r, &blob))).unwrap_err();
    // Path A recomputes h[25] from frame 25 and it won't equal the stored
    // h[19].
    assert_eq!(
        err,
        VerifyError::PrefixHashMismatch { path: VerifyPath::FromFrameV }
    );
}

// --- 2b. Wrong version caught by Path B when frame v is compacted -----------

#[test]
fn wrong_version_detected_by_path_b_when_frame_v_compacted() {
    // batch_size 13 => batches [0..12][13..25][26..38][39..49]. v=25 is the
    // last frame of batch 1; v+1=26 is the first frame of batch 2, so
    // compacting batch 1 removes frame 25 (Path A) but keeps frame 26 (Path
    // B).
    let mut s = stream(3, 50, 13);
    let (mut r, blob) = take_snapshot::<Account>(&s, 19);
    r.stream_version = 25;
    s.batches.remove(1); // compact the batch holding frames 13..=25
    let err = load_verified::<Account>(&s, Some((&r, &blob))).unwrap_err();
    // Path A unavailable (frame 25 gone); Path B: frame 26's prev is h[25] !=
    // h[19].
    assert_eq!(
        err,
        VerifyError::PrefixHashMismatch { path: VerifyPath::FromFrameVPlus1 }
    );
}

// --- 3a. Tampered tail event -> batch-granular ChainBreakPrev ---------------

#[test]
fn tampered_tail_event_detected_at_next_batch_boundary() {
    let mut s = stream(4, 50, 10); // batches of 10; frame 35 in batch [30..39]
    let (r, blob) = take_snapshot::<Account>(&s, 19);
    s.batches[3].frames[5].payload[1] ^= 0xFF; // tamper frame 35
    let err = load_verified::<Account>(&s, Some((&r, &blob))).unwrap_err();
    // §7.0: frame_hash is not stored, so the tamper localizes to the batch and
    // surfaces at the NEXT batch's crypto_chain continuity check (base 40).
    assert_eq!(err, VerifyError::ChainBreakPrev { at_version: 40 });
}

// --- 3b. Tamper + "fixed frame_hash": no stored frame_hash exists, so this ---
// collapses to the same batch-granular detection (the spike's frame-precise
// ChainBreakPrev{36} is retired, §11).

#[test]
fn tampered_tail_event_with_no_stored_hash_still_caught_at_batch_boundary() {
    let mut s = stream(5, 50, 10);
    let (r, blob) = take_snapshot::<Account>(&s, 19);
    // There is nothing per-frame to "fix up" (§6.2); tamper is the whole
    // attack.
    s.batches[3].frames[5].payload[1] ^= 0xFF;
    let err = load_verified::<Account>(&s, Some((&r, &blob))).unwrap_err();
    assert_eq!(err, VerifyError::ChainBreakPrev { at_version: 40 });
}

// --- 3c. Tamper in the FINAL tail batch -> only the head anchor catches it ---

#[test]
fn tampered_last_event_caught_only_by_head_anchor() {
    let mut s = stream(6, 50, 10); // frame 49 in the final batch [40..49]
    let (r, blob) = take_snapshot::<Account>(&s, 19);
    s.batches[4].frames[9].payload[1] ^= 0xFF; // tamper frame 49
    let err = load_verified::<Account>(&s, Some((&r, &blob))).unwrap_err();
    assert!(matches!(err, VerifyError::HeadMismatch { .. }));
}

// --- 3d. Tamper BEFORE the snapshot point -> invisible to snapshot load,
// ------ caught by full verified replay.

#[test]
fn tampered_prefix_event_invisible_to_load_but_caught_by_full_replay() {
    let mut s = stream(7, 50, 10);
    let (r, blob) = take_snapshot::<Account>(&s, 19);
    s.batches[0].frames[5].payload[1] ^= 0xFF; // tamper frame 5 (in the prefix)

    // Snapshot load recomputes h[19] from batch [10..19]'s crypto_chain, never
    // reads frame 5, so it passes. Document, don't hide.
    let out = load_verified::<Account>(&s, Some((&r, &blob))).unwrap();
    assert!(!out.rebuilt_by_replay);

    // Full verified replay from genesis catches it at the next batch boundary
    // (batch [10..19], base 10) — batch-granular, ChainBreakFrameHash retired.
    let err = full_replay_verified::<Account>(&s).unwrap_err();
    assert_eq!(err, VerifyError::ChainBreakPrev { at_version: 10 });
}

// --- 4. fold_version mismatch -> invalidate + rebuild by replay -------------

#[test]
fn fold_version_mismatch_invalidates_and_rebuilds() {
    let s = stream(8, 40, 8);
    let (mut r, blob) = take_snapshot::<Account>(&s, 19);
    r.fold_version = Account::FOLD_VERSION - 1; // simulate older fold logic
    let out = load_verified::<Account>(&s, Some((&r, &blob))).unwrap();
    assert!(
        out.rebuilt_by_replay,
        "must rebuild by replay, not trust the stale fold"
    );
    assert_eq!(out.tail_len, 40);
    assert_eq!(out.state, full_replay_verified::<Account>(&s).unwrap());
}

// --- 5a. Truncated tail (head anchor intact) -> HeadMismatch ----------------

#[test]
fn truncated_tail_detected() {
    let mut s = stream(9, 50, 10);
    let (r, blob) = take_snapshot::<Account>(&s, 19);
    // Lose the last 5 frames; the durable head anchor (h[49]) is unchanged.
    s.batches[4].frames.truncate(5); // batch [40..44] now
    let err = load_verified::<Account>(&s, Some((&r, &blob))).unwrap_err();
    assert!(matches!(err, VerifyError::HeadMismatch { .. }));
}

// --- 5b. Truncation BELOW the snapshot version -> SnapshotBeyondHead --------

#[test]
fn truncation_below_snapshot_version_detected() {
    let mut s = stream(10, 50, 10);
    let (r, blob) = take_snapshot::<Account>(&s, 19);
    s.batches.truncate(1); // keep only frames 0..=9
    let err = load_verified::<Account>(&s, Some((&r, &blob))).unwrap_err();
    assert_eq!(
        err,
        VerifyError::SnapshotBeyondHead {
            snapshot_version: 19,
            head:             Some(9),
        }
    );
}

// --- 5c. Reordered tail (two adjacent frames swapped) -> VersionOutOfOrder ---

#[test]
fn reordered_tail_detected() {
    let mut s = stream(11, 50, 10);
    let (r, blob) = take_snapshot::<Account>(&s, 19);
    // Swap frames 30 and 31 (both in batch [30..39]); FrameRec carries version.
    s.batches[3].frames.swap(0, 1);
    let err = load_verified::<Account>(&s, Some((&r, &blob))).unwrap_err();
    assert_eq!(err, VerifyError::VersionOutOfOrder { expected: 30, got: 31 });
}

// --- 6. Cross-stream confusion -> rejected by the genesis stream_id binding --

#[test]
fn cross_stream_snapshot_rejected_by_genesis_binding() {
    // Two streams with IDENTICAL payloads; present A's snapshot when loading B.
    let a = stream(100, 30, 10);
    let b = stream(200, 30, 10); // identical payload sequence, different id
    let (mut r, blob) = take_snapshot::<Account>(&a, 19);
    r.stream_id = 200; // attacker relabels A's snapshot as B's
    // Even with identical payloads, h chains differ: h[-1] binds the stream_id.
    let err = load_verified::<Account>(&b, Some((&r, &blob))).unwrap_err();
    assert_eq!(
        err,
        VerifyError::PrefixHashMismatch { path: VerifyPath::FromFrameV }
    );
}

// --- 6b. COUNTERFACTUAL: without stream_id in the genesis, the attack
// SUCCEEDS.

#[test]
fn cross_stream_snapshot_accepted_without_genesis_binding() {
    // An unbound genesis shared by all streams (the rejected §3.1 alternative).
    let unbound: Hash = blob_hash(b"mess-stream-unbound");
    let ps = payloads(30);
    let a = build_cert(100, &ps, 10, Some(unbound));
    let b = build_cert(200, &ps, 10, Some(unbound)); // identical chain values

    // Build A's snapshot against the unbound genesis by hand (take_snapshot
    // uses the real bound genesis, which is exactly what this
    // counterfactual drops).
    let mut h = unbound;
    let mut state = Account::init();
    for v in 0..=19u64 {
        let blk = a
            .batches
            .iter()
            .find(|bt| {
                bt.base_version <= v
                    && v < bt.base_version + bt.frames.len() as u64
            })
            .unwrap();
        let idx = (v - blk.base_version) as usize;
        let payload = &blk.frames[idx].payload;
        h = advance(&h, v, payload);
        state.apply(payload);
    }
    let blob = state.to_bytes();
    let forged = SnapshotRef {
        stream_id:           200, // relabeled to B
        stream_version:      19,
        fold_version:        Account::FOLD_VERSION,
        covers_empty_prefix: false,
        event_prefix_hash:   h,
        state_hash:          blob_hash(&blob),
    };
    // The forged cross-stream snapshot passes every check: the attack succeeds,
    // proving the stream_id binding is the load-bearing protection.
    let out = load_verified::<Account>(&b, Some((&forged, &blob))).unwrap();
    assert!(!out.rebuilt_by_replay);
}

// --- 7. Forged frame header fooling Path B, caught by tail replay -----------

#[test]
fn forged_prev_fooling_path_b_caught_by_tail_replay() {
    let bogus: Hash = [0xAB; 32];

    // (a) With frame 19 retained, Path A rejects first (recomputed h[19] !=
    // bogus).
    {
        let mut s = stream(12, 50, 10);
        let (mut r, blob) = take_snapshot::<Account>(&s, 19);
        r.event_prefix_hash = bogus;
        s.batches[2].crypto_chain = bogus; // forge batch [20..29]'s entry
        let err = load_verified::<Account>(&s, Some((&r, &blob))).unwrap_err();
        assert_eq!(
            err,
            VerifyError::PrefixHashMismatch { path: VerifyPath::FromFrameV }
        );
    }

    // (b) With frame 19's batch compacted (Path A unavailable), Path B is
    // fooled by the forged crypto_chain — but the chain then breaks at the
    // NEXT batch boundary during tail replay (batch [30..39], base 30).
    {
        let mut s = stream(12, 50, 10);
        let (mut r, blob) = take_snapshot::<Account>(&s, 19);
        r.event_prefix_hash = bogus;
        s.batches[2].crypto_chain = bogus; // frame 20's prev now == bogus (Path B passes)
        s.batches.remove(1); // compact batch [10..19] so Path A is unavailable
        let err = load_verified::<Account>(&s, Some((&r, &blob))).unwrap_err();
        assert_eq!(err, VerifyError::ChainBreakPrev { at_version: 30 });
    }
}
