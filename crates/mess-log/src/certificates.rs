//! Fold certificates and `load_verified` (`bn-1d0`), implementing
//! **[`docs/spec/05-fold-certificates.md`] §2, §4, §7** against the **real**
//! per-batch crypto chain (§3, §6) of [`crate::fold_chain`].
//!
//! # What a certificate proves (§1.1 — a normative scope limit)
//!
//! [`load_verified`] proves that a stored snapshot summarizes **the exact
//! committed event prefix** of a stream — not a stale prefix, not a prefix of a
//! *different* stream (the genesis `stream_id` binding, §3.1), not a corrupted
//! blob, and not a truncated or reordered log. It proves **prefix identity and
//! integrity**. It does **NOT** prove that the fold code (`apply`) is
//! semantically correct — that is `fold_version` + the drift test (§9). Both
//! scope limits are stated here per §1.1 / §9.
//!
//! # The verification model (log-side)
//!
//! This module owns the *algorithm* over an in-memory view of a stream's
//! retained batches ([`StreamCert`]); wiring the real `mess-store`
//! `SnapshotRef` and on-disk reads into it is the dependent
//! snapshot-certificate bone. The chain values are real (BLAKE3, byte-exact to
//! §3); the tail replay is **batch-granular** (§7.0) because the G10 layout
//! (§6.2) stores no per-frame hash.
//!
//! # Certification paths (§7.1) and why replay is never skipped
//!
//! The prefix claim `event_prefix_hash == h[v]` is discharged by any of Path A
//! (recompute `h[v]` from frame `v`), Path B (read frame `v+1`'s chain value),
//! Path C (a durable retention anchor). **The tail replay (§7 step 6) and the
//! head anchor (§7 step 7) run regardless of which prefix path was used** —
//! they are the proof; the prefix path only certifies the starting point. A
//! forged `prev_stream_hash` that fools Path B still breaks during tail replay
//! at the next batch's `crypto_chain` continuity check and at the head anchor;
//! skipping replay because "Path B passed" is **unsound and MUST NOT** be done
//! (§7.1 Path B, normative).
//!
//! [`docs/spec/05-fold-certificates.md`]: ../../../../docs/spec/05-fold-certificates.md

use crate::fold_chain::{
    self, Hash, blob_hash, chain_at, chain_step, frame_hash, genesis,
};
use crate::footer_ext::SnapshotAnchor;

/// An aggregate: the fold that snapshots summarize (§9). In the real system
/// this is `#[aggregate(fold_version = N)]` on a derive; here it is a trait so
/// the verification algorithm and the ported attack suite can exercise it.
pub trait Aggregate: Sized {
    /// The explicit, human-bumped semantic version of the fold (§9). A snapshot
    /// whose `fold_version` differs is invalidated and rebuilt by replay.
    const FOLD_VERSION: u32;
    /// `fold_init`: the initial state, having applied nothing (§4.2).
    fn init() -> Self;
    /// Fold one event's committed payload into the state.
    fn apply(&mut self, payload: &[u8]);
    /// Serialize the state to the snapshot blob.
    fn to_bytes(&self) -> Vec<u8>;
    /// Deserialize a snapshot blob, or `None` on a malformed blob
    /// (`StateDecode`).
    fn from_bytes(bytes: &[u8]) -> Option<Self>;
}

/// One retained frame in the verification view: its committed version and
/// on-disk payload. No per-frame hash is stored (§6.2); all chain values are
/// recomputed from the batch's `crypto_chain`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameRec {
    /// The frame's committed `stream_version` (0-based).
    pub version: u64,
    /// The payload bytes exactly as committed on disk (§3.2).
    pub payload: Vec<u8>,
}

/// One retained batch: a contiguous ascending run of one stream's frames
/// (§6.2), plus the single stored `crypto_chain = h[base_version - 1]` (§6.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchRec {
    /// The `stream_version` of this batch's first frame.
    pub base_version: u64,
    /// The stored per-batch chain entry `h[base_version - 1]` (§6.2). For the
    /// batch containing frame 0 this is the genesis `h[-1]`.
    pub crypto_chain: Hash,
    /// The batch's frames, in ascending version order.
    pub frames:       Vec<FrameRec>,
}

impl BatchRec {
    fn last_version(&self) -> Option<u64> {
        self.frames.last().map(|f| f.version)
    }

    fn payload_refs(&self) -> impl Iterator<Item = &[u8]> + Clone {
        self.frames.iter().map(|f| f.payload.as_slice())
    }
}

/// The untrusted log view a verifier reads for one stream: its retained batches
/// (prefix + tail), the durable Tier-1 head anchor (§5), and any Path-C
/// retention anchors (§8.2). Every field is checked; nothing here is trusted.
#[derive(Debug, Clone)]
pub struct StreamCert {
    /// Interned stream id (D3); the genesis binding derives from it (§3.1).
    pub stream_id:        u64,
    /// Retained batches in ascending version order (contiguous within the
    /// retained range).
    pub batches:          Vec<BatchRec>,
    /// The durable head anchor `A(S) = (head_version, head_hash)` (§5), read
    /// from the sealed footer's `StreamHeadTable` (or the active in-memory
    /// head for an unsealed tail). `None` only for a chain-disabled /
    /// anchorless stream (degraded verification, §8.1).
    pub head_anchor:      Option<(u64, Hash)>,
    /// Durable Path-C retention anchors (§8.2), one per certified snapshot
    /// version whose frames were compacted.
    pub snapshot_anchors: Vec<SnapshotAnchor>,
}

impl StreamCert {
    /// The genesis `h[-1]` for this stream (§3.1).
    #[must_use]
    pub fn genesis(&self) -> Hash { genesis(self.stream_id) }

    /// The highest retained frame version, or `None` if no frames are retained.
    #[must_use]
    pub fn last_retained_version(&self) -> Option<u64> {
        self.batches.iter().filter_map(BatchRec::last_version).max()
    }

    /// The committed event count used by the §7 step-3 bound check: one past
    /// the highest retained frame version (matching the spike's
    /// `next_version`), or, when no frames are retained (pure Path-C), one
    /// past the anchor's head.
    #[must_use]
    pub fn committed_count(&self) -> u64 {
        match self.last_retained_version() {
            Some(v) => v + 1,
            None => self.head_anchor.map_or(0, |(hv, _)| hv + 1),
        }
    }

    /// The batch containing frame `version`, if retained.
    fn batch_of(&self, version: u64) -> Option<&BatchRec> {
        self.batches.iter().find(|b| match (b.base_version, b.last_version()) {
            (base, Some(last)) => base <= version && version <= last,
            _ => false,
        })
    }
}

/// The certification path that discharged (or rejected) the prefix claim
/// (§7.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyPath {
    /// Path A — recompute `h[v]` from frame `v`'s payload + `prev_stream_hash`
    /// (the latter recomputed intra-batch from `crypto_chain`). Authoritative.
    FromFrameV,
    /// Path B — frame `v+1`'s `prev_stream_hash` equals `event_prefix_hash`. A
    /// cheap pre-check; sound only in combination with tail replay (§7.1).
    FromFrameVPlus1,
    /// Path C — a durable footer `SnapshotAnchor` records `h[v]` (§8.2).
    FromRetentionAnchor,
    /// The empty-prefix check: `event_prefix_hash == genesis` (§4.2).
    EmptyPrefix,
}

/// The certificate a snapshot carries (§2.1). Stored untrusted; every field is
/// checked. Field semantics match the normative `SnapshotRef` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRef {
    /// Interned stream id. Cheap first-line check; the genesis binding (§3.1)
    /// is the real protection.
    pub stream_id:           u64,
    /// 0-based index of the **last** event summarized (§4.1). Ignored when
    /// `covers_empty_prefix`.
    pub stream_version:      u64,
    /// Explicit, human-bumped semantic version of the fold (§9).
    pub fold_version:        u32,
    /// `flags` bit 0 = `covers_empty_prefix` (§4.2). Modeled as a bool.
    pub covers_empty_prefix: bool,
    /// The chain value `h[stream_version]` (or the genesis when
    /// `covers_empty_prefix`). Proves prefix identity (§2.1).
    pub event_prefix_hash:   Hash,
    /// `BLAKE3(state_blob)`. Proves blob integrity (§2.1).
    pub state_hash:          Hash,
}

/// The typed outcomes of `load_verified` (§11). `ChainBreakFrameHash` is
/// **retired** (§7.0): the G10 layout stores no per-frame hash, so payload
/// tampering is localized to the **batch**, surfaced as
/// [`VerifyError::ChainBreakPrev`] (or [`VerifyError::HeadMismatch`] for the
/// final tail batch).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VerifyError {
    /// `ref.stream_id` names a different stream (§7 step 1).
    #[error("stream id mismatch: expected {expected}, got {got}")]
    StreamIdMismatch { expected: u64, got: u64 },
    /// `ref.stream_version >= committed count` (§7 step 3).
    #[error(
        "snapshot beyond head: snapshot_version {snapshot_version}, head \
         {head:?}"
    )]
    SnapshotBeyondHead { snapshot_version: u64, head: Option<u64> },
    /// Blob does not hash to `state_hash` (§7 step 4).
    #[error("state hash mismatch: blob does not match state_hash")]
    StateHashMismatch,
    /// Blob fails to deserialize (§7 step 4).
    #[error("state decode failed")]
    StateDecode,
    /// A certification path (A/B/C, or the empty-prefix check) rejected
    /// `event_prefix_hash` (§7 step 5).
    #[error("prefix hash mismatch on path {path:?}")]
    PrefixHashMismatch { path: VerifyPath },
    /// No frame and no `SnapshotAnchor` can certify the prefix (§7 step 5).
    #[error("no certification path for version {version}")]
    NoCertificationPath { version: u64 },
    /// A tail frame at position `i` claims a different version (reorder, §7
    /// step 6).
    #[error("version out of order: expected {expected}, got {got}")]
    VersionOutOfOrder { expected: u64, got: u64 },
    /// At a tail **batch boundary**, the batch whose base version is
    /// `at_version` carries a stored `crypto_chain` that does not equal the
    /// running chain value carried out of the previous batch (§7 step 6).
    /// Batch-granular (§7.0): it localizes any divergence to the region
    /// ending at this boundary — a payload tamper in the preceding tail
    /// batch(es) or a spliced/reordered batch. Never raised with
    /// `at_version == base_B0` for a mid-batch first tail batch (that
    /// boundary check is skipped, §7 step 6).
    #[error("chain break at batch boundary, base version {at_version}")]
    ChainBreakPrev { at_version: u64 },
    /// Final chain value != the durable head anchor (§7 step 7): truncation, a
    /// consistent whole-suffix rewrite, or a payload tamper in the **final**
    /// tail batch (§7.0).
    #[error("head mismatch: computed != durable head anchor")]
    HeadMismatch { computed: Hash, expected: Hash },
}

/// The result of a successful [`load_verified`].
#[derive(Debug)]
pub struct LoadOutcome<A> {
    /// The verified aggregate state (snapshot + tail replay, or full rebuild).
    pub state:             A,
    /// Which prefix-certification paths were checked (all available were run).
    pub paths_used:        Vec<VerifyPath>,
    /// Number of tail events replayed after the snapshot.
    pub tail_len:          u64,
    /// True if the snapshot was unusable (`fold_version` mismatch / missing)
    /// and the state was rebuilt by full verified replay (§7 step 2).
    pub rebuilt_by_replay: bool,
}

/// Discharge the prefix claim `r.event_prefix_hash == h[v]` by every available
/// path (§7.1), requiring at least one. All available paths are run and any
/// mismatch is a hard [`VerifyError::PrefixHashMismatch`].
fn certify_prefix(
    cert: &StreamCert,
    r: &SnapshotRef,
) -> Result<Vec<VerifyPath>, VerifyError> {
    let v = r.stream_version;
    let mut paths = Vec::new();

    // Path A: recompute h[v] from frame v (payload) + prev_stream_hash (from
    // the batch's crypto_chain, §6.4). frame_hash is ALWAYS recomputed
    // (§3.2).
    if let Some(b) = cert.batch_of(v) {
        let Some((_prev, h_v)) =
            chain_at(&b.crypto_chain, b.base_version, b.payload_refs(), v)
        else {
            return Err(VerifyError::NoCertificationPath { version: v });
        };
        if h_v != r.event_prefix_hash {
            return Err(VerifyError::PrefixHashMismatch {
                path: VerifyPath::FromFrameV,
            });
        }
        paths.push(VerifyPath::FromFrameV);
    }

    // Path B: frame v+1 carries h[v] as its prev_stream_hash (§7.1). Cheap
    // pre-check; the tail replay + head anchor are the proof.
    if let Some(b) = cert.batch_of(v + 1)
        && let Some((prev, _h)) =
            chain_at(&b.crypto_chain, b.base_version, b.payload_refs(), v + 1)
    {
        if prev != r.event_prefix_hash {
            return Err(VerifyError::PrefixHashMismatch {
                path: VerifyPath::FromFrameVPlus1,
            });
        }
        paths.push(VerifyPath::FromFrameVPlus1);
    }

    // Path C: a durable SnapshotAnchor records h[v] (§8.2). No frame reads.
    if let Some(a) = cert.snapshot_anchors.iter().find(|a| a.version == v) {
        if a.chain_hash != r.event_prefix_hash {
            return Err(VerifyError::PrefixHashMismatch {
                path: VerifyPath::FromRetentionAnchor,
            });
        }
        paths.push(VerifyPath::FromRetentionAnchor);
    }

    if paths.is_empty() {
        return Err(VerifyError::NoCertificationPath { version: v });
    }
    Ok(paths)
}

/// Replay the tail `start_version ..= head`, **batch by batch** (§7 step 6),
/// seeding the running chain at `seed_h` (the certified `h[v]`, or the genesis
/// for a full replay). Applies each payload, recomputes the chain, and ends at
/// the durable head anchor (§7 step 7). Returns the number of events applied.
fn replay_tail<A: Aggregate>(
    cert: &StreamCert,
    start_version: u64,
    seed_h: Hash,
    state: &mut A,
) -> Result<u64, VerifyError> {
    let mut h = seed_h;
    let mut expected = start_version;
    let mut applied = 0u64;
    let mut first_tail = true;

    for b in &cert.batches {
        // Skip batches entirely below the replay start (they are prefix).
        match b.last_version() {
            Some(last) if last < start_version => continue,
            None => continue,
            _ => {}
        }
        let base = b.base_version;
        // First-partial-batch exception (§7 step 6): when v is mid-batch, B0's
        // stored crypto_chain is h[base-1], NOT the certified seed h[v], so its
        // boundary check is skipped — the deferred comparison lands at the next
        // batch boundary (or the head anchor).
        let mid_first = first_tail && base < start_version;
        if !mid_first {
            // Batch-boundary continuity — the one independent check per batch.
            if b.crypto_chain != h {
                return Err(VerifyError::ChainBreakPrev { at_version: base });
            }
        }
        for (k, frame) in b.frames.iter().enumerate() {
            let pos_v = base + k as u64;
            // Skip the already-summarized prefix of a mid-batch first tail
            // batch (re-applying would double-apply state; the seed
            // already == h[v]).
            if pos_v < start_version {
                continue;
            }
            if frame.version != expected {
                return Err(VerifyError::VersionOutOfOrder {
                    expected,
                    got: frame.version,
                });
            }
            let fh = frame_hash(expected, &frame.payload);
            h = chain_step(&h, &fh, expected);
            state.apply(&frame.payload);
            expected += 1;
            applied += 1;
        }
        first_tail = false;
    }

    // Head anchor (§7 step 7): catches truncation and any consistent whole-
    // suffix rewrite, and is the surfacing point for a tamper in the final
    // batch.
    if let Some((_hv, head_hash)) = cert.head_anchor
        && h != head_hash
    {
        return Err(VerifyError::HeadMismatch {
            computed: h,
            expected: head_hash,
        });
    }
    Ok(applied)
}

/// Full verified replay from genesis (no usable snapshot): the §7 step-2
/// rebuild path and the from-scratch load. Seeds at the genesis `h[-1]` and
/// replays the whole retained log, ending at the head anchor.
pub fn full_replay_verified<A: Aggregate>(
    cert: &StreamCert,
) -> Result<A, VerifyError> {
    let mut state = A::init();
    replay_tail(cert, 0, cert.genesis(), &mut state)?;
    Ok(state)
}

/// `load_verified::<A>(stream_id)` (§7). Runs the steps in order against the
/// real per-batch crypto chain; returns the verified state or exactly one
/// [`VerifyError`].
///
/// **Scope (normative, §1.1 / §9):** this proves the snapshot summarizes the
/// exact committed prefix (prefix identity + integrity); it does **NOT** prove
/// the fold code was correct — that is `fold_version` + the drift test.
pub fn load_verified<A: Aggregate>(
    cert: &StreamCert,
    snapshot: Option<(&SnapshotRef, &[u8])>,
) -> Result<LoadOutcome<A>, VerifyError> {
    let Some((r, blob)) = snapshot else {
        // No snapshot: full verified replay from genesis.
        let state = full_replay_verified::<A>(cert)?;
        let tail_len = cert.committed_count();
        return Ok(LoadOutcome {
            state,
            paths_used: vec![],
            tail_len,
            rebuilt_by_replay: true,
        });
    };

    // Step 1 — stream identity (redundant with the genesis binding, clearer
    // error).
    if r.stream_id != cert.stream_id {
        return Err(VerifyError::StreamIdMismatch {
            expected: cert.stream_id,
            got:      r.stream_id,
        });
    }

    // Step 2 — fold version. A mismatch is NOT an error: invalidate + rebuild
    // by full verified replay (§7 step 2, the deploy story).
    if r.fold_version != A::FOLD_VERSION {
        let state = full_replay_verified::<A>(cert)?;
        let tail_len = cert.committed_count();
        return Ok(LoadOutcome {
            state,
            paths_used: vec![],
            tail_len,
            rebuilt_by_replay: true,
        });
    }

    // Step 4 (blob integrity) is shared; do it once. Decode after the bound
    // check so a beyond-head snapshot reports SnapshotBeyondHead first.
    if r.covers_empty_prefix {
        return load_empty_prefix::<A>(cert, r, blob);
    }

    // Step 3 — snapshot bound.
    let count = cert.committed_count();
    if r.stream_version >= count {
        return Err(VerifyError::SnapshotBeyondHead {
            snapshot_version: r.stream_version,
            head:             cert.last_retained_version(),
        });
    }

    // Step 4 — blob integrity + decode.
    if blob_hash(blob) != r.state_hash {
        return Err(VerifyError::StateHashMismatch);
    }
    let mut state = A::from_bytes(blob).ok_or(VerifyError::StateDecode)?;

    // Step 5 — prefix certificate (every available path; ≥1 required).
    let paths_used = certify_prefix(cert, r)?;

    // Steps 6 + 7 — batch-granular tail replay seeded at the certified h[v],
    // then the durable head anchor.
    let tail_len = replay_tail(
        cert,
        r.stream_version + 1,
        r.event_prefix_hash,
        &mut state,
    )?;

    Ok(LoadOutcome { state, paths_used, tail_len, rebuilt_by_replay: false })
}

/// The empty-prefix load (§4.2): `event_prefix_hash == genesis`, blob decodes
/// to `fold_init`, then replay the **entire** log `0..=head` as the tail.
fn load_empty_prefix<A: Aggregate>(
    cert: &StreamCert,
    r: &SnapshotRef,
    blob: &[u8],
) -> Result<LoadOutcome<A>, VerifyError> {
    // event_prefix_hash MUST equal the genesis (§4.2).
    if r.event_prefix_hash != cert.genesis() {
        return Err(VerifyError::PrefixHashMismatch {
            path: VerifyPath::EmptyPrefix,
        });
    }
    // Blob integrity + decode; the certified state MUST be fold_init.
    if blob_hash(blob) != r.state_hash {
        return Err(VerifyError::StateHashMismatch);
    }
    let decoded = A::from_bytes(blob).ok_or(VerifyError::StateDecode)?;
    if decoded.to_bytes() != A::init().to_bytes() {
        return Err(VerifyError::StateDecode);
    }
    // Replay the whole log 0..=head, seeded at genesis (still exercises the
    // head anchor). Start fresh from init so no prefix is double-counted.
    let mut state = A::init();
    let tail_len = replay_tail(cert, 0, cert.genesis(), &mut state)?;
    Ok(LoadOutcome {
        state,
        paths_used: vec![VerifyPath::EmptyPrefix],
        tail_len,
        rebuilt_by_replay: false,
    })
}

// ---------------------------------------------------------------------------
// Test-support builders and the reference aggregate (shared with tests/)
// ---------------------------------------------------------------------------

/// Build a [`StreamCert`] from a flat payload list by chopping it into batches
/// of `batch_size`, materializing each batch's real `crypto_chain` via
/// [`fold_chain::ChainHead`] and recording the true durable head anchor. This
/// is the honest, spec-conformant construction the attack suite mutates.
///
/// `genesis_override` seeds the chain from an arbitrary genesis instead of the
/// stream's `genesis(stream_id)` — used only to model the §3.1 counterfactual
/// (an unbound genesis); pass `None` for the real, bound construction.
#[must_use]
pub fn build_cert(
    stream_id: u64,
    payloads: &[Vec<u8>],
    batch_size: usize,
    genesis_override: Option<Hash>,
) -> StreamCert {
    assert!(batch_size >= 1, "batch_size must be >= 1");
    let mut head = match genesis_override {
        Some(g) => fold_chain::ChainHead::with_genesis_hash(g),
        None => fold_chain::ChainHead::genesis(stream_id),
    };
    let mut batches = Vec::new();
    let mut version = 0u64;
    for chunk in payloads.chunks(batch_size) {
        let entry = head.entry();
        let base_version = version;
        let mut frames = Vec::with_capacity(chunk.len());
        for p in chunk {
            frames.push(FrameRec { version, payload: p.clone() });
            head.absorb(p);
            version += 1;
        }
        batches.push(BatchRec { base_version, crypto_chain: entry, frames });
    }
    let head_anchor = version.checked_sub(1).map(|last| (last, head.entry()));
    StreamCert { stream_id, batches, head_anchor, snapshot_anchors: Vec::new() }
}

/// Take an honest snapshot at `version` over a cert built by [`build_cert`]:
/// fold `0..=version`, capturing the real `event_prefix_hash = h[version]` and
/// `state_hash`. Returns the `(SnapshotRef, blob)`.
#[must_use]
pub fn take_snapshot<A: Aggregate>(
    cert: &StreamCert,
    version: u64,
) -> (SnapshotRef, Vec<u8>) {
    let mut state = A::init();
    let mut h = cert.genesis();
    for v in 0..=version {
        let b = cert.batch_of(v).expect("frame retained for snapshot");
        let (_prev, h_v) =
            chain_at(&b.crypto_chain, b.base_version, b.payload_refs(), v)
                .expect("frame in batch");
        h = h_v;
        let idx = (v - b.base_version) as usize;
        state.apply(&b.frames[idx].payload);
    }
    let blob = state.to_bytes();
    let r = SnapshotRef {
        stream_id:           cert.stream_id,
        stream_version:      version,
        fold_version:        A::FOLD_VERSION,
        covers_empty_prefix: false,
        event_prefix_hash:   h,
        state_hash:          blob_hash(&blob),
    };
    (r, blob)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A toy bank-account aggregate (mirrors the `fold_cert` spike). Payload:
    /// byte 0 = tag (0 deposit, 1 withdraw), bytes 1..9 = u64 LE amount.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct Account {
        pub balance:  i64,
        pub tx_count: u64,
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

    fn stream(id: u64, n: u64, batch_size: usize) -> StreamCert {
        let payloads: Vec<Vec<u8>> =
            (0..n).map(|i| ev((i % 3 == 2) as u8, 10 + i)).collect();
        build_cert(id, &payloads, batch_size, None)
    }

    #[test]
    fn build_cert_versions_are_contiguous_and_base_correct() {
        let s = stream(1, 25, 10);
        assert_eq!(s.batches.len(), 3);
        assert_eq!(s.batches[0].base_version, 0);
        assert_eq!(s.batches[1].base_version, 10);
        assert_eq!(s.batches[2].base_version, 20);
        assert_eq!(s.committed_count(), 25);
        // Batch 1's crypto_chain is genesis; batch 2's is batch 1's exit head.
        assert_eq!(s.batches[0].crypto_chain, s.genesis());
    }

    #[test]
    fn honest_snapshot_load_succeeds_at_batch_boundary() {
        let s = stream(2, 50, 10);
        let (r, blob) = take_snapshot::<Account>(&s, 19);
        let out = load_verified::<Account>(&s, Some((&r, &blob))).unwrap();
        assert!(!out.rebuilt_by_replay);
        assert_eq!(out.tail_len, 30); // frames 20..=49
        assert!(out.paths_used.contains(&VerifyPath::FromFrameV));
        assert!(out.paths_used.contains(&VerifyPath::FromFrameVPlus1));
        // State equals a full replay.
        let full = full_replay_verified::<Account>(&s).unwrap();
        assert_eq!(out.state, full);
    }

    #[test]
    fn honest_snapshot_load_succeeds_mid_batch() {
        // v=25 is mid-batch [20..=29]; tail starts at 26 (first-partial-batch).
        let s = stream(3, 50, 10);
        let (r, blob) = take_snapshot::<Account>(&s, 25);
        let out = load_verified::<Account>(&s, Some((&r, &blob))).unwrap();
        assert!(!out.rebuilt_by_replay);
        assert_eq!(out.tail_len, 24); // frames 26..=49
        let full = full_replay_verified::<Account>(&s).unwrap();
        assert_eq!(out.state, full);
    }

    #[test]
    fn empty_prefix_snapshot_loads() {
        let s = stream(4, 10, 4);
        let blob = Account::init().to_bytes();
        let r = SnapshotRef {
            stream_id:           4,
            stream_version:      0,
            fold_version:        Account::FOLD_VERSION,
            covers_empty_prefix: true,
            event_prefix_hash:   s.genesis(),
            state_hash:          blob_hash(&blob),
        };
        let out = load_verified::<Account>(&s, Some((&r, &blob))).unwrap();
        assert_eq!(out.paths_used, vec![VerifyPath::EmptyPrefix]);
        assert_eq!(out.tail_len, 10); // the whole log is the tail
        assert_eq!(out.state, full_replay_verified::<Account>(&s).unwrap());
    }

    #[test]
    fn empty_prefix_with_wrong_hash_rejected() {
        let s = stream(5, 5, 2);
        let blob = Account::init().to_bytes();
        let r = SnapshotRef {
            stream_id:           5,
            stream_version:      0,
            fold_version:        Account::FOLD_VERSION,
            covers_empty_prefix: true,
            event_prefix_hash:   [0xAB; 32], // not the genesis
            state_hash:          blob_hash(&blob),
        };
        let err = load_verified::<Account>(&s, Some((&r, &blob))).unwrap_err();
        assert_eq!(
            err,
            VerifyError::PrefixHashMismatch { path: VerifyPath::EmptyPrefix }
        );
    }

    #[test]
    fn no_snapshot_rebuilds_by_full_replay() {
        let s = stream(6, 12, 5);
        let out = load_verified::<Account>(&s, None).unwrap();
        assert!(out.rebuilt_by_replay);
        assert_eq!(out.state, full_replay_verified::<Account>(&s).unwrap());
    }
}
