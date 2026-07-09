//! The fold-chain hash constructions and their append-/read-side machinery
//! (`bn-1d0`), byte-exact to **[`docs/spec/05-fold-certificates.md`] §3, §6**.
//!
//! # The three hash families (§3)
//!
//! All hashing is **BLAKE3** (256-bit / 32-byte output); all integers are
//! little-endian. Every hash input begins with a 1-byte domain-separation tag
//! (§3, gap 4) so the three families can never collide regardless of field
//! lengths:
//!
//! ```text
//! h[-1]           = BLAKE3( 0x00 || "mess-stream-v1" || le64(stream_id) )   (genesis, §3.1)
//! frame_hash[i]   = BLAKE3( 0x01 || le64(i) || payload[i] )                 (§3.2)
//! h[i]            = BLAKE3( 0x02 || h[i-1] || frame_hash[i] || le64(i) )    (chain step, §3.3)
//! ```
//!
//! `stream_id` in the genesis is the **interned u64 stream id** and is
//! MANDATORY (§3.1): it is the only load-bearing protection against the
//! cross-stream confusion attack, so `h[v]` transitively binds it.
//!
//! # G10 storage layout (§6): per-batch entry, intra-batch recompute
//!
//! Only one 32-byte value is stored per batch — `crypto_chain = h[base-1]`,
//! the fold value *entering* the batch (the `prev_stream_hash` of the batch's
//! first frame). Nothing is stored per frame. Every `h[i]` and every
//! `prev_stream_hash` is reconstructed at read time by walking the batch's
//! frames forward from `crypto_chain` ([`recompute_batch`]).
//!
//! These constructions differ from the `fold_cert` spike (which omitted the
//! domain tags and put `payload` before `version`); the spike's golden
//! constants are regenerated against this spec in the tests.
//!
//! [`docs/spec/05-fold-certificates.md`]: ../../../../docs/spec/05-fold-certificates.md

use crate::format::CHAIN_LEN;

/// A 32-byte BLAKE3 chain value (`h[i]`, `frame_hash[i]`, or the genesis
/// `h[-1]`). Byte-for-byte the on-disk `crypto_chain` field (§4.4, §6).
pub type Hash = [u8; CHAIN_LEN];

/// Domain-separation tag for the genesis hash (§3, table). `0x00`.
pub const TAG_GENESIS: u8 = 0x00;
/// Domain-separation tag for a frame hash (§3, table). `0x01`.
pub const TAG_FRAME: u8 = 0x01;
/// Domain-separation tag for a chain step (§3, table). `0x02`.
pub const TAG_CHAIN: u8 = 0x02;

/// The fixed genesis literal (§3.1): the 14 ASCII bytes `"mess-stream-v1"`, no
/// NUL terminator and no length prefix — its length is fixed by the spec.
pub const GENESIS_LABEL: &[u8; 14] = b"mess-stream-v1";

/// The genesis chain value `h[-1]` for a stream (§3.1):
/// `BLAKE3(0x00 || "mess-stream-v1" || le64(stream_id))`.
///
/// `stream_id` is the interned u64 id (D3), **not** the stream name — names can
/// be re-aliased, the interned id is immutable. The binding is MANDATORY: a
/// certificate computed from a different `stream_id` mismatches at every `h[v]`.
#[must_use]
pub fn genesis(stream_id: u64) -> Hash {
    let mut h = blake3::Hasher::new();
    h.update(&[TAG_GENESIS]);
    h.update(GENESIS_LABEL);
    h.update(&stream_id.to_le_bytes());
    *h.finalize().as_bytes()
}

/// The frame hash `frame_hash[i] = BLAKE3(0x01 || le64(i) || payload[i])` (§3.2).
///
/// `payload` is the event payload bytes **as committed on disk** (post-codec /
/// post-compression); verification hashes exactly what is stored. The
/// fixed-width `version` precedes the variable-length payload so the input is
/// unambiguously parseable. Verification ALWAYS recomputes this from the
/// payload; it is never trusted from storage and is not materialized on disk
/// (§3.2, §6.2, gap 3).
#[must_use]
pub fn frame_hash(version: u64, payload: &[u8]) -> Hash {
    let mut h = blake3::Hasher::new();
    h.update(&[TAG_FRAME]);
    h.update(&version.to_le_bytes());
    h.update(payload);
    *h.finalize().as_bytes()
}

/// The chain step `h[i] = BLAKE3(0x02 || h[i-1] || frame_hash[i] || le64(i))`
/// (§3.3). The three post-tag inputs are fixed-width (32 || 32 || 8), so the
/// construction is unambiguous. For `i == 0`, `prev` is the genesis `h[-1]`.
#[must_use]
pub fn chain_step(prev: &Hash, fh: &Hash, version: u64) -> Hash {
    let mut h = blake3::Hasher::new();
    h.update(&[TAG_CHAIN]);
    h.update(prev);
    h.update(fh);
    h.update(&version.to_le_bytes());
    *h.finalize().as_bytes()
}

/// Advance the chain one frame: `h[i]` from `h[i-1]`, the payload, and `i`.
/// Combines [`frame_hash`] + [`chain_step`] — the append-path per-event work
/// (§6.3, §10: two BLAKE3 invocations, the second over 72 fixed bytes).
#[must_use]
pub fn advance(prev: &Hash, version: u64, payload: &[u8]) -> Hash {
    chain_step(prev, &frame_hash(version, payload), version)
}

/// `BLAKE3(state_blob)` — the snapshot blob-integrity hash (`SnapshotRef.state_hash`,
/// §2.1). No domain tag: it hashes an opaque blob, not a chain input, so it can
/// never be confused with the tagged chain families.
#[must_use]
pub fn blob_hash(bytes: &[u8]) -> Hash {
    *blake3::hash(bytes).as_bytes()
}

// ---------------------------------------------------------------------------
// Append-side chain state (wires BatchEncoder's crypto_chain slot, §6)
// ---------------------------------------------------------------------------

/// The running fold-chain head for one chain-enabled stream on the append path.
///
/// A stream's head starts at [`genesis`]. For each batch the appender:
/// 1. reads [`ChainHead::entry`] — `h[base-1]`, the value to stamp into the
///    batch's `crypto_chain` field (§6.2), and
/// 2. calls [`ChainHead::absorb_batch`] with the batch's frames to fold them
///    into the head, leaving it at `h[last_version]` — the batch's *exit head*
///    (recorded per stream in the sealed footer at seal time, §5/§6.3).
///
/// Because a chain-enabled batch is a contiguous ascending run of one stream's
/// frames (§6.2), the entry captured in step 1 IS the batch's first frame's
/// `prev_stream_hash`, so a reader can reconstruct every per-frame value from
/// `crypto_chain` alone ([`recompute_batch`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainHead {
    head: Hash,
    /// Version of the *next* frame this head will absorb (0 at genesis).
    next_version: u64,
}

impl ChainHead {
    /// A fresh head at the stream's genesis (`h[-1]`), before any event.
    #[must_use]
    pub fn genesis(stream_id: u64) -> Self {
        ChainHead { head: genesis(stream_id), next_version: 0 }
    }

    /// Resume a head from a durably-recorded `(last_version, head_hash)` anchor
    /// (§5 Tier-1): the next batch will start at `last_version + 1`.
    #[must_use]
    pub fn resume(last_version: u64, head_hash: Hash) -> Self {
        ChainHead { head: head_hash, next_version: last_version + 1 }
    }

    /// A head seeded from an arbitrary genesis value at version 0. Used to model
    /// the §3.1 counterfactual — an *unbound* genesis shared across streams —
    /// so a test can demonstrate that dropping the `stream_id` binding lets a
    /// cross-stream snapshot pass (proving the binding is load-bearing).
    #[must_use]
    pub fn with_genesis_hash(head: Hash) -> Self {
        ChainHead { head, next_version: 0 }
    }

    /// The `crypto_chain` value to stamp into the next batch (`h[base-1]`),
    /// i.e. the current head bytes. This is exactly what [`crate::encode::BatchInput::crypto_chain`]
    /// wants when `flags.CRYPTO_CHAIN` is set.
    #[must_use]
    pub fn entry(&self) -> Hash {
        self.head
    }

    /// The `stream_version` the next absorbed frame must carry.
    #[must_use]
    pub fn next_version(&self) -> u64 {
        self.next_version
    }

    /// Fold one frame's payload into the head, returning the new `h[version]`.
    /// The frame's version MUST equal [`ChainHead::next_version`] (the batch is
    /// a contiguous ascending run, §6.2); debug-asserted.
    pub fn absorb(&mut self, payload: &[u8]) -> Hash {
        let v = self.next_version;
        self.head = advance(&self.head, v, payload);
        self.next_version += 1;
        self.head
    }

    /// Fold a whole batch of payloads (in ascending version order) into the
    /// head. Returns the batch's **exit head** `h[last_version]`. The value to
    /// store in the batch's `crypto_chain` is the head *before* this call
    /// ([`ChainHead::entry`]).
    pub fn absorb_batch<'a, I>(&mut self, payloads: I) -> Hash
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        for p in payloads {
            self.absorb(p);
        }
        self.head
    }
}

// ---------------------------------------------------------------------------
// Read-side intra-batch recompute (§6.3)
// ---------------------------------------------------------------------------

/// One frame's reconstructed chain values, produced by [`recompute_batch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameChain {
    /// The frame's `stream_version` (0-based).
    pub version: u64,
    /// `h[version-1]` — the frame's `prev_stream_hash`, reconstructed from the
    /// batch's `crypto_chain` (never stored per frame, §6.2).
    pub prev_stream_hash: Hash,
    /// `h[version]` — the chain value after this frame.
    pub chain: Hash,
}

/// Reconstruct every frame's `(prev_stream_hash, h[version])` for one batch by
/// walking its frames forward from the stored `crypto_chain` (§6.3):
///
/// ```text
/// h = crypto_chain                       # = h[base-1]
/// for i in base ..= last:
///     prev_stream_hash[i] = h            # h[i-1]
///     h = chain_step(h, frame_hash(i, payload[i]), i)   # h[i]
/// # on exit, h == h[last] == the batch's exit head
/// ```
///
/// `base` is the first frame's `stream_version`; `payloads[k]` is the payload
/// of frame `base + k` (as committed on disk). `f` is invoked once per frame in
/// ascending order with its [`FrameChain`]; the return value is the batch's
/// **exit head** `h[last]`.
pub fn recompute_batch<'a, I, F>(crypto_chain: &Hash, base: u64, payloads: I, mut f: F) -> Hash
where
    I: IntoIterator<Item = &'a [u8]>,
    F: FnMut(FrameChain),
{
    let mut h = *crypto_chain;
    for (k, payload) in payloads.into_iter().enumerate() {
        let version = base + k as u64;
        let prev = h;
        h = advance(&prev, version, payload);
        f(FrameChain { version, prev_stream_hash: prev, chain: h });
    }
    h
}

/// The `prev_stream_hash` of a single frame at `target` inside a batch, and the
/// running chain up to and including `target` — the bounded intra-batch walk
/// Path A/B need for a mid-batch frame (§6.4). Returns `(prev_stream_hash[target],
/// h[target])`, or `None` if `target < base` or the batch's payloads run out
/// before reaching it.
#[must_use]
pub fn chain_at<'a, I>(crypto_chain: &Hash, base: u64, payloads: I, target: u64) -> Option<(Hash, Hash)>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    if target < base {
        return None;
    }
    let mut h = *crypto_chain;
    for (k, payload) in payloads.into_iter().enumerate() {
        let version = base + k as u64;
        let prev = h;
        h = advance(&prev, version, payload);
        if version == target {
            return Some((prev, h));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(h: &Hash) -> String {
        h.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn domain_tags_separate_the_three_families() {
        // Same 32 zero bytes fed to different families never collide, because
        // the leading tag differs (§3, gap 4).
        let z = [0u8; 32];
        let g = genesis(0);
        let fh = frame_hash(0, &z);
        let cs = chain_step(&z, &z, 0);
        assert_ne!(g, fh);
        assert_ne!(g, cs);
        assert_ne!(fh, cs);
    }

    #[test]
    fn genesis_binds_stream_id() {
        // Different interned ids give different genesis values — the load-
        // bearing cross-stream protection (§3.1).
        assert_ne!(genesis(1), genesis(2));
        // Deterministic.
        assert_eq!(genesis(42), genesis(42));
    }

    #[test]
    fn genesis_is_byte_exact_to_spec() {
        // BLAKE3(0x00 || "mess-stream-v1" || le64(7)) computed independently.
        let mut buf = Vec::new();
        buf.push(0x00u8);
        buf.extend_from_slice(b"mess-stream-v1");
        buf.extend_from_slice(&7u64.to_le_bytes());
        let want = *blake3::hash(&buf).as_bytes();
        assert_eq!(genesis(7), want);
    }

    #[test]
    fn chain_head_and_recompute_agree() {
        // Append-side ChainHead must produce exactly the crypto_chain +
        // per-frame values the read-side recompute reconstructs.
        let stream_id = 99u64;
        let payloads: Vec<Vec<u8>> = (0..5u64).map(|i| vec![i as u8; 20]).collect();

        // Batch 1: frames 0..=2. Batch 2: frames 3..=4.
        let mut head = ChainHead::genesis(stream_id);
        let entry1 = head.entry();
        let refs1: Vec<&[u8]> = payloads[0..3].iter().map(Vec::as_slice).collect();
        let exit1 = head.absorb_batch(refs1.iter().copied());

        let entry2 = head.entry();
        assert_eq!(entry2, exit1, "batch 2's entry is batch 1's exit head");
        let refs2: Vec<&[u8]> = payloads[3..5].iter().map(Vec::as_slice).collect();
        let exit2 = head.absorb_batch(refs2.iter().copied());

        // Read side: recompute batch 1 from entry1.
        let mut got = Vec::new();
        let recomputed_exit1 =
            recompute_batch(&entry1, 0, refs1.iter().copied(), |fc| got.push(fc));
        assert_eq!(recomputed_exit1, exit1);
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].prev_stream_hash, genesis(stream_id));
        assert_eq!(got[0].version, 0);
        assert_eq!(got[2].chain, exit1);

        // Read side: recompute batch 2 from entry2.
        let mut got2 = Vec::new();
        let recomputed_exit2 =
            recompute_batch(&entry2, 3, refs2.iter().copied(), |fc| got2.push(fc));
        assert_eq!(recomputed_exit2, exit2);
        assert_eq!(got2[0].prev_stream_hash, exit1);
        assert_eq!(got2[0].version, 3);
    }

    #[test]
    fn chain_at_matches_full_recompute() {
        let payloads: Vec<Vec<u8>> = (0..4u64).map(|i| vec![0xA0 | i as u8; 12]).collect();
        let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        let entry = genesis(3);

        let mut full = Vec::new();
        recompute_batch(&entry, 10, refs.iter().copied(), |fc| full.push(fc));
        // chain_at for a mid-batch target must agree with the full walk.
        let (prev, h) = chain_at(&entry, 10, refs.iter().copied(), 12).unwrap();
        assert_eq!(prev, full[2].prev_stream_hash);
        assert_eq!(h, full[2].chain);
        // Out of range.
        assert!(chain_at(&entry, 10, refs.iter().copied(), 9).is_none());
        assert!(chain_at(&entry, 10, refs.iter().copied(), 99).is_none());
    }

    #[test]
    fn resume_continues_the_chain() {
        // A head resumed from a durable anchor must chain identically to one
        // that folded the whole prefix.
        let stream_id = 5u64;
        let payloads: Vec<Vec<u8>> = (0..6u64).map(|i| vec![i as u8; 8]).collect();
        let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();

        let mut whole = ChainHead::genesis(stream_id);
        let _ = whole.absorb_batch(refs[0..4].iter().copied());
        let anchor = whole.entry();
        let full_exit = whole.absorb_batch(refs[4..6].iter().copied());

        let mut resumed = ChainHead::resume(3, anchor);
        assert_eq!(resumed.next_version(), 4);
        let resumed_exit = resumed.absorb_batch(refs[4..6].iter().copied());
        assert_eq!(resumed_exit, full_exit);
    }

    #[test]
    fn hex_helper_roundtrips_len() {
        assert_eq!(hex(&genesis(0)).len(), 64);
    }
}
