//! fold_cert spike: fold certificates for snapshot verification.
//!
//! Prototypes the mechanism from notes/mess-research/08_novel_mechanisms.md §2
//! and 12_convergence.md D2 + D4:
//!
//! ```text
//! frame_hash[i]   = BLAKE3(payload[i] || le64(i))
//! h[-1]           = BLAKE3("mess-stream" || stream_id)          (genesis)
//! h[i]            = BLAKE3(h[i-1] || frame_hash[i] || le64(i))  (fold chain)
//! frame[i].prev_stream_hash = h[i-1]                            (D2 rule)
//! ```
//!
//! `stream_version` is the 0-based index of a frame; a snapshot at version `v`
//! summarizes frames `0..=v` and carries `event_prefix_hash = h[v]`.
//!
//! Scope stays honest (D4): the certificate proves the snapshot summarizes the
//! exact committed prefix; it does NOT prove the fold code was correct.

pub type Hash = [u8; 32];

// ---------------------------------------------------------------------------
// Chain primitives
// ---------------------------------------------------------------------------

/// h[-1]. Domain-separated per stream so a snapshot from stream A can never
/// certify a prefix of stream B (see tests/negative.rs::cross_stream_*).
///
/// SPEC GAP: 08_novel_mechanisms.md says `H("mess", stream_id)`, the task/D2
/// framing says `BLAKE3("mess-stream" || stream_id)`. Neither pins the exact
/// byte layout (separator? length prefix?). We pick plain concatenation of the
/// literal `"mess-stream"` and the stream id bytes.
pub fn genesis_hash(stream_id: &str) -> Hash {
    let mut h = blake3::Hasher::new();
    h.update(b"mess-stream");
    h.update(stream_id.as_bytes());
    *h.finalize().as_bytes()
}

/// frame_hash = BLAKE3(payload || version).
/// SPEC GAP: integer encoding unspecified in the notes; we pick u64 LE.
pub fn frame_hash(payload: &[u8], version: u64) -> Hash {
    let mut h = blake3::Hasher::new();
    h.update(payload);
    h.update(&version.to_le_bytes());
    *h.finalize().as_bytes()
}

/// h[i] = BLAKE3(h[i-1] || frame_hash[i] || i).
pub fn chain_next(prev: &Hash, fh: &Hash, version: u64) -> Hash {
    let mut h = blake3::Hasher::new();
    h.update(prev);
    h.update(fh);
    h.update(&version.to_le_bytes());
    *h.finalize().as_bytes()
}

pub fn blob_hash(bytes: &[u8]) -> Hash {
    *blake3::hash(bytes).as_bytes()
}

// ---------------------------------------------------------------------------
// Stream of frames
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Frame {
    pub stream_version: u64,
    pub payload: Vec<u8>,
    /// Stored BLAKE3(payload || version). Verification RECOMPUTES this from
    /// the payload; the stored copy is cross-checked but never trusted alone.
    pub frame_hash: Hash,
    /// The fold-chain value h[i-1] (D2: "prev_stream_hash IS the fold-chain
    /// value h[i-1], not merely the previous frame's hash").
    /// For frame 0 this is the genesis hash h[-1].
    pub prev_stream_hash: Hash,
}

#[derive(Clone, Debug)]
pub struct Stream {
    pub stream_id: String,
    pub frames: Vec<Frame>,
    /// Trusted head anchor: the current chain value h[n-1] (== genesis when
    /// empty). In a real store this must live somewhere the verifier trusts
    /// independently of the frame bytes — see SPEC GAP 6 in REPORT.md.
    pub head_hash: Hash,
}

impl Stream {
    pub fn new(stream_id: &str) -> Self {
        Self::with_genesis(stream_id, genesis_hash(stream_id))
    }

    /// Escape hatch used by tests to demonstrate what breaks when the
    /// genesis does NOT bind the stream_id (cross-stream confusion attack).
    pub fn with_genesis(stream_id: &str, genesis: Hash) -> Self {
        Stream { stream_id: stream_id.to_string(), frames: Vec::new(), head_hash: genesis }
    }

    /// Next version to be assigned (== number of frames).
    pub fn next_version(&self) -> u64 {
        self.frames.len() as u64
    }

    /// Version of the last frame, if any.
    pub fn head_version(&self) -> Option<u64> {
        self.frames.len().checked_sub(1).map(|v| v as u64)
    }

    /// Append with full chain maintenance: two BLAKE3 invocations per event.
    pub fn append(&mut self, payload: Vec<u8>) -> u64 {
        let v = self.next_version();
        let fh = frame_hash(&payload, v);
        let prev = self.head_hash;
        self.head_hash = chain_next(&prev, &fh, v);
        self.frames.push(Frame { stream_version: v, payload, frame_hash: fh, prev_stream_hash: prev });
        v
    }

    pub fn frame(&self, version: u64) -> Option<&Frame> {
        self.frames.get(version as usize)
    }
}

// ---------------------------------------------------------------------------
// Aggregates / folds (D4)
// ---------------------------------------------------------------------------

/// In the real system this is `#[aggregate(fold_version = N)]` on a derive;
/// here it is a plain trait constant.
pub trait Aggregate: Sized {
    const FOLD_VERSION: u32;
    fn init() -> Self;
    fn apply(&mut self, payload: &[u8]);
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Option<Self>;
}

/// Toy aggregate: a bank account.
///
/// Event payload encoding (first 9 bytes; the rest is padding so benches can
/// use realistic ~250 B payloads):
///   byte 0        tag: 0 = Deposit, 1 = Withdraw
///   bytes 1..9    amount, u64 LE
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Account {
    pub balance: i64,
    pub tx_count: u64,
}

pub const WITHDRAW_FEE: u64 = 1; // only charged under --features drift

impl Aggregate for Account {
    // Bump this when apply() semantics change. The golden test in
    // tests/golden.rs is the guard against forgetting.
    const FOLD_VERSION: u32 = 1;

    fn init() -> Self {
        Account { balance: 0, tx_count: 0 }
    }

    fn apply(&mut self, payload: &[u8]) {
        let tag = payload[0];
        let amount = u64::from_le_bytes(payload[1..9].try_into().unwrap());
        match tag {
            0 => self.balance += amount as i64,
            1 => {
                self.balance -= amount as i64;
                // Deliberate semantic drift for the D4 demo: withdrawals now
                // also charge a fee. Same fold_version -> golden test fails.
                #[cfg(feature = "drift")]
                {
                    self.balance -= WITHDRAW_FEE as i64;
                }
            }
            _ => {} // unknown event types are skipped (see SPEC GAP 8)
        }
        self.tx_count += 1;
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16);
        out.extend_from_slice(&self.balance.to_le_bytes());
        out.extend_from_slice(&self.tx_count.to_le_bytes());
        out
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 16 {
            return None;
        }
        Some(Account {
            balance: i64::from_le_bytes(bytes[0..8].try_into().ok()?),
            tx_count: u64::from_le_bytes(bytes[8..16].try_into().ok()?),
        })
    }
}

pub fn encode_event(tag: u8, amount: u64, pad_to: usize) -> Vec<u8> {
    let mut p = vec![0u8; pad_to.max(9)];
    p[0] = tag;
    p[1..9].copy_from_slice(&amount.to_le_bytes());
    p
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct SnapshotRef {
    pub stream_id: String,
    /// Version of the LAST event summarized (0-based). The snapshot state is
    /// fold(init, frames[0..=stream_version]).
    pub stream_version: u64,
    /// Chain value h[stream_version] — proves prefix identity.
    pub event_prefix_hash: Hash,
    /// BLAKE3 of the serialized state blob — blob integrity.
    pub state_hash: Hash,
    /// EXPLICIT semantic version, human-bumped (D4).
    pub fold_version: u32,
}

/// Minimal snapshot store: latest snapshot per stream, blob stored alongside.
#[derive(Default)]
pub struct SnapshotStore {
    snaps: std::collections::HashMap<String, (SnapshotRef, Vec<u8>)>,
}

impl SnapshotStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold the stream up to and including `version` and store the snapshot.
    pub fn take_snapshot<A: Aggregate>(&mut self, stream: &Stream, version: u64) -> SnapshotRef {
        assert!(version < stream.next_version(), "snapshot version beyond head");
        let mut state = A::init();
        let mut h = genesis_hash(&stream.stream_id);
        for i in 0..=version {
            let f = &stream.frames[i as usize];
            h = chain_next(&h, &frame_hash(&f.payload, i), i);
            state.apply(&f.payload);
        }
        let blob = state.to_bytes();
        let r = SnapshotRef {
            stream_id: stream.stream_id.clone(),
            stream_version: version,
            event_prefix_hash: h,
            state_hash: blob_hash(&blob),
            fold_version: A::FOLD_VERSION,
        };
        self.snaps.insert(stream.stream_id.clone(), (r.clone(), blob));
        r
    }

    pub fn get(&self, stream_id: &str) -> Option<&(SnapshotRef, Vec<u8>)> {
        self.snaps.get(stream_id)
    }

    pub fn get_mut(&mut self, stream_id: &str) -> Option<&mut (SnapshotRef, Vec<u8>)> {
        self.snaps.get_mut(stream_id)
    }

    pub fn invalidate(&mut self, stream_id: &str) {
        self.snaps.remove(stream_id);
    }

    /// Insert an externally-constructed (possibly forged) snapshot. Used by
    /// the negative tests to model attacker-controlled snapshot storage.
    pub fn insert_raw(&mut self, r: SnapshotRef, blob: Vec<u8>) {
        self.snaps.insert(r.stream_id.clone(), (r, blob));
    }
}

// ---------------------------------------------------------------------------
// Verification (load_verified)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyError {
    /// ref.stream_id != the stream we are loading. Cheap first-line check;
    /// the prefix hash would also catch it (because genesis binds stream_id).
    StreamIdMismatch { expected: String, got: String },
    /// Snapshot produced by different fold logic. Caller must invalidate and
    /// rebuild by replay (load_verified does this automatically).
    FoldVersionMismatch { snapshot: u32, current: u32 },
    /// Snapshot blob does not hash to state_hash (corrupt blob).
    StateHashMismatch,
    /// Snapshot blob fails to deserialize.
    StateDecode,
    /// The claimed event_prefix_hash does not match what the frames imply.
    PrefixHashMismatch { path: VerifyPath },
    /// Neither frame v nor frame v+1 is available to certify the prefix.
    NoCertificationPath { version: u64 },
    /// While replaying the tail, frame i's prev_stream_hash != running h[i-1].
    ChainBreakPrev { at_version: u64 },
    /// Frame i's stored frame_hash != BLAKE3(payload || i) (payload tamper).
    ChainBreakFrameHash { at_version: u64 },
    /// Frame at position i claims a different stream_version (reorder).
    VersionOutOfOrder { expected: u64, got: u64 },
    /// Stream ended before the expected head (truncated tail), or the final
    /// recomputed chain value != trusted head_hash.
    HeadMismatch { computed: Hash, expected: Hash },
    /// Snapshot claims a version beyond the stream head.
    SnapshotBeyondHead { snapshot_version: u64, head: Option<u64> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyPath {
    /// Recompute h[v] from frame v alone:
    ///   h[v] = BLAKE3(frame[v].prev_stream_hash || BLAKE3(payload[v] || v) || v)
    /// Reads: frame v (payload + prev_stream_hash). Always available while
    /// frame v is retained; the ONLY path when the tail is empty.
    FromFrameV,
    /// frame[v+1].prev_stream_hash == h[v], by the D2 storage rule.
    /// Reads: frame v+1 (header only, no payload hashing). Only available
    /// when the tail is non-empty; trusts frame v+1's stored header, which is
    /// then transitively validated by the tail replay + head anchor.
    FromFrameVPlus1,
}

#[derive(Debug)]
pub struct LoadOutcome<A> {
    pub state: A,
    /// Which certification paths were checked (both, when both frames exist).
    pub paths_used: Vec<VerifyPath>,
    /// Number of tail events replayed after the snapshot.
    pub tail_len: u64,
    /// True if the snapshot was unusable (fold_version mismatch / missing)
    /// and the state was rebuilt by full verified replay.
    pub rebuilt_by_replay: bool,
}

/// Certify that `r.event_prefix_hash` really is h[r.stream_version] for this
/// stream, using the frames. Returns the paths successfully used.
pub fn certify_prefix(stream: &Stream, r: &SnapshotRef) -> Result<Vec<VerifyPath>, VerifyError> {
    let v = r.stream_version;
    let mut paths = Vec::new();

    // Path A: frame v alone. frame_hash is RECOMPUTED from the payload —
    // trusting the stored frame_hash would make the certificate vacuous
    // against tampering of frame v itself.
    if let Some(f) = stream.frame(v) {
        let fh = frame_hash(&f.payload, v);
        let h_v = chain_next(&f.prev_stream_hash, &fh, v);
        if h_v != r.event_prefix_hash {
            return Err(VerifyError::PrefixHashMismatch { path: VerifyPath::FromFrameV });
        }
        paths.push(VerifyPath::FromFrameV);
    }

    // Path B: frame v+1 carries h[v] directly as its prev_stream_hash.
    if let Some(f_next) = stream.frame(v + 1) {
        if f_next.prev_stream_hash != r.event_prefix_hash {
            return Err(VerifyError::PrefixHashMismatch { path: VerifyPath::FromFrameVPlus1 });
        }
        paths.push(VerifyPath::FromFrameVPlus1);
    }

    if paths.is_empty() {
        // Frame v compacted away AND no tail: nothing certifies the claim.
        return Err(VerifyError::NoCertificationPath { version: v });
    }
    Ok(paths)
}

/// Replay frames `from..=head`, chaining h forward from `h_start`, verifying
/// every link, applying each payload to `state`. Ends by comparing the final
/// chain value against the trusted head anchor.
pub fn replay_verified<A: Aggregate>(
    stream: &Stream,
    from: u64,
    mut h: Hash,
    state: &mut A,
) -> Result<u64, VerifyError> {
    let mut replayed = 0u64;
    let mut expected = from;
    for f in stream.frames.iter().skip(from as usize) {
        if f.stream_version != expected {
            return Err(VerifyError::VersionOutOfOrder { expected, got: f.stream_version });
        }
        if f.prev_stream_hash != h {
            return Err(VerifyError::ChainBreakPrev { at_version: expected });
        }
        let fh = frame_hash(&f.payload, expected);
        if fh != f.frame_hash {
            // Redundant with the prev check one frame later, but localizes
            // the tamper to THIS frame instead of the next.
            return Err(VerifyError::ChainBreakFrameHash { at_version: expected });
        }
        h = chain_next(&h, &fh, expected);
        state.apply(&f.payload);
        expected += 1;
        replayed += 1;
    }
    if h != stream.head_hash {
        // Covers truncation (loop ended early -> h short of head) and any
        // whole-suffix rewrite that kept internal links consistent.
        return Err(VerifyError::HeadMismatch { computed: h, expected: stream.head_hash });
    }
    Ok(replayed)
}

/// Full verified replay from genesis (no snapshot). Also the rebuild path
/// after snapshot invalidation.
pub fn full_replay_verified<A: Aggregate>(stream: &Stream) -> Result<A, VerifyError> {
    let mut state = A::init();
    replay_verified(stream, 0, genesis_hash(&stream.stream_id), &mut state)?;
    Ok(state)
}

/// The D4 `load_verified` entry point.
///
/// Algorithm (what it reads, in order):
///   1. ref.stream_id == stream.stream_id             (no frame reads)
///   2. ref.fold_version == A::FOLD_VERSION           -> mismatch: invalidate
///      snapshot, rebuild by full verified replay (reads ALL frames)
///   3. BLAKE3(blob) == ref.state_hash; decode blob   (no frame reads)
///   4. certify_prefix: reads frame v (payload) and/or frame v+1 (header) —
///      both when both exist, at least one required
///   5. tail replay v+1..=head: reads every tail frame, verifies
///      prev_stream_hash linkage + recomputed frame_hash per frame, folds
///      state forward
///   6. final chain value == stream.head_hash          (trusted anchor)
///
/// The certificate proves the snapshot summarizes the exact committed prefix;
/// it does NOT prove the fold code was correct (D4 scope note).
pub fn load_verified<A: Aggregate>(
    stream: &Stream,
    store: &mut SnapshotStore,
) -> Result<LoadOutcome<A>, VerifyError> {
    let Some((r, blob)) = store.get(&stream.stream_id) else {
        // No snapshot at all: full verified replay.
        let state = full_replay_verified::<A>(stream)?;
        let tail_len = stream.next_version();
        return Ok(LoadOutcome { state, paths_used: vec![], tail_len, rebuilt_by_replay: true });
    };
    let r = r.clone();
    let blob = blob.clone();

    // 1. stream identity
    if r.stream_id != stream.stream_id {
        return Err(VerifyError::StreamIdMismatch {
            expected: stream.stream_id.clone(),
            got: r.stream_id.clone(),
        });
    }

    // 2. fold_version: mismatch is NOT an error surfaced to the caller — the
    // snapshot is invalidated and the state rebuilt by replay (D4).
    if r.fold_version != A::FOLD_VERSION {
        store.invalidate(&stream.stream_id);
        let state = full_replay_verified::<A>(stream)?;
        let tail_len = stream.next_version();
        return Ok(LoadOutcome { state, paths_used: vec![], tail_len, rebuilt_by_replay: true });
    }

    // Sanity: snapshot can't be ahead of the stream head.
    if r.stream_version >= stream.next_version() {
        return Err(VerifyError::SnapshotBeyondHead {
            snapshot_version: r.stream_version,
            head: stream.head_version(),
        });
    }

    // 3. blob integrity + decode
    if blob_hash(&blob) != r.state_hash {
        return Err(VerifyError::StateHashMismatch);
    }
    let mut state = A::from_bytes(&blob).ok_or(VerifyError::StateDecode)?;

    // 4. prefix certificate (both paths when available)
    let paths_used = certify_prefix(stream, &r)?;

    // 5 + 6. tail replay chaining h forward from h[v], then head anchor check
    let tail_len = replay_verified(stream, r.stream_version + 1, r.event_prefix_hash, &mut state)?;

    Ok(LoadOutcome { state, paths_used, tail_len, rebuilt_by_replay: false })
}

// ---------------------------------------------------------------------------
// Golden fixture (D4 fold-drift test data; asserted in tests/golden.rs)
// ---------------------------------------------------------------------------

/// Fixture events for the fold-drift golden test. In the real system the
/// derive macro would generate fixture + expected constants.
pub fn golden_fixture_events() -> Vec<Vec<u8>> {
    vec![
        encode_event(0, 100, 9), // deposit 100
        encode_event(0, 50, 9),  // deposit 50
        encode_event(1, 30, 9),  // withdraw 30
        encode_event(1, 20, 9),  // withdraw 20
        encode_event(0, 7, 9),   // deposit 7
    ]
}

/// Expected folded state under FOLD_VERSION = 1 semantics.
pub const GOLDEN_EXPECTED_BALANCE: i64 = 107;
pub const GOLDEN_EXPECTED_TX_COUNT: u64 = 5;
/// BLAKE3 of Account{107,5}.to_bytes() — pinned so even representation
/// changes trip the test. (hex)
pub const GOLDEN_EXPECTED_STATE_HASH: &str =
    "55ef5dada9f1e1b9ce14d144ce61cc52891b480924fb68c9c02ec604537800f3";
