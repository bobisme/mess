//! The public snapshot API: stable identity ([`StableSnapshotId`],
//! [`SnapshotCompatibility`]), the coverage lattice ([`SnapshotCoverage`]), the
//! lookup/publication decisions ([`SnapshotLookup`], [`SnapshotSaveOutcome`]),
//! the bounded administrative scan vocabulary, and the two seams
//! ([`Snapshottable`], [`SnapshotStore`]).
//!
//! # The one law
//!
//! **A snapshot is discardable acceleration; the event log is the sole
//! authority.** Absence, corruption, an unknown format, an unreadable pack, an
//! identity this binary does not recognize — every one of them is a *miss*,
//! which the caller answers with a full replay. Nothing in this module may make
//! the event store unavailable, and nothing here authorizes log retention.
//!
//! There is exactly one exception, and it is not about snapshot *data*: two
//! aggregates in one process claiming one complete [`SnapshotCompatibility`]
//! is a program bug, not a storage condition, and it fails loudly
//! ([`register_snapshot_identity`]).
//!
//! # Identity: what a snapshot is *for*
//!
//! ADR 0002 §1 makes stream name plus [`SnapshotCompatibility`] the **complete
//! lookup key**. Compatibility is four author-supplied, stable values:
//!
//! | field | bump it when |
//! |---|---|
//! | [`aggregate_schema_id`](SnapshotCompatibility::aggregate_schema_id) | never — it *names* the aggregate for all time |
//! | [`fold_version`](SnapshotCompatibility::fold_version) | [`apply`](mess_core::Aggregate::apply) semantics change |
//! | [`codec_id`](SnapshotCompatibility::codec_id) | the state representation is swapped for a different one |
//! | [`codec_version`](SnapshotCompatibility::codec_version) | the same codec's byte shape changes incompatibly |
//!
//! None of them may be derived from `type_name`, [`std::any::TypeId`],
//! a process-randomized hash, or any other compiler- or build-dependent value:
//! those change under a rename, a refactor, a compiler upgrade, or a rebuild,
//! and a snapshot that silently changes identity is a snapshot that silently
//! stops working — or, worse, one that silently matches the wrong state. The
//! identity is typed so that mistake is not expressible: a
//! [`StableSnapshotId`] can only be built from bytes the author wrote down.
//!
//! # Coverage: what a snapshot *summarizes*
//!
//! [`SnapshotCoverage`] is a lattice, not a number:
//!
//! ```text
//! Empty < Through(0) < Through(1) < … < Through(u64::MAX)
//! ```
//!
//! Code compares `SnapshotCoverage`, never a raw covered version, so "folds the
//! empty prefix" and "folds event 0" can never collide.
//!
//! # Old snapshots
//!
//! Snapshots written under any earlier identity or storage format are
//! **discardable**: they miss cleanly and are rebuilt by replay. No migration
//! exists, is planned, or is needed — see `docs/snapshots.md`.

use std::any::TypeId;
use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::{Arc, LazyLock, RwLock};

use crate::backend::Backend;
use crate::version::Version;

// ---------------------------------------------------------------------------
// Stable identity
// ---------------------------------------------------------------------------

/// Why a byte string is not a valid [`StableSnapshotId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StableSnapshotIdError {
    /// The id was empty. An id must name something.
    Empty,
    /// The id was longer than [`StableSnapshotId::MAX_LEN`]. Ids are
    /// length-capped so they live inline in a record header and a corrupt
    /// length can never drive an unbounded allocation on the decode path.
    TooLong {
        /// The length that was offered.
        len: usize,
    },
    /// The id contained a byte outside the canonical alphabet.
    InvalidByte {
        /// 0-based position of the offending byte.
        index: usize,
        /// The offending byte.
        byte:  u8,
    },
}

impl fmt::Display for StableSnapshotIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => {
                f.write_str("a stable snapshot id must not be empty")
            }
            Self::TooLong { len } => write!(
                f,
                "stable snapshot id is {len} bytes, the cap is {}",
                StableSnapshotId::MAX_LEN
            ),
            Self::InvalidByte { index, byte } => write!(
                f,
                "byte {byte:#04x} at index {index} is not allowed in a stable \
                 snapshot id (allowed: a-z 0-9 . - _ : /)"
            ),
        }
    }
}

impl std::error::Error for StableSnapshotIdError {}

/// An author-supplied, stable, length-capped identifier with **one** canonical
/// encoding.
///
/// # Its law
///
/// Two ids are equal exactly when their bytes are equal. There is no case
/// folding, no Unicode normalization and no alias table, because each of those
/// turns "are these the same snapshot?" into a judgement call. The alphabet is
/// restricted to lowercase ASCII letters, digits and `. - _ : /` — an id is a
/// name an operator reads in a filename, a log line and a JSON report, and it
/// means the same thing in all three.
///
/// # It is never derived
///
/// An id must never come from `type_name`, [`std::any::TypeId`], a
/// process-randomized hash, or any compiler/build-dependent value (ADR 0002
/// §1): none of those survive a rename, a refactor, a dependency bump or a
/// rebuild. [`new`](Self::new) is a `const fn` that rejects an invalid id at
/// **compile time**, so the intended spelling is a literal in the source:
///
/// ```
/// use mess_store::StableSnapshotId;
///
/// const USER: StableSnapshotId = StableSnapshotId::new("social.user");
/// assert_eq!(USER.as_str(), "social.user");
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StableSnapshotId {
    /// Zero-padded. `0` is outside the alphabet, so `Ord` over the padded
    /// bytes is exactly lexicographic order over the logical strings.
    bytes: [u8; StableSnapshotId::MAX_LEN],
    len:   u8,
}

impl StableSnapshotId {
    /// The inclusive cap on an id's length, in bytes.
    pub const MAX_LEN: usize = 64;

    /// Whether `byte` is in the canonical alphabet: `a-z`, `0-9`, `.`, `-`,
    /// `_`, `:`, `/`.
    #[must_use]
    pub const fn is_canonical_byte(byte: u8) -> bool {
        byte.is_ascii_lowercase()
            || byte.is_ascii_digit()
            || matches!(byte, b'.' | b'-' | b'_' | b':' | b'/')
    }

    /// Build an id from a literal, **failing at compile time** when used in a
    /// `const` and the id is not canonical.
    ///
    /// # Panics
    ///
    /// Panics if `s` is empty, longer than [`MAX_LEN`](Self::MAX_LEN), or
    /// contains a byte outside the alphabet. In a `const` context that panic is
    /// a compile error, which is the point: an invalid identity never reaches a
    /// running program.
    #[must_use]
    pub const fn new(s: &str) -> Self {
        let src = s.as_bytes();
        assert!(!src.is_empty(), "a stable snapshot id must not be empty");
        assert!(
            src.len() <= Self::MAX_LEN,
            "a stable snapshot id must be at most 64 bytes"
        );
        let mut bytes = [0u8; Self::MAX_LEN];
        let mut i = 0;
        while i < src.len() {
            assert!(
                Self::is_canonical_byte(src[i]),
                "a stable snapshot id may only contain a-z 0-9 . - _ : /"
            );
            bytes[i] = src[i];
            i += 1;
        }
        Self { bytes, len: src.len() as u8 }
    }

    /// Build an id from bytes decided at runtime (a config file, a wire
    /// header), reporting *why* an id was rejected.
    ///
    /// # Errors
    ///
    /// [`StableSnapshotIdError`] when `s` is empty, over the cap, or contains a
    /// byte outside the canonical alphabet.
    pub fn parse(s: &str) -> Result<Self, StableSnapshotIdError> {
        let src = s.as_bytes();
        if src.is_empty() {
            return Err(StableSnapshotIdError::Empty);
        }
        if src.len() > Self::MAX_LEN {
            return Err(StableSnapshotIdError::TooLong { len: src.len() });
        }
        let mut bytes = [0u8; Self::MAX_LEN];
        for (index, byte) in src.iter().copied().enumerate() {
            if !Self::is_canonical_byte(byte) {
                return Err(StableSnapshotIdError::InvalidByte { index, byte });
            }
            bytes[index] = byte;
        }
        Ok(Self { bytes, len: src.len() as u8 })
    }

    /// The id's canonical bytes: never empty, never longer than
    /// [`MAX_LEN`](Self::MAX_LEN).
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] { &self.bytes[..self.len as usize] }

    /// The id as a string. Always valid UTF-8 — the alphabet is ASCII.
    #[must_use]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(self.as_bytes()).unwrap_or("")
    }
}

impl fmt::Display for StableSnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for StableSnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StableSnapshotId({:?})", self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Compatibility
// ---------------------------------------------------------------------------

/// The complete compatibility identity of a snapshot record.
///
/// # Its law
///
/// Two records are interchangeable **iff** their `SnapshotCompatibility` values
/// are equal. Stream name plus this value is the complete lookup key (ADR 0002
/// §1): a store never falls back from one compatibility to another, never
/// "upgrades" a record in place, and never lets one identity hide or delete
/// another's head. A record whose compatibility differs from the one the caller
/// asked for is a [`SnapshotMiss::Incompatible`] — and a miss is a replay.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct SnapshotCompatibility {
    /// Names the aggregate *and* its state schema, for all time. Changing it
    /// means "this is a different aggregate", not "a new version of one".
    pub aggregate_schema_id: StableSnapshotId,
    /// The explicit, human-bumped semantic version of the fold. Bump whenever
    /// [`apply`](mess_core::Aggregate::apply) semantics change, including
    /// newly handling a previously-ignored event type.
    pub fold_version:        u32,
    /// Names the state codec. Two aggregates may legitimately share one (a
    /// shared serde format), so this is not an aggregate identity.
    pub codec_id:            StableSnapshotId,
    /// The codec's byte-shape version. Bump when
    /// [`decode_state`](Snapshottable::decode_state) can no longer read what
    /// the previous version's [`encode_state`](Snapshottable::encode_state)
    /// wrote.
    pub codec_version:       u32,
}

impl fmt::Display for SnapshotCompatibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}@{}/{}@{}",
            self.aggregate_schema_id,
            self.fold_version,
            self.codec_id,
            self.codec_version
        )
    }
}

// ---------------------------------------------------------------------------
// Coverage
// ---------------------------------------------------------------------------

/// What prefix of a stream a snapshot summarizes.
///
/// # Its law
///
/// The total order is exactly
/// `Empty < Through(0) < Through(1) < … < Through(u64::MAX)`, which is what the
/// derived [`Ord`] gives. Compare `SnapshotCoverage`; never compare a raw
/// covered version, or "summarizes nothing" and "summarizes event 0" collide.
/// Publication is monotone in this order within one `(stream, compatibility)`
/// key: a higher coverage supersedes, a lower one is refused.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum SnapshotCoverage {
    /// Summarizes nothing: the aggregate's initial state, having applied no
    /// events. Strictly weaker than every `Through`.
    Empty,
    /// Summarizes events `0..=n` of the stream.
    Through(u64),
}

impl SnapshotCoverage {
    /// The coverage of a state folded up to `version`.
    #[must_use]
    pub fn of_version(version: Version) -> Self {
        match version.position() {
            Some(n) => SnapshotCoverage::Through(n),
            None => SnapshotCoverage::Empty,
        }
    }

    /// The version a tail replay resumes *after* when folding on top of a state
    /// with this coverage.
    #[must_use]
    pub fn resume_from(self) -> Version {
        match self {
            SnapshotCoverage::Empty => Version::NoStream,
            SnapshotCoverage::Through(n) => Version::At(n),
        }
    }

    /// The last summarized 0-based event index, or `None` for
    /// [`Empty`](Self::Empty).
    #[must_use]
    pub fn covered_version(self) -> Option<u64> {
        match self {
            SnapshotCoverage::Empty => None,
            SnapshotCoverage::Through(n) => Some(n),
        }
    }

    /// Whether this coverage claims more of the stream than `head` holds — a
    /// snapshot ahead of its stream, which is always a miss.
    #[must_use]
    pub fn is_beyond(self, head: Version) -> bool {
        match (self, head.position()) {
            (SnapshotCoverage::Empty, _) => false,
            (SnapshotCoverage::Through(n), Some(h)) => n > h,
            (SnapshotCoverage::Through(_), None) => true,
        }
    }
}

impl fmt::Display for SnapshotCoverage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SnapshotCoverage::Empty => f.write_str("empty"),
            SnapshotCoverage::Through(n) => write!(f, "through:{n}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Trust
// ---------------------------------------------------------------------------

/// A 256-bit hash (BLAKE3) — the shape `docs/spec/05-fold-certificates.md` §3
/// gives every semantic snapshot hash.
///
/// Defined so the certified trust mode has its final shape when the
/// snapshot-side fold-chain wiring lands; nothing in this crate computes one
/// today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Hash256(pub [u8; 32]);

/// How much a stored record's *semantics* are vouched for.
///
/// # Its law
///
/// The record hash protects framing and physical bytes; it is **never**
/// evidence that the state is the result of the fold. Only a certified record
/// carries that claim, and even then only against an honest chain.
///
/// This enum is `#[non_exhaustive]` on purpose. ADR 0002 §1 defines a second
/// mode, `CertifiedSnapshotRef`, which requires *both* semantic hashes and the
/// snapshot-side fold-chain wiring that does not exist yet (see
/// `docs/snapshots.md`). Adding that variant must not be a source break, so
/// callers already have to handle the unknown case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SnapshotTrust {
    /// Both semantic hashes are absent. Compatibility, coverage, bounds, codec
    /// validation and the record hash protect routing and physical integrity;
    /// an *honest producer* is assumed. A buggy or compromised writer can still
    /// persist plausible-but-wrong state, which is why the snapshot-equivalence
    /// law — not this record — is what makes an accelerated load correct.
    UnverifiedCache,
}

// ---------------------------------------------------------------------------
// The record and its blob
// ---------------------------------------------------------------------------

/// The routing header stored alongside a snapshot's state blob.
///
/// Everything here is *routing*: it decides whether a record may be used at
/// all. It carries no proof that the state is right — see [`SnapshotTrust`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRef {
    /// The complete compatibility identity; half of the lookup key.
    pub compatibility: SnapshotCompatibility,
    /// What prefix of the stream this record summarizes.
    pub coverage:      SnapshotCoverage,
    /// How much the record's semantics are vouched for.
    pub trust:         SnapshotTrust,
    /// A stable cross-check derived from the stream **name**
    /// ([`interim_stream_id`]). Acceleration and diagnostics only: the stream
    /// name is the canonical identity at the [`Backend`] seam, and this value
    /// is never a lookup key.
    pub stream_id:     u64,
}

/// A [`SnapshotRef`] together with its serialized aggregate state.
///
/// The store never interprets `state_blob` — only the owning aggregate, via
/// [`Snapshottable`], knows how to read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSnapshot {
    /// The routing header.
    pub snapshot_ref: SnapshotRef,
    /// The serialized aggregate state ([`Snapshottable::encode_state`]).
    pub state_blob:   Vec<u8>,
}

/// A failure to (de)serialize aggregate **state**.
///
/// Distinct from [`mess_core::CodecError`], which is about *event* payloads:
/// state serialization is the aggregate author's choice and never touches the
/// event codec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateCodecError(pub String);

impl fmt::Display for StateCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "aggregate state codec error: {}", self.0)
    }
}

impl std::error::Error for StateCodecError {}

// ---------------------------------------------------------------------------
// Lookup decision
// ---------------------------------------------------------------------------

/// Why a lookup did not produce a usable record.
///
/// # Its law
///
/// Every variant means **replay**, never failure. The variants exist so an
/// operator can tell a cold cache from a stale deploy from a damaged sidecar;
/// they never change what the caller must do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SnapshotMiss {
    /// No record is published for this stream under any identity: a cold
    /// cache, a wiped sidecar, or a store that never saved one.
    Absent,
    /// This stream has a published head, but under a different identity — a
    /// `fold_version` bump on deploy, a codec swap, or a renamed aggregate.
    /// `stored` is the highest-coverage foreign identity found, for reporting.
    /// The foreign record is *not* used, *not* migrated and *not* deleted.
    Incompatible {
        /// The foreign identity that was found instead.
        stored: SnapshotCompatibility,
    },
    /// A head for the exact identity is published, but its bytes did not
    /// validate: a missing pack, a corrupt frame, a leaf that does not bind the
    /// bytes it names, or a record format this binary does not understand.
    Unreadable,
}

impl fmt::Display for SnapshotMiss {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Absent => f.write_str("absent"),
            Self::Incompatible { stored } => {
                write!(f, "incompatible (stored {stored})")
            }
            Self::Unreadable => f.write_str("unreadable"),
        }
    }
}

/// The outcome of consulting a snapshot store for one
/// `(stream, compatibility)` key.
///
/// # Its law
///
/// A `Hit` is a record whose identity is *exactly* the one that was asked for.
/// There is no near miss and no fallback: everything else is a
/// [`Miss`](Self::Miss) carrying the reason, and every reason means replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotLookup<T = StoredSnapshot> {
    /// A record for the exact requested identity resolved and validated.
    Hit(T),
    /// No usable record, and why.
    Miss(SnapshotMiss),
}

impl<T> SnapshotLookup<T> {
    /// The record, if this is a hit.
    pub fn hit(self) -> Option<T> {
        match self {
            Self::Hit(v) => Some(v),
            Self::Miss(_) => None,
        }
    }

    /// The reason, if this is a miss.
    #[must_use]
    pub fn miss(&self) -> Option<SnapshotMiss> {
        match self {
            Self::Hit(_) => None,
            Self::Miss(m) => Some(*m),
        }
    }

    /// Whether a usable record was found.
    #[must_use]
    pub fn is_hit(&self) -> bool { matches!(self, Self::Hit(_)) }

    /// Transform the record of a hit, preserving a miss and its reason.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> SnapshotLookup<U> {
        match self {
            Self::Hit(v) => SnapshotLookup::Hit(f(v)),
            Self::Miss(m) => SnapshotLookup::Miss(m),
        }
    }
}

// ---------------------------------------------------------------------------
// Publication decision
// ---------------------------------------------------------------------------

/// What a store did with a save.
///
/// # Its law
///
/// Publication is monotone in [`SnapshotCoverage`] within one
/// `(stream, compatibility)` key, and a save never destroys a *valid* record it
/// cannot prove obsolete. Every variant other than
/// [`Published`](Self::Published) and [`Repaired`](Self::Repaired) means
/// nothing was written — which is always safe, because the caller already holds
/// the state it tried to persist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SnapshotSaveOutcome {
    /// The record was written and is now the head for its key.
    Published,
    /// A byte-identical record already held this exact coverage. Nothing was
    /// written; a retry is a no-op.
    Idempotent,
    /// The head at this coverage did not validate (missing, unreadable or
    /// corrupt bytes) and was superseded by this valid record. Worth
    /// reporting: it means something damaged the sidecar.
    Repaired,
    /// The published head already covers more of the stream. The save was
    /// dropped rather than written as an unreachable orphan.
    CoverageRegressed {
        /// The coverage the head holds.
        current: SnapshotCoverage,
    },
    /// A *valid* record with different content already holds this exact
    /// coverage. It remains current and the save was dropped.
    ///
    /// Two records that claim to fold the same prefix of the same stream under
    /// the same identity to different states cannot both be right. The cause is
    /// a non-deterministic fold, or a state encoding that changed without a
    /// [`codec_version`](SnapshotCompatibility::codec_version) bump. Refusing
    /// the write keeps the older evidence and makes the bug visible instead of
    /// alternating between two answers.
    Conflict {
        /// The contested coverage.
        coverage: SnapshotCoverage,
    },
}

/// What a store found at a `(stream, compatibility)` key just before a save.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CurrentHead {
    /// Nothing is published for this key.
    Vacant,
    /// A head is published with this coverage.
    Published(SnapshotCoverage),
}

/// The ADR 0002 §1 publication rule, in one place, for every store.
///
/// # Its law
///
/// Within one `(stream, compatibility)` key:
///
/// 1. a strictly **higher** coverage supersedes the head;
/// 2. a strictly **lower** coverage is refused
///    ([`CoverageRegressed`](SnapshotSaveOutcome::CoverageRegressed));
/// 3. at **equal** coverage the current record is read and fully validated
///    first —
///    - invalid or unreadable current bytes may be superseded
///      ([`Repaired`](SnapshotSaveOutcome::Repaired)),
///    - identical valid bytes are
///      [`Idempotent`](SnapshotSaveOutcome::Idempotent),
///    - different valid bytes are a [`Conflict`](SnapshotSaveOutcome::Conflict)
///      and the current record stays.
///
/// Validating *before* superseding is what lets a corrupt head be repaired
/// without weakening the split-brain detection in rule 3.
///
/// `validate_current` is called at most once, only in the equal-coverage case.
/// It returns the current record's canonical identity bytes, or `None` if that
/// record did not validate. `identity` is the same canonical form for the
/// incoming record. Each store picks that form — an encoded record body, a
/// state blob — it only has to be the same form on both sides.
pub fn publication_decision(
    current: CurrentHead,
    incoming: SnapshotCoverage,
    identity: &[u8],
    validate_current: impl FnOnce() -> Option<Vec<u8>>,
) -> SnapshotSaveOutcome {
    let CurrentHead::Published(current) = current else {
        return SnapshotSaveOutcome::Published;
    };
    if incoming > current {
        return SnapshotSaveOutcome::Published;
    }
    if incoming < current {
        return SnapshotSaveOutcome::CoverageRegressed { current };
    }
    match validate_current() {
        None => SnapshotSaveOutcome::Repaired,
        Some(bytes) if bytes == identity => SnapshotSaveOutcome::Idempotent,
        Some(_) => SnapshotSaveOutcome::Conflict { coverage: current },
    }
}

// ---------------------------------------------------------------------------
// Bounded administrative scan
// ---------------------------------------------------------------------------

/// The hard cap on one [scan](SnapshotScanPage) page, so no caller can ask a
/// store to materialize an unbounded number of heads.
pub const MAX_SNAPSHOT_SCAN_LIMIT: u32 = 4_096;

/// Identity of exactly one published discovery root.
///
/// # Its law
///
/// A root generation is monotone and never reused within a store namespace, so
/// this value names one immutable set of heads for all time. A cursor or pin
/// bearing a different `SnapshotRootId` than the store serves is rejected,
/// never silently reinterpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotRootId {
    store:      [u8; 16],
    generation: u64,
}

impl SnapshotRootId {
    /// Name the `generation`-th root of the store namespace `store`.
    #[must_use]
    pub fn new(store: [u8; 16], generation: u64) -> Self {
        Self { store, generation }
    }

    /// The monotone, never-reused generation number.
    #[must_use]
    pub fn generation(self) -> u64 { self.generation }
}

impl fmt::Display for SnapshotRootId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in self.store {
            write!(f, "{b:02x}")?;
        }
        write!(f, "-{:016x}", self.generation)
    }
}

/// A held pin on exactly one validated discovery root.
///
/// # Its law
///
/// While a pin lives, the root it names stays resolvable: it holds a shared,
/// **non-mutating** deletion lease that reclamation honors. Pinning never takes
/// the writer lock, never repairs and never creates anything, so a read-only
/// tool may pin a live store's root. A destructive caller must complete every
/// page of one pinned root with no diagnostic *and* revalidate the same pin
/// before mutating: a changed or lost pin fails closed.
pub struct PinnedSnapshotRoot {
    id:     SnapshotRootId,
    _lease: Arc<dyn std::any::Any + Send + Sync>,
}

impl PinnedSnapshotRoot {
    /// Pin `id`, holding `lease` alive for the pin's lifetime.
    ///
    /// `lease` is whatever token the store's reclamation honors — for the pack
    /// sidecar it is the immutable head list that root published, so a pinned
    /// view cannot be recycled while a scan walks it.
    #[must_use]
    pub fn new(
        id: SnapshotRootId,
        lease: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Self {
        Self { id, _lease: lease }
    }

    /// The root this pin names.
    #[must_use]
    pub fn id(&self) -> SnapshotRootId { self.id }
}

impl fmt::Debug for PinnedSnapshotRoot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedSnapshotRoot").field("id", &self.id).finish()
    }
}

/// The complete key of one published head, in scan order.
///
/// # Its law
///
/// Scan order is lexicographic by stream name, then by compatibility, so a
/// cursor names one position in one total order and pages neither overlap nor
/// skip.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SnapshotScanKey {
    /// The stream name.
    pub stream_id:     String,
    /// The identity under which this stream has a head.
    pub compatibility: SnapshotCompatibility,
}

/// An opaque continuation token for a bounded scan.
///
/// # Its law
///
/// A cursor binds the root identity it was minted against plus the last key it
/// returned. Presented with a pin naming a *different* root it is
/// [`Rejected`](SnapshotScanDiagnostic::Rejected) — never reinterpreted against
/// the new root, because the key it names may mean something else there.
///
/// It is deliberately not serializable: paging happens inside one command, so a
/// wire form would be public API with no consumer. When one is needed it gains
/// a checksum along with its encoding; the type stays opaque either way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotScanCursor {
    root:  SnapshotRootId,
    after: SnapshotScanKey,
}

impl SnapshotScanCursor {
    /// Mint a cursor that resumes strictly after `key` within `root`.
    #[must_use]
    pub fn new(root: SnapshotRootId, key: SnapshotScanKey) -> Self {
        Self { root, after: key }
    }

    /// The root this cursor is bound to.
    #[must_use]
    pub fn root(&self) -> SnapshotRootId { self.root }

    /// The key this cursor resumes strictly after.
    #[must_use]
    pub fn after(&self) -> &SnapshotScanKey { &self.after }
}

/// One validated head returned by a scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotScanEntry {
    /// The head's complete key.
    pub key:       SnapshotScanKey,
    /// What prefix of the stream it summarizes.
    pub coverage:  SnapshotCoverage,
    /// How much its semantics are vouched for.
    pub trust:     SnapshotTrust,
    /// Length of the encoded aggregate state, in bytes.
    pub state_len: u64,
    /// On-disk size of the whole record frame, in bytes.
    pub frame_len: u64,
}

/// Whether a page can be trusted as complete.
///
/// # Its law
///
/// [`Complete`](Self::Complete) is the **only** value that licenses a
/// destructive caller (retention, GC) to act on what it read. Anything else
/// fails closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SnapshotScanDiagnostic {
    /// Every head in this page resolved and validated.
    Complete,
    /// Some heads in this page did not validate and were **skipped**, never
    /// fabricated. The inventory is degraded, not wrong.
    Partial {
        /// How many heads in this page were skipped.
        unresolved: u64,
    },
    /// The pin or cursor does not name a root this store can serve. No entries
    /// are returned and the caller must re-pin.
    Rejected,
}

/// One bounded page of an administrative scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotScanPage {
    /// The validated heads in this page, in scan order. At most the requested
    /// limit, itself capped at [`MAX_SNAPSHOT_SCAN_LIMIT`].
    pub entries:     Vec<SnapshotScanEntry>,
    /// Resume token, or `None` when the pinned root has no further heads.
    pub next_cursor: Option<SnapshotScanCursor>,
    /// Whether this page is complete.
    pub diagnostic:  SnapshotScanDiagnostic,
}

impl SnapshotScanPage {
    /// The page a store returns when a pin or cursor does not name its root.
    #[must_use]
    pub fn rejected() -> Self {
        Self {
            entries:     Vec::new(),
            next_cursor: None,
            diagnostic:  SnapshotScanDiagnostic::Rejected,
        }
    }
}

/// Clamp a caller-supplied page limit to [`MAX_SNAPSHOT_SCAN_LIMIT`].
///
/// The limit type is [`NonZeroU32`], so "zero entries per page" — an infinite
/// loop — is not expressible.
#[must_use]
pub fn clamp_scan_limit(limit: NonZeroU32) -> u32 {
    limit.get().min(MAX_SNAPSHOT_SCAN_LIMIT)
}

// ---------------------------------------------------------------------------
// Identity registry
// ---------------------------------------------------------------------------

/// Two aggregates in one process claim one complete
/// [`SnapshotCompatibility`].
///
/// A program bug, not a storage condition: the two share a lookup key, so they
/// would overwrite and mis-decode each other's snapshots. It is reported the
/// first time the second aggregate touches the snapshot path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotIdentityConflict {
    /// The contested identity.
    pub compatibility:   SnapshotCompatibility,
    /// The Rust type that claimed it first.
    ///
    /// Diagnostics only. `type_name` is unstable and must never be *stored*,
    /// but naming the colliding types in the message is exactly what an author
    /// needs to fix the bug.
    pub claimed_by:      &'static str,
    /// The Rust type that claimed it second.
    pub also_claimed_by: &'static str,
}

impl fmt::Display for SnapshotIdentityConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "snapshot identity {} is claimed by both {} and {}; one identity \
             is one lookup key and must name exactly one aggregate fold",
            self.compatibility, self.claimed_by, self.also_claimed_by
        )
    }
}

impl std::error::Error for SnapshotIdentityConflict {}

static IDENTITY_REGISTRY: LazyLock<
    RwLock<HashMap<SnapshotCompatibility, (TypeId, &'static str)>>,
> = LazyLock::new(|| RwLock::new(HashMap::new()));

/// Claim `A`'s complete
/// [`snapshot_compatibility`](Snapshottable::snapshot_compatibility) for `A`,
/// failing loudly if another aggregate already claimed it.
///
/// # Its law
///
/// One identity is one lookup key, so it must name exactly one aggregate fold.
/// Every snapshot save and every accelerated load calls this first, so a
/// collision surfaces at the first touch of the snapshot path with both type
/// names in the message — instead of as two aggregates quietly overwriting and
/// mis-decoding each other's heads. Repeat claims by the same type are free.
///
/// # Why the *complete* identity and not the schema id alone
///
/// The harm this prevents is a shared head key, and the head key is the
/// complete identity. Two types that share a schema id at *different* fold
/// versions are what a fold bump looks like from inside one process — the old
/// fold and the new one, or a migration test holding both — and they cannot
/// touch each other's heads, because a different identity is a different key.
/// Flagging that would make honest deploy modelling impossible while catching
/// nothing.
///
/// The registry is process-global and keyed by [`TypeId`]: that is a *runtime
/// diagnostic*, never a persisted identity.
///
/// # Errors
///
/// [`SnapshotIdentityConflict`] when a different type already claimed the
/// identity.
// The error carries two 64-byte identifiers plus two type names, which is large
// for a `Result`. It is not boxed here because this is a cold, once-per-type
// call whose error is a program bug; the hot path's
// [`StoreError`](crate::StoreError) boxes it so no ordinary `Result` widens.
#[allow(clippy::result_large_err)]
pub fn register_snapshot_identity<A: Snapshottable>()
-> Result<(), SnapshotIdentityConflict> {
    use std::collections::hash_map::Entry;

    let id = A::snapshot_compatibility();
    let me = TypeId::of::<A>();
    let name = std::any::type_name::<A>();

    {
        let map = IDENTITY_REGISTRY.read().unwrap_or_else(|e| e.into_inner());
        match map.get(&id) {
            Some((owner, _)) if *owner == me => return Ok(()),
            Some((_, owner_name)) => {
                return Err(SnapshotIdentityConflict {
                    compatibility:   id,
                    claimed_by:      owner_name,
                    also_claimed_by: name,
                });
            }
            None => {}
        }
    }

    let mut map = IDENTITY_REGISTRY.write().unwrap_or_else(|e| e.into_inner());
    match map.entry(id) {
        Entry::Occupied(e) if e.get().0 == me => Ok(()),
        Entry::Occupied(e) => Err(SnapshotIdentityConflict {
            compatibility:   id,
            claimed_by:      e.get().1,
            also_claimed_by: name,
        }),
        Entry::Vacant(e) => {
            e.insert((me, name));
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// The aggregate seam
// ---------------------------------------------------------------------------

/// An [`Aggregate`](mess_core::Aggregate) that can be snapshotted.
///
/// # Its law
///
/// An implementor declares a **stable identity** it will honor forever, plus a
/// state codec. Nothing about that identity may be derived from the Rust type —
/// see [`StableSnapshotId`].
///
/// This is a `mess-store`-local extension trait, not a change to `mess-core`'s
/// [`Aggregate`](mess_core::Aggregate): an aggregate that never snapshots pays
/// nothing, and the state codec is explicit `encode`/`decode` methods rather
/// than a `serde` bound, so `mess-store` pulls in no serialization dependency.
///
/// # The smallest honest impl
///
/// ```
/// use mess_store::{StableSnapshotId, Snapshottable, StateCodecError};
/// # use mess_core::{Aggregate, CodecError, Event};
/// # #[derive(Default, Debug, PartialEq)]
/// # struct Counter(i64);
/// # #[derive(Debug)]
/// # struct Bumped;
/// # impl Event for Bumped {
/// #     fn name(&self) -> &'static str { "bumped" }
/// #     fn encode(&self) -> Result<Vec<u8>, CodecError> { Ok(Vec::new()) }
/// #     fn decode(_: &str, _: &[u8]) -> Result<Self, CodecError> { Ok(Bumped) }
/// # }
/// # impl Aggregate for Counter {
/// #     type Event = Bumped;
/// #     fn apply(&mut self, _: &Bumped) { self.0 += 1; }
/// # }
/// impl Snapshottable for Counter {
///     const AGGREGATE_SCHEMA_ID: StableSnapshotId =
///         StableSnapshotId::new("example.counter");
///     const FOLD_VERSION: u32 = 1;
///
///     fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
///         Ok(self.0.to_le_bytes().to_vec())
///     }
///
///     fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
///         let bytes: [u8; 8] = bytes
///             .try_into()
///             .map_err(|_| StateCodecError("want 8 bytes".into()))?;
///         Ok(Counter(i64::from_le_bytes(bytes)))
///     }
/// }
/// ```
///
/// [`CODEC_ID`](Self::CODEC_ID) defaults to the aggregate's own schema id — the
/// common case, where the aggregate hand-rolls the codec for its own state —
/// and [`CODEC_VERSION`](Self::CODEC_VERSION) defaults to `1`. Override them
/// when the state codec has a life of its own (a shared serde format, a
/// third-party encoding) so its byte shape can be versioned without a
/// [`FOLD_VERSION`](Self::FOLD_VERSION) bump, and vice versa.
pub trait Snapshottable: mess_core::Aggregate {
    /// Names this aggregate and its state schema for all time.
    ///
    /// Choose it once, write it down, never change it: changing it means every
    /// existing snapshot of this aggregate misses and is rebuilt (safe, and
    /// slow). Together with the other three values it must name exactly one
    /// aggregate in the process — [`register_snapshot_identity`] enforces
    /// that.
    const AGGREGATE_SCHEMA_ID: StableSnapshotId;

    /// The explicit, human-bumped semantic version of this aggregate's fold
    /// (`docs/spec/05-fold-certificates.md` §9).
    ///
    /// Bump it whenever [`apply`](mess_core::Aggregate::apply) semantics change
    /// — including newly handling a previously-ignored event type. A bump gives
    /// every older snapshot a different identity, so they miss and are rebuilt
    /// by full replay instead of folding a new tail onto an old meaning.
    const FOLD_VERSION: u32;

    /// Names the state codec. Defaults to
    /// [`AGGREGATE_SCHEMA_ID`](Self::AGGREGATE_SCHEMA_ID).
    const CODEC_ID: StableSnapshotId = Self::AGGREGATE_SCHEMA_ID;

    /// The state codec's byte-shape version. Defaults to `1`.
    ///
    /// Bump it whenever [`decode_state`](Self::decode_state) can no longer read
    /// what a previous build's [`encode_state`](Self::encode_state) wrote. A
    /// missed bump here is exactly what produces a
    /// [`Conflict`](SnapshotSaveOutcome::Conflict).
    const CODEC_VERSION: u32 = 1;

    /// This aggregate's complete compatibility identity.
    ///
    /// Never override: it is the four declared values, assembled.
    #[must_use]
    fn snapshot_compatibility() -> SnapshotCompatibility {
        SnapshotCompatibility {
            aggregate_schema_id: Self::AGGREGATE_SCHEMA_ID,
            fold_version:        Self::FOLD_VERSION,
            codec_id:            Self::CODEC_ID,
            codec_version:       Self::CODEC_VERSION,
        }
    }

    /// Serialize this state into a snapshot blob.
    ///
    /// # Errors
    ///
    /// [`StateCodecError`] if the state cannot be represented.
    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError>;

    /// Reconstruct state from a blob produced by
    /// [`encode_state`](Self::encode_state) under the *same*
    /// [`CODEC_ID`](Self::CODEC_ID) and
    /// [`CODEC_VERSION`](Self::CODEC_VERSION).
    ///
    /// # Errors
    ///
    /// [`StateCodecError`] on a malformed blob. The accelerated load treats
    /// that as a miss and replays.
    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError>;
}

// ---------------------------------------------------------------------------
// The store seam
// ---------------------------------------------------------------------------

/// The discardable snapshot keyspace — an **opt-in extension** of [`Backend`].
///
/// # Its law
///
/// Reads never fail for a snapshot reason: a missing, foreign, corrupt or
/// incompatible record is a [`SnapshotMiss`] and the caller replays. `Err` is
/// reserved for the *event-log* backend underneath failing. Writes may fail (a
/// full disk, a lost writer lock) but never make the event store unavailable.
///
/// # Why a `Backend` supertrait and not `Backend` itself
///
/// Snapshots are orthogonal to commit authority — the event log is the sole
/// commit authority. Folding this keyspace into [`Backend`] would force every
/// commit engine and test double to carry snapshot storage it does not need.
/// Reusing [`Backend::Error`] means snapshot plumbing failures flow through the
/// existing [`StoreError`](crate::StoreError) with no new public error type.
///
/// # Why administrative enumeration is not here
///
/// ADR 0002's `pin_snapshot_root`/`scan_snapshots` are properties of a store
/// that *has* a discovery root, and their only callers are offline tools
/// holding that store concretely (see
/// [`Sidecar::pin_root`](crate::pack_snapshot::Sidecar::pin_root)). Putting
/// them on this seam would force every backend to fake a root, and
/// [`EventStore`](crate::EventStore) must never enumerate heads on the ordinary
/// path anyway. The vocabulary they speak — [`PinnedSnapshotRoot`],
/// [`SnapshotScanCursor`], [`SnapshotScanPage`] — is public here so every
/// implementation says the same thing.
pub trait SnapshotStore: Backend {
    /// Publish `snapshot` for `stream_id` under its own identity, applying the
    /// [`publication_decision`] rule.
    ///
    /// # Errors
    ///
    /// [`Backend::Error`] only when the write plumbing failed. A refused save
    /// (a regression, a conflict) is a successful call reporting a
    /// [`SnapshotSaveOutcome`], not an error.
    fn save_snapshot(
        &self,
        stream_id: &str,
        snapshot: StoredSnapshot,
    ) -> impl std::future::Future<
        Output = Result<SnapshotSaveOutcome, Self::Error>,
    > + Send;

    /// Look up the record for exactly `(stream_id, compatibility)`.
    ///
    /// # Errors
    ///
    /// [`Backend::Error`] only when the event-log backend underneath failed.
    /// Every snapshot-side problem is a [`SnapshotMiss`].
    fn load_snapshot(
        &self,
        stream_id: &str,
        compatibility: SnapshotCompatibility,
    ) -> impl std::future::Future<Output = Result<SnapshotLookup, Self::Error>> + Send;
}

// ---------------------------------------------------------------------------
// Warm-path policy
// ---------------------------------------------------------------------------

/// An opt-in policy for **proactively persisting** snapshots on the warm write
/// path ([`EventStore::command_cached`](crate::EventStore::command_cached)).
///
/// The warm path is an in-memory write-through cache; on its own it never
/// writes a durable snapshot. So a normally-running app persists **no**
/// snapshots and `mess doctor`'s drift check has nothing to inspect. This
/// policy is the opt-in that changes that: `command_cached` persists the folded
/// state it *already holds* (no extra replay) once every `every_n_events`
/// events on a stream.
///
/// The default is [`never`](Self::never). A failed write is swallowed: the
/// snapshot is discardable and the command already succeeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SnapshotPolicy {
    every_n_events: Option<u64>,
}

impl SnapshotPolicy {
    /// Never persist a snapshot from the warm path (the default).
    #[must_use]
    pub fn never() -> Self { Self { every_n_events: None } }

    /// Persist a snapshot each time a stream's committed event count crosses a
    /// multiple of `n`. `n == 0` is [`never`](Self::never).
    #[must_use]
    pub fn every_n_events(n: u64) -> Self {
        Self { every_n_events: (n > 0).then_some(n) }
    }

    /// The configured interval, or `None` when snapshotting is off.
    #[must_use]
    pub fn interval(self) -> Option<u64> { self.every_n_events }

    /// Whether an append that moved a stream from `events_before` to
    /// `events_after` total events should trigger a snapshot: true exactly when
    /// the count crossed a multiple of the interval, so one big append fires at
    /// most once.
    #[must_use]
    pub fn should_snapshot(
        self,
        events_before: u64,
        events_after: u64,
    ) -> bool {
        match self.every_n_events {
            Some(n) => events_before / n != events_after / n,
            None => false,
        }
    }
}

/// Derive a stable cross-check id from a stream **name**.
///
/// FNV-1a (64-bit) — deterministic, dependency-free and identical in every
/// build, so it is safe to *report*. It is a cross-check and a display id,
/// never a lookup key: see [`SnapshotRef::stream_id`].
#[must_use]
pub fn interim_stream_id(stream_id: &str) -> u64 {
    const OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01B3;
    let mut hash = OFFSET;
    for byte in stream_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: StableSnapshotId = StableSnapshotId::new("test.a");

    #[test]
    fn ids_are_canonical_bytes_only() {
        assert_eq!(A.as_str(), "test.a");
        assert_eq!(A.as_bytes(), b"test.a");
        assert_eq!(
            StableSnapshotId::parse("a/b:c-d_e.9").unwrap().as_str(),
            "a/b:c-d_e.9"
        );

        assert_eq!(
            StableSnapshotId::parse(""),
            Err(StableSnapshotIdError::Empty)
        );
        assert_eq!(
            StableSnapshotId::parse(&"x".repeat(65)),
            Err(StableSnapshotIdError::TooLong { len: 65 })
        );
        // No case folding: uppercase is simply not in the alphabet, so
        // "Social.user" cannot become a second spelling of one id.
        assert_eq!(
            StableSnapshotId::parse("Social.user"),
            Err(StableSnapshotIdError::InvalidByte { index: 0, byte: b'S' })
        );
        assert!(StableSnapshotId::parse("has space").is_err());
        assert!(StableSnapshotId::parse("nul\0").is_err());
        // The cap is inclusive.
        assert!(StableSnapshotId::parse(&"x".repeat(64)).is_ok());
    }

    #[test]
    fn id_order_is_lexicographic_over_the_logical_string() {
        let ab = StableSnapshotId::new("ab");
        let abc = StableSnapshotId::new("abc");
        let b = StableSnapshotId::new("b");
        assert!(ab < abc, "a prefix sorts before its extension");
        assert!(abc < b);
    }

    #[test]
    fn coverage_is_a_lattice_not_a_number() {
        assert!(SnapshotCoverage::Empty < SnapshotCoverage::Through(0));
        assert!(SnapshotCoverage::Through(0) < SnapshotCoverage::Through(1));
        assert!(
            SnapshotCoverage::Through(u64::MAX - 1)
                < SnapshotCoverage::Through(u64::MAX)
        );
        // Empty and "covers event 0" are distinct values, not two spellings.
        assert_ne!(SnapshotCoverage::Empty, SnapshotCoverage::Through(0));
    }

    #[test]
    fn coverage_maps_to_and_from_versions() {
        assert_eq!(
            SnapshotCoverage::of_version(Version::NoStream),
            SnapshotCoverage::Empty
        );
        assert_eq!(
            SnapshotCoverage::of_version(Version::At(7)),
            SnapshotCoverage::Through(7)
        );
        assert_eq!(SnapshotCoverage::Empty.resume_from(), Version::NoStream);
        assert_eq!(SnapshotCoverage::Through(7).resume_from(), Version::At(7));
        assert_eq!(SnapshotCoverage::Empty.covered_version(), None);
        assert_eq!(SnapshotCoverage::Through(7).covered_version(), Some(7));
    }

    #[test]
    fn a_snapshot_ahead_of_its_stream_is_detectable() {
        assert!(!SnapshotCoverage::Empty.is_beyond(Version::NoStream));
        assert!(SnapshotCoverage::Through(0).is_beyond(Version::NoStream));
        assert!(!SnapshotCoverage::Through(3).is_beyond(Version::At(3)));
        assert!(SnapshotCoverage::Through(4).is_beyond(Version::At(3)));
    }

    fn decide(
        current: CurrentHead,
        incoming: SnapshotCoverage,
        identity: &[u8],
        current_bytes: Option<&[u8]>,
    ) -> SnapshotSaveOutcome {
        publication_decision(current, incoming, identity, || {
            current_bytes.map(<[u8]>::to_vec)
        })
    }

    #[test]
    fn publication_is_monotone_in_coverage() {
        assert_eq!(
            decide(CurrentHead::Vacant, SnapshotCoverage::Empty, b"x", None),
            SnapshotSaveOutcome::Published
        );
        assert_eq!(
            decide(
                CurrentHead::Published(SnapshotCoverage::Empty),
                SnapshotCoverage::Through(0),
                b"x",
                None
            ),
            SnapshotSaveOutcome::Published,
            "Empty -> Through(0) is an increase"
        );
        assert_eq!(
            decide(
                CurrentHead::Published(SnapshotCoverage::Through(0)),
                SnapshotCoverage::Empty,
                b"x",
                None
            ),
            SnapshotSaveOutcome::CoverageRegressed {
                current: SnapshotCoverage::Through(0),
            },
            "Through(0) -> Empty is a regression"
        );
    }

    #[test]
    fn equal_coverage_validates_the_current_record_first() {
        let cur = CurrentHead::Published(SnapshotCoverage::Through(4));
        let cov = SnapshotCoverage::Through(4);

        // 1. invalid current bytes: repair.
        assert_eq!(
            decide(cur, cov, b"new", None),
            SnapshotSaveOutcome::Repaired
        );
        // 2. identical valid bytes: idempotent.
        assert_eq!(
            decide(cur, cov, b"same", Some(b"same")),
            SnapshotSaveOutcome::Idempotent
        );
        // 3. different valid bytes: conflict, current stays.
        assert_eq!(
            decide(cur, cov, b"new", Some(b"old")),
            SnapshotSaveOutcome::Conflict { coverage: cov }
        );
    }

    #[test]
    fn the_current_record_is_only_read_on_an_exact_coverage_collision() {
        let calls = std::cell::Cell::new(0);
        let count = |cur: CurrentHead, inc: SnapshotCoverage| {
            publication_decision(cur, inc, b"x", || {
                calls.set(calls.get() + 1);
                None
            })
        };
        count(CurrentHead::Vacant, SnapshotCoverage::Through(1));
        count(
            CurrentHead::Published(SnapshotCoverage::Through(0)),
            SnapshotCoverage::Through(1),
        );
        count(
            CurrentHead::Published(SnapshotCoverage::Through(2)),
            SnapshotCoverage::Through(1),
        );
        assert_eq!(
            calls.get(),
            0,
            "no read for vacant, higher, or lower coverage"
        );
        count(
            CurrentHead::Published(SnapshotCoverage::Through(1)),
            SnapshotCoverage::Through(1),
        );
        assert_eq!(
            calls.get(),
            1,
            "exactly one read on an equal-coverage collision"
        );
    }

    #[test]
    fn scan_limits_are_nonzero_and_capped() {
        let cap = NonZeroU32::new(MAX_SNAPSHOT_SCAN_LIMIT + 1).unwrap();
        assert_eq!(clamp_scan_limit(cap), MAX_SNAPSHOT_SCAN_LIMIT);
        assert_eq!(clamp_scan_limit(NonZeroU32::new(7).unwrap()), 7);
    }

    #[test]
    fn a_root_id_prints_its_namespace_and_generation() {
        let id = SnapshotRootId::new([0xAB; 16], 9);
        assert_eq!(id.generation(), 9);
        assert_eq!(
            id.to_string(),
            "abababababababababababababababab-0000000000000009"
        );
        assert_ne!(id, SnapshotRootId::new([0xAB; 16], 10));
        assert_ne!(id, SnapshotRootId::new([0xAC; 16], 9));
    }

    #[test]
    fn scan_keys_order_by_stream_then_compatibility() {
        let compat = |fold| SnapshotCompatibility {
            aggregate_schema_id: A,
            fold_version:        fold,
            codec_id:            A,
            codec_version:       1,
        };
        let k = |s: &str, fold| SnapshotScanKey {
            stream_id:     s.to_owned(),
            compatibility: compat(fold),
        };
        assert!(k("a", 9) < k("b", 1), "stream name dominates");
        assert!(k("a", 1) < k("a", 2), "then compatibility");
    }

    #[test]
    fn never_policy_is_the_default_and_never_fires() {
        assert_eq!(SnapshotPolicy::default(), SnapshotPolicy::never());
        assert_eq!(SnapshotPolicy::never().interval(), None);
        assert!(!SnapshotPolicy::never().should_snapshot(0, 1_000));
    }

    #[test]
    fn zero_interval_is_treated_as_off() {
        let p = SnapshotPolicy::every_n_events(0);
        assert_eq!(p, SnapshotPolicy::never());
        assert!(!p.should_snapshot(0, 100));
    }

    #[test]
    fn every_n_fires_once_per_boundary_crossing() {
        let p = SnapshotPolicy::every_n_events(5);
        assert_eq!(p.interval(), Some(5));
        assert!(p.should_snapshot(0, 5));
        assert!(!p.should_snapshot(5, 7));
        assert!(p.should_snapshot(8, 12));
        assert!(!p.should_snapshot(0, 3));
        assert!(p.should_snapshot(0, 23));
    }
}
