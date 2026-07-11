//! Snapshot API surface: [`SnapshotRef`], the [`Snapshottable`] aggregate
//! extension, and the throwaway [`SnapshotStore`] keyspace.
//!
//! # Why this exists in Phase 1
//!
//! Doc 12's roadmap puts the *real* snapshot store in Phase 4 and the fold
//! **certificate** (the crypto chain that proves a snapshot summarizes the
//! exact committed prefix) in Phase 5. But the roadmap deliberately puts the
//! snapshot **API shape** in Phase 1: an accelerated load that transparently
//! uses `snapshot + tail` when a valid snapshot exists and falls back to full
//! replay otherwise, so that *user code never changes* when the engine
//! underneath is upgraded. This module is that shape, backed by an explicitly
//! **throwaway** in-memory keyspace.
//!
//! # What is real here and what is reserved
//!
//! [`SnapshotRef`] carries every field of the normative struct in
//! `docs/spec/05-fold-certificates.md` §2.1, but only the fields that Phase 1
//! actually *uses* are populated:
//!
//! - [`fold_version`](SnapshotRef::fold_version) — **live.** A snapshot whose
//!   `fold_version` differs from the aggregate's current
//!   [`Snapshottable::FOLD_VERSION`] is invalidated and the load falls back to
//!   full replay (§9). This is the snapshot-invalidation-on-deploy story, and
//!   it is exercised by the acceptance test.
//! - [`stream_version`](SnapshotRef::stream_version) /
//!   [`covers_empty_prefix`](SnapshotRef::covers_empty_prefix) — **live.** They
//!   pin the 0-based last index the snapshot summarizes (§4).
//! - [`event_prefix_hash`](SnapshotRef::event_prefix_hash) /
//!   [`state_hash`](SnapshotRef::state_hash) — **reserved** (`None`). These are
//!   the BLAKE3 fold-chain and blob-integrity hashes; the chain machinery lands
//!   in Phase 5. They are typed and documented now so the struct never changes
//!   shape, but the interim store computes neither: per this bone, *correctness
//!   comes from the law (the snapshot-equivalence property test), not from a
//!   trusted store.*
//!
//! No BLAKE3 dependency is pulled in for Phase 1 precisely because nothing
//! here trusts a stored hash yet.

use crate::backend::Backend;

/// A 256-bit hash, reserved for the Phase 5 fold certificate.
///
/// This is the `Hash256` of `docs/spec/05-fold-certificates.md` §3 (BLAKE3,
/// 32 bytes). It is defined now so [`SnapshotRef`] has its final shape, but
/// Phase 1 never computes or checks one — see the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Hash256(pub [u8; 32]);

/// An opaque pointer to a serialized state blob.
///
/// In the real store (Phase 4, layout owned by `04-registry.md`) this locates
/// the blob in the snapshot store. In the interim keyspace the blob is stored
/// **co-located** with its [`SnapshotRef`] inside a [`StoredSnapshot`], so the
/// pointer is a logical stand-in only; the keyspace looks blobs up by stream
/// key, not by this value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlobPtr(pub u64);

/// The certificate that accompanies a stored snapshot — every field of the
/// normative struct in `docs/spec/05-fold-certificates.md` §2.1.
///
/// See the [module docs](self) for which fields are live in Phase 1 and which
/// are reserved for the Phase 5 fold certificate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRef {
    /// Interned stream id (D3). A cheap first-line check only; the real
    /// protection is the genesis binding of the Phase 5 chain (§3.1).
    ///
    /// The registry that assigns interned ids is Phase 4 work, so the interim
    /// store derives a stable placeholder from the stream **name** via
    /// [`interim_stream_id`]. Documented as a stand-in; not load-bearing.
    pub stream_id:           u64,
    /// 0-based index of the **last** event summarized (§4.1). Ignored when
    /// [`covers_empty_prefix`](Self::covers_empty_prefix) is set.
    pub stream_version:      u64,
    /// Explicit, human-bumped semantic version of the fold (§9). Copied from
    /// [`Snapshottable::FOLD_VERSION`] at save time; a mismatch against the
    /// aggregate's current value invalidates the snapshot.
    pub fold_version:        u32,
    /// `flags` bit 0 (§4.2): the snapshot summarizes the **empty** prefix,
    /// i.e. the aggregate's initial state having applied nothing.
    /// `stream_version` is then `0` and ignored.
    pub covers_empty_prefix: bool,
    /// The fold-chain value `h[stream_version]` (§3) — **reserved** (`None`)
    /// until the Phase 5 chain machinery lands.
    pub event_prefix_hash:   Option<Hash256>,
    /// `BLAKE3(state_blob)` (§2.1) — **reserved** (`None`) until Phase 5. The
    /// interim store is trusted (throwaway); blob integrity is a Phase 5
    /// concern.
    pub state_hash:          Option<Hash256>,
    /// Pointer to the state blob (§2.1). Interim: co-located, see [`BlobPtr`].
    pub snapshot_ptr:        BlobPtr,
}

/// A [`SnapshotRef`] together with its serialized aggregate-state blob, as the
/// interim keyspace stores it. The blob is opaque bytes to the store — only
/// the owning aggregate (via [`Snapshottable`]) knows how to (de)serialize it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSnapshot {
    /// The certificate.
    pub snapshot_ref: SnapshotRef,
    /// The serialized aggregate state (`Snapshottable::encode_state`).
    pub state_blob:   Vec<u8>,
}

/// A failure to (de)serialize aggregate **state** for the interim snapshot
/// store.
///
/// Distinct from [`mess_core::CodecError`], which is about *event* payloads:
/// state serialization is the aggregate author's choice (serde, hand-rolled,
/// anything) and never touches the event codec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateCodecError(pub String);

impl std::fmt::Display for StateCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "aggregate state codec error: {}", self.0)
    }
}

impl std::error::Error for StateCodecError {}

/// An [`Aggregate`](mess_core::Aggregate) that can be snapshotted.
///
/// This is a **`mess-store`-local extension trait**, not a change to
/// `mess-core`'s [`Aggregate`](mess_core::Aggregate). That is deliberate and
/// is the least-invasive design available:
///
/// - `mess-core`'s `Aggregate` stays pristine — folding and command handling
///   carry no snapshot or serialization obligations, so an aggregate that never
///   snapshots pays nothing.
/// - Snapshotting bundles the two things the interim store needs — the
///   [`FOLD_VERSION`](Self::FOLD_VERSION) declared per aggregate type (§9) and
///   a **state codec** — into one opt-in trait, so `save_snapshot` and the
///   accelerated load require exactly `A: Snapshottable` and nothing leaks into
///   the base traits.
/// - The state codec is expressed as explicit `encode`/`decode` methods rather
///   than a `serde` bound, so `mess-store` pulls in **no** new serialization
///   dependency; the aggregate author picks the representation. (A
///   `#[derive(Aggregate)]` can generate a serde-backed impl mechanically.)
pub trait Snapshottable: mess_core::Aggregate {
    /// The explicit, human-bumped semantic version of this aggregate's fold
    /// (§9). Bump it whenever [`apply`](mess_core::Aggregate::apply) semantics
    /// change (including newly handling a previously-ignored event type); doing
    /// so invalidates every older snapshot, which is then rebuilt by full
    /// replay.
    const FOLD_VERSION: u32;

    /// Serialize this state into a blob for the interim snapshot store.
    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError>;

    /// Reconstruct state from a blob produced by
    /// [`encode_state`](Self::encode_state).
    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError>;
}

/// The throwaway snapshot keyspace — an **opt-in extension** of [`Backend`].
///
/// # Why a `Backend` supertrait and not the `Backend` trait itself
///
/// Snapshots are explicitly throwaway and orthogonal to the commit authority
/// (`docs/spec/05-fold-certificates.md`; D1 makes the event log the sole
/// commit authority). Folding the snapshot keyspace into [`Backend`] would
/// force the future RocksDB commit engine — and every test double — to carry
/// snapshot storage it does not need. Making [`SnapshotStore`] a **supertrait
/// extension** keeps the base `Backend` seam minimal and lets a backend opt in
/// to snapshot acceleration. Reusing [`Backend::Error`] (via the supertrait)
/// means snapshot failures flow through the existing
/// [`StoreError`](crate::StoreError) with no change to any public error type
/// or to the [`load`](crate::EventStore::load) /
/// [`command`](crate::EventStore::command) signatures.
///
/// The store persists **opaque** `(SnapshotRef, state_blob)` records keyed by
/// stream — it never interprets the blob. All folding and (de)serialization
/// happen a layer up, in [`EventStore`](crate::EventStore).
pub trait SnapshotStore: Backend {
    /// Persist `snapshot` for `stream_id`, replacing any prior snapshot.
    fn save_snapshot(
        &self,
        stream_id: &str,
        snapshot: StoredSnapshot,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;

    /// Load the current snapshot for `stream_id`, if any.
    fn load_snapshot(
        &self,
        stream_id: &str,
    ) -> impl std::future::Future<
        Output = Result<Option<StoredSnapshot>, Self::Error>,
    > + Send;
}

/// An opt-in policy for **proactively persisting** snapshots on the warm write
/// path ([`EventStore::command_cached`](crate::EventStore::command_cached)).
///
/// The warm path is a hot-aggregate *in-memory* write-through cache; on its own
/// it never writes a durable snapshot ([`command_cached`] only *reads* the
/// snapshot store, on a cold miss). So a normally-running app persists **no**
/// snapshots, and `mess doctor`'s fold-version drift check has nothing to
/// inspect — the check is structurally vacuous against a live app store. This
/// policy is the opt-in that changes that: an app that sets a non-default
/// policy asks `command_cached` to persist the folded state it *already holds*
/// on the success path (no extra replay) once every `every_n_events` events on
/// a stream.
///
/// The default is [`never`](Self::never): nothing is written and behavior is
/// byte-for-byte what it was before this policy existed, so no existing caller
/// changes. An app opts in with
/// [`EventStore::with_snapshot_policy`](crate::EventStore::with_snapshot_policy).
///
/// The persisted snapshot is throwaway (spec 05's interim keyspace);
/// correctness of a later load never rests on it (that is the
/// snapshot-equivalence law), so a failed write is swallowed and the command
/// still succeeds.
///
/// [`command_cached`]: crate::EventStore::command_cached
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SnapshotPolicy {
    every_n_events: Option<u64>,
}

impl SnapshotPolicy {
    /// Never persist a snapshot from the warm path (the default).
    #[must_use]
    pub fn never() -> Self { Self { every_n_events: None } }

    /// Persist a snapshot each time a stream's committed event count crosses a
    /// multiple of `n` on the warm path. `n == 0` is treated as
    /// [`never`](Self::never) (a zero interval would divide by zero and means
    /// "off" anyway).
    #[must_use]
    pub fn every_n_events(n: u64) -> Self {
        Self { every_n_events: (n > 0).then_some(n) }
    }

    /// The configured interval, or `None` when snapshotting is off.
    #[must_use]
    pub fn interval(self) -> Option<u64> { self.every_n_events }

    /// Whether an append that moved a stream from `events_before` total events
    /// to `events_after` total events should trigger a snapshot: true exactly
    /// when the count crossed a multiple of the interval (so a single append of
    /// many events fires at most once, and never fires when off).
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

/// Derive a stable interim interned-id stand-in from a stream **name**.
///
/// FNV-1a (64-bit) — deterministic, dependency-free, and good enough for the
/// [`SnapshotRef::stream_id`] first-line check. The real interned id comes
/// from the registry (D3) in Phase 4; this is a documented placeholder.
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

    #[test]
    fn never_policy_is_the_default_and_never_fires() {
        assert_eq!(SnapshotPolicy::default(), SnapshotPolicy::never());
        assert_eq!(SnapshotPolicy::never().interval(), None);
        // Even a large jump never triggers when off.
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
        // 0 -> 5 crosses the first multiple.
        assert!(p.should_snapshot(0, 5));
        // 5 -> 7 stays within the same bucket.
        assert!(!p.should_snapshot(5, 7));
        // 8 -> 12 crosses the 10 boundary (a multi-event append fires once).
        assert!(p.should_snapshot(8, 12));
        // 0 -> 3 has not reached the first multiple yet.
        assert!(!p.should_snapshot(0, 3));
        // A single append that leaps several buckets still fires (once).
        assert!(p.should_snapshot(0, 23));
    }
}
