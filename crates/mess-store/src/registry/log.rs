//! The `$registry` stream **as it lives in the engine's v3 log** (`bn-2di`).
//!
//! [`state`](super::state)'s [`RegistryState`] is the one and only fold — this
//! module does not add a second registry representation. What it adds is the
//! thin, spec-pinned glue between that fold and `mess-log`'s frame envelope:
//! the reserved envelope constants (REG1/REG5), the record builders the engine
//! emits when it mints a new id, and [`Fold`] — an ordered accumulator that
//! turns "the stream-0 batches recovery found, in global-position order" into
//! a [`RegistryState`].
//!
//! # Where the registry lives on v3 (the scope decision for this bone)
//!
//! Spec 04 is written against the v4 control-prelude capsule, which is merged
//! but off by default (`bn-9mw`). This bone lands the registry on the **v3**
//! format instead: `$registry` records are ordinary `EventSubframe`s in
//! ordinary single-stream batches, on the reserved `stream_id 0`
//! ([`REGISTRY_STREAM_ID`]). They therefore consume global positions like any
//! other event — the accepted cost of not blocking Fjall retirement on the v4
//! cutover. Everything else about spec 04 holds verbatim: the reserved ids
//! (REG1), the frozen `codec_id 0` wire format (REG7, [`super::codec`]), the
//! envelope constraints (REG5/REG6), and the fold's REG-rule enforcement
//! (REG12/REG14/REG16, [`RegistryState::apply`]).
//!
//! The engine hides stream 0 from the user-facing global read path, so a
//! `$registry` record is never delivered to a subscriber or a `read_all` —
//! only its *position* is consumed. See `engine.rs`'s `read_global`.

use super::codec::RegistryRecord;
use super::error::RegistryError;
use super::state::RegistryState;

/// REG1: the reserved `stream_id` of the `$registry` system stream.
pub const REGISTRY_STREAM_ID: u64 = 0;
/// REG1: the reserved `event_type_id` every `$registry` frame carries.
pub const REGISTRY_EVENT_TYPE_ID: u32 = 0;
/// REG1/REG5: the frozen bootstrap codec — the only codec legal on stream 0,
/// and (REG9) never legal for a domain event type.
pub const REGISTRY_CODEC_ID: u16 = 0;
/// REG5/D-REG-A: `$registry` frames are not schema-versioned (`0` is the "do
/// not route this through the upcaster" sentinel).
pub const REGISTRY_SCHEMA_VERSION: u16 = 0;

// bn-26pp: the sealed registry-delta sidecar (`.reg`) lives in `mess-index`,
// which sits below this crate and cannot name the constant above, so it
// restates it. This is the seam where the two must agree: a `.reg` written for
// the wrong stream would simply never cross-check and every open would silently
// fall back to the `pread` path — a performance cliff with no test failure
// anywhere. Fail the build instead.
const _: () = assert!(
    mess_index::sealed::regdelta::REGISTRY_STREAM_ID == REGISTRY_STREAM_ID
);

/// The `codec_id` the engine declares for the event types **it** mints.
///
/// REG9 forbids `0` here (that value is reserved to `$registry`'s own frames),
/// so the engine declares `1` — spec 01's "MessagePack named-field / domain
/// payload" codec, which is what the `mess-core` `Event` encoding above this
/// layer actually is.
///
/// Note for the reviewer: v3 domain *frames* currently stamp `codec_id 0` in
/// the envelope (`EventInput::plain(tid, 0, 0, ..)` in `engine.rs`), which is
/// a pre-existing deviation from REG5 that predates this bone and that this
/// bone deliberately does not change (it is a frame-format question, and
/// changing it would rewrite every frame's envelope). The consequence is that
/// the optional step-3 check spec 04 §7.1 mentions — "validate an
/// `event_type_id`'s frame-declared `codec_id` against its registry-declared
/// one" — is **not** performed by this engine; nothing depends on it today.
pub const DOMAIN_CODEC_ID: u16 = 1;

/// The `schema_fingerprint` the engine records for a minted event type.
///
/// D-REG-D makes this an opaque, permanent audit anchor for `schema_version 1`
/// of the name. The `Backend` seam the engine sits behind knows an event type
/// only by its `message_type` string — it never sees a schema, a struct, or a
/// codec — so there is nothing to digest here and the engine records the
/// all-zero fingerprint rather than inventing a fake one. A future layer that
/// *does* know the schema (the `mess-derive` `Event` impl) can start supplying
/// a real digest without any format change: the field is already there.
pub const UNKNOWN_SCHEMA_FINGERPRINT: [u8; 32] = [0u8; 32];

/// The `category_id` the engine stamps on registered streams.
///
/// The `Backend` facade does not model categories (`engine.rs`'s
/// `CATEGORY_ID`), so every stream registers into the reserved `$system`
/// category `0` — which §3.4 explicitly permits (`MUST be 0 or an
/// already-registered category_id`). When the facade grows categories, this
/// becomes a real `CategoryRegistered` id and nothing else here changes.
pub const ENGINE_CATEGORY_ID: u64 = 0;

/// The `StreamRegistered` record minting `stream_id` for `name` (§3.4).
#[must_use]
pub fn stream_registered(stream_id: u64, name: &str) -> RegistryRecord {
    RegistryRecord::StreamRegistered {
        stream_id,
        category_id: ENGINE_CATEGORY_ID,
        name: name.to_string(),
    }
}

/// The `EventTypeRegistered` record minting `event_type_id` for `name` (§3.5).
#[must_use]
pub fn event_type_registered(event_type_id: u32, name: &str) -> RegistryRecord {
    RegistryRecord::EventTypeRegistered {
        event_type_id,
        codec_id: DOMAIN_CODEC_ID,
        schema_fingerprint: UNKNOWN_SCHEMA_FINGERPRINT,
        name: name.to_string(),
    }
}

/// An ordered `$registry` fold (§7.1 step 2).
///
/// Recovery finds stream-0 batches out of order (sealed tiers first, then the
/// scanned head segment; concurrent writers even interleave registrations
/// across streams), but [`RegistryState::apply`] is defined against **replay
/// order** — batch commit order, then subframe index (§4.2). `Fold` restores
/// that: [`push_batch`](Self::push_batch) accumulates each batch's payloads
/// keyed by its `first_global_pos`, and [`finish`](Self::finish) sorts by that
/// key before decoding and applying anything.
///
/// Sorting is what makes the fold correct for `NameAliased` (whose meaning
/// genuinely depends on order — REG15). The `*Registered` records the engine
/// itself emits are order-insensitive in the fold (`apply` only ever *inserts*
/// them and takes `hwm = max(hwm, id)`), which is why two concurrent appenders
/// minting ids 1 and 2 may legitimately land their registrations in either
/// order in the log without tripping REG14.
#[derive(Debug, Default)]
pub struct Fold {
    /// `(first_global_pos, payloads)` — one entry per stream-0 batch.
    batches: Vec<(u64, Vec<Vec<u8>>)>,
}

impl Fold {
    #[must_use]
    pub fn new() -> Self { Self::default() }

    /// Whether any `$registry` batch has been seen at all. `false` means the
    /// log carries no registry — a fresh store, or a legacy store that has
    /// not been migrated (`mess migrate registry`).
    #[must_use]
    pub fn is_empty(&self) -> bool { self.batches.is_empty() }

    /// The number of `$registry` **events** accumulated so far — which, because
    /// stream 0's versions are dense from 0, is also the stream's next version.
    #[must_use]
    pub fn record_count(&self) -> u64 {
        self.batches.iter().map(|(_, p)| p.len() as u64).sum()
    }

    /// Accumulate one stream-0 batch: its `first_global_pos` (the replay-order
    /// key) and its frames' payloads, in subframe order.
    pub fn push_batch(
        &mut self,
        first_global_pos: u64,
        payloads: Vec<Vec<u8>>,
    ) {
        self.batches.push((first_global_pos, payloads));
    }

    /// Decode and apply every accumulated record in replay order (§4.2),
    /// returning the materialized state (§7.1 step 2 complete).
    ///
    /// Every REG-rule violation — a corrupt tag, a double registration
    /// (REG14), a name rebound to a second id (REG16), a dangling reference
    /// (REG12) — surfaces here as a typed error and is fatal to the open. That
    /// is the intent: a registry that does not fold is a store whose ids have
    /// no meaning, and guessing is worse than refusing.
    pub fn finish<E>(mut self) -> Result<RegistryState, RegistryError<E>> {
        // Replay order: batch commit order (== `first_global_pos`, which the
        // committer assigns densely and never reuses), then subframe index
        // (the payloads' own order within a batch).
        self.batches.sort_by_key(|(pos, _)| *pos);
        let mut state = RegistryState::new();
        for (_, payloads) in &self.batches {
            for payload in payloads {
                let record = RegistryRecord::decode(payload)?;
                state.apply(record)?;
            }
        }
        Ok(state)
    }
}
