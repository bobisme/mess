//! The `$registry` stream: bootstrap, name/ID resolution, and dictionary
//! registration — `docs/spec/04-registry.md` (NORMATIVE), decision **D3**.
//!
//! # Layout
//!
//! - [`codec`] — `codec_id 0`, the frozen bootstrap wire format (§2-§3): the
//!   five [`RegistryRecord`](codec::RegistryRecord) shapes, byte-exact,
//!   golden-tested against the spec's §3.9 worked example.
//! - [`state`] — [`RegistryState`], the pure replay/apply state machine
//!   (§4-§7): id<->name maps, the dictionary table, and the four high-water
//!   marks, built by folding decoded records with no backend dependency at all.
//! - This module — [`Registry`], the async wrapper that drives `state` against
//!   a live [`Backend`]: [`Registry::bootstrap`] replays `$registry` from
//!   genesis (§7.2 step 2), and the `register_*`/`alias_*` methods are the
//!   writer-assigned allocation path (§4.1) that appends new records and folds
//!   them into the same state on success.
//!
//! # Bootstrap acyclicity (§7, the first acceptance criterion)
//!
//! [`RegistryRecord::decode`](codec::RegistryRecord::decode) is a pure
//! function of payload bytes and this module's compiled-in constants — it
//! never consults a [`RegistryState`], live or otherwise (§7.1: "step 2
//! never reads its own in-progress table"). [`Registry::bootstrap`] proves
//! this in code: it decodes and folds every `$registry` record starting
//! from [`RegistryState::new`] (the empty state), so a process that has
//! *never* seen a single byte of the log can still decode the very first
//! `$registry` record — there is nothing upstream of `codec_id 0` and the
//! four reserved IDs (REG1) to depend on.
//!
//! # Engine-agnostic
//!
//! Like the rest of this crate, [`Registry<B>`] is generic over any
//! [`Backend`] — it reads and appends to the [`REGISTRY_STREAM`] name
//! (`"$registry"`, the [`Backend`]-level key standing in for the spec's
//! reserved `stream_id 0`) through the same `head`/`read_stream`/
//! `append_batch` seam every other stream uses, so it works against the
//! in-memory [`MockBackend`](crate::MockBackend) today and the real engine
//! later with no code change.

pub mod codec;
pub mod state;

mod error;

pub use codec::{
    REGISTRY_EVENT_TYPE_NAME, REGISTRY_STREAM, RegistryRecord,
    TARGET_KIND_CATEGORY, TARGET_KIND_EVENT_TYPE, TARGET_KIND_STREAM,
};
pub use error::RegistryError;
pub use state::{
    DictMeta, EventTypeMeta, RESERVED_CATEGORY_ID, RESERVED_CATEGORY_NAME,
    RESERVED_DICT_ID, RESERVED_EVENT_TYPE_ID, RESERVED_EVENT_TYPE_NAME,
    RESERVED_STREAM_ID, RESERVED_STREAM_NAME, RegistryState,
};

use crate::backend::{AppendError, Backend, RecordToAppend};
use crate::version::Version;

/// The page size [`Registry::bootstrap`] uses when paging through
/// `$registry` — an internal replay detail, not part of the public API.
const BOOTSTRAP_PAGE_SIZE: usize = 1_000;

/// Replay `$registry` from genesis into a fresh [`RegistryState`] (§7.2 step
/// 2). Returns the folded state and the stream's version after replay (used
/// as the next `expected` version for an append).
///
/// This is the free function the acyclicity proof rests on: it takes only a
/// `&B` and produces a [`RegistryState`] — no prior registry, no side
/// state, nothing but the bytes [`Backend::read_stream`] returns and
/// [`RegistryRecord::decode`]'s compiled-in tables.
pub async fn replay<B: Backend>(
    backend: &B,
) -> Result<(RegistryState, Version), RegistryError<B::Error>> {
    let mut state = RegistryState::new();
    let mut after = Version::NoStream;
    loop {
        let page = backend
            .read_stream(REGISTRY_STREAM, after, BOOTSTRAP_PAGE_SIZE)
            .await
            .map_err(RegistryError::Backend)?;
        let page_len = page.len();
        for rec in &page {
            let record = RegistryRecord::decode(&rec.data)?;
            state.apply(record)?;
            after = Version::At(rec.stream_position);
        }
        if page_len < BOOTSTRAP_PAGE_SIZE {
            break;
        }
    }
    Ok((state, after))
}

/// The `$registry` stream, live over a [`Backend`]: bootstrapped state plus
/// the writer-assigned allocation path (§4.1).
///
/// Per D9 (single writer), nothing here races itself — `register_*`/
/// `alias_*` take `&mut self` and serialize through the in-memory `head`/
/// `state`, matching the single-writer model the allocation algorithm (§4.1)
/// assumes. Concurrent external writers to the same backend stream (a
/// misuse this type does not protect against) would surface as
/// [`AppendError::Conflict`] from the backend, same as any other stream.
pub struct Registry<B: Backend> {
    backend: B,
    state:   RegistryState,
    head:    Version,
}

impl<B: Backend> Registry<B> {
    /// Bootstrap a [`Registry`] by replaying `$registry` from genesis
    /// (§7.2). Works identically whether `backend` is empty (a brand-new
    /// log — the state stays at [`RegistryState::new`]'s defaults, only the
    /// four reserved IDs resolve) or already holds registrations (a
    /// recovered/copied log — I5: every name resolves from bytes alone).
    pub async fn bootstrap(
        backend: B,
    ) -> Result<Self, RegistryError<B::Error>> {
        let (state, head) = replay(&backend).await?;
        Ok(Self { backend, state, head })
    }

    /// The materialized state (id<->name maps, dictionary table, high-water
    /// marks) — read-only access for resolution.
    #[must_use]
    pub fn state(&self) -> &RegistryState { &self.state }

    /// Borrow the underlying backend.
    pub fn backend(&self) -> &B { &self.backend }

    /// Append one already-built [`RegistryRecord`] to `$registry` at the
    /// current head, then fold it into `state` on success. `state.apply`
    /// is the single source of REG-rule enforcement, so a record this
    /// method builds incorrectly is rejected the same way a corrupt one
    /// would be on replay — there is no separate, potentially-drifting
    /// writer-side validation path.
    ///
    /// REG13 requires a writer to enforce REG12 (and every other REG-rule)
    /// *at append time*, i.e. before the record is durably committed:
    /// `$registry` is append-only and never compacted (REG4/I1), so a
    /// REG-rule-violating record that made it to the backend would live
    /// there forever even though the caller sees an `Err` (REG14 — such a
    /// record must never legitimately exist). We therefore validate against
    /// a scratch clone of `state` first and only touch the backend — and
    /// only then advance the real `state`/`head` — once that validation has
    /// succeeded.
    async fn append(
        &mut self,
        record: RegistryRecord,
    ) -> Result<(), RegistryError<B::Error>> {
        // Validate before committing anything: apply to a throwaway clone
        // of the current state so a rejected record never reaches the
        // backend and never touches `self.state`/`self.head`.
        let mut trial_state = self.state.clone();
        trial_state.apply(record.clone())?;

        let data = record.encode();
        let rec = RecordToAppend {
            message_type: REGISTRY_EVENT_TYPE_NAME.to_string(),
            data,
        };
        let appended = self
            .backend
            .append_batch(
                REGISTRY_STREAM,
                self.head,
                std::slice::from_ref(&rec),
            )
            .await
            .map_err(|e| match e {
                // D9 (single writer): nothing else should ever be writing
                // $registry, so a conflict here means that assumption was
                // violated — a deployment bug, not a data problem. Panic
                // loudly rather than silently mismodeling it as a REG
                // violation.
                AppendError::Conflict { expected, actual } => panic!(
                    "concurrent write to $registry detected (D9 violation): \
                     expected version {expected:?}, actual {actual:?} — \
                     $registry must have exactly one writer"
                ),
                AppendError::Backend(e) => RegistryError::Backend(e),
            })?;
        // The backend commit succeeded with the exact bytes validated
        // above, so folding into the real state cannot fail; adopt the
        // already-validated trial state rather than re-running `apply`.
        self.state = trial_state;
        self.head = appended.version;
        Ok(())
    }

    /// Register a new category, allocating the next `category_id` (§4.1).
    pub async fn register_category(
        &mut self,
        name: impl Into<String>,
    ) -> Result<u64, RegistryError<B::Error>> {
        let category_id = self.state.category_high_water_mark() + 1;
        let name = name.into();
        self.append(RegistryRecord::CategoryRegistered { category_id, name })
            .await?;
        Ok(category_id)
    }

    /// Register a new stream, allocating the next `stream_id` (§4.1).
    /// `category_id` must already be registered (or `0`, the reserved
    /// `$system` category) — REG12.
    pub async fn register_stream(
        &mut self,
        name: impl Into<String>,
        category_id: u64,
    ) -> Result<u64, RegistryError<B::Error>> {
        let stream_id = self.state.stream_high_water_mark() + 1;
        let name = name.into();
        self.append(RegistryRecord::StreamRegistered {
            stream_id,
            category_id,
            name,
        })
        .await?;
        Ok(stream_id)
    }

    /// Register a new event type, allocating the next `event_type_id`
    /// (§4.1). `codec_id` MUST be `>= 1` (REG9).
    pub async fn register_event_type(
        &mut self,
        name: impl Into<String>,
        codec_id: u16,
        schema_fingerprint: [u8; 32],
    ) -> Result<u32, RegistryError<B::Error>> {
        let event_type_id = self.state.event_type_high_water_mark() + 1;
        let name = name.into();
        self.append(RegistryRecord::EventTypeRegistered {
            event_type_id,
            codec_id,
            schema_fingerprint,
            name,
        })
        .await?;
        Ok(event_type_id)
    }

    /// Register a new compression dictionary, allocating the next `dict_id`
    /// (§4.1/§6). `scope_id` must already be registered in the namespace
    /// `scope_kind` selects (REG20); `codec_id` MUST be `>= 1`. There is no
    /// dictionary deletion in v1 (REG19) — a registered `dict_id` lives
    /// forever.
    pub async fn register_dict(
        &mut self,
        scope_kind: u8,
        scope_id: u64,
        codec_id: u16,
        dict_bytes: Vec<u8>,
    ) -> Result<u16, RegistryError<B::Error>> {
        let dict_id = self.state.dict_high_water_mark() + 1;
        self.append(RegistryRecord::DictRegistered {
            dict_id,
            scope_kind,
            scope_id,
            codec_id,
            dict_bytes,
        })
        .await?;
        Ok(dict_id)
    }

    /// Alias (rename) a stream: the old name keeps resolving forever
    /// (REG15/REG16); `stream_id` becomes `new_name`'s current preferred
    /// name.
    pub async fn alias_stream(
        &mut self,
        stream_id: u64,
        new_name: impl Into<String>,
    ) -> Result<(), RegistryError<B::Error>> {
        self.append(RegistryRecord::NameAliased {
            target_kind: TARGET_KIND_STREAM,
            target_id:   stream_id,
            new_name:    new_name.into(),
        })
        .await
    }

    /// Alias (rename) a category.
    pub async fn alias_category(
        &mut self,
        category_id: u64,
        new_name: impl Into<String>,
    ) -> Result<(), RegistryError<B::Error>> {
        self.append(RegistryRecord::NameAliased {
            target_kind: TARGET_KIND_CATEGORY,
            target_id:   category_id,
            new_name:    new_name.into(),
        })
        .await
    }

    /// Alias (rename) an event type. `event_type_id` is zero-extended into
    /// the `u64` wire field per Decision D-REG-E.
    pub async fn alias_event_type(
        &mut self,
        event_type_id: u32,
        new_name: impl Into<String>,
    ) -> Result<(), RegistryError<B::Error>> {
        self.append(RegistryRecord::NameAliased {
            target_kind: TARGET_KIND_EVENT_TYPE,
            target_id:   u64::from(event_type_id),
            new_name:    new_name.into(),
        })
        .await
    }
}
