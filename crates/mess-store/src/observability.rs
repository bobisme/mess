//! One coherent operational account of the composed engine (`bn-11ba`).
//!
//! Every Asterism component already keeps local counters — the committer's
//! barrier histogram, the sealer's duration/skip counts, the block cache's
//! hit rate, the sealed-candidate refutation log, the owner ring's occupancy.
//! What did not exist was a single place that says, in one breath:
//!
//! 1. **which state is canonical** and which is a discardable accelerator,
//! 2. **which accelerators are actually installed** right now, per segment,
//! 3. **whether background work or fallback paths are accumulating.**
//!
//! [`EngineObservability`] is that place. It is a *composition*, not a new
//! subsystem: almost every number in it is read from a counter that already
//! existed ([`crate::EngineMetrics`],
//! [`mess_index::sealed::SealMetricsSnapshot`],
//! [`crate::SealedCandidateHealth`], `SealedSegmentIndex`'s per-segment
//! accessors, `RegistryState`'s high-water marks). Build one with
//! [`LogEngine::observability`](crate::LogEngine::observability).
//!
//! # Authority
//!
//! [`AUTHORITY_MODEL`] is the single source of truth for the vocabulary of
//! ADR 0002 (`docs/adr/0002-asterism-capability-authority.md`): the v3 log
//! and its `$registry` records are **canonical**; `.seal` / `.pidx` /
//! `.pcol` / `.filter` / `.reg` / `.par` and the snapshot packs are
//! **discardable accelerators**. Both the in-process report here and the
//! offline `mess doctor` authority view render that one table, so a surface
//! can never present an accelerator as authoritative or disagree with
//! another surface about what happens when one is lost.
//!
//! # Names, units, cardinality
//!
//! Field names are stable and documented in `docs/observability.md`, which is
//! the normative list. Two rules bound this surface:
//!
//! - **Units are in the name.** `*_bytes`, `*_nanos`, `*_secs`, `*_count`,
//!   plain counters are dimensionless totals since this engine handle opened.
//! - **Cardinality is bounded by the segment count.** The only per-item
//!   collections are per *sealed segment* ([`SealedSegmentReport`]) and per
//!   *refuted candidate* — both bounded by how many segments exist on disk, an
//!   operator-scale number. No stream name, stream id, event type, or batch id
//!   is ever a key or a label anywhere in this report.

use std::time::Duration;

use mess_index::sealed::{PackIdentity, SealMetricsSnapshot, dircodec_name};
use mess_log::committer::{CommitterMetrics, Durability};
use mess_log::metrics::LatencySnapshot;

use crate::AppendInputMetrics;
use crate::sealed_candidate::Refutation;

// ---------------------------------------------------------------------------
// Authority model (ADR 0002 vocabulary, shared with the offline CLI view)
// ---------------------------------------------------------------------------

/// Whether an artifact carries event authority or merely accelerates access
/// to state the canonical bytes already determine.
///
/// The distinction is ADR 0002's, and it is not cosmetic: a
/// [`Role::Canonical`] artifact's loss is data loss, while a
/// [`Role::DiscardableAccelerator`]'s loss costs time and nothing else. A
/// report that blurs the two is worse than no report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Event authority. Losing these bytes loses events.
    Canonical,
    /// Derived, rebuildable acceleration. Losing it degrades performance and
    /// nothing else; the engine falls back to the canonical bytes.
    DiscardableAccelerator,
}

impl Role {
    /// The stable machine token used in reports (`canonical`,
    /// `discardable-accelerator`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Canonical => "canonical",
            Role::DiscardableAccelerator => "discardable-accelerator",
        }
    }
}

/// One class of on-disk artifact, with its authority role and — the part an
/// operator actually needs — what the engine does when it is missing or
/// refuses to validate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactClass {
    /// The filename shape or logical name (`seg-*.log`, `.seal`, `$registry`).
    pub name:    &'static str,
    /// Canonical or discardable.
    pub role:    Role,
    /// One line: what the artifact holds.
    pub what:    &'static str,
    /// One line: what the engine does when it is absent, unreadable, or
    /// refuted. For a canonical artifact this is the failure mode; for an
    /// accelerator it is the fallback path.
    pub on_loss: &'static str,
}

/// The complete authority classification, in report order: canonical sources
/// first, then every discardable accelerator.
///
/// This is the one table. `mess doctor`'s offline authority view and
/// [`EngineObservability::authority`] both render it, so the CLI and the
/// engine cannot drift apart about which bytes are authoritative.
pub const AUTHORITY_MODEL: &[ArtifactClass] = &[
    ArtifactClass {
        name:    "seg-*.log",
        role:    Role::Canonical,
        what:    "the v3 event log: every committed batch, in global position \
                  order. The only event authority.",
        on_loss: "data loss. A missing or truncated segment is unrecoverable \
                  from any other artifact in the store; recovery accepts only \
                  the durable prefix that validates.",
    },
    ArtifactClass {
        name:    "$registry",
        role:    Role::Canonical,
        what:    "stream/category/event-type/dict name registrations, as real \
                  committed log events on the reserved stream (spec 04). \
                  Canonical, not a side table.",
        on_loss: "data loss of the name space, and only with the log records \
                  that carry it. It is re-derived by folding the log on every \
                  open; no separate file can be lost.",
    },
    ArtifactClass {
        name:    ".seal",
        role:    Role::DiscardableAccelerator,
        what:    "the consolidated SealPack: stream directory, pointer blocks \
                  + skips, stream filter, payload columns and registry delta \
                  for one sealed segment, in one file.",
        on_loss: "the segment is served by scanning its log bytes on open, \
                  and the background sealer is owed a fresh seal. Reads stay \
                  correct and complete; cold open and sealed reads get slower.",
    },
    ArtifactClass {
        name:    ".pidx",
        role:    Role::DiscardableAccelerator,
        what:    "the legacy loose pointer sidecar — the pre-SealPack shape \
                  of the same stream directory and pointer blocks. Shadowed \
                  whenever a sibling .seal exists.",
        on_loss: "identical to a lost .seal: log scan on open plus an owed \
                  re-seal.",
    },
    ArtifactClass {
        name:    ".pcol",
        role:    Role::DiscardableAccelerator,
        what:    "columnar payload blocks for a loose-sealed segment, \
                  attached lazily at open.",
        on_loss: "payloads are reassembled from the log frames instead. Same \
                  bytes, more I/O.",
    },
    ArtifactClass {
        name:    ".filter",
        role:    Role::DiscardableAccelerator,
        what:    "a per-segment stream membership filter, consulted to skip \
                  segments a stream cannot be in.",
        on_loss: "every segment is probed through its pointer directory \
                  instead. Same results, more lookups.",
    },
    ArtifactClass {
        name:    ".reg",
        role:    Role::DiscardableAccelerator,
        what:    "a loose-sealed segment's $registry batches, laid out for \
                  one sequential read at open instead of one random read per \
                  registration.",
        on_loss: "recovery point-reads the same $registry batches out of the \
                  log through the pointer index. Identical registry, \
                  O(#names) reads instead of O(#segments).",
    },
    ArtifactClass {
        name:    ".par",
        role:    Role::DiscardableAccelerator,
        what:    "opt-in Reed-Solomon parity over a sealed segment's log \
                  bytes, for `mess verify --repair`.",
        on_loss: "no repair option; detection of corruption is unaffected \
                  (the batch CRCs and fold chain are in the log itself).",
    },
    ArtifactClass {
        name:    "snapshot pack",
        role:    Role::DiscardableAccelerator,
        what:    "aggregate state snapshots and their discovery root (ADR \
                  0002 §1). Never an event record; no SnapshotInstalled event \
                  exists in v3.",
        on_loss: "a load miss: the aggregate is rebuilt by full replay from \
                  the log. Never an availability failure.",
    },
];

/// Look up one [`ArtifactClass`] by its `name`.
#[must_use]
pub fn artifact_class(name: &str) -> Option<&'static ArtifactClass> {
    AUTHORITY_MODEL.iter().find(|c| c.name == name)
}

// ---------------------------------------------------------------------------
// Durability mode
// ---------------------------------------------------------------------------

/// The engine's configured durability contract, rendered for a report.
///
/// [`Durability`] itself is a `mess-log` type with no stable string form;
/// this is the reportable projection of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurabilityMode {
    /// `process`, `os`, or `group`.
    pub name:            &'static str,
    /// Group mode's coalescing deadline, nanoseconds. `None` otherwise.
    pub max_delay_nanos: Option<u64>,
    /// Group mode's byte ceiling per group. `None` otherwise.
    pub max_bytes:       Option<u64>,
}

impl DurabilityMode {
    /// Project a [`Durability`] into its reportable form.
    #[must_use]
    pub fn of(d: Durability) -> Self {
        match d {
            Durability::Process => DurabilityMode {
                name:            "process",
                max_delay_nanos: None,
                max_bytes:       None,
            },
            Durability::Os => DurabilityMode {
                name:            "os",
                max_delay_nanos: None,
                max_bytes:       None,
            },
            Durability::Group { max_delay, max_bytes } => DurabilityMode {
                name:            "group",
                max_delay_nanos: Some(duration_nanos(max_delay)),
                max_bytes:       Some(max_bytes),
            },
        }
    }
}

fn duration_nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

// ---------------------------------------------------------------------------
// The composed report
// ---------------------------------------------------------------------------

/// The composed in-process account of one open [`LogEngine`](crate::LogEngine).
///
/// Snapshot semantics match [`crate::EngineMetrics`]: each field is loaded
/// independently from a relaxed atomic or a short read lock, so a single
/// report is internally coherent only while appends are quiescent. Deltas
/// between two reports are exact for the monotone counters.
#[derive(Debug, Clone)]
pub struct EngineObservability {
    /// Which state is canonical and which is discardable — the ADR 0002
    /// table, carried so a consumer of this report never has to guess.
    pub authority:    &'static [ArtifactClass],
    /// Append-owner queue, group commit, and outcome counters.
    pub owner:        OwnerReport,
    /// Durability mode and every barrier/degradation signal.
    pub durability:   DurabilityReport,
    /// Canonical watermarks and resident state.
    pub state:        StateReport,
    /// Which accelerators are installed, per sealed segment.
    pub accelerators: AcceleratorReport,
    /// Fallback and refutation counters — the "is a degraded path
    /// accumulating?" question.
    pub fallbacks:    FallbackReport,
    /// Background work still owed.
    pub backlog:      BacklogReport,
}

/// Append-owner saturation, group-commit shape, and per-append outcomes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OwnerReport {
    /// The configured durability contract (it decides whether the owner
    /// coalesces at all).
    pub durability_mode:       DurabilityMode,
    /// Intent slots currently occupied in the bounded owner channel.
    pub queue_slots_in_use:    usize,
    /// The channel's fixed slot bound.
    pub queue_slots_capacity:  usize,
    /// Byte permits currently held by admitted intents and by producer
    /// preparation still under construction.
    pub queue_bytes_in_use:    usize,
    /// The owner ring's fixed byte bound.
    pub queue_bytes_capacity:  usize,
    /// Group size distribution, in **intents per gathered group** (not
    /// nanoseconds — the histogram primitive is shared, the unit is not).
    pub group_width:           LatencySnapshot,
    /// How long each group spent gathering before it was committed,
    /// nanoseconds. This is the owner-side queue delay.
    pub group_wait:            LatencySnapshot,
    /// End-to-end append latency, nanoseconds: admission to the owner ring
    /// until the caller's outcome is fulfilled. Covers queue wait, gather,
    /// write, barrier, and publish.
    pub ack_latency:           LatencySnapshot,
    /// The owner's durable-commit span per group, nanoseconds: write plus
    /// barrier as the owner sees it. The barrier half alone is
    /// [`DurabilityReport::barrier`]; the difference is the write half.
    pub commit_latency:        LatencySnapshot,
    /// Appends refused for an expected-version mismatch since this engine
    /// opened.
    pub conflicts:             u64,
    /// Appends whose caller dropped its future before the outcome was
    /// delivered. The events still committed and published — this counts a
    /// caller that stopped listening, not lost work.
    pub cancellations:         u64,
    /// Commit groups committed (one barrier each in a barriered mode).
    pub groups:                u64,
    /// Batches durably written.
    pub batches:               u64,
    /// Events durably written, `$registry` records included.
    pub events:                u64,
    /// Payload plus framing bytes durably written.
    pub bytes:                 u64,
    /// Owner result slots retained for reuse across groups.
    pub outcome_scratch_slots: usize,
    /// Owner result scratch retained, bytes.
    pub outcome_scratch_bytes: usize,
    /// Oversize owner result groups whose transient capacity was shed.
    pub outcome_scratch_trims: usize,
    /// Ownership-transfer and defensive-copy counters for the append
    /// boundary.
    pub append_input:          AppendInputMetrics,
}

impl OwnerReport {
    /// Occupied slots as a fraction of the channel bound, `[0, 1]`.
    #[must_use]
    pub fn slot_saturation(&self) -> f64 {
        ratio(self.queue_slots_in_use, self.queue_slots_capacity)
    }

    /// Held byte permits as a fraction of the ring bound, `[0, 1]`.
    #[must_use]
    pub fn byte_saturation(&self) -> f64 {
        ratio(self.queue_bytes_in_use, self.queue_bytes_capacity)
    }
}

fn ratio(num: usize, den: usize) -> f64 {
    if den == 0 { 0.0 } else { num as f64 / den as f64 }
}

/// Durability contract plus every barrier and degradation signal, commit path
/// and seal path alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurabilityReport {
    /// The configured mode.
    pub mode:                        DurabilityMode,
    /// Commit-path `fdatasync` latency, nanoseconds (spec 03 §2.6).
    pub barrier:                     LatencySnapshot,
    /// Sticky: commit-path barrier latency crossed the threshold at least
    /// once. Slow, not wrong.
    pub barrier_degraded:            bool,
    /// Commit-path barriers that crossed the threshold.
    pub barrier_degraded_trips:      u64,
    /// The active commit-path degradation threshold, nanoseconds.
    pub barrier_threshold_nanos:     u64,
    /// Seal-path barrier latency, nanoseconds (the sidecar and directory
    /// fsyncs the sealer issues off the append path).
    pub seal_barrier:                LatencySnapshot,
    /// Sticky seal-path degradation flag.
    pub seal_barrier_degraded:       bool,
    /// Seal-path barriers that crossed the threshold.
    pub seal_barrier_degraded_trips: u64,
    /// A barrier **fault** has poisoned the store: writes fail fast and reads
    /// clamp to the frozen watermark. Distinct from `barrier_degraded`, which
    /// is merely slow.
    pub poisoned:                    bool,
    /// Whether the tamper-evident per-stream fold chain is on for this store.
    pub fold_chain_enabled:          bool,
}

/// Canonical watermarks and the resident state built on top of them.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StateReport {
    /// The canonical v3 log format version this engine reads and writes.
    /// v4 was declined (ADR 0003), so this is the only value.
    pub log_format_version:          u16,
    /// The published read watermark: the exclusive end of the global position
    /// sequence visible to readers. Includes filtered `$registry` positions,
    /// so it is not an application-event count.
    pub published_watermark:         u64,
    /// The owner's durable watermark. May lead
    /// [`published_watermark`](Self::published_watermark) while the in-process
    /// index tiers catch up.
    pub durable_watermark:           u64,
    /// The hot (active) index's applied exclusive end. Every active-tier
    /// resolve clamps to this.
    pub active_index_applied_end:    u64,
    /// Seconds since this process opened the active segment.
    pub active_segment_age_secs:     f64,
    /// Sealed segments installed in the cold tier.
    pub sealed_segment_count:        usize,
    /// The highest sealed-store install generation. Monotone per open; it
    /// advances on every install, so a change between two reports means the
    /// cold tier was re-published.
    pub sealed_install_generation:   u64,
    /// Summed `resident_bytes()` over every installed sealed index — the one
    /// exact index-memory number the engine has.
    pub sealed_index_resident_bytes: u64,
    /// Live blocks in the sealed block cache.
    pub block_cache_entries:         usize,
    /// Resident block-cache weight, bytes.
    pub block_cache_bytes:           u64,
    /// Cumulative block-cache hits.
    pub block_cache_hits:            u64,
    /// Cumulative block-cache misses.
    pub block_cache_misses:          u64,
    /// Block-cache hit rate over all lookups so far, `[0, 1]`.
    pub block_cache_hit_rate:        f64,
    /// Highest assigned stream id in the canonical registry fold.
    pub registry_stream_hwm:         u64,
    /// Highest assigned category id.
    pub registry_category_hwm:       u64,
    /// Highest assigned event-type id.
    pub registry_event_type_hwm:     u32,
    /// Highest assigned payload-dictionary id.
    pub registry_dict_hwm:           u16,
    /// Payload frames materialised during this open's recovery. Zero for
    /// every chain-off open.
    pub recover_payload_decodes:     u64,
}

/// Which sealed-index shape is actually serving one segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealedRepresentation {
    /// A consolidated `.seal` SealPack.
    SealPack,
    /// The legacy loose `.pidx` sidecar family.
    LooseSidecar,
}

impl SealedRepresentation {
    /// The stable machine token (`seal-pack`, `loose-sidecar`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            SealedRepresentation::SealPack => "seal-pack",
            SealedRepresentation::LooseSidecar => "loose-sidecar",
        }
    }
}

/// The accelerators installed for one sealed segment.
///
/// One row per **installed sealed segment**, so the collection is bounded by
/// the store's segment count. No stream or event-type identity appears here;
/// `stream_count` is a number, never a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedSegmentReport {
    /// The segment this index covers.
    pub segment_id:          u64,
    /// The segment's first global position.
    pub base_pos:            u64,
    /// Events the segment holds.
    pub event_count:         u64,
    /// Distinct streams the segment touches (a count, not a label).
    pub stream_count:        usize,
    /// SealPack or loose sidecar.
    pub representation:      SealedRepresentation,
    /// The SealPack's whole-pack identity, lowercase hex. `None` for a loose
    /// sidecar, which has no identity to bind a footer to.
    pub pack_identity_hex:   Option<String>,
    /// The SealPack format version. `None` for a loose sidecar.
    pub pack_format_version: Option<u16>,
    /// The stream-directory codec id.
    pub dir_codec:           u16,
    /// The stream-directory codec's stable name (`bitrank`, `sorted`).
    pub dir_codec_name:      &'static str,
    /// Bytes this index holds resident.
    pub resident_bytes:      u64,
    /// Whether the pointer sections are held resident rather than re-read
    /// per lookup.
    pub sections_resident:   bool,
    /// Whether a payload accelerator (`.pcol` or the pack's payload section)
    /// is attached.
    pub has_payload_index:   bool,
    /// Whether per-event type ids are available without decoding payloads.
    pub has_event_types:     bool,
    /// Whether a registry delta is reachable through this index.
    pub has_registry_delta:  bool,
    /// The sealed-store install generation for this segment.
    pub install_generation:  u64,
    /// Whether the segment's active-index entries have been evicted (the
    /// cold tier is now the only server for it).
    pub active_evicted:      bool,
}

/// The per-segment accelerator inventory plus its rollup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceleratorReport {
    /// Whether this engine seals new segments as SealPacks (the default) or
    /// in the loose-sidecar compatibility mode.
    pub seal_pack_enabled:      bool,
    /// Segments served by a `.seal` pack.
    pub seal_pack_segments:     usize,
    /// Segments served by a loose `.pidx` family.
    pub loose_sidecar_segments: usize,
    /// One row per installed sealed segment, ascending by segment id.
    pub segments:               Vec<SealedSegmentReport>,
}

/// Every counter that says "a degraded or fallback path ran".
///
/// None of these is data loss: each names a place where the engine declined
/// to trust an accelerator and used the canonical log instead. Non-zero means
/// something to look at, not something to panic about — but a value that
/// keeps growing across reopens means the store has not converged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FallbackReport {
    /// Sealed-index candidates this open refused to admit.
    pub sealed_candidates_refuted:     u64,
    /// Refuted candidates successfully renamed out of the candidate
    /// namespace. Equal to `sealed_candidates_refuted` in the healthy case.
    pub sealed_candidates_quarantined: u64,
    /// Refuted candidates whose quarantine rename failed (read-only or full
    /// filesystem). The store is correct but has not converged.
    pub quarantine_failures:           u64,
    /// Per-candidate refutation reasons, in classification order. Bounded by
    /// the candidate count.
    pub refutations:                   Vec<Refutation>,
    /// Sealed segments whose `$registry` batches were read out of an admitted
    /// registry delta at open (the fast path).
    pub registry_delta_admitted:       u64,
    /// Sealed segments whose registry delta was absent, unreadable, or
    /// rejected by the layout cross-check, so recovery point-read the same
    /// `$registry` batches out of the log instead. Correct either way; this
    /// is the O(#names) path.
    pub registry_delta_fallback:       u64,
    /// Seals the background sealer gave up on rather than completing. The
    /// segment stays durable and unsealed, served from the log.
    pub seals_skipped:                 u64,
}

impl FallbackReport {
    /// Whether any fallback or refutation path has run at all.
    #[must_use]
    pub fn any(&self) -> bool {
        self.sealed_candidates_refuted > 0
            || self.quarantine_failures > 0
            || self.registry_delta_fallback > 0
            || self.seals_skipped > 0
    }
}

/// Background work the engine still owes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacklogReport {
    /// Rolled segments re-queued for sealing at this open — the owed re-seal
    /// set. Zero for a healthy store.
    pub reseals_owed_at_open: u64,
    /// The segment ids in that set. Bounded by the segment count.
    pub pending_reseal:       Vec<u64>,
    /// Seal jobs queued or in progress right now: rolls reported by the
    /// committer plus re-seals enqueued at open, minus jobs the sealer thread
    /// has finished.
    pub seal_queue_depth:     usize,
    /// Seal jobs the sealer thread has dequeued since this open.
    pub seal_jobs_dequeued:   u64,
    /// Segments sealed since this open.
    pub seals_completed:      u64,
    /// Seals skipped rather than completed.
    pub seals_skipped:        u64,
    /// Seal wall-clock duration (`roll` to `sealed installed`), nanoseconds.
    pub seal_duration:        LatencySnapshot,
}

impl BacklogReport {
    /// Whether any background seal work is outstanding.
    #[must_use]
    pub fn draining(&self) -> bool { self.seal_queue_depth > 0 }
}

// ---------------------------------------------------------------------------
// Builders used by `LogEngine::observability` (kept here so `engine.rs` only
// gains one small, additive method)
// ---------------------------------------------------------------------------

impl DurabilityReport {
    /// Compose the durability picture out of the committer's and sealer's
    /// existing snapshots.
    #[must_use]
    pub fn compose(
        durability: Durability,
        commit: &CommitterMetrics,
        seal: &SealMetricsSnapshot,
        poisoned: bool,
        fold_chain_enabled: bool,
    ) -> Self {
        DurabilityReport {
            mode: DurabilityMode::of(durability),
            barrier: commit.fsync,
            barrier_degraded: commit.fsync_degraded,
            barrier_degraded_trips: commit.fsync_degraded_trips,
            barrier_threshold_nanos: commit.fsync_threshold_nanos,
            seal_barrier: seal.fsync,
            seal_barrier_degraded: seal.fsync_degraded,
            seal_barrier_degraded_trips: seal.fsync_degraded_trips,
            poisoned,
            fold_chain_enabled,
        }
    }
}

impl SealedSegmentReport {
    /// Read one installed sealed index's accelerator inventory.
    #[must_use]
    pub fn of(
        index: &mess_index::sealed::segment::SealedSegmentIndex,
        install_generation: u64,
        active_evicted: bool,
    ) -> Self {
        let identity: Option<PackIdentity> = index.pack_identity();
        SealedSegmentReport {
            segment_id: index.segment_id(),
            base_pos: index.base_pos(),
            event_count: index.event_count(),
            stream_count: index.stream_count(),
            representation: if identity.is_some() {
                SealedRepresentation::SealPack
            } else {
                SealedRepresentation::LooseSidecar
            },
            pack_identity_hex: identity.map(|i| i.hex()),
            pack_format_version: index.pack_format_version(),
            dir_codec: index.dir_codec(),
            dir_codec_name: dircodec_name(index.dir_codec()),
            resident_bytes: index.resident_bytes() as u64,
            sections_resident: index.sections_resident(),
            has_payload_index: index.has_payload(),
            has_event_types: index.has_event_types(),
            has_registry_delta: index.has_registry_delta(),
            install_generation,
            active_evicted,
        }
    }
}
