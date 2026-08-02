//! `mess doctor`'s authority view (`bn-11ba`) — which bytes are canonical,
//! which are discardable acceleration, and what the engine does when an
//! accelerator is missing or refuses to validate.
//!
//! Every other doctor check answers "is this artifact healthy?". This one
//! answers the question an operator asks *first*, and the one a report can
//! get catastrophically wrong: **does losing this cost me data, or only
//! time?** ADR 0002 (`docs/adr/0002-asterism-capability-authority.md`) fixed
//! the answer — the v3 log and its `$registry` records are the only event
//! authority; `.seal`, `.pidx`, `.pcol`, `.filter`, `.reg`, `.par` and the
//! snapshot packs are discardable accelerators — and
//! [`mess_store::AUTHORITY_MODEL`] is that decision as data.
//!
//! This module renders that one table. It does not define a second one: if
//! the engine's own [`LogEngine::observability`](mess_store::LogEngine)
//! report and this offline view ever disagreed about what is authoritative,
//! one of them would be lying to an operator mid-incident. They read the same
//! `const`.
//!
//! # Offline scope
//!
//! This runs against files, with no lock and no engine, so it reports the
//! *installed* state of each accelerator and the fallback the engine **would**
//! take. Live counters — how many candidates were actually refuted, how deep
//! the seal backlog is, whether the registry-delta fallback is running — are
//! in-process facts only, and a separate reader process cannot observe another
//! process's counters. Those live on
//! `mess_store::EngineObservability`; an advisory in the report says so.
//!
//! # Cardinality
//!
//! One row per segment on disk, one entry per artifact class. Both are
//! bounded by the store's segment count. No stream name, stream id, event
//! type, or batch id appears anywhere in this section.

use std::path::Path;

use mess_index::sealed::segment::SealedSegmentIndex;
use mess_index::sealed::{dir_codec_of, dircodec_name};
use mess_store::observability::{AUTHORITY_MODEL, Role};
use serde_json::{Value, json};

use crate::report::{Finding, Report, Severity};
use crate::store::{self, SealedArtifact, SegmentFile};

/// Whether one artifact is installed, missing, or present-but-unusable.
///
/// The three-way split is the point. "Absent" is an ordinary, expected state
/// (nothing has sealed this segment yet, parity is opt-in); "degraded" means
/// the bytes exist and the engine would refuse them — a crash window or a
/// bug, and the only one of the three worth waking up for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactState {
    /// On disk and structurally valid.
    Present,
    /// Not on disk.
    Absent,
    /// On disk but unreadable, wrong-segment, or failing its own checksums.
    Degraded,
    /// On disk but inert because a higher-preference artifact serves this
    /// segment (a `.pidx` shadowed by a `.seal`).
    Shadowed,
}

impl ArtifactState {
    /// The stable machine token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ArtifactState::Present => "present",
            ArtifactState::Absent => "absent",
            ArtifactState::Degraded => "degraded",
            ArtifactState::Shadowed => "shadowed",
        }
    }
}

/// Attach the authority section to `report` and emit its single summary
/// finding.
///
/// One finding, not one per segment: `check_segments`/`check_sealed_artifact`
/// already report each unreadable pack or missing sidecar at its own
/// severity, and a second per-segment finding at a *different* severity for
/// the same file would make the report argue with itself. The value this
/// check adds is the classification and the fallback text, which live in the
/// structured payload.
pub fn check(report: &mut Report, dir: &Path) {
    let segs = store::discover_segments(dir);
    let mut rows: Vec<Value> = Vec::with_capacity(segs.len());
    let (mut present, mut absent, mut degraded) = (0usize, 0usize, 0usize);
    let (mut packs, mut sidecars, mut unindexed) = (0usize, 0usize, 0usize);

    for seg in &segs {
        let row = segment_row(seg);
        for state in row.states {
            match state {
                ArtifactState::Present => present += 1,
                ArtifactState::Absent => absent += 1,
                ArtifactState::Degraded => degraded += 1,
                ArtifactState::Shadowed => {}
            }
        }
        match row.serving {
            Serving::SealPack => packs += 1,
            Serving::LooseSidecar => sidecars += 1,
            Serving::LogScan => unindexed += 1,
        }
        rows.push(row.json);
    }

    let canonical: Vec<Value> = AUTHORITY_MODEL
        .iter()
        .filter(|c| c.role == Role::Canonical)
        .map(class_json)
        .collect();
    let accelerators: Vec<Value> = AUTHORITY_MODEL
        .iter()
        .filter(|c| c.role == Role::DiscardableAccelerator)
        .map(class_json)
        .collect();

    report.set(
        "authority",
        json!({
            // Offline: file state and the fallback the engine WOULD take.
            "scope": "offline",
            // ADR 0003 declined v4; v3 is the only canonical log version.
            "log_format_version": 3,
            "canonical": canonical,
            "accelerators": accelerators,
            "summary": {
                "segments": segs.len(),
                "served_by_seal_pack": packs,
                "served_by_loose_sidecar": sidecars,
                "served_by_log_scan": unindexed,
                "artifacts_present": present,
                "artifacts_absent": absent,
                "artifacts_degraded": degraded,
            },
            "segments": rows,
        }),
    );
    // A big store has a lot of segments; the JSON payload always carries them
    // all, the human/agent render shows the head.
    report.limit_display("authority.segments", 20);

    let (severity, message) = if degraded > 0 {
        (
            Severity::Warn,
            format!(
                "{degraded} accelerator artifact(s) are present but unusable; \
                 the log is canonical and still serves every read — see \
                 authority.segments for the per-segment fallback"
            ),
        )
    } else if unindexed > 0 {
        (
            Severity::Info,
            format!(
                "{unindexed} segment(s) have no sealed index and are served \
                 by scanning their log bytes; the log is canonical, so reads \
                 are correct and complete"
            ),
        )
    } else {
        (
            Severity::Ok,
            format!(
                "the log and its $registry records are canonical; all {} \
                 accelerator class(es) are discardable and every installed \
                 one validated",
                accelerator_class_count()
            ),
        )
    };
    report.push_finding(
        Finding::new(severity, "authority", "authority-accelerators", message)
            .with("artifacts_present", present)
            .with("artifacts_absent", absent)
            .with("artifacts_degraded", degraded)
            .with("served_by_log_scan", unindexed),
    );

    report.advice.push(json!({
        "level": "info",
        "type": "authority-scope",
        "message": "authority.* is an offline file-state view: it reports \
                    which accelerators are installed and what the engine \
                    would fall back to. Live counters (candidates refuted \
                    this open, seal backlog depth, registry-delta fallbacks) \
                    are in-process only — read them from \
                    LogEngine::observability() inside the writer.",
    }));
}

fn accelerator_class_count() -> usize {
    AUTHORITY_MODEL
        .iter()
        .filter(|c| c.role == Role::DiscardableAccelerator)
        .count()
}

/// One artifact class as a report row: its name, its role, and — the
/// operationally load-bearing half — what losing it costs.
///
/// `ArtifactClass::what` ("what the artifact holds") is deliberately *not*
/// here. It is reference documentation, it lives in `docs/observability.md`
/// and in the `AUTHORITY_MODEL` doc comments, and including it would roughly
/// double a `text`-format doctor run for an audience the CLI conventions ask
/// us to keep token-efficient. `on_loss` is the part an operator cannot look
/// up fast enough mid-incident.
fn class_json(c: &mess_store::ArtifactClass) -> Value {
    json!({
        "name": c.name,
        "role": c.role.as_str(),
        "on_loss": c.on_loss,
    })
}

/// Which shape actually serves a segment's reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Serving {
    SealPack,
    LooseSidecar,
    /// No usable sealed index: the engine scans the segment's log bytes.
    LogScan,
}

impl Serving {
    fn as_str(self) -> &'static str {
        match self {
            Serving::SealPack => "seal-pack",
            Serving::LooseSidecar => "pidx",
            Serving::LogScan => "log-scan",
        }
    }

    /// What the engine does for this segment, in one line.
    fn fallback(self) -> &'static str {
        match self {
            Serving::SealPack => {
                "reads resolve through the .seal pack's pointer directory; \
                 losing it costs a log scan on open and an owed re-seal, not \
                 data"
            }
            Serving::LooseSidecar => {
                "reads resolve through the .pidx pointer sidecar; losing it \
                 costs a log scan on open and an owed re-seal, not data"
            }
            Serving::LogScan => {
                "no usable sealed index: recovery scans this segment's log \
                 bytes and the background sealer is owed a seal. Reads are \
                 correct and complete throughout — the log is the authority"
            }
        }
    }
}

struct SegmentRow {
    json:    Value,
    serving: Serving,
    /// Every artifact class's state, for the rollup.
    states:  Vec<ArtifactState>,
}

fn segment_row(seg: &SegmentFile) -> SegmentRow {
    // The pack and the sidecar are the two *primary* candidates, and the
    // engine's dual read prefers the pack. Validate whichever is load-bearing
    // the same way the engine would (header + section directory + trailer;
    // `open_pack`, not `open_pack_eager`, so this asks "would the next open
    // install this?" rather than "does every byte reassemble?" — that is
    // `mess verify`'s question).
    let seal_state = if seg.has_seal {
        match SealedSegmentIndex::open_pack(&seg.seal_path) {
            Ok(idx) if idx.segment_id() == seg.segment_id => {
                ArtifactState::Present
            }
            _ => ArtifactState::Degraded,
        }
    } else {
        ArtifactState::Absent
    };
    let pidx_state = if !seg.has_pidx {
        ArtifactState::Absent
    } else if seal_state == ArtifactState::Present {
        // bn-3of: a sidecar under a healthy pack is inert, not broken.
        ArtifactState::Shadowed
    } else {
        match SealedSegmentIndex::open(&seg.pidx_path) {
            Ok(idx) if idx.segment_id() == seg.segment_id => {
                ArtifactState::Present
            }
            _ => ArtifactState::Degraded,
        }
    };

    let serving = match (seal_state, pidx_state) {
        (ArtifactState::Present, _) => Serving::SealPack,
        (_, ArtifactState::Present) => Serving::LooseSidecar,
        _ => Serving::LogScan,
    };

    // The `.pidx` family's siblings and the opt-in parity file: presence
    // only. Their integrity is `mess verify`'s job, and a pack-sealed segment
    // carries the same content as sections inside the `.seal` rather than as
    // separate files, which is why they read "absent" there and that is
    // correct, not a fault.
    let pcol_state = presence(seg.has_pcol);
    let filter_state = presence(seg.filter_path.exists());
    let reg_state = presence(seg.has_reg);
    let par_state = presence(seg.has_par);

    // Pack identity + directory codec: what the *serving* artifact is, read
    // straight off its header. Never a hash of the whole file — that
    // comparison is `mess verify`'s.
    let (identity, codec) = match serving {
        Serving::SealPack => (
            SealedSegmentIndex::open_pack(&seg.seal_path)
                .ok()
                .and_then(|i| i.pack_identity())
                .map(|i| i.hex()),
            dir_codec_of(&seg.seal_path).ok(),
        ),
        Serving::LooseSidecar => (None, dir_codec_of(&seg.pidx_path).ok()),
        Serving::LogScan => (None, None),
    };

    let json = json!({
        "segment_id": seg.segment_id,
        "serving": serving.as_str(),
        "serving_role": Role::DiscardableAccelerator.as_str(),
        "canonical_source": "seg-*.log",
        "fallback": serving.fallback(),
        "sealed_artifact": seg.sealed_artifact().as_str(),
        "pack_identity": identity,
        "dir_codec": codec,
        "dir_codec_name": codec.map(dircodec_name),
        "artifacts": {
            ".seal": seal_state.as_str(),
            ".pidx": pidx_state.as_str(),
            ".pcol": pcol_state.as_str(),
            ".filter": filter_state.as_str(),
            ".reg": reg_state.as_str(),
            ".par": par_state.as_str(),
        },
    });

    SegmentRow {
        json,
        serving,
        states: vec![
            seal_state,
            pidx_state,
            pcol_state,
            filter_state,
            reg_state,
            par_state,
        ],
    }
}

fn presence(exists: bool) -> ArtifactState {
    if exists { ArtifactState::Present } else { ArtifactState::Absent }
}

/// Whether a segment has any sealed index at all, using the engine's own
/// preference order. Kept here so a caller can ask the authority question
/// without re-deriving [`SealedArtifact`].
#[must_use]
pub fn is_accelerated(seg: &SegmentFile) -> bool {
    seg.sealed_artifact() != SealedArtifact::None
}
