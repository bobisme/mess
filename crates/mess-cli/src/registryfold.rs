//! Fold the `$registry` out of a store's raw log segments, offline (`bn-2di`).
//!
//! `mess inspect` used to read stream/type names out of fjall's
//! `stream_names`/`type_names` tables. Those keyspaces are gone — the log is
//! the sole source of truth for the `id → name` bijection now — so the CLI
//! reads them the same way recovery does: by decoding the `$registry` stream
//! (spec `04-registry.md`).
//!
//! This is deliberately **offline and lock-free**: it opens the segment files
//! read-only and never takes the store lock, so `mess inspect` keeps
//! working against a store held by a live writer — which was the whole reason
//! it was a separate code path from the engine in the first place. It is also
//! the most direct possible demonstration of the bone's claim: names come out
//! of the log bytes, with nothing else present.

use std::path::Path;

use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::scanner;
use mess_store::registry::{Fold, REGISTRY_STREAM_ID, RegistryState};

use crate::store;

/// Fold every `$registry` record in `dir`'s segments into a [`RegistryState`].
///
/// Returns `Err` only when a segment cannot be read or a registry record does
/// not decode/apply — a store whose registry does not fold is one whose ids
/// have no meaning, and the caller should say so loudly rather than show a
/// half-named report. A store with no `$registry` at all (a brand-new,
/// never-appended store) folds to the empty state, which is correct: it has no
/// names yet.
pub fn fold(dir: &Path) -> Result<RegistryState, String> {
    let rt = RealRuntime::new();
    let fs = rt.fs();

    let mut fold = Fold::new();
    for seg in store::discover_segments(dir) {
        let segment_id = seg.segment_id;
        let (rec, image) =
            scanner::recover_segment_with_image(&fs, &seg.log_path)
                .map_err(|e| format!("scan seg {segment_id}: {e}"))?;
        if rec.header.is_none() {
            continue;
        }
        for b in &rec.accepted {
            if b.stream_id != REGISTRY_STREAM_ID {
                continue;
            }
            let frames = b.frames(&image).map_err(|e| {
                format!("seg {segment_id} registry frames: {e}")
            })?;
            fold.push_batch(
                b.first_global_pos,
                frames.map(|f| f.payload.to_vec()).collect(),
            );
        }
    }
    fold.finish::<std::convert::Infallible>()
        .map_err(|e| format!("$registry does not fold: {e}"))
}

/// `(stream_id, name)` for every registered stream, ascending — the shape
/// `inspect` reports and joins its stream heads against.
#[must_use]
pub fn stream_names(state: &RegistryState) -> Vec<(u64, String)> {
    (1..=state.stream_high_water_mark())
        .filter_map(|id| Some((id, state.stream_name(id)?.to_string())))
        .collect()
}

/// `(event_type_id, name)` for every registered event type, ascending.
#[must_use]
pub fn event_type_names(state: &RegistryState) -> Vec<(u32, String)> {
    (1..=state.event_type_high_water_mark())
        .filter_map(|id| Some((id, state.event_type_name(id)?.to_string())))
        .collect()
}
