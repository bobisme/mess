//! Recovery rebuild of the active index (F6). The active index lives only in
//! memory and persists at seal (D5), so after a crash it must be **rebuilt for
//! every segment that lacks a sealed footer** — and under async sealing a
//! *rolled-but-unsealed* segment is normal, not exceptional (F6): a segment can
//! be full and no longer the append head yet still carry no trailer because its
//! background seal has not completed. Recovery therefore rebuilds the active
//! index for *every* trailer-less segment, not just "the last one".
//!
//! # One code path, not two
//!
//! Rebuild does not re-derive index insertion. It runs each unsealed segment
//! through [`mess_log::sealer::recover_fast`] (which prefers the R2 trailer and
//! falls back to the authoritative full scan — [`mess_log::scanner`]), turns
//! the recovered accepted prefix into [`BatchEntry`]s, and feeds them to the
//! **same** [`ActiveIndex::apply_committed`] the committer uses. The recovery
//! scan *is* the commit authority (D1): its accepted prefix is exactly the
//! committed events, so `recovery.next_pos` is the durable watermark for that
//! segment and every accepted batch ends at or below it.
//!
//! # Seam: sealed segments are bn-20e (later)
//!
//! A segment that `recover_fast` trusts via its footer trailer is **sealed**;
//! its pointer index was consolidated into packed index blocks at seal (D5),
//! and reading those blocks back is a separate bone (bn-20e). This rebuild
//! therefore **skips** sealed segments — it only advances the A1 position chain
//! across them (their `end_pos` seeds the next segment) and records them in
//! [`RebuildReport::sealed_skipped`]. The active index it produces covers the
//! unsealed segments only. That is the documented seam: the *active* head of a
//! stream whose latest events are unsealed is re-derived here (Tier 2,
//! `05-fold-certificates.md` §5.2); a stream whose latest events are in a
//! sealed segment is resolved through bn-20e's sealed index blocks / the G6
//! `StreamHeadTable`, not through this structure.

use std::io;
use std::path::PathBuf;

use mess_log::runtime::Fs;
use mess_log::sealer::{self, FastRecovery};

use crate::active::{ActiveIndex, BatchEntry, EventPtr};

/// What [`rebuild`] did with each segment it was given.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RebuildReport {
    /// `segment_id`s that were unsealed (trailer-less) and were replayed into
    /// the active index, in the order processed.
    pub unsealed_rebuilt: Vec<u64>,
    /// `segment_id`s that were trusted-sealed via their footer trailer and
    /// therefore **skipped** — their pointer index is bn-20e (the documented
    /// seam). Position chain was still advanced across them.
    pub sealed_skipped: Vec<u64>,
    /// `segment_id`s whose header/body did not validate at all (no committed
    /// batches of this generation); nothing was inserted for them.
    pub empty_or_invalid: Vec<u64>,
    /// The global position following the last committed event across all
    /// segments — the exclusive durable end the rebuilt index is applied to.
    pub next_pos: u64,
}

/// Turn one unsealed segment's recovered accepted prefix into the
/// [`BatchEntry`]s the active index inserts. Pure — no I/O — so it is trivially
/// testable against a scan result.
fn batch_entries_from(rec: &mess_log::scanner::Recovery, segment_id: u64) -> Vec<BatchEntry> {
    rec.accepted
        .iter()
        .map(|b| BatchEntry {
            stream_id: b.stream_id,
            first_stream_version: b.first_stream_version,
            frame_count: b.frame_count,
            first_global_pos: b.first_global_pos,
            ptr: EventPtr { segment_id, offset: b.offset },
        })
        .collect()
}

/// Rebuild the active index from `segments`, which MUST be ordered by
/// `segment_id` (the A1 `base_pos` chain, `02-recovery.md` §8.1). Each segment
/// is recovered via the R2 fast path; unsealed segments are replayed into the
/// index through [`ActiveIndex::apply_committed`], sealed segments are skipped
/// (their index is bn-20e). Returns the rebuilt index and a [`RebuildReport`].
///
/// Every I/O goes through the [`Fs`] seam, so the whole rebuild runs on the sim
/// fault filesystem in tests exactly as it does on the real one.
pub fn rebuild<F: Fs>(fs: &F, segments: &[PathBuf]) -> io::Result<(ActiveIndex, RebuildReport)> {
    let index = ActiveIndex::new();
    let report = rebuild_into(&index, fs, segments)?;
    Ok((index, report))
}

/// [`rebuild`] into a caller-provided (typically empty) [`ActiveIndex`]. Lets a
/// caller choose the shard count via [`ActiveIndex::with_shards`].
pub fn rebuild_into<F: Fs>(
    index: &ActiveIndex,
    fs: &F,
    segments: &[PathBuf],
) -> io::Result<RebuildReport> {
    let mut report = RebuildReport::default();
    let mut next_pos = report.next_pos;
    for path in segments {
        match sealer::recover_fast(fs, path)? {
            FastRecovery::Sealed { catalog, .. } => {
                // Sealed: trust the trailer for the position chain, but its
                // pointer index is bn-20e — skip inserting (documented seam).
                report.sealed_skipped.push(catalog.segment_id);
                next_pos = catalog.end_pos;
            }
            FastRecovery::Scanned(rec) => {
                let Some(header) = rec.header else {
                    // No valid header ⇒ no committed batches of this
                    // generation. Nothing to insert; leave the chain as-is.
                    report.empty_or_invalid.push(0);
                    continue;
                };
                let segment_id = header.segment_id;
                if rec.accepted.is_empty() {
                    // Valid header, empty/torn body: an unsealed segment with
                    // no committed batches (e.g. crash right after roll).
                    report.empty_or_invalid.push(segment_id);
                    next_pos = rec.next_pos;
                    continue;
                }
                let entries = batch_entries_from(&rec, segment_id);
                // The scan's accepted prefix IS the committed set (D1); its
                // next_pos is the durable watermark for this segment. Same
                // insertion code as the live committer.
                index.apply_committed(rec.next_pos, &entries);
                report.unsealed_rebuilt.push(segment_id);
                next_pos = rec.next_pos;
            }
        }
    }
    report.next_pos = next_pos;
    Ok(report)
}
