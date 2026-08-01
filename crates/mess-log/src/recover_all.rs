//! Whole-log recovery (bn-2en): the orchestrator that recovers **every**
//! segment of a log and stitches them into a single committed prefix, plus the
//! R1 per-segment parallel scan and the R2 manifest-seeded fast path.
//!
//! Spec: [`docs/spec/02-recovery.md`] §8 is normative. The per-*segment* scan
//! (the acceptance kernel, A1–A12) lives in [`crate::scanner`]; the R2 footer
//! fast path and the [`SegmentCatalogEntry`] live in [`crate::sealer`]; this
//! module is the layer above them — it does not re-implement any acceptance
//! rule, it only *composes* per-segment recoveries and enforces the
//! cross-segment invariants of §8.1.
//!
//! # The two authorities, and why they agree (§8.2 / §8.3 / §8.4)
//!
//! - [`RecoveryMode::Full`] scans and CRC-validates every batch of every
//!   segment (§8.2, the authority, D1): footers and the manifest are never
//!   trusted in its place.
//! - [`RecoveryMode::Fast`] trusts each sealed segment via its footer trailer
//!   (§8.3, R2, one `pread` from EOF — or one manifest read for all of them)
//!   and fully scans only the unsealed active tail.
//!
//! Both drive the *same* stitch and MUST agree on the committed prefix (§8.4);
//! [`WholeLog::prefix_eq`] is the equivalence relation the property tests
//! assert over DST-style seeded corpora (including torn tails).
//!
//! # Cross-segment stitch (§8.1)
//!
//! Segments are ordered by `segment_id`. Contiguity is threaded across the
//! boundary by `base_pos`: segment *k+1*'s `base_pos` MUST equal segment *k*'s
//! `end_pos`, and the `epoch` chain MUST be strictly increasing (a fresh/rolled
//! segment always carries a larger epoch, A9). A segment that validates
//! internally but breaks either chain **breaks the log and MUST fail recovery**
//! (§8.2 R1) — surfaced as a typed [`StitchError`]. A torn tail in the *active*
//! (last) segment is not a break: it is the normal stop-at-first-hole point
//! (A10), and everything in later segment files is dead space.
//!
//! # R1 parallelism, and its sim-determinism gate
//!
//! Because A8 makes segments independent and each carries its own A1/A9 seed,
//! per-segment recovery is embarrassingly parallel (§8.2 R1).
//! [`RecoverOptions`] carries a `parallel` flag: when set, segments are
//! recovered on a [`std::thread::scope`] worker fan-out over the [`Fs`] seam;
//! when clear, they are recovered serially in-order. **Determinism:** the
//! parallel path only affects *scheduling*, never the result (the stitch is
//! serial and pure), and the deterministic-simulation harness always passes
//! `parallel: false` so a sim run is bit-reproducible. Callers on the real
//! runtime pass `true` to reach the device ceiling.
//!
//! [`docs/spec/02-recovery.md`]: ../../../../docs/spec/02-recovery.md

use std::io;
use std::path::PathBuf;

use crate::manifest::Manifest;
use crate::runtime::{FileHandle, Fs, OpenOpts};
use crate::scanner::{self, ScanStop, SegmentHeaderInfo};
use crate::sealer::{self, SegmentCatalogEntry};

/// One segment file to recover: its `segment_id` (the sort/stitch key) and its
/// path. The caller enumerates these — segment-file naming lives *above* the
/// [`Fs`] seam (which deliberately has no `read_dir`), so whole-log recovery
/// takes the ordered set as input rather than discovering it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFile {
    pub segment_id: u64,
    pub path:       PathBuf,
}

impl SegmentFile {
    pub fn new(segment_id: u64, path: impl Into<PathBuf>) -> Self {
        SegmentFile { segment_id, path: path.into() }
    }
}

/// Which recovery authority to run (§8.2 vs §8.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMode {
    /// Scan and CRC-validate every batch of every segment (§8.2, D1 authority).
    Full,
    /// Trust sealed segments via their footer/manifest; scan only the unsealed
    /// active tail (§8.3, R2 fast path).
    Fast,
}

/// How a segment's catalog was obtained — a provenance tag, **not** part of the
/// committed prefix (see [`WholeLog::prefix_eq`], which ignores it: the full
/// and fast authorities legitimately differ here while agreeing on the bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoverySource {
    /// Trusted via the sealed footer trailer or the manifest (R2), body not
    /// scanned.
    Footer,
    /// Fully scanned (§8.2) — the authority, and the only source for the
    /// unsealed active tail.
    Scan,
}

/// Options for a whole-log recovery.
#[derive(Debug, Clone, Copy)]
pub struct RecoverOptions {
    pub mode:     RecoveryMode,
    /// Run per-segment recovery on parallel worker threads (R1). MUST be
    /// `false` under deterministic simulation.
    pub parallel: bool,
}

impl RecoverOptions {
    /// The authoritative serial full scan (§8.2).
    pub fn full() -> Self {
        RecoverOptions { mode: RecoveryMode::Full, parallel: false }
    }

    /// The serial R2 fast path (§8.3).
    pub fn fast() -> Self {
        RecoverOptions { mode: RecoveryMode::Fast, parallel: false }
    }

    /// This mode, with per-segment parallelism enabled (R1).
    pub fn parallel(mut self) -> Self {
        self.parallel = true;
        self
    }
}

/// The per-segment summary that composes the committed prefix (§1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReport {
    pub segment_id:  u64,
    /// A1 seed: the global position of this segment's first event.
    pub base_pos:    u64,
    /// A9 generation.
    pub epoch:       u64,
    /// `base_pos + accepted events`: the A1 seed handed to the next segment.
    pub end_pos:     u64,
    pub batch_count: u64,
    pub event_count: u64,
    /// Byte offset of the first byte past the accepted content — the
    /// truncation/resume point for the active segment (§1). For a footer-
    /// trusted sealed segment this is `ext_offset` (the first byte after the
    /// last `CommitMarker`), which a full scan of the same segment also
    /// reports (it stops at the footer magic there).
    pub safe_offset: u64,
    pub source:      RecoverySource,
}

/// The outcome of recovering a whole log: the committed prefix (§1) as a list
/// of per-segment reports plus the writer's resume state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WholeLog {
    /// The live segments in `segment_id` order, up to (and including) the
    /// active tail. Segment files beyond the first cross-segment hole are dead
    /// space (A10) and are absent here.
    pub segments:          Vec<SegmentReport>,
    /// `next_global_pos`: the A1 position the next append will stamp (§1).
    pub next_pos:          u64,
    /// The current `epoch` — the last live segment's generation (§1). The next
    /// segment MUST carry a strictly larger one (A9). `0` for an empty log.
    pub next_epoch:        u64,
    /// `next_batch_id`: the per-segment id the next append stamps *into the
    /// active segment*. `0` when the last live segment is sealed (the next
    /// append opens a fresh segment) or the log is empty (§1, D-FMT-5).
    pub next_batch_id:     u64,
    /// The active (unsealed) segment the writer resumes into, if any. `None`
    /// when every live segment is sealed (a new segment will be opened) or the
    /// log is empty.
    pub active_segment_id: Option<u64>,
    /// Total accepted batches across the committed prefix.
    pub total_batches:     u64,
    /// Total accepted events across the committed prefix.
    pub total_events:      u64,
}

impl WholeLog {
    /// The committed-prefix equivalence relation of §8.4: two recoveries agree
    /// iff they accept the same batches at the same positions with the same
    /// resume state — **ignoring** each report's [`RecoverySource`], which is
    /// provenance (full scanned vs footer-trusted), not committed data. This is
    /// exactly the full-recovery / fast-recovery equivalence the conformance
    /// bar requires.
    pub fn prefix_eq(&self, other: &WholeLog) -> bool {
        self.next_pos == other.next_pos
            && self.next_epoch == other.next_epoch
            && self.next_batch_id == other.next_batch_id
            && self.active_segment_id == other.active_segment_id
            && self.total_batches == other.total_batches
            && self.total_events == other.total_events
            && self.segments.len() == other.segments.len()
            && self.segments.iter().zip(&other.segments).all(|(a, b)| {
                a.segment_id == b.segment_id
                    && a.base_pos == b.base_pos
                    && a.epoch == b.epoch
                    && a.end_pos == b.end_pos
                    && a.batch_count == b.batch_count
                    && a.event_count == b.event_count
                    && a.safe_offset == b.safe_offset
            })
    }
}

/// A cross-segment stitch failure (§8.1/§8.2 R1): a segment validated
/// internally but broke the whole-log chain. Recovery MUST fail rather than
/// silently accept a prefix with a hole punched through it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StitchError {
    /// A1 across the boundary: segment `at_segment_id`'s `base_pos` does not
    /// continue its predecessor's `end_pos` (§8.1). The log is broken.
    #[error(
        "segment {at_segment_id}: base_pos {found_base_pos} does not continue \
         predecessor end_pos {expected_base_pos} (A1 cross-segment)"
    )]
    PositionGap {
        at_segment_id:     u64,
        expected_base_pos: u64,
        found_base_pos:    u64,
    },
    /// A9 across the boundary: segment `at_segment_id`'s `epoch` is not
    /// strictly greater than its predecessor's — a stale or out-of-order
    /// generation (§8.1). The log is broken.
    #[error(
        "segment {at_segment_id}: epoch {found_epoch} does not exceed \
         predecessor epoch {prev_epoch} (A9 chain)"
    )]
    EpochChainBroken {
        at_segment_id: u64,
        prev_epoch:    u64,
        found_epoch:   u64,
    },
}

/// A whole-log recovery failure: either an I/O error reaching a segment, or a
/// cross-segment [`StitchError`].
#[derive(Debug, thiserror::Error)]
pub enum RecoverError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("stitch: {0}")]
    Stitch(#[from] StitchError),
}

/// The normalized per-segment recovery result the stitch consumes. Internal:
/// it carries everything the stitch and the resume state need, from whichever
/// authority produced it.
struct SegRec {
    segment_id:    u64,
    /// `None` iff the segment's own `SegmentHeader` did not validate — the
    /// segment holds no committed batches of any generation (§8.3 posture).
    header:        Option<SegmentHeaderInfo>,
    end_pos:       u64,
    batch_count:   u64,
    event_count:   u64,
    safe_offset:   u64,
    next_batch_id: u64,
    source:        RecoverySource,
    /// Whether the segment is *complete*: its whole batch region was accepted,
    /// ending in a clean end-of-segment or at a `SegmentFooter` (sealed —
    /// trusted, or present-but-corrupt). A complete segment may legitimately
    /// be followed by another. `false` means a genuine torn tail (the scan
    /// stopped mid-body on a fault): an incomplete segment is a hole
    /// (A10), so nothing after it in the log is committed. Always `true`
    /// for a footer-trusted (immutable) segment.
    complete:      bool,
}

/// Recover a whole log: recover each segment (per `opts.mode`, optionally in
/// parallel per `opts.parallel`), then stitch them in `segment_id` order
/// enforcing the §8.1 cross-segment chain. A `manifest`, when supplied and
/// [`RecoveryMode::Fast`], seeds sealed segments without a per-segment trailer
/// `pread` — but only where the cached entry stays coherent with the segment's
/// own header; any mismatch falls back to reading the trailer, then to a scan
/// (R2: the manifest is advisory and never authoritative).
///
/// The `segments` slice need not be pre-sorted; it is stitched in ascending
/// `segment_id` order regardless.
pub fn recover_whole_log<F: Fs + Sync>(
    fs: &F,
    segments: &[SegmentFile],
    manifest: Option<&Manifest>,
    opts: RecoverOptions,
) -> Result<WholeLog, RecoverError> {
    // Stitch in segment_id order; do not mutate the caller's slice.
    let mut order: Vec<usize> = (0..segments.len()).collect();
    order.sort_by_key(|&i| segments[i].segment_id);
    let ordered: Vec<&SegmentFile> =
        order.iter().map(|&i| &segments[i]).collect();

    let recs = recover_each(fs, &ordered, manifest, opts)?;
    stitch(recs)
}

/// Recover every segment, serially or on a parallel worker fan-out (R1). The
/// map is pure per segment; only the schedule differs, so the stitched result
/// is identical either way.
fn recover_each<F: Fs + Sync>(
    fs: &F,
    ordered: &[&SegmentFile],
    manifest: Option<&Manifest>,
    opts: RecoverOptions,
) -> io::Result<Vec<SegRec>> {
    if opts.parallel {
        // R1: one worker per segment over the Fs seam. `F: Sync` lets every
        // worker share `&fs`; each returns owned data, and the stitch that
        // consumes them is serial and pure — the threads change wall time, not
        // the result. Excluded from the sim/miri lanes (real threads).
        std::thread::scope(|scope| {
            let handles: Vec<_> = ordered
                .iter()
                .map(|seg| {
                    let entry =
                        manifest.and_then(|m| m.get(seg.segment_id)).copied();
                    scope.spawn(move || {
                        resolve_segment(fs, seg, entry.as_ref(), opts.mode)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("recovery worker panicked"))
                .collect()
        })
    } else {
        ordered
            .iter()
            .map(|seg| {
                let entry = manifest.and_then(|m| m.get(seg.segment_id));
                resolve_segment(fs, seg, entry, opts.mode)
            })
            .collect()
    }
}

/// Recover a single segment into the normalized [`SegRec`], honoring the mode.
///
/// - **Full**: always scan the body (§8.2, the authority). Classification as
///   sealed/complete vs an unsealed tail comes from the scan's own stop reason
///   plus a footer cross-check, so a validly sealed segment (whose body scan
///   stops at the footer magic) is *complete*, not a hole.
/// - **Fast**: trust the manifest entry (if coherent with the header), else the
///   footer trailer (if coherent), else fall back to a full scan of the tail.
fn resolve_segment<F: Fs>(
    fs: &F,
    seg: &SegmentFile,
    manifest_entry: Option<&SegmentCatalogEntry>,
    mode: RecoveryMode,
) -> io::Result<SegRec> {
    let header = sealer::read_segment_header(fs, &seg.path)?;

    match mode {
        RecoveryMode::Fast => {
            // R2 fast path. Prefer the manifest (no trailer pread), then the
            // trailer, then a scan — each guarded by a header cross-check so a
            // stale/corrupt seed can only cost work, never mislead (§8.3).
            if let Some(hdr) = header {
                if let Some(cat) = manifest_entry
                    && catalog_coherent_with_header(cat, &hdr)
                {
                    return Ok(sealed_rec(
                        seg.segment_id,
                        hdr,
                        *cat,
                        RecoverySource::Footer,
                    ));
                }
                if let Some(cat) = sealer::read_trailer(fs, &seg.path)?
                    && catalog_coherent_with_header(&cat, &hdr)
                {
                    return Ok(sealed_rec(
                        seg.segment_id,
                        hdr,
                        cat,
                        RecoverySource::Footer,
                    ));
                }
            }
            // Unsealed / torn / no coherent seed: scan the tail (the
            // authority).
            let rec = scanner::recover_segment(fs, &seg.path)?;
            scanned_rec(fs, &seg.path, seg.segment_id, &rec)
        }
        RecoveryMode::Full => {
            // §8.2: scan the body authoritatively regardless of sealing.
            let rec = scanner::recover_segment(fs, &seg.path)?;
            // Classify a coherent seal: its body scan stops at the footer magic
            // (`BadMagic` at `content_len`), so it is *complete*, not a hole.
            // Consult the trailer only to decide provenance/completeness (never
            // to seed counts — those come from the scan) and cross-check its
            // counts against the scan (§8.2: "re-derive the counts by scanning
            // and cross-check them against both manifest and trailer").
            let sealed = header.is_some()
                && sealer::read_trailer(fs, &seg.path)?
                    .filter(|cat| {
                        catalog_coherent_with_header(
                            cat,
                            header.as_ref().unwrap(),
                        )
                    })
                    .map(|cat| {
                        cat.batch_count == rec.accepted.len() as u64
                            && cat.end_pos == rec.next_pos
                    })
                    .unwrap_or(false);
            let mut out = scanned_rec(fs, &seg.path, seg.segment_id, &rec)?;
            if sealed {
                // Tag it `Footer` — the same provenance the fast path records —
                // so the two authorities agree tag-for-tag and the manifest can
                // be rebuilt from a Full recovery too. Counts stay the
                // *scanned* (authoritative) ones, which equal
                // the trailer's (cross-checked above).
                out.source = RecoverySource::Footer;
                out.complete = true;
            }
            Ok(out)
        }
    }
}

/// Whether a cached/footer catalog entry belongs to *this* segment header
/// (§3.3.1 cross-check, mirrored from [`sealer::recover_fast`]): a mismatch
/// means the entry is stale/foreign and MUST NOT be trusted.
fn catalog_coherent_with_header(
    cat: &SegmentCatalogEntry,
    hdr: &SegmentHeaderInfo,
) -> bool {
    cat.segment_id == hdr.segment_id
        && cat.epoch == hdr.epoch
        && cat.base_pos == hdr.base_pos
        && cat.end_pos == cat.base_pos + cat.event_count
        && cat.ext_offset >= crate::format::SEGMENT_HEADER_LEN as u64
        && (cat.ext_len != 0 || cat.ext_crc == 0)
}

/// A [`SegRec`] for a footer-trusted sealed segment (R2): complete by
/// construction, counts from the trailer/manifest catalog.
fn sealed_rec(
    segment_id: u64,
    hdr: SegmentHeaderInfo,
    cat: SegmentCatalogEntry,
    source: RecoverySource,
) -> SegRec {
    SegRec {
        segment_id,
        header: Some(hdr),
        end_pos: cat.end_pos,
        batch_count: cat.batch_count,
        event_count: cat.event_count,
        safe_offset: cat.ext_offset,
        // A footer-trusted segment is sealed and immutable — a new segment is
        // opened for the next append, so its per-segment resume id is 0.
        next_batch_id: 0,
        source,
        complete: true,
    }
}

/// A [`SegRec`] from a full body scan (§8.2). Completeness is decided from the
/// scan's stop: a clean `EndOfSegment` is complete, and a stop *at a
/// `SegmentFooter` magic* (a sealed segment whose body ends at its footer,
/// whether the footer's CRC is intact or not) is also complete — only a stop
/// mid-body on a genuine fault is a torn tail (a hole, A10).
fn scanned_rec<F: Fs>(
    fs: &F,
    path: &std::path::Path,
    segment_id: u64,
    rec: &scanner::Recovery,
) -> io::Result<SegRec> {
    let complete = rec.stop == ScanStop::EndOfSegment
        || stops_at_footer(fs, path, rec.safe_offset)?;
    Ok(SegRec {
        segment_id,
        header: rec.header,
        end_pos: rec.next_pos,
        batch_count: rec.accepted.len() as u64,
        event_count: rec.next_pos
            - rec.header.map(|h| h.base_pos).unwrap_or(rec.next_pos),
        safe_offset: rec.safe_offset,
        next_batch_id: rec.next_batch_id,
        source: RecoverySource::Scan,
        complete,
    })
}

/// Whether the bytes at `safe_offset` begin a `SegmentFooter` (the
/// `FOOTER_MAGIC` tag). When a body scan stops there, the batch region ended
/// exactly at the segment's footer — the segment is a *complete* sealed segment
/// (even if the footer's own CRC is corrupt, so the trailer was not trusted).
/// This is what distinguishes "complete, seal present" from "torn mid-body"
/// without trusting the footer's contents.
fn stops_at_footer<F: Fs>(
    fs: &F,
    path: &std::path::Path,
    safe_offset: u64,
) -> io::Result<bool> {
    let file = fs.open(path, OpenOpts::read_only())?;
    let mut buf = [0u8; 4];
    let mut filled = 0usize;
    while filled < 4 {
        let n = file.pread(safe_offset + filled as u64, &mut buf[filled..])?;
        if n == 0 {
            return Ok(false); // fewer than 4 bytes remain: not a footer.
        }
        filled += n;
    }
    Ok(u32::from_le_bytes(buf) == crate::format::FOOTER_MAGIC)
}

/// Stitch per-segment recoveries into the committed prefix (§8.1), enforcing
/// cross-segment A1 (`base_pos` chain) and A9 (strictly-increasing `epoch`),
/// and stopping at the first hole (A10): a torn active tail, a segment whose
/// header did not survive, or the end of the enumerated set.
fn stitch(recs: Vec<SegRec>) -> Result<WholeLog, RecoverError> {
    let mut segments: Vec<SegmentReport> = Vec::new();
    let mut next_pos = 0u64;
    let mut next_epoch = 0u64;
    let mut next_batch_id = 0u64;
    let mut active_segment_id = None;
    let mut total_batches = 0u64;
    let mut total_events = 0u64;
    // (end_pos, epoch, complete) of the last live segment.
    let mut prev: Option<(u64, u64, bool)> = None;

    for rec in recs {
        let Some(hdr) = rec.header else {
            // The segment's header did not survive: it holds no committed
            // batches. This is the end of the live prefix (A10) — a clean
            // outcome for an empty active segment, and the stop point for a
            // vanished interior segment. Everything after is dead space.
            break;
        };

        if let Some((prev_end, prev_epoch, prev_complete)) = prev {
            // §8.1 A1: this segment's base_pos must continue the predecessor.
            if hdr.base_pos != prev_end {
                if prev_complete {
                    // Two internally-valid segments with a broken position
                    // chain (§8.2 R1): the log is broken,
                    // fail recovery.
                    return Err(StitchError::PositionGap {
                        at_segment_id:     rec.segment_id,
                        expected_base_pos: prev_end,
                        found_base_pos:    hdr.base_pos,
                    }
                    .into());
                }
                // The predecessor was a torn tail (an incomplete active
                // segment); this and every later segment file are past the hole
                // and are dead space (A10). Stop cleanly.
                break;
            }
            // §8.1 A9: the epoch chain is strictly increasing.
            if hdr.epoch <= prev_epoch {
                return Err(StitchError::EpochChainBroken {
                    at_segment_id: rec.segment_id,
                    prev_epoch,
                    found_epoch: hdr.epoch,
                }
                .into());
            }
        }

        segments.push(SegmentReport {
            segment_id:  rec.segment_id,
            base_pos:    hdr.base_pos,
            epoch:       hdr.epoch,
            end_pos:     rec.end_pos,
            batch_count: rec.batch_count,
            event_count: rec.event_count,
            safe_offset: rec.safe_offset,
            source:      rec.source,
        });
        total_batches += rec.batch_count;
        total_events += rec.event_count;
        next_pos = rec.end_pos;
        next_epoch = hdr.epoch;
        prev = Some((rec.end_pos, hdr.epoch, rec.complete));

        // Resume state: the active segment is the last live *scanned*
        // (unsealed) segment; a sealed segment resumes into a fresh
        // one.
        if rec.source == RecoverySource::Scan {
            active_segment_id = Some(rec.segment_id);
            next_batch_id = rec.next_batch_id;
        } else {
            active_segment_id = None;
            next_batch_id = 0;
        }
    }

    Ok(WholeLog {
        segments,
        next_pos,
        next_epoch,
        next_batch_id,
        active_segment_id,
        total_batches,
        total_events,
    })
}

/// Build the advisory manifest (R2) from a completed recovery: the catalog
/// entries for the sealed segments of `whole`, in `segment_id` order. A footer-
/// sourced report is a sealed segment; a scanned report is the (unsealed)
/// active tail and is intentionally omitted (only immutable sealed segments are
/// cached — the tail changes on every append). The `epoch`-and-position fields
/// come straight from the reports; the extension is Phase-3 empty. This is the
/// "rebuildable from the footers" construction the spec requires.
pub fn manifest_entries(whole: &WholeLog) -> Vec<SegmentCatalogEntry> {
    whole
        .segments
        .iter()
        .filter(|s| s.source == RecoverySource::Footer)
        .map(|s| SegmentCatalogEntry {
            // The report does not carry the trailer's flag bits, so a rebuilt
            // manifest entry is conservatively unflagged. Harmless because
            // nothing decides SealPack trust from a manifest (spec 01 §3.3.3
            // reads the trailer itself); see the `flags` note on
            // `MANIFEST_ENTRY_LEN`.
            flags:       0,
            segment_id:  s.segment_id,
            epoch:       s.epoch,
            base_pos:    s.base_pos,
            end_pos:     s.end_pos,
            batch_count: s.batch_count,
            event_count: s.event_count,
            ext_offset:  s.safe_offset,
            ext_len:     0,
            ext_crc:     0,
        })
        .collect()
}
