//! The **sealed-index candidate lifecycle** (bn-30u): what happens to a
//! sidecar that the engine finds on disk but cannot admit.
//!
//! # The problem this module closes
//!
//! A sealed sidecar (`sealed/seg-<id>.pidx` or the consolidated
//! `sealed/seg-<id>.seal`) is a **candidate** until open proves it may be
//! served. Proving it takes two independent facts:
//!
//! 1. it parses (magic, version, CRC/whole-pack hash, structural bounds), and
//! 2. the segment bytes it claims to cover are actually durable — either
//!    because the segment carries a cross-checking **footer trailer** (the
//!    footer `fsync` is what proves it), or because the recovery scan of that
//!    segment reached the candidate's coverage end; and
//! 3. (bn-11g) if that footer **names** a SealPack (spec 01 §3.3.3), this
//!    candidate is that exact pack — its own verified
//!    [`PackIdentity`](mess_index::sealed::PackIdentity) equals the named one.
//!    Facts 1 and 2 are both satisfied by a stale pack from an earlier seal of
//!    the same range, or by a pack copied in from another store; fact 3 is what
//!    makes the installed pack cryptographically specific instead of merely
//!    plausible for the segment.
//!
//! Before this bone a candidate that failed either test was simply *not
//! installed*. Nothing on disk changed. The segment was served correctly from
//! the raw log — no data loss, ever — but the same doomed candidate was
//! re-read and re-refuted on **every** reopen, and the rolled raw segment was
//! never re-queued for sealing, so the store could never regain the sealed
//! path on its own. A single crash in a ~10 ms window permanently demoted a
//! segment to raw scan.
//!
//! # The lifecycle
//!
//! Classification runs once per open, in `load_sealed` then `recover`:
//!
//! ```text
//!   ABSENT ──────a seal writes one durably (temp→fsync→rename)──────▶ CANDIDATE
//!      ▲                                                                 │
//!      │                                                     parses? ────┤ no ──▶ REFUTED (Unparsable)
//!      │                                                                 │ yes
//!      │                       footer names a pack (bn-11g)? ────────────┤
//!      │                          resolvable? ──── no ──────────────────▶├──────▶ REFUTED (PackIdentityUnresolvable)
//!      │                          matches this candidate? ── no ────────▶├──────▶ REFUTED (PackIdentityMismatch)
//!      │                                                                 │ yes / footer names none
//!      │                                        footer cross-checks? ────┤ yes ─▶ ADMITTED   (installed, trust-skipped)
//!      │                                                                 │ no
//!      │                                                            PENDING
//!      │                                                                 │
//!      │                             scan proves the coverage durable? ──┤ yes ─▶ CONFIRMED  (installed, scanned)
//!      │                                                                 │ no
//!      │                                                                 ├──────▶ REFUTED (IdentityMismatch)
//!      │                                                                 ├──────▶ REFUTED (CoverageUnproven)
//!      │                                       segment never scanned ────┴──────▶ REFUTED (Orphan)
//!      │                                                                             │
//!      └───────── QUARANTINE: rename out of the candidate namespace ◀────────────────┘
//!                 (durable: rename + directory fsync — and it leaves the
//!                  `*.refuted` marker that re-queues the segment's seal)
//! ```
//!
//! **ADMITTED** and **CONFIRMED** are installed in the
//! [`SealedStore`](mess_index::sealed::SealedStore) and served from the cold
//! tier. Everything else is **REFUTED**, and a refuted candidate takes a
//! durable, deterministic action: [`quarantine`].
//!
//! # Quarantine, not delete — and why the count is bounded
//!
//! A refuted candidate is *evidence*: either a crash landed in a window the
//! design says is survivable (fine, and the reason string says which one), or
//! the sealer has a bug (very much not fine, and the bytes are the only
//! witness). Deleting it destroys the only artifact an operator could hand
//! back. So the action is a rename out of the candidate namespace —
//! `seg-<id>.pidx` → `seg-<id>.pidx.refuted` — exactly the trick the
//! crash-atomic sidecar writer already relies on for its `*.pidx.tmp` husks:
//! after the rename the file's *extension* is no longer `pidx`/`seal`, so
//! every structural name parse in the tree (the engine's `load_sealed`, the
//! CLI's `discover_sidecars`) skips it for free, with no new "is this
//! quarantined?" predicate anywhere.
//!
//! The obvious objection to quarantine is unbounded growth. It does not apply
//! here, because the quarantine name is **derived, not unique**: there is
//! exactly one quarantine slot per (segment, artifact kind), and a second
//! refutation for the same segment renames over the first (POSIX `rename` is
//! atomic and replaces). The on-disk cost is therefore bounded by the number
//! of segments — the same order as the sidecars themselves — and can never
//! grow with the number of reopens, which is the only growth that mattered.
//! No reaper, no counter, no policy knob.
//!
//! # Idempotence and crash convergence
//!
//! Every step is a no-op when repeated, and every crash window lands in a
//! state the next open makes progress from:
//!
//! | crash point | next open sees | action |
//! |---|---|---|
//! | before quarantine | the same candidate | refute + quarantine again (identical work, no oscillation) |
//! | mid-quarantine | a partially renamed family, primary still in place | re-refute; `rename` the rest, `NotFound` on the already-moved ones is skipped |
//! | after quarantine, before the enqueue | the marker, no candidate | re-seal enqueued off the marker |
//! | after the new sidecar is durable, before the footer | a fresh footerless candidate + the marker | scan confirms it (served cold), and it is enqueued once more so the footer lands |
//! | after the footer | an admitted candidate | nothing; the marker is inert |
//!
//! The re-seal trigger is deliberately **not** the in-memory fact "a candidate
//! was refuted during this open" but the **durable quarantine marker on
//! disk**. That is what makes the middle rows converge: the quarantine erases
//! the candidate, so a rule keyed on refutation would strand the segment
//! forever if the process died between the rename and the enqueue. Keyed on
//! the marker — written, and directory-fsynced, strictly *before* the enqueue
//! — the intent survives every crash. The marker deliberately outlives the
//! repair: it is the evidence quarantine exists to preserve, and it stops
//! triggering the moment the segment is footer-verified-admitted again.
//!
//! Two segments are never re-queued, both for the same reason — the sealer's
//! finalize step writes the segment footer:
//!
//! - the **live head**, which is still being appended to (a refuted candidate
//!   over it is still quarantined; the next roll seals it normally);
//! - a segment with **no valid header or no events**, which has nothing to
//!   seal.
//!
//! A healthy store enqueues nothing at all: it has no markers.
//!
//! # What this module does NOT touch
//!
//! The raw log stays authority throughout. A missing, corrupt, or quarantined
//! candidate costs the segment its cold-tier acceleration and nothing else:
//! reads, verify, repair, and a later successful seal all run off the `.log`
//! bytes, which no step here reads, writes, or renames.

use std::path::{Path, PathBuf};
use std::{fmt, io};

/// The suffix a refuted candidate's file name grows to leave the candidate
/// namespace.
///
/// Chosen so `Path::extension()` of the renamed file is `refuted`, never
/// `pidx`/`seal` — the same structural-name-parse property that already makes
/// the crash-atomic writer's `*.pidx.tmp` husks invisible to every reader.
pub const QUARANTINE_SUFFIX: &str = ".refuted";

/// The sidecar extensions that are **primary candidates**: the artifacts open
/// classifies and, if refuted, quarantines. `.filter` / `.pcol` / `.reg` are
/// derived siblings of a `.pidx` and move with it; `.par` is parity over the
/// `.log` bytes and is deliberately left alone (it stays valid however the
/// pointer index fares).
pub const PRIMARY_EXTENSIONS: [&str; 2] = ["seal", "pidx"];

/// Siblings a legacy `.pidx` candidate owns: they are derived from the very
/// index being refuted, so they must not outlive it. A consolidated `.seal`
/// pack has none — every section lives in the one file.
const PIDX_SIBLING_EXTENSIONS: [&str; 3] = ["filter", "pcol", "reg"];

/// Why a sealed-index candidate could not be admitted.
///
/// The string forms are stable machine tokens (kebab-case, matching the
/// `mess doctor` / `verify` finding-code convention) so an operator can grep
/// for one across the metrics surface and the loud log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefutationReason {
    /// The bytes did not parse: bad magic, unknown version, CRC or whole-pack
    /// hash mismatch, a truncated/footerless image, or an out-of-range
    /// directory span.
    Unparsable,
    /// The candidate parsed but describes a different segment than the one
    /// whose `.log` it names — its `base_pos` contradicts the segment header.
    IdentityMismatch,
    /// The candidate parsed and matches its segment, but the segment's durable
    /// committed prefix stops short of the coverage end the candidate claims.
    /// The classic sidecar-before-data crash: the sidecar's `fsync` landed and
    /// the covered tail bytes did not.
    CoverageUnproven,
    /// The candidate names a segment recovery never saw — no `seg-<id>.log` at
    /// all, or one with no valid header.
    Orphan,
    /// bn-11g: the segment's footer **names** a SealPack (spec 01 §3.3.3) and
    /// this candidate is not it — a different identity, or no identity at all
    /// (a legacy `.pidx` offered where a named pack is required).
    ///
    /// This is the refutation that coverage alone could never make: a stale
    /// pack from an earlier seal of the same range, a pack copied in from
    /// another store, and any same-coverage substitute all pass the
    /// `segment_id`/`base_pos`/`end_pos` cross-check and fail here.
    PackIdentityMismatch,
    /// bn-11g: the segment's footer names a SealPack but the name cannot be
    /// resolved — the extension region failed its `ext_crc`, carried no (or
    /// more than one) `SealPackIdentity` section, named a different segment,
    /// or used an `identity_kind` this build does not know.
    ///
    /// Distinct from [`Self::PackIdentityMismatch`] because the operator
    /// action differs: a mismatch means the pack on disk is the wrong one, an
    /// unresolvable identity means the *footer* is damaged and the pack may
    /// well be fine. Both refuse to install, per §3.3.3 reader rule 2 — an
    /// unreadable identity must never be read as "no pack was named", which is
    /// the legacy coverage-only state.
    PackIdentityUnresolvable,
}

impl RefutationReason {
    /// The stable machine token for this reason.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            RefutationReason::Unparsable => "unparsable",
            RefutationReason::IdentityMismatch => "identity-mismatch",
            RefutationReason::CoverageUnproven => "coverage-unproven",
            RefutationReason::Orphan => "orphan",
            RefutationReason::PackIdentityMismatch => "pack-identity-mismatch",
            RefutationReason::PackIdentityUnresolvable => {
                "pack-identity-unresolvable"
            }
        }
    }
}

impl fmt::Display for RefutationReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One refuted candidate: which segment, why, and whether the durable
/// quarantine action actually landed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refutation {
    /// The segment the candidate claimed (or, for an unparsable candidate
    /// whose header could not be read, the id parsed from its file name).
    pub segment_id:  u64,
    /// Why it was refuted.
    pub reason:      RefutationReason,
    /// Whether the candidate was successfully renamed out of the candidate
    /// namespace. `false` means the rename failed (a read-only or full
    /// filesystem) and this candidate WILL be re-evaluated on the next
    /// reopen — still correct (the log is authority), just not yet converged.
    pub quarantined: bool,
}

impl fmt::Display for Refutation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "segment {}: {}", self.segment_id, self.reason)?;
        if !self.quarantined {
            f.write_str(" (quarantine FAILED)")?;
        }
        Ok(())
    }
}

/// What one open learned about its sealed-index candidates — the observability
/// surface for the whole lifecycle.
///
/// Built during [`LogEngine::open_with`](crate::engine::LogEngine::open_with)
/// and immutable afterwards: candidate classification happens exactly once per
/// open, before any reader exists. Read back via
/// [`LogEngine::sealed_candidate_health`](crate::engine::LogEngine::sealed_candidate_health);
/// the plain counters are also mirrored into the `Copy`
/// [`EngineMetrics`](crate::engine::EngineMetrics).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SealedCandidateHealth {
    /// Every candidate this open refuted, in classification order.
    pub refutations:      Vec<Refutation>,
    /// Segments re-queued for sealing at this open — the *pending re-seal*
    /// set. Includes segments whose candidate was refuted AND segments that
    /// simply have no admitted candidate (see the module docs on why the
    /// trigger is the segment's state, not the refutation).
    pub pending_reseal:   Vec<u64>,
    /// Candidates that could not be renamed out of the candidate namespace.
    /// Non-zero means the store is still correct but has not converged.
    pub quarantine_fails: u64,
}

impl SealedCandidateHealth {
    /// Number of candidates refuted at this open.
    #[must_use]
    pub fn refuted(&self) -> u64 { self.refutations.len() as u64 }

    /// Number of refuted candidates successfully quarantined.
    #[must_use]
    pub fn quarantined(&self) -> u64 {
        self.refutations.iter().filter(|r| r.quarantined).count() as u64
    }

    /// Number of segments re-queued for sealing at this open.
    #[must_use]
    pub fn reseals_enqueued(&self) -> u64 { self.pending_reseal.len() as u64 }

    /// The most recent refutation, rendered as `"segment N: reason"` — the
    /// single reason string an operator reads first.
    #[must_use]
    pub fn last_refutation(&self) -> Option<String> {
        self.refutations.last().map(ToString::to_string)
    }

    /// Record a refutation, performing the durable quarantine action on
    /// `primary` (the candidate file that was refuted) and logging loudly.
    ///
    /// Loud by design and unconditional: a refutation is a genuine anomaly,
    /// it happens at most once per candidate *ever* (the quarantine is what
    /// guarantees that), and an operator who never sees the line has no way to
    /// learn a seal was thrown away. There is no rate limit precisely because
    /// the quarantine already bounds the volume.
    pub(crate) fn refute(
        &mut self,
        segment_id: u64,
        reason: RefutationReason,
        primary: &Path,
    ) {
        let quarantined = match quarantine(primary) {
            Ok(_) => true,
            Err(e) => {
                self.quarantine_fails += 1;
                eprintln!(
                    "!!! mess SEALED CANDIDATE quarantine failed for segment \
                     {segment_id} ({}): {e} — {} stays on disk and will be \
                     re-evaluated at the next reopen",
                    reason,
                    primary.display(),
                );
                false
            }
        };
        let refutation = Refutation { segment_id, reason, quarantined };
        eprintln!(
            "!!! mess SEALED CANDIDATE REFUTED: {refutation} — {} not \
             installed; segment served from the raw log (authority) and \
             re-queued for a fresh seal",
            primary.display(),
        );
        self.refutations.push(refutation);
    }
}

/// The quarantine name for `path`: the same path with [`QUARANTINE_SUFFIX`]
/// appended, so its extension is no longer a candidate extension.
///
/// Appends to the whole file name rather than replacing the extension, so the
/// original kind stays legible (`seg-…​.pidx.refuted` vs `seg-…​.seal.refuted`)
/// and a `.pidx` quarantine can never collide with a `.seal` one.
#[must_use]
pub fn quarantine_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(QUARANTINE_SUFFIX);
    PathBuf::from(name)
}

/// Whether `path` is a **primary candidate** by structural name parse — the
/// same test the engine's reopen loader applies, exported so the quarantine
/// contract ("after the rename this is no longer true") is checkable.
#[must_use]
pub fn is_candidate(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| PRIMARY_EXTENSIONS.contains(&e))
}

/// The files that move together when `primary` is quarantined, **in rename
/// order**: the derived siblings first, the candidate itself last.
///
/// A consolidated `.seal` pack is self-contained, so its family is just
/// itself. This matters: a store mid-format-migration can hold both a `.seal`
/// and a `.pidx` for one segment, and quarantining a refuted `.seal` must not
/// drag away the siblings of a perfectly good `.pidx`.
///
/// # Why the primary moves last
///
/// The order is the crash-safety contract. A crash partway through leaves the
/// primary still in the candidate namespace, so the next open re-classifies
/// it, re-refutes it, and finishes the job — the *candidate* remains the
/// thing that drives cleanup. The opposite order would leave orphaned derived
/// siblings with no candidate to pull them, and a stale `.filter` re-attached
/// to a later re-seal of the same segment (identity is only cross-checked by
/// `segment_id`, which would still match) could wrongly exclude a stream.
#[must_use]
pub fn quarantine_family(primary: &Path) -> Vec<PathBuf> {
    let mut out = Vec::with_capacity(1 + PIDX_SIBLING_EXTENSIONS.len());
    if primary.extension().and_then(|e| e.to_str()) == Some("pidx") {
        out.extend(
            PIDX_SIBLING_EXTENSIONS
                .iter()
                .map(|ext| primary.with_extension(ext)),
        );
    }
    out.push(primary.to_path_buf());
    out
}

/// Rename a refuted candidate's whole family out of the candidate namespace,
/// then make the renames durable with a directory `fsync`.
///
/// Returns how many files actually moved. Idempotent in both directions:
/// a family member that is already gone (a crash between two of the renames)
/// yields `NotFound` and is skipped, and renaming onto an existing quarantine
/// slot replaces it atomically — so repeating the call after any crash
/// converges to the same on-disk state without ever growing it.
///
/// # Errors
///
/// Any rename failure other than "the source does not exist" — a read-only or
/// full filesystem, a permissions problem. The caller records the failure and
/// carries on: the store is still correct (the raw log is authority), it just
/// has not converged yet and will retry at the next open.
pub fn quarantine(primary: &Path) -> io::Result<u32> {
    let mut moved = 0;
    for path in quarantine_family(primary) {
        match std::fs::rename(&path, quarantine_path(&path)) {
            Ok(()) => moved += 1,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    // Make the directory entries durable. Best-effort for exactly the reason
    // the sidecar writer's own directory fsync is: some filesystems reject an
    // `O_RDONLY` directory fsync, and the rename's atomicity does not depend
    // on it. A lost rename simply replays the whole refutation next open.
    if moved > 0
        && let Some(parent) = primary.parent()
        && let Ok(dir) = std::fs::File::open(parent)
    {
        let _ = dir.sync_all();
    }
    Ok(moved)
}

/// The segment id encoded in a sealed-sidecar file name (`seg-<id:020>.<ext>`),
/// by structural name parse — the fallback identity for a candidate whose
/// bytes could not be parsed at all, so the refutation can still name a
/// segment. `None` when the name does not follow the scheme.
#[must_use]
pub fn segment_id_from_name(path: &Path) -> Option<u64> {
    path.file_name()
        .and_then(|n| n.to_str())?
        .strip_prefix("seg-")?
        .split('.')
        .next()?
        .parse::<u64>()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn touch(path: &Path) { std::fs::write(path, b"x").expect("write"); }

    #[test]
    fn quarantine_name_leaves_the_candidate_namespace() {
        for ext in PRIMARY_EXTENSIONS {
            let p = PathBuf::from(format!("/s/sealed/seg-{:020}.{ext}", 7));
            assert!(is_candidate(&p));
            let q = quarantine_path(&p);
            assert!(
                !is_candidate(&q),
                "{} must not parse as a candidate",
                q.display()
            );
            assert_eq!(q.extension().and_then(|e| e.to_str()), Some("refuted"));
        }
    }

    #[test]
    fn seal_and_pidx_quarantines_never_collide() {
        let seal = PathBuf::from("/s/sealed/seg-00000000000000000007.seal");
        let pidx = PathBuf::from("/s/sealed/seg-00000000000000000007.pidx");
        assert_ne!(quarantine_path(&seal), quarantine_path(&pidx));
    }

    #[test]
    fn pidx_family_includes_derived_siblings_and_never_par() {
        let pidx = PathBuf::from("/s/sealed/seg-00000000000000000007.pidx");
        let fam = quarantine_family(&pidx);
        let names: Vec<String> = fam
            .iter()
            .map(|p| {
                p.extension().unwrap().to_str().unwrap().to_ascii_lowercase()
            })
            .collect();
        assert_eq!(
            names,
            ["filter", "pcol", "reg", "pidx"],
            "siblings first, the primary candidate last (crash-safety order)"
        );
        assert!(!names.contains(&"par".to_string()), "parity stays put");
    }

    #[test]
    fn seal_pack_family_is_self_contained() {
        let seal = PathBuf::from("/s/sealed/seg-00000000000000000007.seal");
        assert_eq!(quarantine_family(&seal).len(), 1);
    }

    #[test]
    fn quarantine_moves_the_family_and_is_idempotent() {
        let d = mess_testkit::sweeping_temp_dir("cand-quarantine-family");
        let dir = d.path();
        let pidx = dir.join("seg-00000000000000000003.pidx");
        for ext in ["pidx", "filter", "pcol", "reg", "par"] {
            touch(&dir.join(format!("seg-{:020}.{ext}", 3)));
        }

        assert_eq!(quarantine(&pidx).expect("quarantine"), 4);
        for ext in ["pidx", "filter", "pcol", "reg"] {
            let p = dir.join(format!("seg-{:020}.{ext}", 3));
            assert!(!p.exists(), "{} moved", p.display());
            assert!(quarantine_path(&p).exists(), "{} landed", p.display());
        }
        // Parity is untouched: it is over the `.log` bytes, not the index.
        assert!(dir.join(format!("seg-{:020}.par", 3)).exists());

        // Repeat: every source is gone, so nothing moves and nothing errors.
        assert_eq!(quarantine(&pidx).expect("repeat"), 0);
    }

    /// Interrupted cleanup: a crash after some of the family was renamed. The
    /// next call finishes the job rather than failing on the missing ones.
    #[test]
    fn quarantine_resumes_after_a_partial_family_move() {
        let d = mess_testkit::sweeping_temp_dir("cand-quarantine-partial");
        let dir = d.path();
        let pidx = dir.join("seg-00000000000000000009.pidx");
        for ext in ["pidx", "filter", "pcol"] {
            touch(&dir.join(format!("seg-{:020}.{ext}", 9)));
        }
        // Simulate the crash: only the first sibling made it across.
        let filter = dir.join(format!("seg-{:020}.filter", 9));
        std::fs::rename(&filter, quarantine_path(&filter)).expect("partial");

        assert_eq!(quarantine(&pidx).expect("resume"), 2);
        for ext in ["pidx", "filter", "pcol"] {
            let p = dir.join(format!("seg-{:020}.{ext}", 9));
            assert!(!p.exists());
            assert!(quarantine_path(&p).exists());
        }
    }

    /// A second refutation for the same segment renames ONTO the first
    /// quarantine slot: the on-disk count is bounded by segment count and can
    /// never grow with the number of reopens.
    #[test]
    fn repeat_refutation_replaces_the_quarantine_slot() {
        let d = mess_testkit::sweeping_temp_dir("cand-quarantine-bounded");
        let dir = d.path();
        let pidx = dir.join("seg-00000000000000000001.pidx");

        for round in 0..5u8 {
            std::fs::write(&pidx, [round]).expect("write candidate");
            assert_eq!(quarantine(&pidx).expect("quarantine"), 1);
            let entries: Vec<_> = std::fs::read_dir(dir)
                .expect("read_dir")
                .flatten()
                .map(|e| e.file_name())
                .collect();
            assert_eq!(
                entries.len(),
                1,
                "exactly one quarantine slot per (segment, kind), round \
                 {round}: {entries:?}"
            );
        }
        assert_eq!(
            std::fs::read(quarantine_path(&pidx)).expect("read"),
            vec![4u8],
            "the newest refuted bytes win the slot"
        );
    }

    #[test]
    fn segment_id_parses_from_the_structural_name() {
        assert_eq!(
            segment_id_from_name(Path::new(
                "/s/sealed/seg-00000000000000000042.pidx"
            )),
            Some(42)
        );
        assert_eq!(
            segment_id_from_name(Path::new(
                "/s/sealed/seg-00000000000000000042.pidx.refuted"
            )),
            Some(42)
        );
        assert_eq!(segment_id_from_name(Path::new("/s/sealed/junk")), None);
        assert_eq!(
            segment_id_from_name(Path::new("/s/sealed/seg-x.pidx")),
            None
        );
    }

    #[test]
    fn health_summarises_reason_and_pending_reseal() {
        let mut h = SealedCandidateHealth::default();
        assert_eq!(h.last_refutation(), None);
        h.refutations.push(Refutation {
            segment_id:  2,
            reason:      RefutationReason::CoverageUnproven,
            quarantined: true,
        });
        h.refutations.push(Refutation {
            segment_id:  5,
            reason:      RefutationReason::Unparsable,
            quarantined: false,
        });
        h.pending_reseal.push(2);
        assert_eq!(h.refuted(), 2);
        assert_eq!(h.quarantined(), 1);
        assert_eq!(h.reseals_enqueued(), 1);
        assert_eq!(
            h.last_refutation().as_deref(),
            Some("segment 5: unparsable (quarantine FAILED)")
        );
    }

    #[test]
    fn reason_tokens_are_stable() {
        assert_eq!(RefutationReason::Unparsable.as_str(), "unparsable");
        assert_eq!(
            RefutationReason::IdentityMismatch.as_str(),
            "identity-mismatch"
        );
        assert_eq!(
            RefutationReason::CoverageUnproven.as_str(),
            "coverage-unproven"
        );
        assert_eq!(RefutationReason::Orphan.as_str(), "orphan");
    }
}
