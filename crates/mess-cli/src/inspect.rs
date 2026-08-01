//! `mess inspect <dir> [--segment N] [--stream ID]` — a read-only overview of
//! the segment chain, stream head versions, and the registry.

use std::collections::BTreeMap;
use std::path::Path;

use mess_log::footer_ext::decode_extension;
use mess_log::runtime::real::RealFs;
use mess_log::sealer::read_extension;
use serde_json::json;

use crate::lockprobe::{self, LockState};
use crate::metaread;
use crate::registryfold;
use crate::report::{Finding, Report, Severity};
use crate::scan::{SegmentScan, scan_segment};
use crate::store;

/// Default cap on how many `stream_heads` rows `text`/`pretty` render before
/// truncating with a `... and K more` line (bn-1yz: unfiltered `inspect` on
/// an app-scale store previously dumped every stream head in one shot,
/// unusable at a few hundred streams). `--format json` always carries every
/// stream head; this only bounds the human/agent-facing render.
pub const DEFAULT_STREAM_HEADS_LIMIT: usize = 20;

/// Options for [`run`].
#[derive(Debug, Default, Clone)]
pub struct InspectOptions {
    /// Restrict the segment overview to this segment id.
    pub segment:     Option<u64>,
    /// Restrict the stream-head overview to one stream, by its interned
    /// numeric id (e.g. `"7"`) or its registered name (e.g. `"user-42"`).
    /// Name matching requires the log's `$registry` to fold — which needs no
    /// lock and so works against a live writer; see `registry.available`.
    pub stream:      Option<String>,
    /// Disable the default `text`/`pretty` truncation of `stream_heads` and
    /// show every entry. No effect on `--format json`, which is always
    /// complete.
    pub all_streams: bool,
}

/// A parsed `--stream` filter: either an interned numeric id or a name to
/// resolve against the registry.
enum StreamFilter {
    Id(u64),
    Name(String),
}

impl StreamFilter {
    fn parse(s: &str) -> Self {
        match s.parse::<u64>() {
            Ok(id) => StreamFilter::Id(id),
            Err(_) => StreamFilter::Name(s.to_string()),
        }
    }

    fn matches(&self, sid: u64, name: Option<&String>) -> bool {
        match self {
            StreamFilter::Id(id) => sid == *id,
            StreamFilter::Name(n) => name.is_some_and(|resolved| resolved == n),
        }
    }
}

/// Inspect the store at `dir`. Read-only; works against a live-locked store.
pub fn run(dir: &Path, opts: &InspectOptions) -> Report {
    let mut report = Report::new("inspect", "segments");
    report.set("dir", json!(dir.display().to_string()));

    let lock = lockprobe::probe(dir);
    report.set("lock", lock_json(&lock));

    // Segment chain overview.
    let segments = store::discover_segments(dir);
    let mut stream_heads: BTreeMap<u64, u64> = BTreeMap::new();
    // Offline metrics accumulators (bn-e2y). `mess inspect` opens the store
    // read-only from a SEPARATE process, so it cannot observe another process's
    // in-process runtime counters (fsync latency, cache hit rate, subscription
    // lag) — those live behind `LogEngine::metrics()` in the writer process.
    // What IS observable offline is the on-disk shape: segment sizes, ages,
    // seal state, and durable event/batch counts. That is what this section
    // reports; see the `metrics` advisory below on the split.
    let mut total_size_bytes: u64 = 0;
    let mut total_events: u64 = 0;
    let mut total_batches: u64 = 0;
    let mut sealed_count: u64 = 0;
    let mut active_count: u64 = 0;
    let mut active_age_secs: Option<f64> = None;
    for seg in &segments {
        if let Some(want) = opts.segment
            && seg.segment_id != want
        {
            continue;
        }
        match scan_segment(seg.segment_id, &seg.log_path) {
            Ok(scan) => {
                for (&sid, &v) in &scan.recovery.stream_heads {
                    stream_heads
                        .entry(sid)
                        .and_modify(|cur| *cur = (*cur).max(v))
                        .or_insert(v);
                }
                total_size_bytes += scan.file_len;
                total_events += scan.event_count();
                total_batches += scan.batch_count() as u64;
                if scan.is_sealed() {
                    sealed_count += 1;
                } else {
                    active_count += 1;
                    // The unsealed head's age from its file mtime — the only
                    // durable age signal available offline (there is no
                    // per-segment start timestamp in the header).
                    if let Some(age) = file_age_secs(&seg.log_path) {
                        active_age_secs =
                            Some(active_age_secs.map_or(age, |a| a.max(age)));
                    }
                }
                report.push_row(json!({
                    "segment_id": seg.segment_id,
                    "epoch": scan.epoch(),
                    "base_pos": scan.base_pos(),
                    "size_bytes": scan.file_len,
                    "sealed": scan.is_sealed(),
                    "batch_count": scan.batch_count(),
                    "event_count": scan.event_count(),
                    "has_pidx": seg.has_pidx,
                    "has_pcol": seg.has_pcol,
                    "dir_codec": dir_codec(dir, seg.segment_id, &seg.pidx_path),
                    "has_seal": seg.has_seal,
                    // bn-3of dual read: which sealed artifact a reader would
                    // actually use for this segment (`seal-pack` wins over a
                    // shadowed `pidx`; `none` for the unsealed head or a
                    // segment whose candidate was quarantined). `has_pidx` /
                    // `has_seal` remain the raw presence bits.
                    "sealed_artifact": seg.sealed_artifact().as_str(),
                    // bn-11g: the SealPack identity this segment's footer
                    // NAMES (spec 01 §3.3.3), lowercase hex, or null for a
                    // legacy/unsealed footer that names none. `mess verify`
                    // is what compares it against the pack on disk; inspect
                    // reports the recorded fact so an operator can see, per
                    // segment, which footers are bound and which are still on
                    // the coverage-only compatibility policy.
                    "named_seal_pack": named_seal_pack(seg, &scan),
                    "safe_offset": scan.recovery.safe_offset,
                    "next_pos": scan.recovery.next_pos,
                }));
            }
            Err(e) => {
                report.push_finding(
                    Finding::new(
                        Severity::Error,
                        "segment-read",
                        "segment-io",
                        format!("segment {} unreadable: {e}", seg.segment_id),
                    )
                    .with("segment_id", seg.segment_id),
                );
            }
        }
    }

    // Offline metrics section (bn-e2y): the on-disk shape a separate read-only
    // process CAN observe. Runtime latency/cache/lag metrics are in-process
    // only — see the advisory below.
    report.set(
        "metrics",
        json!({
            "scope": "offline",
            "segment_count": active_count + sealed_count,
            "active_segment_count": active_count,
            "sealed_segment_count": sealed_count,
            "total_size_bytes": total_size_bytes,
            "durable_event_count": total_events,
            "durable_batch_count": total_batches,
            "active_segment_age_secs": active_age_secs,
        }),
    );
    // bn-30u: quarantined candidates (`sealed/*.refuted`). Always present (an
    // empty array on a healthy store), so the JSON schema does not change shape
    // with the store's health — the same lock-state-independence rule bn-1yz
    // established for `registry`. `inspect` DESCRIBES them; `doctor` judges
    // their re-seal state and `verify` names the refutation class.
    report.set(
        "quarantined",
        json!(
            store::discover_quarantined(dir)
                .iter()
                .map(|q| json!({
                    "segment_id": q.segment_id,
                    "artifact": q.original_ext,
                    "primary": q.is_primary,
                    "path": q.path.display().to_string(),
                }))
                .collect::<Vec<_>>()
        ),
    );

    report.advise(
        "metrics-scope",
        "runtime metrics (fdatasync p50/p95/p99, degradation flag, \
         block-cache hit rate, subscription lag) are in-process only, exposed \
         by LogEngine::metrics() in the writer process; `mess inspect` opens \
         read-only from a separate process and reports only \
         offline-observable shape (segment sizes/ages/counts). See \
         docs/spec/03-durability.md §2.6.",
    );

    // Stream heads, names, and snapshots.
    //
    // `bn-2di`: NAMES COME FROM THE LOG — the `id -> name` bijection is the
    // `$registry` stream (spec `04-registry.md`), and `registryfold` decodes it
    // straight out of the segment bytes, needing no lock.
    //
    // `bn-fj34`: and so does the SNAPSHOT half now. `metaread::read` is the
    // lock-free pack-sidecar reader and is infallible, so neither half of this
    // report degrades under a live writer any more.
    //
    // bn-1yz: the JSON schema here is LOCK-STATE-INDEPENDENT. `registry` is
    // always present with the same fields; `registry.available` signals whether
    // the registry FOLD succeeded, and `snapshots` is `[]` (not missing) when
    // the store has none.
    let folded = registryfold::fold(dir);
    let mut names: BTreeMap<u64, String> = BTreeMap::new();
    // `$registry` (stream 0) is named by SPEC TEXT, not by any record
    // (REG1/REG2 — the reserved ids are exactly what makes registry
    // bootstrap non-circular, so there is nothing upstream of them to
    // record them).
    names.insert(
        mess_store::registry::REGISTRY_STREAM_ID,
        mess_store::registry::RESERVED_STREAM_NAME.to_string(),
    );
    let (stream_names, type_names) = match &folded {
        Ok(state) => {
            let streams = registryfold::stream_names(state);
            for (id, name) in &streams {
                names.insert(*id, name.clone());
            }
            (streams, registryfold::event_type_names(state))
        }
        Err(reason) => {
            report.advise("registry-fold-failed", reason);
            (Vec::new(), Vec::new())
        }
    };

    let snapshots = metaread::read(dir)
        .snapshots
        .iter()
        .map(|s| {
            json!({
                "stream_id": s.stream_id,
                "version": s.version,
                "fold_version": s.fold_version,
                "covers_empty_prefix": s.covers_empty_prefix,
            })
        })
        .collect::<Vec<_>>();
    report.set(
        "registry",
        json!({
            "available": folded.is_ok(),
            "source": "log",
            "reason": folded.as_ref().err(),
            // bn-fj34: pinned `true`, and kept only so the JSON schema this
            // report has always emitted stays stable for existing consumers.
            // The snapshot half used to be a fallible key-value-store open
            // (locked by a live writer => `false` + a `registry-unavailable`
            // advisory); it is now the lock-free pack sidecar, whose reader
            // answers "no snapshots" instead of failing, so there is no longer
            // a state in which snapshots are *unavailable* as opposed to
            // absent. An empty `snapshots` array is the honest report of a
            // store that has none.
            "snapshots_available": true,
            "stream_names": stream_names.iter()
                .map(|(id, n)| json!({ "stream_id": id, "name": n }))
                .collect::<Vec<_>>(),
            "type_names": type_names.iter()
                .map(|(id, n)| json!({ "event_type_id": id, "name": n }))
                .collect::<Vec<_>>(),
            "snapshots": snapshots,
        }),
    );
    if !opts.all_streams {
        // registry.stream_names/type_names/snapshots are just as unbounded at
        // app scale as stream_heads (bn-1yz) — one row per stream/event-type
        // ever seen. Cap all listings together under one flag.
        report
            .limit_display("registry.stream_names", DEFAULT_STREAM_HEADS_LIMIT);
        report.limit_display("registry.type_names", DEFAULT_STREAM_HEADS_LIMIT);
        report.limit_display("registry.snapshots", DEFAULT_STREAM_HEADS_LIMIT);
    }

    let filter = opts.stream.as_deref().map(StreamFilter::parse);
    // bn-fj34: re-anchored on the registry FOLD. This advisory used to fire
    // when the metadata store could not be opened; that store is gone, and
    // since bn-2di the thing a `--stream <name>` lookup actually needs is the
    // log's own `$registry` fold. A failed fold is the one remaining state in
    // which no name can match, so that is what it reports now.
    if let Some(StreamFilter::Name(_)) = &filter
        && folded.is_err()
    {
        report.advise(
            "stream-name-lookup-unavailable",
            "--stream named a non-numeric stream and the $registry could not \
             be folded out of the log (see \
             registry.available/registry.reason), so no name could match; \
             pass the interned numeric stream id instead",
        );
    }

    // Sorted by head_version (position) descending, stream_id ascending as a
    // deterministic tiebreak, so a `text`/`pretty` truncation shows the most
    // active streams first — see `DEFAULT_STREAM_HEADS_LIMIT`.
    let mut heads_all: Vec<(u64, u64)> = stream_heads.into_iter().collect();
    heads_all.sort_by(|(sid_a, v_a), (sid_b, v_b)| {
        v_b.cmp(v_a).then(sid_a.cmp(sid_b))
    });

    let heads: Vec<_> = heads_all
        .iter()
        .filter(|(sid, _)| {
            let name = names.get(sid);
            filter.as_ref().is_none_or(|f| f.matches(*sid, name))
        })
        .map(|(sid, v)| {
            json!({
                "stream_id": sid,
                "name": names.get(sid),
                "head_version": v,
            })
        })
        .collect();
    report.set("stream_heads", json!(heads));
    if !opts.all_streams {
        report.limit_display("stream_heads", DEFAULT_STREAM_HEADS_LIMIT);
    }

    report
}

/// Which `STREAM_DIRECTORY` codec this segment's sealed artifact used
/// (bn-we9x): `"bitrank"` or `"sorted"`, or `null` when the segment has no
/// sealed artifact (or it is unreadable — `doctor` is the surface that
/// *judges* a sealed artifact; `inspect` only describes one).
///
/// The consolidated `.seal` pack wins over a legacy `.pidx` when a store
/// carries both, matching the engine's own dual-read preference. Reads only
/// the artifact's header + section directory + trailer, never a section body,
/// so this stays far cheaper than the full log scan the row already paid for.
fn dir_codec(
    dir: &Path,
    segment_id: u64,
    pidx_path: &Path,
) -> Option<&'static str> {
    let seal =
        mess_index::sealed::seal_pack_path(&dir.join("sealed"), segment_id);
    [seal.as_path(), pidx_path]
        .into_iter()
        .find_map(|p| mess_index::sealed::dir_codec_of(p).ok())
        .map(mess_index::sealed::dircodec_name)
}

/// The SealPack identity this segment's footer **names** (spec 01 §3.3.3,
/// bn-11g), as lowercase hex — or `null` when the footer names none (unsealed,
/// or the legacy coverage-only compatibility policy, D-FMT-10).
///
/// Deliberately reports only what the footer *records*, never what the pack on
/// disk hashes to: `mess inspect` is the cheap read-only survey, and confirming
/// the two agree means opening and hashing every pack, which is `mess verify`'s
/// job. A non-null value here plus a `seal-pack-*` error there is the pair that
/// tells an operator "this segment is bound, and the binding is broken".
fn named_seal_pack(
    seg: &store::SegmentFile,
    scan: &SegmentScan,
) -> serde_json::Value {
    let Some(trailer) = &scan.trailer else {
        return serde_json::Value::Null;
    };
    if !trailer.names_seal_pack() {
        return serde_json::Value::Null;
    }
    read_extension(&RealFs, &seg.log_path, trailer)
        .ok()
        .flatten()
        .and_then(|ext| decode_extension(&ext).pack_identity)
        .filter(|n| n.segment_id == trailer.segment_id)
        .map_or(serde_json::Value::Null, |n| serde_json::Value::from(n.hex()))
}

/// Seconds since a file was last modified, or `None` if the mtime is
/// unavailable (e.g. a filesystem without mtime, or a clock skew that would
/// make the age negative).
fn file_age_secs(path: &Path) -> Option<f64> {
    let modified = std::fs::metadata(path).ok()?.modified().ok()?;
    modified.elapsed().ok().map(|d| d.as_secs_f64())
}

fn lock_json(lock: &LockState) -> serde_json::Value {
    match lock {
        LockState::Free => json!({ "state": "free" }),
        LockState::Held { pid } => json!({ "state": "held", "pid": pid }),
        LockState::Unknown { reason } => {
            json!({ "state": "unknown", "reason": reason })
        }
    }
}
