//! `mess inspect <dir> [--segment N] [--stream ID]` — a read-only overview of
//! the segment chain, stream head versions, and the registry.

use std::collections::BTreeMap;
use std::path::Path;

use serde_json::json;

use crate::lockprobe::{self, LockState};
use crate::metaread;
use crate::report::{Finding, Report, Severity};
use crate::scan::scan_segment;
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
    pub segment: Option<u64>,
    /// Restrict the stream-head overview to one stream, by its interned
    /// numeric id (e.g. `"7"`) or its registered name (e.g. `"user-42"`).
    /// Name matching requires the metadata registry to be readable (not
    /// blocked by a live writer's lock); see the `registry.available` field.
    pub stream: Option<String>,
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
                        active_age_secs = Some(active_age_secs.map_or(age, |a| a.max(age)));
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
    report.advise(
        "metrics-scope",
        "runtime metrics (fdatasync p50/p95/p99, degradation flag, block-cache \
         hit rate, subscription lag) are in-process only, exposed by \
         LogEngine::metrics() in the writer process; `mess inspect` opens \
         read-only from a separate process and reports only offline-observable \
         shape (segment sizes/ages/counts). See docs/spec/03-durability.md §2.6.",
    );

    // Stream heads and registry — from the durable metadata store when it can
    // be opened (a live writer holds fjall's lock, so degrade to the
    // recovered-from-log heads otherwise).
    //
    // bn-1yz: the JSON schema here is LOCK-STATE-INDEPENDENT. Earlier this
    // set a top-level `registry_source` scalar and, only on success, a
    // sibling `registry` object — so `registry.stream_names` was a real
    // array when the store was free and simply ABSENT (not even `null`)
    // when a live writer held the meta lock. A reader had to branch on
    // whether the key existed at all. Now `registry` is always present with
    // the same fields; `registry.available` is the one place that state is
    // signaled, and `stream_names`/`type_names`/`snapshots` are `[]` (not
    // missing) when degraded.
    let meta = metaread::read(dir);
    let mut names: BTreeMap<u64, String> = BTreeMap::new();
    let registry = match &meta {
        Ok(facts) => {
            for (id, name) in &facts.stream_names {
                names.insert(*id, name.clone());
            }
            json!({
                "available": true,
                "source": "meta",
                "reason": null,
                "stream_names": facts.stream_names.iter()
                    .map(|(id, n)| json!({ "stream_id": id, "name": n }))
                    .collect::<Vec<_>>(),
                "type_names": facts.type_names.iter()
                    .map(|(id, n)| json!({ "event_type_id": id, "name": n }))
                    .collect::<Vec<_>>(),
                "snapshots": facts.snapshots.iter()
                    .map(|s| json!({
                        "stream_id": s.stream_id,
                        "version": s.version,
                        "fold_version": s.fold_version,
                        "covers_empty_prefix": s.covers_empty_prefix,
                    }))
                    .collect::<Vec<_>>(),
            })
        }
        Err(reason) => {
            report.advise("registry-unavailable", reason);
            json!({
                "available": false,
                "source": "unavailable",
                "reason": reason,
                "stream_names": [],
                "type_names": [],
                "snapshots": [],
            })
        }
    };
    report.set("registry", registry);
    if !opts.all_streams {
        // registry.stream_names/type_names/snapshots are just as unbounded at
        // app scale as stream_heads (bn-1yz) — one row per stream/event-type
        // ever seen. Cap all four listings together under one flag.
        report.limit_display("registry.stream_names", DEFAULT_STREAM_HEADS_LIMIT);
        report.limit_display("registry.type_names", DEFAULT_STREAM_HEADS_LIMIT);
        report.limit_display("registry.snapshots", DEFAULT_STREAM_HEADS_LIMIT);
    }

    let filter = opts.stream.as_deref().map(StreamFilter::parse);
    if let Some(StreamFilter::Name(_)) = &filter
        && meta.is_err()
    {
        report.advise(
            "stream-name-lookup-unavailable",
            "--stream named a non-numeric stream and the metadata registry \
             could not be opened (see registry.available), so no name could \
             match; pass the interned numeric stream id instead, or retry \
             once the store is free",
        );
    }

    // Sorted by head_version (position) descending, stream_id ascending as a
    // deterministic tiebreak, so a `text`/`pretty` truncation shows the most
    // active streams first — see `DEFAULT_STREAM_HEADS_LIMIT`.
    let mut heads_all: Vec<(u64, u64)> = stream_heads.into_iter().collect();
    heads_all.sort_by(|(sid_a, v_a), (sid_b, v_b)| v_b.cmp(v_a).then(sid_a.cmp(sid_b)));

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
        LockState::Unknown { reason } => json!({ "state": "unknown", "reason": reason }),
    }
}
