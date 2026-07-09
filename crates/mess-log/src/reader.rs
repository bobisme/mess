//! Reader coordination: serving the committed prefix of the **active**
//! segment without ever reaching into in-flight bytes.
//!
//! The committer ([`crate::committer`]) advances one number, the
//! position-ordered durable watermark ([`crate::watermark`], D7,
//! `docs/spec/03-durability.md` §3). This module is the read side of that
//! number: it turns the watermark into something a reader can consume
//! safely while the writer is actively appending.
//!
//! # The boundary this enforces (D7 / W1)
//!
//! `docs/spec/06-subscriptions.md` names two data sources for a
//! subscription; **history** (paged `read_from`) is the *authoritative*
//! one, and its defining property (§3) is that it serves position `p` only
//! once the durable watermark has reached `p` — "nothing unacknowledged is
//! ever visible through it." W1 (§4) is the writer-side mirror: the
//! watermark is advanced past `p` **before** `p` is offered anywhere. A
//! reader that scanned the raw segment tail would violate the history
//! side of that contract: the tail can hold bytes that are written — even
//! written *and* fully valid — but not yet watermark-covered (a `Group`
//! window whose barrier has returned but whose §2.1-step-5 advance has not
//! run yet; or, under `Process`, bytes with no barrier behind them at
//! all). Serving those would let a subscriber confirm through "history" a
//! position it must not yet see.
//!
//! [`ReadView`] closes that gap by construction: every read **snapshots
//! the watermark first**, then clamps the scanned batches to it. Snapshot
//! order matters and is the TOCTOU-relevant choice — because `advance` is
//! monotone and only runs after a batch's barrier returns
//! (`03-durability.md` §3), every position below the snapshot is already
//! durable and therefore fully written and readable, so a scan taken *at
//! or after* the snapshot is guaranteed to recover all of them; and any
//! batch the scan additionally finds on disk but that lies at or past the
//! snapshot is dropped, never served. The result is exactly the committed
//! prefix `[base_pos, watermark)` — no gaps (the durable prefix is dense,
//! A1) and nothing past the line.
//!
//! # Relationship to recovery
//!
//! The clamp reuses the recovery scanner ([`crate::scanner`]) wholesale
//! rather than re-deriving batch framing/CRC/A-rule acceptance: the
//! scanner already yields the byte-valid, position-contiguous, current-
//! epoch prefix and already stops dead at the first torn/absent batch in
//! the in-flight tail (its 26-case adversarial suite is the evidence).
//! Recovery answers "what is on disk and structurally committed"; the
//! watermark clamp answers the *stricter* "what is additionally
//! **durable-acknowledged** right now" — the two agree on everything below
//! the watermark and the clamp discards the rest.

use std::io;
use std::path::{Path, PathBuf};

use crate::format::SEGMENT_HEADER_LEN;
use crate::runtime::Fs;
use crate::scanner::{recover_segment, AcceptedBatch};
use crate::watermark::Watermark;

/// The committed prefix of a segment as of one watermark snapshot: exactly
/// the batches whose positions all lie strictly below [`watermark`], in
/// position order, plus the byte length that prefix occupies.
///
/// [`watermark`]: CommittedPrefix::watermark
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedPrefix {
    /// The exclusive durable end this prefix was clamped to — the watermark
    /// value snapshotted at the start of the read. Every served position is
    /// `< watermark`; nothing at or past it is included.
    pub watermark: u64,
    /// The segment's base position (`SegmentHeader.base_pos`), i.e. the
    /// position of this prefix's first event. `0` for a fresh segment.
    pub base_pos: u64,
    /// The clamped batches, in on-disk (== position) order. Dense: batch
    /// `i+1` begins exactly where batch `i` ends.
    pub batches: Vec<AcceptedBatch>,
    /// Byte length of the durable prefix: the offset one past the last
    /// served batch (the segment-header length when the prefix is empty).
    /// A caller wanting a bounded `pread` of only committed bytes reads
    /// `[0, durable_len)` and never touches an in-flight byte.
    pub durable_len: u64,
}

impl CommittedPrefix {
    /// The position following the last served event: `base_pos` plus the
    /// events in this prefix. Equal to [`watermark`](CommittedPrefix::watermark)
    /// whenever the prefix reaches the watermark (the normal case: the
    /// watermark always sits on a batch boundary), and equal to `base_pos`
    /// when empty.
    pub fn next_pos(&self) -> u64 {
        self.batches
            .last()
            .map(|b| b.first_global_pos + u64::from(b.frame_count))
            .unwrap_or(self.base_pos)
    }

    /// The number of events (not batches) in the committed prefix.
    pub fn event_count(&self) -> u64 {
        self.next_pos() - self.base_pos
    }

    /// The number of batches in the committed prefix.
    pub fn len(&self) -> usize {
        self.batches.len()
    }

    /// Whether the committed prefix carries no batches.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }
}

/// A cheap-to-clone read handle over one active segment, bounded by the
/// committer's durable watermark. Every read snapshots the watermark and
/// serves exactly the committed prefix below it (see the module docs);
/// clones share the same underlying watermark, so all readers observe one
/// monotone boundary.
///
/// Generic over the filesystem `F` (the same seam the writer and scanner
/// use), so it is driven identically by the real fs and the sim
/// fault-injecting fs.
#[derive(Clone)]
pub struct ReadView<F: Fs> {
    fs: F,
    path: PathBuf,
    watermark: Watermark,
}

impl<F: Fs> ReadView<F> {
    /// A view over the segment at `path`, gated by `watermark` (obtain it
    /// from [`crate::committer::Committer::watermark`] /
    /// [`crate::committer::Appender::watermark`]).
    pub fn new(fs: F, path: impl Into<PathBuf>, watermark: Watermark) -> Self {
        ReadView { fs, path: path.into(), watermark }
    }

    /// A clone of the underlying durable watermark — for callers that want
    /// to [`await_past`](Watermark::await_past) a position directly.
    pub fn watermark(&self) -> Watermark {
        self.watermark.clone()
    }

    /// The current durable end (exclusive): the watermark's value now.
    pub fn durable_end(&self) -> u64 {
        self.watermark.get()
    }

    /// Snapshot the durable watermark and return the committed prefix below
    /// it. Never returns a batch that reaches into in-flight (at-or-past-
    /// watermark) bytes, regardless of what the writer is doing concurrently.
    pub fn read_committed(&self) -> io::Result<CommittedPrefix> {
        // Snapshot FIRST (see module docs: this ordering is what makes the
        // clamp both complete below the line and safe at it).
        let target = self.watermark.get();
        self.read_clamped_to(target)
    }

    /// Await position `position` becoming durable, then serve the committed
    /// prefix — the catch-up/read step of a D11 subscription
    /// (`docs/spec/06-subscriptions.md`): a subscriber blocks until the
    /// watermark passes `position`, then reads the now-larger committed
    /// prefix. The returned prefix's watermark is `> position`.
    pub async fn read_past(&self, position: u64) -> io::Result<CommittedPrefix> {
        self.watermark.await_past(position).await;
        self.read_committed()
    }

    /// The clamp itself, factored out so `target` is fixed for the whole
    /// scan: recover the on-disk prefix, then keep only the batches wholly
    /// below `target`.
    fn read_clamped_to(&self, target: u64) -> io::Result<CommittedPrefix> {
        let rec = recover_segment(&self.fs, &self.path)?;
        let base_pos = rec.header.map_or(0, |h| h.base_pos);
        let mut batches = Vec::new();
        // Empty prefix ends where the batches would start: after the header.
        let mut durable_len = SEGMENT_HEADER_LEN as u64;
        for b in rec.accepted {
            let end_pos = b.first_global_pos + u64::from(b.frame_count);
            if end_pos <= target {
                durable_len = b.offset + b.total_len;
                batches.push(b);
            } else {
                // The watermark sits on a batch boundary, so the first batch
                // that is not wholly below it starts exactly at `target`;
                // everything after is dead/in-flight. Stop — never serve a
                // batch that straddles or exceeds the durable end.
                break;
            }
        }
        Ok(CommittedPrefix { watermark: target, base_pos, batches, durable_len })
    }
}

impl<F: Fs> ReadView<F> {
    /// The segment path this view reads.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::committer::{
        AppendOutcome, AppendRequest, Committer, Durability, EventInput,
    };
    use crate::runtime::{RealRuntime, Runtime, SimRuntime};
    use crate::writer::{SegmentParams, SegmentWriter};
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    // Batch `n_events` events for `stream` starting at stream-version
    // `version`; payloads are irrelevant to reader coordination.
    fn req(stream: u64, version: u64, n_events: usize) -> AppendRequest {
        AppendRequest {
            stream_id: stream,
            category_id: 0,
            first_stream_version: version,
            events: (0..n_events)
                .map(|i| EventInput::plain(1, 1, 0, vec![(i as u8) ^ 0x5A; 12]))
                .collect(),
        }
    }

    // The per-writer batch plan used by the concurrent tests: writer `w`
    // appends stream `w`, three batches of 1, then 2, then 3 events, at
    // stream-versions 0, 1, 3. 4 writers => 24 events over [0, 24).
    const N_WRITERS: u64 = 4;
    const BATCH_SIZES: [usize; 3] = [1, 2, 3];
    const EVENTS_PER_WRITER: u64 = 6; // 1 + 2 + 3
    const TOTAL_EVENTS: u64 = N_WRITERS * EVENTS_PER_WRITER; // 24

    // Every (stream_id, first_stream_version, frame_count) the plan submits;
    // a served batch MUST be one of these, and a stream's served versions
    // MUST be a prefix of its plan.
    fn expected_batches() -> BTreeMap<(u64, u64), u32> {
        let mut m = BTreeMap::new();
        for w in 0..N_WRITERS {
            let mut v = 0u64;
            for &n in &BATCH_SIZES {
                m.insert((w, v), n as u32);
                v += n as u64;
            }
        }
        m
    }

    // Assert a snapshot is exactly the committed prefix [base, watermark):
    // dense, position-ordered, clamped to the watermark, and every batch a
    // genuine plan batch with monotone per-stream versions.
    fn assert_prefix_is_committed(p: &CommittedPrefix, plan: &BTreeMap<(u64, u64), u32>) {
        assert_eq!(p.base_pos, 0, "test segments seed at base_pos 0");
        // Never serve at or past the durable end.
        assert!(
            p.next_pos() <= p.watermark,
            "served past the watermark: next_pos={} watermark={}",
            p.next_pos(),
            p.watermark,
        );
        // The watermark sits on a batch boundary, so a snapshot recovers the
        // whole prefix up to it: served end == watermark exactly.
        assert_eq!(
            p.next_pos(),
            p.watermark,
            "committed prefix must reach the watermark exactly",
        );

        // Density + per-stream version contiguity + membership in the plan.
        let mut expected_pos = p.base_pos;
        let mut stream_next_ver: BTreeMap<u64, u64> = BTreeMap::new();
        for b in &p.batches {
            assert_eq!(
                b.first_global_pos, expected_pos,
                "positions must be dense/contiguous",
            );
            let frames = *plan
                .get(&(b.stream_id, b.first_stream_version))
                .unwrap_or_else(|| {
                    panic!(
                        "served a batch not in the plan: stream={} ver={}",
                        b.stream_id, b.first_stream_version,
                    )
                });
            assert_eq!(b.frame_count, frames, "frame count must match the plan");
            let want_ver = stream_next_ver.entry(b.stream_id).or_insert(0);
            assert_eq!(
                b.first_stream_version, *want_ver,
                "a stream's served versions must be a contiguous prefix",
            );
            *want_ver += u64::from(b.frame_count);
            expected_pos += u64::from(b.frame_count);
        }
        assert_eq!(expected_pos, p.next_pos());
        // durable_len is the byte end of the last served batch.
        if let Some(last) = p.batches.last() {
            assert_eq!(p.durable_len, last.offset + last.total_len);
        } else {
            assert_eq!(p.durable_len, SEGMENT_HEADER_LEN as u64);
        }
    }

    // -- Sim: a sequential baseline ---------------------------------------

    #[test]
    fn read_committed_serves_the_full_sequential_log() {
        let rt = SimRuntime::new(1);
        let fs = rt.fs();
        let path = Path::new("/seg-seq");
        let writer = SegmentWriter::create(&fs, path, SegmentParams::new(0, 0, 1, 0)).unwrap();
        let prefix = rt.block_on(async {
            let c = Committer::spawn(&rt, writer, Durability::Os);
            let view = ReadView::new(fs.clone(), path, c.watermark());
            // Read after each append: the prefix grows by exactly the batch.
            let mut last_len = 0u64;
            for (v, n) in [(0u64, 3usize), (3, 5), (8, 2)] {
                let out = c.append(req(1, v, n)).await.unwrap();
                assert!(matches!(out, AppendOutcome::Acked { .. }));
                let p = view.read_committed().unwrap();
                assert_eq!(p.next_pos(), p.watermark);
                assert!(p.event_count() > last_len);
                last_len = p.event_count();
            }
            let p = view.read_committed().unwrap();
            c.shutdown().await;
            p
        });
        assert_eq!(prefix.event_count(), 10, "3+5+2 events tile [0,10)");
        assert_eq!(prefix.watermark, 10);
        assert_eq!(prefix.len(), 3);
    }

    // -- Sim: the concurrent property test (deterministic) ----------------
    //
    // Writers append concurrently while subscription-style readers wake on
    // each watermark advance (await_past) and re-read. Every snapshot every
    // reader ever observes MUST be exactly the committed prefix — dense,
    // clamped, plan-consistent — and the final read MUST be the whole log.
    // Many seeds exercise many interleavings; the sim executor makes each
    // deterministic.
    #[test]
    fn concurrent_readers_see_exactly_the_committed_prefix() {
        let plan = expected_batches();
        for seed in 0..40u64 {
            let rt = SimRuntime::new(seed);
            let fs = rt.fs();
            let path = Path::new("/seg-conc");
            let writer =
                SegmentWriter::create(&fs, path, SegmentParams::new(0, 0, 1, 0)).unwrap();

            let (max_seen, final_prefix, final_wm) = rt.block_on(async {
                let c = Committer::spawn(&rt, writer, Durability::group_default());
                let view = ReadView::new(fs.clone(), path, c.watermark());

                // Two subscription-style readers, driven by the watermark
                // (await_past), validating every snapshot they observe.
                let plan = plan.clone();
                let mut readers = Vec::new();
                for _ in 0..2u64 {
                    let view = view.clone();
                    let plan = plan.clone();
                    readers.push(rt.spawn(async move {
                        let mut max_reached = 0u64;
                        loop {
                            let p = view.read_committed().unwrap();
                            assert_prefix_is_committed(&p, &plan);
                            max_reached = max_reached.max(p.next_pos());
                            let w = p.watermark;
                            if w >= TOTAL_EVENTS {
                                break;
                            }
                            // Wake exactly when the watermark passes `w`
                            // (position `w` commits): a live subscriber's
                            // gate. Resolves immediately if already crossed.
                            view.watermark().await_past(w).await;
                        }
                        max_reached
                    }));
                }

                // Concurrent writers, one stream each.
                let mut writers = Vec::new();
                for w in 0..N_WRITERS {
                    let ap = c.appender();
                    writers.push(rt.spawn(async move {
                        let mut v = 0u64;
                        for &n in &BATCH_SIZES {
                            let out = ap.append(req(w, v, n)).await.unwrap();
                            assert!(matches!(out, AppendOutcome::Acked { .. }));
                            v += n as u64;
                        }
                    }));
                }
                for j in writers {
                    j.await;
                }
                let mut max_seen = 0u64;
                for r in readers {
                    max_seen = max_seen.max(r.await);
                }

                let final_wm = view.durable_end();
                let final_prefix = view.read_committed().unwrap();
                c.shutdown().await;
                (max_seen, final_prefix, final_wm)
            });

            assert_eq!(final_wm, TOTAL_EVENTS, "seed {seed}: all events durable");
            assert_prefix_is_committed(&final_prefix, &plan);
            assert_eq!(
                final_prefix.event_count(),
                TOTAL_EVENTS,
                "seed {seed}: final read serves the whole log",
            );
            assert_eq!(
                final_prefix.len() as u64,
                N_WRITERS * BATCH_SIZES.len() as u64,
                "seed {seed}: every planned batch is served",
            );
            // Readers observed live growth, not just the final state.
            assert!(max_seen > 0, "seed {seed}: readers observed committed events");
        }
    }

    // -- Real: concurrent smoke over the real fs + real threads -----------
    //
    // The true TOCTOU: real threads let a reader snapshot the watermark
    // while the committer is mid-flight between a barrier and its watermark
    // advance. Every snapshot must still be exactly the committed prefix.
    // fs-touching + real threads => excluded from the Miri lane.

    fn real_tmp(name: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let base = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let mut dir = base;
        dir.push(".cache");
        dir.push("mess-reader-scratch");
        std::fs::create_dir_all(&dir).unwrap();
        dir.push(format!("{}-{}-{}", std::process::id(), n, name));
        dir
    }

    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn real_concurrent_readers_never_see_past_the_watermark() {
        let plan = expected_batches();
        let path = real_tmp("reader-smoke");
        let _c = Cleanup(path.clone());

        let rt = RealRuntime::new();
        let fs = rt.fs();
        let writer = SegmentWriter::create(
            &fs,
            &path,
            SegmentParams { segment_size: 64 * 1024 * 1024, ..SegmentParams::new(0, 0, 1, 0) },
        )
        .unwrap();

        let final_prefix = rt.block_on(async {
            let c = Committer::spawn(&rt, writer, Durability::group_default());
            let view = ReadView::new(fs, path.clone(), c.watermark());

            let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let mut readers = Vec::new();
            for _ in 0..3u64 {
                let view = view.clone();
                let plan = plan.clone();
                let done = done.clone();
                readers.push(rt.spawn(async move {
                    // Free-run until the writers signal done, then one last
                    // read to catch the final prefix. Every snapshot checked.
                    loop {
                        let p = view.read_committed().unwrap();
                        assert_prefix_is_committed(&p, &plan);
                        if done.load(Ordering::Acquire) {
                            let p = view.read_committed().unwrap();
                            assert_prefix_is_committed(&p, &plan);
                            break;
                        }
                    }
                }));
            }

            let mut writers = Vec::new();
            for w in 0..N_WRITERS {
                let ap = c.appender();
                writers.push(rt.spawn(async move {
                    let mut v = 0u64;
                    for &n in &BATCH_SIZES {
                        let out = ap.append(req(w, v, n)).await.unwrap();
                        assert!(matches!(out, AppendOutcome::Acked { .. }));
                        v += n as u64;
                    }
                }));
            }
            for j in writers {
                j.await;
            }
            done.store(true, Ordering::Release);
            for r in readers {
                r.await;
            }

            let final_prefix = view.read_committed().unwrap();
            c.shutdown().await;
            final_prefix
        });

        assert_prefix_is_committed(&final_prefix, &plan);
        assert_eq!(final_prefix.event_count(), TOTAL_EVENTS);
        assert_eq!(final_prefix.watermark, TOTAL_EVENTS);
    }

    // -- Real: await_past drives a live handoff ---------------------------
    //
    // await_past exposed end-to-end: a reader parked on await_past(p) must
    // wake once the writer commits p, and the read it then takes must
    // include p. Real threads so the wake is a genuine cross-thread event.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn await_past_wakes_a_reader_when_its_position_commits() {
        let path = real_tmp("await-past");
        let _c = Cleanup(path.clone());
        let rt = RealRuntime::new();
        let fs = rt.fs();
        let writer =
            SegmentWriter::create(&fs, &path, SegmentParams::new(0, 0, 1, 0)).unwrap();

        let saw = rt.block_on(async {
            let c = Committer::spawn(&rt, writer, Durability::Os);
            let view = ReadView::new(fs, path.clone(), c.watermark());

            // Reader blocks until position 5 is durable, then reads.
            let reader = {
                let view = view.clone();
                rt.spawn(async move { view.read_past(5).await.unwrap() })
            };

            // Writer commits positions 0..8 in single-event batches; the
            // reader must wake no earlier than the commit of position 5.
            for v in 0..8u64 {
                let out = c.append(req(1, v, 1)).await.unwrap();
                assert!(matches!(out, AppendOutcome::Acked { .. }));
            }

            let p = reader.await;
            c.shutdown().await;
            p
        });

        // read_past(5) resolves only once the watermark passed position 5,
        // so the served prefix includes position 5 (next_pos > 5).
        assert!(saw.next_pos() > 5, "read_past(5) must include position 5");
        assert!(saw.watermark > 5, "watermark must have passed position 5");
    }
}
