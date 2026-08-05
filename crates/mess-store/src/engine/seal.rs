//! The seal machinery: the background auto-roll sealer thread (`bn-1vu`) and
//! the backlog guard one seal job holds (`bn-11ba`), the on-demand
//! `seal_active`, the `SealInput` a segment's durable bytes are re-read into,
//! and the segment-footer finalize step.
//!
//! A seal is best-effort and off the append path (D5): a segment that fails to
//! seal stays durable and unsealed, served from the log and re-sealable on
//! reopen, so a failed seal never loses data.

use super::*;

/// bn-11ba: holds one unit of the background-seal backlog gauge for the
/// lifetime of one seal job. A guard rather than a bare `fetch_sub` because
/// [`LogEngine::run_roll_sealer`]'s loop body has four `continue` exits;
/// a hand-placed decrement would eventually be forgotten on a new one.
/// Saturating, so an over-counted enqueue (see `seal_queue_depth`'s
/// construction) can never wrap the gauge.
struct SealJobGuard<'a> {
    backlog: &'a AtomicUsize,
}

impl Drop for SealJobGuard<'_> {
    fn drop(&mut self) {
        let _ = self.backlog.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |d| Some(d.saturating_sub(1)),
        );
    }
}

impl LogEngine {
    /// The background auto-roll sealer loop (`bn-1vu`), run on its own thread.
    /// For each rolled (durable, unsealed) segment reported over `rx`, it
    /// **reads the rolled segment back** (bn-2ib — the durable bytes are the
    /// seal's sole source; the deleted record book used to be) to build the
    /// pointer + payload sidecars, finalizes the segment footer (writes +
    /// fsyncs the trailer), and installs the segment into the cold
    /// [`SealedStore`] — all off the append path (D5). A seal failure is
    /// best-effort: the rolled segment stays durable + unsealed and is served
    /// from the log (and re-sealable) on reopen, so a failed seal never loses
    /// data. The loop exits when the roll channel closes (the committer task
    /// dropped its [`Roller`]), draining every queued seal first.
    ///
    /// Readiness gates on the hot index's applied end **and the canonical
    /// published watermark** (bn-2ib; previously the record book's length):
    /// sealing only what is published preserves the invariant that the sealed
    /// tier never serves a position `read_global` cannot — the cold read path
    /// is not watermark-clamped, so this gate is what keeps it safe.
    ///
    /// `bn-u6o`: `seal_metrics` counts + loudly (rate-limited) logs every
    /// segment this loop gives up waiting on (see the `record_seal_skipped`
    /// call sites below) — before this bone that skip was silent, so an
    /// operator had no way to learn a segment stayed unsealed until reopen.
    /// `shutdown_deadline` is unset during live operation (each segment gets
    /// its own `spin.per_seal_budget`, ~10s by default — the original bn-1vu
    /// bound); once `Inner::drop` publishes it, it becomes a single deadline
    /// shared by every segment still queued, bounding the TOTAL shutdown drain
    /// instead of `per_seal_budget` per abandoned roll.
    #[allow(clippy::too_many_arguments)] // internal seam; each arg is a distinct shared handle
    pub(super) fn run_roll_sealer(
        rx: mpsc::Receiver<SegmentSummary>,
        driver: SealDriver,
        active: Arc<ActiveIndex>,
        published: Watermark,
        fs: EngineFs,
        dir: PathBuf,
        seal_metrics: Arc<SealMetrics>,
        shutdown_deadline: Arc<OnceLock<Instant>>,
        spin: SpinConfig,
        // bn-11ba: the live backlog gauge and its cumulative drain counter.
        // Decremented once per job, however that job ends (sealed, skipped,
        // or empty), so the gauge returns to zero on a quiescent store.
        backlog: Arc<AtomicUsize>,
        dequeued: Arc<AtomicU64>,
    ) {
        for summary in rx {
            let _job = SealJobGuard { backlog: &backlog };
            dequeued.fetch_add(1, Ordering::Relaxed);
            let base = summary.base_pos;
            let end = summary.end_pos;

            // Wait until the hot index + published watermark cover every
            // event of this segment (post-ack discipline). Under the current
            // serialised append gate this already holds by the time the roll
            // notification lands; the bounded wait keeps it robust if a
            // future append gate (bn-1s0) relaxes that ordering. A gone
            // writer can never lower either value, so this cannot deadlock.
            //
            // The per-segment deadline (`spin.per_seal_budget` out) is clamped
            // to `shutdown_deadline` when the latter is set (bn-u6o) — see the
            // fn doc.
            let per_seal_deadline = Instant::now() + spin.per_seal_budget;
            loop {
                if active.applied_end() >= end && published.get() >= end {
                    break;
                }
                let deadline = match shutdown_deadline.get() {
                    Some(&sd) => sd.min(per_seal_deadline),
                    None => per_seal_deadline,
                };
                if Instant::now() >= deadline {
                    // Give up on this seal rather than hang. The segment stays
                    // durable + unsealed (served from the log).
                    break;
                }
                std::thread::sleep(spin.poll_interval);
            }

            if active.applied_end() < end || published.get() < end {
                // bn-u6o: this is the silent-skip site the bone exists to fix —
                // count it and log loudly (rate-limited) so an operator can see
                // a segment stayed unsealed rather than discovering it only at
                // reopen.
                seal_metrics.record_seal_skipped(&format!(
                    "segment {} [{base}, {end}) never caught up \
                     (applied_end={}, published={}, shutdown_deadline={})",
                    summary.segment_id,
                    active.applied_end(),
                    published.get(),
                    shutdown_deadline.get().is_some(),
                ));
                continue; // incomplete (see the bounded wait above) — leave it unsealed
            }

            // Re-read the rolled segment: the durable bytes are the seal's
            // input — both the batch pointers (REAL offsets, so the sealed
            // sidecar's `EventPtr`s dereference) and the payloads for the
            // columnar `.pcol` sidecar (bn-zge / D6), in stored
            // (global-position) order. Verify-on-seal inside the driver
            // byte-compares the `.pcol` reassembly against these frames
            // before anything is written.
            let seg_id = summary.segment_id;
            let seg_path = segment_path(&dir, seg_id);
            let input = match seal_input_from_segment(
                &fs, &seg_path, seg_id, base, end,
            ) {
                Ok(input) => input,
                Err(why) => {
                    seal_metrics.record_seal_skipped(&format!(
                        "segment {seg_id} [{base}, {end}): {why}"
                    ));
                    continue; // unsealed + recoverable — served from the log
                }
            };
            if input.streams.is_empty() {
                continue;
            }

            // Finalize: write + fsync the fixed footer trailer (§3.3.1), making
            // the segment R2-trusted, only after the sidecars are durable.
            let seg_path_for_parity = seg_path.clone();
            let sum = summary;
            let finalize =
                move |identity| finalize_footer(&seg_path, &sum, identity);
            // Best-effort: on failure the segment stays unsealed + recoverable.
            if driver.seal(input, finalize).is_ok() {
                // bn-2za: once the footer is finalized the `.log` bytes are
                // complete — emit the RS parity sidecar over them (no-op unless
                // parity is enabled). Best-effort, like the `.filter`: a parity
                // failure never fails the seal.
                let _ =
                    driver.write_parity_sidecar(&seg_path_for_parity, seg_id);
            }
        }
    }

    /// Seal the current active segment into the cold [`SealedStore`], driving
    /// the real `mess-index` [`SealDriver`] (sidecar encode → durable write →
    /// install → evict-from-active). After this, `read_stream` for the sealed
    /// streams routes through the [`ReplaySet`] cold path.
    ///
    /// This seals the CURRENT (live head) active segment on demand, distinct
    /// from the automatic seal a live roll triggers (bn-1vu: when the head
    /// fills the committer rolls and the background sealer seals the rolled
    /// segment off the append path). It is what the sealed-replay bench and
    /// the sealed read path exercise directly.
    pub fn seal_active(&self) -> Result<(), EngineError> {
        // The live head is the highest-id segment on disk; its committed
        // prefix is re-read from the durable bytes and clamped to the
        // published watermark (bn-2ib — the deleted record book used to be
        // the payload source, and the segment was assumed to be seg 1 at
        // base 0, which broke after a live roll).
        let ids = enumerate_segment_ids(&self.inner.dir);
        let Some(&head_id) = ids.last() else {
            return Ok(());
        };
        let end = self.inner.read_watermark.get();
        let seg_path = segment_path(&self.inner.dir, head_id);
        let header =
            scanner::read_segment_header(&self.inner.rt.fs(), &seg_path)
                .map_err(|e| EngineError::SealedRead(format!("header: {e}")))?;
        let Some(header) = header else {
            return Ok(()); // an unheadered head holds nothing to seal
        };
        let input = seal_input_from_segment(
            &self.inner.rt.fs(),
            &seg_path,
            head_id,
            header.base_pos,
            end,
        )
        .map_err(EngineError::SealedRead)?;
        if input.streams.is_empty() {
            return Ok(());
        }
        let driver = SealDriver::new(
            Arc::clone(&self.inner.sealed),
            self.inner.dir.join("sealed"),
        )
        .with_metrics(Arc::clone(&self.inner.seal_metrics))
        .with_pack(self.inner.seal_pack);
        std::fs::create_dir_all(self.inner.dir.join("sealed")).map_err(
            |e| EngineError::SealedRead(format!("mkdir sealed: {e}")),
        )?;
        // The finalize step would seal the mess-log segment footer; the engine
        // keeps the segment live for continued appends, so this is a no-op here
        // (the sealed *index* sidecar is what the cold read path consumes).
        //
        // bn-11g: writing no footer means writing no accepted installation
        // record, so the pack identity is deliberately dropped here rather
        // than recorded somewhere else. A live head has no footer at all and
        // its candidate is confirmed by the recovery scan, exactly as before
        // (spec 01 §3.3.3 / D-FMT-10, last paragraph); inventing a second
        // record for it is the thing that decision explicitly rejects. The
        // segment's next real roll-seal writes a footer that DOES name its
        // pack.
        driver
            .seal(input, |_identity| Ok(()))
            .map_err(|e| EngineError::SealedRead(format!("seal: {e}")))?;
        Ok(())
    }
}

/// Build a [`SealInput`] for the segment at `seg_path` by reading its durable
/// committed prefix back (bn-2ib — review V2: the seal's sole source is the
/// rolled raw segment, not the deleted record book). Batches are clamped to
/// `[base_pos, end_pos)` (for an on-demand seal of a still-growing head
/// segment, `end_pos` is the published watermark; for a rolled segment it is
/// the roll summary's `end_pos` and the clamp admits everything). The
/// returned input carries:
///
/// - per-stream [`SealBatch`]es with the batch's **real** byte offset, so the
///   sealed pointer sidecar's [`EventPtr`]s dereference straight into the raw
///   segment;
/// - every clamped payload in stored (global-position) order, so the driver
///   emits the columnar `.pcol` sidecar (bn-zge / D6) — and its permanent
///   verify-on-seal byte-compares the reassembly against these exact frames
///   before anything is written.
///
/// `Err(String)` when the scan fails or recovers less than `end_pos` (the
/// caller leaves the segment unsealed — served from the log, losing nothing).
fn seal_input_from_segment(
    fs: &EngineFs,
    seg_path: &Path,
    segment_id: u64,
    base_pos: u64,
    end_pos: u64,
) -> Result<SealInput, String> {
    let (rec, image) = scanner::recover_segment_with_image(fs, seg_path)
        .map_err(|e| format!("re-read for seal: {e}"))?;
    if rec.header.is_none() {
        return Err("re-read for seal: no valid segment header".to_string());
    }
    if rec.next_pos < end_pos {
        return Err(format!(
            "re-read for seal recovered only [{}, {}) of [{base_pos}, \
             {end_pos})",
            base_pos, rec.next_pos
        ));
    }
    let mut order: Vec<&AcceptedBatch> = rec.accepted.iter().collect();
    order.sort_by_key(|b| b.first_global_pos);

    let mut streams: BTreeMap<u64, Vec<SealBatch>> = BTreeMap::new();
    let mut payloads: Vec<Vec<u8>> = Vec::new();
    // bn-3of: collect per-event `event_type_id` in stored order so the
    // consolidated SealPath path can emit the `EVENT_TYPE_IDS` section and cold
    // `message_type` reads never decode the raw batch. Free to gather here —
    // the frames are already decoded for the payload columns.
    let mut event_type_ids: Vec<u32> = Vec::new();
    for b in &order {
        if b.first_global_pos < base_pos || b.first_global_pos >= end_pos {
            continue;
        }
        streams.entry(b.stream_id).or_default().push(SealBatch {
            first_version:    b.first_stream_version,
            frame_count:      b.frame_count,
            first_global_pos: b.first_global_pos,
            offset:           b.offset,
        });
        let frames =
            b.frames(&image).map_err(|e| format!("seal frames: {e}"))?;
        for f in frames {
            payloads.push(f.payload.to_vec());
            event_type_ids.push(f.event_type_id);
        }
    }
    let streams: Vec<SealStream> = streams
        .into_iter()
        .map(|(stream_id, batches)| SealStream { stream_id, batches })
        .collect();
    Ok(SealInput {
        segment_id,
        base_pos,
        streams,
        payloads: None,
        event_type_ids: None,
    }
    .with_payloads(payloads)
    .with_event_type_ids(event_type_ids))
}

/// Finalize a rolled segment's footer (bn-1vu, bn-11g): write the footer at
/// `content_len` and `fsync`, so recovery's R2 fast path can trust the segment.
/// Called from the background sealer's finalize step, only after the sidecars
/// are durable — a crash before this leaves the segment unsealed (fully scanned
/// by recovery), losing nothing.
///
/// When `pack` is `Some`, this footer becomes the **accepted installation
/// record** for that exact SealPack (spec 01 §3.3.3): the extension region
/// carries a `SealPackIdentity` section naming it and the trailer sets
/// `SEAL_PACK_IDENTITY`. The driver has already made the named pack durable —
/// written, hash-verified by parse-back, `fsync`ed, renamed, parent directory
/// `fsync`ed — before handing the identity here, so this write can never name
/// bytes that are absent or partial (see `SealDriver::seal_consolidated`).
///
/// `None` writes the pre-bn-11g footer byte-for-byte (empty extension, zero
/// flags): a segment sealed without a pack names none, and its candidates keep
/// the documented coverage-only trust (D-FMT-10).
///
/// The whole footer — extension **and** trailer — is one `write_all` followed
/// by one `sync_all`, so the two coverage domains (`ext_crc`, `footer_crc`)
/// become durable together and a torn write leaves a footer that fails
/// `footer_crc`, i.e. an unsealed segment, not a half-named one.
fn finalize_footer(
    seg_path: &Path,
    summary: &SegmentSummary,
    pack: Option<PackIdentity>,
) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    let identity = pack.map(|id| SealPackIdentity {
        identity_kind:       SEAL_PACK_IDENTITY_HDRDIR_BLAKE3,
        pack_format_version: PACK_FORMAT_VERSION,
        segment_id:          summary.segment_id,
        identity:            *id.as_bytes(),
    });
    let seal_summary = SealSummary {
        segment_id:  summary.segment_id,
        epoch:       summary.epoch,
        base_pos:    summary.base_pos,
        batch_count: summary.batch_count,
        event_count: summary.event_count,
        content_len: summary.content_len,
    };
    let (footer, _fields) =
        encode_sealed_footer(&seal_summary, &[], &[], identity.as_ref());
    let mut f = std::fs::OpenOptions::new().write(true).open(seg_path)?;
    f.seek(SeekFrom::Start(summary.content_len))?;
    f.write_all(&footer)?;
    // The trailer MUST occupy the final `SEGMENT_TRAILER_LEN` bytes (§3.3.1,
    // R2 pread-from-EOF). Before bn-11g every footer was exactly the trailer,
    // so a re-seal always overwrote the previous one exactly and the length
    // took care of itself. A footer can now SHRINK — a store re-sealed with
    // `seal_pack` turned off writes a 100-byte unnamed footer over a longer
    // named one — which would leave stale bytes past it and make the segment
    // read as unsealed (a full scan: correct, but a silent, permanent
    // regression). Truncating to exactly what was written keeps the trailer at
    // EOF for every transition, in both directions.
    f.set_len(summary.content_len + footer.len() as u64)?;
    f.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod seal_skip_tests {
    use mess_index::sealed::SealedStore;
    use mess_log::writer::SegmentSummary;

    use super::*;

    fn summary(segment_id: u64, base_pos: u64, end_pos: u64) -> SegmentSummary {
        SegmentSummary {
            segment_id,
            epoch: 1,
            base_pos,
            end_pos,
            batch_count: 1,
            event_count: end_pos - base_pos,
            content_len: 100,
        }
    }

    /// Item 1: a queued seal whose end the index/book never reaches must,
    /// once the (test-shrunk) per-segment spin bound elapses, be counted in
    /// `seals_skipped` and logged loudly — not silently dropped, which was
    /// the bn-1vu review nit this bone exists to fix.
    #[test]
    fn run_roll_sealer_counts_and_logs_a_spin_bound_skip() {
        let tmp = mess_testkit::sweeping_temp_dir("engine-src-tmp");
        let (tx, rx) = mpsc::channel();
        tx.send(summary(1, 0, 10)).expect("send");
        drop(tx); // close the channel so the loop drains this one item and exits

        let active = Arc::new(ActiveIndex::new()); // never advanced: applied_end stays 0
        let published = Watermark::new(0); // never advanced
        let sealed = Arc::new(SealedStore::new());
        let driver =
            SealDriver::new(Arc::clone(&sealed), tmp.path().join("sealed"));
        let seal_metrics = Arc::new(SealMetrics::new());
        let shutdown_deadline: Arc<OnceLock<Instant>> =
            Arc::new(OnceLock::new());
        // A tiny bound so the test does not wait out the real ~10s default.
        let spin = SpinConfig {
            per_seal_budget: Duration::from_millis(30),
            poll_interval:   Duration::from_millis(1),
        };

        let start = Instant::now();
        LogEngine::run_roll_sealer(
            rx,
            driver,
            active,
            published,
            RealRuntime::new().fs(),
            tmp.path().to_path_buf(),
            Arc::clone(&seal_metrics),
            shutdown_deadline,
            spin,
            // bn-11ba: the backlog gauge + drain counter this loop maintains.
            Arc::new(AtomicUsize::new(1)),
            Arc::new(AtomicU64::new(0)),
        );
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(1),
            "must give up promptly, took {elapsed:?}"
        );
        assert_eq!(
            seal_metrics.snapshot().seals_skipped,
            1,
            "the abandoned seal must be counted as skipped"
        );
    }

    /// Item 3: the bounded TOTAL shutdown wait `Inner::drop` arranges. Two
    /// queued seals that can never complete must both end up skipped (and
    /// counted) within ONE shared deadline — not `per_seal_budget` each —
    /// proving the fix bounds the drain's TOTAL wall time rather than only
    /// each individual seal's wait.
    #[test]
    fn shutdown_deadline_bounds_total_wait_across_every_queued_seal() {
        let tmp = mess_testkit::sweeping_temp_dir("engine-src-tmp-1");
        let (tx, rx) = mpsc::channel();
        tx.send(summary(1, 0, 10)).expect("send");
        tx.send(summary(2, 10, 20)).expect("send");
        drop(tx);

        let active = Arc::new(ActiveIndex::new());
        let published = Watermark::new(0);
        let sealed = Arc::new(SealedStore::new());
        let driver =
            SealDriver::new(Arc::clone(&sealed), tmp.path().join("sealed"));
        let seal_metrics = Arc::new(SealMetrics::new());
        let shutdown_deadline: Arc<OnceLock<Instant>> =
            Arc::new(OnceLock::new());
        // A per-seal budget far bigger than the shared shutdown budget set
        // below — proving the SHUTDOWN deadline (not the per-seal one) is
        // what bounds this run, exactly as `Inner::drop` clamps the two.
        let spin = SpinConfig {
            per_seal_budget: Duration::from_secs(30),
            poll_interval:   Duration::from_millis(1),
        };
        // Mimic `Inner::drop`: publish the shared deadline BEFORE the sealer
        // loop's wait runs (the real drop sets it, then joins the thread).
        let budget = Duration::from_millis(150);
        shutdown_deadline.set(Instant::now() + budget).expect("first set");

        let start = Instant::now();
        LogEngine::run_roll_sealer(
            rx,
            driver,
            active,
            published,
            RealRuntime::new().fs(),
            tmp.path().to_path_buf(),
            Arc::clone(&seal_metrics),
            shutdown_deadline,
            spin,
            // bn-11ba: the backlog gauge + drain counter this loop maintains.
            Arc::new(AtomicUsize::new(1)),
            Arc::new(AtomicU64::new(0)),
        );
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(1),
            "TOTAL shutdown drain must be bounded near `budget` regardless of \
             queue depth, took {elapsed:?}"
        );
        assert_eq!(
            seal_metrics.snapshot().seals_skipped,
            2,
            "both abandoned seals must be counted as skipped"
        );
    }
}
