//! Open and recovery (`bn-20b`, `bn-2ib`, `bn-30u`, `bn-26pp`):
//! `LogEngine::open` and `open_with` with the whole open-time wiring — the
//! sealer-thread spawn and the roll-channel re-seal enqueue — plus the on-open
//! recovery pipeline those call: `recover`'s three-step scan/fold/resolve (spec
//! 04 §7.1), the sealed-sidecar reload in `load_sealed`, the quarantine paths
//! (`refute_orphans`, `refute_unparsable`), and the bounded point-read
//! `read_batch_payloads` the `$registry` fold uses instead of a full segment
//! scan.

use super::*;

/// How [`LogEngine::recover`] resolved the active segment: continue an existing
/// one in place, or start fresh.
enum ResumePlan {
    /// No committed segment on disk (or an unheaderable one): create a fresh
    /// active segment at global position 0, epoch 1.
    Fresh,
    /// An existing active segment with a committed prefix: resume appending at
    /// its recovered `safe_offset`, continuing the A1/`batch_id` chain.
    Resume(ResumeInfo),
}

/// Everything [`LogEngine::recover`] hands back to
/// [`open_with`](LogEngine::open_with) (bn-2ib).
struct Recovered {
    /// Interners (folded out of `$registry`, or — for an unmigrated legacy
    /// store — reloaded from the meta name tables) + per-stream heads.
    book:               Book,
    /// How to resume the live head segment.
    plan:               ResumePlan,
    /// Per-stream fold-chain exit heads (chain-on stores only; spec 05 §6).
    chain_heads:        HashMap<u64, ChainHead>,
    /// The recovered durable/published canonical event count — the exclusive
    /// end of the global-position sequence, including `$registry` events that
    /// application reads filter. Seeds both the publish sequencer and the
    /// published read watermark.
    watermark:          u64,
    /// Payload frames materialised during recovery — 0 on every chain-off
    /// open (the bn-2ib gate observable,
    /// [`LogEngine::recover_payload_decodes`]).
    decodes:            u64,
    /// bn-30u: every segment recovery **scanned** (so: every segment not
    /// already served from a footer-verified sidecar) that carries a valid
    /// header and at least one event, with the roll summary a fresh seal would
    /// need — read straight off the scan that just proved those bytes durable.
    ///
    /// This is the *candidate* set, not the enqueue set:
    /// [`open_with`](LogEngine::open_with) narrows it to the segments actually
    /// owed a re-seal (a durable `*.refuted` quarantine marker, and no
    /// footer-verified sidecar) and excludes the live head, which must never
    /// be footer-finalized while it is still being appended to.
    resealable:         Vec<SegmentSummary>,
    /// bn-11ba: sealed segments whose `$registry` batches came out of an
    /// admitted registry delta (the pack's `REGISTRY_DELTA` section or the
    /// sibling `.reg`), i.e. the accelerated `O(#segments)` path.
    reg_delta_admitted: u64,
    /// bn-11ba: sealed segments that carry `$registry` batches but whose
    /// delta was absent, unreadable, or rejected by the layout cross-check,
    /// so recovery point-read the same batches through the pointer index
    /// instead. Always correct — the delta is discardable acceleration (D1) —
    /// but the `O(#names)` path, and the number an operator needs in order to
    /// know a cold open is slow *because* the accelerator was refused.
    reg_delta_fallback: u64,
}

/// A sealed-index candidate that parsed but is not yet admitted: the parsed
/// index plus the **path it came from**, which the refutation path needs in
/// order to quarantine it (bn-30u).
struct PendingCandidate {
    index: SealedSegmentRef,
    /// The primary candidate file (`sealed/seg-<id>.pidx` or `…​.seal`).
    path:  PathBuf,
}

/// Everything [`LogEngine::load_sealed`] hands back.
struct LoadedSealed {
    /// The cold tier, pre-loaded with every footer-verified sidecar.
    store:       SealedStore,
    /// Footer-verified segment ids — the ones recovery may trust-skip.
    ids:         HashSet<u64>,
    /// Parsed-but-unproven candidates, keyed by segment id; recovery either
    /// confirms (installs) or refutes (quarantines) each one.
    pending:     HashMap<u64, PendingCandidate>,
    /// bn-30u: segment ids carrying a `*.refuted` quarantine marker from an
    /// EARLIER open. The marker is the durable record that a refutation
    /// happened, and it is what makes the re-seal survive a crash between the
    /// quarantine and the enqueue.
    quarantined: HashSet<u64>,
    /// Candidates already refuted at load (they did not parse), and the
    /// running observability record the recovery pass appends to.
    health:      SealedCandidateHealth,
}

/// Recovery step 1's output (spec 04 §7.1), held until the `$registry` fold
/// (step 2) has run and names can finally be resolved (step 3) — `bn-2di`.
///
/// Before this bone the scan resolved names inline, because they came from a
/// key-value store and were already loaded. They come from the log now, so the
/// scan cannot resolve anything: it accumulates here instead.
#[derive(Default)]
struct ScanOutput {
    /// Stream-0 batches from a SCANNED segment, taken straight out of the
    /// segment image the scan already holds — `(first_global_pos, payloads)`.
    registry_batches:   Vec<(u64, Vec<Vec<u8>>)>,
    /// Stream-0 batches located WITHOUT reading their segment: one bounded
    /// `pread` each, after the scan. Only sidecar-trusted sealed segments take
    /// this path — they are never read as bytes at all (Spike C), so their
    /// `$registry` batches are resolved through the sealed per-stream pointer
    /// index and point-read individually, and a sealed segment with no
    /// registration in it costs one hash lookup and no I/O.
    registry_ptrs:      Vec<EventPtr>,
    /// Every `stream_id` the accepted log actually references. Each must
    /// resolve to a name once the fold completes, or the store cannot open.
    referenced_streams: HashSet<u64>,
    /// The largest `event_type_id` any subframe header the scan READ carries
    /// (review F3). Ids are dense, so this one value decides the whole
    /// namespace's integrity check — see
    /// [`finish_recovery`](LogEngine::finish_recovery), which also explains
    /// why sidecar-trusted sealed segments contribute nothing here and
    /// need not.
    max_event_type_id:  u32,
    /// `stream id → last stream position`.
    heads:              HashMap<u64, u64>,
}

/// The recovered resume state threaded from [`LogEngine::recover`] into
/// [`SegmentWriter::resume`].
struct ResumeInfo {
    segment_id:    u64,
    base_pos:      u64,
    epoch:         u64,
    write_off:     u64,
    next_batch_id: u64,
    next_pos:      u64,
    batch_count:   u64,
    event_count:   u64,
}

impl LogEngine {
    /// Open (creating if absent) a composed engine rooted at `dir`, default
    /// options.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, EngineError> {
        Self::open_with(dir, EngineOptions::default())
    }

    /// Open with explicit [`EngineOptions`].
    pub fn open_with(
        dir: impl AsRef<Path>,
        opts: EngineOptions,
    ) -> Result<Self, EngineError> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)
            .map_err(|e| EngineError::Open(format!("create_dir_all: {e}")))?;
        let lock = StoreLock::acquire(dir)
            .map_err(|e| EngineError::Open(format!("lock: {e}")))?;

        let rt = RealRuntime::new();

        // Reload the sealed tier from the durable sidecars written by prior
        // seals (see `load_sealed`). Without this the `SealedStore` starts
        // empty on every reopen, so a stream that was sealed before a restart
        // would silently fall back to hot replay instead of the sealed tier.
        // The returned `sealed_ids` are the segments already served cold, so
        // recovery does not re-seed the hot index with their batches (bn-1vu).
        let LoadedSealed {
            store: sealed,
            ids: sealed_ids,
            pending: pending_sidecars,
            quarantined: mut owed_reseal,
            health: mut candidate_health,
        } = Self::load_sealed(dir, &rt.fs());
        let sealed = Arc::new(sealed);

        // Recovery on open (F6 + bn-1vu + bn-2ib): reload the interners from
        // the meta name tables, re-seed the hot index + heads from a
        // batch-metadata scan of the unsealed segments (no payload frame
        // decode), take fully-sealed segments' heads/coverage straight from
        // their durable sidecars (no byte scan at all), and learn how to
        // resume the last (active) segment.
        let active = Arc::new(ActiveIndex::new());
        let recovered = Self::recover(
            &rt,
            dir,
            &active,
            &sealed,
            &sealed_ids,
            &pending_sidecars,
            &mut candidate_health,
            opts.chain,
        )?;
        let Recovered {
            book,
            plan,
            chain_heads,
            watermark,
            decodes,
            resealable,
            reg_delta_admitted,
            reg_delta_fallback,
        } = recovered;
        // The active segment is the highest-id `seg-*.log`; on a fresh store it
        // is `ACTIVE_SEGMENT_ID`. A roll numbers the next one `+1` from here.
        let active_seg_id = match &plan {
            ResumePlan::Fresh => ACTIVE_SEGMENT_ID,
            ResumePlan::Resume(info) => info.segment_id,
        };
        let seg_path = segment_path(dir, active_seg_id);
        let writer = match plan {
            ResumePlan::Fresh => {
                let params = SegmentParams {
                    segment_id:         ACTIVE_SEGMENT_ID,
                    base_pos:           0,
                    epoch:              1,
                    prev_segment_epoch: 0,
                    created_unix_nanos: 0,
                    segment_size:       opts.segment_size,
                };
                SegmentWriter::create(&rt.fs(), &seg_path, params).map_err(
                    |e| EngineError::Open(format!("segment create: {e}")),
                )?
            }
            ResumePlan::Resume(info) => {
                // Resume the existing active segment in place (no header
                // rewrite, no epoch bump): new appends extend the one
                // contiguous committed prefix from the recovered safe_offset.
                let params = ResumeParams {
                    segment_id:    info.segment_id,
                    base_pos:      info.base_pos,
                    epoch:         info.epoch,
                    segment_size:  opts.segment_size,
                    write_off:     info.write_off,
                    next_batch_id: info.next_batch_id,
                    next_pos:      info.next_pos,
                    batch_count:   info.batch_count,
                    event_count:   info.event_count,
                };
                SegmentWriter::resume(&rt.fs(), &seg_path, params).map_err(
                    |e| EngineError::Open(format!("segment resume: {e}")),
                )?
            }
        };

        // Wire live auto-roll (bn-1vu): the committer rolls to a fresh segment
        // when the active one fills and reports each rolled segment over this
        // channel; the seal thread turns it into durable sidecars + a footer
        // off the append path.
        let (roll_tx, roll_rx) = mpsc::channel::<SegmentSummary>();
        // bn-30u: the re-seal enqueue RIDES this same channel — a re-seal of a
        // rolled segment is byte-for-byte the job the sealer already does for
        // a live roll (re-read the durable prefix, write the sidecars,
        // finalize the footer, install), so there is no second scheduler, no
        // second code path, and no second set of failure semantics. The clone
        // is dropped as soon as the backlog is queued so the sealer thread
        // still exits when the committer drops its `Roller`.
        let reseal_tx = roll_tx.clone();
        let dir_for_paths = dir.to_path_buf();
        // bn-11ba: the live background-seal backlog. Incremented once per job
        // *queued* and decremented once per job the sealer thread *finishes*,
        // so a non-zero reading means the sealer is behind — the number the
        // "is background work accumulating?" question actually wants, which
        // neither `SealMetrics` (completions only) nor `SealedCandidateHealth`
        // (open-time owed set only) could answer before.
        //
        // The roll half is counted inside the `path_for` callback because
        // that is the one hook mess-store owns on the roll path: the
        // committer invokes it exactly once per roll, naming the *next*
        // segment, immediately before it reports the rolled one. The single
        // inaccuracy is a roll whose `open_next` then fails (StoreFull/EIO) —
        // an error the append itself also surfaces — which leaves the depth
        // one high until the next completion; the decrement side saturates so
        // it can never wrap.
        let seal_queue_depth = Arc::new(AtomicUsize::new(0));
        let roll_depth = Arc::clone(&seal_queue_depth);
        let roller = Roller::new(
            move |id| {
                roll_depth.fetch_add(1, Ordering::Relaxed);
                segment_path(&dir_for_paths, id)
            },
            roll_tx,
        );
        // Fold chain (`bn-3l0`, spec 05 §6): opt-in. When on, seed the
        // committer with the per-stream heads rehydrated by recovery so
        // an append after reopen continues each stream's chain from its
        // durable exit head; when off, `ChainInit::off()` keeps the
        // on-disk bytes byte-identical.
        let chain_init = if opts.chain {
            ChainInit::on(chain_heads)
        } else {
            ChainInit::off()
        };
        let direct = DirectCommitter::with_roll_chained(
            &rt,
            writer,
            opts.durability,
            roller,
            chain_init,
        );
        let durable_watermark = direct.watermark();

        let book = Arc::new(RwLock::new(book));

        // Shared seal-path metrics (bn-e2y): the background roll-sealer and any
        // on-demand `seal_active` both feed this one sink, so seal-barrier
        // fsync latency, its degradation alarm, and seal durations aggregate.
        let seal_metrics = Arc::new(SealMetrics::new());

        // Spawn the background auto-roll sealer thread.
        std::fs::create_dir_all(dir.join("sealed")).map_err(|e| {
            EngineError::SealedRead(format!("mkdir sealed: {e}"))
        })?;
        // `bn-u6o`: unset until `Inner::drop` publishes it once, turning the
        // sealer's normal ~10s-per-segment wait into a single deadline shared
        // by every seal still queued at shutdown (see the `Inner` field doc).
        let shutdown_deadline: Arc<OnceLock<Instant>> =
            Arc::new(OnceLock::new());
        // bn-11ba: cumulative seal jobs the sealer thread has taken off the
        // channel — the drain-side companion to `seal_queue_depth`.
        let seal_jobs_dequeued = Arc::new(AtomicU64::new(0));
        // The published read watermark seeds at the recovered event count —
        // 0 on a fresh store. Created before the seal thread so the sealer can
        // gate each rolled segment's seal on the canonical published
        // watermark (bn-2ib; previously it gated on the record book's
        // length).
        let read_watermark = Watermark::new(watermark);
        let seal_thread = {
            let driver =
                SealDriver::new(Arc::clone(&sealed), dir.join("sealed"))
                    .with_metrics(Arc::clone(&seal_metrics))
                    .with_parity(opts.parity)
                    .with_pack(opts.seal_pack);
            let active = Arc::clone(&active);
            let published = read_watermark.clone();
            let fs = rt.fs();
            let dir = dir.to_path_buf();
            let seal_metrics_for_thread = Arc::clone(&seal_metrics);
            let shutdown_deadline = Arc::clone(&shutdown_deadline);
            let backlog = Arc::clone(&seal_queue_depth);
            let dequeued = Arc::clone(&seal_jobs_dequeued);
            std::thread::Builder::new()
                .name("mess-engine-roll-sealer".into())
                .spawn(move || {
                    Self::run_roll_sealer(
                        roll_rx,
                        driver,
                        active,
                        published,
                        fs,
                        dir,
                        seal_metrics_for_thread,
                        shutdown_deadline,
                        SpinConfig::default(),
                        backlog,
                        dequeued,
                    )
                })
                .map_err(|e| EngineError::Open(format!("spawn sealer: {e}")))?
        };

        // bn-30u: re-queue a fresh seal for every rolled segment that is OWED
        // one — that is, one whose candidate was refuted (now, or by an
        // earlier open that left the `*.refuted` quarantine marker) and that
        // is not already being served from a footer-verified sidecar.
        //
        // The trigger is the DURABLE quarantine marker, not the in-memory fact
        // that this open refuted something. That is what makes the crash
        // windows converge: the quarantine erases the candidate, so a trigger
        // keyed on "a candidate was refuted this open" would strand the
        // segment forever if the process died between the rename and the
        // enqueue. Keyed on the marker, every one of these states makes
        // progress and none oscillates:
        //
        //   crash before the quarantine → same candidate, refuted again
        //   crash after the quarantine  → marker present, re-seal enqueued
        //   crash after the new sidecar, before the footer
        //                               → candidate confirmed by the scan and
        //                                 served cold, and (still not
        //                                 footer-verified) enqueued once more
        //                                 so the footer finally lands
        //   after the footer            → admitted; the marker is inert
        //
        // The marker deliberately outlives the repair: it is the forensic
        // evidence quarantine exists to preserve, and once the segment is
        // admitted it costs one `HashSet` entry at open and nothing else.
        //
        // The live head is excluded unconditionally: it is still being
        // appended to, and the sealer's finalize step writes the segment
        // footer. A refuted candidate over the head (from `seal_active`) is
        // still quarantined; the next roll seals that segment normally.
        //
        // At most one job per segment per open, so the queue is bounded by the
        // segment count and cannot grow across reopens.
        owed_reseal
            .extend(candidate_health.refutations.iter().map(|r| r.segment_id));
        let mut pending_reseal = Vec::new();
        for summary in resealable {
            let seg_id = summary.segment_id;
            if seg_id == active_seg_id
                || sealed_ids.contains(&seg_id)
                || !owed_reseal.contains(&seg_id)
            {
                continue;
            }
            if reseal_tx.send(summary).is_ok() {
                // bn-11ba: an owed re-seal is a queued seal job like any
                // other, so it joins the same backlog gauge.
                seal_queue_depth.fetch_add(1, Ordering::Relaxed);
                pending_reseal.push(seg_id);
            }
        }
        drop(reseal_tx);
        candidate_health.pending_reseal = pending_reseal;

        let rt_fs = rt.fs();
        let reader = Arc::new(BlockReader::new(
            rt_fs,
            dir.to_path_buf(),
            opts.capsule_cache_budget_bytes,
        ));
        let publish = Arc::new(PublishState {
            active:         Arc::clone(&active),
            book:           Arc::clone(&book),
            reader:         Arc::clone(&reader),
            read_watermark: read_watermark.clone(),
        });
        let (owner_tx, owner_rx) = tokio_mpsc::channel(OWNER_RING_CAPACITY);
        let owner_bytes = Arc::new(Semaphore::new(OWNER_RING_BYTES));
        let owner_inflight = Arc::new(AtomicUsize::new(0));
        #[cfg(test)]
        let owner_cohort_gate = Arc::new(TestOwnerCohortGate::default());
        let owner_status = Arc::new(OwnerStatus {
            metrics:                        direct.metrics_handle(),
            degraded:                       AtomicBool::new(false),
            outcome_scratch_retained_slots: AtomicUsize::new(0),
            outcome_scratch_retained_bytes: AtomicUsize::new(0),
            outcome_scratch_trims:          AtomicUsize::new(0),
            outcomes:                       OutcomeCounters::default(),
        });
        let owner = FlatOwner {
            direct,
            publish,
            durability: opts.durability,
            inflight: Arc::clone(&owner_inflight),
            status: Arc::clone(&owner_status),
            target: 1,
            outcomes: Vec::new(),
            outcome_reported_retained_slots: 0,
            outcome_reported_retained_bytes: 0,
            #[cfg(test)]
            cohort_gate: Arc::clone(&owner_cohort_gate),
        };
        let owner_join = std::thread::Builder::new()
            .name("mess-flat-owner".into())
            .spawn(move || owner.run(owner_rx))
            .map_err(|e| {
                EngineError::Open(format!("spawn append owner: {e}"))
            })?;

        Ok(LogEngine {
            inner: Arc::new(Inner {
                rt,
                owner: AppendOwner {
                    tx: Some(owner_tx),
                    bytes: owner_bytes,
                    inflight: owner_inflight,
                    durable_watermark,
                    status: owner_status,
                    durability: opts.durability,
                    chain_enabled: opts.chain,
                    join: Some(owner_join),
                    #[cfg(test)]
                    cohort_gate: owner_cohort_gate,
                },
                append_input: AppendInputCounters::default(),
                seal_thread: Some(seal_thread),
                _lock: lock,
                active,
                sealed,
                block_cache: if opts.block_cache_budget_bytes == 0 {
                    BlockCache::disabled()
                } else {
                    // `est_blocks` is a rough shard-sizing seed (budget ÷ a
                    // conservative average block size), not a hard cap.
                    let est_blocks =
                        (opts.block_cache_budget_bytes / 8192).max(64) as usize;
                    BlockCache::with_budget_bytes(
                        opts.block_cache_budget_bytes,
                        est_blocks,
                    )
                },
                seal_metrics,
                book,
                reader,
                recover_decodes: decodes,
                read_watermark,
                opened_at: Instant::now(),
                dir: dir.to_path_buf(),
                seal_pack: opts.seal_pack,
                shutdown_deadline,
                shutdown_seal_budget: opts.shutdown_seal_budget,
                candidate_health,
                seal_queue_depth,
                seal_jobs_dequeued,
                reg_delta_admitted,
                reg_delta_fallback,
            }),
        })
    }

    /// Rehydrate the record book and rebuild the hot index from the durable
    /// log (bn-20b + bn-1vu), and decide how to resume the active segment.
    ///
    /// With live auto-roll the store holds a chain of segments `seg-*.log`
    /// (`seg-1` … `seg-N`, `N` the live head). Recovery walks them in
    /// ascending id order, and — since bn-2ib — reads as little as each
    /// segment's service tier requires. **No payload frame is ever decoded**
    /// on a chain-off open; reads materialise bytes lazily from the durable
    /// blocks instead (see the module docs).
    ///
    /// - **Fully-sealed, non-head segments** (id in `sealed_ids` — i.e. the
    ///   sidecar cross-checked against a valid segment **footer** at load
    ///   (review F2: the footer fsync is what proves the covered bytes are
    ///   durable) — with coverage contiguous with the running watermark and the
    ///   next segment's header `base_pos`) are not read at all beyond their
    ///   52-byte header + 100-byte trailer: per-stream heads come from the
    ///   sidecar directory ([`SealedSegmentIndex::stream_head`]) and the
    ///   watermark advances by the sidecar's `event_count`. This is what makes
    ///   reopen cost O(unsealed bytes), not O(history).
    /// - **Everything else** — unsealed segments (including a
    ///   rolled-but-not-yet-sealed one after a mid-seal crash), a sealed
    ///   segment whose sidecar coverage does not line up (e.g. an on-demand
    ///   [`seal_active`](LogEngine::seal_active) of a segment that kept
    ///   growing), and always the live head — is scanned with
    ///   [`recover_segment`](scanner::recover_segment): batch **metadata** only
    ///   (the mandatory byte-layer CRC still runs; no frame decode). Scanned
    ///   batches seed the hot index with their real `(segment_id, offset)`
    ///   pointers — except the slice a sealed sidecar already covers, which
    ///   stays cold-served.
    /// - The last (highest-id) headed segment is the resumable live head; if
    ///   the head file carries no valid header, the highest headed scanned or
    ///   sealed segment is scanned for resume instead (positions must never
    ///   restart at 0 while durable history exists).
    ///
    /// With the fold chain **on** (spec 05 §6) every segment is still fully
    /// scanned and every payload folded — the chain head is a function of all
    /// payload bytes; `decodes` reports how many frames that materialised.
    #[allow(clippy::too_many_lines)] // one linear pass; splitting obscures the watermark threading
    #[allow(clippy::too_many_arguments)] // one open-time seam; each arg is a distinct recovered surface
    fn recover(
        rt: &RealRuntime,
        dir: &Path,
        active: &ActiveIndex,
        sealed: &SealedStore,
        sealed_ids: &HashSet<u64>,
        pending_sidecars: &HashMap<u64, PendingCandidate>,
        health: &mut SealedCandidateHealth,
        chain: bool,
    ) -> Result<Recovered, EngineError> {
        // `bn-2di` — the interner is no longer loaded up front.
        //
        // Spec 04 §7.1 makes recovery three strictly layered steps, and the
        // whole point of the layering is that the middle one did not exist in
        // this engine before this bone:
        //
        //   step 1  accept batches from bytes    (the scan below; needs
        // nothing)   step 2  materialize $registry        (the fold;
        // needs step 1 only)   step 3  resolve names / rebuild
        // (needs step 2's finished table)
        //
        // The old code collapsed 1 and 3 into one pass because step 2's input
        // came from fjall, not the log — it could resolve a name mid-scan. Now
        // the names ARE in the log, so the scan may not resolve anything: it
        // collects (heads, hot entries, the stream-0 batch pointers) and the
        // resolution happens after the fold, in `finish_recovery`. REG21 in
        // code.
        //
        // Per-stream fold-chain heads rehydrated from the recovered frames
        // (spec 05 §5/§6, `bn-3l0`); empty (and no frame decoded) otherwise.
        let mut chain_heads: HashMap<u64, ChainHead> = HashMap::new();
        let mut decodes = 0u64;

        // Step-1 output, held until the fold has run (step 2).
        let mut scan = ScanOutput::default();

        // bn-30u: pending candidates this pass has resolved (installed or
        // refuted). Whatever is left over at the end named a segment recovery
        // never saw — an ORPHAN — and is refuted too, so a sidecar whose
        // `.log` was deleted or never headed cannot sit in the candidate
        // namespace being re-parsed forever.
        let mut resolved: HashSet<u64> = HashSet::new();
        // bn-30u: the roll summaries a fresh seal of each scanned segment
        // would need (see `Recovered::resealable`).
        let mut resealable: Vec<SegmentSummary> = Vec::new();
        // bn-11ba: registry-delta accelerator use vs. fallback at this open.
        let mut reg_delta_admitted = 0u64;
        let mut reg_delta_fallback = 0u64;

        // Enumerate the segment chain in ascending id order.
        let segment_ids = enumerate_segment_ids(dir);
        if segment_ids.is_empty() {
            // A fresh store: no log, so nothing to fold and nothing to name.
            // It is `LogDerived` from birth — the very first append will write
            // its `$registry` batch. Any candidate under `sealed/` is an
            // orphan by construction.
            Self::refute_orphans(health, pending_sidecars, &resolved);
            return Ok(Recovered {
                book: Book::new(),
                plan: ResumePlan::Fresh,
                chain_heads,
                watermark: 0,
                decodes,
                resealable,
                reg_delta_admitted,
                reg_delta_fallback,
            });
        }

        // Every segment's (cheap, 52-byte) header up front: the sidecar-trust
        // check needs the NEXT segment's base_pos to prove a sealed sidecar
        // covers its whole segment.
        let fs = rt.fs();
        let headers: Vec<Option<scanner::SegmentHeaderInfo>> = segment_ids
            .iter()
            .map(|&id| {
                scanner::read_segment_header(&fs, &segment_path(dir, id))
                    .map_err(|e| {
                        EngineError::Open(format!("header seg {id}: {e}"))
                    })
            })
            .collect::<Result<_, _>>()?;
        let head_id = *segment_ids.last().expect("non-empty");
        // bn-26pp: where the sidecar-trusted branch below looks for a
        // loose-sealed segment's `.reg` registry delta. (bn-3h64: a pack-sealed
        // segment's delta is a section inside its `.seal`, reached through the
        // index rather than by path.)
        let sealed_dir = dir.join("sealed");

        let mut hot_entries: Vec<BatchEntry> = Vec::new();
        let mut last_headed: Option<ResumeInfo> = None;
        let mut watermark = 0u64;

        for (i, &seg_id) in segment_ids.iter().enumerate() {
            let is_head = seg_id == head_id;

            // Sidecar-trusted skip: a fully-sealed, non-head segment whose
            // sidecar coverage is contiguous on both sides needs no byte
            // scan at all (chain-on stores scan everything — the fold needs
            // every payload).
            if !chain
                && !is_head
                && sealed_ids.contains(&seg_id)
                && let Some(sref) = sealed.get(seg_id)
                && let Some(hdr) = headers[i]
                && hdr.segment_id == seg_id
                && hdr.base_pos == watermark
                && sref.base_pos() == watermark
                && headers[i + 1].is_some_and(|h| {
                    h.base_pos == watermark + sref.event_count()
                })
            {
                for &sid in sref.stream_ids() {
                    if let Some(v) = sref.stream_head(sid) {
                        scan.heads
                            .entry(sid)
                            .and_modify(|h| *h = (*h).max(v))
                            .or_insert(v);
                    }
                    scan.referenced_streams.insert(sid);
                }
                // `bn-2di`: this segment's bytes are never read — Spike C's
                // header-only open for sealed segments is exactly what makes
                // reopen cheap, and folding `$registry` must not undo it. It
                // does not have to: `$registry` has its own `stream_id`, so the
                // sealed sidecar's per-stream pointer index resolves its
                // batches DIRECTLY. If the segment carries no stream-0 batch
                // (the overwhelmingly common case — registrations are one per
                // name ever, not one per event) this costs one hash lookup and
                // reads nothing at all. If it does, we `pread` exactly those
                // batches and no others.
                //
                // `bn-26pp`: ...unless the segment's seal also left a registry
                // delta, in which case those same batches are read sequentially
                // as one small contiguous run instead. That is the whole point
                // of the format: one random read per registration is
                // `O(#names)` and cost 89.9% of a 10.5 s cold open at 250k
                // streams (bn-2u01); the delta makes it `O(#segments)`
                // sequential. It is used only after `accepts_registry_delta`
                // cross-checks its batch layout against this very sidecar's
                // directory, it is dropped as soon as the fold has its bytes,
                // and a segment without one takes the `pread` path below
                // unchanged — so a store sealed before this bone, a store with
                // only some segments sealed since, and a store whose deltas
                // were deleted or damaged all recover the identical registry.
                //
                // `bn-3h64`: the delta comes from one of two places, and a
                // segment has at most one of them. A **pack-sealed** segment
                // carries it as the pack's own `REGISTRY_DELTA` section — one
                // bounded `pread` through the directory the open already
                // verified (bn-dbz), cross-checked against that same pack's
                // pointer directory — and `SealDriver` writes no sibling `.reg`
                // for it. A **loose-sidecar** segment has the `.reg` file. Ask
                // the index first and fall through to the file, so a mixed
                // store (pack segments, `.pidx` segments, either kind with or
                // without a delta) folds the identical registry however its
                // segments were sealed. Both branches end in the same
                // `RegistryDelta`, drained into the same fold and dropped.
                if sref.stream_ids().contains(&registry::REGISTRY_STREAM_ID) {
                    // REG1's `$registry` stream id is mess-store's fact, so the
                    // check that a delta is really the registry's belongs here
                    // whichever container produced it. The *layout* check is
                    // the sidecar's, and `read_registry_delta` has already run
                    // it against the pack's own directory — so it is applied
                    // once, on the branch that has not had it.
                    let is_registry = |d: &RegistryDelta| {
                        d.stream_id() == registry::REGISTRY_STREAM_ID
                    };
                    let delta = sref
                        .read_registry_delta()
                        .filter(&is_registry)
                        .or_else(|| {
                            RegistryDelta::open(&reg_path(&sealed_dir, seg_id))
                                .ok()
                                .filter(|d| {
                                    is_registry(d)
                                        && sref.accepts_registry_delta(d)
                                })
                        });
                    match delta {
                        Some(delta) => {
                            reg_delta_admitted += 1;
                            for b in delta.batches() {
                                scan.registry_batches.push((
                                    b.first_global_pos(),
                                    b.payloads().map(<[u8]>::to_vec).collect(),
                                ));
                            }
                        }
                        None => {
                            // bn-11ba: the accelerator was absent or refused
                            // its cross-check; this open pays the point-read
                            // path for this segment's registrations.
                            reg_delta_fallback += 1;
                            let entries = sref
                                .stream_entries(registry::REGISTRY_STREAM_ID)
                                .map_err(|e| {
                                    EngineError::Open(format!(
                                        "recover: $registry entries of sealed \
                                         seg {seg_id}: {e}"
                                    ))
                                })?;
                            for e in entries {
                                scan.registry_ptrs.push(e.ptr);
                            }
                        }
                    }
                }
                watermark += sref.event_count();
                continue;
            }

            // Scan path: the segment image + its batch metadata.
            //
            // `bn-2di` (review F3): the image is kept on EVERY scan, not just
            // the chain-on one. It costs nothing — `recover_segment` reads the
            // whole segment through the `Fs` seam anyway (it must: the A4/A12
            // batch CRC covers every byte) and merely dropped the buffer — and
            // it is what lets the integrity check walk the 28-byte subframe
            // HEADERS for their `event_type_id`s. No payload is decoded, and
            // the buffer dies with this loop iteration, so neither the "zero
            // payload decodes" gate nor the open's peak RSS moves.
            let seg_path = segment_path(dir, seg_id);
            let (rec, image) =
                scanner::recover_segment_with_image(&fs, &seg_path).map_err(
                    |e| EngineError::Open(format!("recover seg {seg_id}: {e}")),
                )?;
            let image = Some(image);
            let Some(header) = rec.header else {
                // A file with no valid header carries no committed batches of
                // this generation — skip it (never resumed, never seeds).
                continue;
            };
            // The slice of this segment a sealed sidecar already serves cold
            // (an on-demand seal of a still-growing segment covers a prefix;
            // batches past it must stay hot-served).
            let sealed_end = if sealed_ids.contains(&seg_id) {
                // Footer-verified at load (F2): already installed.
                resolved.insert(seg_id);
                sealed.get(seg_id).map(|s| s.base_pos() + s.event_count())
            } else if let Some(cand) = pending_sidecars.get(&seg_id) {
                // A footerless sidecar (an on-demand `seal_active` of the
                // live head, or a roll-seal whose footer fsync a crash
                // preceded — review F2): install it only now that THIS scan
                // has proven the durable committed prefix reaches its
                // coverage end.
                //
                // bn-30u: a candidate this scan REFUTES is not merely left
                // uninstalled — it is quarantined, so the identical judgement
                // is not re-run on every future open, and its segment is
                // re-queued for a fresh seal below. Either way the segment is
                // served from the raw log meanwhile, losing nothing.
                resolved.insert(seg_id);
                let end = cand.index.base_pos() + cand.index.event_count();
                if header.base_pos != cand.index.base_pos() {
                    health.refute(
                        seg_id,
                        RefutationReason::IdentityMismatch,
                        &cand.path,
                    );
                    None
                } else if rec.next_pos < end {
                    health.refute(
                        seg_id,
                        RefutationReason::CoverageUnproven,
                        &cand.path,
                    );
                    None
                } else {
                    sealed.install(Arc::clone(&cand.index));
                    Some(end)
                }
            } else {
                None
            };

            // bn-30u: this segment was scanned, which means it is NOT being
            // served from a footer-verified sidecar. Record what a fresh seal
            // of it would need; `open_with` drops the live head and the
            // already-admitted ids and enqueues the rest. `header.segment_id`
            // is required to agree with the file name — the sealer addresses
            // the segment by summary id, so a disagreeing header must not
            // steer it at another file.
            if header.segment_id == seg_id && rec.next_pos > header.base_pos {
                resealable.push(SegmentSummary {
                    segment_id:  seg_id,
                    epoch:       header.epoch,
                    base_pos:    header.base_pos,
                    end_pos:     rec.next_pos,
                    batch_count: rec.accepted.len() as u64,
                    event_count: rec.next_pos - header.base_pos,
                    content_len: rec.safe_offset,
                });
            }

            let mut order: Vec<&AcceptedBatch> = rec.accepted.iter().collect();
            order.sort_by_key(|b| b.first_global_pos);
            for b in &order {
                let sid = b.stream_id;
                // `bn-2di`: the name check MOVED to `finish_recovery` (spec 04
                // §7.1/REG21 — resolution is step 3 and may not run until the
                // fold, step 2, has completed). Record the reference; the check
                // itself is just as loud, only later.
                scan.referenced_streams.insert(sid);
                // Review F3: every `event_type_id` this batch references, taken
                // from its subframe headers — the ids the fold must be able to
                // name. Header-only: `frames` yields borrowed slices and
                // decodes nothing.
                if let Some(image) = &image {
                    let frames = b.frames(image).map_err(|e| {
                        EngineError::Open(format!("recover: {e}"))
                    })?;
                    for f in frames {
                        scan.max_event_type_id =
                            scan.max_event_type_id.max(f.event_type_id);
                    }
                }
                if sid == registry::REGISTRY_STREAM_ID {
                    // A `$registry` batch in a scanned segment: take its
                    // payloads straight from the image we already hold. (The
                    // `pread`-by-pointer path below exists for the sealed
                    // segments the scan never reads at all.)
                    if let Some(image) = &image {
                        let frames = b.frames(image).map_err(|e| {
                            EngineError::Open(format!("recover: {e}"))
                        })?;
                        scan.registry_batches.push((
                            b.first_global_pos,
                            frames.map(|f| f.payload.to_vec()).collect(),
                        ));
                    } else {
                        scan.registry_ptrs.push(EventPtr {
                            segment_id: header.segment_id,
                            offset:     b.offset,
                        });
                    }
                }
                if chain && let Some(image) = &image {
                    // Fold the on-disk payloads into the stream's head, in
                    // ascending version order (§6.2). bn-221: `frames` is
                    // fallible but this caller always passes the exact image
                    // `b` was recovered from — still propagated so a future
                    // refactor fails loudly instead of panicking.
                    let frames = b.frames(image).map_err(|e| {
                        EngineError::Open(format!("recover: {e}"))
                    })?;
                    let head = chain_heads
                        .entry(sid)
                        .or_insert_with(|| ChainHead::genesis(sid));
                    for frame in frames {
                        head.absorb(frame.payload);
                        decodes += 1;
                    }
                }
                scan.heads
                    .entry(sid)
                    .and_modify(|h| *h = (*h).max(b.last_stream_version()))
                    .or_insert(b.last_stream_version());
                // Seed the hot index with every batch a sealed sidecar does
                // not already cover.
                if sealed_end.is_none_or(|end| b.first_global_pos >= end) {
                    hot_entries.push(BatchEntry {
                        stream_id:            sid,
                        first_stream_version: b.first_stream_version,
                        frame_count:          b.frame_count,
                        first_global_pos:     b.first_global_pos,
                        ptr:                  EventPtr {
                            segment_id: header.segment_id,
                            offset:     b.offset,
                        },
                    });
                }
            }
            watermark = watermark.max(rec.next_pos);

            // The highest-id headed segment is the resumable live head.
            last_headed = Some(ResumeInfo {
                segment_id:    header.segment_id,
                base_pos:      header.base_pos,
                epoch:         header.epoch,
                write_off:     rec.safe_offset,
                next_batch_id: rec.next_batch_id,
                next_pos:      rec.next_pos,
                batch_count:   rec.accepted.len() as u64,
                event_count:   rec.next_pos - header.base_pos,
            });
        }

        // The head segment normally produced the resume info above (it is
        // always scanned). If it could not (no valid header — e.g. a crash
        // between the roll's file creation and its header write), resume
        // from the highest segment that DOES head — scanning it now if it
        // was sidecar-skipped — rather than ever falling back to a fresh
        // segment at position 0 over live durable history.
        if last_headed.is_none() && watermark > 0 {
            for &seg_id in segment_ids.iter().rev() {
                let rec =
                    scanner::recover_segment(&fs, &segment_path(dir, seg_id))
                        .map_err(|e| {
                        EngineError::Open(format!("recover seg {seg_id}: {e}"))
                    })?;
                if let Some(header) = rec.header {
                    last_headed = Some(ResumeInfo {
                        segment_id:    header.segment_id,
                        base_pos:      header.base_pos,
                        epoch:         header.epoch,
                        write_off:     rec.safe_offset,
                        next_batch_id: rec.next_batch_id,
                        next_pos:      rec.next_pos,
                        batch_count:   rec.accepted.len() as u64,
                        event_count:   rec.next_pos - header.base_pos,
                    });
                    break;
                }
            }
        }

        active.apply_committed(watermark, &hot_entries);

        // bn-30u: any pending candidate this pass never reached names a
        // segment recovery could not see at all — the `.log` is gone, or it
        // carries no valid header, so nothing will ever confirm the candidate.
        Self::refute_orphans(health, pending_sidecars, &resolved);

        // Steps 2 and 3 (spec 04 §7.1): fold `$registry`, then — and only then
        // — resolve names.
        let book = Self::finish_recovery(&fs, dir, scan)?;

        let plan = match last_headed {
            Some(info) => ResumePlan::Resume(info),
            None => ResumePlan::Fresh,
        };
        Ok(Recovered {
            book,
            plan,
            chain_heads,
            watermark,
            decodes,
            resealable,
            reg_delta_admitted,
            reg_delta_fallback,
        })
    }

    /// Refute every pending candidate the recovery pass never resolved
    /// (bn-30u): its segment has no `.log`, or one with no valid header, so no
    /// future scan can ever confirm it. Deterministic order (ascending segment
    /// id) so the loud log lines and the health record are reproducible.
    fn refute_orphans(
        health: &mut SealedCandidateHealth,
        pending: &HashMap<u64, PendingCandidate>,
        resolved: &HashSet<u64>,
    ) {
        let mut orphans: Vec<u64> = pending
            .keys()
            .copied()
            .filter(|id| !resolved.contains(id))
            .collect();
        orphans.sort_unstable();
        for seg_id in orphans {
            let cand = &pending[&seg_id];
            health.refute(seg_id, RefutationReason::Orphan, &cand.path);
        }
    }

    /// Recovery steps 2 and 3 (spec 04 §7.1), `bn-2di`: materialize `$registry`
    /// from the scan's stream-0 batches, build the interner from it, and only
    /// then check that every `stream_id` the log actually references resolves
    /// to a name.
    ///
    /// The log is the **sole** source of truth for the `id → name` bijection.
    /// There is no second copy anywhere in the store to fall back on, and that
    /// is the point: it is what made the whole metadata keyspace a derived
    /// cache, and therefore deletable — which bn-fj34 did.
    ///
    /// A referenced id that the fold cannot name is therefore fatal, full stop.
    /// It should also be **unreachable**: the append path pushes a
    /// `*Registered` record into the committer's channel under the same
    /// `Book` lock that publishes the id it mints (see `append_batch`), and
    /// recovery accepts a contiguous prefix of the log — so a batch
    /// referencing an id can never out-run that id's registration, in any
    /// crash. If this error ever fires, that invariant has been broken and
    /// guessing a name would be far worse than refusing to open.
    fn finish_recovery(
        fs: &EngineFs,
        dir: &Path,
        scan: ScanOutput,
    ) -> Result<Book, EngineError> {
        let ScanOutput {
            mut registry_batches,
            registry_ptrs,
            referenced_streams,
            max_event_type_id,
            heads,
        } = scan;

        // Step 2a: `pread` the stream-0 batches the scan located but did not
        // decode. One pread per REGISTRATION BATCH — not per event, not per
        // segment: bounded by how many distinct names the store has ever had,
        // which is what makes this affordable on a cold open of a large store.
        for ptr in registry_ptrs {
            registry_batches.push(read_batch_payloads(fs, dir, ptr)?);
        }

        // Step 2b: the fold itself (`RegistryState`, the single fold impl).
        let mut fold = registry::Fold::new();
        for (first_global_pos, payloads) in registry_batches {
            fold.push_batch(first_global_pos, payloads);
        }
        let registry_events = fold.record_count();
        let state: registry::RegistryState = fold
            .finish::<std::convert::Infallible>()
            .map_err(|e| EngineError::Registry(format!("fold: {e}")))?;

        // Step 3: the book IS the fold.
        let mut book = Book::from_registry(state, registry_events)?;
        book.heads = heads;

        // Step 3, the loud part (REG21): a committed event whose stream name
        // cannot be resolved is unrecoverable. This is also the acceptance
        // criterion for the whole bone — it proves the registration of every
        // referenced id really did reach the log no later than the batch
        // referencing it (REG12), or the store refuses to open.
        for sid in referenced_streams {
            if book.stream_name_opt(sid).is_none() {
                return Err(EngineError::Registry(format!(
                    "recover: no interned name for stream_id {sid} \
                     ({registry_events} $registry record(s) folded). The log \
                     is the sole source of truth for names; refusing to open \
                     a store whose ids have no meaning."
                )));
            }
        }

        // ...and the same for EVENT TYPE ids (review F3). Without this a
        // dangling `event_type_id` opened CLEANLY and then poisoned
        // `read_stream` for the whole stream ("no interned name for
        // event_type_id N") — silently, at read time, long after the open that
        // should have refused. Arguably worse than the loud refusal above.
        //
        // # Why a single `max` is a complete check
        //
        // `Book::from_registry` has just proved both namespaces are DENSE
        // (`1..=hwm`, or the open already failed above). So `id` resolves iff
        // `id <= hwm`, and "every referenced id resolves" iff "the LARGEST
        // referenced id resolves". Tracking a `u32` max costs one compare per
        // subframe header, against a `HashSet` insert per event.
        //
        // # Why walking the scan is complete coverage
        //
        // `max_event_type_id` is taken from the subframe HEADERS of every batch
        // the open actually reads — never a payload, so Spike C's "zero payload
        // decodes on a sealed open" property is untouched. Sidecar-trusted
        // sealed segments are not read at all (that is Spike C's entire win:
        // a cold open touches segment headers and sidecars, not segment bytes),
        // and reading them just to re-derive type ids would cost a full pread
        // of every sealed segment — subframe headers are interleaved with the
        // payloads, so there is no "headers only" pread of a segment.
        //
        // They do not need to be read, because a dangling id cannot reach them:
        // a registration is written AHEAD of the batch that first references
        // its id (one owner-side ordered unit), so it holds a LOWER global
        // position; recovery accepts a contiguous PREFIX of positions,
        // so anything below a durable batch is durable too; and a
        // sealed segment is by construction wholly below the recovered
        // head. The only way a use could ever out-live its registration
        // was a partial commit (a rejected `$registry` batch with
        // an accepted batch behind it), which `AppendError::UnitAborted` and
        // serial owner-side staging now make impossible — and
        // which, if it ever did happen, would strike the live tail,
        // which IS scanned.
        if max_event_type_id != registry::REGISTRY_EVENT_TYPE_ID
            && book.type_name_opt(max_event_type_id).is_none()
        {
            return Err(EngineError::Registry(format!(
                "recover: no interned name for event_type_id \
                 {max_event_type_id} ({registry_events} $registry record(s) \
                 folded, event-type high-water {}). The log is the sole \
                 source of truth for names; refusing to open a store whose \
                 ids have no meaning.",
                book.registry.event_type_high_water_mark()
            )));
        }
        Ok(book)
    }

    /// Rebuild a [`SealedStore`] from the sealed sidecars already durable under
    /// `dir/sealed` — the reopen counterpart to [`seal_active`]/the background
    /// sealer. Each complete `.pidx` (with its opportunistic sibling `.filter`
    /// and `.pcol`, re-attached by [`SealedSegmentIndex::open`]) is admitted
    /// so a stream sealed before a restart is served from the cold tier
    /// again rather than silently falling back to hot replay.
    ///
    /// **Crash-mid-seal safety.** The sidecar writer is crash-atomic
    /// (temp-file → fsync → rename, see `SealDriver`'s `write_durable`): a
    /// crash during a seal leaves either the previous state or a complete
    /// `.pidx`, never a torn one under its real name. A partial
    /// `*.pidx.tmp` husk (a seal interrupted before its rename) is ignored
    /// here — it does not match the `.pidx` extension — and a `.pidx` that
    /// fails to parse (CRC / truncation) is skipped, not fatal: the durable
    /// log remains the authority, so that stream is served from the hot
    /// tier until it is re-sealed. Either way the engine reopens into a
    /// readable, recoverable state.
    ///
    /// **Sidecar-before-data crash safety (bn-2ib review F2).** The seal
    /// pipeline makes the sidecar durable strictly BEFORE the segment
    /// footer's whole-file fsync, so a power loss between the two can leave a
    /// CRC-valid sidecar whose covered tail bytes never reached the device
    /// (under `Process` durability nothing else fsynced them). A sidecar is
    /// therefore installed here only when its segment carries a **valid
    /// footer trailer** that cross-checks (`segment_id`, `base_pos`, and
    /// `end_pos == coverage end`) — the footer fsync is what proves the data
    /// bytes are durable. Anything else (notably an on-demand
    /// [`seal_active`](LogEngine::seal_active) sidecar over the still-live
    /// head, which never has a footer) is returned as a **pending candidate**
    /// instead: [`recover`](LogEngine::recover) scans those segments anyway
    /// and installs a candidate only after the scan proves the durable
    /// committed prefix reaches the sidecar's coverage end.
    ///
    /// **Refuted candidates (bn-30u).** A candidate that does not parse at all
    /// is refuted right here and
    /// [quarantined](crate::sealed_candidate::quarantine) — renamed out of the
    /// candidate namespace — so it is never re-read on a later open. Before
    /// bn-30u it stayed on disk and was re-parsed and re-refuted on *every*
    /// reopen while its segment was never re-queued for sealing. The segment
    /// is served from the raw log either way (the log is authority and this
    /// path loses nothing); what changes is that the store now converges back
    /// to a sealed segment instead of degrading permanently. See
    /// [`crate::sealed_candidate`] for the whole lifecycle.
    ///
    /// Returns [`LoadedSealed`]: recovery trust-skips only the footer-verified
    /// ids and scan-verifies the pending ones.
    fn load_sealed(dir: &Path, fs: &EngineFs) -> LoadedSealed {
        let store = SealedStore::new();
        let mut ids = HashSet::new();
        let mut pending: HashMap<u64, PendingCandidate> = HashMap::new();
        let mut health = SealedCandidateHealth::default();
        let sealed_dir = dir.join("sealed");
        let Ok(entries) = std::fs::read_dir(&sealed_dir) else {
            // No sealed directory yet: nothing has been sealed.
            return LoadedSealed {
                store,
                ids,
                pending,
                quarantined: HashSet::new(),
                health,
            };
        };

        // bn-3of DUAL-READ. A segment may have a consolidated `.seal` pack
        // (new path), a legacy `.pidx`+`.filter`+`.pcol` trio (old path), or —
        // during a format migration — both. The `.seal` is preferred: parse
        // every `.seal` first and remember which segment ids it covers, then
        // fold in `.pidx`es only for segments the pack path did not.
        let mut opened: HashMap<u64, PendingCandidate> = HashMap::new();
        let mut from_pack: HashSet<u64> = HashSet::new();
        let mut packs: Vec<std::path::PathBuf> = Vec::new();
        let mut sidecars: Vec<std::path::PathBuf> = Vec::new();
        let mut quarantined: HashSet<u64> = HashSet::new();
        // Collect the whole directory listing BEFORE touching anything.
        // Refuting a candidate renames it, and `readdir` over a directory
        // being mutated may skip or repeat entries — so the classification
        // (which renames) may not run inside the enumeration.
        for entry in entries.flatten() {
            let path = entry.path();
            match path.extension().and_then(|e| e.to_str()) {
                Some("seal") => packs.push(path),
                Some("pidx") => sidecars.push(path),
                // bn-30u: a quarantine marker left by an earlier open. Its
                // segment lost a candidate to refutation and — unless it has
                // since been re-sealed and admitted — is owed a fresh seal.
                // This is the DURABLE re-seal intent: it is written before the
                // enqueue, so a crash in between still converges.
                Some("refuted") => {
                    if let Some(id) =
                        sealed_candidate::segment_id_from_name(&path)
                    {
                        quarantined.insert(id);
                    }
                }
                // `.pidx.tmp`/`.seal.tmp` husks, `.pcol`/`.filter` siblings
                // (re-attached by `open`), `.par`, and anything else.
                _ => {}
            }
        }
        for path in packs {
            // A complete `.seal` is crash-atomic (temp → fsync → rename); a
            // torn `*.seal.tmp` husk is a different extension and was ignored
            // above. A pack that fails to parse (whole-pack hash /
            // mandatory-section CRC) is REFUTED — the log stays authority and
            // the segment is served from the log — and quarantined so it is
            // not re-parsed on every later open (bn-30u).
            match SealedSegmentIndex::open_pack(&path) {
                Ok(index) => {
                    let seg_id = index.segment_id();
                    from_pack.insert(seg_id);
                    opened.insert(
                        seg_id,
                        PendingCandidate { index: Arc::new(index), path },
                    );
                }
                Err(_) => Self::refute_unparsable(&mut health, &path),
            }
        }
        for path in sidecars {
            let index = match SealedSegmentIndex::open(&path) {
                Ok(index) => index,
                Err(_) => {
                    // bn-30u: a `.pidx` that fails its CRC / is truncated is
                    // refuted and quarantined, exactly like an unparsable
                    // pack. Its derived `.filter`/`.pcol`/`.reg` siblings move
                    // with it — they were built from the very index being
                    // thrown away, and a stale `.filter` re-attached to a
                    // LATER re-seal of the same segment could wrongly exclude
                    // a stream.
                    Self::refute_unparsable(&mut health, &path);
                    continue;
                }
            };
            let seg_id = index.segment_id();
            // A `.seal` for this segment wins over its legacy sidecars. The
            // `.pidx` is left entirely unclassified in that case (not opened
            // for judgement, so never refuted): it is inert while the pack
            // serves, and becomes the primary candidate only if the pack is
            // ever refuted and quarantined — which makes that a two-open
            // convergence, not a loop.
            if from_pack.contains(&seg_id) {
                continue;
            }
            opened.insert(
                seg_id,
                PendingCandidate { index: Arc::new(index), path },
            );
        }

        for (seg_id, cand) in opened {
            let coverage_end = cand.index.base_pos() + cand.index.event_count();
            let seg_path = segment_path(dir, seg_id);
            let trailer = read_trailer(fs, &seg_path).ok().flatten();

            // bn-11g: if the footer NAMES a SealPack, resolve the name before
            // anything else. Coverage cannot distinguish the pack this segment
            // was sealed with from a stale one, a copied one, or any
            // same-coverage substitute — the identity can, and a footer that
            // names one is an instruction to require it (spec 01 §3.3.3 reader
            // rules 2 and 3). A failure here refutes and quarantines, so the
            // segment converges to a fresh, correctly-named seal (bn-30u)
            // rather than serving unnamed bytes forever.
            if let Some(t) = &trailer
                && t.segment_id == seg_id
                && t.names_seal_pack()
            {
                match Self::check_named_pack(fs, &seg_path, t, &cand) {
                    Ok(()) => {}
                    Err(reason) => {
                        health.refute(seg_id, reason, &cand.path);
                        continue;
                    }
                }
            }

            // F2 (unchanged trust semantics): only a valid, cross-checking
            // footer proves the covered bytes are durable — install trust-free;
            // everything else is a pending candidate the recovery scan must
            // confirm reaches the coverage end before installing.
            let footer_ok = trailer.is_some_and(|t| {
                t.segment_id == seg_id
                    && t.base_pos == cand.index.base_pos()
                    && t.end_pos == coverage_end
            });
            if footer_ok {
                ids.insert(seg_id);
                store.install(cand.index);
            } else {
                pending.insert(seg_id, cand);
            }
        }
        LoadedSealed { store, ids, pending, quarantined, health }
    }

    /// Resolve a footer's `SealPackIdentity` and check `cand` **is** the pack
    /// it names (bn-11g, spec 01 §3.3.3 reader rules 2–3). `Ok(())` means the
    /// candidate may proceed to the ordinary coverage cross-check; `Err` is the
    /// refutation reason.
    ///
    /// Called only when the trailer's `SEAL_PACK_IDENTITY` flag is set — a bit
    /// covered by `footer_crc`, not by `ext_crc`, so this function is reached
    /// even when the extension itself is damaged. That is the whole point: an
    /// unreadable identity MUST fail closed here rather than read as "the
    /// footer named no pack", which is the legacy coverage-only state a
    /// substituted pack would sail through. Every path below therefore refuses
    /// to install; none of them can fall back to coverage-only trust.
    ///
    /// The candidate is not installed on `Err`, so the segment is served from
    /// the raw log — the canonical bytes, and the only authority (D1). Nothing
    /// here can lose a committed batch.
    fn check_named_pack(
        fs: &EngineFs,
        seg_path: &Path,
        trailer: &mess_log::sealer::SegmentCatalogEntry,
        cand: &PendingCandidate,
    ) -> Result<(), RefutationReason> {
        // (a) the extension region, verified against `ext_crc`. `None` covers
        // an empty region, a malformed locator, a short read, and a CRC
        // mismatch — all "the name is not readable".
        let Ok(Some(ext)) = read_extension(fs, seg_path, trailer) else {
            return Err(RefutationReason::PackIdentityUnresolvable);
        };
        // (b) exactly one well-formed kind-3 section, and (c) a kind this
        // build can check, naming this segment.
        let Some(named) = decode_extension(&ext).pack_identity else {
            return Err(RefutationReason::PackIdentityUnresolvable);
        };
        if !named.kind_is_known() || named.segment_id != trailer.segment_id {
            return Err(RefutationReason::PackIdentityUnresolvable);
        }

        // Rule 3: the candidate must BE that pack. A legacy `.pidx` has no
        // identity to offer and is therefore not the named pack — it is a
        // same-coverage artifact, which is exactly the substitution the
        // identity exists to reject.
        let observed = cand
            .index
            .pack_identity()
            .ok_or(RefutationReason::PackIdentityMismatch)?;
        if observed.as_bytes() != &named.identity {
            eprintln!(
                "!!! mess SEALED PACK IDENTITY MISMATCH: segment {} footer \
                 names {} but {} is {} — pack not installed; segment served \
                 from the raw log (authority)",
                trailer.segment_id,
                named.hex(),
                cand.path.display(),
                observed.hex(),
            );
            return Err(RefutationReason::PackIdentityMismatch);
        }
        if cand.index.pack_format_version() != Some(named.pack_format_version) {
            return Err(RefutationReason::PackIdentityMismatch);
        }
        Ok(())
    }

    /// Refute a candidate whose bytes did not parse (bn-30u). The segment id
    /// comes from the file name by structural parse — the header is exactly
    /// what could not be trusted — falling back to `u64::MAX` for a name that
    /// does not follow the scheme (an operator-dropped file), which is only
    /// ever used to label the log line.
    fn refute_unparsable(health: &mut SealedCandidateHealth, path: &Path) {
        let seg_id =
            sealed_candidate::segment_id_from_name(path).unwrap_or(u64::MAX);
        health.refute(seg_id, RefutationReason::Unparsable, path);
    }
}

/// `pread` + CRC-validate + decode ONE batch at `ptr`, returning its
/// `(first_global_pos, payloads)` — the bounded point-read recovery uses to
/// materialize `$registry` without reading a whole segment (`bn-2di`).
///
/// This is the same two-`pread` shape as [`BlockReader::read_at`] (header for
/// the length, then the batch), minus the capsule cache and the sealed-payload
/// sidecar: recovery runs before either exists. It goes through the recovery
/// scanner's byte layer ([`scanner::accepted_batch_at`]), so the mandatory
/// A4/A12 CRC check applies exactly as it does to every other durable read —
/// a `$registry` record is never taken on trust.
fn read_batch_payloads(
    fs: &EngineFs,
    dir: &Path,
    ptr: EventPtr,
) -> Result<(u64, Vec<Vec<u8>>), EngineError> {
    let path = segment_path(dir, ptr.segment_id);
    let file = fs.open(&path, OpenOpts::read_only()).map_err(|e| {
        EngineError::Open(format!("open seg {}: {e}", ptr.segment_id))
    })?;
    let mut hdr = [0u8; scanner::BATCH_HEADER_LEN];
    pread_exact(&file, ptr.offset, &mut hdr).map_err(|e| {
        EngineError::Open(format!(
            "seg {} off {}: header pread: {e}",
            ptr.segment_id, ptr.offset
        ))
    })?;
    let total_len = scanner::peek_batch_total_len(&hdr, 0).map_err(|s| {
        EngineError::Open(format!(
            "seg {} off {}: bad batch header: {s:?}",
            ptr.segment_id, ptr.offset
        ))
    })?;
    let mut buf = vec![0u8; total_len as usize];
    pread_exact(&file, ptr.offset, &mut buf).map_err(|e| {
        EngineError::Open(format!(
            "seg {} off {}: batch pread: {e}",
            ptr.segment_id, ptr.offset
        ))
    })?;
    let accepted = scanner::accepted_batch_at(&buf, 0).map_err(|s| {
        EngineError::Open(format!(
            "seg {} off {}: batch decode: {s:?}",
            ptr.segment_id, ptr.offset
        ))
    })?;
    if accepted.stream_id != registry::REGISTRY_STREAM_ID {
        return Err(EngineError::Open(format!(
            "seg {} off {}: expected a $registry batch, found stream_id {}",
            ptr.segment_id, ptr.offset, accepted.stream_id
        )));
    }
    let frames = accepted
        .frames(&buf)
        .map_err(|e| EngineError::Open(format!("registry frames: {e}")))?;
    Ok((
        accepted.first_global_pos,
        frames.map(|f| f.payload.to_vec()).collect(),
    ))
}
