//! The in-process soak driver: a deterministic mixed workload over one real
//! [`LogEngine`] on a real-fs directory, with continuous invariant probes.
//!
//! Every action the loop takes is chosen from the seeded [`Rng`], and crash
//! cycles fire on a deterministic ACTION count (never wall-clock), so the whole
//! run — including the store state at every crash point — is reproducible from
//! `(seed, config)` on any machine. Each probe is the pure function in
//! [`crate::probe`]; the driver only supplies the real engine reads and the
//! [`Shadow`] expectation. The first violation returns an [`Aborted`] carrying
//! a full state dump — the driver never continues past a broken invariant.
//!
//! The driver is **strictly sequential**: every `append_batch` is awaited to a
//! definite result before the next action, so nothing is ever in flight when a
//! crash cycle drops the engine (a concurrent-writer mode for the in-process
//! driver is future work; `--writers` currently applies only to the sigkill
//! child). It also REFUSES a non-empty `--dir`: the shadow starts empty, so a
//! leftover store would read as fabricated extras and version conflicts — the
//! actual root cause of the original bn-3dr finding.

use std::time::{Duration, Instant};

use mess_store::backend::{Backend, RecordToAppend, StoredRecord};
use mess_store::{Appended, LogEngine, Version};

use crate::config::Config;
use crate::metrics::LatencyHist;
use crate::prng::{Rng, Zipf};
use crate::probe::{self, SubCursor, Violation};
use crate::resource;
use crate::shadow::{Shadow, ShadowEvent};

const EVENT_TYPES: [&str; 3] = ["created", "updated", "archived"];
/// Cap on how many events a single density scan pages in, so the probe stays
/// O(cap) even on a very hot stream late in a multi-hour run.
const DENSITY_SCAN_CAP: usize = 50_000;

/// Short hex fingerprint of a payload for the `--dump-extras` samples.
fn hexp(bytes: &[u8]) -> String {
    let mut s = String::new();
    for b in bytes.iter().take(24) {
        s.push_str(&format!("{b:02x}"));
    }
    if bytes.len() > 24 {
        s.push_str("...");
    }
    format!("{}B[{s}]", bytes.len())
}

/// Terminal outcome: an invariant fired. Carries the operator-facing dump.
#[derive(Debug, Clone)]
pub struct Aborted {
    pub violation: Violation,
    pub dump:      String,
}

impl std::fmt::Display for Aborted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.dump)
    }
}
impl std::error::Error for Aborted {}

/// Roll-up stats for a completed run.
#[derive(Debug, Clone, Default)]
pub struct SoakReport {
    pub actions:             u64,
    pub appends:             u64,
    pub events:              u64,
    pub conflicts:           u64,
    pub crashes:             u64,
    pub index_checks:        u64,
    pub density_checks:      u64,
    pub head_checks:         u64,
    pub subscription_reads:  u64,
    pub subscribers_created: u64,
    pub max_reopen:          Duration,
    pub last_reopen:         Duration,
    pub sealed_segments:     usize,
    pub final_events:        u64,
    pub fsync:               String,
}

/// The driver state.
pub struct Driver {
    cfg:                   Config,
    rng:                   Rng,
    zipf:                  Zipf,
    shadow:                Shadow,
    engine:                Option<LogEngine>,
    subs:                  Vec<SubCursor>,
    hist:                  LatencyHist,
    write_nonce:           u64,
    sub_seq:               u64,
    report:                SoakReport,
    started:               Instant,
    /// `report.actions` at the last crash cycle — the crash trigger is pinned
    /// to the deterministic action count (`crash_every_actions`), never
    /// wall-clock, so crash points reproduce exactly across machines and CPU
    /// load (reviewer nit, bn-3dr).
    actions_at_last_crash: u64,
    last_metrics:          Instant,
    /// Payloads the driver GENERATED for an append that did not return `Ok`
    /// (a version conflict, or an error) — so the events were never acked and
    /// (correctly) never written durably. An engine "extra" whose payload is
    /// in here means the engine surfaced a conflicted/failed append's
    /// bytes: a real bug (it was never a durable, ordered write),
    /// classified as `fabricated`.
    generated_unacked:     std::collections::HashSet<Vec<u8>>,
    /// The spec-02 A6 candidate set: payloads the driver has SUBMITTED to
    /// `append_batch` but whose result it has not yet observed. A crash that
    /// interrupts an in-flight append leaves its payload here; recovery MAY
    /// then legally surface that complete-but-unacked batch (spec 02 §6 A6
    /// / §3.1 Z1), so a post-reopen "extra" whose payload matches a
    /// candidate is correct, not a violation. In this strictly-sequential
    /// driver every `append_batch` is awaited to a definite result before
    /// the next action (a crash only fires *between* actions), so this set
    /// is empty at every probe point — which is *why* the sequential
    /// driver can never legally surface an extra. Tracked explicitly so
    /// the invariant is the spec-correct candidate-set form (robust if the
    /// workload ever gains real concurrency) rather than a blunt count
    /// compare.
    inflight:              std::collections::HashMap<Vec<u8>, (String, u64)>,
}

impl Driver {
    /// Open the engine and build a driver. Fails if the dir is non-empty (a
    /// leftover store — the actual bn-3dr root cause: a killed prior attempt's
    /// store read as thousands of "extra" events and conflict fodder), if the
    /// dir is tmpfs, or if the engine cannot open.
    pub fn open(cfg: Config) -> Result<Self, String> {
        resource::guard_fresh_dir(&cfg.dir)?;
        if resource::is_tmpfs(&cfg.dir)
            .map_err(|e| format!("tmpfs check: {e}"))?
        {
            return Err(format!(
                "refusing to soak on tmpfs dir {} — fdatasync is a no-op \
                 there, so a durability/crash soak would validate nothing \
                 (--dir must be a real device)",
                cfg.dir.display()
            ));
        }
        std::fs::create_dir_all(&cfg.dir)
            .map_err(|e| format!("create_dir_all: {e}"))?;
        let engine = LogEngine::open_with(&cfg.dir, cfg.engine_options())
            .map_err(|e| format!("open engine: {e}"))?;
        let rng = Rng::new(cfg.seed);
        let zipf = Zipf::new(cfg.streams, cfg.zipf_skew);
        let now = Instant::now();
        Ok(Driver {
            cfg,
            rng,
            zipf,
            shadow: Shadow::new(),
            engine: Some(engine),
            subs: Vec::new(),
            hist: LatencyHist::new(),
            write_nonce: 0,
            sub_seq: 0,
            report: SoakReport::default(),
            started: now,
            actions_at_last_crash: 0,
            last_metrics: now,
            generated_unacked: std::collections::HashSet::new(),
            inflight: std::collections::HashMap::new(),
        })
    }

    fn engine(&self) -> &LogEngine {
        self.engine.as_ref().expect("engine present between crash cycles")
    }

    fn stream_name(&self, idx: usize) -> String { format!("stream-{idx:05}") }

    /// Run the soak to completion (or to the first violation).
    pub async fn run(&mut self) -> Result<SoakReport, Aborted> {
        println!("[soak] starting\n  {}", self.cfg.summary());
        let deadline = self.started + self.cfg.duration;
        while Instant::now() < deadline {
            self.step().await?;

            // Crash trigger pinned to the ACTION COUNT, not wall-clock: the
            // action stream is a pure function of (seed, config), so the store
            // state at every crash point reproduces exactly on any machine.
            if self.cfg.crash_every_actions != 0
                && self.report.actions - self.actions_at_last_crash
                    >= self.cfg.crash_every_actions
            {
                self.crash_and_recover().await?;
                self.actions_at_last_crash = self.report.actions;
            }
            if self.last_metrics.elapsed() >= self.cfg.metrics_every {
                self.print_metrics();
                self.last_metrics = Instant::now();
            }
        }
        // A final full reconciliation so the run ends on a proven-consistent
        // store.
        self.reconcile_after_reopen().await?;
        self.report.final_events = self.engine().total_events() as u64;
        self.report.sealed_segments = self.engine().sealed_segment_count();
        self.report.fsync = self.hist.summary();
        self.print_metrics();
        Ok(self.report.clone())
    }

    /// One weighted workload action.
    async fn step(&mut self) -> Result<(), Aborted> {
        self.report.actions += 1;
        match self.rng.below(100) {
            0..=57 => self.do_append().await?,
            58..=72 => self.do_subscription_step().await?,
            73..=84 => self.do_index_check().await?,
            85..=91 => self.do_density_check().await?,
            92..=96 => self.do_head_check().await?,
            _ => self.do_resource_check()?,
        }
        Ok(())
    }

    // ---- write ----

    async fn do_append(&mut self) -> Result<(), Aborted> {
        let idx = self.zipf.sample(&mut self.rng);
        let stream = self.stream_name(idx);
        let expected = self.shadow.head(&stream);
        let batch = 1 + self.rng.below(self.cfg.max_batch as u64) as usize;

        let mut records = Vec::with_capacity(batch);
        let mut shadow_events = Vec::with_capacity(batch);
        let first_sp = expected.next_position();
        for k in 0..batch {
            let nonce = self.write_nonce;
            self.write_nonce += 1;
            let ty = EVENT_TYPES[(nonce as usize) % EVENT_TYPES.len()];
            let mut data = Vec::with_capacity(24);
            data.extend_from_slice(&nonce.to_le_bytes());
            data.extend_from_slice(&(first_sp + k as u64).to_le_bytes());
            data.extend_from_slice(&self.cfg.seed.to_le_bytes());
            records.push(RecordToAppend {
                message_type: ty.to_string(),
                data:         data.clone(),
            });
            shadow_events
                .push(ShadowEvent { message_type: ty.to_string(), data });
        }

        // Mark every event of this batch in-flight (a spec-02 A6 candidate)
        // for the window in which its durability is submitted-but-unobserved.
        // In this sequential driver the `.await` below resolves before the next
        // action, so these are drained (to acked / conflicted) before any crash
        // probe — the candidate set is empty at reconcile. Tracked so the probe
        // is the spec-correct candidate-set form all the same.
        for (k, ev) in shadow_events.iter().enumerate() {
            self.inflight
                .insert(ev.data.clone(), (stream.clone(), first_sp + k as u64));
        }

        let t0 = Instant::now();
        let res = self.engine().append_batch(&stream, expected, &records).await;
        let elapsed = t0.elapsed();
        self.hist.record(elapsed);
        for ev in &shadow_events {
            self.inflight.remove(&ev.data);
        }

        match res {
            Ok(Appended { version, last_global_position }) => {
                let k = batch as u64;
                let first_global = last_global_position + 1 - k;
                // The engine's own accounting must agree with what we asked
                // for.
                let want_version = Version::At(first_sp + k - 1);
                if version != want_version {
                    return Err(self.abort(Violation::HeadMismatch {
                        stream:   stream.clone(),
                        expected: want_version,
                        got:      version,
                    }));
                }
                self.shadow.record_append(
                    &stream,
                    first_sp,
                    first_global,
                    &shadow_events,
                );
                self.report.appends += 1;
                self.report.events += k;
                Ok(())
            }
            Err(mess_store::backend::AppendError::Conflict { .. }) => {
                // The driver is the single writer of record, so a conflict is
                // unexpected but not itself a durability violation — count it
                // and move on (the shadow is untouched). Remember the generated
                // payloads: they were never acked and must never surface
                // durably (the --dump-extras classifier checks
                // exactly this).
                for ev in &shadow_events {
                    self.generated_unacked.insert(ev.data.clone());
                }
                self.report.conflicts += 1;
                Ok(())
            }
            Err(mess_store::backend::AppendError::Backend(e)) => Err(self
                .abort(Violation::RecoveryLoss {
                    detail: format!("append to {stream} failed: {e}"),
                })),
        }
    }

    // ---- subscription ----

    async fn do_subscription_step(&mut self) -> Result<(), Aborted> {
        // Churn: sometimes a subscriber leaves, sometimes a new one joins.
        if !self.subs.is_empty() && self.rng.chance(0.1) {
            let victim = self.rng.below(self.subs.len() as u64) as usize;
            self.subs.remove(victim);
        }
        if self.subs.len() < self.cfg.subscribers && self.rng.chance(0.5) {
            self.sub_seq += 1;
            let total = self.shadow.total();
            // Half join full (from 0), half join at the live tail.
            let start = if self.rng.chance(0.5) { 0 } else { total };
            self.subs.push(SubCursor::joining_from(
                format!("sub-{}", self.sub_seq),
                start,
            ));
            self.report.subscribers_created += 1;
        }
        if self.subs.is_empty() {
            return Ok(());
        }

        let which = self.rng.below(self.subs.len() as u64) as usize;
        let next = self.subs[which].next_expected;
        let after = if next == 0 { None } else { Some(next - 1) };
        let page = match self.engine().read_global(after, 128).await {
            Ok(p) => p,
            Err(e) => {
                return Err(self.abort(Violation::RecoveryLoss {
                    detail: format!("read_global failed: {e}"),
                }));
            }
        };
        self.report.subscription_reads += 1;
        for rec in page {
            // Cross-check the delivered payload against the shadow at that
            // global position while we are here (a subscription that delivers
            // the wrong bytes is as bad as a gap).
            if let Some(expected) =
                self.shadow.event_at_global(rec.global_position)
            {
                let gref = self.shadow.global_ref(rec.global_position).unwrap();
                if let Err(v) = probe::check_record(
                    &gref.stream,
                    gref.stream_pos,
                    expected,
                    &rec,
                ) {
                    return Err(self.abort(v));
                }
            }
            if let Err(v) = self.subs[which].observe(rec.global_position) {
                return Err(self.abort(v));
            }
        }
        Ok(())
    }

    // ---- index == log ----

    async fn do_index_check(&mut self) -> Result<(), Aborted> {
        let total = self.shadow.total();
        if total == 0 {
            return Ok(());
        }
        let gp = self.rng.below(total);
        let Some(expected) = self.shadow.event_at_global(gp).cloned() else {
            return Ok(());
        };
        let gref = self.shadow.global_ref(gp).unwrap().clone();

        // Path A: the global tier.
        let after = if gp == 0 { None } else { Some(gp - 1) };
        let page = match self.engine().read_global(after, 1).await {
            Ok(p) => p,
            Err(e) => {
                return Err(self.abort(Violation::RecoveryLoss {
                    detail: format!("read_global({gp}) failed: {e}"),
                }));
            }
        };
        let Some(rec) = page.into_iter().next() else {
            return Err(self.abort(Violation::IndexMismatch {
                stream:     gref.stream.clone(),
                stream_pos: gref.stream_pos,
                field:      "global_missing",
                expected:   format!("event at global {gp}"),
                got:        "empty read_global page".into(),
            }));
        };
        if rec.global_position != gp {
            return Err(self.abort(Violation::IndexMismatch {
                stream:     gref.stream.clone(),
                stream_pos: gref.stream_pos,
                field:      "global_position",
                expected:   gp.to_string(),
                got:        rec.global_position.to_string(),
            }));
        }
        if let Err(v) =
            probe::check_record(&gref.stream, gref.stream_pos, &expected, &rec)
        {
            return Err(self.abort(v));
        }

        // Path B: the stream tier (hot ActiveIndex or sealed cold path,
        // whichever this stream currently routes through).
        let after_v = if gref.stream_pos == 0 {
            Version::NoStream
        } else {
            Version::At(gref.stream_pos - 1)
        };
        let spage =
            match self.engine().read_stream(&gref.stream, after_v, 1).await {
                Ok(p) => p,
                Err(e) => {
                    return Err(self.abort(Violation::RecoveryLoss {
                        detail: format!(
                            "read_stream({}, {}) failed: {e}",
                            gref.stream, gref.stream_pos
                        ),
                    }));
                }
            };
        let Some(srec) = spage.into_iter().next() else {
            return Err(self.abort(Violation::IndexMismatch {
                stream:     gref.stream.clone(),
                stream_pos: gref.stream_pos,
                field:      "stream_missing",
                expected:   format!(
                    "event at {}@{}",
                    gref.stream, gref.stream_pos
                ),
                got:        "empty read_stream page".into(),
            }));
        };
        if let Err(v) =
            probe::check_record(&gref.stream, gref.stream_pos, &expected, &srec)
        {
            return Err(self.abort(v));
        }
        self.report.index_checks += 1;
        Ok(())
    }

    // ---- per-stream density ----

    async fn do_density_check(&mut self) -> Result<(), Aborted> {
        let names = self.shadow.stream_names();
        if names.is_empty() {
            return Ok(());
        }
        let name = names[self.rng.below(names.len() as u64) as usize].clone();
        let positions = match self.read_stream_positions(&name).await {
            Ok(p) => p,
            Err(v) => return Err(self.abort(v)),
        };
        if let Err(v) = probe::check_density(&name, &positions) {
            return Err(self.abort(v));
        }
        // Count must also match the shadow (a truncated tail is a loss even if
        // the surviving prefix is dense).
        let expect_len = self.shadow.stream_events(&name).len();
        if positions.len() < expect_len.min(DENSITY_SCAN_CAP) {
            return Err(self.abort(Violation::Density {
                stream: name.clone(),
                detail: format!(
                    "truncated: shadow has {expect_len} events, engine \
                     returned {}",
                    positions.len()
                ),
            }));
        }
        self.report.density_checks += 1;
        Ok(())
    }

    /// Page a stream's `stream_position`s in order, up to [`DENSITY_SCAN_CAP`].
    async fn read_stream_positions(
        &self,
        name: &str,
    ) -> Result<Vec<u64>, Violation> {
        let mut out = Vec::new();
        let mut cursor = Version::NoStream;
        loop {
            let page = self
                .engine()
                .read_stream(name, cursor, 1024)
                .await
                .map_err(|e| Violation::RecoveryLoss {
                    detail: format!("read_stream({name}) paging failed: {e}"),
                })?;
            if page.is_empty() {
                break;
            }
            for rec in &page {
                out.push(rec.stream_position);
                cursor = Version::At(rec.stream_position);
            }
            if out.len() >= DENSITY_SCAN_CAP || page.len() < 1024 {
                break;
            }
        }
        Ok(out)
    }

    // ---- head ----

    async fn do_head_check(&mut self) -> Result<(), Aborted> {
        let names = self.shadow.stream_names();
        if names.is_empty() {
            return Ok(());
        }
        let name = names[self.rng.below(names.len() as u64) as usize].clone();
        let got = match self.engine().head(&name).await {
            Ok(h) => h,
            Err(e) => {
                return Err(self.abort(Violation::RecoveryLoss {
                    detail: format!("head({name}) failed: {e}"),
                }));
            }
        };
        if let Err(v) = probe::check_head(&name, self.shadow.head(&name), got) {
            return Err(self.abort(v));
        }
        self.report.head_checks += 1;
        Ok(())
    }

    // ---- resource ceilings ----

    fn do_resource_check(&mut self) -> Result<(), Aborted> {
        if let Some(rss) = resource::current_rss_bytes()
            && let Err(v) = probe::check_rss(rss, self.cfg.rss_ceiling_bytes)
        {
            return Err(self.abort(v));
        }
        if let Some(fd) = resource::current_fd_count()
            && let Err(v) = probe::check_fd(fd, self.cfg.fd_ceiling)
        {
            return Err(self.abort(v));
        }
        let p99 = self.hist.percentile(99.0);
        if let Err(v) = probe::check_fsync_p99(p99, self.cfg.fsync_p99_ceiling)
        {
            return Err(self.abort(v));
        }
        Ok(())
    }

    // ---- crash / recover ----

    async fn crash_and_recover(&mut self) -> Result<(), Aborted> {
        // We are between actions: this sequential driver awaits every append to
        // a definite result before the next, so no append is in flight and the
        // A6 candidate set ([`inflight`](Self::inflight)) is empty here — which
        // is exactly why the engine may legally surface nothing beyond the
        // acked set (the reconcile below still asserts the general
        // spec-02 A6 form, so it stays correct if the workload ever
        // gains real concurrency). Drop the handle (Inner::Drop shuts
        // the committer + joins the seal thread → graceful flush + full
        // seal drain) and reopen the SAME dir with NO shared Arc — a
        // genuine recover-from-disk cycle.
        if self.cfg.verbose {
            println!(
                "[soak] crash cycle: dropping and reopening {}",
                self.cfg.dir.display()
            );
        }
        self.engine = None; // runs Inner::Drop.
        let t0 = Instant::now();
        let engine =
            LogEngine::open_with(&self.cfg.dir, self.cfg.engine_options())
                .map_err(|e| {
                    self.abort(Violation::RecoveryLoss {
                        detail: format!("reopen: {e}"),
                    })
                })?;
        let reopen = t0.elapsed();
        self.engine = Some(engine);
        self.report.crashes += 1;
        self.report.last_reopen = reopen;
        self.report.max_reopen = self.report.max_reopen.max(reopen);
        self.reconcile_after_reopen().await
    }

    /// After a reopen the engine must present the SPEC-CORRECT recovered image,
    /// not a byte-identical copy of the shadow (spec `02-recovery.md` §6 A6 /
    /// §3.1 Z1: recovery MAY legally surface a *complete, unacked* batch — a
    /// batch whose durability was never acknowledged to the caller — so
    /// `engine_total >= shadow_total` is legal, and a bare count-equality probe
    /// is too strict). The invariant this asserts, in three parts:
    ///
    /// 1. **No acked loss, byte-exact.** Every shadow-acked event is present at
    ///    its recorded global position with byte-exact `(stream, stream_pos,
    ///    type, payload)` — acked history is a recovered prefix, never dropped
    ///    or moved.
    /// 2. **Dense global order.** The engine's global positions tile `0..N`
    ///    exactly (no gap, no duplicate position).
    /// 3. **Every extra is a legal A6 candidate.** Each engine event that is
    ///    *not* a shadow-acked position MUST byte-match a distinct
    ///    submitted-but-unacked append (the [`inflight`](Self::inflight)
    ///    candidate set), each candidate consumed at most once. An extra whose
    ///    payload duplicates an acked event is a **double-replay** engine bug;
    ///    an extra matching no submitted append at all is **fabricated** — both
    ///    fatal. (In this sequential driver the candidate set is empty at every
    ///    crash, so any extra is a bug: the A6 tolerance is FORWARD-LOOKING —
    ///    correct for a future concurrent-writer driver — and loses no teeth
    ///    today: empty candidates ⇒ every extra classifies as fabricated ⇒
    ///    abort.)
    ///
    /// Historical note (bn-3dr): the original "2727 extras" finding was NOT an
    /// A6 surfacing and NOT an engine bug — the run had opened a dirty `--dir`
    /// holding a killed prior attempt's store, so the extras were pre-existing
    /// events the shadow never saw (they classify as *fabricated* under this
    /// probe) and the 277k conflicts were writers racing the leftover stream
    /// heads. The real fix is the fresh-dir refusal in [`Driver::open`]; this
    /// invariant would have identified the leftovers instantly instead of
    /// reporting a bare count mismatch.
    ///
    /// Also samples per-stream density so a stream whose positions gained a
    /// duplicate/gap is caught even if the global tiling happens to stay dense.
    async fn reconcile_after_reopen(&mut self) -> Result<(), Aborted> {
        debug_assert!(self.shadow.is_dense(), "driver bug: shadow not dense");
        let engine_total = self.engine().total_events() as u64;
        let shadow_total = self.shadow.total();

        // An acked event beyond the recovered end is a hard loss (A6 permits a
        // longer tail, never a shorter one — "no acked batch is ever lost").
        if let Some(max_acked) = self.shadow.max_global()
            && max_acked >= engine_total
        {
            return Err(self.abort(Violation::RecoveryLoss {
                detail: format!(
                    "acked global {max_acked} lost: recovered total is only \
                     {engine_total} events"
                ),
            }));
        }

        // Page the whole recovered global order once.
        let mut engine: Vec<StoredRecord> =
            Vec::with_capacity(engine_total as usize);
        let mut after = None;
        loop {
            let page = match self.engine().read_global(after, 2048).await {
                Ok(p) => p,
                Err(e) => {
                    return Err(self.abort(Violation::RecoveryLoss {
                        detail: format!("read_global reconcile failed: {e}"),
                    }));
                }
            };
            if page.is_empty() {
                break;
            }
            after = Some(page.last().unwrap().global_position);
            engine.extend(page);
        }

        // Candidate multiset: the submitted-but-unacked payloads recovery may
        // legally surface (empty in this sequential driver — see `inflight`).
        let mut candidates: std::collections::HashMap<Vec<u8>, u32> =
            std::collections::HashMap::new();
        for payload in self.inflight.keys() {
            *candidates.entry(payload.clone()).or_insert(0) += 1;
        }
        let acked_index = self.shadow.acked_payload_index();

        let mut acked_seen = 0u64;
        let mut extras = probe::ExtraCounts::default();
        for (i, rec) in engine.iter().enumerate() {
            let gp = i as u64;
            if rec.global_position != gp {
                return Err(self.abort(Violation::RecoveryLoss {
                    detail: format!(
                        "recovered global gap/dupe: expected {gp}, got {}",
                        rec.global_position
                    ),
                }));
            }
            match self.shadow.global_ref(gp) {
                Some(gref) => {
                    // Part 1: acked event must be byte-exact at its position.
                    let gref = gref.clone();
                    let expected =
                        self.shadow.event_at_global(gp).unwrap().clone();
                    if let Err(v) = probe::check_record(
                        &gref.stream,
                        gref.stream_pos,
                        &expected,
                        rec,
                    ) {
                        return Err(self.abort(v));
                    }
                    acked_seen += 1;
                }
                None => {
                    // Part 3: an extra — classify it (pure probe fn).
                    extras.record(probe::classify_extra(
                        &rec.data,
                        &acked_index,
                        &mut candidates,
                    ));
                }
            }
        }
        if acked_seen != shadow_total {
            return Err(self.abort(Violation::RecoveryLoss {
                detail: format!(
                    "acked events unaccounted: matched {acked_seen} of \
                     {shadow_total} at their positions"
                ),
            }));
        }

        // Part 3 verdict: any duplicate-of-acked or fabricated extra is a real
        // engine recovery bug; submitted-unacked extras are spec-legal (A6).
        if extras.illegal() > 0 {
            if self.cfg.dump_extras.is_some() {
                self.classify_and_dump_extras(shadow_total, engine_total).await;
            }
            return Err(self.abort(Violation::RecoveryLoss {
                detail: format!(
                    "illegal extras after reopen: {} duplicate-of-acked \
                     (double-replay) + {} fabricated (out of {} total extras; \
                     {} were legal submitted-unacked A6 candidates)",
                    extras.duplicate_of_acked,
                    extras.fabricated,
                    engine_total - shadow_total,
                    extras.submitted_unacked,
                ),
            }));
        }

        // Part 2 (per stream): sample up to 32 streams and assert the engine's
        // positions are dense with an acked prefix, no duplicate/gap.
        let names = self.shadow.stream_names();
        let sample = names.len().min(32);
        for _ in 0..sample {
            let name =
                names[self.rng.below(names.len() as u64) as usize].clone();
            let positions = match self.read_stream_positions(&name).await {
                Ok(p) => p,
                Err(v) => return Err(self.abort(v)),
            };
            if let Err(v) = probe::check_density(&name, &positions) {
                return Err(self.abort(v));
            }
            // The engine head may legally lead the shadow head by a
            // submitted-unacked tail, but never trail it (that would be loss).
            let engine_head = match self.engine().head(&name).await {
                Ok(h) => h,
                Err(e) => {
                    return Err(self.abort(Violation::RecoveryLoss {
                        detail: format!(
                            "head({name}) after reopen failed: {e}"
                        ),
                    }));
                }
            };
            if engine_head.next_position()
                < self.shadow.head(&name).next_position()
            {
                return Err(self.abort(Violation::HeadMismatch {
                    stream:   name.clone(),
                    expected: self.shadow.head(&name),
                    got:      engine_head,
                }));
            }
        }
        Ok(())
    }

    /// Classify every engine event against the shadow after a count mismatch
    /// (bn-3dr `--dump-extras`). Partitions the surplus into the three buckets
    /// the diagnosis needs — duplicate-of-acked (double-replay), fabricated
    /// (matches no submitted append), submitted-unacked (a payload the driver
    /// generated but whose append never returned `Ok`) — and writes the whole
    /// picture to the configured JSON path. Best-effort: any I/O error here is
    /// printed, never masks the abort that follows.
    async fn classify_and_dump_extras(
        &self,
        shadow_total: u64,
        engine_total: u64,
    ) {
        let Some(path) = self.cfg.dump_extras.clone() else { return };

        // Page the engine's whole global order.
        let mut engine: Vec<StoredRecord> =
            Vec::with_capacity(engine_total as usize);
        let mut after = None;
        loop {
            let page = match self.engine().read_global(after, 4096).await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("[dump-extras] read_global failed: {e}");
                    break;
                }
            };
            if page.is_empty() {
                break;
            }
            after = Some(page.last().unwrap().global_position);
            engine.extend(page);
        }

        // Multiset of engine payloads → global positions.
        let mut engine_by_payload: std::collections::HashMap<
            Vec<u8>,
            Vec<u64>,
        > = std::collections::HashMap::new();
        for rec in &engine {
            engine_by_payload
                .entry(rec.data.clone())
                .or_default()
                .push(rec.global_position);
        }
        let acked = self.shadow.acked_payload_index();

        let (
            mut duplicate_of_acked,
            mut acked_missing,
            mut submitted_unacked,
            mut fabricated,
        ) = (0u64, 0u64, 0u64, 0u64);
        let mut dup_samples: Vec<String> = Vec::new();
        let mut fab_samples: Vec<String> = Vec::new();
        for (payload, gps) in &engine_by_payload {
            let ecount = gps.len() as u64;
            match acked.get(payload) {
                Some(acked_gps) => {
                    let scount = acked_gps.len() as u64;
                    if ecount > scount {
                        duplicate_of_acked += ecount - scount;
                        if dup_samples.len() < 20 {
                            dup_samples.push(format!(
                                "payload {} acked@{:?} engine@{:?}",
                                hexp(payload),
                                acked_gps,
                                gps
                            ));
                        }
                    } else if scount > ecount {
                        acked_missing += scount - ecount;
                    }
                }
                None => {
                    // Legal A6 candidates are the still-in-flight submitted
                    // payloads. A conflicted payload (in `generated_unacked`)
                    // was NEVER a durable ordered write, so surfacing it is a
                    // bug — classified with fabricated, but tagged so the dump
                    // shows it came from a conflict.
                    if self.inflight.contains_key(payload) {
                        submitted_unacked += ecount;
                    } else {
                        fabricated += ecount;
                        if fab_samples.len() < 20 {
                            let tag =
                                if self.generated_unacked.contains(payload) {
                                    "was a CONFLICTED (never-durable) append"
                                } else {
                                    "matches NO submitted append"
                                };
                            fab_samples.push(format!(
                                "payload {} engine@{:?} ({tag})",
                                hexp(payload),
                                gps
                            ));
                        }
                    }
                }
            }
        }

        // Is the shadow prefix 0..shadow_total byte-exact, i.e. are the extras
        // a clean tail? (A mid-log double-count would shift the
        // prefix.)
        let mut prefix_divergent = 0u64;
        let bound = shadow_total.min(engine.len() as u64);
        for gp in 0..bound {
            if let Some(exp) = self.shadow.event_at_global(gp) {
                let got = &engine[gp as usize];
                if got.data != exp.data || got.message_type != exp.message_type
                {
                    prefix_divergent += 1;
                }
            }
        }

        let json = format!(
            "{{\n  \"shadow_total\": {shadow_total},\n  \"engine_total\": \
             {engine_total},\n  \"extras\": {extras},\n  \"conflicts\": \
             {conflicts},\n  \"generated_unacked_distinct\": {gu},\n  \
             \"classification\": {{\n    \"duplicateOfAcked\": \
             {duplicate_of_acked},\n    \"fabricated\": {fabricated},\n    \
             \"submittedUnacked\": {submitted_unacked},\n    \
             \"ackedMissing\": {acked_missing}\n  }},\n  \
             \"prefix_divergent\": {prefix_divergent},\n  \"clean_tail\": \
             {clean_tail},\n  \"duplicate_samples\": [\n    {dups}\n  ],\n  \
             \"fabricated_samples\": [\n    {fabs}\n  ]\n}}\n",
            extras = engine_total.saturating_sub(shadow_total),
            conflicts = self.report.conflicts,
            gu = self.generated_unacked.len(),
            clean_tail = prefix_divergent == 0,
            dups = dup_samples
                .iter()
                .map(|s| format!("\"{}\"", s.replace('"', "'")))
                .collect::<Vec<_>>()
                .join(",\n    "),
            fabs = fab_samples
                .iter()
                .map(|s| format!("\"{}\"", s.replace('"', "'")))
                .collect::<Vec<_>>()
                .join(",\n    "),
        );
        match std::fs::write(&path, &json) {
            Ok(()) => eprintln!(
                "[dump-extras] wrote classification to {}",
                path.display()
            ),
            Err(e) => {
                eprintln!("[dump-extras] write {} failed: {e}", path.display())
            }
        }
        eprintln!("[dump-extras] {json}");
    }

    // ---- reporting ----

    fn print_metrics(&self) {
        let elapsed = self.started.elapsed();
        let rss = resource::current_rss_bytes()
            .map(|b| b / (1024 * 1024))
            .unwrap_or(0);
        let fd = resource::current_fd_count().unwrap_or(0);
        println!(
            "[soak] t={:>5.0}s actions={} appends={} events={} crashes={} \
             sealed={} subs={}({} created)\n       rss={}MiB fd={} \
             reopen(last/max)={:?}/{:?} fsync[{}]",
            elapsed.as_secs_f64(),
            self.report.actions,
            self.report.appends,
            self.report.events,
            self.report.crashes,
            self.engine().sealed_segment_count(),
            self.subs.len(),
            self.report.subscribers_created,
            rss,
            fd,
            self.report.last_reopen,
            self.report.max_reopen,
            self.hist.summary(),
        );
    }

    /// Build the abort dump for a violation. This is the "reproducible state
    /// bundle" the acceptance criteria require: seed, full config, store dir
    /// (left on disk for post-mortem), live stats, and the fsync histogram.
    fn abort(&self, violation: Violation) -> Aborted {
        let rss = resource::current_rss_bytes().unwrap_or(0);
        let fd = resource::current_fd_count().unwrap_or(0);
        let dump = format!(
            "\n================= SOAK INVARIANT VIOLATION =================\n\
             VIOLATION: {violation}\n\
             ----------------------------------------------------------\n\
             REPRODUCE: mess-soak --seed {seed:#x} --dir <FRESH-DIR> --streams {streams} \
             --duration <>=elapsed> --crash-every-actions {crash}\n\
             (this run's dir {dir} is left intact on disk for post-mortem; \
             reproduce into a FRESH dir — a non-empty --dir is refused)\n\
             ----------------------------------------------------------\n\
             CONFIG:\n  {cfg}\n\
             ----------------------------------------------------------\n\
             STATE: elapsed={elapsed:?} actions={actions} appends={appends} events={events} \
             conflicts={conflicts} crashes={crashes}\n  \
             index_checks={ic} density_checks={dc} head_checks={hc} \
             subscription_reads={sr} live_subs={subs}\n  \
             shadow_total={total} shadow_dense={dense} engine_total={etotal} \
             sealed_segments={sealed}\n  \
             reopen(last/max)={last:?}/{max:?} rss={rss}B fd={fd}\n  \
             fsync[{fsync}]\n\
             ===========================================================\n",
            violation = violation,
            seed = self.cfg.seed,
            dir = self.cfg.dir.display(),
            streams = self.cfg.streams,
            crash = self.cfg.crash_every_actions,
            cfg = self.cfg.summary(),
            elapsed = self.started.elapsed(),
            actions = self.report.actions,
            appends = self.report.appends,
            events = self.report.events,
            conflicts = self.report.conflicts,
            crashes = self.report.crashes,
            ic = self.report.index_checks,
            dc = self.report.density_checks,
            hc = self.report.head_checks,
            sr = self.report.subscription_reads,
            subs = self.subs.len(),
            total = self.shadow.total(),
            dense = self.shadow.is_dense(),
            etotal = self.engine.as_ref().map(|e| e.total_events()).unwrap_or(0),
            sealed = self.engine.as_ref().map(|e| e.sealed_segment_count()).unwrap_or(0),
            last = self.report.last_reopen,
            max = self.report.max_reopen,
            rss = rss,
            fd = fd,
            fsync = self.hist.summary(),
        );
        Aborted { violation, dump }
    }
}
