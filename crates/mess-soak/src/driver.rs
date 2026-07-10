//! The in-process soak driver: a deterministic mixed workload over one real
//! [`LogEngine`] on a real-fs directory, with continuous invariant probes.
//!
//! Every action the loop takes is chosen from the seeded [`Rng`], so a
//! crash-free run is reproducible from `(seed, config)`. Each probe is the pure
//! function in [`crate::probe`]; the driver only supplies the real engine reads
//! and the [`Shadow`] expectation. The first violation returns an [`Aborted`]
//! carrying a full state dump — the driver never continues past a broken
//! invariant.

use std::time::{Duration, Instant};

use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{Appended, LogEngine, Version};

use crate::config::Config;
use crate::metrics::LatencyHist;
use crate::probe::{self, SubCursor, Violation};
use crate::prng::{Rng, Zipf};
use crate::resource;
use crate::shadow::{Shadow, ShadowEvent};

const EVENT_TYPES: [&str; 3] = ["created", "updated", "archived"];
/// Cap on how many events a single density scan pages in, so the probe stays
/// O(cap) even on a very hot stream late in a multi-hour run.
const DENSITY_SCAN_CAP: usize = 50_000;

/// Terminal outcome: an invariant fired. Carries the operator-facing dump.
#[derive(Debug, Clone)]
pub struct Aborted {
    pub violation: Violation,
    pub dump: String,
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
    pub actions: u64,
    pub appends: u64,
    pub events: u64,
    pub conflicts: u64,
    pub crashes: u64,
    pub index_checks: u64,
    pub density_checks: u64,
    pub head_checks: u64,
    pub subscription_reads: u64,
    pub subscribers_created: u64,
    pub max_reopen: Duration,
    pub last_reopen: Duration,
    pub sealed_segments: usize,
    pub final_events: u64,
    pub fsync: String,
}

/// The driver state.
pub struct Driver {
    cfg: Config,
    rng: Rng,
    zipf: Zipf,
    shadow: Shadow,
    engine: Option<LogEngine>,
    subs: Vec<SubCursor>,
    hist: LatencyHist,
    write_nonce: u64,
    sub_seq: u64,
    report: SoakReport,
    started: Instant,
    last_crash: Instant,
    last_metrics: Instant,
}

impl Driver {
    /// Open the engine and build a driver. Fails if the dir is tmpfs or the
    /// engine cannot open.
    pub fn open(cfg: Config) -> Result<Self, String> {
        if resource::is_tmpfs(&cfg.dir).map_err(|e| format!("tmpfs check: {e}"))? {
            return Err(format!(
                "refusing to soak on tmpfs dir {} — fdatasync is a no-op there, so a \
                 durability/crash soak would validate nothing (--dir must be a real device)",
                cfg.dir.display()
            ));
        }
        std::fs::create_dir_all(&cfg.dir).map_err(|e| format!("create_dir_all: {e}"))?;
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
            last_crash: now,
            last_metrics: now,
        })
    }

    fn engine(&self) -> &LogEngine {
        self.engine.as_ref().expect("engine present between crash cycles")
    }

    fn stream_name(&self, idx: usize) -> String {
        format!("stream-{idx:05}")
    }

    /// Run the soak to completion (or to the first violation).
    pub async fn run(&mut self) -> Result<SoakReport, Aborted> {
        println!("[soak] starting\n  {}", self.cfg.summary());
        let deadline = self.started + self.cfg.duration;
        while Instant::now() < deadline {
            self.step().await?;

            if self.cfg.crash_every != Duration::ZERO
                && self.last_crash.elapsed() >= self.cfg.crash_every
            {
                self.crash_and_recover().await?;
                self.last_crash = Instant::now();
            }
            if self.last_metrics.elapsed() >= self.cfg.metrics_every {
                self.print_metrics();
                self.last_metrics = Instant::now();
            }
        }
        // A final full reconciliation so the run ends on a proven-consistent store.
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
            records.push(RecordToAppend { message_type: ty.to_string(), data: data.clone() });
            shadow_events.push(ShadowEvent { message_type: ty.to_string(), data });
        }

        let t0 = Instant::now();
        let res = self.engine().append_batch(&stream, expected, &records).await;
        let elapsed = t0.elapsed();
        self.hist.record(elapsed);

        match res {
            Ok(Appended { version, last_global_position }) => {
                let k = batch as u64;
                let first_global = last_global_position + 1 - k;
                // The engine's own accounting must agree with what we asked for.
                let want_version = Version::At(first_sp + k - 1);
                if version != want_version {
                    return Err(self.abort(Violation::HeadMismatch {
                        stream: stream.clone(),
                        expected: want_version,
                        got: version,
                    }));
                }
                self.shadow.record_append(&stream, first_sp, first_global, &shadow_events);
                self.report.appends += 1;
                self.report.events += k;
                Ok(())
            }
            Err(mess_store::backend::AppendError::Conflict { .. }) => {
                // The driver is the single writer of record, so a conflict is
                // unexpected but not itself a durability violation — count it
                // and move on (the shadow is untouched).
                self.report.conflicts += 1;
                Ok(())
            }
            Err(mess_store::backend::AppendError::Backend(e)) => {
                Err(self.abort(Violation::RecoveryLoss {
                    detail: format!("append to {stream} failed: {e}"),
                }))
            }
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
            self.subs
                .push(SubCursor::joining_from(format!("sub-{}", self.sub_seq), start));
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
            if let Some(expected) = self.shadow.event_at_global(rec.global_position) {
                let gref = self.shadow.global_ref(rec.global_position).unwrap();
                if let Err(v) =
                    probe::check_record(&gref.stream, gref.stream_pos, expected, &rec)
                {
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
                stream: gref.stream.clone(),
                stream_pos: gref.stream_pos,
                field: "global_missing",
                expected: format!("event at global {gp}"),
                got: "empty read_global page".into(),
            }));
        };
        if rec.global_position != gp {
            return Err(self.abort(Violation::IndexMismatch {
                stream: gref.stream.clone(),
                stream_pos: gref.stream_pos,
                field: "global_position",
                expected: gp.to_string(),
                got: rec.global_position.to_string(),
            }));
        }
        if let Err(v) = probe::check_record(&gref.stream, gref.stream_pos, &expected, &rec) {
            return Err(self.abort(v));
        }

        // Path B: the stream tier (hot ActiveIndex or sealed cold path,
        // whichever this stream currently routes through).
        let after_v =
            if gref.stream_pos == 0 { Version::NoStream } else { Version::At(gref.stream_pos - 1) };
        let spage = match self.engine().read_stream(&gref.stream, after_v, 1).await {
            Ok(p) => p,
            Err(e) => {
                return Err(self.abort(Violation::RecoveryLoss {
                    detail: format!("read_stream({}, {}) failed: {e}", gref.stream, gref.stream_pos),
                }));
            }
        };
        let Some(srec) = spage.into_iter().next() else {
            return Err(self.abort(Violation::IndexMismatch {
                stream: gref.stream.clone(),
                stream_pos: gref.stream_pos,
                field: "stream_missing",
                expected: format!("event at {}@{}", gref.stream, gref.stream_pos),
                got: "empty read_stream page".into(),
            }));
        };
        if let Err(v) = probe::check_record(&gref.stream, gref.stream_pos, &expected, &srec) {
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
                    "truncated: shadow has {expect_len} events, engine returned {}",
                    positions.len()
                ),
            }));
        }
        self.report.density_checks += 1;
        Ok(())
    }

    /// Page a stream's `stream_position`s in order, up to [`DENSITY_SCAN_CAP`].
    async fn read_stream_positions(&self, name: &str) -> Result<Vec<u64>, Violation> {
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
        if let Err(v) = probe::check_fsync_p99(p99, self.cfg.fsync_p99_ceiling) {
            return Err(self.abort(v));
        }
        Ok(())
    }

    // ---- crash / recover ----

    async fn crash_and_recover(&mut self) -> Result<(), Aborted> {
        // We are between actions: no append is in flight, so the engine's
        // durable state and the shadow agree exactly. Drop the handle
        // (Inner::Drop shuts the committer + joins the seal thread → graceful
        // flush + full seal drain) and reopen the SAME dir with NO shared Arc —
        // a genuine recover-from-disk cycle.
        if self.cfg.verbose {
            println!("[soak] crash cycle: dropping and reopening {}", self.cfg.dir.display());
        }
        self.engine = None; // runs Inner::Drop.
        let t0 = Instant::now();
        let engine = LogEngine::open_with(&self.cfg.dir, self.cfg.engine_options())
            .map_err(|e| self.abort(Violation::RecoveryLoss { detail: format!("reopen: {e}") }))?;
        let reopen = t0.elapsed();
        self.engine = Some(engine);
        self.report.crashes += 1;
        self.report.last_reopen = reopen;
        self.report.max_reopen = self.report.max_reopen.max(reopen);
        self.reconcile_after_reopen().await
    }

    /// After a reopen the engine must present EXACTLY the shadow: same total,
    /// dense global prefix, and matching heads on a sample of streams. (The
    /// per-action index/density probes keep checking payloads continuously; the
    /// reconciliation's job is to catch whole-store loss/gain the moment
    /// recovery finishes.)
    async fn reconcile_after_reopen(&mut self) -> Result<(), Aborted> {
        debug_assert!(self.shadow.is_dense(), "driver bug: shadow not dense");
        let engine_total = self.engine().total_events() as u64;
        let shadow_total = self.shadow.total();
        if engine_total != shadow_total {
            return Err(self.abort(Violation::RecoveryLoss {
                detail: format!(
                    "event count changed across reopen: shadow {shadow_total}, engine {engine_total}"
                ),
            }));
        }
        // Dense global prefix: page the whole global order and check positions.
        let mut expect = 0u64;
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
            for rec in &page {
                if rec.global_position != expect {
                    return Err(self.abort(Violation::RecoveryLoss {
                        detail: format!(
                            "recovered global gap/dupe: expected {expect}, got {}",
                            rec.global_position
                        ),
                    }));
                }
                expect += 1;
                after = Some(rec.global_position);
            }
        }
        if expect != shadow_total {
            return Err(self.abort(Violation::RecoveryLoss {
                detail: format!("recovered {expect} global events, shadow has {shadow_total}"),
            }));
        }
        // Sample up to 32 stream heads.
        let names = self.shadow.stream_names();
        let sample = names.len().min(32);
        for _ in 0..sample {
            let name = names[self.rng.below(names.len() as u64) as usize].clone();
            let got = match self.engine().head(&name).await {
                Ok(h) => h,
                Err(e) => {
                    return Err(self.abort(Violation::RecoveryLoss {
                        detail: format!("head({name}) after reopen failed: {e}"),
                    }));
                }
            };
            if let Err(v) = probe::check_head(&name, self.shadow.head(&name), got) {
                return Err(self.abort(v));
            }
        }
        Ok(())
    }

    // ---- reporting ----

    fn print_metrics(&self) {
        let elapsed = self.started.elapsed();
        let rss = resource::current_rss_bytes().map(|b| b / (1024 * 1024)).unwrap_or(0);
        let fd = resource::current_fd_count().unwrap_or(0);
        println!(
            "[soak] t={:>5.0}s actions={} appends={} events={} crashes={} \
             sealed={} subs={}({} created)\n       rss={}MiB fd={} reopen(last/max)={:?}/{:?} \
             fsync[{}]",
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
             REPRODUCE: mess-soak --seed {seed:#x} --dir {dir} --streams {streams} \
             --writers {writers} --duration <>=elapsed> --crash-every {crash:?}\n\
             (the store dir is left intact on disk for post-mortem)\n\
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
            writers = self.cfg.writers,
            crash = self.cfg.crash_every,
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
