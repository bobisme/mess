//! DST harness support (bn-3kn): a seed-reproducible **whole-store**
//! scenario driver composing the store's actors — the real production
//! [`Committer`], subscriber-style readers ([`ReadView`] +
//! [`Watermark::await_past`]), and a sealer stand-in built on the real
//! [`SegmentWriter::seal`] — under [`SimRuntime`]'s virtual time and fault
//! filesystem, across a **chain** of segments.
//!
//! # Why this lives in `tests/`, not `src/`
//!
//! Every type this module touches (`Committer`, `SegmentWriter`, `ReadView`,
//! `Watermark`, `SimRuntime`/`SimFs`, `recover_segment`, `sealer::recover_fast`)
//! is already `pub` on `mess-log`'s normal API. Nothing here needed a new
//! production seam — it is pure test-side composition, so it belongs in
//! `tests/`, matching `crash_harness.rs`/`sigkill_harness.rs`. It is a
//! `tests/dst_support/mod.rs` (the `mod.rs` special case), not
//! `tests/dst_support.rs`, so Cargo does not compile it as its own
//! (harness-less, pointless) test binary; every `tests/dst_*.rs` pulls it in
//! with `#[path = "dst_support/mod.rs"] mod dst_support;`.
//!
//! # What one scenario does
//!
//! A `u64` seed derives, via [`mess_log::runtime::Rng`] (the same
//! dependency-free seeded PRNG the sim scheduler itself uses — no `rand`
//! crate needed), a **chain** of 2–4 segments. Each segment is one of:
//!
//! - **`Committer`** — the real production [`Committer`] over a fresh
//!   [`SegmentWriter`], a randomized [`Durability`] mode, 1–3 concurrent
//!   appenders, and 0–3 concurrent laggy readers. This is the
//!   `crash_harness.rs` shape, now additionally racing live readers.
//! - **`Sealed`** — a raw [`SegmentWriter`] driven directly by the scenario
//!   (append → periodic `sync` → **`seal`**), so the harness itself keeps
//!   ownership of the writer and can call [`SegmentWriter::seal`] — the
//!   sealer stand-in the bone asks for. Concurrent readers race the append
//!   *and* the seal barrier itself (the "seal-race" the bone names).
//!
//! Every segment composes fault injection from **production fault-injection
//! primitives**, no test-only `Fs` wrapper needed:
//!
//! - **crash mid-barrier** — `SimFs::inject_enospc(path, EnospcSite::Fdatasync)`:
//!   the barrier fails WITHOUT promoting durability (`sim_fs.rs`'s own
//!   documented semantics), exactly the write-then-barrier crash window
//!   `crash_harness.rs`'s `FireOnFsync` models — but reached here through the
//!   real ENOSPC path rather than a bespoke wrapper.
//! - **ENOSPC** — the same primitive at `EnospcSite::Pwrite` (a batch write
//!   refused) or `EnospcSite::Fdatasync` (the barrier refused).
//! - **torn sectors** — every segment runs on the `Fault::SECTOR_512` medium
//!   (sector-granular reordering + tear, ported from `spikes/torn_write`) and
//!   ends with a seeded `SimFs::crash_random` call: any batch that was
//!   `pwrite`n but never covered by a returning `fdatasync` is torn exactly
//!   as a real crash would leave it. On a segment that finished cleanly this
//!   is a no-op (nothing pending) — the harness always calls it, so a
//!   scenario that plans no explicit fault still gets this pass, cheaply.
//! - **reader races** — every segment's concurrent readers keep re-reading
//!   (with a random virtual-time lag) for the segment's whole lifetime,
//!   including through the seal barrier.
//!
//! # Invariants checked after every segment
//!
//! - **Well-formed recovery**: [`recover_segment`]'s prefix is
//!   position/byte-contiguous, current-epoch, and never invents an event
//!   that was never submitted (mirrors `crash_harness.rs`'s
//!   `assert_recovery_wellformed`).
//! - **Acked ⟹ recovered**, per the [`Durability`] contract (§1 of
//!   `03-durability.md`): every position the committer/writer durably
//!   acknowledged (a real `Acked` outcome, or a raw-writer `sync()` that
//!   returned `Ok`) is inside the recovered prefix. Scoped to `Os`/`Group`
//!   (and every `Sealed`-kind segment, which only ever advances on a real
//!   `sync()` `Ok`): `Durability::Process` is explicitly excluded, because
//!   BY DESIGN it advances its watermark with no barrier at all (§1.1) —
//!   see `SegOutcome::durability_guaranteed`.
//! - **Reader-never-past-watermark, globally** — same scope: every reader
//!   observation recorded during the LIVE run of a barrier-backed segment is
//!   `<=` the position recovery later proves durable — i.e. nothing a
//!   subscriber was ever shown evaporates on crash. This is the composed
//!   property `dst_harness_self_test.rs` demonstrates the harness can
//!   actually catch (a deliberately broken watermark).
//! - **R2 agrees with the full scan**: [`sealer::recover_fast`]'s
//!   `end_pos`/`batch_count`, whichever path it took (trusted trailer or
//!   fallback scan), always equals the independent [`recover_segment`] call
//!   — the A12 discipline (a fast path may only seed-and-skip, never accept
//!   what a scan would reject).
//! - **Idempotent re-recovery**: scanning the same durable image twice
//!   yields identical [`Recovery`] values.
//! - **Contiguous chain**: the next segment's `base_pos`/`epoch` are exactly
//!   the previous segment's recovered `next_pos` / a strictly larger epoch —
//!   whole-store continuity across the crash.
//!
//! # Determinism
//!
//! Everything — the scenario plan, the sim scheduler's interleaving choices,
//! and the seeded crash plans — is drawn from one `u64` seed through either
//! [`Rng::new`] directly or [`SimRuntime::with_rng`] (the SAME seeded stream
//! the scheduler itself consumes). A seed therefore reproduces a
//! byte-identical run end to end; `dst_scenarios.rs`'s
//! `same_seed_is_byte_identical` proves it by comparing two full
//! [`Trace`]s (which record a CRC32C of every segment's final on-disk image,
//! not just summary counts).
//!
//! # Deliberately out of scope (documented, not silently dropped)
//!
//! - **Scenario shrinking.** A failing seed reprints and re-runs
//!   byte-identically (this module's whole determinism story), which is
//!   enough to debug a failure by hand; automatically minimizing the FAILING
//!   seed's scenario plan (fewer segments/batches while preserving the
//!   failure) is real, valuable follow-up work this bone does not attempt.
//! - **True cross-segment overlap.** Segments in a chain run strictly
//!   sequentially in this driver (segment `i+1`'s `Committer`/writer is not
//!   constructed until segment `i`'s driver future — including its seal —
//!   has resolved). Readers race a segment's own seal (the seal-race the
//!   bone names), but "segment N's seal barrier literally overlaps segment
//!   N+1's first append" is not modeled; doing that soundly means spawning
//!   whole per-segment drivers as independent tasks with `Send` plumbing
//!   throughout, a materially bigger lift than this bone's `m` size affords.
//! - **`model.rs` as a numeric oracle here.** [`mess_log::model`] is a
//!   byte-format-optional abstraction (slots of 3 independently-persisted
//!   parts) already exhaustively checked against the production acceptance
//!   kernel by `tests/stateright.rs`. Wiring it as a second oracle for THIS
//!   harness's concrete byte-level scenarios would need translating
//!   `SimFs`'s actual per-sector pending/fate bookkeeping (private to
//!   `sim_fs.rs`, not part of the public `Fs` seam) into the model's
//!   header/body/marker `PartFate`s — a nontrivial, easy-to-get-subtly-wrong
//!   mapping for a property (`acked ⟹ recovered`) this module already checks
//!   directly against the real scanner. What DOES transfer, and is used
//!   here: the exact PROPERTY `model.rs`'s `State::acked` field encodes
//!   ("an ack is a promise") is the same property `assert_acked_implies_recovered`
//!   below checks — model.rs proved it holds for the abstract kernel across
//!   every reachable interleaving within its bounds; this harness checks it
//!   holds for the concrete bytes across REAL concurrent schedules the model
//!   does not represent (timing, subscriber races, multi-actor composition).
//!
//! [`docs/spec/01-log-format.md`]: ../../../../../docs/spec/01-log-format.md

#![allow(dead_code)]

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mess_log::committer::{
    AppendError, AppendOutcome, AppendRequest, Committer, Durability, EventInput,
};
use mess_log::encode::Subframe;
use mess_log::format::SEGMENT_HEADER_LEN;
use mess_log::reader::ReadView;
use mess_log::runtime::{Clock, EnospcSite, Fault, Fs, Rng, Runtime, SimFs, SimRuntime};
use mess_log::scanner::{recover_segment, Recovery};
use mess_log::sealer::{recover_fast, FastRecovery};
use mess_log::watermark::Watermark;
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter, WriteError};

// ===========================================================================
// Scenario plan derivation (everything from one seed)
// ===========================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegKind {
    /// Driven by the real production [`Committer`].
    Committer,
    /// Driven directly by the scenario, ending in an explicit
    /// [`SegmentWriter::seal`] — the sealer stand-in.
    Sealed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPhase {
    /// During the append/sync (or committer group-commit) phase.
    AppendSync,
    /// During the seal barrier (`Sealed` segments only).
    Seal,
}

#[derive(Debug, Clone, Copy)]
pub struct EnospcFault {
    pub site: EnospcSite,
    pub phase: FaultPhase,
}

#[derive(Debug, Clone)]
pub struct PlannedBatch {
    pub stream_id: u64,
    pub first_stream_version: u64,
    pub events: Vec<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct SegPlan {
    pub kind: SegKind,
    pub durability: Durability,
    /// Number of concurrent appenders (`Committer` kind only; `1` for `Sealed`).
    pub writers: u64,
    /// Per-writer batch sequence for `Committer`; `batches[0]` is the whole
    /// flat sequence for `Sealed`.
    pub batches: Vec<Vec<PlannedBatch>>,
    pub n_readers: u64,
    pub reader_lag_max_us: u64,
    pub reader_seed_base: u64,
    /// `Sealed` kind only: sync after every `sync_every` batches.
    pub sync_every: usize,
    pub enospc: Option<EnospcFault>,
    /// Seeded tear probability for the always-on end-of-segment
    /// `crash_random` pass (torn sectors).
    pub tear_prob: f64,
    pub writer_seed: u64,
}

#[derive(Debug, Clone)]
pub struct ScenarioPlan {
    pub seed: u64,
    pub segments: Vec<SegPlan>,
}

fn random_events(rng: &mut Rng, max_events: u64, max_len: u64) -> Vec<Vec<u8>> {
    let n = 1 + rng.below(max_events.max(1));
    (0..n)
        .map(|_| {
            let len = rng.below(max_len + 1);
            (0..len).map(|_| rng.below(256) as u8).collect()
        })
        .collect()
}

fn plan_segment(rng: &mut Rng, seg_idx: u64) -> SegPlan {
    let kind = if rng.bool() { SegKind::Committer } else { SegKind::Sealed };
    let durability = match rng.below(3) {
        0 => Durability::Process,
        1 => Durability::Os,
        _ => Durability::group_default(),
    };
    let writers = if kind == SegKind::Committer { 1 + rng.below(3) } else { 1 };
    let batches_per = 1 + rng.below(4); // 1..=4

    let mut batches: Vec<Vec<PlannedBatch>> = Vec::new();
    for w in 0..writers {
        let mut version = 0u64;
        let mut seq = Vec::new();
        for _ in 0..batches_per {
            let events = random_events(rng, 3, 48);
            seq.push(PlannedBatch { stream_id: 10 * seg_idx + w, first_stream_version: version, events: events.clone() });
            version += events.len() as u64;
        }
        batches.push(seq);
    }

    let n_readers = rng.below(4); // 0..=3
    let reader_lag_max_us = [0u64, 5, 50, 500][rng.below(4) as usize];

    let enospc = if rng.chance(0.35) {
        let site = match rng.below(2) {
            0 => EnospcSite::Pwrite,
            _ => EnospcSite::Fdatasync,
        };
        let phase = if kind == SegKind::Sealed && rng.bool() { FaultPhase::Seal } else { FaultPhase::AppendSync };
        Some(EnospcFault { site, phase })
    } else {
        None
    };

    let tear_prob = (rng.below(1_000_001) as f64) / 1_000_000.0 * 0.5; // 0..=0.5

    SegPlan {
        kind,
        durability,
        writers,
        batches,
        n_readers,
        reader_lag_max_us,
        reader_seed_base: rng.next_u64(),
        sync_every: 1 + rng.below(3) as usize,
        enospc,
        tear_prob,
        writer_seed: rng.next_u64(),
    }
}

pub fn plan_scenario(seed: u64) -> ScenarioPlan {
    let mut rng = Rng::new(seed);
    let n_segments = 2 + rng.below(3); // 2..=4
    let segments = (0..n_segments).map(|i| plan_segment(&mut rng, i)).collect();
    ScenarioPlan { seed, segments }
}

// ===========================================================================
// Trace — the byte-identical-replay evidence
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceEvent {
    SegmentDone {
        seg_idx: usize,
        recovered_next_pos: u64,
        recovered_batches: u64,
        recovered_epoch: u64,
        image_crc32c: u32,
        image_len: u64,
        acked_end: u64,
        max_reader_seen: u64,
        sealed_via_r2: bool,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Trace {
    pub events: Vec<TraceEvent>,
}

// ===========================================================================
// Reader actor: subscribe-and-relag, recording the max position it ever saw
// as "committed"
// ===========================================================================

/// Resolve as soon as EITHER the watermark passes `at_least`, OR `cap`
/// virtual time elapses — whichever comes first. This is what gives a
/// reader real subscriber-lag flavor (it re-reads no sooner than roughly
/// `cap` after its last read) WITHOUT ever busy-polling: a bare
/// `rt.sleep(1ns)` loop re-wakes every virtual nanosecond even while the
/// writer is legitimately waiting out a much longer barrier (e.g. `Group`
/// mode's up-to-`max_delay` gather window) — millions of pointless
/// `recover_segment` re-scans for a wait that is really microseconds of
/// WALL time, just expressed in nanosecond ticks. Racing a bounded
/// `Watermark::wait_for` against a capped sleep bounds every idle cycle to
/// `cap`, while still waking IMMEDIATELY on real progress.
async fn wait_progress_or_cap(wm: &Watermark, at_least: u64, rt: &SimRuntime, cap: Duration) {
    let deadline = rt.now().saturating_add(cap);
    let mut wait_fut = std::pin::pin!(wm.wait_for(at_least));
    let mut sleep_fut = std::pin::pin!(rt.sleep_until(deadline));
    std::future::poll_fn(move |cx| {
        if wait_fut.as_mut().poll(cx).is_ready() {
            return std::task::Poll::Ready(());
        }
        if sleep_fut.as_mut().poll(cx).is_ready() {
            return std::task::Poll::Ready(());
        }
        std::task::Poll::Pending
    })
    .await
}

async fn reader_loop(
    view: ReadView<SimFs>,
    rt: SimRuntime,
    lag_max_us: u64,
    seed: u64,
    done: Arc<AtomicBool>,
    max_seen: Arc<AtomicU64>,
) {
    let mut rng = Rng::new(seed);
    let wm = view.watermark();
    loop {
        let mut last_watermark = 0u64;
        if let Ok(p) = view.read_committed() {
            assert!(
                p.next_pos() <= p.watermark,
                "reader observed past its own watermark snapshot: next_pos={} watermark={}",
                p.next_pos(),
                p.watermark
            );
            max_seen.fetch_max(p.next_pos(), Ordering::SeqCst);
            last_watermark = p.watermark;
        }
        if done.load(Ordering::Acquire) {
            // One final read after the writer signals done, to catch the
            // segment's true final state.
            if let Ok(p) = view.read_committed() {
                max_seen.fetch_max(p.next_pos(), Ordering::SeqCst);
            }
            break;
        }
        let cap_us = if lag_max_us > 0 { 1 + rng.below(lag_max_us) } else { 1 };
        wait_progress_or_cap(&wm, last_watermark + 1, &rt, Duration::from_micros(cap_us)).await;
    }
}

// The lifetime bound mirrors `Runtime::spawn`'s own RPITIT, whose returned
// future's opaque type is conservatively tied to the `&self` call site
// (even though the `SimJoin` it actually returns owns everything it needs) —
// so this cannot be `'static` when called through a borrowed `&SimRuntime`.
// Every reader is `.await`ed before the borrow ends, so a non-`'static`
// bound costs nothing here.
type BoxedFuture<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;

fn spawn_readers<'a>(
    rt: &'a SimRuntime,
    fs: &SimFs,
    path: &Path,
    watermark: &Watermark,
    plan: &SegPlan,
    done: &Arc<AtomicBool>,
    max_seen: &Arc<AtomicU64>,
) -> Vec<BoxedFuture<'a>> {
    let mut readers: Vec<BoxedFuture<'a>> = Vec::new();
    for r in 0..plan.n_readers {
        let view = ReadView::new(fs.clone(), path.to_path_buf(), watermark.clone());
        let rt2 = rt.clone();
        let done2 = done.clone();
        let max2 = max_seen.clone();
        let seed = plan.reader_seed_base.wrapping_add(r);
        let lag = plan.reader_lag_max_us;
        readers.push(Box::pin(rt.spawn(reader_loop(view, rt2, lag, seed, done2, max2))));
    }
    readers
}

fn to_request(pb: &PlannedBatch) -> AppendRequest {
    AppendRequest {
        stream_id: pb.stream_id,
        category_id: 1000 + pb.stream_id,
        first_stream_version: pb.first_stream_version,
        events: pb.events.iter().map(|p| EventInput::plain(1, 1, 0, p.clone())).collect(),
    }
}

// ===========================================================================
// Segment outcome (feeds the invariant checks + the chain's next base_pos)
// ===========================================================================

struct SegOutcome {
    /// End of the durably-acknowledged prefix this segment produced, per its
    /// Durability contract (`base_pos + every guaranteed-acked event`).
    acked_end: u64,
    /// Whether THIS segment's durability mode guarantees `acked_end`/
    /// `max_reader_seen` survive a crash: true for `Os`/`Group` (barrier-
    /// backed) and for a raw `Sealed`-kind writer (every advance here is
    /// gated on a real `sync()` `Ok`). **False for `Durability::Process`**:
    /// by design (§1.1 of `03-durability.md`, and `watermark.rs`'s own
    /// documented "`Durability::Process`'s crash-visibility hazard") its
    /// watermark advances the instant bytes are `pwrite`n, with NO barrier
    /// at all — a reader can legitimately be shown a position that the
    /// harness's own always-on tear pass then legitimately takes back. This
    /// flag is what keeps the acked-implies-recovered AND the
    /// reader-never-past-watermark checks honest about that documented
    /// exception, rather than papering over it.
    durability_guaranteed: bool,
    max_reader_seen: u64,
    /// Whether this segment ended sealed with no fault having fired.
    attempted_seal_cleanly: bool,
}

// ===========================================================================
// Committer-kind segment
// ===========================================================================

async fn run_committer_segment(
    rt: &SimRuntime,
    fs: &SimFs,
    path: &Path,
    params: SegmentParams,
    plan: &SegPlan,
) -> SegOutcome {
    let writer = SegmentWriter::create(fs, path, params)
        .unwrap_or_else(|e| panic!("segment create failed: {e}"));
    let c = Committer::spawn(rt, writer, plan.durability);

    let done = Arc::new(AtomicBool::new(false));
    let max_seen = Arc::new(AtomicU64::new(params.base_pos));
    let readers = spawn_readers(rt, fs, path, &c.watermark(), plan, &done, &max_seen);

    if let Some(fault) = &plan.enospc {
        // Fires on the first matching op reached from here on (best-effort
        // "somewhere mid-segment"; precise per-site targeting is enospc.rs's
        // job, not this harness's).
        fs.inject_enospc(path, fault.site);
    }

    let acked: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let mut joins = Vec::new();
    for w in 0..plan.writers as usize {
        let ap = c.appender();
        let batches = plan.batches[w].clone();
        let acked2 = acked.clone();
        joins.push(rt.spawn(async move {
            for pb in &batches {
                match ap.append(to_request(pb)).await {
                    Ok(AppendOutcome::Acked { first_position, last_position }) => {
                        acked2.lock().unwrap().push((first_position, last_position));
                    }
                    Ok(AppendOutcome::Indeterminate) => {}
                    Err(AppendError::StorePoisoned) | Err(AppendError::StoreFull) => {}
                    Err(e) => panic!("valid batch pre-flight rejected: {e}"),
                }
            }
        }));
    }
    for j in joins {
        j.await;
    }
    done.store(true, Ordering::Release);
    for r in readers {
        r.await;
    }
    c.shutdown().await;

    let mut acked = Arc::try_unwrap(acked).unwrap().into_inner().unwrap();
    acked.sort_unstable();
    let mut expect = params.base_pos;
    for &(first, _last) in &acked {
        assert_eq!(first, expect, "acked positions must be a dense prefix");
        expect = _last + 1;
    }
    let acked_end = expect;
    let durability_guaranteed = matches!(plan.durability, Durability::Os | Durability::Group { .. });

    SegOutcome {
        acked_end,
        durability_guaranteed,
        max_reader_seen: max_seen.load(Ordering::Acquire),
        attempted_seal_cleanly: false,
    }
}

// ===========================================================================
// Sealed-kind segment: the harness keeps ownership of the writer so it can
// call SegmentWriter::seal itself — the sealer stand-in.
// ===========================================================================

async fn run_sealed_segment(
    rt: &SimRuntime,
    fs: &SimFs,
    path: &Path,
    params: SegmentParams,
    plan: &SegPlan,
) -> SegOutcome {
    let mut writer = SegmentWriter::create(fs, path, params)
        .unwrap_or_else(|e| panic!("segment create failed: {e}"));
    let wm = Watermark::new(writer.next_pos());

    let done = Arc::new(AtomicBool::new(false));
    let max_seen = Arc::new(AtomicU64::new(params.base_pos));
    let readers = spawn_readers(rt, fs, path, &wm, plan, &done, &max_seen);

    if let Some(fault) = &plan.enospc
        && fault.phase == FaultPhase::AppendSync
    {
        fs.inject_enospc(path, fault.site);
    }

    let flat = &plan.batches[0];
    let mut rngw = Rng::new(plan.writer_seed);
    let mut acked_end = params.base_pos;
    let mut barrier_failed = false;
    for (i, pb) in flat.iter().enumerate() {
        let subs: Vec<Subframe> = pb.events.iter().map(|p| Subframe::plain(1, 1, 0, p)).collect();
        let spec = BatchSpec {
            stream_id: pb.stream_id,
            category_id: 1000 + pb.stream_id,
            first_stream_version: pb.first_stream_version,
            crypto_chain: None,
            subframes: &subs,
        };
        match writer.append(&spec) {
            Ok(_) => {}
            // StorePoisoned: a prior barrier failed. Io (typically the
            // injected ENOSPC-at-Pwrite fault, `bn-36y`'s "adversarial"
            // site — with preallocation a real pwrite should not fail
            // mid-commit, but the store must still refuse to corrupt if it
            // does): the batch never landed. Either way, stop appending —
            // this segment's durable prefix is exactly what already synced.
            Err(WriteError::StorePoisoned) | Err(WriteError::Io(_)) => {
                barrier_failed = true;
                break;
            }
            Err(e) => panic!("valid batch pre-flight rejected: {e}"),
        }
        let is_last = i + 1 == flat.len();
        if (i + 1) % plan.sync_every == 0 || is_last {
            match writer.sync() {
                Ok(()) => {
                    wm.advance(writer.next_pos());
                    acked_end = writer.next_pos();
                }
                Err(_) => {
                    barrier_failed = true;
                    break;
                }
            }
        }
        rt.sleep(Duration::from_nanos(1 + rngw.below(50))).await;
    }

    let mut attempted_seal_cleanly = false;
    if !barrier_failed && !writer.is_poisoned() {
        if let Some(fault) = &plan.enospc
            && fault.phase == FaultPhase::Seal
        {
            fs.inject_enospc(path, fault.site);
        }
        rt.sleep(Duration::from_nanos(1)).await;
        let had_fault = plan.enospc.as_ref().is_some_and(|f| f.phase == FaultPhase::Seal);
        if writer.seal().is_ok() && !had_fault {
            attempted_seal_cleanly = true;
        }
    }

    done.store(true, Ordering::Release);
    for r in readers {
        r.await;
    }

    SegOutcome {
        acked_end,
        durability_guaranteed: true, // every counted position had a real sync() Ok
        max_reader_seen: max_seen.load(Ordering::Acquire),
        attempted_seal_cleanly,
    }
}

// ===========================================================================
// crc32c of a segment's current on-disk bytes (the trace's byte-identity
// evidence)
// ===========================================================================

fn image_bytes(fs: &SimFs, path: &Path) -> Vec<u8> {
    use mess_log::runtime::{FileHandle, OpenOpts};
    let f = fs.open(path, OpenOpts::read_only()).unwrap();
    let len = f.len().unwrap() as usize;
    let mut buf = vec![0u8; len];
    let mut filled = 0;
    while filled < len {
        let n = f.pread(filled as u64, &mut buf[filled..]).unwrap();
        if n == 0 {
            break;
        }
        filled += n;
    }
    buf.truncate(filled);
    buf
}

// ===========================================================================
// The full scenario: chain the segments, checking every invariant
// ===========================================================================

/// Run ONE segment of the chain: the append/seal phase, the always-on tear
/// pass, and every per-segment invariant. Returns its [`TraceEvent`] plus the
/// `(base_pos, epoch)` the NEXT segment in a chain must continue from.
/// Shared by [`run_scenario`] (a full multi-segment chain) and
/// [`run_single_segment_scenario`] (one explicit, hand-built [`SegPlan`] —
/// the dedicated composed scenario class).
#[allow(clippy::too_many_arguments)] // one (rt, fs) pair + one segment's full identity; splitting hurts readability more than it helps
fn run_one_segment(
    rt: &SimRuntime,
    fs: &SimFs,
    seed: u64,
    seg_idx: usize,
    seg: &SegPlan,
    base_pos: u64,
    epoch: u64,
    prev_epoch: u64,
) -> (TraceEvent, u64, u64) {
    let path = PathBuf::from(format!("/seg-{seg_idx}"));
    let params = SegmentParams {
        segment_id: seg_idx as u64,
        base_pos,
        epoch,
        prev_segment_epoch: prev_epoch,
        created_unix_nanos: 0,
        segment_size: 4 * 1024 * 1024, // small: cheap in-memory images, still plenty of room
    };

    let outcome = rt.block_on(async {
        match seg.kind {
            SegKind::Committer => run_committer_segment(rt, fs, &path, params, seg).await,
            SegKind::Sealed => run_sealed_segment(rt, fs, &path, params, seg).await,
        }
    });

    // Always-on end-of-segment tear pass: anything pending (never covered
    // by a returning fdatasync) is torn exactly as a real crash would leave
    // it. A clean segment has nothing pending: a no-op.
    rt.with_rng(|rng| fs.crash_random(&path, rng, seg.tear_prob))
        .unwrap_or_else(|e| panic!("seed {seed} seg {seg_idx}: crash_random failed: {e}"));

    let rec = recover_segment(fs, &path)
        .unwrap_or_else(|e| panic!("seed {seed} seg {seg_idx}: recover failed: {e}"));
    assert_recovery_wellformed(seed, seg_idx, &rec, base_pos, epoch);

    // acked ⟹ recovered, per the durability contract. Durability::Process
    // is explicitly excluded (see SegOutcome::durability_guaranteed): its
    // watermark advances with NO barrier at all, by design (§1.1).
    if outcome.durability_guaranteed {
        assert!(
            rec.next_pos >= outcome.acked_end,
            "seed {seed} seg {seg_idx}: LOST ACKED DATA: recovered next_pos={} < acked_end={}",
            rec.next_pos,
            outcome.acked_end
        );
    }

    // reader-never-past-watermark, globally: anything a live reader was
    // ever shown as "committed" must actually be recoverable — again scoped
    // to durability modes that make that promise (see
    // SegOutcome::durability_guaranteed).
    if outcome.durability_guaranteed {
        assert!(
            rec.next_pos >= outcome.max_reader_seen,
            "seed {seed} seg {seg_idx}: a reader observed position {} as committed, \
             but recovery only reconstructed up to {}",
            outcome.max_reader_seen,
            rec.next_pos
        );
    }

    // R2 (sealer) agrees with the authoritative full scan, whichever path
    // it took.
    let fr = recover_fast(fs, &path)
        .unwrap_or_else(|e| panic!("seed {seed} seg {seg_idx}: recover_fast failed: {e}"));
    assert_eq!(
        fr.end_pos(),
        rec.next_pos,
        "seed {seed} seg {seg_idx}: R2 end_pos disagrees with the full scan"
    );
    assert_eq!(
        fr.batch_count(),
        rec.accepted.len() as u64,
        "seed {seed} seg {seg_idx}: R2 batch_count disagrees with the full scan"
    );
    let sealed_via_r2 = matches!(fr, FastRecovery::Sealed { .. });
    if seg.kind == SegKind::Sealed && outcome.attempted_seal_cleanly {
        assert!(
            sealed_via_r2,
            "seed {seed} seg {seg_idx}: a cleanly-sealed segment with no injected \
             fault must be trusted via the R2 fast path"
        );
    }
    if seg.kind == SegKind::Committer {
        assert!(
            !sealed_via_r2,
            "seed {seed} seg {seg_idx}: a Committer-kind segment is never sealed; \
             R2 must always fall back to a full scan"
        );
    }

    // Idempotent re-recovery.
    let rec2 = recover_segment(fs, &path)
        .unwrap_or_else(|e| panic!("seed {seed} seg {seg_idx}: re-recover failed: {e}"));
    assert_eq!(rec, rec2, "seed {seed} seg {seg_idx}: re-recovery not idempotent");

    let img = image_bytes(fs, &path);
    let image_crc32c = crc32c::crc32c(&img);

    let event = TraceEvent::SegmentDone {
        seg_idx,
        recovered_next_pos: rec.next_pos,
        recovered_batches: rec.accepted.len() as u64,
        recovered_epoch: rec.header.map_or(0, |h| h.epoch),
        image_crc32c,
        image_len: img.len() as u64,
        acked_end: outcome.acked_end,
        max_reader_seen: outcome.max_reader_seen,
        sealed_via_r2,
    };
    // Chain: the next segment continues from exactly what recovery proved
    // durable (safe regardless of whether THIS segment's seal succeeded,
    // failed, or was torn — sealing never changes which batches are
    // durable, only whether the trailer is trustworthy).
    (event, rec.next_pos, epoch + 1)
}

/// Run one whole-store scenario end to end. Panics (with the seed and the
/// offending segment index in the message) on any invariant violation.
/// Returns the [`Trace`] — every seed's run produces a byte-identical trace
/// (`dst_scenarios.rs::same_seed_is_byte_identical` proves it).
pub fn run_scenario(seed: u64) -> Trace {
    let plan = plan_scenario(seed);
    let rt = SimRuntime::with_fault(seed, Fault::SECTOR_512);
    let fs = rt.fs();
    let mut trace = Trace::default();

    let mut base_pos = 0u64;
    let mut epoch = 1u64;
    let mut prev_epoch = 0u64;

    for (seg_idx, seg) in plan.segments.iter().enumerate() {
        let (event, next_base, next_epoch) =
            run_one_segment(&rt, &fs, seed, seg_idx, seg, base_pos, epoch, prev_epoch);
        trace.events.push(event);
        prev_epoch = epoch;
        base_pos = next_base;
        epoch = next_epoch;
    }

    trace
}

/// Run a SINGLE, explicitly hand-built [`SegPlan`] as a one-segment
/// "scenario" — for tests that want precise control over the composition
/// (e.g. `crash_lag_seal_race_composed`) rather than the general seeded
/// planner's random mix. `seed` is used only for the fault-fs's own
/// scheduling/crash-plan randomness (`SimRuntime::with_fault`), NOT to
/// derive the plan.
pub fn run_single_segment_scenario(seed: u64, seg: SegPlan) -> Trace {
    let rt = SimRuntime::with_fault(seed, Fault::SECTOR_512);
    let fs = rt.fs();
    let (event, _, _) = run_one_segment(&rt, &fs, seed, 0, &seg, 0, 1, 0);
    Trace { events: vec![event] }
}

/// Structural invariants that hold for EVERY recovery in the chain: a valid
/// header seeded from `base_pos`/`epoch`, a densely contiguous accepted
/// prefix, `safe_offset` exactly past the accepted bytes.
fn assert_recovery_wellformed(seed: u64, seg_idx: usize, rec: &Recovery, base_pos: u64, epoch: u64) {
    let header = rec
        .header
        .unwrap_or_else(|| panic!("seed {seed} seg {seg_idx}: durable SegmentHeader must always survive"));
    assert_eq!(header.base_pos, base_pos, "seed {seed} seg {seg_idx}: header base_pos");
    assert_eq!(header.epoch, epoch, "seed {seed} seg {seg_idx}: header epoch");

    let mut expect_pos = base_pos;
    let mut off = SEGMENT_HEADER_LEN as u64;
    for b in &rec.accepted {
        assert_eq!(b.first_global_pos, expect_pos, "seed {seed} seg {seg_idx}: not position-contiguous");
        assert_eq!(b.offset, off, "seed {seed} seg {seg_idx}: not byte-contiguous");
        assert_eq!(b.segment_epoch, epoch, "seed {seed} seg {seg_idx}: wrong epoch");
        assert!(b.frame_count >= 1, "seed {seed} seg {seg_idx}: empty batch accepted (A5)");
        expect_pos += u64::from(b.frame_count);
        off += b.total_len;
    }
    assert_eq!(rec.next_pos, expect_pos, "seed {seed} seg {seg_idx}: next_pos != contiguous end");
    assert_eq!(rec.safe_offset, off, "seed {seed} seg {seg_idx}: safe_offset != end of accepted bytes");
}
