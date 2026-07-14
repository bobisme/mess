//! The committer (`bn-11m`): the single thread, per store, that gathers a
//! commit group, assigns positions centrally, issues one coalesced write
//! and one durability barrier, advances the position-ordered durable
//! watermark ([`crate::watermark`]), and acks — exactly the round-4 design
//! of [`docs/spec/03-durability.md`] §2.1.
//!
//! # Where this sits
//!
//! It **consumes** [`SegmentWriter`](crate::writer) (the canonical write
//! path, D1) and [`BatchEncoder`](crate::encode) (length/validation) and
//! is generic over the runtime seam ([`Runtime`](crate::runtime)) so the
//! same code runs on [`SimRuntime`](crate::runtime::SimRuntime) for
//! deterministic tests and on [`RealRuntime`](crate::runtime::RealRuntime)
//! for the perf numbers §2.3 targets. Appenders hand encoded intent to the
//! committer over a single channel (the §2.4 centralized gather point that
//! closes the convoy-split race by construction) and `await` their ack;
//! **the committer alone owns the `fdatasync`** — no appender, and no lock
//! an appender needs to gather into the *next* group, is ever held across
//! the barrier (§2.5).
//!
//! # Durability modes (§1)
//!
//! | [`Durability`] | coalesce | barrier | ack meaning |
//! |---|---|---|---|
//! | `Process` | no | none | `write(2)` accepted into page cache; watermark advances immediately. Loss window: unbounded, OS-writeback-governed (§1.1). |
//! | `Os` | no (group of one) | one `fdatasync` per batch | barrier returned; degrades to sync-per-batch (§1.2) — this is the baseline `Group` must beat. |
//! | `Group{max_delay,max_bytes}` | yes (early-close) | one `fdatasync` per group | covering group barrier returned AND watermark advanced past this batch (§1.3); bit-for-bit `Os` strength, amortized. |
//!
//! # Deviation from §2.1 step 3 (recorded)
//!
//! §2.1 step 3 describes "ONE coalesced write syscall for the whole
//! group." This committer builds on [`SegmentWriter::append`], which does
//! one positioned write per batch, so a group of `k` batches issues `k`
//! `pwrite`s followed by one barrier. This is **durability-equivalent**:
//! every one of those writes lands in the page cache strictly before the
//! single covering `fdatasync`, exactly as a coalesced write would, and
//! the barrier — not the `pwrite` count — is what the throughput of §2.3
//! amortizes (a small-event `pwrite` into page cache is ~1 µs; the barrier
//! is milliseconds). A coalesced-write API on `SegmentWriter` would let
//! this match the letter of step 3 and shave the residual per-`pwrite`
//! cost; see the crate's open items.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use smallvec::SmallVec;

use crate::degraded::{Degraded, PoisonCause};
pub use crate::degraded::{
    Degraded as StoreDegraded, PoisonCause as BarrierPoisonCause,
};
use crate::encode::{
    BatchEncoder, BatchInput, EncodeError, PreparedBatch, Subframe,
    SubframeError,
};
use crate::fold_chain::ChainHead;
use crate::runtime::{Fs, Runtime};
use crate::watermark::Watermark;
pub use crate::watermark::Watermark as DurableWatermark;
use crate::writer::{
    BatchSpec, PreparedBatchSpec, SegmentSummary, SegmentWriter, WriteError,
};

// ---------------------------------------------------------------------------
// Public config + request/outcome types
// ---------------------------------------------------------------------------

/// The durability mode of a store (`docs/spec/03-durability.md` §1). The
/// spec's canonical name for the weakest mode is `Process` (survives a
/// process crash only); it is the same mode the bone brief calls
/// "Buffered."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Ack the moment the covering `write(2)` returns; no barrier. Watermark
    /// advances immediately. Loss window unbounded, OS-governed (§1.1).
    Process,
    /// Ack after the batch's own `fdatasync` returns. Every append is its
    /// own group of one — sync-per-batch (§1.2).
    Os,
    /// Ack after the covering group's single `fdatasync` returns and the
    /// watermark advances past the batch (§1.3). Early-close group commit.
    Group {
        /// A **cap** on how long the gather window may stay open — never a
        /// fixed sleep (§2.2). The window closes on the *first* of
        /// early-close, `max_bytes`, or this cap.
        max_delay: Duration,
        /// The coalesced group's accumulated byte size at which the window
        /// closes regardless of `max_delay` (§2.2).
        max_bytes: u64,
    },
}

impl Durability {
    /// The spec's fixed defaults for `Group` (§2.2): `max_delay = 1 ms`,
    /// `max_bytes = 8 MiB` (≈5× the measured knee).
    pub fn group_default() -> Self {
        Durability::Group {
            max_delay: Duration::from_millis(1),
            max_bytes: 8 * 1024 * 1024,
        }
    }
}

/// Append-side fold-chain configuration for a committer (`bn-3l0`, spec 05 §6).
///
/// Opt-in and **off by default**: a committer built with [`ChainInit::off`]
/// never sets `flags.CRYPTO_CHAIN` and writes byte-for-byte the same bytes it
/// did before this feature existed. When [`enabled`](Self::enabled) is set, the
/// committer maintains one running [`ChainHead`] per stream (G10 per-batch
/// chain): before each batch it stamps the head's current value (`h[base-1]`)
/// into the batch's `crypto_chain` slot (offset
/// [`HEADER_LEN`](crate::format::HEADER_LEN), §4.4), then folds the batch's
/// on-disk payloads into the head so the next batch of that stream continues
/// the chain.
///
/// `heads` seeds the per-stream state on open: fresh stores pass an empty map
/// (every stream starts at [`ChainHead::genesis`] on first sight); recovery
/// rehydrates it by folding the recovered frames (spec 05 §5), so an append
/// after crash-reopen continues each stream's chain exactly where the durable
/// prefix left off.
#[derive(Debug, Clone, Default)]
pub struct ChainInit {
    /// Whether to materialize `crypto_chain` into each batch. `false` keeps
    /// the on-disk bytes byte-identical to a non-chained store.
    pub enabled: bool,
    /// Per-stream resumed heads (`stream_id → head`), from recovery. A stream
    /// absent here starts at its [`ChainHead::genesis`] the first time it is
    /// appended.
    pub heads:   HashMap<u64, ChainHead>,
}

impl ChainInit {
    /// Chain disabled — the default. On-disk bytes stay identical to a store
    /// that never knew about the fold chain.
    #[must_use]
    pub fn off() -> Self { ChainInit { enabled: false, heads: HashMap::new() } }

    /// Chain enabled, seeded with `heads` (empty for a fresh store, or the
    /// per-stream heads rehydrated by recovery).
    #[must_use]
    pub fn on(heads: HashMap<u64, ChainHead>) -> Self {
        ChainInit { enabled: true, heads }
    }
}

/// One event to append, owned so it can cross the gather channel to the
/// committer thread (§2.1 step 1: the writer hands encoded intent over).
/// Mirrors the fields of [`Subframe`] for the common uncompressed shape.
#[derive(Debug, Clone)]
pub struct EventInput {
    /// Interned event type id (§4.3).
    pub event_type_id:  u32,
    /// Schema version at write time.
    pub schema_version: u16,
    /// Interned payload codec id (`0` = bootstrap).
    pub codec_id:       u16,
    /// The uncompressed, verbatim payload bytes.
    pub payload:        Vec<u8>,
}

impl EventInput {
    /// A plain uncompressed event carrying `payload`.
    pub fn plain(
        event_type_id: u32,
        schema_version: u16,
        codec_id: u16,
        payload: Vec<u8>,
    ) -> Self {
        EventInput { event_type_id, schema_version, codec_id, payload }
    }
}

/// One batch handed to [`Committer::append`]. Single-stream (D-FMT-6). The
/// committer stamps `segment_epoch` / `batch_id` / `first_global_pos`
/// centrally (§2.1 step 2).
#[derive(Debug, Clone)]
pub struct AppendRequest {
    /// Batch-constant stream id.
    pub stream_id:            u64,
    /// Batch-constant category id.
    pub category_id:          u64,
    /// Stream version of this batch's first event (§4.2).
    pub first_stream_version: u64,
    /// The events, in order. A5: MUST be non-empty.
    pub events:               Vec<EventInput>,
}

/// The durability outcome of an [`append`](Committer::append), per
/// `docs/spec/03-durability.md` §7.2. Distinguishes a positive ack from
/// every other state — there is no silent "probably worked."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendOutcome {
    /// The watermark has passed this batch's positions under the store's
    /// configured [`Durability`] mode. Unconditional.
    Acked {
        /// Global position of the batch's first event (A1).
        first_position: u64,
        /// Global position of the batch's last event.
        last_position:  u64,
        /// The segment the batch was durably written into (bn-2ib). With live
        /// auto-roll a batch can land in a segment the caller never named, so
        /// the ack carries the real placement — the caller's index entry
        /// (`mess-index`'s `EventPtr` shape) can then point at the actual
        /// bytes instead of a pseudo location.
        segment_id:     u64,
        /// Byte offset of the batch within that segment (the writer
        /// [`Receipt`](crate::writer::Receipt)'s `offset`).
        offset:         u64,
    },
    /// The barrier that would cover this batch did not complete (an
    /// `fdatasync` fault, or the crash window §6/§7): the batch MAY still
    /// be durable and MAY be accepted by a later recovery scan (A6), but no
    /// ack was earned here. The caller MUST retry with the same dedupe key
    /// (the full `DedupeKey` of §7.2 is modelled by the client/dedupe bone;
    /// this committer surfaces the *condition*).
    Indeterminate,
}

/// A direct-owner result paired with the input events it consumed.
///
/// Returning ownership lets a caller reuse the already-owned payloads for
/// publication caches after the write, instead of cloning a large batch solely
/// to keep those bytes alive across the commit.
pub enum DirectBatchEvents {
    Inputs(Vec<EventInput>),
    Prepared(PreparedBatch),
}

pub struct DirectBatchOutcome {
    pub outcome: Result<AppendOutcome, AppendError>,
    pub events:  DirectBatchEvents,
}

pub enum DirectAppendRequest {
    Inputs(AppendRequest),
    Prepared {
        stream_id:            u64,
        category_id:          u64,
        first_stream_version: u64,
        batch:                PreparedBatch,
    },
}

impl From<AppendRequest> for DirectAppendRequest {
    fn from(value: AppendRequest) -> Self { DirectAppendRequest::Inputs(value) }
}

/// A pre-flight rejection of an [`append`](Committer::append): detected
/// before the batch is durable, and distinct from an [`AppendOutcome`]
/// (which is a durability verdict on an accepted request).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AppendError {
    /// The batch is not encodable (A5 empty / A2 too large / bad subframe).
    #[error("encode: {0}")]
    Encode(#[from] EncodeError),
    /// A8: the batch would not fit the active segment. Segment roll is the
    /// sealer/allocator's job (Phase 4); this committer surfaces the fault.
    #[error("segment full: batch needs {needed} bytes, {remaining} remain")]
    SegmentFull {
        /// The batch's `total_len`.
        needed:    u64,
        /// Bytes left before `segment_size`.
        remaining: u64,
    },
    /// The committer has shut down (explicitly via
    /// [`Committer::shutdown`], or implicitly by dropping the
    /// [`Committer`], `bn-3da`); no more appends can be accepted. Surfaced
    /// both for a new append attempted after shutdown and for one already
    /// queued/racing in when shutdown was signalled and never gathered —
    /// callers get this typed error instead of hanging forever on an ack
    /// that will never be fulfilled.
    #[error("committer is closed")]
    Closed,
    /// The store is full: preallocating a new segment failed with `ENOSPC`
    /// (`bn-36y`, `WriteError::StoreFull`). The committed prefix stays durable
    /// and readable; disk-full struck at the single recoverable point (segment
    /// roll). Reached only through a roll — not the committer's own append path
    /// today — but surfaced here so the client boundary is typed.
    #[error("store full")]
    StoreFull,
    /// The store is poisoned: a prior durability barrier (`fdatasync`) failed —
    /// with `EIO`, `ENOSPC`, or anything else — so the segment's durable state
    /// is unknowable (the full D8 policy, `bn-25e`,
    /// `docs/spec/03-durability.md` §2.6; see [`crate::degraded`]). The barrier
    /// that tripped the poison surfaced its own group as
    /// [`AppendOutcome::Indeterminate`] and was **never retried** (retrying
    /// `fdatasync` after `EIO` is the classic fsyncgate corruption); every
    /// write after it — through *any* entry point — fails fast with this typed
    /// error. The poison is sticky for the committer's lifetime with no reset:
    /// **reopen and recover** ([`docs/spec/02-recovery.md`]) is the only exit.
    /// Reads remain permitted meanwhile, clamped to the now-frozen watermark
    /// (degraded reads); query [`Committer::is_degraded`] /
    /// [`Appender::is_degraded`] to detect the mode.
    #[error("store poisoned")]
    StorePoisoned,
    /// An **earlier batch of this ordered unit** ([`Appender::submit_ordered`])
    /// did not ack, so this batch was skipped and **never written** (`bn-2di`).
    ///
    /// `submit_ordered` gives a unit file ORDER; this error is what makes it
    /// also fail ATOMICALLY. Without it, a unit's first batch could be rejected
    /// (`SegmentFull` → roll → `StoreFull`) while a later batch of the SAME
    /// unit — which the earlier one was ordered ahead of *because it
    /// depends on it* — was accepted and durably committed. For
    /// `mess-store`'s `$registry` that is unrecoverable corruption: the
    /// domain batch would reference ids whose `*Registered` records are
    /// nowhere in the log.
    ///
    /// So: the first non-[`Acked`](AppendOutcome::Acked) outcome in a unit
    /// **poisons the rest of it**. Every later batch of the same unit is failed
    /// with this error, before any bytes are written and without consuming a
    /// global position. A caller seeing this knows the batch is definitively
    /// NOT in the log (unlike [`AppendOutcome::Indeterminate`]) and that the
    /// real cause is on an earlier batch of the same unit.
    #[error(
        "ordered unit aborted: an earlier batch of the unit did not commit"
    )]
    UnitAborted,
}

// ---------------------------------------------------------------------------
// Fsync metric (§2.6: barrier-latency metric MUST be exposed)
// ---------------------------------------------------------------------------

use crate::metrics::{Counter, DegradationAlarm, LatencyHistogram};
pub use crate::metrics::{DEFAULT_FSYNC_THRESHOLD, LatencySnapshot};

/// Barrier (`fdatasync`) instrumentation the store exposes per §2.6 — "a store
/// that cannot show its own p50/p99 barrier latency cannot be operated." The
/// full log-scale latency histogram (p50/p95/p99), the mandatory degradation
/// alarm (§2.6: a near-full device's ~50× `fdatasync` stall MUST be surfaced
/// loudly), and the append-throughput counters, all lock-free.
struct Metrics {
    /// `fdatasync` barrier latency distribution.
    fsync:                           LatencyHistogram,
    /// The mandatory degradation alarm on barrier latency (§2.6).
    alarm:                           DegradationAlarm,
    /// Commit groups committed (one barrier each in a barriered mode).
    groups:                          Counter,
    /// Batches durably written.
    batches:                         Counter,
    /// Events durably written.
    events:                          Counter,
    /// Payload+framing bytes durably written (the `encoded_len` sum).
    bytes:                           Counter,
    /// Direct-owner retained batch slots after the most recent commit.
    direct_scratch_retained_batches: AtomicU64,
    /// Direct-owner retained request/effect/index bytes after the most recent
    /// commit.
    direct_scratch_retained_bytes:   AtomicU64,
    /// Oversize direct groups whose transient capacity was shed on return.
    direct_scratch_trims:            Counter,
    /// Input batches whose subframe descriptors exceeded the inline stack.
    direct_subframe_spills:          Counter,
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics {
            fsync:                           LatencyHistogram::new(),
            alarm:                           DegradationAlarm::new(
                "fdatasync",
                DEFAULT_FSYNC_THRESHOLD,
            ),
            groups:                          Counter::new(),
            batches:                         Counter::new(),
            events:                          Counter::new(),
            bytes:                           Counter::new(),
            direct_scratch_retained_batches: AtomicU64::new(0),
            direct_scratch_retained_bytes:   AtomicU64::new(0),
            direct_scratch_trims:            Counter::new(),
            direct_subframe_spills:          Counter::new(),
        }
    }
}

impl Metrics {
    /// Record one barrier latency and feed the degradation alarm (§2.6). Both
    /// are lock-free `fetch_add`s off a timestamp diff the committer already
    /// holds — the trivial hot-path overhead the bone requires.
    fn record_fsync(&self, dt: Duration) {
        self.fsync.record(dt);
        self.alarm.observe(dt);
    }

    fn snapshot(&self) -> CommitterMetrics {
        CommitterMetrics {
            fsync:                           self.fsync.snapshot(),
            fsync_degraded:                  self.alarm.is_tripped(),
            fsync_degraded_trips:            self.alarm.trips(),
            fsync_threshold_nanos:           self.alarm.threshold_nanos(),
            groups:                          self.groups.get(),
            batches:                         self.batches.get(),
            events:                          self.events.get(),
            bytes:                           self.bytes.get(),
            direct_scratch_retained_batches: self
                .direct_scratch_retained_batches
                .load(Ordering::Relaxed),
            direct_scratch_retained_bytes:   self
                .direct_scratch_retained_bytes
                .load(Ordering::Relaxed),
            direct_scratch_trims:            self.direct_scratch_trims.get(),
            direct_subframe_spills:          self.direct_subframe_spills.get(),
        }
    }
}

/// A point-in-time snapshot of a committer's runtime metrics (`bn-e2y`).
///
/// Read via [`Committer::metrics`]. The barrier-latency percentiles and the
/// degradation flag are the mandatory §2.6 surface; the throughput counters are
/// the doc-03 operational list (append rate, events-per-fsync denominator).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CommitterMetrics {
    /// `fdatasync` barrier latency (p50/p95/p99/max/mean, nanoseconds).
    pub fsync:                           LatencySnapshot,
    /// Whether barrier latency has crossed the degradation threshold — the
    /// sticky store-status flag (§2.6). `true` means the device has shown
    /// degraded-load `fdatasync` latency at least once.
    pub fsync_degraded:                  bool,
    /// Number of barriers that crossed the threshold.
    pub fsync_degraded_trips:            u64,
    /// The active degradation threshold, in nanoseconds.
    pub fsync_threshold_nanos:           u64,
    /// Commit groups committed.
    pub groups:                          u64,
    /// Batches durably written.
    pub batches:                         u64,
    /// Events durably written.
    pub events:                          u64,
    /// Payload+framing bytes durably written.
    pub bytes:                           u64,
    /// Direct-owner request/effect slots retained for reuse.
    pub direct_scratch_retained_batches: u64,
    /// Direct-owner request/effect/index storage retained for reuse, in bytes.
    pub direct_scratch_retained_bytes:   u64,
    /// Number of oversize calls whose transient scratch was shed.
    pub direct_scratch_trims:            u64,
    /// Input batches larger than the 16-subframe inline direct scratch.
    pub direct_subframe_spills:          u64,
}

/// Cloneable query-side view of a committer's lock-free metrics core.
/// Recording remains on the append path; the O(histogram-buckets) percentile
/// scan occurs only when an operator asks for a snapshot.
#[derive(Clone)]
pub struct CommitterMetricsHandle(Arc<Metrics>);

impl CommitterMetricsHandle {
    #[must_use]
    pub fn snapshot(&self) -> CommitterMetrics { self.0.snapshot() }
}

// ---------------------------------------------------------------------------
// Async ack slot (one per in-flight append)
// ---------------------------------------------------------------------------

struct AckState {
    outcome: Option<Result<AppendOutcome, AppendError>>,
    waker:   Option<std::task::Waker>,
}

type Ack = Arc<Mutex<AckState>>;

fn new_ack() -> Ack {
    Arc::new(Mutex::new(AckState { outcome: None, waker: None }))
}

fn fulfill(ack: &Ack, outcome: Result<AppendOutcome, AppendError>) {
    let mut st = ack.lock().unwrap();
    st.outcome = Some(outcome);
    if let Some(w) = st.waker.take() {
        w.wake();
    }
}

// ---------------------------------------------------------------------------
// Completion signal (committer → shutdown), sidestepping the non-`'static`
// RPITIT join handle from `Runtime::spawn`
// ---------------------------------------------------------------------------

struct DoneState {
    done:   bool,
    /// Every waiter currently parked on this signal. Plural — unlike a
    /// single-shot `shutdown()`/`Drop` join (the original, sole use of this
    /// signal), [`AckOrClosed`] below also races an arbitrary number of
    /// concurrently in-flight appends against it (`bn-3da`), so a single
    /// `Option<Waker>` slot would silently drop all but the last registrant
    /// and leave the others parked forever.
    wakers: Vec<std::task::Waker>,
}

type Done = Arc<Mutex<DoneState>>;

fn new_done() -> Done {
    Arc::new(Mutex::new(DoneState { done: false, wakers: Vec::new() }))
}

fn signal_done(done: &Done) {
    let mut st = done.lock().unwrap();
    st.done = true;
    for w in st.wakers.drain(..) {
        w.wake();
    }
}

/// Resolves once the committer task has exited (its final durable handoff
/// is complete).
struct DoneWait {
    done: Done,
}

impl Future for DoneWait {
    type Output = ();

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        let mut st = self.done.lock().unwrap();
        if st.done {
            std::task::Poll::Ready(())
        } else {
            st.wakers.push(cx.waker().clone());
            std::task::Poll::Pending
        }
    }
}

/// Waits for an append's durability verdict, but resolves early with
/// [`AppendError::Closed`] if the committer's task exits (the completion
/// signal fires) before ever fulfilling this ack — the backstop for a
/// request submitted to (or already queued for) a committer that has since
/// been force-closed (`bn-3da`, [`Committer::drop`]/[`Committer::shutdown`]).
///
/// Checking `ack` before `done` is load-bearing: any request the committer
/// actually gathers into a group has its ack fulfilled
/// (`commit_group`'s step 6, unconditionally — success, `Indeterminate`, or
/// a typed error) strictly before the loop can advance to exit and signal
/// `done` (`committer_loop` is a single sequential task), so a real outcome
/// always wins the race; only a request the loop never got to gather at all
/// ever observes `done` first.
struct AckOrClosed {
    ack:  Ack,
    done: Done,
}

impl Future for AckOrClosed {
    type Output = Result<AppendOutcome, AppendError>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        {
            let mut st = self.ack.lock().unwrap();
            if let Some(out) = st.outcome.take() {
                return std::task::Poll::Ready(out);
            }
            st.waker = Some(cx.waker().clone());
        }
        let mut st = self.done.lock().unwrap();
        if st.done {
            return std::task::Poll::Ready(Err(AppendError::Closed));
        }
        st.wakers.push(cx.waker().clone());
        std::task::Poll::Pending
    }
}

// ---------------------------------------------------------------------------
// In-flight gate (the §2.2 early-close counter, with an embedded waker)
// ---------------------------------------------------------------------------

/// Counts writers between `append()` entry and their submission (§2.2). The
/// committer's early-close fires when this reaches zero — "every writer
/// currently in flight is already waiting on this window."
///
/// The embedded waker is load-bearing: when the *last* in-flight writer
/// submits (count → 0), it wakes the committer so its gather can re-check
/// the early-close condition immediately, instead of stalling until the
/// `max_delay` cap fires. Without this, a steady convoy pays ~`max_delay`
/// per group waiting for a zero it already reached (measured: the cap's
/// full 1 ms burned per group).
struct Gate {
    count: AtomicUsize,
    waker: Mutex<Option<std::task::Waker>>,
}

impl Gate {
    fn new() -> Arc<Gate> {
        Arc::new(Gate { count: AtomicUsize::new(0), waker: Mutex::new(None) })
    }

    /// A writer entered `append`.
    fn enter(&self) { self.count.fetch_add(1, Ordering::Relaxed); }

    /// A writer submitted. If it was the last in flight (count → 0), wake
    /// the committer's gather.
    fn leave(&self) {
        if self.count.fetch_sub(1, Ordering::Relaxed) == 1
            && let Some(w) = self.waker.lock().unwrap().take()
        {
            w.wake();
        }
    }

    fn is_zero(&self) -> bool { self.count.load(Ordering::Relaxed) == 0 }

    /// Register the committer's waker to be notified on the next count → 0.
    fn register(&self, w: &std::task::Waker) {
        *self.waker.lock().unwrap() = Some(w.clone());
    }
}

// ---------------------------------------------------------------------------
// Single-consumer async channel (the §2.4 centralized gather point)
// ---------------------------------------------------------------------------

struct ChanInner<T> {
    queue:      std::collections::VecDeque<T>,
    recv_waker: Option<std::task::Waker>,
    senders:    usize,
}

struct Sender<T> {
    inner: Arc<Mutex<ChanInner<T>>>,
}

struct Receiver<T> {
    inner: Arc<Mutex<ChanInner<T>>>,
}

fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Mutex::new(ChanInner {
        queue:      std::collections::VecDeque::new(),
        recv_waker: None,
        senders:    1,
    }));
    (Sender { inner: inner.clone() }, Receiver { inner })
}

impl<T> Sender<T> {
    fn send(&self, v: T) {
        let mut st = self.inner.lock().unwrap();
        st.queue.push_back(v);
        if let Some(w) = st.recv_waker.take() {
            w.wake();
        }
    }

    /// Force a re-poll of a parked receiver without touching the queue or
    /// the sender count — used by forced shutdown (`bn-3da`) to wake
    /// [`recv_unless_closed`] promptly right after flipping `closed`,
    /// instead of waiting on the next real send or the ordinary
    /// last-sender-drops wake (which may never come while an [`Appender`]
    /// outlives the [`Committer`]).
    fn wake_receiver(&self) {
        let mut st = self.inner.lock().unwrap();
        if let Some(w) = st.recv_waker.take() {
            w.wake();
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.inner.lock().unwrap().senders += 1;
        Sender { inner: self.inner.clone() }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut st = self.inner.lock().unwrap();
        st.senders -= 1;
        if st.senders == 0
            && let Some(w) = st.recv_waker.take()
        {
            w.wake();
        }
    }
}

impl<T> Receiver<T> {
    /// Non-blocking dequeue.
    fn try_recv(&self) -> Option<T> {
        self.inner.lock().unwrap().queue.pop_front()
    }

    /// Await one item; resolves `None` once the queue is drained and every
    /// [`Sender`] has dropped (channel closed).
    fn recv(&self) -> Recv<'_, T> { Recv { inner: &self.inner } }
}

struct Recv<'a, T> {
    inner: &'a Arc<Mutex<ChanInner<T>>>,
}

impl<T> Future for Recv<'_, T> {
    type Output = Option<T>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<T>> {
        let mut st = self.inner.lock().unwrap();
        if let Some(v) = st.queue.pop_front() {
            std::task::Poll::Ready(Some(v))
        } else if st.senders == 0 {
            std::task::Poll::Ready(None)
        } else {
            st.recv_waker = Some(cx.waker().clone());
            std::task::Poll::Pending
        }
    }
}

// ---------------------------------------------------------------------------
// Internal request + gather policy
// ---------------------------------------------------------------------------

struct CommitReq {
    stream_id:            u64,
    category_id:          u64,
    first_stream_version: u64,
    events:               CommitEvents,
    /// Precomputed on-disk `total_len`, for the `max_bytes` window bound
    /// (computed once by the appender at submit time, §2.1's "committer
    /// does not re-encode" spirit — it does not recompute the length
    /// either, it reuses this).
    encoded_len:          u64,
    /// Ordered-unit membership (`bn-2di`), `None` for a standalone batch.
    /// Carries the atomic-failure rule of [`AppendError::UnitAborted`].
    unit:                 Option<UnitTag>,
    /// `Some` only on the asynchronous path. `Option<Arc<_>>` has the same
    /// size/layout as the old mandatory `Ack`; direct indexes live in a
    /// parallel owner-only scratch vector, keeping async `CommitReq`
    /// unchanged.
    ack:                  Option<Ack>,
}

// Layout oracle for the async path before direct-owner completions were added.
// Keep this field-for-field copy local: a future field addition or a loss of
// `Arc`'s null niche must fail compilation rather than silently increasing the
// shared request allocation that `bn-21ew` is required to leave unchanged.
#[allow(dead_code)]
struct CommitReqWithMandatoryAck {
    stream_id:            u64,
    category_id:          u64,
    first_stream_version: u64,
    events:               CommitEvents,
    encoded_len:          u64,
    unit:                 Option<UnitTag>,
    ack:                  Ack,
}

const _: () = {
    assert!(std::mem::size_of::<Option<Ack>>() == std::mem::size_of::<Ack>());
    assert!(std::mem::align_of::<Option<Ack>>() == std::mem::align_of::<Ack>());
    assert!(
        std::mem::size_of::<CommitReq>()
            == std::mem::size_of::<CommitReqWithMandatoryAck>()
    );
    assert!(
        std::mem::align_of::<CommitReq>()
            == std::mem::align_of::<CommitReqWithMandatoryAck>()
    );
};

struct CommitEffect {
    /// Present only for the async path. `Option<Arc<_>>` uses Arc's null
    /// niche, so the async per-group allocation keeps the exact old `(Ack,
    /// Result)` size while direct effects carry no synchronization object.
    ack:     Option<Ack>,
    outcome: Result<AppendOutcome, AppendError>,
}

// The async wrapper deliberately keeps its old per-group allocation size.
const _: () = assert!(
    std::mem::size_of::<CommitEffect>()
        == std::mem::size_of::<(Ack, Result<AppendOutcome, AppendError>,)>()
);
const _: () = assert!(
    std::mem::align_of::<CommitEffect>()
        == std::mem::align_of::<(Ack, Result<AppendOutcome, AppendError>,)>()
);

fn commit_effect(
    req: &CommitReq,
    outcome: Result<AppendOutcome, AppendError>,
) -> CommitEffect {
    let ack = req.ack.as_ref().map(Arc::clone);
    CommitEffect { ack, outcome }
}

enum CommitEvents {
    Inputs(Vec<EventInput>),
    Prepared(PreparedBatch),
}

/// Which ordered unit a [`CommitReq`] belongs to, and whether it is the unit's
/// last batch (`bn-2di`).
///
/// A unit's batches are not necessarily gathered into ONE commit group — in the
/// non-coalescing durability modes (`Process`/`Os`) they become *adjacent*
/// groups — so the set of failed units is committer-loop state, not
/// `commit_group` state. `last` is what prunes it: the unit's final batch (be
/// it written or skipped) retires its id from that set, so the set only ever
/// holds units that are still mid-flight.
#[derive(Debug, Clone, Copy)]
struct UnitTag {
    id:   u64,
    last: bool,
}

/// Allocates the process-unique [`UnitTag::id`]s. Monotonic, never reused; a
/// wrap would need 2^64 ordered units.
static NEXT_UNIT_ID: AtomicU64 = AtomicU64::new(1);

/// The gather behaviour distilled from a [`Durability`] mode.
#[derive(Debug, Clone, Copy)]
struct Policy {
    /// Gather concurrently-arriving batches into one group (`Group` only).
    coalesce:  bool,
    /// Issue an `fdatasync` barrier (`Os` and `Group`; not `Process`).
    barrier:   bool,
    max_delay: Duration,
    max_bytes: u64,
}

impl From<Durability> for Policy {
    fn from(d: Durability) -> Self {
        match d {
            Durability::Process => Policy {
                coalesce:  false,
                barrier:   false,
                max_delay: Duration::ZERO,
                max_bytes: 0,
            },
            Durability::Os => Policy {
                coalesce:  false,
                barrier:   true,
                max_delay: Duration::ZERO,
                max_bytes: 0,
            },
            Durability::Group { max_delay, max_bytes } => {
                Policy { coalesce: true, barrier: true, max_delay, max_bytes }
            }
        }
    }
}

fn subframes_of(events: &[EventInput]) -> Vec<Subframe<'_>> {
    events
        .iter()
        .map(|e| {
            Subframe::plain(
                e.event_type_id,
                e.schema_version,
                e.codec_id,
                &e.payload,
            )
        })
        .collect()
}

/// Most owner batches are homogeneous and small. Keep their borrowed framing
/// descriptors on the stack; a larger batch may spill for this call, but that
/// allocation is dropped immediately and can never become retained scratch.
const DIRECT_INLINE_SUBFRAMES: usize = 16;
type DirectSubframes<'a> = SmallVec<[Subframe<'a>; DIRECT_INLINE_SUBFRAMES]>;

fn direct_subframes_of(events: &[EventInput]) -> DirectSubframes<'_> {
    events
        .iter()
        .map(|e| {
            Subframe::plain(
                e.event_type_id,
                e.schema_version,
                e.codec_id,
                &e.payload,
            )
        })
        .collect()
}

/// Allocation-free preflight for [`EventInput`]'s guaranteed-plain framing.
/// It is the closed form of `BatchEncoder::total_len`: `EventInput` fixes
/// compression/metadata to zero, so payload representability and the A2/A5
/// bounds are the only fallible subframe properties.
fn direct_total_len(
    events: &[EventInput],
    chain_enabled: bool,
) -> Result<u64, EncodeError> {
    if events.is_empty() {
        return Err(EncodeError::EmptyBatch);
    }
    if u32::try_from(events.len()).is_err() {
        return Err(EncodeError::TooManyFrames { count: events.len() });
    }
    let mut frames_len = 0u64;
    for (index, event) in events.iter().enumerate() {
        if u32::try_from(event.payload.len()).is_err() {
            return Err(EncodeError::Subframe {
                index,
                reason: SubframeError::PayloadTooLarge {
                    len: event.payload.len(),
                },
            });
        }
        frames_len = frames_len.saturating_add(
            crate::format::SUBFRAME_HDR_LEN as u64 + event.payload.len() as u64,
        );
    }
    let total_len = crate::format::HEADER_LEN as u64
        + if chain_enabled { crate::format::CHAIN_LEN as u64 } else { 0 }
        + frames_len
        + crate::format::MARKER_LEN as u64;
    if total_len > crate::format::MAX_BATCH_LEN {
        return Err(EncodeError::BatchTooLarge {
            total_len,
            max: crate::format::MAX_BATCH_LEN,
        });
    }
    Ok(total_len)
}

fn append_commit_req<F: Fs>(
    writer: &mut SegmentWriter<F>,
    req: &mut CommitReq,
    crypto_chain: Option<&[u8; crate::format::CHAIN_LEN]>,
) -> Result<crate::writer::Receipt, WriteError> {
    match &mut req.events {
        CommitEvents::Inputs(events) => match req.ack.as_ref() {
            Some(_) => {
                let subs = subframes_of(events);
                writer.append(&BatchSpec {
                    stream_id: req.stream_id,
                    category_id: req.category_id,
                    first_stream_version: req.first_stream_version,
                    crypto_chain,
                    subframes: &subs,
                })
            }
            None => {
                let subs = direct_subframes_of(events);
                writer.append(&BatchSpec {
                    stream_id: req.stream_id,
                    category_id: req.category_id,
                    first_stream_version: req.first_stream_version,
                    crypto_chain,
                    subframes: &subs,
                })
            }
        },
        CommitEvents::Prepared(batch) => writer.append_prepared(
            &PreparedBatchSpec {
                stream_id: req.stream_id,
                category_id: req.category_id,
                first_stream_version: req.first_stream_version,
                crypto_chain,
            },
            batch,
        ),
    }
}

// ---------------------------------------------------------------------------
// Forced-shutdown-aware receive (`bn-3da`)
// ---------------------------------------------------------------------------

/// `rx.recv()`, but also resolves to `None` — without waiting for every
/// [`Sender`] to drop — once `closed` is observed set (forced shutdown:
/// [`Committer::drop`]/[`Committer::shutdown`]). Polls `recv` FIRST on every
/// wake, so a request already queued (or one racing in concurrently with the
/// close) is always picked up and gathered as an ordinary group before the
/// close is honored — the same register-then-check discipline `gather`'s
/// early-close uses to close the set-then-signal race. This is how "drain
/// what's already gathering, reject what isn't" (the policy documented on
/// [`Committer`]'s `Drop` impl) is actually implemented: nothing here ever
/// discards a request out of the queue, it just stops picking up NEW ones
/// once closed.
async fn recv_unless_closed(
    rx: &Receiver<CommitReq>,
    closed: &AtomicBool,
) -> Option<CommitReq> {
    std::future::poll_fn(|cx| {
        let mut recv = std::pin::pin!(rx.recv());
        if let std::task::Poll::Ready(v) = recv.as_mut().poll(cx) {
            return std::task::Poll::Ready(v);
        }
        if closed.load(Ordering::Acquire) {
            return std::task::Poll::Ready(None);
        }
        std::task::Poll::Pending
    })
    .await
}

// ---------------------------------------------------------------------------
// The committer loop
// ---------------------------------------------------------------------------

/// Gather one commit group. For a non-coalescing mode (`Process`/`Os`) the
/// group is exactly `first` (a group of one, §1.2). For `Group`, drain what
/// has arrived, then close on the **first** of: `max_bytes` reached, the
/// `max_delay` cap, or early-close — the convoy has fully submitted
/// (`gate.is_zero()`) AND reached its expected width (`pending.len() >=
/// target`, `target` being the previous group's size).
///
/// `gate` counts appenders between `append()` entry and submission; the
/// last one to submit (count → 0) wakes this gather (`Gate`). The `target`
/// guard holds the window for a *reforming* convoy: after a group is acked,
/// its writers re-enter one at a time, and a bare `gate.is_zero()` fires on
/// the first re-submission — splitting the convoy across barriers (measured:
/// intermittent ~294 vs ~394 events/fsync at 4×100). At one writer `target`
/// is 1, so the window still closes immediately: the §2.2 sync-per-batch
/// degradation is preserved. On the single-threaded sim runtime a group
/// closes once every runnable writer has submitted or the cap fires in
/// virtual time — deterministic under a seed.
async fn gather<R: Runtime>(
    rt: &R,
    rx: &Receiver<CommitReq>,
    first: CommitReq,
    policy: &Policy,
    gate: &Gate,
    target: usize,
) -> Vec<CommitReq> {
    let mut pending = vec![first];
    if !policy.coalesce {
        return pending;
    }
    let mut bytes = pending[0].encoded_len;

    let closed = |plen: usize, bytes: u64| {
        bytes >= policy.max_bytes || (gate.is_zero() && plen >= target)
    };

    // Fast path: drain + early-close without arming any timer.
    while let Some(r) = rx.try_recv() {
        bytes += r.encoded_len;
        pending.push(r);
    }
    if closed(pending.len(), bytes) {
        return pending;
    }

    // Slow path: still gathering the convoy. Wait for the next submission,
    // the convoy completing, or the `max_delay` cap — the cap on its own
    // spawned task/thread so the committer's own thread never blocks in a
    // sleep (§2.5; a `RealRuntime` sleep is thread-blocking by design).
    // Clone `rt` into the sleep task so it owns a `'static` runtime.
    let deadline = rt.now().saturating_add(policy.max_delay);
    let rt_sleep = rt.clone();
    let timer = rt.spawn(async move { rt_sleep.sleep_until(deadline).await });
    let mut timer = std::pin::pin!(timer);

    loop {
        while let Some(r) = rx.try_recv() {
            bytes += r.encoded_len;
            pending.push(r);
        }
        if closed(pending.len(), bytes) || rt.now() >= deadline {
            return pending;
        }
        let plen = pending.len();
        let mut recv = std::pin::pin!(rx.recv());
        let got = std::future::poll_fn(|cx| {
            // Register for the gate → 0 wake, then re-check under the same
            // poll to close the set-then-signal race. Treat gate-zero as a
            // close signal only once the convoy has reached `target`;
            // otherwise keep waiting for the reforming convoy, a new
            // submission, or the cap — never busy-spin on a bare zero.
            gate.register(cx.waker());
            if gate.is_zero() && plen >= target {
                return std::task::Poll::Ready(Ok(None));
            }
            if let std::task::Poll::Ready(v) = recv.as_mut().poll(cx) {
                return std::task::Poll::Ready(Ok(v));
            }
            if let std::task::Poll::Ready(()) = timer.as_mut().poll(cx) {
                return std::task::Poll::Ready(Err(()));
            }
            std::task::Poll::Pending
        })
        .await;
        match got {
            Ok(Some(r)) => {
                bytes += r.encoded_len;
                pending.push(r);
            }
            // Convoy complete / channel closed / the delay cap fired: the
            // loop top re-checks and closes the window.
            Ok(None) | Err(()) => {
                if closed(pending.len(), bytes) || rt.now() >= deadline {
                    return pending;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Live segment auto-roll (`bn-1vu`)
// ---------------------------------------------------------------------------

/// Live segment auto-roll wiring for the committer (`bn-1vu`).
///
/// When a batch would overflow the active segment (`WriteError::SegmentFull`,
/// A8) the committer rolls to a fresh segment **in place** — continuing the A1
/// (`base_pos = next_pos`) and A9 (`epoch + 1`) chains and preserving
/// `segment_size` — then retries the batch, so no append is lost or reordered
/// across the boundary (per `docs/spec/02-recovery.md` A7/A8: a rolled segment
/// gets a NEW id/epoch and its own `base_pos`). The just-full segment is made
/// durable and left **unsealed** ([`SegmentWriter::sync_and_summary`]); its
/// [`SegmentSummary`] is reported over `on_rolled` so the owner (the engine)
/// can build the sealed sidecars and finalize the footer off the append path
/// (D5). Sending is non-blocking (an unbounded channel); the committer never
/// waits on the sealer.
pub struct Roller {
    /// Produces the on-disk path for a segment id (the store's naming scheme).
    path_for:  Box<dyn Fn(u64) -> PathBuf + Send>,
    /// Reports each rolled (durable, still-unsealed) segment for background
    /// sealing. Dropped when the committer task exits, which closes the
    /// channel and lets the owner's sealer drain queued work and stop.
    on_rolled: std::sync::mpsc::Sender<SegmentSummary>,
}

impl Roller {
    /// Wire auto-roll: `path_for` names segment files, `on_rolled` receives
    /// each rolled segment's summary for background sealing.
    pub fn new(
        path_for: impl Fn(u64) -> PathBuf + Send + 'static,
        on_rolled: std::sync::mpsc::Sender<SegmentSummary>,
    ) -> Self {
        Roller { path_for: Box::new(path_for), on_rolled }
    }
}

/// Roll the live `writer` to a fresh segment in place (`bn-1vu`): make the
/// current (full) segment durable and unsealed, open the next one continuing
/// the A1/A9/`segment_size` chains, swap it in, and report the rolled segment
/// for background sealing. On `Err` the swap did not happen and the live writer
/// is left intact and usable (the current segment stays durable and readable) —
/// e.g. `bn-36y` `StoreFull` if the next segment could not be preallocated.
fn roll_segment<F: Fs>(
    writer: &mut SegmentWriter<F>,
    roller: &Roller,
) -> Result<(), WriteError> {
    let summary = writer.sync_and_summary()?; // old segment durable + unsealed
    let next_id = writer.segment_id() + 1;
    let next_epoch = writer.epoch() + 1;
    let next_path = (roller.path_for)(next_id);
    let next = writer.open_next(&next_path, next_id, next_epoch, 0)?;
    *writer = next; // swap in place; the old segment's file handle is dropped
    // Best-effort: a gone receiver just means no background sealing runs — the
    // rolled segment is still durable and recovered via a full scan on reopen.
    let _ = roller.on_rolled.send(summary);
    Ok(())
}

/// Write, barrier, advance the watermark, and finalize one gathered group
/// (§2.1 steps 2–6). Pure blocking fs work — no `.await`, so no
/// appender-facing lock spans the barrier (§2.5). Appends one effect per
/// request; its caller either fulfills the async ack or consumes the direct
/// index-aligned result.
///
/// `roller` (`bn-1vu`): when a batch would overflow the active segment (A8),
/// the committer rolls to a fresh segment and retries the batch once, so a
/// full segment is transparent to appenders. A batch larger than a whole empty
/// segment still cannot fit and is surfaced as `SegmentFull` (no infinite
/// roll).
///
/// `failed_units` (`bn-2di`): the ordered-unit atomic-failure rule. A unit
/// ([`Appender::submit_ordered`]) is written batch-by-batch like any other, but
/// the moment ONE of its batches gets a non-[`Acked`](AppendOutcome::Acked)
/// outcome the unit is poisoned: every later batch of it is skipped — never
/// encoded, never written, no global position consumed — and failed with
/// [`AppendError::UnitAborted`]. The set lives in [`committer_loop`] and is
/// threaded through here because a unit can span adjacent commit GROUPS in the
/// non-coalescing modes.
#[allow(clippy::too_many_arguments)] // internal seam; each arg is a distinct shared handle
fn commit_group_inner<R: Runtime, F: Fs>(
    rt: &R,
    writer: &mut SegmentWriter<F>,
    group: &mut [CommitReq],
    policy: &Policy,
    watermark: &Watermark,
    metrics: &Metrics,
    degraded: &Degraded,
    roller: Option<&Roller>,
    chain_enabled: bool,
    heads: &mut HashMap<u64, ChainHead>,
    failed_units: &mut HashSet<u64>,
    effects: &mut Vec<CommitEffect>,
) {
    let effect_start = effects.len();
    // Steps 2–3: assign positions centrally + write each batch. `next_pos`
    // advances only for successfully written batches.
    let mut wrote_any = false;
    for req in group {
        // `bn-2di` (unit atomicity): an earlier batch of this unit already
        // failed, so this one must never reach the file. Skipped BEFORE the
        // write — it consumes no position, and the caller learns definitively
        // that it is not in the log.
        if let Some(unit) = req.unit
            && failed_units.contains(&unit.id)
        {
            if unit.last {
                failed_units.remove(&unit.id);
            }
            effects.push(commit_effect(req, Err(AppendError::UnitAborted)));
            continue;
        }
        // Fold-chain (spec 05 §6, G10): when enabled, stamp this batch's
        // `crypto_chain = h[base-1]` — the current head of this stream, which
        // starts at `genesis(stream_id)` the first time the stream is seen. The
        // head is only *advanced* after a successful write (below), so a
        // rejected/retried batch never desyncs the chain. `entry` is a Copy
        // `[u8; 32]`, so the borrow it hands to `spec` ends before the append
        // returns and cannot conflict with the later `absorb`.
        let entry: Option<ChainHead> = if chain_enabled {
            let head = *heads
                .entry(req.stream_id)
                .or_insert_with(|| ChainHead::genesis(req.stream_id));
            debug_assert_eq!(
                head.next_version(),
                req.first_stream_version,
                "chain head for stream {} at v{} but batch starts at v{}",
                req.stream_id,
                head.next_version(),
                req.first_stream_version,
            );
            Some(head)
        } else {
            None
        };
        let entry_bytes = entry.map(|h| h.entry());
        // `bn-1vu`: try the append; on A8 SegmentFull with auto-roll wired,
        // roll to a fresh segment and retry the SAME batch once. Positions stay
        // dense — the new segment's `base_pos == next_pos`, so the retried
        // batch gets the exact global position it would have had.
        //
        // `bn-u6o`: a batch bigger than an EMPTY segment can never fit no
        // matter how many times we roll — every fresh segment has exactly
        // `writer.empty_segment_capacity()` bytes of room. Rolling anyway would
        // waste a near-empty segment before hitting the same typed error, and a
        // retrying caller would proliferate one such segment per attempt. Check
        // BEFORE rolling and surface the typed error immediately instead.
        let mut outcome = append_commit_req(writer, req, entry_bytes.as_ref());
        if let (Err(WriteError::SegmentFull { needed, .. }), Some(roller)) =
            (&outcome, roller)
            && *needed <= writer.empty_segment_capacity()
        {
            outcome = match roll_segment(writer, roller) {
                Ok(()) => append_commit_req(writer, req, entry_bytes.as_ref()),
                Err(e) => Err(e),
            };
        }
        let res = match outcome {
            Ok(receipt) => {
                wrote_any = true;
                metrics.batches.incr();
                metrics.events.add(u64::from(receipt.frame_count));
                // Exact on-disk bytes written (includes the 32-byte chain slot
                // when enabled), taken from the receipt rather than the
                // chain-unaware `encoded_len` preflight estimate.
                metrics.bytes.add(receipt.total_len);
                // The batch is durably placed: fold its on-disk payloads into
                // the stream's head so the NEXT batch continues the chain
                // (§6.2 exit head). Uncompressed frames (`Subframe::plain`)
                // store the payload verbatim, so the bytes folded here are the
                // exact bytes a reader re-hashes.
                if chain_enabled {
                    let head = heads
                        .get_mut(&req.stream_id)
                        .expect("chain head inserted above");
                    match &req.events {
                        CommitEvents::Inputs(events) => head.absorb_batch(
                            events.iter().map(|e| e.payload.as_slice()),
                        ),
                        CommitEvents::Prepared(batch) => head.absorb_batch(
                            batch.payload_ranges().iter().map(
                                |&(start, end)| {
                                    &batch.bytes()[start as usize..end as usize]
                                },
                            ),
                        ),
                    };
                }
                let first_position = receipt.first_global_pos;
                let last_position =
                    first_position + u64::from(receipt.frame_count) - 1;
                Ok(AppendOutcome::Acked {
                    first_position,
                    last_position,
                    // Read AFTER the (possibly rolled) append: the writer that
                    // accepted the batch is the one whose id names its segment.
                    segment_id: writer.segment_id(),
                    offset: receipt.offset,
                })
            }
            Err(WriteError::Encode(e)) => Err(AppendError::Encode(e)),
            Err(WriteError::SegmentFull { needed, remaining }) => {
                Err(AppendError::SegmentFull { needed, remaining })
            }
            // bn-36y typed disk-full states. StoreFull surfaces when a live
            // auto-roll (bn-1vu) could not preallocate the next segment;
            // StorePoisoned fails fast on every append after a barrier ENOSPC.
            Err(WriteError::StoreFull { .. }) => Err(AppendError::StoreFull),
            Err(WriteError::StorePoisoned) => Err(AppendError::StorePoisoned),
            // A durable-path I/O fault mid-write: the batch's durability is
            // unknown → Indeterminate, and (below) the barrier is treated as
            // failed so the watermark does not advance past this group.
            Err(WriteError::ShortWrite { .. }) | Err(WriteError::Io(_)) => {
                Ok(AppendOutcome::Indeterminate)
            }
            // bn-221: `InvalidResume` is only ever constructed by
            // `SegmentWriter::resume` (rejecting a bogus resumed offset before
            // any append is possible); `append` itself never produces it. Kept
            // as an explicit arm (not folded into a wildcard) so a future
            // `WriteError` variant added to `append`'s real error surface
            // still trips this match at compile time instead of silently
            // falling through here.
            Err(WriteError::InvalidResume { .. }) => {
                unreachable!(
                    "InvalidResume is only returned by SegmentWriter::resume"
                )
            }
        };
        // `bn-2di`: anything but a positive ack poisons the rest of the unit.
        // `Indeterminate` counts: the batch MAY be durable and MAY not, which
        // is precisely the state a dependent later batch must not be committed
        // on top of. (A barrier fault below can still downgrade an `Acked`
        // batch to `Indeterminate` afterwards — but it downgrades the WHOLE
        // group together, unit members included, so the unit never splits into
        // "durable prefix, decided-failed suffix".)
        if let Some(unit) = req.unit {
            if !matches!(res, Ok(AppendOutcome::Acked { .. })) {
                failed_units.insert(unit.id);
            }
            if unit.last {
                failed_units.remove(&unit.id);
            }
        }
        effects.push(commit_effect(req, res));
    }

    // Step 4: the barrier. One `fdatasync` covers every `pwrite` above
    // (Process: none). The FULL D8 policy (`bn-25e`, §2.6): ANY barrier
    // failure — `EIO`, `ENOSPC`, or other — means the durable state is
    // unknowable, so we **poison the whole store**, sticky for the
    // committer's lifetime. This group is downgraded to `Indeterminate`
    // (below), the watermark is frozen (Step 5), and every later write fails
    // fast (`committer_loop`). The failed barrier is NEVER retried — retrying
    // `fdatasync` after `EIO` is the classic fsyncgate corruption (§2.6) — so
    // no later `commit_group` runs and no `close()` re-issues it.
    // `bn-2di`: there is nothing to flush before this barrier any more.
    //
    // This used to run an owner-supplied `PreBarrier` hook — `mess-store`'s
    // co-durable name flush (bn-150/bn-2cj/bn-34o), which made the fjall
    // `stream_names`/`type_names` tables durable inside this window so an event
    // could never out-live its name. Those tables are gone: names are
    // `$registry` records in this very log now, written by this very committer,
    // ordered ahead of the batches that reference them. The invariant is
    // structural, so the hook — and the fsync it carried — is not merely unused
    // but *impossible to reintroduce* through this seam.
    let mut barrier_ok = true;
    if policy.barrier && wrote_any {
        let t0 = rt.now();
        match writer.sync() {
            Ok(()) => {
                metrics.record_fsync(rt.now().saturating_duration_since(t0));
                metrics.groups.incr();
            }
            Err(e) => {
                barrier_ok = false;
                degraded.poison(PoisonCause::classify(&e));
            }
        }
    }

    // Step 5: advance the position-ordered watermark to cover the whole
    // group — but only if the group is durable. On a barrier fault the
    // watermark stays put and every batch in the group is downgraded to
    // Indeterminate.
    if barrier_ok {
        watermark.advance(writer.next_pos());
    }

    // Step 6: finalize in position order (effects were built in gather order,
    // which is position order). The async wrapper fulfills ack slots; the
    // owner consumes the same effects directly by index.
    if !barrier_ok {
        for effect in &mut effects[effect_start..] {
            effect.outcome = match std::mem::replace(
                &mut effect.outcome,
                Ok(AppendOutcome::Indeterminate),
            ) {
                Ok(AppendOutcome::Acked { .. })
                | Ok(AppendOutcome::Indeterminate) => {
                    Ok(AppendOutcome::Indeterminate)
                }
                other => other,
            };
        }
    }
}

/// Async compatibility wrapper. This deliberately preserves the existing ack
/// machinery and allocation shape; only `DirectCommitter` supplies reusable
/// effect storage and index completions.
#[allow(clippy::too_many_arguments)]
fn commit_group<R: Runtime, F: Fs>(
    rt: &R,
    writer: &mut SegmentWriter<F>,
    group: &mut [CommitReq],
    policy: &Policy,
    watermark: &Watermark,
    metrics: &Metrics,
    degraded: &Degraded,
    roller: Option<&Roller>,
    chain_enabled: bool,
    heads: &mut HashMap<u64, ChainHead>,
    failed_units: &mut HashSet<u64>,
) {
    let mut effects = Vec::with_capacity(group.len());
    commit_group_inner(
        rt,
        writer,
        group,
        policy,
        watermark,
        metrics,
        degraded,
        roller,
        chain_enabled,
        heads,
        failed_units,
        &mut effects,
    );
    for effect in effects {
        let ack = effect.ack.expect("async effect carries its ack");
        fulfill(&ack, effect.outcome);
    }
}

#[allow(clippy::too_many_arguments)] // internal spawn seam; each arg is a distinct shared handle
async fn committer_loop<R: Runtime, F: Fs>(
    rt: R,
    mut writer: SegmentWriter<F>,
    rx: Receiver<CommitReq>,
    policy: Policy,
    gate: Arc<Gate>,
    watermark: Watermark,
    metrics: Arc<Metrics>,
    degraded: Degraded,
    closed: Arc<AtomicBool>,
    done: Done,
    roller: Option<Roller>,
    chain_enabled: bool,
    mut heads: HashMap<u64, ChainHead>,
) {
    // The expected convoy width, seeded at 1 (so the first group and the
    // one-writer case both close immediately) and tracking the last group's
    // size thereafter.
    let mut target = 1usize;
    // Ordered units (`bn-2di`) one of whose batches has already failed: every
    // later batch of them must be skipped, never written. Lives here rather
    // than in `commit_group` because a unit can span adjacent groups under
    // `Process`/`Os` (no coalescing). Only failures ever enter it and each
    // unit's last batch retires its id, so it is empty in steady state.
    let mut failed_units: HashSet<u64> = HashSet::new();
    while let Some(first) = recv_unless_closed(&rx, &closed).await {
        let mut group = gather(&rt, &rx, first, &policy, &gate, target).await;
        target = group.len().max(1);
        // D8 sticky poison (`bn-25e`, §2.6): once a barrier has failed, the
        // durable state is unknowable. NEVER write atop it and NEVER re-issue
        // the barrier — fail every gathered batch fast with `StorePoisoned`.
        // A request already in the channel when the poison tripped (the
        // `submit` fast-path could not catch it) is caught here, so no batch
        // is ever written after the poison. The watermark stays frozen because
        // no `commit_group` runs.
        if degraded.is_poisoned() {
            for req in group {
                // `bn-2di`: a poisoned unit member fails like any other batch,
                // and (being non-`Acked`) poisons the rest of its unit too —
                // which here is belt-and-braces, since every later batch of it
                // would hit this same guard.
                if let Some(unit) = req.unit {
                    if unit.last {
                        failed_units.remove(&unit.id);
                    } else {
                        failed_units.insert(unit.id);
                    }
                }
                let ack = req.ack.expect("async request carries its ack");
                fulfill(&ack, Err(AppendError::StorePoisoned));
            }
            continue;
        }
        commit_group(
            &rt,
            &mut writer,
            &mut group,
            &policy,
            &watermark,
            &metrics,
            &degraded,
            roller.as_ref(),
            chain_enabled,
            &mut heads,
            &mut failed_units,
        );
    }
    // Shutdown. On a healthy store, make the handoff durable (a `Process`-mode
    // tail may be unsynced). On a POISONED store, drop the writer WITHOUT a
    // final `fdatasync`: `close()` would re-issue the barrier that just failed,
    // which is exactly the fsyncgate retry §2.6 forbids. Recovery on restart
    // re-establishes the committed prefix — that is the only exit.
    if degraded.is_poisoned() {
        drop(writer);
    } else {
        let _ = writer.close();
    }
    signal_done(&done);
}

// ---------------------------------------------------------------------------
// The public handle
// ---------------------------------------------------------------------------

/// Submit one batch to a committer and await its outcome. Shared by
/// [`Appender::append`] and [`Committer::append`]: mark in-flight, submit
/// over the single gather channel, unmark, await the ack.
async fn submit(
    tx: &Sender<CommitReq>,
    gate: &Gate,
    degraded: &Degraded,
    closed: &Arc<AtomicBool>,
    done: &Done,
    req: AppendRequest,
) -> Result<AppendOutcome, AppendError> {
    // D8 fail-fast (`bn-25e`, §2.6): a poisoned store rejects every write at
    // the entry point, before any gate accounting or encoding — no append
    // must be built atop an indeterminate durable state. This is the fast
    // path; the narrow race where the poison trips AFTER this check but before
    // the committer processes the request is closed by the loop's own poison
    // guard, which fails such a request `StorePoisoned` too.
    if degraded.is_poisoned() {
        return Err(AppendError::StorePoisoned);
    }
    // Forced-shutdown fast path (`bn-3da`): `closed` is set by
    // `Committer::drop`/`shutdown` before the loop necessarily notices, so a
    // new append attempted after (or racing with) shutdown fails fast
    // instead of queuing into a committer that may already be gone. This is
    // an optimization, not the correctness boundary — the narrow race where
    // `closed` trips just AFTER this check is closed below by racing the ack
    // against the completion signal (`AckOrClosed`), never a hang.
    if closed.load(Ordering::Acquire) {
        return Err(AppendError::Closed);
    }
    // §2.2 early-close accounting: mark in-flight at the VERY START of the
    // call — before any encoding — and unmark right after submission. This
    // matches the reference's "writers between append() entry and their
    // submission" definition (`perf_group_commit` §4). It is load-bearing
    // for coalescing: a writer re-entering append after its previous ack
    // must count as in-flight *while it encodes* its next batch, or the
    // committer can observe a spurious `active == 0` and close the group
    // before the convoy reforms (measured: ~318 vs ~381 events/fsync at
    // 4×100 when the increment came only after encode).
    gate.enter();

    // Pre-flight: compute the on-disk length (this also validates A5 / A2 /
    // subframe consistency). Scoped so the `events` borrow ends before we
    // move `events` into the request.
    let total_len = {
        let subs = subframes_of(&req.events);
        let input = BatchInput {
            segment_epoch:        0,
            batch_id:             0,
            first_global_pos:     0,
            stream_id:            req.stream_id,
            category_id:          req.category_id,
            first_stream_version: req.first_stream_version,
            crypto_chain:         None,
            subframes:            &subs,
        };
        BatchEncoder::total_len(&input)
    };
    let encoded_len = match total_len {
        Ok(n) => n,
        Err(e) => {
            gate.leave();
            return Err(AppendError::Encode(e));
        }
    };

    let ack = new_ack();
    let creq = CommitReq {
        stream_id: req.stream_id,
        category_id: req.category_id,
        first_stream_version: req.first_stream_version,
        events: CommitEvents::Inputs(req.events),
        encoded_len,
        // A standalone batch is its own fate: nothing else depends on it and it
        // depends on nothing (`bn-2di`).
        unit: None,
        ack: Some(ack.clone()),
    };

    // Submit, then unmark. No `.await` between the two, so a single-threaded
    // (sim) executor observes the gate return to zero atomically w.r.t. the
    // committer's gather — the sim fast-path is preserved.
    tx.send(creq);
    gate.leave();

    AckOrClosed { ack, done: done.clone() }.await
}

/// Submit N batches as ONE ordered unit and await every outcome, in order
/// (`bn-2di`). Shared by [`Appender::append_ordered`] and
/// [`Committer::append_ordered`].
///
/// # What this guarantees, and why the registry needs it
///
/// `mess-store`'s log-derived `$registry` (spec `04-registry.md` §4.2, REG12)
/// must get a name's `*Registered` record into the log **no later than** the
/// first batch that references the id it mints. This function is the primitive
/// that makes that free rather than expensive:
///
/// - **File order.** Every `CommitReq` is pushed into the single gather channel
///   back-to-back with **no `.await` between the sends**, so the committer
///   receives them in exactly `reqs` order (the channel is FIFO and this is one
///   sender). `commit_group` writes a group in gather order, so `reqs[0]`'s
///   bytes land at a lower offset — and a lower global position — than
///   `reqs[1]`'s, in the same segment. Recovery accepts a **contiguous prefix**
///   of valid batches, so no crash can leave `reqs[1]` durable while `reqs[0]`
///   is not: losing the earlier batch necessarily loses the later one. The
///   ordering guarantee is the log's own, not a second barrier.
/// - **One barrier.** The whole sequence submits inside ONE
///   [`Gate::enter`]/[`Gate::leave`] span, so the committer's `gather` cannot
///   observe `gate.is_zero()` between the sends and close the window early. In
///   `Group` mode the drain therefore takes every request of the unit into the
///   **same commit group**, and its single `fdatasync` covers all of them. In
///   the non-coalescing modes (`Process`/`Os`, §1.2) they become adjacent
///   groups — still in the right file order, which is all correctness needs.
///
/// # Failure semantics — the unit fails ATOMICALLY (`bn-2di`)
///
/// Each batch gets its own [`AppendOutcome`]; the returned `Vec` is
/// index-aligned with `reqs`. Three rules make a unit's failure atomic rather
/// than merely ordered:
///
/// 1. **Pre-flight rejects the whole unit.** An [`AppendError`]
///    (`Encode`/`Closed`/`StorePoisoned`) raised here rejects every batch
///    before anything is sent — no partial submission, and nothing for the
///    caller to undo.
/// 2. **A failed batch aborts the rest of its unit.** Every `CommitReq` of a
///    unit carries its [`UnitTag`]; the first
///    non-[`Acked`](AppendOutcome::Acked) outcome makes `commit_group` skip
///    every LATER batch of that unit — unwritten, no position consumed — with
///    [`AppendError::UnitAborted`]. Without this, file order alone would allow
///    `reqs[0]` (a `$registry` registration) to be rejected by a full store
///    while `reqs[1]` (the domain batch that references the ids it registers)
///    was accepted: permanent corruption from an ordinary `ENOSPC`.
/// 3. **A barrier fault downgrades the whole covering group together**, unit
///    members included — so the caller never sees "the registration is durable
///    but its first use is indeterminate" as a *decided* state.
///
/// What is deliberately NOT provided is the converse: an EARLIER batch of the
/// unit can be durably committed while a LATER one fails (`reqs[1]` too big for
/// an empty segment, say). That is the point of ordering the dependency first,
/// and it is why [`Submitted::wait`] hands back one result per batch instead of
/// short-circuiting — see its docs.
///
/// An empty `reqs` is a no-op returning an empty [`Submitted`].
///
/// # Why this is synchronous (`bn-2di`)
///
/// The push is deliberately **not** `async`: it performs no `.await` at all, so
/// a caller may hold a plain `std` lock across it. `mess-store` needs exactly
/// that. Its name interner (the `Book` mutex) is what publishes a
/// newly-minted id to other appender threads, so the `$registry` record that
/// mints the id must reach the channel **before that mutex is released** —
/// otherwise a second thread could observe the id as interned, build a batch
/// referencing it, and push that batch ahead of the registration. The log's
/// contiguous-prefix recovery rule would then happily accept a batch whose
/// `stream_id`/`event_type_id` no `*Registered` record covers, and the store
/// would fail to open. Pushing under the interner lock closes that window with
/// the lock the engine already holds; awaiting the acks then happens outside
/// it, via [`Submitted::wait`].
fn submit_ordered_now(
    tx: &Sender<CommitReq>,
    gate: &Gate,
    degraded: &Degraded,
    closed: &Arc<AtomicBool>,
    done: &Done,
    reqs: Vec<AppendRequest>,
) -> Result<Submitted, AppendError> {
    if reqs.is_empty() {
        return Ok(Submitted { acks: Vec::new(), done: done.clone() });
    }
    // Same two fail-fast gates as `submit`, hoisted ahead of the whole unit:
    // a poisoned or closed store must reject every batch of it, not strand
    // half of them in the channel.
    if degraded.is_poisoned() {
        return Err(AppendError::StorePoisoned);
    }
    if closed.load(Ordering::Acquire) {
        return Err(AppendError::Closed);
    }

    // Mark in-flight for the WHOLE unit — entered before the first encode and
    // left only after the last send. This is what keeps `gather`'s early-close
    // (`gate.is_zero() && plen >= target`) from firing between our sends and
    // splitting the unit across two barriers.
    gate.enter();

    // Pre-flight every batch BEFORE sending any: an `Encode` fault on
    // `reqs[1]` must not leave `reqs[0]` already committed to the channel.
    let mut creqs: Vec<CommitReq> = Vec::with_capacity(reqs.len());
    let mut acks: Vec<Ack> = Vec::with_capacity(reqs.len());
    // One id for the whole unit (`bn-2di`): a non-`Acked` outcome on any batch
    // aborts every LATER batch of this id, in `commit_group`. A one-batch unit
    // has no "rest" to abort, so it needs no id.
    let unit_id =
        (reqs.len() > 1).then(|| NEXT_UNIT_ID.fetch_add(1, Ordering::Relaxed));
    let last_idx = reqs.len() - 1;
    for (i, req) in reqs.into_iter().enumerate() {
        let total_len = {
            let subs = subframes_of(&req.events);
            let input = BatchInput {
                segment_epoch:        0,
                batch_id:             0,
                first_global_pos:     0,
                stream_id:            req.stream_id,
                category_id:          req.category_id,
                first_stream_version: req.first_stream_version,
                crypto_chain:         None,
                subframes:            &subs,
            };
            BatchEncoder::total_len(&input)
        };
        let encoded_len = match total_len {
            Ok(n) => n,
            Err(e) => {
                gate.leave();
                return Err(AppendError::Encode(e));
            }
        };
        let ack = new_ack();
        acks.push(ack.clone());
        creqs.push(CommitReq {
            stream_id: req.stream_id,
            category_id: req.category_id,
            first_stream_version: req.first_stream_version,
            events: CommitEvents::Inputs(req.events),
            encoded_len,
            unit: unit_id.map(|id| UnitTag { id, last: i == last_idx }),
            ack: Some(ack),
        });
    }

    // Submit the unit back-to-back, then unmark. No `.await` between the sends
    // (nor between the last send and `leave`), so the committer observes the
    // whole unit as one contiguous run of the channel — on the single-threaded
    // sim runtime it cannot interleave at all, and on a real runtime the gate
    // holds the group window open across them.
    for creq in creqs {
        tx.send(creq);
    }
    gate.leave();

    Ok(Submitted { acks, done: done.clone() })
}

/// A pushed-but-not-yet-durable ordered unit (`bn-2di`): the handle
/// [`Appender::submit_ordered`] returns so the caller can release its own
/// locks before blocking on the barrier. [`wait`](Submitted::wait) resolves
/// to one [`AppendOutcome`] per batch, index-aligned with the `reqs` that
/// produced it.
///
/// Dropping a `Submitted` without waiting does **not** cancel anything: the
/// batches are already in the committer's channel and will be written and
/// (in a durable mode) barriered like any other. The caller simply never
/// learns their outcome — the same "a cancelled append still fully commits"
/// property `spawn_blocking` gives the rest of the append path.
#[must_use = "the batches are already queued; drop only if the outcome is \
              genuinely not needed"]
pub struct Submitted {
    acks: Vec<Ack>,
    done: Done,
}

impl Submitted {
    /// Await every batch's durability outcome, in submission order — one result
    /// **per batch**, index-aligned with the `reqs` that produced it.
    ///
    /// # Why this is not `Result<Vec<_>>` (`bn-2di`)
    ///
    /// A unit can fail *partially*: `reqs[0]` writes and is barriered,
    /// `reqs[1]` hits [`AppendError::SegmentFull`] (a batch larger than an
    /// empty segment can never fit, no matter how many times the roller
    /// rolls). The first batch's global positions are then a permanent,
    /// irreversible fact — the watermark has already advanced past them —
    /// and the caller MUST still publish it, or it leaves a
    /// committed-but-never-published position that stalls every higher
    /// position forever (the `bn-3nz` hazard).
    ///
    /// A short-circuiting `Result<Vec<_>>` would throw that outcome away along
    /// with the error. So the per-batch results are handed back whole, and it
    /// is the caller's job to publish everything that landed before
    /// surfacing the failure. [`append_ordered`](Appender::append_ordered)
    /// short-circuits, for callers with nothing to publish.
    pub async fn wait(self) -> Vec<Result<AppendOutcome, AppendError>> {
        let mut outcomes = Vec::with_capacity(self.acks.len());
        for ack in self.acks {
            outcomes.push(AckOrClosed { ack, done: self.done.clone() }.await);
        }
        outcomes
    }

    /// How many batches this unit carries.
    #[must_use]
    pub fn len(&self) -> usize { self.acks.len() }

    /// Whether this unit is empty (nothing was pushed).
    #[must_use]
    pub fn is_empty(&self) -> bool { self.acks.is_empty() }
}

/// A cheap-to-clone, `Send + Sync` submit-side handle to a running
/// committer — the object appender tasks hold. Unlike [`Committer`] (which
/// owns the committer's join handle and is not itself shareable across
/// tasks), an `Appender` can be cloned into any number of spawned writers.
///
/// An `Appender` does **not** keep the committer alive on its own (`bn-3da`):
/// the owning [`Committer`] is authoritative over the task's lifecycle, and
/// dropping it deterministically stops and joins the committer task even
/// while `Appender` clones still exist — see [`Committer`]'s `Drop` impl for
/// the exact policy. An `Appender` outlived by its `Committer` simply starts
/// getting [`AppendError::Closed`] from [`append`](Appender::append) instead
/// of hanging.
#[derive(Clone)]
pub struct Appender {
    tx:        Sender<CommitReq>,
    gate:      Arc<Gate>,
    watermark: Watermark,
    degraded:  Degraded,
    closed:    Arc<AtomicBool>,
    done:      Done,
}

impl Appender {
    /// Durably append one batch and await its outcome (see
    /// [`Committer::append`]). Fails fast with
    /// [`AppendError::StorePoisoned`] if the store has been poisoned by a
    /// prior barrier failure (D8, §2.6), or with [`AppendError::Closed`] if
    /// the owning [`Committer`] has shut down (`bn-3da`) — never hangs.
    pub async fn append(
        &self,
        req: AppendRequest,
    ) -> Result<AppendOutcome, AppendError> {
        submit(
            &self.tx,
            &self.gate,
            &self.degraded,
            &self.closed,
            &self.done,
            req,
        )
        .await
    }

    /// Push N batches into the committer as ONE ordered unit, **synchronously**
    /// (`bn-2di`), returning a [`Submitted`] to await later. See
    /// [`submit_ordered_now`] for the file-order and single-barrier guarantees
    /// this buys, and why the log-derived `$registry` depends on being able to
    /// push while holding its interner lock.
    pub fn submit_ordered(
        &self,
        reqs: Vec<AppendRequest>,
    ) -> Result<Submitted, AppendError> {
        submit_ordered_now(
            &self.tx,
            &self.gate,
            &self.degraded,
            &self.closed,
            &self.done,
            reqs,
        )
    }

    /// Durably append N batches as ONE ordered unit and await every outcome —
    /// [`submit_ordered`](Appender::submit_ordered) followed immediately by
    /// [`Submitted::wait`], for callers with no lock to release in between.
    pub async fn append_ordered(
        &self,
        reqs: Vec<AppendRequest>,
    ) -> Result<Vec<AppendOutcome>, AppendError> {
        self.submit_ordered(reqs)?.wait().await.into_iter().collect()
    }

    /// A clone of the durable watermark this committer advances. After a
    /// barrier poisons the store the committer stops advancing it, so a reader
    /// holding this watermark can still serve the pre-poison committed prefix
    /// (degraded reads) but never anything past the last known-durable
    /// position.
    pub fn watermark(&self) -> Watermark { self.watermark.clone() }

    /// Whether the store is poisoned/degraded (D8, §2.6): a prior barrier
    /// failed, writes now fail fast, and reads are clamped to the frozen
    /// watermark. Sticky for the committer's lifetime — restart + recovery is
    /// the only exit.
    pub fn is_degraded(&self) -> bool { self.degraded.is_poisoned() }

    /// A clone of the shared [`Degraded`] flag, so a reader built from this
    /// appender's [`watermark`](Appender::watermark) can *observe* that it is
    /// reading a degraded store rather than a live one.
    pub fn degraded(&self) -> Degraded { self.degraded.clone() }
}

/// A running committer. Owns the committer task's join handle and the
/// root sender. Hand out [`Appender`]s (`Send + Sync`, cloneable into
/// spawned writer tasks) via [`appender`](Committer::appender). Generic
/// over the runtime `R` (concurrency + time) and the filesystem `F` of its
/// segment — usually `F == R::Fs`, decoupled so tests can wrap the
/// segment's fs independently.
pub struct Committer<R: Runtime> {
    tx:        Option<Sender<CommitReq>>,
    gate:      Arc<Gate>,
    watermark: Watermark,
    metrics:   Arc<Metrics>,
    degraded:  Degraded,
    /// Forced-shutdown flag (`bn-3da`): set by [`Drop`]/[`shutdown`]
    /// (`begin_shutdown`) so the committer loop and any in-flight/future
    /// [`Appender::append`] observe closure deterministically, independent
    /// of how many `Appender` clones are still alive (plain sender
    /// ref-counting alone cannot express "the owner says stop").
    closed:    Arc<AtomicBool>,
    done:      Done,
    /// Owned so [`Drop`] can call [`Runtime::block_on`] to join the
    /// committer task synchronously — correct on both runtimes: on
    /// [`crate::runtime::real::RealRuntime`] the task runs on its own OS
    /// thread already, so this just parks; on
    /// [`crate::runtime::sim::SimRuntime`] tasks only make progress while
    /// *something* steps the shared single-threaded executor, so `Drop`
    /// must drive it itself rather than block-parking a thread nothing else
    /// will ever wake.
    rt:        R,
}

/// Synchronous commit core for a caller that is itself the single append
/// owner. Unlike [`Committer`], this owns no channel or worker task: the caller
/// gathers and validates work, then invokes [`commit_ordered_group`] on the
/// same thread that owns the [`SegmentWriter`]. This is the B0Direct seam used
/// by `mess-store`'s flat-combined append owner.
///
/// All durability behavior remains shared with the asynchronous committer:
/// live roll, fold-chain stamping, ordered-unit forward atomicity, sticky D8
/// poison, watermark discipline, and the existing metrics implementation all
/// run through [`commit_group`].
pub struct DirectCommitter<R: Runtime, F: Fs> {
    rt:            R,
    writer:        Option<SegmentWriter<F>>,
    policy:        Policy,
    watermark:     Watermark,
    metrics:       Arc<Metrics>,
    degraded:      Degraded,
    roller:        Option<Roller>,
    chain_enabled: bool,
    heads:         HashMap<u64, ChainHead>,
    failed_units:  HashSet<u64>,
    scratch:       DirectScratch,
}

const DIRECT_RETAINED_BATCHES: usize = 256;
const DIRECT_RETAINED_BYTE_CAP: usize = 1024 * 1024;
#[derive(Clone, Copy)]
struct DirectIndex {
    unit:  u32,
    batch: u32,
}
const _: () = assert!(
    DIRECT_RETAINED_BATCHES
        * (std::mem::size_of::<CommitReq>()
            + std::mem::size_of::<CommitEffect>()
            + std::mem::size_of::<DirectIndex>())
        <= DIRECT_RETAINED_BYTE_CAP
);

#[derive(Default)]
struct DirectScratch {
    requests:         Vec<CommitReq>,
    effects:          Vec<CommitEffect>,
    indexes:          Vec<DirectIndex>,
    reported_batches: usize,
    reported_bytes:   usize,
}

impl DirectScratch {
    /// An untrusted group may temporarily grow either vector. Never let that
    /// capacity become a permanent owner-thread memory tax.
    fn cap_retained(&mut self) -> bool {
        let mut trimmed = false;
        if self.requests.capacity() > DIRECT_RETAINED_BATCHES {
            self.requests = Vec::with_capacity(DIRECT_RETAINED_BATCHES);
            trimmed = true;
        }
        if self.effects.capacity() > DIRECT_RETAINED_BATCHES {
            self.effects = Vec::with_capacity(DIRECT_RETAINED_BATCHES);
            trimmed = true;
        }
        if self.indexes.capacity() > DIRECT_RETAINED_BATCHES {
            self.indexes = Vec::with_capacity(DIRECT_RETAINED_BATCHES);
            trimmed = true;
        }
        trimmed
    }
}

impl<R: Runtime, F: Fs> DirectCommitter<R, F> {
    /// Build a direct commit core with the production roll and fold-chain
    /// behavior. The caller becomes the sole owner of `writer`.
    pub fn with_roll_chained(
        rt: &R,
        writer: SegmentWriter<F>,
        durability: Durability,
        roller: Roller,
        chain: ChainInit,
    ) -> Self {
        let watermark = Watermark::new(writer.next_pos());
        let ChainInit { enabled: chain_enabled, heads } = chain;
        DirectCommitter {
            rt: rt.clone(),
            writer: Some(writer),
            policy: Policy::from(durability),
            watermark,
            metrics: Arc::new(Metrics::default()),
            degraded: Degraded::new(),
            roller: Some(roller),
            chain_enabled,
            heads,
            failed_units: HashSet::new(),
            scratch: DirectScratch::default(),
        }
    }

    /// Commit a gathered set of ordered units with one covering durability
    /// decision. Each inner vector is one ordered unit. After the covering
    /// barrier and watermark decision, `complete(unit_index, batch_index,
    /// outcome)` is invoked exactly once per input in gather order; no ack
    /// lock or nested result shape is materialized. Preflight validates the
    /// gathered set before any write, so an invalid later member cannot strand
    /// an earlier registry record. A failure in one unit never aborts an
    /// unrelated unit once writing begins, while a failed member aborts every
    /// later member of that same unit.
    pub fn commit_ordered_group(
        &mut self,
        units: Vec<Vec<DirectAppendRequest>>,
        mut complete: impl FnMut(usize, usize, DirectBatchOutcome),
    ) -> Result<(), AppendError> {
        if self.degraded.is_poisoned() {
            return Err(AppendError::StorePoisoned);
        }

        // Load-bearing ordered-unit preflight: the engine may put a registry
        // batch immediately before the domain batch that first uses its ids.
        // Validate every member before the first byte lands. Prepared batches
        // already carry validated final framing, so their check is O(1);
        // ordinary plain inputs use the canonical framing formula without
        // materializing borrowed subframes (differentially tested below).
        let mut subframe_spills = 0u64;
        let preflight: Result<(), AppendError> = (|| {
            for req in units.iter().flatten() {
                match req {
                    DirectAppendRequest::Inputs(req) => {
                        if req.events.len() > DIRECT_INLINE_SUBFRAMES {
                            subframe_spills += 1;
                        }
                        direct_total_len(&req.events, self.chain_enabled)
                            .map_err(AppendError::Encode)?;
                    }
                    DirectAppendRequest::Prepared { batch, .. } => batch
                        .validate_for_chain(self.chain_enabled)
                        .map_err(AppendError::Encode)?,
                }
            }
            Ok(())
        })();
        if subframe_spills != 0 {
            self.metrics.direct_subframe_spills.add(subframe_spills);
        }
        preflight?;

        debug_assert!(self.scratch.requests.is_empty());
        debug_assert!(self.scratch.effects.is_empty());
        debug_assert!(self.scratch.indexes.is_empty());
        let request_count = units.iter().map(Vec::len).sum();
        self.scratch.requests.reserve(request_count);
        self.scratch.effects.reserve(request_count);
        self.scratch.indexes.reserve(request_count);
        for (unit_index, reqs) in units.into_iter().enumerate() {
            let unit_index = u32::try_from(unit_index)
                .expect("a direct group cannot contain 2^32 ordered units");
            let unit_id = (reqs.len() > 1)
                .then(|| NEXT_UNIT_ID.fetch_add(1, Ordering::Relaxed));
            let last_idx = reqs.len().saturating_sub(1);
            for (batch_index, req) in reqs.into_iter().enumerate() {
                let batch_index = u32::try_from(batch_index)
                    .expect("an ordered unit cannot contain 2^32 batches");
                let (stream_id, category_id, first_stream_version, events) =
                    match req {
                        DirectAppendRequest::Inputs(req) => (
                            req.stream_id,
                            req.category_id,
                            req.first_stream_version,
                            CommitEvents::Inputs(req.events),
                        ),
                        DirectAppendRequest::Prepared {
                            stream_id,
                            category_id,
                            first_stream_version,
                            batch,
                        } => (
                            stream_id,
                            category_id,
                            first_stream_version,
                            CommitEvents::Prepared(batch),
                        ),
                    };
                self.scratch.requests.push(CommitReq {
                    stream_id,
                    category_id,
                    first_stream_version,
                    events,
                    // Direct groups are already gathered by the owner, so
                    // they do not need the asynchronous committer's byte
                    // window accounting. Avoid a second O(events) framing
                    // walk here; SegmentWriter performs the authoritative
                    // validation while encoding below.
                    encoded_len: 0,
                    unit: unit_id.map(|id| UnitTag {
                        id,
                        last: batch_index as usize == last_idx,
                    }),
                    ack: None,
                });
                self.scratch.indexes.push(DirectIndex {
                    unit:  unit_index,
                    batch: batch_index,
                });
            }
        }

        if self.policy.barrier && !self.policy.coalesce {
            // `Os` is sync-per-batch by contract. Ordered-unit members remain
            // adjacent and share `failed_units`, but each accepted batch earns
            // its own fdatasync before the next is written.
            for req in &mut self.scratch.requests {
                if self.degraded.is_poisoned() {
                    if let Some(unit) = req.unit {
                        if unit.last {
                            self.failed_units.remove(&unit.id);
                        } else {
                            self.failed_units.insert(unit.id);
                        }
                    }
                    self.scratch.effects.push(CommitEffect {
                        ack:     None,
                        outcome: Err(AppendError::StorePoisoned),
                    });
                    continue;
                }
                commit_group_inner(
                    &self.rt,
                    self.writer.as_mut().expect("direct committer is live"),
                    std::slice::from_mut(req),
                    &self.policy,
                    &self.watermark,
                    &self.metrics,
                    &self.degraded,
                    self.roller.as_ref(),
                    self.chain_enabled,
                    &mut self.heads,
                    &mut self.failed_units,
                    &mut self.scratch.effects,
                );
            }
        } else if !self.scratch.requests.is_empty() {
            commit_group_inner(
                &self.rt,
                self.writer.as_mut().expect("direct committer is live"),
                &mut self.scratch.requests,
                &self.policy,
                &self.watermark,
                &self.metrics,
                &self.degraded,
                self.roller.as_ref(),
                self.chain_enabled,
                &mut self.heads,
                &mut self.failed_units,
                &mut self.scratch.effects,
            );
        }

        debug_assert_eq!(
            self.scratch.requests.len(),
            self.scratch.effects.len()
        );
        debug_assert_eq!(
            self.scratch.requests.len(),
            self.scratch.indexes.len()
        );
        for ((req, effect), index) in self
            .scratch
            .requests
            .drain(..)
            .zip(self.scratch.effects.drain(..))
            .zip(self.scratch.indexes.drain(..))
        {
            debug_assert!(req.ack.is_none());
            debug_assert!(effect.ack.is_none());
            let events = match req.events {
                CommitEvents::Inputs(events) => {
                    DirectBatchEvents::Inputs(events)
                }
                CommitEvents::Prepared(batch) => {
                    DirectBatchEvents::Prepared(batch)
                }
            };
            complete(
                index.unit as usize,
                index.batch as usize,
                DirectBatchOutcome { outcome: effect.outcome, events },
            );
        }
        let trimmed = self.scratch.cap_retained();
        if trimmed {
            self.metrics.direct_scratch_trims.incr();
        }
        let retained_batches = self
            .scratch
            .requests
            .capacity()
            .max(self.scratch.effects.capacity())
            .max(self.scratch.indexes.capacity());
        if trimmed || retained_batches != self.scratch.reported_batches {
            self.metrics
                .direct_scratch_retained_batches
                .store(retained_batches as u64, Ordering::Relaxed);
            self.scratch.reported_batches = retained_batches;
        }
        let retained_bytes = self
            .scratch
            .requests
            .capacity()
            .saturating_mul(std::mem::size_of::<CommitReq>())
            .saturating_add(
                self.scratch
                    .effects
                    .capacity()
                    .saturating_mul(std::mem::size_of::<CommitEffect>()),
            )
            .saturating_add(
                self.scratch
                    .indexes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<DirectIndex>()),
            );
        debug_assert!(retained_bytes <= DIRECT_RETAINED_BYTE_CAP);
        if trimmed || retained_bytes != self.scratch.reported_bytes {
            self.metrics
                .direct_scratch_retained_bytes
                .store(retained_bytes as u64, Ordering::Relaxed);
            self.scratch.reported_bytes = retained_bytes;
        }
        Ok(())
    }

    pub fn watermark(&self) -> Watermark { self.watermark.clone() }

    pub fn is_degraded(&self) -> bool { self.degraded.is_poisoned() }

    pub fn metrics(&self) -> CommitterMetrics { self.metrics.snapshot() }

    /// Shared query-side view for a direct core hosted on a private owner
    /// thread. Cloning it is O(1); percentile scans happen only on `snapshot`.
    pub fn metrics_handle(&self) -> CommitterMetricsHandle {
        CommitterMetricsHandle(Arc::clone(&self.metrics))
    }
}

impl<R: Runtime, F: Fs> Drop for DirectCommitter<R, F> {
    fn drop(&mut self) {
        let Some(writer) = self.writer.take() else { return };
        if self.degraded.is_poisoned() {
            drop(writer);
        } else {
            let _ = writer.close();
        }
    }
}

impl<R: Runtime> Committer<R> {
    /// Spawn the committer over `writer` in the given [`Durability`] mode.
    /// The committer thread/task takes ownership of `writer` and runs until
    /// [`shutdown`](Committer::shutdown) (or the handle drops).
    pub fn spawn<F>(
        rt: &R,
        writer: SegmentWriter<F>,
        durability: Durability,
    ) -> Self
    where
        F: Fs + Send + 'static,
        F::File: Send,
    {
        Self::spawn_inner(rt, writer, durability, None, ChainInit::off())
    }

    /// Spawn the committer with the fold chain enabled (`bn-3l0`, spec 05 §6):
    /// every batch carries its real on-disk `crypto_chain`, per-stream heads
    /// seeded from `chain.heads`. Otherwise identical to
    /// [`spawn`](Committer::spawn).
    pub fn spawn_chained<F>(
        rt: &R,
        writer: SegmentWriter<F>,
        durability: Durability,
        chain: ChainInit,
    ) -> Self
    where
        F: Fs + Send + 'static,
        F::File: Send,
    {
        Self::spawn_inner(rt, writer, durability, None, chain)
    }

    /// Spawn the committer with live segment auto-roll (`bn-1vu`): when a batch
    /// would overflow the active segment, the committer rolls to a fresh
    /// segment in place and reports the rolled segment via `roller` for
    /// background sealing. Otherwise identical to [`spawn`](Committer::spawn).
    pub fn spawn_with_roll<F>(
        rt: &R,
        writer: SegmentWriter<F>,
        durability: Durability,
        roller: Roller,
    ) -> Self
    where
        F: Fs + Send + 'static,
        F::File: Send,
    {
        Self::spawn_inner(
            rt,
            writer,
            durability,
            Some(roller),
            ChainInit::off(),
        )
    }

    /// Spawn with both live segment auto-roll (`bn-1vu`) and the fold chain
    /// (`bn-3l0`, spec 05 §6): the production engine path. Per-stream heads are
    /// seeded from `chain.heads` (rehydrated by recovery); each batch carries
    /// its real on-disk `crypto_chain` when `chain.enabled`.
    pub fn spawn_with_roll_chained<F>(
        rt: &R,
        writer: SegmentWriter<F>,
        durability: Durability,
        roller: Roller,
        chain: ChainInit,
    ) -> Self
    where
        F: Fs + Send + 'static,
        F::File: Send,
    {
        Self::spawn_inner(rt, writer, durability, Some(roller), chain)
    }

    fn spawn_inner<F>(
        rt: &R,
        writer: SegmentWriter<F>,
        durability: Durability,
        roller: Option<Roller>,
        chain: ChainInit,
    ) -> Self
    where
        F: Fs + Send + 'static,
        F::File: Send,
    {
        let policy = Policy::from(durability);
        let (tx, rx) = channel::<CommitReq>();
        let gate = Gate::new();
        let watermark = Watermark::new(writer.next_pos());
        let metrics = Arc::new(Metrics::default());
        let degraded = Degraded::new();
        let closed = Arc::new(AtomicBool::new(false));
        let done = new_done();

        // Fire-and-forget: the spawned task runs to completion regardless of
        // its join handle (which `Runtime::spawn` returns as a non-`'static`
        // RPITIT we cannot store). `Drop`/`shutdown` await `done` instead
        // (`bn-3da`).
        let ChainInit { enabled: chain_enabled, heads } = chain;
        drop(rt.spawn(committer_loop(
            rt.clone(),
            writer,
            rx,
            policy,
            gate.clone(),
            watermark.clone(),
            metrics.clone(),
            degraded.clone(),
            closed.clone(),
            done.clone(),
            roller,
            chain_enabled,
            heads,
        )));

        Committer {
            tx: Some(tx),
            gate,
            watermark,
            metrics,
            degraded,
            closed,
            done,
            rt: rt.clone(),
        }
    }

    /// A cheap, `Send + Sync` submit-side handle to clone into spawned
    /// writer tasks (see [`Appender`]).
    pub fn appender(&self) -> Appender {
        Appender {
            // `tx` is `Some` for the whole life of a live `Committer`; it is
            // only cleared by `shutdown`/`Drop` (`begin_shutdown`).
            tx:        self.tx.as_ref().expect("committer is live").clone(),
            gate:      self.gate.clone(),
            watermark: self.watermark.clone(),
            degraded:  self.degraded.clone(),
            closed:    self.closed.clone(),
            done:      self.done.clone(),
        }
    }

    /// A clone of the durable watermark ([`crate::watermark`]) this
    /// committer advances — the object D11 subscriptions will read. After a
    /// barrier poisons the store the committer stops advancing it (D8, §2.6),
    /// so it stays frozen at the last known-durable position and reads clamp
    /// there.
    pub fn watermark(&self) -> Watermark { self.watermark.clone() }

    /// Whether the store is poisoned/degraded (D8, §2.6): a prior barrier
    /// (`fdatasync`) failed — with `EIO`, `ENOSPC`, or other — so the durable
    /// state is unknowable. Writes now fail fast with
    /// [`AppendError::StorePoisoned`]; reads remain permitted but are clamped
    /// to the frozen [`watermark`](Committer::watermark). Sticky for the
    /// committer's lifetime with no reset — the only exit is process restart +
    /// recovery ([`docs/spec/02-recovery.md`]).
    pub fn is_degraded(&self) -> bool { self.degraded.is_poisoned() }

    /// The [`PoisonCause`] if the store is degraded, else `None`. Diagnostics
    /// only: every cause carries the identical permanent policy.
    pub fn poison_cause(&self) -> Option<PoisonCause> { self.degraded.cause() }

    /// A clone of the shared [`Degraded`] flag. Hand it to a
    /// [`ReadView`](crate::reader::ReadView) built from this committer's
    /// [`watermark`](Committer::watermark) so the reader can *observe* it is
    /// serving a degraded store (the flag readers query, D8, §2.6).
    pub fn degraded(&self) -> Degraded { self.degraded.clone() }

    /// Number of `fdatasync` barriers issued so far (§2.6 metric; the
    /// denominator for events-per-fsync).
    pub fn fsync_count(&self) -> u64 { self.metrics.fsync.count() }

    /// Mean barrier latency in nanoseconds so far, or `0` before any
    /// barrier (§2.6: barrier latency MUST be observable).
    pub fn mean_fsync_nanos(&self) -> u64 { self.metrics.fsync.mean_nanos() }

    /// A point-in-time snapshot of this committer's runtime metrics
    /// (`bn-e2y`): barrier-latency percentiles (p50/p95/p99), the mandatory
    /// degradation flag (§2.6), and append-throughput counters. Lock-free and
    /// cheap — safe to poll from any thread while the committer runs.
    pub fn metrics(&self) -> CommitterMetrics { self.metrics.snapshot() }

    /// Whether barrier latency has crossed the degradation threshold at least
    /// once — the sticky store-status flag §2.6 makes mandatory. `true` means
    /// the device has demonstrably shown degraded-load `fdatasync` latency
    /// (a near-full/contended consumer SSD's ~50× stall).
    pub fn is_fsync_degraded(&self) -> bool { self.metrics.alarm.is_tripped() }

    /// Reconfigure the fsync-degradation alarm threshold at runtime
    /// (default [`DEFAULT_FSYNC_THRESHOLD`], 50 ms).
    pub fn set_fsync_alarm_threshold(&self, threshold: Duration) {
        self.metrics.alarm.set_threshold(threshold);
    }

    /// Durably append one batch and await its outcome. Validates the batch
    /// (A5/A2) before submitting, hands it to the committer over the single
    /// gather channel, and `await`s the ack — which the committer delivers
    /// strictly after the covering barrier returns and the watermark
    /// advances past this batch (`Process` mode: after the write, §1.1).
    pub async fn append(
        &self,
        req: AppendRequest,
    ) -> Result<AppendOutcome, AppendError> {
        match &self.tx {
            Some(tx) => {
                submit(
                    tx,
                    &self.gate,
                    &self.degraded,
                    &self.closed,
                    &self.done,
                    req,
                )
                .await
            }
            None => Err(AppendError::Closed),
        }
    }

    /// Durably append N batches as ONE ordered unit (`bn-2di`) — see
    /// [`submit_ordered_now`].
    pub async fn append_ordered(
        &self,
        reqs: Vec<AppendRequest>,
    ) -> Result<Vec<AppendOutcome>, AppendError> {
        match &self.tx {
            Some(tx) => submit_ordered_now(
                tx,
                &self.gate,
                &self.degraded,
                &self.closed,
                &self.done,
                reqs,
            )?
            .wait()
            .await
            .into_iter()
            .collect(),
            None => Err(AppendError::Closed),
        }
    }

    /// Signal forced shutdown (`bn-3da`) and return the completion future to
    /// await ([`shutdown`](Committer::shutdown)) or block on
    /// ([`Drop`](Committer::drop)). Sets `closed` (observed by
    /// [`recv_unless_closed`] in the committer loop and by the fast path in
    /// [`submit`], so every live [`Appender`] — not just this handle — stops
    /// getting new work gathered), drops this handle's own [`Sender`] (the
    /// ordinary last-sender-closes-the-channel path, still exercised
    /// whenever no `Appender` outlives the `Committer`), and wakes a parked
    /// receiver so the forced close is noticed promptly rather than waiting
    /// on the next real send.
    ///
    /// Idempotent: calling this a second time (e.g. the `Drop` that runs
    /// immediately after `shutdown()` returns, since `shutdown` consumes
    /// `self`) is a cheap no-op — `tx` is already `None` and `done` is
    /// already `true`, so the returned [`DoneWait`] resolves on its first
    /// poll without blocking.
    fn begin_shutdown(&mut self) -> DoneWait {
        self.closed.store(true, Ordering::Release);
        if let Some(tx) = self.tx.take() {
            tx.wake_receiver();
        }
        DoneWait { done: self.done.clone() }
    }

    /// Close the gather channel and await the committer's shutdown (its
    /// final durable handoff). Unlike plain [`drop`], this is the
    /// async-friendly form for a caller already inside an executor —
    /// dropping the `Committer` (see the `Drop` impl) has the identical
    /// effect but blocks the calling thread to join.
    pub async fn shutdown(mut self) { self.begin_shutdown().await; }
}

impl<R: Runtime> Drop for Committer<R> {
    /// Deterministically stop and join the committer task (`bn-3da`).
    /// Closing only the gather channel — the original behaviour, still
    /// exercised here whenever no [`Appender`] outlives this `Committer` —
    /// is not enough on its own: nothing then confirmed the task had
    /// actually finished, so an in-process reopen of the same segment could
    /// race the still-running commit thread's own tail write/close. And if
    /// an `Appender` clone DOES outlive the `Committer`, plain sender
    /// ref-counting never closes the channel at all — the task, and the
    /// thread holding it (`RealRuntime`), would leak for the rest of the
    /// process.
    ///
    /// **Policy — reject, not drain, for anything not already gathering.**
    /// The bone requires picking one and documenting it: a request the loop
    /// has already gathered into a commit group runs to completion
    /// unaffected by this drop — its ack carries a real durability verdict,
    /// because [`recv_unless_closed`] always lets an already-queued or
    /// concurrently-racing-in request through as an ordinary group before it
    /// honors `closed`. Anything not yet gathered by the time `closed` is
    /// observed is never gathered afterward: a live [`Appender`]'s next
    /// [`append`](Appender::append) gets a typed [`AppendError::Closed`]
    /// (the `submit` fast path), and a request that already raced into the
    /// channel gets the same typed error via [`AckOrClosed`] racing its ack
    /// against the completion signal — never a hang.
    ///
    /// Joining blocks the calling thread via [`Runtime::block_on`] (not a
    /// bespoke thread-park): on
    /// [`RealRuntime`](crate::runtime::real::RealRuntime) the committer
    /// already runs on its own OS thread, so this just parks
    /// until it signals done; on
    /// [`SimRuntime`](crate::runtime::sim::SimRuntime) — a single-threaded
    /// cooperative executor with no independent progress of its own — a
    /// bare park would deadlock (nothing left to step the committer task to
    /// its `signal_done`), so `block_on` drives the shared executor itself.
    fn drop(&mut self) {
        let wait = self.begin_shutdown();
        self.rt.block_on(wait);
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, AtomicI32};

    use super::*;
    use crate::runtime::{CrashPlan, EnospcSite, Fault, TailPlan};
    use crate::runtime::{
        FileHandle, OpenOpts, RealRuntime, SimFs, SimRuntime,
    };
    use crate::writer::SegmentParams;

    fn req(stream: u64, version: u64, n_events: usize) -> AppendRequest {
        AppendRequest {
            stream_id:            stream,
            category_id:          0,
            first_stream_version: version,
            events:               (0..n_events)
                .map(|i| {
                    EventInput::plain(
                        1,
                        1,
                        0,
                        vec![(i as u8).wrapping_add(0xA0); 16],
                    )
                })
                .collect(),
        }
    }

    fn seg<F: Fs>(fs: &F, path: &Path) -> SegmentWriter<F> {
        SegmentWriter::create(fs, path, SegmentParams::new(0, 0, 1, 0)).unwrap()
    }

    // -- Os mode: every ack is durable and dense -------------------------

    #[test]
    fn os_mode_acks_are_durable_and_position_dense() {
        let rt = SimRuntime::new(7);
        let fs = rt.fs();
        let path = Path::new("/seg-os");
        let writer = seg(&fs, path);
        let outcomes = rt.block_on(async {
            let c = Committer::spawn(&rt, writer, Durability::Os);
            let wm = c.watermark();
            // Three sequential single-stream batches: 3, 5, 2 events.
            let mut acked = Vec::new();
            for (v, n) in [(0u64, 3usize), (3, 5), (8, 2)] {
                let out = c.append(req(1, v, n)).await.unwrap();
                let wm_now = wm.get();
                acked.push((out, wm_now));
            }
            c.shutdown().await;
            acked
        });
        // Positions tile [0, 10) densely, and at each ack the watermark
        // already covered the batch (ack strictly after barrier + advance).
        let mut expect_first = 0u64;
        for (out, wm_now) in &outcomes {
            let AppendOutcome::Acked { first_position, last_position, .. } =
                *out
            else {
                panic!("Os must ack every batch");
            };
            assert_eq!(first_position, expect_first, "positions must be dense");
            assert!(
                *wm_now > last_position,
                "ack delivered before watermark covered it: wm={wm_now} \
                 last={last_position}"
            );
            expect_first = last_position + 1;
        }
        assert_eq!(expect_first, 10, "3+5+2 events must tile [0,10)");
    }

    // -- Process mode: no barrier, immediate ack -------------------------

    #[test]
    fn process_mode_issues_no_barrier() {
        let rt = SimRuntime::new(3);
        let fs = rt.fs();
        let path = Path::new("/seg-proc");
        let writer = seg(&fs, path);
        let fsyncs = rt.block_on(async {
            let c = Committer::spawn(&rt, writer, Durability::Process);
            for v in 0..4u64 {
                let out = c.append(req(9, v, 1)).await.unwrap();
                assert!(matches!(out, AppendOutcome::Acked { .. }));
            }
            let n = c.fsync_count();
            c.shutdown().await;
            n
        });
        assert_eq!(
            fsyncs, 0,
            "Process mode must not issue a barrier before shutdown"
        );
    }

    // -- Metrics: throughput counters + degradation alarm (bn-e2y) -------

    #[test]
    fn metrics_count_throughput_and_barriers() {
        let rt = SimRuntime::new(11);
        let fs = rt.fs();
        let writer = seg(&fs, Path::new("/seg-metrics"));
        let m = rt.block_on(async {
            let c = Committer::spawn(&rt, writer, Durability::Os);
            // 3 batches: 3, 5, 2 events = 10 events, 3 batches, 3 barriers
            // (Os).
            for (v, n) in [(0u64, 3usize), (3, 5), (8, 2)] {
                c.append(req(1, v, n)).await.unwrap();
            }
            let m = c.metrics();
            c.shutdown().await;
            m
        });
        assert_eq!(m.events, 10, "every durable event is counted");
        assert_eq!(m.batches, 3);
        assert_eq!(m.groups, 3, "Os: one barrier per batch");
        assert_eq!(m.fsync.count, 3, "fsync histogram counts every barrier");
        assert!(m.bytes > 0, "encoded bytes are counted");
        // A healthy sim barrier is well under 50 ms: the alarm stays clear.
        assert!(!m.fsync_degraded, "healthy barriers must not trip the alarm");
        assert_eq!(m.fsync_degraded_trips, 0);
    }

    #[test]
    fn degradation_alarm_fires_on_slow_barrier() {
        // Inject "slowness" deterministically by dropping the threshold to
        // zero: every real barrier latency (>= 0) then crosses it, so
        // the mandatory §2.6 store-status flag latches — the alarm
        // wiring is exercised end-to-end through the committer without
        // a flaky real-time slow fsync.
        let rt = SimRuntime::new(5);
        let fs = rt.fs();
        let writer = seg(&fs, Path::new("/seg-degraded"));
        let m = rt.block_on(async {
            let c = Committer::spawn(&rt, writer, Durability::Os);
            c.set_fsync_alarm_threshold(Duration::ZERO);
            assert!(!c.is_fsync_degraded(), "clean before any barrier");
            c.append(req(1, 0, 2)).await.unwrap();
            assert!(
                c.is_fsync_degraded(),
                "a barrier past threshold must latch the flag"
            );
            let m = c.metrics();
            c.shutdown().await;
            m
        });
        assert!(m.fsync_degraded);
        assert!(m.fsync_degraded_trips >= 1);
        assert_eq!(m.fsync_threshold_nanos, 0);
    }

    // -- bn-u6o item 2: oversized-batch pre-check -------------------------

    /// A batch bigger than a whole EMPTY segment can never fit no matter how
    /// many times the committer rolls — every fresh segment has exactly the
    /// same `empty_segment_capacity`. Before this bone, the `SegmentFull`
    /// branch always rolled first and retried, so this shape wasted a
    /// near-empty segment (and its `on_rolled` notification) before
    /// re-failing with the identical typed error. The fix checks
    /// `needed > writer.empty_segment_capacity()` BEFORE rolling: the typed
    /// error must come back immediately, with NO roll (nothing sent on the
    /// roll channel).
    #[test]
    fn oversized_batch_skips_the_wasted_roll_and_surfaces_typed_error() {
        let rt = SimRuntime::new(21);
        let fs = rt.fs();
        // 52-byte header + exactly 128 bytes of room — matches
        // `writer_suite`'s `writer_rolls_on_segment_full` sizing, so one
        // 128-byte batch fits and a second copy of it does not.
        let mut params = SegmentParams::new(10, 0, 100, 0);
        params.segment_size = 180;
        let writer =
            SegmentWriter::create(&fs, Path::new("/seg-oversized"), params)
                .unwrap();

        let (roll_tx, roll_rx) = std::sync::mpsc::channel();
        let roller = Roller::new(
            |id| PathBuf::from(format!("/seg-oversized-{id}")),
            roll_tx,
        );

        // A single batch with a 512-byte payload: its encoded length is well
        // past 128 bytes (the capacity of this writer's segment_size EMPTY),
        // so it can never fit even a fresh segment.
        let big = AppendRequest {
            stream_id:            1,
            category_id:          0,
            first_stream_version: 0,
            events:               vec![EventInput::plain(
                1,
                1,
                0,
                vec![0xABu8; 512],
            )],
        };

        let err = rt.block_on(async {
            let c =
                Committer::spawn_with_roll(&rt, writer, Durability::Os, roller);
            let err = c.append(big).await.unwrap_err();
            c.shutdown().await;
            err
        });

        match err {
            AppendError::SegmentFull { needed, .. } => {
                assert!(
                    needed > 128,
                    "the batch must genuinely exceed an empty segment: \
                     needed={needed}"
                );
            }
            other => panic!("expected a typed SegmentFull, got {other:?}"),
        }
        assert!(
            roll_rx.try_recv().is_err(),
            "an oversized batch must not roll at all — no SegmentSummary \
             should ever be reported"
        );
    }

    // -- Group mode: concurrent appenders, watermark covers every ack ----

    #[test]
    fn group_mode_watermark_covers_every_ack() {
        // Several seeds so different interleavings are exercised.
        for seed in 0..24u64 {
            let rt = SimRuntime::new(seed);
            let fs = rt.fs();
            let path = Path::new("/seg-grp");
            let writer = seg(&fs, path);
            let total = rt.block_on(async {
                let c =
                    Committer::spawn(&rt, writer, Durability::group_default());
                let wm = c.watermark();
                // 4 concurrent writers, distinct streams, 3 batches each.
                let mut joins = Vec::new();
                for w in 0..4u64 {
                    let ap = c.appender();
                    let wm = wm.clone();
                    joins.push(rt.spawn(async move {
                        for b in 0..3u64 {
                            let out =
                                ap.append(req(w, b * 4, 4)).await.unwrap();
                            let AppendOutcome::Acked { last_position, .. } =
                                out
                            else {
                                panic!("group must ack");
                            };
                            assert!(
                                wm.get() > last_position,
                                "ack before watermark covered it"
                            );
                        }
                    }));
                }
                for j in joins {
                    j.await;
                }
                let total = wm.get();
                c.shutdown().await;
                total
            });
            // 4 writers × 3 batches × 4 events = 48 durable positions.
            assert_eq!(total, 48, "seed {seed}: all events durable");
        }
    }

    // -- The crash test: acks strictly after the covering barrier --------
    //
    // A `FreezeFs` wraps the sim fs; when frozen, `fdatasync` fails without
    // making bytes durable — modelling a crash landing *between the write
    // and the barrier*. The committer must NOT ack such a batch (it earns
    // Indeterminate), and after a crash keeping only the synced prefix, the
    // acked batch survives and the frozen one is gone.

    #[derive(Clone)]
    struct FreezeFs {
        inner:  SimFs,
        frozen: Arc<AtomicBool>,
    }
    #[derive(Clone)]
    struct FreezeFile {
        inner:  <SimFs as Fs>::File,
        frozen: Arc<AtomicBool>,
    }

    #[test]
    fn acks_strictly_after_covering_barrier() {
        // Sim fs on the torn-tail medium so a crash truncates cleanly.
        let sim_fs = SimFs::new(Fault::Tail);
        let rt = SimRuntime::new(1);
        let frozen = Arc::new(AtomicBool::new(false));
        let ffs = FreezeFs { inner: sim_fs.clone(), frozen: frozen.clone() };
        let path = Path::new("/seg-crash");

        // Batch 1 is 3 events; know its on-disk end so we can assert the
        // truncation point after the crash.
        let b1 = req(1, 0, 3);
        let len1 = {
            let subs = subframes_of(&b1.events);
            let input = BatchInput {
                segment_epoch:        1,
                batch_id:             0,
                first_global_pos:     0,
                stream_id:            b1.stream_id,
                category_id:          b1.category_id,
                first_stream_version: b1.first_stream_version,
                crypto_chain:         None,
                subframes:            &subs,
            };
            BatchEncoder::total_len(&input).unwrap()
        };
        let b1_end = crate::format::SEGMENT_HEADER_LEN as u64 + len1;

        let writer = seg(&ffs, path);
        let (o1, o2) = rt.block_on(async {
            let c = Committer::spawn(&rt, writer, Durability::Os);
            let o1 = c.append(b1).await.unwrap(); // real barrier → Acked
            frozen.store(true, Ordering::SeqCst); // next barrier fails
            let o2 = c.append(req(1, 3, 4)).await.unwrap(); // written, barrier fails
            c.shutdown().await;
            (o1, o2)
        });

        assert!(
            matches!(
                o1,
                AppendOutcome::Acked {
                    first_position: 0,
                    last_position: 2,
                    ..
                }
            ),
            "batch 1 got a real barrier: {o1:?}"
        );
        assert_eq!(
            o2,
            AppendOutcome::Indeterminate,
            "batch 2's barrier failed: it must NOT be acked"
        );

        // Crash keeping only the synced prefix (keep=0 clamps up to the
        // fdatasync watermark, which is batch 1's end — batch 2 was never
        // synced).
        sim_fs
            .crash(
                path,
                CrashPlan::Tail(TailPlan { keep: 0, scramble: vec![] }),
            )
            .unwrap();

        let g = sim_fs.open(path, OpenOpts::read_only()).unwrap();
        assert_eq!(
            g.len().unwrap(),
            b1_end,
            "acked batch 1 survives; unacked batch 2 truncated"
        );
        // Batch 1's header magic is intact at its offset.
        let mut magic = [0u8; 4];
        g.pread(crate::format::SEGMENT_HEADER_LEN as u64, &mut magic).unwrap();
        assert_eq!(
            u32::from_le_bytes(magic),
            crate::format::HEADER_MAGIC,
            "acked batch header survived the crash"
        );
        // Nothing at batch 2's would-be offset.
        let mut tail = [0u8; 4];
        assert_eq!(g.pread(b1_end, &mut tail).unwrap(), 0, "batch 2 is gone");
    }

    impl Fs for FreezeFs {
        type File = FreezeFile;

        fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<FreezeFile> {
            Ok(FreezeFile {
                inner:  self.inner.open(path, opts)?,
                frozen: self.frozen.clone(),
            })
        }

        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.inner.rename(from, to)
        }
    }

    impl FileHandle for FreezeFile {
        fn pwrite(&self, off: u64, buf: &[u8]) -> io::Result<usize> {
            self.inner.pwrite(off, buf)
        }

        fn pread(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
            self.inner.pread(off, buf)
        }

        fn fdatasync(&self) -> io::Result<()> {
            if self.frozen.load(Ordering::SeqCst) {
                // Barrier fault: bytes are NOT made durable.
                Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "frozen barrier",
                ))
            } else {
                self.inner.fdatasync()
            }
        }

        fn len(&self) -> io::Result<u64> { self.inner.len() }
    }

    // -- D8: fsync-EIO/ENOSPC poisoning + degraded reads (bn-25e) ---------
    //
    // The full D8 policy (docs/spec/03 §2.6): ANY barrier failure => the
    // durable state is unknowable => permanent, sticky poison. A
    // `BarrierFaultFs` wraps the sim fs and returns a CHOSEN errno from
    // `fdatasync` once armed, without promoting shadow → durable (a real
    // barrier that reports failure having flushed nothing). It also COUNTS
    // every `fdatasync` attempt, so a test can prove the failed barrier is
    // never retried — not at the next append, not at shutdown/close (the
    // fsyncgate discipline: retrying `fdatasync` after EIO is the classic
    // corruption).

    #[derive(Clone)]
    struct BarrierFaultFs {
        inner: SimFs,
        /// `0` = healthy; otherwise the raw errno `fdatasync` returns.
        errno: Arc<AtomicI32>,
        /// Total `fdatasync` attempts across every handle to this fs.
        syncs: Arc<AtomicUsize>,
    }
    #[derive(Clone)]
    struct BarrierFaultFile {
        inner: <SimFs as Fs>::File,
        errno: Arc<AtomicI32>,
        syncs: Arc<AtomicUsize>,
    }

    impl Fs for BarrierFaultFs {
        type File = BarrierFaultFile;

        fn open(
            &self,
            path: &Path,
            opts: OpenOpts,
        ) -> io::Result<BarrierFaultFile> {
            Ok(BarrierFaultFile {
                inner: self.inner.open(path, opts)?,
                errno: self.errno.clone(),
                syncs: self.syncs.clone(),
            })
        }

        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.inner.rename(from, to)
        }

        fn remove(&self, path: &Path) -> io::Result<()> {
            self.inner.remove(path)
        }
    }

    impl FileHandle for BarrierFaultFile {
        fn pwrite(&self, off: u64, buf: &[u8]) -> io::Result<usize> {
            self.inner.pwrite(off, buf)
        }

        fn pread(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
            self.inner.pread(off, buf)
        }

        fn fdatasync(&self) -> io::Result<()> {
            self.syncs.fetch_add(1, Ordering::SeqCst);
            let errno = self.errno.load(Ordering::SeqCst);
            if errno != 0 {
                // Barrier fault: bytes are NOT promoted to durable.
                Err(io::Error::from_raw_os_error(errno))
            } else {
                self.inner.fdatasync()
            }
        }

        fn len(&self) -> io::Result<u64> { self.inner.len() }

        fn allocate(&self, len: u64) -> io::Result<()> {
            self.inner.allocate(len)
        }
    }

    fn direct_fault_core(
        rt: &SimRuntime,
        durability: Durability,
        fs: &BarrierFaultFs,
        path: &'static str,
    ) -> DirectCommitter<SimRuntime, BarrierFaultFs> {
        let writer = seg(fs, Path::new(path));
        let (roll_tx, _roll_rx) = std::sync::mpsc::channel();
        let prefix = path.to_string();
        let roller = Roller::new(
            move |id| PathBuf::from(format!("{prefix}-{id}")),
            roll_tx,
        );
        DirectCommitter::with_roll_chained(
            rt,
            writer,
            durability,
            roller,
            ChainInit::off(),
        )
    }

    #[test]
    fn direct_group_uses_one_barrier_and_returns_dense_ordered_outcomes() {
        let rt = SimRuntime::new(91);
        let fs = BarrierFaultFs {
            inner: SimFs::new(Fault::Tail),
            errno: Arc::new(AtomicI32::new(0)),
            syncs: Arc::new(AtomicUsize::new(0)),
        };
        let mut direct = direct_fault_core(
            &rt,
            Durability::group_default(),
            &fs,
            "/direct-group",
        );
        fs.syncs.store(0, Ordering::SeqCst);
        let mut outcomes: Vec<Vec<DirectBatchOutcome>> =
            (0..3).map(|_| Vec::new()).collect();
        direct
            .commit_ordered_group(
                vec![
                    vec![req(1, 0, 2).into()],
                    vec![req(2, 0, 3).into()],
                    vec![req(3, 0, 1).into()],
                ],
                |unit, batch, outcome| {
                    assert_eq!(batch, outcomes[unit].len());
                    outcomes[unit].push(outcome);
                },
            )
            .unwrap();
        let spans: Vec<(u64, u64)> = outcomes
            .into_iter()
            .map(|mut unit| match unit.remove(0).outcome.unwrap() {
                AppendOutcome::Acked {
                    first_position, last_position, ..
                } => (first_position, last_position),
                AppendOutcome::Indeterminate => panic!("healthy group"),
            })
            .collect();
        assert_eq!(spans, vec![(0, 1), (2, 4), (5, 5)]);
        assert_eq!(fs.syncs.load(Ordering::SeqCst), 1);
        assert_eq!(direct.watermark().get(), 6);
    }

    #[test]
    fn direct_plain_preflight_matches_canonical_encoder() {
        let chain = [0u8; crate::format::CHAIN_LEN];
        for count in [1usize, DIRECT_INLINE_SUBFRAMES, 17, 1000] {
            let request = req(7, 9, count);
            let subframes = subframes_of(&request.events);
            for chain_enabled in [false, true] {
                let canonical = BatchEncoder::total_len(&BatchInput {
                    segment_epoch:        0,
                    batch_id:             0,
                    first_global_pos:     0,
                    stream_id:            request.stream_id,
                    category_id:          request.category_id,
                    first_stream_version: request.first_stream_version,
                    crypto_chain:         chain_enabled.then_some(&chain),
                    subframes:            &subframes,
                });
                assert_eq!(
                    direct_total_len(&request.events, chain_enabled),
                    canonical,
                    "count={count} chain={chain_enabled}"
                );
            }
        }
        assert_eq!(direct_total_len(&[], false), Err(EncodeError::EmptyBatch));
    }

    #[test]
    fn direct_os_uses_one_barrier_per_batch() {
        let rt = SimRuntime::new(92);
        let fs = BarrierFaultFs {
            inner: SimFs::new(Fault::Tail),
            errno: Arc::new(AtomicI32::new(0)),
            syncs: Arc::new(AtomicUsize::new(0)),
        };
        let mut direct =
            direct_fault_core(&rt, Durability::Os, &fs, "/direct-os");
        fs.syncs.store(0, Ordering::SeqCst);
        let mut outcomes = Vec::new();
        direct
            .commit_ordered_group(
                vec![vec![req(1, 0, 1).into(), req(1, 1, 1).into()]],
                |unit, batch, outcome| {
                    assert_eq!(unit, 0);
                    assert_eq!(batch, outcomes.len());
                    outcomes.push(outcome);
                },
            )
            .unwrap();
        assert!(
            outcomes
                .iter()
                .all(|o| matches!(o.outcome, Ok(AppendOutcome::Acked { .. })))
        );
        assert_eq!(fs.syncs.load(Ordering::SeqCst), 2);
        assert_eq!(direct.metrics().groups, 2);
    }

    #[test]
    fn direct_scratch_is_index_aligned_observable_and_sheds_oversize_capacity()
    {
        let rt = SimRuntime::new(94);
        let fs = BarrierFaultFs {
            inner: SimFs::new(Fault::Tail),
            errno: Arc::new(AtomicI32::new(0)),
            syncs: Arc::new(AtomicUsize::new(0)),
        };
        let mut direct =
            direct_fault_core(&rt, Durability::Process, &fs, "/direct-cap");
        let count = DIRECT_RETAINED_BATCHES + 1;
        let units =
            (0..count).map(|i| vec![req(i as u64 + 1, 0, 1).into()]).collect();
        let mut completed = 0;
        direct
            .commit_ordered_group(units, |unit, batch, outcome| {
                assert_eq!(unit, completed);
                assert_eq!(batch, 0);
                assert!(matches!(
                    outcome.outcome,
                    Ok(AppendOutcome::Acked { .. })
                ));
                completed += 1;
            })
            .unwrap();
        assert_eq!(completed, count);
        let metrics = direct.metrics();
        assert_eq!(metrics.direct_scratch_trims, 1);
        assert!(direct.scratch.requests.capacity() <= DIRECT_RETAINED_BATCHES);
        assert!(direct.scratch.effects.capacity() <= DIRECT_RETAINED_BATCHES);
        assert!(direct.scratch.indexes.capacity() <= DIRECT_RETAINED_BATCHES);
        assert!(
            metrics.direct_scratch_retained_batches
                <= DIRECT_RETAINED_BATCHES as u64
        );
        let retained_bytes = direct.scratch.requests.capacity()
            * std::mem::size_of::<CommitReq>()
            + direct.scratch.effects.capacity()
                * std::mem::size_of::<CommitEffect>()
            + direct.scratch.indexes.capacity()
                * std::mem::size_of::<DirectIndex>();
        assert_eq!(
            metrics.direct_scratch_retained_bytes,
            retained_bytes as u64
        );
        assert!(
            metrics.direct_scratch_retained_bytes
                <= DIRECT_RETAINED_BYTE_CAP as u64
        );
        direct
            .commit_ordered_group(
                vec![vec![req(1, 1, DIRECT_INLINE_SUBFRAMES + 1).into()]],
                |unit, batch, outcome| {
                    assert_eq!((unit, batch), (0, 0));
                    assert!(matches!(
                        outcome.outcome,
                        Ok(AppendOutcome::Acked { .. })
                    ));
                },
            )
            .unwrap();
        let metrics = direct.metrics();
        assert_eq!(metrics.direct_subframe_spills, 1);
        assert_eq!(metrics.direct_scratch_trims, 1);
        let retained_batches = metrics.direct_scratch_retained_batches;
        let retained_bytes = metrics.direct_scratch_retained_bytes;
        let mut completed = 0;
        direct
            .commit_ordered_group(
                vec![
                    vec![req(2, 0, DIRECT_INLINE_SUBFRAMES + 1).into()],
                    vec![req(3, 0, DIRECT_INLINE_SUBFRAMES + 2).into()],
                ],
                |unit, batch, outcome| {
                    assert_eq!((unit, batch), (completed, 0));
                    assert!(matches!(
                        outcome.outcome,
                        Ok(AppendOutcome::Acked { .. })
                    ));
                    completed += 1;
                },
            )
            .unwrap();
        assert_eq!(completed, 2);
        let metrics = direct.metrics();
        assert_eq!(metrics.direct_subframe_spills, 3);
        assert_eq!(metrics.direct_scratch_retained_batches, retained_batches);
        assert_eq!(metrics.direct_scratch_retained_bytes, retained_bytes);
    }

    #[test]
    fn direct_ordered_unit_keeps_partial_success_and_aborts_later_members() {
        let rt = SimRuntime::new(95);
        let fs = rt.fs();
        let mut params = SegmentParams::new(10, 0, 100, 0);
        // Exactly one 132-byte small batch fits in the 200-byte content area.
        params.segment_size = 252;
        let writer =
            SegmentWriter::create(&fs, Path::new("/direct-unit"), params)
                .unwrap();
        let (roll_tx, roll_rx) = std::sync::mpsc::channel();
        let roller = Roller::new(
            |id| PathBuf::from(format!("/direct-unit-{id}")),
            roll_tx,
        );
        let mut direct = DirectCommitter::with_roll_chained(
            &rt,
            writer,
            Durability::Process,
            roller,
            ChainInit::off(),
        );
        let too_large = AppendRequest {
            stream_id:            1,
            category_id:          0,
            first_stream_version: 1,
            events:               vec![EventInput::plain(
                1,
                1,
                0,
                vec![0xAB; 512],
            )],
        };
        let mut outcomes = Vec::new();
        direct
            .commit_ordered_group(
                vec![vec![
                    req(1, 0, 1).into(),
                    too_large.into(),
                    req(1, 2, 1).into(),
                ]],
                |unit, batch, outcome| {
                    assert_eq!(unit, 0);
                    assert_eq!(batch, outcomes.len());
                    outcomes.push(outcome.outcome);
                },
            )
            .unwrap();
        assert!(matches!(outcomes[0], Ok(AppendOutcome::Acked { .. })));
        assert!(matches!(outcomes[1], Err(AppendError::SegmentFull { .. })));
        assert_eq!(outcomes[2], Err(AppendError::UnitAborted));
        assert_eq!(direct.watermark().get(), 1);
        assert_eq!(direct.metrics().batches, 1);
        assert!(roll_rx.try_recv().is_err());
    }

    #[test]
    fn direct_live_roll_preserves_dense_outcomes_and_reports_segment() {
        let rt = SimRuntime::new(96);
        let fs = rt.fs();
        let mut params = SegmentParams::new(10, 0, 100, 0);
        // 52-byte segment header + 200 bytes: one 132-byte small batch fits,
        // and the second forces a live roll into the same-sized segment.
        params.segment_size = 252;
        let writer =
            SegmentWriter::create(&fs, Path::new("/direct-roll"), params)
                .unwrap();
        let (roll_tx, roll_rx) = std::sync::mpsc::channel();
        let roller = Roller::new(
            |id| PathBuf::from(format!("/direct-roll-{id}")),
            roll_tx,
        );
        let mut direct = DirectCommitter::with_roll_chained(
            &rt,
            writer,
            Durability::Process,
            roller,
            ChainInit::off(),
        );
        let mut placed = Vec::new();
        direct
            .commit_ordered_group(
                vec![vec![req(1, 0, 1).into(), req(1, 1, 1).into()]],
                |_, _, outcome| placed.push(outcome.outcome.unwrap()),
            )
            .unwrap();
        let positions: Vec<_> = placed
            .iter()
            .map(|outcome| match outcome {
                AppendOutcome::Acked { first_position, segment_id, .. } => {
                    (*first_position, *segment_id)
                }
                AppendOutcome::Indeterminate => panic!("healthy roll"),
            })
            .collect();
        assert_eq!(positions, vec![(0, 10), (1, 11)]);
        let rolled = roll_rx.try_recv().expect("old segment reported");
        assert_eq!(rolled.segment_id, 10);
        assert_eq!(direct.watermark().get(), 2);
    }

    #[test]
    fn direct_roll_enospc_keeps_prefix_and_aborts_dependent_member() {
        let rt = SimRuntime::new(97);
        let fs = rt.fs();
        let mut params = SegmentParams::new(10, 0, 100, 0);
        params.segment_size = 252;
        let writer =
            SegmentWriter::create(&fs, Path::new("/direct-enospc"), params)
                .unwrap();
        let next = Path::new("/direct-enospc-11");
        fs.inject_enospc(next, EnospcSite::Allocate);
        let (roll_tx, roll_rx) = std::sync::mpsc::channel();
        let roller = Roller::new(
            |id| PathBuf::from(format!("/direct-enospc-{id}")),
            roll_tx,
        );
        let mut direct = DirectCommitter::with_roll_chained(
            &rt,
            writer,
            Durability::Process,
            roller,
            ChainInit::off(),
        );
        let mut outcomes = Vec::new();
        direct
            .commit_ordered_group(
                vec![vec![
                    req(1, 0, 1).into(),
                    req(1, 1, 1).into(),
                    req(1, 2, 1).into(),
                ]],
                |_, _, outcome| outcomes.push(outcome.outcome),
            )
            .unwrap();
        assert!(matches!(outcomes[0], Ok(AppendOutcome::Acked { .. })));
        assert_eq!(outcomes[1], Err(AppendError::StoreFull));
        assert_eq!(outcomes[2], Err(AppendError::UnitAborted));
        assert_eq!(direct.watermark().get(), 1);
        assert!(roll_rx.try_recv().is_err(), "failed roll is not reported");
        assert!(fs.open(next, OpenOpts::read_only()).is_err());
    }

    #[test]
    fn direct_barrier_eio_is_indeterminate_sticky_and_never_retried_on_drop() {
        let rt = SimRuntime::new(93);
        let fs = BarrierFaultFs {
            inner: SimFs::new(Fault::Tail),
            errno: Arc::new(AtomicI32::new(0)),
            syncs: Arc::new(AtomicUsize::new(0)),
        };
        let mut direct = direct_fault_core(
            &rt,
            Durability::group_default(),
            &fs,
            "/direct-eio",
        );
        fs.syncs.store(0, Ordering::SeqCst);
        fs.errno.store(libc::EIO, Ordering::SeqCst);
        let mut first = None;
        direct
            .commit_ordered_group(
                vec![vec![req(1, 0, 1).into()]],
                |unit, batch, outcome| {
                    assert_eq!((unit, batch), (0, 0));
                    first = Some(outcome);
                },
            )
            .unwrap();
        assert_eq!(
            first.expect("direct outcome").outcome,
            Ok(AppendOutcome::Indeterminate)
        );
        assert!(direct.is_degraded());
        assert_eq!(direct.watermark().get(), 0);
        assert!(matches!(
            direct.commit_ordered_group(
                vec![vec![req(1, 0, 1).into()]],
                |_, _, _| {}
            ),
            Err(AppendError::StorePoisoned)
        ));
        assert_eq!(fs.syncs.load(Ordering::SeqCst), 1);
        drop(direct);
        assert_eq!(
            fs.syncs.load(Ordering::SeqCst),
            1,
            "poisoned drop must not retry the failed barrier"
        );
    }

    #[test]
    fn direct_barrier_enospc_is_indeterminate_and_freezes_watermark() {
        let rt = SimRuntime::new(98);
        let fs = BarrierFaultFs {
            inner: SimFs::new(Fault::Tail),
            errno: Arc::new(AtomicI32::new(0)),
            syncs: Arc::new(AtomicUsize::new(0)),
        };
        let mut direct = direct_fault_core(
            &rt,
            Durability::group_default(),
            &fs,
            "/direct-enospc-barrier",
        );
        fs.syncs.store(0, Ordering::SeqCst);
        fs.errno.store(libc::ENOSPC, Ordering::SeqCst);
        let mut outcome = None;
        direct
            .commit_ordered_group(
                vec![vec![req(1, 0, 1).into()]],
                |_, _, completed| outcome = Some(completed.outcome),
            )
            .unwrap();
        assert_eq!(outcome, Some(Ok(AppendOutcome::Indeterminate)));
        assert!(direct.is_degraded());
        assert_eq!(direct.watermark().get(), 0);
        assert_eq!(fs.syncs.load(Ordering::SeqCst), 1);
    }

    /// One good `Os`-mode append (barrier succeeds), then a second whose
    /// barrier fails with EIO: assert the FULL D8 policy in one shot — the
    /// failed group is `Indeterminate` (never acked), the store is poisoned
    /// (cause EIO), the watermark is frozen at the last known-durable
    /// position, every subsequent write fails fast typed, the degraded flag
    /// is readable, and the failed barrier is never retried (no extra
    /// `fdatasync`, at the poisoning append or at shutdown).
    #[test]
    fn barrier_eio_poisons_store_full_d8_policy() {
        let rt = SimRuntime::new(5);
        let sim_fs = SimFs::new(Fault::Tail);
        let errno = Arc::new(AtomicI32::new(0));
        let syncs = Arc::new(AtomicUsize::new(0));
        let bfs = BarrierFaultFs {
            inner: sim_fs,
            errno: errno.clone(),
            syncs: syncs.clone(),
        };
        let path = Path::new("/seg-eio");
        let writer = seg(&bfs, path);

        let r = rt.block_on(async {
            let c = Committer::spawn(&rt, writer, Durability::Os);
            let wm = c.watermark();

            let o1 = c.append(req(1, 0, 3)).await.unwrap(); // real barrier → Acked
            assert!(!c.is_degraded(), "healthy after a good barrier");
            let wm_good = wm.get();

            // Arm EIO on the NEXT barrier.
            errno.store(libc::EIO, Ordering::SeqCst);
            let o2 = c.append(req(1, 3, 4)).await.unwrap(); // written, barrier EIO
            let degraded = c.is_degraded();
            let cause = c.poison_cause();
            let wm_frozen = wm.get();
            let syncs_at_poison = syncs.load(Ordering::SeqCst);

            // Every later write fails fast, and the store stays poisoned no
            // matter how many attempts (sticky, no reset).
            let mut later = Vec::new();
            for v in [7u64, 9, 11] {
                later.push(c.append(req(1, v, 2)).await);
                assert!(
                    c.is_degraded(),
                    "poison is sticky across the lifetime"
                );
            }

            c.shutdown().await;
            let syncs_after_shutdown = syncs.load(Ordering::SeqCst);
            (
                o1,
                o2,
                degraded,
                cause,
                wm_good,
                wm_frozen,
                later,
                syncs_at_poison,
                syncs_after_shutdown,
            )
        });
        let (
            o1,
            o2,
            degraded,
            cause,
            wm_good,
            wm_frozen,
            later,
            syncs_at_poison,
            syncs_after,
        ) = r;

        assert!(
            matches!(
                o1,
                AppendOutcome::Acked {
                    first_position: 0,
                    last_position: 2,
                    ..
                }
            ),
            "batch 1 earned a real barrier: {o1:?}"
        );
        assert_eq!(wm_good, 3, "watermark covers the acked prefix [0,3)");
        assert_eq!(
            o2,
            AppendOutcome::Indeterminate,
            "the failed barrier's group is Indeterminate, never Acked \
             (fsyncgate)"
        );
        assert!(degraded, "an EIO barrier poisons the whole store");
        assert_eq!(
            cause,
            Some(PoisonCause::Eio),
            "cause is retained for diagnostics"
        );
        assert_eq!(
            wm_frozen, 3,
            "watermark is FROZEN at the last known-durable position"
        );
        assert!(
            later.iter().all(|r| *r == Err(AppendError::StorePoisoned)),
            "every write after poison fails fast with a typed StorePoisoned: \
             {later:?}"
        );
        // header(1) + o1 barrier(1) + o2 faulted barrier(1) = 3, and NOTHING
        // after: the failed barrier is never retried, not by a later append
        // (they fail before the writer) nor by close() at shutdown.
        assert_eq!(
            syncs_at_poison, 3,
            "one barrier per: header, o1, o2's fault"
        );
        assert_eq!(
            syncs_after, 3,
            "poisoned shutdown drops the writer WITHOUT re-issuing fdatasync"
        );
    }

    /// The SAME policy via the `ENOSPC`-at-barrier path, injected through the
    /// sim fs's own fault set (`EnospcSite::Fdatasync`): poison, frozen
    /// watermark, fail-fast writes, cause `Enospc`.
    #[test]
    fn barrier_enospc_poisons_store_full_d8_policy() {
        let rt = SimRuntime::new(9);
        let sim_fs = SimFs::new(Fault::Tail);
        let path = Path::new("/seg-enospc");
        let writer = seg(&sim_fs, path);

        let (o2, degraded, cause, wm_frozen, o3) = rt.block_on(async {
            let c = Committer::spawn(&rt, writer, Durability::Os);
            let wm = c.watermark();
            let _o1 = c.append(req(1, 0, 3)).await.unwrap(); // good barrier, wm → 3

            // Arm ENOSPC on the next fdatasync (one-shot, FIFO).
            sim_fs.inject_enospc(path, EnospcSite::Fdatasync);
            let o2 = c.append(req(1, 3, 4)).await.unwrap(); // barrier ENOSPC
            let degraded = c.is_degraded();
            let cause = c.poison_cause();
            let wm_frozen = wm.get();
            let o3 = c.append(req(1, 7, 2)).await; // fail fast
            c.shutdown().await;
            (o2, degraded, cause, wm_frozen, o3)
        });

        assert_eq!(
            o2,
            AppendOutcome::Indeterminate,
            "ENOSPC barrier group is Indeterminate"
        );
        assert!(
            degraded,
            "an ENOSPC barrier poisons the store, same policy as EIO"
        );
        assert_eq!(cause, Some(PoisonCause::Enospc));
        assert_eq!(wm_frozen, 3, "watermark frozen at the durable prefix");
        assert_eq!(
            o3,
            Err(AppendError::StorePoisoned),
            "writes fail fast after ENOSPC poison"
        );
    }

    /// Degraded reads: after a barrier poisons the store, a `ReadView` on the
    /// same segment still serves — but only the pre-poison committed prefix,
    /// because the watermark it snapshots is frozen. The never-durable bytes
    /// of the poisoning batch sit in the medium yet are clamped away. The
    /// reader can also OBSERVE the store is degraded via the shared flag.
    #[test]
    fn degraded_reads_serve_only_the_pre_poison_prefix() {
        use crate::reader::ReadView;

        let rt = SimRuntime::new(3);
        let sim_fs = SimFs::new(Fault::Tail);
        let errno = Arc::new(AtomicI32::new(0));
        let syncs = Arc::new(AtomicUsize::new(0));
        let bfs = BarrierFaultFs { inner: sim_fs, errno: errno.clone(), syncs };
        let path = Path::new("/seg-degraded-read");
        let writer = seg(&bfs, path);

        let (prefix, degraded_flag) = rt.block_on(async {
            let c = Committer::spawn(&rt, writer, Durability::Os);
            // Two durable batches (3 + 2 events → positions [0,5)).
            c.append(req(1, 0, 3)).await.unwrap();
            c.append(req(1, 3, 2)).await.unwrap();

            // Poison on the next barrier.
            errno.store(libc::EIO, Ordering::SeqCst);
            let o = c.append(req(1, 5, 4)).await.unwrap();
            assert_eq!(o, AppendOutcome::Indeterminate);
            assert!(c.is_degraded());

            // A reader built from the committer's (now-frozen) watermark and
            // the shared degraded flag.
            let view = ReadView::new(bfs.clone(), path, c.watermark());
            let flag = c.degraded();
            let prefix = view.read_committed().unwrap();
            let degraded_flag = flag.is_poisoned();
            c.shutdown().await;
            (prefix, degraded_flag)
        });

        assert_eq!(prefix.watermark, 5, "clamped to the frozen durable end");
        assert_eq!(
            prefix.next_pos(),
            5,
            "reads reach exactly the last durable position"
        );
        assert_eq!(
            prefix.event_count(),
            5,
            "only the two pre-poison batches (3+2 events)"
        );
        assert_eq!(
            prefix.len(),
            2,
            "the never-durable third batch is NOT served"
        );
        assert!(degraded_flag, "a reader can query the degraded flag");
    }

    /// Group mode: a barrier failure poisons the store the same way, and the
    /// entire coalesced group that shared the failed barrier is downgraded to
    /// `Indeterminate` (none of its batches is acked), with the poison sticky
    /// for the whole convoy.
    #[test]
    fn group_barrier_eio_poisons_whole_group() {
        let rt = SimRuntime::new(2);
        let sim_fs = SimFs::new(Fault::Tail);
        let errno = Arc::new(AtomicI32::new(0));
        let syncs = Arc::new(AtomicUsize::new(0));
        let bfs = BarrierFaultFs { inner: sim_fs, errno: errno.clone(), syncs };
        let path = Path::new("/seg-grp-eio");
        let writer = seg(&bfs, path);

        let outcomes = rt.block_on(async {
            // Arm EIO before any append: the FIRST group's barrier fails, so
            // every batch coalesced into it must be Indeterminate.
            errno.store(libc::EIO, Ordering::SeqCst);
            let c = Committer::spawn(&rt, writer, Durability::group_default());
            let mut joins = Vec::new();
            for w in 0..4u64 {
                let ap = c.appender();
                joins.push(
                    rt.spawn(async move { ap.append(req(w, 0, 3)).await }),
                );
            }
            let mut outs = Vec::new();
            for j in joins {
                outs.push(j.await);
            }
            let degraded = c.is_degraded();
            c.shutdown().await;
            (outs, degraded)
        });
        let (outs, degraded) = outcomes;
        assert!(degraded, "the group's failed barrier poisons the store");
        // Every writer either shared the failed barrier (Indeterminate) or
        // arrived after the poison tripped (StorePoisoned) — none is Acked.
        for o in &outs {
            match o {
                Ok(AppendOutcome::Indeterminate)
                | Err(AppendError::StorePoisoned) => {}
                other => panic!(
                    "no batch may be acked after a failed barrier: {other:?}"
                ),
            }
        }
    }

    // -- Early-close beats sync-per-batch at 4 writers (RealRuntime) ------
    //
    // Regression-tested ratio with a generous margin. `Os` is sync-per-batch
    // (one fdatasync per append); `Group` coalesces the 4 concurrent writers
    // into far fewer barriers, so it must complete the same workload with
    // strictly fewer fsyncs and in less wall time.

    /// A scratch dir on a real persistent device (ext4 here) OUTSIDE the
    /// repo — `TMPDIR`/`$HOME/.cache/mess-test-tmp` (bn-2jr's shared
    /// self-sweeping namespace), not `std::env::temp_dir()`. `/tmp` is
    /// commonly `tmpfs`, where `fdatasync` is a no-op and any durability
    /// measurement is a fiction; a path inside the crate would pollute the
    /// source tree. This keeps the barrier a real device flush so the
    /// throughput numbers and the sync-per-batch/early-close ratio mean
    /// something.
    fn real_tmp(name: &str) -> mess_testkit::SweepingTempDir {
        mess_testkit::sweeping_temp_dir(name)
    }

    /// Run `writers × batches` appends of `events_per` events each under
    /// `durability` on the real runtime; return `(elapsed, fsyncs, events)`.
    fn run_real_workload(
        path: &Path,
        durability: Durability,
        writers: u64,
        batches: u64,
        events_per: usize,
    ) -> (Duration, u64, u64) {
        let rt = RealRuntime::new();
        let fs = rt.fs();
        let writer = SegmentWriter::create(
            &fs,
            path,
            SegmentParams {
                segment_size: 4 * 1024 * 1024 * 1024,
                ..SegmentParams::new(0, 0, 1, 0)
            },
        )
        .unwrap();
        // Pre-build every request BEFORE timing, so the measured window is
        // the committer's durable throughput, not each writer's per-append
        // payload allocation. This mirrors the reference workload, which
        // reuses a pre-encoded `Arc<Vec<Vec<u8>>>`: a writer's re-submit gap
        // after an ack must be small, or it reads as a spurious `active == 0`
        // and fragments the group (§2.4).
        let mut pool: Vec<Vec<AppendRequest>> = (0..writers)
            .map(|w| {
                (0..batches)
                    .map(|b| req(w, b * events_per as u64, events_per))
                    .collect()
            })
            .collect();

        rt.block_on(async {
            let c = Committer::spawn(&rt, writer, durability);
            let start = std::time::Instant::now();
            let mut joins = Vec::new();
            for reqs in pool.drain(..) {
                let ap = c.appender();
                joins.push(rt.spawn(async move {
                    for r in reqs {
                        let out = ap.append(r).await.unwrap();
                        assert!(matches!(out, AppendOutcome::Acked { .. }));
                    }
                }));
            }
            for j in joins {
                j.await;
            }
            let elapsed = start.elapsed();
            let fsyncs = c.fsync_count();
            c.shutdown().await;
            (elapsed, fsyncs, writers * batches * events_per as u64)
        })
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real filesystem + threads
    fn early_close_beats_sync_per_batch_at_4_writers() {
        let dir_os = real_tmp("committer-ratio-os");
        let dir_grp = real_tmp("committer-ratio-grp");
        let p_os = dir_os.path().join("seg");
        let p_grp = dir_grp.path().join("seg");

        let (os_t, os_fsyncs, _) =
            run_real_workload(&p_os, Durability::Os, 4, 60, 100);
        let (grp_t, grp_fsyncs, _) =
            run_real_workload(&p_grp, Durability::group_default(), 4, 60, 100);

        // Os is exactly one fsync per append (240 appends).
        assert_eq!(os_fsyncs, 240, "Os is sync-per-batch");
        // Group must coalesce: strictly fewer barriers, and the workload must
        // finish faster. Generous margins to avoid CI/scheduler flakiness.
        assert!(
            grp_fsyncs < os_fsyncs,
            "group commit must issue fewer barriers than sync-per-batch: \
             grp={grp_fsyncs} os={os_fsyncs}"
        );
        assert!(
            grp_t < os_t,
            "early-close group commit must beat sync-per-batch: grp={grp_t:?} \
             os={os_t:?}"
        );
    }

    // -- Perf floor: >=100k durable ev/s at 4×100 (release; ignored) -----
    //
    // Run manually with a release build to reproduce the §2.3 reference
    // number (measured 121k durable ev/s at 4×100; this committer reaches
    // ~130–140k on a settled device here). Ignored by default: a debug build
    // and the default `cargo test` gate is debug; more importantly, absolute
    // durable throughput is device state, not a constant (§2.6) — sustained
    // back-to-back barrier load degrades a near-full consumer SSD's fsync/s
    // several-fold until it idles for minutes (§1's methodology warning). So
    // this takes the BEST of several drift-controlled passes and asserts on
    // that, the way REPORT.md §1 measures — coalescing (events/fsync) is the
    // committer-controlled quantity and stays ~4.0 (near-ideal for 4 writers)
    // regardless of device drift.
    #[test]
    #[ignore = "perf: run with --release to reproduce >=100k durable ev/s"]
    fn perf_100k_durable_ev_per_s_4x100() {
        // Warm the device/page cache.
        let wdir = real_tmp("committer-perf-warm");
        let wpath = wdir.path().join("seg");
        let _ =
            run_real_workload(&wpath, Durability::group_default(), 4, 100, 100);

        let mut best_ev_per_s = 0.0f64;
        let mut best_per_fsync = 0.0f64;
        for pass in 0..5 {
            let dir = real_tmp("committer-perf-4x100");
            let path = dir.path().join("seg");
            let (elapsed, fsyncs, events) = run_real_workload(
                &path,
                Durability::group_default(),
                4,
                1500,
                100,
            );
            let ev_per_s = events as f64 / elapsed.as_secs_f64();
            let per_fsync = events as f64 / fsyncs.max(1) as f64;
            eprintln!(
                "perf 4x100 pass {pass}: {ev_per_s:.0} durable ev/s, {fsyncs} \
                 fsyncs ({per_fsync:.1} ev/fsync), {elapsed:?}"
            );
            if ev_per_s > best_ev_per_s {
                best_ev_per_s = ev_per_s;
                best_per_fsync = per_fsync;
            }
        }
        eprintln!(
            "perf 4x100 BEST: {best_ev_per_s:.0} durable ev/s at \
             {best_per_fsync:.1} ev/fsync"
        );
        // Coalescing must be near-ideal (≈4 batches/group for 4 writers)
        // independent of device drift — this is the committer's own quantity.
        assert!(
            best_per_fsync >= 350.0,
            "expected near-ideal coalescing (~4 batches/group), got \
             {best_per_fsync:.1} ev/fsync"
        );
        assert!(
            best_ev_per_s >= 100_000.0,
            "expected >=100k durable ev/s on a settled device, got \
             {best_ev_per_s:.0}"
        );
    }

    // -- bn-3da: Drop deterministically stops and joins the committer -----
    //
    // Before this bone, dropping a `Committer` only closed the gather
    // channel (via `Sender`/`Option` ref-counting): nothing confirmed the
    // task had actually finished, and an `Appender` clone that outlived the
    // `Committer` kept the channel open (and the task/thread alive)
    // forever. Both tests below bound every blocking call so a regression
    // back to that behaviour FAILS the test instead of hanging it.

    /// `RealRuntime`: the committer runs on its own OS thread
    /// (`std::thread::spawn` in `runtime::real`). Dropping the `Committer`
    /// while a cloned `Appender` is still alive must still join that thread
    /// promptly (no leak), and the lingering `Appender`'s next append must
    /// come back as a typed `Closed` error, not hang.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn drop_joins_the_real_thread_and_closes_lingering_appenders() {
        let rt = RealRuntime::new();
        let fs = rt.fs();
        let dir = real_tmp("committer-drop-join");
        let path = dir.path().join("seg");
        let writer =
            SegmentWriter::create(&fs, &path, SegmentParams::new(0, 0, 1, 0))
                .unwrap();

        let c = Committer::spawn(&rt, writer, Durability::Os);
        let ap = c.appender();

        // Prove the store is live, and leave the group's ack racing the
        // drop below (not awaited here) — the "pending append" the bone
        // asks for.
        let pending = {
            let rt2 = rt.clone();
            let ap2 = ap.clone();
            std::thread::spawn(move || rt2.block_on(ap2.append(req(1, 0, 2))))
        };
        let out = pending.join().unwrap().unwrap();
        assert!(
            matches!(out, AppendOutcome::Acked { .. }),
            "store must be live before the drop"
        );

        // Drop the Committer on its own thread while `ap` is STILL ALIVE.
        // Bounded: if `Drop` regressed to "only closes the channel", `ap`
        // being alive means sender ref-counting never reaches zero and the
        // join would hang forever instead of returning.
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            drop(c);
            let _ = done_tx.send(());
        });
        done_rx.recv_timeout(Duration::from_secs(5)).expect(
            "Committer::drop must join the committer thread promptly, not hang",
        );

        // The committer is gone. The lingering `Appender` clone did NOT
        // keep it alive (bn-3da): its next append must fail typed and
        // fast — never silently hang awaiting an ack that will never be
        // fulfilled.
        let (err_tx, err_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let r = rt.block_on(ap.append(req(1, 2, 1)));
            let _ = err_tx.send(r);
        });
        let err = err_rx.recv_timeout(Duration::from_secs(5)).expect(
            "a lingering Appender's append after Committer::drop must return, \
             not hang",
        );
        assert_eq!(
            err,
            Err(AppendError::Closed),
            "post-drop append on a lingering Appender must be a typed Closed \
             error"
        );
    }

    /// `SimRuntime`: a single-threaded, cooperative executor with NO
    /// independent progress of its own — tasks only advance while
    /// something calls `core.step()`. Dropping the `Committer` from
    /// *inside* `rt.block_on(...)` (so `Drop` must reentrantly drive the
    /// very same executor to join the committer task) is the scenario a
    /// naive thread-park `Drop` would deadlock: nothing else could ever
    /// step the committer task to its completion signal. `SimRuntime`'s own
    /// `block_on` panics loudly on a genuine deadlock ("sim runtime
    /// deadlock: ...") rather than hanging, so a regression here fails
    /// fast.
    #[test]
    fn drop_inside_the_sim_executor_does_not_deadlock() {
        let rt = SimRuntime::new(11);
        let fs = rt.fs();
        let path = Path::new("/seg-drop-sim");
        let writer = seg(&fs, path);

        let (out, closed_err) = rt.block_on(async {
            let c = Committer::spawn(&rt, writer, Durability::Os);
            let ap = c.appender();
            let out = ap.append(req(1, 0, 2)).await.unwrap();

            // Drop while `ap` is still alive, from within the executor's
            // own `block_on` — the reentrant-`block_on` path (`bn-3da`).
            drop(c);

            let closed_err = ap.append(req(1, 2, 1)).await;
            (out, closed_err)
        });

        assert!(matches!(out, AppendOutcome::Acked { .. }));
        assert_eq!(
            closed_err,
            Err(AppendError::Closed),
            "a lingering Appender's append after Committer::drop must be a \
             typed Closed error"
        );
    }
}
