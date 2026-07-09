//! Subscription runtime: the catch-up → live handoff of D11
//! (`docs/spec/06-subscriptions.md`, NORMATIVE), built on the real engine
//! primitives — the durable [`Watermark`](crate::watermark::Watermark) and
//! the paged, watermark-clamped [`ReadView`](crate::reader::ReadView).
//!
//! # The guarantee (spec §2)
//!
//! A subscription created at cursor `c` delivers **exactly** the committed
//! positions `c, c+1, …, watermark-1` in ascending order, with no gaps and
//! no duplicates, regardless of concurrent appends, consumer speed, or how
//! many times the subscriber falls behind and recovers.
//!
//! (The spec states the guarantee over positions with a 1-based cursor —
//! "deliver `c+1, c+2, …`". mess-log positions are 0-based with an
//! *exclusive* durable end, so this module takes `cursor` to be the **next
//! position to deliver**: a subscription at `cursor = 0` replays the whole
//! log; one at `cursor = watermark` is exactly-at-head. The delivered
//! sequence is `cursor .. final_watermark`. This is the same guarantee in
//! 0-based clothing — see the note on the `<` dedupe comparator below.)
//!
//! # Two sources, asymmetric authority (spec §3)
//!
//! - **History** — [`ReadView`] paged reads. *Authoritative.* Serves only
//!   positions `< watermark` (the D7 durable end): nothing unacknowledged
//!   is ever visible. A subscription that polled history exclusively would
//!   already satisfy §2, just with worse tail latency.
//! - **Live feed** — a bounded, per-subscriber broadcast buffer
//!   ([`LiveReceiver`]) fed by the [`LiveTap`] pump. *Optimization only,
//!   zero correctness weight.* Its whole job is to avoid a history
//!   round-trip per commit while caught up.
//!
//! # Writer obligation W1, satisfied by construction (spec §4)
//!
//! W1: for every committed position `p`, the watermark is advanced past `p`
//! **before** `p` is offered to any live buffer, and live-feed order equals
//! position order. This module honors W1 **without touching the commit
//! path** (`committer.rs` is untouched): the [`LiveTap`] pump is driven by
//! the watermark itself. It only ever publishes a position `p` *after*
//! `watermark.await_past(p)` has resolved (so `watermark > p`: `p` is
//! already durable and readable from history), and it publishes strictly
//! ascending positions from a single task (so publish order == position
//! order). Both clauses of W1 therefore hold by construction — the
//! committer never learns the live feed exists. See [`LiveTap`].
//!
//! The spike (`spikes/sub_handoff`) put W1 inside `Log::append`; here the
//! monotone durable watermark *is* the "published-only-once-durable"
//! boundary, so the pump is a pure downstream reader of it. The anomaly
//! counter (SUB8, [`SubMetrics::anomaly_regressions`]) is the tripwire that
//! proves this held: it MUST be 0 across the conformance suite.
//!
//! # State machine (spec §5–§8)
//!
//! ```text
//! states:   CatchUp -> Switching -> Live
//! overflow: {Switching, Live} --Lagged/anomaly--> CatchUp (last unchanged)
//! ```
//!
//! - **CatchUp** — page history from `cursor`; an empty page (proof we
//!   reached the watermark, SUB2) → Switching.
//! - **Switching** — history drained; draining the live buffer, no live
//!   delivery yet since entering.
//! - **Live** — delivered ≥1 live position since the last switch.
//!
//! On a received live position `p` (SUB4, the load-bearing `<=` rule):
//! `p < cursor` → drop (overlap dedupe), `p == cursor` → deliver, `p >
//! cursor` → anomaly regress. On a `Lagged` overflow → regress to CatchUp,
//! `cursor` unchanged (SUB6): lossy at the buffer, loud via a counter
//! (SUB5/SUB7), invisible to the consumer's delivered sequence.
//!
//! > **The `<` comparator is the spec's `<=`, not `==`.** The spec keeps a
//! > `last`-*delivered* cursor and drops `p <= last`. This module keeps
//! > `cursor` = *next to deliver* = `last + 1`, so `p <= last` becomes
//! > `p < cursor` and `p == last + 1` becomes `p == cursor`. Getting this
//! > wrong (`== last`, i.e. `p == cursor - 1` only) silently under-delivers
//! > after the *second* catch-up — see SUB4 and the spike's 52k dedupe
//! > drops. The `< cursor` branch below is that rule.
//!
//! # Conformance (spec §11)
//!
//! `tests/` ports the spike's randomized, adversarial, exact-sequence
//! suite against this real-engine implementation, including the naive
//! "catch-up-then-subscribe" negative control (§12(a)). See the module's
//! test section.

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::reader::ReadView;
use crate::runtime::{Fs, Runtime};
use crate::watermark::Watermark;

/// The default per-subscriber live-buffer capacity. The spec (§9) leaves
/// capacity an implementation parameter and only obligates the sizing
/// guidance: size it **at or above the typical commit-batch size**, because
/// a single burst larger than the buffer sends even an attentive subscriber
/// through a full history round-trip. Correctness is independent of the
/// value (the conformance suite property-tests capacities from 2 upward);
/// 64 is a reasonable default over the group-commit batch sizes of
/// `03-durability.md`. Prefer [`LiveTap::spawn_with_capacity`] to size it
/// against a known workload.
pub const DEFAULT_LIVE_CAPACITY: usize = 64;

// ===========================================================================
// Bounded broadcast — the live feed primitive (spec §3, §8; SUB5)
// ===========================================================================
//
// A multi-producer/one-producer, multi-consumer broadcast of positions with
// an independent bounded ring buffer per receiver cursor and an EXPLICIT
// overflow signal ([`RecvOutcome::Lagged`]). This is the mess-log analogue
// of the spike's `tokio::sync::broadcast`: mess-log has no tokio (it runs on
// its own [`Runtime`]), so the primitive is built here over `std` sync + a
// waker list, and works identically under the real and sim runtimes.
//
// Semantics (tokio-broadcast-shaped): the shared ring holds the most-recent
// `cap` positions. Each receiver tracks the next absolute sequence number it
// wants. If the producer laps a receiver (its wanted seq falls below the
// oldest still-buffered seq), the next `recv` yields `Lagged(missed)` and the
// receiver resumes at the oldest available position — never a silent gap
// (SUB5: the signal is distinct from both "value" and "closed").

/// The result of a [`LiveReceiver::recv`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvOutcome {
    /// The next live position, in order.
    Value(u64),
    /// The bounded buffer overflowed: `n` positions were evicted before
    /// this receiver read them. Loud, distinguishable-from-`Value` overflow
    /// (SUB5). The subscription treats it as the regress-to-CatchUp trigger
    /// (SUB6); nothing is lost because history is authoritative.
    Lagged(u64),
    /// Every sender has dropped and the buffer is drained: no position can
    /// ever arrive again.
    Closed,
}

struct BroadcastInner {
    /// Most-recent `cap` positions, oldest at the front.
    buf: VecDeque<u64>,
    cap: usize,
    /// Absolute sequence number of `buf.front()` (0 if empty). `next_seq -
    /// buf.len()`.
    base_seq: u64,
    /// Total positions ever pushed == sequence one past the newest.
    next_seq: u64,
    /// Live sender count; `Closed` is observable once it reaches 0.
    senders: usize,
    /// Parked receiver wakers, woken on every push/close (each re-registers
    /// on its next poll if still not satisfied — the watermark's discipline).
    wakers: Vec<Waker>,
}

impl BroadcastInner {
    fn wake_all(&mut self) {
        for w in self.wakers.drain(..) {
            w.wake();
        }
    }
}

/// The producing half of the live broadcast. Exactly one is held by the
/// [`LiveTap`] pump; when the pump task ends it drops, and receivers observe
/// [`RecvOutcome::Closed`] once drained.
struct LiveSender {
    inner: Arc<Mutex<BroadcastInner>>,
}

impl LiveSender {
    /// Publish `pos` to every receiver. If the ring is full the oldest
    /// buffered position is evicted (lagging receivers will observe the
    /// eviction as [`RecvOutcome::Lagged`]).
    fn send(&self, pos: u64) {
        let mut st = self.inner.lock().unwrap();
        if st.buf.len() == st.cap {
            st.buf.pop_front();
            st.base_seq += 1;
        }
        st.buf.push_back(pos);
        st.next_seq += 1;
        st.wake_all();
    }
}

impl Drop for LiveSender {
    fn drop(&mut self) {
        let mut st = self.inner.lock().unwrap();
        st.senders -= 1;
        if st.senders == 0 {
            st.wake_all();
        }
    }
}

/// A cheap-to-clone subscription factory for the live broadcast. Making a
/// receiver does **not** count as a sender, so it never keeps the channel
/// open; it only reads.
#[derive(Clone)]
pub struct LiveHandle {
    inner: Arc<Mutex<BroadcastInner>>,
}

impl LiveHandle {
    /// Attach a fresh receiver positioned at the **current tail**: it sees
    /// exactly the positions published *after* this call. This is the
    /// subscribe-before-read attach point of SUB1 — everything the pump
    /// publishes after attach is either received here or covered by an
    /// explicit `Lagged`.
    pub fn subscribe(&self) -> LiveReceiver {
        let read_seq = self.inner.lock().unwrap().next_seq;
        LiveReceiver { inner: self.inner.clone(), read_seq }
    }
}

/// The consuming half of the live broadcast: one independent bounded cursor
/// over the shared ring.
pub struct LiveReceiver {
    inner: Arc<Mutex<BroadcastInner>>,
    read_seq: u64,
}

impl LiveReceiver {
    /// Await the next [`RecvOutcome`]. Resolves immediately if a value (or a
    /// lag, or closure) is already available; otherwise parks until the next
    /// [`LiveSender::send`] or the sender drops.
    pub fn recv(&mut self) -> Recv<'_> {
        Recv { rx: self }
    }

    /// Non-blocking poll of the receiver's ring cursor. `None` means "no
    /// value, lag, or closure available right now" (would park).
    fn poll_next(&mut self, cx: Option<&mut Context<'_>>) -> Option<RecvOutcome> {
        let mut st = self.inner.lock().unwrap();
        if self.read_seq < st.base_seq {
            // Lapped: the producer overran this receiver. Resume at the
            // oldest still-buffered position; report how many were missed.
            let missed = st.base_seq - self.read_seq;
            self.read_seq = st.base_seq;
            return Some(RecvOutcome::Lagged(missed));
        }
        if self.read_seq < st.next_seq {
            let idx = (self.read_seq - st.base_seq) as usize;
            let v = st.buf[idx];
            self.read_seq += 1;
            return Some(RecvOutcome::Value(v));
        }
        // Caught up to the tail.
        if st.senders == 0 {
            return Some(RecvOutcome::Closed);
        }
        if let Some(cx) = cx {
            st.wakers.push(cx.waker().clone());
        }
        None
    }
}

/// The future returned by [`LiveReceiver::recv`].
pub struct Recv<'a> {
    rx: &'a mut LiveReceiver,
}

impl Future for Recv<'_> {
    type Output = RecvOutcome;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<RecvOutcome> {
        let me = self.get_mut();
        match me.rx.poll_next(Some(cx)) {
            Some(o) => Poll::Ready(o),
            None => Poll::Pending,
        }
    }
}

// ===========================================================================
// LiveTap — the watermark-driven pump (W1 by construction)
// ===========================================================================

/// A one-position notification with a waker list, used to signal pump
/// shutdown. (A minimal `Notify`; the runtime provides no such primitive.)
struct Notify {
    inner: Arc<Mutex<(bool, Vec<Waker>)>>,
}

impl Notify {
    fn new() -> Self {
        Notify { inner: Arc::new(Mutex::new((false, Vec::new()))) }
    }
    fn handle(&self) -> Notify {
        Notify { inner: self.inner.clone() }
    }
    fn fire(&self) {
        let mut st = self.inner.lock().unwrap();
        st.0 = true;
        for w in st.1.drain(..) {
            w.wake();
        }
    }
    fn wait(&self) -> NotifyWait {
        NotifyWait { inner: self.inner.clone() }
    }
}

struct NotifyWait {
    inner: Arc<Mutex<(bool, Vec<Waker>)>>,
}

impl Future for NotifyWait {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut st = self.inner.lock().unwrap();
        if st.0 {
            Poll::Ready(())
        } else {
            st.1.push(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Biased race of two `()` futures: polls `a` first, then `b`. Used by the
/// pump to wait on "watermark advanced" *or* "shutdown". Returns which
/// side fired (`true` = the `a`/watermark side).
struct Race<A, B> {
    a: A,
    b: B,
}

impl<A, B> Future for Race<A, B>
where
    A: Future<Output = ()> + Unpin,
    B: Future<Output = ()> + Unpin,
{
    type Output = bool;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<bool> {
        // Both futures are `Unpin` (the watermark `WaitFor` and `NotifyWait`
        // hold only `Arc`s and a `u64`), so no unsafe pin projection is
        // needed.
        let me = self.get_mut();
        if Pin::new(&mut me.a).poll(cx).is_ready() {
            return Poll::Ready(true);
        }
        if Pin::new(&mut me.b).poll(cx).is_ready() {
            return Poll::Ready(false);
        }
        Poll::Pending
    }
}

/// The live-feed pump: a downstream reader of the durable watermark that
/// fans committed positions out to per-subscriber [`LiveReceiver`]s,
/// **without any hook into the commit path** (`committer.rs` untouched).
///
/// W1 (spec §4) holds by construction here (see the module docs): a
/// position `p` is published only after `watermark.await_past(p)` resolves
/// (durable-before-publish), and the single pump task publishes strictly
/// ascending positions (publish-order == position-order). The committer
/// already advances the watermark past `p` only once `p` is durable
/// (`03-durability.md`), so the pump inherits both clauses for free.
///
/// One tap serves a whole store: any number of subscriptions attach via
/// [`LiveTap::subscribe`] / [`subscribe`](crate::subscription::subscribe).
pub struct LiveTap {
    handle: LiveHandle,
    watermark: Watermark,
    stop: Notify,
    done: Notify,
}

impl LiveTap {
    /// Spawn a pump for `watermark` on `rt` with the [`DEFAULT_LIVE_CAPACITY`]
    /// per-subscriber buffer.
    pub fn spawn<R: Runtime>(rt: &R, watermark: Watermark) -> LiveTap {
        Self::spawn_with_capacity(rt, watermark, DEFAULT_LIVE_CAPACITY)
    }

    /// Spawn a pump whose per-subscriber live buffer holds `capacity`
    /// positions. Size it at or above the typical commit-batch size (spec
    /// §9); `capacity` is clamped to at least 1.
    pub fn spawn_with_capacity<R: Runtime>(
        rt: &R,
        watermark: Watermark,
        capacity: usize,
    ) -> LiveTap {
        let cap = capacity.max(1);
        let inner = Arc::new(Mutex::new(BroadcastInner {
            buf: VecDeque::with_capacity(cap),
            cap,
            base_seq: 0,
            next_seq: 0,
            senders: 1,
            wakers: Vec::new(),
        }));
        let sender = LiveSender { inner: inner.clone() };
        let handle = LiveHandle { inner };
        let stop = Notify::new();
        let done = Notify::new();
        // The pump starts from the watermark's current value: it publishes
        // only positions committed from now on. Positions already durable at
        // attach time are served from history (authoritative) — the live
        // feed is only the tail optimization.
        let start = watermark.get();
        let pump_wm = watermark.clone();
        let pump_stop = stop.handle();
        let pump_done = done.handle();
        // Fire-and-forget, exactly like the committer: `Runtime::spawn`'s
        // join handle is a non-`'static` RPITIT borrowing `rt`, so it cannot
        // be stored. Shutdown is coordinated through the `done` notify the
        // pump fires on exit instead.
        drop(rt.spawn(async move {
            run_pump(pump_wm, sender, pump_stop, start).await;
            pump_done.fire();
        }));
        LiveTap { handle, watermark, stop, done }
    }

    /// Attach a receiver to the live feed (SUB1: before the first history
    /// read). Prefer [`subscribe`](crate::subscription::subscribe), which
    /// wires this to a [`ReadView`] and the state machine.
    pub fn subscribe(&self) -> LiveReceiver {
        self.handle.subscribe()
    }

    /// A clone of the underlying handle, for building subscriptions.
    pub fn handle(&self) -> LiveHandle {
        self.handle.clone()
    }

    /// The watermark this tap pumps (for building [`ReadView`]s / lag).
    pub fn watermark(&self) -> Watermark {
        self.watermark.clone()
    }

    /// Signal the pump to stop and await its exit. After this the tap no
    /// longer publishes (the pump's sender drops → receivers see `Closed`
    /// once drained).
    pub async fn shutdown(self) {
        self.stop.fire();
        self.done.wait().await;
    }
}

impl Drop for LiveTap {
    fn drop(&mut self) {
        // Best-effort: if the tap is dropped without an explicit
        // `shutdown().await`, still tell the pump to exit so a real-runtime
        // pump thread does not leak (we cannot await its exit here).
        self.stop.fire();
    }
}

async fn run_pump(watermark: Watermark, sender: LiveSender, stop: Notify, start: u64) {
    let mut cursor = start;
    loop {
        // Wake on the next watermark advance past `cursor` OR on shutdown.
        let advanced = Race { a: watermark.await_past(cursor), b: stop.wait() }.await;
        if !advanced {
            break; // shutdown
        }
        let wm = watermark.get();
        // Publish strictly ascending positions now known durable. Ascending
        // order + durable-before-publish == W1.
        while cursor < wm {
            sender.send(cursor);
            cursor += 1;
        }
    }
    // `sender` drops here → receivers observe `Closed` once drained.
}

// ===========================================================================
// Subscription — the D11 state machine (spec §5–§8)
// ===========================================================================

/// The observable state of a [`Subscription`] (spec §5). `Switching` and
/// `Live` share one code path; the distinction is purely observational
/// (has a live position been delivered since the last switch) and is
/// surfaced only for metrics/introspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubState {
    /// Paging through history from `cursor`.
    CatchUp,
    /// History drained; draining the live buffer, no live delivery yet
    /// since entering.
    Switching,
    /// Delivering live positions in order.
    Live,
}

/// Per-subscription counters (spec §7, §8, §9). All but [`lag`] are
/// monotone tallies of the interesting paths; the conformance suite asserts
/// [`anomaly_regressions`](SubMetrics::anomaly_regressions) is 0.
///
/// [`lag`]: Subscription::lag
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SubMetrics {
    /// Positions delivered from history (the authoritative source).
    pub history_delivered: u64,
    /// Positions delivered from the live feed.
    pub live_delivered: u64,
    /// Live positions dropped by the `< cursor` overlap dedupe (SUB4).
    pub dedupe_skips: u64,
    /// Regressions to CatchUp caused by a `Lagged` overflow (SUB6). Loud
    /// operational signal, never a consumer-visible error (SUB7).
    pub lag_regressions: u64,
    /// Regressions caused by an in-order gap with **no** preceding overflow
    /// signal (SUB8). Impossible under W1+SUB1; MUST be 0 across the
    /// conformance suite — a nonzero count is a broken W1 (publish escaped
    /// the durable-before-publish/ascending discipline).
    pub anomaly_regressions: u64,
    /// CatchUp → Switching transitions (empty-page switches, SUB2).
    pub switches: u64,
    /// History pages read (including the empty page that triggers a switch).
    pub catchup_pages: u64,
}

/// A subscription that could not (re)attach at its cursor because the log
/// end is behind it (spec §10, D7). Positions `log_end .. cursor` were
/// delivered but no longer exist after recovery; only the consumer's
/// application logic knows whether their downstream effects need
/// compensating, so this MUST surface rather than be silently re-subscribed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("cursor {cursor} regressed past log end {log_end} (positions {log_end}..{cursor} no longer exist)")]
pub struct CursorRegressed {
    /// The subscription's next-to-deliver cursor.
    pub cursor: u64,
    /// The freshly observed exclusive durable end (log end).
    pub log_end: u64,
}

/// An error from [`Subscription::next`]. Either a durability/recovery
/// condition the consumer must handle (`CursorRegressed`, SUB10) or an I/O
/// error from the authoritative history read.
#[derive(Debug, thiserror::Error)]
pub enum SubError {
    /// The cursor is past the recovered log end (spec §10).
    #[error(transparent)]
    CursorRegressed(#[from] CursorRegressed),
    /// A history [`ReadView`] read failed.
    #[error("history read: {0}")]
    Io(#[from] io::Error),
}

/// A catch-up → live subscription over one segment's committed prefix and
/// the store's live feed. Drives the D11 state machine; the consumer pulls
/// positions with [`next`](Subscription::next).
///
/// Delivered unit is the **global position** (`u64`) — the guarantee in
/// spec §2 is stated over positions, and this matches the conformance
/// suite's exact-sequence check. Payload/event hydration for a delivered
/// position is a thin follow-on read via the same [`ReadView`] and is out
/// of scope for the handoff protocol itself.
///
/// Build one with [`subscribe`](crate::subscription::subscribe).
pub struct Subscription<F: Fs> {
    view: ReadView<F>,
    rx: LiveReceiver,
    /// Next position to deliver. `cursor - 1` is the spec's `last`
    /// delivered; the `< cursor` dedupe below is the spec's `<= last`.
    cursor: u64,
    state: SubState,
    page_limit: usize,
    metrics: SubMetrics,
    /// Positions from the current history page not yet handed out.
    pending: VecDeque<u64>,
    /// Set once the live feed is observed closed AND history is drained.
    ended: bool,
    /// Shared lag view: the watermark, read on demand for `watermark -
    /// cursor` (SUB9).
    watermark: Watermark,
}

/// Create a subscription at `cursor` (next position to deliver) over
/// `view`, attached to `tap`'s live feed.
///
/// **SUB1 (subscribe-before-read):** the live receiver is attached *before*
/// the subscription performs any history read, so every position published
/// after this point is either received live or covered by an explicit
/// overflow signal — there is no gap between "history already read" and
/// "not yet subscribed".
pub fn subscribe<F: Fs>(
    tap: &LiveTap,
    view: ReadView<F>,
    cursor: u64,
    page_limit: usize,
) -> Subscription<F> {
    // SUB1: attach to the live feed FIRST.
    let rx = tap.subscribe();
    let watermark = tap.watermark();
    Subscription {
        view,
        rx,
        cursor,
        state: SubState::CatchUp,
        page_limit: page_limit.max(1),
        metrics: SubMetrics::default(),
        pending: VecDeque::new(),
        ended: false,
        watermark,
    }
}

impl<F: Fs> Subscription<F> {
    /// The current observable state (spec §5).
    pub fn state(&self) -> SubState {
        self.state
    }

    /// A snapshot of this subscription's counters.
    pub fn metrics(&self) -> SubMetrics {
        self.metrics
    }

    /// The next position this subscription will deliver.
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Per-subscription lag: `watermark - cursor` (spec §9, SUB9) — the
    /// count of committed positions not yet delivered. `0` when caught up.
    /// Saturating (a cursor momentarily at/ahead of a just-read watermark
    /// reads as 0 lag).
    pub fn lag(&self) -> u64 {
        self.watermark.get().saturating_sub(self.cursor)
    }

    /// Read one history page: positions `[cursor, min(watermark,
    /// cursor+limit))`, derived from a real watermark-clamped
    /// [`ReadView::read_committed`] read. An empty result is proof the
    /// subscriber reached the durable watermark as of this read (SUB2).
    ///
    /// SUB10: if the cursor is past the served end (`log_end`), the log
    /// regressed under it — surface [`CursorRegressed`].
    fn read_page(&mut self) -> Result<Vec<u64>, SubError> {
        let prefix = self.view.read_committed()?;
        let end = prefix.next_pos(); // == watermark on a batch boundary
        self.metrics.catchup_pages += 1;
        if self.cursor > end {
            return Err(CursorRegressed { cursor: self.cursor, log_end: end }.into());
        }
        let page_end = end.min(self.cursor.saturating_add(self.page_limit as u64));
        // Positions [cursor, page_end) are all durable and dense (A1), so
        // they are genuinely present in the committed prefix just read.
        Ok((self.cursor..page_end).collect())
    }

    /// Pull the next delivered position, driving the state machine.
    ///
    /// - `Ok(Some(p))` — position `p` delivered in order (`p == cursor`
    ///   before the call).
    /// - `Ok(None)` — the subscription ended: the live feed closed and
    ///   history is fully drained (nothing can ever arrive).
    /// - `Err(CursorRegressed)` — SUB10; the consumer must handle it (§10)
    ///   before re-subscribing from `log_end`.
    /// - `Err(Io)` — an authoritative history read failed.
    pub async fn next(&mut self) -> Result<Option<u64>, SubError> {
        loop {
            // Hand out any buffered history-page position first.
            if let Some(p) = self.pending.pop_front() {
                debug_assert_eq!(p, self.cursor, "history pages are dense (SUB3)");
                self.cursor = p + 1;
                self.metrics.history_delivered += 1;
                return Ok(Some(p));
            }
            if self.ended {
                return Ok(None);
            }
            match self.state {
                SubState::CatchUp => {
                    let page = self.read_page()?;
                    if page.is_empty() {
                        // Empty page == reached the watermark (SUB2): switch.
                        self.state = SubState::Switching;
                        self.metrics.switches += 1;
                    } else {
                        self.pending = page.into();
                        // Loop: deliver the page's first position.
                    }
                }
                SubState::Switching | SubState::Live => {
                    match self.rx.recv().await {
                        RecvOutcome::Value(p) if p < self.cursor => {
                            // Overlap dedupe (SUB4, the `<=` rule): a live
                            // position already delivered from history.
                            self.metrics.dedupe_skips += 1;
                        }
                        RecvOutcome::Value(p) if p == self.cursor => {
                            self.cursor = p + 1;
                            self.metrics.live_delivered += 1;
                            self.state = SubState::Live;
                            return Ok(Some(p));
                        }
                        RecvOutcome::Value(_) => {
                            // p > cursor with no preceding Lagged: impossible
                            // under W1+SUB1 (SUB8). Regress defensively and
                            // count it as the W1 tripwire.
                            self.metrics.anomaly_regressions += 1;
                            self.state = SubState::CatchUp;
                        }
                        RecvOutcome::Lagged(_) => {
                            // Bounded buffer overflowed while we were slow
                            // (SUB6): regress to CatchUp, cursor unchanged.
                            // Nothing lost — history has everything.
                            self.metrics.lag_regressions += 1;
                            self.state = SubState::CatchUp;
                        }
                        RecvOutcome::Closed => {
                            // Writer/pump gone. One last history drain, then
                            // end if it too is empty.
                            let page = self.read_page()?;
                            if page.is_empty() {
                                self.ended = true;
                                return Ok(None);
                            }
                            self.pending = page.into();
                            self.state = SubState::CatchUp;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::committer::{
        AppendOutcome, AppendRequest, Committer, Durability, EventInput,
    };
    use crate::runtime::{Clock, RealRuntime, Runtime, SimRuntime};
    use crate::writer::{SegmentParams, SegmentWriter};
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    // A single-stream batch of `n` events; payloads are irrelevant to the
    // handoff problem (delivery is over positions).
    fn req(stream: u64, version: u64, n: usize) -> AppendRequest {
        AppendRequest {
            stream_id: stream,
            category_id: 0,
            first_stream_version: version,
            events: (0..n)
                .map(|i| EventInput::plain(1, 1, 0, vec![(i as u8) ^ 0x5A; 8]))
                .collect(),
        }
    }

    // ===================================================================
    // Unit: the bounded broadcast primitive (Value / Lagged / Closed)
    // ===================================================================

    fn broadcast(cap: usize) -> (LiveSender, LiveHandle) {
        let inner = Arc::new(Mutex::new(BroadcastInner {
            buf: VecDeque::with_capacity(cap),
            cap,
            base_seq: 0,
            next_seq: 0,
            senders: 1,
            wakers: Vec::new(),
        }));
        (LiveSender { inner: inner.clone() }, LiveHandle { inner })
    }

    #[test]
    fn broadcast_delivers_in_order_from_attach_tail() {
        let (tx, h) = broadcast(8);
        tx.send(1); // published before attach: not seen
        let mut rx = h.subscribe();
        tx.send(2);
        tx.send(3);
        assert_eq!(rx.poll_next(None), Some(RecvOutcome::Value(2)));
        assert_eq!(rx.poll_next(None), Some(RecvOutcome::Value(3)));
        assert_eq!(rx.poll_next(None), None, "caught up to tail parks");
    }

    #[test]
    fn broadcast_reports_lagged_then_resumes_at_oldest() {
        let (tx, h) = broadcast(2);
        let mut rx = h.subscribe();
        for p in 10..15 {
            tx.send(p); // 5 sends into a cap-2 ring: evicts 10, 11, 12
        }
        // Missed 10,11,12 (3): resume at the oldest still buffered (13).
        assert_eq!(rx.poll_next(None), Some(RecvOutcome::Lagged(3)));
        assert_eq!(rx.poll_next(None), Some(RecvOutcome::Value(13)));
        assert_eq!(rx.poll_next(None), Some(RecvOutcome::Value(14)));
        assert_eq!(rx.poll_next(None), None);
    }

    #[test]
    fn broadcast_closed_after_sender_drops_and_drains() {
        let (tx, h) = broadcast(4);
        let mut rx = h.subscribe();
        tx.send(1);
        drop(tx);
        // Buffered value first, THEN closed.
        assert_eq!(rx.poll_next(None), Some(RecvOutcome::Value(1)));
        assert_eq!(rx.poll_next(None), Some(RecvOutcome::Closed));
    }

    // ===================================================================
    // Property harness: the ported spike suite against the real engine
    // ===================================================================
    //
    // Each scenario runs deterministically on the SimRuntime (seeded
    // executor + in-memory sim fs): a real Committer (group commit, real
    // watermark), a real ReadView over the real segment bytes for history,
    // and a real LiveTap pump for the live feed. Different seeds pick
    // different task interleavings (sim.rs: the next runnable actor is
    // rng-chosen), so the suite covers races the way the spike's tokio
    // multi-thread scheduler did — reproducibly.
    //
    // THE assertion, every scenario, every subscriber: the delivered
    // sequence is EXACTLY `start_cursor .. final_watermark`. No gap, no
    // duplicate, in order, across every handoff and every regression. Plus:
    // anomaly_regressions == 0 everywhere (the W1 tripwire, SUB8).

    #[derive(Clone)]
    struct SubPlan {
        start_cursor: u64,
        page_limit: usize,
        slow: bool,
    }

    #[derive(Clone)]
    struct Scenario {
        live_capacity: usize,
        pre_events: u64,
        bursts: Vec<u64>, // extra-event bursts committed by the writer
        subs: Vec<SubPlan>,
    }

    fn gen_scenario(rng: &mut crate::runtime::Rng, tiny_buffer: bool) -> Scenario {
        let live_capacity = if tiny_buffer {
            2 + rng.below(3) as usize // 2..=4
        } else {
            [2usize, 3, 4, 8, 8, 16, 64][rng.below(7) as usize]
        };
        let pre_events = if rng.chance(0.2) { 0 } else { rng.below(41) };
        let n_bursts = 1 + rng.below(8);
        let bursts: Vec<u64> = (0..n_bursts).map(|_| 1 + rng.below(12)).collect();
        let extra: u64 = bursts.iter().sum();
        let final_wm = pre_events + extra;
        let n_subs = 1 + rng.below(3);
        let subs = (0..n_subs)
            .map(|_| {
                let start_cursor = match rng.below(3) {
                    0 => 0,
                    1 => rng.below(final_wm + 1).min(pre_events),
                    _ => pre_events, // exactly at the (pre) head
                };
                SubPlan {
                    start_cursor,
                    page_limit: 1 + rng.below(24) as usize,
                    slow: tiny_buffer || rng.chance(0.4),
                }
            })
            .collect();
        Scenario { live_capacity, pre_events, bursts, subs }
    }

    // Check the one property that matters: got == start_cursor..=final_wm-1.
    fn check_exact(label: &str, start: u64, final_wm: u64, got: &[u64]) {
        let mut expect = start;
        for (i, &p) in got.iter().enumerate() {
            assert_eq!(
                p, expect,
                "{label}: at index {i} expected {expect} got {p} ({})",
                if p > expect { "gap" } else { "dup/reorder" },
            );
            expect += 1;
        }
        assert_eq!(
            expect, final_wm,
            "{label}: delivered up to {expect} but final watermark is {final_wm} (missing tail)",
        );
    }

    // Aggregate counters proving the interesting paths ran.
    #[derive(Default)]
    struct Agg {
        scenarios: u64,
        subscribers: u64,
        history: u64,
        live: u64,
        dedupe: u64,
        lags: u64,
        switches: u64,
        pages: u64,
    }
    impl Agg {
        fn add(&mut self, m: &[SubMetrics]) {
            self.scenarios += 1;
            for s in m {
                self.subscribers += 1;
                self.history += s.history_delivered;
                self.live += s.live_delivered;
                self.dedupe += s.dedupe_skips;
                self.lags += s.lag_regressions;
                self.switches += s.switches;
                self.pages += s.catchup_pages;
                assert_eq!(
                    s.anomaly_regressions, 0,
                    "in-order gap without Lagged — W1/S1 broken (SUB8)",
                );
            }
        }
        fn print(&self, name: &str) {
            println!(
                "[{name}] scenarios={} subscribers={} delivered(history={}, live={}) \
                 dedupe_skips={} lag_regressions={} switches={} catchup_pages={}",
                self.scenarios, self.subscribers, self.history, self.live,
                self.dedupe, self.lags, self.switches, self.pages,
            );
        }
    }

    // Run one scenario to completion; return per-subscriber metrics. Panics
    // on any property violation.
    fn run_scenario(seed: u64, sc: Scenario) -> Vec<SubMetrics> {
        let rt = SimRuntime::new(seed);
        let fs = rt.fs();
        let path = Path::new("/seg");
        let writer =
            SegmentWriter::create(&fs, path, SegmentParams::new(0, 0, 1, 0)).unwrap();
        let extra: u64 = sc.bursts.iter().sum();
        let final_wm = sc.pre_events + extra;

        rt.block_on(async {
            let c = Committer::spawn(&rt, writer, Durability::group_default());

            // Pre-history: committed before the tap/subscribers exist.
            let mut v = 0u64;
            let mut left = sc.pre_events;
            while left > 0 {
                let n = left.min(5);
                let out = c.append(req(0, v, n as usize)).await.unwrap();
                assert!(matches!(out, AppendOutcome::Acked { .. }));
                v += n;
                left -= n;
            }
            assert_eq!(c.watermark().get(), sc.pre_events);

            let tap = LiveTap::spawn_with_capacity(&rt, c.watermark(), sc.live_capacity);

            // Subscribers, each owning its Subscription and draining it to
            // its expected count (delivery is exactly start..final_wm).
            let mut consumers = Vec::new();
            for plan in sc.subs.iter().cloned() {
                let view = ReadView::new(fs.clone(), path, tap.watermark());
                let sub = subscribe(&tap, view, plan.start_cursor, plan.page_limit);
                let rt2 = rt.clone();
                let expected = (final_wm - plan.start_cursor) as usize;
                consumers.push(rt.spawn(async move {
                    let mut sub = sub;
                    let mut got = Vec::with_capacity(expected);
                    while got.len() < expected {
                        match sub.next().await.unwrap() {
                            Some(p) => {
                                got.push(p);
                                // A slow consumer's yielding lets the
                                // writer/pump race ahead and overflow the
                                // bounded live buffer while it is not polling.
                                if plan.slow && got.len() % 3 == 0 {
                                    rt2.sleep(Duration::from_micros(50)).await;
                                }
                            }
                            None => break,
                        }
                    }
                    (got, sub.metrics())
                }));
            }

            // Writer: commit the extra events in paced bursts (one stream so
            // positions are dense and easy to reason about).
            let ap = c.appender();
            let bursts = sc.bursts.clone();
            let rt3 = rt.clone();
            let pre = sc.pre_events;
            let writer_task = rt.spawn(async move {
                let mut start_ver = pre;
                for b in bursts {
                    let out = ap.append(req(0, start_ver, b as usize)).await.unwrap();
                    assert!(matches!(out, AppendOutcome::Acked { .. }));
                    start_ver += b;
                    rt3.sleep(Duration::from_micros(20)).await;
                }
            });

            writer_task.await;
            assert_eq!(c.watermark().get(), final_wm);

            let mut metrics = Vec::new();
            for (i, ch) in consumers.into_iter().enumerate() {
                let (got, m) = ch.await;
                check_exact(
                    &format!("seed {seed} sub {i} (cursor {})", sc.subs[i].start_cursor),
                    sc.subs[i].start_cursor,
                    final_wm,
                    &got,
                );
                metrics.push(m);
            }

            tap.shutdown().await;
            c.shutdown().await;
            metrics
        })
    }

    /// The big randomized run: varied buffer capacities, burst sizes/pacing,
    /// cursors (0 / mid-history / at-head), 1-3 concurrent subscribers.
    #[test]
    #[cfg_attr(miri, ignore)] // thousands of scenarios — far too slow for Miri
    fn randomized_scenarios() {
        const N: u64 = 4000;
        let mut agg = Agg::default();
        for seed in 0..N {
            let mut rng = crate::runtime::Rng::new(seed ^ 0x5EED_1234);
            let sc = gen_scenario(&mut rng, false);
            agg.add(&run_scenario(seed ^ 0x5EED_1234, sc));
        }
        agg.print("randomized");
        assert!(agg.switches > 0, "never switched to live");
        assert!(agg.lags > 0, "never overflowed the live buffer");
        assert!(agg.dedupe > 0, "never exercised the overlap dedupe");
        assert!(agg.live > 0, "never delivered a live position");
    }

    /// Overflow storm: tiny buffers (2-4) + slow consumers force frequent
    /// Lagged regressions. Still: exact delivery, no flapping, no gaps.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn overflow_storm() {
        const N: u64 = 1000;
        let mut agg = Agg::default();
        for seed in 0..N {
            let mut rng = crate::runtime::Rng::new(0x0F10_0000 + seed);
            let sc = gen_scenario(&mut rng, true);
            agg.add(&run_scenario(0x0F10_0000 + seed, sc));
        }
        agg.print("overflow_storm");
        assert!(agg.lags >= 100, "expected frequent Lagged regressions, got {}", agg.lags);
    }

    /// Racing switch: subscriber starts exactly at the pre-head while the
    /// writer hammers, so the empty-page→Switching transition races fresh
    /// commits every time.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn racing_switch() {
        const N: u64 = 600;
        let mut agg = Agg::default();
        for seed in 0..N {
            let mut rng = crate::runtime::Rng::new(0xACE0_0000 + seed);
            let pre = 10 + rng.below(40);
            let n_bursts = 4 + rng.below(8);
            let bursts: Vec<u64> = (0..n_bursts).map(|_| 1 + rng.below(10)).collect();
            let sc = Scenario {
                live_capacity: 4 + rng.below(13) as usize,
                pre_events: pre,
                bursts,
                subs: vec![SubPlan {
                    start_cursor: pre, // at head: switches immediately
                    page_limit: 1 + rng.below(8) as usize,
                    slow: rng.chance(0.3),
                }],
            };
            agg.add(&run_scenario(0xACE0_0000 + seed, sc));
        }
        agg.print("racing_switch");
        assert!(agg.switches >= N, "each racing-switch scenario switches at least once");
    }

    // ===================================================================
    // Targeted edges
    // ===================================================================

    /// Subscriber permanently slower than the writer: stable catch-up, no
    /// flapping, fed entirely from (authoritative) history, and it still
    /// finishes with an exact delivery. The "no flapping" property (spec §9)
    /// shows as a tiny switch count — while behind, `read_from` never returns
    /// empty, so the CatchUp→Switching transition rarely (here never) fires.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn forever_slower_stays_in_stable_catchup() {
        let sc = Scenario {
            live_capacity: 4,
            pre_events: 0,
            bursts: vec![16; 30], // 480 events in fast bursts
            subs: vec![SubPlan { start_cursor: 0, page_limit: 16, slow: true }],
        };
        let m = run_scenario(0xF0F0, sc);
        let s = m[0];
        println!("[forever_slower] {s:?}");
        assert!(
            s.history_delivered > s.live_delivered,
            "a permanently slow subscriber should be fed mostly from history",
        );
        // No flapping: it does not oscillate CatchUp<->Switching. (The
        // overflow/lag-regression path itself is exercised in bulk by
        // `overflow_storm` and pinned deterministically by
        // `overflow_regress_is_loud_and_lossless`.)
        assert!(s.switches <= 2, "must not flap between CatchUp and Switching");
        assert_eq!(s.anomaly_regressions, 0);
    }

    /// Overflow is lossy-but-loud (spec §8): a burst larger than the tiny
    /// live buffer, delivered while the at-head subscriber is parked in
    /// Switching, MUST surface as a `Lagged` regression (loud) and MUST
    /// still deliver every position exactly once via the authoritative
    /// history fallback (lossless), with zero anomalies. Deterministic.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn overflow_regress_is_loud_and_lossless() {
        let sc = Scenario {
            live_capacity: 2, // tiny
            pre_events: 5,
            bursts: vec![20], // one 20-event burst overruns the cap-2 buffer
            subs: vec![SubPlan { start_cursor: 5, page_limit: 8, slow: false }],
        };
        let m = run_scenario(0xB00F, sc);
        let s = m[0];
        println!("[overflow_regress] {s:?}");
        assert!(s.lag_regressions >= 1, "the burst must overflow the buffer (loud)");
        assert_eq!(s.anomaly_regressions, 0, "no anomaly on overflow (SUB8)");
        // run_scenario already asserted exact delivery of 5..25 (lossless).
    }

    /// Start exactly at the watermark on an idle-then-active writer: one
    /// empty catch-up page, then the first delivery is live.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn start_exactly_at_watermark_delivers_live() {
        let sc = Scenario {
            live_capacity: 32,
            pre_events: 30,
            bursts: vec![1; 15], // paced single-event commits, subscriber keeps up
            subs: vec![SubPlan { start_cursor: 30, page_limit: 8, slow: false }],
        };
        let m = run_scenario(0x1234, sc);
        let s = m[0];
        println!("[start_at_watermark] {s:?}");
        assert!(s.live_delivered > 0, "at-head subscriber should deliver live");
        assert_eq!(s.history_delivered, 0, "nothing to catch up on at head");
        assert_eq!(s.anomaly_regressions, 0);
    }

    /// Start on an empty log.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn start_on_empty_log() {
        let sc = Scenario {
            live_capacity: 8,
            pre_events: 0,
            bursts: vec![10],
            subs: vec![SubPlan { start_cursor: 0, page_limit: 4, slow: false }],
        };
        let m = run_scenario(0xEEEE, sc);
        assert_eq!(m[0].anomaly_regressions, 0);
    }

    // ===================================================================
    // CursorRegressed (SUB10)
    // ===================================================================

    /// A cursor past the log end surfaces `CursorRegressed` rather than
    /// silently hanging or re-subscribing (spec §10).
    #[test]
    fn cursor_past_log_end_surfaces_regression() {
        let rt = SimRuntime::new(7);
        let fs = rt.fs();
        let path = Path::new("/seg-reg");
        let writer =
            SegmentWriter::create(&fs, path, SegmentParams::new(0, 0, 1, 0)).unwrap();
        rt.block_on(async {
            let c = Committer::spawn(&rt, writer, Durability::Os);
            // Commit 5 positions: log end (exclusive) == 5.
            let out = c.append(req(0, 0, 5)).await.unwrap();
            assert!(matches!(out, AppendOutcome::Acked { .. }));
            let tap = LiveTap::spawn(&rt, c.watermark());
            let view = ReadView::new(fs.clone(), path, tap.watermark());
            // Subscribe at cursor 9 > log_end 5: the log regressed under us.
            let mut sub = subscribe(&tap, view, 9, 8);
            let err = sub.next().await.unwrap_err();
            match err {
                SubError::CursorRegressed(r) => {
                    assert_eq!(r.cursor, 9);
                    assert_eq!(r.log_end, 5);
                }
                other => panic!("expected CursorRegressed, got {other:?}"),
            }
            // At-head cursor == log_end is NOT a regression (parks for live).
            let view2 = ReadView::new(fs.clone(), path, tap.watermark());
            let mut sub2 = subscribe(&tap, view2, 5, 8);
            // Drive one commit so the parked at-head sub has something live.
            let out = c.append(req(0, 5, 1)).await.unwrap();
            assert!(matches!(out, AppendOutcome::Acked { .. }));
            let p = sub2.next().await.unwrap();
            assert_eq!(p, Some(5), "at-head sub delivers the next committed position");
            tap.shutdown().await;
            c.shutdown().await;
        });
    }

    // ===================================================================
    // Negative control (SUB11 / spec §12(a)): naive "catch-up then
    // subscribe" demonstrably loses events.
    // ===================================================================
    //
    // The naive protocol reverses SUB1: it drains history to empty, THEN
    // attaches to the live feed. Any position committed in the window
    // between the last empty read and the attach is in neither source. We
    // construct that exact race deterministically (commit the window's
    // events between the naive reader's empty read and its late attach) and
    // assert the naive protocol loses them while the SPECIFIED protocol,
    // run over the identical construction, delivers everything.

    #[test]
    #[cfg_attr(miri, ignore)]
    fn naive_protocol_drops_events_specified_protocol_survives() {
        // NAIVE side: catch-up-to-empty THEN subscribe. We construct the
        // losing interleaving deterministically — the writer commits the
        // switch-window events and the pump publishes them BEFORE the late
        // subscribe (the "writer wins the race" case) — so the window
        // positions are in neither source for the naive subscriber.
        let rt = SimRuntime::new(0xBAD_F00D);
        let fs = rt.fs();
        let path = Path::new("/seg-naive");
        let writer =
            SegmentWriter::create(&fs, path, SegmentParams::new(0, 0, 1, 0)).unwrap();
        let (naive_len, final_wm) = rt.block_on(async {
            let c = Committer::spawn(&rt, writer, Durability::Os);
            c.append(req(0, 0, 12)).await.unwrap(); // pre-history: 0..12
            let tap = LiveTap::spawn(&rt, c.watermark());
            let view = ReadView::new(fs.clone(), path, tap.watermark());

            // Phase 1: drain history to empty WITHOUT subscribing first.
            let mut cursor = 0u64;
            let mut got = Vec::new();
            loop {
                let end = view.read_committed().unwrap().next_pos();
                if cursor >= end {
                    break;
                }
                while cursor < end {
                    got.push(cursor);
                    cursor += 1;
                }
            }

            // Switch WINDOW: commit 7 events (positions 12..19) after the
            // last empty read but before the (late) subscribe.
            c.append(req(0, 12, 7)).await.unwrap();
            // Let the pump publish the window into the ring (writer wins).
            rt.sleep(Duration::from_micros(100)).await;

            // Phase 2: subscribe LATE — the receiver starts at the tail, so
            // the already-published window is behind its cursor: unseen.
            let mut rx = tap.subscribe();
            let final_wm = c.watermark().get();
            rt.sleep(Duration::from_micros(100)).await;
            loop {
                match rx.poll_next(None) {
                    Some(RecvOutcome::Value(p)) if p == cursor => {
                        got.push(p);
                        cursor += 1;
                    }
                    Some(RecvOutcome::Value(_)) | Some(RecvOutcome::Lagged(_)) => {}
                    Some(RecvOutcome::Closed) | None => break,
                }
                if cursor >= final_wm {
                    break;
                }
            }
            tap.shutdown().await;
            c.shutdown().await;
            (got.len() as u64, final_wm)
        });
        assert_eq!(final_wm, 19);
        assert!(
            naive_len < final_wm,
            "naive protocol must lose the switch-window events (got {naive_len}, final {final_wm})",
        );

        // SPECIFIED side: the exact same shape (12 pre + a 7-event burst),
        // run through the conformant protocol. run_scenario asserts
        // element-for-element exact delivery internally; here we also pin
        // that no anomaly regression occurred.
        let sc = Scenario {
            live_capacity: 64,
            pre_events: 12,
            bursts: vec![7],
            subs: vec![SubPlan { start_cursor: 0, page_limit: 8, slow: false }],
        };
        let m = run_scenario(0xC0FFEE, sc);
        assert_eq!(m[0].anomaly_regressions, 0);
    }

    // ===================================================================
    // Real runtime: end-to-end over real threads + real fs
    // ===================================================================

    fn real_tmp(name: &str) -> std::path::PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let base = std::env::var_os("HOME")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let mut dir = base;
        dir.push(".cache");
        dir.push("mess-subscription-scratch");
        std::fs::create_dir_all(&dir).unwrap();
        dir.push(format!("{}-{}-{}", std::process::id(), n, name));
        dir
    }

    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn real_end_to_end_catchup_then_live() {
        let path = real_tmp("e2e");
        let _c = Cleanup(path.clone());
        let rt = RealRuntime::new();
        let fs = rt.fs();
        let writer = SegmentWriter::create(
            &fs,
            &path,
            SegmentParams { segment_size: 64 * 1024 * 1024, ..SegmentParams::new(0, 0, 1, 0) },
        )
        .unwrap();

        let (got, metrics) = rt.block_on(async {
            let c = Committer::spawn(&rt, writer, Durability::group_default());
            // Pre-history: 20 positions.
            for v in (0..20u64).step_by(4) {
                c.append(req(0, v, 4)).await.unwrap();
            }
            let tap = LiveTap::spawn(&rt, c.watermark());
            let view = ReadView::new(fs, path.clone(), tap.watermark());
            let sub = subscribe(&tap, view, 0, 5);

            let expected = 50usize; // 20 pre + 30 live
            let consumer = rt.spawn(async move {
                let mut sub = sub;
                let mut got = Vec::new();
                while got.len() < expected {
                    match sub.next().await.unwrap() {
                        Some(p) => got.push(p),
                        None => break,
                    }
                }
                (got, sub.metrics())
            });

            // Live: 30 more positions in paced batches.
            let ap = c.appender();
            let writer = rt.spawn(async move {
                let mut v = 20u64;
                for _ in 0..10 {
                    ap.append(req(0, v, 3)).await.unwrap();
                    v += 3;
                }
            });
            writer.await;
            let out = consumer.await;
            tap.shutdown().await;
            c.shutdown().await;
            out
        });

        check_exact("real_e2e", 0, 50, &got);
        assert_eq!(metrics.anomaly_regressions, 0);
        println!("[real_e2e] {metrics:?}");
    }
}
