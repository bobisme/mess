//! Spike: subscription catch-up -> live handoff protocol.
//!
//! Question under test: a subscriber replaying history from a cursor must
//! transition to receiving live events without gaps or duplicates while a
//! writer keeps appending. This models the log as pure positions (payloads
//! are irrelevant to the handoff problem) and property-tests the protocol
//! to destruction.
//!
//! # The protocol (subscribe-first, dedupe-overlap)
//!
//! ```text
//!            +-----------------------------------------------+
//!            v                                               |
//!   [CatchUp] --read_from empty--> [Switching] --p==last+1--> [Live]
//!       ^                             |    ^                   |
//!       |                       Lagged|    |                   |Lagged
//!       +-----------------------------+    +-------------------+
//!                (regress to CatchUp from last_delivered)
//! ```
//!
//! 1. Subscribe to the live broadcast FIRST (the bounded channel buffers
//!    on our behalf).
//! 2. CatchUp: page through history (`read_from(last, limit)`) delivering
//!    each position, until a page comes back empty (we have reached the
//!    committed watermark as of that read).
//! 3. Switching: drain the live receiver. Positions `<= last` are the
//!    catch-up/live overlap — drop them (dedupe by position). The first
//!    position `== last + 1` is delivered and we are Live.
//! 4. Live: deliver in order. On `Lagged` (bounded live buffer overflowed
//!    because we were slow), regress to CatchUp from `last` — nothing was
//!    lost, history has everything.
//!
//! # The gapless invariant
//!
//! Writer-side ordering (see [`Log::append`]):
//!
//! > **W1**: a position `p` is published on the live channel only *after*
//! > the committed watermark is `>= p`, and publish order equals position
//! > order (single writer / write lock).
//!
//! Subscriber-side consequence:
//!
//! > **S1**: if the subscriber subscribes at time `T0` and later finishes
//! > catch-up at watermark `W_end` (its last empty `read_from`), then every
//! > position `p > W_end` was published *after* `T0` (by W1, `p`'s publish
//! > happened when the watermark was already `>= p > W_end`, and the
//! > watermark at `T0` was `<= W_end`). Therefore `p` is in the
//! > subscriber's live stream — either delivered by `recv()` or reported
//! > as `Lagged`, never silently absent.
//!
//! So: every position is covered by catch-up (`<= W_end`), by the live
//! stream (`> W_end`), or by a `Lagged` signal that sends us back to
//! catch-up. Duplicates (the catch-up/live overlap, positions published
//! after `T0` but `<= W_end`) are removed by the `p <= last` check.
//! Delivery is therefore exactly `cursor+1, cursor+2, ...` — no gap, no
//! duplicate, in order, across any number of regressions.
//!
//! # Why broadcast (and not watch + pull)
//!
//! `tokio::sync::broadcast` gives each receiver an independent bounded
//! buffer with an *explicit* overflow signal (`RecvError::Lagged`). That is
//! exactly the failure we must detect to regress safely; a `watch`-based
//! design has no per-receiver buffer, so every subscriber is permanently in
//! pull mode (see the alternatives note in REPORT.md).
//!
//! # The intentionally broken alternative
//!
//! [`naive::run_subscriber_gapped`] implements "catch up until empty, THEN
//! subscribe" — the classic bug. Appends racing the switch window are
//! neither in history-already-read nor in the (not yet existing) live
//! subscription. The property tests demonstrate it loses events.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc, watch};

/// Minimal in-memory log model: positions are 1-based and dense; the
/// committed watermark is the highest committed position. Payloads are
/// irrelevant to the handoff problem, so an event *is* its position.
pub struct Log {
    /// Committed watermark: positions `1..=committed` exist and are
    /// readable. Maps to the real design's active committed watermark
    /// (convergence doc, D7 / Phase 3).
    committed: AtomicU64,
    /// Enforces D9's single-writer rule inside the model: watermark
    /// advance and live publish happen atomically with respect to other
    /// appends, so publish order == position order (invariant W1).
    write_lock: Mutex<()>,
    /// Bounded live feed. Slow receivers overflow and observe `Lagged`.
    live: broadcast::Sender<u64>,
}

impl Log {
    pub fn new(live_capacity: usize) -> Arc<Self> {
        let (live, _) = broadcast::channel(live_capacity);
        Arc::new(Self {
            committed: AtomicU64::new(0),
            write_lock: Mutex::new(()),
            live,
        })
    }

    /// Append one event, returning its position.
    ///
    /// Invariant W1 lives here: the watermark is advanced BEFORE the
    /// position is published on the live channel, and the write lock keeps
    /// publish order equal to position order. If publish ever preceded the
    /// watermark advance (or two appends could interleave advance/publish),
    /// a subscriber could observe a live position it cannot yet read from
    /// history, and S1 would not hold.
    pub fn append(&self) -> u64 {
        let _g = self.write_lock.lock().unwrap();
        let pos = self.committed.fetch_add(1, Ordering::AcqRel) + 1;
        // No receivers is fine; the log does not care.
        let _ = self.live.send(pos);
        pos
    }

    pub fn watermark(&self) -> u64 {
        self.committed.load(Ordering::Acquire)
    }

    pub fn subscribe_live(&self) -> broadcast::Receiver<u64> {
        self.live.subscribe()
    }

    /// Page-based history read: positions `from+1 ..= min(watermark,
    /// from+limit)`, i.e. `from` is an exclusive cursor. Empty result means
    /// "caught up to the committed watermark as of this read".
    pub fn read_from(&self, from: u64, limit: usize) -> Vec<u64> {
        let wm = self.watermark();
        let end = wm.min(from.saturating_add(limit as u64));
        (from + 1..=end).collect()
    }
}

/// Subscriber state machine states (exposed for tests/reporting).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Paging through history from `last`.
    CatchUp,
    /// History drained; consuming the live receiver but has not yet
    /// delivered a live event since the last switch (deduping overlap).
    Switching,
    /// Delivering live events in order.
    Live,
}

/// Counters proving the interesting paths actually executed.
#[derive(Debug, Default, Clone)]
pub struct SubStats {
    pub history_delivered: u64,
    pub live_delivered: u64,
    /// Live positions dropped because `p <= last` (catch-up/live overlap).
    pub dedupe_skips: u64,
    /// Regressions to CatchUp caused by live-buffer overflow (`Lagged`).
    pub lag_regressions: u64,
    /// Regressions caused by an in-order gap WITHOUT a `Lagged` signal.
    /// By W1+S1 this must never happen; property tests assert it is 0.
    pub anomaly_regressions: u64,
    /// CatchUp -> Switching transitions.
    pub switches: u64,
    pub catchup_pages: u64,
}

/// Run the handoff subscriber.
///
/// * `start_cursor` — exclusive: delivery begins at `start_cursor + 1`.
/// * `sink` — bounded delivery channel; a slow consumer backpressures the
///   subscriber here, which is what makes the live buffer overflow.
/// * `fin` — set to `Some(final_watermark)` when the writer is done; the
///   subscriber returns once it has delivered everything up to it.
///
/// Returns stats. Delivery is exactly
/// `start_cursor+1 ..= final_watermark`, in order.
pub async fn run_subscriber(
    log: Arc<Log>,
    start_cursor: u64,
    page_limit: usize,
    sink: mpsc::Sender<u64>,
    mut fin: watch::Receiver<Option<u64>>,
) -> SubStats {
    let mut stats = SubStats::default();
    // Step 1 of the protocol: subscribe BEFORE the first history read.
    // Everything published after this line is in `rx` (or Lagged).
    let mut rx = log.subscribe_live();
    let mut last = start_cursor;
    let mut state = State::CatchUp;
    let mut live_closed = false;

    loop {
        if let Some(f) = *fin.borrow() {
            if last >= f {
                break;
            }
        }
        match state {
            State::CatchUp => {
                let page = log.read_from(last, page_limit);
                stats.catchup_pages += 1;
                if page.is_empty() {
                    if live_closed {
                        // Writer gone and history drained: nothing more
                        // can ever arrive.
                        break;
                    }
                    state = State::Switching;
                    stats.switches += 1;
                } else {
                    for p in page {
                        debug_assert_eq!(p, last + 1, "history pages are dense");
                        if sink.send(p).await.is_err() {
                            return stats; // consumer went away
                        }
                        last = p;
                        stats.history_delivered += 1;
                    }
                }
            }
            State::Switching | State::Live => {
                tokio::select! {
                    biased;
                    res = rx.recv() => match res {
                        Ok(p) if p <= last => {
                            // Catch-up/live overlap: published after we
                            // subscribed but already delivered from
                            // history. Dedupe by position.
                            stats.dedupe_skips += 1;
                        }
                        Ok(p) if p == last + 1 => {
                            if sink.send(p).await.is_err() {
                                return stats;
                            }
                            last = p;
                            stats.live_delivered += 1;
                            if state == State::Switching {
                                state = State::Live;
                            }
                        }
                        Ok(_) => {
                            // p > last + 1 with no Lagged signal. W1+S1 say
                            // impossible; regress defensively and count it
                            // so the property tests will scream if the
                            // reasoning is wrong.
                            stats.anomaly_regressions += 1;
                            state = State::CatchUp;
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            // Bounded live buffer overflowed while we were
                            // slow. Nothing is lost: regress to CatchUp
                            // from `last`; history has everything.
                            stats.lag_regressions += 1;
                            state = State::CatchUp;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            // Writer dropped the channel. Do a final
                            // history drain, then exit.
                            live_closed = true;
                            state = State::CatchUp;
                        }
                    },
                    // Wakes us when the final watermark is announced so the
                    // `last >= f` check at the top of the loop can run even
                    // if the writer is idle and recv() would pend forever.
                    _ = fin.changed() => {}
                }
            }
        }
    }
    stats
}

/// The rejected alternative, kept as an executable counterexample.
pub mod naive {
    use super::*;
    use std::time::Duration;

    /// INTENTIONALLY BROKEN protocol: catch up until `read_from` returns
    /// empty, THEN subscribe to the live feed, then deliver whatever the
    /// feed produces (deduping `p <= last`, tolerating Lagged by skipping —
    /// i.e. trusting the feed).
    ///
    /// The race: any position appended after the final (empty) history read
    /// but before `subscribe_live()` is in neither source. It is gone
    /// forever from this subscriber's point of view. The property test
    /// `naive_protocol_drops_events` measures how often that actually
    /// happens under a racing writer.
    ///
    /// Returns stats-free; the delivered sequence (via `sink`) is the
    /// evidence. Exits when `fin` is reached OR when nothing arrives for
    /// `stall` after `fin` is known (a real deployment would simply hang —
    /// the missing events never come).
    pub async fn run_subscriber_gapped(
        log: Arc<Log>,
        start_cursor: u64,
        page_limit: usize,
        sink: mpsc::Sender<u64>,
        mut fin: watch::Receiver<Option<u64>>,
        stall: Duration,
    ) {
        let mut last = start_cursor;
        // Phase 1: catch up completely...
        loop {
            let page = log.read_from(last, page_limit);
            if page.is_empty() {
                break;
            }
            for p in page {
                if sink.send(p).await.is_err() {
                    return;
                }
                last = p;
            }
        }
        // ... the switch window: a racing writer appends HERE ...
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        // Phase 2: ... then subscribe.
        let mut rx = log.subscribe_live();
        loop {
            let fin_now = *fin.borrow();
            if let Some(f) = fin_now {
                if last >= f {
                    return;
                }
                // fin known but not reached: the missing events may simply
                // never arrive. Bail out after a stall instead of hanging.
                match tokio::time::timeout(stall, rx.recv()).await {
                    Ok(Ok(p)) => {
                        if p > last {
                            let _ = sink.send(p).await;
                            last = p;
                        }
                    }
                    Ok(Err(broadcast::error::RecvError::Lagged(_))) => {}
                    Ok(Err(broadcast::error::RecvError::Closed)) | Err(_) => return,
                }
            } else {
                tokio::select! {
                    res = rx.recv() => match res {
                        Ok(p) => {
                            if p > last {
                                let _ = sink.send(p).await;
                                last = p;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                        Err(broadcast::error::RecvError::Closed) => return,
                    },
                    _ = fin.changed() => {}
                }
            }
            if let Some(f) = *fin.borrow() {
                if last >= f {
                    return;
                }
            }
        }
    }
}
