//! The position-ordered durable watermark (D7) — the single integer the
//! committer advances once per group after that group's barrier returns
//! (`docs/spec/03-durability.md` §3), and the object
//! `06-subscriptions.md`'s D11 protocol will later read to gate what
//! `read_from` / the live feed may serve.
//!
//! # What the value means
//!
//! The watermark holds an **exclusive durable end**: the number `w` such
//! that every global position `< w` is durable, and no position `>= w` is
//! yet known-durable. This is exactly the spec's *log end* ("the
//! watermark's value at any instant"): for a segment seeded at `base_pos`
//! with `n` durable events it reads `base_pos + n`.
//!
//! Consequences, both used by D11 later:
//! - a **batch** covering positions `[first, first + count)` is durable
//!   once `value >= first + count` — this is what an appender awaits to be
//!   acked ([`Watermark::wait_for`]).
//! - a **position** `p` is durable (may be served) once `value >= p + 1`,
//!   i.e. `value > p`; a consumer awaits `wait_for(p + 1)`.
//!
//! # Watch-channel shape
//!
//! [`Watermark`] is a cheap-to-clone handle over shared state. Any number
//! of readers may hold a clone and `await` [`Watermark::wait_for`]; the
//! single committer is the only writer and calls [`Watermark::advance`].
//! `advance` is monotone (a smaller target is a no-op), matching the
//! centrally-assigned, monotone positions of §2.1 step 2 — so a reader
//! that has observed `value >= p` never later observes it regress while
//! the store is live. (`Durability::Process`'s crash-visibility hazard,
//! spec §6 `CursorRegressed`, is about a *crash* re-establishing a smaller
//! log end via recovery, not about this in-memory value moving backward.)

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct State {
    /// Exclusive durable end (log end). Monotone non-decreasing while live.
    value: u64,
    /// `(threshold, waker)` for each parked `wait_for`. Drained on every
    /// `advance`; a waiter whose threshold is still unmet re-registers when
    /// it is re-polled.
    waiters: Vec<(u64, Waker)>,
}

/// A cheap-to-clone handle over the durable watermark. Clones share one
/// underlying value; the committer advances it, readers await it.
#[derive(Clone)]
pub struct Watermark {
    inner: Arc<Mutex<State>>,
}

impl Watermark {
    /// A watermark whose durable end starts at `initial` (a segment's
    /// `base_pos`: no events durable yet).
    pub fn new(initial: u64) -> Self {
        Watermark {
            inner: Arc::new(Mutex::new(State { value: initial, waiters: Vec::new() })),
        }
    }

    /// The current durable end (the log end). Monotone while the store is
    /// live.
    pub fn get(&self) -> u64 {
        self.inner.lock().unwrap().value
    }

    /// Advance the durable end to `to`, covering every position `< to`.
    /// Monotone: a `to` not greater than the current value is a no-op.
    /// Wakes every parked waiter (satisfied ones resolve; the rest
    /// re-register), so callers waiting on any threshold `<= to` make
    /// progress.
    pub fn advance(&self, to: u64) {
        let mut st = self.inner.lock().unwrap();
        if to <= st.value {
            return;
        }
        st.value = to;
        // Wake everyone; unsatisfied waiters re-park on their next poll.
        for (_, w) in st.waiters.drain(..) {
            w.wake();
        }
    }

    /// Resolve once the durable end reaches `threshold` (i.e. every
    /// position `< threshold` is durable). Resolves immediately if the
    /// watermark is already there. This is the primitive an appender uses
    /// to await its batch's ack and a D11 subscriber uses to gate a read.
    pub fn wait_for(&self, threshold: u64) -> WaitFor {
        WaitFor { inner: self.inner.clone(), threshold }
    }

    /// Resolve once **position** `position` has become durable — i.e. once
    /// the watermark has advanced strictly *past* it (`value > position`,
    /// equivalently `value >= position + 1`). This is the position-indexed
    /// face of [`wait_for`](Watermark::wait_for) and the primitive a D11
    /// subscription (`docs/spec/06-subscriptions.md`) awaits: a subscriber
    /// parked on `await_past(p)` wakes exactly when `p` joins the committed
    /// prefix `read_from`/the read view may serve — never earlier (the
    /// exclusive-durable-end meaning of the value is documented on the
    /// module), and, because [`advance`](Watermark::advance) is monotone,
    /// never spuriously before the crossing.
    ///
    /// Position-ordered by construction: `await_past(a)` resolves no later
    /// than `await_past(b)` for `a <= b`, since one monotone value gates
    /// both. `position == u64::MAX` saturates to waiting for
    /// `value == u64::MAX` (the log can hold no position past it).
    pub fn await_past(&self, position: u64) -> WaitFor {
        self.wait_for(position.saturating_add(1))
    }
}

/// The future returned by [`Watermark::wait_for`].
pub struct WaitFor {
    inner: Arc<Mutex<State>>,
    threshold: u64,
}

impl Future for WaitFor {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut st = self.inner.lock().unwrap();
        if st.value >= self.threshold {
            Poll::Ready(())
        } else {
            st.waiters.push((self.threshold, cx.waker().clone()));
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Wake;

    struct Noop(AtomicBool);
    impl Wake for Noop {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn ready_when_already_past_threshold() {
        let wm = Watermark::new(10);
        let w = Arc::new(Noop(AtomicBool::new(false)));
        let waker = Waker::from(w.clone());
        let mut cx = Context::from_waker(&waker);
        let mut f = Box::pin(wm.wait_for(10));
        assert_eq!(f.as_mut().poll(&mut cx), Poll::Ready(()));
        let mut f2 = Box::pin(wm.wait_for(5));
        assert_eq!(f2.as_mut().poll(&mut cx), Poll::Ready(()));
    }

    #[test]
    fn wakes_on_advance_and_is_monotone() {
        let wm = Watermark::new(0);
        let w = Arc::new(Noop(AtomicBool::new(false)));
        let waker = Waker::from(w.clone());
        let mut cx = Context::from_waker(&waker);

        let mut f = Box::pin(wm.wait_for(4));
        assert_eq!(f.as_mut().poll(&mut cx), Poll::Pending);

        // A non-covering advance still wakes (waiter re-parks); not ready.
        wm.advance(3);
        assert!(w.0.swap(false, Ordering::SeqCst), "advance must wake waiters");
        assert_eq!(f.as_mut().poll(&mut cx), Poll::Pending);

        // A regressive advance is a no-op.
        wm.advance(1);
        assert_eq!(wm.get(), 3);

        // Covering advance resolves.
        wm.advance(4);
        assert!(w.0.swap(false, Ordering::SeqCst));
        assert_eq!(f.as_mut().poll(&mut cx), Poll::Ready(()));
        assert_eq!(wm.get(), 4);
    }

    // -- await_past: position semantics (D11 subscription primitive) ------

    #[test]
    fn await_past_wakes_exactly_when_the_watermark_crosses_the_position() {
        // Position 2 is served once the exclusive durable end passes it,
        // i.e. value >= 3 (positions 0,1,2 durable). value == 2 (only 0,1
        // durable) must NOT resolve await_past(2).
        let wm = Watermark::new(0);
        let w = Arc::new(Noop(AtomicBool::new(false)));
        let waker = Waker::from(w.clone());
        let mut cx = Context::from_waker(&waker);

        let mut f = Box::pin(wm.await_past(2));
        assert_eq!(f.as_mut().poll(&mut cx), Poll::Pending);

        // value reaching exactly `position` is not "past" it: position 2 is
        // still in-flight when the exclusive end is 2.
        wm.advance(2);
        assert!(w.0.swap(false, Ordering::SeqCst), "advance wakes waiters");
        assert_eq!(
            f.as_mut().poll(&mut cx),
            Poll::Pending,
            "value == position is not past it (position still in-flight)"
        );

        // Crossing (value > position) resolves.
        wm.advance(3);
        assert!(w.0.swap(false, Ordering::SeqCst));
        assert_eq!(f.as_mut().poll(&mut cx), Poll::Ready(()));
    }

    #[test]
    fn await_past_is_ready_when_already_crossed() {
        let wm = Watermark::new(10);
        let w = Arc::new(Noop(AtomicBool::new(false)));
        let waker = Waker::from(w.clone());
        let mut cx = Context::from_waker(&waker);
        // Positions 0..10 are durable (exclusive end 10), so every position
        // < 9 is already past; position 9 needs value >= 10 — satisfied.
        assert_eq!(Box::pin(wm.await_past(9)).as_mut().poll(&mut cx), Poll::Ready(()));
        assert_eq!(Box::pin(wm.await_past(0)).as_mut().poll(&mut cx), Poll::Ready(()));
        // Position 10 needs value >= 11 — not yet.
        assert_eq!(Box::pin(wm.await_past(10)).as_mut().poll(&mut cx), Poll::Pending);
    }

    #[test]
    fn await_past_is_position_ordered() {
        // One monotone value gates every position: a single advance can
        // satisfy a run of await_past(p) in ascending p, and never satisfies
        // a larger p before a smaller one.
        let wm = Watermark::new(0);
        let w = Arc::new(Noop(AtomicBool::new(false)));
        let waker = Waker::from(w.clone());
        let mut cx = Context::from_waker(&waker);

        let mut lo = Box::pin(wm.await_past(1)); // needs value >= 2
        let mut hi = Box::pin(wm.await_past(4)); // needs value >= 5
        assert_eq!(lo.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(hi.as_mut().poll(&mut cx), Poll::Pending);

        wm.advance(3); // crosses position 1, not position 4
        assert_eq!(lo.as_mut().poll(&mut cx), Poll::Ready(()));
        assert_eq!(
            hi.as_mut().poll(&mut cx),
            Poll::Pending,
            "a larger position must not resolve before the value reaches it"
        );

        wm.advance(5);
        assert_eq!(hi.as_mut().poll(&mut cx), Poll::Ready(()));
    }

    #[test]
    fn await_past_saturates_at_u64_max() {
        // No off-by-one panic at the top of the range: await_past(MAX) folds
        // to wait_for(MAX), satisfied only at the maximal value.
        let wm = Watermark::new(u64::MAX);
        let w = Arc::new(Noop(AtomicBool::new(false)));
        let waker = Waker::from(w.clone());
        let mut cx = Context::from_waker(&waker);
        assert_eq!(Box::pin(wm.await_past(u64::MAX)).as_mut().poll(&mut cx), Poll::Ready(()));
    }
}
