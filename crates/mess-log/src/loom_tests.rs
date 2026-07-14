//! Loom memory-ordering interleaving checks (bn-1gx) for the cross-thread
//! atomic protocols the rest of the verification stack cannot reach.
//!
//! # Why loom, on top of Kani / DST / fuzzing
//!
//! The existing layers cover different failure classes: Kani proves the pure
//! functions over all inputs, the stateright DST explores *task-level*
//! interleavings, and the fuzzers throw hostile bytes. None of them exercises
//! the **C11 memory-ordering interleavings of the cross-thread atomics** — a
//! relaxed-ordering bug (e.g. a future lock-free rewrite of the watermark that
//! advances with `Relaxed` instead of `Release`) is invisible to every one of
//! them, yet it directly breaks `06-subscriptions.md`'s **W1** gapless
//! invariant. Loom is the tool for exactly that gap: it drives the model under
//! a C11-faithful scheduler and enumerates every legal ordering, including the
//! stale-relaxed-read outcomes real hardware only produces intermittently.
//!
//! # What is modelled here
//!
//! Per the bn-1gx / bn-12d approach, these are **small extracted models** of
//! each protocol (2–3 threads, 2–3 operations — loom's exhaustiveness explodes
//! fast), each using loom's real sync primitives (`loom::sync::{Arc, Mutex,
//! atomic}`) with the **identical operations and memory orderings the
//! production code uses**, so a wrong ordering choice in production would be
//! reproduced here. Each model cites the exact production lines it mirrors.
//!
//! Deliberately extracted rather than cfg(loom)-swapping the production structs
//! in place: loom's atomics are not `const fn`, so swapping `std::sync::atomic`
//! wholesale would break the `static`/`Default` atomics elsewhere in
//! `committer.rs`, and this crate is edited by three workers in parallel —
//! keeping the loom surface in one owned file avoids churn on the shared files.
//! The correspondence to production is pinned line-by-line in each model's doc.
//!
//! 1. [`watermark_publish`] — the W1 publish ordering (`03-durability.md` §3,
//!    `06-subscriptions.md` W1): watermark-advance-before-publish is a
//!    release/acquire message pass; a relaxed advance is caught.
//! 2. [`watermark_wakeup`] — `Watermark::advance` vs `WaitFor::poll`
//!    (`watermark.rs`): the single `Mutex<State>` serialises value-write and
//!    waiter-register, so no wakeup is lost and reads stay monotone.
//! 3. [`committer_handoff`] — the §2.2 early-close `Gate` (`committer.rs`):
//!    appender `enter`/`leave` vs the committer's `register`-then-`is_zero`
//!    re-check; the waker mutex supplies the happens-before that makes even the
//!    `Relaxed` count load safe, so no batch is stranded unsubmitted.
//! 4. [`index_swap`] — the seal-time active-index swap: **blocked on bn-25d**
//!    (the index does not exist yet); the planned model is documented, not run.
//!
//! # Running
//!
//! ```text
//! RUSTFLAGS='--cfg loom' cargo test -p mess-log --release loom_
//! # or:
//! just loom
//! ```
//!
//! Loom needs `--cfg loom` (it replaces `std::sync` inside this module) and a
//! release build (checked models are slow in debug). `just loom` sets
//! `LOOM_MAX_PREEMPTIONS=3` — every model here has a small enough reachable
//! state space to explore *fully* at that bound (see the per-test
//! `EXPLORED` counters), so the bound is a safety cap, not a soundness
//! compromise. CI runs the same recipe (`.github/workflows/loom.yml`).

// Each model bumps this before its assertions so the harness can print the
// number of interleavings loom actually explored — evidence the suites are not
// trivially passing (a model loom fully explores in 1 iteration proves
// nothing). Plain `std` atomic: it is a bare invocation counter, not part of
// any modelled shared state, so it is intentionally outside loom's tracking.
use std::sync::atomic::{AtomicUsize, Ordering as StdOrdering};

fn count_iteration(counter: &AtomicUsize) {
    counter.fetch_add(1, StdOrdering::Relaxed);
}

// ---------------------------------------------------------------------------
// 1. Watermark publish ordering — 06-subscriptions.md W1 / 03-durability.md §3
// ---------------------------------------------------------------------------

/// The **W1 gapless invariant** as a message-passing litmus.
///
/// `06-subscriptions.md` W1: *"For every committed position `p`: the exclusive
/// durable watermark MUST be advanced past `p` **before** `p` is offered to
/// any live buffer."* `03-durability.md` §3: *"watermark advance, then publish
/// the batch's positions in order — never ... ahead of the watermark advance
/// that covers them."*
///
/// Modelled as: the committer writes the batch's data cell, then advances the
/// watermark; a subscriber loads the watermark and, if it is past the
/// position, reads the data. This is the classic release/acquire message pass.
/// Today production serialises the two stores under the committer's write +
/// `Watermark::advance` `Mutex` (`watermark.rs:76`), which supplies exactly the
/// release/acquire edge modelled here — so this test guards a **future
/// lock-free rewrite** of the watermark against silently dropping to `Relaxed`,
/// the one bug class §3's spec note demands be "re-derived by construction, not
/// by inspection."
#[cfg(loom)]
mod watermark_publish {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static EXPLORED: AtomicUsize = AtomicUsize::new(0);

    /// One committer publishing position 0, one subscriber gating on the
    /// watermark. `advance_order`/`gate_order` parameterise the release side
    /// (the watermark store) and the acquire side (the subscriber's load).
    fn publish(advance_order: Ordering, gate_order: Ordering) {
        loom::model(move || {
            count_iteration(&EXPLORED);

            // The batch's bytes (a stand-in payload) and the exclusive durable
            // end. Both start empty / at 0 (position 0 not yet durable).
            let data = Arc::new(AtomicU64::new(0));
            let watermark = Arc::new(AtomicU64::new(0));

            let d = data.clone();
            let w = watermark.clone();
            let committer = loom::thread::spawn(move || {
                // Step: the batch lands (page cache). Ordered purely by the
                // release-store below, exactly as the real payload write is
                // ordered by `advance`'s mutex release.
                d.store(42, Ordering::Relaxed);
                // Step 5 (03-durability.md §2.1): advance the watermark past
                // position 0. RELEASE publishes every prior write to any
                // thread that acquires the new value.
                w.store(1, advance_order);
            });

            // The subscriber / read view (06 §5): serve position 0 only once
            // the watermark is `>= 1` (exclusive-durable-end meaning,
            // watermark.rs module doc). If it observes the crossing it MUST see
            // the batch's bytes — otherwise it would publish a gap, the exact
            // W1 violation §5's "authoritative and complete up to the
            // watermark" premise forbids.
            if watermark.load(gate_order) >= 1 {
                let seen = data.load(Ordering::Relaxed);
                assert_eq!(
                    seen, 42,
                    "W1 violated: watermark advanced past position 0 but its \
                     data is not yet visible — a subscriber would publish a \
                     gap"
                );
            }

            committer.join().unwrap();
        });
    }

    /// The real invariant: RELEASE advance + ACQUIRE gate. Loom explores every
    /// interleaving and finds no gap — W1 holds by construction.
    #[test]
    fn loom_watermark_publish_release_acquire_is_gapless() {
        publish(Ordering::Release, Ordering::Acquire);
        eprintln!(
            "loom watermark_publish (Release/Acquire): {} interleavings, \
             gapless",
            EXPLORED.swap(0, StdOrdering::Relaxed)
        );
    }

    /// Test-of-the-test (bn-12d acceptance): drop the advance to RELAXED and
    /// loom finds the ordering where the subscriber sees the watermark past
    /// position 0 while its data is still stale — the W1 gap. The `#[should_
    /// panic]` asserts loom actually catches it, so this suite cannot silently
    /// pass a broken ordering.
    #[test]
    #[should_panic(expected = "W1 violated")]
    fn loom_watermark_publish_relaxed_advance_is_caught() {
        publish(Ordering::Relaxed, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// 2. Watermark wakeup / monotonicity — watermark.rs advance vs WaitFor::poll
// ---------------------------------------------------------------------------

/// `Watermark::advance` (`watermark.rs:75`) vs `WaitFor::poll`
/// (`watermark.rs:124`), both of which take the **single** `Mutex<State>`
/// guarding *both* `value` and `waiters`. The model mirrors that discipline:
///
/// - `poll`: lock; if `value >= threshold` resolve; else push a waiter
///   (`watermark.rs:126-131`).
/// - `advance`: lock; raise `value`; drain + wake every waiter
///   (`watermark.rs:76-84`).
///
/// The one lock is what closes the register-vs-drain race: a `poll` that parks
/// and an `advance` that covers its threshold cannot interleave inside the
/// critical section, so **no wakeup is lost** and a reader **never observes the
/// value regress**. Loom enumerates both orderings (park-then-advance and
/// advance-then-park) and confirms it.
#[cfg(loom)]
mod watermark_wakeup {
    use loom::sync::Arc;
    use loom::sync::Mutex;

    use super::*;

    static EXPLORED: AtomicUsize = AtomicUsize::new(0);

    /// Mirror of `watermark::State`: the value and, for the single waiter this
    /// bounded model needs, whether it is parked and whether it was woken.
    struct State {
        value:  u64,
        parked: bool,
        woken:  bool,
    }

    /// One waiter polling `wait_for(THRESHOLD)`, one committer advancing past
    /// it — the real advance/poll pair under the shared `Mutex<State>`.
    #[test]
    fn loom_watermark_advance_wakes_parked_waiter_monotonically() {
        const THRESHOLD: u64 = 4;
        loom::model(|| {
            count_iteration(&EXPLORED);
            let st = Arc::new(Mutex::new(State {
                value:  0,
                parked: false,
                woken:  false,
            }));

            // Reader: WaitFor::poll. Snapshots the value it observes so we can
            // assert monotonicity (it must never later see a smaller value).
            let r = st.clone();
            let reader = loom::thread::spawn(move || {
                let mut g = r.lock().unwrap();
                let observed = g.value;
                if observed < THRESHOLD {
                    // Not yet satisfied: register (watermark.rs:129).
                    g.parked = true;
                }
                observed
            });

            // Committer: advance(THRESHOLD) — raise value, drain+wake waiters.
            {
                let mut g = st.lock().unwrap();
                if THRESHOLD > g.value {
                    g.value = THRESHOLD; // watermark.rs:80
                    if g.parked {
                        g.woken = true; // watermark.rs:82-84 wake drained waiter
                    }
                }
            }

            let first_observed = reader.join().unwrap();

            let g = st.lock().unwrap();
            // Monotone reads (module doc: "never later observes it regress"):
            // whatever the reader saw, the value only ever grew.
            assert!(
                g.value >= first_observed,
                "watermark regressed: {} < {}",
                g.value,
                first_observed
            );
            assert_eq!(g.value, THRESHOLD, "advance must reach its target");
            // No lost wakeup: if the reader parked (saw value < THRESHOLD
            // before the advance landed), the advance that raised
            // value past THRESHOLD must have woken it. The shared
            // mutex guarantees this.
            assert!(
                !g.parked || g.woken,
                "lost wakeup: a waiter parked below the threshold was not \
                 woken by the covering advance"
            );
        });
        eprintln!(
            "loom watermark_wakeup (single mutex): {} interleavings, no lost \
             wakeup",
            EXPLORED.swap(0, StdOrdering::Relaxed)
        );
    }

    /// Test-of-the-test: the same protocol but with `value` and the waiter set
    /// behind **separate** mutexes — a plausible "finer-grained locking"
    /// refactor. Loom finds the interleaving where the reader reads `value`
    /// (below threshold) under lock A, the committer raises `value` and finds
    /// the waiter set (lock B) still empty, and only *then* the reader
    /// registers under lock B — a lost wakeup. This is exactly the hazard
    /// the single `Mutex<State>` design prevents.
    #[test]
    #[should_panic(expected = "lost wakeup")]
    fn loom_watermark_split_lock_loses_wakeup() {
        const THRESHOLD: u64 = 4;
        loom::model(|| {
            let value = Arc::new(Mutex::new(0u64));
            // (parked, woken)
            let waiters = Arc::new(Mutex::new((false, false)));

            let v = value.clone();
            let wq = waiters.clone();
            let reader = loom::thread::spawn(move || {
                let observed = *v.lock().unwrap();
                if observed < THRESHOLD {
                    // Register AFTER releasing the value lock — the split-lock
                    // window the single-mutex design collapses.
                    wq.lock().unwrap().0 = true;
                }
            });

            {
                let mut val = value.lock().unwrap();
                *val = THRESHOLD;
                drop(val);
                let mut q = waiters.lock().unwrap();
                if q.0 {
                    q.1 = true;
                }
            }

            reader.join().unwrap();
            let q = waiters.lock().unwrap();
            assert!(!q.0 || q.1, "lost wakeup: waiter parked but not woken");
        });
    }
}

// ---------------------------------------------------------------------------
// 3. Committer group-handoff gate — committer.rs Gate (§2.2 early-close)
// ---------------------------------------------------------------------------

/// The §2.2 early-close [`Gate`](../committer/index.html): the committer must
/// never park its gather forever while the last in-flight appender has already
/// submitted (a stranded, never-committed batch).
///
/// Production shape (`committer.rs`):
/// - `Gate::enter` — `count.fetch_add(1, Relaxed)` (`committer.rs:322`).
/// - `Gate::leave` — `if count.fetch_sub(1, Relaxed) == 1 { take + wake the
///   registered waker }` (`committer.rs:327-333`).
/// - the gather's poll — `register(waker)` **then** re-check `is_zero()` under
///   the same poll (`committer.rs:559-561`), the "set-then-signal race" the
///   code comment calls out.
///
/// The `count` is `Relaxed` — a load could read a **stale** non-zero even after
/// the decrement. Correctness comes from the **waker `Mutex`**: because the
/// committer registers (mutex release) *before* its `is_zero` load, and `leave`
/// does its decrement *before* taking the waker (mutex acquire), whichever
/// critical section runs first supplies a happens-before that makes the other
/// side safe:
/// - `leave`'s section first ⟹ its `fetch_sub` happens-before the committer's
///   later `Relaxed` load, so the load sees `0` and the committer closes;
/// - the committer's `register` first ⟹ `leave` sees the waker and wakes it.
///
/// Either way `closed || woken` — no stranded batch. Loom enumerates both.
#[cfg(loom)]
mod committer_handoff {
    use loom::sync::Arc;
    use loom::sync::Mutex;
    use loom::sync::atomic::{AtomicUsize as LoomUsize, Ordering};

    use super::*;

    static EXPLORED: AtomicUsize = AtomicUsize::new(0);

    /// One appender that has entered the gather window and then submits
    /// (`leave`), one committer that registers its gather waker and re-checks
    /// `is_zero`. Mirrors the real op order exactly.
    #[test]
    fn loom_committer_gate_no_stranded_batch() {
        loom::model(|| {
            count_iteration(&EXPLORED);
            let count = Arc::new(LoomUsize::new(0));
            // The Gate's `waker: Mutex<Option<Waker>>`, modelled as
            // (registered, woken) — a Waker is not a loom primitive, but its
            // *synchronisation* is the mutex, which is.
            let waker = Arc::new(Mutex::new((false, false)));

            // An appender is in flight (Gate::enter, committer.rs:322).
            count.fetch_add(1, Ordering::Relaxed);

            // Gate::leave (committer.rs:327-333): submit, and if last, wake.
            let c = count.clone();
            let wk = waker.clone();
            let appender = loom::thread::spawn(move || {
                if c.fetch_sub(1, Ordering::Relaxed) == 1 {
                    let mut w = wk.lock().unwrap();
                    if w.0 {
                        w.1 = true; // registered waker present → wake it
                    }
                }
            });

            // The gather poll (committer.rs:559-561): register FIRST, then
            // load.
            {
                let mut w = waker.lock().unwrap();
                w.0 = true;
            }
            let closed = count.load(Ordering::Relaxed) == 0;

            appender.join().unwrap();

            let w = waker.lock().unwrap();
            assert!(
                closed || w.1,
                "batch stranded: the committer neither observed the gate \
                 reach zero nor was woken by the last appender's submission"
            );
        });
        eprintln!(
            "loom committer_handoff (register-before-check): {} \
             interleavings, no stranded batch",
            EXPLORED.swap(0, StdOrdering::Relaxed)
        );
    }

    /// Test-of-the-test: reverse the two lines — load `is_zero` **before**
    /// registering the waker (the bug `committer.rs:557`'s comment guards
    /// against). Loom finds the interleaving where the committer's stale
    /// `Relaxed` load misses the decrement AND the appender leaves before the
    /// waker is registered: the gather parks forever on an already-submitted
    /// convoy. (In production the `max_delay` cap is the real-time backstop for
    /// this — but the cap is wall-clock, invisible to loom, so the gate alone
    /// must not lose the wakeup, which the correct order guarantees.)
    #[test]
    #[should_panic(expected = "batch stranded")]
    fn loom_committer_gate_check_before_register_strands() {
        loom::model(|| {
            let count = Arc::new(LoomUsize::new(0));
            let waker = Arc::new(Mutex::new((false, false)));

            count.fetch_add(1, Ordering::Relaxed);

            let c = count.clone();
            let wk = waker.clone();
            let appender = loom::thread::spawn(move || {
                if c.fetch_sub(1, Ordering::Relaxed) == 1 {
                    let mut w = wk.lock().unwrap();
                    if w.0 {
                        w.1 = true;
                    }
                }
            });

            // BUGGY ORDER: load before register.
            let closed = count.load(Ordering::Relaxed) == 0;
            {
                let mut w = waker.lock().unwrap();
                w.0 = true;
            }

            appender.join().unwrap();
            let w = waker.lock().unwrap();
            assert!(closed || w.1, "batch stranded: lost gate wakeup");
        });
    }
}

// ---------------------------------------------------------------------------
// 4. Seal-time active-index swap — BLOCKED on bn-25d (documented, not run)
// ---------------------------------------------------------------------------

/// **Planned, blocked on bn-25d.** The seal-time active-index swap does not
/// exist in the crate yet, so there is no production primitive to model — this
/// module is documentation of the loom test to add when bn-25d lands, not a
/// running test.
///
/// # The protocol (as designed in bn-25d's brief)
///
/// At segment seal the active in-memory index is replaced: the sealer builds
/// the new (sealed) index, publishes it via an atomic pointer swap
/// (`AtomicPtr`/`arc-swap`-style `store(Release)`), and readers load it
/// (`Acquire`) to route lookups. This carries the same publish ordering burden
/// as the watermark ([`watermark_publish`]): a reader that observes the new
/// index pointer MUST see the fully-built index behind it (release/acquire), or
/// it reads a half-constructed index — a torn-read the other verification
/// layers cannot catch.
///
/// # The loom model to add
///
/// - 1 sealer: build the new index (write its cells `Relaxed`), then
///   `swap.store(new_ptr, Release)`.
/// - N readers: `swap.load(Acquire)`; if it is the new pointer, assert every
///   cell reads back its built value (no torn read); the old-index arc must not
///   be dropped while a reader still holds it (loom's `Arc` drop-order check).
/// - Test-of-the-test: `Release → Relaxed` on the swap must FAIL under loom,
///   exactly as [`watermark_publish`]'s relaxed variant does.
///
/// Tracked by bn-25d; this file's [`watermark_publish`] model is the reusable
/// template for it.
#[cfg(loom)]
mod index_swap {
    // Intentionally empty: no production active-index primitive exists yet
    // (bn-25d). See the module doc for the model to add once it does.
}
