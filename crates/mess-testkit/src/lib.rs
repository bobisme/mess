//! mess v1: shared test harnesses and deterministic-simulation-testing
//! (DST) utilities, including crash/torn-write harness support (the
//! A-rule harnesses; see `notes/mess-research/12_convergence.md`).
//!
//! # Given-When-Then aggregate test kit
//!
//! [`AggregateTest`] is a store-free Given-When-Then harness for
//! `mess-core` aggregates: no backend, no async, no I/O. Fold `given`
//! events into an aggregate's default state, run a command through
//! [`Decide::decide`], and assert on the outcome with [`WhenOutcome`]:
//!
//! ```ignore
//! AggregateTest::<Account>::given([AccountEvent::Opened { owner: "alice".into() }])
//!     .when(Withdraw { amount: 1_000 })
//!     .then_error(AccountError::InsufficientFunds { balance: 0, requested: 1_000 });
//! ```
//!
//! Ported from the store-free ground truth in `spikes/dx_api/src/testkit.rs`,
//! retargeted at `mess-core`'s production traits. The one substantive
//! change: `Decide::Rejection` is a typed, per-command associated error
//! instead of the spike's stringly `DomainError`, so
//! [`WhenOutcome::then_error`] takes a matcher (see [`matching`]) rather
//! than requiring a fully-reconstructed error value.
//!
//! # Scope
//!
//! - the Given-When-Then kit above (bn-2cn)
//! - verification support for commit authority and recovery (D1) and batch
//!   framing (D2) invariants, shared across crates' test suites (lands
//!   separately as Phase 1 work continues)

use std::fmt::Debug;

use mess_core::{Aggregate, Decide};

pub mod fixture;

pub use fixture::{
    FixtureError, UPDATE_ENV, assert_fixture_compat, assert_fold_drift,
    check_fixture_compat, check_fold_drift, fold,
};

/// Arrange: prior events for the aggregate under test.
///
/// Generic over the aggregate `A` only — not over any concrete command or
/// event type — so a `#[derive(Aggregate)]`-generated type can be tested
/// with exactly the same call shape a hand-written one uses.
pub struct AggregateTest<A: Aggregate> {
    given: Vec<A::Event>,
}

impl<A: Aggregate> AggregateTest<A> {
    /// Start from a history of events.
    pub fn given(events: impl IntoIterator<Item = A::Event>) -> Self {
        Self { given: events.into_iter().collect() }
    }

    /// Start from a blank (never-written) aggregate. Equivalent to
    /// `given([])`; spells out intent at call sites.
    pub fn given_no_events() -> Self { Self { given: Vec::new() } }

    /// Act: fold the given events, then run the command through `decide`.
    pub fn when<C>(self, cmd: C) -> WhenOutcome<A, C>
    where
        A: Decide<C>,
    {
        let mut state = A::default();
        for event in &self.given {
            state.apply(event);
        }
        WhenOutcome { result: state.decide(cmd) }
    }
}

/// Assert: the outcome of [`Decide::decide`] for one `when(cmd)` call.
pub struct WhenOutcome<A, C>
where
    A: Decide<C>,
{
    result: Result<Vec<A::Event>, <A as Decide<C>>::Rejection>,
}

impl<A, C> WhenOutcome<A, C>
where
    A: Decide<C>,
    A::Event: Debug + PartialEq,
    <A as Decide<C>>::Rejection: Debug + PartialEq,
{
    /// Expect the command to be accepted and emit exactly these events, in
    /// order. Panics with a positional diff on mismatch.
    #[track_caller]
    pub fn then_events(self, expected: impl IntoIterator<Item = A::Event>) {
        let expected: Vec<A::Event> = expected.into_iter().collect();
        match self.result {
            Ok(actual) => {
                if actual != expected {
                    panic!("{}", diff_events(&expected, &actual));
                }
            }
            Err(err) => panic!(
                "expected the command to emit events, but it was \
                 rejected\nexpected events: {expected:#?}\nrejection:       \
                 {err:?}"
            ),
        }
    }

    /// Expect the command to be rejected, matching `matcher` against
    /// [`Decide::Rejection`].
    ///
    /// Accepts either a concrete rejection value (exact match via
    /// `PartialEq`) or a labeled predicate built with [`matching`] — so a
    /// caller that only cares about the rejection's variant, not every
    /// field, isn't forced to reconstruct it exactly:
    ///
    /// ```ignore
    /// .then_error(AccountError::AlreadyOpen) // exact value, via `Into`
    /// .then_error(matching("insufficient funds", |e| {
    ///     matches!(e, AccountError::InsufficientFunds { .. })
    /// }))
    /// ```
    #[track_caller]
    pub fn then_error(
        self,
        matcher: impl Into<ErrorMatch<<A as Decide<C>>::Rejection>>,
    ) {
        let matcher = matcher.into();
        match self.result {
            Ok(events) => panic!(
                "expected the command to be rejected, but it emitted \
                 events\nexpected rejection: {}\nemitted events: {events:#?}",
                matcher.describe()
            ),
            Err(actual) => {
                if !matcher.matches(&actual) {
                    panic!(
                        "command was rejected, but with the wrong \
                         error\nexpected: {}\nactual:   {actual:?}",
                        matcher.describe()
                    );
                }
            }
        }
    }
}

/// A way to check a [`Decide::Rejection`] in [`WhenOutcome::then_error`].
///
/// Built either from a concrete rejection value (via `Into`, exact match)
/// or from [`matching`] (a labeled predicate, for asserting on shape rather
/// than pinning down every field).
pub struct ErrorMatch<R> {
    kind: MatchKind<R>,
}

enum MatchKind<R> {
    Exact(R),
    Predicate { label: String, test: Box<dyn Fn(&R) -> bool> },
}

// A single, non-blanket-vs-blanket `From` impl: `matching()` builds an
// `ErrorMatch<R>` directly (using std's reflexive `impl<T> From<T> for T`
// to satisfy `then_error`'s `Into` bound), so there is no second generic
// impl here to conflict with this one.
impl<R: Debug + PartialEq> From<R> for ErrorMatch<R> {
    fn from(expected: R) -> Self {
        ErrorMatch { kind: MatchKind::Exact(expected) }
    }
}

impl<R: Debug> ErrorMatch<R> {
    fn matches(&self, actual: &R) -> bool
    where
        R: PartialEq,
    {
        match &self.kind {
            MatchKind::Exact(expected) => expected == actual,
            MatchKind::Predicate { test, .. } => test(actual),
        }
    }

    fn describe(&self) -> String {
        match &self.kind {
            MatchKind::Exact(expected) => format!("{expected:?}"),
            MatchKind::Predicate { label, .. } => label.clone(),
        }
    }
}

/// Build a labeled predicate [`ErrorMatch`] for [`WhenOutcome::then_error`],
/// for asserting a rejection's shape (e.g. its variant) without pinning
/// down every field. `label` is shown in failure output in place of a
/// `Debug` rendering of the (nonexistent) expected value.
pub fn matching<R>(
    label: impl Into<String>,
    test: impl Fn(&R) -> bool + 'static,
) -> ErrorMatch<R> {
    ErrorMatch {
        kind: MatchKind::Predicate {
            label: label.into(),
            test:  Box::new(test),
        },
    }
}

/// Render a readable positional diff between expected and actual events.
///
/// Hand-rolled rather than pulling in `pretty_assertions`: event streams
/// compare positionally (index-by-index, with a missing/extra tail when
/// lengths differ), which is a different shape than a line-level text diff
/// over `{:#?}` output — a general-purpose text differ would highlight
/// which *lines of the pretty-printed struct* changed, not "event 2 didn't
/// match; everything before and after it did." A dozen lines here keeps
/// `mess-testkit` dependency-free and gives exactly the shape this kit
/// needs.
fn diff_events<E: Debug + PartialEq>(expected: &[E], actual: &[E]) -> String {
    let mut out = String::from("emitted events did not match\n");
    let len = expected.len().max(actual.len());
    for i in 0..len {
        match (expected.get(i), actual.get(i)) {
            (Some(e), Some(a)) if e == a => {
                out.push_str(&format!("  [{i}] ok        {e:?}\n"));
            }
            (Some(e), Some(a)) => {
                out.push_str(&format!(
                    "  [{i}] MISMATCH\n        expected: {e:?}\n        \
                     actual:   {a:?}\n"
                ));
            }
            (Some(e), None) => {
                out.push_str(&format!("  [{i}] MISSING   expected: {e:?}\n"));
            }
            (None, Some(a)) => {
                out.push_str(&format!("  [{i}] EXTRA     actual:   {a:?}\n"));
            }
            (None, None) => unreachable!(),
        }
    }
    out.push_str(&format!(
        "expected {} event(s), got {}",
        expected.len(),
        actual.len()
    ));
    out
}

#[cfg(test)]
mod tests {
    use mess_core::CodecError;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum CounterEvent {
        Bumped { by: i64 },
    }

    impl mess_core::Event for CounterEvent {
        fn name(&self) -> &'static str { "counter.bumped" }

        fn encode(&self) -> Result<Vec<u8>, CodecError> {
            let CounterEvent::Bumped { by } = self;
            Ok(by.to_le_bytes().to_vec())
        }

        fn decode(name: &str, data: &[u8]) -> Result<Self, CodecError> {
            if name != "counter.bumped" {
                return Err(CodecError::UnknownEventName(name.to_string()));
            }
            let arr: [u8; 8] =
                data.try_into().map_err(|_| CodecError::Decode {
                    event_name: name.to_string(),
                    source:     "expected 8 payload bytes".to_string(),
                })?;
            Ok(CounterEvent::Bumped { by: i64::from_le_bytes(arr) })
        }
    }

    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    struct Counter {
        value: i64,
    }

    impl Aggregate for Counter {
        type Event = CounterEvent;

        fn apply(&mut self, event: &CounterEvent) {
            let CounterEvent::Bumped { by } = event;
            self.value += by;
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum CounterError {
        WouldGoNegative { value: i64, by: i64 },
    }

    impl std::fmt::Display for CounterError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{self:?}")
        }
    }

    impl std::error::Error for CounterError {}

    struct Bump(i64);

    impl Decide<Bump> for Counter {
        type Rejection = CounterError;

        fn decide(&self, cmd: Bump) -> Result<Vec<CounterEvent>, CounterError> {
            if self.value + cmd.0 < 0 {
                return Err(CounterError::WouldGoNegative {
                    value: self.value,
                    by:    cmd.0,
                });
            }
            Ok(vec![CounterEvent::Bumped { by: cmd.0 }])
        }
    }

    #[test]
    fn then_events_passes_on_match() {
        AggregateTest::<Counter>::given_no_events()
            .when(Bump(3))
            .then_events([CounterEvent::Bumped { by: 3 }]);
    }

    #[test]
    fn then_events_folds_given_events_first() {
        AggregateTest::<Counter>::given([CounterEvent::Bumped { by: 10 }])
            .when(Bump(-4))
            .then_events([CounterEvent::Bumped { by: -4 }]);
    }

    #[test]
    fn then_error_exact_value_passes_on_match() {
        AggregateTest::<Counter>::given_no_events()
            .when(Bump(-1))
            .then_error(CounterError::WouldGoNegative { value: 0, by: -1 });
    }

    #[test]
    fn then_error_matching_predicate_passes_on_match() {
        AggregateTest::<Counter>::given_no_events().when(Bump(-1)).then_error(
            matching("would go negative", |e| {
                matches!(e, CounterError::WouldGoNegative { .. })
            }),
        );
    }

    #[test]
    #[should_panic(expected = "MISMATCH")]
    fn then_events_panics_with_positional_diff_on_mismatch() {
        AggregateTest::<Counter>::given_no_events()
            .when(Bump(3))
            .then_events([CounterEvent::Bumped { by: 999 }]);
    }

    #[test]
    fn then_events_diff_reports_missing_and_extra_positionally() {
        let panic = std::panic::catch_unwind(|| {
            AggregateTest::<Counter>::given_no_events()
                .when(Bump(3))
                .then_events([
                    CounterEvent::Bumped { by: 3 },
                    CounterEvent::Bumped { by: 7 },
                ]);
        })
        .expect_err("should have panicked");
        let msg = panic.downcast_ref::<String>().unwrap();
        assert!(msg.contains("[0] ok"), "diff: {msg}");
        assert!(msg.contains("[1] MISSING"), "diff: {msg}");
        assert!(msg.contains("expected 2 event(s), got 1"), "diff: {msg}");
    }

    #[test]
    #[should_panic(expected = "expected the command to emit events")]
    fn then_events_panics_when_rejected_instead() {
        AggregateTest::<Counter>::given_no_events()
            .when(Bump(-1))
            .then_events([CounterEvent::Bumped { by: -1 }]);
    }

    #[test]
    #[should_panic(expected = "expected the command to be rejected")]
    fn then_error_panics_when_accepted_instead() {
        AggregateTest::<Counter>::given_no_events()
            .when(Bump(3))
            .then_error(CounterError::WouldGoNegative { value: 0, by: 3 });
    }

    #[test]
    #[should_panic(expected = "wrong error")]
    fn then_error_panics_on_exact_value_mismatch() {
        AggregateTest::<Counter>::given_no_events().when(Bump(-1)).then_error(
            CounterError::WouldGoNegative { value: 999, by: -1 },
        );
    }

    #[test]
    #[should_panic(expected = "wrong error")]
    fn then_error_panics_on_predicate_mismatch() {
        AggregateTest::<Counter>::given_no_events()
            .when(Bump(-1))
            .then_error(matching("never matches", |_| false));
    }
}
