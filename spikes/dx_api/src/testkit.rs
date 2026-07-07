//! Given-When-Then test kit for aggregates. Pure in-memory: no store, no
//! async, no I/O.
//!
//! ```ignore
//! AggregateTest::<Account>::given([Opened { .. }])
//!     .when(Withdraw { amount: 100 })
//!     .then_error(DomainError::new("insufficient funds ..."));
//! ```

use std::fmt::Debug;

use crate::{Aggregate, Decide, DomainError};

/// Arrange: prior events for the aggregate under test.
pub struct AggregateTest<A: Aggregate> {
    given: Vec<A::Event>,
}

impl<A: Aggregate> AggregateTest<A> {
    /// Start from a history of events.
    pub fn given(events: impl IntoIterator<Item = A::Event>) -> Self {
        Self { given: events.into_iter().collect() }
    }

    /// Start from a blank (never-written) aggregate.
    pub fn given_no_events() -> Self {
        Self { given: Vec::new() }
    }

    /// Act: fold the given events, then run the command through `decide`.
    pub fn when<C>(self, cmd: C) -> WhenOutcome<A>
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

/// Assert: the outcome of `decide`.
pub struct WhenOutcome<A: Aggregate> {
    result: Result<Vec<A::Event>, DomainError>,
}

impl<A: Aggregate> WhenOutcome<A>
where
    A::Event: Debug + PartialEq,
{
    /// Expect the command to be accepted and emit exactly these events.
    /// Panics with a positional diff on mismatch.
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
                "expected the command to emit events, but it was rejected\n\
                 expected events: {expected:#?}\n\
                 rejection:       {err}"
            ),
        }
    }

    /// Expect the command to be rejected with exactly this error.
    #[track_caller]
    pub fn then_error(self, expected: DomainError) {
        match self.result {
            Ok(events) => panic!(
                "expected the command to be rejected, but it emitted \
                 events\nexpected rejection: {expected}\nemitted events: \
                 {events:#?}"
            ),
            Err(actual) => {
                if actual != expected {
                    panic!(
                        "command was rejected, but with the wrong \
                         error\nexpected: {:?}\nactual:   {:?}",
                        expected.0, actual.0
                    );
                }
            }
        }
    }
}

/// Render a readable positional diff between expected and actual events.
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
