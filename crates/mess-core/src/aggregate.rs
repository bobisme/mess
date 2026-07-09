//! The [`Aggregate`] and [`Decide`] traits: state folding and command
//! handling.

use crate::event::Event;

/// Aggregate state folded from a stream of events.
///
/// [`apply`](Aggregate::apply) is **total and infallible** by contract: an
/// event that reached the log is history, and replaying history must never
/// fail. All validation happens earlier, in [`Decide::decide`]; `apply` only
/// mutates state. This split is what lets a backend replay a stream at load
/// time without any error path.
pub trait Aggregate: Default + Send + Sync + 'static {
    /// The event type this aggregate folds.
    type Event: Event;

    /// Fold one event into the state. Must be total and infallible.
    fn apply(&mut self, event: &Self::Event);
}

/// A command handler for aggregate `Self` and command `C`.
///
/// `decide` never mutates state; it only inspects the current state and either
/// emits new events or rejects the command with a **typed** rejection.
///
/// # The `Rejection` associated type
///
/// In the `dx_api` spike the rejection was a stringly-typed `DomainError`,
/// with the note that a real implementation should "let the aggregate define
/// its own error enum (`type Rejection: std::error::Error`)". This is that
/// production form. The rejection is attached to `Decide<C>` rather than to
/// [`Aggregate`] so that each command may reject with its own precise error
/// type; an aggregate that wants a single shared error enum simply names that
/// one enum in every `impl Decide<_>`.
///
/// A `#[derive]` (bn-hy7) can generate this impl mechanically: it knows the
/// command type from the handler, the event type from the aggregate, and emits
/// `type Rejection = <the declared error>;`.
pub trait Decide<C>: Aggregate {
    /// The typed business-rule rejection this handler can produce.
    type Rejection: std::error::Error;

    /// Inspect state and either emit events or reject the command.
    fn decide(&self, cmd: C) -> Result<Vec<Self::Event>, Self::Rejection>;
}
