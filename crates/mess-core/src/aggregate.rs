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
///
/// # Cross-aggregate preconditions
///
/// `decide` takes `&self` — one aggregate's folded state — and nothing else,
/// so it structurally cannot check a *different* aggregate's stream (e.g. "is
/// this foreign id registered?"). That is by design, not a gap: see
/// `docs/cross-aggregate-invariants.md` for why (one stream is the store's
/// only atomic boundary) and the blessed patterns (accept-and-reconcile
/// downstream, escalating to a saga/compensating command) for when a rule
/// needs another aggregate's facts.
pub trait Decide<C>: Aggregate {
    /// The typed business-rule rejection this handler can produce.
    type Rejection: std::error::Error;

    /// Inspect state and either emit events or reject the command.
    fn decide(&self, cmd: C) -> Result<Vec<Self::Event>, Self::Rejection>;
}

/// A command that names the **actor stream** it must be committed against.
///
/// # The problem: self-referential invariants echo the stream id
///
/// [`Decide::decide`] is a pure function of *one* aggregate's folded state; it
/// never sees the stream id that state was loaded from. So a *self-referential*
/// invariant — "a user cannot follow **themselves**", "an account cannot
/// transfer to **itself**" — has no way to learn *which* aggregate it is
/// deciding for, and the command is forced to carry that id explicitly (e.g.
/// `Follow { follower, .. }`). That id is redundant with the stream key the
/// command is dispatched to (`user-<follower>`), and the redundancy is
/// dangerous: a `Follow { follower: X }` committed to *Y*'s stream is
/// **representable**, type-checks, and `decide` — which never learns Y — cannot
/// catch it. (Dogfood finding bn-2i3, from the social domain's `Follow`.)
///
/// # The convention, now checked
///
/// A command implements `Actor` to declare, from its own fields, the stream id
/// it asserts it belongs to. The store's *authored* command path
/// (`EventStore::command_as` / `EventStore::command_cached_as`) compares that
/// declaration to the stream it is actually dispatched against **before** it
/// calls `decide`, so an actor/stream divergence fails fast instead of silently
/// writing an event to the wrong stream. The actor field stays — the pattern is
/// *blessed*, not removed — but it is now **validated**, not merely trusted.
///
/// The declaration is a stream id `String` (not the bare actor id) so the store
/// can compare it verbatim to the `stream_id` it was handed without knowing any
/// application's `user-<id>` naming scheme. Build it with the *same* helper the
/// caller dispatches with (e.g. `user_stream(self.follower)`), so the two
/// spellings share one source of truth.
pub trait Actor {
    /// The stream id this command asserts it is authored against, built from
    /// the command's own actor field(s).
    fn actor_stream(&self) -> String;
}
