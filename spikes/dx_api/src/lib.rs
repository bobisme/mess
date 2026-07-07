//! Spike: north-star event-sourcing DX on top of the existing `mess_db`
//! RocksDB actor backend.
//!
//! Everything here is plain traits — no proc macros. The point is to prove
//! that this calling code works end-to-end against the real backend:
//!
//! ```ignore
//! store.command::<Account, _>("account-123", Withdraw { amount: 50 }).await?;
//! ```
//!
//! See `REPORT.md` for findings.

pub mod store;
pub mod testkit;

use std::fmt;

/// A business-rule rejection produced by [`Decide::decide`].
///
/// Deliberately a dumb string wrapper for the spike. A real implementation
/// would let the aggregate define its own error enum
/// (`type Rejection: std::error::Error`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainError(pub String);

impl DomainError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl fmt::Display for DomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "command rejected: {}", self.0)
    }
}

impl std::error::Error for DomainError {}

/// Failure while encoding or decoding an event payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    Encode(String),
    Decode { event_name: String, source: String },
    /// The stored `message_type` does not correspond to any known event.
    UnknownEventName(String),
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::Encode(e) => write!(f, "event encode failed: {e}"),
            CodecError::Decode { event_name, source } => {
                write!(f, "event decode failed for {event_name:?}: {source}")
            }
            CodecError::UnknownEventName(name) => {
                write!(f, "unknown event name {name:?}")
            }
        }
    }
}

impl std::error::Error for CodecError {}

/// A domain event: has a stable name and a serde-based wire codec.
///
/// For the spike this is implemented manually on an enum; the north star is
/// `#[derive(Event)]` generating exactly this impl.
pub trait Event: Sized + Send + Sync + 'static {
    /// Stable, unique name for this event, stored as the message type
    /// (e.g. `"account.opened"`).
    fn name(&self) -> &'static str;

    /// Serialize the event payload for storage.
    fn encode(&self) -> Result<Vec<u8>, CodecError>;

    /// Deserialize an event from its stored name + payload.
    fn decode(name: &str, data: &[u8]) -> Result<Self, CodecError>;
}

/// Aggregate state folded from a stream of events.
pub trait Aggregate: Default + Send + Sync + 'static {
    type Event: Event;

    /// Fold one event into the state. Must be total and infallible.
    fn apply(&mut self, event: &Self::Event);
}

/// A command handler for aggregate `Self` and command `C`.
///
/// `decide` never mutates state; it only inspects it and either emits new
/// events or rejects the command.
pub trait Decide<C>: Aggregate {
    fn decide(&self, cmd: C) -> Result<Vec<Self::Event>, DomainError>;
}
