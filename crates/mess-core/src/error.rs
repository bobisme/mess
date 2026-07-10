//! Error taxonomy for the mess core vocabulary.
//!
//! Two error types live here:
//!
//! - [`CodecError`] — a failure while encoding or decoding an event payload,
//!   surfaced by [`crate::Event::encode`] / [`crate::Event::decode`]. Its shape
//!   is frozen from the `dx_api` spike.
//! - [`CommandError`] — the outcome of a full command round (load → decide →
//!   append). It keeps the spike's three-variant shape (`Domain` / `Conflict` /
//!   `Store`) but is generic over the domain rejection `R` and the store error
//!   `S` so `mess-core` never has to name a backend type (a backend crate
//!   supplies `S`; the aggregate supplies `R` via
//!   [`crate::Decide::Rejection`]).

use std::fmt;

/// Failure while encoding or decoding an event payload.
///
/// Kept byte-for-byte in shape from the `dx_api` spike so a `#[derive(Event)]`
/// (bn-hy7) can emit exactly these variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// Serialization of an event payload failed.
    Encode(String),
    /// Deserialization failed for a known event name.
    Decode {
        /// The event name whose payload failed to decode.
        event_name: String,
        /// The underlying decoder error, rendered.
        source:     String,
    },
    /// The stored name does not correspond to any known event.
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

/// The outcome of a failed command round.
///
/// Generic parameters keep `mess-core` free of any backend dependency while
/// preserving the spike's three-variant shape:
///
/// - `R` — the domain rejection, i.e. [`crate::Decide::Rejection`], returned by
///   [`crate::Decide::decide`] when a business rule refuses the command.
/// - `S` — the infrastructure (store) error, supplied by whichever backend
///   crate drives the load/append; `mess-core` never names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandError<R, S> {
    /// The aggregate rejected the command (a business rule).
    Domain(R),
    /// Optimistic-retry budget exhausted: the stream kept moving under us.
    Conflict {
        /// The stream that could not be written.
        stream:   String,
        /// How many optimistic attempts were made before giving up.
        attempts: u32,
    },
    /// The underlying store failed.
    Store(S),
}

impl<R: fmt::Display, S: fmt::Display> fmt::Display for CommandError<R, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandError::Domain(e) => write!(f, "{e}"),
            CommandError::Conflict { stream, attempts } => write!(
                f,
                "gave up after {attempts} optimistic attempts on stream \
                 {stream:?}: concurrent writers kept changing the stream"
            ),
            CommandError::Store(e) => write!(f, "{e}"),
        }
    }
}

impl<R, S> std::error::Error for CommandError<R, S>
where
    R: std::error::Error + 'static,
    S: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CommandError::Domain(e) => Some(e),
            CommandError::Store(e) => Some(e),
            CommandError::Conflict { .. } => None,
        }
    }
}
