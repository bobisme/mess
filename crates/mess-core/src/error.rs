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

/// A type-erased store error: any `std::error::Error` that is `Send + Sync +
/// 'static`, boxed.
///
/// This is the erasure target of [`CommandError::erase_store`]. It is a thin
/// newtype around `Box<dyn Error + Send + Sync>`, **not** a bare type alias
/// for one, for a real reason: `std` gives `Box<dyn Error + Send + Sync>`
/// a `Display` impl (the blanket `impl<T: Display + ?Sized> Display for
/// Box<T>`) but deliberately *not* an `Error` impl — `impl<E: Error> Error
/// for Box<E>` requires `E: Sized`, which a trait object is not, and there is
/// no separate blanket for the unsized case. A bare alias would therefore
/// satisfy `CommandError`'s `Display` bound but silently fail its `Error`
/// bound (`S: std::error::Error + 'static`), which is exactly the bound that
/// keeps `source()` reachable through `CommandError` itself — so this
/// newtype's whole reason to exist is forwarding [`Error::source`] by hand.
///
/// [`Error::source`]: std::error::Error::source
#[derive(Debug)]
pub struct BoxedStoreError(Box<dyn std::error::Error + Send + Sync>);

impl BoxedStoreError {
    /// Box `err`, erasing its concrete type.
    pub fn new(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        BoxedStoreError(Box::new(err))
    }
}

impl fmt::Display for BoxedStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for BoxedStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl std::ops::Deref for BoxedStoreError {
    type Target = dyn std::error::Error + Send + Sync;

    fn deref(&self) -> &Self::Target { self.0.as_ref() }
}

impl<R, S> CommandError<R, S> {
    /// Change the store-error type parameter `S` to `S2` by applying `f`,
    /// leaving [`Domain`](CommandError::Domain) and
    /// [`Conflict`](CommandError::Conflict) untouched.
    ///
    /// This is the general form [`erase_store`](Self::erase_store) is built
    /// from; reach for it directly when the target is some other type than
    /// [`BoxedStoreError`] (e.g. mapping `S` to a seam-local enum variant, the
    /// way `social`'s `WriteError::Store(String)` does today via
    /// `s.to_string()`).
    #[must_use]
    pub fn map_store<S2>(self, f: impl FnOnce(S) -> S2) -> CommandError<R, S2> {
        match self {
            CommandError::Domain(r) => CommandError::Domain(r),
            CommandError::Conflict { stream, attempts } => {
                CommandError::Conflict { stream, attempts }
            }
            CommandError::Store(s) => CommandError::Store(f(s)),
        }
    }

    /// Erase the store-error type parameter `S`, keeping the typed domain
    /// rejection `R`.
    ///
    /// `CommandError<R, S>` is precise — `S` is exactly the backend's error
    /// type — but that precision means `S` ripples into every signature
    /// downstream of a `command()` call: a seam trait generic over the
    /// backend (like `social`'s `WriteOps`) either stays generic over `S`
    /// too, or its errors must be flattened to something backend-agnostic.
    /// `erase_store` is the "keep `R` typed, drop `S` to a trait object"
    /// middle ground: cheaper than a full stringly-typed enum (the boxed
    /// error still round-trips through `Display` and `source()`, so nothing
    /// about the failure is lost — only its concrete type), and zero-cost
    /// when unused (nothing here runs unless called).
    ///
    /// # Before / after
    ///
    /// Modeled on `examples/social/src/contracts.rs`'s `WriteError`, which
    /// today renders the store error to a `String` because keeping
    /// `WriteError` generic over the backend would leak it into every HTTP
    /// handler signature:
    ///
    /// ```ignore
    /// // Before: WriteError::Store(String) — the source chain is gone,
    /// // only its rendered text survives.
    /// enum WriteError {
    ///     User(UserError),
    ///     Post(PostError),
    ///     Conflict { stream: String, attempts: u32 },
    ///     Store(String),
    /// }
    ///
    /// fn user_err<S: std::fmt::Display>(e: CommandError<UserError, S>) -> WriteError {
    ///     match e {
    ///         CommandError::Domain(d) => WriteError::User(d),
    ///         CommandError::Conflict { stream, attempts } => {
    ///             WriteError::Conflict { stream, attempts }
    ///         }
    ///         CommandError::Store(s) => WriteError::Store(s.to_string()),
    ///     }
    /// }
    /// ```
    ///
    /// ```ignore
    /// // After: WriteError::Store(BoxedStoreError) — same one-liner at the
    /// // call site, but `source()` still walks into the backend's own error
    /// // chain (e.g. an `io::Error` under a `thiserror` backend variant).
    /// enum WriteError {
    ///     User(UserError),
    ///     Post(PostError),
    ///     Conflict { stream: String, attempts: u32 },
    ///     Store(mess_core::BoxedStoreError),
    /// }
    ///
    /// fn user_err<S>(e: CommandError<UserError, S>) -> WriteError
    /// where
    ///     S: std::error::Error + Send + Sync + 'static,
    /// {
    ///     match e.erase_store() {
    ///         CommandError::Domain(d) => WriteError::User(d),
    ///         CommandError::Conflict { stream, attempts } => {
    ///             WriteError::Conflict { stream, attempts }
    ///         }
    ///         CommandError::Store(s) => WriteError::Store(s),
    ///     }
    /// }
    /// ```
    ///
    /// `WriteOps` itself is unchanged either way — the point of erasure is
    /// that the *seam* (`WriteError`, `user_err`/`post_err`) is the one place
    /// that ever names `S`, whether it collapses it to a `String` or erases
    /// it to a `BoxedStoreError`.
    #[must_use]
    pub fn erase_store(self) -> CommandError<R, BoxedStoreError>
    where
        S: std::error::Error + Send + Sync + 'static,
    {
        self.map_store(BoxedStoreError::new)
    }
}
