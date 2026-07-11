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

/// A type-erased store error that is also cloneable — the seam
/// [`BoxedStoreError`] cannot fill: `Box<dyn Error>` is intrinsically
/// non-`Clone` (cloning a trait object needs a `clone_box`-style method the
/// `Error` trait does not provide), so any seam-local enum that wraps a
/// `BoxedStoreError` variant loses `#[derive(Clone)]` for the whole enum, not
/// just that variant. `SharedStoreError` swaps the `Box` for an `Arc`, which
/// *is* `Clone` regardless of what it points to — cloning shares the
/// allocation rather than deep-copying the underlying error.
///
/// # Equality: `PartialEq` by `Arc::ptr_eq`, not by rendered text
///
/// `SharedStoreError` implements `PartialEq` (and, since pointer identity is
/// reflexive/symmetric/transitive, `Eq`) as **pointer identity**:
/// `Arc::ptr_eq` on the inner `Arc`. Concretely:
///
/// - Two clones of the *same* erased error (`let b = a.clone();`) are equal —
///   they point at the same heap allocation.
/// - Two *separately* erased errors are unequal even if their concrete type and
///   `Display` text are byte-identical — each `SharedStoreError::new` call
///   allocates a fresh `Arc`, so there is nothing shared to compare.
///
/// This is the honest choice, not the convenient one. The tempting
/// alternative — equality by `Display` (`self.to_string() ==
/// other.to_string()`) — was rejected: it is lossy (two genuinely different
/// errors that happen to render the same text become equal) and surprising
/// (equality would silently depend on how much detail the concrete error's
/// `Display` impl chooses to include, a decision made far away from this
/// type and with no obligation to be injective). Pointer identity has none
/// of that ambiguity: `a == b` means "these are the same erasure", full
/// stop, which is exactly what a test double that clones one canned error
/// into several call sites needs (see `examples/social/src/contracts.rs`'s
/// `WriteError::Store`) — every clone of *that one* store failure compares
/// equal to every other clone of it, and to nothing else.
///
/// If a caller genuinely wants text-based comparison, render explicitly
/// (`format!("{a}") == format!("{b}")`) at the call site — that keeps the
/// lossy, order-of-magnitude-cheaper-to-misuse choice visible in the caller's
/// code instead of hidden in this type's `PartialEq` impl.
#[derive(Debug, Clone)]
pub struct SharedStoreError(
    std::sync::Arc<dyn std::error::Error + Send + Sync>,
);

impl SharedStoreError {
    /// Erase and share `err`, wrapping it in an `Arc`.
    pub fn new(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        SharedStoreError(std::sync::Arc::new(err))
    }
}

impl fmt::Display for SharedStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for SharedStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl PartialEq for SharedStoreError {
    /// Identity, not content: see the type's doc comment for why.
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }
}

/// Pointer-identity equality is reflexive, symmetric, and transitive, so
/// `Eq` holds even though the pointee is not itself `Eq` (it is not even
/// `PartialEq` — `dyn Error` has no such bound).
impl Eq for SharedStoreError {}

impl std::ops::Deref for SharedStoreError {
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
    /// # `Clone`/`PartialEq` seams: use [`erase_store_shared`] instead
    ///
    /// [`BoxedStoreError`] wraps a `Box<dyn Error + Send + Sync>`, and a
    /// boxed trait object cannot be `Clone` (there is no `clone_box` in
    /// `std::error::Error`). That makes plain `erase_store` a trap for any
    /// seam-local enum that needs `#[derive(Clone)]` (or `PartialEq`, which
    /// `Box<dyn Error>` also lacks) — wrapping a `BoxedStoreError` in one
    /// variant silently poisons the derive for *every* variant of the enum,
    /// not just that one. If the seam's error type must be `Clone`, or
    /// compared with `==` (a test double that clones a canned
    /// `Result<_, SeamError>` into several call sites, the way
    /// `examples/social/src/web/tests.rs`'s `FakeWriteOps` does, is exactly
    /// this shape), reach for [`erase_store_shared`] and its
    /// [`SharedStoreError`] instead — same one-liner at the call site, `Arc`
    /// instead of `Box`, `Clone` and identity-based `PartialEq`/`Eq` for
    /// free. See [`SharedStoreError`]'s doc comment for what that equality
    /// means and why.
    ///
    /// # Before / after
    ///
    /// Modeled on `examples/social/src/contracts.rs`'s `WriteError`, which
    /// today renders the store error to a `String` because keeping
    /// `WriteError` generic over the backend would leak it into every HTTP
    /// handler signature — and `WriteError` must stay `Clone + PartialEq`
    /// for `FakeWriteOps` above, which rules out plain `erase_store`:
    ///
    /// ```ignore
    /// // Before: WriteError::Store(String) — the source chain is gone,
    /// // only its rendered text survives.
    /// #[derive(Clone, PartialEq)]
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
    /// // After: WriteError::Store(SharedStoreError) — the derives survive,
    /// // and `source()` still walks into the backend's own error chain
    /// // (e.g. an `io::Error` under a `thiserror` backend variant).
    /// #[derive(Clone, PartialEq)]
    /// enum WriteError {
    ///     User(UserError),
    ///     Post(PostError),
    ///     Conflict { stream: String, attempts: u32 },
    ///     Store(mess_core::SharedStoreError),
    /// }
    ///
    /// fn user_err<S>(e: CommandError<UserError, S>) -> WriteError
    /// where
    ///     S: std::error::Error + Send + Sync + 'static,
    /// {
    ///     match e.erase_store_shared() {
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
    /// it to a shared trait object.
    ///
    /// [`erase_store_shared`]: Self::erase_store_shared
    #[must_use]
    pub fn erase_store(self) -> CommandError<R, BoxedStoreError>
    where
        S: std::error::Error + Send + Sync + 'static,
    {
        self.map_store(BoxedStoreError::new)
    }

    /// Erase the store-error type parameter `S` to [`SharedStoreError`], the
    /// `Arc`-backed sibling of [`erase_store`](Self::erase_store)'s
    /// `BoxedStoreError` — reach for this variant whenever the seam's error
    /// type must be `Clone` and/or `PartialEq`/`Eq` (a `Box<dyn Error>`
    /// cannot be, so plain `erase_store` cannot serve that seam at all; see
    /// `erase_store`'s doc comment for the full story and
    /// [`SharedStoreError`]'s for what its `PartialEq` means).
    #[must_use]
    pub fn erase_store_shared(self) -> CommandError<R, SharedStoreError>
    where
        S: std::error::Error + Send + Sync + 'static,
    {
        self.map_store(SharedStoreError::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, thiserror::Error)]
    #[error("disk write failed")]
    struct DiskError {
        #[source]
        io: std::io::Error,
    }

    #[test]
    fn shared_store_error_clone_preserves_display_and_source() {
        let io_err = std::io::Error::other("no space left on device");
        let io_msg = io_err.to_string();
        let original = SharedStoreError::new(DiskError { io: io_err });

        let cloned = original.clone();

        assert_eq!(original.to_string(), "disk write failed");
        assert_eq!(cloned.to_string(), "disk write failed");

        let source = std::error::Error::source(&cloned)
            .expect("clone keeps the source chain");
        assert_eq!(source.to_string(), io_msg);
    }

    #[test]
    fn shared_store_error_eq_is_identity_not_content() {
        let a = SharedStoreError::new(std::io::Error::other("boom"));
        let a_clone = a.clone();
        let b = SharedStoreError::new(std::io::Error::other("boom"));

        // Two clones of the same erasure: equal (same allocation).
        assert_eq!(a, a_clone);
        // Two separate erasures with byte-identical Display text: unequal
        // (this is the documented, deliberate semantics — see the type's
        // doc comment).
        assert_ne!(a, b);
        assert_eq!(a.to_string(), b.to_string());
    }

    /// A wrapping seam enum with `derive(Clone, PartialEq)` must compile and
    /// behave: this is the shape `examples/social/src/contracts.rs`'s
    /// `WriteError` needs.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum SeamError {
        Domain(String),
        Store(SharedStoreError),
    }

    #[test]
    fn wrapping_enum_with_derive_clone_partial_eq_compiles_and_behaves() {
        let shared = SharedStoreError::new(std::io::Error::other("boom"));
        let seam = SeamError::Store(shared.clone());
        let seam_clone = seam.clone();

        assert_eq!(seam, seam_clone);

        let other_domain = SeamError::Domain("nope".to_string());
        assert_ne!(seam, other_domain);

        let separately_erased = SeamError::Store(SharedStoreError::new(
            std::io::Error::other("boom"),
        ));
        assert_ne!(seam, separately_erased);
    }

    #[test]
    fn erase_store_shared_preserves_domain_conflict_and_store() {
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct DomainRejection(&'static str);
        impl fmt::Display for DomainRejection {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
        impl std::error::Error for DomainRejection {}

        let domain: CommandError<DomainRejection, DiskError> =
            CommandError::Domain(DomainRejection("nope"));
        match domain.erase_store_shared() {
            CommandError::Domain(DomainRejection(msg)) => {
                assert_eq!(msg, "nope");
            }
            other => panic!("expected Domain, got {other:?}"),
        }

        let conflict: CommandError<DomainRejection, DiskError> =
            CommandError::Conflict { stream: "s-1".into(), attempts: 3 };
        match conflict.erase_store_shared() {
            CommandError::Conflict { stream, attempts } => {
                assert_eq!(stream, "s-1");
                assert_eq!(attempts, 3);
            }
            other => panic!("expected Conflict, got {other:?}"),
        }

        let io_err = std::io::Error::other("no space left on device");
        let store: CommandError<DomainRejection, DiskError> =
            CommandError::Store(DiskError { io: io_err });
        let erased = store.erase_store_shared();
        // The erased CommandError itself is Clone now.
        let erased_clone = erased.clone();
        assert_eq!(erased.to_string(), erased_clone.to_string());
        match erased {
            CommandError::Store(s) => {
                assert_eq!(s.to_string(), "disk write failed");
            }
            other => panic!("expected Store, got {other:?}"),
        }
    }
}
