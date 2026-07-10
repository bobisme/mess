//! The **web layer**: a small [`axum`] server rendering server-side HTML for
//! the social domain, written against the [`contracts`](crate::contracts) seam
//! ([`ReadModels`](crate::contracts::ReadModels) for queries,
//! [`WriteOps`](crate::contracts::WriteOps) for commands) so it builds and
//! tests against the in-memory [`FakeReadModels`](crate::FakeReadModels) with
//! no dependency on the real projections bone.
//!
//! # Why [`AppState`] is generic, not a trait object
//!
//! An earlier version of this module hand-wrote `Send`-boxed mirror traits
//! (`DynRead`/`DynWrite`) purely to route around a gap in
//! [`ReadModels`](crate::contracts::ReadModels)/[`WriteOps`](crate::contracts::WriteOps):
//! those traits used bare `async fn`, whose futures carry no `Send` bound, so
//! a router generic over `R: ReadModels` would not compile — axum's
//! [`Handler`](axum::handler::Handler) requires handler futures to be `Send`,
//! and the compiler cannot prove that for an unconstrained generic `R`'s
//! async-fn futures. The shim traded that gap away by wrapping every call in
//! `Box::pin(..)`, provably `Send` only because each macro expansion
//! (`impl_dyn_read!`/`impl_dyn_write!`) fixed a single *concrete* backing
//! type — one hand-written impl per backend, plus
//! `Arc<dyn DynRead>`/`Arc<dyn DynWrite>` dynamic dispatch on every call.
//!
//! Now that `ReadModels`/`WriteOps` state their futures' `Send`-ness directly
//! (`-> impl Future<Output = T> + Send`, RPITIT), the gap that shim worked
//! around is gone, so it — and the per-concrete-type
//! `impl_dyn_read!`/`impl_dyn_write!` macros that used to live in
//! [`store_backend`](crate::store_backend) — is deleted. [`AppState<R, W>`]
//! is generic directly over the two traits, and [`router`] monomorphizes once
//! per `(R, W)` pair a binary instantiates it with: no boxing, no dynamic
//! dispatch, no per-type macro. `src/bin/social-web.rs` picks between
//! [`MemBackend`] (`AppState<MemBackend, MemBackend>`) and the real
//! store-backed pair (`AppState<Projections<LogEngine>, Store>`) at startup,
//! building its own `Router` and calling `axum::serve` in each branch —
//! the price of "generic state, not a trait object" is that two backends
//! that both exist at once (rather than one chosen at startup) would need two
//! monomorphized servers, not a shared one. For this demo, where exactly one
//! backend is live per process, that price is free.
//!
//! [`Projections`]: crate::Projections
//!
//! # No more side directory
//!
//! A `handle <-> Id` directory used to live here too, hand-maintained by the
//! web layer because `ReadModels` could answer queries by handle but had no
//! way to resolve a handle to the `Id` that `WriteOps` and id-comparisons
//! need. [`ReadModels::resolve`](crate::contracts::ReadModels::resolve)
//! closes that gap directly, so:
//!
//! - The acting-user cookie ([`ACTING_COOKIE`]) stores the handle itself, not
//!   an id — trivial to render ("@" + the cookie value, no lookup) and
//!   trivially resolved to an id with one `resolve` call wherever a write
//!   needs one.
//! - A handle typed into a URL (`/u/alice/follow`) resolves to a target id
//!   the same way.
//!
//! No boot-time bulk directory is needed either, which is why
//! `Projections::directory` — previously `pub`, called once at server startup
//! to seed the directory — is gone.

use std::sync::Arc;

mod error;
mod handlers;
pub mod mem;
mod views;

#[cfg(test)]
mod tests;

pub use handlers::router;
pub use mem::MemBackend;

/// How many posts a feed page shows.
pub const PAGE_SIZE: usize = 20;

/// The name of the cookie holding the acting user's **handle** (the
/// "act-as-user" demo picker — **not** real auth; see the `/whoami` page).
/// Stores the handle, not an [`ident::Id`]: see the module docs' "No more
/// side directory" section for why.
pub const ACTING_COOKIE: &str = "acting_user";

// ===========================================================================
// Application state
// ===========================================================================

/// The shared state every handler receives (cheap to clone — both fields are
/// `Arc`s), generic over the concrete `ReadModels`/`WriteOps` implementation.
/// See the module docs for why this replaced a trait-object `AppState` plus a
/// `Directory`.
pub struct AppState<R, W> {
    pub read: Arc<R>,
    pub write: Arc<W>,
}

// Written by hand rather than `#[derive(Clone)]`: a derive would add
// `R: Clone, W: Clone` bounds, but only the `Arc`s need cloning.
impl<R, W> Clone for AppState<R, W> {
    fn clone(&self) -> Self {
        Self { read: self.read.clone(), write: self.write.clone() }
    }
}

impl<R, W> AppState<R, W> {
    /// Build state from a reader and a writer.
    #[must_use]
    pub fn new(read: Arc<R>, write: Arc<W>) -> Self {
        Self { read, write }
    }
}
