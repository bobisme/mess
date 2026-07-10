//! Type aliases naming the concrete on-disk backend/read-model pair the demo
//! binaries use.
//!
//! `Store` pins the concrete backend the demo binaries use:
//! [`mess_store::EventStore`] over [`mess_store::LogEngine`], the same
//! composed on-disk engine [`crate::seed`] writes through and `mess doctor` /
//! `mess verify` operate on. `StoreProjections` is its rebuildable read
//! model, [`Projections`] over the same engine.
//!
//! # Why this no longer needs a macro-generated shim
//!
//! An earlier version of this module called `impl_dyn_read!`/`impl_dyn_write!`
//! here to implement the web layer's local, object-safe `DynRead`/`DynWrite`
//! mirror traits for these two foreign types. Rust's orphan rule allows a
//! *local* trait to be implemented for a *foreign* type only from within the
//! crate that defines the trait, so — with `DynRead`/`DynWrite` local to
//! `social::web` — that impl had to live here even though only the demo
//! binaries used it.
//!
//! Now that [`crate::contracts::ReadModels`]/[`crate::contracts::WriteOps`]
//! declare their futures `Send` directly (see `web`'s module docs),
//! [`web::AppState`](crate::web::AppState) is generic over any
//! `R: ReadModels`/`W: WriteOps` instead of a pair of trait objects. `Store`
//! and `StoreProjections` already implement those traits via the blanket
//! `impl<B: Backend> WriteOps for EventStore<B>` in
//! [`crate::contracts`] and `impl<B: Backend> ReadModels for Projections<B>`
//! in [`crate::projections`] — there is nothing left to generate for them
//! here, so this module is down to the two type aliases.
use crate::projections::Projections;

/// The on-disk backend the demo binaries write through and read from:
/// [`mess_store::EventStore`] over [`mess_store::LogEngine`].
pub type Store = mess_store::EventStore<mess_store::LogEngine>;

/// The on-disk backend's rebuildable read model: [`Projections`] over the
/// same [`mess_store::LogEngine`].
pub type StoreProjections = Projections<mess_store::LogEngine>;
