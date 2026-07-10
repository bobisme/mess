//! Wiring the **real**, on-disk store into the web layer's object-safe
//! [`DynRead`](crate::web::DynRead)/[`DynWrite`](crate::web::DynWrite) seam
//! (bn-1mw, completing the bn-13l dogfood note that swapping in the real
//! backend was "the one remaining wiring step").
//!
//! `Store` pins the concrete backend the demo binaries use:
//! [`mess_store::EventStore`] over [`mess_store::LogEngine`], the same
//! composed on-disk engine [`crate::seed`] writes through and `mess doctor` /
//! `mess verify` operate on.
//!
//! # Why this lives in its own module, not inline in a bin
//!
//! [`impl_dyn_read!`](crate::impl_dyn_read)/[`impl_dyn_write!`](crate::impl_dyn_write)
//! expand to `impl $crate::web::DynRead for $ty` / `impl ... DynWrite for
//! $ty`. Rust's orphan rule allows a **local trait** (`DynRead`/`DynWrite`,
//! defined in this crate) to be implemented for a **foreign type**
//! (`EventStore`/`LogEngine`, defined in `mess-store`) — but only from
//! *within the crate that defines the trait*. A binary crate (`social-web`)
//! cannot write this impl itself: neither the trait nor the type would be
//! local to it. So the impl has to live here, in the `social` library, even
//! though only the demo binaries use it.
use crate::projections::Projections;

/// The on-disk backend the demo binaries write through and read from:
/// [`mess_store::EventStore`] over [`mess_store::LogEngine`].
pub type Store = mess_store::EventStore<mess_store::LogEngine>;

/// The on-disk backend's rebuildable read model: [`Projections`] over the
/// same [`mess_store::LogEngine`].
pub type StoreProjections = Projections<mess_store::LogEngine>;

crate::impl_dyn_write!(Store);
crate::impl_dyn_read!(StoreProjections);
