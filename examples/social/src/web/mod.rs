//! The **web layer**: a small [`axum`] server rendering server-side HTML for
//! the social domain, written against the [`contracts`](crate::contracts) seam
//! ([`ReadModels`] for queries, [`WriteOps`] for commands) so it builds and
//! tests against the in-memory [`FakeReadModels`] with no dependency on the
//! real projections bone.
//!
//! # Why the local [`DynRead`] / [`DynWrite`] adapter traits
//!
//! The bn-154 [`ReadModels`]/[`WriteOps`] traits use bare `async fn` in traits
//! with **no `Send` bound** (their doc calls this "fine for an example"). But
//! axum's [`Handler`](axum::handler::Handler) requires handler futures to be
//! `Send`, and a router built generically over `R: ReadModels` will **not**
//! compile — the compiler cannot prove the trait's futures are `Send`.
//!
//! Rather than edit the shared `contracts.rs` (built against in parallel by the
//! projections bone), the web layer defines its own object-safe,
//! `Send`-boxed **mirror traits** here and implements them for the concrete
//! backing types. `Box::pin(ReadModels::foo(self, ..))` coerces to
//! `dyn Future + Send` because for a *concrete* `Self` the future's `Send`-ness
//! is known. Handlers are then non-generic over `Arc<dyn DynRead>` /
//! `Arc<dyn DynWrite>` and the router compiles. (Dogfood finding: logged on the
//! bone — a production contract would add `+ Send` and this whole shim would
//! disappear.)

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use ident::Id;

use crate::contracts::{ProfileView, PostView, TimelinePage, WriteError};

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

/// The name of the cookie holding the acting user's [`Id`] (the "act-as-user"
/// demo picker — **not** real auth; see the `/whoami` page).
pub const ACTING_COOKIE: &str = "acting_user";

// ===========================================================================
// Send-boxed mirror traits (see module docs for the why)
// ===========================================================================

/// A pinned, boxed, `Send` future — the shape every [`DynRead`]/[`DynWrite`]
/// method returns so the trait is object-safe and its futures are `Send`.
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Object-safe, `Send`-bounded mirror of [`ReadModels`].
pub trait DynRead: Send + Sync {
    fn home_timeline(
        &self,
        user: Id,
        cursor: Option<String>,
        limit: usize,
    ) -> BoxFut<'_, TimelinePage>;
    fn user_posts<'a>(
        &'a self,
        handle: &'a str,
        cursor: Option<String>,
        limit: usize,
    ) -> BoxFut<'a, TimelinePage>;
    fn firehose(
        &self,
        cursor: Option<String>,
        limit: usize,
    ) -> BoxFut<'_, TimelinePage>;
    fn profile<'a>(
        &'a self,
        handle: &'a str,
        viewer: Option<Id>,
    ) -> BoxFut<'a, Option<ProfileView>>;
    fn post<'a>(
        &'a self,
        id: &'a str,
        viewer: Option<Id>,
    ) -> BoxFut<'a, Option<PostView>>;
    fn wait_for(&self, position: u64) -> BoxFut<'_, ()>;
}

/// Generate a [`DynRead`] impl for one **concrete** [`ReadModels`] type.
///
/// It must be per-concrete-type, not a blanket `impl<R: ReadModels>`: the
/// `Box::pin` coercions to `dyn Future + Send` are only provable when `Self`
/// is concrete (a generic `R`'s async-fn futures carry no `Send` bound — the
/// very gap that forced this shim).
#[macro_export]
macro_rules! impl_dyn_read {
    ($ty:ty) => {
        impl $crate::web::DynRead for $ty {
            fn home_timeline(
                &self,
                user: ::ident::Id,
                cursor: ::std::option::Option<::std::string::String>,
                limit: usize,
            ) -> $crate::web::BoxFut<'_, $crate::contracts::TimelinePage> {
                ::std::boxed::Box::pin(
                    $crate::contracts::ReadModels::home_timeline(
                        self, user, cursor, limit,
                    ),
                )
            }
            fn user_posts<'a>(
                &'a self,
                handle: &'a str,
                cursor: ::std::option::Option<::std::string::String>,
                limit: usize,
            ) -> $crate::web::BoxFut<'a, $crate::contracts::TimelinePage> {
                ::std::boxed::Box::pin(
                    $crate::contracts::ReadModels::user_posts(
                        self, handle, cursor, limit,
                    ),
                )
            }
            fn firehose(
                &self,
                cursor: ::std::option::Option<::std::string::String>,
                limit: usize,
            ) -> $crate::web::BoxFut<'_, $crate::contracts::TimelinePage> {
                ::std::boxed::Box::pin(
                    $crate::contracts::ReadModels::firehose(self, cursor, limit),
                )
            }
            fn profile<'a>(
                &'a self,
                handle: &'a str,
                viewer: ::std::option::Option<::ident::Id>,
            ) -> $crate::web::BoxFut<
                'a,
                ::std::option::Option<$crate::contracts::ProfileView>,
            > {
                ::std::boxed::Box::pin(
                    $crate::contracts::ReadModels::profile(self, handle, viewer),
                )
            }
            fn post<'a>(
                &'a self,
                id: &'a str,
                viewer: ::std::option::Option<::ident::Id>,
            ) -> $crate::web::BoxFut<
                'a,
                ::std::option::Option<$crate::contracts::PostView>,
            > {
                ::std::boxed::Box::pin(
                    $crate::contracts::ReadModels::post(self, id, viewer),
                )
            }
            fn wait_for(&self, position: u64) -> $crate::web::BoxFut<'_, ()> {
                ::std::boxed::Box::pin(
                    $crate::contracts::ReadModels::wait_for(self, position),
                )
            }
        }
    };
}

impl_dyn_read!(crate::contracts::FakeReadModels);

/// Object-safe, `Send`-bounded mirror of the [`WriteOps`] methods the web layer
/// uses. Each returns the write's global log position (the read-your-writes
/// token) or a [`WriteError`].
pub trait DynWrite: Send + Sync {
    fn register(
        &self,
        user: Id,
        handle: String,
        display_name: String,
    ) -> BoxFut<'_, Result<u64, WriteError>>;
    fn follow(&self, follower: Id, target: Id)
    -> BoxFut<'_, Result<u64, WriteError>>;
    fn unfollow(
        &self,
        follower: Id,
        target: Id,
    ) -> BoxFut<'_, Result<u64, WriteError>>;
    fn create_post(
        &self,
        post: Id,
        author: Id,
        body: String,
    ) -> BoxFut<'_, Result<u64, WriteError>>;
    fn delete_post(&self, post: Id, by: Id)
    -> BoxFut<'_, Result<u64, WriteError>>;
    fn like(&self, post: Id, user: Id) -> BoxFut<'_, Result<u64, WriteError>>;
    fn unlike(&self, post: Id, user: Id) -> BoxFut<'_, Result<u64, WriteError>>;
}

/// Generate a [`DynWrite`] impl for one **concrete** [`WriteOps`] type (same
/// per-concrete-type constraint as [`impl_dyn_read!`]).
#[macro_export]
macro_rules! impl_dyn_write {
    ($ty:ty) => {
        impl $crate::web::DynWrite for $ty {
            fn register(
                &self,
                user: ::ident::Id,
                handle: ::std::string::String,
                display_name: ::std::string::String,
            ) -> $crate::web::BoxFut<
                '_,
                ::std::result::Result<u64, $crate::contracts::WriteError>,
            > {
                ::std::boxed::Box::pin($crate::contracts::WriteOps::register(
                    self,
                    user,
                    handle,
                    display_name,
                ))
            }
            fn follow(
                &self,
                follower: ::ident::Id,
                target: ::ident::Id,
            ) -> $crate::web::BoxFut<
                '_,
                ::std::result::Result<u64, $crate::contracts::WriteError>,
            > {
                ::std::boxed::Box::pin($crate::contracts::WriteOps::follow(
                    self, follower, target,
                ))
            }
            fn unfollow(
                &self,
                follower: ::ident::Id,
                target: ::ident::Id,
            ) -> $crate::web::BoxFut<
                '_,
                ::std::result::Result<u64, $crate::contracts::WriteError>,
            > {
                ::std::boxed::Box::pin($crate::contracts::WriteOps::unfollow(
                    self, follower, target,
                ))
            }
            fn create_post(
                &self,
                post: ::ident::Id,
                author: ::ident::Id,
                body: ::std::string::String,
            ) -> $crate::web::BoxFut<
                '_,
                ::std::result::Result<u64, $crate::contracts::WriteError>,
            > {
                ::std::boxed::Box::pin($crate::contracts::WriteOps::create_post(
                    self, post, author, body,
                ))
            }
            fn delete_post(
                &self,
                post: ::ident::Id,
                by: ::ident::Id,
            ) -> $crate::web::BoxFut<
                '_,
                ::std::result::Result<u64, $crate::contracts::WriteError>,
            > {
                ::std::boxed::Box::pin($crate::contracts::WriteOps::delete_post(
                    self, post, by,
                ))
            }
            fn like(
                &self,
                post: ::ident::Id,
                user: ::ident::Id,
            ) -> $crate::web::BoxFut<
                '_,
                ::std::result::Result<u64, $crate::contracts::WriteError>,
            > {
                ::std::boxed::Box::pin($crate::contracts::WriteOps::like(
                    self, post, user,
                ))
            }
            fn unlike(
                &self,
                post: ::ident::Id,
                user: ::ident::Id,
            ) -> $crate::web::BoxFut<
                '_,
                ::std::result::Result<u64, $crate::contracts::WriteError>,
            > {
                ::std::boxed::Box::pin($crate::contracts::WriteOps::unlike(
                    self, post, user,
                ))
            }
        }
    };
}

// ===========================================================================
// Directory: the handle <-> id resolver the ReadModels trait does not provide
// ===========================================================================

/// A bidirectional `handle <-> Id` map.
///
/// **Dogfood finding.** [`ReadModels`] answers queries by *handle* and returns
/// [`PostView`]/[`ProfileView`] carrying handles, but exposes **no** way to
/// resolve a handle to the [`Id`] that [`WriteOps`] requires (follow/unfollow
/// take `Id`s, not handles), nor to map the acting-user cookie's `Id` back to a
/// handle for the banner. The web layer therefore keeps this side directory,
/// populated at registration. (The contracts module even ships a
/// `HandleIndex` type for exactly this gap.)
#[derive(Debug, Default)]
pub struct Directory {
    by_handle: HashMap<String, Id>,
    by_id: HashMap<Id, String>,
}

impl Directory {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a `handle <-> id` pair (idempotent).
    pub fn insert(&mut self, id: Id, handle: &str) {
        self.by_handle.insert(handle.to_string(), id);
        self.by_id.insert(id, handle.to_string());
    }

    #[must_use]
    pub fn id_of(&self, handle: &str) -> Option<Id> {
        self.by_handle.get(handle).copied()
    }

    #[must_use]
    pub fn handle_of(&self, id: Id) -> Option<String> {
        self.by_id.get(&id).cloned()
    }
}

// ===========================================================================
// Application state
// ===========================================================================

/// The shared state every handler receives (cheap to clone — all `Arc`s).
#[derive(Clone)]
pub struct AppState {
    pub read: Arc<dyn DynRead>,
    pub write: Arc<dyn DynWrite>,
    pub dir: Arc<RwLock<Directory>>,
}

impl AppState {
    /// Build state from a reader, a writer, and a pre-seeded directory.
    #[must_use]
    pub fn new(
        read: Arc<dyn DynRead>,
        write: Arc<dyn DynWrite>,
        dir: Directory,
    ) -> Self {
        Self { read, write, dir: Arc::new(RwLock::new(dir)) }
    }

    /// The acting user's handle, if the cookie resolves to a known user.
    fn handle_of(&self, id: Id) -> Option<String> {
        self.dir.read().expect("dir lock").handle_of(id)
    }

    fn id_of(&self, handle: &str) -> Option<Id> {
        self.dir.read().expect("dir lock").id_of(handle)
    }
}
