
//! Social-feed domain: users, posts, a follow graph, and likes — the mess v1
//! API's second showcase example after `examples/bank`.
//!
//! Where `examples/bank` teaches the vocabulary on one aggregate, this crate
//! is the *shape of a real application*: two aggregates that reference each
//! other by id without sharing a stream, plus a [`contracts`] seam that lets
//! the read-model and HTTP bones build against a stable interface instead of
//! against each other. Read `examples/bank/src/lib.rs` first for the
//! walkthrough of `#[derive(Event)]` / `#[derive(Aggregate)]` / `Decide`.
//!
//! # The tour, in the order a newcomer meets it
//!
//! 1. [`domain::user`] — the [`User`](domain::user::User) aggregate: register,
//!    rename, and a **follow set** that lives on the *follower's own stream*.
//!    That module's docs explain why the edge set lives there and why
//!    `decide` deliberately *cannot* check that a follow target exists.
//! 2. [`domain::post`] — the [`Post`](domain::post::Post) aggregate: create,
//!    delete (author-only), like, unlike. Its docs record two decisions:
//!    moderation events were absorbed into `Deleted { by }`, and **self-like
//!    is allowed** (unlike self-follow).
//! 3. [`contracts`] — the DTOs ([`PostView`](contracts::PostView),
//!    [`ProfileView`](contracts::ProfileView),
//!    [`TimelinePage`](contracts::TimelinePage)), the [`ReadModels`] query
//!    trait with its [`wait_for`](contracts::ReadModels::wait_for)
//!    read-your-writes barrier, the [`WriteOps`] wrapper over
//!    [`EventStore`](mess_store::EventStore), and a deterministic
//!    [`FakeReadModels`] for frontend tests.
//!
//! # Streams
//!
//! Each aggregate is one stream *family*. A user lives at `user-<id>`, a post
//! at `post-<id>`; [`user_stream`] and [`post_stream`] are the single source
//! of that convention, used by both [`WriteOps`] and the examples.
//!
//! `examples/social.rs` runs the whole thing end-to-end against a real
//! [`EventStore`](mess_store::EventStore); `tests/gwt.rs` exercises every
//! accept and every rejection store-free through `mess-testkit`.

use ident::Id;

pub mod contracts;
pub mod domain;
pub mod projections;
pub mod seed;
pub mod store_backend;
pub mod web;

// Re-export the domain surface at the crate root so call sites read
// `social::RegisterUser` rather than `social::domain::user::RegisterUser`,
// matching `examples/bank`'s flat surface.
pub use domain::post::{
    BODY_MAX_LEN, CreatePost, DeletePost, Like, Post, PostError, PostEvent,
    Unlike,
};
pub use domain::user::{
    Follow, HANDLE_MAX_LEN, RegisterUser, SetDisplayName, Unfollow, User,
    UserError, UserEvent, handle_is_valid,
};

pub use contracts::{
    FakeReadModels, PostView, ProfileView, ReadModels, TimelinePage,
    WriteError, WriteOps,
};

pub use projections::{PostLookup, Projections};

/// The stream id for a user's aggregate: `user-<id>`.
///
/// The one place this convention is written down; [`WriteOps`] and the
/// examples both call it so a rename is a single edit.
#[must_use]
pub fn user_stream(id: Id) -> String {
    format!("user-{id}")
}

/// The stream id for a post's aggregate: `post-<id>`.
#[must_use]
pub fn post_stream(id: Id) -> String {
    format!("post-{id}")
}
