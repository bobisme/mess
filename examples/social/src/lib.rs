//! Social-feed domain: users, posts, a follow graph, and likes — the mess v1
//! API's second showcase example after `examples/bank`.
//!
//! Where `examples/bank` teaches the vocabulary on one aggregate, this crate
//! is the *shape of a real application at scale*: **every aggregate has
//! bounded state**, plus a [`contracts`] seam that lets the read-model and
//! HTTP bones build against a stable interface instead of against each other.
//! Read `examples/bank/src/lib.rs` first for the walkthrough of
//! `#[derive(Event)]` / `#[derive(Aggregate)]` / `Decide`.
//!
//! # Why relationship streams (bounded state)
//!
//! A crowd — the likers of a post, the followers of a user — is **not**
//! aggregate state here. Each relationship is its own tiny aggregate: a
//! [`Like`](domain::like::Like) per `(post, user)` and a
//! [`Follow`](domain::follow::Follow) per `(follower, followee)`, each an
//! alternating two-state machine on its own stream. So a `Post` command is
//! O(1) whether the post has one like or ten million, and a snapshot of any
//! aggregate is a handful of fields — never an O(crowd) set. The crowd only
//! reappears as *counts* and *membership sets* in the projection, rebuilt from
//! the relationship streams at query time.
//!
//! # The tour, in the order a newcomer meets it
//!
//! 1. [`domain::user`] — the [`User`](domain::user::User) entity: register and
//!    rename. Bounded. Its docs carry the crate's canonical **accept-and-
//!    reconcile** posture (why `decide` deliberately *cannot* check a follow
//!    target exists).
//! 2. [`domain::post`] — the [`Post`](domain::post::Post) entity: create and
//!    delete (author-only). Bounded. Its docs record that moderation events
//!    were absorbed into `Deleted { by }`.
//! 3. [`domain::like`] / [`domain::follow`] — the two **relationship**
//!    aggregates, each a tiny alternating machine on a `(id, id)`-keyed stream.
//!    Self-like is allowed; self-follow is refused at the [`WriteOps`] seam.
//! 4. [`contracts`] — the DTOs ([`PostView`](contracts::PostView),
//!    [`ProfileView`](contracts::ProfileView),
//!    [`TimelinePage`](contracts::TimelinePage)), the [`ReadModels`] query
//!    trait with its [`wait_for`](contracts::ReadModels::wait_for)
//!    read-your-writes barrier, the [`WriteOps`] wrapper over
//!    [`EventStore`](mess_store::EventStore), and a deterministic
//!    [`FakeReadModels`] for frontend tests.
//!
//! # Streams
//!
//! Each aggregate is one stream *family*, routed by
//! [`StoredRecord::category`](mess_store::StoredRecord::category) (the segment
//! before the first `-`):
//!
//! - `user-<id>` — one [`User`]; [`user_stream`].
//! - `post-<id>` — one [`Post`]; [`post_stream`].
//! - `like-<post>_<user>` — one [`Like`]; [`like_stream`].
//! - `follow-<follower>_<followee>` — one [`Follow`]; [`follow_stream`].
//!
//! These helpers are the single source of each convention, used by both
//! [`WriteOps`] and the examples. See [`like_stream`] for the two-id suffix
//! format invariant (the `_` separator and why it is unambiguous).
//!
//! `examples/social.rs` runs the whole thing end-to-end against a real
//! [`EventStore`](mess_store::EventStore); `tests/gwt.rs` exercises every
//! accept and every rejection store-free through `mess-testkit`.

pub mod contracts;
pub mod domain;
pub mod id;
pub mod projections;
pub mod rebuild;
pub mod seed;
pub mod store_backend;
pub mod web;

// Re-export the domain surface at the crate root so call sites read
// `social::RegisterUser` rather than `social::domain::user::RegisterUser`,
// matching `examples/bank`'s flat surface.
pub use contracts::{
    FakeReadModels, PostView, ProfileView, ReadModels, TimelinePage,
    WriteError, WriteOps,
};
pub use domain::follow::{
    Follow, FollowError, FollowEvent, PlaceFollow, RemoveFollow,
};
pub use domain::like::{Like, LikeError, LikeEvent, PlaceLike, RemoveLike};
pub use domain::post::{
    BODY_MAX_LEN, CreatePost, DeletePost, Post, PostError, PostEvent,
};
pub use domain::user::{
    HANDLE_MAX_LEN, RegisterUser, SetDisplayName, User, UserError, UserEvent,
    handle_is_valid,
};
pub use id::{Id, IdParseError};
pub use projections::{
    Cardinalities, PROJECTION_VERSION, PostLookup, Projections,
};

/// The separator between the two [`Id`]s inside a relationship stream's suffix
/// (e.g. the `_` in `like-<post>_<user>`).
///
/// # Format invariant
///
/// An [`Id`] renders (via its `Display`/`FromStr`) as a fixed 26-character
/// string over the alphabet `[0-9a-hjkmnp-tv-z]` — lowercase Crockford
/// base32 digits, **no separators of any kind** (see [`id`] for the full
/// codec). It therefore contains **neither `-` nor `_`**, which is what
/// makes `_` an unambiguous — and simpler than before — pair separator:
///
/// - [`StoredRecord::category`](mess_store::StoredRecord::category) splits the
///   stream id at the *first* `-`, yielding category `"like"`/`"follow"` and a
///   suffix `<id1>_<id2>`. Since neither id can contain `-` at all (not just
///   "no leading `-`"), that split is trivially safe — there is no internal `-`
///   an id could contribute to confuse it.
/// - Splitting that suffix once on `_` recovers exactly the two ids, because
///   neither id can contain a `_` either. A `-` separator would still work fine
///   here too (ids have none), but `_` is kept for continuity with the
///   pre-bn-gt5 stream names.
pub const PAIR_SEP: char = '_';

/// The stream id for a user's aggregate: `user-<id>`.
///
/// The one place this convention is written down; [`WriteOps`] and the
/// examples both call it so a rename is a single edit.
#[must_use]
pub fn user_stream(id: Id) -> String { format!("user-{id}") }

/// The stream id for a post's aggregate: `post-<id>`.
#[must_use]
pub fn post_stream(id: Id) -> String { format!("post-{id}") }

/// The stream id for a like relationship: `like-<post>_<user>`.
///
/// The `_` joins the two ids unambiguously — see [`PAIR_SEP`] for the format
/// invariant. Parse it back with [`parse_pair`].
#[must_use]
pub fn like_stream(post: Id, user: Id) -> String {
    format!("like-{post}{PAIR_SEP}{user}")
}

/// The stream id for a follow relationship: `follow-<follower>_<followee>`.
///
/// The `_` joins the two ids unambiguously — see [`PAIR_SEP`] for the format
/// invariant. Parse it back with [`parse_pair`].
#[must_use]
pub fn follow_stream(follower: Id, followee: Id) -> String {
    format!("follow-{follower}{PAIR_SEP}{followee}")
}

/// Recover the two [`Id`]s from a relationship stream's *suffix* (the part
/// after the category, e.g. what
/// [`StoredRecord::stream_suffix`](mess_store::StoredRecord::stream_suffix)
/// returns for a `like-`/`follow-` stream).
///
/// Returns `None` if the suffix does not split into exactly two parseable ids
/// on the single [`PAIR_SEP`] — the projection treats that as an unroutable
/// stream. See [`PAIR_SEP`] for why one `_` split is unambiguous.
#[must_use]
pub fn parse_pair(suffix: &str) -> Option<(Id, Id)> {
    let (a, b) = suffix.split_once(PAIR_SEP)?;
    Some((a.parse().ok()?, b.parse().ok()?))
}
