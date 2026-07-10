//! The write side: the four aggregates and their commands.
//!
//! Each submodule is one aggregate — one stream family, one folded state, one
//! typed rejection, and one `Decide` impl per command. Nothing here knows
//! about storage, HTTP, or read models; that separation is the whole design.
//!
//! Two of the four are *entity* aggregates and two are *relationship*
//! aggregates — the split that makes every aggregate's state **bounded**, the
//! prerequisite for scaling to millions of likes/followers:
//!
//! - [`user`] — the [`User`](user::User) entity: registration and display name.
//!   Bounded.
//! - [`post`] — the [`Post`](post::Post) entity: content, author, delete
//!   tombstone. Bounded.
//! - [`like`] — the [`Like`](like::Like) relationship: one post-user like edge,
//!   an alternating `not-liked <-> liked` machine on its own tiny stream. O(1)
//!   forever.
//! - [`follow`] — the [`Follow`](follow::Follow) relationship: one
//!   follower-followee edge, an alternating `not-following <-> following`
//!   machine on its own tiny stream. O(1) forever.
//!
//! The *crowds* (who likes a post, who follows a user) are therefore **not**
//! aggregate state anywhere — they are reconstructed as counts/sets in the
//! projection ([`crate::projections`]) at query time. See [`user`]'s module
//! docs for the accept-and-reconcile posture this shape rests on.

pub mod follow;
pub mod like;
pub mod post;
pub mod user;

/// The tiny length-prefixed byte codec the four aggregates share for their
/// [`Snapshottable`](mess_store::Snapshottable) state blobs. Crate-internal —
/// the on-wire snapshot shape is an implementation detail, not public API.
pub(crate) mod snapshot_codec;
