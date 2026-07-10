//! The **Like** relationship aggregate: one post-user like edge as its own
//! tiny stream.
//!
//! One stream per `(post, user)` pair, keyed `like-<post>_<user>` (see
//! [`crate::like_stream`]). This is the scaling-correct event-sourcing model
//! for a crowd relationship: the *set of likers* is **not** aggregate state on
//! the post — it is a fan of independent one-edge aggregates, each an
//! alternating `not-liked <-> liked` state machine. A post with a million
//! likes is a million tiny like streams, never a million-element `HashSet`
//! folded on every `Post` command. Commands stay O(1) forever; the crowd count
//! lives in the projection ([`crate::projections`]), computed at query time.
//!
//! # Why the events and commands carry no ids
//!
//! Both ids that identify this relationship — the post and the liker — live in
//! the **stream key** `like-<post>_<user>`, so the aggregate never needs them
//! in a command or an event to decide or fold. The only fact `decide` checks
//! is "is this edge currently liked?", a pure lookup in this one stream's
//! folded [`bool`]. That is also why [`crate::WriteOps`] dispatches these with
//! the plain [`command`](mess_store::EventStore::command), not
//! [`command_as`](mess_store::EventStore::command_as): there is no *restated*
//! actor id that could diverge from the stream it is written to (contrast the
//! pre-refactor `Follow`-on-the-user-stream, which echoed a `follower` id the
//! authored path had to validate). Here the key *is* the full identity, so
//! there is nothing for `command_as` to check.
//!
//! # Self-like is ALLOWED (accept-and-reconcile)
//!
//! An author liking their own post is a legitimate, countable signal on real
//! platforms, so [`PlaceLike`] has no self-check — nor could it, since the
//! aggregate cannot see who authored the post (that fact is on the `post-<id>`
//! stream, which `decide` may not load).
//!
//! # Like-on-a-deleted-post is reconciled downstream, not refused here
//!
//! The pre-refactor `Post` aggregate rejected `Like` on a deleted post
//! (`LikeOnDeleted`) because the like lived on the post's own stream, so the
//! delete tombstone was right there in folded state. A relationship aggregate
//! **cannot** see the post's stream — the same single-stream boundary that
//! makes it scale. Per the documented accept-and-reconcile posture (see
//! [`crate::domain::user`]), the write is accepted unconditionally and the
//! **projection reconciles**: a deleted post drops out of every feed and
//! single-post query, so any likes recorded against it simply never surface.
//! The like edge is harmless bookkeeping on a stream nobody reads.

use mess_core::Decide;
use mess_derive::{Aggregate, Event};

// ---------------------------------------------------------------------------
// Events: the wire vocabulary for one like edge's stream.
// ---------------------------------------------------------------------------

/// Every fact that can happen to one post-user like edge.
///
/// `#[event(name = "like", version = 1)]` → wire names `"like.liked"` /
/// `"like.unliked"`. Both are payload-free: the `(post, user)` pair is carried
/// by the stream key `like-<post>_<user>`, never restated in the event.
#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "like", version = 1)]
pub enum LikeEvent {
    /// The user liked the post.
    Liked,
    /// The user retracted their like.
    Unliked,
}

// ---------------------------------------------------------------------------
// The aggregate: one like edge's folded state.
// ---------------------------------------------------------------------------

/// The read-model folded from one like edge's stream: a single alternating
/// bit. `Default` (`liked: false`) is the never-liked edge, which is also the
/// state of a stream that has never been written.
#[derive(Debug, Default, Clone, PartialEq, Eq, Aggregate)]
#[aggregate(event = LikeEvent)]
pub struct Like {
    /// `true` while the most recent event is [`LikeEvent::Liked`].
    pub liked: bool,
}

impl Like {
    /// Fold one event into state. Infallible by construction (see
    /// `examples/bank`).
    pub fn apply(&mut self, event: &LikeEvent) {
        match event {
            LikeEvent::Liked => self.liked = true,
            LikeEvent::Unliked => self.liked = false,
        }
    }
}

// ---------------------------------------------------------------------------
// Commands, the typed rejection, and one `Decide` impl per command.
// ---------------------------------------------------------------------------

/// Every way a command against [`Like`] can be refused. The two rejections
/// that used to live on `Post` (`AlreadyLiked` / `NotLiked`) moved here with
/// the like relationship.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LikeError {
    /// [`PlaceLike`] on an edge that is already liked.
    AlreadyLiked,
    /// [`RemoveLike`] on an edge that is not currently liked.
    NotLiked,
}

impl std::fmt::Display for LikeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LikeError::AlreadyLiked => write!(f, "already liked this post"),
            LikeError::NotLiked => write!(f, "have not liked this post"),
        }
    }
}

impl std::error::Error for LikeError {}

/// Like the post this stream is keyed to. Payload-free: the `(post, user)`
/// pair is the stream key.
#[derive(Debug, Clone, Copy)]
pub struct PlaceLike;

/// Retract the like on the post this stream is keyed to.
#[derive(Debug, Clone, Copy)]
pub struct RemoveLike;

impl Decide<PlaceLike> for Like {
    type Rejection = LikeError;

    fn decide(&self, _cmd: PlaceLike) -> Result<Vec<LikeEvent>, LikeError> {
        if self.liked {
            return Err(LikeError::AlreadyLiked);
        }
        Ok(vec![LikeEvent::Liked])
    }
}

impl Decide<RemoveLike> for Like {
    type Rejection = LikeError;

    fn decide(&self, _cmd: RemoveLike) -> Result<Vec<LikeEvent>, LikeError> {
        if !self.liked {
            return Err(LikeError::NotLiked);
        }
        Ok(vec![LikeEvent::Unliked])
    }
}
