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
use mess_store::{Snapshottable, StateCodecError};

use crate::domain::snapshot_codec::Reader;

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

/// A [`Like`] edge's snapshot is a single byte — the alternating bit. This is
/// the payoff of the bounded relationship remodel (`bn-jes`): what used to be
/// an O(likers) `HashSet` folded onto the post is now one bit per edge, so the
/// warm-write path ([`command_cached`](mess_store::EventStore::command_cached))
/// and the snapshot-accelerated cold load are both trivially cheap here.
///
/// **`FOLD_VERSION` bump rule.** Bump it whenever [`Like::apply`] semantics
/// change in a way an old snapshot blob would misrepresent — a new
/// [`LikeEvent`] variant that `apply` now folds, or any change to the
/// `encode_state` / `decode_state` byte shape below. A bump invalidates every
/// older snapshot, which `load_cached` then rebuilds by full replay (§9). A
/// pure refactor that leaves the folded state and blob shape identical does
/// **not** bump.
impl Snapshottable for Like {
    const FOLD_VERSION: u32 = 1;

    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
        Ok(vec![u8::from(self.liked)])
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
        let mut r = Reader::new(bytes);
        let liked = r.read_bool()?;
        r.finish()?;
        Ok(Like { liked })
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

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    #[test]
    fn state_round_trips_through_bytes() {
        for liked in [false, true] {
            let state = Like { liked };
            let bytes = state.encode_state().unwrap();
            assert_eq!(Like::decode_state(&bytes).unwrap(), state);
        }
    }

    #[test]
    fn a_liked_edge_snapshots_to_one_byte() {
        assert_eq!(Like { liked: true }.encode_state().unwrap().len(), 1);
    }

    #[test]
    fn malformed_blob_errors_not_panics() {
        // Empty blob: nothing to read the bool from.
        assert!(Like::decode_state(&[]).is_err());
        // Trailing byte past the single-bit state.
        assert!(Like::decode_state(&[1, 0]).is_err());
    }
}
