//! The **Follow** relationship aggregate: one follower-followee edge as its
//! own tiny stream.
//!
//! One stream per `(follower, followee)` pair, keyed
//! `follow-<follower>_<followee>` (see [`crate::follow_stream`]). Like
//! [`super::like`], this is the scaling-correct model for a crowd
//! relationship: a user's follow graph is **not** aggregate state on the
//! `User` — it is a fan of independent one-edge aggregates, each an
//! alternating `not-following <-> following` state machine. A hub user with a
//! million followers is a million tiny follow streams, never a
//! million-element `HashSet` folded on every `User` command. Commands stay
//! O(1) forever; follower/following *counts* live in the projection
//! ([`crate::projections`]), computed at query time.
//!
//! # Why the events and commands carry no ids — plain `command`, not `command_as`
//!
//! Both ids that identify this relationship — the follower and the followee —
//! live in the **stream key** `follow-<follower>_<followee>`, so the aggregate
//! never needs them in a command or an event to decide or fold. The only fact
//! `decide` checks is "is this edge currently active?", a pure lookup in this
//! one stream's folded [`bool`]. That is why [`crate::WriteOps`] dispatches
//! these with the plain [`command`](mess_store::EventStore::command), not
//! [`command_as`](mess_store::EventStore::command_as): the pre-refactor
//! `Follow` lived on the *follower's* `user-<id>` stream and echoed a
//! `follower` id separately from the key, so the authored path
//! ([`command_as`](mess_store::EventStore::command_as) + an
//! [`Actor`](mess_core::Actor) impl) had to assert the echoed id matched the
//! stream. Moving the edge onto its own
//! `follow-<follower>_<followee>` stream folds the whole actor identity **into
//! the key**: there is no separately-restated id left to diverge, so there is
//! nothing for `command_as` to check and plain `command` is exactly correct.
//!
//! # Where the self-follow rule lives now
//!
//! Self-follow (`follower == followee`) is **not** a transition of this
//! machine — a `follow-X_X` stream is a malformed key, not a legal edge in an
//! illegal state. Because both ids are in the key and *neither* is visible to
//! `decide`, this aggregate structurally cannot compare them. The rule is
//! therefore enforced once, up front, at the [`WriteOps`](crate::WriteOps)
//! seam (`WriteError::SelfFollow`), before a degenerate stream is ever
//! created — see `crate::contracts`. This keeps [`FollowError`] to the two
//! genuine state-machine rejections.
//!
//! # Follow-of-a-nonexistent-user is reconciled downstream (accept-and-reconcile)
//!
//! As before (see [`super::user`]'s module docs), `decide` cannot verify the
//! followee is a registered user — that fact is on another stream. The edge is
//! accepted and the projection reconciles: a follow of a user who never
//! registered contributes nothing to any timeline (that user has no posts) and
//! a dangling follower count is harmless.

use mess_core::Decide;
use mess_derive::{Aggregate, Event};

// ---------------------------------------------------------------------------
// Events: the wire vocabulary for one follow edge's stream.
// ---------------------------------------------------------------------------

/// Every fact that can happen to one follower-followee edge.
///
/// `#[event(name = "follow", version = 1)]` → wire names `"follow.followed"` /
/// `"follow.unfollowed"`. Both are payload-free: the `(follower, followee)`
/// pair is carried by the stream key `follow-<follower>_<followee>`.
#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "follow", version = 1)]
pub enum FollowEvent {
    /// The follower started following the followee.
    Followed,
    /// The follower stopped following the followee.
    Unfollowed,
}

// ---------------------------------------------------------------------------
// The aggregate: one follow edge's folded state.
// ---------------------------------------------------------------------------

/// The read-model folded from one follow edge's stream: a single alternating
/// bit. `Default` (`following: false`) is the never-followed edge, which is
/// also the state of a stream that has never been written.
#[derive(Debug, Default, Clone, PartialEq, Eq, Aggregate)]
#[aggregate(event = FollowEvent)]
pub struct Follow {
    /// `true` while the most recent event is [`FollowEvent::Followed`].
    pub following: bool,
}

impl Follow {
    /// Fold one event into state. Infallible by construction (see
    /// `examples/bank`).
    pub fn apply(&mut self, event: &FollowEvent) {
        match event {
            FollowEvent::Followed => self.following = true,
            FollowEvent::Unfollowed => self.following = false,
        }
    }
}

// ---------------------------------------------------------------------------
// Commands, the typed rejection, and one `Decide` impl per command.
// ---------------------------------------------------------------------------

/// Every way a command against [`Follow`] can be refused. The two rejections
/// that used to live on `User` (`AlreadyFollowing` / `NotFollowing`) moved
/// here with the follow relationship. `SelfFollow` did **not** — it is a
/// key-well-formedness check enforced at the [`WriteOps`](crate::WriteOps)
/// seam (see the module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FollowError {
    /// [`PlaceFollow`] on an edge that is already active.
    AlreadyFollowing,
    /// [`RemoveFollow`] on an edge that is not currently active.
    NotFollowing,
}

impl std::fmt::Display for FollowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FollowError::AlreadyFollowing => {
                write!(f, "already following that user")
            }
            FollowError::NotFollowing => write!(f, "not following that user"),
        }
    }
}

impl std::error::Error for FollowError {}

/// Start following, on the edge this stream is keyed to. Payload-free: the
/// `(follower, followee)` pair is the stream key.
#[derive(Debug, Clone, Copy)]
pub struct PlaceFollow;

/// Stop following, on the edge this stream is keyed to.
#[derive(Debug, Clone, Copy)]
pub struct RemoveFollow;

impl Decide<PlaceFollow> for Follow {
    type Rejection = FollowError;

    fn decide(
        &self,
        _cmd: PlaceFollow,
    ) -> Result<Vec<FollowEvent>, FollowError> {
        if self.following {
            return Err(FollowError::AlreadyFollowing);
        }
        Ok(vec![FollowEvent::Followed])
    }
}

impl Decide<RemoveFollow> for Follow {
    type Rejection = FollowError;

    fn decide(
        &self,
        _cmd: RemoveFollow,
    ) -> Result<Vec<FollowEvent>, FollowError> {
        if !self.following {
            return Err(FollowError::NotFollowing);
        }
        Ok(vec![FollowEvent::Unfollowed])
    }
}
