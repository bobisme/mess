//! The **Post** aggregate: one post's lifecycle — created, liked, deleted.
//!
//! One stream per post, keyed `post-<id>` (see [`crate::post_stream`]). As in
//! [`super::user`], every rule is enforced in [`Decide::decide`] against a
//! single stream's folded state.
//!
//! # Decision: the moderation events were absorbed, not kept
//!
//! An earlier version of this crate modeled visibility with
//! `HiddenByPoster` / `HiddenByModerator` events and a `PostStatus` enum, to
//! demonstrate an authorization rule (only the poster may hide their own
//! post). This rework **drops** those in favor of a single `Deleted { by }`
//! event, because:
//!
//! - The authorization lesson is preserved — better, even: `DeletePost`
//!   enforces *only the author may delete*, the same "typed rejection carrying
//!   a business rule" teaching point, without a second actor role.
//! - **Moderation is a distinct bounded context.** A moderator hiding content
//!   is about roles, appeals, and audit trails that belong in a dedicated
//!   moderation bone, not smuggled into the core post aggregate. Keeping both
//!   here would blur the teaching example. When moderation lands it can add its
//!   own `Hidden { by, reason }` event without disturbing this stream's
//!   like/delete rules.
//! - `Deleted { by }` records *who* deleted, so a future moderator-delete is a
//!   one-line extension (widen the `DeletePost` author check) rather than a new
//!   event.
//!
//! # Decision: self-like is ALLOWED
//!
//! `Like` does **not** reject the author liking their own post. Contrast with
//! `Follow`, which forbids self-follow. The difference is semantic: a follow
//! edge to yourself is meaningless in every timeline/graph query (you always
//! "see your own posts"), so it is pure noise. A like is a *reaction count*;
//! real platforms (X, Instagram) let authors like their own posts and it is a
//! legitimate, countable signal. Forbidding it would be a surprising rule with
//! no invariant behind it. (Documented per bn-154's "self-like allowed?
//! DECIDE" instruction.)

use std::collections::HashSet;

use ident::Id;
use mess_core::Decide;
use mess_derive::{Aggregate, Event};

// ---------------------------------------------------------------------------
// Events: the wire vocabulary for one post's stream.
// ---------------------------------------------------------------------------

/// Every fact that can happen to a post.
///
/// `#[event(name = "post", version = 1)]` → wire names `"post.posted"`,
/// `"post.deleted"`, `"post.liked"`, `"post.unliked"`.
#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "post", version = 1)]
pub enum PostEvent {
    /// The post was created by `author` with `body`.
    Posted { author: Id, body: String },
    /// The post was deleted `by` a user (only ever the author — see
    /// [`DeletePost`]).
    Deleted { by: Id },
    /// `user` liked the post.
    Liked { user: Id },
    /// `user` retracted their like.
    Unliked { user: Id },
}

// ---------------------------------------------------------------------------
// The aggregate: one post's folded state.
// ---------------------------------------------------------------------------

/// The read-model folded from one post's event stream.
///
/// Like [`super::user::User`], `likes` is a [`HashSet`] (an order-independent
/// set; `ident::Id` is not `Ord`, so a `BTreeSet` is not an option — see that
/// type's docs). `author` is `Option<Id>` so `Default` (an unwritten post) is
/// representable — `created` is the explicit existence flag commands check.
#[derive(Debug, Default, Clone, PartialEq, Eq, Aggregate)]
#[aggregate(event = PostEvent)]
pub struct Post {
    /// `false` until a `Posted` event is folded.
    pub created: bool,
    /// `true` once a `Deleted` event is folded. A deleted post is a tombstone:
    /// it still exists in the log, but new likes are refused.
    pub deleted: bool,
    /// The author, set by `Posted`. `None` before creation.
    pub author:  Option<Id>,
    /// The post body.
    pub body:    String,
    /// The set of users who currently like this post.
    pub likes:   HashSet<Id>,
}

impl Post {
    /// Fold one event into state. Infallible by construction (see
    /// `examples/bank`).
    pub fn apply(&mut self, event: &PostEvent) {
        match event {
            PostEvent::Posted { author, body } => {
                self.created = true;
                self.author = Some(*author);
                body.clone_into(&mut self.body);
            }
            PostEvent::Deleted { .. } => {
                self.deleted = true;
            }
            PostEvent::Liked { user } => {
                self.likes.insert(*user);
            }
            PostEvent::Unliked { user } => {
                self.likes.remove(user);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Body validation.
// ---------------------------------------------------------------------------

/// The maximum length, in characters, of a post body.
pub const BODY_MAX_LEN: usize = 500;

// ---------------------------------------------------------------------------
// Commands, the typed rejection, and one `Decide` impl per command.
// ---------------------------------------------------------------------------

/// Every way a command against [`Post`] can be refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostError {
    /// `CreatePost` on a stream that already holds a post.
    AlreadyCreated,
    /// Any command other than `CreatePost` on a stream with no post yet.
    NotCreated,
    /// `CreatePost` with a body of zero characters.
    EmptyBody,
    /// `CreatePost` with a body longer than [`BODY_MAX_LEN`] characters.
    BodyTooLong { len: usize, max: usize },
    /// `DeletePost` by anyone other than the author.
    NotAuthor,
    /// `DeletePost` on an already-deleted post.
    AlreadyDeleted,
    /// `Like` on a deleted post.
    LikeOnDeleted,
    /// `Like` by a user who already likes the post.
    AlreadyLiked,
    /// `Unlike` by a user who does not currently like the post.
    NotLiked,
}

impl std::fmt::Display for PostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PostError::AlreadyCreated => write!(f, "post already exists"),
            PostError::NotCreated => write!(f, "post does not exist yet"),
            PostError::EmptyBody => write!(f, "post body must not be empty"),
            PostError::BodyTooLong { len, max } => {
                write!(f, "post body is {len} characters; the maximum is {max}")
            }
            PostError::NotAuthor => {
                write!(f, "only the author may delete this post")
            }
            PostError::AlreadyDeleted => write!(f, "post is already deleted"),
            PostError::LikeOnDeleted => {
                write!(f, "cannot like a deleted post")
            }
            PostError::AlreadyLiked => write!(f, "already liked this post"),
            PostError::NotLiked => write!(f, "have not liked this post"),
        }
    }
}

impl std::error::Error for PostError {}

/// Create (publish) a new post.
#[derive(Debug, Clone)]
pub struct CreatePost {
    pub author: Id,
    pub body:   String,
}

/// Delete a post. `by` is the actor requesting the delete; the aggregate
/// rejects anyone but the author.
#[derive(Debug, Clone, Copy)]
pub struct DeletePost {
    pub by: Id,
}

/// Like a post as `user`. Self-like (author liking their own post) is allowed
/// — see the module docs.
#[derive(Debug, Clone, Copy)]
pub struct Like {
    pub user: Id,
}

/// Retract a like as `user`.
#[derive(Debug, Clone, Copy)]
pub struct Unlike {
    pub user: Id,
}

impl Decide<CreatePost> for Post {
    type Rejection = PostError;

    fn decide(&self, cmd: CreatePost) -> Result<Vec<PostEvent>, PostError> {
        if self.created {
            return Err(PostError::AlreadyCreated);
        }
        // Count `char`s, not bytes: a 500-emoji post is 500 characters, not
        // 2000 bytes. The limit is a user-facing length, so it is measured in
        // the unit the user sees.
        let len = cmd.body.chars().count();
        if len == 0 {
            return Err(PostError::EmptyBody);
        }
        if len > BODY_MAX_LEN {
            return Err(PostError::BodyTooLong { len, max: BODY_MAX_LEN });
        }
        Ok(vec![PostEvent::Posted { author: cmd.author, body: cmd.body }])
    }
}

impl Decide<DeletePost> for Post {
    type Rejection = PostError;

    fn decide(&self, cmd: DeletePost) -> Result<Vec<PostEvent>, PostError> {
        match self.author {
            None => return Err(PostError::NotCreated),
            Some(author) if author != cmd.by => {
                return Err(PostError::NotAuthor);
            }
            Some(_) => {}
        }
        if self.deleted {
            return Err(PostError::AlreadyDeleted);
        }
        Ok(vec![PostEvent::Deleted { by: cmd.by }])
    }
}

impl Decide<Like> for Post {
    type Rejection = PostError;

    fn decide(&self, cmd: Like) -> Result<Vec<PostEvent>, PostError> {
        if !self.created {
            return Err(PostError::NotCreated);
        }
        if self.deleted {
            return Err(PostError::LikeOnDeleted);
        }
        if self.likes.contains(&cmd.user) {
            return Err(PostError::AlreadyLiked);
        }
        Ok(vec![PostEvent::Liked { user: cmd.user }])
    }
}

impl Decide<Unlike> for Post {
    type Rejection = PostError;

    fn decide(&self, cmd: Unlike) -> Result<Vec<PostEvent>, PostError> {
        if !self.created {
            return Err(PostError::NotCreated);
        }
        // Note: we do NOT reject unliking a *deleted* post. Only `Like` is
        // blocked on deletion (bn-154 lists "no like-on-deleted" but not its
        // mirror). Retracting a like you placed before deletion is harmless
        // bookkeeping, and gating it would add a rejection reason no rule asks
        // for. The membership check below is the only gate.
        if !self.likes.contains(&cmd.user) {
            return Err(PostError::NotLiked);
        }
        Ok(vec![PostEvent::Unliked { user: cmd.user }])
    }
}
