//! The **Post** aggregate: one post's lifecycle — created and deleted.
//!
//! One stream per post, keyed `post-<id>` (see [`crate::post_stream`]). As in
//! [`super::user`], every rule is enforced in [`Decide::decide`] against a
//! single stream's folded state.
//!
//! # Bounded state: the like *crowd* is not stored here
//!
//! An earlier version of this crate folded a `likes: HashSet<Id>` of every
//! liker into the post's own state, with `Liked` / `Unliked` events on this
//! stream. That is **unbounded** aggregate state: a viral post with a million
//! likes meant a million-element set replayed on every `Post` command and
//! written into every snapshot. This rework moves each like to its own tiny
//! [`Like`](super::like::Like) relationship stream (`like-<post>_<user>`), so
//! `Post` keeps only **bounded** state — content, author, delete tombstone —
//! and its commands stay O(1) regardless of how many likes the post accrues.
//! The `AlreadyLiked` / `NotLiked` rejections and the like-on-deleted rule
//! moved with the relationship; see [`super::like`].
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
//!   own `Hidden { by, reason }` event without disturbing this stream's delete
//!   rule.
//! - `Deleted { by }` records *who* deleted, so a future moderator-delete is a
//!   one-line extension (widen the `DeletePost` author check) rather than a new
//!   event.

use mess_core::Decide;
use mess_derive::{Aggregate, Event};
use mess_store::{Snapshottable, StableSnapshotId, StateCodecError};

use crate::Id;
use crate::domain::snapshot_codec;
use crate::domain::snapshot_codec::{Reader, put_bool, put_opt_id, put_str};

// ---------------------------------------------------------------------------
// Events: the wire vocabulary for one post's stream.
// ---------------------------------------------------------------------------

/// Every fact that can happen to a post.
///
/// `#[event(name = "post", version = 1)]` → wire names `"post.posted"` and
/// `"post.deleted"`. (Likes are no longer post events — they live on the
/// `like-<post>_<user>` streams; see the module docs.)
#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "post", version = 1)]
pub enum PostEvent {
    /// The post was created by `author` with `body`.
    Posted { author: Id, body: String },
    /// The post was deleted `by` a user (only ever the author — see
    /// [`DeletePost`]).
    Deleted { by: Id },
}

// ---------------------------------------------------------------------------
// The aggregate: one post's folded state.
// ---------------------------------------------------------------------------

/// The read-model folded from one post's event stream — now fully **bounded**:
/// three scalar-ish fields, no crowd set. `author` is `Option<Id>` so
/// `Default` (an unwritten post) is representable — `created` is the explicit
/// existence flag commands check.
#[derive(Debug, Default, Clone, PartialEq, Eq, Aggregate)]
#[aggregate(event = PostEvent)]
pub struct Post {
    /// `false` until a `Posted` event is folded.
    pub created: bool,
    /// `true` once a `Deleted` event is folded. A deleted post is a tombstone:
    /// it still exists in the log, but the projection drops it from every
    /// feed.
    pub deleted: bool,
    /// The author, set by `Posted`. `None` before creation.
    pub author:  Option<Id>,
    /// The post body.
    pub body:    String,
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
        }
    }
}

/// A [`Post`] snapshot is two existence bits, an optional author id, and the
/// length-capped body — bounded now that the like *crowd* lives on its own
/// [`Like`](super::like::Like) relationship streams (`bn-jes`). The pre-remodel
/// aggregate would have serialized an O(likers) `HashSet` into every snapshot;
/// this one never does. (The retired unbounded shape is measured directly by
/// the `hot-post` benchmark in `tests/hot_post_bench.rs`.)
///
/// **`FOLD_VERSION` bump rule.** Bump whenever [`Post::apply`] semantics or the
/// `encode_state` / `decode_state` byte shape change such that an old blob
/// would misrepresent the state (a newly-folded [`PostEvent`] variant, a
/// new/removed field, a reordered blob). A bump invalidates older snapshots
/// (rebuilt by full replay, §9); a pure refactor preserving both does not bump.
impl Snapshottable for Post {
    const AGGREGATE_SCHEMA_ID: StableSnapshotId =
        StableSnapshotId::new("social.post");
    // The state codec is shared with the other three aggregates, so it is
    // named and versioned separately from this fold.
    const CODEC_ID: StableSnapshotId = snapshot_codec::CODEC_ID;
    const CODEC_VERSION: u32 = snapshot_codec::CODEC_VERSION;
    const FOLD_VERSION: u32 = 1;

    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
        let mut out = Vec::new();
        put_bool(&mut out, self.created);
        put_bool(&mut out, self.deleted);
        put_opt_id(&mut out, self.author);
        put_str(&mut out, &self.body);
        Ok(out)
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
        let mut r = Reader::new(bytes);
        let created = r.read_bool()?;
        let deleted = r.read_bool()?;
        let author = r.read_opt_id()?;
        let body = r.read_str()?;
        r.finish()?;
        Ok(Post { created, deleted, author, body })
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

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    #[test]
    fn state_round_trips_through_bytes() {
        let author = Id::new();
        let states = [
            Post::default(),
            Post {
                created: true,
                deleted: false,
                author:  Some(author),
                body:    "hello world".into(),
            },
            Post {
                created: true,
                deleted: true,
                author:  Some(author),
                body:    String::new(),
            },
        ];
        for state in states {
            let bytes = state.encode_state().unwrap();
            assert_eq!(Post::decode_state(&bytes).unwrap(), state);
        }
    }

    #[test]
    fn malformed_blob_errors_not_panics() {
        // created+deleted bits, an author-present tag, then nothing.
        assert!(Post::decode_state(&[1, 0, 1]).is_err());
        assert!(Post::decode_state(&[]).is_err());
    }
}
