//! Social-feed domain: posts and moderation, on the mess v1 API.
//!
//! `examples/social` (this crate) is the second showcase example after
//! `examples/bank`: see that crate's `src/lib.rs` for a first walkthrough
//! of `#[derive(Event)]` / `#[derive(Aggregate)]` / `Decide`; this one adds
//! authorization logic (only the original poster may hide their own post)
//! to show a typed rejection carrying more than one business rule.
//!
//! `examples/social.rs` runs the domain end-to-end against a
//! [`mess_store::EventStore`]; `tests/gwt.rs` exercises it store-free
//! through `mess-testkit`.
//!
//! This crate previously ran on an earlier, now-retired prototype's
//! game-engine-flavored storage vocabulary. It is ported here onto
//! `mess-core`'s event-sourcing vocabulary per
//! `notes/mess-research/09_implementation_plan.md` Phase 0: what that
//! prototype called an object's identity is just the stream id below, its
//! per-object record type is the `Aggregate` (`Post`), and its storage
//! facade is the `EventStore`.

use ident::Id;
use mess_core::Decide;
use mess_derive::{Aggregate, Event};

// ---------------------------------------------------------------------------
// Events: the wire vocabulary for one post's stream.
// ---------------------------------------------------------------------------

/// Every fact that can happen to a post.
///
/// `#[event(name = "post", version = 1)]` gives `Posted` the wire name
/// `"post.posted"`, and so on — see `examples/bank`'s `AccountEvent` for
/// the full explanation of what `#[derive(Event)]` generates.
#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "post", version = 1)]
pub enum PostEvent {
    Posted { poster_id: Id, body: String },
    HiddenByPoster,
    HiddenByModerator,
}

// ---------------------------------------------------------------------------
// The aggregate: one post's folded state.
// ---------------------------------------------------------------------------

/// Whether (and why) a post is currently visible.
///
/// `#[default]` marks the state a post starts in before any event has been
/// applied — required because `Post` below derives `Default`, and every
/// field of a `#[derive(Default)]` struct must itself implement `Default`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum PostStatus {
    #[default]
    Unpublished,
    Visible,
    HiddenByPoster,
    HiddenByModerator,
}

/// The read-model folded from one post's event stream.
///
/// In the v1 API a "stream id" is just the string key passed to
/// `EventStore::load` / `command` / `append` — there is no separate
/// identity type standing between a caller and that key; it is simply
/// the stream id a caller chooses (see `examples/social.rs`).
#[derive(Debug, Default, Clone, PartialEq, Eq, Aggregate)]
#[aggregate(event = PostEvent)]
pub struct Post {
    pub poster_id: Option<Id>,
    pub body: String,
    pub status: PostStatus,
}

impl Post {
    /// Fold one event into state. Infallible by construction — see
    /// `examples/bank`'s `Account::apply` for why `apply` never rejects.
    pub fn apply(&mut self, event: &PostEvent) {
        match event {
            PostEvent::Posted { poster_id, body } => {
                self.poster_id = Some(*poster_id);
                body.clone_into(&mut self.body);
                self.status = PostStatus::Visible;
            }
            PostEvent::HiddenByPoster => {
                self.status = PostStatus::HiddenByPoster;
            }
            PostEvent::HiddenByModerator => {
                self.status = PostStatus::HiddenByModerator;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Commands, the typed rejection, and one `Decide` impl per command.
// ---------------------------------------------------------------------------

/// Every way a command against [`Post`] can be refused — see
/// `examples/bank`'s `AccountError` for what this typed `Decide::Rejection`
/// buys callers over a stringly error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostError {
    AlreadyPosted,
    NotPosted,
    AlreadyHidden,
    /// Authorization failure: only the original poster may hide their own
    /// post via [`HideByPoster`] — anyone else must go through
    /// [`HideByModerator`].
    NotYourPost,
}

impl std::fmt::Display for PostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PostError::AlreadyPosted => write!(f, "post already exists"),
            PostError::NotPosted => write!(f, "post does not exist yet"),
            PostError::AlreadyHidden => write!(f, "post is already hidden"),
            PostError::NotYourPost => {
                write!(f, "only the original poster may hide their own post")
            }
        }
    }
}

impl std::error::Error for PostError {}

/// Publish a new post. Commands are plain structs — see `examples/bank`.
#[derive(Debug, Clone)]
pub struct Publish {
    pub poster_id: Id,
    pub body: String,
}

/// The poster hides their own post.
#[derive(Debug, Clone, Copy)]
pub struct HideByPoster {
    pub requester: Id,
}

/// A moderator hides someone else's post; no ownership check.
#[derive(Debug, Clone, Copy)]
pub struct HideByModerator;

impl Decide<Publish> for Post {
    type Rejection = PostError;

    fn decide(&self, cmd: Publish) -> Result<Vec<PostEvent>, PostError> {
        if self.poster_id.is_some() {
            return Err(PostError::AlreadyPosted);
        }
        Ok(vec![PostEvent::Posted {
            poster_id: cmd.poster_id,
            body: cmd.body,
        }])
    }
}

impl Decide<HideByPoster> for Post {
    type Rejection = PostError;

    fn decide(&self, cmd: HideByPoster) -> Result<Vec<PostEvent>, PostError> {
        match self.poster_id {
            None => return Err(PostError::NotPosted),
            Some(poster_id) if poster_id != cmd.requester => {
                return Err(PostError::NotYourPost);
            }
            Some(_) => {}
        }
        if self.status != PostStatus::Visible {
            return Err(PostError::AlreadyHidden);
        }
        Ok(vec![PostEvent::HiddenByPoster])
    }
}

impl Decide<HideByModerator> for Post {
    type Rejection = PostError;

    fn decide(&self, _cmd: HideByModerator) -> Result<Vec<PostEvent>, PostError> {
        if self.poster_id.is_none() {
            return Err(PostError::NotPosted);
        }
        if self.status != PostStatus::Visible {
            return Err(PostError::AlreadyHidden);
        }
        Ok(vec![PostEvent::HiddenByModerator])
    }
}
