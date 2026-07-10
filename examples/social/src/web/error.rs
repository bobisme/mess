//! Mapping typed domain rejections to friendly, user-facing flash messages.
//!
//! Every [`WriteError`] variant — the four domain rejection enums
//! ([`UserError`]/[`PostError`]/[`LikeError`]/[`FollowError`]), the seam-level
//! self-follow check, plus the infrastructure outcomes — is turned into a
//! short sentence a visitor can act on. **No rejection ever becomes a 500**: a
//! refused command is a normal, expected outcome of a business rule, so it
//! round-trips as a flash banner on the redirected page.
//!
//! [`UserError`]: crate::domain::user::UserError
//! [`PostError`]: crate::domain::post::PostError
//! [`LikeError`]: crate::domain::like::LikeError
//! [`FollowError`]: crate::domain::follow::FollowError

use crate::contracts::WriteError;
use crate::domain::follow::FollowError;
use crate::domain::like::LikeError;
use crate::domain::post::PostError;
use crate::domain::user::UserError;

/// Render a [`WriteError`] as a friendly, imperative sentence for the flash
/// banner.
///
/// # Exhausted-retry behavior
///
/// [`WriteError::Conflict`] means the store's bounded optimistic-retry budget
/// (default 64 attempts, jittered) was exhausted under sustained contention on
/// one stream. The handler does **not** retry further or 500; it tells the user
/// the action was not applied and to try again — the write is safely a no-op
/// (nothing was appended), so a re-submit is correct and idempotent-at-the-UI.
#[must_use]
pub fn friendly(err: &WriteError) -> String {
    match err {
        WriteError::User(u) => friendly_user(u),
        WriteError::Post(p) => friendly_post(p),
        WriteError::Like(l) => friendly_like(l),
        WriteError::Follow(fo) => friendly_follow(fo),
        WriteError::SelfFollow => "You cannot follow yourself.".to_string(),
        WriteError::Conflict { .. } => "That action hit heavy contention and \
                                        was not applied. Please try again."
            .to_string(),
        WriteError::Store(_) => {
            "Something went wrong saving that. Please try again.".to_string()
        }
    }
}

fn friendly_user(err: &UserError) -> String {
    match err {
        UserError::AlreadyRegistered => {
            "That handle is already taken — pick another.".to_string()
        }
        UserError::NotRegistered => "That user does not exist yet.".to_string(),
        UserError::InvalidHandle { .. } => "Handles must be 1-30 characters \
                                            of lowercase a-z, 0-9, or _."
            .to_string(),
    }
}

fn friendly_post(err: &PostError) -> String {
    match err {
        PostError::AlreadyCreated => "That post already exists.".to_string(),
        PostError::NotCreated => "That post does not exist.".to_string(),
        PostError::EmptyBody => "Your post cannot be empty.".to_string(),
        PostError::BodyTooLong { max, .. } => {
            format!("Your post is too long (max {max} characters).")
        }
        PostError::NotAuthor => {
            "You can only delete your own posts.".to_string()
        }
        PostError::AlreadyDeleted => {
            "That post was already deleted.".to_string()
        }
    }
}

fn friendly_like(err: &LikeError) -> String {
    match err {
        LikeError::AlreadyLiked => "You already liked that post.".to_string(),
        LikeError::NotLiked => "You have not liked that post.".to_string(),
    }
}

fn friendly_follow(err: &FollowError) -> String {
    match err {
        FollowError::AlreadyFollowing => {
            "You already follow that user.".to_string()
        }
        FollowError::NotFollowing => "You do not follow that user.".to_string(),
    }
}
