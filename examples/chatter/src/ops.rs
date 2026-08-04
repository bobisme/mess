//! The **write seam**: one method per user action, each mapping to exactly one
//! warm-path [`EventStore::command_cached`] call on the right stream.
//!
//! Same design as `examples/social`'s `contracts::WriteOps`, minus the DTO /
//! read-trait half (chatter has no web layer, so the read side is queried
//! directly off [`crate::Projections`]):
//!
//! - The store call is generic in three type parameters and returns a
//!   `CommandError<R, S>` whose parameters leak the backend type into every
//!   caller's signature. [`WriteOps`] pins the stream-naming conventions in one
//!   place and collapses the error to the backend-agnostic [`WriteError`].
//! - Every method returns the write's **global log position**, the token a
//!   caller feeds to [`Projections::wait_for`](crate::Projections::wait_for) to
//!   read its own write.
//! - `command_cached` (not `command`): both aggregates are `Snapshottable +
//!   Clone` with bounded state, so the hot-aggregate write-through cache
//!   applies. It matters far more here than in social — a channel stream is
//!   thousands of events deep, so a cache miss is a thousands-event replay
//!   while a hit reads zero events.

use std::future::Future;

use mess_core::{CommandError, SharedStoreError};
use mess_store::{EventStore, SnapshotStore};

use crate::Id;
use crate::domain::channel::{
    AddReaction, ArchiveChannel, Channel, ChannelError, CreateChannel,
    PostMessage,
};
use crate::domain::user::{RegisterUser, SetDisplayName, User, UserError};
use crate::{channel_stream, user_stream};

/// The failure of a [`WriteOps`] call, flattening the two aggregates' typed
/// rejections plus the store-side outcomes into one enum.
///
/// `Store` holds [`mess_core::SharedStoreError`] (via
/// [`CommandError::erase_store_shared`]) rather than a boxed error so the type
/// stays `Clone + PartialEq + Eq` — handy for tests — while `source()` still
/// walks into the backend's own error chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteError {
    /// A [`User`] business rule refused the command.
    User(UserError),
    /// A [`Channel`] business rule refused the command.
    Channel(ChannelError),
    /// The optimistic-retry budget was exhausted.
    Conflict { stream: String, attempts: u32 },
    /// The storage backend failed.
    Store(SharedStoreError),
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::User(e) => write!(f, "{e}"),
            WriteError::Channel(e) => write!(f, "{e}"),
            WriteError::Conflict { stream, attempts } => write!(
                f,
                "write conflict on {stream:?} after {attempts} attempts"
            ),
            WriteError::Store(e) => write!(f, "store error: {e}"),
        }
    }
}

impl std::error::Error for WriteError {}

/// Map a [`User`] command's [`CommandError`] into a [`WriteError`].
fn user_err<S>(e: CommandError<UserError, S>) -> WriteError
where
    S: std::error::Error + Send + Sync + 'static,
{
    match e.erase_store_shared() {
        CommandError::Domain(d) => WriteError::User(d),
        CommandError::Conflict { stream, attempts } => {
            WriteError::Conflict { stream, attempts }
        }
        CommandError::Store(s) => WriteError::Store(s),
    }
}

/// Map a [`Channel`] command's [`CommandError`] into a [`WriteError`].
fn channel_err<S>(e: CommandError<ChannelError, S>) -> WriteError
where
    S: std::error::Error + Send + Sync + 'static,
{
    match e.erase_store_shared() {
        CommandError::Domain(d) => WriteError::Channel(d),
        CommandError::Conflict { stream, attempts } => {
            WriteError::Conflict { stream, attempts }
        }
        CommandError::Store(s) => WriteError::Store(s),
    }
}

/// The write surface: one method per action. Every method returns the write's
/// global log position.
///
/// `-> impl Future<..> + Send` rather than bare `async fn` so a caller generic
/// over `W: WriteOps` can rely on the future being `Send` without a hand-rolled
/// boxed mirror.
pub trait WriteOps {
    fn register(
        &self,
        user: Id,
        handle: String,
        display_name: String,
    ) -> impl Future<Output = Result<u64, WriteError>> + Send;

    fn set_display_name(
        &self,
        user: Id,
        display_name: String,
    ) -> impl Future<Output = Result<u64, WriteError>> + Send;

    fn create_channel(
        &self,
        channel: Id,
        slug: String,
        topic: String,
    ) -> impl Future<Output = Result<u64, WriteError>> + Send;

    fn post_message(
        &self,
        channel: Id,
        author: Id,
        body: String,
    ) -> impl Future<Output = Result<u64, WriteError>> + Send;

    fn add_reaction(
        &self,
        channel: Id,
        target: u64,
        by: Id,
        emoji: String,
    ) -> impl Future<Output = Result<u64, WriteError>> + Send;

    fn archive_channel(
        &self,
        channel: Id,
    ) -> impl Future<Output = Result<u64, WriteError>> + Send;
}

impl<B: SnapshotStore> WriteOps for EventStore<B>
where
    B::Error: std::fmt::Display,
{
    async fn register(
        &self,
        user: Id,
        handle: String,
        display_name: String,
    ) -> Result<u64, WriteError> {
        let commit = self
            .command_cached::<User, _>(
                &user_stream(user),
                RegisterUser { handle, display_name },
            )
            .await
            .map_err(user_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }

    async fn set_display_name(
        &self,
        user: Id,
        display_name: String,
    ) -> Result<u64, WriteError> {
        let commit = self
            .command_cached::<User, _>(
                &user_stream(user),
                SetDisplayName { display_name },
            )
            .await
            .map_err(user_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }

    async fn create_channel(
        &self,
        channel: Id,
        slug: String,
        topic: String,
    ) -> Result<u64, WriteError> {
        let commit = self
            .command_cached::<Channel, _>(
                &channel_stream(channel),
                CreateChannel { slug, topic },
            )
            .await
            .map_err(channel_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }

    async fn post_message(
        &self,
        channel: Id,
        author: Id,
        body: String,
    ) -> Result<u64, WriteError> {
        let commit = self
            .command_cached::<Channel, _>(
                &channel_stream(channel),
                PostMessage { author, body },
            )
            .await
            .map_err(channel_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }

    async fn add_reaction(
        &self,
        channel: Id,
        target: u64,
        by: Id,
        emoji: String,
    ) -> Result<u64, WriteError> {
        let commit = self
            .command_cached::<Channel, _>(
                &channel_stream(channel),
                AddReaction { target, by, emoji },
            )
            .await
            .map_err(channel_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }

    async fn archive_channel(&self, channel: Id) -> Result<u64, WriteError> {
        let commit = self
            .command_cached::<Channel, _>(
                &channel_stream(channel),
                ArchiveChannel,
            )
            .await
            .map_err(channel_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }
}
