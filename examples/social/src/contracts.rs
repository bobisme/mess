//! The **contract seam**: the read/write surface the HTTP layer and the
//! read-model projections agree on, so those two bones can be built in
//! parallel against this module instead of against each other.
//!
//! Three things live here:
//!
//! 1. **DTOs** ([`PostView`], [`ProfileView`], [`TimelinePage`]) — the shapes a
//!    frontend renders. They are *not* aggregates: an aggregate
//!    ([`crate::domain::post::Post`]) is the write-side fold of one stream; a
//!    [`PostView`] is a denormalized, viewer-relative projection joining a post
//!    with its author's profile and the viewer's own like state.
//! 2. **[`ReadModels`]** — the query trait the frontend calls. Real impls read
//!    from projections built by tailing the event log; the [`FakeReadModels`]
//!    here is a deterministic in-memory stand-in for frontend tests.
//! 3. **[`WriteOps`]** — a thin wrapper mapping each HTTP write action to one
//!    [`EventStore::command`] call, returning the global log position of the
//!    write so a caller can pass it to [`ReadModels::wait_for`] for
//!    read-your-writes.

use std::future::Future;

use ident::Id;
use mess_core::CommandError;
use mess_store::{Backend, EventStore};

use crate::domain::post::{
    CreatePost, DeletePost, Like, Post, PostError, Unlike,
};
use crate::domain::user::{
    Follow, RegisterUser, SetDisplayName, Unfollow, User, UserError,
};
use crate::{post_stream, user_stream};

// ===========================================================================
// DTOs
// ===========================================================================

/// One post as rendered in a feed, denormalized and viewer-relative.
///
/// `created_seq` is the post's position in the global log (a monotonic
/// sequence), used both as a stable sort key for feeds and as the pagination
/// cursor (see [`TimelinePage`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostView {
    /// The post's id (also its stream id, `post-<id>`, less the prefix).
    pub id:             Id,
    /// The author's id, joined from their profile. Compare this — not
    /// [`author_handle`](Self::author_handle) — for author-only UI logic
    /// (e.g. "show the delete button"): an id compare cannot be fooled by two
    /// users momentarily sharing a display string, and needs no string
    /// allocation.
    pub author_id:      Id,
    /// The author's handle, joined from their profile.
    pub author_handle:  String,
    /// The author's current display name, joined from their profile.
    pub author_display: String,
    /// The post body.
    pub body:           String,
    /// Total number of likes.
    pub likes:          u64,
    /// Whether the requesting viewer likes this post. `false` for an
    /// anonymous viewer.
    pub liked_by_me:    bool,
    /// The post's global-log sequence — stable feed sort key and cursor.
    pub created_seq:    u64,
}

/// A user's profile, viewer-relative.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileView {
    pub handle:          String,
    pub display_name:    String,
    /// Number of (non-deleted) posts this user has authored.
    pub post_count:      u64,
    /// Number of users who follow this user.
    pub follower_count:  u64,
    /// Number of users this user follows.
    pub following_count: u64,
    /// Whether the requesting viewer follows this user. `false` for anonymous.
    pub followed_by_me:  bool,
}

/// One page of a feed, plus an opaque cursor for the next page.
///
/// `next_cursor` is `None` when the page is the last one. A caller pages by
/// feeding the returned cursor back into the same query; the cursor's format
/// is an implementation detail of the [`ReadModels`] impl and must be treated
/// as opaque.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelinePage {
    pub entries:     Vec<PostView>,
    pub next_cursor: Option<String>,
}

// ===========================================================================
// ReadModels
// ===========================================================================

/// The frontend's query surface.
///
/// **Why `-> impl Future<..> + Send` and not bare `async fn`.** Real
/// implementations read from projection stores (a key-value store, a SQL read
/// replica) — I/O that must not block the async runtime, hence async. But a
/// bare `async fn` in a trait carries no `Send` bound on its returned future,
/// so a caller generic over `R: ReadModels` (an axum handler built once and
/// reused for any backend) cannot prove the future it `.await`s is `Send` —
/// and axum's [`Handler`](https://docs.rs/axum/latest/axum/handler/trait.Handler.html)
/// requires exactly that. Spelling the bound on the trait itself (return
/// position `impl Trait` in traits, RPITIT) fixes this at the seam instead of
/// downstream: every implementation below is a plain `async fn` (the desugared
/// future is checked against the bound at the `impl` site, same as any other
/// trait method), and a generic caller gets to rely on `Send` without a
/// hand-rolled `Box::pin` mirror. See `web::AppState`'s docs for the shim this
/// removed.
pub trait ReadModels {
    /// Posts from the users `user` follows (and their own), newest first.
    fn home_timeline(
        &self,
        user: Id,
        cursor: Option<String>,
        limit: usize,
    ) -> impl Future<Output = TimelinePage> + Send;

    /// Posts authored by `handle`, newest first.
    fn user_posts(
        &self,
        handle: &str,
        cursor: Option<String>,
        limit: usize,
    ) -> impl Future<Output = TimelinePage> + Send;

    /// Every post in the system, newest first — the global firehose.
    fn firehose(
        &self,
        cursor: Option<String>,
        limit: usize,
    ) -> impl Future<Output = TimelinePage> + Send;

    /// The profile for `handle`, viewer-relative to `viewer` (for
    /// `followed_by_me`). `None` if no such user.
    fn profile(
        &self,
        handle: &str,
        viewer: Option<Id>,
    ) -> impl Future<Output = Option<ProfileView>> + Send;

    /// One post by id, viewer-relative to `viewer` (for `liked_by_me`). `None`
    /// if no such post (or it was deleted).
    fn post(
        &self,
        id: Id,
        viewer: Option<Id>,
    ) -> impl Future<Output = Option<PostView>> + Send;

    /// Resolve a handle to the id of the user currently holding it, or `None`
    /// if no such user is registered.
    ///
    /// This is the trait's answer to the "handle resolution" gap: a frontend
    /// routes and links by handle (readable URLs, `/u/alice`) but every
    /// [`WriteOps`] call and every id-comparison in a [`PostView`] needs an
    /// [`Id`]. Before this method existed the web layer kept its own
    /// `handle -> Id` side directory, bulk-populated at boot from a
    /// `Projections::directory` bulk-export method that returned every
    /// registered user — a second, hand-maintained copy of data the read
    /// model already has. `resolve` is the one query that closes that gap
    /// without widening every DTO to carry a redundant id, which is why that
    /// bulk-export method is gone too; see `web::AppState`'s docs for how the
    /// acting-user cookie uses `resolve` instead of a directory.
    fn resolve(&self, handle: &str) -> impl Future<Output = Option<Id>> + Send;

    /// **Read-your-writes barrier.** Block until this read model has processed
    /// the event log up to at least global `position`. A caller does
    /// `let seq = write_ops.something(..).await?; reads.wait_for(seq).await;`
    /// and is then guaranteed the following query reflects that write.
    ///
    /// The [`FakeReadModels`] is synchronously consistent (a write mutates it
    /// in place) so this is a no-op there; a real projection-backed impl
    /// awaits its tailer catching up to `position`.
    fn wait_for(&self, position: u64) -> impl Future<Output = ()> + Send;
}

// ===========================================================================
// WriteOps
// ===========================================================================

/// The failure of a [`WriteOps`] call, flattening the two aggregates' typed
/// rejections plus the store-side outcomes into one enum the HTTP layer can
/// map to status codes.
///
/// This is the one place the two domain error types
/// ([`UserError`]/[`PostError`]) and the infrastructure outcomes
/// ([`CommandError::Conflict`]/[`CommandError::Store`]) are unified. The store
/// error is rendered to a `String` rather than carried generically so
/// `WriteError` is not itself generic over the backend — HTTP handlers stay
/// backend-agnostic. (Dogfood note: `CommandError<R, S>` is precise but its
/// two type parameters ripple outward; collapsing them at this seam is the
/// pragmatic trade.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteError {
    /// A [`User`] business rule refused the command.
    User(UserError),
    /// A [`Post`] business rule refused the command.
    Post(PostError),
    /// The optimistic-retry budget was exhausted.
    Conflict { stream: String, attempts: u32 },
    /// The storage backend failed (rendered).
    Store(String),
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::User(e) => write!(f, "{e}"),
            WriteError::Post(e) => write!(f, "{e}"),
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
fn user_err<S: std::fmt::Display>(e: CommandError<UserError, S>) -> WriteError {
    match e {
        CommandError::Domain(d) => WriteError::User(d),
        CommandError::Conflict { stream, attempts } => {
            WriteError::Conflict { stream, attempts }
        }
        CommandError::Store(s) => WriteError::Store(s.to_string()),
    }
}

/// Map a [`Post`] command's [`CommandError`] into a [`WriteError`].
fn post_err<S: std::fmt::Display>(e: CommandError<PostError, S>) -> WriteError {
    match e {
        CommandError::Domain(d) => WriteError::Post(d),
        CommandError::Conflict { stream, attempts } => {
            WriteError::Conflict { stream, attempts }
        }
        CommandError::Store(s) => WriteError::Store(s.to_string()),
    }
}

/// The write surface the HTTP layer targets: one method per user action, each
/// mapping to exactly one [`EventStore::command`] call on the right stream.
///
/// **Why a wrapper and not "HTTP calls `store.command` directly".** The store
/// call is generic in three type parameters (aggregate, command, backend) and
/// returns a `CommandError<R, S>` whose two parameters leak the backend type
/// into every handler signature. [`WriteOps`] pins the stream-naming
/// convention (`user-<id>`, `post-<id>`) in one place and collapses the error
/// to the backend-agnostic [`WriteError`], so an HTTP handler is a
/// one-liner with a monomorphic signature. Every method returns the write's
/// **global log position**, the token a caller feeds to
/// [`ReadModels::wait_for`] to read its own write.
///
/// Like [`ReadModels`], every method is `-> impl Future<..> + Send` rather
/// than bare `async fn`, for the same reason: it lets a caller generic over
/// `W: WriteOps` (an axum handler) satisfy axum's `Send`-future requirement
/// without a hand-rolled boxed mirror trait.
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

    fn follow(
        &self,
        follower: Id,
        target: Id,
    ) -> impl Future<Output = Result<u64, WriteError>> + Send;

    fn unfollow(
        &self,
        follower: Id,
        target: Id,
    ) -> impl Future<Output = Result<u64, WriteError>> + Send;

    fn create_post(
        &self,
        post: Id,
        author: Id,
        body: String,
    ) -> impl Future<Output = Result<u64, WriteError>> + Send;

    fn delete_post(
        &self,
        post: Id,
        by: Id,
    ) -> impl Future<Output = Result<u64, WriteError>> + Send;

    fn like(
        &self,
        post: Id,
        user: Id,
    ) -> impl Future<Output = Result<u64, WriteError>> + Send;

    fn unlike(
        &self,
        post: Id,
        user: Id,
    ) -> impl Future<Output = Result<u64, WriteError>> + Send;
}

impl<B: Backend> WriteOps for EventStore<B>
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
            .command::<User, _>(
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
            .command::<User, _>(
                &user_stream(user),
                SetDisplayName { display_name },
            )
            .await
            .map_err(user_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }

    async fn follow(
        &self,
        follower: Id,
        target: Id,
    ) -> Result<u64, WriteError> {
        let commit = self
            .command::<User, _>(
                &user_stream(follower),
                Follow { follower, target },
            )
            .await
            .map_err(user_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }

    async fn unfollow(
        &self,
        follower: Id,
        target: Id,
    ) -> Result<u64, WriteError> {
        let commit = self
            .command::<User, _>(&user_stream(follower), Unfollow { target })
            .await
            .map_err(user_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }

    async fn create_post(
        &self,
        post: Id,
        author: Id,
        body: String,
    ) -> Result<u64, WriteError> {
        let commit = self
            .command::<Post, _>(&post_stream(post), CreatePost { author, body })
            .await
            .map_err(post_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }

    async fn delete_post(&self, post: Id, by: Id) -> Result<u64, WriteError> {
        let commit = self
            .command::<Post, _>(&post_stream(post), DeletePost { by })
            .await
            .map_err(post_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }

    async fn like(&self, post: Id, user: Id) -> Result<u64, WriteError> {
        let commit = self
            .command::<Post, _>(&post_stream(post), Like { user })
            .await
            .map_err(post_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }

    async fn unlike(&self, post: Id, user: Id) -> Result<u64, WriteError> {
        let commit = self
            .command::<Post, _>(&post_stream(post), Unlike { user })
            .await
            .map_err(post_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }
}

// ===========================================================================
// FakeReadModels — deterministic in-memory implementation for frontend tests
// ===========================================================================

/// A user row in the fake, mirroring the folded [`User`] aggregate.
#[derive(Debug, Clone)]
struct FakeUser {
    id:           Id,
    handle:       String,
    display_name: String,
    /// Ids this user follows.
    following:    Vec<Id>,
}

/// A post row in the fake, mirroring the folded [`Post`] aggregate plus its
/// global sequence.
#[derive(Debug, Clone)]
struct FakePost {
    id:      Id,
    author:  Id,
    body:    String,
    likes:   Vec<Id>,
    deleted: bool,
    seq:     u64,
}

/// A deterministic, synchronously-consistent [`ReadModels`] for frontend
/// tests.
///
/// Seed it with [`with_user`](FakeReadModels::with_user) /
/// [`with_post`](FakeReadModels::with_post) / [`with_follow`] / [`with_like`];
/// every query then returns a stable, sorted result with no I/O and no clock.
/// Feeds are ordered by `seq` **descending** (newest first) and paginated with
/// a cursor that is the stringified `seq` to page *before*.
#[derive(Debug, Clone, Default)]
pub struct FakeReadModels {
    users:    Vec<FakeUser>,
    posts:    Vec<FakePost>,
    /// Monotonic sequence handed to the next seeded post.
    next_seq: u64,
}

impl FakeReadModels {
    /// A new, empty fake.
    #[must_use]
    pub fn new() -> Self { Self::default() }

    /// Seed a registered user. Chainable.
    #[must_use]
    pub fn with_user(
        mut self,
        id: Id,
        handle: &str,
        display_name: &str,
    ) -> Self {
        self.users.push(FakeUser {
            id,
            handle: handle.to_string(),
            display_name: display_name.to_string(),
            following: Vec::new(),
        });
        self
    }

    /// Seed a follow edge `follower -> target`. Chainable.
    #[must_use]
    pub fn with_follow(mut self, follower: Id, target: Id) -> Self {
        if let Some(u) = self.users.iter_mut().find(|u| u.id == follower)
            && !u.following.contains(&target)
        {
            u.following.push(target);
        }
        self
    }

    /// Seed a post authored by `author`, identified by `id` — the same
    /// [`Id`] a [`PostView`] reports. Assigns the next global sequence.
    /// Chainable.
    #[must_use]
    pub fn with_post(mut self, id: Id, author: Id, body: &str) -> Self {
        self.next_seq += 1;
        self.posts.push(FakePost {
            id,
            author,
            body: body.to_string(),
            likes: Vec::new(),
            deleted: false,
            seq: self.next_seq,
        });
        self
    }

    /// Seed a like by `user` on post `id`. Chainable.
    #[must_use]
    pub fn with_like(mut self, id: Id, user: Id) -> Self {
        if let Some(p) = self.posts.iter_mut().find(|p| p.id == id)
            && !p.likes.contains(&user)
        {
            p.likes.push(user);
        }
        self
    }

    /// Mark post `id` deleted. Chainable.
    #[must_use]
    pub fn with_deleted(mut self, id: Id) -> Self {
        if let Some(p) = self.posts.iter_mut().find(|p| p.id == id) {
            p.deleted = true;
        }
        self
    }

    fn user_by_id(&self, id: Id) -> Option<&FakeUser> {
        self.users.iter().find(|u| u.id == id)
    }

    fn user_by_handle(&self, handle: &str) -> Option<&FakeUser> {
        self.users.iter().find(|u| u.handle == handle)
    }

    /// Render one post row into a viewer-relative [`PostView`].
    fn view_of(&self, p: &FakePost, viewer: Option<Id>) -> PostView {
        let (handle, display) = self
            .user_by_id(p.author)
            .map(|u| (u.handle.clone(), u.display_name.clone()))
            .unwrap_or_default();
        PostView {
            id:             p.id,
            author_id:      p.author,
            author_handle:  handle,
            author_display: display,
            body:           p.body.clone(),
            likes:          p.likes.len() as u64,
            liked_by_me:    viewer.is_some_and(|v| p.likes.contains(&v)),
            created_seq:    p.seq,
        }
    }

    /// Build one page from a pre-filtered post set, newest-first, applying the
    /// `cursor`/`limit`. Shared by all three feed queries so pagination is
    /// defined exactly once.
    fn page(
        &self,
        mut posts: Vec<&FakePost>,
        viewer: Option<Id>,
        cursor: Option<String>,
        limit: usize,
    ) -> TimelinePage {
        // Newest first.
        posts.sort_by_key(|p| std::cmp::Reverse(p.seq));
        // A cursor is the seq to page strictly *before*.
        if let Some(before) = cursor.and_then(|c| c.parse::<u64>().ok()) {
            posts.retain(|p| p.seq < before);
        }
        let has_more = posts.len() > limit;
        let entries: Vec<PostView> = posts
            .into_iter()
            .take(limit)
            .map(|p| self.view_of(p, viewer))
            .collect();
        let next_cursor = if has_more {
            entries.last().map(|e| e.created_seq.to_string())
        } else {
            None
        };
        TimelinePage { entries, next_cursor }
    }
}

impl ReadModels for FakeReadModels {
    async fn home_timeline(
        &self,
        user: Id,
        cursor: Option<String>,
        limit: usize,
    ) -> TimelinePage {
        // Authors whose posts belong in `user`'s home feed: everyone they
        // follow, plus themselves.
        let mut authors: Vec<Id> = self
            .user_by_id(user)
            .map(|u| u.following.clone())
            .unwrap_or_default();
        authors.push(user);
        let posts: Vec<&FakePost> = self
            .posts
            .iter()
            .filter(|p| !p.deleted && authors.contains(&p.author))
            .collect();
        self.page(posts, Some(user), cursor, limit)
    }

    async fn user_posts(
        &self,
        handle: &str,
        cursor: Option<String>,
        limit: usize,
    ) -> TimelinePage {
        let author = self.user_by_handle(handle).map(|u| u.id);
        let posts: Vec<&FakePost> = self
            .posts
            .iter()
            .filter(|p| !p.deleted && Some(p.author) == author)
            .collect();
        self.page(posts, None, cursor, limit)
    }

    async fn firehose(
        &self,
        cursor: Option<String>,
        limit: usize,
    ) -> TimelinePage {
        let posts: Vec<&FakePost> =
            self.posts.iter().filter(|p| !p.deleted).collect();
        self.page(posts, None, cursor, limit)
    }

    async fn profile(
        &self,
        handle: &str,
        viewer: Option<Id>,
    ) -> Option<ProfileView> {
        let u = self.user_by_handle(handle)?;
        let post_count = self
            .posts
            .iter()
            .filter(|p| !p.deleted && p.author == u.id)
            .count() as u64;
        let follower_count =
            self.users.iter().filter(|o| o.following.contains(&u.id)).count()
                as u64;
        let followed_by_me = viewer.is_some_and(|v| {
            self.user_by_id(v).is_some_and(|vu| vu.following.contains(&u.id))
        });
        Some(ProfileView {
            handle: u.handle.clone(),
            display_name: u.display_name.clone(),
            post_count,
            follower_count,
            following_count: u.following.len() as u64,
            followed_by_me,
        })
    }

    async fn post(&self, id: Id, viewer: Option<Id>) -> Option<PostView> {
        self.posts
            .iter()
            .find(|p| p.id == id && !p.deleted)
            .map(|p| self.view_of(p, viewer))
    }

    async fn resolve(&self, handle: &str) -> Option<Id> {
        self.user_by_handle(handle).map(|u| u.id)
    }

    async fn wait_for(&self, _position: u64) {
        // Synchronously consistent: seeding mutates in place, so any write is
        // already visible. A projection-backed impl would await its tailer.
    }
}
