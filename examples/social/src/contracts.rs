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
//!    [`EventStore::command_cached`] (warm-path) call, returning the global log
//!    position of the write so a caller can pass it to [`ReadModels::wait_for`]
//!    for read-your-writes.

use std::future::Future;

use ident::Id;
use mess_core::CommandError;
use mess_store::{EventStore, SnapshotStore};

use crate::domain::follow::{Follow, FollowError, PlaceFollow, RemoveFollow};
use crate::domain::like::{Like, LikeError, PlaceLike, RemoveLike};
use crate::domain::post::{CreatePost, DeletePost, Post, PostError};
use crate::domain::user::{RegisterUser, SetDisplayName, User, UserError};
use crate::{follow_stream, like_stream, post_stream, user_stream};

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
/// This is the one place the four domain error types
/// ([`UserError`]/[`PostError`]/[`LikeError`]/[`FollowError`]), the seam-level
/// [`SelfFollow`](WriteError::SelfFollow) check, and the infrastructure
/// outcomes ([`CommandError::Conflict`]/[`CommandError::Store`]) are unified.
///
/// # Dogfood note: why `Store` is a rendered `String`, not `CommandError::erase_store`
///
/// `mess_core::CommandError::erase_store` (bn-188) exists precisely to keep a
/// seam like this one from stringifying the store error — it would let `Store`
/// hold a `BoxedStoreError` that still round-trips `source()`. We deliberately
/// **do not** adopt it here: this `WriteError` must stay `Clone + PartialEq +
/// Eq` because the web layer's test double (`web::tests::FakeWriteOps`) hands
/// back a *cloned* canned `Result<u64, WriteError>` on every call, and several
/// handler tests compare `WriteError` values. `BoxedStoreError`
/// (`Box<dyn Error>`) is none of `Clone`/`PartialEq`/`Eq`, so erasing the
/// store type would forfeit all three derives and break those test doubles.
/// Rendering to a `String` keeps the seam backend-agnostic *and* comparable;
/// the lost `source()` chain is an acceptable trade for an example crate whose
/// store errors are never programmatically inspected. (Reported as a limitation
/// of `erase_store` for equality-carrying seams.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteError {
    /// A [`User`] business rule refused the command.
    User(UserError),
    /// A [`Post`] business rule refused the command.
    Post(PostError),
    /// A [`Like`] relationship business rule refused the command.
    Like(LikeError),
    /// A [`Follow`] relationship business rule refused the command.
    Follow(FollowError),
    /// A follow whose follower and followee are the same user. Enforced at
    /// this seam, before a degenerate `follow-X_X` stream is created — see
    /// [`crate::domain::follow`] for why this well-formedness check lives here
    /// rather than in the relationship aggregate's `decide`.
    SelfFollow,
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
            WriteError::Like(e) => write!(f, "{e}"),
            WriteError::Follow(e) => write!(f, "{e}"),
            WriteError::SelfFollow => {
                write!(f, "a user cannot follow themselves")
            }
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

/// Map a [`Like`] command's [`CommandError`] into a [`WriteError`].
fn like_err<S: std::fmt::Display>(e: CommandError<LikeError, S>) -> WriteError {
    match e {
        CommandError::Domain(d) => WriteError::Like(d),
        CommandError::Conflict { stream, attempts } => {
            WriteError::Conflict { stream, attempts }
        }
        CommandError::Store(s) => WriteError::Store(s.to_string()),
    }
}

/// Map a [`Follow`] command's [`CommandError`] into a [`WriteError`].
fn follow_err<S: std::fmt::Display>(
    e: CommandError<FollowError, S>,
) -> WriteError {
    match e {
        CommandError::Domain(d) => WriteError::Follow(d),
        CommandError::Conflict { stream, attempts } => {
            WriteError::Conflict { stream, attempts }
        }
        CommandError::Store(s) => WriteError::Store(s.to_string()),
    }
}

/// The write surface the HTTP layer targets: one method per user action, each
/// mapping to exactly one warm-path
/// [`EventStore::command_cached`] call on the right stream (see the blanket
/// impl below for why `command_cached`, not `command`). The relationship
/// actions (`follow`/`unfollow`/`like`/`unlike`) route to the per-edge
/// relationship streams (`follow-<a>_<b>`, `like-<p>_<u>`) rather than folding
/// a crowd into the `User`/`Post` streams — the public signatures are
/// unchanged, only the stream each targets moved.
///
/// **Why plain `command_cached`, never `command_cached_as`.** No method here
/// has an
/// author-id *restated* alongside a stream key it must match: entity commands
/// (`register`, `create_post`, …) key on the entity id directly, and the
/// relationship commands fold their whole identity into the stream key
/// (`follow-<follower>_<followee>`), leaving nothing for
/// [`command_cached_as`](EventStore::command_cached_as) to validate. (The
/// pre-refactor `Follow`-on-the-user-stream was the one genuine authored site;
/// moving the edge onto its own stream dissolved that need — see
/// `domain::follow`.)
///
/// **Why a wrapper and not "HTTP calls `store.command_cached` directly".** The
/// store
/// call is generic in three type parameters (aggregate, command, backend) and
/// returns a `CommandError<R, S>` whose two parameters leak the backend type
/// into every handler signature. [`WriteOps`] pins the stream-naming
/// conventions (`user-<id>`, `post-<id>`, `like-<post>_<user>`,
/// `follow-<follower>_<followee>`) in one place and collapses the error to the
/// backend-agnostic [`WriteError`], so an HTTP handler is a one-liner with a
/// monomorphic signature. Every method returns the write's **global log
/// position**, the token a caller feeds to [`ReadModels::wait_for`] to read
/// its own write.
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

// ---------------------------------------------------------------------------
// Warm writes: this blanket impl routes every write through `command_cached`
// ---------------------------------------------------------------------------
//
// Post-`bn-jes` every aggregate has *bounded* state and implements
// `Snapshottable` + `Clone` (see `domain::{user,post,like,follow}`), which are
// exactly the bounds `EventStore::command_cached` (the hot-aggregate
// write-through cache + snapshot-accelerated cold load) requires. `bn-o9z`
// flips the app onto that warm path: this impl is bound at `B: SnapshotStore`
// (the capability `command_cached` lives behind) and calls `command_cached`
// rather than `command`.
//
// Correctness is unchanged and *proven*: `command_cached` is semantically
// identical to `command` — same optimistic retry, same `Commit`, same error
// taxonomy — differing only in cost (a warm hit reads zero events; a conflict
// re-reads only the delta). `tests/snapshots.rs` is the differential that pins
// this: it drives the same corpus through `command` and `command_cached` and
// asserts byte-identical folded state and identical typed results
// (cache-off == cache-miss == cache-hit). `tests/hot_post_bench.rs` measures
// the speedup on a deep stream.
//
// Tightening the bound from `B: Backend` to `B: SnapshotStore` is what makes
// the on-disk `Store = EventStore<FjallSnapshotBackend<LogEngine>>` (a
// `SnapshotStore`) the warm-write path — see `crate::store_backend`. The demo's
// former plain `EventStore<LogEngine>` was *not* a `SnapshotStore`, which is
// why the flip is a construction-site change (wrap the engine in
// `FjallSnapshotBackend`) threaded through `seed::generate` and the binaries.
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

    async fn follow(
        &self,
        follower: Id,
        target: Id,
    ) -> Result<u64, WriteError> {
        // Self-follow is a key-well-formedness check, enforced here before a
        // degenerate `follow-X_X` stream is ever created — see
        // `domain::follow`. The relationship `decide` cannot see both ids (they
        // are in the key, not visible to a single-stream fold), so it cannot
        // make this check itself.
        if follower == target {
            return Err(WriteError::SelfFollow);
        }
        // Plain `command_cached`, not `command_cached_as`: the follow edge now
        // lives on its own `follow-<follower>_<followee>` stream, so the whole
        // actor identity is in the stream key — there is no separately-restated
        // id to diverge from it, hence nothing for the authored path to check.
        // See `domain::follow`'s module docs.
        let commit = self
            .command_cached::<Follow, _>(
                &follow_stream(follower, target),
                PlaceFollow,
            )
            .await
            .map_err(follow_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }

    async fn unfollow(
        &self,
        follower: Id,
        target: Id,
    ) -> Result<u64, WriteError> {
        let commit = self
            .command_cached::<Follow, _>(
                &follow_stream(follower, target),
                RemoveFollow,
            )
            .await
            .map_err(follow_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }

    async fn create_post(
        &self,
        post: Id,
        author: Id,
        body: String,
    ) -> Result<u64, WriteError> {
        let commit = self
            .command_cached::<Post, _>(
                &post_stream(post),
                CreatePost { author, body },
            )
            .await
            .map_err(post_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }

    async fn delete_post(&self, post: Id, by: Id) -> Result<u64, WriteError> {
        let commit = self
            .command_cached::<Post, _>(&post_stream(post), DeletePost { by })
            .await
            .map_err(post_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }

    async fn like(&self, post: Id, user: Id) -> Result<u64, WriteError> {
        // Plain `command_cached`: like edge on its own `like-<post>_<user>`
        // stream — the key is the full identity, so an authored variant would
        // be a tautology
        // (see `domain::like`).
        let commit = self
            .command_cached::<Like, _>(&like_stream(post, user), PlaceLike)
            .await
            .map_err(like_err)?;
        Ok(commit.last_global_position.unwrap_or(0))
    }

    async fn unlike(&self, post: Id, user: Id) -> Result<u64, WriteError> {
        let commit = self
            .command_cached::<Like, _>(&like_stream(post, user), RemoveLike)
            .await
            .map_err(like_err)?;
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
