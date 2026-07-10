//! Real [`ReadModels`] over the store: **rebuildable** in-memory projections
//! that tail the global event log.
//!
//! This is the read-path counterpart to [`crate::contracts::FakeReadModels`].
//! Where the fake is seeded imperatively and is synchronously consistent, this
//! [`Projections`] type is built the way a production read model is: by
//! **replaying the event log from position 0**, folding every event into
//! in-memory tables, and then **following the log live** as new events land.
//! Construction *is* a rebuild — there is no separate "rebuild" entry point,
//! because rebuild-from-zero is the only way it is ever built. That is the
//! event-sourcing showcase: the same [`new`](Projections::new) call that a
//! long-running process invokes once at boot is exactly what a test invokes to
//! rebuild a fresh, identical view (see the `rebuild == live` equivalence
//! test).
//!
//! # What the store hands an application (dogfood finding)
//!
//! The app-facing [`EventStore`] exposes **no** subscription or live-tail API.
//! The only global-read primitive reachable from an application is
//! [`Backend::read_global`] via [`EventStore::backend`] — a paged
//! `read_global(after, limit)` pull. The catch-up→live subscription runtime in
//! `mess-log` (`subscription.rs`, spec D11) runs on that crate's own
//! `ReadView`/`Watermark` primitives and is **not** wired to
//! [`EventStore`]/[`Backend`]. So this projection is built on the one thing
//! that *is* exposed — `read_global` — with a small **polling adapter**: a
//! background task that pages `read_global` from a cursor, applies each event,
//! and advances a watermark. A proposed store-level API to remove the polling
//! is filed as this bone's top open question.
//!
//! # Design decisions
//!
//! - **Pull (query-time filtering), not push (fan-out).** A home timeline is
//!   computed at query time by filtering all posts against the viewer's
//!   *current* follow set — never materialized as a per-user inbox at follow
//!   time. This makes **follow-after-post retroactive for free**: following
//!   someone surfaces their old posts, and because both the live path and a
//!   from-scratch rebuild ask the same question of the same folded state, the
//!   two paths are identical by construction (the classic fan-out projection
//!   bug — live and rebuild disagreeing on retroactive follows — cannot arise
//!   here). The equivalence test covers exactly this case.
//! - **Deleted posts drop from every feed, but a permalink resolves to a
//!   tombstone.** [`ReadModels::post`] returns `None` for a deleted post (feed
//!   semantics: it is gone), matching the trait contract and the fake. A direct
//!   link, however, should not 404 — so the inherent
//!   [`Projections::lookup_post`] returns the post *including* deleted ones
//!   with a `deleted` flag, letting a permalink handler render "this post was
//!   deleted" with author attribution intact instead of a dead link.
//! - **`created_seq` is the global position of the `Posted` event.** It is the
//!   stable, monotonic feed sort key and pagination cursor, assigned once when
//!   the post is created and never moved by later likes/deletes.

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ident::Id;
use mess_core::Event;
use mess_store::{Backend, EventStore, StoredRecord};
use tokio::sync::{Notify, RwLock};

use crate::contracts::{PostView, ProfileView, ReadModels, TimelinePage};
use crate::domain::post::PostEvent;
use crate::domain::user::UserEvent;

/// How many global events to pull per `read_global` page.
const BATCH: usize = 512;

/// How long the live pump sleeps when it has drained the log, before polling
/// `read_global` again. Small because the only backpressure signal available
/// over the `read_global` pull is "an empty page means caught up" — there is
/// no commit notification to await (see the module docs' dogfood finding).
const POLL: Duration = Duration::from_millis(1);

// ===========================================================================
// Folded state
// ===========================================================================

/// One user row, folded from that user's `user-<id>` stream.
#[derive(Debug, Default, Clone)]
struct UserRow {
    handle:       String,
    display_name: String,
    /// Ids this user currently follows (the follow set, mirroring the write
    /// aggregate's `following`).
    following:    HashSet<Id>,
}

/// One post row, folded from that post's `post-<id>` stream.
#[derive(Debug, Clone)]
struct PostRow {
    /// The post's id (also its stream id, `post-<id>`, less the prefix).
    id:      Id,
    author:  Id,
    body:    String,
    likes:   HashSet<Id>,
    deleted: bool,
    /// Global position of the `Posted` event — the stable feed sort key.
    seq:     u64,
}

/// The whole in-memory read model. All queries read this behind an
/// [`RwLock`]; the live pump is the only writer.
#[derive(Debug, Default)]
struct State {
    /// user id -> folded user row.
    users:     HashMap<Id, UserRow>,
    /// handle -> user id, so handle-keyed queries are O(1).
    handles:   HashMap<String, Id>,
    /// target id -> the set of users following it (reverse of `following`), so
    /// `follower_count` is O(1) rather than a full scan.
    followers: HashMap<Id, HashSet<Id>>,
    /// post id -> folded post row.
    posts:     HashMap<Id, PostRow>,
}

impl State {
    /// Fold one user event into the tables. `owner` is the id parsed from the
    /// `user-<id>` stream suffix — the user whose stream this is.
    fn apply_user(&mut self, owner: Id, ev: &UserEvent) {
        match ev {
            UserEvent::Registered { handle, display_name } => {
                self.handles.insert(handle.clone(), owner);
                let row = self.users.entry(owner).or_default();
                handle.clone_into(&mut row.handle);
                display_name.clone_into(&mut row.display_name);
            }
            UserEvent::DisplayNameChanged { display_name } => {
                if let Some(row) = self.users.get_mut(&owner) {
                    display_name.clone_into(&mut row.display_name);
                }
            }
            UserEvent::Followed { target } => {
                self.users.entry(owner).or_default().following.insert(*target);
                self.followers.entry(*target).or_default().insert(owner);
            }
            UserEvent::Unfollowed { target } => {
                if let Some(row) = self.users.get_mut(&owner) {
                    row.following.remove(target);
                }
                if let Some(set) = self.followers.get_mut(target) {
                    set.remove(&owner);
                }
            }
        }
    }

    /// Fold one post event into the tables. `id` is parsed from the
    /// `post-<id>` stream suffix; `gp` is the event's global position (used
    /// only to stamp `seq` on creation).
    fn apply_post(&mut self, id: Id, gp: u64, ev: &PostEvent) {
        match ev {
            PostEvent::Posted { author, body } => {
                self.posts.insert(
                    id,
                    PostRow {
                        id,
                        author: *author,
                        body: body.clone(),
                        likes: HashSet::new(),
                        deleted: false,
                        seq: gp,
                    },
                );
            }
            PostEvent::Deleted { .. } => {
                if let Some(p) = self.posts.get_mut(&id) {
                    p.deleted = true;
                }
            }
            PostEvent::Liked { user } => {
                if let Some(p) = self.posts.get_mut(&id) {
                    p.likes.insert(*user);
                }
            }
            PostEvent::Unliked { user } => {
                if let Some(p) = self.posts.get_mut(&id) {
                    p.likes.remove(user);
                }
            }
        }
    }

    /// Route one stored record to the right fold by
    /// [`StoredRecord::category`] — the blessed, typed replacement for
    /// hand-rolled stream-prefix string surgery. Records on streams this
    /// projection does not understand (an unknown category, or a suffix/
    /// payload that fails to decode) are skipped — a projection must
    /// tolerate a log wider than the slice of it that it models.
    fn apply_record(&mut self, rec: &StoredRecord) {
        let (category, suffix) = rec.category_and_suffix();
        match category {
            "user" => {
                if let Ok(owner) = Id::from_str(suffix)
                    && let Ok(ev) =
                        UserEvent::decode(&rec.message_type, &rec.data)
                {
                    self.apply_user(owner, &ev);
                }
            }
            "post" => {
                if let Ok(id) = Id::from_str(suffix)
                    && let Ok(ev) =
                        PostEvent::decode(&rec.message_type, &rec.data)
                {
                    self.apply_post(id, rec.global_position, &ev);
                }
            }
            _ => {}
        }
    }

    /// Render one post row into a viewer-relative [`PostView`], joining the
    /// author's profile.
    fn view_of(&self, p: &PostRow, viewer: Option<Id>) -> PostView {
        let (handle, display) = self
            .users
            .get(&p.author)
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
    /// opaque `cursor`/`limit`. The cursor is the `seq` to page strictly
    /// *before* — identical semantics to [`FakeReadModels`], so a real and a
    /// fake read model paginate the same way.
    ///
    /// [`FakeReadModels`]: crate::contracts::FakeReadModels
    fn page(
        &self,
        mut posts: Vec<&PostRow>,
        viewer: Option<Id>,
        cursor: Option<String>,
        limit: usize,
    ) -> TimelinePage {
        posts.sort_by_key(|p| std::cmp::Reverse(p.seq));
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

// ===========================================================================
// Projections: the live, rebuildable read model
// ===========================================================================

/// A permalink lookup result from [`Projections::lookup_post`]: the post's
/// view plus whether it has been deleted.
///
/// Feeds drop deleted posts entirely (that is what [`ReadModels::post`]
/// enforces by returning `None`). A *direct link*, though, should resolve to a
/// tombstone rather than a dead 404 — so this carries the view even for a
/// deleted post, with `deleted` telling a handler to render "this post was
/// deleted" instead of the body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostLookup {
    /// The rendered post. For a deleted post the fields are still populated
    /// (author attribution survives); a handler chooses whether to show
    /// `view.body` based on `deleted`.
    pub view:    PostView,
    /// Whether this post has been deleted (a tombstone).
    pub deleted: bool,
}

/// A rebuildable, live-tailing [`ReadModels`] implementation over any
/// [`Backend`].
///
/// Cloneable is intentionally **not** derived: the value owns the live pump
/// task and aborts it on drop, so there is exactly one owner of the tail.
/// Share it behind an `Arc` if several handlers need it.
#[derive(Debug)]
pub struct Projections<B: Backend> {
    state:    Arc<RwLock<State>>,
    /// The read watermark: the number of global events applied, i.e. one past
    /// the highest global position folded in. `wait_for(p)` waits for this to
    /// exceed `p`.
    applied:  Arc<AtomicU64>,
    /// Pulsed after every batch the pump applies, so `wait_for` waiters wake.
    notify:   Arc<Notify>,
    /// The live pump; aborted on drop.
    pump:     tokio::task::JoinHandle<()>,
    _backend: PhantomData<B>,
}

impl<B: Backend> Drop for Projections<B> {
    fn drop(&mut self) { self.pump.abort(); }
}

impl<B: Backend + Clone> Projections<B> {
    /// Build the read model by **replaying the whole log from position 0**,
    /// then spawn the live pump that follows it forward.
    ///
    /// Two phases, the catch-up→live handoff:
    /// 1. **Catch-up** (synchronous): drain `read_global` from the start into
    ///    the tables, so every query issued immediately after `new` returns
    ///    reflects all events already committed at build time.
    /// 2. **Live** (background task): continue paging `read_global` from the
    ///    catch-up cursor, applying new events and advancing the watermark as
    ///    they land.
    ///
    /// Because phase 1 always starts from 0, **construction is a full
    /// rebuild** — that is the property the equivalence test exercises.
    pub async fn new(store: &EventStore<B>) -> Self {
        let backend = store.backend().clone();
        let state = Arc::new(RwLock::new(State::default()));
        let applied = Arc::new(AtomicU64::new(0));
        let notify = Arc::new(Notify::new());

        // Phase 1: synchronous catch-up from position 0 to the current head.
        let mut after: Option<u64> = None;
        {
            let mut st = state.write().await;
            loop {
                let page = match backend.read_global(after, BATCH).await {
                    Ok(p) => p,
                    // A transient read error ends catch-up early; the live pump
                    // resumes from `after` and self-heals.
                    Err(_) => break,
                };
                if page.is_empty() {
                    break;
                }
                let short = page.len() < BATCH;
                for rec in &page {
                    st.apply_record(rec);
                    after = Some(rec.global_position);
                }
                if short {
                    break;
                }
            }
        }
        applied.store(watermark_of(after), Ordering::Release);

        // Phase 2: spawn the live pump.
        let pump = tokio::spawn(pump_loop(
            backend,
            state.clone(),
            applied.clone(),
            notify.clone(),
            after,
        ));

        Self { state, applied, notify, pump, _backend: PhantomData }
    }

    /// Permalink lookup: resolve a post by id **including deleted posts**,
    /// returning a [`PostLookup`] with the tombstone flag. `None` only if no
    /// post with that id ever existed. See [`PostLookup`].
    pub async fn lookup_post(
        &self,
        id: Id,
        viewer: Option<Id>,
    ) -> Option<PostLookup> {
        let st = self.state.read().await;
        st.posts.get(&id).map(|p| PostLookup {
            view:    st.view_of(p, viewer),
            deleted: p.deleted,
        })
    }
}

/// The watermark for a "last applied global position" cursor: one past it, or
/// 0 when nothing has been applied.
fn watermark_of(after: Option<u64>) -> u64 { after.map_or(0, |p| p + 1) }

/// The live pump: page `read_global` from `after`, apply each event, advance
/// the watermark, and pulse waiters — sleeping briefly whenever the log is
/// drained. Runs until the [`Projections`] is dropped (which aborts it).
async fn pump_loop<B: Backend>(
    backend: B,
    state: Arc<RwLock<State>>,
    applied: Arc<AtomicU64>,
    notify: Arc<Notify>,
    mut after: Option<u64>,
) {
    loop {
        let page = match backend.read_global(after, BATCH).await {
            Ok(p) => p,
            Err(_) => {
                tokio::time::sleep(POLL).await;
                continue;
            }
        };
        if page.is_empty() {
            // Caught up: nothing to await over a bare `read_global` pull, so
            // poll again shortly.
            tokio::time::sleep(POLL).await;
            continue;
        }
        {
            let mut st = state.write().await;
            for rec in &page {
                st.apply_record(rec);
                after = Some(rec.global_position);
            }
        }
        applied.store(watermark_of(after), Ordering::Release);
        notify.notify_waiters();
    }
}

impl<B: Backend> ReadModels for Projections<B> {
    async fn home_timeline(
        &self,
        user: Id,
        cursor: Option<String>,
        limit: usize,
    ) -> TimelinePage {
        let st = self.state.read().await;
        // Authors in `user`'s home feed: everyone they currently follow, plus
        // themselves. Computed at query time against the *current* follow set,
        // which is what makes follow-after-post retroactive.
        let mut authors: HashSet<Id> = st
            .users
            .get(&user)
            .map(|u| u.following.clone())
            .unwrap_or_default();
        authors.insert(user);
        let posts: Vec<&PostRow> = st
            .posts
            .values()
            .filter(|p| !p.deleted && authors.contains(&p.author))
            .collect();
        st.page(posts, Some(user), cursor, limit)
    }

    async fn user_posts(
        &self,
        handle: &str,
        cursor: Option<String>,
        limit: usize,
    ) -> TimelinePage {
        let st = self.state.read().await;
        let author = st.handles.get(handle).copied();
        let posts: Vec<&PostRow> = st
            .posts
            .values()
            .filter(|p| !p.deleted && Some(p.author) == author)
            .collect();
        st.page(posts, None, cursor, limit)
    }

    async fn firehose(
        &self,
        cursor: Option<String>,
        limit: usize,
    ) -> TimelinePage {
        let st = self.state.read().await;
        let posts: Vec<&PostRow> =
            st.posts.values().filter(|p| !p.deleted).collect();
        st.page(posts, None, cursor, limit)
    }

    async fn profile(
        &self,
        handle: &str,
        viewer: Option<Id>,
    ) -> Option<ProfileView> {
        let st = self.state.read().await;
        let id = *st.handles.get(handle)?;
        let u = st.users.get(&id)?;
        let post_count =
            st.posts.values().filter(|p| !p.deleted && p.author == id).count()
                as u64;
        let follower_count =
            st.followers.get(&id).map_or(0, HashSet::len) as u64;
        let followed_by_me = viewer.is_some_and(|v| {
            st.users.get(&v).is_some_and(|vu| vu.following.contains(&id))
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
        let st = self.state.read().await;
        st.posts.get(&id).filter(|p| !p.deleted).map(|p| st.view_of(p, viewer))
    }

    async fn resolve(&self, handle: &str) -> Option<Id> {
        let st = self.state.read().await;
        st.handles.get(handle).copied()
    }

    async fn wait_for(&self, position: u64) {
        // Block until the pump has applied the event at global `position`,
        // i.e. the watermark (count applied) has passed it. Arm the notified()
        // future *before* the final check to avoid a lost wakeup between the
        // load and the await.
        loop {
            if self.applied.load(Ordering::Acquire) > position {
                return;
            }
            let notified = self.notify.notified();
            if self.applied.load(Ordering::Acquire) > position {
                return;
            }
            notified.await;
        }
    }
}
