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
//! - **Relationships are reconciled here, not in the writer.** The `Like` and
//!   `Follow` aggregates cannot see the `post-<id>`/`user-<id>` streams, so
//!   they accept edges the entity cannot validate (a like on a since-deleted
//!   post, a follow of a user who never registered). This projection is where
//!   those are made harmless: a like on a deleted post never surfaces because
//!   the *post* is filtered out of every feed and the single-post query first;
//!   a follow of a nonexistent user contributes no posts to a timeline. This
//!   mirrors how the home timeline has always filtered by the viewer's
//!   *current* follow set at query time — the crowd membership is the
//!   projection's job, computed from the relationship streams, not the
//!   aggregate's.

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ident::Id;
use mess_core::{CodecError, Event};
use mess_store::{
    AnomalyKind, Backend, EventStore, ProjectionAnomalies,
    ProjectionAnomaliesSnapshot, StoredRecord,
};
use tokio::sync::{Notify, RwLock};

use crate::contracts::{PostView, ProfileView, ReadModels, TimelinePage};
use crate::domain::follow::FollowEvent;
use crate::domain::like::LikeEvent;
use crate::domain::post::PostEvent;
use crate::domain::user::UserEvent;
use crate::parse_pair;

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
///
/// Bounded, mirroring the write aggregate: the follow *graph* is **not** here.
/// It is reconstructed from the `follow-<a>_<b>` relationship streams into
/// [`State::following`]/[`State::followers`] — see the module docs on why the
/// crowd lives in the projection, not the aggregate.
#[derive(Debug, Default, Clone)]
struct UserRow {
    handle:       String,
    display_name: String,
}

/// One post row, folded from that post's `post-<id>` stream.
///
/// Bounded, mirroring the write aggregate: the like *crowd* is **not** here.
/// It is reconstructed from the `like-<post>_<user>` relationship streams into
/// [`State::likes`].
#[derive(Debug, Clone)]
struct PostRow {
    /// The post's id (also its stream id, `post-<id>`, less the prefix).
    id:      Id,
    author:  Id,
    body:    String,
    deleted: bool,
    /// Global position of the `Posted` event — the stable feed sort key.
    seq:     u64,
}

/// The whole in-memory read model. All queries read this behind an
/// [`RwLock`]; the live pump is the only writer.
///
/// The `following`/`followers`/`likes` maps are the projection's
/// reconstruction of the crowds that used to be aggregate state: folded from
/// the relationship streams, keyed for O(1) counts and membership, and kept
/// independent of the entity rows so a relationship event that lands before
/// its entity (any global order is possible) folds cleanly regardless.
#[derive(Debug, Default)]
struct State {
    /// user id -> folded user row.
    users:     HashMap<Id, UserRow>,
    /// handle -> user id, so handle-keyed queries are O(1).
    handles:   HashMap<String, Id>,
    /// follower id -> the set of users it currently follows (folded from
    /// `follow-<follower>_<followee>` streams), so `following_count` and a
    /// home-timeline author set are O(1) lookups.
    following: HashMap<Id, HashSet<Id>>,
    /// followee id -> the set of users following it (the reverse index), so
    /// `follower_count` is O(1) rather than a full scan.
    followers: HashMap<Id, HashSet<Id>>,
    /// post id -> folded post row.
    posts:     HashMap<Id, PostRow>,
    /// post id -> the set of users who currently like it (folded from
    /// `like-<post>_<user>` streams), so a like count and `liked_by_me` are
    /// O(1) lookups.
    likes:     HashMap<Id, HashSet<Id>>,
}

/// Record one anomaly hit at `rec`'s position, and — only on that counter's
/// **first-ever** occurrence — log a one-time warning naming the stream,
/// message type, and position (`bn-3uu`).
///
/// This is the adoption site the [`mess_store::ProjectionAnomalies`] module
/// docs describe: the counter type itself has no logging dependency, so
/// *this* crate decides what a warning line looks like. Logging only on the
/// `0 -> 1` transition keeps a projection stuck skipping the same bad stream
/// forever from flooding stderr, while still guaranteeing the very first
/// occurrence of each kind is loud. The liveness policy is unchanged: the
/// caller still skips the record either way.
fn record_anomaly(
    anomalies: &ProjectionAnomalies,
    kind: AnomalyKind,
    rec: &StoredRecord,
) {
    if anomalies.record(kind, rec.global_position) {
        eprintln!(
            "social projections: WARNING first {kind}: stream={:?} \
             message_type={:?} global_position={}",
            rec.stream_id, rec.message_type, rec.global_position
        );
    }
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
        }
    }

    /// Fold one like-relationship event into the [`likes`](State::likes) index.
    /// `post` and `user` are parsed from the `like-<post>_<user>` stream
    /// suffix. Stored independent of any [`PostRow`], so a like folded before
    /// its post's `Posted` event (any global order is possible) is retained and
    /// surfaces once the post appears.
    fn apply_like(&mut self, post: Id, user: Id, ev: &LikeEvent) {
        match ev {
            LikeEvent::Liked => {
                self.likes.entry(post).or_default().insert(user);
            }
            LikeEvent::Unliked => {
                if let Some(set) = self.likes.get_mut(&post) {
                    set.remove(&user);
                }
            }
        }
    }

    /// Fold one follow-relationship event into the
    /// [`following`](State::following)/[`followers`](State::followers) indexes.
    /// `follower` and `followee` are parsed from the
    /// `follow-<follower>_<followee>` stream suffix.
    fn apply_follow(&mut self, follower: Id, followee: Id, ev: &FollowEvent) {
        match ev {
            FollowEvent::Followed => {
                self.following.entry(follower).or_default().insert(followee);
                self.followers.entry(followee).or_default().insert(follower);
            }
            FollowEvent::Unfollowed => {
                if let Some(set) = self.following.get_mut(&follower) {
                    set.remove(&followee);
                }
                if let Some(set) = self.followers.get_mut(&followee) {
                    set.remove(&follower);
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
    ///
    /// **Silent-skip is still the liveness policy** — that does not change
    /// here. What changes (dogfood finding, `bn-3uu`) is that every skip site
    /// now increments the matching [`mess_store::ProjectionAnomalies`]
    /// counter via [`record_anomaly`], so schema drift or a routing bug shows
    /// up in [`Projections::anomalies`] instead of vanishing silently. Each
    /// counter also logs one warning line the first time it goes nonzero (see
    /// `record_anomaly`), without spamming on every subsequent occurrence.
    fn apply_record(
        &mut self,
        rec: &StoredRecord,
        anomalies: &ProjectionAnomalies,
    ) {
        let (category, suffix) = rec.category_and_suffix();
        match category {
            "user" => match Id::from_str(suffix) {
                Ok(owner) => {
                    match UserEvent::decode(&rec.message_type, &rec.data) {
                        Ok(ev) => self.apply_user(owner, &ev),
                        Err(CodecError::UnknownEventName(_)) => record_anomaly(
                            anomalies,
                            AnomalyKind::UnknownEventKind,
                            rec,
                        ),
                        Err(_) => record_anomaly(
                            anomalies,
                            AnomalyKind::UndecodablePayload,
                            rec,
                        ),
                    }
                }
                Err(_) => record_anomaly(
                    anomalies,
                    AnomalyKind::UnroutableStream,
                    rec,
                ),
            },
            "post" => match Id::from_str(suffix) {
                Ok(id) => {
                    match PostEvent::decode(&rec.message_type, &rec.data) {
                        Ok(ev) => self.apply_post(id, rec.global_position, &ev),
                        Err(CodecError::UnknownEventName(_)) => record_anomaly(
                            anomalies,
                            AnomalyKind::UnknownEventKind,
                            rec,
                        ),
                        Err(_) => record_anomaly(
                            anomalies,
                            AnomalyKind::UndecodablePayload,
                            rec,
                        ),
                    }
                }
                Err(_) => record_anomaly(
                    anomalies,
                    AnomalyKind::UnroutableStream,
                    rec,
                ),
            },
            // Relationship streams: the suffix is a `<id>_<id>` pair (see
            // `crate::parse_pair` / `crate::PAIR_SEP`), not a single id.
            "like" => match parse_pair(suffix) {
                Some((post, user)) => {
                    match LikeEvent::decode(&rec.message_type, &rec.data) {
                        Ok(ev) => self.apply_like(post, user, &ev),
                        Err(CodecError::UnknownEventName(_)) => record_anomaly(
                            anomalies,
                            AnomalyKind::UnknownEventKind,
                            rec,
                        ),
                        Err(_) => record_anomaly(
                            anomalies,
                            AnomalyKind::UndecodablePayload,
                            rec,
                        ),
                    }
                }
                None => record_anomaly(
                    anomalies,
                    AnomalyKind::UnroutableStream,
                    rec,
                ),
            },
            "follow" => match parse_pair(suffix) {
                Some((follower, followee)) => {
                    match FollowEvent::decode(&rec.message_type, &rec.data) {
                        Ok(ev) => self.apply_follow(follower, followee, &ev),
                        Err(CodecError::UnknownEventName(_)) => record_anomaly(
                            anomalies,
                            AnomalyKind::UnknownEventKind,
                            rec,
                        ),
                        Err(_) => record_anomaly(
                            anomalies,
                            AnomalyKind::UndecodablePayload,
                            rec,
                        ),
                    }
                }
                None => record_anomaly(
                    anomalies,
                    AnomalyKind::UnroutableStream,
                    rec,
                ),
            },
            _ => record_anomaly(anomalies, AnomalyKind::UnroutableStream, rec),
        }
    }

    /// Render one post row into a viewer-relative [`PostView`], joining the
    /// author's profile and the like crowd (from [`likes`](State::likes),
    /// keyed by post id).
    fn view_of(&self, p: &PostRow, viewer: Option<Id>) -> PostView {
        let (handle, display) = self
            .users
            .get(&p.author)
            .map(|u| (u.handle.clone(), u.display_name.clone()))
            .unwrap_or_default();
        let likers = self.likes.get(&p.id);
        PostView {
            id:             p.id,
            author_id:      p.author,
            author_handle:  handle,
            author_display: display,
            body:           p.body.clone(),
            likes:          likers.map_or(0, HashSet::len) as u64,
            liked_by_me:    viewer
                .is_some_and(|v| likers.is_some_and(|s| s.contains(&v))),
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
    state:     Arc<RwLock<State>>,
    /// The read watermark: the number of global events applied, i.e. one past
    /// the highest global position folded in. `wait_for(p)` waits for this to
    /// exceed `p`.
    applied:   Arc<AtomicU64>,
    /// Pulsed after every batch the pump applies, so `wait_for` waiters wake.
    notify:    Arc<Notify>,
    /// Counts of records this projection could not decode or route, across
    /// both the catch-up replay and the live pump (`bn-3uu`). Held behind an
    /// `Arc` (not the `state` lock) so [`Projections::anomalies`] reads it
    /// without contending with the fold.
    anomalies: Arc<ProjectionAnomalies>,
    /// The live pump; aborted on drop.
    pump:      tokio::task::JoinHandle<()>,
    _backend:  PhantomData<B>,
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
        let anomalies = Arc::new(ProjectionAnomalies::new());

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
                    st.apply_record(rec, &anomalies);
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
            anomalies.clone(),
            after,
        ));

        Self { state, applied, notify, anomalies, pump, _backend: PhantomData }
    }

    /// The undecodable-payload / unroutable-stream / unknown-event-kind
    /// counters for records this projection has skipped, across both the
    /// initial catch-up replay and the live pump (`bn-3uu`). The liveness
    /// policy is unchanged — a skip is still a skip — but a schema drift or
    /// routing bug now shows up here (and, on each counter's first hit, as a
    /// one-time warning line) instead of vanishing silently. See
    /// [`mess_store::ProjectionAnomalies`].
    #[must_use]
    pub fn anomalies(&self) -> ProjectionAnomaliesSnapshot {
        self.anomalies.snapshot()
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
    anomalies: Arc<ProjectionAnomalies>,
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
                st.apply_record(rec, &anomalies);
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
        // themselves. Computed at query time against the *current* follow set
        // (folded from the follow-relationship streams), which is what makes
        // follow-after-post retroactive.
        let mut authors: HashSet<Id> =
            st.following.get(&user).cloned().unwrap_or_default();
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
        let following_count =
            st.following.get(&id).map_or(0, HashSet::len) as u64;
        let followed_by_me = viewer.is_some_and(|v| {
            st.following.get(&v).is_some_and(|s| s.contains(&id))
        });
        Some(ProfileView {
            handle: u.handle.clone(),
            display_name: u.display_name.clone(),
            post_count,
            follower_count,
            following_count,
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
