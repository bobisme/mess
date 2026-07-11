//! Real [`ReadModels`] over the store: **rebuildable** in-memory projections
//! that tail the global event log via [`EventStore::subscribe`], and
//! **checkpoint** their folded state so a restart resumes from where it left
//! off instead of replaying the whole log from position 0.
//!
//! This is the read-path counterpart to [`crate::contracts::FakeReadModels`].
//! Where the fake is seeded imperatively and is synchronously consistent, this
//! [`Projections`] type is built the way a production read model is: by folding
//! the event log into in-memory tables and following the log live as new events
//! land. [`Projections::new`] always folds **from position 0** — a full,
//! from-scratch rebuild, the event-sourcing showcase and the equivalence test's
//! baseline. [`Projections::with_checkpoint`] is the operational path: it
//! **resumes** from a persisted checkpoint (folded state + the global position
//! it was current to) and only replays the *suffix* of the log committed since,
//! falling back to a clean from-0 rebuild whenever the checkpoint is missing,
//! stale, or corrupt.
//!
//! # The store primitives this is built on
//!
//! The app-facing [`EventStore`] exposes an **event-bounded** catch-up→live
//! subscription ([`EventStore::subscribe`]) plus a committed-watermark barrier
//! ([`EventStore::watermark`]/[`EventStore::await_past`]), all driven by commit
//! notification rather than polling — the [`SubscribeBackend`] capability. This
//! projection adopts them directly:
//!
//! - **Live tail.** The pump owns one [`Subscription`] and pulls batches with
//!   [`Subscription::next_batch`]. While caught up it is *parked on the
//!   watermark*, woken by the next commit — there is no `sleep`-based poll loop
//!   (the previous 1 ms `read_global` poll is gone).
//! - **`wait_for` is event-bounded.** The read-your-writes barrier resolves the
//!   instant the pump has *folded* past the requested position. Because the
//!   pump advances only on a real commit-notification wake, `wait_for` inherits
//!   event-bounded latency — it no longer waits out a poll interval.
//!
//! # Checkpoint placement (deliberate)
//!
//! The checkpoint is a **sidecar file next to the store dir**
//! (`<dir>/.social-projections.ckpt`), written with an **atomic
//! write-to-temp-then-rename** so a crash mid-write can never leave a
//! half-written checkpoint in place (the rename is atomic; a stale/partial
//! `.tmp` is simply ignored on the next load). It is deliberately **not**
//! written into the event log itself: a projection checkpoint is derived,
//! disposable read-model state, and appending it to the domain history would
//! pollute the log every application would then have to replay and skip. A
//! sidecar keeps the log pure and the checkpoint trivially discardable.
//!
//! # Versioning & crash-safety
//!
//! [`PROJECTION_VERSION`] mirrors a `fold_version`: bump it whenever the fold
//! logic changes, so a checkpoint written by older logic is **discarded** and
//! the projection rebuilds clean from 0 — never partially trusted. The file
//! also carries a [`CKPT_MAGIC`] magic and an envelope [`CKPT_FORMAT_VERSION`]
//! (orthogonal to `PROJECTION_VERSION`: the envelope framing versus the fold
//! logic), so a truncated or foreign file that happens to decode is rejected on
//! the header rather than half-loaded. A corrupt/truncated file simply fails to
//! decode and triggers a from-0 rebuild — no panic. Crash-safety is therefore:
//! at worst re-fold the suffix from the last checkpoint, and folds are
//! idempotent from a position (the resume path folds exactly the positions the
//! checkpoint had not yet seen), which the `tests/projections.rs` kill/restart
//! and live-pump-checkpoint tests prove against a from-0 rebuild.
//!
//! # The counter story
//!
//! Like/follower counts are projection-maintained (folded from the
//! relationship streams into the crowd maps below), so they live **inside** the
//! checkpointed [`State`] and resume with it — a restart does not re-scan the
//! log to recount. What the checkpoint does **not** carry is the
//! [`ProjectionAnomalies`] counters: those count the records *this process*
//! skipped and are observability of a run, not domain-derived state, so a fresh
//! process starts them at zero (persisting cross-restart totals would conflate
//! distinct processes' skip histories — an explicitly rejected choice).
//!
//! # Design decisions (unchanged from the pre-checkpoint model)
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
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mess_core::{CodecError, Event};
use mess_store::{
    AnomalyKind, Backend, EventStore, ProjectionAnomalies,
    ProjectionAnomaliesSnapshot, StoredRecord, SubscribeBackend, Subscription,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, RwLock};

use crate::Id;
use crate::contracts::{PostView, ProfileView, ReadModels, TimelinePage};
use crate::domain::follow::FollowEvent;
use crate::domain::like::LikeEvent;
use crate::domain::post::PostEvent;
use crate::domain::user::UserEvent;
use crate::parse_pair;

/// The **projection-logic version**, mirroring a `fold_version`.
///
/// Bump this whenever the fold logic (what [`State::apply_record`] does with an
/// event, or the shape of [`State`]) changes in a way that makes an older
/// checkpoint's folded state wrong. A checkpoint whose `projection_version`
/// does not match this constant is **discarded** and the projection rebuilds
/// clean from position 0 — never partially trusted. This is deliberately
/// distinct from [`CKPT_FORMAT_VERSION`] (the on-disk envelope framing): the
/// framing can be stable while the fold logic changes, and vice versa.
pub const PROJECTION_VERSION: u32 = 1;

/// Magic bytes at the head of a checkpoint file, distinct from
/// [`PROJECTION_VERSION`]. A truncated, foreign, or garbage file that happens
/// to decode into the envelope shape is rejected here rather than half-loaded.
const CKPT_MAGIC: u32 = 0x534F_4350; // "SOCP"

/// On-disk checkpoint **envelope** format version — the framing of the file,
/// orthogonal to [`PROJECTION_VERSION`]. Bump only if the [`Checkpoint`] struct
/// layout changes; a mismatch discards the checkpoint like any other.
const CKPT_FORMAT_VERSION: u16 = 1;

/// Default live-pump checkpoint cadence: write a checkpoint after this many
/// folded events. Bounds staleness by event count.
const CKPT_EVERY_N: u64 = 256;

/// Default live-pump checkpoint cadence by wall time: also write a checkpoint
/// if this long has elapsed since the last one (whichever trips first). Only
/// evaluated when a batch lands — a quiescent projection has nothing new to
/// persist.
const CKPT_EVERY_T: Duration = Duration::from_secs(5);

/// How long the pump backs off after a *transient backend error* from the
/// subscription before retrying. This is an error backoff, **not** a
/// steady-state poll: on the happy path the pump is parked on the store
/// watermark (event-bounded), never sleeping.
const PUMP_ERROR_BACKOFF: Duration = Duration::from_millis(50);

// ===========================================================================
// Folded state
// ===========================================================================

/// One user row, folded from that user's `user-<id>` stream.
///
/// Bounded, mirroring the write aggregate: the follow *graph* is **not** here.
/// It is reconstructed from the `follow-<a>_<b>` relationship streams into
/// [`State::following`]/[`State::followers`] — see the module docs on why the
/// crowd lives in the projection, not the aggregate.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct UserRow {
    handle:       String,
    display_name: String,
}

/// One post row, folded from that post's `post-<id>` stream.
///
/// Bounded, mirroring the write aggregate: the like *crowd* is **not** here.
/// It is reconstructed from the `like-<post>_<user>` relationship streams into
/// [`State::likes`].
#[derive(Debug, Clone, Serialize, Deserialize)]
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
///
/// `Serialize`/`Deserialize` are derived so the whole folded view — including
/// the like/follower crowd maps that back the counts — round-trips into a
/// checkpoint (see [`Checkpoint`]). `Clone` lets the pump snapshot the state
/// under a brief read lock and serialize the copy outside the lock.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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

    /// A canonical, iteration-order-independent byte fingerprint of the folded
    /// state: msgpack over a **key-sorted** view of every map (and every
    /// membership set sorted too). Two [`State`]s with identical folded content
    /// serialize to byte-identical fingerprints regardless of `HashMap`/
    /// `HashSet` iteration order (which is per-instance randomized) — the basis
    /// of the `--rebuild` checkpoint-correctness byte-compare. Sorted by each
    /// [`Id`]'s own `Ord` (bn-gt5: the UUIDv7-backed `Id` derives it) rather
    /// than by comparing its `Display` string — simpler than the pre-bn-gt5
    /// version, which had to sort by string because `Id` had no `Ord`. (The
    /// two orders agree by construction — see `id`'s module docs — so this is
    /// a pure simplification, not a behavior change.) The map values are
    /// still carried as `Id::to_string()` in the final tuple purely because
    /// that is what serializes; only the *sort key* changed.
    fn canonical_bytes(&self) -> Vec<u8> {
        fn sorted_ids(set: &HashSet<Id>) -> Vec<String> {
            let mut ids: Vec<Id> = set.iter().copied().collect();
            ids.sort();
            ids.iter().map(ToString::to_string).collect()
        }
        fn sorted_map<V>(map: &HashMap<Id, V>) -> Vec<(String, &V)> {
            let mut v: Vec<(Id, &V)> =
                map.iter().map(|(&id, val)| (id, val)).collect();
            v.sort_by_key(|(id, _)| *id);
            v.into_iter().map(|(id, val)| (id.to_string(), val)).collect()
        }
        fn sorted_sets(
            map: &HashMap<Id, HashSet<Id>>,
        ) -> Vec<(String, Vec<String>)> {
            let mut v: Vec<(Id, Vec<String>)> =
                map.iter().map(|(&id, set)| (id, sorted_ids(set))).collect();
            v.sort_by_key(|(id, _)| *id);
            v.into_iter().map(|(id, s)| (id.to_string(), s)).collect()
        }
        let mut handles: Vec<(&String, String)> =
            self.handles.iter().map(|(h, id)| (h, id.to_string())).collect();
        handles.sort_by(|a, b| a.0.cmp(b.0));

        // A tuple of sorted views — serialized deterministically by msgpack.
        let canon = (
            sorted_map(&self.users),
            handles,
            sorted_sets(&self.following),
            sorted_sets(&self.followers),
            sorted_map(&self.posts),
            sorted_sets(&self.likes),
        );
        rmp_serde::to_vec(&canon).expect("canonical state serializes")
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
// Checkpoint: the on-disk sidecar
// ===========================================================================

/// The on-disk checkpoint envelope: a magic + envelope-format header, the
/// projection-logic version it was written under, the global position the
/// folded state is current *to* (its watermark, i.e. one past the highest
/// global position folded), and the folded [`State`] itself.
///
/// Serialized with msgpack (`rmp-serde`) — the same codec `#[derive(Event)]`
/// uses for event payloads, so no new serialization strategy enters the crate.
/// The header fields come first so a truncated file fails to decode (or fails
/// the header check) rather than half-loading a plausible-looking state.
#[derive(Serialize, Deserialize)]
struct Checkpoint {
    /// [`CKPT_MAGIC`] — a foreign/garbage file that decodes is rejected here.
    magic:              u32,
    /// [`CKPT_FORMAT_VERSION`] — the envelope framing version.
    format_version:     u16,
    /// [`PROJECTION_VERSION`] the state was folded under; a mismatch discards.
    projection_version: u32,
    /// The watermark the state is current to: one past the highest folded
    /// global position, i.e. the `from` a resume subscribes at.
    applied:            u64,
    /// The folded read model, crowd count maps included.
    state:              State,
}

impl Checkpoint {
    /// Load and **validate** a checkpoint from `path`. Returns the `(applied,
    /// state)` to resume from, or `None` — a full from-0 rebuild — whenever the
    /// file is missing, undecodable (corrupt/truncated), or fails any header
    /// check (wrong magic, envelope format, or [`PROJECTION_VERSION`]). Never
    /// panics and never partially trusts a checkpoint.
    fn load(path: &Path) -> Option<(u64, State)> {
        let bytes = std::fs::read(path).ok()?;
        // A truncated or garbage file fails to decode here -> None -> rebuild.
        let ck: Checkpoint = rmp_serde::from_slice(&bytes).ok()?;
        if ck.magic != CKPT_MAGIC
            || ck.format_version != CKPT_FORMAT_VERSION
            || ck.projection_version != PROJECTION_VERSION
        {
            return None;
        }
        Some((ck.applied, ck.state))
    }
}

/// Serialize `state` (current to `applied`, folded under projection-logic
/// version `version`) and write it to `path` **atomically**: write a sibling
/// temp file, then rename it over `path`. The rename is atomic on every target
/// filesystem, so a reader (this process on the next boot, or a concurrent one)
/// never observes a half-written checkpoint; a crash mid-write leaves at worst
/// a stale `.tmp` that the next [`Checkpoint::load`] ignores.
fn write_checkpoint(
    state: State,
    applied: u64,
    version: u32,
    path: &Path,
) -> std::io::Result<()> {
    let ck = Checkpoint {
        magic: CKPT_MAGIC,
        format_version: CKPT_FORMAT_VERSION,
        projection_version: version,
        applied,
        state,
    };
    let bytes = rmp_serde::to_vec(&ck).map_err(std::io::Error::other)?;
    let tmp = tmp_path(path);
    std::fs::write(&tmp, &bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// The sibling temp path for the atomic write: `<path>.tmp`.
fn tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

/// Live-pump checkpoint cadence + destination, shared (behind an `Arc`) between
/// the pump task and [`Projections::checkpoint_now`].
#[derive(Debug)]
struct CheckpointCfg {
    /// The sidecar checkpoint file to (atomically) write.
    path:    PathBuf,
    /// Write a checkpoint after this many folded events.
    every_n: u64,
    /// ...or after this much wall time since the last checkpoint, whichever
    /// trips first (evaluated only when a batch lands).
    every_t: Duration,
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
/// Coarse cardinalities of a folded read model, from
/// [`Projections::cardinalities`] — the counts the `--rebuild` proof prints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cardinalities {
    /// Registered users folded.
    pub users:        usize,
    /// Posts folded (deleted ones included — the tombstone still folds).
    pub posts:        usize,
    /// Active follow edges across all `following` sets.
    pub follow_edges: usize,
    /// Active like edges across all `likes` sets.
    pub like_edges:   usize,
}

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
/// [`SubscribeBackend`], with optional checkpointed resume.
///
/// Cloneable is intentionally **not** derived: the value owns the live pump
/// task and aborts it on drop, so there is exactly one owner of the tail.
/// Share it behind an `Arc` if several handlers need it.
#[derive(Debug)]
pub struct Projections<B: Backend> {
    state:        Arc<RwLock<State>>,
    /// The read watermark: the number of global events applied, i.e. one past
    /// the highest global position folded in. `wait_for(p)` waits for this to
    /// exceed `p`.
    applied:      Arc<AtomicU64>,
    /// Pulsed after every batch the pump applies, so `wait_for` waiters wake.
    notify:       Arc<Notify>,
    /// Counts of records this projection could not decode or route, across
    /// both the catch-up replay and the live pump (`bn-3uu`). Held behind an
    /// `Arc` (not the `state` lock) so [`Projections::anomalies`] reads it
    /// without contending with the fold.
    anomalies:    Arc<ProjectionAnomalies>,
    /// The global position this instance resumed folding from: `0` for a full
    /// from-0 rebuild (including a discarded/absent checkpoint), or the
    /// checkpoint's watermark when it resumed. Exposed by
    /// [`Projections::resumed_from`] so a test can assert whether a rebuild or
    /// a resume happened.
    resumed_from: u64,
    /// The checkpoint destination + cadence, if this instance checkpoints.
    /// `None` for [`Projections::new`] (a pure from-0 rebuild that never
    /// persists). Shared with the pump task.
    checkpoint:   Option<Arc<CheckpointCfg>>,
    /// The live pump; aborted on drop.
    pump:         tokio::task::JoinHandle<()>,
    _backend:     PhantomData<B>,
}

impl<B: Backend> Drop for Projections<B> {
    fn drop(&mut self) { self.pump.abort(); }
}

impl<B: SubscribeBackend + Clone> Projections<B> {
    /// Build the read model by **replaying the whole log from position 0**,
    /// then spawn the live pump that follows it forward. This instance does
    /// **not** checkpoint — it is the pure, from-scratch rebuild the
    /// equivalence tests use as their baseline and the public "rebuild from 0"
    /// seam. Use [`with_checkpoint`](Self::with_checkpoint) for the operational
    /// path that resumes.
    ///
    /// Construction is a full rebuild: phase 1 folds all committed history
    /// synchronously (so every query issued immediately after `new` reflects
    /// it) and phase 2 tails the log live via [`EventStore::subscribe`].
    pub async fn new(store: &EventStore<B>) -> Self {
        Self::build(store, State::default(), 0, None).await
    }

    /// Build the read model, **resuming from a checkpoint** at `path` when one
    /// is present and valid, else falling back to a clean from-0 rebuild.
    ///
    /// On resume, only the log *suffix* committed since the checkpoint is
    /// replayed — startup does not re-fold the whole log. The checkpoint is
    /// discarded (and a full rebuild done) whenever it is missing, corrupt/
    /// truncated, header-invalid, of a mismatched [`PROJECTION_VERSION`], or —
    /// a safety guard — ahead of the store's current watermark (which would
    /// mean the state is inconsistent with the log). The instance then
    /// checkpoints during the live pump at the default cadence
    /// ([`CKPT_EVERY_N`] events / [`CKPT_EVERY_T`]) and on
    /// [`checkpoint_now`](Self::checkpoint_now).
    pub async fn with_checkpoint(
        store: &EventStore<B>,
        path: impl Into<PathBuf>,
    ) -> Self {
        Self::with_checkpoint_cadence(store, path, CKPT_EVERY_N, CKPT_EVERY_T)
            .await
    }

    /// [`with_checkpoint`](Self::with_checkpoint) with an explicit live-pump
    /// cadence — a checkpoint is written after `every_n` folded events or
    /// `every_t` elapsed since the last one, whichever trips first. Tests use a
    /// very large `every_n` to make checkpointing happen only on an explicit
    /// [`checkpoint_now`](Self::checkpoint_now).
    pub async fn with_checkpoint_cadence(
        store: &EventStore<B>,
        path: impl Into<PathBuf>,
        every_n: u64,
        every_t: Duration,
    ) -> Self {
        let path = path.into();
        let head = store.watermark().await.unwrap_or(0);
        // Resume only from a checkpoint that is valid AND not ahead of the
        // store's watermark — a checkpoint ahead of the log means the folded
        // state reflects positions the store no longer has (e.g. after a store
        // rollback), so it cannot be trusted: rebuild clean.
        let (state, from) = match Checkpoint::load(&path) {
            Some((applied, state)) if applied <= head => (state, applied),
            _ => (State::default(), 0),
        };
        let cfg = Arc::new(CheckpointCfg { path, every_n, every_t });
        Self::build(store, state, from, Some(cfg)).await
    }

    /// The shared construction core: seed `initial` state as current to global
    /// position `from`, synchronously catch up to the store's current
    /// watermark, then spawn the live pump.
    async fn build(
        store: &EventStore<B>,
        initial: State,
        from: u64,
        checkpoint: Option<Arc<CheckpointCfg>>,
    ) -> Self {
        let head = store.watermark().await.unwrap_or(0);
        let state = Arc::new(RwLock::new(initial));
        let applied = Arc::new(AtomicU64::new(from));
        let notify = Arc::new(Notify::new());
        let anomalies = Arc::new(ProjectionAnomalies::new());

        // One subscription serves both phases: catch-up reads committed history
        // page-by-page (never blocking while `position < head`), then the same
        // cursor is handed to the pump for the event-bounded live tail — no gap
        // between the two.
        let mut sub = store.subscribe(Some(from));

        // Phase 1: synchronous catch-up from `from` to the current watermark.
        {
            let mut st = state.write().await;
            while sub.position() < head {
                match sub.next_batch().await {
                    Ok(batch) => {
                        for rec in &batch {
                            st.apply_record(rec, &anomalies);
                        }
                    }
                    // A transient read error ends catch-up early; the live pump
                    // resumes from the same cursor and self-heals.
                    Err(_) => break,
                }
            }
        }
        applied.store(sub.position(), Ordering::Release);

        // Phase 2: spawn the live pump over the same subscription.
        let pump = tokio::spawn(pump_loop(
            sub,
            state.clone(),
            applied.clone(),
            notify.clone(),
            anomalies.clone(),
            checkpoint.clone(),
        ));

        Self {
            state,
            applied,
            notify,
            anomalies,
            resumed_from: from,
            checkpoint,
            pump,
            _backend: PhantomData,
        }
    }

    /// Persist a checkpoint **now**, atomically, current to whatever the pump
    /// has folded so far. A no-op (`Ok(())`) for an instance built without a
    /// checkpoint destination ([`Projections::new`]). Call this on a clean
    /// shutdown so the next boot resumes from the very last folded position
    /// rather than the last periodic cadence write.
    pub async fn checkpoint_now(&self) -> std::io::Result<()> {
        self.checkpoint_as(PROJECTION_VERSION).await
    }

    /// Test hook: persist a checkpoint stamped with an arbitrary
    /// `projection_version`, so a test can plant a *stale-version* checkpoint
    /// and prove the resume path discards it in favour of a from-0 rebuild.
    /// Not part of the supported surface.
    #[doc(hidden)]
    pub async fn checkpoint_now_as_version(
        &self,
        version: u32,
    ) -> std::io::Result<()> {
        self.checkpoint_as(version).await
    }

    async fn checkpoint_as(&self, version: u32) -> std::io::Result<()> {
        let Some(cfg) = &self.checkpoint else { return Ok(()) };
        let applied = self.applied.load(Ordering::Acquire);
        // Snapshot under a brief read lock, serialize + write outside it.
        let snap = self.state.read().await.clone();
        write_checkpoint(snap, applied, version, &cfg.path)
    }
}

impl<B: Backend> Projections<B> {
    /// The undecodable-payload / unroutable-stream / unknown-event-kind
    /// counters for records this projection has skipped, across both the
    /// initial catch-up replay and the live pump (`bn-3uu`). The liveness
    /// policy is unchanged — a skip is still a skip — but a schema drift or
    /// routing bug now shows up here (and, on each counter's first hit, as a
    /// one-time warning line) instead of vanishing silently. Not carried in
    /// the checkpoint (per-process observability, not domain state), so a
    /// resumed instance starts these at zero. See
    /// [`mess_store::ProjectionAnomalies`].
    #[must_use]
    pub fn anomalies(&self) -> ProjectionAnomaliesSnapshot {
        self.anomalies.snapshot()
    }

    /// The global position this instance resumed folding from: `0` for a full
    /// from-0 rebuild (a fresh store, or a discarded/absent/stale checkpoint),
    /// or the checkpoint's watermark when it resumed and replayed only the
    /// suffix. The test marker for "did we rebuild or resume?".
    #[must_use]
    pub fn resumed_from(&self) -> u64 { self.resumed_from }

    /// The current read watermark: one past the highest global position folded
    /// (equivalently, the count of events applied). This is exactly what a
    /// checkpoint written now would record as its `applied`.
    #[must_use]
    pub fn applied_position(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }

    /// A deterministic byte fingerprint of the current folded read-model state,
    /// for the `--rebuild` checkpoint-correctness proof (see
    /// [`crate::rebuild`]). Two projections that folded the same log — one
    /// rebuilt from position 0, one resumed from a checkpoint and caught up to
    /// the same head — produce **byte-identical** fingerprints. Canonicalized
    /// (keys and membership sets sorted) so the fingerprint depends only on
    /// folded content, never on `HashMap`/`HashSet` iteration order.
    pub async fn state_fingerprint(&self) -> Vec<u8> {
        self.state.read().await.canonical_bytes()
    }

    /// Coarse cardinalities of the folded read model — the counts the
    /// `--rebuild` report prints alongside PASS/FAIL. Cheap (map lengths, one
    /// pass over the crowd sets under a read lock).
    pub async fn cardinalities(&self) -> Cardinalities {
        let st = self.state.read().await;
        Cardinalities {
            users:        st.users.len(),
            posts:        st.posts.len(),
            follow_edges: st.following.values().map(HashSet::len).sum(),
            like_edges:   st.likes.values().map(HashSet::len).sum(),
        }
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

/// The live pump: pull the next committed batch from the subscription (parked
/// event-bounded on the store watermark while caught up — no poll loop), fold
/// each record, advance the watermark, pulse `wait_for` waiters, and write a
/// checkpoint when the cadence trips. Runs until the [`Projections`] is dropped
/// (which aborts it).
async fn pump_loop<B: SubscribeBackend>(
    mut sub: Subscription<B>,
    state: Arc<RwLock<State>>,
    applied: Arc<AtomicU64>,
    notify: Arc<Notify>,
    anomalies: Arc<ProjectionAnomalies>,
    checkpoint: Option<Arc<CheckpointCfg>>,
) {
    let mut since_ckpt: u64 = 0;
    let mut last_ckpt = Instant::now();
    loop {
        let batch = match sub.next_batch().await {
            Ok(b) => b,
            // Transient backend error: back off briefly (an error backoff, not
            // a steady-state poll) and retry from the same cursor.
            Err(_) => {
                tokio::time::sleep(PUMP_ERROR_BACKOFF).await;
                continue;
            }
        };
        {
            let mut st = state.write().await;
            for rec in &batch {
                st.apply_record(rec, &anomalies);
            }
        }
        let now_applied = sub.position();
        applied.store(now_applied, Ordering::Release);
        notify.notify_waiters();

        if let Some(cfg) = &checkpoint {
            since_ckpt += batch.len() as u64;
            if since_ckpt >= cfg.every_n || last_ckpt.elapsed() >= cfg.every_t {
                let snap = state.read().await.clone();
                if let Err(e) = write_checkpoint(
                    snap,
                    now_applied,
                    PROJECTION_VERSION,
                    &cfg.path,
                ) {
                    eprintln!(
                        "social projections: WARNING checkpoint write failed \
                         at position {now_applied}: {e}"
                    );
                }
                since_ckpt = 0;
                last_ckpt = Instant::now();
            }
        }
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
        // Block until the pump has *folded* past global `position` — i.e. the
        // applied watermark exceeds it. The pump advances only on a real
        // commit-notification wake from the subscription (it is parked on the
        // store watermark while caught up, never polling), so this barrier is
        // event-bounded: it resolves as soon as the write is folded, not after
        // a poll interval. Arm the `notified()` future *before* the final check
        // to avoid a lost wakeup between the load and the await.
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
