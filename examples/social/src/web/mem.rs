//! An in-memory, coherent read+write backend for the runnable demo binary.
//!
//! # Why this exists
//!
//! The web layer is *tested* against the seedable [`FakeReadModels`] plus a
//! programmable fake writer. But a runnable demo needs **read-your-writes
//! coherence** — post something and see it appear — which the append-only
//! `FakeReadModels` builders cannot provide at runtime, and which the *real*
//! projection tailer is the parallel bn-2xl bone (not depended on here).
//!
//! [`MemBackend`] bridges the gap without duplicating any domain rule or query:
//!
//! - **Writes** fold the real [`User`]/[`Post`] aggregates and run their real
//!   [`Decide`] impls, so every business rule (self-follow, delete-not-yours,
//!   double-like, …) is enforced exactly as in production. A rejection maps
//!   straight to [`WriteError`].
//! - **Reads** rebuild a [`FakeReadModels`] from the current folded state on
//!   each query and delegate — so all feed/profile/pagination logic is reused,
//!   never reimplemented.
//!
//! This is deliberately O(n)-per-read and single-locked: it is a demo, not the
//! production read model. Swapping it for the real `EventStore` + bn-2xl
//! projections is the final wiring step described in the bone.

use std::sync::Mutex;

use ident::Id;
use mess_core::Decide;

use crate::contracts::{
    FakeReadModels, PostView, ProfileView, ReadModels, TimelinePage,
    WriteError, WriteOps,
};
use crate::domain::post::{CreatePost, DeletePost, Like, Post, Unlike};
use crate::domain::user::{
    Follow, RegisterUser, SetDisplayName, Unfollow, User,
};

struct UserRow {
    id: Id,
    agg: User,
}

struct PostRow {
    id: Id,
    agg: Post,
    created_pos: u64,
}

#[derive(Default)]
struct Inner {
    users: Vec<UserRow>,
    posts: Vec<PostRow>,
    pos: u64,
}

/// A coherent in-memory backend implementing both [`ReadModels`] and
/// [`WriteOps`] for the demo binary. See the module docs.
#[derive(Default)]
pub struct MemBackend {
    inner: Mutex<Inner>,
}

impl MemBackend {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Inner {
    /// Assign and return the next global position for a freshly-appended event.
    fn next_pos(&mut self) -> u64 {
        self.pos += 1;
        self.pos
    }

    fn user(&self, id: Id) -> Option<&UserRow> {
        self.users.iter().find(|r| r.id == id)
    }
    fn user_mut(&mut self, id: Id) -> Option<&mut UserRow> {
        self.users.iter_mut().find(|r| r.id == id)
    }
    fn post_mut(&mut self, id: Id) -> Option<&mut PostRow> {
        self.posts.iter_mut().find(|r| r.id == id)
    }

    /// Rebuild a [`FakeReadModels`] snapshot from current folded state so every
    /// query reuses the contract's own logic. Posts are seeded in creation
    /// order so the fake's sequence matches the real feed order.
    fn snapshot(&self) -> FakeReadModels {
        let mut rm = FakeReadModels::new();
        for u in &self.users {
            rm = rm.with_user(u.id, &u.agg.handle, &u.agg.display_name);
        }
        let mut posts: Vec<&PostRow> = self.posts.iter().collect();
        posts.sort_by_key(|p| p.created_pos);
        for p in posts {
            let Some(author) = p.agg.author else { continue };
            rm = rm.with_post(p.id, author, &p.agg.body);
            for liker in &p.agg.likes {
                rm = rm.with_like(p.id, *liker);
            }
            if p.agg.deleted {
                rm = rm.with_deleted(p.id);
            }
        }
        for u in &self.users {
            for t in &u.agg.following {
                rm = rm.with_follow(u.id, *t);
            }
        }
        rm
    }
}

// --- reads: delegate to a fresh snapshot -----------------------------------

impl ReadModels for MemBackend {
    async fn home_timeline(
        &self,
        user: Id,
        cursor: Option<String>,
        limit: usize,
    ) -> TimelinePage {
        // Snapshot (and drop the guard) *before* awaiting: the `MutexGuard` is
        // not `Send`, so it must not be held across the `.await`.
        let snap = self.inner.lock().unwrap().snapshot();
        snap.home_timeline(user, cursor, limit).await
    }
    async fn user_posts(
        &self,
        handle: &str,
        cursor: Option<String>,
        limit: usize,
    ) -> TimelinePage {
        let snap = self.inner.lock().unwrap().snapshot();
        snap.user_posts(handle, cursor, limit).await
    }
    async fn firehose(
        &self,
        cursor: Option<String>,
        limit: usize,
    ) -> TimelinePage {
        let snap = self.inner.lock().unwrap().snapshot();
        snap.firehose(cursor, limit).await
    }
    async fn profile(
        &self,
        handle: &str,
        viewer: Option<Id>,
    ) -> Option<ProfileView> {
        let snap = self.inner.lock().unwrap().snapshot();
        snap.profile(handle, viewer).await
    }
    async fn post(&self, id: Id, viewer: Option<Id>) -> Option<PostView> {
        let snap = self.inner.lock().unwrap().snapshot();
        snap.post(id, viewer).await
    }
    async fn resolve(&self, handle: &str) -> Option<Id> {
        let g = self.inner.lock().unwrap();
        g.users.iter().find(|r| r.agg.handle == handle).map(|r| r.id)
    }
    async fn wait_for(&self, _position: u64) {
        // Synchronously consistent: a write mutates state in place.
    }
}

// --- writes: fold the real aggregates, run the real Decide -----------------

impl WriteOps for MemBackend {
    async fn register(
        &self,
        user: Id,
        handle: String,
        display_name: String,
    ) -> Result<u64, WriteError> {
        let mut g = self.inner.lock().unwrap();
        let existing = g.user(user).map(|r| r.agg.clone()).unwrap_or_default();
        let events = existing
            .decide(RegisterUser { handle, display_name })
            .map_err(WriteError::User)?;
        let mut agg = existing;
        let mut pos = g.pos;
        for e in &events {
            agg.apply(e);
            pos = g.next_pos();
        }
        match g.user_mut(user) {
            Some(row) => row.agg = agg,
            None => g.users.push(UserRow { id: user, agg }),
        }
        Ok(pos)
    }

    async fn set_display_name(
        &self,
        user: Id,
        display_name: String,
    ) -> Result<u64, WriteError> {
        let mut g = self.inner.lock().unwrap();
        let mut agg =
            g.user(user).map(|r| r.agg.clone()).unwrap_or_default();
        let events = agg
            .decide(SetDisplayName { display_name })
            .map_err(WriteError::User)?;
        let mut pos = g.pos;
        for e in &events {
            agg.apply(e);
            pos = g.next_pos();
        }
        if let Some(row) = g.user_mut(user) {
            row.agg = agg;
        }
        Ok(pos)
    }

    async fn follow(
        &self,
        follower: Id,
        target: Id,
    ) -> Result<u64, WriteError> {
        let mut g = self.inner.lock().unwrap();
        let mut agg =
            g.user(follower).map(|r| r.agg.clone()).unwrap_or_default();
        let events = agg
            .decide(Follow { follower, target })
            .map_err(WriteError::User)?;
        let mut pos = g.pos;
        for e in &events {
            agg.apply(e);
            pos = g.next_pos();
        }
        if let Some(row) = g.user_mut(follower) {
            row.agg = agg;
        }
        Ok(pos)
    }

    async fn unfollow(
        &self,
        follower: Id,
        target: Id,
    ) -> Result<u64, WriteError> {
        let mut g = self.inner.lock().unwrap();
        let mut agg =
            g.user(follower).map(|r| r.agg.clone()).unwrap_or_default();
        let events =
            agg.decide(Unfollow { target }).map_err(WriteError::User)?;
        let mut pos = g.pos;
        for e in &events {
            agg.apply(e);
            pos = g.next_pos();
        }
        if let Some(row) = g.user_mut(follower) {
            row.agg = agg;
        }
        Ok(pos)
    }

    async fn create_post(
        &self,
        post: Id,
        author: Id,
        body: String,
    ) -> Result<u64, WriteError> {
        let mut g = self.inner.lock().unwrap();
        let existing =
            g.post_mut(post).map(|r| r.agg.clone()).unwrap_or_default();
        let events = existing
            .decide(CreatePost { author, body })
            .map_err(WriteError::Post)?;
        let mut agg = existing;
        let mut pos = g.pos;
        for e in &events {
            agg.apply(e);
            pos = g.next_pos();
        }
        match g.post_mut(post) {
            Some(row) => row.agg = agg,
            None => g.posts.push(PostRow { id: post, agg, created_pos: pos }),
        }
        Ok(pos)
    }

    async fn delete_post(
        &self,
        post: Id,
        by: Id,
    ) -> Result<u64, WriteError> {
        self.mutate_post(post, |p| p.decide(DeletePost { by }))
    }

    async fn like(&self, post: Id, user: Id) -> Result<u64, WriteError> {
        self.mutate_post(post, |p| p.decide(Like { user }))
    }

    async fn unlike(&self, post: Id, user: Id) -> Result<u64, WriteError> {
        self.mutate_post(post, |p| p.decide(Unlike { user }))
    }
}

impl MemBackend {
    /// Shared body for post commands on an existing stream: fold, decide, apply.
    fn mutate_post(
        &self,
        post: Id,
        decide: impl FnOnce(
            &Post,
        )
            -> Result<Vec<crate::domain::post::PostEvent>, crate::domain::post::PostError>,
    ) -> Result<u64, WriteError> {
        let mut g = self.inner.lock().unwrap();
        let mut agg =
            g.post_mut(post).map(|r| r.agg.clone()).unwrap_or_default();
        let events = decide(&agg).map_err(WriteError::Post)?;
        let mut pos = g.pos;
        for e in &events {
            agg.apply(e);
            pos = g.next_pos();
        }
        if let Some(row) = g.post_mut(post) {
            row.agg = agg;
        }
        Ok(pos)
    }
}
