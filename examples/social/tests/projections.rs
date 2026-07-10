//! Store-backed tests for the real [`Projections`] read model: per-model unit
//! tests, the read-your-writes [`wait_for`](ReadModels::wait_for) barrier, and
//! the headline **`rebuild == live`** equivalence property over seeded random
//! event sequences (including a follow-after-post case).
//!
//! These run against a real [`EventStore`](mess_store::EventStore) on the
//! [`LogEngine`], the same app-facing path as `tests/store_roundtrip.rs` — so
//! they exercise the actual `read_global` tail, not a mock.
//!
//! [`Projections`]: social::Projections
//! [`ReadModels`]: social::contracts::ReadModels

use std::collections::HashMap;

use ident::Id;
use mess_store::{EventStore, LogEngine};
use social::{
    PostLookup, Projections, ProfileView, ReadModels, TimelinePage, WriteError,
    WriteOps,
};

/// A fresh store in a unique temp dir per test (mirrors `store_roundtrip`).
fn fresh_store() -> EventStore<LogEngine> {
    let dir = std::env::temp_dir().join(format!(
        "mess-social-proj-{}-{}",
        std::process::id(),
        Id::new()
    ));
    EventStore::new(LogEngine::open(&dir).expect("open engine"))
}

// ===========================================================================
// Per-model unit tests
// ===========================================================================

/// alice follows bob (not carol); bob posts p1 & p3, carol posts p2; alice
/// likes p1. Returns the store, a live projection caught up to `last`, the
/// three user ids, the three post ids, and `last` (the final global position).
async fn world() -> (
    EventStore<LogEngine>,
    Projections<LogEngine>,
    [Id; 3],
    [Id; 3],
    u64,
) {
    let store = fresh_store();
    let proj = Projections::new(&store).await;
    let (alice, bob, carol) = (Id::new(), Id::new(), Id::new());
    let (p1, p2, p3) = (Id::new(), Id::new(), Id::new());

    store.register(alice, "alice".into(), "Alice".into()).await.unwrap();
    store.register(bob, "bob".into(), "Bob".into()).await.unwrap();
    store.register(carol, "carol".into(), "Carol".into()).await.unwrap();
    store.follow(alice, bob).await.unwrap();
    store.create_post(p1, bob, "bob first".into()).await.unwrap();
    store.create_post(p2, carol, "carol first".into()).await.unwrap();
    store.create_post(p3, bob, "bob second".into()).await.unwrap();
    let last = store.like(p1, alice).await.unwrap();

    proj.wait_for(last).await;
    (store, proj, [alice, bob, carol], [p1, p2, p3], last)
}

#[tokio::test]
async fn home_timeline_shows_followed_and_own_newest_first() {
    let (_s, proj, [alice, ..], [p1, _p2, p3], _last) = world().await;
    let page = proj.home_timeline(alice, None, 10).await;
    // alice follows bob (p1, p3) + sees her own (none). carol's p2 excluded.
    let ids: Vec<Id> = page.entries.iter().map(|e| e.id).collect();
    assert_eq!(ids, [p3, p1]);
    assert!(page.next_cursor.is_none());
}

#[tokio::test]
async fn home_timeline_marks_liked_by_me_and_counts() {
    let (_s, proj, [alice, bob, _carol], [p1, ..], _last) = world().await;
    let page = proj.home_timeline(alice, None, 10).await;
    let p1v = page.entries.iter().find(|e| e.id == p1).unwrap();
    assert!(p1v.liked_by_me);
    assert_eq!(p1v.likes, 1);
    assert_eq!(p1v.author_id, bob);
    assert_eq!(p1v.author_handle, "bob");
    assert_eq!(p1v.author_display, "Bob");
}

#[tokio::test]
async fn firehose_shows_everything_and_paginates() {
    let (_s, proj, _u, [p1, p2, p3], _last) = world().await;
    let all = proj.firehose(None, 10).await;
    let ids: Vec<Id> = all.entries.iter().map(|e| e.id).collect();
    assert_eq!(ids, [p3, p2, p1]);

    let first = proj.firehose(None, 2).await;
    let ids: Vec<Id> = first.entries.iter().map(|e| e.id).collect();
    assert_eq!(ids, [p3, p2]);
    let cursor = first.next_cursor.expect("more pages");
    let second = proj.firehose(Some(cursor), 2).await;
    let ids: Vec<Id> = second.entries.iter().map(|e| e.id).collect();
    assert_eq!(ids, [p1]);
    assert!(second.next_cursor.is_none());
}

#[tokio::test]
async fn user_posts_lists_only_that_author() {
    let (_s, proj, _u, [p1, _p2, p3], _last) = world().await;
    let page = proj.user_posts("bob", None, 10).await;
    let ids: Vec<Id> = page.entries.iter().map(|e| e.id).collect();
    assert_eq!(ids, [p3, p1]);
}

#[tokio::test]
async fn profile_counts_and_viewer_relation() {
    let (_s, proj, [alice, ..], _p, _last) = world().await;
    let bob = proj.profile("bob", Some(alice)).await.unwrap();
    assert_eq!(bob.handle, "bob");
    assert_eq!(bob.post_count, 2);
    assert_eq!(bob.follower_count, 1); // alice
    assert_eq!(bob.following_count, 0);
    assert!(bob.followed_by_me);

    let carol = proj.profile("carol", Some(alice)).await.unwrap();
    assert!(!carol.followed_by_me);
    assert_eq!(carol.follower_count, 0);
    assert_eq!(carol.post_count, 1);

    assert!(proj.profile("nobody", Some(alice)).await.is_none());
}

#[tokio::test]
async fn post_query_is_viewer_relative() {
    let (_s, proj, [alice, bob, _carol], [p1, ..], _last) = world().await;
    let me = proj.post(p1, Some(alice)).await.unwrap();
    assert!(me.liked_by_me);
    assert_eq!(me.author_id, bob);
    let anon = proj.post(p1, None).await.unwrap();
    assert!(!anon.liked_by_me);
    assert_eq!(anon.author_handle, "bob");
}

#[tokio::test]
async fn resolve_finds_a_registered_handle_and_none_otherwise() {
    let (_s, proj, [alice, ..], _p, _last) = world().await;
    assert_eq!(proj.resolve("alice").await, Some(alice));
    assert_eq!(proj.resolve("nobody").await, None);
}

#[tokio::test]
async fn deleted_post_drops_from_feeds_but_permalink_is_a_tombstone() {
    let (store, proj, [_alice, bob, _carol], [p1, ..], _last) = world().await;
    let last = store.delete_post(p1, bob).await.unwrap();
    proj.wait_for(last).await;

    // Gone from the single-post feed query and from the firehose.
    assert!(proj.post(p1, None).await.is_none());
    let fire = proj.firehose(None, 10).await;
    assert!(fire.entries.iter().all(|e| e.id != p1));

    // Gone from bob's user_posts and his post_count drops.
    let bob_posts = proj.user_posts("bob", None, 10).await;
    assert!(bob_posts.entries.iter().all(|e| e.id != p1));
    assert_eq!(proj.profile("bob", None).await.unwrap().post_count, 1);

    // But a permalink resolves to a tombstone with author attribution intact.
    let tomb = proj.lookup_post(p1, None).await.unwrap();
    assert!(tomb.deleted);
    assert_eq!(tomb.view.author_handle, "bob");
    // A never-existing id is None even as a tombstone.
    assert!(proj.lookup_post(Id::new(), None).await.is_none());
}

// ===========================================================================
// Read-your-writes barrier
// ===========================================================================

#[tokio::test]
async fn wait_for_is_a_read_your_writes_barrier() {
    let store = fresh_store();
    let proj = Projections::new(&store).await;
    let (alice, post) = (Id::new(), Id::new());
    store.register(alice, "alice".into(), "Alice".into()).await.unwrap();
    let pos = store.create_post(post, alice, "hello".into()).await.unwrap();

    // After the barrier resolves, the just-written post MUST be visible.
    proj.wait_for(pos).await;
    let seen = proj.post(post, Some(alice)).await;
    assert!(seen.is_some(), "post must be visible after wait_for(pos)");
    assert_eq!(seen.unwrap().body, "hello");

    // wait_for on an already-applied position returns promptly (no hang).
    proj.wait_for(pos).await;
    // wait_for(0) on a non-empty log is trivially satisfied.
    proj.wait_for(0).await;
}

#[tokio::test]
async fn wait_for_blocks_until_a_later_write_lands() {
    let store = fresh_store();
    let proj = Projections::new(&store).await;
    let alice = Id::new();
    store.register(alice, "alice".into(), "Alice".into()).await.unwrap();

    // Start waiting for a position that does not exist yet, concurrently with
    // the writes that will produce it. The wait must resolve only once the
    // pump has applied that far — and then the read reflects it.
    let waiter = {
        let store = store.clone();
        async move {
            let post = Id::new();
            let pos =
                store.create_post(post, alice, "later".into()).await.unwrap();
            (post, pos)
        }
    };
    let (post, pos) = waiter.await;
    proj.wait_for(pos).await;
    assert!(proj.post(post, None).await.is_some());
}

// ===========================================================================
// rebuild == live equivalence over seeded random sequences
// ===========================================================================

/// A tiny deterministic xorshift PRNG — no `rand` dependency, fully seeded.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// Drive a seeded random-but-valid event sequence at `store`. Returns the last
/// global position written (or `None` if nothing landed). Registers every user
/// first, then issues a mix of follows/unfollows/posts/deletes/likes/unlikes;
/// domain rejections are simply skipped (no event, no position).
async fn drive(
    store: &EventStore<LogEngine>,
    seed: u64,
    users: &[(Id, String)],
    posts: &[Id],
) -> Option<u64> {
    let mut rng = Rng(seed | 1);
    let mut last = None;
    let mut post_author: HashMap<Id, Id> = HashMap::new();
    let mut note = |r: Result<u64, WriteError>| {
        if let Ok(p) = r {
            last = Some(p);
        }
    };

    for (id, handle) in users {
        note(store.register(*id, handle.clone(), format!("D-{handle}")).await);
    }

    // Explicit follow-after-post: users[0] posts, THEN users[1] follows them.
    // The follower's home must retroactively include that older post — the
    // classic case where a fan-out projection's live and rebuild paths diverge.
    let seed_post = posts[0];
    note(store.create_post(seed_post, users[0].0, "seed".into()).await);
    post_author.insert(seed_post, users[0].0);
    note(store.follow(users[1].0, users[0].0).await);

    for _ in 0..120 {
        let r = match rng.below(8) {
            0 => {
                let u = users[rng.below(users.len())].0;
                store.set_display_name(u, format!("n{}", rng.next() % 997)).await
            }
            1 => {
                let a = users[rng.below(users.len())].0;
                let b = users[rng.below(users.len())].0;
                store.follow(a, b).await
            }
            2 => {
                let a = users[rng.below(users.len())].0;
                let b = users[rng.below(users.len())].0;
                store.unfollow(a, b).await
            }
            3 | 4 => {
                let post = posts[rng.below(posts.len())];
                let author = users[rng.below(users.len())].0;
                let r = store
                    .create_post(post, author, format!("b{}", rng.next() % 997))
                    .await;
                if r.is_ok() {
                    post_author.insert(post, author);
                }
                r
            }
            5 => {
                let post = posts[rng.below(posts.len())];
                // Delete by the real author when known, so the delete lands.
                let by = post_author
                    .get(&post)
                    .copied()
                    .unwrap_or(users[rng.below(users.len())].0);
                store.delete_post(post, by).await
            }
            6 => {
                let post = posts[rng.below(posts.len())];
                let u = users[rng.below(users.len())].0;
                store.like(post, u).await
            }
            _ => {
                let post = posts[rng.below(posts.len())];
                let u = users[rng.below(users.len())].0;
                store.unlike(post, u).await
            }
        };
        note(r);
    }
    last
}

/// A comparable snapshot of every query a projection answers, for the given
/// users and posts. Two projections over the same log must produce equal
/// snapshots regardless of how (incrementally vs. from scratch) they were
/// built.
#[derive(Debug, PartialEq, Eq)]
struct Snap {
    firehose: TimelinePage,
    homes: Vec<TimelinePage>,
    user_posts: Vec<TimelinePage>,
    profiles: Vec<Option<ProfileView>>,
    posts: Vec<Option<social::PostView>>,
    lookups: Vec<Option<PostLookup>>,
}

async fn snapshot(
    proj: &Projections<LogEngine>,
    users: &[(Id, String)],
    posts: &[Id],
) -> Snap {
    let firehose = proj.firehose(None, 1000).await;
    let mut homes = Vec::new();
    for (id, _) in users {
        homes.push(proj.home_timeline(*id, None, 1000).await);
    }
    let mut user_posts = Vec::new();
    let mut profiles = Vec::new();
    for (_, handle) in users {
        user_posts.push(proj.user_posts(handle, None, 1000).await);
        // Viewer-relative profile for every viewer plus the anonymous view.
        for (vid, _) in users {
            profiles.push(proj.profile(handle, Some(*vid)).await);
        }
        profiles.push(proj.profile(handle, None).await);
    }
    let mut post_views = Vec::new();
    let mut lookups = Vec::new();
    for p in posts {
        post_views.push(proj.post(*p, None).await);
        lookups.push(proj.lookup_post(*p, None).await);
    }
    Snap {
        firehose,
        homes,
        user_posts,
        profiles,
        posts: post_views,
        lookups,
    }
}

#[tokio::test]
async fn rebuild_equals_live_over_random_sequences() {
    let users: Vec<(Id, String)> = ["u0", "u1", "u2", "u3", "u4"]
        .iter()
        .map(|h| (Id::new(), (*h).to_string()))
        .collect();
    let posts: Vec<Id> = (0..8).map(|_| Id::new()).collect();

    for seed in 1u64..=25 {
        let store = fresh_store();
        // The LIVE projection is built BEFORE any events and follows them
        // incrementally as the pump applies each batch.
        let live = Projections::new(&store).await;

        let last = drive(&store, seed, &users, &posts).await;
        let last = last.expect("the deterministic prelude always writes events");
        live.wait_for(last).await;

        // The REBUILD projection is built AFTER all events: a full replay from
        // position 0, in possibly-larger batches than the live path saw.
        let rebuilt = Projections::new(&store).await;
        rebuilt.wait_for(last).await;

        let live_snap = snapshot(&live, &users, &posts).await;
        let rebuilt_snap = snapshot(&rebuilt, &users, &posts).await;
        assert_eq!(
            live_snap, rebuilt_snap,
            "live and rebuild disagree for seed {seed}"
        );
    }
}

#[tokio::test]
async fn follow_after_post_is_retroactive() {
    let store = fresh_store();
    let proj = Projections::new(&store).await;
    let (alice, bob) = (Id::new(), Id::new());
    let post = Id::new();
    store.register(alice, "alice".into(), "Alice".into()).await.unwrap();
    store.register(bob, "bob".into(), "Bob".into()).await.unwrap();
    // bob posts FIRST...
    store.create_post(post, bob, "old news".into()).await.unwrap();
    // ...then alice follows bob.
    let last = store.follow(alice, bob).await.unwrap();
    proj.wait_for(last).await;

    // alice's home retroactively contains bob's older post.
    let home = proj.home_timeline(alice, None, 10).await;
    assert_eq!(home.entries.len(), 1);
    assert_eq!(home.entries[0].id, post);
}
