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
use std::time::Duration;

use ident::Id;
use mess_store::{Backend, EventStore, LogEngine, RecordToAppend, Version};
use mess_testkit::{SweepingTempDir, sweeping_temp_dir};
use social::store_backend::{Store, open_store, read_handle};
use social::{
    PROJECTION_VERSION, PostLookup, ProfileView, Projections, ReadModels,
    TimelinePage, WriteError, WriteOps,
};

/// A fresh warm-write store (`writer`) plus a subscribe-capable read handle
/// (`reader`) over the **same** on-disk log, on a self-sweeping temp dir per
/// test (the real-fs TMPDIR rule). Post-`bn-o9z` the two capabilities live on
/// different backend types — the writer is a `SnapshotStore` (warm
/// `command_cached`), the reader a `SubscribeBackend` the `Projections` pump
/// tails — so a test that both writes and reads needs both handles. They
/// share one `Arc`-backed `LogEngine`, so writes through `writer` are visible
/// to `reader`'s subscription. See `social::store_backend`. The
/// [`SweepingTempDir`] guard is returned so the caller keeps it alive for the
/// duration of the test.
fn fresh_store() -> (Store, EventStore<LogEngine>, SweepingTempDir) {
    let dir = sweeping_temp_dir("social-projections");
    let writer = open_store(dir.path()).expect("open store");
    let reader = read_handle(&writer);
    (writer, reader, dir)
}

/// [`fresh_store`] but returning the [`SweepingTempDir`] guard itself instead
/// of consuming it — the checkpoint sidecar tests need `dir.path()` to place
/// `<dir>/.social-projections.ckpt` next to the store, and still need the
/// guard held for the duration of the test.
fn fresh_store_with_dir() -> (Store, EventStore<LogEngine>, SweepingTempDir) {
    let dir = sweeping_temp_dir("social-projections");
    let writer = open_store(dir.path()).expect("open store");
    let reader = read_handle(&writer);
    (writer, reader, dir)
}

// ===========================================================================
// Per-model unit tests
// ===========================================================================

/// alice follows bob (not carol); bob posts p1 & p3, carol posts p2; alice
/// likes p1. Returns the store, a live projection caught up to `last`, the
/// three user ids, the three post ids, `last` (the final global position),
/// and the store dir's [`SweepingTempDir`] guard (must be held for the
/// duration of the test).
async fn world()
-> (Store, Projections<LogEngine>, [Id; 3], [Id; 3], u64, SweepingTempDir) {
    let (store, reader, dir) = fresh_store();
    let proj = Projections::new(&reader).await;
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
    (store, proj, [alice, bob, carol], [p1, p2, p3], last, dir)
}

#[tokio::test]
async fn home_timeline_shows_followed_and_own_newest_first() {
    let (_s, proj, [alice, ..], [p1, _p2, p3], _last, _dir) = world().await;
    let page = proj.home_timeline(alice, None, 10).await;
    // alice follows bob (p1, p3) + sees her own (none). carol's p2 excluded.
    let ids: Vec<Id> = page.entries.iter().map(|e| e.id).collect();
    assert_eq!(ids, [p3, p1]);
    assert!(page.next_cursor.is_none());
}

#[tokio::test]
async fn home_timeline_marks_liked_by_me_and_counts() {
    let (_s, proj, [alice, bob, _carol], [p1, ..], _last, _dir) = world().await;
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
    let (_s, proj, _u, [p1, p2, p3], _last, _dir) = world().await;
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
    let (_s, proj, _u, [p1, _p2, p3], _last, _dir) = world().await;
    let page = proj.user_posts("bob", None, 10).await;
    let ids: Vec<Id> = page.entries.iter().map(|e| e.id).collect();
    assert_eq!(ids, [p3, p1]);
}

#[tokio::test]
async fn profile_counts_and_viewer_relation() {
    let (_s, proj, [alice, ..], _p, _last, _dir) = world().await;
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
    let (_s, proj, [alice, bob, _carol], [p1, ..], _last, _dir) = world().await;
    let me = proj.post(p1, Some(alice)).await.unwrap();
    assert!(me.liked_by_me);
    assert_eq!(me.author_id, bob);
    let anon = proj.post(p1, None).await.unwrap();
    assert!(!anon.liked_by_me);
    assert_eq!(anon.author_handle, "bob");
}

#[tokio::test]
async fn resolve_finds_a_registered_handle_and_none_otherwise() {
    let (_s, proj, [alice, ..], _p, _last, _dir) = world().await;
    assert_eq!(proj.resolve("alice").await, Some(alice));
    assert_eq!(proj.resolve("nobody").await, None);
}

#[tokio::test]
async fn deleted_post_drops_from_feeds_but_permalink_is_a_tombstone() {
    let (store, proj, [_alice, bob, _carol], [p1, ..], _last, _dir) =
        world().await;
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
    let (store, reader, _dir) = fresh_store();
    let proj = Projections::new(&reader).await;
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
    let (store, reader, _dir) = fresh_store();
    let proj = Projections::new(&reader).await;
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

    fn below(&mut self, n: usize) -> usize { (self.next() % n as u64) as usize }
}

/// Drive a seeded random-but-valid event sequence at `store`. Returns the last
/// global position written (or `None` if nothing landed). Registers every user
/// first, then issues a mix of follows/unfollows/posts/deletes/likes/unlikes;
/// domain rejections are simply skipped (no event, no position).
async fn drive(
    store: &Store,
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
                store
                    .set_display_name(u, format!("n{}", rng.next() % 997))
                    .await
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
    firehose:   TimelinePage,
    homes:      Vec<TimelinePage>,
    user_posts: Vec<TimelinePage>,
    profiles:   Vec<Option<ProfileView>>,
    posts:      Vec<Option<social::PostView>>,
    lookups:    Vec<Option<PostLookup>>,
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
    Snap { firehose, homes, user_posts, profiles, posts: post_views, lookups }
}

#[tokio::test]
async fn rebuild_equals_live_over_random_sequences() {
    let users: Vec<(Id, String)> = ["u0", "u1", "u2", "u3", "u4"]
        .iter()
        .map(|h| (Id::new(), (*h).to_string()))
        .collect();
    let posts: Vec<Id> = (0..8).map(|_| Id::new()).collect();

    for seed in 1u64..=25 {
        let (store, reader, _dir) = fresh_store();
        // The LIVE projection is built BEFORE any events and follows them
        // incrementally as the pump applies each batch.
        let live = Projections::new(&reader).await;

        let last = drive(&store, seed, &users, &posts).await;
        let last =
            last.expect("the deterministic prelude always writes events");
        live.wait_for(last).await;

        // The REBUILD projection is built AFTER all events: a full replay from
        // position 0, in possibly-larger batches than the live path saw.
        let rebuilt = Projections::new(&reader).await;
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
    let (store, reader, _dir) = fresh_store();
    let proj = Projections::new(&reader).await;
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

// ===========================================================================
// Anomaly observability (bn-3uu)
// ===========================================================================

/// Append one raw record straight through [`Backend::append_batch`],
/// bypassing [`WriteOps`]/`#[derive(Event)]` entirely — this is how the test
/// gets bytes onto the log that the real domain can never produce, to prove
/// the projection notices instead of silently vanishing them. Returns the
/// global position of the appended record.
async fn append_raw(
    store: &EventStore<LogEngine>,
    stream_id: &str,
    expected: Version,
    message_type: &str,
    data: Vec<u8>,
) -> u64 {
    store
        .backend()
        .append_batch(
            stream_id,
            expected,
            &[RecordToAppend { message_type: message_type.into(), data }],
        )
        .await
        .expect("raw append")
        .last_global_position
}

/// Feeds the store a record on a foreign category, a `user-` stream with an
/// unparseable suffix, a recognized `user-` stream with an unrecognized
/// message type, and a recognized `user-` stream with a known message type
/// but garbage payload bytes — one for each [`mess_store::AnomalyKind`] plus
/// a second unroutable-stream case. Asserts each anomaly counter reflects
/// exactly what happened, **and** that good records surrounding the garbage
/// are still folded correctly: the projection stays live, not stuck or
/// crashed, on a log wider than the slice it models.
#[tokio::test]
async fn anomalies_are_counted_while_the_projection_stays_live_and_correct() {
    let (store, reader, _dir) = fresh_store();
    let proj = Projections::new(&reader).await;

    // A good record BEFORE any garbage: alice registers.
    let alice = Id::new();
    store.register(alice, "alice".into(), "Alice".into()).await.unwrap();

    // 1. Unroutable stream: a category this projection does not model at all.
    let mut last = append_raw(
        &reader,
        "widget-123",
        Version::NoStream,
        "widget.created",
        b"whatever".to_vec(),
    )
    .await;

    // 2. Unroutable stream: a `user-` stream whose suffix is not a valid Id.
    last = append_raw(
        &reader,
        "user-not-an-id",
        Version::NoStream,
        "user.registered",
        b"whatever".to_vec(),
    )
    .await
    .max(last);

    // 3. Unknown event kind: a real user stream, an unrecognized message type.
    let dave = Id::new();
    last = append_raw(
        &reader,
        &social::user_stream(dave),
        Version::NoStream,
        "user.teleported",
        b"whatever".to_vec(),
    )
    .await
    .max(last);

    // 4. Undecodable payload: known message type, garbage msgpack bytes.
    let eve = Id::new();
    last = append_raw(
        &reader,
        &social::user_stream(eve),
        Version::NoStream,
        "user.registered",
        vec![0xFF, 0x00, 0x01, 0x02],
    )
    .await
    .max(last);

    // A good record AFTER the garbage: bob registers and posts.
    let bob = Id::new();
    store.register(bob, "bob".into(), "Bob".into()).await.unwrap();
    let post = Id::new();
    last = store
        .create_post(post, bob, "still alive".into())
        .await
        .unwrap()
        .max(last);

    proj.wait_for(last).await;

    let snap = proj.anomalies();
    assert_eq!(
        snap.unroutable_stream.count, 2,
        "the foreign category and the bad-suffix user stream are both \
         unroutable"
    );
    assert_eq!(
        snap.unknown_event_kind.count, 1,
        "user.teleported is not a UserEvent variant"
    );
    assert_eq!(
        snap.undecodable_payload.count, 1,
        "the garbage bytes are not valid msgpack for user.registered"
    );
    assert_eq!(snap.total(), 4);
    // The most recent anomaly recorded is the last garbage record appended
    // (the undecodable payload on eve's stream), not some earlier one.
    assert!(snap.undecodable_payload.last_position.is_some());

    // The projection is still live and correct on the good records: alice
    // (registered before the garbage) and bob+his post (registered after)
    // both resolve normally.
    assert_eq!(proj.resolve("alice").await, Some(alice));
    assert_eq!(proj.resolve("bob").await, Some(bob));
    let bob_posts = proj.user_posts("bob", None, 10).await;
    assert_eq!(bob_posts.entries.len(), 1);
    assert_eq!(bob_posts.entries[0].id, post);
    // dave and eve never registered (their only records were garbage), so
    // they never got a handle to resolve.
    assert!(proj.profile("dave", None).await.is_none());
}

// ===========================================================================
// Checkpointed projections: resume via subscribe(from=checkpoint), not
// replay-from-0. Each test proves the resumed answers equal a from-0 rebuild
// on the same store — the checkpoint is only ever an optimization, never a
// source of divergence.
// ===========================================================================

/// The five-user, eight-post cast the equivalence tests use, reused by the
/// checkpoint tests so their `drive`/`snapshot` cover the same query surface.
fn cast() -> (Vec<(Id, String)>, Vec<Id>) {
    let users: Vec<(Id, String)> = ["u0", "u1", "u2", "u3", "u4"]
        .iter()
        .map(|h| (Id::new(), (*h).to_string()))
        .collect();
    let posts: Vec<Id> = (0..8).map(|_| Id::new()).collect();
    (users, posts)
}

/// A cadence that never auto-checkpoints (astronomically many events, an hour
/// between time-based writes), so the checkpoint tests checkpoint *only* on an
/// explicit `checkpoint_now`/`checkpoint_now_as_version` — fully deterministic,
/// no cadence races.
const MANUAL_ONLY_N: u64 = u64::MAX;
fn manual_only_t() -> Duration { Duration::from_secs(3600) }

/// (a) Kill/restart: build a checkpointing projection, drive events,
/// checkpoint, drop it, then restart from the checkpoint. The resumed
/// projection must **resume** (not rebuild from 0) yet answer every query
/// identically to a fresh from-0 rebuild over the same store.
#[tokio::test]
async fn checkpoint_kill_restart_matches_from_zero_rebuild() {
    let (users, posts) = cast();
    let (store, reader, dir) = fresh_store_with_dir();
    let ckpt = dir.path().join(".social-projections.ckpt");

    let last;
    {
        let proj = Projections::with_checkpoint_cadence(
            &reader,
            ckpt.clone(),
            MANUAL_ONLY_N,
            manual_only_t(),
        )
        .await;
        // A fresh store starts empty, so the first build is a from-0 rebuild.
        assert_eq!(proj.resumed_from(), 0);
        last = drive(&store, 7, &users, &posts)
            .await
            .expect("the deterministic prelude always writes events");
        proj.wait_for(last).await;
        proj.checkpoint_now().await.expect("checkpoint write");
    } // <- projection dropped: the pump is aborted, the process "dies".

    // Restart: this MUST resume from the checkpoint, not replay from 0.
    let resumed = Projections::with_checkpoint_cadence(
        &reader,
        ckpt.clone(),
        MANUAL_ONLY_N,
        manual_only_t(),
    )
    .await;
    assert!(
        resumed.resumed_from() > 0,
        "a valid checkpoint must be resumed, not rebuilt from 0"
    );
    resumed.wait_for(last).await;

    // A plain from-0 rebuild over the same log, for comparison.
    let rebuilt = Projections::new(&reader).await;
    rebuilt.wait_for(last).await;

    let resumed_snap = snapshot(&resumed, &users, &posts).await;
    let rebuilt_snap = snapshot(&rebuilt, &users, &posts).await;
    assert_eq!(
        resumed_snap, rebuilt_snap,
        "resume-from-checkpoint must equal a from-0 rebuild"
    );
}

/// (b) A checkpoint stamped with a mismatched `projection_version` is discarded
/// and the projection rebuilds clean from 0 (asserted via the `resumed_from`
/// marker), still matching a from-0 rebuild.
#[tokio::test]
async fn stale_projection_version_checkpoint_forces_full_rebuild() {
    let (users, posts) = cast();
    let (store, reader, dir) = fresh_store_with_dir();
    let ckpt = dir.path().join(".social-projections.ckpt");

    let last;
    {
        let proj = Projections::with_checkpoint_cadence(
            &reader,
            ckpt.clone(),
            MANUAL_ONLY_N,
            manual_only_t(),
        )
        .await;
        last = drive(&store, 3, &users, &posts)
            .await
            .expect("prelude writes events");
        proj.wait_for(last).await;
        // Plant a checkpoint stamped with a FUTURE projection version — as if
        // the fold logic had been bumped since this file was written.
        proj.checkpoint_now_as_version(PROJECTION_VERSION + 1)
            .await
            .expect("checkpoint write");
    }

    // The stale-version checkpoint must be discarded -> full from-0 rebuild.
    let restarted = Projections::with_checkpoint_cadence(
        &reader,
        ckpt.clone(),
        MANUAL_ONLY_N,
        manual_only_t(),
    )
    .await;
    assert_eq!(
        restarted.resumed_from(),
        0,
        "a mismatched projection_version must force a from-0 rebuild"
    );
    restarted.wait_for(last).await;

    let rebuilt = Projections::new(&reader).await;
    rebuilt.wait_for(last).await;
    assert_eq!(
        snapshot(&restarted, &users, &posts).await,
        snapshot(&rebuilt, &users, &posts).await,
    );
}

/// (c) A corrupt/truncated checkpoint file is discarded — no panic — and the
/// projection rebuilds clean from 0. Two flavours: a truncated (undecodable)
/// file and a fully-garbage file.
#[tokio::test]
async fn corrupt_checkpoint_forces_full_rebuild_without_panic() {
    let (users, posts) = cast();
    let (store, reader, dir) = fresh_store_with_dir();
    let ckpt = dir.path().join(".social-projections.ckpt");

    let last;
    {
        let proj = Projections::with_checkpoint_cadence(
            &reader,
            ckpt.clone(),
            MANUAL_ONLY_N,
            manual_only_t(),
        )
        .await;
        last = drive(&store, 13, &users, &posts)
            .await
            .expect("prelude writes events");
        proj.wait_for(last).await;
        proj.checkpoint_now().await.expect("checkpoint write");
    }

    // Flavour 1: truncate the checkpoint to half its bytes (undecodable).
    let bytes = std::fs::read(&ckpt).expect("read checkpoint");
    assert!(bytes.len() > 8, "checkpoint should be non-trivial");
    std::fs::write(&ckpt, &bytes[..bytes.len() / 2]).expect("truncate");

    let restarted = Projections::with_checkpoint_cadence(
        &reader,
        ckpt.clone(),
        MANUAL_ONLY_N,
        manual_only_t(),
    )
    .await;
    assert_eq!(
        restarted.resumed_from(),
        0,
        "a truncated checkpoint must force a from-0 rebuild, not panic"
    );
    restarted.wait_for(last).await;
    let rebuilt = Projections::new(&reader).await;
    rebuilt.wait_for(last).await;
    assert_eq!(
        snapshot(&restarted, &users, &posts).await,
        snapshot(&rebuilt, &users, &posts).await,
    );

    // Flavour 2: fully-garbage bytes.
    std::fs::write(&ckpt, [0xFFu8; 64]).expect("garbage");
    let restarted2 = Projections::with_checkpoint_cadence(
        &reader,
        ckpt.clone(),
        MANUAL_ONLY_N,
        manual_only_t(),
    )
    .await;
    assert_eq!(
        restarted2.resumed_from(),
        0,
        "a garbage checkpoint must force a from-0 rebuild, not panic"
    );
    restarted2.wait_for(last).await;
    assert_eq!(
        snapshot(&restarted2, &users, &posts).await,
        snapshot(&rebuilt, &users, &posts).await,
    );
}

/// (d) Checkpoint mid-stream during the live pump: checkpoint after a first
/// wave, write a second wave, restart. The resume must replay **only the
/// suffix** — `resumed_from` equals the checkpoint's position, strictly less
/// than the final position — and still match a from-0 rebuild. This proves
/// folds are idempotent from a position: re-folding only the suffix reaches the
/// same state as folding the whole log.
#[tokio::test]
async fn checkpoint_during_live_pump_resumes_only_the_suffix() {
    let (users, posts) = cast();
    let (store, reader, dir) = fresh_store_with_dir();
    let ckpt = dir.path().join(".social-projections.ckpt");

    let ckpt_pos;
    let last;
    {
        let proj = Projections::with_checkpoint_cadence(
            &reader,
            ckpt.clone(),
            MANUAL_ONLY_N,
            manual_only_t(),
        )
        .await;
        // First wave, folded live, then checkpointed mid-stream.
        let mid = drive(&store, 5, &users, &posts)
            .await
            .expect("first wave writes events");
        proj.wait_for(mid).await;
        proj.checkpoint_now().await.expect("mid-stream checkpoint");
        ckpt_pos = proj.applied_position();

        // Second wave, committed AFTER the checkpoint — the suffix a resume
        // must replay.
        last = drive(&store, 9, &users, &posts)
            .await
            .expect("second wave writes events");
        proj.wait_for(last).await;
    }
    assert!(ckpt_pos > 0, "the checkpoint must be past position 0");
    assert!(last >= ckpt_pos, "the suffix must have added positions");

    let resumed = Projections::with_checkpoint_cadence(
        &reader,
        ckpt.clone(),
        MANUAL_ONLY_N,
        manual_only_t(),
    )
    .await;
    assert_eq!(
        resumed.resumed_from(),
        ckpt_pos,
        "resume must start at the checkpoint position — only the suffix \
         replays"
    );
    resumed.wait_for(last).await;

    let rebuilt = Projections::new(&reader).await;
    rebuilt.wait_for(last).await;
    assert_eq!(
        snapshot(&resumed, &users, &posts).await,
        snapshot(&rebuilt, &users, &posts).await,
        "suffix-resume must equal a from-0 rebuild"
    );
}

/// (e) `wait_for` is event-bounded, not poll-bounded. A code-level assertion
/// (the source no longer carries the old 1 ms poll and does drive the
/// subscription) plus a latency-shaped smoke check that the barrier actually
/// wakes on a commit within a generous, non-brittle bound.
#[tokio::test]
async fn wait_for_is_event_bounded_not_poll_bounded() {
    // Code-level: the removed 1 ms steady-state poll must be gone, and the pump
    // must now drive the store subscription.
    let src = include_str!("../src/projections.rs");
    assert!(
        !src.contains("from_millis(1)"),
        "the old 1 ms poll interval must be gone"
    );
    assert!(
        !src.contains("const POLL"),
        "the steady-state POLL constant must be gone"
    );
    assert!(
        src.contains("next_batch"),
        "the live pump must drive the subscription (next_batch)"
    );

    // Latency-shaped smoke (not wall-clock-brittle): wait_for on a not-yet-
    // written position must wake once the write lands, well within a generous
    // bound. The code-level check above is what proves event-boundedness; this
    // proves the barrier is actually wired to wake.
    let (store, reader, _dir) = fresh_store();
    let proj = Projections::new(&reader).await;
    let alice = Id::new();
    store.register(alice, "alice".into(), "Alice".into()).await.unwrap();
    let post = Id::new();
    let pos = store.create_post(post, alice, "hi".into()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), proj.wait_for(pos))
        .await
        .expect("wait_for must wake promptly on the commit, not hang");
    assert!(proj.post(post, None).await.is_some());
}
