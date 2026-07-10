//! Tests for the [`contracts`] seam: the deterministic [`FakeReadModels`]
//! that the frontend bone builds its own tests against. These assert the
//! query shapes (viewer-relative fields, feed membership, pagination) that a
//! real projection-backed [`ReadModels`] must also satisfy.
//!
//! [`contracts`]: social::contracts
//! [`FakeReadModels`]: social::contracts::FakeReadModels
//! [`ReadModels`]: social::contracts::ReadModels

use ident::Id;
use social::contracts::{FakeReadModels, ReadModels};

/// Build a small world: alice follows bob (not carol); bob and carol each
/// post; alice likes bob's first post.
fn world() -> (FakeReadModels, Id, Id, Id) {
    let alice = Id::new();
    let bob = Id::new();
    let carol = Id::new();
    let rm = FakeReadModels::new()
        .with_user(alice, "alice", "Alice")
        .with_user(bob, "bob", "Bob")
        .with_user(carol, "carol", "Carol")
        .with_follow(alice, bob)
        .with_post("p1", bob, "bob first") // seq 1
        .with_post("p2", carol, "carol first") // seq 2
        .with_post("p3", bob, "bob second") // seq 3
        .with_like("p1", alice);
    (rm, alice, bob, carol)
}

#[tokio::test]
async fn home_timeline_shows_followed_authors_newest_first() {
    let (rm, alice, ..) = world();
    let page = rm.home_timeline(alice, None, 10).await;
    // alice follows bob (p1, p3) and sees her own posts (none). carol's p2 is
    // excluded. Newest first: p3 then p1.
    let ids: Vec<&str> = page.entries.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(ids, ["p3", "p1"]);
    assert!(page.next_cursor.is_none());
}

#[tokio::test]
async fn home_timeline_marks_liked_by_me() {
    let (rm, alice, ..) = world();
    let page = rm.home_timeline(alice, None, 10).await;
    let p1 = page.entries.iter().find(|e| e.id == "p1").unwrap();
    assert!(p1.liked_by_me);
    assert_eq!(p1.likes, 1);
    let p3 = page.entries.iter().find(|e| e.id == "p3").unwrap();
    assert!(!p3.liked_by_me);
}

#[tokio::test]
async fn firehose_shows_everything_newest_first() {
    let (rm, ..) = world();
    let page = rm.firehose(None, 10).await;
    let ids: Vec<&str> = page.entries.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(ids, ["p3", "p2", "p1"]);
}

#[tokio::test]
async fn firehose_paginates_by_cursor() {
    let (rm, ..) = world();
    let first = rm.firehose(None, 2).await;
    let ids: Vec<&str> = first.entries.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(ids, ["p3", "p2"]);
    let cursor = first.next_cursor.expect("more pages");
    let second = rm.firehose(Some(cursor), 2).await;
    let ids: Vec<&str> = second.entries.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(ids, ["p1"]);
    assert!(second.next_cursor.is_none());
}

#[tokio::test]
async fn user_posts_lists_only_that_author() {
    let (rm, ..) = world();
    let page = rm.user_posts("bob", None, 10).await;
    let ids: Vec<&str> = page.entries.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(ids, ["p3", "p1"]);
}

#[tokio::test]
async fn profile_counts_and_viewer_relation() {
    let (rm, alice, bob, carol) = world();
    // bob viewed by alice: alice follows bob.
    let bob_profile = rm.profile("bob", Some(alice)).await.unwrap();
    assert_eq!(bob_profile.handle, "bob");
    assert_eq!(bob_profile.post_count, 2);
    assert_eq!(bob_profile.follower_count, 1); // alice
    assert_eq!(bob_profile.following_count, 0);
    assert!(bob_profile.followed_by_me);
    // carol viewed by alice: alice does NOT follow carol.
    let carol_profile = rm.profile("carol", Some(alice)).await.unwrap();
    assert!(!carol_profile.followed_by_me);
    assert_eq!(carol_profile.follower_count, 0);
    // unknown handle -> None.
    assert!(rm.profile("nobody", Some(bob)).await.is_none());
    let _ = carol;
}

#[tokio::test]
async fn post_query_is_viewer_relative_and_hides_deleted() {
    let (rm, alice, bob, _carol) = world();
    let p1 = rm.post("p1", Some(alice)).await.unwrap();
    assert!(p1.liked_by_me);
    let p1_anon = rm.post("p1", None).await.unwrap();
    assert!(!p1_anon.liked_by_me);
    // Anonymous view of a post bob authored still shows author fields.
    assert_eq!(p1_anon.author_handle, "bob");
    let _ = bob;

    // A deleted post disappears from single-post and feed queries.
    let rm2 = rm.with_deleted("p1");
    assert!(rm2.post("p1", Some(alice)).await.is_none());
    let firehose = rm2.firehose(None, 10).await;
    assert!(firehose.entries.iter().all(|e| e.id != "p1"));
}

#[tokio::test]
async fn wait_for_is_a_noop_on_the_fake() {
    let (rm, alice, ..) = world();
    // The fake is synchronously consistent; wait_for returns immediately.
    rm.wait_for(999).await;
    let page = rm.home_timeline(alice, None, 10).await;
    assert_eq!(page.entries.len(), 2);
}
