//! Store-backed roundtrip: drive `register -> follow -> post -> like ->
//! delete` through a real [`EventStore`](mess_store::EventStore) via the
//! [`WriteOps`] wrapper, then assert the folded aggregates match. This is the
//! integration counterpart to the store-free `tests/gwt.rs` specs, and the
//! home of the two seam-level rules `decide` cannot express: self-follow
//! refusal and self-like allowance.

use mess_testkit::{SweepingTempDir, sweeping_temp_dir};
use social::Id;
use social::domain::follow::Follow;
use social::domain::like::Like;
use social::domain::post::Post;
use social::domain::user::User;
use social::store_backend::{Store, open_store};
use social::{
    FollowError, WriteError, WriteOps, follow_stream, like_stream, post_stream,
    user_stream,
};

/// A fresh warm-write store on a self-sweeping temp dir per test (the real-fs
/// TMPDIR rule). The guard is returned so the caller keeps it alive for the
/// duration of the test (dropped last, after the store closes).
fn fresh_store() -> (Store, SweepingTempDir) {
    let dir = sweeping_temp_dir("social-roundtrip");
    let store = open_store(dir.path()).expect("open store");
    (store, dir)
}

#[tokio::test]
async fn happy_path_roundtrip() -> Result<(), WriteError> {
    let (store, _dir) = fresh_store();
    let alice = Id::new();
    let bob = Id::new();
    let post = Id::new();

    store.register(alice, "alice".into(), "Alice".into()).await?;
    store.register(bob, "bob".into(), "Bob".into()).await?;
    store.follow(alice, bob).await?;
    store.create_post(post, bob, "hello".into()).await?;
    store.like(post, alice).await?;
    store.delete_post(post, bob).await?;

    // The entities keep only bounded state.
    let alice_state =
        store.load::<User>(&user_stream(alice)).await.unwrap().state;
    assert!(alice_state.registered);
    assert_eq!(alice_state.handle, "alice");

    let post_state =
        store.load::<Post>(&post_stream(post)).await.unwrap().state;
    assert!(post_state.created);
    assert!(post_state.deleted);
    assert_eq!(post_state.author, Some(bob));

    // The crowds live on their own relationship streams.
    let edge =
        store.load::<Follow>(&follow_stream(alice, bob)).await.unwrap().state;
    assert!(edge.following, "alice -> bob follow edge is active");

    let like =
        store.load::<Like>(&like_stream(post, alice)).await.unwrap().state;
    assert!(like.liked, "alice's like on the post is active");
    Ok(())
}

#[tokio::test]
async fn typed_rejections_surface_through_the_store() {
    let (store, _dir) = fresh_store();
    let alice = Id::new();
    let bob = Id::new();
    let post = Id::new();

    store.register(alice, "alice".into(), "Alice".into()).await.unwrap();
    store.register(bob, "bob".into(), "Bob".into()).await.unwrap();

    // Invalid handle rejected as a typed User rejection.
    match store.register(Id::new(), "Bad Handle".into(), "X".into()).await {
        Err(WriteError::User(social::UserError::InvalidHandle { .. })) => {}
        other => panic!("expected InvalidHandle, got {other:?}"),
    }

    // Double-follow rejected — now a Follow-relationship rejection.
    store.follow(alice, bob).await.unwrap();
    match store.follow(alice, bob).await {
        Err(WriteError::Follow(FollowError::AlreadyFollowing)) => {}
        other => panic!("expected AlreadyFollowing, got {other:?}"),
    }

    // Non-author delete rejected.
    store.create_post(post, bob, "hi".into()).await.unwrap();
    match store.delete_post(post, alice).await {
        Err(WriteError::Post(social::PostError::NotAuthor)) => {}
        other => panic!("expected NotAuthor, got {other:?}"),
    }
}

/// Self-follow is refused at the [`WriteOps`] seam — the relationship
/// aggregate cannot see both ids (they are the stream key), so the
/// well-formedness check lives here and never creates a `follow-X_X` stream.
#[tokio::test]
async fn self_follow_is_refused_at_the_seam() {
    let (store, _dir) = fresh_store();
    let alice = Id::new();
    store.register(alice, "alice".into(), "Alice".into()).await.unwrap();

    match store.follow(alice, alice).await {
        Err(WriteError::SelfFollow) => {}
        other => panic!("expected SelfFollow, got {other:?}"),
    }
    // No degenerate stream was ever written.
    let edge =
        store.load::<Follow>(&follow_stream(alice, alice)).await.unwrap();
    assert!(!edge.state.following, "no self-follow edge should exist");
}

/// Self-like IS allowed: an author liking their own post is a legitimate,
/// countable signal, so the like relationship has no self-check and the write
/// simply succeeds.
#[tokio::test]
async fn self_like_is_allowed() {
    let (store, _dir) = fresh_store();
    let author = Id::new();
    let post = Id::new();
    store.register(author, "author".into(), "Author".into()).await.unwrap();
    store.create_post(post, author, "mine".into()).await.unwrap();

    store.like(post, author).await.expect("author may like own post");
    let like =
        store.load::<Like>(&like_stream(post, author)).await.unwrap().state;
    assert!(like.liked);
}
