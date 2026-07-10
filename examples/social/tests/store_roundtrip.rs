//! Store-backed roundtrip: drive `register -> follow -> post -> like ->
//! delete` through a real [`EventStore`](mess_store::EventStore) via the
//! [`WriteOps`] wrapper, then assert the folded aggregates match. This is the
//! integration counterpart to the store-free `tests/gwt.rs` specs.

use ident::Id;
use mess_store::{EventStore, LogEngine};
use social::domain::post::Post;
use social::domain::user::User;
use social::{WriteError, WriteOps, post_stream, user_stream};

/// A fresh store in a unique temp dir per test invocation.
fn fresh_store() -> EventStore<LogEngine> {
    let dir = std::env::temp_dir().join(format!(
        "mess-social-test-{}-{}",
        std::process::id(),
        Id::new()
    ));
    EventStore::new(LogEngine::open(&dir).expect("open engine"))
}

#[tokio::test]
async fn happy_path_roundtrip() -> Result<(), WriteError> {
    let store = fresh_store();
    let alice = Id::new();
    let bob = Id::new();
    let post = Id::new();

    store.register(alice, "alice".into(), "Alice".into()).await?;
    store.register(bob, "bob".into(), "Bob".into()).await?;
    store.follow(alice, bob).await?;
    store.create_post(post, bob, "hello".into()).await?;
    store.like(post, alice).await?;
    store.delete_post(post, bob).await?;

    let alice_state =
        store.load::<User>(&user_stream(alice)).await.unwrap().state;
    assert!(alice_state.registered);
    assert_eq!(alice_state.handle, "alice");
    assert!(alice_state.following.contains(&bob));

    let post_state = store.load::<Post>(&post_stream(post)).await.unwrap().state;
    assert!(post_state.created);
    assert!(post_state.deleted);
    assert_eq!(post_state.author, Some(bob));
    assert!(post_state.likes.contains(&alice));
    Ok(())
}

#[tokio::test]
async fn typed_rejections_surface_through_the_store() {
    let store = fresh_store();
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

    // Double-follow rejected.
    store.follow(alice, bob).await.unwrap();
    match store.follow(alice, bob).await {
        Err(WriteError::User(social::UserError::AlreadyFollowing)) => {}
        other => panic!("expected AlreadyFollowing, got {other:?}"),
    }

    // Non-author delete rejected.
    store.create_post(post, bob, "hi".into()).await.unwrap();
    match store.delete_post(post, alice).await {
        Err(WriteError::Post(social::PostError::NotAuthor)) => {}
        other => panic!("expected NotAuthor, got {other:?}"),
    }
}
