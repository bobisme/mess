//! The social-feed domain, run end-to-end through the mess v1 API.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example social
//! ```
//!
//! See `src/lib.rs` for the domain (events, aggregate, commands) and
//! `tests/gwt.rs` for the same domain exercised store-free through
//! `mess-testkit`.

use ident::Id;
use mess_core::CommandError;
use mess_store::{EventStore, LogEngine};
use social::{HideByModerator, HideByPoster, Post, PostError, Publish};

#[tokio::main]
async fn main() {
    // `EventStore` over the composed production engine (`LogEngine`:
    // `mess-log` + `mess-index`), the default backend — see `examples/bank`'s
    // `examples/bank.rs` for the full explanation. The interim in-memory
    // backend is behind mess-store's `mock` feature; nothing below changes.
    let dir = std::env::temp_dir().join(format!("mess-social-{}", std::process::id()));
    let store = EventStore::new(LogEngine::open(&dir).expect("open engine"));

    let alice = Id::new();

    // A stream id is just the string key a caller chooses to identify one
    // post's history — there is no separate identity type standing between
    // a caller and that key.
    let post_stream = format!("post-{}", Id::new());

    store
        .command::<Post, _>(
            &post_stream,
            Publish {
                poster_id: alice,
                body: "here is some stupid post".into(),
            },
        )
        .await
        .expect("publish post");
    println!("alice published a post on stream {post_stream:?}");

    let loaded = store.load::<Post>(&post_stream).await.expect("load post");
    println!(
        "post state: poster={:?} body={:?} status={:?}",
        loaded.state.poster_id, loaded.state.body, loaded.state.status
    );

    // Authorization is a business rule the aggregate itself enforces: a
    // stranger cannot hide alice's post. `CommandError::Domain` carries
    // `Post`'s own `PostError`, not a generic string.
    let stranger = Id::new();
    match store
        .command::<Post, _>(&post_stream, HideByPoster { requester: stranger })
        .await
    {
        Err(CommandError::Domain(PostError::NotYourPost)) => {
            println!("a stranger cannot hide alice's post (as expected)");
        }
        other => panic!("expected NotYourPost, got {other:?}"),
    }

    // The original poster can hide their own post.
    store
        .command::<Post, _>(&post_stream, HideByPoster { requester: alice })
        .await
        .expect("poster hides their own post");
    let loaded = store.load::<Post>(&post_stream).await.expect("load post");
    println!("after self-hide: status={:?}", loaded.state.status);

    // A second post, this time moderated away regardless of author.
    let second_stream = format!("post-{}", Id::new());
    store
        .command::<Post, _>(
            &second_stream,
            Publish { poster_id: alice, body: "a spammy post".into() },
        )
        .await
        .expect("publish second post");
    store
        .command::<Post, _>(&second_stream, HideByModerator)
        .await
        .expect("moderator hides the post");
    let loaded = store.load::<Post>(&second_stream).await.expect("load post");
    println!(
        "moderated post: status={:?} (replayed {} events)",
        loaded.state.status, loaded.events_replayed
    );
    assert_eq!(loaded.state.status, social::PostStatus::HiddenByModerator);

    println!("social example OK");
}
