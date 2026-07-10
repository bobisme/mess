//! The social-feed domain, run end-to-end through the mess v1 API:
//! register two users, wire a follow, post, like, and delete — every write
//! going through the [`WriteOps`] wrapper onto a real
//! [`EventStore`](mess_store::EventStore), then read the folded aggregates
//! back with [`load`](mess_store::EventStore::load).
//!
//! Run with:
//!
//! ```sh
//! cargo run --example social
//! ```
//!
//! See `src/lib.rs` for the domain and `tests/gwt.rs` for the same rules
//! exercised store-free through `mess-testkit`.

use ident::Id;
use mess_store::{EventStore, LogEngine};
use social::domain::post::Post;
use social::domain::user::User;
use social::{WriteError, WriteOps, post_stream, user_stream};

#[tokio::main]
async fn main() -> Result<(), WriteError> {
    // `EventStore` over the composed production engine (`LogEngine`:
    // `mess-log` + `mess-index`). Nothing below this line names the backend —
    // that is the API-first payoff. See `examples/bank/examples/bank.rs`.
    let dir =
        std::env::temp_dir().join(format!("mess-social-{}", std::process::id()));
    let store = EventStore::new(LogEngine::open(&dir).expect("open engine"));

    let alice = Id::new();
    let bob = Id::new();
    let post = Id::new();

    // Every write is one `WriteOps` call — the thin wrapper that maps an HTTP
    // action to one `store.command` on the right stream and hands back the
    // global log position of the write (the read-your-writes token).
    store.register(alice, "alice".into(), "Alice".into()).await?;
    store.register(bob, "bob".into(), "Bob".into()).await?;
    println!("registered alice ({alice}) and bob ({bob})");

    // The follow edge is recorded on ALICE's stream (the follower's own
    // aggregate) — see `domain::user` for why.
    store.follow(alice, bob).await?;
    println!("alice now follows bob");

    store.create_post(post, bob, "hello, mess!".into()).await?;
    println!("bob posted {}", post_stream(post));

    let seq = store.like(post, alice).await?;
    println!("alice liked bob's post (log position {seq})");

    // A business rule surfaces as a typed `WriteError`: a stranger cannot
    // delete bob's post.
    match store.delete_post(post, alice).await {
        Err(WriteError::Post(social::PostError::NotAuthor)) => {
            println!("alice cannot delete bob's post (as expected)");
        }
        other => panic!("expected NotAuthor, got {other:?}"),
    }

    // The author can.
    store.delete_post(post, bob).await?;
    println!("bob deleted his own post");

    // Read the folded aggregates straight back — the same replay `command`
    // does internally before it decides.
    let loaded_alice = store
        .load::<User>(&user_stream(alice))
        .await
        .expect("load alice");
    assert!(loaded_alice.state.following.contains(&bob));
    println!(
        "alice's aggregate: handle={:?} following {} user(s)",
        loaded_alice.state.handle,
        loaded_alice.state.following.len()
    );

    let loaded_post =
        store.load::<Post>(&post_stream(post)).await.expect("load post");
    assert!(loaded_post.state.deleted);
    assert!(loaded_post.state.likes.contains(&alice));
    println!(
        "post aggregate: author={:?} deleted={} likes={} (replayed {} events)",
        loaded_post.state.author,
        loaded_post.state.deleted,
        loaded_post.state.likes.len(),
        loaded_post.events_replayed,
    );

    println!("social example OK");
    Ok(())
}
