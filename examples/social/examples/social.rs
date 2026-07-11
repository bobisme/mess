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

use social::Id;
use social::domain::follow::Follow;
use social::domain::like::Like;
use social::domain::post::Post;
use social::domain::user::User;
use social::store_backend::open_store;
use social::{
    WriteError, WriteOps, follow_stream, like_stream, post_stream, user_stream,
};

#[tokio::main]
async fn main() -> Result<(), WriteError> {
    // The warm-write on-disk `Store`: an `EventStore` over the composed
    // production engine (`LogEngine`: `mess-log` + `mess-index`) wrapped in a
    // `FjallSnapshotBackend` so writes take the `command_cached` warm path.
    // Nothing below this line names the backend — that is the API-first payoff.
    // See `examples/bank/examples/bank.rs`.
    //
    // A self-sweeping temp dir (the real-fs TMPDIR rule, bn-cxr/bn-imm):
    // removed on a clean exit, and bounded by the sweep even if this process
    // is killed mid-run instead of leaking a 256MiB-preallocated store
    // forever under `TMPDIR`.
    let dir = mess_testkit::sweeping_temp_dir("social-example");
    let store = open_store(dir.path()).expect("open store");

    let alice = Id::new();
    let bob = Id::new();
    let post = Id::new();

    // Every write is one `WriteOps` call — the thin wrapper that maps an HTTP
    // action to one warm-path `store.command_cached` on the right stream and
    // hands back the global log position of the write (the read-your-writes
    // token).
    store.register(alice, "alice".into(), "Alice".into()).await?;
    store.register(bob, "bob".into(), "Bob".into()).await?;
    println!("registered alice ({alice}) and bob ({bob})");

    // The follow edge is its own tiny relationship stream,
    // `follow-<alice>_<bob>` — not state on alice's user stream. See
    // `domain::follow` for why the crowd lives in per-edge aggregates.
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
    // does internally before it decides. The entities are bounded: alice's own
    // stream no longer carries her follow graph, and the post's stream no
    // longer carries its likers.
    let loaded_alice =
        store.load::<User>(&user_stream(alice)).await.expect("load alice");
    println!(
        "alice's aggregate: handle={:?} (bounded — no follow set on this \
         stream)",
        loaded_alice.state.handle,
    );

    let loaded_post =
        store.load::<Post>(&post_stream(post)).await.expect("load post");
    assert!(loaded_post.state.deleted);
    println!(
        "post aggregate: author={:?} deleted={} (replayed {} events; likes \
         live on their own streams)",
        loaded_post.state.author,
        loaded_post.state.deleted,
        loaded_post.events_replayed,
    );

    // The follow and like *edges* live on their own tiny relationship streams
    // — each a bounded, alternating state machine.
    let edge = store
        .load::<Follow>(&follow_stream(alice, bob))
        .await
        .expect("load follow edge");
    assert!(edge.state.following);
    println!("follow edge {}: active", follow_stream(alice, bob));

    let like = store
        .load::<Like>(&like_stream(post, alice))
        .await
        .expect("load like edge");
    assert!(like.state.liked);
    println!("like edge {}: active", like_stream(post, alice));

    println!("social example OK");
    Ok(())
}
