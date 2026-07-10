//! The runnable demo server for the social example.
//!
//! ```sh
//! cargo run -p social --bin social-web
//! # then open http://127.0.0.1:3000
//! ```
//!
//! It wires the web layer ([`social::web`]) to a coherent in-memory backend
//! ([`social::web::MemBackend`]) seeded with a small world, so posting, liking,
//! following, and the act-as-user picker all work interactively. Swapping the
//! backend for the real `EventStore` + the bn-2xl projection tailer is the one
//! remaining wiring step — the handlers do not change.

use std::sync::{Arc, RwLock};

use ident::Id;
use social::WriteOps;
use social::web::{AppState, Directory, MemBackend, router};

#[tokio::main]
async fn main() {
    let backend = Arc::new(MemBackend::new());
    let mut dir = Directory::new();

    // Seed a small world through the real write path so the demo has content.
    let alice = Id::new();
    let bob = Id::new();
    let carol = Id::new();
    for (id, handle, display) in [
        (alice, "alice", "Alice"),
        (bob, "bob", "Bob"),
        (carol, "carol", "Carol"),
    ] {
        backend
            .register(id, handle.into(), display.into())
            .await
            .expect("seed register");
        dir.insert(id, handle);
    }
    backend.follow(alice, bob).await.expect("seed follow");
    let p1 = Id::new();
    let p2 = Id::new();
    let p3 = Id::new();
    backend
        .create_post(p1, bob, "hello, mess! first post here.".into())
        .await
        .expect("seed post");
    backend
        .create_post(p2, carol, "carol checking in 👋".into())
        .await
        .expect("seed post");
    backend
        .create_post(p3, bob, "event sourcing is neat.".into())
        .await
        .expect("seed post");
    backend.like(p1, alice).await.expect("seed like");

    let state = AppState {
        read: backend.clone(),
        write: backend.clone(),
        dir: Arc::new(RwLock::new(dir)),
    };

    let addr = "127.0.0.1:3000";
    let listener =
        tokio::net::TcpListener::bind(addr).await.expect("bind");
    println!("social-web listening on http://{addr}");
    axum::serve(listener, router(state)).await.expect("serve");
}
