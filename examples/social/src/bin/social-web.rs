//! The runnable demo server for the social example.
//!
//! ```sh
//! # in-memory toy world (three hardcoded users, no persistence):
//! cargo run -p social --bin social-web
//!
//! # the real thing: seed a store, then serve it (see examples/social/README.md)
//! cargo run -p social --bin social-seed
//! cargo run -p social --bin social-web -- --dir ~/.cache/mess-social-demo/store
//! # then open http://127.0.0.1:3000
//! ```
//!
//! Two backends, selected by `--dir`:
//!
//! - **No `--dir` (default).** Wires the web layer ([`social::web`]) to a
//!   coherent in-memory backend ([`social::web::MemBackend`]) seeded with a
//!   tiny hardcoded world — a quick sanity check that needs no setup.
//! - **`--dir PATH`.** Opens a real [`mess_store::EventStore`] over
//!   [`mess_store::LogEngine`] at `PATH` (written by `social-seed`, or by
//!   using the app), and builds a real [`social::Projections`] read model by
//!   replaying that store's log from position 0 — this is the "the read
//!   model rebuilds from the log" party trick the README walks through: stop
//!   this process and start it again, and `Projections::new` redoes exactly
//!   that replay before serving a single request.

use std::path::PathBuf;
use std::sync::Arc;

use ident::Id;
use mess_store::{EventStore, LogEngine};
use social::WriteOps;
use social::Projections;
use social::store_backend::Store;
use social::web::{AppState, MemBackend, router};

struct Args {
    dir: Option<PathBuf>,
    addr: String,
}

fn print_usage() {
    eprintln!(
        "Usage: social-web [--dir PATH] [--addr HOST:PORT]\n\n\
         Options:\n  \
         --dir PATH        serve a real on-disk store (written by social-seed).\n  \
         --addr HOST:PORT  listen address (default: 127.0.0.1:3000)\n\n\
         With no --dir, serves a tiny hardcoded in-memory demo world instead."
    );
}

fn parse_args() -> Args {
    let mut dir = None;
    let mut addr = "127.0.0.1:3000".to_string();
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--dir" => {
                dir = Some(it.next().map(PathBuf::from).unwrap_or_else(|| {
                    eprintln!("--dir requires a path argument");
                    std::process::exit(2);
                }));
            }
            "--addr" => {
                addr = it.next().unwrap_or_else(|| {
                    eprintln!("--addr requires a HOST:PORT argument");
                    std::process::exit(2);
                });
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}\n");
                print_usage();
                std::process::exit(2);
            }
        }
    }
    Args { dir, addr }
}

/// The tiny in-memory demo world used when no `--dir` is given: an
/// interactive sanity check with three hardcoded users, no persistence, no
/// setup required.
async fn mem_state() -> AppState<MemBackend, MemBackend> {
    let backend = Arc::new(MemBackend::new());

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

    AppState::new(backend.clone(), backend)
}

/// The real, store-backed world: open `dir` and rebuild the read model by
/// replaying its whole log (see [`Projections::new`]). No directory to
/// populate: handle resolution goes through
/// [`ReadModels::resolve`](social::contracts::ReadModels::resolve) instead —
/// see `social::web`'s module docs.
async fn store_state(
    dir: &std::path::Path,
) -> AppState<Projections<LogEngine>, Store> {
    if !dir.is_dir() {
        eprintln!(
            "error: store directory not found at {}\n  Run `cargo run -p social --bin social-seed -- --dir {}` first.",
            dir.display(),
            dir.display()
        );
        std::process::exit(1);
    }
    let backend = LogEngine::open(dir).unwrap_or_else(|e| {
        eprintln!("error: could not open store at {}: {e}", dir.display());
        std::process::exit(1);
    });
    let store: Store = EventStore::new(backend);

    print!("replaying log from position 0 ... ");
    use std::io::Write;
    std::io::stdout().flush().ok();
    let projections = Arc::new(Projections::new(&store).await);
    println!("done.");

    AppState::new(projections, Arc::new(store))
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let listener =
        tokio::net::TcpListener::bind(&args.addr).await.expect("bind");

    // `mem_state`/`store_state` return different `AppState<R, W>`
    // instantiations, so `router`/`axum::serve` are called once per branch
    // rather than through one shared variable — see `social::web`'s module
    // docs on what "generic state, not a trait object" costs here.
    match &args.dir {
        Some(dir) => {
            let state = store_state(dir).await;
            println!(
                "social-web listening on http://{} (store at {})",
                args.addr,
                dir.display()
            );
            axum::serve(listener, router(state)).await.expect("serve");
        }
        None => {
            let state = mem_state().await;
            println!(
                "social-web listening on http://{} (in-memory demo world)",
                args.addr
            );
            axum::serve(listener, router(state)).await.expect("serve");
        }
    }
}
