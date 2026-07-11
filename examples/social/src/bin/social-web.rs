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
//!   [`mess_store::LogEngine`] at `PATH` (written by `social-seed`, or by using
//!   the app), and builds a real [`social::Projections`] read model that
//!   **resumes from a checkpoint** ([`social::Projections::with_checkpoint`])
//!   when one is present next to the store (`PATH/.social-projections.ckpt`),
//!   replaying only the log *suffix* committed since. With no valid checkpoint
//!   (a fresh store, or a stale/corrupt file) it falls back to the "the read
//!   model rebuilds from the log" party trick — a full replay from position 0.
//!   On clean shutdown (Ctrl-C) it writes a fresh checkpoint so the next boot
//!   resumes from the very last folded position. The from-0 rebuild seam stays
//!   public as [`social::Projections::new`].

use std::path::PathBuf;
use std::sync::Arc;

use ident::Id;
use social::WriteOps;
use social::rebuild::rebuild_check;
use social::store_backend::{
    Store, StoreProjections, checkpoint_path, open_store,
};
use social::web::{AppState, MemBackend, router};

struct Args {
    dir:     Option<PathBuf>,
    addr:    String,
    /// `--rebuild`: run the checkpoint-correctness proof against `--dir` and
    /// exit (nonzero on mismatch) instead of serving. Requires `--dir`.
    rebuild: bool,
}

fn print_usage() {
    eprintln!(
        "Usage: social-web [--dir PATH] [--addr HOST:PORT] [--rebuild]\n\n\
         Options:\n  --dir PATH        serve a real on-disk store (written by \
         social-seed).\n  --addr HOST:PORT  listen address (default: \
         127.0.0.1:3000)\n  --rebuild         run the checkpoint-correctness \
         proof against --dir and exit\n                    (nonzero on \
         mismatch); does not serve. Requires --dir.\n\nWith no --dir, serves a \
         tiny hardcoded in-memory demo world instead."
    );
}

fn parse_args() -> Args {
    let mut dir = None;
    let mut addr = "127.0.0.1:3000".to_string();
    let mut rebuild = false;
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
            "--rebuild" => rebuild = true,
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
    Args { dir, addr, rebuild }
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

/// The real, store-backed world: open `dir` as the warm-write [`Store`]
/// ([`open_store`]) and build the read model by subscribing directly on that
/// same store handle — `FjallSnapshotBackend` forwards
/// [`SubscribeBackend`](mess_store::SubscribeBackend), so no second handle
/// over a cloned log is needed — **resuming from the sidecar checkpoint** when
/// present and valid, else a full replay from position 0 (see
/// [`StoreProjections::with_checkpoint`]). Returns the [`AppState`] plus the
/// projections handle so the caller can checkpoint on a clean shutdown. No
/// directory to populate: handle resolution goes through
/// [`ReadModels::resolve`](social::contracts::ReadModels::resolve) instead —
/// see `social::web`'s module docs.
async fn store_state(
    dir: &std::path::Path,
) -> (AppState<StoreProjections, Store>, Arc<StoreProjections>) {
    if !dir.is_dir() {
        eprintln!(
            "error: store directory not found at {}\n  Run `cargo run -p \
             social --bin social-seed -- --dir {}` first.",
            dir.display(),
            dir.display()
        );
        std::process::exit(1);
    }
    let store: Store = open_store(dir).unwrap_or_else(|e| {
        eprintln!("error: could not open store at {}: {e}", dir.display());
        std::process::exit(1);
    });

    print!("building read model (resume-or-rebuild) ... ");
    use std::io::Write;
    std::io::stdout().flush().ok();
    let projections = Arc::new(
        StoreProjections::with_checkpoint(&store, checkpoint_path(dir)).await,
    );
    match projections.resumed_from() {
        0 => println!("done (full rebuild from position 0)."),
        pos => println!("done (resumed from checkpoint at position {pos})."),
    }

    let state = AppState::new(projections.clone(), Arc::new(store));
    (state, projections)
}

/// Run the `--rebuild` checkpoint-correctness proof against `dir` and exit:
/// print PASS/FAIL + counts, exit nonzero on mismatch. See
/// [`social::rebuild`].
async fn run_rebuild(dir: &std::path::Path) -> ! {
    if !dir.is_dir() {
        eprintln!(
            "error: store directory not found at {}\n  Run `cargo run -p \
             social --bin social-seed -- --dir {}` first.",
            dir.display(),
            dir.display()
        );
        std::process::exit(1);
    }
    let report = rebuild_check(dir).await.unwrap_or_else(|e| {
        eprintln!("error: could not open store at {}: {e}", dir.display());
        std::process::exit(1);
    });
    println!("{}", report.render());
    std::process::exit(i32::from(!report.matched));
}

#[tokio::main]
async fn main() {
    let args = parse_args();

    // `--rebuild` is a proof-and-exit mode, not a server: run it first.
    if args.rebuild {
        match &args.dir {
            Some(dir) => run_rebuild(dir).await,
            None => {
                eprintln!("--rebuild requires --dir PATH\n");
                print_usage();
                std::process::exit(2);
            }
        }
    }

    let listener =
        tokio::net::TcpListener::bind(&args.addr).await.expect("bind");

    // `mem_state`/`store_state` return different `AppState<R, W>`
    // instantiations, so `router`/`axum::serve` are called once per branch
    // rather than through one shared variable — see `social::web`'s module
    // docs on what "generic state, not a trait object" costs here.
    match &args.dir {
        Some(dir) => {
            let (state, projections) = store_state(dir).await;
            println!(
                "social-web listening on http://{} (store at {})",
                args.addr,
                dir.display()
            );
            // Serve until Ctrl-C, then write a final checkpoint so the next
            // boot resumes from the very last folded position rather than the
            // last periodic cadence write.
            axum::serve(listener, router(state))
                .with_graceful_shutdown(async {
                    tokio::signal::ctrl_c().await.ok();
                })
                .await
                .expect("serve");
            print!("\nwriting shutdown checkpoint ... ");
            use std::io::Write;
            std::io::stdout().flush().ok();
            match projections.checkpoint_now().await {
                Ok(()) => println!("done."),
                Err(e) => println!("failed: {e}"),
            }
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
