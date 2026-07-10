//! `social-seed` — generate the deterministic demo corpus (bn-1mw).
//!
//! ```sh
//! cargo run -p social --bin social-seed
//! # or, to reseed on top of a previous run:
//! cargo run -p social --bin social-seed -- --force
//! ```
//!
//! Registers ~50 users, builds a Zipf-ish follow graph, publishes ~500 posts
//! with varied phrase-combinator bodies, likes them Zipf-distributed, deletes
//! a handful, and unfollows a handful — all through real
//! [`WriteOps`](social::contracts::WriteOps) calls against a real
//! [`mess_store::EventStore`]/[`mess_store::LogEngine`] on disk. See
//! [`social::seed`] for the generator itself.

use std::path::PathBuf;

use mess_store::{EventStore, LogEngine};
use social::seed::{self, SeedConfig};

/// The store directory `social-web --dir` should point at afterward, unless
/// `--dir` overrides it. Lives under `$HOME/.cache/mess-social-demo` per the
/// project convention for generated demo state (never `/tmp` — tmpfs, and
/// this is meant to persist across a `seed` / `serve` pair of invocations).
fn default_dir() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".cache/mess-social-demo/store")
}

struct Args {
    dir: PathBuf,
    seed: u64,
    force: bool,
}

fn print_usage() {
    eprintln!(
        "Usage: social-seed [--dir PATH] [--seed N] [--force]\n\n\
         Options:\n  \
         --dir PATH   store directory to seed (default: {})\n  \
         --seed N     PRNG seed; same seed -> same corpus (default: 1337)\n  \
         --force      wipe --dir first if it already holds a store\n",
        default_dir().display()
    );
}

fn parse_args() -> Args {
    let mut dir = default_dir();
    let mut seed = 1337u64;
    let mut force = false;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--dir" => {
                dir = it.next().map(PathBuf::from).unwrap_or_else(|| {
                    eprintln!("--dir requires a path argument");
                    std::process::exit(2);
                });
            }
            "--seed" => {
                let raw = it.next().unwrap_or_else(|| {
                    eprintln!("--seed requires a numeric argument");
                    std::process::exit(2);
                });
                seed = raw.parse().unwrap_or_else(|_| {
                    eprintln!("--seed must be a u64, got {raw:?}");
                    std::process::exit(2);
                });
            }
            "--force" => force = true,
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
    Args { dir, seed, force }
}

#[tokio::main]
async fn main() {
    let args = parse_args();

    if args.force && args.dir.exists() {
        println!("--force: removing existing store at {}", args.dir.display());
        if let Err(e) = std::fs::remove_dir_all(&args.dir) {
            eprintln!("error: could not remove {}: {e}", args.dir.display());
            std::process::exit(1);
        }
    } else if let Err(msg) = seed::guard_fresh_dir(&args.dir) {
        eprintln!("{msg}");
        std::process::exit(1);
    }

    let backend = LogEngine::open(&args.dir).unwrap_or_else(|e| {
        eprintln!("error: could not open store at {}: {e}", args.dir.display());
        std::process::exit(1);
    });
    let store = EventStore::new(backend);

    println!(
        "seeding {} (seed={}) ...",
        args.dir.display(),
        args.seed
    );
    let cfg = SeedConfig { seed: args.seed, ..SeedConfig::default() };
    let report = seed::generate(&store, &cfg).await;

    println!(
        "done in {:.2}s: {} users, {} follows, {} posts, {} likes, {} deletes, {} unfollows",
        report.elapsed.as_secs_f64(),
        report.users,
        report.follows,
        report.posts,
        report.likes,
        report.deletes,
        report.unfollows,
    );
    println!(
        "next: cargo run -p social --bin social-web -- --dir {}",
        args.dir.display()
    );
}
