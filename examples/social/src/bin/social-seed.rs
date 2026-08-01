//! `social-seed` — generate the deterministic demo/large corpus (bn-1mw; scale
//! tiers + pipelining bn-o9z).
//!
//! ```sh
//! # demo tier (~1,488 events, the README tour):
//! cargo run -p social --bin social-seed
//! # large tier (~50k+ events, pipelined):
//! cargo run -p social --bin social-seed -- --scale large --dir /path/to/store
//! # reseed on top of a previous run:
//! cargo run -p social --bin social-seed -- --force
//! # force sequential execution (for a pipelined-vs-sequential wall-clock A/B):
//! cargo run -p social --bin social-seed -- --scale large --sequential --dir ...
//! ```
//!
//! Writes the corpus through real
//! [`WriteOps`](social::contracts::WriteOps) (warm-path `command_cached`) calls
//! against a real on-disk warm-write [`Store`](social::store_backend::Store) —
//! a [`mess_store::LogEngine`] wrapped in a
//! [`mess_store::PackSnapshotBackend`]. See [`social::seed`] for the
//! generator.

use std::path::{Path, PathBuf};

use social::seed::{self, SeedConfig};
use social::store_backend::open_store;

/// The store directory `social-web --dir` should point at afterward, unless
/// `--dir` overrides it. Lives under `$HOME/.cache/mess-social-demo` per the
/// project convention for generated demo state (never `/tmp` — tmpfs, and
/// this is meant to persist across a `seed` / `serve` pair of invocations).
fn default_dir() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".cache/mess-social-demo/store")
}

#[derive(Clone, Copy)]
enum Scale {
    Demo,
    Large,
}

struct Args {
    dir:         PathBuf,
    seed:        u64,
    force:       bool,
    scale:       Scale,
    sequential:  bool,
    concurrency: Option<usize>,
}

fn print_usage() {
    eprintln!(
        "Usage: social-seed [--dir PATH] [--seed N] [--scale demo|large] \
         [--sequential] [--concurrency N] [--force]\n\nOptions:\n  \
         --dir PATH        store directory to seed (default: {})\n  \
         --seed N          PRNG seed; same seed -> same corpus (default: \
         1337)\n  --scale demo|large  corpus size tier (default: demo)\n  \
         --sequential      force sequential execution (concurrency=1)\n  \
         --concurrency N   override the tier's pipelining window (A/B \
         timing)\n  --force           wipe --dir first if it already holds a \
         store\n",
        default_dir().display()
    );
}

fn parse_args() -> Args {
    let mut dir = default_dir();
    let mut seed = 1337u64;
    let mut force = false;
    let mut scale = Scale::Demo;
    let mut sequential = false;
    let mut concurrency = None;
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
            "--scale" => {
                let raw = it.next().unwrap_or_else(|| {
                    eprintln!("--scale requires demo|large");
                    std::process::exit(2);
                });
                scale = match raw.as_str() {
                    "demo" => Scale::Demo,
                    "large" => Scale::Large,
                    other => {
                        eprintln!("--scale must be demo|large, got {other:?}");
                        std::process::exit(2);
                    }
                };
            }
            "--sequential" => sequential = true,
            "--concurrency" => {
                let raw = it.next().unwrap_or_else(|| {
                    eprintln!("--concurrency requires a positive integer");
                    std::process::exit(2);
                });
                let n = raw.parse().unwrap_or_else(|_| {
                    eprintln!("--concurrency must be a usize, got {raw:?}");
                    std::process::exit(2);
                });
                concurrency = Some(n);
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
    Args { dir, seed, force, scale, sequential, concurrency }
}

fn build_cfg(args: &Args) -> SeedConfig {
    let mut cfg = match args.scale {
        Scale::Demo => SeedConfig { seed: args.seed, ..SeedConfig::demo() },
        Scale::Large => SeedConfig::large(args.seed),
    };
    if let Some(n) = args.concurrency {
        cfg.concurrency = n.max(1);
    }
    if args.sequential {
        cfg.concurrency = 1;
    }
    cfg
}

fn scale_name(scale: Scale) -> &'static str {
    match scale {
        Scale::Demo => "demo",
        Scale::Large => "large",
    }
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

    let store = open_store(Path::new(&args.dir)).unwrap_or_else(|e| {
        eprintln!("error: could not open store at {}: {e}", args.dir.display());
        std::process::exit(1);
    });

    let cfg = build_cfg(&args);
    println!(
        "seeding {} (scale={}, seed={}, concurrency={}) ...",
        args.dir.display(),
        scale_name(args.scale),
        cfg.seed,
        cfg.concurrency,
    );
    let report = seed::generate(&store, &cfg).await;

    println!(
        "done in {:.2}s: {} users, {} follows, {} posts, {} likes, {} \
         deletes, {} unfollows",
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
