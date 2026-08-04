//! `chatter` — the CLI over [`chatter`](chatter), a deep-stream chat/feed
//! corpus generator and reader (bn-1m6c).
//!
//! ```sh
//! # a multi-segment sealed store in a few seconds
//! cargo run --release -p chatter --bin chatter -- seed --dir /tmp/chat
//! # what did that actually produce?
//! cargo run --release -p chatter --bin chatter -- stats --dir /tmp/chat
//! # page backward through the busiest channel's sealed history
//! cargo run --release -p chatter --bin chatter -- scrollback --dir /tmp/chat
//! # follow the log live (SUB2 subscriber semantics)
//! cargo run --release -p chatter --bin chatter -- tail --dir /tmp/chat --seconds 5
//! # prove the checkpoint is an acceleration, not a second truth
//! cargo run --release -p chatter --bin chatter -- rebuild --dir /tmp/chat
//! # five timed cells as JSON on stdout
//! cargo run --release -p chatter --bin chatter -- bench --dir /tmp/chat-bench
//! ```
//!
//! Argument parsing is hand-rolled over [`std::env::args`], matching
//! `examples/social`'s binaries — an example that teaches an event store should
//! not also be teaching a CLI framework, and adding one for six subcommands
//! would be the tail wagging the dog.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use chatter::bench::{self, BenchConfig};
use chatter::projections::Projections;
use chatter::rebuild::rebuild_compare;
use chatter::scrollback::ScrollBack;
use chatter::seed::{self, Scale, SeedConfig};
use chatter::store_backend::{
    checkpoint_path, create_store, human_bytes, open_store, read_config,
    sealed_census,
};
use chatter::tail::{self, Tailer};

/// The default store directory, under `$HOME/.cache` per the project
/// convention for generated demo state (never `/tmp` — tmpfs, and this is meant
/// to persist across a `seed` / read pair of invocations).
fn default_dir() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".cache/mess-chatter-demo/store")
}

fn usage() -> String {
    format!(
        "Usage: chatter <command> [options]

Commands:
  seed        generate a deterministic corpus (deep channels + many users)
  stats       print the sealed-tier census and the per-channel projection
  scrollback  page backward through a channel's history (sealed reads)
  tail        follow the global log with SUB2 subscriber semantics
  rebuild     byte-compare a checkpoint resume against a from-0 rebuild
  bench       run the five timed cells and print JSON on stdout

Common options:
  --dir PATH            store directory (default: {})

seed options:
  --scale demo|large|huge   preset for every knob below (default: demo)
  --seed N                  PRNG seed; same seed -> same corpus (default: 1337)
  --channels N              deep streams to open
  --users N                 shallow streams to register (registry pressure)
  --messages N              messages to post across all channels
  --reactions F             reactions per message, as a factor (e.g. 0.6)
  --segment-bytes N[K|M|G]  active-segment size; small values force rolls
  --seal-pack on|off        one .seal per sealed segment, or loose sidecars
  --concurrency N           pipelining window (1 = sequential, byte-identical)
  --sequential              shorthand for --concurrency 1
  --force                   wipe --dir first if it already holds a store

scrollback options:
  --channel SLUG            which channel (default: the busiest one)
  --pages N                 how many backward pages to print (default: 5)
  --page-size N             events per page (default: 20)

tail options:
  --from N                  starting global position (default: the head)
  --seconds N               stop after N seconds (default: run until Ctrl-C)
  --limit N                 stop once at least N records have been delivered
                            (batches are never truncated -- see the note in
                            the source on why that would corrupt the cursor)

bench options:
  the seed options above, plus:
  --scroll-pages N          backward pages in the scroll_back cell
  --scroll-page-size N      events per backward page
  --tail-page-size N        records per read_global_page in tail_catch_up
",
        default_dir().display()
    )
}

fn die(msg: impl std::fmt::Display) -> ! {
    eprintln!("chatter: {msg}");
    std::process::exit(2)
}

/// Parse a byte count with an optional binary suffix: `1048576`, `1M`, `64MiB`,
/// `2G`.
fn parse_bytes(raw: &str) -> Result<u64, String> {
    let s = raw.trim();
    let lower = s.to_ascii_lowercase();
    let (digits, mult) = if let Some(d) = lower.strip_suffix("gib") {
        (d, 1024u64 * 1024 * 1024)
    } else if let Some(d) = lower.strip_suffix("mib") {
        (d, 1024 * 1024)
    } else if let Some(d) = lower.strip_suffix("kib") {
        (d, 1024)
    } else if let Some(d) = lower.strip_suffix('g') {
        (d, 1024 * 1024 * 1024)
    } else if let Some(d) = lower.strip_suffix('m') {
        (d, 1024 * 1024)
    } else if let Some(d) = lower.strip_suffix('k') {
        (d, 1024)
    } else if let Some(d) = lower.strip_suffix('b') {
        (d, 1)
    } else {
        (lower.as_str(), 1)
    };
    digits.trim().parse::<u64>().map(|n| n * mult).map_err(|_| {
        format!("expected a byte count like 1048576 or 64MiB, got {raw:?}")
    })
}

/// A tiny argument cursor: `--flag value` pairs plus bare `--switch`es.
struct Args {
    items: Vec<String>,
    at:    usize,
}

impl Args {
    fn new(items: Vec<String>) -> Self { Self { items, at: 0 } }

    fn next(&mut self) -> Option<String> {
        let item = self.items.get(self.at).cloned();
        if item.is_some() {
            self.at += 1;
        }
        item
    }

    fn value(&mut self, flag: &str) -> String {
        self.next().unwrap_or_else(|| die(format!("{flag} requires a value")))
    }

    fn usize_value(&mut self, flag: &str) -> usize {
        let raw = self.value(flag);
        raw.parse().unwrap_or_else(|_| {
            die(format!("{flag} must be an integer, got {raw:?}"))
        })
    }

    fn u64_value(&mut self, flag: &str) -> u64 {
        let raw = self.value(flag);
        raw.parse().unwrap_or_else(|_| {
            die(format!("{flag} must be an integer, got {raw:?}"))
        })
    }

    fn f64_value(&mut self, flag: &str) -> f64 {
        let raw = self.value(flag);
        raw.parse().unwrap_or_else(|_| {
            die(format!("{flag} must be a number, got {raw:?}"))
        })
    }

    fn bool_value(&mut self, flag: &str) -> bool {
        let raw = self.value(flag);
        match raw.as_str() {
            "on" | "true" | "yes" | "1" => true,
            "off" | "false" | "no" | "0" => false,
            other => die(format!("{flag} must be on|off, got {other:?}")),
        }
    }
}

/// The knobs `seed` and `bench` share.
struct SeedArgs {
    dir:   PathBuf,
    cfg:   SeedConfig,
    scale: Scale,
    force: bool,
}

/// Parse the shared seed knobs, letting an explicit flag override whatever the
/// `--scale` preset chose. `--scale` is applied first no matter where it
/// appears, so flag order never changes the result.
fn parse_seed_args(
    argv: &[String],
    extra: &mut dyn FnMut(&str, &mut Args) -> bool,
) -> SeedArgs {
    let items: Vec<String> = argv.to_vec();
    // Pass 1: find --scale and --seed so the preset is the baseline.
    let mut scale = Scale::Demo;
    let mut seed_value = 1337u64;
    let mut i = 0;
    while i < items.len() {
        match items[i].as_str() {
            "--scale" => {
                let raw = items
                    .get(i + 1)
                    .unwrap_or_else(|| die("--scale requires a value"));
                scale = Scale::parse(raw).unwrap_or_else(|e| die(e));
                i += 2;
            }
            "--seed" => {
                let raw = items
                    .get(i + 1)
                    .unwrap_or_else(|| die("--seed requires a value"));
                seed_value = raw.parse().unwrap_or_else(|_| {
                    die(format!("--seed must be a u64, got {raw:?}"))
                });
                i += 2;
            }
            _ => i += 1,
        }
    }
    let mut cfg = SeedConfig::for_scale(scale, seed_value);
    let mut dir = default_dir();
    let mut force = false;

    // Pass 2: everything else, overriding the preset.
    let mut args = Args::new(items);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            // Already applied in pass 1; skip their values.
            "--scale" | "--seed" => {
                let _ = args.value(&flag);
            }
            "--dir" => dir = PathBuf::from(args.value("--dir")),
            "--channels" => cfg.channels = args.usize_value("--channels"),
            "--users" => cfg.users = args.usize_value("--users"),
            "--messages" => cfg.messages = args.usize_value("--messages"),
            "--reactions" => {
                cfg.reaction_factor = args.f64_value("--reactions").max(0.0)
            }
            "--segment-bytes" => {
                let raw = args.value("--segment-bytes");
                cfg.segment_bytes =
                    parse_bytes(&raw).unwrap_or_else(|e| die(e));
            }
            "--seal-pack" => cfg.seal_pack = args.bool_value("--seal-pack"),
            "--concurrency" => {
                cfg.concurrency = args.usize_value("--concurrency").max(1)
            }
            "--sequential" => cfg.concurrency = 1,
            "--force" => force = true,
            "-h" | "--help" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            other => {
                if !extra(other, &mut args) {
                    eprintln!("unknown argument: {other}\n{}", usage());
                    std::process::exit(2);
                }
            }
        }
    }
    SeedArgs { dir, cfg, scale, force }
}

/// `--dir` on its own, for the read-side commands.
fn parse_dir(
    argv: &[String],
    mut extra: impl FnMut(&str, &mut Args) -> bool,
) -> PathBuf {
    let mut dir = default_dir();
    let mut args = Args::new(argv.to_vec());
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--dir" => dir = PathBuf::from(args.value("--dir")),
            "-h" | "--help" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            other => {
                if !extra(other, &mut args) {
                    eprintln!("unknown argument: {other}\n{}", usage());
                    std::process::exit(2);
                }
            }
        }
    }
    dir
}

// ===========================================================================
// Commands
// ===========================================================================

async fn cmd_seed(argv: &[String]) {
    let args = parse_seed_args(argv, &mut |_flag, _args| false);
    if args.force && args.dir.exists() {
        println!("--force: removing existing store at {}", args.dir.display());
        if let Err(e) = std::fs::remove_dir_all(&args.dir) {
            die(format!("could not remove {}: {e}", args.dir.display()));
        }
    } else if let Err(msg) = seed::guard_fresh_dir(&args.dir) {
        eprintln!("{msg}");
        std::process::exit(1);
    }

    let cfg = args.cfg;
    println!(
        "seeding {} (scale={}, seed={}, channels={}, users={}, messages={}, \
         segment-bytes={}, seal-pack={}, concurrency={}) ...",
        args.dir.display(),
        args.scale.name(),
        cfg.seed,
        cfg.channels,
        cfg.users,
        cfg.messages,
        human_bytes(cfg.segment_bytes),
        cfg.seal_pack,
        cfg.concurrency,
    );

    let store = create_store(&args.dir, cfg.store_config())
        .unwrap_or_else(|e| die(format!("could not open store: {e}")));
    let report = seed::generate(&store, &cfg).await;
    drop(store);
    // Reopen so the census reflects every segment the background sealer
    // finished during shutdown drain — the number a later cold open will see.
    let store = open_store(&args.dir)
        .unwrap_or_else(|e| die(format!("could not reopen store: {e}")));
    let after = sealed_census(&store, &args.dir);
    drop(store);

    println!(
        "done in {:.2}s: {} users, {} channels, {} messages, {} reactions, {} \
         renames, {} archives ({} events total)",
        report.elapsed.as_secs_f64(),
        report.users,
        report.channels,
        report.messages,
        report.reactions,
        report.renames,
        report.archives,
        report.events(),
    );
    if let Some(slug) = &report.deepest_channel_slug {
        println!(
            "busiest channel: #{slug} with {} messages; quietest has {}",
            report.deepest_channel, report.shallowest_channel
        );
    }
    println!(
        "store: {} across {} segment file(s); sealed tier: {} segment(s) ({} \
         SealPack, {} loose, {} with a payload accelerator)",
        human_bytes(after.bytes),
        after.segment_files,
        after.sealed_segments,
        after.seal_pack,
        after.loose_sidecar,
        after.payload_indexed,
    );
    if after.sealed_segments < 2 {
        println!(
            "NOTE: fewer than two sealed segments. Lower --segment-bytes or \
             raise --messages to reach the sealed tier (see the README)."
        );
    }
    println!("next: chatter stats --dir {}", args.dir.display());
}

async fn cmd_stats(argv: &[String]) {
    let dir = parse_dir(argv, |_, _| false);
    let store = open_store(&dir)
        .unwrap_or_else(|e| die(format!("could not open store: {e}")));
    let cfg = read_config(&dir);
    let census = sealed_census(&store, &dir);
    let t = Instant::now();
    let proj =
        Projections::with_checkpoint(&store, checkpoint_path(&dir)).await;
    let build = t.elapsed();
    let cards = proj.cardinalities().await;

    println!("store {}", dir.display());
    println!(
        "  knobs:  segment-bytes={} seal-pack={}",
        human_bytes(cfg.segment_bytes),
        cfg.seal_pack
    );
    println!(
        "  disk:   {} across {} segment file(s)",
        human_bytes(census.bytes),
        census.segment_files
    );
    println!(
        "  sealed: {} segment(s) — {} SealPack, {} loose, {} with a payload \
         accelerator",
        census.sealed_segments,
        census.seal_pack,
        census.loose_sidecar,
        census.payload_indexed,
    );
    println!("  events: {} published", census.published_events);
    println!(
        "  read model built in {:.3}s ({:?}): {} users, {} channels, {} \
         messages, {} reactions",
        build.as_secs_f64(),
        proj.checkpoint_status(),
        cards.users,
        cards.channels,
        cards.messages,
        cards.reactions,
    );

    println!("\nchannels (deepest first):");
    for c in proj.channels().await.iter().take(20) {
        println!(
            "  #{:<24} {:>8} messages {:>8} reactions{}",
            c.slug,
            c.messages,
            c.reactions,
            if c.archived { "  [archived]" } else { "" }
        );
    }

    println!("\nrecent timeline:");
    for row in proj.timeline(10).await {
        println!(
            "  [{}] #{}/{} @{} {}",
            row.seq,
            row.channel_slug,
            row.ordinal,
            row.author_handle,
            row.preview
        );
    }
    // A clean shutdown persists the checkpoint at the exact position folded,
    // so the next command resumes instead of replaying.
    if let Err(e) = proj.checkpoint_now().await {
        eprintln!("chatter: WARNING could not write checkpoint: {e}");
    }
}

async fn cmd_scrollback(argv: &[String]) {
    let mut slug: Option<String> = None;
    let mut pages = 5usize;
    let mut page_size = 20usize;
    let dir = parse_dir(argv, |flag, args| match flag {
        "--channel" => {
            slug = Some(args.value("--channel"));
            true
        }
        "--pages" => {
            pages = args.usize_value("--pages");
            true
        }
        "--page-size" => {
            page_size = args.usize_value("--page-size");
            true
        }
        _ => false,
    });

    let store = open_store(&dir)
        .unwrap_or_else(|e| die(format!("could not open store: {e}")));
    let proj =
        Projections::with_checkpoint(&store, checkpoint_path(&dir)).await;
    let channels = proj.channels().await;
    let target = match &slug {
        Some(s) => channels
            .iter()
            .find(|c| &c.slug == s)
            .unwrap_or_else(|| die(format!("no channel with slug {s:?}"))),
        None => channels
            .first()
            .unwrap_or_else(|| die("this store has no channels")),
    };
    println!(
        "scrolling back through #{} ({} messages, {} reactions)\n",
        target.slug, target.messages, target.reactions
    );

    let mut back = ScrollBack::open(&store, target.id, page_size)
        .await
        .unwrap_or_else(|e| die(e));
    let mut printed = 0usize;
    while printed < pages && back.has_more() {
        let t = Instant::now();
        let page = back.next_page().await.unwrap_or_else(|e| die(e));
        printed += 1;
        println!(
            "-- page {printed} ({} records, {} messages, {} carried \
             reactions) in {:.2} ms --",
            page.records,
            page.messages.len(),
            page.carried_reactions,
            t.elapsed().as_secs_f64() * 1000.0,
        );
        for m in &page.messages {
            let body: String = m.body.chars().take(96).collect();
            println!("  #{:<6} [gp {}] {body}", m.ordinal, m.global_position);
            if !m.reactions.is_empty() {
                let emojis: Vec<&str> =
                    m.reactions.iter().map(|(_, e)| e.as_str()).collect();
                println!("          reactions: {}", emojis.join(" "));
            }
        }
    }
    if back.has_more() {
        println!(
            "\n(stopped at stream position {}; --pages for more)",
            back.cursor()
        );
    } else {
        println!("\n(reached the start of the channel)");
    }
}

async fn cmd_tail(argv: &[String]) {
    let mut from: Option<u64> = None;
    let mut seconds: Option<u64> = None;
    let mut limit: Option<u64> = None;
    let dir = parse_dir(argv, |flag, args| match flag {
        "--from" => {
            from = Some(args.u64_value("--from"));
            true
        }
        "--seconds" => {
            seconds = Some(args.u64_value("--seconds"));
            true
        }
        "--limit" => {
            limit = Some(args.u64_value("--limit"));
            true
        }
        _ => false,
    });

    let store = open_store(&dir)
        .unwrap_or_else(|e| die(format!("could not open store: {e}")));
    let head = store.watermark().await.unwrap_or(0);
    let start = from.unwrap_or(head);
    println!(
        "tailing {} from global position {start} (head is {head}); Ctrl-C to \
         stop",
        dir.display()
    );

    let mut tailer = Tailer::from(&store, start, 256);
    let deadline = seconds.map(|s| Instant::now() + Duration::from_secs(s));
    let mut delivered = 0u64;
    loop {
        if let Some(max) = limit
            && delivered >= max
        {
            break;
        }
        let wait = match deadline {
            Some(d) => match d.checked_duration_since(Instant::now()) {
                Some(remaining) if !remaining.is_zero() => remaining,
                _ => break,
            },
            None => Duration::from_secs(86_400),
        };
        let batch = tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => break,
            _ = tokio::time::sleep(wait) => continue,
            batch = tailer.next_batch() => {
                batch.unwrap_or_else(|e| die(e))
            }
        };
        // Always consume the WHOLE batch before re-checking `--limit`. The
        // tailer's cursor has already advanced past the page, so stopping
        // mid-batch would print a resume cursor that skips the records we
        // chose not to print. `--limit` is therefore "stop once at least N
        // records have been delivered", not "print exactly N".
        for rec in &batch {
            println!("{}", tail::describe(rec));
            delivered += 1;
        }
    }
    println!(
        "\ndelivered {delivered} record(s); resume cursor is {}",
        tailer.position()
    );
}

async fn cmd_rebuild(argv: &[String]) {
    let dir = parse_dir(argv, |_, _| false);
    let store = open_store(&dir)
        .unwrap_or_else(|e| die(format!("could not open store: {e}")));
    let report = rebuild_compare(&store, &checkpoint_path(&dir)).await;
    println!("{}", report.render());
    if !report.matched {
        std::process::exit(1);
    }
}

async fn cmd_bench(argv: &[String]) {
    let mut scroll_pages = 20usize;
    let mut scroll_page_size = 50usize;
    let mut tail_page_size = 512usize;
    let args = parse_seed_args(argv, &mut |flag, a| match flag {
        "--scroll-pages" => {
            scroll_pages = a.usize_value("--scroll-pages");
            true
        }
        "--scroll-page-size" => {
            scroll_page_size = a.usize_value("--scroll-page-size");
            true
        }
        "--tail-page-size" => {
            tail_page_size = a.usize_value("--tail-page-size");
            true
        }
        _ => false,
    });
    let dir = if args.dir == default_dir() {
        bench::default_bench_dir()
    } else {
        args.dir
    };
    let cfg = BenchConfig {
        dir,
        seed: args.cfg,
        scroll_pages,
        scroll_page_size,
        tail_page_size,
    };
    if cfg!(debug_assertions) {
        eprintln!(
            "chatter bench: WARNING this is a DEBUG build; its timings are \
             several times slower than release and MUST NOT be quoted. Rerun \
             with --release."
        );
    }
    let report = bench::run(&cfg)
        .await
        .unwrap_or_else(|e| die(format!("bench failed: {e}")));
    println!("{}", report.to_json());
}

#[tokio::main]
async fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let Some(command) = argv.first().cloned() else {
        println!("{}", usage());
        std::process::exit(2);
    };
    let rest = &argv[1..];
    match command.as_str() {
        "seed" => cmd_seed(rest).await,
        "stats" => cmd_stats(rest).await,
        "scrollback" | "scroll-back" => cmd_scrollback(rest).await,
        "tail" => cmd_tail(rest).await,
        "rebuild" => cmd_rebuild(rest).await,
        "bench" => cmd_bench(rest).await,
        "-h" | "--help" | "help" => println!("{}", usage()),
        other => {
            eprintln!("unknown command: {other}\n{}", usage());
            std::process::exit(2);
        }
    }
}
