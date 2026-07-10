//! The deterministic demo-corpus generator (bn-1mw).
//!
//! [`generate`] drives ~50 users, a Zipf-ish follow graph, ~500 posts with
//! varied phrase-combinator bodies, Zipf-distributed likes, a handful of
//! deletes, and a handful of unfollows — **entirely through
//! [`WriteOps`](crate::contracts::WriteOps)**, i.e. one real
//! [`EventStore::command`](mess_store::EventStore::command) call per action,
//! the same call path a real client makes. There is no raw-append shortcut:
//! every invariant (`handle_is_valid`, no self-follow, no double-like, …) is
//! enforced by the real `Decide` impls exactly as in production, which is
//! also why the generator tracks its own shadow state (who follows whom, who
//! liked what) — not to skip validation, but to avoid *wasting* a command
//! attempt on a rejection it can see coming for free.
//!
//! Generic over `B: Backend` (not pinned to [`crate::store_backend::Store`])
//! so the same generator seeds a real on-disk [`mess_store::LogEngine`] *and*
//! an in-memory test store —
//! [`tests::generate_is_deterministic_for_a_fixed_seed`] below exercises the
//! latter.
//!
//! # Determinism
//!
//! Every draw comes from one [`StdRng`] seeded from
//! [`SeedConfig::seed`](SeedConfig::seed) — no wall-clock, no thread
//! scheduling dependence in the *shape* of the corpus (handles, display
//! names, bodies, the follow/like graphs, which posts get deleted). Two runs
//! with the same seed produce the same sequence of commands in the same
//! order. (The event *ids* embed randomness too, via [`Id::from_u128`] fed by
//! the same RNG, so they are reproducible byte-for-byte as well — the whole
//! corpus, not just its shape, is a pure function of `seed`.)

use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, Instant};

use ident::Id;
use mess_store::{Backend, EventStore};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use crate::contracts::WriteOps;

// ===========================================================================
// Config / report
// ===========================================================================

/// Corpus shape. The `Default` is the bone's target shape (~50 users, ~500
/// posts, …); override individual fields (e.g. in a fast test) via struct
/// update syntax: `SeedConfig { users: 8, posts: 20, ..SeedConfig::default()
/// }`.
#[derive(Debug, Clone)]
pub struct SeedConfig {
    /// The PRNG seed. Same seed, same corpus.
    pub seed:      u64,
    /// How many users to register.
    pub users:     usize,
    /// How many posts to create.
    pub posts:     usize,
    /// How many posts to delete (author-authorized) after they have
    /// accumulated some likes.
    pub deletes:   usize,
    /// How many follow edges to retract after the follow graph is built.
    pub unfollows: usize,
}

impl Default for SeedConfig {
    fn default() -> Self {
        Self {
            seed:      1337,
            users:     50,
            posts:     500,
            deletes:   30,
            unfollows: 20,
        }
    }
}

/// What actually got committed — the numbers the CLI prints and a test can
/// assert on. Counts can differ slightly from the `SeedConfig` request
/// (e.g. `deletes` is clamped to the number of posts that exist) but never
/// exceed it.
#[derive(Debug, Default, Clone)]
pub struct SeedReport {
    pub users:     usize,
    pub follows:   usize,
    pub posts:     usize,
    pub likes:     usize,
    pub deletes:   usize,
    pub unfollows: usize,
    pub elapsed:   Duration,
}

// ===========================================================================
// The fresh-dir guard
// ===========================================================================

/// The fresh-`--dir` guard, mirroring `mess_soak::resource::guard_fresh_dir`
/// (bn-3dr) for the same reason it exists there: this generator's
/// determinism claim — same seed, same corpus — only holds starting from an
/// empty log. Seeding into a directory that already has a store would append
/// the corpus *on top of* whatever is already there (doubling it, or worse,
/// mixing two seeds' worth of state), silently breaking reproducibility.
///
/// Returns `Err(message)` naming the leftover artifacts when `dir` exists and
/// is non-empty; `Ok(())` for a missing or empty dir. There is no adopt/merge
/// mode — the caller either points `--dir` at a fresh path or passes
/// `--force` to wipe it first (see `social-seed`'s `main`).
pub fn guard_fresh_dir(dir: &Path) -> Result<(), String> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        // Missing dir: `LogEngine::open` creates it. Nothing to refuse.
        Err(_) => return Ok(()),
    };
    let mut segments = 0usize;
    let mut markers: Vec<String> = Vec::new();
    let mut others = 0usize;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("seg-") && name.ends_with(".log") {
            segments += 1;
        } else if name == "LOCK" || name == "meta" || name == "sealed" {
            markers.push(name);
        } else {
            others += 1;
        }
    }
    if segments == 0 && markers.is_empty() && others == 0 {
        return Ok(());
    }
    markers.sort();
    Err(format!(
        "refusing to seed on non-empty --dir {}: found a leftover store \
         ({segments} seg-*.log segment file(s), markers: [{}]{}) — seeding \
         assumes an empty starting log, so pre-existing events would either \
         double the corpus or mix two seeds' worth of state, silently \
         breaking the \"same seed, same corpus\" guarantee. Pass --force to \
         wipe the directory first, or point --dir at a fresh path.",
        dir.display(),
        markers.join(", "),
        if others > 0 {
            format!(", plus {others} other entr(ies)")
        } else {
            String::new()
        },
    ))
}

// ===========================================================================
// Zipf-ish weighted sampling
// ===========================================================================

/// A precomputed Zipf-like distribution over `0..n`: rank `r`'s weight is
/// `1 / (r + 1) ^ exponent`, so rank 0 is the heaviest. Used both for "who
/// gets followed" (a handful of hub users end up with most of the followers)
/// and "which post gets liked" (a handful of posts go semi-viral) — the
/// classic long-tail shape a lively demo corpus should have instead of a
/// uniform graph.
struct ZipfWeights {
    /// Cumulative weight up to and including rank `i`; `cumulative[n-1]` is
    /// the total.
    cumulative: Vec<f64>,
}

impl ZipfWeights {
    fn new(n: usize, exponent: f64) -> Self {
        let mut cumulative = Vec::with_capacity(n);
        let mut acc = 0.0;
        for rank in 0..n {
            acc += 1.0 / ((rank + 1) as f64).powf(exponent);
            cumulative.push(acc);
        }
        Self { cumulative }
    }

    /// Sample a rank in `0..n`, biased toward 0.
    fn sample(&self, rng: &mut StdRng) -> usize {
        let total = *self.cumulative.last().expect("ZipfWeights::new(0, _)");
        let x: f64 = rng.random::<f64>() * total;
        self.cumulative
            .partition_point(|&c| c < x)
            .min(self.cumulative.len() - 1)
    }
}

// ===========================================================================
// Phrase combinator — handles, display names, post bodies
// ===========================================================================

const ADJ: &[&str] = &[
    "quiet", "brave", "lazy", "witty", "sunny", "cozy", "swift", "calm",
    "bold", "tiny", "spare", "loud", "dusty", "salty", "early", "late",
    "fuzzy", "sharp", "mellow", "vivid", "stormy", "frosty", "rusty", "plucky",
    "nimble", "sturdy", "gentle", "cranky", "sly", "chill",
];

const NOUN: &[&str] = &[
    "otter", "kestrel", "comet", "maple", "ember", "harbor", "quartz",
    "willow", "badger", "canyon", "meridian", "juniper", "ferret", "glacier",
    "thistle", "raven", "cobalt", "pixel", "lantern", "orchard", "tundra",
    "cinder", "heron", "basil", "granite", "marlin", "clover", "vapor",
    "anchor", "kettle",
];

const FIRST: &[&str] = &[
    "Alex",
    "Priya",
    "Jordan",
    "Sam",
    "Morgan",
    "Riley",
    "Casey",
    "Devon",
    "Taylor",
    "Jamie",
    "Robin",
    "Quinn",
    "Avery",
    "Charlie",
    "Skyler",
    "Reese",
    "Rowan",
    "Kai",
    "Nadia",
    "Omar",
    "Yuki",
    "Leah",
    "Marcus",
    "Elena",
    "Theo",
    "Priscilla",
    "Diego",
    "Ines",
    "Noah",
    "Zara",
];

const SURNAME: &[&str] = &[
    "Vance",
    "Okafor",
    "Nakamura",
    "Petrov",
    "Silva",
    "Byrne",
    "Kessler",
    "Odom",
    "Whitfield",
    "Marsh",
    "Delacroix",
    "Falk",
    "Ibarra",
    "Sokolov",
    "Renner",
    "Achebe",
    "Lindqvist",
    "Duarte",
    "Hollis",
    "Bergstrom",
    "Castellano",
    "Mbeki",
    "Winters",
    "Alavi",
    "Hargrove",
    "Novak",
    "Osei",
    "Vidal",
    "Krantz",
    "Amadi",
];

const TOPICS: &[&str] = &[
    "rust",
    "event sourcing",
    "cold brew",
    "trail running",
    "vinyl records",
    "sourdough starters",
    "chess openings",
    "cloud costs",
    "cast iron pans",
    "night hikes",
    "generative art",
    "keyboard layouts",
    "houseplants",
    "vintage synths",
    "distributed systems",
    "bike commuting",
    "tape backups",
    "urban gardening",
    "board games",
    "static site generators",
    "espresso shots",
    "open source",
    "terminal themes",
    "sea kayaking",
    "linux window managers",
    "home labs",
    "3am debugging sessions",
    "stand mixers",
    "letterpress",
    "ultralight backpacking",
];

/// Time-of-day/week flavor prefixes — the "timestamps-in-content" texture the
/// bone asked for: since the domain has no `created_at` field, the *text*
/// carries the sense of when a post happened instead.
const TIME_FLAVORS: &[&str] = &[
    "",
    "",
    "", // empty more often than not — most posts have no time cue
    "3am and",
    "monday morning,",
    "still awake,",
    "fresh coffee in hand,",
    "on the train,",
    "during lunch,",
    "after a long shift,",
    "sunday night,",
    "first thing today,",
    "way too late,",
    "friday afternoon,",
];

/// Draw one syntactically-valid, unique handle (see
/// [`crate::handle_is_valid`]): `{adj}_{noun}`, sometimes with a numeric
/// suffix for extra variety (`quiet_otter`, `byte_forge92`).
fn gen_handle(rng: &mut StdRng, used: &mut HashSet<String>) -> String {
    loop {
        let a = ADJ[rng.random_range(0..ADJ.len())];
        let n = NOUN[rng.random_range(0..NOUN.len())];
        let mut handle = format!("{a}_{n}");
        if rng.random_bool(0.35) {
            handle.push_str(&rng.random_range(1..99u32).to_string());
        }
        debug_assert!(
            crate::handle_is_valid(&handle),
            "generated invalid handle {handle:?}"
        );
        if used.insert(handle.clone()) {
            return handle;
        }
    }
}

/// Draw a human-plausible display name: `"{First} {Surname}"`, occasionally
/// abbreviated to `"{First} {S}."`.
fn gen_display_name(rng: &mut StdRng) -> String {
    let f = FIRST[rng.random_range(0..FIRST.len())];
    let s = SURNAME[rng.random_range(0..SURNAME.len())];
    if rng.random_bool(0.25) {
        format!("{f} {}.", &s[0..1])
    } else {
        format!("{f} {s}")
    }
}

/// Draw one post body from a small template bank crossed with [`TOPICS`] and
/// [`TIME_FLAVORS`] — varied and human-plausible, no lorem ipsum.
fn gen_body(rng: &mut StdRng) -> String {
    let topic = TOPICS[rng.random_range(0..TOPICS.len())];
    let topic2 = TOPICS[rng.random_range(0..TOPICS.len())];
    let time = TIME_FLAVORS[rng.random_range(0..TIME_FLAVORS.len())];
    let t = if time.is_empty() { String::new() } else { format!("{time} ") };
    match rng.random_range(0..20u32) {
        0 => format!("{t}just discovered {topic} and I'm obsessed."),
        1 => format!("hot take: {topic} is criminally underrated."),
        2 => format!("spent the whole weekend on {topic}. worth it."),
        3 => format!("does anyone else think about {topic} way too much?"),
        4 => format!("{topic} + coffee = a good morning."),
        5 => format!("three things I learned about {topic} today."),
        6 => format!("unpopular opinion: {topic} beats {topic2}."),
        7 => format!("finally finished my {topic} project. feels good."),
        8 => format!("can we talk about {topic} for a sec?"),
        9 => format!("{topic} update: still going strong."),
        10 => format!("{t}rabbit-holing on {topic} again."),
        11 => format!("note to self: stop buying more {topic} gear."),
        12 => format!("{topic} is why I haven't slept."),
        13 => format!("PSA: {topic} will change how you think about {topic2}."),
        14 => format!(
            "day {} of trying to get better at {topic}.",
            rng.random_range(1..90u32)
        ),
        15 => format!("{topic} appreciation post."),
        16 => format!("what's everyone's favorite {topic} resource?"),
        17 => format!("started a {topic} log. day one: promising."),
        18 => format!("{t}reading about {topic} instead of sleeping."),
        _ => format!("trying to explain {topic} to my cat. going okay."),
    }
}

// ===========================================================================
// Generation
// ===========================================================================

/// Generate the demo corpus into `store`, entirely through
/// [`WriteOps`](crate::contracts::WriteOps) — see the module docs.
///
/// Generic over `B: Backend` so callers can point this at a real on-disk
/// store or a throwaway test one.
pub async fn generate<B>(store: &EventStore<B>, cfg: &SeedConfig) -> SeedReport
where
    B: Backend,
    B::Error: std::fmt::Display,
{
    let start = Instant::now();
    let mut rng = StdRng::seed_from_u64(cfg.seed);
    let mut report = SeedReport::default();

    // --- users -------------------------------------------------------------
    let mut used_handles = HashSet::new();
    let mut user_ids: Vec<Id> = Vec::with_capacity(cfg.users);
    for _ in 0..cfg.users {
        let handle = gen_handle(&mut rng, &mut used_handles);
        let display = gen_display_name(&mut rng);
        let id = Id::from_u128(rng.random::<u128>());
        store.register(id, handle, display).await.expect(
            "seed: register should never be rejected (handles are validated, \
             ids are fresh)",
        );
        user_ids.push(id);
        report.users += 1;
    }

    // --- follow graph (Zipf-ish: a few hub users end up widely followed) --
    let popularity = ZipfWeights::new(user_ids.len(), 1.0);
    // Out-degree per follower is itself Zipf-skewed toward small numbers
    // (most users follow a few people; a few follow a lot).
    let out_degree = ZipfWeights::new(12, 1.5);
    let mut following: Vec<HashSet<usize>> =
        vec![HashSet::new(); user_ids.len()];
    // Parallel to `following`, but insertion-ordered: the unfollow phase below
    // needs a stable draw order, and `HashSet`'s default hasher is randomized
    // per-process (not seeded by `cfg.seed`), so iterating `following` there
    // would silently reintroduce nondeterminism into an otherwise
    // seed-pure generator. Recording edges as we create them sidesteps it.
    let mut edge_list: Vec<(usize, usize)> = Vec::new();
    let mut follow_order: Vec<usize> = (0..user_ids.len()).collect();
    follow_order.shuffle(&mut rng);
    for &fi in &follow_order {
        let degree = 1 + out_degree.sample(&mut rng);
        let mut added = 0usize;
        let mut attempts = 0usize;
        while added < degree && attempts < degree * 8 {
            attempts += 1;
            let ti = popularity.sample(&mut rng);
            if ti == fi || following[fi].contains(&ti) {
                continue;
            }
            match store.follow(user_ids[fi], user_ids[ti]).await {
                Ok(_) => {
                    following[fi].insert(ti);
                    edge_list.push((fi, ti));
                    report.follows += 1;
                    added += 1;
                }
                Err(e) => {
                    // Should not happen — `following[fi]` mirrors exactly what
                    // the aggregate would reject on. Surfaced rather than
                    // silently swallowed in case it ever does (dogfood signal).
                    eprintln!("seed: unexpected follow rejection: {e}");
                }
            }
        }
    }

    // --- posts (author chosen Zipf-ish: a few prolific posters) -----------
    let author_weights = ZipfWeights::new(user_ids.len(), 0.8);
    let mut post_ids: Vec<Id> = Vec::with_capacity(cfg.posts);
    let mut post_author: Vec<usize> = Vec::with_capacity(cfg.posts);
    for _ in 0..cfg.posts {
        let ai = author_weights.sample(&mut rng);
        let body = gen_body(&mut rng);
        let id = Id::from_u128(rng.random::<u128>());
        store.create_post(id, user_ids[ai], body).await.expect(
            "seed: create_post should never be rejected (bodies are 1..=500 \
             chars)",
        );
        post_ids.push(id);
        post_author.push(ai);
        report.posts += 1;
    }

    // --- likes (Zipf on posts: a handful go semi-viral) --------------------
    if !post_ids.is_empty() {
        let post_popularity = ZipfWeights::new(post_ids.len(), 1.1);
        let mut liked: Vec<HashSet<usize>> =
            vec![HashSet::new(); post_ids.len()];
        let target_likes = (cfg.posts as f64 * 1.6).round() as usize;
        let mut attempts = 0usize;
        while report.likes < target_likes && attempts < target_likes.max(1) * 4
        {
            attempts += 1;
            let pi = post_popularity.sample(&mut rng);
            let ui = rng.random_range(0..user_ids.len());
            if liked[pi].contains(&ui) {
                continue;
            }
            match store.like(post_ids[pi], user_ids[ui]).await {
                Ok(_) => {
                    liked[pi].insert(ui);
                    report.likes += 1;
                }
                Err(e) => eprintln!("seed: unexpected like rejection: {e}"),
            }
        }
    }

    // --- deletes (author-authorized, after posts have accrued likes) -------
    let mut delete_candidates: Vec<usize> = (0..post_ids.len()).collect();
    delete_candidates.shuffle(&mut rng);
    for &pi in delete_candidates.iter().take(cfg.deletes.min(post_ids.len())) {
        match store.delete_post(post_ids[pi], user_ids[post_author[pi]]).await {
            Ok(_) => report.deletes += 1,
            Err(e) => eprintln!("seed: unexpected delete rejection: {e}"),
        }
    }

    // --- unfollows (retract a handful of the follow edges just built) ------
    edge_list.shuffle(&mut rng);
    for &(fi, ti) in edge_list.iter().take(cfg.unfollows.min(edge_list.len())) {
        match store.unfollow(user_ids[fi], user_ids[ti]).await {
            Ok(_) => report.unfollows += 1,
            Err(e) => eprintln!("seed: unexpected unfollow rejection: {e}"),
        }
    }

    report.elapsed = start.elapsed();
    report
}

#[cfg(test)]
mod tests {
    use mess_store::LogEngine;

    use super::*;

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "mess-social-seed-test-{tag}-{}-{}",
            std::process::id(),
            Id::new()
        ))
    }

    fn small_cfg(seed: u64) -> SeedConfig {
        SeedConfig { seed, users: 8, posts: 20, deletes: 2, unfollows: 2 }
    }

    #[tokio::test]
    async fn generate_produces_the_requested_shape() {
        let dir = temp_dir("shape");
        let store =
            EventStore::new(LogEngine::open(&dir).expect("open engine"));
        let report = generate(&store, &small_cfg(42)).await;
        assert_eq!(report.users, 8);
        assert_eq!(report.posts, 20);
        assert!(report.follows > 0, "expected at least one follow edge");
        assert!(report.likes > 0, "expected at least one like");
        assert_eq!(report.deletes, 2);
        assert_eq!(report.unfollows, 2);
    }

    /// The determinism contract the whole bone hinges on: same seed, same
    /// corpus. Compares the two stores' global logs event-for-event —
    /// stream id, wire message type, and encoded payload — which is a
    /// stronger check than comparing folded state (it also pins event
    /// *order* and the generated [`Id`]s themselves).
    #[tokio::test]
    async fn generate_is_deterministic_for_a_fixed_seed() {
        let dir_a = temp_dir("det-a");
        let dir_b = temp_dir("det-b");
        let store_a =
            EventStore::new(LogEngine::open(&dir_a).expect("open engine"));
        let store_b =
            EventStore::new(LogEngine::open(&dir_b).expect("open engine"));

        let cfg = small_cfg(7);
        generate(&store_a, &cfg).await;
        generate(&store_b, &cfg).await;

        let log_a = store_a.backend().read_global(None, 10_000).await.unwrap();
        let log_b = store_b.backend().read_global(None, 10_000).await.unwrap();
        assert_eq!(log_a.len(), log_b.len(), "same event count");
        assert!(!log_a.is_empty());
        for (a, b) in log_a.iter().zip(log_b.iter()) {
            assert_eq!(a.stream_id, b.stream_id);
            assert_eq!(a.message_type, b.message_type);
            assert_eq!(a.data, b.data);
        }
    }

    // ---- fresh-dir guard ----

    #[test]
    fn fresh_dir_guard_accepts_missing_and_empty() {
        let t = temp_dir("guard-empty");
        assert!(guard_fresh_dir(&t.join("does-not-exist")).is_ok());
        std::fs::create_dir_all(&t).unwrap();
        assert!(guard_fresh_dir(&t).is_ok());
        std::fs::remove_dir_all(&t).ok();
    }

    #[test]
    fn fresh_dir_guard_refuses_a_leftover_store() {
        let t = temp_dir("guard-nonempty");
        std::fs::create_dir_all(&t).unwrap();
        std::fs::write(t.join("seg-00000001.log"), b"x").unwrap();
        std::fs::write(t.join("LOCK"), b"x").unwrap();
        let err = guard_fresh_dir(&t).unwrap_err();
        assert!(err.contains("refusing to seed"));
        assert!(err.contains("seg-*.log"));
        std::fs::remove_dir_all(&t).ok();
    }

    #[test]
    fn generated_handles_are_always_valid() {
        let mut rng = StdRng::seed_from_u64(999);
        let mut used = HashSet::new();
        for _ in 0..500 {
            let h = gen_handle(&mut rng, &mut used);
            assert!(crate::handle_is_valid(&h), "invalid handle: {h:?}");
        }
    }

    #[test]
    fn generated_bodies_are_within_bounds() {
        let mut rng = StdRng::seed_from_u64(555);
        for _ in 0..500 {
            let b = gen_body(&mut rng);
            let len = b.chars().count();
            assert!(
                (1..=crate::BODY_MAX_LEN).contains(&len),
                "body out of bounds: {len} chars"
            );
        }
    }
}
