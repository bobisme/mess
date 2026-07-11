//! The deterministic corpus generator (bn-1mw; scale tiers + pipelining
//! bn-o9z).
//!
//! [`generate`] drives users, a Zipf-ish follow graph, posts with varied
//! phrase-combinator bodies, Zipf-distributed likes, a handful of deletes, and
//! a handful of unfollows — **entirely through
//! [`WriteOps`](crate::contracts::WriteOps)**, i.e. one real warm-path
//! [`EventStore::command_cached`](mess_store::EventStore::command_cached) call
//! per action, the same call path a real client makes. There is no raw-append
//! shortcut: every invariant (`handle_is_valid`, no self-follow, no
//! double-like, …) is enforced by the real `Decide` impls exactly as in
//! production, which is also why the planner tracks shadow state (who follows
//! whom, who liked what) — not to skip validation, but to avoid *drawing* a
//! command it knows would be rejected.
//!
//! Two tiers, selected by [`SeedConfig`]: [`SeedConfig::demo`] is the
//! historical ~1,488-event world (sequential, byte-identical),
//! [`SeedConfig::large`] is a ~50k+-event world with realistic skew (celebrity
//! users, viral posts, a long tail), seeded with bounded-concurrency pipelining
//! per `docs/perf/bulk-writes.md`.
//!
//! Generic over `B: SnapshotStore + Clone` (not pinned to
//! [`crate::store_backend::Store`]) so the same generator seeds a real on-disk
//! snapshot store *and* an in-memory test one —
//! [`tests::demo_seed_is_byte_identical_for_a_fixed_seed`] below exercises the
//! latter.
//!
//! # Determinism (plan / execute split)
//!
//! [`plan_corpus`] draws the whole corpus from one [`StdRng`] seeded from
//! [`SeedConfig::seed`](SeedConfig::seed) — no wall-clock, no store, no
//! `await` — with a fixed draw order, so the *corpus* (handles, display names,
//! bodies, the follow/like graphs, which posts get deleted, and the
//! [`draw_id`]-drawn ids themselves) is a pure function of `seed`. Ids come
//! from [`Id::from_parts`] fed a fixed [`SEED_BASE_MS`] timestamp plus 10
//! bytes pulled straight off the seeded RNG — never [`Id::new`]/wall-clock —
//! which is what keeps the corpus deterministic (see `draw_id`).
//! Execution then honors [`SeedConfig::concurrency`]: sequential execution
//! (demo) reproduces the historical **byte-identical** global log; pipelined
//! execution (large) commits distinct streams concurrently, so per-stream
//! heads and total counts stay deterministic while the global interleaving does
//! not — see [`SeedConfig`]'s docs and the two determinism tests below.

use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::time::{Duration, Instant};

use mess_store::{EventStore, SnapshotStore};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use tokio::task::JoinSet;

use crate::Id;
use crate::contracts::{WriteError, WriteOps};

// ===========================================================================
// Config / report
// ===========================================================================

/// Corpus shape. The `Default` is the **demo** tier (~50 users, ~500 posts, the
/// deterministic 1,488-event world the README tours); override individual
/// fields (e.g. in a fast test) via struct update syntax:
/// `SeedConfig { users: 8, posts: 20, ..SeedConfig::default() }`. Use
/// [`SeedConfig::large`] for the ~50k+-event scale tier.
///
/// # Determinism and `concurrency`
///
/// The *corpus* — every id, handle, body, and the follow/like graphs — is a
/// pure function of [`seed`](Self::seed): [`plan_corpus`] draws it from one
/// seeded [`StdRng`] with a fixed draw order, entirely before any store write.
/// [`concurrency`](Self::concurrency) then only affects **how** the planned
/// commands are executed, never *what* they are:
///
/// - `concurrency == 1` (demo default) executes every command sequentially in
///   plan order, so the global log is **byte-identical** run-to-run (the
///   demo-tier determinism test asserts exactly this).
/// - `concurrency > 1` (large tier) fans the commands of each phase out over
///   [`JoinSet`] with a bounded window (the `docs/perf/bulk-writes.md`
///   pattern), across **distinct** streams only — so per-stream event order is
///   still deterministic and folded heads/counts are identical run-to-run, but
///   the *global interleaving* (and thus each event's global position) is not.
///   The large-tier determinism test asserts equal counts + watermark + equal
///   spot-checked stream heads accordingly.
#[derive(Debug, Clone)]
pub struct SeedConfig {
    /// The PRNG seed. Same seed, same corpus.
    pub seed:                u64,
    /// How many users to register.
    pub users:               usize,
    /// How many posts to create.
    pub posts:               usize,
    /// How many posts to delete (author-authorized) after they have
    /// accumulated some likes.
    pub deletes:             usize,
    /// How many follow edges to retract after the follow graph is built.
    pub unfollows:           usize,
    /// Average target likes per post, as a multiplier on
    /// [`posts`](Self::posts) (`target_likes = round(posts *
    /// like_factor)`). Demo uses `1.6`; the large tier turns this up so a
    /// handful of Zipf-hot posts collect thousands of likes.
    pub like_factor:         f64,
    /// Typical follow out-degree ceiling: each user's out-degree is drawn Zipf
    /// over `1..=out_degree` (skewed toward the low end), so the mean is well
    /// below this. Larger values thicken the follow graph, which — with the
    /// Zipf *popularity* target — is what gives a couple of celebrity users
    /// thousands of followers at the large tier.
    pub out_degree:          usize,
    /// Zipf exponent for *who gets followed* — the follow-target weighting:
    /// rank `r`'s weight is `1 / (r+1)^exponent`, so a larger exponent
    /// concentrates followers on the top few users. Demo uses `1.0` (the
    /// historical value, preserved for byte-identity); the large tier sharpens
    /// it so the top one or two users become celebrities with thousands of
    /// followers. (Post *like* popularity is a separate fixed `1.1` — sharp
    /// enough at any scale that the top posts go viral — so it is not
    /// parameterized here.)
    pub popularity_exponent: f64,
    /// Bounded execution concurrency (see the struct docs). `1` = sequential,
    /// byte-identical demo tier; `>1` = pipelined large tier.
    pub concurrency:         usize,
}

impl Default for SeedConfig {
    fn default() -> Self { Self::demo() }
}

impl SeedConfig {
    /// The **demo** tier: the deterministic ~1,488-event world the README tours
    /// and every existing test uses. Sequential (byte-identical) by default.
    #[must_use]
    pub fn demo() -> Self {
        Self {
            seed:                1337,
            users:               50,
            posts:               500,
            deletes:             30,
            unfollows:           20,
            like_factor:         1.6,
            out_degree:          12,
            popularity_exponent: 1.0,
            concurrency:         1,
        }
    }

    /// The **large** tier: a deterministic bigger world with realistic skew —
    /// ~3.5k users, ~3.5k posts, a sharply-Zipf follow graph whose top one or
    /// two users are celebrities with thousands of followers, and ~38k likes
    /// whose top posts go viral with thousands each. Totals ~50k+ events.
    /// Pipelined (`concurrency = 32`) so it rides the durable committer's
    /// group-commit window (see `docs/perf/bulk-writes.md`) — under a
    /// fsync-per-batch durability it seeds in a fraction of the sequential
    /// wall-clock.
    #[must_use]
    pub fn large(seed: u64) -> Self {
        Self {
            seed,
            users: 3_500,
            posts: 3_500,
            deletes: 200,
            unfollows: 200,
            like_factor: 10.0,
            out_degree: 20,
            // Sharper than demo's 1.0 so the top users become genuine
            // celebrities (thousands of followers) and the top posts genuinely
            // go viral, rather than a gentle long tail.
            popularity_exponent: 1.5,
            concurrency: 32,
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
// Planning: the deterministic, store-free corpus draw
// ===========================================================================

/// A fixed, arbitrary Unix-millisecond timestamp
/// (2024-01-01T00:00:00Z) every seeded id's embedded UUIDv7 timestamp is
/// drawn from — see [`draw_id`]. Fixed rather than wall-clock so the corpus
/// stays a pure function of [`SeedConfig::seed`]; the actual value carries no
/// meaning beyond "some fixed point", since the corpus's realism never
/// depends on ids' timestamps looking like "now".
const SEED_BASE_MS: u64 = 1_704_067_200_000;

/// Draw one deterministic [`Id`] from the seeded RNG: [`SEED_BASE_MS`] plus
/// 10 bytes pulled off `rng`. This is the seed generator's replacement for
/// the pre-bn-gt5 `Id::from_u128(rng.random())` draw — same "pure function of
/// the RNG stream, same draw order every run" contract, going through
/// [`Id::from_parts`] since the UUIDv7-backed `Id` has no raw-`u128`
/// constructor (a v7 id is not "any 128 bits", it has a fixed shape — see
/// `id`'s module docs).
fn draw_id(rng: &mut StdRng) -> Id {
    Id::from_parts(SEED_BASE_MS, rng.random())
}

/// The fully-planned corpus: every command's arguments, drawn from the seeded
/// [`StdRng`] with a fixed draw order, **before** any store write. Splitting
/// planning from execution is what lets execution be either sequential
/// (byte-identical demo tier) or pipelined (large tier) without changing a
/// single id, body, or edge — see [`SeedConfig`]'s determinism docs.
///
/// Each `Vec` is one execution *phase*, and within a phase every entry targets
/// a **distinct** stream (users by id, follows/likes by their unique edge,
/// posts by id), which is exactly what makes the phase safe to fan out
/// concurrently over distinct streams (`docs/perf/bulk-writes.md` §2). Phases
/// run in order, so the two streams an id touches across phases (a post's
/// create then delete, an edge's follow then unfollow) are never concurrent.
struct Corpus {
    /// `(user_id, handle, display_name)`.
    users:     Vec<(Id, String, String)>,
    /// `(follower_id, target_id)`.
    follows:   Vec<(Id, Id)>,
    /// `(post_id, author_id, body)`.
    posts:     Vec<(Id, Id, String)>,
    /// `(post_id, user_id)`.
    likes:     Vec<(Id, Id)>,
    /// `(post_id, author_id)`.
    deletes:   Vec<(Id, Id)>,
    /// `(follower_id, target_id)`.
    unfollows: Vec<(Id, Id)>,
}

/// Draw the whole corpus deterministically from `cfg.seed`. Pure: no store, no
/// clock, no `await`. The RNG draw order is pinned (handles→names→ids for
/// users; shuffle→degree→targets for follows; author→body→id for posts;
/// post→user for likes; the two shuffles for deletes/unfollows), so the demo
/// tier reproduces its historical byte-identical 1,488-event corpus and any
/// tier is a pure function of the seed.
fn plan_corpus(cfg: &SeedConfig) -> Corpus {
    let mut rng = StdRng::seed_from_u64(cfg.seed);

    // --- users -------------------------------------------------------------
    let mut used_handles = HashSet::new();
    let mut user_ids: Vec<Id> = Vec::with_capacity(cfg.users);
    let mut users: Vec<(Id, String, String)> = Vec::with_capacity(cfg.users);
    for _ in 0..cfg.users {
        let handle = gen_handle(&mut rng, &mut used_handles);
        let display = gen_display_name(&mut rng);
        let id = draw_id(&mut rng);
        user_ids.push(id);
        users.push((id, handle, display));
    }

    // --- follow graph (Zipf-ish: a few hub users end up widely followed) --
    let popularity = ZipfWeights::new(user_ids.len(), cfg.popularity_exponent);
    // Out-degree per follower is itself Zipf-skewed toward small numbers
    // (most users follow a few people; a few follow a lot).
    let out_degree = ZipfWeights::new(cfg.out_degree, 1.5);
    let mut following: Vec<HashSet<usize>> =
        vec![HashSet::new(); user_ids.len()];
    // Parallel to `following`, but insertion-ordered: the unfollow phase below
    // needs a stable draw order, and `HashSet`'s default hasher is randomized
    // per-process (not seeded by `cfg.seed`), so iterating `following` there
    // would silently reintroduce nondeterminism into an otherwise
    // seed-pure generator. Recording edges as we create them sidesteps it.
    let mut edge_index: Vec<(usize, usize)> = Vec::new();
    let mut follows: Vec<(Id, Id)> = Vec::new();
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
            following[fi].insert(ti);
            edge_index.push((fi, ti));
            follows.push((user_ids[fi], user_ids[ti]));
            added += 1;
        }
    }

    // --- posts (author chosen Zipf-ish: a few prolific posters) -----------
    let author_weights = ZipfWeights::new(user_ids.len(), 0.8);
    let mut post_ids: Vec<Id> = Vec::with_capacity(cfg.posts);
    let mut post_author: Vec<usize> = Vec::with_capacity(cfg.posts);
    let mut posts: Vec<(Id, Id, String)> = Vec::with_capacity(cfg.posts);
    for _ in 0..cfg.posts {
        let ai = author_weights.sample(&mut rng);
        let body = gen_body(&mut rng);
        let id = draw_id(&mut rng);
        post_ids.push(id);
        post_author.push(ai);
        posts.push((id, user_ids[ai], body));
    }

    // --- likes (Zipf on posts: a handful go semi-viral) --------------------
    let mut likes: Vec<(Id, Id)> = Vec::new();
    if !post_ids.is_empty() {
        let post_popularity = ZipfWeights::new(post_ids.len(), 1.1);
        let mut liked: Vec<HashSet<usize>> =
            vec![HashSet::new(); post_ids.len()];
        let target_likes =
            (cfg.posts as f64 * cfg.like_factor).round() as usize;
        let mut attempts = 0usize;
        while likes.len() < target_likes && attempts < target_likes.max(1) * 4 {
            attempts += 1;
            let pi = post_popularity.sample(&mut rng);
            let ui = rng.random_range(0..user_ids.len());
            if liked[pi].contains(&ui) {
                continue;
            }
            liked[pi].insert(ui);
            likes.push((post_ids[pi], user_ids[ui]));
        }
    }

    // --- deletes (author-authorized, after posts have accrued likes) -------
    let mut delete_candidates: Vec<usize> = (0..post_ids.len()).collect();
    delete_candidates.shuffle(&mut rng);
    let deletes: Vec<(Id, Id)> = delete_candidates
        .iter()
        .take(cfg.deletes.min(post_ids.len()))
        .map(|&pi| (post_ids[pi], user_ids[post_author[pi]]))
        .collect();

    // --- unfollows (retract a handful of the follow edges just built) ------
    edge_index.shuffle(&mut rng);
    let unfollows: Vec<(Id, Id)> = edge_index
        .iter()
        .take(cfg.unfollows.min(edge_index.len()))
        .map(|&(fi, ti)| (user_ids[fi], user_ids[ti]))
        .collect();

    Corpus { users, follows, posts, likes, deletes, unfollows }
}

// ===========================================================================
// Execution: sequential (demo) or bounded-concurrency pipelined (large)
// ===========================================================================

/// Drive an iterator of single-command futures to completion with bounded
/// concurrency `k`. `k <= 1` awaits them **in order** (the byte-identical demo
/// path); `k > 1` runs a bounded [`JoinSet`] window of `k` in flight, refilled
/// one-for-one as each completes — the `docs/perf/bulk-writes.md` §2.2 pattern.
///
/// Every future is one seeded `WriteOps` command. A seeded corpus never
/// produces a rejection (the plan mirrors every invariant: valid handles,
/// distinct edges, authorized deletes), so a `WriteError` here is a real bug
/// and panics loudly rather than being silently swallowed.
async fn drive<I, Fut>(iter: I, k: usize)
where
    I: IntoIterator<Item = Fut>,
    Fut: Future<Output = Result<u64, WriteError>> + Send + 'static,
{
    if k <= 1 {
        for fut in iter {
            fut.await.expect("seed: command unexpectedly rejected");
        }
        return;
    }
    let mut it = iter.into_iter();
    let mut set: JoinSet<Result<u64, WriteError>> = JoinSet::new();
    for fut in it.by_ref().take(k) {
        set.spawn(fut);
    }
    while let Some(joined) = set.join_next().await {
        joined
            .expect("seed: command task panicked")
            .expect("seed: command unexpectedly rejected");
        if let Some(fut) = it.next() {
            set.spawn(fut);
        }
    }
}

/// Generate a corpus into `store`, entirely through
/// [`WriteOps`](crate::contracts::WriteOps) — the same warm-path
/// [`command_cached`](mess_store::EventStore::command_cached) call a real
/// client makes, one per action, with every invariant enforced by the real
/// `Decide` impls (see the module docs).
///
/// Planning is deterministic ([`plan_corpus`]); execution honors
/// [`SeedConfig::concurrency`] — sequential and byte-identical at the demo
/// tier, bounded-concurrency pipelined over distinct streams at the large tier.
/// The returned [`SeedReport`] carries the committed counts and the measured
/// execution wall-clock (planning excluded — it is store-free and negligible).
///
/// Generic over `B: SnapshotStore + Clone` (not pinned to
/// [`crate::store_backend::Store`]) so the same generator seeds a real on-disk
/// snapshot store *and* an in-memory test one. `Clone` is needed for the
/// pipelined fan-out: each in-flight task owns a cheap clone of the store
/// handle (they share one backend + warm cache), per the bulk-writes guide.
pub async fn generate<B>(store: &EventStore<B>, cfg: &SeedConfig) -> SeedReport
where
    B: SnapshotStore + Clone,
    B::Error: std::fmt::Display,
{
    let corpus = plan_corpus(cfg);
    let report = SeedReport {
        users:     corpus.users.len(),
        follows:   corpus.follows.len(),
        posts:     corpus.posts.len(),
        likes:     corpus.likes.len(),
        deletes:   corpus.deletes.len(),
        unfollows: corpus.unfollows.len(),
        elapsed:   Duration::default(),
    };
    let k = cfg.concurrency;
    let start = Instant::now();

    // Phase order is the global-log order at the demo tier: users, follows,
    // posts, likes, deletes, unfollows. Each phase's streams are distinct, and
    // phases are sequential, so no stream is ever written concurrently with
    // itself (create-before-delete, follow-before-unfollow both hold).
    drive(
        corpus.users.into_iter().map(|(id, handle, display)| {
            let s = store.clone();
            async move { s.register(id, handle, display).await }
        }),
        k,
    )
    .await;
    drive(
        corpus.follows.into_iter().map(|(f, t)| {
            let s = store.clone();
            async move { s.follow(f, t).await }
        }),
        k,
    )
    .await;
    drive(
        corpus.posts.into_iter().map(|(id, author, body)| {
            let s = store.clone();
            async move { s.create_post(id, author, body).await }
        }),
        k,
    )
    .await;
    drive(
        corpus.likes.into_iter().map(|(p, u)| {
            let s = store.clone();
            async move { s.like(p, u).await }
        }),
        k,
    )
    .await;
    drive(
        corpus.deletes.into_iter().map(|(p, by)| {
            let s = store.clone();
            async move { s.delete_post(p, by).await }
        }),
        k,
    )
    .await;
    drive(
        corpus.unfollows.into_iter().map(|(f, t)| {
            let s = store.clone();
            async move { s.unfollow(f, t).await }
        }),
        k,
    )
    .await;

    SeedReport { elapsed: start.elapsed(), ..report }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use mess_store::{Backend, FjallSnapshotBackend, LogEngine, StoredRecord};
    use mess_testkit::{SweepingTempDir, sweeping_temp_dir};

    use super::*;

    /// A snapshot-backed store on a self-sweeping temp dir (the real-fs TMPDIR
    /// rule). The returned [`SweepingTempDir`] guard must be held (and dropped
    /// last) for the duration of the test.
    fn snap_store(
        tag: &str,
    ) -> (EventStore<FjallSnapshotBackend<LogEngine>>, SweepingTempDir) {
        let dir = sweeping_temp_dir(tag);
        let engine =
            LogEngine::open(dir.path().join("log")).expect("open engine");
        let backend =
            FjallSnapshotBackend::open(engine, dir.path().join("snap"))
                .expect("open snapshot backend");
        (EventStore::new(backend), dir)
    }

    fn small_cfg(seed: u64) -> SeedConfig {
        SeedConfig {
            seed,
            users: 8,
            posts: 20,
            deletes: 2,
            unfollows: 2,
            ..SeedConfig::demo()
        }
    }

    /// A `stream_id -> Vec<(message_type, data)>` view of a store's whole log:
    /// the per-stream event sequence, independent of the global interleaving.
    /// Two stores seeded from the same seed have identical per-stream maps
    /// regardless of execution concurrency (pipelining reorders *across*
    /// streams, never *within* one).
    async fn per_stream_events<B: Backend>(
        store: &EventStore<B>,
    ) -> BTreeMap<String, Vec<(String, Vec<u8>)>> {
        let log: Vec<StoredRecord> =
            store.backend().read_global(None, 1_000_000).await.unwrap();
        let mut map: BTreeMap<String, Vec<(String, Vec<u8>)>> = BTreeMap::new();
        for rec in log {
            map.entry(rec.stream_id.clone())
                .or_default()
                .push((rec.message_type.clone(), rec.data.clone()));
        }
        map
    }

    #[tokio::test]
    async fn generate_produces_the_requested_shape() {
        let (store, _dir) = snap_store("shape");
        let report = generate(&store, &small_cfg(42)).await;
        assert_eq!(report.users, 8);
        assert_eq!(report.posts, 20);
        assert!(report.follows > 0, "expected at least one follow edge");
        assert!(report.likes > 0, "expected at least one like");
        assert_eq!(report.deletes, 2);
        assert_eq!(report.unfollows, 2);
    }

    /// The demo-tier determinism contract the whole bone hinges on: same seed,
    /// **byte-identical** global log. Compares the two stores' global logs
    /// event-for-event — stream id, wire message type, and encoded payload —
    /// which is a stronger check than comparing folded state (it also pins
    /// event *order* and the generated [`Id`]s themselves). Demo tier is
    /// sequential (`concurrency == 1`), so the global interleaving is
    /// deterministic too.
    #[tokio::test]
    async fn demo_seed_is_byte_identical_for_a_fixed_seed() {
        let (store_a, _da) = snap_store("det-a");
        let (store_b, _db) = snap_store("det-b");

        let cfg = small_cfg(7);
        assert_eq!(cfg.concurrency, 1, "demo tier is sequential");
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

    /// Pipelining preserves the corpus: a demo-shaped seed run **sequentially**
    /// and **pipelined** (`concurrency > 1`) commits the exact same per-stream
    /// event sequences and the same total event count — only the global
    /// interleaving differs. This is the property the large-tier determinism
    /// rests on, proven cheaply at demo scale in the normal test run.
    #[tokio::test]
    async fn pipelined_execution_matches_sequential_per_stream() {
        let (seq_store, _ds) = snap_store("pipe-seq");
        let (pipe_store, _dp) = snap_store("pipe-conc");

        let seq_cfg = small_cfg(11);
        let pipe_cfg = SeedConfig { concurrency: 8, ..seq_cfg.clone() };

        let seq_report = generate(&seq_store, &seq_cfg).await;
        let pipe_report = generate(&pipe_store, &pipe_cfg).await;

        assert_eq!(seq_report.users, pipe_report.users);
        assert_eq!(seq_report.follows, pipe_report.follows);
        assert_eq!(seq_report.posts, pipe_report.posts);
        assert_eq!(seq_report.likes, pipe_report.likes);

        let seq_map = per_stream_events(&seq_store).await;
        let pipe_map = per_stream_events(&pipe_store).await;
        assert_eq!(
            seq_map, pipe_map,
            "per-stream event sequences must be identical regardless of \
             execution concurrency"
        );
    }

    /// Max follower count (net of unfollows) and max likes-on-one-post in a
    /// planned corpus — the realistic-skew evidence.
    fn skew(corpus: &Corpus) -> (usize, usize) {
        let mut followers: HashMap<Id, i64> = HashMap::new();
        for (_f, t) in &corpus.follows {
            *followers.entry(*t).or_default() += 1;
        }
        for (_f, t) in &corpus.unfollows {
            *followers.entry(*t).or_default() -= 1;
        }
        let mut likes: HashMap<Id, usize> = HashMap::new();
        for (p, _u) in &corpus.likes {
            *likes.entry(*p).or_default() += 1;
        }
        let max_followers =
            followers.values().copied().max().unwrap_or(0).max(0) as usize;
        let max_likes = likes.values().copied().max().unwrap_or(0);
        (max_followers, max_likes)
    }

    fn corpus_events(c: &Corpus) -> usize {
        c.users.len()
            + c.follows.len()
            + c.posts.len()
            + c.likes.len()
            + c.deletes.len()
            + c.unfollows.len()
    }

    /// The large tier hits its target scale (>=50k events) and realistic skew
    /// (a celebrity with thousands of followers, a viral post with thousands of
    /// likes). Planning-only (no store), so it runs in the normal suite and its
    /// printed line is the README's skew evidence.
    #[test]
    fn large_tier_has_target_scale_and_skew() {
        let cfg = SeedConfig::large(1337);
        let corpus = plan_corpus(&cfg);
        let events = corpus_events(&corpus);
        let (max_followers, max_likes) = skew(&corpus);
        println!(
            "large tier @seed=1337: {events} events ({} users, {} follows, {} \
             posts, {} likes, {} deletes, {} unfollows); top user {} \
             followers; hottest post {} likes",
            corpus.users.len(),
            corpus.follows.len(),
            corpus.posts.len(),
            corpus.likes.len(),
            corpus.deletes.len(),
            corpus.unfollows.len(),
            max_followers,
            max_likes,
        );
        assert!(
            events >= 50_000,
            "large tier must be >=50k events, got {events}"
        );
        assert!(
            max_followers >= 2_000,
            "a celebrity should have thousands of followers, got \
             {max_followers}"
        );
        assert!(
            max_likes >= 2_000,
            "a hot post should have thousands of likes, got {max_likes}"
        );
    }

    /// Large-scale determinism: two **pipelined** runs of the same seed commit
    /// the same event count, reach the same final watermark, and produce
    /// byte-identical per-stream event sequences (pipelining reorders only the
    /// global interleaving). `#[ignore]`d because it seeds a big corpus twice;
    /// run on demand with `--ignored`. Uses a mid-size tier so the whole test
    /// stays within a couple of minutes while still exercising the pipeline at
    /// tens of thousands of events.
    #[tokio::test]
    #[ignore = "seeds a large corpus twice; run on demand"]
    async fn large_scale_pipelined_determinism() {
        // ~24k events: big enough to exercise the pipeline hard, small enough
        // to seed twice within the measured-run budget.
        let cfg = SeedConfig {
            users: 1_500,
            posts: 1_500,
            like_factor: 12.0,
            ..SeedConfig::large(1337)
        };
        let (store_a, _da) = snap_store("large-det-a");
        let (store_b, _db) = snap_store("large-det-b");
        let ra = generate(&store_a, &cfg).await;
        let rb = generate(&store_b, &cfg).await;

        assert_eq!(ra.users, rb.users);
        assert_eq!(ra.follows, rb.follows);
        assert_eq!(ra.posts, rb.posts);
        assert_eq!(ra.likes, rb.likes);

        let wa = store_a.backend().read_global(None, 1_000_000).await.unwrap();
        let wb = store_b.backend().read_global(None, 1_000_000).await.unwrap();
        assert_eq!(wa.len(), wb.len(), "equal event count (final watermark)");
        assert!(wa.len() > 20_000, "should be a large corpus");

        // Full per-stream byte-compare (cheap enough at this scale).
        assert_eq!(
            per_stream_events(&store_a).await,
            per_stream_events(&store_b).await,
            "per-stream event sequences identical across pipelined runs"
        );
    }

    // ---- fresh-dir guard ----

    #[test]
    fn fresh_dir_guard_accepts_missing_and_empty() {
        // Self-sweeping temp dir (the real-fs TMPDIR rule): `t.path()` already
        // exists (empty) for the second assertion; a not-yet-created child of
        // it covers the "missing" case. The guard cleans up on drop at the end
        // of this scope (the success path); the sweep is the safety net if a
        // future panic ever short-circuits that.
        let t = sweeping_temp_dir("seed-guard-empty");
        assert!(guard_fresh_dir(&t.path().join("does-not-exist")).is_ok());
        assert!(guard_fresh_dir(t.path()).is_ok());
    }

    #[test]
    fn fresh_dir_guard_refuses_a_leftover_store() {
        let t = sweeping_temp_dir("seed-guard-nonempty");
        std::fs::write(t.path().join("seg-00000001.log"), b"x").unwrap();
        std::fs::write(t.path().join("LOCK"), b"x").unwrap();
        let err = guard_fresh_dir(t.path()).unwrap_err();
        assert!(err.contains("refusing to seed"));
        assert!(err.contains("seg-*.log"));
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
