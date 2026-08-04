//! The deterministic corpus generator: the thing that makes this crate an
//! instrument rather than a toy.
//!
//! [`generate`] registers users, opens channels, posts Zipf-distributed
//! messages with realistic payload sizes, and adds reactions — **entirely
//! through [`WriteOps`](crate::ops::WriteOps)**, i.e. one real warm-path
//! [`EventStore::command_cached`](mess_store::EventStore::command_cached) call
//! per action, the same call path a real client makes. There is no raw-append
//! shortcut: every invariant (`handle_is_valid`, `slug_is_valid`, a reaction's
//! target ordinal, the archive rule) is enforced by the real `Decide` impls
//! exactly as in production, which is why the planner tracks shadow state — not
//! to skip validation, but to avoid *drawing* a command it knows would be
//! rejected.
//!
//! # The shape, and why it matters
//!
//! Channel choice is Zipf-weighted, so message volume concentrates: the top
//! channel of a demo-scale corpus holds thousands of events while the tail
//! holds tens. Users are drawn near-uniformly, so every user stream stays one
//! or two events deep. One corpus, both pressures — deep streams *and* a large
//! stream registry — which is the whole point (see the crate docs).
//!
//! # Payload realism
//!
//! Message bodies are **lognormal-ish in the 100–800 byte band**
//! ([`MIN_BODY_BYTES`]..=[`MAX_BODY_BYTES`]), drawn from the seeded PRNG via
//! Box–Muller. That band is what makes `--segment-bytes` a meaningful knob: a
//! segment holds a realistic number of realistically-sized records, so roll
//! cadence and sealed-segment shape resemble a real store's rather than a
//! micro-benchmark's.
//!
//! # Determinism (plan / execute split)
//!
//! [`plan_corpus`] draws the whole corpus from one seeded [`fastrand::Rng`]
//! — no wall clock, no store, no `await` — with a fixed draw order, so the
//! corpus (handles, slugs, bodies, the channel/author choices, and the ids
//! themselves via [`Id::from_parts`]) is a pure function of
//! [`SeedConfig::seed`]. Execution then honours
//! [`SeedConfig::concurrency`]:
//!
//! - `concurrency == 1` executes every command in plan order, so the global log
//!   is **byte-identical** run to run.
//! - `concurrency > 1` runs whole *channels* concurrently (and users/reactions
//!   fanned out over distinct streams), so per-stream event order — and
//!   therefore every message ordinal — is still deterministic while the global
//!   interleaving is not.
//!
//! `tests/determinism.rs` asserts both halves.

use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::time::{Duration, Instant};

use fastrand::Rng;
use mess_store::{EventStore, SnapshotStore};
use tokio::task::JoinSet;

use crate::Id;
use crate::ops::{WriteError, WriteOps};
use crate::store_backend::StoreConfig;

// ===========================================================================
// Config / report
// ===========================================================================

/// The scale presets `--scale` selects. Each sets sensible defaults for every
/// knob; an explicit flag on the command line overrides whatever the preset
/// chose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scale {
    /// A multi-segment sealed store in seconds — the default, and what the
    /// tests assert against.
    Demo,
    /// A gigabyte-class corpus: hundreds of channels, tens of thousands of
    /// users, millions of messages.
    Large,
    /// Several gigabytes: the cold-open-at-scale and retention shape.
    Huge,
}

impl Scale {
    /// The `--scale` spelling.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Scale::Demo => "demo",
            Scale::Large => "large",
            Scale::Huge => "huge",
        }
    }

    /// Parse a `--scale` argument.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "demo" => Ok(Scale::Demo),
            "large" => Ok(Scale::Large),
            "huge" => Ok(Scale::Huge),
            other => {
                Err(format!("--scale must be demo|large|huge, got {other:?}"))
            }
        }
    }
}

/// Corpus shape plus the store knobs the shape needs to actually roll
/// segments. The `Default` is the [`Scale::Demo`] tier; override individual
/// fields with struct update syntax.
#[derive(Debug, Clone)]
pub struct SeedConfig {
    /// The PRNG seed. Same seed, same corpus.
    pub seed:             u64,
    /// How many channels to open (the deep streams).
    pub channels:         usize,
    /// How many users to register (the shallow streams — registry pressure).
    pub users:            usize,
    /// How many messages to post across all channels.
    pub messages:         usize,
    /// Reactions to add, as a multiplier on [`messages`](Self::messages).
    pub reaction_factor:  f64,
    /// Zipf exponent for *which channel* a message lands in: rank `r`'s weight
    /// is `1 / (r + 1) ^ exponent`. Larger concentrates traffic on the top few
    /// channels — the hot-channel skew a real chat product has.
    pub channel_exponent: f64,
    /// How many users change their display name (a second event on a shallow
    /// stream, so the user family is not uniformly one-deep).
    pub renames:          usize,
    /// How many channels are archived at the end.
    pub archives:         usize,
    /// Bounded execution concurrency. `1` = sequential and byte-identical.
    pub concurrency:      usize,
    /// **The knob `examples/social` lacks.** Active-segment size in bytes;
    /// small values force rolls, and every roll is sealed.
    pub segment_bytes:    u64,
    /// `true` for one consolidated `.seal` pack per sealed segment (the engine
    /// default); `false` for the legacy loose sidecar trio.
    pub seal_pack:        bool,
}

impl Default for SeedConfig {
    fn default() -> Self { Self::for_scale(Scale::Demo, 1337) }
}

impl SeedConfig {
    /// The preset for `scale`, seeded with `seed`.
    #[must_use]
    pub fn for_scale(scale: Scale, seed: u64) -> Self {
        match scale {
            // ~20k messages of ~100-800 B over 24 channels in a store whose
            // segments are 1 MiB: roughly ten segments, so a demo seed always
            // leaves a MULTI-SEGMENT SEALED store behind. That is the bone's
            // core assertion (`tests/segments.rs`), and it is why the demo
            // preset is much bigger than social's 1,488-event demo.
            Scale::Demo => Self {
                seed,
                channels: 24,
                users: 400,
                messages: 20_000,
                reaction_factor: 0.6,
                channel_exponent: 1.1,
                renames: 40,
                archives: 2,
                // Sequential: byte-identical global log, and 20k warm-path
                // commands is a second or two.
                concurrency: 1,
                segment_bytes: 1024 * 1024,
                seal_pack: true,
            },
            // Gigabyte class: ~3M messages at ~350 B mean is >1 GB of log
            // before reactions, across ~20 sealed 64 MiB segments, with 25k
            // registered users pressing on the stream registry.
            Scale::Large => Self {
                seed,
                channels: 256,
                users: 25_000,
                messages: 3_000_000,
                reaction_factor: 0.8,
                channel_exponent: 1.2,
                renames: 2_500,
                archives: 12,
                concurrency: 32,
                segment_bytes: 64 * 1024 * 1024,
                seal_pack: true,
            },
            // Several gigabytes and ~40 sealed segments: the cold-open-at-
            // scale and retention shape.
            Scale::Huge => Self {
                seed,
                channels: 1_024,
                users: 150_000,
                messages: 12_000_000,
                reaction_factor: 1.0,
                channel_exponent: 1.3,
                renames: 15_000,
                archives: 40,
                concurrency: 32,
                segment_bytes: 128 * 1024 * 1024,
                seal_pack: true,
            },
        }
    }

    /// The demo preset at the default seed.
    #[must_use]
    pub fn demo() -> Self { Self::for_scale(Scale::Demo, 1337) }

    /// A deliberately tiny corpus that still rolls **several** segments —
    /// what the test suite seeds. Small segments, not few events: the point is
    /// to reach the sealed tier in under a second.
    #[must_use]
    pub fn tiny(seed: u64) -> Self {
        Self {
            seed,
            channels: 6,
            users: 40,
            messages: 900,
            reaction_factor: 0.5,
            channel_exponent: 1.1,
            renames: 5,
            archives: 1,
            concurrency: 1,
            segment_bytes: 64 * 1024,
            seal_pack: true,
        }
    }

    /// The store knobs this corpus needs, for
    /// [`create_store`](crate::store_backend::create_store).
    #[must_use]
    pub fn store_config(&self) -> StoreConfig {
        StoreConfig {
            segment_bytes: self.segment_bytes,
            seal_pack:     self.seal_pack,
        }
    }

    /// Reactions this config asks for.
    #[must_use]
    pub fn reactions(&self) -> usize {
        (self.messages as f64 * self.reaction_factor).round().max(0.0) as usize
    }
}

/// What actually got committed — the numbers the CLI prints and a test asserts
/// on.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SeedReport {
    pub users:                usize,
    pub channels:             usize,
    pub messages:             usize,
    pub reactions:            usize,
    pub renames:              usize,
    pub archives:             usize,
    /// Messages in the busiest channel — the depth evidence.
    pub deepest_channel:      usize,
    /// Messages in the quietest channel — the long-tail evidence.
    pub shallowest_channel:   usize,
    /// The busiest channel's id and slug. `chatter bench` scrolls back through
    /// exactly this channel, and `chatter seed` prints it so a human knows
    /// which room to open first.
    pub deepest_channel_id:   Option<Id>,
    pub deepest_channel_slug: Option<String>,
    pub elapsed:              Duration,
}

impl SeedReport {
    /// Total domain events committed (one per action).
    #[must_use]
    pub fn events(&self) -> usize {
        self.users
            + self.channels
            + self.messages
            + self.reactions
            + self.renames
            + self.archives
    }
}

// ===========================================================================
// The fresh-dir guard
// ===========================================================================

/// The fresh-`--dir` guard, mirroring `examples/social`'s for the same reason:
/// the determinism claim — same seed, same corpus — only holds starting from an
/// empty log. Seeding into a directory that already holds a store would append
/// on top of whatever is there, silently breaking reproducibility.
///
/// Returns `Err(message)` naming the leftovers when `dir` exists and is
/// non-empty; `Ok(())` for a missing or empty dir. There is no adopt/merge
/// mode — either point `--dir` at a fresh path or pass `--force`.
pub fn guard_fresh_dir(dir: &Path) -> Result<(), String> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        // Missing dir: the engine creates it. Nothing to refuse.
        Err(_) => return Ok(()),
    };
    let mut segments = 0usize;
    let mut markers: Vec<String> = Vec::new();
    let mut others = 0usize;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("seg-") && name.ends_with(".log") {
            segments += 1;
        } else if name == "LOCK"
            || name == "meta"
            || name == "sealed"
            || name == ".chatter-store"
        {
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
/// `1 / (r + 1) ^ exponent`, so rank 0 is the heaviest. This is what turns a
/// flat list of channels into a few hot ones and a long quiet tail.
struct ZipfWeights {
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
    fn sample(&self, rng: &mut Rng) -> usize {
        let total = *self.cumulative.last().expect("ZipfWeights::new(0, _)");
        let x = rng.f64() * total;
        self.cumulative
            .partition_point(|&c| c < x)
            .min(self.cumulative.len() - 1)
    }
}

// ===========================================================================
// Text banks
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

/// Channel slug stems — the room names a real workspace grows.
const ROOM: &[&str] = &[
    "general",
    "random",
    "deploys",
    "incidents",
    "design",
    "backend",
    "frontend",
    "platform",
    "oncall",
    "release",
    "support",
    "hiring",
    "standup",
    "watercooler",
    "docs",
    "infra",
    "security",
    "data",
    "mobile",
    "growth",
    "billing",
    "api",
    "storage",
    "observability",
];

const TOPIC: &[&str] = &[
    "everything that does not fit elsewhere",
    "shipping the next release",
    "post-incident notes and follow-ups",
    "design reviews and mocks",
    "the storage engine rewrite",
    "cold brew and other essentials",
    "paging, alerts, and who is awake",
    "customer questions worth answering twice",
    "benchmarks, flamegraphs, and regrets",
    "week planning and blockers",
];

/// Sentence fragments the body generator assembles until it hits its target
/// length. Deliberately chatty and uneven so bodies read like messages rather
/// than lorem ipsum.
const FRAGMENT: &[&str] = &[
    "pushed the branch, tests are green locally but CI is still chewing on it",
    "I think the retry budget is the thing biting us here, not the timeout",
    "does anyone have context on why this segment size was picked",
    "reproduced it on my machine with a smaller corpus, notes in the thread",
    "the flamegraph says we spend most of the time in the decode path",
    "we can probably drop that allocation if we borrow instead of cloning",
    "reverted for now, will pick it back up tomorrow morning",
    "the cold open went from four seconds to a hundred milliseconds",
    "left a couple of comments, nothing blocking",
    "heads up: the staging store was wiped, reseed before you test",
    "that number looks suspiciously round, want to double check the units",
    "agreed, let us not ship a knob we cannot explain",
    "the checkpoint is discardable, so deleting it only costs a rebuild",
    "I would rather measure it than argue about it",
    "adding a floor so this cannot silently regress",
    "the long tail is what makes this interesting, the hot path is easy",
    "paging myself, this is going to take the rest of the afternoon",
    "reading the spec again, section six is clearer than I remembered",
    "we have two writers and one of them is not supposed to exist",
    "small patch, big diff, mostly comments",
    "the sealed sidecar is doing exactly what it says on the tin",
    "can we get a graph of that over the last week",
    "I will write it up properly once the numbers settle",
    "this is the third time this month, time for a real fix",
];

/// Reaction shortcodes and emoji — a mix so the corpus carries multi-byte
/// UTF-8 payloads too.
const EMOJI: &[&str] =
    &["+1", "eyes", "tada", "rocket", "heart", "🎉", "👀", "🚀", "✅", "🔥"];

/// The lower bound of the realistic payload band, in bytes.
pub const MIN_BODY_BYTES: usize = 100;

/// The upper bound of the realistic payload band, in bytes.
pub const MAX_BODY_BYTES: usize = 800;

/// Median body length, in bytes, before the lognormal spread.
const BODY_MEDIAN_BYTES: f64 = 240.0;

/// Lognormal sigma. 0.6 puts roughly the top decile above 500 B while keeping
/// the bulk in the 150–400 B range — the shape a chat corpus actually has.
const BODY_SIGMA: f64 = 0.6;

/// Draw a body length in bytes: lognormal about [`BODY_MEDIAN_BYTES`], clamped
/// to the [`MIN_BODY_BYTES`]..=[`MAX_BODY_BYTES`] band. Box–Muller off two
/// uniforms from the seeded PRNG, so it is a pure function of the seed and the
/// draw order.
fn draw_body_len(rng: &mut Rng) -> usize {
    // `f64()` is [0, 1); nudge away from 0 so `ln` stays finite.
    let u1 = rng.f64().max(f64::MIN_POSITIVE);
    let u2 = rng.f64();
    let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
    let len = BODY_MEDIAN_BYTES * (BODY_SIGMA * z).exp();
    (len.round() as i64).clamp(MIN_BODY_BYTES as i64, MAX_BODY_BYTES as i64)
        as usize
}

/// Draw one syntactically-valid, unique handle: `{adj}_{noun}` with a numeric
/// suffix when needed for uniqueness.
fn gen_handle(rng: &mut Rng, used: &mut HashSet<String>) -> String {
    let a = ADJ[rng.usize(0..ADJ.len())];
    let n = NOUN[rng.usize(0..NOUN.len())];
    let base = format!("{a}_{n}");
    if used.insert(base.clone()) {
        debug_assert!(crate::handle_is_valid(&base));
        return base;
    }
    // Deterministic disambiguation: walk suffixes until one is free. Never an
    // unbounded RNG retry loop, so the draw order stays fixed no matter how
    // many collisions happen.
    for suffix in 2..u32::MAX {
        let candidate = format!("{base}{suffix}");
        if candidate.chars().count() <= crate::HANDLE_MAX_LEN
            && used.insert(candidate.clone())
        {
            debug_assert!(crate::handle_is_valid(&candidate));
            return candidate;
        }
    }
    unreachable!("handle space exhausted")
}

/// Draw a human-plausible display name.
fn gen_display_name(rng: &mut Rng) -> String {
    let f = FIRST[rng.usize(0..FIRST.len())];
    let s = SURNAME[rng.usize(0..SURNAME.len())];
    if rng.f64() < 0.25 {
        format!("{f} {}.", &s[0..1])
    } else {
        format!("{f} {s}")
    }
}

/// Draw a unique channel slug.
fn gen_slug(rng: &mut Rng, used: &mut HashSet<String>) -> String {
    let room = ROOM[rng.usize(0..ROOM.len())];
    if used.insert(room.to_string()) {
        debug_assert!(crate::slug_is_valid(room));
        return room.to_string();
    }
    for suffix in 2..u32::MAX {
        let candidate = format!("{room}-{suffix}");
        if candidate.chars().count() <= crate::SLUG_MAX_LEN
            && used.insert(candidate.clone())
        {
            debug_assert!(crate::slug_is_valid(&candidate));
            return candidate;
        }
    }
    unreachable!("slug space exhausted")
}

/// Assemble a message body of roughly `target` bytes from [`FRAGMENT`],
/// occasionally decorated. Always at least one character, never longer than
/// [`crate::BODY_MAX_LEN`] characters.
fn gen_body(rng: &mut Rng, target: usize) -> String {
    let mut body = String::with_capacity(target + 64);
    while body.len() < target {
        if !body.is_empty() {
            body.push(' ');
        }
        body.push_str(FRAGMENT[rng.usize(0..FRAGMENT.len())]);
    }
    // A minority of messages carry an emoji, so the corpus exercises
    // multi-byte payloads rather than being uniformly ASCII.
    if rng.f64() < 0.12 {
        body.push(' ');
        body.push_str(EMOJI[rng.usize(0..EMOJI.len())]);
    }
    debug_assert!(body.chars().count() <= crate::BODY_MAX_LEN);
    body
}

// ===========================================================================
// Planning: the deterministic, store-free corpus draw
// ===========================================================================

/// A fixed, arbitrary Unix-millisecond timestamp (2024-01-01T00:00:00Z) every
/// seeded id's embedded UUIDv7 timestamp is drawn from — fixed rather than
/// wall-clock so the corpus stays a pure function of the seed.
const SEED_BASE_MS: u64 = 1_704_067_200_000;

/// Draw one deterministic [`Id`]: [`SEED_BASE_MS`] plus 10 bytes off `rng`.
fn draw_id(rng: &mut Rng) -> Id {
    let mut bytes = [0u8; 10];
    rng.fill(&mut bytes);
    Id::from_parts(SEED_BASE_MS, bytes)
}

/// One planned write onto a channel stream.
///
/// Messages and reactions are planned as **one interleaved sequence**, not as
/// two phases. That is what makes a channel stream read like a conversation:
/// a message, a couple of reactions to what was just said, another message. A
/// two-phase plan (all messages, then all reactions) would push every reaction
/// past every message, so the newest pages of a scroll-back would be nothing
/// but reactions — realistic of no chat system that has ever existed, and it
/// would hide the sealed-history read path the example exists to show.
#[derive(Debug, Clone)]
enum PlannedAction {
    /// The *ordinal* is deliberately absent — the aggregate stamps it (see
    /// [`crate::domain::channel`]).
    Message { channel: usize, author: Id, body: String },
    /// `target` is a message ordinal the channel has already reached at this
    /// point in the plan, so the aggregate's bounded existence check passes.
    Reaction { channel: usize, target: u64, by: Id, emoji: String },
}

impl PlannedAction {
    fn channel(&self) -> usize {
        match self {
            PlannedAction::Message { channel, .. }
            | PlannedAction::Reaction { channel, .. } => *channel,
        }
    }

    /// Only the assertions need to distinguish the two arms; execution matches
    /// on them directly.
    #[cfg(test)]
    fn is_message(&self) -> bool {
        matches!(self, PlannedAction::Message { .. })
    }
}

/// The identities drawn before any conversation: every user and every channel.
///
/// These are planned up front and in full — they are small (a handful of short
/// strings each) and every later draw refers to them.
struct Identities {
    /// `(user_id, handle, display_name)`.
    users:    Vec<(Id, String, String)>,
    /// `(channel_id, slug, topic)`.
    channels: Vec<(Id, String, String)>,
}

/// The trailing phases, drawn once the conversation is complete.
struct Epilogue {
    /// `(user_id, new_display_name)`.
    renames:  Vec<(Id, String)>,
    /// Channel ids to archive, last.
    archives: Vec<Id>,
}

/// How many messages one planning chunk draws before it is handed to the
/// executor.
///
/// **This is a memory bound, not a tuning knob.** A message body averages a few
/// hundred bytes, so planning `--scale huge`'s twelve million messages in one
/// `Vec` would hold several gigabytes of bodies in RAM before the first write.
/// Chunking caps the planner's live footprint at roughly
/// `PLAN_CHUNK_MESSAGES * (body + overhead)` — tens of megabytes — at every
/// scale.
///
/// It does **not** affect the corpus: the RNG is threaded straight through the
/// chunk boundary, so the draw sequence is exactly what a single-pass planner
/// would produce. It does not affect per-stream order either — chunks execute
/// strictly in order, so a channel's events are still committed in plan order
/// whatever the concurrency. (Under `concurrency > 1` it does bound how far
/// apart two channels' writes can drift in the global interleaving, which was
/// already unspecified.)
const PLAN_CHUNK_MESSAGES: usize = 20_000;

/// The corpus draw: one seeded [`Rng`] and a fixed draw order, yielding
/// identities, then the conversation in chunks, then the epilogue. Pure — no
/// store, no clock, no `await`.
struct Planner {
    rng:               Rng,
    user_ids:          Vec<Id>,
    channel_ids:       Vec<Id>,
    channel_weights:   ZipfWeights,
    total_messages:    usize,
    total_reactions:   usize,
    /// Messages drawn so far — the absolute index the reaction quota uses.
    emitted_messages:  usize,
    /// Reactions drawn so far.
    emitted_reactions: usize,
    /// Messages per channel index, for the depth report.
    depth:             Vec<usize>,
}

impl Planner {
    /// Draw the identities and return the planner positioned at the start of
    /// the conversation.
    fn start(cfg: &SeedConfig) -> (Self, Identities) {
        let mut rng = Rng::with_seed(cfg.seed);

        // --- users (the shallow streams: registry pressure) ----------------
        let mut used_handles = HashSet::new();
        let mut user_ids: Vec<Id> = Vec::with_capacity(cfg.users);
        let mut users: Vec<(Id, String, String)> =
            Vec::with_capacity(cfg.users);
        for _ in 0..cfg.users {
            let handle = gen_handle(&mut rng, &mut used_handles);
            let display = gen_display_name(&mut rng);
            let id = draw_id(&mut rng);
            user_ids.push(id);
            users.push((id, handle, display));
        }

        // --- channels (the deep streams) -----------------------------------
        let mut used_slugs = HashSet::new();
        let mut channel_ids: Vec<Id> = Vec::with_capacity(cfg.channels);
        let mut channels: Vec<(Id, String, String)> =
            Vec::with_capacity(cfg.channels);
        for _ in 0..cfg.channels {
            let slug = gen_slug(&mut rng, &mut used_slugs);
            let topic = TOPIC[rng.usize(0..TOPIC.len())].to_string();
            let id = draw_id(&mut rng);
            channel_ids.push(id);
            channels.push((id, slug, topic));
        }

        let channel_weights =
            ZipfWeights::new(channel_ids.len().max(1), cfg.channel_exponent);
        let depth = vec![0usize; channel_ids.len()];
        // A corpus with no channels or no users has no conversation to draw.
        let plannable = !channel_ids.is_empty() && !user_ids.is_empty();
        let planner = Planner {
            rng,
            user_ids,
            channel_ids,
            channel_weights,
            total_messages: if plannable { cfg.messages } else { 0 },
            total_reactions: if plannable { cfg.reactions() } else { 0 },
            emitted_messages: 0,
            emitted_reactions: 0,
            depth,
        };
        (planner, Identities { users, channels })
    }

    /// Whether any of the conversation remains to be drawn.
    fn has_more(&self) -> bool { self.emitted_messages < self.total_messages }

    /// Draw the next chunk of interleaved messages and reactions.
    ///
    /// Channel choice is Zipf-weighted, so a few rooms carry most of the
    /// traffic. Reactions are emitted right after the message that provoked
    /// them (targeting a recent ordinal in the *same* channel), which is both
    /// realistic and what keeps a scroll-back page full of actual messages.
    fn next_chunk(&mut self) -> Vec<PlannedAction> {
        let take = PLAN_CHUNK_MESSAGES
            .min(self.total_messages - self.emitted_messages);
        let mut actions: Vec<PlannedAction> = Vec::with_capacity(take * 2);
        for _ in 0..take {
            let i = self.emitted_messages;
            let ci = self.channel_weights.sample(&mut self.rng);
            let author = self.user_ids[self.rng.usize(0..self.user_ids.len())];
            let target_len = draw_body_len(&mut self.rng);
            let body = gen_body(&mut self.rng, target_len);
            self.depth[ci] += 1;
            self.emitted_messages += 1;
            actions.push(PlannedAction::Message { channel: ci, author, body });

            // Exactly `total_reactions` reactions overall, spread evenly by
            // integer arithmetic (never a floating-point accumulator, which
            // would make the total depend on rounding). After message `i`,
            // emit the difference between the running quota at `i + 1` and at
            // `i`.
            let denominator = self.total_messages.max(1) as u128;
            let quota_before =
                (i as u128 * self.total_reactions as u128) / denominator;
            let quota_after =
                ((i + 1) as u128 * self.total_reactions as u128) / denominator;
            for _ in quota_before..quota_after {
                // Bias toward recent messages: draw a uniform, square it, and
                // measure back from the newest ordinal. Reactions cluster on
                // what was just said, like they do in a real room.
                let u = self.rng.f64();
                let back = ((u * u) * self.depth[ci] as f64) as u64;
                let target = (self.depth[ci] as u64 - 1).saturating_sub(back);
                let by = self.user_ids[self.rng.usize(0..self.user_ids.len())];
                let emoji = EMOJI[self.rng.usize(0..EMOJI.len())].to_string();
                self.emitted_reactions += 1;
                actions.push(PlannedAction::Reaction {
                    channel: ci,
                    target,
                    by,
                    emoji,
                });
            }
        }
        actions
    }

    /// Draw the trailing phases. Call once, after the conversation is complete.
    fn finish(&mut self, cfg: &SeedConfig) -> Epilogue {
        // --- renames (a second event on some shallow streams) --------------
        let mut rename_order: Vec<usize> = (0..self.user_ids.len()).collect();
        self.rng.shuffle(&mut rename_order);
        let renames: Vec<(Id, String)> = rename_order
            .iter()
            .take(cfg.renames.min(self.user_ids.len()))
            .map(|&ui| (self.user_ids[ui], gen_display_name(&mut self.rng)))
            .collect();

        // --- archives (terminal writes on a few channels) ------------------
        let mut archive_order: Vec<usize> =
            (0..self.channel_ids.len()).collect();
        self.rng.shuffle(&mut archive_order);
        let archives: Vec<Id> = archive_order
            .iter()
            .take(cfg.archives.min(self.channel_ids.len()))
            .map(|&ci| self.channel_ids[ci])
            .collect();

        Epilogue { renames, archives }
    }

    /// The busiest channel's index, if any channel got a message.
    fn deepest(&self) -> Option<usize> {
        self.depth
            .iter()
            .enumerate()
            .max_by_key(|&(_, &d)| d)
            .filter(|&(_, &d)| d > 0)
            .map(|(ci, _)| ci)
    }
}

/// The whole corpus, materialised in one `Vec` — the test-only view of exactly
/// the chunk sequence [`generate`] executes. Only call this at scales that fit
/// in memory; the real generator never does (see [`PLAN_CHUNK_MESSAGES`]).
#[cfg(test)]
struct Corpus {
    users:    Vec<(Id, String, String)>,
    channels: Vec<(Id, String, String)>,
    actions:  Vec<PlannedAction>,
    renames:  Vec<(Id, String)>,
    archives: Vec<Id>,
    depth:    Vec<usize>,
}

#[cfg(test)]
impl Corpus {
    fn messages(&self) -> usize {
        self.actions.iter().filter(|a| a.is_message()).count()
    }

    fn reactions(&self) -> usize { self.actions.len() - self.messages() }
}

/// Concatenate every chunk [`generate`] would execute, in order. Test-only.
#[cfg(test)]
fn plan_corpus(cfg: &SeedConfig) -> Corpus {
    let (mut planner, ids) = Planner::start(cfg);
    let mut actions = Vec::new();
    while planner.has_more() {
        actions.extend(planner.next_chunk());
    }
    let epilogue = planner.finish(cfg);
    Corpus {
        users: ids.users,
        channels: ids.channels,
        actions,
        renames: epilogue.renames,
        archives: epilogue.archives,
        depth: planner.depth,
    }
}

// ===========================================================================
// Execution
// ===========================================================================

/// Drive an iterator of single-command futures with bounded concurrency `k`.
/// `k <= 1` awaits them **in order** (the byte-identical path); `k > 1` keeps a
/// bounded [`JoinSet`] window of `k` in flight, refilled one-for-one.
///
/// Every future is one seeded `WriteOps` command. A seeded corpus never
/// produces a rejection (the plan mirrors every invariant), so a `WriteError`
/// here is a real bug and panics loudly rather than being swallowed.
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

/// Apply one planned action through the real write seam.
async fn apply_action<B>(
    store: &EventStore<B>,
    channel: Id,
    action: PlannedAction,
) where
    B: SnapshotStore + Clone,
    B::Error: std::fmt::Display,
{
    match action {
        PlannedAction::Message { author, body, .. } => {
            store
                .post_message(channel, author, body)
                .await
                .expect("seed: post_message unexpectedly rejected");
        }
        PlannedAction::Reaction { target, by, emoji, .. } => {
            store
                .add_reaction(channel, target, by, emoji)
                .await
                .expect("seed: add_reaction unexpectedly rejected");
        }
    }
}

/// Execute the interleaved channel actions.
///
/// `k <= 1` runs them in plan order, so the global log is byte-identical run to
/// run. `k > 1` groups them **by channel** and runs at most `k` channels
/// concurrently: a channel is ONE stream, so its actions must commit in plan
/// order or the aggregate's ordinals — and a reaction's target check — would
/// depend on scheduling. Grouping is what makes the pipelined path safe.
async fn run_actions<B>(
    store: &EventStore<B>,
    channel_ids: &[Id],
    actions: Vec<PlannedAction>,
    k: usize,
) where
    B: SnapshotStore + Clone,
    B::Error: std::fmt::Display,
{
    if k <= 1 {
        for action in actions {
            let channel = channel_ids[action.channel()];
            apply_action(store, channel, action).await;
        }
        return;
    }
    let mut groups: Vec<Vec<PlannedAction>> =
        (0..channel_ids.len()).map(|_| Vec::new()).collect();
    for action in actions {
        groups[action.channel()].push(action);
    }
    let mut tasks: Vec<(Id, Vec<PlannedAction>)> = groups
        .into_iter()
        .enumerate()
        .filter(|(_, g)| !g.is_empty())
        .map(|(ci, g)| (channel_ids[ci], g))
        .collect();
    // Biggest channels first: the pipeline drains evenly instead of ending on
    // one enormous straggler.
    tasks.sort_by_key(|(_, g)| std::cmp::Reverse(g.len()));
    let mut it = tasks.into_iter();
    let mut set: JoinSet<()> = JoinSet::new();
    let spawn = |set: &mut JoinSet<()>,
                 (channel, group): (Id, Vec<PlannedAction>)| {
        let s = store.clone();
        set.spawn(async move {
            for action in group {
                apply_action(&s, channel, action).await;
            }
        });
    };
    for task in it.by_ref().take(k) {
        spawn(&mut set, task);
    }
    while let Some(joined) = set.join_next().await {
        joined.expect("seed: channel task panicked");
        if let Some(task) = it.next() {
            spawn(&mut set, task);
        }
    }
}

/// Generate a corpus into `store`, entirely through
/// [`WriteOps`](crate::ops::WriteOps).
///
/// Planning and execution are **interleaved in chunks** (see
/// [`PLAN_CHUNK_MESSAGES`]), so the generator's peak memory is bounded at every
/// scale — `--scale huge`'s twelve million bodies are never all resident. The
/// draw sequence is identical to a single-pass plan, so the corpus is
/// unchanged.
///
/// Generic over `B: SnapshotStore + Clone` so the same generator seeds a real
/// on-disk store *and* an in-memory test one.
pub async fn generate<B>(store: &EventStore<B>, cfg: &SeedConfig) -> SeedReport
where
    B: SnapshotStore + Clone,
    B::Error: std::fmt::Display,
{
    let (mut planner, ids) = Planner::start(cfg);
    let channel_ids: Vec<Id> = ids.channels.iter().map(|c| c.0).collect();
    let slugs: Vec<String> = ids.channels.iter().map(|c| c.1.clone()).collect();
    let users = ids.users.len();
    let channels = ids.channels.len();
    let k = cfg.concurrency.max(1);
    let start = Instant::now();

    // Phase order is the global-log order at `concurrency == 1`: users,
    // channels, the conversation, renames, archives. Phases are sequential, so
    // a channel is never written concurrently with itself across phases
    // (create-before-post, post-before-react, everything-before-archive).
    drive(
        ids.users.into_iter().map(|(id, handle, display)| {
            let s = store.clone();
            async move { s.register(id, handle, display).await }
        }),
        k,
    )
    .await;
    drive(
        ids.channels.into_iter().map(|(id, slug, topic)| {
            let s = store.clone();
            async move { s.create_channel(id, slug, topic).await }
        }),
        k,
    )
    .await;
    while planner.has_more() {
        let chunk = planner.next_chunk();
        run_actions(store, &channel_ids, chunk, k).await;
    }
    let epilogue = planner.finish(cfg);
    drive(
        epilogue.renames.into_iter().map(|(id, name)| {
            let s = store.clone();
            async move { s.set_display_name(id, name).await }
        }),
        k,
    )
    .await;
    let archives = epilogue.archives.len();
    drive(
        epilogue.archives.into_iter().map(|id| {
            let s = store.clone();
            async move { s.archive_channel(id).await }
        }),
        k,
    )
    .await;
    let elapsed = start.elapsed();

    let deepest = planner.deepest();
    SeedReport {
        users,
        channels,
        messages: planner.emitted_messages,
        reactions: planner.emitted_reactions,
        renames: cfg.renames.min(users),
        archives,
        deepest_channel: planner.depth.iter().copied().max().unwrap_or(0),
        shallowest_channel: planner.depth.iter().copied().min().unwrap_or(0),
        deepest_channel_id: deepest.map(|ci| channel_ids[ci]),
        deepest_channel_slug: deepest.map(|ci| slugs[ci].clone()),
        elapsed,
    }
}

#[cfg(test)]
mod tests {
    use mess_store::{LogEngine, PackSnapshotBackend};
    use mess_testkit::{SweepingTempDir, sweeping_temp_dir};

    use super::*;

    fn snap_store(
        tag: &str,
    ) -> (EventStore<PackSnapshotBackend<LogEngine>>, SweepingTempDir) {
        let dir = sweeping_temp_dir(tag);
        let engine =
            LogEngine::open(dir.path().join("log")).expect("open engine");
        let backend =
            PackSnapshotBackend::open(engine, dir.path().join("snap"))
                .expect("open snapshot backend");
        (EventStore::new(backend), dir)
    }

    #[test]
    fn body_lengths_land_in_the_realistic_band() {
        let mut rng = Rng::with_seed(9);
        let mut lens: Vec<usize> = (0..20_000)
            .map(|_| {
                let target = draw_body_len(&mut rng);
                gen_body(&mut rng, target).len()
            })
            .collect();
        lens.sort_unstable();
        let p50 = lens[lens.len() / 2];
        let p95 = lens[lens.len() * 95 / 100];
        // The band the bone asks for. The upper clamp plus the
        // fragment-assembly overshoot means a body can exceed
        // `MAX_BODY_BYTES` by at most one fragment.
        assert!(lens[0] >= MIN_BODY_BYTES, "shortest body was {} B", lens[0]);
        assert!(
            *lens.last().unwrap() < MAX_BODY_BYTES + 128,
            "longest body was {} B",
            lens.last().unwrap()
        );
        // Lognormal, not uniform: the median sits well below the midpoint of
        // the band and the tail reaches up toward it.
        assert!((150..400).contains(&p50), "p50 was {p50} B");
        assert!(p95 > 400, "p95 was {p95} B");
    }

    #[test]
    fn plan_is_a_pure_function_of_the_seed() {
        let cfg = SeedConfig::tiny(4242);
        let a = plan_corpus(&cfg);
        let b = plan_corpus(&cfg);
        assert_eq!(a.users, b.users);
        assert_eq!(a.channels, b.channels);
        assert_eq!(a.depth, b.depth);
        assert_eq!(a.renames, b.renames);
        assert_eq!(a.archives, b.archives);
        assert_eq!(a.messages(), cfg.messages);
        assert_eq!(a.reactions(), cfg.reactions());
        assert_eq!(a.actions.len(), b.actions.len());
        for (x, y) in a.actions.iter().zip(b.actions.iter()) {
            match (x, y) {
                (
                    PlannedAction::Message { channel, author, body },
                    PlannedAction::Message {
                        channel: c2,
                        author: a2,
                        body: b2,
                    },
                ) => {
                    assert_eq!((channel, author, body), (c2, a2, b2));
                }
                (
                    PlannedAction::Reaction { channel, target, by, emoji },
                    PlannedAction::Reaction {
                        channel: c2,
                        target: t2,
                        by: y2,
                        emoji: e2,
                    },
                ) => {
                    assert_eq!((channel, target, by, emoji), (c2, t2, y2, e2));
                }
                _ => panic!("action kinds diverged between two plans"),
            }
        }
    }

    /// Messages and reactions must be *interleaved*, not phased: a channel
    /// stream has to read like a conversation, and the newest scroll-back page
    /// has to contain messages rather than a wall of trailing reactions.
    #[test]
    fn reactions_are_interleaved_with_messages() {
        let cfg = SeedConfig::tiny(11);
        let corpus = plan_corpus(&cfg);
        let last_message = corpus
            .actions
            .iter()
            .rposition(PlannedAction::is_message)
            .expect("some messages");
        let first_reaction = corpus
            .actions
            .iter()
            .position(|a| !a.is_message())
            .expect("some reactions");
        assert!(
            first_reaction < last_message,
            "reactions must start before the last message (phased plan \
             detected)"
        );
        // The tail of the plan is not one long run of reactions.
        let tail_messages = corpus.actions[corpus.actions.len() * 9 / 10..]
            .iter()
            .filter(|a| a.is_message())
            .count();
        assert!(
            tail_messages > 0,
            "the last tenth of the plan must still contain messages"
        );
    }

    #[test]
    fn channel_traffic_is_zipf_skewed() {
        // The property the whole example rests on: a few channels get deep,
        // the tail stays shallow. Checked at planning time (no store), so it
        // runs in the ordinary suite.
        let cfg = SeedConfig::demo();
        let corpus = plan_corpus(&cfg);
        let mut depth = corpus.depth.clone();
        depth.sort_unstable_by(|a, b| b.cmp(a));
        let deepest = depth[0];
        let shallowest = *depth.last().unwrap();
        println!(
            "demo channel depth: deepest {deepest}, median {}, shallowest \
             {shallowest}",
            depth[depth.len() / 2]
        );
        assert!(
            deepest > 2_000,
            "the hottest channel should be thousands of messages deep, got \
             {deepest}"
        );
        assert!(
            deepest > shallowest * 8,
            "expected a long tail: deepest {deepest} vs shallowest \
             {shallowest}"
        );
    }

    /// Every reaction must name an ordinal the channel has ALREADY reached at
    /// that point in the plan — the aggregate refuses anything else, so this is
    /// what keeps `generate`'s "a rejection is a bug" panic honest.
    #[test]
    fn reaction_targets_are_always_in_range_at_plan_time() {
        for seed in [1u64, 7, 99, 4242] {
            let cfg = SeedConfig::tiny(seed);
            let corpus = plan_corpus(&cfg);
            let mut so_far = vec![0u64; corpus.channels.len()];
            for action in &corpus.actions {
                match action {
                    PlannedAction::Message { channel, .. } => {
                        so_far[*channel] += 1
                    }
                    PlannedAction::Reaction { channel, target, .. } => {
                        assert!(
                            *target < so_far[*channel],
                            "reaction targets ordinal {target} but channel \
                             {channel} has only reached {} messages",
                            so_far[*channel]
                        );
                    }
                }
            }
            assert_eq!(so_far.iter().sum::<u64>(), cfg.messages as u64);
            assert_eq!(corpus.reactions(), cfg.reactions());
        }
    }

    /// The chunk boundary is a memory bound, not a semantic one: a corpus that
    /// spans several chunks still commits exactly the requested number of
    /// messages and reactions, and every reaction still targets an ordinal its
    /// channel had already reached — including across a boundary, where the
    /// quota arithmetic could most easily go wrong.
    #[test]
    fn planning_across_chunk_boundaries_preserves_the_corpus() {
        let messages = PLAN_CHUNK_MESSAGES * 2 + 137;
        let cfg = SeedConfig {
            messages,
            reaction_factor: 0.37,
            ..SeedConfig::tiny(2024)
        };
        let (mut planner, ids) = Planner::start(&cfg);
        let mut chunks = 0usize;
        let mut so_far = vec![0u64; ids.channels.len()];
        let mut seen_messages = 0usize;
        let mut seen_reactions = 0usize;
        while planner.has_more() {
            let chunk = planner.next_chunk();
            chunks += 1;
            assert!(!chunk.is_empty(), "a chunk must make progress");
            for action in &chunk {
                match action {
                    PlannedAction::Message { channel, .. } => {
                        so_far[*channel] += 1;
                        seen_messages += 1;
                    }
                    PlannedAction::Reaction { channel, target, .. } => {
                        assert!(
                            *target < so_far[*channel],
                            "reaction across a chunk boundary targets ordinal \
                             {target} but channel {channel} has only reached \
                             {}",
                            so_far[*channel]
                        );
                        seen_reactions += 1;
                    }
                }
            }
        }
        assert!(chunks >= 3, "expected several chunks, got {chunks}");
        assert_eq!(seen_messages, messages);
        assert_eq!(seen_reactions, cfg.reactions());
        assert_eq!(planner.emitted_messages, seen_messages);
        assert_eq!(planner.emitted_reactions, seen_reactions);
    }

    #[test]
    fn generated_handles_and_slugs_are_valid_and_unique() {
        let cfg =
            SeedConfig { users: 5_000, channels: 200, ..SeedConfig::tiny(3) };
        let corpus = plan_corpus(&cfg);
        let mut handles = HashSet::new();
        for (_, h, _) in &corpus.users {
            assert!(crate::handle_is_valid(h), "invalid handle {h:?}");
            assert!(handles.insert(h.clone()), "duplicate handle {h:?}");
        }
        let mut slugs = HashSet::new();
        for (_, s, _) in &corpus.channels {
            assert!(crate::slug_is_valid(s), "invalid slug {s:?}");
            assert!(slugs.insert(s.clone()), "duplicate slug {s:?}");
        }
    }

    #[tokio::test]
    async fn generate_produces_the_requested_shape() {
        let (store, _dir) = snap_store("chatter-shape");
        let cfg = SeedConfig::tiny(42);
        let report = generate(&store, &cfg).await;
        assert_eq!(report.users, cfg.users);
        assert_eq!(report.channels, cfg.channels);
        assert_eq!(report.messages, cfg.messages);
        assert_eq!(report.reactions, cfg.reactions());
        assert_eq!(report.renames, cfg.renames);
        assert_eq!(report.archives, cfg.archives);
        assert_eq!(
            report.events(),
            cfg.users
                + cfg.channels
                + cfg.messages
                + cfg.reactions()
                + cfg.renames
                + cfg.archives
        );
    }

    #[test]
    fn scale_presets_round_trip_their_names() {
        for scale in [Scale::Demo, Scale::Large, Scale::Huge] {
            assert_eq!(Scale::parse(scale.name()).unwrap(), scale);
        }
        assert!(Scale::parse("enormous").is_err());
    }

    #[test]
    fn fresh_dir_guard_accepts_missing_and_empty() {
        let t = sweeping_temp_dir("chatter-guard-empty");
        assert!(guard_fresh_dir(&t.path().join("does-not-exist")).is_ok());
        assert!(guard_fresh_dir(t.path()).is_ok());
    }

    #[test]
    fn fresh_dir_guard_refuses_a_leftover_store() {
        let t = sweeping_temp_dir("chatter-guard-nonempty");
        std::fs::write(t.path().join("seg-00000001.log"), b"x").unwrap();
        std::fs::write(t.path().join("LOCK"), b"x").unwrap();
        let err = guard_fresh_dir(t.path()).unwrap_err();
        assert!(err.contains("refusing to seed"));
        assert!(err.contains("seg-*.log"));
    }
}
