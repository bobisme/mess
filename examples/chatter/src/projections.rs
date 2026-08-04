//! The read model: a **per-channel** projection plus a **global timeline**
//! projection, folded from the event log, with a discardable checkpoint
//! sidecar.
//!
//! Same architecture as `examples/social`'s `projections` — one live pump over
//! an [`EventStore::subscribe`] cursor, an atomic write-rename checkpoint
//! sidecar next to the store dir, a `PROJECTION_VERSION` fold-version gate, and
//! a canonical byte fingerprint for the `chatter rebuild` proof. Read that
//! module for the full rationale; this one documents only what chatter does
//! differently.
//!
//! # Bounded read model over an unbounded log
//!
//! The whole point of chatter is a corpus that reaches gigabytes. A read model
//! that kept every message body would then need gigabytes of RAM *and* would
//! write a gigabyte checkpoint every cadence tick — which would make the
//! checkpoint a liability rather than an acceleration. So the folded [`State`]
//! is bounded by construction:
//!
//! - one row per **user** (registry pressure: many rows, each tiny),
//! - one row per **channel** with counters, not contents,
//! - a fixed-size **recent window** of the last [`TIMELINE_WINDOW`] messages
//!   (the "what's happening now" feed), each carrying a short preview.
//!
//! Message *history* is not the read model's job. It lives in the log, and
//! [`crate::scrollback`] pages backward through it — which is exactly the read
//! path that goes through the sealed tier's payload accelerator.
//!
//! The window is deterministic under both fold paths: a from-0 rebuild and a
//! checkpoint resume both fold records in ascending global order and both keep
//! the last [`TIMELINE_WINDOW`], so they end byte-identical. That is what the
//! `chatter rebuild` byte-compare proves.
//!
//! # The discardable-acceleration law, made observable
//!
//! The log is the sole authority; the checkpoint is acceleration you are
//! allowed to throw away. Concretely, and asserted by `tests/checkpoint.rs`:
//!
//! - **Absent** checkpoint → silent full rebuild from 0. Not a warning, not an
//!   error: a store that has never been read has no checkpoint, and that is
//!   normal.
//! - **Corrupt / truncated / foreign / stale-version** checkpoint →
//!   **reported** (a warning line, and [`Projections::checkpoint_status`]
//!   returns [`CheckpointStatus::Rejected`]) and then a full rebuild from 0.
//!   Reads still work. It is never an error that blocks a read.

use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mess_core::{CodecError, Event};
use mess_store::{
    AnomalyKind, Backend, EventStore, ProjectionAnomalies,
    ProjectionAnomaliesSnapshot, StoredRecord, SubscribeBackend, Subscription,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, RwLock};

use crate::Id;
use crate::domain::channel::ChannelEvent;
use crate::domain::user::UserEvent;

/// The **projection-logic version**, mirroring a `fold_version`. Bump whenever
/// the fold logic or the shape of [`State`] changes in a way that makes an
/// older checkpoint's folded state wrong; a mismatched checkpoint is discarded
/// (and reported), never partially trusted.
pub const PROJECTION_VERSION: u32 = 1;

/// Magic bytes at the head of a checkpoint file ("CHTP").
const CKPT_MAGIC: u32 = 0x4348_5450;

/// On-disk checkpoint **envelope** format version — the framing of the file,
/// orthogonal to [`PROJECTION_VERSION`].
const CKPT_FORMAT_VERSION: u16 = 1;

/// Default live-pump checkpoint cadence: write a checkpoint after this many
/// folded events.
const CKPT_EVERY_N: u64 = 4_096;

/// ...or after this much wall time since the last one, whichever trips first.
const CKPT_EVERY_T: Duration = Duration::from_secs(5);

/// How long the pump backs off after a *transient backend error*. An error
/// backoff, not a steady-state poll: on the happy path the pump is parked on
/// the store watermark.
const PUMP_ERROR_BACKOFF: Duration = Duration::from_millis(50);

/// How many recent messages the global timeline projection keeps. The read
/// model is a "what's happening now" feed, not an archive — see the module
/// docs on why this is bounded.
pub const TIMELINE_WINDOW: usize = 256;

/// How many characters of a message body a timeline row previews.
pub const PREVIEW_CHARS: usize = 72;

// ===========================================================================
// Folded state
// ===========================================================================

/// One user row, folded from that user's `user-<id>` stream.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct UserRow {
    handle:       String,
    display_name: String,
}

/// One channel row, folded from that channel's (deep) `channel-<id>` stream.
/// Counters, never contents — see the module docs.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ChannelRow {
    slug:        String,
    topic:       String,
    messages:    u64,
    reactions:   u64,
    archived:    bool,
    /// Global position of the channel's `Created` event.
    created_seq: u64,
    /// Global position of the most recent event on this channel.
    last_seq:    u64,
}

/// One row of the bounded global timeline: a recent message, previewed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TimelineRowState {
    channel: Id,
    ordinal: u64,
    author:  Id,
    preview: String,
    seq:     u64,
}

/// The whole in-memory read model, bounded by construction. All queries read
/// it behind an [`RwLock`]; the live pump is the only writer.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct State {
    /// user id -> folded user row.
    users:     HashMap<Id, UserRow>,
    /// handle -> user id, so handle-keyed queries are O(1).
    handles:   HashMap<String, Id>,
    /// channel id -> folded channel row.
    channels:  HashMap<Id, ChannelRow>,
    /// slug -> channel id.
    slugs:     HashMap<String, Id>,
    /// The last [`TIMELINE_WINDOW`] messages, oldest first.
    timeline:  VecDeque<TimelineRowState>,
    /// Total messages folded across all channels.
    messages:  u64,
    /// Total reactions folded across all channels.
    reactions: u64,
}

/// Record one anomaly hit at `rec`'s position, and — only on that counter's
/// first-ever occurrence — log a one-time warning. The liveness policy is
/// unchanged (the caller still skips the record); the counter is what keeps a
/// routing bug from vanishing silently.
fn record_anomaly(
    anomalies: &ProjectionAnomalies,
    kind: AnomalyKind,
    rec: &StoredRecord,
) {
    if anomalies.record(kind, rec.global_position) {
        eprintln!(
            "chatter projections: WARNING first {kind}: stream={:?} \
             message_type={:?} global_position={}",
            rec.stream_id, rec.message_type, rec.global_position
        );
    }
}

/// Truncate `body` to [`PREVIEW_CHARS`] characters, appending an ellipsis when
/// it was cut. Character-counted, not byte-sliced, so a multi-byte body can
/// never be split mid-codepoint.
fn preview(body: &str) -> String {
    let mut out: String = body.chars().take(PREVIEW_CHARS).collect();
    if body.chars().nth(PREVIEW_CHARS).is_some() {
        out.push('…');
    }
    out
}

impl State {
    fn apply_user(&mut self, owner: Id, ev: &UserEvent) {
        match ev {
            UserEvent::Registered { handle, display_name } => {
                self.handles.insert(handle.clone(), owner);
                let row = self.users.entry(owner).or_default();
                handle.clone_into(&mut row.handle);
                display_name.clone_into(&mut row.display_name);
            }
            UserEvent::DisplayNameChanged { display_name } => {
                if let Some(row) = self.users.get_mut(&owner) {
                    display_name.clone_into(&mut row.display_name);
                }
            }
        }
    }

    fn apply_channel(&mut self, id: Id, gp: u64, ev: &ChannelEvent) {
        let row = self.channels.entry(id).or_default();
        row.last_seq = gp;
        match ev {
            ChannelEvent::Created { slug, topic } => {
                slug.clone_into(&mut row.slug);
                topic.clone_into(&mut row.topic);
                row.created_seq = gp;
                self.slugs.insert(slug.clone(), id);
            }
            ChannelEvent::MessagePosted { ordinal, author, body } => {
                row.messages += 1;
                self.messages += 1;
                // The bounded window: push newest, evict oldest.
                self.timeline.push_back(TimelineRowState {
                    channel: id,
                    ordinal: *ordinal,
                    author:  *author,
                    preview: preview(body),
                    seq:     gp,
                });
                while self.timeline.len() > TIMELINE_WINDOW {
                    self.timeline.pop_front();
                }
            }
            ChannelEvent::ReactionAdded { .. } => {
                row.reactions += 1;
                self.reactions += 1;
            }
            ChannelEvent::Archived => row.archived = true,
        }
    }

    /// Route one stored record by [`StoredRecord::category_and_suffix`] — the
    /// blessed replacement for hand-rolled stream-prefix string surgery.
    /// Records this projection does not understand are **skipped** (a
    /// projection must tolerate a log wider than the slice it models) and
    /// counted in [`ProjectionAnomalies`].
    fn apply_record(
        &mut self,
        rec: &StoredRecord,
        anomalies: &ProjectionAnomalies,
    ) {
        let (category, suffix) = rec.category_and_suffix();
        match category {
            "user" => match Id::from_str(suffix) {
                Ok(owner) => {
                    match UserEvent::decode(&rec.message_type, &rec.data) {
                        Ok(ev) => self.apply_user(owner, &ev),
                        Err(CodecError::UnknownEventName(_)) => record_anomaly(
                            anomalies,
                            AnomalyKind::UnknownEventKind,
                            rec,
                        ),
                        Err(_) => record_anomaly(
                            anomalies,
                            AnomalyKind::UndecodablePayload,
                            rec,
                        ),
                    }
                }
                Err(_) => record_anomaly(
                    anomalies,
                    AnomalyKind::UnroutableStream,
                    rec,
                ),
            },
            "channel" => match Id::from_str(suffix) {
                Ok(id) => {
                    match ChannelEvent::decode(&rec.message_type, &rec.data) {
                        Ok(ev) => {
                            self.apply_channel(id, rec.global_position, &ev)
                        }
                        Err(CodecError::UnknownEventName(_)) => record_anomaly(
                            anomalies,
                            AnomalyKind::UnknownEventKind,
                            rec,
                        ),
                        Err(_) => record_anomaly(
                            anomalies,
                            AnomalyKind::UndecodablePayload,
                            rec,
                        ),
                    }
                }
                Err(_) => record_anomaly(
                    anomalies,
                    AnomalyKind::UnroutableStream,
                    rec,
                ),
            },
            _ => record_anomaly(anomalies, AnomalyKind::UnroutableStream, rec),
        }
    }

    /// A canonical, iteration-order-independent byte fingerprint of the folded
    /// state: msgpack over a **key-sorted** view of every map, plus the
    /// timeline window in its own (already deterministic) order. Two [`State`]s
    /// with identical folded content serialize to byte-identical fingerprints
    /// regardless of `HashMap` iteration order — the basis of the
    /// `chatter rebuild` byte-compare.
    fn canonical_bytes(&self) -> Vec<u8> {
        fn sorted_map<V>(map: &HashMap<Id, V>) -> Vec<(String, &V)> {
            let mut v: Vec<(Id, &V)> =
                map.iter().map(|(&id, val)| (id, val)).collect();
            v.sort_by_key(|(id, _)| *id);
            v.into_iter().map(|(id, val)| (id.to_string(), val)).collect()
        }
        fn sorted_index(map: &HashMap<String, Id>) -> Vec<(&String, String)> {
            let mut v: Vec<(&String, String)> =
                map.iter().map(|(k, id)| (k, id.to_string())).collect();
            v.sort_by(|a, b| a.0.cmp(b.0));
            v
        }
        let timeline: Vec<&TimelineRowState> = self.timeline.iter().collect();
        let canon = (
            sorted_map(&self.users),
            sorted_index(&self.handles),
            sorted_map(&self.channels),
            sorted_index(&self.slugs),
            timeline,
            self.messages,
            self.reactions,
        );
        rmp_serde::to_vec(&canon).expect("canonical state serializes")
    }
}

// ===========================================================================
// Public query DTOs
// ===========================================================================

/// The per-channel projection's answer: counters and identity, never contents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChannelSummary {
    pub id:          Id,
    pub slug:        String,
    pub topic:       String,
    pub messages:    u64,
    pub reactions:   u64,
    pub archived:    bool,
    /// Global position of the channel's `Created` event.
    pub created_seq: u64,
    /// Global position of the most recent event on this channel.
    pub last_seq:    u64,
}

/// One row of the global timeline projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TimelineRow {
    pub channel:       Id,
    pub channel_slug:  String,
    pub ordinal:       u64,
    pub author:        Id,
    pub author_handle: String,
    pub preview:       String,
    /// Global position of the message's event — the stable feed sort key.
    pub seq:           u64,
}

/// Coarse cardinalities of a folded read model — the counts the rebuild proof
/// prints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Cardinalities {
    pub users:     usize,
    pub channels:  usize,
    pub messages:  u64,
    pub reactions: u64,
}

/// What happened to the checkpoint at construction time — the observable half
/// of the discardable-acceleration law (see the module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointStatus {
    /// This instance does not checkpoint ([`Projections::new`]).
    Disabled,
    /// No checkpoint file was present. A silent full rebuild; not a problem.
    Absent,
    /// A valid checkpoint was resumed from this global position.
    Loaded { applied: u64 },
    /// A checkpoint file existed but was not usable. **Reported**, then a full
    /// rebuild from 0. Reads still work.
    Rejected { reason: String },
}

// ===========================================================================
// Checkpoint: the on-disk sidecar
// ===========================================================================

/// The on-disk checkpoint envelope. Header fields come first so a truncated
/// file fails to decode (or fails the header check) rather than half-loading a
/// plausible-looking state.
#[derive(Serialize, Deserialize)]
struct Checkpoint {
    magic:              u32,
    format_version:     u16,
    projection_version: u32,
    /// The watermark the state is current to: one past the highest folded
    /// global position, i.e. the `from` a resume subscribes at.
    applied:            u64,
    state:              State,
}

/// Load and **validate** a checkpoint from `path`, distinguishing *absent*
/// (silent) from *present but unusable* (reported) — see [`CheckpointStatus`].
fn load_checkpoint(path: &Path) -> (Option<(u64, State)>, CheckpointStatus) {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return (None, CheckpointStatus::Absent);
        }
        Err(e) => {
            return (
                None,
                CheckpointStatus::Rejected {
                    reason: format!("unreadable: {e}"),
                },
            );
        }
    };
    let ck: Checkpoint = match rmp_serde::from_slice(&bytes) {
        Ok(ck) => ck,
        Err(e) => {
            return (
                None,
                CheckpointStatus::Rejected {
                    reason: format!("undecodable ({} bytes): {e}", bytes.len()),
                },
            );
        }
    };
    if ck.magic != CKPT_MAGIC {
        return (
            None,
            CheckpointStatus::Rejected {
                reason: format!("bad magic {:#010X}", ck.magic),
            },
        );
    }
    if ck.format_version != CKPT_FORMAT_VERSION {
        return (
            None,
            CheckpointStatus::Rejected {
                reason: format!(
                    "envelope format {} (want {CKPT_FORMAT_VERSION})",
                    ck.format_version
                ),
            },
        );
    }
    if ck.projection_version != PROJECTION_VERSION {
        return (
            None,
            CheckpointStatus::Rejected {
                reason: format!(
                    "projection version {} (want {PROJECTION_VERSION})",
                    ck.projection_version
                ),
            },
        );
    }
    let applied = ck.applied;
    (Some((applied, ck.state)), CheckpointStatus::Loaded { applied })
}

/// Serialize `state` and write it to `path` **atomically**: write a sibling
/// temp file, then rename it over `path`. The rename is atomic on every target
/// filesystem, so a reader never observes a half-written checkpoint.
fn write_checkpoint(
    state: State,
    applied: u64,
    version: u32,
    path: &Path,
) -> std::io::Result<()> {
    let ck = Checkpoint {
        magic: CKPT_MAGIC,
        format_version: CKPT_FORMAT_VERSION,
        projection_version: version,
        applied,
        state,
    };
    let bytes = rmp_serde::to_vec(&ck).map_err(std::io::Error::other)?;
    let tmp = tmp_path(path);
    std::fs::write(&tmp, &bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// The sibling temp path for the atomic write: `<path>.tmp`.
fn tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

/// Live-pump checkpoint cadence + destination, shared with the pump task.
#[derive(Debug)]
struct CheckpointCfg {
    path:    PathBuf,
    every_n: u64,
    every_t: Duration,
}

// ===========================================================================
// Projections
// ===========================================================================

/// A rebuildable, live-tailing read model over any [`SubscribeBackend`], with
/// optional checkpointed resume.
///
/// `Clone` is intentionally not derived: the value owns the live pump task and
/// aborts it on drop, so there is exactly one owner of the tail.
#[derive(Debug)]
pub struct Projections<B: Backend> {
    state:        Arc<RwLock<State>>,
    /// The read watermark: one past the highest global position folded.
    applied:      Arc<AtomicU64>,
    /// Pulsed after every batch the pump applies, so `wait_for` waiters wake.
    notify:       Arc<Notify>,
    anomalies:    Arc<ProjectionAnomalies>,
    resumed_from: u64,
    status:       CheckpointStatus,
    checkpoint:   Option<Arc<CheckpointCfg>>,
    /// Set by the pump when a cadence checkpoint write fails. Observability,
    /// not an error: a failed checkpoint costs a colder next start.
    ckpt_errors:  Arc<Mutex<Vec<String>>>,
    /// The live pump; aborted on drop.
    pump:         tokio::task::JoinHandle<()>,
    _backend:     PhantomData<B>,
}

impl<B: Backend> Drop for Projections<B> {
    fn drop(&mut self) { self.pump.abort() }
}

impl<B: SubscribeBackend + Clone> Projections<B> {
    /// Build the read model by **replaying the whole log from position 0**,
    /// then spawn the live pump. This instance does **not** checkpoint — it is
    /// the pure from-scratch rebuild the `chatter rebuild` proof uses as its
    /// baseline.
    pub async fn new(store: &EventStore<B>) -> Self {
        Self::build(
            store,
            State::default(),
            0,
            None,
            CheckpointStatus::Disabled,
        )
        .await
    }

    /// Build the read model, **resuming from a checkpoint** at `path` when one
    /// is present and valid, else falling back to a clean from-0 rebuild.
    pub async fn with_checkpoint(
        store: &EventStore<B>,
        path: impl Into<PathBuf>,
    ) -> Self {
        Self::with_checkpoint_cadence(store, path, CKPT_EVERY_N, CKPT_EVERY_T)
            .await
    }

    /// [`with_checkpoint`](Self::with_checkpoint) with an explicit live-pump
    /// cadence. Tests use a very large `every_n` so checkpointing happens only
    /// on an explicit [`checkpoint_now`](Self::checkpoint_now).
    pub async fn with_checkpoint_cadence(
        store: &EventStore<B>,
        path: impl Into<PathBuf>,
        every_n: u64,
        every_t: Duration,
    ) -> Self {
        let path = path.into();
        let head = store.watermark().await.unwrap_or(0);
        let (loaded, mut status) = load_checkpoint(&path);
        if let CheckpointStatus::Rejected { reason } = &status {
            // REPORT and continue — never an error that blocks a read.
            eprintln!(
                "chatter projections: WARNING discarding checkpoint {} \
                 ({reason}); rebuilding the read model from the log",
                path.display()
            );
        }
        // Resume only from a checkpoint that is valid AND not ahead of the
        // store's watermark — a checkpoint ahead of the log reflects positions
        // the store no longer has, so it cannot be trusted.
        let (state, from) = match loaded {
            Some((applied, state)) if applied <= head => (state, applied),
            Some((applied, _)) => {
                let reason = format!(
                    "checkpoint position {applied} is ahead of the store \
                     watermark {head}"
                );
                eprintln!(
                    "chatter projections: WARNING discarding checkpoint {} \
                     ({reason}); rebuilding the read model from the log",
                    path.display()
                );
                status = CheckpointStatus::Rejected { reason };
                (State::default(), 0)
            }
            None => (State::default(), 0),
        };
        let cfg = Arc::new(CheckpointCfg { path, every_n, every_t });
        Self::build(store, state, from, Some(cfg), status).await
    }

    /// The shared construction core: seed `initial` state as current to global
    /// position `from`, synchronously catch up to the store's current
    /// watermark, then spawn the live pump.
    async fn build(
        store: &EventStore<B>,
        initial: State,
        from: u64,
        checkpoint: Option<Arc<CheckpointCfg>>,
        status: CheckpointStatus,
    ) -> Self {
        let head = store.watermark().await.unwrap_or(0);
        let state = Arc::new(RwLock::new(initial));
        let applied = Arc::new(AtomicU64::new(from));
        let notify = Arc::new(Notify::new());
        let anomalies = Arc::new(ProjectionAnomalies::new());
        let ckpt_errors = Arc::new(Mutex::new(Vec::new()));

        // One subscription serves both phases: catch-up reads committed
        // history page-by-page, then the same cursor is handed to the pump for
        // the event-bounded live tail — no gap between the two.
        let mut sub = store.subscribe(Some(from));

        {
            let mut st = state.write().await;
            while sub.position() < head {
                match sub.next_batch().await {
                    Ok(batch) => {
                        for rec in &batch {
                            st.apply_record(rec, &anomalies);
                        }
                    }
                    // A transient read error ends catch-up early; the live pump
                    // resumes from the same cursor and self-heals.
                    Err(_) => break,
                }
            }
        }
        applied.store(sub.position(), Ordering::Release);

        let pump = tokio::spawn(pump_loop(
            sub,
            state.clone(),
            applied.clone(),
            notify.clone(),
            anomalies.clone(),
            checkpoint.clone(),
            ckpt_errors.clone(),
        ));

        Self {
            state,
            applied,
            notify,
            anomalies,
            resumed_from: from,
            status,
            checkpoint,
            ckpt_errors,
            pump,
            _backend: PhantomData,
        }
    }

    /// Persist a checkpoint **now**, atomically, current to whatever the pump
    /// has folded so far. A no-op for an instance built without a checkpoint
    /// destination.
    pub async fn checkpoint_now(&self) -> std::io::Result<()> {
        self.checkpoint_as(PROJECTION_VERSION).await
    }

    /// Test hook: persist a checkpoint stamped with an arbitrary
    /// `projection_version`, so a test can plant a stale-version checkpoint and
    /// prove the resume path reports and discards it.
    #[doc(hidden)]
    pub async fn checkpoint_now_as_version(
        &self,
        version: u32,
    ) -> std::io::Result<()> {
        self.checkpoint_as(version).await
    }

    async fn checkpoint_as(&self, version: u32) -> std::io::Result<()> {
        let Some(cfg) = &self.checkpoint else { return Ok(()) };
        let applied = self.applied.load(Ordering::Acquire);
        // Snapshot under a brief read lock, serialize + write outside it.
        let snap = self.state.read().await.clone();
        write_checkpoint(snap, applied, version, &cfg.path)
    }
}

impl<B: Backend> Projections<B> {
    /// What happened to the checkpoint at construction: absent (silent),
    /// loaded, or rejected-and-reported. See [`CheckpointStatus`].
    #[must_use]
    pub fn checkpoint_status(&self) -> &CheckpointStatus { &self.status }

    /// Cadence checkpoint writes that failed since construction. Never fatal —
    /// a failed checkpoint costs a colder next start, never a fact.
    #[must_use]
    pub fn checkpoint_errors(&self) -> Vec<String> {
        self.ckpt_errors.lock().expect("checkpoint error lock").clone()
    }

    /// Counters for records this projection could not decode or route.
    #[must_use]
    pub fn anomalies(&self) -> ProjectionAnomaliesSnapshot {
        self.anomalies.snapshot()
    }

    /// The global position this instance resumed folding from: `0` for a full
    /// from-0 rebuild, or the checkpoint's watermark when it resumed.
    #[must_use]
    pub fn resumed_from(&self) -> u64 { self.resumed_from }

    /// The current read watermark: one past the highest global position folded.
    #[must_use]
    pub fn applied_position(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }

    /// A deterministic byte fingerprint of the folded state, for the
    /// `chatter rebuild` byte-compare proof.
    pub async fn state_fingerprint(&self) -> Vec<u8> {
        self.state.read().await.canonical_bytes()
    }

    /// Coarse cardinalities of the folded read model.
    pub async fn cardinalities(&self) -> Cardinalities {
        let st = self.state.read().await;
        Cardinalities {
            users:     st.users.len(),
            channels:  st.channels.len(),
            messages:  st.messages,
            reactions: st.reactions,
        }
    }

    // --- the per-channel projection ---------------------------------------

    /// Resolve a channel slug to its id.
    pub async fn resolve_channel(&self, slug: &str) -> Option<Id> {
        self.state.read().await.slugs.get(slug).copied()
    }

    /// Resolve a user handle to their id.
    pub async fn resolve_user(&self, handle: &str) -> Option<Id> {
        self.state.read().await.handles.get(handle).copied()
    }

    /// The per-channel projection for one channel, by slug.
    pub async fn channel(&self, slug: &str) -> Option<ChannelSummary> {
        let st = self.state.read().await;
        let id = *st.slugs.get(slug)?;
        st.channels.get(&id).map(|row| summary_of(id, row))
    }

    /// The per-channel projection for one channel, by id.
    pub async fn channel_by_id(&self, id: Id) -> Option<ChannelSummary> {
        let st = self.state.read().await;
        st.channels.get(&id).map(|row| summary_of(id, row))
    }

    /// Every channel, deepest first — the "which streams actually got deep?"
    /// view the seed report and `chatter stats` print.
    pub async fn channels(&self) -> Vec<ChannelSummary> {
        let st = self.state.read().await;
        let mut out: Vec<ChannelSummary> =
            st.channels.iter().map(|(&id, row)| summary_of(id, row)).collect();
        out.sort_by(|a, b| {
            b.messages.cmp(&a.messages).then_with(|| a.slug.cmp(&b.slug))
        });
        out
    }

    // --- the global timeline projection ------------------------------------

    /// The most recent `limit` messages across every channel, newest first.
    /// Bounded by [`TIMELINE_WINDOW`] — see the module docs.
    pub async fn timeline(&self, limit: usize) -> Vec<TimelineRow> {
        let st = self.state.read().await;
        st.timeline
            .iter()
            .rev()
            .take(limit)
            .map(|row| TimelineRow {
                channel:       row.channel,
                channel_slug:  st
                    .channels
                    .get(&row.channel)
                    .map(|c| c.slug.clone())
                    .unwrap_or_default(),
                ordinal:       row.ordinal,
                author:        row.author,
                author_handle: st
                    .users
                    .get(&row.author)
                    .map(|u| u.handle.clone())
                    .unwrap_or_default(),
                preview:       row.preview.clone(),
                seq:           row.seq,
            })
            .collect()
    }

    /// **Read-your-writes barrier.** Block until this read model has folded
    /// past global `position`. Event-bounded: the pump advances only on a real
    /// commit-notification wake.
    pub async fn wait_for(&self, position: u64) {
        loop {
            if self.applied.load(Ordering::Acquire) > position {
                return;
            }
            // Arm the `notified()` future *before* the final check to avoid a
            // lost wakeup between the load and the await.
            let notified = self.notify.notified();
            if self.applied.load(Ordering::Acquire) > position {
                return;
            }
            notified.await;
        }
    }
}

fn summary_of(id: Id, row: &ChannelRow) -> ChannelSummary {
    ChannelSummary {
        id,
        slug: row.slug.clone(),
        topic: row.topic.clone(),
        messages: row.messages,
        reactions: row.reactions,
        archived: row.archived,
        created_seq: row.created_seq,
        last_seq: row.last_seq,
    }
}

/// The live pump: pull the next committed batch (parked event-bounded on the
/// store watermark while caught up — no poll loop), fold each record, advance
/// the watermark, pulse `wait_for` waiters, and write a checkpoint when the
/// cadence trips.
#[allow(clippy::too_many_arguments)]
async fn pump_loop<B: SubscribeBackend>(
    mut sub: Subscription<B>,
    state: Arc<RwLock<State>>,
    applied: Arc<AtomicU64>,
    notify: Arc<Notify>,
    anomalies: Arc<ProjectionAnomalies>,
    checkpoint: Option<Arc<CheckpointCfg>>,
    ckpt_errors: Arc<Mutex<Vec<String>>>,
) {
    let mut since_ckpt: u64 = 0;
    let mut last_ckpt = Instant::now();
    loop {
        let batch = match sub.next_batch().await {
            Ok(b) => b,
            Err(_) => {
                tokio::time::sleep(PUMP_ERROR_BACKOFF).await;
                continue;
            }
        };
        {
            let mut st = state.write().await;
            for rec in &batch {
                st.apply_record(rec, &anomalies);
            }
        }
        let now_applied = sub.position();
        applied.store(now_applied, Ordering::Release);
        notify.notify_waiters();

        if let Some(cfg) = &checkpoint {
            since_ckpt += batch.len() as u64;
            if since_ckpt >= cfg.every_n || last_ckpt.elapsed() >= cfg.every_t {
                let snap = state.read().await.clone();
                if let Err(e) = write_checkpoint(
                    snap,
                    now_applied,
                    PROJECTION_VERSION,
                    &cfg.path,
                ) {
                    let msg = format!(
                        "checkpoint write failed at position {now_applied}: \
                         {e}"
                    );
                    eprintln!("chatter projections: WARNING {msg}");
                    ckpt_errors
                        .lock()
                        .expect("checkpoint error lock")
                        .push(msg);
                }
                since_ckpt = 0;
                last_ckpt = Instant::now();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preview_is_character_bounded_and_never_splits_a_codepoint() {
        let body = "🎉".repeat(PREVIEW_CHARS + 10);
        let p = preview(&body);
        assert_eq!(p.chars().count(), PREVIEW_CHARS + 1, "preview + ellipsis");
        assert!(p.ends_with('…'));
        let short = "hello";
        assert_eq!(preview(short), "hello");
    }

    #[test]
    fn timeline_window_is_bounded() {
        let mut st = State::default();
        let channel = Id::from_parts(1, [1; 10]);
        let author = Id::from_parts(1, [2; 10]);
        for i in 0..(TIMELINE_WINDOW as u64 * 3) {
            st.apply_channel(
                channel,
                i,
                &ChannelEvent::MessagePosted {
                    ordinal: i,
                    author,
                    body: format!("message {i}"),
                },
            );
        }
        assert_eq!(st.timeline.len(), TIMELINE_WINDOW);
        assert_eq!(st.messages, TIMELINE_WINDOW as u64 * 3);
        // The window holds the NEWEST messages.
        assert_eq!(
            st.timeline.back().unwrap().ordinal,
            TIMELINE_WINDOW as u64 * 3 - 1
        );
    }

    #[test]
    fn canonical_bytes_are_iteration_order_independent() {
        let mut a = State::default();
        let mut b = State::default();
        let users: Vec<Id> =
            (0..64u64).map(|i| Id::from_parts(i, [i as u8; 10])).collect();
        for (i, &u) in users.iter().enumerate() {
            a.apply_user(
                u,
                &UserEvent::Registered {
                    handle:       format!("u{i}"),
                    display_name: format!("User {i}"),
                },
            );
        }
        for (i, &u) in users.iter().enumerate().rev() {
            b.apply_user(
                u,
                &UserEvent::Registered {
                    handle:       format!("u{i}"),
                    display_name: format!("User {i}"),
                },
            );
        }
        assert_eq!(a.canonical_bytes(), b.canonical_bytes());
    }

    #[test]
    fn checkpoint_load_distinguishes_absent_from_corrupt() {
        let t = mess_testkit::sweeping_temp_dir("chatter-ckpt-load");
        let path = t.path().join("ckpt");
        assert!(matches!(load_checkpoint(&path).1, CheckpointStatus::Absent));
        std::fs::write(&path, b"\x00\x01\x02 not a checkpoint").unwrap();
        assert!(matches!(
            load_checkpoint(&path).1,
            CheckpointStatus::Rejected { .. }
        ));
        write_checkpoint(State::default(), 7, PROJECTION_VERSION, &path)
            .unwrap();
        assert_eq!(
            load_checkpoint(&path).1,
            CheckpointStatus::Loaded { applied: 7 }
        );
        // A stale fold version is rejected, not half-trusted.
        write_checkpoint(State::default(), 7, PROJECTION_VERSION + 1, &path)
            .unwrap();
        assert!(matches!(
            load_checkpoint(&path).1,
            CheckpointStatus::Rejected { .. }
        ));
    }
}
