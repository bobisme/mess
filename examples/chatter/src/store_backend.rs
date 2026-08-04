//! Type aliases, the on-disk knobs, and the open helpers naming the concrete
//! backend the `chatter` binary writes through.
//!
//! [`Store`] is the same warm-write stack `examples/social` pins — an
//! [`EventStore`] over a [`PackSnapshotBackend`] wrapping a [`LogEngine`] — so
//! the hot-aggregate write-through cache (`command_cached`) and the
//! subscription capability both come from one handle. What is *new* here is
//! that chatter reaches into [`EngineOptions`] for the two knobs the sealed
//! tier is gated on:
//!
//! - **`segment_size`** — the knob `examples/social` never exposes. A small
//!   value makes the active segment fill and roll in seconds, and every rolled
//!   segment is sealed by the background sealer. That is the whole difference
//!   between a corpus that exercises the sealed tier and one that cannot.
//! - **`seal_pack`** — SealPack (one `.seal` per sealed segment, the default)
//!   versus the legacy loose sidecar trio (`.pidx`/`.filter`/`.pcol`). Both are
//!   permanently supported shapes; flipping the flag between two seeds is a
//!   reproducible A/B (bn-1yor's matrix, from an example).
//!
//! # Why the knobs are persisted in a sidecar
//!
//! [`EngineOptions`] is per-*open*, not per-store, and the engine
//! **re-allocates the resumed active segment to the opened
//! `segment_size`**. So reopening a store that was seeded with 1 MiB segments
//! under the 256 MiB default would silently grow the active segment file and
//! stop rolling — which would make every cold-reopen measurement a measurement
//! of a different store than the one that was seeded. [`write_config`] records
//! the knobs next to the log at seed time and [`open_store`] reads them back,
//! so `chatter seed` and every later `chatter …` invocation open the *same*
//! store. The sidecar is configuration, not data: deleting it costs the knobs
//! (the store then opens with engine defaults), never a fact.

use std::path::{Path, PathBuf};

use mess_store::{EngineOptions, EventStore, LogEngine, PackSnapshotBackend};
use serde::{Deserialize, Serialize};

use crate::projections::Projections;

/// The on-disk **warm-write** store: [`EventStore`] over
/// [`PackSnapshotBackend<LogEngine>`]. Being a
/// [`SnapshotStore`](mess_store::SnapshotStore) is what unlocks
/// `command_cached`; being a [`SubscribeBackend`](mess_store::SubscribeBackend)
/// (forwarded straight to the wrapped [`LogEngine`]) is what lets
/// [`StoreProjections`] tail the very same handle.
pub type Store = EventStore<PackSnapshotBackend<LogEngine>>;

/// The on-disk backend's rebuildable read model.
pub type StoreProjections = Projections<PackSnapshotBackend<LogEngine>>;

/// Default hot-aggregate cache capacity. Channels are few and hot; users are
/// many and cold. A modest window keeps every live channel's bounded state
/// resident, which is what turns a message append into an O(1) write on a
/// stream that is thousands of events deep.
const DEFAULT_CACHE_CAPACITY: usize = 8192;

/// The smallest `--segment-bytes` this example accepts. A segment must
/// comfortably fit its 52-byte header plus several batches; anything under
/// this is a footgun rather than a knob, and rolling every few events makes
/// the sealer, not the workload, the thing being measured.
pub const MIN_SEGMENT_BYTES: u64 = 64 * 1024;

/// Failure opening the on-disk store (either half — the event log or the
/// snapshot sidecar). Rendered to a `String` at the seam so the example's
/// error type does not name either half's concrete error.
#[derive(Debug)]
pub enum OpenError {
    /// The event-log engine failed to open.
    Log(String),
    /// The snapshot sidecar failed to open. In practice this means another
    /// live process already holds its writer lock — a corrupt or foreign
    /// sidecar does not fail, it degrades to replay-on-miss.
    Snapshot(String),
    /// The store-config sidecar could not be written.
    Config(String),
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenError::Log(e) => write!(f, "opening event log: {e}"),
            OpenError::Snapshot(e) => write!(f, "opening snapshot store: {e}"),
            OpenError::Config(e) => write!(f, "writing store config: {e}"),
        }
    }
}

impl std::error::Error for OpenError {}

// ===========================================================================
// The store-config sidecar
// ===========================================================================

/// The engine knobs a chatter store was created with. See the module docs for
/// why they are persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreConfig {
    /// Active-segment size in bytes — the roll (and therefore seal) cadence.
    pub segment_bytes: u64,
    /// `true` for one consolidated `.seal` pack per sealed segment (the engine
    /// default); `false` for the loose `.pidx`/`.filter`/`.pcol` trio.
    pub seal_pack:     bool,
}

impl Default for StoreConfig {
    fn default() -> Self {
        let defaults = EngineOptions::default();
        Self {
            segment_bytes: defaults.segment_size,
            seal_pack:     defaults.seal_pack,
        }
    }
}

impl StoreConfig {
    /// Clamp [`segment_bytes`](Self::segment_bytes) to [`MIN_SEGMENT_BYTES`].
    #[must_use]
    pub fn clamped(self) -> Self {
        Self {
            segment_bytes: self.segment_bytes.max(MIN_SEGMENT_BYTES),
            ..self
        }
    }

    fn engine_options(self) -> EngineOptions {
        EngineOptions {
            segment_size: self.segment_bytes.max(MIN_SEGMENT_BYTES),
            seal_pack: self.seal_pack,
            ..EngineOptions::default()
        }
    }
}

/// Magic + envelope version so a foreign or truncated file is rejected rather
/// than half-decoded into plausible-looking knobs.
#[derive(Serialize, Deserialize)]
struct ConfigEnvelope {
    magic:   u32,
    version: u16,
    config:  StoreConfig,
}

const CONFIG_MAGIC: u32 = 0x4348_5443; // "CHTC"
const CONFIG_VERSION: u16 = 1;

/// The store-config sidecar path: `<dir>/.chatter-store`.
#[must_use]
pub fn config_path(dir: &Path) -> PathBuf { dir.join(".chatter-store") }

/// Persist `cfg` next to the log. Called once, by `chatter seed`, before the
/// store is created.
pub fn write_config(dir: &Path, cfg: StoreConfig) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let env = ConfigEnvelope {
        magic:   CONFIG_MAGIC,
        version: CONFIG_VERSION,
        config:  cfg.clamped(),
    };
    let bytes = rmp_serde::to_vec(&env).map_err(std::io::Error::other)?;
    let path = config_path(dir);
    let mut tmp = path.clone().into_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    std::fs::write(&tmp, &bytes)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

/// Read the store-config sidecar, or [`StoreConfig::default`] when it is
/// missing, unreadable, or invalid. Never an error: the knobs are convenience,
/// the log is the store.
#[must_use]
pub fn read_config(dir: &Path) -> StoreConfig {
    let Ok(bytes) = std::fs::read(config_path(dir)) else {
        return StoreConfig::default();
    };
    match rmp_serde::from_slice::<ConfigEnvelope>(&bytes) {
        Ok(env)
            if env.magic == CONFIG_MAGIC && env.version == CONFIG_VERSION =>
        {
            env.config.clamped()
        }
        _ => {
            eprintln!(
                "chatter: WARNING store config at {} is unreadable; opening \
                 with engine defaults",
                config_path(dir).display()
            );
            StoreConfig::default()
        }
    }
}

// ===========================================================================
// Paths
// ===========================================================================

/// The snapshot sidecar directory kept **inside** the store dir:
/// `<dir>/.snapshots.packs`. The name matches `mess_cli::store`'s, so `mess
/// doctor` and `mess inspect` find the same sidecar this app writes.
#[must_use]
pub fn snapshot_root(dir: &Path) -> PathBuf { dir.join(".snapshots.packs") }

/// The projection checkpoint sidecar: `<dir>/.chatter-projections.ckpt`. The
/// one place this path is written down, so the binary, the rebuild proof
/// ([`crate::rebuild`]), and every test agree on it.
#[must_use]
pub fn checkpoint_path(dir: &Path) -> PathBuf {
    dir.join(".chatter-projections.ckpt")
}

/// The engine's sealed-artifact directory: `<dir>/sealed`.
#[must_use]
pub fn sealed_dir(dir: &Path) -> PathBuf { dir.join("sealed") }

// ===========================================================================
// Open
// ===========================================================================

/// Open the warm-write [`Store`] over `dir`, honouring the persisted
/// [`StoreConfig`] (engine defaults when the sidecar is absent).
pub fn open_store(dir: &Path) -> Result<Store, OpenError> {
    open_store_with(dir, read_config(dir))
}

/// Open the warm-write [`Store`] over `dir` with explicit knobs, **without**
/// touching the config sidecar. Used by `chatter seed` after
/// [`write_config`], and by tests that build a store in one step.
pub fn open_store_with(
    dir: &Path,
    cfg: StoreConfig,
) -> Result<Store, OpenError> {
    let engine = LogEngine::open_with(dir, cfg.engine_options())
        .map_err(|e| OpenError::Log(e.to_string()))?;
    let backend = PackSnapshotBackend::open(engine, snapshot_root(dir))
        .map_err(|e| OpenError::Snapshot(e.to_string()))?;
    Ok(EventStore::new(backend).with_cache_capacity(DEFAULT_CACHE_CAPACITY))
}

/// Write the config sidecar and open the store in one step — the `chatter
/// seed` entry point.
pub fn create_store(dir: &Path, cfg: StoreConfig) -> Result<Store, OpenError> {
    write_config(dir, cfg).map_err(|e| OpenError::Config(e.to_string()))?;
    open_store_with(dir, cfg)
}

// ===========================================================================
// Sealed-tier inspection (what makes this example the instrument it is)
// ===========================================================================

/// A coarse census of the store's sealed tier — the numbers this example
/// exists to make non-zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct SealedCensus {
    /// Sealed segments the engine has installed in its cold tier.
    pub sealed_segments:  usize,
    /// Sealed segments served by a consolidated `.seal` pack.
    pub seal_pack:        usize,
    /// Sealed segments served by a loose `.pidx` family.
    pub loose_sidecar:    usize,
    /// Sealed segments with a payload accelerator attached (the pack's payload
    /// section, or a loose `.pcol`) — what a sealed history read goes through.
    pub payload_indexed:  usize,
    /// `seg-*.log` files on disk, sealed and active.
    pub segment_files:    usize,
    /// Total bytes under the store dir, sidecars included.
    pub bytes:            u64,
    /// Events the engine has published (the read watermark), `$registry`
    /// records included.
    pub published_events: u64,
}

/// Take a [`SealedCensus`] of an open store rooted at `dir`.
///
/// Reads the engine's own observability report (never the private index), plus
/// a directory walk for the byte total.
#[must_use]
pub fn sealed_census(store: &Store, dir: &Path) -> SealedCensus {
    let engine = store.backend().inner();
    let obs = engine.observability();
    let metrics = engine.metrics();
    SealedCensus {
        sealed_segments:  obs.accelerators.segments.len(),
        seal_pack:        obs.accelerators.seal_pack_segments,
        loose_sidecar:    obs.accelerators.loose_sidecar_segments,
        payload_indexed:  obs
            .accelerators
            .segments
            .iter()
            .filter(|s| s.has_payload_index)
            .count(),
        segment_files:    count_segment_files(dir),
        bytes:            dir_bytes(dir),
        published_events: metrics.total_events,
    }
}

/// Count `seg-*.log` files directly under `dir`.
#[must_use]
pub fn count_segment_files(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else { return 0 };
    entries
        .flatten()
        .filter(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.starts_with("seg-") && name.ends_with(".log")
        })
        .count()
}

/// Count the sealed-artifact files under `<dir>/sealed` whose extension is
/// `ext` (e.g. `seal`, `pcol`, `pidx`). The on-disk cross-check for the
/// engine-reported [`SealedCensus`].
#[must_use]
pub fn count_sealed_artifacts(dir: &Path, ext: &str) -> usize {
    let Ok(entries) = std::fs::read_dir(sealed_dir(dir)) else { return 0 };
    entries
        .flatten()
        .filter(|e| {
            e.path().extension().is_some_and(|e| e.eq_ignore_ascii_case(ext))
        })
        .count()
}

/// Total bytes of every regular file under `dir`, recursively. Used for the
/// "how big is this corpus" line the seed and bench commands print.
#[must_use]
pub fn dir_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else { return 0 };
    let mut total = 0u64;
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            total += dir_bytes(&entry.path());
        } else if meta.is_file() {
            total += meta.len();
        }
    }
    total
}

/// Render a byte count in the units a human reads.
#[must_use]
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use mess_testkit::sweeping_temp_dir;

    use super::*;

    #[test]
    fn config_round_trips_and_clamps() {
        let t = sweeping_temp_dir("chatter-cfg");
        let cfg = StoreConfig { segment_bytes: 1024, seal_pack: false };
        write_config(t.path(), cfg).unwrap();
        let back = read_config(t.path());
        assert_eq!(back.seal_pack, false);
        assert_eq!(back.segment_bytes, MIN_SEGMENT_BYTES, "clamped on write");
    }

    #[test]
    fn missing_config_falls_back_to_defaults() {
        let t = sweeping_temp_dir("chatter-cfg-missing");
        assert_eq!(read_config(t.path()), StoreConfig::default());
    }

    #[test]
    fn corrupt_config_falls_back_to_defaults_without_erroring() {
        let t = sweeping_temp_dir("chatter-cfg-corrupt");
        std::fs::write(config_path(t.path()), b"not msgpack at all").unwrap();
        assert_eq!(read_config(t.path()), StoreConfig::default());
    }

    #[test]
    fn human_bytes_renders_units() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(2 * 1024), "2.0 KiB");
        assert_eq!(human_bytes(3 * 1024 * 1024), "3.0 MiB");
    }
}
