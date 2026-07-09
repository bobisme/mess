//! [`StateCache`]: the hot-aggregate state cache — doc-02's "mutable hot cache
//! over immutable cold truth", living **above** the [`Backend`](crate::Backend)
//! seam so it works identically on the interim [`MockBackend`](crate::mock) and
//! the future RocksDB engine (build once).
//!
//! # What it caches
//!
//! One entry per stream: `(version, folded aggregate state)`. That is all the
//! warm path needs — [`command`](crate::EventStore::command_cached) does
//! `version-check → decide → append` with **no replay and no fold** on the warm
//! path, because the version check is the one the append performs anyway, and
//! on a successful append the caller already holds the events it just wrote, so
//! it folds them straight into the cached state (**write-through fold** — no
//! invalidation protocol, no re-read).
//!
//! # Why type erasure
//!
//! [`EventStore`](crate::EventStore) is generic over the *backend*, not the
//! aggregate: one store serves many aggregate types, each on its own streams.
//! So an entry stores its state as `Arc<dyn Any + Send + Sync>` and is
//! downcast back to the concrete `A` on read. A key that ever holds the wrong
//! type simply downcasts to `None` and is treated as a miss — safe by
//! construction.
//!
//! # Why `quick_cache`
//!
//! `quick_cache` is **already a workspace dependency** — the retired
//! `mess_ecs::ComponentStore` used it for exactly this "hot component" role, so
//! adopting it here pulls in **zero new dependencies** and reuses a vetted
//! choice (the bone calls this its proper home). It is a sharded, concurrent
//! LRU with interior mutability: `&self` get/insert, no external lock — which
//! matters because this cache is **shared across the contended writers** (the
//! 8-writer path), where a coarse `Mutex<LruCache>` would serialize them.
//! `moka` was the alternative but is heavier (spawns maintenance tasks) and
//! buys nothing this workload needs.
//!
//! # Off-switch, and "cache-off == cache-miss"
//!
//! A [`disabled`](StateCache::disabled) cache holds no map at all; every
//! [`get`](StateCache::get) returns `None`, which is byte-for-byte the
//! cache-miss signal. The caller therefore runs **the same fallthrough code**
//! (snapshot + tail via `load_cached`) whether the cache is off or merely cold
//! — there is no second code path to keep in sync. See
//! [`EventStore::command_cached`](crate::EventStore::command_cached).
//!
//! # Durability caveat (same as D7 cursors)
//!
//! The cache is **in-memory only and never persisted** — a persisted folded
//! state *is* a snapshot (Phase 4/5, with certificates), which this is
//! deliberately not. A crash drops it and the next load rebuilds from the log.
//! Under a **Buffered** durability mode a backend may ack an append before it
//! is durable; the write-through fold then reflects an acked-but-not-yet-durable
//! event — exactly the documented D7 read-cursor caveat, and no worse: a crash
//! that loses the tail also drops the cache, so the two heal together.

use std::any::Any;
use std::sync::Arc;

use quick_cache::sync::Cache;

use crate::version::Version;

/// One cached aggregate: the version it was folded to, plus its type-erased
/// state. Cheap to clone — the state is shared behind an `Arc`.
#[derive(Clone)]
struct Entry {
    version: Version,
    state: Arc<dyn Any + Send + Sync>,
}

/// A capacity-bounded, in-memory cache of hot aggregate state keyed by stream.
///
/// Cheap to clone; a clone shares the same underlying map (the map lives behind
/// the `Arc` `quick_cache` uses internally), so every
/// [`EventStore`](crate::EventStore) clone sees the same warm entries. See the
/// [module docs](self) for the design rationale and the durability caveat.
#[derive(Clone)]
pub struct StateCache {
    /// `None` is the **off-switch**: no map, every `get` misses, so the caller
    /// runs the identical cache-miss fallthrough (no forked code path).
    inner: Option<Arc<Cache<String, Entry>>>,
}

impl std::fmt::Debug for StateCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateCache")
            .field("enabled", &self.inner.is_some())
            .field("len", &self.len())
            .finish()
    }
}

impl Default for StateCache {
    /// Disabled — the default keeps [`EventStore::new`](crate::EventStore::new)
    /// behavior (and every existing test) unchanged until a caller opts in.
    fn default() -> Self {
        Self::disabled()
    }
}

impl StateCache {
    /// The off-switch: a cache that stores nothing and always misses.
    #[must_use]
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    /// An enabled cache holding at most `capacity` hot aggregates (clamped to at
    /// least 1). Eviction is `quick_cache`'s LRU-ish policy; an evicted stream
    /// simply falls back to the snapshot + tail load on its next touch.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self { inner: Some(Arc::new(Cache::new(capacity.max(1)))) }
    }

    /// Whether this cache actually stores entries (the off-switch is off).
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Number of live entries (0 when disabled).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.as_ref().map_or(0, |c| c.len())
    }

    /// Whether the cache holds no entries (always true when disabled).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The warm-path read: the cached `(version, state)` for `stream_id`, if a
    /// live entry of the requested aggregate type `A` exists.
    ///
    /// Returns `None` on the off-switch, on a cold key, or on a type mismatch —
    /// all three are the same "miss" the caller falls through on. The state is
    /// **cloned** out so the caller can fold into it without disturbing the
    /// shared entry (a concurrent writer may still be reading the old one).
    #[must_use]
    pub fn get<A: Clone + Send + Sync + 'static>(
        &self,
        stream_id: &str,
    ) -> Option<(Version, A)> {
        let entry = self.inner.as_ref()?.get(stream_id)?;
        let state = entry.state.downcast_ref::<A>()?.clone();
        Some((entry.version, state))
    }

    /// The write-through update: record that `stream_id` is folded to `version`
    /// with `state`. A no-op when disabled.
    pub fn put<A: Send + Sync + 'static>(
        &self,
        stream_id: &str,
        version: Version,
        state: A,
    ) {
        if let Some(cache) = &self.inner {
            cache.insert(
                stream_id.to_string(),
                Entry { version, state: Arc::new(state) },
            );
        }
    }

    /// Forget any entry for `stream_id` (e.g. a poisoned or tainted read). A
    /// no-op when disabled or already absent.
    pub fn invalidate(&self, stream_id: &str) {
        if let Some(cache) = &self.inner {
            cache.remove(stream_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct S(i64);

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Other(String);

    #[test]
    fn disabled_always_misses_and_never_stores() {
        let cache = StateCache::disabled();
        assert!(!cache.is_enabled());
        cache.put("s", Version::At(3), S(9));
        // The put was a no-op; a get still misses — identical to a cold key.
        assert_eq!(cache.get::<S>("s"), None);
        assert!(cache.is_empty());
    }

    #[test]
    fn round_trips_version_and_state() {
        let cache = StateCache::with_capacity(4);
        cache.put("s", Version::At(2), S(42));
        assert_eq!(cache.get::<S>("s"), Some((Version::At(2), S(42))));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn type_mismatch_is_a_miss_not_a_panic() {
        let cache = StateCache::with_capacity(4);
        cache.put("s", Version::At(0), S(1));
        // Wrong aggregate type for this key: safe miss, no downcast panic.
        assert_eq!(cache.get::<Other>("s"), None);
        // The correct type still reads.
        assert_eq!(cache.get::<S>("s"), Some((Version::At(0), S(1))));
    }

    #[test]
    fn invalidate_forgets_the_entry() {
        let cache = StateCache::with_capacity(4);
        cache.put("s", Version::NoStream, S(0));
        cache.invalidate("s");
        assert_eq!(cache.get::<S>("s"), None);
    }
}
