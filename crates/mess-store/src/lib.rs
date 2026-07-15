//! mess v1: the `EventStore` facade — the north-star DX seam.
//!
//! This crate turns [`mess_core`]'s trait vocabulary ([`Event`],
//! [`Aggregate`], [`Decide`]) into a working store with three calls:
//!
//! - [`EventStore::load`] — replay a stream into aggregate state (paged, no
//!   length ceiling at the API level).
//! - [`EventStore::append`] — write events at an exact expected version.
//! - [`EventStore::command`] — the north star: `load → decide → append`, with
//!   bounded, jittered optimistic retry on version conflicts.
//!
//! # Engine-agnostic
//!
//! The facade sits on the [`Backend`] trait (`head` / `read_stream` /
//! `read_global` / borrowed and owned append), distilled from how
//! `spikes/dx_api` drove the original database actor. Production composes
//! [`FjallSnapshotBackend`] over [`LogEngine`]; the optional in-memory
//! [`MockBackend`] enforces real expected-version conflict semantics under a
//! lock so facade retry and concurrency behavior can also be tested without
//! durable storage.
//!
//! # Error shape
//!
//! [`EventStore::command`] speaks [`mess_core::CommandError<R, S>`]: `R` is the
//! aggregate's [`Decide::Rejection`] (a business rule refused the command),
//! `S` is this crate's [`StoreError`] (the plumbing failed), and the third
//! variant, [`CommandError::Conflict`](mess_core::CommandError::Conflict), is
//! the distinct **conflict-exhaustion** outcome carrying the attempt count.
//!
//! An app seam that wraps several aggregates behind one trait (an HTTP
//! write surface, say) usually wants `R` typed per call but does not want
//! `S` — this crate's `StoreError<B::Error>` — rippling into its own error
//! type. [`CommandError::erase_store`](mess_core::CommandError::erase_store)
//! (and the more general
//! [`map_store`](mess_core::CommandError::map_store)) exist for exactly that
//! seam: they keep `R` typed and collapse `S` to
//! [`mess_core::BoxedStoreError`], preserving `Display` and the `source()`
//! chain. See that method's doc comment for a worked before/after.
//!
//! [`Event`]: mess_core::Event
//! [`Aggregate`]: mess_core::Aggregate
//! [`Decide`]: mess_core::Decide
//! [`Decide::Rejection`]: mess_core::Decide::Rejection
//! [`MockBackend`]: crate::mock::MockBackend

pub mod anomalies;
pub mod backend;
pub mod cache;
pub mod engine;
pub mod fjall_snapshot;
#[cfg(feature = "mock")]
pub mod mock;
pub mod registry;
pub mod retry;
pub mod snapshot;
pub mod store;
pub mod subscription;
pub mod version;

pub use anomalies::{
    AnomalyCounter, AnomalyCounterSnapshot, AnomalyKind, ProjectionAnomalies,
    ProjectionAnomaliesSnapshot,
};
pub use backend::{
    AppendError, Appended, Backend, GlobalPage, OwnedAppendBatch,
    RecordToAppend, StoredRecord, SubscribeBackend,
};
pub use cache::StateCache;
pub use engine::{
    AppendInputMetrics, CommitterMetrics, EngineError, EngineMetrics,
    EngineOptions, LogEngine,
};
pub use fjall_snapshot::{FjallSnapshotBackend, SnapshotBackendError};
// Re-export the core command error (with its store-erasure target and the
// authored-command trait) the facade returns so callers need not depend on
// `mess-core` directly just to match on a command outcome, erase the
// backend type at an app seam, or implement [`EventStore::command_as`].
pub use mess_core::{Actor, BoxedStoreError, CommandError};
/// Re-export of `mess-log`'s fsync-mode enum, so an app can pick
/// [`EngineOptions::durability`] (e.g. `Durability::group_default()` for
/// fsync-coalesced group commit, `docs/perf/bulk-writes.md`) through
/// `mess-store`'s public API alone — no direct `mess-log` dependency
/// needed.
pub use mess_log::committer::Durability;
#[cfg(feature = "mock")]
pub use mock::MockBackend;
pub use retry::{DEFAULT_MAX_ATTEMPTS, RetryPolicy};
pub use snapshot::{
    BlobPtr, Hash256, SnapshotPolicy, SnapshotRef, SnapshotStore,
    Snapshottable, StateCodecError, StoredSnapshot, interim_stream_id,
};
pub use store::{
    AuthoredCommandError, Commit, DEFAULT_PAGE_SIZE, EventStore, Loaded,
    SnapshotMetrics, StoreError,
};
pub use subscription::Subscription;
pub use version::Version;
