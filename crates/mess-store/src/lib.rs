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
//! The facade sits on the [`Backend`] trait
//! (`head` / `read_stream` / `read_global` / `append_batch`), distilled from
//! how `spikes/dx_api` drove the real `mess_db` actor. This crate ships one
//! backend — the in-memory [`MockBackend`], which enforces **real**
//! expected-version conflict semantics under a lock — so the facade, its
//! retry machinery, and its concurrency guarantees are all testable with no
//! RocksDB. The `mess_db` wrapper is a separate Phase 2 bone.
//!
//! # Error shape
//!
//! [`EventStore::command`] speaks [`mess_core::CommandError<R, S>`]: `R` is the
//! aggregate's [`Decide::Rejection`] (a business rule refused the command),
//! `S` is this crate's [`StoreError`] (the plumbing failed), and the third
//! variant, [`CommandError::Conflict`](mess_core::CommandError::Conflict), is
//! the distinct **conflict-exhaustion** outcome carrying the attempt count.
//!
//! [`Event`]: mess_core::Event
//! [`Aggregate`]: mess_core::Aggregate
//! [`Decide`]: mess_core::Decide
//! [`Decide::Rejection`]: mess_core::Decide::Rejection

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
pub mod version;

pub use backend::{
    AppendError, Appended, Backend, RecordToAppend, StoredRecord,
};
pub use cache::StateCache;
pub use engine::{EngineError, EngineOptions, LogEngine};
pub use fjall_snapshot::{FjallSnapshotBackend, SnapshotBackendError};
#[cfg(feature = "mock")]
pub use mock::MockBackend;
pub use retry::{DEFAULT_MAX_ATTEMPTS, RetryPolicy};
pub use snapshot::{
    BlobPtr, Hash256, SnapshotRef, SnapshotStore, Snapshottable,
    StateCodecError, StoredSnapshot, interim_stream_id,
};
pub use store::{Commit, DEFAULT_PAGE_SIZE, EventStore, Loaded, StoreError};
pub use version::Version;

// Re-export the core command error the facade returns so callers need not
// depend on `mess-core` directly just to match on a command outcome.
pub use mess_core::CommandError;
