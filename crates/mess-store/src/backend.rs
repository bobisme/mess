//! The [`Backend`] trait: the engine-agnostic seam the [`EventStore`] facade
//! sits on.
//!
//! The facade never names a storage engine. It speaks four operations —
//! [`head`](Backend::head), [`read_stream`](Backend::read_stream),
//! [`read_global`](Backend::read_global), and
//! [`append_batch`](Backend::append_batch) — distilled from how
//! `spikes/dx_api/src/store.rs` drove the `mess_db` actor. Payloads cross this
//! seam as opaque `(message_type, data)` byte records: encoding is the
//! facade's job (via [`mess_core::Event`]), durability and ordering are the
//! backend's.
//!
//! [`EventStore`](crate::EventStore) is generic over `B: Backend`, so the
//! trait uses native `async fn` (return-position `impl Future`) with explicit
//! `Send` bounds rather than boxing — the futures must be `Send` so a caller
//! can `tokio::spawn` a command. The in-memory [`MockBackend`](crate::mock)
//! implements it with real expected-version conflict semantics; the RocksDB
//! wrapper is Phase 2 work.

use std::future::Future;

use crate::version::Version;

/// One event to append, as it crosses the backend seam: a stable message-type
/// name plus its already-encoded payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordToAppend {
    /// The event's stable name (`mess_core::Event::name`), stored as the
    /// message type.
    pub message_type: String,
    /// The encoded event payload (`mess_core::Event::encode`).
    pub data: Vec<u8>,
}

/// One event as read back from a backend: the stored bytes plus the positions
/// the backend assigned it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRecord {
    /// The stream this event belongs to.
    pub stream_id: String,
    /// The event's stored message type (its `mess_core::Event::name`).
    pub message_type: String,
    /// The encoded event payload.
    pub data: Vec<u8>,
    /// 0-based position of this event within its stream.
    pub stream_position: u64,
    /// Monotonic position of this event across the whole store.
    pub global_position: u64,
}

/// A successful append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Appended {
    /// The stream's version after the append (position of the last event
    /// written).
    pub version: Version,
    /// Global position of the last event written.
    pub last_global_position: u64,
}

/// Why an [`append_batch`](Backend::append_batch) did not commit.
///
/// [`Conflict`](AppendError::Conflict) is the optimistic-concurrency signal
/// the facade retries on; every other failure is an opaque
/// [`Backend`](AppendError::Backend) error the facade surfaces as-is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendError<E> {
    /// The stream's actual version did not match the expected version — a
    /// concurrent writer moved it. The facade reloads and retries.
    Conflict {
        /// The version the append expected the stream to be at.
        expected: Version,
        /// The version the stream was actually at.
        actual: Version,
    },
    /// An engine-level failure with no optimistic-retry semantics.
    Backend(E),
}

impl<E: std::fmt::Display> std::fmt::Display for AppendError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppendError::Conflict { expected, actual } => write!(
                f,
                "version conflict: expected {expected:?}, actual {actual:?}"
            ),
            AppendError::Backend(e) => write!(f, "{e}"),
        }
    }
}

impl<E: std::error::Error> std::error::Error for AppendError<E> {}

/// The storage engine beneath [`EventStore`](crate::EventStore).
///
/// Implementations must enforce **exact expected-version** semantics on
/// [`append_batch`](Backend::append_batch): the batch commits atomically iff
/// the stream is exactly at `expected`, otherwise it returns
/// [`AppendError::Conflict`]. That single guarantee is what makes the facade's
/// optimistic retry correct under concurrency.
pub trait Backend: Send + Sync + 'static {
    /// The engine's own failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// The current version of `stream_id` (its last event's position, or
    /// [`Version::NoStream`]).
    fn head(
        &self,
        stream_id: &str,
    ) -> impl Future<Output = Result<Version, Self::Error>> + Send;

    /// Read up to `limit` events of `stream_id` strictly after the `after`
    /// cursor, in ascending stream order.
    ///
    /// The facade calls this in a loop, advancing `after`, so there is no
    /// per-call ceiling on stream length at the API level. Returning fewer
    /// than `limit` events signals the end of the stream.
    fn read_stream(
        &self,
        stream_id: &str,
        after: Version,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<StoredRecord>, Self::Error>> + Send;

    /// Read up to `limit` events across all streams strictly after global
    /// position `after` (exclusive; pass `None` to start from the beginning),
    /// in ascending global order.
    fn read_global(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<StoredRecord>, Self::Error>> + Send;

    /// Append `records` to `stream_id` iff it is exactly at `expected`.
    ///
    /// On a version mismatch this must return [`AppendError::Conflict`] and
    /// write nothing. An empty `records` slice is a no-op that still validates
    /// `expected`.
    fn append_batch(
        &self,
        stream_id: &str,
        expected: Version,
        records: &[RecordToAppend],
    ) -> impl Future<Output = Result<Appended, AppendError<Self::Error>>> + Send;
}
