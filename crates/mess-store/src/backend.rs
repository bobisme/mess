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
    pub data:         Vec<u8>,
}

/// One event as read back from a backend: the stored bytes plus the positions
/// the backend assigned it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRecord {
    /// The stream this event belongs to.
    pub stream_id:       String,
    /// The event's stored message type (its `mess_core::Event::name`).
    pub message_type:    String,
    /// The encoded event payload.
    pub data:            Vec<u8>,
    /// 0-based position of this event within its stream.
    pub stream_position: u64,
    /// Monotonic position of this event across the whole store.
    pub global_position: u64,
}

impl StoredRecord {
    /// Split [`stream_id`](Self::stream_id) into its category and suffix at
    /// the **first** `-` — the one blessed way to route a
    /// [`read_global`](crate::Backend::read_global)/subscribe record by
    /// category without ad-hoc prefix surgery at every call site.
    ///
    /// This mirrors the app-level convention every stream id is built from
    /// (e.g. `examples/social`'s `user_stream`/`post_stream`:
    /// `format!("{category}-{id}")`): a category tag, a `-`, then an entity
    /// id. The engine does not (yet) intern a per-stream category — the real
    /// [`LogEngine`](crate::LogEngine) currently registers every stream
    /// under the reserved system category — so this is well-defined string
    /// splitting over the convention, not a lookup into engine state.
    ///
    /// # Invariants
    ///
    /// - Splits at the **first** `-` only, so a suffix that itself contains `-`
    ///   (e.g. `post-abc-123`) stays intact: category `"post"`, suffix
    ///   `"abc-123"`.
    /// - No `-` anywhere in `stream_id` (e.g. the reserved `"$registry"`
    ///   stream): the whole string is the category, the suffix is `""`.
    /// - A leading `-` (e.g. `"-42"`) yields an empty category `""` and suffix
    ///   `"42"` — never `None`; category/suffix are always defined, just
    ///   possibly empty.
    /// - The split is on the ASCII byte `-`, which is always a full UTF-8
    ///   character on its own, so this never mis-splits inside a multi-byte
    ///   unicode codepoint on either side.
    ///
    /// Returns `(category, suffix)`; [`category`](Self::category) and
    /// [`stream_suffix`](Self::stream_suffix) are convenience accessors for
    /// just one half.
    #[must_use]
    pub fn category_and_suffix(&self) -> (&str, &str) {
        self.stream_id.split_once('-').unwrap_or((&self.stream_id, ""))
    }

    /// The category tag of [`stream_id`](Self::stream_id) — everything
    /// before the first `-`, or the whole id if there is no `-`. See
    /// [`category_and_suffix`](Self::category_and_suffix) for the full
    /// invariants.
    #[must_use]
    pub fn category(&self) -> &str { self.category_and_suffix().0 }

    /// The entity-id suffix of [`stream_id`](Self::stream_id) — everything
    /// after the first `-`, or `""` if there is no `-`. See
    /// [`category_and_suffix`](Self::category_and_suffix) for the full
    /// invariants.
    #[must_use]
    pub fn stream_suffix(&self) -> &str { self.category_and_suffix().1 }
}

#[cfg(test)]
mod stored_record_tests {
    use super::StoredRecord;

    fn rec(stream_id: &str) -> StoredRecord {
        StoredRecord {
            stream_id:       stream_id.to_string(),
            message_type:    "Test".to_string(),
            data:            Vec::new(),
            stream_position: 0,
            global_position: 0,
        }
    }

    #[test]
    fn splits_category_and_suffix_at_first_dash() {
        let r = rec("user-42");
        assert_eq!(r.category(), "user");
        assert_eq!(r.stream_suffix(), "42");
        assert_eq!(r.category_and_suffix(), ("user", "42"));
    }

    #[test]
    fn separator_in_suffix_stays_in_suffix() {
        // Only the FIRST `-` is the category/suffix boundary.
        let r = rec("post-abc-123");
        assert_eq!(r.category(), "post");
        assert_eq!(r.stream_suffix(), "abc-123");
    }

    #[test]
    fn no_separator_is_all_category() {
        let r = rec("$registry");
        assert_eq!(r.category(), "$registry");
        assert_eq!(r.stream_suffix(), "");
    }

    #[test]
    fn empty_stream_id_is_all_category() {
        let r = rec("");
        assert_eq!(r.category(), "");
        assert_eq!(r.stream_suffix(), "");
    }

    #[test]
    fn leading_separator_is_empty_category() {
        let r = rec("-42");
        assert_eq!(r.category(), "");
        assert_eq!(r.stream_suffix(), "42");
    }

    #[test]
    fn trailing_separator_is_empty_suffix() {
        let r = rec("user-");
        assert_eq!(r.category(), "user");
        assert_eq!(r.stream_suffix(), "");
    }

    #[test]
    fn unicode_category_and_suffix() {
        // The separator `-` is a single-byte ASCII char, so splitting on it
        // never lands inside a multi-byte codepoint on either side.
        let r = rec("café-☕42");
        assert_eq!(r.category(), "café");
        assert_eq!(r.stream_suffix(), "☕42");
    }

    #[test]
    fn only_separator() {
        let r = rec("-");
        assert_eq!(r.category(), "");
        assert_eq!(r.stream_suffix(), "");
    }
}

/// A successful append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Appended {
    /// The stream's version after the append (position of the last event
    /// written).
    pub version:              Version,
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
        actual:   Version,
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

/// A [`Backend`] that also exposes the **committed global watermark** and an
/// event-bounded wait on it — the two primitives the app-facing subscription /
/// live-tail API ([`EventStore::subscribe`](crate::EventStore::subscribe),
/// [`EventStore::watermark`](crate::EventStore::watermark),
/// [`EventStore::await_past`](crate::EventStore::await_past)) is built on.
///
/// This is an **additive** capability trait: the base [`Backend`] seam (and its
/// [`read_global`](Backend::read_global) catch-up path) is untouched, so a
/// backend that only stores events need not implement it. A backend that *does*
/// implement it promises a monotone commit-notification hook rather than a
/// parallel signalling system — internally both shipped backends reuse the
/// `mess-log` durable watermark (`mess_log::watermark::Watermark`, the same
/// primitive the log's D11 subscription runtime awaits).
///
/// # Watermark meaning
///
/// The watermark is the **exclusive end of the committed global-position
/// sequence**: every global position `< watermark` is committed and visible to
/// [`read_global`](Backend::read_global), and no position `>= watermark` is yet
/// readable. Equivalently it is the count of committed events. It is monotone
/// non-decreasing while the store is live (it can only regress across a crash +
/// recovery, never in-process).
///
/// For the composed [`LogEngine`](crate::LogEngine) the watermark tracks the
/// **published** end — a position is counted only once its payload is resident
/// in the read path that [`read_global`](Backend::read_global) serves, which is
/// strictly after the durable committer acked it. So a waiter woken by
/// [`await_watermark_past`](SubscribeBackend::await_watermark_past) is
/// guaranteed the position it waited for is already readable, not merely
/// durable-but-not-yet-materialised.
pub trait SubscribeBackend: Backend {
    /// The current committed global watermark (see the trait docs): the
    /// exclusive end of the readable global-position sequence.
    fn watermark(
        &self,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Resolve once the committed watermark has advanced strictly **past**
    /// global position `pos` — i.e. once `watermark > pos`, so position `pos`
    /// is committed and visible to [`read_global`](Backend::read_global).
    /// Resolves immediately if the watermark is already there.
    ///
    /// This is the event-bounded live-tail primitive: it is driven by commit
    /// notification (the durable watermark's waker list), never by busy
    /// polling. Dropping the returned future (e.g. a cancelled `next_batch`)
    /// deregisters the waiter and can never wedge the committer.
    fn await_watermark_past(
        &self,
        pos: u64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
