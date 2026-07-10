//! Adoption test for [`CommandError::erase_store`] /
//! [`CommandError::map_store`] (bn-188): an app-style seam, modeled on
//! `examples/social/src/contracts.rs`'s `WriteError`, that collapses several
//! aggregates' `CommandError<R, S>` outcomes into one backend-agnostic error
//! while keeping each aggregate's typed domain rejection.
//!
//! Two things are demonstrated:
//!
//! 1. [`erase_store_preserves_domain_and_conflict`] / the `Display`/`source`
//!    checks below — the erasure is lossless: `Domain` and `Conflict` are
//!    untouched, and a `Store` error's `Display` text and `source()` chain
//!    survive being boxed.
//! 2. [`seam_collapses_two_aggregates_into_one_error`] — the actual seam
//!    pattern: one `AppError` enum, built from two different aggregates'
//!    `CommandError`s via `.erase_store()`, driven end-to-end through
//!    [`EventStore::command`] against a backend that genuinely fails.

use std::fmt;

use mess_core::{
    Aggregate, BoxedStoreError, CodecError, CommandError, Decide, Event,
};
use mess_store::backend::{
    AppendError, Appended, Backend, RecordToAppend, StoredRecord,
};
use mess_store::{EventStore, MockBackend, Version};

// ===========================================================================
// Erasure semantics, no I/O: `map_store`/`erase_store` touch only `Store`.
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
struct DomainRejection(&'static str);

impl fmt::Display for DomainRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for DomainRejection {}

#[test]
fn erase_store_preserves_domain_and_conflict() {
    let domain: CommandError<DomainRejection, std::io::Error> =
        CommandError::Domain(DomainRejection("nope"));
    match domain.erase_store() {
        CommandError::Domain(DomainRejection(msg)) => assert_eq!(msg, "nope"),
        other => panic!("expected Domain, got {other:?}"),
    }

    let conflict: CommandError<DomainRejection, std::io::Error> =
        CommandError::Conflict { stream: "s-1".into(), attempts: 3 };
    match conflict.erase_store() {
        CommandError::Conflict { stream, attempts } => {
            assert_eq!(stream, "s-1");
            assert_eq!(attempts, 3);
        }
        other => panic!("expected Conflict, got {other:?}"),
    }
}

/// A store error with a real `#[source]` chain (an inner `io::Error`), the
/// way a `thiserror`-based backend error typically looks.
#[derive(Debug, thiserror::Error)]
#[error("disk write failed")]
struct DiskError {
    #[source]
    io: std::io::Error,
}

#[test]
fn erase_store_preserves_display_and_source_chain() {
    let io_err = std::io::Error::other("no space left on device");
    let io_msg = io_err.to_string();
    let store_err: CommandError<DomainRejection, DiskError> =
        CommandError::Store(DiskError { io: io_err });

    let erased: CommandError<DomainRejection, BoxedStoreError> =
        store_err.erase_store();

    // `Display` is unchanged by boxing.
    assert_eq!(erased.to_string(), "disk write failed");

    // `source()` still walks into the boxed error's own chain: erasure drops
    // the concrete type, not the chain.
    let CommandError::Store(boxed) = &erased else {
        panic!("expected Store");
    };
    let source = std::error::Error::source(boxed)
        .expect("boxed store error keeps its source");
    assert_eq!(source.to_string(), io_msg);

    // And the outer `CommandError` itself still exposes that same chain via
    // its own `source()` (the `Error` impl requires `S: Error + 'static`,
    // which `BoxedStoreError` satisfies).
    let outer_source = std::error::Error::source(&erased).expect(
        "CommandError<_, BoxedStoreError> still chains to its store error",
    );
    assert_eq!(outer_source.to_string(), "disk write failed");
}

// ===========================================================================
// The seam: two aggregates, one backend-agnostic error.
// ===========================================================================

/// A minimal aggregate whose only rule is "don't go negative" — just enough
/// domain shape to prove `Domain` survives the seam typed.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CounterEvent {
    Incremented { by: i64 },
}

impl Event for CounterEvent {
    fn name(&self) -> &'static str { "counter.incremented" }

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let CounterEvent::Incremented { by } = self;
        Ok(by.to_le_bytes().to_vec())
    }

    fn decode(name: &str, data: &[u8]) -> Result<Self, CodecError> {
        if name != "counter.incremented" {
            return Err(CodecError::UnknownEventName(name.to_string()));
        }
        let bytes: [u8; 8] =
            data.try_into().map_err(|_| CodecError::Decode {
                event_name: name.to_string(),
                source:     format!(
                    "expected 8 payload bytes, got {}",
                    data.len()
                ),
            })?;
        Ok(CounterEvent::Incremented { by: i64::from_le_bytes(bytes) })
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Counter {
    value: i64,
}

impl Aggregate for Counter {
    type Event = CounterEvent;

    fn apply(&mut self, event: &CounterEvent) {
        let CounterEvent::Incremented { by } = event;
        self.value += by;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CounterError(String);

impl fmt::Display for CounterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for CounterError {}

#[derive(Debug, Clone, Copy)]
struct Increment {
    by: i64,
}

impl Decide<Increment> for Counter {
    type Rejection = CounterError;

    fn decide(
        &self,
        cmd: Increment,
    ) -> Result<Vec<CounterEvent>, CounterError> {
        if self.value + cmd.by < 0 {
            return Err(CounterError(format!(
                "increment by {} would take {} negative",
                cmd.by, self.value
            )));
        }
        Ok(vec![CounterEvent::Incremented { by: cmd.by }])
    }
}

/// The seam error: `examples/social/src/contracts.rs`'s `WriteError` shape,
/// but with `Store` erased to [`BoxedStoreError`] rather than stringified —
/// this is the "after" from [`CommandError::erase_store`]'s doc comment.
#[derive(Debug)]
enum AppError {
    Counter(CounterError),
    Conflict { stream: String, attempts: u32 },
    Store(BoxedStoreError),
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::Counter(e) => write!(f, "{e}"),
            AppError::Conflict { stream, attempts } => {
                write!(
                    f,
                    "write conflict on {stream:?} after {attempts} attempts"
                )
            }
            AppError::Store(e) => write!(f, "store error: {e}"),
        }
    }
}

impl std::error::Error for AppError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AppError::Counter(e) => Some(e),
            AppError::Conflict { .. } => None,
            AppError::Store(e) => Some(e),
        }
    }
}

/// One place `S` is ever named for the `Counter` aggregate — the same shape
/// as social's `user_err`/`post_err`, just erasing instead of stringifying.
fn counter_err<S>(e: CommandError<CounterError, S>) -> AppError
where
    S: std::error::Error + Send + Sync + 'static,
{
    match e.erase_store() {
        CommandError::Domain(d) => AppError::Counter(d),
        CommandError::Conflict { stream, attempts } => {
            AppError::Conflict { stream, attempts }
        }
        CommandError::Store(s) => AppError::Store(s),
    }
}

/// A [`Backend`] that delegates to a [`MockBackend`] but can be told to fail
/// every subsequent `append_batch` with a real `#[source]`-chained error, to
/// prove the seam's `Store` variant carries a genuine backend failure (not
/// just a conflict) through to `AppError`.
#[derive(Clone)]
struct FlakyBackend {
    inner:        MockBackend,
    fail_appends: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl FlakyBackend {
    fn new() -> Self {
        Self {
            inner:        MockBackend::new(),
            fail_appends: std::sync::Arc::new(
                std::sync::atomic::AtomicBool::new(false),
            ),
        }
    }

    fn start_failing(&self) {
        self.fail_appends.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Backend for FlakyBackend {
    type Error = DiskError;

    async fn head(&self, stream_id: &str) -> Result<Version, Self::Error> {
        // `MockBackend::Error` is `Infallible`.
        let Ok(v) = self.inner.head(stream_id).await;
        Ok(v)
    }

    async fn read_stream(
        &self,
        stream_id: &str,
        after: Version,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        let Ok(page) = self.inner.read_stream(stream_id, after, limit).await;
        Ok(page)
    }

    async fn read_global(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        let Ok(page) = self.inner.read_global(after, limit).await;
        Ok(page)
    }

    async fn append_batch(
        &self,
        stream_id: &str,
        expected: Version,
        records: &[RecordToAppend],
    ) -> Result<Appended, AppendError<Self::Error>> {
        if self.fail_appends.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(AppendError::Backend(DiskError {
                io: std::io::Error::other("no space left on device"),
            }));
        }
        match self.inner.append_batch(stream_id, expected, records).await {
            Ok(a) => Ok(a),
            Err(AppendError::Conflict { expected, actual }) => {
                Err(AppendError::Conflict { expected, actual })
            }
            Err(AppendError::Backend(inf)) => match inf {},
        }
    }
}

#[tokio::test]
async fn seam_collapses_two_aggregates_into_one_error() {
    let backend = FlakyBackend::new();
    let store = EventStore::new(backend.clone());

    // Domain rejection: erased seam still carries the typed `CounterError`.
    let err = store
        .command::<Counter, _>("counter-1", Increment { by: -1 })
        .await
        .unwrap_err();
    match counter_err(err) {
        AppError::Counter(CounterError(msg)) => {
            assert!(msg.contains("would take"), "unexpected message: {msg}");
        }
        other => panic!("expected AppError::Counter, got {other:?}"),
    }

    // A real command still round-trips.
    store
        .command::<Counter, _>("counter-1", Increment { by: 5 })
        .await
        .unwrap();

    // Store failure: the backend genuinely fails, and the boxed error's
    // Display + source chain both survive to the seam.
    backend.start_failing();
    let err = store
        .command::<Counter, _>("counter-1", Increment { by: 1 })
        .await
        .unwrap_err();
    match counter_err(err) {
        AppError::Store(boxed) => {
            // `boxed` erases `mess_store::StoreError<DiskError>`; its Display
            // wraps `DiskError`'s.
            assert!(boxed.to_string().contains("disk write failed"));
            // One `source()` hop unwraps `StoreError::Backend` to our
            // `DiskError` ...
            let disk_err = std::error::Error::source(&boxed)
                .expect("StoreError::Backend keeps its DiskError source");
            assert!(disk_err.to_string().contains("disk write failed"));
            // ... and one more reaches the original `io::Error` underneath
            // it — the chain a `String`-collapsed seam would have thrown
            // away entirely.
            let io_err = std::error::Error::source(disk_err)
                .expect("DiskError keeps its io::Error source");
            assert!(io_err.to_string().contains("no space left on device"));
        }
        other => panic!("expected AppError::Store, got {other:?}"),
    }
}
