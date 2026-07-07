//! `EventStore`: a thin DX layer over `mess_db`'s tokio actor
//! (`ActorHandle`), providing load / append / command with bounded
//! optimistic retry.

use std::borrow::Cow;
use std::fmt;
use std::path::Path;

use ident::Id;
use mess_db::{
    error::Error as DbError, read::GetMessages, rocks::db::DB,
    svc::ActorHandle, write::WriteMessage, StreamPos,
};

use crate::{Aggregate, CodecError, Decide, Event};

/// A stream's version, in mess_db's convention: the `StreamPos` of the
/// **last** message in the stream. `NoStream` means the stream has no
/// messages (mess_db: `expected_stream_position: None`).
///
/// Note: mess_db has no "Any" — every append MUST carry the exact current
/// version. There is no unconditional append. (See REPORT.md.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Version {
    NoStream,
    At(u64),
}

impl Version {
    fn to_expected(self) -> Option<StreamPos> {
        match self {
            Version::NoStream => None,
            Version::At(v) => Some(StreamPos::Sequential(v)),
        }
    }
}

/// Result of replaying a stream through [`Aggregate::apply`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Loaded<A> {
    pub state: A,
    /// Version the stream was at when loaded; feed this to `append` as the
    /// expected version.
    pub version: Version,
    pub events_replayed: usize,
}

/// Result of a successful append/command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Commit {
    /// Stream version after the commit.
    pub version: Version,
    /// Global position of the last appended message (None if nothing was
    /// appended, e.g. the command decided zero events).
    pub last_global_position: Option<u64>,
    pub events_appended: usize,
    /// How many optimistic attempts `command` needed (always 1 for a direct
    /// `append`).
    pub attempts: u32,
}

/// Infrastructure-level store failure.
#[derive(Debug)]
pub enum StoreError {
    Db(DbError),
    Codec(CodecError),
    /// mess_db surfaces per-message read errors; we fail the whole load.
    CorruptMessage(String),
}

impl StoreError {
    /// Is this an optimistic-concurrency conflict (someone else wrote to the
    /// stream between our load and our append)?
    pub fn is_version_conflict(&self) -> bool {
        matches!(self, StoreError::Db(DbError::WrongStreamPosition { .. }))
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Db(e) => write!(f, "database error: {e}"),
            StoreError::Codec(e) => write!(f, "codec error: {e}"),
            StoreError::CorruptMessage(e) => {
                write!(f, "corrupt message in stream: {e}")
            }
        }
    }
}

impl std::error::Error for StoreError {}

impl From<DbError> for StoreError {
    fn from(e: DbError) -> Self {
        StoreError::Db(e)
    }
}

impl From<CodecError> for StoreError {
    fn from(e: CodecError) -> Self {
        StoreError::Codec(e)
    }
}

/// Failure of a `command` round.
#[derive(Debug)]
pub enum CommandError {
    /// The aggregate rejected the command (business rule).
    Domain(crate::DomainError),
    /// Optimistic retry budget exhausted: the stream kept moving under us.
    Conflict { stream: String, attempts: u32 },
    Store(StoreError),
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandError::Domain(e) => e.fmt(f),
            CommandError::Conflict { stream, attempts } => write!(
                f,
                "gave up after {attempts} optimistic attempts on stream \
                 {stream:?}: concurrent writers kept changing the stream"
            ),
            CommandError::Store(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for CommandError {}

impl From<StoreError> for CommandError {
    fn from(e: StoreError) -> Self {
        CommandError::Store(e)
    }
}

pub const DEFAULT_MAX_ATTEMPTS: u32 = 16;

/// Event store facade over mess_db's write/read actor.
#[derive(Clone)]
pub struct EventStore {
    actor: ActorHandle,
    max_attempts: u32,
}

impl EventStore {
    /// Open (or create) a RocksDB-backed store at `path` and spawn its
    /// actor. Must be called from within a tokio runtime (the actor is a
    /// spawned task).
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let db = DB::new(path)?;
        Ok(Self::new(ActorHandle::new(db)))
    }

    /// Wrap an existing actor handle.
    pub fn new(actor: ActorHandle) -> Self {
        Self { actor, max_attempts: DEFAULT_MAX_ATTEMPTS }
    }

    /// Set the optimistic-retry budget for `command` (min 1).
    pub fn with_max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts.max(1);
        self
    }

    /// Load an aggregate by replaying its stream through `apply`.
    pub async fn load<A: Aggregate>(
        &self,
        stream_id: &str,
    ) -> Result<Loaded<A>, StoreError> {
        // NOTE: the actor caps reads at LIMIT_MAX (10_000) and ignores the
        // start-position option for stream reads, so streams longer than
        // 10k events cannot be fully replayed. Fine for a spike; a real
        // wart for the backend (see REPORT.md).
        let req = GetMessages::default()
            .in_stream(stream_id)
            .with_limit(mess_db::read::LIMIT_MAX);
        let messages = self.actor.fetch_messages(req).await?;

        let mut state = A::default();
        let mut version = Version::NoStream;
        let mut events_replayed = 0;
        for res in messages {
            let msg =
                res.map_err(|e| StoreError::CorruptMessage(e.to_string()))?;
            let event =
                <A::Event as Event>::decode(&msg.message_type, &msg.data)?;
            state.apply(&event);
            version = Version::At(msg.stream_position.position());
            events_replayed += 1;
        }
        Ok(Loaded { state, version, events_replayed })
    }

    /// Append events to a stream at an exact expected version.
    ///
    /// WARNING (backend limitation): mess_db's actor writes ONE message per
    /// request, so a multi-event append is a sequence of single-message
    /// writes chained by expected version. It is not atomic: a concurrent
    /// writer (or crash) between two messages leaves a partial append and
    /// this function returns a version conflict mid-batch. See REPORT.md.
    pub async fn append<E: Event>(
        &self,
        stream_id: &str,
        expected: Version,
        events: &[E],
    ) -> Result<Commit, StoreError> {
        let mut expected = expected;
        let mut last_global = None;
        for event in events {
            let msg = WriteMessage {
                id: Id::new(),
                stream_name: Cow::Borrowed(stream_id),
                message_type: Cow::Borrowed(event.name()),
                data: Cow::Owned(event.encode()?),
                metadata: Cow::Borrowed(&[]),
                expected_stream_position: expected.to_expected(),
            };
            let pos = self.actor.put_message(msg).await?;
            expected = Version::At(pos.stream.position());
            last_global = Some(pos.global);
        }
        Ok(Commit {
            version: expected,
            last_global_position: last_global,
            events_appended: events.len(),
            attempts: 1,
        })
    }

    /// The north-star call: load -> decide -> append-with-expected-version,
    /// with bounded optimistic retry on version conflicts.
    ///
    /// `C: Clone` because `decide` consumes the command and we may need it
    /// again on retry. (A derive layer would likely switch `decide` to take
    /// `&C` and drop this bound.)
    pub async fn command<A, C>(
        &self,
        stream_id: &str,
        cmd: C,
    ) -> Result<Commit, CommandError>
    where
        A: Aggregate + Decide<C>,
        C: Clone,
    {
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let loaded = self.load::<A>(stream_id).await?;
            let events = loaded
                .state
                .decide(cmd.clone())
                .map_err(CommandError::Domain)?;
            if events.is_empty() {
                return Ok(Commit {
                    version: loaded.version,
                    last_global_position: None,
                    events_appended: 0,
                    attempts: attempt,
                });
            }
            match self.append(stream_id, loaded.version, &events).await {
                Ok(commit) => return Ok(Commit { attempts: attempt, ..commit }),
                Err(err) if err.is_version_conflict() => {
                    if attempt >= self.max_attempts {
                        return Err(CommandError::Conflict {
                            stream: stream_id.to_string(),
                            attempts: attempt,
                        });
                    }
                    // Someone else won the race; reload and re-decide.
                    continue;
                }
                Err(err) => return Err(err.into()),
            }
        }
    }
}
