#![warn(clippy::missing_const_for_fn, clippy::must_use_candidate)]

use std::borrow::Cow;

pub mod error;
pub mod read;
pub mod rocks;
pub mod svc;
pub mod write;

/// A position within a single stream (a stream version), stored as a plain
/// `u64`.
///
/// Historically this was an `enum { Sequential, Relaxed }` that packed an
/// ordering-mode discriminant into the low bit of the stored integer. That
/// bit-flag encoding was retired (see `docs/adr/0001-mess-db-dispositions.md`):
/// a v1 stream version is a plain `u64`, and relaxed/causal ordering returns
/// post-v1 via doc 06's algebraic modes rather than an in-band encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamPos(pub u64);

impl StreamPos {
    #[must_use]
    pub const fn new(position: u64) -> Self {
        Self(position)
    }

    /// Encode to the `u64` stored on disk. Identity now that the
    /// Sequential/Relaxed bit-flag has been retired; kept as the explicit
    /// DB-serialization boundary.
    #[must_use]
    pub const fn encode(self) -> u64 {
        self.0
    }

    /// Decode from the `u64` stored on disk. Inverse of [`Self::encode`].
    #[must_use]
    pub const fn decode(stored_position: u64) -> Self {
        Self(stored_position)
    }

    /// Returns the position as a `u64`.
    #[must_use]
    pub const fn position(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

// Compile-time test cases for StreamPos
const _: () = {
    qed::const_assert!(StreamPos::new(0b111).encode() == 0b111);
    qed::const_assert_matches!(StreamPos::decode(0b111), StreamPos(0b111));
    qed::const_assert!(StreamPos::new(0b111).position() == 0b111);
    qed::const_assert!(StreamPos::new(0b111).next().position() == 0b1000);
};

/// Optimistic-concurrency precondition for an append.
///
/// Replaces the old `Option<StreamPos>` convention (`None` = "stream must be
/// empty", `Some(v)` = "stream head is exactly `v`"), which had no way to
/// express an unconditional append and forced a read-before-write on every
/// call. See dx_api friction #3.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpectedVersion {
    /// The stream must not exist yet (no records). Equivalent to the old
    /// `expected_stream_position: None`.
    NoStream,
    /// The stream head must be exactly this position. Equivalent to the old
    /// `Some(v)`.
    Exact(StreamPos),
    /// Append unconditionally: skip the head read entirely and take whatever
    /// the current head is. No `WrongStreamPosition` is ever returned.
    Any,
}

impl From<Option<StreamPos>> for ExpectedVersion {
    /// Bridges the retired `Option<StreamPos>` convention: `None` becomes
    /// [`ExpectedVersion::NoStream`], `Some(v)` becomes
    /// [`ExpectedVersion::Exact`].
    fn from(opt: Option<StreamPos>) -> Self {
        match opt {
            None => Self::NoStream,
            Some(v) => Self::Exact(v),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Position {
    pub global: u64,
    pub stream: StreamPos,
}

impl Position {
    #[must_use]
    pub const fn new(global: u64, stream: StreamPos) -> Self {
        Self { global, stream }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message<'a> {
    pub global_position: u64,
    pub stream_position: StreamPos,
    // time_ms: u64,
    pub stream_name: Cow<'a, str>,
    pub message_type: Cow<'a, str>,
    pub data: Cow<'a, [u8]>,
    pub metadata: Option<Cow<'a, [u8]>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedMessage {
    pub global_position: u64,
    pub stream_position: StreamPos,
    // time_ms: u64,
    pub stream_name: String,
    pub message_type: String,
    pub data: Vec<u8>,
    pub metadata: Option<Vec<u8>>,
}

impl From<Message<'_>> for OwnedMessage {
    fn from(msg: Message<'_>) -> Self {
        OwnedMessage {
            stream_name: msg.stream_name.to_string(),
            message_type: msg.message_type.to_string(),
            data: msg.data.to_vec(),
            metadata: msg.metadata.as_ref().map(|x| x.to_vec()),
            global_position: msg.global_position,
            stream_position: msg.stream_position,
        }
    }
}

impl From<OwnedMessage> for Message<'_> {
    fn from(msg: OwnedMessage) -> Self {
        Message {
            global_position: msg.global_position,
            stream_position: msg.stream_position,
            stream_name: msg.stream_name.into(),
            message_type: msg.message_type.into(),
            data: msg.data.into(),
            metadata: msg.metadata.map(|x| x.into()),
        }
    }
}
