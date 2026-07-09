//! [`Version`]: a stream's optimistic-concurrency token.

/// A stream's version: the position of the **last** event in the stream.
///
/// [`NoStream`](Version::NoStream) means the stream has no events yet. This is
/// the exact-version convention proven in `spikes/dx_api` — every append
/// carries the precise current version; there is no "append anywhere"
/// (unconditional) mode. A backend uses it two ways:
///
/// - as the **expected version** on an append (the write is rejected with a
///   conflict if the stream has moved past it), and
/// - as an exclusive **read cursor** for paged replay: `read_stream(stream,
///   after)` returns events strictly after `after`, so `NoStream` reads from
///   the very start and `At(n)` resumes after position `n`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Version {
    /// The stream has no events.
    NoStream,
    /// The last event sits at this 0-based stream position.
    At(u64),
}

impl Version {
    /// The next 0-based position an appended event would occupy.
    #[must_use]
    pub fn next_position(self) -> u64 {
        match self {
            Version::NoStream => 0,
            Version::At(n) => n + 1,
        }
    }

    /// The position value, if the stream has any events.
    #[must_use]
    pub fn position(self) -> Option<u64> {
        match self {
            Version::NoStream => None,
            Version::At(n) => Some(n),
        }
    }
}
