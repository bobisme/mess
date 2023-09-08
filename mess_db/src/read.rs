use std::borrow::Cow;

use crate::StreamPos;

pub const LIMIT_MAX: usize = 10_000;
pub const LIMIT_DEFAULT: usize = 1_000;

/// # Default
///
/// ```
/// use mess_db::read::ReadMessages;
/// let read_messages = ReadMessages::default();
///
/// assert_eq!(read_messages.global_position(), 0);
/// assert_eq!(read_messages.stream_name(), None);
/// assert_eq!(read_messages.limit(), 1000);
/// ```
///
/// ```
/// use mess_db::read::ReadMessages;
/// let read_messages = ReadMessages::default()
///     .from_global_position(200)
///     .from_stream("some_stream_name")
///     .with_limit(100);
///
/// assert_eq!(read_messages.global_position(), 200);
/// assert!(matches!(read_messages.stream_name(), Some(_)));
/// assert_eq!(read_messages.stream_name().unwrap(), "some_stream_name");
/// assert_eq!(read_messages.limit(), 100);
/// ```
// type states for GetMessages options
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct Unset;
#[derive(Debug, Clone, PartialEq)]
pub struct OptStream<'a>(pub(crate) Cow<'a, str>);
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct OptGlobalPos(pub(crate) u64);
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OptStreamPos(pub(crate) StreamPos);

#[derive(Clone, PartialEq, PartialOrd)]
pub struct GetMessages<Strm, G, S> {
    pub(crate) start_global_position: G,
    pub(crate) start_stream_position: S,
    pub(crate) limit: usize,
    pub(crate) stream: Strm,
}

impl<P, G, S> GetMessages<P, G, S> {
    pub const fn with_limit(mut self, limit: usize) -> Self {
        self.limit = match limit {
            x if x < 1 => 1,
            x if x > LIMIT_MAX => LIMIT_MAX,
            _ => limit,
        };
        self
    }
}

impl<P, G, S> GetMessages<P, G, S> {
    #[allow(clippy::missing_const_for_fn)]
    pub fn from_global(self, position: u64) -> GetMessages<P, OptGlobalPos, S> {
        GetMessages {
            start_global_position: OptGlobalPos(position),
            start_stream_position: self.start_stream_position,
            limit: self.limit,
            stream: self.stream,
        }
    }
}

impl<P, G, S> GetMessages<P, G, S> {
    pub fn in_stream(self, name: &str) -> GetMessages<OptStream, G, S> {
        let mut name = name.to_string();
        GetMessages {
            start_global_position: self.start_global_position,
            start_stream_position: self.start_stream_position,
            limit: self.limit,
            stream: OptStream(name.into()),
        }
    }
}

impl Default for GetMessages<Unset, Unset, Unset> {
    fn default() -> Self {
        Self {
            start_global_position: Default::default(),
            start_stream_position: Default::default(),
            limit: LIMIT_DEFAULT,
            stream: Default::default(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::*;

    mod test_get_messages {
        use super::*;

        mod from_stream {
            use super::*;
            use pretty_assertions::assert_eq;

            #[rstest]
            async fn default_is_none() {
                let get = GetMessages::default();
                assert_eq!(get.stream, Unset);
            }

            #[rstest]
            async fn sets_some_stream() {
                let get = GetMessages::default().in_stream("a-stream");
                assert_eq!(get.stream, OptStream("a-stream".into()));
            }
        }

        mod from_global_position {
            use super::*;
            use pretty_assertions::assert_eq;

            #[rstest]
            async fn default_is_zero() {
                let get = GetMessages::default();
                assert_eq!(get.start_global_position, Unset);
            }

            #[rstest]
            async fn it_sets_given_position() {
                let get = GetMessages::default().from_global(42);
                assert_eq!(get.start_global_position, OptGlobalPos(42));
            }
        }

        mod with_limit {
            use super::*;
            use pretty_assertions::assert_eq;

            #[rstest]
            async fn it_sets_the_given_limit() {
                let get = GetMessages::default().with_limit(500);
                assert_eq!(get.limit, 500);
            }

            #[rstest]
            async fn min_limit_is_1() {
                let get = GetMessages::default().with_limit(0);
                assert_eq!(get.limit, 1);
            }

            #[rstest]
            async fn max_limit_is_limit_max() {
                let get = GetMessages::default().with_limit(usize::MAX);
                assert_eq!(get.limit, LIMIT_MAX);
            }
        }
    }
}
