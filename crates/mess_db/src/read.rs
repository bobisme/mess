use std::borrow::Cow;

use crate::StreamPos;

pub const LIMIT_MAX: usize = 10_000;
pub const LIMIT_DEFAULT: usize = 1_000;

/// Typestate builder for read requests:
///
/// ```
/// use mess_db::StreamPos;
/// use mess_db::read::GetMessages;
///
/// let global = GetMessages::default().from_global(200).with_limit(100);
/// let stream = GetMessages::default()
///     .in_stream("some_stream_name")
///     .from_stream_position(StreamPos::new(3));
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
pub struct GetMessages<Strm, Gpos, Spos> {
    pub(crate) start_global_position: Gpos,
    pub(crate) start_stream_position: Spos,
    pub(crate) limit:                 usize,
    pub(crate) stream:                Strm,
}

impl<Strm, Gpos, Spos> GetMessages<Strm, Gpos, Spos> {
    pub const fn with_limit(mut self, limit: usize) -> Self {
        self.limit = match limit {
            x if x < 1 => 1,
            x if x > LIMIT_MAX => LIMIT_MAX,
            _ => limit,
        };
        self
    }
}

impl<Strm, Gpos, Spos> GetMessages<Strm, Gpos, Spos> {
    #[allow(clippy::missing_const_for_fn)]
    pub fn from_global(
        self,
        position: u64,
    ) -> GetMessages<Strm, OptGlobalPos, Spos> {
        GetMessages {
            start_global_position: OptGlobalPos(position),
            start_stream_position: self.start_stream_position,
            limit:                 self.limit,
            stream:                self.stream,
        }
    }
}

impl<Strm, Gpos, Spos> GetMessages<Strm, Gpos, Spos> {
    #[allow(clippy::missing_const_for_fn)]
    pub fn from_stream_position(
        self,
        position: StreamPos,
    ) -> GetMessages<Strm, Gpos, OptStreamPos> {
        GetMessages {
            start_global_position: self.start_global_position,
            start_stream_position: OptStreamPos(position),
            limit:                 self.limit,
            stream:                self.stream,
        }
    }
}

impl<Strm, Gpos, Spos> GetMessages<Strm, Gpos, Spos> {
    pub fn in_stream(
        self,
        name: &str,
    ) -> GetMessages<OptStream<'_>, Gpos, Spos> {
        let name = name.to_string();
        GetMessages {
            start_global_position: self.start_global_position,
            start_stream_position: self.start_stream_position,
            limit:                 self.limit,
            stream:                OptStream(name.into()),
        }
    }
}

impl Default for GetMessages<Unset, Unset, Unset> {
    fn default() -> Self {
        Self {
            start_global_position: Default::default(),
            start_stream_position: Default::default(),
            limit:                 LIMIT_DEFAULT,
            stream:                Default::default(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    mod test_get_messages {
        use super::*;

        mod from_stream {
            use pretty_assertions::assert_eq;

            use super::*;

            #[tokio::test]
            async fn default_is_none() {
                let get = GetMessages::default();
                assert_eq!(get.stream, Unset);
            }

            #[tokio::test]
            async fn sets_some_stream() {
                let get = GetMessages::default().in_stream("a-stream");
                assert_eq!(get.stream, OptStream("a-stream".into()));
            }
        }

        mod from_global_position {
            use pretty_assertions::assert_eq;

            use super::*;

            #[tokio::test]
            async fn default_is_zero() {
                let get = GetMessages::default();
                assert_eq!(get.start_global_position, Unset);
            }

            #[tokio::test]
            async fn it_sets_given_position() {
                let get = GetMessages::default().from_global(42);
                assert_eq!(get.start_global_position, OptGlobalPos(42));
            }
        }

        mod with_limit {
            use pretty_assertions::assert_eq;

            use super::*;

            #[tokio::test]
            async fn it_sets_the_given_limit() {
                let get = GetMessages::default().with_limit(500);
                assert_eq!(get.limit, 500);
            }

            #[tokio::test]
            async fn min_limit_is_1() {
                let get = GetMessages::default().with_limit(0);
                assert_eq!(get.limit, 1);
            }

            #[tokio::test]
            async fn max_limit_is_limit_max() {
                let get = GetMessages::default().with_limit(usize::MAX);
                assert_eq!(get.limit, LIMIT_MAX);
            }
        }
    }
}
