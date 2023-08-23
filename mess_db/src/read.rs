// /// Traits for the db.
// use crate::msg::Message;

// pub trait WriteMessage {
//     /// Returns next topic position.
//     fn write_message(
//         msg: impl Message,
//         stream_name: &str,
//         expected_topic_position: Option<u64>,
//     ) -> u64;
// }

// pub struct ReadTopicMessages<'a, Reader> {
//     reader: &Reader,
//     pub topic_name: &'a str,
//     pub topic_position: u64,
//     pub batch_size: u64,
//     pub condition: Option<&'a str>,
// }

// impl<'a> ReadTopicMessages<'a> {
//     pub fn new(reader: &Reader, topic_name: &'a str) -> Self {
//         Self {
//             reader,
//             topic_name,
//             topic_position: 0,
//             batch_size: 1000,
//             condition: None,
//         }
//     }

//     pub fn go(&self) -> Vec<impl Message> {
//         self.reader._read_topic_messages(
//             self.topic_name,
//             self.topic_position,
//             self.batch_size,
//             self.condition,
//         )
//     }
// }

use std::borrow::Cow;

pub const LIMIT_MAX: u32 = 10_000;

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
#[derive(Clone, Debug)]
pub struct ReadMessages<'a> {
    pub(crate) global_position: u64,
    pub(crate) stream_name: Option<Cow<'a, str>>,
    pub(crate) limit: u32,
}

impl<'a> ReadMessages<'a> {
    pub fn from_global_position(mut self, position: u64) -> Self {
        self.global_position = position;
        self
    }

    pub fn from_stream(mut self, name: impl Into<Cow<'a, str>>) -> Self {
        self.stream_name = Some(name.into());
        self
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = limit.clamp(1, LIMIT_MAX);
        self
    }

    pub fn global_position(&self) -> u64 {
        self.global_position
    }

    pub fn stream_name(&self) -> Option<&Cow<'_, str>> {
        self.stream_name.as_ref()
    }

    pub fn limit(&self) -> u32 {
        self.limit
    }
}

impl<'a> Default for ReadMessages<'a> {
    fn default() -> Self {
        Self { global_position: 0, stream_name: None, limit: 1_000 }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::*;

    mod readmessages {
        use super::*;

        mod from_stream {
            use super::*;
            use pretty_assertions::assert_eq;

            #[rstest]
            async fn default_is_none() {
                let rm = ReadMessages::default();
                assert_eq!(rm.stream_name, None);
            }

            #[rstest]
            async fn sets_some_stream_name() {
                let rm = ReadMessages::default().from_stream("a-stream");
                assert_eq!(rm.stream_name, Some(Cow::Borrowed("a-stream")));
            }
        }

        mod from_global_position {
            use super::*;
            use pretty_assertions::assert_eq;

            #[rstest]
            async fn default_is_zero() {
                let rm = ReadMessages::default();
                assert_eq!(rm.global_position, 0);
            }

            #[rstest]
            async fn it_sets_given_position() {
                let rm = ReadMessages::default().from_global_position(42);
                assert_eq!(rm.global_position, 42);
            }
        }

        mod with_limit {
            use super::*;
            use pretty_assertions::assert_eq;

            #[rstest]
            async fn it_sets_the_given_limit() {
                let rm = ReadMessages::default().with_limit(500);
                assert_eq!(rm.limit, 500);
            }

            #[rstest]
            async fn min_limit_is_1() {
                let rm = ReadMessages::default().with_limit(0);
                assert_eq!(rm.limit, 1);
            }

            #[rstest]
            async fn max_limit_is_limit_max() {
                let rm = ReadMessages::default().with_limit(u32::MAX);
                assert_eq!(rm.limit, LIMIT_MAX);
            }
        }
    }
}
