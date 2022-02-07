/// Traits for the db.
use crate::msg::Message;

pub trait WriteMessage {
    /// Returns next topic position.
    fn write_message(
        msg: impl Message,
        stream_name: &str,
        expected_topic_position: Option<u64>,
    ) -> u64;
}

pub struct ReadTopicMessages<'a, Reader> {
    reader: &Reader,
    pub topic_name: &'a str,
    pub topic_position: u64,
    pub batch_size: u64,
    pub condition: Option<&'a str>,
}

impl<'a> ReadTopicMessages<'a> {
    pub fn new(reader: &Reader, topic_name: &'a str) -> Self {
        Self {
            reader,
            topic_name,
            topic_position: 0,
            batch_size: 1000,
            condition: None,
        }
    }

    pub fn go(&self) -> Vec<impl Message> {
        self.reader._read_topic_messages(
            self.topic_name,
            self.topic_position,
            self.batch_size,
            self.condition,
        )
    }
}

pub trait ReadMessages {
    /// Returns next topic position.
    /// CREATE OR REPLACE FUNCTION message_store.get_stream_messages(
    //   stream_name varchar,
    //   "position" bigint DEFAULT 0,
    //   batch_size bigint DEFAULT 1000,
    //   condition varchar DEFAULT NULL
    // )
    fn _read_topic_messages(
        &mut self,
        topic_name: &str,
        expected_topic_position: u64,
        batch_size: u64,
        condition: Option<&str>,
    ) -> Vec<impl Message>;

    fn read_topic_messages<'a>(
        topic_name: &str,
    ) -> ReadTopicMessages<'a, Self> {
        ReadTopicMessages::new(reader, topic_name)
    }
}
