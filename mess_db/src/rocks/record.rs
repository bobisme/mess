use std::borrow::Cow;

use crate::{
    error::{Error, MessResult},
    write::{WriteMessage, WriteSerialMessage},
    Message, StreamPos,
};
use rkyv::{Archive, Deserialize, Serialize};

pub(crate) const SEPARATOR: u8 = b'|';

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
// Derives can be passed through to the generated type:
#[archive_attr(derive(Debug))]
pub(crate) struct GlobalRecord {
    pub(crate) id: String,
    pub(crate) stream_name: String,
    pub(crate) stream_position: u64,
    pub(crate) message_type: String,
    pub(crate) data: Vec<u8>,
    pub(crate) metadata: Vec<u8>,
    pub(crate) ord: u64,
}

impl GlobalRecord {
    pub(crate) fn from_write_message_partial<D, M>(
        msg: &WriteMessage<'_, D, M>,
    ) -> MessResult<Self>
    where
        D: serde::Serialize,
        M: serde::Serialize,
    {
        let stream_position =
            msg.expected_stream_position.map(|x| x + 1).unwrap_or(0);
        Ok(Self {
            id: msg.id.to_string(),
            stream_name: msg.stream_name.as_ref().into(),
            stream_position,
            message_type: msg.message_type.as_ref().into(),
            data: Default::default(),
            metadata: Default::default(),
            ord: 0,
        })
    }

    pub(crate) fn from_write_serial_message(
        msg: &WriteSerialMessage,
    ) -> MessResult<Self> {
        let stream_position = msg
            .expected_position
            .map(|x| x.next())
            .unwrap_or(StreamPos::Serial(0))
            .to_store();
        Ok(Self {
            id: msg.id.to_string(),
            stream_name: msg.stream_name.as_ref().into(),
            stream_position,
            message_type: msg.message_type.as_ref().into(),
            data: msg.data.to_vec(),
            metadata: msg.metadata.to_vec(),
            ord: 0,
        })
    }

    pub(crate) fn from_bytes(
        bytes: &[u8],
    ) -> MessResult<&ArchivedGlobalRecord> {
        rkyv::check_archived_root::<Self>(&bytes[..])
            .map_err(|e| Error::DeserError(e.to_string()))
    }
}

impl ArchivedGlobalRecord {
    pub(crate) fn to_message(&self, global_position: u64) -> Message {
        Message {
            global_position,
            stream_position: StreamPos::from_store(
                self.stream_position.value(),
            ),
            stream_name: self.stream_name.to_owned(),
            message_type: self.message_type.to_owned(),
            data: self.data.to_owned(),
            metadata: if self.metadata.is_empty() {
                None
            } else {
                Some(self.metadata.to_owned())
            },
        }
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
// Derives can be passed through to the generated type:
#[archive_attr(derive(Debug))]
pub(crate) struct StreamRecord {
    pub(crate) global_position: u64,
    pub(crate) id: String,
    pub(crate) message_type: String,
    pub(crate) data: Vec<u8>,
    pub(crate) metadata: Vec<u8>,
    pub(crate) ord: u64,
}

impl StreamRecord {
    pub(crate) fn set_global_position(mut self, pos: u64) -> Self {
        self.global_position = pos;
        self
    }

    pub(crate) fn from_write_message_partial<D, M>(
        msg: &WriteMessage<'_, D, M>,
    ) -> MessResult<Self>
    where
        D: serde::Serialize,
        M: serde::Serialize,
    {
        Ok(Self {
            id: msg.id.to_string(),
            global_position: Default::default(),
            message_type: msg.message_type.as_ref().into(),
            data: Default::default(),
            metadata: Default::default(),
            ord: 0,
        })
    }

    pub(crate) fn from_write_serial_message(
        msg: &WriteSerialMessage,
        global_position: u64,
    ) -> MessResult<Self> {
        Ok(Self {
            id: msg.id.to_string(),
            global_position,
            message_type: msg.message_type.as_ref().into(),
            data: msg.data.to_vec(),
            metadata: msg.metadata.to_vec(),
            ord: 0,
        })
    }

    pub(crate) fn from_bytes(
        bytes: &[u8],
    ) -> MessResult<&ArchivedStreamRecord> {
        rkyv::check_archived_root::<Self>(&bytes[..])
            .map_err(|e| Error::DeserError(e.to_string()))
    }
}

impl ArchivedStreamRecord {
    pub(crate) fn to_message(
        &self,
        stream: impl Into<String>,
        stream_position: StreamPos,
    ) -> Message {
        Message {
            global_position: self.global_position.value(),
            stream_position,
            stream_name: stream.into(),
            message_type: self.message_type.to_owned(),
            data: self.data.to_owned(),
            metadata: if self.metadata.is_empty() {
                None
            } else {
                Some(self.metadata.to_owned())
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GlobalKey(pub(crate) u64);

impl GlobalKey {
    pub fn new(position: u64) -> Self {
        GlobalKey(position)
    }

    pub fn as_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub fn from_bytes(bytes: &[u8]) -> MessResult<Self> {
        let position = u64::from_be_bytes(
            bytes.try_into().map_err(|_| Error::ParseKeyError)?,
        );
        Ok(GlobalKey(position))
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamKey<'a> {
    pub(crate) stream: Cow<'a, str>,
    pub(crate) position: StreamPos,
}

impl<'a> StreamKey<'a> {
    pub fn new(stream: impl Into<Cow<'a, str>>, position: StreamPos) -> Self {
        Self { stream: stream.into(), position }
    }

    pub fn max(stream: impl Into<Cow<'a, str>>) -> Self {
        Self { stream: stream.into(), position: StreamPos::Causal(u64::MAX) }
    }

    pub fn next(&self) -> Self {
        Self { stream: self.stream.clone(), position: self.position.next() }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = self.stream.as_bytes().to_vec();
        bytes.push(SEPARATOR);
        bytes.extend_from_slice(&self.position.to_store().to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> MessResult<Self> {
        let (stream, sep_position) = bytes.split_at(bytes.len() - 9);
        if sep_position.len() != 9 || stream.is_empty() {
            return Err(Error::ParseKeyError);
        }
        if sep_position[0] != SEPARATOR {
            return Err(Error::ParseKeyError);
        }
        let position = &sep_position[1..];
        let position = u64::from_be_bytes(
            position.try_into().map_err(|_| Error::ParseKeyError)?,
        );
        Ok(StreamKey {
            stream: String::from_utf8(stream.to_vec())
                .map_err(|_| Error::ParseKeyError)?
                .into(),
            position: StreamPos::from_store(position),
        })
    }
}
