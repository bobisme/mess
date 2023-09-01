use std::borrow::Cow;

use super::keys::StreamKey;
use crate::{
    error::{Error, MessResult},
    write::{WriteMessage, WriteSerialMessage},
    Message, StreamPos,
};

#[derive(
    Clone,
    Debug,
    PartialEq, // rkyv::Archive, rkyv::Deserialize, rkyv::Serialize,
    serde::Serialize,
    serde::Deserialize,
)]
// #[archive(compare(PartialEq), check_bytes)]
// #[archive_attr(derive(Debug))]
pub struct GlobalRecord<'a> {
    pub(crate) id: Cow<'a, str>,
    pub(crate) stream_name: Cow<'a, str>,
    pub(crate) stream_position: u64,
    pub(crate) message_type: Cow<'a, str>,
    pub(crate) data: Cow<'a, [u8]>,
    pub(crate) metadata: Cow<'a, [u8]>,
    pub(crate) ord: u64,
}

impl<'a> GlobalRecord<'a> {
    pub(crate) fn from_write_serial_message(
        msg: &'a WriteSerialMessage,
    ) -> MessResult<Self> {
        let stream_position = msg
            .expected_position
            .map(|x| x.next())
            .unwrap_or(StreamPos::Serial(0))
            .encode();
        Ok(Self {
            id: msg.id.to_string().into(),
            stream_name: msg.stream_name.as_ref().into(),
            stream_position,
            message_type: msg.message_type.as_ref().into(),
            data: msg.data.as_ref().into(),
            metadata: msg.metadata.as_ref().into(),
            ord: 0,
        })
    }

    pub(crate) fn from_bytes<B: AsRef<[u8]>>(bytes: B) -> MessResult<Self> {
        postcard::from_bytes(bytes.as_ref())
            .map_err(|e| Error::DeserError(e.to_string()))
    }

    pub(crate) fn to_message(&'a self, global_position: u64) -> Message<'a> {
        Message {
            global_position,
            stream_position: StreamPos::decode(self.stream_position),
            stream_name: self.stream_name.clone(),
            message_type: self.message_type.clone(),
            data: self.data.clone().to_owned(),
            metadata: if self.metadata.is_empty() {
                None
            } else {
                Some(self.metadata.clone().to_owned())
            },
        }
    }

    pub(crate) fn into_message(self, global_position: u64) -> Message<'a> {
        Message {
            global_position,
            stream_position: StreamPos::decode(self.stream_position),
            stream_name: self.stream_name.into(),
            message_type: self.message_type.into(),
            data: self.data.into(),
            metadata: if self.metadata.is_empty() {
                None
            } else {
                Some(self.metadata.into())
            },
        }
    }
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
// #[archive(compare(PartialEq), check_bytes)]
// Derives can be passed through to the generated type:
// #[archive_attr(derive(Debug))]
pub struct StreamRecord<'a> {
    pub(crate) global_position: u64,
    pub(crate) id: Cow<'a, str>,
    pub(crate) message_type: Cow<'a, str>,
    pub(crate) data: Cow<'a, [u8]>,
    pub(crate) metadata: Cow<'a, [u8]>,
    pub(crate) ord: u64,
}

impl<'a> StreamRecord<'a> {
    pub(crate) fn set_global_position(mut self, pos: u64) -> Self {
        self.global_position = pos;
        self
    }

    pub(crate) fn from_write_serial_message(
        msg: &'a WriteSerialMessage,
        global_position: u64,
    ) -> MessResult<Self> {
        Ok(Self {
            id: msg.id.to_string().into(),
            global_position,
            message_type: msg.message_type.as_ref().into(),
            data: msg.data.as_ref().into(),
            metadata: msg.metadata.as_ref().into(),
            ord: 0,
        })
    }

    pub(crate) fn from_bytes(bytes: impl AsRef<[u8]>) -> MessResult<Self> {
        postcard::from_bytes(bytes.as_ref())
            .map_err(|e| Error::DeserError(e.to_string()))
    }

    pub(crate) fn into_message(
        self,
        stream: Cow<'a, str>,
        position: StreamPos,
    ) -> Message<'a> {
        Message {
            global_position: self.global_position,
            stream_position: position,
            stream_name: stream,
            message_type: self.message_type,
            data: self.data,
            metadata: if self.metadata.is_empty() {
                None
            } else {
                Some(self.metadata)
            },
        }
    }
}
