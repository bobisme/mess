use std::borrow::Cow;

use crate::{
    error::{Error, Result},
    write::WriteSerialMessage,
    Message, StreamPos,
};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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
    ) -> Result<Self> {
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

    pub(crate) fn from_bytes<B: AsRef<[u8]>>(bytes: B) -> Result<Self> {
        postcard::from_bytes(bytes.as_ref())
            .map_err(|e| Error::DeserError(e.to_string()))
    }

    pub(crate) fn into_message(self, global_position: u64) -> Message<'a> {
        Message {
            global_position,
            stream_position: StreamPos::decode(self.stream_position),
            stream_name: self.stream_name,
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

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StreamRecord<'a> {
    pub(crate) global_position: u64,
    pub(crate) id: Cow<'a, str>,
    pub(crate) message_type: Cow<'a, str>,
    pub(crate) data: Cow<'a, [u8]>,
    pub(crate) metadata: Cow<'a, [u8]>,
    pub(crate) ord: u64,
}

impl<'a> StreamRecord<'a> {
    #[allow(clippy::missing_const_for_fn)]
    pub(crate) fn set_global_position(mut self, pos: u64) -> Self {
        self.global_position = pos;
        self
    }

    pub(crate) fn from_write_serial_message(
        msg: &'a WriteSerialMessage,
        global_position: u64,
    ) -> Result<Self> {
        Ok(Self {
            id: msg.id.to_string().into(),
            global_position,
            message_type: msg.message_type.as_ref().into(),
            data: msg.data.as_ref().into(),
            metadata: msg.metadata.as_ref().into(),
            ord: 0,
        })
    }

    pub(crate) fn from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self> {
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
