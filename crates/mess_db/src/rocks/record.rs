use std::borrow::Cow;

use ident::Id;

use crate::{
    Message, StreamPos,
    error::{Error, Result},
};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GlobalRecord<'a> {
    pub(crate) id:              Cow<'a, str>,
    pub(crate) stream_name:     Cow<'a, str>,
    pub(crate) stream_position: u64,
    pub(crate) message_type:    Cow<'a, str>,
    pub(crate) data:            Cow<'a, [u8]>,
    pub(crate) metadata:        Cow<'a, [u8]>,
    pub(crate) ord:             u64,
}

impl<'a> GlobalRecord<'a> {
    /// Build a global-CF record for one event at an explicit stream position.
    /// Positions are assigned by the batch writer, not derived from the
    /// expected version, so each event in a multi-event append gets its own.
    pub(crate) fn build(
        id: &Id,
        stream_name: &'a str,
        stream_position: u64,
        message_type: &'a str,
        data: &'a [u8],
        metadata: &'a [u8],
    ) -> Self {
        Self {
            id: id.to_string().into(),
            stream_name: stream_name.into(),
            stream_position,
            message_type: message_type.into(),
            data: data.into(),
            metadata: metadata.into(),
            ord: 0,
        }
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
    pub(crate) id:              Cow<'a, str>,
    pub(crate) message_type:    Cow<'a, str>,
    pub(crate) data:            Cow<'a, [u8]>,
    pub(crate) metadata:        Cow<'a, [u8]>,
    pub(crate) ord:             u64,
}

impl<'a> StreamRecord<'a> {
    /// Build a stream-CF record for one event at an explicit global position.
    pub(crate) fn build(
        id: &Id,
        global_position: u64,
        message_type: &'a str,
        data: &'a [u8],
        metadata: &'a [u8],
    ) -> Self {
        Self {
            id: id.to_string().into(),
            global_position,
            message_type: message_type.into(),
            data: data.into(),
            metadata: metadata.into(),
            ord: 0,
        }
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
            stream_name:     stream,
            message_type:    self.message_type,
            data:            self.data,
            metadata:        if self.metadata.is_empty() {
                None
            } else {
                Some(self.metadata)
            },
        }
    }
}
