use std::borrow::Cow;

use ident::Id;

use crate::StreamPos;

#[derive(Clone, Debug)]
pub struct WriteMessageOld<'a, D, M> {
    pub id: Id,
    pub stream_name: Cow<'a, str>,
    pub message_type: Cow<'a, str>,
    pub data: D,
    pub metadata: Option<M>,
    pub expected_stream_position: Option<StreamPos>,
}

#[derive(Clone, Debug)]
pub struct WriteMessage<'a> {
    pub id: Id,
    pub stream_name: Cow<'a, str>,
    pub message_type: Cow<'a, str>,
    pub data: Cow<'a, [u8]>,
    pub metadata: Cow<'a, [u8]>,
    pub expected_stream_position: Option<StreamPos>,
}

#[derive(Clone, Debug)]
pub struct OwnedWriteMessage {
    pub id: Id,
    pub stream_name: String,
    pub message_type: String,
    pub data: Vec<u8>,
    pub metadata: Vec<u8>,
    pub expected_stream_position: Option<StreamPos>,
}

impl From<WriteMessage<'_>> for OwnedWriteMessage {
    fn from(msg: WriteMessage<'_>) -> Self {
        OwnedWriteMessage {
            id: msg.id,
            stream_name: msg.stream_name.to_string(),
            message_type: msg.message_type.to_string(),
            data: msg.data.to_vec(),
            metadata: msg.metadata.to_vec(),
            expected_stream_position: msg.expected_stream_position,
        }
    }
}

impl From<OwnedWriteMessage> for WriteMessage<'_> {
    fn from(msg: OwnedWriteMessage) -> Self {
        WriteMessage {
            id: msg.id,
            stream_name: msg.stream_name.into(),
            message_type: msg.message_type.into(),
            data: msg.data.into(),
            metadata: msg.metadata.into(),
            expected_stream_position: msg.expected_stream_position,
        }
    }
}

#[derive(Clone, Debug)]
pub struct WriteSerialMessage<'a> {
    pub id: Id,
    pub stream_name: Cow<'a, str>,
    pub message_type: Cow<'a, str>,
    pub data: Cow<'a, [u8]>,
    pub metadata: Cow<'a, [u8]>,
    pub expected_position: Option<StreamPos>,
}

impl<'a> From<WriteMessage<'a>> for WriteSerialMessage<'a> {
    fn from(msg: WriteMessage<'a>) -> Self {
        Self {
            id: msg.id,
            stream_name: msg.stream_name,
            message_type: msg.message_type,
            data: msg.data,
            metadata: msg.metadata,
            expected_position: msg.expected_stream_position,
        }
    }
}
