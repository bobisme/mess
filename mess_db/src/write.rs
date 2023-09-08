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
pub struct WriteSerialMessage<'a> {
    pub id: Id,
    pub stream_name: Cow<'a, str>,
    pub message_type: Cow<'a, str>,
    pub data: Cow<'a, [u8]>,
    pub metadata: Cow<'a, [u8]>,
    pub expected_position: Option<StreamPos>,
}
