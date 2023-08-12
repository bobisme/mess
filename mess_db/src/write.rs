use std::borrow::Cow;

use ident::Id;

pub struct WriteMessage<'a, D, M> {
    pub id: Id,
    pub stream_name: Cow<'a, str>,
    pub message_type: Cow<'a, str>,
    pub data: D,
    pub metadata: Option<M>,
    pub expected_stream_position: Option<i64>,
}
