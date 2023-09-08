use std::borrow::Cow;

use crossbeam_queue::SegQueue;
use tokio::sync::oneshot::{self, Sender};
use tracing::error;

use crate::{
    error::{Error, Result},
    read::{GetMessages, OptGlobalPos, OptStream, OptStreamPos, Unset},
    write::WriteMessage,
    Message, Position, StreamPos,
};

pub(crate) enum RequestBody<'a> {
    GetGlobalMessages {
        stream: Option<Cow<'a, str>>,
        global_pos: u64,
        limit: usize,
    },
    GetStreamMessages {
        stream: Cow<'a, str>,
        stream_pos: Option<StreamPos>,
        limit: usize,
    },
    Write(WriteMessage<'a>),
}

impl<'a> From<GetMessages<Unset, OptGlobalPos, Unset>> for Request<'a> {
    fn from(val: GetMessages<Unset, OptGlobalPos, Unset>) -> Self {
        Request {
            body: RequestBody::GetGlobalMessages {
                stream: None,
                global_pos: val.start_global_position.0,
                limit: val.limit,
            },
        }
    }
}

impl<'a> From<GetMessages<OptStream<'a>, OptGlobalPos, Unset>> for Request<'a> {
    fn from(val: GetMessages<OptStream<'a>, OptGlobalPos, Unset>) -> Self {
        Request {
            body: RequestBody::GetGlobalMessages {
                stream: Some(val.stream.0),
                global_pos: val.start_global_position.0,
                limit: val.limit,
            },
        }
    }
}

impl<'a> From<GetMessages<OptStream<'a>, Unset, Unset>> for Request<'a> {
    fn from(val: GetMessages<OptStream<'a>, Unset, Unset>) -> Self {
        Request {
            body: RequestBody::GetStreamMessages {
                stream: val.stream.0,
                stream_pos: None,
                limit: val.limit,
            },
        }
    }
}

impl<'a> From<GetMessages<OptStream<'a>, Unset, OptStreamPos>> for Request<'a> {
    fn from(val: GetMessages<OptStream<'a>, Unset, OptStreamPos>) -> Self {
        Request {
            body: RequestBody::GetStreamMessages {
                stream: val.stream.0,
                stream_pos: Some(val.start_stream_position.0),
                limit: val.limit,
            },
        }
    }
}
pub struct Request<'a> {
    pub(crate) body: RequestBody<'a>,
}

pub type DynMessageIter<'a> = Box<dyn Iterator<Item = Message<'a>>>;

pub enum ResponseBody<'a> {
    Messages { messages: DynMessageIter<'a> },
    Write { pos: Position },
}

impl<'a> std::fmt::Debug for ResponseBody<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Messages { messages: _ } => f
                .debug_struct("Messages")
                .field("messages", &"<...messages...>")
                .finish(),
            Self::Write { pos } => {
                f.debug_struct("Write").field("pos", pos).finish()
            }
        }
    }
}

pub struct Response<'a> {
    pub body: ResponseBody<'a>,
}

#[derive(Default)]
pub struct Connection<'a> {
    queue: SegQueue<(Request<'a>, Sender<Response<'a>>)>,
}

impl<'a> Connection<'a> {
    #[must_use]
    pub const fn new() -> Self {
        Self { queue: SegQueue::new() }
    }

    pub async fn fetch_messages(
        &'a self,
        req: impl Into<Request<'a>>,
    ) -> Result<Box<dyn Iterator<Item = Message>>> {
        let (send, recv) = oneshot::channel();
        self.queue.push((req.into(), send));
        let res = recv.await.map_err(Error::from)?;
        match res.body {
            ResponseBody::Messages { messages } => Ok(messages),
            resp => {
                error!(?resp, "unexpected service response body");
                Err(Error::SvcResponse)
            }
        }
    }

    pub async fn put_message(
        &'a self,
        wm: WriteMessage<'a>,
    ) -> Result<Position> {
        let req = Request { body: RequestBody::Write(wm) };
        let (send, recv) = oneshot::channel();
        self.queue.push((req, send));
        let res = recv.await.map_err(Error::from)?;
        match res.body {
            ResponseBody::Write { pos } => Ok(pos),
            resp => {
                error!(?resp, "unexpected service response body");
                Err(Error::SvcResponse)
            }
        }
    }
}
