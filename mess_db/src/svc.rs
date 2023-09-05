use std::borrow::Cow;

use crossbeam_queue::SegQueue;
use tokio::sync::oneshot::{self, Sender};

use crate::{
    error::Error,
    rocks::{
        keys::SEPARATOR_CHAR,
        read::{LIMIT_DEFAULT, LIMIT_MAX},
    },
    Message, StreamPos,
};
//
// type states for GetMessages options
#[derive(Default, Clone, Copy)]
pub struct Unset;
#[derive(Default, Clone)]
pub struct OptStream<'a>(Cow<'a, str>);
#[derive(Default, Clone, Copy)]
pub struct OptGlobalPos(u64);
#[derive(Clone, Copy)]
pub struct OptStreamPos(StreamPos);

#[derive(Clone, PartialEq, PartialOrd)]
pub struct GetMessages<Strm, G, S> {
    pub(crate) start_global_position: G,
    pub(crate) start_stream_position: S,
    pub(crate) limit: usize,
    pub(crate) stream: Strm,
}

impl<P, G, S> GetMessages<P, G, S> {
    pub const fn limit(mut self, limit: usize) -> Self {
        self.limit = match limit {
            x if x < 1 => 1,
            x if x > LIMIT_MAX => LIMIT_MAX,
            _ => limit,
        };
        self
    }
}

impl<P, G, S> GetMessages<P, G, S> {
    #[allow(clippy::missing_const_for_fn)]
    pub fn from_global(self, position: u64) -> GetMessages<P, OptGlobalPos, S> {
        GetMessages {
            start_global_position: OptGlobalPos(position),
            start_stream_position: self.start_stream_position,
            limit: self.limit,
            stream: self.stream,
        }
    }
}

impl<P, G, S> GetMessages<P, G, S> {
    pub fn in_stream(self, name: &str) -> GetMessages<OptStream, G, S> {
        let mut name = name.to_string();
        name.push(SEPARATOR_CHAR);
        GetMessages {
            start_global_position: self.start_global_position,
            start_stream_position: self.start_stream_position,
            limit: self.limit,
            stream: OptStream(name.into()),
        }
    }
}

impl Default for GetMessages<Unset, Unset, Unset> {
    fn default() -> Self {
        Self {
            start_global_position: Default::default(),
            start_stream_position: Default::default(),
            limit: LIMIT_DEFAULT,
            stream: Default::default(),
        }
    }
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
}
pub struct Request<'a> {
    pub(crate) body: RequestBody<'a>,
}
pub enum ResponseBody {
    // Messages { iter: I },
}
pub struct Response {
    pub body: ResponseBody,
}

#[derive(Default)]
pub struct Connection<'a> {
    queue:
        SegQueue<(Request<'a>, Sender<Box<dyn Iterator<Item = Message<'a>>>>)>,
}

impl<'a> Connection<'a> {
    #[must_use]
    pub const fn new() -> Self {
        Self { queue: SegQueue::new() }
    }

    pub async fn fetch_messages(
        &'a self,
        req: impl Into<Request<'a>>,
    ) -> Result<Box<dyn Iterator<Item = Message>>, Error> {
        let (send, recv) = oneshot::channel();
        self.queue.push((req.into(), send));
        recv.await.map_err(Error::from)
    }
}
