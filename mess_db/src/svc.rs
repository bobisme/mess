use std::borrow::Cow;

use crossbeam_queue::SegQueue;
use ident::Id;
use tokio::{
    sync::oneshot::{self, Sender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::{
    error::{Error, Result},
    read::{GetMessages, OptGlobalPos, OptStream, OptStreamPos, Unset},
    rocks::{db::DB, read::Fetch, write::WriteSerializer},
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

pub type DynMessageIter<'iter, 'msg> =
    Box<dyn 'iter + Iterator<Item = Result<Message<'msg>>> + Send + Sync>;

pub enum ResponseBody<'iter, 'msg> {
    Messages { messages: DynMessageIter<'iter, 'msg> },
    Write { pos: Result<Position> },
}

impl std::fmt::Debug for ResponseBody<'_, '_> {
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

#[derive(Debug)]
pub struct Response<'iter, 'msg> {
    pub body: ResponseBody<'iter, 'msg>,
}

const _: () = {
    const fn is_sync(_: &impl Sync) -> bool {
        true
    }
    const U: &[u8] = &[];
    const REQ: Request = Request {
        body: RequestBody::Write(WriteMessage {
            id: Id::from_u128(1234),
            stream_name: Cow::Borrowed(""),
            message_type: Cow::Borrowed(""),
            data: Cow::Borrowed(U),
            metadata: Cow::Borrowed(U),
            expected_stream_position: None,
        }),
    };
    assert!(is_sync(&REQ));
    const RES: Response = Response {
        body: ResponseBody::Write {
            pos: Ok(Position { global: 0, stream: StreamPos::Sequential(0) }),
        },
    };
    assert!(is_sync(&RES));
};

type QueueItem<'req, 'iter, 'msg> =
    (Request<'req>, Sender<Response<'iter, 'msg>>);

pub struct Connection<'req, 'iter, 'msg> {
    db: DB,
    queue: SegQueue<QueueItem<'req, 'iter, 'msg>>,
    // task: tokio::runtime::Handle,
}

impl<'req, 'iter, 'msg> Connection<'req, 'iter, 'msg> {
    #[must_use]
    pub const fn new(db: DB) -> Self {
        Self { queue: SegQueue::new(), db }
    }

    pub async fn fetch_messages<'conn: 'iter + 'msg>(
        &'conn self,
        req: impl Into<Request<'req>>,
    ) -> Result<DynMessageIter<'iter, 'msg>> {
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
        &self,
        wm: WriteMessage<'req>,
    ) -> Result<Position> {
        let req = Request { body: RequestBody::Write(wm) };
        let (send, recv) = oneshot::channel();
        self.queue.push((req, send));
        let res = recv.await.map_err(Error::from)?;
        match res.body {
            ResponseBody::Write { pos } => pos,
            resp => {
                error!(?resp, "unexpected service response body");
                Err(Error::SvcResponse)
            }
        }
    }

    pub fn handle_messages_tokio(
        &'static self,
        // conn: &'static Connection<'_, '_, '_>,
        token: Option<CancellationToken>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ser = WriteSerializer::<1024>::new();
            while token.as_ref().filter(|t| t.is_cancelled()).is_none() {
                handle_messages_inner(self, &mut ser).await;
            }
        })
    }

    pub fn handle_messages_thread<'slf: 'iter>(
        &'slf self,
        token: CancellationToken,
    ) {
        let mut ser = WriteSerializer::<1024>::new();
        while !token.is_cancelled() {
            handle_messages_inner_thread(self, &mut ser);
        }
    }
}

// pub fn handle_messages(
//     conn: &'static Connection<'_, '_, '_>,
//     token: CancellationToken,
// ) -> JoinHandle<()> {
//     tokio::spawn(async move {
//         let mut ser = WriteSerializer::<1024>::new();
//         loop {
//             if token.is_cancelled() {
//                 break;
//             }
//             handle_messages_inner(conn, &mut ser).await;
//         }
//     })
// }

async fn handle_messages_inner<'conn: 'iter, 'iter>(
    conn: &'conn Connection<'_, 'iter, '_>,
    ser: &mut WriteSerializer,
) {
    let popped = conn.queue.pop();
    let Some((req, resp_ch)) = popped else {
        tokio::task::yield_now().await;
        return;
    };
    match req.body {
        RequestBody::GetGlobalMessages { stream: _, global_pos, limit } => {
            let opts = GetMessages::default()
                .from_global(global_pos)
                .with_limit(limit);
            let messages =
                Fetch::<Unset, OptGlobalPos, _>::fetch(&conn.db, opts);
            let messages = messages.unwrap();
            let resp = Response {
                body: ResponseBody::Messages { messages: Box::new(messages) },
            };
            resp_ch.send(resp).unwrap();
        }
        RequestBody::GetStreamMessages {
            stream: _,
            stream_pos: _,
            limit: _,
        } => {}
        RequestBody::Write(message) => {
            let res = crate::rocks::write::write_mess(&conn.db, message, ser);
            resp_ch
                .send(Response { body: ResponseBody::Write { pos: res } })
                .unwrap();
        }
    }
}

fn handle_messages_inner_thread<'conn: 'iter, 'iter>(
    conn: &'conn Connection<'_, 'iter, '_>,
    ser: &mut WriteSerializer,
) {
    let popped = conn.queue.pop();
    let Some((req, resp_ch)) = popped else {
        std::thread::yield_now();
        // std::hint::spin_loop();
        return;
    };
    match req.body {
        RequestBody::GetGlobalMessages { stream, global_pos, limit } => {
            let opts = GetMessages::default()
                .from_global(global_pos)
                .with_limit(limit);
            let messages =
                Fetch::<Unset, OptGlobalPos, _>::fetch(&conn.db, opts);
            let messages = messages.unwrap();
            let resp = Response {
                body: ResponseBody::Messages { messages: Box::new(messages) },
            };
            resp_ch.send(resp).unwrap();
        }
        RequestBody::GetStreamMessages { stream, stream_pos, limit } => {}
        RequestBody::Write(message) => {
            let res = crate::rocks::write::write_mess(&conn.db, message, ser);
            resp_ch
                .send(Response { body: ResponseBody::Write { pos: res } })
                .unwrap();
        }
    }
}
