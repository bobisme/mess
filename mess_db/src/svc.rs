use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::{
    error::{Error, Result},
    read::{GetMessages, OptGlobalPos, OptStream, OptStreamPos, Unset},
    rocks::{db::DB, read::Fetch, write::WriteSerializer},
    write::{OwnedWriteMessage, WriteMessage},
    Message, OwnedMessage, Position, StreamPos,
};

#[derive(Debug)]
pub enum RequestBody {
    GetGlobalMessages {
        stream: Option<String>,
        global_pos: u64,
        limit: usize,
    },
    GetStreamMessages {
        stream: String,
        stream_pos: Option<StreamPos>,
        limit: usize,
    },
    Write(OwnedWriteMessage),
}

impl From<GetMessages<Unset, OptGlobalPos, Unset>> for RequestBody {
    fn from(val: GetMessages<Unset, OptGlobalPos, Unset>) -> Self {
        RequestBody::GetGlobalMessages {
            stream: None,
            global_pos: val.start_global_position.0,
            limit: val.limit,
        }
    }
}

impl From<GetMessages<OptStream<'_>, OptGlobalPos, Unset>> for RequestBody {
    fn from(val: GetMessages<OptStream, OptGlobalPos, Unset>) -> Self {
        RequestBody::GetGlobalMessages {
            stream: Some(val.stream.0.to_string()),
            global_pos: val.start_global_position.0,
            limit: val.limit,
        }
    }
}

impl From<GetMessages<OptStream<'_>, Unset, Unset>> for RequestBody {
    fn from(val: GetMessages<OptStream, Unset, Unset>) -> Self {
        RequestBody::GetStreamMessages {
            stream: val.stream.0.to_string(),
            stream_pos: None,
            limit: val.limit,
        }
    }
}

impl From<GetMessages<OptStream<'_>, Unset, OptStreamPos>> for RequestBody {
    fn from(val: GetMessages<OptStream, Unset, OptStreamPos>) -> Self {
        RequestBody::GetStreamMessages {
            stream: val.stream.0.to_string(),
            stream_pos: Some(val.start_stream_position.0),
            limit: val.limit,
        }
    }
}

#[derive(Debug)]
pub struct Request {
    pub(crate) body: RequestBody,
    pub(crate) response_chan: oneshot::Sender<Response>,
}

impl Request {
    fn new(
        body: RequestBody,
        response_chan: oneshot::Sender<Response>,
    ) -> Request {
        Request { body, response_chan }
    }
}

pub type DynMessageIter<'iter, 'msg> =
    Box<dyn 'iter + Iterator<Item = Result<Message<'msg>>> + Send + Sync>;

pub type DynOwnedMessageIter =
    Box<dyn Iterator<Item = Result<OwnedMessage>> + Send + Sync>;

#[derive(Debug)]
pub enum ResponseBody {
    Messages { messages: Vec<Result<OwnedMessage>> },
    Write { pos: Result<Position> },
    Err,
}

// impl std::fmt::Debug for ResponseBody {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             Self::Messages { messages: _ } => f
//                 .debug_struct("Messages")
//                 .field("messages", &"<...messages...>")
//                 .finish(),
//             Self::Write { pos } => {
//                 f.debug_struct("Write").field("pos", pos).finish()
//             }
//         }
//     }
// }

#[derive(Debug)]
pub struct Response {
    pub body: ResponseBody,
}

const _: () = {
    const fn is_sync(_: &impl Sync) -> bool {
        true
    }
    const U: &[u8] = &[];
    // const REQ: Request = Request {
    //     body: RequestBody::Write(WriteMessage {
    //         id: Id::from_u128(1234),
    //         stream_name: Cow::Borrowed(""),
    //         message_type: Cow::Borrowed(""),
    //         data: Cow::Borrowed(U),
    //         metadata: Cow::Borrowed(U),
    //         expected_stream_position: None,
    //     }),
    // };
    // assert!(is_sync(&REQ));
    // const RES: Response = Response {
    //     body: ResponseBody::Write {
    //         pos: Ok(Position { global: 0, stream: StreamPos::Sequential(0) }),
    //     },
    // };
    // assert!(is_sync(&RES));
};

pub struct Actor {
    inbox: mpsc::Receiver<Request>,
    // Only the actor can touch the DB.
    db: DB,
    ser: WriteSerializer,
    token: CancellationToken,
}

impl Actor {
    async fn handle_req(&mut self, req: Request) -> Result<()> {
        if self.token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let resp = match req.body {
            RequestBody::GetGlobalMessages { stream, global_pos, limit } => {
                let opts = GetMessages::default()
                    .from_global(global_pos)
                    .with_limit(limit);
                let messages = Fetch::<OptGlobalPos>::fetch(&self.db, opts);
                let messages: Vec<_> =
                    messages.map(|res| res.map(|msg| msg.into())).collect();
                Response { body: ResponseBody::Messages { messages } }
            }
            RequestBody::GetStreamMessages { stream, stream_pos, limit } => {
                let opts =
                    GetMessages::default().in_stream(&stream).with_limit(limit);
                let messages = Fetch::<OptStream>::fetch(&self.db, opts);
                let messages: Vec<_> =
                    messages.map(|res| res.map(|msg| msg.into())).collect();
                Response { body: ResponseBody::Messages { messages } }
            }
            RequestBody::Write(message) => {
                let pos = crate::rocks::write::write_mess(
                    &self.db,
                    message.into(),
                    &mut self.ser,
                );
                Response { body: ResponseBody::Write { pos } }
            }
        };
        debug!(?resp, "responding with");
        let _ = req.response_chan.send(resp);
        Ok(())
    }
}

async fn run_actor(mut actor: Actor) {
    while let Some(req) = actor.inbox.recv().await {
        debug!(?req, "got request");
        if actor.token.is_cancelled() {
            debug!("actor cancelled");
            break;
        }
        actor.handle_req(req).await.unwrap();
    }
    debug!("actor killed");
}

#[derive(Clone)]
pub struct ActorHandle<const S: usize = 4096> {
    outbox: mpsc::Sender<Request>,
    token: CancellationToken,
}

impl<const S: usize> ActorHandle<S> {
    #[must_use]
    pub fn new(db: DB) -> Self {
        // TODO: REMOVE MAGIC NUMBER!
        let (outbox, inbox) = mpsc::channel(S);
        let token = CancellationToken::new();
        let actor = Actor {
            inbox,
            db,
            token: token.clone(),
            ser: WriteSerializer::new(),
        };
        tokio::spawn(run_actor(actor));
        Self { outbox, token }
    }

    pub fn kill(&self) {
        self.token.cancel()
    }

    pub async fn put_message(&self, wm: WriteMessage<'_>) -> Result<Position> {
        if self.token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let (send, recv) = oneshot::channel();
        let req = Request {
            body: RequestBody::Write(wm.into()),
            response_chan: send,
        };
        // Ignore send errors and handle it on the recv end below.
        let _ = self.outbox.send(req).await;
        let res = recv.await?;
        debug!("put messages");
        match res.body {
            ResponseBody::Write { pos } => pos,
            resp => {
                error!(?resp, "unexpected service response body");
                Err(Error::SvcResponse)
            }
        }
    }

    pub async fn fetch_messages(
        &self,
        req_body: impl Into<RequestBody>,
    ) -> Result<Vec<Result<OwnedMessage>>> {
        let (send, recv) = oneshot::channel();
        let req_body = req_body.into();
        let req = Request::new(req_body, send);
        // Ignore send errors and handle it on the recv end below.
        let _ = self.outbox.send(req).await;
        let resp = recv.await.unwrap();
        debug!("fetch messages");
        match resp.body {
            ResponseBody::Messages { messages } => Ok(messages),
            resp => {
                error!(?resp, "unexpected service response body");
                Err(Error::SvcResponse)
            }
        }
    }
}
