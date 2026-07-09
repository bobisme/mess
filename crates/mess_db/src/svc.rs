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
    const fn new(
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
                let messages: Vec<Result<OwnedMessage>> = match stream {
                    Some(stream) => {
                        let opts = opts.in_stream(&stream);
                        Fetch::<(OptStream, OptGlobalPos)>::fetch(
                            &self.db, opts,
                        )
                        .map(|res| res.map(|msg| msg.into()))
                        .collect()
                    }
                    None => Fetch::<OptGlobalPos>::fetch(&self.db, opts)
                        .map(|res| res.map(|msg| msg.into()))
                        .collect(),
                };
                Response { body: ResponseBody::Messages { messages } }
            }
            RequestBody::GetStreamMessages { stream, stream_pos, limit } => {
                let opts =
                    GetMessages::default().in_stream(&stream).with_limit(limit);
                let messages: Vec<Result<OwnedMessage>> = match stream_pos {
                    Some(pos) => {
                        let opts = opts.from_stream_position(pos);
                        Fetch::<(OptStream, OptStreamPos)>::fetch(
                            &self.db, opts,
                        )
                        .map(|res| res.map(|msg| msg.into()))
                        .collect()
                    }
                    None => Fetch::<OptStream>::fetch(&self.db, opts)
                        .map(|res| res.map(|msg| msg.into()))
                        .collect(),
                };
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
        // The actor must outlive any single failed request: log and keep
        // serving. Per-request errors travel back on the response channel.
        if let Err(err) = actor.handle_req(req).await {
            error!(?err, "actor failed to handle request");
        }
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
        if self.token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let (send, recv) = oneshot::channel();
        let req_body = req_body.into();
        let req = Request::new(req_body, send);
        // Ignore send errors and handle it on the recv end below.
        let _ = self.outbox.send(req).await;
        let resp = recv.await?;
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

#[cfg(test)]
mod test_actor {
    use super::*;
    use assert2::assert;
    use ident::Id;

    struct TmpHandle {
        handle: ActorHandle,
        path: std::path::PathBuf,
    }

    impl TmpHandle {
        fn new() -> Self {
            let path = std::env::temp_dir().join(Id::new().to_string());
            let db = DB::new(&path).unwrap();
            Self { handle: ActorHandle::new(db), path }
        }

        async fn cleanup(self) {
            self.handle.kill();
            drop(self.handle);
            // Give the actor task a moment to drop the DB, then best-effort
            // destroy the on-disk files.
            tokio::task::yield_now().await;
            let _ = ::rocksdb::DB::destroy(
                &::rocksdb::Options::default(),
                &self.path,
            );
        }
    }

    fn write_msg(
        stream: &str,
        expected: Option<StreamPos>,
    ) -> WriteMessage<'static> {
        WriteMessage {
            id: Id::new(),
            stream_name: stream.to_owned().into(),
            message_type: "SomeType".to_owned().into(),
            data: b"{\"a\": 1}".as_slice().into(),
            metadata: [].as_slice().into(),
            expected_stream_position: expected,
        }
    }

    #[tokio::test]
    async fn actor_survives_write_errors_and_keeps_serving() {
        let h = TmpHandle::new();

        // A wrong expected position comes back as its typed error...
        let res = h
            .handle
            .put_message(write_msg("s1", Some(StreamPos::new(41))))
            .await;
        assert!(let Err(Error::WrongStreamPosition { .. }) = res);

        // ...without killing the actor: a valid write still succeeds.
        let pos = h.handle.put_message(write_msg("s1", None)).await.unwrap();
        assert!(pos.global == 1);

        h.cleanup().await;
    }

    #[tokio::test]
    async fn stream_fetch_honors_start_position() {
        let h = TmpHandle::new();
        h.handle.put_message(write_msg("s1", None)).await.unwrap();
        for v in 0..3 {
            h.handle
                .put_message(write_msg(
                    "s1",
                    Some(StreamPos::new(v)),
                ))
                .await
                .unwrap();
        }

        let req = GetMessages::default()
            .in_stream("s1")
            .from_stream_position(StreamPos::new(2));
        let messages = h.handle.fetch_messages(req).await.unwrap();
        let messages: Result<Vec<_>> = messages.into_iter().collect();
        let messages = messages.unwrap();

        assert!(messages.len() == 2);
        assert!(messages[0].stream_position == StreamPos::new(2));
        assert!(messages[1].stream_position == StreamPos::new(3));

        h.cleanup().await;
    }

    #[tokio::test]
    async fn global_fetch_honors_stream_filter() {
        let h = TmpHandle::new();
        h.handle.put_message(write_msg("s1", None)).await.unwrap();
        h.handle.put_message(write_msg("s2", None)).await.unwrap();
        h.handle
            .put_message(write_msg("s1", Some(StreamPos::new(0))))
            .await
            .unwrap();

        let req = GetMessages::default().from_global(0).in_stream("s1");
        let messages = h.handle.fetch_messages(req).await.unwrap();
        let messages: Result<Vec<_>> = messages.into_iter().collect();
        let messages = messages.unwrap();

        assert!(messages.len() == 2);
        assert!(messages.iter().all(|m| m.stream_name == "s1"));

        h.cleanup().await;
    }

    #[tokio::test]
    async fn handle_errors_instead_of_panicking_when_actor_gone() {
        let h = TmpHandle::new();
        h.handle.kill();
        let res = h.handle.put_message(write_msg("s1", None)).await;
        assert!(res.is_err());
        let req = GetMessages::default().from_global(0);
        let res = h.handle.fetch_messages(req).await;
        assert!(res.is_err());
        h.cleanup().await;
    }
}
