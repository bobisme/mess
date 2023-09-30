use std::{borrow::Cow, io::IsTerminal, sync::Arc};

use bumpalo::Bump;
use ident::Id;
use mess::{
    db::{rocks::db::DB, svc::Connection, Message},
    ecs::{
        streams::StreamName, ApplyEvents, Component, ComponentStore, Entity,
        EventDB,
    },
};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn, Level};

#[derive(Clone, Default, Debug, serde::Serialize, serde::Deserialize)]
pub enum PostStatus {
    #[default]
    Unpublished,
    Visible,
    HiddenByPoster,
    HiddenByModerator,
}

#[derive(Clone, Default, Debug, serde::Serialize, serde::Deserialize)]
pub struct PostData {
    poster_id: Id,
    body: String,
    status: PostStatus,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Event {
    Unknown,
    Posted(PostData),
    HiddenByModerator,
    HiddenByPoster,
}

impl Event {
    pub fn from_msg(
        msg: Message<'_>,
    ) -> Result<Option<Self>, Box<dyn std::error::Error>> {
        match msg.message_type.as_ref() {
            "posted" => Ok(Some(Event::Posted(serde_json::from_slice(
                msg.data.as_ref(),
            )?))),
            _ => {
                warn!("unknown message");
                Ok(None)
            }
        }
    }
}

const EMPTY_SLICE: &[u8] = &[];

impl mess::ecs::Event for Event {
    fn name<'a>(&self) -> Cow<'a, str> {
        match self {
            Event::Unknown => Cow::Borrowed("Unknown"),
            Event::Posted(_) => Cow::Borrowed("Posted"),
            Event::HiddenByModerator => Cow::Borrowed("HiddenByModerator"),
            Event::HiddenByPoster => Cow::Borrowed("HiddenByPoster"),
        }
    }

    fn data<'a>(&self) -> mess::ecs::Result<Cow<'a, [u8]>> {
        Ok(match self {
            Event::Unknown => Cow::Borrowed(EMPTY_SLICE),
            Event::Posted(data) => Cow::Owned(
                serde_json::to_vec(data).map_err(mess::ecs::Error::external)?,
            ),
            Event::HiddenByModerator => Cow::Borrowed(EMPTY_SLICE),
            Event::HiddenByPoster => Cow::Borrowed(EMPTY_SLICE),
        })
    }

    fn metadata<'a>(&self) -> mess::ecs::Result<Cow<'a, [u8]>> {
        Ok(Cow::Borrowed(EMPTY_SLICE))
    }
}

impl From<Message<'_>> for Event {
    fn from(msg: Message) -> Self {
        let data = match serde_json::from_slice(&msg.data) {
            Ok(x) => x,
            Err(err) => {
                error!(?err, "error parsing msg.data");
                return Self::Unknown;
            }
        };
        match msg.message_type.as_ref() {
            "Posted" => Self::Posted(data),
            "HiddenByModerator" => Self::HiddenByModerator,
            "HiddenByPoster" => Self::HiddenByPoster,
            _ => {
                warn!("unknown message");
                Self::Unknown
            }
        }
    }
}

impl ApplyEvents for PostData {
    type Event = Event;
    fn apply_events(&mut self, events: impl Iterator<Item = Self::Event>) {
        for event in events {
            match event {
                Event::Posted(data) => {
                    self.poster_id = data.poster_id;
                    self.body = data.body;
                    self.status = data.status;
                }
                Event::HiddenByModerator => {
                    self.status = PostStatus::HiddenByModerator
                }
                Event::HiddenByPoster => {
                    self.status = PostStatus::HiddenByPoster
                }
                Event::Unknown => {}
            }
        }
    }
}

pub type Post = Component<PostData>;

pub fn configure_logging() {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let reg =
        tracing_subscriber::registry().with(EnvFilter::from_default_env());
    let use_json = !std::io::stdout().is_terminal()
        || matches!(
            std::env::var("LOG_FMT").unwrap_or_default().as_str(),
            "json" | "JSON"
        );
    match use_json {
        true => reg.with(fmt::layer().json()).init(),
        _ => reg.with(fmt::layer().pretty()).init(),
    };
}

#[tokio::main]
async fn main() -> ! {
    configure_logging();

    let bump = Bump::new();
    // static BUMP: Lazy<Arc<Bump>> = Lazy::new(|| {
    //     let bump = Bump::new();
    //     Arc::new(bump)
    // });
    // let bump = BUMP.lock_arc();

    // static CONN: Lazy<Arc<Connection>> = Lazy::new(|| {
    //     let db: DB = DB::new("xyz").expect("could not open xyz db");
    //     let c = Arc::new(Connection::new(db));
    //     c
    // });
    // let bump_conn = bump.alloc(Connection::new(
    //     DB::new(bump.alloc("xyz")).expect("could not open xyz db"),
    // ));
    let conn = Connection::new(DB::new(bump.alloc("xyz")).unwrap());
    let boxed_conn = bumpalo::boxed::Box::new_in(conn, &bump);
    let conn = Arc::new(boxed_conn);
    let evt_conn = Arc::clone(&conn);
    //
    let tok = CancellationToken::new();
    let sub_token = tok.clone();
    // let jh = std::thread::spawn(move || conn.handle_messages_thread(sub_token));
    // let jh = bumpalo::boxed::Box::new_in(jh, &bump);

    // let evdb = Arc::new(EventDB::new(evt_conn));
    // let evdb = bump.alloc(EventDB::new(evt_conn));
    let evdb = EventDB::new(evt_conn);
    let evdb = bumpalo::boxed::Box::new_in(evdb, &bump);
    let evdb = Arc::new(evdb);
    // let e
    let post_store =
        ComponentStore::<PostData, _, _, _, _>::new(Arc::clone(&evdb));
    // let post_store =
    //     Arc::new(ComponentStore::<PostData>::new(Arc::clone(&conn)));
    let poster = Entity::new();
    let post = Entity::new();
    // let stream = StreamName::from_component_and_id("post", post.id(), None);
    let stream = bump.alloc(StreamName::from_component_and_id(
        &bump.alloc("post"),
        post.id(),
        None,
    ));
    let event = Event::Posted(PostData {
        poster_id: poster.id(),
        body: "here is some stupid post".into(),
        status: PostStatus::Visible,
    });
    // let ps = Arc::clone(&post_store);
    let position = evdb
        .put(stream.source(), &event, None)
        .await
        .expect("complete failure");
    info!(?position, "wrote thing");

    let post = post_store.fetch(post, stream.source()).await.unwrap();
    info!(?post, "post");

    // shut down
    tok.cancel();
    loop {}
    // jh.join();
}
