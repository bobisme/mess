use std::{borrow::Cow, io::IsTerminal, sync::Arc, time::Duration};

use ident::Id;
use mess::{
    db::{rocks::db::DB, svc::ActorHandle, Message},
    ecs::{
        streams::StreamName, ApplyEvents, Component, ComponentStore, Entity,
        EventDB,
    },
};
use tracing::{error, info, warn};

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
async fn main() {
    configure_logging();

    let db = DB::new("xyz").unwrap();
    let handle = ActorHandle::new(db);
    let evdb = EventDB::new(handle.clone());
    let evdb = Arc::new(evdb);
    let post_store = ComponentStore::<PostData, _>::new(Arc::clone(&evdb));
    let poster = Entity::new();
    let post = Entity::new();
    let stream = StreamName::from_component_and_id("post", post.id(), None);
    let event = Event::Posted(PostData {
        poster_id: poster.id(),
        body: "here is some stupid post".into(),
        status: PostStatus::Visible,
    });
    let position = evdb
        .put(stream.source(), &event, None)
        .await
        .expect("complete failure");
    info!(?position, "wrote thing");

    let post = post_store.fetch(post, stream.source()).await.unwrap();
    info!(?post, "post");

    // shut down
    tokio::time::sleep(Duration::from_secs(1)).await;
    handle.kill();
    // loop {}
    // jh.join();
}
