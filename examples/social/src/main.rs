use std::{default, sync::Arc};

use ident::Id;
use mess::{
    db::{svc::Connection, Message},
    ecs::{ApplyEvents, Component, ComponentStore, Entity, Version},
};
use tracing::{error, warn};

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

impl<'a> From<Message<'a>> for Event {
    // fn from_msg(
    //     msg: Message<'_>,
    // ) -> Result<Option<Self>, Box<dyn std::error::Error>> {
    //     match msg.message_type.as_ref() {
    //         "posted" => Ok(Some(Event::Posted(serde_json::from_slice(
    //             msg.data.as_ref(),
    //         )?))),
    //         _ => {
    //             warn!("unknown message");
    //             Ok(None)
    //         }
    //     }
    // }
    //
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

#[tokio::main]
async fn main() {
    let conn = Arc::new(Connection::new());
    let post_store = ComponentStore::<PostData>::new(Arc::clone(&conn));
    let poster = Entity::new();
    let post = Entity::new();
    // post_store
    //     .put(
    //
    //         Event::Posted(PostData {
    //         poster_id: poster,
    //         body: "here is some stupid post".into(),
    //         status: PostStatus::Visible,
    //     }),
    //         None
    //     )
    //     .await
}
