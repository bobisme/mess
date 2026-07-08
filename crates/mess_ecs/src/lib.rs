#![warn(
    clippy::pedantic,
    // clippy::nursery,
    clippy::missing_inline_in_public_items
)]

pub mod error;
pub mod streams;

use std::{
    borrow::Cow, convert::Into, fmt::Display, marker::PhantomData, ops::Deref,
    sync::Arc,
};

use crate::error::Error;
use ident::Id;
use mess_db::{
    svc::ActorHandle, write::WriteMessage, Message, Position, StreamPos,
};
use parking_lot::RwLock;
use quick_cache::sync::Cache;
use tracing::debug;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Entity(Id);

impl Entity {
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self(Id::new())
    }

    #[must_use]
    #[inline]
    pub const fn id(&self) -> Id {
        self.0
    }
}

impl Default for Entity {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Display for Entity {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Version {
    Sequential(u64),
    Relaxed(u64),
}

impl From<Version> for StreamPos {
    #[inline]
    fn from(value: Version) -> Self {
        match value {
            Version::Sequential(x) => Self::Sequential(x),
            Version::Relaxed(x) => Self::Relaxed(x),
        }
    }
}

impl From<StreamPos> for Version {
    #[inline]
    fn from(value: StreamPos) -> Self {
        match value {
            StreamPos::Sequential(x) => Self::Sequential(x),
            StreamPos::Relaxed(x) => Self::Relaxed(x),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Component<Data> {
    entity: Entity,
    data: Arc<RwLock<Data>>,
}

impl<Data> Component<Data>
where
    Data: Default,
{
    #[must_use]
    #[inline]
    pub fn new(entity: Entity) -> Self {
        Self { entity, data: Arc::new(RwLock::new(Default::default())) }
    }
}

impl<Data> PartialEq for Component<Data>
where
    Data: PartialEq,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.entity == other.entity
            && *self.data.read_arc() == *other.data.read_arc()
    }
}

impl<Data> Eq for Component<Data> where Data: Eq {}

pub trait Event {
    fn name<'a>(&self) -> Cow<'a, str>;
    /// Return serialized version of the event's data.
    ///
    /// # Errors
    ///
    /// Return an error if there is a problem serializing the data.
    fn data<'a>(&self) -> Result<Cow<'a, [u8]>, Error>;
    /// Return serialized version of the event's metadata.
    ///
    /// # Errors
    ///
    /// Return an error if there is a problem serializing the metadata.
    fn metadata<'a>(&self) -> Result<Cow<'a, [u8]>, Error>;
}

pub trait ApplyEvents {
    type Event;
    fn apply_events(&mut self, events: impl Iterator<Item = Self::Event>);
}

impl<Data> ApplyEvents for Component<Data>
where
    Data: ApplyEvents,
{
    type Event = <Data as ApplyEvents>::Event;
    #[inline]
    fn apply_events(&mut self, events: impl Iterator<Item = Self::Event>) {
        let mut data = self.data.write_arc();
        data.apply_events(events);
    }
}

pub trait ApplyMessages<'a> {
    fn apply_messages(&mut self, messages: impl Iterator<Item = Message<'a>>);
}

impl<'a, T, E> ApplyMessages<'a> for T
where
    E: From<Message<'a>>,
    T: ApplyEvents<Event = E>,
{
    #[inline]
    fn apply_messages(&mut self, messages: impl Iterator<Item = Message<'a>>) {
        let events = messages.map(Into::into);
        self.apply_events(events);
    }
}

pub struct ComponentCache<Data> {
    pub cache: Cache<Entity, Arc<RwLock<Data>>>,
}

impl<Data> ComponentCache<Data> {
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self { cache: Cache::new(10_000) }
    }
}

impl<Data> Default for ComponentCache<Data> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<Data> Deref for ComponentCache<Data> {
    type Target = Cache<Entity, Arc<RwLock<Data>>>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.cache
    }
}

pub struct EventDB {
    db_actor: ActorHandle,
}

impl EventDB {
    #[must_use]
    #[inline]
    pub fn new(db_actor: ActorHandle) -> Self {
        Self { db_actor }
    }

    /// Write the given event as a message in the database.
    ///
    /// # Errors
    ///
    /// This function will return an error if there is an error in serializing
    /// the data or metadata of the event.
    #[inline]
    pub async fn put(
        &self,
        stream_name: &str,
        event: &impl Event,
        expected_version: Option<Version>,
    ) -> Result<Position, Error> {
        // let msg = event.into();
        debug!(stream_name, ?expected_version, "putting message");
        let stream_name = stream_name.to_string().into();
        let data = event.data().map_err(Error::external_to_string)?;
        let metadata = event.metadata().map_err(Error::external_to_string)?;
        let req = WriteMessage {
            id: Id::new(),
            stream_name,
            message_type: event.name(),
            data,
            metadata,
            expected_stream_position: expected_version.map(Into::into),
        };
        let put_res = self.db_actor.put_message(req);
        put_res.await.map_err(Error::from)
        // Ok(Position { global: 0, stream: StreamPos::Sequential(0) })
    }
}

// pub struct ComponentStore<Data, Conn> {
pub struct ComponentStore<Data, Db> {
    cache: ComponentCache<Data>,
    event_db: Db,
}

impl<Data, Db: AsRef<EventDB>> ComponentStore<Data, Db> {
    #[must_use]
    #[inline]
    // pub fn new(db: Arc<EventDB<Conn>>) -> Self {
    pub fn new(event_db: Db) -> Self {
        Self { cache: ComponentCache::new(), event_db }
    }
    /// Fetch messages from the event store.
    ///
    /// # Errors
    ///
    /// This function will return an error if fetching messages errors.
    #[inline]
    pub async fn fetch<'msg>(
        &self,
        entity: Entity,
        stream_name: &str,
    ) -> Result<Component<Data>, Error>
    where
        Component<Data>: ApplyMessages<'msg>,
        Data: Default + Send + Sync,
    {
        if let Some(cached) = self.cache.get(&entity) {
            return Ok(Component { entity, data: cached });
        }
        let mut comp = Component::<Data>::new(entity);
        let fetch = self.event_db.as_ref().db_actor.fetch_messages(
            mess_db::read::GetMessages::default().in_stream(stream_name),
        );
        let messages = fetch.await?;
        let messages = messages.into_iter().filter_map(|res| {
            if let Err(err) = &res {
                eprintln!("message error: {err:?}");
            }
            res.map(Into::into).ok()
        });
        comp.apply_messages(messages);
        Ok(comp)
    }
}
