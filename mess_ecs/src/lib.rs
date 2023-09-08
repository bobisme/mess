#![warn(
    clippy::pedantic,
    // clippy::nursery,
    clippy::missing_inline_in_public_items
)]

pub mod error;
pub mod streams;

use std::{borrow::Cow, fmt::Display, ops::Deref, sync::Arc};

use crate::error::Error;
use ident::Id;
use mess_db::{svc::Connection, Message, Position, StreamPos};
use parking_lot::RwLock;
use quick_cache::sync::Cache;

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

pub enum Version {
    Sequential(u64),
    Relaxed(u64),
}

impl From<Version> for StreamPos {
    #[inline]
    fn from(value: Version) -> Self {
        match value {
            Version::Sequential(x) => Self::Serial(x),
            Version::Relaxed(x) => Self::Causal(x),
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
    fn data<'a>(&self) -> Cow<'a, [u8]>;
    fn metadata<'a>(&self) -> Cow<'a, [u8]>;
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

pub trait ApplyMessages {
    fn apply_messages<'a>(
        &mut self,
        messages: impl Iterator<Item = Message<'a>>,
    );
}

impl<Data> ApplyMessages for Component<Data>
where
    Data: ApplyMessages,
{
    #[inline]
    fn apply_messages<'a>(
        &mut self,
        messages: impl Iterator<Item = Message<'a>>,
    ) {
        // let events = messages.map(|m| m.into());
        // self.data.apply_(events);
        self.data.write_arc().apply_messages(messages);
    }
}
//
// impl<'b, T, E> ApplyMessages for T
// where
//     T: ApplyEvents<Event = E>,
//     E: From<Message<'b>>,
// {
//     fn apply_messages<'a>(
//         &mut self,
//         messages: impl Iterator<Item = Message<'a>>,
//     ) {
//         let events = messages.map(|m| m.into());
//         self.apply_events(events)
//     }
// }

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

pub struct ComponentStore<'a, Data> {
    cache: ComponentCache<Data>,
    db: Arc<Connection<'a>>,
}

impl<'a, Data> ComponentStore<'a, Data> {
    #[must_use]
    #[inline]
    pub fn new(db: Arc<Connection<'a>>) -> Self {
        Self { cache: ComponentCache::new(), db }
    }
}

impl<'a: 'b, 'b, Data> ComponentStore<'b, Data> {
    /// Fetch messages from the event store.
    ///
    /// # Errors
    ///
    /// This function will return an error if fetching messages errors.
    #[inline]
    pub async fn fetch(
        &'a self,
        entity: Entity,
        stream_name: &'a str,
    ) -> Result<Component<Data>, Error>
    where
        Data: Default + Send + Sync + ApplyMessages,
    {
        if let Some(cached) = self.cache.get(&entity) {
            return Ok(Component { entity, data: cached });
        }
        let mut comp = Component::<Data>::new(entity);
        let fetch = self.db.fetch_messages(
            mess_db::read::GetMessages::default().in_stream(stream_name),
        );
        let messages = fetch.await?;
        comp.apply_messages(messages);
        Ok(comp)
    }

    #[inline]
    pub async fn put<E>(
        &'a self,
        stream_name: &'a str,
        event: &E,
        expected_version: Version,
    ) -> Result<Position, Error>
    where
        Data: Default + Send + Sync + ApplyMessages,
        E: Event,
    {
        // let msg = event.into();
        // let req = WriteMessage {
        //     id: Id::new(),
        //     stream_name: stream_name.into(),
        //     message_type: event.name(),
        //     data: event.data(),
        //     metadata: event.metadata(),
        //     expected_stream_position,
        // };
        // self.db.put_message(req)
        Ok(Position { global: 0, stream: StreamPos::Serial(0) })
    }
}
