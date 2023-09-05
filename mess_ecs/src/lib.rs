#![warn(
    clippy::pedantic,
    // clippy::nursery,
    clippy::missing_inline_in_public_items
)]

pub mod error;
pub mod streams;

use std::{ops::Deref, sync::Arc};

use crate::error::Error;
use ident::{Id, Identifiable};
use mess_db::{svc::Connection, Message};
use parking_lot::RwLock;
use quick_cache::sync::Cache;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Entity(Id);

impl Entity {
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self(Id::new())
    }
}

impl Identifiable for Entity {
    #[inline]
    fn id(&self) -> Id {
        self.0
    }
}

impl Default for Entity {
    #[inline]
    fn default() -> Self {
        Self::new()
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
        Self { entity, data: Arc::default() }
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

pub trait ApplyEvents {
    fn apply<'a>(&mut self, events: impl Iterator<Item = Message<'a>>);
}

impl<Data> ApplyEvents for Component<Data>
where
    Data: ApplyEvents,
{
    #[inline]
    fn apply<'a>(&mut self, events: impl Iterator<Item = Message<'a>>) {
        let mut data = self.data.write_arc();
        data.apply(events);
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
        Data: Default + Send + Sync + ApplyEvents,
    {
        if let Some(cached) = self.cache.get(&entity) {
            return Ok(Component { entity, data: cached });
        }
        let mut comp = Component::<Data>::new(entity);
        let fetch = self.db.fetch_messages(
            mess_db::svc::GetMessages::default().in_stream(stream_name),
        );
        let events = fetch.await?;
        comp.apply(events);
        Ok(comp)
    }
}
