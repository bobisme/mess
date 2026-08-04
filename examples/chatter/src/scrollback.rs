//! Scroll-back: paging **backward** through a channel's history.
//!
//! This is the read path the sealed tier exists for. A channel that has been
//! alive for a while has most of its history in **sealed** segments, so the
//! oldest pages of a scroll-back are served through the sealed payload
//! accelerator (the SealPack's payload section, or a loose `.pcol`) rather than
//! from the active segment. `examples/social` cannot reach that path at all —
//! its streams are one event long, so every read is a hot read.
//!
//! # Why backward paging is a *read_stream* problem, not a projection problem
//!
//! The read model ([`crate::projections`]) deliberately keeps only a bounded
//! recent window, because keeping every message would put the whole corpus in
//! RAM and in the checkpoint. History therefore comes from the log itself,
//! which is exactly right: the log is the authority, and a chat client
//! scrolling up is asking the log a question about a range of one stream.
//!
//! [`Backend::read_stream`] reads *forward* from an exclusive cursor, so a
//! backward pager works out the position range of the page it wants and reads
//! that range forward, then presents it newest-first. Each page is one
//! bounded read: paging back through a 50,000-message channel never
//! materialises 50,000 records.
//!
//! # Reactions
//!
//! A channel stream interleaves messages and reactions. A page attaches every
//! reaction whose target message happens to fall in the same page and reports
//! the rest as [`carried_reactions`](ScrollPage::carried_reactions) — reactions
//! to older messages, which a UI would fold into the message when the reader
//! scrolls far enough to see it.

use mess_core::Event;
use mess_store::{Backend, EventStore, Version};

use crate::domain::channel::ChannelEvent;
use crate::{Id, channel_stream};

/// One message as rendered by a scroll-back page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScrollMessage {
    /// The channel-local ordinal stamped by the aggregate.
    pub ordinal:         u64,
    pub author:          Id,
    pub body:            String,
    /// 0-based position of this event within the channel stream.
    pub stream_position: u64,
    /// Canonical global position.
    pub global_position: u64,
    /// Reactions to this message that appeared in the same page.
    pub reactions:       Vec<(Id, String)>,
}

/// One page of scroll-back, newest first.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ScrollPage {
    /// Messages in the page, **newest first**.
    pub messages:          Vec<ScrollMessage>,
    /// Reactions in this page whose target message is older than the page.
    pub carried_reactions: usize,
    /// Raw records read for this page (messages + reactions + lifecycle
    /// events) — the honest cost of the page.
    pub records:           usize,
    /// The exclusive upper bound of the next, older page. `None` means this
    /// page reached the start of the stream.
    pub next_before:       Option<u64>,
}

/// Failure reading a channel's history. The backend's error is stringified at
/// this seam so callers do not name the concrete engine error type.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("reading {stream}: {message}")]
pub struct ScrollError {
    pub stream:  String,
    pub message: String,
}

/// A backward pager over one channel's stream.
///
/// Construct with [`open`](ScrollBack::open) (which reads the stream head),
/// then call [`next_page`](ScrollBack::next_page) repeatedly. Each call returns
/// the next older page and advances the cursor; the last page reports
/// `next_before == None`.
#[derive(Debug)]
pub struct ScrollBack<'a, B: Backend> {
    store:  &'a EventStore<B>,
    stream: String,
    page:   u64,
    /// Exclusive upper bound: the next page covers stream positions
    /// `[before - page, before)`. `0` means the whole stream has been paged.
    before: u64,
}

impl<'a, B: Backend> ScrollBack<'a, B>
where
    B::Error: std::fmt::Display,
{
    /// Open a pager positioned at the **end** of `channel`'s stream.
    pub async fn open(
        store: &'a EventStore<B>,
        channel: Id,
        page: usize,
    ) -> Result<Self, ScrollError> {
        Self::open_stream(store, channel_stream(channel), page).await
    }

    /// [`open`](Self::open) for an explicit stream id.
    pub async fn open_stream(
        store: &'a EventStore<B>,
        stream: String,
        page: usize,
    ) -> Result<Self, ScrollError> {
        let head = store.backend().head(&stream).await.map_err(|e| {
            ScrollError { stream: stream.clone(), message: e.to_string() }
        })?;
        Ok(Self {
            store,
            stream,
            page: (page.max(1)) as u64,
            // `next_position()` is one past the last event: the exclusive
            // upper bound of the newest page.
            before: head.next_position(),
        })
    }

    /// The stream this pager reads.
    #[must_use]
    pub fn stream(&self) -> &str { &self.stream }

    /// Whether any older page remains.
    #[must_use]
    pub fn has_more(&self) -> bool { self.before > 0 }

    /// The exclusive upper bound of the next page.
    #[must_use]
    pub fn cursor(&self) -> u64 { self.before }

    /// Read the next (older) page. Returns an empty page once the start of the
    /// stream has been reached.
    pub async fn next_page(&mut self) -> Result<ScrollPage, ScrollError> {
        if self.before == 0 {
            return Ok(ScrollPage::default());
        }
        let start = self.before.saturating_sub(self.page);
        let limit = (self.before - start) as usize;
        // `read_stream` is exclusive of `after`, so a page starting at stream
        // position `start` reads after `start - 1` (or from the very start).
        let after = match start.checked_sub(1) {
            Some(prev) => Version::At(prev),
            None => Version::NoStream,
        };
        let records = self
            .store
            .backend()
            .read_stream(&self.stream, after, limit)
            .await
            .map_err(|e| ScrollError {
                stream:  self.stream.clone(),
                message: e.to_string(),
            })?;

        let mut messages: Vec<ScrollMessage> = Vec::new();
        let mut pending: Vec<(u64, Id, String)> = Vec::new();
        let raw = records.len();
        for rec in &records {
            // A page is a slice of ONE stream, so anything that fails to
            // decode is a schema drift, not a routing question: skip it and
            // keep the page readable.
            let Ok(ev) = ChannelEvent::decode(&rec.message_type, &rec.data)
            else {
                continue;
            };
            match ev {
                ChannelEvent::MessagePosted { ordinal, author, body } => {
                    messages.push(ScrollMessage {
                        ordinal,
                        author,
                        body,
                        stream_position: rec.stream_position,
                        global_position: rec.global_position,
                        reactions: Vec::new(),
                    });
                }
                ChannelEvent::ReactionAdded { target, by, emoji } => {
                    pending.push((target, by, emoji));
                }
                ChannelEvent::Created { .. } | ChannelEvent::Archived => {}
            }
        }
        // Attach the reactions whose target is in this page; count the rest.
        let mut carried = 0usize;
        for (target, by, emoji) in pending {
            match messages.iter_mut().find(|m| m.ordinal == target) {
                Some(m) => m.reactions.push((by, emoji)),
                None => carried += 1,
            }
        }
        // Newest first, the direction a reader scrolls.
        messages.reverse();

        self.before = start;
        Ok(ScrollPage {
            messages,
            carried_reactions: carried,
            records: raw,
            next_before: (start > 0).then_some(start),
        })
    }
}

#[cfg(test)]
mod tests {
    use mess_store::{LogEngine, PackSnapshotBackend};
    use mess_testkit::sweeping_temp_dir;

    use super::*;
    use crate::ops::WriteOps;

    #[tokio::test]
    async fn pages_backward_through_a_channel_in_order() {
        let dir = sweeping_temp_dir("chatter-scrollback");
        let engine =
            LogEngine::open(dir.path().join("log")).expect("open engine");
        let backend =
            PackSnapshotBackend::open(engine, dir.path().join("snap"))
                .expect("open snapshot backend");
        let store = EventStore::new(backend);

        let channel = Id::from_parts(1, [9; 10]);
        let author = Id::from_parts(1, [8; 10]);
        store
            .create_channel(channel, "general".into(), "all".into())
            .await
            .unwrap();
        for i in 0..25u64 {
            store
                .post_message(channel, author, format!("message {i}"))
                .await
                .unwrap();
        }
        store.add_reaction(channel, 24, author, "🎉".into()).await.unwrap();
        store.add_reaction(channel, 0, author, "+1".into()).await.unwrap();

        let mut back = ScrollBack::open(&store, channel, 10).await.unwrap();
        let mut seen: Vec<u64> = Vec::new();
        let mut carried = 0usize;
        while back.has_more() {
            let page = back.next_page().await.unwrap();
            // Newest-first within a page.
            let ordinals: Vec<u64> =
                page.messages.iter().map(|m| m.ordinal).collect();
            let mut sorted = ordinals.clone();
            sorted.sort_unstable_by(|a, b| b.cmp(a));
            assert_eq!(ordinals, sorted, "a page must be newest-first");
            carried += page.carried_reactions;
            seen.extend(ordinals);
        }
        // Every message, exactly once, newest to oldest overall.
        assert_eq!(seen.len(), 25);
        assert_eq!(seen, (0..25u64).rev().collect::<Vec<_>>());
        // The reaction on ordinal 0 lives in the newest page (it was appended
        // last), far from its target — so it is carried, not attached.
        assert_eq!(carried, 1);
        let empty = back.next_page().await.unwrap();
        assert!(empty.messages.is_empty());
        assert_eq!(empty.next_before, None);
    }

    #[tokio::test]
    async fn an_empty_channel_pages_to_nothing() {
        let dir = sweeping_temp_dir("chatter-scrollback-empty");
        let engine =
            LogEngine::open(dir.path().join("log")).expect("open engine");
        let backend =
            PackSnapshotBackend::open(engine, dir.path().join("snap"))
                .expect("open snapshot backend");
        let store = EventStore::new(backend);
        let channel = Id::from_parts(1, [3; 10]);
        let mut back = ScrollBack::open(&store, channel, 8).await.unwrap();
        assert!(!back.has_more());
        assert_eq!(back.next_page().await.unwrap(), ScrollPage::default());
    }
}
