//! A live subscriber written **directly against
//! [`Backend::read_global_page`]**, so the SUB2 cursor rules are visible in the
//! example rather than hidden inside
//! [`EventStore::subscribe`](mess_store::EventStore::subscribe).
//!
//! `mess-store` already ships a correct subscription
//! ([`mess_store::Subscription`], which [`crate::projections`] uses). This
//! module exists because the two rules it encodes are the ones application
//! authors get wrong, and an example should show them being obeyed:
//!
//! 1. **`read_global_page(after)` is exclusive of `after`.** To deliver *from*
//!    cursor `c`, you read after `c - 1` — never after `c`, which would
//!    silently drop the record at `c`. [`Tailer::poll_batch`] does
//!    `self.cursor.checked_sub(1)`, which also handles `c == 0` (read from the
//!    very beginning) without an off-by-one.
//! 2. **Advance by the frontier, not by the last record.** The engine assigns
//!    canonical global positions to its own `$registry` records and then
//!    filters them from application reads, so a page can come back *empty*
//!    while positions below the watermark remain, and a non-empty page can end
//!    with skipped positions after its last record.
//!    [`GlobalPage::frontier`](mess_store::GlobalPage::frontier) is how far the
//!    scan actually got: resume there, or a consumer either re-scans a hole
//!    forever or jumps past a record it never read.
//!
//! What a consumer must never do is jump the cursor to the watermark on an
//! empty page: a page can also end early because it hit `limit` or a tier
//! handoff, and only the frontier distinguishes "examined and empty" from "not
//! examined yet".

use mess_store::{
    Backend, EventStore, GlobalPage, StoredRecord, SubscribeBackend,
};

use crate::domain::channel::ChannelEvent;
use crate::domain::user::UserEvent;

/// Failure pulling from the log. The backend's error is stringified at this
/// seam.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("tailing the global log: {0}")]
pub struct TailError(pub String);

/// A catch-up → live tail over the global log.
///
/// Cheap to construct and cheap to drop; it holds a clone of the backend handle
/// and an opaque cursor. Persist [`position`](Self::position) after processing
/// a batch and pass it back to [`Tailer::from`] to resume.
#[derive(Debug)]
pub struct Tailer<B: Backend> {
    backend: B,
    /// The next canonical global position to scan.
    cursor:  u64,
    page:    usize,
}

impl<B: SubscribeBackend + Clone> Tailer<B> {
    /// A tailer whose first scanned position is `from` (`0` for the whole
    /// log).
    #[must_use]
    pub fn from(store: &EventStore<B>, from: u64, page: usize) -> Self {
        Self {
            backend: store.backend().clone(),
            cursor:  from,
            page:    page.max(1),
        }
    }

    /// The safe resume cursor: the next position that will be scanned. Persist
    /// it only after the previous batch has been fully processed.
    #[must_use]
    pub fn position(&self) -> u64 { self.cursor }

    /// Read **one** page, without blocking on the watermark.
    ///
    /// Returns the delivered records (possibly empty when the tail is caught up
    /// or crossed a run of engine-internal positions) and advances the cursor
    /// by the frontier rule. This is the method that encodes both SUB2 rules —
    /// see the module docs.
    pub async fn poll_batch(&mut self) -> Result<Vec<StoredRecord>, TailError> {
        // RULE 1: `read_global_page(after)` is exclusive of `after`, so
        // delivering FROM `cursor` means reading strictly after `cursor - 1`.
        // `checked_sub` gives `None` at cursor 0, which reads from the start.
        let after = self.cursor.checked_sub(1);
        let GlobalPage { records, frontier } = self
            .backend
            .read_global_page(after, self.page)
            .await
            .map_err(|e| TailError(e.to_string()))?;
        // RULE 2: advance past the last delivered record, but never behind the
        // frontier — trailing positions the scan examined and filtered must not
        // be re-scanned. On an empty page the frontier alone moves the cursor.
        self.cursor = match records.last() {
            Some(last) => last.global_position.saturating_add(1).max(frontier),
            None => frontier.max(self.cursor),
        };
        Ok(records)
    }

    /// Deliver the next non-empty batch, **blocking** (event-bounded, parked on
    /// the store watermark) until a writer commits something visible.
    ///
    /// Never returns an empty batch: an empty page whose frontier moved is a
    /// skipped-only run and is retried immediately, without parking; an empty
    /// page whose frontier did not move means the tail is genuinely caught up,
    /// so it parks on the watermark instead of spinning.
    pub async fn next_batch(&mut self) -> Result<Vec<StoredRecord>, TailError> {
        loop {
            let before = self.cursor;
            let batch = self.poll_batch().await?;
            if !batch.is_empty() {
                return Ok(batch);
            }
            if self.cursor > before {
                // Skipped-only run: progress was made, re-read immediately.
                continue;
            }
            self.backend
                .await_watermark_past(self.cursor)
                .await
                .map_err(|e| TailError(e.to_string()))?;
        }
    }

    /// Drain everything already committed, calling `on_batch` for each page,
    /// and return the number of records delivered. Stops as soon as the tail is
    /// caught up — it never blocks, which is what makes it usable as a
    /// measurable "catch-up" cell.
    pub async fn catch_up(
        &mut self,
        mut on_batch: impl FnMut(&[StoredRecord]),
    ) -> Result<u64, TailError> {
        let mut delivered = 0u64;
        loop {
            let before = self.cursor;
            let batch = self.poll_batch().await?;
            if batch.is_empty() {
                if self.cursor > before {
                    // Crossed engine-internal positions; keep going.
                    continue;
                }
                return Ok(delivered);
            }
            delivered += batch.len() as u64;
            on_batch(&batch);
        }
    }
}

/// A one-line, human-readable rendering of a delivered record — what
/// `chatter tail` prints.
///
/// Records on streams this example does not model render as a terse `?` line
/// rather than being dropped: a tail should never lie about what is in the log.
#[must_use]
pub fn describe(rec: &StoredRecord) -> String {
    use mess_core::Event;

    let (category, suffix) = rec.category_and_suffix();
    let short = suffix.get(..8).unwrap_or(suffix);
    match category {
        "channel" => match ChannelEvent::decode(&rec.message_type, &rec.data) {
            Ok(ChannelEvent::Created { slug, .. }) => {
                format!("[{}] channel {slug} created", rec.global_position)
            }
            Ok(ChannelEvent::MessagePosted { ordinal, body, .. }) => {
                let preview: String = body.chars().take(60).collect();
                format!("[{}] {short}#{ordinal} {preview}", rec.global_position)
            }
            Ok(ChannelEvent::ReactionAdded { target, emoji, .. }) => {
                format!(
                    "[{}] {short}#{target} reaction {emoji}",
                    rec.global_position
                )
            }
            Ok(ChannelEvent::Archived) => {
                format!("[{}] {short} archived", rec.global_position)
            }
            Err(e) => format!(
                "[{}] {short} undecodable channel event {:?}: {e}",
                rec.global_position, rec.message_type
            ),
        },
        "user" => match UserEvent::decode(&rec.message_type, &rec.data) {
            Ok(UserEvent::Registered { handle, .. }) => {
                format!("[{}] user {handle} registered", rec.global_position)
            }
            Ok(UserEvent::DisplayNameChanged { display_name }) => {
                format!(
                    "[{}] user {short} renamed to {display_name}",
                    rec.global_position
                )
            }
            Err(e) => format!(
                "[{}] {short} undecodable user event {:?}: {e}",
                rec.global_position, rec.message_type
            ),
        },
        other => format!(
            "[{}] ? {other}-{short} {}",
            rec.global_position, rec.message_type
        ),
    }
}

#[cfg(test)]
mod tests {
    use mess_store::{LogEngine, PackSnapshotBackend};
    use mess_testkit::sweeping_temp_dir;

    use super::*;
    use crate::Id;
    use crate::ops::WriteOps;

    #[tokio::test]
    async fn catch_up_then_live_delivers_each_record_exactly_once() {
        let dir = sweeping_temp_dir("chatter-tail");
        let engine =
            LogEngine::open(dir.path().join("log")).expect("open engine");
        let backend =
            PackSnapshotBackend::open(engine, dir.path().join("snap"))
                .expect("open snapshot backend");
        let store = EventStore::new(backend);

        let channel = Id::from_parts(1, [4; 10]);
        let author = Id::from_parts(1, [5; 10]);
        store
            .create_channel(channel, "general".into(), "all".into())
            .await
            .unwrap();
        for i in 0..10u64 {
            store.post_message(channel, author, format!("m{i}")).await.unwrap();
        }

        let mut tail = Tailer::from(&store, 0, 4);
        let mut seen: Vec<u64> = Vec::new();
        let caught = tail
            .catch_up(|batch| {
                seen.extend(batch.iter().map(|r| r.global_position))
            })
            .await
            .unwrap();
        assert_eq!(caught, 11, "1 created + 10 messages");
        assert_eq!(seen.len(), 11);

        // The frontier the tail stopped at.
        let frontier = tail.position();

        // Append PAST the frontier and prove it is delivered exactly once.
        let pos = store
            .post_message(channel, author, "after the frontier".into())
            .await
            .unwrap();
        assert!(
            pos >= frontier,
            "the new record must sit at or past the tail frontier"
        );
        let batch = tail.next_batch().await.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].global_position, pos);

        // ...and never again: a second catch-up delivers nothing.
        let again = tail.catch_up(|_| {}).await.unwrap();
        assert_eq!(again, 0, "no record may be delivered twice");

        // Exclusivity, stated directly: reading after `pos` yields nothing,
        // reading after `pos - 1` yields exactly the record at `pos`.
        let page = store
            .backend()
            .read_global_page(Some(pos), 10)
            .await
            .expect("read_global_page");
        assert!(page.records.is_empty(), "after(pos) must exclude pos");
        let page = store
            .backend()
            .read_global_page(pos.checked_sub(1), 10)
            .await
            .expect("read_global_page");
        assert_eq!(page.records.first().map(|r| r.global_position), Some(pos));
    }

    #[tokio::test]
    async fn a_resumed_cursor_replays_nothing_it_already_delivered() {
        let dir = sweeping_temp_dir("chatter-tail-resume");
        let engine =
            LogEngine::open(dir.path().join("log")).expect("open engine");
        let backend =
            PackSnapshotBackend::open(engine, dir.path().join("snap"))
                .expect("open snapshot backend");
        let store = EventStore::new(backend);

        let channel = Id::from_parts(1, [6; 10]);
        let author = Id::from_parts(1, [7; 10]);
        store
            .create_channel(channel, "general".into(), "all".into())
            .await
            .unwrap();
        for i in 0..20u64 {
            store.post_message(channel, author, format!("m{i}")).await.unwrap();
        }

        let mut first = Tailer::from(&store, 0, 5);
        let mut seen_a: Vec<u64> = Vec::new();
        // Consume exactly one page, then persist the cursor.
        let batch = first.next_batch().await.unwrap();
        seen_a.extend(batch.iter().map(|r| r.global_position));
        let resume = first.position();

        let mut second = Tailer::from(&store, resume, 5);
        let mut seen_b: Vec<u64> = Vec::new();
        second
            .catch_up(|b| seen_b.extend(b.iter().map(|r| r.global_position)))
            .await
            .unwrap();

        let mut all = seen_a.clone();
        all.extend(&seen_b);
        assert_eq!(all.len(), 21, "1 created + 20 messages, no duplicates");
        let mut dedup = all.clone();
        dedup.sort_unstable();
        dedup.dedup();
        assert_eq!(dedup.len(), all.len(), "no record delivered twice");
        assert!(
            seen_b.iter().all(|p| !seen_a.contains(p)),
            "a resumed cursor must not replay"
        );
    }

    #[tokio::test]
    async fn describe_renders_every_modelled_event() {
        let dir = sweeping_temp_dir("chatter-tail-describe");
        let engine =
            LogEngine::open(dir.path().join("log")).expect("open engine");
        let backend =
            PackSnapshotBackend::open(engine, dir.path().join("snap"))
                .expect("open snapshot backend");
        let store = EventStore::new(backend);

        let channel = Id::from_parts(1, [1; 10]);
        let user = Id::from_parts(1, [2; 10]);
        store.register(user, "quiet_otter".into(), "Q O".into()).await.unwrap();
        store.set_display_name(user, "Quiet Otter".into()).await.unwrap();
        store
            .create_channel(channel, "deploys".into(), "ships".into())
            .await
            .unwrap();
        store.post_message(channel, user, "hello".into()).await.unwrap();
        store.add_reaction(channel, 0, user, "🎉".into()).await.unwrap();
        store.archive_channel(channel).await.unwrap();

        let mut tail = Tailer::from(&store, 0, 32);
        let mut lines: Vec<String> = Vec::new();
        tail.catch_up(|b| lines.extend(b.iter().map(describe))).await.unwrap();
        assert_eq!(lines.len(), 6);
        assert!(
            lines.iter().any(|l| l.contains("user quiet_otter registered"))
        );
        assert!(lines.iter().any(|l| l.contains("renamed to Quiet Otter")));
        assert!(lines.iter().any(|l| l.contains("channel deploys created")));
        assert!(lines.iter().any(|l| l.contains("#0 hello")));
        assert!(lines.iter().any(|l| l.contains("reaction 🎉")));
        assert!(lines.iter().any(|l| l.contains("archived")));
        assert!(
            !lines.iter().any(|l| l.contains("undecodable")),
            "every modelled event must render"
        );
    }
}
