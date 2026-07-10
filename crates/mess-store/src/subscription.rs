//! The app-facing subscription: catch-up history → live tail, over the
//! [`EventStore`](crate::EventStore) facade.
//!
//! This is the store-level counterpart of `mess-log`'s D11 subscription
//! runtime (`mess_log::subscription`). It reuses the *same* commit-notification
//! primitive — the durable [`Watermark`](mess_log::watermark::Watermark),
//! surfaced through [`SubscribeBackend`] — rather than duplicating a signalling
//! system, but it serves **history from the app-facing read path**
//! ([`Backend::read_global`](crate::backend::Backend::read_global), i.e. the
//! record book with materialised payloads and stream names) so a delivered
//! [`StoredRecord`] is exactly what every other read returns.
//!
//! # Catch-up → live handoff
//!
//! A subscription is a cursor `c` over the **global** position sequence: the
//! next global position it will deliver.
//! [`next_batch`](Subscription::next_batch) runs one step of the D11 state
//! machine:
//!
//! 1. **Catch-up.** Read a page of history from `c` via `read_global`. If it is
//!    non-empty, advance `c` past it and hand it back — the subscriber is
//!    replaying committed history and never blocks.
//! 2. **Live tail.** An empty page proves `c` reached the watermark. Park on
//!    [`await_watermark_past(c)`](SubscribeBackend::await_watermark_past) — an
//!    **event-bounded** wait woken by the next commit that passes `c`, *not* a
//!    poll — then loop back to step 1, where the read now returns the freshly
//!    committed positions.
//!
//! History is authoritative; the watermark is only the wake signal. There is no
//! window in which a committed position is in neither source: the watermark is
//! advanced (by the backend) only after the position is resident in the read
//! path `read_global` serves, and the subscriber always re-reads history after
//! a wake. So a subscription started at `c` delivers **exactly** the positions
//! `c, c+1, …` as they commit — gap-free, in ascending global order, each once.
//!
//! # Delivery guarantees
//!
//! - **Gap-free & in-order.** Each batch is a dense ascending run of global
//!   positions beginning at the cursor; the cursor advances by exactly the
//!   batch length. No position is skipped or reordered.
//! - **At-least-once framing, exactly-once positions.** A delivered position is
//!   never re-delivered by the same subscription. (A subscription does not
//!   persist its cursor; a consumer that wants resumption records the last
//!   delivered [`StoredRecord::global_position`] and re-subscribes `from` the
//!   next one.)
//! - **Live is not busy-polling.** While caught up, the subscription is parked
//!   on the watermark and consumes no CPU until a commit wakes it.
//!
//! # Cancellation / drop
//!
//! [`next_batch`](Subscription::next_batch) and [`next`](Subscription::next)
//! are cancellation-safe: dropping the returned future before it resolves
//! leaves the cursor unchanged (nothing was consumed) and deregisters the
//! watermark waiter. Dropping the whole [`Subscription`] simply releases its
//! backend handle and any parked waiter — it can **never** wedge the committer,
//! because the watermark drains and re-wakes its waiter set on every advance
//! regardless of which waiters are still alive. A store supports one writer and
//! many independent subscribers this way; subscribers come and go freely.

use std::collections::VecDeque;

use crate::backend::{Backend, StoredRecord, SubscribeBackend};
use crate::store::StoreError;

/// A live catch-up → tail subscription over an
/// [`EventStore`](crate::EventStore).
///
/// Build one with [`EventStore::subscribe`](crate::EventStore::subscribe). Pull
/// committed [`StoredRecord`]s in global order with
/// [`next_batch`](Self::next_batch) (a page at a time) or [`next`](Self::next)
/// (one at a time); both block — event-bounded — once caught up, until the next
/// commit. See the [module docs](self) for the full delivery contract.
pub struct Subscription<B: Backend> {
    backend:   B,
    /// Next global position to deliver.
    cursor:    u64,
    /// Catch-up page size for `read_global`.
    page_size: usize,
    /// One-at-a-time buffer for [`next`](Self::next): the unread tail of the
    /// last page fetched by `next`. Empty except while a `next` walk is in
    /// progress.
    buffered:  VecDeque<StoredRecord>,
}

impl<B: SubscribeBackend> Subscription<B> {
    /// Create a subscription whose first delivered global position is `from`.
    /// Internal — callers use
    /// [`EventStore::subscribe`](crate::EventStore::subscribe).
    pub(crate) fn new(backend: B, from: u64, page_size: usize) -> Self {
        Subscription {
            backend,
            cursor: from,
            page_size: page_size.max(1),
            buffered: VecDeque::new(),
        }
    }

    /// The next global position this subscription will deliver — its cursor.
    /// After delivering position `p` this reads `p + 1`.
    #[must_use]
    pub fn position(&self) -> u64 { self.cursor }

    /// Deliver the next non-empty batch of committed records, in ascending
    /// global order starting at the cursor.
    ///
    /// Replays history a page at a time until caught up, then parks
    /// (event-bounded) on the watermark and returns the next committed page as
    /// soon as a writer commits past the cursor. Never returns an empty batch:
    /// it blocks until at least one record is available (or the backend
    /// errors).
    ///
    /// Cancellation-safe — see the [module docs](self).
    pub async fn next_batch(
        &mut self,
    ) -> Result<Vec<StoredRecord>, StoreError<B::Error>> {
        // Drain any records a prior `next` walk fetched but did not hand out,
        // so `next` and `next_batch` can be interleaved without dropping
        // events.
        if !self.buffered.is_empty() {
            return Ok(self.buffered.drain(..).collect());
        }
        loop {
            // `read_global(after)` is exclusive of `after`; deliver-from
            // `cursor` means read strictly after `cursor - 1` (or
            // from the start at 0).
            let after = self.cursor.checked_sub(1);
            let page = self
                .backend
                .read_global(after, self.page_size)
                .await
                .map_err(StoreError::Backend)?;
            if let Some(last) = page.last() {
                debug_assert_eq!(
                    page[0].global_position, self.cursor,
                    "read_global must return a dense run beginning at the \
                     cursor",
                );
                self.cursor = last.global_position + 1;
                return Ok(page);
            }
            // Caught up to the watermark: block until a commit passes the
            // cursor, then loop to read the newly-committed
            // positions from history.
            self.backend
                .await_watermark_past(self.cursor)
                .await
                .map_err(StoreError::Backend)?;
        }
    }

    /// Deliver the next single committed record, in global order.
    ///
    /// A thin buffered convenience over [`next_batch`](Self::next_batch): it
    /// fetches a page when its buffer runs dry and hands records out one at a
    /// time. Same blocking / cancellation semantics as
    /// [`next_batch`](Self::next_batch).
    pub async fn next(&mut self) -> Result<StoredRecord, StoreError<B::Error>> {
        if let Some(rec) = self.buffered.pop_front() {
            return Ok(rec);
        }
        let mut page: VecDeque<StoredRecord> = self.next_batch().await?.into();
        let first =
            page.pop_front().expect("next_batch never returns an empty batch");
        self.buffered = page;
        Ok(first)
    }
}
