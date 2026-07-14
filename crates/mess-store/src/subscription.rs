//! The app-facing subscription: catch-up history → live tail, over the
//! [`EventStore`](crate::EventStore) facade.
//!
//! This is the store-level counterpart of `mess-log`'s D11 subscription
//! runtime (`mess_log::subscription`). It reuses the *same* commit-notification
//! primitive — [`Watermark`](mess_log::watermark::Watermark), surfaced through
//! [`SubscribeBackend`] — rather than duplicating a signalling system. For
//! [`LogEngine`](crate::LogEngine) this is the **published read watermark**,
//! which advances after the direct owner's durable watermark, once the index
//! tiers can serve the covered positions. The subscription serves **history
//! from that same app-facing read path**
//! ([`Backend::read_global`](crate::backend::Backend::read_global), i.e. the
//! record book with materialised payloads and stream names) so a delivered
//! [`StoredRecord`] is exactly what every other read returns.
//!
//! # Catch-up → live handoff
//!
//! A subscription is a cursor `c` over the **canonical global** position
//! sequence: the next position it will scan. The cursor is an opaque monotone
//! resume token, not a dense index into application events.
//! [`next_batch`](Subscription::next_batch) runs one step of the D11 state
//! machine:
//!
//! 1. **Catch-up.** Read a page of history from `c` via `read_global`. If it is
//!    non-empty, advance `c` past it and hand it back — the subscriber is
//!    replaying committed history and never blocks.
//! 2. **Live tail.** An empty page proves `c` reached the published read
//!    watermark. Park on
//!    [`await_watermark_past(c)`](SubscribeBackend::await_watermark_past) — an
//!    **event-bounded** wait woken by the next commit that passes `c`, *not* a
//!    poll — then loop back to step 1, where the read now returns the freshly
//!    committed positions.
//!
//! History is authoritative; the watermark is only the wake signal. There is no
//! window in which a committed application record is in neither source: the
//! watermark is advanced (by the backend) only after the covered canonical
//! positions are resident in the read path `read_global` serves, and the
//! subscriber always re-reads history after a wake.
//!
//! The v3 [`LogEngine`](crate::LogEngine) also assigns canonical positions to
//! `$registry` events and then filters those engine records from this
//! application-facing path. A subscription therefore delivers every visible
//! record at or after `c`, once and in ascending order, but it does **not**
//! promise consecutive numeric positions. [`GlobalPage::frontier`] advances
//! the scan cursor safely across filtered-only ranges.
//!
//! # Delivery guarantees
//!
//! - **Complete & in-order.** No application-visible record at or after the
//!   starting cursor is skipped or reordered. Numeric gaps are expected when
//!   engine records occupy the intervening canonical positions.
//! - **At-least-once framing, exactly-once records.** A delivered record is
//!   never re-delivered by the same subscription. A subscription does not
//!   persist a processing checkpoint. After successfully processing a
//!   `next_batch` page or a single `next` record, a consumer may persist
//!   [`position`](Subscription::position); while `next` has prefetched later
//!   records, `position()` reports the first buffered record rather than the
//!   farther-ahead internal scan frontier. Do not derive a backlog count from
//!   cursor subtraction.
//! - **Live is not busy-polling.** While caught up, the subscription is parked
//!   on the watermark and consumes no CPU until a commit wakes it.
//!
//! # Cancellation / drop
//!
//! [`next_batch`](Subscription::next_batch) and [`next`](Subscription::next)
//! are cancellation-safe for visible delivery: dropping the returned future
//! before it resolves never consumes or skips an application record. A scan
//! may already have advanced internally across filtered-only positions before
//! it parks, and that progress is safe because nothing was delivered or
//! buffered there. Cancellation deregisters the watermark waiter. Dropping the
//! whole [`Subscription`] simply releases its backend handle and any parked
//! waiter — it can **never** wedge the committer,
//! because the watermark drains and re-wakes its waiter set on every advance
//! regardless of which waiters are still alive. A store supports one writer and
//! many independent subscribers this way; subscribers come and go freely.

use std::collections::VecDeque;

use crate::backend::{Backend, GlobalPage, StoredRecord, SubscribeBackend};
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
    /// Next canonical global position to scan.
    cursor:    u64,
    /// Whether the starting/resumed cursor has been checked against the
    /// current published log end (SUB10).
    validated: bool,
    /// Catch-up page size for `read_global`.
    page_size: usize,
    /// One-at-a-time buffer for [`next`](Self::next): the unread tail of the
    /// last page fetched by `next`. Empty except while a `next` walk is in
    /// progress.
    buffered:  VecDeque<StoredRecord>,
}

impl<B: SubscribeBackend> Subscription<B> {
    /// Create a subscription whose first canonical position scanned is `from`;
    /// it delivers the first application-visible record at or after that
    /// position. Internal — callers use
    /// [`EventStore::subscribe`](crate::EventStore::subscribe).
    pub(crate) fn new(backend: B, from: u64, page_size: usize) -> Self {
        Subscription {
            backend,
            cursor: from,
            validated: false,
            page_size: page_size.max(1),
            buffered: VecDeque::new(),
        }
    }

    /// The safe next-to-deliver resume cursor for records already handed to
    /// the caller.
    ///
    /// It can point at a filtered `$registry` position and can advance past
    /// positions that were not delivered. [`next`](Self::next) may prefetch a
    /// whole page and move the internal scan frontier farther ahead; while its
    /// private buffer is non-empty, this method instead returns the first
    /// buffered record's position so persisting it cannot skip unread records.
    ///
    /// Persist this value only after successfully processing the entire
    /// `next_batch` result or the preceding single `next` result, then pass it
    /// back to [`EventStore::subscribe`](crate::EventStore::subscribe). It is
    /// not an application-event count or dense index.
    #[must_use]
    pub fn position(&self) -> u64 {
        self.buffered
            .front()
            .map_or(self.cursor, |record| record.global_position)
    }

    /// Deliver the next non-empty batch of committed application records, in
    /// ascending canonical global order at or after the cursor.
    ///
    /// Replays history a page at a time until caught up, then parks
    /// (event-bounded) on the watermark and returns the next committed page as
    /// soon as a writer commits past the cursor. Never returns an empty batch:
    /// it blocks until at least one record is available (or the backend
    /// errors). Consecutive returned records may have non-consecutive global
    /// positions because engine-internal records are filtered.
    ///
    /// On the first pull, a resumed cursor beyond the current published log
    /// end returns [`StoreError::CursorRegressed`] rather than hanging or
    /// rewinding silently.
    ///
    /// Cancellation-safe — see the [module docs](self).
    pub async fn next_batch(
        &mut self,
    ) -> Result<Vec<StoredRecord>, StoreError<B::Error>> {
        if !self.validated {
            let log_end =
                self.backend.watermark().await.map_err(StoreError::Backend)?;
            if self.cursor > log_end {
                return Err(StoreError::CursorRegressed {
                    cursor: self.cursor,
                    log_end,
                });
            }
            self.validated = true;
        }
        // Drain any records a prior `next` walk fetched but did not hand out,
        // so `next` and `next_batch` can be interleaved without dropping
        // events.
        if !self.buffered.is_empty() {
            return Ok(self.buffered.drain(..).collect());
        }
        loop {
            // `read_global_page(after)` is exclusive of `after`; deliver-from
            // `cursor` means read strictly after `cursor - 1` (or
            // from the start at 0).
            let after = self.cursor.checked_sub(1);
            let page = self
                .backend
                .read_global_page(after, self.page_size)
                .await
                .map_err(StoreError::Backend)?;
            // `bn-2di`: the delivered sequence is no longer necessarily dense.
            // The engine's `$registry` records consume global positions but are
            // never delivered, so `page.records[0]` may sit past the cursor and
            // a page may legitimately come back EMPTY with positions still
            // below the watermark. `frontier` — how far the scan actually got —
            // is what makes both cases safe:
            //
            //   * a non-empty page: advance past the last delivered record, but
            //     never behind the frontier (trailing skipped positions must
            //     not be re-scanned on the next call);
            //   * an empty page whose frontier moved: the scan crossed a run of
            //     engine-internal positions and found nothing deliverable. Take
            //     the frontier and loop — WITHOUT parking on the watermark,
            //     which is already past the cursor and would spin.
            //
            // What we may never do is jump the cursor to the watermark: a page
            // can also end early because it hit `page_size` or a raced tier
            // handoff, and the frontier is precisely the value that
            // distinguishes "examined and empty" from "not examined yet".
            let GlobalPage { records, frontier } = page;
            if let Some(last) = records.last() {
                debug_assert!(
                    records[0].global_position >= self.cursor,
                    "read_global_page must not return records before the \
                     cursor",
                );
                self.cursor =
                    last.global_position.saturating_add(1).max(frontier);
                return Ok(records);
            }
            if frontier > self.cursor {
                // Skipped-only run: make progress, then re-read.
                self.cursor = frontier;
                continue;
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
    /// time. Although the internal scan cursor advances for the fetched page,
    /// [`position`](Self::position) accounts for the unread buffer and remains
    /// a safe resume cursor after the returned record has been processed. Same
    /// blocking / cancellation semantics as [`next_batch`](Self::next_batch).
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
