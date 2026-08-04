//! The two read paths this example exists to exercise, against a store that
//! really is multi-segment:
//!
//! - **Scroll-back** — backward paging through a channel whose older history
//!   lives in sealed segments, so the oldest pages are served through the
//!   sealed payload accelerator rather than the active segment.
//! - **Tail** — a subscriber written directly against `read_global_page`, with
//!   the SUB2 exclusive-of-`after` cursor rule and frontier advancement.

use chatter::Id;
use chatter::ops::WriteOps;
use chatter::projections::Projections;
use chatter::scrollback::ScrollBack;
use chatter::seed::{self, SeedConfig};
use chatter::store_backend::{
    Store, checkpoint_path, create_store, sealed_census,
};
use chatter::tail::Tailer;
use mess_store::Backend;
use mess_testkit::{SweepingTempDir, sweeping_temp_dir};

/// A seeded, verified-multi-segment store.
async fn multi_segment_store(
    tag: &str,
    seed: u64,
) -> (SweepingTempDir, std::path::PathBuf, Store, seed::SeedReport) {
    let t = sweeping_temp_dir(tag);
    let dir = t.path().join("store");
    let cfg = SeedConfig::tiny(seed);
    let store = create_store(&dir, cfg.store_config()).expect("create store");
    let report = seed::generate(&store, &cfg).await;
    let census = sealed_census(&store, &dir);
    assert!(
        census.sealed_segments > 1,
        "these read-path tests are only meaningful over a multi-segment \
         store, got {} sealed segment(s)",
        census.sealed_segments
    );
    (t, dir, store, report)
}

#[tokio::test]
async fn scroll_back_walks_a_deep_channel_newest_to_oldest_exactly_once() {
    let (_t, _dir, store, report) =
        multi_segment_store("chatter-scroll-sealed", 31).await;
    let channel = report.deepest_channel_id.expect("a busiest channel");
    let depth = report.deepest_channel;
    assert!(depth > 100, "fixture sanity: a genuinely deep channel");

    let mut back = ScrollBack::open(&store, channel, 32).await.expect("open");
    let mut ordinals: Vec<u64> = Vec::new();
    let mut pages = 0usize;
    let mut attached = 0usize;
    while back.has_more() {
        let page = back.next_page().await.expect("page");
        pages += 1;
        attached +=
            page.messages.iter().map(|m| m.reactions.len()).sum::<usize>();
        // Within a page: strictly descending ordinals.
        let page_ordinals: Vec<u64> =
            page.messages.iter().map(|m| m.ordinal).collect();
        let mut sorted = page_ordinals.clone();
        sorted.sort_unstable_by(|a, b| b.cmp(a));
        assert_eq!(page_ordinals, sorted, "page {pages} was not newest-first");
        ordinals.extend(page_ordinals);
    }
    assert!(pages > 1, "a deep channel must take more than one page");
    assert_eq!(
        ordinals.len(),
        depth,
        "every message in the channel must appear exactly once"
    );
    // Newest to oldest, no gaps, no repeats: exactly `depth-1 .. 0`.
    assert_eq!(ordinals, (0..depth as u64).rev().collect::<Vec<_>>());
    assert!(
        attached > 0,
        "an interleaved corpus should attach some reactions to messages in \
         the same page"
    );
    // Past the start of the stream the pager is inert, not erroring.
    let empty = back.next_page().await.expect("page past the start");
    assert!(empty.messages.is_empty());
    assert_eq!(empty.next_before, None);
}

#[tokio::test]
async fn scroll_back_agrees_with_the_forward_read_of_the_same_stream() {
    // A differential: the backward pager must reconstruct exactly what a plain
    // forward `read_stream` sees, in reverse. If backward paging ever
    // mis-computed a range boundary, this diverges.
    let (_t, _dir, store, report) =
        multi_segment_store("chatter-scroll-diff", 32).await;
    let channel = report.deepest_channel_id.expect("a busiest channel");

    let mut forward: Vec<(u64, String)> = Vec::new();
    let stream = chatter::channel_stream(channel);
    let mut cursor = mess_store::Version::NoStream;
    loop {
        let page = store
            .backend()
            .read_stream(&stream, cursor, 256)
            .await
            .expect("read_stream");
        if page.is_empty() {
            break;
        }
        for rec in &page {
            if let Ok(chatter::ChannelEvent::MessagePosted {
                ordinal,
                body,
                ..
            }) = <chatter::ChannelEvent as mess_core::Event>::decode(
                &rec.message_type,
                &rec.data,
            ) {
                forward.push((ordinal, body));
            }
        }
        cursor = mess_store::Version::At(
            page.last().expect("non-empty").stream_position,
        );
    }

    let mut backward: Vec<(u64, String)> = Vec::new();
    let mut back = ScrollBack::open(&store, channel, 17).await.expect("open");
    while back.has_more() {
        let page = back.next_page().await.expect("page");
        for m in page.messages {
            backward.push((m.ordinal, m.body));
        }
    }
    backward.reverse();
    assert_eq!(
        forward, backward,
        "backward paging must reconstruct the forward read exactly"
    );
}

#[tokio::test]
async fn tail_delivers_a_post_frontier_append_exactly_once() {
    let (_t, dir, store, _report) =
        multi_segment_store("chatter-tail-sealed", 33).await;

    // Catch up over the whole (partly sealed) log.
    let mut tail = Tailer::from(&store, 0, 128);
    let mut seen: Vec<u64> = Vec::new();
    let delivered = tail
        .catch_up(|batch| seen.extend(batch.iter().map(|r| r.global_position)))
        .await
        .expect("catch up");
    assert!(delivered > 0);
    assert_eq!(delivered as usize, seen.len());
    let mut dedup = seen.clone();
    dedup.sort_unstable();
    dedup.dedup();
    assert_eq!(dedup.len(), seen.len(), "catch-up delivered a duplicate");
    let frontier = tail.position();

    // Append PAST the frontier.
    let proj =
        Projections::with_checkpoint(&store, checkpoint_path(&dir)).await;
    let channel = proj.channels().await.first().map(|c| c.id).expect("channel");
    let author = Id::from_parts(1, [77; 10]);
    let pos = store
        .post_message(channel, author, "past the frontier".into())
        .await
        .expect("post");
    assert!(pos >= frontier, "the append must land at or past the frontier");

    // Delivered exactly once...
    let batch = tail.next_batch().await.expect("next batch");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].global_position, pos);
    assert_eq!(
        tail.catch_up(|_| {}).await.expect("drain"),
        0,
        "no record may be delivered twice"
    );

    // ...and the SUB2 exclusivity rule the tailer is built on holds directly.
    let page = store
        .backend()
        .read_global_page(Some(pos), 8)
        .await
        .expect("read_global_page");
    assert!(
        page.records.is_empty(),
        "read_global_page(after) must EXCLUDE `after`"
    );
    let page = store
        .backend()
        .read_global_page(pos.checked_sub(1), 8)
        .await
        .expect("read_global_page");
    assert_eq!(
        page.records.first().map(|r| r.global_position),
        Some(pos),
        "reading after `pos - 1` must include `pos`"
    );
}

#[tokio::test]
async fn tail_catch_up_matches_the_projection_and_the_watermark() {
    // Cross-check the hand-rolled tailer against the store's own subscription
    // (which `Projections` uses): both must see the same records.
    let (_t, dir, store, report) =
        multi_segment_store("chatter-tail-crosscheck", 34).await;

    let mut tail = Tailer::from(&store, 0, 64);
    let mut messages = 0u64;
    let mut reactions = 0u64;
    tail.catch_up(|batch| {
        for rec in batch {
            match rec.message_type.as_str() {
                "channel.message_posted" => messages += 1,
                "channel.reaction_added" => reactions += 1,
                _ => {}
            }
        }
    })
    .await
    .expect("catch up");

    let proj =
        Projections::with_checkpoint(&store, checkpoint_path(&dir)).await;
    let cards = proj.cardinalities().await;
    assert_eq!(messages, cards.messages, "tail and projection must agree");
    assert_eq!(reactions, cards.reactions);
    assert_eq!(messages as usize, report.messages);
    assert_eq!(reactions as usize, report.reactions);

    let head = store.watermark().await.expect("watermark");
    assert!(
        tail.position() >= head,
        "a caught-up tail's cursor must have reached the watermark: {} vs \
         {head}",
        tail.position()
    );
}
