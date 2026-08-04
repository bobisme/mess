//! Chatter: a chat/feed domain whose streams get **deep** — the corpus
//! generator `examples/social` structurally cannot be (bn-1m6c, motivated by
//! bn-3jqg).
//!
//! # Why this example exists
//!
//! `examples/social` is stream-*per-aggregate* all the way down: a user, a
//! post, a like edge, a follow edge each get their own stream, and the measured
//! result is **1.007 events per stream** in a 17 MB corpus with no
//! segment-size knob. No segment ever rolls, so nothing in the sealed tier is
//! reachable from `examples/`: not `.pcol` payload sidecars, not `.reg`
//! registry deltas, not SealPack, not quarantine/re-seal, not retention, not
//! cold-open-at-scale. Every one of those is a real, shipped part of mess that
//! the examples could not demonstrate or measure.
//!
//! Chatter is the complementary shape, and it carries **both** pressures in one
//! corpus:
//!
//! - **Stream depth.** A channel ([`domain::channel`]) is one long-lived
//!   stream. Channel choice is Zipf-distributed, so a few hot channels
//!   accumulate thousands of messages and reactions while a long tail stays
//!   shallow — exactly the skew a real chat product has.
//! - **Registry pressure.** Every user ([`domain::user`]) is a distinct,
//!   nearly-empty `user-<id>` stream, so the engine's stream registry has tens
//!   of thousands of names to intern and carry in per-segment deltas.
//!
//! Combine that with the `--segment-bytes` knob (the one social lacks — see
//! [`store_backend`]) and even the **demo** scale produces a multi-segment
//! sealed store. That is the bone's core purpose, and `tests/segments.rs`
//! asserts it so it cannot silently regress.
//!
//! # The tour, in the order a newcomer meets it
//!
//! 1. [`domain::channel`] — the deep stream. Bounded folded state on an
//!    unbounded stream, and the one referential check (a reaction's target
//!    ordinal) that stays bounded.
//! 2. [`domain::user`] — the shallow stream. Registry pressure.
//! 3. [`ops`] — the write seam: one warm-path `command_cached` call per action.
//! 4. [`seed`] — the deterministic corpus generator and its scale presets.
//! 5. [`projections`] — the per-channel and global-timeline read models, with
//!    the discardable checkpoint sidecar.
//! 6. [`scrollback`] — backward paging through a channel's sealed history (the
//!    read path that goes through the sealed payload accelerator).
//! 7. [`tail`] — a subscriber written against `read_global_page` + frontier, so
//!    the SUB2 exclusive-of-`after` cursor rule is visible rather than implied.
//! 8. [`rebuild`] — the checkpoint-correctness byte-compare proof.
//! 9. [`bench`] — `chatter bench`: five process-timed cells emitted as JSON.
//!
//! # Streams
//!
//! Each aggregate is one stream *family*, routed by
//! [`StoredRecord::category`](mess_store::StoredRecord::category) (the segment
//! before the first `-`):
//!
//! - `channel-<id>` — one [`Channel`](domain::channel::Channel); deep.
//! - `user-<id>` — one [`User`](domain::user::User); shallow.
//!
//! [`Id`]'s canonical string contains no `-`, so that split is unambiguous —
//! see [`id`].

pub mod bench;
pub mod domain;
pub mod id;
pub mod ops;
pub mod projections;
pub mod rebuild;
pub mod scrollback;
pub mod seed;
pub mod store_backend;
pub mod tail;

pub use domain::channel::{
    AddReaction, ArchiveChannel, BODY_MAX_LEN, Channel, ChannelError,
    ChannelEvent, CreateChannel, PostMessage, SLUG_MAX_LEN, slug_is_valid,
};
pub use domain::user::{
    HANDLE_MAX_LEN, RegisterUser, SetDisplayName, User, UserError, UserEvent,
    handle_is_valid,
};
pub use id::{Id, IdParseError};
pub use ops::{WriteError, WriteOps};
pub use projections::{
    Cardinalities, ChannelSummary, CheckpointStatus, PROJECTION_VERSION,
    Projections, TimelineRow,
};

/// The stream id for a channel's aggregate: `channel-<id>`.
///
/// The one place this convention is written down; [`ops::WriteOps`], the
/// projection's router, and [`scrollback`] all call it, so a rename is a single
/// edit.
#[must_use]
pub fn channel_stream(id: Id) -> String { format!("channel-{id}") }

/// The stream id for a user's aggregate: `user-<id>`.
#[must_use]
pub fn user_stream(id: Id) -> String { format!("user-{id}") }

/// Recover a channel id from a `channel-<id>` stream id, or `None` if it is
/// not one.
#[must_use]
pub fn parse_channel_stream(stream_id: &str) -> Option<Id> {
    stream_id.strip_prefix("channel-")?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_ids_split_unambiguously_at_the_first_hyphen() {
        let id = Id::from_parts(1_704_067_200_000, [3; 10]);
        let s = channel_stream(id);
        let (category, suffix) = s.split_once('-').unwrap();
        assert_eq!(category, "channel");
        assert_eq!(suffix.parse::<Id>().unwrap(), id);
        assert_eq!(parse_channel_stream(&s), Some(id));
        assert_eq!(parse_channel_stream(&user_stream(id)), None);
    }
}
