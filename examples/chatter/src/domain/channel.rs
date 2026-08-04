//! The **Channel** aggregate: a long-lived conversation — one **deep** stream
//! per channel, keyed `channel-<id>` (see [`crate::channel_stream`]).
//!
//! This is the shape `examples/social` structurally cannot produce, and the
//! reason this crate exists (bn-3jqg): social is stream-*per-aggregate* with
//! ~1.007 events per stream, so no segment ever rolls and the whole sealed
//! tier — `.pcol` payload sidecars, `.reg` registry deltas, SealPack,
//! quarantine/re-seal, retention, cold-open-at-scale — is unreachable from
//! `examples/`. A chat channel is the opposite: one stream that accumulates
//! *thousands* of messages and reactions over its life, so a seeded corpus
//! rolls and seals segments as a matter of course.
//!
//! # Bounded state on an unbounded stream
//!
//! The stream is deep; the folded state is **not**. [`Channel`] holds an
//! existence flag, an archive flag, two short strings, and two counters —
//! O(1) bytes whether the channel has ten messages or ten million. That is the
//! property that makes `command_cached`'s snapshot path meaningful here: the
//! snapshot of a 50,000-event channel is still a few dozen bytes.
//!
//! Message *bodies* are never folded into state. A channel does not need to
//! remember what was said to decide whether the next thing may be said.
//!
//! # The one cross-entity check that stays bounded
//!
//! A reaction names the message it reacts to by its **ordinal** — the
//! channel-local sequence number stamped on the message when it was posted.
//! Because the aggregate already counts messages, `target < messages` is a
//! complete, race-free existence check against *this stream's own state*: no
//! set of message ids, no unbounded growth, no cross-stream read. It is the
//! rare case where the single-stream invariant rule and a referential check
//! coincide, and it is why messages carry an explicit ordinal at all (see
//! [`ChannelEvent::MessagePosted`]).
//!
//! What is deliberately **not** checked: that `author`/`by` name a registered
//! user. `decide` is a pure function of one stream's state and cannot load
//! `user-<id>` — the accept-and-reconcile posture `examples/social`'s
//! `domain::user` documents at length. The projection reconciles: a message
//! from an unknown author renders with an empty handle rather than being
//! dropped.
//!
//! Duplicate reactions (the same user reacting twice with the same emoji) are
//! likewise accepted rather than refused — deduplicating them would require
//! remembering every (message, user, emoji) triple, i.e. exactly the unbounded
//! state this aggregate exists to avoid. Reaction *counts* are the projection's
//! job.

use mess_core::Decide;
use mess_derive::{Aggregate, Event};
use mess_store::{Snapshottable, StableSnapshotId, StateCodecError};

use crate::Id;
use crate::domain::snapshot_codec;
use crate::domain::snapshot_codec::{Reader, put_bool, put_str, put_u64};

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/// Every fact that can happen in a channel. Wire names:
/// `"channel.created"`, `"channel.message_posted"`,
/// `"channel.reaction_added"`, `"channel.archived"`.
#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "channel", version = 1)]
pub enum ChannelEvent {
    /// The channel was opened with a human-facing `slug` and a `topic`.
    Created { slug: String, topic: String },
    /// A message was posted.
    ///
    /// `ordinal` is the channel-local sequence number, stamped by
    /// [`Decide::decide`] from the folded message counter and therefore
    /// authoritative at write time (there is no other writer). Carrying it on
    /// the wire — rather than recomputing it by counting `MessagePosted`
    /// events during a replay — is what lets a **backward** pager
    /// ([`crate::scrollback`]) render a page of sealed history without first
    /// scanning the stream forward from position 0, and it is what a
    /// [`ReactionAdded`](ChannelEvent::ReactionAdded) names as its target.
    MessagePosted { ordinal: u64, author: Id, body: String },
    /// A reaction was added to the message with channel-local ordinal
    /// `target`.
    ReactionAdded { target: u64, by: Id, emoji: String },
    /// The channel was archived: no further messages or reactions.
    Archived,
}

// ---------------------------------------------------------------------------
// The aggregate
// ---------------------------------------------------------------------------

/// The read-model folded from one channel's (deep) event stream — **bounded**
/// by construction. See the module docs.
#[derive(Debug, Default, Clone, PartialEq, Eq, Aggregate)]
#[aggregate(event = ChannelEvent)]
pub struct Channel {
    /// `false` until a `Created` event is folded.
    pub created:   bool,
    /// `true` once `Archived` is folded — a terminal state for writes.
    pub archived:  bool,
    /// The human-facing slug (`general`, `deploys`, …).
    pub slug:      String,
    /// The channel topic line.
    pub topic:     String,
    /// How many messages have been posted. Also the ordinal the *next*
    /// message will carry, and the exclusive upper bound a reaction's target
    /// must fall below.
    pub messages:  u64,
    /// How many reactions have been added (duplicates included — see the
    /// module docs).
    pub reactions: u64,
}

impl Channel {
    /// Fold one event into state. Infallible by construction.
    pub fn apply(&mut self, event: &ChannelEvent) {
        match event {
            ChannelEvent::Created { slug, topic } => {
                self.created = true;
                slug.clone_into(&mut self.slug);
                topic.clone_into(&mut self.topic);
            }
            ChannelEvent::MessagePosted { ordinal, .. } => {
                debug_assert_eq!(
                    *ordinal, self.messages,
                    "a message's stamped ordinal must equal the count of \
                     messages folded before it"
                );
                self.messages += 1;
            }
            ChannelEvent::ReactionAdded { .. } => self.reactions += 1,
            ChannelEvent::Archived => self.archived = true,
        }
    }
}

/// A [`Channel`] snapshot is two bits, two short strings, and two counters —
/// a few dozen bytes for a stream of any depth. That is the whole point: the
/// warm-write path's snapshot of a 50,000-message channel is the same size as
/// its snapshot after the first message.
///
/// **`FOLD_VERSION` bump rule.** Bump whenever [`Channel::apply`] semantics or
/// the blob shape change such that an old blob would misrepresent the state.
impl Snapshottable for Channel {
    const AGGREGATE_SCHEMA_ID: StableSnapshotId =
        StableSnapshotId::new("chatter.channel");
    const CODEC_ID: StableSnapshotId = snapshot_codec::CODEC_ID;
    const CODEC_VERSION: u32 = snapshot_codec::CODEC_VERSION;
    const FOLD_VERSION: u32 = 1;

    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
        let mut out = Vec::new();
        put_bool(&mut out, self.created);
        put_bool(&mut out, self.archived);
        put_str(&mut out, &self.slug);
        put_str(&mut out, &self.topic);
        put_u64(&mut out, self.messages);
        put_u64(&mut out, self.reactions);
        Ok(out)
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
        let mut r = Reader::new(bytes);
        let created = r.read_bool()?;
        let archived = r.read_bool()?;
        let slug = r.read_str()?;
        let topic = r.read_str()?;
        let messages = r.read_u64()?;
        let reactions = r.read_u64()?;
        r.finish()?;
        Ok(Channel { created, archived, slug, topic, messages, reactions })
    }
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// The maximum length, in characters, of a channel slug.
pub const SLUG_MAX_LEN: usize = 40;

/// The maximum length, in characters, of a message body.
pub const BODY_MAX_LEN: usize = 2000;

/// The maximum length, in characters, of a reaction emoji/shortcode.
pub const EMOJI_MAX_LEN: usize = 32;

/// Is `slug` a syntactically valid channel slug? 1 to [`SLUG_MAX_LEN`]
/// characters of ASCII lowercase, digits, `-`, or `_`.
#[must_use]
pub fn slug_is_valid(slug: &str) -> bool {
    let len = slug.chars().count();
    (1..=SLUG_MAX_LEN).contains(&len)
        && slug.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_'
        })
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Every way a command against [`Channel`] can be refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelError {
    /// `CreateChannel` on a stream that already holds a channel.
    AlreadyCreated,
    /// Any command other than `CreateChannel` on a stream with no channel.
    NotCreated,
    /// `CreateChannel` with a slug that fails [`slug_is_valid`].
    InvalidSlug { slug: String },
    /// A write to an archived channel.
    Archived,
    /// `PostMessage` with a body of zero characters.
    EmptyBody,
    /// `PostMessage` with a body longer than [`BODY_MAX_LEN`] characters.
    BodyTooLong { len: usize, max: usize },
    /// `AddReaction` with an empty or over-long emoji.
    InvalidEmoji { len: usize, max: usize },
    /// `AddReaction` naming a message ordinal this channel has not reached —
    /// the bounded referential check (see the module docs).
    NoSuchMessage { target: u64, messages: u64 },
    /// `ArchiveChannel` on an already-archived channel.
    AlreadyArchived,
}

impl std::fmt::Display for ChannelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelError::AlreadyCreated => write!(f, "channel already exists"),
            ChannelError::NotCreated => write!(f, "channel does not exist yet"),
            ChannelError::InvalidSlug { slug } => write!(
                f,
                "invalid slug {slug:?}: must be 1-{SLUG_MAX_LEN} characters \
                 of lowercase a-z, 0-9, hyphen, or underscore"
            ),
            ChannelError::Archived => write!(f, "channel is archived"),
            ChannelError::EmptyBody => {
                write!(f, "message body must not be empty")
            }
            ChannelError::BodyTooLong { len, max } => write!(
                f,
                "message body is {len} characters; the maximum is {max}"
            ),
            ChannelError::InvalidEmoji { len, max } => {
                write!(f, "reaction is {len} characters; must be 1-{max}")
            }
            ChannelError::NoSuchMessage { target, messages } => write!(
                f,
                "no message with ordinal {target}; this channel has \
                 {messages} message(s)"
            ),
            ChannelError::AlreadyArchived => {
                write!(f, "channel is already archived")
            }
        }
    }
}

impl std::error::Error for ChannelError {}

/// Open a new channel.
#[derive(Debug, Clone)]
pub struct CreateChannel {
    pub slug:  String,
    pub topic: String,
}

/// Post a message. The ordinal is assigned by the aggregate, never by the
/// caller.
#[derive(Debug, Clone)]
pub struct PostMessage {
    pub author: Id,
    pub body:   String,
}

/// React to the message with channel-local ordinal `target`.
#[derive(Debug, Clone)]
pub struct AddReaction {
    pub target: u64,
    pub by:     Id,
    pub emoji:  String,
}

/// Archive the channel: no further writes.
#[derive(Debug, Clone, Copy)]
pub struct ArchiveChannel;

impl Decide<CreateChannel> for Channel {
    type Rejection = ChannelError;

    fn decide(
        &self,
        cmd: CreateChannel,
    ) -> Result<Vec<ChannelEvent>, ChannelError> {
        if self.created {
            return Err(ChannelError::AlreadyCreated);
        }
        if !slug_is_valid(&cmd.slug) {
            return Err(ChannelError::InvalidSlug { slug: cmd.slug });
        }
        Ok(vec![ChannelEvent::Created { slug: cmd.slug, topic: cmd.topic }])
    }
}

impl Decide<PostMessage> for Channel {
    type Rejection = ChannelError;

    fn decide(
        &self,
        cmd: PostMessage,
    ) -> Result<Vec<ChannelEvent>, ChannelError> {
        if !self.created {
            return Err(ChannelError::NotCreated);
        }
        if self.archived {
            return Err(ChannelError::Archived);
        }
        // Count `char`s, not bytes: the limit is a user-facing length.
        let len = cmd.body.chars().count();
        if len == 0 {
            return Err(ChannelError::EmptyBody);
        }
        if len > BODY_MAX_LEN {
            return Err(ChannelError::BodyTooLong { len, max: BODY_MAX_LEN });
        }
        Ok(vec![ChannelEvent::MessagePosted {
            // The channel-local sequence number, stamped here from the folded
            // counter — see the module docs and `ChannelEvent::MessagePosted`.
            ordinal: self.messages,
            author:  cmd.author,
            body:    cmd.body,
        }])
    }
}

impl Decide<AddReaction> for Channel {
    type Rejection = ChannelError;

    fn decide(
        &self,
        cmd: AddReaction,
    ) -> Result<Vec<ChannelEvent>, ChannelError> {
        if !self.created {
            return Err(ChannelError::NotCreated);
        }
        if self.archived {
            return Err(ChannelError::Archived);
        }
        let len = cmd.emoji.chars().count();
        if !(1..=EMOJI_MAX_LEN).contains(&len) {
            return Err(ChannelError::InvalidEmoji { len, max: EMOJI_MAX_LEN });
        }
        // The bounded referential check: a message ordinal is valid iff it is
        // below the count this stream has folded. No id set, no cross-stream
        // read.
        if cmd.target >= self.messages {
            return Err(ChannelError::NoSuchMessage {
                target:   cmd.target,
                messages: self.messages,
            });
        }
        Ok(vec![ChannelEvent::ReactionAdded {
            target: cmd.target,
            by:     cmd.by,
            emoji:  cmd.emoji,
        }])
    }
}

impl Decide<ArchiveChannel> for Channel {
    type Rejection = ChannelError;

    fn decide(
        &self,
        _cmd: ArchiveChannel,
    ) -> Result<Vec<ChannelEvent>, ChannelError> {
        if !self.created {
            return Err(ChannelError::NotCreated);
        }
        if self.archived {
            return Err(ChannelError::AlreadyArchived);
        }
        Ok(vec![ChannelEvent::Archived])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open() -> Channel {
        let mut c = Channel::default();
        c.apply(&ChannelEvent::Created {
            slug:  "general".into(),
            topic: "everything".into(),
        });
        c
    }

    #[test]
    fn state_round_trips_through_bytes() {
        let mut c = open();
        c.apply(&ChannelEvent::MessagePosted {
            ordinal: 0,
            author:  Id::from_parts(7, [1; 10]),
            body:    "hi".into(),
        });
        c.apply(&ChannelEvent::ReactionAdded {
            target: 0,
            by:     Id::from_parts(7, [2; 10]),
            emoji:  "🎉".into(),
        });
        let bytes = c.encode_state().unwrap();
        assert_eq!(Channel::decode_state(&bytes).unwrap(), c);
        // The bounded promise, made concrete: a snapshot blob small enough to
        // print. Depth does not move this number.
        assert!(bytes.len() < 128, "snapshot blob was {} bytes", bytes.len());
    }

    #[test]
    fn snapshot_size_is_independent_of_depth() {
        let mut c = open();
        let author = Id::from_parts(7, [1; 10]);
        let shallow = c.encode_state().unwrap().len();
        for i in 0..5_000u64 {
            c.apply(&ChannelEvent::MessagePosted {
                ordinal: i,
                author,
                body: "a".repeat(500),
            });
        }
        assert_eq!(c.messages, 5_000);
        assert_eq!(c.encode_state().unwrap().len(), shallow);
    }

    #[test]
    fn malformed_blob_errors_not_panics() {
        assert!(Channel::decode_state(&[1, 0]).is_err());
        assert!(Channel::decode_state(&[]).is_err());
    }

    #[test]
    fn message_ordinals_are_assigned_by_the_aggregate() {
        let mut c = open();
        let author = Id::from_parts(7, [1; 10]);
        for expect in 0..4u64 {
            let events = c
                .decide(PostMessage { author, body: format!("m{expect}") })
                .unwrap();
            assert_eq!(
                events,
                vec![ChannelEvent::MessagePosted {
                    ordinal: expect,
                    author,
                    body: format!("m{expect}"),
                }]
            );
            for e in &events {
                c.apply(e);
            }
        }
    }

    #[test]
    fn reaction_to_a_future_ordinal_is_refused() {
        let mut c = open();
        let user = Id::from_parts(7, [3; 10]);
        assert_eq!(
            c.decide(AddReaction {
                target: 0,
                by:     user,
                emoji:  "+1".into(),
            }),
            Err(ChannelError::NoSuchMessage { target: 0, messages: 0 })
        );
        c.apply(&ChannelEvent::MessagePosted {
            ordinal: 0,
            author:  user,
            body:    "hi".into(),
        });
        assert!(
            c.decide(AddReaction {
                target: 0,
                by:     user,
                emoji:  "+1".into(),
            })
            .is_ok()
        );
        assert_eq!(
            c.decide(AddReaction {
                target: 1,
                by:     user,
                emoji:  "+1".into(),
            }),
            Err(ChannelError::NoSuchMessage { target: 1, messages: 1 })
        );
    }

    #[test]
    fn archived_channel_refuses_writes() {
        let mut c = open();
        c.apply(&ChannelEvent::Archived);
        let user = Id::from_parts(7, [4; 10]);
        assert_eq!(
            c.decide(PostMessage { author: user, body: "hi".into() }),
            Err(ChannelError::Archived)
        );
        assert_eq!(
            c.decide(AddReaction {
                target: 0,
                by:     user,
                emoji:  "x".into(),
            }),
            Err(ChannelError::Archived)
        );
        assert_eq!(
            c.decide(ArchiveChannel),
            Err(ChannelError::AlreadyArchived)
        );
    }

    #[test]
    fn body_bounds_are_enforced() {
        let c = open();
        let user = Id::from_parts(7, [5; 10]);
        assert_eq!(
            c.decide(PostMessage { author: user, body: String::new() }),
            Err(ChannelError::EmptyBody)
        );
        let long = "x".repeat(BODY_MAX_LEN + 1);
        assert_eq!(
            c.decide(PostMessage { author: user, body: long }),
            Err(ChannelError::BodyTooLong {
                len: BODY_MAX_LEN + 1,
                max: BODY_MAX_LEN,
            })
        );
    }

    #[test]
    fn slug_validation() {
        assert!(slug_is_valid("general"));
        assert!(slug_is_valid("team-deploys_2"));
        assert!(!slug_is_valid(""));
        assert!(!slug_is_valid("General"));
        assert!(!slug_is_valid(&"x".repeat(SLUG_MAX_LEN + 1)));
    }
}
