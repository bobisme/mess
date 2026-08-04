//! The two aggregates, one per stream *family*:
//!
//! - [`channel`] — the **deep** stream. One `channel-<id>` per conversation,
//!   thousands of events long, with bounded folded state. This is the shape
//!   that rolls segments.
//! - [`user`] — the **shallow** stream. One `user-<id>` per account, one or two
//!   events long. This is the shape that applies registry pressure.
//!
//! Both are `#[derive(Aggregate)]` folds with `Decide` impls per command and a
//! typed rejection, exactly as `examples/bank` and `examples/social` teach.
//! [`snapshot_codec`] is the hand-rolled length-prefixed state codec they
//! share.

pub mod channel;
pub mod snapshot_codec;
pub mod user;
