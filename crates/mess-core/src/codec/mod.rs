//! Codec layer: the [`StoredEvent`] envelope, `codec_id` payload codecs,
//! and the [`Upcast`]/[`UpcastsTo`] upcaster chain that lets read-side code
//! decode any historical `schema_version` to the latest Rust type.
//!
//! Self-contained port of `spikes/codec_bakeoff` (see that spike's
//! `REPORT.md` for the full bake-off rationale). Ground truth decisions
//! carried over verbatim:
//!
//! - `codec_id 1` = MessagePack, named-field mode
//!   (`rmp_serde::to_vec_named`) — the only codec in the bake-off that is
//!   both fast and evolution-safe. `codec_id 0` is the frozen bootstrap
//!   codec; it is reserved here but owned/implemented by the registry
//!   (docs/spec/04-registry.md), not this layer.
//! - Field widths (`schema_version: u16`, `codec_id: u16`) match the
//!   on-disk `EventSubframe` header (docs/spec/01-log-format.md).
//! - Unknown `schema_version`/`codec_id` fail loudly with actionable
//!   errors, never silently — see [`CodecError`] and [`UpcastError`].

mod envelope;
mod error;
mod msgpack;
mod upcast;

pub use envelope::StoredEvent;
pub use error::{CodecError, UpcastError};
pub use msgpack::{
    CODEC_ID_BOOTSTRAP, CODEC_ID_MSGPACK_NAMED, Codec, MsgpackNamed,
    decode_payload, encode_payload,
};
pub use upcast::{Upcast, UpcastsTo};

// `event_versions!` and its `__mess_core_upcast_impls!` helper are declared
// with `#[macro_export]` in `upcast.rs`, which places them at the crate
// root (`crate::event_versions!`) — macro_rules! export is not
// path-scoped to this module, so there is nothing to re-export here.
