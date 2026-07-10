//! Loud-failure error types for the codec and upcaster layers.
//!
//! Every variant names the offending value and, where the space of valid
//! values is small and fixed, what was expected — per the bake-off's
//! finding that silent corruption (not merely errors) is what disqualifies
//! a codec. See `spikes/codec_bakeoff/REPORT.md` section 3.

use thiserror::Error;

/// Errors from encoding/decoding a raw payload for a given `codec_id`.
#[derive(Debug, Error)]
pub enum CodecError {
    /// `codec_id` did not match any codec this layer implements.
    #[error(
        "unknown codec_id {codec_id}: mess-core's codec layer implements \
         codec_id 1 (msgpack-named); codec_id 0 is the frozen bootstrap \
         codec, owned by the registry (docs/spec/04-registry.md), not this \
         layer"
    )]
    UnknownCodecId {
        /// The `codec_id` found on the envelope.
        codec_id: u16,
    },

    /// `codec_id` was 0 (bootstrap). Valid, but not decodable here: the
    /// bootstrap codec's frozen wire format is owned by the registry crate,
    /// not the domain payload codec layer.
    #[error(
        "codec_id 0 is the frozen bootstrap codec owned by the registry \
         (docs/spec/04-registry.md, section 2-3); this codec layer only \
         encodes/decodes domain payload codec_id 1 (msgpack-named)"
    )]
    BootstrapCodecUnsupported,

    /// `rmp_serde::to_vec_named` failed.
    #[error("msgpack-named encode failed: {0}")]
    Encode(#[source] rmp_serde::encode::Error),

    /// `rmp_serde::from_slice` failed (includes truncated/corrupt payloads).
    #[error("msgpack-named decode failed: {0}")]
    Decode(#[source] rmp_serde::decode::Error),

    /// The payload's msgpack nesting depth exceeds `MAX_MSGPACK_DEPTH`
    /// (checked *before* handing the bytes to `rmp_serde`). Without this
    /// check, an adversarial payload — in practice an unknown/extra map
    /// field whose value serde's default `#[serde(deny_unknown_fields)]`-less
    /// struct decoding must still fully skip via `IgnoredAny`, recursing
    /// once per nesting level — can overflow the native call stack: a
    /// SIGABRT/SIGSEGV that no `Result` can report, not a panic `catch_unwind`
    /// can catch. See `codec::msgpack::check_msgpack_depth`.
    #[error(
        "msgpack payload nesting depth exceeds the {max} level cap (bn-meo: \
         unbounded nesting can stack-overflow the recursive decoder rather \
         than fail loudly)"
    )]
    TooDeeplyNested {
        /// The cap that was exceeded.
        max: usize,
    },
}

/// Errors from the upcaster dispatch layer (`event_versions!`-generated
/// `decode_*` functions): wraps [`CodecError`] with the additional failure
/// modes of routing a [`StoredEvent`](super::StoredEvent) to the right
/// versioned type.
#[derive(Debug, Error)]
pub enum UpcastError {
    /// The `StoredEvent::event_name` did not match the event this decoder
    /// was generated for.
    #[error("wrong event name: expected {expected:?}, got {got:?}")]
    WrongEventName {
        /// The name this decoder dispatches for.
        expected: &'static str,
        /// The name actually found on the envelope.
        got:      String,
    },

    /// `schema_version` did not match any version this event's
    /// `event_versions!` declaration knows how to upcast.
    #[error(
        "unknown schema_version {version} for event {event_name:?}: known \
         versions are {known_versions:?}"
    )]
    UnknownSchemaVersion {
        /// The event name being decoded.
        event_name:     String,
        /// The unrecognized `schema_version` found on the envelope.
        version:        u16,
        /// Every `schema_version` this decoder was generated to handle,
        /// in ascending declaration order.
        known_versions: &'static [u16],
    },

    /// The payload failed to encode/decode under its declared `codec_id`.
    #[error(transparent)]
    Codec(#[from] CodecError),
}
