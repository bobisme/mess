//! `codec_id 1`: MessagePack, named-field mode (`rmp_serde::to_vec_named`).
//!
//! Ground truth: `spikes/codec_bakeoff` (see `REPORT.md` section 5). Of the
//! six candidates benchmarked there (json, cbor, msgpack-named,
//! msgpack-compact, postcard, bincode), msgpack-named is the only one that
//! is both fast and evolution-safe: postcard, bincode, and msgpack-compact
//! all silently swap values on a same-typed field reorder or enum-variant
//! reorder (zero errors, wrong data) — disqualifying for an event store
//! whose payloads outlive the code that wrote them. See
//! `tests/codec_evolution_matrix.rs` for the locked-in classification.

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::error::CodecError;

/// `codec_id` reserved for the bootstrap codec (frozen forever, owned by
/// the registry — see docs/spec/04-registry.md §2-3). This codec layer
/// does not implement it; [`decode_payload`]/[`encode_payload`] report
/// [`CodecError::BootstrapCodecUnsupported`] for it.
pub const CODEC_ID_BOOTSTRAP: u16 = 0;

/// `codec_id` for MessagePack named-field mode — the codec this module
/// implements.
pub const CODEC_ID_MSGPACK_NAMED: u16 = 1;

/// A payload codec identified by a registry `codec_id`.
///
/// Implementors are zero-sized dispatch tokens (see [`MsgpackNamed`]); the
/// free functions [`encode_payload`]/[`decode_payload`] match a runtime
/// `codec_id` to the right implementation. The trait itself is generic
/// (not object-safe) because payload types vary per call site — dispatch
/// happens on `codec_id`, not on a trait object.
pub trait Codec {
    /// The registry `codec_id` this codec implements.
    const CODEC_ID: u16;

    /// Encode `value` to this codec's wire format.
    fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError>;

    /// Decode `bytes` (produced by [`Codec::encode`]) back to `T`.
    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError>;
}

/// MessagePack, named-field mode: `rmp_serde::to_vec_named`. Struct fields
/// are encoded as map entries keyed by field name; enum variants are
/// encoded by name. This is what makes it evolution-safe: decode reads by
/// name, not by position, so reordering fields/variants cannot silently
/// swap values.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct MsgpackNamed;

impl Codec for MsgpackNamed {
    const CODEC_ID: u16 = CODEC_ID_MSGPACK_NAMED;

    fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
        rmp_serde::to_vec_named(value).map_err(CodecError::Encode)
    }

    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
        rmp_serde::from_slice(bytes).map_err(CodecError::Decode)
    }
}

/// Encode `value` under the codec identified by `codec_id`.
///
/// Fails loudly (naming the offending `codec_id` and the range this layer
/// implements) for anything other than `codec_id 1`.
pub fn encode_payload<T: Serialize>(
    codec_id: u16,
    value: &T,
) -> Result<Vec<u8>, CodecError> {
    match codec_id {
        CODEC_ID_MSGPACK_NAMED => MsgpackNamed::encode(value),
        CODEC_ID_BOOTSTRAP => Err(CodecError::BootstrapCodecUnsupported),
        other => Err(CodecError::UnknownCodecId { codec_id: other }),
    }
}

/// Decode `bytes` under the codec identified by `codec_id`.
///
/// Fails loudly (naming the offending `codec_id` and the range this layer
/// implements) for anything other than `codec_id 1`.
pub fn decode_payload<T: DeserializeOwned>(
    codec_id: u16,
    bytes: &[u8],
) -> Result<T, CodecError> {
    match codec_id {
        CODEC_ID_MSGPACK_NAMED => MsgpackNamed::decode(bytes),
        CODEC_ID_BOOTSTRAP => Err(CodecError::BootstrapCodecUnsupported),
        other => Err(CodecError::UnknownCodecId { codec_id: other }),
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Sample {
        id: u32,
        name: String,
    }

    #[test]
    fn round_trips_through_codec_id_1() {
        let v = Sample { id: 7, name: "alice".into() };
        let bytes = encode_payload(CODEC_ID_MSGPACK_NAMED, &v).unwrap();
        let back: Sample =
            decode_payload(CODEC_ID_MSGPACK_NAMED, &bytes).unwrap();
        assert_eq!(back, v);
    }

    #[test]
    fn encoder_is_deterministic() {
        let v = Sample { id: 7, name: "alice".into() };
        let a = encode_payload(CODEC_ID_MSGPACK_NAMED, &v).unwrap();
        let b = encode_payload(CODEC_ID_MSGPACK_NAMED, &v).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn unknown_codec_id_fails_loudly() {
        let v = Sample { id: 7, name: "alice".into() };
        let err = encode_payload(200, &v).unwrap_err();
        assert!(matches!(err, CodecError::UnknownCodecId { codec_id: 200 }));
        let err = decode_payload::<Sample>(200, &[]).unwrap_err();
        assert!(matches!(err, CodecError::UnknownCodecId { codec_id: 200 }));
    }

    #[test]
    fn bootstrap_codec_id_is_reserved_not_decodable_here() {
        let v = Sample { id: 7, name: "alice".into() };
        let err = encode_payload(CODEC_ID_BOOTSTRAP, &v).unwrap_err();
        assert!(matches!(err, CodecError::BootstrapCodecUnsupported));
        let err =
            decode_payload::<Sample>(CODEC_ID_BOOTSTRAP, &[]).unwrap_err();
        assert!(matches!(err, CodecError::BootstrapCodecUnsupported));
    }

    #[test]
    fn corrupt_payload_fails_loudly() {
        let err =
            decode_payload::<Sample>(CODEC_ID_MSGPACK_NAMED, &[0xff, 0x01])
                .unwrap_err();
        assert!(matches!(err, CodecError::Decode(_)));
    }
}
