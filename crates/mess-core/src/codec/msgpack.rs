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

/// Nesting depth cap enforced by [`check_msgpack_depth`] before any payload
/// reaches `rmp_serde`. Generous relative to any real event shape (deeply
/// nested `Option<Vec<Struct>>` chains in hand-written domain events don't
/// come close), tight enough that the worst case — `MAX_MSGPACK_DEPTH`
/// levels of native recursion inside `serde`'s `IgnoredAny` skip or a
/// container visitor — cannot threaten an 8 MiB thread stack.
pub const MAX_MSGPACK_DEPTH: usize = 64;

/// Walk `data` as a single top-level msgpack value, computing its maximum
/// container nesting depth **without native recursion** (an explicit
/// `Vec`-backed stack stands in for the call stack), and fail loudly if it
/// exceeds [`MAX_MSGPACK_DEPTH`] instead of letting a later recursive
/// decode step (`rmp_serde::from_slice`, and specifically `serde`'s
/// `IgnoredAny` skip of unknown/extra map fields) overflow the native call
/// stack — a SIGABRT/SIGSEGV no `Result` can report.
///
/// This is deliberately *not* a full msgpack validator: on truncated or
/// otherwise malformed bytes it returns `Ok(())` as soon as it runs out of
/// bytes to walk, leaving every other failure mode (truncation, bad
/// markers, wrong types for the target struct, …) to `rmp_serde` — the
/// real decoder — to report with its own proper error. This function's
/// only job is the one check that a fully-recursive decoder cannot safely
/// make on its own: bounding depth *before* recursing.
pub(crate) fn check_msgpack_depth(
    data: &[u8],
    max_depth: usize,
) -> Result<(), CodecError> {
    // Each stack entry is "how many more sibling values remain to be read
    // at this nesting level, including the one about to be read next".
    // Depth at any point is `stack.len()`. Seeded with 1: we need to walk
    // exactly one top-level value.
    // u64, not u32: a map32/array32 length is a u32 element count, and a
    // map's *pair* count (what this walker tracks) doubles that — up to
    // ~8.6 billion, which would silently wrap in a u32.
    let mut stack: Vec<u64> = vec![1];
    let mut pos = 0usize;

    // Bounds-checked big-endian length-prefix reads. `None` means "not
    // enough bytes" — the caller (the loop below) treats that as "stop
    // walking, let rmp_serde report the truncation".
    fn read_u8(data: &[u8], pos: &mut usize) -> Option<usize> {
        let b = *data.get(*pos)?;
        *pos += 1;
        Some(b as usize)
    }
    fn read_u16(data: &[u8], pos: &mut usize) -> Option<usize> {
        let s = data.get(*pos..*pos + 2)?;
        *pos += 2;
        Some(u16::from_be_bytes(s.try_into().unwrap()) as usize)
    }
    fn read_u32(data: &[u8], pos: &mut usize) -> Option<usize> {
        let s = data.get(*pos..*pos + 4)?;
        *pos += 4;
        Some(u32::from_be_bytes(s.try_into().unwrap()) as usize)
    }

    'walk: while let Some(&top) = stack.last() {
        if top == 0 {
            stack.pop();
            continue;
        }
        // About to consume one value at the current level.
        *stack.last_mut().unwrap() -= 1;

        let Some(marker) = data.get(pos).copied() else { break };
        pos += 1;

        // Extra data-byte length for scalar/leaf markers (0 for markers
        // with no trailing bytes at all, e.g. nil/bool/fixint).
        let extra: Option<usize> = match marker {
            0x00..=0x7F | 0xE0..=0xFF => Some(0), // fixint
            0xC0 | 0xC2 | 0xC3 => Some(0),        // nil, false, true
            0xC1 => Some(0),                      // reserved/unused marker
            0xA0..=0xBF => Some((marker & 0x1F) as usize), // fixstr
            0xC4 => read_u8(data, &mut pos),      // bin8
            0xC5 => read_u16(data, &mut pos),     // bin16
            0xC6 => read_u32(data, &mut pos),     // bin32
            0xC7 => read_u8(data, &mut pos).map(|n| n + 1), // ext8 (+type byte)
            0xC8 => read_u16(data, &mut pos).map(|n| n + 1), // ext16
            0xC9 => read_u32(data, &mut pos).map(|n| n + 1), // ext32
            0xCA => Some(4),                      // f32
            0xCB => Some(8),                      // f64
            0xCC => Some(1),                      // u8
            0xCD => Some(2),                      // u16
            0xCE => Some(4),                      // u32
            0xCF => Some(8),                      // u64
            0xD0 => Some(1),                      // i8
            0xD1 => Some(2),                      // i16
            0xD2 => Some(4),                      // i32
            0xD3 => Some(8),                      // i64
            0xD4 => Some(2),                      // fixext1 (type + 1)
            0xD5 => Some(3),                      // fixext2
            0xD6 => Some(5),                      // fixext4
            0xD7 => Some(9),                      // fixext8
            0xD8 => Some(17),                     // fixext16
            0xD9 => read_u8(data, &mut pos),      // str8
            0xDA => read_u16(data, &mut pos),     // str16
            0xDB => read_u32(data, &mut pos),     // str32
            _ => None,                            /* container marker, or
                                                    * unreachable — handled
                                                    * below */
        };

        if let Some(extra) = extra {
            // Leaf value: skip its data bytes (bounds-checked; running out
            // just stops the walk, per the truncation contract above).
            match data.get(pos..pos + extra) {
                Some(_) => pos += extra,
                None => break 'walk,
            }
            continue;
        }

        // Container markers: push a new frame for their children and
        // check the depth this introduces.
        let children: Option<usize> = match marker {
            0x80..=0x8F => Some(2 * (marker & 0x0F) as usize), // fixmap
            0x90..=0x9F => Some((marker & 0x0F) as usize),     // fixarray
            0xDC => read_u16(data, &mut pos),                  // array16
            0xDD => read_u32(data, &mut pos),                  // array32
            0xDE => read_u16(data, &mut pos).map(|n| 2 * n),   // map16
            0xDF => read_u32(data, &mut pos).map(|n| 2 * n),   // map32
            _ => Some(0), /* marker byte alone was the whole value (shouldn't
                           * happen given the match above is exhaustive over
                           * every byte value, but stay conservative) */
        };
        let Some(children) = children else { break 'walk };
        if children > 0 {
            stack.push(children as u64);
            if stack.len() > max_depth {
                return Err(CodecError::TooDeeplyNested { max: max_depth });
            }
        }
    }
    Ok(())
}

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
        // Depth-check before recursing: see `check_msgpack_depth`'s doc for
        // why `rmp_serde::from_slice` alone cannot safely reject this on
        // its own (unknown-field skipping recurses natively).
        check_msgpack_depth(bytes, MAX_MSGPACK_DEPTH)?;
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
        id:   u32,
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
            decode_payload::<Sample>(CODEC_ID_MSGPACK_NAMED, &[0xFF, 0x01])
                .unwrap_err();
        assert!(matches!(err, CodecError::Decode(_)));
    }

    fn deep_nested_array(depth: usize) -> Vec<u8> {
        let mut v = vec![0x91u8; depth]; // depth x fixarray, len 1
        v.push(0x00); // fixint 0
        v
    }

    /// Regression (bn-meo fuzzing): before `check_msgpack_depth` existed, a
    /// struct payload with an unknown/extra map field whose value was
    /// nested hundreds of thousands of arrays deep crashed the process with
    /// a native stack overflow (SIGABRT) inside `serde`'s `IgnoredAny` skip
    /// of that field — no `Result` was ever returned. `depth = 200_000`
    /// reproduces that crash pre-fix; post-fix it must come back as a typed
    /// `CodecError::TooDeeplyNested`, not a crash.
    #[test]
    fn deeply_nested_unknown_field_fails_loudly_not_crash() {
        let mut payload = Vec::new();
        payload.push(0x82); // fixmap, 2 entries: known + unknown
        payload.extend_from_slice(&[0xA2, b'i', b'd']);
        payload.push(0x01); // id: 1
        payload.extend_from_slice(&[0xA5]);
        payload.extend_from_slice(b"extra");
        payload.extend_from_slice(&deep_nested_array(200_000));

        let err = decode_payload::<Sample>(CODEC_ID_MSGPACK_NAMED, &payload)
            .unwrap_err();
        assert!(
            matches!(err, CodecError::TooDeeplyNested { max } if max == MAX_MSGPACK_DEPTH),
            "expected TooDeeplyNested, got {err:?}"
        );
    }

    /// A payload nested right up to (but not past) the cap must still
    /// decode/fail on its own merits (here: not the target shape), not on
    /// depth — the cap must not be off-by-one against real, merely-complex
    /// payloads. `deep_nested_array(n)` nests `n` arrays around a scalar,
    /// which walks to stack depth `n + 1` (the top-level value's own
    /// frame, plus one push per array) — so `MAX_MSGPACK_DEPTH - 1` arrays
    /// is exactly the deepest nesting `check_msgpack_depth` accepts.
    #[test]
    fn nesting_at_the_cap_is_not_rejected_for_depth() {
        let payload = deep_nested_array(MAX_MSGPACK_DEPTH - 1);
        let err = decode_payload::<Sample>(CODEC_ID_MSGPACK_NAMED, &payload)
            .unwrap_err();
        assert!(
            !matches!(err, CodecError::TooDeeplyNested { .. }),
            "got {err:?}"
        );

        // One level deeper must be the first depth rejected.
        let too_deep = deep_nested_array(MAX_MSGPACK_DEPTH);
        let err = decode_payload::<Sample>(CODEC_ID_MSGPACK_NAMED, &too_deep)
            .unwrap_err();
        assert!(
            matches!(err, CodecError::TooDeeplyNested { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn check_msgpack_depth_accepts_shallow_and_rejects_deep() {
        assert!(
            check_msgpack_depth(&deep_nested_array(10), MAX_MSGPACK_DEPTH)
                .is_ok()
        );
        assert!(matches!(
            check_msgpack_depth(&deep_nested_array(1000), MAX_MSGPACK_DEPTH),
            Err(CodecError::TooDeeplyNested { max }) if max == MAX_MSGPACK_DEPTH
        ));
    }

    /// Truncated/malformed bytes must not confuse the depth walker into a
    /// panic or hang -- it stops and lets `rmp_serde` report the real
    /// error.
    #[test]
    fn check_msgpack_depth_never_panics_on_truncated_or_random_bytes() {
        for len in 0..64 {
            let mut buf = vec![0u8; len];
            for (i, b) in buf.iter_mut().enumerate() {
                *b = ((i * 37 + 11) % 256) as u8;
            }
            let _ = check_msgpack_depth(&buf, MAX_MSGPACK_DEPTH);
        }
        // A container header claiming far more data than exists.
        assert!(
            check_msgpack_depth(
                &[0xDF, 0xFF, 0xFF, 0xFF, 0xFF],
                MAX_MSGPACK_DEPTH
            )
            .is_ok()
        );
        assert!(
            check_msgpack_depth(
                &[0xDB, 0xFF, 0xFF, 0xFF, 0xFF],
                MAX_MSGPACK_DEPTH
            )
            .is_ok()
        );
    }
}
