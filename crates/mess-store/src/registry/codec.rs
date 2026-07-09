//! `codec_id 0` — the bootstrap codec, frozen forever (REG7).
//!
//! Every byte offset/width in this file is copied directly from
//! `docs/spec/04-registry.md` §3 and MUST NOT change: not field order, not
//! field width, not the meaning of an existing `record_kind` (REG7/REG8).
//! This is a hand-rolled, fixed-offset encoder/decoder — deliberately not
//! `postcard` or any other general-purpose codec, because REG7 requires a
//! byte-exact table-of-offsets format with no varint-driven layout (see
//! Decision D-REG-B, which rejects LEB128 precisely for this reason).
//!
//! `record_kind` (payload offset 0) selects which of the five frozen record
//! shapes follows; `0x00` and `>= 0x06` are permanently invalid (REG8).

use super::error::RegistryError;

/// `record_kind = 0x01` (§3.4).
pub const RECORD_KIND_STREAM_REGISTERED: u8 = 0x01;
/// `record_kind = 0x02` (§3.5).
pub const RECORD_KIND_EVENT_TYPE_REGISTERED: u8 = 0x02;
/// `record_kind = 0x03` (§3.6).
pub const RECORD_KIND_CATEGORY_REGISTERED: u8 = 0x03;
/// `record_kind = 0x04` (§3.7).
pub const RECORD_KIND_NAME_ALIASED: u8 = 0x04;
/// `record_kind = 0x05` (§3.8).
pub const RECORD_KIND_DICT_REGISTERED: u8 = 0x05;

/// `NameAliased.target_kind` / `DictRegistered.scope_kind` tag: stream.
pub const TARGET_KIND_STREAM: u8 = 1;
/// category.
pub const TARGET_KIND_CATEGORY: u8 = 2;
/// event_type.
pub const TARGET_KIND_EVENT_TYPE: u8 = 3;

/// The event type name every `stream_id == 0` frame carries (REG1): the
/// single event type `$registry` ever holds, regardless of `record_kind`.
pub const REGISTRY_EVENT_TYPE_NAME: &str = "RegistryEventV1";

/// The reserved stream name for `$registry` itself (REG1), used as the
/// [`Backend`](crate::backend::Backend)-level stream key this module reads
/// and appends to.
pub const REGISTRY_STREAM: &str = "$registry";

/// A decoded `codec_id 0` payload: one of the five frozen record shapes
/// (§3.3-§3.8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryRecord {
    /// §3.4.
    StreamRegistered { stream_id: u64, category_id: u64, name: String },
    /// §3.5.
    EventTypeRegistered {
        event_type_id: u32,
        codec_id: u16,
        schema_fingerprint: [u8; 32],
        name: String,
    },
    /// §3.6.
    CategoryRegistered { category_id: u64, name: String },
    /// §3.7. `target_kind` is one of the `TARGET_KIND_*` constants;
    /// `target_id` is always the full `u64`, zero-extended for the `u32`
    /// `event_type` namespace (Decision D-REG-E).
    NameAliased { target_kind: u8, target_id: u64, new_name: String },
    /// §3.8. `scope_kind` reuses `target_kind`'s tag values, restricted to
    /// `TARGET_KIND_CATEGORY` / `TARGET_KIND_EVENT_TYPE` (stream is not a
    /// legal dictionary scope).
    DictRegistered {
        dict_id: u16,
        scope_kind: u8,
        scope_id: u64,
        codec_id: u16,
        dict_bytes: Vec<u8>,
    },
}

impl RegistryRecord {
    /// The `record_kind` tag byte this record encodes to.
    #[must_use]
    pub fn record_kind(&self) -> u8 {
        match self {
            RegistryRecord::StreamRegistered { .. } => {
                RECORD_KIND_STREAM_REGISTERED
            }
            RegistryRecord::EventTypeRegistered { .. } => {
                RECORD_KIND_EVENT_TYPE_REGISTERED
            }
            RegistryRecord::CategoryRegistered { .. } => {
                RECORD_KIND_CATEGORY_REGISTERED
            }
            RegistryRecord::NameAliased { .. } => RECORD_KIND_NAME_ALIASED,
            RegistryRecord::DictRegistered { .. } => {
                RECORD_KIND_DICT_REGISTERED
            }
        }
    }

    /// Encode into the exact `codec_id 0` byte layout (§3.4-§3.8). Infallible:
    /// every field already has a fixed-width or length-prefixed shape, and
    /// string/blob lengths are bounds-checked by the writer-side registration
    /// API (`u16`/`u32`::MAX are astronomically larger than any real name or
    /// dictionary), not here.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        match self {
            RegistryRecord::StreamRegistered {
                stream_id,
                category_id,
                name,
            } => {
                let mut buf = Vec::with_capacity(19 + name.len());
                buf.push(RECORD_KIND_STREAM_REGISTERED);
                buf.extend_from_slice(&stream_id.to_le_bytes());
                buf.extend_from_slice(&category_id.to_le_bytes());
                write_str(&mut buf, name);
                buf
            }
            RegistryRecord::EventTypeRegistered {
                event_type_id,
                codec_id,
                schema_fingerprint,
                name,
            } => {
                let mut buf = Vec::with_capacity(41 + name.len());
                buf.push(RECORD_KIND_EVENT_TYPE_REGISTERED);
                buf.extend_from_slice(&event_type_id.to_le_bytes());
                buf.extend_from_slice(&codec_id.to_le_bytes());
                buf.extend_from_slice(schema_fingerprint);
                write_str(&mut buf, name);
                buf
            }
            RegistryRecord::CategoryRegistered { category_id, name } => {
                let mut buf = Vec::with_capacity(11 + name.len());
                buf.push(RECORD_KIND_CATEGORY_REGISTERED);
                buf.extend_from_slice(&category_id.to_le_bytes());
                write_str(&mut buf, name);
                buf
            }
            RegistryRecord::NameAliased {
                target_kind,
                target_id,
                new_name,
            } => {
                let mut buf = Vec::with_capacity(12 + new_name.len());
                buf.push(RECORD_KIND_NAME_ALIASED);
                buf.push(*target_kind);
                buf.extend_from_slice(&target_id.to_le_bytes());
                write_str(&mut buf, new_name);
                buf
            }
            RegistryRecord::DictRegistered {
                dict_id,
                scope_kind,
                scope_id,
                codec_id,
                dict_bytes,
            } => {
                let mut buf = Vec::with_capacity(18 + dict_bytes.len());
                buf.push(RECORD_KIND_DICT_REGISTERED);
                buf.extend_from_slice(&dict_id.to_le_bytes());
                buf.push(*scope_kind);
                buf.extend_from_slice(&scope_id.to_le_bytes());
                buf.extend_from_slice(&codec_id.to_le_bytes());
                write_blob(&mut buf, dict_bytes);
                buf
            }
        }
    }

    /// Decode a `codec_id 0` payload per §3.3-§3.8. This function is the
    /// bootstrap-acyclicity base case (§7.1 step 2): it consults nothing but
    /// `payload` and this module's compiled-in constants — never a registry
    /// table, live or otherwise.
    pub fn decode<E>(payload: &[u8]) -> Result<Self, RegistryError<E>> {
        let Some(&record_kind) = payload.first() else {
            return Err(RegistryError::PayloadTooShort {
                record_kind: None,
                need: 1,
                got: 0,
            });
        };
        match record_kind {
            RECORD_KIND_STREAM_REGISTERED => {
                need(payload, record_kind, 19)?;
                let stream_id = read_u64(payload, 1);
                let category_id = read_u64(payload, 9);
                let name = read_str(payload, record_kind, 17)?;
                Ok(RegistryRecord::StreamRegistered {
                    stream_id,
                    category_id,
                    name,
                })
            }
            RECORD_KIND_EVENT_TYPE_REGISTERED => {
                need(payload, record_kind, 41)?;
                let event_type_id = read_u32(payload, 1);
                let codec_id = read_u16(payload, 5);
                let mut schema_fingerprint = [0u8; 32];
                schema_fingerprint.copy_from_slice(&payload[7..39]);
                let name = read_str(payload, record_kind, 39)?;
                Ok(RegistryRecord::EventTypeRegistered {
                    event_type_id,
                    codec_id,
                    schema_fingerprint,
                    name,
                })
            }
            RECORD_KIND_CATEGORY_REGISTERED => {
                need(payload, record_kind, 11)?;
                let category_id = read_u64(payload, 1);
                let name = read_str(payload, record_kind, 9)?;
                Ok(RegistryRecord::CategoryRegistered { category_id, name })
            }
            RECORD_KIND_NAME_ALIASED => {
                need(payload, record_kind, 12)?;
                let target_kind = payload[1];
                let target_id = read_u64(payload, 2);
                let new_name = read_str(payload, record_kind, 10)?;
                Ok(RegistryRecord::NameAliased {
                    target_kind,
                    target_id,
                    new_name,
                })
            }
            RECORD_KIND_DICT_REGISTERED => {
                need(payload, record_kind, 18)?;
                let dict_id = read_u16(payload, 1);
                let scope_kind = payload[3];
                let scope_id = read_u64(payload, 4);
                let codec_id = read_u16(payload, 12);
                let dict_bytes = read_blob(payload, record_kind, 14)?;
                Ok(RegistryRecord::DictRegistered {
                    dict_id,
                    scope_kind,
                    scope_id,
                    codec_id,
                    dict_bytes,
                })
            }
            other => Err(RegistryError::UnknownRecordKind(other)),
        }
    }
}

/// Check `payload` is at least `min_len` bytes (the fixed-offset prefix
/// before any variable-length field), else a decode error.
fn need<E>(
    payload: &[u8],
    record_kind: u8,
    min_len: usize,
) -> Result<(), RegistryError<E>> {
    if payload.len() < min_len {
        return Err(RegistryError::PayloadTooShort {
            record_kind: Some(record_kind),
            need: min_len,
            got: payload.len(),
        });
    }
    Ok(())
}

fn read_u16(payload: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(payload[at..at + 2].try_into().unwrap())
}

fn read_u32(payload: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(payload[at..at + 4].try_into().unwrap())
}

fn read_u64(payload: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(payload[at..at + 8].try_into().unwrap())
}

/// Read a `str` field (§3.1: `u16` LE length prefix + raw UTF-8 bytes) whose
/// length prefix starts at `len_at`.
fn read_str<E>(
    payload: &[u8],
    record_kind: u8,
    len_at: usize,
) -> Result<String, RegistryError<E>> {
    let len = read_u16(payload, len_at) as usize;
    let start = len_at + 2;
    let end = start.checked_add(len).ok_or(
        RegistryError::TrailingLengthMismatch {
            record_kind,
            need: len,
            got: payload.len().saturating_sub(start),
        },
    )?;
    let bytes = payload.get(start..end).ok_or(
        RegistryError::TrailingLengthMismatch {
            record_kind,
            need: len,
            got: payload.len().saturating_sub(start),
        },
    )?;
    String::from_utf8(bytes.to_vec())
        .map_err(|_| RegistryError::InvalidUtf8 { record_kind })
}

/// Read a `blob` field (§3.1: `u32` LE length prefix + raw bytes) whose
/// length prefix starts at `len_at`.
fn read_blob<E>(
    payload: &[u8],
    record_kind: u8,
    len_at: usize,
) -> Result<Vec<u8>, RegistryError<E>> {
    let len =
        u32::from_le_bytes(payload[len_at..len_at + 4].try_into().unwrap())
            as usize;
    let start = len_at + 4;
    let end = start.checked_add(len).ok_or(
        RegistryError::TrailingLengthMismatch {
            record_kind,
            need: len,
            got: payload.len().saturating_sub(start),
        },
    )?;
    let bytes = payload.get(start..end).ok_or(
        RegistryError::TrailingLengthMismatch {
            record_kind,
            need: len,
            got: payload.len().saturating_sub(start),
        },
    )?;
    Ok(bytes.to_vec())
}

/// Write a `str` field: `u16` LE length prefix + raw UTF-8 bytes (§3.1).
fn write_str(buf: &mut Vec<u8>, s: &str) {
    let len: u16 = s
        .len()
        .try_into()
        .expect("registry name exceeds u16::MAX bytes (§3.1 max length)");
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

/// Write a `blob` field: `u32` LE length prefix + raw bytes (§3.1).
fn write_blob(buf: &mut Vec<u8>, bytes: &[u8]) {
    let len: u32 = bytes
        .len()
        .try_into()
        .expect("registry dict_bytes exceeds u32::MAX bytes (§3.1)");
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// §3.9 worked example, verbatim: `CategoryRegistered { category_id: 1,
    /// name: "orders" }` must encode to exactly these 17 bytes.
    #[test]
    fn golden_worked_example_category_registered() {
        let record = RegistryRecord::CategoryRegistered {
            category_id: 1,
            name: "orders".to_string(),
        };
        let bytes = record.encode();
        let expected: [u8; 17] = [
            0x03, // record_kind
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, // category_id = 1
            0x06, 0x00, // name_len = 6
            0x6F, 0x72, 0x64, 0x65, 0x72, 0x73, // "orders"
        ];
        assert_eq!(bytes, expected);
        assert_eq!(bytes.len(), 17);

        let decoded: RegistryRecord =
            RegistryRecord::decode::<std::convert::Infallible>(&bytes)
                .expect("decode golden bytes");
        assert_eq!(decoded, record);
    }

    #[test]
    fn round_trips_stream_registered() {
        let record = RegistryRecord::StreamRegistered {
            stream_id: 7,
            category_id: 1,
            name: "orders.stream-42".to_string(),
        };
        let bytes = record.encode();
        assert_eq!(bytes.len(), 19 + "orders.stream-42".len());
        let decoded =
            RegistryRecord::decode::<std::convert::Infallible>(&bytes).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn round_trips_event_type_registered() {
        let record = RegistryRecord::EventTypeRegistered {
            event_type_id: 3,
            codec_id: 1,
            schema_fingerprint: [0xAB; 32],
            name: "orders.OrderPlaced".to_string(),
        };
        let bytes = record.encode();
        assert_eq!(bytes.len(), 41 + "orders.OrderPlaced".len());
        let decoded =
            RegistryRecord::decode::<std::convert::Infallible>(&bytes).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn round_trips_name_aliased() {
        let record = RegistryRecord::NameAliased {
            target_kind: TARGET_KIND_STREAM,
            target_id: 7,
            new_name: "orders.renamed".to_string(),
        };
        let bytes = record.encode();
        assert_eq!(bytes.len(), 12 + "orders.renamed".len());
        let decoded =
            RegistryRecord::decode::<std::convert::Infallible>(&bytes).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn round_trips_dict_registered() {
        let record = RegistryRecord::DictRegistered {
            dict_id: 1,
            scope_kind: TARGET_KIND_CATEGORY,
            scope_id: 1,
            codec_id: 1,
            dict_bytes: vec![1, 2, 3, 4, 5],
        };
        let bytes = record.encode();
        assert_eq!(bytes.len(), 18 + 5);
        let decoded =
            RegistryRecord::decode::<std::convert::Infallible>(&bytes).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn rejects_reserved_record_kind_zero() {
        let err = RegistryRecord::decode::<std::convert::Infallible>(&[0x00])
            .unwrap_err();
        assert_eq!(err, RegistryError::UnknownRecordKind(0x00));
    }

    #[test]
    fn rejects_out_of_range_record_kind() {
        let err = RegistryRecord::decode::<std::convert::Infallible>(&[0x06])
            .unwrap_err();
        assert_eq!(err, RegistryError::UnknownRecordKind(0x06));
        let err = RegistryRecord::decode::<std::convert::Infallible>(&[0xFF])
            .unwrap_err();
        assert_eq!(err, RegistryError::UnknownRecordKind(0xFF));
    }

    #[test]
    fn rejects_empty_payload() {
        let err = RegistryRecord::decode::<std::convert::Infallible>(&[])
            .unwrap_err();
        assert_eq!(
            err,
            RegistryError::PayloadTooShort {
                record_kind: None,
                need: 1,
                got: 0
            }
        );
    }

    #[test]
    fn rejects_truncated_payload() {
        // record_kind says CategoryRegistered but only 3 bytes follow.
        let err = RegistryRecord::decode::<std::convert::Infallible>(&[
            0x03, 0x01, 0x00,
        ])
        .unwrap_err();
        assert!(matches!(
            err,
            RegistryError::PayloadTooShort { record_kind: Some(0x03), .. }
        ));
    }

    #[test]
    fn rejects_invalid_utf8_name() {
        let mut bytes = vec![0x03u8];
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&2u16.to_le_bytes()); // name_len = 2
        bytes.extend_from_slice(&[0xFF, 0xFE]); // invalid UTF-8
        let err = RegistryRecord::decode::<std::convert::Infallible>(&bytes)
            .unwrap_err();
        assert_eq!(
            err,
            RegistryError::InvalidUtf8 {
                record_kind: RECORD_KIND_CATEGORY_REGISTERED
            }
        );
    }

    #[test]
    fn rejects_length_prefix_past_end_of_payload() {
        let mut bytes = vec![0x03u8];
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&100u16.to_le_bytes()); // claims 100 bytes
        bytes.extend_from_slice(b"short");
        let err = RegistryRecord::decode::<std::convert::Infallible>(&bytes)
            .unwrap_err();
        assert!(matches!(
            err,
            RegistryError::TrailingLengthMismatch { record_kind: 0x03, .. }
        ));
    }
}
