//! The v4 control-record TLV codec (§7–§14): decode/encode the frozen control
//! primitives that ride in a capsule's control region.
//!
//! # Layering (review V6/S4)
//!
//! This module owns only the **byte layer** of a control record: the
//! `(kind, version, payload_len)` TLV frame (§7) and each kind's fixed-offset
//! payload (§8). It deliberately does **not** own registry *semantics* —
//! whether `stream_id == next_stream_id`, whether a name is already bound to a
//! different ID, the REG14 no-double-register rule. Those belong to the
//! [`RegistryState`] replay machine in `mess-store`, which this crate cannot
//! depend on (the dependency runs the other way). The recovery scanner reaches
//! that machine through the [`crate::v4::recover::RegistryView`] seam, whose
//! production impl (in `mess-store`) folds these decoded records into
//! `RegistryState`. Here we only prove a record's *bytes* are well-formed and
//! within their §19 caps.
//!
//! # Criticality (§23.6)
//!
//! Every v4 control is **critical**: an unknown `(kind, version)` is a hard
//! decode error ([`ControlDecodeError::UnknownKindVersion`]) — recovery stops,
//! it never skips a state-changing record it cannot interpret.

use super::format::*;

/// A decoded control record (§9). Byte-level only — the semantic REG-rules are
/// the [`RegistryView`](super::recover::RegistryView) seam's job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlRecord {
    /// `StreamRegisteredV1` (§10): assign a dense stream ID + canonical
    /// name/category.
    StreamRegistered { stream_id: u64, category_id: u64, name: String },
    /// `EventTypeRegisteredV1` (§11): assign a type ID, name, codec/schema
    /// metadata.
    EventTypeRegistered {
        event_type_id:          u32,
        codec_id:               u16,
        current_schema_version: u16,
        schema_fingerprint:     [u8; 32],
        name:                   String,
    },
    /// `CategoryRegisteredV1` (§9): assign a category ID/name.
    CategoryRegistered { category_id: u64, name: String },
    /// `DedupeKeyV1` (§12): the exact key covering this capsule.
    DedupeKey { scope_kind: u8, scope_id: u64, key: Vec<u8> },
    /// `SnapshotInstalledV1` (§13): advance a snapshot head to a durable blob.
    /// The three hashes are optional (review V4 hash-presence flags).
    SnapshotInstalled {
        stream_id:               u64,
        covered_version:         u64,
        covered_global_position: u64,
        snapshot_slot:           u64,
        pack_id:                 u64,
        pack_offset:             u64,
        blob_len:                u32,
        codec_id:                u16,
        fold_version:            u32,
        state_hash:              Option<[u8; 32]>,
        event_prefix_hash:       Option<[u8; 32]>,
        blob_hash:               Option<[u8; 32]>,
    },
    /// `ProjectionCheckpointV1` (§14): advance a position/frontier.
    ProjectionCheckpoint {
        projection_id:  u64,
        position:       u64,
        state_ref_kind: u16,
        state_ref:      Vec<u8>,
    },
}

/// Why a control record's bytes are not well-formed. Every variant makes the
/// containing capsule invalid (recovery stops — §23.6, controls are critical).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ControlDecodeError {
    /// The TLV frame or a fixed-offset payload prefix is shorter than required.
    #[error("control record too short: need {need} bytes, got {got}")]
    TooShort { need: usize, got: usize },
    /// The `(kind, version)` pair is not one this build knows (§23.6). Recovery
    /// stops rather than skip a state-changing record it cannot interpret.
    #[error("unknown control (kind={kind:#06x}, version={version})")]
    UnknownKindVersion { kind: u16, version: u16 },
    /// A `payload_len` in the TLV frame does not equal the payload the fixed
    /// kind actually needs (variable fields included) — the record does not
    /// tile the declared frame exactly.
    #[error(
        "control payload length mismatch for kind {kind:#06x}: frame declares \
         {declared}, decode consumed {consumed}"
    )]
    PayloadLenMismatch { kind: u16, declared: usize, consumed: usize },
    /// A length-prefixed field (name / key / state_ref) runs past the payload.
    #[error("length-prefixed field overruns payload: need {need}, got {got}")]
    FieldOverrun { need: usize, got: usize },
    /// A length-prefixed field exceeds its §19 cap.
    #[error("field exceeds cap: {len} > {cap}")]
    FieldTooLong { len: usize, cap: usize },
    /// A name field is not valid UTF-8 (§8: names match the registry spec).
    #[error("control name is not valid UTF-8")]
    InvalidUtf8,
    /// A reserved byte/word that MUST be zero was nonzero.
    #[error("reserved field nonzero in control kind {kind:#06x}")]
    ReservedNonzero { kind: u16 },
    /// A `DedupeKeyV1.scope_kind` outside the known set.
    #[error("invalid dedupe scope_kind {0}")]
    BadDedupeScope(u8),
    /// A `SnapshotInstalledV1.hash_presence` bit outside the known mask.
    #[error("unknown snapshot hash-presence bits {0:#04x}")]
    BadHashPresence(u8),
}

/// The exact on-disk byte length of one control record (TLV header + payload),
/// so an encoder can size the region and a decoder can advance by it.
impl ControlRecord {
    /// This record's `kind` tag (§9).
    #[must_use]
    pub fn kind(&self) -> u16 {
        match self {
            ControlRecord::StreamRegistered { .. } => CTL_STREAM_REGISTERED,
            ControlRecord::EventTypeRegistered { .. } => {
                CTL_EVENT_TYPE_REGISTERED
            }
            ControlRecord::CategoryRegistered { .. } => CTL_CATEGORY_REGISTERED,
            ControlRecord::DedupeKey { .. } => CTL_DEDUPE_KEY,
            ControlRecord::SnapshotInstalled { .. } => CTL_SNAPSHOT_INSTALLED,
            ControlRecord::ProjectionCheckpoint { .. } => {
                CTL_PROJECTION_CHECKPOINT
            }
        }
    }

    /// The payload byte length (excluding the 8-byte TLV header).
    #[must_use]
    pub fn payload_len(&self) -> usize {
        match self {
            ControlRecord::StreamRegistered { name, .. } => {
                8 + 8 + 2 + name.len()
            }
            ControlRecord::EventTypeRegistered { name, .. } => {
                4 + 2 + 2 + 32 + 2 + name.len()
            }
            ControlRecord::CategoryRegistered { name, .. } => {
                8 + 2 + name.len()
            }
            ControlRecord::DedupeKey { key, .. } => 1 + 3 + 8 + 4 + key.len(),
            ControlRecord::SnapshotInstalled {
                state_hash,
                event_prefix_hash,
                blob_hash,
                ..
            } => {
                // stream_id..fold_version fixed prefix + presence + 3 reserved
                let fixed = 8 + 8 + 8 + 8 + 8 + 8 + 4 + 2 + 4 + 1 + 3;
                let hashes = 32
                    * (usize::from(state_hash.is_some())
                        + usize::from(event_prefix_hash.is_some())
                        + usize::from(blob_hash.is_some()));
                fixed + hashes
            }
            ControlRecord::ProjectionCheckpoint { state_ref, .. } => {
                8 + 8 + 2 + 2 + 4 + state_ref.len()
            }
        }
    }

    /// Total on-disk length: TLV header + payload.
    #[must_use]
    pub fn on_disk_len(&self) -> usize {
        CONTROL_TLV_HDR_LEN + self.payload_len()
    }

    /// Encode this record's TLV frame (`kind | version | payload_len |
    /// payload`) into `buf`. Always emits `CONTROL_VERSION_V1`.
    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        let payload_len = self.payload_len();
        buf.extend_from_slice(&self.kind().to_le_bytes());
        buf.extend_from_slice(&CONTROL_VERSION_V1.to_le_bytes());
        buf.extend_from_slice(&(payload_len as u32).to_le_bytes());
        let start = buf.len();
        self.encode_payload(buf);
        debug_assert_eq!(
            buf.len() - start,
            payload_len,
            "payload_len mismatch"
        );
    }

    fn encode_payload(&self, buf: &mut Vec<u8>) {
        match self {
            ControlRecord::StreamRegistered {
                stream_id,
                category_id,
                name,
            } => {
                buf.extend_from_slice(&stream_id.to_le_bytes());
                buf.extend_from_slice(&category_id.to_le_bytes());
                write_name(buf, name);
            }
            ControlRecord::EventTypeRegistered {
                event_type_id,
                codec_id,
                current_schema_version,
                schema_fingerprint,
                name,
            } => {
                buf.extend_from_slice(&event_type_id.to_le_bytes());
                buf.extend_from_slice(&codec_id.to_le_bytes());
                buf.extend_from_slice(&current_schema_version.to_le_bytes());
                buf.extend_from_slice(schema_fingerprint);
                write_name(buf, name);
            }
            ControlRecord::CategoryRegistered { category_id, name } => {
                buf.extend_from_slice(&category_id.to_le_bytes());
                write_name(buf, name);
            }
            ControlRecord::DedupeKey { scope_kind, scope_id, key } => {
                buf.push(*scope_kind);
                buf.extend_from_slice(&[0u8; 3]); // reserved
                buf.extend_from_slice(&scope_id.to_le_bytes());
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
            }
            ControlRecord::SnapshotInstalled {
                stream_id,
                covered_version,
                covered_global_position,
                snapshot_slot,
                pack_id,
                pack_offset,
                blob_len,
                codec_id,
                fold_version,
                state_hash,
                event_prefix_hash,
                blob_hash,
            } => {
                buf.extend_from_slice(&stream_id.to_le_bytes());
                buf.extend_from_slice(&covered_version.to_le_bytes());
                buf.extend_from_slice(&covered_global_position.to_le_bytes());
                buf.extend_from_slice(&snapshot_slot.to_le_bytes());
                buf.extend_from_slice(&pack_id.to_le_bytes());
                buf.extend_from_slice(&pack_offset.to_le_bytes());
                buf.extend_from_slice(&blob_len.to_le_bytes());
                buf.extend_from_slice(&codec_id.to_le_bytes());
                buf.extend_from_slice(&fold_version.to_le_bytes());
                let mut presence = 0u8;
                if state_hash.is_some() {
                    presence |= SNAP_HAS_STATE_HASH;
                }
                if event_prefix_hash.is_some() {
                    presence |= SNAP_HAS_EVENT_PREFIX_HASH;
                }
                if blob_hash.is_some() {
                    presence |= SNAP_HAS_BLOB_HASH;
                }
                buf.push(presence);
                buf.extend_from_slice(&[0u8; 3]); // reserved
                if let Some(h) = state_hash {
                    buf.extend_from_slice(h);
                }
                if let Some(h) = event_prefix_hash {
                    buf.extend_from_slice(h);
                }
                if let Some(h) = blob_hash {
                    buf.extend_from_slice(h);
                }
            }
            ControlRecord::ProjectionCheckpoint {
                projection_id,
                position,
                state_ref_kind,
                state_ref,
            } => {
                buf.extend_from_slice(&projection_id.to_le_bytes());
                buf.extend_from_slice(&position.to_le_bytes());
                buf.extend_from_slice(&state_ref_kind.to_le_bytes());
                buf.extend_from_slice(&0u16.to_le_bytes()); // reserved
                buf.extend_from_slice(&(state_ref.len() as u32).to_le_bytes());
                buf.extend_from_slice(state_ref);
            }
        }
    }

    /// Decode one control record starting at `off` in `region`, returning the
    /// record and the number of bytes consumed (`on_disk_len`). Every length is
    /// checked against its §19 cap **before** slicing (requirement: all
    /// arithmetic checked before allocation).
    pub fn decode_at(
        region: &[u8],
        off: usize,
    ) -> Result<(ControlRecord, usize), ControlDecodeError> {
        let rem = region.len().checked_sub(off).ok_or(
            ControlDecodeError::TooShort { need: CONTROL_TLV_HDR_LEN, got: 0 },
        )?;
        if rem < CONTROL_TLV_HDR_LEN {
            return Err(ControlDecodeError::TooShort {
                need: CONTROL_TLV_HDR_LEN,
                got:  rem,
            });
        }
        let kind = rd_u16(region, off + CT_KIND_OFF);
        let version = rd_u16(region, off + CT_VERSION_OFF);
        let payload_len = rd_u32(region, off + CT_PAYLOAD_LEN_OFF) as usize;

        // Cap the declared payload BEFORE using it to slice (§7: all arithmetic
        // checked before slicing/allocation).
        if payload_len > MAX_CONTROL_LEN as usize {
            return Err(ControlDecodeError::FieldTooLong {
                len: payload_len,
                cap: MAX_CONTROL_LEN as usize,
            });
        }
        let payload_start = off + CONTROL_TLV_HDR_LEN;
        let payload_end = payload_start.checked_add(payload_len).ok_or(
            ControlDecodeError::FieldOverrun { need: payload_len, got: 0 },
        )?;
        if payload_end > region.len() {
            return Err(ControlDecodeError::FieldOverrun {
                need: payload_len,
                got:  region.len().saturating_sub(payload_start),
            });
        }
        let payload = &region[payload_start..payload_end];

        let record = decode_payload(kind, version, payload)?;
        // Exact-tiling cross-check: the frame's declared payload_len must equal
        // what the kind actually consumes (variable fields included).
        let consumed = record.payload_len();
        if consumed != payload_len {
            return Err(ControlDecodeError::PayloadLenMismatch {
                kind,
                declared: payload_len,
                consumed,
            });
        }
        Ok((record, CONTROL_TLV_HDR_LEN + payload_len))
    }
}

fn decode_payload(
    kind: u16,
    version: u16,
    p: &[u8],
) -> Result<ControlRecord, ControlDecodeError> {
    if version != CONTROL_VERSION_V1 {
        return Err(ControlDecodeError::UnknownKindVersion { kind, version });
    }
    match kind {
        CTL_STREAM_REGISTERED => {
            need(p, 18)?;
            let stream_id = rd_u64(p, 0);
            let category_id = rd_u64(p, 8);
            let name = read_name(p, 16)?;
            Ok(ControlRecord::StreamRegistered { stream_id, category_id, name })
        }
        CTL_EVENT_TYPE_REGISTERED => {
            need(p, 42)?;
            let event_type_id = rd_u32(p, 0);
            let codec_id = rd_u16(p, 4);
            let current_schema_version = rd_u16(p, 6);
            let mut schema_fingerprint = [0u8; 32];
            schema_fingerprint.copy_from_slice(&p[8..40]);
            let name = read_name(p, 40)?;
            Ok(ControlRecord::EventTypeRegistered {
                event_type_id,
                codec_id,
                current_schema_version,
                schema_fingerprint,
                name,
            })
        }
        CTL_CATEGORY_REGISTERED => {
            need(p, 10)?;
            let category_id = rd_u64(p, 0);
            let name = read_name(p, 8)?;
            Ok(ControlRecord::CategoryRegistered { category_id, name })
        }
        CTL_DEDUPE_KEY => {
            need(p, 16)?;
            let scope_kind = p[0];
            if p[1..4] != [0u8; 3] {
                return Err(ControlDecodeError::ReservedNonzero { kind });
            }
            if scope_kind != DEDUPE_SCOPE_STREAM
                && scope_kind != DEDUPE_SCOPE_GLOBAL
            {
                return Err(ControlDecodeError::BadDedupeScope(scope_kind));
            }
            let scope_id = rd_u64(p, 4);
            let key = read_blob(p, 12, MAX_DEDUPE_KEY_LEN)?;
            Ok(ControlRecord::DedupeKey { scope_kind, scope_id, key })
        }
        CTL_SNAPSHOT_INSTALLED => {
            // Fixed prefix is 62 bytes: stream_id(8) covered_version(8)
            // covered_global_position(8) snapshot_slot(8) pack_id(8)
            // pack_offset(8) blob_len(4) codec_id(2) fold_version(4)
            // presence(1) reserved(3); the present hashes follow (§13).
            need(p, 62)?;
            let stream_id = rd_u64(p, 0);
            let covered_version = rd_u64(p, 8);
            let covered_global_position = rd_u64(p, 16);
            let snapshot_slot = rd_u64(p, 24);
            let pack_id = rd_u64(p, 32);
            let pack_offset = rd_u64(p, 40);
            let blob_len = rd_u32(p, 48);
            let codec_id = rd_u16(p, 52);
            let fold_version = rd_u32(p, 54);
            let presence = p[58];
            if presence & !SNAP_HASH_PRESENCE_KNOWN_MASK != 0 {
                return Err(ControlDecodeError::BadHashPresence(presence));
            }
            if p[59..62] != [0u8; 3] {
                return Err(ControlDecodeError::ReservedNonzero { kind });
            }
            let mut cur = 62;
            let mut take_hash =
                |flag: u8| -> Result<Option<[u8; 32]>, ControlDecodeError> {
                    if presence & flag != 0 {
                        if cur + 32 > p.len() {
                            return Err(ControlDecodeError::TooShort {
                                need: cur + 32,
                                got:  p.len(),
                            });
                        }
                        let mut h = [0u8; 32];
                        h.copy_from_slice(&p[cur..cur + 32]);
                        cur += 32;
                        Ok(Some(h))
                    } else {
                        Ok(None)
                    }
                };
            let state_hash = take_hash(SNAP_HAS_STATE_HASH)?;
            let event_prefix_hash = take_hash(SNAP_HAS_EVENT_PREFIX_HASH)?;
            let blob_hash = take_hash(SNAP_HAS_BLOB_HASH)?;
            Ok(ControlRecord::SnapshotInstalled {
                stream_id,
                covered_version,
                covered_global_position,
                snapshot_slot,
                pack_id,
                pack_offset,
                blob_len,
                codec_id,
                fold_version,
                state_hash,
                event_prefix_hash,
                blob_hash,
            })
        }
        CTL_PROJECTION_CHECKPOINT => {
            need(p, 20)?;
            let projection_id = rd_u64(p, 0);
            let position = rd_u64(p, 8);
            let state_ref_kind = rd_u16(p, 16);
            if rd_u16(p, 18) != 0 {
                return Err(ControlDecodeError::ReservedNonzero { kind });
            }
            let state_ref = read_blob(p, 20, MAX_STATE_REF_LEN)?;
            Ok(ControlRecord::ProjectionCheckpoint {
                projection_id,
                position,
                state_ref_kind,
                state_ref,
            })
        }
        _ => Err(ControlDecodeError::UnknownKindVersion { kind, version }),
    }
}

fn need(p: &[u8], min: usize) -> Result<(), ControlDecodeError> {
    if p.len() < min {
        Err(ControlDecodeError::TooShort { need: min, got: p.len() })
    } else {
        Ok(())
    }
}

/// Read a `Bytes16` name (u16 length + UTF-8 bytes) at `len_at`.
fn read_name(p: &[u8], len_at: usize) -> Result<String, ControlDecodeError> {
    if len_at + 2 > p.len() {
        return Err(ControlDecodeError::TooShort {
            need: len_at + 2,
            got:  p.len(),
        });
    }
    let len = rd_u16(p, len_at) as usize;
    if len > MAX_NAME_LEN {
        return Err(ControlDecodeError::FieldTooLong { len, cap: MAX_NAME_LEN });
    }
    let start = len_at + 2;
    let end = start
        .checked_add(len)
        .ok_or(ControlDecodeError::FieldOverrun { need: len, got: 0 })?;
    let bytes = p.get(start..end).ok_or(ControlDecodeError::FieldOverrun {
        need: len,
        got:  p.len().saturating_sub(start),
    })?;
    String::from_utf8(bytes.to_vec())
        .map_err(|_| ControlDecodeError::InvalidUtf8)
}

/// Read a `Bytes32` blob (u32 length + bytes) at `len_at`, capped at `cap`.
fn read_blob(
    p: &[u8],
    len_at: usize,
    cap: usize,
) -> Result<Vec<u8>, ControlDecodeError> {
    if len_at + 4 > p.len() {
        return Err(ControlDecodeError::TooShort {
            need: len_at + 4,
            got:  p.len(),
        });
    }
    let len = rd_u32(p, len_at) as usize;
    if len > cap {
        return Err(ControlDecodeError::FieldTooLong { len, cap });
    }
    let start = len_at + 4;
    let end = start
        .checked_add(len)
        .ok_or(ControlDecodeError::FieldOverrun { need: len, got: 0 })?;
    let bytes = p.get(start..end).ok_or(ControlDecodeError::FieldOverrun {
        need: len,
        got:  p.len().saturating_sub(start),
    })?;
    Ok(bytes.to_vec())
}

/// Write a `Bytes16` name field.
///
/// # Panics
/// If `name` exceeds `u16::MAX` bytes (the writer-side registration API bounds
/// this long before it reaches here; a longer name is a caller bug).
fn write_name(buf: &mut Vec<u8>, name: &str) {
    let len: u16 = name
        .len()
        .try_into()
        .expect("control name exceeds u16::MAX bytes (§19 MAX_NAME_LEN)");
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(name.as_bytes());
}

#[inline]
fn rd_u16(d: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(d[o..o + 2].try_into().unwrap())
}
#[inline]
fn rd_u32(d: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(d[o..o + 4].try_into().unwrap())
}
#[inline]
fn rd_u64(d: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(d[o..o + 8].try_into().unwrap())
}
