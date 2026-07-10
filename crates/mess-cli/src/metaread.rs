//! Read-only access to the durable metadata (`<dir>/meta`): the live snapshot
//! set (for `retention explain`), the per-snapshot `fold_version` (for
//! `doctor`'s drift check), and the stream/type name registry (for `inspect`).
//!
//! Opening the fjall metadata store takes fjall's own directory lock, so under
//! a live writer this open fails — every caller treats that as best-effort and
//! degrades to an advisory rather than a hard error.

use mess_index::meta::{MetaStore, StreamId};

/// One live snapshot decoded from the `snapshot_heads` table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveSnapshot {
    pub stream_id:           u64,
    /// The stream version this snapshot folds up to (its `covered_version`).
    pub version:             u64,
    /// The fold used to produce it (from the opaque snapshot ref, §9).
    pub fold_version:        u32,
    /// A §4.2 empty-prefix snapshot has no certification frames.
    pub covers_empty_prefix: bool,
}

/// Everything `retention`/`doctor`/`inspect` read out of the metadata store.
#[derive(Debug, Default)]
pub struct MetaFacts {
    /// Live snapshots (one per stream that has a snapshot head).
    pub snapshots:    Vec<LiveSnapshot>,
    /// `(stream_id, name)` interned pairs.
    pub stream_names: Vec<(u64, String)>,
    /// `(event_type_id, name)` interned pairs.
    pub type_names:   Vec<(u32, String)>,
}

/// The opaque snapshot-ref v1 layout (mirrors `mess_store::fjall_snapshot`):
/// `tag(1) || fold_version(4 LE) || flags(1) || snapshot_ptr(8 LE)`; flags bit
/// 0 = `covers_empty_prefix`. Returns `None` for any record this binary does
/// not understand (legacy / future / garbled) — treated as no usable snapshot.
fn decode_ref(bytes: &[u8]) -> Option<(u32, bool)> {
    const REF_FORMAT_V1: u8 = 0x01;
    const REF_V1_LEN: usize = 1 + 4 + 1 + 8;
    if bytes.len() != REF_V1_LEN || bytes[0] != REF_FORMAT_V1 {
        return None;
    }
    let fold_version = u32::from_le_bytes(bytes[1..5].try_into().ok()?);
    let covers_empty_prefix = bytes[5] != 0;
    Some((fold_version, covers_empty_prefix))
}

/// Open the metadata store read-only and pull the facts. Returns an error
/// string (never panics) when the store can't be opened — e.g. it is locked by
/// a live writer, or absent on a fresh directory.
pub fn read(dir: &std::path::Path) -> Result<MetaFacts, String> {
    let meta_path = crate::store::meta_dir(dir);
    if !meta_path.exists() {
        return Err(format!("no metadata store at {}", meta_path.display()));
    }
    let meta = MetaStore::open(&meta_path).map_err(|e| e.to_string())?;
    let stream_names = meta.stream_names().map_err(|e| e.to_string())?;
    let type_names = meta.type_names().map_err(|e| e.to_string())?;

    let mut snapshots = Vec::new();
    for (id, _name) in &stream_names {
        if let Ok(Some(head)) = meta.snapshot_head(StreamId(*id)) {
            let (fold_version, covers_empty_prefix) =
                decode_ref(&head.snapshot_ref).unwrap_or((0, false));
            snapshots.push(LiveSnapshot {
                stream_id: *id,
                version: head.covered_version,
                fold_version,
                covers_empty_prefix,
            });
        }
    }
    snapshots.sort_by_key(|s| s.stream_id);
    Ok(MetaFacts { snapshots, stream_names, type_names })
}
