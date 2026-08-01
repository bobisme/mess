//! Shared corpus builder for the CLI acceptance tests.
//!
//! Builds a real store with [`mess_store::LogEngine`] (real appends + a real
//! seal, producing genuine `.pidx`/`.pcol`/`.filter` sidecars), then — for the
//! verify tests — turns the active `.log` into a properly *sealed* segment by
//! writing its checksummed trailer, so the recovery scanner has ground truth
//! to detect body corruption against. The corruptors below are the same
//! byte-flip / truncate / overwrite operations the mess-log crash/torn
//! harnesses and the engine_reopen sidecar tests inject.

#![allow(dead_code)]

use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use mess_cli::scan::scan_segment;
use mess_cli::store;
use mess_log::sealer::{TrailerFields, encode_trailer};
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::engine::{EngineOptions, LogEngine};
use mess_store::version::Version;

/// The single interim active segment id the engine seals (`ACTIVE_SEGMENT_ID`).
pub const SEG_ID: u64 = 1;

/// `bn-2di`: how many global positions the `$registry` consumes in these
/// fixtures.
///
/// The log-derived registry writes a `StreamRegistered` the first time it sees
/// a stream name and an `EventTypeRegistered` the first time it sees a type
/// name, as ordinary log events — so they take global positions like anything
/// else. Every corpus here writes to exactly ONE stream with exactly ONE event
/// type, so that is exactly two records, both landing ahead of the first user
/// event. A store's watermark is therefore `user_events + REGISTRY_EVENTS`.
///
/// (These records are never *delivered*: `read_global` skips them. They are
/// visible only in position accounting like this.)
pub const REGISTRY_EVENTS: usize = 2;

fn rec(message_type: &str, data: &[u8]) -> RecordToAppend {
    RecordToAppend {
        message_type: message_type.into(),
        data:         data.to_vec(),
    }
}

/// Build a corpus at `dir`: append `n_batches` single-event batches to one
/// stream, then seal (writing the durable sidecars). Leaves the store closed
/// (lock released). Returns nothing; the caller inspects the files.
pub fn build_corpus(dir: &Path, n_batches: u64) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let opts = EngineOptions {
            // Small segments keep the preallocated file tiny; all our batches
            // fit inside one segment (they are a handful of bytes each).
            segment_size: 1 << 20,
            ..EngineOptions::default()
        };
        let engine = LogEngine::open_with(dir, opts).expect("open engine");
        for i in 0..n_batches {
            let expected =
                if i == 0 { Version::NoStream } else { Version::At(i - 1) };
            let payload = format!("event-{i}").into_bytes();
            engine
                .append_batch(
                    "acct-1",
                    expected,
                    &[rec("account.happened", &payload)],
                )
                .await
                .expect("append");
        }
        // Real seal: writes sealed/seg-*.pidx + .pcol + .filter.
        engine.seal_active().expect("seal");
    });
    // Engine dropped here → lock released.
}

/// Build a **pack-sealed** corpus at `dir` (bn-3of): the same appends as
/// [`build_corpus`], but with [`EngineOptions::seal_pack`] on, so the seal
/// emits one consolidated `sealed/seg-<id>.seal` and none of the
/// `.pidx`/`.pcol`/ `.filter` trio. Leaves the segment footerless —
/// [`seal_pack_footer`] writes the footer, which is the knob these tests need.
pub fn build_pack_corpus(dir: &Path, n_batches: u64) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let opts = EngineOptions {
            segment_size: 1 << 20,
            seal_pack: true,
            ..EngineOptions::default()
        };
        let engine = LogEngine::open_with(dir, opts).expect("open engine");
        for i in 0..n_batches {
            let expected =
                if i == 0 { Version::NoStream } else { Version::At(i - 1) };
            let payload = format!("event-{i}").into_bytes();
            engine
                .append_batch(
                    "acct-1",
                    expected,
                    &[rec("account.happened", &payload)],
                )
                .await
                .expect("append");
        }
        // Real seal, pack shape: writes sealed/seg-*.seal and nothing else.
        engine.seal_active().expect("seal");
    });
}

/// The consolidated SealPack path for the corpus segment.
pub fn seal(dir: &Path) -> std::path::PathBuf { store::seal_path(dir, SEG_ID) }

/// The identity the pack on disk hashes to — what any reader derives.
pub fn pack_identity(dir: &Path) -> [u8; 32] {
    *mess_index::sealed::SealedSegmentIndex::open_pack_eager(&seal(dir))
        .expect("pack opens")
        .pack_identity()
        .expect("a pack carries an identity")
        .as_bytes()
}

/// After [`build_pack_corpus`], write the segment footer the production sealer
/// writes: the §3.3.1 fixed trailer plus (bn-11g) the extension naming the
/// SealPack on disk, so the segment is bound to its exact pack.
///
/// `pack` names the identity to bind, defaulting to the real one via
/// [`seal_pack_footer`]; [`seal_pack_footer_unnamed`] writes the legacy
/// (pre-bn-11g, D-FMT-10) footer that names nothing.
fn write_pack_footer(dir: &Path, pack: Option<[u8; 32]>) {
    use mess_log::footer_ext::{
        SealPackIdentity, SealSummary, encode_sealed_footer,
    };
    use mess_log::format::SEAL_PACK_IDENTITY_HDRDIR_BLAKE3;

    let log = store::log_path(dir, SEG_ID);
    let s = scan_segment(SEG_ID, &log).expect("scan");
    let content_len = s.recovery.safe_offset;
    let summary = SealSummary {
        segment_id: SEG_ID,
        epoch: s.epoch().expect("epoch"),
        base_pos: s.base_pos().expect("base_pos"),
        batch_count: s.batch_count() as u64,
        event_count: s.event_count(),
        content_len,
    };
    let identity = pack.map(|identity| SealPackIdentity {
        identity_kind: SEAL_PACK_IDENTITY_HDRDIR_BLAKE3,
        pack_format_version: mess_index::sealed::PACK_FORMAT_VERSION,
        segment_id: SEG_ID,
        identity,
    });
    let (footer, _) =
        encode_sealed_footer(&summary, &[], &[], identity.as_ref());

    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&log)
        .expect("open log");
    f.set_len(content_len).expect("truncate to content");
    f.seek(SeekFrom::Start(content_len)).expect("seek");
    f.write_all(&footer).expect("write footer");
    f.sync_all().expect("sync");
}

/// Seal the corpus segment's footer, binding it to the pack now on disk.
pub fn seal_pack_footer(dir: &Path) {
    write_pack_footer(dir, Some(pack_identity(dir)));
}

/// Seal the corpus segment's footer WITHOUT naming a pack — the legacy
/// coverage-only policy (D-FMT-10).
pub fn seal_pack_footer_unnamed(dir: &Path) { write_pack_footer(dir, None); }

/// A ready-to-inspect pack-sealed store: a real `.seal` pack plus the footer
/// that names it. The shape a healthy `seal_pack` store has on disk.
pub fn build_sealed_pack_store(dir: &Path, n_batches: u64) {
    build_pack_corpus(dir, n_batches);
    seal_pack_footer(dir);
}

/// After [`build_corpus`], turn the active `.log` into a sealed segment by
/// writing its checksummed trailer (§3.3.1) at the recovered content length.
/// This gives `verify` the ground truth (batch/event counts, content length)
/// it cross-checks a corrupted body against.
pub fn seal_log_trailer(dir: &Path) {
    let log = store::log_path(dir, SEG_ID);
    let scan = scan_segment(SEG_ID, &log).expect("scan");
    let content_len = scan.recovery.safe_offset;
    let batch_count = scan.batch_count() as u64;
    let event_count = scan.event_count();
    let epoch = scan.epoch().expect("epoch");
    let base_pos = scan.base_pos().expect("base_pos");

    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&log)
        .expect("open log");
    f.set_len(content_len).expect("truncate to content");
    f.seek(SeekFrom::Start(content_len)).expect("seek");
    let trailer = encode_trailer(&TrailerFields::phase3(
        SEG_ID,
        epoch,
        base_pos,
        batch_count,
        event_count,
        content_len,
    ));
    f.write_all(&trailer).expect("write trailer");
    f.sync_all().expect("sync");
}

/// The sealed pointer-sidecar path for the corpus segment.
pub fn pidx(dir: &Path) -> std::path::PathBuf { store::pidx_path(dir, SEG_ID) }

/// The payload-sidecar path for the corpus segment.
pub fn pcol(dir: &Path) -> std::path::PathBuf {
    pidx(dir).with_extension("pcol")
}

// --- corruptors (reused byte-level injectors) -----------------------------

/// Flip one byte at `offset` (crash/torn-harness style bit corruption).
pub fn flip_byte(path: &Path, offset: u64) {
    let mut bytes = std::fs::read(path).expect("read");
    let i = offset as usize;
    assert!(
        i < bytes.len(),
        "flip offset {i} out of range (len {})",
        bytes.len()
    );
    bytes[i] ^= 0xFF;
    std::fs::write(path, bytes).expect("write");
}

/// Overwrite a file wholesale with garbage (engine_reopen sidecar corruptor).
pub fn overwrite(path: &Path, bytes: &[u8]) {
    std::fs::write(path, bytes).expect("overwrite");
}

/// Truncate a file to `len` bytes.
pub fn truncate(path: &Path, len: u64) {
    let f = std::fs::OpenOptions::new().write(true).open(path).expect("open");
    f.set_len(len).expect("truncate");
}

// --- bn-2za: parity sidecar helpers ---------------------------------------

use mess_index::sealed::parity::{ParityConfig, generate as generate_parity};

/// A small-shard parity config for the tiny corpus (so a few hundred bytes span
/// several groups): 64-byte shards, 4 data + 2 parity per group.
pub fn parity_test_cfg() -> ParityConfig {
    ParityConfig {
        enabled:          true,
        shard_size:       64,
        data_per_group:   4,
        parity_per_group: 2,
    }
}

/// Generate a `.par` parity sidecar over the (already sealed) segment's current
/// `.log` bytes and write it next to the pointer sidecar. Returns the sidecar
/// byte length. Call after [`seal_log_trailer`].
pub fn write_parity(dir: &Path, cfg: ParityConfig) -> u64 {
    let log = store::log_path(dir, SEG_ID);
    let bytes = std::fs::read(&log).expect("read log");
    let par_bytes =
        generate_parity(SEG_ID, &bytes, &cfg).expect("generate parity");
    let path = store::par_path(dir, SEG_ID);
    std::fs::create_dir_all(path.parent().expect("sealed parent"))
        .expect("mkdir sealed");
    let len = par_bytes.len() as u64;
    std::fs::write(&path, par_bytes).expect("write par");
    len
}

/// The parity-sidecar path for the corpus segment.
pub fn par(dir: &Path) -> std::path::PathBuf { store::par_path(dir, SEG_ID) }
