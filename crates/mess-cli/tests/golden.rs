//! bn-yvi — format-stability e2e: golden store directories.
//!
//! On-disk compatibility IS the contract. This file pins it two ways:
//!
//! - [`generate_golden_v3`] (`#[ignore]`, run by hand) builds a small but
//!   COMPLETE deterministic store with the production [`LogEngine`] — multiple
//!   named streams, a tiny (16 KiB) active segment so the log genuinely rolls
//!   and the background sealer produces sealed segments (columnar `.pcol` +
//!   pointer `.pidx` + a finalized fixed trailer), the fjall meta name registry
//!   (`stream_names`/`type_names`), and a snapshot carrying a `fold_version` in
//!   a [`FjallSnapshotBackend`] nested under the store dir. It records an
//!   `expected-events.json` manifest and packs the store dir as `store.tar.zst`
//!   under `tests/golden/v3/`.
//!
//! - [`golden_v3_opens_and_verifies`] (runs in normal CI) unpacks the committed
//!   v3 golden, opens it with the CURRENT code, and proves: full recovery scan
//!   is green, the registry + names hydrated, every event replays byte-exact
//!   against the manifest, sealed streams are served from the COLD tier, the
//!   snapshot loads with its `fold_version`, `load_verified` is green on the
//!   chained stream (with its fold-chain hashes pinned in the manifest), and
//!   `mess verify --full` exits 0.
//!
//! # The crypto chain / `load_verified` scope note
//!
//! The composed [`LogEngine`] backend does not itself emit the optional
//! per-batch `crypto_chain` bytes (`docs/spec/01` §4.4) into its segments — its
//! append path writes plain frames. So the fold-certificate contract is pinned
//! against the SAME committed corpus a different way: the chained stream's
//! on-disk payloads are re-fed through `mess-log`'s spec-conformant
//! [`build_cert`] construction and verified with [`load_verified`], and the
//! resulting genesis/chain/head hashes (BLAKE3, byte-exact to §3 of spec 05)
//! are frozen in the manifest. A change to the fold-chain byte derivation
//! breaks the committed hashes exactly as a segment-format change breaks the
//! open/replay path. When the engine grows a real on-disk chain, a new `vN`
//! golden supersedes this construction (see `tests/golden/README.md`).
//!
//! # Determinism
//!
//! The corpus is fixed (no wall-clock in any payload; single-threaded,
//! sequential appends), so the manifest — global order, per-stream versions,
//! the snapshot blob, and every fold-chain hash — is reproducible byte-for-byte
//! across regenerations. The STORE BYTES are not byte-compared: fjall embeds
//! internal timestamps, so goldens are OPENED and verified, never diffed whole.
//! Two generator runs therefore yield equivalent stores (identical manifest,
//! both pass the check) even though their tarballs differ.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use mess_cli::verify::{self, VerifyOptions};
use mess_log::certificates::{
    Aggregate, build_cert, load_verified, take_snapshot,
};
use mess_log::runtime::real::RealFs;
use mess_log::scanner::recover_segment_with_image;
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{
    BlobPtr, EngineOptions, FjallSnapshotBackend, LogEngine, SnapshotRef,
    SnapshotStore, StoredSnapshot, Version, interim_stream_id,
};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Shared, format-frozen constants (generator and check MUST agree).
// ---------------------------------------------------------------------------

/// The historical golden: format v3 layout, engine emitting PLAIN frames (no
/// on-disk `crypto_chain`). Immutable.
const GOLDEN_VERSION: &str = "v3";
/// The chained golden (`bn-3l0`): same format v3 layout, but the engine emits
/// the real per-batch on-disk `crypto_chain` (spec 05 §6), so `mess verify
/// --full` validates the fold chain against the actual stored bytes.
const GOLDEN_V4: &str = "v4";
/// Tiny active segment so a few hundred small batches roll + seal repeatedly.
const SEGMENT_SIZE: u64 = 16 * 1024;
/// Named streams `orders-0` .. `orders-{N-1}`.
const STREAMS: usize = 8;
/// Events per stream.
const EVENTS_PER_STREAM: usize = 60;
/// The designated chained stream — the fold-certificate + snapshot subject.
/// `orders-0` is fully in the first (sealed) segment, so it is the COLD-read
/// subject; the chained stream is the LAST stream, whose tail stays hot.
const CHAINED_STREAM: &str = "orders-7";
const CHAINED_STREAM_IDX: usize = 7;
/// The interned stream id fed to the fold-chain genesis (§3.1). Fixed +
/// recorded.
const CHAINED_STREAM_ID: u64 = 0x0007_0000_0007;
/// Batch granularity of the reconstructed fold-chain view (§6.2).
const CHAINED_BATCH_SIZE: usize = 4;
/// The 0-based last version the committed snapshot summarizes.
const SNAPSHOT_VERSION: u64 = 19;
/// The stream that must be served entirely from the COLD (sealed) tier.
const COLD_STREAM: &str = "orders-0";

/// The rolling engine options both phases open the store with. `chain` selects
/// whether the engine emits the real on-disk fold chain (v4+) or plain frames
/// (v3, the historical layout).
fn opts(chain: bool) -> EngineOptions {
    EngineOptions {
        segment_size: SEGMENT_SIZE,
        chain,
        ..EngineOptions::default()
    }
}

/// One of a few distinct message types, to exercise the type-name registry.
fn message_type(idx: usize) -> &'static str {
    ["order.created", "order.updated", "order.shipped"][idx % 3]
}

/// Deterministic payload for stream index `s`, stream version `v`. No
/// wall-clock; varies in length and content so a byte-exact replay is
/// meaningful.
fn payload(s: usize, v: usize) -> Vec<u8> {
    let mut out = format!("ord{s:02}-v{v:04}-").into_bytes();
    let len = 8 + (v % 12);
    out.extend(
        (0..len).map(|k| b'a' + u8::try_from((s + v + k) % 26).unwrap()),
    );
    out
}

// ---------------------------------------------------------------------------
// The reference aggregate for the fold-certificate path (mess-log Aggregate).
// ---------------------------------------------------------------------------

/// A tiny byte-sum aggregate. Works over arbitrary payloads, so the SAME bytes
/// the engine committed for the chained stream drive both the snapshot blob and
/// the fold-chain verification.
struct SumAgg {
    sum:   u64,
    count: u64,
}

impl Aggregate for SumAgg {
    const FOLD_VERSION: u32 = 7;

    fn init() -> Self { SumAgg { sum: 0, count: 0 } }

    fn apply(&mut self, payload: &[u8]) {
        self.sum = self
            .sum
            .wrapping_add(payload.iter().map(|&b| u64::from(b)).sum::<u64>());
        self.count += 1;
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut o = Vec::with_capacity(16);
        o.extend_from_slice(&self.sum.to_le_bytes());
        o.extend_from_slice(&self.count.to_le_bytes());
        o
    }

    fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.len() != 16 {
            return None;
        }
        Some(SumAgg {
            sum:   u64::from_le_bytes(b[0..8].try_into().ok()?),
            count: u64::from_le_bytes(b[8..16].try_into().ok()?),
        })
    }
}

// ---------------------------------------------------------------------------
// Hex + path helpers.
// ---------------------------------------------------------------------------

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(s, "{b:02x}").expect("write hex");
    }
    s
}

fn from_hex(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd-length hex");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex digit"))
        .collect()
}

/// The repo-root `tests/golden/<version>/` directory (two levels up from the
/// crate manifest dir).
fn golden_dir(version: &str) -> PathBuf {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("repo root");
    repo_root.join("tests").join("golden").join(version)
}

/// Run a shell pipeline, asserting success. Used for `tar`/`zstd` (the system
/// tools — no Rust crate dependency added for the golden's compression).
fn sh(cmd: &str) {
    let status =
        Command::new("sh").arg("-c").arg(cmd).status().expect("spawn sh");
    assert!(status.success(), "command failed ({status}): {cmd}");
}

fn rec(message_type: &str, data: &[u8]) -> RecordToAppend {
    RecordToAppend {
        message_type: message_type.to_string(),
        data:         data.to_vec(),
    }
}

// ===========================================================================
// GENERATOR (run explicitly:  cargo test -p mess-cli --test golden -- --ignored
// --nocapture generate_golden_v3)
// ===========================================================================

#[tokio::test]
#[ignore = "golden generator: run explicitly to (re)create tests/golden/v3; \
            commit its output"]
async fn generate_golden_v3() { generate(GOLDEN_VERSION, false).await; }

#[tokio::test]
#[ignore = "golden generator: run explicitly to (re)create tests/golden/v4; \
            commit its output"]
async fn generate_golden_v4() { generate(GOLDEN_V4, true).await; }

/// Build a complete golden store `version` with the production engine and pack
/// it under `tests/golden/<version>/`. `chain` selects whether segments carry
/// the real on-disk `crypto_chain` (v4) or plain frames (v3).
async fn generate(version: &str, chain: bool) {
    let work = tempfile::tempdir().expect("tempdir");
    let store = work.path().join("store");

    // ---- 1. Build the store with the production engine (rolling segments).
    // ----
    {
        let engine =
            LogEngine::open_with(&store, opts(chain)).expect("open fresh");
        for s in 0..STREAMS {
            let name = format!("orders-{s}");
            let mut expected = Version::NoStream;
            if s == CHAINED_STREAM_IDX {
                // The chained stream is appended in multi-event batches to
                // exercise the columnar multi-frame sealed path.
                let mut v = 0usize;
                while v < EVENTS_PER_STREAM {
                    let n = CHAINED_BATCH_SIZE.min(EVENTS_PER_STREAM - v);
                    let recs: Vec<RecordToAppend> = (0..n)
                        .map(|k| rec(message_type(v + k), &payload(s, v + k)))
                        .collect();
                    let out = engine
                        .append_batch(&name, expected, &recs)
                        .await
                        .expect("append");
                    expected = out.version;
                    v += n;
                }
            } else {
                for v in 0..EVENTS_PER_STREAM {
                    let out = engine
                        .append_batch(
                            &name,
                            expected,
                            &[rec(message_type(v), &payload(s, v))],
                        )
                        .await
                        .expect("append");
                    expected = out.version;
                }
            }
        }

        // Capture the authoritative global order straight from the engine.
        let global = engine
            .read_global(None, STREAMS * EVENTS_PER_STREAM * 2)
            .await
            .expect("read");
        assert_eq!(
            global.len(),
            STREAMS * EVENTS_PER_STREAM,
            "every event committed"
        );

        // ---- 2. Chained stream: spec-conformant fold-chain view + snapshot.
        // ----
        let chained_payloads: Vec<Vec<u8>> = global
            .iter()
            .filter(|r| r.stream_id == CHAINED_STREAM)
            .map(|r| r.data.clone())
            .collect();
        assert_eq!(chained_payloads.len(), EVENTS_PER_STREAM);
        let cert = build_cert(
            CHAINED_STREAM_ID,
            &chained_payloads,
            CHAINED_BATCH_SIZE,
            None,
        );
        let (head_ver, head_hash) = cert.head_anchor.expect("head anchor");
        assert_eq!(head_ver, EVENTS_PER_STREAM as u64 - 1);
        let (snap_ref, snap_blob) =
            take_snapshot::<SumAgg>(&cert, SNAPSHOT_VERSION);
        // Sanity: the honest snapshot verifies against the honest cert now.
        let out = load_verified::<SumAgg>(&cert, Some((&snap_ref, &snap_blob)))
            .expect("verify");
        assert!(!out.rebuilt_by_replay);
        let expected_state =
            SumAgg::from_bytes(&out.state.to_bytes()).expect("state");

        // ---- 3. Save a snapshot (with fold_version) into the nested store.
        // ----
        {
            let snaps = FjallSnapshotBackend::open(
                engine.clone(),
                store.join("snapshots"),
            )
            .expect("open snapshot store");
            let stored = StoredSnapshot {
                snapshot_ref: SnapshotRef {
                    stream_id:           interim_stream_id(CHAINED_STREAM),
                    stream_version:      SNAPSHOT_VERSION,
                    fold_version:        SumAgg::FOLD_VERSION,
                    covers_empty_prefix: false,
                    event_prefix_hash:   None,
                    state_hash:          None,
                    snapshot_ptr:        BlobPtr(0),
                },
                state_blob:   snap_blob.clone(),
            };
            snaps
                .save_snapshot(CHAINED_STREAM, stored)
                .await
                .expect("save snapshot");
            snaps.persist().expect("persist snapshot head");
        }

        // ---- 4. Build the manifest (BTreeMap keys => deterministic JSON).
        // ----
        let events: Vec<Value> = global
            .iter()
            .map(|r| {
                json!({
                    "g": r.global_position,
                    "stream": r.stream_id,
                    "type": r.message_type,
                    "sv": r.stream_position,
                    "data": to_hex(&r.data),
                })
            })
            .collect();
        let stream_heads: Vec<Value> = (0..STREAMS)
            .map(|s| json!({ "stream": format!("orders-{s}"), "head": EVENTS_PER_STREAM as u64 - 1 }))
            .collect();
        let manifest = json!({
            "format_version": 3,
            "golden": version,
            "on_disk_chain": chain,
            "generator": "bn-yvi crates/mess-cli/tests/golden.rs",
            "note": "Open + verify only; store bytes are NOT byte-compared (fjall timestamps).",
            "segment_size": SEGMENT_SIZE,
            "total_events": events.len(),
            "streams": (0..STREAMS).map(|s| format!("orders-{s}")).collect::<Vec<_>>(),
            "stream_heads": stream_heads,
            "cold_stream": COLD_STREAM,
            "min_sealed_segments": 1,
            "chained_stream": {
                "name": CHAINED_STREAM,
                "stream_id": CHAINED_STREAM_ID,
                "batch_size": CHAINED_BATCH_SIZE,
                "fold_version": SumAgg::FOLD_VERSION,
                "head_hash": to_hex(&head_hash),
                "snapshot": {
                    "stream_version": SNAPSHOT_VERSION,
                    "event_prefix_hash": to_hex(&snap_ref.event_prefix_hash),
                    "state_hash": to_hex(&snap_ref.state_hash),
                    "state_blob": to_hex(&snap_blob),
                },
                "expected_state": { "sum": expected_state.sum, "count": expected_state.count },
            },
            "events": events,
        });

        let out_dir = golden_dir(version);
        std::fs::create_dir_all(&out_dir).expect("mkdir golden dir");
        let manifest_bytes =
            serde_json::to_vec_pretty(&manifest).expect("serialize manifest");
        std::fs::write(out_dir.join("expected-events.json"), &manifest_bytes)
            .expect("write manifest");

        // ---- 5. Verify the freshly built store BEFORE packing it. ----
        let report =
            verify::run(&store, &VerifyOptions { full: true, repair: false });
        assert_eq!(
            report.exit_code(),
            0,
            "mess verify --full on fresh store: {}",
            report.to_pretty()
        );

        // Drop the engine + snapshot backend (drains the sealer so sidecars +
        // footers are durable) before packing.
    }

    // At least one segment must have rolled + sealed.
    let sealed_pidx = std::fs::read_dir(store.join("sealed"))
        .expect("sealed dir")
        .flatten()
        .filter(|e| {
            e.path().extension().and_then(|x| x.to_str()) == Some("pidx")
        })
        .count();
    assert!(sealed_pidx >= 1, "expected >=1 sealed segment, got {sealed_pidx}");

    // ---- 6. Pack the store dir as store.tar.zst (system tar + zstd). ----
    let out_dir = golden_dir(version);
    let tarball = out_dir.join("store.tar.zst");
    let parent = store.parent().expect("store parent");
    sh(&format!(
        "tar -cf - -C {} store | zstd -q -f -19 -o {}",
        parent.display(),
        tarball.display()
    ));

    let size = std::fs::metadata(&tarball).expect("tarball stat").len();
    assert!(
        size < 2 * 1024 * 1024,
        "golden tarball must stay < 2 MiB, got {size} bytes"
    );
    eprintln!(
        "golden {version}: {} events, {sealed_pidx} sealed segment(s), \
         tarball {size} bytes",
        STREAMS * EVENTS_PER_STREAM
    );
}

// ===========================================================================
// CHECK (normal CI:  cargo test -p mess-cli --test golden
// golden_v3_opens_and_verifies)
// ===========================================================================

#[tokio::test]
async fn golden_v3_opens_and_verifies() { check(GOLDEN_VERSION, false).await; }

#[tokio::test]
async fn golden_v4_opens_and_verifies() { check(GOLDEN_V4, true).await; }

async fn check(version: &str, chain: bool) {
    let dir = golden_dir(version);
    let tarball = dir.join("store.tar.zst");
    let manifest_path = dir.join("expected-events.json");
    assert!(
        tarball.exists() && manifest_path.exists(),
        "missing committed golden {version}; regenerate with `cargo test -p \
         mess-cli --test golden -- --ignored generate_golden_{version}`"
    );

    // ---- 1. Unpack the committed golden into a scratch dir. ----
    let scratch = tempfile::tempdir().expect("tempdir");
    sh(&format!(
        "zstd -dqc {} | tar -xf - -C {}",
        tarball.display(),
        scratch.path().display()
    ));
    let store = scratch.path().join("store");
    assert!(store.join("meta").is_dir(), "unpacked store missing meta dir");

    let manifest: Value = serde_json::from_slice(
        &std::fs::read(&manifest_path).expect("read manifest"),
    )
    .expect("parse");
    let events = manifest["events"].as_array().expect("events array");
    let total = manifest["total_events"].as_u64().expect("total") as usize;
    assert_eq!(events.len(), total);

    // ---- 2. Open with CURRENT code: full recovery over the whole chain. ----
    let engine = LogEngine::open_with(&store, opts(chain))
        .expect("reopen committed golden");
    assert_eq!(engine.total_events(), total, "recovery rehydrated every event");
    assert_eq!(
        manifest["on_disk_chain"].as_bool().unwrap_or(false),
        chain,
        "golden {version} manifest disagrees on the on-disk-chain flag"
    );

    // ---- 3. Registry + names hydrated; sealed tier reloaded. ----
    assert!(
        engine.sealed_segment_count() >= 1,
        "sealed sidecars must reload into the cold tier on reopen"
    );
    for sh in manifest["stream_heads"].as_array().expect("stream_heads") {
        let name = sh["stream"].as_str().unwrap();
        let head = sh["head"].as_u64().unwrap();
        assert_eq!(
            engine.head(name).await.unwrap(),
            Version::At(head),
            "head for {name}"
        );
    }

    // ---- 4. Every event replays byte-exact vs the manifest (global order).
    // ----
    let global = engine.read_global(None, total * 2).await.unwrap();
    assert_eq!(global.len(), total, "global read count");
    for (got, want) in global.iter().zip(events) {
        assert_eq!(
            got.global_position,
            want["g"].as_u64().unwrap(),
            "global_position"
        );
        assert_eq!(
            got.stream_id,
            want["stream"].as_str().unwrap(),
            "stream name (registry)"
        );
        assert_eq!(
            got.message_type,
            want["type"].as_str().unwrap(),
            "message type (registry)"
        );
        assert_eq!(
            got.stream_position,
            want["sv"].as_u64().unwrap(),
            "stream_version"
        );
        assert_eq!(
            got.data,
            from_hex(want["data"].as_str().unwrap()),
            "payload bytes"
        );
    }

    // ---- 5. Cold reads: a fully-sealed stream is served byte-exact. ----
    let cold_name = manifest["cold_stream"].as_str().unwrap();
    let cold =
        engine.read_stream(cold_name, Version::NoStream, total).await.unwrap();
    let cold_expected: Vec<&Value> = events
        .iter()
        .filter(|e| e["stream"].as_str().unwrap() == cold_name)
        .collect();
    assert_eq!(cold.len(), cold_expected.len(), "cold stream event count");
    for (got, want) in cold.iter().zip(&cold_expected) {
        assert_eq!(got.stream_position, want["sv"].as_u64().unwrap());
        assert_eq!(
            got.data,
            from_hex(want["data"].as_str().unwrap()),
            "cold payload byte-exact"
        );
    }

    // ---- 6. Snapshot loads with its fold_version + byte-exact blob. ----
    let chained = &manifest["chained_stream"];
    let snaps =
        FjallSnapshotBackend::open(engine.clone(), store.join("snapshots"))
            .expect("open snapshot store");
    let loaded = snaps
        .load_snapshot(chained["name"].as_str().unwrap())
        .await
        .expect("load snapshot")
        .expect("snapshot present");
    assert_eq!(
        loaded.snapshot_ref.fold_version,
        chained["fold_version"].as_u64().unwrap() as u32,
        "snapshot fold_version"
    );
    assert_eq!(
        loaded.snapshot_ref.stream_version,
        chained["snapshot"]["stream_version"].as_u64().unwrap(),
        "snapshot stream_version"
    );
    assert_eq!(
        loaded.state_blob,
        from_hex(chained["snapshot"]["state_blob"].as_str().unwrap()),
        "snapshot state blob byte-exact"
    );

    // ---- 7. load_verified green on the chained stream; hashes format-stable.
    // ----
    let mut chained_payloads: Vec<(u64, Vec<u8>)> = events
        .iter()
        .filter(|e| e["stream"].as_str().unwrap() == CHAINED_STREAM)
        .map(|e| {
            (e["sv"].as_u64().unwrap(), from_hex(e["data"].as_str().unwrap()))
        })
        .collect();
    chained_payloads.sort_by_key(|(v, _)| *v);
    let payloads: Vec<Vec<u8>> =
        chained_payloads.into_iter().map(|(_, p)| p).collect();

    let stream_id = chained["stream_id"].as_u64().unwrap();
    let batch_size = chained["batch_size"].as_u64().unwrap() as usize;
    let cert = build_cert(stream_id, &payloads, batch_size, None);

    // The genesis/chain/head hashes must match the frozen manifest bytes.
    let (_hv, head_hash) = cert.head_anchor.expect("head anchor");
    assert_eq!(
        to_hex(&head_hash),
        chained["head_hash"].as_str().unwrap(),
        "fold-chain head hash drifted from the committed golden"
    );
    let (snap_ref, snap_blob) =
        take_snapshot::<SumAgg>(&cert, SNAPSHOT_VERSION);
    assert_eq!(
        to_hex(&snap_ref.event_prefix_hash),
        chained["snapshot"]["event_prefix_hash"].as_str().unwrap(),
        "event_prefix_hash drifted"
    );
    assert_eq!(
        to_hex(&snap_ref.state_hash),
        chained["snapshot"]["state_hash"].as_str().unwrap(),
        "state_hash drifted"
    );

    let out = load_verified::<SumAgg>(&cert, Some((&snap_ref, &snap_blob)))
        .expect("load_verified green on the chained stream");
    assert!(
        !out.rebuilt_by_replay,
        "honest snapshot should not force a rebuild"
    );
    assert_eq!(
        out.state.sum,
        chained["expected_state"]["sum"].as_u64().unwrap(),
        "verified aggregate sum"
    );
    assert_eq!(
        out.state.count,
        chained["expected_state"]["count"].as_u64().unwrap(),
        "verified aggregate count"
    );

    // ---- 8. `mess verify --full` exits 0 on the committed golden. ----
    let report =
        verify::run(&store, &VerifyOptions { full: true, repair: false });
    assert_eq!(
        report.exit_code(),
        0,
        "mess verify --full must pass:\n{}",
        report.to_pretty()
    );

    // ---- 9. (chained goldens only) the segments really carry the on-disk
    //         fold chain, so §8's `verify --full` validated real stored bytes —
    //         not a silently-skipped empty chain. Every `.log` segment must
    //         carry `crypto_chain` on every batch.
    if chain {
        let segments = mess_cli::store::discover_segments(&store);
        assert!(!segments.is_empty(), "chained golden must have segments");
        let mut chained_batches = 0u64;
        for seg in &segments {
            let (rec, _img) =
                recover_segment_with_image(&RealFs, &seg.log_path)
                    .expect("recover golden segment");
            for b in &rec.accepted {
                assert!(
                    b.has_crypto_chain,
                    "golden {version} segment {} batch at {} is missing its \
                     on-disk crypto_chain",
                    seg.segment_id, b.offset
                );
                chained_batches += 1;
            }
        }
        assert!(
            chained_batches >= 1,
            "chained golden must carry >=1 on-disk chained batch"
        );
    }
}
