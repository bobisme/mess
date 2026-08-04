//! **The bone's core assertion** (bn-1m6c): a chatter seed leaves behind a
//! store with MORE THAN ONE sealed segment.
//!
//! This is the whole reason the crate exists. `examples/social` produces a
//! 17 MB corpus at ~1.007 events per stream with no segment-size knob, so its
//! active segment never fills, nothing ever rolls, and the sealed tier —
//! `.pcol` payload accelerators, `.reg` registry deltas, SealPack, cold open at
//! scale — is structurally unreachable from `examples/` (bn-3jqg). If chatter
//! ever stopped rolling segments it would quietly become another social, and
//! every downstream measurement built on it would be measuring the hot tier.
//! These tests make that regression impossible to land silently.

use chatter::seed::{self, SeedConfig};
use chatter::store_backend::{
    SealedCensus, count_sealed_artifacts, create_store, open_store,
    sealed_census,
};
use mess_testkit::sweeping_temp_dir;

/// Seed `cfg` into a fresh store, close it, and reopen — returning the census a
/// **cold reader** sees. Reopening matters: the sealed tier is installed at
/// open from the durable sidecars, so this is the authoritative number, not a
/// live-process artifact.
async fn seed_and_census(
    tag: &str,
    cfg: &SeedConfig,
) -> (mess_testkit::SweepingTempDir, std::path::PathBuf, SealedCensus) {
    let t = sweeping_temp_dir(tag);
    let dir = t.path().join("store");
    let store = create_store(&dir, cfg.store_config()).expect("create store");
    let report = seed::generate(&store, cfg).await;
    assert_eq!(report.messages, cfg.messages, "the whole corpus committed");
    drop(store);

    let store = open_store(&dir).expect("reopen store");
    let census = sealed_census(&store, &dir);
    drop(store);
    (t, dir, census)
}

/// THE assertion. Demo scale, the preset a human runs first, must leave a
/// multi-segment sealed store.
#[tokio::test]
async fn demo_scale_leaves_more_than_one_sealed_segment() {
    let cfg = SeedConfig::demo();
    let (_t, dir, census) =
        seed_and_census("chatter-demo-segments", &cfg).await;

    println!(
        "demo scale: {} bytes across {} segment file(s); sealed: {} ({} \
         SealPack, {} loose, {} payload-indexed)",
        census.bytes,
        census.segment_files,
        census.sealed_segments,
        census.seal_pack,
        census.loose_sidecar,
        census.payload_indexed,
    );

    assert!(
        census.sealed_segments > 1,
        "bn-1m6c's core purpose: a demo-scale chatter seed MUST roll and seal \
         more than one segment, got {} (segment_bytes={}). If this fails, the \
         corpus has drifted shallow or --segment-bytes stopped taking effect, \
         and every sealed-tier read path this example exists to exercise is \
         gone.",
        census.sealed_segments,
        cfg.segment_bytes,
    );
    // Cross-check the engine's own report against the artifacts on disk: an
    // engine that reported sealed segments it had not written would be a much
    // worse bug than a shallow corpus.
    let on_disk = count_sealed_artifacts(&dir, "seal");
    assert_eq!(
        on_disk, census.sealed_segments,
        "every sealed segment must have a .seal pack on disk"
    );
    // The active segment is not sealed, so there is always one more segment
    // file than sealed segments.
    assert_eq!(
        census.segment_files,
        census.sealed_segments + 1,
        "sealed segments plus exactly one active segment"
    );
    // Sealed history reads go through the payload accelerator — the `.pcol`
    // path (bn-bka2) that `examples/social` cannot reach at all.
    assert_eq!(
        census.payload_indexed, census.sealed_segments,
        "every sealed segment should carry a payload accelerator"
    );
}

/// The `--segment-bytes` knob is real: the same corpus in a store with larger
/// segments seals fewer of them. This is the knob `examples/social` lacks, and
/// the test that proves it is wired to the engine rather than decorative.
#[tokio::test]
async fn segment_bytes_controls_the_seal_cadence() {
    let small = SeedConfig { segment_bytes: 64 * 1024, ..SeedConfig::tiny(7) };
    let large =
        SeedConfig { segment_bytes: 4 * 1024 * 1024, ..SeedConfig::tiny(7) };

    let (_ts, _ds, small_census) =
        seed_and_census("chatter-seg-small", &small).await;
    let (_tl, _dl, large_census) =
        seed_and_census("chatter-seg-large", &large).await;

    println!(
        "64 KiB segments -> {} sealed; 4 MiB segments -> {} sealed",
        small_census.sealed_segments, large_census.sealed_segments
    );
    assert!(
        small_census.sealed_segments > 1,
        "the tiny preset must still be multi-segment, got {}",
        small_census.sealed_segments
    );
    assert!(
        small_census.sealed_segments > large_census.sealed_segments,
        "smaller segments must seal MORE of them: {} vs {}",
        small_census.sealed_segments,
        large_census.sealed_segments
    );
}

/// `--seal-pack off` selects the legacy loose sidecar family. Both shapes are
/// permanently supported, and flipping this flag between two seeds is the A/B
/// bn-1yor's matrix ran — reproducible from an example for the first time.
#[tokio::test]
async fn seal_pack_off_produces_loose_sidecars_instead() {
    let cfg = SeedConfig { seal_pack: false, ..SeedConfig::tiny(21) };
    let (_t, dir, census) = seed_and_census("chatter-loose", &cfg).await;

    assert!(
        census.sealed_segments > 1,
        "loose-sidecar mode must still roll multiple segments, got {}",
        census.sealed_segments
    );
    assert_eq!(
        census.seal_pack, 0,
        "no segment should be served by a .seal pack in loose mode"
    );
    assert_eq!(
        census.loose_sidecar, census.sealed_segments,
        "every sealed segment should be served by a loose sidecar family"
    );
    // The loose family's payload accelerator is a real `.pcol` file per
    // segment — the sidecar the pack mode folds into one artifact.
    assert_eq!(
        count_sealed_artifacts(&dir, "pcol"),
        census.sealed_segments,
        "loose mode writes one .pcol per sealed segment"
    );
    assert_eq!(
        count_sealed_artifacts(&dir, "seal"),
        0,
        "loose mode writes no .seal packs"
    );
}
