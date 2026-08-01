//! bn-11g — the **segment footer names the exact SealPack** it accepted, and
//! open refuses anything else (spec 01 §3.3.3, D-FMT-10).
//!
//! Before this bone the footer proved only that the segment's *bytes* were
//! durable; which pack served them was decided by a coverage cross-check
//! (`segment_id` / `base_pos` / `end_pos`). Coverage is not specific: a stale
//! pack from an earlier seal of the same range, a pack copied in from another
//! store, and any same-coverage substitute all pass it. These tests build a
//! real rolled pack-mode store and then attack that gap directly.
//!
//! # The validation matrix these tests pin
//!
//! | footer | pack on disk | behaviour |
//! |---|---|---|
//! | names an identity | that exact pack | ADMITTED, served cold |
//! | names an identity | a different pack, same coverage | REFUTED `pack-identity-mismatch`, raw fallback |
//! | names an identity | a legacy `.pidx` instead | REFUTED `pack-identity-mismatch` |
//! | names an identity | absent | no candidate; segment re-sealed |
//! | names an identity | unparsable | REFUTED `unparsable` (bn-30u, unchanged) |
//! | identity bits corrupt | any | REFUTED `pack-identity-unresolvable` — never downgraded to coverage-only |
//! | names none (legacy) | any parseable pack | ADMITTED on coverage alone — the documented policy |
//! | absent (torn) | durable pack | footerless candidate, scan-confirmed, re-queued |
//!
//! Every refusal costs the segment its cold-tier accelerator and nothing else:
//! each test re-asserts the whole read surface byte-exact against a baseline
//! taken before the damage, because "the raw log is authority" is the entire
//! justification for throwing a pack away.

use std::path::{Path, PathBuf};

use mess_index::sealed::{
    PackInput, SealBatch, SealInput, SealStream, SealedSegmentIndex,
    encode_pack, encode_sidecar,
};
use mess_log::footer_ext::{
    SealPackIdentity, SealSummary, decode_extension, encode_sealed_footer,
};
use mess_log::format::{EXT_SECTION_HDR_LEN, SEAL_PACK_IDENTITY_HDRDIR_BLAKE3};
use mess_log::runtime::real::RealFs;
use mess_log::sealer::{read_extension, read_trailer};
use mess_store::backend::{Backend, RecordToAppend, StoredRecord};
use mess_store::{EngineOptions, LogEngine, RefutationReason, Version};

// ---------------------------------------------------------------------------
// Corpus — the bn-30u lifecycle shape, in pack mode
// ---------------------------------------------------------------------------

const STREAMS: usize = 8;
const PER: usize = 45;

fn pack_opts() -> EngineOptions {
    EngineOptions {
        segment_size: 16 * 1024,
        seal_pack: true,
        ..EngineOptions::default()
    }
}

fn sidecar_opts() -> EngineOptions {
    EngineOptions {
        segment_size: 16 * 1024,
        seal_pack: false,
        ..EngineOptions::default()
    }
}

fn payload(s: usize, i: usize) -> Vec<u8> {
    format!("s{s:03}-e{i:04}").into_bytes()
}

async fn build_rolled_store(
    store: &Path,
    opts: EngineOptions,
) -> Vec<StoredRecord> {
    let engine = LogEngine::open_with(store, opts).expect("open fresh");
    for s in 0..STREAMS {
        let name = format!("stream-{s}");
        let mut expected = Version::NoStream;
        for i in 0..PER {
            let out = engine
                .append_batch(
                    &name,
                    expected,
                    &[RecordToAppend {
                        message_type: "ev".to_string(),
                        data:         payload(s, i),
                    }],
                )
                .await
                .expect("append");
            expected = out.version;
        }
    }
    let baseline =
        engine.read_global(None, STREAMS * PER * 2).await.expect("baseline");
    assert_eq!(baseline.len(), STREAMS * PER);
    drop(engine); // drains the sealer: packs + footers durable
    baseline
}

/// Every read path agrees with the baseline, byte for byte.
async fn assert_reads_match(engine: &LogEngine, baseline: &[StoredRecord]) {
    let g =
        engine.read_global(None, STREAMS * PER * 2).await.expect("global read");
    assert_eq!(g.len(), baseline.len(), "global event count");
    for (got, want) in g.iter().zip(baseline) {
        assert_eq!(got.stream_id, want.stream_id);
        assert_eq!(got.message_type, want.message_type);
        assert_eq!(got.data, want.data, "payload bytes");
        assert_eq!(got.stream_position, want.stream_position);
        assert_eq!(got.global_position, want.global_position);
    }
    for s in 0..STREAMS {
        let name = format!("stream-{s}");
        let evs = engine
            .read_stream(&name, Version::NoStream, PER * 2)
            .await
            .expect("stream read");
        assert_eq!(evs.len(), PER, "{name}: every event served");
        for (i, e) in evs.iter().enumerate() {
            assert_eq!(e.data, payload(s, i), "{name} event {i}");
        }
    }
}

// ---------------------------------------------------------------------------
// Layout helpers
// ---------------------------------------------------------------------------

fn sealed_dir(store: &Path) -> PathBuf { store.join("sealed") }

fn pack_path(store: &Path, seg: u64) -> PathBuf {
    sealed_dir(store).join(format!("seg-{seg:020}.seal"))
}

fn quarantined_pack(store: &Path, seg: u64) -> PathBuf {
    sealed_dir(store).join(format!("seg-{seg:020}.seal.refuted"))
}

fn log_path(store: &Path, seg: u64) -> PathBuf {
    store.join(format!("seg-{seg:08}.log"))
}

fn segment_ids(store: &Path) -> Vec<u64> {
    let mut ids: Vec<u64> = std::fs::read_dir(store)
        .expect("store dir")
        .flatten()
        .filter_map(|e| {
            let name = e.file_name();
            let name = name.to_str()?;
            name.strip_prefix("seg-")?.strip_suffix(".log")?.parse().ok()
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// A rolled (non-head) segment whose footer really does name a pack — the one
/// the tests attack.
fn a_named_rolled_segment(store: &Path) -> u64 {
    let ids = segment_ids(store);
    let head = *ids.last().expect("non-empty");
    ids.into_iter()
        .filter(|&id| id != head && pack_path(store, id).exists())
        .find(|&id| named_identity(store, id).is_some())
        .expect("a rolled segment whose footer names its pack")
}

/// The identity a segment's footer names, if any (the reader's view).
fn named_identity(store: &Path, seg: u64) -> Option<SealPackIdentity> {
    let log = log_path(store, seg);
    let cat = read_trailer(&RealFs, &log).ok().flatten()?;
    if !cat.names_seal_pack() {
        return None;
    }
    let ext = read_extension(&RealFs, &log, &cat).ok().flatten()?;
    decode_extension(&ext).pack_identity
}

/// The identity the pack on disk actually hashes to.
fn pack_identity_on_disk(store: &Path, seg: u64) -> Option<[u8; 32]> {
    SealedSegmentIndex::open_pack_eager(&pack_path(store, seg))
        .ok()?
        .pack_identity()
        .map(|i| *i.as_bytes())
}

fn count_ext(dir: &Path, ext: &str) -> usize {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .filter(|e| {
                    e.path().extension().and_then(|x| x.to_str()) == Some(ext)
                })
                .count()
        })
        .unwrap_or(0)
}

fn await_path(path: &Path) -> bool {
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    false
}

/// The victim's own pack, re-encoded with every batch offset shifted — a pack
/// with **identical coverage** (same `segment_id`, `base_pos`, event count,
/// same streams and versions) and different content. This is the substitute the
/// pre-bn-11g coverage cross-check cannot tell from the real thing; if it were
/// ever admitted its pointers would dereference the wrong bytes, so the read
/// assertions would fail loudly rather than the test passing on a technicality.
fn forged_same_coverage_pack(store: &Path, seg: u64) -> Vec<u8> {
    let real = SealedSegmentIndex::open_pack_eager(&pack_path(store, seg))
        .expect("the real pack opens");
    let streams: Vec<SealStream> = real
        .stream_ids()
        .iter()
        .map(|&sid| SealStream {
            stream_id: sid,
            batches:   real
                .stream_entries(sid)
                .expect("entries")
                .into_iter()
                .map(|e| SealBatch {
                    first_version:    e.first_version,
                    frame_count:      e.frame_count,
                    first_global_pos: e.first_global_pos,
                    offset:           e.ptr.offset + 8, // the only change
                })
                .collect(),
        })
        .collect();
    encode_pack(&PackInput {
        segment_id:     real.segment_id(),
        base_pos:       real.base_pos(),
        streams:        &streams,
        event_type_ids: &[],
        filter:         None,
        payload_bytes:  None,
        registry_delta: None,
    })
}

/// Rewrite `seg`'s footer, keeping every catalog field the real seal wrote and
/// changing only what the test is attacking. `mutate` sees the identity entry
/// the real footer named; returning `None` writes a *legacy* footer (no name).
fn rewrite_footer(
    store: &Path,
    seg: u64,
    mutate: impl FnOnce(SealPackIdentity) -> Option<SealPackIdentity>,
) {
    use std::io::{Seek, SeekFrom, Write};

    let log = log_path(store, seg);
    let cat = read_trailer(&RealFs, &log).expect("read").expect("sealed");
    let named = named_identity(store, seg).expect("footer names a pack");
    let summary = SealSummary {
        segment_id:  cat.segment_id,
        epoch:       cat.epoch,
        base_pos:    cat.base_pos,
        batch_count: cat.batch_count,
        event_count: cat.event_count,
        content_len: cat.ext_offset,
    };
    let replacement = mutate(named);
    let (footer, _) =
        encode_sealed_footer(&summary, &[], &[], replacement.as_ref());
    let mut f =
        std::fs::OpenOptions::new().write(true).open(&log).expect("open log");
    f.seek(SeekFrom::Start(cat.ext_offset)).expect("seek");
    f.write_all(&footer).expect("write footer");
    f.sync_all().expect("fsync");
    // The new footer may be shorter than the old (legacy has no extension);
    // truncate so the trailer really is at EOF, as a real seal leaves it.
    f.set_len(cat.ext_offset + footer.len() as u64).expect("truncate");
    f.sync_all().expect("fsync");
}

// ---------------------------------------------------------------------------
// The happy path: a real pack-mode seal binds every rolled segment
// ---------------------------------------------------------------------------

/// A live, rolling, pack-mode store names **every** rolled segment's pack, and
/// each name is exactly what a reader derives from the pack on disk. This is
/// the writer-side AC: new writers always emit the binding, not just when
/// asked.
#[tokio::test]
async fn every_rolled_pack_segment_footer_names_its_pack() {
    let d = mess_testkit::sweeping_temp_dir("ident-happy");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, pack_opts()).await;

    let ids = segment_ids(&store);
    let head = *ids.last().expect("head");
    let mut checked = 0;
    for id in ids.iter().copied().filter(|&i| i != head) {
        if !pack_path(&store, id).exists() {
            continue;
        }
        let named = named_identity(&store, id)
            .unwrap_or_else(|| panic!("segment {id} footer names no pack"));
        assert_eq!(named.identity_kind, SEAL_PACK_IDENTITY_HDRDIR_BLAKE3);
        assert_eq!(named.segment_id, id, "the entry names its own segment");
        assert!(named.kind_is_known());
        assert_eq!(
            Some(named.identity),
            pack_identity_on_disk(&store, id),
            "segment {id}: the footer names the pack that is actually there"
        );
        checked += 1;
    }
    assert!(checked >= 1, "the corpus must roll at least one packed segment");

    // And the store reopens clean, serving everything from the cold tier.
    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen");
    assert_eq!(engine.sealed_candidate_health().refuted(), 0);
    assert_eq!(engine.sealed_candidate_health().pending_reseal, Vec::new());
    assert_eq!(engine.sealed_segment_count(), checked);
    assert_reads_match(&engine, &baseline).await;
}

// ---------------------------------------------------------------------------
// Substitution: the attacks coverage cannot see
// ---------------------------------------------------------------------------

/// **Stale replacement.** The segment is re-sealed (so the footer names the NEW
/// pack) and the OLD pack file is put back. Coverage is byte-identical — same
/// segment, same base_pos, same event count — so the pre-bn-11g check admits
/// it. The identity does not.
#[tokio::test]
async fn a_stale_pack_under_a_fresh_footer_is_refuted() {
    let d = mess_testkit::sweeping_temp_dir("ident-stale");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, pack_opts()).await;
    let victim = a_named_rolled_segment(&store);

    // Snapshot the pack the current footer names.
    let old_pack = std::fs::read(pack_path(&store, victim)).expect("read pack");
    let old_id = pack_identity_on_disk(&store, victim).expect("old identity");

    // Force a re-seal: quarantine the pack so open re-queues the segment. The
    // fresh seal writes a NEW pack and a footer naming it.
    std::fs::rename(
        pack_path(&store, victim),
        quarantined_pack(&store, victim),
    )
    .expect("quarantine to force a re-seal");
    {
        let engine = LogEngine::open_with(&store, pack_opts())
            .expect("reopen to reseal");
        assert!(await_path(&pack_path(&store, victim)), "re-seal lands");
        drop(engine);
    }
    let new_id = pack_identity_on_disk(&store, victim).expect("new identity");
    let named = named_identity(&store, victim).expect("named");
    assert_eq!(named.identity, new_id, "the footer names the FRESH pack");

    // Now put the stale pack back. Coverage still matches perfectly.
    std::fs::write(pack_path(&store, victim), &old_pack).expect("restore old");
    assert_eq!(pack_identity_on_disk(&store, victim), Some(old_id));
    let stale = SealedSegmentIndex::open_pack_eager(&pack_path(&store, victim))
        .expect("the stale pack is perfectly valid on its own");
    let cat =
        read_trailer(&RealFs, &log_path(&store, victim)).unwrap().unwrap();
    assert_eq!(stale.segment_id(), cat.segment_id, "coverage: segment_id");
    assert_eq!(stale.base_pos(), cat.base_pos, "coverage: base_pos");
    assert_eq!(
        stale.base_pos() + stale.event_count(),
        cat.end_pos,
        "coverage: end_pos — the pre-bn-11g check would ADMIT this"
    );

    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    assert_eq!(health.refuted(), 1, "the stale pack is refused");
    assert_eq!(health.refutations[0].segment_id, victim);
    assert_eq!(
        health.refutations[0].reason,
        RefutationReason::PackIdentityMismatch
    );
    assert!(health.refutations[0].quarantined);
    assert!(quarantined_pack(&store, victim).exists());
    assert_eq!(health.pending_reseal, vec![victim], "and re-queued");
    assert_reads_match(&engine, &baseline).await;

    // Convergence: the re-seal lands and the next open is clean again.
    assert!(await_path(&pack_path(&store, victim)));
    drop(engine);
    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen 2");
    assert_eq!(engine.sealed_candidate_health().refuted(), 0);
    assert_eq!(
        named_identity(&store, victim).map(|n| n.identity),
        pack_identity_on_disk(&store, victim),
        "the converged footer names the converged pack"
    );
    assert_reads_match(&engine, &baseline).await;
}

/// **Copied wrong pack — same coverage, different content.** The substitution
/// the pre-bn-11g check is blind to: a structurally perfect pack claiming the
/// victim's exact `segment_id`/`base_pos`/event count, whose pointers are not
/// the victim's. It parses, it opens, it cross-checks coverage — and its
/// identity is not the one the footer named.
#[tokio::test]
async fn a_same_coverage_pack_with_different_content_is_refuted() {
    let d = mess_testkit::sweeping_temp_dir("ident-copied");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, pack_opts()).await;
    let victim = a_named_rolled_segment(&store);

    let forged = forged_same_coverage_pack(&store, victim);
    std::fs::write(pack_path(&store, victim), &forged).expect("plant");

    // It is a perfectly valid pack, and its coverage matches the footer's
    // exactly — every check that existed before bn-11g passes.
    let planted =
        SealedSegmentIndex::open_pack_eager(&pack_path(&store, victim))
            .expect("the forged pack is structurally valid");
    let cat =
        read_trailer(&RealFs, &log_path(&store, victim)).unwrap().unwrap();
    assert_eq!(planted.segment_id(), cat.segment_id, "coverage: segment_id");
    assert_eq!(planted.base_pos(), cat.base_pos, "coverage: base_pos");
    assert_eq!(
        planted.base_pos() + planted.event_count(),
        cat.end_pos,
        "coverage: end_pos — the pre-bn-11g check would ADMIT this"
    );
    assert_ne!(
        pack_identity_on_disk(&store, victim),
        named_identity(&store, victim).map(|n| n.identity),
        "...and only the identity says otherwise"
    );

    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    let mine: Vec<_> =
        health.refutations.iter().filter(|r| r.segment_id == victim).collect();
    assert_eq!(mine.len(), 1, "{:?}", health.refutations);
    assert_eq!(mine[0].reason, RefutationReason::PackIdentityMismatch);
    assert!(mine[0].quarantined);
    assert_reads_match(&engine, &baseline).await;
}

/// **A legacy `.pidx` where a pack is named.** A sidecar has no identity to
/// offer, so it can never satisfy a footer that names a pack — otherwise
/// deleting the `.seal` and dropping in any same-coverage `.pidx` would restore
/// exactly the substitution the identity exists to reject.
#[tokio::test]
async fn a_sidecar_cannot_stand_in_for_a_named_pack() {
    let d = mess_testkit::sweeping_temp_dir("ident-pidx-standin");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, pack_opts()).await;
    let victim = a_named_rolled_segment(&store);

    // Build a real `.pidx` for the victim from its own pack's pointers, then
    // remove the pack: same coverage, real data, no identity.
    let pack = SealedSegmentIndex::open_pack_eager(&pack_path(&store, victim))
        .expect("pack opens");
    let sidecar = encode_sidecar(&SealInput {
        segment_id:     pack.segment_id(),
        base_pos:       pack.base_pos(),
        streams:        pack
            .stream_ids()
            .iter()
            .map(|&sid| SealStream {
                stream_id: sid,
                batches:   pack
                    .stream_entries(sid)
                    .expect("entries")
                    .into_iter()
                    .map(|e| SealBatch {
                        first_version:    e.first_version,
                        frame_count:      e.frame_count,
                        first_global_pos: e.first_global_pos,
                        offset:           e.ptr.offset,
                    })
                    .collect(),
            })
            .collect(),
        payloads:       None,
        event_type_ids: None,
    });
    std::fs::remove_file(pack_path(&store, victim)).expect("drop the pack");
    std::fs::write(
        sealed_dir(&store).join(format!("seg-{victim:020}.pidx")),
        &sidecar,
    )
    .expect("plant the sidecar");

    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    let mine: Vec<_> =
        health.refutations.iter().filter(|r| r.segment_id == victim).collect();
    assert_eq!(mine.len(), 1, "{:?}", health.refutations);
    assert_eq!(mine[0].reason, RefutationReason::PackIdentityMismatch);
    assert_reads_match(&engine, &baseline).await;
}

// ---------------------------------------------------------------------------
// Identity-bit corruption: the downgrade that must not happen
// ---------------------------------------------------------------------------

/// **A flipped bit in the stored identity.** The extension fails `ext_crc`, so
/// the name is unresolvable — but the flag saying a name was written lives in
/// the trailer, under `footer_crc`, and survives. The pack is therefore
/// refused, NOT quietly re-admitted under the legacy coverage-only policy.
///
/// This is the test that distinguishes bn-11g from a naive implementation: an
/// implementation that inferred "a pack was named" from the extension contents
/// would see an empty/garbage extension here and fall straight back to
/// coverage-only trust, which is the hole.
#[tokio::test]
async fn a_corrupt_identity_refuses_the_pack_and_never_downgrades() {
    let d = mess_testkit::sweeping_temp_dir("ident-bitflip");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, pack_opts()).await;
    let victim = a_named_rolled_segment(&store);

    // Flip one bit inside the identity bytes of the footer's extension.
    let log = log_path(&store, victim);
    let cat = read_trailer(&RealFs, &log).unwrap().unwrap();
    let at = cat.ext_offset + EXT_SECTION_HDR_LEN as u64 + 16;
    {
        use std::io::{Read, Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&log)
            .expect("open");
        let mut b = [0u8; 1];
        f.seek(SeekFrom::Start(at)).unwrap();
        f.read_exact(&mut b).unwrap();
        b[0] ^= 0x01;
        f.seek(SeekFrom::Start(at)).unwrap();
        f.write_all(&b).unwrap();
        f.sync_all().unwrap();
    }

    // The trailer — and therefore the flag — is untouched.
    let cat = read_trailer(&RealFs, &log).unwrap().expect("still sealed");
    assert!(cat.names_seal_pack(), "the flag survives extension damage");
    assert!(
        read_extension(&RealFs, &log, &cat).unwrap().is_none(),
        "but the extension no longer verifies"
    );
    // The pack itself is untouched and perfectly valid.
    assert!(pack_identity_on_disk(&store, victim).is_some());

    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    let mine: Vec<_> =
        health.refutations.iter().filter(|r| r.segment_id == victim).collect();
    assert_eq!(mine.len(), 1, "{:?}", health.refutations);
    assert_eq!(
        mine[0].reason,
        RefutationReason::PackIdentityUnresolvable,
        "an unreadable name must fail closed, never read as 'no name'"
    );
    assert!(mine[0].quarantined);
    assert_reads_match(&engine, &baseline).await;

    // And it converges: the re-seal rewrites both the pack and its footer.
    assert!(await_path(&pack_path(&store, victim)));
    drop(engine);
    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen 2");
    assert_eq!(engine.sealed_candidate_health().refuted(), 0);
    assert_eq!(
        named_identity(&store, victim).map(|n| n.identity),
        pack_identity_on_disk(&store, victim),
        "the repaired footer names the repaired pack"
    );
    assert_reads_match(&engine, &baseline).await;
}

/// An `identity_kind` this build cannot check is likewise unresolvable: the
/// *section* kind is known, so advisory-skip does not apply — the reader simply
/// cannot verify the claim and must not trust any pack (§3.3.3 rule 2c).
#[tokio::test]
async fn an_unknown_identity_kind_refuses_the_pack() {
    let d = mess_testkit::sweeping_temp_dir("ident-unknown-kind");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, pack_opts()).await;
    let victim = a_named_rolled_segment(&store);

    rewrite_footer(&store, victim, |mut named| {
        named.identity_kind = 0xBEEF; // a future hash domain
        Some(named)
    });

    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    let mine: Vec<_> =
        health.refutations.iter().filter(|r| r.segment_id == victim).collect();
    assert_eq!(mine.len(), 1, "{:?}", health.refutations);
    assert_eq!(mine[0].reason, RefutationReason::PackIdentityUnresolvable);
    assert_reads_match(&engine, &baseline).await;
}

/// An identity entry naming a *different* segment is unresolvable too — the
/// cross-check that stops a whole footer being lifted from another segment.
#[tokio::test]
async fn an_identity_naming_another_segment_refuses_the_pack() {
    let d = mess_testkit::sweeping_temp_dir("ident-wrong-segment");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, pack_opts()).await;
    let victim = a_named_rolled_segment(&store);

    rewrite_footer(&store, victim, |mut named| {
        named.segment_id = named.segment_id.wrapping_add(1000);
        Some(named)
    });

    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen");
    let mine: Vec<_> = engine
        .sealed_candidate_health()
        .refutations
        .iter()
        .filter(|r| r.segment_id == victim)
        .cloned()
        .collect();
    assert_eq!(mine.len(), 1);
    assert_eq!(mine[0].reason, RefutationReason::PackIdentityUnresolvable);
    assert_reads_match(&engine, &baseline).await;
}

// ---------------------------------------------------------------------------
// The legacy compatibility policy (D-FMT-10)
// ---------------------------------------------------------------------------

/// **A legacy footer keeps coverage-only trust.** Rewriting a named footer back
/// to its pre-bn-11g shape (empty extension, zero flags) must admit the pack
/// exactly as before: every store written before this bone is in that state,
/// and demoting them all to raw scans on upgrade would be a real availability
/// loss traded for a hypothetical one.
#[tokio::test]
async fn a_legacy_footer_still_admits_its_pack_on_coverage_alone() {
    let d = mess_testkit::sweeping_temp_dir("ident-legacy-policy");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, pack_opts()).await;
    let victim = a_named_rolled_segment(&store);
    let sealed_before = count_ext(&sealed_dir(&store), "seal");

    rewrite_footer(&store, victim, |_| None); // the pre-bn-11g footer

    let cat = read_trailer(&RealFs, &log_path(&store, victim))
        .unwrap()
        .expect("still a valid trailer");
    assert!(!cat.names_seal_pack(), "the legacy shape names nothing");
    assert_eq!(cat.ext_len, 0);
    assert_eq!(cat.ext_crc, 0);

    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    assert_eq!(
        health.refuted(),
        0,
        "a legacy footer must not refute its pack: {:?}",
        health.refutations
    );
    assert_eq!(
        engine.sealed_segment_count(),
        sealed_before,
        "and the segment is still served from the cold tier"
    );
    assert_reads_match(&engine, &baseline).await;
}

/// A sidecar-mode store writes legacy footers and legacy candidates, and is
/// wholly unaffected — the identity binding is a pack-path concern.
#[tokio::test]
async fn a_sidecar_mode_store_names_nothing_and_is_unaffected() {
    let d = mess_testkit::sweeping_temp_dir("ident-sidecar-mode");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, sidecar_opts()).await;

    let ids = segment_ids(&store);
    let head = *ids.last().expect("head");
    for id in ids.iter().copied().filter(|&i| i != head) {
        assert!(
            named_identity(&store, id).is_none(),
            "segment {id}: a sidecar seal must name no pack"
        );
        let cat = read_trailer(&RealFs, &log_path(&store, id)).unwrap();
        if let Some(cat) = cat {
            assert!(!cat.names_seal_pack());
            assert_eq!(cat.ext_len, 0, "byte-identical to the pre-bn-11g seal");
        }
    }

    let engine = LogEngine::open_with(&store, sidecar_opts()).expect("reopen");
    assert_eq!(engine.sealed_candidate_health().refuted(), 0);
    assert_reads_match(&engine, &baseline).await;
}

// ---------------------------------------------------------------------------
// Ordering: the crash windows either side of the footer write
// ---------------------------------------------------------------------------

/// **Torn pack/footer ordering.** The pack is made durable strictly before the
/// footer that names it, so the only reachable "half state" is a durable pack
/// with no footer — never a footer naming absent or partial bytes. Truncating
/// the whole footer (extension **and** trailer) reproduces exactly that crash
/// point: the candidate is footerless, so the recovery scan confirms it, it is
/// served cold, and the segment is re-queued so the naming footer lands.
#[tokio::test]
async fn a_crash_between_the_pack_and_its_footer_converges() {
    let d = mess_testkit::sweeping_temp_dir("ident-torn-ordering");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, pack_opts()).await;
    let victim = a_named_rolled_segment(&store);
    let sealed_before = count_ext(&sealed_dir(&store), "seal");

    // The durable re-seal intent an earlier refutation left. bn-30u keys the
    // re-seal on this marker rather than on "a candidate was refuted this
    // open", precisely so this crash point converges; the state under test is
    // "the fresh pack landed, the footer did not".
    std::fs::write(quarantined_pack(&store, victim), b"refuted bytes")
        .expect("marker");

    // Drop the whole footer — the state a crash between the pack rename and
    // the footer fsync leaves. `content_len` is where the footer begins.
    let cat =
        read_trailer(&RealFs, &log_path(&store, victim)).unwrap().unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(log_path(&store, victim))
        .expect("open log")
        .set_len(cat.ext_offset)
        .expect("drop the footer");
    assert!(
        read_trailer(&RealFs, &log_path(&store, victim)).unwrap().is_none()
            || !read_trailer(&RealFs, &log_path(&store, victim))
                .unwrap()
                .unwrap()
                .names_seal_pack(),
        "the segment is now footerless"
    );
    // The pack is untouched and complete — the ordering guarantee in action.
    assert!(pack_identity_on_disk(&store, victim).is_some());

    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    assert_eq!(
        health.refuted(),
        0,
        "a footerless candidate is confirmable, not refutable: {:?}",
        health.refutations
    );
    assert_eq!(
        health.pending_reseal,
        vec![victim],
        "confirmed but unnamed: still owed a footer"
    );
    assert_eq!(engine.sealed_segment_count(), sealed_before, "served cold");
    assert_reads_match(&engine, &baseline).await;

    // The enqueued re-seal writes the footer, which names the pack.
    drop(engine);
    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen 2");
    assert_eq!(engine.sealed_candidate_health().refuted(), 0);
    assert_eq!(
        named_identity(&store, victim).map(|n| n.identity),
        pack_identity_on_disk(&store, victim),
        "convergence: the footer names the pack that is there"
    );
    assert_reads_match(&engine, &baseline).await;
}

/// **A footer naming an absent pack.** The pack is gone. There is no candidate
/// to refute, so nothing is quarantined and nothing is falsely accepted — the
/// segment simply loses its cold tier and is served from the raw log, which is
/// the same non-event a missing sidecar has always been (D1/I5).
#[tokio::test]
async fn a_named_footer_with_no_pack_serves_from_the_log() {
    let d = mess_testkit::sweeping_temp_dir("ident-missing-pack");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, pack_opts()).await;
    let victim = a_named_rolled_segment(&store);
    let sealed_before = count_ext(&sealed_dir(&store), "seal");

    std::fs::remove_file(pack_path(&store, victim)).expect("delete the pack");

    let engine = LogEngine::open_with(&store, pack_opts()).expect("reopen");
    let health = engine.sealed_candidate_health();
    assert_eq!(
        health.refuted(),
        0,
        "nothing on disk to refute: {:?}",
        health.refutations
    );
    assert_eq!(
        engine.sealed_segment_count(),
        sealed_before - 1,
        "the segment lost its cold tier and nothing else"
    );
    assert_reads_match(&engine, &baseline).await;
}

/// **Turning `seal_pack` off after a pack-mode store existed.** The re-seal
/// writes a *shorter* footer (no extension, no name) over a longer named one.
/// The trailer must still land at EOF — otherwise the segment silently reads as
/// unsealed forever, which is correct but a permanent, invisible regression to
/// full scans.
#[tokio::test]
async fn re_sealing_with_packs_off_leaves_the_trailer_at_eof() {
    let d = mess_testkit::sweeping_temp_dir("ident-mode-switch");
    let store = d.path().join("store");
    let baseline = build_rolled_store(&store, pack_opts()).await;
    let victim = a_named_rolled_segment(&store);

    // Refute the pack so the segment is re-queued, then reopen in SIDECAR mode:
    // the fresh seal writes a `.pidx` and an unnamed 100-byte footer.
    std::fs::write(pack_path(&store, victim), b"not a pack").expect("corrupt");
    let engine =
        LogEngine::open_with(&store, sidecar_opts()).expect("reopen off");
    assert_eq!(engine.sealed_candidate_health().refuted(), 1);
    assert_eq!(engine.sealed_candidate_health().pending_reseal, vec![victim]);
    assert!(
        await_path(&sealed_dir(&store).join(format!("seg-{victim:020}.pidx"))),
        "the sidecar-mode re-seal lands"
    );
    drop(engine);

    let cat = read_trailer(&RealFs, &log_path(&store, victim))
        .expect("read")
        .expect("the shorter footer still parses from EOF");
    assert!(!cat.names_seal_pack(), "a sidecar seal names no pack");
    assert_eq!(cat.ext_len, 0);
    assert_eq!(
        std::fs::metadata(log_path(&store, victim)).unwrap().len(),
        cat.ext_offset + mess_log::format::SEGMENT_TRAILER_LEN as u64,
        "no stale bytes past the trailer"
    );

    let engine = LogEngine::open_with(&store, sidecar_opts()).expect("reopen");
    assert_eq!(engine.sealed_candidate_health().refuted(), 0);
    assert_reads_match(&engine, &baseline).await;
}
