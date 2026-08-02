//! bn-3m62: what a **pack-default** store puts on disk for the segment that is
//! not sealed yet — and, by construction, what it does not.
//!
//! bn-3qh0 left a follow-up note claiming the live head of a `seal_pack` store
//! still picked up a legacy loose `.pidx` from the non-pack branch, which would
//! mean every pack-default store carried one loose sidecar family forever and
//! would quietly cost bn-1yor's "1.00 files per sealed segment" its headline.
//! It does not, and this suite is the standing proof — the interesting kind,
//! because the property is a *negative* one that no existing test asserted and
//! that one dropped `.with_pack(…)` at either engine call site would silently
//! break.
//!
//! The two writers of a `.pidx` in the whole tree are `SealDriver::seal`'s
//! non-pack branch — unreachable when the driver has `pack` set, because `seal`
//! returns into `seal_consolidated` before it — and the offline
//! `mess rebuild-index`. Both engine drivers are constructed with
//! `.with_pack(opts.seal_pack)` (the background roll-sealer and the on-demand
//! `seal_active`), and `EngineOptions::default()` sets it. So the head gets
//! nothing at all until it rolls, and the fix for the "inventory is muddied"
//! complaint belongs in the inventory surfaces, not here. See
//! `mess-cli/tests/head_inventory_reseal_severity.rs` for that half.

#![cfg(not(miri))]

use std::path::Path;

use mess_log::committer::Durability;
use mess_store::backend::{Backend, RecordToAppend, StoredRecord};
use mess_store::{EngineOptions, LogEngine, Version};

const STREAMS: usize = 4;

fn pack_opts(segment_size: u64) -> EngineOptions {
    EngineOptions {
        durability: Durability::Process,
        segment_size,
        seal_pack: true,
        ..EngineOptions::default()
    }
}

fn payload(i: u64) -> Vec<u8> { vec![(i % 251) as u8; 300] }

/// Append `n` single-event batches round-robin over [`STREAMS`] streams.
async fn seed(engine: &LogEngine, heads: &mut [Version], n: u64) {
    for i in 0..n {
        let s = (i % STREAMS as u64) as usize;
        let out = engine
            .append_batch(
                &format!("acct-{s}"),
                heads[s],
                &[RecordToAppend {
                    message_type: format!("t{}", i % 3),
                    data:         payload(i),
                }],
            )
            .await
            .expect("append");
        heads[s] = out.version;
    }
}

/// Every file under `sealed/`, sorted. The whole point of these assertions is
/// the *complete* listing: a test that only checked "no `.pidx` for the head"
/// would miss a stray `.filter`/`.pcol`/`.reg`, which is the same bug.
fn sealed_listing(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir.join("sealed"))
        .map(|rd| {
            rd.filter_map(Result::ok)
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

fn segment_ids(dir: &Path) -> Vec<u64> {
    let mut ids: Vec<u64> = std::fs::read_dir(dir)
        .expect("read dir")
        .filter_map(Result::ok)
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.strip_prefix("seg-")
                .and_then(|r| r.strip_suffix(".log"))
                .and_then(|n| n.parse::<u64>().ok())
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// The one-file-per-sealed-segment law, stated as a listing: every name under
/// `sealed/` is `seg-<id>.seal`, and the set of ids is exactly `expect_sealed`.
fn assert_only_packs_for(dir: &Path, expect_sealed: &[u64], ctx: &str) {
    let listing = sealed_listing(dir);
    let want: Vec<String> =
        expect_sealed.iter().map(|id| format!("seg-{id:020}.seal")).collect();
    assert_eq!(
        listing, want,
        "{ctx}: sealed/ must hold exactly one .seal per sealed segment and \
         nothing else (no .pidx, .filter, .pcol or .reg) — bn-1yor's 1.00 \
         files per sealed segment"
    );
}

/// A pack-default store that has really rolled: the sealed segments each carry
/// exactly one `.seal`, and the **live head carries nothing at all**.
///
/// The head's absence is asserted path by path as well as through the listing,
/// so a failure names which artifact leaked rather than just diffing a vector.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pack_default_store_writes_no_artifact_for_the_live_head() {
    let d = mess_testkit::sweeping_temp_dir("bn3m62-head-empty");
    let mut heads = vec![Version::NoStream; STREAMS];
    let engine =
        LogEngine::open_with(d.path(), pack_opts(32 * 1024)).expect("open");
    seed(&engine, &mut heads, 400).await;
    // Let the background roll-sealer drain the rolls it queued.
    std::thread::sleep(std::time::Duration::from_millis(400));

    let ids = segment_ids(d.path());
    assert!(ids.len() >= 3, "the corpus must really roll: {ids:?}");
    let head = *ids.last().expect("non-empty");
    let rolled: Vec<u64> = ids.iter().copied().filter(|&i| i != head).collect();

    assert_only_packs_for(d.path(), &rolled, "live store");
    for ext in ["pidx", "filter", "pcol", "reg", "seal", "par"] {
        let p = d.path().join("sealed").join(format!("seg-{head:020}.{ext}"));
        assert!(
            !p.exists(),
            "the live head must carry no .{ext}: {} exists",
            p.display()
        );
    }

    // …and closing the store does not conjure one either: `Inner::drop` drains
    // the sealer, which seals ROLLED segments, never the head.
    drop(engine);
    assert_only_packs_for(d.path(), &rolled, "after close");

    // A reopen re-classifies every candidate and re-queues what is owed. A
    // healthy store owes nothing, so the listing is byte-identical again.
    let engine =
        LogEngine::open_with(d.path(), pack_opts(32 * 1024)).expect("reopen");
    let _ = engine.read_global(None, 8).await.expect("read");
    drop(engine);
    assert_only_packs_for(d.path(), &rolled, "after reopen");
}

/// When the head finally rolls it gains **exactly one** artifact — a `.seal` —
/// and the segment that succeeds it starts out empty again. This is the
/// "superseded when the segment rolls" half of the story, pinned as a
/// before/after inventory rather than an assumption.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_head_gains_exactly_one_pack_when_it_rolls() {
    let d = mess_testkit::sweeping_temp_dir("bn3m62-head-rolls");
    let mut heads = vec![Version::NoStream; STREAMS];
    let engine =
        LogEngine::open_with(d.path(), pack_opts(32 * 1024)).expect("open");
    seed(&engine, &mut heads, 200).await;
    std::thread::sleep(std::time::Duration::from_millis(400));

    let before = segment_ids(d.path());
    let old_head = *before.last().expect("non-empty");
    assert!(
        !d.path()
            .join("sealed")
            .join(format!("seg-{old_head:020}.seal"))
            .exists(),
        "precondition: the head is unsealed"
    );

    // Keep appending until the head rolls at least once more.
    let mut pushed = 0u64;
    while segment_ids(d.path()).len() == before.len() && pushed < 4_000 {
        seed(&engine, &mut heads, 40).await;
        pushed += 40;
    }
    std::thread::sleep(std::time::Duration::from_millis(400));
    drop(engine);

    let after = segment_ids(d.path());
    assert!(after.len() > before.len(), "the head must have rolled");
    let new_head = *after.last().expect("non-empty");
    let rolled: Vec<u64> =
        after.iter().copied().filter(|&i| i != new_head).collect();
    assert!(rolled.contains(&old_head), "the old head is now rolled");

    assert_only_packs_for(d.path(), &rolled, "after the roll");
    for ext in ["pidx", "filter", "pcol", "reg", "seal"] {
        assert!(
            !d.path()
                .join("sealed")
                .join(format!("seg-{new_head:020}.{ext}"))
                .exists(),
            "the NEW head must carry no .{ext} either"
        );
    }
}

/// An **unrolled** store — nothing has ever been sealed, `sealed/` is empty —
/// cold-opens and serves every event off the log.
///
/// This is the law the head's empty artifact set rests on: the head has no
/// accelerator because it does not need one, and the only authority is the log
/// (D1). If this ever failed, the head would need an artifact and the whole
/// question this bone asks would have a different answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cold_open_of_an_unrolled_store_serves_every_event_from_the_log() {
    let d = mess_testkit::sweeping_temp_dir("bn3m62-unrolled");
    let baseline: Vec<StoredRecord> = {
        let engine = LogEngine::open_with(d.path(), pack_opts(1 << 20))
            .expect("open fresh");
        let mut heads = vec![Version::NoStream; STREAMS];
        seed(&engine, &mut heads, 120).await;
        let b = engine.read_global(None, 1024).await.expect("baseline");
        drop(engine);
        b
    };
    assert_eq!(baseline.len(), 120, "every appended event is readable");

    assert_eq!(
        segment_ids(d.path()),
        vec![1],
        "precondition: the store never rolled"
    );
    assert!(
        sealed_listing(d.path()).is_empty(),
        "precondition: nothing was sealed, so sealed/ is empty: {:?}",
        sealed_listing(d.path())
    );

    // Cold open: a brand-new process's view of the same bytes.
    let engine =
        LogEngine::open_with(d.path(), pack_opts(1 << 20)).expect("cold open");
    let got = engine.read_global(None, 1024).await.expect("cold global read");
    assert_eq!(got.len(), baseline.len(), "cold open serves every event");
    for (a, b) in got.iter().zip(&baseline) {
        assert_eq!(a.stream_id, b.stream_id);
        assert_eq!(a.message_type, b.message_type);
        assert_eq!(a.data, b.data, "payload bytes come off the log");
        assert_eq!(a.global_position, b.global_position);
        assert_eq!(a.stream_position, b.stream_position);
    }
    for s in 0..STREAMS {
        let name = format!("acct-{s}");
        let want: Vec<&StoredRecord> =
            baseline.iter().filter(|r| r.stream_id == name).collect();
        let got = engine
            .read_stream(&name, Version::NoStream, 1024)
            .await
            .expect("cold stream read");
        assert_eq!(got.len(), want.len(), "{name}: every event");
        for (a, b) in got.iter().zip(&want) {
            assert_eq!(a.data, b.data, "{name}: payload bytes");
        }
    }
    drop(engine);

    // And the cold open did not decide to leave an artifact behind.
    assert!(
        sealed_listing(d.path()).is_empty(),
        "a cold open of an unrolled store writes no sealed artifact: {:?}",
        sealed_listing(d.path())
    );
}

/// The one way a live head legitimately acquires a sealed artifact — an
/// explicit `seal_active()` — produces a **pack**, not the legacy trio.
///
/// This is the assertion that would have caught bn-3qh0's note directly:
/// `seal_active` is the only engine entry point that seals the still-live head,
/// and it honours `seal_pack` exactly like the roll-sealer does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_active_over_a_live_head_writes_a_pack_not_a_pidx() {
    let d = mess_testkit::sweeping_temp_dir("bn3m62-seal-active");
    let engine =
        LogEngine::open_with(d.path(), pack_opts(1 << 20)).expect("open");
    let mut heads = vec![Version::NoStream; STREAMS];
    seed(&engine, &mut heads, 80).await;
    assert!(sealed_listing(d.path()).is_empty(), "nothing sealed yet");

    engine.seal_active().expect("seal_active");

    assert_eq!(
        sealed_listing(d.path()),
        vec![format!("seg-{:020}.seal", 1u64)],
        "seal_active on a pack-default store writes ONE .seal for the head, \
         and no .pidx/.filter/.pcol/.reg"
    );

    // The segment is still live and footerless — the pack is a candidate the
    // recovery scan confirms, exactly as bn-11g's D-FMT-10 note describes.
    let reads = engine.read_global(None, 1024).await.expect("read");
    assert_eq!(reads.len(), 80);
    drop(engine);
}
