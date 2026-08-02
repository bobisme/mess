//! `bn-11ba`: the composed observability surface —
//! [`LogEngine::observability`] — reports the authority model, the installed
//! accelerators, and the fallback/backlog counters, and **flips** when a
//! fault is injected.
//!
//! The point of this suite is not that the fields exist. It is that each one
//! is wired to the thing it claims to describe: delete a sidecar and the
//! representation changes; corrupt a candidate and the refutation counters
//! move; force a version conflict and the conflict counter moves; drop an
//! append future and the cancellation counter moves. A metric nobody has ever
//! watched change is indistinguishable from a metric hardwired to zero, which
//! is exactly the failure `bn-11n` documented on the fsync alarm.
#![cfg(not(miri))]

use std::path::{Path, PathBuf};

use mess_store::backend::{Backend, RecordToAppend};
use mess_store::observability::{Role, SealedRepresentation};
use mess_store::{EngineOptions, LogEngine, RefutationReason, Version};

const STREAMS: usize = 8;
const PER: usize = 45;

fn rec(t: &str, d: &[u8]) -> RecordToAppend {
    RecordToAppend { message_type: t.to_string(), data: d.to_vec() }
}

fn payload(s: usize, i: usize) -> Vec<u8> {
    format!("s{s:03}-e{i:04}").into_bytes()
}

/// A tiny active segment so a few hundred small batches roll several times and
/// the background sealer produces real sealed segments. Mirrors
/// `sealed_candidate_lifecycle::rolling_opts` but leaves `seal_pack` at the
/// caller's choice — the representation is exactly what this suite asserts.
fn rolling_opts(seal_pack: bool) -> EngineOptions {
    EngineOptions {
        segment_size: 16 * 1024,
        seal_pack,
        ..EngineOptions::default()
    }
}

/// Build a multi-segment store and drain its sealer.
async fn build_rolled_store(store: &Path, opts: EngineOptions) {
    let engine = LogEngine::open_with(store, opts).expect("open fresh");
    for s in 0..STREAMS {
        let name = format!("stream-{s}");
        let mut expected = Version::NoStream;
        for i in 0..PER {
            let out = engine
                .append_batch(&name, expected, &[rec("ev", &payload(s, i))])
                .await
                .expect("append");
            expected = out.version;
        }
    }
    drop(engine); // drains the sealer: sidecars + footers durable
}

fn sealed_dir(store: &Path) -> PathBuf { store.join("sealed") }

fn candidate_path(store: &Path, seg_id: u64, ext: &str) -> PathBuf {
    sealed_dir(store).join(format!("seg-{seg_id:020}.{ext}"))
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

/// A rolled (non-head) segment that really carries a `.<ext>` candidate.
fn a_rolled_sealed_segment(store: &Path, ext: &str) -> u64 {
    let ids = segment_ids(store);
    assert!(ids.len() >= 2, "the corpus must have rolled: {ids:?}");
    let head = *ids.last().expect("non-empty");
    ids.into_iter()
        .filter(|&id| id != head)
        .find(|&id| candidate_path(store, id, ext).exists())
        .unwrap_or_else(|| panic!("no rolled segment carries a .{ext}"))
}

/// Every read still comes back complete. Asserted alongside every fault
/// injection below: the whole claim of the authority model is that throwing
/// an accelerator away costs time, not data.
async fn assert_reads_complete(engine: &LogEngine) {
    let g =
        engine.read_global(None, STREAMS * PER * 2).await.expect("global read");
    assert_eq!(g.len(), STREAMS * PER, "every event still served");
    for s in 0..STREAMS {
        let name = format!("stream-{s}");
        let evs = engine
            .read_stream(&name, Version::NoStream, PER * 2)
            .await
            .expect("stream read");
        assert_eq!(evs.len(), PER, "{name}: every event served");
    }
}

// ---------------------------------------------------------------------------
// Authority
// ---------------------------------------------------------------------------

/// The authority model is carried on the report, and it says exactly what
/// ADR 0002 says: the log and `$registry` are canonical, everything else is
/// discardable. Nothing is unclassified, and no accelerator is canonical.
#[tokio::test]
async fn the_report_classifies_every_artifact_and_never_promotes_one() {
    let d = mess_testkit::sweeping_temp_dir("obs-authority-model");
    let engine = LogEngine::open(d.path()).expect("open");
    let o = engine.observability();

    let canonical: Vec<&str> = o
        .authority
        .iter()
        .filter(|c| c.role == Role::Canonical)
        .map(|c| c.name)
        .collect();
    assert_eq!(
        canonical,
        vec!["seg-*.log", "$registry"],
        "exactly the two canonical sources ADR 0002 names"
    );

    for class in o.authority {
        assert!(!class.what.is_empty(), "{}: needs a description", class.name);
        assert!(
            !class.on_loss.is_empty(),
            "{}: an operator must be told what happens on loss",
            class.name
        );
    }
    // The sealed-index artifacts are accelerators, full stop.
    for name in [".seal", ".pidx", ".pcol", ".filter", ".reg", ".par"] {
        let c = o
            .authority
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("{name} must be classified"));
        assert_eq!(
            c.role,
            Role::DiscardableAccelerator,
            "{name} must never be reported as authoritative"
        );
        assert_eq!(c.role.as_str(), "discardable-accelerator");
    }
    // ADR 0003 declined v4: v3 is the only canonical log version reported.
    assert_eq!(o.state.log_format_version, 3);
}

// ---------------------------------------------------------------------------
// Owner: queue, group shape, outcomes
// ---------------------------------------------------------------------------

/// A durable workload moves the owner's group-shape and latency histograms,
/// and a quiescent engine reports an empty queue at its documented capacity.
#[tokio::test]
async fn owner_queue_group_and_latency_move_under_a_durable_workload() {
    let d = mess_testkit::sweeping_temp_dir("obs-owner-group");
    let opts = EngineOptions {
        durability: mess_log::committer::Durability::group_default(),
        ..EngineOptions::default()
    };
    let engine = LogEngine::open_with(d.path(), opts).expect("open");

    let before = engine.observability();
    assert_eq!(before.owner.durability_mode.name, "group");
    assert!(before.owner.durability_mode.max_delay_nanos.is_some());
    assert_eq!(before.owner.group_width.count, 0, "no group gathered yet");
    assert_eq!(before.owner.ack_latency.count, 0);
    assert!(
        before.owner.queue_slots_capacity > 0
            && before.owner.queue_bytes_capacity > 0,
        "the bounds must be reported, or saturation is uncomputable"
    );
    assert_eq!(before.owner.slot_saturation(), 0.0);
    assert_eq!(before.owner.byte_saturation(), 0.0);

    for i in 0..8u64 {
        let expected =
            if i == 0 { Version::NoStream } else { Version::At(i - 1) };
        engine
            .append_batch("acct-1", expected, &[rec("Ev", b"x")])
            .await
            .expect("append");
    }

    let o = engine.observability();
    assert!(o.owner.group_width.count >= 1, "a group was gathered");
    assert!(
        o.owner.group_width.max_nanos >= 1,
        "group width is in intents and at least one intent was gathered"
    );
    assert_eq!(
        o.owner.group_wait.count, o.owner.group_width.count,
        "width and wait are recorded together, once per group"
    );
    assert!(o.owner.ack_latency.count >= 8, "one ack sample per append");
    assert!(
        o.owner.ack_latency.max_nanos > 0,
        "an append cannot complete in zero time"
    );
    assert!(o.owner.commit_latency.count >= 1, "durable commit spans timed");
    assert_eq!(o.owner.conflicts, 0, "no conflicting append was made");
    assert_eq!(o.owner.cancellations, 0, "no caller dropped its future");
    assert!(o.owner.events >= 8);
    assert_eq!(
        o.owner.queue_slots_in_use, 0,
        "a quiescent owner retains no intent slots"
    );
    assert_eq!(o.owner.queue_bytes_in_use, 0);
    // The append-input counters are composed in, not re-derived.
    assert_eq!(
        o.owner.append_input,
        engine.append_input_metrics(),
        "append-input counters are composed, not copied"
    );
}

/// A refused expected-version append moves the conflict counter — and only
/// the conflict counter.
#[tokio::test]
async fn a_version_conflict_moves_the_conflict_counter() {
    let d = mess_testkit::sweeping_temp_dir("obs-conflict");
    let engine = LogEngine::open(d.path()).expect("open");

    engine
        .append_batch("acct-1", Version::NoStream, &[rec("Opened", b"x")])
        .await
        .expect("first append");
    assert_eq!(engine.observability().owner.conflicts, 0);

    // Re-append at the version that is already taken.
    let err = engine
        .append_batch("acct-1", Version::NoStream, &[rec("Opened", b"y")])
        .await
        .expect_err("expected-version conflict");
    assert!(
        matches!(err, mess_store::AppendError::Conflict { .. }),
        "got {err:?}"
    );

    let o = engine.observability();
    assert_eq!(o.owner.conflicts, 1, "the refusal is counted exactly once");
    assert_eq!(o.owner.cancellations, 0, "a conflict is not a cancellation");

    // A second conflict counts again; a success does not.
    let _ = engine
        .append_batch("acct-1", Version::NoStream, &[rec("Opened", b"z")])
        .await
        .expect_err("second conflict");
    engine
        .append_batch("acct-1", Version::At(0), &[rec("Closed", b"w")])
        .await
        .expect("valid append");
    assert_eq!(engine.observability().owner.conflicts, 2);
}

/// A caller that drops its append future before the outcome lands is counted
/// as a cancellation. The events still commit and publish (`bn-3nz`) — this
/// counts a caller that stopped listening, which is exactly why it needs its
/// own counter rather than being folded into an error rate.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_append_future_is_counted_as_a_cancellation() {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    let d = mess_testkit::sweeping_temp_dir("obs-cancel");
    let engine = LogEngine::open(d.path()).expect("open");

    // Poll once (launching the detached commit) and drop. Whether one poll is
    // enough to leave it `Pending` is pure scheduling, so this is a retried
    // *precondition*, never an asserted outcome — the same discipline
    // `engine_publish_cancel` uses.
    let mut cancelled = 0u64;
    let mut version = Version::NoStream;
    for _ in 0..50 {
        // The future (and the storage `pin!` puts it in) lives exactly as
        // long as this block, so leaving the block on the `Pending` arm IS
        // the drop-in-flight. An explicit `drop` of the `Pin<&mut _>` would
        // not be one — it only drops the pointer.
        let outcome = {
            let records = [rec("Ev", b"x")];
            let mut fut = pin!(engine.append_batch("s", version, &records));
            let mut cx = Context::from_waker(Waker::noop());
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(out) => Some(out.expect("append").version),
                Poll::Pending => None,
            }
        };
        match outcome {
            // It completed inside one poll: no cancellation, but the stream
            // advanced, so keep the expected version honest.
            Some(v) => version = v,
            None => {
                cancelled += 1;
                break;
            }
        }
    }
    assert_eq!(
        cancelled, 1,
        "the append path stopped parking at all — that is a behaviour change \
         worth failing on, not host-load noise"
    );

    // The detached commit still has to run and fail its send. Bounded wait.
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(10);
    while engine.observability().owner.cancellations == 0
        && std::time::Instant::now() < deadline
    {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let o = engine.observability();
    assert_eq!(
        o.owner.cancellations, 1,
        "the dropped future is counted exactly once"
    );
    assert_eq!(o.owner.conflicts, 0, "a cancellation is not a conflict");
}

// ---------------------------------------------------------------------------
// Accelerator representation, per segment
// ---------------------------------------------------------------------------

/// The default (`seal_pack: true`) store reports every sealed segment as a
/// SealPack, with an identity and a pack format version; the compatibility
/// mode reports the loose sidecar shape, with neither. The two runs differ in
/// exactly one option, so the field really is reading the representation.
#[tokio::test(flavor = "multi_thread")]
async fn the_report_names_the_sealed_representation_per_segment() {
    for seal_pack in [true, false] {
        let d = mess_testkit::sweeping_temp_dir(&format!(
            "obs-representation-{seal_pack}"
        ));
        let store = d.path().join("store");
        build_rolled_store(&store, rolling_opts(seal_pack)).await;

        let engine = LogEngine::open_with(&store, rolling_opts(seal_pack))
            .expect("reopen");
        let o = engine.observability();
        assert_eq!(o.accelerators.seal_pack_enabled, seal_pack);
        assert!(
            !o.accelerators.segments.is_empty(),
            "the corpus rolled and sealed at least one segment"
        );
        for row in &o.accelerators.segments {
            if seal_pack {
                assert_eq!(
                    row.representation,
                    SealedRepresentation::SealPack,
                    "seg {}: pack-sealed store",
                    row.segment_id
                );
                assert!(
                    row.pack_identity_hex
                        .as_ref()
                        .is_some_and(|h| h.len() == 64
                            && h.chars().all(|c| c.is_ascii_hexdigit())),
                    "seg {}: a pack carries a 32-byte identity",
                    row.segment_id
                );
                assert!(row.pack_format_version.is_some());
            } else {
                assert_eq!(
                    row.representation,
                    SealedRepresentation::LooseSidecar,
                    "seg {}: loose-sidecar store",
                    row.segment_id
                );
                assert!(
                    row.pack_identity_hex.is_none(),
                    "a loose sidecar has no pack identity to bind"
                );
                assert!(row.pack_format_version.is_none());
            }
            assert!(row.event_count > 0);
            assert!(row.resident_bytes > 0, "an installed index holds bytes");
            assert!(
                !row.dir_codec_name.is_empty(),
                "the directory codec must be named"
            );
        }
        assert_eq!(
            o.accelerators.seal_pack_segments
                + o.accelerators.loose_sidecar_segments,
            o.accelerators.segments.len(),
            "every segment is classified into exactly one representation"
        );
        assert_eq!(
            o.state.sealed_segment_count,
            o.accelerators.segments.len(),
            "the rollup and the rows agree"
        );
        assert_eq!(
            o.state.sealed_index_resident_bytes,
            o.accelerators.segments.iter().map(|r| r.resident_bytes).sum(),
            "resident bytes is the sum of the rows, not a separate estimate"
        );
        assert_reads_complete(&engine).await;
    }
}

// ---------------------------------------------------------------------------
// Fault injection: the fallback and backlog counters must FLIP
// ---------------------------------------------------------------------------

/// **Delete an accelerator.** Removing a rolled segment's whole `.pidx`
/// family leaves the store correct and drops that segment out of the
/// accelerator inventory. The report says so before the background re-seal
/// puts it back.
#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_sidecar_removes_the_segment_from_the_inventory() {
    let d = mess_testkit::sweeping_temp_dir("obs-sidecar-deleted");
    let store = d.path().join("store");
    build_rolled_store(&store, rolling_opts(false)).await;

    let victim = a_rolled_sealed_segment(&store, "pidx");
    let before = {
        let engine =
            LogEngine::open_with(&store, rolling_opts(false)).expect("reopen");
        let o = engine.observability();
        assert!(
            o.accelerators.segments.iter().any(|r| r.segment_id == victim),
            "seg {victim} is indexed before the fault"
        );
        o.accelerators.segments.len()
    };

    for ext in ["pidx", "pcol", "filter", "reg"] {
        let p = candidate_path(&store, victim, ext);
        if p.exists() {
            std::fs::remove_file(&p).expect("delete accelerator");
        }
    }

    // Loose-sidecar mode with the sealer stopped: open, read the report, then
    // let the engine drop (which is when the re-seal would land).
    let engine =
        LogEngine::open_with(&store, rolling_opts(false)).expect("reopen");
    let o = engine.observability();
    assert!(
        !o.accelerators.segments.iter().any(|r| r.segment_id == victim),
        "seg {victim} lost its index and must not be reported as accelerated"
    );
    assert_eq!(
        o.accelerators.segments.len(),
        before - 1,
        "exactly the damaged segment dropped out"
    );
    // The whole claim: losing an accelerator costs time, not data.
    assert_reads_complete(&engine).await;
}

/// **Corrupt a candidate.** A `.pidx` that fails its own checksum is refuted
/// at open, quarantined, and its segment re-queued — and every one of those
/// three facts is visible on the report, with the reason string.
#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_candidate_flips_the_refutation_and_backlog_counters() {
    let d = mess_testkit::sweeping_temp_dir("obs-candidate-corrupt");
    let store = d.path().join("store");
    build_rolled_store(&store, rolling_opts(false)).await;

    let victim = a_rolled_sealed_segment(&store, "pidx");
    {
        // A healthy reopen refutes nothing and owes nothing — the control.
        let engine =
            LogEngine::open_with(&store, rolling_opts(false)).expect("reopen");
        let o = engine.observability();
        assert_eq!(o.fallbacks.sealed_candidates_refuted, 0);
        assert_eq!(o.backlog.reseals_owed_at_open, 0);
        assert!(!o.fallbacks.any(), "a healthy store runs no fallback path");
    }

    // Truncate the candidate so it parses as garbage.
    let path = candidate_path(&store, victim, "pidx");
    let bytes = std::fs::read(&path).expect("read candidate");
    std::fs::write(&path, &bytes[..bytes.len() / 2]).expect("truncate");

    let engine =
        LogEngine::open_with(&store, rolling_opts(false)).expect("reopen");
    let o = engine.observability();
    assert_eq!(
        o.fallbacks.sealed_candidates_refuted, 1,
        "the damaged candidate was refused"
    );
    assert_eq!(o.fallbacks.sealed_candidates_quarantined, 1);
    assert_eq!(o.fallbacks.quarantine_failures, 0);
    assert!(o.fallbacks.any(), "a fallback path ran");
    let r = &o.fallbacks.refutations[0];
    assert_eq!(r.segment_id, victim);
    assert_eq!(r.reason, RefutationReason::Unparsable);
    assert!(r.quarantined);
    assert_eq!(
        o.backlog.reseals_owed_at_open, 1,
        "the segment is owed a fresh seal"
    );
    assert_eq!(o.backlog.pending_reseal, vec![victim]);
    assert!(
        o.backlog.seal_jobs_dequeued >= 1 || o.backlog.seal_queue_depth >= 1,
        "the owed re-seal is either queued or already picked up (depth={}, \
         dequeued={})",
        o.backlog.seal_queue_depth,
        o.backlog.seal_jobs_dequeued,
    );
    assert_reads_complete(&engine).await;
}

/// **Corrupt a SealPack candidate.** The pack shape takes the same lifecycle,
/// and the report tells the same story — so an operator reading it does not
/// have to know which sealing mode the store was written with.
#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_seal_pack_flips_the_same_counters() {
    let d = mess_testkit::sweeping_temp_dir("obs-pack-corrupt");
    let store = d.path().join("store");
    build_rolled_store(&store, rolling_opts(true)).await;

    let victim = a_rolled_sealed_segment(&store, "seal");
    let path = candidate_path(&store, victim, "seal");
    let bytes = std::fs::read(&path).expect("read pack");
    std::fs::write(&path, &bytes[..bytes.len() / 2]).expect("truncate");

    let engine =
        LogEngine::open_with(&store, rolling_opts(true)).expect("reopen");
    let o = engine.observability();
    assert_eq!(o.fallbacks.sealed_candidates_refuted, 1);
    assert_eq!(o.fallbacks.sealed_candidates_quarantined, 1);
    assert_eq!(o.backlog.reseals_owed_at_open, 1);
    assert_eq!(o.backlog.pending_reseal, vec![victim]);
    assert!(
        !o.accelerators.segments.iter().any(|r| r.segment_id == victim),
        "a refuted pack is not an installed accelerator"
    );
    assert_reads_complete(&engine).await;
}

/// **The registry-delta accelerator.** A sealed store's reopen either admits
/// each segment's registry delta or point-reads the same batches out of the
/// log; deleting the deltas moves the count from the first bucket to the
/// second, and folds the identical registry either way.
#[tokio::test(flavor = "multi_thread")]
async fn deleting_registry_deltas_moves_the_fallback_count_not_the_registry() {
    let d = mess_testkit::sweeping_temp_dir("obs-regdelta-fallback");
    let store = d.path().join("store");
    build_rolled_store(&store, rolling_opts(false)).await;

    let (admitted, fallback, hwm) = {
        let engine =
            LogEngine::open_with(&store, rolling_opts(false)).expect("reopen");
        let o = engine.observability();
        (
            o.fallbacks.registry_delta_admitted,
            o.fallbacks.registry_delta_fallback,
            o.state.registry_stream_hwm,
        )
    };
    assert!(
        admitted + fallback > 0,
        "a rolled+sealed store reopens through at least one sealed segment \
         carrying $registry batches"
    );
    assert!(hwm > 0, "the corpus registered stream names");

    // Delete every `.reg` sidecar: the accelerator is gone, the batches are
    // not.
    let mut deleted = 0;
    for e in
        std::fs::read_dir(sealed_dir(&store)).expect("sealed dir").flatten()
    {
        if e.path().extension().and_then(|x| x.to_str()) == Some("reg") {
            std::fs::remove_file(e.path()).expect("delete .reg");
            deleted += 1;
        }
    }
    assert!(deleted > 0, "the loose-sidecar corpus wrote registry deltas");

    let engine =
        LogEngine::open_with(&store, rolling_opts(false)).expect("reopen");
    let o = engine.observability();
    assert_eq!(
        o.fallbacks.registry_delta_admitted, 0,
        "no delta survives to be admitted"
    );
    assert_eq!(
        o.fallbacks.registry_delta_fallback,
        admitted + fallback,
        "every segment that used a delta now takes the point-read path"
    );
    assert_eq!(
        o.state.registry_stream_hwm, hwm,
        "the accelerator is discardable: the folded registry is identical"
    );
    assert_reads_complete(&engine).await;
}

/// The backlog gauge returns to zero on a quiescent store: a report that
/// only ever counted up would make every healthy store look like it was
/// falling behind.
#[tokio::test(flavor = "multi_thread")]
async fn the_seal_backlog_drains_to_zero_on_a_healthy_store() {
    let d = mess_testkit::sweeping_temp_dir("obs-backlog-drains");
    let store = d.path().join("store");
    build_rolled_store(&store, rolling_opts(true)).await;

    let engine =
        LogEngine::open_with(&store, rolling_opts(true)).expect("reopen");
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(10);
    while engine.observability().backlog.draining()
        && std::time::Instant::now() < deadline
    {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let o = engine.observability();
    assert_eq!(o.backlog.seal_queue_depth, 0, "the sealer caught up");
    assert!(!o.backlog.draining());
    assert_eq!(o.backlog.reseals_owed_at_open, 0);
    assert_eq!(o.fallbacks.seals_skipped, 0);
    assert_eq!(o.backlog.seals_skipped, o.fallbacks.seals_skipped);
}

// ---------------------------------------------------------------------------
// Cardinality
// ---------------------------------------------------------------------------

/// The report's only per-item collections are bounded by the segment count.
/// A stream id or event type leaking in as a key is the failure mode that
/// makes an observability surface unusable at app scale, so it is asserted
/// rather than assumed.
#[tokio::test(flavor = "multi_thread")]
async fn cardinality_is_bounded_by_the_segment_count() {
    let d = mess_testkit::sweeping_temp_dir("obs-cardinality");
    let store = d.path().join("store");
    build_rolled_store(&store, rolling_opts(true)).await;

    let engine =
        LogEngine::open_with(&store, rolling_opts(true)).expect("reopen");
    let o = engine.observability();
    let segments = segment_ids(&store).len();

    assert!(
        o.accelerators.segments.len() <= segments,
        "one accelerator row per segment at most ({} rows, {segments} \
         segments)",
        o.accelerators.segments.len()
    );
    assert!(o.fallbacks.refutations.len() <= segments);
    assert!(o.backlog.pending_reseal.len() <= segments);
    // The corpus has 8 streams x 45 events; nothing in the report scales with
    // either. `stream_count` is a number on a segment row, never a key.
    let total_stream_slots: usize =
        o.accelerators.segments.iter().map(|r| r.stream_count).sum();
    assert!(
        total_stream_slots >= STREAMS,
        "sanity: the segments really do cover the corpus's streams"
    );
    // Rows are ordered and unique by segment id.
    let ids: Vec<u64> =
        o.accelerators.segments.iter().map(|r| r.segment_id).collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(ids, sorted, "rows are ascending and unique by segment id");
}
