//! bn-3jg — the randomized crash-recovery loop, ported from
//! `spikes/crash_log`'s 12k-iteration kill loop onto the **production**
//! types. There are zero spike copies of the format here: every byte is
//! produced by [`BatchEncoder`]/[`SegmentWriter`] driving the real
//! [`Committer`] over [`SimRuntime`], crashed through the sim fault fs
//! (`Fault::Tail` = the ported `crash_log::FaultWriter`/`TailDisk`), and
//! recovered by the production [`recover_segment`] scanner.
//!
//! # What each case does
//!
//! 1. Derive a workload + crash plan from a single per-case seed (the sole
//!    knob: a failing seed reprints and re-runs byte-identically).
//! 2. Drive `writers` concurrent appenders through one [`Committer`] in a
//!    randomized [`Durability`] mode on `SimRuntime`, recording every
//!    `Acked{first,last}` position.
//! 3. Inject a crash via [`CrashFs`] at a randomized point — critically
//!    including **inside the committer's write-then-barrier window**
//!    (`FireOnFsync`: a batch's bytes are `pwrite`-durable in the page cache
//!    but its covering `fdatasync` never returns, so it is never acked). Then
//!    materialize the surviving on-disk image with a randomized `TailPlan`
//!    (torn tail + scramble) and the mode-appropriate crash class (a
//!    page-cache-preserving process crash vs. a power crash).
//! 4. Recover with the production scanner and assert the four invariants the
//!    spike established:
//!      - **acked ⟹ recovered**, *per the [`Durability`] contract*
//!        (`03-durability.md` §1): `Os`/`Group` acked data survives any crash;
//!        `Process` acked data survives a process crash but MAY be lost to a
//!        power crash (§1.1);
//!      - **no partial batch is ever visible** (the scanner accepts only
//!        complete, CRC-valid, contiguous batches);
//!      - **contiguous positions after resume** (a fresh production writer
//!        seeded at `next_pos` continues the global-position sequence);
//!      - **idempotent re-recovery** (same image ⟹ identical `Recovery`;
//!        truncating to `safe_offset` ⟹ same batches, clean end).
//!
//! # Profiles
//!
//! - `randomized_crash_recovery_loop` — the fast profile (~1.5k cases), under
//!   60 s in a debug `cargo test`, on every CI run.
//! - `randomized_crash_recovery_loop_full` — the 12k+ profile, `#[ignore]`d by
//!   default and run by the nightly `crash-harness` workflow.
//! - `reproduces_a_fixed_seed` — a single-seed smoke case that also runs under
//!   Miri (the loops are Miri-ignored: thousands of iterations are far too slow
//!   for the isolation interpreter, and `SimFs` is already covered there by the
//!   writer suite).

use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use mess_log::committer::{
    AppendError, AppendOutcome, AppendRequest, Committer, Durability,
    EventInput,
};
use mess_log::encode::Subframe;
use mess_log::format::{SEGMENT_HEADER_LEN, SEGMENT_SIZE};
use mess_log::runtime::{
    CrashPlan, Fault, FileHandle, Fs, OpenOpts, Rng, Runtime, SimFs,
    SimRuntime, TailPlan,
};
use mess_log::scanner::{Recovery, recover_segment};
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter};

// ===========================================================================
// CrashFs — the failpoint fs wrapper (test-only; production types untouched)
// ===========================================================================
//
// Wraps the runtime `SimFs` and, at a seeded fire point, models the process
// dying: from the fire onward every `fdatasync` fails (no durability) and
// every `pwrite` makes no progress (`Ok(0)`, so the writer surfaces a
// short-write fault and the committer downgrades the batch to
// `Indeterminate`). The two triggers exercise distinct crash geometries:
//
//   * `FireOnFsync(k)` — the k-th `fdatasync` returns `Err`. The batch(es)
//     whose `pwrite`s completed just before it are on disk but their barrier
//     never returned: the write-then-barrier window the bone targets. Never
//     acked → recovery MAY surface them (A6) but need not.
//   * `FireOnPwrite(k)` — the k-th `pwrite` makes no progress, modelling the
//     process dying before a batch's bytes reached the device. That batch
//     contributes zero bytes (clean boundary; the medium's `TailPlan` is what
//     introduces torn/partial trailing bytes).
//
// k counts from 1 and skips the segment header's own create-time
// `pwrite`/`fdatasync` (both are op #1), so k >= 2 always leaves a valid,
// durable `SegmentHeader`.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Trigger {
    /// The k-th `fdatasync` fires (write-then-barrier window).
    FireOnFsync(u64),
    /// The k-th `pwrite` fires (crash before a batch's bytes land).
    FireOnPwrite(u64),
    /// Never fires — a clean run (recovery must return everything).
    None,
}

struct CrashCtl {
    trigger: Trigger,
    fsyncs:  Mutex<u64>,
    pwrites: Mutex<u64>,
    fired:   Mutex<bool>,
}

impl CrashCtl {
    fn new(trigger: Trigger) -> Self {
        CrashCtl {
            trigger,
            fsyncs: Mutex::new(0),
            pwrites: Mutex::new(0),
            fired: Mutex::new(false),
        }
    }

    fn is_fired(&self) -> bool { *self.fired.lock().unwrap() }

    fn fire(&self) { *self.fired.lock().unwrap() = true; }

    /// Returns `true` if this `pwrite` must make no progress (fired or firing).
    fn on_pwrite(&self) -> bool {
        if self.is_fired() {
            return true;
        }
        let mut n = self.pwrites.lock().unwrap();
        *n += 1;
        if let Trigger::FireOnPwrite(k) = self.trigger
            && *n == k
        {
            drop(n);
            self.fire();
            return true;
        }
        false
    }

    /// Returns `true` if this `fdatasync` must fail (fired or firing).
    fn on_fdatasync(&self) -> bool {
        if self.is_fired() {
            return true;
        }
        let mut n = self.fsyncs.lock().unwrap();
        *n += 1;
        if let Trigger::FireOnFsync(k) = self.trigger
            && *n == k
        {
            drop(n);
            self.fire();
            return true;
        }
        false
    }
}

#[derive(Clone)]
struct CrashFs {
    inner: SimFs,
    ctl:   Arc<CrashCtl>,
}

#[derive(Clone)]
struct CrashFile {
    inner: <SimFs as Fs>::File,
    ctl:   Arc<CrashCtl>,
}

impl Fs for CrashFs {
    type File = CrashFile;

    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<CrashFile> {
        Ok(CrashFile {
            inner: self.inner.open(path, opts)?,
            ctl:   self.ctl.clone(),
        })
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.inner.rename(from, to)
    }
}

impl FileHandle for CrashFile {
    fn pwrite(&self, off: u64, buf: &[u8]) -> io::Result<usize> {
        if self.ctl.on_pwrite() {
            // Process dead: the write never reached the device.
            return Ok(0);
        }
        self.inner.pwrite(off, buf)
    }

    fn pread(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        // Reads are unaffected by the crash trigger; recovery reads the
        // durable image through the base `SimFs` directly anyway.
        self.inner.pread(off, buf)
    }

    fn fdatasync(&self) -> io::Result<()> {
        if self.ctl.on_fdatasync() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "crashed barrier",
            ));
        }
        self.inner.fdatasync()
    }

    fn len(&self) -> io::Result<u64> { self.inner.len() }
}

// ===========================================================================
// Workload + crash-plan derivation (all from one seed)
// ===========================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CrashClass {
    /// Process crash / panic / kill -9: the OS keeps running, so the page
    /// cache (everything `pwrite`-accepted) survives (§1.1). Modeled as
    /// `keep = len`.
    Process,
    /// Power loss / OS crash: the un-`fdatasync`ed tail is torn at an
    /// arbitrary point and may be scrambled. Only the barrier watermark is
    /// guaranteed.
    Power,
}

/// A batch the harness will submit, plus its bookkeeping.
#[derive(Clone)]
struct PlannedBatch {
    stream_id:            u64,
    first_stream_version: u64,
    events:               Vec<Vec<u8>>,
}

struct CasePlan {
    durability:     Durability,
    writers:        u64,
    base_pos:       u64,
    epoch:          u64,
    trigger:        Trigger,
    class:          CrashClass,
    keep_frac:      f64,
    scramble_fracs: Vec<f64>,
    /// `plans[w]` is writer w's sequence of batches.
    plans:          Vec<Vec<PlannedBatch>>,
    /// Resume workload: batches appended to a fresh segment seeded at
    /// `next_pos` after recovery.
    resume_events:  Vec<Vec<Vec<u8>>>,
}

fn plan_case(seed: u64) -> CasePlan {
    let mut rng = Rng::new(seed);

    let durability = match rng.below(3) {
        0 => Durability::Process,
        1 => Durability::Os,
        _ => Durability::group_default(),
    };
    let writers = 1 + rng.below(4); // 1..=4
    let batches_per = 1 + rng.below(4); // 1..=4 per writer

    // Build each writer's batch sequence (distinct stream per writer so
    // stream versions never collide across the concurrent writers).
    let mut plans: Vec<Vec<PlannedBatch>> = Vec::new();
    for w in 0..writers {
        let mut version = 0u64;
        let mut seq = Vec::new();
        for _ in 0..batches_per {
            let n_events = 1 + rng.below(4); // 1..=4 events
            let events: Vec<Vec<u8>> = (0..n_events)
                .map(|_| {
                    let len = rng.below(65); // 0..=64 bytes
                    (0..len).map(|_| (rng.below(256)) as u8).collect()
                })
                .collect();
            seq.push(PlannedBatch {
                stream_id: w,
                first_stream_version: version,
                events,
            });
            version += n_events;
        }
        plans.push(seq);
    }

    // Trigger: bias toward actually firing (~78%), with a small k so it
    // lands within the run rather than past its last barrier. k >= 2 always
    // spares the segment header.
    let trigger = match rng.below(9) {
        0 | 1 => Trigger::None,
        2..=4 => Trigger::FireOnFsync(2 + rng.below(6)),
        _ => Trigger::FireOnPwrite(2 + rng.below(8)),
    };

    let class =
        if rng.bool() { CrashClass::Process } else { CrashClass::Power };
    let keep_frac = (rng.below(1_000_001) as f64) / 1_000_000.0;
    let n_scramble = rng.below(4); // 0..=3
    let scramble_fracs = (0..n_scramble)
        .map(|_| (rng.below(1_000_001) as f64) / 1_000_000.0)
        .collect();

    let base_pos = rng.below(1_000);
    let epoch = 1 + rng.below(1_000);

    let n_resume = rng.below(4); // 0..=3 resume batches
    let resume_events: Vec<Vec<Vec<u8>>> = (0..n_resume)
        .map(|_| {
            let n_events = 1 + rng.below(3);
            (0..n_events)
                .map(|_| {
                    let len = rng.below(33);
                    (0..len).map(|_| (rng.below(256)) as u8).collect()
                })
                .collect()
        })
        .collect();

    CasePlan {
        durability,
        writers,
        base_pos,
        epoch,
        trigger,
        class,
        keep_frac,
        scramble_fracs,
        plans,
        resume_events,
    }
}

fn to_request(pb: &PlannedBatch) -> AppendRequest {
    AppendRequest {
        stream_id:            pb.stream_id,
        category_id:          100 + pb.stream_id,
        first_stream_version: pb.first_stream_version,
        events:               pb
            .events
            .iter()
            .map(|p| EventInput::plain(1, 1, 0, p.clone()))
            .collect(),
    }
}

// ===========================================================================
// One case
// ===========================================================================

#[derive(Debug, Default, Clone, Copy)]
struct CaseStats {
    fired:            bool,
    acked_batches:    u64,
    surfaced_unacked: bool,
}

fn run_case(seed: u64) -> CaseStats {
    let plan = plan_case(seed);
    let path = Path::new("/seg");

    let rt = SimRuntime::with_fault(seed, Fault::Tail);
    let base_fs = rt.fs();
    let ctl = Arc::new(CrashCtl::new(plan.trigger));
    let cfs = CrashFs { inner: base_fs.clone(), ctl: ctl.clone() };

    let params = SegmentParams {
        segment_id:         0,
        base_pos:           plan.base_pos,
        epoch:              plan.epoch,
        prev_segment_epoch: 0,
        created_unix_nanos: 0,
        segment_size:       SEGMENT_SIZE,
    };
    // The header's create-time pwrite+fdatasync are op #1 and always succeed
    // (triggers use k >= 2), so the SegmentHeader is durable in every case.
    let writer = SegmentWriter::create(&cfs, path, params)
        .unwrap_or_else(|e| panic!("seed {seed}: segment create failed: {e}"));

    let total_submitted_events: u64 = plan
        .plans
        .iter()
        .flat_map(|s| s.iter())
        .map(|b| b.events.len() as u64)
        .sum();

    // Drive the real committer; collect every Acked position range.
    let acked: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    rt.block_on(async {
        let c = Committer::spawn(&rt, writer, plan.durability);
        let mut joins = Vec::new();
        for w in 0..plan.writers as usize {
            let ap = c.appender();
            let batches = plan.plans[w].clone();
            let acked = acked.clone();
            joins.push(rt.spawn(async move {
                for pb in &batches {
                    match ap.append(to_request(pb)).await {
                        Ok(AppendOutcome::Acked {
                            first_position,
                            last_position,
                        }) => {
                            acked
                                .lock()
                                .unwrap()
                                .push((first_position, last_position));
                        }
                        Ok(AppendOutcome::Indeterminate) => {}
                        // D8 (bn-25e): after a barrier fault poisons the store,
                        // every later append fails fast with StorePoisoned —
                        // exactly this harness's "once one barrier fails every
                        // later group fails too" model, now surfaced as a typed
                        // error instead of Indeterminate. Both are non-acks, so
                        // the acked-prefix invariant below is unchanged.
                        Err(AppendError::StorePoisoned) => {}
                        Err(e) => panic!(
                            "seed {seed}: valid batch rejected pre-flight: {e}"
                        ),
                    }
                }
            }));
        }
        for j in joins {
            j.await;
        }
        c.shutdown().await;
    });

    let fired = ctl.is_fired();
    let mut acked = Arc::try_unwrap(acked).unwrap().into_inner().unwrap();
    acked.sort_unstable();

    // The acked positions MUST form a contiguous prefix [base_pos, N): the
    // committer assigns positions centrally in commit order, a group is
    // acked all-or-nothing, and once one barrier fails (the fire) every later
    // group fails too — so acked batches are exactly a prefix of the log.
    let mut expect = plan.base_pos;
    for &(first, last) in &acked {
        assert_eq!(
            first, expect,
            "seed {seed}: acked positions not a dense prefix (gap/overlap at \
             {first}, expected {expect})"
        );
        assert!(
            last >= first,
            "seed {seed}: inverted ack range {first}..={last}"
        );
        expect = last + 1;
    }
    let acked_end = expect; // first_global_pos just past the last acked event
    let acked_batches = acked.len() as u64;

    // ---- crash: materialize the surviving on-disk image ------------------
    let len = {
        let f = base_fs.open(path, OpenOpts::read_only()).unwrap();
        f.len().unwrap()
    };
    let (keep, scramble) = match plan.class {
        // Process crash: the page cache (everything pwritten) survives.
        CrashClass::Process => (len, Vec::new()),
        // Power crash: tear the un-synced tail at a seeded point and scramble
        // a few of the surviving-but-unsynced bytes. `TailDisk::crash` clamps
        // `keep` up to the fdatasync watermark, so the durable prefix is
        // always retained; scramble only bites the unsynced region.
        CrashClass::Power => {
            let keep = (plan.keep_frac * (len as f64 + 1.0)) as u64;
            let keep = keep.min(len);
            let scramble: Vec<usize> = plan
                .scramble_fracs
                .iter()
                .map(|f| {
                    ((f * len as f64) as usize)
                        .min(len.saturating_sub(1) as usize)
                })
                .collect();
            (keep, scramble)
        }
    };
    base_fs
        .crash(
            path,
            CrashPlan::Tail(TailPlan { keep: keep as usize, scramble }),
        )
        .unwrap_or_else(|e| panic!("seed {seed}: crash failed: {e}"));

    // ---- recover with the PRODUCTION scanner -----------------------------
    let rec = recover_segment(&base_fs, path)
        .unwrap_or_else(|e| panic!("seed {seed}: recover failed: {e}"));

    assert_recovery_wellformed(
        seed,
        &rec,
        plan.base_pos,
        plan.epoch,
        total_submitted_events,
    );

    // Invariant: acked ⟹ recovered, per the Durability contract (§1).
    // Os/Group acks are barrier-backed and survive any crash; Process acks
    // survive a process crash (page cache intact) but MAY be lost to a power
    // crash (§1.1). Whenever the guarantee holds, every acked event MUST be
    // inside the recovered prefix.
    let acked_guaranteed = match plan.durability {
        Durability::Os | Durability::Group { .. } => true,
        Durability::Process => plan.class == CrashClass::Process,
    };
    if acked_guaranteed && acked_batches > 0 {
        assert!(
            rec.next_pos >= acked_end,
            "seed {seed}: LOST ACKED DATA: recovered next_pos={} < \
             acked_end={} (mode={:?}, class={:?}, trigger={:?})",
            rec.next_pos,
            acked_end,
            plan.durability,
            plan.class,
            plan.trigger,
        );
    }

    // Did recovery surface an unacked-but-complete batch (A6)? Legal, and
    // only ever a contiguous continuation past the acked prefix.
    let surfaced_unacked = rec.next_pos > acked_end;

    // Invariant: idempotent re-recovery (same durable image ⟹ identical).
    let rec2 = recover_segment(&base_fs, path)
        .unwrap_or_else(|e| panic!("seed {seed}: re-recover failed: {e}"));
    assert_eq!(rec, rec2, "seed {seed}: re-recovery not idempotent");

    // Invariant: truncating to safe_offset ⟹ same batches, clean end. Seed a
    // fresh sim file with exactly the accepted-prefix bytes and re-scan it.
    let image = read_image(&base_fs, path);
    let truncated = image[..rec.safe_offset as usize].to_vec();
    let tfs = SimFs::new(Fault::Tail);
    let tpath = Path::new("/truncated");
    tfs.seed(tpath, Fault::Tail, truncated);
    let rec3 = recover_segment(&tfs, tpath).unwrap_or_else(|e| {
        panic!("seed {seed}: truncated recover failed: {e}")
    });
    assert_eq!(
        rec3.accepted, rec.accepted,
        "seed {seed}: truncated recovery differs"
    );
    assert_eq!(
        rec3.next_pos, rec.next_pos,
        "seed {seed}: truncated next_pos differs"
    );
    assert_eq!(
        rec3.safe_offset, rec.safe_offset,
        "seed {seed}: truncated safe_offset differs"
    );
    assert_eq!(
        rec3.stop,
        mess_log::scanner::ScanStop::EndOfSegment,
        "seed {seed}: a truncated-to-safe-offset image must scan to a clean \
         end"
    );

    // Invariant: contiguous positions after resume. A fresh production writer
    // seeded at the recovered `next_pos` (and a strictly larger epoch, A9)
    // continues the global-position sequence with no gap.
    assert_resume_contiguous(
        seed,
        plan.base_pos,
        &rec,
        plan.epoch,
        &plan.resume_events,
    );

    CaseStats { fired, acked_batches, surfaced_unacked }
}

/// Structural invariants that hold for EVERY recovery: a valid header seeded
/// from `base_pos`, a densely contiguous accepted prefix (no partial/torn
/// batch ever accepted), `safe_offset` exactly past the accepted bytes, and no
/// event invented that was never submitted.
fn assert_recovery_wellformed(
    seed: u64,
    rec: &Recovery,
    base_pos: u64,
    epoch: u64,
    total_submitted_events: u64,
) {
    let header = rec.header.unwrap_or_else(|| {
        panic!("seed {seed}: durable SegmentHeader must always survive")
    });
    assert_eq!(header.base_pos, base_pos, "seed {seed}: header base_pos");
    assert_eq!(header.epoch, epoch, "seed {seed}: header epoch");

    let mut expect_pos = base_pos;
    let mut off = SEGMENT_HEADER_LEN as u64;
    for b in &rec.accepted {
        assert_eq!(
            b.first_global_pos, expect_pos,
            "seed {seed}: accepted batch not position-contiguous"
        );
        assert_eq!(
            b.offset, off,
            "seed {seed}: accepted batch not byte-contiguous"
        );
        assert_eq!(
            b.segment_epoch, epoch,
            "seed {seed}: accepted batch wrong epoch"
        );
        assert!(b.frame_count >= 1, "seed {seed}: empty batch accepted (A5)");
        expect_pos += u64::from(b.frame_count);
        off += b.total_len;
    }
    assert_eq!(
        rec.next_pos, expect_pos,
        "seed {seed}: next_pos != contiguous end"
    );
    assert_eq!(
        rec.safe_offset, off,
        "seed {seed}: safe_offset != end of accepted bytes"
    );
    assert_eq!(
        rec.next_batch_id,
        rec.accepted.len() as u64,
        "seed {seed}: next_batch_id must equal the accepted batch count"
    );
    // Recovery can never invent events that were never written.
    assert!(
        rec.next_pos <= base_pos + total_submitted_events,
        "seed {seed}: recovered {} events past base but only {} were ever \
         submitted",
        rec.next_pos - base_pos,
        total_submitted_events,
    );
}

/// After recovery, a fresh production `SegmentWriter` seeded at `rec.next_pos`
/// (epoch bumped for A9) must continue the global-position sequence with no
/// gap: its batches start exactly at `next_pos` and tile densely.
fn assert_resume_contiguous(
    seed: u64,
    _base_pos: u64,
    rec: &Recovery,
    epoch: u64,
    resume_events: &[Vec<Vec<u8>>],
) {
    let rfs = SimFs::new(Fault::Tail);
    let rpath = Path::new("/resume-seg");
    let mut w = SegmentWriter::create(
        &rfs,
        rpath,
        SegmentParams {
            segment_id:         1,
            base_pos:           rec.next_pos,
            epoch:              epoch + 1,
            prev_segment_epoch: epoch,
            created_unix_nanos: 0,
            segment_size:       SEGMENT_SIZE,
        },
    )
    .unwrap_or_else(|e| {
        panic!("seed {seed}: resume segment create failed: {e}")
    });

    let mut resumed_events = 0u64;
    for evs in resume_events {
        let subs: Vec<Subframe> =
            evs.iter().map(|p| Subframe::plain(1, 1, 0, p)).collect();
        let spec = BatchSpec {
            stream_id:            7,
            category_id:          7,
            first_stream_version: resumed_events,
            crypto_chain:         None,
            subframes:            &subs,
        };
        w.append(&spec).unwrap_or_else(|e| {
            panic!("seed {seed}: resume append failed: {e}")
        });
        resumed_events += evs.len() as u64;
    }
    w.close()
        .unwrap_or_else(|e| panic!("seed {seed}: resume close failed: {e}"));

    let rrec = recover_segment(&rfs, rpath)
        .unwrap_or_else(|e| panic!("seed {seed}: resume recover failed: {e}"));
    assert_eq!(
        rrec.stop,
        mess_log::scanner::ScanStop::EndOfSegment,
        "seed {seed}: cleanly-written resume segment must scan clean"
    );
    if let Some(first) = rrec.accepted.first() {
        assert_eq!(
            first.first_global_pos, rec.next_pos,
            "seed {seed}: resume did not continue the global position sequence"
        );
    }
    assert_eq!(
        rrec.next_pos,
        rec.next_pos + resumed_events,
        "seed {seed}: resume positions not contiguous with the recovered log"
    );
}

fn read_image(fs: &SimFs, path: &Path) -> Vec<u8> {
    let f = fs.open(path, OpenOpts::read_only()).unwrap();
    let len = f.len().unwrap() as usize;
    let mut buf = vec![0u8; len];
    let mut filled = 0;
    while filled < len {
        let n = f.pread(filled as u64, &mut buf[filled..]).unwrap();
        if n == 0 {
            break;
        }
        filled += n;
    }
    buf.truncate(filled);
    buf
}

// ===========================================================================
// Profiles
// ===========================================================================

fn run_loop(count: u64) {
    let mut fired = 0u64;
    let mut acked_total = 0u64;
    let mut surfaced = 0u64;
    for seed in 0..count {
        let s = run_case(seed);
        if s.fired {
            fired += 1;
        }
        acked_total += s.acked_batches;
        if s.surfaced_unacked {
            surfaced += 1;
        }
    }
    println!(
        "crash harness: {count} cases, {fired} with an injected crash, \
         {acked_total} acked batches verified recovered (per mode contract), \
         {surfaced} cases surfaced an unacked-but-complete batch (A6, \
         allowed), 0 partial batches visible, 0 acked losses against contract"
    );
}

/// Fast profile: ~1.5k cases, well under 60 s in a debug build. Runs on every
/// CI invocation.
#[test]
#[cfg_attr(miri, ignore = "thousands of iterations are too slow under Miri")]
fn randomized_crash_recovery_loop() { run_loop(1_500); }

/// Full profile: 12k+ cases. `#[ignore]`d by default (kept out of the 60 s
/// gate); the nightly `crash-harness` workflow runs it with `--ignored`.
#[test]
#[ignore = "full 12k-case profile: run via `cargo test -- --ignored` (nightly \
            CI)"]
fn randomized_crash_recovery_loop_full() { run_loop(12_000); }

/// A single fixed seed, cheap enough to also run under Miri: proves a seed is
/// self-contained and reproducible, and gives the Miri lane one real pass over
/// the whole crash/recover path.
#[test]
fn reproduces_a_fixed_seed() {
    let a = run_case(0xC0FFEE);
    let b = run_case(0xC0FFEE);
    assert_eq!(
        (a.fired, a.acked_batches, a.surfaced_unacked),
        (b.fired, b.acked_batches, b.surfaced_unacked),
        "a fixed seed must reproduce byte-identically"
    );
}
