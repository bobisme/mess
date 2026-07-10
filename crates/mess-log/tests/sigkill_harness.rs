//! bn-2iq: SIGKILL child-writer harness.
//!
//! Real-process kills catch kernel-buffering surprises a simulated fault
//! layer cannot (`spikes/recovery_scale`'s and `spikes/perf_group_commit`'s
//! `crash`/SIGKILL phases are the precedent this harness generalizes into
//! `mess-log`'s own test suite, over the production [`Committer`] +
//! [`recover_segment`] rather than spike-local reimplementations).
//!
//! # Design
//!
//! The parent spawns `sigkill_child` (a small bin target, `src/bin/`) as a
//! real OS process, writing under a chosen [`Durability`] mode to a real
//! temp dir on a real device — `$HOME/.cache`, not a `tmpfs`-backed
//! `/tmp`, matching `committer.rs`'s own real-fs perf tests, because
//! `Os`/`Group`'s barrier means nothing on a filesystem where `fdatasync`
//! is a no-op. The child reports each ack as one line on stdout
//! (`"<first_pos> <last_pos>\n"`) **strictly after** the committer's
//! `append().await` resolves `Acked` — so the "ack ledger" this harness
//! reconciles is defined as *what the parent's reader thread actually
//! read off the pipe before the kill*, not what the child believed it
//! sent. A line that write(2) never completed cannot be in that ledger by
//! construction (see the child's doc comment on write atomicity); a line
//! it did complete survives the SIGKILL because it already left the
//! child's address space into the pipe's kernel buffer.
//!
//! The parent then SIGKILLs at a randomized delay, runs the production
//! [`recover_segment`] scanner, and reconciles the ledger against the
//! mode's contract (`docs/spec/03-durability.md` §1):
//!
//! - **`Os` / `Group`**: acked implies recovered. Every acked batch's
//!   `last_position` MUST be `< recovery.next_pos` (the recovered watermark) —
//!   the ack means the covering barrier returned and the durable watermark
//!   advanced past the batch (§1.2/§1.3); recovery MUST reconstruct at least
//!   that prefix.
//! - **`Process`**: no such guarantee (§1.1 — a bare page-cache write, no
//!   barrier). The harness asserts *only* prefix consistency: the recovered
//!   batches tile `[base_pos, next_pos)` with no gap or overlap, which is what
//!   "the log is a well-formed prefix" means independent of what happened to be
//!   acked. This holds for every mode, and is asserted for all three below, not
//!   only `Process`.
//!
//! 3 rounds/mode run unconditionally (`cargo test -p mess-log`) — see the
//! per-round budget in [`run_round`]; totals a few seconds. The 15-round
//! nightly profile is `#[ignore]`d and wired into
//! `.github/workflows/sigkill.yml`'s nightly schedule (matching the
//! `miri`/`sanitizers` lanes' split between per-PR and nightly).
//!
//! # Zero-ack rounds are legitimate, not a harness failure
//!
//! `Os`/`Group` acks are barrier-gated: the first ack cannot land until a
//! real `fdatasync` (plus process-spawn/exec overhead) has completed. When
//! a round's randomized kill delay lands near the low end of its window,
//! SIGKILL can fire before the child has produced a single ack. That is
//! **not** a contract violation — 0 acks trivially satisfies "acked
//! implies recovered" (Os/Group) and "no such guarantee" (Process) alike —
//! so this harness does not treat it as a failure. What it *does* still
//! check is that the child was actually alive to be killed: [`run_round`]
//! inspects the child's exit status and hard-fails only when the process
//! exited on its own (crash/panic/early-return) rather than dying to our
//! `SIGKILL`, which is the "child never got going" case worth catching.
//! See [`ChildDeath`].

use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::scanner::recover_segment;

/// Removes the round's segment file on drop, including on panic (the
/// prior explicit `fs::remove_file` at the end of [`run_round`] was only
/// reached on the non-panicking path, so a failed/flaky round would leak
/// its scratch file under `$HOME/.cache/mess-sigkill-scratch` forever).
struct CleanupGuard<'a>(&'a Path);

impl Drop for CleanupGuard<'_> {
    fn drop(&mut self) { let _ = fs::remove_file(self.0); }
}

/// How the child process ended, established from its
/// [`std::process::ExitStatus`].
enum ChildDeath {
    /// Died to the signal we sent it — the expected outcome of every round.
    Killed,
    /// Exited on its own (crash, panic, early return) before we killed it.
    /// A 0-ack round is only a legitimate "SIGKILL beat the first barrier"
    /// outcome when the child was still alive to be killed; if it instead
    /// died on its own, that's a real bug and must not be waved through as
    /// "vacuously reconciling".
    DiedOnItsOwn { status: std::process::ExitStatus, stderr: String },
}

/// A scratch dir on a real, persistent device OUTSIDE the repo —
/// `$HOME/.cache`, not `std::env::temp_dir()` (commonly `tmpfs`, where
/// `fdatasync` is a no-op and `Os`/`Group`'s barrier contract would be
/// untestable). Mirrors `committer.rs`'s `real_tmp` helper.
fn real_tmp_dir(name: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let base = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let mut dir = base;
    dir.push(".cache");
    dir.push("mess-sigkill-scratch");
    fs::create_dir_all(&dir).unwrap();
    dir.push(format!("{}-{}-{}", std::process::id(), n, name));
    dir
}

/// splitmix64 — a tiny, dependency-free PRNG. Only used to jitter each
/// round's kill delay; no cryptographic or DST properties needed.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

fn seed_for(mode: &str, round: u64) -> u64 {
    let mut h = 0xD1B5_4A32_D192_ED03u64;
    for b in mode.bytes() {
        h = splitmix64(h ^ u64::from(b));
    }
    splitmix64(h ^ round)
}

struct RoundResult {
    acked:              usize,
    recovered_batches:  usize,
    recovered_next_pos: u64,
    kill_after:         Duration,
}

/// Run one SIGKILL round for `mode`: spawn the child, read acks off its
/// stdout pipe concurrently, kill at a randomized delay in
/// `[min_kill_ms, max_kill_ms]`, recover, and reconcile per the mode's
/// contract. Panics (via `assert!`) on any contract violation. A 0-ack
/// round is only accepted as vacuously-reconciling when the child died to
/// our `SIGKILL` (see [`ChildDeath`]) — if it exited on its own instead,
/// that is a real bug and still hard-fails the round.
fn run_round(
    mode: &str,
    seed: u64,
    min_kill_ms: u64,
    max_kill_ms: u64,
) -> RoundResult {
    let seg_path = real_tmp_dir(&format!("seg-{mode}-{seed:016x}.log"));
    fs::create_dir_all(seg_path.parent().unwrap()).unwrap();
    let _ = fs::remove_file(&seg_path);
    let _cleanup = CleanupGuard(&seg_path);

    let exe = env!("CARGO_BIN_EXE_sigkill_child");
    let mut child = Command::new(exe)
        .arg(&seg_path)
        .arg(mode)
        .arg("4") // writers
        .arg("8") // events per batch
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn sigkill_child");

    let stderr = child.stderr.take().expect("piped stderr");
    let stderr_reader = thread::spawn(move || {
        let mut buf = String::new();
        let _ = BufReader::new(stderr).read_to_string(&mut buf);
        buf
    });

    let stdout = child.stdout.take().expect("piped stdout");
    let acks: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let acks_reader = acks.clone();
    // Reads concurrently with the sleep-then-kill below, so whatever has
    // been read by the time the pipe closes (child dies -> write end
    // closes -> read returns Ok(0)) IS the ledger, by definition — not an
    // approximation of it.
    let reader = thread::spawn(move || {
        let mut r = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match r.read_line(&mut line) {
                Ok(0) => break, // EOF: child's write end closed.
                Ok(_) => {
                    let trimmed = line.trim_end_matches('\n');
                    let mut parts = trimmed.split(' ');
                    let (Some(a), Some(b), None) =
                        (parts.next(), parts.next(), parts.next())
                    else {
                        continue; // torn/partial trailing line at kill time: drop it (safe direction).
                    };
                    if let (Ok(first), Ok(last)) =
                        (a.parse::<u64>(), b.parse::<u64>())
                    {
                        acks_reader.lock().unwrap().push((first, last));
                    }
                }
                Err(_) => break,
            }
        }
    });

    let jitter =
        min_kill_ms + (splitmix64(seed) % (max_kill_ms - min_kill_ms + 1));
    let kill_after = Duration::from_millis(jitter);
    thread::sleep(kill_after);
    child.kill().expect("SIGKILL child");
    let status = child.wait().expect("wait for killed child");
    reader.join().expect("join stdout reader");
    let stderr_out = stderr_reader.join().unwrap_or_default();

    // `Child::kill` sends SIGKILL regardless of whether the process had
    // already exited on its own (a zombie PID is still a valid signal
    // target until reaped), so it cannot itself tell us which happened —
    // only the exit status can. `status.signal() == Some(SIGKILL)` is the
    // expected outcome of every round; anything else means the child died
    // (crashed/panicked/returned) before we ever got to kill it.
    const SIGKILL: i32 = 9;
    let death = if status.signal() == Some(SIGKILL) {
        ChildDeath::Killed
    } else {
        ChildDeath::DiedOnItsOwn { status, stderr: stderr_out }
    };

    let acked = acks.lock().unwrap().clone();
    match &death {
        ChildDeath::Killed => {
            // 0 acks here means SIGKILL raced (and won) the barrier-gated
            // first ack — contract-legal and vacuously-reconciling for
            // every mode (see the module doc), not a failure.
        }
        ChildDeath::DiedOnItsOwn { status, stderr } => {
            panic!(
                "{mode} seed {seed:016x}: child exited on its own before \
                 being killed (status={status:?}, kill_after={kill_after:?}, \
                 acked={acked_len}) — this is NOT a legitimate vacuous round; \
                 child stderr:\n{stderr}",
                acked_len = acked.len()
            );
        }
    }

    let fs = RealRuntime::new().fs();
    let recovery = match recover_segment(&fs, &seg_path) {
        Ok(r) => r,
        // SIGKILL can beat not just the first barrier-gated ack but even
        // `SegmentWriter::create`'s initial header write — i.e. the child
        // never got far enough to create the segment file at all. That is
        // the same class of legitimate vacuous outcome as a 0-ack round
        // (see the module doc): nothing was ever durable, so there is
        // nothing to reconcile against. Only accepted when the child died
        // to our SIGKILL and produced no acks; any other combination (e.g.
        // acks recorded but the file vanished) is a real bug.
        Err(e)
            if e.kind() == std::io::ErrorKind::NotFound
                && matches!(death, ChildDeath::Killed)
                && acked.is_empty() =>
        {
            return RoundResult {
                acked: 0,
                recovered_batches: 0,
                recovered_next_pos: 0,
                kill_after,
            };
        }
        Err(e) => {
            panic!("{mode} seed {seed:016x}: recover_segment failed: {e}")
        }
    };

    // Prefix consistency (holds for every mode, unconditionally): the
    // accepted batches tile [base_pos, next_pos) with no gap or overlap.
    let mut expected_next = recovery.header.map(|h| h.base_pos).unwrap_or(0);
    for b in &recovery.accepted {
        assert_eq!(
            b.first_global_pos, expected_next,
            "{mode} seed {seed:016x}: recovered prefix has a gap/overlap at \
             batch_id={}",
            b.batch_id
        );
        expected_next += u64::from(b.frame_count);
    }
    assert_eq!(
        expected_next, recovery.next_pos,
        "{mode} seed {seed:016x}: next_pos disagrees with the accepted tiling"
    );

    if mode != "process" {
        // Os/Group contract (03-durability.md §1.2/§1.3): acked implies
        // recovered — every acked position <= the recovered watermark.
        for &(first, last) in &acked {
            assert!(
                last < recovery.next_pos,
                "{mode} seed {seed:016x}: acked batch [{first},{last}] did \
                 NOT survive recovery (recovered watermark next_pos={}) — \
                 contract violation",
                recovery.next_pos
            );
        }
    }
    // `Process` (§1.1): deliberately no acked-implies-recovered assertion
    // here — the mode promises none. SIGKILL alone will not usually lose a
    // page-cache write (only an OS crash / power loss would; see
    // `spikes/recovery_scale`'s SIGKILL-realism note), so `acked >
    // recovered` would be a real (if rare-under-SIGKILL) possibility this
    // harness intentionally does not treat as failure.

    // Cleanup happens via `_cleanup`'s `Drop` (also runs if an assertion
    // above panicked), not an explicit call here.
    RoundResult {
        acked: acked.len(),
        recovered_batches: recovery.accepted.len(),
        recovered_next_pos: recovery.next_pos,
        kill_after,
    }
}

fn run_rounds(
    mode: &'static str,
    rounds: u64,
    min_kill_ms: u64,
    max_kill_ms: u64,
    salt: u64,
) {
    for round in 0..rounds {
        let seed = seed_for(mode, round ^ salt);
        let r = run_round(mode, seed, min_kill_ms, max_kill_ms);
        eprintln!(
            "sigkill {mode} round {round}: kill_after={:?} acked={} \
             recovered_batches={} next_pos={}",
            r.kill_after, r.acked, r.recovered_batches, r.recovered_next_pos
        );
    }
}

// -- Smoke: 3 rounds/mode, short kill delays, runs in `cargo test -p mess-log`
// --

#[test]
#[cfg_attr(miri, ignore)] // real process spawn + real fs; Miri cannot do either.
fn sigkill_smoke_process() { run_rounds("process", 3, 80, 300, 0); }

#[test]
#[cfg_attr(miri, ignore)]
fn sigkill_smoke_os() { run_rounds("os", 3, 80, 300, 1); }

#[test]
#[cfg_attr(miri, ignore)]
fn sigkill_smoke_group() { run_rounds("group", 3, 80, 300, 2); }

// -- Nightly: 15 rounds/mode, wider kill-delay window
// (.github/workflows/sigkill.yml) --

// Already unconditionally `#[ignore]`d, so no separate `#[cfg_attr(miri,
// ignore)]` is needed here (unlike the smoke tests below) — it would only
// produce an "unused attribute" warning under `cargo miri test -- --ignored`.

#[test]
#[ignore = "nightly: 15 real SIGKILL rounds, seconds each; see \
            .github/workflows/sigkill.yml"]
fn sigkill_nightly_process() { run_rounds("process", 15, 200, 2500, 100); }

#[test]
#[ignore = "nightly: 15 real SIGKILL rounds, seconds each; see \
            .github/workflows/sigkill.yml"]
fn sigkill_nightly_os() { run_rounds("os", 15, 200, 2500, 101); }

#[test]
#[ignore = "nightly: 15 real SIGKILL rounds, seconds each; see \
            .github/workflows/sigkill.yml"]
fn sigkill_nightly_group() { run_rounds("group", 15, 200, 2500, 102); }
