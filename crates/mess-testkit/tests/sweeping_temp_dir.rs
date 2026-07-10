//! bn-cxr acceptance tests for `mess_testkit::tempdir`: exercised against an
//! isolated fake namespace root (a `tempfile::tempdir()` scratch dir with
//! its own `mess-tests` leaf) so these tests never touch the real
//! process-wide namespace (`namespace_root()`/the `Once`-guarded auto-sweep
//! in [`mess_testkit::sweeping_temp_dir`]) or race other tests/processes
//! sharing it.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use mess_testkit::{AUTO_SWEEP_MAX_AGE, sweep_stale, temp_dir_in};

/// A fake `<base>/mess-tests` root, isolated per test.
fn fake_root() -> (tempfile::TempDir, PathBuf) {
    let base = tempfile::tempdir().expect("scratch base");
    let root = base.path().join("mess-tests");
    fs::create_dir_all(&root).expect("create fake mess-tests root");
    (base, root)
}

/// Deterministically dead: exceeds any realistic Linux `pid_max` (default
/// configs top out well under 4_194_304). The same "fake stale pid"
/// convention `mess-log`'s `lock.rs` tests already use.
const DEAD_PID: u32 = 999_999_999;

fn backdate(path: &Path, age: Duration) {
    let old = SystemTime::now().checked_sub(age).expect("age fits before now");
    let f = fs::File::open(path).expect("open dir to edit mtime");
    f.set_modified(old).expect("set_modified");
}

/// The core acceptance criterion from the bone: a sweep over dirs shaped
/// like ours removes exactly the old-and-dead one, and leaves both a fresh
/// dead-pid dir and an old live-pid dir untouched.
#[test]
fn sweep_removes_old_dead_keeps_fresh_and_live_pid() {
    let (_base, root) = fake_root();

    let old_dead = root.join(format!("job-{DEAD_PID}-aaaa1111"));
    fs::create_dir_all(&old_dead).unwrap();
    backdate(&old_dead, AUTO_SWEEP_MAX_AGE + Duration::from_secs(3600));

    let fresh_dead = root.join(format!("job-{DEAD_PID}-bbbb2222"));
    fs::create_dir_all(&fresh_dead).unwrap();
    // Left at "just created" mtime: too young to sweep even though its pid
    // is dead.

    let live_pid = std::process::id();
    let old_live = root.join(format!("job-{live_pid}-cccc3333"));
    fs::create_dir_all(&old_live).unwrap();
    backdate(&old_live, AUTO_SWEEP_MAX_AGE + Duration::from_secs(3600));

    sweep_stale(&root, AUTO_SWEEP_MAX_AGE);

    assert!(!old_dead.exists(), "old dir with a dead pid must be swept");
    assert!(fresh_dead.exists(), "fresh dir must survive despite dead pid");
    assert!(
        old_live.exists(),
        "a live pid's dir must survive regardless of age"
    );
}

/// Directories that don't match the `<name>-<pid>-<nonce>` convention are
/// never auto-swept, no matter how old — the sweep only ever touches
/// entries it can positively identify as its own.
#[test]
fn sweep_leaves_unrecognized_entries_alone() {
    let (_base, root) = fake_root();

    let not_ours = root.join("some-other-tool-left-this-here");
    fs::create_dir_all(&not_ours).unwrap();
    backdate(&not_ours, AUTO_SWEEP_MAX_AGE + Duration::from_secs(3600));

    let plain_file = root.join(format!("filejob-{DEAD_PID}-dddd4444"));
    fs::write(&plain_file, b"not a directory").unwrap();
    backdate(&plain_file, AUTO_SWEEP_MAX_AGE + Duration::from_secs(3600));

    sweep_stale(&root, AUTO_SWEEP_MAX_AGE);

    assert!(not_ours.exists(), "non-matching name must never be swept");
    assert!(plain_file.exists(), "a plain file must never be swept");
}

/// Defensive namespacing: a sweep pointed at a root that is NOT literally
/// named `mess-tests` (or is empty, or resolves to `/`) is a hard no-op —
/// even when it contains entries that would otherwise match the sweep
/// convention exactly.
#[test]
fn sweep_refuses_to_operate_outside_the_mess_tests_namespace() {
    let base = tempfile::tempdir().expect("scratch base");
    let wrong_root = base.path().join("not-mess-tests");
    fs::create_dir_all(&wrong_root).unwrap();

    let looks_sweepable = wrong_root.join(format!("job-{DEAD_PID}-eeee5555"));
    fs::create_dir_all(&looks_sweepable).unwrap();
    backdate(&looks_sweepable, AUTO_SWEEP_MAX_AGE + Duration::from_secs(3600));

    sweep_stale(&wrong_root, AUTO_SWEEP_MAX_AGE);
    assert!(
        looks_sweepable.exists(),
        "a root whose leaf isn't `mess-tests` must never be swept"
    );

    sweep_stale(Path::new("/"), AUTO_SWEEP_MAX_AGE);
    sweep_stale(Path::new(""), AUTO_SWEEP_MAX_AGE);
    // No panic, no observable effect — nothing to assert beyond "didn't
    // blow up", which the test reaching this point already proves.
}

/// A symlink planted inside the namespace — even one shaped like a
/// sweepable entry and pointing at something old-and-dead-looking outside
/// the namespace — must never be followed. The sweep must not delete
/// whatever it points at, and (being symlink-aware) leaves the symlink
/// itself alone too.
#[test]
fn sweep_does_not_follow_symlinks_out_of_the_namespace() {
    let (_base, root) = fake_root();

    let outside = tempfile::tempdir().expect("outside scratch dir");
    let sentinel = outside.path().join("sentinel.txt");
    fs::write(&sentinel, b"do not delete me").unwrap();

    let link = root.join(format!("linkjob-{DEAD_PID}-ffff6666"));
    #[cfg(unix)]
    std::os::unix::fs::symlink(outside.path(), &link)
        .expect("create symlink into the namespace");

    sweep_stale(&root, AUTO_SWEEP_MAX_AGE);

    assert!(
        sentinel.exists(),
        "sweep must never follow a symlink out of the namespace"
    );
    assert!(
        std::fs::symlink_metadata(&link).is_ok(),
        "the symlink entry itself must be left alone, not removed"
    );
}

/// The bone's SIGKILL-suite acceptance criterion: a dir left behind by a
/// "first run" whose process has since died is removed by a "second run"'s
/// init sweep. Simulated (per the bone's own suggestion — "assert via the
/// helper's own sweep logic") by backdating the leaked dir's mtime rather
/// than literally waiting 24h+ between two real suite invocations: the
/// leaked dir is created with exactly the shape `sweeping_temp_dir` would
/// produce, and the same `sweep_stale` call the real helper's init guard
/// makes is what removes it.
#[test]
fn second_run_sweeps_first_runs_leaked_dir() {
    let (_base, root) = fake_root();

    // "Run 1": a crash/SIGKILL-harness round that got killed mid-flight —
    // its dir was never cleaned up by `Drop` because the process never got
    // to run it. `DEAD_PID` stands in for "that process has since exited".
    let leaked = temp_dir_in(&root, "sigkill-os");
    let leaked_path = leaked.path().to_path_buf();
    // Rename it to encode a pid we can positively prove dead (the real
    // dir's own pid is *this* test process's pid, which is of course
    // alive) and backdate it past the auto-sweep threshold, standing in
    // for "left over from a run more than 24h ago".
    let stand_in = root.join(format!("sigkill-os-{DEAD_PID}-deadbeef"));
    fs::rename(&leaked_path, &stand_in).unwrap();
    backdate(&stand_in, AUTO_SWEEP_MAX_AGE + Duration::from_secs(3600));
    // The guard's own `Drop` would otherwise try to remove the
    // now-renamed-away path; disarm it since `stand_in` is what we're
    // actually tracking from here.
    std::mem::forget(leaked);

    assert!(stand_in.exists(), "sanity: the leaked dir exists before sweeping");

    // "Run 2": the same init sweep `sweeping_temp_dir` performs once per
    // process, run explicitly here against the fake root.
    sweep_stale(&root, AUTO_SWEEP_MAX_AGE);

    assert!(
        !stand_in.exists(),
        "run 2's init sweep must remove run 1's leaked dir"
    );

    // And a fresh dir created by "run 2" itself survives its own sweep.
    let run2_dir = temp_dir_in(&root, "sigkill-os");
    assert!(run2_dir.path().exists());
}

/// `SweepingTempDir::drop` removes its own directory (the common,
/// non-killed path — most test runs exit normally and this is what keeps
/// the namespace from growing unboundedly in the first place).
#[test]
fn drop_removes_the_directory() {
    let (_base, root) = fake_root();
    let path = {
        let dir = temp_dir_in(&root, "drop-check");
        let p = dir.path().to_path_buf();
        assert!(p.exists());
        p
        // `dir` drops here.
    };
    assert!(!path.exists(), "SweepingTempDir must remove its dir on drop");
}
