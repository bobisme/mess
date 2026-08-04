//! The CLI, end to end: hand-rolled argument parsing is the part of an example
//! nothing else tests, and a broken `--segment-bytes` or `--scale` override
//! would quietly produce a single-segment store.
//!
//! Each invocation runs the real `chatter` binary as a subprocess (the store
//! lock is process-exclusive, so this is also the only way to exercise the
//! commands in the order a human would).

use std::path::Path;
use std::process::Command;

use mess_testkit::sweeping_temp_dir;
use serde_json::Value;

fn chatter(dir: &Path, args: &[&str]) -> (String, String, bool) {
    let out = Command::new(env!("CARGO_BIN_EXE_chatter"))
        .args(args)
        .arg("--dir")
        .arg(dir)
        .output()
        .expect("run the chatter binary");
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        out.status.success(),
    )
}

#[tokio::test]
async fn the_cli_seeds_reads_and_proves_a_store() {
    let t = sweeping_temp_dir("chatter-cli");
    let dir = t.path().join("store");

    // --- seed, with explicit knobs overriding the --scale preset ----------
    let (out, err, ok) = chatter(
        &dir,
        &[
            "seed",
            "--scale",
            "demo",
            "--seed",
            "99",
            "--channels",
            "3",
            "--users",
            "12",
            "--messages",
            "500",
            "--reactions",
            "0.5",
            "--segment-bytes",
            "64K",
        ],
    );
    assert!(ok, "seed failed:\n{out}\n{err}");
    assert!(out.contains("500 messages"), "seed output:\n{out}");
    assert!(out.contains("250 reactions"), "seed output:\n{out}");
    // The explicit --segment-bytes must beat the demo preset's 1 MiB.
    assert!(out.contains("segment-bytes=64.0 KiB"), "seed output:\n{out}");
    let sealed = sealed_count(&out);
    assert!(
        sealed > 1,
        "the CLI must produce a multi-segment sealed store, got \
         {sealed}:\n{out}"
    );
    assert!(
        !out.contains("NOTE: fewer than two sealed segments"),
        "seed output:\n{out}"
    );

    // --- seeding again is refused (the fresh-dir guard) -------------------
    let (_out, err, ok) = chatter(&dir, &["seed", "--messages", "10"]);
    assert!(!ok, "seeding onto a non-empty dir must fail");
    assert!(err.contains("refusing to seed"), "stderr:\n{err}");

    // --- stats ------------------------------------------------------------
    let (out, err, ok) = chatter(&dir, &["stats"]);
    assert!(ok, "stats failed:\n{out}\n{err}");
    assert!(out.contains("sealed:"), "stats output:\n{out}");
    assert!(out.contains("500 messages"), "stats output:\n{out}");
    assert!(out.contains("channels (deepest first)"), "stats output:\n{out}");
    assert!(out.contains("recent timeline"), "stats output:\n{out}");

    // --- scrollback -------------------------------------------------------
    let (out, err, ok) =
        chatter(&dir, &["scrollback", "--pages", "2", "--page-size", "10"]);
    assert!(ok, "scrollback failed:\n{out}\n{err}");
    assert!(out.contains("scrolling back through #"), "scrollback:\n{out}");
    assert!(out.contains("-- page 1"), "scrollback:\n{out}");
    assert!(out.contains("-- page 2"), "scrollback:\n{out}");

    // An unknown slug is a clean refusal, not a panic.
    let (_out, err, ok) = chatter(&dir, &["scrollback", "--channel", "nope"]);
    assert!(!ok);
    assert!(err.contains("no channel with slug"), "stderr:\n{err}");

    // --- tail (bounded, so the test terminates) ---------------------------
    let (out, err, ok) = chatter(
        &dir,
        &["tail", "--from", "0", "--limit", "5", "--seconds", "5"],
    );
    assert!(ok, "tail failed:\n{out}\n{err}");
    assert!(out.contains("resume cursor is"), "tail output:\n{out}");
    assert!(
        out.contains("registered") || out.contains("created"),
        "tail should render the start of the log:\n{out}"
    );

    // --- rebuild (the checkpoint byte-compare proof) ----------------------
    // `stats` above wrote a checkpoint, so this is a real resume.
    let (out, err, ok) = chatter(&dir, &["rebuild"]);
    assert!(ok, "rebuild failed:\n{out}\n{err}");
    assert!(out.contains("rebuild-compare: PASS"), "rebuild output:\n{out}");
    assert!(
        out.contains("resumed from checkpoint at position"),
        "rebuild should have found the checkpoint `stats` wrote:\n{out}"
    );

    // --- reads survive a deleted checkpoint -------------------------------
    let ckpt = chatter::store_backend::checkpoint_path(&dir);
    std::fs::remove_file(&ckpt).expect("delete the checkpoint");
    let (out, err, ok) = chatter(&dir, &["stats"]);
    assert!(ok, "stats after checkpoint deletion failed:\n{out}\n{err}");
    assert!(out.contains("500 messages"), "stats output:\n{out}");

    // --- and a corrupt one is reported, not fatal -------------------------
    std::fs::write(&ckpt, b"not a checkpoint").expect("corrupt the checkpoint");
    let (out, err, ok) = chatter(&dir, &["stats"]);
    assert!(ok, "stats after checkpoint corruption failed:\n{out}\n{err}");
    assert!(out.contains("500 messages"), "stats output:\n{out}");
    assert!(
        err.contains("discarding checkpoint"),
        "a corrupt checkpoint must be REPORTED on stderr:\n{err}"
    );
}

#[tokio::test]
async fn the_bench_subcommand_prints_json_on_stdout() {
    let t = sweeping_temp_dir("chatter-cli-bench");
    let dir = t.path().join("bench-store");
    let (out, err, ok) = chatter(
        &dir,
        &[
            "bench",
            "--seed",
            "3",
            "--channels",
            "3",
            "--users",
            "12",
            "--messages",
            "400",
            "--reactions",
            "0.5",
            "--segment-bytes",
            "64K",
            "--scroll-pages",
            "2",
            "--scroll-page-size",
            "16",
        ],
    );
    assert!(ok, "bench failed:\n{out}\n{err}");
    let v: Value =
        serde_json::from_str(out.trim()).expect("stdout must be pure JSON");
    assert_eq!(v["tool"], "chatter-bench");
    let names: Vec<&str> = v["cells"]
        .as_array()
        .expect("cells")
        .iter()
        .map(|c| c["name"].as_str().expect("name"))
        .collect();
    assert_eq!(names, chatter::bench::CELL_NAMES.to_vec());
    assert!(v["store"]["census"]["sealed_segments"].as_u64().unwrap() > 1);
}

#[test]
fn an_unknown_command_exits_with_usage() {
    let out = Command::new(env!("CARGO_BIN_EXE_chatter"))
        .arg("frobnicate")
        .output()
        .expect("run");
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("unknown command: frobnicate"), "stderr:\n{err}");
    assert!(err.contains("Usage: chatter"), "stderr:\n{err}");
}

/// Pull the sealed-segment count out of the seed command's summary line.
fn sealed_count(out: &str) -> usize {
    let line = out
        .lines()
        .find(|l| l.contains("sealed tier:"))
        .unwrap_or_else(|| panic!("no sealed-tier line in:\n{out}"));
    let after = line.split("sealed tier:").nth(1).expect("split");
    after
        .split_whitespace()
        .next()
        .expect("count")
        .parse()
        .unwrap_or_else(|_| panic!("unparseable sealed count in {line:?}"))
}
