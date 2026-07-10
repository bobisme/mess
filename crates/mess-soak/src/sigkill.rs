//! The out-of-process `SIGKILL` crash orchestrator (`--crash-mode sigkill`).
//!
//! The in-process drop-and-reopen crash is a *graceful* shutdown in disguise:
//! [`Inner::drop`](mess_store) shuts the committer and joins the seal thread,
//! so every destructor runs. That exercises recovery, but it is not the "hour
//! three, the box lost power / the process was `OOM`-killed" crash — the one
//! where no destructor runs and whatever the durability barrier had NOT yet
//! forced to disk is simply gone.
//!
//! This mode is faithful to that: it forks the `soak-child` worker (a real
//! [`LogEngine`] under `Durability::Os`, so every acked append is `fdatasync`ed
//! before its ack is reported), reads the child's ack ledger off a pipe
//! concurrently, `SIGKILL`s it at a randomized delay, then reopens the store
//! and reconciles. The contract recovery must honor:
//!
//! - **No acked loss.** Every `(stream, stream_pos, global_pos)` the parent
//!   received before the kill MUST be present, unchanged, after recovery.
//! - **Dense prefix.** The recovered global order is `0..N` with no gap or
//!   dupe.
//! - **Tail slack is legal.** The child may have earned an ack the parent never
//!   read (killed between the `fdatasync` and the stdout write). Extra
//!   recovered tail events beyond the ledger are correct, not a violation.
//!
//! Mirrors `mess-log`'s proven `sigkill_harness`, one tier up.

use std::io::{BufRead, BufReader};
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mess_soak::config::Config;
use mess_soak::resource;
use mess_store::backend::Backend;
use mess_store::{LogEngine, Version};

/// One acked batch as reported by the child.
#[derive(Debug, Clone, Copy)]
struct Ack {
    stream_idx:       u64,
    first_stream_pos: u64,
    first_global:     u64,
    count:            u64,
}

pub fn run(cfg: Config) {
    if let Err(e) = guard_dir(&cfg) {
        eprintln!("error: {e}");
        std::process::exit(2);
    }
    let child_exe = match locate_child() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: cannot locate soak-child binary: {e}");
            std::process::exit(2);
        }
    };
    let kill_delay = if cfg.crash_every == Duration::ZERO {
        Duration::from_secs(5)
    } else {
        cfg.crash_every
    };
    println!("[soak/sigkill] starting\n  {}", cfg.summary());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let deadline = Instant::now() + cfg.duration;
    let mut round = 0u64;
    let mut total_acks = 0u64;
    while Instant::now() < deadline {
        round += 1;
        let acks = run_round(&cfg, &child_exe, kill_delay, round);
        total_acks += acks.len() as u64;
        if let Err(detail) = rt.block_on(reconcile(&cfg, &acks)) {
            eprintln!(
                "\n================= SOAK SIGKILL RECOVERY VIOLATION =================\n\
                 ROUND {round}: {detail}\n\
                 REPRODUCE: mess-soak --crash-mode sigkill --seed {seed:#x} --dir {dir} \
                 --streams {streams} --writers {writers} --crash-every {kd:?}\n\
                 (the store dir is left intact on disk for post-mortem)\n\
                 CONFIG:\n  {cfg}\n\
                 ==================================================================\n",
                seed = cfg.seed,
                dir = cfg.dir.display(),
                streams = cfg.streams,
                writers = cfg.writers,
                kd = kill_delay,
                cfg = cfg.summary(),
            );
            std::process::exit(1);
        }
        println!(
            "[soak/sigkill] round {round}: {} acks reconciled, store intact",
            acks.len()
        );
    }
    println!(
        "\n[soak/sigkill] COMPLETE — {round} kill/recover rounds, \
         {total_acks} acked events reconciled, zero recovery violations."
    );
}

fn guard_dir(cfg: &Config) -> Result<(), String> {
    // Fresh at STARTUP (bn-3dr): a leftover store from a prior invocation
    // would corrupt every round's reconcile. Rounds *within* this invocation
    // then intentionally reuse the dir — each round's ledger is checked
    // against the accumulated store.
    resource::guard_fresh_dir(&cfg.dir)?;
    if resource::is_tmpfs(&cfg.dir).map_err(|e| format!("tmpfs check: {e}"))? {
        return Err(format!(
            "refusing to soak on tmpfs dir {} — fdatasync is a no-op there \
             (--dir must be a real device)",
            cfg.dir.display()
        ));
    }
    std::fs::create_dir_all(&cfg.dir)
        .map_err(|e| format!("create_dir_all: {e}"))
}

/// Spawn `soak-child`, read its ack ledger concurrently, kill it after
/// `kill_delay`, and return whatever acks the parent had actually received by
/// the time the pipe closed (that set IS the ledger, by definition).
fn run_round(
    cfg: &Config,
    child_exe: &PathBuf,
    kill_delay: Duration,
    round: u64,
) -> Vec<Ack> {
    let mut child = Command::new(child_exe)
        .arg(&cfg.dir)
        .arg(cfg.seed.to_string())
        .arg(cfg.streams.to_string())
        .arg(cfg.writers.to_string())
        .arg(cfg.segment_size.to_string())
        .arg(round.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn soak-child");

    let stderr = child.stderr.take().expect("piped stderr");
    let stderr_reader = std::thread::spawn(move || {
        let mut s = String::new();
        let _ =
            std::io::Read::read_to_string(&mut BufReader::new(stderr), &mut s);
        s
    });

    let stdout = child.stdout.take().expect("piped stdout");
    let acks: Arc<Mutex<Vec<Ack>>> = Arc::new(Mutex::new(Vec::new()));
    let acks_r = acks.clone();
    let reader = std::thread::spawn(move || {
        let mut r = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match r.read_line(&mut line) {
                Ok(0) => break, // child's write end closed (it died).
                Ok(_) => {
                    if let Some(ack) = parse_ack(line.trim_end()) {
                        acks_r.lock().unwrap().push(ack);
                    }
                }
                Err(_) => break,
            }
        }
    });

    std::thread::sleep(kill_delay);
    child.kill().expect("SIGKILL soak-child");
    let status = child.wait().expect("wait killed child");
    reader.join().expect("join reader");
    let stderr_out = stderr_reader.join().unwrap_or_default();

    const SIGKILL: i32 = libc::SIGKILL;
    if status.signal() != Some(SIGKILL) {
        // The child exited on its own before we killed it — for a soak worker
        // that loops forever, that is a real crash/panic and a genuine bug.
        eprintln!(
            "[soak/sigkill] round {round}: child exited on its own \
             (status={status:?}) — this is a real failure, not a vacuous \
             round.\nchild stderr:\n{stderr_out}"
        );
        std::process::exit(1);
    }
    acks.lock().unwrap().clone()
}

fn parse_ack(line: &str) -> Option<Ack> {
    // "A <stream_idx> <first_stream_pos> <first_global> <count>"
    let mut f = line.split(' ');
    if f.next()? != "A" {
        return None;
    }
    let stream_idx = f.next()?.parse().ok()?;
    let first_stream_pos = f.next()?.parse().ok()?;
    let first_global = f.next()?.parse().ok()?;
    let count = f.next()?.parse().ok()?;
    if f.next().is_some() {
        return None; // torn trailing line at kill time: drop it (safe direction).
    }
    Some(Ack { stream_idx, first_stream_pos, first_global, count })
}

/// Reopen the store and check recovery against the ack ledger + global density.
async fn reconcile(cfg: &Config, acks: &[Ack]) -> Result<(), String> {
    let engine = LogEngine::open_with(&cfg.dir, cfg.engine_options())
        .map_err(|e| format!("reopen after kill failed: {e}"))?;
    let total = engine.total_events() as u64;

    // 1) Dense global prefix 0..total (no gap / no dupe survived the kill).
    let mut expect = 0u64;
    let mut after = None;
    while expect < total {
        let page = engine
            .read_global(after, 4096)
            .await
            .map_err(|e| format!("read_global reconcile: {e}"))?;
        if page.is_empty() {
            break;
        }
        for rec in &page {
            if rec.global_position != expect {
                return Err(format!(
                    "recovered global gap/dupe: expected {expect}, got {}",
                    rec.global_position
                ));
            }
            expect += 1;
            after = Some(rec.global_position);
        }
    }
    if expect != total {
        return Err(format!(
            "recovered {expect} events but total_events()={total}"
        ));
    }

    // 2) No acked loss. The ledger's highest global must be < total, and a
    //    sample of acked events must be present exactly where they were acked.
    let ledger_max = acks.iter().map(|a| a.first_global + a.count - 1).max();
    if let Some(m) = ledger_max
        && m >= total
    {
        return Err(format!(
            "acked global {m} lost: recovered total is only {total} events"
        ));
    }
    // Verify up to 256 acked events precisely (bounded cost).
    let stride = (acks.len() / 256).max(1);
    for ack in acks.iter().step_by(stride) {
        for k in 0..ack.count {
            let gp = ack.first_global + k;
            let want_stream = format!("stream-{:05}", ack.stream_idx);
            let want_sp = ack.first_stream_pos + k;
            let after = if gp == 0 { None } else { Some(gp - 1) };
            let page = engine
                .read_global(after, 1)
                .await
                .map_err(|e| format!("read_global({gp}) reconcile: {e}"))?;
            let rec = page.into_iter().next().ok_or_else(|| {
                format!("acked global {gp} missing after recovery")
            })?;
            if rec.global_position != gp {
                return Err(format!(
                    "acked global {gp} recovered at {}",
                    rec.global_position
                ));
            }
            if rec.stream_id != want_stream {
                return Err(format!(
                    "acked global {gp} recovered on stream {} (expected \
                     {want_stream})",
                    rec.stream_id
                ));
            }
            if rec.stream_position != want_sp {
                return Err(format!(
                    "acked global {gp} recovered at stream_pos {} (expected \
                     {want_sp})",
                    rec.stream_position
                ));
            }
        }
        // Head must be at least as advanced as this ack.
        let head = engine
            .head(&format!("stream-{:05}", ack.stream_idx))
            .await
            .map_err(|e| format!("head reconcile: {e}"))?;
        let want_last = ack.first_stream_pos + ack.count - 1;
        match head {
            Version::At(v) if v >= want_last => {}
            other => {
                return Err(format!(
                    "stream-{:05} head {other:?} regressed below acked pos \
                     {want_last}",
                    ack.stream_idx
                ));
            }
        }
    }
    Ok(())
}

/// Sibling `soak-child` lives next to this executable in the target dir.
fn locate_child() -> Result<PathBuf, String> {
    let me = std::env::current_exe().map_err(|e| e.to_string())?;
    let dir = me.parent().ok_or("current_exe has no parent")?;
    let candidate = dir.join("soak-child");
    if candidate.exists() {
        Ok(candidate)
    } else {
        Err(format!(
            "{} not found (build the `soak-child` bin)",
            candidate.display()
        ))
    }
}
