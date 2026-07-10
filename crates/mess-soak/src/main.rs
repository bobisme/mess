//! `mess-soak` — the operator entry point.
//!
//! Parses `--flags` into a [`Config`], refuses a tmpfs `--dir`, and runs either
//! the in-process drop-and-reopen soak (default) or the out-of-process
//! `SIGKILL` crash soak (`--crash-mode sigkill`), which forks the `soak-child`
//! worker, kills it mid-flight, and reconciles recovery against its ack ledger.

use std::path::PathBuf;
use std::time::Duration;

use mess_log::committer::Durability;
use mess_soak::config::{Config, CrashMode};

mod sigkill;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_help();
        return;
    }
    let cfg = match parse_args(&args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}\n");
            print_help();
            std::process::exit(2);
        }
    };

    // Fresh-`--dir` guard (bn-3dr root cause): a leftover store from a prior
    // (possibly killed) run would read as thousands of fabricated "extras" and
    // permanent version conflicts against the empty shadow. Refuse up front
    // with the usage exit code; there is no resume mode.
    if let Err(e) = mess_soak::resource::guard_fresh_dir(&cfg.dir) {
        eprintln!("error: {e}");
        std::process::exit(2);
    }

    match cfg.crash_mode {
        CrashMode::DropReopen => run_in_process(cfg),
        CrashMode::Sigkill => sigkill::run(cfg),
    }
}

fn run_in_process(cfg: Config) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let result = rt.block_on(mess_soak::run(cfg));
    match result {
        Ok(report) => {
            println!(
                "\n[soak] COMPLETE — no invariant violations.\n  {report:#?}"
            );
        }
        Err(aborted) => {
            eprintln!("{}", aborted.dump);
            // Abort (not a clean exit) so a supervising harness sees the crash
            // signal and the core/dump machinery captures the process state.
            std::process::exit(1);
        }
    }
}

fn parse_args(args: &[String]) -> Result<Config, String> {
    let mut cfg = Config::default();
    let mut i = 0;
    while i < args.len() {
        let key = &args[i];
        let mut value = || -> Result<&String, String> {
            i += 1;
            args.get(i).ok_or_else(|| format!("{key} needs a value"))
        };
        match key.as_str() {
            "--duration" => cfg.duration = parse_secs(value()?)?,
            "--streams" => {
                cfg.streams = parse_usize(value()?, "streams")?.max(1)
            }
            "--writers" => {
                cfg.writers = parse_usize(value()?, "writers")?.max(1)
            }
            "--crash-every-actions" => {
                cfg.crash_every_actions = parse_u64(value()?)?
            }
            "--crash-every" => cfg.crash_every = parse_secs(value()?)?,
            "--seed" => cfg.seed = parse_u64(value()?)?,
            "--dir" => cfg.dir = PathBuf::from(value()?),
            "--zipf-skew" => {
                cfg.zipf_skew =
                    value()?.parse().map_err(|_| "zipf-skew: expected float")?
            }
            "--max-batch" => {
                cfg.max_batch = parse_usize(value()?, "max-batch")?.max(1)
            }
            "--subscribers" => {
                cfg.subscribers = parse_usize(value()?, "subscribers")?
            }
            "--segment-size" => {
                cfg.segment_size = parse_u64(value()?)?.max(4096)
            }
            "--durability" => cfg.durability = parse_durability(value()?)?,
            "--rss-ceiling" => cfg.rss_ceiling_bytes = parse_u64(value()?)?,
            "--fd-ceiling" => {
                cfg.fd_ceiling = parse_usize(value()?, "fd-ceiling")?
            }
            "--fsync-p99-ceiling" => {
                cfg.fsync_p99_ceiling = parse_millis(value()?)?
            }
            "--crash-mode" => cfg.crash_mode = parse_crash_mode(value()?)?,
            "--metrics-every" => cfg.metrics_every = parse_secs(value()?)?,
            "--dump-extras" => cfg.dump_extras = Some(PathBuf::from(value()?)),
            "--verbose" => cfg.verbose = true,
            other => return Err(format!("unknown flag {other}")),
        }
        i += 1;
    }
    Ok(cfg)
}

fn parse_secs(s: &str) -> Result<Duration, String> {
    let v: f64 =
        s.parse().map_err(|_| format!("expected seconds, got {s:?}"))?;
    Ok(Duration::from_secs_f64(v.max(0.0)))
}
fn parse_millis(s: &str) -> Result<Duration, String> {
    let v: f64 =
        s.parse().map_err(|_| format!("expected milliseconds, got {s:?}"))?;
    Ok(Duration::from_secs_f64(v.max(0.0) / 1e3))
}
fn parse_usize(s: &str, what: &str) -> Result<usize, String> {
    s.parse().map_err(|_| format!("{what}: expected integer, got {s:?}"))
}
fn parse_u64(s: &str) -> Result<u64, String> {
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u64::from_str_radix(hex, 16)
            .map_err(|_| format!("expected hex u64, got {s:?}"))
    } else {
        s.parse().map_err(|_| format!("expected u64, got {s:?}"))
    }
}
fn parse_durability(s: &str) -> Result<Durability, String> {
    match s {
        "process" => Ok(Durability::Process),
        "os" => Ok(Durability::Os),
        "group" => Ok(Durability::group_default()),
        other => {
            Err(format!("durability: want process|os|group, got {other:?}"))
        }
    }
}
fn parse_crash_mode(s: &str) -> Result<CrashMode, String> {
    match s {
        "drop" | "drop-reopen" => Ok(CrashMode::DropReopen),
        "sigkill" => Ok(CrashMode::Sigkill),
        other => Err(format!("crash-mode: want drop|sigkill, got {other:?}")),
    }
}

fn print_help() {
    let d = Config::default();
    println!(
        "mess-soak — multi-hour mixed-workload soak with continuous invariant \
         checking\n\nUSAGE: mess-soak [FLAGS]\n\nFLAGS (defaults in \
         brackets):\n--duration <secs>          total run length \
         [{dur}]\n--streams <n>              distinct streams the Zipf \
         sampler ranges over [{streams}]\n--writers <n>              \
         sigkill-mode child writer tasks ONLY; the in-process\ndrop-reopen \
         driver is strictly sequential [{writers}]\n--subscribers <n>          \
         target concurrent subscribers [{subs}]\n--crash-every-actions <n>  \
         drop mode: actions between crash cycles (deterministic,\nnever \
         wall-clock); 0 = never [{cea}]\n--crash-every <secs>       sigkill \
         mode ONLY: wall delay before the child is killed \
         [{crash}]\n--crash-mode <drop|sigkill>  drop-and-reopen (in-proc) or \
         fork+SIGKILL child [drop]\n--seed <u64|0xHEX>         master seed; \
         deterministic per seed [{seed:#x}]\n--dir <path>               store \
         dir; MUST be EMPTY/fresh and MUST NOT be tmpfs\n(both refused) \
         [$HOME/.cache/mess-soak]\n--dump-extras <path>       write the \
         extra-event classification JSON there if the\npost-reopen reconcile \
         finds illegal extras\n--zipf-skew <f>            0=uniform, \
         higher=hotter head [{skew}]\n--max-batch <n>            max events \
         per append [{mb}]\n--segment-size <bytes>     active segment size; \
         small => frequent rolls/seals [{seg}]\n--durability \
         <process|os|group>  ack barrier [os]\n--rss-ceiling <bytes>      RSS \
         abort ceiling; 0 disables [{rss}]\n--fd-ceiling <n>           \
         open-fd abort ceiling; 0 disables [{fd}]\n--fsync-p99-ceiling <ms>   \
         fsync p99 abort ceiling; 0 disables [off]\n--metrics-every <secs>     \
         periodic metric print interval [{me}]\n--verbose                  \
         per-crash chatter\n-h, --help                 this help\n\nSee \
         crates/mess-soak/README.md for the 2h nightly profile and how to \
         read an abort dump.",
        dur = d.duration.as_secs(),
        streams = d.streams,
        writers = d.writers,
        subs = d.subscribers,
        cea = d.crash_every_actions,
        crash = d.crash_every.as_secs(),
        seed = d.seed,
        skew = d.zipf_skew,
        mb = d.max_batch,
        seg = d.segment_size,
        rss = d.rss_ceiling_bytes,
        fd = d.fd_ceiling,
        me = d.metrics_every.as_secs(),
    );
}
