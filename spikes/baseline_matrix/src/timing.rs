//! Competing-load guard + peak-RSS reader, ported verbatim in spirit from
//! `spikes/epoch_dedupe/src/timing.rs`. Never publish contaminated numbers:
//! every measured phase must be preceded by [`ensure_quiet`].

use std::time::Instant;

/// Current 1-minute load average.
pub fn load1() -> f64 {
    std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|s| s.split_whitespace().next().map(str::to_string))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0)
}

/// Competing-load guard. "Quiet" = no rustc/cc/ld processes anywhere, no
/// cargo/bench processes OUTSIDE our own ancestor chain, and 1-min loadavg
/// below 6.0. The load floor is 6.0 (not ~1) because this host carries a
/// persistent ~4-5 ambient load from unrelated user processes on other cores
/// (documented in spikes/epoch_dedupe/REPORT.md §3); a compiler storm sits
/// far above the floor and always blocks.
pub fn contention() -> Option<String> {
    let mut ancestors = Vec::new();
    let mut pid = std::process::id();
    while pid != 0 {
        ancestors.push(pid);
        let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(s) => s,
            Err(_) => break,
        };
        // ppid is the second field after the parenthesised comm.
        pid = stat
            .rfind(')')
            .map(|i| &stat[i + 1..])
            .and_then(|rest| rest.split_whitespace().nth(1))
            .and_then(|p| p.parse().ok())
            .unwrap_or(0);
    }

    let mut offenders = Vec::new();
    if let Ok(rd) = std::fs::read_dir("/proc") {
        for e in rd.flatten() {
            let name = e.file_name();
            let Some(p) =
                name.to_str().and_then(|s| s.parse::<u32>().ok())
            else {
                continue;
            };
            if ancestors.contains(&p) {
                continue;
            }
            let comm = std::fs::read_to_string(format!("/proc/{p}/comm"))
                .unwrap_or_default();
            let comm = comm.trim();
            if matches!(
                comm,
                "rustc" | "cc" | "cc1" | "ld" | "lld" | "cargo" | "bench"
            ) {
                offenders.push(format!("{p}:{comm}"));
            }
        }
    }
    let load1 = load1();
    if offenders.is_empty() && load1 < 6.0 {
        None
    } else {
        Some(format!("load1={load1:.2} procs=[{}]", offenders.join(",")))
    }
}

/// Block until the machine is quiet (sleep-and-retry). Panics after
/// `max_wait_s` so a wedged host never silently produces contaminated numbers.
pub fn ensure_quiet(max_wait_s: u64) {
    let t0 = Instant::now();
    loop {
        match contention() {
            None => return,
            Some(why) => {
                if t0.elapsed().as_secs() > max_wait_s {
                    panic!(
                        "machine still contended after {max_wait_s}s: {why}"
                    );
                }
                eprintln!("[quiet-guard] contended ({why}); sleeping 5s");
                std::thread::sleep(std::time::Duration::from_secs(5));
            }
        }
    }
}
