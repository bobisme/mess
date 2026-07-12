//! Differential corpus (small default; the full >=100k gate run goes
//! through `bench corpus 100000` so `cargo test` stays quick).
//! `SEGEFF_HISTORIES=<n>` overrides.

use segment_effect::recover::differential_check;

#[test]
fn differential_corpus() {
    let n: u64 = std::env::var("SEGEFF_HISTORIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000);
    let threads = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(8)
        .min(16) as u64;
    std::thread::scope(|s| {
        for t in 0..threads {
            s.spawn(move || {
                let mut seed = t;
                while seed < n {
                    differential_check(seed);
                    seed += threads;
                }
            });
        }
    });
}
