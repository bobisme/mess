//! `mess-soak` — the multi-hour mixed-workload soak driver (bn-1fo).
//!
//! Some bugs only exist at hour three: slow leaks, seal/retention churn
//! interactions, watermark drift, fsync degradation on aging devices, dedupe
//! window turnover. No unit test runs long enough to surface them. This crate
//! is a long-running driver that hammers a **real** [`LogEngine`] on a **real**
//! filesystem with a Zipf-skewed mixed workload — writers, subscribers joining
//! and leaving, frequent segment rolls + background seals, and periodic
//! crash-and-recover cycles — while a set of continuous invariant probes checks
//! the engine against an in-driver shadow model on every action. The first
//! violation dumps a reproducible state bundle and aborts; the run's exit
//! criteria are invariants and resource ceilings, not throughput.
//!
//! # Layout
//!
//! - [`config`] — every `--flag` and resource ceiling.
//! - [`prng`] — seeded SplitMix64 + a Zipf stream sampler (determinism per seed).
//! - [`shadow`] — the source-of-truth model the probes check the engine against.
//! - [`probe`] — the invariant checks, as pure functions (each unit-tested by
//!   feeding it a doctored input; see the module's tests).
//! - [`metrics`] — the bounded-memory fsync-latency histogram.
//! - [`resource`] — RSS / fd readers and the tmpfs guard.
//! - [`driver`] — the in-process soak loop (drop-and-reopen crashes).
//!
//! The out-of-process `SIGKILL` crash mode lives in the `soak-child` bin and is
//! orchestrated by the `mess-soak` bin; see `README.md`.
//!
//! [`LogEngine`]: mess_store::LogEngine

pub mod config;
pub mod driver;
pub mod metrics;
pub mod probe;
pub mod prng;
pub mod resource;
pub mod shadow;

pub use config::{Config, CrashMode};
pub use driver::{Aborted, Driver, SoakReport};
pub use probe::Violation;

/// Open the engine and run a soak to completion (or to the first violation).
/// The convenience entry point the binary and the in-process smoke test share.
pub async fn run(cfg: Config) -> Result<SoakReport, Aborted> {
    let mut driver = Driver::open(cfg).map_err(|e| Aborted {
        violation: Violation::RecoveryLoss { detail: e.clone() },
        dump: format!("startup failed: {e}"),
    })?;
    driver.run().await
}
