//! Spike A (bn-hzc): dense stream-head tables — direct designs vs fjall and
//! HashMap. See REPORT.md for results; `src/main.rs` is the driver.

pub mod candidates;
pub mod direct;
pub mod shim;

#[cfg(not(loom))]
pub mod keys;
#[cfg(not(loom))]
pub mod timing;
