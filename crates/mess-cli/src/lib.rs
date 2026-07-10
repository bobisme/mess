//! mess v1: the operational CLI library (doc 09 Phase 10 surface).
//!
//! The `mess` binary is a thin clap front end over these modules; each command
//! is a pure `run(dir, opts) -> Report` function so it is directly testable
//! without spawning a process. See `.agents/edict/design/cli-conventions.md`
//! for the output/exit-code conventions the [`report`] and [`format`] modules
//! implement.
//!
//! Commands:
//! - [`doctor`] — lock, epoch, footer/trailer, sidecar, fsync, fold-version.
//! - [`inspect`] — segment chain, stream heads, registry.
//! - [`verify`] — the recovery scanner over every corruption class.
//! - [`rebuild`] — I5: rebuild pointer sidecars (byte-equal) + meta.
//! - [`retention`] — the bn-2ug retention verdict per sealed segment.
//! - [`backup`] / [`restore`] — bn-2ln online backup: consistent cut,
//!   incremental copy, retention lease ([`lease`]).

pub mod backup;
pub mod doctor;
pub mod format;
pub mod inspect;
pub mod lease;
pub mod lockprobe;
pub mod metaread;
pub mod rebuild;
pub mod report;
pub mod restore;
pub mod retention;
pub mod scan;
pub mod store;
pub mod verify;
