//! The `mess` operational CLI binary (doc 09 Phase 10).
//!
//! A thin clap front end over [`mess_cli`]. Each subcommand resolves the
//! output format, runs the library `run` function, prints the rendered report
//! to stdout (errors to stderr), and exits with the report's exit code.

use std::io::IsTerminal;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand, ValueEnum};
use mess_cli::backup::{self, BackupOptions};
use mess_cli::doctor::{self, DoctorOptions};
use mess_cli::format::{self, Format};
use mess_cli::inspect::{self, InspectOptions};
use mess_cli::rebuild::{self, RebuildOptions};
use mess_cli::report::{EXIT_SYSTEM, Report};
use mess_cli::restore::{self, RestoreOptions};
use mess_cli::retention;
use mess_cli::verify::{self, VerifyOptions};

/// `mess` — operate and diagnose a mess event-store directory.
#[derive(Debug, Parser)]
#[command(name = "mess", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// The documented `--format` values (`text` | `pretty` | `json`).
#[derive(Debug, Clone, Copy, ValueEnum)]
enum FormatArg {
    Text,
    Pretty,
    Json,
}

impl From<FormatArg> for Format {
    fn from(a: FormatArg) -> Self {
        match a {
            FormatArg::Text => Format::Text,
            FormatArg::Pretty => Format::Pretty,
            FormatArg::Json => Format::Json,
        }
    }
}

/// Flags shared by every subcommand.
#[derive(Debug, Args)]
struct Common {
    /// Output format: text|pretty|json [default: auto — pretty on a TTY,
    /// text when piped].
    #[arg(long, value_enum, global = true)]
    format: Option<FormatArg>,
    /// Hidden alias for `--format json` (agents frequently guess this).
    #[arg(long, hide = true, global = true)]
    json:   bool,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Health checks: lock, epoch, footer/trailer, sealed index (`.seal` pack
    /// or `.pidx` sidecar), quarantined candidates, fsync, fold-version,
    /// registry.
    ///
    /// Every check but one is safe against a live writer. The $registry fold
    /// is the exception: it opens the engine, which a live writer holds under
    /// the store's single-writer lock, so against a running app that one
    /// check can't run and degrades to an info-severity
    /// "registry-store-locked" finding instead of failing the whole command
    /// — expected behavior, not a bug. For the full check, stop the writer
    /// first, or point `dir` at a `mess backup`/`mess restore` copy instead
    /// of the live directory.
    Doctor {
        /// The store directory.
        dir:                 PathBuf,
        /// Expect every live snapshot to carry this fold_version.
        #[arg(long)]
        expect_fold_version: Option<u32>,
        #[command(flatten)]
        common:              Common,
    },
    /// Segment chain, stream heads, and registry overview.
    Inspect {
        /// The store directory.
        dir:         PathBuf,
        /// Restrict the segment overview to this segment id.
        #[arg(long)]
        segment:     Option<u64>,
        /// Restrict the stream-head overview to one stream: its interned
        /// numeric id or its registered name (name lookup needs the
        /// metadata registry to be readable).
        #[arg(long)]
        stream:      Option<String>,
        /// Show every stream head in `text`/`pretty` output instead of the
        /// default top-N truncation. `--format json` is always complete.
        #[arg(long)]
        all_streams: bool,
        #[command(flatten)]
        common:      Common,
    },
    /// Recovery scanner: detect every corruption class; exit non-zero on any.
    Verify {
        /// The store directory.
        dir:    PathBuf,
        /// Full byte-integrity pass (payload reassembly) + fold-chain
        /// linkage recompute.
        #[arg(long)]
        full:   bool,
        /// Attempt Reed-Solomon repair of damaged sealed segments from their
        /// `.par` parity sidecars (bn-2za): reconstruct damaged blocks,
        /// re-verify against batch CRCs + fold chain, keep the damaged
        /// original as `.damaged-<ts>`, and write the repaired segment
        /// atomically.
        #[arg(long)]
        repair: bool,
        #[command(flatten)]
        common: Common,
    },
    /// Rebuild the legacy `.pidx` pointer sidecars (byte-equal) from the log
    /// segments.
    ///
    /// Pack-sealed segments (bn-3of `.seal`) are skipped: this command emits
    /// only the legacy sidecar shape, which a pack-sealed segment either
    /// shadows or — when its footer names a SealPack (bn-11g) — refuses
    /// outright. Re-sealing is the engine's job for those.
    ///
    /// KNOWN GAP since bn-ccx1 made pack sealing the engine default: every
    /// newly sealed segment is pack-sealed, so on a store written by a current
    /// engine this command rebuilds nothing and reports
    /// `pack-sealed-segment-skipped` per segment. It still rebuilds legacy
    /// loose-sidecar stores (and any store run with `seal_pack: false`). An
    /// offline pack encoder is tracked as bn-3qh0.
    RebuildIndex {
        /// The store directory.
        dir:     PathBuf,
        /// Preview without writing anything.
        #[arg(long)]
        dry_run: bool,
        #[command(flatten)]
        common:  Common,
    },
    /// Retention verdict + blockers per sealed segment.
    Retention {
        #[command(subcommand)]
        what: RetentionCmd,
    },
    /// Copy a consistent cut of a live store to a backup destination.
    Backup {
        /// The source store directory.
        dir:         PathBuf,
        /// The backup destination directory.
        #[arg(long)]
        to:          PathBuf,
        /// Copy only sealed segments/sidecars not already present at the
        /// destination (verified by size + CRC).
        #[arg(long)]
        incremental: bool,
        #[command(flatten)]
        common:      Common,
    },
    /// Restore a store from a backup: copy back, recover, and verify --full.
    Restore {
        /// The backup source directory.
        src:    PathBuf,
        /// The (empty or absent) target store directory.
        #[arg(long)]
        to:     PathBuf,
        #[command(flatten)]
        common: Common,
    },
}

#[derive(Debug, Subcommand)]
enum RetentionCmd {
    /// Explain each sealed segment's retention verdict and blockers.
    Explain {
        /// The store directory.
        dir:    PathBuf,
        #[command(flatten)]
        common: Common,
    },
}

fn resolve_format(common: &Common) -> Format {
    Format::resolve(
        common.format.map(Into::into),
        common.json,
        std::io::stdout().is_terminal(),
    )
}

/// Emit the report and return its process exit code.
fn emit(report: &Report, format: Format) -> ExitCode {
    println!("{}", format::render(report, format));
    ExitCode::from(report.exit_code() as u8)
}

/// Guard: refuse to run against a path that is not a directory, so every
/// command fails fast with an actionable system error rather than an empty
/// report.
fn require_dir(dir: &std::path::Path) -> Result<(), ExitCode> {
    if dir.is_dir() {
        Ok(())
    } else {
        eprintln!(
            "Error: store directory not found at {}\n  Pass the path to a \
             mess store directory (the one holding LOCK and seg-*.log).",
            dir.display()
        );
        Err(ExitCode::from(EXIT_SYSTEM as u8))
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Doctor { dir, expect_fold_version, common } => {
            if let Err(code) = require_dir(&dir) {
                return code;
            }
            let report =
                doctor::run(&dir, &DoctorOptions { expect_fold_version });
            emit(&report, resolve_format(&common))
        }
        Command::Inspect { dir, segment, stream, all_streams, common } => {
            if let Err(code) = require_dir(&dir) {
                return code;
            }
            let report = inspect::run(
                &dir,
                &InspectOptions { segment, stream, all_streams },
            );
            emit(&report, resolve_format(&common))
        }
        Command::Verify { dir, full, repair, common } => {
            if let Err(code) = require_dir(&dir) {
                return code;
            }
            let report = verify::run(&dir, &VerifyOptions { full, repair });
            emit(&report, resolve_format(&common))
        }
        Command::RebuildIndex { dir, dry_run, common } => {
            if let Err(code) = require_dir(&dir) {
                return code;
            }
            let report = rebuild::run(&dir, &RebuildOptions { dry_run });
            emit(&report, resolve_format(&common))
        }
        Command::Retention { what } => match what {
            RetentionCmd::Explain { dir, common } => {
                if let Err(code) = require_dir(&dir) {
                    return code;
                }
                let report = retention::run(&dir);
                emit(&report, resolve_format(&common))
            }
        },
        Command::Backup { dir, to, incremental, common } => {
            if let Err(code) = require_dir(&dir) {
                return code;
            }
            let report = backup::run(&dir, &to, &BackupOptions { incremental });
            emit(&report, resolve_format(&common))
        }
        Command::Restore { src, to, common } => {
            if let Err(code) = require_dir(&src) {
                return code;
            }
            let report = restore::run(&src, &to, &RestoreOptions::default());
            emit(&report, resolve_format(&common))
        }
    }
}
