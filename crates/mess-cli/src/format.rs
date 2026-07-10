//! Output-format resolution (`.agents/edict/design/cli-conventions.md`).
//!
//! Resolution order: explicit `--format` / `--json` > `FORMAT` env var > TTY
//! auto-detect (TTY → pretty, pipe → text). `json` is always an object
//! envelope with stable field names.

use crate::report::Report;

/// The three output audiences.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// Concise, token-efficient plain text (agents, pipes) — the non-TTY
    /// default.
    Text,
    /// Tables / tags for humans at a terminal — the TTY default.
    Pretty,
    /// Structured, stable-schema object envelope for machines.
    Json,
}

impl Format {
    /// Resolve the effective format. `flag` is the parsed `--format` value
    /// (`None` if unset); `json_shorthand` is the hidden `--json` alias.
    /// `is_tty` is whether stdout is a terminal.
    #[must_use]
    pub fn resolve(
        flag: Option<Format>,
        json_shorthand: bool,
        is_tty: bool,
    ) -> Format {
        if json_shorthand {
            return Format::Json;
        }
        if let Some(f) = flag {
            return f;
        }
        if let Ok(env) = std::env::var("FORMAT") {
            match env.as_str() {
                "json" => return Format::Json,
                "text" => return Format::Text,
                "pretty" => return Format::Pretty,
                _ => {}
            }
        }
        if is_tty { Format::Pretty } else { Format::Text }
    }

    /// Parse the documented `--format` values.
    #[must_use]
    pub fn parse(s: &str) -> Option<Format> {
        match s {
            "text" => Some(Format::Text),
            "pretty" => Some(Format::Pretty),
            "json" => Some(Format::Json),
            _ => None,
        }
    }
}

/// Render a report to a string in the chosen format.
#[must_use]
pub fn render(report: &Report, format: Format) -> String {
    match format {
        Format::Json => serde_json::to_string_pretty(&report.to_json())
            .unwrap_or_else(|e| {
                format!("{{\"error\":\"json render failed: {e}\"}}")
            }),
        Format::Text => report.to_text(),
        Format::Pretty => report.to_pretty(),
    }
}
