//! The shared report / finding model every `mess` subcommand speaks.
//!
//! A command runs a set of checks and produces a [`Report`]: a named data
//! collection (the command-specific payload) plus a flat list of typed
//! [`Finding`]s and an `advice` array (the CLI-conventions envelope, see
//! `.agents/edict/design/cli-conventions.md`). The report renders to three
//! audiences — `text` (agents/pipes), `pretty` (humans), and `json`
//! (machines, stable field names) — and computes the process exit code from
//! the most severe finding.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde_json::{Map, Value, json};

/// A finding's severity. The process exit code is derived from the most
/// severe finding across the whole report (see [`Report::exit_code`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// A passing check — informational, never affects the exit code.
    Ok,
    /// Neutral information (a fact worth surfacing, not a problem).
    Info,
    /// A soft problem: the store is usable but something needs attention.
    Warn,
    /// A hard problem: corruption, a failed integrity check, a broken
    /// invariant. Forces a non-zero exit.
    Error,
}

impl Severity {
    /// The stable lowercase token used in `text`/`json` output.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Ok => "ok",
            Severity::Info => "info",
            Severity::Warn => "warn",
            Severity::Error => "error",
        }
    }

    /// The bracketed glyph used in `pretty`/`text` summaries.
    #[must_use]
    pub fn tag(self) -> &'static str {
        match self {
            Severity::Ok => "[OK]",
            Severity::Info => "[INFO]",
            Severity::Warn => "[WARN]",
            Severity::Error => "[FAIL]",
        }
    }
}

/// One typed check result. `kind` is a stable kebab-case identifier for
/// programmatic matching (e.g. `log-batch-corrupt`, `sidecar-crc`,
/// `retention-blocked`); `fields` carries the structured detail the JSON
/// consumer needs. `message` is the one-line human/agent rendering.
#[derive(Debug, Clone)]
pub struct Finding {
    pub severity: Severity,
    /// Which check produced this (e.g. `lock`, `segment-scan`, `sidecar`).
    pub check:    String,
    /// Stable kebab-case finding type for programmatic matching.
    pub kind:     String,
    /// One-line human/agent message.
    pub message:  String,
    /// Structured detail (stable field names).
    pub fields:   Map<String, Value>,
}

impl Finding {
    #[must_use]
    pub fn new(
        severity: Severity,
        check: impl Into<String>,
        kind: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Finding {
            severity,
            check: check.into(),
            kind: kind.into(),
            message: message.into(),
            fields: Map::new(),
        }
    }

    /// Attach a structured field (builder form).
    #[must_use]
    pub fn with(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.fields.insert(key.to_string(), value.into());
        self
    }

    fn to_json(&self) -> Value {
        let mut obj = Map::new();
        obj.insert("severity".into(), json!(self.severity.as_str()));
        obj.insert("check".into(), json!(self.check));
        obj.insert("kind".into(), json!(self.kind));
        obj.insert("message".into(), json!(self.message));
        for (k, v) in &self.fields {
            obj.insert(k.clone(), v.clone());
        }
        Value::Object(obj)
    }

    /// Token-efficient one-line text form: `severity  kind  check  message`.
    fn to_text_line(&self) -> String {
        format!(
            "{}  {}  {}  {}",
            self.severity.as_str(),
            self.kind,
            self.check,
            self.message
        )
    }
}

/// A command's full result: the named data payload, the findings list, and
/// the advice array (CLI-conventions envelope).
#[derive(Debug, Clone)]
pub struct Report {
    /// The command name (`doctor`, `verify`, ...), used as a header in
    /// pretty/text output and never in the JSON envelope keys.
    pub command:        String,
    /// The named data collection key (`checks`, `segments`, `verdicts`, ...).
    pub collection_key: String,
    /// The command-specific structured payload (rows of the collection).
    pub collection:     Vec<Value>,
    /// Extra top-level scalar/object fields folded into every envelope
    /// (e.g. `dir`, `summary`).
    pub extra:          BTreeMap<String, Value>,
    /// The typed findings.
    pub findings:       Vec<Finding>,
    /// Advisory notes (kebab `type`), CLI-conventions `advice` array.
    pub advice:         Vec<Value>,
    /// Display caps applied ONLY when rendering `text`/`pretty` (`json`
    /// always carries the complete data — see the backward-compatibility
    /// note on [`Report::to_json`]). Keyed by dotted path from the top of
    /// `extra` (`"stream_heads"`, `"registry.stream_names"`, ...); a path
    /// with no entry renders in full. Set via [`Report::limit_display`]; a
    /// command uses this for a section that can grow unboundedly at app
    /// scale (e.g. `inspect`'s per-stream listings) without truncating the
    /// machine-readable payload.
    pub display_limits: BTreeMap<String, usize>,
}

impl Report {
    #[must_use]
    pub fn new(
        command: impl Into<String>,
        collection_key: impl Into<String>,
    ) -> Self {
        Report {
            command:        command.into(),
            collection_key: collection_key.into(),
            collection:     Vec::new(),
            extra:          BTreeMap::new(),
            findings:       Vec::new(),
            advice:         Vec::new(),
            display_limits: BTreeMap::new(),
        }
    }

    pub fn push_row(&mut self, row: Value) { self.collection.push(row); }

    pub fn push_finding(&mut self, f: Finding) { self.findings.push(f); }

    pub fn set(&mut self, key: &str, value: impl Into<Value>) {
        self.extra.insert(key.to_string(), value.into());
    }

    pub fn advise(&mut self, kind: &str, message: &str) {
        self.advice
            .push(json!({ "level": "warn", "type": kind, "message": message }));
    }

    /// Cap how many array items a `text`/`pretty` render shows before it
    /// truncates with a `... and K more` line, for the array at `path` — a
    /// top-level `extra` key (`"stream_heads"`) or a dotted path into a
    /// nested object (`"registry.stream_names"`). Has no effect on
    /// `to_json`, which always carries the complete array — this only bounds
    /// the human/agent-facing render of a section that can grow unboundedly
    /// (e.g. `inspect`'s per-stream listings at app scale).
    pub fn limit_display(&mut self, path: &str, max: usize) {
        self.display_limits.insert(path.to_string(), max);
    }

    /// The most severe finding severity, or `Ok` when there are none.
    #[must_use]
    pub fn worst(&self) -> Severity {
        self.findings.iter().map(|f| f.severity).max().unwrap_or(Severity::Ok)
    }

    /// Exit code: `0` clean, `EXIT_FINDINGS` when any `Error` finding is
    /// present. `Warn`-only reports still exit `0` (a warning is not a
    /// failure), matching the doctor convention that only hard failures are
    /// non-zero.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        if self.worst() >= Severity::Error { EXIT_FINDINGS } else { EXIT_OK }
    }

    /// The full JSON envelope: the named collection, folded-in extras, the
    /// findings array, and the `advice` array (always present).
    #[must_use]
    pub fn to_json(&self) -> Value {
        let mut obj = Map::new();
        for (k, v) in &self.extra {
            obj.insert(k.clone(), v.clone());
        }
        obj.insert(
            self.collection_key.clone(),
            Value::Array(self.collection.clone()),
        );
        obj.insert(
            "findings".into(),
            Value::Array(self.findings.iter().map(Finding::to_json).collect()),
        );
        obj.insert("advice".into(), Value::Array(self.advice.clone()));
        obj.insert(
            "summary".into(),
            json!({
                "worst": self.worst().as_str(),
                "findings": self.findings.len(),
                "errors": self.findings.iter().filter(|f| f.severity == Severity::Error).count(),
                "warnings": self.findings.iter().filter(|f| f.severity == Severity::Warn).count(),
            }),
        );
        Value::Object(obj)
    }

    /// Concise, token-efficient text: findings, the rows of the collection
    /// (already ID-first Values rendered compactly), then every `extra`
    /// section in readable `key: value`/table form, then advisories. Every
    /// field a command puts in `extra` (dir, lock, metrics, registry, ...)
    /// must be visible here — `text` is the piped/agent default, so an
    /// extra section that only showed up in `json` was effectively invisible
    /// to that audience.
    #[must_use]
    pub fn to_text(&self) -> String {
        let mut out = String::new();
        for f in &self.findings {
            out.push_str(&f.to_text_line());
            out.push('\n');
        }
        for row in &self.collection {
            out.push_str(&row.to_string());
            out.push('\n');
        }
        for (k, v) in &self.extra {
            render_extra_entry(&mut out, k, v, "", k, &self.display_limits);
        }
        for a in &self.advice {
            let _ = writeln!(
                out,
                "advice  {}  {}",
                a.get("type").and_then(Value::as_str).unwrap_or(""),
                a.get("message").and_then(Value::as_str).unwrap_or(""),
            );
        }
        out
    }

    /// Human pretty rendering: a header with every `extra` section in
    /// readable form, tagged findings, advisories, then the collection rows.
    #[must_use]
    pub fn to_pretty(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("mess {}\n", self.command));
        for (k, v) in &self.extra {
            render_extra_entry(&mut out, k, v, "  ", k, &self.display_limits);
        }
        out.push('\n');
        for f in &self.findings {
            out.push_str(&format!(
                "{} {}: {}\n",
                f.severity.tag(),
                f.kind,
                f.message
            ));
        }
        if !self.advice.is_empty() {
            out.push('\n');
            for a in &self.advice {
                out.push_str(&format!(
                    "[ADVICE] {}: {}\n",
                    a.get("type").and_then(Value::as_str).unwrap_or(""),
                    a.get("message").and_then(Value::as_str).unwrap_or(""),
                ));
            }
        }
        if !self.collection.is_empty() {
            out.push('\n');
            for row in &self.collection {
                out.push_str(&format!("  {row}\n"));
            }
        }
        let w = self.worst();
        out.push('\n');
        out.push_str(&format!(
            "{} {} finding(s); worst = {}\n",
            w.tag(),
            self.findings.len(),
            w.as_str()
        ));
        out
    }
}

// --- extra-section rendering (text/pretty) ---------------------------------
//
// A generic, format-agnostic renderer for `Report::extra` values: scalars as
// `key: value`, empty/scalar arrays inline, arrays of objects as a small
// ID-first table (capped by `Report::display_limits`), objects recursed one
// level. Shared by `to_text` and `to_pretty` (`indent` is `""` for text,
// `"  "` for pretty, which nests everything else two spaces deeper).

/// Render one `extra` entry at `path` (the dotted path from the top of
/// `extra`, e.g. `"registry.stream_names"` — looked up in `limits` to cap an
/// object-array's shown rows; scalar-only arrays are always a single compact
/// line regardless of any limit).
fn render_extra_entry(
    out: &mut String,
    key: &str,
    value: &Value,
    indent: &str,
    path: &str,
    limits: &BTreeMap<String, usize>,
) {
    match value {
        Value::Null => {
            let _ = writeln!(out, "{indent}{key}: -");
        }
        Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            let _ = writeln!(out, "{indent}{key}: {}", scalar_str(value));
        }
        Value::Array(items) => render_extra_array(
            out,
            key,
            items,
            indent,
            limits.get(path).copied(),
        ),
        Value::Object(map) => {
            let _ = writeln!(out, "{indent}{key}:");
            let child_indent = format!("{indent}  ");
            for (k, v) in map {
                let child_path = format!("{path}.{k}");
                render_extra_entry(
                    out,
                    k,
                    v,
                    &child_indent,
                    &child_path,
                    limits,
                );
            }
        }
    }
}

fn render_extra_array(
    out: &mut String,
    key: &str,
    items: &[Value],
    indent: &str,
    limit: Option<usize>,
) {
    if items.is_empty() {
        let _ = writeln!(out, "{indent}{key}: (none)");
        return;
    }
    if items.iter().all(is_scalar) {
        let joined =
            items.iter().map(scalar_str).collect::<Vec<_>>().join(", ");
        let _ = writeln!(out, "{indent}{key}: {joined}");
        return;
    }
    let total = items.len();
    let shown = limit.map_or(total, |l| l.min(total));
    let _ = writeln!(out, "{indent}{key} ({total}):");
    let child_indent = format!("{indent}  ");
    for item in &items[..shown] {
        match item {
            Value::Object(obj) => {
                let _ = writeln!(out, "{child_indent}{}", render_row(obj));
            }
            other => {
                let _ = writeln!(out, "{child_indent}{}", scalar_str(other));
            }
        }
    }
    if shown < total {
        let _ = writeln!(
            out,
            "{child_indent}... and {} more (showing {shown} of {total}; see \
             --format json or a filter flag for the rest)",
            total - shown
        );
    }
}

fn is_scalar(v: &Value) -> bool {
    !matches!(v, Value::Array(_) | Value::Object(_))
}

fn scalar_str(v: &Value) -> String {
    match v {
        Value::Null => "-".to_string(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// One table row for an array-of-objects `extra` section: `k=v` pairs,
/// two-space delimited, with common id-shaped fields sorted first (ID-first,
/// per the CLI text-format convention) and the rest alphabetical (`Map`'s
/// natural key order).
fn render_row(obj: &Map<String, Value>) -> String {
    const PRIORITY: [&str; 5] =
        ["stream_id", "segment_id", "event_type_id", "id", "name"];
    let mut keys: Vec<&String> = obj.keys().collect();
    keys.sort_by_key(|k| {
        let p = PRIORITY
            .iter()
            .position(|&pk| pk == k.as_str())
            .unwrap_or(PRIORITY.len());
        (p, k.as_str())
    });
    keys.into_iter()
        .map(|k| format!("{k}={}", scalar_str(&obj[k])))
        .collect::<Vec<_>>()
        .join("  ")
}

/// Clean exit.
pub const EXIT_OK: i32 = 0;
/// Usage error (clap owns this; documented for callers).
pub const EXIT_USAGE: i32 = 1;
/// System error (I/O, permissions, could-not-open).
pub const EXIT_SYSTEM: i32 = 2;
/// The command completed but reported `Error`-severity findings.
pub const EXIT_FINDINGS: i32 = 3;
