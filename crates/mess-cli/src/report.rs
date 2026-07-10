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
    pub check: String,
    /// Stable kebab-case finding type for programmatic matching.
    pub kind: String,
    /// One-line human/agent message.
    pub message: String,
    /// Structured detail (stable field names).
    pub fields: Map<String, Value>,
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
        format!("{}  {}  {}  {}", self.severity.as_str(), self.kind, self.check, self.message)
    }
}

/// A command's full result: the named data payload, the findings list, and
/// the advice array (CLI-conventions envelope).
#[derive(Debug, Clone)]
pub struct Report {
    /// The command name (`doctor`, `verify`, ...), used as a header in
    /// pretty/text output and never in the JSON envelope keys.
    pub command: String,
    /// The named data collection key (`checks`, `segments`, `verdicts`, ...).
    pub collection_key: String,
    /// The command-specific structured payload (rows of the collection).
    pub collection: Vec<Value>,
    /// Extra top-level scalar/object fields folded into every envelope
    /// (e.g. `dir`, `summary`).
    pub extra: BTreeMap<String, Value>,
    /// The typed findings.
    pub findings: Vec<Finding>,
    /// Advisory notes (kebab `type`), CLI-conventions `advice` array.
    pub advice: Vec<Value>,
}

impl Report {
    #[must_use]
    pub fn new(command: impl Into<String>, collection_key: impl Into<String>) -> Self {
        Report {
            command: command.into(),
            collection_key: collection_key.into(),
            collection: Vec::new(),
            extra: BTreeMap::new(),
            findings: Vec::new(),
            advice: Vec::new(),
        }
    }

    pub fn push_row(&mut self, row: Value) {
        self.collection.push(row);
    }

    pub fn push_finding(&mut self, f: Finding) {
        self.findings.push(f);
    }

    pub fn set(&mut self, key: &str, value: impl Into<Value>) {
        self.extra.insert(key.to_string(), value.into());
    }

    pub fn advise(&mut self, kind: &str, message: &str) {
        self.advice.push(json!({ "level": "warn", "type": kind, "message": message }));
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
        obj.insert(self.collection_key.clone(), Value::Array(self.collection.clone()));
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

    /// Concise, token-efficient text: one finding per line, then the rows of
    /// the collection (already ID-first Values rendered compactly).
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
        out
    }

    /// Human pretty rendering: a header, tagged findings, then the rows.
    #[must_use]
    pub fn to_pretty(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("mess {}\n", self.command));
        for (k, v) in &self.extra {
            out.push_str(&format!("  {k}: {v}\n"));
        }
        out.push('\n');
        for f in &self.findings {
            out.push_str(&format!("{} {}: {}\n", f.severity.tag(), f.kind, f.message));
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

/// Clean exit.
pub const EXIT_OK: i32 = 0;
/// Usage error (clap owns this; documented for callers).
pub const EXIT_USAGE: i32 = 1;
/// System error (I/O, permissions, could-not-open).
pub const EXIT_SYSTEM: i32 = 2;
/// The command completed but reported `Error`-severity findings.
pub const EXIT_FINDINGS: i32 = 3;
