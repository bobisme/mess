//! Committed golden fixtures for evolution and fold-drift safety (bn-3ui).
//!
//! Two failure modes that are otherwise *silent at runtime* are made **loud
//! at CI time** by golden tests whose fixtures are committed to the repo:
//!
//! 1. **fixture-compat** ([`check_fixture_compat`]): bytes written by an
//!    older version of the code must decode *forever*. Renaming or removing a
//!    field, or changing its type, without an upcaster breaks old payloads —
//!    a defect that would otherwise only surface when a real old event is
//!    replayed in production. See `docs/spec/05-fold-certificates.md` and the
//!    `spikes/codec_bakeoff` evolution matrix.
//! 2. **fold-drift** ([`check_fold_drift`]): fixture events fold to a pinned
//!    expected state. Changing `apply` semantics without bumping
//!    `#[aggregate(fold_version = N)]` silently invalidates every stored
//!    snapshot (D4); the golden test catches it with a
//!    *"bump `fold_version` or fix your fold"* message. See spec §9 and the
//!    `spikes/fold_cert` cfg-flagged drift demo.
//!
//! # Snapshot-test ergonomics, committed fixtures
//!
//! Both checks **scaffold** their fixture file on first run and **assert**
//! against it thereafter. Unlike an inline snapshot library, the fixture is a
//! plain committed file that is reviewed in the PR that introduces it and is
//! never regenerated silently:
//!
//! - If the fixture file is **missing**, the check fails and tells you to run
//!   with `UPDATE_FIXTURES=1` to scaffold it.
//! - With `UPDATE_FIXTURES=1` set, the check (re)writes the fixture **and
//!   still fails** — scaffolding is never a green run, so a freshly generated
//!   or regenerated fixture always shows up as a red test that must be
//!   reviewed and committed before the suite goes green.
//! - With the fixture present and no env var, the check asserts and passes
//!   only when the committed bytes/state still match.
//!
//! # Why testkit helpers rather than a proc-macro
//!
//! These are ordinary functions, not `#[derive]` output, for two reasons.
//! First, the fixture *lifecycle* (locating the file under the consuming
//! crate, scaffolding, refusing to pass on scaffold) is runtime filesystem
//! work, not code generation. Second — and decisively — the acceptance
//! criteria require driving the **failure** paths directly: calling the check
//! against a deliberately-drifted fold, or feeding old bytes to a
//! field-renamed type, and asserting the actionable error. A function that
//! returns `Result<(), FixtureError>` is unit-testable in exactly that way; a
//! macro that expands to a `#[test]` which panics is not. The `#[derive]`
//! layer's only role here is to surface `A::FOLD_VERSION`
//! (`crates/mess-derive`); the drift/compat machinery lives here.

use std::fmt::Debug;
use std::path::{Path, PathBuf};

use mess_core::Event;

/// Environment variable that opts in to (re)writing fixture files.
pub const UPDATE_ENV: &str = "UPDATE_FIXTURES";

/// The outcome of a golden-fixture check that did not pass.
///
/// Every variant renders (via [`Display`](std::fmt::Display)) an actionable,
/// operator-facing message. The variants are matched directly by the negative
/// acceptance tests, so their identity is part of this module's contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FixtureError {
    /// The fixture file does not exist and `UPDATE_FIXTURES` was not set.
    Missing { path: PathBuf, kind: &'static str },
    /// The fixture file was just (re)written because `UPDATE_FIXTURES` was
    /// set. The check deliberately fails so scaffolding is never a green run.
    Scaffolded { path: PathBuf, kind: &'static str },
    /// The committed fixture file is malformed / unparseable.
    Corrupt { path: PathBuf, detail: String },
    /// `apply` semantics drifted: fixture events fold to a different state
    /// than the pinned one, while `fold_version` was left unchanged.
    FoldDrift {
        aggregate: String,
        fold_version: u32,
        expected_state: String,
        actual_state: String,
    },
    /// `A::FOLD_VERSION` differs from the version pinned in the fixture. A
    /// legitimate bump must regenerate the fixture (`UPDATE_FIXTURES=1`).
    FoldVersionChanged { aggregate: String, pinned: u32, current: u32 },
    /// Committed old bytes no longer decode under the current type: a
    /// breaking wire change (renamed/removed/retyped field) with no upcaster.
    CompatDecodeFailed { label: String, wire_name: String, source: String },
    /// Committed bytes still decode, but to a different value than pinned —
    /// a silent semantic change in what those bytes *mean*.
    CompatValueDrift { label: String, expected: String, actual: String },
    /// The live sample set no longer lines up with the committed fixture
    /// (e.g. a variant was added/removed). Regenerate to re-pin.
    CompatShapeChanged { detail: String },
    /// The fixture file could not be read or written.
    Io { path: PathBuf, detail: String },
}

impl std::fmt::Display for FixtureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FixtureError::Missing { path, kind } => write!(
                f,
                "{kind} fixture is missing: {}\n\
                 => run with `{UPDATE_ENV}=1` to scaffold it, then review and \
                 commit the generated file.",
                path.display()
            ),
            FixtureError::Scaffolded { path, kind } => write!(
                f,
                "{kind} fixture was (re)generated at {}\n\
                 => this run fails on purpose: review the generated file and \
                 commit it. Fixtures are never regenerated silently.",
                path.display()
            ),
            FixtureError::Corrupt { path, detail } => write!(
                f,
                "fixture file is corrupt: {} ({detail})\n\
                 => fix it by hand or regenerate with `{UPDATE_ENV}=1`.",
                path.display()
            ),
            FixtureError::FoldDrift {
                aggregate,
                fold_version,
                expected_state,
                actual_state,
            } => write!(
                f,
                "FOLD DRIFT DETECTED for `{aggregate}` (fold_version = \
                 {fold_version}):\n  \
                 expected state: {expected_state}\n  \
                 actual state:   {actual_state}\n\
                 apply() semantics changed but fold_version did not.\n\
                 => bump `#[aggregate(fold_version = N)]` (invalidating \
                 existing snapshots) or fix your fold."
            ),
            FixtureError::FoldVersionChanged { aggregate, pinned, current } => {
                write!(
                    f,
                    "FOLD_VERSION for `{aggregate}` changed {pinned} -> \
                     {current}: this is the legitimate bump path.\n\
                     => regenerate the golden fixture with `{UPDATE_ENV}=1` so \
                     it re-pins to the new version and state."
                )
            }
            FixtureError::CompatDecodeFailed { label, wire_name, source } => {
                write!(
                    f,
                    "FIXTURE-COMPAT BROKEN for `{label}` (wire \
                     `{wire_name}`): committed bytes no longer decode under \
                     the current type:\n  {source}\n\
                     => you made a breaking wire change (renamed/removed/\
                     retyped a field) without an upcaster. Add an upcaster \
                     that reads the old shape, or — only if the old data is \
                     truly gone — regenerate with `{UPDATE_ENV}=1`."
                )
            }
            FixtureError::CompatValueDrift { label, expected, actual } => {
                write!(
                    f,
                    "FIXTURE-COMPAT DRIFT for `{label}`: committed bytes still \
                     decode, but to a different value:\n  \
                     expected: {expected}\n  actual:   {actual}\n\
                     => the meaning of stored bytes changed. Fix the decode \
                     path or regenerate with `{UPDATE_ENV}=1`."
                )
            }
            FixtureError::CompatShapeChanged { detail } => write!(
                f,
                "FIXTURE-COMPAT sample set changed: {detail}\n\
                 => a new event variant/sample is not covered by the committed \
                 fixture. Regenerate with `{UPDATE_ENV}=1` to re-pin."
            ),
            FixtureError::Io { path, detail } => {
                write!(f, "fixture I/O error at {}: {detail}", path.display())
            }
        }
    }
}

impl std::error::Error for FixtureError {}

/// Whether `UPDATE_FIXTURES` is set to a truthy (non-empty, non-`0`) value.
fn update_requested() -> bool {
    match std::env::var(UPDATE_ENV) {
        Ok(v) => !v.is_empty() && v != "0",
        Err(_) => false,
    }
}

fn write_fixture(path: &Path, contents: &str) -> Result<(), FixtureError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| FixtureError::Io {
            path: parent.to_path_buf(),
            detail: e.to_string(),
        })?;
    }
    std::fs::write(path, contents).map_err(|e| FixtureError::Io {
        path: path.to_path_buf(),
        detail: e.to_string(),
    })
}

// ---------------------------------------------------------------------------
// Fold-drift golden
// ---------------------------------------------------------------------------

/// Fold `events` into the aggregate's default state via its infallible
/// `apply`. The pure fold used by [`check_fold_drift`]; exposed so callers can
/// build the state string themselves (e.g. to drive the failure path with a
/// deliberately different fold).
pub fn fold<A>(events: &[A::Event]) -> A
where
    A: mess_core::Aggregate,
{
    let mut state = A::default();
    for e in events {
        state.apply(e);
    }
    state
}

/// Assert that folding a pinned event fixture still produces the pinned
/// state, and that `fold_version` still matches the pinned version.
///
/// `state_debug` is the `Debug` rendering of the folded aggregate state
/// (representation changes therefore also trip the check, per spec §9). It is
/// taken as a string — rather than folding internally — precisely so a test
/// can pass a *deliberately drifted* state to exercise the failure path.
///
/// This is the **canonical, regenerating** entry point: with `UPDATE_FIXTURES`
/// set it (re)writes the fixture and fails with [`FixtureError::Scaffolded`].
/// It is meant to back exactly one golden per fixture (the
/// [`fold_drift_golden!`] macro). Direct-call tests that share the same
/// fixture path must use the read-only [`assert_fold_drift`] so they never
/// race the canonical writer or clobber the golden with off-nominal data.
///
/// See the module docs for the scaffold / assert / `UPDATE_FIXTURES`
/// lifecycle.
pub fn check_fold_drift(
    aggregate: &str,
    fixture_path: impl AsRef<Path>,
    fold_version: u32,
    state_debug: &str,
) -> Result<(), FixtureError> {
    let path = fixture_path.as_ref();

    if update_requested() {
        let current = FoldFixture {
            aggregate: aggregate.to_string(),
            fold_version,
            state: state_debug.to_string(),
        };
        write_fixture(path, &current.serialize())?;
        return Err(FixtureError::Scaffolded {
            path: path.to_path_buf(),
            kind: "fold-drift",
        });
    }

    assert_fold_drift(aggregate, path, fold_version, state_debug)
}

/// Read-only twin of [`check_fold_drift`]: assert against the committed
/// fixture and **never write**, even under `UPDATE_FIXTURES`.
///
/// Use this from every test that shares a fixture path with the canonical
/// golden (positive re-proofs and failure-path drivers alike). Because it
/// never writes, any number of these can run concurrently with the single
/// regenerating [`check_fold_drift`] golden without tearing the file or
/// pinning off-nominal (drifted / bumped-version) data into it.
pub fn assert_fold_drift(
    aggregate: &str,
    fixture_path: impl AsRef<Path>,
    fold_version: u32,
    state_debug: &str,
) -> Result<(), FixtureError> {
    let path = fixture_path.as_ref();

    let Ok(raw) = std::fs::read_to_string(path) else {
        return Err(FixtureError::Missing {
            path: path.to_path_buf(),
            kind: "fold-drift",
        });
    };
    let pinned = FoldFixture::parse(&raw).map_err(|detail| {
        FixtureError::Corrupt { path: path.to_path_buf(), detail }
    })?;

    if pinned.fold_version != fold_version {
        return Err(FixtureError::FoldVersionChanged {
            aggregate: aggregate.to_string(),
            pinned: pinned.fold_version,
            current: fold_version,
        });
    }
    if pinned.state != state_debug {
        return Err(FixtureError::FoldDrift {
            aggregate: aggregate.to_string(),
            fold_version,
            expected_state: pinned.state,
            actual_state: state_debug.to_string(),
        });
    }
    Ok(())
}

struct FoldFixture {
    aggregate: String,
    fold_version: u32,
    state: String,
}

impl FoldFixture {
    fn serialize(&self) -> String {
        // Line-oriented header + verbatim (possibly multi-line) state body,
        // reviewable in a PR diff. The `---` sentinel separates them.
        format!(
            "# mess fold-drift golden fixture (bn-3ui) — DO NOT EDIT BY HAND.\n\
             # Regenerate with `{UPDATE_ENV}=1`; a bump must re-pin here.\n\
             aggregate: {}\n\
             fold_version: {}\n\
             ---\n\
             {}\n",
            self.aggregate, self.fold_version, self.state
        )
    }

    fn parse(raw: &str) -> Result<Self, String> {
        let (header, body) = raw
            .split_once("\n---\n")
            .ok_or("missing `---` state separator")?;
        let mut aggregate = None;
        let mut fold_version = None;
        for line in header.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, val) =
                line.split_once(':').ok_or_else(|| format!("bad line: {line}"))?;
            match key.trim() {
                "aggregate" => aggregate = Some(val.trim().to_string()),
                "fold_version" => {
                    fold_version = Some(
                        val.trim()
                            .parse::<u32>()
                            .map_err(|_| "fold_version not a u32".to_string())?,
                    )
                }
                other => return Err(format!("unknown key `{other}`")),
            }
        }
        Ok(FoldFixture {
            aggregate: aggregate.ok_or("missing `aggregate`")?,
            fold_version: fold_version.ok_or("missing `fold_version`")?,
            state: body.trim_end_matches('\n').to_string(),
        })
    }
}

// ---------------------------------------------------------------------------
// Fixture-compat golden
// ---------------------------------------------------------------------------

/// Assert that committed old bytes still decode under the current event type
/// `E`, and (once decoded) still mean the same thing.
///
/// `samples` are live values of the current type; on first run their encoded
/// bytes and `Debug` renderings are committed as the fixture. Thereafter the
/// **committed bytes** — not freshly encoded ones — are decoded with the
/// current `E::decode`, so a breaking wire change (a field rename without an
/// upcaster, say) fails loudly via [`FixtureError::CompatDecodeFailed`].
///
/// The samples are keyed by their wire name plus their ordinal among samples
/// sharing that name, so a fixture can pin several instances of the same
/// variant.
/// This is the **canonical, regenerating** entry point: with `UPDATE_FIXTURES`
/// set it (re)writes the fixture and fails with [`FixtureError::Scaffolded`].
/// It is meant to back exactly one golden per fixture (the
/// [`fixture_compat_golden!`] macro). Direct-call tests that share the same
/// fixture path must use the read-only [`assert_fixture_compat`] so they never
/// race the canonical writer or clobber the golden with off-nominal bytes
/// (e.g. a field-renamed sample driving the failure path).
pub fn check_fixture_compat<E>(
    fixture_path: impl AsRef<Path>,
    samples: &[E],
) -> Result<(), FixtureError>
where
    E: Event + Debug,
{
    let path = fixture_path.as_ref();
    let live = build_live_entries(samples)?;

    if update_requested() {
        write_fixture(path, &serialize_compat(&live))?;
        return Err(FixtureError::Scaffolded {
            path: path.to_path_buf(),
            kind: "fixture-compat",
        });
    }

    assert_compat_against_committed::<E>(path, &live)
}

/// Read-only twin of [`check_fixture_compat`]: decode the committed bytes with
/// the current type and **never write**, even under `UPDATE_FIXTURES`.
///
/// Use this from every test that shares a fixture path with the canonical
/// golden — including failure-path drivers that feed a field-renamed type. It
/// cannot scaffold, so it can run concurrently with the single regenerating
/// [`check_fixture_compat`] golden without tearing the file or overwriting the
/// committed old bytes with a wrong-shape sample.
pub fn assert_fixture_compat<E>(
    fixture_path: impl AsRef<Path>,
    samples: &[E],
) -> Result<(), FixtureError>
where
    E: Event + Debug,
{
    let path = fixture_path.as_ref();
    let live = build_live_entries(samples)?;
    assert_compat_against_committed::<E>(path, &live)
}

/// Build live entries (label, wire name, encoded bytes, decoded `Debug`) from
/// the current-type samples. Labels are the wire name plus a per-name ordinal.
fn build_live_entries<E>(samples: &[E]) -> Result<Vec<CompatEntry>, FixtureError>
where
    E: Event + Debug,
{
    let mut live: Vec<CompatEntry> = Vec::with_capacity(samples.len());
    let mut seen: std::collections::HashMap<&str, usize> =
        std::collections::HashMap::new();
    for s in samples {
        let wire = s.name();
        let ordinal = seen.entry(wire).or_insert(0);
        let label = format!("{wire}#{ordinal}");
        *ordinal += 1;
        let bytes = s.encode().map_err(|e| FixtureError::CompatDecodeFailed {
            label: label.clone(),
            wire_name: wire.to_string(),
            source: format!("current type failed to ENCODE the sample: {e}"),
        })?;
        live.push(CompatEntry {
            label,
            wire_name: wire.to_string(),
            payload_hex: to_hex(&bytes),
            decoded_debug: format!("{s:?}"),
        });
    }
    Ok(live)
}

/// Decode the committed fixture bytes with the current type and compare
/// against the pinned `Debug` renderings. Never writes.
fn assert_compat_against_committed<E>(
    path: &Path,
    live: &[CompatEntry],
) -> Result<(), FixtureError>
where
    E: Event + Debug,
{
    let Ok(raw) = std::fs::read_to_string(path) else {
        return Err(FixtureError::Missing {
            path: path.to_path_buf(),
            kind: "fixture-compat",
        });
    };
    let committed = parse_compat(&raw).map_err(|detail| {
        FixtureError::Corrupt { path: path.to_path_buf(), detail }
    })?;

    // The live label set must still match the committed one; a new/removed
    // sample means the pinned coverage no longer matches the code.
    let live_labels: Vec<&str> = live.iter().map(|e| e.label.as_str()).collect();
    let committed_labels: Vec<&str> =
        committed.iter().map(|e| e.label.as_str()).collect();
    if live_labels != committed_labels {
        return Err(FixtureError::CompatShapeChanged {
            detail: format!(
                "committed labels {committed_labels:?} but live samples are \
                 {live_labels:?}"
            ),
        });
    }

    // The heart of the check: decode the COMMITTED bytes with current code.
    for entry in &committed {
        let bytes = from_hex(&entry.payload_hex).map_err(|detail| {
            FixtureError::Corrupt { path: path.to_path_buf(), detail }
        })?;
        match E::decode(&entry.wire_name, &bytes) {
            Err(e) => {
                return Err(FixtureError::CompatDecodeFailed {
                    label: entry.label.clone(),
                    wire_name: entry.wire_name.clone(),
                    source: e.to_string(),
                });
            }
            Ok(decoded) => {
                let actual = format!("{decoded:?}");
                // Compare against what the committed bytes decoded to when
                // pinned. If the current code decodes those same bytes to a
                // different live Debug, that is genuine value drift.
                if actual != entry.decoded_debug {
                    return Err(FixtureError::CompatValueDrift {
                        label: entry.label.clone(),
                        expected: entry.decoded_debug.clone(),
                        actual,
                    });
                }
            }
        }
    }
    Ok(())
}

struct CompatEntry {
    label: String,
    wire_name: String,
    payload_hex: String,
    decoded_debug: String,
}

fn serialize_compat(entries: &[CompatEntry]) -> String {
    let mut out = String::new();
    out.push_str(
        "# mess fixture-compat golden fixture (bn-3ui) — DO NOT EDIT BY HAND.\n",
    );
    out.push_str(
        "# Committed old bytes MUST decode forever. Regenerate only with \
         UPDATE_FIXTURES=1.\n",
    );
    for e in entries {
        out.push_str("---\n");
        out.push_str(&format!("label: {}\n", e.label));
        out.push_str(&format!("wire: {}\n", e.wire_name));
        out.push_str(&format!("payload_hex: {}\n", e.payload_hex));
        // Debug can be multi-line in principle; keep it single-line here since
        // event payloads render on one line, and pin it verbatim.
        out.push_str(&format!("decoded: {}\n", e.decoded_debug));
    }
    out
}

fn parse_compat(raw: &str) -> Result<Vec<CompatEntry>, String> {
    let mut entries = Vec::new();
    for block in raw.split("---\n").skip(1) {
        let mut label = None;
        let mut wire = None;
        let mut payload_hex = None;
        let mut decoded = None;
        for line in block.lines() {
            if line.trim().is_empty() || line.starts_with('#') {
                continue;
            }
            let (k, v) =
                line.split_once(':').ok_or_else(|| format!("bad line: {line}"))?;
            match k.trim() {
                "label" => label = Some(v.trim().to_string()),
                "wire" => wire = Some(v.trim().to_string()),
                "payload_hex" => payload_hex = Some(v.trim().to_string()),
                "decoded" => decoded = Some(v.trim().to_string()),
                other => return Err(format!("unknown key `{other}`")),
            }
        }
        entries.push(CompatEntry {
            label: label.ok_or("missing `label`")?,
            wire_name: wire.ok_or("missing `wire`")?,
            payload_hex: payload_hex.ok_or("missing `payload_hex`")?,
            decoded_debug: decoded.ok_or("missing `decoded`")?,
        });
    }
    Ok(entries)
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

fn from_hex(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err("odd-length hex string".to_string());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|_| format!("bad hex byte at {i}"))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Ergonomic wrappers: generate the `#[test]` that drives a golden check.
// ---------------------------------------------------------------------------

/// Generate a `#[test]` asserting the fold-drift golden for an aggregate.
///
/// ```ignore
/// mess_testkit::fold_drift_golden!(
///     account_fold_drift,           // test fn name
///     Account,                      // aggregate type (needs A::FOLD_VERSION)
///     concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/account.fold"),
///     account_fixture_events(),     // Vec<Account::Event>
/// );
/// ```
///
/// The aggregate type must expose `FOLD_VERSION` — `#[derive(Aggregate)]`
/// with `#[aggregate(fold_version = N)]` emits exactly that inherent const.
#[macro_export]
macro_rules! fold_drift_golden {
    ($name:ident, $agg:ty, $fixture:expr, $events:expr $(,)?) => {
        #[test]
        fn $name() {
            let events = $events;
            let state = $crate::fold::<$agg>(&events);
            let result = $crate::check_fold_drift(
                ::core::stringify!($agg),
                $fixture,
                <$agg>::FOLD_VERSION,
                &::std::format!("{state:?}"),
            );
            if let ::core::result::Result::Err(e) = result {
                ::core::panic!("{e}");
            }
        }
    };
}

/// Generate a `#[test]` asserting the fixture-compat golden for an event type.
///
/// ```ignore
/// mess_testkit::fixture_compat_golden!(
///     account_event_compat,
///     AccountEvent,
///     concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/account.compat"),
///     vec![AccountEvent::Opened { owner: "alice".into() }, /* ... */],
/// );
/// ```
#[macro_export]
macro_rules! fixture_compat_golden {
    ($name:ident, $event_ty:ty, $fixture:expr, $samples:expr $(,)?) => {
        #[test]
        fn $name() {
            let samples: ::std::vec::Vec<$event_ty> = $samples;
            let result =
                $crate::check_fixture_compat::<$event_ty>($fixture, &samples);
            if let ::core::result::Result::Err(e) = result {
                ::core::panic!("{e}");
            }
        }
    };
}
