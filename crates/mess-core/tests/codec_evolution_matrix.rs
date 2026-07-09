//! Locks in the decisive evolution-matrix findings from
//! `spikes/codec_bakeoff` so a dependency bump that changes codec behavior
//! fails loudly, and so the rationale for choosing `codec_id 1` =
//! msgpack-named lives next to the code that implements it.
//!
//! Ported from `spikes/codec_bakeoff/src/codecs.rs` +
//! `spikes/codec_bakeoff/src/evolution.rs` +
//! `spikes/codec_bakeoff/tests/evolution_matrix.rs`. The disqualified
//! codecs (postcard, bincode, msgpack-compact) are dev-dependencies here
//! purely to prove, byte-for-byte, why they were rejected — mess-core's
//! production codec layer (`src/codec/msgpack.rs`) implements only
//! `codec_id 1` (msgpack-named).
//!
//! Cells:
//!   Ok           — decode succeeded AND values match semantic expectation
//!   Error        — decode failed loudly (acceptable: caller sees a problem)
//!   SilentWrong  — decode succeeded but data is garbage/swapped (disqualifying)

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

// -------------------------------------------------------------- candidates

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Candidate {
    Json,
    Cbor,
    MsgpackNamed,
    MsgpackCompact,
    Postcard,
    Bincode,
}

const ALL_CANDIDATES: [Candidate; 6] = [
    Candidate::Json,
    Candidate::Cbor,
    Candidate::MsgpackNamed,
    Candidate::MsgpackCompact,
    Candidate::Postcard,
    Candidate::Bincode,
];

impl Candidate {
    fn name(self) -> &'static str {
        match self {
            Candidate::Json => "json",
            Candidate::Cbor => "cbor",
            Candidate::MsgpackNamed => "msgpack-named",
            Candidate::MsgpackCompact => "msgpack-compact",
            Candidate::Postcard => "postcard",
            Candidate::Bincode => "bincode",
        }
    }

    fn encode<T: Serialize>(self, v: &T) -> Vec<u8> {
        match self {
            Candidate::Json => serde_json::to_vec(v).expect("json encode"),
            Candidate::Cbor => {
                let mut buf = Vec::new();
                ciborium::into_writer(v, &mut buf).expect("cbor encode");
                buf
            }
            Candidate::MsgpackNamed => {
                rmp_serde::to_vec_named(v).expect("msgpack-named encode")
            }
            Candidate::MsgpackCompact => {
                rmp_serde::to_vec(v).expect("msgpack-compact encode")
            }
            Candidate::Postcard => {
                postcard::to_stdvec(v).expect("postcard encode")
            }
            Candidate::Bincode => {
                bincode::serialize(v).expect("bincode encode")
            }
        }
    }

    fn decode<T: DeserializeOwned>(self, bytes: &[u8]) -> Result<T, String> {
        match self {
            Candidate::Json => {
                serde_json::from_slice(bytes).map_err(|e| e.to_string())
            }
            Candidate::Cbor => {
                ciborium::from_reader(bytes).map_err(|e| e.to_string())
            }
            Candidate::MsgpackNamed | Candidate::MsgpackCompact => {
                rmp_serde::from_slice(bytes).map_err(|e| e.to_string())
            }
            Candidate::Postcard => {
                postcard::from_bytes(bytes).map_err(|e| e.to_string())
            }
            Candidate::Bincode => {
                bincode::deserialize(bytes).map_err(|e| e.to_string())
            }
        }
    }
}

// -------------------------------------------------------------------- Cell

#[derive(Clone, PartialEq, Debug)]
enum Cell {
    Ok,
    Error(String),
    SilentWrong(String),
}

/// Encode `v1` with `candidate`, decode as `V2`, compare against
/// `expected`.
fn check<V1, V2>(candidate: Candidate, v1: &V1, expected: &V2) -> Cell
where
    V1: Serialize,
    V2: DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let bytes = candidate.encode(v1);
    match candidate.decode::<V2>(&bytes) {
        Ok(got) if got == *expected => Cell::Ok,
        Ok(got) => {
            Cell::SilentWrong(format!("got {got:?}, expected {expected:?}"))
        }
        Err(e) => {
            let mut e = e.replace('\n', " ");
            e.truncate(90);
            Cell::Error(e)
        }
    }
}

// -------------------------------------------------------------- V1 baseline

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct V1 {
    id: u32,
    name: String,
    count: u32,
    active: bool,
}

fn v1_sample() -> V1 {
    V1 { id: 7, name: "alice".into(), count: 42, active: true }
}

// (a) added Option field (no serde attribute; serde treats missing Option as None)
#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct V2AddedOption {
    id: u32,
    name: String,
    count: u32,
    active: bool,
    note: Option<String>,
}

// (b) added field with #[serde(default)]
#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct V2AddedDefault {
    id: u32,
    name: String,
    count: u32,
    active: bool,
    #[serde(default)]
    retries: u32,
}

// (c) removed field (`count` dropped)
#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct V2Removed {
    id: u32,
    name: String,
    active: bool,
}

// (d) renamed field (`name` -> `title`, no #[serde(rename)])
#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct V2Renamed {
    id: u32,
    title: String,
    count: u32,
    active: bool,
}

// (e1) reordered fields, adjacent same-typed swap — THE trap case for
//      positional codecs: every stream position still type-checks, so the
//      decode succeeds with width/height silently swapped.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct V1Dims {
    id: u32,
    name: String,
    width: u32,
    height: u32,
    active: bool,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct V2DimsSwapped {
    id: u32,
    name: String,
    height: u32, // declaration order swapped vs V1Dims
    width: u32,
    active: bool,
}

// (e2) reordered fields, mixed types moved — positional codecs now read a
//      String where a u32 was, etc. Usually errors (by luck, not by design).
#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct V2ReorderedMixed {
    count: u32,
    id: u32,
    name: String,
    active: bool,
}

// (h) int widened u32 -> u64 (`count`)
#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct V2Widened {
    id: u32,
    name: String,
    count: u64,
    active: bool,
}

// (f)/(g) enum scenarios
#[derive(Serialize, Deserialize, PartialEq, Debug)]
enum EnumV1 {
    Created(u32),
    Deleted(u32),
}

// (f) added variant at the end
#[derive(Serialize, Deserialize, PartialEq, Debug)]
enum EnumV2Added {
    Created(u32),
    Deleted(u32),
    Archived(u32),
}

// (g) reordered variants (same payload type — the trap case)
#[derive(Serialize, Deserialize, PartialEq, Debug)]
enum EnumV2Reordered {
    Deleted(u32),
    Created(u32),
}

struct ScenarioResult {
    label: &'static str,
    cells: Vec<(Candidate, Cell)>,
}

fn run_matrix() -> Vec<ScenarioResult> {
    let run =
        |label: &'static str, f: &dyn Fn(Candidate) -> Cell| ScenarioResult {
            label,
            cells: ALL_CANDIDATES.iter().map(|&c| (c, f(c))).collect(),
        };

    vec![
        run("(a) added Option field", &|c| {
            check(
                c,
                &v1_sample(),
                &V2AddedOption {
                    id: 7,
                    name: "alice".into(),
                    count: 42,
                    active: true,
                    note: None,
                },
            )
        }),
        run("(b) added field w/ serde default", &|c| {
            check(
                c,
                &v1_sample(),
                &V2AddedDefault {
                    id: 7,
                    name: "alice".into(),
                    count: 42,
                    active: true,
                    retries: 0,
                },
            )
        }),
        run("(c) removed field", &|c| {
            check(
                c,
                &v1_sample(),
                &V2Removed { id: 7, name: "alice".into(), active: true },
            )
        }),
        run("(d) renamed field (no serde rename)", &|c| {
            // "Correct" here means the old value survives under the new name.
            check(
                c,
                &v1_sample(),
                &V2Renamed {
                    id: 7,
                    title: "alice".into(),
                    count: 42,
                    active: true,
                },
            )
        }),
        run("(e1) reordered fields (same-typed adjacent swap)", &|c| {
            check(
                c,
                &V1Dims {
                    id: 7,
                    name: "alice".into(),
                    width: 1920,
                    height: 1080,
                    active: true,
                },
                &V2DimsSwapped {
                    id: 7,
                    name: "alice".into(),
                    height: 1080,
                    width: 1920,
                    active: true,
                },
            )
        }),
        run("(e2) reordered fields (mixed types moved)", &|c| {
            check(
                c,
                &v1_sample(),
                &V2ReorderedMixed {
                    count: 42,
                    id: 7,
                    name: "alice".into(),
                    active: true,
                },
            )
        }),
        run("(f) enum: variant added at end", &|c| {
            check(c, &EnumV1::Deleted(9), &EnumV2Added::Deleted(9))
        }),
        run("(g) enum: variants reordered", &|c| {
            check(c, &EnumV1::Created(9), &EnumV2Reordered::Created(9))
        }),
        run("(h) int widened u32 -> u64", &|c| {
            check(
                c,
                &v1_sample(),
                &V2Widened {
                    id: 7,
                    name: "alice".into(),
                    count: 42,
                    active: true,
                },
            )
        }),
    ]
}

fn cell(
    results: &[ScenarioResult],
    label_prefix: &str,
    candidate: Candidate,
) -> Cell {
    let row = results
        .iter()
        .find(|r| r.label.starts_with(label_prefix))
        .unwrap_or_else(|| panic!("no scenario {label_prefix}"));
    row.cells
        .iter()
        .find(|(c, _)| *c == candidate)
        .map(|(_, cell)| cell.clone())
        .unwrap()
}

// --- The disqualifying cells: positional codecs silently corrupt data. ---

#[test]
fn positional_codecs_silently_swap_same_typed_reordered_fields() {
    let results = run_matrix();
    for candidate in
        [Candidate::Postcard, Candidate::Bincode, Candidate::MsgpackCompact]
    {
        let c = cell(&results, "(e1)", candidate);
        assert!(
            matches!(c, Cell::SilentWrong(_)),
            "{}: got {c:?}",
            candidate.name()
        );
    }
}

#[test]
fn index_encoded_enums_silently_swap_reordered_variants() {
    let results = run_matrix();
    // rmp-serde encodes variant NAMES even in compact mode, so only
    // postcard and bincode (u32 variant indices) hit this trap.
    for candidate in [Candidate::Postcard, Candidate::Bincode] {
        let c = cell(&results, "(g)", candidate);
        assert!(
            matches!(c, Cell::SilentWrong(_)),
            "{}: got {c:?}",
            candidate.name()
        );
    }
    // Verified empirically: variant-by-name saves msgpack-compact here.
    assert!(matches!(
        cell(&results, "(g)", Candidate::MsgpackCompact),
        Cell::Ok
    ));
}

// --- The qualifying property: named codecs never silently corrupt. ---

#[test]
fn named_codecs_have_zero_silent_wrong_cells() {
    let results = run_matrix();
    for row in &results {
        for (candidate, cell) in &row.cells {
            if matches!(
                candidate,
                Candidate::Json | Candidate::Cbor | Candidate::MsgpackNamed
            ) {
                assert!(
                    !matches!(cell, Cell::SilentWrong(_)),
                    "{} / {}: {cell:?}",
                    candidate.name(),
                    row.label
                );
            }
        }
    }
}

// --- Additive changes must be OK on the codec we shipped as codec_id 1. ---

#[test]
fn msgpack_named_tolerates_additive_and_structural_changes() {
    let results = run_matrix();
    for scenario in ["(a)", "(b)", "(c)", "(e1)", "(e2)", "(f)", "(g)", "(h)"] {
        let c = cell(&results, scenario, Candidate::MsgpackNamed);
        assert!(matches!(c, Cell::Ok), "{scenario}: got {c:?}");
    }
    // Rename without #[serde(alias)]/upcaster must fail LOUDLY, not silently.
    let c = cell(&results, "(d)", Candidate::MsgpackNamed);
    assert!(matches!(c, Cell::Error(_)), "(d): got {c:?}");
}
