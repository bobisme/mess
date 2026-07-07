//! Evolution-tolerance matrix: encode with a V1 shape, decode with a V2 shape,
//! classify the result.
//!
//! Cells:
//!   OK           — decode succeeded AND values match semantic expectation
//!   ERROR        — decode failed loudly (acceptable: caller sees a problem)
//!   SILENT-WRONG — decode succeeded but data is garbage/swapped (disqualifying)

use crate::codecs::{Codec, ALL_CODECS};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Debug)]
pub enum Cell {
    Ok,
    Error(String),
    SilentWrong(String),
}

impl Cell {
    pub fn short(&self) -> &'static str {
        match self {
            Cell::Ok => "OK",
            Cell::Error(_) => "ERROR",
            Cell::SilentWrong(_) => "SILENT-WRONG",
        }
    }
    pub fn detail(&self) -> Option<&str> {
        match self {
            Cell::Ok => None,
            Cell::Error(s) | Cell::SilentWrong(s) => Some(s),
        }
    }
}

/// Encode `v1` with `codec`, decode as `V2`, compare against `expected`.
fn check<V1, V2>(codec: Codec, v1: &V1, expected: &V2) -> Cell
where
    V1: Serialize,
    V2: DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let bytes = codec.encode(v1);
    match codec.decode::<V2>(&bytes) {
        Ok(got) if got == *expected => Cell::Ok,
        Ok(got) => Cell::SilentWrong(format!("got {:?}, expected {:?}", got, expected)),
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

pub struct ScenarioResult {
    pub label: &'static str,
    pub cells: Vec<(Codec, Cell)>,
}

pub fn run_matrix() -> Vec<ScenarioResult> {
    let run = |label: &'static str, f: &dyn Fn(Codec) -> Cell| ScenarioResult {
        label,
        cells: ALL_CODECS.iter().map(|&c| (c, f(c))).collect(),
    };

    vec![
        run("(a) added Option field", &|c| {
            check(
                c,
                &v1_sample(),
                &V2AddedOption { id: 7, name: "alice".into(), count: 42, active: true, note: None },
            )
        }),
        run("(b) added field w/ serde default", &|c| {
            check(
                c,
                &v1_sample(),
                &V2AddedDefault { id: 7, name: "alice".into(), count: 42, active: true, retries: 0 },
            )
        }),
        run("(c) removed field", &|c| {
            check(c, &v1_sample(), &V2Removed { id: 7, name: "alice".into(), active: true })
        }),
        run("(d) renamed field (no serde rename)", &|c| {
            // "Correct" here means the old value survives under the new name.
            check(
                c,
                &v1_sample(),
                &V2Renamed { id: 7, title: "alice".into(), count: 42, active: true },
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
                &V2ReorderedMixed { count: 42, id: 7, name: "alice".into(), active: true },
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
                &V2Widened { id: 7, name: "alice".into(), count: 42, active: true },
            )
        }),
    ]
}

/// Markdown rendering of the matrix.
pub fn matrix_markdown(results: &[ScenarioResult]) -> String {
    let mut out = String::new();
    out.push_str("| schema change |");
    for c in ALL_CODECS {
        out.push_str(&format!(" {} |", c.name()));
    }
    out.push('\n');
    out.push_str("|---|");
    for _ in ALL_CODECS {
        out.push_str("---|");
    }
    out.push('\n');
    for r in results {
        out.push_str(&format!("| {} |", r.label));
        for (_, cell) in &r.cells {
            out.push_str(&format!(" {} |", cell.short()));
        }
        out.push('\n');
    }
    out
}
