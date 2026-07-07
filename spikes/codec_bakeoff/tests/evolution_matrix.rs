//! Locks in the decisive evolution-matrix findings so a dependency bump that
//! changes codec behavior fails loudly.

use codec_bakeoff::codecs::Codec;
use codec_bakeoff::evolution::{run_matrix, Cell};

fn cell(label_prefix: &str, codec: Codec) -> Cell {
    let matrix = run_matrix();
    let row = matrix
        .iter()
        .find(|r| r.label.starts_with(label_prefix))
        .unwrap_or_else(|| panic!("no scenario {label_prefix}"));
    row.cells
        .iter()
        .find(|(c, _)| *c == codec)
        .map(|(_, cell)| cell.clone())
        .unwrap()
}

// --- The disqualifying cells: positional codecs silently corrupt data. ---

#[test]
fn positional_codecs_silently_swap_same_typed_reordered_fields() {
    for codec in [Codec::Postcard, Codec::Bincode, Codec::MsgpackCompact] {
        let c = cell("(e1)", codec);
        assert!(
            matches!(c, Cell::SilentWrong(_)),
            "{}: got {c:?}",
            codec.name()
        );
    }
}

#[test]
fn index_encoded_enums_silently_swap_reordered_variants() {
    // rmp-serde encodes variant NAMES even in compact mode, so only
    // postcard and bincode (u32 variant indices) hit this trap.
    for codec in [Codec::Postcard, Codec::Bincode] {
        let c = cell("(g)", codec);
        assert!(
            matches!(c, Cell::SilentWrong(_)),
            "{}: got {c:?}",
            codec.name()
        );
    }
    // Verified empirically: variant-by-name saves msgpack-compact here.
    assert!(matches!(cell("(g)", Codec::MsgpackCompact), Cell::Ok));
}

// --- The qualifying property: named codecs never silently corrupt. ---

#[test]
fn named_codecs_have_zero_silent_wrong_cells() {
    let matrix = run_matrix();
    for row in &matrix {
        for (codec, cell) in &row.cells {
            if matches!(codec, Codec::Json | Codec::Cbor | Codec::MsgpackNamed) {
                assert!(
                    !matches!(cell, Cell::SilentWrong(_)),
                    "{} / {}: {cell:?}",
                    codec.name(),
                    row.label
                );
            }
        }
    }
}

// --- Additive changes must be OK on the codec we plan to ship. ---

#[test]
fn msgpack_named_tolerates_additive_and_structural_changes() {
    for scenario in ["(a)", "(b)", "(c)", "(e1)", "(e2)", "(f)", "(g)", "(h)"] {
        let c = cell(scenario, Codec::MsgpackNamed);
        assert!(matches!(c, Cell::Ok), "{scenario}: got {c:?}");
    }
    // Rename without #[serde(alias)]/upcaster must fail LOUDLY, not silently.
    let c = cell("(d)", Codec::MsgpackNamed);
    assert!(matches!(c, Cell::Error(_)), "(d): got {c:?}");
}
