//! One-shot corpus-seed generator, NOT a fuzz target (bn-meo). Run with:
//!
//! ```text
//! cargo run --manifest-path crates/mess-index/fuzz/Cargo.toml --bin gen_corpus
//! ```
//!
//! Writes small seed files under `corpus/fuzz_reassemble_block/`. Seeds mix
//! two shapes, both consumed by the same fuzz target (`fuzz_reassemble_block`
//! runs its "blind" and "structured" checks unconditionally on every input —
//! see that file's doc comment):
//!
//! - Real `encode_block` output (whole-block and per-column layouts, plus a
//!   forced raw fallback) over payload shapes mirroring
//!   `spikes/perf_compress/src/workload.rs` / `src/columnar/tests.rs`'s
//!   corpus — good material for the "blind" `Block::decode` check.
//! - Bytes laid out in `fuzz_reassemble_block::parse_structured`'s own
//!   sub-format (opts byte, row count, length-prefixed row payloads,
//!   mutation triples) built from the same real payload shapes, so the
//!   "structured" ground-truth check exercises the shredder's canonical
//!   msgpack path from the very first run instead of waiting for
//!   coverage-guided mutation to stumble onto valid rows by chance.
//!
//! Kept minimal (small seeds only, per the bn-gux protocol this project's
//! prior fuzz crate established) — libFuzzer's own coverage-guided mutation
//! does the rest.

use std::fs;
use std::path::{Path, PathBuf};

use mess_index::columnar::{EncodeOpts, encode_block};
use serde::Serialize;

fn write_seed(dir: &Path, name: &str, bytes: &[u8]) {
    fs::create_dir_all(dir).expect("create corpus dir");
    fs::write(dir.join(name), bytes).expect("write seed");
}

// --------------------------------------------------------------- payloads

#[derive(Serialize)]
struct AccountCredited {
    stream: String,
    seq: u64,
    amount_cents: u64,
    currency: &'static str,
    actor: String,
    note: String,
    occurred_at_ms: i64,
}

#[derive(Serialize)]
struct LineItem {
    sku: String,
    qty: u32,
    price_cents: u32,
}

#[derive(Serialize)]
struct OrderPlaced {
    stream: String,
    seq: u64,
    order_id: String,
    items: Vec<LineItem>,
    total_cents: u64,
    placed_at_ms: i64,
}

fn account_credited_rows(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let v = AccountCredited {
                stream: format!("account-{i}"),
                seq: i as u64,
                amount_cents: 1000 + i as u64 * 7,
                currency: "USD",
                actor: "system".to_string(),
                note: format!("credit #{i}"),
                occurred_at_ms: 1_767_225_600_000 + i as i64 * 1000,
            };
            rmp_serde::to_vec_named(&v).expect("encode AccountCredited")
        })
        .collect()
}

fn order_placed_rows(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let v = OrderPlaced {
                stream: format!("order-{i}"),
                seq: i as u64,
                order_id: format!("ord_{i:06}"),
                items: vec![
                    LineItem { sku: "sku-a".into(), qty: 1 + (i as u32 % 3), price_cents: 500 },
                    LineItem { sku: "sku-b".into(), qty: 2, price_cents: 1250 },
                ],
                total_cents: 2000 + i as u64,
                placed_at_ms: 1_767_225_600_000 + i as i64 * 1000,
            };
            rmp_serde::to_vec_named(&v).expect("encode OrderPlaced")
        })
        .collect()
}

/// Deeply nested msgpack array: `depth` fixarray-of-1 markers around a
/// single fixint. Exercises the shredder's `MAX_DEPTH` fallback boundary
/// (`src/columnar/shred.rs`) and the upcast/decode path's recursion limits.
fn deep_nested(depth: usize) -> Vec<u8> {
    let mut v = vec![0x91u8; depth]; // depth x fixarray, len 1
    v.push(0x00); // fixint 0
    v
}

// ------------------------------------------------- structured sub-format

/// Build one `fuzz_reassemble_block::parse_structured`-shaped seed:
/// `opts` byte, row count, length-prefixed rows, then a few mutation
/// triples so the mutated-block self-consistency path also gets exercised
/// from run 1.
fn structured_seed(per_column: bool, level_idx: u8, rows: &[Vec<u8>], mutations: &[(u16, u8)]) -> Vec<u8> {
    let mut out = Vec::new();
    let opts = (per_column as u8) | (level_idx.min(7) << 1);
    out.push(opts);
    out.push(rows.len().min(32) as u8);
    for row in rows.iter().take(32) {
        let len = row.len().min(u16::MAX as usize) as u16;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&row[..len as usize]);
    }
    for (off, xorb) in mutations {
        out.extend_from_slice(&off.to_le_bytes());
        out.push(*xorb);
    }
    out
}

fn main() {
    let base: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus/fuzz_reassemble_block");

    // ---- blind seeds: real encode_block output straight in ----
    let acc = account_credited_rows(16);
    let acc_refs: Vec<&[u8]> = acc.iter().map(|v| v.as_slice()).collect();
    write_seed(&base, "blind_whole_block", &encode_block(&acc_refs, EncodeOpts { level: 9, per_column: false }));
    write_seed(&base, "blind_percol_block", &encode_block(&acc_refs, EncodeOpts { level: 9, per_column: true }));

    let orders = order_placed_rows(12);
    let order_refs: Vec<&[u8]> = orders.iter().map(|v| v.as_slice()).collect();
    write_seed(&base, "blind_orders_whole", &encode_block(&order_refs, EncodeOpts { level: 6, per_column: false }));
    write_seed(&base, "blind_orders_percol", &encode_block(&order_refs, EncodeOpts { level: 6, per_column: true }));

    // Non-msgpack rows -> forced raw fallback block.
    let junk: Vec<Vec<u8>> = (0..8).map(|i| vec![0xff, 0x00, i as u8, 0xca, 0x99]).collect();
    let junk_refs: Vec<&[u8]> = junk.iter().map(|v| v.as_slice()).collect();
    write_seed(&base, "blind_raw_fallback", &encode_block(&junk_refs, EncodeOpts { level: 3, per_column: false }));

    // Deeply nested payload -> shredder's MAX_DEPTH fallback boundary.
    let nested = [deep_nested(40)];
    let nested_refs: Vec<&[u8]> = nested.iter().map(|v| v.as_slice()).collect();
    write_seed(&base, "blind_deep_nested", &encode_block(&nested_refs, EncodeOpts { level: 3, per_column: false }));

    // Degenerate raw bytes.
    write_seed(&base, "blind_empty", &[]);
    write_seed(&base, "blind_header_only", &[1, 0]);
    write_seed(&base, "blind_single_byte", &[0x42]);

    // ---- structured seeds: parse_structured's own sub-format ----
    write_seed(
        &base,
        "structured_whole_basic",
        &structured_seed(false, 6, &account_credited_rows(4), &[(2, 0xff), (10, 0x01)]),
    );
    write_seed(
        &base,
        "structured_percol_basic",
        &structured_seed(true, 6, &account_credited_rows(4), &[(5, 0x80)]),
    );
    write_seed(
        &base,
        "structured_orders_percol",
        &structured_seed(true, 4, &order_placed_rows(6), &[(1, 0xff), (3, 0xff), (7, 0xff)]),
    );
    // Mix of valid and invalid rows -> shredder falls back to raw for the
    // whole block, still must stay byte-exact.
    let mut mixed = account_credited_rows(3);
    mixed.push(vec![0xff, 0xff, 0xff]);
    mixed.push(deep_nested(50));
    write_seed(&base, "structured_mixed_bad", &structured_seed(false, 5, &mixed, &[(0, 0xff)]));
    // No rows at all (n_rows=0) -- exercises the "skip structured checks"
    // early-return path.
    write_seed(&base, "structured_zero_rows", &[0, 0]);

    eprintln!("wrote corpus seeds to {}", base.display());
}
