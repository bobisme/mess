//! bn-meo: coverage-guided fuzzing of the columnar block decoder/reassembler,
//! [`mess_index::columnar::Block::decode`] and its `reassemble_*` methods.
//!
//! Byte-exactness is the tier's core promise (`src/columnar/mod.rs` module
//! doc): reassembly must reproduce the original payload bytes exactly, for
//! every event, always. So the bar for this decode surface is stricter than
//! "never panic" — a corrupt/adversarial block must either fail with a typed
//! `CodecError` or, if it decodes at all, never silently hand back the wrong
//! bytes. `data` is interpreted two ways, run unconditionally on every
//! input (no mode byte gating which path runs — both always fire):
//!
//! 1. **Blind**: `data` fed straight to `Block::decode` as a candidate
//!    compressed block + skeleton table. No known-good "original" to
//!    compare against, so the only checkable invariant is structural: never
//!    panic, never abort (OOM/allocation bomb — `MAX_ULEN` and the
//!    aggregate per-column `ulen` cap this target's early runs found missing
//!    bound any single decode's memory use; see
//!    `CodecError::TotalUlenExceeded`).
//! 2. **Structured** (`parse_structured`, below): a tiny deterministic
//!    sub-format carves zero or more row payloads and a mutation list out of
//!    the *same* `data` (see the format doc on `parse_structured`). Those
//!    rows — real bytes, so libFuzzer's coverage-guided mutation can steer
//!    them toward both canonical msgpack the shredder accepts and hostile
//!    bytes that force raw fallback — are run through the real
//!    `encode_block` shredder, producing a block with a *known* set of
//!    original payloads. Two checks against that ground truth:
//!     - The unmutated block must decode and reassemble byte-exact (both
//!       `reassemble_all` and `reassemble_one`, covering the whole-block and
//!       per-column point-read paths).
//!     - A mutated copy of the block bytes is decoded; if that succeeds,
//!       there's no ground truth left (the mutation may have legitimately
//!       changed what the "right" answer is), so the check is
//!       *self-consistency* instead: re-shredding the reassembled rows and
//!       reassembling that must reproduce the exact same bytes. This holds
//!       unconditionally by construction (raw fallback stores any row
//!       verbatim — see `src/columnar/mod.rs` "byte-exactness is the
//!       acceptance bar"), so any divergence is a genuine "reassemble
//!       produced bytes the codec itself can't stand behind" bug — the
//!       "differential: if it succeeds, output must re-shred to the same
//!       columns" check from bn-meo's plan.
#![no_main]

use libfuzzer_sys::fuzz_target;
use mess_index::columnar::{Block, EncodeOpts, MAX_BLOCK_EVENTS, encode_block};

/// Zstd levels sampled by the 3-bit level selector in [`parse_structured`]'s
/// opts byte — kept low so a fuzz exec compresses fast; `columnar::tests`
/// already covers level 9 (the round-4 default) with real corpora.
const LEVELS: [i32; 8] = [1, 2, 3, 4, 5, 6, 9, 12];

/// Carve `(rows, per_column, level, mutations)` out of `data`. Every length
/// is clamped to what's actually left in `data`, so this never panics on
/// short/malformed input — worst case it returns zero rows and the caller
/// skips the structured checks (the blind `Block::decode(data)` check still
/// ran unconditionally).
///
/// Format (no relation to the columnar block format — this is purely how
/// this fuzz target's own input bytes are sliced up):
/// ```text
/// byte 0:      opts: bit0 = per_column, bits1-3 = level index into LEVELS
/// byte 1:      n_rows (clamped to 32)
/// n_rows x { u16 LE row_len, row_len bytes }   row payloads, back to back
/// remainder:   (u16 LE offset, u8 xor) triples, up to 64, for mutation
/// ```
struct Structured {
    rows: Vec<Vec<u8>>,
    per_column: bool,
    level: i32,
    /// `(byte offset, xor mask)` pairs applied to the encoded block.
    mutations: Vec<(u16, u8)>,
}

fn parse_structured(data: &[u8]) -> Structured {
    if data.len() < 2 {
        return Structured { rows: Vec::new(), per_column: false, level: LEVELS[0], mutations: Vec::new() };
    }
    let opts = data[0];
    let per_column = opts & 1 != 0;
    let level = LEVELS[((opts >> 1) & 0x7) as usize];
    let n_rows = data[1].min(32) as usize;

    let mut rows = Vec::with_capacity(n_rows);
    let mut cursor = 2usize;
    for _ in 0..n_rows {
        if cursor + 2 > data.len() {
            break;
        }
        let row_len = u16::from_le_bytes([data[cursor], data[cursor + 1]]) as usize;
        cursor += 2;
        let row_len = row_len.min(data.len() - cursor);
        rows.push(data[cursor..cursor + row_len].to_vec());
        cursor += row_len;
    }

    let mut mutations = Vec::new();
    while cursor + 3 <= data.len() && mutations.len() < 64 {
        let off = u16::from_le_bytes([data[cursor], data[cursor + 1]]);
        let xorb = data[cursor + 2];
        mutations.push((off, xorb));
        cursor += 3;
    }

    Structured { rows, per_column, level, mutations }
}

fuzz_target!(|data: &[u8]| {
    // 1. Blind: data straight into the decoder. Never panic, never OOM.
    if let Ok(block) = Block::decode(data) {
        let mut out = Vec::new();
        let mut offs = Vec::new();
        let _ = block.reassemble_all(&mut out, &mut offs);
        if !block.is_empty() {
            let _ = block.reassemble_one(0);
        }
    }

    // 2. Structured: known-original rows through the real shredder.
    let Structured { rows, per_column, level, mutations } = parse_structured(data);
    if rows.is_empty() || rows.len() > MAX_BLOCK_EVENTS {
        return;
    }
    let refs: Vec<&[u8]> = rows.iter().map(|v| v.as_slice()).collect();
    let opts = EncodeOpts { level, per_column };
    let block_bytes = encode_block(&refs, opts);

    // 2a. Freshly encoded, unmutated: must always decode and reassemble
    // byte-exact. `encode_block` never fails (unshreddable input routes to
    // raw fallback), so this must always hold, for ANY row bytes.
    let block =
        Block::decode(&block_bytes).expect("freshly encoded block must always decode");
    let mut out = Vec::new();
    let mut offs = Vec::new();
    block.reassemble_all(&mut out, &mut offs).expect("freshly encoded block must always reassemble");
    assert_eq!(offs.len(), rows.len() + 1, "offset count mismatch");
    for i in 0..rows.len() {
        let a = offs[i] as usize;
        let b = offs[i + 1] as usize;
        assert_eq!(&out[a..b], rows[i].as_slice(), "byte-exact violation on unmutated block, row {i}");
        let one = block.reassemble_one(i).expect("point read of a valid row must succeed");
        assert_eq!(one, rows[i], "reassemble_one byte-exact violation, row {i}");
    }

    // 2b. Mutated: no ground truth, so check self-consistency instead of
    // exact-match against the pre-mutation originals.
    let mut mutated = block_bytes.clone();
    if !mutated.is_empty() {
        for (off, xorb) in &mutations {
            let idx = (*off as usize) % mutated.len();
            mutated[idx] ^= xorb;
        }
    }
    if let Ok(block2) = Block::decode(&mutated) {
        let mut out2 = Vec::new();
        let mut offs2 = Vec::new();
        if block2.reassemble_all(&mut out2, &mut offs2).is_ok() {
            let rows2: Vec<&[u8]> =
                offs2.windows(2).map(|w| &out2[w[0] as usize..w[1] as usize]).collect();
            if rows2.len() <= MAX_BLOCK_EVENTS {
                // Re-shred what reassembly claims the rows are, and confirm
                // reassembling THAT reproduces the exact same bytes — a
                // fixed point that holds for any &[u8] rows by construction
                // (raw fallback is always byte-exact), so any mismatch here
                // means block2's reassembly handed back bytes the codec
                // itself considers inconsistent.
                let reencoded = encode_block(&rows2, opts);
                let block3 = Block::decode(&reencoded)
                    .expect("re-shredding reassembled bytes must always decode");
                let mut out3 = Vec::new();
                let mut offs3 = Vec::new();
                block3
                    .reassemble_all(&mut out3, &mut offs3)
                    .expect("re-shredding reassembled bytes must always reassemble");
                assert_eq!(offs3, offs2, "re-shred idempotency: offsets diverged");
                assert_eq!(out3, out2, "re-shred idempotency: bytes diverged (silent corruption)");
            }
        }
    }
});
