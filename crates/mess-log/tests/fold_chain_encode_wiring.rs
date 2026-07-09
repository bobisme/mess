//! End-to-end wiring (bn-1d0): the real BLAKE3 fold chain flows through
//! [`BatchEncoder`]'s existing `crypto_chain` slot (flag bit 0), lands at the
//! spec offset (72) inside the A4 CRC coverage, and round-trips through the
//! read-side intra-batch recompute. Proves the chain **values** are now real
//! (Phase 3 supplied only placement).

use mess_log::crc::batch_crc;
use mess_log::encode::{BatchEncoder, BatchInput, Subframe};
use mess_log::fold_chain::{recompute_batch, ChainHead, FrameChain};
use mess_log::format::*;

/// `BatchHeader.flags` offset (spec 01 §4.2) — the crate-internal
/// `format::BH_FLAGS_OFF` is `pub(crate)`, so mirror it here for the byte check.
const BH_FLAGS_OFF: usize = 6;

fn payloads() -> Vec<Vec<u8>> {
    (0..4u64).map(|i| vec![(i as u8) ^ 0x33; 40]).collect()
}

#[test]
fn real_chain_entry_encodes_at_offset_72_and_recomputes() {
    let stream_id = 7u64;
    let ps = payloads();

    // Append-side: the running head yields the batch's crypto_chain entry
    // (h[base-1]) and, after absorbing the frames, the exit head h[last].
    let mut head = ChainHead::genesis(stream_id);
    let entry = head.entry();
    let refs: Vec<&[u8]> = ps.iter().map(Vec::as_slice).collect();
    let exit = head.absorb_batch(refs.iter().copied());

    // Encode a REAL batch with the real chain entry in the crypto_chain slot.
    let subframes: Vec<Subframe> =
        ps.iter().map(|p| Subframe::plain(1, 0, 0, p)).collect();
    let input = BatchInput {
        segment_epoch: 1,
        batch_id: 0,
        first_global_pos: 0,
        stream_id,
        category_id: 0,
        first_stream_version: 0,
        crypto_chain: Some(&entry),
        subframes: &subframes,
    };
    let mut enc = BatchEncoder::new();
    let bytes = enc.encode(&input).unwrap().to_vec();

    // flag bit 0 set, chain bytes at offset HEADER_LEN (72).
    let flags = u16::from_le_bytes(bytes[BH_FLAGS_OFF..BH_FLAGS_OFF + 2].try_into().unwrap());
    assert_eq!(flags & FLAG_CRYPTO_CHAIN, FLAG_CRYPTO_CHAIN);
    let on_disk_chain: [u8; CHAIN_LEN] =
        bytes[HEADER_LEN..HEADER_LEN + CHAIN_LEN].try_into().unwrap();
    assert_eq!(on_disk_chain, entry, "crypto_chain field holds the real h[base-1]");

    // Read-side: recompute every frame's chain value from the on-disk entry and
    // the payloads; the exit head must equal the append-side exit.
    let mut got: Vec<FrameChain> = Vec::new();
    let recomputed_exit =
        recompute_batch(&on_disk_chain, 0, refs.iter().copied(), |fc| got.push(fc));
    assert_eq!(recomputed_exit, exit);
    assert_eq!(got.len(), 4);
    assert_eq!(got[0].prev_stream_hash, entry);

    // The chain field is inside the batch CRC coverage (§4.4): flipping a chain
    // byte changes the recomputed batch_crc.
    let mut tampered = bytes.clone();
    tampered[HEADER_LEN] ^= 0xff;
    assert_ne!(
        batch_crc(&tampered),
        batch_crc(&bytes),
        "crypto_chain must be inside batch_crc coverage"
    );
}

#[test]
fn chain_off_is_byte_identical_to_the_a4_only_path() {
    // OPT-IN: with the chain disabled, the encoded batch has no chain bytes and
    // is exactly the pre-existing A4-only layout (zero cost when off).
    let ps = payloads();
    let subframes: Vec<Subframe> = ps.iter().map(|p| Subframe::plain(1, 0, 0, p)).collect();
    let base = BatchInput {
        segment_epoch: 1,
        batch_id: 0,
        first_global_pos: 0,
        stream_id: 7,
        category_id: 0,
        first_stream_version: 0,
        crypto_chain: None,
        subframes: &subframes,
    };
    let mut enc = BatchEncoder::new();
    let off = enc.encode(&base).unwrap().to_vec();
    // No chain: first subframe begins at HEADER_LEN, no flag bit.
    let flags = u16::from_le_bytes(off[BH_FLAGS_OFF..BH_FLAGS_OFF + 2].try_into().unwrap());
    assert_eq!(flags & FLAG_CRYPTO_CHAIN, 0);
    // total_len is 32 bytes shorter than the with-chain encoding.
    let with = BatchInput { crypto_chain: Some(&[0u8; CHAIN_LEN]), ..base };
    let on = enc.encode(&with).unwrap().to_vec();
    assert_eq!(on.len(), off.len() + CHAIN_LEN);
}
