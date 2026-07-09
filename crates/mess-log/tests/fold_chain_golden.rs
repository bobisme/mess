//! Byte-exact golden constants for the fold-chain hash constructions (bn-1d0,
//! spec 05 §3). These pin the on-disk hash values so any accidental change to
//! the encoding (domain tags, field order, integer width) trips a test. Per
//! §3.3's note, the `fold_cert` spike's golden constants are **regenerated**
//! here against this spec (the spike omitted domain tags and ordered
//! `payload || version`; this spec uses `0x01 || le64(version) || payload`).

use mess_log::fold_chain::{advance, chain_step, frame_hash, genesis};

fn hex(h: &[u8; 32]) -> String {
    h.iter().map(|b| format!("{b:02x}")).collect()
}

/// `genesis(7) = BLAKE3(0x00 || "mess-stream-v1" || le64(7))`.
const GENESIS_7: &str = "84eb178e62ecee1eb7536fc4bdf2dd6a2737c209d1ca2106531163f33b41369f";

/// `frame_hash(0, ev(deposit,100))` — deposit-of-100 payload, version 0.
const FH0: &str = "446a38d171c5409718efebea5b8798cd40b535b3f2349b26413d7488cc4875ad";

/// `chain_step(0, 0, 0) = BLAKE3(0x02 || 0^32 || 0^32 || le64(0))`.
const CHAIN_STEP_ZERO: &str = "74af87d638630a6c386ea2e8d1f53cd10f49c93ee84f2ab778d930c63ae1f28a";

/// `h[4]` over the 5-event fixture (deposit 100, 50; withdraw 30, 20; deposit 7)
/// for `stream_id = 1` — the regenerated equivalent of the spike's golden state.
const H4_STREAM1: &str = "a5d6fca0230ce75e264ac448f5ccd6dcccf2efa8204b95246a63ad30d22a9f9c";

fn ev(tag: u8, amount: u64) -> Vec<u8> {
    let mut p = vec![0u8; 9];
    p[0] = tag;
    p[1..9].copy_from_slice(&amount.to_le_bytes());
    p
}

#[test]
fn genesis_golden() {
    assert_eq!(hex(&genesis(7)), GENESIS_7);
}

#[test]
fn frame_hash_golden() {
    assert_eq!(hex(&frame_hash(0, &ev(0, 100))), FH0);
}

#[test]
fn chain_step_golden() {
    assert_eq!(hex(&chain_step(&[0u8; 32], &[0u8; 32], 0)), CHAIN_STEP_ZERO);
}

#[test]
fn fixture_prefix_hash_golden() {
    let fixture = [ev(0, 100), ev(0, 50), ev(1, 30), ev(1, 20), ev(0, 7)];
    let mut h = genesis(1);
    for (v, p) in fixture.iter().enumerate() {
        h = advance(&h, v as u64, p);
    }
    assert_eq!(hex(&h), H4_STREAM1);
}
