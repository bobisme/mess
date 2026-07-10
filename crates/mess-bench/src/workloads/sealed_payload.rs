//! Sealed payload codec (D6 columnar sidecar): seal + verify, then full
//! sequential replay. Ported from `mess-index`'s `payload_replay_bench`
//! (`src/sealed/payload.rs`, `#[ignore]`d unit test) — same block-clustered
//! ~90% shreddable-msgpack / ~10% binary-fallback corpus shape (one
//! unshreddable event routes its whole 128-event block to raw, so a
//! realistic corpus clusters rather than sprinkling). Full-size N=1,000,000
//! matches `phase5.sealed.bytes_per_event` / `phase5.sealed.columnar_replay.ev_per_s`
//! in `docs/perf/envelope.md`.
//!
//! `payload_replay_bench`'s workload generator (`msgpack_event`/
//! `binary_event`/`Rng`) lives in that module's `#[cfg(test)]` block and is
//! crate-private, so it is not reachable from here — this is an independent
//! but equivalent generator built on the same public
//! [`mess_index::columnar::emit_str`]/[`emit_int`] primitives the original
//! uses.

use std::time::Instant;

use mess_index::columnar::{emit_int, emit_str};
use mess_index::sealed::payload::{
    DEFAULT_BLOCK_EVENTS, NoDicts, PayloadSealOpts, SealedPayloadIndex,
    encode_payload_sidecar,
};

use crate::{Metric, RunSize};

/// Tiny deterministic xorshift RNG (no external rng dep), matching the
/// source bench's generator.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9e37_79b9_7f4a_7c15)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

fn msgpack_event(rng: &mut Rng, seq: u64) -> Vec<u8> {
    let mut v = vec![0x83]; // 3-field map
    emit_str(&mut v, b"stream");
    emit_str(&mut v, format!("s{:05}", rng.below(400)).as_bytes());
    emit_str(&mut v, b"seq");
    emit_int(&mut v, seq as i64);
    emit_str(&mut v, b"amount");
    emit_int(&mut v, rng.below(1_000_000) as i64);
    v
}

fn binary_event(rng: &mut Rng) -> Vec<u8> {
    let len = 1 + rng.below(48) as usize;
    (0..len).map(|_| rng.next_u64() as u8).collect()
}

pub fn run(size: RunSize) -> Vec<Metric> {
    let total: usize = match size {
        RunSize::Full => 1_000_000,
        RunSize::Smoke => 2_000,
    };

    let mut rng = Rng::new(0xB5EED);
    let mut evs: Vec<Vec<u8>> = Vec::with_capacity(total);
    let mut i = 0u64;
    while evs.len() < total {
        let binary_block = rng.below(10) == 0;
        for _ in 0..DEFAULT_BLOCK_EVENTS {
            if evs.len() == total {
                break;
            }
            evs.push(if binary_block {
                binary_event(&mut rng)
            } else {
                msgpack_event(&mut rng, i)
            });
            i += 1;
        }
    }
    let raw_bytes: usize = evs.iter().map(Vec::len).sum();
    let refs: Vec<&[u8]> = evs.iter().map(Vec::as_slice).collect();

    let t = Instant::now();
    let bytes =
        encode_payload_sidecar(1, &refs, &PayloadSealOpts::default()).unwrap();
    let seal_dt = t.elapsed();
    let sidecar_len = bytes.len();

    let idx = SealedPayloadIndex::from_bytes(bytes).unwrap();

    let t = Instant::now();
    let mut out = Vec::new();
    let mut offs = Vec::new();
    idx.reassemble_all(&NoDicts, &mut out, &mut offs).unwrap();
    let replay_dt = t.elapsed();
    assert_eq!(offs.len(), evs.len() + 1);

    let bytes_per_event = sidecar_len as f64 / total as f64;
    let replay_ev_per_s = total as f64 / replay_dt.as_secs_f64();
    let seal_ev_per_s = total as f64 / seal_dt.as_secs_f64();

    vec![
        Metric::new(
            "phase5.sealed.bytes_per_event",
            bytes_per_event,
            "B/event",
            format!(
                "columnar .pcol payload sidecar, D6 default (zstd-9, {DEFAULT_BLOCK_EVENTS}-event \
                 blocks, no dicts); {total}-event block-clustered corpus (~90% shreddable msgpack, \
                 ~10% binary row fallback); raw {:.1} B/event",
                raw_bytes as f64 / total as f64
            ),
        ),
        Metric::new(
            "phase5.sealed.columnar_replay.ev_per_s",
            replay_ev_per_s,
            "ev/s",
            "reassemble_all over the .pcol sidecar: columnar-decode all events in stored order; \
             single core, in-memory"
                .to_string(),
        ),
        Metric::new(
            "phase5.seal_verify.ev_per_s",
            seal_ev_per_s,
            "ev/s",
            "columnar seal encode + permanent verify-on-seal (every block reassembled and \
             byte-compared)"
                .to_string(),
        ),
    ]
}
