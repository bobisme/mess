//! Byte-exact round-trip proof for the columnar codec.
//!
//! The corpus mirrors `spikes/perf_compress/src/workload.rs`: real rmp-serde
//! named-mode payloads (the actual Phase-1 bytes) across four event
//! categories with nested maps, arrays, bools, strings, and monotone-ish
//! timestamps, in sealed cluster order. Byte-identity of every reassembled
//! event against its original bytes is the acceptance bar (`0 mismatches`).

use serde::Serialize;

use super::*;

// ---------------------------------------------------------------------------
// Corpus (envelope rows) — faithful to the perf_compress spike shapes.
// ---------------------------------------------------------------------------

const CATEGORIES: usize = 4;
const EPOCH_2026_MS: i64 = 1_767_225_600_000;

#[derive(Serialize)]
struct AccountCredited {
    stream:         String,
    seq:            u64,
    amount_cents:   u64,
    currency:       &'static str,
    actor:          String,
    source:         &'static str,
    note:           String,
    occurred_at_ms: i64,
    schema_v:       u32,
}

#[derive(Serialize)]
struct LineItem {
    sku:         String,
    qty:         u32,
    price_cents: u32,
}

#[derive(Serialize)]
struct OrderPlaced {
    stream:       String,
    seq:          u64,
    order_id:     String,
    items:        Vec<LineItem>,
    total_cents:  u64,
    currency:     &'static str,
    customer:     String,
    placed_at_ms: i64,
    schema_v:     u32,
}

#[derive(Serialize)]
struct ProfileFields {
    display_name:     String,
    locale:           &'static str,
    tz:               &'static str,
    marketing_opt_in: bool,
}

#[derive(Serialize)]
struct ProfileUpdated {
    stream:        String,
    seq:           u64,
    fields:        ProfileFields,
    updated_by:    String,
    reason:        String,
    updated_at_ms: i64,
}

#[derive(Serialize)]
struct SensorReading {
    stream:         String,
    seq:            u64,
    temp_dc:        i32,
    humidity_dpct:  u32,
    pressure_dhpa:  u32,
    battery_pct:    u32,
    rssi:           i32,
    status:         &'static str,
    site:           String,
    window_s:       u32,
    tags:           Vec<String>,
    recorded_at_ms: i64,
}

/// Tiny deterministic xorshift RNG — no external rng version churn in tests.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self { Rng(seed ^ 0x9E37_79B9_7F4A_7C15) }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 { self.next_u64() % n }

    fn word(&mut self, len: usize) -> String {
        (0..len).map(|_| (b'a' + (self.below(26)) as u8) as char).collect()
    }

    /// Zipf-ish skewed stream id in `[0, streams)` (heavy head, no rand_distr).
    fn zipf_stream(&mut self, streams: u64) -> u64 {
        let r = self.below(1000);
        if r < 500 {
            self.below(streams / 50 + 1)
        } else if r < 850 {
            self.below(streams / 5 + 1)
        } else {
            self.below(streams)
        }
    }
}

fn payload(
    rng: &mut Rng,
    cat: usize,
    stream: u32,
    seq: u64,
    ts_ms: i64,
) -> Vec<u8> {
    let sname = format!("s{stream:07}");
    match cat {
        0 => rmp_serde::to_vec_named(&AccountCredited {
            stream: sname,
            seq,
            amount_cents: rng.below(500_000),
            currency: if rng.below(2) == 0 { "USD" } else { "EUR" },
            actor: rng.word(6),
            source: "ledger",
            note: {
                let nlen = 8 + rng.below(16) as usize;
                rng.word(nlen)
            },
            occurred_at_ms: ts_ms,
            schema_v: 1,
        })
        .unwrap(),
        1 => {
            let nitems = 1 + rng.below(4) as usize;
            let items = (0..nitems)
                .map(|_| LineItem {
                    sku:         rng.word(8),
                    qty:         1 + rng.below(9) as u32,
                    price_cents: rng.below(20_00) as u32,
                })
                .collect();
            rmp_serde::to_vec_named(&OrderPlaced {
                stream: sname,
                seq,
                order_id: rng.word(12),
                items,
                total_cents: rng.below(50_000),
                currency: "USD",
                customer: rng.word(10),
                placed_at_ms: ts_ms,
                schema_v: 1,
            })
            .unwrap()
        }
        2 => rmp_serde::to_vec_named(&ProfileUpdated {
            stream: sname,
            seq,
            fields: ProfileFields {
                display_name:     rng.word(10),
                locale:           "en-US",
                tz:               "UTC",
                marketing_opt_in: rng.below(2) == 0,
            },
            updated_by: rng.word(6),
            reason: rng.word(8),
            updated_at_ms: ts_ms,
        })
        .unwrap(),
        _ => {
            let ntags = rng.below(3) as usize;
            let tags = (0..ntags).map(|_| rng.word(4)).collect();
            rmp_serde::to_vec_named(&SensorReading {
                stream: sname,
                seq,
                temp_dc: -200 + rng.below(600) as i32,
                humidity_dpct: rng.below(1000) as u32,
                pressure_dhpa: 9000 + rng.below(2000) as u32,
                battery_pct: rng.below(101) as u32,
                rssi: -(rng.below(100) as i32),
                status: if rng.below(10) == 0 { "warn" } else { "ok" },
                site: rng.word(5),
                window_s: 60,
                tags,
                recorded_at_ms: ts_ms,
            })
            .unwrap()
        }
    }
}

/// Generate `total` events in sealed cluster order, per category, chunked into
/// `be`-event blocks. Returns `Vec<block>` where each block is a
/// `Vec<payload>`.
fn corpus_blocks(total: usize, be: usize, streams: u64) -> Vec<Vec<Vec<u8>>> {
    let mut rng = Rng::new(0xC0FFEE);
    let per_cat = total / CATEGORIES;
    let mut blocks = Vec::new();
    for cat in 0..CATEGORIES {
        // Assign each event to a stream (Zipf), then sort by (stream, seq) to
        // get cluster order. seq is monotone per stream; ts advances globally.
        let mut evs: Vec<(u32, u64, i64)> = Vec::with_capacity(per_cat);
        let mut ts = EPOCH_2026_MS;
        let mut seqs = std::collections::HashMap::<u32, u64>::new();
        for _ in 0..per_cat {
            let s = (rng.zipf_stream(streams) as u32) * CATEGORIES as u32
                + cat as u32;
            let seq = seqs.entry(s).or_insert(0);
            evs.push((s, *seq, ts));
            *seq += 1;
            ts += 1 + rng.below(50) as i64;
        }
        evs.sort_by_key(|&(s, seq, _)| (s, seq));
        let payloads: Vec<Vec<u8>> = evs
            .iter()
            .map(|&(s, seq, ts)| payload(&mut rng, cat, s, seq, ts))
            .collect();
        for chunk in payloads.chunks(be) {
            blocks.push(chunk.to_vec());
        }
    }
    blocks
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn refs(evs: &[Vec<u8>]) -> Vec<&[u8]> {
    evs.iter().map(|e| e.as_slice()).collect()
}

/// Encode with `opts`, decode, and assert every event reassembles byte-exact,
/// both via full range and via single point reads. Returns encoded length.
fn assert_roundtrip(evs: &[Vec<u8>], opts: EncodeOpts) -> usize {
    let r = refs(evs);
    let enc = encode_block(&r, opts);
    // frame header invariants
    assert_eq!(enc[0], COLUMNAR_VERSION);
    assert_eq!(enc[1] & FLAG_RESERVED_MASK, 0);

    let block = Block::decode(&enc).expect("decode");
    assert_eq!(block.len(), evs.len());

    // full reassembly
    let mut out = Vec::new();
    let mut offs = Vec::new();
    block.reassemble_all(&mut out, &mut offs).expect("reassemble_all");
    assert_eq!(offs.len(), evs.len() + 1);
    for (i, w) in offs.windows(2).enumerate() {
        let got = &out[w[0] as usize..w[1] as usize];
        assert_eq!(got, evs[i].as_slice(), "range mismatch at event {i}");
    }

    // point reads
    for (i, ev) in evs.iter().enumerate() {
        let got = block.reassemble_one(i).expect("reassemble_one");
        assert_eq!(&got, ev, "point-read mismatch at event {i}");
    }
    enc.len()
}

// ---------------------------------------------------------------------------
// varint / zigzag (pure — safe under miri)
// ---------------------------------------------------------------------------

#[test]
fn varint_roundtrip() {
    for v in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX, 1 << 40] {
        let mut b = Vec::new();
        write_varint(&mut b, v);
        assert_eq!(b.len(), varint_len(v));
        let mut p = 0;
        assert_eq!(read_varint(&b, &mut p), Some(v));
        assert_eq!(p, b.len());
    }
}

#[test]
fn varint_truncated_and_overlong() {
    // high-bit-set byte with no continuation -> None, not panic
    let mut p = 0;
    assert_eq!(read_varint(&[0x80], &mut p), None);
    // 11 continuation bytes overflow the shift -> None
    let overlong = [0x80u8; 11];
    let mut p = 0;
    assert_eq!(read_varint(&overlong, &mut p), None);
}

#[test]
fn zigzag_roundtrip() {
    for v in [0i64, -1, 1, i64::MIN, i64::MAX, -1000, 1000] {
        assert_eq!(unzz(zz(v)), v);
    }
}

// ---------------------------------------------------------------------------
// Round-trip on the real corpus (whole-block and per-column)
// ---------------------------------------------------------------------------

#[test]
#[cfg_attr(miri, ignore)] // zstd is C; miri can't run it
fn corpus_roundtrip_whole() {
    let blocks = corpus_blocks(4096, 128, 400);
    let mut events = 0usize;
    let mut comp = 0usize;
    for blk in &blocks {
        comp += assert_roundtrip(blk, EncodeOpts::default());
        events += blk.len();
    }
    assert!(events > 3000);
    eprintln!(
        "whole: {events} events, {comp} B, {:.1} B/event",
        comp as f64 / events as f64
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn corpus_roundtrip_percol() {
    let blocks = corpus_blocks(4096, 128, 400);
    for blk in &blocks {
        assert_roundtrip(blk, EncodeOpts { level: 9, per_column: true });
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn empty_and_singleton_blocks() {
    // empty block
    let enc = encode_block(&[], EncodeOpts::default());
    let b = Block::decode(&enc).unwrap();
    assert_eq!(b.len(), 0);
    assert!(b.is_empty());
    let mut out = Vec::new();
    let mut offs = Vec::new();
    b.reassemble_all(&mut out, &mut offs).unwrap();
    assert_eq!(offs, vec![0]);
    assert!(out.is_empty());

    // singleton
    let mut rng = Rng::new(1);
    let ev = payload(&mut rng, 0, 3, 0, EPOCH_2026_MS);
    assert_roundtrip(&[ev], EncodeOpts::default());
}

// ---------------------------------------------------------------------------
// Fallback: non-msgpack, non-canonical, and unsupported markers
// ---------------------------------------------------------------------------

fn is_raw(enc: &[u8]) -> bool { enc[1] & FLAG_COLUMNAR == 0 }

#[test]
#[cfg_attr(miri, ignore)]
fn random_binary_falls_back_byte_exact() {
    let mut rng = Rng::new(42);
    let evs: Vec<Vec<u8>> = (0..200)
        .map(|_| {
            let len = 1 + rng.below(64) as usize;
            (0..len).map(|_| rng.next_u64() as u8).collect()
        })
        .collect();
    let r = refs(&evs);
    let enc = encode_block(&r, EncodeOpts::default());
    // Overwhelmingly these are not valid canonical msgpack maps -> raw.
    assert!(is_raw(&enc), "expected raw fallback for random bytes");
    let block = Block::decode(&enc).unwrap();
    let mut out = Vec::new();
    let mut offs = Vec::new();
    block.reassemble_all(&mut out, &mut offs).unwrap();
    for (i, w) in offs.windows(2).enumerate() {
        assert_eq!(&out[w[0] as usize..w[1] as usize], evs[i].as_slice());
    }
    // point reads too
    for (i, ev) in evs.iter().enumerate() {
        assert_eq!(&block.reassemble_one(i).unwrap(), ev);
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn non_canonical_int_falls_back() {
    // A one-field map {"a": 5} but 5 encoded non-minimally as uint8 (0xcc 05)
    // instead of fixint 0x05. Must round-trip byte-exact via raw fallback.
    let ev = vec![0x81, 0xA1, b'a', 0xCC, 0x05];
    let r = refs(std::slice::from_ref(&ev));
    let enc = encode_block(&r, EncodeOpts::default());
    assert!(is_raw(&enc), "non-canonical int must fall back");
    let block = Block::decode(&enc).unwrap();
    assert_eq!(block.reassemble_one(0).unwrap(), ev);
}

#[test]
#[cfg_attr(miri, ignore)]
fn float_payload_falls_back() {
    // {"x": <f64 1.5>}  (0xcb marker) — floats not modeled -> raw.
    let mut ev = vec![0x81, 0xA1, b'x', 0xCB];
    ev.extend_from_slice(&1.5f64.to_be_bytes());
    let r = refs(std::slice::from_ref(&ev));
    let enc = encode_block(&r, EncodeOpts::default());
    assert!(is_raw(&enc));
    let block = Block::decode(&enc).unwrap();
    assert_eq!(block.reassemble_one(0).unwrap(), ev);
}

#[test]
#[cfg_attr(miri, ignore)]
fn mixed_block_with_one_bad_event_falls_back_whole_block() {
    // A block of good msgpack plus one binary event: the whole block goes raw,
    // still byte-exact.
    let mut rng = Rng::new(7);
    let mut evs: Vec<Vec<u8>> =
        (0..8).map(|_| payload(&mut rng, 3, 5, 0, EPOCH_2026_MS)).collect();
    evs.insert(4, vec![0xFF, 0x00, 0xCA, 0x99]); // not shreddable
    let r = refs(&evs);
    let enc = encode_block(&r, EncodeOpts::default());
    assert!(is_raw(&enc));
    let block = Block::decode(&enc).unwrap();
    for (i, ev) in evs.iter().enumerate() {
        assert_eq!(&block.reassemble_one(i).unwrap(), ev);
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn nil_and_bool_supported() {
    // {"a": nil, "b": true, "c": false} — nil kept as literal, bools columnar.
    let ev = vec![0x83, 0xA1, b'a', 0xC0, 0xA1, b'b', 0xC3, 0xA1, b'c', 0xC2];
    assert_roundtrip(&[ev], EncodeOpts::default());
}

// ---------------------------------------------------------------------------
// Property test: random canonical msgpack must shred AND round-trip exact.
// ---------------------------------------------------------------------------

fn put_str(out: &mut Vec<u8>, s: &[u8]) { emit_str(out, s); }

fn gen_value(rng: &mut Rng, out: &mut Vec<u8>, depth: u32) {
    // At depth cap, only scalars.
    let choice = if depth == 0 { rng.below(4) } else { rng.below(6) };
    match choice {
        0 => emit_int(out, rng.next_u64() as i64),
        1 => {
            let l = rng.below(40) as usize;
            let s = rng.word(l);
            put_str(out, s.as_bytes());
        }
        2 => out.push(if rng.below(2) == 0 { 0xC3 } else { 0xC2 }),
        3 => out.push(0xC0), // nil
        4 => {
            // array
            let n = rng.below(5);
            if n < 16 {
                out.push(0x90 | n as u8);
            }
            for _ in 0..n {
                gen_value(rng, out, depth - 1);
            }
        }
        _ => {
            // map with distinct short string keys
            let n = rng.below(6);
            if n < 16 {
                out.push(0x80 | n as u8);
            }
            for k in 0..n {
                let key = format!("k{k}");
                put_str(out, key.as_bytes());
                gen_value(rng, out, depth - 1);
            }
        }
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn property_random_canonical_msgpack() {
    let mut rng = Rng::new(0xBADF00D);
    // Build blocks of canonical msgpack values with a fixed top-level map shape
    // family so columns are shared across events, exercising the shredder.
    for _ in 0..40 {
        let n = 1 + rng.below(128) as usize;
        let evs: Vec<Vec<u8>> = (0..n)
            .map(|_| {
                let mut v = Vec::new();
                // top-level map so it looks like an envelope
                v.push(0x82);
                put_str(&mut v, b"id");
                emit_int(&mut v, rng.next_u64() as i64);
                put_str(&mut v, b"body");
                gen_value(&mut rng, &mut v, 3);
                v
            })
            .collect();
        assert_roundtrip(&evs, EncodeOpts::default());
        assert_roundtrip(&evs, EncodeOpts { level: 3, per_column: true });
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn arbitrary_bytes_never_panic() {
    // Fuzz-ish: many random byte blocks through encode/decode/reassemble.
    let mut rng = Rng::new(0x5EED);
    for _ in 0..200 {
        let n = 1 + rng.below(16) as usize;
        let evs: Vec<Vec<u8>> = (0..n)
            .map(|_| {
                let len = rng.below(48) as usize;
                (0..len).map(|_| rng.next_u64() as u8).collect()
            })
            .collect();
        let r = refs(&evs);
        let enc = encode_block(
            &r,
            EncodeOpts { level: 1, per_column: rng.below(2) == 0 },
        );
        let block = Block::decode(&enc).unwrap();
        let mut out = Vec::new();
        let mut offs = Vec::new();
        block.reassemble_all(&mut out, &mut offs).unwrap();
        for (i, w) in offs.windows(2).enumerate() {
            assert_eq!(&out[w[0] as usize..w[1] as usize], evs[i].as_slice());
        }
    }
}

// ---------------------------------------------------------------------------
// Corrupt-block decode: never panic, surface CodecError
// ---------------------------------------------------------------------------

#[test]
fn decode_rejects_bad_header() {
    assert_eq!(Block::decode(&[]).unwrap_err(), CodecError::Truncated);
    assert!(matches!(
        Block::decode(&[9, 0]),
        Err(CodecError::UnsupportedVersion(9))
    ));
    assert!(matches!(
        Block::decode(&[COLUMNAR_VERSION, 0b1000_0000]),
        Err(CodecError::ReservedFlags(_))
    ));
}

/// Regression (bn-meo fuzzing, `fuzz_reassemble_block` crash
/// `crash-c4d78dac2593fedadf4d3d484c82cb4ec520e741`): a raw-fallback
/// block's offset table validated only the *last* offset against the
/// payload region. A non-monotonic table — some interior offset far past
/// the buffer while the final offset still looks in-bounds — slipped
/// through `decode_raw` and then panicked slicing `buf` in
/// `reassemble_range` ("range end index ... out of range for slice").
#[test]
fn decode_raw_rejects_non_monotonic_offsets() {
    // Raw block body: u32 ulen, then a zstd frame of the row image
    // (u16 n, (n+1) u32 offsets, payload bytes). n = 2 events, 3 offsets:
    // [0, 0xFFFF_FFFF, 4] — offsets[1] is huge but offsets[2] (the last)
    // is small and in-bounds, so only checking the last offset misses it.
    let mut image = Vec::new();
    image.extend_from_slice(&2u16.to_le_bytes()); // n = 2 events
    image.extend_from_slice(&0u32.to_le_bytes()); // off[0]
    image.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes()); // off[1] -- bogus
    image.extend_from_slice(&4u32.to_le_bytes()); // off[2] -- looks fine alone
    image.extend_from_slice(&[1, 2, 3, 4]); // 4 bytes of "payload"

    let compressed = zstd::bulk::compress(&image, 3).unwrap();
    let mut body = Vec::new();
    body.extend_from_slice(&(image.len() as u32).to_le_bytes());
    body.extend_from_slice(&compressed);

    let mut block = vec![COLUMNAR_VERSION, 0]; // FLAG_COLUMNAR clear = raw block
    block.extend_from_slice(&body);

    // Must fail loudly at decode, never panic downstream in reassembly.
    assert_eq!(Block::decode(&block).unwrap_err(), CodecError::Truncated);
}

/// Regression (bn-meo fuzzing): a per-column block's directory has one
/// `ulen` per column (up to `n_cols`, a u16), unlike the whole-block/raw
/// layouts' single `ulen` field. Before the fix, only each column's
/// individual `ulen` was capped at `MAX_ULEN` (64 MiB) — nothing capped the
/// *sum* across columns, so a directory naming a handful of columns each
/// claiming a large `ulen` (in practice backed by tiny, highly-compressible
/// zstd frames, e.g. all-zero data) could force allocating far more memory
/// than the ~38-byte input here would suggest. This hand-built directory
/// never even reaches a real zstd frame — `clen` is 0 for every column — so
/// the aggregate check must reject it purely from the header numbers, before
/// any decompression is attempted.
#[test]
fn decode_percol_rejects_aggregate_ulen_bomb() {
    let mut body = Vec::new();
    body.extend_from_slice(&0u16.to_le_bytes()); // n events
    body.extend_from_slice(&0u16.to_le_bytes()); // n_skels
    body.extend_from_slice(&3u16.to_le_bytes()); // n_cols
    // 3 columns, each claiming a 30_000_000-byte `ulen` (sum 90_000_000 >
    // MAX_ULEN's 64 MiB) with 0 compressed bytes (clen = 0) — the aggregate
    // check must fire before any of these clen=0 "columns" are read.
    for _ in 0..3 {
        body.push(K_INT);
        body.push(ENC_RAW);
        body.extend_from_slice(&0u32.to_le_bytes()); // clen
        body.extend_from_slice(&30_000_000u32.to_le_bytes()); // ulen
    }
    let mut block = vec![COLUMNAR_VERSION, FLAG_COLUMNAR | FLAG_PERCOL];
    block.extend_from_slice(&body);
    assert_eq!(block.len(), 38);
    assert_eq!(
        Block::decode(&block).unwrap_err(),
        CodecError::TotalUlenExceeded
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn decode_truncated_body_is_error_not_panic() {
    let mut rng = Rng::new(3);
    let evs: Vec<Vec<u8>> =
        (0..16).map(|_| payload(&mut rng, 0, 1, 0, EPOCH_2026_MS)).collect();
    let r = refs(&evs);
    let enc = encode_block(&r, EncodeOpts::default());
    for cut in 2..enc.len() {
        // Truncating the compressed body should error, never panic.
        let _ = Block::decode(&enc[..cut]);
    }
}

// ---------------------------------------------------------------------------
// Bench: ratio + throughput + point-read on a 1M-event corpus.
// Run with:  cargo test -p mess-index --release columnar_bench -- --ignored
// --nocapture
// ---------------------------------------------------------------------------

#[test]
#[ignore = "perf bench; run explicitly with --ignored --nocapture"]
#[cfg_attr(miri, ignore)]
fn columnar_bench() {
    use std::time::Instant;
    const TOTAL: usize = 1_000_000;
    const BE: usize = 128;
    let blocks = corpus_blocks(TOTAL, BE, 10_000);
    let total_events: usize = blocks.iter().map(|b| b.len()).sum();
    let raw_bytes: usize = blocks.iter().flatten().map(|e| e.len()).sum();

    for per_column in [false, true] {
        let opts = EncodeOpts { level: 9, per_column };
        let mut enc = BlockEncoder::new(opts);

        // shred / encode
        let t = Instant::now();
        let mut encoded: Vec<Vec<u8>> = Vec::with_capacity(blocks.len());
        for blk in &blocks {
            let r = refs(blk);
            encoded.push(enc.encode(&r));
        }
        let shred_dt = t.elapsed();
        let comp_bytes: usize = encoded.iter().map(|e| e.len()).sum();
        let raw_fallbacks = encoded.iter().filter(|e| is_raw(e)).count();

        // decode + reassemble every event; verify byte-exact
        let t = Instant::now();
        let mut out = Vec::new();
        let mut offs = Vec::new();
        let mut mismatches = 0u64;
        let mut ev_i = 0usize;
        for (bi, e) in encoded.iter().enumerate() {
            let block = Block::decode(e).unwrap();
            out.clear();
            offs.clear();
            block.reassemble_all(&mut out, &mut offs).unwrap();
            for (k, w) in offs.windows(2).enumerate() {
                if &out[w[0] as usize..w[1] as usize]
                    != blocks[bi][k].as_slice()
                {
                    mismatches += 1;
                }
                ev_i += 1;
            }
        }
        let re_dt = t.elapsed();
        assert_eq!(mismatches, 0, "byte-exact reassembly failed");
        assert_eq!(ev_i, total_events);

        // point reads: sample one event per block
        let decoded: Vec<Block> =
            encoded.iter().map(|e| Block::decode(e).unwrap()).collect();
        let t = Instant::now();
        let mut sink = 0u64;
        let mut pr = 0usize;
        for b in &decoded {
            if b.is_empty() {
                continue;
            }
            let idx = b.len() / 2;
            let v = b.reassemble_one(idx).unwrap();
            sink = sink.wrapping_add(v.len() as u64 + v[0] as u64);
            pr += 1;
        }
        let pr_dt = t.elapsed();
        std::hint::black_box(sink);

        let label = if per_column { "per-column" } else { "whole-block" };
        eprintln!("=== {label} (zstd-9, {BE}-event blocks) ===");
        eprintln!("  events            {total_events}");
        eprintln!(
            "  raw payload       {raw_bytes} B ({:.1} B/event)",
            raw_bytes as f64 / total_events as f64
        );
        eprintln!(
            "  compressed        {comp_bytes} B ({:.2} B/event, ratio {:.2}x)",
            comp_bytes as f64 / total_events as f64,
            raw_bytes as f64 / comp_bytes as f64
        );
        eprintln!(
            "  raw fallbacks     {raw_fallbacks}/{} blocks",
            encoded.len()
        );
        eprintln!(
            "  shred throughput  {:.2} M ev/s",
            total_events as f64 / shred_dt.as_secs_f64() / 1e6
        );
        eprintln!(
            "  reassemble        {:.2} M ev/s",
            total_events as f64 / re_dt.as_secs_f64() / 1e6
        );
        eprintln!(
            "  point read        {:.2} us/read ({pr} reads)",
            pr_dt.as_secs_f64() * 1e6 / pr as f64
        );
    }
}
