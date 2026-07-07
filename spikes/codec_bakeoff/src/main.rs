//! Codec bake-off: size, compression, speed, and the evolution matrix.
//! Run with: cargo run --release

use codec_bakeoff::codecs::{Codec, ALL_CODECS};
use codec_bakeoff::events::{corpus, gen_order, gen_shipment, gen_user};
use codec_bakeoff::evolution;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::hint::black_box;
use std::time::Instant;

const N_TRAIN: usize = 5_000; // held-out dictionary training samples per type
const N_TEST: usize = 10_000; // measured corpus per type
const DICT_SIZE: usize = 16 * 1024;
const ZSTD_LEVEL: i32 = 3;
const BLOCK_EVENTS: usize = 128;
const SPEED_REPS: usize = 5;

struct TypeResult {
    type_name: &'static str,
    n: usize,
    raw_bytes: u64,
    // per-event compression
    zstd_solo_bytes: u64,
    zstd_dict_bytes: u64,
    // 128-event blocks
    block_bytes: u64,
    block_dict_bytes: u64,
    // speed (seconds, best of SPEED_REPS, over all n events)
    enc_s: f64,
    dec_s: f64,
    enc_dict_s: f64, // encode + zstd-dict compress
    dec_dict_s: f64, // zstd-dict decompress + decode
}

fn best_of<F: FnMut()>(reps: usize, mut f: F) -> f64 {
    let mut best = f64::MAX;
    for _ in 0..reps {
        let t = Instant::now();
        f();
        best = best.min(t.elapsed().as_secs_f64());
    }
    best
}

fn bench_type<T>(
    type_name: &'static str,
    codec: Codec,
    train: &[T],
    test: &[T],
) -> (Vec<Vec<u8>>, TypeResult)
where
    T: Serialize + DeserializeOwned,
{
    let train_enc: Vec<Vec<u8>> = train.iter().map(|e| codec.encode(e)).collect();
    let test_enc: Vec<Vec<u8>> = test.iter().map(|e| codec.encode(e)).collect();
    let raw_bytes: u64 = test_enc.iter().map(|b| b.len() as u64).sum();

    // Dictionary trained per codec per type on held-out samples.
    let dict = zstd::dict::from_samples(&train_enc, DICT_SIZE).expect("dict training");

    let mut comp_solo = zstd::bulk::Compressor::new(ZSTD_LEVEL).unwrap();
    let mut comp_dict = zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, &dict).unwrap();

    let zstd_solo_bytes: u64 = test_enc
        .iter()
        .map(|b| comp_solo.compress(b).unwrap().len() as u64)
        .sum();
    let dict_compressed: Vec<Vec<u8>> = test_enc
        .iter()
        .map(|b| comp_dict.compress(b).unwrap())
        .collect();
    let zstd_dict_bytes: u64 = dict_compressed.iter().map(|b| b.len() as u64).sum();

    // 128-event blocks: concatenated payloads, compressed as one unit
    // (with and without the dictionary).
    let mut block_bytes = 0u64;
    let mut block_dict_bytes = 0u64;
    for chunk in test_enc.chunks(BLOCK_EVENTS) {
        let concat: Vec<u8> = chunk.iter().flat_map(|b| b.iter().copied()).collect();
        block_bytes += comp_solo.compress(&concat).unwrap().len() as u64;
        block_dict_bytes += comp_dict.compress(&concat).unwrap().len() as u64;
    }

    let max_raw = test_enc.iter().map(|b| b.len()).max().unwrap_or(0);

    // ---- speed ----
    let enc_s = best_of(SPEED_REPS, || {
        for e in test {
            black_box(codec.encode(black_box(e)));
        }
    });
    let dec_s = best_of(SPEED_REPS, || {
        for b in &test_enc {
            black_box(codec.decode::<T>(black_box(b)).unwrap());
        }
    });
    let enc_dict_s = best_of(SPEED_REPS, || {
        for e in test {
            let b = codec.encode(black_box(e));
            black_box(comp_dict.compress(&b).unwrap());
        }
    });
    let mut dec = zstd::bulk::Decompressor::with_dictionary(&dict).unwrap();
    let dec_dict_s = best_of(SPEED_REPS, || {
        for b in &dict_compressed {
            let raw = dec.decompress(black_box(b), max_raw + 64).unwrap();
            black_box(codec.decode::<T>(&raw).unwrap());
        }
    });

    (
        test_enc,
        TypeResult {
            type_name,
            n: test.len(),
            raw_bytes,
            zstd_solo_bytes,
            zstd_dict_bytes,
            block_bytes,
            block_dict_bytes,
            enc_s,
            dec_s,
            enc_dict_s,
            dec_dict_s,
        },
    )
}

fn main() {
    let (order_train, order_test) = corpus(0xC0DEC_01, N_TRAIN, N_TEST, gen_order);
    let (user_train, user_test) = corpus(0xC0DEC_02, N_TRAIN, N_TEST, gen_user);
    let (ship_train, ship_test) = corpus(0xC0DEC_03, N_TRAIN, N_TEST, gen_shipment);

    println!("# codec bake-off");
    println!();
    println!(
        "corpus: {} test + {} train events per type x 3 types; zstd level {}, {} KiB dictionaries; blocks of {} events",
        N_TEST, N_TRAIN, ZSTD_LEVEL, DICT_SIZE / 1024, BLOCK_EVENTS
    );
    println!();

    let mut per_codec: Vec<(Codec, Vec<TypeResult>)> = Vec::new();
    for codec in ALL_CODECS {
        let (_, r1) = bench_type("OrderPlaced", codec, &order_train, &order_test);
        let (_, r2) = bench_type("UserRegistered", codec, &user_train, &user_test);
        let (_, r3) = bench_type("ShipmentEvent", codec, &ship_train, &ship_test);
        per_codec.push((codec, vec![r1, r2, r3]));
        eprintln!("done: {}", codec.name());
    }

    // ---- Table 1: raw mean size per event, per type + overall ----
    println!("## Table 1 — mean encoded size (bytes/event, uncompressed)");
    println!();
    println!("| codec | OrderPlaced | UserRegistered | ShipmentEvent | overall |");
    println!("|---|---|---|---|---|");
    for (codec, rs) in &per_codec {
        let total: u64 = rs.iter().map(|r| r.raw_bytes).sum();
        let n: usize = rs.iter().map(|r| r.n).sum();
        print!("| {} |", codec.name());
        for r in rs {
            print!(" {:.1} |", r.raw_bytes as f64 / r.n as f64);
        }
        println!(" {:.1} |", total as f64 / n as f64);
    }
    println!();

    // ---- Table 2: compressed sizes (overall means) ----
    println!("## Table 2 — compressed size (bytes/event, overall mean)");
    println!();
    println!("| codec | raw | zstd-3 solo | zstd-3 +16KiB dict | 128-ev block | 128-ev block +dict | dict vs best-raw* |");
    println!("|---|---|---|---|---|---|---|");
    let best_raw = per_codec
        .iter()
        .map(|(_, rs)| rs.iter().map(|r| r.raw_bytes).sum::<u64>())
        .min()
        .unwrap() as f64
        / (3 * N_TEST) as f64;
    for (codec, rs) in &per_codec {
        let n: usize = rs.iter().map(|r| r.n).sum();
        let raw: u64 = rs.iter().map(|r| r.raw_bytes).sum();
        let solo: u64 = rs.iter().map(|r| r.zstd_solo_bytes).sum();
        let dict: u64 = rs.iter().map(|r| r.zstd_dict_bytes).sum();
        let block: u64 = rs.iter().map(|r| r.block_bytes).sum();
        let blockd: u64 = rs.iter().map(|r| r.block_dict_bytes).sum();
        let dict_mean = dict as f64 / n as f64;
        println!(
            "| {} | {:.1} | {:.1} | {:.1} | {:.1} | {:.1} | {:+.0}% |",
            codec.name(),
            raw as f64 / n as f64,
            solo as f64 / n as f64,
            dict_mean,
            block as f64 / n as f64,
            blockd as f64 / n as f64,
            (dict_mean / best_raw - 1.0) * 100.0
        );
    }
    println!();
    println!("*dict vs best-raw: per-event dictionary-compressed size relative to the smallest UNcompressed codec (postcard/bincode class).");
    println!();

    // ---- Table 3: speed ----
    println!("## Table 3 — throughput (30k events, best of {} reps)", SPEED_REPS);
    println!();
    println!("| codec | encode Mev/s | encode MB/s | decode Mev/s | decode MB/s | enc+dictzstd Mev/s | dictzstd+dec Mev/s |");
    println!("|---|---|---|---|---|---|---|");
    for (codec, rs) in &per_codec {
        let n: usize = rs.iter().map(|r| r.n).sum();
        let raw: u64 = rs.iter().map(|r| r.raw_bytes).sum();
        let enc_s: f64 = rs.iter().map(|r| r.enc_s).sum();
        let dec_s: f64 = rs.iter().map(|r| r.dec_s).sum();
        let encd_s: f64 = rs.iter().map(|r| r.enc_dict_s).sum();
        let decd_s: f64 = rs.iter().map(|r| r.dec_dict_s).sum();
        let mb = raw as f64 / 1e6;
        println!(
            "| {} | {:.2} | {:.0} | {:.2} | {:.0} | {:.2} | {:.2} |",
            codec.name(),
            n as f64 / enc_s / 1e6,
            mb / enc_s,
            n as f64 / dec_s / 1e6,
            mb / dec_s,
            n as f64 / encd_s / 1e6,
            n as f64 / decd_s / 1e6,
        );
    }
    println!();

    // ---- per-type detail for the report appendix ----
    println!("## Appendix — per-type dictionary compression (bytes/event)");
    println!();
    println!("| codec | type | raw | +dict | ratio |");
    println!("|---|---|---|---|---|");
    for (codec, rs) in &per_codec {
        for r in rs {
            println!(
                "| {} | {} | {:.1} | {:.1} | {:.2}x |",
                codec.name(),
                r.type_name,
                r.raw_bytes as f64 / r.n as f64,
                r.zstd_dict_bytes as f64 / r.n as f64,
                r.raw_bytes as f64 / r.zstd_dict_bytes as f64
            );
        }
    }
    println!();
    // ---- evolution matrix ----
    println!("## Evolution matrix (encode V1, decode V2)");
    println!();
    let matrix = evolution::run_matrix();
    print!("{}", evolution::matrix_markdown(&matrix));
    println!();
    println!("### SILENT-WRONG details");
    println!();
    for row in &matrix {
        for (codec, cell) in &row.cells {
            if let evolution::Cell::SilentWrong(d) = cell {
                println!("- `{}` / {}: {}", codec.name(), row.label, d);
            }
        }
    }
    println!();
    println!("### ERROR details (loud failures, acceptable)");
    println!();
    for row in &matrix {
        for (codec, cell) in &row.cells {
            if let evolution::Cell::Error(d) = cell {
                println!("- `{}` / {}: {}", codec.name(), row.label, d);
            }
        }
    }
}
