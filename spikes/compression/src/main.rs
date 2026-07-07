//! Compression spike for mess (design doc 12_convergence.md, section D6).
//!
//! Measures per-category zstd compression of schema-homogeneous JSON event
//! payloads: per-event vs block, with and without trained dictionaries.
//!
//! Run with: cargo run --release

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::json;
use std::hint::black_box;
use std::time::Instant;

const EVENTS_PER_CATEGORY: usize = 50_000;
const TRAIN_SAMPLES: usize = 5_000;
const BLOCK_EVENTS: usize = 128;
const ZSTD_LEVEL: i32 = 3;
const DICT_SIZES: [usize; 2] = [16 * 1024, 64 * 1024];
const RANDOM_READS: usize = 20_000;

// ---------------------------------------------------------------------------
// id / text generators
// ---------------------------------------------------------------------------

const CROCKFORD: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

fn ulid(rng: &mut StdRng, ts_ms: u64) -> String {
    let mut out = [0u8; 26];
    let mut t = ts_ms;
    for i in (0..10).rev() {
        out[i] = CROCKFORD[(t & 31) as usize];
        t >>= 5;
    }
    for slot in out.iter_mut().skip(10) {
        *slot = CROCKFORD[rng.random_range(0..32usize)];
    }
    String::from_utf8(out.to_vec()).unwrap()
}

fn uuid(rng: &mut StdRng) -> String {
    let b: u128 = rng.random();
    let h = format!("{b:032x}");
    format!(
        "{}-{}-4{}-{}{}-{}",
        &h[0..8],
        &h[8..12],
        &h[13..16],
        ["8", "9", "a", "b"][rng.random_range(0..4usize)],
        &h[17..20],
        &h[20..32]
    )
}

const VOCAB: &[&str] = &[
    "the", "a", "of", "to", "and", "in", "that", "for", "with", "just", "really", "never",
    "always", "today", "morning", "coffee", "team", "shipping", "release", "finally", "great",
    "terrible", "amazing", "weather", "meeting", "project", "deadline", "weekend", "friday",
    "monday", "thinking", "about", "building", "database", "event", "stream", "cannot",
    "believe", "how", "fast", "slow", "new", "old", "update", "broke", "fixed", "again",
    "everyone", "should", "read", "this", "thread", "hot", "take", "unpopular", "opinion",
    "honestly", "actually", "literally", "working", "from", "home", "office", "commute",
    "train", "delayed", "traffic", "lunch", "dinner", "recipe", "tried", "making", "bread",
    "sourdough", "garden", "tomatoes", "growing", "season", "playoffs", "game", "score",
    "overtime", "incredible", "watch", "movie", "series", "episode", "spoilers", "book",
    "reading", "chapter", "author", "music", "album", "concert", "tickets", "sold", "out",
    "running", "marathon", "training", "mile", "personal", "best", "kids", "school", "dog",
    "cat", "walk", "park", "beach", "vacation", "flight", "airport", "hotel", "review",
    "stars", "recommend", "avoid", "customer", "service", "waited", "hours", "refund",
    "battery", "phone", "laptop", "keyboard", "mechanical", "setup", "desk", "monitor",
    "conference", "talk", "slides", "keynote", "announced", "launch", "startup", "funding",
    "hiring", "remote", "interview", "offer", "quit", "job", "boss", "colleague", "code",
    "review", "merge", "conflict", "debugging", "production", "incident", "postmortem",
    "coverage", "tests", "passing", "deploy", "rollback", "latency", "throughput", "cache",
    "invalidation", "naming", "things", "hard", "problems", "computer", "science", "rust",
    "compiler", "borrow", "checker", "lifetime", "async", "await", "runtime", "thread",
];

fn english_ish(rng: &mut StdRng, min_words: usize, max_words: usize) -> String {
    let n = rng.random_range(min_words..=max_words);
    let mut s = String::new();
    let mut cap = true;
    for i in 0..n {
        let w = VOCAB[rng.random_range(0..VOCAB.len())];
        if !s.is_empty() {
            s.push(' ');
        }
        if cap {
            let mut c = w.chars();
            if let Some(f) = c.next() {
                s.push(f.to_ascii_uppercase());
                s.push_str(c.as_str());
            }
            cap = false;
        } else {
            s.push_str(w);
        }
        if i + 1 < n && rng.random_bool(0.08) {
            s.push(',');
        } else if i + 1 < n && rng.random_bool(0.06) {
            s.push('.');
            cap = true;
        }
    }
    s.push('.');
    s
}

fn rfc3339(rng: &mut StdRng, base_ms: u64) -> (String, u64) {
    // Advance a synthetic clock with jitter; format epoch ms as RFC3339-ish.
    let ms = base_ms + rng.random_range(1..30_000u64);
    let secs = ms / 1000;
    let sub = ms % 1000;
    // crude civil-time conversion (good enough for realistic-looking variance)
    let days = secs / 86_400;
    let tod = secs % 86_400;
    let (h, m, s) = (tod / 3600, (tod % 3600) / 60, tod % 60);
    let year = 2024 + (days / 365) % 3;
    let doy = days % 365;
    let month = 1 + (doy / 31).min(11);
    let dom = 1 + doy % 28;
    (
        format!("{year:04}-{month:02}-{dom:02}T{h:02}:{m:02}:{s:02}.{sub:03}Z"),
        ms,
    )
}

// ---------------------------------------------------------------------------
// category generators
// ---------------------------------------------------------------------------

fn gen_transactions(n: usize) -> Vec<Vec<u8>> {
    let mut rng = StdRng::seed_from_u64(0xACC7);
    let currencies = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "SEK"];
    let channels = ["card", "ach", "wire", "p2p", "atm", "check"];
    let types = [
        "TransactionPosted",
        "TransactionPending",
        "TransactionReversed",
    ];
    let mut ts = 1_720_000_000_000u64;
    (0..n)
        .map(|_| {
            let (posted_at, new_ts) = rfc3339(&mut rng, ts);
            ts = new_ts;
            let amount = (rng.random_range(1..2_000_000i64) as f64) / 100.0
                * if rng.random_bool(0.55) { -1.0 } else { 1.0 };
            let balance = (rng.random_range(0..90_000_000i64) as f64) / 100.0;
            let v = json!({
                "event_type": types[rng.random_range(0..types.len())],
                "transaction_id": ulid(&mut rng, ts),
                "account_id": format!("acct_{}", uuid(&mut rng)),
                "counterparty_id": format!("acct_{}", uuid(&mut rng)),
                "amount": format!("{amount:.2}"),
                "currency": currencies[rng.random_range(0..currencies.len())],
                "channel": channels[rng.random_range(0..channels.len())],
                "posted_at": posted_at,
                "balance_after": format!("{balance:.2}"),
                "reference": format!("REF-{:08X}", rng.random::<u32>()),
            });
            serde_json::to_vec(&v).unwrap()
        })
        .collect()
}

fn gen_social_posts(n: usize) -> Vec<Vec<u8>> {
    let mut rng = StdRng::seed_from_u64(0x50C1);
    let clients = ["web", "ios", "android", "api"];
    let langs = ["en", "en", "en", "en", "es", "de", "fr", "pt"];
    let tags = [
        "rustlang", "coffee", "nba", "wfh", "buildinpublic", "cooking", "running", "movies",
        "music", "gamedev",
    ];
    let mut ts = 1_720_000_000_000u64;
    (0..n)
        .map(|_| {
            let (created_at, new_ts) = rfc3339(&mut rng, ts);
            ts = new_ts;
            let mentions: Vec<String> = (0..rng.random_range(0..4usize))
                .map(|_| format!("user_{}", &uuid(&mut rng)[0..8]))
                .collect();
            let hashtags: Vec<&str> = (0..rng.random_range(0..3usize))
                .map(|_| tags[rng.random_range(0..tags.len())])
                .collect();
            let reply_to = if rng.random_bool(0.35) {
                serde_json::Value::String(ulid(&mut rng, ts))
            } else {
                serde_json::Value::Null
            };
            let v = json!({
                "event_type": "PostCreated",
                "post_id": ulid(&mut rng, ts),
                "author_id": format!("user_{}", uuid(&mut rng)),
                "body": english_ish(&mut rng, 5, 80),
                "mentions": mentions,
                "hashtags": hashtags,
                "lang": langs[rng.random_range(0..langs.len())],
                "client": clients[rng.random_range(0..clients.len())],
                "reply_to": reply_to,
                "created_at": created_at,
            });
            serde_json::to_vec(&v).unwrap()
        })
        .collect()
}

fn gen_orders(n: usize) -> Vec<Vec<u8>> {
    let mut rng = StdRng::seed_from_u64(0x08DE);
    let types = [
        "OrderPlaced",
        "OrderPaid",
        "OrderShipped",
        "OrderDelivered",
        "OrderCancelled",
    ];
    let streets = [
        "Maple Ave", "Oak St", "Cedar Ln", "Elm Dr", "Pine Rd", "Birch Blvd", "Walnut Way",
        "2nd St", "Highland Ave", "Sunset Blvd",
    ];
    let cities = [
        ("Portland", "OR", "972"),
        ("Austin", "TX", "787"),
        ("Denver", "CO", "802"),
        ("Madison", "WI", "537"),
        ("Raleigh", "NC", "276"),
        ("Tucson", "AZ", "857"),
        ("Boise", "ID", "837"),
        ("Buffalo", "NY", "142"),
    ];
    let carriers = ["ups", "fedex", "usps", "dhl"];
    let mut ts = 1_720_000_000_000u64;
    (0..n)
        .map(|_| {
            let (at, new_ts) = rfc3339(&mut rng, ts);
            ts = new_ts;
            let items: Vec<serde_json::Value> = (0..rng.random_range(1..5usize))
                .map(|_| {
                    json!({
                        "sku": format!("SKU-{:04}-{:04X}", rng.random_range(0..10_000u32), rng.random::<u16>()),
                        "qty": rng.random_range(1..6u32),
                        "unit_price": format!("{:.2}", rng.random_range(99..49_999u32) as f64 / 100.0),
                    })
                })
                .collect();
            let (city, state, zip3) = cities[rng.random_range(0..cities.len())];
            let v = json!({
                "event_type": types[rng.random_range(0..types.len())],
                "order_id": ulid(&mut rng, ts),
                "customer_id": format!("cust_{}", uuid(&mut rng)),
                "items": items,
                "shipping_address": {
                    "line1": format!("{} {}", rng.random_range(1..9_999u32), streets[rng.random_range(0..streets.len())]),
                    "city": city,
                    "state": state,
                    "zip": format!("{zip3}{:02}", rng.random_range(0..100u32)),
                    "country": "US",
                },
                "carrier": carriers[rng.random_range(0..carriers.len())],
                "occurred_at": at,
            });
            serde_json::to_vec(&v).unwrap()
        })
        .collect()
}

fn gen_iot(n: usize) -> Vec<Vec<u8>> {
    let mut rng = StdRng::seed_from_u64(0x107D);
    // fleet of 500 devices, so ids repeat but readings vary
    let devices: Vec<String> = (0..500)
        .map(|_| {
            let b: u64 = rng.random();
            format!("dev-{:012x}", b & 0xFFFF_FFFF_FFFF)
        })
        .collect();
    let mut ts = 1_720_000_000_000u64;
    let mut seq = 0u64;
    (0..n)
        .map(|_| {
            ts += rng.random_range(50..2_000u64);
            seq += 1;
            let v = json!({
                "event_type": "SensorReading",
                "device_id": devices[rng.random_range(0..devices.len())],
                "site": format!("site-{:02}", rng.random_range(0..40u32)),
                "ts": ts,
                "seq": seq,
                "metrics": {
                    "temp_c": (rng.random_range(-1_000..4_500i32) as f64) / 100.0,
                    "humidity_pct": (rng.random_range(100..9_900i32) as f64) / 100.0,
                    "battery_v": (rng.random_range(310..420i32) as f64) / 100.0,
                    "rssi": -rng.random_range(40..110i32),
                },
                "fw": format!("1.{}.{}", rng.random_range(0..6u32), rng.random_range(0..12u32)),
            });
            serde_json::to_vec(&v).unwrap()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// measurement
// ---------------------------------------------------------------------------

struct StrategyResult {
    name: String,
    compressed: u64,
    ratio: f64,
    comp_mb_s: f64,
    decomp_mb_s: f64,
}

fn mb_s(bytes: u64, secs: f64) -> f64 {
    (bytes as f64 / 1_000_000.0) / secs
}

/// Compress each event individually; returns (result, compressed events).
fn per_event(
    name: &str,
    eval: &[Vec<u8>],
    raw: u64,
    dict: Option<&[u8]>,
) -> (StrategyResult, Vec<Vec<u8>>) {
    let mut comp = match dict {
        Some(d) => zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, d).unwrap(),
        None => zstd::bulk::Compressor::new(ZSTD_LEVEL).unwrap(),
    };
    let t0 = Instant::now();
    let out: Vec<Vec<u8>> = eval.iter().map(|e| comp.compress(e).unwrap()).collect();
    let ct = t0.elapsed().as_secs_f64();

    let mut dec = match dict {
        Some(d) => zstd::bulk::Decompressor::with_dictionary(d).unwrap(),
        None => zstd::bulk::Decompressor::new().unwrap(),
    };
    let t0 = Instant::now();
    let mut check = 0usize;
    for (c, orig) in out.iter().zip(eval) {
        let d = dec.decompress(c, orig.len()).unwrap();
        check += d.len();
    }
    let dt = t0.elapsed().as_secs_f64();
    assert_eq!(check as u64, raw);
    black_box(check);

    let compressed: u64 = out.iter().map(|c| c.len() as u64).sum();
    (
        StrategyResult {
            name: name.to_string(),
            compressed,
            ratio: raw as f64 / compressed as f64,
            comp_mb_s: mb_s(raw, ct),
            decomp_mb_s: mb_s(raw, dt),
        },
        out,
    )
}

/// Concatenate events into blocks of BLOCK_EVENTS, compress each block.
fn block(
    name: &str,
    eval: &[Vec<u8>],
    raw: u64,
    dict: Option<&[u8]>,
) -> (StrategyResult, Vec<Vec<u8>>, Vec<usize>) {
    let blocks: Vec<Vec<u8>> = eval
        .chunks(BLOCK_EVENTS)
        .map(|ch| ch.concat())
        .collect();
    let block_sizes: Vec<usize> = blocks.iter().map(|b| b.len()).collect();

    let mut comp = match dict {
        Some(d) => zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, d).unwrap(),
        None => zstd::bulk::Compressor::new(ZSTD_LEVEL).unwrap(),
    };
    let t0 = Instant::now();
    let out: Vec<Vec<u8>> = blocks.iter().map(|b| comp.compress(b).unwrap()).collect();
    let ct = t0.elapsed().as_secs_f64();

    let mut dec = match dict {
        Some(d) => zstd::bulk::Decompressor::with_dictionary(d).unwrap(),
        None => zstd::bulk::Decompressor::new().unwrap(),
    };
    let t0 = Instant::now();
    let mut check = 0usize;
    for (c, b) in out.iter().zip(&blocks) {
        let d = dec.decompress(c, b.len()).unwrap();
        check += d.len();
    }
    let dt = t0.elapsed().as_secs_f64();
    assert_eq!(check as u64, raw);
    black_box(check);

    let compressed: u64 = out.iter().map(|c| c.len() as u64).sum();
    (
        StrategyResult {
            name: name.to_string(),
            compressed,
            ratio: raw as f64 / compressed as f64,
            comp_mb_s: mb_s(raw, ct),
            decomp_mb_s: mb_s(raw, dt),
        },
        out,
        block_sizes,
    )
}

/// Avg latency (ns) to decode one random event compressed individually.
fn random_read_per_event(compressed: &[Vec<u8>], sizes: &[usize], dict: Option<&[u8]>) -> f64 {
    let mut dec = match dict {
        Some(d) => zstd::bulk::Decompressor::with_dictionary(d).unwrap(),
        None => zstd::bulk::Decompressor::new().unwrap(),
    };
    let mut rng = StdRng::seed_from_u64(0x2EAD);
    let idx: Vec<usize> = (0..RANDOM_READS)
        .map(|_| rng.random_range(0..compressed.len()))
        .collect();
    let t0 = Instant::now();
    let mut check = 0usize;
    for &i in &idx {
        check += dec.decompress(&compressed[i], sizes[i]).unwrap().len();
    }
    black_box(check);
    t0.elapsed().as_nanos() as f64 / RANDOM_READS as f64
}

/// Avg latency (ns) to fetch one random event when it lives inside a
/// compressed block: decompress the whole block to extract one event.
fn random_read_block(compressed: &[Vec<u8>], sizes: &[usize], dict: Option<&[u8]>) -> f64 {
    let mut dec = match dict {
        Some(d) => zstd::bulk::Decompressor::with_dictionary(d).unwrap(),
        None => zstd::bulk::Decompressor::new().unwrap(),
    };
    let mut rng = StdRng::seed_from_u64(0xB10C);
    let idx: Vec<usize> = (0..RANDOM_READS)
        .map(|_| rng.random_range(0..compressed.len()))
        .collect();
    let t0 = Instant::now();
    let mut check = 0usize;
    for &i in &idx {
        check += dec.decompress(&compressed[i], sizes[i]).unwrap().len();
    }
    black_box(check);
    t0.elapsed().as_nanos() as f64 / RANDOM_READS as f64
}

fn run_category(name: &str, events: Vec<Vec<u8>>) {
    let (train, eval) = events.split_at(TRAIN_SAMPLES);
    let raw: u64 = eval.iter().map(|e| e.len() as u64).sum();
    let avg = raw as f64 / eval.len() as f64;
    let sizes: Vec<usize> = eval.iter().map(|e| e.len()).collect();

    println!("\n## Category: {name}");
    println!(
        "eval events: {}  raw: {:.2} MB  avg event: {:.0} B  (trained on {} held-out samples)",
        eval.len(),
        raw as f64 / 1_000_000.0,
        avg,
        train.len()
    );

    // dictionaries
    let mut dicts = Vec::new();
    for &max in &DICT_SIZES {
        let t0 = Instant::now();
        let dict = zstd::dict::from_samples(train, max).expect("dict training failed");
        let dt = t0.elapsed();
        println!(
            "dict (max {} KiB): trained size {} B in {:.0} ms",
            max / 1024,
            dict.len(),
            dt.as_secs_f64() * 1000.0
        );
        dicts.push((max / 1024, dict));
    }

    let mut results: Vec<StrategyResult> = Vec::new();
    let mut latencies: Vec<(String, f64)> = Vec::new();

    // per-event, no dict
    let (r, comp) = per_event("per-event  L3  no dict", eval, raw, None);
    latencies.push((
        "per-event no dict".into(),
        random_read_per_event(&comp, &sizes, None),
    ));
    results.push(r);

    // per-event with dicts
    for (k, dict) in &dicts {
        let (r, comp) = per_event(&format!("per-event  L3  dict {k}K"), eval, raw, Some(dict));
        latencies.push((
            format!("per-event dict {k}K"),
            random_read_per_event(&comp, &sizes, Some(dict)),
        ));
        results.push(r);
    }

    // block, no dict
    let (r, comp, bsizes) = block("block-128  L3  no dict", eval, raw, None);
    latencies.push((
        "block-128 no dict".into(),
        random_read_block(&comp, &bsizes, None),
    ));
    results.push(r);

    // block with dicts
    for (k, dict) in &dicts {
        let (r, comp, bsizes) = block(&format!("block-128  L3  dict {k}K"), eval, raw, Some(dict));
        latencies.push((
            format!("block-128 dict {k}K"),
            random_read_block(&comp, &bsizes, Some(dict)),
        ));
        results.push(r);
    }

    println!(
        "\n{:<24} {:>12} {:>7} {:>12} {:>12}",
        "strategy", "compressed", "ratio", "comp MB/s", "decomp MB/s"
    );
    for r in &results {
        println!(
            "{:<24} {:>10} B {:>6.2}x {:>12.0} {:>12.0}",
            r.name, r.compressed, r.ratio, r.comp_mb_s, r.decomp_mb_s
        );
    }

    println!("\nrandom-read latency (decode one event, avg of {RANDOM_READS} reads):");
    for (n, ns) in &latencies {
        println!("  {:<22} {:>8.2} us", n, ns / 1000.0);
    }
}

fn main() {
    println!("compression spike: zstd L{ZSTD_LEVEL}, {EVENTS_PER_CATEGORY} events/category, block = {BLOCK_EVENTS} events");
    println!("dict training: {TRAIN_SAMPLES} samples (held out of evaluation)");

    let cats: Vec<(&str, fn(usize) -> Vec<Vec<u8>>)> = vec![
        ("account-transactions", gen_transactions),
        ("social-posts", gen_social_posts),
        ("order-lifecycle", gen_orders),
        ("iot-sensor-readings", gen_iot),
    ];

    for (name, gen) in cats {
        let t0 = Instant::now();
        let events = gen(EVENTS_PER_CATEGORY);
        let gt = t0.elapsed();
        eprintln!("[gen {name}: {:.1}s]", gt.as_secs_f64());
        run_category(name, events);
    }
}
