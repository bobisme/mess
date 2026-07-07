//! Corpus: the seal_pipeline workload regenerated at the codec decision.
//!
//! Same shape as spikes/seal_pipeline/src/workload.rs — 1M events, batches of
//! 10, 10,000 streams Zipf(1.1), 4 categories (stream % 4), same field
//! distributions — but payloads are **rmp-serde named-mode MessagePack**
//! (`codec_id 1`, the codec_bakeoff decision) instead of hand-formatted JSON.
//!
//! Deliberate deltas from seal_pipeline's corpus (noted in REPORT.md):
//!   - msgpack-named instead of JSON (the actual Phase 1 payload bytes);
//!   - timestamps are realistic monotone-ish i64 millis advancing with the
//!     log (seal_pipeline used one constant string, which compresses to
//!     nothing and flattered the row ratio);
//!   - `schema_v` int field instead of "v1" string.
//!
//! The corpus is materialized directly in SEALED CLUSTER ORDER
//! (category, stream, version) — the seal-time layout every measurement here
//! operates on — with the original per-event bytes kept as ground truth for
//! byte-exact verification.

use std::collections::HashMap;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rand_distr::{Distribution, Zipf};
use serde::Serialize;

pub const STREAMS: u64 = 10_000;
pub const BATCH: usize = 10;
pub const ZIPF_S: f64 = 1.1;
pub const CATEGORIES: usize = 4;
pub const CATEGORY_NAMES: [&str; CATEGORIES] = ["account", "order", "user", "sensor"];
pub const EPOCH_2026_MS: i64 = 1_767_225_600_000;

#[inline]
pub fn category_of(stream: u32) -> usize {
    (stream % CATEGORIES as u32) as usize
}

fn word(rng: &mut StdRng, len: usize) -> String {
    (0..len).map(|_| (b'a' + rng.random_range(0..26u8)) as char).collect()
}

// ---------------------------------------------------------------------------
// Event shapes (serde structs -> rmp_serde::to_vec_named)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct AccountCredited<'a> {
    stream: &'a str,
    seq: u64,
    amount_cents: u64,
    currency: &'a str,
    actor: String,
    source: &'a str,
    note: String,
    occurred_at_ms: i64,
    schema_v: u32,
}

#[derive(Serialize)]
struct LineItem {
    sku: String,
    qty: u32,
    price_cents: u32,
}

#[derive(Serialize)]
struct OrderPlaced<'a> {
    stream: &'a str,
    seq: u64,
    order_id: String,
    items: Vec<LineItem>,
    total_cents: u64,
    currency: &'a str,
    customer: String,
    placed_at_ms: i64,
    schema_v: u32,
}

#[derive(Serialize)]
struct ProfileFields<'a> {
    display_name: String,
    locale: &'a str,
    tz: &'a str,
    marketing_opt_in: bool,
}

#[derive(Serialize)]
struct ProfileUpdated<'a> {
    stream: &'a str,
    seq: u64,
    fields: ProfileFields<'a>,
    updated_by: String,
    reason: String,
    updated_at_ms: i64,
}

#[derive(Serialize)]
struct SensorReading<'a> {
    stream: &'a str,
    seq: u64,
    temp_dc: i32,
    humidity_dpct: u32,
    pressure_dhpa: u32,
    battery_pct: u32,
    rssi: i32,
    status: &'a str,
    site: String,
    window_s: u32,
    tags: Vec<String>,
    recorded_at_ms: i64,
}

/// Returns (msgpack-named bytes, JSON-equivalent length in bytes).
/// The JSON-equivalent is serde_json of the SAME struct — what the payload
/// would have cost under the JSON codec — used to relate ratios to the
/// seal_pipeline figures, which were measured on a JSON corpus.
pub fn payload(rng: &mut StdRng, stream: u32, seq: u64, ts_ms: i64) -> (Vec<u8>, u32) {
    match category_of(stream) {
        0 => {
            let sid = format!("account-{stream:05}");
            let note_len = rng.random_range(10..80);
            let v = AccountCredited {
                stream: &sid,
                seq,
                amount_cents: rng.random_range(100..10_000_000u64),
                currency: "USD",
                actor: format!("user-{:04}", rng.random_range(0..10_000u32)),
                source: "seal-pipeline-bench",
                note: word(rng, note_len),
                occurred_at_ms: ts_ms,
                schema_v: 1,
            };
            (rmp_serde::to_vec_named(&v).unwrap(), serde_json::to_vec(&v).unwrap().len() as u32)
        }
        1 => {
            let sid = format!("order-{stream:05}");
            let n_items: usize = rng.random_range(1..=2);
            let items: Vec<LineItem> = (0..n_items)
                .map(|_| LineItem {
                    sku: format!("SKU-{:05}", rng.random_range(0..100_000u32)),
                    qty: rng.random_range(1..5u32),
                    price_cents: rng.random_range(100..20_000u32),
                })
                .collect();
            let total: u64 = items.iter().map(|i| (i.qty * i.price_cents) as u64).sum();
            let v = OrderPlaced {
                stream: &sid,
                seq,
                order_id: format!("ord-{:08x}", rng.random_range(0..u32::MAX)),
                items,
                total_cents: total,
                currency: "USD",
                customer: format!("cust-{:05}", rng.random_range(0..50_000u32)),
                placed_at_ms: ts_ms,
                schema_v: 1,
            };
            (rmp_serde::to_vec_named(&v).unwrap(), serde_json::to_vec(&v).unwrap().len() as u32)
        }
        2 => {
            let sid = format!("user-{stream:05}");
            let name_len = rng.random_range(6..16);
            let reason_len = rng.random_range(8..40);
            let tz = ["America/New_York", "Europe/Berlin", "Asia/Tokyo", "UTC"]
                [rng.random_range(0..4usize)];
            let v = ProfileUpdated {
                stream: &sid,
                seq,
                fields: ProfileFields {
                    display_name: word(rng, name_len),
                    locale: "en-US",
                    tz,
                    marketing_opt_in: rng.random_range(0..2u8) == 1,
                },
                updated_by: format!("admin-{:02}", rng.random_range(0..100u32)),
                reason: word(rng, reason_len),
                updated_at_ms: ts_ms,
            };
            (rmp_serde::to_vec_named(&v).unwrap(), serde_json::to_vec(&v).unwrap().len() as u32)
        }
        _ => {
            let sid = format!("sensor-{stream:05}");
            let tag_len = rng.random_range(4..12);
            let v = SensorReading {
                stream: &sid,
                seq,
                temp_dc: rng.random_range(-200..450),
                humidity_dpct: rng.random_range(200..900u32),
                pressure_dhpa: rng.random_range(9800..10400u32),
                battery_pct: rng.random_range(5..100u32),
                rssi: -(rng.random_range(40..95i32)),
                status: "ok",
                site: format!("site-{:02}", rng.random_range(0..40u32)),
                window_s: 60,
                tags: vec![word(rng, tag_len)],
                recorded_at_ms: ts_ms,
            };
            (rmp_serde::to_vec_named(&v).unwrap(), serde_json::to_vec(&v).unwrap().len() as u32)
        }
    }
}

// ---------------------------------------------------------------------------
// Corpus in sealed cluster order
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
#[allow(dead_code)]
pub struct Ev {
    pub stream: u32,
    pub ver: u32,
    pub off: u32,
    pub len: u32,
}

pub struct Corpus {
    /// Payload bytes, concatenated in (category, stream, version) order.
    pub arena: Vec<u8>,
    /// Per-event refs, same order.
    pub evs: Vec<Ev>,
    /// Clustered event-index boundaries per category (len CATEGORIES+1).
    pub cat_start: [usize; CATEGORIES + 1],
    /// stream -> (first clustered event index, event count)
    pub stream_range: HashMap<u32, (u32, u32)>,
    pub present: Vec<u32>,
    pub hottest: u32,
    /// Total JSON-equivalent payload bytes (same structs via serde_json).
    pub json_bytes: u64,
}

impl Corpus {
    #[inline]
    pub fn bytes_of(&self, i: usize) -> &[u8] {
        let e = self.evs[i];
        &self.arena[e.off as usize..(e.off + e.len) as usize]
    }
}

pub fn generate(n_events: usize, seed: u64) -> Corpus {
    // 1. Log order (Zipf batches of 10), as the active segment would receive it.
    let mut rng = StdRng::seed_from_u64(seed);
    let zipf = Zipf::new(STREAMS as f64, ZIPF_S).unwrap();
    let mut counts = vec![0u32; STREAMS as usize];
    let mut ts = EPOCH_2026_MS;
    let mut json_bytes = 0u64;
    let mut recs: Vec<(u32, u32, Vec<u8>)> = Vec::with_capacity(n_events);
    for _ in 0..n_events / BATCH {
        let s = (zipf.sample(&mut rng) as u64).clamp(1, STREAMS) as u32 - 1;
        let first = counts[s as usize];
        for i in 0..BATCH as u32 {
            ts += rng.random_range(0..80i64);
            let (mp, jlen) = payload(&mut rng, s, (first + i) as u64, ts);
            json_bytes += jlen as u64;
            recs.push((s, first + i, mp));
        }
        counts[s as usize] += BATCH as u32;
    }

    // 2. Cluster by (category, stream, version) — the seal layout.
    let mut idx: Vec<u32> = (0..recs.len() as u32).collect();
    idx.sort_by_key(|&i| {
        let r = &recs[i as usize];
        (category_of(r.0), r.0, r.1)
    });

    let total: usize = recs.iter().map(|r| r.2.len()).sum();
    let mut arena = Vec::with_capacity(total);
    let mut evs = Vec::with_capacity(recs.len());
    let mut cat_counts = [0usize; CATEGORIES];
    let mut stream_range: HashMap<u32, (u32, u32)> = HashMap::new();
    for (k, &i) in idx.iter().enumerate() {
        let (s, v, ref p) = recs[i as usize];
        let off = arena.len() as u32;
        arena.extend_from_slice(p);
        evs.push(Ev { stream: s, ver: v, off, len: p.len() as u32 });
        cat_counts[category_of(s)] += 1;
        let e = stream_range.entry(s).or_insert((k as u32, 0));
        e.1 += 1;
    }
    let mut cat_start = [0usize; CATEGORIES + 1];
    for c in 0..CATEGORIES {
        cat_start[c + 1] = cat_start[c] + cat_counts[c];
    }
    let present: Vec<u32> =
        (0..STREAMS as u32).filter(|&s| counts[s as usize] > 0).collect();
    let hottest = (0..STREAMS as u32).max_by_key(|&s| counts[s as usize]).unwrap();
    let _ = n_events;
    Corpus { arena, evs, cat_start, stream_range, present, hottest, json_bytes }
}
