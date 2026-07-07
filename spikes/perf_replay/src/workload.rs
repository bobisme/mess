//! Seeded workload, adapted from spikes/vertical_slice/src/workload.rs:
//! ~250-byte JSON-ish payloads with realistic variance, 10,000 streams
//! Zipf(s=1.1), batches of 10. New here: 4 categories derived from stream id
//! (stream % 4), each with a distinct event shape, so per-category zstd
//! dictionaries have something real to learn.

use rand::rngs::StdRng;
use rand::RngExt;

pub const STREAMS: u64 = 10_000;
pub const BATCH: usize = 10;
pub const ZIPF_S: f64 = 1.1;
pub const CATEGORIES: usize = 4;

pub fn category_of(stream: u64) -> usize {
    (stream % CATEGORIES as u64) as usize
}

pub const CATEGORY_NAMES: [&str; CATEGORIES] = ["account", "order", "user", "sensor"];

fn word(rng: &mut StdRng, len: usize) -> String {
    (0..len)
        .map(|_| (b'a' + rng.random_range(0..26u8)) as char)
        .collect()
}

pub fn payload(rng: &mut StdRng, stream: u64, seq: u64) -> Vec<u8> {
    match category_of(stream) {
        0 => {
            // AccountCredited — same shape as the vertical_slice workload.
            let amount: u32 = rng.random_range(1..100_000);
            let cents: u32 = rng.random_range(0..100);
            let user: u32 = rng.random_range(0..10_000);
            let note_len = rng.random_range(10..80);
            let note = word(rng, note_len);
            format!(
                r#"{{"type":"AccountCredited","stream":"account-{stream:05}","seq":{seq},"amount":{amount}.{cents:02},"currency":"USD","actor":"user-{user:04}","source":"seal-pipeline-bench","note":"{note}","occurred_at":"2026-07-07T12:34:56.789Z","schema":"v1"}}"#
            )
            .into_bytes()
        }
        1 => {
            // OrderPlaced with a 1-2 item line array.
            let n_items: usize = rng.random_range(1..=2);
            let mut items = String::new();
            let mut total: u64 = 0;
            for i in 0..n_items {
                if i > 0 {
                    items.push(',');
                }
                let sku: u32 = rng.random_range(0..100_000);
                let qty: u32 = rng.random_range(1..5);
                let price: u32 = rng.random_range(100..20_000);
                total += (qty * price) as u64;
                items.push_str(&format!(
                    r#"{{"sku":"SKU-{sku:05}","qty":{qty},"price":{}.{:02}}}"#,
                    price / 100,
                    price % 100
                ));
            }
            let cust: u32 = rng.random_range(0..50_000);
            let oid: u32 = rng.random_range(0..u32::MAX);
            format!(
                r#"{{"type":"OrderPlaced","stream":"order-{stream:05}","seq":{seq},"order_id":"ord-{oid:08x}","items":[{items}],"total":{}.{:02},"currency":"USD","customer":"cust-{cust:05}","placed_at":"2026-07-07T12:34:56.789Z","schema":"v1"}}"#,
                total / 100,
                total % 100
            )
            .into_bytes()
        }
        2 => {
            // ProfileUpdated with a small nested fields object.
            let name_len = rng.random_range(6..16);
            let name = word(rng, name_len);
            let reason_len = rng.random_range(8..40);
            let reason = word(rng, reason_len);
            let tz = ["America/New_York", "Europe/Berlin", "Asia/Tokyo", "UTC"]
                [rng.random_range(0..4usize)];
            let admin: u32 = rng.random_range(0..100);
            let opt = rng.random_range(0..2u8) == 1;
            format!(
                r#"{{"type":"ProfileUpdated","stream":"user-{stream:05}","seq":{seq},"fields":{{"display_name":"{name}","locale":"en-US","tz":"{tz}","marketing_opt_in":{opt}}},"updated_by":"admin-{admin:02}","reason":"{reason}","updated_at":"2026-07-07T12:34:56.789Z"}}"#
            )
            .into_bytes()
        }
        _ => {
            // SensorReading — numeric-heavy.
            let temp: i32 = rng.random_range(-200..450);
            let hum: u32 = rng.random_range(200..900);
            let hpa: u32 = rng.random_range(9800..10400);
            let batt: u32 = rng.random_range(5..100);
            let rssi: i32 = -(rng.random_range(40..95i32));
            let site: u32 = rng.random_range(0..40);
            let tag_len = rng.random_range(4..12);
            let tag = word(rng, tag_len);
            format!(
                r#"{{"type":"SensorReading","stream":"sensor-{stream:05}","seq":{seq},"temp_c":{}.{},"humidity":{}.{},"pressure_hpa":{}.{},"battery_pct":{batt},"rssi":{rssi},"status":"ok","site":"site-{site:02}","window_s":60,"tags":["{tag}"],"recorded_at":"2026-07-07T12:34:56.789Z"}}"#,
                temp / 10,
                (temp % 10).abs(),
                hum / 10,
                hum % 10,
                hpa / 10,
                hpa % 10
            )
            .into_bytes()
        }
    }
}
