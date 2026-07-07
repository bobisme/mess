//! Test corpus: three realistic event shapes with seeded, realistic variance.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------- OrderPlaced

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum SalesChannel {
    Web,
    Mobile,
    Api,
    PointOfSale,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct LineItem {
    pub sku: String,
    pub name: String,
    pub quantity: u16,
    pub unit_price_cents: u32,
    pub discount_cents: u32,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Address {
    pub line1: String,
    pub city: String,
    pub region: String,
    pub postal_code: String,
    pub country: String,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct OrderPlaced {
    pub order_id: u64,
    pub customer_id: u64,
    pub placed_at_ms: i64,
    pub currency: String,
    pub total_cents: u64,
    pub coupon_code: Option<String>,
    pub sales_channel: SalesChannel,
    pub items: Vec<LineItem>,
    pub shipping: Address,
    pub gift_message: Option<String>,
    pub loyalty_points_earned: u32,
}

// -------------------------------------------------------------- UserRegistered

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum SignupSource {
    Organic,
    Invite { inviter_id: u64 },
    Campaign { name: String },
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum PlanTier {
    Free,
    Pro,
    Team,
    Enterprise,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Plan {
    pub tier: PlanTier,
    pub seats: u16,
    pub trial_days: u8,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct UserRegistered {
    pub user_id: u64,
    pub email: String,
    pub display_name: String,
    pub locale: String,
    pub referrer: Option<String>,
    pub marketing_opt_in: bool,
    pub signup_source: SignupSource,
    pub created_at_ms: i64,
    pub initial_plan: Plan,
}

// -------------------------------------------------------------- ShipmentEvent

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum ShipmentEvent {
    Dispatched {
        shipment_id: u64,
        order_id: u64,
        carrier: String,
        tracking: String,
        at_ms: i64,
        estimated_days: u8,
    },
    LocationScanned {
        shipment_id: u64,
        facility: String,
        city: String,
        country: String,
        at_ms: i64,
        lat_e6: i32,
        lon_e6: i32,
    },
    Delivered {
        shipment_id: u64,
        at_ms: i64,
        signed_by: Option<String>,
        photo_ref: Option<String>,
    },
    Exception {
        shipment_id: u64,
        at_ms: i64,
        code: u16,
        description: String,
    },
}

// ---------------------------------------------------------------- generators

const FIRST_NAMES: &[&str] = &[
    "ava", "liam", "noah", "mia", "kai", "zoe", "eli", "ida", "otto", "nina", "ravi", "sana",
    "theo", "vera", "yuki", "omar",
];
const LAST_NAMES: &[&str] = &[
    "smith", "garcia", "chen", "kumar", "novak", "haas", "silva", "berg", "tanaka", "okafor",
    "meyer", "russo",
];
const CITIES: &[&str] = &[
    "Portland", "Leipzig", "Osaka", "Austin", "Tallinn", "Porto", "Lyon", "Bergen", "Denver",
    "Kyoto", "Ghent", "Turin",
];
const REGIONS: &[&str] = &["OR", "SN", "27", "TX", "37", "13", "ARA", "46", "CO", "26", "VLG", "TO"];
const COUNTRIES: &[&str] = &["US", "DE", "JP", "US", "EE", "PT", "FR", "NO", "US", "JP", "BE", "IT"];
const STREETS: &[&str] = &[
    "Oak Ave", "Hauptstrasse", "Sakura-dori", "Cedar Ln", "Pikk", "Rua Nova", "Rue Centrale",
    "Storgata", "Elm St", "Kawaramachi", "Veldstraat", "Via Roma",
];
const PRODUCTS: &[&str] = &[
    "walnut desk organizer", "ceramic pour-over set", "merino beanie", "usb-c dock 8-in-1",
    "cast iron skillet 26cm", "linen tea towel", "trail running socks", "field notebook a6",
    "brass desk lamp", "espresso tamper 58mm", "canvas tote bag", "mechanical pencil 0.5",
];
const CARRIERS: &[&str] = &["UPS", "DHL", "FedEx", "YamatoTA", "PostNord", "GLS"];
const FACILITIES: &[&str] = &[
    "SORT-PDX-04", "HUB-LEJ-01", "GATEWAY-KIX", "SORT-AUS-11", "HUB-TLL-02", "DEPOT-OPO-3",
];
const LOCALES: &[&str] = &["en-US", "de-DE", "ja-JP", "en-GB", "et-EE", "pt-PT", "fr-FR", "nb-NO"];
const CAMPAIGNS: &[&str] = &["spring-launch", "podcast-q2", "newsletter-may", "conf-berlin"];
const EXCEPTION_DESCS: &[&str] = &[
    "address incomplete, contacting recipient",
    "customs clearance delay",
    "weather hold at regional hub",
    "recipient not available, second attempt scheduled",
];

fn pick<'a>(rng: &mut StdRng, list: &'a [&'a str]) -> &'a str {
    list[rng.gen_range(0..list.len())]
}

const EPOCH_2026: i64 = 1_767_225_600_000; // 2026-01-01T00:00:00Z in ms

pub fn gen_order(rng: &mut StdRng) -> OrderPlaced {
    let n_items = rng.gen_range(1..=5usize);
    let items: Vec<LineItem> = (0..n_items)
        .map(|_| {
            let unit = rng.gen_range(450..25_000u32);
            LineItem {
                sku: format!("SKU-{:06}", rng.gen_range(0..500_000u32)),
                name: pick(rng, PRODUCTS).to_string(),
                quantity: rng.gen_range(1..=4u16),
                unit_price_cents: unit,
                discount_cents: if rng.gen_bool(0.25) { unit / 10 } else { 0 },
            }
        })
        .collect();
    let total: u64 = items
        .iter()
        .map(|i| (i.unit_price_cents - i.discount_cents) as u64 * i.quantity as u64)
        .sum();
    OrderPlaced {
        order_id: rng.gen_range(1_000_000..900_000_000u64),
        customer_id: rng.gen_range(1_000..5_000_000u64),
        placed_at_ms: EPOCH_2026 + rng.gen_range(0..15_552_000_000i64),
        currency: ["USD", "EUR", "JPY", "GBP"][rng.gen_range(0..4)].to_string(),
        total_cents: total,
        coupon_code: if rng.gen_bool(0.2) {
            Some(format!("SAVE{}", rng.gen_range(5..30) * 5))
        } else {
            None
        },
        sales_channel: match rng.gen_range(0..4) {
            0 => SalesChannel::Web,
            1 => SalesChannel::Mobile,
            2 => SalesChannel::Api,
            _ => SalesChannel::PointOfSale,
        },
        items,
        shipping: Address {
            line1: format!("{} {}", rng.gen_range(1..2000), pick(rng, STREETS)),
            city: pick(rng, CITIES).to_string(),
            region: pick(rng, REGIONS).to_string(),
            postal_code: format!("{:05}", rng.gen_range(1000..99999u32)),
            country: pick(rng, COUNTRIES).to_string(),
        },
        gift_message: if rng.gen_bool(0.07) {
            Some("Happy birthday! Hope you enjoy this one.".to_string())
        } else {
            None
        },
        loyalty_points_earned: (total / 100) as u32,
    }
}

pub fn gen_user(rng: &mut StdRng) -> UserRegistered {
    let first = pick(rng, FIRST_NAMES);
    let last = pick(rng, LAST_NAMES);
    UserRegistered {
        user_id: rng.gen_range(1_000..50_000_000u64),
        email: format!("{}.{}{}@example.com", first, last, rng.gen_range(1..999u32)),
        display_name: format!("{} {}", capitalize(first), capitalize(last)),
        locale: pick(rng, LOCALES).to_string(),
        referrer: if rng.gen_bool(0.3) {
            Some(format!("https://ref.example.net/u/{:x}", rng.gen::<u32>()))
        } else {
            None
        },
        marketing_opt_in: rng.gen_bool(0.4),
        signup_source: match rng.gen_range(0..3) {
            0 => SignupSource::Organic,
            1 => SignupSource::Invite {
                inviter_id: rng.gen_range(1_000..50_000_000u64),
            },
            _ => SignupSource::Campaign {
                name: pick(rng, CAMPAIGNS).to_string(),
            },
        },
        created_at_ms: EPOCH_2026 + rng.gen_range(0..15_552_000_000i64),
        initial_plan: Plan {
            tier: match rng.gen_range(0..4) {
                0 => PlanTier::Free,
                1 => PlanTier::Pro,
                2 => PlanTier::Team,
                _ => PlanTier::Enterprise,
            },
            seats: rng.gen_range(1..50u16),
            trial_days: [0, 7, 14, 30][rng.gen_range(0..4)],
        },
    }
}

pub fn gen_shipment(rng: &mut StdRng) -> ShipmentEvent {
    let shipment_id = rng.gen_range(1_000_000..900_000_000u64);
    let at_ms = EPOCH_2026 + rng.gen_range(0..15_552_000_000i64);
    match rng.gen_range(0..10) {
        0..=2 => ShipmentEvent::Dispatched {
            shipment_id,
            order_id: rng.gen_range(1_000_000..900_000_000u64),
            carrier: pick(rng, CARRIERS).to_string(),
            tracking: format!("1Z{:012X}", rng.gen::<u64>() & 0xFFFF_FFFF_FFFF),
            at_ms,
            estimated_days: rng.gen_range(1..12u8),
        },
        3..=7 => ShipmentEvent::LocationScanned {
            shipment_id,
            facility: pick(rng, FACILITIES).to_string(),
            city: pick(rng, CITIES).to_string(),
            country: pick(rng, COUNTRIES).to_string(),
            at_ms,
            lat_e6: rng.gen_range(-60_000_000..70_000_000i32),
            lon_e6: rng.gen_range(-180_000_000..180_000_000i32),
        },
        8 => ShipmentEvent::Delivered {
            shipment_id,
            at_ms,
            signed_by: if rng.gen_bool(0.6) {
                Some(capitalize(pick(rng, LAST_NAMES)))
            } else {
                None
            },
            photo_ref: if rng.gen_bool(0.5) {
                Some(format!("pod/{:016x}.jpg", rng.gen::<u64>()))
            } else {
                None
            },
        },
        _ => ShipmentEvent::Exception {
            shipment_id,
            at_ms,
            code: rng.gen_range(100..600u16),
            description: pick(rng, EXCEPTION_DESCS).to_string(),
        },
    }
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

/// Deterministic corpus: (train, test) split for one generator.
pub fn corpus<T>(seed: u64, n_train: usize, n_test: usize, gen: fn(&mut StdRng) -> T) -> (Vec<T>, Vec<T>) {
    let mut rng = StdRng::seed_from_u64(seed);
    let train = (0..n_train).map(|_| gen(&mut rng)).collect();
    let test = (0..n_test).map(|_| gen(&mut rng)).collect();
    (train, test)
}
