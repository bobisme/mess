//! `load_verified` throughput: snapshot at v=0 (so the tail is the whole
//! stream) forces the full prefix cert + tail replay + head anchor path.
//! Ported from `mess-log/tests/crash_verify.rs`'s `verify_throughput_bench`.
//! Full-size N=1,000,000, batch 100, best-of-3 matches
//! `phase5.verify.load_verified.ev_per_s` in `docs/perf/envelope.md`.

use std::time::Instant;

use mess_log::certificates::{
    Aggregate, build_cert, load_verified, take_snapshot,
};

use crate::{Metric, RunSize};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Account {
    balance: i64,
    tx_count: u64,
}
impl Aggregate for Account {
    const FOLD_VERSION: u32 = 1;
    fn init() -> Self {
        Account { balance: 0, tx_count: 0 }
    }
    fn apply(&mut self, payload: &[u8]) {
        let tag = payload[0];
        let amount = u64::from_le_bytes(payload[1..9].try_into().unwrap());
        match tag {
            0 => self.balance += amount as i64,
            1 => self.balance -= amount as i64,
            _ => {}
        }
        self.tx_count += 1;
    }
    fn to_bytes(&self) -> Vec<u8> {
        let mut o = Vec::with_capacity(16);
        o.extend_from_slice(&self.balance.to_le_bytes());
        o.extend_from_slice(&self.tx_count.to_le_bytes());
        o
    }
    fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.len() != 16 {
            return None;
        }
        Some(Account {
            balance: i64::from_le_bytes(b[0..8].try_into().ok()?),
            tx_count: u64::from_le_bytes(b[8..16].try_into().ok()?),
        })
    }
}

fn ev(tag: u8, amount: u64) -> Vec<u8> {
    let mut p = vec![0u8; 32];
    p[0] = tag;
    p[1..9].copy_from_slice(&amount.to_le_bytes());
    p
}

fn workload(n: u64) -> Vec<Vec<u8>> {
    (0..n).map(|i| ev((i % 3 == 2) as u8, 10 + i)).collect()
}

const STREAM_ID: u64 = 42;

pub fn run(size: RunSize) -> Vec<Metric> {
    let (n, batch, reps): (u64, usize, usize) = match size {
        RunSize::Full => (1_000_000, 100, 3),
        RunSize::Smoke => (2_000, 50, 1),
    };
    let payloads = workload(n);
    let cert = build_cert(STREAM_ID, &payloads, batch, None);
    let (r, blob) = take_snapshot::<Account>(&cert, 0);

    // Warmup.
    let _ = load_verified::<Account>(&cert, Some((&r, &blob))).unwrap();

    let mut best = f64::INFINITY;
    for _ in 0..reps {
        let t = Instant::now();
        let out = load_verified::<Account>(&cert, Some((&r, &blob))).unwrap();
        let dt = t.elapsed().as_secs_f64();
        assert_eq!(out.tail_len, n - 1);
        best = best.min(dt);
    }

    vec![Metric::new(
        "phase5.verify.load_verified.ev_per_s",
        n as f64 / best,
        "ev/s",
        format!(
            "load_verified over a {n}-event tail (snapshot at v=0 => full prefix cert + tail replay \
             + head anchor); batch {batch}; best-of-{reps}; single core, in-memory"
        ),
    )]
}
