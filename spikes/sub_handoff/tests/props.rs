//! Property tests for the catch-up -> live handoff protocol.
//!
//! Every scenario is derived deterministically from a seed (scenario shape,
//! rates, delays, cursors are all seeded; task interleavings come from the
//! real tokio multi-thread scheduler, which is the point — we want real
//! races). THE assertion, every scenario: each subscriber's delivered
//! sequence is EXACTLY `cursor+1 ..= final_watermark`. No gap, no
//! duplicate, in order, across every handoff and every regression.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::time::Duration;
use sub_handoff::{naive, run_subscriber, Log, SubStats};
use tokio::sync::{mpsc, watch};

// ---------------------------------------------------------------- plumbing

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .unwrap()
}

#[derive(Debug, Clone, Copy)]
enum Delay {
    None,
    Yields(u32),
    SleepUs(u64), // tokio timer granularity is ~1ms; use sparingly
}

async fn pause(d: Delay) {
    match d {
        Delay::None => {}
        Delay::Yields(k) => {
            for _ in 0..k {
                tokio::task::yield_now().await;
            }
        }
        Delay::SleepUs(u) => tokio::time::sleep(Duration::from_micros(u)).await,
    }
}

fn rand_delay(rng: &mut StdRng, sleep_prob: f64) -> Delay {
    if rng.gen_bool(sleep_prob) {
        Delay::SleepUs(rng.gen_range(1..=1500))
    } else if rng.gen_bool(0.5) {
        Delay::Yields(rng.gen_range(1..=12))
    } else {
        Delay::None
    }
}

/// Writer plan: bursts of appends separated by delays (including long
/// pauses), fully derived from the seed.
#[derive(Debug, Clone)]
struct WriterPlan {
    ops: Vec<(u32, Delay)>, // (burst_size, delay_after)
}

fn writer_plan(rng: &mut StdRng, total: u64) -> WriterPlan {
    let mut ops = Vec::new();
    let mut left = total;
    while left > 0 {
        let burst = rng.gen_range(1..=20).min(left) as u32;
        left -= burst as u64;
        ops.push((burst, rand_delay(rng, 0.03)));
    }
    WriterPlan { ops }
}

#[derive(Debug, Clone)]
struct SubPlan {
    start_cursor: u64,
    page_limit: usize,
    sink_cap: usize,
    consumer_sleep_prob: f64,
    consumer_yield_max: u32,
}

#[derive(Debug, Clone)]
struct Scenario {
    live_capacity: usize,
    pre_events: u64,
    extra_events: u64,
    subs: Vec<SubPlan>,
}

fn scenario(rng: &mut StdRng, force_tiny_buffer: bool) -> Scenario {
    let live_capacity = if force_tiny_buffer {
        rng.gen_range(2..=4)
    } else {
        *[2usize, 3, 4, 8, 8, 16, 64]
            .get(rng.gen_range(0..7))
            .unwrap()
    };
    let pre_events = if rng.gen_bool(0.2) {
        0
    } else {
        rng.gen_range(0..=60)
    };
    let extra_events = rng.gen_range(20..=250);
    let n_subs = rng.gen_range(1..=3);
    let subs = (0..n_subs)
        .map(|_| {
            let start_cursor = match rng.gen_range(0..3) {
                0 => 0,                              // full replay
                1 => rng.gen_range(0..=pre_events),  // mid-history
                _ => pre_events,                     // exactly at the watermark
            };
            SubPlan {
                start_cursor,
                page_limit: rng.gen_range(1..=32),
                sink_cap: rng.gen_range(1..=16),
                consumer_sleep_prob: if force_tiny_buffer { 0.02 } else { 0.005 },
                consumer_yield_max: if force_tiny_buffer {
                    rng.gen_range(4..=24)
                } else {
                    rng.gen_range(0..=8)
                },
            }
        })
        .collect();
    Scenario {
        live_capacity,
        pre_events,
        extra_events,
        subs,
    }
}

/// Check the one property that matters.
fn check_exact(
    label: &str,
    start_cursor: u64,
    final_wm: u64,
    got: &[u64],
) -> Result<(), String> {
    let mut expect = start_cursor + 1;
    for (i, &p) in got.iter().enumerate() {
        if p != expect {
            return Err(format!(
                "{label}: at index {i} expected {expect}, got {p} \
                 ({} = gap, dup, or reorder)",
                if p > expect { "gap" } else { "dup/reorder" }
            ));
        }
        expect += 1;
    }
    if expect != final_wm + 1 {
        return Err(format!(
            "{label}: delivered up to {} but final watermark is {final_wm} (missing tail)",
            expect - 1
        ));
    }
    Ok(())
}

/// Run one full scenario; returns per-subscriber stats for aggregation.
async fn run_scenario(seed: u64, sc: Scenario) -> Vec<SubStats> {
    let log = Log::new(sc.live_capacity);
    for _ in 0..sc.pre_events {
        log.append();
    }
    let (fin_tx, fin_rx) = watch::channel(None::<u64>);

    // Subscribers + their consumers (a slow consumer backpressures the
    // subscriber through the bounded sink, which is what overflows the
    // bounded live buffer).
    let mut sub_handles = Vec::new();
    let mut consumer_handles = Vec::new();
    for (i, plan) in sc.subs.iter().cloned().enumerate() {
        let (tx, mut rx) = mpsc::channel::<u64>(plan.sink_cap);
        let mut crng = StdRng::seed_from_u64(seed ^ (0xC0FFEE + i as u64));
        consumer_handles.push(tokio::spawn(async move {
            let mut got = Vec::new();
            while let Some(p) = rx.recv().await {
                got.push(p);
                let d = if crng.gen_bool(plan.consumer_sleep_prob) {
                    Delay::SleepUs(crng.gen_range(100..=2000))
                } else if plan.consumer_yield_max > 0 {
                    Delay::Yields(crng.gen_range(0..=plan.consumer_yield_max))
                } else {
                    Delay::None
                };
                pause(d).await;
            }
            got
        }));
        sub_handles.push(tokio::spawn(run_subscriber(
            log.clone(),
            plan.start_cursor,
            plan.page_limit,
            tx,
            fin_rx.clone(),
        )));
    }

    // Writer races the subscribers from the start (appends race the switch
    // because subscribers with start_cursor == pre_events switch almost
    // immediately).
    let wplan = {
        let mut wrng = StdRng::seed_from_u64(seed ^ 0xB0B);
        writer_plan(&mut wrng, sc.extra_events)
    };
    let wlog = log.clone();
    let writer = tokio::spawn(async move {
        for (burst, d) in wplan.ops {
            for _ in 0..burst {
                wlog.append();
            }
            pause(d).await;
        }
    });
    writer.await.unwrap();
    let final_wm = log.watermark();
    assert_eq!(final_wm, sc.pre_events + sc.extra_events);
    fin_tx.send(Some(final_wm)).unwrap();

    let mut all_stats = Vec::new();
    for (i, (sh, ch)) in sub_handles
        .into_iter()
        .zip(consumer_handles.into_iter())
        .enumerate()
    {
        let stats = tokio::time::timeout(Duration::from_secs(20), sh)
            .await
            .unwrap_or_else(|_| panic!("seed {seed} sub {i}: subscriber hung (livelock?)"))
            .unwrap();
        let got = ch.await.unwrap();
        if let Err(e) = check_exact(
            &format!("seed {seed} sub {i} (cursor {})", sc.subs[i].start_cursor),
            sc.subs[i].start_cursor,
            final_wm,
            &got,
        ) {
            panic!("PROPERTY VIOLATION: {e}\nstats: {stats:?}");
        }
        assert_eq!(
            stats.anomaly_regressions, 0,
            "seed {seed} sub {i}: in-order gap without Lagged — invariant W1/S1 broken"
        );
        all_stats.push(stats);
    }
    all_stats
}

#[derive(Default)]
struct Agg {
    scenarios: u64,
    subscribers: u64,
    history: u64,
    live: u64,
    dedupe: u64,
    lags: u64,
    switches: u64,
    pages: u64,
}

impl Agg {
    fn add(&mut self, stats: &[SubStats]) {
        self.scenarios += 1;
        for s in stats {
            self.subscribers += 1;
            self.history += s.history_delivered;
            self.live += s.live_delivered;
            self.dedupe += s.dedupe_skips;
            self.lags += s.lag_regressions;
            self.switches += s.switches;
            self.pages += s.catchup_pages;
        }
    }
    fn print(&self, name: &str) {
        println!(
            "[{name}] scenarios={} subscribers={} delivered(history={}, live={}) \
             dedupe_skips={} lag_regressions={} switches={} catchup_pages={}",
            self.scenarios,
            self.subscribers,
            self.history,
            self.live,
            self.dedupe,
            self.lags,
            self.switches,
            self.pages
        );
    }
}

// ------------------------------------------------------------- the big ones

/// 4000 fully randomized scenarios: random buffer sizes, append
/// bursts/pauses, cursors (0 / mid / at-head), consumer delays, 1-3
/// concurrent subscribers per scenario.
#[test]
fn randomized_scenarios_4000() {
    let rt = rt();
    let mut agg = Agg::default();
    for seed in 0..4000u64 {
        let sc = scenario(&mut StdRng::seed_from_u64(seed), false);
        let stats = rt.block_on(run_scenario(seed, sc));
        agg.add(&stats);
    }
    agg.print("randomized_4000");
    assert!(agg.switches > 0);
    assert!(agg.lags > 0, "randomized suite never overflowed the live buffer");
    assert!(agg.dedupe > 0, "randomized suite never exercised the overlap dedupe");
}

/// 1000 scenarios engineered for live-buffer overflow: tiny broadcast
/// buffers (2-4) and slow consumers. Regressions to CatchUp must be
/// frequent, and still: exact delivery.
#[test]
fn overflow_storm_1000() {
    let rt = rt();
    let mut agg = Agg::default();
    for seed in 0..1000u64 {
        let sc = scenario(&mut StdRng::seed_from_u64(0x0F1_0000 + seed), true);
        let stats = rt.block_on(run_scenario(0x0F1_0000 + seed, sc));
        agg.add(&stats);
    }
    agg.print("overflow_storm_1000");
    assert!(
        agg.lags >= 100,
        "expected frequent Lagged regressions, got {}",
        agg.lags
    );
}

/// 600 scenarios of appends racing the switch: subscriber starts exactly at
/// the watermark while a writer hammers with no delay, so the empty-page ->
/// Switching transition races fresh appends every time.
#[test]
fn racing_switch_600() {
    let rt = rt();
    let mut agg = Agg::default();
    for seed in 0..600u64 {
        let mut rng = StdRng::seed_from_u64(0xACE_0000 + seed);
        let pre = rng.gen_range(10..=100);
        let sc = Scenario {
            live_capacity: rng.gen_range(4..=16),
            pre_events: pre,
            extra_events: rng.gen_range(50..=200),
            subs: vec![SubPlan {
                start_cursor: pre, // at head: switches immediately
                page_limit: rng.gen_range(1..=8),
                sink_cap: rng.gen_range(1..=4),
                consumer_sleep_prob: 0.0,
                consumer_yield_max: rng.gen_range(0..=4),
            }],
        };
        let stats = rt.block_on(run_scenario(0xACE_0000 + seed, sc));
        agg.add(&stats);
    }
    agg.print("racing_switch_600");
    assert!(agg.switches >= 600);
}

// ------------------------------------------------------------ targeted edges

/// Subscriber permanently slower than the writer: must remain in stable
/// catch-up (lag -> CatchUp -> lag -> ...) with bounded memory (broadcast
/// buffer is bounded by construction; the subscriber holds at most one
/// page) and no livelock — it keeps delivering, and finishes once the
/// writer stops.
#[test]
fn forever_slower_consumer_stays_in_stable_catchup() {
    let rt = rt();
    rt.block_on(async {
        let log = Log::new(4);
        let (fin_tx, fin_rx) = watch::channel(None::<u64>);
        let (tx, mut rx) = mpsc::channel::<u64>(1);

        let consumer = tokio::spawn(async move {
            let mut got = Vec::new();
            while let Some(p) = rx.recv().await {
                got.push(p);
                // Much slower than the writer, forever: ~4 events/ms.
                if got.len() % 4 == 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            got
        });
        let sub = tokio::spawn(run_subscriber(log.clone(), 0, 16, tx, fin_rx));

        // Writer runs ~4x faster than the consumer for a fixed wall-clock
        // window (16 events/ms in bursts of 16 against a live buffer of 4,
        // so every burst overflows a live-mode subscriber).
        let wlog = log.clone();
        let writer = tokio::spawn(async move {
            let t0 = std::time::Instant::now();
            while t0.elapsed() < Duration::from_millis(300) {
                for _ in 0..16 {
                    wlog.append();
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
        writer.await.unwrap();
        let final_wm = log.watermark();
        fin_tx.send(Some(final_wm)).unwrap();

        let stats = tokio::time::timeout(Duration::from_secs(30), sub)
            .await
            .expect("subscriber livelocked while permanently behind")
            .unwrap();
        let got = consumer.await.unwrap();
        check_exact("forever_slower", 0, final_wm, &got).unwrap();
        println!(
            "[forever_slower] final_wm={final_wm} stats={stats:?} (history \
             should dominate live; repeated lag regressions expected)"
        );
        assert!(stats.lag_regressions > 0, "writer never outran the subscriber");
        assert!(
            stats.history_delivered > stats.live_delivered,
            "a permanently slow subscriber should be fed mostly from history"
        );
        assert_eq!(stats.anomaly_regressions, 0);
    });
}

/// Writer completely idle during the switch: subscriber catches up, sits in
/// Switching with an empty live buffer, then the writer resumes.
#[test]
fn writer_idle_during_switch() {
    let rt = rt();
    rt.block_on(async {
        let log = Log::new(8);
        for _ in 0..40 {
            log.append();
        }
        let (fin_tx, fin_rx) = watch::channel(None::<u64>);
        let (tx, mut rx) = mpsc::channel::<u64>(8);
        let consumer = tokio::spawn(async move {
            let mut got = Vec::new();
            while let Some(p) = rx.recv().await {
                got.push(p);
            }
            got
        });
        let sub = tokio::spawn(run_subscriber(log.clone(), 0, 7, tx, fin_rx));

        // Give the subscriber ample time to catch up and park in Switching.
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Paced writer (1ms/event): the subscriber keeps up, so the resumed
        // events are delivered live. (First version of this test appended in
        // a yield-paced burst and the subscriber delivered 100% from history
        // after one Lagged — see REPORT.md "burst outruns wakeup".)
        for _ in 0..25 {
            log.append();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let final_wm = log.watermark();
        fin_tx.send(Some(final_wm)).unwrap();

        let stats = sub.await.unwrap();
        let got = consumer.await.unwrap();
        check_exact("idle_during_switch", 0, final_wm, &got).unwrap();
        println!("[writer_idle_during_switch] stats={stats:?}");
        assert!(stats.live_delivered > 0, "resumed events should arrive live");
        assert_eq!(stats.anomaly_regressions, 0);
    });
}

/// Subscription starting exactly at the watermark: catch-up sees one empty
/// page and the very first delivery is live.
#[test]
fn start_exactly_at_watermark() {
    let rt = rt();
    rt.block_on(async {
        let log = Log::new(8);
        for _ in 0..30 {
            log.append();
        }
        let cursor = log.watermark();
        let (fin_tx, fin_rx) = watch::channel(None::<u64>);
        let (tx, mut rx) = mpsc::channel::<u64>(8);
        let consumer = tokio::spawn(async move {
            let mut got = Vec::new();
            while let Some(p) = rx.recv().await {
                got.push(p);
            }
            got
        });
        let sub = tokio::spawn(run_subscriber(log.clone(), cursor, 8, tx, fin_rx));
        tokio::time::sleep(Duration::from_millis(20)).await;
        for _ in 0..15 {
            log.append();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let final_wm = log.watermark();
        fin_tx.send(Some(final_wm)).unwrap();

        let stats = sub.await.unwrap();
        let got = consumer.await.unwrap();
        check_exact("start_at_watermark", cursor, final_wm, &got).unwrap();
        println!("[start_exactly_at_watermark] stats={stats:?}");
        assert!(
            stats.live_delivered > 0,
            "at-head subscriber with a paced writer should deliver live"
        );
        assert_eq!(stats.anomaly_regressions, 0);
    });
}

/// Start on a completely empty log with an initially idle writer.
#[test]
fn start_on_empty_log() {
    let rt = rt();
    rt.block_on(async {
        let log = Log::new(8);
        let (fin_tx, fin_rx) = watch::channel(None::<u64>);
        let (tx, mut rx) = mpsc::channel::<u64>(8);
        let consumer = tokio::spawn(async move {
            let mut got = Vec::new();
            while let Some(p) = rx.recv().await {
                got.push(p);
            }
            got
        });
        let sub = tokio::spawn(run_subscriber(log.clone(), 0, 4, tx, fin_rx));
        tokio::time::sleep(Duration::from_millis(20)).await;
        for _ in 0..10 {
            log.append();
        }
        let final_wm = log.watermark();
        fin_tx.send(Some(final_wm)).unwrap();
        let stats = sub.await.unwrap();
        let got = consumer.await.unwrap();
        check_exact("empty_log", 0, final_wm, &got).unwrap();
        println!("[start_on_empty_log] stats={stats:?}");
        assert_eq!(stats.anomaly_regressions, 0);
    });
}

// -------------------------------------------- the rejected alternative dies

/// Executable proof of why "catch up until empty, THEN subscribe" is
/// rejected: under a racing writer it silently loses the events appended in
/// the window between the last empty history read and the subscribe call.
/// We run 300 seeded races and count how many lose events.
#[test]
fn naive_protocol_drops_events() {
    let rt = rt();
    let mut broken = 0u32;
    let total = 300u64;
    for seed in 0..total {
        let lost = rt.block_on(async {
            let mut rng = StdRng::seed_from_u64(0xBAD_0000 + seed);
            let log = Log::new(64); // generous buffer: the bug is not overflow
            let pre = rng.gen_range(5..=40);
            for _ in 0..pre {
                log.append();
            }
            let (fin_tx, fin_rx) = watch::channel(None::<u64>);
            let (tx, mut rx) = mpsc::channel::<u64>(64);
            let consumer = tokio::spawn(async move {
                let mut got = Vec::new();
                while let Some(p) = rx.recv().await {
                    got.push(p);
                }
                got
            });
            let sub = tokio::spawn(naive::run_subscriber_gapped(
                log.clone(),
                0,
                8,
                tx,
                fin_rx,
                Duration::from_millis(30),
            ));
            // Writer hammers while the subscriber is switching.
            let wlog = log.clone();
            let extra = rng.gen_range(20..=120);
            let writer = tokio::spawn(async move {
                for _ in 0..extra {
                    wlog.append();
                    tokio::task::yield_now().await;
                }
            });
            writer.await.unwrap();
            let final_wm = log.watermark();
            fin_tx.send(Some(final_wm)).unwrap();
            let _ = tokio::time::timeout(Duration::from_secs(5), sub).await;
            let got = consumer.await.unwrap();
            check_exact("naive", 0, final_wm, &got).is_err()
        });
        if lost {
            broken += 1;
        }
    }
    println!(
        "[naive_protocol_drops_events] {broken}/{total} seeded races lost events \
         (any nonzero count kills the design)"
    );
    assert!(
        broken > 0,
        "expected the naive protocol to lose events under racing appends; \
         widen the switch window if this ever fails"
    );
}
