//! Common phase gate and exact latency statistics.

use std::future::poll_fn;
use std::sync::{Condvar, Mutex};
use std::task::{Poll, Waker};

#[derive(Default)]
struct GateState {
    ready:   usize,
    started: bool,
    waiters: Vec<Waker>,
}

#[derive(Default)]
pub struct StartGate {
    state:         Mutex<GateState>,
    ready_changed: Condvar,
}

impl StartGate {
    pub async fn arrive_and_wait(&self) {
        let mut arrived = false;
        poll_fn(|context| {
            let mut state = self.state.lock().expect("start gate poisoned");
            if !arrived {
                state.ready += 1;
                arrived = true;
                self.ready_changed.notify_all();
            }
            if state.started {
                Poll::Ready(())
            } else {
                if !state
                    .waiters
                    .iter()
                    .any(|waker| waker.will_wake(context.waker()))
                {
                    state.waiters.push(context.waker().clone());
                }
                Poll::Pending
            }
        })
        .await;
    }

    pub fn wait_until_ready(&self, expected: usize) {
        let mut state = self.state.lock().expect("start gate poisoned");
        while state.ready != expected {
            assert!(
                state.ready < expected,
                "too many writers reached start gate"
            );
            state =
                self.ready_changed.wait(state).expect("start gate poisoned");
        }
    }

    pub fn start(&self) {
        let waiters = {
            let mut state = self.state.lock().expect("start gate poisoned");
            assert!(!state.started, "start gate released twice");
            state.started = true;
            std::mem::take(&mut state.waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }
}

pub fn nearest_rank(
    sorted: &[u64],
    numerator: usize,
    denominator: usize,
) -> u64 {
    assert!(!sorted.is_empty());
    assert!(numerator > 0 && numerator <= denominator && denominator > 0);
    let rank = sorted.len().saturating_mul(numerator).div_ceil(denominator);
    sorted[rank.saturating_sub(1)]
}

pub fn post_warmup(mut per_writer: Vec<Vec<u64>>) -> Vec<u64> {
    let retained =
        per_writer.iter().map(|values| values.len() - values.len() / 10).sum();
    let mut merged = Vec::with_capacity(retained);
    for values in &mut per_writer {
        let skip = values.len() / 10;
        merged.extend_from_slice(&values[skip..]);
    }
    merged.sort_unstable();
    merged
}

/// Exact `floor(numerator * scale / denominator)` without overflowing while
/// multiplying the proper fraction by a small integer scale.
pub fn scaled_ratio(numerator: u128, denominator: u128, scale: u64) -> u64 {
    assert!(denominator > 0 && scale > 0);
    let whole = numerator / denominator;
    let value = numerator % denominator;
    let mut quotient = 0_u128;
    let mut remainder = 0_u128;
    let highest_bit = 63 - scale.leading_zeros();
    for bit in (0..=highest_bit).rev() {
        quotient = quotient.checked_mul(2).expect("scaled ratio overflow");
        if remainder > denominator / 2
            || (denominator % 2 == 0 && remainder == denominator / 2)
        {
            remainder -= denominator - remainder;
            quotient += 1;
        } else {
            remainder *= 2;
        }
        if scale & (1_u64 << bit) != 0 {
            if remainder >= denominator - value {
                remainder -= denominator - value;
                quotient += 1;
            } else {
                remainder += value;
            }
        }
    }
    let result = whole
        .checked_mul(u128::from(scale))
        .and_then(|base| base.checked_add(quotient))
        .expect("scaled ratio result overflow");
    u64::try_from(result).expect("scaled ratio exceeds u64")
}

#[derive(Clone, Copy, Debug)]
pub struct FairnessInput {
    pub completed_events: u64,
    pub elapsed_ns:       u64,
    pub p99_ns:           u64,
}

pub fn fairness_ppb(samples: &[FairnessInput]) -> (u64, u64, u64) {
    assert_eq!(samples.len(), 64);
    let mut rates: Vec<u128> = samples
        .iter()
        .map(|sample| {
            assert!(sample.elapsed_ns > 0);
            u128::from(sample.completed_events) * 1_000_000_000_000_000_000
                / u128::from(sample.elapsed_ns)
        })
        .collect();
    assert!(rates.iter().all(|rate| *rate > 0));
    rates.sort_unstable();
    let rate_median_twice =
        rates[31].checked_add(rates[32]).expect("fairness median overflow");
    let rate_sum = rates
        .iter()
        .try_fold(0_u128, |sum, rate| sum.checked_add(*rate).ok_or(()))
        .expect("fairness rate sum overflow");
    let rate_square_sum = rates
        .iter()
        .try_fold(0_u128, |sum, rate| {
            rate.checked_mul(*rate)
                .and_then(|square| sum.checked_add(square))
                .ok_or(())
        })
        .expect("fairness rate square sum overflow");
    let jain_numerator = rate_sum
        .checked_mul(rate_sum)
        .expect("fairness Jain numerator overflow");
    let jain_denominator = (samples.len() as u128)
        .checked_mul(rate_square_sum)
        .expect("fairness Jain denominator overflow");
    let jain = scaled_ratio(jain_numerator, jain_denominator, 1_000_000_000);
    let mut p99s: Vec<u64> =
        samples.iter().map(|sample| sample.p99_ns).collect();
    p99s.sort_unstable();
    (
        jain,
        u64::try_from(
            rates[0]
                .checked_mul(2_000_000_000)
                .expect("fairness minimum-rate ratio overflow")
                / rate_median_twice,
        )
        .expect("fairness minimum-rate ratio exceeds u64"),
        u64::try_from(
            u128::from(*p99s.last().expect("writer p99"))
                .checked_mul(2_000_000_000)
                .expect("fairness p99 ratio overflow")
                / u128::from(
                    p99s[31]
                        .checked_add(p99s[32])
                        .expect("fairness p99 median overflow"),
                ),
        )
        .expect("fairness p99 ratio exceeds u64"),
    )
}
