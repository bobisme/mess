//! [`RetryPolicy`]: the optimistic-retry budget and jittered backoff for
//! [`EventStore::command`](crate::EventStore::command).

use std::cell::Cell;
use std::time::Duration;

/// The default optimistic-retry budget.
///
/// `spikes/dx_api` measured its original budget of 16 as too small on one hot
/// stream (8 writers × 25 commands drove a single command up to **24**
/// attempts). The default here is deliberately **above** that observed
/// worst case so the north-star workload succeeds without tuning.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 64;

/// Bounded optimistic retry with exponential, fully-jittered backoff.
///
/// On each version conflict the facade sleeps a random duration in
/// `[0, cap]`, where `cap = min(max_backoff, base_backoff · 2^(attempt-1))`
/// (the "full jitter" schedule) — spreading concurrent writers out so they
/// stop colliding, instead of retrying in lockstep. When the budget is
/// exhausted the facade returns the distinct
/// [`CommandError::Conflict`](mess_core::CommandError::Conflict) variant,
/// carrying the attempt count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Maximum number of load→decide→append attempts before giving up with a
    /// conflict-exhaustion error. Always at least 1.
    pub max_attempts: u32,
    /// The base backoff, doubled each attempt before jitter. Zero disables
    /// backoff (retry immediately) — useful in tests.
    pub base_backoff: Duration,
    /// The ceiling the doubled backoff is clamped to before jitter.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            base_backoff: Duration::from_micros(100),
            max_backoff: Duration::from_millis(5),
        }
    }
}

impl RetryPolicy {
    /// A policy with `max_attempts` (clamped to at least 1) and default
    /// backoff.
    #[must_use]
    pub fn with_max_attempts(max_attempts: u32) -> Self {
        Self { max_attempts: max_attempts.max(1), ..Self::default() }
    }

    /// A policy that retries immediately with no backoff sleeps.
    #[must_use]
    pub fn no_backoff(max_attempts: u32) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            base_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        }
    }

    /// The fully-jittered backoff to sleep before the retry that follows a
    /// conflict on `attempt` (1-based). `Duration::ZERO` when backoff is
    /// disabled.
    #[must_use]
    pub fn backoff_for(&self, attempt: u32) -> Duration {
        if self.base_backoff.is_zero() {
            return Duration::ZERO;
        }
        let base = self.base_backoff.as_nanos() as u64;
        let cap = self.max_backoff.as_nanos().max(1) as u64;
        // base · 2^(attempt-1), saturating, then clamped to the cap.
        let shift = attempt.saturating_sub(1).min(63);
        let exp = base.saturating_mul(1u64 << shift).min(cap);
        let jittered = next_jitter() % exp.max(1);
        Duration::from_nanos(jittered)
    }
}

thread_local! {
    static RNG: Cell<u64> = Cell::new(seed());
}

/// A per-thread seed that differs across threads and runs without pulling in
/// a PRNG crate.
fn seed() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut h = RandomState::new().build_hasher();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    h.write_u64(nanos);
    h.write_usize(&RNG as *const _ as usize);
    let s = h.finish();
    // xorshift must never be seeded with 0.
    if s == 0 { 0x9E37_79B9_7F4A_7C15 } else { s }
}

/// Next value from a thread-local xorshift64* generator — cheap, non-crypto
/// jitter, no external dependency.
fn next_jitter() -> u64 {
    RNG.with(|cell| {
        let mut x = cell.get();
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        cell.set(x);
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    })
}
