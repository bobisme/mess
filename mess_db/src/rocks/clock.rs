//!Hybrid Logical Clock, yo

use std::{
    marker::PhantomData,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};

/// Seconds since Unix epoch for 2020-01-01T00:00:00Z.
const SECOND_EPOCH: u64 = 1_577_836_800;

#[must_use] pub fn time_to_tick(time: Duration) -> u64 {
    (((time.as_secs_f64() - SECOND_EPOCH as f64) * 20.0) as u64) << 16
}

#[derive(Copy, Clone, Debug, PartialEq, PartialOrd)]
pub struct Tick(u64);

impl Tick {
    #[must_use] pub fn to_u64(self) -> u64 {
        self.0
    }

    #[must_use] pub fn to_secs(self) -> u64 {
        (self.0 >> 16) / 20 + SECOND_EPOCH
    }

    #[must_use] pub fn to_secs_f64(self) -> f64 {
        (self.0 >> 16) as f64 / 20.0 + SECOND_EPOCH as f64
    }
}

/// Convert from a Duration
impl From<Duration> for Tick {
    fn from(value: Duration) -> Self {
        Self(time_to_tick(value))
    }
}

/// Convert from a tuple of (Duration, offset)
impl From<(Duration, u16)> for Tick {
    fn from(value: (Duration, u16)) -> Self {
        let (time, offset) = value;
        let tick = time_to_tick(time);
        Self(tick + offset as u64)
    }
}

/// Provider of a simple now() function. This is just here for testing.
/// By default, SystemTime is used.
pub trait Now {
    fn now() -> Duration;
}

impl Now for SystemTime {
    /// Returns a the duration since the unix epoch.
    fn now() -> Duration {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("failed to get system time since unix epoch")
    }
}

#[derive(Debug)]
pub struct Clock<Sys> {
    last: AtomicU64,
    time: PhantomData<Sys>,
}

impl<Sys> Clock<Sys>
where
    Sys: Now,
{
    pub fn last(&self) -> Tick {
        Tick(self.last.load(Ordering::SeqCst))
    }

    pub fn observe(&self, tick: Tick) {
        loop {
            let last = self.last();
            if tick <= last {
                return;
            }
            let res = self.last.compare_exchange(
                last.0,
                tick.0,
                Ordering::Release,
                Ordering::Acquire,
            );
            if res.is_ok() {
                return;
            }
            // Someone got there before us. Loop in case our tick is more recent.
            std::hint::spin_loop();
        }
        // self.last.store(tick.0, Ordering::SeqCst);
    }

    pub fn next(&self) -> Tick {
        let now = Sys::now();
        let current = Tick::from(now);
        let last = self.last();
        if last > current {
            Tick(last.0 + 1)
        } else {
            current
        }
    }
}

impl Default for Clock<SystemTime> {
    fn default() -> Self {
        Self { last: Default::default(), time: Default::default() }
    }
}

#[cfg(test)]
mod test_tick {
    use super::*;
    use assert2::assert;

    // 2023-01-02T03:04:05.678Z
    const TIMESTAMP: f64 = 1672628645.678;

    #[test]
    fn test_time_to_tick() {
        let dur = Duration::from_secs_f64(TIMESTAMP);
        let tick = time_to_tick(dur);
        assert!((tick >> 16) / 20 + SECOND_EPOCH == TIMESTAMP as u64);
    }

    #[test]
    fn test_tick_to_secs() {
        let tick =
            Tick((((TIMESTAMP - SECOND_EPOCH as f64) * 20.0) as u64) << 16);
        assert!(tick.to_secs() == TIMESTAMP as u64);
    }

    #[test]
    fn test_tick_to_secs_f64() {
        let tick =
            Tick((((TIMESTAMP - SECOND_EPOCH as f64) * 20.0) as u64) << 16);
        assert!((tick.to_secs_f64() - TIMESTAMP).abs() < 0.1);
    }

    #[test]
    fn test_tick_from_duration() {
        let dur = Duration::from_secs_f64(TIMESTAMP);
        let tick: Tick = dur.into();
        let expected =
            Tick((((TIMESTAMP - SECOND_EPOCH as f64) * 20.0) as u64) << 16);
        assert!(tick == expected);
    }

    #[test]
    fn test_tick_from_duration_offset() {
        let dur = Duration::from_secs_f64(TIMESTAMP);
        let tick: Tick = (dur, 17u16).into();
        let expected = Tick(
            ((((TIMESTAMP - SECOND_EPOCH as f64) * 20.0) as u64) << 16) + 17,
        );
        assert!(tick == expected);
    }
}

#[cfg(test)]
mod test_clock {
    use super::*;
    use assert2::assert;

    static TIME_MS: AtomicU64 = AtomicU64::new(1_000_000);

    fn must_be_sync_and_send(_x: impl Sync + Send) {
        assert!(true);
    }

    #[rstest::rstest]
    fn it_is_sync_and_send() {
        let clock = Clock::default();
        must_be_sync_and_send(&clock);
        must_be_sync_and_send(clock);
    }

    #[derive(Copy, Clone, Debug)]
    struct BrokenClock;

    fn dur_from_ms(ms: u64) -> Duration {
        Duration::from_millis(ms + SECOND_EPOCH * 1_000)
    }

    impl Now for BrokenClock {
        fn now() -> Duration {
            dur_from_ms(TIME_MS.load(Ordering::SeqCst))
        }
    }

    #[rstest::rstest]
    fn it_can_observe_and_return_values(#[values(0, 100, 1 << 48)] input: u64) {
        let clock = Clock::default();
        clock.observe(Tick(input));
        let stored = clock.last();
        assert!(stored.0 == input);
    }

    #[rstest::rstest]
    fn observe_stores_the_given_tick_if_higher() {
        let clock = Clock::default();
        clock.observe(Tick(200));
        clock.observe(Tick(300));
        clock.observe(Tick(100));
        clock.observe(Tick(150));
        let stored = clock.last.load(Ordering::SeqCst);
        assert!(stored == 300);
    }

    #[rstest::rstest]
    fn next_uses_current_time_if_ahead_of_last() {
        let clock = Clock {
            time: PhantomData::<BrokenClock>,
            last: Default::default(),
        };
        clock.observe(Tick::from(dur_from_ms(900_000)));
        TIME_MS.store(1_000_000, Ordering::SeqCst);
        assert!(clock.next() == Tick::from(dur_from_ms(1_000_000)));
    }

    #[rstest::rstest]
    fn next_increments_last_if_current_is_behind() {
        let clock = Clock {
            time: PhantomData::<BrokenClock>,
            last: Default::default(),
        };
        clock.observe(Tick::from(dur_from_ms(1_100_000)));
        TIME_MS.store(1_000_000, Ordering::SeqCst);
        assert!(clock.next() == Tick::from((dur_from_ms(1_100_000), 1)));
    }
}
