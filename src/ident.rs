use std::{
    num::NonZeroU128,
    ops::Deref,
    time::{self, SystemTime, UNIX_EPOCH},
};

#[rustfmt::skip]
static ENCODING_DIGITS: [char; 32] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k',
    'm', 'n', 'p', 'q', 'r', 's', 't', 'v', 'w', 'x',
    'y', 'z',
];

fn encode_u128(x: u128, out: &mut [u8]) {
    (0..=125).rev().step_by(5).enumerate().for_each(|(i, shift)| {
        out[i] = ENCODING_DIGITS[(x >> shift & 0b11111) as usize] as u8;
    })
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|t| t.as_millis() as u64)
        .unwrap()
}

pub trait Rng {
    fn u64(&self) -> u64;
}

// impl Rng for fastrand::Rng {
//     fn u64(&self) -> u64 {
//         self.u64(0..=u64::MAX)
//     }
// }

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Id(NonZeroU128);

#[allow(clippy::inline_always)]
impl Id {
    pub fn new(x: NonZeroU128) -> Self {
        Self(x)
    }

    #[inline(always)]
    pub fn as_str(&self) -> &str {
        self.deref()
    }
}

impl core::fmt::Display for Id {
    #[inline(always)]
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(f, "{}", self.deref())
    }
}

impl std::ops::Deref for Id {
    type Target = str;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe {
            let word = self.0.get();
            #[cfg(target_endian = "little")]
            let len = (16 - word.leading_zeros() / 8) as usize;
            #[cfg(target_endian = "big")]
            let len = (16 - word.trailing_zeros() / 8) as usize;
            let slice = core::slice::from_raw_parts(
                &self.0 as *const _ as *const u8,
                len,
            );
            std::str::from_utf8_unchecked(slice)
        }
    }
}

pub trait Clock {
    fn now_millis(&self) -> u64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|t| t.as_millis() as u64)
            .unwrap()
    }
}

pub mod control {
    use super::*;

    pub struct FrozenClock(pub u64);

    impl Clock for FrozenClock {
        fn now_millis(&self) -> u64 {
            self.0
        }
    }
}

/// ```
/// use mess::ident::Identifier;
/// use mess::ident::control::FrozenClock;
///
/// let rng = fastrand::Rng::with_seed(0);
/// // Using frozen clock for test purposes, but Identifier::new(rnd) should
/// // be preferred.
/// let clk = FrozenClock(1234567890);
/// let identifier = Identifier::with_clock(rng, clk);
/// assert_eq!(*identifier.id().as_str(), *"000jcp0b91275kmy");
/// ```
///
/// Prefer ::new(rng).
/// ```
/// use mess::ident::Identifier;
///
/// let rng = fastrand::Rng::with_seed(0);
/// let identifier = Identifier::new(rng);
/// assert_eq!(identifier.id().len(), 16);
/// ```
pub struct Identifier<R, C> {
    rng: R,
    clk: C,
}

impl<R> Identifier<R, SystemClock>
where
    R: Rng + Sized,
{
    pub fn new(rng: R) -> Self {
        Self { rng, clk: SystemClock }
    }
}

impl<R, C> Identifier<R, C>
where
    R: Rng + Sized,
    C: Clock,
{
    pub fn with_clock(rng: R, clk: C) -> Self {
        Self { rng, clk }
    }

    pub fn id(&self) -> Id {
        let mut buf = [0u8; 26];
        let x = new_u128_with(&self.rng, &self.clk);
        encode_u128(x, &mut buf);
        let y = u128::from_le_bytes(buf[4..20]);
        Id({ unsafe { Self(n) } })
    }
}

pub fn new_ulid_with(rng: &impl Rng) -> u128 {
    let t = unix_ms() as u128;
    let x = rng.u64() as u128;
    let y = rng.u64() as u128;
    (t << 80) | ((x & 0xFFFF) << 64) | y
}

pub fn new_u128_with(rng: &impl Rng, clk: &impl Clock) -> u128 {
    let t = clk.now_millis() as u128;
    let x = rng.u64() as u128;
    (t << 64) | x
}
