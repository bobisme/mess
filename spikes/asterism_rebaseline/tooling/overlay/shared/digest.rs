//! Generation-neutral logical digest shared by public and bare surfaces.

const OFFSET: u64 = 0xCBF29CE484222325;
const PRIME: u64 = 0x100000001B3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalDigest(u64);

impl Default for LogicalDigest {
    fn default() -> Self { Self(OFFSET) }
}

impl LogicalDigest {
    pub fn update_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(PRIME);
        }
    }

    pub fn update_u64(&mut self, value: u64) {
        self.update_bytes(&value.to_le_bytes());
    }

    pub fn committed_append(
        &mut self,
        writer: u64,
        append: u64,
        stream_ordinal: u64,
        batch: u64,
        payload: &[u8],
    ) {
        self.update_u64(writer);
        self.update_u64(append);
        self.update_u64(stream_ordinal);
        self.update_u64(batch);
        self.update_u64(payload.len() as u64);
        self.update_bytes(payload);
    }

    pub fn combine(mut self, other: Self) -> Self {
        self.update_u64(other.0);
        self
    }

    pub fn value(self) -> u64 { self.0 }
}
