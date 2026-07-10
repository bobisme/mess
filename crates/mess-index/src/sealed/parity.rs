//! Reed-Solomon parity sidecar (`.par`) for **sealed** segments (bn-2za).
//!
//! The store *detects* at-rest corruption precisely — a per-batch CRC localizes
//! damage to the exact batch, and the fold chain (spec 05) proves committed
//! content — but until now it could not *repair* it: the only answer to a
//! latent sector error / bit-rot on a sealed segment was a backup restore. This
//! module adds an optional **parity sidecar** written at seal, and the offline
//! reconstruction ([`ParitySidecar::plan_repair`]) that `mess verify --repair`
//! drives.
//!
//! # Why Reed-Solomon (not a fountain code)
//!
//! For static, on-device parity with a *known* redundancy budget, RS is
//! MDS-optimal (any `m` erasures per group are correctable with exactly `m`
//! parity shards) and deterministic. Rateless/fountain codes buy their
//! advantages (unknown loss rate, many receivers, no feedback) in a *transport*
//! setting that does not apply to a file on one disk; their home in this system
//! is the future replication / anti-entropy path, not seal-time local parity.
//! We use the `reed-solomon-simd` crate (O(n log n), SIMD, fast at these sizes).
//!
//! # Grouped layout & the default budget
//!
//! The sealed `.log` bytes are cut into fixed-size **shards** (default
//! `4096 B` — one SSD page/sector, so a single latent-sector error damages
//! exactly one shard). Shards are bundled into **groups** of `K` data shards,
//! and each group gets its own `M` RS parity shards. The default `K=16, M=1`
//! gives:
//!
//! - **6.25 % storage overhead** (`M/K`), plus a ~0.1 % per-shard CRC table;
//! - correction of **any single damaged shard within each 64 KiB group** —
//!   i.e. scattered latent errors are corrected independently as long as no
//!   single 64 KiB window loses more than one page.
//!
//! Grouping (vs one RS block over the whole file) matches the *scattered*
//! nature of bit-rot: losses spread across groups rather than piling into one
//! budget. `K`/`M`/shard-size are configurable ([`ParityConfig`]).
//!
//! # Determinism
//!
//! The sidecar carries **no timestamps or nondeterministic fields**. RS encoding
//! is a pure function of the input shards, so the same segment bytes always
//! produce byte-identical `.par` bytes (asserted by a test). This is what lets a
//! rebuild or an independent re-seal reproduce and cross-check the sidecar.
//!
//! # File format (`.par`)
//!
//! ```text
//! offset  size  field
//! 0       4     magic          = PAR_MAGIC
//! 4       2     version        = PAR_VERSION
//! 6       2     flags          (reserved, 0)
//! 8       8     segment_id
//! 16      8     source_len     (byte length of the sealed .log this covers)
//! 24      4     shard_size     (bytes, even, > 0)
//! 28      2     data_per_group (K)
//! 30      2     parity_per_group (M)
//! 32      8     data_shard_count (= ceil(source_len / shard_size))
//! 40      4     source_crc     (crc32c over the whole sealed .log — identity)
//! 44      4     reserved       (0)
//! 48      ...   data_crc_table : data_shard_count × u32 LE
//!               (crc32c of each zero-padded data shard — block localizer)
//! ...     ...   parity_blob    : num_groups × M × shard_size bytes
//! end-4   4     content_crc    : crc32c over [0, end-4)  (self-protection)
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crc32c::crc32c;

/// `.par` sidecar magic (distinct from the segment/footer magics).
pub const PAR_MAGIC: u32 = 0x5EA1_9A55;
/// `.par` on-disk format version.
pub const PAR_VERSION: u16 = 1;
/// Fixed header length (bytes before the per-shard CRC table).
const PAR_HEADER_LEN: usize = 48;

/// Configuration for the seal-time parity sidecar (bn-2za).
///
/// **OFF by default** (`enabled == false`): the feature is evidence-gated per
/// the bone — same-device parity does not survive device death (backups /
/// replication remain the durability story), SSDs already run internal LDPC,
/// and whole-device failure dominates observed SSD failure modes. It is adopted
/// by measured need (verify telemetry / user demand) or as a differentiating
/// ops feature, not by default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParityConfig {
    /// Master switch. When `false`, no `.par` is written at seal.
    pub enabled: bool,
    /// Shard (byte-block) size. Must be even and non-zero. Default `4096`.
    pub shard_size: u32,
    /// Data shards per RS group (`K`). Must be ≥ 1. Default `16`.
    pub data_per_group: u16,
    /// Parity shards per RS group (`M`). Must be ≥ 1. Default `1`
    /// (⇒ 6.25 % overhead, corrects one damaged shard per group).
    pub parity_per_group: u16,
}

impl Default for ParityConfig {
    fn default() -> Self {
        ParityConfig {
            enabled: false,
            shard_size: 4096,
            data_per_group: 16,
            parity_per_group: 1,
        }
    }
}

impl ParityConfig {
    fn validate(&self) -> Result<(), ParityError> {
        if self.shard_size == 0 || !self.shard_size.is_multiple_of(2) {
            return Err(ParityError::Config("shard_size must be even and non-zero"));
        }
        if self.data_per_group == 0 {
            return Err(ParityError::Config("data_per_group (K) must be ≥ 1"));
        }
        if self.parity_per_group == 0 {
            return Err(ParityError::Config("parity_per_group (M) must be ≥ 1"));
        }
        let k = self.data_per_group as usize;
        let m = self.parity_per_group as usize;
        if !reed_solomon_simd::ReedSolomonEncoder::supports(k, m) {
            return Err(ParityError::Config("RS does not support this K/M"));
        }
        Ok(())
    }
}

/// A typed failure of parity generation, parsing, or repair.
#[derive(Debug, thiserror::Error)]
pub enum ParityError {
    /// A [`ParityConfig`] value is out of range.
    #[error("invalid parity config: {0}")]
    Config(&'static str),
    /// The `.par` bytes are shorter than a valid sidecar can be.
    #[error("parity sidecar too short ({0} bytes)")]
    TooShort(usize),
    /// The leading magic is not [`PAR_MAGIC`].
    #[error("parity sidecar bad magic")]
    BadMagic,
    /// The version field is not [`PAR_VERSION`].
    #[error("parity sidecar unsupported version {0}")]
    BadVersion(u16),
    /// The sidecar's own content CRC does not match — the **parity is itself
    /// damaged**; no repair may be attempted from it.
    #[error("parity sidecar content CRC mismatch (parity itself corrupt)")]
    ParityCorrupt,
    /// The sidecar's declared geometry is internally inconsistent with its
    /// byte length.
    #[error("parity sidecar geometry inconsistent: {0}")]
    Geometry(&'static str),
    /// The segment being repaired is a different length than the sidecar was
    /// built over — RS block repair only handles in-place bit-rot, not
    /// truncation/extension.
    #[error("segment length {actual} != parity source length {expected}")]
    LengthMismatch { expected: u64, actual: u64 },
    /// At least one RS group has more damaged shards than its parity budget can
    /// correct. Repair is refused **entirely** (originals untouched).
    #[error(
        "group {group} has {damaged} damaged shards but only {tolerance} parity shards \
         (repair refused; {beyond_groups} group(s) beyond tolerance)"
    )]
    BeyondTolerance {
        /// The first group found beyond tolerance.
        group: u64,
        /// Damaged shard count in that group.
        damaged: usize,
        /// Parity budget per group (`M`).
        tolerance: usize,
        /// How many groups in total exceeded tolerance.
        beyond_groups: usize,
    },
    /// RS reconstruction produced bytes that do not match the recorded shard
    /// CRC — a self-check tripwire (should never fire for a correct decode).
    #[error("reconstructed shard {shard} failed its recorded CRC")]
    ReconstructVerify {
        /// Global data-shard index that failed re-verification.
        shard: u64,
    },
    /// The underlying `reed-solomon-simd` codec returned an error.
    #[error("reed-solomon codec error: {0}")]
    Rs(String),
}

/// The `.par` parity-sidecar path for `segment_id` under the **sealed** dir
/// (`<sealed_dir>/seg-<id:020>.par`), a sibling of the `.pidx`/`.pcol`.
#[must_use]
pub fn par_path(sealed_dir: &Path, segment_id: u64) -> PathBuf {
    sealed_dir.join(format!("seg-{segment_id:020}.par"))
}

/// The zero-padded bytes of data shard `global` of `source` (length
/// `shard_size`). Real bytes come from the file; the tail past EOF (and any
/// shard fully beyond EOF) is zero.
fn data_shard(source: &[u8], global: usize, shard_size: usize) -> Vec<u8> {
    let start = global * shard_size;
    let mut shard = vec![0u8; shard_size];
    if start < source.len() {
        let end = (start + shard_size).min(source.len());
        shard[..end - start].copy_from_slice(&source[start..end]);
    }
    shard
}

/// Generate the `.par` sidecar bytes covering `source` (the sealed `.log`
/// bytes). Deterministic: identical `source` + `cfg` ⇒ identical output.
pub fn generate(
    segment_id: u64,
    source: &[u8],
    cfg: &ParityConfig,
) -> Result<Vec<u8>, ParityError> {
    cfg.validate()?;
    let shard_size = cfg.shard_size as usize;
    let k = cfg.data_per_group as usize;
    let m = cfg.parity_per_group as usize;

    let data_shard_count = source.len().div_ceil(shard_size);
    let num_groups = data_shard_count.div_ceil(k);

    // Fixed header.
    let mut out = Vec::with_capacity(
        PAR_HEADER_LEN + 4 * data_shard_count + num_groups * m * shard_size + 4,
    );
    out.extend_from_slice(&PAR_MAGIC.to_le_bytes());
    out.extend_from_slice(&PAR_VERSION.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // flags
    out.extend_from_slice(&segment_id.to_le_bytes());
    out.extend_from_slice(&(source.len() as u64).to_le_bytes());
    out.extend_from_slice(&(shard_size as u32).to_le_bytes());
    out.extend_from_slice(&cfg.data_per_group.to_le_bytes());
    out.extend_from_slice(&cfg.parity_per_group.to_le_bytes());
    out.extend_from_slice(&(data_shard_count as u64).to_le_bytes());
    out.extend_from_slice(&crc32c(source).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes()); // reserved
    debug_assert_eq!(out.len(), PAR_HEADER_LEN);

    // Per-shard CRC table (the block localizer).
    for global in 0..data_shard_count {
        let crc = crc32c(&data_shard(source, global, shard_size));
        out.extend_from_slice(&crc.to_le_bytes());
    }

    // Parity blob, group-major.
    for g in 0..num_groups {
        let shards: Vec<Vec<u8>> =
            (0..k).map(|j| data_shard(source, g * k + j, shard_size)).collect();
        let recovery = reed_solomon_simd::encode(k, m, &shards)
            .map_err(|e| ParityError::Rs(e.to_string()))?;
        for shard in recovery {
            debug_assert_eq!(shard.len(), shard_size);
            out.extend_from_slice(&shard);
        }
    }

    // Self-protecting content CRC over everything written so far.
    let content_crc = crc32c(&out);
    out.extend_from_slice(&content_crc.to_le_bytes());
    Ok(out)
}

/// A parsed, CRC-validated `.par` sidecar.
#[derive(Debug, Clone)]
pub struct ParitySidecar {
    segment_id: u64,
    source_len: u64,
    shard_size: usize,
    data_per_group: usize,
    parity_per_group: usize,
    data_shard_count: usize,
    source_crc: u32,
    /// Per-data-shard crc32c (length `data_shard_count`).
    data_crcs: Vec<u32>,
    /// Group-major parity shards: `num_groups × M`, each `shard_size` bytes.
    parity: Vec<u8>,
}

/// The outcome of a successful [`ParitySidecar::plan_repair`]: the repaired
/// segment image and exactly which data shards (byte blocks) were reconstructed.
#[derive(Debug, Clone)]
pub struct RepairPlan {
    /// The reconstructed, byte-complete segment image (same length as input).
    pub image: Vec<u8>,
    /// Global data-shard indices that were damaged and reconstructed, ascending.
    pub repaired_blocks: Vec<u64>,
    /// Byte ranges `[start, end)` of each repaired block within the segment
    /// (aligned to the shard grid; the last block is clamped to `source_len`).
    pub repaired_ranges: Vec<(u64, u64)>,
}

fn rd_u16(d: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(d[at..at + 2].try_into().unwrap())
}
fn rd_u32(d: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(d[at..at + 4].try_into().unwrap())
}
fn rd_u64(d: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(d[at..at + 8].try_into().unwrap())
}

impl ParitySidecar {
    /// The segment id this sidecar covers.
    #[must_use]
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }
    /// The byte length of the sealed `.log` this sidecar was built over.
    #[must_use]
    pub fn source_len(&self) -> u64 {
        self.source_len
    }
    /// crc32c of the whole original sealed `.log` (identity witness).
    #[must_use]
    pub fn source_crc(&self) -> u32 {
        self.source_crc
    }
    /// Shard (byte-block) size in bytes.
    #[must_use]
    pub fn shard_size(&self) -> usize {
        self.shard_size
    }
    /// Data shards per group (`K`).
    #[must_use]
    pub fn data_per_group(&self) -> usize {
        self.data_per_group
    }
    /// Parity shards per group (`M`) — the per-group correction budget.
    #[must_use]
    pub fn parity_per_group(&self) -> usize {
        self.parity_per_group
    }
    /// Total number of data shards covered.
    #[must_use]
    pub fn data_shard_count(&self) -> usize {
        self.data_shard_count
    }

    /// Open and validate a `.par` sidecar from disk. Outer `io::Result` is the
    /// read; inner `Result` is the parse/CRC validation.
    pub fn open(path: &Path) -> std::io::Result<Result<Self, ParityError>> {
        Ok(Self::from_bytes(std::fs::read(path)?))
    }

    /// Parse and CRC-validate a `.par` byte image.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, ParityError> {
        if bytes.len() < PAR_HEADER_LEN + 4 {
            return Err(ParityError::TooShort(bytes.len()));
        }
        if rd_u32(&bytes, 0) != PAR_MAGIC {
            return Err(ParityError::BadMagic);
        }
        let version = rd_u16(&bytes, 4);
        if version != PAR_VERSION {
            return Err(ParityError::BadVersion(version));
        }
        // Self CRC over [0, end-4). Verify BEFORE trusting any geometry field.
        let footer = bytes.len() - 4;
        let stored = rd_u32(&bytes, footer);
        if crc32c(&bytes[..footer]) != stored {
            return Err(ParityError::ParityCorrupt);
        }

        let segment_id = rd_u64(&bytes, 8);
        let source_len = rd_u64(&bytes, 16);
        let shard_size = rd_u32(&bytes, 24) as usize;
        let data_per_group = rd_u16(&bytes, 28) as usize;
        let parity_per_group = rd_u16(&bytes, 30) as usize;
        let data_shard_count = rd_u64(&bytes, 32) as usize;
        let source_crc = rd_u32(&bytes, 40);

        if shard_size == 0 || !shard_size.is_multiple_of(2) {
            return Err(ParityError::Geometry("bad shard_size"));
        }
        if data_per_group == 0 || parity_per_group == 0 {
            return Err(ParityError::Geometry("bad K/M"));
        }
        // data_shard_count must agree with source_len and shard_size.
        if data_shard_count != (source_len as usize).div_ceil(shard_size) {
            return Err(ParityError::Geometry("data_shard_count vs source_len"));
        }
        let num_groups = data_shard_count.div_ceil(data_per_group);
        let crc_table_len = 4 * data_shard_count;
        let parity_len = num_groups * parity_per_group * shard_size;
        let expected = PAR_HEADER_LEN + crc_table_len + parity_len + 4;
        if bytes.len() != expected {
            return Err(ParityError::Geometry("total length"));
        }

        let mut data_crcs = Vec::with_capacity(data_shard_count);
        let mut p = PAR_HEADER_LEN;
        for _ in 0..data_shard_count {
            data_crcs.push(rd_u32(&bytes, p));
            p += 4;
        }
        let parity = bytes[p..p + parity_len].to_vec();

        Ok(ParitySidecar {
            segment_id,
            source_len,
            shard_size,
            data_per_group,
            parity_per_group,
            data_shard_count,
            source_crc,
            data_crcs,
            parity,
        })
    }

    /// The `M` parity shards for group `g`.
    fn group_parity(&self, g: usize) -> Vec<&[u8]> {
        let base = g * self.parity_per_group * self.shard_size;
        (0..self.parity_per_group)
            .map(|p| {
                let s = base + p * self.shard_size;
                &self.parity[s..s + self.shard_size]
            })
            .collect()
    }

    /// Localize damaged data shards in `current` by comparing each shard's
    /// recomputed crc32c against the recorded table. Returns ascending global
    /// indices. (`current.len()` must equal [`source_len`](Self::source_len).)
    #[must_use]
    pub fn damaged_shards(&self, current: &[u8]) -> Vec<u64> {
        let mut damaged = Vec::new();
        for i in 0..self.data_shard_count {
            let got = crc32c(&data_shard(current, i, self.shard_size));
            if got != self.data_crcs[i] {
                damaged.push(i as u64);
            }
        }
        damaged
    }

    /// Attempt to reconstruct every damaged block of `current` from parity,
    /// **without writing anything**. Returns the repaired image + the exact
    /// blocks reconstructed. Refuses (typed error, no partial output) if the
    /// segment length differs, or any group exceeds its parity budget.
    ///
    /// The caller MUST still re-verify the returned [`RepairPlan::image`]
    /// against the segment's own batch CRCs (+ fold chain) before installing
    /// it — parity proves *erasure recovery*, the batch CRC/chain prove the
    /// bytes are the committed bytes.
    pub fn plan_repair(&self, current: &[u8]) -> Result<RepairPlan, ParityError> {
        if current.len() as u64 != self.source_len {
            return Err(ParityError::LengthMismatch {
                expected: self.source_len,
                actual: current.len() as u64,
            });
        }
        let k = self.data_per_group;
        let m = self.parity_per_group;
        let shard_size = self.shard_size;

        let damaged = self.damaged_shards(current);

        // Group the damaged shards and check tolerance up-front.
        let mut per_group: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for &g_shard in &damaged {
            let idx = g_shard as usize;
            per_group.entry(idx / k).or_default().push(idx);
        }
        let beyond: Vec<(usize, usize)> = per_group
            .iter()
            .filter(|(_, v)| v.len() > m)
            .map(|(g, v)| (*g, v.len()))
            .collect();
        if let Some(&(group, damaged_ct)) = beyond.first() {
            return Err(ParityError::BeyondTolerance {
                group: group as u64,
                damaged: damaged_ct,
                tolerance: m,
                beyond_groups: beyond.len(),
            });
        }

        let mut image = current.to_vec();
        let mut repaired_blocks = Vec::new();
        let mut repaired_ranges = Vec::new();

        for (&g, missing) in &per_group {
            // Surviving original shards (all non-damaged slots of the group).
            let missing_set: std::collections::BTreeSet<usize> = missing.iter().copied().collect();
            let mut originals: Vec<(usize, Vec<u8>)> = Vec::with_capacity(k);
            for j in 0..k {
                let global = g * k + j;
                if missing_set.contains(&global) {
                    continue; // erased
                }
                originals.push((j, data_shard(current, global, shard_size)));
            }
            let parity = self.group_parity(g);
            let recovery: Vec<(usize, &[u8])> =
                parity.iter().enumerate().map(|(p, s)| (p, *s)).collect();

            let restored = reed_solomon_simd::decode(k, m, originals, recovery)
                .map_err(|e| ParityError::Rs(e.to_string()))?;

            for (slot, bytes) in restored {
                let global = g * k + slot;
                if global >= self.data_shard_count {
                    continue; // a pure padding slot — nothing on disk to write
                }
                // Tripwire: the reconstructed shard must match its recorded CRC.
                if crc32c(&bytes) != self.data_crcs[global] {
                    return Err(ParityError::ReconstructVerify { shard: global as u64 });
                }
                let start = global * shard_size;
                let end = (start + shard_size).min(image.len());
                image[start..end].copy_from_slice(&bytes[..end - start]);
                repaired_blocks.push(global as u64);
                repaired_ranges.push((start as u64, end as u64));
            }
        }

        repaired_blocks.sort_unstable();
        repaired_ranges.sort_unstable();
        Ok(RepairPlan { image, repaired_blocks, repaired_ranges })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic xorshift for reproducible corpora.
    struct Rng(u64);
    impl Rng {
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn fill(&mut self, buf: &mut [u8]) {
            for b in buf.iter_mut() {
                *b = (self.next_u64() & 0xFF) as u8;
            }
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next_u64() % n as u64) as usize
        }
    }

    fn cfg() -> ParityConfig {
        ParityConfig { enabled: true, shard_size: 64, data_per_group: 4, parity_per_group: 2 }
    }

    fn corpus(len: usize, seed: u64) -> Vec<u8> {
        let mut r = Rng(seed);
        let mut v = vec![0u8; len];
        r.fill(&mut v);
        v
    }

    #[test]
    fn generation_is_deterministic() {
        let src = corpus(4000, 1);
        let a = generate(7, &src, &cfg()).unwrap();
        let b = generate(7, &src, &cfg()).unwrap();
        assert_eq!(a, b, "same segment must yield byte-identical .par");
    }

    #[test]
    fn parse_roundtrips_geometry() {
        let src = corpus(4000, 2);
        let bytes = generate(42, &src, &cfg()).unwrap();
        let side = ParitySidecar::from_bytes(bytes).unwrap();
        assert_eq!(side.segment_id(), 42);
        assert_eq!(side.source_len(), 4000);
        assert_eq!(side.source_crc(), crc32c(&src));
        assert_eq!(side.data_shard_count(), 4000usize.div_ceil(64));
    }

    #[test]
    fn no_damage_repairs_to_noop() {
        let src = corpus(4000, 3);
        let side = ParitySidecar::from_bytes(generate(1, &src, &cfg()).unwrap()).unwrap();
        let plan = side.plan_repair(&src).unwrap();
        assert!(plan.repaired_blocks.is_empty());
        assert_eq!(plan.image, src);
    }

    /// Corrupt exactly `M` shards in one group (the tolerance edge) — repair is
    /// byte-exact.
    #[test]
    fn repairs_within_tolerance_byte_exact() {
        let src = corpus(4000, 4);
        let c = cfg();
        let side = ParitySidecar::from_bytes(generate(1, &src, &c).unwrap()).unwrap();
        let mut damaged = src.clone();
        // Damage shards 0 and 1 (group 0, M=2) — flip a byte in each.
        damaged[0] ^= 0xFF;
        damaged[c.shard_size as usize] ^= 0xFF;
        let plan = side.plan_repair(&damaged).unwrap();
        assert_eq!(plan.repaired_blocks, vec![0, 1]);
        assert_eq!(plan.image, src, "repaired image must be byte-exact");
    }

    /// Scatter one damaged shard across several groups — all corrected.
    #[test]
    fn repairs_scattered_single_shard_per_group() {
        let src = corpus(20_000, 5);
        let c = cfg();
        let side = ParitySidecar::from_bytes(generate(1, &src, &c).unwrap()).unwrap();
        let ss = c.shard_size as usize;
        let k = c.data_per_group as usize;
        let mut damaged = src.clone();
        let mut expect = Vec::new();
        let groups = side.data_shard_count().div_ceil(k);
        for g in 0..groups {
            let shard = g * k + (g % k); // one shard, varied slot
            if shard >= side.data_shard_count() {
                continue;
            }
            damaged[shard * ss] ^= 0xAA;
            expect.push(shard as u64);
        }
        let plan = side.plan_repair(&damaged).unwrap();
        assert_eq!(plan.repaired_blocks, expect);
        assert_eq!(plan.image, src);
    }

    /// Damage `M+1` shards in one group — typed refusal, no output.
    #[test]
    fn beyond_tolerance_refuses() {
        let src = corpus(4000, 6);
        let c = cfg(); // M=2
        let side = ParitySidecar::from_bytes(generate(1, &src, &c).unwrap()).unwrap();
        let ss = c.shard_size as usize;
        let mut damaged = src.clone();
        for j in 0..3 {
            damaged[j * ss] ^= 0xFF; // shards 0,1,2 all in group 0
        }
        match side.plan_repair(&damaged) {
            Err(ParityError::BeyondTolerance { group, damaged: d, tolerance, .. }) => {
                assert_eq!(group, 0);
                assert_eq!(d, 3);
                assert_eq!(tolerance, 2);
            }
            other => panic!("expected BeyondTolerance, got {other:?}"),
        }
    }

    /// A corrupted `.par` sidecar is detected by its own content CRC; no repair.
    #[test]
    fn parity_self_corruption_detected() {
        let src = corpus(4000, 7);
        let mut bytes = generate(1, &src, &cfg()).unwrap();
        // Flip a byte inside the parity blob (covered by the content CRC).
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        match ParitySidecar::from_bytes(bytes) {
            Err(ParityError::ParityCorrupt) => {}
            other => panic!("expected ParityCorrupt, got {other:?}"),
        }
    }

    #[test]
    fn length_mismatch_refuses() {
        let src = corpus(4000, 8);
        let side = ParitySidecar::from_bytes(generate(1, &src, &cfg()).unwrap()).unwrap();
        let mut short = src.clone();
        short.truncate(3000);
        match side.plan_repair(&short) {
            Err(ParityError::LengthMismatch { expected, actual }) => {
                assert_eq!(expected, 4000);
                assert_eq!(actual, 3000);
            }
            other => panic!("expected LengthMismatch, got {other:?}"),
        }
    }

    /// Fuzz: many random corpora, corrupt up to M shards per group at random —
    /// always byte-exact recovery.
    #[test]
    fn random_within_tolerance_always_recovers() {
        let c = ParityConfig { enabled: true, shard_size: 128, data_per_group: 8, parity_per_group: 3 };
        let ss = c.shard_size as usize;
        let k = c.data_per_group as usize;
        let m = c.parity_per_group as usize;
        for seed in 0..40u64 {
            let len = 500 + (seed as usize) * 137;
            let src = corpus(len, seed * 31 + 1);
            let side = ParitySidecar::from_bytes(generate(seed, &src, &c).unwrap()).unwrap();
            let mut damaged = src.clone();
            let mut r = Rng(seed * 7 + 3);
            let groups = side.data_shard_count().div_ceil(k);
            for g in 0..groups {
                let dmg = r.below(m + 1); // 0..=M
                let mut slots: Vec<usize> = (0..k).collect();
                for d in 0..dmg {
                    let pick = r.below(slots.len());
                    let slot = slots.remove(pick);
                    let global = g * k + slot;
                    if global >= side.data_shard_count() {
                        continue;
                    }
                    let byte = global * ss + r.below(ss.min(len.saturating_sub(global * ss).max(1)));
                    if byte < damaged.len() {
                        damaged[byte] ^= 0x5A | (d as u8 + 1);
                    }
                }
            }
            let plan = side.plan_repair(&damaged).unwrap();
            assert_eq!(plan.image, src, "seed {seed}: recovery not byte-exact");
        }
    }
}

#[cfg(test)]
mod bench {
    use super::*;
    use std::time::Instant;

    fn corpus(len: usize, seed: u64) -> Vec<u8> {
        let mut x = seed | 1;
        let mut v = vec![0u8; len];
        for b in v.iter_mut() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *b = (x & 0xFF) as u8;
        }
        v
    }

    /// bn-2za measurement: parity-generation throughput + storage overhead on a
    /// representative large sealed segment. A sealed segment maxes at the
    /// engine's 256 MiB `SEGMENT_SIZE`; we measure on a full-size 256 MiB image
    /// with the production default config (4 KiB shards, 16+1 per group).
    ///
    /// ```text
    /// TMPDIR=$HOME/.cache/mess-test-tmp cargo test -p mess-index --release \
    ///   sealed::parity::bench::measure_parity_throughput_and_overhead -- \
    ///   --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "measurement; run explicitly with --ignored --nocapture"]
    fn measure_parity_throughput_and_overhead() {
        let cfg = ParityConfig { enabled: true, ..Default::default() };
        for &mib in &[64usize, 256] {
            let len = mib * 1024 * 1024;
            let src = corpus(len, 0xC0FFEE ^ mib as u64);

            let t0 = Instant::now();
            let par = generate(1, &src, &cfg).unwrap();
            let dt = t0.elapsed();

            let mbps = (len as f64 / (1024.0 * 1024.0)) / dt.as_secs_f64();
            let overhead = par.len() as f64 / len as f64 * 100.0;
            println!(
                "[parity] {mib} MiB segment | shard={}B K={} M={} | gen {:?} \
                 ({mbps:.0} MiB/s) | sidecar {} bytes | overhead {overhead:.3}%",
                cfg.shard_size, cfg.data_per_group, cfg.parity_per_group, dt, par.len()
            );

            // Sanity: a full round of decode over a lightly-damaged copy works.
            let side = ParitySidecar::from_bytes(par).unwrap();
            let mut damaged = src.clone();
            damaged[123] ^= 0xFF; // one damaged shard, group 0
            let t1 = Instant::now();
            let plan = side.plan_repair(&damaged).unwrap();
            let repair_dt = t1.elapsed();
            assert_eq!(plan.image, src);
            println!("[parity] {mib} MiB single-block repair (localize+decode+verify): {repair_dt:?}");
        }
    }
}
