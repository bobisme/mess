# Research 03: succinct indexes, active microblocks, and exact epoch dedupe

## 1. Design rule

Asterism uses a different data structure for each lifecycle phase:

```text
writer-owned hot state     -> dense arrays / append-only arenas
small mutable overlays     -> ordinary high-quality hash tables
sealed immutable sets      -> rank/select, Elias–Fano, packed static functions
negative acceleration      -> BinaryFuse / blocked age-partitioned filters
exact identity             -> canonical key comparison
```

No approximate or static function is allowed to decide a user-visible fact without exact verification.

## 2. Dense head tables

### 2.1 Addressing

Stream IDs are monotonically allocated `u64`s. Let `P = 2^12 = 4096` cells per page:

```text
page = stream_id >> 12
slot = stream_id & 4095
```

The top-level page directory can be a grow-only vector of atomic/RCU page pointers. Pages are allocated in ID order and remain stable.

### 2.2 Cell choices

Candidate A — two atomics plus page sequence:

```rust
struct HeadCell {
    version: AtomicU64,
    global: AtomicU64,
}
```

Candidate B — double-buffered cell:

```rust
struct HeadCell {
    copy: [UnsafeCell<Head>; 2],
    active: AtomicU8,
}
```

The second design needs careful proof that a reader cannot observe a copy while the writer reuses it; a page sequence or reader epoch may still be required. Candidate A is simpler and valid under Rust’s atomic rules.

Candidate C — target-specific atomic 128-bit packed head. This may be fastest on CPUs with lock-free 128-bit CAS/load but must not become the portable format or default without runtime capability detection.

### 2.3 Expected bounds

Information content is 128 bits per present stream before presence metadata. A 16-byte resident cell is therefore close to the uncompressed lower bound. The top-level/page overhead is negligible at filled pages. Empty tail pages are not allocated.

### 2.4 Presence

Dense assigned IDs imply every ID from `1..next_id` has a registry object, but not necessarily an event head. Use either:

- `version = u64::MAX` as `NoStream`; or
- a presence bitvector per page.

A presence bit costs 0.125 bytes/stream and makes all 64-bit version values available. The benchmark decides.

## 3. Active pointer microblocks

### 3.1 Shape

One stream can have many append batches in the active/unsealed region. A fixed-capacity block avoids per-stream vector growth:

```rust
struct BatchPtr {
    first_version_delta: u32,
    frame_count: u32,
    first_global_delta: u32,
    offset_delta: u32,
}

struct PtrMicroblock {
    stream_id: u64,
    base_version: u64,
    base_global: u64,
    base_offset: u64,
    previous: u32,
    published: AtomicU16,
    entries: [MaybeUninit<BatchPtr>; 32],
}
```

If a delta does not fit, encode an escape entry with full-width values in an overflow arena. The active segment is bounded, so most deltas should fit; this must be measured.

### 3.2 Publication

The single writer writes entry bytes, then release-stores `published = n+1`. Readers acquire-load `published` before reading slots. A new block is initialized privately, then its ID is release-stored into the stream’s tail cell.

### 3.3 Lookup

For a recent stream version:

1. load tail block ID;
2. compare version against block range;
3. binary search or linear scan up to 32 entries;
4. follow `previous` only when needed.

Linear scan may beat binary search because 32 compact entries fit in a few cache lines and branch prediction is good. Benchmark both. For very hot streams, add every eighth block to a per-stream skip chain.

### 3.4 Slab reclamation

Allocate blocks in segment-generation slabs. Once a segment’s SealPack is installed and active readers release that generation, free the whole slab. No per-block free list or epoch reclamation is required.

## 4. Sealed stream-directory representations

For a segment, define:

```text
keys = sorted distinct stream IDs
n    = number of keys
U    = max(keys) - min(keys) + 1
ρ    = n/U
```

### 4.1 Sorted fixed array

Baseline:

```text
keys[n] u64
entries[n] packed directory payload
```

Lookup is branchy binary search but construction is trivial and exact. It remains the baseline every clever structure must beat.

### 4.2 Bitvector + rank

Use when density is high enough.

```text
bits[U]
superblock rank every 512 bits
subblock rank every 64 bits
entries[n]
```

Exact lookup:

```text
bit = keys_present[id-min]
if !bit: absent
slot = super_rank + popcount(subblock prefix)
```

On CPUs with hardware `popcnt`, rank is cheap. Space:

```text
U bits + O(U/word) rank metadata
```

At `U/n <= 8`, the key directory is about one byte/key before rank metadata, often much smaller than storing an eight-byte key.

### 4.3 Partitioned Elias–Fano

Use for sparse monotone IDs. Elias–Fano represents `n` keys from universe `U` in approximately:

```text
n * ceil(log2(U/n)) + 2n bits
```

Partition into blocks of 128–512 keys so each partition has local `min/max`, a short high-bit unary vector, low bits, and a small directory. This improves cache locality and permits SIMD/vectorized predecessor search.

Lookup performs predecessor/rank and then exact equality. The entry array is stored in the same order, so the rank is the directory slot.

### 4.4 PtrHash

PtrHash is a 2025 minimal perfect hash optimized for throughput rather than absolute minimum bits. The paper reports 2.4 bits/key and 8–12 ns integer-query throughput in streaming/scalar modes on its hardware. For Mess:

```text
slot = ptrhash(stream_id)
if packed_keys[slot] != stream_id: absent
return entries[slot]
```

Exact key verification is mandatory because an MPHF gives arbitrary values for nonmembers. Store packed keys or a sufficiently compact exact representation.

PtrHash construction and query code must be integrated in Rust without hidden allocator or serialization costs. Its paper implementation is Rust, which makes it unusually practical to evaluate.

### 4.5 Cache-line k-perfect hashing

The July 2026 k-PHF work maps keys to bins of capacity `k`, targeting one cache-line miss at high load. A Mess bin could be:

```text
Bin<k=4 or 8>:
  key deltas / full keys
  directory slot payloads
```

Lookup computes one bin, loads one cache line, compares all keys branchlessly, and returns the matching entry. This may outperform a 1-PHF when the PHF metadata no longer fits cache. The paper’s result is fresh and architecture-dependent; label it experimental until reproduced.

### 4.6 Ribbon retrieval

A Ribbon retrieval function returns `r` bits for keys in a static set using space near `r*n` bits. It can map a stream ID to:

- a small partition ID;
- a compressed directory slot delta;
- a fingerprint plus candidate bucket.

For nonmembers, the returned value is arbitrary. The query must verify the full key in the selected candidate region. Ribbon is attractive when values are short and the exact keys are already stored in a compact sorted form.

### 4.7 Why the old FKS result does not settle this

The repo’s FKS implementation used a two-level layout with substantial empty-slot/value overhead and achieved roughly 80 bytes/key. PtrHash and k-PHF use different placement models and target a few bits/key of routing metadata. Bitmap/rank and Elias–Fano exploit ordered dense integers rather than hashing at all. The new tournament is justified, but only once.

## 5. Packed entry columns

The current 56-byte `DirEntry` can be decomposed into columns:

```text
first_version[n]      monotone-ish per stream order, not globally monotone
last_version[n]
ptr_end[n]            monotone
skip_end[n]           monotone
n_batches[n]
flags[n]
```

Pointers and skip offsets are naturally monotone and should store end offsets as Elias–Fano or Stream VByte deltas. `n_batches` is usually small and can use byte/varint control streams. First/last versions may use:

- fixed 64-bit for simplicity;
- frame-of-reference per partition;
- Stream VByte deltas from a partition base;
- bit packing selected by partition max width.

Separate columns permit a lookup to touch only the values it needs.

## 6. SIMD integer decoding

Stream VByte separates control and data streams and has demonstrated extremely high differential-integer decode throughput. It is a good match for pointer deltas and directory endpoints when:

- scans decode many consecutive entries;
- point lookup has a nearby skip/partition boundary;
- the format records an explicit codec ID and scalar fallback.

For point reads, small bit-packed fixed-width partition columns may be faster than decoding a variable prefix. The benchmark must include both one-point and scan paths.

## 7. Exact epoch dedupe design

### 7.1 Semantic model

Configuration:

```rust
struct DedupeWindow {
    span_positions: u64,
    epoch_positions: u64, // e.g. span/8, rounded
}
```

A key committed at position `p` is live at end `w` iff `p >= w-span`.

### 7.2 Canonical record

The capsule control prelude stores:

```text
scope kind
stream ID or global scope ID
key length
full key bytes
```

The full key is stored once. The resident index stores a keyed BLAKE3/AES-derived 128-bit fingerprint and capsule pointer.

### 7.3 Active table

Candidate structures:

- `hashbrown::RawTable` with 128-bit fingerprints and compact values;
- a SwissTable-style fixed layout specialized to 16-byte keys;
- IcebergHT-inspired low-associativity primary table plus rare overflow;
- quotient-filter variants only if they preserve all exact collision candidates.

The table never deletes individual rows during an epoch. That makes insertion simpler and stability more valuable than general resize/delete behavior.

Preallocate from the configured epoch size. If an epoch exceeds its estimate, seal it early or grow into a second table; do not pause the append owner for a giant rehash.

### 7.4 Frozen epoch

At close:

```text
sort (fingerprint, position, capsule_ptr)
build equal-fingerprint run offsets
build BinaryFuse16 or blocked APBF negative accelerator
optionally build k-PHF/Ribbon candidate routing
write into SegmentEffect/SealPack
```

For a sorted baseline, a filter-negative is a few nanoseconds and a filter-positive uses binary search over 128-bit fingerprints. Equal fingerprints preserve all positions.

### 7.5 Query exactness

Pseudo-code:

```rust
fn is_duplicate(scope: Scope, key: &[u8], w: u64) -> bool {
    let fp = keyed_fingerprint(scope, key);
    for candidate in active.equal_fingerprint(fp) {
        if candidate.position >= w - span && canonical_key(candidate.ptr) == key {
            return true;
        }
    }
    for epoch in frozen.iter().rev() {
        if epoch.max_position < w - span { break; }
        if !epoch.filter.maybe_contains(fp) { continue; }
        for candidate in epoch.equal_fingerprint(fp) {
            if candidate.position >= w - span && canonical_key(candidate.ptr) == key {
                return true;
            }
        }
    }
    false
}
```

Synthetic tests force identical fingerprints for distinct keys to prove no overwrite/false-negative bug exists.

### 7.6 Age-Partitioned Bloom relevance

APBFs were designed for sliding-window duplicate detection and report 2–3 cache-line accesses per insert and 2–4 per query in a blocked variant, with moderate slack. Asterism can borrow the age partitioning and blocked locality, but not its probabilistic answer as the final dedupe decision. It is a negative accelerator in front of the exact candidate arrays.

## 8. Name-to-ID static lookup

A store with millions of streams may spend more memory on string hash-table overhead than on heads. Use the same static-function strategy at checkpoint:

```text
canonical string table: compressed blocks of UTF-8 names
fingerprint[slot]: keyed 128-bit or full hash
static router: fingerprint -> candidate slot/bin
ID array: slot -> dense numeric ID
active overlay: names registered after checkpoint
```

Lookup hashes the input once, routes to a candidate, and compares the full canonical name bytes. Alias resolution may return a small candidate list.

For category-prefixed stream names, front-code or dictionary-compress repeated prefixes in the canonical string table. This affects memory/storage only; exact comparison reconstructs the canonical bytes.

## 9. Representation selection policy

Seal/checkpoint chooses a representation with a deterministic policy:

```text
if U <= bitmap_threshold * n:
    bitmap+rank
else if n < small_sorted_threshold:
    sorted array
else:
    benchmark-selected PEF or static hash/retrieval variant
```

The policy version is stored. A new binary must continue to decode old choices. Construction failure always falls back to sorted/PEF; no segment seal may fail merely because a perfect-hash builder did not converge.

## 10. Benchmark dimensions

Every candidate is measured on:

```text
positive hit, uniform
negative miss
Zipf-hot positive
streaming batches of 8/32/128 lookups
cold-cache and warm-cache
1k, 10k, 100k, 1M, 30M keys
actual segment ID distributions
construction p50/p95/p99
serialized bytes
resident bytes after open
open/parse time
branch misses, LLC misses, instructions
```

The winner may differ by size. A representation chooser is worthwhile only if its complexity produces clear regions of dominance.

## 11. Primary references

- [PtrHash](https://arxiv.org/abs/2502.15539)
- [Non-minimal k-perfect hashing](https://arxiv.org/abs/2607.07257)
- [Ribbon retrieval](https://arxiv.org/abs/2109.01892)
- [Elias–Fano dynamic ordered sets and the EF space bound](https://arxiv.org/abs/2003.11835)
- [Stream VByte](https://arxiv.org/abs/1709.08990)
- [Age-Partitioned Bloom Filters](https://arxiv.org/abs/2001.03147)
- [IcebergHT](https://arxiv.org/abs/2210.04068)
- [Mess D10 experiments](https://github.com/bobisme/mess/blob/43e4aca0192f01bb47670627f41182bca182759e/docs/perf/experiments-d10.md)
