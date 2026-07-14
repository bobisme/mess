# Research 07: literature map and maturity assessment

**Cutoff:** searched through 2026-07-11.  
**Policy:** primary papers, official documentation, and source repositories are preferred. Fresh 2026 preprints are explicitly labeled and never treated as production proof.

## 1. Fjall and the correct baseline

### Fjall

- **Source:** [Fjall repository](https://github.com/fjall-rs/fjall), [docs.rs](https://docs.rs/fjall/3.1.6/fjall/)
- **Maturity:** active production-grade Rust project; current inspected Mess pin is 3.1.6.
- **Relevant ideas:** safe-Rust LSM, keyspaces, atomic batches, journal/memtable absorption, range/prefix reads, block tables, filters, compaction, optional KV separation.
- **What Asterism borrows:** none of the physical LSM machinery; Fjall remains
  the measured control implementation and a short-term operational rollback
  option while the replacement is adopted. There is no legacy-store migration
  or migration-fallback program; canonical replay is the correctness fallback.
- **Why specialization can win:** Mess needs dense latest-value tables, an append-only registry, and a bounded recent set—not arbitrary ordered mutation/ranges. Asterism removes journal/memtable/SST/compaction work rather than reimplementing it.
- **Caution:** any claim to “beat Fjall” must use the actual Mess operation mix, not a cherry-picked direct-array microbenchmark.

### SplinterDB / Bε-tree lineage

- **Source:** [SplinterDB, USENIX ATC 2020](https://www.usenix.org/conference/atc20/presentation/conway)
- **Maturity:** peer-reviewed systems work and open implementation lineage.
- **Result claimed by authors:** 6–10× insertion and 2–2.6× point-query throughput over RocksDB in evaluated configurations, with lower write amplification.
- **Relevant idea:** buffered tree nodes turn many small updates into large sequential propagation while retaining point/range semantics.
- **Asterism stance:** if Mess still needed a general ordered mutable index, an STBε/Bε design would be more interesting than a hand-built B-tree. It does not beat deleting the general index from the canonical write path.

### WiscKey / key-value separation

- **Source:** [WiscKey, FAST 2016](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf)
- **Maturity:** peer-reviewed, foundational.
- **Relevant idea:** keep values out of LSM compaction.
- **Asterism extension:** event payloads are already in the canonical log; Asterism also removes most keys from the LSM and derives current state directly from the same log.

## 2. Flat combining, ownership, and hot/cold state

### FlintKV

- **Source:** [FlintKV, arXiv:2607.02401](https://arxiv.org/abs/2607.02401)
- **Date/maturity:** submitted 2026-07-02; fresh preprint, NVM-specific; claims require independent reproduction.
- **Relevant idea:** flat-combining concurrency control co-designed with durable multi-version state, atomic batches, snapshots, and iteration.
- **Asterism use:** evidence that centralized combination can be a performance technique rather than merely a serialization bottleneck. Asterism uses one owner for SSD log grouping and workload-specific state; it does **not** adopt FlintKV’s NVM skiplist or assume its reported 75% improvement transfers.

### FASTER and F2

- **Sources:** [FASTER](https://www.microsoft.com/en-us/research/publication/faster-a-concurrent-key-value-store-with-in-place-updates/), [F2](https://arxiv.org/abs/2305.01516)
- **Maturity:** substantial systems research; FASTER has broad implementation experience.
- **Relevant ideas:** hot/cold separation, hybrid logs, cache-optimized indexes, record-oriented tiering under skew.
- **Asterism use:** resident direct heads and hot active capsules; immutable cold segments and packs; bounded caches. Events remain immutable—no in-place event update.

### IcebergHT / Iceberg hashing

- **Source:** [IcebergHT](https://arxiv.org/abs/2210.04068)
- **Maturity:** research hash-table design, oriented toward persistent memory.
- **Relevant ideas:** stability, low associativity, predictable few-cache-line access, rare overflow.
- **Asterism use:** candidate active dedupe-table shape because entries never individually delete within an epoch. NVM persistence protocol is not adopted.

### VIP hashing

- **Source:** [VIP Hashing](https://arxiv.org/abs/2206.12380)
- **Maturity:** research prototype.
- **Relevant idea:** adapt layout to skewed popularity.
- **Asterism use:** optional cache/registry placement research; not a correctness structure. A simpler TinyLFU/hot overlay may dominate operationally.

## 3. Succinct static dictionaries and functions

### PtrHash

- **Source:** [PtrHash: Minimal Perfect Hashing at RAM Throughput](https://arxiv.org/abs/2502.15539)
- **Date/maturity:** SEA 2025; paper and Rust software.
- **Author results:** default 2.4 bits/key; 12 ns scalar and 8 ns streaming integer lookups at one-billion-key scale on evaluated hardware; faster construction/query than compared MPHFs.
- **Relevant idea:** sacrifice the last fraction of a bit for memory-bandwidth-level query throughput, using fixed pilots and CacheLineEF remapping.
- **Asterism use:** optional sealed stream directory or registry fingerprint router. Exact key verification is mandatory for nonmembers.
- **Caution:** the existing Mess FKS spike showed a “perfect hash” can be slower and larger than `hashbrown`. PtrHash must be measured in the exact Rust/open/serialization path.

### Non-minimal k-perfect hashing

- **Source:** [Non-minimal k-perfect hashing](https://arxiv.org/abs/2607.07257)
- **Date/maturity:** submitted 2026-07-08; accepted to ESA 2026 but only three days old at this review.
- **Author results:** cache-line-sized bins; up to 1.5× static-set speedup for large sets on two of three tested architectures.
- **Relevant idea:** route a key to one high-load cache-line bin rather than one exact slot, allowing tiny routing metadata and exact comparisons within the bin.
- **Asterism use:** highly experimental large sealed directories and dedupe epochs. The simple packed-bin shape is attractive because Mess must verify exact keys anyway.

### Ribbon retrieval

- **Source:** [Fast Succinct Retrieval and Approximate Membership using Ribbon](https://arxiv.org/abs/2109.01892)
- **Maturity:** established research with implementations in major storage ecosystems.
- **Author result:** practical retrieval structures with well below 1% space overhead above `r*n` bits in evaluated settings.
- **Relevant idea:** static function `f:S -> r-bit values` near the information-theoretic lower bound.
- **Asterism use:** map immutable keys/fingerprints to candidate partitions or compact values; pair with canonical exact verification. Never use arbitrary nonmember output as an exact result.

### Elias–Fano

- **Source:** [Succinct Dynamic Ordered Sets with Random Access](https://arxiv.org/abs/2003.11835) and classical Elias–Fano literature.
- **Maturity:** decades-old, well-understood succinct structure.
- **Space bound used:** `n ceil(log2(U/n)) + 2n` bits for `n` ordered integers in universe `U`, before lower-order metadata.
- **Asterism use:** sparse sorted stream IDs, monotone offsets/endpoints, checkpoint page IDs, and partition directories. It is lower risk than modern perfect hashing.

### Stream VByte / SIMD BP128

- **Sources:** [Stream VByte](https://arxiv.org/abs/1709.08990), [SIMD compression and intersection](https://arxiv.org/abs/1401.6399)
- **Maturity:** mature integer-codec research with production descendants.
- **Author result:** Stream VByte exceeded four billion differentially coded integers/s from RAM to L1 on the evaluated Haswell system.
- **Asterism use:** pointer and monotone-directory deltas. Point lookup still requires partition/skip boundaries; never decode from the beginning of a giant list.

### PGM / learned indexes

- **Source:** [PGM-index](https://arxiv.org/abs/1910.06169)
- **Maturity:** strong research and implementations.
- **Relevant idea:** bounded-error piecewise-linear position model.
- **Asterism stance:** dense global positions do not need learning. Stream IDs are better served by bitmap/rank, Elias–Fano, or exact static routing. Learned string indexes remain a benchmark curiosity for enormous registry name sets, not a default.

## 4. Sliding-window membership and dedupe

### Age-Partitioned Bloom Filters

- **Source:** [APBF](https://arxiv.org/abs/2001.03147)
- **Maturity:** research design, specifically matched to sliding-window duplicate detection.
- **Author claim:** blocked variant with roughly 2–3 cache-line accesses per insertion and 2–4 per query, trading moderate slack for compactness.
- **Asterism use:** age partitioning and a negative filter in front of exact fingerprint/canonical-key candidates.
- **Not adopted:** probabilistic “duplicate” as a final answer. Exact batch
  idempotency is an optional product capability under `bn-2ctq` and ADR 0002,
  not a current public Mess contract. If admitted, its configured window must
  use canonical-key verification and remain exact.

### Cuckoo, quotient, BinaryFuse, Ribbon filters

- **Sources:** [Cuckoo filter](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf), [Binary Fuse](https://arxiv.org/abs/2201.01174), [Ribbon](https://arxiv.org/abs/2103.02515)
- **Maturity:** Cuckoo and static XOR/Fuse family widely studied; Mess already measured BinaryFuse16 successfully.
- **Asterism use:** negative accelerators. The current BinaryFuse result remains the default until another structure wins the exact epoch workload.

## 5. SSD placement and write amplification

### How to Write to SSDs

- **Source:** [How to Write to SSDs, arXiv:2603.09927](https://arxiv.org/abs/2603.09927)
- **Date/maturity:** accepted to PVLDB 2026.
- **Author results:** out-of-place LeanStore redesign improved evaluated YCSB-A throughput 1.65–2.24× and reduced flash writes 6.2–9.8×; also supports ZNS/FDP.
- **Relevant ideas:** out-of-place page writing, packing, death-time/lifetime grouping, and host/device co-design.
- **Asterism use:** immutable event/SealPack/checkpoint-page objects and explicit lifetime classes. The reported factors are not Asterism projections.

### ZNS and FDP

- **Sources:** [Zoned Storage introduction](https://zonedstorage.io/docs/introduction/zns), NVMe specifications and device documentation.
- **Maturity:** deployed but hardware/OS ecosystem dependent.
- **Asterism use:** optional mapping of segments/packs to sequential zones and lifetime classes to FDP placement handles. File-based capsule semantics remain portable truth.

### F2FS

- **Source:** [Linux F2FS documentation](https://docs.kernel.org/filesystems/f2fs.html)
- **Maturity:** production filesystem.
- **Relevant ideas:** hot/warm/cold logs and flash-aware placement.
- **Asterism use:** reinforces separating short-lived dedupe/temp objects from long-lived event segments.

## 6. Concurrency publication

### Linux sequence counters / seqlocks

- **Source:** [Linux sequence counters and sequential locks](https://docs.kernel.org/locking/seqlock.html)
- **Maturity:** production kernel primitive with strict applicability constraints.
- **Relevant idea:** readers retry when a writer changes a coherent multi-field state.
- **Asterism use:** page-level head consistency only with atomic fields and a bounded slow path.
- **Caution:** sequence counters cannot protect pointer lifetimes or make Rust data races legal. Page pointers require RCU/Arc lifetime management; fields read concurrently remain atomic.

### RCU / epoch reclamation

- **Sources:** Linux RCU literature and Rust crossbeam-style epochs.
- **Asterism use:** rare page-directory growth and active-generation slab retirement. Do not use per-event RCU allocation.

## 7. Compression and columns

### Current Mess columnar result

- **Source:** [Mess performance envelope](https://github.com/bobisme/mess/blob/43e4aca0192f01bb47670627f41182bca182759e/docs/perf/envelope.md)
- **Maturity:** code and measured regression gate in the repository.
- **Relevant result:** the current `.pcol` implementation has already surpassed its size/replay gates on the reference corpus.
- **Asterism use:** retain and place inside SealPack. Do not restart the compression search unless a new corpus shows a problem.

### DBMS columnar encodings

- **Relevant families:** frame-of-reference, delta/bit packing, dictionary, run-length, SIMD codecs.
- **Asterism use:** metadata/pointer columns in addition to payload columns. Each section declares codec/version and has scalar fallback.

## 8. Algebra and incremental computation

### Monoids, actions, semilattices

- **Source:** mathematical foundations already developed in the Mess research pack.
- **Maturity:** standard mathematics.
- **Asterism use:** event histories as a free monoid; aggregate folds as monoid actions; right-biased partial-map effects; frontier joins; conflict-detecting registry union.

### DBSP / differential dataflow lineage

- **Source:** [DBSP](https://arxiv.org/abs/2203.16684)
- **Maturity:** active systems/research lineage.
- **Relevant idea:** incremental view maintenance as algebra over changes.
- **Asterism use:** intellectual support for representing state changes as composable effects. Asterism does not require a general DBSP engine for storage metadata.

## 9. Ideas explicitly rejected or demoted

| idea | status | reason |
|---|---|---|
| build a general custom B-tree/LSM first | rejected | retains the wrong abstraction and huge correctness surface |
| naive FKS perfect hash | rejected by existing Mess measurement | slower/larger than `hashbrown` in tested form |
| learned index for global position | rejected | positions are dense/direct-addressable |
| mandatory crypto for recovery | rejected | CRC is faster and already load-bearing; crypto chain remains opt-in |
| mmap everything | rejected default | SIGBUS/external mutation and weak backpressure story |
| striped durable logs on one device | rejected v1 | existing spike found global-order latency coupling despite parallel flush capacity |
| per-event compression in active log | rejected default | prior spikes found poor ratio without dictionaries; sealing is the right transform point |
| probabilistic dedupe decision | rejected | exact semantics require full-key verification |
| one file per snapshot | targeted for replacement | metadata/file-count overhead and poor packing |
| static structure in active hot path | rejected | construction churn; use append-only mutable forms until seal |

## 10. Research-confidence tiers

### Tier A — use unless implementation disproves

- one canonical log authority;
- single-owner group commit;
- dense direct tables for dense IDs;
- append-only active microblocks;
- right-biased SegmentEffects;
- content-addressed discardable checkpoints;
- bitmap/rank and Elias–Fano;
- exact canonical-key verification;
- out-of-place immutable packs;
- explicit lifetime classes.

### Tier B — benchmark tournament

- page seqlock vs double-copy publication;
- specialized active dedupe hash layout;
- Stream VByte vs fixed bit packing for pointer columns;
- consolidated SealPack performance;
- snapshot packs;
- TinyLFU/adaptive cache admission.

### Tier C — fresh/experimental

- cache-line k-perfect hashing from July 2026;
- PtrHash in the exact Mess directory path;
- Ribbon retrieval for values rather than filters;
- FDP placement handles;
- ZNS backend;
- learned string index for registry names.

Tier C mechanisms must have simple fallbacks and cannot be prerequisites for v1 correctness.
