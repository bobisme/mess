# 10 — Bibliography and source map

## Existing Mess repository observations

The old repository already has the right seed:

- Workspace split into `mess_db`, `mess_ecs`, and an example app.
- `mess_db` models global and stream positions.
- Writes include expected stream position for optimistic concurrency.
- RocksDB backend uses global and stream column families.
- Current RocksDB records duplicate payload bytes across global and stream records.
- `mess_ecs` contains early aggregate/projection-like traits, though the ECS terminology is misleading.

Repository: https://github.com/bobisme/mess

## Storage engines and KV design

- RocksDB transactions and `WriteBatch` atomicity: https://github.com/facebook/rocksdb/wiki/Transactions
- RocksDB column families: https://github.com/facebook/rocksdb/wiki/Column-Families
- redb docs: https://docs.rs/redb/latest/redb/
- Fjall docs: https://docs.rs/fjall/latest/fjall/
- WiscKey: Separating Keys from Values in SSD-conscious Storage: https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf
- FASTER: A Concurrent Key-Value Store with In-Place Updates: https://www.microsoft.com/en-us/research/publication/faster-a-concurrent-key-value-store-with-in-place-updates/
- CompassDB: Pioneering High-Performance Key-Value Store with Perfect Hash: https://arxiv.org/abs/2406.18099
- Cascade Log: Reference-Stable Windowing over Tiered Append Sequences: https://arxiv.org/abs/2606.05467
- FlintKV: A Fast Durable Storage Engine for Modern Databases: https://arxiv.org/abs/2607.02401

## Flash, filesystems, and async I/O

- Linux F2FS documentation: https://docs.kernel.org/filesystems/f2fs.html
- Zoned Namespace SSDs introduction: https://zonedstorage.io/docs/introduction/zns
- io_uring paper: https://kernel.dk/io_uring.pdf
- High-Performance DBMSs with io_uring: When and How to use it: https://arxiv.org/abs/2512.04859
- Performance Characterization of NVMe Flash Devices with Zoned Namespaces: https://arxiv.org/abs/2310.19094
- NATS JetStream persistence/durability docs: https://docs.nats.io/nats-concepts/jetstream

## Probabilistic filters

- Cuckoo Filter: Practically Better Than Bloom: https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf
- Xor Filters: Faster and Smaller Than Bloom and Cuckoo Filters: https://arxiv.org/abs/1912.08258
- Ribbon Filter: Practically Smaller Than Bloom and Xor: https://arxiv.org/abs/2103.02515
- Binary Fuse Filters: Fast and Smaller Than Xor Filters: https://arxiv.org/abs/2201.01174
- ZOR filters: https://arxiv.org/abs/2601.04843

## Learned and compact indexes

- The Case for Learned Index Structures: https://arxiv.org/abs/1712.01208
- PGM-index: https://arxiv.org/abs/1910.06169
- RadixSpline: A Single-Pass Learned Index: https://arxiv.org/abs/2004.14541
- RadixStringSpline: Learned String Indexes: https://arxiv.org/abs/2104.11346

## Algebra, distributed semantics, and projection theory

- CRDT overview: https://arxiv.org/abs/1805.06358
- Coordination Avoidance in Database Systems / invariant confluence: https://arxiv.org/abs/1402.2237
- Keeping CALM: When Distributed Consistency is Easy: https://arxiv.org/abs/1901.01930
- Tree Clock Data Structure for Causal Orderings: https://arxiv.org/abs/2201.06325
- DBSP: Automatic Incremental View Maintenance for Rich Query Languages: https://arxiv.org/abs/2203.16684
- Kurrent/EventStore projections docs: https://docs.kurrent.io/server/v25.0/features/projections/

## Main design conclusions from the sources

1. LSM engines are good generic write engines, but immutable event payloads should not be repeatedly compacted or duplicated.
2. Key/value separation from WiscKey maps naturally to event-payload/log and pointer-index separation.
3. redb and Fjall are the serious pure-Rust embedded index candidates; RocksDB is the pragmatic baseline.
4. SSD/ZNS/F2FS literature all reward sequential append, hot/cold separation, and careful cleaning/retention.
5. io_uring is promising but should be hidden behind an I/O backend after correctness exists.
6. Static filters and learned/perfect indexes fit sealed immutable segments better than active hot writes.
7. Snapshots are algebraic fold checkpoints; adding fold hashes makes them verifiable.
8. CRDT/CALM/invariant-confluence theory can guide future relaxed/distributed stream modes instead of guessing.

