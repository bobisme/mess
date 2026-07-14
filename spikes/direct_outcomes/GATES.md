# bn-21ew measurement contract

The frozen external reference is `../baseline_matrix/BN-2SU-FINAL.csv`.
This contract was written before the production change.

- Correctness is absolute: bytes, ordered-unit outcomes, roll behavior,
  barrier poison, watermark advancement and barrier counts may not change.
- Direct micro rows must remove `Ack`/`Arc`/`Mutex` from the owner path. Every
  batch-size row must save at least one allocation and 64 allocated bytes per
  append. Median owner-thread CPU and wall time must remain within 105% of the
  paired pre-change row.
- Production Process throughput must remain at least 95% of both the paired
  pre-change row and the matching 24-byte/4-writer BN-2SU row. Group must
  remain at least 90% of both. p99 may increase by at most 10%; barriers may
  not increase.
- Os has no BN-2SU reference: it must retain at least 90% of paired throughput,
  p99 may increase by at most 10%, and it must remain sync-per-batch.
- Reusable direct scratch retains at most 256 batch request/effect/result
  slots, 4096 subframes and 1 MiB aggregate. Oversize calls may allocate
  transiently but must shed excess before returning; payloads are never kept
  in scratch.
