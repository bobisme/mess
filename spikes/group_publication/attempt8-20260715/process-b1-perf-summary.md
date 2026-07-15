# Process/b1 post-rejection `perf stat` diagnostic

This is a diagnostic, not admission evidence. It ran after the locked Attempt
8 decision against the same frozen A/B binaries, with three counterbalanced
cycles (`A,B,B,A`; `B,A,A,B`; `A,B,B,A`), load1 below 6 before every
observation, and no compiler or other benchmark process. `perf stat` itself
lowers absolute throughput, so only within-cycle B/A ratios are interpreted.

| metric | median B/A | cycle ratios |
| --- | ---: | --- |
| throughput | 0.961630 | 0.9568, 0.9616, 0.9733 |
| append p50 | 1.048023 | 1.0454, 1.0510, 1.0480 |
| append p99 | 1.001624 | 1.0016, 1.0273, 0.9564 |
| task clock | 0.992388 | 0.9924, 0.9971, 0.9908 |
| user cycles | 0.987339 | 0.9844, 1.0030, 0.9873 |
| user instructions | 0.991721 | 0.9889, 0.9917, 0.9917 |
| cache references | 0.954104 | 0.9637, 0.9541, 0.9481 |
| cache misses | 1.016648 | 1.0386, 1.0166, 1.0063 |
| branches | 0.986960 | 0.9899, 0.9870, 0.9825 |
| branch misses | 0.996692 | 1.0101, 0.9967, 0.9588 |

The candidate again loses wall throughput (3.8%) and p50 latency (4.8%), but
does not execute more user instructions or cycles for the fixed event count.
That falsifies “three metric RMWs or scratch instructions dominate” as the
primary explanation. The stronger hypothesis is lost pipeline utilization:
the candidate warms/stages all four writers' plans and publishes the whole
cohort before waking any producer, while the control wakes an early producer
as it continues retiring later plans. The next experiment must phase-time the
post-I/O path and move correctness-transparent cache work out of the interval
before the first completion without reintroducing multiple visibility cuts.
