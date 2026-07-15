# Publication v2 candidate-only owner diagnosis

This is a diagnostic run, not the paired admission gate. The temporary
checkpoint `1da7b53c4ff74df6cb9a28ce23b1389aef234060` added an env-gated sidecar
to the otherwise unchanged `owned_append_bench` paired CSV path. Every
additive `OwnerDiagnostics` field is a measured-interval delta. The sampled
maximum group width is a cumulative gauge, so the sidecar records its
before/after values rather than subtracting it.

Each cell has ten independent candidate observations. The deterministic
one-in-64 sampler yielded 6,941 Process/b1, 1,011 Process/b100, and exactly
10 Group/b1000 commit-phase samples.

| cell | rows | throughput mean (range), events/s | p99 mean (range), us | actual groups | sampled gathers | sampled width mean / max | sampled close reasons | sampled commit groups |
|---|---:|---:|---:|---:|---:|---:|---|---:|
| Process/b1 | 10 | 150,464 (146,728..153,866) | 47.5 (43.9..51.6) | n/a | 6,941 | 3.601 / 4 | deadline 6,941 | 6,941 |
| Process/b100 | 10 | 3,418,549 (3,304,074..3,537,415) | 230.0 (215.6..253.0) | n/a | 1,011 | 1.550 / 4 | deadline 1,011 | 1,011 |
| Group/b1000 | 10 | 937,864 (909,468..957,617) | 7,351.8 (6,986.5..7,865.5) | 1,024 | 10 | 3.900 / 4 | inflight-drained 9; deadline 1 | 10 |

Mean sampled phase duration:

| cell | direct commit, us | direct return to PUBLIC frontier, us | frontier to first warm/completion, us | first to last warm/completion, us |
|---|---:|---:|---:|---:|
| Process/b1 | 5.763 | 0.877 | 0.537 | 2.742 |
| Process/b100 | 26.753 | 0.658 | 1.913 | 1.659 |
| Group/b1000 | 3,293.276 | 1.692 | 0.944 | 4.782 |

For Group/b1000, the direct commit phase is approximately 99.8% of the four
measured phase totals. The canonical publication cut itself averages 1.692 us,
and all post-cut warming/completion averages 5.726 us. Nine of ten sampled
gathers close because the four admitted producers are drained; none close on
capacity or bytes. This establishes only that Group/b1000 is dominated by
durable direct commit/fsync below the cut. Process/b1 still spends about
3.279 us from the frontier through the last warm/completion versus 5.763 us in
direct commit; candidate-only phase timing cannot decide whether that is the
next standalone target. The paired admission gate is required for that call.

Raw workload rows are in `candidate.csv`; interval diagnostics are in
`owner_diagnostics.csv`; stdout is in `run.log`; machine and command context
are in `provenance.txt`; hashes are in `SHA256SUMS`.
