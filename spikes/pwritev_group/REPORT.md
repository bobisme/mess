# bn-1zv6 outcome report

Verdict: **`INCONCLUSIVE_FATAL` — no performance verdict**

The ordered-`pwritev` decision experiment did not reach selection or
confirmation. Production storage-engine code was not changed, and neither
attempt's timing rows are admissible for an adoption or decline decision.

## Canonical Attempt 2

Attempt 2 ran once from exact clean source
`54215100a825da9eb7cfbd8f045e54ef05fb29bd` with release binary SHA-256
`12b1dafe765a77d1a17115b0ae12cff973f20ed7d6d770fbd790695fe081288e`.
It used the fresh external output directory
`/home/bob/.cache/mess-bench/results/bn-1zv6-attempt2-5421510-20260715T183800Z`.
An immutable copy and its hashes are committed under
`evidence/attempt2-inconclusive-fatal/`.

Before the fatal transition, the runner durably recorded:

- 136/136 prepared-pipeline rows, all `accepted`;
- 952/952 exploratory rows, all `accepted`;
- 440/440 boundary rows, all `accepted`;
- zero `rejected_noisy`, failed-row, replacement, or retry observations.

The frozen quiet-load guard paused before rows during two host-load spikes and
resumed without creating a row. After all 1,528 Stage-1 rows completed, the
runner attempted to execute `spikes/pwritev_group/evaluate.py` directly. The
file mode was `0644`, so the operating system returned:

```text
[Errno 13] Permission denied: '.../spikes/pwritev_group/evaluate.py'
```

The runner emitted `failure.json` with classification `FATAL` at
`2026-07-15T19:07:27.663334+00:00` and exited 3. It produced no
`selection.json`, confirmation manifest/rows, or `evaluation.json`.
Per the frozen one-attempt rule, the evaluator was not invoked manually, the
run was not resumed or restarted, and no third attempt is permitted.

## Attempt 1

Attempt 1 remains separately classified `INCONCLUSIVE_INFRASTRUCTURE`. It
fail-stopped after a post-row load violation at boundary row 11, before
selection. Its untouched evidence is under
`evidence/attempt1-inconclusive-infrastructure/`. Attempt 2 reused none of its
timing rows.

## Root cause and process lesson

The evaluator itself had passing static self-tests, but the preflight invoked
it as `python3 evaluate.py self-test`; it did not exercise the runner's exact
stage-transition command, which launches the path directly. Source audits
validated decision-artifact and fail-closed semantics but likewise did not
test executable permission at that call boundary.

Future benchmark protocols must smoke-test every exact subprocess transition
before timed row zero, including executable mode, argv, interpreter choice,
artifact readback, and exit/verdict consistency. A Python helper should either
be deliberately executable and permission-checked or be launched through the
captured Python interpreter. That smoke test must run through the same runner
code path used after timed phases, without producing or consuming timing
evidence.

The 1,528 clean raw rows may be retained for incident analysis, but they must
not be manually selected, summarized as a performance result, or used to
authorize storage-engine integration.

