# Attempt 1: inconclusive infrastructure evidence

Classification: `INCONCLUSIVE_INFRASTRUCTURE`

This directory is an immutable copy of the first bn-1zv6 experiment attempt.
The attempt ran from source commit
`015d813736625640f12d26c5d349b92c14fd2fb4` and stopped after boundary row 11
failed the post-row quiet-load guard. No selector ran and no confirmation row
ran. The evidence contains 136 prepared-pipeline rows, 952 exploratory rows,
11 boundary rows, the complete predeclared manifests, provenance, correctness,
plan, candidate rules, and raw run log.

These timing rows are retained only for auditability. They must never be used
by Attempt 2 selection, confirmation, or final evaluation. Attempt 2 starts at
row zero in a new output directory under the prospective protocol frozen in
`../../EXPERIMENT.md` at commit
`a410a40ca87687f0b68fbc03e994c951857fcf74`.

`SHA256SUMS` records hashes calculated before the copy and verified again after
the copy. CSV line counts include one header:

- `prepared_pipeline.csv`: 137 lines (136 rows)
- `exploratory.csv`: 953 lines (952 rows)
- `boundaries.csv`: 12 lines (11 rows)
- `exploratory_manifest.csv`: 953 lines (952 planned rows)
- `boundary_manifest.csv`: 441 lines (440 planned rows)

