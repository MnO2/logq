# Execution, reuse and capacity milestones

The user authorized planning and execution of these directions on 2026-09-06,
following the recommendation to prioritize nested projection and batch output.
Baseline: `b27ab090b1b8d2d0453134a358035fb06f221641`. Existing untracked
`docs/performance-audit-2026-09-05.md` is preserved as user work.

This work spans more than eight files. Each milestone must remain independently
usable and reversible through code changes alone. Experiments culminate in an
explicit adoption or rejection decision; measured kernels do not automatically
become production features. No external account, credential or service is
required. The existing instruction to push master remains applicable after
verification; no release, publication or version bump is planned.

## M1: nested JSON projection

- [x] Add semantic regressions before implementing a required-path tree.
- [x] Retain only requested object descendants across plain, gzip, sharded,
  row and batch readers. Whole-root uses override narrower requirements;
  unsupported wildcard/array/scoped shapes retain the existing safe fallback.
- [x] Preserve full consumed-line validation, duplicate-key replacement,
  missing/null/type drift and demand-driven LIMIT.
- [x] Compare nested payload widths with direct-root and narrow controls;
  retain only a repeatable full-query/allocation improvement.

## M2: batch-native JSON and NDJSON output

- [x] Test byte equivalence against the row serializer before adding borrowed
  column serialization and a shared row/batch execution result boundary.
- [x] Cover active rows, duplicate aliases, key order, mixed values, float
  spelling, final flush errors and lazy execution. Retain table/CSV behavior.
- [x] Measure full-result projection and high-cardinality grouping, including
  small-result controls and separately recorded output size.

## M3: typed arithmetic

- [x] Extend only pure Float32 arithmetic supported by the current type and
  function-binding contracts, with scalar differential tests first.
- [x] Compare computed aggregation and projection on narrow and wide input;
  preserve per-operation rounding, masks and error/call order.
- [x] Keep integer overflow-aware expressions on their safe evaluation path.

## M4: query lifecycle and prepared reuse

- [x] Implement a reproducible in-process fresh-plan/reused-plan experiment
  that reopens inputs and creates fresh execution state on every run.
- [x] Check complete results and changed-file visibility; measure actual
  1/10/100-query sequences with planning and execution reported separately.
- [x] Assess native session adoption from same-engine evidence. Reassess
  prepared Parquet against the existing amortization/representation controls
  and Arrow's standard reader; document any remaining adoption blockers.
  No transparent cache or automatic source conversion.

## M5: larger-than-memory sorting

- [x] Reproduce full-sort budget failures and distinguish retained estimates
  from RSS and output-buffer costs.
- [x] Build a bounded external-sort experiment with sorted runs and bounded
  merge fan-in, using temporary files and explicit disk accounting.
- [x] Verify full ordering, stable ties, value representation, errors and
  cleanup; compare in-memory and disk-backed behavior at constrained budgets.
- [x] Decide production scope from semantic coverage and cost. Aggregate,
  distinct and join spilling require separate evidence and are excluded from
  this first external-sort implementation.

## M6: semantics, numerical contract and executable documentation

- [x] Pin numeric boundary behavior and specify an Int64/Float64 migration
  contract covering readers, casts, arithmetic, keys, aggregates and output.
  CLAUDE currently requires Int32/Float32: no precision change is hidden in
  M1–M5. Any wider-runtime adoption must explicitly revise that contract.
- [x] Reproduce and fix supported aggregate expressions in HAVING; verify
  stale conformance skips against current code before updating them.
- [x] Add generated cross-path result comparisons and executable capability
  documentation; correct remaining verified documentation drift.
- [x] Document measured adoption decisions and diagnostic phase boundaries.
  A full user-facing per-operator EXPLAIN ANALYZE is only promoted if its
  accounting is reliable for overlapping parallel work.

## Verification and measurement

Write meaningful tests first; run `cargo test` before each meaningful commit.
Root serializes integration, timing and commits; agents edit disjoint areas.
Performance runs use immutable input, exact baseline/candidate binary hashes,
complete answer checks, alternating paired samples and 1/auto thread controls.
Report wall time and output bytes, with allocation/RSS measurements separately.
Warm synthetic measurements are labeled as such; no cold-I/O or production
claim follows from them. Promote gains exceeding noise with no unexplained
control regression; otherwise retain the experiment and reject the default.

Final checks: `cargo test --all-features --locked`,
`cargo fmt --all -- --check`,
`cargo clippy --all-targets --all-features --locked -- -D warnings`,
`cargo check --all-features --locked`,
`cargo +1.85.0 check --all-targets --all-features --locked`, and
`python3 -m unittest discover -s scripts/bench_e2e -p 'test_*.py'`.
Review filesystem/input boundaries, architecture and adversarial compositions
before the master push. Raw measurements remain in uniquely named experiment
directories; durable docs contain reproducible commands and bounded conclusions.

## Completion and decisions

All six milestones were executed and reviewed. M1–M3 and the M6 correctness
repairs are enabled. M4 and M5 remain reproducible diagnostic experiments after
measuring reuse benefit and fixed-schema sorting capacity; a production session
API, native Parquet reader and general spilling are not adopted. Runtime widening
and per-operator timing remain gated by their explicit compatibility/accounting
contracts. See the [measured report](../performance-execution-2026-09-06.md) for
final binary identity, full-query controls, experiment results and next gates.
