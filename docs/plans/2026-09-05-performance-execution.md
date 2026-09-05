# Performance execution plan

Baseline: `8baa7fa`, logq 0.2.0. The user authorized planning and implementing the
performance audit on 2026-09-05. Preserve the audit as historical evidence;
implementation results belong here and in the benchmark documentation.

The scope is the raw-file query engine and its measurement harness. No new
storage service, persisted schema migration, credentials, or external publication
is required. Changes are reversible code commits. Existing PartiQL NULL/MISSING,
JSON validation, output ordering and public numeric representation are constraints.

## Work and acceptance

- [x] Correctness foundation: COUNT(column) excludes NULL/MISSING, COUNT aliases
  survive scan pushdown, grouped output advertises its schema before sorting,
  grouping keys preserve validity/equality, row/batch accumulators agree on
  numeric precision. Add failing regressions before each fix.
- [x] Trustworthy measurements: errors go to stderr and exit nonzero; benchmarks
  unwrap results, validate answers, parse all SQL and avoid timing LIMIT cleanup.
  Record revision, data/query hashes, actual settings; fix runner output paths.
- [x] Predicates: reuse regex search caches, bind constants without per-row
  allocation where feasible, retain dictionary matching under selection masks.
  Compare literal/wildcard/Unicode/newline/NULL behavior and rerun LIKE timings.
- [x] JSON input: direct strict deserialization into logq Values, then preserve
  only required root fields. Validate ignored values, duplicate keys, numeric
  extremes, nesting and trailing input. Support files, shards and gzip with the
  same reader. Full/wildcard projections retain complete records.
- [x] Planner: distinguish aggregate cardinality from SELECT-star output;
  preserve dependencies through time buckets and aliases; make EXPLAIN and
  execution agree, including thread and memory options. Verify redundant WHERE
  does not cause a fast-path cliff and composed operators return identical data.
- [x] Bounded parallel execution: shared mmap ranges, controlled workers,
  bounded queues, stable input order, cancellation/error propagation; partial
  associative aggregates and final merging avoid collecting full input batches.
  Normalize auto threads only after buffering is bounded. Measure 1/2/4/8 workers
  and memory growth with fixed-cardinality inputs.
- [x] Batch coverage and memory: query-local JSON batches/column slots for
  selected query shapes, conservative fallback for unsupported dynamic shapes;
  account retained batch/operator state in the shared query budget. A generous
  ceiling must preserve fast execution; a small ceiling must stop cleanly.
- [x] Remaining allocation hotspots: measured group key allocation, tokenizer
  offsets, repeated timestamp work and early TopK payload materialization.
  Apply changes only with meaningful semantic tests and representative timings.
- [x] Final verification: full Rust tests, feature/benchmark build, Python
  harness tests, formatting/clippy, 100 MiB and 1 GiB answer-checked comparisons,
  RSS/CPU scaling, independent review and resolution of findings.

Implementation is split across reader, benchmark, planner/operators, and parallel
execution workstreams (more than eight files). The parent serializes integration,
git operations and commits; `cargo test` must pass before each commit as required
by CLAUDE.md. CHANGELOG is updated for each completed implementation phase. No
push, tag or release is included in this authorization.

## Execution notes

- Started with the existing audit as the only untracked file; no user changes
  were overwritten. Current baseline measurements are in the ignored
  `scripts/bench_e2e/results/audit-2026-09-05/` directory.

- Integration found additional dependency bugs in aliases, scope-hoisted constants,
  array roots and nested sorting. Regression tests now cover the actual SQL path,
  including time-bucket literals that had previously bypassed the optimization.
- The bounded large-range scan initially serialized producer work behind ordered
  queues. Scans now use 256 KiB newline-aligned tasks; associative aggregates use
  contiguous worker ranges and merge typed partial states. No complete input
  batch collection remains in production parallel execution.
- Raw ELB/ALB format does not guarantee sorted input. Time bucketing retains its
  compiled direct projection but uses memory-accounted hash grouping, preserving
  repeated buckets across NULL/MISSING and out-of-order timestamps.
- JSON globs open one file at a time; an 80-shard fixture succeeds with a 64-file
  descriptor process limit. General nested sort keys conservatively use row path
  evaluation with the shared stable comparator.

- Final controls identified unchanged memory charges contending on the shared
  mutex. A failing concurrency regression preceded the no-op guard in `874153c`;
  full tests/Clippy passed and comparisons were rerun. See
  [final measurements and boundaries](../performance-2026-09-05.md).
