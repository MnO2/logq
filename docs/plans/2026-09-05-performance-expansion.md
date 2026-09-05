# Performance expansion milestone

Baseline: `0d21af7` (the previously validated performance changes). The user
authorized planning and executing the recommended next milestone on 2026-09-05.
The existing untracked historical audit is preserved. This work spans more than
eight files; independent reader, expression, and measurement workstreams share
the checkout, while the parent serializes integration, timing, and commits.

## Deliverables and acceptance

- [x] Preserve a release baseline binary and establish paired, deterministic
  workloads: cardinality/skew, repeated/unique strings, wide/nested values,
  expressions, Top-K/full sort, and equivalent plain/sharded/gzip inputs.
  Independently verify complete answers before timing; record exact commands,
  binary/data/query/script hashes, thread settings, wall time and CPU time.
- [x] Add failing numeric-boundary regressions, then replace silent COUNT and
  approximate cardinality narrowing with explicit overflow errors. Preserve the
  existing public i32/f32 representation in this milestone and document the
  separate i64/f64 migration contract, including equality and hashing.
- [x] Reuse dictionary predicate kernels for selected JSON string columns when
  cardinality/length justify construction. Include high-cardinality controls and
  keep the optimization only if total scan/query cost supports it.
- [x] Build primitive JSON columns directly instead of constructing a complete
  Vec<Value> intermediate. Keep strict ignored-field validation, last duplicate
  key semantics, escaped names, nested values, and dynamic type changes.
- [x] Add scanner controls using the same JSON kernel with different reader
  backends, plus a direct batch/reader benchmark with allocation counters. Do not
  interpret existing --threads 1 versus auto as a pure scheduling comparison.
- [x] Extend batch projection/expression execution for useful common expressions
  while retaining exact scalar semantics and local fallback. Test actual SQL,
  EXPLAIN, scope/aliases, NULL/MISSING, errors, masked branches and aggregate
  inputs. Measure simple and expression query pairs against the saved baseline.
- [ ] Run representative paired measurements after compilation has stopped;
  inspect regressions and fix or reject changes that add cost without benefit.
  Keep the existing five-query suite as a regression control and promote useful
  cases to larger inputs when the first measurements justify it.
- [ ] Complete full Rust tests, formatting, all-target/all-feature Clippy, Rust
  1.85 compatibility, Python harness tests, benchmark smoke checks, independent
  review and a report that separates measurements from remaining hypotheses.

Correct answers and error behavior are mandatory. Performance acceptance uses
repeatable changes larger than run-to-run variation, considers both default and
single-worker settings, and checks contrasting workloads rather than a selected
best case. A rejected optimization is a valid measured outcome. No claimed cold
cache, real-world corpus, or heap profile may be substituted with warm synthetic
measurements or total RSS.

## Follow-on directions and advancement criteria

These are independently shippable extensions after this recommended milestone;
the paired cases above determine which merits implementation next.

| Direction | First controlled experiment | Advance when | Constraints |
| --- | --- | --- | --- |
| High-cardinality aggregation / DISTINCT | Fixed row count with 9, 100K and near-unique keys; uniform and skewed distributions; isolate local and final merge cost | Key state or merge dominates; specialized keys/partitioned merge lower CPU or bytes/group | Preserve key equality, first appearance and floating-point merge policy |
| Worker-local Top-K / deferred payload | K=10/1000 with narrow and wide payload, stable global row positions | Central consumer or payload construction dominates; candidate reduction beats extra heaps | Account workers times K memory; preserve ties, malformed-input errors and cancellation |
| Small shards / gzip scheduling | Identical rows as one file, small shards and gzip | Sequential file/decompression path dominates after parser improvements | Bounded workers, queues and file descriptors; deterministic input order |
| Larger-than-memory execution | Fixed operator budgets, full sort and near-unique grouping; compare buffered and mapped input separately | Workloads need bounded-memory completion; measured pressure identifies the operator | Batch lifetime accounting before sorted runs / partition spill; cleanup on error/cancel |
| Columnar cache / skipping indexes | Repeated-query total cost at 1/10/100 repetitions including conversion, storage and validation | Reuse amortizes preparation; measured skipped bytes materially improve total time | Standard Parquet evaluation; immutable input identity; append/replacement invalidation; NULL/MISSING encoding |
| Wider numeric representation | Boundary/coercion/equality tests, then i64/f64 prototype across parser, columns, functions, hashes and serialization | Contract is consistent across scalar/batch and public interfaces | Exact integer keys; explicit overflow; no silent floating-point conversion of integers |

## Verification commands

`cargo test --all-features --locked`, `cargo fmt --all -- --check`,
`cargo clippy --all-targets --all-features --locked -- -D warnings`,
`cargo +1.85.0 check --all-targets --all-features --locked`, and
`python3 -m unittest discover -s scripts/bench_e2e -p 'test_*.py'`.
Run `cargo test` before every meaningful commit as required by CLAUDE.md.

Implementation rollback reverts code commits; no user input files are modified.
Generated measurements live under ignored benchmark results directories.

## Execution notes

- Saved the unchanged release baseline to
  `/tmp/logq-milestone-20260905/logq-baseline-0d21af7`, SHA-256
  `e1492f8eb16727a9beb5fc9b88ec3aa1c4efc30822d76e80c7cb2e5d323d122b`.

- Direct-column and batch-expression implementation is complete. General new
  expression routes are limited to JSONL, and prefix LIMIT retains demand-driven
  row evaluation. Computed aggregate projection inputs and COUNT fast-path
  narrowing received failing-before/passing-after regressions.
- JSON dictionary-on/off CLI controls did not show a stable total-query win;
  dictionary construction remains available only through test/benchmark hooks.
- Same-kernel reader controls supported using a 64 KiB sequential JSON buffer.
  Oversized duplicate-string retention and SIMD-padding capacity doubling were
  found by independent review and fixed with regression coverage.
