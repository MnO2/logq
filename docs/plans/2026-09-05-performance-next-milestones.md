# Performance next milestones

The user authorized execution of M1–M4 on 2026-09-05. Baseline is master
`00b7d67200673e481aad0fbeab4c7d482cdd79c1` (implementation `3f24c61`).
The existing untracked `docs/performance-audit-2026-09-05.md` is user work and
is preserved. This plan spans more than eight files. Each milestone remains
usable independently; experiments may reject a candidate rather than enable
an unmeasured optimization by default.

## M1 — eliminate avoidable ownership work and establish controls

- [x] Preserve the exact previous release binary and provenance.
- [x] Add tests before fixing nested root cloning, mixed projection copies,
  and scalar predicate extraction of unrelated columns. Preserve aliases,
  active masks, evaluation/error order, NULL/MISSING and demand-driven LIMIT.
- [x] Add contrasting wide/narrow, nested leaf, predicate-selectivity and
  mixed-projection workloads with complete independent answer checking.
- [x] Measure gzip decode-only, strict parse-only and full query costs; compare
  the pinned flate2 miniz backend with zlib-rs in isolated experiment builds.
- [ ] Run paired release measurements with builds stopped; retain candidates
  with repeatable target CPU/allocation or end-to-end improvements and no
  comparable unexplained control regressions. Update CHANGELOG and commit.

## M2 — parallelize complete input pipelines

- [ ] Extend eligible Files aggregation to a shared, bounded worker pool;
  preserve file order, cancellation, errors, memory reservations and the
  existing partial aggregate semantics. Include independent gzip shards.
- [ ] Compare equivalent 1/8/32/125-file corpora across the per-file mmap
  threshold, including plain/gzip and one-worker controls.
- [ ] Use decode/parse measurements to evaluate a bounded single-gzip
  producer/parser pipeline; keep the simpler path if queue/copy costs erase
  the gain. Cover long lines, UTF-8, malformed/truncated gzip and cancellation.
- [ ] Validate and commit the independently useful changes.

## M3 — specialize only measured operator and scheduling costs

- [ ] Add a benchmark-only same-input aggregation probe separating local
  accumulate, merge, finish and serialization; validate complete output.
- [ ] Compare 9/100K/near-unique keys and use the dominant phase to choose
  typed/batched finalization, key specialization or partitioned merge.
- [ ] Evaluate function binding and typed expression kernels independently,
  retaining scalar semantics for errors, volatile/custom functions and CASE.
- [ ] Compare clustered/dispersed expensive rows and fixed-reader worker
  controls before changing task policy; do not infer aggregate behavior from
  the generic scan task size or infer heap usage from mmap RSS.
- [ ] Keep only evidence-supported changes; validate and commit.

## M4 — measure columnar reuse and storage-aware comparisons

- [ ] Execute an opt-in standard Parquet preparation/read experiment and a
  persisted ClickHouse control on immutable, manifest-owned synthetic input.
  Record preparation time, storage, query latency and 1/10/100-query totals.
- [ ] Test the representation contract for absent/null/dynamic values, numeric
  widths and input replacement/append invalidation before considering a native
  persistent cache. A lossy fixed-schema experiment is not a logq cache.
- [ ] Evaluate column pruning/selective reads/deferred payload with narrow and
  wide queries; record actual measured boundaries and cache policy. A cold-read
  claim requires verified eviction and physical I/O evidence; otherwise label
  it unmeasured rather than simulate it by renaming files.
- [ ] Make a documented adoption/rejection decision from amortized cost and
  semantic coverage. No automatic input conversion or mutation of source data.

## Shared acceptance and verification

Correct answers and error behavior are mandatory. Use independent full-result
oracles before and after timed runs, both single-worker and auto controls,
wall/CPU time, separate allocation or RSS runs, exact commands and source/data/
binary hashes. Target improvements must exceed variability; approximately 5%
end-to-end is a useful promotion threshold, not a promised outcome. Preserve
existing narrow/low-cardinality/fixed-format controls. All timings are warm
synthetic unless explicitly demonstrated otherwise.

Tests are written before implementation. Run `cargo test` before every commit.
Final gates: `cargo test --all-features --locked`, `cargo fmt --all -- --check`,
`cargo clippy --all-targets --all-features --locked -- -D warnings`,
`cargo +1.85.0 check --all-targets --all-features --locked`, Python unittest
discovery in `scripts/bench_e2e`, and all five Criterion smoke targets. Review
architecture, filesystem/decompression boundaries and adversarial composition.
Root serializes timing, integration and commits; agents own disjoint code.

The current request authorizes implementation and experiments. No release,
package publication or version change is needed. Reverting a code commit
restores behavior without changing input files; experimental prepared data is
isolated under newly created benchmark directories.
