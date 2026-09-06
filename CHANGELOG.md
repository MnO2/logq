# CHANGELOG — PartiQL Implementation Progress

## Unreleased — correctness, performance, and documentation audit (2026-09-06)

- Read every member of concatenated gzip files, including rows split across members, and report corruption or truncation in later members. Keep JSON prefix LIMIT demand-driven for simple projections, filters, and DISTINCT.
- Preserve literal-prefixed field names and scientific notation; allow spaced operators after CAST, CASE, and postfix predicates. Validate aggregate arity and percentile bounds during planning, accept case-insensitive aggregate names, and prevent zero-argument scalar calls or non-boolean arithmetic predicates from panicking.
- Correct Unicode character lengths, extreme substring/split indexes, integer overflow and invalid bit shifts. Validate arity on public function-registry calls and give array_sort the same total ordering as ORDER BY. MIN/MAX and percentiles ignore NULL/MISSING; mixed numeric extrema, discrete percentile ranks/endpoints, and descending approximate percentiles now return correct results.
- Propagate final output-write failures for every query output format and schema tables, reject unknown schema formats with a failing exit status, and accept underscores in table names.
- Bound parallel newline searches for oversized log rows, reduce Levenshtein workspace to the shorter string, stop split_part after the requested segment, and reuse cached regex matching workspace. Add correctness and allocation regressions for these paths.
- Protect benchmark result directories from accidental reuse so failed reruns cannot mix new samples with an earlier report.
- Refresh current architecture, CLI examples, development checks, memory and numeric limits, and historical benchmark labels.
- Make closed-stdout regressions deterministic when macOS tests launch subprocesses concurrently.

## Unreleased — next performance milestones (2026-09-05)

- Borrow nested batch values when resolving paths, retain movable columns in mixed projections, and evaluate supported complex predicates without copying unrelated columns. Preserve aliases, active masks, lazy errors and demand-driven LIMIT; budgeted mixed projections retain their existing materialization policy.
- Add answer-checked narrow/wide, predicate-selectivity, plain/gzip and 1/8/32/125-file controls, with paired CPU/wall measurements and reproducible input manifests. Track independent follow-on work in [the milestone plan](docs/plans/2026-09-05-performance-next-milestones.md).
- Execute eligible JSONL shards through one bounded worker pool with per-file filtering, projection and partial aggregation, including independent gzip decoders. Preserve ordered merging and cancellation; non-regular inputs retain lazy sequential processing so later FIFOs cannot block earlier errors.
- Use flate2's Rust zlib-rs backend after isolated decode/parse/CLI comparisons; keep the pinned flate2 version and Rust 1.85 compatibility requirement.
- Pipeline eligible single-gzip full aggregations through bounded decoded chunks and worker-local filters, expressions and aggregate states. Count the decoder within `--threads`; preserve one-thread and memory-budget fallbacks, decoded-prefix errors, cancellation and worker teardown.
- Bind batch calls to their actual function definitions and specialize trusted Float32 Plus chains into typed output, preserving per-step rounding, custom functions, masks, duplicate aliases and lazy errors. Serialize borrowed JSON/NDJSON records without rebuilding object trees, retaining exact public float formatting.
- Add diagnostic aggregation, expression, reader/task and gzip phase probes. Keep plain aggregate task scheduling unchanged after skew controls; do not enable broader kernels from isolated timings alone.
- Add opt-in Parquet/persisted ClickHouse reuse experiments with full representation checks, immutable prepared identities, preparation/storage costs and actual 1/10/100-query fresh-process and session controls. General transparent caching remains deferred outside the validated input contract.
- Resolve the existing lru dependency to 0.18.2 for RUSTSEC-2026-0253 and pass the advisory gates.
- Record final paired CLI controls, phase measurements, adoption decisions and limitations in [the next-milestones report](docs/performance-next-milestones-2026-09-05.md).

## Unreleased — performance expansion (2026-09-05)

- Build homogeneous JSONL primitive columns directly, avoiding per-row owned values and long-string allocations; preserve strict parsing, duplicate keys, mixed types and validity masks. Use a 64 KiB sequential JSON buffer.
- Execute supported JSONL projections and computed aggregate inputs with bound batch expressions. Preserve active-row masks, lazy CASE branches, duplicate aliases and demand-driven expression LIMIT behavior; fixed-format expressions retain their existing reader semantics.
- Correct computed SUM/AVG/COUNT inputs that previously read missing projection fields. Keep separate function occurrences independent and report COUNT/approximate cardinality outside the public Int32 range instead of silently narrowing, including the fixed-format COUNT scan fast path.
- Add deterministic paired workloads with independent answer checking, thread/CPU/RSS controls and reproducible provenance. Add identical-kernel JSON reader and allocation probes; JSON dictionary construction remains experimental because measured total query cost did not show a stable benefit.
- Track implementation and the acceptance criteria for subsequent aggregation, Top-K, shard scheduling, spilling, columnar reuse and numeric-width work in [the expansion plan](docs/plans/2026-09-05-performance-expansion.md).
- Recorded paired 100K/500K-row workloads, isolated scanner/allocation experiments and original JSONL/ELB/ClickHouse controls in [the expansion report](docs/performance-expansion-2026-09-05.md), including the remaining COUNT gap and CPU/RSS tradeoffs.

## Unreleased — performance corrections (2026-09-05)

- Parse JSONL directly into required root fields and typed batches while validating ignored input; preserve nested values, NULL/MISSING, duplicate keys and aliases. Keep unsupported dynamic projections on the strict row reader.
- Execute scans with shared mmap ranges, lazy file opening, bounded queues and controlled workers. Small ordered scan tasks avoid queue stalls; worker-local COUNT/SUM/AVG states merge without rounding intermediate results. `--threads 0` now resolves to available CPUs.
- Preserve batch execution under `--max-memory`, charging queued batches and retained grouping, sorting, DISTINCT and bucket state through shared reservations. Failures cancel workers and return nonzero CLI status.
- Correct COUNT(column), aggregate aliases and schemas, typed/Mixed grouping equality, NULL/MISSING predicates and aggregates, projection dependencies, stable mixed-value sorting, duplicate aliases and constant projections. Accumulate SUM in f64 and round once to the existing public f32 representation.
- Skip unchanged memory reservations so a generous budget does not serialize fixed-size parallel aggregate updates.
- Reuse LIKE regex search caches and dictionary matching, tokenizer offset storage and group key buffers. Compare TopK keys before constructing rejected payloads, and move full-sort output through its permutation.
- Compile time-bucket intervals once and aggregate computed batch buckets without assuming raw logs are sorted. Preserve timezone offsets, fractional timestamps and arbitrary input ordering.
- Make benchmark queries fail visibly, validate answers against an independent oracle, fix multi-key ORDER BY parsing and LIMIT timing, and record thread settings, data/query/binary hashes and build provenance.
- Recorded answer-checked 100 MiB/1 GiB comparisons, 1/2/4/8-thread scaling and memory-ceiling controls in [the performance report](docs/performance-2026-09-05.md).

## 0.2.0 - 2026-07-11

- Query a substantially expanded PartiQL subset, including INNER/RIGHT joins, subqueries, set operations, three-valued NULL/MISSING logic, approximate aggregates, array/string/date functions, and completed `time_bucket` intervals.
- Read gzip files transparently and scan deterministic globs or comma-separated shards without concatenating them first.
- Query schema-free JSONL, custom named-capture regex formats, and built-in Apache/nginx `clf` and `combined` logs.
- Stream newline-delimited JSON with `--output ndjson`; inspect batch/row selection with pipeline-aware `explain`; cap materializing queries with `--max-memory`.
- Diagnose syntax, planning, schema, and runtime failures with source locations, carets, contextual hints, and did-you-mean suggestions.
- Bound `ORDER BY ... LIMIT` memory to O(k), retain fixed-schema batch execution where supported, and publish reproducible end-to-end performance and memory measurements.
- Require Rust 1.85 or newer and ship current, advisory-clean dependencies with multi-platform CI.

## Current Status
Phase 4 complete. All phases done.

## Improvement Roadmap (2026-07-11)

- **WS1:** Cleared all compiler and Clippy warnings through Rust 1.97, restored all-target benchmark compilation, and modernized CI with current GitHub Actions, strict formatting/Clippy gates, an all-features check, and Linux/macOS/Windows tests.
- **WS2:** Added transparent gzip input plus deterministic glob and comma-list expansion for tables. Mixed compressed/plain shards are supported, while eligible plain files retain per-file mmap scanning.
- **WS3:** Migrated from clap 2's YAML configuration to clap 4's derive API, with version metadata sourced directly from `Cargo.toml`. Replaced the unmaintained `json` crate with `serde_json`, preserving JSONL field order and making nested objects/arrays round-trip through JSON output. Adopted Rust edition 2024 with a documented and CI-tested Rust 1.85 MSRV. Updated the remaining dependency graph to the newest MSRV-compatible releases, removed unused or vulnerable direct dependencies, and modernized chrono/criterion call sites. CI now rejects RustSec advisories through both `cargo audit` and `cargo deny`.
- **WS4:** Added a nightly parser-fuzz target with 221 source-derived seeds and a one-minute non-blocking CI smoke run, plus six proptest properties for numeric comparisons, three-valued logic, null arithmetic, and null ordering. Fuzzing found and fixed panics on overflowing integers and `LIMIT` values. Added a 54-case, source-attributed PartiQL execution subset with a reasoned skip list for unsupported upstream areas.
- **WS5:** Added deterministic 100 MB/1 GB ELB, ALB, and JSONL dataset generation (including gzip), a hyperfine-based four-tool comparison harness, peak-RSS collection, and Markdown formatting. Published the 100 MiB JSONL results and recorded the scan, grouping, LIKE, and top-N gaps that feed WS8.
- **WS6:** Added statement-level syntax locations and hand-rendered multi-line caret diagnostics, with targeted hints for malformed syntax. Planner errors now identify and suggest functions, tables, and fixed-schema columns while preserving PartiQL `MISSING` behavior for dynamic JSONL fields. Runtime type and argument failures retain query context and point to the failing expression.
- **WS7a:** Audited INNER, bare, and RIGHT OUTER JOIN behavior across aliases, NULL keys, unmatched rows, aggregation, and residual predicates. Fixed qualified table scopes in hash and nested-loop joins and kept output columns in SELECT-list order.
- **WS7b:** Added user-defined regex log formats through `--table it:regex=... --format-file definition.toml`. Named captures become columns with optional integer, float, and chrono-formatted datetime types; the normal stdin, gzip, glob, and multi-file readers remain available.
- **WS7c:** Added `clf` and `combined` formats for standard Apache/nginx logs as predefined definitions on the regex-format engine, including typed timestamps, status codes, byte counts, and null handling for `-` numeric placeholders.
- **WS7d:** Completed `time_bucket` with compact `s`/`m`/`h`/`d` intervals and offset-preserving calendar-day buckets, including grouped coverage through both batch and row execution pipelines.
- **WS7e:** Added streaming `--output ndjson`, emitting one JSON object per row without buffering the complete result set.
- **WS8 (observability):** Extended `explain` with the selected batch/row execution pipeline and a specific plan-node reason whenever batch execution falls back.
- **WS8 (top-N):** Added bounded-heap execution for `ORDER BY ... LIMIT` in both batch and row pipelines, with tests that cap retained candidates at `k`. Corrected parallel batch planning so LIMIT never truncates the input before sorting, grouping, filtering, or deduplication.
- **WS8 (ceilings):** Measured the deterministic 1 GiB JSONL corpus: high-cardinality GROUP BY peaked at 1,318.6 MiB RSS, full ORDER BY at 2,140.6 MiB, and DISTINCT at 1,485.4 MiB. The WS5 query set consistently falls back at the dynamic JSONL datasource rather than at an unsupported batch operator.
- **WS8 (memory budget):** Added `--max-memory` with byte and `KiB`/`MiB`/`GiB` values. One query-wide tracker now covers sorting, top-N, grouping, deduplication, set operations, and materialized joins, while JSON array output streams without retaining the full result. Execution aborts with a clear error before combined estimated state crosses the ceiling.
- **WS9:** Published version 0.2.0 to crates.io and released dist 0.32.0 binaries for Apple Silicon/Intel macOS, x86-64 musl Linux, and x86-64 Windows, with shell and PowerShell installers and checksums attached to the `v0.2.0` GitHub Release.

## Completed Tasks

### Phase 0: Code Cleanup (2026-04-04)
- **Step 1:** Fixed case-sensitivity — replaced all `tag()` with `tag_no_case()`, expanded KEYWORDS list, removed lowercasing. Fixed precedence table panic on uppercase AND/OR.
- **Step 2:** Migrated `failure` to `thiserror`/`anyhow` — 13 error types, 23 manual `From` impls replaced. Net -210 lines.
- **Step 3:** Deduplicated `get_value_by_path_expr` into common/types.rs.
- **Step 4:** Fixed `ApproxCountDistinctAggregate::PartialEq` stub.
- **Step 5:** Fixed version mismatch (cli.yml → 0.1.19).
- **Step 6:** Fixed `is_match_group_by_fields` nondeterministic HashSet bug.
- **Step 7:** Fixed `LimitStream` early termination bug.

### Phase 1: Foundation (2026-04-04)
- **Step 8:** Float arithmetic + NULL/MISSING propagation in binary ops.
- **Step 9:** Int/Float coercion in comparisons, NULL returns None.
- **Step 10:** Three-valued logic — Formula::evaluate returns Option<bool>.
- **Step 11:** IS [NOT] NULL/MISSING operators + NULL/MISSING literals.
- **Step 12:** ORDER BY handles NULL/MISSING (last ASC, first DESC).
- **Step 13:** Multi-branch CASE WHEN.
- **Step 14:** parse_logic handles FuncCall/CaseWhen/Column via ExpressionPredicate.

### Phase 2: Expressions (2026-04-04)
- **Step 15:** Post-parse AST desugaring infrastructure (desugar.rs).
- **Step 16:** LIKE/NOT LIKE with % and _ wildcards (regex-based, NULL propagation).
- **Step 17:** BETWEEN/NOT BETWEEN parsed as postfix, desugared to >= AND <=.
- **Step 18:** IN/NOT IN with NULL-aware membership testing.
- **Step 19:** CAST(expr AS type) for Int/Float/Varchar/Boolean conversions.
- **Step 20:** String concatenation (||) as binary operator.
- **Step 21:** COALESCE/NULLIF desugared to CASE WHEN.
- **Step 22:** String functions (UPPER, LOWER, CHAR_LENGTH, SUBSTRING, TRIM) + date_part extended to Hour/Day/Month/Year.

### Phase 3: Clauses & Query Structure (2026-04-04)
- **Step 23:** SELECT VALUE for scalar/tuple/array value constructors.
- **Step 24:** DISTINCT via DistinctStream with HashSet dedup.
- **Step 25:** Path wildcards ([*] and .*) for array/tuple iteration.
- **Step 26:** CROSS JOIN (explicit and comma syntax) with nested-loop stream.
- **Step 27:** LEFT [OUTER] JOIN ... ON with NULL-padded non-matching rows. Refactored AST to use FromClause enum (Tables | Join) instead of Vec<TableReference>.
- **Step 28:** Non-correlated scalar subqueries in WHERE and SELECT. Added Expression::Subquery, recursive parse_query, data_source to ParsingContext.

### Phase 4: Set Operations (2026-04-04)
- **Step 29:** UNION / UNION ALL — top-level Query enum wrapping SelectStatement + SetOp. UnionStream drains left then right. UNION uses Distinct for dedup.
- **Step 30:** INTERSECT / EXCEPT (+ ALL variants) — materializes right query into multiset, filters left. Fixed IN/INTERSECT parser ambiguity with word boundary check.
- **Step 31:** Comprehensive integration tests exercising full pipeline.

### Performance Optimization (2026-04-05)

**Benchmark infrastructure:** Added Criterion microbenchmarks for parser (6 tiers), execution (E2E + operators), datasource (5 formats), and UDFs (6 functions).

**Optimizations applied (Rounds 1–15):**
- Replaced `HashMap` with `hashbrown::HashMap` across codebase (5–10% across all ops)
- Pre-sized `Variables` maps via `with_capacity` in hot paths
- Eliminated redundant `to_lowercase()` calls in GroupBy key comparison
- Converted `DateTime` from `Box<DateTime>` to inline `Value::DateTime(DateTime)` (udf -42%)
- Switched datasource field storage from `BTreeMap` to `Vec<(String,Value)>` → `LinkedHashMap`
- Pre-allocated `FunctionRegistry` HashMap capacity, hoisted registry creation out of bench loops
- Added `into_tuples()` consuming method to avoid cloning record fields at output
- Zero-clone rename-free projection path in MapStream

**Attempted but reverted:**
- Projection pushdown (skipping unused fields in datasource parser): correct in principle but `count(*)` leaks `Named::Star` into the Map projection list, causing `collect_needed_fields` to treat all GROUP BY queries as `SELECT *`. Would require top-down pushdown rewrite to fix correctly.

**Final benchmark results (cumulative):**
| Benchmark | Before | After | Improvement |
|-----------|--------|-------|-------------|
| E1 (scan+limit) | 121 us | 31.9 us | 74% |
| E2 (groupby+count) | 6.79 ms | 2.16 ms | 68% |
| E3 (filter+orderby) | 8.58 us | 2.19 us | 74% |
| map/100K | 75.4 ms | 21.4 ms | 72% |
| filter/100K | 52.8 ms | 14.9 ms | 72% |
| datasource/ELB | 2.89 ms | 933 us | 68% |

## Failed Approaches
- Worktree isolation caused branch confusion when two agents ran in parallel. Avoided worktrees after that.

## Known Limitations
- No correlated subqueries (only non-correlated scalar subqueries supported)
- No window functions
- No PIVOT, Ion literals, bag literals
