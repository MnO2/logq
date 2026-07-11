# CHANGELOG — PartiQL Implementation Progress

## Current Status
Phase 4 complete. All phases done.

## Improvement Roadmap (2026-07-11)

- **WS1:** Cleared all compiler and Clippy warnings, restored all-target benchmark compilation, and modernized CI with current GitHub Actions, strict formatting/Clippy gates, an all-features check, and Linux/macOS/Windows tests.
- **WS2:** Added transparent gzip input plus deterministic glob and comma-list expansion for tables. Mixed compressed/plain shards are supported, while eligible plain files retain per-file mmap scanning.
- **WS3:** Migrated from clap 2's YAML configuration to clap 4's derive API, with version metadata sourced directly from `Cargo.toml`. Replaced the unmaintained `json` crate with `serde_json`, preserving JSONL field order and making nested objects/arrays round-trip through JSON output. Adopted Rust edition 2024 with a documented and CI-tested Rust 1.85 MSRV. Updated the remaining dependency graph to the newest MSRV-compatible releases, removed unused or vulnerable direct dependencies, and modernized chrono/criterion call sites. CI now rejects RustSec advisories through both `cargo audit` and `cargo deny`.
- **WS4:** Added a nightly parser-fuzz target with 221 source-derived seeds and a non-blocking CI smoke run, plus six proptest properties for numeric comparisons, three-valued logic, null arithmetic, and null ordering. Fuzzing found and fixed panics on overflowing integers and `LIMIT` values. Added a 54-case, source-attributed PartiQL execution subset with a reasoned skip list for unsupported upstream areas.
- **WS5:** Added deterministic 100 MB/1 GB ELB, ALB, and JSONL dataset generation (including gzip), a hyperfine-based four-tool comparison harness, peak-RSS collection, and Markdown formatting. Published the 100 MiB JSONL results and recorded the scan, grouping, LIKE, and top-N gaps that feed WS8.
- **WS6:** Added statement-level syntax locations and hand-rendered multi-line caret diagnostics, with targeted hints for malformed syntax. Planner errors now identify and suggest functions, tables, and fixed-schema columns while preserving PartiQL `MISSING` behavior for dynamic JSONL fields. Runtime type and argument failures retain query context and point to the failing expression.
- **WS7a:** Audited INNER, bare, and RIGHT OUTER JOIN behavior across aliases, NULL keys, unmatched rows, aggregation, and residual predicates. Fixed qualified table scopes in hash and nested-loop joins and kept output columns in SELECT-list order.
- **WS7b:** Added user-defined regex log formats through `--table it:regex=... --format-file definition.toml`. Named captures become columns with optional integer, float, and chrono-formatted datetime types; the normal stdin, gzip, glob, and multi-file readers remain available.
- **WS7c:** Added `clf` and `combined` formats for standard Apache/nginx logs as predefined definitions on the regex-format engine, including typed timestamps, status codes, byte counts, and null handling for `-` numeric placeholders.
- **WS7d:** Completed `time_bucket` with compact `s`/`m`/`h`/`d` intervals and offset-preserving calendar-day buckets, including grouped coverage through both batch and row execution pipelines.

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
