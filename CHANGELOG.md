# CHANGELOG — PartiQL Implementation Progress

## Current Status
Phase 4 complete. All phases done.

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
- No INNER JOIN (can simulate with CROSS JOIN + WHERE)
- No window functions
- No PIVOT, Ion literals, bag literals
