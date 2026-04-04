# CHANGELOG — PartiQL Implementation Progress

## Current Status
Phase 2 complete. Starting Phase 3: Clauses & Query Structure.

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

## Failed Approaches
- Worktree isolation caused branch confusion when two agents ran in parallel. Avoided worktrees after that.

## Known Limitations
- No subqueries
- No UNION/INTERSECT/EXCEPT
