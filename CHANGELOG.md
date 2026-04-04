# CHANGELOG — PartiQL Implementation Progress

## Current Status
Phase 1 complete. Starting Phase 2: Expressions.

## Completed Tasks

### Phase 0: Code Cleanup (2026-04-04)
- **Step 1:** Fixed case-sensitivity — replaced all `tag()` with `tag_no_case()`, made `from_str` case-insensitive, expanded KEYWORDS list to 44 entries, removed `to_ascii_lowercase()` from main.rs. Fixed critical bug: precedence table lookup panicked on uppercase AND/OR.
- **Step 2:** Migrated `failure` crate to `thiserror`/`anyhow` — 13 error types across 6 files, 23 manual `From` impls replaced with `#[from]`. Net -210 lines.
- **Step 3:** Deduplicated `get_value_by_path_expr` — moved from execution/types.rs and execution/stream.rs to common/types.rs.
- **Step 4:** Fixed `ApproxCountDistinctAggregate::PartialEq` stub — replaced always-true with proper HyperLogLog count comparison.
- **Step 5:** Fixed version mismatch — synced cli.yml to 0.1.19.
- **Step 6:** Fixed `is_match_group_by_fields` nondeterministic bug — replaced positional HashSet iteration with proper set equality.
- **Step 7:** Fixed `LimitStream` early termination bug — added early return when limit reached.

### Phase 1: Foundation (2026-04-04)
- **Step 8:** Added float arithmetic and NULL/MISSING propagation — Plus/Minus/Times/Divide handle Float, mixed Int/Float promotion, and propagate NULL/MISSING. Integer division by zero returns NULL.
- **Step 9:** Added Int/Float coercion in comparisons — Relation::apply handles mixed Int/Float and returns None for NULL/MISSING comparisons.
- **Step 10:** Implemented three-valued logic — Formula::evaluate returns Option<bool>. AND/OR/NOT follow PartiQL spec (FALSE AND UNKNOWN = FALSE, TRUE OR UNKNOWN = TRUE). FilterStream treats None as "not true".
- **Step 11:** Added IS [NOT] NULL/MISSING operators and NULL/MISSING literals — new postfix operators in parser, routed through logical planner, evaluated in Formula.
- **Step 12:** Fixed ORDER BY mixed-type handling — NULL/MISSING sort last ascending, first descending. Mixed types maintain stability.
- **Step 13:** Multi-branch CASE WHEN — CaseWhenExpression now supports multiple WHEN branches. No ELSE returns NULL.
- **Step 14:** Refactored parse_logic — replaced unreachable!() with ExpressionPredicate for FuncCall, CaseWhen, Column in boolean context.

## Failed Approaches
- Worktree isolation caused branch confusion when two agents ran in parallel. Avoided worktrees after that.

## Known Limitations
- No LIKE, BETWEEN, IN, CAST, string concat, COALESCE/NULLIF
- No JOINs, subqueries, DISTINCT, SELECT VALUE (parsed but unimplemented)
- No UNION/INTERSECT/EXCEPT
- date_part only supports Second and Minute
