VERDICT: APPROVED

## Summary Assessment

All four critical issues from round 1 have been properly resolved. The `PaddedVecBuilder` / `PaddedVec` lifecycle is now structurally sound, the conjunct extraction and reconstruction logic correctly operates on the actual `Formula` enum, the residual filter field propagation is complete, and the field indices match the codebase. No new critical issues were introduced by the fixes. Two minor suggestions below.

## Critical Issues (must fix)

None.

## Suggestions (nice to have)

### S1. StringFilterCache adaptive check runs AFTER Pass 1 completes all hashing -- wasted work on high-cardinality columns

In `StringFilterCache::evaluate_cached`, Pass 1 iterates all N rows, inserting into the cache (hashing every row's string and potentially calling `filter_fn` for each unique value). Only after the entire pass completes does the cardinality check fire (`cache.len() > len / 10`). For a high-cardinality column like `user_agent` (where nearly every value is unique), Pass 1 does N hash insertions and ~N filter evaluations before the check fires and falls back to `direct_filter_evaluation`, which then does another N filter evaluations. This means high-cardinality columns pay roughly 2x the cost of direct evaluation.

A simple improvement: check the cardinality threshold incrementally during Pass 1 (e.g., every 64 rows), and bail out to direct evaluation early if the ratio is already above threshold. This avoids the worst case without complicating the common case.

### S2. `parse_field_column_selected` emits offsets for inactive rows (Utf8 path), creating misleading zero-length strings

In Section 9, the `parse_field_column_selected` Utf8 branch always pushes an offset via `offsets_builder.push(data_builder.len() as u32)` for every row, including inactive ones. This means inactive rows get zero-length string entries (offset[i] == offset[i+1]) rather than being marked as MISSING. This is functionally correct as long as downstream operators always respect the selection vector, but it means the Utf8 column's data layout does not distinguish "empty string" from "not parsed". If any operator or debug code ever reads the column without checking selection, it would see empty strings instead of MISSING. Consider setting the `missing` bitmap bit to 0 for inactive rows to be explicit, or document that inactive-row values in Phase 2 columns are undefined and must never be read.

## Verified Claims (things confirmed correct)

1. **PaddedVecBuilder/PaddedVec lifecycle is correct at all usage sites.** Section 5 (string storage) constructs via `PaddedVecBuilder::with_capacity`, uses `push`/`extend_from_slice`, then calls `.seal()` to produce `PaddedVec`. Section 9 (`parse_field_column_selected`) follows the same pattern. No site attempts to mutate a `PaddedVec` after construction. The removal of `DerefMut` from `PaddedVec` makes this structurally enforced.

2. **Conjunct extraction logic is correct.** `extract_conjuncts` recursively flattens `InfixOperator(And, left, right)` into a flat `Vec<Formula>`. `rebuild_conjunction` pops from the back and wraps in right-associative `InfixOperator(And, next, result)`. For input `[a, b, c]`, this produces `And(a, And(b, c))`, which is semantically equivalent regardless of associativity. The clone in `extract_conjuncts` (via `other => vec![other.clone()]`) is acceptable since this runs once at plan time.

3. **Predicate cost model uses correct Formula variants.** Verified against the actual `Formula` enum in `src/logical/types.rs` (lines 272-286):
   - `Formula::IsNull`, `Formula::IsNotNull`, `Formula::IsMissing`, `Formula::IsNotMissing` -- all exist (lines 277-280)
   - `Formula::Constant(bool)` -- exists (line 275)
   - `Formula::Predicate(Relation, Box<Expression>, Box<Expression>)` -- exists (line 276)
   - `Formula::Like(Box<Expression>, Box<Expression>)` -- exists (line 282)
   - `Formula::NotLike(Box<Expression>, Box<Expression>)` -- exists (line 283)
   - `Formula::In(Box<Expression>, Vec<Expression>)` -- exists (line 284)
   - `Formula::NotIn(Box<Expression>, Vec<Expression>)` -- exists (line 285)
   - `Formula::ExpressionPredicate(Box<Expression>)` -- exists (line 281)
   - `Formula::PrefixOperator(LogicPrefixOp, Box<Formula>)` -- exists (line 274)
   - `Formula::InfixOperator(LogicInfixOp, Box<Formula>, Box<Formula>)` -- exists (line 273)
   - The wildcard `_ => 60` arm is technically unreachable since all variants are covered, but is harmless as a defensive default.

4. **`is_numeric_column` and `is_string_column` pattern match on `Expression::Variable(path_expr)`.** Verified that the logical `Expression` enum (line 180) has `Variable(ast::PathExpr)`, so `Expression::Variable(path_expr)` is the correct pattern. The helper functions `schema.field_index_by_path(path_expr)` and `schema.field_type(field_idx)` are new abstractions introduced by this design (on the to-be-created `LogSchema` type), which is appropriate.

5. **Residual filter field propagation is complete.** The `PushdownResult` struct carries `residual_field_indices`. The planner passes `pushdown.residual_field_indices` into `BatchScanOperator::residual_filter_fields`. Phase 2 merges `projected_fields` and `residual_filter_fields`, deduplicates against Phase 1 fields, and parses them for surviving rows. The downstream `BatchFilterOperator` wraps the scan when there is a residual predicate. The chain is: planner computes residual fields -> scan operator stores them -> Phase 2 parses them -> residual filter operator evaluates on them.

6. **Field indices in the Appendix are correct.** Verified against `AWS_ELB_FIELD_NAMES` (lines 217-237) and `ClassicLoadBalancerLogField` enum (lines 401-419) in `datasource.rs`:
   - `elb_status_code` is at index 7 (`ELBStatusCode = 7`) -- correct
   - `user_agent` is at index 12 (`UserAgent = 12`) -- correct
   - The full schema listing in the Appendix (indices 0-16) matches the enum values exactly.

7. **ALB string field count corrected to 16.** The table in Section 4 now shows 16 String fields for ALB. Counting `AWS_ALB_DATATYPES` (lines 240-269): indices 0, 2, 8, 9, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24 = 16 String fields. Correct.

8. **Impact analysis arithmetic is correct.** For `SELECT user_agent FROM elb WHERE elb_status_code = '200' AND user_agent LIKE '%bot%'` with 20% match: Phase 1 parses field #7 for 1024 rows (1024 parses). Phase 2 parses field #12 for 200 surviving rows (200 parses). Total: 1224 field parses vs 17*1024 = 17408 in v3. Ratio: 17408/1224 = 14.2x. The stated "14x fewer field parses" is correct.

## Resolution of Round 1 Issues

### C1: PaddedVec API incomplete -- RESOLVED

The design adopted option (b) from the review: `PaddedVec<T>` is now immutable after construction (only `Deref<Target=[T]>`, no `DerefMut`). `PaddedVecBuilder<T>` provides `push`, `extend_from_slice`, `with_capacity`, and consumes itself via `seal()` to produce an immutable `PaddedVec<T>`. The `debug_assert!` checks from S1 are incorporated. All usage sites in Sections 5 and 9 correctly use `PaddedVecBuilder` for construction and `PaddedVec` for immutable access.

### C2: Lazy parsing does not account for non-pushed WHERE columns -- RESOLVED

The `BatchScanOperator` now has a `residual_filter_fields: Vec<usize>` field. `try_push_predicate` returns a `PushdownResult` that includes `residual_field_indices` for non-pushed conjuncts. Phase 2 merges `projected_fields` and `residual_filter_fields` (with deduplication against Phase 1 fields) and parses them for surviving rows. The planner correctly wires up a `BatchFilterOperator` for the residual predicate when needed.

### C3: Predicate cost model uses non-existent Formula variants -- RESOLVED

The `predicate_cost` function now matches on the actual `Formula` enum variants from `src/logical/types.rs`: `Predicate(Relation, ...)`, `Like(...)`, `NotLike(...)`, `In(...)`, `NotIn(...)`, `IsNull(...)`, etc. The `extract_conjuncts` function correctly flattens `InfixOperator(LogicInfixOp::And, ...)` trees. The `rebuild_conjunction` function correctly reconstructs them. All variant names verified against the source.

### C4: Appendix example uses wrong field indices -- RESOLVED

The Appendix now uses `elb_status_code` at index 7 and `user_agent` at index 12, both verified correct against `ClassicLoadBalancerLogField` in `datasource.rs`. The query uses valid column names (`elb_status_code`, `user_agent`). The full ELB schema reference listing in the Appendix matches the actual enum values exactly.
