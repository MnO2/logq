VERDICT: NEEDS_REVISION

## Summary Assessment

The plan covers the overall design well and has a correct dependency ordering, but it contains several factual errors about the codebase (wrong occurrence counts, missing files in scope) and omits critical update sites that will cause compilation failures mid-execution.

## Critical Issues (must fix)

### C1: Step 2 omits 4 test call sites in `logical/types.rs` and miscounts `app.rs` occurrences

The plan says there are **4 occurrences** of `PhysicalPlanCreator::new(data_source)` in `app.rs` to update. There are actually **5** (lines 106, 123, 208, 234, 260). Additionally, Step 2 does not mention updating the **4 test call sites** in `src/logical/types.rs` (lines 689, 708, 752, 846) where `PhysicalPlanCreator::new(DataSource::Stdin(...))` is called. After removing the `data_source` parameter from `new()`, these 4 tests will fail to compile. Step 2 will not pass `cargo test` as written.

### C2: Step 4 is far too large for "2-5 minutes" -- it is 10+ sub-steps touching a single file with ~200 lines of changes

Step 4 contains sub-steps 4a through 4m, rewriting at least 7 functions, adding 2 new helper functions, updating the subquery site, updating all test `ParsingContext` constructions (9 occurrences in the test module), and updating all `parse_query()` calls in tests (7 occurrences). This is easily 30-45 minutes of careful work, not 2-5 minutes. It should be split into at least 3 tasks:
- 4A: Rewrite `check_env`/`check_env_ref`/`to_bindings`/`to_bindings_for_ref` signatures
- 4B: Add helpers and rewrite `build_from_node`/`parse_query_top`/`parse_query`
- 4C: Update all test constructions

### C3: Step 5 claims "~20 tests to update" in `app.rs` but does not enumerate them -- the actual count is much higher

By grep, there are approximately 48 `run(` calls and many `run_to_vec(` calls in the `app.rs` test module. Most go through `run_format_query` / `run_format_query_to_vec` helpers (which the plan does update), but there are ~15 individual tests that construct `DataSource` directly and pass it to `run()` or `run_to_vec()` (e.g., `test_run_explain_mode`, `test_run_real_flat_log`, `test_run_real_jsonl_log`, `test_run_cross_join_jsonl`, `test_run_cross_join_comma_syntax`, `test_run_cross_join_with_where`, `test_run_left_join_jsonl`, `test_run_left_join_no_match`, `test_run_left_outer_join_jsonl`, `test_run_subquery_in_where`, `test_run_subquery_in_select`, `test_run_union`, `test_run_intersect`, `test_run_except`, and multiple integration tests). Step 5 is also too large for a single task.

### C4: `is_match_group_by_fields` with `jsonl` format hits `unreachable!()` -- plan's Star+GroupBy check is incomplete

The plan adds a `StarGroupByMultiTable` error for `SELECT * + GROUP BY` in multi-table queries, but the existing `is_match_group_by_fields` function already has an `unreachable!()` branch for the `jsonl` format in the `Named::Star` arm (line 1064). This means `SELECT *, count(*) FROM it GROUP BY x` with a single JSONL table will **panic** today. The plan should note this pre-existing bug and either fix it or explicitly mark it as out of scope. At minimum, the new `StarGroupByMultiTable` check should also cover single-table `jsonl` queries to prevent the panic.

### C5: Step 4i's `parse_query` rewrite passes `data_sources` redundantly

In Step 4i, the plan constructs `ParsingContext { data_sources: data_sources.clone(), ... }` and then also passes `&data_sources` separately to `build_from_node`. Since `build_from_node` already receives `ctx: &ParsingContext` which contains `data_sources`, the separate parameter is redundant. This works but is confusing and inconsistent with the design doc. Either `build_from_node` should access `ctx.data_sources` internally, or it should not receive `ctx` at all.

### C6: No step to update `DataSource` import in `logical/types.rs` tests

Step 2 says to check if `DataSource` is still used elsewhere in `logical/types.rs` and correctly notes it is (in `Node::DataSource`). However, the plan does not note that the `use crate::common::types::{DataSource, VariableName};` import at line 2 of `types.rs` is used **both** in production code and in tests. Since `PhysicalPlanCreator::new()` no longer takes `DataSource`, the tests in `types.rs` still use `DataSource` directly for `Node::DataSource(...)` construction, so the import stays. This is fine, but the plan should explicitly call this out since it asks the implementer to "check if `DataSource` is used elsewhere" -- it is used extensively in the test module.

## Suggestions (nice to have)

### S1: Step 1a says "no test needed" but TDD rules require test-first

The project's CLAUDE.md rules say "Write tests BEFORE implementation (test-first)." Step 1 adds type aliases and error variants with no test. While a type alias is trivially correct, the error variants could have a simple test that constructs each variant and checks its Display output. This would validate the error messages are correct.

### S2: Dedup the table-spec parsing logic between `query` and `explain` subcommands

Step 6 duplicates the table-spec parsing loop for `query` (6b) and `explain` (6c). The design doc mentions they "share the same parsing logic" but the plan has them as separate copy-paste blocks. Extract a `parse_table_specs(values: clap::Values) -> Result<DataSourceRegistry, AppError>` function in `main.rs` to avoid duplication.

### S3: Test for stdin-in-right-side-of-join is missing from Step 8

The design doc's testing section (items 8-9) specifies tests for `StdinInJoinRightSide` errors, but Step 8's test code only covers cross join, left join, unknown table, and backward compatibility. There are no tests for the stdin constraint. Add tests that verify:
- `StdinInJoinRightSide` when stdin is the right side of a cross join
- `StdinInJoinRightSide` when stdin is the right side of a left join
- Duplicate stdin table names error

### S4: `explain` subcommand in Step 6c silently ignores bad table-spec strings

In Step 6c, if `TABLE_SPEC_REGEX.captures(table_spec_string)` returns `None`, the loop just continues without inserting anything. The `query` subcommand (Step 6b) correctly errors with `AppError::InvalidTableSpecString`. The `explain` path should do the same.

### S5: Step 6c does not validate `file_format` for `explain`

The `query` path validates that `file_format` is one of `elb/alb/squid/s3/jsonl`. The `explain` path in Step 6c does not, allowing invalid formats through.

### S6: Consider adding a test for different file formats across tables

The design doc's test plan (item 15) mentions "Join different file formats (JSONL + ELB)" but Step 8 only tests JSONL-to-JSONL joins. This is a meaningful integration test that should be included.

### S7: Step 7 should also note the bench-internals test in `logical/types.rs`

The `#[derive(PartialEq)]` on `PhysicalPlanCreator` includes `data_source`. After removing the field, the derive still works but changes semantics for equality comparisons in tests. The plan should note this.

## Verified Claims (things you confirmed are correct)

1. **File paths are accurate**: All referenced files (`src/common/types.rs`, `src/logical/parser.rs`, `src/logical/types.rs`, `src/app.rs`, `src/main.rs`, `src/cli.yml`, `src/lib.rs`, `benches/bench_execution.rs`) exist at the stated paths.

2. **`ParsingContext` structure is accurately described**: Lines 397-401 of `common/types.rs` match the plan's "before" snapshot exactly (fields: `table_name`, `data_source`, `registry`).

3. **`PhysicalPlanCreator` structure is accurately described**: Lines 394-413 of `logical/types.rs` match the plan's "before" snapshot (fields: `counter`, `data_source`).

4. **`data_source` field in `PhysicalPlanCreator` is indeed dead**: Confirmed -- the field is set in `new()` but never read anywhere in the codebase. Only `counter` and `new_constant_name()` are used.

5. **`check_env_ref` current implementation matches**: Lines 513-533 match the plan's description. The empty-check bug (checking emptiness after accessing `[0]`) is correctly identified in the design doc's changes-from-v2.

6. **`build_from_node` current signature and implementation match**: Lines 614-652 correctly match the plan's "before" description.

7. **`cli.yml` structure is correct**: The `table` arg exists under `query` without `multiple: true`, and `explain` has no `table` arg -- both as the plan states.

8. **`lib.rs` bench-internals exports `DataSource` but not `DataSourceRegistry`**: Line 43 confirms `DataSource` is exported but `DataSourceRegistry` would need to be added.

9. **`benches/bench_execution.rs` structure matches**: Lines 32-48 show the `data_source` construction and `run_to_records_with_registry` call that needs updating.

10. **Test commands are correct**: `cargo test`, `cargo check`, and `cargo check --features bench-internals` all work in this codebase and are the right commands.

11. **Dependency ordering is correct**: Steps 1-9 have valid dependency chains. Steps 6, 7, 8 can indeed run in parallel after Step 5 (they touch independent files).

12. **`TABLE_SPEC_REGEX` in `main.rs` correctly captures table name, format, and file path**: Line 17 confirms the regex pattern.

13. **The subquery site at line 345 of `parser.rs` exists**: Confirmed `ctx.data_source.clone()` at that line which needs to change to `ctx.data_sources.clone()`.

14. **9 `ParsingContext` constructions in parser.rs tests**: There are 9 occurrences in the test module (plus 1 in production code), matching the plan's claim about needing bulk updates.

15. **7 `parse_query(before, data_source, registry)` calls in parser.rs tests**: All 7 need their second argument changed from `DataSource` to `DataSourceRegistry`.
