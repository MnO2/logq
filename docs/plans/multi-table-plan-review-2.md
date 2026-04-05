VERDICT: NEEDS_REVISION

## Summary Assessment

The v2 plan successfully addresses nearly all critical issues from the first review (C1-C6 and S2-S5), with correct step splitting, accurate call site counts, and proper deduplication. However, there is a test count inconsistency, missing tests from the design doc's testing plan, and a task sizing issue that require revision.

## Critical Issues (must fix)

### C1: Step 5C header says "24 test functions" but lists 26

The opening paragraph of Step 5C states: "There are 24 test functions that directly construct DataSource and pass it to run()/run_to_vec()." The numbered list below it enumerates 26 tests (items 1-26). By my independent grep count, 26 is the correct number. The text should say "26" to match both the list and the codebase.

This matters because an implementer trusting the "24" count may stop searching after finding 24 tests, leaving 2 tests un-updated and causing compilation failures.

### C2: Missing tests from the design doc's testing plan

The design doc's testing section (items 1-18) specifies several tests that are not included in Step 8:

1. **Table name case mismatch** (design doc item 12): `--table MyT:jsonl=f.jsonl` with `FROM myt` should error with `UnknownTable`. This is an important edge case given the decision to use case-sensitive lookup.

2. **Join different file formats** (design doc item 15): Cross-format joins like `JSONL + ELB` are a key multi-table use case that should be tested.

3. **Duplicate table name error**: The `parse_table_specs` helper in Step 6b checks for duplicate table names, but no test verifies this behavior.

4. **Multiple stdin tables error**: The S3 header in the plan mentions "duplicate stdin error" but the actual test code in Step 8e does not include a test for `--table a:jsonl=stdin --table b:jsonl=stdin`.

Without these tests, regressions or untested code paths could slip through.

### C3: Step 8 new tests share `app.rs` with Step 5C -- file conflict in dependency table

The dependency table (bottom of the plan) says Step 8 depends on Step 5B and notes "except Step 8 shares app.rs with 5C but only adds new tests." This is risky: if Step 5C and Step 8 are executed by different agents or in parallel, they will have merge conflicts in `app.rs` since both modify the `mod tests` block. The dependency table should mark Step 8 as depending on Step 5C (not just 5B), or the text should explicitly state that Steps 5C and 8 must be sequential.

## Suggestions (nice to have)

### S1: Step 4B-f's guard location may cause false rejections for single-table structured formats

The Star+GroupBy guard at step 4B-f checks:
```rust
if non_aggregates.iter().any(|n| matches!(n, types::Named::Star)) {
    if data_sources.len() > 1 || file_format == "jsonl" {
        return Err(ParseError::StarGroupByUnsupported);
    }
}
```

This guard is applied before the `is_match_group_by_fields` call. For single-table structured formats (elb, alb, squid, s3), Star+GroupBy is valid and already handled correctly by `is_match_group_by_fields`. The guard correctly passes those through (since `data_sources.len() == 1` and `file_format != "jsonl"`). However, the comment in the plan should make this clear to avoid confusion during implementation.

### S2: Consider adding a `#[test] fn test_explain_with_table_flags()` in Step 8

The design doc's testing plan item 16 specifies that `explain` with `--table` flags should work. This is currently only testable via the CLI (Step 9c manual test). A unit test in `app.rs` could call `app::explain()` with a multi-table registry to verify the explain path works with the new signature, even if it can't test CLI flag parsing.

### S3: The `is_match_group_by_fields` unreachable fix at line 1064 should have a test

Step 4B-f replaces the `unreachable!()` with `return false;` for the jsonl Star case, but the only test coverage for this comes through the new guard in `parse_query` which returns `StarGroupByUnsupported` before `is_match_group_by_fields` is ever called with jsonl+Star. Consider adding a direct unit test for `is_match_group_by_fields` with `file_format = "jsonl"` and `Named::Star` to verify the fix works even if the guard is later removed.

## Verified Claims (things I confirmed are correct)

1. **C1 from review 1 is fixed**: The plan correctly identifies 5 `PhysicalPlanCreator::new(data_source)` call sites in `app.rs` (lines 106, 123, 208, 234, 260) and 4 in `logical/types.rs` tests (lines 689, 708, 752, 846). These match the actual codebase exactly.

2. **C2 from review 1 is fixed**: Step 4 is properly split into 4A (check_env/to_bindings rewrites), 4B (helpers + build_from_node + parse_query), and 4C (test updates). Each sub-step is a reasonable size.

3. **C3 from review 1 is fixed**: Tests in `app.rs` are enumerated. The actual list of 26 tests is complete, though the header count is wrong (says 24).

4. **C4 from review 1 is fixed**: The `unreachable!()` in `is_match_group_by_fields` for jsonl is addressed both by replacing it with `return false` (line 1064 fix) and by adding a guard in `parse_query` to prevent reaching it.

5. **C5 from review 1 is fixed**: `build_from_node` uses `ctx.data_sources` internally instead of receiving a separate `data_sources` parameter. The plan's signature is `fn build_from_node(ctx: &common::ParsingContext, from_clause: &FromClause)` with only 2 params.

6. **C6 from review 1 is addressed**: The plan explicitly notes that the `DataSource` import in `logical/types.rs` must stay because it is used in `Node::DataSource` and test assertions.

7. **S2 from review 1 is addressed**: `parse_table_specs` helper is extracted in Step 6b, shared between `query` and `explain`.

8. **S3 from review 1 is addressed**: Stdin constraint tests are added in Step 8e for both cross join and left join right-side errors.

9. **S4/S5 from review 1 are addressed**: The `explain` path uses the shared `parse_table_specs` helper, which validates format and regex.

10. **9 ParsingContext constructions in parser.rs tests**: Confirmed by grep -- 9 test instances plus 1 production instance at line 691. The plan's count is correct.

11. **10 `parse_query()` calls in parser.rs tests**: Confirmed -- lines 1273, 1328, 1411, 1447, 1490, 1673, 1709, 1741, 1797, 1858. The plan's count is correct.

12. **File paths are accurate**: All referenced files exist. The clap YAML structure matches the plan's "before" state. `cli.yml` has no `table` arg under `explain` and no `multiple: true` under `query` -- both as stated.

13. **clap 2.33 API compatibility**: `values_of("table")` returning `Option<Values<'_>>` is correct for clap 2.x with `multiple: true`. The `parse_table_specs` function signature using `clap::Values` is valid.

14. **Dependency ordering is correct**: Steps 1 through 9 have valid dependency chains. The parallel groups (6, 7 after 5A; 8 after 5B) touch largely independent files, with the caveat noted in C3.

15. **Test commands are correct**: `cargo test`, `cargo check`, and `cargo check --features bench-internals` are all valid for this codebase.

16. **Design doc's error variant names diverge intentionally**: The design doc uses `StarGroupByMultiTable` while the plan uses `StarGroupByUnsupported`. The plan's variant is more general (covering both multi-table and single-table jsonl), which is the correct behavior given the combined guard logic.

17. **Backward compatibility is preserved**: Single-table usage creates a one-entry `DataSourceRegistry`. All existing test patterns wrap their `DataSource` in a registry with the same table name. The `query` subcommand still errors when no `--table` flag is provided.

18. **Benchmark query table names match**: The Tier A queries in `benches/helpers/queries.rs` use `FROM elb` (e.g., `EXEC_E1 = "SELECT * FROM elb LIMIT 10"`), which matches the plan's Step 7a registry key `"elb"`. No mismatch risk.
