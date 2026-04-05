VERDICT: NEEDS_REVISION

## Summary Assessment

The v2 design is a substantial improvement over v1 -- it addresses all five critical issues and most suggestions from the first review with detailed code-level specifications. However, two new issues emerged from verifying the design against the actual codebase: (1) the `check_env_ref` rewrite has a logic bug where an empty-segments check runs after an index-0 access that would panic, and (2) the `file_format` extraction for `is_match_group_by_fields` will hit an existing `unreachable!()` panic for JSONL tables when `SELECT *` appears with `GROUP BY`.

## Critical Issues (must fix)

### C1. `check_env_ref` has a dead-code path ordering bug

In section 5b, the rewritten `check_env_ref` accesses `path_segments[0]` first (line 124 of the design), and then checks `path_expr.path_segments.is_empty()` afterward (line 138). If `path_segments` is empty, the index-0 access will panic before the empty check runs. The current code (line 513-533 of `logical/parser.rs`) has this same ordering issue, but it has never triggered because the SQL parser guarantees at least one path segment for any parsed identifier. The design should either:

- Remove the dead `is_empty()` check entirely (it can never be reached after a successful `[0]` access), or
- Reorder to check `is_empty()` first, return the error, then access `[0]`.

This is not a runtime risk today (the parser prevents it), but it is dead code that could mislead a reader and would be caught by a careful code reviewer. Mark it as a known inherited defect or fix it.

### C2. `is_match_group_by_fields` panics on JSONL format with `SELECT *` and `GROUP BY`

The design's section 5e correctly identifies that `file_format` must come from the primary table's `DataSource`. However, it does not address a pre-existing bug at line 1064 of `logical/parser.rs`: `is_match_group_by_fields` hits `unreachable!()` when `Named::Star` is encountered with `file_format == "jsonl"`. Today this is harmless because `SELECT *, count(*) FROM it GROUP BY ...` with a JSONL source always fails earlier at `GroupByFieldsMismatch` (JSONL has no fixed schema, so `Star` expansion is impossible for the field-matching check). But with multi-table support, a query like:

```sql
SELECT *, count(*) FROM elb_table, jsonl_table GROUP BY ...
```

would extract `file_format` from the primary (first) table. If the primary table is JSONL while `Star` appears in the non-aggregate list, it will panic. If the primary table is ELB, it will wrongly expand `*` using ELB fields even though a JSONL table is also in scope.

The design must specify how `Star` expansion interacts with multi-table scenarios. A reasonable approach: for multi-table queries, `SELECT *` with `GROUP BY` should be an error (since there is no single schema to expand into), or `Star` expansion should merge field names from all tables in scope.

## Suggestions (nice to have)

### S1. Table names that collide with SQL keywords cannot be used

The `identifier` parser (line 118-151 of `syntax/parser.rs`) rejects SQL keywords. This means a user cannot write `--table from:jsonl=f.jsonl` or `--table select:jsonl=f.jsonl` and then reference those tables in FROM clauses. The CLI regex allows these names (`[0-9a-zA-Z]+`), so a user could define them as `--table` flags but the query parser would reject them. The design should either:

- Restrict `TABLE_SPEC_REGEX` to reject keyword names at CLI parse time with a clear error, or
- Document this as a known limitation.

### S2. Consider the `to_bindings_for_ref` behavior with registry-based path stripping

In the v2 design's `to_bindings_for_ref` (section 5c), when the first path segment is a known registry key it gets stripped. The else branch (`data_sources.contains_key(s)` is false) preserves the full path. But by this point, `check_env_ref` has already validated that the first segment IS a registry key. So the else branch is dead code. This is not harmful but could be simplified: since we know the first segment is always a valid table name at this point, the conditional is unnecessary -- always strip. This would make the code clearer and slightly faster.

### S3. Stdin constraint #2 is not fully specified for implicit cross joins

Section 8 says "Stdin cannot be the right side of a join" and this is enforced in `build_from_node`. For explicit JOINs, the right side is clear. But for implicit cross joins (`FROM a, b, c`), the code in `build_from_node` builds a left-associative tree: `CrossJoin(CrossJoin(a, b), c)`. Here `b` is the right side of the first join and `c` is the right side of the second. The design correctly handles this by checking each right-side `DataSource` in the loop. However, if the stdin table appears as the *first* table (`a`) and a file table appears as `b`, this should also fail because the stdin table would be re-read for every row of a self-join. Wait -- actually this is fine because the first table (left side) is only read once sequentially. The design is correct here. But it should note that `FROM stdin_table` as the first table in a multi-table list is fine since it is read only once (left side streams sequentially), while `FROM file_table, stdin_table` correctly errors because stdin is the right side.

### S4. The `explain` subcommand lacks a `--table` flag in `cli.yml`

The design mentions updating `explain` to accept `--table` flags, but the "Files Changed" table lists `cli.yml` only once with "Add `multiple: true` to `table` arg" -- this only affects the `query` subcommand. The `explain` subcommand in `cli.yml` (lines 21-25) currently has no `table` arg at all. The design needs to also add the `table` arg to the `explain` subcommand in `cli.yml`, or explicitly state that `explain` inherits it through shared arg definitions (which clap-yaml does not do -- each subcommand needs its own args).

### S5. Error message determinism for `UnknownTable`

The `UnknownTable` error formats available table names as `data_sources.keys().cloned().collect::<Vec<_>>().join(", ")`. Since `HashMap` iteration order is non-deterministic, the error message listing available tables will show them in arbitrary order across runs. Consider sorting the keys before joining, for consistent and testable error messages.

### S6. Test plan is missing a negative test for keyword table names

The test plan should include a test verifying that `--table from:jsonl=f.jsonl` with `SELECT * FROM from` produces a clear error (either at CLI parse or SQL parse), rather than a confusing parse failure.

## Verified Claims (things I confirmed are correct)

1. **`PhysicalPlanCreator.data_source` is a dead field** -- Confirmed. Searched all usages of `physical_plan_creator.data_source` across the codebase: zero reads. The field is written at construction (`PhysicalPlanCreator::new(data_source)`) but never accessed. The design's S1 fix (remove it entirely) is correct and safe.

2. **`identifier` parser preserves case** -- Confirmed at line 118-151 of `syntax/parser.rs`. The function returns the original `&str` slice without any case folding. Table name lookup will therefore be case-sensitive, and the design's S2 claim about `--table MyTable:jsonl=f.jsonl` with `FROM mytable` producing `UnknownTable` is accurate.

3. **`check_env` and `check_env_ref` currently compare against a single `table_name` string** -- Confirmed at lines 513-541 of `logical/parser.rs`. The rewrite to use `data_sources.contains_key(s)` is the correct mechanical transformation.

4. **`build_from_node` currently clones the same `data_source` for every join leg** -- Confirmed at lines 614-652 of `logical/parser.rs`. Lines 624, 628, 631, and 640 all use `data_source.clone()`. The design's per-table `lookup_data_source` approach correctly replaces this.

5. **Subquery at line 345 uses `ctx.data_source.clone()`** -- Confirmed. The design's fix to use `ctx.data_sources.clone()` (giving the subquery access to the full registry) is correct for non-correlated subqueries.

6. **`CrossJoinStream` and `LeftJoinStream` re-create the right stream per left row** -- Confirmed at lines 736-787 and 789-870 of `execution/stream.rs`. The `right_node.get(...)` call at line 775 re-opens the data source. If the right side is stdin, it would silently return empty on the second read. The design's `StdinInJoinRightSide` error correctly prevents this.

7. **`collect_table_references` correctly traverses `FromClause::Join` recursively** -- Confirmed at lines 46-55 of `syntax/ast.rs`. The left side is recursed, the right `TableReference` is appended. This means `check_env` will validate all table references in nested join structures.

8. **The `cli.yml` `table` arg currently lacks `multiple: true`** -- Confirmed at lines 13-16 of `cli.yml`. Adding `multiple: true` is the correct clap-yaml mechanism.

9. **The `explain` subcommand hardcodes `DataSource::Stdin("jsonl", "it")`** -- Confirmed at line 74 of `main.rs`. The design's plan to make it use `--table` with a fallback default is necessary.

10. **Bench file `bench_execution.rs` uses `run_to_records_with_registry` with a single `DataSource`** -- Confirmed at lines 40-44 of `benches/bench_execution.rs`. It will need updating but this is straightforward.
