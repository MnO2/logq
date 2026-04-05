VERDICT: NEEDS_REVISION

## Summary Assessment

The design correctly identifies the need to replace single-`DataSource` threading with a registry, but it omits critical details about how `check_env`, `build_from_node`, and `to_bindings_for_ref` must be rewritten to resolve per-table-reference data sources from the registry, and it does not address the fundamental problem that stdin can only be read once yet the current join executor re-opens data sources for every left row.

## Critical Issues (must fix)

### C1. `check_env` / `check_env_ref` rewrite is missing

The design says `build_from_node` looks up each table reference's base name in the registry, but it does not explain how `check_env` and `check_env_ref` must change. Today these functions validate that **every** FROM-clause table reference's first path segment equals the single `table_name`. With a registry, they must instead check that the first path segment exists as a **key** in the registry. The design's section 5 mentions `check_env` in passing ("validates that every table name in the FROM clause exists in the `DataSourceRegistry`") but does not show how this interacts with the existing `FromClausePathInvalidTableReference` error or the path-stripping logic in `to_bindings_for_ref` (line 543-572 of `logical/parser.rs`). This is the most load-bearing change in the logical planner and needs explicit treatment.

### C2. `build_from_node` must look up different `DataSource` per table reference

Currently `build_from_node` receives a single `&DataSource` and clones it for every `Node::DataSource(...)` it creates (lines 624, 628, 631, 640 of `logical/parser.rs`). With multi-table support, each table reference's base name must be used to look up its **own** `DataSource` from the registry. The design says this will happen but does not explain the mechanics: how does the function extract the base name from each `TableReference`? What happens when a table reference has no `AS` clause (currently valid for `FROM it`)? The existing code strips the table-name prefix in `to_bindings_for_ref` -- this must be coordinated with the registry lookup. Without this detail the implementer will likely get the bindings wrong or accidentally pass the wrong `DataSource` to a join leg.

### C3. Stdin data sources cannot be used in joins (or even twice)

The `CrossJoinStream` and `LeftJoinStream` implementations re-create the right-side stream for every left row by calling `right_node.get(...)`. When the right-side `DataSource` is `Stdin`, this reads from `io::stdin()` -- which can only be read once. Today this is not a real problem because both join sides use the same `DataSource` (self-join), but multi-table support opens the door to `--table a:jsonl=stdin --table b:jsonl=file.jsonl` or even two stdin tables. The design does not address this. At minimum:
- Two tables cannot both be `stdin` (must error).
- A stdin table used in a join's right side will silently produce empty results on the second iteration.
- This constraint should be validated at CLI parse time and documented.

### C4. `parse_query` still extracts `file_format` from a single `DataSource` for `is_match_group_by_fields`

At line 684-687 of `logical/parser.rs`, `parse_query` destructures the single `data_source` to get `file_format`, which is later used by `is_match_group_by_fields` (line 971) to expand `Star` into field names for structured log formats (ELB, ALB, etc.). With a registry, there is no single file format. The design does not address what happens with `SELECT *, count(*) FROM a GROUP BY ...` when `a` is an ELB log. The file format must come from the registry entry for the relevant table, not from a single global source.

### C5. `explain` subcommand is not updated

The design mentions updating `app::explain` to take `DataSourceRegistry`, but the current `explain` implementation (line 72-80 of `main.rs`) hardcodes `DataSource::Stdin("jsonl", "it")` and does not even accept a `--table` flag. The design's "Files Changed" table does not call this out, and the testing plan has no test for `explain` with multi-table. This will break or remain a single-table-only pathway.

## Suggestions (nice to have)

### S1. `PhysicalPlanCreator` does not need `DataSource` at all

The design's section 6 says to update `PhysicalPlanCreator` to hold the registry. However, grep of the codebase shows that `PhysicalPlanCreator.data_source` is **never read** -- it is a dead field. The `DataSource` is already embedded in each `Node::DataSource(...)` variant from the logical plan. The simplest fix is to remove the `data_source` field entirely rather than replacing it with a registry. This would also eliminate the unnecessary `data_source.clone()` in `app::run` and `app::explain`.

### S2. Consider case-sensitivity of table names

The `TABLE_SPEC_REGEX` captures table names as `[0-9a-zA-Z]+`, and the query parser may or may not fold case on identifiers. The design does not specify whether table name lookup is case-sensitive or case-insensitive. The FROM clause currently does case-sensitive comparison (`s.eq(table_name)` at line 516). If the SQL parser lowercases identifiers (check the syntax parser), then `--table MyTable:jsonl=...` with `SELECT * FROM mytable` would fail. This should be specified explicitly.

### S3. Backward compatibility for omitting `--table` or using positional args

Today, if no `--table` flag is provided, the error is `InvalidTableSpecString`. The design does not address whether a future default behavior (e.g., reading from stdin with a default table name `it`) should be preserved. Many existing usage patterns may rely on this.

### S4. Integration test for different file formats in a join

The testing plan includes cross-join and left-join tests for "two different files" but does not specify whether they test **different formats** (e.g., joining a JSONL file with an ELB log). This is a realistic use case and an important edge case because the two sides will have completely different schemas.

### S5. `Subquery` in `parse_value_expression` passes `ctx.data_source.clone()`

At line 345 of `logical/parser.rs`, subqueries use `ctx.data_source.clone()`. With a registry, the `ParsingContext` will need the full registry, and subqueries must be able to reference any table in the registry. The design mentions "Subqueries and set operations pass the full registry down" which is correct directionally, but the actual code site is worth calling out to the implementer.

### S6. The `run_to_records_with_registry` bench helper needs updating too

The "Files Changed" table omits `benches/bench_execution.rs` and the `bench_internals` module. The `run_to_records_with_registry` function at line 248 of `app.rs` takes a single `DataSource` and will need a registry. The benchmark won't compile after this change unless updated.

## Verified Claims (things you confirmed are correct)

1. **`ParsingContext` currently has `table_name` and `data_source` fields** -- confirmed at line 397-401 of `common/types.rs`. These are the fields the design proposes to replace.

2. **`parse_query_top` and `parse_query` currently receive a single `DataSource`** -- confirmed at lines 655 and 681 of `logical/parser.rs`. The design's claim about their signatures is accurate.

3. **`PhysicalPlanCreator` currently holds a single `DataSource`** -- confirmed at lines 395-405 of `logical/types.rs`.

4. **`cli.yml` currently lacks `multiple: true` on the `table` arg** -- confirmed at lines 13-16 of `cli.yml`. Adding `multiple: true` is the correct clap-yaml approach.

5. **Cross joins and left joins currently pass the same `data_source` to both sides** -- confirmed in `build_from_node` (lines 628-631 of `logical/parser.rs`). The design correctly identifies this as the site that needs per-table-reference lookup.

6. **All existing tests use a single `DataSource` with table name "it"** -- confirmed across all test helpers in `app.rs`.

7. **`TABLE_SPEC_REGEX` already parses the `name:format=path` syntax** -- confirmed at line 17 of `main.rs`. The regex is reusable for multi-value parsing.
