# Plan: Multi-Table CLI Support (v2)

**Goal**: Support multiple `--table` flags so queries can join data from different files (e.g., `--table a:jsonl=d1.jsonl --table b:jsonl=d2.jsonl`).
**Architecture**: Replace single `DataSource` with `DataSourceRegistry` (`HashMap<String, DataSource>`) throughout parser/planner/app layers. Per-table lookup in `build_from_node`.
**Tech Stack**: Rust, clap (YAML), nom, criterion (benchmarks)
**Design Doc**: `docs/plans/2026-04-05-multi-table-design-final.md`

---

## Changes from previous version

| Review item | What changed in v2 |
|-------------|---------------------|
| **C1** | Fixed `PhysicalPlanCreator::new()` call site count in `app.rs` from 4 to 5 (lines 106, 123, 208, 234, 260). Added the 4 test call sites in `logical/types.rs` (lines 689, 708, 752, 846) to Step 2. |
| **C2** | Split Step 4 into three sub-tasks: 4A (check_env/to_bindings rewrites), 4B (helpers + build_from_node + parse_query rewrites), 4C (test updates). Each is independently compilable (with expected errors in later steps). |
| **C3** | Enumerated all 24 tests in `app.rs` that directly construct `DataSource` and pass it to `run()`/`run_to_vec()`. Split Step 5 into 5A (function signatures), 5B (test helpers), 5C (individual test updates). |
| **C4** | Added explicit handling for the pre-existing `unreachable!()` in `is_match_group_by_fields` for `jsonl` format. The `Star+GroupBy` guard now covers all formats including single-table `jsonl` queries (returns `false` for `jsonl` since there is no fixed schema to expand `*` into). |
| **C5** | Removed redundant `data_sources` parameter from `build_from_node`. It now uses `ctx.data_sources` internally instead of receiving a separate reference. |
| **C6** | Explicitly noted that the `DataSource` import in `logical/types.rs` stays because it is used in `Node::DataSource` and in test assertions. |
| **S2** | Extracted shared `parse_table_specs` helper in `main.rs` for `query` and `explain` subcommands, eliminating copy-paste duplication. |
| **S3** | Added stdin constraint tests to Step 8: `StdinInJoinRightSide` for cross join and left join, and duplicate stdin error. |
| **S4/S5** | `explain` path now validates table specs (format check, regex match) identically to `query` via the shared `parse_table_specs` helper. |

---

## Step 1: Add `DataSourceRegistry` type alias and new error variants

**File**: `src/common/types.rs`

### 1a. Write failing test

No test needed -- this is a type alias and error variants that will not compile until used.

### 1b. Write implementation

At the top of `src/common/types.rs`, add the `HashMap` import and the type alias after the existing `DataSource` enum (after line 416):

```rust
use std::collections::HashMap;
// ... (existing code)

pub type DataSourceRegistry = HashMap<String, DataSource>;
```

**File**: `src/logical/parser.rs`

Add three new error variants to `ParseError` enum (after `FromClauseMissingAsForPathExpr` at line 32):

```rust
#[error("Unknown table '{0}'. Available tables: {1}")]
UnknownTable(String, String),
#[error("Stdin cannot be used as the right side of a join (it can only be read once)")]
StdinInJoinRightSide,
#[error("SELECT * with GROUP BY is not supported for jsonl or multi-table queries (no fixed schema to expand)")]
StarGroupByUnsupported,
```

### 1c. Verify compilation

```bash
cargo check
```

---

## Step 2: Remove dead `PhysicalPlanCreator.data_source` field

**File**: `src/logical/types.rs`

### 2a. Write implementation

Change `PhysicalPlanCreator` (lines 394-413) from:

```rust
pub(crate) struct PhysicalPlanCreator {
    counter: u32,
    data_source: DataSource,
}

impl PhysicalPlanCreator {
    pub(crate) fn new(data_source: DataSource) -> Self {
        PhysicalPlanCreator {
            counter: 0,
            data_source,
        }
    }
```

to:

```rust
pub(crate) struct PhysicalPlanCreator {
    counter: u32,
}

impl PhysicalPlanCreator {
    pub(crate) fn new() -> Self {
        PhysicalPlanCreator {
            counter: 0,
        }
    }
```

**Note on `DataSource` import**: The `use crate::common::types::{DataSource, VariableName};` import at line 2 of `types.rs` must stay. `DataSource` is used in `Node::DataSource(DataSource, Vec<common::Binding>)` (line 20) and extensively in test assertions. Do not remove it.

**File**: `src/app.rs`

Update all 5 `PhysicalPlanCreator::new(data_source)` calls to `PhysicalPlanCreator::new()`:
- Line 106: in `explain()`
- Line 123: in `run()`
- Line 208: in `run_to_vec()`
- Line 234: in `run_to_records()`
- Line 260: in `run_to_records_with_registry()`

**File**: `src/logical/types.rs` (tests)

Update all 4 test call sites to `PhysicalPlanCreator::new()`:
- Line 689: in `test_formula_gen_physical()`
- Line 708: in `test_expression_gen_physical()`
- Line 752: in `test_filter_with_map_gen_physical()`
- Line 846: in `test_group_by_gen_physical()`

### 2b. Verify

```bash
cargo test
```

---

## Step 3: Update `ParsingContext` to hold `DataSourceRegistry`

**File**: `src/common/types.rs`

### 3a. Write implementation

Change `ParsingContext` (lines 397-401) from:

```rust
pub(crate) struct ParsingContext {
    pub(crate) table_name: String,
    pub(crate) data_source: DataSource,
    pub(crate) registry: Arc<FunctionRegistry>,
}
```

to:

```rust
pub(crate) struct ParsingContext {
    pub(crate) data_sources: DataSourceRegistry,
    pub(crate) registry: Arc<FunctionRegistry>,
}
```

Update `Debug` impl (lines 403-410) to remove `table_name` and `data_source`, add `data_sources`:

```rust
impl std::fmt::Debug for ParsingContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParsingContext")
            .field("data_sources", &self.data_sources)
            .finish()
    }
}
```

This will cause compilation errors in `logical/parser.rs` -- expected, fixed in Step 4.

---

## Step 4A: Rewrite `check_env`/`check_env_ref` and `to_bindings`/`to_bindings_for_ref`

**File**: `src/logical/parser.rs`

### 4A-a. Rewrite `check_env_ref` (lines 513-533)

From:
```rust
fn check_env_ref(table_name: &String, table_reference: &TableReference) -> ParseResult<()> {
```

To:
```rust
fn check_env_ref(data_sources: &common::DataSourceRegistry, table_reference: &TableReference) -> ParseResult<()> {
    let path_expr = &table_reference.path_expr;
    if path_expr.path_segments.is_empty() {
        return Err(ParseError::FromClausePathInvalidTableReference);
    }

    match &path_expr.path_segments[0] {
        PathSegment::AttrName(s) => {
            if !data_sources.contains_key(s) {
                let mut available: Vec<_> = data_sources.keys().cloned().collect();
                available.sort();
                return Err(ParseError::UnknownTable(s.clone(), available.join(", ")));
            }
        }
        _ => return Err(ParseError::FromClausePathInvalidTableReference),
    }

    if path_expr.path_segments.len() > 1 && table_reference.as_clause.is_none() {
        return Err(ParseError::FromClauseMissingAsForPathExpr);
    }
    Ok(())
}
```

### 4A-b. Rewrite `check_env` (lines 535-541)

From:
```rust
fn check_env(table_name: &String, from_clause: &FromClause) -> ParseResult<()> {
```

To:
```rust
fn check_env(data_sources: &common::DataSourceRegistry, from_clause: &FromClause) -> ParseResult<()> {
    let refs = from_clause.collect_table_references();
    for table_reference in refs {
        check_env_ref(data_sources, table_reference)?;
    }
    Ok(())
}
```

### 4A-c. Rewrite `to_bindings_for_ref` (lines 543-572)

Change signature from `table_name: &String` to `data_sources: &common::DataSourceRegistry`:

```rust
fn to_bindings_for_ref(data_sources: &common::DataSourceRegistry, table_reference: &TableReference) -> Vec<common::Binding> {
    let path_expr = match &table_reference.path_expr.path_segments[0] {
        PathSegment::AttrName(s) => {
            if data_sources.contains_key(s) {
                PathExpr::new(
                    table_reference.path_expr.path_segments.iter().skip(1).cloned().collect(),
                )
            } else {
                table_reference.path_expr.clone()
            }
        }
        _ => table_reference.path_expr.clone(),
    };

    if let Some(name) = table_reference.as_clause.clone() {
        vec![common::Binding {
            path_expr,
            name,
            idx_name: table_reference.at_clause.clone(),
        }]
    } else {
        vec![]
    }
}
```

### 4A-d. Rewrite `to_bindings` (lines 574-579)

```rust
fn to_bindings(data_sources: &common::DataSourceRegistry, table_references: &Vec<TableReference>) -> Vec<common::Binding> {
    table_references
        .iter()
        .flat_map(|table_reference| to_bindings_for_ref(data_sources, table_reference))
        .collect()
}
```

---

## Step 4B: Add helpers + rewrite `build_from_node` + `parse_query_top` + `parse_query`

**File**: `src/logical/parser.rs`

### 4B-a. Add `lookup_data_source` helper

Add new function before `build_from_node`:

```rust
fn lookup_data_source(
    data_sources: &common::DataSourceRegistry,
    table_ref: &TableReference,
) -> ParseResult<common::DataSource> {
    match &table_ref.path_expr.path_segments[0] {
        PathSegment::AttrName(name) => {
            data_sources.get(name).cloned().ok_or_else(|| {
                let mut available: Vec<_> = data_sources.keys().cloned().collect();
                available.sort();
                ParseError::UnknownTable(name.clone(), available.join(", "))
            })
        }
        _ => Err(ParseError::FromClausePathInvalidTableReference),
    }
}
```

### 4B-b. Add `extract_base_name` helper

```rust
fn extract_base_name(table_ref: &TableReference) -> ParseResult<String> {
    match &table_ref.path_expr.path_segments[0] {
        PathSegment::AttrName(s) => Ok(s.clone()),
        _ => Err(ParseError::FromClausePathInvalidTableReference),
    }
}
```

### 4B-c. Rewrite `build_from_node` (lines 614-652)

Remove the separate `data_source: &common::DataSource` and `table_name: &String` parameters. Use `ctx.data_sources` instead (addressing C5 -- no redundant parameter):

```rust
fn build_from_node(
    ctx: &common::ParsingContext,
    from_clause: &FromClause,
) -> ParseResult<types::Node> {
    match from_clause {
        FromClause::Tables(table_references) => {
            if table_references.len() == 1 {
                let ref0 = &table_references[0];
                let ds = lookup_data_source(&ctx.data_sources, ref0)?;
                let bindings = to_bindings_for_ref(&ctx.data_sources, ref0);
                Ok(types::Node::DataSource(ds, bindings))
            } else {
                let ref0 = &table_references[0];
                let ds0 = lookup_data_source(&ctx.data_sources, ref0)?;
                let first_bindings = to_bindings_for_ref(&ctx.data_sources, ref0);
                let mut node = types::Node::DataSource(ds0, first_bindings);
                for table_ref in table_references.iter().skip(1) {
                    let ds = lookup_data_source(&ctx.data_sources, table_ref)?;
                    // Check stdin constraint for right side of implicit cross join
                    if matches!(&ds, common::DataSource::Stdin(..)) {
                        return Err(ParseError::StdinInJoinRightSide);
                    }
                    let ref_bindings = to_bindings_for_ref(&ctx.data_sources, table_ref);
                    let right = types::Node::DataSource(ds, ref_bindings);
                    node = types::Node::CrossJoin(Box::new(node), Box::new(right));
                }
                Ok(node)
            }
        }
        FromClause::Join { left, right, join_type, condition } => {
            let left_node = build_from_node(ctx, left)?;
            let ds_right = lookup_data_source(&ctx.data_sources, right)?;
            // Check stdin constraint for right side of explicit join
            if matches!(&ds_right, common::DataSource::Stdin(..)) {
                return Err(ParseError::StdinInJoinRightSide);
            }
            let right_bindings = to_bindings_for_ref(&ctx.data_sources, right);
            let right_node = types::Node::DataSource(ds_right, right_bindings);
            match join_type {
                JoinType::Cross => {
                    Ok(types::Node::CrossJoin(Box::new(left_node), Box::new(right_node)))
                }
                JoinType::Left => {
                    let on_expr = condition.as_ref().expect("LEFT JOIN requires ON condition");
                    let formula = parse_logic(ctx, on_expr)?;
                    Ok(types::Node::LeftJoin(Box::new(left_node), Box::new(right_node), formula))
                }
            }
        }
    }
}
```

### 4B-d. Rewrite `parse_query_top` (line 655)

Change signature from `data_source: common::DataSource` to `data_sources: common::DataSourceRegistry`:

```rust
pub(crate) fn parse_query_top(q: ast::Query, data_sources: common::DataSourceRegistry, registry: Arc<FunctionRegistry>) -> ParseResult<types::Node> {
    match q {
        ast::Query::Select(stmt) => parse_query(stmt, data_sources, registry),
        ast::Query::SetOp { op, all, left, right } => {
            let left_node = parse_query_top(*left, data_sources.clone(), registry.clone())?;
            let right_node = parse_query_top(*right, data_sources, registry)?;
            // ... rest unchanged
```

### 4B-e. Rewrite `parse_query` (line 681)

Change signature from `data_source: common::DataSource` to `data_sources: common::DataSourceRegistry`.

Replace lines 684-697:

```rust
pub(crate) fn parse_query(query: ast::SelectStatement, data_sources: common::DataSourceRegistry, registry: Arc<FunctionRegistry>) -> ParseResult<types::Node> {
    let from_clause = &query.from_clause;

    check_env(&data_sources, from_clause)?;

    // Extract file_format from the primary (leftmost) table
    let primary_table_name = {
        fn leftmost_name(fc: &FromClause) -> ParseResult<String> {
            match fc {
                FromClause::Tables(refs) => extract_base_name(&refs[0]),
                FromClause::Join { left, .. } => leftmost_name(left),
            }
        }
        leftmost_name(from_clause)?
    };
    let primary_ds = data_sources.get(&primary_table_name).unwrap();
    let file_format = match primary_ds {
        common::DataSource::File(_, fmt, _) => fmt.clone(),
        common::DataSource::Stdin(fmt, _) => fmt.clone(),
    };

    let parsing_context = common::ParsingContext {
        data_sources: data_sources.clone(),
        registry: registry.clone(),
    };

    let mut root = build_from_node(&parsing_context, from_clause)?;
    // ... rest of function unchanged until is_match_group_by_fields call
```

### 4B-f. Fix `is_match_group_by_fields` for `jsonl` format (C4)

The existing `is_match_group_by_fields` function (line 1025) has `unreachable!()` in the `Named::Star` arm for `jsonl` format (line 1064). This is a pre-existing bug: `SELECT *, count(*) FROM it GROUP BY x` with a single JSONL table will panic.

Fix by replacing the `unreachable!()` at line 1064 with:

```rust
} else {
    // jsonl has no fixed schema -- cannot expand Star.
    // Return false so parse_query reports GroupByFieldsMismatch.
    return false;
}
```

Additionally, add a guard in `parse_query` before the `is_match_group_by_fields` call (at approximately line 971). This guard covers both multi-table and single-table jsonl cases:

```rust
// SELECT * with GROUP BY is not supported for jsonl or multi-table queries
// because there is no fixed schema to expand Star into field names.
if non_aggregates.iter().any(|n| matches!(n, types::Named::Star)) {
    if data_sources.len() > 1 || file_format == "jsonl" {
        return Err(ParseError::StarGroupByUnsupported);
    }
}
```

### 4B-g. Update subquery site (line 345)

Change `ctx.data_source.clone()` to `ctx.data_sources.clone()`:

```rust
ast::Expression::Subquery(stmt) => {
    let inner_node = parse_query(*stmt.clone(), ctx.data_sources.clone(), ctx.registry.clone())?;
```

---

## Step 4C: Update all test `ParsingContext` constructions and `parse_query` calls in tests

**File**: `src/logical/parser.rs`

### 4C-a. Update `ParsingContext` constructions in tests

There are 9 test `ParsingContext` constructions (plus 1 in production code already updated in 4B-e). Each must change from:

```rust
let parsing_context = ParsingContext {
    table_name: "a".to_string(),
    data_source: common::DataSource::Stdin("jsonl".to_string(), "a".to_string()),
    registry: registry.clone(),
};
```

to:

```rust
let data_source = common::DataSource::Stdin("jsonl".to_string(), "a".to_string());
let mut data_sources = common::DataSourceRegistry::new();
data_sources.insert("a".to_string(), data_source);
let parsing_context = ParsingContext {
    data_sources,
    registry: registry.clone(),
};
```

Test functions requiring this change (9 total):
1. `test_parse_logic_expression` (lines 1087, 1111 -- two constructions)
2. `test_parse_value_expression` (line 1149)
3. `test_parse_aggregate` (line 1177)
4. `test_parse_condition` (line 1201)
5. `test_parse_logic_func_call_in_boolean_context` (line 1508)
6. `test_parse_logic_column_in_boolean_context` (line 1536)
7. `test_parse_logic_case_when_in_boolean_context` (line 1574)
8. `test_parse_condition_non_binary_expression` (line 1602)

For tests using table name `"it"`, use the same pattern with `"it"` instead of `"a"`.

### 4C-b. Update `parse_query` calls in tests

There are 10 `parse_query()` calls in tests that pass a single `DataSource` -- each must wrap it in a `DataSourceRegistry`:

```rust
// Before:
let data_source = common::DataSource::Stdin("jsonl".to_string(), "it".to_string());
let ans = parse_query(before, data_source, registry).unwrap();

// After:
let data_source = common::DataSource::Stdin("jsonl".to_string(), "it".to_string());
let mut data_sources = common::DataSourceRegistry::new();
data_sources.insert("it".to_string(), data_source);
let ans = parse_query(before, data_sources, registry).unwrap();
```

Test functions requiring this change:
1. `test_parse_query_with_simple_select_where_with_bindings` (line 1273)
2. `test_parse_query_with_simple_select_where_no_from_clause_bindings` (line 1328)
3. `test_parse_query_with_group_by` (line 1411)
4. `test_parse_query_group_by_without_aggregate` (line 1447)
5. `test_parse_query_group_by_mismatch_fields` (line 1490)
6. `test_parse_query_select_value_expression` (line 1673)
7. `test_parse_query_select_value_literal` (line 1709)
8. `test_parse_query_cross_join_produces_cross_join_node` (line 1741)
9. `test_parse_query_single_from_no_cross_join` (line 1797)
10. `test_parse_query_left_join_produces_left_join_node` (line 1858)

### 4C-c. Verify

```bash
cargo test
```

---

## Step 5A: Update `app.rs` function signatures

**File**: `src/app.rs`

### 5A-a. Update `explain` (line 97)

```rust
pub fn explain(query_str: &str, data_sources: common::types::DataSourceRegistry) -> AppResult<()> {
```

Update body -- remove `data_source.clone()` from `parse_query_top` call, pass `data_sources`:
```rust
    let node = logical::parser::parse_query_top(q, data_sources, registry)?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new();
```

### 5A-b. Update `run` (line 114)

```rust
pub fn run(query_str: &str, data_sources: common::types::DataSourceRegistry, output_mode: OutputMode) -> AppResult<()> {
```

Update body:
```rust
    let node = logical::parser::parse_query_top(q, data_sources, registry.clone())?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new();
```

### 5A-c. Update `run_to_vec` (line 196)

```rust
pub(crate) fn run_to_vec(
    query_str: &str,
    data_sources: common::types::DataSourceRegistry,
) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
```

Update body:
```rust
    let node = logical::parser::parse_query_top(q, data_sources, registry.clone())?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new();
```

### 5A-d. Update `run_to_records` (line 222)

```rust
pub fn run_to_records(
    query_str: &str,
    data_sources: common::types::DataSourceRegistry,
) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
```

Update body:
```rust
    let node = logical::parser::parse_query_top(q, data_sources, registry.clone())?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new();
```

### 5A-e. Update `run_to_records_with_registry` (line 248)

```rust
pub fn run_to_records_with_registry(
    query_str: &str,
    data_sources: common::types::DataSourceRegistry,
    registry: Arc<functions::FunctionRegistry>,
) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
```

Update body:
```rust
    let node = logical::parser::parse_query_top(q, data_sources, registry.clone())?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new();
```

---

## Step 5B: Update test helpers in `app.rs`

**File**: `src/app.rs`

### 5B-a. Update `run_format_query` (line 280)

```rust
fn run_format_query(format: &str, lines: &[&str], query: &str) -> AppResult<()> {
    // ... file setup unchanged ...
    let data_source =
        common::types::DataSource::File(file_path, format.to_string(), "it".to_string());
    let mut data_sources = common::types::DataSourceRegistry::new();
    data_sources.insert("it".to_string(), data_source);
    let result = run(query, data_sources, OutputMode::Csv);
    dir.close().unwrap();
    result
}
```

### 5B-b. Update `run_format_query_to_vec` (line 297)

```rust
fn run_format_query_to_vec(
    format: &str,
    lines: &[&str],
    query: &str,
) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
    // ... file setup unchanged ...
    let data_source =
        common::types::DataSource::File(file_path, format.to_string(), "it".to_string());
    let mut data_sources = common::types::DataSourceRegistry::new();
    data_sources.insert("it".to_string(), data_source);
    let result = run_to_vec(query, data_sources);
    dir.close().unwrap();
    result
}
```

---

## Step 5C: Update individual test functions in `app.rs`

**File**: `src/app.rs`

There are 24 test functions that directly construct `DataSource` and pass it to `run()`/`run_to_vec()`. Each requires wrapping the `DataSource` in a one-entry `DataSourceRegistry`.

The pattern for each test is:

```rust
// Before:
let data_source = common::types::DataSource::File(file_path, file_format.clone(), table_name.clone());
let result = run(query, data_source.clone(), OutputMode::Csv);

// After:
let data_source = common::types::DataSource::File(file_path, file_format.clone(), table_name.clone());
let mut data_sources = common::types::DataSourceRegistry::new();
data_sources.insert(table_name.clone(), data_source);
let result = run(query, data_sources.clone(), OutputMode::Csv);
```

Complete list of tests to update:

1. `test_run_explain_mode` (line 319) -- 1 `run()` call
2. `test_run_real_flat_log` (line 339) -- 4 `run()` calls using `data_source.clone()`
3. `test_run_real_jsonl_log` (line 382) -- 3 `run()` calls using `data_source.clone()`
4. `test_run_cross_join_jsonl` (line 422) -- 1 `run()` call
5. `test_run_cross_join_comma_syntax` (line 447) -- 1 `run()` call
6. `test_run_cross_join_with_where` (line 472) -- 1 `run()` call
7. `test_run_left_join_jsonl` (line 498) -- 1 `run()` call
8. `test_run_left_join_no_match` (line 524) -- 1 `run()` call
9. `test_run_left_outer_join_jsonl` (line 549) -- 1 `run()` call
10. `test_run_subquery_in_where` (line 574) -- 1 `run()` call
11. `test_run_subquery_in_select` (line 600) -- 1 `run()` call
12. `test_run_union` (line 625) -- 2 `run()` calls using `data_source.clone()`
13. `test_run_intersect` (line 659) -- 1 `run()` call
14. `test_run_except` (line 684) -- 1 `run()` call
15. `test_integration_mixed_case_keywords` (line 711) -- 3 `run()` calls
16. `test_integration_null_missing_propagation` (line 753) -- 3 `run()` calls
17. `test_integration_case_when` (line 795) -- 1 `run()` call
18. `test_integration_like_between_in` (line 821) -- 4 `run()` calls
19. `test_integration_cast_and_concat` (line 871) -- 2 `run()` calls
20. `test_integration_string_functions` (line 904) -- 1 `run()` call
21. `test_integration_distinct_and_order_by` (line 928) -- 1 `run()` call
22. `test_integration_join_with_aggregation` (line 956) -- 2 `run()` calls
23. `test_integration_subquery_and_union` (line 990) -- 4 `run()` calls
24. `test_integration_nullif` (line 1040) -- 1 `run()` call
25. `test_integration_json_output_mode` (line 1065) -- 2 `run()` calls
26. `test_integration_nested_path_and_array` (line 1098) -- 2 `run()` calls

In each test, construct the `DataSourceRegistry` once and use `.clone()` for subsequent calls.

### 5C-a. Verify

```bash
cargo test
```

---

## Step 6: Update `main.rs` CLI parsing

**File**: `src/cli.yml`

### 6a. Add `multiple: true` to `table` arg

Change:
```yaml
          - table:
              help: table to file mapping
              long: table
              takes_value: true
```

to:
```yaml
          - table:
              help: table to file mapping
              long: table
              takes_value: true
              multiple: true
```

Also add `table` arg to `explain` subcommand:
```yaml
    - explain:
        about: dump the query plan graph
        args:
          - table:
              help: table to file mapping
              long: table
              takes_value: true
              multiple: true
          - query:
              help: query string
              index: 1
```

### 6b. Extract shared `parse_table_specs` helper (S2)

**File**: `src/main.rs`

Add a shared helper function to eliminate duplication between `query` and `explain`:

```rust
fn parse_table_specs(table_specs: clap::Values) -> Result<common::types::DataSourceRegistry, AppError> {
    let mut data_sources = common::types::DataSourceRegistry::new();
    let mut stdin_count = 0u32;

    for table_spec_string in table_specs {
        if let Some(cap) = TABLE_SPEC_REGEX.captures(table_spec_string) {
            let table_name = cap.get(1).map_or("", |m| m.as_str()).to_string();
            let file_format = cap.get(2).map_or("", |m| m.as_str()).to_string();
            let file_path = cap.get(3).map_or("", |m| m.as_str()).to_string();

            if !["elb", "alb", "squid", "s3", "jsonl"].contains(&&*file_format) {
                return Err(AppError::InvalidLogFileFormat);
            }

            if data_sources.contains_key(&table_name) {
                eprintln!("Duplicate table name: {}", table_name);
                std::process::exit(1);
            }

            if file_path == "stdin" {
                stdin_count += 1;
                if stdin_count > 1 {
                    eprintln!("Multiple tables cannot use stdin. Only one --table may specify stdin as its source.");
                    std::process::exit(1);
                }
                let ds = common::types::DataSource::Stdin(file_format, table_name.clone());
                data_sources.insert(table_name, ds);
            } else {
                let path = Path::new(&file_path);
                let ds = common::types::DataSource::File(path.to_path_buf(), file_format, table_name.clone());
                data_sources.insert(table_name, ds);
            }
        } else {
            return Err(AppError::InvalidTableSpecString);
        }
    }

    Ok(data_sources)
}
```

### 6c. Rewrite `query` subcommand (lines 39-63)

```rust
let result = if let Some(table_specs) = sub_m.values_of("table") {
    match parse_table_specs(table_specs) {
        Ok(data_sources) => app::run(query_str, data_sources, output_mode),
        Err(e) => Err(e),
    }
} else {
    Err(AppError::InvalidTableSpecString)
};
```

### 6d. Rewrite `explain` subcommand (lines 72-80)

```rust
("explain", Some(sub_m)) => {
    if let Some(query_str) = sub_m.value_of("query") {
        let data_sources = if let Some(table_specs) = sub_m.values_of("table") {
            match parse_table_specs(table_specs) {
                Ok(ds) => ds,
                Err(e) => {
                    println!("{}", e);
                    return;
                }
            }
        } else {
            let mut ds = common::types::DataSourceRegistry::new();
            ds.insert("it".to_string(), common::types::DataSource::Stdin("jsonl".to_string(), "it".to_string()));
            ds
        };
        let result = app::explain(query_str, data_sources);

        if let Err(e) = result {
            println!("{}", e);
        }
    } else {
        println!("{}", sub_m.usage());
    }
}
```

### 6e. Verify

```bash
cargo test
```

---

## Step 7: Update benchmark file

**File**: `benches/bench_execution.rs`

### 7a. Write implementation

Add import:
```rust
use logq::common::types::DataSourceRegistry;
```

Update Tier A benchmark (lines 32-47) -- wrap `DataSource` in a one-entry registry:

```rust
let data_source = ctypes::DataSource::File(
    path.clone(),
    "elb".to_string(),
    "elb".to_string(),
);
let mut data_sources = DataSourceRegistry::new();
data_sources.insert("elb".to_string(), data_source);
let reg = registry.clone();
group.bench_function(*name, |b| {
    b.iter(|| {
        let result = logq::app::run_to_records_with_registry(
            black_box(query),
            data_sources.clone(),
            reg.clone(),
        );
        let _ = black_box(result);
    });
});
```

**File**: `src/lib.rs`

Update bench-internals export (line 43) to include `DataSourceRegistry`:
```rust
pub use crate::common::types::{Value, Variables, VariableName, DataSource, DataSourceRegistry};
```

### 7b. Verify

```bash
cargo check --features bench-internals
```

---

## Step 8: Write new tests

**File**: `src/app.rs` (in `mod tests`)

### 8a. Multi-table cross join test

```rust
#[test]
fn test_multi_table_cross_join_different_files() {
    let dir = tempdir().unwrap();

    let file_a = dir.path().join("a.jsonl");
    let mut fa = File::create(file_a.clone()).unwrap();
    writeln!(fa, r#"{{"id": 1, "x": "hello"}}"#).unwrap();
    writeln!(fa, r#"{{"id": 2, "x": "world"}}"#).unwrap();
    fa.sync_all().unwrap();
    drop(fa);

    let file_b = dir.path().join("b.jsonl");
    let mut fb = File::create(file_b.clone()).unwrap();
    writeln!(fb, r#"{{"id": 1, "y": 100}}"#).unwrap();
    writeln!(fb, r#"{{"id": 2, "y": 200}}"#).unwrap();
    fb.sync_all().unwrap();
    drop(fb);

    let mut data_sources = common::types::DataSourceRegistry::new();
    data_sources.insert(
        "a".to_string(),
        common::types::DataSource::File(file_a, "jsonl".to_string(), "a".to_string()),
    );
    data_sources.insert(
        "b".to_string(),
        common::types::DataSource::File(file_b, "jsonl".to_string(), "b".to_string()),
    );

    let results = run_to_vec(
        r#"select a.x, b.y from a as a, b as b"#,
        data_sources.clone(),
    ).unwrap();
    assert_eq!(results.len(), 4); // 2 x 2 = 4

    dir.close().unwrap();
}
```

### 8b. Multi-table left join test

```rust
#[test]
fn test_multi_table_left_join_different_files() {
    let dir = tempdir().unwrap();

    let file_a = dir.path().join("a.jsonl");
    let mut fa = File::create(file_a.clone()).unwrap();
    writeln!(fa, r#"{{"id": 1, "x": "hello"}}"#).unwrap();
    writeln!(fa, r#"{{"id": 2, "x": "world"}}"#).unwrap();
    writeln!(fa, r#"{{"id": 3, "x": "foo"}}"#).unwrap();
    fa.sync_all().unwrap();
    drop(fa);

    let file_b = dir.path().join("b.jsonl");
    let mut fb = File::create(file_b.clone()).unwrap();
    writeln!(fb, r#"{{"id": 1, "y": 100}}"#).unwrap();
    writeln!(fb, r#"{{"id": 2, "y": 200}}"#).unwrap();
    fb.sync_all().unwrap();
    drop(fb);

    let mut data_sources = common::types::DataSourceRegistry::new();
    data_sources.insert(
        "a".to_string(),
        common::types::DataSource::File(file_a, "jsonl".to_string(), "a".to_string()),
    );
    data_sources.insert(
        "b".to_string(),
        common::types::DataSource::File(file_b, "jsonl".to_string(), "b".to_string()),
    );

    let results = run_to_vec(
        r#"select a.x, b.y from a as a left join b as b on a.id = b.id"#,
        data_sources.clone(),
    ).unwrap();
    assert_eq!(results.len(), 3);

    dir.close().unwrap();
}
```

### 8c. Unknown table error test

```rust
#[test]
fn test_unknown_table_in_query_errors() {
    let dir = tempdir().unwrap();
    let file_a = dir.path().join("a.jsonl");
    let mut fa = File::create(file_a.clone()).unwrap();
    writeln!(fa, r#"{{"id": 1}}"#).unwrap();
    fa.sync_all().unwrap();
    drop(fa);

    let mut data_sources = common::types::DataSourceRegistry::new();
    data_sources.insert(
        "a".to_string(),
        common::types::DataSource::File(file_a, "jsonl".to_string(), "a".to_string()),
    );

    let result = run_to_vec(r#"select * from b"#, data_sources);
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("Unknown table"));
    assert!(err_msg.contains("b"));

    dir.close().unwrap();
}
```

### 8d. Single table backward compatibility test

```rust
#[test]
fn test_single_table_backward_compat() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("compat.jsonl");
    let mut f = File::create(file_path.clone()).unwrap();
    writeln!(f, r#"{{"x": 42}}"#).unwrap();
    f.sync_all().unwrap();
    drop(f);

    let mut data_sources = common::types::DataSourceRegistry::new();
    data_sources.insert(
        "it".to_string(),
        common::types::DataSource::File(file_path, "jsonl".to_string(), "it".to_string()),
    );

    let results = run_to_vec(r#"select x from it"#, data_sources).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0][0].1, common::types::Value::Int(42));

    dir.close().unwrap();
}
```

### 8e. Stdin constraint tests (S3)

```rust
#[test]
fn test_stdin_in_cross_join_right_side_errors() {
    // Stdin as right side of cross join should error at planning time
    let dir = tempdir().unwrap();
    let file_a = dir.path().join("a.jsonl");
    let mut fa = File::create(file_a.clone()).unwrap();
    writeln!(fa, r#"{{"id": 1}}"#).unwrap();
    fa.sync_all().unwrap();
    drop(fa);

    let mut data_sources = common::types::DataSourceRegistry::new();
    data_sources.insert(
        "a".to_string(),
        common::types::DataSource::File(file_a, "jsonl".to_string(), "a".to_string()),
    );
    data_sources.insert(
        "b".to_string(),
        common::types::DataSource::Stdin("jsonl".to_string(), "b".to_string()),
    );

    // Comma syntax: a is left, b (stdin) is right
    let result = run_to_vec(r#"select a.id from a as a, b as b"#, data_sources);
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("Stdin"));

    dir.close().unwrap();
}

#[test]
fn test_stdin_in_left_join_right_side_errors() {
    let dir = tempdir().unwrap();
    let file_a = dir.path().join("a.jsonl");
    let mut fa = File::create(file_a.clone()).unwrap();
    writeln!(fa, r#"{{"id": 1}}"#).unwrap();
    fa.sync_all().unwrap();
    drop(fa);

    let mut data_sources = common::types::DataSourceRegistry::new();
    data_sources.insert(
        "a".to_string(),
        common::types::DataSource::File(file_a, "jsonl".to_string(), "a".to_string()),
    );
    data_sources.insert(
        "b".to_string(),
        common::types::DataSource::Stdin("jsonl".to_string(), "b".to_string()),
    );

    let result = run_to_vec(
        r#"select a.id from a as a left join b as b on a.id = b.id"#,
        data_sources,
    );
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("Stdin"));

    dir.close().unwrap();
}
```

### 8f. Verify

```bash
cargo test
```

---

## Step 9: End-to-end verification

### 9a. Run full test suite

```bash
cargo test
```

### 9b. Check benchmark compilation

```bash
cargo check --features bench-internals
```

### 9c. Manual CLI test (optional)

```bash
echo '{"id":1,"x":"hello"}' > /tmp/a.jsonl
echo '{"id":1,"y":100}' > /tmp/b.jsonl
cargo run -- query --table a:jsonl=/tmp/a.jsonl --table b:jsonl=/tmp/b.jsonl 'select a.x, b.y from a as a, b as b'
```

---

## Task Dependencies

| Group | Steps | Can Parallelize | Files Touched |
|-------|-------|-----------------|---------------|
| 1 | Step 1 | N/A (first) | `common/types.rs`, `logical/parser.rs` |
| 2 | Step 2 | No (depends on 1) | `logical/types.rs`, `app.rs` |
| 3 | Step 3 | No (depends on 2) | `common/types.rs` |
| 4A | Step 4A | No (depends on 3) | `logical/parser.rs` |
| 4B | Step 4B | No (depends on 4A) | `logical/parser.rs` |
| 4C | Step 4C | No (depends on 4B) | `logical/parser.rs` |
| 5A | Step 5A | No (depends on 4C) | `app.rs` |
| 5B | Step 5B | No (depends on 5A) | `app.rs` |
| 5C | Step 5C | No (depends on 5B) | `app.rs` |
| 6 | Step 6 | No (depends on 5A) | `cli.yml`, `main.rs` |
| 7 | Step 7 | No (depends on 5A) | `benches/bench_execution.rs`, `lib.rs` |
| 8 | Step 8 | No (depends on 5B) | `app.rs` |
| 9 | Step 9 | No (depends on all) | -- |

Steps 6, 7, 8 can run in parallel after Step 5B completes (they touch independent files, except Step 8 shares `app.rs` with 5C but only adds new tests).
