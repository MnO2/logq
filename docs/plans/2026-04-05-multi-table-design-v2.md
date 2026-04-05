# Multi-Table CLI Support Design (v2)

## Changes from previous version

This revision addresses all critical issues and several suggestions from the design review.

| Issue | What changed |
|-------|-------------|
| **C1** | Added detailed specification of `check_env`/`check_env_ref` rewrite from single-name to registry-key validation, including interaction with `FromClausePathInvalidTableReference` and path-stripping. |
| **C2** | Added step-by-step mechanics of per-table `DataSource` lookup in `build_from_node`, including how the base name is extracted from each `TableReference` and how it coordinates with `to_bindings_for_ref`. |
| **C3** | Added explicit stdin constraints: at most one stdin table across all `--table` flags; stdin table cannot appear as the right side of a join. Validated at CLI parse time. |
| **C4** | Added handling for `file_format` extraction from registry per-table in `is_match_group_by_fields` and `parse_query`. |
| **C5** | Updated `explain` subcommand to accept `--table` flags and use the registry, instead of hardcoding `DataSource::Stdin("jsonl", "it")`. |
| **S1** | `PhysicalPlanCreator.data_source` field is removed entirely (dead field), rather than updated to hold a registry. |
| **S2** | Specified that table name lookup is case-sensitive, matching the existing `identifier` parser behavior. |
| **S5** | Called out the `Subquery` site at `parse_value_expression` line 345 explicitly; subqueries receive the full registry via `ParsingContext`. |
| **S6** | Added `benches/bench_execution.rs` and `run_to_records_with_registry` to files-changed table. |

---

## Summary

Add support for multiple `--table` flags so that queries can reference different data source files for different table names. This enables real multi-table joins:

```bash
logq query --table a:jsonl=data1.jsonl --table b:jsonl=data2.jsonl "SELECT * FROM a, b WHERE a.id = b.id"
```

## Approach

Use the **repeated `--table` flag** pattern (like Docker's `-v`, `-e`). Each flag specifies one table-name-to-file mapping. A `DataSourceRegistry` (`HashMap<String, DataSource>`) replaces the single `DataSource` parameter throughout the pipeline.

### Decision Log

| Decision | Choice | Alternatives Considered |
|----------|--------|------------------------|
| Multi-table CLI syntax | Repeated `--table` flag | Comma-separated, semicolon-separated |
| Data source plumbing | `HashMap<String, DataSource>` registry | Single DataSource + optional override map |
| Unknown table in query | Hard error at planning time | Silent empty results |
| Duplicate table names | Error | Last-one-wins |
| Table name case sensitivity | Case-sensitive (matches SQL identifier parser) | Case-insensitive fold |
| Stdin usage | At most one stdin table; cannot be right side of join | Unrestricted stdin |

## Design

### 1. CLI Parsing (`main.rs`)

- `cli.yml`: Add `multiple: true` to the `table` arg.
- `main.rs`: Iterate `values_of("table")` instead of `value_of("table")`. Parse each value with `TABLE_SPEC_REGEX`, collecting into a `HashMap<String, DataSource>`. Error on:
  - Invalid format (not matching regex)
  - Unsupported file format (not in `elb/alb/squid/s3/jsonl`)
  - Duplicate table names across flags
  - More than one table using `stdin` as the file path (see section 8)

Pass the `HashMap<String, DataSource>` (the `DataSourceRegistry`) to `app::run` / `app::explain`.

#### `explain` subcommand

The current `explain` implementation hardcodes `DataSource::Stdin("jsonl", "it")` and ignores `--table`. This must be updated:
- `explain` shares the same `--table` flag parsing logic as `query`.
- If no `--table` flag is provided for `explain`, a sensible default is used: `{"it" => DataSource::Stdin("jsonl", "it")}` (preserving current behavior for simple `explain` usage).
- `app::explain` receives `DataSourceRegistry` instead of a single `DataSource`.

### 2. DataSourceRegistry Type (`common/types.rs`)

```rust
pub type DataSourceRegistry = HashMap<String, DataSource>;
```

### 3. App Layer (`app.rs`)

- `app::run(query_str, data_sources: DataSourceRegistry, output_mode)` replaces the single `DataSource` param.
- `app::explain(query_str, data_sources: DataSourceRegistry)` same.
- `app::run_to_vec` (test helper) same.
- `app::run_to_records` (bench helper) same.
- `app::run_to_records_with_registry` (bench helper) same.

Since `PhysicalPlanCreator.data_source` is a dead field (see section 6), the `data_source.clone()` calls currently used to construct `PhysicalPlanCreator` are removed entirely. The `PhysicalPlanCreator::new()` constructor takes no `DataSource` argument.

### 4. ParsingContext (`common/types.rs`)

```rust
pub(crate) struct ParsingContext {
    pub(crate) data_sources: DataSourceRegistry,
    pub(crate) registry: Arc<FunctionRegistry>,
}
```

Remove the `table_name` and single `data_source` fields. The registry contains all table-name-to-DataSource mappings.

### 5. Logical Planner (`logical/parser.rs`)

#### 5a. `parse_query_top` and `parse_query` signatures

Both receive `DataSourceRegistry` instead of a single `DataSource`:

```rust
pub(crate) fn parse_query_top(
    q: ast::Query,
    data_sources: DataSourceRegistry,
    registry: Arc<FunctionRegistry>,
) -> ParseResult<types::Node>

pub(crate) fn parse_query(
    query: ast::SelectStatement,
    data_sources: DataSourceRegistry,
    registry: Arc<FunctionRegistry>,
) -> ParseResult<types::Node>
```

Set operations (`parse_query_top`) clone the full `DataSourceRegistry` for each branch, just as the current code clones the single `DataSource`.

#### 5b. `check_env` / `check_env_ref` rewrite (C1)

**Current behavior**: `check_env_ref(table_name, table_reference)` verifies that the first `PathSegment` of every table reference equals the single `table_name` string. If not, it returns `FromClausePathInvalidTableReference`.

**New behavior**: `check_env_ref` takes `&DataSourceRegistry` instead of `&String`:

```rust
fn check_env_ref(
    data_sources: &DataSourceRegistry,
    table_reference: &TableReference,
) -> ParseResult<()> {
    match &table_reference.path_expr.path_segments[0] {
        PathSegment::AttrName(s) => {
            if !data_sources.contains_key(s) {
                return Err(ParseError::UnknownTable(
                    s.clone(),
                    data_sources.keys().cloned().collect::<Vec<_>>().join(", "),
                ));
            }
        }
        _ => return Err(ParseError::FromClausePathInvalidTableReference),
    }

    let path_expr = &table_reference.path_expr;
    if path_expr.path_segments.is_empty() {
        return Err(ParseError::FromClausePathInvalidTableReference);
    }
    if path_expr.path_segments.len() > 1 && table_reference.as_clause.is_none() {
        return Err(ParseError::FromClauseMissingAsForPathExpr);
    }
    Ok(())
}
```

Key differences:
- Instead of `s.eq(table_name)`, we check `data_sources.contains_key(s)`.
- On failure, we return `UnknownTable(name, available_tables)` instead of the generic `FromClausePathInvalidTableReference`. This gives the user a clear error message listing available tables.
- The path-length and `as_clause` checks remain unchanged.

`check_env` similarly takes `&DataSourceRegistry`:

```rust
fn check_env(data_sources: &DataSourceRegistry, from_clause: &FromClause) -> ParseResult<()> {
    let refs = from_clause.collect_table_references();
    for table_reference in refs {
        check_env_ref(data_sources, table_reference)?;
    }
    Ok(())
}
```

**Case sensitivity (S2)**: Table name lookup uses `HashMap::contains_key`, which is case-sensitive. The syntax parser's `identifier` function preserves the case of identifiers as written in the query. The `TABLE_SPEC_REGEX` also preserves case. Therefore, `--table MyTable:jsonl=f.jsonl` with `SELECT * FROM mytable` will produce `UnknownTable("mytable", "MyTable")`. This matches standard SQL behavior where unquoted identifiers are case-sensitive in the absence of explicit folding.

#### 5c. `to_bindings_for_ref` and `to_bindings` changes

These functions currently take `table_name: &String` and strip the table-name prefix from path expressions. With the registry, the same logic applies -- the first segment is always a table name (now validated as a registry key), so it gets stripped:

```rust
fn to_bindings_for_ref(
    data_sources: &DataSourceRegistry,
    table_reference: &TableReference,
) -> Vec<common::Binding> {
    let path_expr = match &table_reference.path_expr.path_segments[0] {
        PathSegment::AttrName(s) => {
            if data_sources.contains_key(s) {
                // Strip the table-name prefix, keep remaining segments
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

`to_bindings` calls `to_bindings_for_ref` for each reference, passing `data_sources`.

#### 5d. `build_from_node` per-table DataSource lookup (C2)

**Current behavior**: `build_from_node` receives a single `&DataSource` and clones it for every `Node::DataSource(...)`.

**New behavior**: `build_from_node` receives `&DataSourceRegistry`. For each table reference, it extracts the base table name (first `PathSegment::AttrName`) and looks up the corresponding `DataSource` from the registry.

```rust
fn build_from_node(
    ctx: &ParsingContext,
    from_clause: &FromClause,
    data_sources: &DataSourceRegistry,
) -> ParseResult<types::Node> {
    match from_clause {
        FromClause::Tables(table_references) => {
            if table_references.len() == 1 {
                let ref0 = &table_references[0];
                let ds = lookup_data_source(data_sources, ref0)?;
                let bindings = to_bindings_for_ref(data_sources, ref0);
                Ok(types::Node::DataSource(ds, bindings))
            } else {
                let ref0 = &table_references[0];
                let ds0 = lookup_data_source(data_sources, ref0)?;
                let first_bindings = to_bindings_for_ref(data_sources, ref0);
                let mut node = types::Node::DataSource(ds0, first_bindings);
                for table_ref in table_references.iter().skip(1) {
                    let ds = lookup_data_source(data_sources, table_ref)?;
                    let ref_bindings = to_bindings_for_ref(data_sources, table_ref);
                    let right = types::Node::DataSource(ds, ref_bindings);
                    node = types::Node::CrossJoin(Box::new(node), Box::new(right));
                }
                Ok(node)
            }
        }
        FromClause::Join { left, right, join_type, condition } => {
            let left_node = build_from_node(ctx, left, data_sources)?;
            let ds_right = lookup_data_source(data_sources, right)?;
            let right_bindings = to_bindings_for_ref(data_sources, right);
            let right_node = types::Node::DataSource(ds_right, right_bindings);
            match join_type {
                JoinType::Cross => {
                    Ok(types::Node::CrossJoin(Box::new(left_node), Box::new(right_node)))
                }
                JoinType::Left => {
                    let on_expr = condition.as_ref()
                        .expect("LEFT JOIN requires ON condition");
                    let formula = parse_logic(ctx, on_expr)?;
                    Ok(types::Node::LeftJoin(
                        Box::new(left_node), Box::new(right_node), formula,
                    ))
                }
            }
        }
    }
}
```

The helper `lookup_data_source` extracts the base name from a `TableReference` and looks it up:

```rust
fn lookup_data_source(
    data_sources: &DataSourceRegistry,
    table_ref: &TableReference,
) -> ParseResult<DataSource> {
    match &table_ref.path_expr.path_segments[0] {
        PathSegment::AttrName(name) => {
            data_sources.get(name).cloned().ok_or_else(|| {
                ParseError::UnknownTable(
                    name.clone(),
                    data_sources.keys().cloned().collect::<Vec<_>>().join(", "),
                )
            })
        }
        _ => Err(ParseError::FromClausePathInvalidTableReference),
    }
}
```

**Coordination note**: By the time `build_from_node` runs, `check_env` has already validated that every table reference's base name exists in the registry. The `lookup_data_source` call is a safety net (should never fail after `check_env`), but we keep it for defense-in-depth.

#### 5e. `file_format` extraction for `is_match_group_by_fields` (C4)

**Current behavior**: `parse_query` destructures the single `data_source` at lines 684-687 to get `file_format`, which is later used by `is_match_group_by_fields` to expand `Star` into field names for structured log formats.

**New behavior**: With multiple tables, there is no single global `file_format`. The `file_format` must come from the FROM clause's primary table. The logic:

1. Extract the base table name from the first table reference in the FROM clause.
2. Look up that table's `DataSource` from the registry.
3. Extract `file_format` from it.

```rust
let primary_table_name = match &from_clause {
    FromClause::Tables(refs) => extract_base_name(&refs[0])?,
    FromClause::Join { left, .. } => {
        // Walk left until we find a Tables variant
        fn leftmost_table_name(fc: &FromClause) -> ParseResult<String> {
            match fc {
                FromClause::Tables(refs) => extract_base_name(&refs[0]),
                FromClause::Join { left, .. } => leftmost_table_name(left),
            }
        }
        leftmost_table_name(left)?
    }
};
let primary_ds = data_sources.get(&primary_table_name).unwrap();
let file_format = match primary_ds {
    DataSource::File(_, fmt, _) => fmt.clone(),
    DataSource::Stdin(fmt, _) => fmt.clone(),
};
```

`is_match_group_by_fields` continues to receive `&file_format` as today. This is correct because `SELECT *, count(*) FROM a GROUP BY ...` needs to know `a`'s schema to expand `*`.

#### 5f. Subquery handling (S5)

At line 345 of `logical/parser.rs`, the current code is:

```rust
ast::Expression::Subquery(stmt) => {
    let inner_node = parse_query(*stmt.clone(), ctx.data_source.clone(), ctx.registry.clone())?;
```

With the registry change, `ParsingContext` no longer has a `data_source` field. Instead it has `data_sources: DataSourceRegistry`. The subquery call becomes:

```rust
ast::Expression::Subquery(stmt) => {
    let inner_node = parse_query(*stmt.clone(), ctx.data_sources.clone(), ctx.registry.clone())?;
```

This correctly gives the subquery access to all registered tables, which is the right behavior for non-correlated subqueries like `SELECT x FROM a WHERE x = (SELECT max(y) FROM b)`.

### 6. Physical Plan Creator (`logical/types.rs`) (S1)

**Remove** `PhysicalPlanCreator.data_source` entirely. It is a dead field -- never read after construction. The `DataSource` is already embedded in each `Node::DataSource(...)` variant from the logical plan.

```rust
pub(crate) struct PhysicalPlanCreator {
    counter: u32,
}

impl PhysicalPlanCreator {
    pub(crate) fn new() -> Self {
        PhysicalPlanCreator { counter: 0 }
    }

    pub(crate) fn new_constant_name(&mut self) -> VariableName {
        let constant_name = format!("const_{:09}", self.counter);
        self.counter += 1;
        constant_name
    }
}
```

All call sites (`app::run`, `app::explain`, `app::run_to_vec`, `app::run_to_records`, `app::run_to_records_with_registry`) change from `PhysicalPlanCreator::new(data_source)` to `PhysicalPlanCreator::new()`.

### 7. Backward Compatibility

Single-table usage is unchanged -- a single `--table it:jsonl=data.jsonl` produces a one-entry registry `{"it" => DataSource::File(...)}`. All existing tests continue to work by constructing one-entry registries.

Test helpers (`run_format_query`, `run_format_query_to_vec`, etc.) are updated to build a one-entry `DataSourceRegistry` from their existing `DataSource`:

```rust
let data_source = DataSource::File(file_path, format.to_string(), "it".to_string());
let mut data_sources = DataSourceRegistry::new();
data_sources.insert("it".to_string(), data_source);
```

### 8. Stdin Constraints (C3)

Stdin (`io::stdin()`) can only be read once. The `CrossJoinStream` and `LeftJoinStream` implementations re-create the right-side stream for every left row. If the right side is stdin, it will silently produce empty results on the second iteration.

**Constraints enforced at CLI parse time (`main.rs`)**:

1. **At most one stdin table**: If more than one `--table` flag uses `stdin` as the file path, error immediately with a clear message: `"Multiple tables cannot use stdin. Only one --table may specify stdin as its source."`.

2. **Stdin cannot be the right side of a join**: This constraint is harder to enforce at CLI parse time (we don't know the query structure yet). Instead, enforce it at **planning time** in `build_from_node`:
   - When constructing a `CrossJoin` or `LeftJoin`, check if the right-side `DataSource` is `DataSource::Stdin(...)`. If so, return a new error: `ParseError::StdinInJoinRightSide`.
   - Exception: if the query has only one table reference (no join), stdin is fine.

Add new error variant:
```rust
#[error("Stdin cannot be used as the right side of a join (it can only be read once)")]
StdinInJoinRightSide,
```

**Note on self-joins with stdin**: Even a single-stdin self-join (`FROM it AS a CROSS JOIN it AS b` where `it` is stdin) will fail at the right-side check. This is correct -- stdin truly cannot be re-read.

### 9. New Error Variants

```rust
#[error("Unknown table '{0}'. Available tables: {1}")]
UnknownTable(String, String),

#[error("Stdin cannot be used as the right side of a join (it can only be read once)")]
StdinInJoinRightSide,
```

## Testing

### CLI Argument Parsing Tests

1. Single table: `--table it:jsonl=data.jsonl` produces one-entry registry
2. Multiple tables: `--table a:jsonl=d1.jsonl --table b:jsonl=d2.jsonl` produces two-entry registry
3. Invalid format: `--table a:badformat=d.jsonl` errors
4. Duplicate table names: `--table a:jsonl=d1.jsonl --table a:jsonl=d2.jsonl` errors
5. Missing table flag: errors with `InvalidTableSpecString`
6. Unknown table in query: `--table a:jsonl=d.jsonl` with `SELECT * FROM b` errors with `UnknownTable("b", "a")`
7. Two stdin tables: `--table a:jsonl=stdin --table b:jsonl=stdin` errors

### Planner Tests

8. Stdin as right side of cross join errors with `StdinInJoinRightSide`
9. Stdin as right side of left join errors with `StdinInJoinRightSide`
10. Different tables in cross join produce `Node::CrossJoin` with different `DataSource` per side
11. Different tables in left join produce `Node::LeftJoin` with different `DataSource` per side
12. Table name case mismatch: `--table MyT:jsonl=f.jsonl` with `FROM myt` errors with `UnknownTable`

### Integration Tests

13. Cross join two different files: `SELECT a.x, b.y FROM a, b`
14. Left join two different files: `SELECT a.x, b.y FROM a LEFT JOIN b ON a.id = b.id`
15. Join different file formats (JSONL + ELB): `SELECT a.x, b.timestamp FROM a LEFT JOIN b ON ...`
16. `explain` with `--table a:jsonl=f1.jsonl --table b:jsonl=f2.jsonl` works
17. All existing ~58 tests pass (one-entry registry)

### Benchmark Tests

18. `benches/bench_execution.rs` compiles and runs with registry-based signatures

## Files Changed

| File | Change |
|------|--------|
| `src/cli.yml` | Add `multiple: true` to `table` arg |
| `src/main.rs` | Multi-value table parsing, duplicate detection, stdin-count validation, build registry; update `explain` to use `--table` with default fallback |
| `src/common/types.rs` | Add `DataSourceRegistry` type alias, update `ParsingContext` (remove `table_name`/`data_source`, add `data_sources`) |
| `src/app.rs` | Change all function signatures to take `DataSourceRegistry`; remove `data_source` arg from `PhysicalPlanCreator::new()` calls |
| `src/logical/parser.rs` | Rewrite `check_env`/`check_env_ref` for registry lookup; rewrite `build_from_node` with per-table `lookup_data_source`; update `to_bindings`/`to_bindings_for_ref` to take registry; extract `file_format` from primary table; update subquery site; add `UnknownTable` and `StdinInJoinRightSide` errors |
| `src/logical/types.rs` | Remove `data_source` field from `PhysicalPlanCreator`; update `new()` to take no args |
| `benches/bench_execution.rs` | Update `run_to_records_with_registry` calls to pass `DataSourceRegistry` instead of single `DataSource` |

## Implementation Order

1. Add `DataSourceRegistry` type alias and `UnknownTable`/`StdinInJoinRightSide` error variants.
2. Remove `PhysicalPlanCreator.data_source` field (S1) -- simplest change, unblocks everything.
3. Update `ParsingContext` to hold `data_sources: DataSourceRegistry`.
4. Rewrite `check_env`/`check_env_ref`, `to_bindings`/`to_bindings_for_ref`, `build_from_node`, and `parse_query`/`parse_query_top`.
5. Update `app.rs` signatures and call sites.
6. Update `main.rs` CLI parsing (multi-value iteration, duplicate/stdin validation, explain).
7. Update `benches/bench_execution.rs`.
8. Run full test suite. Write new tests per the plan above.
