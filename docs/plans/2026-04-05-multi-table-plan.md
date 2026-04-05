# Plan: Multi-Table CLI Support

**Goal**: Support multiple `--table` flags so queries can join data from different files (e.g., `--table a:jsonl=d1.jsonl --table b:jsonl=d2.jsonl`).
**Architecture**: Replace single `DataSource` with `DataSourceRegistry` (`HashMap<String, DataSource>`) throughout parser/planner/app layers. Per-table lookup in `build_from_node`.
**Tech Stack**: Rust, clap (YAML), nom, criterion (benchmarks)
**Design Doc**: `docs/plans/2026-04-05-multi-table-design-final.md`

---

## Step 1: Add `DataSourceRegistry` type alias and new error variants

**File**: `src/common/types.rs`

### 1a. Write failing test

No test needed — this is a type alias and error variants that won't compile until used.

### 1b. Write implementation

At the top of `src/common/types.rs`, add the `HashMap` import and the type alias after the existing `DataSource` enum (after line 416):

```rust
use std::collections::HashMap;
// ... (existing code)

pub type DataSourceRegistry = HashMap<String, DataSource>;
```

**File**: `src/logical/parser.rs`

Add three new error variants to `ParseError` enum (after `FromClauseMissingAsForPathExpr`):

```rust
#[error("Unknown table '{0}'. Available tables: {1}")]
UnknownTable(String, String),
#[error("Stdin cannot be used as the right side of a join (it can only be read once)")]
StdinInJoinRightSide,
#[error("SELECT * with GROUP BY is not supported in multi-table queries")]
StarGroupByMultiTable,
```

Add `PartialEq` match arms in `src/app.rs` `AppError::PartialEq` impl — not needed since these are `ParseError` variants, not `AppError`.

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

Remove the `DataSource` import if no longer used: `use crate::common::types::{DataSource, VariableName};` → check if `DataSource` is used elsewhere in the file (it is, in `Node::DataSource`).

**File**: `src/app.rs`

Update all `PhysicalPlanCreator::new(data_source)` calls (4 occurrences) to `PhysicalPlanCreator::new()`:
- Line 106: `let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new();`
- Line 123: same
- Line 208: same
- Line 259: same

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

This will cause compilation errors in `logical/parser.rs` — expected, fixed in Step 4.

---

## Step 4: Rewrite logical planner for registry-based lookup

**File**: `src/logical/parser.rs`

This is the largest step. All changes are in this single file.

### 4a. Rewrite `check_env_ref` (lines 513-533)

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

### 4b. Rewrite `check_env` (lines 535-541)

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

### 4c. Rewrite `to_bindings_for_ref` (lines 543-572)

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

### 4d. Rewrite `to_bindings` (lines 574-579)

```rust
fn to_bindings(data_sources: &common::DataSourceRegistry, table_references: &Vec<TableReference>) -> Vec<common::Binding> {
    table_references
        .iter()
        .flat_map(|table_reference| to_bindings_for_ref(data_sources, table_reference))
        .collect()
}
```

### 4e. Add `lookup_data_source` helper

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

### 4f. Add `extract_base_name` helper

```rust
fn extract_base_name(table_ref: &TableReference) -> ParseResult<String> {
    match &table_ref.path_expr.path_segments[0] {
        PathSegment::AttrName(s) => Ok(s.clone()),
        _ => Err(ParseError::FromClausePathInvalidTableReference),
    }
}
```

### 4g. Rewrite `build_from_node` (lines 614-652)

Change signature — remove `data_source: &common::DataSource` and `table_name: &String`, add `data_sources: &common::DataSourceRegistry`:

```rust
fn build_from_node(
    ctx: &common::ParsingContext,
    from_clause: &FromClause,
    data_sources: &common::DataSourceRegistry,
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
                    // Check stdin constraint for right side of implicit cross join
                    if matches!(&ds, common::DataSource::Stdin(..)) {
                        return Err(ParseError::StdinInJoinRightSide);
                    }
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
            // Check stdin constraint for right side of explicit join
            if matches!(&ds_right, common::DataSource::Stdin(..)) {
                return Err(ParseError::StdinInJoinRightSide);
            }
            let right_bindings = to_bindings_for_ref(data_sources, right);
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

### 4h. Rewrite `parse_query_top` (line 655)

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

### 4i. Rewrite `parse_query` (line 681)

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

    let mut root = build_from_node(&parsing_context, from_clause, &data_sources)?;
    // ... rest of function unchanged until is_match_group_by_fields call
```

### 4j. Add `StarGroupByMultiTable` check before `is_match_group_by_fields`

At line ~971 (before the `is_match_group_by_fields` call), add:

```rust
// Check for SELECT * with GROUP BY in multi-table queries
if data_sources.len() > 1 && non_aggregates.iter().any(|n| matches!(n, types::Named::Star)) {
    return Err(ParseError::StarGroupByMultiTable);
}
```

### 4k. Update subquery site (line 345)

Change `ctx.data_source.clone()` to `ctx.data_sources.clone()`:

```rust
ast::Expression::Subquery(stmt) => {
    let inner_node = parse_query(*stmt.clone(), ctx.data_sources.clone(), ctx.registry.clone())?;
```

### 4l. Update all test `ParsingContext` constructions

In `mod test` (from line 1073 onward), every `ParsingContext { table_name: ..., data_source: ..., registry: ... }` must change to `ParsingContext { data_sources: ..., registry: ... }`.

For each test, replace:
```rust
let parsing_context = ParsingContext {
    table_name: "a".to_string(),
    data_source: common::DataSource::Stdin("jsonl".to_string(), "a".to_string()),
    registry: registry.clone(),
};
```
with:
```rust
let data_source = common::DataSource::Stdin("jsonl".to_string(), "a".to_string());
let mut data_sources = common::DataSourceRegistry::new();
data_sources.insert("a".to_string(), data_source);
let parsing_context = ParsingContext {
    data_sources,
    registry: registry.clone(),
};
```

Similarly for tests using table name `"it"`.

Also update all `parse_query(stmt, data_source, registry)` calls in tests to pass a `DataSourceRegistry` instead.

### 4m. Verify

```bash
cargo test
```

---

## Step 5: Update `app.rs` signatures and all call sites

**File**: `src/app.rs`

### 5a. Write implementation

Change `explain` signature (line 97):
```rust
pub fn explain(query_str: &str, data_sources: common::types::DataSourceRegistry) -> AppResult<()> {
```

Update body — remove `data_source` destructuring. The `parse_query_top` and `PhysicalPlanCreator::new()` calls already updated:
```rust
    let node = logical::parser::parse_query_top(q, data_sources, registry)?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new();
```

Change `run` signature (line 114):
```rust
pub fn run(query_str: &str, data_sources: common::types::DataSourceRegistry, output_mode: OutputMode) -> AppResult<()> {
```

Update body similarly.

Change `run_to_vec` signature (line 196):
```rust
pub(crate) fn run_to_vec(
    query_str: &str,
    data_sources: common::types::DataSourceRegistry,
) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
```

Change `run_to_records` signature (line 222):
```rust
pub fn run_to_records(
    query_str: &str,
    data_sources: common::types::DataSourceRegistry,
) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
```

Change `run_to_records_with_registry` signature (line 248):
```rust
pub fn run_to_records_with_registry(
    query_str: &str,
    data_sources: common::types::DataSourceRegistry,
    registry: Arc<functions::FunctionRegistry>,
) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
```

### 5b. Update test helpers in `app.rs` `mod tests`

Update `run_format_query` (line 280) — build a one-entry registry:
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

Update `run_format_query_to_vec` (line 297) similarly.

Update all individual test functions that construct `DataSource` directly and pass to `run`/`run_to_vec` — wrap in a one-entry `DataSourceRegistry`. There are ~20 tests to update. The pattern is always the same:
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

### 5c. Verify

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

### 6b. Rewrite `main.rs` query subcommand

**File**: `src/main.rs`

Add import at top:
```rust
use logq::common::types::DataSourceRegistry;
```

Replace the `--table` parsing block (lines 39-63) with multi-table parsing:

```rust
let result = if let Some(table_specs) = sub_m.values_of("table") {
    let mut data_sources = DataSourceRegistry::new();
    let mut stdin_count = 0u32;
    let mut parse_error: Option<AppError> = None;

    for table_spec_string in table_specs {
        if let Some(cap) = TABLE_SPEC_REGEX.captures(table_spec_string) {
            let table_name = cap.get(1).map_or("", |m| m.as_str()).to_string();
            let file_format = cap.get(2).map_or("", |m| m.as_str()).to_string();
            let file_path = cap.get(3).map_or("", |m| m.as_str()).to_string();

            if !["elb", "alb", "squid", "s3", "jsonl"].contains(&&*file_format) {
                parse_error = Some(AppError::InvalidLogFileFormat);
                break;
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
            parse_error = Some(AppError::InvalidTableSpecString);
            break;
        }
    }

    if let Some(e) = parse_error {
        Err(e)
    } else {
        app::run(query_str, data_sources, output_mode)
    }
} else {
    Err(AppError::InvalidTableSpecString)
};
```

### 6c. Update `explain` subcommand (lines 72-80)

```rust
("explain", Some(sub_m)) => {
    if let Some(query_str) = sub_m.value_of("query") {
        let data_sources = if let Some(table_specs) = sub_m.values_of("table") {
            let mut ds = DataSourceRegistry::new();
            for table_spec_string in table_specs {
                if let Some(cap) = TABLE_SPEC_REGEX.captures(table_spec_string) {
                    let table_name = cap.get(1).map_or("", |m| m.as_str()).to_string();
                    let file_format = cap.get(2).map_or("", |m| m.as_str()).to_string();
                    let file_path = cap.get(3).map_or("", |m| m.as_str()).to_string();
                    if file_path == "stdin" {
                        ds.insert(table_name.clone(), common::types::DataSource::Stdin(file_format, table_name));
                    } else {
                        let path = Path::new(&file_path);
                        ds.insert(table_name.clone(), common::types::DataSource::File(path.to_path_buf(), file_format, table_name));
                    }
                }
            }
            ds
        } else {
            let mut ds = DataSourceRegistry::new();
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

### 6d. Verify

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

Update Tier A benchmark (line 32-46) — wrap `DataSource` in a one-entry registry:

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

Also update `lib.rs` bench_internals to export `DataSourceRegistry` (line 43):
```rust
pub use crate::common::types::{Value, Variables, VariableName, DataSource, DataSourceRegistry};
```

### 7b. Verify

```bash
cargo check --features bench-internals
```

---

## Step 8: Write new tests — CLI argument parsing

**File**: `src/app.rs` (in `mod tests`)

### 8a. Write tests

```rust
#[test]
fn test_multi_table_cross_join_different_files() {
    let dir = tempdir().unwrap();

    // Create file for table "a"
    let file_a = dir.path().join("a.jsonl");
    let mut fa = File::create(file_a.clone()).unwrap();
    writeln!(fa, r#"{{"id": 1, "x": "hello"}}"#).unwrap();
    writeln!(fa, r#"{{"id": 2, "x": "world"}}"#).unwrap();
    fa.sync_all().unwrap();
    drop(fa);

    // Create file for table "b"
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

    // Cross join (comma syntax)
    let results = run_to_vec(
        r#"select a.x, b.y from a as a, b as b"#,
        data_sources.clone(),
    ).unwrap();
    assert_eq!(results.len(), 4); // 2 x 2 = 4

    dir.close().unwrap();
}

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
    // 3 rows: id=1 matches, id=2 matches, id=3 has NULL for b.y
    assert_eq!(results.len(), 3);

    dir.close().unwrap();
}

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

### 8b. Verify

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
| 4 | Step 4 | No (depends on 3) | `logical/parser.rs` |
| 5 | Step 5 | No (depends on 4) | `app.rs` |
| 6 | Step 6 | No (depends on 5) | `cli.yml`, `main.rs` |
| 7 | Step 7 | No (depends on 5) | `benches/bench_execution.rs`, `lib.rs` |
| 8 | Step 8 | No (depends on 5) | `app.rs` |
| 9 | Step 9 | No (depends on all) | — |

Steps 6, 7, 8 can run in parallel (independent files) after Step 5 completes.
