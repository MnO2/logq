# Plan: Selective Field Parsing, Two-Phase Lazy Parsing, and Batch-Native GroupBy

**Goal**: Unlock the projected 2.4x speedup by implementing three optimizations: selective field parsing, two-phase lazy parsing in BatchScanOperator, and batch-native GroupBy — eliminating the parsing waste and double-materialization overhead.

**Architecture**: `extract_required_fields` computes required field indices from the query plan tree. `try_get_batch` threads `required_fields` top-down. `BatchScanOperator` gains two-phase parsing with pushed predicates. `BatchGroupByOperator` aggregates directly on `ColumnBatch`es. `Node::Map` batch support enables the full batch path through GroupBy.

**Tech Stack**: Rust (edition 2018, stable), criterion benchmarks.

**Design Reference**: `docs/plans/2026-04-06-selective-parsing-design-final.md`

---

## Task Dependencies

| Group | Steps | Can Parallelize | Notes |
|-------|-------|-----------------|-------|
| 1 | Steps 1-2 | Yes | Field analysis + parse_field_column_selected extension |
| 2 | Step 3 | No | Thread required_fields through try_get_batch. Depends on Group 1. |
| 3 | Step 4 | No | Two-phase BatchScanOperator. Depends on Group 2. |
| 4 | Step 5 | No | Predicate pushdown in try_get_batch. Depends on Groups 3. |
| 5 | Step 6 | No | BatchGroupByOperator. Independent of Groups 3-4 but placed here for ordering. |
| 6 | Step 7 | No | Node::Map + Node::GroupBy in try_get_batch. Depends on Groups 5. |
| 7 | Step 8 | No | End-to-end verification + benchmarks. Depends on all. |

---

## Step 1: Implement extract_required_fields

**File (create)**: `src/execution/field_analysis.rs`

### 1a. Write failing tests

```rust
// src/execution/field_analysis.rs

use crate::execution::log_schema::LogSchema;
use crate::execution::types::{Expression, Formula, Named, NamedAggregate, Node};
use crate::syntax::ast::{PathExpr, PathSegment};
use hashbrown::HashSet;

/// Collect all field names referenced by an Expression.
fn collect_expr_fields(expr: &Expression, out: &mut HashSet<String>) {
    match expr {
        Expression::Variable(path) => {
            if path.path_segments.len() == 1 {
                if let PathSegment::AttrName(name) = &path.path_segments[0] {
                    out.insert(name.clone());
                }
            }
        }
        Expression::Constant(_) => {}
        Expression::Function(_, args) => {
            for arg in args {
                match arg {
                    Named::Expression(e, _) => collect_expr_fields(e, out),
                    Named::Star => {
                        // Star means all fields — caller handles this
                        out.insert("*".to_string());
                    }
                }
            }
        }
        Expression::Branch(branches, else_expr) => {
            for (formula, then_expr) in branches {
                collect_formula_fields(formula, out);
                collect_expr_fields(then_expr, out);
            }
            if let Some(e) = else_expr {
                collect_expr_fields(e, out);
            }
        }
        Expression::Cast(inner, _) => collect_expr_fields(inner, out),
        Expression::Logic(formula) => collect_formula_fields(formula, out),
        Expression::Subquery(_) => {
            // Conservative: mark as needing all fields
            out.insert("*".to_string());
        }
    }
}

/// Collect all field names referenced by a Formula.
fn collect_formula_fields(formula: &Formula, out: &mut HashSet<String>) {
    match formula {
        Formula::Constant(_) => {}
        Formula::Predicate(_, left, right) => {
            collect_expr_fields(left, out);
            collect_expr_fields(right, out);
        }
        Formula::And(left, right) | Formula::Or(left, right) => {
            collect_formula_fields(left, out);
            collect_formula_fields(right, out);
        }
        Formula::Not(inner) => collect_formula_fields(inner, out),
        Formula::IsNull(e) | Formula::IsNotNull(e)
        | Formula::IsMissing(e) | Formula::IsNotMissing(e) => {
            collect_expr_fields(e, out);
        }
        Formula::ExpressionPredicate(e) => collect_expr_fields(e, out),
        Formula::Like(left, right) | Formula::NotLike(left, right) => {
            collect_expr_fields(left, out);
            collect_expr_fields(right, out);
        }
        Formula::In(expr, list) | Formula::NotIn(expr, list) => {
            collect_expr_fields(expr, out);
            for e in list {
                collect_expr_fields(e, out);
            }
        }
    }
}

/// Collect field names referenced by a Named (projection item).
fn collect_named_fields(named: &Named, out: &mut HashSet<String>) {
    match named {
        Named::Expression(expr, _) => collect_expr_fields(expr, out),
        Named::Star => { out.insert("*".to_string()); }
    }
}

/// Walk the physical plan tree and collect all referenced field names.
fn collect_node_fields(node: &Node, out: &mut HashSet<String>) {
    match node {
        Node::Map(named_list, source) => {
            for named in named_list {
                collect_named_fields(named, out);
            }
            collect_node_fields(source, out);
        }
        Node::Filter(source, formula) => {
            collect_formula_fields(formula, out);
            collect_node_fields(source, out);
        }
        Node::GroupBy(keys, aggregates, source) => {
            for key in keys {
                if let Some(PathSegment::AttrName(name)) = key.path_segments.last() {
                    out.insert(name.clone());
                }
            }
            for na in aggregates {
                match &na.aggregate {
                    crate::execution::types::Aggregate::Count(_, named)
                    | crate::execution::types::Aggregate::Sum(_, named)
                    | crate::execution::types::Aggregate::Avg(_, named)
                    | crate::execution::types::Aggregate::Min(_, named)
                    | crate::execution::types::Aggregate::Max(_, named)
                    | crate::execution::types::Aggregate::First(_, named)
                    | crate::execution::types::Aggregate::Last(_, named)
                    | crate::execution::types::Aggregate::ApproxCountDistinct(_, named)
                    | crate::execution::types::Aggregate::GroupAs(_, named) => {
                        collect_named_fields(named, out);
                    }
                    crate::execution::types::Aggregate::PercentileDisc(_, col_name)
                    | crate::execution::types::Aggregate::ApproxPercentile(_, col_name) => {
                        out.insert(col_name.clone());
                    }
                }
            }
            collect_node_fields(source, out);
        }
        Node::Limit(_, source)
        | Node::Distinct(source) => {
            collect_node_fields(source, out);
        }
        Node::OrderBy(columns, _, source) => {
            for col in columns {
                if let Some(PathSegment::AttrName(name)) = col.path_segments.last() {
                    out.insert(name.clone());
                }
            }
            collect_node_fields(source, out);
        }
        Node::DataSource(_, _) => {}
        Node::CrossJoin(left, right) | Node::Union(left, right) => {
            collect_node_fields(left, out);
            collect_node_fields(right, out);
        }
        Node::LeftJoin(left, right, cond) => {
            collect_node_fields(left, out);
            collect_node_fields(right, out);
            collect_formula_fields(cond, out);
        }
        Node::Intersect(left, right, _) | Node::Except(left, right, _) => {
            collect_node_fields(left, out);
            collect_node_fields(right, out);
        }
    }
}

/// Extract field names referenced by a Formula and resolve to schema indices.
pub(crate) fn extract_fields_from_formula(formula: &Formula, schema: &LogSchema) -> Vec<usize> {
    let mut names = HashSet::new();
    collect_formula_fields(formula, &mut names);
    resolve_field_names(&names, schema)
}

/// Extract required field indices from a Node tree for a given log schema.
/// Returns all field indices if a wildcard (*) is found.
pub(crate) fn extract_required_fields(node: &Node, schema: &LogSchema) -> Vec<usize> {
    let mut names = HashSet::new();
    collect_node_fields(node, &mut names);
    resolve_field_names(&names, schema)
}

fn resolve_field_names(names: &HashSet<String>, schema: &LogSchema) -> Vec<usize> {
    if names.contains("*") {
        return (0..schema.field_count()).collect();
    }
    let mut indices: Vec<usize> = names.iter()
        .filter_map(|name| schema.field_index(name))
        .collect();
    indices.sort();
    indices.dedup();
    indices
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{DataSource, Value};
    use crate::execution::types::Relation;

    fn elb_schema() -> LogSchema {
        LogSchema::from_format("elb")
    }

    #[test]
    fn test_simple_filter_extracts_one_field() {
        let schema = elb_schema();
        // WHERE elb_status_code = '200'
        let formula = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("elb_status_code".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::String("200".to_string()))),
        );
        let fields = extract_fields_from_formula(&formula, &schema);
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0], schema.field_index("elb_status_code").unwrap());
    }

    #[test]
    fn test_and_filter_extracts_both_fields() {
        let schema = elb_schema();
        let f1 = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("elb_status_code".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::String("200".to_string()))),
        );
        let f2 = Formula::Predicate(
            Relation::MoreThan,
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("request_processing_time".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::Float(ordered_float::OrderedFloat(0.5)))),
        );
        let formula = Formula::And(Box::new(f1), Box::new(f2));
        let fields = extract_fields_from_formula(&formula, &schema);
        assert_eq!(fields.len(), 2);
    }

    #[test]
    fn test_node_star_returns_all_fields() {
        let schema = elb_schema();
        let ds = Node::DataSource(
            DataSource::File("test.log".into(), "elb".to_string(), "elb".to_string()),
            vec![],
        );
        let map = Node::Map(vec![Named::Star], Box::new(ds));
        let fields = extract_required_fields(&map, &schema);
        assert_eq!(fields.len(), schema.field_count());
    }

    #[test]
    fn test_node_select_specific_fields() {
        let schema = elb_schema();
        let ds = Node::DataSource(
            DataSource::File("test.log".into(), "elb".to_string(), "elb".to_string()),
            vec![],
        );
        let map = Node::Map(vec![
            Named::Expression(
                Expression::Variable(PathExpr::new(vec![
                    PathSegment::AttrName("elb_status_code".to_string()),
                ])),
                None,
            ),
        ], Box::new(ds));
        let fields = extract_required_fields(&map, &schema);
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0], schema.field_index("elb_status_code").unwrap());
    }

    #[test]
    fn test_filter_plus_projection() {
        let schema = elb_schema();
        let ds = Node::DataSource(
            DataSource::File("test.log".into(), "elb".to_string(), "elb".to_string()),
            vec![],
        );
        let filter = Node::Filter(
            Box::new(ds),
            Box::new(Formula::Predicate(
                Relation::Equal,
                Box::new(Expression::Variable(PathExpr::new(vec![
                    PathSegment::AttrName("elb_status_code".to_string()),
                ]))),
                Box::new(Expression::Constant(Value::String("200".to_string()))),
            )),
        );
        let map = Node::Map(vec![
            Named::Expression(
                Expression::Variable(PathExpr::new(vec![
                    PathSegment::AttrName("timestamp".to_string()),
                ])),
                None,
            ),
        ], Box::new(filter));
        let fields = extract_required_fields(&map, &schema);
        // Should include both timestamp (projection) and elb_status_code (filter)
        assert_eq!(fields.len(), 2);
        assert!(fields.contains(&schema.field_index("timestamp").unwrap()));
        assert!(fields.contains(&schema.field_index("elb_status_code").unwrap()));
    }

    #[test]
    fn test_expression_predicate_recurses() {
        let schema = elb_schema();
        let formula = Formula::ExpressionPredicate(Box::new(
            Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("ssl_protocol".to_string()),
            ])),
        ));
        let fields = extract_fields_from_formula(&formula, &schema);
        assert_eq!(fields.len(), 1);
    }

    #[test]
    fn test_like_extracts_field() {
        let schema = elb_schema();
        let formula = Formula::Like(
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("request".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::String("%GET%".to_string()))),
        );
        let fields = extract_fields_from_formula(&formula, &schema);
        assert!(fields.len() >= 1);
    }

    #[test]
    fn test_unknown_field_ignored() {
        let schema = elb_schema();
        let formula = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("nonexistent_field".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::String("x".to_string()))),
        );
        let fields = extract_fields_from_formula(&formula, &schema);
        assert_eq!(fields.len(), 0);
    }
}
```

### 1b. Run tests (should fail — module not registered)

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::field_analysis
```

### 1c. Register the module

Add `pub mod field_analysis;` to `src/execution/mod.rs`.

### 1d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::field_analysis
```

### 1e. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git add src/execution/field_analysis.rs src/execution/mod.rs && git commit -m "execution: add field analysis for extracting required field indices from plan tree"
```

---

## Step 2: Extend parse_field_column_selected for all types

**File**: `src/execution/field_parser.rs`

### 2a. Write failing tests

```rust
// Append to existing tests module in src/execution/field_parser.rs

#[test]
fn test_parse_int_selected_skips_inactive() {
    let lines: Vec<Vec<u8>> = vec![
        b"100".to_vec(),
        b"200".to_vec(),
        b"300".to_vec(),
    ];
    let fields = vec![
        vec![(0, 3)],
        vec![(0, 3)],
        vec![(0, 3)],
    ];
    let mut sel_bm = Bitmap::all_unset(3);
    sel_bm.set(0);
    sel_bm.set(2);
    let sel = SelectionVector::Bitmap(sel_bm);
    let col = parse_field_column_selected(&lines, &fields, 0, &DataType::Integral, &sel);
    match col {
        TypedColumn::Int32 { data, null, .. } => {
            assert_eq!(data[0], 100);
            // row 1 inactive — should be 0 with null unset
            assert_eq!(data[1], 0);
            assert!(!null.is_set(1));
            assert_eq!(data[2], 300);
        }
        _ => panic!("expected Int32"),
    }
}

#[test]
fn test_parse_float_selected_skips_inactive() {
    let lines: Vec<Vec<u8>> = vec![
        b"1.5".to_vec(),
        b"2.5".to_vec(),
        b"3.5".to_vec(),
    ];
    let fields = vec![
        vec![(0, 3)],
        vec![(0, 3)],
        vec![(0, 3)],
    ];
    let mut sel_bm = Bitmap::all_unset(3);
    sel_bm.set(0);
    let sel = SelectionVector::Bitmap(sel_bm);
    let col = parse_field_column_selected(&lines, &fields, 0, &DataType::Float, &sel);
    match col {
        TypedColumn::Float32 { data, null, .. } => {
            assert!((data[0] - 1.5).abs() < 0.01);
            // rows 1,2 inactive — default 0.0
            assert_eq!(data[1], 0.0);
            assert!(!null.is_set(1));
            assert_eq!(data[2], 0.0);
            assert!(!null.is_set(2));
        }
        _ => panic!("expected Float32"),
    }
}
```

### 2b. Run tests (should fail — currently falls back to parse_field_column for non-String)

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::field_parser::tests::test_parse_int_selected
```

### 2c. Write implementation

Replace the catch-all `_ => parse_field_column(...)` in `parse_field_column_selected` with type-specific implementations that skip inactive rows:

```rust
// Replace the existing parse_field_column_selected function body
pub(crate) fn parse_field_column_selected(
    lines: &[Vec<u8>],
    fields: &[Vec<(usize, usize)>],
    field_idx: usize,
    datatype: &DataType,
    selection: &SelectionVector,
) -> TypedColumn {
    let len = lines.len();
    match datatype {
        DataType::String => {
            // ... (unchanged — already handles selection)
            let mut data_builder = PaddedVecBuilder::<u8>::new();
            let mut offsets_builder = PaddedVecBuilder::<u32>::with_capacity(len + 1);
            offsets_builder.push(0);
            let null_bm = Bitmap::all_set(len);
            let missing_bm = Bitmap::all_set(len);
            for row in 0..len {
                if selection.is_active(row, len) && field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let bytes = strip_quotes(&lines[row][start..end]);
                    data_builder.extend_from_slice(bytes);
                }
                offsets_builder.push(data_builder.len() as u32);
            }
            TypedColumn::Utf8 {
                data: data_builder.seal(),
                offsets: offsets_builder.seal(),
                null: null_bm,
                missing: missing_bm,
            }
        }
        DataType::Integral => {
            let mut data = Vec::with_capacity(len);
            let mut null_bm = Bitmap::all_set(len);
            let missing_bm = Bitmap::all_set(len);
            for row in 0..len {
                if !selection.is_active(row, len) {
                    data.push(0);
                    null_bm.unset(row);
                    continue;
                }
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let s = std::str::from_utf8(&lines[row][start..end]).unwrap_or("0");
                    match s.parse::<i32>() {
                        Ok(v) => data.push(v),
                        Err(_) => { data.push(0); null_bm.unset(row); }
                    }
                } else {
                    data.push(0);
                    null_bm.unset(row);
                }
            }
            TypedColumn::Int32 {
                data: PaddedVec::from_vec(data),
                null: null_bm,
                missing: missing_bm,
            }
        }
        DataType::Float => {
            let mut data = Vec::with_capacity(len);
            let mut null_bm = Bitmap::all_set(len);
            let missing_bm = Bitmap::all_set(len);
            for row in 0..len {
                if !selection.is_active(row, len) {
                    data.push(0.0);
                    null_bm.unset(row);
                    continue;
                }
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let s = std::str::from_utf8(&lines[row][start..end]).unwrap_or("0");
                    match s.parse::<f32>() {
                        Ok(v) => data.push(v),
                        Err(_) => { data.push(0.0); null_bm.unset(row); }
                    }
                } else {
                    data.push(0.0);
                    null_bm.unset(row);
                }
            }
            TypedColumn::Float32 {
                data: PaddedVec::from_vec(data),
                null: null_bm,
                missing: missing_bm,
            }
        }
        DataType::DateTime => {
            let mut data = Vec::with_capacity(len);
            let null_bm = Bitmap::all_set(len);
            let missing_bm = Bitmap::all_set(len);
            for row in 0..len {
                if !selection.is_active(row, len) {
                    data.push(Value::Null);
                    continue;
                }
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let raw = &lines[row][start..end];
                    let s = std::str::from_utf8(strip_quotes(raw)).unwrap_or("");
                    match crate::execution::datasource::parse_utc_timestamp(s) {
                        Ok(dt) => data.push(Value::DateTime(dt)),
                        Err(_) => data.push(Value::Null),
                    }
                } else {
                    data.push(Value::Null);
                }
            }
            TypedColumn::Mixed { data, null: null_bm, missing: missing_bm }
        }
        DataType::Host => {
            let mut data = Vec::with_capacity(len);
            let null_bm = Bitmap::all_set(len);
            let missing_bm = Bitmap::all_set(len);
            for row in 0..len {
                if !selection.is_active(row, len) {
                    data.push(Value::Null);
                    continue;
                }
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let s = std::str::from_utf8(&lines[row][start..end]).unwrap_or("-");
                    if s == "-" {
                        data.push(Value::Null);
                    } else {
                        match crate::common::types::parse_host(s) {
                            Ok(host) => data.push(Value::Host(Box::new(host))),
                            Err(_) => data.push(Value::Null),
                        }
                    }
                } else {
                    data.push(Value::Null);
                }
            }
            TypedColumn::Mixed { data, null: null_bm, missing: missing_bm }
        }
        DataType::HttpRequest => {
            let mut data = Vec::with_capacity(len);
            let null_bm = Bitmap::all_set(len);
            let missing_bm = Bitmap::all_set(len);
            for row in 0..len {
                if !selection.is_active(row, len) {
                    data.push(Value::Null);
                    continue;
                }
                if field_idx < fields[row].len() {
                    let (start, end) = fields[row][field_idx];
                    let raw = &lines[row][start..end];
                    let s = std::str::from_utf8(strip_quotes(raw)).unwrap_or("");
                    match crate::common::types::parse_http_request(s) {
                        Ok(req) => data.push(Value::HttpRequest(Box::new(req))),
                        Err(_) => data.push(Value::Null),
                    }
                } else {
                    data.push(Value::Null);
                }
            }
            TypedColumn::Mixed { data, null: null_bm, missing: missing_bm }
        }
    }
}
```

### 2d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::field_parser
```

### 2e. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git add src/execution/field_parser.rs && git commit -m "execution: extend parse_field_column_selected to skip inactive rows for all types"
```

---

## Step 3: Thread required_fields through try_get_batch

**File**: `src/execution/types.rs`

### 3a. Write failing test

```rust
// Add to existing tests in src/execution/types.rs

#[test]
fn test_try_get_batch_uses_required_fields() {
    // This test verifies that try_get_batch passes required_fields to BatchScanOperator
    // instead of all fields. We test indirectly through Node::get().
    let path = std::path::PathBuf::from("data/AWSELB.log");
    if !path.exists() { return; }

    let registry = test_registry();
    let ds = Node::DataSource(
        DataSource::File(path, "elb".to_string(), "elb".to_string()),
        vec![],
    );
    // SELECT elb_status_code FROM elb — should only need 1 field
    let map = Node::Map(vec![
        Named::Expression(
            Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("elb_status_code".to_string()),
            ])),
            None,
        ),
    ], Box::new(ds));

    let mut stream = map.get(Variables::new(), registry).unwrap();
    let record = stream.next().unwrap().unwrap();
    // Verify the record has elb_status_code
    assert!(record.to_variables().contains_key("elb_status_code"));
}
```

### 3b. Modify try_get_batch signature

Change `try_get_batch` to accept `required_fields`:

```rust
fn try_get_batch(
    &self,
    variables: &Variables,
    registry: &Arc<FunctionRegistry>,
    required_fields: &[usize],
) -> Option<CreateStreamResult<Box<dyn BatchStream>>>
```

### 3c. Update Node::get() to compute and pass required_fields

```rust
pub fn get(&self, variables: Variables, registry: Arc<FunctionRegistry>) -> CreateStreamResult<Box<dyn RecordStream>> {
    // Try batch pipeline first for supported node patterns
    // Compute required fields — need a schema context to resolve names
    let required = self.compute_required_fields_for_batch();
    if let Some(batch_result) = self.try_get_batch(&variables, &registry, &required) {
        let batch_stream = batch_result?;
        return Ok(Box::new(BatchToRowAdapter::new(batch_stream)));
    }
    // ... rest unchanged
}
```

Add helper method on Node:

```rust
fn compute_required_fields_for_batch(&self) -> Vec<usize> {
    // Find the DataSource leaf to determine the schema
    if let Some((format, _)) = self.find_datasource_format() {
        if format != "jsonl" {
            let schema = LogSchema::from_format(&format);
            return crate::execution::field_analysis::extract_required_fields(self, &schema);
        }
    }
    vec![] // No schema — will skip batch path anyway
}

fn find_datasource_format(&self) -> Option<(String, std::path::PathBuf)> {
    match self {
        Node::DataSource(DataSource::File(path, format, _), _) => {
            Some((format.clone(), path.clone()))
        }
        Node::Filter(source, _) | Node::Map(_, source) | Node::Limit(_, source)
        | Node::Distinct(source) | Node::OrderBy(_, _, source) => {
            source.find_datasource_format()
        }
        Node::GroupBy(_, _, source) => source.find_datasource_format(),
        _ => None,
    }
}
```

### 3d. Update Node::DataSource in try_get_batch to use required_fields

```rust
Node::DataSource(data_source, bindings) => {
    if !bindings.is_empty() {
        return None;
    }
    match data_source {
        DataSource::File(path, file_format, _) => {
            if file_format == "jsonl" {
                return None;
            }
            let schema = LogSchema::from_format(file_format);
            // Use required_fields instead of all fields
            let fields = if required_fields.is_empty() {
                (0..schema.field_count()).collect()
            } else {
                required_fields.to_vec()
            };
            match std::fs::File::open(path) {
                Ok(file) => {
                    let reader: Box<dyn std::io::BufRead> =
                        Box::new(std::io::BufReader::new(file));
                    let scan = BatchScanOperator::new(reader, schema, fields);
                    Some(Ok(Box::new(scan) as Box<dyn BatchStream>))
                }
                Err(_) => Some(Err(CreateStreamError::Io)),
            }
        }
        _ => None,
    }
}
```

### 3e. Update existing Node::Filter and Node::Limit arms to pass required_fields

```rust
Node::Filter(source, formula) => {
    match source.try_get_batch(variables, registry, required_fields) {
        // ... same as before
    }
}
Node::Limit(count, source) => {
    match source.try_get_batch(variables, registry, required_fields) {
        // ... same as before
    }
}
```

### 3f. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 3g. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git add src/execution/types.rs && git commit -m "execution: thread required_fields through try_get_batch for selective field parsing"
```

---

## Step 4: Two-phase BatchScanOperator

**File**: `src/execution/batch_scan.rs`

### 4a. Write failing tests

```rust
// Add to existing tests module in src/execution/batch_scan.rs

#[test]
fn test_two_phase_scan_filters_rows() {
    use crate::execution::types::{Formula, Expression, Relation};
    use crate::syntax::ast::{PathExpr, PathSegment};
    use crate::common::types::Value;
    use crate::execution::stream::RecordStream;
    use crate::execution::batch::BatchToRowAdapter;
    use std::sync::Arc;

    // Create test data: 4 lines, filter on field 0 = "match"
    let data = b"match extra1\nskip extra2\nmatch extra3\nskip extra4\n";
    let reader: Box<dyn std::io::BufRead> = Box::new(std::io::Cursor::new(data.to_vec()));

    let schema = LogSchema::from_format("squid");
    // Project fields 0 and 1, filter on field 0
    let projected = vec![0, 1];
    let filter_fields = vec![0];

    let formula = Formula::Predicate(
        Relation::Equal,
        Box::new(Expression::Variable(PathExpr::new(vec![
            PathSegment::AttrName(schema.field_name(0).to_string()),
        ]))),
        Box::new(Expression::Constant(Value::String("match".to_string()))),
    );
    let registry = Arc::new(crate::functions::register_all().unwrap());
    let variables = crate::common::types::Variables::new();

    let scan = BatchScanOperator::new(
        reader, schema, projected, filter_fields,
        Some((formula, variables, registry)),
    );
    let mut adapter = BatchToRowAdapter::new(Box::new(scan));

    // Should only get 2 rows (the "match" ones)
    let r1 = adapter.next().unwrap();
    assert!(r1.is_some());
    let r2 = adapter.next().unwrap();
    assert!(r2.is_some());
    let r3 = adapter.next().unwrap();
    assert!(r3.is_none());
}

#[test]
fn test_two_phase_scan_no_predicate_unchanged() {
    // Without a pushed predicate, behavior is identical to before
    let data = b"hello world\nfoo bar\n";
    let reader: Box<dyn std::io::BufRead> = Box::new(std::io::Cursor::new(data.to_vec()));
    let schema = LogSchema::from_format("squid");
    let projected = vec![0, 1];
    let mut scan = BatchScanOperator::new(reader, schema, projected, vec![], None);

    let batch = scan.next_batch().unwrap().unwrap();
    assert_eq!(batch.len, 2);
    assert_eq!(batch.columns.len(), 2);
}
```

### 4b. Update BatchScanOperator to support two-phase parsing

```rust
use crate::execution::batch_predicate::evaluate_batch_predicate;
use crate::execution::field_parser::parse_field_column_selected;
use crate::execution::types::Formula;
use crate::common::types::Variables;
use crate::functions::FunctionRegistry;
use std::sync::Arc;

pub(crate) struct BatchScanOperator {
    reader: Box<dyn BufRead>,
    schema: LogSchema,
    projected_fields: Vec<usize>,
    filter_field_indices: Vec<usize>,
    pushed_predicate: Option<(Formula, Variables, Arc<FunctionRegistry>)>,
    batch_schema: BatchSchema,
    done: bool,
    buf: String,
}

impl BatchScanOperator {
    pub fn new(
        reader: Box<dyn BufRead>,
        schema: LogSchema,
        projected_fields: Vec<usize>,
        filter_field_indices: Vec<usize>,
        pushed_predicate: Option<(Formula, Variables, Arc<FunctionRegistry>)>,
    ) -> Self {
        let batch_schema = BatchSchema {
            names: projected_fields.iter()
                .map(|&i| schema.field_name(i).to_string())
                .collect(),
            types: projected_fields.iter()
                .map(|&i| datatype_to_column_type(&schema.field_type(i)))
                .collect(),
        };
        Self {
            reader, schema, projected_fields, filter_field_indices,
            pushed_predicate, batch_schema, done: false,
            buf: String::with_capacity(512),
        }
    }

    // read_lines unchanged
}

impl BatchStream for BatchScanOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        loop {
            if self.done { return Ok(None); }

            let lines = self.read_lines();
            if lines.is_empty() { return Ok(None); }

            let len = lines.len();
            let line_fields: Vec<Vec<(usize, usize)>> = lines.iter()
                .map(|line| tokenize_line(line)).collect();

            if let Some((ref formula, ref variables, ref registry)) = self.pushed_predicate {
                // Phase 1: Parse filter fields only
                let mut filter_columns = Vec::with_capacity(self.filter_field_indices.len());
                let mut filter_names = Vec::with_capacity(self.filter_field_indices.len());
                for &fi in &self.filter_field_indices {
                    let dt = self.schema.field_type(fi);
                    let col = parse_field_column(&lines, &line_fields, fi, &dt);
                    filter_columns.push(col);
                    filter_names.push(self.schema.field_name(fi).to_string());
                }

                let filter_batch = ColumnBatch {
                    columns: filter_columns,
                    names: filter_names,
                    selection: SelectionVector::All,
                    len,
                };

                let selection_bm = evaluate_batch_predicate(formula, &filter_batch, variables, registry)?;

                if selection_bm.count_ones() == 0 {
                    continue; // All rows filtered, try next batch
                }

                let selection = SelectionVector::Bitmap(selection_bm);

                // Phase 2: Parse remaining projected fields with selection
                let mut all_columns = Vec::with_capacity(self.projected_fields.len());
                let mut all_names = Vec::with_capacity(self.projected_fields.len());

                // Decompose filter_batch to reuse its columns
                let ColumnBatch { columns: filter_cols, names: filter_col_names, .. } = filter_batch;
                let mut filter_map: Vec<(usize, TypedColumn)> = self.filter_field_indices.iter()
                    .copied().zip(filter_cols.into_iter()).collect();

                for &pi in &self.projected_fields {
                    // Check if this field was already parsed in Phase 1
                    if let Some(pos) = filter_map.iter().position(|(fi, _)| *fi == pi) {
                        let (_, col) = filter_map.remove(pos);
                        all_columns.push(col);
                    } else {
                        // Phase 2: parse with selection — skip inactive rows
                        let dt = self.schema.field_type(pi);
                        let col = parse_field_column_selected(
                            &lines, &line_fields, pi, &dt, &selection,
                        );
                        all_columns.push(col);
                    }
                    all_names.push(self.schema.field_name(pi).to_string());
                }

                return Ok(Some(ColumnBatch {
                    columns: all_columns,
                    names: all_names,
                    selection,
                    len,
                }));
            } else {
                // No predicate: parse all projected fields (current behavior)
                let mut columns = Vec::with_capacity(self.projected_fields.len());
                let mut names = Vec::with_capacity(self.projected_fields.len());
                for &fi in &self.projected_fields {
                    let dt = self.schema.field_type(fi);
                    let col = parse_field_column(&lines, &line_fields, fi, &dt);
                    columns.push(col);
                    names.push(self.schema.field_name(fi).to_string());
                }
                return Ok(Some(ColumnBatch {
                    columns, names,
                    selection: SelectionVector::All,
                    len,
                }));
            }
        }
    }

    fn schema(&self) -> &BatchSchema { &self.batch_schema }
    fn close(&self) {}
}
```

### 4c. Update existing tests to use new constructor signature

The existing tests in `batch_scan.rs` call `BatchScanOperator::new(reader, schema, all_fields)`. Update to `BatchScanOperator::new(reader, schema, all_fields, vec![], None)`.

### 4d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch_scan
```

### 4e. Run full test suite (constructor change may break callers)

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 4f. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git add src/execution/batch_scan.rs && git commit -m "execution: add two-phase lazy parsing to BatchScanOperator with pushed predicates"
```

---

## Step 5: Predicate pushdown in try_get_batch

**File**: `src/execution/types.rs`

### 5a. Write test

```rust
// Add to tests in src/execution/types.rs

#[test]
fn test_predicate_pushdown_produces_correct_results() {
    let path = std::path::PathBuf::from("data/AWSELB.log");
    if !path.exists() { return; }

    let registry = test_registry();

    // Build: SELECT elb_status_code FROM elb WHERE elb_status_code = '200'
    let ds = Node::DataSource(
        DataSource::File(path.clone(), "elb".to_string(), "elb".to_string()),
        vec![],
    );
    let filter = Node::Filter(
        Box::new(ds),
        Box::new(Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("elb_status_code".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::String("200".to_string()))),
        )),
    );

    let mut stream = filter.get(Variables::new(), registry).unwrap();
    let mut count = 0;
    while let Some(record) = stream.next().unwrap() {
        assert_eq!(
            record.to_variables().get("elb_status_code"),
            Some(&Value::String("200".to_string()))
        );
        count += 1;
    }
    assert!(count > 0);
}
```

### 5b. Modify Node::Filter arm in try_get_batch for predicate pushdown

```rust
Node::Filter(source, formula) => {
    // Try predicate pushdown into DataSource
    if let Node::DataSource(ds, bindings) = &**source {
        if bindings.is_empty() {
            if let DataSource::File(path, file_format, _) = ds {
                if file_format != "jsonl" {
                    let schema = LogSchema::from_format(file_format);
                    let filter_fields = crate::execution::field_analysis::extract_fields_from_formula(formula, &schema);
                    // Union filter fields with required_fields from parent
                    let mut all_fields = required_fields.to_vec();
                    for &fi in &filter_fields {
                        if !all_fields.contains(&fi) {
                            all_fields.push(fi);
                        }
                    }
                    all_fields.sort();
                    all_fields.dedup();
                    if all_fields.is_empty() {
                        all_fields = (0..schema.field_count()).collect();
                    }

                    match std::fs::File::open(path) {
                        Ok(file) => {
                            let reader: Box<dyn std::io::BufRead> =
                                Box::new(std::io::BufReader::new(file));
                            let scan = BatchScanOperator::new(
                                reader, schema, all_fields,
                                filter_fields,
                                Some((*formula.clone(), variables.clone(), registry.clone())),
                            );
                            return Some(Ok(Box::new(scan) as Box<dyn BatchStream>));
                        }
                        Err(_) => return Some(Err(CreateStreamError::Io)),
                    }
                }
            }
        }
    }
    // Fallback: wrap with BatchFilterOperator
    match source.try_get_batch(variables, registry, required_fields) {
        Some(Ok(batch_stream)) => {
            let filter = BatchFilterOperator::new(
                batch_stream, *formula.clone(), variables.clone(), registry.clone(),
            );
            Some(Ok(Box::new(filter) as Box<dyn BatchStream>))
        }
        Some(Err(e)) => Some(Err(e)),
        None => None,
    }
}
```

### 5c. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 5d. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git add src/execution/types.rs && git commit -m "execution: add predicate pushdown into BatchScanOperator for two-phase parsing"
```

---

## Step 6: Implement BatchGroupByOperator

**File (create)**: `src/execution/batch_groupby.rs`

### 6a. Write failing tests

```rust
// src/execution/batch_groupby.rs

use crate::common::types::{Tuple, Value, Variables};
use crate::execution::batch::*;
use crate::execution::types::{
    Aggregate, Aggregate as Agg, CountAggregate, SumAggregate, AvgAggregate,
    MinAggregate, MaxAggregate, FirstAggregate, LastAggregate,
    ApproxCountDistinctAggregate, GroupAsAggregate,
    NamedAggregate, Named, Expression, StreamResult, StreamError,
};
use crate::execution::stream::RecordStream;
use crate::functions::FunctionRegistry;
use crate::simd::selection::SelectionVector;
use crate::syntax::ast::{PathExpr, PathSegment};
use hashbrown::HashSet;
use std::sync::Arc;

/// Batch-native GroupBy operator. Consumes all batches from child,
/// accumulates per-group aggregates, then emits results as a single batch.
pub(crate) struct BatchGroupByOperator {
    child: Box<dyn BatchStream>,
    group_keys: Vec<PathExpr>,
    aggregates: Vec<NamedAggregate>,
    variables: Variables,
    registry: Arc<FunctionRegistry>,
    consumed: bool,
    result: Option<ColumnBatch>,
}

impl BatchGroupByOperator {
    pub fn new(
        child: Box<dyn BatchStream>,
        group_keys: Vec<PathExpr>,
        aggregates: Vec<NamedAggregate>,
        variables: Variables,
        registry: Arc<FunctionRegistry>,
    ) -> Self {
        Self {
            child, group_keys, aggregates, variables, registry,
            consumed: false, result: None,
        }
    }
}

impl BatchStream for BatchGroupByOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        if self.consumed {
            return Ok(self.result.take());
        }
        self.consumed = true;

        // We reuse the existing Aggregate machinery which already has
        // per-key HashMap storage via add_record/get_aggregated.
        let mut groups: HashSet<Option<Tuple>> = HashSet::new();
        let mut aggregates = self.aggregates.clone();

        while let Some(batch) = self.child.next_batch()? {
            for row in 0..batch.len {
                if !batch.selection.is_active(row, batch.len) {
                    continue;
                }

                // Build group key
                let key = if self.group_keys.is_empty() {
                    None
                } else {
                    let mut key_values = Vec::with_capacity(self.group_keys.len());
                    for key_path in &self.group_keys {
                        // Find the column for this key
                        if let Some(PathSegment::AttrName(name)) = key_path.path_segments.last() {
                            if let Some(col_idx) = batch.names.iter().position(|n| n == name) {
                                key_values.push(BatchToRowAdapter::extract_value(&batch.columns[col_idx], row));
                            } else {
                                key_values.push(Value::Missing);
                            }
                        } else {
                            key_values.push(Value::Missing);
                        }
                    }
                    Some(key_values)
                };

                groups.insert(key.clone());

                // Build row variables for expression evaluation
                let mut row_vars = self.variables.clone();
                for (i, col) in batch.columns.iter().enumerate() {
                    let val = BatchToRowAdapter::extract_value(col, row);
                    row_vars.insert(batch.names[i].clone(), val);
                }

                // Update each aggregate
                for na in aggregates.iter_mut() {
                    match &mut na.aggregate {
                        Agg::Count(ref mut inner, named) => {
                            match named {
                                Named::Star => { inner.add_row(&key)?; }
                                Named::Expression(expr, _) => {
                                    let val = expr.expression_value(&row_vars, &self.registry)
                                        .map_err(|e| StreamError::Expression(e))?;
                                    inner.add_record(&key, &val)?;
                                }
                            }
                        }
                        Agg::Sum(ref mut inner, Named::Expression(expr, _)) => {
                            let val = expr.expression_value(&row_vars, &self.registry)
                                .map_err(|e| StreamError::Expression(e))?;
                            inner.add_record(&key, &val)?;
                        }
                        Agg::Avg(ref mut inner, Named::Expression(expr, _)) => {
                            let val = expr.expression_value(&row_vars, &self.registry)
                                .map_err(|e| StreamError::Expression(e))?;
                            inner.add_record(&key, &val)?;
                        }
                        Agg::Min(ref mut inner, Named::Expression(expr, _)) => {
                            let val = expr.expression_value(&row_vars, &self.registry)
                                .map_err(|e| StreamError::Expression(e))?;
                            inner.add_record(&key, &val)?;
                        }
                        Agg::Max(ref mut inner, Named::Expression(expr, _)) => {
                            let val = expr.expression_value(&row_vars, &self.registry)
                                .map_err(|e| StreamError::Expression(e))?;
                            inner.add_record(&key, &val)?;
                        }
                        Agg::First(ref mut inner, Named::Expression(expr, _)) => {
                            let val = expr.expression_value(&row_vars, &self.registry)
                                .map_err(|e| StreamError::Expression(e))?;
                            inner.add_record(&key, &val)?;
                        }
                        Agg::Last(ref mut inner, Named::Expression(expr, _)) => {
                            let val = expr.expression_value(&row_vars, &self.registry)
                                .map_err(|e| StreamError::Expression(e))?;
                            inner.add_record(&key, &val)?;
                        }
                        Agg::ApproxCountDistinct(ref mut inner, Named::Expression(expr, _)) => {
                            let val = expr.expression_value(&row_vars, &self.registry)
                                .map_err(|e| StreamError::Expression(e))?;
                            inner.add_record(&key, &val)?;
                        }
                        Agg::GroupAs(ref mut inner, named) => {
                            let val = match named {
                                Named::Expression(_, _) => {
                                    Value::Object(Box::new(row_vars.clone()))
                                }
                                Named::Star => unreachable!(),
                            };
                            inner.add_record(&key, &val)?;
                        }
                        Agg::PercentileDisc(ref mut inner, col_name) => {
                            let val = row_vars.get(col_name).cloned().unwrap_or(Value::Null);
                            inner.add_record(&key, &val)?;
                        }
                        Agg::ApproxPercentile(ref mut inner, col_name) => {
                            let val = row_vars.get(col_name).cloned().unwrap_or(Value::Null);
                            inner.add_record(&key, &val)?;
                        }
                        _ => {} // Star variants for non-Count aggregates are unreachable
                    }
                }
            }
        }

        // Emit results: convert groups + aggregates into a ColumnBatch
        // Use Mixed columns for simplicity (the BatchToRowAdapter will
        // materialize them into Records for downstream operators)
        let group_list: Vec<Option<Tuple>> = groups.into_iter().collect();

        if group_list.is_empty() && self.group_keys.is_empty() {
            // No rows, no GROUP BY keys → emit one row with defaults
            let mut values = Vec::new();
            let mut names = Vec::new();
            for (idx, na) in aggregates.iter_mut().enumerate() {
                let name = na.name_opt.clone().unwrap_or_else(|| format!("_{}", idx + 1));
                names.push(name);
                // For COUNT, default is 0; for others, NULL
                let val = match &na.aggregate {
                    Agg::Count(_, _) => Value::Int(0),
                    _ => Value::Null,
                };
                values.push(val);
            }
            let columns: Vec<TypedColumn> = values.into_iter().map(|v| {
                TypedColumn::Mixed {
                    data: vec![v],
                    null: crate::simd::bitmap::Bitmap::all_set(1),
                    missing: crate::simd::bitmap::Bitmap::all_set(1),
                }
            }).collect();
            self.result = Some(ColumnBatch {
                columns, names,
                selection: SelectionVector::All,
                len: 1,
            });
            return Ok(self.result.take());
        }

        if group_list.is_empty() {
            return Ok(None);
        }

        let num_rows = group_list.len();
        let mut all_names = Vec::new();
        let mut all_values: Vec<Vec<Value>> = Vec::new();

        // Group key columns
        if !self.group_keys.is_empty() {
            for (ki, key_path) in self.group_keys.iter().enumerate() {
                let name = match key_path.path_segments.last() {
                    Some(PathSegment::AttrName(s)) => s.clone(),
                    _ => format!("_{}", ki + 1),
                };
                all_names.push(name);
                let mut col_vals = Vec::with_capacity(num_rows);
                for key in &group_list {
                    if let Some(ref key_vals) = key {
                        col_vals.push(key_vals[ki].clone());
                    } else {
                        col_vals.push(Value::Null);
                    }
                }
                all_values.push(col_vals);
            }
        }

        // Aggregate columns
        for (ai, na) in aggregates.iter_mut().enumerate() {
            let name = na.name_opt.clone().unwrap_or_else(|| {
                format!("_{}", all_names.len() + 1)
            });
            all_names.push(name);
            let mut col_vals = Vec::with_capacity(num_rows);
            for key in &group_list {
                let val = na.aggregate.get_aggregated(key)
                    .unwrap_or(Value::Null);
                col_vals.push(val);
            }
            all_values.push(col_vals);
        }

        // Build ColumnBatch with Mixed columns
        let columns: Vec<TypedColumn> = all_values.into_iter().map(|vals| {
            TypedColumn::Mixed {
                data: vals,
                null: crate::simd::bitmap::Bitmap::all_set(num_rows),
                missing: crate::simd::bitmap::Bitmap::all_set(num_rows),
            }
        }).collect();

        self.result = Some(ColumnBatch {
            columns,
            names: all_names,
            selection: SelectionVector::All,
            len: num_rows,
        });
        Ok(self.result.take())
    }

    fn schema(&self) -> &BatchSchema {
        // The schema is dynamic based on group keys + aggregates
        // Return a placeholder — callers use column names, not schema types
        &BatchSchema { names: vec![], types: vec![] }
    }

    fn close(&self) {
        self.child.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simd::bitmap::Bitmap;
    use crate::simd::padded_vec::{PaddedVec, PaddedVecBuilder};
    use crate::common::types::Value;
    use ordered_float::OrderedFloat;

    fn make_test_batch() -> ColumnBatch {
        // 4 rows: status=["200","404","200","200"], time=[1.0, 2.0, 3.0, 4.0]
        let mut data_builder = PaddedVecBuilder::<u8>::new();
        let mut offsets_builder = PaddedVecBuilder::<u32>::with_capacity(5);
        offsets_builder.push(0);
        for s in &["200", "404", "200", "200"] {
            data_builder.extend_from_slice(s.as_bytes());
            offsets_builder.push(data_builder.len() as u32);
        }
        let status_col = TypedColumn::Utf8 {
            data: data_builder.seal(),
            offsets: offsets_builder.seal(),
            null: Bitmap::all_set(4),
            missing: Bitmap::all_set(4),
        };
        let time_col = TypedColumn::Float32 {
            data: PaddedVec::from_vec(vec![1.0, 2.0, 3.0, 4.0]),
            null: Bitmap::all_set(4),
            missing: Bitmap::all_set(4),
        };
        ColumnBatch {
            columns: vec![status_col, time_col],
            names: vec!["status".to_string(), "time".to_string()],
            selection: SelectionVector::All,
            len: 4,
        }
    }

    struct OneBatch { batch: Option<ColumnBatch>, schema: BatchSchema }
    impl BatchStream for OneBatch {
        fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> { Ok(self.batch.take()) }
        fn schema(&self) -> &BatchSchema { &self.schema }
        fn close(&self) {}
    }

    #[test]
    fn test_batch_groupby_count_star() {
        let batch = make_test_batch();
        let schema = BatchSchema {
            names: vec!["status".to_string(), "time".to_string()],
            types: vec![ColumnType::Utf8, ColumnType::Float32],
        };
        let child = Box::new(OneBatch { batch: Some(batch), schema });

        let registry = Arc::new(crate::functions::register_all().unwrap());
        let mut groupby = BatchGroupByOperator::new(
            child,
            vec![PathExpr::new(vec![PathSegment::AttrName("status".to_string())])],
            vec![NamedAggregate::new(
                Aggregate::Count(CountAggregate::new(), Named::Star),
                Some("cnt".to_string()),
            )],
            Variables::new(),
            registry,
        );

        let result = groupby.next_batch().unwrap().unwrap();
        assert_eq!(result.len, 2); // two groups: "200" and "404"

        // Verify via adapter
        let mut adapter = BatchToRowAdapter::new(Box::new(crate::execution::batch::tests_helper::SingleBatch(Some(result))));
        let mut rows = Vec::new();
        while let Some(record) = adapter.next().unwrap() {
            rows.push(record);
        }
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_batch_groupby_empty_no_keys_returns_count_zero() {
        // Empty input, no GROUP BY keys → should return COUNT(*) = 0
        struct EmptyStream { schema: BatchSchema }
        impl BatchStream for EmptyStream {
            fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> { Ok(None) }
            fn schema(&self) -> &BatchSchema { &self.schema }
            fn close(&self) {}
        }
        let schema = BatchSchema { names: vec![], types: vec![] };
        let child = Box::new(EmptyStream { schema });
        let registry = Arc::new(crate::functions::register_all().unwrap());

        let mut groupby = BatchGroupByOperator::new(
            child,
            vec![],
            vec![NamedAggregate::new(
                Aggregate::Count(CountAggregate::new(), Named::Star),
                Some("cnt".to_string()),
            )],
            Variables::new(),
            registry,
        );

        let result = groupby.next_batch().unwrap().unwrap();
        assert_eq!(result.len, 1);
        let val = BatchToRowAdapter::extract_value(&result.columns[0], 0);
        assert_eq!(val, Value::Int(0));
    }
}
```

Note: The `tests_helper` module reference in the first test may need adjustment. Replace with an inline struct if needed. The test for `test_batch_groupby_count_star` verifies correct grouping indirectly — the exact count values depend on ordering which is non-deterministic from HashSet.

### 6b. Register the module

Add `pub mod batch_groupby;` to `src/execution/mod.rs`.

### 6c. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test --lib execution::batch_groupby
```

### 6d. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git add src/execution/batch_groupby.rs src/execution/mod.rs && git commit -m "execution: add BatchGroupByOperator for batch-native aggregation"
```

---

## Step 7: Add Node::Map and Node::GroupBy to try_get_batch

**File**: `src/execution/types.rs`

### 7a. Write tests

```rust
// Add to tests in src/execution/types.rs

#[test]
fn test_batch_groupby_through_node_get() {
    let path = std::path::PathBuf::from("data/AWSELB.log");
    if !path.exists() { return; }

    let registry = test_registry();

    // SELECT elb_status_code, count(*) FROM elb GROUP BY elb_status_code
    let ds = Node::DataSource(
        DataSource::File(path, "elb".to_string(), "elb".to_string()),
        vec![],
    );
    let groupby = Node::GroupBy(
        vec![PathExpr::new(vec![PathSegment::AttrName("elb_status_code".to_string())])],
        vec![NamedAggregate::new(
            Aggregate::Count(CountAggregate::new(), Named::Star),
            Some("cnt".to_string()),
        )],
        Box::new(ds),
    );
    let map = Node::Map(vec![
        Named::Expression(
            Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("elb_status_code".to_string()),
            ])),
            None,
        ),
        Named::Expression(
            Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("cnt".to_string()),
            ])),
            None,
        ),
    ], Box::new(groupby));

    let mut stream = map.get(Variables::new(), registry).unwrap();
    let mut total_rows = 0;
    while let Some(record) = stream.next().unwrap() {
        assert!(record.to_variables().contains_key("elb_status_code"));
        assert!(record.to_variables().contains_key("cnt"));
        total_rows += 1;
    }
    assert!(total_rows > 0);
}
```

### 7b. Add Node::Map arm to try_get_batch

```rust
Node::Map(named_list, source) => {
    // Only handle simple variable projections in batch path
    let is_simple = named_list.iter().all(|named| match named {
        Named::Expression(Expression::Variable(_), _) => true,
        Named::Expression(Expression::Constant(_), _) => true,
        Named::Star => true,
        _ => false,
    });
    if !is_simple {
        return None;
    }
    match source.try_get_batch(variables, registry, required_fields) {
        Some(Ok(batch_stream)) => {
            // Extract output column names from named_list
            let output_names: Vec<String> = named_list.iter().filter_map(|named| {
                match named {
                    Named::Expression(Expression::Variable(path), _) => {
                        path.path_segments.last().and_then(|seg| {
                            if let PathSegment::AttrName(name) = seg {
                                Some(name.clone())
                            } else {
                                None
                            }
                        })
                    }
                    _ => None,
                }
            }).collect();
            if output_names.is_empty() && named_list.iter().any(|n| matches!(n, Named::Star)) {
                // SELECT * — pass through without projection
                return Some(Ok(batch_stream));
            }
            let project = crate::execution::batch_project::BatchProjectOperator::new(
                batch_stream, output_names,
            );
            Some(Ok(Box::new(project) as Box<dyn BatchStream>))
        }
        Some(Err(e)) => Some(Err(e)),
        None => None,
    }
}
```

### 7c. Add Node::GroupBy arm to try_get_batch

```rust
Node::GroupBy(keys, aggregates, source) => {
    match source.try_get_batch(variables, registry, required_fields) {
        Some(Ok(batch_stream)) => {
            let groupby = crate::execution::batch_groupby::BatchGroupByOperator::new(
                batch_stream,
                keys.clone(),
                aggregates.clone(),
                variables.clone(),
                registry.clone(),
            );
            Some(Ok(Box::new(groupby) as Box<dyn BatchStream>))
        }
        Some(Err(e)) => Some(Err(e)),
        None => None,
    }
}
```

### 7d. Run tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 7e. Commit

```bash
cd /Users/paulmeng/Develop/logq && cargo test && git add src/execution/types.rs && git commit -m "execution: add Node::Map and Node::GroupBy to batch pipeline in try_get_batch"
```

---

## Step 8: End-to-end verification and benchmarks

### 8a. Run full test suite

```bash
cd /Users/paulmeng/Develop/logq && cargo test
```

### 8b. Run benchmarks

```bash
cd /Users/paulmeng/Develop/logq && cargo bench --features bench-internals -- profiling/ 2>&1 | tee docs/plans/phase4-benchmarks.txt
```

### 8c. Compare against baseline

Compare results against `docs/plans/phase0-results.md`:
- `profiling/scan_only`: expect improvement from selective parsing
- `profiling/scan_filter`: expect major improvement from two-phase + SIMD
- `profiling/scan_filter_groupby`: expect improvement from batch GroupBy

### 8d. Run representative queries manually

```bash
cd /Users/paulmeng/Develop/logq && cargo build --release

# Scan only
./target/release/logq -f elb "SELECT * FROM elb LIMIT 10" data/AWSELB.log

# Filter
./target/release/logq -f elb "SELECT elb_status_code FROM elb WHERE elb_status_code = '200' LIMIT 10" data/AWSELB.log

# Filter + GroupBy
./target/release/logq -f elb "SELECT elb_status_code, count(*) FROM elb WHERE elb_status_code = '200' GROUP BY elb_status_code" data/AWSELB.log

# GroupBy without filter
./target/release/logq -f elb "SELECT elb_status_code, count(*) FROM elb GROUP BY elb_status_code" data/AWSELB.log
```

### 8e. Document results

Create `docs/plans/phase4-results.md` with before/after comparison table.

### 8f. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add docs/plans/phase4-* && git commit -m "docs: record Phase 4 benchmark results for selective parsing + batch GroupBy"
```
