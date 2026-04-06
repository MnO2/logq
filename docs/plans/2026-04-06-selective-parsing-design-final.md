# Design: Selective Field Parsing, Two-Phase Lazy Parsing, and Batch-Native GroupBy (Final)

**Date**: 2026-04-06
**Goal**: Unlock the projected 2.4x speedup by eliminating the three bottlenecks identified in the Phase 2-3 results.

**Review note**: This design went through 2 rounds of adversarial review. Round 2 identified 3 remaining issues which are all addressed in this final version. Please review carefully before proceeding.

**Changes from v2**:
- Removed spurious `CountDistinct` from Accumulator enum — only `ApproxCountDistinct` exists in the Aggregate enum (Review #2, Critical Issue #1)
- Fixed Formula traversal spec: removed non-existent `Between`, added `Constant`, `ExpressionPredicate`, `NotLike`, `NotIn` (Review #2, Critical Issue #2)
- Resolved `project_fields` placeholder by threading `required_fields: Vec<usize>` parameter through `try_get_batch` (Review #2, Critical Issue #3)
- Clarified column renaming in Node::Map batch path: aliased projections fall back to row path
- Standardized Avg accumulator to use f32 to match existing AvgAggregate precision

## Background

Phase 2-3 wired the batch pipeline into `Node::get()` but benchmarks showed a 12-16% regression because:
1. `BatchScanOperator` parses ALL fields into columns, whereas the old row path parsed on-demand
2. `BatchToRowAdapter` materializes columnar data back to rows for GroupBy/Map
3. No two-phase lazy parsing — filter fields aren't parsed before project fields

The Phase 0 baseline showed filtering at 60.3% of total time, scanning at 27.7%, and aggregation at 12%. The target is 2.4x overall speedup on the representative scan+filter+groupby workload.

## Optimization 1: Selective Field Parsing

### Problem
`try_get_batch` currently passes `(0..schema.field_count()).collect()` (all 17 ELB fields) to `BatchScanOperator`. For `WHERE elb_status_code = '200'`, only field 7 is needed for filtering plus whatever fields are projected.

### Solution: extract_required_fields

New file `src/execution/field_analysis.rs` with:

```rust
pub(crate) fn extract_required_fields(node: &Node, schema: &LogSchema) -> Vec<usize>
```

Walks the physical plan tree and collects field names, resolves to indices via `LogSchema::field_index()`.

**Complete recursive traversal spec**:

For `Expression`:
- `Variable(path)` — extract attribute name from single-segment paths
- `Constant(_)` — no fields
- `Function(name, args)` — recurse into each `Named::Expression(expr, _)`; `Named::Star` contributes all fields
- `Branch(branches, else_expr)` — recurse into each branch's `Formula` condition and `Expression` result; recurse into `else_expr` if present
- `Cast(inner, _)` — recurse into inner expression
- `Logic(formula)` — recurse into the formula
- `Subquery(_)` — conservative fallback: return all schema fields

For `Formula`:
- `Predicate(_, left, right)` — recurse into both expressions
- `And(left, right)`, `Or(left, right)` — recurse into both formulas
- `Not(inner)` — recurse into inner formula
- `IsNull(expr)`, `IsNotNull(expr)` — recurse into expression
- `IsMissing(expr)`, `IsNotMissing(expr)` — recurse into expression
- `Like(expr, pattern)`, `NotLike(expr, pattern)` — recurse into both expressions
- `In(expr, list)`, `NotIn(expr, list)` — recurse into expr and all list expressions
- `Constant(_)` — no fields (boolean constant)
- `ExpressionPredicate(expr)` — recurse into the wrapped expression

For `Node`:
- `Map(named_list, source)` — extract from all Named expressions, recurse into source
- `Filter(source, formula)` — extract from formula, recurse into source
- `GroupBy(fields, aggregates, source)` — extract from group key PathExprs, recurse into each aggregate's expression, recurse into source
- `Limit(_, source)`, `OrderBy(_, _, source)`, `Distinct(source)` — recurse into source
- `DataSource(_, _)` — base case

### Integration: Threading required_fields through try_get_batch

The `try_get_batch` signature changes to accept a `required_fields` parameter:

```rust
fn try_get_batch(
    &self,
    variables: &Variables,
    registry: &Arc<FunctionRegistry>,
    required_fields: &[usize],
) -> Option<CreateStreamResult<Box<dyn BatchStream>>>
```

The root caller in `Node::get()` computes the full required fields once:

```rust
pub fn get(&self, variables: Variables, registry: Arc<FunctionRegistry>) -> CreateStreamResult<Box<dyn RecordStream>> {
    // Compute required fields if we have a log schema context
    let required = self.compute_required_fields();
    if let Some(batch_result) = self.try_get_batch(&variables, &registry, &required) {
        let batch_stream = batch_result?;
        return Ok(Box::new(BatchToRowAdapter::new(batch_stream)));
    }
    // ... existing row-based fallback
}
```

Each `try_get_batch` arm propagates the required fields to children. The `Node::DataSource` arm uses the `required_fields` directly instead of all fields. The `Node::Filter` arm uses them to know what `project_fields` the parent needs (resolving the v2 placeholder):

```rust
Node::Filter(source, formula) => {
    if let Node::DataSource(ds, bindings) = &**source {
        if bindings.is_empty() {
            if let DataSource::File(path, file_format, _) = ds {
                if file_format != "jsonl" {
                    let schema = LogSchema::from_format(file_format);
                    let filter_fields = extract_fields_from_formula(formula, &schema);
                    // required_fields comes from parent — already computed
                    let all_fields = union(&filter_fields, required_fields);
                    let scan = BatchScanOperator::new(
                        reader, schema, all_fields,
                        filter_fields,
                        Some((*formula.clone(), variables.clone(), registry.clone())),
                    );
                    return Some(Ok(Box::new(scan)));
                }
            }
        }
        None
    } else {
        // Non-DataSource child: wrap with BatchFilterOperator
        match source.try_get_batch(variables, registry, required_fields) {
            Some(Ok(batch_stream)) => {
                let filter = BatchFilterOperator::new(
                    batch_stream, *formula.clone(), variables.clone(), registry.clone(),
                );
                Some(Ok(Box::new(filter)))
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}
```

### Change Points
- New file `src/execution/field_analysis.rs`
- Modified `try_get_batch` signature and all call sites in `src/execution/types.rs`

## Optimization 2: Two-Phase Lazy Parsing in BatchScanOperator

### Prerequisite: Extend parse_field_column_selected for all types

Extend `parse_field_column_selected` to skip inactive rows for all types:

```rust
DataType::Integral => {
    for row in 0..len {
        if !selection.is_active(row, len) || field_idx >= fields[row].len() {
            data.push(0);
            null_bm.unset(row);
            continue;
        }
        // ... actual parsing
    }
}
// Same pattern for Float, DateTime, Host, HttpRequest
```

### Solution: Two-Phase BatchScanOperator

Extended constructor:

```rust
pub fn new(
    reader: Box<dyn BufRead>,
    schema: LogSchema,
    projected_fields: Vec<usize>,      // all fields needed in output
    filter_field_indices: Vec<usize>,   // subset needed for predicate
    pushed_predicate: Option<(Formula, Variables, Arc<FunctionRegistry>)>,
) -> Self
```

The `next_batch` method:

1. **Tokenize**: Read lines, tokenize into field byte ranges
2. **Phase 1** (if pushed_predicate is Some):
   - Parse only `filter_field_indices` into columns
   - Build temporary `ColumnBatch`, call `evaluate_batch_predicate` → `Bitmap`
   - If all rows filtered out, skip to next batch of lines
3. **Phase 2**: Parse `projected_fields \ filter_field_indices` using `parse_field_column_selected` with the selection bitmap
4. **Combine**: Merge Phase 1 columns (reused) with Phase 2 columns. Return `ColumnBatch` with selection applied.

If `pushed_predicate` is None, all projected fields are parsed normally (current behavior).

### Change Points
- `parse_field_column_selected` in `field_parser.rs` extended for all types
- `BatchScanOperator` struct and constructor updated
- `BatchScanOperator::next_batch` gains two-phase logic

## Optimization 3: Batch-Native GroupBy

### Plan Tree Structure
A typical aggregation query produces:
```
Map([status, _count]) → GroupBy([status], [Count(*)]) → Filter(...) → DataSource
```

For the batch GroupBy path to fire, `try_get_batch` must handle both `Node::GroupBy` and `Node::Map`.

### Node::Map in try_get_batch

Handles simple column projections (variable references and constants). Aliased projections (e.g., `SELECT status AS s`) fall back to row path because `BatchProjectOperator` matches columns by name and does not support renaming.

```rust
Node::Map(named_list, source) => {
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
            let output_names: Vec<String> = /* extract column names from named_list */;
            let project = BatchProjectOperator::new(batch_stream, output_names);
            Some(Ok(Box::new(project)))
        }
        Some(Err(e)) => Some(Err(e)),
        None => None,
    }
}
```

### BatchGroupByOperator

New file `src/execution/batch_groupby.rs`.

**Accumulator enum** — all 11 aggregate types (no spurious CountDistinct):

```rust
enum Accumulator {
    Count(i64),
    Sum(OrderedFloat<f32>),
    Min(Option<Value>),
    Max(Option<Value>),
    Avg { sum: OrderedFloat<f32>, count: i64 },  // f32 to match existing AvgAggregate
    First(Option<Value>),
    Last(Option<Value>),
    ApproxCountDistinct(HyperLogLog),
    PercentileDisc { values: Vec<Value>, percentile: f64 },
    ApproxPercentile { digest: TDigest, percentile: f64 },
    GroupAs(Vec<Value>),
}
```

**Fallback guard**:
```rust
fn all_aggregates_supported(aggregates: &[NamedAggregate]) -> bool {
    aggregates.iter().all(|na| match &na.aggregate {
        Aggregate::Count(_) | Aggregate::Sum(_) | Aggregate::Min(_) |
        Aggregate::Max(_) | Aggregate::Avg(_) | Aggregate::First(_) |
        Aggregate::Last(_) | Aggregate::ApproxCountDistinct(_, _) |
        Aggregate::PercentileDisc(_, _) | Aggregate::ApproxPercentile(_, _) |
        Aggregate::GroupAs(_) => true,
    })
}
```

**Accumulation phase**: For each incoming `ColumnBatch`:
1. Iterate active rows (per selection vector)
2. Extract group key values using `BatchToRowAdapter::extract_value` → build `Option<Tuple>` key
3. Look up or create `Vec<Accumulator>` for that key
4. For each aggregate, extract column value and update accumulator

**Emit phase** (after child returns `None`):
1. If `groups` is empty AND GROUP BY key list is empty → emit one row:
   - COUNT → 0, SUM → NULL, AVG → NULL, MIN → NULL, MAX → NULL, others → NULL
2. Otherwise → one row per group as a `ColumnBatch`
3. Column names: group key names + aggregate alias names from `NamedAggregate::name_opt`

### try_get_batch for Node::GroupBy

```rust
Node::GroupBy(fields, aggregates, source) => {
    if !all_aggregates_supported(aggregates) {
        return None;
    }
    match source.try_get_batch(variables, registry, required_fields) {
        Some(Ok(batch_stream)) => {
            let groupby = BatchGroupByOperator::new(
                batch_stream, fields.clone(), aggregates.clone(),
            );
            Some(Ok(Box::new(groupby)))
        }
        Some(Err(e)) => Some(Err(e)),
        None => None,
    }
}
```

### Change Points
- New file `src/execution/batch_groupby.rs`
- Modified `try_get_batch` in `src/execution/types.rs` to handle `Node::GroupBy` and `Node::Map`

## Edge Cases

1. **Wildcard SELECT (`SELECT *`)**: `extract_required_fields` returns all schema fields when it encounters `Named::Star`.
2. **Predicate on non-schema fields**: Falls back to `BatchScanOperator` (projected fields) → `BatchFilterOperator`.
3. **Mixed-type fields in predicates**: Uses `evaluate_batch_predicate`'s existing scalar fallback.
4. **Complex aggregate expressions**: `extract_required_fields` fully recurses through Function, Branch, Cast, Logic, and ExpressionPredicate variants.
5. **Empty GROUP BY result**: Emits one row with SQL-null defaults (COUNT → 0, others → NULL).
6. **HAVING clause**: `Node::Filter` above `Node::Map` above `Node::GroupBy` — won't match pushdown pattern, stays as `BatchFilterOperator`.
7. **Aliased projections**: `SELECT x AS y` — `Node::Map` batch path returns `None`, falls back to row-based `MapStream`.
8. **Expression::Subquery**: Conservative fallback returns all schema fields.
9. **Formula::ExpressionPredicate**: Recurses into wrapped expression to capture all field references.

## Testing Strategy

- Existing 588 tests provide regression safety
- New unit tests for `extract_required_fields` with various query shapes
- New unit tests for extended `parse_field_column_selected` for all types
- New unit tests for two-phase `BatchScanOperator`
- New unit tests for `BatchGroupByOperator` with all 11 aggregate types
- New unit tests for `Node::Map` batch path (simple projections and aliased fallback)
- Integration tests comparing batch vs row GroupBy output for real ELB data
- Benchmark comparison against Phase 0 baseline and Phase 2-3 results
