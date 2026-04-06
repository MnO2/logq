# Design: Selective Field Parsing, Two-Phase Lazy Parsing, and Batch-Native GroupBy (v2)

**Date**: 2026-04-06
**Goal**: Unlock the projected 2.4x speedup by eliminating the three bottlenecks identified in the Phase 2-3 results.

**Changes from previous version**:
- Added `Node::Map` handling in `try_get_batch` (Critical Issue #1)
- Extended `parse_field_column_selected` to skip inactive rows for all numeric types (Critical Issue #2)
- Added fallback guard for unsupported aggregate types, explicitly listing all 11 aggregate variants (Critical Issue #3)
- Fully specified recursive traversal for `extract_required_fields` across all Expression/Formula variants (Critical Issue #4)
- Showed explicit pattern matching for predicate pushdown vs. BatchFilterOperator wrapping (Critical Issue #5)
- Adjusted SUM accumulator to use f32 to match existing GroupByStream behavior
- Specified SQL-null semantics for empty GROUP BY results

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

Add a helper function that walks the physical plan tree and collects field names:

```rust
fn extract_required_fields(node: &Node, schema: &LogSchema) -> Vec<usize>
```

**Complete recursive traversal spec**:

For `Expression`:
- `Variable(path)` — extract attribute name from single-segment paths
- `Constant(_)` — no fields
- `Function(name, args)` — recurse into each `Named::Expression(expr, _)` in args; `Named::Star` contributes all fields
- `Branch(branches, else_expr)` — recurse into each branch's `Formula` condition and `Expression` result; recurse into `else_expr` if present
- `Cast(inner, _)` — recurse into inner expression
- `Logic(formula)` — recurse into the formula
- `Subquery(_)` — conservative fallback: return all schema fields (these are rare and complex)

For `Formula`:
- `Predicate(_, left, right)` — recurse into both expressions
- `And(left, right)`, `Or(left, right)` — recurse into both formulas
- `Not(inner)` — recurse into inner formula
- `IsNull(expr)`, `IsNotNull(expr)` — recurse into expression
- `IsMissing(expr)`, `IsNotMissing(expr)` — recurse into expression
- `Like(expr, pattern)` — recurse into both expressions
- `Between(expr, low, high)` — recurse into all three expressions
- `In(expr, list)` — recurse into expr and all list expressions

For `Node`:
- `Map(named_list, source)` — extract from all Named expressions, recurse into source
- `Filter(source, formula)` — extract from formula, recurse into source
- `GroupBy(fields, aggregates, source)` — extract from group key field names, recurse into each aggregate's expression, recurse into source
- `Limit(_, source)` — recurse into source
- `OrderBy(columns, _, source)` — extract column names, recurse into source
- `DataSource(_, _)` — base case, no additional fields

Resolve collected field names to indices via `LogSchema::field_index()`. Unknown names are ignored.

### Integration
Modify `try_get_batch` to compute the required field set top-down. The `Node::DataSource` arm receives the computed field set instead of all fields.

### Change Points
- New file `src/execution/field_analysis.rs` with `extract_required_fields` and helpers
- Modified `try_get_batch` in `src/execution/types.rs`
- `BatchScanOperator::new` already accepts `projected_fields: Vec<usize>` — no structural change needed

## Optimization 2: Two-Phase Lazy Parsing in BatchScanOperator

### Problem
Even with selective field parsing, all projected fields are parsed for every row — including rows that will be filtered out.

### Prerequisite: Extend parse_field_column_selected for all types

Currently `parse_field_column_selected` only optimizes String fields (falls back to `parse_field_column` for others). Extend it to skip inactive rows for all types:

```rust
// For Int32:
for row in 0..len {
    if !selection.is_active(row, len) {
        data.push(0);
        offsets_or_noop; // keep vectors aligned
        continue;
    }
    // ... actual parsing
}

// Same pattern for Float32, DateTime, Host, HttpRequest
```

This ensures two-phase lazy parsing saves work for ALL projected field types, not just strings.

### Solution: Two-Phase BatchScanOperator

Extend `BatchScanOperator::new` to accept:
- `filter_field_indices: Vec<usize>` — fields needed for predicate evaluation
- `pushed_predicate: Option<(Formula, Variables, Arc<FunctionRegistry>)>` — the filter to evaluate inside the scan

The `next_batch` method becomes three-phase:

1. **Tokenize**: Read lines, tokenize into field byte ranges (unchanged)
2. **Phase 1**: Parse only `filter_field_indices` into columns, build a temporary `ColumnBatch`, call `evaluate_batch_predicate` to get a `Bitmap`
3. **Phase 2**: Parse remaining `projected_fields \ filter_field_indices` using `parse_field_column_selected` with the selection bitmap. Combine with Phase 1 columns (reused, not re-parsed). Return `ColumnBatch` with selection applied.

### Integration: Explicit Predicate Pushdown Pattern Matching

The `try_get_batch` method for `Node::Filter` explicitly checks if the source is a `DataSource` to decide between pushdown and wrapping:

```rust
Node::Filter(source, formula) => {
    if let Node::DataSource(ds, bindings) = &**source {
        // Predicate pushdown: push formula into BatchScanOperator
        if bindings.is_empty() {
            if let DataSource::File(path, file_format, _) = ds {
                if file_format != "jsonl" {
                    let schema = LogSchema::from_format(file_format);
                    let filter_fields = extract_fields_from_formula(formula, &schema);
                    let project_fields = /* from parent context */;
                    let all_fields = union(filter_fields, project_fields);
                    // Build scan with pushed predicate
                    let scan = BatchScanOperator::new(
                        reader, schema, all_fields,
                        filter_fields, Some((formula, variables, registry)),
                    );
                    return Some(Ok(Box::new(scan)));
                }
            }
        }
        None // Fall back to row path
    } else {
        // Non-DataSource child: wrap with BatchFilterOperator as before
        match source.try_get_batch(variables, registry) {
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

### Key Detail: Overlapping Fields
`filter_field_indices` and `projected_fields` may overlap. Fields parsed in Phase 1 are reused directly in the output — no double-parsing.

### Change Points
- `parse_field_column_selected` in `field_parser.rs` extended for all types
- `BatchScanOperator` struct gains `filter_field_indices` and `pushed_predicate` fields
- `BatchScanOperator::next_batch` gains two-phase logic
- `try_get_batch` for `Node::Filter` uses explicit pattern matching for pushdown

## Optimization 3: Batch-Native GroupBy

### Problem
`Node::GroupBy` is not handled in `try_get_batch`, so queries with GROUP BY fall back to row materialization via `BatchToRowAdapter → GroupByStream`.

### Plan Tree Structure
A typical aggregation query produces:
```
Map([status, _count]) → GroupBy([status], [Count(*)]) → Filter(...) → DataSource
```

For the batch GroupBy path to fire, **`try_get_batch` must also handle `Node::Map`** (at minimum for simple column projections), so the batch path extends from `DataSource → Filter → GroupBy → Map` without interruption.

### Solution: BatchGroupByOperator

New `BatchGroupByOperator` in `src/execution/batch_groupby.rs`.

**Accumulator enum** — covers all 11 aggregate types with fallback:

```rust
enum Accumulator {
    Count(i64),
    Sum(OrderedFloat<f32>),       // matches existing f32 precision
    Min(Option<Value>),
    Max(Option<Value>),
    Avg { sum: f64, count: i64 },
    CountDistinct(HashSet<Value>),
    First(Option<Value>),
    Last(Option<Value>),
    ApproxCountDistinct(HyperLogLog),
    PercentileDisc { values: Vec<Value>, percentile: f64 },
    ApproxPercentile { digest: TDigest, percentile: f64 },
    GroupAs(Vec<Value>),
}
```

**Fallback guard**: Before constructing `BatchGroupByOperator`, `try_get_batch` checks whether all aggregates in the query are supported. If any aggregate uses a type that `BatchGroupByOperator` cannot handle (currently none — all 11 are covered), return `None` to fall back to row-based `GroupByStream`. This guard is a safety net for future aggregate types.

```rust
fn all_aggregates_supported(aggregates: &[NamedAggregate]) -> bool {
    aggregates.iter().all(|na| match &na.aggregate {
        Aggregate::Count(_) | Aggregate::Sum(_) | Aggregate::Min(_) |
        Aggregate::Max(_) | Aggregate::Avg(_) | Aggregate::CountDistinct(_) |
        Aggregate::First(_) | Aggregate::Last(_) |
        Aggregate::ApproxCountDistinct(_) | Aggregate::PercentileDisc(_, _) |
        Aggregate::ApproxPercentile(_, _) | Aggregate::GroupAs(_) => true,
    })
}
```

**Accumulation phase**: For each incoming `ColumnBatch`:
1. Iterate active rows (per selection vector)
2. Extract group key values using `BatchToRowAdapter::extract_value` → build `Option<Tuple>` key (matching existing GroupByStream's key type)
3. Look up or create accumulators for that key
4. For each aggregate, extract the relevant column value and update accumulator

**Emit phase** (after child returns `None`):
1. If `groups` is empty and GROUP BY key list is empty, emit one row with SQL-null defaults:
   - COUNT → 0
   - SUM → NULL
   - AVG → NULL  
   - MIN → NULL
   - MAX → NULL
   - Other aggregates → NULL
2. Otherwise, convert `HashMap` into a `ColumnBatch` — one row per group
3. Group key columns become output columns, aggregate results become additional columns

### Node::Map in try_get_batch

Add `Node::Map` handling for simple column projections (column selection and renaming):

```rust
Node::Map(named_list, source) => {
    // Check if all projections are simple variable references or constants
    let is_simple = named_list.iter().all(|named| match named {
        Named::Expression(Expression::Variable(_), _) => true,
        Named::Expression(Expression::Constant(_), _) => true,
        Named::Star => true,
        _ => false,
    });
    if !is_simple {
        return None; // Complex expressions need row-based MapStream
    }
    match source.try_get_batch(variables, registry) {
        Some(Ok(batch_stream)) => {
            // Apply column projection/renaming on the batch
            let output_names = compute_output_names(named_list);
            let project = BatchProjectOperator::new(batch_stream, output_names);
            Some(Ok(Box::new(project)))
        }
        Some(Err(e)) => Some(Err(e)),
        None => None,
    }
}
```

For Map nodes with complex expressions (function calls, CASE WHEN, CAST), `try_get_batch` returns `None` and falls back to the row-based `MapStream`. This is safe because complex projections are rare in the performance-critical path.

### Change Points
- New file `src/execution/batch_groupby.rs`
- Modified `try_get_batch` in `src/execution/types.rs` to handle `Node::GroupBy` and `Node::Map`
- Uses existing `BatchProjectOperator` for Map support

## Edge Cases

1. **Wildcard SELECT (`SELECT *`)**: `extract_required_fields` returns the full field set when it encounters `Named::Star`.

2. **Predicate on non-schema fields**: Predicates referencing computed expressions can't be pushed into the scan. Falls back to `BatchScanOperator` (projected fields) → `BatchFilterOperator`.

3. **Mixed-type fields in predicates**: Fields like `client_and_port` (Host type) use `evaluate_batch_predicate`'s existing scalar fallback.

4. **Aggregates with expressions**: `extract_required_fields` fully recurses into aggregate expressions including Function, Branch, Cast, and Logic variants.

5. **Empty GROUP BY result**: `BatchGroupByOperator` with no input rows and no GROUP BY fields emits a single row with SQL-null defaults: COUNT → 0, all others → NULL.

6. **HAVING clause**: Lives as `Node::Filter` above `Node::Map` above `Node::GroupBy`. Filters on aggregate results, not raw columns, so it won't match the pushdown pattern — stays as `BatchFilterOperator` or row-based `FilterStream`.

7. **Unsupported aggregates**: `all_aggregates_supported` guard returns `None` from `try_get_batch`, falling back to row-based `GroupByStream`. Currently all 11 types are covered, but the guard protects against future additions.

8. **Expression::Subquery in extract_required_fields**: Conservative fallback returns all schema fields since subquery field dependencies are complex to analyze.

## Testing Strategy

- Existing 588 tests provide regression safety
- New unit tests for `extract_required_fields` with various query shapes (simple filter, GROUP BY, CASE WHEN, nested functions, subquery)
- New unit tests for extended `parse_field_column_selected` verifying skip behavior for Int32, Float32, DateTime, Host, HttpRequest
- New unit tests for two-phase `BatchScanOperator` verifying filtered rows skip Phase 2 parsing
- New unit tests for `BatchGroupByOperator` with all 11 aggregate types
- New unit tests for `Node::Map` batch path with simple and complex projections
- Integration tests comparing batch GroupBy output against row-based GroupBy for real ELB data
- Benchmark comparison against Phase 0 baseline and Phase 2-3 results
