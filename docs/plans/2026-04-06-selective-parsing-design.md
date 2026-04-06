# Design: Selective Field Parsing, Two-Phase Lazy Parsing, and Batch-Native GroupBy

**Date**: 2026-04-06
**Goal**: Unlock the projected 2.4x speedup by eliminating the three bottlenecks identified in the Phase 2-3 results: parsing all 17 fields, double materialization through BatchToRowAdapter for GroupBy, and lack of predicate pushdown into the scan.

## Background

Phase 2-3 wired the batch pipeline into `Node::get()` but benchmarks showed a 12-16% regression because:
1. `BatchScanOperator` parses ALL fields into columns, whereas the old row path parsed on-demand
2. `BatchToRowAdapter` materializes columnar data back to rows for GroupBy/Map
3. No two-phase lazy parsing — filter fields aren't parsed before project fields

The Phase 0 baseline showed filtering at 60.3% of total time, scanning at 27.7%, and aggregation at 12%. The target is 2.4x overall speedup on the representative scan+filter+groupby workload.

## Optimization 1: Selective Field Parsing

### Problem
`try_get_batch` currently passes `(0..schema.field_count()).collect()` (all 17 ELB fields) to `BatchScanOperator`. For `WHERE elb_status_code = '200'`, only field 7 is needed for filtering plus whatever fields are projected.

### Solution
Add a helper function `extract_required_fields` that walks the physical plan tree and collects field names referenced by:
- `Expression::Variable(path)` — extracts the attribute name from single-segment paths
- `Formula` predicates — recursively walks both sides of comparisons, AND/OR/NOT
- `Named` projections in `Node::Map` — each named expression's variables
- `Aggregate` expressions in `Node::GroupBy` — the expression inside SUM, MIN, MAX, AVG, etc.
- Group-by key field names

The function signature:
```rust
fn extract_required_fields(node: &Node, schema: &LogSchema) -> Vec<usize>
```

It resolves collected field names to indices via `LogSchema::field_index()`. Unknown names (computed expressions, aliases) are ignored — they'll be handled by the row-based fallback.

### Integration
Modify `try_get_batch` to compute the required field set top-down:
- `Node::DataSource`: receives `required_fields` from parent, passes to `BatchScanOperator`
- `Node::Filter(source, formula)`: computes `filter_fields ∪ parent_project_fields`, passes union to child
- `Node::Map(named_list, source)`: extracts field names from projections
- `Node::GroupBy(fields, aggregates, source)`: extracts from group keys + aggregate expressions

The `try_get_batch` signature changes to accept an optional `required_fields` parameter, or computes it internally by examining the full node subtree.

### Change Points
- New function in `src/execution/types.rs` or `src/execution/field_analysis.rs`
- Modified `try_get_batch` in `src/execution/types.rs`
- `BatchScanOperator::new` already accepts `projected_fields: Vec<usize>` — no structural change needed

## Optimization 2: Two-Phase Lazy Parsing

### Problem
Even with selective field parsing, all projected fields are parsed for every row — including rows that will be filtered out. For a query like `SELECT * FROM elb WHERE elb_status_code = '200'` with 10% selectivity, 90% of field parsing is wasted.

### Solution
Extend `BatchScanOperator` to accept a pushed predicate and perform two-phase parsing:

**Phase 1 — Filter**:
1. Tokenize all lines into field byte ranges (unchanged)
2. Parse only `filter_field_indices` into typed columns
3. Build temporary `ColumnBatch` with filter columns
4. Call `evaluate_batch_predicate` to get a `Bitmap` of surviving rows

**Phase 2 — Project**:
1. Compute remaining fields: `projected_fields - filter_field_indices`
2. Parse remaining fields using `parse_field_column_selected` with the selection bitmap — only materializes string data for surviving rows
3. Combine Phase 1 columns (reused, not re-parsed) with Phase 2 columns
4. Return `ColumnBatch` with the selection bitmap applied

### New BatchScanOperator signature
```rust
impl BatchScanOperator {
    pub fn new(
        reader: Box<dyn BufRead>,
        schema: LogSchema,
        projected_fields: Vec<usize>,
        filter_field_indices: Vec<usize>,
        pushed_predicate: Option<(Formula, Variables, Arc<FunctionRegistry>)>,
    ) -> Self
}
```

### Integration
When `try_get_batch` matches `Node::Filter(source, formula)` wrapping a `Node::DataSource`:
- Extract filter field indices from the formula
- Push the predicate into `BatchScanOperator` instead of wrapping with `BatchFilterOperator`
- `BatchFilterOperator` is still used when:
  - Predicate references computed expressions (not direct field references)
  - Source is not a DataSource (e.g., joined streams)
  - Multiple stacked filters (first pushed, rest stay as BatchFilterOperator)

### Key Detail: Overlapping Fields
`filter_field_indices` and `projected_fields` may overlap (e.g., `SELECT elb_status_code WHERE elb_status_code = '200'`). Fields parsed in Phase 1 are reused directly in the output — no double-parsing.

### Change Points
- `BatchScanOperator` struct gains `filter_field_indices` and `pushed_predicate` fields
- `BatchScanOperator::next_batch` gains two-phase logic
- `try_get_batch` for `Node::Filter` pushes predicate into scan when possible

## Optimization 3: Batch-Native GroupBy

### Problem
Currently, `Node::GroupBy` is not handled in `try_get_batch`, so queries with GROUP BY fall back to: `BatchScanOperator → BatchFilterOperator → BatchToRowAdapter → GroupByStream`. The `BatchToRowAdapter` materializes every surviving row into a `Record` (HashMap-based), negating most of the columnar benefits.

### Solution
New `BatchGroupByOperator` in `src/execution/batch_groupby.rs`:

**Struct**:
```rust
pub(crate) struct BatchGroupByOperator {
    child: Box<dyn BatchStream>,
    group_fields: Vec<String>,
    aggregates: Vec<NamedAggregate>,
    groups: HashMap<Vec<Value>, Vec<Accumulator>>,
    done: bool,
    result: Option<ColumnBatch>,
}
```

**Accumulator enum**:
```rust
enum Accumulator {
    Count(i64),
    Sum(f64),
    Min(Value),
    Max(Value),
    Avg { sum: f64, count: i64 },
    CountDistinct(HashSet<Value>),
}
```

**Accumulation phase**:
For each incoming `ColumnBatch`:
1. Iterate active rows (per selection vector)
2. Extract group key values from relevant columns using `BatchToRowAdapter::extract_value`
3. Look up or create `Vec<Accumulator>` for that key in the HashMap
4. For each aggregate, extract the relevant column value and update the accumulator

**Emit phase** (after child returns `None`):
1. Convert accumulated `HashMap` into a `ColumnBatch` — one row per group
2. Group key columns become output columns
3. Aggregate results become additional columns (named per `NamedAggregate`)
4. Return this batch once, then `None`

### Integration
Extend `try_get_batch` to match:
```rust
Node::GroupBy(fields, aggregates, source) => {
    match source.try_get_batch(variables, registry) {
        Some(Ok(batch_stream)) => {
            let groupby = BatchGroupByOperator::new(
                batch_stream, fields.clone(), aggregates.clone(),
            );
            Some(Ok(Box::new(groupby)))
        }
        ...
    }
}
```

### Limitation
Group key extraction uses `extract_value` (scalar per-row) to build `Vec<Value>` hash keys. This is simpler than a fully vectorized hash table but still avoids the full `BatchToRowAdapter` overhead — only group-key + aggregate columns are extracted per row, not all 17 fields.

## Edge Cases

1. **Wildcard SELECT (`SELECT *`)**: `extract_required_fields` returns the full field set when it encounters a star projection.

2. **Predicate on non-schema fields**: Predicates referencing computed expressions can't be pushed into the scan. Falls back to `BatchScanOperator` (projected fields) → `BatchFilterOperator`.

3. **Mixed-type fields in predicates**: Fields like `client_and_port` (Host type) use `evaluate_batch_predicate`'s existing scalar fallback.

4. **Aggregates with expressions**: `extract_required_fields` recurses into aggregate expressions (e.g., `SUM(a + b)` needs fields for both `a` and `b`).

5. **Empty GROUP BY result**: `BatchGroupByOperator` with no input rows and no GROUP BY fields emits a single row with aggregate defaults (`COUNT(*) = 0`), matching `GroupByStream` behavior.

6. **HAVING clause**: Lives as `Node::Filter` above `Node::GroupBy`. Filters on aggregate results, not raw columns, so it won't be pushed into the scan — stays as `BatchFilterOperator` or row-based `FilterStream`.

## Testing Strategy

- Existing 589 tests provide regression safety
- New unit tests for `extract_required_fields` with various query shapes
- New unit tests for two-phase `BatchScanOperator` verifying filtered rows skip Phase 2 parsing
- New unit tests for `BatchGroupByOperator` with COUNT, SUM, MIN, MAX, AVG, COUNT(DISTINCT)
- Integration tests comparing batch GroupBy output against row-based GroupBy for real ELB data
- Benchmark comparison against Phase 0 baseline and Phase 2-3 results
