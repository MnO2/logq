VERDICT: NEEDS_REVISION

## Summary Assessment

The v2 design addresses the structural concerns from round 1 (Node::Map handling, complete traversal spec, aggregate coverage, predicate pushdown pattern matching), but introduces two new critical issues: the `extract_required_fields` traversal spec references Formula variants that do not exist in the codebase and omits variants that do, and the predicate pushdown code has an unresolved `project_fields` placeholder that cannot be computed without changing the `try_get_batch` method signature.

## Round 1 Issue Resolution

### Issue #1: Node::Map must be handled in try_get_batch -- RESOLVED
The design now explicitly adds `Node::Map` handling in `try_get_batch` for simple column projections (Variable references and Constants), with a `return None` fallback for complex expressions. This correctly enables the batch path to extend through `DataSource -> Filter -> GroupBy -> Map`.

### Issue #2: parse_field_column_selected only optimizes String fields -- RESOLVED
The design specifies extending `parse_field_column_selected` to skip inactive rows for Int32, Float32, DateTime, Host, and HttpRequest types using the same `if !selection.is_active(row, len) { push default; continue; }` pattern. This is straightforward and correct.

### Issue #3: BatchGroupByOperator missing aggregate types -- RESOLVED
The design now shows all 11 Accumulator variants and an `all_aggregates_supported` guard function. However, see Critical Issue #1 below for a factual error in the variant names.

### Issue #4: extract_required_fields traversal under-specified -- MOSTLY RESOLVED
The design now provides a full recursive traversal spec for Expression and Formula variants. However, see Critical Issue #2 below for mismatches with the actual enum variants.

### Issue #5: Predicate pushdown pattern matching -- RESOLVED (structurally)
The design now shows explicit `if let Node::DataSource(...) = &**source` pattern matching inside the `Node::Filter` arm, with a fallback to `BatchFilterOperator` wrapping for non-DataSource children. However, see Critical Issue #3 below for an unresolved placeholder.

## Critical Issues (must fix)

### 1. Accumulator and all_aggregates_supported reference non-existent Aggregate::CountDistinct

The design's `Accumulator` enum includes `CountDistinct(HashSet<Value>)` and the `all_aggregates_supported` function lists `Aggregate::CountDistinct(_)`. However, the actual `Aggregate` enum (types.rs:794-807) has no `CountDistinct` variant. The only count-distinct variant is `ApproxCountDistinct(ApproxCountDistinctAggregate, Named)`, which uses HyperLogLog internally.

The design separately also lists `ApproxCountDistinct(HyperLogLog)` as a distinct accumulator, making this doubly confusing. The fix is simple: remove `CountDistinct` from the `Accumulator` enum and from `all_aggregates_supported`, since it does not exist in the codebase. Only `ApproxCountDistinct` is needed.

As-is, the `all_aggregates_supported` function would fail to compile because `Aggregate::CountDistinct` is not a valid pattern.

### 2. extract_required_fields Formula traversal spec does not match the actual Formula enum

The design's traversal spec for `Formula` lists these variants:
- `Like(expr, pattern)` -- exists
- `Between(expr, low, high)` -- DOES NOT EXIST in the physical Formula enum
- `In(expr, list)` -- exists

But it omits these actual Formula variants:
- `Constant(bool)` -- no fields, but needs a no-op match arm
- `ExpressionPredicate(Box<Expression>)` -- wraps an arbitrary expression; field references inside it would be missed
- `NotLike(Box<Expression>, Box<Expression>)` -- same structure as Like, needs same handling
- `NotIn(Box<Expression>, Vec<Expression>)` -- same structure as In, needs same handling

The `Between` variant is desugared at the AST level (see `src/syntax/desugar.rs:112-121`) into `AND(>=, <=)` and never reaches the physical `Formula` enum. The design specifies handling for a non-existent variant while missing four variants that do exist.

Of these, `ExpressionPredicate` is the most important to handle correctly -- it wraps an `Expression` that may contain `Variable` references, so `extract_required_fields` must recurse into it or it will miss field dependencies.

### 3. Predicate pushdown code uses `project_fields` from undefined parent context

In the `Node::Filter` pushdown code (design lines 126-127):

```rust
let project_fields = /* from parent context */;
let all_fields = union(filter_fields, project_fields);
```

The comment `/* from parent context */` is a placeholder with no implementation. The `try_get_batch` method has this signature:

```rust
fn try_get_batch(&self, variables: &Variables, registry: &Arc<FunctionRegistry>) -> Option<CreateStreamResult<Box<dyn BatchStream>>>
```

There is no `required_fields` parameter. When `Node::Filter` is executing its `try_get_batch`, it has no way to know what fields the parent node (e.g., `Node::Map` or `Node::GroupBy`) needs. The method only has access to `self` (the Filter node) and its child (the DataSource).

The design's Optimization 1 section says `extract_required_fields` walks the plan tree top-down and the `DataSource` arm receives the computed field set. But the predicate pushdown happens in the `Node::Filter` arm, not the `DataSource` arm, and the top-down context is not threaded through.

Two possible fixes:
1. Thread a `required_fields: Option<Vec<usize>>` parameter through `try_get_batch` calls. The root caller passes the full `extract_required_fields` result, and each node propagates it to children.
2. When pushdown fires inside `Node::Filter`, compute `project_fields` by calling `extract_required_fields` on the Filter node itself (i.e., walk the filter's formula plus assume all fields are needed for the parent). But this loses the selective parsing benefit for the parent.

The design must specify which approach is used and show the actual implementation, not a placeholder comment.

## Suggestions (nice to have)

### 1. Consider whether Node::Map batch path needs to handle column renaming

The proposed `Node::Map` batch handler uses `BatchProjectOperator::new(batch_stream, output_names)`. Looking at `BatchProjectOperator` (batch_project.rs:41-46), it matches output columns by name against input batch column names. This works for simple projections like `SELECT status, elb_status_code FROM ...` where the output names match the input column names.

However, for `SELECT status AS s FROM ...`, the output name is `s` but the input column name is `status`. The design's `compute_output_names(named_list)` function is not specified. It needs to produce a mapping from input column name to output column name, not just a list of output names. The current `BatchProjectOperator` only does column selection by matching names -- it does not support renaming.

If `compute_output_names` returns `["s"]`, then `BatchProjectOperator` will look for a column named `s` in the input batch, find none, and produce an empty result. The design should either extend `BatchProjectOperator` to accept (input_name, output_name) pairs, or document that aliased projections fall through to the row-based path.

### 2. SUM accumulator precision is specified correctly but should be documented as intentional

The design says `Sum(OrderedFloat<f32>)` to match the existing `SumAggregate` which uses `OrderedFloat<f32>` (types.rs:1057). This is consistent. However, `Avg { sum: f64, count: i64 }` in the design uses `f64` for the sum while the existing `AvgAggregate` uses `OrderedFloat<f32>` for averages (types.rs:1046-1048). This precision mismatch between batch and row paths for AVG could produce slightly different results. Consider using `f32` for both or `f64` for both.

### 3. Empty GROUP BY result edge case should clarify interaction with Node::Map

The design correctly specifies that `BatchGroupByOperator` with no input rows and no GROUP BY fields emits one row with SQL-null defaults. However, this row then flows into `Node::Map` in the batch path. The `Node::Map` batch handler only supports simple Variable projections. If the Map references aggregate output columns (e.g., `_count`), it needs to find those column names in the batch. The design should confirm that `BatchGroupByOperator`'s emit phase produces a `ColumnBatch` with column names matching what the Map expects (i.e., the aggregate alias names from `NamedAggregate::name_opt`).

### 4. The fallback `None` return at the bottom of the Node::Filter pushdown arm silently drops to row path

When `Node::Filter` wraps `Node::DataSource` but bindings are non-empty or the format is JSONL, the code returns `None` (line 137 of the design). This is correct behavior, but worth documenting that this means the entire subtree below (DataSource) also won't use the batch path, even though `DataSource` alone could. The current code already handles this case correctly via the recursive `source.get()` call in the row path, so no action needed -- just worth noting for future readers.

## Verified Claims (things you confirmed are correct)

1. **Aggregate enum has exactly 11 variants** -- Confirmed. types.rs:795-807 shows: Avg, Count, First, Last, Max, Min, Sum, ApproxCountDistinct, PercentileDisc, ApproxPercentile, GroupAs. The design's `all_aggregates_supported` function matches this (except for the spurious `CountDistinct` -- see Critical Issue #1).

2. **Formula enum does NOT have Between** -- Confirmed. Between is desugared at the AST level (desugar.rs:112-121) into AND(>=, <=) and never appears in the physical Formula enum (types.rs:315-330).

3. **Formula enum DOES have ExpressionPredicate, NotLike, NotIn** -- Confirmed at types.rs:325-329. These are missing from the design's traversal spec.

4. **SumAggregate uses OrderedFloat<f32>** -- Confirmed. types.rs:1057 shows `sums: HashMap<Option<Tuple>, OrderedFloat<f32>>`. The design's `Sum(OrderedFloat<f32>)` accumulator matches this.

5. **try_get_batch currently handles DataSource, Filter, and Limit only** -- Confirmed at types.rs:591-641. GroupBy, Map, and all other node types fall through to `_ => None`.

6. **try_get_batch has no required_fields parameter** -- Confirmed. The signature at types.rs:586-590 takes only `variables` and `registry`.

7. **Node::Map in the plan tree wraps GroupBy** -- Confirmed from `Node::get()` at types.rs:658-663 where Map calls `source.get()` on its child. A SELECT with GROUP BY produces Map -> GroupBy -> Filter -> DataSource, so Map must be handled in `try_get_batch` for the batch path to extend to GroupBy.

8. **BatchProjectOperator matches columns by name** -- Confirmed at batch_project.rs:41-46. It uses `col_map.iter().position(|(n, _)| n == output_name)` to find columns.

9. **parse_field_column_selected falls back to parse_field_column for non-String types** -- Confirmed at field_parser.rs:181: `_ => parse_field_column(lines, fields, field_idx, datatype)`.

10. **Node::GroupBy key uses Vec<PathExpr>** -- Confirmed at types.rs:572: `GroupBy(Vec<PathExpr>, Vec<NamedAggregate>, Box<Node>)`. The design's use of `Option<Tuple>` for group keys matches the existing `GroupByStream` convention (stream.rs:410-414).
