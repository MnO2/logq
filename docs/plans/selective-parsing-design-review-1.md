VERDICT: NEEDS_REVISION

## Summary Assessment

The design correctly identifies the three main bottlenecks and proposes reasonable optimizations, but contains several critical gaps: the `extract_required_fields` approach is under-specified for the actual plan tree structure (Map wraps GroupBy, not the other way around), the two-phase lazy parsing only saves work for String fields due to a limitation in `parse_field_column_selected`, and the `BatchGroupByOperator` omits 5 of 11 aggregate types while also requiring `Node::Map` batch support that is not addressed.

## Critical Issues (must fix)

### 1. Node::Map must be handled in try_get_batch for GroupBy to fire

The design proposes adding `Node::GroupBy` support to `try_get_batch`, but does not address `Node::Map`. In the actual plan tree, `SELECT status, COUNT(*) FROM elb GROUP BY status` produces:

```
Map([status, _count]) -> GroupBy([status], [Count(*)]) -> Filter(...) -> DataSource
```

`Node::get()` starts at the top-level `Map`, calls `self.try_get_batch()`, which returns `None` because `Map` is not handled. It then falls through to `Map::get()`, which calls `source.get()` on `GroupBy`. At that point, `GroupBy::get()` would call `self.try_get_batch()` -- but this only works if `GroupBy` is the node where `try_get_batch` is tried. Looking at the code flow in `Node::get()` (line 645-650), `try_get_batch` is called on `self` before the match, so `GroupBy::try_get_batch` WOULD fire at line 647. However, the result is wrapped in `BatchToRowAdapter` at line 649, then `GroupByStream` receives that row stream at line 687 -- defeating the purpose.

For `BatchGroupByOperator` to actually be used, **`try_get_batch` must also handle `Node::Map`** (at minimum for simple column projections), so the batch path can extend from `DataSource -> Filter -> GroupBy -> Map` without being interrupted by a `BatchToRowAdapter`.

Without this, the batch GroupBy will never be reached in practice for any query that has a SELECT clause (which is all of them).

### 2. parse_field_column_selected only optimizes String fields

The two-phase lazy parsing design claims that Phase 2 "only materializes string data for surviving rows" via `parse_field_column_selected`. Looking at the actual code (field_parser.rs:151-183), the function only skips inactive rows for `DataType::String`. For all other types (`Integral`, `Float`, `DateTime`, `Host`, `HttpRequest`), it falls back to `parse_field_column` which processes ALL rows unconditionally (line 181: `_ => parse_field_column(lines, fields, field_idx, datatype)`).

This means two-phase lazy parsing provides zero benefit for non-string projected fields. For ELB logs, fields like `request_processing_time` (Float), `elb_status_code` (Integral), `backend_processing_time` (Float), and `timestamp` (DateTime) will be fully parsed for all rows regardless of the selection bitmap.

The design should either:
- Extend `parse_field_column_selected` to skip inactive rows for Int32, Float32, DateTime, Host, and HttpRequest types (straightforward for Int32/Float32; add an `if !selection.is_active(row, len) { data.push(0); continue; }` branch)
- Or document that the two-phase savings are limited to string-typed projected fields and adjust the speedup projections accordingly

### 3. BatchGroupByOperator is missing 5 of 11 aggregate types

The design's `Accumulator` enum covers: Count, Sum, Min, Max, Avg, CountDistinct (6 variants). The actual `Aggregate` enum in types.rs:795-807 has 11 variants: Avg, Count, First, Last, Max, Min, Sum, ApproxCountDistinct, PercentileDisc, ApproxPercentile, GroupAs.

Missing from the design:
- **First** -- needs ordering-aware tracking
- **Last** -- needs ordering-aware tracking
- **ApproxCountDistinct** -- uses HyperLogLog, not HashSet
- **PercentileDisc** -- collects all values, computes at emit time
- **ApproxPercentile** -- uses TDigest data structure
- **GroupAs** -- collects entire records into arrays

The design should either:
- Add these accumulator types, or
- Explicitly document that queries using these aggregates fall back to the row-based `GroupByStream`, and add the guard logic in `try_get_batch` to detect unsupported aggregates and return `None`

Without the fallback guard, a query using `FIRST(x)` would hit the `BatchGroupByOperator` and either panic or silently produce wrong results.

### 4. extract_required_fields does not account for aggregate expressions evaluating against the FunctionRegistry

The design says `extract_required_fields` walks `Expression::Variable(path)` and `Formula` predicates. But aggregate expressions like `SUM(a + b)` or `COUNT(CASE WHEN x > 0 THEN 1 END)` contain `Expression::Function`, `Expression::Branch`, and `Expression::Cast` variants. The design says "recurses into aggregate expressions" but does not specify handling for `Expression::Function(name, args)` where the arguments are `Named::Expression(expr, _)` -- this requires recursing into the `Named` list.

More importantly, `Expression::Branch` (CASE WHEN) can contain arbitrary `Formula` predicates as conditions. If `extract_required_fields` does not recurse into branch conditions, it will miss field references.

The design should specify the complete recursive traversal: `Expression::Function` -> recurse into args (Vec<Named>), `Expression::Branch` -> recurse into both condition Formulas and result Expressions, `Expression::Cast` -> recurse into inner expression, `Expression::Logic` -> recurse into inner Formula, `Expression::Subquery` -> return all fields (conservative fallback).

### 5. Two-phase predicate pushdown does not handle predicate-scan separation correctly for Node::Filter wrapping non-DataSource

The design says predicate pushdown happens when `try_get_batch` matches `Node::Filter(source, formula)` wrapping a `Node::DataSource`. But `try_get_batch` currently uses recursive descent -- `Node::Filter` calls `source.try_get_batch()`. The design proposes pattern-matching `Node::Filter` wrapping `Node::DataSource` directly, but the current code structure recurses into the child. This means the filter code needs to inspect whether `source` is a `DataSource` to decide between pushdown and wrapping with `BatchFilterOperator`.

This requires an explicit `match **source` inside the `Node::Filter` arm, which changes the current recursive structure. The design should show the actual match logic:

```rust
Node::Filter(source, formula) => {
    if let Node::DataSource(ds, bindings) = &**source {
        // Push predicate into scan
        ...
    } else {
        // Wrap with BatchFilterOperator as before
        match source.try_get_batch(variables, registry) { ... }
    }
}
```

## Suggestions (nice to have)

### 1. Consider computing required fields at plan construction time, not execution time

The `extract_required_fields` function walks the plan tree at execution time in `try_get_batch`. Since the plan tree is immutable after construction, this analysis could be done once during logical planning and stored as metadata on `Node::DataSource` or passed through as annotations. This avoids repeated tree walks if `try_get_batch` is called multiple times (though currently it is only called once per query).

### 2. The Accumulator::Sum should use f64, not Value, to match existing SumAggregate behavior

The design shows `Sum(f64)` in the Accumulator, which matches the existing `SumAggregate` that uses `OrderedFloat<f32>`. However, note the existing code uses `f32` (types.rs:1057: `HashMap<Option<Tuple>, OrderedFloat<f32>>`). Using `f64` in the batch path would produce different precision results than the row path. Either use `f32` to match, or upgrade both paths to `f64`.

### 3. Consider a simpler alternative for Optimization 1: defer field projection to after filter

Instead of the full `extract_required_fields` tree walk, a simpler approach: always parse all fields but apply `BatchProjectOperator` after filtering. This eliminates the complexity of field analysis while still getting most of the benefit from two-phase parsing (Optimization 2). The field analysis is mainly valuable for reducing the *number* of fields parsed; if two-phase parsing already skips 90% of rows for non-filter fields, selective parsing provides diminishing returns.

### 4. Two-phase parsing requires holding all raw lines in memory alongside Phase 1 columns

The design's Phase 1 tokenizes lines and parses filter fields, then Phase 2 uses the same `lines` and `line_fields` vectors to parse remaining columns. This means the raw byte data for the entire batch (up to 1024 lines) must be retained in memory until Phase 2 completes. Currently, `BatchScanOperator::read_lines` returns `Vec<Vec<u8>>` which is already doing this, so no change is needed -- but it is worth noting that this doubles the peak memory for each batch (raw lines + parsed columns).

### 5. Edge case: COUNT(*) with no GROUP BY and empty input

The design mentions this edge case (section 5 under Edge Cases) but should be more specific: the existing `GroupByStream` handles `COUNT(*)` returning 0 for empty input through `CountAggregate::add_row`. The `BatchGroupByOperator` must replicate this by checking if `groups` is empty at emit time and, if the GROUP BY key list is empty, emitting a single row with default aggregate values. The design says "aggregate defaults (COUNT(*) = 0)" but should specify that SUM returns NULL, AVG returns NULL, MIN returns NULL, MAX returns NULL for empty input (matching SQL semantics), not 0.

### 6. Consider short-circuit evaluation for AND predicates in two-phase

When the pushed predicate is `AND(p1, p2)`, the current `evaluate_batch_predicate` evaluates both sides and ANDs the bitmaps. For two-phase, if the first conjunct eliminates most rows, evaluating the second conjunct is wasteful. Consider short-circuit evaluation where the first conjunct's bitmap is used as a selection vector for the second.

## Verified Claims (things you confirmed are correct)

1. **"try_get_batch currently passes all fields"** -- Confirmed. Line 602 of types.rs: `let all_fields: Vec<usize> = (0..schema.field_count()).collect()`.

2. **"BatchScanOperator::new already accepts projected_fields: Vec<usize>"** -- Confirmed. batch_scan.rs line 24-26 shows the constructor accepts `projected_fields`.

3. **"Node::GroupBy is not handled in try_get_batch"** -- Confirmed. The match in try_get_batch (types.rs:591-642) only handles DataSource, Filter, and Limit. GroupBy falls to the `_ => None` catch-all at line 641.

4. **"BatchToRowAdapter materializes every surviving row into a Record"** -- Confirmed. batch.rs lines 196-214 show that for each active row, a `LinkedHashMap` is constructed with all column values cloned.

5. **"parse_field_column_selected exists and uses selection bitmap"** -- Confirmed. field_parser.rs:151-183. It accepts a `SelectionVector` parameter and skips inactive rows for String type columns.

6. **"evaluate_batch_predicate supports AND/OR/NOT, comparisons, IS NULL/MISSING"** -- Confirmed. batch_predicate.rs lines 21-58 show the full match with fast paths for column-vs-constant comparisons plus scalar fallback.

7. **"588 tests"** -- The design says "589 tests" but the actual count is 588 (verified via `cargo test`). Minor discrepancy, not impactful.

8. **"LogSchema has field_index method"** -- Confirmed. log_schema.rs:30 implements `field_index(&self, name: &str) -> Option<usize>`.

9. **"BatchFilterOperator combines with existing selection"** -- Confirmed. batch_filter.rs:42-48 shows it ANDs the predicate bitmap with any existing selection vector.

10. **"Aggregate types use HashMap<Option<Tuple>, ...> for group keying"** -- Confirmed. All aggregate structs (types.rs:1011-1297) use `HashMap<Option<Tuple>, ...>` as their internal storage, which validates the design's choice of `Vec<Value>` as hash keys (though the actual code uses `Option<Tuple>` not `Vec<Value>` -- the BatchGroupByOperator would need to convert to `Option<Tuple>` for compatibility, or reimplement the accumulation logic independently).
