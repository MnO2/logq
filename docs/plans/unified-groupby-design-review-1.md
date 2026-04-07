VERDICT: NEEDS_REVISION

## Summary Assessment

The core idea -- consolidating N+1 per-aggregate HashMaps into a single `HashMap<Option<Tuple>, GroupState>` -- is sound and well-motivated. However, the design has several concrete type-system and API-shape issues that will block compilation or silently change semantics if implemented as written.

## Critical Issues (must fix)

### 1. `finalize(&self)` signature is wrong for ApproxPercentile

The design specifies `finalize(&self) -> Value` on `AccumulatorState`. However, the current `ApproxPercentileAggregate::get_aggregated` requires `&mut self` because it must flush the internal buffer (up to 10,000 values) into the TDigest before computing the quantile. The same is true at the `Aggregate::get_aggregated` level, which takes `&mut self`.

The design's `finalize(&self)` will not compile for `AccumulatorState::ApproxPercentile` unless the signature changes to `finalize(&mut self)` or the buffer is flushed eagerly (which would change the merge behavior of TDigest).

**Fix**: Change to `finalize(&mut self) -> Value`, or add an explicit `flush_buffer(&mut self)` step before finalization.

### 2. `AccumulatorState` cannot derive `PartialEq` or `Eq`

The design places `HyperLogLog<Value>` and `TDigest` inside the `AccumulatorState` enum. The current code already has to work around this:

- `HyperLogLog` (from `pdatastructs 0.6`) derives `Clone` but NOT `PartialEq` or `Eq`. The existing `ApproxCountDistinctAggregate` has a manual `PartialEq` impl (types.rs:1414-1425).
- `TDigest` derives `PartialEq` and `Clone` but NOT `Eq`. The existing `ApproxPercentileAggregate` derives `Eq` (types.rs:1043) which only works because `Centroid` manually implements `Eq` for its `f64` fields -- this is technically unsound for NaN values but happens to work.

If `AccumulatorState` needs `#[derive(Clone, PartialEq, Eq)]` (to match the pattern of the current `Aggregate` enum which derives all three), it will require manual trait impls. The design does not mention this at all. If the enum does NOT need these traits, that should be stated explicitly, and the `GroupState` / `GroupByStream` derive requirements should be audited.

**Fix**: State explicitly whether `AccumulatorState` needs `Clone`/`PartialEq`/`Eq`, and if so, specify the manual impls needed for the `ApproxCountDistinct` and `ApproxPercentile` variants.

### 3. `Sum(f32)` will not compile -- needs `OrderedFloat<f32>`

The design specifies `AccumulatorState::Sum(f32)`. The current `SumAggregate` stores `OrderedFloat<f32>` (types.rs:1171), and `Value::Float` wraps `OrderedFloat<f32>`. Bare `f32` doesn't implement `Eq` or `Hash`, so this will fail to compile if the enum derives `Eq`. Even without `Eq`, using bare `f32` diverges from the existing convention unnecessarily.

Similarly, `AccumulatorState::Avg { sum: f64, count: i64 }` should use `OrderedFloat<f64>` to match the current `AvgAggregate::sums` type (types.rs:1134) and avoid `Eq` issues.

**Fix**: Use `Sum(OrderedFloat<f32>)` and `Avg { sum: OrderedFloat<f64>, count: i64 }`.

### 4. GroupAs extraction doesn't go through `extract_value` -- design's accumulation loop is wrong

The design proposes a uniform loop:
```rust
match def.extract_value(&variables, &self.registry)? {
    Some(val) => state.accumulators[i].accumulate(&val)?,
    None => state.accumulators[i].accumulate_row()?,
}
```

But `GroupAs` does NOT evaluate an expression. It captures the entire record as `Value::Object(Box::new(record.to_variables().clone()))` (stream.rs:423). This requires access to the raw `record`, not just the variables. The design's `AggregateDef.expr: Option<Expression>` field cannot represent "capture the whole record as an object."

Similarly, `PercentileDisc` and `ApproxPercentile` use a raw column name `String` (not `Named`/`Expression`) and do a direct `variables.get(column_name)` lookup (stream.rs:519, 523). The design's `AggregateDef` only has `expr: Option<Expression>`, which doesn't model this case.

**Fix**: Either (a) add a variant to `AccumulatorKind` that carries the column name for percentile-style aggregates and a "capture record" flag for GroupAs, or (b) extend `AggregateDef` to have an enum for value extraction strategy (expression eval, column lookup, record capture, none-for-count-star).

### 5. Count null-skipping semantics must be preserved in `accumulate`

The current `CountAggregate::add_record` skips `Value::Null` (types.rs:1218-1221), but `add_row` (for count(*)) does not. The design separates these into `Count` and `CountStar` variants, which is good, but the `accumulate(&mut self, val: &Value)` method on `AccumulatorState::Count` must preserve the null-skip behavior. The design does not spell this out, and a naive increment-on-every-call implementation would be a correctness regression.

**Fix**: Explicitly document that `AccumulatorState::Count::accumulate` must skip null values.

## Suggestions (nice to have)

### A. Consider `IndexMap` instead of `HashMap` for the unified group table

The design proposes `HashMap<Option<Tuple>, GroupState>`. While the current `HashSet` also doesn't guarantee insertion order, switching to `IndexMap` (from the `indexmap` crate, already a transitive dependency of many Rust projects) would provide deterministic insertion-order iteration at negligible performance cost. This would make output order predictable and test-friendly, and would prevent any subtle behavior changes if there are integration tests that happen to pass due to current hash ordering stability.

### B. The `batch_groupby.rs` alignment is not optional -- it's necessary

The design says aligning `batch_groupby.rs` is "optional, for consistency." But if the new `AccumulatorState`/`GroupState` types are added alongside the existing per-aggregate types (`CountAggregate`, `SumAggregate`, etc.), you'll have two parallel accumulator systems. Either (a) the batch path should also migrate to the new types simultaneously, or (b) the old types should be explicitly marked as deprecated. Leaving both alive indefinitely is a maintenance burden.

### C. Performance claim should be qualified

The design claims "30-50% improvement" on GroupBy benchmarks. This is plausible for multi-aggregate queries but should be validated. The existing benchmarks (`benches/bench_execution.rs`, query `E2_groupby_count`) only test a single-aggregate case (`count(*)`), where the improvement would be closer to 2x fewer hash lookups (from 3 to 1), not a 30-50% wall-clock improvement since hashing is not the only cost. Consider adding a multi-aggregate benchmark (e.g., `count(*), sum(bytes), avg(bytes)`) to actually measure the claimed gains.

### D. Approach B (GroupKey normalization) should be deferred

Approach B (normalized single-column keys) adds complexity with `enum GroupKey` variants. Given that it's an optimization for a specific case, I'd recommend implementing Approach A first, measuring, and only adding Approach B if profiling shows the Vec<Value> allocation is a significant remaining cost. The design doesn't make it clear whether A and B are supposed to land together or separately.

### E. Empty-input global aggregate behavior should be specified

The current `GroupByStream` does NOT handle the case where there's no input and no group keys (it would return `Ok(None)` because the groups HashSet is empty). The batch version (`batch_groupby.rs:256-278`) does handle this by emitting a single row with `COUNT -> 0` and others -> `Null`. The design should specify which behavior the new implementation follows, and ideally fix this discrepancy.

## Verified Claims (things I confirmed are correct)

1. **N+1 hash structures**: Confirmed. The current code has 1 `HashSet<Option<Tuple>>` for group tracking plus each aggregate type (`CountAggregate`, `SumAggregate`, etc.) contains its own `HashMap<Option<Tuple>, _>`. For a query with N aggregates, the key is hashed 2+N times per row (2 for the HashSet contains+insert, plus N aggregate HashMap lookups).

2. **~110 lines of match arms**: Confirmed. Lines 420-527 of `stream.rs` contain 108 lines of per-aggregate match arms that the unified loop would replace.

3. **Key cloning on every insert**: Confirmed. Each aggregate's `add_record` calls `key.clone()` via `entry(key.clone())` (or `self.counts.insert(key.clone(), ...)` etc.). Unifying to a single HashMap would eliminate N-1 of these clones.

4. **hashbrown HashMap usage**: Confirmed. The codebase uses `hashbrown::HashMap` (types.rs:16), so referencing `hashbrown::hash_map::IntoIter` in the design is consistent.

5. **No ordering guarantee in current code**: Confirmed. The current `GroupByStream` uses `std::collections::hash_set::HashSet` which has no insertion-order guarantee, so switching to `HashMap` iteration does not change ordering semantics.

6. **Existing aggregate types each have their own per-group HashMap**: Confirmed across all aggregate types: `CountAggregate::counts`, `SumAggregate::sums`, `AvgAggregate::sums`+`counts` (two!), `MaxAggregate::maxs`, `MinAggregate::mins`, `FirstAggregate::firsts`, `LastAggregate::lasts`, `GroupAsAggregate::tuples`, `ApproxCountDistinctAggregate::counts`, `PercentileDiscAggregate::partitions`, `ApproxPercentileAggregate::partitions`+`buffer` (two!). The `AvgAggregate` case is actually N+2 for Avg since it has two HashMaps.

7. **Pull-based RecordStream architecture**: Confirmed. `GroupByStream` implements the `RecordStream` trait with `fn next(&mut self) -> StreamResult<Option<Record>>`. The design's approach of consuming all input then iterating is consistent with the current two-phase pattern.
