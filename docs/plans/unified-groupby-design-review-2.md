VERDICT: NEEDS_REVISION

## Summary Assessment

The v2 revision made substantive progress on all 5 critical issues from round 1, but introduced two new issues: a semantic mismatch in the `ColumnLookup` extraction strategy vs the current stream.rs code, and an incomplete null-handling specification that misses `ApproxCountDistinct`. The empty-input global aggregate spec also has a gap for the new `CountStar` variant.

## Critical Issues (must fix)

### 1. ColumnLookup uses `unwrap_or(Value::Missing)` but stream.rs uses `.unwrap()` -- semantic change

The design's accumulation loop (line 178) specifies:
```rust
ExtractionStrategy::ColumnLookup(col_name) => {
    let val = variables.get(col_name).cloned().unwrap_or(Value::Missing);
```

However, the current `GroupByStream` in `stream.rs:519,523` uses `.unwrap()` (panic on missing column):
```rust
Aggregate::PercentileDisc(ref mut inner, column_name) => {
    let val = variables.get(column_name).unwrap();
```

Meanwhile, `batch_groupby.rs:237,244` uses `.unwrap_or(Value::Missing)`.

This means `stream.rs` and `batch_groupby.rs` already have a behavioral discrepancy. The design should explicitly acknowledge this and state which behavior is intended. If `.unwrap_or(Value::Missing)` is the correct fix (it likely is -- panicking on missing columns is a bug), that's fine, but it should be called out as a deliberate semantic change, not silently introduced. A missing column fed into `PercentileDisc` or `ApproxPercentile` would then accumulate `Value::Missing` values, which may cause `AggregateError::InvalidType` during finalization (since these aggregates only handle `Float` and `Int`). The design should specify what happens.

**Fix**: State explicitly that the `ColumnLookup` strategy changes the current `stream.rs` panic-on-missing-column to returning `Value::Missing`, aligning with `batch_groupby.rs`. Consider whether `Value::Missing` should be skipped by percentile accumulators the same way `Value::Null` is skipped by Count.

### 2. ApproxCountDistinct also skips nulls -- design only documents null-skipping for Count

The design's Critical #5 fix explicitly documents null-skipping for `AccumulatorState::Count` (line 78-81), but the current `ApproxCountDistinctAggregate::add_record` (types.rs:1435-1438) also skips `Value::Null`:
```rust
if let &Value::Null = value {
    //Null value doesn't contribute to the total count
    return Ok(());
};
```

The design's `AccumulatorState::ApproxCountDistinct` variant does not mention this null-skipping behavior. If the implementer follows only the design doc, they will add null values to the HyperLogLog, changing the distinct-count semantics.

**Fix**: Add an explicit note on `AccumulatorState::ApproxCountDistinct` that `accumulate` must also skip `Value::Null` inputs, same as `Count`.

### 3. Empty-input global aggregate does not account for `CountStar` variant

The design (line 209-213) specifies the empty-input default behavior as:
- `Count` / `CountStar` -> `Value::Int(0)`
- All other accumulators -> `Value::Null`

This is semantically correct. However, the reference code in `batch_groupby.rs:262-265` only matches:
```rust
Aggregate::Count(_, _) => Value::Int(0),
_ => Value::Null,
```

The `batch_groupby.rs` `Aggregate::Count(_, _)` catches both `count(expr)` and `count(*)` because they share the same enum variant differentiated only by the `Named` payload. In the new design, `Count` and `CountStar` are separate `AccumulatorState` variants, so the empty-input logic must match BOTH. The design prose gets this right ("Count / CountStar -> Value::Int(0)"), but the implementation detail of `GroupState::new(...)` + calling `finalize()` on a freshly-created accumulator relies on the initial state being correct:

- `AccumulatorState::Count(0)` -> `finalize()` returns `Value::Int(0)` -- correct.
- `AccumulatorState::CountStar(0)` -> `finalize()` returns `Value::Int(0)` -- correct.
- `AccumulatorState::Sum(OrderedFloat(0.0))` -> `finalize()` would return `Value::Float(0.0)`, NOT `Value::Null`.

The design says to "synthesize a single GroupState via GroupState::new(...) and call finalize() on each accumulator without any prior accumulate calls." But `AccumulatorState::new` for `Sum` initializes to `Sum(OrderedFloat(0.0))`, and `finalize()` on that would return `Value::Float(0.0)`, not `Value::Null`. The same applies to `Avg { sum: 0.0, count: 0 }` (which would need special-case division-by-zero handling). And `Min(None)` / `Max(None)` need `finalize()` to return `Value::Null` when the inner Option is `None`.

This means the "just call finalize on a fresh accumulator" approach requires careful coordination between the `new()` initial state and `finalize()` behavior for EVERY variant. The design should specify the expected `finalize()` output for each variant when no `accumulate` calls have been made.

**Fix**: Add a table or list specifying the `finalize()` output for each `AccumulatorState` variant in the "zero-accumulate" case:
- `Count(0)` -> `Value::Int(0)`
- `CountStar(0)` -> `Value::Int(0)`
- `Sum(0.0)` -> `Value::Null` (NOT 0.0 -- SQL standard: SUM of empty set is NULL)
- `Avg { sum: 0.0, count: 0 }` -> `Value::Null` (division by zero -> NULL)
- `Min(None)` -> `Value::Null`
- `Max(None)` -> `Value::Null`
- `First(None)` -> `Value::Null`
- `Last(None)` -> `Value::Null`
- `GroupAs([])` -> `Value::Array([])`  (empty array -- verify this is desired)
- `ApproxCountDistinct(empty HLL)` -> `Value::Int(0)`
- `PercentileDisc { values: [] }` -> `Value::Null` (or error?)
- `ApproxPercentile { digest: empty, buffer: [] }` -> `Value::Null` (or error?)

This is critical because `Sum` initialized to `OrderedFloat(0.0)` will silently return the wrong value (0.0 instead of NULL) on empty input.

## Suggestions (nice to have)

### A. Consider whether `Value::Missing` should be treated like `Value::Null` for accumulation

Several aggregates currently don't handle `Value::Missing` at all. With the `ColumnLookup` strategy now returning `Value::Missing` instead of panicking, values like `Missing` could flow into `Sum`, `Avg`, `Min`, `Max`, etc. The current code for these aggregates (e.g., `SumAggregate::add_record`) returns `Err(AggregateError::InvalidType)` for any non-numeric type. This means a missing column will cause the entire query to fail rather than gracefully skip. The design should specify the policy: skip `Missing` values (like `Null` for `Count`), or propagate the error.

### B. GroupAs accumulator stores `Vec<Value>` but finalize should return `Value::Array`

The design shows `GroupAs(Vec<Value>)` as the accumulator state, which matches the current `GroupAsAggregate::tuples`. The current `get_aggregated` returns `Value::Array(tuples.clone())`. The design should confirm that `finalize()` for `GroupAs` wraps the vec in `Value::Array(...)`. This is presumably obvious but worth stating since the other finalize behaviors are implicit.

### C. `AccumulatorKind` is missing `HyperLogLog` initialization parameter

The existing `ApproxCountDistinctAggregate` creates `HyperLogLog::new(8)` (types.rs:1443) -- the `8` is the number of bits for the hash. The `AccumulatorKind::ApproxCountDistinct` variant carries no parameters. This means `AccumulatorState::new` for this kind must hardcode the `8`. This is fine if `8` is always the right value, but the design should note this explicit decision (or add a `precision: u8` field to the kind).

### D. The accumulation loop clones the key once per group-entry, not per row

The design (line 192) correctly notes "the group key is hashed exactly once per row." However, `entry(key.clone())` also clones the key on every row, not just when a new group is created. The `or_insert_with` closure runs only for new groups, but `key.clone()` happens unconditionally because Rust's `entry()` API requires an owned key. This is still better than the current N+1 clones, but the design could note this as a future optimization target (e.g., using `raw_entry_mut` from hashbrown to avoid the clone when the key already exists).

## Verified Claims (things I confirmed are correct)

1. **Critical #1 (finalize &mut self)**: Correctly addressed. The design now specifies `finalize(&mut self)` and documents why (ApproxPercentile buffer flush). Confirmed that `ApproxPercentileAggregate::get_aggregated` takes `&mut self` (types.rs:1096).

2. **Critical #2 (derive bounds)**: Correctly addressed. The design states `AccumulatorState` derives only `Debug` and `Clone`, explicitly documenting why `PartialEq`/`Eq` are excluded. Confirmed `HyperLogLog` does not derive `PartialEq` (types.rs:1409) and `ApproxCountDistinctAggregate` has a manual `PartialEq` impl (types.rs:1414-1425).

3. **Critical #3 (OrderedFloat)**: Correctly addressed. `Sum(OrderedFloat<f32>)` and `Avg { sum: OrderedFloat<f64>, ... }` now match the existing types (`SumAggregate` at types.rs:1171, `AvgAggregate` at types.rs:1134).

4. **Critical #4 (ExtractionStrategy)**: Substantially addressed. The new `ExtractionStrategy` enum with four variants (`Expression`, `ColumnLookup`, `RecordCapture`, `None`) correctly models all four value-extraction modes present in the current code. This is a significant improvement over the original `expr: Option<Expression>` approach.

5. **Critical #5 (Count null-skipping)**: Partially addressed. The design documents null-skipping for `Count` but misses `ApproxCountDistinct` (see Critical #2 above).

6. **Suggestion A (IndexMap)**: Evaluated and rejected with sound rationale (insertion overhead, existing convention, no ordering regression).

7. **Suggestion B (batch_groupby alignment)**: Addressed with a concrete migration plan (deprecation warnings, follow-up PR).

8. **Suggestion C (performance claims)**: Qualified appropriately with caveats about measurement methodology and adding a multi-aggregate benchmark.

9. **Suggestion D (Approach B deferred)**: Explicitly deferred with clear criteria for when to pursue it.

10. **Suggestion E (empty-input global aggregate)**: Specified, though with the `Sum` initial-value issue noted in Critical #3 above.

11. **ExtractionStrategy enum correctness**: Verified against all 11 aggregate match arms in stream.rs:420-527. The four variants cover all cases:
    - `Expression`: Avg, Count(expr), First, Last, Max, Min, Sum, ApproxCountDistinct (9 arms using `expr.expression_value()`)
    - `ColumnLookup`: PercentileDisc, ApproxPercentile (2 arms using `variables.get(column_name)`)
    - `RecordCapture`: GroupAs (1 arm using `record.to_variables().clone()`)
    - `None`: Count(*) (1 arm using `inner.add_row()`)

12. **No new compilation blockers**: The proposed types should compile. `AccumulatorState` only derives `Debug + Clone`, both of which are implemented by `HyperLogLog` (Clone) and `TDigest` (Clone). `Debug` is derived for both via the `#[derive(Debug)]` on their wrappers.
