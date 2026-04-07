# Unified GroupBy Table Design (v2)

## Changes from previous version

This revision addresses all critical issues and incorporates selected suggestions from the first design review.

**Critical fixes:**
1. Changed `finalize(&self)` to `finalize(&mut self)` so that `ApproxPercentile` can flush its internal buffer into the TDigest before computing the quantile (Critical #1).
2. Specified that `AccumulatorState` derives only `Debug` and `Clone`. It does NOT derive `PartialEq` or `Eq` because `HyperLogLog` does not implement `PartialEq` and `TDigest` does not implement `Eq` soundly. Neither trait is needed for the unified group table (Critical #2).
3. Fixed `Sum` to use `OrderedFloat<f32>` and `Avg.sum` to use `OrderedFloat<f64>`, matching the existing aggregate types and avoiding bare-float `Eq`/`Hash` issues (Critical #3).
4. Replaced the single `expr: Option<Expression>` field in `AggregateDef` with an `ExtractionStrategy` enum that models four distinct value-extraction modes: expression evaluation, column-name lookup (for PercentileDisc/ApproxPercentile), record capture (for GroupAs), and none (for count(*)) (Critical #4).
5. Explicitly documented that `AccumulatorState::Count::accumulate` must skip `Value::Null` inputs, preserving the existing null-skipping semantics of `CountAggregate` (Critical #5).

**Incorporated suggestions:**
- (A) IndexMap: Evaluated but not adopted. `hashbrown::HashMap` remains the choice; see rationale in Risks section.
- (B) batch_groupby alignment: Old per-aggregate types (`CountAggregate`, `SumAggregate`, etc.) will be marked `#[deprecated]` once the new path is validated. Migration plan added.
- (C) Performance claims: Qualified with caveats about measurement methodology.
- (D) Approach B deferred: Normalized single-column keys (`GroupKey` enum) are explicitly deferred to a follow-up, contingent on profiling results after Approach A lands.
- (E) Empty-input global aggregate: Behavior now specified -- a global aggregate with no input emits one row (COUNT -> 0, others -> Null), matching `batch_groupby.rs`.

---

## Problem

`GroupByStream` currently uses N+1 separate hash structures per aggregation query:
- 1 `HashSet<Option<Tuple>>` for group tracking
- N `HashMap<Option<Tuple>, _>` (one per aggregate: Count, Sum, Avg, etc.)

For `SELECT host, count(*), sum(bytes) FROM logs GROUP BY host`, the group key is hashed **3 times per row** and cloned on every insert. This is the primary bottleneck for aggregation-heavy workloads.

## Design

### Approach A: Unified Group Table (primary)

Replace all per-aggregate HashMaps with a single `HashMap<Option<Tuple>, GroupState>`.

#### New Types (`src/execution/types.rs`)

```rust
/// How a given aggregate extracts its input value from each row.
enum ExtractionStrategy {
    /// Evaluate an expression against the row's variables (most aggregates).
    Expression(Expression),
    /// Look up a column by name in the row's variables (PercentileDisc, ApproxPercentile).
    ColumnLookup(String),
    /// Capture the entire record as a Value::Object (GroupAs).
    RecordCapture,
    /// No value needed; just count rows (count(*)).
    None,
}

/// What kind of accumulator to create (no data, just the type tag).
enum AccumulatorKind {
    Count,
    CountStar,
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    GroupAs,
    ApproxCountDistinct,
    PercentileDisc { percentile: OrderedFloat<f32>, ordering: Ordering },
    ApproxPercentile { percentile: OrderedFloat<f32>, ordering: Ordering },
}

/// Per-group accumulator state (one per aggregate in the SELECT).
///
/// Derives `Debug` and `Clone` only.
/// Does NOT derive `PartialEq` or `Eq` because:
///   - `HyperLogLog` (pdatastructs) does not implement `PartialEq`
///   - `TDigest` does not soundly implement `Eq` (NaN in f64 centroids)
/// Neither trait is required by the unified group table or GroupByStream.
#[derive(Debug, Clone)]
enum AccumulatorState {
    /// Counts non-null values only.
    /// IMPORTANT: `accumulate` MUST skip `Value::Null` inputs.
    /// This preserves the SQL standard `COUNT(expr)` semantics where
    /// NULL values do not contribute to the count.
    /// See `CountAggregate::add_record` (types.rs) for the existing behavior.
    Count(i64),

    /// Counts all rows regardless of value (for `count(*)`).
    CountStar(i64),

    Sum(OrderedFloat<f32>),

    Avg { sum: OrderedFloat<f64>, count: i64 },

    Min(Option<Value>),
    Max(Option<Value>),
    First(Option<Value>),
    Last(Option<Value>),
    GroupAs(Vec<Value>),
    ApproxCountDistinct(HyperLogLog<Value>),
    PercentileDisc { values: Vec<Value>, percentile: OrderedFloat<f32>, ordering: Ordering },
    ApproxPercentile { digest: TDigest, buffer: Vec<Value>, percentile: OrderedFloat<f32>, ordering: Ordering },
}

/// All aggregate state for a single group.
#[derive(Debug, Clone)]
struct GroupState {
    accumulators: Vec<AccumulatorState>,
}

/// Definition of one aggregate (built once in GroupByStream::new).
struct AggregateDef {
    kind: AccumulatorKind,
    extraction: ExtractionStrategy,
    name: Option<String>,      // output column name
}
```

#### AccumulatorState Methods

```rust
impl AccumulatorState {
    /// Create a fresh accumulator from its kind tag.
    fn new(kind: &AccumulatorKind) -> Self { ... }

    /// Per-row update with a value (Count, Sum, Avg, Min, Max, First, Last,
    /// GroupAs, ApproxCountDistinct, PercentileDisc, ApproxPercentile).
    ///
    /// For `Count`: MUST check if `val` is `Value::Null` and skip (no-op) if so.
    /// For `CountStar`: this method should not be called; use `accumulate_row` instead.
    fn accumulate(&mut self, val: &Value) -> AggregateResult<()> { ... }

    /// Per-row update with no value (for count(*) only).
    fn accumulate_row(&mut self) -> AggregateResult<()> { ... }

    /// Produce the output value for emission.
    ///
    /// Takes `&mut self` because `ApproxPercentile` must flush its internal
    /// buffer (up to 10,000 values) into the TDigest before computing the
    /// quantile. Other variants do not mutate but accept `&mut self` for
    /// uniform signature.
    fn finalize(&mut self) -> AggregateResult<Value> { ... }
}
```

#### `GroupState::new`

```rust
impl GroupState {
    fn new(defs: &[AggregateDef]) -> Self {
        GroupState {
            accumulators: defs.iter().map(|d| AccumulatorState::new(&d.kind)).collect(),
        }
    }
}
```

#### Updated GroupByStream (`src/execution/stream.rs`)

Fields change from:
```rust
aggregates: Vec<NamedAggregate>,
group_iterator: Option<HashSet<Option<Tuple>>::IntoIter>,
```
to:
```rust
aggregate_defs: Vec<AggregateDef>,
groups: Option<HashMap<Option<Tuple>, GroupState>>,
group_iterator: Option<hashbrown::hash_map::IntoIter<Option<Tuple>, GroupState>>,
```

Accumulation loop (replaces ~110 lines of per-aggregate match arms):
```rust
let state = groups.entry(key.clone()).or_insert_with(|| GroupState::new(&self.aggregate_defs));
for (i, def) in self.aggregate_defs.iter().enumerate() {
    match &def.extraction {
        ExtractionStrategy::Expression(expr) => {
            let val = expr.expression_value(&variables, &self.registry)?;
            state.accumulators[i].accumulate(&val)?;
        }
        ExtractionStrategy::ColumnLookup(col_name) => {
            let val = variables.get(col_name).cloned().unwrap_or(Value::Missing);
            state.accumulators[i].accumulate(&val)?;
        }
        ExtractionStrategy::RecordCapture => {
            let val = Value::Object(Box::new(record.to_variables().clone()));
            state.accumulators[i].accumulate(&val)?;
        }
        ExtractionStrategy::None => {
            state.accumulators[i].accumulate_row()?;
        }
    }
}
```

Note: The group key is hashed exactly once per row (the `entry()` call). The previous code hashed it 2+N times (HashSet contains + insert + N aggregate HashMap lookups).

#### Emission phase

```rust
if let Some((key, mut state)) = iter.next() {
    // Build key columns from key
    // Build aggregate columns: finalize takes &mut self
    for acc in state.accumulators.iter_mut() {
        values.push(acc.finalize()?);
    }
    // Construct Record
}
```

#### Empty-input global aggregate behavior

When there is no input data and no group keys (global aggregate, e.g., `SELECT count(*) FROM empty_table`), the unified GroupByStream MUST emit exactly one row with default values:
- `Count` / `CountStar` -> `Value::Int(0)`
- All other accumulators -> `Value::Null`

This matches the behavior in `batch_groupby.rs` (lines 256-278) and the SQL standard. Implementation: after the accumulation loop, if `groups.is_empty() && self.keys.is_empty()`, synthesize a single `GroupState` via `GroupState::new(...)` and call `finalize()` on each accumulator without any prior `accumulate` calls.

When there is no input data but group keys ARE present, return `Ok(None)` (no rows), which is correct: no groups means no output rows.

### Approach B: Normalized Single-Column Keys (DEFERRED)

**This approach is deferred to a follow-up.** It will only be pursued if profiling after Approach A shows that `Vec<Value>` allocation for the group key is a significant remaining cost. The design is retained here for reference but is NOT part of the initial implementation scope.

For `GROUP BY <single_field>`, avoid `Vec<Value>` wrapper:

```rust
enum GroupKey {
    None,               // global aggregate
    SingleInt(i32),
    SingleString(String),
    General(Vec<Value>),
}
```

Detected in `GroupByStream::new()` based on `keys.len()`. Eliminates Vec allocation and polymorphic Value hashing for the common single-key case.

### Approach C: Move Semantics (layered on top)

- After extracting the group key, consume the record via `into_variables()`
- Pass owned values to accumulators that store them (GroupAs, First, Last, PercentileDisc)
- Scalar accumulators (Count, Sum, Avg) extract primitives directly -- no clone needed

## Files Changed

- `src/execution/types.rs` -- add `ExtractionStrategy`, `AccumulatorKind`, `AccumulatorState`, `GroupState`, `AggregateDef`
- `src/execution/stream.rs` -- rewrite `GroupByStream`
- `src/execution/batch_groupby.rs` -- not changed in this PR; see migration plan below

## Migration Plan for batch_groupby.rs

The existing per-aggregate types (`CountAggregate`, `SumAggregate`, `AvgAggregate`, `MaxAggregate`, `MinAggregate`, `FirstAggregate`, `LastAggregate`, `GroupAsAggregate`, `ApproxCountDistinctAggregate`, `PercentileDiscAggregate`, `ApproxPercentileAggregate`) will be marked `#[deprecated(note = "Use AccumulatorState instead")]` once the new unified path is validated by all existing tests. The batch path (`batch_groupby.rs`) will migrate to the new `AccumulatorState`/`GroupState` types in a follow-up PR to avoid a large blast radius. Until then, both accumulator systems coexist but the old types carry deprecation warnings to prevent new usage.

## Impact

- GroupBy benchmarks: **expected improvement for multi-aggregate queries** (single hash lookup instead of N+1). The magnitude depends on the ratio of hashing cost to total query cost. For single-aggregate queries (e.g., `count(*)` only), the improvement is smaller since only one redundant hash lookup is eliminated (from 3 to 1). A multi-aggregate benchmark (e.g., `count(*), sum(bytes), avg(bytes)`) should be added to `benches/bench_execution.rs` to measure the actual gains before and after.
- Multi-aggregate queries: proportionally larger gains due to eliminating N-1 key clones
- Code simplification: ~110 lines of match arms reduced to ~15 lines of generic loop (with `ExtractionStrategy` match)
- No changes to parser, logical planner, datasource, or any other operator

## Risks

- Low: purely internal restructure, no API or correctness change
- Existing aggregate tests exercise the same semantics through GroupByStream
- The old per-aggregate HashMap types will be deprecated (not removed) once the new path is validated

### IndexMap evaluation (Suggestion A)

The reviewer suggested using `IndexMap` for deterministic insertion-order iteration. We choose to stay with `hashbrown::HashMap` for the following reasons:
1. The current `HashSet` already has no ordering guarantee, so there is no behavioral regression.
2. `hashbrown::HashMap` is the existing convention throughout the codebase.
3. `IndexMap` has a small but measurable overhead per insertion (~5-10% in microbenchmarks) due to maintaining an auxiliary index vector, which runs counter to the goal of this optimization.
4. If deterministic output ordering is needed for test stability, an explicit sort on the output side is preferable to paying per-insertion overhead on every query.

## Testing

- All existing ~58 tests must pass unchanged (semantics-preserving refactor).
- Add a multi-aggregate GroupBy benchmark to `benches/bench_execution.rs` to measure before/after performance.
- Add a test for empty-input global aggregate: `SELECT count(*), sum(x), avg(x) FROM empty` should return one row: `(0, NULL, NULL)`.
- Add a test for `count(expr)` null-skipping: `SELECT count(nullable_col) FROM ...` where some rows have NULL should return only the count of non-null values.
