# Unified GroupBy Table Design (final)

## Changes from v2

This revision addresses the 3 remaining critical issues from the round-2 review.

**Round 2 fixes:**
1. **ColumnLookup semantic change**: Explicitly documented that the `ColumnLookup` strategy changes the current `stream.rs` panic-on-missing-column (`.unwrap()`) to returning `Value::Missing`, aligning with `batch_groupby.rs`. Added policy that all accumulators skip `Value::Missing` the same way `Count` skips `Value::Null`.
2. **ApproxCountDistinct null-skipping**: Added explicit documentation that `ApproxCountDistinct::accumulate` must skip `Value::Null` inputs, same as `Count`, preserving the existing behavior in `ApproxCountDistinctAggregate::add_record`.
3. **Zero-accumulate finalize table**: Added a complete table specifying the expected `finalize()` output for each `AccumulatorState` variant when no `accumulate` calls have been made. Critically, `Sum` must return `Value::Null` (not `0.0`) for empty input per SQL standard. This requires a `has_value: bool` flag in the `Sum` variant (or using `Option<OrderedFloat<f32>>`).

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
    ///
    /// DELIBERATE SEMANTIC CHANGE: The current `stream.rs` uses `.unwrap()` which
    /// panics on missing columns. This design changes to `.unwrap_or(Value::Missing)`,
    /// aligning with the behavior in `batch_groupby.rs`. This is a bug fix, not a
    /// regression — panicking on a missing column is incorrect behavior.
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
///
/// ### Null / Missing handling policy
///
/// All accumulators apply the following policy uniformly:
/// - `Value::Null`: Skip (no-op) for Count, ApproxCountDistinct. Accumulate normally
///   for Min/Max/First/Last (they store the value). Error for Sum/Avg (InvalidType).
/// - `Value::Missing`: Skip (no-op) for ALL accumulators. Missing means the field
///   does not exist in the row; it should never contribute to an aggregate.
///
#[derive(Debug, Clone)]
enum AccumulatorState {
    /// Counts non-null, non-missing values only.
    Count(i64),

    /// Counts all rows regardless of value (for `count(*)`).
    CountStar(i64),

    /// Sum of numeric values. Uses Option to distinguish "no values seen" from "sum is 0".
    Sum(Option<OrderedFloat<f32>>),

    Avg { sum: OrderedFloat<f64>, count: i64 },

    Min(Option<Value>),
    Max(Option<Value>),
    First(Option<Value>),
    Last(Option<Value>),
    GroupAs(Vec<Value>),

    /// IMPORTANT: `accumulate` must skip `Value::Null` inputs, same as `Count`.
    /// This preserves the existing behavior in `ApproxCountDistinctAggregate::add_record`.
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

#### Zero-Accumulate Finalize Table

When no `accumulate` calls have been made (empty input for global aggregates), `finalize()` must return these values:

| Variant | Initial State | `finalize()` output | Notes |
|---------|--------------|---------------------|-------|
| `Count(0)` | 0 | `Value::Int(0)` | SQL standard |
| `CountStar(0)` | 0 | `Value::Int(0)` | SQL standard |
| `Sum(None)` | None | `Value::Null` | SQL standard: SUM of empty set is NULL, not 0 |
| `Avg { sum: 0, count: 0 }` | 0/0 | `Value::Null` | Division by zero -> NULL |
| `Min(None)` | None | `Value::Null` | No values seen |
| `Max(None)` | None | `Value::Null` | No values seen |
| `First(None)` | None | `Value::Null` | No values seen |
| `Last(None)` | None | `Value::Null` | No values seen |
| `GroupAs([])` | empty vec | `Value::Array([])` | Empty group produces empty array |
| `ApproxCountDistinct(empty)` | empty HLL(8) | `Value::Int(0)` | No distinct values seen |
| `PercentileDisc { values: [] }` | empty vec | `Value::Null` | No values to compute percentile |
| `ApproxPercentile { empty }` | empty | `Value::Null` | No values to compute percentile |

Note: `Sum` changed from `Sum(OrderedFloat<f32>)` to `Sum(Option<OrderedFloat<f32>>)` to support this. `accumulate` sets `Some(val)` on first value, adds on subsequent values. `finalize` returns `Value::Null` when `None`, `Value::Float(v)` when `Some(v)`.

#### AccumulatorState Methods

```rust
impl AccumulatorState {
    /// Create a fresh accumulator from its kind tag.
    fn new(kind: &AccumulatorKind) -> Self {
        match kind {
            AccumulatorKind::Count => AccumulatorState::Count(0),
            AccumulatorKind::CountStar => AccumulatorState::CountStar(0),
            AccumulatorKind::Sum => AccumulatorState::Sum(None),
            AccumulatorKind::Avg => AccumulatorState::Avg {
                sum: OrderedFloat(0.0), count: 0
            },
            AccumulatorKind::Min => AccumulatorState::Min(None),
            AccumulatorKind::Max => AccumulatorState::Max(None),
            AccumulatorKind::First => AccumulatorState::First(None),
            AccumulatorKind::Last => AccumulatorState::Last(None),
            AccumulatorKind::GroupAs => AccumulatorState::GroupAs(Vec::new()),
            AccumulatorKind::ApproxCountDistinct =>
                AccumulatorState::ApproxCountDistinct(HyperLogLog::new(8)),
            AccumulatorKind::PercentileDisc { percentile, ordering } =>
                AccumulatorState::PercentileDisc {
                    values: Vec::new(), percentile: *percentile, ordering: ordering.clone()
                },
            AccumulatorKind::ApproxPercentile { percentile, ordering } =>
                AccumulatorState::ApproxPercentile {
                    digest: TDigest::new_with_size(100),
                    buffer: Vec::new(),
                    percentile: *percentile,
                    ordering: ordering.clone(),
                },
        }
    }

    /// Per-row update with a value.
    ///
    /// Null/Missing policy:
    /// - Value::Missing: always skipped (no-op) for all variants
    /// - Value::Null: skipped for Count and ApproxCountDistinct;
    ///   accumulated normally for Min/Max/First/Last/GroupAs;
    ///   returns InvalidType error for Sum/Avg
    fn accumulate(&mut self, val: &Value) -> AggregateResult<()> {
        // Universal Missing skip
        if matches!(val, Value::Missing) {
            return Ok(());
        }
        match self {
            AccumulatorState::Count(count) => {
                if !matches!(val, Value::Null) { *count += 1; }
            }
            AccumulatorState::Sum(opt_sum) => {
                let fval = match val {
                    Value::Int(i) => OrderedFloat::from(*i as f32),
                    Value::Float(f) => *f,
                    _ => return Err(AggregateError::InvalidType),
                };
                *opt_sum = Some(match opt_sum {
                    Some(s) => OrderedFloat(s.into_inner() + fval.into_inner()),
                    None => fval,
                });
            }
            AccumulatorState::Avg { sum, count } => {
                let fval: f64 = match val {
                    Value::Int(i) => *i as f64,
                    Value::Float(f) => f.into_inner() as f64,
                    _ => return Err(AggregateError::InvalidType),
                };
                *sum += fval;
                *count += 1;
            }
            AccumulatorState::Min(current) => {
                match current {
                    None => { *current = Some(val.clone()); }
                    Some(cur) => {
                        if value_less_than(val, cur) { *current = Some(val.clone()); }
                    }
                }
            }
            AccumulatorState::Max(current) => {
                match current {
                    None => { *current = Some(val.clone()); }
                    Some(cur) => {
                        if value_less_than(cur, val) { *current = Some(val.clone()); }
                    }
                }
            }
            AccumulatorState::First(slot) => {
                if slot.is_none() { *slot = Some(val.clone()); }
            }
            AccumulatorState::Last(slot) => {
                *slot = Some(val.clone());
            }
            AccumulatorState::GroupAs(vals) => {
                vals.push(val.clone());
            }
            AccumulatorState::ApproxCountDistinct(hll) => {
                if !matches!(val, Value::Null) { hll.add(val); }
            }
            AccumulatorState::PercentileDisc { values, .. } => {
                values.push(val.clone());
            }
            AccumulatorState::ApproxPercentile { digest, buffer, .. } => {
                buffer.push(val.clone());
                if buffer.len() >= 10000 {
                    let mut fvec = Vec::new();
                    for v in buffer.iter() {
                        match v {
                            Value::Float(f) => fvec.push(f64::from(f.into_inner())),
                            Value::Int(i) => fvec.push(f64::from(*i)),
                            _ => return Err(AggregateError::InvalidType),
                        }
                    }
                    *digest = digest.merge_unsorted(fvec);
                    buffer.clear();
                }
            }
            AccumulatorState::CountStar(_) => {
                // Should use accumulate_row(), not accumulate()
                unreachable!("CountStar should use accumulate_row()");
            }
        }
        Ok(())
    }

    /// Per-row update with no value (for count(*) only).
    fn accumulate_row(&mut self) -> AggregateResult<()> {
        match self {
            AccumulatorState::CountStar(count) => { *count += 1; Ok(()) }
            _ => unreachable!("accumulate_row called on non-CountStar"),
        }
    }

    /// Produce the output value for emission.
    ///
    /// Takes `&mut self` because `ApproxPercentile` must flush its internal
    /// buffer into the TDigest before computing the quantile.
    fn finalize(&mut self) -> AggregateResult<Value> {
        match self {
            AccumulatorState::Count(c) | AccumulatorState::CountStar(c)
                => Ok(Value::Int(*c as i32)),
            AccumulatorState::Sum(opt_sum)
                => Ok(opt_sum.map(Value::Float).unwrap_or(Value::Null)),
            AccumulatorState::Avg { sum, count } => {
                if *count == 0 { return Ok(Value::Null); }
                Ok(Value::Float(OrderedFloat((sum.into_inner() / *count as f64) as f32)))
            }
            AccumulatorState::Min(v) | AccumulatorState::Max(v)
                | AccumulatorState::First(v) | AccumulatorState::Last(v)
                => Ok(v.clone().unwrap_or(Value::Null)),
            AccumulatorState::GroupAs(vals)
                => Ok(Value::Array(vals.clone())),
            AccumulatorState::ApproxCountDistinct(hll)
                => Ok(Value::Int(hll.count() as i32)),
            AccumulatorState::PercentileDisc { values, percentile, ordering } => {
                if values.is_empty() { return Ok(Value::Null); }
                // Sort and pick percentile index (same logic as current impl)
                values.sort_by(|a, b| value_cmp(a, b, ordering));
                let f32_pct: f32 = percentile.into_inner();
                let idx = ((values.len() as f32) * f32_pct) as usize;
                Ok(values[idx].clone())
            }
            AccumulatorState::ApproxPercentile { digest, buffer, percentile, .. } => {
                if buffer.is_empty() && digest.count() == 0 { return Ok(Value::Null); }
                // Flush remaining buffer
                if !buffer.is_empty() {
                    let mut fvec = Vec::new();
                    for v in buffer.iter() {
                        match v {
                            Value::Float(f) => fvec.push(f64::from(f.into_inner())),
                            Value::Int(i) => fvec.push(f64::from(*i)),
                            _ => return Err(AggregateError::InvalidType),
                        }
                    }
                    *digest = digest.merge_unsorted(fvec);
                    buffer.clear();
                }
                let f64_pct = f64::from(percentile.into_inner());
                let result = digest.estimate_quantile(f64_pct);
                Ok(Value::Float(OrderedFloat(result as f32)))
            }
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
            // DELIBERATE CHANGE from stream.rs .unwrap() to .unwrap_or(Value::Missing).
            // Aligns with batch_groupby.rs behavior. Missing values are skipped
            // by the universal Missing-skip in accumulate().
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

Note on key cloning: `entry(key.clone())` clones the key on every row, not just on first-seen. This is a limitation of Rust's `entry()` API. For a future optimization, `hashbrown::raw_entry_mut` could be used to avoid the clone when the key already exists (hash once, clone only on insert). This is deferred as Approach C.

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

When there is no input data and no group keys (global aggregate, e.g., `SELECT count(*) FROM empty_table`), the unified GroupByStream MUST emit exactly one row. Implementation: after the accumulation loop, if `groups.is_empty() && self.keys.is_empty()`, synthesize a single `GroupState` via `GroupState::new(...)` and call `finalize()` on each accumulator without any prior `accumulate` calls. The zero-accumulate finalize table above specifies the exact output for each variant.

When there is no input data but group keys ARE present, return `Ok(None)` (no rows): no groups means no output rows.

### Approach B: Normalized Single-Column Keys (DEFERRED)

**This approach is deferred to a follow-up.** It will only be pursued if profiling after Approach A shows that `Vec<Value>` allocation for the group key is a significant remaining cost.

### Approach C: Move Semantics + raw_entry (DEFERRED)

Deferred to a follow-up. Includes:
- `into_variables()` to consume records instead of cloning for aggregates that store values
- `hashbrown::raw_entry_mut` to avoid key cloning on existing groups

## Files Changed

- `src/execution/types.rs` — add `ExtractionStrategy`, `AccumulatorKind`, `AccumulatorState`, `GroupState`, `AggregateDef`; add `value_less_than` and `value_cmp` helper functions
- `src/execution/stream.rs` — rewrite `GroupByStream`
- `src/execution/batch_groupby.rs` — not changed in this PR; see migration plan below

## Migration Plan for batch_groupby.rs

The existing per-aggregate types (`CountAggregate`, `SumAggregate`, etc.) will be marked `#[deprecated(note = "Use AccumulatorState instead")]` once the new unified path is validated by all existing tests. The batch path (`batch_groupby.rs`) will migrate to the new `AccumulatorState`/`GroupState` types in a follow-up PR.

## Impact

- GroupBy benchmarks: **expected improvement for multi-aggregate queries** (single hash lookup instead of N+1). The magnitude depends on the ratio of hashing cost to total query cost. A multi-aggregate benchmark should be added to measure actual gains.
- Multi-aggregate queries: proportionally larger gains due to eliminating N-1 key clones
- Code simplification: ~110 lines of match arms reduced to ~15 lines of generic loop
- No changes to parser, logical planner, datasource, or any other operator

## Risks

- Low: purely internal restructure with one deliberate semantic change (ColumnLookup .unwrap() -> .unwrap_or(Value::Missing))
- Existing aggregate tests exercise the same semantics through GroupByStream
- The old per-aggregate HashMap types will be deprecated (not removed) once the new path is validated

## Testing

- All existing ~58 tests must pass unchanged (semantics-preserving refactor)
- Add a multi-aggregate GroupBy benchmark to `benches/bench_execution.rs`
- Add test for empty-input global aggregate: `SELECT count(*), sum(x), avg(x) FROM empty` -> `(0, NULL, NULL)`
- Add test for `count(expr)` null-skipping
- Add test for `count(*)` counting all rows including those with null fields
- Verify PercentileDisc/ApproxPercentile behavior with missing columns (should produce NULL, not panic)
