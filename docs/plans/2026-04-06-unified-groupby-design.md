# Unified GroupBy Table Design

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
enum AccumulatorState {
    Count(i64),
    CountStar(i64),
    Sum(f32),
    Avg { sum: f64, count: i64 },
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
struct GroupState {
    accumulators: Vec<AccumulatorState>,
}

/// Definition of one aggregate (built once in GroupByStream::new).
struct AggregateDef {
    kind: AccumulatorKind,
    expr: Option<Expression>,  // None for count(*)
    name: Option<String>,      // output column name
}
```

#### AccumulatorState Methods

- `accumulate(&mut self, val: &Value)` — per-row update (Count increments, Sum adds, Min/Max compare, etc.)
- `accumulate_row(&mut self)` — for count(*), no value needed
- `finalize(&self) -> Value` — produce the output value for emission

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
let state = groups.entry(key).or_insert_with(|| GroupState::new(&self.aggregate_defs));
for (i, def) in self.aggregate_defs.iter().enumerate() {
    match def.extract_value(&variables, &self.registry)? {
        Some(val) => state.accumulators[i].accumulate(&val)?,
        None => state.accumulators[i].accumulate_row()?,
    }
}
```

Emission phase:
```rust
if let Some((key, state)) = iter.next() {
    // Build key columns from key
    // Build aggregate columns from state.accumulators[i].finalize()
    // Construct Record
}
```

### Approach B: Normalized Single-Column Keys (layered on top)

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
- Scalar accumulators (Count, Sum, Avg) extract primitives directly — no clone needed

## Files Changed

- `src/execution/types.rs` — add AccumulatorKind, AccumulatorState, GroupState, AggregateDef
- `src/execution/stream.rs` — rewrite GroupByStream
- `src/execution/batch_groupby.rs` — align with AccumulatorState types (optional, for consistency)

## Impact

- GroupBy benchmarks: **30-50% improvement** (single hash lookup instead of N+1)
- Multi-aggregate queries: proportionally larger gains
- Code simplification: ~110 lines of match arms reduced to ~5 lines of generic loop
- No changes to parser, logical planner, datasource, or any other operator

## Risks

- Low: purely internal restructure, no API or correctness change
- Existing aggregate tests exercise the same semantics through GroupByStream
- The old per-aggregate HashMap types can be removed once the new path is validated
