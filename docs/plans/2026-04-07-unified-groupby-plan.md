# Plan: Unified GroupBy Table

**Goal**: Replace the N+1 separate hash structures in `GroupByStream` with a single `HashMap<Option<Tuple>, GroupState>` to eliminate redundant key hashing and cloning, improving aggregation-heavy query performance.

**Architecture**: New `AccumulatorState` enum replaces per-aggregate HashMap types. Single unified hash table per GroupBy operator. No changes to parser, logical planner, or other operators.

**Tech Stack**: Rust, hashbrown, ordered-float, pdatastructs (HyperLogLog), tdigest

**Design doc**: `docs/plans/2026-04-06-unified-groupby-design-final.md`

---

## Step 1: Add `value_less_than` and `value_cmp` helpers

**File**: `src/execution/types.rs`

These are needed by `AccumulatorState::Min`, `Max`, and `PercentileDisc::finalize`.

### 1a. Write failing test

```rust
#[cfg(test)]
mod accumulator_tests {
    use super::*;
    use crate::common::types::Value;
    use ordered_float::OrderedFloat;

    #[test]
    fn test_value_less_than_ints() {
        assert!(value_less_than(&Value::Int(1), &Value::Int(2)));
        assert!(!value_less_than(&Value::Int(2), &Value::Int(1)));
        assert!(!value_less_than(&Value::Int(1), &Value::Int(1)));
    }

    #[test]
    fn test_value_less_than_floats() {
        assert!(value_less_than(
            &Value::Float(OrderedFloat(1.0)),
            &Value::Float(OrderedFloat(2.0))
        ));
    }

    #[test]
    fn test_value_less_than_strings() {
        assert!(value_less_than(
            &Value::String("a".to_string()),
            &Value::String("b".to_string())
        ));
    }

    #[test]
    fn test_value_cmp_asc_desc() {
        let a = Value::Int(1);
        let b = Value::Int(2);
        assert_eq!(value_cmp(&a, &b, &Ordering::Asc), std::cmp::Ordering::Less);
        assert_eq!(value_cmp(&a, &b, &Ordering::Desc), std::cmp::Ordering::Greater);
    }
}
```

### 1b. Run test to verify it fails

```bash
cargo test accumulator_tests -- --nocapture
```

### 1c. Write implementation

Add at the bottom of `src/execution/types.rs` (before existing tests):

```rust
/// Returns true if `a < b` for comparable Value types.
/// Used by Min/Max accumulators.
pub(crate) fn value_less_than(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(i1), Value::Int(i2)) => i1 < i2,
        (Value::Float(f1), Value::Float(f2)) => f1 < f2,
        (Value::String(s1), Value::String(s2)) => s1 < s2,
        (Value::DateTime(d1), Value::DateTime(d2)) => d1 < d2,
        (Value::Boolean(b1), Value::Boolean(b2)) => !b1 & b2,
        _ => false,
    }
}

/// Compare two Values with a given ordering direction.
/// Used by PercentileDisc finalize for sorting.
pub(crate) fn value_cmp(a: &Value, b: &Value, ordering: &Ordering) -> std::cmp::Ordering {
    let base = match (a, b) {
        (Value::Int(i1), Value::Int(i2)) => i1.cmp(i2),
        (Value::Float(f1), Value::Float(f2)) => f1.cmp(f2),
        (Value::String(s1), Value::String(s2)) => s1.cmp(s2),
        (Value::DateTime(d1), Value::DateTime(d2)) => d1.cmp(d2),
        (Value::Boolean(b1), Value::Boolean(b2)) => b1.cmp(b2),
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Host(h1), Value::Host(h2)) => h1.to_string().cmp(&h2.to_string()),
        (Value::HttpRequest(h1), Value::HttpRequest(h2)) => h1.to_string().cmp(&h2.to_string()),
        _ => std::cmp::Ordering::Equal,
    };
    match ordering {
        Ordering::Asc => base,
        Ordering::Desc => base.reverse(),
    }
}
```

### 1d. Run test to verify it passes

```bash
cargo test accumulator_tests -- --nocapture
```

---

## Step 2: Add `AccumulatorKind`, `ExtractionStrategy`, and `AccumulatorState` types

**File**: `src/execution/types.rs`

### 2a. Write failing test

```rust
#[test]
fn test_accumulator_state_new_count() {
    let state = AccumulatorState::new(&AccumulatorKind::Count);
    assert!(matches!(state, AccumulatorState::Count(0)));
}

#[test]
fn test_accumulator_state_new_sum() {
    let state = AccumulatorState::new(&AccumulatorKind::Sum);
    assert!(matches!(state, AccumulatorState::Sum(None)));
}

#[test]
fn test_accumulator_state_new_avg() {
    let state = AccumulatorState::new(&AccumulatorKind::Avg);
    match state {
        AccumulatorState::Avg { count, .. } => assert_eq!(count, 0),
        _ => panic!("expected Avg"),
    }
}
```

### 2b. Run test to verify it fails

```bash
cargo test accumulator_tests -- --nocapture
```

### 2c. Write implementation

Add enums and `AccumulatorState::new()` to `src/execution/types.rs` as specified in the design doc. The full code is in the design at lines 30-173.

Key points:
- `ExtractionStrategy` enum with 4 variants
- `AccumulatorKind` enum with 12 variants
- `AccumulatorState` enum — `#[derive(Debug, Clone)]` only, no `PartialEq`/`Eq`
- `GroupState` struct with `accumulators: Vec<AccumulatorState>`
- `AggregateDef` struct with `kind`, `extraction`, `name`
- `AccumulatorState::new(&AccumulatorKind) -> Self`

### 2d. Run test to verify it passes

```bash
cargo test accumulator_tests -- --nocapture
```

---

## Step 3: Implement `AccumulatorState::accumulate` and `accumulate_row`

**File**: `src/execution/types.rs`

### 3a. Write failing tests

```rust
#[test]
fn test_count_accumulate_skips_null() {
    let mut state = AccumulatorState::new(&AccumulatorKind::Count);
    state.accumulate(&Value::Int(1)).unwrap();
    state.accumulate(&Value::Null).unwrap();
    state.accumulate(&Value::Int(2)).unwrap();
    assert!(matches!(state, AccumulatorState::Count(2)));
}

#[test]
fn test_count_accumulate_skips_missing() {
    let mut state = AccumulatorState::new(&AccumulatorKind::Count);
    state.accumulate(&Value::Int(1)).unwrap();
    state.accumulate(&Value::Missing).unwrap();
    assert!(matches!(state, AccumulatorState::Count(1)));
}

#[test]
fn test_count_star_accumulate_row() {
    let mut state = AccumulatorState::new(&AccumulatorKind::CountStar);
    state.accumulate_row().unwrap();
    state.accumulate_row().unwrap();
    state.accumulate_row().unwrap();
    assert!(matches!(state, AccumulatorState::CountStar(3)));
}

#[test]
fn test_sum_accumulate() {
    let mut state = AccumulatorState::new(&AccumulatorKind::Sum);
    state.accumulate(&Value::Int(10)).unwrap();
    state.accumulate(&Value::Int(20)).unwrap();
    match &state {
        AccumulatorState::Sum(Some(s)) => assert_eq!(s.into_inner(), 30.0),
        _ => panic!("expected Sum(Some(...))"),
    }
}

#[test]
fn test_sum_skips_missing() {
    let mut state = AccumulatorState::new(&AccumulatorKind::Sum);
    state.accumulate(&Value::Missing).unwrap();
    assert!(matches!(state, AccumulatorState::Sum(None)));
}

#[test]
fn test_min_accumulate() {
    let mut state = AccumulatorState::new(&AccumulatorKind::Min);
    state.accumulate(&Value::Int(5)).unwrap();
    state.accumulate(&Value::Int(2)).unwrap();
    state.accumulate(&Value::Int(8)).unwrap();
    match &state {
        AccumulatorState::Min(Some(Value::Int(2))) => {}
        _ => panic!("expected Min(Some(Int(2)))"),
    }
}

#[test]
fn test_max_accumulate() {
    let mut state = AccumulatorState::new(&AccumulatorKind::Max);
    state.accumulate(&Value::Int(5)).unwrap();
    state.accumulate(&Value::Int(8)).unwrap();
    state.accumulate(&Value::Int(2)).unwrap();
    match &state {
        AccumulatorState::Max(Some(Value::Int(8))) => {}
        _ => panic!("expected Max(Some(Int(8)))"),
    }
}

#[test]
fn test_first_last_accumulate() {
    let mut first = AccumulatorState::new(&AccumulatorKind::First);
    let mut last = AccumulatorState::new(&AccumulatorKind::Last);
    for i in [1, 2, 3] {
        first.accumulate(&Value::Int(i)).unwrap();
        last.accumulate(&Value::Int(i)).unwrap();
    }
    assert!(matches!(first, AccumulatorState::First(Some(Value::Int(1)))));
    assert!(matches!(last, AccumulatorState::Last(Some(Value::Int(3)))));
}

#[test]
fn test_approx_count_distinct_skips_null() {
    let mut state = AccumulatorState::new(&AccumulatorKind::ApproxCountDistinct);
    state.accumulate(&Value::Int(1)).unwrap();
    state.accumulate(&Value::Null).unwrap();
    state.accumulate(&Value::Int(2)).unwrap();
    // HLL count should be ~2, not 3
    match &state {
        AccumulatorState::ApproxCountDistinct(hll) => {
            assert!(hll.count() >= 1); // HLL is approximate
        }
        _ => panic!("expected ApproxCountDistinct"),
    }
}
```

### 3b. Run test to verify they fail

```bash
cargo test accumulator_tests -- --nocapture
```

### 3c. Write implementation

Implement `accumulate()` and `accumulate_row()` as specified in the design doc (lines 176-263). Key behaviors:
- Universal `Value::Missing` skip at the top
- `Count` and `ApproxCountDistinct` skip `Value::Null`
- `Sum` uses `Option<OrderedFloat<f32>>` — `None` until first value
- `CountStar` panics if `accumulate()` is called (must use `accumulate_row()`)

### 3d. Run test to verify they pass

```bash
cargo test accumulator_tests -- --nocapture
```

---

## Step 4: Implement `AccumulatorState::finalize`

**File**: `src/execution/types.rs`

### 4a. Write failing tests

```rust
#[test]
fn test_finalize_count_zero() {
    let mut state = AccumulatorState::new(&AccumulatorKind::Count);
    assert_eq!(state.finalize().unwrap(), Value::Int(0));
}

#[test]
fn test_finalize_sum_empty_is_null() {
    let mut state = AccumulatorState::new(&AccumulatorKind::Sum);
    assert_eq!(state.finalize().unwrap(), Value::Null);
}

#[test]
fn test_finalize_sum_with_values() {
    let mut state = AccumulatorState::new(&AccumulatorKind::Sum);
    state.accumulate(&Value::Int(10)).unwrap();
    state.accumulate(&Value::Int(20)).unwrap();
    assert_eq!(state.finalize().unwrap(), Value::Float(OrderedFloat(30.0)));
}

#[test]
fn test_finalize_avg_empty_is_null() {
    let mut state = AccumulatorState::new(&AccumulatorKind::Avg);
    assert_eq!(state.finalize().unwrap(), Value::Null);
}

#[test]
fn test_finalize_avg_with_values() {
    let mut state = AccumulatorState::new(&AccumulatorKind::Avg);
    state.accumulate(&Value::Int(10)).unwrap();
    state.accumulate(&Value::Int(20)).unwrap();
    assert_eq!(state.finalize().unwrap(), Value::Float(OrderedFloat(15.0)));
}

#[test]
fn test_finalize_min_max_empty_is_null() {
    let mut min = AccumulatorState::new(&AccumulatorKind::Min);
    let mut max = AccumulatorState::new(&AccumulatorKind::Max);
    assert_eq!(min.finalize().unwrap(), Value::Null);
    assert_eq!(max.finalize().unwrap(), Value::Null);
}

#[test]
fn test_finalize_group_as_empty() {
    let mut state = AccumulatorState::new(&AccumulatorKind::GroupAs);
    assert_eq!(state.finalize().unwrap(), Value::Array(vec![]));
}
```

### 4b. Run test to verify they fail

```bash
cargo test accumulator_tests -- --nocapture
```

### 4c. Write implementation

Implement `finalize(&mut self)` as specified in the design doc (lines 278-323). Key behaviors per the zero-accumulate table:
- `Count/CountStar` -> `Value::Int(n as i32)`
- `Sum(None)` -> `Value::Null`, `Sum(Some(v))` -> `Value::Float(v)`
- `Avg { count: 0 }` -> `Value::Null`
- `Min/Max/First/Last(None)` -> `Value::Null`
- `GroupAs([])` -> `Value::Array([])`
- `ApproxCountDistinct` -> `Value::Int(hll.count() as i32)`
- `PercentileDisc { values: [] }` -> `Value::Null`
- `ApproxPercentile` flushes buffer then estimates, or `Value::Null` if empty

### 4d. Run test to verify they pass

```bash
cargo test accumulator_tests -- --nocapture
```

---

## Step 5: Add `AggregateDef` conversion from `NamedAggregate`

**File**: `src/execution/types.rs`

### 5a. Write failing test

```rust
#[test]
fn test_aggregate_def_from_count_star() {
    let na = NamedAggregate::new(
        Aggregate::Count(CountAggregate::new(), Named::Star),
        Some("cnt".to_string()),
    );
    let def = AggregateDef::from_named_aggregate(&na);
    assert!(matches!(def.kind, AccumulatorKind::CountStar));
    assert!(matches!(def.extraction, ExtractionStrategy::None));
    assert_eq!(def.name, Some("cnt".to_string()));
}

#[test]
fn test_aggregate_def_from_sum_expr() {
    let path = PathExpr::new(vec![PathSegment::AttrName("x".to_string())]);
    let na = NamedAggregate::new(
        Aggregate::Sum(
            SumAggregate::new(),
            Named::Expression(Expression::Variable(path), Some("x".to_string())),
        ),
        Some("total".to_string()),
    );
    let def = AggregateDef::from_named_aggregate(&na);
    assert!(matches!(def.kind, AccumulatorKind::Sum));
    assert!(matches!(def.extraction, ExtractionStrategy::Expression(_)));
    assert_eq!(def.name, Some("total".to_string()));
}

#[test]
fn test_aggregate_def_from_percentile_disc() {
    let na = NamedAggregate::new(
        Aggregate::PercentileDisc(
            PercentileDiscAggregate::new(OrderedFloat(0.5), Ordering::Asc),
            "x".to_string(),
        ),
        None,
    );
    let def = AggregateDef::from_named_aggregate(&na);
    assert!(matches!(def.kind, AccumulatorKind::PercentileDisc { .. }));
    assert!(matches!(def.extraction, ExtractionStrategy::ColumnLookup(_)));
}

#[test]
fn test_aggregate_def_from_group_as() {
    let na = NamedAggregate::new(
        Aggregate::GroupAs(
            GroupAsAggregate::new(),
            Named::Expression(
                Expression::Constant(Value::Null),
                None,
            ),
        ),
        Some("grp".to_string()),
    );
    let def = AggregateDef::from_named_aggregate(&na);
    assert!(matches!(def.kind, AccumulatorKind::GroupAs));
    assert!(matches!(def.extraction, ExtractionStrategy::RecordCapture));
}
```

### 5b. Run test to verify they fail

```bash
cargo test accumulator_tests -- --nocapture
```

### 5c. Write implementation

```rust
impl AggregateDef {
    pub(crate) fn from_named_aggregate(na: &NamedAggregate) -> Self {
        let (kind, extraction) = match &na.aggregate {
            Aggregate::Count(_, Named::Star) => (
                AccumulatorKind::CountStar,
                ExtractionStrategy::None,
            ),
            Aggregate::Count(_, Named::Expression(expr, _)) => (
                AccumulatorKind::Count,
                ExtractionStrategy::Expression(expr.clone()),
            ),
            Aggregate::Sum(_, Named::Expression(expr, _)) => (
                AccumulatorKind::Sum,
                ExtractionStrategy::Expression(expr.clone()),
            ),
            Aggregate::Avg(_, Named::Expression(expr, _)) => (
                AccumulatorKind::Avg,
                ExtractionStrategy::Expression(expr.clone()),
            ),
            Aggregate::Min(_, Named::Expression(expr, _)) => (
                AccumulatorKind::Min,
                ExtractionStrategy::Expression(expr.clone()),
            ),
            Aggregate::Max(_, Named::Expression(expr, _)) => (
                AccumulatorKind::Max,
                ExtractionStrategy::Expression(expr.clone()),
            ),
            Aggregate::First(_, Named::Expression(expr, _)) => (
                AccumulatorKind::First,
                ExtractionStrategy::Expression(expr.clone()),
            ),
            Aggregate::Last(_, Named::Expression(expr, _)) => (
                AccumulatorKind::Last,
                ExtractionStrategy::Expression(expr.clone()),
            ),
            Aggregate::ApproxCountDistinct(_, Named::Expression(expr, _)) => (
                AccumulatorKind::ApproxCountDistinct,
                ExtractionStrategy::Expression(expr.clone()),
            ),
            Aggregate::GroupAs(_, _) => (
                AccumulatorKind::GroupAs,
                ExtractionStrategy::RecordCapture,
            ),
            Aggregate::PercentileDisc(agg, col_name) => (
                AccumulatorKind::PercentileDisc {
                    percentile: agg.percentile,
                    ordering: agg.ordering.clone(),
                },
                ExtractionStrategy::ColumnLookup(col_name.clone()),
            ),
            Aggregate::ApproxPercentile(agg, col_name) => (
                AccumulatorKind::ApproxPercentile {
                    percentile: agg.percentile,
                    ordering: agg.ordering.clone(),
                },
                ExtractionStrategy::ColumnLookup(col_name.clone()),
            ),
            _ => unreachable!("Star variant not valid for non-Count aggregates"),
        };
        AggregateDef {
            kind,
            extraction,
            name: na.name_opt.clone(),
        }
    }
}
```

### 5d. Run test to verify they pass

```bash
cargo test accumulator_tests -- --nocapture
```

---

## Step 6: Rewrite `GroupByStream` to use unified group table

**File**: `src/execution/stream.rs`

### 6a. Write failing test

No new test needed — all existing GroupBy tests in `stream.rs::tests` and integration tests serve as the regression suite. The rewrite must pass them unchanged.

### 6b. Run existing tests to establish baseline

```bash
cargo test -- --nocapture
```

### 6c. Write implementation

Replace the `GroupByStream` struct and its `RecordStream` impl:

**Struct fields** change to:
```rust
pub struct GroupByStream {
    keys: Vec<ast::PathExpr>,
    variables: Variables,
    aggregate_defs: Vec<AggregateDef>,
    source: Box<dyn RecordStream>,
    groups: Option<HashMap<Option<Tuple>, GroupState>>,
    group_iterator: Option<hashbrown::hash_map::IntoIter<Option<Tuple>, GroupState>>,
    registry: Arc<FunctionRegistry>,
}
```

**`new()` constructor** converts `Vec<NamedAggregate>` to `Vec<AggregateDef>`:
```rust
pub fn new(..., aggregates: Vec<NamedAggregate>, ...) -> Self {
    let aggregate_defs: Vec<AggregateDef> = aggregates.iter()
        .map(AggregateDef::from_named_aggregate)
        .collect();
    GroupByStream { keys, variables, aggregate_defs, source, groups: None, group_iterator: None, registry }
}
```

**`next()` accumulation phase** — replace the ~110-line match with the unified loop from the design doc (lines 343-365). Key points:
- Single `groups.entry(key.clone()).or_insert_with(...)` per row
- `ExtractionStrategy` match for each aggregate def
- `RecordCapture` uses `record.to_variables().clone()`

**`next()` emission phase** — iterate `group_iterator`, call `finalize()` per accumulator.

**Empty-input global aggregate** — after accumulation, if `groups.is_empty() && self.keys.is_empty()`, synthesize one `GroupState::new(...)` and finalize without accumulation.

### 6d. Run all tests to verify they pass

```bash
cargo test -- --nocapture
```

---

## Step 7: Add multi-aggregate GroupBy benchmark

**File**: `benches/helpers/queries.rs` and `benches/bench_execution.rs`

### 7a. No failing test needed (benchmark addition)

### 7b. Write implementation

Add to `benches/helpers/queries.rs`:
```rust
pub const EXEC_E4: &str = "SELECT elbname, count(*), sum(received_bytes), avg(received_bytes) FROM elb GROUP BY elbname";
```

Add `E4` to the benchmark loop in `benches/bench_execution.rs` `bench_execution_tier_a`:
```rust
let queries = [
    ("E1_scan_limit", EXEC_E1),
    ("E2_groupby_count", EXEC_E2),
    ("E3_filter_orderby", EXEC_E3),
    ("E4_groupby_multi_agg", EXEC_E4),
];
```

### 7c. Run benchmark

```bash
cargo bench --bench bench_execution
```

---

## Step 8: Add edge case tests

**File**: `src/execution/stream.rs` (in `#[cfg(test)] mod tests`)

### 8a-c. Write and run tests

```rust
#[test]
fn test_groupby_count_skips_null() {
    // SELECT count(a) FROM t where some a values are Null
    // count(a) should not count nulls
    let path_expr_a = ast::PathExpr::new(vec![ast::PathSegment::AttrName("a".to_string())]);
    let mut records = VecDeque::new();
    records.push_back(Record::new(&vec!["a".to_string()], vec![Value::Int(1)]));
    records.push_back(Record::new(&vec!["a".to_string()], vec![Value::Null]));
    records.push_back(Record::new(&vec!["a".to_string()], vec![Value::Int(3)]));
    let stream = Box::new(InMemoryStream::new(records));
    let registry = Arc::new(crate::functions::register_all().unwrap());

    let aggregates = vec![
        types::NamedAggregate::new(
            types::Aggregate::Count(
                types::CountAggregate::new(),
                types::Named::Expression(
                    types::Expression::Variable(path_expr_a),
                    Some("a".to_string()),
                ),
            ),
            Some("cnt".to_string()),
        ),
    ];

    let mut stream = GroupByStream::new(vec![], Variables::default(), aggregates, stream, registry);

    let record = stream.next().unwrap().unwrap();
    assert_eq!(record.to_variables()["cnt"], Value::Int(2));
    assert!(stream.next().unwrap().is_none());
}

#[test]
fn test_groupby_count_star_counts_all() {
    let mut records = VecDeque::new();
    records.push_back(Record::new(&vec!["a".to_string()], vec![Value::Int(1)]));
    records.push_back(Record::new(&vec!["a".to_string()], vec![Value::Null]));
    records.push_back(Record::new(&vec!["a".to_string()], vec![Value::Int(3)]));
    let stream = Box::new(InMemoryStream::new(records));
    let registry = Arc::new(crate::functions::register_all().unwrap());

    let aggregates = vec![
        types::NamedAggregate::new(
            types::Aggregate::Count(types::CountAggregate::new(), types::Named::Star),
            Some("cnt".to_string()),
        ),
    ];

    let mut stream = GroupByStream::new(vec![], Variables::default(), aggregates, stream, registry);

    let record = stream.next().unwrap().unwrap();
    assert_eq!(record.to_variables()["cnt"], Value::Int(3));
    assert!(stream.next().unwrap().is_none());
}
```

### 8d. Run tests

```bash
cargo test -- --nocapture
```

---

## Step 9: End-to-end verification

### 9a. Run full test suite

```bash
cargo test -- --nocapture
```

### 9b. Run benchmarks and compare

```bash
cargo bench --bench bench_execution 2>&1 | tee bench_unified_groupby.log
```

Compare E2 (groupby_count) and E4 (groupby_multi_agg) against baseline numbers from `CHANGELOG.md`:
- E2 baseline: 2.16 ms
- E4: new benchmark, record initial numbers

### 9c. Verify no compiler warnings

```bash
cargo build --release 2>&1 | grep warning
```

---

## Task Dependencies

| Group | Steps | Can Parallelize | Files Touched |
|-------|-------|-----------------|---------------|
| 1 | Step 1 | Single | `src/execution/types.rs` |
| 2 | Steps 2, 3, 4 | Sequential (each builds on prior) | `src/execution/types.rs` |
| 3 | Step 5 | No (depends on Group 2) | `src/execution/types.rs` |
| 4 | Step 6 | No (depends on Group 3) | `src/execution/stream.rs` |
| 5 | Steps 7, 8 | Yes (independent) | `benches/`, `src/execution/stream.rs` |
| 6 | Step 9 | No (depends on all) | N/A (verification only) |
