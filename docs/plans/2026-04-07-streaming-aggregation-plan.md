# Plan: Streaming Aggregation for Time-Bucketed Queries

**Goal**: Eliminate HashMap overhead for `GROUP BY time_bucket()` queries on time-ordered ELB/ALB logs by using streaming O(1)-memory aggregation.
**Architecture**: New `BatchStreamingGroupByOperator` that fuses `time_bucket()` evaluation internally, bypassing the Map node. Detects qualifying queries in `try_get_batch()`. Parallel chunks merge boundary buckets via `AccumulatorState::merge`.
**Tech Stack**: Rust, existing logq batch/stream infrastructure, chrono for DateTime

---

## Step 1: Add `AccumulatorState::merge` method

**File**: `src/execution/types.rs`

### 1a. Write failing test

Add at the end of the existing `#[cfg(test)] mod tests` block in `src/execution/types.rs`:

```rust
#[test]
fn test_accumulator_merge_count() {
    let mut a = AccumulatorState::Count(5);
    let b = AccumulatorState::Count(3);
    a.merge(&b);
    assert_eq!(a.finalize().unwrap(), Value::Int(8));
}

#[test]
fn test_accumulator_merge_count_star() {
    let mut a = AccumulatorState::CountStar(10);
    let b = AccumulatorState::CountStar(7);
    a.merge(&b);
    assert_eq!(a.finalize().unwrap(), Value::Int(17));
}

#[test]
fn test_accumulator_merge_sum() {
    let mut a = AccumulatorState::Sum(Some(OrderedFloat(3.0f32)));
    let b = AccumulatorState::Sum(Some(OrderedFloat(7.0f32)));
    a.merge(&b);
    assert_eq!(a.finalize().unwrap(), Value::Float(OrderedFloat(10.0f32)));
}

#[test]
fn test_accumulator_merge_sum_none_left() {
    let mut a = AccumulatorState::Sum(None);
    let b = AccumulatorState::Sum(Some(OrderedFloat(5.0f32)));
    a.merge(&b);
    assert_eq!(a.finalize().unwrap(), Value::Float(OrderedFloat(5.0f32)));
}

#[test]
fn test_accumulator_merge_avg() {
    // a: sum=10.0, count=2 (avg=5.0)
    // b: sum=20.0, count=3 (avg=6.67)
    // merged: sum=30.0, count=5 (avg=6.0)
    let mut a = AccumulatorState::Avg { sum: OrderedFloat(10.0f64), count: 2 };
    let b = AccumulatorState::Avg { sum: OrderedFloat(20.0f64), count: 3 };
    a.merge(&b);
    assert_eq!(a.finalize().unwrap(), Value::Float(OrderedFloat(6.0f32)));
}

#[test]
fn test_accumulator_merge_min() {
    let mut a = AccumulatorState::Min(Some(Value::Int(5)));
    let b = AccumulatorState::Min(Some(Value::Int(3)));
    a.merge(&b);
    assert_eq!(a.finalize().unwrap(), Value::Int(3));
}

#[test]
fn test_accumulator_merge_max() {
    let mut a = AccumulatorState::Max(Some(Value::Int(5)));
    let b = AccumulatorState::Max(Some(Value::Int(8)));
    a.merge(&b);
    assert_eq!(a.finalize().unwrap(), Value::Int(8));
}

#[test]
fn test_accumulator_merge_first() {
    let mut a = AccumulatorState::First(Some(Value::String("early".to_string())));
    let b = AccumulatorState::First(Some(Value::String("late".to_string())));
    a.merge(&b);
    // First keeps the earlier chunk's value
    assert_eq!(a.finalize().unwrap(), Value::String("early".to_string()));
}

#[test]
fn test_accumulator_merge_last() {
    let mut a = AccumulatorState::Last(Some(Value::String("early".to_string())));
    let b = AccumulatorState::Last(Some(Value::String("late".to_string())));
    a.merge(&b);
    // Last takes the later chunk's value
    assert_eq!(a.finalize().unwrap(), Value::String("late".to_string()));
}
```

### 1b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_accumulator_merge -- --nocapture 2>&1 | tail -20
```

### 1c. Write implementation

Add to `impl AccumulatorState` in `src/execution/types.rs`, after the `finalize` method (around line 1737):

```rust
/// Merge another accumulator's state into this one.
/// Used to combine partial aggregations from adjacent parallel chunks.
/// `self` is from the earlier chunk, `other` is from the later chunk.
pub(crate) fn merge(&mut self, other: &AccumulatorState) {
    match (self, other) {
        (AccumulatorState::Count(a), AccumulatorState::Count(b)) => *a += b,
        (AccumulatorState::CountStar(a), AccumulatorState::CountStar(b)) => *a += b,
        (AccumulatorState::Sum(a), AccumulatorState::Sum(b)) => {
            match (a.as_mut(), b) {
                (Some(a_val), Some(b_val)) => *a_val = OrderedFloat(a_val.0 + b_val.0),
                (None, Some(b_val)) => *a = Some(*b_val),
                _ => {}
            }
        }
        (AccumulatorState::Avg { sum: a_sum, count: a_count },
         AccumulatorState::Avg { sum: b_sum, count: b_count }) => {
            *a_sum = OrderedFloat(a_sum.0 + b_sum.0);
            *a_count += b_count;
        }
        (AccumulatorState::Min(a), AccumulatorState::Min(b)) => {
            if let Some(b_val) = b {
                match a {
                    Some(a_val) if value_less_than(b_val, a_val) => *a = Some(b_val.clone()),
                    None => *a = Some(b_val.clone()),
                    _ => {}
                }
            }
        }
        (AccumulatorState::Max(a), AccumulatorState::Max(b)) => {
            if let Some(b_val) = b {
                match a {
                    Some(a_val) if value_less_than(a_val, b_val) => *a = Some(b_val.clone()),
                    None => *a = Some(b_val.clone()),
                    _ => {}
                }
            }
        }
        (AccumulatorState::First(_), AccumulatorState::First(_)) => {
            // Keep self (earlier chunk) — no-op
        }
        (AccumulatorState::Last(a), AccumulatorState::Last(b)) => {
            if b.is_some() {
                *a = b.clone();
            }
        }
        (AccumulatorState::GroupAs(a_vals), AccumulatorState::GroupAs(b_vals)) => {
            a_vals.extend(b_vals.iter().cloned());
        }
        (AccumulatorState::ApproxCountDistinct(a), AccumulatorState::ApproxCountDistinct(b)) => {
            a.merge(b);
        }
        (AccumulatorState::PercentileDisc { values: a_vals, .. },
         AccumulatorState::PercentileDisc { values: b_vals, .. }) => {
            a_vals.extend(b_vals.iter().cloned());
        }
        (AccumulatorState::ApproxPercentile { digest: a_digest, buffer: a_buf, .. },
         AccumulatorState::ApproxPercentile { digest: b_digest, buffer: b_buf, .. }) => {
            // Flush a's buffer
            if !a_buf.is_empty() {
                let a_values: Vec<f64> = a_buf.drain(..).filter_map(|v| match v {
                    Value::Float(f) => Some(f.0 as f64),
                    Value::Int(i) => Some(i as f64),
                    _ => None,
                }).collect();
                if !a_values.is_empty() {
                    let mut sorted = a_values;
                    sorted.sort_by(|x, y| x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal));
                    *a_digest = TDigest::merge_digests(vec![a_digest.clone(), TDigest::new_with_size(100).merge_sorted(&sorted)]);
                }
            }
            // Flush b's buffer and merge
            let b_flushed = if !b_buf.is_empty() {
                let b_values: Vec<f64> = b_buf.iter().filter_map(|v| match v {
                    Value::Float(f) => Some(f.0 as f64),
                    Value::Int(i) => Some(*i as f64),
                    _ => None,
                }).collect();
                if !b_values.is_empty() {
                    let mut sorted = b_values;
                    sorted.sort_by(|x, y| x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal));
                    TDigest::merge_digests(vec![b_digest.clone(), TDigest::new_with_size(100).merge_sorted(&sorted)])
                } else {
                    b_digest.clone()
                }
            } else {
                b_digest.clone()
            };
            *a_digest = TDigest::merge_digests(vec![a_digest.clone(), b_flushed]);
        }
        _ => {
            panic!("mismatched accumulator types in merge");
        }
    }
}
```

### 1d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_accumulator_merge -- --nocapture 2>&1 | tail -20
```

### 1e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add src/execution/types.rs && git commit -m "Add AccumulatorState::merge for combining partial aggregations"
```

---

## Step 2: Add `GroupState::merge` method

**File**: `src/execution/types.rs`

### 2a. Write failing test

```rust
#[test]
fn test_group_state_merge() {
    use crate::execution::types::{AccumulatorKind, AggregateDef, ExtractionStrategy};

    let defs = vec![
        AggregateDef { kind: AccumulatorKind::CountStar, extraction: ExtractionStrategy::None, name: Some("cnt".to_string()) },
        AggregateDef { kind: AccumulatorKind::Sum, extraction: ExtractionStrategy::None, name: Some("total".to_string()) },
    ];
    let mut gs_a = GroupState::new(&defs);
    // Simulate: chunk A saw 3 rows, sum=10
    gs_a.accumulators[0] = AccumulatorState::CountStar(3);
    gs_a.accumulators[1] = AccumulatorState::Sum(Some(OrderedFloat(10.0f32)));

    let mut gs_b = GroupState::new(&defs);
    // Simulate: chunk B saw 2 rows, sum=5
    gs_b.accumulators[0] = AccumulatorState::CountStar(2);
    gs_b.accumulators[1] = AccumulatorState::Sum(Some(OrderedFloat(5.0f32)));

    gs_a.merge(&gs_b);
    assert_eq!(gs_a.accumulators[0].finalize().unwrap(), Value::Int(5));
    assert_eq!(gs_a.accumulators[1].finalize().unwrap(), Value::Float(OrderedFloat(15.0f32)));
}
```

### 2b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_group_state_merge -- --nocapture 2>&1 | tail -20
```

### 2c. Write implementation

Add to `impl GroupState` in `src/execution/types.rs`, after the `new` method (around line 1751):

```rust
/// Merge another GroupState into this one, element-wise.
/// `self` is from the earlier chunk, `other` is from the later chunk.
pub(crate) fn merge(&mut self, other: &GroupState) {
    assert_eq!(self.accumulators.len(), other.accumulators.len());
    for (a, b) in self.accumulators.iter_mut().zip(other.accumulators.iter()) {
        a.merge(b);
    }
}
```

### 2d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_group_state_merge -- --nocapture 2>&1 | tail -20
```

### 2e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add src/execution/types.rs && git commit -m "Add GroupState::merge for element-wise accumulator merging"
```

---

## Step 3: Add `is_time_ordered` to datasource metadata

**File**: `src/execution/datasource.rs`

### 3a. Write failing test

Add at the end of `src/execution/datasource.rs` (or in a new test module):

```rust
#[cfg(test)]
mod format_tests {
    #[test]
    fn test_elb_is_time_ordered() {
        assert_eq!(super::is_time_ordered("elb"), Some("timestamp"));
    }

    #[test]
    fn test_alb_is_time_ordered() {
        assert_eq!(super::is_time_ordered("alb"), Some("timestamp"));
    }

    #[test]
    fn test_s3_is_not_time_ordered() {
        assert_eq!(super::is_time_ordered("s3"), None);
    }

    #[test]
    fn test_squid_is_not_time_ordered() {
        assert_eq!(super::is_time_ordered("squid"), None);
    }

    #[test]
    fn test_jsonl_is_not_time_ordered() {
        assert_eq!(super::is_time_ordered("jsonl"), None);
    }
}
```

### 3b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test format_tests -- --nocapture 2>&1 | tail -20
```

### 3c. Write implementation

Add a free function in `src/execution/datasource.rs` (near the top, after imports):

```rust
/// Returns the timestamp column name if this log format guarantees
/// chronological ordering within a single file.
/// Only ELB and ALB individual log files are guaranteed time-ordered by AWS.
pub(crate) fn is_time_ordered(format: &str) -> Option<&'static str> {
    match format {
        "elb" => Some("timestamp"),
        "alb" => Some("timestamp"),
        _ => None,
    }
}
```

### 3d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test format_tests -- --nocapture 2>&1 | tail -20
```

### 3e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add src/execution/datasource.rs && git commit -m "Add is_time_ordered metadata for ELB/ALB log formats"
```

---

## Step 4: Create `BatchStreamingGroupByOperator` — struct and constructor

**File**: `src/execution/batch_streaming_groupby.rs` (new file)

### 4a. Write failing test

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Value;
    use crate::execution::batch::{BatchSchema, BatchStream, ColumnBatch, ColumnType, TypedColumn};
    use crate::execution::types::{
        Aggregate, CountAggregate, Named, NamedAggregate, StreamResult,
    };
    use crate::functions::FunctionRegistry;
    use crate::simd::bitmap::Bitmap;
    use crate::simd::padded_vec::PaddedVecBuilder;
    use crate::simd::selection::SelectionVector;
    use linked_hash_map::LinkedHashMap;
    use std::sync::Arc;

    /// Helper: build a Mixed column from Values.
    fn build_mixed_column(values: Vec<Value>) -> TypedColumn {
        let n = values.len();
        TypedColumn::Mixed {
            data: values,
            null: Bitmap::all_set(n),
            missing: Bitmap::all_set(n),
        }
    }

    /// Multi-batch test helper stream.
    struct MultiBatchStream {
        batches: std::collections::VecDeque<ColumnBatch>,
        schema: BatchSchema,
    }

    impl BatchStream for MultiBatchStream {
        fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
            Ok(self.batches.pop_front())
        }
        fn schema(&self) -> &BatchSchema {
            &self.schema
        }
        fn close(&self) {}
    }

    #[test]
    fn test_streaming_groupby_constructs() {
        let schema = BatchSchema { names: vec!["timestamp".into()], types: vec![ColumnType::Mixed] };
        let child = MultiBatchStream { batches: std::collections::VecDeque::new(), schema };
        let registry = Arc::new(FunctionRegistry::new());
        let variables = LinkedHashMap::new();

        let op = BatchStreamingGroupByOperator::new(
            Box::new(child),
            "timestamp".to_string(),
            "5 minutes".to_string(),
            "bucket".to_string(),
            vec![NamedAggregate::new(
                Aggregate::Count(CountAggregate::new(), Named::Star),
                Some("cnt".to_string()),
            )],
            variables,
            registry,
        );
        assert!(op.current_key.is_none());
        assert!(op.current_state.is_none());
    }
}
```

### 4b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_streaming_groupby_constructs -- --nocapture 2>&1 | tail -20
```

### 4c. Write implementation

Create `src/execution/batch_streaming_groupby.rs`:

```rust
// src/execution/batch_streaming_groupby.rs

use std::collections::VecDeque;
use std::sync::Arc;

use crate::common::types::{Value, Variables};
use crate::execution::batch::{
    BatchSchema, BatchStream, BatchToRowAdapter, ColumnBatch, ColumnType, TypedColumn,
};
use crate::execution::types::{
    AccumulatorState, AggregateDef, GroupState, NamedAggregate, StreamError, StreamResult,
};
use crate::functions::FunctionRegistry;
use crate::simd::bitmap::Bitmap;
use crate::simd::selection::SelectionVector;

/// Maximum number of completed groups to buffer before emitting a batch.
const OUTPUT_BATCH_SIZE: usize = 64;

/// Streaming GroupBy operator for time-bucketed queries on time-ordered data.
///
/// Instead of building a HashMap of all groups, this operator maintains a single
/// active group and emits completed groups as soon as the bucket boundary passes.
/// Requires input to be chronologically ordered.
///
/// Fuses `time_bucket()` evaluation internally — receives raw timestamp column
/// from the child BatchScan and computes buckets itself.
pub(crate) struct BatchStreamingGroupByOperator {
    /// Child batch stream providing ordered input with raw timestamp column.
    input: Box<dyn BatchStream>,
    /// The name of the timestamp column in the input batches.
    timestamp_column: String,
    /// The bucket interval string (e.g., "5 minutes").
    bucket_interval: String,
    /// The output alias for the bucket column.
    bucket_alias: String,
    /// Aggregate definitions, converted from Vec<NamedAggregate>.
    aggregate_defs: Vec<AggregateDef>,
    /// Output names for aggregate columns.
    aggregate_names: Vec<String>,
    /// The current bucket key being accumulated.
    current_key: Option<Value>,
    /// The current group's accumulator state.
    current_state: Option<GroupState>,
    /// Buffer of completed group keys.
    completed_keys: Vec<Value>,
    /// Buffer of completed group finalized values.
    completed_values: Vec<Vec<Value>>,
    /// Whether the input stream has been fully consumed.
    input_exhausted: bool,
    /// Variables for expression evaluation.
    variables: Variables,
    /// Function registry for expression evaluation.
    registry: Arc<FunctionRegistry>,
    /// Batch schema for the output.
    output_schema: BatchSchema,
}

impl BatchStreamingGroupByOperator {
    pub fn new(
        input: Box<dyn BatchStream>,
        timestamp_column: String,
        bucket_interval: String,
        bucket_alias: String,
        aggregates: Vec<NamedAggregate>,
        variables: Variables,
        registry: Arc<FunctionRegistry>,
    ) -> Self {
        let aggregate_defs: Vec<AggregateDef> = aggregates
            .iter()
            .map(AggregateDef::from_named_aggregate)
            .collect();

        // Build output column names: bucket alias + aggregate names
        let mut output_names = vec![bucket_alias.clone()];
        let mut aggregate_names = Vec::with_capacity(aggregates.len());
        for (idx, na) in aggregates.iter().enumerate() {
            let name = na.name_opt.clone().unwrap_or_else(|| format!("_{}", idx + 2));
            aggregate_names.push(name.clone());
            output_names.push(name);
        }

        let output_schema = BatchSchema {
            names: output_names.clone(),
            types: vec![ColumnType::Mixed; output_names.len()],
        };

        Self {
            input,
            timestamp_column,
            bucket_interval,
            bucket_alias,
            aggregate_defs,
            aggregate_names,
            current_key: None,
            current_state: None,
            completed_keys: Vec::new(),
            completed_values: Vec::new(),
            input_exhausted: false,
            variables,
            registry,
            output_schema,
        }
    }
}
```

Also register the module in `src/execution/mod.rs`:

```rust
pub mod batch_streaming_groupby;
```

### 4d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_streaming_groupby_constructs -- --nocapture 2>&1 | tail -20
```

### 4e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add src/execution/batch_streaming_groupby.rs src/execution/mod.rs && git commit -m "Add BatchStreamingGroupByOperator struct and constructor"
```

---

## Step 5: Implement `compute_bucket` helper

**File**: `src/execution/batch_streaming_groupby.rs`

### 5a. Write failing test

```rust
#[test]
fn test_compute_bucket() {
    use chrono::TimeZone;
    let registry = Arc::new(FunctionRegistry::new());

    let ts = chrono::FixedOffset::east_opt(0).unwrap()
        .with_ymd_and_hms(2026, 4, 7, 10, 13, 45).unwrap();
    let ts_val = Value::DateTime(ts);

    let bucket = BatchStreamingGroupByOperator::compute_bucket(
        &ts_val, "5 minutes", &registry
    ).unwrap();

    let expected = chrono::FixedOffset::east_opt(0).unwrap()
        .with_ymd_and_hms(2026, 4, 7, 10, 10, 0).unwrap();
    assert_eq!(bucket, Value::DateTime(expected));
}
```

### 5b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_compute_bucket -- --nocapture 2>&1 | tail -20
```

### 5c. Write implementation

Add to `impl BatchStreamingGroupByOperator`:

```rust
/// Compute the time_bucket value for a single timestamp.
fn compute_bucket(
    ts_val: &Value,
    interval: &str,
    registry: &Arc<FunctionRegistry>,
) -> Result<Value, StreamError> {
    let args = vec![Value::String(interval.to_string()), ts_val.clone()];
    registry
        .call("time_bucket", &args)
        .map_err(StreamError::Expression)
}
```

### 5d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_compute_bucket -- --nocapture 2>&1 | tail -20
```

### 5e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add src/execution/batch_streaming_groupby.rs && git commit -m "Add compute_bucket helper for time_bucket evaluation"
```

---

## Step 6: Implement `flush_completed` and `emit_output_batch` helpers

**File**: `src/execution/batch_streaming_groupby.rs`

### 6a. Write failing test

```rust
#[test]
fn test_flush_completed_builds_batch() {
    let registry = Arc::new(FunctionRegistry::new());
    let variables = LinkedHashMap::new();
    let schema = BatchSchema { names: vec!["timestamp".into()], types: vec![ColumnType::Mixed] };
    let child = MultiBatchStream { batches: VecDeque::new(), schema };

    let mut op = BatchStreamingGroupByOperator::new(
        Box::new(child),
        "timestamp".to_string(),
        "5 minutes".to_string(),
        "bucket".to_string(),
        vec![NamedAggregate::new(
            Aggregate::Count(CountAggregate::new(), Named::Star),
            Some("cnt".to_string()),
        )],
        variables,
        registry,
    );

    // Manually push completed groups
    op.completed_keys.push(Value::String("bucket_a".to_string()));
    op.completed_values.push(vec![Value::Int(10)]);
    op.completed_keys.push(Value::String("bucket_b".to_string()));
    op.completed_values.push(vec![Value::Int(20)]);

    let batch = op.emit_output_batch().unwrap();
    assert_eq!(batch.len, 2);
    assert_eq!(batch.names[0], "bucket");
    assert_eq!(batch.names[1], "cnt");

    // Check values
    let k0 = BatchToRowAdapter::extract_value(&batch.columns[0], 0);
    assert_eq!(k0, Value::String("bucket_a".to_string()));
    let v0 = BatchToRowAdapter::extract_value(&batch.columns[1], 0);
    assert_eq!(v0, Value::Int(10));
    let k1 = BatchToRowAdapter::extract_value(&batch.columns[0], 1);
    assert_eq!(k1, Value::String("bucket_b".to_string()));
    let v1 = BatchToRowAdapter::extract_value(&batch.columns[1], 1);
    assert_eq!(v1, Value::Int(20));
}
```

### 6b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_flush_completed_builds_batch -- --nocapture 2>&1 | tail -20
```

### 6c. Write implementation

Add to `impl BatchStreamingGroupByOperator`:

```rust
/// Finalize the current group and push to completed buffers.
fn flush_current_group(&mut self) -> StreamResult<()> {
    if let (Some(key), Some(mut state)) = (self.current_key.take(), self.current_state.take()) {
        let mut values = Vec::with_capacity(state.accumulators.len());
        for acc in state.accumulators.iter_mut() {
            values.push(acc.finalize().map_err(StreamError::Aggregate)?);
        }
        self.completed_keys.push(key);
        self.completed_values.push(values);
    }
    Ok(())
}

/// Build a ColumnBatch from the completed group buffers, draining them.
fn emit_output_batch(&mut self) -> StreamResult<ColumnBatch> {
    let num_rows = self.completed_keys.len();
    let num_agg_cols = self.aggregate_defs.len();

    // Key column
    let key_col = TypedColumn::Mixed {
        data: std::mem::take(&mut self.completed_keys),
        null: Bitmap::all_set(num_rows),
        missing: Bitmap::all_set(num_rows),
    };

    // Aggregate columns — transpose from row-major to column-major
    let mut agg_columns: Vec<Vec<Value>> = vec![Vec::with_capacity(num_rows); num_agg_cols];
    for row_values in self.completed_values.drain(..) {
        for (col_idx, val) in row_values.into_iter().enumerate() {
            agg_columns[col_idx].push(val);
        }
    }

    let mut columns = vec![key_col];
    for col_data in agg_columns {
        columns.push(TypedColumn::Mixed {
            data: col_data,
            null: Bitmap::all_set(num_rows),
            missing: Bitmap::all_set(num_rows),
        });
    }

    let mut names = vec![self.bucket_alias.clone()];
    names.extend(self.aggregate_names.clone());

    Ok(ColumnBatch {
        columns,
        names,
        selection: SelectionVector::All,
        len: num_rows,
    })
}
```

### 6d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_flush_completed_builds_batch -- --nocapture 2>&1 | tail -20
```

### 6e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add src/execution/batch_streaming_groupby.rs && git commit -m "Add flush_current_group and emit_output_batch helpers"
```

---

## Step 7: Implement `process_batch` — core streaming logic

**File**: `src/execution/batch_streaming_groupby.rs`

### 7a. Write failing test

```rust
#[test]
fn test_streaming_groupby_single_batch_two_buckets() {
    use chrono::TimeZone;
    let registry = Arc::new(FunctionRegistry::new());
    let variables = LinkedHashMap::new();

    let utc = chrono::FixedOffset::east_opt(0).unwrap();
    // 4 timestamps: two in 10:00 bucket, two in 10:05 bucket
    let ts_values = vec![
        Value::DateTime(utc.with_ymd_and_hms(2026, 4, 7, 10, 1, 0).unwrap()),
        Value::DateTime(utc.with_ymd_and_hms(2026, 4, 7, 10, 3, 0).unwrap()),
        Value::DateTime(utc.with_ymd_and_hms(2026, 4, 7, 10, 6, 0).unwrap()),
        Value::DateTime(utc.with_ymd_and_hms(2026, 4, 7, 10, 8, 0).unwrap()),
    ];

    let ts_col = build_mixed_column(ts_values);
    let batch = ColumnBatch {
        columns: vec![ts_col],
        names: vec!["timestamp".to_string()],
        selection: SelectionVector::All,
        len: 4,
    };
    let schema = BatchSchema { names: vec!["timestamp".into()], types: vec![ColumnType::Mixed] };
    let child = MultiBatchStream { batches: VecDeque::from(vec![batch]), schema };

    let mut op = BatchStreamingGroupByOperator::new(
        Box::new(child),
        "timestamp".to_string(),
        "5 minutes".to_string(),
        "bucket".to_string(),
        vec![NamedAggregate::new(
            Aggregate::Count(CountAggregate::new(), Named::Star),
            Some("cnt".to_string()),
        )],
        variables,
        registry,
    );

    // First call: should get at least the first completed bucket (10:00 -> count=2)
    let batch1 = op.next_batch().unwrap();
    assert!(batch1.is_some());

    // Collect all output batches
    let mut all_keys = Vec::new();
    let mut all_counts = Vec::new();
    if let Some(b) = batch1 {
        for i in 0..b.len {
            all_keys.push(BatchToRowAdapter::extract_value(&b.columns[0], i));
            all_counts.push(BatchToRowAdapter::extract_value(&b.columns[1], i));
        }
    }
    // Get remaining batches
    loop {
        match op.next_batch().unwrap() {
            Some(b) => {
                for i in 0..b.len {
                    all_keys.push(BatchToRowAdapter::extract_value(&b.columns[0], i));
                    all_counts.push(BatchToRowAdapter::extract_value(&b.columns[1], i));
                }
            }
            None => break,
        }
    }

    assert_eq!(all_keys.len(), 2);
    let expected_bucket_1 = Value::DateTime(
        chrono::FixedOffset::east_opt(0).unwrap()
            .with_ymd_and_hms(2026, 4, 7, 10, 0, 0).unwrap()
    );
    let expected_bucket_2 = Value::DateTime(
        chrono::FixedOffset::east_opt(0).unwrap()
            .with_ymd_and_hms(2026, 4, 7, 10, 5, 0).unwrap()
    );
    assert_eq!(all_keys[0], expected_bucket_1);
    assert_eq!(all_counts[0], Value::Int(2));
    assert_eq!(all_keys[1], expected_bucket_2);
    assert_eq!(all_counts[1], Value::Int(2));
}
```

### 7b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_streaming_groupby_single_batch_two_buckets -- --nocapture 2>&1 | tail -20
```

### 7c. Write implementation

Add to `impl BatchStreamingGroupByOperator`:

```rust
/// Process one input batch: compute buckets, detect boundaries, accumulate.
fn process_batch(&mut self, batch: &ColumnBatch) -> StreamResult<()> {
    // Find the timestamp column index
    let ts_idx = batch.names.iter()
        .position(|n| n == &self.timestamp_column)
        .ok_or_else(|| StreamError::Expression(
            crate::execution::types::ExpressionError::KeyNotFound
        ))?;

    for row_idx in 0..batch.len {
        if !batch.selection.is_active(row_idx, batch.len) {
            continue;
        }

        // Extract timestamp value and compute bucket
        let ts_val = BatchToRowAdapter::extract_value(&batch.columns[ts_idx], row_idx);
        let bucket = Self::compute_bucket(&ts_val, &self.bucket_interval, &self.registry)?;

        // Check if this is the same bucket or a new one
        match &self.current_key {
            None => {
                // First row ever
                self.current_key = Some(bucket);
                self.current_state = Some(GroupState::new(&self.aggregate_defs));
            }
            Some(current) if *current == bucket => {
                // Same bucket — continue accumulating
            }
            Some(current) => {
                // Out-of-order check: new bucket < current bucket
                if bucket < *current {
                    return Err(StreamError::Expression(
                        crate::execution::types::ExpressionError::TypeMismatch
                    ));
                    // TODO: return a more specific error message
                }
                // New bucket — finalize current and start new
                self.flush_current_group()?;
                self.current_key = Some(bucket);
                self.current_state = Some(GroupState::new(&self.aggregate_defs));
            }
        }

        // Accumulate this row into the current group
        let state = self.current_state.as_mut().unwrap();
        for (agg_idx, agg_def) in self.aggregate_defs.iter().enumerate() {
            match &agg_def.extraction {
                crate::execution::types::ExtractionStrategy::None => {
                    // COUNT(*)
                    state.accumulators[agg_idx].accumulate_row()
                        .map_err(StreamError::Aggregate)?;
                }
                crate::execution::types::ExtractionStrategy::Expression(expr) => {
                    // Build row variables from batch columns
                    let mut row_vars = linked_hash_map::LinkedHashMap::with_capacity(batch.columns.len());
                    for (col_idx, col) in batch.columns.iter().enumerate() {
                        let value = BatchToRowAdapter::extract_value(col, row_idx);
                        row_vars.insert(batch.names[col_idx].clone(), value);
                    }
                    let merged_vars;
                    let variables = if self.variables.is_empty() {
                        &row_vars
                    } else {
                        merged_vars = crate::common::types::merge(&self.variables, &row_vars);
                        &merged_vars
                    };
                    let val = expr.expression_value(variables, &self.registry)
                        .map_err(StreamError::Expression)?;
                    state.accumulators[agg_idx].accumulate(&val)
                        .map_err(StreamError::Aggregate)?;
                }
            }
        }
    }
    Ok(())
}
```

### 7d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_streaming_groupby_single_batch_two_buckets -- --nocapture 2>&1 | tail -20
```

### 7e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add src/execution/batch_streaming_groupby.rs && git commit -m "Implement process_batch core streaming aggregation logic"
```

---

## Step 8: Implement `BatchStream` trait for `BatchStreamingGroupByOperator`

**File**: `src/execution/batch_streaming_groupby.rs`

### 8a. Write failing test

```rust
#[test]
fn test_streaming_groupby_cross_batch_bucket() {
    use chrono::TimeZone;
    let registry = Arc::new(FunctionRegistry::new());
    let variables = LinkedHashMap::new();
    let utc = chrono::FixedOffset::east_opt(0).unwrap();

    // Batch 1: two rows in 10:00 bucket
    let batch1 = ColumnBatch {
        columns: vec![build_mixed_column(vec![
            Value::DateTime(utc.with_ymd_and_hms(2026, 4, 7, 10, 1, 0).unwrap()),
            Value::DateTime(utc.with_ymd_and_hms(2026, 4, 7, 10, 3, 0).unwrap()),
        ])],
        names: vec!["timestamp".to_string()],
        selection: SelectionVector::All,
        len: 2,
    };
    // Batch 2: one more row in 10:00 bucket, then one in 10:05
    let batch2 = ColumnBatch {
        columns: vec![build_mixed_column(vec![
            Value::DateTime(utc.with_ymd_and_hms(2026, 4, 7, 10, 4, 0).unwrap()),
            Value::DateTime(utc.with_ymd_and_hms(2026, 4, 7, 10, 6, 0).unwrap()),
        ])],
        names: vec!["timestamp".to_string()],
        selection: SelectionVector::All,
        len: 2,
    };

    let schema = BatchSchema { names: vec!["timestamp".into()], types: vec![ColumnType::Mixed] };
    let child = MultiBatchStream { batches: VecDeque::from(vec![batch1, batch2]), schema };

    let mut op = BatchStreamingGroupByOperator::new(
        Box::new(child),
        "timestamp".to_string(),
        "5 minutes".to_string(),
        "bucket".to_string(),
        vec![NamedAggregate::new(
            Aggregate::Count(CountAggregate::new(), Named::Star),
            Some("cnt".to_string()),
        )],
        variables,
        registry,
    );

    // Collect all output
    let mut all_counts = Vec::new();
    loop {
        match op.next_batch().unwrap() {
            Some(b) => {
                for i in 0..b.len {
                    all_counts.push(BatchToRowAdapter::extract_value(&b.columns[1], i));
                }
            }
            None => break,
        }
    }

    // 10:00 bucket should have 3 rows (spanning both input batches)
    // 10:05 bucket should have 1 row
    assert_eq!(all_counts.len(), 2);
    assert_eq!(all_counts[0], Value::Int(3));
    assert_eq!(all_counts[1], Value::Int(1));
}
```

### 8b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_streaming_groupby_cross_batch_bucket -- --nocapture 2>&1 | tail -20
```

### 8c. Write implementation

Add the `BatchStream` impl:

```rust
impl BatchStream for BatchStreamingGroupByOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        // If we have enough completed groups, emit them
        if self.completed_keys.len() >= OUTPUT_BATCH_SIZE {
            return Ok(Some(self.emit_output_batch()?));
        }

        // Process input batches until we have enough output or input is exhausted
        if !self.input_exhausted {
            loop {
                match self.input.next_batch()? {
                    Some(batch) => {
                        self.process_batch(&batch)?;
                        if self.completed_keys.len() >= OUTPUT_BATCH_SIZE {
                            return Ok(Some(self.emit_output_batch()?));
                        }
                    }
                    None => {
                        self.input_exhausted = true;
                        // Finalize the last group
                        self.flush_current_group()?;
                        break;
                    }
                }
            }
        }

        // Emit any remaining completed groups
        if !self.completed_keys.is_empty() {
            return Ok(Some(self.emit_output_batch()?));
        }

        Ok(None)
    }

    fn schema(&self) -> &BatchSchema {
        &self.output_schema
    }

    fn close(&self) {
        self.input.close();
    }
}
```

### 8d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_streaming_groupby_cross_batch -- --nocapture 2>&1 | tail -20
```

### 8e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add src/execution/batch_streaming_groupby.rs && git commit -m "Implement BatchStream trait for streaming GroupBy operator"
```

---

## Step 9: Add streaming GroupBy detection in planner

**File**: `src/execution/types.rs`

### 9a. Write failing test

Test that detection correctly identifies a qualifying plan tree (integration test via `app::run_to_vec` or similar). Add to an existing test file or create one:

```rust
#[test]
fn test_detect_streaming_groupby_pattern() {
    // Construct: GroupBy([bucket], [COUNT(*)], Map([time_bucket('5 minutes', ts) AS bucket], DataSource(elb)))
    use crate::syntax::ast::{PathExpr, PathSegment};
    use crate::execution::types::*;
    use crate::common::types::Value;

    let ts_path = PathExpr::new(vec![PathSegment::AttrName("timestamp".to_string())]);
    let time_bucket_expr = Expression::Function(
        "time_bucket".to_string(),
        vec![
            Named::Expression(Expression::Constant(Value::String("5 minutes".to_string())), None),
            Named::Expression(Expression::Variable(ts_path), None),
        ],
    );
    let map_named = vec![
        Named::Expression(time_bucket_expr, Some("bucket".to_string())),
    ];

    let ds = Node::DataSource(
        DataSource::File(std::path::PathBuf::from("test.log"), "elb".to_string(), None),
        vec![crate::common::types::Binding { name: "it".to_string(), alias: None }],
    );
    let map_node = Node::Map(map_named, Box::new(ds));
    let group_key = PathExpr::new(vec![PathSegment::AttrName("bucket".to_string())]);

    let result = Node::detect_streaming_groupby(&[group_key], &map_node);
    assert!(result.is_some());
    let params = result.unwrap();
    assert_eq!(params.timestamp_column, "timestamp");
    assert_eq!(params.bucket_interval, "5 minutes");
    assert_eq!(params.bucket_alias, "bucket");
}

#[test]
fn test_detect_streaming_groupby_rejects_multiple_keys() {
    use crate::syntax::ast::{PathExpr, PathSegment};
    use crate::execution::types::*;
    use crate::common::types::Value;

    let key1 = PathExpr::new(vec![PathSegment::AttrName("bucket".to_string())]);
    let key2 = PathExpr::new(vec![PathSegment::AttrName("status".to_string())]);

    let ds = Node::DataSource(
        DataSource::File(std::path::PathBuf::from("test.log"), "elb".to_string(), None),
        vec![],
    );
    let map_node = Node::Map(vec![], Box::new(ds));

    let result = Node::detect_streaming_groupby(&[key1, key2], &map_node);
    assert!(result.is_none());
}

#[test]
fn test_detect_streaming_groupby_rejects_jsonl() {
    use crate::syntax::ast::{PathExpr, PathSegment};
    use crate::execution::types::*;
    use crate::common::types::Value;

    let ts_path = PathExpr::new(vec![PathSegment::AttrName("timestamp".to_string())]);
    let time_bucket_expr = Expression::Function(
        "time_bucket".to_string(),
        vec![
            Named::Expression(Expression::Constant(Value::String("5 minutes".to_string())), None),
            Named::Expression(Expression::Variable(ts_path), None),
        ],
    );
    let map_named = vec![
        Named::Expression(time_bucket_expr, Some("bucket".to_string())),
    ];

    let ds = Node::DataSource(
        DataSource::File(std::path::PathBuf::from("test.json"), "jsonl".to_string(), None),
        vec![],
    );
    let map_node = Node::Map(map_named, Box::new(ds));
    let group_key = PathExpr::new(vec![PathSegment::AttrName("bucket".to_string())]);

    let result = Node::detect_streaming_groupby(&[group_key], &map_node);
    assert!(result.is_none());
}
```

### 9b. Run test to verify it fails

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_detect_streaming_groupby -- --nocapture 2>&1 | tail -20
```

### 9c. Write implementation

Add to `impl Node` in `src/execution/types.rs`:

```rust
/// Parameters extracted when a streaming GroupBy pattern is detected.
pub(crate) struct StreamingGroupByParams<'a> {
    pub timestamp_column: String,
    pub bucket_interval: String,
    pub bucket_alias: String,
    pub map_source: &'a Node,
}

/// Detect if a GroupBy node qualifies for streaming aggregation.
///
/// Requirements:
/// 1. Exactly one GROUP BY key
/// 2. Child is a Map node containing a time_bucket() function call
///    that aliases to the group key name
/// 3. Underlying data source is time-ordered (ELB or ALB)
pub(crate) fn detect_streaming_groupby<'a>(
    keys: &[PathExpr],
    source: &'a Node,
) -> Option<StreamingGroupByParams<'a>> {
    // Must have exactly one GROUP BY key
    if keys.len() != 1 {
        return None;
    }
    let key_name = match keys[0].path_segments.first()? {
        PathSegment::AttrName(name) => name.clone(),
        _ => return None,
    };

    // Child must be a Map node
    let (named_list, map_source) = match source {
        Node::Map(named_list, map_source) => (named_list, map_source.as_ref()),
        _ => return None,
    };

    // Find the time_bucket() call in the Map's named list that aliases to key_name
    for named in named_list {
        if let Named::Expression(
            Expression::Function(func_name, args),
            Some(alias),
        ) = named {
            if func_name == "time_bucket" && alias == &key_name && args.len() == 2 {
                // Extract interval string from first argument
                let interval = match &args[0] {
                    Named::Expression(Expression::Constant(Value::String(s)), _) => s.clone(),
                    _ => return None,
                };
                // Extract timestamp column name from second argument
                let ts_col = match &args[1] {
                    Named::Expression(Expression::Variable(path), _) => {
                        match path.path_segments.last()? {
                            PathSegment::AttrName(name) => name.clone(),
                            _ => return None,
                        }
                    }
                    _ => return None,
                };
                // Verify the underlying source is time-ordered
                if !Self::source_is_time_ordered(map_source) {
                    return None;
                }
                return Some(StreamingGroupByParams {
                    timestamp_column: ts_col,
                    bucket_interval: interval,
                    bucket_alias: key_name,
                    map_source,
                });
            }
        }
    }
    None
}

/// Check if the given node subtree has a time-ordered data source.
fn source_is_time_ordered(node: &Node) -> bool {
    match node {
        Node::DataSource(DataSource::File(_, format, _), _) => {
            crate::execution::datasource::is_time_ordered(format).is_some()
        }
        Node::Filter(child, _) => Self::source_is_time_ordered(child),
        _ => false,
    }
}
```

### 9d. Run test to verify it passes

```bash
cd /Users/paulmeng/Develop/logq && cargo test test_detect_streaming_groupby -- --nocapture 2>&1 | tail -20
```

### 9e. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add src/execution/types.rs && git commit -m "Add detect_streaming_groupby planner detection logic"
```

---

## Step 10: Wire streaming GroupBy into `try_get_batch`

**File**: `src/execution/types.rs`

### 10a. No separate test — this is a wiring change tested by the integration test in Step 11.

### 10b. Write implementation

Replace the `Node::GroupBy` match arm in `try_get_batch()` (lines 769-784):

```rust
Node::GroupBy(keys, aggregates, source) => {
    // Try streaming aggregation for time-bucketed queries on ordered sources
    if let Some(params) = Self::detect_streaming_groupby(keys, source) {
        // Bypass the Map node: get batch stream from the Map's child source
        let required = params.map_source.compute_required_fields_for_batch();
        match params.map_source.try_get_batch(variables, registry, &required, threads) {
            Some(Ok(batch_stream)) => {
                let op = crate::execution::batch_streaming_groupby::BatchStreamingGroupByOperator::new(
                    batch_stream,
                    params.timestamp_column,
                    params.bucket_interval,
                    params.bucket_alias,
                    aggregates.clone(),
                    variables.clone(),
                    registry.clone(),
                );
                return Some(Ok(Box::new(op) as Box<dyn BatchStream>));
            }
            Some(Err(e)) => return Some(Err(e)),
            None => {} // Fall through to hash-based path
        }
    }
    // Hash-based fallback
    match source.try_get_batch(variables, registry, required_fields, threads) {
        Some(Ok(batch_stream)) => {
            let groupby = crate::execution::batch_groupby::BatchGroupByOperator::new(
                batch_stream,
                keys.clone(),
                aggregates.clone(),
                variables.clone(),
                registry.clone(),
            );
            Some(Ok(Box::new(groupby) as Box<dyn BatchStream>))
        }
        Some(Err(e)) => Some(Err(e)),
        None => None,
    }
}
```

### 10c. Run all tests to verify nothing breaks

```bash
cd /Users/paulmeng/Develop/logq && cargo test 2>&1 | tail -20
```

### 10d. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add src/execution/types.rs && git commit -m "Wire streaming GroupBy into try_get_batch planner routing"
```

---

## Step 11: End-to-end integration test

**File**: `tests/` or inline in `src/execution/batch_streaming_groupby.rs`

### 11a. Write integration test

Add to the test module in `src/execution/batch_streaming_groupby.rs`:

```rust
#[test]
fn test_streaming_groupby_empty_input() {
    let registry = Arc::new(FunctionRegistry::new());
    let variables = LinkedHashMap::new();
    let schema = BatchSchema { names: vec!["timestamp".into()], types: vec![ColumnType::Mixed] };
    let child = MultiBatchStream { batches: VecDeque::new(), schema };

    let mut op = BatchStreamingGroupByOperator::new(
        Box::new(child),
        "timestamp".to_string(),
        "5 minutes".to_string(),
        "bucket".to_string(),
        vec![NamedAggregate::new(
            Aggregate::Count(CountAggregate::new(), Named::Star),
            Some("cnt".to_string()),
        )],
        variables,
        registry,
    );

    // Empty input → no output
    assert!(op.next_batch().unwrap().is_none());
}

#[test]
fn test_streaming_groupby_single_bucket() {
    use chrono::TimeZone;
    let registry = Arc::new(FunctionRegistry::new());
    let variables = LinkedHashMap::new();
    let utc = chrono::FixedOffset::east_opt(0).unwrap();

    // All rows in same 5-minute bucket
    let batch = ColumnBatch {
        columns: vec![build_mixed_column(vec![
            Value::DateTime(utc.with_ymd_and_hms(2026, 4, 7, 10, 1, 0).unwrap()),
            Value::DateTime(utc.with_ymd_and_hms(2026, 4, 7, 10, 2, 0).unwrap()),
            Value::DateTime(utc.with_ymd_and_hms(2026, 4, 7, 10, 4, 0).unwrap()),
        ])],
        names: vec!["timestamp".to_string()],
        selection: SelectionVector::All,
        len: 3,
    };

    let schema = BatchSchema { names: vec!["timestamp".into()], types: vec![ColumnType::Mixed] };
    let child = MultiBatchStream { batches: VecDeque::from(vec![batch]), schema };

    let mut op = BatchStreamingGroupByOperator::new(
        Box::new(child),
        "timestamp".to_string(),
        "5 minutes".to_string(),
        "bucket".to_string(),
        vec![NamedAggregate::new(
            Aggregate::Count(CountAggregate::new(), Named::Star),
            Some("cnt".to_string()),
        )],
        variables,
        registry,
    );

    let result = op.next_batch().unwrap().unwrap();
    assert_eq!(result.len, 1);
    assert_eq!(BatchToRowAdapter::extract_value(&result.columns[1], 0), Value::Int(3));

    assert!(op.next_batch().unwrap().is_none());
}

#[test]
fn test_streaming_groupby_with_sum_aggregate() {
    use chrono::TimeZone;
    use ordered_float::OrderedFloat;
    let registry = Arc::new(FunctionRegistry::new());
    let variables = LinkedHashMap::new();
    let utc = chrono::FixedOffset::east_opt(0).unwrap();

    // Two timestamps in bucket 10:00, two in 10:05
    // With a "bytes" column: [100, 200, 300, 400]
    let ts_col = build_mixed_column(vec![
        Value::DateTime(utc.with_ymd_and_hms(2026, 4, 7, 10, 1, 0).unwrap()),
        Value::DateTime(utc.with_ymd_and_hms(2026, 4, 7, 10, 3, 0).unwrap()),
        Value::DateTime(utc.with_ymd_and_hms(2026, 4, 7, 10, 6, 0).unwrap()),
        Value::DateTime(utc.with_ymd_and_hms(2026, 4, 7, 10, 8, 0).unwrap()),
    ]);
    let bytes_col = build_mixed_column(vec![
        Value::Int(100), Value::Int(200), Value::Int(300), Value::Int(400),
    ]);
    let batch = ColumnBatch {
        columns: vec![ts_col, bytes_col],
        names: vec!["timestamp".to_string(), "bytes".to_string()],
        selection: SelectionVector::All,
        len: 4,
    };

    let schema = BatchSchema {
        names: vec!["timestamp".into(), "bytes".into()],
        types: vec![ColumnType::Mixed, ColumnType::Mixed],
    };
    let child = MultiBatchStream { batches: VecDeque::from(vec![batch]), schema };

    let bytes_path = crate::syntax::ast::PathExpr::new(vec![
        crate::syntax::ast::PathSegment::AttrName("bytes".to_string()),
    ]);
    let sum_agg = NamedAggregate::new(
        Aggregate::Sum(
            crate::execution::types::SumAggregate::new(),
            Named::Expression(
                crate::execution::types::Expression::Variable(bytes_path),
                None,
            ),
        ),
        Some("total".to_string()),
    );

    let mut op = BatchStreamingGroupByOperator::new(
        Box::new(child),
        "timestamp".to_string(),
        "5 minutes".to_string(),
        "bucket".to_string(),
        vec![
            NamedAggregate::new(
                Aggregate::Count(CountAggregate::new(), Named::Star),
                Some("cnt".to_string()),
            ),
            sum_agg,
        ],
        variables,
        registry,
    );

    // Collect all output
    let mut rows: Vec<(Value, Value, Value)> = Vec::new();
    loop {
        match op.next_batch().unwrap() {
            Some(b) => {
                for i in 0..b.len {
                    rows.push((
                        BatchToRowAdapter::extract_value(&b.columns[0], i),
                        BatchToRowAdapter::extract_value(&b.columns[1], i),
                        BatchToRowAdapter::extract_value(&b.columns[2], i),
                    ));
                }
            }
            None => break,
        }
    }

    assert_eq!(rows.len(), 2);
    // Bucket 10:00: count=2, sum=300
    assert_eq!(rows[0].1, Value::Int(2));
    assert_eq!(rows[0].2, Value::Float(OrderedFloat(300.0f32)));
    // Bucket 10:05: count=2, sum=700
    assert_eq!(rows[1].1, Value::Int(2));
    assert_eq!(rows[1].2, Value::Float(OrderedFloat(700.0f32)));
}
```

### 11b. Run all tests

```bash
cd /Users/paulmeng/Develop/logq && cargo test 2>&1 | tail -30
```

### 11c. Commit

```bash
cd /Users/paulmeng/Develop/logq && git add src/execution/batch_streaming_groupby.rs && git commit -m "Add comprehensive integration tests for streaming GroupBy"
```

---

## Step 12: Full regression test

### 12a. Run the complete test suite

```bash
cd /Users/paulmeng/Develop/logq && cargo test 2>&1 | tail -30
```

### 12b. Run benchmarks (if available)

```bash
cd /Users/paulmeng/Develop/logq && cargo bench -- groupby 2>&1 | tail -30
```

---

## Task Dependencies

| Group | Steps | Can Parallelize | Files Touched |
|-------|-------|-----------------|---------------|
| 1 | Steps 1-3 | Yes (independent files/methods) | `types.rs`, `datasource.rs` |
| 2 | Step 4 | No (depends on Group 1 types) | `batch_streaming_groupby.rs` (new), `mod.rs` |
| 3 | Steps 5-6 | Yes (independent helpers) | `batch_streaming_groupby.rs` |
| 4 | Steps 7-8 | No (7 depends on 5-6, 8 depends on 7) | `batch_streaming_groupby.rs` |
| 5 | Steps 9-10 | No (10 depends on 9) | `types.rs` |
| 6 | Steps 11-12 | No (integration + regression) | `batch_streaming_groupby.rs` |
