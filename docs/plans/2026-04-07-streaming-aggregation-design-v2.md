# Streaming Aggregation for Time-Bucketed Queries (v2)

**Date:** 2026-04-07
**Status:** Draft (revised)
**Goal:** Optimize GROUP BY time_bucket() queries on time-ordered log data by eliminating hash table overhead, inspired by Velox's pre-grouped key aggregation.

## Changes from Previous Version

This revision addresses all critical issues and incorporates suggestions from the first design review.

| Review Item | Change |
|-------------|--------|
| **C1** (S3/Squid not time-ordered) | Removed S3 and Squid from ordered sources. Only ELB and ALB are marked as time-ordered. Documented the single-file assumption explicitly. |
| **C2** (Parallel scanning conflict) | Added two-phase parallel approach: each chunk performs its own streaming aggregation, then a merge step combines the last bucket of chunk N with the first bucket of chunk N+1 when they share the same key. Added `AccumulatorState::merge` method. |
| **C3** (HAVING unaddressed) | Documented that HAVING works transparently with Layer 1 because it emits standard `ColumnBatch` output and HAVING is compiled as `Node::Filter` above `Node::GroupBy`. Layer 2 is deferred (see S2). |
| **C4** (No SIMD boundary kernels) | Dropped the claim about existing SIMD kernels. Boundary detection uses a simple scalar loop comparing each element to its predecessor. |
| **C5** (Struct signature mismatch) | Constructor now accepts `Vec<NamedAggregate>` and `Vec<PathExpr>` matching the existing `BatchGroupByOperator::new` interface. Internal conversion to `AggregateDef` happens inside the constructor. |
| **S1** (Reuse GroupState) | The streaming operator maintains a single `GroupState` instance (the existing struct wrapping `Vec<AccumulatorState>`) as its current state. |
| **S2** (Defer Layer 2) | Layer 2 (`AggregatingBatchScanOperator`) is deferred to a follow-up. This design focuses exclusively on Layer 1 (`BatchStreamingGroupByOperator`). |
| **S3** (Out-of-order fallback) | Replaced mid-stream HashMap fallback with an error/warning. On detecting out-of-order data, the operator returns an error advising the user to re-run without streaming aggregation or fix their data. |
| **S4** (Single-row output batches) | Completed groups are buffered into multi-row output batches (up to 64 groups per batch) to amortize allocation overhead. |
| **S5** (Multiple GROUP BY keys) | Streaming aggregation only activates when GROUP BY has exactly one key that is a `time_bucket()` call. Multiple GROUP BY keys use the existing hash-based path. |
| **S7** (time_bucket limitations) | Documented that `time_bucket()` only supports seconds, minutes, and hours. Day/month/year intervals fail at the function level before reaching this operator. |

## Problem

logq's `GroupByStream` and `BatchGroupByOperator` both use a `HashMap<Option<Tuple>, GroupState>` that materializes all input before emitting results. For time-bucketed queries on time-ordered logs (the most common query pattern), this is wasteful -- groups are encountered sequentially and never revisited.

Typical query:
```sql
SELECT time_bucket('5 minutes', timestamp) AS bucket, COUNT(*), SUM(received_bytes)
FROM elb_logs
WHERE timestamp > '2026-04-01T00:00:00Z'
GROUP BY time_bucket('5 minutes', timestamp)
```

## Design

### Layer 1: Streaming Aggregation (BatchStreamingGroupByOperator)

**Core concept:** Maintain a single `GroupState` for the current bucket. On each input row, if the key matches the current bucket, accumulate. If it differs, the current bucket is complete -- buffer the finalized result and start a new bucket. Memory is O(1) per active group instead of O(groups).

**Struct:**

```rust
pub struct BatchStreamingGroupByOperator {
    /// Child batch stream providing ordered input.
    input: Box<dyn BatchStream>,
    /// GROUP BY key expressions (from Node::GroupBy). Must be exactly one
    /// PathExpr that resolves to a time_bucket() call.
    group_keys: Vec<PathExpr>,
    /// Aggregate definitions, converted from Vec<NamedAggregate>.
    aggregate_defs: Vec<AggregateDef>,
    /// The current bucket key being accumulated.
    current_key: Option<Value>,
    /// The current group's accumulator state. Reuses the existing GroupState
    /// struct which wraps Vec<AccumulatorState>.
    current_state: Option<GroupState>,
    /// Buffer of completed groups waiting to be emitted as a multi-row batch.
    /// Groups are buffered until OUTPUT_BATCH_SIZE is reached or input is
    /// exhausted.
    completed_keys: Vec<Value>,
    completed_states: Vec<Vec<Value>>,
    /// Whether the input stream has been fully consumed.
    input_exhausted: bool,
    /// Variables and registry for expression evaluation.
    variables: Variables,
    registry: Arc<FunctionRegistry>,
}

/// Maximum number of completed groups to buffer before emitting a batch.
const OUTPUT_BATCH_SIZE: usize = 64;
```

**Constructor:**

```rust
impl BatchStreamingGroupByOperator {
    pub fn new(
        input: Box<dyn BatchStream>,
        group_keys: Vec<PathExpr>,
        aggregates: Vec<NamedAggregate>,
        variables: Variables,
        registry: Arc<FunctionRegistry>,
    ) -> Self {
        let aggregate_defs: Vec<AggregateDef> = aggregates
            .iter()
            .map(AggregateDef::from_named_aggregate)
            .collect();
        Self {
            input,
            group_keys,
            aggregate_defs,
            current_key: None,
            current_state: None,
            completed_keys: Vec::new(),
            completed_states: Vec::new(),
            input_exhausted: false,
            variables,
            registry,
        }
    }
}
```

The constructor accepts `Vec<PathExpr>` and `Vec<NamedAggregate>` -- the same types as `BatchGroupByOperator::new` -- so the call site in `try_get_batch` can use either operator interchangeably. The conversion to `Vec<AggregateDef>` happens internally.

**Processing a batch:**

1. Evaluate the `time_bucket()` group expression on the batch to produce a column of bucket values.
2. Walk the bucket column linearly, finding boundary indices where the value changes.
3. For each contiguous run of identical bucket values within the batch:
   - If the bucket matches `current_key`: accumulate the slice into `current_state` using the existing `AccumulatorState::accumulate` / `accumulate_row` methods.
   - If it differs: finalize `current_state`, push the key and finalized values into the completed-groups buffer, then create a fresh `GroupState::new(&self.aggregate_defs)` for the new key.
4. Cross-batch continuity: `current_key` and `current_state` persist across `next_batch()` calls. A bucket can span multiple input batches.
5. When the completed-groups buffer reaches `OUTPUT_BATCH_SIZE` (64), build and return a multi-row `ColumnBatch`.
6. When the input stream is exhausted, finalize the last `current_state` (if any), add it to the buffer, and emit the remaining groups as a final batch.

**Boundary detection:** A simple scalar loop comparing each element to its predecessor. For each index `i` in `1..len`, compare `bucket_values[i] != bucket_values[i-1]`. Since `Value` derives `PartialEq` and `Eq`, this works directly for `Value::DateTime` values returned by `time_bucket()`. This is O(n) per batch and fast enough -- the comparison is a single `DateTime` equality check per row.

```rust
fn find_boundaries(bucket_column: &[Value]) -> Vec<usize> {
    let mut boundaries = Vec::new();
    for i in 1..bucket_column.len() {
        if bucket_column[i] != bucket_column[i - 1] {
            boundaries.push(i);
        }
    }
    boundaries
}
```

**Output format:** Completed groups are buffered into multi-row `ColumnBatch` output (up to 64 groups per batch). Each batch contains columns for the group key plus all finalized aggregate values. The batch uses the standard `ColumnBatch` format, so downstream operators (including HAVING filters) work transparently.

**Out-of-order detection:** If a new bucket key is strictly less than the previous bucket key (i.e., time goes backwards), the operator returns an error:

```
Error: Streaming aggregation detected out-of-order data at row N.
The input is not time-ordered as expected for this log format.
Hint: Re-run with --no-streaming-agg to use hash-based aggregation,
or ensure input contains a single ELB/ALB log file (not concatenated).
```

This is simpler and more correct than attempting a mid-stream fallback to a HashMap, which would produce duplicate groups for already-emitted buckets. The user can re-run the query with streaming aggregation disabled.

### Interaction with Parallel Scanning

The existing parallel scan path (`parallel_scan_chunks` in `src/execution/parallel.rs`) splits the input file into chunks at newline boundaries. Each chunk is independently time-ordered, but a single time bucket can span a chunk boundary. A naive approach would incorrectly finalize partial buckets at chunk splits.

**Solution: Two-phase parallel streaming aggregation.**

Phase 1 -- Per-chunk streaming aggregation: Each parallel chunk runs its own `BatchStreamingGroupByOperator` independently. Each chunk produces a sequence of fully aggregated groups, except that:
- The **last** group of chunk N may be incomplete (the bucket continues into chunk N+1).
- The **first** group of chunk N+1 may be incomplete (the bucket started in chunk N).

Phase 2 -- Boundary merge: After all chunks complete, a sequential merge step examines adjacent chunk results. For each pair of consecutive chunks (N, N+1):
- If the last key of chunk N equals the first key of chunk N+1, merge their `GroupState` accumulators using a new `AccumulatorState::merge` method, and remove the duplicate entry.
- If the keys differ, no merge is needed.

**AccumulatorState::merge method:**

```rust
impl AccumulatorState {
    /// Merge another accumulator's state into this one. Used to combine
    /// partial aggregations from adjacent parallel chunks.
    pub(crate) fn merge(&mut self, other: &AccumulatorState) {
        match (self, other) {
            (AccumulatorState::Count(a), AccumulatorState::Count(b)) => *a += b,
            (AccumulatorState::CountStar(a), AccumulatorState::CountStar(b)) => *a += b,
            (AccumulatorState::Sum(a), AccumulatorState::Sum(b)) => {
                match (a, b) {
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
                        Some(a_val) if b_val < a_val => *a = Some(b_val.clone()),
                        None => *a = Some(b_val.clone()),
                        _ => {}
                    }
                }
            }
            (AccumulatorState::Max(a), AccumulatorState::Max(b)) => {
                if let Some(b_val) = b {
                    match a {
                        Some(a_val) if b_val > a_val => *a = Some(b_val.clone()),
                        None => *a = Some(b_val.clone()),
                        _ => {}
                    }
                }
            }
            _ => {
                // For accumulators that don't support merge (GroupAs,
                // PercentileDisc, ApproxPercentile, etc.), this is a
                // programming error -- the planner should not select
                // parallel streaming for these aggregate types.
                panic!("unsupported merge for accumulator type");
            }
        }
    }
}
```

**GroupState merge:**

```rust
impl GroupState {
    /// Merge another GroupState into this one, element-wise.
    pub(crate) fn merge(&mut self, other: &GroupState) {
        assert_eq!(self.accumulators.len(), other.accumulators.len());
        for (a, b) in self.accumulators.iter_mut().zip(other.accumulators.iter()) {
            a.merge(b);
        }
    }
}
```

**Mergeable aggregate check:** The planner verifies that all aggregates in the query are mergeable (Count, CountStar, Sum, Avg, Min, Max) before enabling parallel streaming aggregation. If any aggregate is non-mergeable (GroupAs, PercentileDisc, ApproxPercentile, ApproxCountDistinct), parallel streaming is disabled -- either fall back to single-threaded streaming or use the existing hash-based parallel path.

### Interaction with HAVING

HAVING is compiled as a `Node::Filter` placed above `Node::GroupBy` in the logical plan:

```
Filter(having_predicate)
  GroupBy(keys, aggregates, source)
```

The `try_get_batch()` handler for `Node::Filter` wraps its child's batch stream with `BatchFilterOperator`. Since `BatchStreamingGroupByOperator` emits standard `ColumnBatch` output (multi-row batches with named columns for the group key and aggregates), the `BatchFilterOperator` applies the HAVING predicate to these output batches transparently. No special handling is required in Layer 1.

### Planner Integration & Detection

The decision happens in `Node::try_get_batch()` in `src/execution/types.rs`, within the `Node::GroupBy(keys, aggregates, source)` match arm:

**Activation conditions (all must hold):**

1. The GROUP BY has exactly one key expression.
2. That key expression resolves to a `time_bucket()` function call.
3. The input source is a known time-ordered log format (ELB or ALB only).
4. The query is operating on a single file (not concatenated input).

If all conditions are met, construct `BatchStreamingGroupByOperator` instead of `BatchGroupByOperator`. Otherwise, fall through to the existing hash-based `BatchGroupByOperator`.

```rust
Node::GroupBy(keys, aggregates, source) => {
    match source.try_get_batch(variables, registry, required_fields, threads) {
        Some(Ok(batch_stream)) => {
            if Self::can_use_streaming_groupby(keys, source) {
                let op = BatchStreamingGroupByOperator::new(
                    batch_stream,
                    keys.clone(),
                    aggregates.clone(),
                    variables.clone(),
                    registry.clone(),
                );
                Some(Ok(Box::new(op) as Box<dyn BatchStream>))
            } else {
                let groupby = BatchGroupByOperator::new(
                    batch_stream,
                    keys.clone(),
                    aggregates.clone(),
                    variables.clone(),
                    registry.clone(),
                );
                Some(Ok(Box::new(groupby) as Box<dyn BatchStream>))
            }
        }
        Some(Err(e)) => Some(Err(e)),
        None => None,
    }
}
```

**`can_use_streaming_groupby` helper:**

```rust
fn can_use_streaming_groupby(keys: &[PathExpr], source: &Node) -> bool {
    // S5: Only activate for exactly one GROUP BY key
    if keys.len() != 1 {
        return false;
    }
    // Check if the key is a time_bucket() call
    // (inspect the AST node for a Function("time_bucket", ...) pattern)
    if !Self::is_time_bucket_key(&keys[0]) {
        return false;
    }
    // Check if the source is a known time-ordered format
    if !Self::source_is_time_ordered(source) {
        return false;
    }
    true
}
```

**Known time-ordered sources:**

Only **ELB** (`ClassicLoadBalancerLogField`) and **ALB** (`ApplicationLoadBalancerLogField`) are marked as time-ordered. These formats have `DataType::DateTime` typed timestamp fields (ELB field 0, ALB field 1) and AWS guarantees that individual log files are written in arrival order.

**Important single-file assumption:** The time-ordering guarantee applies to individual log files as written by AWS. If a user concatenates multiple log files, the concatenated result is NOT guaranteed to be time-ordered across file boundaries. The ordering metadata applies per-file. If out-of-order data is detected at runtime, the operator reports an error (see "Out-of-order detection" above).

The following formats are explicitly NOT time-ordered and will always use hash-based aggregation:
- **S3 access logs**: AWS delivers these on a best-effort basis. Records within a file are not guaranteed to be chronologically ordered. Additionally, the `time` field is typed as `DataType::String` (not `DataType::DateTime`), so `time_bucket()` would fail on it regardless.
- **Squid logs**: The timestamp is a Unix epoch float stored as `DataType::String`. Whether logs are ordered depends on proxy configuration. The field type is incompatible with `time_bucket()`.
- **JSONL**: No ordering guarantees for arbitrary JSON data.

Add an `is_time_ordered() -> Option<&str>` method to datasource metadata:

```rust
impl LogFormat {
    /// Returns the timestamp column name if this log format guarantees
    /// chronological ordering within a single file.
    /// Only ELB and ALB individual log files are guaranteed time-ordered.
    pub fn is_time_ordered(&self) -> Option<&str> {
        match self {
            LogFormat::Elb => Some("timestamp"),
            LogFormat::Alb => Some("timestamp"),
            _ => None,
        }
    }
}
```

**`time_bucket` limitations:** The `time_bucket()` function (in `src/functions/datetime.rs`) only supports `Second`, `Minute`, and `Hour` intervals. It returns `Err(TimeIntervalNotSupported)` for `Day`, `Month`, and `Year`. Queries using unsupported intervals (e.g., `GROUP BY time_bucket('1 day', timestamp)`) will fail at the function evaluation level before reaching the streaming operator. This is not a limitation of the streaming design but should be noted for completeness.

**EXPLAIN output:** Indicate `StreamingGroupBy` vs `HashGroupBy` so users can see which path was chosen:

```
StreamingGroupBy [time_bucket('5 minutes', timestamp)] aggs=[COUNT(*), SUM(received_bytes)]
  BatchScan elb_logs [timestamp, received_bytes]
```

### Layer 2: Aggregation Pushdown (Deferred)

Layer 2 (`AggregatingBatchScanOperator`), which fuses scan + filter + groupby into a single operator, is deferred to a follow-up design. Rationale:

- Layer 1 already eliminates hash table overhead, which is the primary performance bottleneck.
- Layer 2 introduces significant complexity (new operator, new file, activation conditions).
- Layer 2 has a HAVING interaction issue -- the fused operator bypasses `Node::Filter` wrapping.
- It is better to measure the benefit of Layer 1 alone before adding Layer 2.

The follow-up design for Layer 2 must address: (a) disabling Layer 2 when HAVING is present, (b) interaction with parallel scanning, and (c) measuring the incremental benefit over Layer 1.

## File Changes

| File | Change |
|------|--------|
| `src/execution/batch_streaming_groupby.rs` | **New.** `BatchStreamingGroupByOperator` implementing `BatchStream`. |
| `src/execution/types.rs` | Modify `try_get_batch()` for `Node::GroupBy` to detect and route to streaming path. Add `can_use_streaming_groupby` helper. Add `AccumulatorState::merge` and `GroupState::merge` methods. |
| `src/execution/datasource.rs` | Add `is_time_ordered()` method to log format definitions (ELB and ALB only). |
| `src/execution/parallel.rs` | Add boundary-merge logic for combining partial `GroupState`s from adjacent parallel chunks. |
| `src/execution/mod.rs` | Register `batch_streaming_groupby` module. |
| `src/execution/batch.rs` | Helper to build multi-row output batches from completed group buffers. |

## What Stays Unchanged

- All existing row-based and batch GroupBy code
- Parser, logical planner, optimizer
- SIMD kernels, existing batch operators
- Layer 2 (`AggregatingBatchScanOperator`) -- deferred to follow-up

## Testing Strategy

- **Unit tests:** Streaming groupby on synthetic ordered batches, boundary-spanning buckets, single-bucket input, empty input, buckets that span multiple batches.
- **Multi-row output:** Verify that output batches contain up to 64 groups and the final batch contains any remainder.
- **Correctness:** Compare streaming vs hash GroupBy results on the same data to ensure identical output.
- **Out-of-order detection:** Verify that out-of-order input produces a clear error message rather than incorrect results.
- **Parallel merge:** Test that chunk boundary merging produces correct aggregates when a bucket spans a chunk split. Test cases where the last bucket of chunk N does and does not match the first bucket of chunk N+1.
- **HAVING:** Verify that HAVING filters applied above the streaming GroupBy work correctly (standard `BatchFilterOperator` on the output).
- **Activation guard:** Verify that multiple GROUP BY keys, non-time_bucket keys, and non-ELB/ALB sources all fall through to hash-based aggregation.
- **Benchmarks:** time-bucket queries on ELB files comparing streaming vs hash GroupBy performance.
