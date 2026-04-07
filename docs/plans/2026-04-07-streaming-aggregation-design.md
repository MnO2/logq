# Streaming Aggregation for Time-Bucketed Queries

**Date:** 2026-04-07
**Status:** Draft
**Goal:** Optimize GROUP BY time_bucket() queries on time-ordered log data by eliminating hash table overhead.

## Problem

logq's `GroupByStream` and `BatchGroupByOperator` both use a `HashMap<Option<Tuple>, GroupState>` that materializes all input before emitting results. For time-bucketed queries on time-ordered logs (the most common query pattern), this is wasteful — groups are encountered sequentially and never revisited.

Typical query:
```sql
SELECT time_bucket('5 minutes', timestamp) AS bucket, COUNT(*), SUM(received_bytes)
FROM elb_logs
WHERE timestamp > '2026-04-01T00:00:00Z'
GROUP BY time_bucket('5 minutes', timestamp)
```

## Design

### Layer 1: Streaming Aggregation (BatchStreamingGroupByOperator)

**Core concept:** Maintain a single `GroupState` for the current bucket. On each input row, if the key matches the current bucket, accumulate. If it differs, the current bucket is complete — finalize and emit it, then start a new bucket. Memory is O(1) per group instead of O(groups).

**Struct:**

```rust
pub struct BatchStreamingGroupByOperator {
    input: Box<dyn BatchStream>,
    group_expr: Expression,
    aggregates: Vec<AggregateDef>,
    current_key: Option<Value>,
    current_state: Vec<AccumulatorState>,
    pending_output: VecDeque<ColumnBatch>,
}
```

**Processing a batch:**

1. Evaluate `group_expr` on the batch to produce a column of bucket values.
2. Walk the bucket column linearly, finding boundary indices where the value changes (data is time-ordered, so boundaries are monotonic).
3. For each contiguous run of identical bucket values within the batch:
   - If the bucket matches `current_key`: accumulate the slice into `current_state`.
   - If it differs: finalize `current_state` into a single-row output batch, push to `pending_output`, reset with new key.
4. Cross-batch continuity: `current_key` and `current_state` persist across `next_batch()` calls. A bucket can span multiple batches.

**Boundary detection** uses logq's existing SIMD kernels — compare each element to the next and find positions where they differ.

**Output format:** Each completed group becomes a single-row `ColumnBatch` with the group key + finalized aggregate values.

### Layer 2: Aggregation Pushdown Into Scanner (AggregatingBatchScanOperator)

For simple queries where all SELECT expressions are the group key or simple aggregates (COUNT, SUM, MIN, MAX, AVG) with direct column reference arguments, collapse the entire pipeline `BatchScan -> BatchFilter -> BatchStreamingGroupBy` into a single fused operator.

**Activation conditions (all must hold):**
- Streaming GroupBy is selected (time-ordered source + time_bucket)
- All SELECT expressions are either the group key or simple aggregates
- Aggregate arguments are direct column references (not computed expressions)
- The filter (if any) involves only columns available during scan

**How it works:**

1. Read and tokenize each line.
2. Parse only the needed fields (timestamp + filter columns + aggregate columns).
3. Evaluate the filter — skip non-matching rows immediately.
4. Compute the time bucket from the timestamp.
5. Accumulate into the current bucket's state (no ColumnBatch created).
6. On bucket boundary, emit a single-row result batch.

**Benefit:** Eliminates all intermediate memory allocation — no column vectors, no selection vectors, no batch-to-batch handoff.

### Planner Integration & Detection

The decision happens in `Node::try_get_batch()` in `src/execution/types.rs`:

1. Check if any group-by expression is a `time_bucket()` call.
2. Check if the input source is a known-ordered log format (ELB, ALB, S3, Squid all have timestamp as an early field).
3. If both: use `BatchStreamingGroupByOperator` (or `AggregatingBatchScanOperator` if pushdown conditions are met).
4. Otherwise: fall through to existing `BatchGroupByOperator`.

**Known-ordered sources:** Add an `is_time_ordered() -> Option<&str>` method to datasource metadata that returns the timestamp column name if the format guarantees chronological order. ELB/ALB logs are written in arrival order. JSONL returns `None`.

**Graceful fallback:** If the streaming operator encounters an out-of-order row (bucket key goes backwards), fall back to collecting remaining rows into a HashMap. Log a warning.

**EXPLAIN output:** Indicate `StreamingGroupBy` vs `HashGroupBy` so users can see which path was chosen.

## File Changes

| File | Change |
|------|--------|
| `src/execution/batch_streaming_groupby.rs` | **New.** `BatchStreamingGroupByOperator` |
| `src/execution/batch_aggregating_scan.rs` | **New.** `AggregatingBatchScanOperator` |
| `src/execution/types.rs` | Modify `try_get_batch()` for detection and routing |
| `src/execution/datasource.rs` | Add ordering metadata to log format definitions |
| `src/execution/mod.rs` | Register new modules |
| `src/execution/batch.rs` | Helper to build single-row output batches |

## What Stays Unchanged

- All existing row-based and batch GroupBy code
- Parser, logical planner, optimizer
- SIMD kernels, existing batch operators

## Testing Strategy

- Unit tests: streaming groupby on synthetic ordered batches, boundary-spanning buckets, single-bucket input, empty input
- Correctness: compare streaming vs hash GroupBy results on same data
- Fallback: verify out-of-order detection triggers HashMap fallback
- Benchmarks: time-bucket queries on ELB files comparing before/after
