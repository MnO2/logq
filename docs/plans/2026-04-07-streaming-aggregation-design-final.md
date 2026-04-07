# Streaming Aggregation for Time-Bucketed Queries (Final)

**Date:** 2026-04-07
**Status:** Final
**Goal:** Optimize GROUP BY time_bucket() queries on time-ordered log data by eliminating hash table overhead, inspired by Velox's pre-grouped key aggregation.

## Changes from v2

This revision addresses all critical issues and incorporates suggestions from the second design review.

| Review Item | Change |
|-------------|--------|
| **C1** (batch path unreachable for time_bucket) | The streaming GroupBy operator now fuses the `time_bucket()` evaluation internally. Instead of relying on the upstream Map node to compute the bucket column, the operator receives the raw timestamp column from BatchScan and computes `time_bucket()` itself. The plan tree becomes: `BatchScan -> BatchFilter (optional) -> BatchStreamingGroupBy (with internal time_bucket eval)`, skipping the Map node entirely for the group key. The planner detects the pattern at the `Node::GroupBy` level, looks through the child Map node to find the `time_bucket()` function call expression, extracts the interval string and timestamp column name, and passes these directly to the streaming operator constructor. |
| **C2** (is_time_bucket_key impossible on PathExpr) | Detection now happens during physical plan creation in `Node::GroupBy`'s `try_get_batch()` handler, where the child Map node is accessible. The helper inspects the child `Node::Map`'s named list to find the expression associated with the group key alias, checks if it is an `Expression::Function("time_bucket", ...)`, and extracts the interval and timestamp column arguments. Detection no longer attempts to inspect the `PathExpr` itself. |
| **C3** (non-existent CLI flag) | Removed the reference to `--no-streaming-agg`. The error message now reads: "Error: input data is not time-ordered. The streaming aggregation optimization requires chronologically ordered input. Consider using the standard hash-based GROUP BY path." No CLI flag is needed since this is an automatic optimization with transparent fallback to the hash-based path for non-qualifying queries. |
| **S1** (First/Last are mergeable) | Added `First` and `Last` to the set of mergeable accumulators. For time-ordered parallel chunks, the correct `First` value is always from the earlier chunk (keep left), and the correct `Last` value is always from the later chunk (take right). |
| **S2** (ApproxCountDistinct/ApproxPercentile mergeable) | Added `ApproxCountDistinct` and `ApproxPercentile` to the mergeable set. `HyperLogLog` supports `merge()` natively, and `TDigest` supports `merge_digests()`. |
| **S3** (GroupAs/PercentileDisc merge is concatenation) | Added `GroupAs` and `PercentileDisc` to the mergeable set. Both accumulate values into a `Vec<Value>`, so merge is concatenation (`extend`). `PercentileDisc` sorts at finalization, so concatenation preserves correctness. |
| **S4** (OUTPUT_BATCH_SIZE tunability) | Documented the tradeoff. The constant remains at 64 but is defined as a named `const` that can be tuned based on benchmarks. |
| **S5** (cross-batch out-of-order uses `<` not `!=`) | Made the distinction between intra-batch boundary detection (`!=`, where a different key means a new bucket) and cross-batch out-of-order detection (`<`, where only a strictly earlier key is an error) explicit in the implementation guidance. |
| **S6** (time_bucket interval divisibility constraint) | Added note about the even-division constraint (`60 % n == 0` for seconds/minutes, `24 % n == 0` for hours). |
| **S7** (EXPLAIN shows reason for path choice) | EXPLAIN output now includes the reason when streaming is not chosen, e.g., `HashGroupBy [reason: multiple GROUP BY keys]`. |

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

**Key architectural decision: Fused time_bucket evaluation.** The streaming operator computes `time_bucket()` internally rather than relying on an upstream Map node. This is necessary because the Map node's `try_get_batch()` handler rejects function expressions (it only supports simple variable projections and `*`). The operator receives the raw timestamp column from the BatchScan output and applies the `time_bucket()` function itself, using the interval string and timestamp column name extracted by the planner during detection.

**Plan tree for a qualifying query:**

```
StreamingGroupBy [interval="5 minutes", ts_col="timestamp"] aggs=[COUNT(*), SUM(received_bytes)]
  BatchFilter(timestamp > '2026-04-01T00:00:00Z')  -- optional
    BatchScan elb_logs [timestamp, received_bytes]
```

The Map node that would normally compute `time_bucket()` is bypassed entirely. The streaming operator's child is the BatchScan (or BatchFilter wrapping BatchScan), providing raw timestamp values.

**Struct:**

```rust
pub struct BatchStreamingGroupByOperator {
    /// Child batch stream providing ordered input with raw timestamp column.
    input: Box<dyn BatchStream>,
    /// The name of the timestamp column in the input batches (e.g., "timestamp").
    timestamp_column: String,
    /// The bucket interval string (e.g., "5 minutes").
    bucket_interval: String,
    /// The output alias for the bucket column (e.g., "bucket" or "_1").
    bucket_alias: String,
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
    /// Variables and registry for expression evaluation (needed for aggregate
    /// accumulation, not for time_bucket which is computed directly).
    variables: Variables,
    registry: Arc<FunctionRegistry>,
}

/// Maximum number of completed groups to buffer before emitting a batch.
/// This value balances allocation amortization (larger = fewer allocations)
/// against streaming latency (smaller = earlier output to downstream
/// operators like HAVING filters). Tunable based on benchmarks.
const OUTPUT_BATCH_SIZE: usize = 64;
```

**Constructor:**

```rust
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
        Self {
            input,
            timestamp_column,
            bucket_interval,
            bucket_alias,
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

The constructor no longer accepts `Vec<PathExpr>` group keys (since the key is always a single `time_bucket()` call). Instead, it accepts the deconstructed parameters: `timestamp_column`, `bucket_interval`, and `bucket_alias`. This makes the operator's role explicit and avoids needing to re-parse the group key expression at runtime.

**Processing a batch:**

1. Extract the raw timestamp column from the input batch by name (`self.timestamp_column`).
2. Compute `time_bucket()` for each timestamp value using `self.bucket_interval` and the `FunctionRegistry`, producing a column of bucket values. This is done by calling `registry.call("time_bucket", &[Value::String(self.bucket_interval.clone()), ts_value])` for each row, or equivalently by inlining the bucketing arithmetic for performance.
3. Walk the bucket column linearly, finding boundary indices where the value changes.
4. For each contiguous run of identical bucket values within the batch:
   - If the bucket matches `current_key`: accumulate the slice into `current_state` using the existing `AccumulatorState::accumulate` / `accumulate_row` methods.
   - If it differs: finalize `current_state`, push the key and finalized values into the completed-groups buffer, then create a fresh `GroupState::new(&self.aggregate_defs)` for the new key.
5. Cross-batch continuity: `current_key` and `current_state` persist across `next_batch()` calls. A bucket can span multiple input batches.
6. When the completed-groups buffer reaches `OUTPUT_BATCH_SIZE` (64), build and return a multi-row `ColumnBatch`.
7. When the input stream is exhausted, finalize the last `current_state` (if any), add it to the buffer, and emit the remaining groups as a final batch.

**Boundary detection (intra-batch):** A simple scalar loop comparing each element to its predecessor. For each index `i` in `1..len`, compare `bucket_values[i] != bucket_values[i-1]`. The `!=` comparison is correct here because within a batch, a different bucket value always indicates a new group (the data is time-ordered, so a different value means the previous bucket is complete). Since `Value` derives `PartialEq` and `Eq`, this works directly for `Value::DateTime` values returned by `time_bucket()`. This is O(n) per batch and fast enough -- the comparison is a single `DateTime` equality check per row.

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

**Out-of-order detection (cross-batch):** When a new batch arrives, the first bucket value is compared against `current_key`. If the new bucket is strictly less than the current key (i.e., `new_key < current_key`), time has gone backwards across a batch boundary, indicating out-of-order data. The operator returns an error:

```
Error: input data is not time-ordered. The streaming aggregation optimization
requires chronologically ordered input. Consider using the standard hash-based
GROUP BY path.
```

The comparison uses `<` (strictly less than), not `!=`. An equal key across batch boundaries is normal and expected -- it means the current bucket spans multiple batches and accumulation continues. A greater key means a new bucket has started, which is also normal. Only a strictly lesser key indicates a time-ordering violation.

This is simpler and more correct than attempting a mid-stream fallback to a HashMap, which would produce duplicate groups for already-emitted buckets. Since streaming aggregation is an automatic optimization, the user does not need a CLI flag to disable it; non-qualifying queries (e.g., non-ELB/ALB sources, multiple GROUP BY keys) naturally use the hash-based path. If a user encounters this error on data they expected to be ordered, they should verify they are querying a single ELB/ALB log file and not a concatenated multi-file input. A query hint mechanism (e.g., `/*+ NO_STREAMING_AGG */`) can be added in the future if needed.

**Output format:** Completed groups are buffered into multi-row `ColumnBatch` output (up to 64 groups per batch). Each batch contains columns for the group key (using `bucket_alias` as the column name) plus all finalized aggregate values. The batch uses the standard `ColumnBatch` format, so downstream operators (including HAVING filters) work transparently.

### Interaction with Parallel Scanning

The existing parallel scan path (`parallel_scan_chunks` in `src/execution/parallel.rs`) splits the input file into chunks at newline boundaries. Each chunk is independently time-ordered, but a single time bucket can span a chunk boundary. A naive approach would incorrectly finalize partial buckets at chunk splits.

**Solution: Two-phase parallel streaming aggregation.**

Phase 1 -- Per-chunk streaming aggregation: Each parallel chunk runs its own `BatchStreamingGroupByOperator` independently. Each chunk produces a sequence of fully aggregated groups, except that:
- The **last** group of chunk N may be incomplete (the bucket continues into chunk N+1).
- The **first** group of chunk N+1 may be incomplete (the bucket started in chunk N).

Phase 2 -- Boundary merge: After all chunks complete, a sequential merge step examines adjacent chunk results. For each pair of consecutive chunks (N, N+1):
- If the last key of chunk N equals the first key of chunk N+1, merge their `GroupState` accumulators using the `AccumulatorState::merge` method, and remove the duplicate entry.
- If the keys differ, no merge is needed.

**AccumulatorState::merge method:**

```rust
impl AccumulatorState {
    /// Merge another accumulator's state into this one. Used to combine
    /// partial aggregations from adjacent parallel chunks.
    /// `self` is from the earlier chunk, `other` is from the later chunk.
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
            (AccumulatorState::First(_a), AccumulatorState::First(_b)) => {
                // Keep self (from the earlier chunk) -- it saw the first values
                // for this bucket. No-op.
            }
            (AccumulatorState::Last(a), AccumulatorState::Last(b)) => {
                // Take other (from the later chunk) -- it saw the last values
                // for this bucket.
                if b.is_some() {
                    *a = b.clone();
                }
            }
            (AccumulatorState::GroupAs(a_vals), AccumulatorState::GroupAs(b_vals)) => {
                // Concatenate: earlier chunk values followed by later chunk values.
                a_vals.extend(b_vals.iter().cloned());
            }
            (AccumulatorState::ApproxCountDistinct(a), AccumulatorState::ApproxCountDistinct(b)) => {
                a.merge(b);
            }
            (AccumulatorState::PercentileDisc { values: a_vals, .. },
             AccumulatorState::PercentileDisc { values: b_vals, .. }) => {
                // Concatenate values; sorting happens at finalization.
                a_vals.extend(b_vals.iter().cloned());
            }
            (AccumulatorState::ApproxPercentile { digest: a_digest, buffer: a_buf, .. },
             AccumulatorState::ApproxPercentile { digest: b_digest, buffer: b_buf, .. }) => {
                // Flush both buffers into their respective digests, then merge.
                // (Buffer flush logic mirrors AccumulatorState::finalize.)
                if !a_buf.is_empty() {
                    let a_values: Vec<f64> = a_buf.drain(..).filter_map(|v| match v {
                        Value::Float(f) => Some(f.0 as f64),
                        Value::Int(i) => Some(i as f64),
                        _ => None,
                    }).collect();
                    *a_digest = TDigest::merge_digests(vec![a_digest.clone(), TDigest::new_with_size(100).merge_sorted(&a_values)]);
                }
                if !b_buf.is_empty() {
                    let b_values: Vec<f64> = b_buf.iter().filter_map(|v| match v {
                        Value::Float(f) => Some(f.0 as f64),
                        Value::Int(i) => Some(*i as f64),
                        _ => None,
                    }).collect();
                    let b_flushed = TDigest::merge_digests(vec![b_digest.clone(), TDigest::new_with_size(100).merge_sorted(&b_values)]);
                    *a_digest = TDigest::merge_digests(vec![a_digest.clone(), b_flushed]);
                } else {
                    *a_digest = TDigest::merge_digests(vec![a_digest.clone(), b_digest.clone()]);
                }
            }
            _ => {
                // Mismatched accumulator types is a programming error --
                // the planner should never construct a merge between
                // different accumulator kinds.
                panic!("mismatched accumulator types in merge");
            }
        }
    }
}
```

**GroupState merge:**

```rust
impl GroupState {
    /// Merge another GroupState into this one, element-wise.
    /// `self` is from the earlier chunk, `other` is from the later chunk.
    pub(crate) fn merge(&mut self, other: &GroupState) {
        assert_eq!(self.accumulators.len(), other.accumulators.len());
        for (a, b) in self.accumulators.iter_mut().zip(other.accumulators.iter()) {
            a.merge(b);
        }
    }
}
```

**Mergeable aggregate check:** All accumulator types now support merge. Count, CountStar, Sum, Avg, Min, Max are algebraically mergeable. First and Last are mergeable because chunks are time-ordered (earlier chunk has the true first, later chunk has the true last). GroupAs and PercentileDisc merge via concatenation. ApproxCountDistinct merges via HyperLogLog's native `merge()`. ApproxPercentile merges via TDigest's `merge_digests()`. Therefore, parallel streaming aggregation is always available when streaming aggregation is activated -- no aggregate-type gate is needed.

### Interaction with HAVING

HAVING is compiled as a `Node::Filter` placed above `Node::GroupBy` in the logical plan:

```
Filter(having_predicate)
  GroupBy(keys, aggregates, source)
```

The `try_get_batch()` handler for `Node::Filter` wraps its child's batch stream with `BatchFilterOperator`. Since `BatchStreamingGroupByOperator` emits standard `ColumnBatch` output (multi-row batches with named columns for the group key and aggregates), the `BatchFilterOperator` applies the HAVING predicate to these output batches transparently. No special handling is required in Layer 1.

### Planner Integration & Detection

The decision happens in `Node::try_get_batch()` in `src/execution/types.rs`, within the `Node::GroupBy(keys, aggregates, source)` match arm. The critical insight is that the GROUP BY key is a `PathExpr` alias (e.g., `AttrName("bucket")`), and the actual `time_bucket()` function call lives in the child `Node::Map` node's named expression list. Detection must look through the Map node to find the original expression.

**Detection approach:**

1. Verify the GROUP BY has exactly one key.
2. Look at the child node: it must be a `Node::Map(named_list, map_source)`.
3. Search `named_list` for a `Named::Expression(Expression::Function("time_bucket", args), Some(alias))` where `alias` matches the GROUP BY key's `AttrName`.
4. From the matched `time_bucket()` call, extract:
   - The interval string: first argument must be `Expression::Constant(Value::String(interval))`.
   - The timestamp column: second argument must be `Named::Expression(Expression::Variable(path), _)` where the path resolves to a column name.
5. Verify the underlying source (below the Map node) is a known time-ordered log format (ELB or ALB).
6. If all conditions are met, construct `BatchStreamingGroupByOperator` with the extracted interval and timestamp column, using `map_source` (not `source`) as the input stream -- bypassing the Map node.

**Activation conditions (all must hold):**

1. The GROUP BY has exactly one key expression.
2. The child node is a `Node::Map` containing a `time_bucket()` function call that aliases to the group key name.
3. The underlying data source is a known time-ordered log format (ELB or ALB only).
4. The query is operating on a single file (not concatenated input).

```rust
Node::GroupBy(keys, aggregates, source) => {
    if let Some(streaming_params) = Self::detect_streaming_groupby(keys, source) {
        // Bypass the Map node: get batch stream from the Map's child source.
        let map_source = streaming_params.map_source;
        match map_source.try_get_batch(variables, registry, required_fields, threads) {
            Some(Ok(batch_stream)) => {
                let op = BatchStreamingGroupByOperator::new(
                    batch_stream,
                    streaming_params.timestamp_column,
                    streaming_params.bucket_interval,
                    streaming_params.bucket_alias,
                    aggregates.clone(),
                    variables.clone(),
                    registry.clone(),
                );
                Some(Ok(Box::new(op) as Box<dyn BatchStream>))
            }
            Some(Err(e)) => Some(Err(e)),
            None => {
                // Map's child doesn't support batch -- fall through to
                // hash-based path below.
                Self::try_hash_groupby(source, keys, aggregates, variables, registry, required_fields, threads)
            }
        }
    } else {
        Self::try_hash_groupby(source, keys, aggregates, variables, registry, required_fields, threads)
    }
}
```

**`detect_streaming_groupby` helper:**

```rust
struct StreamingGroupByParams<'a> {
    timestamp_column: String,
    bucket_interval: String,
    bucket_alias: String,
    map_source: &'a Node,
}

fn detect_streaming_groupby<'a>(
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
```

**`try_hash_groupby` helper (extracted for clarity):**

```rust
fn try_hash_groupby(
    source: &Node,
    keys: &[PathExpr],
    aggregates: &[NamedAggregate],
    variables: &Variables,
    registry: &Arc<FunctionRegistry>,
    required_fields: &HashSet<String>,
    threads: usize,
) -> Option<Result<Box<dyn BatchStream>, StreamError>> {
    match source.try_get_batch(variables, registry, required_fields, threads) {
        Some(Ok(batch_stream)) => {
            let groupby = BatchGroupByOperator::new(
                batch_stream,
                keys.to_vec(),
                aggregates.to_vec(),
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

**`time_bucket` limitations:** The `time_bucket()` function (in `src/functions/datetime.rs`) only supports `Second`, `Minute`, and `Hour` intervals. It returns `Err(TimeIntervalNotSupported)` for `Day`, `Month`, and `Year`. Additionally, the interval value must evenly divide the base unit: `60 % n == 0` for seconds and minutes, `24 % n == 0` for hours. For example, `time_bucket('7 seconds', timestamp)` returns `TimeIntervalNotSupported` because 60 is not divisible by 7. Valid intervals include 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60 for seconds/minutes, and 1, 2, 3, 4, 6, 8, 12, 24 for hours. Queries using unsupported intervals will fail at the function evaluation level before producing incorrect results. This is not a limitation of the streaming design but should be noted for completeness.

**EXPLAIN output:** Indicate `StreamingGroupBy` vs `HashGroupBy` so users can see which path was chosen. When streaming is not chosen, include the reason:

```
StreamingGroupBy [interval="5 minutes", ts_col="timestamp"] aggs=[COUNT(*), SUM(received_bytes)]
  BatchScan elb_logs [timestamp, received_bytes]
```

```
HashGroupBy [reason: multiple GROUP BY keys] [status_code, method] aggs=[COUNT(*)]
  BatchScan elb_logs [status_code, method]
```

```
HashGroupBy [reason: source not time-ordered] [time_bucket('5 minutes', timestamp)] aggs=[COUNT(*)]
  BatchScan s3_logs [time, size]
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
| `src/execution/batch_streaming_groupby.rs` | **New.** `BatchStreamingGroupByOperator` implementing `BatchStream`. Accepts `timestamp_column`, `bucket_interval`, and `bucket_alias` instead of `Vec<PathExpr>` group keys. Internally computes `time_bucket()` on raw timestamp column from input batches. |
| `src/execution/types.rs` | Modify `try_get_batch()` for `Node::GroupBy` to detect streaming-eligible queries via `detect_streaming_groupby()`. This helper inspects the child `Node::Map` to find `time_bucket()` calls, extracts interval/column parameters, and bypasses the Map node when constructing the streaming operator. Extract `try_hash_groupby()` for the fallback path. Add `AccumulatorState::merge` and `GroupState::merge` methods (all accumulator types supported). Add `StreamingGroupByParams` struct. |
| `src/execution/datasource.rs` | Add `is_time_ordered()` method to log format definitions (ELB and ALB only). |
| `src/execution/parallel.rs` | Add boundary-merge logic for combining partial `GroupState`s from adjacent parallel chunks. |
| `src/execution/mod.rs` | Register `batch_streaming_groupby` module. |
| `src/execution/batch.rs` | Helper to build multi-row output batches from completed group buffers. |

## What Stays Unchanged

- All existing row-based and batch GroupBy code
- Parser, logical planner, optimizer
- The `Node::Map` handler in `try_get_batch()` (no changes needed; Map is simply bypassed)
- SIMD kernels, existing batch operators
- Layer 2 (`AggregatingBatchScanOperator`) -- deferred to follow-up

## Testing Strategy

- **Unit tests:** Streaming groupby on synthetic ordered batches, boundary-spanning buckets, single-bucket input, empty input, buckets that span multiple batches.
- **Internal time_bucket computation:** Verify that the operator's internally computed bucket values match those produced by calling `time_bucket()` through the FunctionRegistry, ensuring the fused evaluation is correct.
- **Multi-row output:** Verify that output batches contain up to 64 groups and the final batch contains any remainder.
- **Correctness:** Compare streaming vs hash GroupBy results on the same data to ensure identical output.
- **Out-of-order detection:** Verify that out-of-order input (new key `<` current key across batch boundary) produces a clear error message rather than incorrect results. Verify that equal keys across batch boundaries (bucket spanning batches) do NOT trigger the error.
- **Parallel merge:** Test that chunk boundary merging produces correct aggregates when a bucket spans a chunk split. Test cases where the last bucket of chunk N does and does not match the first bucket of chunk N+1. Test all accumulator types in merge: Count, CountStar, Sum, Avg, Min, Max, First, Last, GroupAs, ApproxCountDistinct, PercentileDisc, ApproxPercentile.
- **HAVING:** Verify that HAVING filters applied above the streaming GroupBy work correctly (standard `BatchFilterOperator` on the output).
- **Activation guard:** Verify that multiple GROUP BY keys, non-time_bucket keys, non-ELB/ALB sources, and Map nodes without time_bucket expressions all fall through to hash-based aggregation.
- **Map bypass:** Verify that the streaming path correctly bypasses the Map node and receives raw timestamp values from the BatchScan, not pre-computed bucket values.
- **Benchmarks:** time-bucket queries on ELB files comparing streaming vs hash GroupBy performance.
