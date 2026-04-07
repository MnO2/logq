VERDICT: NEEDS_REVISION

## Summary Assessment

The core idea -- streaming aggregation for time-ordered data with O(1) memory per group -- is sound and well-motivated. However, the design has several critical issues: a flawed assumption about S3/Squid log ordering, a fundamental incompatibility between parallel scanning and streaming aggregation, missing HAVING clause support, and an inaccurate claim about existing SIMD boundary-detection kernels. The design also proposes a struct definition that diverges from the existing accumulator infrastructure without justification.

## Critical Issues (must fix)

### C1. S3 and Squid logs are NOT safely time-ordered

The design states: "ELB/ALB logs are written in arrival order. Add `is_time_ordered()` ... S3, Squid all have timestamp as an early field."

This is misleading and partly wrong:

- **S3 access logs**: AWS documentation explicitly states that S3 server access logs are delivered on a "best effort" basis and log records can be delivered in any order. Multiple log files can cover the same time period, and records within a single file are not guaranteed to be chronologically ordered. The S3 `time` field is also stored as a bracket-enclosed string `[06/Feb/2019:00:00:38 +0000]` (not ISO 8601), which the existing datasource parser stores as `DataType::String` (not `DataType::DateTime`). Even if ordering were guaranteed, `time_bucket()` would fail on this field because it expects a `Value::DateTime`, not a `Value::String`. See `src/execution/datasource.rs` lines 336-364 where all S3 fields are typed as `DataType::String`.

- **Squid logs**: The timestamp is a Unix epoch float stored as `DataType::String`. Same issue -- it is not parsed as `DateTime`. Whether Squid logs are ordered depends on the proxy's configuration and logging mode (buffered vs. unbuffered).

- **ELB logs**: Individual ELB log files are indeed written in arrival order per the AWS documentation. However, if a user concatenates multiple ELB files (a common workflow), the concatenated file is NOT guaranteed to be time-ordered across file boundaries.

**Fix**: Only mark `Elb` and `Alb` as potentially time-ordered, and document the single-file assumption. Remove `S3` and `Squid` from the ordered list. Consider adding a runtime sanity check beyond just the first out-of-order detection.

### C2. Parallel scanning fundamentally conflicts with streaming aggregation

The design does not address the interaction between parallel scanning (`parallel_scan_chunks` in `src/execution/parallel.rs`) and the streaming operator. The current `try_get_batch()` for `Node::DataSource` (lines 612-627 of `types.rs`) chooses the mmap parallel path when `threads > 1`, splitting data into chunks at arbitrary newline boundaries via `split_chunks()`. Each chunk is independently ordered, but:

1. A single time bucket can span a chunk boundary -- the last bucket of chunk N and the first bucket of chunk N+1 may be the same bucket but will be processed by different threads.
2. The parallel path returns a `PrecomputedBatchStream` with all batches flattened in chunk order, but the streaming operator assumes a single contiguous ordered stream.
3. The existing k-way merge (`kway_merge`) operates on sorted records, not on partially-aggregated GroupStates.

The design's proposed `BatchStreamingGroupByOperator` would produce incorrect results if it receives batches from the parallel path because bucket boundaries at chunk splits would be incorrectly finalized. For example, with a 5-minute bucket spanning the boundary between chunk 1 and chunk 2, the operator would emit two separate partial counts instead of one merged count.

**Fix**: Either (a) disable parallel scanning when the streaming GroupBy path is selected, or (b) add a merge step that can combine partial `GroupState`s from adjacent chunks (the existing `merge_count`/`merge_sum`/etc. functions in `parallel.rs` operate on the old hash-based aggregates, not on `AccumulatorState`), or (c) use a two-phase approach where each chunk does its own streaming aggregation, then merge the results, being careful that the last bucket of chunk N must be combined with the first bucket of chunk N+1 if they share the same key.

### C3. HAVING clause is completely unaddressed

The design does not mention HAVING at all. In the existing codebase, HAVING is compiled as a `Node::Filter` placed *above* the `Node::GroupBy` in the logical plan (`src/logical/parser.rs` line 1033). This means the plan looks like:

```
Filter(having_predicate)
  GroupBy(keys, aggregates, source)
```

The current `try_get_batch()` for `Node::Filter` (line 643 of `types.rs`) handles the case where the child is a `DataSource` (predicate pushdown) or wraps any other batch child with `BatchFilterOperator`. This means HAVING will work transparently IF the streaming GroupBy emits standard `ColumnBatch` output. However, the design's Layer 2 (`AggregatingBatchScanOperator`) fuses everything into one operator, which would bypass the `Node::Filter` wrapping for HAVING. The design must specify how HAVING interacts with both layers, and the Layer 2 fused operator must be disabled when HAVING is present.

### C4. No SIMD boundary-detection kernels exist

The design claims: "Boundary detection uses logq's existing SIMD kernels -- compare each element to the next and find positions where they differ."

Examining `src/simd/kernels.rs`, the existing kernels are:
- `filter_ge_i32`, `filter_eq_i32`, `filter_gt_i32`, etc. (compare against a scalar threshold)
- `str_eq_batch`, `str_contains_scalar` (compare against a fixed needle)
- `sum_i32_selected`, `count_selected` (aggregation kernels)
- `hash_column_i32`, `hash_combine` (hashing kernels)

None of these perform adjacent-element comparison or boundary detection. The claimed functionality does not exist. Boundary detection for `Value::DateTime` elements (the output of `time_bucket()`) cannot use these i32/f32 kernels directly anyway, since `DateTime` is a complex type wrapping `chrono::DateTime<FixedOffset>`.

**Fix**: Either (a) write a simple scalar boundary detection loop (compare each element to its predecessor), which is probably fast enough since it is O(n) with early-exit per batch, or (b) describe what new SIMD kernels would be needed and add them to the file changes table.

### C5. Design's struct uses `Vec<AccumulatorState>` but BatchGroupByOperator uses `Vec<NamedAggregate>` -- incompatible interfaces

The proposed `BatchStreamingGroupByOperator` struct stores:
```rust
current_state: Vec<AccumulatorState>,
aggregates: Vec<AggregateDef>,
```

But the existing `BatchGroupByOperator` (which the new operator must be plugged into the same `try_get_batch` match arm) takes `Vec<NamedAggregate>` and `Vec<PathExpr>` as constructor arguments -- these come from `Node::GroupBy(keys, aggregates, source)` where `aggregates` is `Vec<NamedAggregate>`.

The design's struct shows `aggregates: Vec<AggregateDef>` but the constructor would need to accept `Vec<NamedAggregate>` (to match the call site in `try_get_batch` line 772-778) and internally convert to `Vec<AggregateDef>` using `AggregateDef::from_named_aggregate`. The design should also accept `Vec<PathExpr>` for group keys, not `group_expr: Expression`.

**Fix**: Align the struct signature with the existing planner interface. The constructor should accept the same types as `BatchGroupByOperator::new`.

## Suggestions (nice to have)

### S1. Consider reusing `GroupState` directly

The codebase already has `GroupState` (types.rs line 1742) which is exactly `Vec<AccumulatorState>`. The streaming operator could maintain a single `GroupState` as its current state, and call `AccumulatorState::accumulate` and `AccumulatorState::finalize` on it. This avoids inventing new infrastructure.

### S2. Layer 2 (AggregatingBatchScanOperator) is premature

Layer 2 fuses scan + filter + groupby into one operator. This is a significant complexity increase (new operator, new file, activation conditions) for a gain that may be modest -- the streaming GroupBy already eliminates hash table overhead, and the batch pipeline already has good scan + filter fusion. Consider deferring Layer 2 to a follow-up, especially since it has the HAVING interaction issue (C3). Measure the benefit of Layer 1 alone first.

### S3. The fallback from streaming to HashMap is underspecified

The design says: "If the streaming operator encounters an out-of-order row (bucket key goes backwards), fall back to collecting remaining rows into a HashMap."

This needs more detail:
- What happens to already-emitted groups? If group A was already finalized and emitted, and then a late row for group A arrives, the fallback HashMap will create a new entry for group A with count=1, producing a duplicate in the output.
- The fallback should either (a) buffer all output until input is exhausted (defeating the purpose of streaming), or (b) switch to error mode, or (c) merge the HashMap results with already-emitted results (complex).
- A simpler approach: just raise an error or warning and fall back to the existing `BatchGroupByOperator` for the entire query, re-processing from the beginning. But this requires the input to be re-readable.

### S4. Single-row output batches are inefficient

The design specifies: "Each completed group becomes a single-row ColumnBatch." This means if there are 1000 time buckets, the operator emits 1000 single-row batches, each requiring allocation of column vectors, bitmaps, etc. Consider buffering completed groups and emitting multi-row batches (e.g., every 64 or 128 completed groups).

### S5. Multiple GROUP BY keys need clarification

For queries like `GROUP BY time_bucket('5 minutes', timestamp), status_code`, the streaming optimization requires ALL rows with the same composite key to be contiguous. Time-ordered data guarantees contiguity for the time bucket, but within a bucket, different status codes will be interleaved. The design's linear scan would detect a boundary on every status_code change, incorrectly finalizing the time bucket early.

The design should specify that streaming aggregation is only activated when the GROUP BY contains exactly one key that is a `time_bucket()` call, OR when the time bucket is the leading key and the operator maintains a HashMap for the secondary keys within each bucket (a hybrid approach).

### S6. Graceful degradation metric

The design mentions "Log a warning" on out-of-order detection. Consider also exposing this in EXPLAIN ANALYZE output (e.g., "StreamingGroupBy: fell back to HashGroupBy after 45000 rows") so users can understand performance characteristics.

### S7. `time_bucket` only supports seconds/minutes/hours

The `time_bucket` function (`src/functions/datetime.rs` lines 34-127) only supports `Second`, `Minute`, and `Hour` units. It returns `Err(TimeIntervalNotSupported)` for `Day`, `Month`, and `Year`. The design should note this limitation. Queries like `GROUP BY time_bucket('1 day', timestamp)` will fail at the function level before reaching the streaming operator.

## Verified Claims (things you confirmed are correct)

1. **`time_bucket()` exists and returns `Value::DateTime`**: Confirmed in `src/functions/datetime.rs` lines 34-127. The function takes `(Value::String, Value::DateTime)` and returns `Value::DateTime`. Since `Value` derives `PartialEq` and `Eq` (`src/common/types.rs` line 14), output values can be directly compared for boundary detection using `==`.

2. **`BatchGroupByOperator` uses HashMap and materializes all input**: Confirmed in `src/execution/batch_groupby.rs`. The `consume_and_build` method consumes all child batches (line 100: `while let Some(batch) = self.child.next_batch()?`) before emitting results. Uses `HashSet<Option<Tuple>>` for group tracking.

3. **`AccumulatorState` infrastructure exists and is reusable**: Confirmed in `src/execution/types.rs` lines 1489-1737. The `AccumulatorState` enum has `new()`, `accumulate()`, `accumulate_row()`, and `finalize()` methods. The `GroupState` struct wraps `Vec<AccumulatorState>`. Both `GroupByStream` (row-based) and `AggregateDef::from_named_aggregate` already use this infrastructure.

4. **`try_get_batch()` routing for `Node::GroupBy`**: Confirmed at lines 769-784 of `types.rs`. Currently directly constructs `BatchGroupByOperator`. Adding a streaming variant here is the correct insertion point.

5. **ELB logs have timestamp as field 0, ALB has timestamp as field 1**: Confirmed via `ClassicLoadBalancerLogField::Timestamp = 0` and `ApplicationLoadBalancerLogField::Timestamp = 1` in `datasource.rs`. Both are typed as `DataType::DateTime`, meaning they are parsed into `Value::DateTime` and are directly usable by `time_bucket()`.

6. **Parallel merge functions exist for the old Aggregate types**: Confirmed `merge_count`, `merge_sum`, `merge_avg`, `merge_min`, `merge_max` in `src/execution/parallel.rs` lines 140-216. These operate on the old `CountAggregate`/`SumAggregate`/etc. types, NOT on `AccumulatorState`.

7. **HAVING is compiled as Filter above GroupBy**: Confirmed in `src/logical/parser.rs` lines 1031-1034.
