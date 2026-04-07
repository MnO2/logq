VERDICT: NEEDS_REVISION

## Summary Assessment

The v2 design made strong progress addressing the first review's issues -- S3/Squid exclusion, HAVING transparency, SIMD claim removal, and the parallel chunk merge approach are all solid improvements. However, there is one critical architectural gap: the current batch pipeline cannot reach the streaming GroupBy operator for `time_bucket()` queries because the Map node (which evaluates the function call) blocks the batch path. This must be resolved before implementation can proceed.

## Critical Issues (must fix)

### C1. The batch path is unreachable for time_bucket GROUP BY queries

The design proposes inserting `BatchStreamingGroupByOperator` into the `Node::GroupBy` match arm of `try_get_batch()` (line 769 of `types.rs`). However, this code path requires the child node's `try_get_batch()` to return `Some(Ok(...))`. For a typical `time_bucket` query:

```sql
SELECT time_bucket('5 minutes', timestamp) AS bucket, COUNT(*)
FROM elb_logs
GROUP BY time_bucket('5 minutes', timestamp) AS bucket
```

The logical plan tree is:

```
GroupBy(keys=[AttrName("bucket")], aggregates, source=
  Map(named_list=[Function("time_bucket", ...) AS "bucket", ...], source=
    DataSource(elb_logs)))
```

The `time_bucket()` function call is placed in a `Map` node below the `GroupBy` by the parser (see `src/logical/parser.rs` lines 746-783 and 933). When `GroupBy.try_get_batch()` calls `source.try_get_batch()`, it hits the `Map` node's handler (line 724). But the Map handler at lines 726-731 checks:

```rust
let is_simple = named_list.iter().all(|named| match named {
    Named::Expression(Expression::Variable(_), _) => true,
    Named::Star => true,
    _ => false,
});
if !is_simple { return None; }
```

Since `time_bucket()` is a `Function(...)` expression (not a `Variable`), `is_simple` is `false`, and `try_get_batch()` returns `None`. The entire batch path is bypassed, and the query falls through to the row-based `GroupByStream`. The streaming GroupBy operator would never be constructed.

**Fix options (pick one):**

(a) **Extend the Map batch handler** to support function call expressions. This means the Map operator would need to evaluate functions column-by-column (or row-by-row) on the batch and produce a new batch with the computed column. This is the most general solution but has significant scope.

(b) **Fuse the function evaluation into the streaming GroupBy operator itself.** Instead of relying on the Map node to compute `time_bucket()`, have the streaming operator accept the original GROUP BY expression (the `Expression::Function("time_bucket", ...)` from the parser) and evaluate it per-row internally. The `can_use_streaming_groupby` helper would need access to the original GROUP BY expression from the AST, not just the PathExpr alias. This requires either changing the `Node::GroupBy` representation to carry the original expressions alongside the PathExpr keys, or walking the plan tree to find the Map node below and extracting the expression.

(c) **Restructure the plan tree** so that the `time_bucket()` evaluation is handled within the GroupBy node rather than in a separate Map node below it. This is a parser-level change.

Any of these is a non-trivial change. The design should specify which approach will be taken and update the File Changes table accordingly.

### C2. The `is_time_bucket_key` detection cannot work on PathExpr

The design's `can_use_streaming_groupby` helper proposes:

```rust
if !Self::is_time_bucket_key(&keys[0]) {
    return false;
}
```

But `keys[0]` is a `PathExpr` like `AttrName("bucket")` or `AttrName("_1")` -- it's just a name reference. There is no function call information in a `PathExpr` (it can only contain `AttrName`, `ArrayIndex`, `Wildcard`, `WildcardAttr` segments). The method cannot determine whether the key originated from a `time_bucket()` call by inspecting the `PathExpr` alone.

This is directly related to C1. The detection must either inspect the original GROUP BY AST expression (before it was reduced to a PathExpr alias), or find the Map node below and check the expression associated with the alias name.

**Fix:** Store the original GROUP BY expressions in `Node::GroupBy` (alongside the PathExpr keys used for lookup), or provide a mechanism to trace the PathExpr alias back to its defining expression in the child Map node.

### C3. Missing `--no-streaming-agg` CLI flag

The out-of-order error message tells users: `Hint: Re-run with --no-streaming-agg to use hash-based aggregation`. But no such CLI flag is defined in `cli.yml` or implemented anywhere in the codebase. The error message references a non-existent escape hatch.

**Fix:** Add `--no-streaming-agg` (or a similar flag) to the `cli.yml` args for the `query` subcommand, thread it through to `try_get_batch`, and skip the streaming path when it's set. Alternatively, change the error message to suggest a workaround that actually exists (e.g., `--threads 1` if parallel scanning is the issue, or simply removing the workaround hint).

## Suggestions (nice to have)

### S1. First/Last accumulators are mergeable for time-ordered parallel chunks

The design lists only Count, CountStar, Sum, Avg, Min, Max as mergeable. For time-ordered data split into sequential chunks, `First` and `Last` are also mergeable:

- For a boundary bucket spanning chunks N and N+1: the correct `First` is from chunk N (earlier data), and the correct `Last` is from chunk N+1 (later data).

```rust
(AccumulatorState::First(a), AccumulatorState::First(_b)) => {
    // Keep a (from the earlier chunk) -- it's already the first value
}
(AccumulatorState::Last(_a), AccumulatorState::Last(b)) => {
    // Take b (from the later chunk) -- it's the last value
    *a = b.clone();
}
```

This would expand the set of queries that can use parallel streaming aggregation.

### S2. ApproxCountDistinct and ApproxPercentile are also mergeable

The `pdatastructs::HyperLogLog` supports merge, and the `tdigest::TDigest` supports `merge_digests`. The large-file-throughput design (`docs/plans/2026-04-06-large-file-throughput-design-final.md` line 393) explicitly calls this out. Listing them as non-mergeable in the panic arm is overly conservative. These could be supported with:

```rust
(AccumulatorState::ApproxCountDistinct(a), AccumulatorState::ApproxCountDistinct(b)) => {
    a.merge(b);
}
(AccumulatorState::ApproxPercentile { digest: a, buffer: a_buf, .. },
 AccumulatorState::ApproxPercentile { digest: b, buffer: b_buf, .. }) => {
    // Flush both buffers, then merge digests
    // ...
}
```

This is a future optimization and not blocking.

### S3. GroupAs and PercentileDisc merge is concatenation

`GroupAs` accumulates all values into a `Vec<Value>`. Merge is concatenation: `a_vals.extend(b_vals)`. Similarly, `PercentileDisc` accumulates all values and sorts at finalize. Merge is also concatenation. These are technically mergeable, just not noted in the design. Whether they're worth enabling depends on usage frequency.

### S4. Output batch size of 64 is reasonable but should be configurable or benchmarked

The 64-group batch size is reasonable for amortizing allocation overhead. However, downstream operators like `BatchFilterOperator` (for HAVING) process batches in full, so very small batches create overhead while very large batches delay streaming. Consider making this a const that can be tuned based on benchmarks, and document the tradeoff.

### S5. Out-of-order detection uses strict less-than, but equal keys across batch boundaries are normal

The design says: "If a new bucket key is strictly less than the previous bucket key (i.e., time goes backwards), the operator returns an error." This is correct -- equal keys across batch boundaries are expected (bucket spans batches) and not an error. The implementation should be careful to compare with `<` (strictly less than), not `!=`. The design's `find_boundaries` function correctly uses `!=` for boundary detection within a batch (where different means a new bucket), but the cross-batch check should use `<` for the error case. This distinction should be made explicit in the implementation guidance.

### S6. The design should note that `time_bucket` requires the interval to evenly divide its base unit

The `time_bucket()` function rejects intervals where `60 % n != 0` (for seconds/minutes) or `24 % n != 0` (for hours). For example, `time_bucket('7 seconds', timestamp)` returns `TimeIntervalNotSupported`. This means only intervals like 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60 seconds/minutes are valid. The design should note this constraint as it affects what queries can use the streaming path (all valid `time_bucket` queries can, but users might be surprised by the function's restrictions).

### S7. EXPLAIN output should indicate WHY streaming was not chosen

The design proposes showing `StreamingGroupBy` vs `HashGroupBy` in EXPLAIN output. Consider also showing the reason when streaming is not chosen (e.g., "HashGroupBy [reason: multiple GROUP BY keys]" or "HashGroupBy [reason: source not time-ordered]"). This helps users understand and optimize their queries.

## Verified Claims (things I confirmed are correct)

1. **C1 from review 1 properly addressed (S3/Squid exclusion):** The v2 design correctly restricts time-ordered sources to ELB and ALB only. Confirmed: S3 fields are all `DataType::String` in `AWS_S3_DATATYPES` (datasource.rs line 336-364), and Squid fields are all `DataType::String` in `SQUID_DATATYPES` (datasource.rs line 384-394). The single-file assumption is now explicitly documented.

2. **C2 from review 1 properly addressed (parallel chunk merge):** The two-phase approach (per-chunk streaming + boundary merge) is architecturally sound. The `AccumulatorState::merge` method handles Count, CountStar, Sum, Avg, Min, Max correctly. The Sum merge at line 171 correctly handles `None` cases. The GroupState merge correctly asserts accumulator count equality and merges element-wise.

3. **C3 from review 1 properly addressed (HAVING):** HAVING as `Node::Filter` above `Node::GroupBy` (confirmed at parser.rs line 1031-1033) works transparently because the streaming operator emits standard `ColumnBatch` output. Layer 2 (which would break HAVING) is correctly deferred.

4. **C4 from review 1 properly addressed (SIMD claim removed):** The v2 design uses a simple scalar boundary detection loop (`find_boundaries`). This is appropriate. The existing SIMD kernels in `src/simd/kernels.rs` are all scalar-threshold comparisons (filter_ge_i32, etc.) and string matching -- none perform adjacent-element comparison. The design no longer claims otherwise.

5. **C5 from review 1 properly addressed (constructor signature):** The constructor now accepts `Vec<PathExpr>` and `Vec<NamedAggregate>`, matching the existing `BatchGroupByOperator::new` signature (batch_groupby.rs line 34-40). Internal conversion to `Vec<AggregateDef>` via `AggregateDef::from_named_aggregate` matches the pattern used by `GroupByStream::new` (stream.rs line 386-388).

6. **S3 from review 1 addressed (error on out-of-order):** The error approach is simpler and more correct than a mid-stream fallback. As the review noted, a fallback would produce duplicate groups for already-emitted buckets.

7. **S4 from review 1 addressed (multi-row output batches):** Groups are now buffered up to 64 per batch instead of emitting single-row batches.

8. **S5 from review 1 addressed (single key only):** Streaming activates only for exactly one GROUP BY key that is `time_bucket()`. Multiple keys fall through to hash-based aggregation.

9. **Value comparison for boundary detection:** `Value` derives `PartialEq` and `Eq` (common/types.rs line 14), so `bucket_values[i] != bucket_values[i-1]` works directly for `Value::DateTime` values. `value_less_than` (types.rs line 1831) supports `DateTime` comparison for out-of-order detection.

10. **ELB timestamp is field 0, ALB timestamp is field 1:** Confirmed via `AWS_ELB_DATATYPES[0]` = `DataType::DateTime` and `AWS_ALB_DATATYPES[1]` = `DataType::DateTime`.

11. **time_bucket limitations confirmed:** The function (datetime.rs lines 33-127) only supports Second, Minute, and Hour intervals. Day/Month/Year return `TimeIntervalNotSupported`. Additionally, the interval must evenly divide the base unit (e.g., `60 % n == 0` for seconds/minutes).

12. **Existing hash-based GroupBy materializes all input:** Confirmed in `BatchGroupByOperator::consume_and_build` (batch_groupby.rs line 100: `while let Some(batch) = self.child.next_batch()?`) and `GroupByStream` (stream.rs line 405: `while let Some(record) = self.source.next()?`). Both consume entire input before emitting any results.

13. **Parallel merge functions operate on old Aggregate types, not AccumulatorState:** Confirmed in parallel.rs lines 140-216. Functions like `merge_count` operate on `CountAggregate` (which has a `HashMap<Option<Tuple>, i64>`), not on `AccumulatorState::Count(i64)`. The new `AccumulatorState::merge` method is correctly needed.
