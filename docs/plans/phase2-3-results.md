# Phase 2-3 Benchmark Results

Date: 2026-04-06
Data file: `data/AWSELB.log` (668 lines, 276 KB)
Platform: macOS (Darwin 25.4.0), Apple Silicon

## Profiling Benchmarks: Before/After

| Benchmark              | Phase 0 Baseline | Phase 2-3 | Change  |
|------------------------|-----------------|-----------|---------|
| profiling/scan_only    | 1.113 ms        | 1.298 ms  | +16.6%  |
| profiling/scan_filter  | 3.538 ms        | 3.969 ms  | +12.2%  |
| profiling/scan_filter_groupby | 4.019 ms | 4.529 ms  | +12.7%  |

## Analysis

The batch pipeline introduces a **slight regression** in this first integration.
The root cause is the conversion overhead at the adapter boundaries:

1. **Columnar parsing overhead**: `BatchScanOperator` parses ALL fields into
   `TypedColumn` vectors (17 fields for ELB), whereas the old row-based path
   only parsed fields on-demand. This is visible in the scan_only regression.

2. **Double materialization**: Data is parsed into columnar format, then
   `BatchToRowAdapter` materializes it back into `Record` (HashMap-based)
   for downstream row-based operators (GroupBy, Map, OrderBy). This
   row→column→row round-trip adds overhead without benefiting from the
   columnar format.

3. **Small dataset**: With only 668 lines, the SIMD filtering gains (~2x on
   string equality) don't outweigh the conversion overhead. The crossover
   point where batch processing becomes faster is estimated at ~5K-10K lines.

## Optimization Opportunities (Phase 4)

To realize the projected 2.4x speedup, the following optimizations are needed:

1. **Selective field parsing**: Only parse fields referenced by the query's
   WHERE, SELECT, and GROUP BY clauses. For `WHERE elb_status_code = '200'`,
   only parse field index 7 for filtering, plus projected fields for output.
   This would eliminate ~80% of parsing work for typical queries.

2. **Batch-native GroupBy**: Implement a `BatchGroupByOperator` that operates
   directly on columnar data, avoiding the row materialization for aggregation
   queries. This would eliminate the largest conversion overhead.

3. **Two-phase lazy parsing**: Parse only filter fields first, evaluate the
   predicate to get a selection vector, then parse remaining projected fields
   only for surviving rows. This is the design in the original plan but
   requires predicate pushdown into the scan operator.

4. **Larger test datasets**: Create 10K+ line test data to demonstrate the
   crossover where batch processing outperforms row-based processing.

## What's Working

- The batch pipeline is correctly wired into `Node::get()` for structured
  log formats (ELB, ALB, S3, Squid).
- All 589 existing tests pass with zero regressions.
- The SIMD filter kernels produce correct results through the full pipeline.
- BatchToRowAdapter correctly materializes columnar data back to Records.
- The architecture supports incremental optimization — each item above can
  be implemented independently.
