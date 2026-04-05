# Performance Analysis & Optimization Results

## Baseline (2026-04-05, branch: autoresearch/perf-apr5, pre-optimization)

| Benchmark | Baseline |
|---|---|
| parser/L1_trivial | 2.55 us |
| parser/L2_where | 8.09 us |
| parser/L3_group_order_limit | 9.40 us |
| parser/L4_having_like | 16.12 us |
| parser/L5_casewhen_join | 18.52 us |
| parser/L6_in_union | 8.70 us |
| execution_map/1K | 754 us |
| execution_map/10K | 7.71 ms |
| execution_map/100K | 75.4 ms |
| execution_filter/1K | 526 us |
| execution_filter/10K | 5.28 ms |
| execution_filter/100K | 52.8 ms |
| execution_limit/1K | 132 us |
| execution_limit/10K | 1.32 ms |
| execution_limit/100K | 13.3 ms |
| E1_scan_limit | 121 us |
| E2_groupby_count | 6.79 ms |
| E3_filter_orderby | 8.58 us |
| datasource/ELB | 2.89 ms |
| datasource/ALB | 4.52 ms |
| datasource/S3 | 3.12 ms |
| datasource/Squid | 1.29 ms |
| datasource/JSONL | 819 us |
| udf/upper | 40.2 ns |
| udf/round | 25.9 ns |
| udf/date_part | 32.1 ns |
| udf/array_contains | 33.2 ns |
| udf/map_keys | 72.6 ns |
| udf/regexp_like | 2.82 us |

## Optimizations Applied

1. **Static precedence table in parser**: Replaced per-call HashMap<String,...> construction with lazy_static HashMap<&str,...>. Eliminates 14 String allocations per `expression()` call.

2. **Skip merge when variables empty**: In FilterStream, MapStream, and GroupByStream, skip the expensive `merge()` call when `self.variables` is empty (common case). Avoids O(n_cols) String+Value clones per record.

3. **Direct Variables construction in MapStream**: Build output Variables directly instead of separate field_names+data vectors then Record::new(). Eliminates intermediate allocations.

4. **Pre-compute column names in MapStream**: Compute column names once at construction instead of cloning from named_list on every record.

5. **Resolve datasource format once**: Move format string comparisons out of per-field loop. Pre-resolve field_names, datatypes, and field_count before iterating regex matches.

6. **Remove redundant clone in expression_value**: `get_value_by_path_expr` already returns owned Value; removed the extra `.clone()`.

7. **Pre-lowercase function names at plan creation**: Lowercase function names once during logical→physical plan conversion instead of per-call `to_ascii_lowercase()` in FunctionRegistry.

8. **Pre-allocate LinkedHashMap capacity**: Use `with_capacity()` for Variables in MapStream output and datasource record creation.

## Final Results

| Benchmark | Baseline | After | Improvement |
|---|---|---|---|
| **Parser** | | | |
| L1_trivial | 2.55 us | 2.17 us | **-15%** |
| L2_where | 8.09 us | 6.67 us | **-18%** |
| L3_group_order_limit | 9.40 us | 8.35 us | **-11%** |
| L4_having_like | 16.12 us | 14.04 us | **-13%** |
| L5_casewhen_join | 18.52 us | 16.13 us | **-13%** |
| L6_in_union | 8.70 us | 6.52 us | **-25%** |
| **Execution (synthetic)** | | | |
| execution_map/1K | 754 us | 307 us | **-59%** |
| execution_map/10K | 7.71 ms | 3.05 ms | **-60%** |
| execution_map/100K | 75.4 ms | 30.3 ms | **-60%** |
| execution_filter/1K | 526 us | 171 us | **-67%** |
| execution_filter/10K | 5.28 ms | 1.72 ms | **-67%** |
| execution_filter/100K | 52.8 ms | 17.0 ms | **-68%** |
| execution_limit/1K | 132 us | 132 us | ~0% |
| execution_limit/10K | 1.32 ms | 1.32 ms | ~0% |
| execution_limit/100K | 13.3 ms | 13.1 ms | -1% |
| **Execution (e2e)** | | | |
| E1_scan_limit | 121 us | 93.3 us | **-23%** |
| E2_groupby_count | 6.79 ms | 3.97 ms | **-42%** |
| E3_filter_orderby | 8.58 us | 7.51 us | **-12%** |
| **Datasource** | | | |
| ELB | 2.89 ms | 2.57 ms | **-11%** |
| ALB | 4.52 ms | 4.08 ms | **-10%** |
| S3 | 3.12 ms | 2.64 ms | **-15%** |
| Squid | 1.29 ms | 1.09 ms | **-16%** |
| JSONL | 819 us | 810 us | -1% |
| **UDF** | | | |
| upper | 40.2 ns | 24.9 ns | **-38%** |
| round | 25.9 ns | 13.1 ns | **-49%** |
| date_part | 32.1 ns | 21.0 ns | **-35%** |
| array_contains | 33.2 ns | 17.9 ns | **-46%** |
| map_keys | 72.6 ns | 61.5 ns | **-15%** |
| regexp_like | 2.82 us | 2.83 us | ~0% |

## Summary

- **Execution filter/map: 60-68% faster** — dominated by merge elimination
- **E2 groupby e2e: 42% faster** — merge + capacity + function registry
- **Parser: 11-25% faster** — static precedence table
- **Datasource: 10-16% faster** — format resolution + pre-allocation
- **UDFs: 15-49% faster** — pre-lowercased function names
- No new dependencies, all 488 tests pass, all changes backwards-compatible
