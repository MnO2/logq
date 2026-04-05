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

## Round 2 & 3 Optimizations (2026-04-05)

9. **Avoid String allocation in keyword check**: `eq_ignore_ascii_case` instead of `to_ascii_lowercase()` in parser identifier(). Eliminates heap alloc per identifier parsed.
10. **Byte-level checks in identifier**: `is_ascii_digit()` and `bytes().all()` instead of `chars()` iteration.
11. **Pre-allocate Vec in Expression::Function**: `Vec::with_capacity(arguments.len())` for function args.
12. **Short-circuit And/Or formula evaluation**: Skip evaluating right side when left already determines result.
13. **Enum-based LogFormat dispatch in Reader**: Resolve format string to enum at Reader construction, avoid per-call string comparisons.
14. **Reuse line buffer across read_record calls**: `String::with_capacity(512)` reused via `clear()` instead of allocating new String per line.
15. **Pre-allocate LinkedHashMap in Record::new and project**: `with_capacity()` for known sizes.
16. **Box large Value variants**: DateTime, Host, HttpRequest, Object wrapped in Box to reduce Value enum from ~64+ to ~32 bytes. Improves cache locality and reduces memcpy for all Value operations.

## Cumulative Results (Original Baseline → Current)

| Benchmark | Baseline | Current | Improvement |
|---|---|---|---|
| **Parser** | | | |
| L1_trivial | 2.55 us | 2.18 us | **-15%** |
| L2_where | 8.09 us | 6.62 us | **-18%** |
| L3_group_order_limit | 9.40 us | 7.76 us | **-17%** |
| L4_having_like | 16.12 us | 13.77 us | **-15%** |
| L5_casewhen_join | 18.52 us | 15.59 us | **-16%** |
| L6_in_union | 8.70 us | 6.49 us | **-25%** |
| **Execution (synthetic)** | | | |
| execution_map/1K | 754 us | 281 us | **-63%** |
| execution_map/10K | 7.71 ms | 2.80 ms | **-64%** |
| execution_map/100K | 75.4 ms | 28.2 ms | **-63%** |
| execution_filter/1K | 526 us | 157 us | **-70%** |
| execution_filter/10K | 5.28 ms | 1.57 ms | **-70%** |
| execution_filter/100K | 52.8 ms | 15.7 ms | **-70%** |
| execution_limit/1K | 132 us | 121 us | **-8%** |
| execution_limit/10K | 1.32 ms | 1.21 ms | **-8%** |
| execution_limit/100K | 13.3 ms | 12.6 ms | **-5%** |
| **Execution (e2e)** | | | |
| E1_scan_limit | 121 us | 93.2 us | **-23%** |
| E2_groupby_count | 6.79 ms | 3.97 ms | **-42%** |
| E3_filter_orderby | 8.58 us | 7.25 us | **-16%** |
| **Datasource** | | | |
| ELB | 2.89 ms | 2.59 ms | **-10%** |
| ALB | 4.52 ms | 3.95 ms | **-13%** |
| S3 | 3.12 ms | 2.55 ms | **-18%** |
| Squid | 1.29 ms | 1.06 ms | **-18%** |
| JSONL | 819 us | 769 us | **-6%** |
| **UDF** | | | |
| upper | 40.2 ns | 30.0 ns | **-25%** |
| round | 25.9 ns | 19.5 ns | **-25%** |
| date_part | 32.1 ns | 26.4 ns | **-18%** |
| array_contains | 33.2 ns | 21.8 ns | **-34%** |
| map_keys | 72.6 ns | 56.3 ns | **-22%** |
| regexp_like | 2.82 us | 2.90 us | ~0% |

## Round 4 Optimizations (2026-04-05)

17. **VerboseError → nom::error::Error in parser**: Replaced `VerboseError<&str>` with `nom::error::Error<&str>` throughout. VerboseError maintains a `Vec` of all error contexts, allocating on every parse error path. `nom::error::Error` stores only last error (input + code). Eliminates Vec allocation/growth in error tracking.
18. **First-char keyword dispatch**: Replaced linear scan of ~40 keywords via `KEYWORDS.iter().any()` with first-char byte-dispatch function `is_keyword()`. Reduces average comparisons from ~40 to 2-3 per identifier. Non-keyword identifiers (column names) return false after a single byte comparison.
19. **Fast-path single-segment Variable lookup**: Added direct `LinkedHashMap::get()` for the common case of single-segment AttrName path expressions, bypassing the full `get_value_by_path_expr()` call.

## Cumulative Results (Original Baseline → Current, Round 4)

| Benchmark | Baseline | Current | Improvement |
|---|---|---|---|
| **Parser** | | | |
| L1_trivial | 2.55 us | 759 ns | **-70%** |
| L2_where | 8.09 us | 2.08 us | **-74%** |
| L3_group_order_limit | 9.40 us | 2.58 us | **-73%** |
| L4_having_like | 16.12 us | 4.25 us | **-74%** |
| L5_casewhen_join | 18.52 us | 5.13 us | **-72%** |
| L6_in_union | 8.70 us | 2.02 us | **-77%** |
| **Execution (synthetic)** | | | |
| execution_map/1K | 754 us | 337 us | **-55%** |
| execution_map/10K | 7.71 ms | 3.07 ms | **-60%** |
| execution_map/100K | 75.4 ms | 31.5 ms | **-58%** |
| execution_filter/1K | 526 us | 178 us | **-66%** |
| execution_filter/10K | 5.28 ms | 1.79 ms | **-66%** |
| execution_filter/100K | 52.8 ms | 17.8 ms | **-66%** |
| execution_limit/1K | 132 us | 125 us | **-5%** |
| execution_limit/10K | 1.32 ms | 1.23 ms | **-7%** |
| execution_limit/100K | 13.3 ms | 12.9 ms | **-3%** |
| **Execution (e2e)** | | | |
| E1_scan_limit | 121 us | 107 us | **-12%** |
| E2_groupby_count | 6.79 ms | 4.49 ms | **-34%** |
| E3_filter_orderby | 8.58 us | 2.35 us | **-73%** |
| **Datasource** | | | |
| ELB | 2.89 ms | 2.94 ms | ~0% |
| ALB | 4.52 ms | 4.44 ms | -2% |
| S3 | 3.12 ms | 2.83 ms | **-9%** |
| Squid | 1.29 ms | 1.12 ms | **-13%** |
| JSONL | 819 us | 883 us | ~0% |
| **UDF** | | | |
| upper | 40.2 ns | 30.3 ns | **-25%** |
| round | 25.9 ns | 19.9 ns | **-23%** |
| date_part | 32.1 ns | 26.8 ns | **-17%** |
| array_contains | 33.2 ns | 21.9 ns | **-34%** |
| map_keys | 72.6 ns | 57.1 ns | **-21%** |
| regexp_like | 2.82 us | 2.91 us | ~0% |

## Round 5 Optimizations (2026-04-05)

20. **Move-based MapStream for simple projections**: When all SELECT expressions are simple single-segment Variable references with no extra variables to merge, take ownership of the source record and use `remove()` to move Values instead of cloning. Eliminates Value::clone (including heap-allocating String clone) per projected column per record.
21. **SELECT * passthrough**: When the named list is a single Star with no extra variables, return the source record directly without any copying or rebuilding.
22. **Fix Formula::NotIn cloning**: Previously delegated to `Formula::In(expr.clone(), list.clone())`, cloning the entire expression tree per evaluation. Now inlines the NOT IN logic with negated return values.
23. **Cache LIKE regex compilations**: Thread-local LRU cache (capacity 64) for compiled LIKE patterns, same pattern as regexp functions. Avoids `Regex::new()` on every LIKE evaluation when patterns repeat.

## Cumulative Results (Original Baseline → Current, Round 5)

| Benchmark | Baseline | Current | Improvement |
|---|---|---|---|
| **Parser** | | | |
| L1_trivial | 2.55 us | 749 ns | **-71%** |
| L2_where | 8.09 us | 2.06 us | **-75%** |
| L3_group_order_limit | 9.40 us | 2.54 us | **-73%** |
| L4_having_like | 16.12 us | 4.38 us | **-73%** |
| L5_casewhen_join | 18.52 us | 5.03 us | **-73%** |
| L6_in_union | 8.70 us | 2.09 us | **-76%** |
| **Execution (synthetic)** | | | |
| execution_map/1K | 754 us | 268 us | **-64%** |
| execution_map/10K | 7.71 ms | 2.69 ms | **-65%** |
| execution_map/100K | 75.4 ms | 26.9 ms | **-64%** |
| execution_filter/1K | 526 us | 166 us | **-68%** |
| execution_filter/10K | 5.28 ms | 1.64 ms | **-69%** |
| execution_filter/100K | 52.8 ms | 16.6 ms | **-69%** |
| execution_limit/1K | 132 us | 135 us | ~0% |
| execution_limit/10K | 1.32 ms | 1.40 ms | ~0% |
| execution_limit/100K | 13.3 ms | 13.3 ms | ~0% |
| **Execution (e2e)** | | | |
| E1_scan_limit | 121 us | 76 us | **-37%** |
| E2_groupby_count | 6.79 ms | 3.94 ms | **-42%** |
| E3_filter_orderby | 8.58 us | 2.19 us | **-74%** |
| **Datasource** | | | |
| ELB | 2.89 ms | 2.59 ms | **-10%** |
| ALB | 4.52 ms | 4.09 ms | **-10%** |
| S3 | 3.12 ms | 2.59 ms | **-17%** |
| Squid | 1.29 ms | 1.06 ms | **-18%** |
| JSONL | 819 us | 791 us | **-3%** |
| **UDF** | | | |
| upper | 40.2 ns | 32.5 ns | **-19%** |
| round | 25.9 ns | 21.1 ns | **-19%** |
| date_part | 32.1 ns | 27.8 ns | **-13%** |
| array_contains | 33.2 ns | 23.0 ns | **-31%** |
| map_keys | 72.6 ns | 57.1 ns | **-21%** |
| regexp_like | 2.82 us | 2.91 us | ~0% |

## Summary

- **Parser: 71-76% faster** — VerboseError→Error, first-char keyword dispatch
- **Execution map: 64-65% faster** — merge elimination + Value boxing + move-based projections
- **Execution filter: 68-69% faster** — merge elimination + Value boxing + pre-allocation
- **E1 scan_limit e2e: 37% faster** — SELECT * passthrough eliminates record rebuilding
- **E2 groupby e2e: 42% faster** — merge + capacity + function registry
- **E3 filter e2e: 74% faster** — VerboseError→Error + move-based optimizations
- **Datasource: 3-18% faster** — format resolution + pre-allocation + buffer reuse + Value boxing
- **UDFs: 13-31% faster** — pre-lowercased function names (partially offset by Box indirection)
- No new dependencies, all 488 tests pass, all changes backwards-compatible
