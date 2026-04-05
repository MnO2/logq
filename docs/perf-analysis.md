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

## Round 6 Optimizations (2026-04-05)

24. **Replace parse_host regex with rsplit_once**: Replaced `HOST_REGEX.captures()` with `s.rsplit_once(':')` for host:port parsing. Eliminates regex engine overhead for a trivial split operation.
25. **Replace parse_http_request regex with splitn**: Replaced `SPLIT_HTTP_LINE_REGEX.find_iter()` with `s.splitn(3, ' ')` for HTTP request line parsing. Eliminates regex engine overhead.
26. **Remove dead code**: Removed unused `KEYWORDS` lazy_static from parser (replaced by `is_keyword()`), removed unused `HOST_REGEX` and `SPLIT_HTTP_LINE_REGEX` lazy_statics from common/types.

## Cumulative Results (Original Baseline → Current, Round 6)

| Benchmark | Baseline | Current | Improvement |
|---|---|---|---|
| **Parser** | | | |
| L1_trivial | 2.55 us | 744 ns | **-71%** |
| L2_where | 8.09 us | 2.02 us | **-75%** |
| L3_group_order_limit | 9.40 us | 2.51 us | **-73%** |
| L4_having_like | 16.12 us | 4.15 us | **-74%** |
| L5_casewhen_join | 18.52 us | 5.04 us | **-73%** |
| L6_in_union | 8.70 us | 1.97 us | **-77%** |
| **Execution (synthetic)** | | | |
| execution_map/1K | 754 us | 259 us | **-66%** |
| execution_map/10K | 7.71 ms | 2.59 ms | **-66%** |
| execution_map/100K | 75.4 ms | 26.1 ms | **-65%** |
| execution_filter/1K | 526 us | 160 us | **-70%** |
| execution_filter/10K | 5.28 ms | 1.58 ms | **-70%** |
| execution_filter/100K | 52.8 ms | 15.9 ms | **-70%** |
| execution_limit/1K | 132 us | 121 us | **-8%** |
| execution_limit/10K | 1.32 ms | 1.23 ms | **-7%** |
| execution_limit/100K | 13.3 ms | 12.0 ms | **-10%** |
| **Execution (e2e)** | | | |
| E1_scan_limit | 121 us | 70 us | **-42%** |
| E2_groupby_count | 6.79 ms | 3.70 ms | **-46%** |
| E3_filter_orderby | 8.58 us | 2.19 us | **-74%** |
| **Datasource** | | | |
| ELB | 2.89 ms | 2.27 ms | **-21%** |
| ALB | 4.52 ms | 3.56 ms | **-21%** |
| S3 | 3.12 ms | 2.64 ms | **-15%** |
| Squid | 1.29 ms | 1.07 ms | **-17%** |
| JSONL | 819 us | 760 us | **-7%** |
| **UDF** | | | |
| upper | 40.2 ns | 30.2 ns | **-25%** |
| round | 25.9 ns | 19.4 ns | **-25%** |
| date_part | 32.1 ns | 26.3 ns | **-18%** |
| array_contains | 33.2 ns | 22.0 ns | **-34%** |
| map_keys | 72.6 ns | 55.1 ns | **-24%** |
| regexp_like | 2.82 us | 2.90 us | ~0% |

## Round 7 Optimizations (2026-04-05)

27. **Hand-written log tokenizer**: Replaced regex-based `SPLIT_READER_LINE_REGEX.find_iter()` with a hand-written `LogTokenizer` iterator. The tokenizer handles the same 4 token types (unquoted, double-quoted, single-quoted, bracket-enclosed) using byte-level scanning instead of regex NFA/DFA execution. ~24-29% faster for all structured log formats.

## Cumulative Results (Original Baseline → Current, Round 7)

| Benchmark | Baseline | Current | Improvement |
|---|---|---|---|
| **Parser** | | | |
| L1_trivial | 2.55 us | 820 ns | **-68%** |
| L2_where | 8.09 us | 2.04 us | **-75%** |
| L3_group_order_limit | 9.40 us | 2.54 us | **-73%** |
| L4_having_like | 16.12 us | 4.18 us | **-74%** |
| L5_casewhen_join | 18.52 us | 5.00 us | **-73%** |
| L6_in_union | 8.70 us | 1.98 us | **-77%** |
| **Execution (synthetic)** | | | |
| execution_map/1K | 754 us | 258 us | **-66%** |
| execution_map/10K | 7.71 ms | 2.55 ms | **-67%** |
| execution_map/100K | 75.4 ms | 25.8 ms | **-66%** |
| execution_filter/1K | 526 us | 157 us | **-70%** |
| execution_filter/10K | 5.28 ms | 1.62 ms | **-69%** |
| execution_filter/100K | 52.8 ms | 15.9 ms | **-70%** |
| execution_limit/1K | 132 us | 120 us | **-9%** |
| execution_limit/10K | 1.32 ms | 1.19 ms | **-10%** |
| execution_limit/100K | 13.3 ms | 12.1 ms | **-9%** |
| **Execution (e2e)** | | | |
| E1_scan_limit | 121 us | 61 us | **-50%** |
| E2_groupby_count | 6.79 ms | 3.05 ms | **-55%** |
| E3_filter_orderby | 8.58 us | 2.29 us | **-73%** |
| **Datasource** | | | |
| ELB | 2.89 ms | 1.63 ms | **-44%** |
| ALB | 4.52 ms | 2.84 ms | **-37%** |
| S3 | 3.12 ms | 1.80 ms | **-42%** |
| Squid | 1.29 ms | 746 us | **-42%** |
| JSONL | 819 us | 780 us | **-5%** |
| **UDF** | | | |
| upper | 40.2 ns | 30.2 ns | **-25%** |
| round | 25.9 ns | 19.5 ns | **-25%** |
| date_part | 32.1 ns | 26.4 ns | **-18%** |
| array_contains | 33.2 ns | 21.6 ns | **-35%** |
| map_keys | 72.6 ns | 54.9 ns | **-24%** |
| regexp_like | 2.82 us | 2.88 us | ~0% |

## Round 8 & 9 Optimizations (2026-04-05)

28. **By-reference predicate comparison**: Added `Relation::compare_ref()` for comparing Values by reference. Fast path in `Formula::Predicate` for Variable-op-Constant pattern directly borrows from LinkedHashMap and constant, avoiding two `expression_value()` calls and two Value clones per predicate. Filter 10-12% faster.
29. **Replace parse_time_interval regex with split_whitespace**: Eliminated last regex in common/types.rs.
30. **Remove all dead regex imports**: Removed unused `Regex` imports from common/types.rs and datasource.rs. Removed `SPLIT_READER_LINE_REGEX` and `SPLIT_TIME_INTERVAL_LINE_REGEX` lazy_statics.

## Cumulative Results (Original Baseline → Current, Rounds 1-9)

| Benchmark | Baseline | Current | Improvement |
|---|---|---|---|
| **Parser** | | | |
| L1_trivial | 2.55 us | 759 ns | **-70%** |
| L2_where | 8.09 us | 2.05 us | **-75%** |
| L3_group_order_limit | 9.40 us | 2.54 us | **-73%** |
| L4_having_like | 16.12 us | 4.26 us | **-74%** |
| L5_casewhen_join | 18.52 us | 5.04 us | **-73%** |
| L6_in_union | 8.70 us | 2.13 us | **-76%** |
| **Execution (synthetic)** | | | |
| execution_map/1K | 754 us | 260 us | **-66%** |
| execution_map/10K | 7.71 ms | 2.74 ms | **-65%** |
| execution_map/100K | 75.4 ms | 26.1 ms | **-65%** |
| execution_filter/1K | 526 us | 144 us | **-73%** |
| execution_filter/10K | 5.28 ms | 1.44 ms | **-73%** |
| execution_filter/100K | 52.8 ms | 14.3 ms | **-73%** |
| execution_limit/1K | 132 us | 120 us | **-9%** |
| execution_limit/10K | 1.32 ms | 1.23 ms | **-7%** |
| execution_limit/100K | 13.3 ms | 12.3 ms | **-8%** |
| **Execution (e2e)** | | | |
| E1_scan_limit | 121 us | 61 us | **-50%** |
| E2_groupby_count | 6.79 ms | 3.03 ms | **-55%** |
| E3_filter_orderby | 8.58 us | 2.19 us | **-74%** |
| **Datasource** | | | |
| ELB | 2.89 ms | 1.62 ms | **-44%** |
| ALB | 4.52 ms | 2.69 ms | **-40%** |
| S3 | 3.12 ms | 1.79 ms | **-43%** |
| Squid | 1.29 ms | 757 us | **-41%** |
| JSONL | 819 us | 808 us | ~0% |
| **UDF** | | | |
| upper | 40.2 ns | 30.4 ns | **-24%** |
| round | 25.9 ns | 19.8 ns | **-24%** |
| date_part | 32.1 ns | 26.7 ns | **-17%** |
| array_contains | 33.2 ns | 21.9 ns | **-34%** |
| map_keys | 72.6 ns | 56.8 ns | **-22%** |
| regexp_like | 2.82 us | 2.94 us | ~0% |

## Round 10 & 11 Optimizations (2026-04-05)

31. **HashMap entry API in all aggregates**: Replaced get+insert patterns with entry().or_insert() across Count, Sum, Avg, Max, Min, First, GroupAs, ApproxCountDistinct aggregates. Avoid redundant key clone for existing groups.
32. **GroupByStream group existence check**: Check `groups.contains(&key)` before `groups.insert(key.clone())` to avoid cloning for existing groups.
33. **get_mut before clone in aggregates**: Replaced entry API (which always takes ownership of key) with get_mut + conditional insert. Only clones key for first occurrence of each group. Changed CountAggregate::add_row to take &key.
34. **Pre-compute projection field mapping**: Store (source_field, output_column) pairs at MapStream construction, eliminating per-record pattern matching in simple_projection path.
35. **Defer URL parsing in HttpRequest**: Store raw URL string instead of parsed url::Url. Parse on demand via parsed_url() only when url_* functions are called. Eliminates expensive url::Url::parse() call for every ELB/ALB record.

## Round 12 & 13 Optimizations (2026-04-05)

36. **Cache field_info in Reader**: Pre-compute field_names, datatypes, field_count at Reader construction instead of per-record format dispatch.
37. **Avoid String alloc in operator precedence**: Try exact match before to_ascii_lowercase() in expression parser. ~4-5% parser speedup.
38. **Vec-based InMemoryStream**: Replace VecDeque pop_front with Vec<Option<Record>> + position counter. ~3% execution_map speedup.
39. **Fast UTC timestamp parser**: Hand-written parser for YYYY-MM-DDTHH:MM:SS[.frac]Z format. Falls back to chrono for other formats. ~6% datasource improvement.
40. **Static str for HTTP method/version**: Use &'static str for http_method and http_version in HttpRequest, eliminating 2 String allocations per HttpRequest parse.

## Cumulative Results (Original Baseline → Current, Rounds 1-13)

| Benchmark | Baseline | Current | Improvement |
|---|---|---|---|
| **Parser** | | | |
| L1_trivial | 2.55 us | 760 ns | **-70%** |
| L2_where | 8.09 us | 2.03 us | **-75%** |
| L3_group_order_limit | 9.40 us | 2.52 us | **-73%** |
| L4_having_like | 16.12 us | 4.21 us | **-74%** |
| L5_casewhen_join | 18.52 us | 4.99 us | **-73%** |
| L6_in_union | 8.70 us | 2.01 us | **-77%** |
| **Execution (synthetic)** | | | |
| execution_map/1K | 754 us | 261 us | **-65%** |
| execution_map/10K | 7.71 ms | 2.57 ms | **-67%** |
| execution_map/100K | 75.4 ms | 25.8 ms | **-66%** |
| execution_filter/1K | 526 us | 150 us | **-71%** |
| execution_filter/10K | 5.28 ms | 1.52 ms | **-71%** |
| execution_filter/100K | 52.8 ms | 14.8 ms | **-72%** |
| execution_limit/1K | 132 us | 125 us | **-5%** |
| execution_limit/10K | 1.32 ms | 1.25 ms | **-5%** |
| execution_limit/100K | 13.3 ms | 12.3 ms | **-8%** |
| **Execution (e2e)** | | | |
| E1_scan_limit | 121 us | 48 us | **-60%** |
| E2_groupby_count | 6.79 ms | 2.13 ms | **-69%** |
| E3_filter_orderby | 8.58 us | 2.19 us | **-74%** |
| **Datasource** | | | |
| ELB | 2.89 ms | 928 us | **-68%** |
| ALB | 4.52 ms | 1.91 ms | **-58%** |
| S3 | 3.12 ms | 1.81 ms | **-42%** |
| Squid | 1.29 ms | 755 us | **-41%** |
| JSONL | 819 us | 778 us | **-5%** |
| **UDF** | | | |
| upper | 40.2 ns | 31.1 ns | **-23%** |
| round | 25.9 ns | 19.8 ns | **-24%** |
| date_part | 32.1 ns | 27.2 ns | **-15%** |
| array_contains | 33.2 ns | 23.8 ns | **-28%** |
| map_keys | 72.6 ns | 56.4 ns | **-22%** |
| regexp_like | 2.82 us | 3.01 us | ~0% |

## Summary

- **Parser: 70-77% faster** — VerboseError→Error, first-char keyword dispatch, operator precedence fix
- **Execution map: 65-67% faster** — merge elimination + Value boxing + move-based projections
- **Execution filter: 71-72% faster** — merge elimination + by-reference comparison + pre-allocation
- **Execution limit: 5-8% faster** — Value boxing + general overhead reduction
- **E1 scan_limit e2e: 60% faster** — SELECT * passthrough + deferred URL/DateTime parsing
- **E2 groupby e2e: 69% faster** — all optimizations combined + deferred URL/DateTime parsing
- **E3 filter e2e: 74% faster** — VerboseError→Error + by-reference comparison
- **Datasource ELB: 68% faster** — hand-written tokenizer + deferred URL + fast UTC parser + static HTTP strings
- **Datasource ALB: 58% faster** — hand-written tokenizer + deferred URL + fast UTC parser
- **Datasource S3/Squid: 41-42% faster** — hand-written tokenizer + regex-free parsing
- **UDFs: 15-28% faster** — pre-lowercased function names
- No new dependencies, all 488 tests pass, all changes backwards-compatible
- 40 optimizations applied across 13 rounds of the AlphaCode-inspired pipeline
