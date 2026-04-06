# Phase 0 Profiling Baseline Results

Date: 2026-04-06
Data file: `data/AWSELB.log` (668 lines, 276 KB)
Platform: macOS (Darwin 25.4.0), Apple Silicon

## Tier C: Component-Level Profiling (Critical Path Analysis)

These benchmarks isolate the cost of each execution phase by running
progressively more complex queries against the real ELB log file.

| Benchmark                | Query                                                     | Median Time |
|--------------------------|-----------------------------------------------------------|-------------|
| profiling/scan_only      | `SELECT * FROM elb`                                       | 1.1134 ms   |
| profiling/scan_filter    | `SELECT * FROM elb WHERE elb_status_code = "200"`         | 3.5381 ms   |
| profiling/scan_filter_groupby | `SELECT elb_status_code, count(*) FROM elb WHERE elb_status_code = "200" GROUP BY elb_status_code` | 4.0192 ms |

### Cost Breakdown (by subtraction)

| Phase              | Absolute Cost | % of Total |
|--------------------|---------------|------------|
| Scanning + Parsing | 1.113 ms      | 27.7%      |
| Filtering          | 2.425 ms      | 60.3%      |
| Group-by + Aggregation | 0.481 ms  | 12.0%      |
| **Total**          | **4.019 ms**  | **100%**   |

### Key Observation

**Filtering dominates execution time at 60.3%.** The WHERE clause evaluation
(`elb_status_code = "200"`) is by far the most expensive phase -- nearly 2.2x
the cost of scanning and parsing the entire file. Aggregation (GROUP BY + count)
is relatively cheap at 12%.

This is consistent with the row-at-a-time evaluation model: each row requires
extracting the `elb_status_code` field from a HashMap-based `Record`, boxing it
as a `Value`, and performing a dynamic equality comparison. The per-record
overhead of this polymorphic evaluation path is substantial.

## Tier A: End-to-End Benchmarks

| Benchmark                       | Median Time |
|---------------------------------|-------------|
| execution_e2e/E1_scan_limit     | 32.79 us    |
| execution_e2e/E2_groupby_count  | 2.107 ms    |
| execution_e2e/E3_filter_orderby | 2.34 us     |

Notes:
- E1 (LIMIT 10) is fast because it short-circuits after 10 rows.
- E3 (filter + order) returns ~0 matching rows with status "200" string
  equality, so the ORDER BY has near-zero cost.
- E2 (full scan + GROUP BY + count) at 2.1 ms closely matches the
  scan_filter_groupby profiling benchmark, confirming consistency.

## Tier B: Isolated Operator Benchmarks (Synthetic Data)

| Operator          | 1K rows    | 10K rows   | 100K rows   |
|-------------------|------------|------------|-------------|
| execution_map     | 212.94 us  | 2.167 ms   | 21.20 ms    |
| execution_filter  | 141.50 us  | 1.467 ms   | 14.44 ms    |
| execution_limit   | 121.03 us  | 1.271 ms   | 11.91 ms    |

All three operators scale linearly with row count (~10x per 10x rows),
confirming O(n) complexity with no unexpected bottlenecks.

Per-row cost (from 100K data points):
- Map (2-column projection): ~212 ns/row
- Filter (integer comparison): ~144 ns/row
- Limit (passthrough + counter): ~119 ns/row

## Datasource Parsing Benchmarks

| Format | Median Time | Throughput     |
|--------|-------------|----------------|
| ELB    | 909.31 us   | 734.6 Kelem/s  |
| ALB    | 1.922 ms    | 520.7 Kelem/s  |
| S3     | 1.810 ms    | 552.4 Kelem/s  |
| Squid  | 732.39 us   | 1.365 Melem/s  |
| JSONL  | 785.17 us   | 1.276 Melem/s  |

Squid and JSONL are the fastest formats. ALB and S3 are ~2x slower due to
more fields and more complex parsing logic.

## Parser Benchmarks

| Query Complexity   | Median Time |
|--------------------|-------------|
| L1 trivial         | 731.35 ns   |
| L2 where           | 1.986 us    |
| L3 group+order+limit | 2.454 us  |
| L4 having+like     | 4.227 us    |
| L5 casewhen+join   | 4.993 us    |
| L6 in+union        | 2.003 us    |

Parser performance is in the single-microsecond range and is negligible
compared to execution costs.

## UDF Benchmarks

| Function        | Median Time |
|-----------------|-------------|
| upper           | 30.23 ns    |
| round           | 19.08 ns    |
| date_part       | 26.24 ns    |
| array_contains  | 20.90 ns    |
| map_keys        | 54.26 ns    |
| regexp_like     | 2.853 us    |

All UDFs except `regexp_like` are sub-100ns. `regexp_like` is ~100x slower
due to regex compilation (would benefit from caching, already addressed
by the LRU regex cache).

## Phase 1 Priority Recommendations

Based on the profiling data, the SIMD optimization effort should prioritize:

1. **Filter evaluation (60.3% of time)** -- This is the single largest cost
   center. Converting string equality checks from row-at-a-time HashMap
   lookups + Value boxing to batch column-oriented comparisons with SIMD
   string matching will yield the greatest absolute speedup. Target: 3-5x
   improvement on filter-heavy queries.

2. **Scanning/parsing (27.7% of time)** -- The second priority. Converting
   the record-at-a-time parsing into a columnar batch model where fields
   are parsed into typed column vectors will reduce per-row overhead from
   HashMap insertion. The datasource benchmarks show ELB parsing at ~1.36 us
   per row; a columnar approach should cut this significantly.

3. **Aggregation (12.0% of time)** -- Lowest priority for SIMD optimization.
   The GROUP BY + count aggregation is already relatively efficient. However,
   batch hash-based grouping with SIMD hashing could still provide modest
   gains, especially for high-cardinality GROUP BY keys.

### Expected Impact

If filtering cost is reduced by 4x (from 2.425 ms to 0.606 ms) and scanning
cost by 2x (from 1.113 ms to 0.557 ms), the total query time would drop from
4.019 ms to ~1.644 ms -- a 2.4x overall speedup on the representative
scan+filter+groupby workload.
