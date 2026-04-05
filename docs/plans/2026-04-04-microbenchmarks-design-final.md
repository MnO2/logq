# Microbenchmarks Design for logq (Final)

## Changes from previous version (v2 -> final)

This revision addresses all six critical issues raised in the round 2 review:

- **C1 (Single-quoted strings)**: All benchmark queries now use double-quoted string literals. The parser's `value()` function (parser.rs:190-191) dispatches to `double_quote_string_literal` only -- single-quoted strings are not valid general values in logq. Fixed queries: Level 4 LIKE pattern, Level 5 CASE WHEN branches, and Tier A E3 WHERE clause.
- **C2 (Bare JOIN unsupported)**: Level 5 now uses `LEFT JOIN ... ON` instead of bare `JOIN`. The parser only supports `LEFT [OUTER] JOIN` (parser.rs:518-533) and `CROSS JOIN` / comma-separated tables (parser.rs:503-516). The `JoinType` enum has only `Cross` and `Left` variants.
- **C3 (IN with subquery unsupported)**: Level 6 no longer uses `IN (SELECT ...)`. The `parse_postfix_in` function (parser.rs:700-748) consumes the opening `(` and then parses a comma-separated expression list -- it does not delegate to `subquery`. Level 6 now uses `IN (10, 20, 30)` (a value list) plus `UNION` and `IS NOT NULL`, which are all supported.
- **C4 (pub use does not fix pub(crate) methods)**: The `bench-internals` feature now uses `#[cfg(feature = "bench-internals")]` to conditionally change method visibility from `pub(crate)` to `pub` on each method that benchmarks need. The design specifies every method that must be changed, with the exact pattern. The `pub use` re-export in `lib.rs` remains for path convenience but is no longer the visibility fix.
- **C5 (Regex caching already exists)**: Removed the incorrect Known Limitation about regex compilation. The codebase already has a thread-local LRU cache (capacity 64) at regexp.rs:10-14 via `get_or_compile_regex`. The benchmark measures steady-state cached regex matching + LRU lookup, not compilation.
- **C6 (app::run writes to stdout)**: Tier A benchmarks no longer use `app::run`. Instead, they use a new `app::run_to_records` function (adapted from the existing `#[cfg(test)] run_to_vec`) that runs the full pipeline and collects results into `Vec<Record>` without any I/O. This function is gated behind `bench-internals`.

Also incorporated from round 2 suggestions:

- **S1 (No FunctionDef lookup API)**: Dropped the "direct call" sub-benchmark for UDFs. `FunctionRegistry` does not expose a `get()` method and the `functions` HashMap is private. The `registry.call()` path already measures the realistic hot path including dispatch overhead.
- **S2 (Warm-cache note for regexp_like)**: Added a note that the benchmark measures steady-state warm-cache performance after the first iteration.

---

## Goals

Comprehensive microbenchmark suite covering four areas: parser, execution engine, datasource readers, and user-defined functions. Serves three purposes:

1. **Regression guard** -- catch performance regressions from refactors
2. **Optimization guide** -- identify hotspots worth optimizing
3. **Comparison baseline** -- before/after numbers for the PartiQL completion phases

## File Structure

```
benches/
  bench_parser.rs        # Parser graduated ladder
  bench_execution.rs     # Execution with small + synthetic data
  bench_datasource.rs    # Per-format parse throughput
  bench_udf.rs           # One function per category
  helpers/
    mod.rs
    synthetic.rs         # Synthetic data generation (1K/10K/100K rows)
    queries.rs           # Shared query strings for parser + execution
```

Each `bench_*.rs` file is a separate `[[bench]]` target in `Cargo.toml` with `harness = false`. Each defines its own `criterion_group!` and `criterion_main!`. The `helpers/` module provides reusable synthetic data generators and query constants shared across benchmark files.

Run individual groups: `cargo bench --bench bench_parser --features bench-internals` or all: `cargo bench --features bench-internals`.

### Cargo.toml changes

Replace the existing `[[bench]]` entry with:

```toml
[features]
bench-internals = []

[[bench]]
name = "bench_parser"
harness = false
required-features = ["bench-internals"]

[[bench]]
name = "bench_execution"
harness = false
required-features = ["bench-internals"]

[[bench]]
name = "bench_datasource"
harness = false
required-features = ["bench-internals"]

[[bench]]
name = "bench_udf"
harness = false
```

The `bench_udf` target does not require `bench-internals` because it only uses publicly exported types from `logq::functions` (which is already `pub`). The parser benchmark accesses `syntax::parser::query()` which requires the feature gate since `query()` is `pub(crate)`. The execution and datasource benchmarks access `pub(crate)` stream operators, `Record`, `ReaderBuilder`, etc.

## Visibility: The `bench-internals` Cargo Feature

Criterion benchmarks compile as external crate consumers and cannot access `pub(crate)` items. We solve this with two complementary mechanisms:

### 1. Change method/constructor visibility behind the feature flag

The `pub use` re-export trick only makes type *paths* visible -- it does not change the visibility of methods or constructors. The actual fix is to conditionally change `pub(crate)` to `pub` on each method that benchmarks need to call. The pattern is:

```rust
// Before:
pub(crate) fn new(...) -> Self { ... }

// After:
#[cfg(feature = "bench-internals")]
pub fn new(...) -> Self { ... }
#[cfg(not(feature = "bench-internals"))]
pub(crate) fn new(...) -> Self { ... }
```

**Methods that must be changed to `pub` under `bench-internals`:**

In `src/execution/stream.rs`:
- `Record::new(field_names, data)` (line 19)
- `Record::to_tuples()` (line 66) -- needed to inspect results
- `InMemoryStream::new(data)` (line 262)
- `MapStream::new(named_list, variables, source, registry)` (line 130)
- `FilterStream::new(formula, variables, source, registry)` (line 228)
- `GroupByStream::new(keys, variables, aggregates, source, registry)` (line 289)
- `LimitStream::new(row_count, source)` (line 190)
- The `RecordStream` trait itself (line 117) and its `next()` method

In `src/execution/datasource.rs`:
- `ReaderBuilder::new(file_format)` (line 620)
- `ReaderBuilder::with_reader(rdr)` (line 632)
- The `RecordRead` trait (line 615) and its `read_record()` method

In `src/execution/types.rs`:
- `Named` enum (line 296)
- `Expression` enum (line 124) and `Expression::Constant` variant
- `Expression::Variable` variant
- `Formula` enum (line 302)
- `Relation` enum (line 233)
- `Ordering` enum (line 118)
- `NamedAggregate` struct
- `Aggregate` enum (line 685)
- `Node` enum (line 455)
- `StreamResult` type alias (line 55)

In `src/syntax/parser.rs`:
- `query()` function (line 947)

### 2. Re-export for path convenience in `lib.rs`

Add a feature-gated module to `src/lib.rs` for convenient import paths:

```rust
#[cfg(feature = "bench-internals")]
pub mod bench_internals {
    // Parser
    pub use crate::syntax::parser::query;

    // Stream types
    pub use crate::execution::stream::{
        Record, RecordStream, InMemoryStream,
        MapStream, FilterStream, GroupByStream, LimitStream,
    };

    // Execution plan types
    pub use crate::execution::types::{
        Named, Formula, Expression, Relation, Node,
        NamedAggregate, Aggregate, Ordering, StreamResult,
    };

    // Datasource
    pub use crate::execution::datasource::{ReaderBuilder, RecordRead};

    // Common types (already pub, but convenient re-export)
    pub use crate::common::types::{Value, Variables, VariableName, DataSource};
}
```

This module provides a single import path (`use logq::bench_internals::*`) for benchmark files. The visibility fix (mechanism 1 above) is what actually makes the methods callable; this re-export just shortens the import paths.

### 3. New `app::run_to_records` function for Tier A benchmarks

The existing `app::run_to_vec` (app.rs:208-231) is gated behind `#[cfg(test)]` and returns `Vec<Vec<(String, Value)>>`. For benchmarks, add a similar function gated behind `bench-internals`:

```rust
#[cfg(feature = "bench-internals")]
pub fn run_to_records(
    query_str: &str,
    data_source: common::types::DataSource,
) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
    // Same implementation as run_to_vec
    let (rest_of_str, q) = syntax::parser::query(&query_str)?;
    if !rest_of_str.is_empty() {
        return Err(AppError::InputNotAllConsumed(rest_of_str.to_string()));
    }
    let q = syntax::desugar::desugar_query(q);
    let registry = Arc::new(functions::register_all()?);
    let node = logical::parser::parse_query_top(q, data_source.clone(), registry.clone())?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new(data_source);
    let (physical_plan, variables) = node.physical(&mut physical_plan_creator)?;
    let mut stream = physical_plan.get(variables, registry)?;
    let mut results = Vec::new();
    while let Some(record) = stream.next()? {
        results.push(record.to_tuples());
    }
    Ok(results)
}
```

This runs the full pipeline (parse -> plan -> execute -> collect) without writing to stdout, making it suitable for Criterion measurement. No stdout I/O means the benchmark measures actual query processing time, not terminal output.

## 1. Parser Benchmarks -- Graduated Ladder

Six queries scaling in complexity, each benchmarked independently to show how parse time grows. All queries have been verified against the parser source to ensure they parse successfully.

```
Level 1: "SELECT a FROM t"
Level 2: "SELECT a, b, c FROM t WHERE a > 10"
Level 3: "SELECT a, count(b) FROM t WHERE a > 10 GROUP BY a ORDER BY a DESC LIMIT 100"
Level 4: "SELECT a, sum(b) FROM t WHERE a > 10 AND c LIKE \"%foo%\" GROUP BY a HAVING sum(b) > 100 ORDER BY a"
Level 5: "SELECT a, CASE WHEN b > 10 THEN \"high\" WHEN b > 5 THEN \"mid\" ELSE \"low\" END AS tier, count(*) FROM t AS t1 LEFT JOIN t AS t2 ON t1.id = t2.id WHERE a BETWEEN 1 AND 100 GROUP BY a, tier"
Level 6: "SELECT a, b FROM t WHERE a IN (10, 20, 30) AND b IS NOT NULL UNION SELECT c, d FROM t2 ORDER BY a LIMIT 50"
```

Each level adds meaningful parser features:
- **Level 1**: Trivial baseline -- SELECT + FROM
- **Level 2**: WHERE + multi-column SELECT
- **Level 3**: Aggregates + ORDER BY + LIMIT
- **Level 4**: HAVING + LIKE (with double-quoted pattern) + compound predicates (AND)
- **Level 5**: CASE WHEN (multi-branch) + LEFT JOIN ... ON + BETWEEN
- **Level 6**: IN (value list) + IS NOT NULL + UNION + ORDER BY + LIMIT

**Verification against parser source:**

- **String literals**: All use double-quoted strings. The parser's `value()` (parser.rs:190-191) only recognizes `double_quote_string_literal`. Verified by checking that single-quoted strings are only used for tuple constructor keys (parser.rs:817), not as general values.
- **LEFT JOIN**: Parsed by `left_join_clause` (parser.rs:518-533) which handles `LEFT [OUTER] JOIN <table_ref> ON <expr>`. Level 5 uses table aliases (`t AS t1`, `t AS t2`) to avoid ambiguity.
- **IN (value list)**: `parse_postfix_in` (parser.rs:700-748) parses `(` then a comma-separated expression list then `)`. Level 6 uses `IN (10, 20, 30)` which matches this pattern.
- **IS NOT NULL**: Parsed by `parse_postfix_is` (confirmed by parser.rs:785 calling it and tests at line 1893+).
- **UNION**: Parsed by `query()` (parser.rs:947-969) which chains `select_query` calls with `set_operator`.
- **HAVING**: Parsed by `having_expression` (parser.rs:535-536).
- **CASE WHEN**: Parsed by `case_when_expression` (parser.rs:45+).
- **BETWEEN**: Parsed by `parse_postfix_between` (parser.rs:751+).
- **LIKE**: Parsed by `parse_postfix_like` (parser.rs:692-696).

The benchmark calls `syntax::parser::query()` (re-exported via `bench_internals::query`), which is the top-level parser entry point. Uses `criterion::black_box` to prevent dead-code elimination. No file I/O or execution -- pure parse cost.

## 2. Execution Benchmarks -- Small + Synthetic Data

### Tier A -- Existing Data (Regression Guard)

Run 3 representative queries against `data/AWSELB.log` (668 lines) end-to-end through the full pipeline (parse SQL -> logical plan -> physical plan -> stream execution -> collect results):

```
E1: "SELECT * FROM elb LIMIT 10"              -- minimal scan
E2: "SELECT elbname, count(*) FROM elb GROUP BY elbname"  -- full scan + aggregation
E3: "SELECT elbname, elb_status_code FROM elb WHERE elb_status_code = \"200\" ORDER BY elbname"  -- filter + sort
```

These benchmarks use `app::run_to_records` (the new `bench-internals`-gated function described in the Visibility section), which runs the full pipeline and collects results into `Vec<Vec<(String, Value)>>` without writing to stdout. This avoids I/O noise that would dominate measurement for small queries like E1 (`LIMIT 10`).

Note: E3 uses `elb_status_code = "200"` with a double-quoted string literal, matching the parser's requirement and consistent with existing tests (e.g., app.rs:1096).

These are end-to-end integration benchmarks: any parser or planner bug will cause benchmark failure, not just slow numbers.

### Tier B -- Synthetic Data (Throughput Characterization)

Generate in-memory `InMemoryStream` datasets at 1K, 10K, and 100K rows with 5 columns (2 string, 1 int, 1 float, 1 boolean). Benchmark these operators in isolation:

```
S1: MapStream (SELECT projection) -- per-row column extraction
S2: FilterStream (WHERE predicate) -- predicate evaluation throughput
S3: GroupByStream (GROUP BY + count) -- hashing + aggregation
S4: LimitStream (LIMIT 100) -- baseline, should be near-constant regardless of input size
```

Each synthetic benchmark constructs the stream operator directly via `bench_internals` re-exports, bypassing parsing, to isolate execution cost. The three data sizes (1K, 10K, 100K) verify algorithmic complexity by plotting throughput across sizes (inspect manually -- Criterion does not automatically verify complexity classes).

**Criterion configuration for large benchmarks**: The 100K-row benchmarks use `.measurement_time(Duration::from_secs(10))` and `.sample_size(20)` to avoid excessively long CI runs while still producing statistically meaningful results. The 1K and 10K sizes use Criterion defaults (5s warm-up, 5s measurement, 100 samples).

### Limitation: No isolated ORDER BY benchmark

ORDER BY is implemented inline inside `Node::OrderBy` (`src/execution/types.rs:515-559`): it materializes all records into a `Vec`, calls `Vec::sort_by`, then wraps the result in `InMemoryStream`. There is no standalone `SortStream` type. Benchmarking sort in isolation would require either:

1. Extracting the sort logic into its own stream type (a code change beyond the benchmark scope), or
2. Benchmarking through `Node::OrderBy.get()`, which couples to the planning layer and `Variables`/`FunctionRegistry` setup.

For now, sort cost is covered indirectly by Tier A benchmark E3 (filter + ORDER BY end-to-end). If sort performance becomes a concern, extracting a `SortStream` as a future refactoring would enable isolated benchmarking.

## 3. Datasource Benchmarks -- Parse Throughput

One benchmark per log format, measuring how fast raw text lines convert into `Record` structs:

```
D1: ELB   -- data/AWSELB.log (668 lines, 17 fields, includes DateTime/Float/Host typed parsing)
D2: ALB   -- data/AWSALB.log (7 lines -> replicated to 1001 lines, 25 fields, widest schema)
D3: S3    -- data/S3.log (5 lines -> replicated to 1000 lines, 24 fields, all-string schema)
D4: Squid -- data/Squid.log (10 lines -> replicated to 1000 lines, 10 fields, narrowest schema)
D5: JSONL -- data/structured.log (3 lines -> replicated to 1002 lines, arbitrary JSON fields)
```

For formats with small source files (ALB, S3, Squid, JSONL), the benchmark replicates lines in memory to reach at least 1000 lines. This is done by cycling through the original `Vec<String>` of lines and pushing copies until the target count is reached: `ceil(1000 / source_lines) * source_lines`. The ELB file at 668 lines is used as-is (already sufficient for reliable measurement).

The benchmark constructs a `ReaderBuilder` (via `bench_internals::ReaderBuilder`) and calls `with_reader()` on a `BufReader::new(concatenated_lines.as_bytes())`, then calls `read_record()` in a loop until exhaustion. This exercises the full per-line parsing path: the regex tokenizer (module-level lazy_static `SPLIT_READER_LINE_REGEX`), per-format field construction, and type coercion into `Value` variants. No SQL parsing, no execution operators.

Pre-load all lines into a `Vec<String>`, concatenate them with newlines, and iterate within the measurement loop, avoiding filesystem noise. Report **lines/second** using `Criterion::Throughput::Elements(line_count)`.

**Note on line replication**: Replicated lines repeat the same content. The regex engine may benefit from branch prediction warming that would not occur with diverse real-world data. This is acceptable for regression detection but should be noted when interpreting absolute throughput numbers.

## 4. UDF Benchmarks -- One Per Category

Six representative functions benchmarked with pre-constructed `Value` arguments:

```
U1: upper("hello world")                                   -- String category (string.rs)
U2: round(3.14159, 2)                                      -- Arithmetic category (arithmetic.rs)
U3: date_part("month", <fixed DateTime 2024-06-15T10:30:00Z>)  -- DateTime category (datetime.rs)
U4: array_contains([1,2,3,4,5], 3)                         -- Array category (array.rs)
U5: map_keys({"a":1, "b":2})                               -- Map category (map.rs)
U6: regexp_like("foo123", "\d+")                            -- Regex category (regexp.rs)
```

Function choices rationale:
- **U1 (`upper`)**: Simple string transformation, user-facing, single argument.
- **U2 (`round`)**: User-facing arithmetic function with `Arity::Range(1, 2)`, exercises real numeric computation (multiply, round, divide). Replaces the internal-only `Plus` operator-dispatch function.
- **U3 (`date_part`)**: Deterministic 2-arg datetime function. Takes a pre-constructed `Value::DateTime` and a string unit, exercises string matching on the date part unit + chrono field extraction. Replaces non-deterministic `now()` and non-existent `date_format`.
- **U4 (`array_contains`)**: Array membership test, exercises `Value::Array` iteration.
- **U5 (`map_keys`)**: Map introspection, exercises `Value::Object` key extraction.
- **U6 (`regexp_like`)**: Regex matching with cached compilation. The codebase has a thread-local LRU cache (capacity 64) at `regexp.rs:10-14` via `get_or_compile_regex()`. With the benchmark using a fixed pattern `"\d+"`, the regex is compiled once on the first call and cached for all subsequent iterations. **The benchmark measures steady-state performance: cached LRU lookup + regex matching**, not regex compilation cost. This is the realistic hot path for repeated queries.

Each benchmark calls `registry.call("upper", &args)` to measure the real-world hot path including HashMap lookup (case-insensitive via `to_ascii_lowercase()`), arity validation, null handling, and dispatch.

The registry is constructed once in a setup block via `functions::register_all()` with all default functions registered. Arguments are pre-allocated as `Vec<Value>` outside the measurement loop.

Skipped categories: `host.rs`, `url.rs`, `bitwise.rs`, `json.rs`, `type_conversion.rs` -- adding benchmarks later follows the same pattern.

## Implementation Notes

- **Framework**: Criterion 0.3 (already in `dev-dependencies`). An upgrade to 0.5 would improve HTML reports and provide `BenchmarkGroup` improvements, but is not required for initial implementation and can be done later without changing the benchmark structure.
- **Cargo.toml**: Replace the existing single `[[bench]]` entry (`logq_benchmark`) with four separate `[[bench]]` targets. Add the `bench-internals` feature.
- **Data generation**: `helpers/synthetic.rs` uses `rand` (already in `dev-dependencies`) to generate reproducible datasets with a fixed seed (`StdRng::seed_from_u64(42)`). The generated `Record` instances use `InMemoryStream` wrapped in `VecDeque<Record>`.
- **Visibility**: Solved via conditional compilation (`#[cfg(feature = "bench-internals")]`) on individual methods/constructors plus `pub use` re-exports in `lib.rs` for path convenience. The feature is not a default feature, so normal `cargo build` and crate consumers are unaffected.
- **Existing benchmark**: The trivial stub at `benches/logq_benchmark.rs` (which just does `1 + 1`) will be removed and replaced by the four new benchmark files.

## Known Limitations

1. **No isolated ORDER BY benchmark**: As discussed in Section 2, `SortStream` does not exist. Sort cost is only measured as part of end-to-end pipelines.
2. **Complexity verification is manual**: The 1K/10K/100K data sizes let you inspect throughput scaling across sizes, but Criterion does not automatically verify algorithmic complexity (e.g., O(n) vs O(n log n)). This requires manual inspection of the generated plots.
3. **Datasource line replication**: Replicated lines for small formats (ALB, S3, Squid, JSONL) repeat the same content. This means the regex engine may benefit from branch prediction warming that would not occur with diverse real-world data. This is acceptable for regression detection but should be noted when interpreting absolute throughput numbers.
4. **No "direct call" UDF benchmark**: `FunctionRegistry` does not expose a `get()` or `lookup()` method -- the `functions` HashMap is private (registry.rs:48). All UDF benchmarks go through `registry.call()`, which includes dispatch overhead. This is the realistic hot path.
