# Microbenchmarks Design for logq (v2)

## Changes from previous version

This revision addresses all critical issues and incorporates several suggestions from the design review:

- **C1 (Parser entry point)**: Changed from `select_query()` to `query()` as the parser entry point. `query()` is the real top-level entry point that handles set operations (UNION/INTERSECT/EXCEPT) by chaining `select_query` calls internally, so it correctly parses all six ladder levels including Level 6's UNION.
- **C2 (No SortStream)**: Removed the standalone `SortStream` benchmark (S4). ORDER BY is implemented inline inside `Node::OrderBy` in `src/execution/types.rs` -- it materializes all records, calls `Vec::sort_by`, and wraps the result in `InMemoryStream`. There is no standalone sort stream type. Added a note in the limitations section. Sort cost is still covered indirectly by Execution Tier A benchmark E3 (which includes ORDER BY in its end-to-end pipeline).
- **C3 (Visibility mechanism)**: Replaced the vague "make targeted items pub or use #[cfg(test)] re-exports" with a concrete mechanism: a Cargo feature `bench-internals` that gates `pub` re-exports from `lib.rs`. Specified exactly which types need re-exporting and the feature-gated module structure.
- **C4 (Arithmetic function)**: Replaced `plus(42, 17)` with `round(3.14159, 2)`. `Plus` is an internal operator-dispatch function (used for `+` expressions), not a user-facing function. `round` is a user-facing function with the more interesting `Arity::Range(1, 2)` signature and real numeric computation (multiply, round, divide).
- **C5 (DateTime function)**: Replaced `now()` / `date_format` with `date_part("month", <fixed_timestamp>)`. `now()` is non-deterministic (syscall to get wall-clock time), producing noisy measurements. `date_format` does not exist in the codebase. `date_part` is a deterministic 2-arg function that extracts components from a pre-constructed `Value::DateTime`, exercising string matching on the unit + chrono field extraction.
- **S2 (Small-file replication)**: Datasource benchmarks now replicate small-file lines in memory to reach at least 1000 lines per format, producing meaningful throughput numbers.
- **S4 (Criterion measurement config)**: Added explicit Criterion configuration for large synthetic benchmarks: `.measurement_time(Duration::from_secs(10))` and `.sample_size(20)` for 100K-row benchmarks to avoid excessively long runs.
- **S7 (Multiple [[bench]] pattern)**: Adopted the simpler multiple-`[[bench]]` pattern instead of a single `bench_main.rs` entry point. Each benchmark file is its own Cargo target, reducing coupling. Shared helpers are in a `benches/helpers/` module imported by each file.

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

Run individual groups: `cargo bench --bench bench_parser` or all: `cargo bench`.

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

Run benchmarks with: `cargo bench --features bench-internals`

The `bench_udf` target does not require `bench-internals` because it only uses publicly exported types from `logq::functions` (which is already `pub`). The parser benchmark accesses `syntax::parser::query()` which requires the feature gate since `query()` is `pub(crate)`. The execution and datasource benchmarks access `pub(crate)` stream operators, `Record`, `ReaderBuilder`, etc.

## Visibility: The `bench-internals` Cargo Feature

Criterion benchmarks compile as external crate consumers and cannot access `pub(crate)` items. We solve this with a Cargo feature `bench-internals` that gates `pub` re-exports.

### Types that need re-exporting

From `src/execution/stream.rs`:
- `Record`
- `RecordStream` (trait)
- `InMemoryStream`
- `MapStream`, `FilterStream`, `GroupByStream`, `LimitStream`

From `src/execution/types.rs`:
- `Named`, `Formula`, `Expression`, `Node`, `NamedAggregate`, `Aggregate`, `Ordering`
- `StreamResult`

From `src/execution/datasource.rs`:
- `ReaderBuilder`
- `RecordRead` (trait)
- `Reader<R>`

From `src/syntax/parser.rs`:
- `query()` function

### Implementation in `lib.rs`

Add a feature-gated module to `src/lib.rs`:

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
        Named, Formula, Expression, Node,
        NamedAggregate, Aggregate, Ordering, StreamResult,
    };

    // Datasource
    pub use crate::execution::datasource::{ReaderBuilder, RecordRead, Reader};

    // Common types (already pub, but convenient re-export)
    pub use crate::common::types::{Value, Variables, VariableName, DataSource};
}
```

This keeps the public API surface unchanged for normal consumers (`bench-internals` is not a default feature) while giving benchmarks full access.

## 1. Parser Benchmarks -- Graduated Ladder

Six queries scaling in complexity, each benchmarked independently to show how parse time grows:

```
Level 1: "SELECT a FROM t"
Level 2: "SELECT a, b, c FROM t WHERE a > 10"
Level 3: "SELECT a, count(b) FROM t WHERE a > 10 GROUP BY a ORDER BY a DESC LIMIT 100"
Level 4: "SELECT a, sum(b) FROM t WHERE a > 10 AND c LIKE '%foo%' GROUP BY a HAVING sum(b) > 100 ORDER BY a"
Level 5: "SELECT a, CASE WHEN b > 10 THEN 'high' WHEN b > 5 THEN 'mid' ELSE 'low' END AS tier, count(*) FROM t JOIN t2 ON t.id = t2.id WHERE a BETWEEN 1 AND 100 GROUP BY a, tier"
Level 6: "SELECT a, b FROM t WHERE a IN (SELECT x FROM t2 WHERE x > 10) UNION SELECT c, d FROM t3 WHERE c IS NOT NULL ORDER BY a LIMIT 50"
```

Each level adds meaningful parser features:
- **Level 1**: Trivial baseline
- **Level 2**: WHERE + multi-column SELECT
- **Level 3**: Aggregates + ORDER BY + LIMIT
- **Level 4**: HAVING + LIKE + compound predicates
- **Level 5**: CASE WHEN + JOIN + BETWEEN
- **Level 6**: Subquery + UNION + IS NULL

The benchmark calls `syntax::parser::query()` (re-exported via `bench_internals::query`), which is the top-level parser entry point. It handles set operations (UNION/INTERSECT/EXCEPT) by chaining `select_query` calls internally, so all six levels parse correctly through the same code path. Uses `criterion::black_box` to prevent dead-code elimination. No file I/O or execution -- pure parse cost.

## 2. Execution Benchmarks -- Small + Synthetic Data

### Tier A -- Existing Data (Regression Guard)

Run 3 representative queries against `data/AWSELB.log` (668 lines) end-to-end through the full pipeline (parse SQL -> logical plan -> physical plan -> stream execution -> collect results):

```
E1: "SELECT * FROM elb LIMIT 10"              -- minimal scan
E2: "SELECT elbname, count(*) FROM elb GROUP BY elbname"  -- full scan + aggregation
E3: "SELECT elbname, elb_status_code FROM elb WHERE elb_status_code = '200' ORDER BY elbname"  -- filter + sort
```

These use real files and the existing `app.rs` pipeline (`app::run` or equivalent), catching regressions in the integrated path. Note that these are end-to-end integration benchmarks: any parser or planner bug will cause benchmark failure, not just slow numbers.

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

For formats with small source files (ALB, S3, Squid, JSONL), the benchmark replicates lines in memory to reach at least 1000 lines. This is done by cycling through the original `Vec<String>` of lines and pushing copies until the target count is reached. The ELB file at 668 lines is used as-is (already sufficient for reliable measurement).

The benchmark constructs a `ReaderBuilder` (via `bench_internals::ReaderBuilder`) and calls `with_reader()` on a `BufReader::new(concatenated_lines.as_bytes())`, then calls `read_record()` in a loop until exhaustion. This exercises the full per-line parsing path: the regex tokenizer (module-level lazy_static `SPLIT_READER_LINE_REGEX`), per-format field construction, and type coercion into `Value` variants. No SQL parsing, no execution operators.

Pre-load all lines into a `Vec<String>`, concatenate them with newlines, and iterate within the measurement loop, avoiding filesystem noise. Report **lines/second** using `Criterion::Throughput::Elements(line_count)`.

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
- **U6 (`regexp_like`)**: Regex compilation + matching, exercises the regex category.

Each benchmark does two things:

1. **Registry lookup + call** -- calls `registry.call("round", &args)` to measure the real-world hot path including HashMap lookup (case-insensitive via `to_ascii_lowercase()`), arity validation, null handling, and dispatch.
2. **Direct call** -- invokes the closure directly via a pre-resolved `FunctionDef` reference, to separate dispatch overhead from actual computation.

The registry is constructed once in a setup block via `functions::register_all()` with all default functions registered. Arguments are pre-allocated as `Vec<Value>` outside the measurement loop.

Skipped categories: `host.rs`, `url.rs`, `bitwise.rs`, `json.rs`, `type_conversion.rs` -- adding benchmarks later follows the same pattern.

## Implementation Notes

- **Framework**: Criterion 0.3 (already in `dev-dependencies`). An upgrade to 0.5 would improve HTML reports and provide `BenchmarkGroup` improvements, but is not required for initial implementation and can be done later without changing the benchmark structure.
- **Cargo.toml**: Replace the existing single `[[bench]]` entry (`logq_benchmark`) with four separate `[[bench]]` targets. Add the `bench-internals` feature.
- **Data generation**: `helpers/synthetic.rs` uses `rand` (already in `dev-dependencies`) to generate reproducible datasets with a fixed seed (`StdRng::seed_from_u64(42)`). The generated `Record` instances use `InMemoryStream` wrapped in `VecDeque<Record>`.
- **Visibility**: Solved via the `bench-internals` Cargo feature as described above. The feature is not a default feature, so normal `cargo build` and crate consumers are unaffected.
- **Existing benchmark**: The trivial stub at `benches/logq_benchmark.rs` (which just does `1 + 1`) will be removed and replaced by the four new benchmark files.

## Known Limitations

1. **No isolated ORDER BY benchmark**: As discussed in Section 2, `SortStream` does not exist. Sort cost is only measured as part of end-to-end pipelines.
2. **Complexity verification is manual**: The 1K/10K/100K data sizes let you inspect throughput scaling across sizes, but Criterion does not automatically verify algorithmic complexity (e.g., O(n) vs O(n log n)). This requires manual inspection of the generated plots.
3. **Regex compilation amortization**: The `regexp_like` benchmark may overstate per-call cost because the regex is compiled on every invocation in the current implementation. If regex caching is added later, benchmark numbers will improve and the benchmark will correctly capture that improvement.
4. **Datasource line replication**: Replicated lines for small formats (ALB, S3, Squid, JSONL) repeat the same content. This means the regex engine may benefit from branch prediction warming that would not occur with diverse real-world data. This is acceptable for regression detection but should be noted when interpreting absolute throughput numbers.
