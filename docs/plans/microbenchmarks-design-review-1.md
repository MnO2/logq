VERDICT: NEEDS_REVISION

## Summary Assessment

The design is well-structured and covers the right surface area, but it has several critical issues around visibility barriers that will block implementation, incorrect assumptions about what types and operators exist in the codebase, and a parser entry point mismatch. The overall architecture (Criterion groups, graduated ladder, synthetic data tiers) is sound and appropriate for a project of this size.

## Critical Issues (must fix)

### C1. Parser entry point is `query`, not `select_query`

The design says each parser benchmark "calls the nom parser's `select_query` entry point directly." However, `select_query` only parses a single SELECT statement. Levels 5-6 use JOINs, UNION, and subqueries, which are handled by `query()` (defined at `src/syntax/parser.rs:947`). `query()` is the real top-level entry point that handles set operations (UNION/INTERSECT/EXCEPT) by chaining `select_query` calls. The benchmarks should use `query()` for consistency and correctness, especially for Level 6 which contains `UNION`.

### C2. No `SortStream` exists -- ORDER BY is inline in `Node::OrderBy`

The design proposes benchmarking "S4: SortStream (ORDER BY) -- sort cost" as an isolated stream operator. There is no `SortStream` struct anywhere in the codebase. Sorting is done inline inside `Node::OrderBy` in `src/execution/types.rs:515-559`: it materializes all records into a `Vec`, calls `.sort_by()`, then wraps the result in an `InMemoryStream`. This means:

1. You cannot construct a `SortStream` in isolation -- it does not exist.
2. To benchmark sort in isolation, you would need to either (a) extract the sort logic into its own stream type first, or (b) benchmark through the `Node::OrderBy` path, which couples to the planning layer.

The design must either acknowledge this and propose creating `SortStream`, or replace S4 with a benchmark through `Node::OrderBy.get()`.

### C3. Visibility wall: nearly everything is `pub(crate)`

The design acknowledges this in its "Implementation Notes" section but underestimates the severity. Almost every type needed for the synthetic execution benchmarks (Tier B) is `pub(crate)`:

- `Record`, `RecordStream`, `InMemoryStream`, `MapStream`, `FilterStream`, `GroupByStream`, `LimitStream` (all in `stream.rs`)
- `Named`, `Formula`, `Expression`, `Relation`, `Node`, `NamedAggregate`, `Aggregate`, `Ordering` (all in `types.rs`)
- `RecordRead` trait (in `datasource.rs`)

Criterion benchmarks compile as external crate consumers. They cannot access `pub(crate)` items at all. The design waves this away with "we'll need to either make targeted items `pub` or use `#[cfg(test)]` re-exports via `lib.rs`." This needs a concrete decision because:

- `#[cfg(test)]` re-exports do **not** work for benchmarks. `cfg(test)` is only active for `cargo test`, not `cargo bench`. You would need `#[cfg(any(test, feature = "bench-internals"))]` with a custom Cargo feature, or similar.
- Making items `pub` changes the crate's public API surface, which has downstream implications (semver, documentation).

The design must specify the exact mechanism. Recommended: add a Cargo feature `bench-internals` that gates `pub` re-exports.

### C4. Arithmetic function name is `"Plus"` (capitalized), not `"plus"`

The design proposes benchmarking `U2: plus(42, 17)`. The actual function is registered as `"Plus"` (capital P) in `src/functions/arithmetic.rs:10`. Due to case-insensitive lookup (`to_ascii_lowercase()`), calling `registry.call("plus", ...)` would work, but calling `registry.call("Plus", ...)` would also work. This is minor but the design should use the canonical registered name to avoid confusion. More importantly, `Plus` is an operator-level function (used internally for `+` expressions), not a user-facing function like `upper` or `array_contains`. The benchmark would be more representative if it used a user-facing arithmetic-adjacent function, or the design should clarify that it is intentionally benchmarking the internal operator dispatch path.

### C5. `now()` returns wall-clock time -- non-deterministic benchmark

The design proposes `U3: now() or date_format(timestamp)`. The `now` function (registered at `src/functions/datetime.rs:351`) returns `chrono::Utc::now()`, which is non-deterministic. Criterion runs the function many times and measures timing statistics, but the function itself has side effects (syscall to get time). This will produce noisy measurements because each invocation does real clock work. `date_format` does not appear to exist in the datetime module. The design should pick a deterministic datetime function instead (e.g., `date_add`, `date_diff`, `to_timestamp` if any exist, or `year`/`month`/`day` extraction functions).

## Suggestions (nice to have)

### S1. Criterion 0.3 is old; consider 0.5

The Cargo.toml pins `criterion = "0.3"`. Criterion 0.5 has significantly better HTML reports, `criterion::BenchmarkGroup` improvements, and better async support. Since you are setting up benchmarks from scratch, upgrading now avoids migration later.

### S2. Datasource benchmarks on tiny files may produce misleading numbers

The ALB file is 7 lines, S3 is 5 lines, Squid is 10 lines, JSONL is 3 lines. Even with the pre-load-into-Vec strategy, Criterion's measurement overhead may dominate. Consider either (a) duplicating the lines in-memory to reach at least 1000 lines per format, or (b) using `iter_batched` with a batch of concatenated lines. The AWSELB file at 668 lines is the only one with enough data for reliable throughput measurement.

### S3. Execution Tier A queries reference `elbname` and `elb_status_code` -- verify these parse correctly

Query E3 filters `WHERE elb_status_code = '200'`. In the ELB schema, `elb_status_code` is typed as `DataType::String` (position 7), so comparing with a string literal `'200'` is correct. Query E2 groups by `elbname` which is also `DataType::String`. These should work, but the design should note that these are end-to-end integration benchmarks and any parser or planner bug will cause benchmark failure (not just slow numbers).

### S4. Consider adding a warm-up and measurement time configuration

The design does not specify Criterion configuration. For small benchmarks (parser, UDF), the default 5-second warm-up + 5-second measurement is fine. For the 100K-row synthetic benchmarks, a single iteration may take significant time. Consider specifying `.measurement_time(Duration::from_secs(10))` and `.sample_size(20)` for the larger benchmarks to avoid excessively long CI runs.

### S5. Missing SortStream means S4 cannot verify O(n log n) scaling claim

Even setting aside the non-existence of SortStream (Critical C2), the design claims "S4 should show O(n log n) scaling." Criterion does not automatically verify algorithmic complexity. You would need to plot throughput across 1K/10K/100K data points and inspect manually. This is fine as a goal but the design reads as if it is an automated assertion.

### S6. Consider benchmarking `ReaderBuilder::with_reader` directly for datasource benchmarks

The design says "the benchmark calls the datasource's line-parsing logic directly -- the `SplitReader` regex tokenizer." There is no `SplitReader` type. The parsing happens inside `Reader<R>::read_record()` (in `datasource.rs`). The regex is a module-level lazy_static `SPLIT_READER_LINE_REGEX`. To benchmark line parsing, use `ReaderBuilder::new(format).with_reader(BufReader::new(line.as_bytes()))` and call `read_record()`. The `with_reader` method exists but is marked `#[allow(dead_code)]` and is `pub(crate)`, so the same visibility issue from C3 applies.

### S7. The `bench_main.rs` multi-file layout is more complex than needed

Criterion supports a simpler pattern where each benchmark file is its own `[[bench]]` target. The multi-file, single-entry-point pattern adds coupling. Since `cargo bench -- parser` filtering works with either approach, consider whether the simpler multiple-`[[bench]]` pattern is better for a project of this size.

## Verified Claims (things I confirmed are correct)

1. **Criterion 0.3 is in dev-dependencies** -- confirmed at `Cargo.toml:38`.
2. **`rand = "0.8"` is in dev-dependencies** -- confirmed at `Cargo.toml:39`.
3. **Data file existence and line counts** -- `data/AWSELB.log` has 668 lines, `data/AWSALB.log` has 7 lines, `data/S3.log` has 5 lines, `data/Squid.log` has 10 lines, `data/structured.log` has 3 lines. All match the design's claims.
4. **ELB has 17 fields** -- confirmed via `ClassicLoadBalancerLogField::len()` returning 17.
5. **ALB has 25 fields** -- confirmed via `ApplicationLoadBalancerLogField::len()` returning 25.
6. **S3 has 24 fields, all-string schema** -- confirmed via `S3Field::len()` returning 24 and `AWS_S3_DATATYPES` being all `DataType::String`.
7. **Squid has 10 fields** -- confirmed via `SquidLogField::len()` returning 10.
8. **`FunctionRegistry::call()` does HashMap lookup + arity-related null checking** -- confirmed in `registry.rs:103-128`.
9. **`InMemoryStream` exists and uses `VecDeque<Record>`** -- confirmed at `stream.rs:257-277`.
10. **`upper`, `array_contains`, `map_keys`, `regexp_like`, `now` all exist as registered functions** -- confirmed in their respective module files.
11. **Current benchmark stub is trivial** (`1 + 1`) -- confirmed at `benches/logq_benchmark.rs`.
12. **`Cargo.toml` excludes `/benches/**` from package** -- confirmed at line 12. This means benchmarks won't ship in the crate but will run locally, which is the correct behavior.
