# Microbenchmarks Design for logq

## Goals

Comprehensive microbenchmark suite covering four areas: parser, execution engine, datasource readers, and user-defined functions. Serves three purposes:

1. **Regression guard** — catch performance regressions from refactors
2. **Optimization guide** — identify hotspots worth optimizing
3. **Comparison baseline** — before/after numbers for the PartiQL completion phases

## File Structure

```
benches/
  bench_main.rs          # criterion_main! entry point, imports all groups
  bench_parser.rs        # Parser graduated ladder
  bench_execution.rs     # Execution with small + synthetic data
  bench_datasource.rs    # Per-format parse throughput
  bench_udf.rs           # One function per category
  helpers/
    mod.rs
    synthetic.rs         # Synthetic data generation (1K/10K/100K rows)
    queries.rs           # Shared query strings for parser + execution
```

Each `bench_*.rs` file defines a `criterion_group!` with its benchmarks. `bench_main.rs` wires them together via `criterion_main!(parser, execution, datasource, udf)`. The `helpers/` module provides reusable synthetic data generators and query constants shared across parser and execution benchmarks.

Criterion's `--bench` flag lets you run a single group: `cargo bench -- parser` vs `cargo bench` for all.

## 1. Parser Benchmarks — Graduated Ladder

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

The benchmark calls the nom parser's `select_query` entry point directly, using `criterion::black_box` to prevent dead-code elimination. No file I/O or execution — pure parse cost.

## 2. Execution Benchmarks — Small + Synthetic Data

### Tier A — Existing Data (Regression Guard)

Run 3 representative queries against `data/AWSELB.log` (668 lines) end-to-end through the full pipeline (parse SQL -> logical plan -> physical plan -> stream execution -> collect results):

```
E1: "SELECT * FROM elb LIMIT 10"              — minimal scan
E2: "SELECT elbname, count(*) FROM elb GROUP BY elbname"  — full scan + aggregation
E3: "SELECT elbname, elb_status_code FROM elb WHERE elb_status_code = '200' ORDER BY elbname"  — filter + sort
```

These use real files and the existing `app.rs` pipeline, catching regressions in the integrated path.

### Tier B — Synthetic Data (Throughput Characterization)

Generate in-memory `InMemoryStream` datasets at 1K, 10K, and 100K rows with 5 columns (2 string, 1 int, 1 float, 1 boolean). Benchmark these operators in isolation:

```
S1: MapStream (SELECT projection) — per-row column extraction
S2: FilterStream (WHERE predicate) — predicate evaluation throughput
S3: GroupByStream (GROUP BY + count) — hashing + aggregation
S4: SortStream (ORDER BY) — sort cost, should show O(n log n) scaling
S5: LimitStream (LIMIT 100) — baseline, should be near-constant regardless of input size
```

Each synthetic benchmark constructs the stream operator directly, bypassing parsing, to isolate execution cost. The three data sizes verify algorithmic complexity.

## 3. Datasource Benchmarks — Parse Throughput

One benchmark per log format, measuring how fast raw text lines convert into `Record` structs:

```
D1: ELB   — data/AWSELB.log (668 lines, 17 fields, includes DateTime/Float/Host typed parsing)
D2: ALB   — data/AWSALB.log (7 lines, 25 fields, widest schema)
D3: S3    — data/S3.log (5 lines, 24 fields, all-string schema)
D4: Squid — data/Squid.log (10 lines, 10 fields, narrowest schema)
D5: JSONL — data/structured.log (3 lines, arbitrary JSON fields)
```

The benchmark calls the datasource's line-parsing logic directly — the `SplitReader` regex tokenizer plus per-format field construction and type coercion into `Value` variants. No SQL parsing, no execution operators.

For meaningful throughput on small files, pre-load all lines into a `Vec<String>` and iterate within the measurement loop, avoiding filesystem noise. Report **lines/second** using `Criterion::Throughput::Elements`.

## 4. UDF Benchmarks — One Per Category

Six representative functions benchmarked with pre-constructed `Value` arguments:

```
U1: upper("hello world")              — String category (string.rs)
U2: plus(42, 17)                      — Arithmetic category (arithmetic.rs)
U3: now() or date_format(timestamp)   — DateTime category (datetime.rs)
U4: array_contains([1,2,3,4,5], 3)   — Array category (array.rs)
U5: map_keys({"a":1, "b":2})         — Map category (map.rs)
U6: regexp_like("foo123", "\d+")     — Regex category (regexp.rs)
```

Each benchmark does two things:

1. **Registry lookup + call** — calls `registry.call(name, &args)` to measure the real-world hot path including HashMap lookup and arity validation.
2. **Direct call** — invokes the closure directly via a pre-resolved `FunctionDef` reference, to separate dispatch overhead from actual computation.

The registry is constructed once in a setup block with all default functions registered. Arguments are pre-allocated outside the measurement loop.

Skipped categories: `host.rs`, `url.rs`, `bitwise.rs`, `json.rs`, `type_conversion.rs` — adding benchmarks later follows the same pattern.

## Implementation Notes

- **Framework**: Criterion 0.3 (already in `dev-dependencies`)
- **Cargo.toml**: Replace the single `[[bench]]` entry with the new `bench_main` target
- **Data generation**: `helpers/synthetic.rs` uses `rand` (already in `dev-dependencies`) to generate reproducible datasets with a fixed seed
- **Visibility**: Some internal types (`Record`, `RecordStream`, stream operators) are `pub(crate)`. Benchmarks live outside the crate, so we'll need to either make targeted items `pub` or use `#[cfg(test)]` re-exports via `lib.rs`
