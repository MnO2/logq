# Plan: Microbenchmarks for logq (v2)

**Goal**: Comprehensive Criterion benchmark suite covering parser, execution, datasource readers, and UDFs, serving as regression guard, optimization guide, and comparison baseline.
**Architecture**: Four separate `[[bench]]` targets gated behind `bench-internals` Cargo feature. Unconditional `pub` visibility on internal types/methods needed by benchmarks (safe for a CLI tool with no library consumers). Helper module for synthetic data generation.
**Tech Stack**: Rust, Criterion 0.3, rand 0.8 (both already in dev-dependencies)

## Changes from previous version

This revision addresses all critical issues and applicable suggestions from the review:

- **C1 (`PathSegment::Field` does not exist)**: Replaced all occurrences of `PathSegment::Field(...)` with `PathSegment::AttrName(...)` in `bench_execution.rs` (Steps 9 S1 and S2 benchmarks). The actual AST variant is `PathSegment::AttrName(String)` (see `src/syntax/ast.rs` line 116).
- **C2 (`PathExpr` and `PathSegment` inaccessible)**: Added `PathExpr` and `PathSegment` to the list of types that need visibility changed from `pub(crate)` to `pub` in `src/syntax/ast.rs` (Step 3). Added both to the `bench_internals` re-export module in `lib.rs` (Step 4).
- **C3 (`DateTime<Utc>` vs `DateTime<FixedOffset>`)**: Changed the `date_part` UDF benchmark in `bench_udf.rs` (Step 7) to use `chrono::DateTime::parse_from_rfc3339("2024-06-15T10:30:00+00:00").unwrap()`, which returns `DateTime<FixedOffset>` matching `Value::DateTime`'s type. Removed the `use chrono::{TimeZone, Utc}` import.
- **C4 (`Variables` and `VariableName` are `pub(crate)`)**: Changed both type aliases from `pub(crate) type` to `pub type` in `src/common/types.rs` (Step 3). These are simple aliases (`String` and `LinkedHashMap<String, Value>`) with no semver risk.
- **C5 (`Reader` struct inaccessible)**: Added `Reader` to the list of types needing `pub(crate)` changed to `pub` in `src/execution/datasource.rs` (Step 3), and added it to the `bench_internals` re-export in `lib.rs` (Step 4).
- **C6 (`PathExpr` tuple-struct syntax invalid)**: Changed all `PathExpr(vec![...])` to `PathExpr::new(vec![...])` in `bench_execution.rs` (Step 9). `PathExpr` is a named struct with a `pub fn new()` constructor, not a tuple struct.
- **C7 (Step 2 contradictory text)**: Rewrote Step 2 to clearly state the single chosen approach: change all needed items from `pub(crate)` to `pub` unconditionally. Since logq is a CLI tool with no library consumers, unconditional `pub` has no real semver impact and avoids error-prone `#[cfg]` duplication of every struct/trait.
- **S2 (Unused `BufReader` import)**: Removed `use std::io::BufReader` from `bench_datasource.rs` (Step 8). The `with_reader()` method wraps its input in `BufReader` internally, so the explicit import was unnecessary.
- **S4 (`linked_hash_map` dev-dependency)**: Added `linked-hash-map = "0.5"` to `[dev-dependencies]` in the Cargo.toml changes (Step 1). This avoids relying on transitive dependency availability for benchmark crates.

---

## Step 1: Update Cargo.toml -- add feature + bench targets

**File**: `Cargo.toml`

### 1a. Implementation

Replace the existing `[[bench]]` block and add the `bench-internals` feature. Also add `linked-hash-map` to dev-dependencies for robustness (benchmarks use `LinkedHashMap` directly):

```toml
[features]
bench-internals = []

[dev-dependencies]
criterion = "0.3"
rand = "0.8"
tempfile = "3.2"
linked-hash-map = "0.5"

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

Remove the old `[[bench]]` entry for `logq_benchmark` and delete `benches/logq_benchmark.rs`.

### 1b. Verify

```bash
cargo check --features bench-internals 2>&1 | head -5
```

Should succeed (no bench files yet, but Cargo.toml is valid).

### 1c. Commit

```bash
git add Cargo.toml && git rm benches/logq_benchmark.rs
git commit -m "Replace stub benchmark with bench-internals feature and 4 bench targets"
```

---

## Step 2: Make stream types pub for benchmark access

**File**: `src/execution/stream.rs`

### Visibility strategy

Since logq is a CLI tool (binary crate) with no external library consumers, making internal items `pub` unconditionally has no real semver impact. This is far simpler and less error-prone than duplicating every struct/trait definition with `#[cfg(feature = "bench-internals")]` / `#[cfg(not(feature = "bench-internals"))]` guards. The actual access control is provided by the `bench_internals` re-export module in `lib.rs`, which is itself gated behind the feature flag.

### 2a. Implementation

Change the following items from `pub(crate)` to `pub` in `stream.rs`:

- `struct Record` -> `pub struct Record`
- `fn Record::new(field_names, data)` -> `pub fn new`
- `fn Record::to_tuples()` -> `pub fn to_tuples`
- `fn Record::to_variables()` -> `pub fn to_variables` (needed by merge)
- `trait RecordStream` -> `pub trait RecordStream`
- (trait methods `next()` and `close()` are already implicitly pub within the trait)
- `struct MapStream` -> `pub struct MapStream`
- `fn MapStream::new()` -> `pub fn new`
- `struct FilterStream` -> `pub struct FilterStream`
- `fn FilterStream::new()` -> `pub fn new`
- `struct GroupByStream` -> `pub struct GroupByStream`
- `fn GroupByStream::new()` -> `pub fn new`
- `struct LimitStream` -> `pub struct LimitStream`
- `fn LimitStream::new()` -> `pub fn new`
- `struct InMemoryStream` -> `pub struct InMemoryStream`
- `fn InMemoryStream::new()` -> `pub fn new`

### 2b. Verify

```bash
cargo check --features bench-internals
cargo test
```

### 2c. Commit

```bash
git commit -am "Make stream types pub for benchmark access"
```

---

## Step 3: Make execution types, datasource types, AST types, and parser entry point pub

### `src/execution/types.rs`

Change from `pub(crate)` to `pub`:
- `enum Ordering` (line 118)
- `enum Expression` (line 124)
- `enum Relation` (line 233)
- `enum Named` (line 296)
- `enum Formula` (line 302)
- `struct NamedAggregate` -- find its definition
- `enum Aggregate` -- find its definition
- `enum Node` (line 455)
- `fn Node::get()` (line 471)
- `type StreamResult` (line 55) -- already `pub`

### `src/execution/datasource.rs`

Change from `pub(crate)` to `pub`:
- `trait RecordRead` (line 615)
- `fn ReaderBuilder::new()` (line 620)
- `fn ReaderBuilder::with_reader()` (line 632)
- `struct Reader` (line 666) -- **added in v2**
- `fn Reader::new()` (line 672)

### `src/syntax/ast.rs`

Change from `pub(crate)` to `pub` -- **added in v2**:
- `enum PathSegment` (line 115)
- `struct PathExpr` (line 123)
- `PathExpr::path_segments` field (line 124) -- needs to be `pub` for the `new()` constructor return value to be useful

### `src/syntax/parser.rs`

The `query()` function needs to be `pub`. Find it (around line 947) and change visibility.

### `src/common/types.rs`

Change from `pub(crate) type` to `pub type` -- **added in v2**:
- `type VariableName = String` (line 277)
- `type Variables = LinkedHashMap<String, Value>` (line 278)

### `src/execution/mod.rs`

Currently: `pub mod datasource; pub mod stream; pub mod types;` -- verify all are `pub mod`.

### 3a. Verify

```bash
cargo check --features bench-internals
cargo test
```

### 3b. Commit

```bash
git commit -am "Make execution types, AST types, and parser entry point pub for benchmarks"
```

---

## Step 4: Add bench_internals module to lib.rs and run_to_records to app.rs

**File**: `src/lib.rs`

Add at the end:

```rust
#[cfg(feature = "bench-internals")]
pub mod bench_internals {
    // Parser
    pub use crate::syntax::parser::query;

    // AST types (needed for constructing Expression::Variable in Tier B benchmarks)
    pub use crate::syntax::ast::{PathExpr, PathSegment};

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
    pub use crate::execution::datasource::{ReaderBuilder, RecordRead, Reader};

    // Common types
    pub use crate::common::types::{Value, Variables, VariableName, DataSource};
}
```

**File**: `src/app.rs`

Add `run_to_records` below `run_to_vec` (after line 231), adapted from the existing `run_to_vec`:

```rust
#[cfg(feature = "bench-internals")]
pub fn run_to_records(
    query_str: &str,
    data_source: common::types::DataSource,
) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
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

### 4a. Verify

```bash
cargo check --features bench-internals
cargo test
```

### 4b. Commit

```bash
git commit -am "Add bench_internals module and run_to_records function"
```

---

## Step 5: Create helpers module

**Files**: `benches/helpers/mod.rs`, `benches/helpers/synthetic.rs`, `benches/helpers/queries.rs`

### 5a. `benches/helpers/queries.rs`

```rust
/// Parser benchmark queries -- graduated ladder
pub const PARSE_L1: &str = "SELECT a FROM t";
pub const PARSE_L2: &str = "SELECT a, b, c FROM t WHERE a > 10";
pub const PARSE_L3: &str = "SELECT a, count(b) FROM t WHERE a > 10 GROUP BY a ORDER BY a DESC LIMIT 100";
pub const PARSE_L4: &str = r#"SELECT a, sum(b) FROM t WHERE a > 10 AND c LIKE "%foo%" GROUP BY a HAVING sum(b) > 100 ORDER BY a"#;
pub const PARSE_L5: &str = r#"SELECT a, CASE WHEN b > 10 THEN "high" WHEN b > 5 THEN "mid" ELSE "low" END AS tier, count(*) FROM t AS t1 LEFT JOIN t AS t2 ON t1.id = t2.id WHERE a BETWEEN 1 AND 100 GROUP BY a, tier"#;
pub const PARSE_L6: &str = "SELECT a, b FROM t WHERE a IN (10, 20, 30) AND b IS NOT NULL UNION SELECT c, d FROM t2 ORDER BY a LIMIT 50";

/// Execution Tier A queries (run against data/AWSELB.log)
pub const EXEC_E1: &str = "SELECT * FROM elb LIMIT 10";
pub const EXEC_E2: &str = "SELECT elbname, count(*) FROM elb GROUP BY elbname";
pub const EXEC_E3: &str = r#"SELECT elbname, elb_status_code FROM elb WHERE elb_status_code = "200" ORDER BY elbname"#;
```

### 5b. `benches/helpers/synthetic.rs`

```rust
use logq::bench_internals::*;
use linked_hash_map::LinkedHashMap;
use ordered_float::OrderedFloat;
use rand::prelude::*;
use std::collections::VecDeque;

/// Generate synthetic records with 5 columns:
/// col_s1 (String), col_s2 (String), col_i (Int), col_f (Float), col_b (Boolean)
pub fn generate_records(count: usize) -> VecDeque<Record> {
    let mut rng = StdRng::seed_from_u64(42);
    let field_names = vec![
        "col_s1".to_string(),
        "col_s2".to_string(),
        "col_i".to_string(),
        "col_f".to_string(),
        "col_b".to_string(),
    ];
    let mut records = VecDeque::with_capacity(count);
    for _ in 0..count {
        let data = vec![
            Value::String(format!("str_{}", rng.gen_range(0..100))),
            Value::String(format!("cat_{}", rng.gen_range(0..10))),
            Value::Int(rng.gen_range(0..1000)),
            Value::Float(OrderedFloat::from(rng.gen::<f32>() * 1000.0)),
            Value::Boolean(rng.gen_bool(0.5)),
        ];
        records.push_back(Record::new(&field_names, data));
    }
    records
}
```

### 5c. `benches/helpers/mod.rs`

```rust
pub mod queries;
pub mod synthetic;
```

### 5d. Verify

```bash
cargo check --features bench-internals
```

### 5e. Commit

```bash
git add benches/helpers/
git commit -m "Add benchmark helper modules for queries and synthetic data generation"
```

---

## Step 6: Create bench_parser.rs

**File**: `benches/bench_parser.rs`

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

mod helpers;
use helpers::queries::*;

fn bench_parser(c: &mut Criterion) {
    let queries = [
        ("L1_trivial", PARSE_L1),
        ("L2_where", PARSE_L2),
        ("L3_group_order_limit", PARSE_L3),
        ("L4_having_like", PARSE_L4),
        ("L5_casewhen_join", PARSE_L5),
        ("L6_in_union", PARSE_L6),
    ];

    let mut group = c.benchmark_group("parser");
    for (name, query) in &queries {
        group.bench_function(*name, |b| {
            b.iter(|| {
                let result = logq::syntax::parser::query(black_box(query));
                let _ = black_box(result);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_parser);
criterion_main!(benches);
```

### 6a. Verify

```bash
cargo bench --bench bench_parser --features bench-internals -- --quick
```

### 6b. Commit

```bash
git add benches/bench_parser.rs
git commit -m "Add parser graduated-ladder benchmarks (6 complexity levels)"
```

---

## Step 7: Create bench_udf.rs

**File**: `benches/bench_udf.rs`

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use logq::functions::{self, FunctionRegistry};
use logq::common::types::Value;
use ordered_float::OrderedFloat;
use linked_hash_map::LinkedHashMap;

fn build_registry() -> FunctionRegistry {
    functions::register_all().unwrap()
}

fn bench_udfs(c: &mut Criterion) {
    let registry = build_registry();

    let mut group = c.benchmark_group("udf");

    // U1: upper("hello world") -- String
    let args_upper = vec![Value::String("hello world".to_string())];
    group.bench_function("upper", |b| {
        b.iter(|| {
            let _ = black_box(registry.call("upper", black_box(&args_upper)));
        });
    });

    // U2: round(3.14159, 2) -- Arithmetic
    let args_round = vec![
        Value::Float(OrderedFloat::from(3.14159f32)),
        Value::Int(2),
    ];
    group.bench_function("round", |b| {
        b.iter(|| {
            let _ = black_box(registry.call("round", black_box(&args_round)));
        });
    });

    // U3: date_part("month", <fixed DateTime>) -- DateTime
    // Value::DateTime wraps DateTime<FixedOffset>, so we parse from RFC 3339.
    let fixed_dt = chrono::DateTime::parse_from_rfc3339("2024-06-15T10:30:00+00:00").unwrap();
    let args_datepart = vec![
        Value::String("month".to_string()),
        Value::DateTime(fixed_dt),
    ];
    group.bench_function("date_part", |b| {
        b.iter(|| {
            let _ = black_box(registry.call("date_part", black_box(&args_datepart)));
        });
    });

    // U4: array_contains([1,2,3,4,5], 3) -- Array
    let args_array = vec![
        Value::Array(vec![
            Value::Int(1), Value::Int(2), Value::Int(3),
            Value::Int(4), Value::Int(5),
        ]),
        Value::Int(3),
    ];
    group.bench_function("array_contains", |b| {
        b.iter(|| {
            let _ = black_box(registry.call("array_contains", black_box(&args_array)));
        });
    });

    // U5: map_keys({"a":1, "b":2}) -- Map
    let mut map = LinkedHashMap::new();
    map.insert("a".to_string(), Value::Int(1));
    map.insert("b".to_string(), Value::Int(2));
    let args_map = vec![Value::Object(map)];
    group.bench_function("map_keys", |b| {
        b.iter(|| {
            let _ = black_box(registry.call("map_keys", black_box(&args_map)));
        });
    });

    // U6: regexp_like("foo123", "\d+") -- Regex (steady-state cached)
    let args_regex = vec![
        Value::String("foo123".to_string()),
        Value::String(r"\d+".to_string()),
    ];
    // Warm the cache
    let _ = registry.call("regexp_like", &args_regex);
    group.bench_function("regexp_like", |b| {
        b.iter(|| {
            let _ = black_box(registry.call("regexp_like", black_box(&args_regex)));
        });
    });

    group.finish();
}

criterion_group!(benches, bench_udfs);
criterion_main!(benches);
```

### 7a. Verify

```bash
cargo bench --bench bench_udf -- --quick
```

Note: `bench_udf` does not require `bench-internals` -- it only uses public types.

### 7b. Commit

```bash
git add benches/bench_udf.rs
git commit -m "Add UDF benchmarks (one representative per category)"
```

---

## Step 8: Create bench_datasource.rs

**File**: `benches/bench_datasource.rs`

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use logq::bench_internals::*;

fn load_and_replicate(path: &str, min_lines: usize) -> (Vec<String>, usize) {
    let content = std::fs::read_to_string(path).unwrap();
    let lines: Vec<String> = content.lines().filter(|l| !l.is_empty()).map(|l| l.to_string()).collect();
    let original_count = lines.len();
    if original_count >= min_lines {
        return (lines, original_count);
    }
    let reps = (min_lines + original_count - 1) / original_count;
    let mut replicated = Vec::with_capacity(reps * original_count);
    for _ in 0..reps {
        replicated.extend(lines.iter().cloned());
    }
    let count = replicated.len();
    (replicated, count)
}

fn bench_datasource(c: &mut Criterion) {
    let formats = [
        ("ELB", "data/AWSELB.log", "elb", 500),
        ("ALB", "data/AWSALB.log", "alb", 1000),
        ("S3", "data/S3.log", "s3", 1000),
        ("Squid", "data/Squid.log", "squid", 1000),
        ("JSONL", "data/structured.log", "jsonl", 1000),
    ];

    let mut group = c.benchmark_group("datasource");

    for (name, path, format, min_lines) in &formats {
        let (lines, line_count) = load_and_replicate(path, *min_lines);
        let concatenated = lines.join("\n") + "\n";

        group.throughput(Throughput::Elements(line_count as u64));
        group.bench_function(*name, |b| {
            b.iter(|| {
                let reader_builder = ReaderBuilder::new(format.to_string());
                let cursor = std::io::Cursor::new(concatenated.as_bytes());
                let mut reader = reader_builder.with_reader(cursor);
                let mut count = 0u64;
                while let Ok(Some(_record)) = reader.read_record() {
                    count += 1;
                }
                black_box(count)
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_datasource);
criterion_main!(benches);
```

### 8a. Verify

```bash
cargo bench --bench bench_datasource --features bench-internals -- --quick
```

### 8b. Commit

```bash
git add benches/bench_datasource.rs
git commit -m "Add datasource parse-throughput benchmarks (all 5 formats)"
```

---

## Step 9: Create bench_execution.rs

**File**: `benches/bench_execution.rs`

```rust
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use logq::bench_internals::*;
use logq::common::types as ctypes;
use logq::functions;

mod helpers;
use helpers::queries::*;
use helpers::synthetic::generate_records;

/// Tier A: End-to-end benchmarks using real AWSELB.log
fn bench_execution_tier_a(c: &mut Criterion) {
    let path = PathBuf::from("data/AWSELB.log");
    if !path.exists() {
        eprintln!("Skipping Tier A benchmarks: data/AWSELB.log not found");
        return;
    }

    let mut group = c.benchmark_group("execution_e2e");

    let queries = [
        ("E1_scan_limit", EXEC_E1),
        ("E2_groupby_count", EXEC_E2),
        ("E3_filter_orderby", EXEC_E3),
    ];

    for (name, query) in &queries {
        let data_source = ctypes::DataSource::File(
            path.clone(),
            "elb".to_string(),
            "elb".to_string(),
        );
        group.bench_function(*name, |b| {
            b.iter(|| {
                let result = logq::app::run_to_records(
                    black_box(query),
                    data_source.clone(),
                );
                let _ = black_box(result);
            });
        });
    }

    group.finish();
}

/// Tier B: Isolated operator benchmarks with synthetic data
fn bench_execution_tier_b(c: &mut Criterion) {
    let sizes: &[(usize, &str)] = &[
        (1_000, "1K"),
        (10_000, "10K"),
        (100_000, "100K"),
    ];

    let registry = Arc::new(functions::register_all().unwrap());

    // S1: MapStream -- SELECT projection (extract 2 of 5 columns)
    {
        let mut group = c.benchmark_group("execution_map");
        for &(size, label) in sizes {
            if size >= 100_000 {
                group.measurement_time(Duration::from_secs(10));
                group.sample_size(20);
            }
            group.bench_with_input(BenchmarkId::from_parameter(label), &size, |b, &sz| {
                b.iter(|| {
                    let data = generate_records(sz);
                    let source = InMemoryStream::new(data);
                    let named_list = vec![
                        Named::Expression(
                            Expression::Variable(PathExpr::new(vec![
                                PathSegment::AttrName("col_s1".to_string()),
                            ])),
                            Some("col_s1".to_string()),
                        ),
                        Named::Expression(
                            Expression::Variable(PathExpr::new(vec![
                                PathSegment::AttrName("col_i".to_string()),
                            ])),
                            Some("col_i".to_string()),
                        ),
                    ];
                    let variables = ctypes::Variables::default();
                    let mut stream = MapStream::new(named_list, variables, Box::new(source), registry.clone());
                    let mut count = 0u64;
                    while let Ok(Some(_)) = stream.next() {
                        count += 1;
                    }
                    black_box(count)
                });
            });
        }
        group.finish();
    }

    // S2: FilterStream -- WHERE col_i > 500 (filters ~half)
    {
        let mut group = c.benchmark_group("execution_filter");
        for &(size, label) in sizes {
            if size >= 100_000 {
                group.measurement_time(Duration::from_secs(10));
                group.sample_size(20);
            }
            group.bench_with_input(BenchmarkId::from_parameter(label), &size, |b, &sz| {
                b.iter(|| {
                    let data = generate_records(sz);
                    let source = InMemoryStream::new(data);
                    let formula = Formula::Predicate(
                        Relation::MoreThan,
                        Box::new(Expression::Variable(PathExpr::new(vec![
                            PathSegment::AttrName("col_i".to_string()),
                        ]))),
                        Box::new(Expression::Constant(Value::Int(500))),
                    );
                    let variables = ctypes::Variables::default();
                    let mut stream = FilterStream::new(formula, variables, Box::new(source), registry.clone());
                    let mut count = 0u64;
                    while let Ok(Some(_)) = stream.next() {
                        count += 1;
                    }
                    black_box(count)
                });
            });
        }
        group.finish();
    }

    // S3: LimitStream -- LIMIT 100 (should be near-constant)
    {
        let mut group = c.benchmark_group("execution_limit");
        for &(size, label) in sizes {
            group.bench_with_input(BenchmarkId::from_parameter(label), &size, |b, &sz| {
                b.iter(|| {
                    let data = generate_records(sz);
                    let source = InMemoryStream::new(data);
                    let mut stream = LimitStream::new(100, Box::new(source));
                    let mut count = 0u64;
                    while let Ok(Some(_)) = stream.next() {
                        count += 1;
                    }
                    black_box(count)
                });
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_execution_tier_a, bench_execution_tier_b);
criterion_main!(benches);
```

**Note**: GroupByStream is omitted from Tier B because constructing `NamedAggregate` and `Aggregate` programmatically requires significant internal type wiring. GroupBy cost is covered by Tier A E2. If needed later, it can be added.

### 9a. Verify

```bash
cargo bench --bench bench_execution --features bench-internals -- --quick
```

### 9b. Commit

```bash
git add benches/bench_execution.rs
git commit -m "Add execution benchmarks (Tier A end-to-end + Tier B isolated operators)"
```

---

## Step 10: End-to-end verification

### 10a. Run all tests to verify no regressions

```bash
cargo test
```

### 10b. Run all benchmarks

```bash
cargo bench --features bench-internals
```

### 10c. Verify each benchmark group produces output

Check that the terminal shows timing results for:
- `parser/L1_trivial` through `parser/L6_in_union`
- `udf/upper`, `udf/round`, `udf/date_part`, `udf/array_contains`, `udf/map_keys`, `udf/regexp_like`
- `datasource/ELB`, `datasource/ALB`, `datasource/S3`, `datasource/Squid`, `datasource/JSONL`
- `execution_e2e/E1_scan_limit`, `execution_e2e/E2_groupby_count`, `execution_e2e/E3_filter_orderby`
- `execution_map/1K`, `execution_filter/1K`, `execution_limit/1K` (and 10K, 100K variants)

---

## Task Dependencies

| Group | Steps | Can Parallelize | Notes |
|-------|-------|-----------------|-------|
| 1 | Step 1 | No | Cargo.toml + delete old bench |
| 2 | Steps 2-3 | Yes (independent files) | Visibility changes |
| 3 | Step 4 | No (depends on Group 2) | lib.rs + app.rs wiring |
| 4 | Step 5 | No (depends on Group 3) | Helpers must compile against bench_internals |
| 5 | Steps 6-9 | Yes (independent files) | All 4 benchmark files |
| 6 | Step 10 | No (depends on all) | End-to-end verification |

## Files Touched

| Action | File |
|--------|------|
| Modify | `Cargo.toml` |
| Delete | `benches/logq_benchmark.rs` |
| Modify | `src/execution/stream.rs` |
| Modify | `src/execution/types.rs` |
| Modify | `src/execution/datasource.rs` |
| Modify | `src/syntax/ast.rs` |
| Modify | `src/syntax/parser.rs` |
| Modify | `src/common/types.rs` |
| Modify | `src/lib.rs` |
| Modify | `src/app.rs` |
| Create | `benches/helpers/mod.rs` |
| Create | `benches/helpers/queries.rs` |
| Create | `benches/helpers/synthetic.rs` |
| Create | `benches/bench_parser.rs` |
| Create | `benches/bench_udf.rs` |
| Create | `benches/bench_datasource.rs` |
| Create | `benches/bench_execution.rs` |
