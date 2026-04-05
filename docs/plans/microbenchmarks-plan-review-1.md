VERDICT: NEEDS_REVISION

## Summary Assessment

The plan has a solid overall structure and covers the design's four benchmark areas well, but contains several critical compilation errors in the benchmark source code that will prevent it from building. The most significant are: using a nonexistent AST variant (`PathSegment::Field` instead of `AttrName`), constructing `DateTime<Utc>` when the codebase only supports `DateTime<FixedOffset>`, and failing to account for `pub(crate)` visibility on `PathExpr`/`PathSegment` and other AST types that the benchmarks reference directly.

## Critical Issues (must fix)

### C1: `PathSegment::Field` does not exist -- benchmark code will not compile

In Steps 5 and 9 (`bench_execution.rs`, `synthetic.rs`), the benchmark code references `logq::syntax::ast::PathSegment::Field("col_s1".to_string())`. The actual enum variant is `PathSegment::AttrName(String)`, not `Field`. This appears in multiple places:
- `bench_execution.rs` lines for MapStream (Step 9, S1 benchmark)
- `bench_execution.rs` lines for FilterStream (Step 9, S2 benchmark)

**Fix**: Replace all `PathSegment::Field(...)` with `PathSegment::AttrName(...)`.

### C2: `PathExpr` and `PathSegment` are `pub(crate)` -- not accessible from benchmarks

The benchmark code in Step 9 directly uses `logq::syntax::ast::PathExpr` and `logq::syntax::ast::PathSegment`. Both are declared `pub(crate)` in `src/syntax/ast.rs` (lines 115, 123). The plan's `bench_internals` re-export module (Step 4) does NOT include these types. Even if added to the re-export, `pub use` cannot re-export `pub(crate)` items to external crates.

**Fix**: Either (a) add `PathExpr` and `PathSegment` to the list of types that need visibility changed from `pub(crate)` to `pub` under `bench-internals` (Steps 2-3), and add them to the `bench_internals` re-export, or (b) restructure the Tier B benchmarks to avoid directly constructing AST nodes (e.g., parse the expressions from strings instead).

### C3: `Value::DateTime` wraps `DateTime<FixedOffset>`, not `DateTime<Utc>` -- UDF benchmark won't compile

In Step 7 (`bench_udf.rs`), the plan constructs:
```rust
let fixed_dt = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap();
let args_datepart = vec![
    Value::String("month".to_string()),
    Value::DateTime(fixed_dt),
];
```

But `Value::DateTime` is defined as `DateTime(chrono::DateTime<chrono::offset::FixedOffset>)` (see `src/common/types.rs` line 28). `Utc.with_ymd_and_hms()` returns `DateTime<Utc>`, which is a different type and won't compile.

**Fix**: Use `chrono::DateTime::parse_from_rfc3339("2024-06-15T10:30:00+00:00").unwrap()` instead, which returns `DateTime<FixedOffset>`. This is exactly how the existing test suite does it (see `src/functions/datetime.rs` line 377: `chrono::DateTime::parse_from_rfc3339(s).unwrap()`). Remove the `use chrono::{TimeZone, Utc}` import.

### C4: `Variables` and `VariableName` are `pub(crate)` -- re-export in `bench_internals` will fail

The plan's Step 4 `bench_internals` module includes:
```rust
pub use crate::common::types::{Value, Variables, VariableName, DataSource};
```

But `Variables` (line 278) and `VariableName` (line 277) are `pub(crate) type` aliases in `src/common/types.rs`. You cannot `pub use` a `pub(crate)` item. `DataSource` is `pub enum` and `Value` is `pub enum`, so those are fine.

**Fix**: Either change `Variables` and `VariableName` to `pub type` (they are just type aliases for `LinkedHashMap<String, Value>` and `String` respectively, so this has no semver risk), or remove them from the re-export and have benchmarks use `LinkedHashMap<String, Value>` directly.

### C5: `Reader<R>` struct is `pub(crate)` -- `with_reader()` return type is inaccessible

In Step 8 (`bench_datasource.rs`), the code calls `reader_builder.with_reader(cursor)`, which returns `Reader<R>`. But `Reader<R>` is `pub(crate) struct` (line 666). Even if `with_reader` is made `pub`, the return type `Reader<R>` cannot be used by external code because the struct itself is `pub(crate)`.

The plan's Step 3 lists `struct Reader` in `datasource.rs` as needing visibility changes but does NOT actually include it in the list of items to change to `pub`. The re-export in `bench_internals` also does not include `Reader`.

**Fix**: Add `Reader` to the list of items that need `pub(crate)` changed to `pub`, and add `Reader` to the `bench_internals` re-export (or at minimum, change it to `pub` so the return type is visible).

### C6: `PathExpr` constructor uses `PathExpr(vec![...])` tuple-struct syntax, but `PathExpr` is a named struct

In Step 9 (`bench_execution.rs`), the benchmark code constructs:
```rust
Expression::Variable(logq::syntax::ast::PathExpr(vec![
    logq::syntax::ast::PathSegment::Field("col_s1".to_string()),
]))
```

But `PathExpr` is a named struct with a field `path_segments`:
```rust
pub(crate) struct PathExpr {
    pub(crate) path_segments: Vec<PathSegment>,
}
```

It has a constructor `PathExpr::new(vec![...])`, not tuple-struct syntax `PathExpr(vec![...])`.

**Fix**: Use `PathExpr::new(vec![PathSegment::AttrName("col_s1".to_string())])` instead. (This also requires `PathExpr::new` to be `pub`, which is already `pub fn new` in the source.)

### C7: Plan's "revised approach" for Step 2 is incoherent -- contradicts itself

Step 2 starts by describing the `#[cfg(feature)]` approach, then proposes a "revised approach" of just making everything `pub` unconditionally, then explains why that doesn't work, then suggests yet another approach. The final instruction is: "Items to change from `pub(crate)` to `pub` in `stream.rs`". But this unconditional `pub` change conflicts with the design document's explicit requirement (Section "Visibility: The `bench-internals` Cargo Feature", mechanism 1) that visibility be conditional behind `#[cfg(feature = "bench-internals")]`.

**Fix**: Pick one approach and state it clearly. The unconditional `pub` approach is pragmatically fine for a CLI tool, but the step text needs to not contradict itself. Remove the abandoned approaches and clearly state the chosen strategy.

## Suggestions (nice to have)

### S1: Task granularity -- Steps 2-3 are not "2-5 minutes"

Steps 2 and 3 involve changing visibility on 15+ items across 4 files, each potentially requiring a `#[cfg(feature)]` duplication or careful evaluation. This is closer to 10-15 minutes of careful work, not 2-5. Consider splitting by file (one step per file).

### S2: `bench_datasource.rs` does not use `BufReader` import

Step 8 imports `use std::io::BufReader` at the top but never uses it. The existing codebase tests wrap cursors in `BufReader::new(...)` (see `datasource.rs` test lines 806, 863, 904), but the benchmark code passes the cursor directly. The `with_reader` function internally wraps in `BufReader`, so this is functionally fine but the unused import will generate a compiler warning.

### S3: Consider verifying that `ordered_float` is in the benchmark dependency path

`bench_udf.rs` and `synthetic.rs` use `ordered_float::OrderedFloat`. Since benchmarks are external crates, they need `ordered_float` as a dev-dependency or explicit dependency. It's currently a regular dependency of logq, so it's transitively available. This should work but could break if the dependency is moved to `pub(crate)` re-exports.

### S4: `linked_hash_map` usage in `bench_udf.rs`

Step 7 imports `use linked_hash_map::LinkedHashMap` directly. Since benchmarks are external crates, `linked_hash_map` must be accessible. It is a regular dependency of logq, so it's re-exported transitively. However, relying on transitive dependencies is fragile. Consider using `logq::bench_internals::Variables` (once C4 is fixed) or adding `linked_hash_map` to `[dev-dependencies]`.

### S5: No TDD cycle

The CLAUDE.md rules state "Write tests BEFORE implementation (test-first)". The plan has no failing-test-first steps for any of the benchmarks. While benchmarks are not traditional tests, the plan could add a step that verifies a minimal benchmark compiles and runs before building out all 4 files.

### S6: Datasource benchmark constructs `Cursor` without `BufReader` wrapper

The test suite in `datasource.rs` always wraps byte slices in `BufReader::new(...)` before passing to `with_reader`. The benchmark passes a raw `Cursor`. While `with_reader` internally creates a `BufReader`, passing a `Cursor` without `BufReader` means the internal `BufReader` wraps a `Cursor`, which is fine but subtly different from the test pattern.

## Verified Claims (things you confirmed are correct)

1. **`logq_benchmark.rs` exists** at `benches/logq_benchmark.rs` and should be deleted -- confirmed.
2. **Data files exist**: `data/AWSELB.log`, `data/AWSALB.log`, `data/S3.log`, `data/Squid.log`, `data/structured.log` all exist in `data/`.
3. **`criterion = "0.3"` and `rand = "0.8"`** are in `[dev-dependencies]` -- confirmed in `Cargo.toml`.
4. **`functions::register_all()` is `pub fn`** -- confirmed at `src/functions/mod.rs:16`.
5. **`FunctionRegistry::call()` is `pub fn`** and takes `(&self, name: &str, args: &[Value])` -- confirmed at `src/functions/registry.rs:103`.
6. **`query()` is `pub(crate)`** in `src/syntax/parser.rs:947` -- confirmed. Needs visibility change.
7. **`execution/mod.rs` has `pub mod` for all three submodules** -- confirmed.
8. **`Record::new` takes `&Vec<VariableName>` and `Vec<Value>`** -- confirmed at `src/execution/stream.rs:19`.
9. **`LimitStream::new` takes `u32`** -- confirmed at `src/execution/stream.rs:190`. The literal `100` in the benchmark will correctly infer as `u32`.
10. **`Value` is `pub enum`** -- confirmed. Accessible from external crates.
11. **`DataSource` is `pub enum`** with `File(PathBuf, String, String)` variant -- confirmed at `src/common/types.rs:431-435`.
12. **`ReaderBuilder` is `pub struct`** -- confirmed at `src/execution/datasource.rs:610`. (But its methods are `pub(crate)`.)
13. **`run_to_vec` exists** as `#[cfg(test)]` function at `src/app.rs:207-231` -- confirmed. The plan's `run_to_records` is modeled after it.
14. **Parser string literal handling**: The parser uses double-quoted strings only (via `double_quote_string_literal`), consistent with all benchmark queries using double-quoted strings.
15. **Regex caching exists**: Thread-local LRU cache in `regexp.rs` -- the design correctly notes this.
16. **`Relation::MoreThan`** (not `GreaterThan`) is the correct variant name -- confirmed. The benchmark code uses `Relation::MoreThan` which matches.
