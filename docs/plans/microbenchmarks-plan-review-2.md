VERDICT: APPROVED

## Summary Assessment

The v2 plan properly addresses all 7 critical issues and 2 applicable suggestions from the first review. The benchmark code now uses correct type constructors, visibility, and import paths that match the actual codebase. After thorough verification of every type, function signature, and module path against the source code, I found no compilation-blocking issues.

## Critical Issues (must fix)

None.

## Suggestions (nice to have)

### S1: GroupByStream omitted from Tier B despite being in the design spec

The design document (Section 2, Tier B) lists S3 as `GroupByStream (GROUP BY + count)`. The plan omits it with a note that constructing `NamedAggregate` and `Aggregate` programmatically requires "significant internal type wiring." This is a reasonable pragmatic decision given that GroupBy cost is covered by Tier A E2, but it is a deviation from the design. If you later want to add it, the plan's visibility changes already make `NamedAggregate` and `Aggregate` available through `bench_internals`.

### S2: `bench_parser` has `required-features = ["bench-internals"]` but does not use `bench_internals`

In `Cargo.toml` (Step 1), `bench_parser` is listed with `required-features = ["bench-internals"]`. However, the `bench_parser.rs` code (Step 6) accesses the parser via `logq::syntax::parser::query(...)` directly, not through `bench_internals`. This works because Step 3 changes `query()` from `pub(crate)` to `pub` unconditionally (not behind a feature flag). The feature gate on the bench target is thus unnecessary -- the parser benchmark would compile even without `--features bench-internals`. This is not a bug (it still works), but it is misleading. Consider either removing `required-features` from `bench_parser`, or changing the benchmark to import via `logq::bench_internals::query` for consistency.

### S3: `ordered_float` and `chrono` used in benchmarks rely on being regular dependencies

The benchmark files `bench_udf.rs` and `benches/helpers/synthetic.rs` use `ordered_float::OrderedFloat` and `chrono::DateTime` directly. This works because these crates are in `[dependencies]`, making them available to all targets (including benchmarks). However, this creates an implicit coupling -- if either crate were ever moved to `pub(crate)` re-exports or removed from direct dependencies, the benchmarks would break silently. Adding `ordered_float = "2.8"` and `chrono = "0.4"` to `[dev-dependencies]` would make the dependency explicit and future-proof. (The `linked-hash-map` case was already handled in the v2 changes.)

### S4: Task granularity assessment

Steps 2 and 3 involve changing visibility on ~20 items across 6 files. While straightforward (each is a mechanical `pub(crate)` -> `pub` replacement), the sheer number of items means these steps are likely 5-10 minutes each rather than the 2-5 minute target. Not a blocking issue, but worth noting for time estimation.

### S5: Criterion `--quick` flag

The verification commands in Steps 6a, 7a, 8a, and 9a use `-- --quick`. Criterion 0.3 does not officially document a `--quick` flag; it will silently ignore unrecognized arguments. This means the benchmarks will run with default settings (100 samples, 5s measurement), which may take longer than expected during the verify step. Consider using `-- --sample-size 10 --measurement-time 3` for quicker verification runs, or simply note that initial runs may take a few minutes.

## Verified Claims (things I confirmed are correct)

### Types, Constructors, and Signatures

1. **`PathSegment::AttrName(String)`** -- confirmed at `src/syntax/ast.rs:116`. The plan correctly uses `AttrName`, not `Field`.
2. **`PathExpr::new(Vec<PathSegment>)`** -- confirmed at `src/syntax/ast.rs:128`. Named struct with `pub fn new` constructor, not tuple struct.
3. **`PathExpr` and `PathSegment` are `pub(crate)`** -- confirmed at `src/syntax/ast.rs:115,123`. Plan correctly lists them for `pub` visibility change in Step 3.
4. **`Expression::Variable(PathExpr)`** -- confirmed at `src/execution/types.rs:127`. Takes `PathExpr` from `syntax::ast`.
5. **`Expression::Constant(Value)`** -- confirmed at `src/execution/types.rs:125`. Takes `Value` from `common::types`.
6. **`Formula::Predicate(Relation, Box<Expression>, Box<Expression>)`** -- confirmed at `src/execution/types.rs:307`.
7. **`Relation::MoreThan`** -- confirmed at `src/execution/types.rs:236`. Not `GreaterThan`.
8. **`Value::Int(i32)`** from common/types -- confirmed at `src/common/types.rs:23`.
9. **`Value::DateTime(chrono::DateTime<chrono::offset::FixedOffset>)`** -- confirmed at `src/common/types.rs:28`. The plan correctly uses `DateTime::parse_from_rfc3339()` which returns `DateTime<FixedOffset>`.
10. **`Record::new(&Vec<VariableName>, Vec<Value>)`** -- confirmed at `src/execution/stream.rs:19`. `VariableName = String`.
11. **`InMemoryStream::new(VecDeque<Record>)`** -- confirmed at `src/execution/stream.rs:262`.
12. **`MapStream::new(Vec<Named>, Variables, Box<dyn RecordStream>, Arc<FunctionRegistry>)`** -- confirmed at `src/execution/stream.rs:130`.
13. **`FilterStream::new(Formula, Variables, Box<dyn RecordStream>, Arc<FunctionRegistry>)`** -- confirmed at `src/execution/stream.rs:228`.
14. **`LimitStream::new(u32, Box<dyn RecordStream>)`** -- confirmed at `src/execution/stream.rs:190`. Literal `100` infers correctly as `u32`.
15. **`NamedAggregate`** is `pub(crate) struct` -- confirmed at `src/execution/types.rs:673`. Plan correctly lists it for `pub` change.
16. **`Aggregate`** is `pub(crate) enum` -- confirmed at `src/execution/types.rs:685`. Plan correctly lists it.
17. **`Named`** is `pub(crate) enum` with `Expression(Expression, Option<VariableName>)` and `Star` variants -- confirmed at `src/execution/types.rs:296-298`.
18. **`Variables`** is `pub(crate) type = LinkedHashMap<String, Value>`** -- confirmed at `src/common/types.rs:278`. Plan correctly changes to `pub type`.
19. **`VariableName`** is `pub(crate) type = String` -- confirmed at `src/common/types.rs:277`. Plan correctly changes to `pub type`.
20. **`DataSource`** is `pub enum` with `File(PathBuf, String, String)` -- confirmed at `src/common/types.rs:431-434`. Derives `Clone`.

### Visibility and Module Paths

21. **`syntax::parser::query`** is `pub(crate) fn` -- confirmed at `src/syntax/parser.rs:947`. Plan correctly changes to `pub`.
22. **`syntax` module** is `pub mod` in `lib.rs` -- confirmed.
23. **`parser` module** is `pub mod` in `src/syntax/mod.rs` -- confirmed.
24. **`execution` modules** (`datasource`, `stream`, `types`) are all `pub mod` -- confirmed at `src/execution/mod.rs:1-3`.
25. **`common::types`** is `pub mod` -- confirmed at `src/common/mod.rs:1`.
26. **`functions::register_all`** is `pub fn` -- confirmed at `src/functions/mod.rs:16`.
27. **`FunctionRegistry`** is `pub struct` -- confirmed at `src/functions/registry.rs:47`.
28. **`FunctionRegistry::call`** is `pub fn(&self, &str, &[Value])` -- confirmed at `src/functions/registry.rs:103`.
29. **`ReaderBuilder`** is `pub struct` but `new()` and `with_reader()` are `pub(crate)` -- confirmed at `src/execution/datasource.rs:610,620,632`. Plan correctly changes both methods to `pub`.
30. **`Reader<R>`** is `pub(crate) struct` -- confirmed at `src/execution/datasource.rs:666`. Plan correctly changes to `pub` (required because `with_reader` returns `Reader<R>` and a `pub` method cannot return a `pub(crate)` type).
31. **`RecordRead`** trait is `pub(crate)` -- confirmed at `src/execution/datasource.rs:615`. Plan correctly changes to `pub`.
32. **`RecordRead` is implemented for `Reader<R>`** -- confirmed at `src/execution/datasource.rs:683`.

### Data Files and Infrastructure

33. **All 5 data files exist**: `data/AWSELB.log`, `data/AWSALB.log`, `data/S3.log`, `data/Squid.log`, `data/structured.log` -- confirmed.
34. **`criterion = "0.3"` and `rand = "0.8"`** are in `[dev-dependencies]` -- confirmed at `Cargo.toml:38-39`.
35. **`ordered-float = "2.8"` and `chrono = "0.4"`** are in `[dependencies]` -- confirmed. Available to benchmark crates.
36. **`linked-hash-map = "0.5"`** is in `[dependencies]` -- confirmed at `Cargo.toml:33`.
37. **`benches/logq_benchmark.rs`** exists and is a trivial stub (`1 + 1`) -- confirmed. Safe to delete.
38. **`run_to_vec`** exists at `src/app.rs:208` gated behind `#[cfg(test)]` -- confirmed. The plan's `run_to_records` correctly mirrors its implementation.

### Benchmark Code Correctness

39. **Parser queries use double-quoted strings** -- confirmed. The parser's `value()` function dispatches to `double_quote_string_literal` only.
40. **`FROM elb` matches `DataSource::File(_, "elb", "elb")`** -- confirmed. The logical planner's `check_env` validates that the table name in `FROM` matches the `DataSource`'s table name (3rd field).
41. **`with_reader` internally wraps in `BufReader`** -- confirmed at `src/execution/datasource.rs:674`. No explicit `BufReader` needed in benchmark.
42. **`reader.read_record()` returns `ReaderResult<Option<Record>>`** -- confirmed. `ReaderResult` and `ReaderError` are both `pub`.
43. **`Variables::default()` works** -- `Variables` is an alias for `LinkedHashMap<String, Value>`, which implements `Default`.
44. **Regex caching exists** for `regexp_like` -- confirmed. Thread-local LRU cache makes the benchmark measure steady-state cached performance.
45. **All items listed in `bench_internals` re-export (Step 4) match types that exist and are being changed to `pub`** in Steps 2-3.
