VERDICT: NEEDS_REVISION

## Summary Assessment

The v2 design shows strong responsiveness to the first review -- all five original critical issues (C1-C5) have been addressed with thoughtful solutions. However, new critical issues have emerged: several benchmark queries use syntax that the parser does not actually support (single-quoted strings, bare JOIN, IN-with-subquery), the visibility mechanism (re-exporting types via `pub use`) does not fix `pub(crate)` methods and constructors, a Known Limitation claims regex caching doesn't exist when it already does, and the Tier A execution benchmarks lack a way to avoid stdout I/O pollution.

## Critical Issues (must fix)

### C1. Parser benchmark queries use single-quoted strings, which the parser does not support

The `value()` parser function (parser.rs:190-191) dispatches to `double_quote_string_literal` only. There is no `single_quote_string_literal` in the value alternatives. Single-quoted strings are only recognized in tuple constructor keys (line 817), not as general string values.

This means the following benchmark queries will fail to parse:

- **Level 4**: `c LIKE '%foo%'` -- must be `c LIKE "%foo%"`
- **Level 4**: `HAVING sum(b) > 100` works, but the LIKE pattern does not.
- **Level 5**: `THEN 'high'`, `THEN 'mid'`, `ELSE 'low'` -- must use double quotes (`"high"`, `"mid"`, `"low"`)
- **Tier A E3**: `WHERE elb_status_code = '200'` -- must be `WHERE elb_status_code = "200"`

All existing tests in the codebase consistently use double-quoted strings (e.g., `app.rs:1096` uses `r#"... WHERE elb_status_code = "200""#`). The benchmark queries must follow the same convention or they will simply fail.

### C2. Level 5 uses bare JOIN (INNER JOIN), which the parser does not support

Level 5 contains `JOIN t2 ON t.id = t2.id`. Bare `JOIN` is equivalent to `INNER JOIN` in SQL. The parser only supports:
- `LEFT [OUTER] JOIN` (parser.rs:518-533, via `left_join_clause`)
- `CROSS JOIN` / comma-separated tables (parser.rs:503-516, via `table_reference_list`)

The `JoinType` enum (`ast.rs:26-29`) only has `Cross` and `Left` variants -- there is no `Inner`. This query will fail to parse. Level 5 should use `LEFT JOIN` or `CROSS JOIN` instead, or be redesigned to avoid JOIN entirely.

### C3. Level 6 uses IN (SELECT ...), but IN only supports value lists

Level 6 contains `WHERE a IN (SELECT x FROM t2 WHERE x > 10)`. The `parse_postfix_in` function (parser.rs:700-748) consumes the opening parenthesis itself, then calls `expression` on the content. For a subquery to work, `expression -> factor -> subquery` would need to match, but `subquery` expects its own `(` + select_query + `)` delimiters. Since `parse_postfix_in` already consumed the outer `(`, the subquery parser cannot find its opening parenthesis and fails.

The existing tests confirm this: `test_in_expression_structure` (line 2114) only tests `IN (1, 2, 3)` -- value lists. The separate `test_subquery_in_where` (line 2420) tests `a = (select max(a) from it)` where the subquery is used with `=`, not `IN`. There is no test for `IN (SELECT ...)`.

Level 6 should be revised to either: (a) remove the `IN (SELECT ...)` and use `IN (1, 2, 3)` or another supported construct, or (b) acknowledge that the query tests future parser capabilities (but then it will cause benchmark failures until that feature is implemented).

### C4. Re-exporting types via `pub use` does not make `pub(crate)` methods callable

The `bench-internals` feature gates `pub use` re-exports of types like `ReaderBuilder`, `Record`, `MapStream`, etc. However, `pub use` only changes the visibility of the *type path* -- it does not change the visibility of the type's methods or constructors. All the following remain `pub(crate)` and uncallable from an external crate:

- `ReaderBuilder::new()` (datasource.rs:620)
- `ReaderBuilder::with_reader()` (datasource.rs:632)
- `Record::new()` (stream.rs:19)
- `MapStream::new()` (stream.rs:130)
- `FilterStream::new()` (stream.rs:228)
- `GroupByStream::new()` (stream.rs:289)
- `LimitStream::new()` (stream.rs:190)
- `InMemoryStream::new()` (stream.rs:262)

The benchmark code will compile with the type names visible but will get "method `new` is not visible" errors when trying to construct any of these types.

The fix requires one of:
1. Changing the methods themselves to `pub` (unconditionally or behind `#[cfg(feature = "bench-internals")]`), not just re-exporting the types.
2. Adding wrapper/factory functions in the `bench_internals` module that construct these types on behalf of the benchmark code.
3. Using `#[cfg(feature = "bench-internals")]` to conditionally change `pub(crate)` to `pub` on the methods themselves.

Option 3 is cleanest: e.g., `#[cfg_attr(feature = "bench-internals", visibility::make(pub))]` or manually writing `#[cfg(feature = "bench-internals")] pub fn new(...)` / `#[cfg(not(feature = "bench-internals"))] pub(crate) fn new(...)`. This needs to be specified concretely.

### C5. Known Limitation 3 incorrectly claims regex is compiled on every invocation

The design states: "The `regexp_like` benchmark may overstate per-call cost because the regex is compiled on every invocation in the current implementation. If regex caching is added later, benchmark numbers will improve..."

This is factually incorrect. The codebase already has a thread-local LRU cache (capacity 64) for compiled regexes at `regexp.rs:10-14`:

```rust
thread_local! {
    static REGEX_CACHE: RefCell<LruCache<String, Regex>> = RefCell::new(
        LruCache::new(NonZeroUsize::new(64).unwrap())
    );
}
```

The `get_or_compile_regex` function (regexp.rs:16-26) checks this cache before compiling. With the benchmark using a fixed pattern `"\d+"`, the regex will be compiled once on the first call and cached for all subsequent iterations. The benchmark will primarily measure cached regex matching + LRU lookup, not compilation. This limitation note should be removed or corrected to describe what is actually being measured.

### C6. Tier A execution benchmarks use `app::run` which writes to stdout

The design says Tier A benchmarks "use real files and the existing `app.rs` pipeline (`app::run` or equivalent)." However, `app::run` always writes results to stdout via `Table::printstd()`, `Writer::from_writer(std::io::stdout())`, or JSON printing (app.rs:140-185). Every Criterion iteration would write to stdout, adding significant I/O overhead that dominates the benchmark measurement for small queries like E1 (`LIMIT 10`).

The design must specify either:
1. A new function that runs the pipeline and collects results into memory (e.g., `Vec<Record>`) without I/O -- this would need to be added to `app.rs` or the benchmark code would need to replicate the pipeline steps manually (requiring `bench-internals`).
2. Redirecting stdout to `/dev/null` within the benchmark iteration (hacky, not recommended).
3. Explicitly building the pipeline from parse -> plan -> execute -> collect within the benchmark code, using `bench-internals` exports.

Option 3 is most consistent with the design's approach but means Tier A benchmarks also depend on `bench-internals`, which contradicts the current framing of "use the existing `app.rs` pipeline."

## Suggestions (nice to have)

### S1. UDF "Direct call" benchmark has no API to access FunctionDef closures

The design says each UDF benchmark does both "Registry lookup + call" and "Direct call -- invokes the closure directly via a pre-resolved `FunctionDef` reference." However, `FunctionRegistry` does not expose a `get()` or `lookup()` method -- the `functions` HashMap is private (registry.rs:48). There is no way to obtain a `&FunctionDef` from outside the registry. Either:
- Add a `pub fn get(&self, name: &str) -> Option<&FunctionDef>` method to `FunctionRegistry`, or
- Drop the "direct call" sub-benchmark (the registry overhead is likely small and `call()` already measures the realistic hot path).

### S2. Consider adding a "warm cache" vs. "cold cache" note for regexp_like

Since regex caching exists (see C5), the benchmark will measure warm-cache performance after the first iteration. This is the realistic hot path for repeated queries but does not capture first-call cost. Consider either: (a) a note saying the benchmark measures steady-state performance, or (b) an additional benchmark for cold-cache (unique pattern per call) if regex compilation cost matters.

### S3. LimitStream benchmark (S4) may not be as trivial as described

The design says LimitStream "should be near-constant regardless of input size." This is correct for the LimitStream itself (it just counts rows), but the benchmark still needs to iterate through the source stream to produce rows. If the source is an InMemoryStream, the VecDeque pop_front cost is O(1) amortized, so S4 with 1K/10K/100K data sizes will show near-constant time per row (dominated by InMemoryStream.next()). However, if the goal is to show that LimitStream adds near-zero overhead, a clearer approach would be to benchmark InMemoryStream alone as a baseline and compare.

### S4. Synthetic data generation determinism should specify rand crate version

The design says `rand` 0.8 with `StdRng::seed_from_u64(42)`. Note that `StdRng` is an alias for `ChaCha12Rng` in rand 0.8 but the underlying algorithm may change across versions. For reproducibility across toolchain updates, consider using `ChaCha12Rng` directly (from `rand_chacha`), or document that exact reproducibility across rand version upgrades is not a goal.

### S5. Datasource benchmark line count arithmetic for JSONL

D5 states "3 lines -> replicated to 1002 lines." Cycling 3 lines to reach 1000 would give exactly 999 (333 * 3) or 1002 (334 * 3). The design says 1002 which implies 334 copies of 3 lines. This is fine but the "at least 1000 lines" goal would be clearer as "replicate to `ceil(1000 / source_lines) * source_lines`" in the implementation notes.

## Verified Claims (things I confirmed are correct)

1. **C1 from v1 properly fixed**: `query()` is indeed the top-level parser entry point at parser.rs:947. It is `pub(crate)` and handles UNION/INTERSECT/EXCEPT by chaining `select_query` calls. Using `query()` instead of `select_query()` is correct.

2. **C2 from v1 properly fixed**: There is no `SortStream`. ORDER BY is implemented inline in `Node::OrderBy` (types.rs:515-559) by materializing into Vec, calling `sort_by`, and wrapping in `InMemoryStream`. The limitation section accurately describes this.

3. **C4 from v1 properly fixed**: `round` is registered as `"round"` (lowercase) at arithmetic.rs:115 with `Arity::Range(1, 2)`. The test at arithmetic.rs:682 confirms `round(3.14159, 2)` returns `3.14`. This is a much better choice than the internal `Plus` operator.

4. **C5 from v1 properly fixed**: `date_part` is registered at datetime.rs:13 with `Arity::Exact(2)`. It is deterministic, taking a string unit and a DateTime value. The test at datetime.rs:395 confirms `date_part("month", <timestamp>)` returns `11.0` (as Float). This correctly replaces the non-deterministic `now()`.

5. **S7 from v1 adopted**: The multiple `[[bench]]` pattern is simpler and correctly specified.

6. **S2 from v1 adopted**: Line replication for small files is addressed with specific target counts.

7. **All six UDF functions exist and are registered**: `upper` (string.rs:23), `round` (arithmetic.rs:115), `date_part` (datetime.rs:13), `array_contains` (array.rs:96), `map_keys` (map.rs:8), `regexp_like` (regexp.rs:31) -- all confirmed.

8. **FunctionRegistry::call() does case-insensitive lookup**: Confirmed at registry.rs:104 via `to_ascii_lowercase()`.

9. **`functions::register_all()` and `FunctionRegistry` are already `pub`**: The `bench_udf` target correctly does not require `bench-internals`.

10. **ReaderBuilder struct is `pub` but methods are `pub(crate)`**: Confirmed at datasource.rs:610 (struct) vs. 620/632 (methods). This is relevant to C4 above.

11. **`bench-internals` is not a default feature**: The design correctly specifies it as a non-default feature, keeping the public API unchanged for normal consumers.

12. **S4 from v1 (Criterion measurement config)**: `.measurement_time(Duration::from_secs(10))` and `.sample_size(20)` are correctly specified for 100K-row benchmarks.
