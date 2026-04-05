VERDICT: NEEDS_REVISION

## Summary Assessment

The plan is well-structured and demonstrates strong understanding of the codebase, but has several critical issues: an inconsistency between the design doc and plan regarding `ParsingContext` (reference vs. `Arc`), incomplete enumeration of call sites that need the registry parameter threaded through, incorrect aggregate detection that misses `group_as`, missing the subquery parsing path for registry threading, and the `HttpRequest` field names in the test code do not match the actual struct fields.

## Critical Issues (must fix)

### 1. ParsingContext: Design says `&FunctionRegistry`, plan says `Arc<FunctionRegistry>` -- both break compilation

The design doc (section 3.2) specifies `ParsingContext` should hold `&FunctionRegistry` (a plain reference), arguing that the planner is synchronous so `Arc` is unnecessary. The plan (Step 5b-iii) says to use `Arc<FunctionRegistry>`. Both approaches have problems:

- **`&FunctionRegistry` (design)**: `ParsingContext` is `#[derive(Debug, Clone, PartialEq, Eq)]` (line 413 of `common/types.rs`). Adding a bare reference requires a lifetime parameter on `ParsingContext<'a>`, which propagates to every function that takes `&ParsingContext` -- including `parse_logic`, `parse_condition`, `parse_value_expression`, `parse_expression`, `parse_aggregate`, `parse_binary_operator`, `parse_unary_operator`, `parse_case_when_expression`, `build_from_node`, `check_env`, and more. This is doable but represents significantly more churn than the plan acknowledges.

- **`Arc<FunctionRegistry>` (plan)**: `ParsingContext` derives `PartialEq` and `Eq`. `FunctionRegistry` contains a `HashMap<String, FunctionDef>`, and `FunctionDef` contains `Box<dyn Fn(...)>` which does not implement `PartialEq` or `Eq`. Therefore `Arc<FunctionRegistry>` also does not implement `PartialEq`/`Eq`, and the derive will fail to compile. The plan mentions "Remove `#[derive(PartialEq, Eq)]`" but does not check whether `PartialEq`/`Eq` on `ParsingContext` is used anywhere. Searching the codebase, `ParsingContext` does not appear to be compared with `==` anywhere, so removing the derives is likely safe -- but the plan should explicitly verify this.

**Recommendation**: Use `Arc<FunctionRegistry>` in `ParsingContext`, and explicitly remove the `PartialEq, Eq` derives from `ParsingContext`. Verify no code compares `ParsingContext` values.

### 2. Subquery parsing path not updated

In `logical/parser.rs` line 317-319, the `Subquery` arm of `parse_value_expression` calls `parse_query(*stmt.clone(), ctx.data_source.clone())`. The plan says `parse_query` gains a registry parameter, but it does not mention updating this Subquery arm to pass `ctx.registry.clone()`. Without this change, subquery validation will not have access to the function registry.

### 3. Aggregate detection list is incomplete -- missing `group_as`

Step 6 defines `is_aggregate` with a hardcoded list:
```
"avg" | "count" | "first" | "last" | "max" | "min" | "sum"
| "approx_count_distinct" | "percentile_disc" | "approx_percentile"
```

This misses `group_as`, which is a valid aggregate function in this codebase (see `logical/parser.rs` line 829, `logical/types.rs` line 450). If a user calls `group_as(...)`, the planner would try to validate it as a scalar function, fail with "Unknown function", and produce a confusing error.

**However**, looking more carefully at the flow: `parse_aggregate` is called first (line 705), and if it fails with `NotAggregateFunction`, the code falls through to the non-aggregate path. The `is_aggregate` check in Step 6 is in `parse_value_expression`, which is a different code path. The `FuncCall` arm in `parse_value_expression` (line 251) is reached for scalar function contexts (e.g., inside WHERE, inside expressions). Aggregate functions in SELECT are handled by `parse_aggregate` first. So actually, `group_as` in a scalar context would be semantically wrong anyway. But the plan should use `from_str()` or a shared constant rather than a separate hardcoded list that could drift.

**Also**: The existing `from_str` function (line 374) does NOT include `percentile_disc` or `approx_percentile` -- these are handled via the `within_group_opt` path in `parse_aggregate` (line 401). So the plan's `is_aggregate` list includes names that `from_str` doesn't recognize, which is correct but fragile. A better approach would be to add a helper method like `is_aggregate_name()` alongside or replacing the hardcoded list.

### 4. HttpRequest struct field mismatch in test code

The plan's test in Step 4a creates an `HttpRequest` with fields `method`, `url`, `version`:
```rust
Value::HttpRequest(HttpRequest {
    method: "GET".to_string(),
    url,
    version: "HTTP/1.1".to_string(),
})
```

But the actual struct (line 93-97 of `common/types.rs`) uses `http_method`, `url`, `http_version`:
```rust
pub(crate) struct HttpRequest {
    pub(crate) http_method: String,
    pub(crate) url: url::Url,
    pub(crate) http_version: String,
}
```

This test code will not compile.

### 5. Step 5 missing: GroupByStream calls expression_value in stream.rs

The plan's Step 5b-vi lists `MapStream`, `FilterStream`, `GroupByStream`, `CrossJoinStream`, and `LeftJoinStream` as needing the registry. However, `GroupByStream::next()` (lines 298-466 of `stream.rs`) calls `expr.expression_value(&variables)?` extensively (lines 326, 337, 348, 359, 370, 381, 392, 403) inside the aggregate accumulation loop. The plan says GroupByStream needs the registry but does NOT enumerate these `expression_value` call sites.

Furthermore, when aggregates call `expression_value`, they also need the registry parameter. This means every `Aggregate::Avg`, `Aggregate::Count`, etc. arm in `GroupByStream::next()` that calls `expr.expression_value(&variables)?` must become `expr.expression_value(&variables, &self.registry)?`. The plan does not mention these call sites at all.

### 6. Formula::evaluate call sites not fully enumerated

The plan says `Formula::evaluate()` gets a registry parameter (Step 5b-v). But the plan does not enumerate ALL the internal call sites within `Formula::evaluate()` itself. Looking at the actual code (lines 804-918 of `execution/types.rs`), there are many calls to `expression_value()` within `Formula::evaluate()`:

- `Formula::IsNull` (line 837)
- `Formula::IsNotNull` (line 841)
- `Formula::IsMissing` (line 845)
- `Formula::IsNotMissing` (line 849)
- `Formula::ExpressionPredicate` (line 853)
- `Formula::Like` (lines 861-862)
- `Formula::NotLike` (lines 875-876)
- `Formula::In` (lines 889, 895)
- `Formula::NotIn` (line 914, via recursive call)

All of these need the registry parameter passed through. The plan mentions "All arms that call expression_value() pass registry through" in a comment, but this is handwaved -- an implementer following the plan might miss some of these.

### 7. lib.rs `#[macro_use] extern crate lazy_static` won't work across crate boundaries

The current `main.rs` (line 4-5) has:
```rust
#[macro_use]
extern crate lazy_static;
```

This `#[macro_use]` imports the `lazy_static!` macro into scope for the entire crate. When code moves to `lib.rs`, the `lazy_static!` macro invocations in `common/types.rs` (lines 12-17) use this crate-level import.

The plan's Step 17 correctly shows `#[macro_use] extern crate lazy_static;` in `lib.rs`. However, the plan does NOT address removing the `extern crate lazy_static` from `main.rs`. Since `main.rs` will now use `logq::` paths, having both a `lib.rs` and `main.rs` declare `extern crate lazy_static` could cause issues. The `main.rs` also has its own `lazy_static!` block (line 22-24 for `TABLE_SPEC_REGEX`), which needs the macro in scope. If `main.rs` changes to `use logq::...`, it can no longer rely on the `#[macro_use]` from `lib.rs`. The plan needs to either:
  - Keep `#[macro_use] extern crate lazy_static;` in `main.rs` (for `TABLE_SPEC_REGEX`)
  - Or move `TABLE_SPEC_REGEX` into the library

### 8. `explain()` function also needs registry

The plan mentions updating `explain()` "similarly" in Step 5b-ii but does not provide details. Looking at `app.rs` line 104-118, `explain()` calls `parse_query_top(q, data_source.clone())` which will need the registry parameter. It does NOT call `physical_plan.get(variables)` in a way that returns a stream (it just prints the plan), but `parse_query_top` still needs the registry for validation. The plan should explicitly enumerate this change.

### 9. `run_to_vec()` also needs updating

The `run_to_vec()` function (line 201-223 of `app.rs`, `#[cfg(test)]`) also calls `parse_query_top()` and `physical_plan.get(variables)`. This must be updated to create the registry and pass it through, or all existing tests will fail. The plan does not mention this function.

## Suggestions (nice to have)

### A. Step sizes are not truly "2-5 minutes"

Step 5 (threading Arc through the pipeline) is described as a single step but involves modifying 5 files with dozens of call sites. Even for an experienced Rust developer, this is more like 30-60 minutes of careful work. Consider splitting Step 5 into sub-steps:
  - 5a: Add `register_all()` to `mod.rs`
  - 5b: Update `ParsingContext` and parser signatures
  - 5c: Update `expression_value`, `Formula::evaluate`, `Relation::apply` signatures
  - 5d: Update `Node::get` and all stream structs
  - 5e: Update `app.rs` (`run`, `explain`, `run_to_vec`)
  - 5f: Delete old `evaluate()` functions

### B. `Concat` bug fix should be called out as a behavioral change

The plan notes that `Concat` currently checks Null before Missing (line 606-608 of `execution/types.rs`):
```rust
(Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
(Value::Missing, _) | (_, Value::Missing) => Ok(Value::Missing),
```
This means `Concat(Null, Missing)` returns `Null`, while the new registry-based implementation would return `Missing` (because `NullHandling::Propagate` gives Missing precedence). This is a deliberate behavioral change. The plan should add a test that explicitly documents this change, and confirm it does not break any existing tests.

### C. Consider `RegistryError` to `ParseError` mapping

Step 6 maps `RegistryError` variants to `ParseError` variants inline. Since `ParseError` already has `UnknownFunction(String)` and `InvalidArguments(String)` variants (lines 21-22 of `logical/parser.rs`), this mapping works. But a `From<RegistryError> for ParseError` impl would be cleaner and reusable.

### D. The `from_str` function name shadows standard library

The aggregate detection function is called `from_str` (line 374 of `logical/parser.rs`), which shadows `std::str::FromStr::from_str`. Consider renaming it to `parse_aggregate_name` or similar to avoid confusion, especially since the plan introduces a new `is_aggregate` check that duplicates its knowledge.

### E. `date_part` and `time_bucket` have `NullHandling::Custom` considerations

The plan says these use `NullHandling::Propagate`, but the existing `date_part` implementation does NOT handle Null/Missing -- it just returns `InvalidArguments` for non-matching types. With `NullHandling::Propagate`, Null/Missing are handled automatically before the closure runs, which changes behavior: previously `date_part('year', NULL)` returned `InvalidArguments`, now it returns `Null`. This is likely the desired behavior, but it should be explicitly tested.

### F. `f32` precision concerns

The plan uses `f32` for float arithmetic (matching the existing code), but many of the new velox functions (like `ln`, `exp`, `power`, `sqrt`) involve floating-point math where `f32` precision is notably limited. This is not a plan issue per se -- it matches the existing codebase -- but worth noting for future work.

## Verified Claims (things you confirmed are correct)

1. **File paths are accurate**: `src/execution/types.rs`, `src/execution/stream.rs`, `src/logical/parser.rs`, `src/app.rs`, `src/main.rs`, `src/common/types.rs` all exist at the stated paths.

2. **`src/functions/` does not exist yet**: Confirmed -- the `src/` directory contains only `app.rs`, `cli.yml`, `common/`, `execution/`, `logical/`, `main.rs`, `syntax/`.

3. **Binary operators are translated to Function calls**: Confirmed at `logical/parser.rs` lines 207-214 -- `parse_binary_operator` converts `+`, `-`, `*`, `/`, `||` into `Expression::Function("Plus"/"Minus"/"Times"/"Divide"/"Concat", args)`.

4. **`evaluate()` free function location**: Confirmed at line 388 of `execution/types.rs`. `evaluate_url_functions` at line 231, `evaluate_host_functions` at line 363.

5. **`coalesce`/`nullif` are desugared**: The design correctly notes these are handled by `syntax/desugar.rs` and never reach the function evaluator.

6. **`RecordStream` is a trait object**: Confirmed -- used as `Box<dyn RecordStream>` throughout (e.g., `stream.rs` line 115-118).

7. **Node::get signature**: Confirmed at line 938 -- `pub(crate) fn get(&self, variables: Variables) -> CreateStreamResult<Box<dyn RecordStream>>`.

8. **Aggregate enum variants**: Confirmed all 11 variants (Avg, Count, First, Last, Max, Min, Sum, ApproxCountDistinct, PercentileDisc, ApproxPercentile, GroupAs) at lines 1152-1164.

9. **`ParsingContext` struct**: Confirmed at lines 413-417 with `#[derive(Debug, Clone, PartialEq, Eq)]` and two fields: `table_name` and `data_source`.

10. **Existing tests in `app.rs`**: Confirmed test module starts at line 226 with multiple integration tests.

11. **Test command format**: `cargo test --lib functions::registry::tests -- --test-threads=1` is a valid Rust test command format for this project.

12. **`thiserror` dependency exists**: Confirmed in `Cargo.toml` line 21.

13. **Concat null precedence bug**: Confirmed at lines 606-608 -- Null is checked before Missing, giving Null incorrect precedence over Missing. The plan correctly identifies this as a behavioral fix.

14. **CrossJoinStream and LeftJoinStream call `node.get()`**: Confirmed at lines 666 and 774-775 of `stream.rs`.

15. **Edition 2018**: Confirmed in `Cargo.toml` line 13.
