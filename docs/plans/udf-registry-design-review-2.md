VERDICT: NEEDS_REVISION

## Summary Assessment

The revised design resolves most of the original review's critical issues -- error types, Arc threading, null propagation semantics, and Value variant coverage are all substantially improved. However, the Subquery execution path is left as a hand-waving "implementation detail" when it is in fact a critical structural problem, and the existing Concat operator's null propagation behavior is inconsistent with the proposed uniform `NullHandling::Propagate` mode, meaning migration would silently change semantics.

## Critical Issues (must fix)

### 1. Subquery execution through `expression_value()` is broken with the proposed `&FunctionRegistry` signature

Section 3.5 shows `expression_value()` accepting `&FunctionRegistry` (a borrowed reference). But `Node::get()` (Section 3.3) requires `Arc<FunctionRegistry>` as an owned parameter. The existing `Expression::Subquery` arm (line 212-226 of `src/execution/types.rs`) calls `node.get(variables.clone())`. Under the new design, this would need `node.get(variables.clone(), registry.clone())`, but you cannot call `.clone()` on a `&FunctionRegistry` to produce an `Arc<FunctionRegistry>`.

The design acknowledges this problem at lines 486-492 but says only:

> Implementation detail: Subquery will need a separate path or the registry Arc stored alongside the node.

This is not an implementation detail -- it is a structural design problem. Either:
- `expression_value()` must accept `Arc<FunctionRegistry>` instead of `&FunctionRegistry` (but then every call site becomes an Arc clone), or
- The `Expression::Subquery` variant must store its own `Arc<FunctionRegistry>` (but `Expression` is `#[derive(Clone, PartialEq, Eq)]` and `Arc<FunctionRegistry>` would break `PartialEq`/`Eq` derives since `FunctionRegistry` contains closures), or
- `Node::get()` must accept `&FunctionRegistry` instead of `Arc<FunctionRegistry>` and each stream struct stores a raw pointer or unsafe reference (bad), or
- Some other approach

The design must specify a concrete solution. A likely workable approach: pass `Arc<FunctionRegistry>` through `expression_value()` and `formula.evaluate()` as well (not just `&FunctionRegistry`), and have each call site pass `self.registry.clone()`. The Arc clone is cheap (refcount bump) and avoids the lifetime/ownership mismatch.

### 2. Concat operator has opposite null precedence from Plus/Minus/Times/Divide -- silent behavioral change

The design registers Concat with `NullHandling::Propagate`, which returns Missing if any arg is Missing (Missing takes precedence over Null). But the current Concat implementation at lines 601-616 of `src/execution/types.rs` does the opposite:

```rust
(Value::Null, _) | (_, Value::Null) => Ok(Value::Null),    // Null checked FIRST
(Value::Missing, _) | (_, Value::Missing) => Ok(Value::Missing),
```

If arg0 is `Missing` and arg1 is `Null`, the current code returns `Null` (Null takes precedence). The proposed `NullHandling::Propagate` would return `Missing` (Missing takes precedence). This is a semantic behavior change that could affect existing queries operating on log data with mixed Null/Missing values.

The design must either:
- Acknowledge and justify this as a deliberate correction (Concat should follow the same Missing>Null precedence as the arithmetic operators, and the existing behavior is a bug), or
- Register Concat with `NullHandling::Custom` to preserve its existing behavior, or
- Document this as a known behavioral change with a migration note

For reference, Plus/Minus/Times/Divide all check Missing first (Missing takes precedence), so Concat is arguably the one with the bug. But this should be called out explicitly rather than silently changed.

## Suggestions (nice to have)

### 1. The `RegistryError` type may need a `From` conversion to `ExpressionError`

The `call()` method returns `ExpressionResult<Value>` (wrapping `ExpressionError`), but internally it constructs `ExpressionError::UnknownFunction` when a function is not found. The design should clarify whether `RegistryError` is ever surfaced from `call()` or whether `call()` always translates registry errors into `ExpressionError` variants. Currently the `call()` code in Section 1 uses `ExpressionError::UnknownFunction` directly, which is consistent. But if someone calls `registry.validate()` and then `registry.call()`, the error types differ (`RegistryError` vs `ExpressionError`). Consider adding `From<RegistryError> for ExpressionError` or unifying the error types.

### 2. The planner validation code (Section 3.2) has a logic error in aggregate detection

The proposed planner code at line 318 shows:

```rust
if from_str(func_name, /* dummy */ ).is_err() {
```

But `from_str()` in the actual codebase (line 374 of `logical/parser.rs`) has signature `fn from_str(value: &str, named: types::Named) -> ParseResult<types::Aggregate>`. This requires a real `Named` argument, not a "dummy." The function internally constructs aggregate variants that wrap the `Named`. You cannot pass a dummy and get a meaningful result.

A better approach: create a `is_aggregate(name: &str) -> bool` function that checks just the name string without constructing an `Aggregate` value, and use it for the guard check.

### 3. The `Formula` enum still uses `InfixOperator`/`PrefixOperator` in the logical layer

The design shows `Formula::evaluate()` with `And`, `Or`, `Not` arms. This matches the **execution** layer's `Formula` enum (in `execution/types.rs`). The **logical** layer's `Formula` enum (in `logical/types.rs`) uses `InfixOperator(LogicInfixOp, ...)` and `PrefixOperator(LogicPrefixOp, ...)` which get converted during the `physical()` step. The design's code is correct for the execution layer, but it might be worth noting this distinction to avoid confusion during implementation.

### 4. Consider `OrderByStream` and its expression evaluation for computed ORDER BY keys

The design says OrderBy doesn't need the registry. While the current `Node::OrderBy` implementation sorts by field lookup (record.get(column_name)), if ORDER BY ever supports computed expressions (e.g., `ORDER BY upper(name)`), it would need the registry. This is not a current problem but is worth noting as a limitation.

### 5. The `thread_local!` regex cache is fine but the `Regex::clone()` cost should be noted

In the regex caching code (Section 4), `get_or_compile_regex()` returns `Ok(re.clone())` after a cache hit. `Regex::clone()` is not free -- it allocates a new compiled NFA/DFA structure. Consider returning a reference from the cache instead (e.g., using `Rc<Regex>` values in the cache so `.clone()` is cheap). Alternatively, restructure the API so the regex is used within the `with()` closure rather than returned out of it.

### 6. The `url_*` functions currently do NOT propagate Null/Missing

The existing url functions (e.g., `url_host` at line 232-248) pattern-match only on `Value::HttpRequest`, returning `Err(ExpressionError::InvalidArguments)` for any other variant, including Null and Missing. The design registers them with `NullHandling::Custom`. This means the closures must handle Null/Missing explicitly, which the current code does not do. Make sure the migration adds Missing/Null handling to these functions' closures. Same applies to `host_name` and `host_port`.

## Verified Claims (things you confirmed are correct)

1. **Error type mismatch is fixed**: The design now uses `ExpressionResult<Value>` (wrapping `ExpressionError`) throughout the registry, matching the existing error hierarchy in `execution/types.rs` (line 82-108). The `FunctionDef::func` closure returns `ExpressionResult<Value>`, and `FunctionRegistry::call()` returns the same. This is consistent with `expression_value()` (line 134).

2. **`Arc<FunctionRegistry>` is the right ownership strategy**: The `RecordStream` trait (line 115-118 of `stream.rs`) is used as `Box<dyn RecordStream>` throughout. Adding a lifetime parameter would require `Box<dyn RecordStream + 'a>` everywhere, which would cascade into every stream struct and the trait itself. `Arc` is the pragmatic choice.

3. **Null propagation semantics are fixed for arithmetic operators**: The `NullHandling::Propagate` implementation in `call()` correctly returns Missing if any arg is Missing (higher precedence), then Null if any arg is Null. This matches the existing behavior for Plus/Minus/Times/Divide (lines 404-408 of `execution/types.rs`), where Missing is checked first.

4. **Binary operators are correctly identified as registry candidates**: The logical planner's `parse_binary_operator()` (line 207 of `logical/parser.rs`) translates `Plus`, `Minus`, `Times`, `Divide`, and `Concat` into `Expression::Function(func_name, args)` using `(*op).to_string()`. This means they already flow through the same `Expression::Function` path as named functions, making registry dispatch natural.

5. **HttpRequest and Host Value variants are now documented**: The type mapping table includes `HttpRequest` and `Host`, and the design correctly explains that existing url_*/host_* functions pattern-match on these domain-specific types while new velox-ported functions operate on `Value::String`.

6. **coalesce/nullif exclusion is correct**: Both are desugared in `syntax/desugar.rs` before reaching the logical planner. They never appear as function calls at execution time. Keeping the existing desugaring mechanism is simpler and correct.

7. **`register_all()` returning `Result` is correct**: Changed from `.unwrap()` to propagating `Result<FunctionRegistry, RegistryError>`, preventing panics on duplicate registrations.

8. **Streams that don't evaluate expressions are correctly identified**: `LimitStream`, `InMemoryStream`, `LogFileStream`, `ProjectionStream`, `DistinctStream`, `UnionStream`, `IntersectStream`, `ExceptStream` -- none of these call `expression_value()` or `formula.evaluate()`. Confirmed by reading the full `stream.rs` source. They do not need the registry field.

9. **The `WithinGroupClause` exclusion is correct**: Only `percentile_disc` and `approx_percentile` use it, and both are handled via the aggregate parsing path (`parse_aggregate()` at line 395 of `logical/parser.rs`), not through the scalar function registry.

10. **`parse_query_top` signature change is feasible**: The function (line 614 of `logical/parser.rs`) currently takes `(q: ast::Query, data_source: common::DataSource)`. Adding `&FunctionRegistry` is straightforward. For subqueries, the recursive call to `parse_query` at line 318 would pass `ctx.registry`, which is the borrowed reference from `ParsingContext`. The `ParsingContext` struct would need a lifetime parameter for the borrow, but since it's only used locally within synchronous parsing functions, this is not problematic.

11. **The `from_str()` function correctly separates aggregates from scalar functions**: At line 374 of `logical/parser.rs`, `from_str()` maps aggregate names to `Aggregate` variants and returns `Err(ParseError::NotAggregateFunction)` for non-aggregates. The design's proposed validation only runs when `from_str()` returns an error, correctly avoiding conflicts between the two dispatch paths.
