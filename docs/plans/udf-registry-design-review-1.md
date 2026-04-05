VERDICT: NEEDS_REVISION

## Summary Assessment

The registry design is architecturally sound and well-motivated -- centralizing function dispatch is the right move. However, there are several critical issues: the type mapping in the design document is wrong for the existing `Value` enum, the `FunctionDef` uses `anyhow::Result` when the codebase uses custom error types throughout, the threading of `&FunctionRegistry` requires pervasive signature changes across the entire execution layer that are underspecified, and the design omits handling of domain-specific `Value` variants (`HttpRequest`, `Host`, `DateTime`) that existing functions depend on.

## Critical Issues (must fix)

### 1. Type mapping table is incorrect for the runtime Value enum

The design's type mapping table (Section 4) claims the runtime uses `Value::Int(i32)`, `Value::Float(f32)`, `Value::String`, `Value::Boolean`, `Value::DateTime`, `Value::Array`, `Value::Object`. These are correct. However, the design completely omits two `Value` variants that exist in `common::types::Value` and are actively used by existing functions:

- `Value::HttpRequest(HttpRequest)` -- used by all `url_*` functions (`url_host`, `url_port`, `url_path`, `url_fragment`, `url_query`, `url_path_segments`, `url_path_bucket`)
- `Value::Host(Host)` -- used by `host_name`, `host_port`

The design's Section 5 (Migration) says these URL functions will move to `url.rs`, but the `FunctionDef` closure signature `Fn(&[Value]) -> Result<Value>` uses `anyhow::Result` while the actual codebase returns `ExpressionResult<Value>`. More importantly, these functions pattern-match on `Value::HttpRequest` -- a domain-specific type -- meaning any ported function in `url.rs` must handle these types. The design needs to acknowledge these types exist and explain how they interact with functions "ported from velox's prestosql implementation" which know nothing about them.

### 2. Error type mismatch: `anyhow::Result` vs. `ExpressionResult`

The design uses `Result<Value>` (presumably `anyhow::Result<Value>`) for the `FunctionDef::func` closure and the `FunctionRegistry::call()` return type. But the codebase consistently uses its own error hierarchy:

- `ExpressionResult<Value>` (wrapping `ExpressionError`) in `execution/types.rs`
- `EvaluateResult` wrapping `EvaluateError`
- `StreamResult` wrapping `StreamError`
- `ParseResult` wrapping `ParseError`

The `expression_value()` method (line 134 of `execution/types.rs`) returns `ExpressionResult<Value>`, and the `evaluate()` function it calls also returns `ExpressionResult<Value>`. Switching the internal function return type to `anyhow::Result` would require either:
- Converting between error types at every call site (adding boilerplate), or
- Changing the entire error hierarchy (massive churn)

The design should specify which error type `FunctionDef::func` uses and how it integrates with the existing `ExpressionError` enum. Using `anyhow` only at the public API boundary while keeping `ExpressionError` internally would be more consistent.

### 3. Threading `&FunctionRegistry` through the execution pipeline is deeply underspecified

The design says: "The `evaluate()` method signature changes to accept `&FunctionRegistry`." This is a one-sentence description of what is actually a large, cascading change. Here is the actual call chain:

1. `app::run()` calls `physical_plan.get(variables)`
2. `Node::get()` creates stream objects (`FilterStream`, `MapStream`, `GroupByStream`, etc.)
3. Streams call `expression_value()` on `Expression` objects
4. `Expression::Function` arm calls the free function `evaluate(name, values)`

For `&FunctionRegistry` to reach step 4, it must be threaded through every single one of these layers:
- `Node::get()` must accept `&FunctionRegistry`
- Every `RecordStream` implementation (`FilterStream`, `MapStream`, `GroupByStream`, `CrossJoinStream`, `LeftJoinStream`, etc.) must store a reference to the registry
- `Expression::expression_value()` must accept `&FunctionRegistry`
- `Formula::evaluate()` must accept `&FunctionRegistry` (because formulas can contain `ExpressionPredicate` which evaluates expressions containing function calls)
- `Relation::apply()` must accept `&FunctionRegistry` (same reason)

This is easily 15+ signature changes across 2 files. More problematically, streams currently own `Box<Formula>` and `Vec<Named>` by value. To store a `&FunctionRegistry` reference, every stream struct needs a lifetime parameter, which means the `RecordStream` trait itself may need a lifetime parameter, fundamentally changing the trait object approach used throughout. Alternatively, the registry could be wrapped in `Arc<FunctionRegistry>` and cloned into each stream, but this is a different design than `&FunctionRegistry`.

The design must specify the ownership strategy: `&'a FunctionRegistry` (lifetime threading) vs `Arc<FunctionRegistry>` (shared ownership). Each has different tradeoffs and the choice affects the entire implementation.

### 4. Binary operators (Plus, Minus, Times, Divide, Concat) are dispatched through the same `evaluate()` function

The design's "After" code snippet (Section 3) shows replacing the entire `evaluate()` body with `registry.call(&name, &evaluated_args)`. But the current `evaluate()` function handles not only user-facing functions like `upper()` and `lower()` but also binary operators: `Plus`, `Minus`, `Times`, `Divide`, and `Concat`. These are passed through as function names from the logical planner (see `parse_binary_operator()` at line 206 of `logical/parser.rs`, which uses `(*op).to_string()` to get names like `"Plus"`, `"Minus"`).

The design must either:
- Register these operators in the registry too (but then `NullHandling::Propagate` needs to distinguish `Missing` vs `Null` precedence -- current code returns `Missing` when either arg is `Missing`, `Null` only when an arg is `Null` but neither is `Missing`, which is different from the "any Null/Missing -> return Null" specification), or
- Keep them as a separate code path in the executor, which complicates the "replace the entire match" story

### 5. Null propagation semantics are subtly wrong

The design says `NullHandling::Propagate` means "any Null/Missing arg -> return Null." But the existing codebase distinguishes between `Null` and `Missing` in its propagation:

```rust
// From current code for "upper" (line 624-628):
Value::Null => Ok(Value::Null),
Value::Missing => Ok(Value::Missing),
```

And for binary operators like `Plus` (line 404-408):
```rust
if matches!(&arguments[0], Value::Missing) || matches!(&arguments[1], Value::Missing) {
    return Ok(Value::Missing);  // Missing takes precedence
}
if matches!(&arguments[0], Value::Null) || matches!(&arguments[1], Value::Null) {
    return Ok(Value::Null);
}
```

The design's `Propagate` mode collapses these into "return Null" which is a behavioral regression. PartiQL distinguishes `NULL` from `MISSING` at the semantic level, and the existing code correctly preserves this distinction. The `Propagate` mode should return `Missing` if any arg is `Missing`, and `Null` if any arg is `Null` (with `Missing` having higher precedence).

## Suggestions (nice to have)

### 1. Consider the scope of the initial port

The design lists 100+ functions across 11 modules. This is a large surface area. Consider a phased approach: Phase 1 migrates existing functions to the registry + adds a few high-value new ones; Phase 2 ports the velox functions in batches. The initial PR should be shippable without requiring all modules to be complete.

### 2. The `register_all()` function uses `.unwrap()` for registration failures

If two modules accidentally register a function with the same name, this will panic at startup. Consider either collecting errors and returning `Result<FunctionRegistry>`, or just logging a warning and allowing the first registration to win. In a library context, panicking is particularly unfriendly.

### 3. Regex compilation in `regexp.rs` functions should be cached

Functions like `regexp_extract`, `regexp_like`, etc. will likely be called once per row. Compiling a regex per row is expensive. The design should consider either:
- A regex cache (LRU map keyed on pattern string)
- A way for functions to maintain state (which the current `Fn(&[Value])` signature does not support)

This applies to the existing `LIKE` implementation too but becomes more acute with `regexp_*` functions.

### 4. The `FuncCall` AST variant uses `Vec<SelectExpression>`, not `Vec<Expression>`

The AST's `FuncCall` is `FuncCall(FuncName, Vec<SelectExpression>, Option<WithinGroupClause>)`. Each `SelectExpression` can be either `Star` or `Expression(Box<Expression>, Option<String>)`. The design's argument count validation at the planner level needs to account for `SelectExpression::Star` args -- `registry.validate(&name, args.len())` would count a `Star` as one argument. The design should document whether `Star` is allowed for scalar functions or should be rejected at validation time.

### 5. `coalesce` and `nullif` are desugared before the planner

The design lists `coalesce` and `nullif` as candidates for `NullHandling::Custom`. However, in the current codebase, both are desugared into `CASE WHEN` expressions in `syntax/desugar.rs` (lines 88-93) before they ever reach the logical planner. They never appear as function calls at execution time. The design should acknowledge this and decide whether to keep the desugar approach (simpler, already works) or migrate them into the registry (more consistent but requires removing the desugaring, which is a working mechanism).

### 6. No `lib.rs` exists -- the project is a binary crate

The design assumes the project has `src/lib.rs` and proposes making modules `pub`. Currently the project only has `src/main.rs` with all modules declared as `mod` (not `pub mod`). To support library consumers, the project would need to be restructured into a library + binary crate (either via a `lib.rs` + `main.rs` split, or a workspace). This is not a trivial change and should be called out explicitly.

### 7. Function overloading (e.g., `reverse` for strings and arrays) is not supported

The design mentions `reverse` as both a string function and an array overload, but the registry uses a flat `HashMap<String, FunctionDef>` with no overloading support. A single `reverse` function would need to handle both types internally, which is fine for dynamic typing but should be documented as the pattern.

### 8. Missing validation: the `WithinGroupClause` in `FuncCall`

The design's `registry.validate(&name, args.len())` call at the planner does not account for the `Option<WithinGroupClause>` in the `FuncCall` AST variant. Functions like `percentile_disc` and `approx_percentile` use this clause. Since these remain as aggregates, this is probably fine, but the design should explicitly state that the validator ignores `WithinGroupClause` and explain why (because scalar functions never use it).

## Verified Claims (things you confirmed are correct)

1. **Current function dispatch is indeed hardcoded** -- The `evaluate()` function in `execution/types.rs` (line 388) uses a large `match` with string-based dispatch, exactly as described. It handles `Plus`, `Minus`, `Times`, `Divide`, `date_part`, `time_bucket`, `Concat`, then falls through to a case-insensitive match for `upper`, `lower`, `char_length`, `character_length`, `substring`, `trim`.

2. **URL functions exist and use `HttpRequest` values** -- `evaluate_url_functions()` (line 231) handles `url_host`, `url_port`, `url_path`, `url_fragment`, `url_query`, `url_path_segments`, `url_path_bucket`, all operating on `Value::HttpRequest`.

3. **Host functions exist separately** -- `evaluate_host_functions()` (line 363) handles `host_name` and `host_port` operating on `Value::Host`.

4. **The logical planner does not validate function names** -- `parse_value_expression()` (line 251) creates `Expression::Function(func_name.clone(), args)` without checking whether the function name is valid. Unknown function errors only surface at execution time.

5. **Aggregate functions use a different dispatch path** -- The `from_str()` function (line 374) in `logical/parser.rs` maps aggregate function names to `Aggregate` enum variants, which have their own stateful accumulator implementation. These are correctly identified as out-of-scope.

6. **The `parse_query_top` function signature** matches what the design describes -- it takes `ast::Query` and `DataSource`, not a registry reference. The design's proposed change to add `&registry` is straightforward at the call site.

7. **The `Value` enum in `common::types`** has `Int(i32)` and `Float(OrderedFloat<f32>)` as claimed in the type mapping table.

8. **The `Concat` operator remains as a binary operator** -- confirmed it's implemented in `evaluate()` (line 601) as the `"Concat"` match arm, separate from function calls.

9. **The project uses `anyhow` as a dependency** (Cargo.toml line 33), so using `anyhow::Result` in the registry is not a new dependency concern, but it is architecturally inconsistent with how the rest of the codebase handles errors.
