# User-Defined Functions for logq (v2)

## Changes from previous version

This revision addresses all critical issues and suggestions from the design review:

1. **Missing Value variants (HttpRequest, Host)**: Added `HttpRequest` and `Host` to the type mapping table. Explained that `url.rs` and `host.rs` functions pattern-match on these domain-specific types directly, and that new velox-ported functions do not need to handle them.

2. **Error type mismatch**: Changed the `FunctionDef::func` closure and all registry methods from `anyhow::Result` to `ExpressionResult<Value>` (wrapping `ExpressionError`), matching the existing codebase error hierarchy.

3. **`&FunctionRegistry` threading underspecified**: Chose `Arc<FunctionRegistry>` over lifetime parameters. The `RecordStream` trait is a trait object (`Box<dyn RecordStream>`) making lifetime threading impractical. Specified every concrete struct, method, and enum variant that must change.

4. **Binary operators dispatched through `evaluate()`**: Binary operators (`Plus`, `Minus`, `Times`, `Divide`, `Concat`) are registered in the registry alongside regular functions. Their null propagation uses the new `NullHandling::Propagate` mode which correctly distinguishes Missing vs Null.

5. **Null propagation semantics**: Fixed `NullHandling::Propagate` to return `Missing` if any argument is `Missing` (higher precedence), and `Null` if any argument is `Null` but none is `Missing`. This preserves the existing PartiQL-correct behavior.

6. **No lib.rs exists**: Removed the `src/lib.rs` assumption. The public API is exposed by creating a `src/lib.rs` alongside the existing `src/main.rs`, with Cargo's automatic binary+library dual-target support.

7. **coalesce/nullif already desugared**: Acknowledged that these are handled by `syntax/desugar.rs` and will not be registered in the function registry.

8. **Regex caching for `regexp_*` functions**: Added a `thread_local!` LRU regex cache design for regexp functions.

9. **FuncCall uses `Vec<SelectExpression>` not `Vec<Expression>`**: Documented that `SelectExpression::Star` is rejected for scalar functions during validation. The validator counts non-Star arguments and rejects Star with a clear error.

10. **Function overloading via dynamic dispatch**: Documented that overloaded functions (e.g., `reverse` for strings and arrays) are handled by a single registration whose closure dispatches on `Value` variant at runtime.

11. **`WithinGroupClause` validation**: Explicitly stated that the scalar function validator ignores this clause because only aggregate functions use it.

12. **`register_all()` uses `.unwrap()`**: Changed to return `Result<FunctionRegistry>` so that duplicate name registration errors propagate cleanly instead of panicking.

---

## Overview

Add a plugin-style function registry to logq that replaces the current hardcoded function dispatch in `execution/types.rs`. The registry provides a public Rust API for registering scalar functions, ships with a comprehensive set of built-in functions ported from velox's prestosql implementation, and validates function calls at planning time.

## Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Extensibility model | Rust API (library crate) | Users build custom binaries; clean, no FFI/ABI concerns |
| Dispatch strategy | Name-only, dynamic typing | Functions receive `&[Value]`, handle types internally; matches logq's runtime model |
| Null handling | Convention-based with opt-out | Auto null propagation by default with correct Missing/Null precedence; `NullHandling::Custom` for functions that need full control |
| Pipeline integration | Validate at planning, dispatch at execution | Early error messages without over-engineering the planner |
| JSON type | Treat as String (Presto semantics) | No type system changes; parse on each call; matches Presto/Trino behavior |
| Registry ownership | `Arc<FunctionRegistry>` | Streams are trait objects (`Box<dyn RecordStream>`); lifetime parameters would require parameterizing the trait itself and all consumers |
| Error type | `ExpressionResult<Value>` | Consistent with the existing error hierarchy throughout the execution layer |
| Binary operators | Registered in the registry | Unifies all function-like dispatch through one path; keeps `evaluate()` clean |

## 1. Registry Core

### Types

```rust
// src/functions/registry.rs

use std::sync::Arc;
use crate::execution::types::{ExpressionResult, ExpressionError};
use crate::common::types::Value;

pub enum NullHandling {
    /// Propagate with correct Missing/Null precedence per PartiQL semantics:
    ///   - If any arg is Missing -> return Missing
    ///   - Else if any arg is Null -> return Null
    ///   - Otherwise, invoke the function body
    Propagate,
    /// Function handles Null/Missing itself (e.g., functions with special null behavior)
    Custom,
}

pub enum Arity {
    Exact(usize),
    Range(usize, usize),  // min, max inclusive
    Variadic(usize),       // minimum args, no maximum
}

pub struct FunctionDef {
    pub name: String,
    pub arity: Arity,
    pub null_handling: NullHandling,
    pub func: Box<dyn Fn(&[Value]) -> ExpressionResult<Value> + Send + Sync>,
}

pub struct FunctionRegistry {
    functions: HashMap<String, FunctionDef>,  // keys are lowercased
}
```

### Registry API

```rust
impl FunctionRegistry {
    pub fn new() -> Self;

    /// Register a function. Errors if name already taken.
    pub fn register(&mut self, def: FunctionDef) -> Result<(), RegistryError>;

    /// Validate function exists and arity matches. Called during logical planning.
    /// Returns Ok(()) or a descriptive ParseError.
    pub fn validate(&self, name: &str, arg_count: usize) -> Result<(), RegistryError>;

    /// Look up and invoke the function. Applies null propagation for Propagate functions.
    pub fn call(&self, name: &str, args: &[Value]) -> ExpressionResult<Value>;
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    #[error("Unknown function: {0}")]
    UnknownFunction(String),
    #[error("Function {name} expects {expected} argument(s), got {actual}")]
    ArityMismatch { name: String, expected: String, actual: usize },
    #[error("Duplicate function registration: {0}")]
    DuplicateFunction(String),
    #[error("Star (*) is not allowed as an argument to scalar function {0}")]
    StarNotAllowed(String),
}
```

### Null Propagation Implementation

The `call()` method implements `NullHandling::Propagate` with correct Missing/Null precedence:

```rust
pub fn call(&self, name: &str, args: &[Value]) -> ExpressionResult<Value> {
    let key = name.to_ascii_lowercase();
    let def = self.functions.get(&key)
        .ok_or(ExpressionError::UnknownFunction)?;

    match def.null_handling {
        NullHandling::Propagate => {
            // Missing takes precedence over Null (PartiQL semantics)
            let mut has_null = false;
            for arg in args {
                match arg {
                    Value::Missing => return Ok(Value::Missing),
                    Value::Null => has_null = true,
                    _ => {}
                }
            }
            if has_null {
                return Ok(Value::Null);
            }
            (def.func)(args)
        }
        NullHandling::Custom => {
            (def.func)(args)
        }
    }
}
```

This preserves the existing behavior where, for example, `Plus` returns `Missing` if either argument is `Missing`, and returns `Null` only if an argument is `Null` but neither is `Missing`.

Function names are lowercased on registration and lookup for case-insensitive matching.

## 2. Module Structure

```
src/functions/
  mod.rs              -- re-exports, register_all()
  registry.rs         -- FunctionRegistry, FunctionDef, Arity, NullHandling, RegistryError
  arithmetic.rs       -- Plus, Minus, Times, Divide (binary operators),
                          abs, ceil, floor, round, power, mod/modulus, sqrt, sign, truncate,
                          ln, log2, log10, exp, rand, pi, e, infinity, nan, is_nan, is_finite,
                          is_infinite, radians, degrees, sin, cos, tan, asin, acos, atan, atan2,
                          cosh, tanh
  string.rs           -- Concat (binary operator), upper, lower, char_length, character_length,
                          substring, trim, replace, split, split_part, starts_with, ends_with,
                          reverse, concat (multi-arg function), lpad, rpad, position/strpos,
                          repeat, ltrim, rtrim, chr, codepoint, word_stem, normalize,
                          to_utf8, from_utf8, soundex, levenshtein_distance, hamming_distance
  bitwise.rs          -- bitwise_and, bitwise_or, bitwise_xor, bitwise_not,
                          bit_count, bitwise_shift_left, bitwise_shift_right,
                          bitwise_left_shift, bitwise_right_shift,
                          bitwise_arithmetic_shift_right, bitwise_logical_shift_right
  regexp.rs           -- regexp_extract, regexp_extract_all, regexp_like, regexp_replace,
                          regexp_split, regexp_count (with thread-local regex caching)
  datetime.rs         -- date_trunc, date_add, date_diff, year, month, day, hour, minute,
                          second, millisecond, quarter, week, day_of_week, day_of_year,
                          now, from_unixtime, to_unixtime, date_format, date_parse,
                          time_bucket, date_part (existing functions migrated here)
  array.rs            -- array_sort, array_distinct, array_contains, array_join,
                          array_min, array_max, array_position, cardinality, flatten,
                          array_intersect, array_union, array_except, array_remove,
                          reverse (array overload, handled in the same `reverse` registration
                          in string.rs via dynamic type dispatch), slice, sequence, repeat,
                          zip, array_average, array_sum, array_normalize,
                          array_cum_sum, array_frequency, array_duplicates, array_has_duplicates,
                          combinations, ngrams, shuffle
  map.rs              -- map_keys, map_values, map_entries, element_at, map_concat,
                          map_filter, map_zip_with, transform_keys, transform_values,
                          map_from_entries, multimap_from_entries
  json.rs             -- json_extract, json_extract_scalar, json_array_length,
                          json_array_contains, json_size, json_format, json_parse,
                          is_json_scalar
  url.rs              -- url_host, url_port, url_path, url_fragment, url_query,
                          url_path_segments, url_path_bucket,
                          url_extract_host, url_extract_port, url_extract_path,
                          url_extract_fragment, url_extract_query, url_extract_parameter,
                          url_extract_protocol, url_encode, url_decode
                          (existing url_* functions operate on Value::HttpRequest;
                          new url_extract_* functions operate on Value::String)
  host.rs             -- host_name, host_port (operate on Value::Host)
  type_conversion.rs  -- typeof, try_cast, to_base, from_base, format_number
```

Each file exports:
```rust
pub fn register(registry: &mut FunctionRegistry) -> Result<(), RegistryError>;
```

Top-level orchestration:
```rust
// src/functions/mod.rs
pub fn register_all() -> Result<FunctionRegistry, RegistryError> {
    let mut registry = FunctionRegistry::new();
    arithmetic::register(&mut registry)?;
    string::register(&mut registry)?;
    bitwise::register(&mut registry)?;
    regexp::register(&mut registry)?;
    datetime::register(&mut registry)?;
    array::register(&mut registry)?;
    map::register(&mut registry)?;
    json::register(&mut registry)?;
    url::register(&mut registry)?;
    host::register(&mut registry)?;
    type_conversion::register(&mut registry)?;
    Ok(registry)
}
```

## 3. Pipeline Integration

### Ownership Strategy: `Arc<FunctionRegistry>`

The `RecordStream` trait is used as a trait object (`Box<dyn RecordStream>`) throughout the execution layer. Adding a lifetime parameter to `RecordStream` would require parameterizing every stream struct, the trait itself, and every `Box<dyn RecordStream>` usage -- a fundamental redesign of the execution model.

Instead, the registry is wrapped in `Arc<FunctionRegistry>` once at startup and cloned (cheaply, as Arc clone is just a refcount bump) into each component that needs it.

### Concrete Changes Required

#### 3.1 Startup (src/app.rs)

```rust
use std::sync::Arc;
use crate::functions;

pub(crate) fn run(
    query_str: &str,
    data_source: common::types::DataSource,
    output_mode: OutputMode,
) -> AppResult<()> {
    let registry = Arc::new(functions::register_all()
        .map_err(|e| AppError::Registry(e))?);

    let (rest_of_str, q) = syntax::parser::query(&query_str)?;
    if !rest_of_str.is_empty() {
        return Err(AppError::InputNotAllConsumed(rest_of_str.to_string()));
    }
    let q = syntax::desugar::desugar_query(q);

    let node = logical::parser::parse_query_top(q, data_source.clone(), &registry)?;
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new(data_source);
    let (physical_plan, variables) = node.physical(&mut physical_plan_creator)?;

    let mut stream = physical_plan.get(variables, registry.clone())?;
    // ... rest unchanged
}

/// For library consumers with custom functions
pub fn run_with_registry(
    query_str: &str,
    registry: Arc<FunctionRegistry>,
    data_source: common::types::DataSource,
    output_mode: OutputMode,
) -> AppResult<()> {
    // same pipeline, uses provided registry
}
```

Add `Registry` variant to `AppError`:
```rust
pub(crate) enum AppError {
    // ... existing variants ...
    #[error("{0}")]
    Registry(#[from] functions::registry::RegistryError),
}
```

#### 3.2 Logical Planner Validation (src/logical/parser.rs)

The `parse_query_top` and `parse_query` functions gain a `&FunctionRegistry` parameter. The `ParsingContext` struct is extended:

```rust
pub(crate) struct ParsingContext {
    pub(crate) table_name: String,
    pub(crate) data_source: DataSource,
    pub(crate) registry: Arc<FunctionRegistry>,  // Arc, not &ref -- ParsingContext derives Clone and is used recursively for subqueries
}
```

Note: Although the planner runs synchronously, `ParsingContext` derives `Clone` and is passed to recursive `parse_query()` calls for subqueries, making `Arc` the practical choice. The `PartialEq`/`Eq` derives are removed (no code compares `ParsingContext` for equality). A manual `Debug` impl is added since `FunctionRegistry` contains closures.

In `parse_value_expression`, the `FuncCall` arm adds validation:

```rust
ast::Expression::FuncCall(func_name, select_exprs, _within_group_opt) => {
    // Reject Star arguments for scalar functions
    for se in select_exprs.iter() {
        if matches!(se, ast::SelectExpression::Star) {
            return Err(ParseError::InvalidArguments(
                format!("Star (*) not allowed as argument to scalar function {}", func_name)
            ));
        }
    }

    // Validate function exists and arity matches (if not an aggregate)
    // is_aggregate() checks just the name string without constructing an Aggregate
    if !is_aggregate(func_name) {
        // Not an aggregate, so validate as a scalar function
        ctx.registry.validate(func_name, select_exprs.len())
            .map_err(|e| match e {
                RegistryError::UnknownFunction(name) => ParseError::UnknownFunction(name),
                RegistryError::ArityMismatch { name, expected, actual } =>
                    ParseError::InvalidArguments(
                        format!("{} expects {} argument(s), got {}", name, expected, actual)
                    ),
                RegistryError::StarNotAllowed(name) =>
                    ParseError::InvalidArguments(
                        format!("Star (*) not allowed for {}", name)
                    ),
                _ => ParseError::InvalidArguments(e.to_string()),
            })?;
    }

    let mut args = Vec::new();
    for select_expr in select_exprs.iter() {
        let arg = parse_expression(ctx, select_expr)?;
        args.push(*arg);
    }
    Ok(Box::new(types::Expression::Function(func_name.clone(), args)))
}
```

The `WithinGroupClause` is deliberately not validated here because it is only used by aggregate functions (`percentile_disc`, `approx_percentile`), which have their own parsing path in `parse_aggregate()` and are not registered in the scalar function registry.

#### 3.3 Execution Layer: `Node::get()` (src/execution/types.rs)

```rust
impl Node {
    pub(crate) fn get(
        &self,
        variables: Variables,
        registry: Arc<FunctionRegistry>,
    ) -> CreateStreamResult<Box<dyn RecordStream>> {
        match self {
            Node::Filter(source, formula) => {
                let record_stream = source.get(variables.clone(), registry.clone())?;
                let stream = FilterStream::new(
                    *formula.clone(), variables, record_stream, registry,
                );
                Ok(Box::new(stream))
            }
            Node::Map(named_list, source) => {
                let record_stream = source.get(variables.clone(), registry.clone())?;
                let stream = MapStream::new(
                    named_list.clone(), variables, record_stream, registry,
                );
                Ok(Box::new(stream))
            }
            // ... all other arms pass registry.clone() through
        }
    }
}
```

#### 3.4 Stream Structs (src/execution/stream.rs)

Each stream struct that calls `expression_value()` or `formula.evaluate()` gains an `Arc<FunctionRegistry>` field:

```rust
pub(crate) struct MapStream {
    pub(crate) named_list: Vec<Named>,
    pub(crate) variables: Variables,
    pub(crate) source: Box<dyn RecordStream>,
    pub(crate) registry: Arc<FunctionRegistry>,  // NEW
}

pub(crate) struct FilterStream {
    formula: Formula,
    variables: Variables,
    source: Box<dyn RecordStream>,
    registry: Arc<FunctionRegistry>,  // NEW
}

pub(crate) struct GroupByStream {
    keys: Vec<ast::PathExpr>,
    variables: Variables,
    aggregates: Vec<NamedAggregate>,
    source: Box<dyn RecordStream>,
    group_iterator: Option<hash_set::IntoIter<Option<Tuple>>>,
    registry: Arc<FunctionRegistry>,  // NEW
}

pub(crate) struct CrossJoinStream {
    left: Box<dyn RecordStream>,
    right_node: Node,
    right_variables: Variables,
    current_left: Option<Record>,
    right_stream: Option<Box<dyn RecordStream>>,
    registry: Arc<FunctionRegistry>,  // NEW
}

pub(crate) struct LeftJoinStream {
    left: Box<dyn RecordStream>,
    right_node: Node,
    right_variables: Variables,
    condition: Formula,
    current_left: Option<Record>,
    right_stream: Option<Box<dyn RecordStream>>,
    matched: bool,
    right_field_names: Option<Vec<String>>,
    registry: Arc<FunctionRegistry>,  // NEW
}
```

The `RecordStream` trait itself does not change -- it remains:
```rust
pub(crate) trait RecordStream {
    fn next(&mut self) -> StreamResult<Option<Record>>;
    fn close(&self);
}
```

Streams that never evaluate expressions (e.g., `LimitStream`, `InMemoryStream`, `LogFileStream`, `ProjectionStream`, `DistinctStream`, `UnionStream`, `IntersectStream`, `ExceptStream`) do not need the registry field.

#### 3.5 Expression and Formula Methods (src/execution/types.rs)

```rust
impl Expression {
    pub(crate) fn expression_value(
        &self,
        variables: &Variables,
        registry: &Arc<FunctionRegistry>,  // Arc, not &ref -- needed for Subquery cloning
    ) -> ExpressionResult<Value> {
        match self {
            Expression::Function(name, arguments) => {
                let mut values: Vec<Value> = Vec::new();
                for arg in arguments.iter() {
                    match arg {
                        Named::Expression(expr, _) => {
                            let value = expr.expression_value(variables, registry)?;
                            values.push(value);
                        }
                        Named::Star => {
                            return Err(ExpressionError::InvalidStar);
                        }
                    }
                }
                registry.call(name, &values)
            }
            Expression::Logic(formula) => {
                let out = formula.evaluate(variables, registry)?;
                match out {
                    Some(b) => Ok(Value::Boolean(b)),
                    None => Ok(Value::Null),
                }
            }
            Expression::Branch(branches, else_expr) => {
                for (formula, then_expr) in branches {
                    let result = formula.evaluate(variables, registry)?;
                    if result == Some(true) {
                        return then_expr.expression_value(variables, registry);
                    }
                }
                match else_expr {
                    Some(expr) => expr.expression_value(variables, registry),
                    None => Ok(Value::Null),
                }
            }
            Expression::Cast(inner, cast_type) => {
                let val = inner.expression_value(variables, registry)?;
                // ... cast logic unchanged
            }
            Expression::Subquery(node) => {
                // expression_value receives Arc<FunctionRegistry>, so we can
                // clone it for the subquery's stream construction.
                let mut stream = node.get(variables.clone(), registry.clone())
                    .map_err(|e| ExpressionError::SubqueryError(e.to_string()))?;
                match stream.next() {
                    Ok(Some(record)) => {
                        // extract scalar value from first column
                        // ... (unchanged logic)
                    }
                    Ok(None) => Ok(Value::Null),
                    Err(e) => Err(ExpressionError::SubqueryError(e.to_string())),
                }
            }
            // Constant and Variable unchanged
            _ => { /* unchanged */ }
        }
    }
}

impl Formula {
    pub(crate) fn evaluate(
        &self,
        variables: &Variables,
        registry: &Arc<FunctionRegistry>,
    ) -> EvaluateResult<Option<bool>> {
        // All arms that call expression_value() pass registry through.
        // Arms that recurse (And, Or, Not) pass registry to child evaluate() calls.
        // ...
    }
}

impl Relation {
    pub(crate) fn apply(
        &self,
        variables: &Variables,
        left: &Expression,
        right: &Expression,
        registry: &Arc<FunctionRegistry>,
    ) -> ExpressionResult<Option<bool>> {
        let left_result = left.expression_value(variables, registry)?;
        let right_result = right.expression_value(variables, registry)?;
        // ... comparison logic unchanged
    }
}
```

**Summary of all signature changes:**

| Type | Method | Change |
|---|---|---|
| `Expression` | `expression_value()` | Add `registry: &Arc<FunctionRegistry>` parameter |
| `Formula` | `evaluate()` | Add `registry: &Arc<FunctionRegistry>` parameter |
| `Relation` | `apply()` | Add `registry: &Arc<FunctionRegistry>` parameter |
| `Node` (execution) | `get()` | Add `registry: Arc<FunctionRegistry>` parameter |
| `MapStream` | `new()`, struct | Add `registry: Arc<FunctionRegistry>` field |
| `FilterStream` | `new()`, struct | Add `registry: Arc<FunctionRegistry>` field |
| `GroupByStream` | `new()`, struct | Add `registry: Arc<FunctionRegistry>` field |
| `CrossJoinStream` | `new()`, struct | Add `registry: Arc<FunctionRegistry>` field |
| `LeftJoinStream` | `new()`, struct | Add `registry: Arc<FunctionRegistry>` field |

Each `RecordStream::next()` implementation that calls `expression_value()` or `formula.evaluate()` passes `&self.registry` (the `Arc<FunctionRegistry>` by reference). This allows `expression_value()` to clone the Arc when constructing subquery streams, while avoiding unnecessary clones in the common case.

#### 3.6 Removing the `evaluate()` Free Function

The current free function `evaluate()` in `execution/types.rs` (line 388) is deleted entirely. All dispatch goes through `registry.call()`. The `evaluate_url_functions()` and `evaluate_host_functions()` helper functions are also deleted; their logic moves into the closures registered in `url.rs` and `host.rs`.

## 4. Function Porting Approach

### Type Mapping

| Velox type | logq `Value` variant | Notes |
|---|---|---|
| int8/16/32/64 | `Int(i32)` | |
| float/double | `Float(OrderedFloat<f32>)` | |
| varchar | `String(String)` | |
| boolean | `Boolean(bool)` | |
| timestamp | `DateTime(chrono::DateTime<...>)` | |
| array(T) | `Array(Vec<Value>)` | |
| map(K,V) / row | `Object(LinkedHashMap<String, Value>)` | |
| json | `String(String)` | Parsed on demand |
| *(no velox equiv)* | `HttpRequest(HttpRequest)` | logq-specific; used by `url_host`, `url_port`, etc. |
| *(no velox equiv)* | `Host(Host)` | logq-specific; used by `host_name`, `host_port` |
| *(no velox equiv)* | `Null` | PartiQL NULL |
| *(no velox equiv)* | `Missing` | PartiQL MISSING |

**Domain-specific types (HttpRequest, Host):** The existing `url_*` functions (`url_host`, `url_port`, `url_path`, `url_fragment`, `url_query`, `url_path_segments`, `url_path_bucket`) pattern-match on `Value::HttpRequest`. These are logq-specific functions tied to the log-parsing domain (ELB/ALB logs produce `HttpRequest` values during record deserialization). They are registered in `url.rs` with `NullHandling::Custom` since they need to handle the `HttpRequest` variant directly. The new velox-ported `url_extract_*` functions operate on `Value::String` (parsing URLs from string values) and are independent.

Similarly, `host_name` and `host_port` pattern-match on `Value::Host` (produced by log deserialization) and are registered in `host.rs`.

### Skipped Functions (no logq type equivalent)

- Functions on `Decimal`, `IPAddress`, `UUID`, `HyperLogLog`, `Varbinary`, `Color`, `Geometry`
- Functions requiring velox's lambda/closure support (`transform`, `filter`, `reduce`, `zip_with`)
- Functions requiring custom aggregation state types

### Example Port

velox `abs` (`Arithmetic.h`):
```cpp
template <typename TExec>
struct AbsFunction {
  template <typename T>
  FOLLY_ALWAYS_INLINE void call(T& result, const T& a) {
    result = abs(a);
  }
};
```

logq equivalent:
```rust
registry.register(FunctionDef {
    name: "abs".to_string(),
    arity: Arity::Exact(1),
    null_handling: NullHandling::Propagate,
    func: Box::new(|args| match &args[0] {
        Value::Int(v) => Ok(Value::Int(v.abs())),
        Value::Float(v) => Ok(Value::Float(OrderedFloat(v.abs()))),
        other => Err(ExpressionError::InvalidArguments),
    }),
})?;
```

### Binary Operator Registration Example

```rust
// In arithmetic.rs
registry.register(FunctionDef {
    name: "Plus".to_string(),
    arity: Arity::Exact(2),
    null_handling: NullHandling::Propagate,  // Missing > Null precedence handled by registry
    func: Box::new(|args| {
        // By the time this closure runs, Propagate has already handled
        // Missing/Null -- args are guaranteed non-null, non-missing.
        match (&args[0], &args[1]) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
            (Value::Float(a), Value::Float(b)) =>
                Ok(Value::Float(OrderedFloat(a.into_inner() + b.into_inner()))),
            (Value::Int(a), Value::Float(b)) =>
                Ok(Value::Float(OrderedFloat(*a as f32 + b.into_inner()))),
            (Value::Float(a), Value::Int(b)) =>
                Ok(Value::Float(OrderedFloat(a.into_inner() + *b as f32))),
            _ => Err(ExpressionError::InvalidArguments),
        }
    }),
})?;
```

The logical planner already translates binary operators `+`, `-`, `*`, `/`, `||` into `Expression::Function("Plus", ...)`, `Expression::Function("Minus", ...)`, etc. (see `parse_binary_operator()` in `logical/parser.rs` line 207). This means binary operators flow through `registry.call()` exactly like named functions, with no special-case code path needed.

### Function Overloading Pattern

For functions like `reverse` that operate on both strings and arrays, a single registration handles both via runtime type dispatch:

```rust
registry.register(FunctionDef {
    name: "reverse".to_string(),
    arity: Arity::Exact(1),
    null_handling: NullHandling::Propagate,
    func: Box::new(|args| match &args[0] {
        Value::String(s) => Ok(Value::String(s.chars().rev().collect())),
        Value::Array(arr) => {
            let mut reversed = arr.clone();
            reversed.reverse();
            Ok(Value::Array(reversed))
        }
        _ => Err(ExpressionError::InvalidArguments),
    }),
})?;
```

This is the standard pattern for all "overloaded" functions. No special overloading mechanism is needed because logq uses dynamic typing -- the closure inspects the `Value` variant at runtime.

### Regex Caching for `regexp_*` Functions

Functions like `regexp_extract`, `regexp_like`, etc. are called once per row with typically the same pattern. Compiling a regex per invocation is expensive. The registry uses a `thread_local!` LRU cache:

```rust
// src/functions/regexp.rs

use std::cell::RefCell;
use lru::LruCache;
use regex::Regex;

thread_local! {
    static REGEX_CACHE: RefCell<LruCache<String, Regex>> =
        RefCell::new(LruCache::new(std::num::NonZeroUsize::new(64).unwrap()));
}

fn get_or_compile_regex(pattern: &str) -> ExpressionResult<Regex> {
    REGEX_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(re) = cache.get(pattern) {
            return Ok(re.clone());
        }
        let re = Regex::new(pattern)
            .map_err(|_| ExpressionError::InvalidArguments)?;
        cache.put(pattern.to_string(), re.clone());
        Ok(re)
    })
}

// Usage in a registered function:
registry.register(FunctionDef {
    name: "regexp_like".to_string(),
    arity: Arity::Exact(2),
    null_handling: NullHandling::Propagate,
    func: Box::new(|args| match (&args[0], &args[1]) {
        (Value::String(s), Value::String(pattern)) => {
            let re = get_or_compile_regex(pattern)?;
            Ok(Value::Boolean(re.is_match(s)))
        }
        _ => Err(ExpressionError::InvalidArguments),
    }),
})?;
```

This adds `lru` as a new dependency. The `thread_local!` approach avoids synchronization overhead and works correctly with the single-threaded execution model.

## 5. Migration of Existing Functions

These currently-hardcoded functions move into the registry:

| Current location | Target module | Notes |
|---|---|---|
| `Plus`, `Minus`, `Times`, `Divide` | `arithmetic.rs` | Binary operators, `NullHandling::Propagate` |
| `Concat` | `string.rs` | Binary operator `\|\|`, `NullHandling::Propagate` (**behavioral fix**: existing code checks Null before Missing, giving Null precedence; the registry's Propagate mode correctly gives Missing precedence, matching PartiQL semantics and the other arithmetic operators) |
| `upper`, `lower`, `char_length`, `character_length`, `substring`, `trim` | `string.rs` | `NullHandling::Propagate` |
| `url_host`, `url_port`, `url_path`, `url_fragment`, `url_query`, `url_path_segments`, `url_path_bucket` | `url.rs` | `NullHandling::Custom` (pattern-match on `HttpRequest`; must add explicit `Null` → `Null`, `Missing` → `Missing` handling in closures since current code returns `InvalidArguments` for non-HttpRequest types) |
| `host_name`, `host_port` | `host.rs` | `NullHandling::Custom` (pattern-match on `Host`; same Null/Missing handling addition needed) |
| `time_bucket` | `datetime.rs` | `NullHandling::Propagate` |
| `date_part` | `datetime.rs` | `NullHandling::Propagate` |

Functions **not** migrated:
- `coalesce` and `nullif`: These are desugared into `CASE WHEN` expressions by `syntax/desugar.rs` before reaching the logical planner. They never appear as function calls at execution time. This existing mechanism works correctly and is simpler than registering them as `NullHandling::Custom` functions. The desugaring is kept as-is.

## 6. Public API Surface

### Creating `src/lib.rs`

The project is currently a binary-only crate (only `src/main.rs` exists). To support library consumers, a `src/lib.rs` is created alongside `src/main.rs`. Cargo automatically recognizes this structure: `lib.rs` defines the library target, and `main.rs` defines the binary target that depends on it.

```rust
// src/lib.rs
pub mod functions;    // FunctionRegistry, FunctionDef, Arity, NullHandling, register_all
pub mod common;       // Value (needs pub(crate) -> pub visibility change)
pub mod app;          // run_with_registry()
// internal modules remain private:
// mod execution;
// mod logical;
// mod syntax;
```

The `main.rs` changes from:
```rust
mod app;
mod common;
mod execution;
mod logical;
mod syntax;
```
to:
```rust
use logq::app;
use logq::common;
// execution, logical, syntax accessed through the library
```

**Visibility changes required:**
- `Value` enum: `pub(crate)` -> `pub` in `common/types.rs`
- `FunctionRegistry`, `FunctionDef`, `Arity`, `NullHandling`: `pub` in `functions/registry.rs`
- `register_all()`: `pub` in `functions/mod.rs`
- `ExpressionError`: `pub(crate)` -> `pub` in `execution/types.rs` (needed for custom function closures)
- `run_with_registry()`: `pub` in `app.rs`

### Usage by Library Consumers

```rust
use logq::functions::{FunctionRegistry, FunctionDef, Arity, NullHandling, register_all};
use logq::common::types::Value;
use std::sync::Arc;

fn main() {
    let mut registry = register_all().expect("built-in registration failed");

    registry.register(FunctionDef {
        name: "mask_email".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => {
                let masked = s.split('@')
                    .next()
                    .map(|u| format!("{}@***", &u[..1.min(u.len())]))
                    .unwrap_or_else(|| s.clone());
                Ok(Value::String(masked))
            }
            other => Err(ExpressionError::InvalidArguments),
        }),
    }).expect("registration failed");

    let registry = Arc::new(registry);
    logq::app::run_with_registry(query, registry, data_source, output_mode);
}
```

## 7. Aggregate Functions

Aggregate functions (`count`, `sum`, `avg`, `min`, `max`, `first`, `last`, `percentile_disc`, `approx_percentile`, `approx_count_distinct`, `group_as`) remain in their current stateful-accumulator implementation in `execution/types.rs`. They are architecturally different from scalar functions (they maintain state across rows) and don't fit the `Fn(&[Value]) -> ExpressionResult<Value>` interface.

The logical planner distinguishes aggregates from scalar functions via the `from_str()` function (line 374 of `logical/parser.rs`), which maps aggregate names to `Aggregate` enum variants. Scalar function validation in the planner only runs when `from_str()` returns `Err(NotAggregateFunction)`, so there is no conflict between the two dispatch paths.

A future `AggregateRegistry` could be added following the same pattern, but that is out of scope for this design.

## 8. Phased Implementation Plan

Given the large surface area (100+ functions across 11 modules), implementation is phased:

### Phase 1: Registry infrastructure + migration of existing functions
- Implement `FunctionRegistry`, `FunctionDef`, `Arity`, `NullHandling`
- Thread `Arc<FunctionRegistry>` through the execution pipeline
- Migrate all existing hardcoded functions (binary operators, string functions, url functions, host functions, datetime functions) into the registry
- Delete the `evaluate()` free function and helpers
- Add planner-time validation
- All existing tests must pass

### Phase 2: High-value new functions
- `regexp_like`, `regexp_extract`, `regexp_replace` (with regex caching)
- `replace`, `split`, `split_part`, `starts_with`, `ends_with`, `position`
- `abs`, `ceil`, `floor`, `round`, `mod`
- `json_extract`, `json_extract_scalar`

### Phase 3: Comprehensive velox port
- Remaining arithmetic, string, bitwise, datetime, array, map, json, type_conversion functions
- `src/lib.rs` creation and public API stabilization

Each phase is independently shippable and all tests pass at each phase boundary.
