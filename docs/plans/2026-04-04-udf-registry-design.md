# User-Defined Functions for logq

## Overview

Add a plugin-style function registry to logq that replaces the current hardcoded function dispatch in `execution/types.rs`. The registry provides a public Rust API for registering scalar functions, ships with a comprehensive set of built-in functions ported from velox's prestosql implementation, and validates function calls at planning time.

## Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Extensibility model | Rust API (library crate) | Users build custom binaries; clean, no FFI/ABI concerns |
| Dispatch strategy | Name-only, dynamic typing | Functions receive `&[Value]`, handle types internally; matches logq's runtime model |
| Null handling | Convention-based with opt-out | Auto null propagation by default; `NullHandling::Custom` for functions like coalesce |
| Pipeline integration | Validate at planning, dispatch at execution | Early error messages without over-engineering the planner |
| JSON type | Treat as String (Presto semantics) | No type system changes; parse on each call; matches Presto/Trino behavior |

## 1. Registry Core

### Types

```rust
// src/functions/registry.rs

pub enum NullHandling {
    /// Any Null/Missing arg -> return Null (default)
    Propagate,
    /// Function handles Null/Missing itself (e.g., coalesce, nullif)
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
    pub func: Box<dyn Fn(&[Value]) -> Result<Value> + Send + Sync>,
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
    pub fn register(&mut self, def: FunctionDef) -> Result<()>;

    /// Validate function exists and arity matches. Called during logical planning.
    pub fn validate(&self, name: &str, arg_count: usize) -> Result<()>;

    /// Look up and invoke the function. Applies null propagation for Propagate functions.
    pub fn call(&self, name: &str, args: &[Value]) -> Result<Value>;
}
```

The `call()` method for `NullHandling::Propagate` functions checks if any argument is `Null` or `Missing` before invoking the function body. If so, it short-circuits and returns `Value::Null`. For `NullHandling::Custom` functions, args are passed through directly.

Function names are lowercased on registration and lookup for case-insensitive matching.

## 2. Module Structure

```
src/functions/
  mod.rs              -- re-exports, register_all()
  registry.rs         -- FunctionRegistry, FunctionDef, Arity, NullHandling
  arithmetic.rs       -- abs, ceil, floor, round, power, mod/modulus, sqrt, sign, truncate,
                          ln, log2, log10, exp, rand, pi, e, infinity, nan, is_nan, is_finite,
                          is_infinite, radians, degrees, sin, cos, tan, asin, acos, atan, atan2,
                          cosh, tanh
  string.rs           -- replace, split, split_part, starts_with, ends_with, reverse,
                          concat (multi-arg), lpad, rpad, position/strpos, repeat, ltrim, rtrim,
                          chr, codepoint, word_stem, normalize, to_utf8, from_utf8, soundex,
                          levenshtein_distance, hamming_distance
  bitwise.rs          -- bitwise_and, bitwise_or, bitwise_xor, bitwise_not,
                          bit_count, bitwise_shift_left, bitwise_shift_right,
                          bitwise_left_shift, bitwise_right_shift,
                          bitwise_arithmetic_shift_right, bitwise_logical_shift_right
  regexp.rs           -- regexp_extract, regexp_extract_all, regexp_like, regexp_replace,
                          regexp_split, regexp_count
  datetime.rs         -- date_trunc, date_add, date_diff, year, month, day, hour, minute,
                          second, millisecond, quarter, week, day_of_week, day_of_year,
                          now, from_unixtime, to_unixtime, date_format, date_parse
  array.rs            -- array_sort, array_distinct, array_contains, array_join,
                          array_min, array_max, array_position, cardinality, flatten,
                          array_intersect, array_union, array_except, array_remove,
                          reverse (array overload), slice, sequence, repeat (array),
                          zip, array_average, array_sum, array_normalize,
                          array_cum_sum, array_frequency, array_duplicates, array_has_duplicates,
                          combinations, ngrams, shuffle
  map.rs              -- map_keys, map_values, map_entries, element_at, map_concat,
                          map_filter, map_zip_with, transform_keys, transform_values,
                          map_from_entries, multimap_from_entries
  json.rs             -- json_extract, json_extract_scalar, json_array_length,
                          json_array_contains, json_size, json_format, json_parse,
                          is_json_scalar
  url.rs              -- url_extract_host, url_extract_port, url_extract_path,
                          url_extract_fragment, url_extract_query, url_extract_parameter,
                          url_extract_protocol, url_encode, url_decode
                          (migrate existing url_host, url_port, etc. as aliases)
  type_conversion.rs  -- typeof, try_cast, to_base, from_base, format_number
```

Each file exports:
```rust
pub fn register(registry: &mut FunctionRegistry) -> Result<()>;
```

Top-level orchestration:
```rust
// src/functions/mod.rs
pub fn register_all() -> FunctionRegistry {
    let mut registry = FunctionRegistry::new();
    arithmetic::register(&mut registry).unwrap();
    string::register(&mut registry).unwrap();
    bitwise::register(&mut registry).unwrap();
    regexp::register(&mut registry).unwrap();
    datetime::register(&mut registry).unwrap();
    array::register(&mut registry).unwrap();
    map::register(&mut registry).unwrap();
    json::register(&mut registry).unwrap();
    url::register(&mut registry).unwrap();
    type_conversion::register(&mut registry).unwrap();
    registry
}
```

## 3. Pipeline Integration

### Startup (src/app.rs)

```rust
pub fn run(query_str: &str, ...) -> Result<()> {
    let registry = functions::register_all();
    let ast = syntax::parser::query(query_str)?;
    let ast = syntax::desugar::desugar_query(ast);
    let plan = logical::parser::parse_query_top(&ast, &registry)?;
    // physical plan and execution receive &registry
    // ...
}

/// For library consumers with custom functions
pub fn run_with_registry(query_str: &str, registry: &FunctionRegistry, ...) -> Result<()> {
    // same pipeline, uses provided registry
}
```

### Logical Planner Validation (src/logical/parser.rs)

When translating `Expression::FuncCall { name, args, .. }` to a logical plan node, call:
```rust
registry.validate(&name, args.len())?;
```

This produces clear errors:
- `"Unknown function: foo"`
- `"Function abs expects 1 argument, got 3"`

No other changes to the planner.

### Execution Dispatch (src/execution/types.rs)

Replace the current hardcoded match in `evaluate()`:

```rust
// Before (current):
match name.to_lowercase().as_str() {
    "upper" => { ... }
    "lower" => { ... }
    "char_length" => { ... }
    // ... many arms
}

// After:
let evaluated_args: Vec<Value> = args.iter()
    .map(|a| a.evaluate(record, registry))
    .collect::<Result<_>>()?;
registry.call(&name, &evaluated_args)
```

The `evaluate()` method signature changes to accept `&FunctionRegistry`.

## 4. Function Porting Approach

### Type Mapping

| Velox type | logq `Value` variant |
|---|---|
| int8/16/32/64 | `Int(i32)` |
| float/double | `Float(f32)` |
| varchar | `String` |
| boolean | `Boolean` |
| timestamp | `DateTime` |
| array(T) | `Array(Vec<Value>)` |
| map(K,V) / row | `Object(LinkedHashMap<String, Value>)` |
| json | `String` (parsed on demand) |

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
        other => Err(anyhow!("abs: expected numeric, got {:?}", other)),
    }),
})?;
```

## 5. Migration of Existing Functions

These currently-hardcoded functions move into the registry:

| Current location | Target module |
|---|---|
| upper, lower, char_length, character_length, substring, trim | `string.rs` |
| url_host, url_port, url_path, url_fragment, url_query, url_path_segments, url_path_bucket | `url.rs` |
| host_name, host_port | `url.rs` (or kept as-is if logq-specific) |
| time_bucket | `datetime.rs` |
| date_part | `datetime.rs` |

The `concat` operator (`||`) remains as a binary operator in the expression evaluator since it's an operator, not a function call. A multi-arg `concat()` function is separately registered in `string.rs`.

## 6. Public API Surface

The following types are made public for library consumers:

```rust
// src/lib.rs
pub mod functions;    // FunctionRegistry, FunctionDef, Arity, NullHandling, register_all
pub mod common;       // Value (already public)
pub mod app;          // run(), run_with_registry()
```

### Usage by Library Consumers

```rust
use logq::functions::{FunctionRegistry, FunctionDef, Arity, NullHandling, register_all};
use logq::common::types::Value;

fn main() {
    let mut registry = register_all();

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
            other => Err(anyhow!("mask_email: expected string, got {:?}", other)),
        }),
    }).unwrap();

    logq::app::run_with_registry(query, &registry, ...);
}
```

## 7. Aggregate Functions

Aggregate functions (count, sum, avg, min, max, first, last, percentile_disc, approx_percentile, approx_count_distinct, group_as) remain in their current stateful-accumulator implementation in `execution/types.rs`. They are architecturally different from scalar functions (they maintain state across rows) and don't fit the `Fn(&[Value]) -> Result<Value>` interface.

A future `AggregateRegistry` could be added following the same pattern, but that is out of scope for this design.
