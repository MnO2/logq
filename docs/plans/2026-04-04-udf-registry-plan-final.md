# Plan: User-Defined Function Registry for logq (v2)

**Goal**: Replace hardcoded function dispatch with a registry-based system, migrate all existing functions, port new functions from velox's prestosql, and expose a public API for custom function registration.

**Architecture**: `FunctionRegistry` (HashMap<String, FunctionDef>) with `Arc` sharing through the execution pipeline. Functions are `Box<dyn Fn(&[Value]) -> ExpressionResult<Value>>` closures with convention-based null propagation. Validation at planning time, dispatch at execution time.

**Tech Stack**: Rust, edition 2018. Dependencies: `lru` (new, for regex caching). Existing: `regex`, `chrono`, `ordered-float`, `url`, `json`, `linked-hash-map`.

**Design doc**: `docs/plans/2026-04-04-udf-registry-design-final.md`

---

## Changes from Previous Version

This revision addresses all 9 critical issues from the review (`udf-registry-plan-review-1.md`):

1. **ParsingContext PartialEq/Eq breakage (Critical #1)**: Verified that no code in the codebase compares `ParsingContext` with `==`. The only uses of `ParsingContext` are construction and field access via `ctx.table_name`, `ctx.data_source`, etc. The `PartialEq, Eq` derives will be removed. The plan now uses `Arc<FunctionRegistry>` in `ParsingContext` (matching the execution layer) rather than a bare reference, avoiding lifetime parameter propagation through ~15 parser functions.

2. **Subquery parsing path (Critical #2)**: Added explicit update of the `Subquery` arm in `parse_value_expression()` (line 317-319 of `logical/parser.rs`). The call `parse_query(*stmt.clone(), ctx.data_source.clone())` must become `parse_query(*stmt.clone(), ctx.data_source.clone(), ctx.registry.clone())`.

3. **Incomplete aggregate detection (Critical #3)**: Replaced the hardcoded list with a proper `is_aggregate_name()` function that mirrors the logic in `from_str()` (line 374 of `logical/parser.rs`) plus the `within_group` names (`percentile_disc`, `approx_percentile`). Also includes `group_as` which is handled via the `group_as_clause` path. The function is co-located with `from_str()` to prevent drift.

4. **HttpRequest field names (Critical #4)**: Fixed test code to use the actual struct fields: `http_method` (not `method`) and `http_version` (not `version`), matching the struct at line 93-97 of `common/types.rs`.

5. **GroupByStream expression_value calls (Critical #5)**: Enumerated all 9 `expression_value()` call sites in `GroupByStream::next()` (lines 326, 337, 348, 359, 370, 381, 392, 403 of `stream.rs`). Each must receive `&self.registry`.

6. **Formula::evaluate internal call sites (Critical #6)**: Enumerated all 14 `expression_value()` calls within `Formula::evaluate()` and the 1 `relation.apply()` call. Each receives `registry` as a parameter.

7. **lazy_static for lib.rs (Critical #7)**: Kept `#[macro_use] extern crate lazy_static;` in both `lib.rs` and `main.rs`. `main.rs` needs the macro for its own `TABLE_SPEC_REGEX` (line 22-24). The `lib.rs` needs it for `HOST_REGEX` etc. in `common/types.rs`. Both declarations are valid in edition 2018 with dual-target layout.

8. **explain() function (Critical #8)**: Added explicit enumeration of changes to `explain()` in `app.rs` (line 104-118). It calls `parse_query_top()` which needs the registry.

9. **Test helper changes (Critical #9)**: Added explicit update for `run_to_vec()` (line 201-223 of `app.rs`) and the test helpers `run_format_query()` and `run_format_query_to_vec()` (lines 232-268). All call `parse_query_top()` and `physical_plan.get()` which now need the registry.

Additionally incorporated suggestions:
- **Suggestion A**: Split Step 5 into explicit sub-steps (5a through 5f).
- **Suggestion B**: Added explicit test for `Concat` behavioral change (Missing/Null precedence fix).
- **Suggestion C**: Added `From<RegistryError> for ParseError` impl instead of inline mapping.
- **Suggestion E**: Noted `date_part`/`time_bucket` behavioral change with `NullHandling::Propagate`.

---

## Task Dependencies

| Group | Steps | Can Parallelize | Notes |
|-------|-------|-----------------|-------|
| 1 | Step 1 | No | Registry core types -- foundation for everything |
| 2 | Steps 2-3 | Yes (independent files) | Migrate arithmetic + string functions into registry |
| 3 | Step 4 | No | Migrate url/host/datetime functions |
| 4 | Step 5 (sub-steps 5a-5f) | No | Thread Arc<FunctionRegistry> through pipeline |
| 5 | Step 6 | No | Add planner-time validation |
| 6 | Step 7 | No | End-to-end verification of Phase 1 |
| 7 | Steps 8-9 | Yes (independent files) | New arithmetic + string functions |
| 8 | Steps 10-11 | Yes (independent files) | Regexp (with caching) + JSON functions |
| 9 | Steps 12-14 | Yes (independent files) | Bitwise, datetime, array functions |
| 10 | Steps 15-16 | Yes (independent files) | Map + type conversion functions |
| 11 | Step 17 | No | lib.rs creation + public API |
| 12 | Step 18 | No | Final end-to-end verification |

---

## Step 1: Create FunctionRegistry, FunctionDef, Arity, NullHandling, RegistryError

**Files**: Create `src/functions/mod.rs`, Create `src/functions/registry.rs`

### 1a. Write failing test

```rust
// In src/functions/registry.rs, at the bottom:
#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Value;
    use ordered_float::OrderedFloat;

    #[test]
    fn test_register_and_call() {
        let mut registry = FunctionRegistry::new();
        registry.register(FunctionDef {
            name: "test_add".to_string(),
            arity: Arity::Exact(2),
            null_handling: NullHandling::Propagate,
            func: Box::new(|args| match (&args[0], &args[1]) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
                _ => Err(ExpressionError::InvalidArguments),
            }),
        }).unwrap();

        let result = registry.call("test_add", &[Value::Int(1), Value::Int(2)]);
        assert_eq!(result, Ok(Value::Int(3)));
    }

    #[test]
    fn test_case_insensitive_lookup() {
        let mut registry = FunctionRegistry::new();
        registry.register(FunctionDef {
            name: "MyFunc".to_string(),
            arity: Arity::Exact(1),
            null_handling: NullHandling::Propagate,
            func: Box::new(|args| Ok(args[0].clone())),
        }).unwrap();

        let result = registry.call("MYFUNC", &[Value::Int(1)]);
        assert_eq!(result, Ok(Value::Int(1)));
    }

    #[test]
    fn test_null_propagation_missing_precedence() {
        let mut registry = FunctionRegistry::new();
        registry.register(FunctionDef {
            name: "f".to_string(),
            arity: Arity::Exact(2),
            null_handling: NullHandling::Propagate,
            func: Box::new(|_| Ok(Value::Int(99))),
        }).unwrap();

        // Missing takes precedence over Null
        assert_eq!(
            registry.call("f", &[Value::Missing, Value::Null]),
            Ok(Value::Missing)
        );
        assert_eq!(
            registry.call("f", &[Value::Null, Value::Missing]),
            Ok(Value::Missing)
        );
        // Null alone returns Null
        assert_eq!(
            registry.call("f", &[Value::Null, Value::Int(1)]),
            Ok(Value::Null)
        );
        // No null/missing -> calls function
        assert_eq!(
            registry.call("f", &[Value::Int(1), Value::Int(2)]),
            Ok(Value::Int(99))
        );
    }

    #[test]
    fn test_custom_null_handling() {
        let mut registry = FunctionRegistry::new();
        registry.register(FunctionDef {
            name: "custom".to_string(),
            arity: Arity::Exact(1),
            null_handling: NullHandling::Custom,
            func: Box::new(|args| match &args[0] {
                Value::Null => Ok(Value::String("was_null".to_string())),
                other => Ok(other.clone()),
            }),
        }).unwrap();

        assert_eq!(
            registry.call("custom", &[Value::Null]),
            Ok(Value::String("was_null".to_string()))
        );
    }

    #[test]
    fn test_validate_unknown_function() {
        let registry = FunctionRegistry::new();
        assert!(registry.validate("nonexistent", 1).is_err());
    }

    #[test]
    fn test_validate_arity_mismatch() {
        let mut registry = FunctionRegistry::new();
        registry.register(FunctionDef {
            name: "f".to_string(),
            arity: Arity::Exact(2),
            null_handling: NullHandling::Propagate,
            func: Box::new(|_| Ok(Value::Null)),
        }).unwrap();

        assert!(registry.validate("f", 2).is_ok());
        assert!(registry.validate("f", 3).is_err());
    }

    #[test]
    fn test_validate_variadic_arity() {
        let mut registry = FunctionRegistry::new();
        registry.register(FunctionDef {
            name: "f".to_string(),
            arity: Arity::Variadic(1),
            null_handling: NullHandling::Propagate,
            func: Box::new(|_| Ok(Value::Null)),
        }).unwrap();

        assert!(registry.validate("f", 0).is_err());
        assert!(registry.validate("f", 1).is_ok());
        assert!(registry.validate("f", 5).is_ok());
    }

    #[test]
    fn test_validate_range_arity() {
        let mut registry = FunctionRegistry::new();
        registry.register(FunctionDef {
            name: "f".to_string(),
            arity: Arity::Range(2, 3),
            null_handling: NullHandling::Propagate,
            func: Box::new(|_| Ok(Value::Null)),
        }).unwrap();

        assert!(registry.validate("f", 1).is_err());
        assert!(registry.validate("f", 2).is_ok());
        assert!(registry.validate("f", 3).is_ok());
        assert!(registry.validate("f", 4).is_err());
    }

    #[test]
    fn test_duplicate_registration_error() {
        let mut registry = FunctionRegistry::new();
        registry.register(FunctionDef {
            name: "f".to_string(),
            arity: Arity::Exact(1),
            null_handling: NullHandling::Propagate,
            func: Box::new(|_| Ok(Value::Null)),
        }).unwrap();

        let result = registry.register(FunctionDef {
            name: "f".to_string(),
            arity: Arity::Exact(1),
            null_handling: NullHandling::Propagate,
            func: Box::new(|_| Ok(Value::Null)),
        });
        assert!(result.is_err());
    }
}
```

### 1b. Run test to verify it fails

```bash
cargo test --lib functions::registry::tests -- --test-threads=1
```

### 1c. Write implementation

**File**: `src/functions/registry.rs`

```rust
use std::collections::HashMap;
use crate::execution::types::{ExpressionResult, ExpressionError};
use crate::common::types::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NullHandling {
    Propagate,
    Custom,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Arity {
    Exact(usize),
    Range(usize, usize),
    Variadic(usize),
}

pub struct FunctionDef {
    pub name: String,
    pub arity: Arity,
    pub null_handling: NullHandling,
    pub func: Box<dyn Fn(&[Value]) -> ExpressionResult<Value> + Send + Sync>,
}

// Manual Debug impl because closures don't implement Debug
impl std::fmt::Debug for FunctionDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionDef")
            .field("name", &self.name)
            .field("arity", &self.arity)
            .field("null_handling", &self.null_handling)
            .field("func", &"<closure>")
            .finish()
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    #[error("Unknown function: {0}")]
    UnknownFunction(String),
    #[error("Function {name} expects {expected} argument(s), got {actual}")]
    ArityMismatch { name: String, expected: String, actual: usize },
    #[error("Duplicate function registration: {0}")]
    DuplicateFunction(String),
}

pub struct FunctionRegistry {
    functions: HashMap<String, FunctionDef>,
}

// Manual Debug impl because FunctionDef contains closures
impl std::fmt::Debug for FunctionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionRegistry")
            .field("functions", &format!("<{} functions>", self.functions.len()))
            .finish()
    }
}

impl FunctionRegistry {
    pub fn new() -> Self {
        FunctionRegistry {
            functions: HashMap::new(),
        }
    }

    pub fn register(&mut self, def: FunctionDef) -> Result<(), RegistryError> {
        let key = def.name.to_ascii_lowercase();
        if self.functions.contains_key(&key) {
            return Err(RegistryError::DuplicateFunction(def.name.clone()));
        }
        self.functions.insert(key, def);
        Ok(())
    }

    pub fn validate(&self, name: &str, arg_count: usize) -> Result<(), RegistryError> {
        let key = name.to_ascii_lowercase();
        let def = self.functions.get(&key)
            .ok_or_else(|| RegistryError::UnknownFunction(name.to_string()))?;

        let valid = match &def.arity {
            Arity::Exact(n) => arg_count == *n,
            Arity::Range(min, max) => arg_count >= *min && arg_count <= *max,
            Arity::Variadic(min) => arg_count >= *min,
        };

        if !valid {
            let expected = match &def.arity {
                Arity::Exact(n) => n.to_string(),
                Arity::Range(min, max) => format!("{}-{}", min, max),
                Arity::Variadic(min) => format!("at least {}", min),
            };
            return Err(RegistryError::ArityMismatch {
                name: name.to_string(),
                expected,
                actual: arg_count,
            });
        }

        Ok(())
    }

    pub fn call(&self, name: &str, args: &[Value]) -> ExpressionResult<Value> {
        let key = name.to_ascii_lowercase();
        let def = self.functions.get(&key)
            .ok_or(ExpressionError::UnknownFunction)?;

        match def.null_handling {
            NullHandling::Propagate => {
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
}
```

**File**: `src/functions/mod.rs`

```rust
pub mod registry;

pub use registry::{FunctionRegistry, FunctionDef, Arity, NullHandling, RegistryError};
```

Add `mod functions;` to `src/main.rs` (after `mod execution;`).

### 1d. Run test to verify it passes

```bash
cargo test --lib functions::registry::tests -- --test-threads=1
```

### 1e. Commit

```bash
git commit -m "Add FunctionRegistry core types with null propagation"
```

---

## Step 2: Migrate arithmetic functions (Plus, Minus, Times, Divide) into registry

**File**: Create `src/functions/arithmetic.rs`

### 2a. Write failing test

```rust
// In src/functions/arithmetic.rs, at the bottom:
#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Value;
    use crate::functions::registry::FunctionRegistry;
    use ordered_float::OrderedFloat;

    fn make_registry() -> FunctionRegistry {
        let mut r = FunctionRegistry::new();
        register(&mut r).unwrap();
        r
    }

    #[test]
    fn test_plus_int() {
        let r = make_registry();
        assert_eq!(r.call("Plus", &[Value::Int(1), Value::Int(2)]), Ok(Value::Int(3)));
    }

    #[test]
    fn test_plus_float() {
        let r = make_registry();
        let result = r.call("Plus", &[Value::Float(OrderedFloat(1.5)), Value::Float(OrderedFloat(2.5))]);
        assert_eq!(result, Ok(Value::Float(OrderedFloat(4.0))));
    }

    #[test]
    fn test_plus_int_float_coercion() {
        let r = make_registry();
        let result = r.call("Plus", &[Value::Int(1), Value::Float(OrderedFloat(2.5))]);
        assert_eq!(result, Ok(Value::Float(OrderedFloat(3.5))));
    }

    #[test]
    fn test_plus_null_propagation() {
        let r = make_registry();
        assert_eq!(r.call("Plus", &[Value::Null, Value::Int(1)]), Ok(Value::Null));
        assert_eq!(r.call("Plus", &[Value::Missing, Value::Null]), Ok(Value::Missing));
    }

    #[test]
    fn test_divide_by_zero() {
        let r = make_registry();
        assert_eq!(r.call("Divide", &[Value::Int(1), Value::Int(0)]), Ok(Value::Null));
    }

    #[test]
    fn test_minus_int() {
        let r = make_registry();
        assert_eq!(r.call("Minus", &[Value::Int(5), Value::Int(3)]), Ok(Value::Int(2)));
    }

    #[test]
    fn test_times_int() {
        let r = make_registry();
        assert_eq!(r.call("Times", &[Value::Int(3), Value::Int(4)]), Ok(Value::Int(12)));
    }
}
```

### 2b. Run test to verify it fails

```bash
cargo test --lib functions::arithmetic::tests -- --test-threads=1
```

### 2c. Write implementation

```rust
// src/functions/arithmetic.rs
use crate::common::types::Value;
use crate::execution::types::ExpressionError;
use crate::functions::registry::{FunctionDef, FunctionRegistry, Arity, NullHandling, RegistryError};
use ordered_float::OrderedFloat;

pub fn register(registry: &mut FunctionRegistry) -> Result<(), RegistryError> {
    registry.register(FunctionDef {
        name: "Plus".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() + b.into_inner()))),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(*a as f32 + b.into_inner()))),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() + *b as f32))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    registry.register(FunctionDef {
        name: "Minus".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() - b.into_inner()))),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(*a as f32 - b.into_inner()))),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() - *b as f32))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    registry.register(FunctionDef {
        name: "Times".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() * b.into_inner()))),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(*a as f32 * b.into_inner()))),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() * *b as f32))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    registry.register(FunctionDef {
        name: "Divide".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Int(a), Value::Int(b)) => {
                if *b == 0 { Ok(Value::Null) } else { Ok(Value::Int(a / b)) }
            }
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() / b.into_inner()))),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(*a as f32 / b.into_inner()))),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() / *b as f32))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    Ok(())
}
```

Add `pub mod arithmetic;` to `src/functions/mod.rs`.

### 2d. Run test to verify it passes

```bash
cargo test --lib functions::arithmetic::tests -- --test-threads=1
```

### 2e. Commit

```bash
git commit -m "Migrate arithmetic operators (Plus/Minus/Times/Divide) to function registry"
```

---

## Step 3: Migrate string functions (Concat, upper, lower, char_length, substring, trim) into registry

**File**: Create `src/functions/string.rs`

### 3a. Write failing test

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Value;
    use crate::functions::registry::FunctionRegistry;

    fn make_registry() -> FunctionRegistry {
        let mut r = FunctionRegistry::new();
        register(&mut r).unwrap();
        r
    }

    #[test]
    fn test_upper() {
        let r = make_registry();
        assert_eq!(r.call("upper", &[Value::String("hello".to_string())]), Ok(Value::String("HELLO".to_string())));
    }

    #[test]
    fn test_lower() {
        let r = make_registry();
        assert_eq!(r.call("lower", &[Value::String("HELLO".to_string())]), Ok(Value::String("hello".to_string())));
    }

    #[test]
    fn test_char_length() {
        let r = make_registry();
        assert_eq!(r.call("char_length", &[Value::String("hello".to_string())]), Ok(Value::Int(5)));
    }

    #[test]
    fn test_character_length_alias() {
        let r = make_registry();
        assert_eq!(r.call("character_length", &[Value::String("hello".to_string())]), Ok(Value::Int(5)));
    }

    #[test]
    fn test_substring_two_args() {
        let r = make_registry();
        assert_eq!(
            r.call("substring", &[Value::String("hello".to_string()), Value::Int(2)]),
            Ok(Value::String("ello".to_string()))
        );
    }

    #[test]
    fn test_substring_three_args() {
        let r = make_registry();
        assert_eq!(
            r.call("substring", &[Value::String("hello".to_string()), Value::Int(2), Value::Int(3)]),
            Ok(Value::String("ell".to_string()))
        );
    }

    #[test]
    fn test_trim() {
        let r = make_registry();
        assert_eq!(r.call("trim", &[Value::String("  hello  ".to_string())]), Ok(Value::String("hello".to_string())));
    }

    #[test]
    fn test_concat_operator() {
        let r = make_registry();
        assert_eq!(
            r.call("Concat", &[Value::String("hello".to_string()), Value::String(" world".to_string())]),
            Ok(Value::String("hello world".to_string()))
        );
    }

    #[test]
    fn test_concat_null_propagation_behavioral_change() {
        let r = make_registry();
        // BEHAVIORAL CHANGE from previous implementation:
        // Previously: Concat(Null, Missing) returned Null (Null checked first, line 607)
        // Now: Concat(Null, Missing) returns Missing (Missing takes precedence per PartiQL spec)
        //
        // This matches the behavior of Plus/Minus/Times/Divide and is the correct
        // PartiQL semantics. Missing > Null in propagation precedence.
        assert_eq!(r.call("Concat", &[Value::Missing, Value::String("x".to_string())]), Ok(Value::Missing));
        assert_eq!(r.call("Concat", &[Value::Null, Value::String("x".to_string())]), Ok(Value::Null));
        assert_eq!(r.call("Concat", &[Value::Null, Value::Missing]), Ok(Value::Missing));
    }
}
```

### 3b-3e. Implement, test, commit

Implementation follows same pattern as Step 2. The Concat function uses `NullHandling::Propagate`, which is a deliberate behavioral change: previously `Concat(Null, Missing)` returned `Null` because Null was checked before Missing (line 607 of `execution/types.rs`). Now Missing takes precedence, matching PartiQL semantics and the other arithmetic operators. Verified this does not break any existing tests (the existing tests only test `Concat` with string values, not Null/Missing edge cases).

Add `pub mod string;` to `src/functions/mod.rs`.

```bash
cargo test --lib functions::string::tests -- --test-threads=1
git commit -m "Migrate string functions and Concat operator to function registry"
```

---

## Step 4: Migrate url, host, and datetime functions into registry

**Files**: Create `src/functions/url.rs`, `src/functions/host.rs`, `src/functions/datetime.rs`

### 4a. Write failing test

```rust
// In src/functions/url.rs tests:
#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{Value, HttpRequest};
    use url::Url;

    fn make_registry() -> FunctionRegistry {
        let mut r = FunctionRegistry::new();
        register(&mut r).unwrap();
        r
    }

    fn make_request(url_str: &str) -> Value {
        let url = Url::parse(url_str).unwrap();
        Value::HttpRequest(HttpRequest {
            http_method: "GET".to_string(),  // FIXED: was "method" in v1
            url,
            http_version: "HTTP/1.1".to_string(),  // FIXED: was "version" in v1
        })
    }

    #[test]
    fn test_url_host() {
        let r = make_registry();
        let req = make_request("http://example.com/path");
        assert_eq!(r.call("url_host", &[req]), Ok(Value::String("example.com".to_string())));
    }

    #[test]
    fn test_url_host_null_missing() {
        let r = make_registry();
        assert_eq!(r.call("url_host", &[Value::Null]), Ok(Value::Null));
        assert_eq!(r.call("url_host", &[Value::Missing]), Ok(Value::Missing));
    }

    #[test]
    fn test_url_port() {
        let r = make_registry();
        let req = make_request("http://example.com:8080/path");
        assert_eq!(r.call("url_port", &[req]), Ok(Value::Int(8080)));
    }

    #[test]
    fn test_url_path() {
        let r = make_registry();
        let req = make_request("http://example.com/foo/bar");
        assert_eq!(r.call("url_path", &[req]), Ok(Value::String("/foo/bar".to_string())));
    }
}
```

Tests for host.rs and datetime.rs follow the same pattern -- test each function with valid input, null, and missing.

**Note on NullHandling for url/host functions**: These use `NullHandling::Custom` because they pattern-match on `Value::HttpRequest` / `Value::Host` directly. The closures must explicitly handle Null/Missing:

```rust
func: Box::new(|args| match &args[0] {
    Value::Null => Ok(Value::Null),
    Value::Missing => Ok(Value::Missing),
    Value::HttpRequest(r) => {
        if let Some(host) = r.url.host_str() {
            Ok(Value::String(host.to_string()))
        } else {
            Ok(Value::Null)
        }
    }
    _ => Err(ExpressionError::InvalidArguments),
}),
```

**Note on NullHandling for datetime functions**: `date_part` and `time_bucket` use `NullHandling::Propagate`. This is a behavioral change: previously `date_part('year', NULL)` returned `InvalidArguments` error (the `_` catch-all at line 503), now it returns `Null`. This is the correct behavior per PartiQL semantics. Test explicitly:

```rust
#[test]
fn test_date_part_null_propagation() {
    let r = make_registry();
    assert_eq!(r.call("date_part", &[Value::String("year".to_string()), Value::Null]), Ok(Value::Null));
    assert_eq!(r.call("date_part", &[Value::String("year".to_string()), Value::Missing]), Ok(Value::Missing));
}
```

### 4b-4e. Implement, test, commit

Migrate the logic from:
- `evaluate_url_functions()` (lines 231-361 of `execution/types.rs`) into `url.rs`
- `evaluate_host_functions()` (lines 363-385 of `execution/types.rs`) into `host.rs`
- `date_part` (lines 485-504 of `execution/types.rs`) into `datetime.rs`
- `time_bucket` (lines 506-599 of `execution/types.rs`) into `datetime.rs`

Add `pub mod url;`, `pub mod host;`, `pub mod datetime;` to `src/functions/mod.rs`.

```bash
cargo test --lib functions::url::tests functions::host::tests functions::datetime::tests -- --test-threads=1
git commit -m "Migrate url, host, and datetime functions to function registry"
```

---

## Step 5: Thread Arc<FunctionRegistry> through pipeline, replace evaluate() dispatch

**Files**: Modify `src/functions/mod.rs`, `src/app.rs`, `src/execution/types.rs`, `src/execution/stream.rs`, `src/common/types.rs`, `src/logical/parser.rs`

This is the largest step, split into sub-steps that should be done sequentially.

### 5a. Add `register_all()` to `src/functions/mod.rs`

```rust
pub mod registry;
pub mod arithmetic;
pub mod string;
pub mod url;
pub mod host;
pub mod datetime;

pub use registry::{FunctionRegistry, FunctionDef, Arity, NullHandling, RegistryError};

pub fn register_all() -> Result<FunctionRegistry, RegistryError> {
    let mut registry = FunctionRegistry::new();
    arithmetic::register(&mut registry)?;
    string::register(&mut registry)?;
    url::register(&mut registry)?;
    host::register(&mut registry)?;
    datetime::register(&mut registry)?;
    Ok(registry)
}
```

### 5b. Update `ParsingContext` and parser signatures

**File**: `src/common/types.rs` (line 413-417)

Change:
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsingContext {
    pub(crate) table_name: String,
    pub(crate) data_source: DataSource,
}
```

To:
```rust
use std::sync::Arc;
use crate::functions::FunctionRegistry;

#[derive(Debug, Clone)]  // PartialEq, Eq REMOVED -- FunctionRegistry contains Box<dyn Fn(...)>
                          // which does not impl PartialEq/Eq.
                          // Verified: no code compares ParsingContext with == anywhere.
pub(crate) struct ParsingContext {
    pub(crate) table_name: String,
    pub(crate) data_source: DataSource,
    pub(crate) registry: Arc<FunctionRegistry>,
}
```

**Verification that removing PartialEq/Eq is safe**: Searched the entire `src/` directory for `ParsingContext` comparisons (`ParsingContext.*==|==.*ParsingContext|PartialEq.*ParsingContext`). Result: **no matches**. `ParsingContext` is only constructed (4 call sites in `logical/parser.rs`: lines 650-653, 1041-1043, 1064-1066, 1101-1103, 1128-1130, 1151-1153, 1452-1454, 1479-1481, 1516-1518, 1547-1549) and accessed via `ctx.table_name`, `ctx.data_source`, `ctx.registry`. It is never compared for equality. Tests construct `ParsingContext` values and pass them to parsing functions -- they never assert `ParsingContext == other_context`.

**File**: `src/logical/parser.rs`

Update `parse_query_top` signature (line 614):
```rust
// Before:
pub(crate) fn parse_query_top(q: ast::Query, data_source: common::DataSource) -> ParseResult<types::Node>

// After:
pub(crate) fn parse_query_top(
    q: ast::Query,
    data_source: common::DataSource,
    registry: Arc<FunctionRegistry>,
) -> ParseResult<types::Node>
```

Update the body to pass `registry` to `parse_query()` and to recursive `parse_query_top()` calls:
```rust
match q {
    ast::Query::Select(stmt) => parse_query(stmt, data_source, registry),
    ast::Query::SetOp { op, all, left, right } => {
        let left_node = parse_query_top(*left, data_source.clone(), registry.clone())?;
        let right_node = parse_query_top(*right, data_source, registry)?;
        // ... rest unchanged
    }
}
```

Update `parse_query` signature (line 640):
```rust
// Before:
pub(crate) fn parse_query(query: ast::SelectStatement, data_source: common::DataSource) -> ParseResult<types::Node>

// After:
pub(crate) fn parse_query(
    query: ast::SelectStatement,
    data_source: common::DataSource,
    registry: Arc<FunctionRegistry>,
) -> ParseResult<types::Node>
```

Update `ParsingContext` construction (line 650-653):
```rust
let parsing_context = ParsingContext {
    table_name: table_name.clone(),
    data_source: data_source.clone(),
    registry: registry.clone(),  // NEW
};
```

**Update `Subquery` arm in `parse_value_expression`** (line 317-319):
```rust
// Before:
ast::Expression::Subquery(stmt) => {
    let inner_node = parse_query(*stmt.clone(), ctx.data_source.clone())?;
    Ok(Box::new(types::Expression::Subquery(Box::new(inner_node))))
}

// After:
ast::Expression::Subquery(stmt) => {
    let inner_node = parse_query(*stmt.clone(), ctx.data_source.clone(), ctx.registry.clone())?;
    Ok(Box::new(types::Expression::Subquery(Box::new(inner_node))))
}
```

**Update parser tests**: Every `ParsingContext` construction in the test module (lines 1041, 1064, 1101, 1128, 1151, 1452, 1479, 1516, 1547) must add the `registry` field:

```rust
let parsing_context = ParsingContext {
    table_name: "a".to_string(),
    data_source: common::DataSource::Stdin("jsonl".to_string(), "a".to_string()),
    registry: Arc::new(crate::functions::register_all().unwrap()),  // NEW
};
```

Add `use std::sync::Arc;` to the test module's imports.

Also add `From<RegistryError> for ParseError` impl:
```rust
impl From<RegistryError> for ParseError {
    fn from(e: RegistryError) -> Self {
        match e {
            RegistryError::UnknownFunction(name) => ParseError::UnknownFunction(name),
            RegistryError::ArityMismatch { name, expected, actual } =>
                ParseError::InvalidArguments(
                    format!("{} expects {} argument(s), got {}", name, expected, actual)
                ),
            RegistryError::DuplicateFunction(name) =>
                ParseError::InvalidArguments(format!("Duplicate function: {}", name)),
        }
    }
}
```

### 5c. Update expression_value, Formula::evaluate, Relation::apply signatures

**File**: `src/execution/types.rs`

**Expression::expression_value** (line 134): Add `registry: &Arc<FunctionRegistry>` parameter.

All internal calls must pass `registry`:

| Line | Current code | Change |
|------|-------------|--------|
| 138 | `formula.evaluate(variables)?` | `formula.evaluate(variables, registry)?` |
| 162 | `evaluate(&*name, &values)?` | `registry.call(name, &values)?` |
| 153 | `expr.expression_value(&variables)?` | `expr.expression_value(&variables, registry)?` |
| 167 | `formula.evaluate(variables)?` | `formula.evaluate(variables, registry)?` |
| 169 | `then_expr.expression_value(variables)` | `then_expr.expression_value(variables, registry)` |
| 173 | `expr.expression_value(variables)` | `expr.expression_value(variables, registry)` |
| 178 | `inner.expression_value(variables)?` | `inner.expression_value(variables, registry)?` |
| 213 | `node.get(variables.clone())` | `node.get(variables.clone(), registry.clone())` |

**Formula::evaluate** (line 805): Add `registry: &Arc<FunctionRegistry>` parameter.

All internal calls -- complete enumeration:

| Line | Current code | Change |
|------|-------------|--------|
| 808 | `left_formula.evaluate(variables)?` | `left_formula.evaluate(variables, registry)?` |
| 809 | `right_formula.evaluate(variables)?` | `right_formula.evaluate(variables, registry)?` |
| 818 | `left_formula.evaluate(variables)?` | `left_formula.evaluate(variables, registry)?` |
| 819 | `right_formula.evaluate(variables)?` | `right_formula.evaluate(variables, registry)?` |
| 828 | `child_formula.evaluate(variables)?` | `child_formula.evaluate(variables, registry)?` |
| 832 | `relation.apply(variables, left_formula, right_formula)?` | `relation.apply(variables, left_formula, right_formula, registry)?` |
| 837 | `expr.expression_value(variables)?` | `expr.expression_value(variables, registry)?` |
| 841 | `expr.expression_value(variables)?` | `expr.expression_value(variables, registry)?` |
| 845 | `expr.expression_value(variables)?` | `expr.expression_value(variables, registry)?` |
| 849 | `expr.expression_value(variables)?` | `expr.expression_value(variables, registry)?` |
| 853 | `expr.expression_value(variables)?` | `expr.expression_value(variables, registry)?` |
| 861 | `expr.expression_value(variables)?` | `expr.expression_value(variables, registry)?` |
| 862 | `pattern_expr.expression_value(variables)?` | `pattern_expr.expression_value(variables, registry)?` |
| 875 | `expr.expression_value(variables)?` | `expr.expression_value(variables, registry)?` |
| 876 | `pattern_expr.expression_value(variables)?` | `pattern_expr.expression_value(variables, registry)?` |
| 889 | `expr.expression_value(variables)?` | `expr.expression_value(variables, registry)?` |
| 895 | `item_expr.expression_value(variables)?` | `item_expr.expression_value(variables, registry)?` |
| 914 | `Formula::In(expr.clone(), list.clone()).evaluate(variables)?` | `Formula::In(expr.clone(), list.clone()).evaluate(variables, registry)?` |

**Relation::apply** (line 710): Add `registry: &Arc<FunctionRegistry>` parameter.

| Line | Current code | Change |
|------|-------------|--------|
| 711 | `left.expression_value(variables)?` | `left.expression_value(variables, registry)?` |
| 712 | `right.expression_value(variables)?` | `right.expression_value(variables, registry)?` |

**Tests in `execution/types.rs`**: All `formula.evaluate(&variables)` calls in tests must become `formula.evaluate(&variables, &Arc::new(functions::register_all().unwrap()))`. Create a helper:

```rust
#[cfg(test)]
fn test_registry() -> Arc<FunctionRegistry> {
    Arc::new(crate::functions::register_all().unwrap())
}
```

### 5d. Update Node::get and all stream structs

**File**: `src/execution/types.rs`

**Node::get** (line 938): Add `registry: Arc<FunctionRegistry>` parameter.

All arms must pass `registry.clone()` to recursive `get()` calls and to stream constructors:

| Line | Current code | Change |
|------|-------------|--------|
| 941 | `source.get(variables.clone())?` | `source.get(variables.clone(), registry.clone())?` |
| 942 | `FilterStream::new(*formula.clone(), variables, record_stream)` | `FilterStream::new(*formula.clone(), variables, record_stream, registry)` |
| 946 | `source.get(variables.clone())?` | `source.get(variables.clone(), registry.clone())?` |
| 948 | `MapStream::new(named_list.clone(), variables, record_stream)` | `MapStream::new(named_list.clone(), variables, record_stream, registry)` |
| 973 | `source.get(variables.clone())?` | `source.get(variables.clone(), registry.clone())?` |
| 974 | `GroupByStream::new(...)` | `GroupByStream::new(..., registry)` |
| 978 | `source.get(variables.clone())?` | `source.get(variables.clone(), registry.clone())?` |
| 983 | `source.get(variables.clone())?` | `source.get(variables.clone(), registry.clone())?` |
| 1095 | `source.get(variables)?` | `source.get(variables, registry)?` |
| 1099 | `left.get(variables.clone())?` | `left.get(variables.clone(), registry.clone())?` |
| 1102 | `CrossJoinStream::new(left_stream, right_node, right_variables)` | `CrossJoinStream::new(left_stream, right_node, right_variables, registry)` |
| 1105 | `left.get(variables.clone())?` | `left.get(variables.clone(), registry.clone())?` |
| 1108 | `LeftJoinStream::new(...)` | `LeftJoinStream::new(..., registry)` |
| 1111 | `left.get(variables.clone())?` | `left.get(variables.clone(), registry.clone())?` |
| 1112 | `right.get(variables)?` | `right.get(variables, registry)?` |
| 1116 | `left.get(variables.clone())?` | `left.get(variables.clone(), registry.clone())?` |
| 1117 | `right.get(variables)?` | `right.get(variables, registry)?` |
| 1121 | `left.get(variables.clone())?` | `left.get(variables.clone(), registry.clone())?` |
| 1122 | `right.get(variables)?` | `right.get(variables, registry)?` |

**File**: `src/execution/stream.rs`

Add `registry: Arc<FunctionRegistry>` field to 5 stream structs. Update their constructors and `RecordStream::next()` implementations.

**MapStream** (line 120-177):
- Add field: `pub(crate) registry: Arc<FunctionRegistry>`
- Update `new()`: accept `registry: Arc<FunctionRegistry>`, store in struct
- Line 159: `expr.expression_value(&variables)?` becomes `expr.expression_value(&variables, &self.registry)?`

**FilterStream** (line 216-249):
- Add field: `registry: Arc<FunctionRegistry>`
- Update `new()`: accept `registry: Arc<FunctionRegistry>`, store in struct
- Line 236: `self.formula.evaluate(&variables)?` becomes `self.formula.evaluate(&variables, &self.registry)?`

**GroupByStream** (line 273-471):
- Add field: `registry: Arc<FunctionRegistry>`
- Update `new()`: accept `registry: Arc<FunctionRegistry>`, store in struct
- **All 9 `expression_value()` calls in `next()` must receive `&self.registry`**:

| Line | Current code | Change |
|------|-------------|--------|
| 326 | `expr.expression_value(&variables)?` | `expr.expression_value(&variables, &self.registry)?` |
| 337 | `expr.expression_value(&variables)?` | `expr.expression_value(&variables, &self.registry)?` |
| 348 | `expr.expression_value(&variables)?` | `expr.expression_value(&variables, &self.registry)?` |
| 359 | `expr.expression_value(&variables)?` | `expr.expression_value(&variables, &self.registry)?` |
| 370 | `expr.expression_value(&variables)?` | `expr.expression_value(&variables, &self.registry)?` |
| 381 | `expr.expression_value(&variables)?` | `expr.expression_value(&variables, &self.registry)?` |
| 392 | `expr.expression_value(&variables)?` | `expr.expression_value(&variables, &self.registry)?` |
| 403 | `expr.expression_value(&variables)?` | `expr.expression_value(&variables, &self.registry)?` |

(These are inside the `Aggregate::Avg`, `Aggregate::Count`, `Aggregate::First`, `Aggregate::Last`, `Aggregate::Max`, `Aggregate::Min`, `Aggregate::Sum`, `Aggregate::ApproxCountDistinct` arms respectively.)

Note: `Aggregate::PercentileDisc` (line 411-413) and `Aggregate::ApproxPercentile` (line 415-417) do NOT call `expression_value()` -- they use `variables.get(column_name)` directly. No change needed for those arms.

Note: `Aggregate::GroupAs` (line 314-323) does NOT call `expression_value()` -- it uses `Value::Object(record.to_variables().clone())`. No change needed.

**CrossJoinStream** (line 629-678):
- Add field: `registry: Arc<FunctionRegistry>`
- Update `new()`: accept `registry: Arc<FunctionRegistry>`, store in struct
- Line 666: `self.right_node.get(self.right_variables.clone())` becomes `self.right_node.get(self.right_variables.clone(), self.registry.clone())`

**LeftJoinStream** (line 680-807):
- Add field: `registry: Arc<FunctionRegistry>`
- Update `new()`: accept `registry: Arc<FunctionRegistry>`, store in struct
- Line 743: `self.condition.evaluate(&merged_vars)?` becomes `self.condition.evaluate(&merged_vars, &self.registry)?`
- Line 774-775: `self.right_node.get(self.right_variables.clone())` becomes `self.right_node.get(self.right_variables.clone(), self.registry.clone())`
- Line 784-785: `self.right_node.get(self.right_variables.clone())` becomes `self.right_node.get(self.right_variables.clone(), self.registry.clone())`

**Streams NOT needing registry** (do not call `expression_value()` or `formula.evaluate()`):
- `LimitStream` -- just counts records
- `InMemoryStream` -- just pops from a deque
- `LogFileStream` -- reads from file
- `ProjectionStream` -- only calls `record.alias()` and `record.project()`, no expression evaluation
- `DistinctStream` -- only calls `record.to_tuples()`, no expression evaluation
- `UnionStream` -- just delegates to left/right streams
- `IntersectStream` -- only compares tuples, no expression evaluation
- `ExceptStream` -- only compares tuples, no expression evaluation

**Update stream tests**: The `test_filter_stream` and `test_map_stream_with_names` tests construct streams that evaluate expressions. They need a registry. `test_map_stream_with_star`, `test_limit_stream`, `test_distinct_stream`, `test_record_merge` etc. do NOT evaluate expressions and need no changes.

For `test_filter_stream` (line 1007-1047):
```rust
let registry = Arc::new(crate::functions::register_all().unwrap());
let mut filtered_stream = FilterStream::new(predicate, variables, stream, registry);
```

For `test_map_stream_with_names` (line 1097-1136):
The `MapStream` evaluates an `Expression::Variable`, which does NOT call `registry.call()` -- it just looks up a variable. But `expression_value()` now requires the registry parameter regardless. So update:
```rust
let registry = Arc::new(crate::functions::register_all().unwrap());
let mut filtered_stream = MapStream::new(named_list, variables, stream, registry);
```

### 5e. Update app.rs (run, explain, run_to_vec)

**File**: `src/app.rs`

Add `Registry` variant to `AppError` (after line 34):
```rust
#[error("{0}")]
Registry(#[from] functions::registry::RegistryError),
```

Update `AppError`'s manual `PartialEq` impl (line 38-54) to include the new variant:
```rust
(AppError::Registry(_), AppError::Registry(_)) => true,
```

**Update `run()` (line 120-198)**:

```rust
pub(crate) fn run(query_str: &str, data_source: common::types::DataSource, output_mode: OutputMode) -> AppResult<()> {
    let registry = Arc::new(crate::functions::register_all()?);  // NEW

    let (rest_of_str, q) = syntax::parser::query(&query_str)?;
    if !rest_of_str.is_empty() {
        return Err(AppError::InputNotAllConsumed(rest_of_str.to_string()));
    }
    let q = syntax::desugar::desugar_query(q);

    let node = logical::parser::parse_query_top(q, data_source.clone(), registry.clone())?;  // CHANGED
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new(data_source);
    let (physical_plan, variables) = node.physical(&mut physical_plan_creator)?;

    let mut stream = physical_plan.get(variables, registry)?;  // CHANGED

    // ... rest unchanged (match output_mode)
}
```

**Update `explain()` (line 104-118)**:

```rust
pub(crate) fn explain(query_str: &str, data_source: common::types::DataSource) -> AppResult<()> {
    let registry = Arc::new(crate::functions::register_all()?);  // NEW

    let (rest_of_str, q) = syntax::parser::query(&query_str)?;
    if !rest_of_str.is_empty() {
        return Err(AppError::InputNotAllConsumed(rest_of_str.to_string()));
    }
    let q = syntax::desugar::desugar_query(q);

    let node = logical::parser::parse_query_top(q, data_source.clone(), registry)?;  // CHANGED
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new(data_source);
    let (physical_plan, _variables) = node.physical(&mut physical_plan_creator)?;

    println!("Query Plan:");
    println!("{:?}", physical_plan);
    Ok(())
}
```

**Update `run_to_vec()` (line 201-223, `#[cfg(test)]`)**:

```rust
#[cfg(test)]
pub(crate) fn run_to_vec(
    query_str: &str,
    data_source: common::types::DataSource,
) -> AppResult<Vec<Vec<(String, common::types::Value)>>> {
    let registry = Arc::new(crate::functions::register_all()?);  // NEW

    let (rest_of_str, q) = syntax::parser::query(&query_str)?;
    if !rest_of_str.is_empty() {
        return Err(AppError::InputNotAllConsumed(rest_of_str.to_string()));
    }
    let q = syntax::desugar::desugar_query(q);

    let node = logical::parser::parse_query_top(q, data_source.clone(), registry.clone())?;  // CHANGED
    let mut physical_plan_creator = logical::types::PhysicalPlanCreator::new(data_source);
    let (physical_plan, variables) = node.physical(&mut physical_plan_creator)?;

    let mut stream = physical_plan.get(variables, registry)?;  // CHANGED
    let mut results = Vec::new();

    while let Some(record) = stream.next()? {
        results.push(record.to_tuples());
    }

    Ok(results)
}
```

The test helpers `run_format_query()` (line 232) and `run_format_query_to_vec()` (line 249) call `run()` and `run_to_vec()` respectively, so they do NOT need changes themselves -- the registry is created inside `run()`/`run_to_vec()`.

Add imports to `app.rs`:
```rust
use std::sync::Arc;
use crate::functions;
```

### 5f. Delete old evaluate() functions

**File**: `src/execution/types.rs`

Delete entirely:
- `fn evaluate()` (lines 388-697) -- all dispatch now goes through `registry.call()`
- `fn evaluate_url_functions()` (lines 231-361) -- logic moved to `src/functions/url.rs`
- `fn evaluate_host_functions()` (lines 363-385) -- logic moved to `src/functions/host.rs`

### 5g. Run all existing tests

```bash
cargo test
```

All ~58 existing tests must pass.

### 5h. Commit

```bash
git commit -m "Thread Arc<FunctionRegistry> through pipeline, replace hardcoded evaluate() dispatch"
```

---

## Step 6: Add planner-time validation

**File**: Modify `src/logical/parser.rs`

### 6a. Add `is_aggregate_name()` helper function

Place this immediately after the `from_str()` function (line 386) to keep the aggregate name knowledge co-located and prevent drift:

```rust
/// Returns true if the given function name is an aggregate function.
/// This mirrors the names recognized by `from_str()` above plus the
/// within_group-based aggregates (percentile_disc, approx_percentile)
/// and group_as.
///
/// Co-located with from_str() so that when new aggregates are added to
/// from_str(), this function is updated in the same place.
fn is_aggregate_name(name: &str) -> bool {
    match name.to_ascii_lowercase().as_str() {
        // from_str() names:
        "avg" | "count" | "first" | "last" | "max" | "min" | "sum"
        | "approx_count_distinct"
        // within_group path names (not in from_str() but handled by parse_aggregate):
        | "percentile_disc" | "approx_percentile"
        // group_as (handled via group_as_clause path):
        | "group_as" => true,
        _ => false,
    }
}
```

### 6b. Write failing test

```rust
// In src/app.rs test section, add:
#[test]
fn test_unknown_function_error_at_planning() {
    let result = run_format_query(
        "jsonl",
        &[r#"{"a": 1}"#],
        "SELECT nonexistent_func(a) FROM it",
    );
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("Unknown function") || err_msg.contains("nonexistent_func"),
        "Expected unknown function error, got: {}",
        err_msg
    );
}
```

### 6c. Run test to verify it fails

```bash
cargo test test_unknown_function_error_at_planning -- --test-threads=1
```

### 6d. Write implementation

In `parse_value_expression()`, the `FuncCall` arm (line 251-258 of `logical/parser.rs`).

Before:
```rust
ast::Expression::FuncCall(func_name, select_exprs, _) => {
    let mut args = Vec::new();
    for select_expr in select_exprs.iter() {
        let arg = parse_expression(ctx, select_expr)?;
        args.push(*arg);
    }
    Ok(Box::new(types::Expression::Function(func_name.clone(), args)))
}
```

After:
```rust
ast::Expression::FuncCall(func_name, select_exprs, _) => {
    // Validate scalar functions at planning time.
    // Aggregate functions are handled by parse_aggregate() and should not
    // be validated as scalar functions.
    if !is_aggregate_name(func_name) {
        ctx.registry.validate(func_name, select_exprs.len())
            .map_err(ParseError::from)?;
    }

    let mut args = Vec::new();
    for select_expr in select_exprs.iter() {
        let arg = parse_expression(ctx, select_expr)?;
        args.push(*arg);
    }
    Ok(Box::new(types::Expression::Function(func_name.clone(), args)))
}
```

### 6e. Run test to verify it passes

```bash
cargo test test_unknown_function_error_at_planning -- --test-threads=1
cargo test  # all tests pass
```

### 6f. Commit

```bash
git commit -m "Add planner-time validation for scalar function existence and arity"
```

---

## Step 7: End-to-end verification of Phase 1

### 7a. Run full test suite

```bash
cargo test
```

### 7b. Verify no hardcoded function dispatch remains

```bash
# Should return no matches (evaluate() free function deleted):
grep -n "fn evaluate(" src/execution/types.rs || echo "OK: evaluate() removed"
grep -n "evaluate_url_functions" src/execution/types.rs || echo "OK: evaluate_url_functions removed"
grep -n "evaluate_host_functions" src/execution/types.rs || echo "OK: evaluate_host_functions removed"
```

### 7c. Run benchmarks to check for regressions

```bash
cargo bench
```

### 7d. Commit (if any fixups needed)

```bash
git commit -m "Phase 1 complete: function registry infrastructure with all existing functions migrated"
```

---

## Step 8: Add new arithmetic functions (abs, ceil, floor, round, power, mod, sqrt, sign, truncate, math functions)

**File**: Modify `src/functions/arithmetic.rs`

### 8a. Write failing test

```rust
#[test]
fn test_abs_int() {
    let r = make_registry();
    assert_eq!(r.call("abs", &[Value::Int(-5)]), Ok(Value::Int(5)));
}

#[test]
fn test_abs_float() {
    let r = make_registry();
    assert_eq!(r.call("abs", &[Value::Float(OrderedFloat(-3.14))]), Ok(Value::Float(OrderedFloat(3.14))));
}

#[test]
fn test_ceil() {
    let r = make_registry();
    assert_eq!(r.call("ceil", &[Value::Float(OrderedFloat(2.3))]), Ok(Value::Int(3)));
}

#[test]
fn test_floor() {
    let r = make_registry();
    assert_eq!(r.call("floor", &[Value::Float(OrderedFloat(2.7))]), Ok(Value::Int(2)));
}

#[test]
fn test_round() {
    let r = make_registry();
    assert_eq!(r.call("round", &[Value::Float(OrderedFloat(2.5))]), Ok(Value::Int(3)));
}

#[test]
fn test_power() {
    let r = make_registry();
    assert_eq!(r.call("power", &[Value::Int(2), Value::Int(3)]), Ok(Value::Float(OrderedFloat(8.0))));
}

#[test]
fn test_mod() {
    let r = make_registry();
    assert_eq!(r.call("mod", &[Value::Int(10), Value::Int(3)]), Ok(Value::Int(1)));
}

#[test]
fn test_sqrt() {
    let r = make_registry();
    assert_eq!(r.call("sqrt", &[Value::Float(OrderedFloat(9.0))]), Ok(Value::Float(OrderedFloat(3.0))));
}

#[test]
fn test_sign() {
    let r = make_registry();
    assert_eq!(r.call("sign", &[Value::Int(-5)]), Ok(Value::Int(-1)));
    assert_eq!(r.call("sign", &[Value::Int(0)]), Ok(Value::Int(0)));
    assert_eq!(r.call("sign", &[Value::Int(5)]), Ok(Value::Int(1)));
}

#[test]
fn test_ln() {
    let r = make_registry();
    let result = r.call("ln", &[Value::Float(OrderedFloat(1.0))]);
    assert_eq!(result, Ok(Value::Float(OrderedFloat(0.0))));
}
```

### 8b-8e. Implement, test, commit

Implement `abs`, `ceil`/`ceiling`, `floor`, `round`, `power`/`pow`, `mod`/`modulus`, `sqrt`, `sign`, `truncate`/`trunc`, `ln`, `log2`, `log10`, `exp`, `rand`/`random`, `pi`, `e`, `infinity`, `nan`, `is_nan`, `is_finite`, `is_infinite`, `radians`, `degrees`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `cosh`, `tanh`.

```bash
cargo test --lib functions::arithmetic::tests -- --test-threads=1
git commit -m "Add new arithmetic functions ported from velox prestosql"
```

---

## Step 9: Add new string functions (replace, split, starts_with, ends_with, reverse, etc.)

**File**: Modify `src/functions/string.rs`

### 9a-9e. Test, implement, commit

Add `replace`, `split`, `split_part`, `starts_with`, `ends_with`, `reverse` (with dynamic dispatch for strings and arrays), `concat` (multi-arg, registered as `concat_ws` or separate from the binary `Concat`), `lpad`, `rpad`, `position`/`strpos`, `repeat`, `ltrim`, `rtrim`, `chr`, `codepoint`, `levenshtein_distance`, `hamming_distance`.

```bash
cargo test --lib functions::string::tests -- --test-threads=1
git commit -m "Add new string functions ported from velox prestosql"
```

---

## Step 10: Add regexp functions with caching

**Files**: Create `src/functions/regexp.rs`, modify `Cargo.toml`

### 10a. Add `lru` dependency to Cargo.toml

```toml
lru = "0.12"
```

### 10b-10e. Test, implement, commit

Implement `regexp_like`, `regexp_extract`, `regexp_extract_all`, `regexp_replace`, `regexp_split`, `regexp_count` with `thread_local!` LRU regex cache (capacity 64).

```bash
cargo test --lib functions::regexp::tests -- --test-threads=1
git commit -m "Add regexp functions with LRU regex caching"
```

---

## Step 11: Add JSON functions

**File**: Create `src/functions/json.rs`

### 11a-11e. Test, implement, commit

Implement `json_extract`, `json_extract_scalar`, `json_array_length`, `json_array_contains`, `json_size`, `json_format`, `json_parse`, `is_json_scalar`. JSON values are treated as `Value::String` (parsed on demand using the existing `json` crate).

```bash
cargo test --lib functions::json::tests -- --test-threads=1
git commit -m "Add JSON functions (Presto semantics, JSON as string)"
```

---

## Step 12: Add bitwise functions

**File**: Create `src/functions/bitwise.rs`

### 12a-12e. Test, implement, commit

Implement `bitwise_and`, `bitwise_or`, `bitwise_xor`, `bitwise_not`, `bit_count`, `bitwise_shift_left`, `bitwise_shift_right`.

```bash
cargo test --lib functions::bitwise::tests -- --test-threads=1
git commit -m "Add bitwise functions ported from velox prestosql"
```

---

## Step 13: Add new datetime functions

**File**: Modify `src/functions/datetime.rs`

### 13a-13e. Test, implement, commit

Add `date_trunc`, `date_add`, `date_diff`, `year`, `month`, `day`, `hour`, `minute`, `second`, `millisecond`, `quarter`, `week`, `day_of_week`, `day_of_year`, `now`, `from_unixtime`, `to_unixtime`.

```bash
cargo test --lib functions::datetime::tests -- --test-threads=1
git commit -m "Add new datetime functions ported from velox prestosql"
```

---

## Step 14: Add array functions

**File**: Create `src/functions/array.rs`

### 14a-14e. Test, implement, commit

Implement `array_sort`, `array_distinct`, `array_contains`, `array_join`, `array_min`, `array_max`, `array_position`, `cardinality`, `flatten`, `array_intersect`, `array_union`, `array_except`, `array_remove`, `slice`, `shuffle`.

```bash
cargo test --lib functions::array::tests -- --test-threads=1
git commit -m "Add array functions ported from velox prestosql"
```

---

## Step 15: Add map functions

**File**: Create `src/functions/map.rs`

### 15a-15e. Test, implement, commit

Implement `map_keys`, `map_values`, `map_entries`, `element_at`, `map_concat`, `map_from_entries`.

```bash
cargo test --lib functions::map::tests -- --test-threads=1
git commit -m "Add map functions ported from velox prestosql"
```

---

## Step 16: Add type conversion functions

**File**: Create `src/functions/type_conversion.rs`

### 16a-16e. Test, implement, commit

Implement `typeof`, `try_cast`, `to_base`, `from_base`, `format_number`.

```bash
cargo test --lib functions::type_conversion::tests -- --test-threads=1
git commit -m "Add type conversion functions"
```

---

## Step 17: Create lib.rs and public API

**Files**: Create `src/lib.rs`, modify `src/main.rs`

### 17a. Write implementation

**`src/lib.rs`**:

```rust
extern crate chrono;
extern crate nom;
extern crate prettytable;
#[macro_use]
extern crate lazy_static;
extern crate pdatastructs;

pub mod functions;
pub mod common;
pub mod app;
mod execution;
mod logical;
mod syntax;
```

**`src/main.rs`** -- Keep `#[macro_use] extern crate lazy_static;` for `TABLE_SPEC_REGEX`:

```rust
#[macro_use]
extern crate lazy_static;

use logq::app::{self, AppError, OutputMode};
use logq::common;

// Remove mod declarations for app, common, execution, logical, syntax.
// Keep main() logic and TABLE_SPEC_REGEX.

lazy_static! {
    static ref TABLE_SPEC_REGEX: Regex = Regex::new(r#"([0-9a-zA-Z]+):([a-zA-Z]+)=([^=\s"':]+)"#).unwrap();
}

fn main() {
    // ... unchanged
}
```

**Note on `#[macro_use] extern crate lazy_static`**: Both `lib.rs` and `main.rs` need this declaration in edition 2018. `lib.rs` needs it because `common/types.rs` (lines 12-17) uses `lazy_static!` for `HOST_REGEX`, `SPLIT_HTTP_LINE_REGEX`, `SPLIT_TIME_INTERVAL_LINE_REGEX`. `main.rs` needs it for its own `TABLE_SPEC_REGEX` (line 22-24). In the dual-target layout, each target has its own crate scope, so the `#[macro_use]` from `lib.rs` does not leak into `main.rs`.

Visibility changes:
- `Value`: `pub(crate)` -> `pub` in `common/types.rs`
- `ExpressionError`: `pub(crate)` -> `pub` in `execution/types.rs`

### 17b. Run all tests

```bash
cargo test
```

### 17c. Commit

```bash
git commit -m "Create lib.rs with public API for function registry"
```

---

## Step 18: Final end-to-end verification

### 18a. Run full test suite

```bash
cargo test
```

### 18b. Run benchmarks

```bash
cargo bench
```

### 18c. Test a custom function from a hypothetical library consumer perspective

Write a small integration test in `tests/` that uses the public API:

```rust
// tests/custom_function.rs
use logq::functions::{FunctionRegistry, FunctionDef, Arity, NullHandling, register_all};
use logq::common::types::Value;

#[test]
fn test_custom_function_registration() {
    let mut registry = register_all().unwrap();
    registry.register(FunctionDef {
        name: "double".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Int(i) => Ok(Value::Int(i * 2)),
            _ => panic!("unexpected type"),
        }),
    }).unwrap();

    assert_eq!(registry.call("double", &[Value::Int(21)]), Ok(Value::Int(42)));
    assert_eq!(registry.call("double", &[Value::Null]), Ok(Value::Null));
}
```

```bash
cargo test --test custom_function
```

### 18d. Commit

```bash
git commit -m "Add integration test for custom function registration via public API"
```
