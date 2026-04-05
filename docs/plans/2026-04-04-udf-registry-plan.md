# Plan: User-Defined Function Registry for logq

**Goal**: Replace hardcoded function dispatch with a registry-based system, migrate all existing functions, port new functions from velox's prestosql, and expose a public API for custom function registration.

**Architecture**: `FunctionRegistry` (HashMap<String, FunctionDef>) with `Arc` sharing through the execution pipeline. Functions are `Box<dyn Fn(&[Value]) -> ExpressionResult<Value>>` closures with convention-based null propagation. Validation at planning time, dispatch at execution time.

**Tech Stack**: Rust, edition 2018. Dependencies: `lru` (new, for regex caching). Existing: `regex`, `chrono`, `ordered-float`, `url`, `json`, `linked-hash-map`.

**Design doc**: `docs/plans/2026-04-04-udf-registry-design-final.md`

---

## Task Dependencies

| Group | Steps | Can Parallelize | Notes |
|-------|-------|-----------------|-------|
| 1 | Step 1 | No | Registry core types — foundation for everything |
| 2 | Steps 2-3 | Yes (independent files) | Migrate arithmetic + string functions into registry |
| 3 | Step 4 | No | Migrate url/host/datetime functions |
| 4 | Step 5 | No | Thread Arc<FunctionRegistry> through pipeline — depends on all functions being registered |
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
    fn test_concat_null_propagation() {
        let r = make_registry();
        // After migration, Missing takes precedence (bug fix from design)
        assert_eq!(r.call("Concat", &[Value::Missing, Value::String("x".to_string())]), Ok(Value::Missing));
        assert_eq!(r.call("Concat", &[Value::Null, Value::String("x".to_string())]), Ok(Value::Null));
    }
}
```

### 3b. Run test to verify it fails

```bash
cargo test --lib functions::string::tests -- --test-threads=1
```

### 3c. Write implementation

```rust
// src/functions/string.rs
use crate::common::types::Value;
use crate::execution::types::ExpressionError;
use crate::functions::registry::{FunctionDef, FunctionRegistry, Arity, NullHandling, RegistryError};

pub fn register(registry: &mut FunctionRegistry) -> Result<(), RegistryError> {
    registry.register(FunctionDef {
        name: "upper".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => Ok(Value::String(s.to_uppercase())),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    registry.register(FunctionDef {
        name: "lower".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => Ok(Value::String(s.to_lowercase())),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    registry.register(FunctionDef {
        name: "char_length".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => Ok(Value::Int(s.len() as i32)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    registry.register(FunctionDef {
        name: "character_length".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => Ok(Value::Int(s.len() as i32)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    registry.register(FunctionDef {
        name: "substring".to_string(),
        arity: Arity::Range(2, 3),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => {
                let start = match &args[1] {
                    Value::Int(i) => (*i - 1).max(0) as usize,
                    _ => return Err(ExpressionError::InvalidArguments),
                };
                if args.len() == 3 {
                    let len = match &args[2] {
                        Value::Int(i) => (*i).max(0) as usize,
                        _ => return Err(ExpressionError::InvalidArguments),
                    };
                    Ok(Value::String(s.chars().skip(start).take(len).collect()))
                } else {
                    Ok(Value::String(s.chars().skip(start).collect()))
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    registry.register(FunctionDef {
        name: "trim".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => Ok(Value::String(s.trim().to_string())),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // Concat binary operator (||)
    registry.register(FunctionDef {
        name: "Concat".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate, // Fixes existing bug: Missing now takes precedence
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(a), Value::String(b)) => {
                let mut result = a.clone();
                result.push_str(b);
                Ok(Value::String(result))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    Ok(())
}
```

Add `pub mod string;` to `src/functions/mod.rs`.

### 3d. Run test to verify it passes

```bash
cargo test --lib functions::string::tests -- --test-threads=1
```

### 3e. Commit

```bash
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
            method: "GET".to_string(),
            url,
            version: "HTTP/1.1".to_string(),
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

Tests for host.rs and datetime.rs follow the same pattern — test each function with valid input, null, and missing.

### 4b. Run test to verify it fails

```bash
cargo test --lib functions::url::tests functions::host::tests functions::datetime::tests -- --test-threads=1
```

### 4c. Write implementation

Migrate the logic from `evaluate_url_functions()` (lines 231-361 of `execution/types.rs`), `evaluate_host_functions()` (lines 363-385), `date_part` (lines 485-504), and `time_bucket` (lines 506-599) into their respective registry modules. Each function is registered with `NullHandling::Custom` for url/host (they pattern-match on domain types and need explicit Null/Missing handling) and `NullHandling::Propagate` for datetime.

Add `pub mod url;`, `pub mod host;`, `pub mod datetime;` to `src/functions/mod.rs`.

### 4d. Run test to verify it passes

```bash
cargo test --lib functions::url::tests functions::host::tests functions::datetime::tests -- --test-threads=1
```

### 4e. Commit

```bash
git commit -m "Migrate url, host, and datetime functions to function registry"
```

---

## Step 5: Thread Arc<FunctionRegistry> through pipeline, replace evaluate() dispatch

**Files**: Modify `src/functions/mod.rs`, `src/app.rs`, `src/execution/types.rs`, `src/execution/stream.rs`, `src/common/types.rs`

This is the largest step. Changes:

### 5a. Write failing test

The existing integration tests in `src/app.rs` (starting at line 200) will serve as the regression suite. They call `app::run()` which currently uses the hardcoded `evaluate()`. After this step, `run()` will create a registry and thread it through.

No new tests needed — existing ~58 tests must pass.

### 5b. Write implementation

**5b-i. `src/functions/mod.rs`** — Add `register_all()`:

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

**5b-ii. `src/app.rs`** — Add `Arc`, create registry, pass to planner and execution:

- Add `use std::sync::Arc;` and `use crate::functions;`
- In `run()`: Create `let registry = Arc::new(functions::register_all().map_err(...)?)` before parsing
- Pass `&registry` to `parse_query_top()`
- Pass `registry.clone()` to `physical_plan.get(variables, registry.clone())`
- Add `Registry` variant to `AppError`
- Update `explain()` similarly (pass `&registry` to planner)

**5b-iii. `src/common/types.rs`** — Add registry reference to `ParsingContext`:

The `ParsingContext` struct cannot hold a borrowed `&FunctionRegistry` without a lifetime parameter. Since `ParsingContext` is `#[derive(Clone)]` and used in `parse_query()` which is called recursively for subqueries, the simplest approach is to use `Arc<FunctionRegistry>`:

```rust
use std::sync::Arc;
use crate::functions::FunctionRegistry;

pub(crate) struct ParsingContext {
    pub(crate) table_name: String,
    pub(crate) data_source: DataSource,
    pub(crate) registry: Arc<FunctionRegistry>,
}
```

Remove `#[derive(PartialEq, Eq)]` from `ParsingContext` (FunctionRegistry doesn't implement these). Or keep `Clone` by using `Arc` which is cloneable.

**5b-iv. `src/logical/parser.rs`** — Update signatures:

- `parse_query_top()`: Add `registry: Arc<FunctionRegistry>` parameter. Pass to `parse_query()`.
- `parse_query()`: Add `registry: Arc<FunctionRegistry>` parameter. Store in `ParsingContext`.
- `ParsingContext` creation (line 650-653): Add `registry: registry.clone()`.

**5b-v. `src/execution/types.rs`** — Thread `&Arc<FunctionRegistry>` through:

- `Expression::expression_value()` (line 134): Add `registry: &Arc<FunctionRegistry>` parameter
- In `Expression::Function` arm: Replace `evaluate(&*name, &values)` with `registry.call(name, &values)`
- In `Expression::Logic` arm: Pass `registry` to `formula.evaluate()`
- In `Expression::Branch` arm: Pass `registry` to `formula.evaluate()` and `expression_value()`
- In `Expression::Cast` arm: Pass `registry` to `inner.expression_value()`
- In `Expression::Subquery` arm: Use `registry.clone()` for `node.get()`
- `Formula::evaluate()` (line 805): Add `registry: &Arc<FunctionRegistry>` parameter. Pass through all recursive calls and `expression_value()` calls.
- `Relation::apply()` (line 710): Add `registry: &Arc<FunctionRegistry>` parameter. Pass to `expression_value()`.
- `Node::get()` (line 938): Add `registry: Arc<FunctionRegistry>` parameter. Pass `registry.clone()` to recursive `get()` calls and to stream constructors.
- Delete `evaluate()` (lines 388-697), `evaluate_url_functions()` (lines 231-361), `evaluate_host_functions()` (lines 363-385).

**5b-vi. `src/execution/stream.rs`** — Add `Arc<FunctionRegistry>` to stream structs:

- `MapStream`: Add `registry: Arc<FunctionRegistry>` field. Pass `&self.registry` to `expression_value()` in `next()`.
- `FilterStream`: Add `registry: Arc<FunctionRegistry>` field. Pass `&self.registry` to `formula.evaluate()` in `next()`.
- `GroupByStream`: Add `registry: Arc<FunctionRegistry>` field. Pass `&self.registry` to `expression_value()` in `next()`.
- `CrossJoinStream`: Add `registry: Arc<FunctionRegistry>` field. Pass `self.registry.clone()` to `node.get()`.
- `LeftJoinStream`: Add `registry: Arc<FunctionRegistry>` field. Pass `&self.registry` to `formula.evaluate()` and `self.registry.clone()` to `node.get()`.
- Update all `new()` constructors to accept `registry: Arc<FunctionRegistry>`.

### 5c. Run all existing tests

```bash
cargo test
```

All ~58 existing tests must pass.

### 5d. Commit

```bash
git commit -m "Thread Arc<FunctionRegistry> through pipeline, replace hardcoded evaluate() dispatch"
```

---

## Step 6: Add planner-time validation

**File**: Modify `src/logical/parser.rs`

### 6a. Write failing test

```rust
// In src/app.rs test section, add:
#[test]
fn test_unknown_function_error_at_planning() {
    let result = run(
        "SELECT nonexistent_func(request) FROM elb",
        common::types::DataSource::File(
            std::path::PathBuf::from("data/AWSELB.log"),
            "elb".to_string(),
            "elb".to_string(),
        ),
        OutputMode::Table,
    );
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("Unknown function") || err_msg.contains("nonexistent_func"));
}
```

### 6b. Run test to verify it fails

```bash
cargo test test_unknown_function_error_at_planning -- --test-threads=1
```

### 6c. Write implementation

In `parse_value_expression()` (line 251 of `logical/parser.rs`), the `FuncCall` arm currently just creates `Expression::Function` without validation. Add:

```rust
ast::Expression::FuncCall(func_name, select_exprs, _) => {
    // Check if it's an aggregate — if so, skip scalar validation
    let is_agg = matches!(
        func_name.to_ascii_lowercase().as_str(),
        "avg" | "count" | "first" | "last" | "max" | "min" | "sum"
        | "approx_count_distinct" | "percentile_disc" | "approx_percentile"
    );

    if !is_agg {
        ctx.registry.validate(func_name, select_exprs.len())
            .map_err(|e| match e {
                RegistryError::UnknownFunction(name) => ParseError::UnknownFunction(name),
                RegistryError::ArityMismatch { name, expected, actual } =>
                    ParseError::InvalidArguments(
                        format!("{} expects {} argument(s), got {}", name, expected, actual)
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

### 6d. Run test to verify it passes

```bash
cargo test test_unknown_function_error_at_planning -- --test-threads=1
cargo test  # all tests pass
```

### 6e. Commit

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

Implement `abs`, `ceil`, `floor`, `round`, `power`, `mod`/`modulus`, `sqrt`, `sign`, `truncate`, `ln`, `log2`, `log10`, `exp`, `rand`, `pi`, `e`, `infinity`, `nan`, `is_nan`, `is_finite`, `is_infinite`, `radians`, `degrees`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `cosh`, `tanh`.

```bash
cargo test --lib functions::arithmetic::tests -- --test-threads=1
git commit -m "Add new arithmetic functions ported from velox prestosql"
```

---

## Step 9: Add new string functions (replace, split, starts_with, ends_with, reverse, etc.)

**File**: Modify `src/functions/string.rs`

### 9a-9e. Test, implement, commit

Add `replace`, `split`, `split_part`, `starts_with`, `ends_with`, `reverse`, `concat` (multi-arg), `lpad`, `rpad`, `position`/`strpos`, `repeat`, `ltrim`, `rtrim`, `chr`, `codepoint`, `levenshtein_distance`, `hamming_distance`.

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

Implement `regexp_like`, `regexp_extract`, `regexp_extract_all`, `regexp_replace`, `regexp_split`, `regexp_count` with `thread_local!` LRU regex cache.

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

**`src/main.rs`** — Change from `mod` declarations to `use logq::`:

```rust
use logq::app::{self, AppError, OutputMode};
use logq::common;
// Remove mod declarations, keep main() logic
```

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
