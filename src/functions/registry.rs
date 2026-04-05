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
        // Try direct lookup first (name is usually pre-lowercased at plan creation time)
        let def = if let Some(d) = self.functions.get(name) {
            d
        } else {
            // Fallback to lowercase for backwards compatibility (tests, etc.)
            let key = name.to_ascii_lowercase();
            self.functions.get(&key)
                .ok_or(ExpressionError::UnknownFunction)?
        };

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Value;

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
