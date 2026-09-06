use crate::common::types::Value;
use crate::execution::types::{ExpressionError, ExpressionResult};
use std::collections::HashMap;
use std::sync::Arc;

pub type ScalarFunction = dyn Fn(&[Value]) -> ExpressionResult<Value> + Send + Sync;

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

impl Arity {
    fn accepts(&self, count: usize) -> bool {
        match self {
            Self::Exact(n) => count == *n,
            Self::Range(min, max) => count >= *min && count <= *max,
            Self::Variadic(min) => count >= *min,
        }
    }
}

pub struct FunctionDef {
    pub name: String,
    pub arity: Arity,
    pub null_handling: NullHandling,
    pub func: Box<ScalarFunction>,
}

impl FunctionDef {
    fn call(&self, args: &[Value]) -> ExpressionResult<Value> {
        match self.null_handling {
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
                (self.func)(args)
            }
            NullHandling::Custom => (self.func)(args),
        }
    }
}

/// Own the definition selected from one registry, including its null policy.
/// No name lookup or registry identity check is needed at execution time.
struct RegisteredFunction {
    definition: FunctionDef,
    builtin_plus: bool,
}

pub(crate) struct ResolvedFunction(Arc<RegisteredFunction>);

impl ResolvedFunction {
    pub(crate) fn call(&self, args: &[Value]) -> ExpressionResult<Value> {
        self.0.definition.call(args)
    }

    pub(crate) fn is_builtin_plus(&self) -> bool {
        self.0.builtin_plus
    }
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
    ArityMismatch {
        name: String,
        expected: String,
        actual: usize,
    },
    #[error("Duplicate function registration: {0}")]
    DuplicateFunction(String),
}

pub struct FunctionRegistry {
    functions: HashMap<String, Arc<RegisteredFunction>>,
}

// Manual Debug impl because FunctionDef contains closures
impl std::fmt::Debug for FunctionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionRegistry")
            .field("functions", &format!("<{} functions>", self.functions.len()))
            .finish()
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionRegistry {
    pub fn new() -> Self {
        FunctionRegistry {
            functions: HashMap::with_capacity(150),
        }
    }

    pub fn register(&mut self, def: FunctionDef) -> Result<(), RegistryError> {
        self.register_entry(def, false)
    }

    // Only arithmetic registration supplies this trusted implementation tag.
    // Public registration is always opaque, including a function named Plus.
    pub(super) fn register_builtin_plus(&mut self, def: FunctionDef) -> Result<(), RegistryError> {
        self.register_entry(def, true)
    }

    fn register_entry(&mut self, def: FunctionDef, builtin_plus: bool) -> Result<(), RegistryError> {
        let key = def.name.to_ascii_lowercase();
        if self.functions.contains_key(&key) {
            return Err(RegistryError::DuplicateFunction(def.name.clone()));
        }
        self.functions.insert(
            key,
            Arc::new(RegisteredFunction {
                definition: def,
                builtin_plus,
            }),
        );
        Ok(())
    }

    pub(crate) fn function_names(&self) -> impl Iterator<Item = &str> {
        self.functions.keys().map(String::as_str)
    }

    pub fn validate(&self, name: &str, arg_count: usize) -> Result<(), RegistryError> {
        let key = name.to_ascii_lowercase();
        let def = self
            .functions
            .get(&key)
            .ok_or_else(|| RegistryError::UnknownFunction(name.to_string()))?;
        let def = &def.definition;

        if !def.arity.accepts(arg_count) {
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
        let definition = &self.lookup(name).ok_or(ExpressionError::UnknownFunction)?.definition;
        if !definition.arity.accepts(args.len()) {
            return Err(ExpressionError::InvalidArguments);
        }
        definition.call(args)
    }

    pub(crate) fn resolve(&self, name: &str) -> Option<ResolvedFunction> {
        self.lookup(name).cloned().map(ResolvedFunction)
    }

    fn lookup(&self, name: &str) -> Option<&Arc<RegisteredFunction>> {
        // Try direct lookup first (name is usually pre-lowercased at plan creation time)
        if let Some(d) = self.functions.get(name) {
            Some(d)
        } else {
            // Fallback to lowercase for backwards compatibility (tests, etc.)
            let key = name.to_ascii_lowercase();
            self.functions.get(&key)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Value;

    #[test]
    fn resolved_handles_keep_registry_identity_and_null_policy() {
        let registry = |value, null_handling| {
            let mut registry = FunctionRegistry::new();
            registry
                .register(FunctionDef {
                    name: "ADD".into(),
                    arity: Arity::Exact(2),
                    null_handling,
                    func: Box::new(move |_| Ok(Value::Int(value))),
                })
                .unwrap();
            registry
        };
        let first = registry(11, NullHandling::Custom);
        let second = registry(22, NullHandling::Propagate);
        let first_handle = first.resolve("aDd").unwrap();
        let second_handle = second.resolve("ADD").unwrap();
        assert!(first.resolve("unknown").is_none());
        drop(first);
        drop(second);
        assert_eq!(first_handle.call(&[Value::Null, Value::Missing]), Ok(Value::Int(11)));
        assert_eq!(second_handle.call(&[Value::Null, Value::Missing]), Ok(Value::Missing));
        assert_eq!(second_handle.call(&[Value::Null, Value::Int(1)]), Ok(Value::Null));
        assert_eq!(second_handle.call(&[Value::Int(1), Value::Int(2)]), Ok(Value::Int(22)));
    }
    #[test]
    fn test_register_and_call() {
        let mut registry = FunctionRegistry::new();
        registry
            .register(FunctionDef {
                name: "test_add".to_string(),
                arity: Arity::Exact(2),
                null_handling: NullHandling::Propagate,
                func: Box::new(|args| match (&args[0], &args[1]) {
                    (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
                    _ => Err(ExpressionError::InvalidArguments),
                }),
            })
            .unwrap();

        let result = registry.call("test_add", &[Value::Int(1), Value::Int(2)]);
        assert_eq!(result, Ok(Value::Int(3)));
    }

    #[test]
    fn test_case_insensitive_lookup() {
        let mut registry = FunctionRegistry::new();
        registry
            .register(FunctionDef {
                name: "MyFunc".to_string(),
                arity: Arity::Exact(1),
                null_handling: NullHandling::Propagate,
                func: Box::new(|args| Ok(args[0].clone())),
            })
            .unwrap();

        let result = registry.call("MYFUNC", &[Value::Int(1)]);
        assert_eq!(result, Ok(Value::Int(1)));
    }

    #[test]
    fn test_null_propagation_missing_precedence() {
        let mut registry = FunctionRegistry::new();
        registry
            .register(FunctionDef {
                name: "f".to_string(),
                arity: Arity::Exact(2),
                null_handling: NullHandling::Propagate,
                func: Box::new(|_| Ok(Value::Int(99))),
            })
            .unwrap();

        // Missing takes precedence over Null
        assert_eq!(registry.call("f", &[Value::Missing, Value::Null]), Ok(Value::Missing));
        assert_eq!(registry.call("f", &[Value::Null, Value::Missing]), Ok(Value::Missing));
        // Null alone returns Null
        assert_eq!(registry.call("f", &[Value::Null, Value::Int(1)]), Ok(Value::Null));
        // No null/missing -> calls function
        assert_eq!(registry.call("f", &[Value::Int(1), Value::Int(2)]), Ok(Value::Int(99)));
    }

    #[test]
    fn test_custom_null_handling() {
        let mut registry = FunctionRegistry::new();
        registry
            .register(FunctionDef {
                name: "custom".to_string(),
                arity: Arity::Exact(1),
                null_handling: NullHandling::Custom,
                func: Box::new(|args| match &args[0] {
                    Value::Null => Ok(Value::String("was_null".to_string().into())),
                    other => Ok(other.clone()),
                }),
            })
            .unwrap();

        assert_eq!(
            registry.call("custom", &[Value::Null]),
            Ok(Value::String("was_null".to_string().into()))
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
        registry
            .register(FunctionDef {
                name: "f".to_string(),
                arity: Arity::Exact(2),
                null_handling: NullHandling::Propagate,
                func: Box::new(|_| Ok(Value::Null)),
            })
            .unwrap();

        assert!(registry.validate("f", 2).is_ok());
        assert!(registry.validate("f", 3).is_err());
    }

    #[test]
    fn test_validate_variadic_arity() {
        let mut registry = FunctionRegistry::new();
        registry
            .register(FunctionDef {
                name: "f".to_string(),
                arity: Arity::Variadic(1),
                null_handling: NullHandling::Propagate,
                func: Box::new(|_| Ok(Value::Null)),
            })
            .unwrap();

        assert!(registry.validate("f", 0).is_err());
        assert!(registry.validate("f", 1).is_ok());
        assert!(registry.validate("f", 5).is_ok());
    }

    #[test]
    fn test_validate_range_arity() {
        let mut registry = FunctionRegistry::new();
        registry
            .register(FunctionDef {
                name: "f".to_string(),
                arity: Arity::Range(2, 3),
                null_handling: NullHandling::Propagate,
                func: Box::new(|_| Ok(Value::Null)),
            })
            .unwrap();

        assert!(registry.validate("f", 1).is_err());
        assert!(registry.validate("f", 2).is_ok());
        assert!(registry.validate("f", 3).is_ok());
        assert!(registry.validate("f", 4).is_err());
    }

    #[test]
    fn test_duplicate_registration_error() {
        let mut registry = FunctionRegistry::new();
        registry
            .register(FunctionDef {
                name: "f".to_string(),
                arity: Arity::Exact(1),
                null_handling: NullHandling::Propagate,
                func: Box::new(|_| Ok(Value::Null)),
            })
            .unwrap();

        let result = registry.register(FunctionDef {
            name: "f".to_string(),
            arity: Arity::Exact(1),
            null_handling: NullHandling::Propagate,
            func: Box::new(|_| Ok(Value::Null)),
        });
        assert!(result.is_err());
    }
}
