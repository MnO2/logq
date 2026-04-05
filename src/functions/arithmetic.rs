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
