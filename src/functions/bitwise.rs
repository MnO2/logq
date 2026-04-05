use crate::common::types::Value;
use crate::execution::types::ExpressionError;
use crate::functions::registry::{FunctionDef, FunctionRegistry, Arity, NullHandling, RegistryError};

pub fn register(registry: &mut FunctionRegistry) -> Result<(), RegistryError> {
    // bitwise_and(a, b) → a & b
    registry.register(FunctionDef {
        name: "bitwise_and".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a & b)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // bitwise_or(a, b) → a | b
    registry.register(FunctionDef {
        name: "bitwise_or".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a | b)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // bitwise_xor(a, b) → a ^ b
    registry.register(FunctionDef {
        name: "bitwise_xor".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a ^ b)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // bitwise_not(a) → !a
    registry.register(FunctionDef {
        name: "bitwise_not".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Int(a) => Ok(Value::Int(!a)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // bit_count(a) → count of 1 bits
    registry.register(FunctionDef {
        name: "bit_count".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Int(a) => Ok(Value::Int(a.count_ones() as i32)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // bitwise_shift_left(a, bits) → a << bits
    registry.register(FunctionDef {
        name: "bitwise_shift_left".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a << b)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // bitwise_shift_right(a, bits) → a >> bits (arithmetic shift)
    registry.register(FunctionDef {
        name: "bitwise_shift_right".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a >> b)),
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

    fn make_registry() -> FunctionRegistry {
        let mut r = FunctionRegistry::new();
        register(&mut r).unwrap();
        r
    }

    #[test]
    fn test_bitwise_and() {
        let r = make_registry();
        assert_eq!(r.call("bitwise_and", &[Value::Int(0b1100), Value::Int(0b1010)]), Ok(Value::Int(0b1000)));
    }

    #[test]
    fn test_bitwise_or() {
        let r = make_registry();
        assert_eq!(r.call("bitwise_or", &[Value::Int(0b1100), Value::Int(0b1010)]), Ok(Value::Int(0b1110)));
    }

    #[test]
    fn test_bitwise_xor() {
        let r = make_registry();
        assert_eq!(r.call("bitwise_xor", &[Value::Int(0b1100), Value::Int(0b1010)]), Ok(Value::Int(0b0110)));
    }

    #[test]
    fn test_bitwise_not() {
        let r = make_registry();
        assert_eq!(r.call("bitwise_not", &[Value::Int(0)]), Ok(Value::Int(-1)));
    }

    #[test]
    fn test_bit_count() {
        let r = make_registry();
        assert_eq!(r.call("bit_count", &[Value::Int(0b10110)]), Ok(Value::Int(3)));
        assert_eq!(r.call("bit_count", &[Value::Int(0)]), Ok(Value::Int(0)));
    }

    #[test]
    fn test_bitwise_shift_left() {
        let r = make_registry();
        assert_eq!(r.call("bitwise_shift_left", &[Value::Int(1), Value::Int(3)]), Ok(Value::Int(8)));
    }

    #[test]
    fn test_bitwise_shift_right() {
        let r = make_registry();
        assert_eq!(r.call("bitwise_shift_right", &[Value::Int(8), Value::Int(2)]), Ok(Value::Int(2)));
    }

    #[test]
    fn test_bitwise_null_propagation() {
        let r = make_registry();
        assert_eq!(r.call("bitwise_and", &[Value::Null, Value::Int(1)]), Ok(Value::Null));
        assert_eq!(r.call("bitwise_and", &[Value::Missing, Value::Int(1)]), Ok(Value::Missing));
    }
}
