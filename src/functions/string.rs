use crate::common::types::Value;
use crate::execution::types::ExpressionError;
use crate::functions::registry::{FunctionDef, FunctionRegistry, Arity, NullHandling, RegistryError};

pub fn register(registry: &mut FunctionRegistry) -> Result<(), RegistryError> {
    // Concat: binary string concatenation operator
    registry.register(FunctionDef {
        name: "Concat".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(a), Value::String(b)) => {
                let mut result = a.clone();
                result.push_str(b);
                Ok(Value::String(result))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // upper: convert string to uppercase
    registry.register(FunctionDef {
        name: "upper".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => Ok(Value::String(s.to_uppercase())),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // lower: convert string to lowercase
    registry.register(FunctionDef {
        name: "lower".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => Ok(Value::String(s.to_lowercase())),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // char_length: return character length of string
    registry.register(FunctionDef {
        name: "char_length".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => Ok(Value::Int(s.len() as i32)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // character_length: alias for char_length
    registry.register(FunctionDef {
        name: "character_length".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => Ok(Value::Int(s.len() as i32)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // substring: SUBSTRING(str, start) or SUBSTRING(str, start, length)
    // SQL SUBSTRING uses 1-based indexing
    registry.register(FunctionDef {
        name: "substring".to_string(),
        arity: Arity::Range(2, 3),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => {
                let start = match &args[1] {
                    Value::Int(i) => (*i - 1).max(0) as usize, // 1-based to 0-based
                    _ => return Err(ExpressionError::InvalidArguments),
                };
                if args.len() == 3 {
                    let len = match &args[2] {
                        Value::Int(i) => (*i).max(0) as usize,
                        _ => return Err(ExpressionError::InvalidArguments),
                    };
                    let result: String = s.chars().skip(start).take(len).collect();
                    Ok(Value::String(result))
                } else {
                    let result: String = s.chars().skip(start).collect();
                    Ok(Value::String(result))
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // trim: trim whitespace from both ends
    registry.register(FunctionDef {
        name: "trim".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => Ok(Value::String(s.trim().to_string())),
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
        // BEHAVIORAL CHANGE: Missing > Null precedence (previously Null > Missing for Concat)
        assert_eq!(r.call("Concat", &[Value::Missing, Value::String("x".to_string())]), Ok(Value::Missing));
        assert_eq!(r.call("Concat", &[Value::Null, Value::String("x".to_string())]), Ok(Value::Null));
        assert_eq!(r.call("Concat", &[Value::Null, Value::Missing]), Ok(Value::Missing));
    }
}
