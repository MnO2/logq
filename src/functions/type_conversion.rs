use crate::common::types::Value;
use crate::execution::types::ExpressionError;
use crate::functions::registry::{FunctionDef, FunctionRegistry, Arity, NullHandling, RegistryError};
use ordered_float::OrderedFloat;

pub fn register(registry: &mut FunctionRegistry) -> Result<(), RegistryError> {
    // typeof(value) → type name as string
    registry.register(FunctionDef {
        name: "typeof".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Custom,
        func: Box::new(|args| match &args[0] {
            Value::Int(_) => Ok(Value::String("integer".into())),
            Value::Float(_) => Ok(Value::String("float".into())),
            Value::String(_) => Ok(Value::String("string".into())),
            Value::Boolean(_) => Ok(Value::String("boolean".into())),
            Value::Null => Ok(Value::String("null".into())),
            Value::Missing => Ok(Value::String("missing".into())),
            Value::DateTime(_) => Ok(Value::String("datetime".into())),
            Value::Array(_) => Ok(Value::String("array".into())),
            Value::Object(_) => Ok(Value::String("object".into())),
            Value::HttpRequest(_) => Ok(Value::String("http_request".into())),
            Value::Host(_) => Ok(Value::String("host".into())),
        }),
    })?;

    // try_cast(value, type_name) → value or Null
    registry.register(FunctionDef {
        name: "try_cast".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (value, Value::String(target_type)) => {
                match target_type.to_ascii_lowercase().as_str() {
                    "integer" | "int" => match value {
                        Value::Int(_) => Ok(value.clone()),
                        Value::Float(f) => Ok(Value::Int(f.into_inner() as i32)),
                        Value::String(s) => match s.parse::<i32>() {
                            Ok(n) => Ok(Value::Int(n)),
                            Err(_) => Ok(Value::Null),
                        },
                        Value::Boolean(b) => Ok(Value::Int(if *b { 1 } else { 0 })),
                        _ => Ok(Value::Null),
                    },
                    "float" | "double" | "real" => match value {
                        Value::Float(_) => Ok(value.clone()),
                        Value::Int(i) => Ok(Value::Float(OrderedFloat(*i as f32))),
                        Value::String(s) => match s.parse::<f32>() {
                            Ok(f) => Ok(Value::Float(OrderedFloat(f))),
                            Err(_) => Ok(Value::Null),
                        },
                        Value::Boolean(b) => Ok(Value::Float(OrderedFloat(if *b { 1.0 } else { 0.0 }))),
                        _ => Ok(Value::Null),
                    },
                    "string" | "varchar" => match value {
                        Value::String(_) => Ok(value.clone()),
                        Value::Int(i) => Ok(Value::String(i.to_string())),
                        Value::Float(f) => Ok(Value::String(f.to_string())),
                        Value::Boolean(b) => Ok(Value::String(b.to_string())),
                        _ => Ok(Value::Null),
                    },
                    "boolean" => match value {
                        Value::Boolean(_) => Ok(value.clone()),
                        Value::String(s) => match s.to_ascii_lowercase().as_str() {
                            "true" => Ok(Value::Boolean(true)),
                            "false" => Ok(Value::Boolean(false)),
                            _ => Ok(Value::Null),
                        },
                        Value::Int(i) => Ok(Value::Boolean(*i != 0)),
                        _ => Ok(Value::Null),
                    },
                    _ => Ok(Value::Null),
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // to_base(number, radix) → string representation in given base
    registry.register(FunctionDef {
        name: "to_base".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Int(n), Value::Int(radix)) => {
                if *radix < 2 || *radix > 36 {
                    return Err(ExpressionError::InvalidArguments);
                }
                let mut result = String::new();
                let mut num = (*n).abs();
                let radix = *radix as u32;
                if num == 0 { return Ok(Value::String("0".into())); }
                while num > 0 {
                    let digit = (num as u32 % radix) as u8;
                    let c = if digit < 10 { b'0' + digit } else { b'a' + digit - 10 };
                    result.push(c as char);
                    num = (num as u32 / radix) as i32;
                }
                if *n < 0 { result.push('-'); }
                Ok(Value::String(result.chars().rev().collect()))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // from_base(string, radix) → integer parsed from given base
    registry.register(FunctionDef {
        name: "from_base".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(s), Value::Int(radix)) => {
                if *radix < 2 || *radix > 36 {
                    return Err(ExpressionError::InvalidArguments);
                }
                match i32::from_str_radix(s, *radix as u32) {
                    Ok(n) => Ok(Value::Int(n)),
                    Err(_) => Err(ExpressionError::InvalidArguments),
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // format_number(number, decimal_places) → formatted string
    registry.register(FunctionDef {
        name: "format_number".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Int(n), Value::Int(places)) => {
                Ok(Value::String(format!("{:.prec$}", *n as f64, prec = *places as usize)))
            }
            (Value::Float(f), Value::Int(places)) => {
                Ok(Value::String(format!("{:.prec$}", f.into_inner() as f64, prec = *places as usize)))
            }
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
    fn test_typeof_int() {
        let r = make_registry();
        assert_eq!(r.call("typeof", &[Value::Int(42)]), Ok(Value::String("integer".into())));
    }

    #[test]
    fn test_typeof_null() {
        let r = make_registry();
        assert_eq!(r.call("typeof", &[Value::Null]), Ok(Value::String("null".into())));
    }

    #[test]
    fn test_typeof_string() {
        let r = make_registry();
        assert_eq!(r.call("typeof", &[Value::String("hi".into())]), Ok(Value::String("string".into())));
    }

    #[test]
    fn test_try_cast_string_to_int() {
        let r = make_registry();
        assert_eq!(
            r.call("try_cast", &[Value::String("42".into()), Value::String("integer".into())]),
            Ok(Value::Int(42))
        );
    }

    #[test]
    fn test_try_cast_invalid_returns_null() {
        let r = make_registry();
        assert_eq!(
            r.call("try_cast", &[Value::String("not_a_number".into()), Value::String("integer".into())]),
            Ok(Value::Null)
        );
    }

    #[test]
    fn test_try_cast_int_to_string() {
        let r = make_registry();
        assert_eq!(
            r.call("try_cast", &[Value::Int(42), Value::String("string".into())]),
            Ok(Value::String("42".into()))
        );
    }

    #[test]
    fn test_try_cast_string_to_boolean() {
        let r = make_registry();
        assert_eq!(
            r.call("try_cast", &[Value::String("true".into()), Value::String("boolean".into())]),
            Ok(Value::Boolean(true))
        );
    }

    #[test]
    fn test_to_base() {
        let r = make_registry();
        assert_eq!(r.call("to_base", &[Value::Int(255), Value::Int(16)]), Ok(Value::String("ff".into())));
        assert_eq!(r.call("to_base", &[Value::Int(10), Value::Int(2)]), Ok(Value::String("1010".into())));
    }

    #[test]
    fn test_from_base() {
        let r = make_registry();
        assert_eq!(r.call("from_base", &[Value::String("ff".into()), Value::Int(16)]), Ok(Value::Int(255)));
        assert_eq!(r.call("from_base", &[Value::String("1010".into()), Value::Int(2)]), Ok(Value::Int(10)));
    }

    #[test]
    fn test_format_number() {
        let r = make_registry();
        assert_eq!(
            r.call("format_number", &[Value::Float(OrderedFloat(3.14159)), Value::Int(2)]),
            Ok(Value::String("3.14".into()))
        );
        assert_eq!(
            r.call("format_number", &[Value::Int(42), Value::Int(3)]),
            Ok(Value::String("42.000".into()))
        );
    }
}
