use crate::common::types::Value;
use crate::execution::types::ExpressionError;
use crate::functions::registry::{FunctionDef, FunctionRegistry, Arity, NullHandling, RegistryError};

pub fn register(registry: &mut FunctionRegistry) -> Result<(), RegistryError> {
    // host_name: extract hostname from Host
    registry.register(FunctionDef {
        name: "host_name".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Custom,
        func: Box::new(|args| match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Missing => Ok(Value::Missing),
            Value::Host(h) => Ok(Value::String(h.hostname.clone().into())),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // host_port: extract port from Host
    registry.register(FunctionDef {
        name: "host_port".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Custom,
        func: Box::new(|args| match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Missing => Ok(Value::Missing),
            Value::Host(h) => Ok(Value::Int(i32::from(h.port))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{Value, Host};

    fn make_registry() -> FunctionRegistry {
        let mut r = FunctionRegistry::new();
        register(&mut r).unwrap();
        r
    }

    fn make_host(hostname: &str, port: u16) -> Value {
        Value::Host(Box::new(Host {
            hostname: hostname.to_string(),
            port,
        }))
    }

    #[test]
    fn test_host_name() {
        let r = make_registry();
        let host = make_host("example.com", 8080);
        assert_eq!(r.call("host_name", &[host]), Ok(Value::String("example.com".to_string().into())));
    }

    #[test]
    fn test_host_name_null_missing() {
        let r = make_registry();
        assert_eq!(r.call("host_name", &[Value::Null]), Ok(Value::Null));
        assert_eq!(r.call("host_name", &[Value::Missing]), Ok(Value::Missing));
    }

    #[test]
    fn test_host_port() {
        let r = make_registry();
        let host = make_host("example.com", 8080);
        assert_eq!(r.call("host_port", &[host]), Ok(Value::Int(8080)));
    }

    #[test]
    fn test_host_port_null_missing() {
        let r = make_registry();
        assert_eq!(r.call("host_port", &[Value::Null]), Ok(Value::Null));
        assert_eq!(r.call("host_port", &[Value::Missing]), Ok(Value::Missing));
    }

    #[test]
    fn test_host_port_zero() {
        let r = make_registry();
        let host = make_host("localhost", 0);
        assert_eq!(r.call("host_port", &[host]), Ok(Value::Int(0)));
    }

    #[test]
    fn test_host_name_invalid_type() {
        let r = make_registry();
        assert_eq!(r.call("host_name", &[Value::Int(42)]), Err(ExpressionError::InvalidArguments));
    }
}
