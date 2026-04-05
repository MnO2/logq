use crate::common::types::Value;
use crate::execution::types::ExpressionError;
use crate::functions::registry::{Arity, FunctionDef, FunctionRegistry, NullHandling, RegistryError};

pub fn register(registry: &mut FunctionRegistry) -> Result<(), RegistryError> {
    // map_keys: Return all keys of an object as an array of strings
    registry.register(FunctionDef {
        name: "map_keys".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Object(map) => {
                let keys: Vec<Value> = map.keys().map(|k| Value::String(k.clone())).collect();
                Ok(Value::Array(keys))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // map_values: Return all values of an object as an array
    registry.register(FunctionDef {
        name: "map_values".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Object(map) => {
                let values: Vec<Value> = map.values().cloned().collect();
                Ok(Value::Array(values))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // map_entries: Return all entries of an object as an array of [key, value] arrays
    registry.register(FunctionDef {
        name: "map_entries".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Object(map) => {
                let entries: Vec<Value> = map
                    .iter()
                    .map(|(k, v)| Value::Array(vec![Value::String(k.clone()), v.clone()]))
                    .collect();
                Ok(Value::Array(entries))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // element_at: Access element by key (object) or 1-based index (array)
    registry.register(FunctionDef {
        name: "element_at".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Object(map), Value::String(key)) => {
                Ok(map.get(key).cloned().unwrap_or(Value::Null))
            }
            (Value::Array(arr), Value::Int(idx)) => {
                let idx = (*idx - 1) as usize; // 1-based to 0-based
                Ok(arr.get(idx).cloned().unwrap_or(Value::Null))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // map_concat: Merge two objects; second object's values win on key conflicts
    registry.register(FunctionDef {
        name: "map_concat".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Object(m1), Value::Object(m2)) => {
                let mut result = m1.clone();
                for (k, v) in m2.iter() {
                    result.insert(k.clone(), v.clone());
                }
                Ok(Value::Object(result))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // map_from_entries: Convert array of [key, value] arrays to an object
    registry.register(FunctionDef {
        name: "map_from_entries".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Array(entries) => {
                let mut map = linked_hash_map::LinkedHashMap::new();
                for entry in entries {
                    match entry {
                        Value::Array(pair) if pair.len() == 2 => match &pair[0] {
                            Value::String(key) => {
                                map.insert(key.clone(), pair[1].clone());
                            }
                            _ => return Err(ExpressionError::InvalidArguments),
                        },
                        _ => return Err(ExpressionError::InvalidArguments),
                    }
                }
                Ok(Value::Object(Box::new(map)))
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
    use linked_hash_map::LinkedHashMap;

    fn make_registry() -> FunctionRegistry {
        let mut r = FunctionRegistry::new();
        register(&mut r).unwrap();
        r
    }

    fn make_object(pairs: &[(&str, Value)]) -> Value {
        let mut map = LinkedHashMap::new();
        for (k, v) in pairs {
            map.insert(k.to_string(), v.clone());
        }
        Value::Object(Box::new(map))
    }

    #[test]
    fn test_map_keys() {
        let r = make_registry();
        let obj = make_object(&[("a", Value::Int(1)), ("b", Value::Int(2))]);
        assert_eq!(
            r.call("map_keys", &[obj]),
            Ok(Value::Array(vec![
                Value::String("a".into()),
                Value::String("b".into())
            ]))
        );
    }

    #[test]
    fn test_map_values() {
        let r = make_registry();
        let obj = make_object(&[("a", Value::Int(1)), ("b", Value::Int(2))]);
        assert_eq!(
            r.call("map_values", &[obj]),
            Ok(Value::Array(vec![Value::Int(1), Value::Int(2)]))
        );
    }

    #[test]
    fn test_map_entries() {
        let r = make_registry();
        let obj = make_object(&[("a", Value::Int(1))]);
        assert_eq!(
            r.call("map_entries", &[obj]),
            Ok(Value::Array(vec![Value::Array(vec![
                Value::String("a".into()),
                Value::Int(1)
            ])]))
        );
    }

    #[test]
    fn test_element_at_object() {
        let r = make_registry();
        let obj = make_object(&[("name", Value::String("Alice".into()))]);
        assert_eq!(
            r.call(
                "element_at",
                &[obj.clone(), Value::String("name".into())]
            ),
            Ok(Value::String("Alice".into()))
        );
        assert_eq!(
            r.call(
                "element_at",
                &[obj, Value::String("missing_key".into())]
            ),
            Ok(Value::Null)
        );
    }

    #[test]
    fn test_element_at_array() {
        let r = make_registry();
        let arr = Value::Array(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
        assert_eq!(
            r.call("element_at", &[arr.clone(), Value::Int(2)]),
            Ok(Value::Int(20))
        );
        assert_eq!(
            r.call("element_at", &[arr, Value::Int(10)]),
            Ok(Value::Null)
        );
    }

    #[test]
    fn test_map_concat() {
        let r = make_registry();
        let m1 = make_object(&[("a", Value::Int(1)), ("b", Value::Int(2))]);
        let m2 = make_object(&[("b", Value::Int(99)), ("c", Value::Int(3))]);
        let result = r.call("map_concat", &[m1, m2]).unwrap();
        // b should be 99 (m2 wins), a=1, c=3
        if let Value::Object(map) = &result {
            assert_eq!(map.get("a"), Some(&Value::Int(1)));
            assert_eq!(map.get("b"), Some(&Value::Int(99)));
            assert_eq!(map.get("c"), Some(&Value::Int(3)));
        } else {
            panic!("Expected Object");
        }
    }

    #[test]
    fn test_map_from_entries() {
        let r = make_registry();
        let entries = Value::Array(vec![
            Value::Array(vec![Value::String("x".into()), Value::Int(1)]),
            Value::Array(vec![Value::String("y".into()), Value::Int(2)]),
        ]);
        let result = r.call("map_from_entries", &[entries]).unwrap();
        if let Value::Object(map) = &result {
            assert_eq!(map.get("x"), Some(&Value::Int(1)));
            assert_eq!(map.get("y"), Some(&Value::Int(2)));
        } else {
            panic!("Expected Object");
        }
    }

    #[test]
    fn test_map_null_propagation() {
        let r = make_registry();
        assert_eq!(r.call("map_keys", &[Value::Null]), Ok(Value::Null));
        assert_eq!(r.call("map_keys", &[Value::Missing]), Ok(Value::Missing));
    }
}
