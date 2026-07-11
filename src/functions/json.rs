use crate::common::types::Value;
use crate::execution::types::ExpressionError;
use crate::functions::registry::{Arity, FunctionDef, FunctionRegistry, NullHandling, RegistryError};
use serde_json::Value as JsonValue;

fn parse_json(input: &str) -> Result<JsonValue, ExpressionError> {
    serde_json::from_str(input).map_err(|_| ExpressionError::InvalidArguments)
}

fn stringify_json(value: &JsonValue) -> Result<String, ExpressionError> {
    serde_json::to_string(value).map_err(|_| ExpressionError::InvalidArguments)
}

/// Navigate a JSON value using a simple JSON path.
///
/// Supported path syntax:
/// - `$` — root value
/// - `$.key` — object member
/// - `$.key.subkey` — nested object member
/// - `$[0]` — array element
/// - `$.key[0]` — object member then array element
/// - `$.key[0].name` — chained access
fn json_navigate(value: &JsonValue, path: &str) -> Option<JsonValue> {
    let path = path.trim();

    // Strip leading "$"
    let rest = path.strip_prefix('$').unwrap_or(path);

    // If nothing remains after "$", return root
    if rest.is_empty() {
        return Some(value.clone());
    }

    // Strip leading "." if present (e.g. "$.key" -> "key")
    let rest = rest.strip_prefix('.').unwrap_or(rest);

    // Parse path into segments: split by "." but also handle "[N]"
    let segments = parse_path_segments(rest);

    let mut current = value.clone();
    for segment in segments {
        match segment {
            PathSegment::Key(key) => {
                current = current.get(&key)?.clone();
            }
            PathSegment::Index(idx) => {
                current = current.get(idx)?.clone();
            }
        }
    }

    Some(current)
}

enum PathSegment {
    Key(String),
    Index(usize),
}

/// Parse a path string like "key.subkey[0].name" into segments.
fn parse_path_segments(path: &str) -> Vec<PathSegment> {
    let mut segments = Vec::new();
    let mut chars = path.chars().peekable();
    let mut current_key = String::new();

    while let Some(&c) = chars.peek() {
        match c {
            '.' => {
                if !current_key.is_empty() {
                    segments.push(PathSegment::Key(current_key.clone()));
                    current_key.clear();
                }
                chars.next();
            }
            '[' => {
                if !current_key.is_empty() {
                    segments.push(PathSegment::Key(current_key.clone()));
                    current_key.clear();
                }
                chars.next(); // consume '['
                let mut idx_str = String::new();
                while let Some(&ic) = chars.peek() {
                    if ic == ']' {
                        chars.next(); // consume ']'
                        break;
                    }
                    idx_str.push(ic);
                    chars.next();
                }
                if let Ok(idx) = idx_str.parse::<usize>() {
                    segments.push(PathSegment::Index(idx));
                }
            }
            _ => {
                current_key.push(c);
                chars.next();
            }
        }
    }

    if !current_key.is_empty() {
        segments.push(PathSegment::Key(current_key));
    }

    segments
}

pub fn register(registry: &mut FunctionRegistry) -> Result<(), RegistryError> {
    // json_extract: json_extract(json_string, json_path) -> String (JSON value) or Null
    registry.register(FunctionDef {
        name: "json_extract".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(json_str), Value::String(path)) => {
                let parsed = parse_json(json_str)?;
                match json_navigate(&parsed, path) {
                    Some(val) => Ok(Value::String(stringify_json(&val)?.into())),
                    None => Ok(Value::Null),
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // json_extract_scalar: json_extract_scalar(json_string, json_path) -> String or Null
    registry.register(FunctionDef {
        name: "json_extract_scalar".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(json_str), Value::String(path)) => {
                let parsed = parse_json(json_str)?;
                match json_navigate(&parsed, path) {
                    Some(val) => {
                        if val.is_object() || val.is_array() || val.is_null() {
                            Ok(Value::Null)
                        } else if val.is_boolean() {
                            Ok(Value::String(val.as_bool().unwrap().to_string().into()))
                        } else if val.is_number() {
                            Ok(Value::String(stringify_json(&val)?.into()))
                        } else if val.is_string() {
                            Ok(Value::String(val.as_str().unwrap().into()))
                        } else {
                            Ok(Value::Null)
                        }
                    }
                    None => Ok(Value::Null),
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // json_array_length: json_array_length(json_string) -> Int or Null
    registry.register(FunctionDef {
        name: "json_array_length".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(json_str) => {
                let parsed = parse_json(json_str)?;
                if parsed.is_array() {
                    Ok(Value::Int(parsed.as_array().unwrap().len() as i32))
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // json_array_contains: json_array_contains(json_string, value) -> Boolean
    registry.register(FunctionDef {
        name: "json_array_contains".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(json_str) => {
                let parsed = parse_json(json_str)?;
                if !parsed.is_array() {
                    return Ok(Value::Boolean(false));
                }
                let search_value = &args[1];
                for member in parsed.as_array().unwrap() {
                    match search_value {
                        Value::String(s) => {
                            if member.is_string() && member.as_str().unwrap() == s.as_str() {
                                return Ok(Value::Boolean(true));
                            }
                        }
                        Value::Int(i) => {
                            if member.is_number() && member.as_i64() == Some(i64::from(*i)) {
                                return Ok(Value::Boolean(true));
                            }
                        }
                        Value::Boolean(b) => {
                            if member.is_boolean() && member.as_bool() == Some(*b) {
                                return Ok(Value::Boolean(true));
                            }
                        }
                        _ => {}
                    }
                }
                Ok(Value::Boolean(false))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // json_size: json_size(json_string, json_path) -> Int or Null
    registry.register(FunctionDef {
        name: "json_size".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(json_str), Value::String(path)) => {
                let parsed = parse_json(json_str)?;
                match json_navigate(&parsed, path) {
                    Some(val) => {
                        if val.is_array() || val.is_object() {
                            let len = val
                                .as_array()
                                .map(Vec::len)
                                .or_else(|| val.as_object().map(serde_json::Map::len))
                                .unwrap_or(0);
                            Ok(Value::Int(len as i32))
                        } else {
                            Ok(Value::Int(0))
                        }
                    }
                    None => Ok(Value::Null),
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // json_format: json_format(value) -> String
    registry.register(FunctionDef {
        name: "json_format".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => Ok(Value::String(s.clone())),
            Value::Int(i) => Ok(Value::String(i.to_string().into())),
            Value::Boolean(b) => Ok(Value::String(b.to_string().into())),
            Value::Float(f) => Ok(Value::String(f.to_string().into())),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // json_parse: json_parse(string) -> String
    registry.register(FunctionDef {
        name: "json_parse".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => {
                // Validate that the string is valid JSON
                parse_json(s)?;
                Ok(Value::String(s.clone()))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // is_json_scalar: is_json_scalar(json_string) -> Boolean
    registry.register(FunctionDef {
        name: "is_json_scalar".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(json_str) => {
                let parsed = parse_json(json_str)?;
                let is_scalar = parsed.is_string() || parsed.is_number() || parsed.is_boolean() || parsed.is_null();
                Ok(Value::Boolean(is_scalar))
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

    fn make_registry() -> FunctionRegistry {
        let mut r = FunctionRegistry::new();
        register(&mut r).unwrap();
        r
    }

    #[test]
    fn test_json_extract() {
        let r = make_registry();
        let json = r#"{"name": "Alice", "age": 30}"#;
        assert_eq!(
            r.call(
                "json_extract",
                &[Value::String(json.into()), Value::String("$.name".into())]
            ),
            Ok(Value::String("\"Alice\"".into()))
        );
    }

    #[test]
    fn test_json_extract_scalar() {
        let r = make_registry();
        let json = r#"{"name": "Alice", "age": 30}"#;
        assert_eq!(
            r.call(
                "json_extract_scalar",
                &[Value::String(json.into()), Value::String("$.name".into())]
            ),
            Ok(Value::String("Alice".into()))
        );
        assert_eq!(
            r.call(
                "json_extract_scalar",
                &[Value::String(json.into()), Value::String("$.age".into())]
            ),
            Ok(Value::String("30".into()))
        );
    }

    #[test]
    fn test_json_extract_nested() {
        let r = make_registry();
        let json = r#"{"store": {"name": "Books R Us"}}"#;
        assert_eq!(
            r.call(
                "json_extract_scalar",
                &[Value::String(json.into()), Value::String("$.store.name".into())]
            ),
            Ok(Value::String("Books R Us".into()))
        );
    }

    #[test]
    fn test_json_extract_array_index() {
        let r = make_registry();
        let json = r#"{"items": [10, 20, 30]}"#;
        assert_eq!(
            r.call(
                "json_extract_scalar",
                &[Value::String(json.into()), Value::String("$.items[1]".into())]
            ),
            Ok(Value::String("20".into()))
        );
    }

    #[test]
    fn test_json_array_length() {
        let r = make_registry();
        assert_eq!(
            r.call("json_array_length", &[Value::String("[1, 2, 3]".into())]),
            Ok(Value::Int(3))
        );
    }

    #[test]
    fn test_json_array_length_not_array() {
        let r = make_registry();
        assert_eq!(
            r.call("json_array_length", &[Value::String(r#"{"a": 1}"#.into())]),
            Ok(Value::Null)
        );
    }

    #[test]
    fn test_json_array_contains() {
        let r = make_registry();
        assert_eq!(
            r.call(
                "json_array_contains",
                &[Value::String("[1, 2, 3]".into()), Value::Int(2)]
            ),
            Ok(Value::Boolean(true))
        );
        assert_eq!(
            r.call(
                "json_array_contains",
                &[Value::String("[1, 2, 3]".into()), Value::Int(5)]
            ),
            Ok(Value::Boolean(false))
        );
    }

    #[test]
    fn test_json_size_array() {
        let r = make_registry();
        assert_eq!(
            r.call(
                "json_size",
                &[Value::String("[1, 2, 3]".into()), Value::String("$".into())]
            ),
            Ok(Value::Int(3))
        );
    }

    #[test]
    fn test_json_size_object() {
        let r = make_registry();
        assert_eq!(
            r.call(
                "json_size",
                &[Value::String(r#"{"a": 1, "b": 2}"#.into()), Value::String("$".into())]
            ),
            Ok(Value::Int(2))
        );
    }

    #[test]
    fn test_json_parse_valid() {
        let r = make_registry();
        assert_eq!(
            r.call("json_parse", &[Value::String(r#"{"a": 1}"#.into())]),
            Ok(Value::String(r#"{"a": 1}"#.into()))
        );
    }

    #[test]
    fn test_json_parse_invalid() {
        let r = make_registry();
        assert!(r.call("json_parse", &[Value::String("not json".into())]).is_err());
    }

    #[test]
    fn test_is_json_scalar() {
        let r = make_registry();
        assert_eq!(
            r.call("is_json_scalar", &[Value::String("42".into())]),
            Ok(Value::Boolean(true))
        );
        assert_eq!(
            r.call("is_json_scalar", &[Value::String(r#""hello""#.into())]),
            Ok(Value::Boolean(true))
        );
        assert_eq!(
            r.call("is_json_scalar", &[Value::String("[1,2]".into())]),
            Ok(Value::Boolean(false))
        );
        assert_eq!(
            r.call("is_json_scalar", &[Value::String(r#"{"a":1}"#.into())]),
            Ok(Value::Boolean(false))
        );
    }

    #[test]
    fn test_json_null_propagation() {
        let r = make_registry();
        assert_eq!(r.call("json_array_length", &[Value::Null]), Ok(Value::Null));
        assert_eq!(r.call("json_array_length", &[Value::Missing]), Ok(Value::Missing));
    }
}
