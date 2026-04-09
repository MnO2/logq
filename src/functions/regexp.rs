use std::cell::RefCell;
use std::num::NonZeroUsize;
use lru::LruCache;
use regex::Regex;

use crate::common::types::Value;
use crate::execution::types::ExpressionError;
use crate::functions::registry::{FunctionDef, FunctionRegistry, Arity, NullHandling, RegistryError};

thread_local! {
    static REGEX_CACHE: RefCell<LruCache<String, Regex>> = RefCell::new(
        LruCache::new(NonZeroUsize::new(64).unwrap())
    );
}

fn get_or_compile_regex(pattern: &str) -> Result<Regex, ExpressionError> {
    REGEX_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(re) = cache.get(pattern) {
            return Ok(re.clone());
        }
        let re = Regex::new(pattern).map_err(|_| ExpressionError::InvalidArguments)?;
        cache.put(pattern.to_string(), re.clone());
        Ok(re)
    })
}

pub fn register(registry: &mut FunctionRegistry) -> Result<(), RegistryError> {
    // regexp_like: regexp_like(string, pattern) -> Boolean
    registry.register(FunctionDef {
        name: "regexp_like".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(s), Value::String(pattern)) => {
                let re = get_or_compile_regex(pattern)?;
                Ok(Value::Boolean(re.is_match(s)))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // regexp_extract: regexp_extract(string, pattern [, group_index]) -> String or Null
    registry.register(FunctionDef {
        name: "regexp_extract".to_string(),
        arity: Arity::Range(2, 3),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(s), Value::String(pattern)) => {
                let re = get_or_compile_regex(pattern)?;
                let group_index = if args.len() == 3 {
                    match &args[2] {
                        Value::Int(i) => *i as usize,
                        _ => return Err(ExpressionError::InvalidArguments),
                    }
                } else {
                    0
                };
                match re.captures(s) {
                    Some(caps) => match caps.get(group_index) {
                        Some(m) => Ok(Value::String(m.as_str().into())),
                        None => Ok(Value::Null),
                    },
                    None => Ok(Value::Null),
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // regexp_extract_all: regexp_extract_all(string, pattern) -> Array of Strings
    registry.register(FunctionDef {
        name: "regexp_extract_all".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(s), Value::String(pattern)) => {
                let re = get_or_compile_regex(pattern)?;
                let matches: Vec<Value> = re.find_iter(s)
                    .map(|m| Value::String(m.as_str().into()))
                    .collect();
                Ok(Value::Array(matches))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // regexp_replace: regexp_replace(string, pattern, replacement) -> String
    registry.register(FunctionDef {
        name: "regexp_replace".to_string(),
        arity: Arity::Exact(3),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1], &args[2]) {
            (Value::String(s), Value::String(pattern), Value::String(replacement)) => {
                let re = get_or_compile_regex(pattern)?;
                Ok(Value::String(re.replace_all(s, replacement.as_str()).into_owned().into()))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // regexp_split: regexp_split(string, pattern) -> Array of Strings
    registry.register(FunctionDef {
        name: "regexp_split".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(s), Value::String(pattern)) => {
                let re = get_or_compile_regex(pattern)?;
                let parts: Vec<Value> = re.split(s)
                    .map(|p| Value::String(p.into()))
                    .collect();
                Ok(Value::Array(parts))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // regexp_count: regexp_count(string, pattern) -> Int
    registry.register(FunctionDef {
        name: "regexp_count".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(s), Value::String(pattern)) => {
                let re = get_or_compile_regex(pattern)?;
                Ok(Value::Int(re.find_iter(s).count() as i32))
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
    fn test_regexp_like() {
        let r = make_registry();
        assert_eq!(
            r.call("regexp_like", &[Value::String("hello123".into()), Value::String("\\d+".into())]),
            Ok(Value::Boolean(true))
        );
        assert_eq!(
            r.call("regexp_like", &[Value::String("hello".into()), Value::String("\\d+".into())]),
            Ok(Value::Boolean(false))
        );
    }

    #[test]
    fn test_regexp_extract() {
        let r = make_registry();
        assert_eq!(
            r.call("regexp_extract", &[Value::String("hello123world".into()), Value::String("(\\d+)".into())]),
            Ok(Value::String("123".into()))
        );
    }

    #[test]
    fn test_regexp_extract_with_group() {
        let r = make_registry();
        assert_eq!(
            r.call("regexp_extract", &[Value::String("2023-01-15".into()), Value::String("(\\d{4})-(\\d{2})-(\\d{2})".into()), Value::Int(2)]),
            Ok(Value::String("01".into()))
        );
    }

    #[test]
    fn test_regexp_extract_no_match() {
        let r = make_registry();
        assert_eq!(
            r.call("regexp_extract", &[Value::String("hello".into()), Value::String("\\d+".into())]),
            Ok(Value::Null)
        );
    }

    #[test]
    fn test_regexp_extract_all() {
        let r = make_registry();
        assert_eq!(
            r.call("regexp_extract_all", &[Value::String("a1b2c3".into()), Value::String("\\d".into())]),
            Ok(Value::Array(vec![Value::String("1".into()), Value::String("2".into()), Value::String("3".into())]))
        );
    }

    #[test]
    fn test_regexp_replace() {
        let r = make_registry();
        assert_eq!(
            r.call("regexp_replace", &[Value::String("hello 123 world 456".into()), Value::String("\\d+".into()), Value::String("NUM".into())]),
            Ok(Value::String("hello NUM world NUM".into()))
        );
    }

    #[test]
    fn test_regexp_split() {
        let r = make_registry();
        assert_eq!(
            r.call("regexp_split", &[Value::String("a1b2c3d".into()), Value::String("\\d".into())]),
            Ok(Value::Array(vec![Value::String("a".into()), Value::String("b".into()), Value::String("c".into()), Value::String("d".into())]))
        );
    }

    #[test]
    fn test_regexp_count() {
        let r = make_registry();
        assert_eq!(
            r.call("regexp_count", &[Value::String("a1b2c3".into()), Value::String("\\d".into())]),
            Ok(Value::Int(3))
        );
    }

    #[test]
    fn test_regexp_null_propagation() {
        let r = make_registry();
        assert_eq!(r.call("regexp_like", &[Value::Null, Value::String("\\d".into())]), Ok(Value::Null));
        assert_eq!(r.call("regexp_like", &[Value::Missing, Value::String("\\d".into())]), Ok(Value::Missing));
    }
}
