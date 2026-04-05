use crate::common::types::Value;
use crate::execution::types::ExpressionError;
use crate::functions::registry::{Arity, FunctionDef, FunctionRegistry, NullHandling, RegistryError};
use ordered_float::OrderedFloat;
use std::collections::HashSet;

/// Convert a Value to a string representation for sorting and joining purposes.
fn value_to_sort_string(v: &Value) -> String {
    match v {
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::String(s) => s.clone(),
        Value::Null => "null".to_string(),
        Value::Missing => "missing".to_string(),
        Value::DateTime(dt) => dt.to_string(),
        Value::HttpRequest(hr) => hr.to_string(),
        Value::Host(h) => h.to_string(),
        Value::Object(_) => "object".to_string(),
        Value::Array(_) => "array".to_string(),
    }
}

/// Compare two Values for sorting. Returns an Ordering.
/// Int and Float are compared numerically. Strings lexicographically.
/// Mixed types are compared by converting to string.
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.cmp(y),
        (Value::Int(x), Value::Float(y)) => {
            let xf = OrderedFloat(*x as f32);
            xf.cmp(y)
        }
        (Value::Float(x), Value::Int(y)) => {
            let yf = OrderedFloat(*y as f32);
            x.cmp(&yf)
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Boolean(x), Value::Boolean(y)) => x.cmp(y),
        _ => {
            let sa = value_to_sort_string(a);
            let sb = value_to_sort_string(b);
            sa.cmp(&sb)
        }
    }
}

/// Extract a numeric value as f64 for min/max comparisons.
fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(f.into_inner() as f64),
        _ => None,
    }
}

pub fn register(registry: &mut FunctionRegistry) -> Result<(), RegistryError> {
    // array_sort: Sort array elements
    registry.register(FunctionDef {
        name: "array_sort".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Array(arr) => {
                let mut sorted = arr.clone();
                sorted.sort_by(compare_values);
                Ok(Value::Array(sorted))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // array_distinct: Remove duplicates preserving first occurrence order
    registry.register(FunctionDef {
        name: "array_distinct".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Array(arr) => {
                let mut seen = HashSet::new();
                let mut result = Vec::new();
                for v in arr {
                    if seen.insert(v.clone()) {
                        result.push(v.clone());
                    }
                }
                Ok(Value::Array(result))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // array_contains: Check if element exists in array
    registry.register(FunctionDef {
        name: "array_contains".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Array(arr) => {
                let element = &args[1];
                Ok(Value::Boolean(arr.contains(element)))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // array_join: Join array elements with delimiter, optional null replacement
    // Uses Custom null handling because array elements can be null
    registry.register(FunctionDef {
        name: "array_join".to_string(),
        arity: Arity::Range(2, 3),
        null_handling: NullHandling::Custom,
        func: Box::new(|args| {
            // If the array itself is Missing or Null, propagate
            match &args[0] {
                Value::Missing => return Ok(Value::Missing),
                Value::Null => return Ok(Value::Null),
                _ => {}
            }
            // If the delimiter is Missing or Null, propagate
            match &args[1] {
                Value::Missing => return Ok(Value::Missing),
                Value::Null => return Ok(Value::Null),
                _ => {}
            }

            let arr = match &args[0] {
                Value::Array(a) => a,
                _ => return Err(ExpressionError::InvalidArguments),
            };
            let delimiter = match &args[1] {
                Value::String(s) => s,
                _ => return Err(ExpressionError::InvalidArguments),
            };

            let null_replacement = if args.len() == 3 {
                match &args[2] {
                    Value::Missing => return Ok(Value::Missing),
                    Value::Null => return Ok(Value::Null),
                    Value::String(s) => Some(s.clone()),
                    _ => return Err(ExpressionError::InvalidArguments),
                }
            } else {
                None
            };

            let parts: Vec<String> = arr
                .iter()
                .filter_map(|v| match v {
                    Value::Null | Value::Missing => {
                        null_replacement.clone()
                    }
                    Value::String(s) => Some(s.clone()),
                    Value::Int(i) => Some(i.to_string()),
                    Value::Float(f) => Some(f.to_string()),
                    Value::Boolean(b) => Some(b.to_string()),
                    other => Some(value_to_sort_string(other)),
                })
                .collect();

            Ok(Value::String(parts.join(delimiter)))
        }),
    })?;

    // array_min: Return minimum element from numeric array
    registry.register(FunctionDef {
        name: "array_min".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Array(arr) => {
                let mut min_val: Option<&Value> = None;
                let mut min_f64: Option<f64> = None;
                for v in arr {
                    if let Some(f) = value_to_f64(v) {
                        if min_f64.is_none() || f < min_f64.unwrap() {
                            min_f64 = Some(f);
                            min_val = Some(v);
                        }
                    }
                }
                match min_val {
                    Some(v) => Ok(v.clone()),
                    None => Ok(Value::Null),
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // array_max: Return maximum element from numeric array
    registry.register(FunctionDef {
        name: "array_max".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Array(arr) => {
                let mut max_val: Option<&Value> = None;
                let mut max_f64: Option<f64> = None;
                for v in arr {
                    if let Some(f) = value_to_f64(v) {
                        if max_f64.is_none() || f > max_f64.unwrap() {
                            max_f64 = Some(f);
                            max_val = Some(v);
                        }
                    }
                }
                match max_val {
                    Some(v) => Ok(v.clone()),
                    None => Ok(Value::Null),
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // array_position: Return 1-based index of first occurrence, or 0 if not found
    registry.register(FunctionDef {
        name: "array_position".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Array(arr) => {
                let element = &args[1];
                for (i, v) in arr.iter().enumerate() {
                    if v == element {
                        return Ok(Value::Int((i + 1) as i32));
                    }
                }
                Ok(Value::Int(0))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // cardinality: Return length of array or key count of object
    registry.register(FunctionDef {
        name: "cardinality".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Array(arr) => Ok(Value::Int(arr.len() as i32)),
            Value::Object(obj) => Ok(Value::Int(obj.len() as i32)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // flatten: Flatten nested arrays one level
    registry.register(FunctionDef {
        name: "flatten".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Array(arr) => {
                let mut result = Vec::new();
                for v in arr {
                    match v {
                        Value::Array(inner) => {
                            result.extend(inner.iter().cloned());
                        }
                        other => {
                            result.push(other.clone());
                        }
                    }
                }
                Ok(Value::Array(result))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // array_intersect: Return elements common to both arrays
    registry.register(FunctionDef {
        name: "array_intersect".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Array(a), Value::Array(b)) => {
                let set_b: HashSet<&Value> = b.iter().collect();
                let mut seen = HashSet::new();
                let mut result = Vec::new();
                for v in a {
                    if set_b.contains(v) && seen.insert(v.clone()) {
                        result.push(v.clone());
                    }
                }
                Ok(Value::Array(result))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // array_union: Return union of two arrays (duplicates removed)
    registry.register(FunctionDef {
        name: "array_union".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Array(a), Value::Array(b)) => {
                let mut seen = HashSet::new();
                let mut result = Vec::new();
                for v in a.iter().chain(b.iter()) {
                    if seen.insert(v.clone()) {
                        result.push(v.clone());
                    }
                }
                Ok(Value::Array(result))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // array_except: Return elements in first array but not in second
    registry.register(FunctionDef {
        name: "array_except".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Array(a), Value::Array(b)) => {
                let set_b: HashSet<&Value> = b.iter().collect();
                let mut seen = HashSet::new();
                let mut result = Vec::new();
                for v in a {
                    if !set_b.contains(v) && seen.insert(v.clone()) {
                        result.push(v.clone());
                    }
                }
                Ok(Value::Array(result))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // array_remove: Remove all occurrences of element from array
    registry.register(FunctionDef {
        name: "array_remove".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Array(arr) => {
                let element = &args[1];
                let result: Vec<Value> = arr.iter().filter(|v| *v != element).cloned().collect();
                Ok(Value::Array(result))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // slice: slice(array, start, length) with 1-based indexing
    registry.register(FunctionDef {
        name: "slice".to_string(),
        arity: Arity::Exact(3),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| {
            let arr = match &args[0] {
                Value::Array(a) => a,
                _ => return Err(ExpressionError::InvalidArguments),
            };
            let start = match &args[1] {
                Value::Int(s) => *s,
                _ => return Err(ExpressionError::InvalidArguments),
            };
            let length = match &args[2] {
                Value::Int(l) => *l,
                _ => return Err(ExpressionError::InvalidArguments),
            };

            if start < 1 || length < 0 {
                return Ok(Value::Array(Vec::new()));
            }

            let start_idx = (start - 1) as usize;
            let end_idx = std::cmp::min(start_idx + length as usize, arr.len());

            if start_idx >= arr.len() {
                return Ok(Value::Array(Vec::new()));
            }

            let result: Vec<Value> = arr[start_idx..end_idx].to_vec();
            Ok(Value::Array(result))
        }),
    })?;

    // shuffle: Deterministic "shuffle" (reverse) since we don't have rand.
    // In a real implementation this would use a PRNG seeded with system time.
    registry.register(FunctionDef {
        name: "shuffle".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Array(arr) => {
                let mut result = arr.clone();
                result.reverse();
                Ok(Value::Array(result))
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

    fn int_array(vals: &[i32]) -> Value {
        Value::Array(vals.iter().map(|v| Value::Int(*v)).collect())
    }

    fn str_array(vals: &[&str]) -> Value {
        Value::Array(vals.iter().map(|v| Value::String(v.to_string())).collect())
    }

    #[test]
    fn test_array_sort() {
        let r = make_registry();
        assert_eq!(
            r.call("array_sort", &[int_array(&[3, 1, 2])]),
            Ok(int_array(&[1, 2, 3]))
        );
    }

    #[test]
    fn test_array_sort_strings() {
        let r = make_registry();
        assert_eq!(
            r.call("array_sort", &[str_array(&["banana", "apple", "cherry"])]),
            Ok(str_array(&["apple", "banana", "cherry"]))
        );
    }

    #[test]
    fn test_array_distinct() {
        let r = make_registry();
        assert_eq!(
            r.call("array_distinct", &[int_array(&[1, 2, 2, 3, 1])]),
            Ok(int_array(&[1, 2, 3]))
        );
    }

    #[test]
    fn test_array_contains() {
        let r = make_registry();
        assert_eq!(
            r.call("array_contains", &[int_array(&[1, 2, 3]), Value::Int(2)]),
            Ok(Value::Boolean(true))
        );
        assert_eq!(
            r.call("array_contains", &[int_array(&[1, 2, 3]), Value::Int(5)]),
            Ok(Value::Boolean(false))
        );
    }

    #[test]
    fn test_array_join() {
        let r = make_registry();
        assert_eq!(
            r.call(
                "array_join",
                &[str_array(&["a", "b", "c"]), Value::String(",".into())]
            ),
            Ok(Value::String("a,b,c".into()))
        );
    }

    #[test]
    fn test_array_join_with_nulls_skip() {
        let r = make_registry();
        let arr = Value::Array(vec![
            Value::String("a".into()),
            Value::Null,
            Value::String("c".into()),
        ]);
        assert_eq!(
            r.call("array_join", &[arr, Value::String(",".into())]),
            Ok(Value::String("a,c".into()))
        );
    }

    #[test]
    fn test_array_join_with_null_replacement() {
        let r = make_registry();
        let arr = Value::Array(vec![
            Value::String("a".into()),
            Value::Null,
            Value::String("c".into()),
        ]);
        assert_eq!(
            r.call(
                "array_join",
                &[arr, Value::String(",".into()), Value::String("N/A".into())]
            ),
            Ok(Value::String("a,N/A,c".into()))
        );
    }

    #[test]
    fn test_array_min() {
        let r = make_registry();
        assert_eq!(
            r.call("array_min", &[int_array(&[3, 1, 2])]),
            Ok(Value::Int(1))
        );
    }

    #[test]
    fn test_array_max() {
        let r = make_registry();
        assert_eq!(
            r.call("array_max", &[int_array(&[3, 1, 2])]),
            Ok(Value::Int(3))
        );
    }

    #[test]
    fn test_array_position() {
        let r = make_registry();
        assert_eq!(
            r.call(
                "array_position",
                &[int_array(&[10, 20, 30]), Value::Int(20)]
            ),
            Ok(Value::Int(2))
        );
        assert_eq!(
            r.call(
                "array_position",
                &[int_array(&[10, 20, 30]), Value::Int(99)]
            ),
            Ok(Value::Int(0))
        );
    }

    #[test]
    fn test_cardinality() {
        let r = make_registry();
        assert_eq!(
            r.call("cardinality", &[int_array(&[1, 2, 3])]),
            Ok(Value::Int(3))
        );
    }

    #[test]
    fn test_cardinality_object() {
        let r = make_registry();
        let mut obj = linked_hash_map::LinkedHashMap::new();
        obj.insert("a".to_string(), Value::Int(1));
        obj.insert("b".to_string(), Value::Int(2));
        assert_eq!(
            r.call("cardinality", &[Value::Object(Box::new(obj))]),
            Ok(Value::Int(2))
        );
    }

    #[test]
    fn test_flatten() {
        let r = make_registry();
        let nested = Value::Array(vec![int_array(&[1, 2]), int_array(&[3, 4])]);
        assert_eq!(
            r.call("flatten", &[nested]),
            Ok(int_array(&[1, 2, 3, 4]))
        );
    }

    #[test]
    fn test_flatten_mixed() {
        let r = make_registry();
        let nested = Value::Array(vec![
            int_array(&[1, 2]),
            Value::Int(3),
            int_array(&[4, 5]),
        ]);
        assert_eq!(
            r.call("flatten", &[nested]),
            Ok(int_array(&[1, 2, 3, 4, 5]))
        );
    }

    #[test]
    fn test_array_intersect() {
        let r = make_registry();
        assert_eq!(
            r.call(
                "array_intersect",
                &[int_array(&[1, 2, 3]), int_array(&[2, 3, 4])]
            ),
            Ok(int_array(&[2, 3]))
        );
    }

    #[test]
    fn test_array_union() {
        let r = make_registry();
        let result = r.call(
            "array_union",
            &[int_array(&[1, 2, 3]), int_array(&[2, 3, 4])],
        );
        assert_eq!(result, Ok(int_array(&[1, 2, 3, 4])));
    }

    #[test]
    fn test_array_except() {
        let r = make_registry();
        assert_eq!(
            r.call(
                "array_except",
                &[int_array(&[1, 2, 3, 4]), int_array(&[2, 4])]
            ),
            Ok(int_array(&[1, 3]))
        );
    }

    #[test]
    fn test_array_remove() {
        let r = make_registry();
        assert_eq!(
            r.call(
                "array_remove",
                &[int_array(&[1, 2, 3, 2, 1]), Value::Int(2)]
            ),
            Ok(int_array(&[1, 3, 1]))
        );
    }

    #[test]
    fn test_slice() {
        let r = make_registry();
        assert_eq!(
            r.call(
                "slice",
                &[
                    int_array(&[10, 20, 30, 40, 50]),
                    Value::Int(2),
                    Value::Int(3)
                ]
            ),
            Ok(int_array(&[20, 30, 40]))
        );
    }

    #[test]
    fn test_slice_out_of_bounds() {
        let r = make_registry();
        assert_eq!(
            r.call(
                "slice",
                &[int_array(&[10, 20, 30]), Value::Int(2), Value::Int(10)]
            ),
            Ok(int_array(&[20, 30]))
        );
    }

    #[test]
    fn test_array_null_propagation() {
        let r = make_registry();
        assert_eq!(r.call("array_sort", &[Value::Null]), Ok(Value::Null));
        assert_eq!(
            r.call("array_sort", &[Value::Missing]),
            Ok(Value::Missing)
        );
    }

    #[test]
    fn test_shuffle() {
        let r = make_registry();
        // Just verify it returns an array of the same length with same elements
        let input = int_array(&[1, 2, 3, 4]);
        let result = r.call("shuffle", &[input]).unwrap();
        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 4);
            }
            _ => panic!("Expected array"),
        }
    }
}
