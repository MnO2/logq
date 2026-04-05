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

    // replace: replace(string, search, replacement) → String
    registry.register(FunctionDef {
        name: "replace".to_string(),
        arity: Arity::Exact(3),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1], &args[2]) {
            (Value::String(s), Value::String(search), Value::String(replacement)) => {
                Ok(Value::String(s.replace(search.as_str(), replacement.as_str())))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // reverse: reverse(string) → String, or reverse(array) → Array
    registry.register(FunctionDef {
        name: "reverse".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => Ok(Value::String(s.chars().rev().collect())),
            Value::Array(a) => {
                let mut reversed = a.clone();
                reversed.reverse();
                Ok(Value::Array(reversed))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // repeat: repeat(string, count) → String
    registry.register(FunctionDef {
        name: "repeat".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(s), Value::Int(n)) => {
                let count = (*n).max(0) as usize;
                Ok(Value::String(s.repeat(count)))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // lpad: lpad(string, size [, pad_string]) → String
    registry.register(FunctionDef {
        name: "lpad".to_string(),
        arity: Arity::Range(2, 3),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => {
                let size = match &args[1] {
                    Value::Int(n) => (*n).max(0) as usize,
                    _ => return Err(ExpressionError::InvalidArguments),
                };
                let pad = if args.len() == 3 {
                    match &args[2] {
                        Value::String(p) => p.clone(),
                        _ => return Err(ExpressionError::InvalidArguments),
                    }
                } else {
                    " ".to_string()
                };
                let char_len = s.chars().count();
                if char_len >= size {
                    // Truncate to size
                    Ok(Value::String(s.chars().take(size).collect()))
                } else {
                    let needed = size - char_len;
                    let pad_chars: Vec<char> = pad.chars().collect();
                    if pad_chars.is_empty() {
                        return Ok(Value::String(s.clone()));
                    }
                    let mut prefix = String::new();
                    for i in 0..needed {
                        prefix.push(pad_chars[i % pad_chars.len()]);
                    }
                    prefix.push_str(s);
                    Ok(Value::String(prefix))
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // rpad: rpad(string, size [, pad_string]) → String
    registry.register(FunctionDef {
        name: "rpad".to_string(),
        arity: Arity::Range(2, 3),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => {
                let size = match &args[1] {
                    Value::Int(n) => (*n).max(0) as usize,
                    _ => return Err(ExpressionError::InvalidArguments),
                };
                let pad = if args.len() == 3 {
                    match &args[2] {
                        Value::String(p) => p.clone(),
                        _ => return Err(ExpressionError::InvalidArguments),
                    }
                } else {
                    " ".to_string()
                };
                let char_len = s.chars().count();
                if char_len >= size {
                    Ok(Value::String(s.chars().take(size).collect()))
                } else {
                    let needed = size - char_len;
                    let pad_chars: Vec<char> = pad.chars().collect();
                    if pad_chars.is_empty() {
                        return Ok(Value::String(s.clone()));
                    }
                    let mut result = s.clone();
                    for i in 0..needed {
                        result.push(pad_chars[i % pad_chars.len()]);
                    }
                    Ok(Value::String(result))
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // ltrim: trim leading whitespace
    registry.register(FunctionDef {
        name: "ltrim".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => Ok(Value::String(s.trim_start().to_string())),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // rtrim: trim trailing whitespace
    registry.register(FunctionDef {
        name: "rtrim".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => Ok(Value::String(s.trim_end().to_string())),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // position / strpos: position(string, substring) → Int (1-based, 0 if not found)
    registry.register(FunctionDef {
        name: "position".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(s), Value::String(sub)) => {
                match s.find(sub.as_str()) {
                    Some(byte_pos) => {
                        // Convert byte position to 1-based character position
                        let char_pos = s[..byte_pos].chars().count() + 1;
                        Ok(Value::Int(char_pos as i32))
                    }
                    None => Ok(Value::Int(0)),
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // strpos: alias for position (same arg order: string, substring)
    registry.register(FunctionDef {
        name: "strpos".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(s), Value::String(sub)) => {
                match s.find(sub.as_str()) {
                    Some(byte_pos) => {
                        let char_pos = s[..byte_pos].chars().count() + 1;
                        Ok(Value::Int(char_pos as i32))
                    }
                    None => Ok(Value::Int(0)),
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // starts_with: starts_with(string, prefix) → Boolean
    registry.register(FunctionDef {
        name: "starts_with".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(s), Value::String(prefix)) => {
                Ok(Value::Boolean(s.starts_with(prefix.as_str())))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // ends_with: ends_with(string, suffix) → Boolean
    registry.register(FunctionDef {
        name: "ends_with".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(s), Value::String(suffix)) => {
                Ok(Value::Boolean(s.ends_with(suffix.as_str())))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // split: split(string, delimiter) → Array of Strings
    registry.register(FunctionDef {
        name: "split".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(s), Value::String(delim)) => {
                let parts: Vec<Value> = s.split(delim.as_str())
                    .map(|part| Value::String(part.to_string()))
                    .collect();
                Ok(Value::Array(parts))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // split_part: split_part(string, delimiter, index) → String (1-based)
    registry.register(FunctionDef {
        name: "split_part".to_string(),
        arity: Arity::Exact(3),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1], &args[2]) {
            (Value::String(s), Value::String(delim), Value::Int(idx)) => {
                let parts: Vec<&str> = s.split(delim.as_str()).collect();
                let index = (*idx - 1) as usize; // 1-based to 0-based
                if index < parts.len() {
                    Ok(Value::String(parts[index].to_string()))
                } else {
                    Ok(Value::String(String::new()))
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // concat_ws: concat_ws(separator, str1, str2, ...) → String
    // Uses Custom null handling to skip null values but propagate null separator
    registry.register(FunctionDef {
        name: "concat_ws".to_string(),
        arity: Arity::Variadic(2),
        null_handling: NullHandling::Custom,
        func: Box::new(|args| {
            // If separator is Missing, return Missing
            if matches!(&args[0], Value::Missing) {
                return Ok(Value::Missing);
            }
            // If separator is Null, return Null
            let sep = match &args[0] {
                Value::String(s) => s.clone(),
                Value::Null => return Ok(Value::Null),
                _ => return Err(ExpressionError::InvalidArguments),
            };
            let mut parts: Vec<String> = Vec::new();
            for arg in &args[1..] {
                match arg {
                    Value::String(s) => parts.push(s.clone()),
                    Value::Null | Value::Missing => continue, // skip nulls/missing
                    _ => return Err(ExpressionError::InvalidArguments),
                }
            }
            Ok(Value::String(parts.join(&sep)))
        }),
    })?;

    // chr: chr(code_point) → String
    registry.register(FunctionDef {
        name: "chr".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Int(cp) => {
                match char::from_u32(*cp as u32) {
                    Some(c) => Ok(Value::String(c.to_string())),
                    None => Err(ExpressionError::InvalidArguments),
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // codepoint: codepoint(char) → Int
    registry.register(FunctionDef {
        name: "codepoint".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::String(s) => {
                let chars: Vec<char> = s.chars().collect();
                if chars.len() != 1 {
                    return Err(ExpressionError::InvalidArguments);
                }
                Ok(Value::Int(chars[0] as i32))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // levenshtein_distance: levenshtein_distance(s1, s2) → Int
    registry.register(FunctionDef {
        name: "levenshtein_distance".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(s1), Value::String(s2)) => {
                let a: Vec<char> = s1.chars().collect();
                let b: Vec<char> = s2.chars().collect();
                let m = a.len();
                let n = b.len();
                let mut dp = vec![vec![0usize; n + 1]; m + 1];
                for i in 0..=m {
                    dp[i][0] = i;
                }
                for j in 0..=n {
                    dp[0][j] = j;
                }
                for i in 1..=m {
                    for j in 1..=n {
                        let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
                        dp[i][j] = (dp[i - 1][j] + 1)
                            .min(dp[i][j - 1] + 1)
                            .min(dp[i - 1][j - 1] + cost);
                    }
                }
                Ok(Value::Int(dp[m][n] as i32))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // hamming_distance: hamming_distance(s1, s2) → Int
    registry.register(FunctionDef {
        name: "hamming_distance".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(s1), Value::String(s2)) => {
                let a: Vec<char> = s1.chars().collect();
                let b: Vec<char> = s2.chars().collect();
                if a.len() != b.len() {
                    return Err(ExpressionError::InvalidArguments);
                }
                let dist = a.iter().zip(b.iter()).filter(|(c1, c2)| c1 != c2).count();
                Ok(Value::Int(dist as i32))
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

    #[test]
    fn test_replace() {
        let r = make_registry();
        assert_eq!(
            r.call("replace", &[Value::String("hello world".into()), Value::String("world".into()), Value::String("rust".into())]),
            Ok(Value::String("hello rust".into()))
        );
    }

    #[test]
    fn test_reverse() {
        let r = make_registry();
        assert_eq!(r.call("reverse", &[Value::String("hello".into())]), Ok(Value::String("olleh".into())));
    }

    #[test]
    fn test_reverse_array() {
        let r = make_registry();
        assert_eq!(
            r.call("reverse", &[Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])]),
            Ok(Value::Array(vec![Value::Int(3), Value::Int(2), Value::Int(1)]))
        );
    }

    #[test]
    fn test_repeat() {
        let r = make_registry();
        assert_eq!(r.call("repeat", &[Value::String("ab".into()), Value::Int(3)]), Ok(Value::String("ababab".into())));
    }

    #[test]
    fn test_lpad() {
        let r = make_registry();
        assert_eq!(r.call("lpad", &[Value::String("hi".into()), Value::Int(5)]), Ok(Value::String("   hi".into())));
        assert_eq!(
            r.call("lpad", &[Value::String("hi".into()), Value::Int(5), Value::String("xy".into())]),
            Ok(Value::String("xyxhi".into()))
        );
    }

    #[test]
    fn test_lpad_truncate() {
        let r = make_registry();
        assert_eq!(r.call("lpad", &[Value::String("hello".into()), Value::Int(3)]), Ok(Value::String("hel".into())));
    }

    #[test]
    fn test_rpad() {
        let r = make_registry();
        assert_eq!(r.call("rpad", &[Value::String("hi".into()), Value::Int(5)]), Ok(Value::String("hi   ".into())));
    }

    #[test]
    fn test_rpad_truncate() {
        let r = make_registry();
        assert_eq!(r.call("rpad", &[Value::String("hello".into()), Value::Int(3)]), Ok(Value::String("hel".into())));
    }

    #[test]
    fn test_ltrim() {
        let r = make_registry();
        assert_eq!(r.call("ltrim", &[Value::String("  hello  ".into())]), Ok(Value::String("hello  ".into())));
    }

    #[test]
    fn test_rtrim() {
        let r = make_registry();
        assert_eq!(r.call("rtrim", &[Value::String("  hello  ".into())]), Ok(Value::String("  hello".into())));
    }

    #[test]
    fn test_position() {
        let r = make_registry();
        assert_eq!(r.call("position", &[Value::String("hello world".into()), Value::String("world".into())]), Ok(Value::Int(7)));
        assert_eq!(r.call("position", &[Value::String("hello".into()), Value::String("xyz".into())]), Ok(Value::Int(0)));
    }

    #[test]
    fn test_strpos() {
        let r = make_registry();
        assert_eq!(r.call("strpos", &[Value::String("hello world".into()), Value::String("world".into())]), Ok(Value::Int(7)));
    }

    #[test]
    fn test_starts_with() {
        let r = make_registry();
        assert_eq!(r.call("starts_with", &[Value::String("hello".into()), Value::String("hel".into())]), Ok(Value::Boolean(true)));
        assert_eq!(r.call("starts_with", &[Value::String("hello".into()), Value::String("xyz".into())]), Ok(Value::Boolean(false)));
    }

    #[test]
    fn test_ends_with() {
        let r = make_registry();
        assert_eq!(r.call("ends_with", &[Value::String("hello".into()), Value::String("llo".into())]), Ok(Value::Boolean(true)));
        assert_eq!(r.call("ends_with", &[Value::String("hello".into()), Value::String("xyz".into())]), Ok(Value::Boolean(false)));
    }

    #[test]
    fn test_split() {
        let r = make_registry();
        assert_eq!(
            r.call("split", &[Value::String("a,b,c".into()), Value::String(",".into())]),
            Ok(Value::Array(vec![Value::String("a".into()), Value::String("b".into()), Value::String("c".into())]))
        );
    }

    #[test]
    fn test_split_part() {
        let r = make_registry();
        assert_eq!(
            r.call("split_part", &[Value::String("a,b,c".into()), Value::String(",".into()), Value::Int(2)]),
            Ok(Value::String("b".into()))
        );
    }

    #[test]
    fn test_split_part_out_of_bounds() {
        let r = make_registry();
        assert_eq!(
            r.call("split_part", &[Value::String("a,b,c".into()), Value::String(",".into()), Value::Int(10)]),
            Ok(Value::String("".into()))
        );
    }

    #[test]
    fn test_concat_ws() {
        let r = make_registry();
        assert_eq!(
            r.call("concat_ws", &[Value::String(",".into()), Value::String("a".into()), Value::String("b".into()), Value::String("c".into())]),
            Ok(Value::String("a,b,c".into()))
        );
    }

    #[test]
    fn test_concat_ws_skip_nulls() {
        let r = make_registry();
        assert_eq!(
            r.call("concat_ws", &[Value::String(",".into()), Value::String("a".into()), Value::Null, Value::String("c".into())]),
            Ok(Value::String("a,c".into()))
        );
    }

    #[test]
    fn test_concat_ws_null_separator() {
        let r = make_registry();
        assert_eq!(
            r.call("concat_ws", &[Value::Null, Value::String("a".into()), Value::String("b".into())]),
            Ok(Value::Null)
        );
    }

    #[test]
    fn test_chr() {
        let r = make_registry();
        assert_eq!(r.call("chr", &[Value::Int(65)]), Ok(Value::String("A".into())));
    }

    #[test]
    fn test_codepoint() {
        let r = make_registry();
        assert_eq!(r.call("codepoint", &[Value::String("A".into())]), Ok(Value::Int(65)));
    }

    #[test]
    fn test_codepoint_error_multi_char() {
        let r = make_registry();
        assert!(r.call("codepoint", &[Value::String("AB".into())]).is_err());
    }

    #[test]
    fn test_levenshtein_distance() {
        let r = make_registry();
        assert_eq!(r.call("levenshtein_distance", &[Value::String("kitten".into()), Value::String("sitting".into())]), Ok(Value::Int(3)));
    }

    #[test]
    fn test_levenshtein_distance_same() {
        let r = make_registry();
        assert_eq!(r.call("levenshtein_distance", &[Value::String("abc".into()), Value::String("abc".into())]), Ok(Value::Int(0)));
    }

    #[test]
    fn test_hamming_distance() {
        let r = make_registry();
        assert_eq!(r.call("hamming_distance", &[Value::String("abc".into()), Value::String("axc".into())]), Ok(Value::Int(1)));
    }

    #[test]
    fn test_hamming_distance_different_lengths() {
        let r = make_registry();
        assert!(r.call("hamming_distance", &[Value::String("abc".into()), Value::String("ab".into())]).is_err());
    }
}
