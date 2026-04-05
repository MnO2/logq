use crate::common;
use crate::functions::FunctionRegistry;
use crate::syntax::ast;
use chrono;
use linked_hash_map::LinkedHashMap;
use ordered_float::OrderedFloat;
use std::fmt;
use std::path::PathBuf;
use std::result;
use std::sync::Arc;
use url;

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum Value {
    Int(i32),
    Float(OrderedFloat<f32>),
    Boolean(bool),
    String(String),
    Null,
    DateTime(chrono::DateTime<chrono::offset::FixedOffset>),
    HttpRequest(Box<common::types::HttpRequest>),
    Host(Box<common::types::Host>),
    Missing,
    Object(Box<LinkedHashMap<String, Value>>),
    Array(Vec<Value>),
}

pub(crate) type ParseHostResult<T> = result::Result<T, ParseHostError>;

#[derive(thiserror::Error, Debug)]
pub enum ParseHostError {
    #[error("Parse Host Error")]
    ParseHost,
    #[error("{0}")]
    ParsePort(#[from] std::num::ParseIntError),
}

pub(crate) type Hostname = String;
pub(crate) type Port = u16;

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct Host {
    pub hostname: Hostname,
    pub port: Port,
}

impl fmt::Display for Host {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(&*self.hostname)?;
        fmt.write_str(":")?;
        fmt.write_str(&*self.port.to_string())?;
        Ok(())
    }
}

pub(crate) fn parse_host(s: &str) -> ParseHostResult<Host> {
    if let Some((hostname_str, port_str)) = s.rsplit_once(':') {
        let port: u16 = port_str.parse::<u16>()?;
        let host = Host { hostname: hostname_str.to_string(), port };
        Ok(host)
    } else {
        Err(ParseHostError::ParseHost)
    }
}

pub(crate) type ParseHttpRequestResult<T> = result::Result<T, ParseHttpRequestError>;

#[derive(thiserror::Error, Debug)]
pub enum ParseHttpRequestError {
    #[error("Parse Http Method Error")]
    ParseHttpMethod,
    #[error("{0}")]
    ParseUrl(#[from] url::ParseError),
    #[error("Parse Http Version Error")]
    ParseHttpVersion,
    #[error("Missing Field")]
    MissingField,
}

pub(crate) type HttpMethod = &'static str;
pub(crate) type HttpVersion = &'static str;

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct HttpRequest {
    pub http_method: &'static str,
    pub url_raw: String,
    pub http_version: &'static str,
}

impl HttpRequest {
    /// Parse the URL on demand. Only needed for url_* functions.
    pub fn parsed_url(&self) -> Result<url::Url, url::ParseError> {
        url::Url::parse(&self.url_raw)
    }
}

impl fmt::Display for HttpRequest {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(self.http_method)?;
        fmt.write_str(" ")?;
        fmt.write_str(&self.url_raw)?;
        fmt.write_str(" ")?;
        fmt.write_str(self.http_version)?;
        Ok(())
    }
}

pub(crate) fn parse_http_method(s: &str) -> ParseHttpRequestResult<HttpMethod> {
    match s {
        "GET" => Ok("GET"),
        "POST" => Ok("POST"),
        "DELETE" => Ok("DELETE"),
        "HEAD" => Ok("HEAD"),
        "PUT" => Ok("PUT"),
        "PATCH" => Ok("PATCH"),
        _ => Err(ParseHttpRequestError::ParseHttpMethod),
    }
}

pub(crate) fn parse_http_version(s: &str) -> ParseHttpRequestResult<HttpVersion> {
    match s {
        "HTTP/1.1" => Ok("HTTP/1.1"),
        "HTTP/1.0" => Ok("HTTP/1.0"),
        "HTTP/2.0" => Ok("HTTP/2.0"),
        _ => Err(ParseHttpRequestError::ParseHttpVersion),
    }
}

pub(crate) fn parse_http_request(s: &str) -> ParseHttpRequestResult<HttpRequest> {
    let mut parts = s.splitn(3, ' ');

    let method_str = parts.next().ok_or(ParseHttpRequestError::MissingField)?;
    let url_raw = parts.next().ok_or(ParseHttpRequestError::MissingField)?;
    let version_str = parts.next().ok_or(ParseHttpRequestError::MissingField)?;

    let http_method = parse_http_method(method_str)?;
    let http_version = parse_http_version(version_str)?;

    Ok(HttpRequest {
        http_method,
        url_raw: url_raw.to_string(),
        http_version,
    })
}

pub(crate) type ParseTimeIntervalResult<T> = result::Result<T, ParseTimeIntervalError>;

#[derive(thiserror::Error, PartialEq, Eq, Clone, Debug)]
pub enum ParseTimeIntervalError {
    #[error("Parse Integral Error: {0}")]
    ParseIntegral(#[from] std::num::ParseIntError),
    #[error("Missing Part")]
    MissingPart,
    #[error("Unknown Time Unit")]
    UnknownTimeUnit,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum TimeIntervalUnit {
    Second,
    Minute,
    Hour,
    Day,
    Month,
    Year,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct TimeInterval {
    pub(crate) n: u32,
    pub(crate) unit: TimeIntervalUnit,
}

pub(crate) type ParseDatePartResult<T> = result::Result<T, ParseDatePartError>;

#[derive(thiserror::Error, PartialEq, Eq, Clone, Debug)]
pub enum ParseDatePartError {
    #[error("Unknown DatePart Unit")]
    UnknownDatePartUnit,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum DatePartUnit {
    Second,
    Minute,
    Hour,
    Day,
    Month,
    Year,
}

pub(crate) fn parse_date_part_unit(s: &str) -> ParseDatePartResult<DatePartUnit> {
    match s {
        "second" => Ok(DatePartUnit::Second),
        "minute" => Ok(DatePartUnit::Minute),
        "hour" => Ok(DatePartUnit::Hour),
        "day" => Ok(DatePartUnit::Day),
        "month" => Ok(DatePartUnit::Month),
        "year" => Ok(DatePartUnit::Year),
        _ => Err(ParseDatePartError::UnknownDatePartUnit),
    }
}

pub(crate) fn parse_time_interval_unit(s: &str, plural: bool) -> ParseTimeIntervalResult<TimeIntervalUnit> {
    if plural {
        match s {
            "seconds" => Ok(TimeIntervalUnit::Second),
            "minutes" => Ok(TimeIntervalUnit::Minute),
            "hours" => Ok(TimeIntervalUnit::Hour),
            "days" => Ok(TimeIntervalUnit::Day),
            "months" => Ok(TimeIntervalUnit::Month),
            "years" => Ok(TimeIntervalUnit::Year),
            _ => Err(ParseTimeIntervalError::UnknownTimeUnit),
        }
    } else {
        match s {
            "second" => Ok(TimeIntervalUnit::Second),
            "minute" => Ok(TimeIntervalUnit::Minute),
            "hour" => Ok(TimeIntervalUnit::Hour),
            "day" => Ok(TimeIntervalUnit::Day),
            "month" => Ok(TimeIntervalUnit::Month),
            "year" => Ok(TimeIntervalUnit::Year),
            _ => Err(ParseTimeIntervalError::UnknownTimeUnit),
        }
    }
}

pub(crate) fn parse_time_interval(s: &str) -> ParseTimeIntervalResult<TimeInterval> {
    let mut iter = s.split_whitespace();

    let integral_opt = if let Some(token) = iter.next() {
        let integral = token.parse::<u32>()?;
        Some(integral)
    } else {
        None
    };

    let time_unit_opt = if let (Some(token), Some(integral)) = (iter.next(), integral_opt) {
        let time_unit = parse_time_interval_unit(token, integral > 1)?;
        Some(time_unit)
    } else {
        None
    };

    if let (Some(integral), Some(time_unit)) = (integral_opt, time_unit_opt) {
        let interval = TimeInterval {
            n: integral,
            unit: time_unit,
        };

        Ok(interval)
    } else {
        Err(ParseTimeIntervalError::MissingPart)
    }
}

pub(crate) type Tuple = Vec<Value>;
pub type VariableName = String;
pub type Variables = LinkedHashMap<String, Value>;

/// Apply remaining path segments (starting at index `from`) to a single Value.
fn apply_path_to_value(path_expr: &ast::PathExpr, from: usize, value: &Value) -> Value {
    if from >= path_expr.path_segments.len() {
        return value.clone();
    }

    match &path_expr.path_segments[from] {
        ast::PathSegment::AttrName(attr_name) => {
            match value {
                Value::Object(o) => {
                    if let Some(v) = o.get(attr_name) {
                        apply_path_to_value(path_expr, from + 1, v)
                    } else {
                        Value::Missing
                    }
                }
                _ => Value::Missing,
            }
        }
        ast::PathSegment::ArrayIndex(_attr_name, idx) => {
            match value {
                Value::Array(a) => {
                    if *idx < a.len() {
                        apply_path_to_value(path_expr, from + 1, &a[*idx])
                    } else {
                        Value::Missing
                    }
                }
                _ => Value::Missing,
            }
        }
        ast::PathSegment::Wildcard => {
            // [*] — iterate all elements of an array
            match value {
                Value::Array(arr) => {
                    let results: Vec<Value> = arr
                        .iter()
                        .map(|elem| apply_path_to_value(path_expr, from + 1, elem))
                        .collect();
                    Value::Array(results)
                }
                _ => Value::Missing,
            }
        }
        ast::PathSegment::WildcardAttr => {
            // .* — iterate all values of a tuple/object
            match value {
                Value::Object(obj) => {
                    let results: Vec<Value> = obj
                        .values()
                        .map(|v| apply_path_to_value(path_expr, from + 1, v))
                        .collect();
                    Value::Array(results)
                }
                _ => Value::Missing,
            }
        }
    }
}

pub(crate) fn get_value_by_path_expr(path_expr: &ast::PathExpr, i: usize, variables: &Variables) -> Value {
    if i >= path_expr.path_segments.len() {
        return Value::Missing;
    }

    match &path_expr.path_segments[i] {
        ast::PathSegment::AttrName(attr_name) => {
            if let Some(val) = variables.get(attr_name) {
                if i + 1 == path_expr.path_segments.len() {
                    return val.clone();
                } else {
                    // Use apply_path_to_value for remaining segments (handles wildcards too)
                    return apply_path_to_value(path_expr, i + 1, val);
                }
            } else {
                Value::Missing
            }
        }
        ast::PathSegment::ArrayIndex(attr_name, idx) => {
            if let Some(val) = variables.get(attr_name) {
                match val {
                    Value::Array(a) => {
                        if *idx < a.len() {
                            if i + 1 == path_expr.path_segments.len() {
                                return a[*idx].clone();
                            } else {
                                return apply_path_to_value(path_expr, i + 1, &a[*idx]);
                            }
                        } else {
                            Value::Missing
                        }
                    }
                    _ => Value::Missing,
                }
            } else {
                Value::Missing
            }
        }
        ast::PathSegment::Wildcard => {
            // [*] at the top level doesn't make sense without a preceding lookup,
            // but handle gracefully
            Value::Missing
        }
        ast::PathSegment::WildcardAttr => {
            // .* at the top level — iterate all values in variables
            let results: Vec<Value> = variables
                .values()
                .map(|v| apply_path_to_value(path_expr, i + 1, v))
                .collect();
            Value::Array(results)
        }
    }
}

pub(crate) fn empty_variables() -> Variables {
    Variables::default()
}

pub(crate) fn merge(left: &Variables, right: &Variables) -> Variables {
    left.iter().chain(right).map(|(k, v)| (k.clone(), v.clone())).collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexBinding {
    pub(crate) idx: usize,
    pub(crate) name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Binding {
    pub(crate) path_expr: ast::PathExpr,
    pub(crate) name: String,
    pub(crate) idx_name: Option<String>,
}

#[derive(Clone)]
pub(crate) struct ParsingContext {
    pub(crate) table_name: String,
    pub(crate) data_source: DataSource,
    pub(crate) registry: Arc<FunctionRegistry>,
}

impl std::fmt::Debug for ParsingContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParsingContext")
            .field("table_name", &self.table_name)
            .field("data_source", &self.data_source)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataSource {
    File(PathBuf, String, String),
    Stdin(String, String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_time_interval() {
        let ans = parse_time_interval("1 minute").unwrap();
        let expected = TimeInterval {
            n: 1,
            unit: TimeIntervalUnit::Minute,
        };

        assert_eq!(expected, ans);

        let ans = parse_time_interval("3 minutes").unwrap();
        let expected = TimeInterval {
            n: 3,
            unit: TimeIntervalUnit::Minute,
        };

        assert_eq!(expected, ans);

        let ans = parse_time_interval("1 second").unwrap();
        let expected = TimeInterval {
            n: 1,
            unit: TimeIntervalUnit::Second,
        };

        assert_eq!(expected, ans);

        let ans = parse_time_interval("13 seconds").unwrap();
        let expected = TimeInterval {
            n: 13,
            unit: TimeIntervalUnit::Second,
        };

        assert_eq!(expected, ans);
    }

    #[test]
    fn test_wildcard_array_path_expr() {
        // Given: variables = { "a": [1, 2, 3] }
        // Path: a[*]
        // Expected: Array([Int(1), Int(2), Int(3)])
        let mut variables = Variables::default();
        variables.insert(
            "a".to_string(),
            Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
        );
        let path = ast::PathExpr::new(vec![
            ast::PathSegment::AttrName("a".to_string()),
            ast::PathSegment::Wildcard,
        ]);
        let result = get_value_by_path_expr(&path, 0, &variables);
        assert_eq!(result, Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]));
    }

    #[test]
    fn test_wildcard_array_on_non_array() {
        // Given: variables = { "a": Int(42) }
        // Path: a[*]
        // Expected: Missing
        let mut variables = Variables::default();
        variables.insert("a".to_string(), Value::Int(42));
        let path = ast::PathExpr::new(vec![
            ast::PathSegment::AttrName("a".to_string()),
            ast::PathSegment::Wildcard,
        ]);
        let result = get_value_by_path_expr(&path, 0, &variables);
        assert_eq!(result, Value::Missing);
    }

    #[test]
    fn test_wildcard_attr_path_expr() {
        // Given: variables = { "a": { "x": 1, "y": 2 } }
        // Path: a.*
        // Expected: Array([Int(1), Int(2)])
        let mut variables = Variables::default();
        let mut obj = LinkedHashMap::default();
        obj.insert("x".to_string(), Value::Int(1));
        obj.insert("y".to_string(), Value::Int(2));
        variables.insert("a".to_string(), Value::Object(Box::new(obj)));
        let path = ast::PathExpr::new(vec![
            ast::PathSegment::AttrName("a".to_string()),
            ast::PathSegment::WildcardAttr,
        ]);
        let result = get_value_by_path_expr(&path, 0, &variables);
        assert_eq!(result, Value::Array(vec![Value::Int(1), Value::Int(2)]));
    }

    #[test]
    fn test_wildcard_attr_on_non_object() {
        // Given: variables = { "a": Int(42) }
        // Path: a.*
        // Expected: Missing
        let mut variables = Variables::default();
        variables.insert("a".to_string(), Value::Int(42));
        let path = ast::PathExpr::new(vec![
            ast::PathSegment::AttrName("a".to_string()),
            ast::PathSegment::WildcardAttr,
        ]);
        let result = get_value_by_path_expr(&path, 0, &variables);
        assert_eq!(result, Value::Missing);
    }

    #[test]
    fn test_wildcard_array_nested_path() {
        // Given: variables = { "a": [ { "b": 10 }, { "b": 20 } ] }
        // Path: a[*].b
        // Expected: Array([Int(10), Int(20)])
        let mut variables = Variables::default();
        let mut obj1 = LinkedHashMap::default();
        obj1.insert("b".to_string(), Value::Int(10));
        let mut obj2 = LinkedHashMap::default();
        obj2.insert("b".to_string(), Value::Int(20));
        variables.insert(
            "a".to_string(),
            Value::Array(vec![Value::Object(Box::new(obj1)), Value::Object(Box::new(obj2))]),
        );
        let path = ast::PathExpr::new(vec![
            ast::PathSegment::AttrName("a".to_string()),
            ast::PathSegment::Wildcard,
            ast::PathSegment::AttrName("b".to_string()),
        ]);
        let result = get_value_by_path_expr(&path, 0, &variables);
        assert_eq!(result, Value::Array(vec![Value::Int(10), Value::Int(20)]));
    }
}
