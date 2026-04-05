use crate::common::types::Value;
use crate::execution::types::ExpressionError;
use crate::functions::registry::{FunctionDef, FunctionRegistry, Arity, NullHandling, RegistryError};

pub fn register(registry: &mut FunctionRegistry) -> Result<(), RegistryError> {
    // url_host: extract hostname from HttpRequest
    registry.register(FunctionDef {
        name: "url_host".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Custom,
        func: Box::new(|args| match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Missing => Ok(Value::Missing),
            Value::HttpRequest(r) => {
                if let Some(host) = r.url.host_str() {
                    Ok(Value::String(host.to_string()))
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // url_port: extract port from HttpRequest
    registry.register(FunctionDef {
        name: "url_port".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Custom,
        func: Box::new(|args| match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Missing => Ok(Value::Missing),
            Value::HttpRequest(r) => {
                if let Some(port) = r.url.port() {
                    Ok(Value::Int(port as i32))
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // url_path: extract URL path from HttpRequest
    registry.register(FunctionDef {
        name: "url_path".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Custom,
        func: Box::new(|args| match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Missing => Ok(Value::Missing),
            Value::HttpRequest(r) => {
                let url_path = r.url.path();
                Ok(Value::String(url_path.to_string()))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // url_fragment: extract fragment from HttpRequest
    registry.register(FunctionDef {
        name: "url_fragment".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Custom,
        func: Box::new(|args| match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Missing => Ok(Value::Missing),
            Value::HttpRequest(r) => {
                if let Some(url_fragment) = r.url.fragment() {
                    Ok(Value::String(url_fragment.to_string()))
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // url_query: extract query string from HttpRequest
    registry.register(FunctionDef {
        name: "url_query".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Custom,
        func: Box::new(|args| match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Missing => Ok(Value::Missing),
            Value::HttpRequest(r) => {
                if let Some(url_query) = r.url.query() {
                    Ok(Value::String(url_query.to_string()))
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // url_path_segments: extract path segment at index from HttpRequest
    registry.register(FunctionDef {
        name: "url_path_segments".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Custom,
        func: Box::new(|args| match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Missing => Ok(Value::Missing),
            Value::HttpRequest(r) => {
                match &args[1] {
                    Value::Int(idx) => {
                        if let Some(url_path_segments) = r.url.path_segments() {
                            let idx = *idx as usize;
                            for (i, segment) in url_path_segments.enumerate() {
                                if i == idx {
                                    return Ok(Value::String(segment.to_string()));
                                }
                            }
                            Ok(Value::Null)
                        } else {
                            Ok(Value::Null)
                        }
                    }
                    _ => Err(ExpressionError::InvalidArguments),
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // url_path_bucket: replace path segment at index with target string
    registry.register(FunctionDef {
        name: "url_path_bucket".to_string(),
        arity: Arity::Exact(3),
        null_handling: NullHandling::Custom,
        func: Box::new(|args| match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Missing => Ok(Value::Missing),
            Value::HttpRequest(r) => {
                match (&args[1], &args[2]) {
                    (Value::Int(idx), Value::String(target)) => {
                        if let Some(url_path_segments) = r.url.path_segments() {
                            let idx = *idx as usize;
                            let mut res = String::new();
                            for (i, segment) in url_path_segments.enumerate() {
                                if i == idx {
                                    res.push('/');
                                    res.push_str(target);
                                } else {
                                    res.push('/');
                                    res.push_str(segment);
                                }
                            }
                            Ok(Value::String(res))
                        } else {
                            Ok(Value::Null)
                        }
                    }
                    _ => Err(ExpressionError::InvalidArguments),
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{Value, HttpRequest};
    use url::Url;

    fn make_registry() -> FunctionRegistry {
        let mut r = FunctionRegistry::new();
        register(&mut r).unwrap();
        r
    }

    fn make_request(url_str: &str) -> Value {
        let url = Url::parse(url_str).unwrap();
        Value::HttpRequest(HttpRequest {
            http_method: "GET".to_string(),
            url,
            http_version: "HTTP/1.1".to_string(),
        })
    }

    #[test]
    fn test_url_host() {
        let r = make_registry();
        let req = make_request("http://example.com/path");
        assert_eq!(r.call("url_host", &[req]), Ok(Value::String("example.com".to_string())));
    }

    #[test]
    fn test_url_host_null_missing() {
        let r = make_registry();
        assert_eq!(r.call("url_host", &[Value::Null]), Ok(Value::Null));
        assert_eq!(r.call("url_host", &[Value::Missing]), Ok(Value::Missing));
    }

    #[test]
    fn test_url_port() {
        let r = make_registry();
        let req = make_request("http://example.com:8080/path");
        assert_eq!(r.call("url_port", &[req]), Ok(Value::Int(8080)));
    }

    #[test]
    fn test_url_port_none() {
        let r = make_registry();
        let req = make_request("http://example.com/path");
        assert_eq!(r.call("url_port", &[req]), Ok(Value::Null));
    }

    #[test]
    fn test_url_path() {
        let r = make_registry();
        let req = make_request("http://example.com/foo/bar");
        assert_eq!(r.call("url_path", &[req]), Ok(Value::String("/foo/bar".to_string())));
    }

    #[test]
    fn test_url_fragment() {
        let r = make_registry();
        let req = make_request("http://example.com/path#section1");
        assert_eq!(r.call("url_fragment", &[req]), Ok(Value::String("section1".to_string())));
    }

    #[test]
    fn test_url_fragment_none() {
        let r = make_registry();
        let req = make_request("http://example.com/path");
        assert_eq!(r.call("url_fragment", &[req]), Ok(Value::Null));
    }

    #[test]
    fn test_url_query() {
        let r = make_registry();
        let req = make_request("http://example.com/path?key=value&a=b");
        assert_eq!(r.call("url_query", &[req]), Ok(Value::String("key=value&a=b".to_string())));
    }

    #[test]
    fn test_url_query_none() {
        let r = make_registry();
        let req = make_request("http://example.com/path");
        assert_eq!(r.call("url_query", &[req]), Ok(Value::Null));
    }

    #[test]
    fn test_url_path_segments() {
        let r = make_registry();
        let req = make_request("http://example.com/foo/bar/baz");
        assert_eq!(r.call("url_path_segments", &[req.clone(), Value::Int(0)]), Ok(Value::String("foo".to_string())));
        assert_eq!(r.call("url_path_segments", &[req.clone(), Value::Int(1)]), Ok(Value::String("bar".to_string())));
        assert_eq!(r.call("url_path_segments", &[req.clone(), Value::Int(2)]), Ok(Value::String("baz".to_string())));
        assert_eq!(r.call("url_path_segments", &[req, Value::Int(5)]), Ok(Value::Null));
    }

    #[test]
    fn test_url_path_segments_null_missing() {
        let r = make_registry();
        assert_eq!(r.call("url_path_segments", &[Value::Null, Value::Int(0)]), Ok(Value::Null));
        assert_eq!(r.call("url_path_segments", &[Value::Missing, Value::Int(0)]), Ok(Value::Missing));
    }

    #[test]
    fn test_url_path_bucket() {
        let r = make_registry();
        let req = make_request("http://example.com/foo/bar/baz");
        assert_eq!(
            r.call("url_path_bucket", &[req, Value::Int(1), Value::String("*".to_string())]),
            Ok(Value::String("/foo/*/baz".to_string()))
        );
    }

    #[test]
    fn test_url_path_bucket_null_missing() {
        let r = make_registry();
        assert_eq!(
            r.call("url_path_bucket", &[Value::Null, Value::Int(0), Value::String("*".to_string())]),
            Ok(Value::Null)
        );
        assert_eq!(
            r.call("url_path_bucket", &[Value::Missing, Value::Int(0), Value::String("*".to_string())]),
            Ok(Value::Missing)
        );
    }
}
