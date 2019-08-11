use crate::common;
use chrono;
use hashbrown::HashMap;
use ordered_float::OrderedFloat;
use regex::Regex;
use std::fmt;
use std::path::PathBuf;
use std::result;
use url;

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub(crate) enum Value {
    Int(i32),
    Float(OrderedFloat<f32>),
    Boolean(bool),
    String(String),
    Null,
    DateTime(chrono::DateTime<chrono::offset::FixedOffset>),
    HttpRequest(common::types::HttpRequest),
    Host(common::types::Host),
}

pub(crate) type ParseHostResult<T> = result::Result<T, ParseHostError>;

#[derive(Fail, Debug)]
pub(crate) enum ParseHostError {
    #[fail(display = "Parse Host Error")]
    ParseHost,
    #[fail(display = "{}", _0)]
    ParsePort(#[cause] std::num::ParseIntError),
}

impl From<std::num::ParseIntError> for ParseHostError {
    fn from(err: std::num::ParseIntError) -> ParseHostError {
        ParseHostError::ParsePort(err)
    }
}

pub(crate) type Hostname = String;
pub(crate) type Port = u16;

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub(crate) struct Host {
    pub(crate) hostname: Hostname,
    pub(crate) port: Port,
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
    //FIXME: use different type for string hostname and Ipv4
    let host_regex_literal = r#"([\.0-9a-zA-Z]+):([0-9]+)"#;
    let host_regex: Regex = Regex::new(host_regex_literal).unwrap();

    if let Some(cap) = host_regex.captures(s) {
        //FIXME: very simplified parsing.
        let hostname = cap.get(1).map_or("", |m| m.as_str()).to_string();
        let port: u16 = cap.get(2).map_or("", |m| m.as_str()).parse::<u16>()?;

        let host = Host { hostname, port };
        Ok(host)
    } else {
        Err(ParseHostError::ParseHost)
    }
}

pub(crate) type ParseHttpRequestResult<T> = result::Result<T, ParseHttpRequestError>;

#[derive(Fail, Debug)]
pub(crate) enum ParseHttpRequestError {
    #[fail(display = "Parse Http Method Error")]
    ParseHttpMethod,
    #[fail(display = "{}", _0)]
    ParseUrl(#[cause] url::ParseError),
    #[fail(display = "Parse Http Version Error")]
    ParseHttpVersion,
    #[fail(display = "Missing Field")]
    MissingField,
}

impl From<url::ParseError> for ParseHttpRequestError {
    fn from(err: url::ParseError) -> ParseHttpRequestError {
        ParseHttpRequestError::ParseUrl(err)
    }
}

pub(crate) type HttpMethod = String;
pub(crate) type HttpVersion = String;

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub(crate) struct HttpRequest {
    pub(crate) http_method: String,
    pub(crate) url: url::Url,
    pub(crate) http_version: String,
}

impl fmt::Display for HttpRequest {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(&*self.http_method)?;
        fmt.write_str(" ")?;
        fmt.write_str(&*self.url.to_string())?;
        fmt.write_str(" ")?;
        fmt.write_str(&*self.http_version)?;
        Ok(())
    }
}

pub(crate) fn parse_http_method(s: &str) -> ParseHttpRequestResult<HttpMethod> {
    if s == "GET" || s == "POST" || s == "DELETE" || s == "HEAD" {
        Ok(s.to_string())
    } else {
        Err(ParseHttpRequestError::ParseHttpMethod)
    }
}

pub(crate) fn parse_http_version(s: &str) -> ParseHttpRequestResult<HttpVersion> {
    if s == "HTTP/1.1" || s == "HTTP/1.0" {
        Ok(s.to_string())
    } else {
        Err(ParseHttpRequestError::ParseHttpVersion)
    }
}

pub(crate) fn parse_http_request(s: &str) -> ParseHttpRequestResult<HttpRequest> {
    //FIXME: use different type for string hostname and Ipv4
    let regex_literal = r#"[^\s"']+|"([^"]*)"|'([^']*)'"#;
    let split_the_line_regex: Regex = Regex::new(regex_literal).unwrap();
    let mut iter = split_the_line_regex.find_iter(&s);

    let http_method_opt = if let Some(m) = iter.next() {
        let method = parse_http_method(m.as_str())?;
        Some(method)
    } else {
        None
    };

    let url_opt = if let Some(m) = iter.next() {
        let url = url::Url::parse(m.as_str())?;
        Some(url)
    } else {
        None
    };

    let http_version_opt = if let Some(m) = iter.next() {
        let version = parse_http_version(m.as_str())?;
        Some(version)
    } else {
        None
    };

    if let (Some(http_method), Some(url), Some(http_version)) = (http_method_opt, url_opt, http_version_opt) {
        let request = HttpRequest {
            http_method,
            url,
            http_version,
        };

        Ok(request)
    } else {
        Err(ParseHttpRequestError::MissingField)
    }
}

pub(crate) type Tuple = Vec<Value>;
pub(crate) type VariableName = String;
pub(crate) type Variables = HashMap<VariableName, Value>;

pub(crate) fn empty_variables() -> Variables {
    Variables::default()
}

pub(crate) fn merge(left: Variables, right: Variables) -> Variables {
    left.into_iter().chain(right).collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DataSource {
    File(PathBuf),
    Stdin,
}
