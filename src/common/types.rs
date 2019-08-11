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
    Url(url::Url),
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
