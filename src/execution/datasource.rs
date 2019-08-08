use super::stream::Record;
use crate::common::types::Value;
use regex::Regex;

use std::fmt;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::path::Path;
use std::result;
use std::str::FromStr;

//Reference: https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html
pub enum ClassicLoadBalancerLogField {
    Timestamp = 0,
    Elbname = 1,
    ClientAndPort = 2,
    BackendAndPort = 3,
    RequestProcessingTime = 4,
    BackendProcessingTime = 5,
    ResponseProcessingTime = 6,
    ELBStatusCode = 7,
    BackendStatusCode = 8,
    ReceivedBytes = 9,
    SentBytes = 10,
    Request = 11,
    UserAgent = 12,
    SSLCipher = 13,
    SSLProtocol = 14,
}

impl fmt::Display for ClassicLoadBalancerLogField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            ClassicLoadBalancerLogField::Timestamp => "timestamp",
            ClassicLoadBalancerLogField::Elbname => "elbname",
            ClassicLoadBalancerLogField::ClientAndPort => "client_and_port",
            ClassicLoadBalancerLogField::BackendAndPort => "backend_and_port",
            _ => unimplemented!(),
        };

        write!(f, "{}", name)
    }
}

impl FromStr for ClassicLoadBalancerLogField {
    type Err = String;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "timestamp" => Ok(ClassicLoadBalancerLogField::Timestamp),
            "elbname" => Ok(ClassicLoadBalancerLogField::Elbname),
            "client_and_port" => Ok(ClassicLoadBalancerLogField::ClientAndPort),
            "backend_and_port" => Ok(ClassicLoadBalancerLogField::BackendAndPort),
            "request_processing_time" => Ok(ClassicLoadBalancerLogField::RequestProcessingTime),
            "backend_processing_time" => Ok(ClassicLoadBalancerLogField::BackendProcessingTime),
            "response_processing_time" => Ok(ClassicLoadBalancerLogField::ResponseProcessingTime),
            "elb_status_code" => Ok(ClassicLoadBalancerLogField::ELBStatusCode),
            "backend_status_code" => Ok(ClassicLoadBalancerLogField::BackendStatusCode),
            "received_bytes" => Ok(ClassicLoadBalancerLogField::ReceivedBytes),
            "sent_bytes" => Ok(ClassicLoadBalancerLogField::SentBytes),
            "request" => Ok(ClassicLoadBalancerLogField::Request),
            "user_agent" => Ok(ClassicLoadBalancerLogField::UserAgent),
            "ssl_cipher" => Ok(ClassicLoadBalancerLogField::SSLCipher),
            "ssl_protocol" => Ok(ClassicLoadBalancerLogField::SSLProtocol),
            _ => Err("unknown column name".to_string()),
        }
    }
}

impl ClassicLoadBalancerLogField {
    pub(crate) fn field_names() -> Vec<String> {
        vec![
            "timestamp".to_string(),
            "elbname".to_string(),
            "client_and_port".to_string(),
            "backend_and_port".to_string(),
            "request_processing_time".to_string(),
            "backend_processing_time".to_string(),
            "response_processing_time".to_string(),
            "elb_status_code".to_string(),
            "backend_status_code".to_string(),
            "received_bytes".to_string(),
            "sent_bytes".to_string(),
            "request".to_string(),
            "user_agent".to_string(),
            "ssl_cipher".to_string(),
            "ssl_protocol".to_string(),
        ]
    }
}

pub(crate) type ReaderResult<T> = result::Result<T, ReaderError>;

#[derive(Fail, Debug)]
pub(crate) enum ReaderError {
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
}

impl From<io::Error> for ReaderError {
    fn from(err: io::Error) -> ReaderError {
        ReaderError::Io(err)
    }
}

#[derive(Debug)]
pub(crate) struct ReaderBuilder {
    capacity: usize,
}

impl Default for ReaderBuilder {
    fn default() -> Self {
        ReaderBuilder {
            capacity: 8 * (1 << 10),
        }
    }
}

pub(crate) trait RecordRead {
    fn read_record(&mut self) -> ReaderResult<Option<Record>>;
}

impl ReaderBuilder {
    pub(crate) fn new() -> Self {
        ReaderBuilder::default()
    }

    pub(crate) fn with_path<P: AsRef<Path>>(&self, path: P) -> ReaderResult<Reader<File>> {
        Ok(Reader::new(self, File::open(path)?))
    }
}

#[derive(Debug)]
pub(crate) struct Reader<R> {
    rdr: io::BufReader<R>,
}

impl<R: io::Read> Reader<R> {
    pub(crate) fn new(builder: &ReaderBuilder, rdr: R) -> Reader<R> {
        Reader {
            rdr: io::BufReader::with_capacity(builder.capacity, rdr),
        }
    }
    fn close(&self) {}
}

impl<R: io::Read> RecordRead for Reader<R> {
    fn read_record(&mut self) -> ReaderResult<Option<Record>> {
        let mut buf = String::new();
        self.rdr.read_line(&mut buf);

        let regex_literal = r#"[^\s"']+|"([^"]*)"|'([^']*)'"#;
        let split_the_line_regex: Regex = Regex::new(regex_literal).unwrap();
        //FIXME: parse to the more specific
        let values: Vec<Value> = split_the_line_regex
            .find_iter(&buf)
            .map(|x| Value::String(x.as_str().to_string()))
            .collect();
        let record = Record::new(ClassicLoadBalancerLogField::field_names(), values);

        Ok(Some(record))
    }
}
