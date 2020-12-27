use super::stream::Record;
use crate::common;
use crate::common::types::Value;
use ordered_float::OrderedFloat;
use regex::Regex;
use url;

use std::fmt;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::path::Path;
use std::result;
use std::str::FromStr;

lazy_static! {
    static ref SPLIT_READER_LINE_REGEX: Regex = Regex::new(r#"[^\s"'\[\]]+|"([^"]*)"|'([^']*)'|\[([^\[\]]*)\]"#).unwrap();
}


#[derive(PartialEq, Eq, Debug, Clone)]
pub(crate) enum DataType {
    DateTime,
    String,
    Integral,
    Float,
    Host,
    HttpRequest,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            DataType::DateTime => "DateTime",
            DataType::String => "String",
            DataType::Integral => "Integral",
            DataType::Float => "Float",
            DataType::Host => "Host",
            DataType::HttpRequest => "HttpRequest",
        };

        write!(f, "{}", name)
    }
}

lazy_static! {
    static ref AWS_ELB_DATATYPES: Vec<DataType> = {
        vec![
            DataType::DateTime,
            DataType::String,
            DataType::Host,
            DataType::Host,
            DataType::Float,
            DataType::Float,
            DataType::Float,
            DataType::String,
            DataType::String,
            DataType::Integral,
            DataType::Integral,
            DataType::HttpRequest,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
        ]
    };
}

lazy_static! {
    static ref AWS_ELB_FIELD_NAMES: Vec<String> = {
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
            "target_group_arn".to_string(),
            "trace_id".to_string(),
        ]
    };
}

lazy_static! {
    static ref AWS_ALB_DATATYPES: Vec<DataType> = {
        vec![
            DataType::String,
            DataType::DateTime,
            DataType::String,
            DataType::Host,
            DataType::Host,
            DataType::Float,
            DataType::Float,
            DataType::Float,
            DataType::String,
            DataType::String,
            DataType::Integral,
            DataType::Integral,
            DataType::HttpRequest,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
        ]
    };
}

lazy_static! {
    static ref AWS_ALB_FIELD_NAMES: Vec<String> = {
        vec![
            "type".to_string(),
            "timestamp".to_string(),
            "elb".to_string(),
            "client_and_port".to_string(),
            "target_and_port".to_string(),
            "request_processing_time".to_string(),
            "target_processing_time".to_string(),
            "response_processing_time".to_string(),
            "elb_status_code".to_string(),
            "target_status_code".to_string(),
            "received_bytes".to_string(),
            "sent_bytes".to_string(),
            "request".to_string(),
            "user_agent".to_string(),
            "ssl_cipher".to_string(),
            "ssl_protocol".to_string(),
            "target_group_arn".to_string(),
            "trace_id".to_string(),
            "domain_name".to_string(),
            "chosen_cert_arn".to_string(),
            "matched_rule_priority".to_string(),
            "request_creation_time".to_string(),
            "action_executed".to_string(),
            "redirect_url".to_string(),
            "error_reason".to_string(),
        ]
    };
}

lazy_static! {
    static ref AWS_S3_FIELD_NAMES: Vec<String> = {
        vec![
            "bucket_owner".to_string(),
            "bucket".to_string(),
            "time".to_string(),
            "remote_ip".to_string(),
            "requester".to_string(),
            "request_id".to_string(),
            "operation".to_string(),
            "key".to_string(),
            "request_uri".to_string(),
            "http_status".to_string(),
            "error_code".to_string(),
            "bytes_sent".to_string(),
            "object_size".to_string(),
            "total_time".to_string(),
            "turn_around_time".to_string(),
            "refererr".to_string(),
            "user_agent".to_string(),
            "version_id".to_string(),
            "host_id".to_string(),
            "signature_version".to_string(),
            "cipher_suite".to_string(),
            "authentication_type".to_string(),
            "host_header".to_string(),
            "tls_version".to_string(),
        ]
    };
}

lazy_static! {
    static ref AWS_S3_DATATYPES: Vec<DataType> = {
        vec![
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
        ]
    };
}

lazy_static! {
    static ref SQUID_FIELD_NAMES: Vec<String> = {
        vec![
            "timestamp".to_string(),
            "elapsed".to_string(),
            "remote_host".to_string(),
            "code_and_status".to_string(),
            "bytes".to_string(),
            "method".to_string(),
            "url".to_string(),
            "rfc931".to_string(),
            "peer_status_and_peer_host".to_string(),
            "type".to_string(),
        ]
    };
}

lazy_static! {
    static ref SQUID_DATATYPES: Vec<DataType> = {
        vec![
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
        ]
    };
}

//Reference: https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html
pub(crate) enum ClassicLoadBalancerLogField {
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
    TargetGroupArn = 15,
    TraceID = 16,
}

impl fmt::Display for ClassicLoadBalancerLogField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            ClassicLoadBalancerLogField::Timestamp => "timestamp",
            ClassicLoadBalancerLogField::Elbname => "elbname",
            ClassicLoadBalancerLogField::ClientAndPort => "client_and_port",
            ClassicLoadBalancerLogField::BackendAndPort => "backend_and_port",
            ClassicLoadBalancerLogField::RequestProcessingTime => "request_processing_time",
            ClassicLoadBalancerLogField::BackendProcessingTime => "backend_processing_time",
            ClassicLoadBalancerLogField::ResponseProcessingTime => "response_processing_time",
            ClassicLoadBalancerLogField::ELBStatusCode => "elb_status_code",
            ClassicLoadBalancerLogField::BackendStatusCode => "backend_status_code",
            ClassicLoadBalancerLogField::ReceivedBytes => "received_bytes",
            ClassicLoadBalancerLogField::SentBytes => "sent_bytes",
            ClassicLoadBalancerLogField::Request => "request",
            ClassicLoadBalancerLogField::UserAgent => "user_agent",
            ClassicLoadBalancerLogField::SSLCipher => "ssl_cipher",
            ClassicLoadBalancerLogField::SSLProtocol => "ssl_protocol",
            ClassicLoadBalancerLogField::TargetGroupArn => "target_group_arn",
            ClassicLoadBalancerLogField::TraceID => "trace_id",
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
            "target_group_arn" => Ok(ClassicLoadBalancerLogField::TargetGroupArn),
            "trace_id" => Ok(ClassicLoadBalancerLogField::TraceID),
            _ => Err("unknown column name".to_string()),
        }
    }
}

impl ClassicLoadBalancerLogField {
    pub(crate) fn len() -> usize {
        17
    }

    pub(crate) fn field_names<'a>() -> &'a Vec<String> {
        &AWS_ELB_FIELD_NAMES
    }

    pub(crate) fn datatypes() -> Vec<DataType> {
        AWS_ELB_DATATYPES.clone()
    }

    pub(crate) fn datatype(idx: usize) -> DataType {
        AWS_ELB_DATATYPES[idx].clone()
    }

    pub(crate) fn schema() -> Vec<(String, DataType)> {
        let fields = Self::field_names().clone();
        let datatypes = Self::datatypes();
        fields.into_iter().zip(datatypes.into_iter()).collect()
    }
}

//Reference: https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html
pub(crate) enum ApplicationLoadBalancerLogField {
    Type = 0,
    Timestamp = 1,
    Elbname = 2,
    ClientAndPort = 3,
    TargetAndPort = 4,
    RequestProcessingTime = 5,
    TargetProcessingTime = 6,
    ResponseProcessingTime = 7,
    ELBStatusCode = 8,
    TargetStatusCode = 9,
    ReceivedBytes = 10,
    SentBytes = 11,
    Request = 12,
    UserAgent = 13,
    SSLCipher = 14,
    SSLProtocol = 15,
    TargetGroupArn = 16,
    TraceID = 17,
    DomainName = 18,
    ChosenCertArn = 19,
    MatchedRulePriority = 20,
    RequestCreationTime = 21,
    ActionExecuted = 22,
    RedirectUrl = 23,
    ErrorReason = 24,
}

impl FromStr for ApplicationLoadBalancerLogField {
    type Err = String;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "type" => Ok(ApplicationLoadBalancerLogField::Type),
            "timestamp" => Ok(ApplicationLoadBalancerLogField::Timestamp),
            "elbname" => Ok(ApplicationLoadBalancerLogField::Elbname),
            "client_and_port" => Ok(ApplicationLoadBalancerLogField::ClientAndPort),
            "backend_and_port" => Ok(ApplicationLoadBalancerLogField::TargetAndPort),
            "request_processing_time" => Ok(ApplicationLoadBalancerLogField::RequestProcessingTime),
            "backend_processing_time" => Ok(ApplicationLoadBalancerLogField::TargetProcessingTime),
            "response_processing_time" => Ok(ApplicationLoadBalancerLogField::ResponseProcessingTime),
            "elb_status_code" => Ok(ApplicationLoadBalancerLogField::ELBStatusCode),
            "backend_status_code" => Ok(ApplicationLoadBalancerLogField::TargetStatusCode),
            "received_bytes" => Ok(ApplicationLoadBalancerLogField::ReceivedBytes),
            "sent_bytes" => Ok(ApplicationLoadBalancerLogField::SentBytes),
            "request" => Ok(ApplicationLoadBalancerLogField::Request),
            "user_agent" => Ok(ApplicationLoadBalancerLogField::UserAgent),
            "ssl_cipher" => Ok(ApplicationLoadBalancerLogField::SSLCipher),
            "ssl_protocol" => Ok(ApplicationLoadBalancerLogField::SSLProtocol),
            "target_group_arn" => Ok(ApplicationLoadBalancerLogField::TargetGroupArn),
            "trace_id" => Ok(ApplicationLoadBalancerLogField::TraceID),
            "domain_name" => Ok(ApplicationLoadBalancerLogField::DomainName),
            "chosen_cert_arn" => Ok(ApplicationLoadBalancerLogField::ChosenCertArn),
            "matched_rule_priority" => Ok(ApplicationLoadBalancerLogField::MatchedRulePriority),
            "request_creation_time" => Ok(ApplicationLoadBalancerLogField::RequestCreationTime),
            "action_executed" => Ok(ApplicationLoadBalancerLogField::ActionExecuted),
            "redirect_url" => Ok(ApplicationLoadBalancerLogField::RedirectUrl),
            "error_reason" => Ok(ApplicationLoadBalancerLogField::ErrorReason),
            _ => Err("unknown column name".to_string()),
        }
    }
}

impl ApplicationLoadBalancerLogField {
    pub(crate) fn len() -> usize {
        25
    }

    pub(crate) fn field_names<'a>() -> &'a Vec<String> {
        &AWS_ALB_FIELD_NAMES
    }

    pub(crate) fn datatypes() -> Vec<DataType> {
        AWS_ALB_DATATYPES.clone()
    }

    pub(crate) fn datatype(idx: usize) -> DataType {
        AWS_ALB_DATATYPES[idx].clone()
    }

    pub(crate) fn schema() -> Vec<(String, DataType)> {
        let fields = Self::field_names().clone();
        let datatypes = Self::datatypes();
        fields.into_iter().zip(datatypes.into_iter()).collect()
    }
}

// https://docs.aws.amazon.com/AmazonS3/latest/dev/LogFormat.html
pub(crate) enum S3Field {
    BucketOwner = 0,
    Bucket = 1,
    Time = 2,
    RemoteIp = 3,
    Requester = 4,
    RequestId = 5,
    Operation = 6,
    Key = 7,
    RequestUri = 8,
    HttpStatus = 9,
    ErrorCode = 10,
    BytesSent = 11,
    ObjectSize = 12,
    TotalTime = 13,
    TurnAroundTime = 14,
    Referrer = 15,
    UserAgent = 16,
    VersionId = 17,
    HostId = 18,
    SignatureVersion = 19,
    CipherSuite = 20,
    AuthenticationType = 21,
    HostHeader = 22,
    TlsVersion = 23,
}

impl FromStr for S3Field {
    type Err = String;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "bucket_owner" => Ok(S3Field::BucketOwner),
            "bucket" => Ok(S3Field::Bucket),
            "time" => Ok(S3Field::Time),
            "remote_ip" => Ok(S3Field::RemoteIp),
            "requester" => Ok(S3Field::Requester),
            "request_id" => Ok(S3Field::RequestId),
            "operation" => Ok(S3Field::Operation),
            "key" => Ok(S3Field::Key),
            "request_uri" => Ok(S3Field::RequestUri),
            "http_status" => Ok(S3Field::HttpStatus),
            "error_code" => Ok(S3Field::ErrorCode),
            "bytes_sent" => Ok(S3Field::BytesSent),
            "object_size" => Ok(S3Field::ObjectSize),
            "total_time" => Ok(S3Field::TotalTime),
            "turn_around_time" => Ok(S3Field::TurnAroundTime),
            "refererr" => Ok(S3Field::Referrer),
            "user_agent" => Ok(S3Field::UserAgent),
            "version_id" => Ok(S3Field::VersionId),
            "host_id" => Ok(S3Field::HostId),
            "signature_version" => Ok(S3Field::SignatureVersion),
            "cipher_suite" => Ok(S3Field::CipherSuite),
            "authentication_type" => Ok(S3Field::AuthenticationType),
            "host_header" => Ok(S3Field::HostHeader),
            "tls_version" => Ok(S3Field::TlsVersion),
            _ => Err("unknown column name".to_string()),
        }
    }
}

impl S3Field {
    pub(crate) fn len() -> usize {
        24
    }

    pub(crate) fn field_names<'a>() -> &'a Vec<String> {
        &AWS_S3_FIELD_NAMES
    }

    pub(crate) fn datatypes() -> Vec<DataType> {
        AWS_S3_DATATYPES.clone()
    }

    pub(crate) fn datatype(idx: usize) -> DataType {
        AWS_S3_DATATYPES[idx].clone()
    }

    pub(crate) fn schema() -> Vec<(String, DataType)> {
        let fields = Self::field_names().clone();
        let datatypes = Self::datatypes();
        fields.into_iter().zip(datatypes.into_iter()).collect()
    }
}

//Reference: https://wiki.squid-cache.org/Features/LogFormat
pub(crate) enum SquidLogField {
    Timestamp = 0,
    Elapsed = 1,
    RemoteHost = 2,
    CodeAndStatus = 3,
    Bytes = 4,
    Method = 5,
    Url = 6,
    Rfc931 = 7,
    PeerstatusAndPeerhost = 8,
    Type = 9,
}

impl FromStr for SquidLogField {
    type Err = String;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "timestamp" => Ok(SquidLogField::Timestamp),
            "elapsed" => Ok(SquidLogField::Elapsed),
            "remote_host" => Ok(SquidLogField::RemoteHost),
            "code_and_status" => Ok(SquidLogField::CodeAndStatus),
            "bytes" => Ok(SquidLogField::Bytes),
            "method" => Ok(SquidLogField::Method),
            "url" => Ok(SquidLogField::Url),
            "rfc931" => Ok(SquidLogField::Rfc931),
            "peer_status_and_peer_host" => Ok(SquidLogField::PeerstatusAndPeerhost),
            "type" => Ok(SquidLogField::Type),
            _ => Err("unknown column name".to_string()),
        }
    }
}

impl SquidLogField {
    pub(crate) fn len() -> usize {
        10
    }

    pub(crate) fn field_names<'a>() -> &'a Vec<String> {
        &SQUID_FIELD_NAMES
    }

    pub(crate) fn datatypes() -> Vec<DataType> {
        SQUID_DATATYPES.clone()
    }

    pub(crate) fn datatype(idx: usize) -> DataType {
        SQUID_DATATYPES[idx].clone()
    }

    pub(crate) fn schema() -> Vec<(String, DataType)> {
        let fields = Self::field_names().clone();
        let datatypes = Self::datatypes();
        fields.into_iter().zip(datatypes.into_iter()).collect()
    }
}

pub(crate) type ReaderResult<T> = result::Result<T, ReaderError>;

#[derive(Fail, Debug)]
pub(crate) enum ReaderError {
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    #[fail(display = "{}", _0)]
    ParseDateTime(#[cause] chrono::format::ParseError),
    #[fail(display = "{}", _0)]
    ParseIntegral(#[cause] std::num::ParseIntError),
    #[fail(display = "{}", _0)]
    ParseFloat(#[cause] std::num::ParseFloatError),
    #[fail(display = "{}", _0)]
    ParseUrl(#[cause] url::ParseError),
    #[fail(display = "{}", _0)]
    ParseHost(#[cause] common::types::ParseHostError),
    #[fail(display = "{}", _0)]
    ParseHttpRequest(#[cause] common::types::ParseHttpRequestError),
}

impl From<io::Error> for ReaderError {
    fn from(err: io::Error) -> ReaderError {
        ReaderError::Io(err)
    }
}

impl From<chrono::format::ParseError> for ReaderError {
    fn from(err: chrono::format::ParseError) -> ReaderError {
        ReaderError::ParseDateTime(err)
    }
}

impl From<std::num::ParseIntError> for ReaderError {
    fn from(err: std::num::ParseIntError) -> ReaderError {
        ReaderError::ParseIntegral(err)
    }
}

impl From<std::num::ParseFloatError> for ReaderError {
    fn from(err: std::num::ParseFloatError) -> ReaderError {
        ReaderError::ParseFloat(err)
    }
}

impl From<common::types::ParseHostError> for ReaderError {
    fn from(err: common::types::ParseHostError) -> ReaderError {
        ReaderError::ParseHost(err)
    }
}

impl From<common::types::ParseHttpRequestError> for ReaderError {
    fn from(err: common::types::ParseHttpRequestError) -> ReaderError {
        ReaderError::ParseHttpRequest(err)
    }
}

impl From<url::ParseError> for ReaderError {
    fn from(err: url::ParseError) -> ReaderError {
        ReaderError::ParseUrl(err)
    }
}

#[derive(Debug)]
pub(crate) struct ReaderBuilder {
    capacity: usize,
    table_name: String,
}

pub(crate) trait RecordRead {
    fn read_record(&mut self) -> ReaderResult<Option<Record>>;
}

impl ReaderBuilder {
    pub(crate) fn new(table_name: String) -> Self {
        ReaderBuilder {
            capacity: 8 * (1 << 10),
            table_name,
        }
    }

    pub(crate) fn with_path<P: AsRef<Path>>(&self, path: P) -> ReaderResult<Reader<File>> {
        Ok(Reader::new(self, File::open(path)?, self.table_name.clone()))
    }

    #[allow(dead_code)]
    pub(crate) fn with_reader<R: io::Read>(&self, rdr: R) -> Reader<R> {
        Reader::new(self, rdr, self.table_name.clone())
    }
}

#[derive(Debug)]
pub(crate) struct Reader<R> {
    rdr: io::BufReader<R>,
    table_name: String,
}

impl<R: io::Read> Reader<R> {
    pub(crate) fn new(builder: &ReaderBuilder, rdr: R, table_name: String) -> Reader<R> {
        Reader {
            rdr: io::BufReader::with_capacity(builder.capacity, rdr),
            table_name,
        }
    }

    #[allow(dead_code)]
    fn close(&self) {}
}

impl<R: io::Read> RecordRead for Reader<R> {
    fn read_record(&mut self) -> ReaderResult<Option<Record>> {
        let mut buf = String::new();
        let more_data = self.rdr.read_line(&mut buf)?;

        if more_data > 0 {
            let field_names = if self.table_name == "elb" {
                ClassicLoadBalancerLogField::field_names()
            } else if self.table_name == "alb" {
                ApplicationLoadBalancerLogField::field_names()
            } else if self.table_name == "s3" {
                S3Field::field_names()
            } else {
                SquidLogField::field_names()
            };

            //FIXME: parse to the more specific
            let mut values: Vec<Value> = Vec::new();
            for (i, m) in SPLIT_READER_LINE_REGEX.find_iter(&buf).enumerate() {
                if self.table_name == "elb" {
                    if i >= ClassicLoadBalancerLogField::len() {
                        break;
                    }
                } else if self.table_name == "alb" {
                    if i >= ApplicationLoadBalancerLogField::len() {
                        break;
                    }
                } else if self.table_name == "squid" {
                    if i >= SquidLogField::len() {
                        break;
                    }
                } else if self.table_name == "s3" {
                    if i >= S3Field::len() {
                        break;
                    }
                } else {
                    unreachable!();
                }

                let s = m.as_str();
                let datatype = if self.table_name == "elb" {
                    ClassicLoadBalancerLogField::datatype(i)
                } else if self.table_name == "alb" {
                    ApplicationLoadBalancerLogField::datatype(i)
                } else if self.table_name == "s3" {
                    S3Field::datatype(i)
                } else {
                    SquidLogField::datatype(i)
                };

                match datatype {
                    DataType::DateTime => {
                        let dt = chrono::DateTime::parse_from_rfc3339(s)?;
                        values.push(Value::DateTime(dt));
                    }
                    DataType::String => {
                        values.push(Value::String(s.to_string()));
                    }
                    DataType::Integral => {
                        let i = s.parse::<i32>()?;
                        values.push(Value::Int(i));
                    }
                    DataType::Float => {
                        let f = s.parse::<f32>()?;
                        values.push(Value::Float(OrderedFloat::from(f)));
                    }
                    DataType::Host => {
                        if s == "-" {
                            values.push(Value::Null)
                        } else {
                            let host = common::types::parse_host(s)?;
                            values.push(Value::Host(host));
                        }
                    }
                    DataType::HttpRequest => {
                        let s = s.trim_matches('"');
                        let request = common::types::parse_http_request(s)?;
                        values.push(Value::HttpRequest(request));
                    }
                }
            }

            //Adjust the width to be the same
            while values.len() < field_names.len() {
                values.push(Value::Null);
            }

            let record = Record::new(field_names, values);

            Ok(Some(record))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common;
    use chrono;
    use std::io::BufReader;
    use std::str::FromStr;

    #[test]
    fn test_aws_elb_reader() {
        let content = r#"2015-11-07T18:45:33.559871Z elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 42355 "GET https://example.com:443/ HTTP/1.1" "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"#;
        let mut reader = ReaderBuilder::new("elb".to_string()).with_reader(BufReader::new(content.as_bytes()));
        let record = reader.read_record().unwrap();
        let fields = ClassicLoadBalancerLogField::field_names();
        let data = vec![
            Value::DateTime(chrono::DateTime::parse_from_rfc3339("2015-11-07T18:45:33.559871Z").unwrap()),
            Value::String("elb1".to_string()),
            Value::Host(common::types::parse_host("78.168.134.92:4586").unwrap()),
            Value::Host(common::types::parse_host("10.0.0.215:80").unwrap()),
            Value::Float(OrderedFloat::from(0.000_036)),
            Value::Float(OrderedFloat::from(0.001_035)),
            Value::Float(OrderedFloat::from(0.000_025)),
            Value::String("200".to_string()),
            Value::String("200".to_string()),
            Value::Int(0),
            Value::Int(42355),
            Value::HttpRequest(common::types::parse_http_request("GET https://example.com:443/ HTTP/1.1").unwrap()),
            Value::String("\"Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36\"".to_string()),
            Value::String("ECDHE-RSA-AES128-GCM-SHA256".to_string()),
            Value::String("TLSv1.2".to_string()),
            Value::Null,
            Value::Null
        ];
        let expected: Option<Record> = Some(Record::new(fields, data));

        assert_eq!(expected, record);

        let content = r#"2015-11-07T18:45:37.691548Z elb1 176.219.166.226:48384 10.0.2.143:80 0.000023 0.000348 0.000025 200 200 0 41690 "GET http://example.com:80/?mode=json&after=&iteration=1 HTTP/1.1" "Mozilla/5.0 (Linux; Android 5.1.1; Nexus 5 Build/LMY48I; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/46.0.2490.76 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/52.0.0.12.18;]" - - arn:aws:elasticloadbalancing:us-west-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337262-36d228ad5d99923122bbe354""#;
        let mut reader = ReaderBuilder::new("elb".to_string()).with_reader(BufReader::new(content.as_bytes()));
        let record = reader.read_record().unwrap();
        let fields = ClassicLoadBalancerLogField::field_names();
        let data = vec![
            Value::DateTime(chrono::DateTime::parse_from_rfc3339("2015-11-07T18:45:37.691548Z").unwrap()),
            Value::String("elb1".to_string()),
            Value::Host(common::types::parse_host("176.219.166.226:48384").unwrap()),
            Value::Host(common::types::parse_host("10.0.2.143:80").unwrap()),
            Value::Float(OrderedFloat::from(0.000_023)),
            Value::Float(OrderedFloat::from(0.000_348)),
            Value::Float(OrderedFloat::from(0.000_025)),
            Value::String("200".to_string()),
            Value::String("200".to_string()),
            Value::Int(0),
            Value::Int(41690),
            Value::HttpRequest(common::types::parse_http_request("GET http://example.com:80/?mode=json&after=&iteration=1 HTTP/1.1").unwrap()),
            Value::String("\"Mozilla/5.0 (Linux; Android 5.1.1; Nexus 5 Build/LMY48I; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/46.0.2490.76 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/52.0.0.12.18;]\"".to_string()),
            Value::String("-".to_string()),
            Value::String("-".to_string()),
            Value::String("arn:aws:elasticloadbalancing:us-west-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067".to_string()),
            Value::String("\"Root=1-58337262-36d228ad5d99923122bbe354\"".to_string()),
        ];
        let expected: Option<Record> = Some(Record::new(fields, data));

        assert_eq!(expected, record)
    }

    #[test]
    fn test_aws_alb_reader() {
        let content = r#"http 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.000 0.001 0.000 200 200 34 366 "GET http://www.example.com:80/ HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337262-36d228ad5d99923122bbe354" "-" "-" 0 2018-07-02T22:22:48.364000Z "forward" "-" "-""#;
        let mut reader = ReaderBuilder::new("alb".to_string()).with_reader(BufReader::new(content.as_bytes()));
        let record = reader.read_record().unwrap();
        let fields = ApplicationLoadBalancerLogField::field_names();
        let data = vec![
            Value::String("http".to_string()),
            Value::DateTime(chrono::DateTime::parse_from_rfc3339("2018-07-02T22:23:00.186641Z").unwrap()),
            Value::String("app/my-loadbalancer/50dc6c495c0c9188".to_string()),
            Value::Host(common::types::parse_host("192.168.131.39:2817").unwrap()),
            Value::Host(common::types::parse_host("10.0.0.1:80").unwrap()),
            Value::Float(OrderedFloat::from(0.000)),
            Value::Float(OrderedFloat::from(0.001)),
            Value::Float(OrderedFloat::from(0.000)),
            Value::String("200".to_string()),
            Value::String("200".to_string()),
            Value::Int(34),
            Value::Int(366),
            Value::HttpRequest(common::types::parse_http_request("GET http://www.example.com:80/ HTTP/1.1").unwrap()),
            Value::String("\"curl/7.46.0\"".to_string()),
            Value::String("-".to_string()),
            Value::String("-".to_string()),
            Value::String(
                "arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067"
                    .to_string(),
            ),
            Value::String("\"Root=1-58337262-36d228ad5d99923122bbe354\"".to_string()),
            Value::String("\"-\"".to_string()),
            Value::String("\"-\"".to_string()),
            Value::String("0".to_string()),
            Value::String("2018-07-02T22:22:48.364000Z".to_string()),
            Value::String("\"forward\"".to_string()),
            Value::String("\"-\"".to_string()),
            Value::String("\"-\"".to_string()),
        ];
        let expected: Option<Record> = Some(Record::new(fields, data));

        assert_eq!(expected, record);
    }

    #[test]
    fn test_aws_s3_reader() {
        let content = r#"79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be awsexamplebucket [06/Feb/2019:00:00:38 +0000] 192.0.2.3 79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be 3E57427F3EXAMPLE REST.GET.VERSIONING - "GET /awsexamplebucket?versioning HTTP/1.1" 200 - 113 - 7 - "-" "S3Console/0.4" - s9lzHYrFp76ZVxRcpX9+5cjAnEH2ROuNkd2BHfIa6UkFVdtjf5mKR3/eTPFvsiP/XV/VLi31234= SigV2 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader awsexamplebucket.s3.amazonaws.com TLSV1.1"#;
        let mut reader = ReaderBuilder::new("s3".to_string()).with_reader(BufReader::new(content.as_bytes()));
        let record = reader.read_record().unwrap();
        let fields = S3Field::field_names();
        let data = vec![
            Value::String("79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be".to_string()),
            Value::String("awsexamplebucket".to_string()),
            Value::String("[06/Feb/2019:00:00:38 +0000]".to_string()),
            Value::String("192.0.2.3".to_string()),
            Value::String("79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be".to_string()),
            Value::String("3E57427F3EXAMPLE".to_string()),
            Value::String("REST.GET.VERSIONING".to_string()),
            Value::String("-".to_string()),
            Value::String("\"GET /awsexamplebucket?versioning HTTP/1.1\"".to_string()),
            Value::String("200".to_string()),
            Value::String("-".to_string()),
            Value::String("113".to_string()),
            Value::String("-".to_string()),
            Value::String("7".to_string()),
            Value::String("-".to_string()),
            Value::String("\"-\"".to_string()),
            Value::String("\"S3Console/0.4\"".to_string()),
            Value::String("-".to_string()),
            Value::String("s9lzHYrFp76ZVxRcpX9+5cjAnEH2ROuNkd2BHfIa6UkFVdtjf5mKR3/eTPFvsiP/XV/VLi31234=".to_string()),
            Value::String("SigV2".to_string()),
            Value::String("ECDHE-RSA-AES128-GCM-SHA256".to_string()),
            Value::String("AuthHeader".to_string()),
            Value::String("awsexamplebucket.s3.amazonaws.com".to_string()),
            Value::String("TLSV1.1".to_string()),
        ];
        let expected: Option<Record> = Some(Record::new(fields, data));

        assert_eq!(expected, record);
    }

    #[test]
    fn test_squid_reader() {
        let content = r#"1515734740.494      1 [MASKEDIPADDRESS] TCP_DENIED/407 3922 CONNECT d.dropbox.com:443 - HIER_NONE/- text/html"#;
        let mut reader = ReaderBuilder::new("squid".to_string()).with_reader(BufReader::new(content.as_bytes()));
        let record = reader.read_record().unwrap();
        let fields = SquidLogField::field_names();
        let data = vec![
            Value::String("1515734740.494".to_string()),
            Value::String("1".to_string()),
            Value::String("[MASKEDIPADDRESS]".to_string()),
            Value::String("TCP_DENIED/407".to_string()),
            Value::String("3922".to_string()),
            Value::String("CONNECT".to_string()),
            Value::String("d.dropbox.com:443".to_string()),
            Value::String("-".to_string()),
            Value::String("HIER_NONE/-".to_string()),
            Value::String("text/html".to_string()),
        ];
        let expected: Option<Record> = Some(Record::new(fields, data));

        assert_eq!(expected, record);
    }

    #[test]
    fn test_reader_on_empty_input() {
        let content = r#"                   \n          "#;
        let mut reader = ReaderBuilder::new("elb".to_string()).with_reader(BufReader::new(content.as_bytes()));
        let record = reader.read_record();

        assert_eq!(record.is_err(), true)
    }

    #[test]
    fn test_reader_on_malformed_input() {
        let content = r#"2015-11-07T18:45:37.691548Z elb1 176.219.166.226:48384 10.0.2.143:80 0.000 on=1 HTTP/1.1" "Mozilla/5.0 (Linux; Android 5.137.36 (KHTML, like Gecko) Version/4.0 Chrome/46.0.2490.76 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/52.0.0.12.18;]" - - arn:aws:elasticloadbalancing:us-west-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337262-36d228ad5d99923122bbe354""#;
        let mut reader = ReaderBuilder::new("elb".to_string()).with_reader(BufReader::new(content.as_bytes()));
        let record = reader.read_record();

        assert_eq!(record.is_err(), true)
    }

    #[test]
    fn test_idempotent_property() {
        for field_name in ClassicLoadBalancerLogField::field_names().iter() {
            let field_enum = ClassicLoadBalancerLogField::from_str(field_name).unwrap();
            let format_field_name = format!("{}", field_enum);
            assert_eq!(&format_field_name, field_name)
        }
    }
}
