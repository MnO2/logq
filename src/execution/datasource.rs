use super::stream::Record;
use crate::common::types::Value;
use ordered_float::OrderedFloat;
use regex::Regex;

use std::fmt;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::path::Path;
use std::result;
use std::str::FromStr;

#[derive(PartialEq, Eq, Debug, Clone)]
pub(crate) enum DataType {
    DateTime,
    String,
    Integral,
    Float,
    Host,
    Url,
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

    pub(crate) fn datatype(idx: usize) -> DataType {
        let v = vec![
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
            DataType::String,
            DataType::String,
            DataType::String,
            DataType::String,
        ];

        v[idx].clone()
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

    #[allow(dead_code)]
    pub(crate) fn with_reader<R: io::Read>(&self, rdr: R) -> Reader<R> {
        Reader::new(self, rdr)
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

    #[allow(dead_code)]
    fn close(&self) {}
}

impl<R: io::Read> RecordRead for Reader<R> {
    fn read_record(&mut self) -> ReaderResult<Option<Record>> {
        let mut buf = String::new();
        let more_data = self.rdr.read_line(&mut buf)?;

        if more_data > 0 {
            let field_names = ClassicLoadBalancerLogField::field_names();
            let regex_literal = r#"[^\s"']+|"([^"]*)"|'([^']*)'"#;
            let split_the_line_regex: Regex = Regex::new(regex_literal).unwrap();
            //FIXME: parse to the more specific
            let mut values: Vec<Value> = Vec::new();
            for (i, m) in split_the_line_regex.find_iter(&buf).enumerate() {
                let s = m.as_str();
                let datatype = ClassicLoadBalancerLogField::datatype(i);

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
                        unimplemented!();
                    }
                    DataType::Url => {
                        unimplemented!();
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
    use std::io::BufReader;

    #[test]
    fn test_reader() {
        let content = r#"2015-11-07T18:45:33.559871Z elb1 78.168.134.92:4586 10.0.0.215:80 0.000036 0.001035 0.000025 200 200 0 42355 "GET https://example.com:443/ HTTP/1.1" "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"#;
        let mut reader = ReaderBuilder::new().with_reader(BufReader::new(content.as_bytes()));
        let record = reader.read_record().unwrap();
        let fields = ClassicLoadBalancerLogField::field_names();
        let data = vec![
            Value::String("2015-11-07T18:45:33.559871Z".to_string()),
            Value::String("elb1".to_string()),
            Value::String("78.168.134.92:4586".to_string()),
            Value::String("10.0.0.215:80".to_string()),
            Value::String("0.000036".to_string()),
            Value::String("0.001035".to_string()),
            Value::String("0.000025".to_string()),
            Value::String("200".to_string()),
            Value::String("200".to_string()),
            Value::String("0".to_string()),
            Value::String("42355".to_string()),
            Value::String("\"GET https://example.com:443/ HTTP/1.1\"".to_string()),
            Value::String("\"Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36\"".to_string()),
            Value::String("ECDHE-RSA-AES128-GCM-SHA256".to_string()),
            Value::String("TLSv1.2".to_string()),
        ];
        let expected: Option<Record> = Some(Record::new(fields, data));

        assert_eq!(expected, record);

        let content = r#"2015-11-07T18:45:37.691548Z elb1 176.219.166.226:48384 10.0.2.143:80 0.000023 0.000348 0.000025 200 200 0 41690 "GET http://example.com:80/?mode=json&after=&iteration=1 HTTP/1.1" "Mozilla/5.0 (Linux; Android 5.1.1; Nexus 5 Build/LMY48I; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/46.0.2490.76 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/52.0.0.12.18;]" - - arn:aws:elasticloadbalancing:us-west-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337262-36d228ad5d99923122bbe354""#;
        let mut reader = ReaderBuilder::new().with_reader(BufReader::new(content.as_bytes()));
        let record = reader.read_record().unwrap();
        let fields = ClassicLoadBalancerLogField::field_names();
        let data = vec![
            Value::String("2015-11-07T18:45:37.691548Z".to_string()),
            Value::String("elb1".to_string()),
            Value::String("176.219.166.226:48384".to_string()),
            Value::String("10.0.2.143:80".to_string()),
            Value::String("0.000023".to_string()),
            Value::String("0.000348".to_string()),
            Value::String("0.000025".to_string()),
            Value::String("200".to_string()),
            Value::String("200".to_string()),
            Value::String("0".to_string()),
            Value::String("41690".to_string()),
            Value::String("\"GET http://example.com:80/?mode=json&after=&iteration=1 HTTP/1.1\"".to_string()),
            Value::String("\"Mozilla/5.0 (Linux; Android 5.1.1; Nexus 5 Build/LMY48I; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/46.0.2490.76 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/52.0.0.12.18;]\"".to_string()),
            Value::String("-".to_string()),
            Value::String("-".to_string()),
            Value::String("arn:aws:elasticloadbalancing:us-west-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067".to_string()),
            Value::String("\"Root=1-58337262-36d228ad5d99923122bbe354\"".to_string()),
        ];
        let expected: Option<Record> = Some(Record::new(fields, data));

        assert_eq!(expected, record)
    }
}
