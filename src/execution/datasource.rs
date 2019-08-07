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
    fn from_i32(i: i32) -> result::Result<Self, String> {
        match i {
            0 => Ok(ClassicLoadBalancerLogField::Timestamp),
            1 => Ok(ClassicLoadBalancerLogField::Elbname),
            2 => Ok(ClassicLoadBalancerLogField::ClientAndPort),
            3 => Ok(ClassicLoadBalancerLogField::BackendAndPort),
            4 => Ok(ClassicLoadBalancerLogField::RequestProcessingTime),
            5 => Ok(ClassicLoadBalancerLogField::BackendProcessingTime),
            6 => Ok(ClassicLoadBalancerLogField::ResponseProcessingTime),
            7 => Ok(ClassicLoadBalancerLogField::ELBStatusCode),
            8 => Ok(ClassicLoadBalancerLogField::BackendStatusCode),
            9 => Ok(ClassicLoadBalancerLogField::ReceivedBytes),
            10 => Ok(ClassicLoadBalancerLogField::SentBytes),
            11 => Ok(ClassicLoadBalancerLogField::Request),
            12 => Ok(ClassicLoadBalancerLogField::UserAgent),
            13 => Ok(ClassicLoadBalancerLogField::SSLCipher),
            14 => Ok(ClassicLoadBalancerLogField::SSLProtocol),
            _ => Err("unknown column name".to_string()),
        }
    }
}

#[derive(Debug)]
pub struct StringRecord {
    pub fields: String,
    split_the_line_regex: Regex,
}

impl StringRecord {
    pub fn new() -> Self {
        let regex_literal = r#"[^\s"']+|"([^"]*)"|'([^']*)'"#;
        let split_the_line_regex: Regex = Regex::new(regex_literal).unwrap();

        StringRecord {
            fields: String::new(),
            split_the_line_regex,
        }
    }

    pub(crate) fn field_names() -> Vec<String> {
        let mut fields = Vec::new();

        for i in 0..15 {
            let f = ClassicLoadBalancerLogField::from_i32(i).unwrap();
            let field = format!("{}", f);
            fields.push(field)
        }

        fields
    }

    pub fn get(&self, i: usize) -> Option<&str> {
        let r: Vec<&str> = self
            .split_the_line_regex
            .find_iter(&self.fields)
            .map(|x| x.as_str())
            .collect();

        if i < r.len() {
            Some(r[i])
        } else {
            None
        }
    }
}

pub type Result<T> = result::Result<T, ReaderError>;

#[derive(Fail, Debug)]
pub enum ReaderError {
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
}

impl From<io::Error> for ReaderError {
    fn from(err: io::Error) -> ReaderError {
        ReaderError::Io(err)
    }
}

#[derive(Debug)]
pub struct ReaderBuilder {
    capacity: usize,
}

impl Default for ReaderBuilder {
    fn default() -> Self {
        ReaderBuilder {
            capacity: 8 * (1 << 10),
        }
    }
}

impl ReaderBuilder {
    pub fn new() -> Self {
        ReaderBuilder::default()
    }

    #[allow(dead_code, clippy::wrong_self_convention)]
    pub fn from_path<P: AsRef<Path>>(&self, path: P) -> Result<Reader<File>> {
        Ok(Reader::new(self, File::open(path)?))
    }
    #[allow(dead_code, clippy::wrong_self_convention)]
    pub fn from_reader<R: io::Read>(&self, rdr: R) -> Reader<R> {
        Reader::new(self, rdr)
    }

    #[allow(dead_code)]
    pub fn buffer_capacity(&mut self, capacity: usize) -> &mut ReaderBuilder {
        self.capacity = capacity;
        self
    }
}

#[derive(Debug)]
pub struct Reader<R> {
    rdr: io::BufReader<R>,
}

impl<R: io::Read> Reader<R> {
    fn new(builder: &ReaderBuilder, rdr: R) -> Reader<R> {
        Reader {
            rdr: io::BufReader::with_capacity(builder.capacity, rdr),
        }
    }

    pub fn from_reader(rdr: R) -> Reader<R> {
        ReaderBuilder::new().from_reader(rdr)
    }

    pub fn read_record(&mut self, record: &mut StringRecord) -> Result<bool> {
        let mut buf = String::new();

        if let Ok(num_of_bytes) = self.rdr.read_line(&mut buf) {
            if num_of_bytes == 0 {
                return Ok(false);
            }

            record.fields = buf;
            return Ok(true);
        }

        Ok(true)
    }

    fn close(&self) {}
}
