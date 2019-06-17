use std::io;
use std::io::BufRead;
use std::fmt;
use std::fs::File;
use std::path::Path;
use std::result;
use regex::Regex;
use crate::classic_load_balancer_log_record::ClassicLoadBalancerLogRecord;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub struct Error(Box<ErrorKind>);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self.0 { 
            ErrorKind::Io(ref err) => err.fmt(f)
        }
    }
}

pub fn new_error(kind: ErrorKind) -> Error {
    Error(Box::new(kind))
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        new_error(ErrorKind::Io(err))
    }
}

#[derive(Debug)]
pub enum ErrorKind {
    Io(io::Error)
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

    pub fn from_path<P: AsRef<Path>>(&self, path: P) -> Result<Reader<File>> {
        Ok(Reader::new(self, File::open(path)?))
    }

    pub fn from_reader<R: io::Read>(&self, rdr: R) -> Reader<R> {
        Reader::new(self, rdr)
    }

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
            rdr: io::BufReader::with_capacity(builder.capacity, rdr)
        }
    }

    pub fn from_reader(rdr: R) -> Reader<R> {
        ReaderBuilder::new().from_reader(rdr)
    }

    pub fn read_record(&mut self, record: &mut ClassicLoadBalancerLogRecord) -> Result<bool> {
        let regex_literal = r#"[^\s"']+|\"([^"]*)"|'([^']*)'"#;
        let split_the_line_regex: Regex = Regex::new(regex_literal).unwrap();
        let mut buf = String::new();

        if let Ok(_) = self.rdr.read_line(&mut buf) {
            let r: Vec<&str> = split_the_line_regex.find_iter(&buf).map(|x| x.as_str()).collect();
            record.timestamp = String::from(r[0]); 
        }

        Ok(true)
    }
}
