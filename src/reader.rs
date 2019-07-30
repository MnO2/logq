use crate::string_record::StringRecord;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::path::Path;
use std::result;

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
}
