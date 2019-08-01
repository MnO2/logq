use super::datasource::{LogFileStream, Reader, ReaderError};
use crate::common::types::{Value, VariableName, Variables};
use std::cell::RefCell;
use std::fs::File;
use std::io;
use std::rc::Rc;
use std::result;

pub(crate) type EvaluateResult<T> = result::Result<T, EvaluateError>;

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum EvaluateError {
    #[fail(display = "Key Not Found")]
    KeyNotFound,
    #[fail(display = "{}", _0)]
    Expression(#[cause] ExpressionError),
}

impl From<ExpressionError> for EvaluateError {
    fn from(err: ExpressionError) -> EvaluateError {
        EvaluateError::Expression(err)
    }
}

pub(crate) type GetResult<T> = result::Result<T, GetError>;

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum GetError {
    #[fail(display = "Key Not Found")]
    KeyNotFound,
    #[fail(display = "Io Error")]
    Io,
}

impl From<io::Error> for GetError {
    fn from(err: io::Error) -> GetError {
        GetError::Io
    }
}

pub(crate) type StreamResult<T> = result::Result<T, StreamError>;

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum StreamError {
    #[fail(display = "{}", _0)]
    Get(#[cause] GetError),
    #[fail(display = "{}", _0)]
    Evaluate(#[cause] EvaluateError),
    #[fail(display = "{}", _0)]
    Expression(#[cause] ExpressionError),
    #[fail(display = "Reader Error")]
    Reader,
    #[fail(display = "End Of Stream")]
    EndOfStream,
}

impl From<GetError> for StreamError {
    fn from(err: GetError) -> StreamError {
        StreamError::Get(err)
    }
}

impl From<EvaluateError> for StreamError {
    fn from(err: EvaluateError) -> StreamError {
        StreamError::Evaluate(err)
    }
}

impl From<ExpressionError> for StreamError {
    fn from(err: ExpressionError) -> StreamError {
        StreamError::Expression(err)
    }
}

impl From<ReaderError> for StreamError {
    fn from(_: ReaderError) -> StreamError {
        StreamError::Reader
    }
}

pub(crate) type ExpressionResult<T> = result::Result<T, ExpressionError>;

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum ExpressionError {
    #[fail(display = "Key Not Found")]
    KeyNotFound,
}

pub(crate) trait Expression {
    fn expression_value(&self, variables: Variables) -> ExpressionResult<Value>;
}

pub(crate) trait NamedExpression: Expression {
    fn name(&self) -> VariableName;
}

pub(crate) struct Variable {
    name: VariableName,
}

impl Variable {
    pub(crate) fn new(name: String) -> Self {
        Variable { name }
    }
}

impl Expression for Variable {
    fn expression_value(&self, variables: Variables) -> ExpressionResult<Value> {
        if let Some(v) = variables.get(&self.name) {
            Ok(v.clone())
        } else {
            Err(ExpressionError::KeyNotFound)
        }
    }
}

pub(crate) trait Formula {
    fn evaluate(&self, variables: Variables) -> EvaluateResult<bool>;
}

pub(crate) trait Node {
    fn get(&self, variables: Variables) -> GetResult<Box<dyn RecordStream>>;
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Record {
    field_names: Vec<VariableName>,
    data: Vec<Value>,
}

impl Record {
    pub(crate) fn new(field_names: Vec<VariableName>, data: Vec<Value>) -> Self {
        Record { field_names, data }
    }
}

pub(crate) trait RecordStream {
    fn next(&mut self) -> StreamResult<Record>;
    fn close(&self);
}

#[derive(Debug)]
pub struct DataSource {
    rdr: Rc<RefCell<Reader<File>>>,
}

impl Node for DataSource {
    fn get(&self, variables: Variables) -> GetResult<Box<dyn RecordStream>> {
        let stream = LogFileStream { rdr: self.rdr.clone() };

        Ok(Box::new(stream))
    }
}
