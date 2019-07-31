use hashbrown::HashMap;
use ordered_float::OrderedFloat;
use std::result;
use std::io;
use crate::datasource::reader::ReaderError;

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub(crate) enum Value {
    Int(i64),
    Float(OrderedFloat<f64>),
    String(String),
    Null,
}

pub(crate) type Tuple = Vec<Value>;
pub(crate) type VariableName = String;
pub(crate) type Variables = HashMap<VariableName, Value>;

pub(crate) type EvaluateResult<T> = result::Result<T, EvaluateError>;

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum EvaluateError {
    #[fail(display = "Key Not Found")]
    KeyNotFound,
}

pub(crate) type GetResult<T> = result::Result<T, GetError>;

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum GetError {
    #[fail(display = "Key Not Found")]
    KeyNotFound,
    #[fail(display = "Io Error")]
    Io
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

pub(crate) trait Formula {
    fn evaluate(&self, variables: Variables) -> EvaluateResult<bool>;
}

pub(crate) trait Node {
    fn get(&self, variables: Variables) -> GetResult<Box<dyn RecordStream>>;
}

pub(crate) struct Record {
    pub(crate) field_names: Vec<VariableName>,
    pub(crate) data: Vec<Value>,
}

pub(crate) trait RecordStream {
    fn next(&mut self) -> StreamResult<Record>;
    fn close(&self);
}
