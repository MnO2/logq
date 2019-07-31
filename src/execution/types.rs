use ordered_float::OrderedFloat;
use hashbrown::HashMap;
use std::result;

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
}

pub(crate) type StreamResult<T> = result::Result<T, StreamError>;

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum StreamError {
    #[fail(display = "{}", _0)]
    Get(#[cause] GetError),
    #[fail(display = "{}", _0)]
    Evaluate(#[cause] EvaluateError),
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

pub(crate) trait Formula {
    fn evaluate(&self, variables: Variables) -> EvaluateResult<bool>;
}

pub(crate) trait Node {
    fn get(&self, variables: Variables) -> GetResult<Box<dyn RecordStream>>;
}

pub(crate) struct Record {
    field_names: Vec<VariableName>,
    data: Vec<Value>,
}

pub(crate) trait RecordStream {
    fn next(&self) -> StreamResult<Record>;
    fn close(&self);
}