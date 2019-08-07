use super::datasource::{Reader, ReaderError};
use super::stream::{FilterStream, LogFileStream, MapStream, RecordStream};
use crate::common::types::{DataSource, Value, VariableName, Variables};
use std::fs::File;
use std::io;
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

pub(crate) type StreamGetResult<T> = result::Result<T, CreateStreamError>;

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum CreateStreamError {
    #[fail(display = "Key Not Found")]
    KeyNotFound,
    #[fail(display = "Io Error")]
    Io,
}

impl From<io::Error> for CreateStreamError {
    fn from(_: io::Error) -> CreateStreamError {
        CreateStreamError::Io
    }
}

pub(crate) type StreamResult<T> = result::Result<T, StreamError>;

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum StreamError {
    #[fail(display = "{}", _0)]
    Get(#[cause] CreateStreamError),
    #[fail(display = "{}", _0)]
    Evaluate(#[cause] EvaluateError),
    #[fail(display = "{}", _0)]
    Expression(#[cause] ExpressionError),
    #[fail(display = "Reader Error")]
    Reader,
}

impl From<CreateStreamError> for StreamError {
    fn from(err: CreateStreamError) -> StreamError {
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
    #[fail(display = "Invalid Arguments")]
    InvalidArguments,
    #[fail(display = "Unknown Function")]
    UnknownFunction,
    #[fail(display = "Invalid Star")]
    InvalidStar,
}

impl From<EvaluateError> for ExpressionError {
    fn from(_: EvaluateError) -> ExpressionError {
        ExpressionError::KeyNotFound
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Expression {
    Logic(Box<Formula>),
    Variable(VariableName),
    Function(String, Vec<Named>),
}

impl Expression {
    pub(crate) fn expression_value(&self, variables: Variables) -> ExpressionResult<Value> {
        match self {
            Expression::Logic(formula) => {
                let out = formula.evaluate(variables)?;
                Ok(Value::Boolean(out))
            }
            Expression::Variable(name) => {
                if let Some(v) = variables.get(name) {
                    Ok(v.clone())
                } else {
                    Err(ExpressionError::KeyNotFound)
                }
            }
            Expression::Function(name, arguments) => {
                let mut values: Vec<Value> = Vec::new();
                for arg in arguments.iter() {
                    match arg {
                        Named::Expression(expr, _) => {
                            let value = expr.expression_value(variables.clone())?;
                            values.push(value);
                        }
                        Named::Star => {
                            return Err(ExpressionError::InvalidStar);
                        }
                    }
                }

                let return_value: Value = evaluate(&*name, &values)?;
                Ok(return_value)
            }
        }
    }
}

fn evaluate(func_name: &str, arguments: &[Value]) -> ExpressionResult<Value> {
    match func_name {
        "Plus" => {
            if arguments.len() != 2 {
                return Err(ExpressionError::InvalidArguments);
            }

            match (&arguments[0], &arguments[1]) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        "Minus" => {
            if arguments.len() != 2 {
                return Err(ExpressionError::InvalidArguments);
            }

            match (&arguments[0], &arguments[1]) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        "Times" => {
            if arguments.len() != 2 {
                return Err(ExpressionError::InvalidArguments);
            }

            match (&arguments[0], &arguments[1]) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        "Divide" => {
            if arguments.len() != 2 {
                return Err(ExpressionError::InvalidArguments);
            }

            match (&arguments[0], &arguments[1]) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a / b)),
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        _ => Err(ExpressionError::InvalidArguments),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Relation {
    Equal,
    NotEqual,
    MoreThan,
    LessThan,
    GreaterEqual,
    LessEqual,
}

impl Relation {
    pub(crate) fn apply(&self, variables: Variables, left: &Expression, right: &Expression) -> ExpressionResult<bool> {
        let left_result = left.expression_value(variables.clone())?;
        let right_result = right.expression_value(variables.clone())?;

        match self {
            Relation::Equal => Ok(left_result == right_result),
            Relation::NotEqual => Ok(left_result != right_result),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Named {
    Expression(Expression, Option<VariableName>),
    Star,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Formula {
    Constant(bool),
    And(Box<Formula>, Box<Formula>),
    Or(Box<Formula>, Box<Formula>),
    Not(Box<Formula>),
    Predicate(Relation, Box<Expression>, Box<Expression>),
}

impl Formula {
    pub(crate) fn evaluate(&self, variables: Variables) -> EvaluateResult<bool> {
        match self {
            Formula::And(left_formula, right_formula) => {
                let left = left_formula.evaluate(variables.clone())?;
                let right = right_formula.evaluate(variables.clone())?;
                Ok(left && right)
            }
            Formula::Or(left_formula, right_formula) => {
                let left = left_formula.evaluate(variables.clone())?;
                let right = right_formula.evaluate(variables.clone())?;
                Ok(left || right)
            }
            Formula::Not(child_formula) => {
                let child = child_formula.evaluate(variables.clone())?;
                Ok(!child)
            }
            Formula::Predicate(relation, left_formula, right_formula) => {
                let result = relation.apply(variables, left_formula, right_formula)?;
                Ok(result)
            }
            Formula::Constant(value) => Ok(*value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Node {
    DataSource(DataSource),
    Filter(Box<Node>, Box<Formula>),
    Map(Vec<Named>, Box<Node>),
    GroupBy(Vec<VariableName>, Vec<Aggregate>, Box<Node>),
}

impl Node {
    pub(crate) fn get(&self, variables: Variables) -> StreamGetResult<Box<dyn RecordStream>> {
        match self {
            Node::Filter(source, formula) => {
                let record_stream = source.get(variables.clone())?;
                let stream = FilterStream::new(*formula.clone(), variables, record_stream);
                Ok(Box::new(stream))
            }
            Node::Map(named_list, source) => {
                let record_stream = source.get(variables.clone())?;

                let stream = MapStream::new(named_list.clone(), variables, record_stream);

                Ok(Box::new(stream))
            }
            Node::DataSource(data_source) => match data_source {
                DataSource::File(path) => {
                    let f = File::open(path)?;
                    let reader = Reader::from_reader(f);
                    let stream = LogFileStream { reader };

                    Ok(Box::new(stream))
                }
                DataSource::Stdin => {
                    unimplemented!();
                }
            },
            Node::GroupBy(_, _, _) => {
                unimplemented!();
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AggregateFunction {
    Avg,
    Count,
    First,
    Last,
    Max,
    Min,
    Sum,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Aggregate {
    pub(crate) aggregate_func: AggregateFunction,
    pub(crate) argument: Named,
}

impl Aggregate {
    pub(crate) fn new(aggregate_func: AggregateFunction, argument: Named) -> Self {
        Aggregate {
            aggregate_func,
            argument,
        }
    }
}
