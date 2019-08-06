use super::datasource::{LogFileStream, Reader, ReaderError};
use super::filter::FilteredStream;
use super::map::MappedStream;
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

impl From<EvaluateError> for ExpressionError {
    fn from(_: EvaluateError) -> ExpressionError {
        ExpressionError::KeyNotFound
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Expression {
    LogicExpression(Box<Formula>),
    Variable(VariableName),
    FunctionExpression(String, Box<Vec<Expression>>),
}

impl Expression {
    pub(crate) fn expression_value(&self, variables: Variables) -> ExpressionResult<Value> {
        match self {
            Expression::LogicExpression(formula) => {
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
            Expression::FunctionExpression(name, args) => {
                unimplemented!();
            }
        }
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
    pub(crate) fn apply(
        &self,
        variables: Variables,
        left: &Box<Expression>,
        right: &Box<Expression>,
    ) -> ExpressionResult<bool> {
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
    Expression(Expression, VariableName),
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
    DataSource(String),
    Filter(Box<Node>, Box<Formula>),
    Map(Vec<Named>, Box<Node>),
    GroupBy(Vec<VariableName>, Vec<Aggregate>, Box<Node>),
}

impl Node {
    fn get(&self, variables: Variables) -> GetResult<Box<dyn RecordStream>> {
        match self {
            Node::Filter(source, formula) => {
                let record_stream = source.get(variables.clone())?;
                let rc_formula = Rc::from(formula.clone());
                let stream = FilteredStream::new(rc_formula, variables, record_stream);
                Ok(Box::new(stream))
            }
            Node::Map(named_list, source) => {
                let record_stream = source.get(variables.clone())?;

                let stream = MappedStream {
                    named_list: named_list.clone(),
                    variables,
                    source: record_stream,
                };

                Ok(Box::new(stream))
            }
            Node::DataSource(filename) => {
                let f = File::open(filename)?;
                let mut reader = Reader::from_reader(f);
                let rdr = Rc::new(RefCell::new(reader));
                let stream = LogFileStream { rdr };

                Ok(Box::new(stream))
            }
            Node::GroupBy(fields, aggregates, source) => {
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
