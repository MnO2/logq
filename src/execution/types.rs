use super::datasource::{ReaderBuilder, ReaderError};
use super::stream::{FilterStream, LogFileStream, MapStream, RecordStream};
use crate::common::types::{DataSource, Tuple, Value, VariableName, Variables};
use hashbrown::HashMap;
use ordered_float::OrderedFloat;
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

pub(crate) type CreateStreamResult<T> = result::Result<T, CreateStreamError>;

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum CreateStreamError {
    #[fail(display = "Key Not Found")]
    KeyNotFound,
    #[fail(display = "Io Error")]
    Io,
    #[fail(display = "Reader Error")]
    Reader,
}

impl From<io::Error> for CreateStreamError {
    fn from(_: io::Error) -> CreateStreamError {
        CreateStreamError::Io
    }
}

impl From<ReaderError> for CreateStreamError {
    fn from(_: ReaderError) -> CreateStreamError {
        CreateStreamError::Reader
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
    pub(crate) fn get(&self, variables: Variables) -> CreateStreamResult<Box<dyn RecordStream>> {
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
                DataSource::ClassicLoadBalancerLogFile(path) => {
                    let reader = ReaderBuilder::new().with_path(path)?;
                    let stream = LogFileStream {
                        reader: Box::new(reader),
                    };

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

pub(crate) type AggregateResult<T> = result::Result<T, AggregateError>;

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum AggregateError {
    #[fail(display = "Key Not Found")]
    KeyNotFound,
    #[fail(display = "Invalid Type")]
    InvalidType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Aggregate {
    Avg(AvgAggregate),
    Count(CountAggregate),
    First,
    Last,
    Max,
    Min,
    Sum,
}

impl Aggregate {
    pub(crate) fn add_record(&mut self, key: Tuple, value: Value) -> AggregateResult<()> {
        match self {
            Aggregate::Avg(agg) => agg.add_record(key, value),
            Aggregate::Count(agg) => agg.add_record(key, value),
            _ => unimplemented!(),
        }
    }
    pub(crate) fn get_aggregated(&self, key: &Tuple) -> AggregateResult<Value> {
        match self {
            Aggregate::Avg(agg) => agg.get_aggregated(key),
            Aggregate::Count(agg) => agg.get_aggregated(key),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AvgAggregate {
    pub(crate) averages: HashMap<Tuple, OrderedFloat<f32>>,
    pub(crate) counts: HashMap<Tuple, i64>,
}

impl AvgAggregate {
    pub(crate) fn new() -> Self {
        AvgAggregate {
            averages: HashMap::new(),
            counts: HashMap::new(),
        }
    }

    pub(crate) fn add_record(&mut self, key: Tuple, value: Value) -> AggregateResult<()> {
        let newValue: OrderedFloat<f32> = match value {
            Value::Int(i) => OrderedFloat::from(i as f32),
            Value::Float(f) => f,
            _ => {
                return Err(AggregateError::InvalidType);
            }
        };

        if let (Some(&average), Some(&count)) = (self.averages.get(&key), self.counts.get(&key)) {
            let new_count = count + 1;
            let f32_average: f32 = average.into();
            let f32_new_value: f32 = newValue.into();
            let new_average: f32 = (f32_average * (count as f32) + f32_new_value) / (new_count as f32);
            self.averages.insert(key.clone(), OrderedFloat::from(new_average));
            self.counts.insert(key.clone(), new_count);
            Ok(())
        } else {
            self.averages.insert(key.clone(), newValue);
            self.counts.insert(key.clone(), 1);

            Ok(())
        }
    }

    pub(crate) fn get_aggregated(&self, key: &Tuple) -> AggregateResult<Value> {
        if let Some(&average) = self.averages.get(key) {
            Ok(Value::Float(average))
        } else {
            Err(AggregateError::KeyNotFound)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CountAggregate {
    pub(crate) counts: HashMap<Tuple, i64>,
}

impl CountAggregate {
    pub(crate) fn new() -> Self {
        CountAggregate { counts: HashMap::new() }
    }

    pub(crate) fn add_record(&mut self, key: Tuple, value: Value) -> AggregateResult<()> {
        let newValue: OrderedFloat<f32> = match value {
            Value::Int(i) => OrderedFloat::from(i as f32),
            Value::Float(f) => f,
            _ => {
                return Err(AggregateError::InvalidType);
            }
        };

        if let Some(&count) = self.counts.get(&key) {
            let newCount = count + 1;
            self.counts.insert(key.clone(), newCount);
            Ok(())
        } else {
            self.counts.insert(key.clone(), 1);

            Ok(())
        }
    }

    pub(crate) fn get_aggregated(&self, key: &Tuple) -> AggregateResult<Value> {
        if let Some(&counts) = self.counts.get(key) {
            Ok(Value::Int(counts as i32))
        } else {
            Err(AggregateError::KeyNotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_avg_iterator_with_one_element() {
        let mut iter = Aggregate::Avg(AvgAggregate::new());
        let tuple = vec![Value::String("key".to_string())];
        let value = Value::Float(OrderedFloat::from(5.0));

        iter.add_record(tuple.clone(), value.clone());
        let aggregate = iter.get_aggregated(&tuple);
        assert_eq!(Ok(value), aggregate);
    }
}
