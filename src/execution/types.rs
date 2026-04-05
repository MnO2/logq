use super::datasource::{ReaderBuilder, ReaderError};
use super::stream::{CrossJoinStream, DistinctStream, ExceptStream, FilterStream, GroupByStream, InMemoryStream, IntersectStream, LeftJoinStream, LimitStream, LogFileStream, MapStream, RecordStream, UnionStream};
use crate::common;
use crate::common::types::{DataSource, Tuple, Value, VariableName, Variables};
use crate::execution::stream::ProjectionStream;
use crate::functions::FunctionRegistry;
use crate::syntax::ast::{CastType, PathExpr};
use hashbrown::HashMap;
use ordered_float::OrderedFloat;
use pdatastructs::hyperloglog::HyperLogLog;
use std::collections::VecDeque;
use std::io;
use std::result;
use std::sync::Arc;
use tdigest::TDigest;

pub type EvaluateResult<T> = result::Result<T, EvaluateError>;

#[derive(thiserror::Error, PartialEq, Eq, Debug)]
pub enum EvaluateError {
    #[error("{0}")]
    Expression(#[from] ExpressionError),
}

pub(crate) type CreateStreamResult<T> = result::Result<T, CreateStreamError>;

#[derive(thiserror::Error, PartialEq, Eq, Debug)]
pub enum CreateStreamError {
    #[error("Io Error")]
    Io,
    #[error("Reader Error")]
    Reader,
    #[error("Stream Error")]
    Stream,
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

impl From<StreamError> for CreateStreamError {
    fn from(_: StreamError) -> CreateStreamError {
        CreateStreamError::Stream
    }
}

pub type StreamResult<T> = result::Result<T, StreamError>;

#[derive(thiserror::Error, PartialEq, Eq, Debug)]
pub enum StreamError {
    #[error("{0}")]
    Get(#[from] CreateStreamError),
    #[error("{0}")]
    Evaluate(#[from] EvaluateError),
    #[error("{0}")]
    Expression(#[from] ExpressionError),
    #[error("Reader Error")]
    Reader,
    #[error("Aggregate Error")]
    Aggregate,
}

impl From<ReaderError> for StreamError {
    fn from(_: ReaderError) -> StreamError {
        StreamError::Reader
    }
}

impl From<AggregateError> for StreamError {
    fn from(_: AggregateError) -> StreamError {
        StreamError::Aggregate
    }
}

pub type ExpressionResult<T> = result::Result<T, ExpressionError>;

#[derive(thiserror::Error, PartialEq, Eq, Debug)]
pub enum ExpressionError {
    #[error("Key Not Found")]
    KeyNotFound,
    #[error("Invalid Arguments")]
    InvalidArguments,
    #[error("Unknown Function")]
    UnknownFunction,
    #[error("Invalid Star")]
    InvalidStar,
    #[error("Missing Else")]
    MissingElse,
    #[error("Type Mismatch")]
    TypeMismatch,
    #[error("{0}")]
    ParseTimeInterval(#[from] common::types::ParseTimeIntervalError),
    #[error("TimeInterval Not Supported Yet")]
    TimeIntervalNotSupported,
    #[error("Zero TimeInterval")]
    TimeIntervalZero,
    #[error("DatePartUnit Not Supported Yet")]
    DatePartUnitNotSupported,
    #[error("{0}")]
    ParseDatePart(#[from] common::types::ParseDatePartError),
}

impl From<EvaluateError> for ExpressionError {
    fn from(_: EvaluateError) -> ExpressionError {
        ExpressionError::KeyNotFound
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Ordering {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expression {
    Constant(Value),
    Logic(Box<Formula>),
    Variable(PathExpr),
    Function(String, Vec<Named>),
    Branch(Vec<(Box<Formula>, Box<Expression>)>, Option<Box<Expression>>),
    Cast(Box<Expression>, CastType),
    Subquery(Box<Node>),
}

impl Expression {
    pub(crate) fn expression_value(&self, variables: &Variables, registry: &Arc<FunctionRegistry>) -> ExpressionResult<Value> {
        match self {
            Expression::Constant(value) => Ok(value.clone()),
            Expression::Logic(formula) => {
                let out = formula.evaluate(variables, registry)?;
                match out {
                    Some(b) => Ok(Value::Boolean(b)),
                    None => Ok(Value::Null),
                }
            }
            Expression::Variable(path_expr) => {
                let v = common::types::get_value_by_path_expr(path_expr, 0, variables);
                Ok(v.clone())
            }
            Expression::Function(name, arguments) => {
                let mut values: Vec<Value> = Vec::new();
                for arg in arguments.iter() {
                    match arg {
                        Named::Expression(expr, _) => {
                            let value = expr.expression_value(&variables, registry)?;
                            values.push(value);
                        }
                        Named::Star => {
                            return Err(ExpressionError::InvalidStar);
                        }
                    }
                }

                let return_value: Value = registry.call(name, &values)?;
                Ok(return_value)
            }
            Expression::Branch(branches, else_expr) => {
                for (formula, then_expr) in branches {
                    let result = formula.evaluate(variables, registry)?;
                    if result == Some(true) {
                        return then_expr.expression_value(variables, registry);
                    }
                }
                match else_expr {
                    Some(expr) => expr.expression_value(variables, registry),
                    None => Ok(Value::Null),
                }
            }
            Expression::Cast(inner, cast_type) => {
                let val = inner.expression_value(variables, registry)?;
                match (&val, cast_type) {
                    (Value::Null, _) => Ok(Value::Null),
                    (Value::Missing, _) => Ok(Value::Missing),
                    // To Int
                    (Value::Int(_), CastType::Int) => Ok(val),
                    (Value::Float(f), CastType::Int) => Ok(Value::Int(f.into_inner() as i32)),
                    (Value::String(s), CastType::Int) => {
                        s.parse::<i32>().map(Value::Int).map_err(|_| ExpressionError::TypeMismatch)
                    }
                    (Value::Boolean(b), CastType::Int) => Ok(Value::Int(if *b { 1 } else { 0 })),
                    // To Float
                    (Value::Float(_), CastType::Float) => Ok(val),
                    (Value::Int(i), CastType::Float) => Ok(Value::Float(OrderedFloat::from(*i as f32))),
                    (Value::String(s), CastType::Float) => {
                        s.parse::<f32>().map(|f| Value::Float(OrderedFloat::from(f))).map_err(|_| ExpressionError::TypeMismatch)
                    }
                    // To String (Varchar)
                    (Value::String(_), CastType::Varchar) => Ok(val),
                    (Value::Int(i), CastType::Varchar) => Ok(Value::String(i.to_string())),
                    (Value::Float(f), CastType::Varchar) => Ok(Value::String(f.to_string())),
                    (Value::Boolean(b), CastType::Varchar) => Ok(Value::String(b.to_string())),
                    // To Boolean
                    (Value::Boolean(_), CastType::Boolean) => Ok(val),
                    (Value::String(s), CastType::Boolean) => {
                        match s.to_lowercase().as_str() {
                            "true" => Ok(Value::Boolean(true)),
                            "false" => Ok(Value::Boolean(false)),
                            _ => Err(ExpressionError::TypeMismatch),
                        }
                    }
                    _ => Err(ExpressionError::TypeMismatch),
                }
            }
            Expression::Subquery(node) => {
                let mut stream = node.get(variables.clone(), registry.clone())
                    .map_err(|_| ExpressionError::InvalidArguments)?;
                if let Some(record) = stream.next()
                    .map_err(|_| ExpressionError::InvalidArguments)? {
                    let tuples = record.to_tuples();
                    if let Some((_, val)) = tuples.into_iter().next() {
                        Ok(val)
                    } else {
                        Ok(Value::Null)
                    }
                } else {
                    Ok(Value::Null)
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Relation {
    Equal,
    NotEqual,
    MoreThan,
    LessThan,
    GreaterEqual,
    LessEqual,
}

impl Relation {
    pub(crate) fn apply(&self, variables: &Variables, left: &Expression, right: &Expression, registry: &Arc<FunctionRegistry>) -> ExpressionResult<Option<bool>> {
        let left_result = left.expression_value(variables, registry)?;
        let right_result = right.expression_value(variables, registry)?;

        // NULL/MISSING: any comparison involving Null or Missing returns None (unknown)
        if matches!(&left_result, Value::Null | Value::Missing) || matches!(&right_result, Value::Null | Value::Missing) {
            return Ok(None);
        }

        match self {
            Relation::Equal => Ok(Some(left_result == right_result)),
            Relation::NotEqual => Ok(Some(left_result != right_result)),
            Relation::GreaterEqual => match (left_result, right_result) {
                (Value::Int(l), Value::Int(r)) => Ok(Some(l >= r)),
                (Value::Float(l), Value::Float(r)) => Ok(Some(l >= r)),
                (Value::Int(l), Value::Float(r)) => Ok(Some(OrderedFloat::from(l as f32) >= r)),
                (Value::Float(l), Value::Int(r)) => Ok(Some(l >= OrderedFloat::from(r as f32))),
                (Value::String(l), Value::String(r)) => Ok(Some(l >= r)),
                (Value::DateTime(l), Value::DateTime(r)) => Ok(Some(l >= r)),
                _ => Err(ExpressionError::TypeMismatch),
            },
            Relation::LessEqual => match (left_result, right_result) {
                (Value::Int(l), Value::Int(r)) => Ok(Some(l <= r)),
                (Value::Float(l), Value::Float(r)) => Ok(Some(l <= r)),
                (Value::Int(l), Value::Float(r)) => Ok(Some(OrderedFloat::from(l as f32) <= r)),
                (Value::Float(l), Value::Int(r)) => Ok(Some(l <= OrderedFloat::from(r as f32))),
                (Value::String(l), Value::String(r)) => Ok(Some(l <= r)),
                (Value::DateTime(l), Value::DateTime(r)) => Ok(Some(l <= r)),
                _ => Err(ExpressionError::TypeMismatch),
            },
            Relation::MoreThan => match (left_result, right_result) {
                (Value::Int(l), Value::Int(r)) => Ok(Some(l > r)),
                (Value::Float(l), Value::Float(r)) => Ok(Some(l > r)),
                (Value::Int(l), Value::Float(r)) => Ok(Some(OrderedFloat::from(l as f32) > r)),
                (Value::Float(l), Value::Int(r)) => Ok(Some(l > OrderedFloat::from(r as f32))),
                (Value::String(l), Value::String(r)) => Ok(Some(l > r)),
                (Value::DateTime(l), Value::DateTime(r)) => Ok(Some(l > r)),
                _ => Err(ExpressionError::TypeMismatch),
            },
            Relation::LessThan => match (left_result, right_result) {
                (Value::Int(l), Value::Int(r)) => Ok(Some(l < r)),
                (Value::Float(l), Value::Float(r)) => Ok(Some(l < r)),
                (Value::Int(l), Value::Float(r)) => Ok(Some(OrderedFloat::from(l as f32) < r)),
                (Value::Float(l), Value::Int(r)) => Ok(Some(l < OrderedFloat::from(r as f32))),
                (Value::String(l), Value::String(r)) => Ok(Some(l < r)),
                (Value::DateTime(l), Value::DateTime(r)) => Ok(Some(l < r)),
                _ => Err(ExpressionError::TypeMismatch),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Named {
    Expression(Expression, Option<VariableName>),
    Star,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Formula {
    Constant(bool),
    And(Box<Formula>, Box<Formula>),
    Or(Box<Formula>, Box<Formula>),
    Not(Box<Formula>),
    Predicate(Relation, Box<Expression>, Box<Expression>),
    IsNull(Box<Expression>),
    IsNotNull(Box<Expression>),
    IsMissing(Box<Expression>),
    IsNotMissing(Box<Expression>),
    ExpressionPredicate(Box<Expression>),
    Like(Box<Expression>, Box<Expression>),
    NotLike(Box<Expression>, Box<Expression>),
    In(Box<Expression>, Vec<Expression>),
    NotIn(Box<Expression>, Vec<Expression>),
}

fn like_pattern_to_regex(pattern: &str) -> String {
    let mut regex = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            // Escape regex special characters
            '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\' => {
                regex.push('\\');
                regex.push(ch);
            }
            _ => regex.push(ch),
        }
    }
    regex.push('$');
    regex
}

impl Formula {
    pub(crate) fn evaluate(&self, variables: &Variables, registry: &Arc<FunctionRegistry>) -> EvaluateResult<Option<bool>> {
        match self {
            Formula::And(left_formula, right_formula) => {
                let left = left_formula.evaluate(variables, registry)?;
                let right = right_formula.evaluate(variables, registry)?;
                // Three-valued AND: false dominates unknown
                match (left, right) {
                    (Some(false), _) | (_, Some(false)) => Ok(Some(false)),
                    (Some(true), Some(true)) => Ok(Some(true)),
                    _ => Ok(None),
                }
            }
            Formula::Or(left_formula, right_formula) => {
                let left = left_formula.evaluate(variables, registry)?;
                let right = right_formula.evaluate(variables, registry)?;
                // Three-valued OR: true dominates unknown
                match (left, right) {
                    (Some(true), _) | (_, Some(true)) => Ok(Some(true)),
                    (Some(false), Some(false)) => Ok(Some(false)),
                    _ => Ok(None),
                }
            }
            Formula::Not(child_formula) => {
                let child = child_formula.evaluate(variables, registry)?;
                Ok(child.map(|b| !b))
            }
            Formula::Predicate(relation, left_formula, right_formula) => {
                let result = relation.apply(variables, left_formula, right_formula, registry)?;
                Ok(result)
            }
            Formula::Constant(value) => Ok(Some(*value)),
            Formula::IsNull(expr) => {
                let val = expr.expression_value(variables, registry)?;
                Ok(Some(val == Value::Null))
            }
            Formula::IsNotNull(expr) => {
                let val = expr.expression_value(variables, registry)?;
                Ok(Some(val != Value::Null))
            }
            Formula::IsMissing(expr) => {
                let val = expr.expression_value(variables, registry)?;
                Ok(Some(val == Value::Missing))
            }
            Formula::IsNotMissing(expr) => {
                let val = expr.expression_value(variables, registry)?;
                Ok(Some(val != Value::Missing))
            }
            Formula::ExpressionPredicate(expr) => {
                let val = expr.expression_value(variables, registry)?;
                match val {
                    Value::Boolean(b) => Ok(Some(b)),
                    Value::Null | Value::Missing => Ok(None),
                    _ => Ok(Some(true)), // non-null, non-missing values are truthy
                }
            }
            Formula::Like(expr, pattern_expr) => {
                let val = expr.expression_value(variables, registry)?;
                let pattern = pattern_expr.expression_value(variables, registry)?;
                match (&val, &pattern) {
                    (Value::Null, _) | (_, Value::Null) => Ok(None),
                    (Value::Missing, _) | (_, Value::Missing) => Ok(None),
                    (Value::String(s), Value::String(p)) => {
                        let regex_pattern = like_pattern_to_regex(p);
                        let re = regex::Regex::new(&regex_pattern).map_err(|_| EvaluateError::Expression(ExpressionError::TypeMismatch))?;
                        Ok(Some(re.is_match(s)))
                    }
                    _ => Err(EvaluateError::Expression(ExpressionError::TypeMismatch)),
                }
            }
            Formula::NotLike(expr, pattern_expr) => {
                let val = expr.expression_value(variables, registry)?;
                let pattern = pattern_expr.expression_value(variables, registry)?;
                match (&val, &pattern) {
                    (Value::Null, _) | (_, Value::Null) => Ok(None),
                    (Value::Missing, _) | (_, Value::Missing) => Ok(None),
                    (Value::String(s), Value::String(p)) => {
                        let regex_pattern = like_pattern_to_regex(p);
                        let re = regex::Regex::new(&regex_pattern).map_err(|_| EvaluateError::Expression(ExpressionError::TypeMismatch))?;
                        Ok(Some(!re.is_match(s)))
                    }
                    _ => Err(EvaluateError::Expression(ExpressionError::TypeMismatch)),
                }
            }
            Formula::In(expr, list) => {
                let val = expr.expression_value(variables, registry)?;
                match &val {
                    Value::Null | Value::Missing => Ok(None),
                    _ => {
                        let mut has_null = false;
                        for item_expr in list {
                            let item = item_expr.expression_value(variables, registry)?;
                            if item == Value::Null || item == Value::Missing {
                                has_null = true;
                                continue;
                            }
                            if val == item {
                                return Ok(Some(true));
                            }
                        }
                        if has_null {
                            Ok(None)
                        } else {
                            Ok(Some(false))
                        }
                    }
                }
            }
            Formula::NotIn(expr, list) => {
                // NOT IN is the negation of IN
                let in_result = Formula::In(expr.clone(), list.clone()).evaluate(variables, registry)?;
                Ok(in_result.map(|b| !b))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Node {
    DataSource(DataSource, Vec<common::types::Binding>),
    Filter(Box<Node>, Box<Formula>),
    Map(Vec<Named>, Box<Node>),
    GroupBy(Vec<PathExpr>, Vec<NamedAggregate>, Box<Node>),
    Limit(u32, Box<Node>),
    OrderBy(Vec<PathExpr>, Vec<Ordering>, Box<Node>),
    Distinct(Box<Node>),
    CrossJoin(Box<Node>, Box<Node>),
    LeftJoin(Box<Node>, Box<Node>, Box<Formula>),
    Union(Box<Node>, Box<Node>),
    Intersect(Box<Node>, Box<Node>, bool),
    Except(Box<Node>, Box<Node>, bool),
}

impl Node {
    pub fn get(&self, variables: Variables, registry: Arc<FunctionRegistry>) -> CreateStreamResult<Box<dyn RecordStream>> {
        match self {
            Node::Filter(source, formula) => {
                let record_stream = source.get(variables.clone(), registry.clone())?;
                let stream = FilterStream::new(*formula.clone(), variables, record_stream, registry);
                Ok(Box::new(stream))
            }
            Node::Map(named_list, source) => {
                let record_stream = source.get(variables.clone(), registry.clone())?;

                let stream = MapStream::new(named_list.clone(), variables, record_stream, registry);

                Ok(Box::new(stream))
            }
            Node::DataSource(data_source, bindings) => match data_source {
                DataSource::File(path, file_format, _table_name) => {
                    let reader = ReaderBuilder::new(file_format.clone()).with_path(path)?;
                    let file_stream = LogFileStream::new(Box::new(reader));

                    if !bindings.is_empty() {
                        let stream = ProjectionStream::new(Box::new(file_stream), bindings.clone());

                        return Ok(Box::new(stream));
                    } else {
                        Ok(Box::new(file_stream))
                    }
                }
                DataSource::Stdin(file_format, _table_name) => {
                    let reader = ReaderBuilder::new(file_format.clone()).with_reader(io::stdin());
                    let stream = LogFileStream::new(Box::new(reader));

                    Ok(Box::new(stream))
                }
            },
            Node::GroupBy(fields, named_aggregates, source) => {
                let record_stream = source.get(variables.clone(), registry.clone())?;
                let stream = GroupByStream::new(fields.clone(), variables, named_aggregates.clone(), record_stream, registry);
                Ok(Box::new(stream))
            }
            Node::Limit(row_count, source) => {
                let record_stream = source.get(variables.clone(), registry)?;
                let stream = LimitStream::new(*row_count, record_stream);
                Ok(Box::new(stream))
            }
            Node::OrderBy(column_names, orderings, source) => {
                let mut record_stream = source.get(variables.clone(), registry)?;
                let mut records = Vec::new();

                while let Some(record) = record_stream.next()? {
                    records.push(record);
                }

                records.sort_by(|a, b| {
                    for idx in 0..column_names.len() {
                        let column_name = &column_names[idx];
                        let curr_ordering = &orderings[idx];

                        let a_value = a.get(column_name);
                        let b_value = b.get(column_name);

                        match (a_value, b_value) {
                            (Value::Int(i1), Value::Int(i2)) => match curr_ordering {
                                Ordering::Asc => {
                                    return i1.cmp(&i2);
                                }
                                Ordering::Desc => {
                                    return i2.cmp(&i1);
                                }
                            },
                            (Value::Boolean(b1), Value::Boolean(b2)) => match curr_ordering {
                                Ordering::Asc => {
                                    return b1.cmp(&b2);
                                }
                                Ordering::Desc => {
                                    return b2.cmp(&b1);
                                }
                            },
                            (Value::Float(f1), Value::Float(f2)) => match curr_ordering {
                                Ordering::Asc => {
                                    return f1.cmp(&f2);
                                }
                                Ordering::Desc => {
                                    return f2.cmp(&f1);
                                }
                            },
                            (Value::String(s1), Value::String(s2)) => match curr_ordering {
                                Ordering::Asc => {
                                    return s1.cmp(&s2);
                                }
                                Ordering::Desc => {
                                    return s2.cmp(&s1);
                                }
                            },
                            (Value::DateTime(dt1), Value::DateTime(dt2)) => match curr_ordering {
                                Ordering::Asc => {
                                    return dt1.cmp(&dt2);
                                }
                                Ordering::Desc => {
                                    return dt2.cmp(&dt1);
                                }
                            },
                            (Value::Null, Value::Null) => {
                                return std::cmp::Ordering::Equal;
                            }
                            (Value::Host(h1), Value::Host(h2)) => {
                                let s1 = h1.to_string();
                                let s2 = h2.to_string();

                                match curr_ordering {
                                    Ordering::Asc => {
                                        return s1.cmp(&s2);
                                    }
                                    Ordering::Desc => {
                                        return s2.cmp(&s1);
                                    }
                                }
                            }
                            (Value::HttpRequest(h1), Value::HttpRequest(h2)) => {
                                let s1 = h1.to_string();
                                let s2 = h2.to_string();

                                match curr_ordering {
                                    Ordering::Asc => {
                                        return s1.cmp(&s2);
                                    }
                                    Ordering::Desc => {
                                        return s2.cmp(&s1);
                                    }
                                }
                            }
                            (Value::Missing, Value::Missing) => {
                                return std::cmp::Ordering::Equal;
                            }
                            (Value::Null, Value::Missing) | (Value::Missing, Value::Null) => {
                                return std::cmp::Ordering::Equal;
                            }
                            (Value::Null, _) | (Value::Missing, _) => match curr_ordering {
                                Ordering::Asc => return std::cmp::Ordering::Greater,
                                Ordering::Desc => return std::cmp::Ordering::Less,
                            },
                            (_, Value::Null) | (_, Value::Missing) => match curr_ordering {
                                Ordering::Asc => return std::cmp::Ordering::Less,
                                Ordering::Desc => return std::cmp::Ordering::Greater,
                            },
                            _ => {
                                return std::cmp::Ordering::Equal;
                            }
                        }
                    }

                    std::cmp::Ordering::Equal
                });

                let stream = InMemoryStream::new(VecDeque::from(records));
                Ok(Box::new(stream))
            }
            Node::Distinct(source) => {
                let record_stream = source.get(variables, registry)?;
                Ok(Box::new(DistinctStream::new(record_stream)))
            }
            Node::CrossJoin(left, right) => {
                let left_stream = left.get(variables.clone(), registry.clone())?;
                let right_node = *right.clone();
                let right_variables = variables;
                Ok(Box::new(CrossJoinStream::new(left_stream, right_node, right_variables, registry)))
            }
            Node::LeftJoin(left, right, condition) => {
                let left_stream = left.get(variables.clone(), registry.clone())?;
                let right_node = *right.clone();
                let right_variables = variables;
                Ok(Box::new(LeftJoinStream::new(left_stream, right_node, right_variables, *condition.clone(), registry)))
            }
            Node::Union(left, right) => {
                let left_stream = left.get(variables.clone(), registry.clone())?;
                let right_stream = right.get(variables, registry)?;
                Ok(Box::new(UnionStream::new(left_stream, right_stream)))
            }
            Node::Intersect(left, right, all) => {
                let left_stream = left.get(variables.clone(), registry.clone())?;
                let right_stream = right.get(variables, registry)?;
                Ok(Box::new(IntersectStream::new(left_stream, right_stream, *all)?))
            }
            Node::Except(left, right, all) => {
                let left_stream = left.get(variables.clone(), registry.clone())?;
                let right_stream = right.get(variables, registry)?;
                Ok(Box::new(ExceptStream::new(left_stream, right_stream, *all)?))
            }
        }
    }
}

pub(crate) type AggregateResult<T> = result::Result<T, AggregateError>;

#[derive(thiserror::Error, PartialEq, Eq, Debug)]
pub enum AggregateError {
    #[error("Key Not Found")]
    KeyNotFound,
    #[error("Invalid Type")]
    InvalidType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedAggregate {
    pub(crate) aggregate: Aggregate,
    pub(crate) name_opt: Option<String>,
}

impl NamedAggregate {
    pub(crate) fn new(aggregate: Aggregate, name_opt: Option<String>) -> Self {
        NamedAggregate { aggregate, name_opt }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Aggregate {
    Avg(AvgAggregate, Named),
    Count(CountAggregate, Named),
    First(FirstAggregate, Named),
    Last(LastAggregate, Named),
    Max(MaxAggregate, Named),
    Min(MinAggregate, Named),
    Sum(SumAggregate, Named),
    ApproxCountDistinct(ApproxCountDistinctAggregate, Named),
    PercentileDisc(PercentileDiscAggregate, String),
    ApproxPercentile(ApproxPercentileAggregate, String),
    GroupAs(GroupAsAggregate, Named),
}

impl Aggregate {
    #[allow(dead_code)]
    pub(crate) fn add_record(&mut self, key: &Option<Tuple>, value: &Value) -> AggregateResult<()> {
        match self {
            Aggregate::GroupAs(agg, _) => agg.add_record(key, value),
            Aggregate::Avg(agg, _) => agg.add_record(key, value),
            Aggregate::Count(agg, _) => agg.add_record(key, value),
            Aggregate::First(agg, _) => agg.add_record(key, value),
            Aggregate::Last(agg, _) => agg.add_record(key, value),
            Aggregate::Sum(agg, _) => agg.add_record(key, value),
            Aggregate::Max(agg, _) => agg.add_record(key, value),
            Aggregate::Min(agg, _) => agg.add_record(key, value),
            Aggregate::ApproxCountDistinct(agg, _) => agg.add_record(key, value),
            Aggregate::PercentileDisc(agg, _) => agg.add_record(key, value),
            Aggregate::ApproxPercentile(agg, _) => agg.add_record(key, value),
        }
    }
    pub(crate) fn get_aggregated(&mut self, key: &Option<Tuple>) -> AggregateResult<Value> {
        match self {
            Aggregate::GroupAs(agg, _) => agg.get_aggregated(key),
            Aggregate::Avg(agg, _) => agg.get_aggregated(key),
            Aggregate::Count(agg, _) => agg.get_aggregated(key),
            Aggregate::First(agg, _) => agg.get_aggregated(key),
            Aggregate::Last(agg, _) => agg.get_aggregated(key),
            Aggregate::Sum(agg, _) => agg.get_aggregated(key),
            Aggregate::Max(agg, _) => agg.get_aggregated(key),
            Aggregate::Min(agg, _) => agg.get_aggregated(key),
            Aggregate::ApproxCountDistinct(agg, _) => agg.get_aggregated(key),
            Aggregate::PercentileDisc(agg, _) => agg.get_aggregated(key),
            Aggregate::ApproxPercentile(agg, _) => agg.get_aggregated(key),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PercentileDiscAggregate {
    pub(crate) partitions: HashMap<Option<Tuple>, Vec<Value>>,
    pub(crate) percentile: OrderedFloat<f32>,
    pub(crate) ordering: Ordering,
}

impl PercentileDiscAggregate {
    pub(crate) fn new(percentile: OrderedFloat<f32>, ordering: Ordering) -> Self {
        PercentileDiscAggregate {
            partitions: HashMap::new(),
            percentile,
            ordering,
        }
    }

    pub(crate) fn add_record(&mut self, key: &Option<Tuple>, value: &Value) -> AggregateResult<()> {
        let v = self.partitions.entry(key.clone()).or_insert(Vec::new());
        v.push(value.clone());

        Ok(())
    }

    pub(crate) fn get_aggregated(&self, key: &Option<Tuple>) -> AggregateResult<Value> {
        //FIXME: expensive operation
        let mut v = self.partitions.get(key).unwrap().clone();
        v.sort_by(|a, b| match (a, b) {
            (Value::Int(i1), Value::Int(i2)) => match self.ordering {
                Ordering::Asc => i1.cmp(i2),
                Ordering::Desc => i2.cmp(i1),
            },
            (Value::Boolean(b1), Value::Boolean(b2)) => match self.ordering {
                Ordering::Asc => b1.cmp(b2),
                Ordering::Desc => b2.cmp(b1),
            },
            (Value::Float(f1), Value::Float(f2)) => match self.ordering {
                Ordering::Asc => f1.cmp(f2),
                Ordering::Desc => f2.cmp(f1),
            },
            (Value::DateTime(dt1), Value::DateTime(dt2)) => match self.ordering {
                Ordering::Asc => dt1.cmp(dt2),
                Ordering::Desc => dt2.cmp(dt1),
            },
            (Value::String(s1), Value::String(s2)) => match self.ordering {
                Ordering::Asc => s1.cmp(s2),
                Ordering::Desc => s2.cmp(s1),
            },
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Host(h1), Value::Host(h2)) => {
                let s1 = h1.to_string();
                let s2 = h2.to_string();

                match self.ordering {
                    Ordering::Asc => s1.cmp(&s2),
                    Ordering::Desc => s2.cmp(&s1),
                }
            }
            (Value::HttpRequest(h1), Value::HttpRequest(h2)) => {
                let s1 = h1.to_string();
                let s2 = h2.to_string();

                match self.ordering {
                    Ordering::Asc => s1.cmp(&s2),
                    Ordering::Desc => s2.cmp(&s1),
                }
            }
            _ => {
                unreachable!();
            }
        });

        let f32_percentile: f32 = self.percentile.into();
        let idx: usize = ((v.len() as f32) * f32_percentile) as usize;
        let ans = v[idx].clone();
        Ok(ans)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ApproxPercentileAggregate {
    pub(crate) partitions: HashMap<Option<Tuple>, TDigest>,
    pub(crate) buffer: HashMap<Option<Tuple>, Vec<Value>>,
    pub(crate) percentile: OrderedFloat<f32>,
    pub(crate) ordering: Ordering,
}

impl ApproxPercentileAggregate {
    pub(crate) fn new(percentile: OrderedFloat<f32>, ordering: Ordering) -> Self {
        ApproxPercentileAggregate {
            partitions: HashMap::new(),
            buffer: HashMap::new(),
            percentile,
            ordering,
        }
    }

    pub(crate) fn add_record(&mut self, key: &Option<Tuple>, value: &Value) -> AggregateResult<()> {
        let buf = self.buffer.entry(key.clone()).or_insert(Vec::new());
        buf.push(value.clone());

        if buf.len() < 10000 {
            Ok(())
        } else {
            let v = self
                .partitions
                .entry(key.clone())
                .or_insert(TDigest::new_with_size(100));

            let mut fvec = Vec::new();
            for val in buf.iter() {
                match val {
                    Value::Float(f) => {
                        fvec.push(f64::from(f.into_inner()));
                    }
                    Value::Int(i) => {
                        fvec.push(f64::from(*i));
                    }
                    _ => {
                        return Err(AggregateError::InvalidType);
                    }
                }
            }

            let new_digest = v.merge_unsorted(fvec);
            self.partitions.insert(key.clone(), new_digest);
            buf.clear();

            Ok(())
        }
    }

    pub(crate) fn get_aggregated(&mut self, key: &Option<Tuple>) -> AggregateResult<Value> {
        let buf = self.buffer.entry(key.clone()).or_insert(Vec::new());
        let t = if !buf.is_empty() {
            let v = self
                .partitions
                .entry(key.clone())
                .or_insert(TDigest::new_with_size(100));

            let mut fvec = Vec::new();
            for val in buf.iter() {
                match val {
                    Value::Float(f) => {
                        fvec.push(f64::from(f.into_inner()));
                    }
                    Value::Int(i) => {
                        fvec.push(f64::from(*i));
                    }
                    _ => {
                        return Err(AggregateError::InvalidType);
                    }
                }
            }

            v.merge_unsorted(fvec)
        } else {
            self.partitions.get(key).unwrap().clone()
        };

        let f32_percentile: f32 = self.percentile.into();
        let f64_percentile: f64 = f64::from(f32_percentile);
        let f64_ans = t.estimate_quantile(f64_percentile);
        let ans = Value::Float(OrderedFloat::from(f64_ans as f32));
        Ok(ans)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AvgAggregate {
    pub(crate) averages: HashMap<Option<Tuple>, OrderedFloat<f32>>,
    pub(crate) counts: HashMap<Option<Tuple>, i64>,
}

impl AvgAggregate {
    pub(crate) fn new() -> Self {
        AvgAggregate {
            averages: HashMap::new(),
            counts: HashMap::new(),
        }
    }

    pub(crate) fn add_record(&mut self, key: &Option<Tuple>, value: &Value) -> AggregateResult<()> {
        let new_value: OrderedFloat<f32> = match value {
            &Value::Int(i) => OrderedFloat::from(i as f32),
            &Value::Float(f) => f,
            _ => {
                return Err(AggregateError::InvalidType);
            }
        };

        if let (Some(&average), Some(&count)) = (self.averages.get(key), self.counts.get(key)) {
            let new_count = count + 1;
            let f32_average: f32 = average.into();
            let f32_new_value: f32 = new_value.into();
            let new_average: f32 = (f32_average * (count as f32) + f32_new_value) / (new_count as f32);
            self.averages.insert(key.clone(), OrderedFloat::from(new_average));
            self.counts.insert(key.clone(), new_count);
            Ok(())
        } else {
            self.averages.insert(key.clone(), new_value);
            self.counts.insert(key.clone(), 1);

            Ok(())
        }
    }

    pub(crate) fn get_aggregated(&self, key: &Option<Tuple>) -> AggregateResult<Value> {
        if let Some(&average) = self.averages.get(key) {
            Ok(Value::Float(average))
        } else {
            Err(AggregateError::KeyNotFound)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SumAggregate {
    pub(crate) sums: HashMap<Option<Tuple>, OrderedFloat<f32>>,
}

impl SumAggregate {
    pub(crate) fn new() -> Self {
        SumAggregate { sums: HashMap::new() }
    }

    pub(crate) fn add_record(&mut self, key: &Option<Tuple>, value: &Value) -> AggregateResult<()> {
        let new_value: OrderedFloat<f32> = match value {
            &Value::Int(i) => OrderedFloat::from(i as f32),
            &Value::Float(f) => f,
            _ => {
                return Err(AggregateError::InvalidType);
            }
        };

        if let Some(&average) = self.sums.get(key) {
            let f32_average: f32 = average.into();
            let f32_new_value: f32 = new_value.into();
            let new_average: f32 = f32_average + f32_new_value;
            self.sums.insert(key.clone(), OrderedFloat::from(new_average));
            Ok(())
        } else {
            self.sums.insert(key.clone(), new_value);

            Ok(())
        }
    }

    pub(crate) fn get_aggregated(&self, key: &Option<Tuple>) -> AggregateResult<Value> {
        if let Some(&average) = self.sums.get(key) {
            Ok(Value::Float(average))
        } else {
            Err(AggregateError::KeyNotFound)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CountAggregate {
    pub(crate) counts: HashMap<Option<Tuple>, i64>,
}

impl CountAggregate {
    pub(crate) fn new() -> Self {
        CountAggregate { counts: HashMap::new() }
    }

    pub(crate) fn add_record(&mut self, key: &Option<Tuple>, value: &Value) -> AggregateResult<()> {
        if let &Value::Null = value {
            //Null value doesn't contribute to the total count
            return Ok(());
        };

        if let Some(&count) = self.counts.get(key) {
            let new_count = count + 1;
            self.counts.insert(key.clone(), new_count);
            Ok(())
        } else {
            self.counts.insert(key.clone(), 1);

            Ok(())
        }
    }

    pub(crate) fn add_row(&mut self, key: Option<Tuple>) -> AggregateResult<()> {
        if let Some(&count) = self.counts.get(&key) {
            let new_count = count + 1;
            self.counts.insert(key.clone(), new_count);
            Ok(())
        } else {
            self.counts.insert(key.clone(), 1);

            Ok(())
        }
    }

    pub(crate) fn get_aggregated(&self, key: &Option<Tuple>) -> AggregateResult<Value> {
        if let Some(&counts) = self.counts.get(key) {
            Ok(Value::Int(counts as i32))
        } else {
            Err(AggregateError::KeyNotFound)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GroupAsAggregate {
    pub(crate) tuples: HashMap<Option<Tuple>, Vec<Value>>,
}

impl GroupAsAggregate {
    pub(crate) fn new() -> Self {
        GroupAsAggregate { tuples: HashMap::new() }
    }

    pub(crate) fn add_record(&mut self, key: &Option<Tuple>, value: &Value) -> AggregateResult<()> {
        if let Some(tuples) = self.tuples.get_mut(key) {
            tuples.push(value.clone());
            Ok(())
        } else {
            self.tuples.insert(key.clone(), vec![value.clone()]);
            Ok(())
        }
    }

    pub(crate) fn get_aggregated(&self, key: &Option<Tuple>) -> AggregateResult<Value> {
        if let Some(tuples) = self.tuples.get(key) {
            Ok(Value::Array(tuples.clone()))
        } else {
            Err(AggregateError::KeyNotFound)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaxAggregate {
    pub(crate) maxs: HashMap<Option<Tuple>, Value>,
}

impl MaxAggregate {
    pub(crate) fn new() -> Self {
        MaxAggregate { maxs: HashMap::new() }
    }

    pub(crate) fn add_record(&mut self, key: &Option<Tuple>, value: &Value) -> AggregateResult<()> {
        if let Some(candidate) = self.maxs.get(key) {
            let less_than = match (candidate, value) {
                (&Value::Int(i1), &Value::Int(i2)) => i1 < i2,
                (&Value::Float(f1), &Value::Float(f2)) => f1 < f2,
                _ => {
                    return Err(AggregateError::InvalidType);
                }
            };

            if less_than {
                self.maxs.insert(key.clone(), value.clone());
            }

            Ok(())
        } else {
            self.maxs.insert(key.clone(), value.clone());
            Ok(())
        }
    }

    pub(crate) fn get_aggregated(&self, key: &Option<Tuple>) -> AggregateResult<Value> {
        if let Some(first) = self.maxs.get(key) {
            Ok(first.clone())
        } else {
            Err(AggregateError::KeyNotFound)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MinAggregate {
    pub(crate) mins: HashMap<Option<Tuple>, Value>,
}

impl MinAggregate {
    pub(crate) fn new() -> Self {
        MinAggregate { mins: HashMap::new() }
    }

    pub(crate) fn add_record(&mut self, key: &Option<Tuple>, value: &Value) -> AggregateResult<()> {
        if let Some(candidate) = self.mins.get(key) {
            let greater_than = match (candidate, value) {
                (&Value::Int(i1), &Value::Int(i2)) => i1 > i2,
                (&Value::Float(f1), &Value::Float(f2)) => f1 > f2,
                _ => {
                    return Err(AggregateError::InvalidType);
                }
            };

            if greater_than {
                self.mins.insert(key.clone(), value.clone());
            }

            Ok(())
        } else {
            self.mins.insert(key.clone(), value.clone());
            Ok(())
        }
    }

    pub(crate) fn get_aggregated(&self, key: &Option<Tuple>) -> AggregateResult<Value> {
        if let Some(first) = self.mins.get(key) {
            Ok(first.clone())
        } else {
            Err(AggregateError::KeyNotFound)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FirstAggregate {
    pub(crate) firsts: HashMap<Option<Tuple>, Value>,
}

impl FirstAggregate {
    pub(crate) fn new() -> Self {
        FirstAggregate { firsts: HashMap::new() }
    }

    pub(crate) fn add_record(&mut self, key: &Option<Tuple>, value: &Value) -> AggregateResult<()> {
        if self.firsts.get(&key).is_some() {
            //do nothing
            Ok(())
        } else {
            self.firsts.insert(key.clone(), value.clone());
            Ok(())
        }
    }

    pub(crate) fn get_aggregated(&self, key: &Option<Tuple>) -> AggregateResult<Value> {
        if let Some(first) = self.firsts.get(key) {
            Ok(first.clone())
        } else {
            Err(AggregateError::KeyNotFound)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LastAggregate {
    pub(crate) lasts: HashMap<Option<Tuple>, Value>,
}

impl LastAggregate {
    pub(crate) fn new() -> Self {
        LastAggregate { lasts: HashMap::new() }
    }

    pub(crate) fn add_record(&mut self, key: &Option<Tuple>, value: &Value) -> AggregateResult<()> {
        self.lasts.insert(key.clone(), value.clone());
        Ok(())
    }

    pub(crate) fn get_aggregated(&self, key: &Option<Tuple>) -> AggregateResult<Value> {
        if let Some(last) = self.lasts.get(key) {
            Ok(last.clone())
        } else {
            Err(AggregateError::KeyNotFound)
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ApproxCountDistinctAggregate {
    pub(crate) counts: HashMap<Option<Tuple>, HyperLogLog<Value>>,
}

impl PartialEq for ApproxCountDistinctAggregate {
    fn eq(&self, other: &Self) -> bool {
        if self.counts.len() != other.counts.len() {
            return false;
        }
        self.counts.keys().all(|k| other.counts.contains_key(k))
            && self
                .counts
                .iter()
                .all(|(k, v)| other.counts.get(k).map_or(false, |ov| v.count() == ov.count()))
    }
}

impl Eq for ApproxCountDistinctAggregate {}

impl ApproxCountDistinctAggregate {
    pub(crate) fn new() -> Self {
        ApproxCountDistinctAggregate { counts: HashMap::new() }
    }

    pub(crate) fn add_record(&mut self, key: &Option<Tuple>, value: &Value) -> AggregateResult<()> {
        if let &Value::Null = value {
            //Null value doesn't contribute to the total count
            return Ok(());
        };

        if let Some(hll) = self.counts.get_mut(key) {
            hll.add(value);
            Ok(())
        } else {
            self.counts.insert(key.clone(), HyperLogLog::new(8));

            Ok(())
        }
    }

    pub(crate) fn get_aggregated(&self, key: &Option<Tuple>) -> AggregateResult<Value> {
        if let Some(hll) = self.counts.get(key) {
            Ok(Value::Int(hll.count() as i32))
        } else {
            Err(AggregateError::KeyNotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_registry() -> Arc<FunctionRegistry> {
        Arc::new(crate::functions::register_all().unwrap())
    }

    #[test]
    fn test_avg_aggregate_with_one_element() {
        let mut iter = Aggregate::Avg(AvgAggregate::new(), Named::Star);
        let tuple = Some(vec![Value::String("key".to_string())]);
        let value = Value::Float(OrderedFloat::from(5.0));

        let _ = iter.add_record(&tuple, &value);
        let aggregate = iter.get_aggregated(&tuple);
        assert_eq!(Ok(value), aggregate);
    }

    #[test]
    fn test_avg_aggregate_with_many_elements() {
        let mut iter = Aggregate::Avg(AvgAggregate::new(), Named::Star);
        let tuple = Some(vec![Value::String("key".to_string())]);

        for i in 1..=10 {
            let value = Value::Float(OrderedFloat::from(i as f32));
            let _ = iter.add_record(&tuple, &value);
        }

        let aggregate = iter.get_aggregated(&tuple);
        assert_eq!(Ok(Value::Float(OrderedFloat::from(5.5))), aggregate);
    }

    #[test]
    fn test_count_aggregate() {
        let mut iter = Aggregate::Count(CountAggregate::new(), Named::Star);
        let tuple = Some(vec![Value::String("key".to_string())]);
        for i in 0..13 {
            let value = Value::Int(i);
            let _ = iter.add_record(&tuple, &value);
        }

        let aggregate = iter.get_aggregated(&tuple);
        assert_eq!(Ok(Value::Int(13)), aggregate);
    }

    #[test]
    fn test_first_aggregate() {
        let mut iter = Aggregate::First(FirstAggregate::new(), Named::Star);
        let tuple = Some(vec![Value::String("key".to_string())]);
        for i in 0..13 {
            let value = Value::Int(i);
            let _ = iter.add_record(&tuple, &value);
        }

        let aggregate = iter.get_aggregated(&tuple);
        assert_eq!(Ok(Value::Int(0)), aggregate);
    }

    #[test]
    fn test_last_aggregate() {
        let mut iter = Aggregate::Last(LastAggregate::new(), Named::Star);
        let tuple = Some(vec![Value::String("key".to_string())]);
        for i in 0..13 {
            let value = Value::Int(i);
            let _ = iter.add_record(&tuple, &value);
        }

        let aggregate = iter.get_aggregated(&tuple);
        assert_eq!(Ok(Value::Int(12)), aggregate);
    }

    #[test]
    fn test_sum_aggregate_with_many_elements() {
        let mut iter = Aggregate::Sum(SumAggregate::new(), Named::Star);
        let tuple = Some(vec![Value::String("key".to_string())]);

        for i in 1..=10 {
            let value = Value::Float(OrderedFloat::from(i as f32));
            let _ = iter.add_record(&tuple, &value);
        }

        let aggregate = iter.get_aggregated(&tuple);
        assert_eq!(Ok(Value::Float(OrderedFloat::from(55.0))), aggregate);
    }

    #[test]
    fn test_max_aggregate() {
        let mut iter = Aggregate::Max(MaxAggregate::new(), Named::Star);
        let tuple = Some(vec![Value::String("key".to_string())]);
        for i in 0..13 {
            let value = Value::Int(i);
            let _ = iter.add_record(&tuple, &value);
        }

        let aggregate = iter.get_aggregated(&tuple);
        assert_eq!(Ok(Value::Int(12)), aggregate);
    }

    #[test]
    fn test_min_aggregate() {
        let mut iter = Aggregate::Min(MinAggregate::new(), Named::Star);
        let tuple = Some(vec![Value::String("key".to_string())]);
        for i in 0..13 {
            let value = Value::Int(i);
            let _ = iter.add_record(&tuple, &value);
        }

        let aggregate = iter.get_aggregated(&tuple);
        assert_eq!(Ok(Value::Int(0)), aggregate);
    }

    #[test]
    fn test_evaluate_host_functions() {
        let registry = test_registry();
        let v = Value::Host(common::types::parse_host("192.168.131.39:2817").unwrap());
        let name = registry.call("host_name", &vec![v.clone()]).unwrap();
        assert_eq!(name, Value::String("192.168.131.39".to_string()));
        let port = registry.call("host_port", &vec![v]).unwrap();
        assert_eq!(port, Value::Int(2817));
    }

    #[test]
    fn test_evaluate_url_functions() {
        let registry = test_registry();
        let v = Value::HttpRequest(
            common::types::parse_http_request(
                "GET http://example.com:8000/users/123?mode=json&after=&iteration=1 HTTP/1.1",
            )
            .unwrap(),
        );
        let name = registry.call("url_host", &vec![v.clone()]).unwrap();
        assert_eq!(name, Value::String("example.com".to_string()));
        let port = registry.call("url_port", &vec![v.clone()]).unwrap();
        assert_eq!(port, Value::Int(8000));
        let path = registry.call("url_path", &vec![v.clone()]).unwrap();
        assert_eq!(path, Value::String("/users/123".to_string()));
        let fragment = registry.call("url_fragment", &vec![v.clone()]).unwrap();
        assert_eq!(fragment, Value::Null);
        let query = registry.call("url_query", &vec![v.clone()]).unwrap();
        assert_eq!(query, Value::String("mode=json&after=&iteration=1".to_string()));
        let path_segments = registry.call("url_path_segments", &vec![v.clone(), Value::Int(1)]).unwrap();
        assert_eq!(path_segments, Value::String("123".to_string()));
        let mapped_path = registry.call(
            "url_path_bucket",
            &vec![v.clone(), Value::Int(1), Value::String("_".to_string())],
        )
        .unwrap();
        assert_eq!(mapped_path, Value::String("/users/_".to_string()));
    }

    #[test]
    fn test_evaluate() {
        let registry = test_registry();
        let v = registry.call("Plus", &vec![Value::Int(1), Value::Int(2)]).unwrap();
        assert_eq!(v, Value::Int(3));

        let v = registry.call("Minus", &vec![Value::Int(2), Value::Int(2)]).unwrap();
        assert_eq!(v, Value::Int(0));

        let v = registry.call("Times", &vec![Value::Int(2), Value::Int(2)]).unwrap();
        assert_eq!(v, Value::Int(4));

        let v = registry.call("Divide", &vec![Value::Int(2), Value::Int(2)]).unwrap();
        assert_eq!(v, Value::Int(1));

        let dt = Value::DateTime(chrono::DateTime::parse_from_rfc3339("2015-11-07T18:45:37.691548Z").unwrap());
        let expected_dt = Value::DateTime(chrono::DateTime::parse_from_rfc3339("2015-11-07T18:45:35.000000Z").unwrap());
        let bucket_dt = registry.call("time_bucket", &vec![Value::String("5 seconds".to_string()), dt.clone()]).unwrap();
        assert_eq!(expected_dt, bucket_dt);

        let expected_dt = Value::DateTime(chrono::DateTime::parse_from_rfc3339("2015-11-07T18:45:00.000000Z").unwrap());
        let bucket_dt = registry.call("time_bucket", &vec![Value::String("5 minutes".to_string()), dt.clone()]).unwrap();
        assert_eq!(expected_dt, bucket_dt);

        let expected_dt = Value::DateTime(chrono::DateTime::parse_from_rfc3339("2015-11-07T18:00:00.000000Z").unwrap());
        let bucket_dt = registry.call("time_bucket", &vec![Value::String("1 hour".to_string()), dt.clone()]).unwrap();
        assert_eq!(expected_dt, bucket_dt);

        let hour = registry.call("date_part", &vec![Value::String("second".to_string()), dt.clone()]).unwrap();
        assert_eq!(Value::Float(OrderedFloat::from(37.0)), hour);
    }

    #[test]
    fn test_arithmetic_null_propagation() {
        let registry = test_registry();
        assert_eq!(registry.call("Plus", &vec![Value::Null, Value::Int(1)]), Ok(Value::Null));
        assert_eq!(registry.call("Plus", &vec![Value::Int(1), Value::Null]), Ok(Value::Null));
        assert_eq!(registry.call("Plus", &vec![Value::Missing, Value::Int(1)]), Ok(Value::Missing));
        assert_eq!(registry.call("Minus", &vec![Value::Null, Value::Int(1)]), Ok(Value::Null));
        assert_eq!(registry.call("Times", &vec![Value::Null, Value::Int(1)]), Ok(Value::Null));
        assert_eq!(registry.call("Divide", &vec![Value::Null, Value::Int(1)]), Ok(Value::Null));
    }

    #[test]
    fn test_float_arithmetic() {
        let registry = test_registry();
        assert_eq!(
            registry.call("Plus", &vec![Value::Float(OrderedFloat::from(1.5f32)), Value::Float(OrderedFloat::from(2.5f32))]),
            Ok(Value::Float(OrderedFloat::from(4.0f32)))
        );
        // Mixed Int/Float promotion
        assert_eq!(
            registry.call("Plus", &vec![Value::Int(1), Value::Float(OrderedFloat::from(2.5f32))]),
            Ok(Value::Float(OrderedFloat::from(3.5f32)))
        );
        assert_eq!(
            registry.call("Plus", &vec![Value::Float(OrderedFloat::from(2.5f32)), Value::Int(1)]),
            Ok(Value::Float(OrderedFloat::from(3.5f32)))
        );
    }

    #[test]
    fn test_comparison_int_float_coercion() {
        let vars = Variables::default();
        let registry = test_registry();
        let rel = Relation::MoreThan;
        let left = Expression::Constant(Value::Float(OrderedFloat::from(2.5f32)));
        let right = Expression::Constant(Value::Int(1));
        assert_eq!(rel.apply(&vars, &left, &right, &registry), Ok(Some(true)));

        let rel = Relation::LessThan;
        let left = Expression::Constant(Value::Int(1));
        let right = Expression::Constant(Value::Float(OrderedFloat::from(2.5f32)));
        assert_eq!(rel.apply(&vars, &left, &right, &registry), Ok(Some(true)));
    }

    #[test]
    fn test_comparison_null_returns_none() {
        let vars = Variables::default();
        let registry = test_registry();
        let rel = Relation::Equal;
        let left = Expression::Constant(Value::Null);
        let right = Expression::Constant(Value::Int(1));
        assert_eq!(rel.apply(&vars, &left, &right, &registry), Ok(None));

        let left = Expression::Constant(Value::Null);
        let right = Expression::Constant(Value::Null);
        assert_eq!(rel.apply(&vars, &left, &right, &registry), Ok(None));
    }

    #[test]
    fn test_three_valued_and() {
        let vars = Variables::default();
        let registry = test_registry();
        // TRUE AND TRUE = TRUE
        let f = Formula::And(Box::new(Formula::Constant(true)), Box::new(Formula::Constant(true)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));
        // FALSE AND TRUE = FALSE
        let f = Formula::And(Box::new(Formula::Constant(false)), Box::new(Formula::Constant(true)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(false)));
        // TRUE AND FALSE = FALSE
        let f = Formula::And(Box::new(Formula::Constant(true)), Box::new(Formula::Constant(false)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(false)));
        // FALSE AND FALSE = FALSE
        let f = Formula::And(Box::new(Formula::Constant(false)), Box::new(Formula::Constant(false)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(false)));
    }

    #[test]
    fn test_three_valued_or() {
        let vars = Variables::default();
        let registry = test_registry();
        // TRUE OR FALSE = TRUE
        let f = Formula::Or(Box::new(Formula::Constant(true)), Box::new(Formula::Constant(false)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));
        // FALSE OR FALSE = FALSE
        let f = Formula::Or(Box::new(Formula::Constant(false)), Box::new(Formula::Constant(false)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(false)));
        // FALSE OR TRUE = TRUE
        let f = Formula::Or(Box::new(Formula::Constant(false)), Box::new(Formula::Constant(true)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));
        // TRUE OR TRUE = TRUE
        let f = Formula::Or(Box::new(Formula::Constant(true)), Box::new(Formula::Constant(true)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));
    }

    #[test]
    fn test_three_valued_not() {
        let vars = Variables::default();
        let registry = test_registry();
        let f = Formula::Not(Box::new(Formula::Constant(true)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(false)));
        let f = Formula::Not(Box::new(Formula::Constant(false)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));
    }

    #[test]
    fn test_three_valued_and_with_null() {
        let vars = Variables::default();
        let registry = test_registry();
        // NULL comparison produces None; TRUE AND None = None
        let null_pred = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Constant(Value::Null)),
            Box::new(Expression::Constant(Value::Int(1))),
        );
        let f = Formula::And(Box::new(Formula::Constant(true)), Box::new(null_pred.clone()));
        assert_eq!(f.evaluate(&vars, &registry), Ok(None));
        // FALSE AND None = FALSE (false dominates)
        let f = Formula::And(Box::new(Formula::Constant(false)), Box::new(null_pred));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(false)));
    }

    #[test]
    fn test_three_valued_or_with_null() {
        let vars = Variables::default();
        let registry = test_registry();
        let null_pred = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Constant(Value::Null)),
            Box::new(Expression::Constant(Value::Int(1))),
        );
        // TRUE OR None = TRUE (true dominates)
        let f = Formula::Or(Box::new(Formula::Constant(true)), Box::new(null_pred.clone()));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));
        // FALSE OR None = None
        let f = Formula::Or(Box::new(Formula::Constant(false)), Box::new(null_pred));
        assert_eq!(f.evaluate(&vars, &registry), Ok(None));
    }

    #[test]
    fn test_three_valued_not_with_null() {
        let vars = Variables::default();
        let registry = test_registry();
        let null_pred = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Constant(Value::Null)),
            Box::new(Expression::Constant(Value::Int(1))),
        );
        // NOT None = None
        let f = Formula::Not(Box::new(null_pred));
        assert_eq!(f.evaluate(&vars, &registry), Ok(None));
    }

    #[test]
    fn test_is_null() {
        let vars = Variables::default();
        let registry = test_registry();
        // IS NULL on Null => true
        let f = Formula::IsNull(Box::new(Expression::Constant(Value::Null)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));
        // IS NULL on non-null => false
        let f = Formula::IsNull(Box::new(Expression::Constant(Value::Int(1))));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(false)));
        // IS NULL on Missing => false
        let f = Formula::IsNull(Box::new(Expression::Constant(Value::Missing)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(false)));
    }

    #[test]
    fn test_is_not_null() {
        let vars = Variables::default();
        let registry = test_registry();
        // IS NOT NULL on Null => false
        let f = Formula::IsNotNull(Box::new(Expression::Constant(Value::Null)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(false)));
        // IS NOT NULL on non-null => true
        let f = Formula::IsNotNull(Box::new(Expression::Constant(Value::Int(1))));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));
        // IS NOT NULL on Missing => true
        let f = Formula::IsNotNull(Box::new(Expression::Constant(Value::Missing)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));
    }

    #[test]
    fn test_is_missing() {
        let vars = Variables::default();
        let registry = test_registry();
        // IS MISSING on Missing => true
        let f = Formula::IsMissing(Box::new(Expression::Constant(Value::Missing)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));
        // IS MISSING on non-missing => false
        let f = Formula::IsMissing(Box::new(Expression::Constant(Value::Int(1))));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(false)));
        // IS MISSING on Null => false
        let f = Formula::IsMissing(Box::new(Expression::Constant(Value::Null)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(false)));
    }

    #[test]
    fn test_is_not_missing() {
        let vars = Variables::default();
        let registry = test_registry();
        // IS NOT MISSING on Missing => false
        let f = Formula::IsNotMissing(Box::new(Expression::Constant(Value::Missing)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(false)));
        // IS NOT MISSING on non-missing => true
        let f = Formula::IsNotMissing(Box::new(Expression::Constant(Value::Int(1))));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));
        // IS NOT MISSING on Null => true
        let f = Formula::IsNotMissing(Box::new(Expression::Constant(Value::Null)));
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));
    }

    #[test]
    fn test_string_concat_evaluation() {
        let registry = test_registry();
        assert_eq!(
            registry.call("Concat", &vec![Value::String("foo".to_string()), Value::String("bar".to_string())]),
            Ok(Value::String("foobar".to_string()))
        );
    }

    #[test]
    fn test_string_concat_null_propagation() {
        let registry = test_registry();
        assert_eq!(
            registry.call("Concat", &vec![Value::Null, Value::String("bar".to_string())]),
            Ok(Value::Null)
        );
        assert_eq!(
            registry.call("Concat", &vec![Value::String("foo".to_string()), Value::Null]),
            Ok(Value::Null)
        );
    }

    #[test]
    fn test_string_concat_missing_propagation() {
        let registry = test_registry();
        assert_eq!(
            registry.call("Concat", &vec![Value::Missing, Value::String("bar".to_string())]),
            Ok(Value::Missing)
        );
        assert_eq!(
            registry.call("Concat", &vec![Value::String("foo".to_string()), Value::Missing]),
            Ok(Value::Missing)
        );
    }

    #[test]
    fn test_like_pattern_to_regex() {
        assert_eq!(like_pattern_to_regex("%"), "^.*$");
        assert_eq!(like_pattern_to_regex("_"), "^.$");
        assert_eq!(like_pattern_to_regex("%foo%"), "^.*foo.*$");
        assert_eq!(like_pattern_to_regex("foo_bar"), "^foo.bar$");
        assert_eq!(like_pattern_to_regex("hello"), "^hello$");
        // Regex special chars should be escaped
        assert_eq!(like_pattern_to_regex("a.b"), "^a\\.b$");
        assert_eq!(like_pattern_to_regex("a+b"), "^a\\+b$");
    }

    #[test]
    fn test_like_evaluation() {
        let vars = Variables::default();
        let registry = test_registry();
        // Basic match: 'foobar' LIKE '%foo%' => true
        let f = Formula::Like(
            Box::new(Expression::Constant(Value::String("foobar".to_string()))),
            Box::new(Expression::Constant(Value::String("%foo%".to_string()))),
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));

        // No match: 'hello' LIKE '%foo%' => false
        let f = Formula::Like(
            Box::new(Expression::Constant(Value::String("hello".to_string()))),
            Box::new(Expression::Constant(Value::String("%foo%".to_string()))),
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(false)));

        // Underscore wildcard: 'abc' LIKE 'a_c' => true
        let f = Formula::Like(
            Box::new(Expression::Constant(Value::String("abc".to_string()))),
            Box::new(Expression::Constant(Value::String("a_c".to_string()))),
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));

        // Underscore wildcard: 'ac' LIKE 'a_c' => false (underscore matches exactly one char)
        let f = Formula::Like(
            Box::new(Expression::Constant(Value::String("ac".to_string()))),
            Box::new(Expression::Constant(Value::String("a_c".to_string()))),
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(false)));

        // Exact match: 'hello' LIKE 'hello' => true
        let f = Formula::Like(
            Box::new(Expression::Constant(Value::String("hello".to_string()))),
            Box::new(Expression::Constant(Value::String("hello".to_string()))),
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));
    }

    #[test]
    fn test_not_like_evaluation() {
        let vars = Variables::default();
        let registry = test_registry();
        // 'hello' NOT LIKE '%foo%' => true
        let f = Formula::NotLike(
            Box::new(Expression::Constant(Value::String("hello".to_string()))),
            Box::new(Expression::Constant(Value::String("%foo%".to_string()))),
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));

        // 'foobar' NOT LIKE '%foo%' => false
        let f = Formula::NotLike(
            Box::new(Expression::Constant(Value::String("foobar".to_string()))),
            Box::new(Expression::Constant(Value::String("%foo%".to_string()))),
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(false)));
    }

    #[test]
    fn test_like_null_propagation() {
        let vars = Variables::default();
        let registry = test_registry();
        // NULL LIKE '%foo%' => None
        let f = Formula::Like(
            Box::new(Expression::Constant(Value::Null)),
            Box::new(Expression::Constant(Value::String("%foo%".to_string()))),
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(None));

        // 'hello' LIKE NULL => None
        let f = Formula::Like(
            Box::new(Expression::Constant(Value::String("hello".to_string()))),
            Box::new(Expression::Constant(Value::Null)),
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(None));

        // MISSING LIKE '%foo%' => None
        let f = Formula::Like(
            Box::new(Expression::Constant(Value::Missing)),
            Box::new(Expression::Constant(Value::String("%foo%".to_string()))),
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(None));
    }

    #[test]
    fn test_in_evaluation() {
        let vars = Variables::default();
        let registry = test_registry();
        // 2 IN (1, 2, 3) => true
        let f = Formula::In(
            Box::new(Expression::Constant(Value::Int(2))),
            vec![
                Expression::Constant(Value::Int(1)),
                Expression::Constant(Value::Int(2)),
                Expression::Constant(Value::Int(3)),
            ],
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));

        // 4 IN (1, 2, 3) => false
        let f = Formula::In(
            Box::new(Expression::Constant(Value::Int(4))),
            vec![
                Expression::Constant(Value::Int(1)),
                Expression::Constant(Value::Int(2)),
                Expression::Constant(Value::Int(3)),
            ],
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(false)));

        // String IN check: "b" IN ("a", "b", "c") => true
        let f = Formula::In(
            Box::new(Expression::Constant(Value::String("b".to_string()))),
            vec![
                Expression::Constant(Value::String("a".to_string())),
                Expression::Constant(Value::String("b".to_string())),
                Expression::Constant(Value::String("c".to_string())),
            ],
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));
    }

    #[test]
    fn test_not_in_evaluation() {
        let vars = Variables::default();
        let registry = test_registry();
        // 4 NOT IN (1, 2, 3) => true
        let f = Formula::NotIn(
            Box::new(Expression::Constant(Value::Int(4))),
            vec![
                Expression::Constant(Value::Int(1)),
                Expression::Constant(Value::Int(2)),
                Expression::Constant(Value::Int(3)),
            ],
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));

        // 2 NOT IN (1, 2, 3) => false
        let f = Formula::NotIn(
            Box::new(Expression::Constant(Value::Int(2))),
            vec![
                Expression::Constant(Value::Int(1)),
                Expression::Constant(Value::Int(2)),
                Expression::Constant(Value::Int(3)),
            ],
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(false)));
    }

    #[test]
    fn test_in_null_propagation() {
        let vars = Variables::default();
        let registry = test_registry();
        // NULL IN (1, 2, 3) => None (unknown)
        let f = Formula::In(
            Box::new(Expression::Constant(Value::Null)),
            vec![
                Expression::Constant(Value::Int(1)),
                Expression::Constant(Value::Int(2)),
                Expression::Constant(Value::Int(3)),
            ],
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(None));

        // 2 IN (1, NULL, 3) where x=2 => None (unknown, because NULL could be 2)
        let f = Formula::In(
            Box::new(Expression::Constant(Value::Int(2))),
            vec![
                Expression::Constant(Value::Int(1)),
                Expression::Constant(Value::Null),
                Expression::Constant(Value::Int(3)),
            ],
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(None));

        // 1 IN (1, NULL, 3) => true (found match before considering NULL)
        let f = Formula::In(
            Box::new(Expression::Constant(Value::Int(1))),
            vec![
                Expression::Constant(Value::Int(1)),
                Expression::Constant(Value::Null),
                Expression::Constant(Value::Int(3)),
            ],
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(Some(true)));

        // MISSING IN (1, 2, 3) => None (unknown)
        let f = Formula::In(
            Box::new(Expression::Constant(Value::Missing)),
            vec![
                Expression::Constant(Value::Int(1)),
                Expression::Constant(Value::Int(2)),
                Expression::Constant(Value::Int(3)),
            ],
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(None));
    }

    #[test]
    fn test_not_in_null_propagation() {
        let vars = Variables::default();
        let registry = test_registry();
        // NULL NOT IN (1, 2, 3) => None
        let f = Formula::NotIn(
            Box::new(Expression::Constant(Value::Null)),
            vec![
                Expression::Constant(Value::Int(1)),
                Expression::Constant(Value::Int(2)),
                Expression::Constant(Value::Int(3)),
            ],
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(None));

        // 2 NOT IN (1, NULL, 3) => None (because IN returns None)
        let f = Formula::NotIn(
            Box::new(Expression::Constant(Value::Int(2))),
            vec![
                Expression::Constant(Value::Int(1)),
                Expression::Constant(Value::Null),
                Expression::Constant(Value::Int(3)),
            ],
        );
        assert_eq!(f.evaluate(&vars, &registry), Ok(None));
    }

    #[test]
    fn test_upper() {
        let registry = test_registry();
        assert_eq!(
            registry.call("upper", &vec![Value::String("hello".to_string())]),
            Ok(Value::String("HELLO".to_string()))
        );
        // Case-insensitive function name
        assert_eq!(
            registry.call("UPPER", &vec![Value::String("hello".to_string())]),
            Ok(Value::String("HELLO".to_string()))
        );
    }

    #[test]
    fn test_lower() {
        let registry = test_registry();
        assert_eq!(
            registry.call("lower", &vec![Value::String("HELLO".to_string())]),
            Ok(Value::String("hello".to_string()))
        );
        // Case-insensitive function name
        assert_eq!(
            registry.call("LOWER", &vec![Value::String("HELLO".to_string())]),
            Ok(Value::String("hello".to_string()))
        );
    }

    #[test]
    fn test_char_length() {
        let registry = test_registry();
        assert_eq!(
            registry.call("char_length", &vec![Value::String("hello".to_string())]),
            Ok(Value::Int(5))
        );
        assert_eq!(
            registry.call("character_length", &vec![Value::String("hello".to_string())]),
            Ok(Value::Int(5))
        );
        assert_eq!(
            registry.call("CHAR_LENGTH", &vec![Value::String("hello".to_string())]),
            Ok(Value::Int(5))
        );
    }

    #[test]
    fn test_substring() {
        let registry = test_registry();
        assert_eq!(
            registry.call("substring", &vec![Value::String("hello".to_string()), Value::Int(2)]),
            Ok(Value::String("ello".to_string()))
        );
        assert_eq!(
            registry.call("substring", &vec![Value::String("hello".to_string()), Value::Int(2), Value::Int(3)]),
            Ok(Value::String("ell".to_string()))
        );
        // Case-insensitive
        assert_eq!(
            registry.call("SUBSTRING", &vec![Value::String("hello".to_string()), Value::Int(1)]),
            Ok(Value::String("hello".to_string()))
        );
    }

    #[test]
    fn test_trim() {
        let registry = test_registry();
        assert_eq!(
            registry.call("trim", &vec![Value::String("  hello  ".to_string())]),
            Ok(Value::String("hello".to_string()))
        );
        assert_eq!(
            registry.call("TRIM", &vec![Value::String("  hello  ".to_string())]),
            Ok(Value::String("hello".to_string()))
        );
    }

    #[test]
    fn test_string_functions_null_propagation() {
        let registry = test_registry();
        assert_eq!(registry.call("upper", &vec![Value::Null]), Ok(Value::Null));
        assert_eq!(registry.call("lower", &vec![Value::Missing]), Ok(Value::Missing));
        assert_eq!(registry.call("char_length", &vec![Value::Null]), Ok(Value::Null));
        assert_eq!(registry.call("substring", &vec![Value::Null, Value::Int(1)]), Ok(Value::Null));
        assert_eq!(registry.call("substring", &vec![Value::Missing, Value::Int(1)]), Ok(Value::Missing));
        assert_eq!(registry.call("trim", &vec![Value::Null]), Ok(Value::Null));
        assert_eq!(registry.call("trim", &vec![Value::Missing]), Ok(Value::Missing));
    }

    #[test]
    fn test_date_part_all_units() {
        let registry = test_registry();
        let dt = Value::DateTime(chrono::DateTime::parse_from_rfc3339("2015-11-07T18:45:37.691548Z").unwrap());
        assert_eq!(
            registry.call("date_part", &vec![Value::String("second".to_string()), dt.clone()]),
            Ok(Value::Float(OrderedFloat::from(37.0)))
        );
        assert_eq!(
            registry.call("date_part", &vec![Value::String("minute".to_string()), dt.clone()]),
            Ok(Value::Float(OrderedFloat::from(45.0)))
        );
        assert_eq!(
            registry.call("date_part", &vec![Value::String("hour".to_string()), dt.clone()]),
            Ok(Value::Float(OrderedFloat::from(18.0)))
        );
        assert_eq!(
            registry.call("date_part", &vec![Value::String("day".to_string()), dt.clone()]),
            Ok(Value::Float(OrderedFloat::from(7.0)))
        );
        assert_eq!(
            registry.call("date_part", &vec![Value::String("month".to_string()), dt.clone()]),
            Ok(Value::Float(OrderedFloat::from(11.0)))
        );
        assert_eq!(
            registry.call("date_part", &vec![Value::String("year".to_string()), dt.clone()]),
            Ok(Value::Float(OrderedFloat::from(2015.0)))
        );
    }

    #[test]
    fn test_cast_int_to_string() {
        let vars = Variables::default();
        let registry = test_registry();
        let expr = Expression::Cast(
            Box::new(Expression::Constant(Value::Int(42))),
            CastType::Varchar,
        );
        assert_eq!(expr.expression_value(&vars, &registry), Ok(Value::String("42".to_string())));
    }

    #[test]
    fn test_cast_string_to_int() {
        let vars = Variables::default();
        let registry = test_registry();
        let expr = Expression::Cast(
            Box::new(Expression::Constant(Value::String("123".to_string()))),
            CastType::Int,
        );
        assert_eq!(expr.expression_value(&vars, &registry), Ok(Value::Int(123)));
    }

    #[test]
    fn test_cast_string_to_int_invalid() {
        let vars = Variables::default();
        let registry = test_registry();
        let expr = Expression::Cast(
            Box::new(Expression::Constant(Value::String("abc".to_string()))),
            CastType::Int,
        );
        assert_eq!(expr.expression_value(&vars, &registry), Err(ExpressionError::TypeMismatch));
    }

    #[test]
    fn test_cast_float_to_int() {
        let vars = Variables::default();
        let registry = test_registry();
        let expr = Expression::Cast(
            Box::new(Expression::Constant(Value::Float(OrderedFloat::from(3.7f32)))),
            CastType::Int,
        );
        assert_eq!(expr.expression_value(&vars, &registry), Ok(Value::Int(3)));
    }

    #[test]
    fn test_cast_int_to_float() {
        let vars = Variables::default();
        let registry = test_registry();
        let expr = Expression::Cast(
            Box::new(Expression::Constant(Value::Int(5))),
            CastType::Float,
        );
        assert_eq!(expr.expression_value(&vars, &registry), Ok(Value::Float(OrderedFloat::from(5.0f32))));
    }

    #[test]
    fn test_cast_string_to_float() {
        let vars = Variables::default();
        let registry = test_registry();
        let expr = Expression::Cast(
            Box::new(Expression::Constant(Value::String("3.14".to_string()))),
            CastType::Float,
        );
        assert_eq!(expr.expression_value(&vars, &registry), Ok(Value::Float(OrderedFloat::from(3.14f32))));
    }

    #[test]
    fn test_cast_bool_to_int() {
        let vars = Variables::default();
        let registry = test_registry();
        let expr_true = Expression::Cast(
            Box::new(Expression::Constant(Value::Boolean(true))),
            CastType::Int,
        );
        assert_eq!(expr_true.expression_value(&vars, &registry), Ok(Value::Int(1)));

        let expr_false = Expression::Cast(
            Box::new(Expression::Constant(Value::Boolean(false))),
            CastType::Int,
        );
        assert_eq!(expr_false.expression_value(&vars, &registry), Ok(Value::Int(0)));
    }

    #[test]
    fn test_cast_bool_to_string() {
        let vars = Variables::default();
        let registry = test_registry();
        let expr = Expression::Cast(
            Box::new(Expression::Constant(Value::Boolean(true))),
            CastType::Varchar,
        );
        assert_eq!(expr.expression_value(&vars, &registry), Ok(Value::String("true".to_string())));
    }

    #[test]
    fn test_cast_string_to_bool() {
        let vars = Variables::default();
        let registry = test_registry();
        let expr_true = Expression::Cast(
            Box::new(Expression::Constant(Value::String("true".to_string()))),
            CastType::Boolean,
        );
        assert_eq!(expr_true.expression_value(&vars, &registry), Ok(Value::Boolean(true)));

        let expr_false = Expression::Cast(
            Box::new(Expression::Constant(Value::String("FALSE".to_string()))),
            CastType::Boolean,
        );
        assert_eq!(expr_false.expression_value(&vars, &registry), Ok(Value::Boolean(false)));
    }

    #[test]
    fn test_cast_string_to_bool_invalid() {
        let vars = Variables::default();
        let registry = test_registry();
        let expr = Expression::Cast(
            Box::new(Expression::Constant(Value::String("yes".to_string()))),
            CastType::Boolean,
        );
        assert_eq!(expr.expression_value(&vars, &registry), Err(ExpressionError::TypeMismatch));
    }

    #[test]
    fn test_cast_null_propagation() {
        let vars = Variables::default();
        let registry = test_registry();
        let expr = Expression::Cast(
            Box::new(Expression::Constant(Value::Null)),
            CastType::Int,
        );
        assert_eq!(expr.expression_value(&vars, &registry), Ok(Value::Null));
    }

    #[test]
    fn test_cast_missing_propagation() {
        let vars = Variables::default();
        let registry = test_registry();
        let expr = Expression::Cast(
            Box::new(Expression::Constant(Value::Missing)),
            CastType::Varchar,
        );
        assert_eq!(expr.expression_value(&vars, &registry), Ok(Value::Missing));
    }

    #[test]
    fn test_cast_identity() {
        let vars = Variables::default();
        let registry = test_registry();
        // Casting Int to Int should be identity
        let expr = Expression::Cast(
            Box::new(Expression::Constant(Value::Int(42))),
            CastType::Int,
        );
        assert_eq!(expr.expression_value(&vars, &registry), Ok(Value::Int(42)));

        // Casting String to Varchar should be identity
        let expr = Expression::Cast(
            Box::new(Expression::Constant(Value::String("hello".to_string()))),
            CastType::Varchar,
        );
        assert_eq!(expr.expression_value(&vars, &registry), Ok(Value::String("hello".to_string())));
    }

    #[test]
    fn test_cast_float_to_string() {
        let vars = Variables::default();
        let registry = test_registry();
        let expr = Expression::Cast(
            Box::new(Expression::Constant(Value::Float(OrderedFloat::from(2.5f32)))),
            CastType::Varchar,
        );
        assert_eq!(expr.expression_value(&vars, &registry), Ok(Value::String("2.5".to_string())));
    }
}
