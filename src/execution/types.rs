use super::datasource::{ReaderBuilder, ReaderError};
use super::stream::{FilterStream, GroupByStream, InMemoryStream, LimitStream, LogFileStream, MapStream, RecordStream};
use crate::common;
use crate::common::types::{DataSource, Tuple, Value, VariableName, Variables};
use crate::execution::stream::ProjectionStream;
use crate::syntax::ast::PathExpr;
use chrono::Timelike;
use hashbrown::HashMap;
use ordered_float::OrderedFloat;
use pdatastructs::hyperloglog::HyperLogLog;
use std::collections::VecDeque;
use std::io;
use std::result;
use tdigest::TDigest;

pub(crate) type EvaluateResult<T> = result::Result<T, EvaluateError>;

#[derive(thiserror::Error, PartialEq, Eq, Debug)]
pub(crate) enum EvaluateError {
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

pub(crate) type StreamResult<T> = result::Result<T, StreamError>;

#[derive(thiserror::Error, PartialEq, Eq, Debug)]
pub(crate) enum StreamError {
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

pub(crate) type ExpressionResult<T> = result::Result<T, ExpressionError>;

#[derive(thiserror::Error, PartialEq, Eq, Debug)]
pub(crate) enum ExpressionError {
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
pub(crate) enum Ordering {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Expression {
    Constant(Value),
    Logic(Box<Formula>),
    Variable(PathExpr),
    Function(String, Vec<Named>),
    Branch(Box<Formula>, Box<Expression>, Option<Box<Expression>>),
}

impl Expression {
    pub(crate) fn expression_value(&self, variables: &Variables) -> ExpressionResult<Value> {
        match self {
            Expression::Constant(value) => Ok(value.clone()),
            Expression::Logic(formula) => {
                let out = formula.evaluate(variables)?;
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
                            let value = expr.expression_value(&variables)?;
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
            Expression::Branch(condition, then_expr, else_expr) => {
                let choose_then_branch = condition.evaluate(variables)?;

                if choose_then_branch == Some(true) {
                    then_expr.expression_value(variables)
                } else {
                    if let Some(e) = else_expr {
                        e.expression_value(variables)
                    } else {
                        Err(ExpressionError::MissingElse)
                    }
                }
            }
        }
    }
}

fn evaluate_url_functions(func_name: &str, arguments: &[Value]) -> ExpressionResult<Value> {
    match func_name {
        "url_host" => {
            if arguments.len() != 1 {
                return Err(ExpressionError::InvalidArguments);
            }

            match &arguments[0] {
                Value::HttpRequest(r) => {
                    if let Some(host) = r.url.host_str() {
                        Ok(Value::String(host.to_string()))
                    } else {
                        Ok(Value::Null)
                    }
                }
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        "url_port" => {
            if arguments.len() != 1 {
                return Err(ExpressionError::InvalidArguments);
            }

            match &arguments[0] {
                Value::HttpRequest(r) => {
                    if let Some(port) = r.url.port() {
                        Ok(Value::Int(port as i32))
                    } else {
                        Ok(Value::Null)
                    }
                }
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        "url_path" => {
            if arguments.len() != 1 {
                return Err(ExpressionError::InvalidArguments);
            }

            match &arguments[0] {
                Value::HttpRequest(r) => {
                    let url_path = r.url.path();
                    Ok(Value::String(url_path.to_string()))
                }
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        "url_fragment" => {
            if arguments.len() != 1 {
                return Err(ExpressionError::InvalidArguments);
            }

            match &arguments[0] {
                Value::HttpRequest(r) => {
                    if let Some(url_fragment) = r.url.fragment() {
                        Ok(Value::String(url_fragment.to_string()))
                    } else {
                        Ok(Value::Null)
                    }
                }
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        "url_query" => {
            if arguments.len() != 1 {
                return Err(ExpressionError::InvalidArguments);
            }

            match &arguments[0] {
                Value::HttpRequest(r) => {
                    if let Some(url_query) = r.url.query() {
                        Ok(Value::String(url_query.to_string()))
                    } else {
                        Ok(Value::Null)
                    }
                }
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        "url_path_segments" => {
            if arguments.len() != 2 {
                return Err(ExpressionError::InvalidArguments);
            }

            match (&arguments[0], &arguments[1]) {
                (Value::HttpRequest(r), Value::Int(idx)) => {
                    if let Some(url_path_segments) = r.url.path_segments() {
                        let idx = *idx as usize;
                        for (i, segment) in url_path_segments.enumerate() {
                            if i == idx {
                                return Ok(Value::String(segment.to_string()));
                            }
                        }
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Null)
                    }
                }
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        "url_path_bucket" => {
            if arguments.len() != 3 {
                return Err(ExpressionError::InvalidArguments);
            }

            match (&arguments[0], &arguments[1], &arguments[2]) {
                (Value::HttpRequest(r), Value::Int(idx), Value::String(target)) => {
                    if let Some(url_path_segments) = r.url.path_segments() {
                        let idx = *idx as usize;
                        let mut res = String::new();
                        for (i, segment) in url_path_segments.enumerate() {
                            if i == idx {
                                res.push('/');
                                res.push_str(target);
                            } else {
                                res.push('/');
                                res.push_str(segment);
                            }
                        }
                        Ok(Value::String(res))
                    } else {
                        Ok(Value::Null)
                    }
                }
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        _ => Err(ExpressionError::UnknownFunction),
    }
}

fn evaluate_host_functions(func_name: &str, arguments: &[Value]) -> ExpressionResult<Value> {
    match func_name {
        "host_name" => {
            if arguments.len() != 1 {
                return Err(ExpressionError::InvalidArguments);
            }
            match &arguments[0] {
                Value::Host(h) => Ok(Value::String(h.hostname.clone())),
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        "host_port" => {
            if arguments.len() != 1 {
                return Err(ExpressionError::InvalidArguments);
            }

            match &arguments[0] {
                Value::Host(h) => Ok(Value::Int(i32::from(h.port))),
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        _ => Err(ExpressionError::UnknownFunction),
    }
}

fn evaluate(func_name: &str, arguments: &[Value]) -> ExpressionResult<Value> {
    if func_name.starts_with("url_") {
        return evaluate_url_functions(func_name, arguments);
    }

    if func_name.starts_with("host_") {
        return evaluate_host_functions(func_name, arguments);
    }

    match func_name {
        "Plus" => {
            if arguments.len() != 2 {
                return Err(ExpressionError::InvalidArguments);
            }

            // NULL/MISSING propagation: Missing takes precedence over Null
            if matches!(&arguments[0], Value::Missing) || matches!(&arguments[1], Value::Missing) {
                return Ok(Value::Missing);
            }
            if matches!(&arguments[0], Value::Null) || matches!(&arguments[1], Value::Null) {
                return Ok(Value::Null);
            }

            match (&arguments[0], &arguments[1]) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat::from(a.into_inner() + b.into_inner()))),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat::from(*a as f32 + b.into_inner()))),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(OrderedFloat::from(a.into_inner() + *b as f32))),
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        "Minus" => {
            if arguments.len() != 2 {
                return Err(ExpressionError::InvalidArguments);
            }

            if matches!(&arguments[0], Value::Missing) || matches!(&arguments[1], Value::Missing) {
                return Ok(Value::Missing);
            }
            if matches!(&arguments[0], Value::Null) || matches!(&arguments[1], Value::Null) {
                return Ok(Value::Null);
            }

            match (&arguments[0], &arguments[1]) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat::from(a.into_inner() - b.into_inner()))),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat::from(*a as f32 - b.into_inner()))),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(OrderedFloat::from(a.into_inner() - *b as f32))),
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        "Times" => {
            if arguments.len() != 2 {
                return Err(ExpressionError::InvalidArguments);
            }

            if matches!(&arguments[0], Value::Missing) || matches!(&arguments[1], Value::Missing) {
                return Ok(Value::Missing);
            }
            if matches!(&arguments[0], Value::Null) || matches!(&arguments[1], Value::Null) {
                return Ok(Value::Null);
            }

            match (&arguments[0], &arguments[1]) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat::from(a.into_inner() * b.into_inner()))),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat::from(*a as f32 * b.into_inner()))),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(OrderedFloat::from(a.into_inner() * *b as f32))),
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        "Divide" => {
            if arguments.len() != 2 {
                return Err(ExpressionError::InvalidArguments);
            }

            if matches!(&arguments[0], Value::Missing) || matches!(&arguments[1], Value::Missing) {
                return Ok(Value::Missing);
            }
            if matches!(&arguments[0], Value::Null) || matches!(&arguments[1], Value::Null) {
                return Ok(Value::Null);
            }

            match (&arguments[0], &arguments[1]) {
                (Value::Int(a), Value::Int(b)) => {
                    if *b == 0 {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Int(a / b))
                    }
                }
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat::from(a.into_inner() / b.into_inner()))),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat::from(*a as f32 / b.into_inner()))),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(OrderedFloat::from(a.into_inner() / *b as f32))),
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        "date_part" => {
            if arguments.len() != 2 {
                return Err(ExpressionError::InvalidArguments);
            }

            match (&arguments[0], &arguments[1]) {
                (Value::String(date_part_unit_str), Value::DateTime(dt)) => {
                    let date_part_unit = common::types::parse_date_part_unit(date_part_unit_str)?;

                    match date_part_unit {
                        common::types::DatePartUnit::Second => Ok(Value::Float(OrderedFloat::from(dt.second() as f32))),
                        common::types::DatePartUnit::Minute => Ok(Value::Float(OrderedFloat::from(dt.minute() as f32))),
                        _ => Err(ExpressionError::DatePartUnitNotSupported),
                    }
                }
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        "time_bucket" => {
            if arguments.len() != 2 {
                return Err(ExpressionError::InvalidArguments);
            }

            match (&arguments[0], &arguments[1]) {
                (Value::String(time_interval_str), Value::DateTime(dt)) => {
                    let time_interval = common::types::parse_time_interval(time_interval_str)?;

                    if time_interval.n == 0 {
                        return Err(ExpressionError::TimeIntervalZero);
                    }

                    match time_interval.unit {
                        common::types::TimeIntervalUnit::Second => {
                            if time_interval.n > 60 || 60 % time_interval.n != 0 {
                                return Err(ExpressionError::TimeIntervalNotSupported);
                            }

                            let mut target_opt: Option<u32> = None;
                            let step_size: usize = time_interval.n as usize;
                            //FIXME: binary search
                            for point in (0..=60u32).rev().step_by(step_size) {
                                if point <= dt.second() {
                                    target_opt = Some(point);
                                    break;
                                }
                            }

                            if let Some(target) = target_opt {
                                let new_dt = dt.with_second(target).and_then(|d| d.with_nanosecond(0)).unwrap();
                                Ok(Value::DateTime(new_dt))
                            } else {
                                unreachable!();
                            }
                        }
                        common::types::TimeIntervalUnit::Minute => {
                            if time_interval.n > 60 || 60 % time_interval.n != 0 {
                                return Err(ExpressionError::TimeIntervalNotSupported);
                            }

                            let mut target_opt: Option<u32> = None;
                            let step_size: usize = time_interval.n as usize;
                            //FIXME: binary search
                            for point in (0..=60u32).rev().step_by(step_size) {
                                if point <= dt.minute() {
                                    target_opt = Some(point);
                                    break;
                                }
                            }

                            if let Some(target) = target_opt {
                                let new_dt = dt
                                    .with_minute(target)
                                    .and_then(|d| d.with_second(0))
                                    .and_then(|d| d.with_nanosecond(0))
                                    .unwrap();
                                Ok(Value::DateTime(new_dt))
                            } else {
                                unreachable!();
                            }
                        }
                        common::types::TimeIntervalUnit::Hour => {
                            if time_interval.n > 24 || 24 % time_interval.n != 0 {
                                return Err(ExpressionError::TimeIntervalNotSupported);
                            }

                            let mut target_opt: Option<u32> = None;
                            let step_size: usize = time_interval.n as usize;
                            //FIXME: binary search
                            for point in (0..=24u32).rev().step_by(step_size) {
                                if point <= dt.hour() {
                                    target_opt = Some(point);
                                    break;
                                }
                            }

                            if let Some(target) = target_opt {
                                let new_dt = dt
                                    .with_hour(target)
                                    .and_then(|d| d.with_minute(0))
                                    .and_then(|d| d.with_second(0))
                                    .and_then(|d| d.with_nanosecond(0))
                                    .unwrap();
                                Ok(Value::DateTime(new_dt))
                            } else {
                                unreachable!();
                            }
                        }
                        _ => Err(ExpressionError::TimeIntervalNotSupported),
                    }
                }
                _ => Err(ExpressionError::InvalidArguments),
            }
        }
        _ => Err(ExpressionError::UnknownFunction),
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
    pub(crate) fn apply(&self, variables: &Variables, left: &Expression, right: &Expression) -> ExpressionResult<Option<bool>> {
        let left_result = left.expression_value(variables)?;
        let right_result = right.expression_value(variables)?;

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
    IsNull(Box<Expression>),
    IsNotNull(Box<Expression>),
    IsMissing(Box<Expression>),
    IsNotMissing(Box<Expression>),
}

impl Formula {
    pub(crate) fn evaluate(&self, variables: &Variables) -> EvaluateResult<Option<bool>> {
        match self {
            Formula::And(left_formula, right_formula) => {
                let left = left_formula.evaluate(variables)?;
                let right = right_formula.evaluate(variables)?;
                // Three-valued AND: false dominates unknown
                match (left, right) {
                    (Some(false), _) | (_, Some(false)) => Ok(Some(false)),
                    (Some(true), Some(true)) => Ok(Some(true)),
                    _ => Ok(None),
                }
            }
            Formula::Or(left_formula, right_formula) => {
                let left = left_formula.evaluate(variables)?;
                let right = right_formula.evaluate(variables)?;
                // Three-valued OR: true dominates unknown
                match (left, right) {
                    (Some(true), _) | (_, Some(true)) => Ok(Some(true)),
                    (Some(false), Some(false)) => Ok(Some(false)),
                    _ => Ok(None),
                }
            }
            Formula::Not(child_formula) => {
                let child = child_formula.evaluate(variables)?;
                Ok(child.map(|b| !b))
            }
            Formula::Predicate(relation, left_formula, right_formula) => {
                let result = relation.apply(variables, left_formula, right_formula)?;
                Ok(result)
            }
            Formula::Constant(value) => Ok(Some(*value)),
            Formula::IsNull(expr) => {
                let val = expr.expression_value(variables)?;
                Ok(Some(val == Value::Null))
            }
            Formula::IsNotNull(expr) => {
                let val = expr.expression_value(variables)?;
                Ok(Some(val != Value::Null))
            }
            Formula::IsMissing(expr) => {
                let val = expr.expression_value(variables)?;
                Ok(Some(val == Value::Missing))
            }
            Formula::IsNotMissing(expr) => {
                let val = expr.expression_value(variables)?;
                Ok(Some(val != Value::Missing))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Node {
    DataSource(DataSource, Vec<common::types::Binding>),
    Filter(Box<Node>, Box<Formula>),
    Map(Vec<Named>, Box<Node>),
    GroupBy(Vec<PathExpr>, Vec<NamedAggregate>, Box<Node>),
    Limit(u32, Box<Node>),
    OrderBy(Vec<PathExpr>, Vec<Ordering>, Box<Node>),
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
                let record_stream = source.get(variables.clone())?;
                let stream = GroupByStream::new(fields.clone(), variables, named_aggregates.clone(), record_stream);
                Ok(Box::new(stream))
            }
            Node::Limit(row_count, source) => {
                let record_stream = source.get(variables.clone())?;
                let stream = LimitStream::new(*row_count, record_stream);
                Ok(Box::new(stream))
            }
            Node::OrderBy(column_names, orderings, source) => {
                let mut record_stream = source.get(variables.clone())?;
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
                            _ => {
                                unreachable!();
                            }
                        }
                    }

                    std::cmp::Ordering::Equal
                });

                let stream = InMemoryStream::new(VecDeque::from(records));
                Ok(Box::new(stream))
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
pub(crate) struct NamedAggregate {
    pub(crate) aggregate: Aggregate,
    pub(crate) name_opt: Option<String>,
}

impl NamedAggregate {
    pub(crate) fn new(aggregate: Aggregate, name_opt: Option<String>) -> Self {
        NamedAggregate { aggregate, name_opt }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Aggregate {
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
        let v = Value::Host(common::types::parse_host("192.168.131.39:2817").unwrap());
        let name = evaluate_host_functions("host_name", &vec![v.clone()]).unwrap();
        assert_eq!(name, Value::String("192.168.131.39".to_string()));
        let port = evaluate_host_functions("host_port", &vec![v]).unwrap();
        assert_eq!(port, Value::Int(2817));
    }

    #[test]
    fn test_evaluate_url_functions() {
        let v = Value::HttpRequest(
            common::types::parse_http_request(
                "GET http://example.com:8000/users/123?mode=json&after=&iteration=1 HTTP/1.1",
            )
            .unwrap(),
        );
        let name = evaluate_url_functions("url_host", &vec![v.clone()]).unwrap();
        assert_eq!(name, Value::String("example.com".to_string()));
        let port = evaluate_url_functions("url_port", &vec![v.clone()]).unwrap();
        assert_eq!(port, Value::Int(8000));
        let path = evaluate_url_functions("url_path", &vec![v.clone()]).unwrap();
        assert_eq!(path, Value::String("/users/123".to_string()));
        let fragment = evaluate_url_functions("url_fragment", &vec![v.clone()]).unwrap();
        assert_eq!(fragment, Value::Null);
        let query = evaluate_url_functions("url_query", &vec![v.clone()]).unwrap();
        assert_eq!(query, Value::String("mode=json&after=&iteration=1".to_string()));
        let path_segments = evaluate_url_functions("url_path_segments", &vec![v.clone(), Value::Int(1)]).unwrap();
        assert_eq!(path_segments, Value::String("123".to_string()));
        let mapped_path = evaluate_url_functions(
            "url_path_bucket",
            &vec![v.clone(), Value::Int(1), Value::String("_".to_string())],
        )
        .unwrap();
        assert_eq!(mapped_path, Value::String("/users/_".to_string()));
    }

    #[test]
    fn test_evaluate() {
        let v = evaluate("Plus", &vec![Value::Int(1), Value::Int(2)]).unwrap();
        assert_eq!(v, Value::Int(3));

        let v = evaluate("Minus", &vec![Value::Int(2), Value::Int(2)]).unwrap();
        assert_eq!(v, Value::Int(0));

        let v = evaluate("Times", &vec![Value::Int(2), Value::Int(2)]).unwrap();
        assert_eq!(v, Value::Int(4));

        let v = evaluate("Divide", &vec![Value::Int(2), Value::Int(2)]).unwrap();
        assert_eq!(v, Value::Int(1));

        let dt = Value::DateTime(chrono::DateTime::parse_from_rfc3339("2015-11-07T18:45:37.691548Z").unwrap());
        let expected_dt = Value::DateTime(chrono::DateTime::parse_from_rfc3339("2015-11-07T18:45:35.000000Z").unwrap());
        let bucket_dt = evaluate("time_bucket", &vec![Value::String("5 seconds".to_string()), dt.clone()]).unwrap();
        assert_eq!(expected_dt, bucket_dt);

        let expected_dt = Value::DateTime(chrono::DateTime::parse_from_rfc3339("2015-11-07T18:45:00.000000Z").unwrap());
        let bucket_dt = evaluate("time_bucket", &vec![Value::String("5 minutes".to_string()), dt.clone()]).unwrap();
        assert_eq!(expected_dt, bucket_dt);

        let expected_dt = Value::DateTime(chrono::DateTime::parse_from_rfc3339("2015-11-07T18:00:00.000000Z").unwrap());
        let bucket_dt = evaluate("time_bucket", &vec![Value::String("1 hour".to_string()), dt.clone()]).unwrap();
        assert_eq!(expected_dt, bucket_dt);

        let hour = evaluate("date_part", &vec![Value::String("second".to_string()), dt.clone()]).unwrap();
        assert_eq!(Value::Float(OrderedFloat::from(37.0)), hour);
    }

    #[test]
    fn test_arithmetic_null_propagation() {
        assert_eq!(evaluate("Plus", &vec![Value::Null, Value::Int(1)]), Ok(Value::Null));
        assert_eq!(evaluate("Plus", &vec![Value::Int(1), Value::Null]), Ok(Value::Null));
        assert_eq!(evaluate("Plus", &vec![Value::Missing, Value::Int(1)]), Ok(Value::Missing));
        assert_eq!(evaluate("Minus", &vec![Value::Null, Value::Int(1)]), Ok(Value::Null));
        assert_eq!(evaluate("Times", &vec![Value::Null, Value::Int(1)]), Ok(Value::Null));
        assert_eq!(evaluate("Divide", &vec![Value::Null, Value::Int(1)]), Ok(Value::Null));
    }

    #[test]
    fn test_float_arithmetic() {
        assert_eq!(
            evaluate("Plus", &vec![Value::Float(OrderedFloat::from(1.5f32)), Value::Float(OrderedFloat::from(2.5f32))]),
            Ok(Value::Float(OrderedFloat::from(4.0f32)))
        );
        // Mixed Int/Float promotion
        assert_eq!(
            evaluate("Plus", &vec![Value::Int(1), Value::Float(OrderedFloat::from(2.5f32))]),
            Ok(Value::Float(OrderedFloat::from(3.5f32)))
        );
        assert_eq!(
            evaluate("Plus", &vec![Value::Float(OrderedFloat::from(2.5f32)), Value::Int(1)]),
            Ok(Value::Float(OrderedFloat::from(3.5f32)))
        );
    }

    #[test]
    fn test_comparison_int_float_coercion() {
        let vars = Variables::default();
        let rel = Relation::MoreThan;
        let left = Expression::Constant(Value::Float(OrderedFloat::from(2.5f32)));
        let right = Expression::Constant(Value::Int(1));
        assert_eq!(rel.apply(&vars, &left, &right), Ok(Some(true)));

        let rel = Relation::LessThan;
        let left = Expression::Constant(Value::Int(1));
        let right = Expression::Constant(Value::Float(OrderedFloat::from(2.5f32)));
        assert_eq!(rel.apply(&vars, &left, &right), Ok(Some(true)));
    }

    #[test]
    fn test_comparison_null_returns_none() {
        let vars = Variables::default();
        let rel = Relation::Equal;
        let left = Expression::Constant(Value::Null);
        let right = Expression::Constant(Value::Int(1));
        assert_eq!(rel.apply(&vars, &left, &right), Ok(None));

        let left = Expression::Constant(Value::Null);
        let right = Expression::Constant(Value::Null);
        assert_eq!(rel.apply(&vars, &left, &right), Ok(None));
    }

    #[test]
    fn test_three_valued_and() {
        let vars = Variables::default();
        // TRUE AND TRUE = TRUE
        let f = Formula::And(Box::new(Formula::Constant(true)), Box::new(Formula::Constant(true)));
        assert_eq!(f.evaluate(&vars), Ok(Some(true)));
        // FALSE AND TRUE = FALSE
        let f = Formula::And(Box::new(Formula::Constant(false)), Box::new(Formula::Constant(true)));
        assert_eq!(f.evaluate(&vars), Ok(Some(false)));
        // TRUE AND FALSE = FALSE
        let f = Formula::And(Box::new(Formula::Constant(true)), Box::new(Formula::Constant(false)));
        assert_eq!(f.evaluate(&vars), Ok(Some(false)));
        // FALSE AND FALSE = FALSE
        let f = Formula::And(Box::new(Formula::Constant(false)), Box::new(Formula::Constant(false)));
        assert_eq!(f.evaluate(&vars), Ok(Some(false)));
    }

    #[test]
    fn test_three_valued_or() {
        let vars = Variables::default();
        // TRUE OR FALSE = TRUE
        let f = Formula::Or(Box::new(Formula::Constant(true)), Box::new(Formula::Constant(false)));
        assert_eq!(f.evaluate(&vars), Ok(Some(true)));
        // FALSE OR FALSE = FALSE
        let f = Formula::Or(Box::new(Formula::Constant(false)), Box::new(Formula::Constant(false)));
        assert_eq!(f.evaluate(&vars), Ok(Some(false)));
        // FALSE OR TRUE = TRUE
        let f = Formula::Or(Box::new(Formula::Constant(false)), Box::new(Formula::Constant(true)));
        assert_eq!(f.evaluate(&vars), Ok(Some(true)));
        // TRUE OR TRUE = TRUE
        let f = Formula::Or(Box::new(Formula::Constant(true)), Box::new(Formula::Constant(true)));
        assert_eq!(f.evaluate(&vars), Ok(Some(true)));
    }

    #[test]
    fn test_three_valued_not() {
        let vars = Variables::default();
        let f = Formula::Not(Box::new(Formula::Constant(true)));
        assert_eq!(f.evaluate(&vars), Ok(Some(false)));
        let f = Formula::Not(Box::new(Formula::Constant(false)));
        assert_eq!(f.evaluate(&vars), Ok(Some(true)));
    }

    #[test]
    fn test_three_valued_and_with_null() {
        let vars = Variables::default();
        // NULL comparison produces None; TRUE AND None = None
        let null_pred = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Constant(Value::Null)),
            Box::new(Expression::Constant(Value::Int(1))),
        );
        let f = Formula::And(Box::new(Formula::Constant(true)), Box::new(null_pred.clone()));
        assert_eq!(f.evaluate(&vars), Ok(None));
        // FALSE AND None = FALSE (false dominates)
        let f = Formula::And(Box::new(Formula::Constant(false)), Box::new(null_pred));
        assert_eq!(f.evaluate(&vars), Ok(Some(false)));
    }

    #[test]
    fn test_three_valued_or_with_null() {
        let vars = Variables::default();
        let null_pred = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Constant(Value::Null)),
            Box::new(Expression::Constant(Value::Int(1))),
        );
        // TRUE OR None = TRUE (true dominates)
        let f = Formula::Or(Box::new(Formula::Constant(true)), Box::new(null_pred.clone()));
        assert_eq!(f.evaluate(&vars), Ok(Some(true)));
        // FALSE OR None = None
        let f = Formula::Or(Box::new(Formula::Constant(false)), Box::new(null_pred));
        assert_eq!(f.evaluate(&vars), Ok(None));
    }

    #[test]
    fn test_three_valued_not_with_null() {
        let vars = Variables::default();
        let null_pred = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Constant(Value::Null)),
            Box::new(Expression::Constant(Value::Int(1))),
        );
        // NOT None = None
        let f = Formula::Not(Box::new(null_pred));
        assert_eq!(f.evaluate(&vars), Ok(None));
    }

    #[test]
    fn test_is_null() {
        let vars = Variables::default();
        // IS NULL on Null => true
        let f = Formula::IsNull(Box::new(Expression::Constant(Value::Null)));
        assert_eq!(f.evaluate(&vars), Ok(Some(true)));
        // IS NULL on non-null => false
        let f = Formula::IsNull(Box::new(Expression::Constant(Value::Int(1))));
        assert_eq!(f.evaluate(&vars), Ok(Some(false)));
        // IS NULL on Missing => false
        let f = Formula::IsNull(Box::new(Expression::Constant(Value::Missing)));
        assert_eq!(f.evaluate(&vars), Ok(Some(false)));
    }

    #[test]
    fn test_is_not_null() {
        let vars = Variables::default();
        // IS NOT NULL on Null => false
        let f = Formula::IsNotNull(Box::new(Expression::Constant(Value::Null)));
        assert_eq!(f.evaluate(&vars), Ok(Some(false)));
        // IS NOT NULL on non-null => true
        let f = Formula::IsNotNull(Box::new(Expression::Constant(Value::Int(1))));
        assert_eq!(f.evaluate(&vars), Ok(Some(true)));
        // IS NOT NULL on Missing => true
        let f = Formula::IsNotNull(Box::new(Expression::Constant(Value::Missing)));
        assert_eq!(f.evaluate(&vars), Ok(Some(true)));
    }

    #[test]
    fn test_is_missing() {
        let vars = Variables::default();
        // IS MISSING on Missing => true
        let f = Formula::IsMissing(Box::new(Expression::Constant(Value::Missing)));
        assert_eq!(f.evaluate(&vars), Ok(Some(true)));
        // IS MISSING on non-missing => false
        let f = Formula::IsMissing(Box::new(Expression::Constant(Value::Int(1))));
        assert_eq!(f.evaluate(&vars), Ok(Some(false)));
        // IS MISSING on Null => false
        let f = Formula::IsMissing(Box::new(Expression::Constant(Value::Null)));
        assert_eq!(f.evaluate(&vars), Ok(Some(false)));
    }

    #[test]
    fn test_is_not_missing() {
        let vars = Variables::default();
        // IS NOT MISSING on Missing => false
        let f = Formula::IsNotMissing(Box::new(Expression::Constant(Value::Missing)));
        assert_eq!(f.evaluate(&vars), Ok(Some(false)));
        // IS NOT MISSING on non-missing => true
        let f = Formula::IsNotMissing(Box::new(Expression::Constant(Value::Int(1))));
        assert_eq!(f.evaluate(&vars), Ok(Some(true)));
        // IS NOT MISSING on Null => true
        let f = Formula::IsNotMissing(Box::new(Expression::Constant(Value::Null)));
        assert_eq!(f.evaluate(&vars), Ok(Some(true)));
    }
}
