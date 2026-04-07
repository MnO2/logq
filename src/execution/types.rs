use super::datasource::{ReaderBuilder, ReaderError};
use super::stream::{CrossJoinStream, DistinctStream, ExceptStream, FilterStream, GroupByStream, InMemoryStream, IntersectStream, LeftJoinStream, LimitStream, LogFileStream, MapStream, RecordStream, UnionStream};
use crate::common;
use crate::common::types::{DataSource, Tuple, Value, VariableName, Variables};
use crate::execution::batch::{BatchSchema, BatchToRowAdapter, BatchStream, PrecomputedBatchStream};
use crate::execution::batch_scan::datatype_to_column_type;
use crate::execution::parallel;
use crate::execution::batch_filter::BatchFilterOperator;
use crate::execution::batch_limit::BatchLimitOperator;
use crate::execution::batch_project::BatchProjectOperator;
use crate::execution::batch_scan::BatchScanOperator;
use crate::execution::log_schema::LogSchema;
use crate::execution::stream::ProjectionStream;
use crate::functions::FunctionRegistry;
use crate::syntax::ast::{CastType, PathExpr, PathSegment};
use hashbrown::HashMap;
use ordered_float::OrderedFloat;
use pdatastructs::hyperloglog::HyperLogLog;
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
                // Fast path: single-segment AttrName is the overwhelmingly common case
                if path_expr.path_segments.len() == 1 {
                    if let PathSegment::AttrName(ref name) = path_expr.path_segments[0] {
                        return Ok(variables.get(name).cloned().unwrap_or(Value::Missing));
                    }
                }
                Ok(common::types::get_value_by_path_expr(path_expr, 0, variables))
            }
            Expression::Function(name, arguments) => {
                let mut values: Vec<Value> = Vec::with_capacity(arguments.len());
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
                let mut stream = node.get(variables.clone(), registry.clone(), 0)
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
    /// Compare two Values by reference. Avoids cloning when values are
    /// already available as references (e.g., from LinkedHashMap::get).
    fn compare_ref(&self, left: &Value, right: &Value) -> ExpressionResult<Option<bool>> {
        if matches!(left, Value::Null | Value::Missing) || matches!(right, Value::Null | Value::Missing) {
            return Ok(None);
        }
        match self {
            Relation::Equal => Ok(Some(left == right)),
            Relation::NotEqual => Ok(Some(left != right)),
            Relation::GreaterEqual => match (left, right) {
                (Value::Int(l), Value::Int(r)) => Ok(Some(l >= r)),
                (Value::Float(l), Value::Float(r)) => Ok(Some(l >= r)),
                (Value::Int(l), Value::Float(r)) => Ok(Some(OrderedFloat::from(*l as f32) >= *r)),
                (Value::Float(l), Value::Int(r)) => Ok(Some(*l >= OrderedFloat::from(*r as f32))),
                (Value::String(l), Value::String(r)) => Ok(Some(l >= r)),
                (Value::DateTime(l), Value::DateTime(r)) => Ok(Some(l >= r)),
                _ => Err(ExpressionError::TypeMismatch),
            },
            Relation::LessEqual => match (left, right) {
                (Value::Int(l), Value::Int(r)) => Ok(Some(l <= r)),
                (Value::Float(l), Value::Float(r)) => Ok(Some(l <= r)),
                (Value::Int(l), Value::Float(r)) => Ok(Some(OrderedFloat::from(*l as f32) <= *r)),
                (Value::Float(l), Value::Int(r)) => Ok(Some(*l <= OrderedFloat::from(*r as f32))),
                (Value::String(l), Value::String(r)) => Ok(Some(l <= r)),
                (Value::DateTime(l), Value::DateTime(r)) => Ok(Some(l <= r)),
                _ => Err(ExpressionError::TypeMismatch),
            },
            Relation::MoreThan => match (left, right) {
                (Value::Int(l), Value::Int(r)) => Ok(Some(l > r)),
                (Value::Float(l), Value::Float(r)) => Ok(Some(l > r)),
                (Value::Int(l), Value::Float(r)) => Ok(Some(OrderedFloat::from(*l as f32) > *r)),
                (Value::Float(l), Value::Int(r)) => Ok(Some(*l > OrderedFloat::from(*r as f32))),
                (Value::String(l), Value::String(r)) => Ok(Some(l > r)),
                (Value::DateTime(l), Value::DateTime(r)) => Ok(Some(l > r)),
                _ => Err(ExpressionError::TypeMismatch),
            },
            Relation::LessThan => match (left, right) {
                (Value::Int(l), Value::Int(r)) => Ok(Some(l < r)),
                (Value::Float(l), Value::Float(r)) => Ok(Some(l < r)),
                (Value::Int(l), Value::Float(r)) => Ok(Some(OrderedFloat::from(*l as f32) < *r)),
                (Value::Float(l), Value::Int(r)) => Ok(Some(*l < OrderedFloat::from(*r as f32))),
                (Value::String(l), Value::String(r)) => Ok(Some(l < r)),
                (Value::DateTime(l), Value::DateTime(r)) => Ok(Some(l < r)),
                _ => Err(ExpressionError::TypeMismatch),
            },
        }
    }

    pub(crate) fn apply(&self, variables: &Variables, left: &Expression, right: &Expression, registry: &Arc<FunctionRegistry>) -> ExpressionResult<Option<bool>> {
        let left_result = left.expression_value(variables, registry)?;
        let right_result = right.expression_value(variables, registry)?;
        self.compare_ref(&left_result, &right_result)
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

use std::cell::RefCell;
use std::num::NonZeroUsize;

thread_local! {
    static LIKE_REGEX_CACHE: RefCell<lru::LruCache<String, regex::Regex>> = RefCell::new(
        lru::LruCache::new(NonZeroUsize::new(64).unwrap())
    );
}

fn get_or_compile_like_regex(pattern: &str) -> Result<regex::Regex, EvaluateError> {
    LIKE_REGEX_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(re) = cache.get(pattern) {
            return Ok(re.clone());
        }
        let regex_pattern = like_pattern_to_regex(pattern);
        let re = regex::Regex::new(&regex_pattern)
            .map_err(|_| EvaluateError::Expression(ExpressionError::TypeMismatch))?;
        cache.put(pattern.to_string(), re.clone());
        Ok(re)
    })
}

impl Formula {
    pub(crate) fn evaluate(&self, variables: &Variables, registry: &Arc<FunctionRegistry>) -> EvaluateResult<Option<bool>> {
        match self {
            Formula::And(left_formula, right_formula) => {
                let left = left_formula.evaluate(variables, registry)?;
                // Three-valued AND: false dominates — short-circuit
                if left == Some(false) {
                    return Ok(Some(false));
                }
                let right = right_formula.evaluate(variables, registry)?;
                match (left, right) {
                    (_, Some(false)) => Ok(Some(false)),
                    (Some(true), Some(true)) => Ok(Some(true)),
                    _ => Ok(None),
                }
            }
            Formula::Or(left_formula, right_formula) => {
                let left = left_formula.evaluate(variables, registry)?;
                // Three-valued OR: true dominates — short-circuit
                if left == Some(true) {
                    return Ok(Some(true));
                }
                let right = right_formula.evaluate(variables, registry)?;
                match (left, right) {
                    (_, Some(true)) => Ok(Some(true)),
                    (Some(false), Some(false)) => Ok(Some(false)),
                    _ => Ok(None),
                }
            }
            Formula::Not(child_formula) => {
                let child = child_formula.evaluate(variables, registry)?;
                Ok(child.map(|b| !b))
            }
            Formula::Predicate(relation, left_formula, right_formula) => {
                // Fast path: Variable op Constant — avoid expression_value overhead
                // by looking up the variable directly and comparing by reference.
                match (left_formula.as_ref(), right_formula.as_ref()) {
                    (Expression::Variable(pe), Expression::Constant(constant))
                        if pe.path_segments.len() == 1 =>
                    {
                        if let PathSegment::AttrName(ref name) = pe.path_segments[0] {
                            match variables.get(name) {
                                Some(val) => return Ok(relation.compare_ref(val, constant)?),
                                None => return Ok(None), // Missing
                            }
                        }
                    }
                    (Expression::Constant(constant), Expression::Variable(pe))
                        if pe.path_segments.len() == 1 =>
                    {
                        if let PathSegment::AttrName(ref name) = pe.path_segments[0] {
                            match variables.get(name) {
                                Some(val) => return Ok(relation.compare_ref(constant, val)?),
                                None => return Ok(None),
                            }
                        }
                    }
                    _ => {}
                }
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
                        let re = get_or_compile_like_regex(p)?;
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
                        let re = get_or_compile_like_regex(p)?;
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
                                return Ok(Some(false));
                            }
                        }
                        if has_null {
                            Ok(None)
                        } else {
                            Ok(Some(true))
                        }
                    }
                }
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
    /// Walk down the plan tree to find the datasource format and path.
    fn find_datasource_format(&self) -> Option<(String, std::path::PathBuf)> {
        match self {
            Node::DataSource(DataSource::File(path, format, _), _) => {
                Some((format.clone(), path.clone()))
            }
            Node::Filter(source, _)
            | Node::Map(_, source)
            | Node::Limit(_, source)
            | Node::Distinct(source)
            | Node::OrderBy(_, _, source) => source.find_datasource_format(),
            Node::GroupBy(_, _, source) => source.find_datasource_format(),
            _ => None,
        }
    }

    /// Compute the set of field indices needed by this plan tree.
    /// Returns an empty vec for JSONL or when the format is unknown.
    fn compute_required_fields_for_batch(&self) -> Vec<usize> {
        if let Some((format, _)) = self.find_datasource_format() {
            if format != "jsonl" {
                let schema = LogSchema::from_format(&format);
                return crate::execution::field_analysis::extract_required_fields(self, &schema);
            }
        }
        vec![]
    }

    /// Try to build a batch pipeline for this node subtree.
    /// Returns None if this node pattern isn't supported for batch execution.
    fn try_get_batch(
        &self,
        variables: &Variables,
        registry: &Arc<FunctionRegistry>,
        required_fields: &[usize],
        threads: usize,
    ) -> Option<CreateStreamResult<Box<dyn BatchStream>>> {
        match self {
            Node::DataSource(data_source, bindings) => {
                if !bindings.is_empty() {
                    return None; // bindings need row-based ProjectionStream
                }
                match data_source {
                    DataSource::File(path, file_format, _) => {
                        if file_format == "jsonl" {
                            return None; // JSONL has no fixed schema
                        }
                        let schema = LogSchema::from_format(file_format);
                        let fields = if required_fields.is_empty() {
                            (0..schema.field_count()).collect()
                        } else {
                            required_fields.to_vec()
                        };
                        // Try parallel mmap path
                        if threads > 1 {
                            if let parallel::ScanStrategy::Mmap(mmap) = parallel::choose_strategy(path) {
                                match parallel::parallel_scan_chunks(
                                    &mmap, threads, &schema, &fields, &[], &None,
                                ) {
                                    Ok(chunk_batches) => {
                                        let batch_schema = BatchSchema {
                                            names: fields.iter().map(|&i| schema.field_name(i).to_string()).collect(),
                                            types: fields.iter().map(|&i| datatype_to_column_type(&schema.field_type(i))).collect(),
                                        };
                                        let all_batches: Vec<_> = chunk_batches.into_iter().flatten().collect();
                                        return Some(Ok(Box::new(PrecomputedBatchStream::new(all_batches, batch_schema))));
                                    }
                                    Err(_) => {} // fall through to sequential
                                }
                            }
                        }
                        // Sequential fallback
                        match std::fs::File::open(path) {
                            Ok(file) => {
                                let reader: Box<dyn std::io::BufRead> =
                                    Box::new(std::io::BufReader::new(file));
                                let scan = BatchScanOperator::new(reader, schema, fields, vec![], None);
                                Some(Ok(Box::new(scan) as Box<dyn BatchStream>))
                            }
                            Err(_) => Some(Err(CreateStreamError::Io)),
                        }
                    }
                    _ => None,
                }
            }
            Node::Filter(source, formula) => {
                // Try predicate pushdown into DataSource
                if let Node::DataSource(ds, bindings) = &**source {
                    if bindings.is_empty() {
                        if let DataSource::File(path, file_format, _) = ds {
                            if file_format != "jsonl" {
                                let schema = LogSchema::from_format(file_format);
                                let filter_fields = crate::execution::field_analysis::extract_fields_from_formula(formula, &schema);
                                // Union filter fields with required_fields from parent
                                let mut all_fields = required_fields.to_vec();
                                for &fi in &filter_fields {
                                    if !all_fields.contains(&fi) {
                                        all_fields.push(fi);
                                    }
                                }
                                all_fields.sort();
                                all_fields.dedup();
                                if all_fields.is_empty() {
                                    all_fields = (0..schema.field_count()).collect();
                                }

                                // Try parallel mmap path with pushed predicate
                                if threads > 1 {
                                    if let parallel::ScanStrategy::Mmap(mmap) = parallel::choose_strategy(path) {
                                        let pushed = Some((*formula.clone(), variables.clone(), registry.clone()));
                                        match parallel::parallel_scan_chunks(
                                            &mmap, threads, &schema, &all_fields, &filter_fields, &pushed,
                                        ) {
                                            Ok(chunk_batches) => {
                                                let batch_schema = BatchSchema {
                                                    names: all_fields.iter().map(|&i| schema.field_name(i).to_string()).collect(),
                                                    types: all_fields.iter().map(|&i| datatype_to_column_type(&schema.field_type(i))).collect(),
                                                };
                                                let all_batches: Vec<_> = chunk_batches.into_iter().flatten().collect();
                                                return Some(Ok(Box::new(PrecomputedBatchStream::new(all_batches, batch_schema))));
                                            }
                                            Err(_) => {} // fall through to sequential
                                        }
                                    }
                                }

                                // Sequential fallback
                                match std::fs::File::open(path) {
                                    Ok(file) => {
                                        let reader: Box<dyn std::io::BufRead> =
                                            Box::new(std::io::BufReader::new(file));
                                        let scan = BatchScanOperator::new(
                                            reader, schema, all_fields,
                                            filter_fields,
                                            Some((*formula.clone(), variables.clone(), registry.clone())),
                                        );
                                        return Some(Ok(Box::new(scan) as Box<dyn BatchStream>));
                                    }
                                    Err(_) => return Some(Err(CreateStreamError::Io)),
                                }
                            }
                        }
                    }
                }
                // Fallback: wrap with BatchFilterOperator for non-DataSource children
                match source.try_get_batch(variables, registry, required_fields, threads) {
                    Some(Ok(batch_stream)) => {
                        let filter = BatchFilterOperator::new(
                            batch_stream, *formula.clone(), variables.clone(), registry.clone(),
                        );
                        Some(Ok(Box::new(filter) as Box<dyn BatchStream>))
                    }
                    Some(Err(e)) => Some(Err(e)),
                    None => None,
                }
            }
            Node::Limit(count, source) => {
                match source.try_get_batch(variables, registry, required_fields, threads) {
                    Some(Ok(batch_stream)) => {
                        let limit = BatchLimitOperator::new(batch_stream, *count);
                        Some(Ok(Box::new(limit) as Box<dyn BatchStream>))
                    }
                    Some(Err(e)) => Some(Err(e)),
                    None => None,
                }
            }
            Node::Map(named_list, source) => {
                // Only handle simple variable projections in batch path
                let is_simple = named_list.iter().all(|named| match named {
                    Named::Expression(Expression::Variable(_), _) => true,
                    Named::Star => true,
                    _ => false,
                });
                if !is_simple {
                    return None; // Complex expressions need row-based MapStream
                }
                // SELECT * over a bare DataSource gains nothing from batch —
                // columnar parse + BatchToRowAdapter is pure overhead.
                if named_list.iter().any(|n| matches!(n, Named::Star))
                    && matches!(**source, Node::DataSource(..))
                {
                    return None;
                }
                match source.try_get_batch(variables, registry, required_fields, threads) {
                    Some(Ok(batch_stream)) => {
                        // Extract output column names from named_list
                        let output_names: Vec<String> = named_list.iter().filter_map(|named| {
                            match named {
                                Named::Expression(Expression::Variable(path), _) => {
                                    path.path_segments.last().and_then(|seg| {
                                        if let PathSegment::AttrName(name) = seg {
                                            Some(name.clone())
                                        } else {
                                            None
                                        }
                                    })
                                }
                                _ => None,
                            }
                        }).collect();
                        if output_names.is_empty() && named_list.iter().any(|n| matches!(n, Named::Star)) {
                            // SELECT * — pass through without projection
                            return Some(Ok(batch_stream));
                        }
                        let project = BatchProjectOperator::new(batch_stream, output_names);
                        Some(Ok(Box::new(project) as Box<dyn BatchStream>))
                    }
                    Some(Err(e)) => Some(Err(e)),
                    None => None,
                }
            }
            Node::GroupBy(keys, aggregates, source) => {
                match source.try_get_batch(variables, registry, required_fields, threads) {
                    Some(Ok(batch_stream)) => {
                        let groupby = crate::execution::batch_groupby::BatchGroupByOperator::new(
                            batch_stream,
                            keys.clone(),
                            aggregates.clone(),
                            variables.clone(),
                            registry.clone(),
                        );
                        Some(Ok(Box::new(groupby) as Box<dyn BatchStream>))
                    }
                    Some(Err(e)) => Some(Err(e)),
                    None => None,
                }
            }
            _ => None,
        }
    }

    pub fn get(&self, variables: Variables, registry: Arc<FunctionRegistry>, threads: usize) -> CreateStreamResult<Box<dyn RecordStream>> {
        // Try batch pipeline first for supported node patterns.
        // Skip for bare DataSource — the columnar round-trip (parse to columns →
        // BatchToRowAdapter back to rows) is slower than direct row-based parsing
        // when there's no downstream operator (Filter, GroupBy) that benefits.
        if !matches!(self, Node::DataSource(..)) {
            let required = self.compute_required_fields_for_batch();
            if let Some(batch_result) = self.try_get_batch(&variables, &registry, &required, threads) {
                let batch_stream = batch_result?;
                return Ok(Box::new(BatchToRowAdapter::new(batch_stream)));
            }
        }

        match self {
            Node::Filter(source, formula) => {
                let record_stream = source.get(variables.clone(), registry.clone(), threads)?;
                let stream = FilterStream::new(*formula.clone(), variables, record_stream, registry);
                Ok(Box::new(stream))
            }
            Node::Map(named_list, source) => {
                let record_stream = source.get(variables.clone(), registry.clone(), threads)?;

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
                let record_stream = source.get(variables.clone(), registry.clone(), threads)?;
                let stream = GroupByStream::new(fields.clone(), variables, named_aggregates.clone(), record_stream, registry);
                Ok(Box::new(stream))
            }
            Node::Limit(row_count, source) => {
                let record_stream = source.get(variables.clone(), registry, threads)?;
                let stream = LimitStream::new(*row_count, record_stream);
                Ok(Box::new(stream))
            }
            Node::OrderBy(column_names, orderings, source) => {
                let mut record_stream = source.get(variables.clone(), registry, threads)?;
                let mut records = Vec::new();

                while let Some(record) = record_stream.next()? {
                    records.push(record);
                }

                let encoder = super::prefix_sort::PrefixSortEncoder::default();
                let sorted = encoder.sort(records, column_names, orderings);

                let stream = InMemoryStream::new(sorted);
                Ok(Box::new(stream))
            }
            Node::Distinct(source) => {
                let record_stream = source.get(variables, registry, threads)?;
                Ok(Box::new(DistinctStream::new(record_stream)))
            }
            Node::CrossJoin(left, right) => {
                let left_stream = left.get(variables.clone(), registry.clone(), threads)?;
                let right_node = *right.clone();
                let right_variables = variables;
                Ok(Box::new(CrossJoinStream::new(left_stream, right_node, right_variables, registry, threads)))
            }
            Node::LeftJoin(left, right, condition) => {
                let left_stream = left.get(variables.clone(), registry.clone(), threads)?;
                let right_node = *right.clone();
                let right_variables = variables;
                Ok(Box::new(LeftJoinStream::new(left_stream, right_node, right_variables, *condition.clone(), registry, threads)))
            }
            Node::Union(left, right) => {
                let left_stream = left.get(variables.clone(), registry.clone(), threads)?;
                let right_stream = right.get(variables, registry, threads)?;
                Ok(Box::new(UnionStream::new(left_stream, right_stream)))
            }
            Node::Intersect(left, right, all) => {
                let left_stream = left.get(variables.clone(), registry.clone(), threads)?;
                let right_stream = right.get(variables, registry, threads)?;
                Ok(Box::new(IntersectStream::new(left_stream, right_stream, *all)?))
            }
            Node::Except(left, right, all) => {
                let left_stream = left.get(variables.clone(), registry.clone(), threads)?;
                let right_stream = right.get(variables, registry, threads)?;
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
    pub(crate) sums: HashMap<Option<Tuple>, OrderedFloat<f64>>,
    pub(crate) counts: HashMap<Option<Tuple>, i64>,
}

impl AvgAggregate {
    pub(crate) fn new() -> Self {
        AvgAggregate {
            sums: HashMap::new(),
            counts: HashMap::new(),
        }
    }

    pub(crate) fn add_record(&mut self, key: &Option<Tuple>, value: &Value) -> AggregateResult<()> {
        let new_value: f64 = match value {
            &Value::Int(i) => i as f64,
            &Value::Float(f) => f.into_inner() as f64,
            _ => {
                return Err(AggregateError::InvalidType);
            }
        };

        *self.sums.entry(key.clone()).or_insert(OrderedFloat(0.0)) += new_value;
        *self.counts.entry(key.clone()).or_insert(0) += 1;
        Ok(())
    }

    pub(crate) fn get_aggregated(&self, key: &Option<Tuple>) -> AggregateResult<Value> {
        if let (Some(&sum), Some(&count)) = (self.sums.get(key), self.counts.get(key)) {
            Ok(Value::Float(OrderedFloat::from((sum.into_inner() / count as f64) as f32)))
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

        if let Some(sum) = self.sums.get_mut(key) {
            let f32_sum: f32 = (*sum).into();
            let f32_new_value: f32 = new_value.into();
            *sum = OrderedFloat::from(f32_sum + f32_new_value);
        } else {
            self.sums.insert(key.clone(), new_value);
        }
        Ok(())
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

        if let Some(count) = self.counts.get_mut(key) {
            *count += 1;
        } else {
            self.counts.insert(key.clone(), 1);
        }
        Ok(())
    }

    pub(crate) fn add_row(&mut self, key: &Option<Tuple>) -> AggregateResult<()> {
        if let Some(count) = self.counts.get_mut(key) {
            *count += 1;
        } else {
            self.counts.insert(key.clone(), 1);
        }
        Ok(())
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
        } else {
            self.tuples.insert(key.clone(), vec![value.clone()]);
        }
        Ok(())
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
        match self.maxs.entry(key.clone()) {
            hashbrown::hash_map::Entry::Occupied(mut e) => {
                let less_than = match (e.get(), value) {
                    (&Value::Int(i1), &Value::Int(i2)) => i1 < i2,
                    (&Value::Float(f1), &Value::Float(f2)) => f1 < f2,
                    _ => {
                        return Err(AggregateError::InvalidType);
                    }
                };
                if less_than {
                    e.insert(value.clone());
                }
                Ok(())
            }
            hashbrown::hash_map::Entry::Vacant(e) => {
                e.insert(value.clone());
                Ok(())
            }
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
        match self.mins.entry(key.clone()) {
            hashbrown::hash_map::Entry::Occupied(mut e) => {
                let greater_than = match (e.get(), value) {
                    (&Value::Int(i1), &Value::Int(i2)) => i1 > i2,
                    (&Value::Float(f1), &Value::Float(f2)) => f1 > f2,
                    _ => {
                        return Err(AggregateError::InvalidType);
                    }
                };
                if greater_than {
                    e.insert(value.clone());
                }
                Ok(())
            }
            hashbrown::hash_map::Entry::Vacant(e) => {
                e.insert(value.clone());
                Ok(())
            }
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
        if !self.firsts.contains_key(key) {
            self.firsts.insert(key.clone(), value.clone());
        }
        Ok(())
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
        } else {
            let mut hll = HyperLogLog::new(8);
            hll.add(value);
            self.counts.insert(key.clone(), hll);
        }
        Ok(())
    }

    pub(crate) fn get_aggregated(&self, key: &Option<Tuple>) -> AggregateResult<Value> {
        if let Some(hll) = self.counts.get(key) {
            Ok(Value::Int(hll.count() as i32))
        } else {
            Err(AggregateError::KeyNotFound)
        }
    }
}

/// How a given aggregate extracts its input value from each row.
#[derive(Debug, Clone)]
pub(crate) enum ExtractionStrategy {
    /// Evaluate an expression against the row's variables (most aggregates).
    Expression(Expression),
    /// Look up a column by name in the row's variables (PercentileDisc, ApproxPercentile).
    ColumnLookup(String),
    /// Capture the entire record as a Value::Object (GroupAs).
    RecordCapture,
    /// No value needed; just count rows (count(*)).
    None,
}

/// What kind of accumulator to create (no data, just the type tag).
#[derive(Debug, Clone)]
pub(crate) enum AccumulatorKind {
    Count,
    CountStar,
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    GroupAs,
    ApproxCountDistinct,
    PercentileDisc { percentile: OrderedFloat<f32>, ordering: Ordering },
    ApproxPercentile { percentile: OrderedFloat<f32>, ordering: Ordering },
}

/// Per-group accumulator state (one per aggregate in the SELECT).
///
/// Derives `Debug` and `Clone` only.
/// Does NOT derive `PartialEq` or `Eq` because:
///   - `HyperLogLog` (pdatastructs) does not implement `PartialEq`
///   - `TDigest` does not soundly implement `Eq` (NaN in f64 centroids)
#[derive(Debug, Clone)]
pub(crate) enum AccumulatorState {
    Count(i64),
    CountStar(i64),
    Sum(Option<OrderedFloat<f32>>),
    Avg { sum: OrderedFloat<f64>, count: i64 },
    Min(Option<Value>),
    Max(Option<Value>),
    First(Option<Value>),
    Last(Option<Value>),
    GroupAs(Vec<Value>),
    ApproxCountDistinct(HyperLogLog<Value>),
    PercentileDisc { values: Vec<Value>, percentile: OrderedFloat<f32>, ordering: Ordering },
    // ordering is stored for future use (TDigest quantile estimation is always ascending)
    #[allow(dead_code)]
    ApproxPercentile { digest: TDigest, buffer: Vec<Value>, percentile: OrderedFloat<f32>, ordering: Ordering },
}

impl AccumulatorState {
    /// Create a fresh accumulator from its kind tag.
    pub(crate) fn new(kind: &AccumulatorKind) -> Self {
        match kind {
            AccumulatorKind::Count => AccumulatorState::Count(0),
            AccumulatorKind::CountStar => AccumulatorState::CountStar(0),
            AccumulatorKind::Sum => AccumulatorState::Sum(None),
            AccumulatorKind::Avg => AccumulatorState::Avg {
                sum: OrderedFloat(0.0),
                count: 0,
            },
            AccumulatorKind::Min => AccumulatorState::Min(None),
            AccumulatorKind::Max => AccumulatorState::Max(None),
            AccumulatorKind::First => AccumulatorState::First(None),
            AccumulatorKind::Last => AccumulatorState::Last(None),
            AccumulatorKind::GroupAs => AccumulatorState::GroupAs(Vec::new()),
            AccumulatorKind::ApproxCountDistinct => {
                AccumulatorState::ApproxCountDistinct(HyperLogLog::new(8))
            }
            AccumulatorKind::PercentileDisc { percentile, ordering } => {
                AccumulatorState::PercentileDisc {
                    values: Vec::new(),
                    percentile: *percentile,
                    ordering: ordering.clone(),
                }
            }
            AccumulatorKind::ApproxPercentile { percentile, ordering } => {
                AccumulatorState::ApproxPercentile {
                    digest: TDigest::new_with_size(100),
                    buffer: Vec::new(),
                    percentile: *percentile,
                    ordering: ordering.clone(),
                }
            }
        }
    }

    /// Per-row update with a value.
    ///
    /// Null/Missing policy:
    /// - Value::Missing: always skipped (no-op) for all variants
    /// - Value::Null: skipped for Count, Sum, Avg, and ApproxCountDistinct;
    ///   accumulated normally for Min/Max/First/Last/GroupAs
    pub(crate) fn accumulate(&mut self, val: &Value) -> AggregateResult<()> {
        // Universal Missing skip
        if matches!(val, Value::Missing) {
            return Ok(());
        }
        match self {
            AccumulatorState::Count(count) => {
                if !matches!(val, Value::Null) {
                    *count += 1;
                }
            }
            AccumulatorState::Sum(opt_sum) => {
                if matches!(val, Value::Null) {
                    return Ok(());
                }
                let fval = match val {
                    Value::Int(i) => OrderedFloat::from(*i as f32),
                    Value::Float(f) => *f,
                    _ => return Err(AggregateError::InvalidType),
                };
                *opt_sum = Some(match opt_sum {
                    Some(s) => OrderedFloat(s.into_inner() + fval.into_inner()),
                    None => fval,
                });
            }
            AccumulatorState::Avg { sum, count } => {
                if matches!(val, Value::Null) {
                    return Ok(());
                }
                let fval: f64 = match val {
                    Value::Int(i) => *i as f64,
                    Value::Float(f) => f.into_inner() as f64,
                    _ => return Err(AggregateError::InvalidType),
                };
                *sum += fval;
                *count += 1;
            }
            AccumulatorState::Min(current) => {
                match current {
                    None => {
                        *current = Some(val.clone());
                    }
                    Some(cur) => {
                        if value_less_than(val, cur) {
                            *current = Some(val.clone());
                        }
                    }
                }
            }
            AccumulatorState::Max(current) => {
                match current {
                    None => {
                        *current = Some(val.clone());
                    }
                    Some(cur) => {
                        if value_less_than(cur, val) {
                            *current = Some(val.clone());
                        }
                    }
                }
            }
            AccumulatorState::First(slot) => {
                if slot.is_none() {
                    *slot = Some(val.clone());
                }
            }
            AccumulatorState::Last(slot) => {
                *slot = Some(val.clone());
            }
            AccumulatorState::GroupAs(vals) => {
                vals.push(val.clone());
            }
            AccumulatorState::ApproxCountDistinct(hll) => {
                if !matches!(val, Value::Null) {
                    hll.add(val);
                }
            }
            AccumulatorState::PercentileDisc { values, .. } => {
                values.push(val.clone());
            }
            AccumulatorState::ApproxPercentile { digest, buffer, .. } => {
                buffer.push(val.clone());
                if buffer.len() >= 10000 {
                    let mut fvec = Vec::new();
                    for v in buffer.iter() {
                        match v {
                            Value::Float(f) => fvec.push(f64::from(f.into_inner())),
                            Value::Int(i) => fvec.push(f64::from(*i)),
                            _ => return Err(AggregateError::InvalidType),
                        }
                    }
                    *digest = digest.merge_unsorted(fvec);
                    buffer.clear();
                }
            }
            AccumulatorState::CountStar(_) => {
                unreachable!("CountStar should use accumulate_row()");
            }
        }
        Ok(())
    }

    /// Per-row update with no value (for count(*) only).
    pub(crate) fn accumulate_row(&mut self) -> AggregateResult<()> {
        match self {
            AccumulatorState::CountStar(count) => {
                *count += 1;
                Ok(())
            }
            _ => unreachable!("accumulate_row called on non-CountStar"),
        }
    }

    /// Produce the output value for emission.
    ///
    /// Takes `&mut self` because `ApproxPercentile` must flush its internal
    /// buffer into the TDigest before computing the quantile.
    pub(crate) fn finalize(&mut self) -> AggregateResult<Value> {
        match self {
            AccumulatorState::Count(c) | AccumulatorState::CountStar(c) => {
                Ok(Value::Int(*c as i32))
            }
            AccumulatorState::Sum(opt_sum) => {
                Ok(opt_sum.map(Value::Float).unwrap_or(Value::Null))
            }
            AccumulatorState::Avg { sum, count } => {
                if *count == 0 {
                    return Ok(Value::Null);
                }
                Ok(Value::Float(OrderedFloat(
                    (sum.into_inner() / *count as f64) as f32,
                )))
            }
            AccumulatorState::Min(v)
            | AccumulatorState::Max(v)
            | AccumulatorState::First(v)
            | AccumulatorState::Last(v) => Ok(v.clone().unwrap_or(Value::Null)),
            AccumulatorState::GroupAs(vals) => Ok(Value::Array(vals.clone())),
            AccumulatorState::ApproxCountDistinct(hll) => {
                Ok(Value::Int(hll.count() as i32))
            }
            AccumulatorState::PercentileDisc {
                values,
                percentile,
                ordering,
            } => {
                if values.is_empty() {
                    return Ok(Value::Null);
                }
                values.sort_by(|a, b| value_cmp(a, b, ordering));
                let f32_pct: f32 = percentile.into_inner();
                let idx = ((values.len() as f32) * f32_pct) as usize;
                Ok(values[idx].clone())
            }
            AccumulatorState::ApproxPercentile {
                digest,
                buffer,
                percentile,
                ..
            } => {
                if buffer.is_empty() && digest.count() <= f64::EPSILON {
                    return Ok(Value::Null);
                }
                // Flush remaining buffer
                if !buffer.is_empty() {
                    let mut fvec = Vec::new();
                    for v in buffer.iter() {
                        match v {
                            Value::Float(f) => fvec.push(f64::from(f.into_inner())),
                            Value::Int(i) => fvec.push(f64::from(*i)),
                            _ => return Err(AggregateError::InvalidType),
                        }
                    }
                    *digest = digest.merge_unsorted(fvec);
                    buffer.clear();
                }
                let f64_pct = f64::from(percentile.into_inner());
                let result = digest.estimate_quantile(f64_pct);
                Ok(Value::Float(OrderedFloat(result as f32)))
            }
        }
    }
}

/// All aggregate state for a single group.
#[derive(Debug, Clone)]
pub(crate) struct GroupState {
    pub(crate) accumulators: Vec<AccumulatorState>,
}

impl GroupState {
    pub(crate) fn new(defs: &[AggregateDef]) -> Self {
        GroupState {
            accumulators: defs.iter().map(|d| AccumulatorState::new(&d.kind)).collect(),
        }
    }
}

/// Definition of one aggregate (built once in GroupByStream::new).
#[derive(Debug, Clone)]
pub(crate) struct AggregateDef {
    pub(crate) kind: AccumulatorKind,
    pub(crate) extraction: ExtractionStrategy,
    pub(crate) name: Option<String>,
}

impl AggregateDef {
    pub(crate) fn from_named_aggregate(na: &NamedAggregate) -> Self {
        let (kind, extraction) = match &na.aggregate {
            Aggregate::Count(_, Named::Star) => (
                AccumulatorKind::CountStar,
                ExtractionStrategy::None,
            ),
            Aggregate::Count(_, Named::Expression(expr, _)) => (
                AccumulatorKind::Count,
                ExtractionStrategy::Expression(expr.clone()),
            ),
            Aggregate::Sum(_, Named::Expression(expr, _)) => (
                AccumulatorKind::Sum,
                ExtractionStrategy::Expression(expr.clone()),
            ),
            Aggregate::Avg(_, Named::Expression(expr, _)) => (
                AccumulatorKind::Avg,
                ExtractionStrategy::Expression(expr.clone()),
            ),
            Aggregate::Min(_, Named::Expression(expr, _)) => (
                AccumulatorKind::Min,
                ExtractionStrategy::Expression(expr.clone()),
            ),
            Aggregate::Max(_, Named::Expression(expr, _)) => (
                AccumulatorKind::Max,
                ExtractionStrategy::Expression(expr.clone()),
            ),
            Aggregate::First(_, Named::Expression(expr, _)) => (
                AccumulatorKind::First,
                ExtractionStrategy::Expression(expr.clone()),
            ),
            Aggregate::Last(_, Named::Expression(expr, _)) => (
                AccumulatorKind::Last,
                ExtractionStrategy::Expression(expr.clone()),
            ),
            Aggregate::ApproxCountDistinct(_, Named::Expression(expr, _)) => (
                AccumulatorKind::ApproxCountDistinct,
                ExtractionStrategy::Expression(expr.clone()),
            ),
            Aggregate::GroupAs(_, _) => (
                AccumulatorKind::GroupAs,
                ExtractionStrategy::RecordCapture,
            ),
            Aggregate::PercentileDisc(agg, col_name) => (
                AccumulatorKind::PercentileDisc {
                    percentile: agg.percentile,
                    ordering: agg.ordering.clone(),
                },
                ExtractionStrategy::ColumnLookup(col_name.clone()),
            ),
            Aggregate::ApproxPercentile(agg, col_name) => (
                AccumulatorKind::ApproxPercentile {
                    percentile: agg.percentile,
                    ordering: agg.ordering.clone(),
                },
                ExtractionStrategy::ColumnLookup(col_name.clone()),
            ),
            _ => unreachable!("Star variant not valid for non-Count aggregates"),
        };
        AggregateDef {
            kind,
            extraction,
            name: na.name_opt.clone(),
        }
    }
}

/// Returns true if `a < b` for comparable Value types.
/// Used by Min/Max accumulators.
pub(crate) fn value_less_than(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(i1), Value::Int(i2)) => i1 < i2,
        (Value::Float(f1), Value::Float(f2)) => f1 < f2,
        (Value::String(s1), Value::String(s2)) => s1 < s2,
        (Value::DateTime(d1), Value::DateTime(d2)) => d1 < d2,
        (Value::Boolean(b1), Value::Boolean(b2)) => !b1 & b2,
        _ => false,
    }
}

/// Compare two Values with a given ordering direction.
/// Used by PercentileDisc finalize for sorting.
pub(crate) fn value_cmp(a: &Value, b: &Value, ordering: &Ordering) -> std::cmp::Ordering {
    let base = match (a, b) {
        (Value::Int(i1), Value::Int(i2)) => i1.cmp(i2),
        (Value::Float(f1), Value::Float(f2)) => f1.cmp(f2),
        (Value::String(s1), Value::String(s2)) => s1.cmp(s2),
        (Value::DateTime(d1), Value::DateTime(d2)) => d1.cmp(d2),
        (Value::Boolean(b1), Value::Boolean(b2)) => b1.cmp(b2),
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Host(h1), Value::Host(h2)) => h1.to_string().cmp(&h2.to_string()),
        (Value::HttpRequest(h1), Value::HttpRequest(h2)) => h1.to_string().cmp(&h2.to_string()),
        _ => std::cmp::Ordering::Equal,
    };
    match ordering {
        Ordering::Asc => base,
        Ordering::Desc => base.reverse(),
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
        let v = Value::Host(Box::new(common::types::parse_host("192.168.131.39:2817").unwrap()));
        let name = registry.call("host_name", &vec![v.clone()]).unwrap();
        assert_eq!(name, Value::String("192.168.131.39".to_string()));
        let port = registry.call("host_port", &vec![v]).unwrap();
        assert_eq!(port, Value::Int(2817));
    }

    #[test]
    fn test_evaluate_url_functions() {
        let registry = test_registry();
        let v = Value::HttpRequest(Box::new(
            common::types::parse_http_request(
                "GET http://example.com:8000/users/123?mode=json&after=&iteration=1 HTTP/1.1",
            )
            .unwrap(),
        ));
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

    #[test]
    fn test_predicate_pushdown_produces_correct_results() {
        use crate::syntax::ast::{PathExpr, PathSegment};

        let path = std::path::PathBuf::from("data/AWSELB.log");
        if !path.exists() {
            return;
        }
        // Build: SELECT elb_status_code FROM elb WHERE elb_status_code = '200'
        let data_source = Node::DataSource(
            DataSource::File(path, "elb".to_string(), "it".to_string()),
            vec![],
        );
        let formula = Box::new(Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName("elb_status_code".to_string())]))),
            Box::new(Expression::Constant(Value::String("200".to_string()))),
        ));
        let filter = Node::Filter(Box::new(data_source), formula);
        let select = Node::Map(
            vec![Named::Expression(
                Expression::Variable(PathExpr::new(vec![PathSegment::AttrName("elb_status_code".to_string())])),
                Some("elb_status_code".to_string()),
            )],
            Box::new(filter),
        );

        let vars = Variables::default();
        let registry = test_registry();
        let mut stream = select.get(vars, registry, 0).expect("should create stream");
        let mut count = 0;
        while let Some(record) = stream.next().unwrap() {
            let tuples = record.to_tuples();
            for (key, value) in &tuples {
                if key == "elb_status_code" {
                    assert_eq!(value, &Value::String("200".to_string()), "all filtered rows should have status 200");
                }
            }
            count += 1;
        }
        assert!(count > 0, "should have at least one matching record");
    }

    #[test]
    fn test_batch_groupby_through_node_get() {
        let path = std::path::PathBuf::from("data/AWSELB.log");
        if !path.exists() { return; }

        let registry = test_registry();

        // SELECT elb_status_code, count(*) FROM elb GROUP BY elb_status_code
        let ds = Node::DataSource(
            DataSource::File(path, "elb".to_string(), "elb".to_string()),
            vec![],
        );
        let groupby = Node::GroupBy(
            vec![PathExpr::new(vec![PathSegment::AttrName("elb_status_code".to_string())])],
            vec![NamedAggregate::new(
                Aggregate::Count(CountAggregate::new(), Named::Star),
                Some("cnt".to_string()),
            )],
            Box::new(ds),
        );
        let map = Node::Map(vec![
            Named::Expression(
                Expression::Variable(PathExpr::new(vec![
                    PathSegment::AttrName("elb_status_code".to_string()),
                ])),
                None,
            ),
            Named::Expression(
                Expression::Variable(PathExpr::new(vec![
                    PathSegment::AttrName("cnt".to_string()),
                ])),
                None,
            ),
        ], Box::new(groupby));

        let mut stream = map.get(Variables::new(), registry, 0).unwrap();
        let mut total_rows = 0;
        while let Some(record) = stream.next().unwrap() {
            let vars = record.to_variables();
            assert!(vars.contains_key("elb_status_code"));
            assert!(vars.contains_key("cnt"));
            total_rows += 1;
        }
        assert!(total_rows > 0);
    }
}

#[cfg(test)]
mod accumulator_tests {
    use super::*;
    use crate::common::types::Value;
    use ordered_float::OrderedFloat;

    #[test]
    fn test_value_less_than_ints() {
        assert!(value_less_than(&Value::Int(1), &Value::Int(2)));
        assert!(!value_less_than(&Value::Int(2), &Value::Int(1)));
        assert!(!value_less_than(&Value::Int(1), &Value::Int(1)));
    }

    #[test]
    fn test_value_less_than_floats() {
        assert!(value_less_than(
            &Value::Float(OrderedFloat(1.0)),
            &Value::Float(OrderedFloat(2.0))
        ));
    }

    #[test]
    fn test_value_less_than_strings() {
        assert!(value_less_than(
            &Value::String("a".to_string()),
            &Value::String("b".to_string())
        ));
    }

    #[test]
    fn test_value_cmp_asc_desc() {
        let a = Value::Int(1);
        let b = Value::Int(2);
        assert_eq!(value_cmp(&a, &b, &Ordering::Asc), std::cmp::Ordering::Less);
        assert_eq!(value_cmp(&a, &b, &Ordering::Desc), std::cmp::Ordering::Greater);
    }

    #[test]
    fn test_accumulator_state_new_count() {
        let state = AccumulatorState::new(&AccumulatorKind::Count);
        assert!(matches!(state, AccumulatorState::Count(0)));
    }

    #[test]
    fn test_accumulator_state_new_count_star() {
        let state = AccumulatorState::new(&AccumulatorKind::CountStar);
        assert!(matches!(state, AccumulatorState::CountStar(0)));
    }

    #[test]
    fn test_accumulator_state_new_sum() {
        let state = AccumulatorState::new(&AccumulatorKind::Sum);
        assert!(matches!(state, AccumulatorState::Sum(None)));
    }

    #[test]
    fn test_accumulator_state_new_avg() {
        let state = AccumulatorState::new(&AccumulatorKind::Avg);
        match state {
            AccumulatorState::Avg { count, .. } => assert_eq!(count, 0),
            _ => panic!("expected Avg"),
        }
    }

    #[test]
    fn test_accumulator_state_new_min_max() {
        let min = AccumulatorState::new(&AccumulatorKind::Min);
        let max = AccumulatorState::new(&AccumulatorKind::Max);
        assert!(matches!(min, AccumulatorState::Min(None)));
        assert!(matches!(max, AccumulatorState::Max(None)));
    }

    #[test]
    fn test_accumulator_state_new_first_last() {
        let first = AccumulatorState::new(&AccumulatorKind::First);
        let last = AccumulatorState::new(&AccumulatorKind::Last);
        assert!(matches!(first, AccumulatorState::First(None)));
        assert!(matches!(last, AccumulatorState::Last(None)));
    }

    #[test]
    fn test_accumulator_state_new_group_as() {
        let state = AccumulatorState::new(&AccumulatorKind::GroupAs);
        match state {
            AccumulatorState::GroupAs(v) => assert!(v.is_empty()),
            _ => panic!("expected GroupAs"),
        }
    }

    #[test]
    fn test_count_accumulate_skips_null() {
        let mut state = AccumulatorState::new(&AccumulatorKind::Count);
        state.accumulate(&Value::Int(1)).unwrap();
        state.accumulate(&Value::Null).unwrap();
        state.accumulate(&Value::Int(2)).unwrap();
        assert!(matches!(state, AccumulatorState::Count(2)));
    }

    #[test]
    fn test_count_accumulate_skips_missing() {
        let mut state = AccumulatorState::new(&AccumulatorKind::Count);
        state.accumulate(&Value::Int(1)).unwrap();
        state.accumulate(&Value::Missing).unwrap();
        assert!(matches!(state, AccumulatorState::Count(1)));
    }

    #[test]
    fn test_count_star_accumulate_row() {
        let mut state = AccumulatorState::new(&AccumulatorKind::CountStar);
        state.accumulate_row().unwrap();
        state.accumulate_row().unwrap();
        state.accumulate_row().unwrap();
        assert!(matches!(state, AccumulatorState::CountStar(3)));
    }

    #[test]
    fn test_sum_accumulate() {
        let mut state = AccumulatorState::new(&AccumulatorKind::Sum);
        state.accumulate(&Value::Int(10)).unwrap();
        state.accumulate(&Value::Int(20)).unwrap();
        match &state {
            AccumulatorState::Sum(Some(s)) => assert_eq!(s.into_inner(), 30.0),
            _ => panic!("expected Sum(Some(...))"),
        }
    }

    #[test]
    fn test_sum_skips_missing() {
        let mut state = AccumulatorState::new(&AccumulatorKind::Sum);
        state.accumulate(&Value::Missing).unwrap();
        assert!(matches!(state, AccumulatorState::Sum(None)));
    }

    #[test]
    fn test_avg_accumulate() {
        let mut state = AccumulatorState::new(&AccumulatorKind::Avg);
        state.accumulate(&Value::Int(10)).unwrap();
        state.accumulate(&Value::Int(20)).unwrap();
        match &state {
            AccumulatorState::Avg { sum, count } => {
                assert_eq!(sum.into_inner(), 30.0);
                assert_eq!(*count, 2);
            }
            _ => panic!("expected Avg"),
        }
    }

    #[test]
    fn test_min_accumulate() {
        let mut state = AccumulatorState::new(&AccumulatorKind::Min);
        state.accumulate(&Value::Int(5)).unwrap();
        state.accumulate(&Value::Int(2)).unwrap();
        state.accumulate(&Value::Int(8)).unwrap();
        match &state {
            AccumulatorState::Min(Some(Value::Int(2))) => {}
            _ => panic!("expected Min(Some(Int(2)))"),
        }
    }

    #[test]
    fn test_max_accumulate() {
        let mut state = AccumulatorState::new(&AccumulatorKind::Max);
        state.accumulate(&Value::Int(5)).unwrap();
        state.accumulate(&Value::Int(8)).unwrap();
        state.accumulate(&Value::Int(2)).unwrap();
        match &state {
            AccumulatorState::Max(Some(Value::Int(8))) => {}
            _ => panic!("expected Max(Some(Int(8)))"),
        }
    }

    #[test]
    fn test_first_last_accumulate() {
        let mut first = AccumulatorState::new(&AccumulatorKind::First);
        let mut last = AccumulatorState::new(&AccumulatorKind::Last);
        for i in [1, 2, 3] {
            first.accumulate(&Value::Int(i)).unwrap();
            last.accumulate(&Value::Int(i)).unwrap();
        }
        assert!(matches!(first, AccumulatorState::First(Some(Value::Int(1)))));
        assert!(matches!(last, AccumulatorState::Last(Some(Value::Int(3)))));
    }

    #[test]
    fn test_approx_count_distinct_skips_null() {
        let mut state = AccumulatorState::new(&AccumulatorKind::ApproxCountDistinct);
        state.accumulate(&Value::Int(1)).unwrap();
        state.accumulate(&Value::Null).unwrap();
        state.accumulate(&Value::Int(2)).unwrap();
        match &state {
            AccumulatorState::ApproxCountDistinct(hll) => {
                assert!(hll.count() >= 1);
            }
            _ => panic!("expected ApproxCountDistinct"),
        }
    }

    #[test]
    fn test_group_as_accumulate() {
        let mut state = AccumulatorState::new(&AccumulatorKind::GroupAs);
        state.accumulate(&Value::Int(1)).unwrap();
        state.accumulate(&Value::Int(2)).unwrap();
        match &state {
            AccumulatorState::GroupAs(v) => assert_eq!(v.len(), 2),
            _ => panic!("expected GroupAs"),
        }
    }

    #[test]
    fn test_finalize_count_zero() {
        let mut state = AccumulatorState::new(&AccumulatorKind::Count);
        assert_eq!(state.finalize().unwrap(), Value::Int(0));
    }

    #[test]
    fn test_finalize_count_star_zero() {
        let mut state = AccumulatorState::new(&AccumulatorKind::CountStar);
        assert_eq!(state.finalize().unwrap(), Value::Int(0));
    }

    #[test]
    fn test_finalize_sum_empty_is_null() {
        let mut state = AccumulatorState::new(&AccumulatorKind::Sum);
        assert_eq!(state.finalize().unwrap(), Value::Null);
    }

    #[test]
    fn test_finalize_sum_with_values() {
        let mut state = AccumulatorState::new(&AccumulatorKind::Sum);
        state.accumulate(&Value::Int(10)).unwrap();
        state.accumulate(&Value::Int(20)).unwrap();
        assert_eq!(
            state.finalize().unwrap(),
            Value::Float(OrderedFloat(30.0))
        );
    }

    #[test]
    fn test_finalize_avg_empty_is_null() {
        let mut state = AccumulatorState::new(&AccumulatorKind::Avg);
        assert_eq!(state.finalize().unwrap(), Value::Null);
    }

    #[test]
    fn test_finalize_avg_with_values() {
        let mut state = AccumulatorState::new(&AccumulatorKind::Avg);
        state.accumulate(&Value::Int(10)).unwrap();
        state.accumulate(&Value::Int(20)).unwrap();
        assert_eq!(
            state.finalize().unwrap(),
            Value::Float(OrderedFloat(15.0))
        );
    }

    #[test]
    fn test_finalize_min_max_empty_is_null() {
        let mut min = AccumulatorState::new(&AccumulatorKind::Min);
        let mut max = AccumulatorState::new(&AccumulatorKind::Max);
        assert_eq!(min.finalize().unwrap(), Value::Null);
        assert_eq!(max.finalize().unwrap(), Value::Null);
    }

    #[test]
    fn test_finalize_first_last_empty_is_null() {
        let mut first = AccumulatorState::new(&AccumulatorKind::First);
        let mut last = AccumulatorState::new(&AccumulatorKind::Last);
        assert_eq!(first.finalize().unwrap(), Value::Null);
        assert_eq!(last.finalize().unwrap(), Value::Null);
    }

    #[test]
    fn test_finalize_group_as_empty() {
        let mut state = AccumulatorState::new(&AccumulatorKind::GroupAs);
        assert_eq!(state.finalize().unwrap(), Value::Array(vec![]));
    }

    #[test]
    fn test_finalize_approx_count_distinct_empty() {
        let mut state = AccumulatorState::new(&AccumulatorKind::ApproxCountDistinct);
        assert_eq!(state.finalize().unwrap(), Value::Int(0));
    }

    #[test]
    fn test_finalize_percentile_disc_empty_is_null() {
        let mut state = AccumulatorState::new(&AccumulatorKind::PercentileDisc {
            percentile: OrderedFloat(0.5),
            ordering: Ordering::Asc,
        });
        assert_eq!(state.finalize().unwrap(), Value::Null);
    }

    #[test]
    fn test_finalize_percentile_disc_with_values() {
        let mut state = AccumulatorState::new(&AccumulatorKind::PercentileDisc {
            percentile: OrderedFloat(0.5),
            ordering: Ordering::Asc,
        });
        for i in [3, 1, 4, 1, 5] {
            state.accumulate(&Value::Int(i)).unwrap();
        }
        let result = state.finalize().unwrap();
        // After sorting [1,1,3,4,5], idx = (5 * 0.5) as usize = 2, so values[2] = 3
        assert_eq!(result, Value::Int(3));
    }

    #[test]
    fn test_finalize_min_max_with_values() {
        let mut min = AccumulatorState::new(&AccumulatorKind::Min);
        let mut max = AccumulatorState::new(&AccumulatorKind::Max);
        for i in [5, 2, 8] {
            min.accumulate(&Value::Int(i)).unwrap();
            max.accumulate(&Value::Int(i)).unwrap();
        }
        assert_eq!(min.finalize().unwrap(), Value::Int(2));
        assert_eq!(max.finalize().unwrap(), Value::Int(8));
    }

    #[test]
    fn test_aggregate_def_from_count_star() {
        let na = NamedAggregate::new(
            Aggregate::Count(CountAggregate::new(), Named::Star),
            Some("cnt".to_string()),
        );
        let def = AggregateDef::from_named_aggregate(&na);
        assert!(matches!(def.kind, AccumulatorKind::CountStar));
        assert!(matches!(def.extraction, ExtractionStrategy::None));
        assert_eq!(def.name, Some("cnt".to_string()));
    }

    #[test]
    fn test_aggregate_def_from_sum_expr() {
        let path = PathExpr::new(vec![PathSegment::AttrName("x".to_string())]);
        let na = NamedAggregate::new(
            Aggregate::Sum(
                SumAggregate::new(),
                Named::Expression(Expression::Variable(path), Some("x".to_string())),
            ),
            Some("total".to_string()),
        );
        let def = AggregateDef::from_named_aggregate(&na);
        assert!(matches!(def.kind, AccumulatorKind::Sum));
        assert!(matches!(def.extraction, ExtractionStrategy::Expression(_)));
        assert_eq!(def.name, Some("total".to_string()));
    }

    #[test]
    fn test_aggregate_def_from_percentile_disc() {
        let na = NamedAggregate::new(
            Aggregate::PercentileDisc(
                PercentileDiscAggregate::new(OrderedFloat(0.5), Ordering::Asc),
                "x".to_string(),
            ),
            None,
        );
        let def = AggregateDef::from_named_aggregate(&na);
        assert!(matches!(def.kind, AccumulatorKind::PercentileDisc { .. }));
        assert!(matches!(def.extraction, ExtractionStrategy::ColumnLookup(_)));
    }

    #[test]
    fn test_aggregate_def_from_group_as() {
        let na = NamedAggregate::new(
            Aggregate::GroupAs(
                GroupAsAggregate::new(),
                Named::Expression(
                    Expression::Constant(Value::Null),
                    None,
                ),
            ),
            Some("grp".to_string()),
        );
        let def = AggregateDef::from_named_aggregate(&na);
        assert!(matches!(def.kind, AccumulatorKind::GroupAs));
        assert!(matches!(def.extraction, ExtractionStrategy::RecordCapture));
    }

    #[test]
    fn test_sum_skips_null() {
        let mut state = AccumulatorState::new(&AccumulatorKind::Sum);
        state.accumulate(&Value::Int(10)).unwrap();
        state.accumulate(&Value::Null).unwrap();
        state.accumulate(&Value::Int(20)).unwrap();
        assert_eq!(
            state.finalize().unwrap(),
            Value::Float(OrderedFloat(30.0))
        );
    }

    #[test]
    fn test_avg_skips_null() {
        let mut state = AccumulatorState::new(&AccumulatorKind::Avg);
        state.accumulate(&Value::Int(10)).unwrap();
        state.accumulate(&Value::Null).unwrap();
        state.accumulate(&Value::Int(20)).unwrap();
        // Average of 10 and 20 = 15 (null skipped)
        assert_eq!(
            state.finalize().unwrap(),
            Value::Float(OrderedFloat(15.0))
        );
    }
}
